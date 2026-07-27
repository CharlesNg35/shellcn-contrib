package vault

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	vaultapi "github.com/hashicorp/vault/api"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	mountCacheTTL = 30 * time.Second
	scanCacheTTL  = 20 * time.Second
	// maxScanCacheEntries bounds what the walk cache may hold; the whole cache is
	// dropped rather than evicted one by one, because a walk is cheap to redo and
	// a partial cache would page against two different snapshots.
	maxScanCacheEntries = 32
)

// mountInfo is the resolved shape of one entry in sys/mounts: the mount path
// keeps its trailing slash so prefix matching against a secret key is exact.
type mountInfo struct {
	Path      string `json:"path"`
	Type      string `json:"type"`
	KVVersion int    `json:"kv_version"`
	out       *vaultapi.MountOutput
}

type mountCache struct {
	at    time.Time
	items []mountInfo
}

// scanCache is one snapshot of a bounded hierarchy walk.
type scanCache struct {
	at        time.Time
	names     []string
	truncated bool
}

type Session struct {
	client    *vaultapi.Client
	transport *http.Transport
	opts      options
	closed    atomic.Bool

	mu     sync.Mutex
	mounts map[string]mountCache
	scans  map[string]scanCache
}

func connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	opts, err := parseOptions(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.Net == nil {
		return nil, fmt.Errorf("%w: network transport is required", plugin.ErrUnavailable)
	}
	tlsConfig, err := clientTLSConfig(opts)
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{
		DialContext:           cfg.Net.DialContext,
		TLSClientConfig:       tlsConfig,
		TLSHandshakeTimeout:   opts.Timeout,
		ResponseHeaderTimeout: opts.Timeout,
		MaxIdleConnsPerHost:   4,
		ForceAttemptHTTP2:     true,
	}
	client, err := vaultapi.NewClient(&vaultapi.Config{
		Address:    opts.address(),
		HttpClient: &http.Client{Transport: transport},
		Timeout:    opts.Timeout,
		MaxRetries: 2,
	})
	if err != nil {
		transport.CloseIdleConnections()
		return nil, fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	// NewClient seeds the token and namespace from VAULT_* environment variables,
	// so both are reset from the connection config unconditionally: the gateway
	// host's environment must never leak into a connection.
	client.SetToken("")
	client.SetNamespace(opts.Namespace)

	s := &Session{client: client, transport: transport, opts: opts, mounts: map[string]mountCache{}, scans: map[string]scanCache{}}
	if err := s.authenticate(ctx); err != nil {
		_ = s.Close()
		return nil, err
	}
	if err := s.HealthCheck(ctx); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

func (s *Session) authenticate(ctx context.Context) error {
	if s.opts.AuthMethod != authAppRole {
		s.client.SetToken(s.opts.Token)
		return nil
	}
	reqCtx, cancel := context.WithTimeout(ctx, s.opts.Timeout)
	defer cancel()
	secret, err := s.client.Logical().WriteWithContext(reqCtx, "auth/"+s.opts.AppRolePath+"/login", map[string]any{
		"role_id":   s.opts.RoleID,
		"secret_id": s.opts.SecretID,
	})
	if err != nil {
		return fmt.Errorf("%w: AppRole login failed: %v", plugin.ErrUnauthorized, vaultMessage(err))
	}
	if secret == nil || secret.Auth == nil || secret.Auth.ClientToken == "" {
		return fmt.Errorf("%w: AppRole login returned no client token", plugin.ErrUnauthorized)
	}
	s.client.SetToken(secret.Auth.ClientToken)
	return nil
}

func (s *Session) HealthCheck(ctx context.Context) error {
	if err := s.ensureOpen(); err != nil {
		return err
	}
	reqCtx, cancel := context.WithTimeout(ctx, s.opts.Timeout)
	defer cancel()
	status, err := s.client.Sys().SealStatusWithContext(reqCtx)
	if err != nil {
		return fmt.Errorf("%w: vault seal status: %v", plugin.ErrUnavailable, vaultMessage(err))
	}
	if status != nil && status.Sealed {
		return fmt.Errorf("%w: vault is sealed", plugin.ErrUnavailable)
	}
	if _, err := s.client.Auth().Token().LookupSelfWithContext(reqCtx); err != nil {
		return fmt.Errorf("%w: vault token lookup failed: %v", plugin.ErrUnauthorized, vaultMessage(err))
	}
	return nil
}

func (s *Session) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}

func (s *Session) Close() error {
	s.closed.Store(true)
	if s.client != nil {
		s.client.ClearToken()
	}
	s.mu.Lock()
	clear(s.mounts)
	clear(s.scans)
	s.mu.Unlock()
	if s.transport != nil {
		s.transport.CloseIdleConnections()
	}
	return nil
}

func (s *Session) ensureOpen() error {
	if s == nil || s.closed.Load() {
		return fmt.Errorf("%w: vault session closed", plugin.ErrUnavailable)
	}
	return nil
}

// namespaceFor resolves the effective Vault namespace for one request: the
// workspace scope selector wins, otherwise the connection's own namespace.
func (s *Session) namespaceFor(rc *plugin.RequestContext) string {
	if ns := normalizeNamespace(param(rc, "namespace")); ns != "" {
		return ns
	}
	return s.opts.Namespace
}

// clientFor returns a namespace-scoped client. WithNamespace copies the client
// with its own header map, so concurrent requests never share scope.
func (s *Session) clientFor(rc *plugin.RequestContext) (*vaultapi.Client, string, error) {
	if err := s.ensureOpen(); err != nil {
		return nil, "", err
	}
	ns := s.namespaceFor(rc)
	return s.client.WithNamespace(ns), ns, nil
}

// mountList returns sys/mounts for one namespace as a freshly owned slice, so a
// caller may sort or filter it without mutating the shared cache.
func (s *Session) mountList(ctx context.Context, client *vaultapi.Client, namespace string, refresh bool) ([]mountInfo, error) {
	s.mu.Lock()
	cached, ok := s.mounts[namespace]
	s.mu.Unlock()
	if ok && !refresh && time.Since(cached.at) < mountCacheTTL {
		return slices.Clone(cached.items), nil
	}
	mounts, err := client.Sys().ListMountsWithContext(ctx)
	if err != nil {
		return nil, vaultErr(err)
	}
	items := make([]mountInfo, 0, len(mounts))
	for path, out := range mounts {
		if out == nil {
			continue
		}
		items = append(items, mountInfo{Path: path, Type: out.Type, KVVersion: kvVersion(out), out: out})
	}
	slices.SortFunc(items, func(a, b mountInfo) int { return strings.Compare(a.Path, b.Path) })
	s.mu.Lock()
	s.mounts[namespace] = mountCache{at: time.Now(), items: items}
	s.mu.Unlock()
	return slices.Clone(items), nil
}

// scan snapshots one bounded hierarchy walk per scope for scanCacheTTL. Vault
// has no server-side paging for LIST, so a listing has to walk; keeping the
// snapshot means every page, sort, and filter of that listing covers one fixed
// set instead of a set that changes shape under the cursor. The returned slice
// belongs to the caller.
func (s *Session) scan(ctx context.Context, key string, walk func(context.Context) ([]string, bool, error)) ([]string, bool, error) {
	s.mu.Lock()
	cached, ok := s.scans[key]
	s.mu.Unlock()
	if ok && time.Since(cached.at) < scanCacheTTL {
		return slices.Clone(cached.names), cached.truncated, nil
	}
	names, truncated, err := walk(ctx)
	if err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	if len(s.scans) >= maxScanCacheEntries {
		clear(s.scans)
	}
	s.scans[key] = scanCache{at: time.Now(), names: names, truncated: truncated}
	s.mu.Unlock()
	return slices.Clone(names), truncated, nil
}

// dropScans invalidates the snapshots after a mutation, so the next listing
// shows the write instead of the walk that preceded it.
func (s *Session) dropScans() {
	s.mu.Lock()
	clear(s.scans)
	s.mu.Unlock()
}

// scanKey scopes a snapshot to everything that changes its contents, so two
// namespaces or two folders never read each other's walk.
func scanKey(parts ...string) string { return strings.Join(parts, "\x00") }

// resolveMount splits a fully qualified secret key such as "secret/team/app"
// into its mount and the path inside it, preferring the longest matching mount
// so nested mounts ("kv/" and "kv/team/") stay unambiguous.
func (s *Session) resolveMount(ctx context.Context, client *vaultapi.Client, namespace, key string) (mountInfo, string, error) {
	key = strings.TrimLeft(strings.TrimSpace(key), "/")
	if key == "" {
		return mountInfo{}, "", fmt.Errorf("%w: secret key is required", plugin.ErrInvalidInput)
	}
	mounts, err := s.mountList(ctx, client, namespace, false)
	if err != nil {
		return mountInfo{}, "", err
	}
	best := mountInfo{}
	for _, m := range mounts {
		if strings.HasPrefix(key, m.Path) && len(m.Path) > len(best.Path) {
			best = m
		}
	}
	if best.Path == "" {
		return mountInfo{}, "", fmt.Errorf("%w: no mounted secrets engine owns %q", plugin.ErrNotFound, key)
	}
	return best, strings.TrimPrefix(key, best.Path), nil
}

// mountByPath looks one mount up by its path, tolerating a caller that omitted
// the trailing slash Vault always reports.
func (s *Session) mountByPath(ctx context.Context, client *vaultapi.Client, namespace, path string) (mountInfo, error) {
	trimmed := strings.Trim(strings.TrimSpace(path), "/")
	if trimmed == "" {
		return mountInfo{}, fmt.Errorf("%w: a secrets engine mount path is required", plugin.ErrInvalidInput)
	}
	want := trimmed + "/"
	mounts, err := s.mountList(ctx, client, namespace, false)
	if err != nil {
		return mountInfo{}, err
	}
	for _, m := range mounts {
		if m.Path == want {
			return m, nil
		}
	}
	return mountInfo{}, fmt.Errorf("%w: secrets engine %q is not mounted", plugin.ErrNotFound, path)
}

func kvVersion(out *vaultapi.MountOutput) int {
	if out == nil {
		return 0
	}
	switch out.Type {
	case "kv":
		if out.Options["version"] == "2" {
			return 2
		}
		return 1
	case "generic", "cubbyhole":
		return 1
	default:
		return 0
	}
}

func unwrap(sess plugin.Session) (*Session, error) {
	if s, ok := sess.(*Session); ok {
		return s, nil
	}
	// rc.Session may be the core's borrowed handle around the live session.
	if h, ok := sess.(interface{ Session() plugin.Session }); ok {
		if s, ok := h.Session().(*Session); ok {
			return s, nil
		}
	}
	return nil, fmt.Errorf("%w: vault session unavailable", plugin.ErrUnavailable)
}

func ensureWritable(s *Session) error {
	if s.opts.ReadOnly {
		return fmt.Errorf("%w: read-only mode blocks write, revoke, and destroy operations", plugin.ErrForbidden)
	}
	return nil
}

// vaultMessage renders a Vault API failure without the request URL, which can
// carry a secret path into logs and audit records. ResponseError.Error() embeds
// that URL, so a response failure never falls through to err.Error() - not even
// when Vault answered with an empty error list.
func vaultMessage(err error) string {
	if err == nil {
		return ""
	}
	var respErr *vaultapi.ResponseError
	if errors.As(err, &respErr) {
		if len(respErr.Errors) > 0 {
			return strings.Join(respErr.Errors, "; ")
		}
		return fmt.Sprintf("vault returned %d", respErr.StatusCode)
	}
	return err.Error()
}

func vaultErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: vault request cancelled", plugin.ErrUnavailable)
	}
	if errors.Is(err, vaultapi.ErrSecretNotFound) {
		return fmt.Errorf("%w: %s", plugin.ErrNotFound, vaultMessage(err))
	}
	var respErr *vaultapi.ResponseError
	if errors.As(err, &respErr) {
		message := vaultMessage(err)
		switch respErr.StatusCode {
		case http.StatusBadRequest, http.StatusUnprocessableEntity:
			return fmt.Errorf("%w: %s", plugin.ErrInvalidInput, message)
		case http.StatusUnauthorized:
			return fmt.Errorf("%w: %s", plugin.ErrUnauthorized, message)
		case http.StatusForbidden:
			return fmt.Errorf("%w: %s", plugin.ErrForbidden, message)
		case http.StatusNotFound:
			return fmt.Errorf("%w: %s", plugin.ErrNotFound, message)
		case http.StatusMethodNotAllowed, http.StatusNotImplemented:
			return fmt.Errorf("%w: %s", plugin.ErrNotSupported, message)
		case http.StatusConflict:
			return fmt.Errorf("%w: %s", plugin.ErrConflict, message)
		}
		return fmt.Errorf("%w: vault returned %d: %s", plugin.ErrUnavailable, respErr.StatusCode, message)
	}
	return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
}
