package consul

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hashicorp/consul/api"

	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

// Session holds one connection's Consul clients. client carries the configured
// request timeout; watcher has none so blocking queries live as long as their
// context.
type Session struct {
	client    *api.Client
	watcher   *api.Client
	transport *http.Transport
	opts      options
	closed    atomic.Bool
}

func connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	opts, err := parseOptions(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.Net == nil {
		return nil, fmt.Errorf("%w: network transport is required", plugin.ErrUnavailable)
	}
	tlsConfig, err := sqldb.TLSConfig(sqldb.TLSOptions{
		Mode:              opts.TLSMode,
		Host:              opts.Host,
		CACertificate:     opts.CACertificate,
		ClientCertificate: opts.ClientCertificate,
	})
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{
		DialContext:         cfg.Net.DialContext,
		TLSClientConfig:     tlsConfig,
		MaxIdleConns:        8,
		MaxIdleConnsPerHost: 8,
		IdleConnTimeout:     90 * time.Second,
	}
	client, err := newClient(opts, transport, opts.Timeout)
	if err != nil {
		return nil, err
	}
	watcher, err := newClient(opts, transport, 0)
	if err != nil {
		return nil, err
	}
	s := &Session{client: client, watcher: watcher, transport: transport, opts: opts}
	if err := s.HealthCheck(ctx); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

func newClient(opts options, transport *http.Transport, timeout time.Duration) (*api.Client, error) {
	client, err := api.NewClient(&api.Config{
		Address:    net.JoinHostPort(opts.Host, strconv.Itoa(opts.Port)),
		Scheme:     opts.scheme(),
		PathPrefix: opts.PathPrefix,
		Datacenter: opts.Datacenter,
		Namespace:  opts.Namespace,
		Partition:  opts.Partition,
		Token:      opts.Token,
		Transport:  transport,
		HttpClient: &http.Client{Transport: transport, Timeout: timeout},
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	return client, nil
}

func unwrap(sess plugin.Session) (*Session, error) {
	if s, ok := sess.(*Session); ok {
		return s, nil
	}
	type sessionGetter interface {
		Session() plugin.Session
	}
	if h, ok := sess.(sessionGetter); ok {
		if s, ok := h.Session().(*Session); ok {
			return s, nil
		}
	}
	return nil, fmt.Errorf("%w: consul session unavailable", plugin.ErrUnavailable)
}

func (s *Session) endpoint() string {
	return s.opts.scheme() + "://" + net.JoinHostPort(s.opts.Host, strconv.Itoa(s.opts.Port)) + s.opts.PathPrefix
}

func (s *Session) HealthCheck(ctx context.Context) error {
	if err := s.ensureOpen(); err != nil {
		return err
	}
	q := (&api.QueryOptions{Datacenter: s.opts.Datacenter}).WithContext(ctx)
	if _, err := s.client.Status().LeaderWithQueryOptions(q); err != nil {
		return fmt.Errorf("%w: consul status: %v", plugin.ErrUnavailable, err)
	}
	return nil
}

func (s *Session) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}

func (s *Session) Close() error {
	s.closed.Store(true)
	if s.transport != nil {
		s.transport.CloseIdleConnections()
	}
	return nil
}

func (s *Session) ensureOpen() error {
	if s == nil || s.closed.Load() {
		return fmt.Errorf("%w: consul session closed", plugin.ErrUnavailable)
	}
	return nil
}

func (s *Session) ensureWritable() error {
	if s.opts.ReadOnly {
		return fmt.Errorf("%w: read-only mode blocks write operations", plugin.ErrForbidden)
	}
	return nil
}

// datacenter resolves the datacenter scope for one request: the workspace scope
// filter wins, then the connection default, then the agent's own datacenter.
func (s *Session) datacenter(rc *plugin.RequestContext) string {
	if dc := paramOrQuery(rc, "datacenter"); dc != "" {
		return dc
	}
	return s.opts.Datacenter
}

// query builds the scoped read options every list and read handler uses, so no
// handler can lose the datacenter, namespace, or partition on a later page.
func (s *Session) query(rc *plugin.RequestContext) *api.QueryOptions {
	return (&api.QueryOptions{
		Datacenter: s.datacenter(rc),
		Namespace:  s.opts.Namespace,
		Partition:  s.opts.Partition,
		AllowStale: s.opts.AllowStale,
	}).WithContext(rc.Ctx)
}

func (s *Session) writeOptions(rc *plugin.RequestContext) *api.WriteOptions {
	return (&api.WriteOptions{
		Datacenter: s.datacenter(rc),
		Namespace:  s.opts.Namespace,
		Partition:  s.opts.Partition,
	}).WithContext(rc.Ctx)
}

func session(rc *plugin.RequestContext) (*Session, error) {
	s, err := unwrap(rc.Session)
	if err != nil {
		return nil, err
	}
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}
	return s, nil
}

func writableSession(rc *plugin.RequestContext) (*Session, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := s.ensureWritable(); err != nil {
		return nil, err
	}
	return s, nil
}

// paramOrQuery reads a renderer-supplied param, falling back to the `p.<name>`
// query form the generic grid uses for list panels.
func paramOrQuery(rc *plugin.RequestContext, key string) string {
	if value := strings.TrimSpace(rc.Param(key)); value != "" {
		return value
	}
	return strings.TrimSpace(rc.Query().Get("p." + key))
}

func requiredParam(rc *plugin.RequestContext, key string) (string, error) {
	value := paramOrQuery(rc, key)
	if value == "" {
		return "", fmt.Errorf("%w: %s is required", plugin.ErrInvalidInput, key)
	}
	return value, nil
}

// aclDisabled reports a cluster that answers ACL endpoints with "ACLs are
// disabled"; those panels degrade to an empty list instead of an error.
func aclDisabled(err error) bool {
	var status api.StatusError
	if !errors.As(err, &status) {
		return false
	}
	if status.Code != http.StatusUnauthorized {
		return false
	}
	return strings.Contains(strings.ToLower(status.Body), "acl")
}

func consulErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: consul request cancelled", plugin.ErrUnavailable)
	}
	var status api.StatusError
	if errors.As(err, &status) {
		body := strings.TrimSpace(status.Body)
		switch status.Code {
		case http.StatusUnauthorized:
			return fmt.Errorf("%w: consul rejected the ACL token: %s", plugin.ErrUnauthorized, body)
		case http.StatusForbidden:
			return fmt.Errorf("%w: the ACL token is missing a required privilege: %s", plugin.ErrForbidden, body)
		case http.StatusNotFound:
			return fmt.Errorf("%w: %s", plugin.ErrNotFound, body)
		case http.StatusBadRequest:
			return fmt.Errorf("%w: %s", plugin.ErrInvalidInput, body)
		case http.StatusConflict, http.StatusPreconditionFailed:
			return fmt.Errorf("%w: %s", plugin.ErrConflict, body)
		}
	}
	return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
}
