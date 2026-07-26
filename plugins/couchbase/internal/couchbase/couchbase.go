// Package couchbase implements the Couchbase Server protocol plugin.
//
// Couchbase's own Go SDK (gocb/gocbcore) opens its own sockets and exposes no
// dialer or transport hook, so it cannot honour the gateway's egress contract.
// This plugin therefore speaks the documented Couchbase REST APIs - the cluster
// management API and the Query (SQL++/N1QL) service - over HTTP clients built on
// cfg.Net, which keeps the gateway the single audited egress point and works
// identically for direct and agent connections.
package couchbase

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn-contrib/shared/dbcred"
	"github.com/charlesng35/shellcn-contrib/shared/searchrest"
	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	protocolName          = "couchbase"
	defaultManagementPort = 8091
	defaultQueryPort      = 8093
	defaultTimeout        = 30 * time.Second
	defaultPageLimit      = 100
	defaultSampleSize     = 50
	credentialField       = "credential_id"
	authPassword          = "password"
	authCredential        = "credential"
)

const iconSVG = `<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" id="Couchbase--Streamline-Svg-Logos" height="24" width="24"><desc>Couchbase Streamline Icon: https://streamlinehq.com</desc><path fill="#ed2226" d="M12 0.25C5.521525 0.25 0.25 5.503775 0.25 12c0 6.478475 5.253775 11.75 11.75 11.75 6.478475 0 11.75 -5.253775 11.75 -11.75S18.478475 0.25 12 0.25Zm7.9339 13.8089c0 0.709975 -0.408225 1.3312 -1.206925 1.4732 -1.38445 0.2485 -4.295325 0.390475 -6.726975 0.390475 -2.43165 0 -5.342525 -0.141975 -6.726975 -0.390475 -0.7987 -0.142 -1.20695 -0.763225 -1.20695 -1.4732V9.4796c0 -0.70995 0.55025 -1.366675 1.20695 -1.473175 0.40825 -0.071 1.3667 -0.142 2.112175 -0.142 0.283975 0 0.514725 0.213 0.514725 0.550225v3.212625l4.117825 -0.08875 4.117825 0.08875V8.41465c0 -0.337225 0.23075 -0.550225 0.514725 -0.550225 0.745475 0 1.703925 0.071 2.112175 0.142 0.67445 0.1065 1.206925 0.763225 1.206925 1.473175 -0.0355 1.5087 -0.0355 3.052875 -0.0355 4.5793Z" stroke-width="0.25"></path></svg>`

type Plugin struct{}

type row = plugin.TableRow

type actionResult struct {
	OK      bool   `json:"ok"`
	Message string `json:"message,omitempty"`
}

// Options is the validated runtime view of a connection's config. Schema
// defaults are UI hints; every fallback is applied here.
type Options struct {
	Host            string
	ManagementPort  int
	QueryPort       int
	Scheme          string
	TLSConfig       *tls.Config
	Auth            string
	Username        string
	Password        string
	Bucket          string
	ScanConsistency string
	ReadOnly        bool
	RequireConfirm  bool
	Timeout         time.Duration
	PageLimit       int
	RedactPatterns  []string
}

func (o Options) managementURL() string {
	return o.Scheme + "://" + net.JoinHostPort(o.Host, strconv.Itoa(o.ManagementPort))
}

func (o Options) queryURL() string {
	return o.Scheme + "://" + net.JoinHostPort(o.Host, strconv.Itoa(o.QueryPort))
}

// Session holds the two REST clients one connection needs: the cluster
// management API and the Query service. Both dial through the gateway.
type Session struct {
	mgmt *searchrest.Client
	// The Query service answers with a rich JSON envelope even on 4xx/5xx, so it
	// gets a plain HTTP client and the envelope decides the SDK error sentinel.
	query    *http.Client
	queryURL string
	auth     string
	net      plugin.NetTransport
	opts     Options

	mu       sync.Mutex
	inflight map[string]context.CancelFunc
	seq      uint64
}

// proxyTransport dials the web console through the gateway, the same egress path
// the REST clients use.
func (s *Session) proxyTransport() http.RoundTripper {
	return &http.Transport{DialContext: s.net.DialContext, TLSClientConfig: s.opts.TLSConfig}
}

func New() *Plugin { return &Plugin{} }

func (p *Plugin) Routes() []plugin.Route { return routes() }

func (p *Plugin) Connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	if cfg.Net == nil {
		return nil, fmt.Errorf("%w: network transport is unavailable", plugin.ErrUnavailable)
	}
	opts, err := parseOptions(cfg)
	if err != nil {
		return nil, err
	}
	auth := searchrest.Auth{
		Header: "Authorization",
		Value:  "Basic " + base64.StdEncoding.EncodeToString([]byte(opts.Username+":"+opts.Password)),
	}
	client := func(endpoint string) *searchrest.Client {
		return searchrest.New(searchrest.Options{
			Endpoint:  endpoint,
			Auth:      auth,
			TLSConfig: opts.TLSConfig,
			Timeout:   opts.Timeout,
			Dialer:    cfg.Net.DialContext,
		})
	}
	s := &Session{
		mgmt: client(opts.managementURL()),
		query: &http.Client{
			Timeout:   opts.Timeout + 30*time.Second,
			Transport: &http.Transport{DialContext: cfg.Net.DialContext, TLSClientConfig: opts.TLSConfig},
		},
		queryURL: opts.queryURL(),
		auth:     auth.Value,
		net:      cfg.Net,
		opts:     opts,
		inflight: map[string]context.CancelFunc{},
	}
	if err := s.HealthCheck(ctx); err != nil {
		s.closeClients()
		return nil, err
	}
	return s, nil
}

func (s *Session) HealthCheck(ctx context.Context) error {
	var out map[string]any
	if err := s.mgmt.Do(ctx, "GET", "/pools/default", nil, nil, &out); err != nil {
		return err
	}
	if len(out) == 0 {
		return fmt.Errorf("%w: cluster is not initialized", plugin.ErrUnavailable)
	}
	return nil
}

func (s *Session) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}

func (s *Session) Close() error {
	s.mu.Lock()
	for _, cancel := range s.inflight {
		cancel()
	}
	s.inflight = map[string]context.CancelFunc{}
	s.mu.Unlock()
	s.closeClients()
	return nil
}

func (s *Session) closeClients() {
	s.mgmt.Close()
	s.query.CloseIdleConnections()
}

func parseOptions(cfg plugin.ConnectConfig) (Options, error) {
	host := strings.TrimSpace(cfg.String("host"))
	if host == "" {
		return Options{}, fmt.Errorf("%w: host is required", plugin.ErrInvalidInput)
	}
	managementPort, ok := cfg.Int("port")
	if !ok || managementPort == 0 {
		managementPort = defaultManagementPort
	}
	queryPort, ok := cfg.Int("query_port")
	if !ok || queryPort == 0 {
		queryPort = defaultQueryPort
	}
	for _, port := range []int{managementPort, queryPort} {
		if port < 1 || port > 65535 {
			return Options{}, fmt.Errorf("%w: port must be between 1 and 65535", plugin.ErrInvalidInput)
		}
	}
	tlsMode := broker.StringValue(cfg.Config, "tls_mode", "disable")
	tlsConfig, err := sqldb.TLSConfig(sqldb.TLSOptions{Mode: tlsMode, Host: host, CACertificate: cfg.String("ca_certificate")})
	if err != nil {
		return Options{}, err
	}
	scheme := "http"
	if tlsConfig != nil {
		scheme = "https"
	}

	authMode := broker.StringValue(cfg.Config, "auth", authPassword)
	var material dbcred.AuthMaterial
	switch authMode {
	case authPassword:
		material = dbcred.AuthMaterial{Username: strings.TrimSpace(cfg.String("username")), Password: cfg.String("password")}
	case authCredential:
		if kind := dbcred.ResolvedKind(cfg, credentialField); kind != "" && kind != plugin.CredentialKindDBPassword {
			return Options{}, fmt.Errorf("%w: Couchbase stored credentials must be database passwords", plugin.ErrInvalidInput)
		}
		material = dbcred.AuthMaterial{
			Username: dbcred.ResolvedIdentity(cfg, credentialField),
			Password: dbcred.ResolvedSecret(cfg, credentialField),
		}
	default:
		return Options{}, fmt.Errorf("%w: unsupported authentication mode %q", plugin.ErrInvalidInput, authMode)
	}
	if material.Username == "" {
		return Options{}, fmt.Errorf("%w: username is required", plugin.ErrInvalidInput)
	}
	if material.Password == "" {
		return Options{}, fmt.Errorf("%w: password is required", plugin.ErrInvalidInput)
	}

	scanConsistency := broker.StringValue(cfg.Config, "scan_consistency", "request_plus")
	switch scanConsistency {
	case "not_bounded", "request_plus":
	default:
		return Options{}, fmt.Errorf("%w: unsupported scan consistency %q", plugin.ErrInvalidInput, scanConsistency)
	}

	bucket := strings.TrimSpace(cfg.String("bucket"))
	if bucket != "" {
		if _, err := safeBucket(bucket); err != nil {
			return Options{}, err
		}
	}
	return Options{
		Host:            host,
		ManagementPort:  managementPort,
		QueryPort:       queryPort,
		Scheme:          scheme,
		TLSConfig:       tlsConfig,
		Auth:            authMode,
		Username:        material.Username,
		Password:        material.Password,
		Bucket:          bucket,
		ScanConsistency: scanConsistency,
		ReadOnly:        broker.BoolValue(cfg.Config, "read_only", true),
		RequireConfirm:  broker.BoolValue(cfg.Config, "require_write_confirmation", true),
		Timeout:         broker.DurationValue(cfg.Config, "timeout", defaultTimeout),
		PageLimit:       broker.IntValue(cfg.Config, "page_limit", defaultPageLimit, 1, plugin.MaxPageLimit),
		RedactPatterns:  sqldb.ParsePatterns(cfg.String("redact_fields"), sqldb.DefaultRedactColumnPatterns()),
	}, nil
}

func configSchema() plugin.Schema {
	passwordAuth := &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: authPassword}}}
	credentialAuth := &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: authCredential}}}
	verifiedTLS := &plugin.Condition{AllOf: []plugin.Rule{{Field: "tls_mode", Op: plugin.OpIn, Value: []any{"verify-ca", "verify-full"}}}}
	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Cluster", Fields: []plugin.Field{
			{Key: "host", Label: "Host", Type: plugin.FieldText, Required: true, Placeholder: "couchbase.example.internal", Help: "Any cluster node; the management API resolves the rest of the topology."},
			{Key: "port", Label: "Management port", Type: plugin.FieldNumber, Required: true, Default: defaultManagementPort, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 65535}}},
			{Key: "query_port", Label: "Query port", Type: plugin.FieldNumber, Required: true, Default: defaultQueryPort, Help: "SQL++ (N1QL) service port: 8093 plain, 18093 with TLS.", Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 65535}}},
			{Key: "bucket", Label: "Default bucket", Type: plugin.FieldText, Help: "Pre-selects this bucket in the workspace bucket picker."},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: authPassword, Options: []plugin.Option{
				{Label: "Password", Value: authPassword},
				{Label: "Stored password", Value: authCredential},
			}},
			{Key: "username", Label: "Username", Type: plugin.FieldText, Required: true, Default: "Administrator", VisibleWhen: passwordAuth},
			{Key: "password", Label: "Password", Type: plugin.FieldPassword, Secret: true, Required: true, VisibleWhen: passwordAuth},
			{Key: credentialField, Label: "Stored password", Type: plugin.FieldCredentialRef, Required: true, VisibleWhen: credentialAuth, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindDBPassword, Protocols: []string{protocolName},
			}, Help: "Reusable Couchbase RBAC credential; its identity supplies the username."},
		}},
		{Name: "TLS", Fields: []plugin.Field{
			{Key: "tls_mode", Label: "TLS mode", Type: plugin.FieldSelect, Required: true, Default: "disable", Options: []plugin.Option{
				{Label: "Disable", Value: "disable"},
				{Label: "Require", Value: "require"},
				{Label: "Verify CA", Value: "verify-ca"},
				{Label: "Verify full", Value: "verify-full"},
			}, Help: "TLS uses the secure ports (18091 / 18093)."},
			{Key: "ca_certificate", Label: "CA certificate", Type: plugin.FieldTextarea, Secret: true, VisibleWhen: verifiedTLS},
		}},
		{Name: "Safety", Fields: []plugin.Field{
			{Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true, Help: "Blocks bucket, scope, collection, document, and index mutations, and every mutating SQL++ statement."},
			{Key: "require_write_confirmation", Label: "Confirm write statements", Type: plugin.FieldToggle, Default: true, Help: "The SQL++ editor asks for confirmation before running a mutating or administrative statement."},
			{Key: "scan_consistency", Label: "Query scan consistency", Type: plugin.FieldSelect, Required: true, Default: "request_plus", Options: []plugin.Option{
				{Label: "Read your own writes (request_plus)", Value: "request_plus"},
				{Label: "Fastest, possibly stale (not_bounded)", Value: "not_bounded"},
			}, Help: "request_plus waits for the index to catch up with recent mutations before answering a SQL++ read."},
			{Key: "timeout", Label: "Request timeout", Type: plugin.FieldDuration, Default: defaultTimeout.String()},
			{Key: "page_limit", Label: "Page limit", Type: plugin.FieldNumber, Default: defaultPageLimit, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: plugin.MaxPageLimit}}},
			{Key: "redact_fields", Label: "Redacted fields", Type: plugin.FieldTextarea, Help: "Comma or newline separated regular expressions for document fields and result columns that must be masked."},
		}},
	}}
}

func unwrap(sess plugin.Session) (*Session, error) {
	if s, ok := sess.(*Session); ok {
		return s, nil
	}
	// rc.Session is the core's borrowed handle, which exposes the live session.
	if h, ok := sess.(interface{ Session() plugin.Session }); ok {
		if s, ok := h.Session().(*Session); ok {
			return s, nil
		}
	}
	return nil, fmt.Errorf("%w: session is not a Couchbase session", plugin.ErrInvalidInput)
}

func session(rc *plugin.RequestContext) (*Session, error) { return unwrap(rc.Session) }

func icon(name string) plugin.Icon { return plugin.Icon{Type: plugin.IconLucide, Value: name} }

func rid(name string) string { return protocolName + "." + name }
