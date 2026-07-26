// Package couchdb implements the ShellCN CouchDB protocol plugin.
package couchdb

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn-contrib/shared/dbcred"
	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugin/webproxy"
)

const (
	protocolName    = "couchdb"
	credentialField = "credential_id"

	defaultHost      = "127.0.0.1"
	defaultPort      = 5984
	defaultTimeout   = 30 * time.Second
	defaultPageLimit = 100
)

type row = plugin.TableRow

// Plugin is the stateless CouchDB plugin singleton; every per-connection value
// lives on Session.
type Plugin struct{}

func New() plugin.Plugin { return Plugin{} }

// options is the validated runtime view of a connection's config. Schema
// defaults are UI hints; every fallback is applied here.
type options struct {
	scheme    string
	host      string
	port      int
	authMode  string
	username  string
	password  string
	tls       *tls.Config
	timeout   time.Duration
	pageLimit int
	readOnly  bool
}

func (o options) addr() string { return net.JoinHostPort(o.host, strconv.Itoa(o.port)) }

func (o options) baseURL() string { return o.scheme + "://" + o.addr() }

// Session is one authenticated CouchDB connection.
type Session struct {
	client *client
	opts   options
	net    plugin.NetTransport

	mu        sync.Mutex
	localBase string
}

// selfEndpoint is the URL CouchDB can use to reach itself. Replication documents
// are executed by the node, not by the gateway, so a local endpoint must be the
// node's own loopback address and port rather than the address ShellCN dialled.
// It is read once from the node configuration and cached for the session.
func (s *Session) selfEndpoint(rc *plugin.RequestContext) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.localBase != "" {
		return s.localBase
	}
	s.localBase = s.opts.baseURL()
	var chttpd map[string]string
	if err := s.client.do(rc.Ctx, http.MethodGet, "/_node/_local/_config/chttpd", nil, nil, &chttpd); err == nil {
		if port := strings.TrimSpace(chttpd["port"]); port != "" && port != "0" {
			s.localBase = "http://127.0.0.1:" + port
		}
	}
	return s.localBase
}

func (Plugin) Routes() []plugin.Route { return routes() }

func (Plugin) Connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	opts, err := parseOptions(cfg)
	if err != nil {
		return nil, err
	}
	c, err := newClient(cfg.Net, opts)
	if err != nil {
		return nil, err
	}
	s := &Session{client: c, opts: opts, net: cfg.Net}
	if err := s.HealthCheck(ctx); err != nil {
		c.close()
		return nil, err
	}
	return s, nil
}

// HealthCheck uses /_up, the endpoint CouchDB documents for readiness probes.
func (s *Session) HealthCheck(ctx context.Context) error {
	var out row
	if err := s.client.do(ctx, http.MethodGet, "/_up", nil, nil, &out); err != nil {
		// Admin-party and pre-3.x servers may not expose /_up; the welcome
		// document is the universal fallback.
		var welcome row
		if fallback := s.client.do(ctx, http.MethodGet, "/", nil, nil, &welcome); fallback != nil {
			return err
		}
	}
	return nil
}

func (s *Session) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}

func (s *Session) Close() error {
	s.client.close()
	return nil
}

// ServeHTTPProxy exposes CouchDB's own Fauxton UI through the gateway mount, so
// the embedded web panel and "open in browser" reuse the audited egress path.
func (s *Session) ServeHTTPProxy(w http.ResponseWriter, r *http.Request) {
	base, err := url.Parse(s.opts.baseURL())
	if err != nil {
		http.Error(w, "no upstream", http.StatusBadGateway)
		return
	}
	webproxy.Serve(w, r, webproxy.Options{
		Base:         base,
		Transport:    s.proxyTransport(),
		UpstreamPath: r.URL.Path,
		PublicPrefix: plugin.RequestProxyPrefix(r),
	})
}

func (s *Session) proxyTransport() http.RoundTripper {
	if _, rt, ok := s.net.HTTP(); ok && rt != nil {
		return rt
	}
	return &http.Transport{DialContext: s.net.DialContext, TLSClientConfig: s.opts.tls}
}

var _ plugin.HTTPProxy = (*Session)(nil)

func parseOptions(cfg plugin.ConnectConfig) (options, error) {
	opts := options{
		scheme:    "http",
		host:      strings.TrimSpace(cfg.String("host")),
		port:      defaultPort,
		authMode:  broker.StringValue(cfg.Config, "auth", "basic"),
		timeout:   broker.DurationValue(cfg.Config, "timeout", defaultTimeout),
		pageLimit: broker.IntValue(cfg.Config, "page_limit", defaultPageLimit, 1, plugin.MaxPageLimit),
		readOnly:  broker.BoolValue(cfg.Config, "read_only", true),
	}
	if opts.host == "" {
		opts.host = defaultHost
	}
	if port, ok := cfg.Int("port"); ok && port > 0 && port <= 65535 {
		opts.port = port
	}

	tlsMode := broker.StringValue(cfg.Config, "tls_mode", "disable")
	tlsConfig, err := sqldb.TLSConfig(sqldb.TLSOptions{
		Mode:          tlsMode,
		Host:          opts.host,
		CACertificate: cfg.String("ca_certificate"),
	})
	if err != nil {
		return options{}, err
	}
	opts.tls = tlsConfig
	if tlsMode != "" && tlsMode != "disable" {
		opts.scheme = "https"
	}

	switch opts.authMode {
	case "none":
	case "basic", "session":
		opts.username = strings.TrimSpace(cfg.String("username"))
		opts.password = cfg.String("password")
	case "credential", "credential_session":
		if kind := cfg.CredentialKindFor(credentialField); kind != "" && kind != plugin.CredentialKindDBPassword {
			return options{}, fmt.Errorf("%w: CouchDB stored credentials must be database passwords", plugin.ErrInvalidInput)
		}
		opts.username = dbcred.ResolvedIdentity(cfg, credentialField)
		opts.password = dbcred.ResolvedSecret(cfg, credentialField)
		if opts.username == "" || opts.password == "" {
			return options{}, fmt.Errorf("%w: the selected credential has no username/password pair", plugin.ErrInvalidInput)
		}
		opts.authMode = "basic"
		if broker.StringValue(cfg.Config, "auth", "") == "credential_session" {
			opts.authMode = "session"
		}
	default:
		return options{}, fmt.Errorf("%w: unsupported authentication mode %q", plugin.ErrInvalidInput, opts.authMode)
	}
	if opts.authMode != "none" && opts.username == "" {
		return options{}, fmt.Errorf("%w: a username is required for %s authentication", plugin.ErrInvalidInput, opts.authMode)
	}
	return opts, nil
}

func directOnly() *plugin.Condition {
	return &plugin.Condition{AllOf: []plugin.Rule{
		{Field: plugin.SchemaContextTransport, Op: plugin.OpEq, Value: string(plugin.TransportDirect)},
	}}
}

func visibleWhenAuth(values ...any) *plugin.Condition {
	return &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpIn, Value: values}}}
}

func configSchema() plugin.Schema {
	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Server", Fields: []plugin.Field{
			{
				Key: "host", Label: "Host", Type: plugin.FieldText, Required: true, Default: defaultHost,
				Help: "CouchDB node address. Supplied by the agent when the connection is tunnelled.", VisibleWhen: directOnly(),
			},
			{
				Key: "port", Label: "Port", Type: plugin.FieldNumber, Default: defaultPort, VisibleWhen: directOnly(),
				Validators: []plugin.Validator{
					{Type: plugin.ValidatorMin, Value: 1},
					{Type: plugin.ValidatorMax, Value: 65535},
				},
			},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{
				Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: "basic",
				Options: []plugin.Option{
					{Label: "Basic auth", Value: "basic"},
					{Label: "Cookie session", Value: "session"},
					{Label: "Stored credential (basic)", Value: "credential"},
					{Label: "Stored credential (cookie session)", Value: "credential_session"},
					{Label: "None (admin party)", Value: "none"},
				},
				Help: "Cookie session exchanges the password once at /_session and reuses the AuthSession cookie.",
			},
			{
				Key: "username", Label: "Username", Type: plugin.FieldText, Default: "admin",
				VisibleWhen: visibleWhenAuth("basic", "session"),
			},
			{
				Key: "password", Label: "Password", Type: plugin.FieldPassword, Secret: true,
				VisibleWhen: visibleWhenAuth("basic", "session"),
				Help:        "Inline password. Prefer a reusable credential when one exists.",
			},
			{
				Key: credentialField, Label: "Stored credential", Type: plugin.FieldCredentialRef, Required: true,
				VisibleWhen: visibleWhenAuth("credential", "credential_session"),
				Credential: &plugin.CredentialSelector{
					Kind:      plugin.CredentialKindDBPassword,
					Protocols: []string{protocolName},
				},
			},
		}},
		{Name: "TLS", Fields: []plugin.Field{
			{
				Key: "tls_mode", Label: "TLS mode", Type: plugin.FieldSelect, Required: true, Default: "disable",
				Options: []plugin.Option{
					{Label: "Disable (http)", Value: "disable"},
					{Label: "Require (https, skip verification)", Value: "require"},
					{Label: "Verify CA", Value: "verify-ca"},
					{Label: "Verify full", Value: "verify-full"},
				},
			},
			{
				Key: "ca_certificate", Label: "CA certificate", Type: plugin.FieldTextarea, Secret: true,
				VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{
					{Field: "tls_mode", Op: plugin.OpIn, Value: []any{"verify-ca", "verify-full"}},
				}},
			},
		}},
		{Name: "Safety", Fields: []plugin.Field{
			{
				Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true,
				Help: "Blocks every document, database, index, design-document and replication mutation.",
			},
			{Key: "timeout", Label: "Request timeout", Type: plugin.FieldDuration, Default: defaultTimeout.String()},
			{
				Key: "page_limit", Label: "Page limit", Type: plugin.FieldNumber, Default: defaultPageLimit,
				Help: "Upper bound on rows fetched per document, view and Mango page.",
				Validators: []plugin.Validator{
					{Type: plugin.ValidatorMin, Value: 1},
					{Type: plugin.ValidatorMax, Value: plugin.MaxPageLimit},
				},
			},
		}},
	}}
}

func session(rc *plugin.RequestContext) (*Session, error) {
	if s, ok := rc.Session.(*Session); ok {
		return s, nil
	}
	if h, ok := rc.Session.(interface{ Session() plugin.Session }); ok {
		if s, ok := h.Session().(*Session); ok {
			return s, nil
		}
	}
	return nil, fmt.Errorf("%w: no CouchDB session on this request", plugin.ErrUnavailable)
}
