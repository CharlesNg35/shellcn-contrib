// Package prometheus implements the Prometheus protocol plugin.
package prometheus

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn-contrib/shared/dbcred"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	protocolName          = "prometheus"
	defaultTimeout        = 15 * time.Second
	defaultPageLimit      = 100
	defaultInterval       = 5 * time.Second
	basicCredentialField  = "basic_credential_id"
	bearerCredentialField = "bearer_credential_id"
)

const iconSVG = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 128 128"><path d="M63.66 2.477c33.477.007 60.957 27.296 60.914 60.5-.043 33.703-27.41 60.617-61.613 60.593-33.441-.023-60.477-27.343-60.453-61.086C2.53 29.488 30.066 2.47 63.66 2.477zm-18.504 21.25c.766 3.777.024 7.3-1.113 10.765-.785 2.399-1.871 4.711-2.52 7.145-1.07 4.008-2.28 8.039-2.726 12.136-.64 5.895 1.676 11.086 5.64 16.25l-18.222-3.835c.031.574 0 .792.062.976 1.727 5.074 4.766 9.348 8.172 13.379.36.426 1.18.644 1.79.644 18.167.036 36.335.032 54.503.008.563 0 1.317-.105 1.66-.468 3.895-4.094 6.871-8.758 8.735-14.63l-19.29 3.778c1.274-2.496 2.723-4.688 3.56-7.098 2.855-8.242 1.671-16.21-2.427-23.726-3.289-6.031-6.324-12.035-4.683-19.305-3.473 3.434-4.809 7.8-5.656 12.3-.832 4.434-1.325 8.93-1.97 13.43-.093-.136-.21-.238-.23-.355a13.317 13.317 0 01-.168-1.422c-.394-7.367-1.832-14.465-4.87-21.246-1.786-3.988-3.758-8.07-1.915-12.832-1.246.66-2.375 1.313-3.183 2.246-2.41 2.785-3.407 6.13-3.664 9.793-.22 3.13-.52 6.274-1.102 9.352-.61 3.234-1.574 6.402-3.75 9.375-.875-6.348-.973-12.63-6.633-16.66zM92 86.75H35.016v9.898H92zm-45.684 15.016c-.046 8.242 8.348 14.382 18.723 13.937 8.602-.371 16.211-7.137 15.559-13.937zm0 0" fill="#e75225"/></svg>`

type Plugin struct{}

type Options struct {
	Endpoint     string
	Auth         authHeader
	TLSConfig    *tls.Config
	Timeout      time.Duration
	PageLimit    int
	PollInterval time.Duration
	AdminAPI     bool
	LifecycleAPI bool
}

type Session struct {
	client *client
	opts   Options
}

type row map[string]any

type authHeader struct {
	Header string
	Value  string
}

func New() plugin.Plugin { return Plugin{} }

func (Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:          plugin.CurrentAPIVersion,
		Name:                protocolName,
		Version:             "0.1.0",
		Title:               "Prometheus",
		Description:         "Prometheus cockpit with PromQL query, targets, alerts, rules, labels, metric metadata, series, status, live overview metrics, and gated admin operations.",
		Icon:                plugin.Icon{Type: plugin.IconSVG, Value: iconSVG},
		Category:            plugin.CategoryObservability,
		Config:              configSchema(),
		Capabilities:        []plugin.Capability{"metrics", "promql", "targets", "alerts", "rules", "status"},
		SupportedTransports: []plugin.Transport{plugin.TransportDirect},
		Layout:              plugin.LayoutSidebarTree,
		Tree:                tree(),
		Resources:           resources(),
		Actions:             actions(),
		Streams: []plugin.Stream{
			{ID: rid("metrics.live"), Kind: plugin.StreamMetrics, RouteID: rid("metrics.live")},
			{ID: rid("query"), Kind: plugin.StreamLogs, RouteID: rid("query")},
		},
	}
}

func (Plugin) Routes() []plugin.Route { return Routes() }

func (Plugin) Connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	opts, err := parseOptions(cfg)
	if err != nil {
		return nil, err
	}
	c := newClient(opts, cfg.Net.DialContext)
	s := &Session{client: c, opts: opts}
	return s, s.HealthCheck(ctx)
}

func (s *Session) HealthCheck(ctx context.Context) error {
	var out map[string]any
	return s.client.api(ctx, http.MethodGet, "/api/v1/status/buildinfo", nil, nil, &out)
}

func (s *Session) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}

func (s *Session) Close() error {
	s.client.http.CloseIdleConnections()
	return nil
}

func parseOptions(cfg plugin.ConnectConfig) (Options, error) {
	rawURL := strings.TrimSpace(cfg.String("endpoint"))
	if rawURL == "" {
		rawURL = "http://localhost:9090"
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return Options{}, fmt.Errorf("%w: endpoint must be an absolute URL", plugin.ErrInvalidInput)
	}
	opts := Options{
		Endpoint:     strings.TrimRight(u.String(), "/"),
		Timeout:      broker.DurationValue(cfg.Config, "timeout", defaultTimeout),
		PageLimit:    broker.IntValue(cfg.Config, "page_limit", defaultPageLimit, 1, plugin.MaxPageLimit),
		PollInterval: broker.DurationValue(cfg.Config, "poll_interval", defaultInterval),
		AdminAPI:     broker.BoolValue(cfg.Config, "admin_api", false),
		LifecycleAPI: broker.BoolValue(cfg.Config, "lifecycle_api", false),
	}
	auth, err := parseAuth(cfg)
	if err != nil {
		return Options{}, err
	}
	opts.Auth = auth
	tlsConfig, err := tlsConfig(tlsOptions{
		Mode:          broker.StringValue(cfg.Config, "tls_mode", "disable"),
		Host:          u.Hostname(),
		CACertificate: cfg.String("ca_certificate"),
	})
	if err != nil {
		return Options{}, err
	}
	opts.TLSConfig = tlsConfig
	return opts, nil
}

func parseAuth(cfg plugin.ConnectConfig) (authHeader, error) {
	switch auth := broker.StringValue(cfg.Config, "auth", "none"); auth {
	case "none":
		return authHeader{}, nil
	case "basic":
		username := strings.TrimSpace(cfg.String("username"))
		if username == "" {
			return authHeader{}, fmt.Errorf("%w: username is required for basic authentication", plugin.ErrInvalidInput)
		}
		return basicAuth(username, cfg.String("password")), nil
	case "bearer":
		token := cfg.String("bearer_token")
		if token == "" {
			return authHeader{}, fmt.Errorf("%w: bearer token is required", plugin.ErrInvalidInput)
		}
		return authHeader{Header: "Authorization", Value: "Bearer " + token}, nil
	case "stored_basic":
		username := dbcred.ResolvedIdentity(cfg, basicCredentialField)
		if username == "" {
			return authHeader{}, fmt.Errorf("%w: Prometheus basic credentials require a username", plugin.ErrInvalidInput)
		}
		return basicAuth(username, dbcred.ResolvedSecret(cfg, basicCredentialField)), nil
	case "stored_bearer":
		token := dbcred.ResolvedSecret(cfg, bearerCredentialField)
		if token == "" {
			return authHeader{}, fmt.Errorf("%w: Prometheus bearer credentials require a token", plugin.ErrInvalidInput)
		}
		return authHeader{Header: "Authorization", Value: "Bearer " + token}, nil
	default:
		return authHeader{}, fmt.Errorf("%w: unsupported authentication mode %q", plugin.ErrInvalidInput, auth)
	}
}

func basicAuth(username, password string) authHeader {
	raw := username + ":" + password
	return authHeader{Header: "Authorization", Value: "Basic " + base64.StdEncoding.EncodeToString([]byte(raw))}
}

func configSchema() plugin.Schema {
	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Server", Fields: []plugin.Field{
			{Key: "endpoint", Label: "Endpoint", Type: plugin.FieldText, Required: true, Default: "http://localhost:9090", Placeholder: "https://prometheus.example.internal"},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: "none", Options: []plugin.Option{
				{Label: "None", Value: "none"},
				{Label: "Basic auth", Value: "basic"},
				{Label: "Bearer token", Value: "bearer"},
				{Label: "Stored basic auth", Value: "stored_basic"},
				{Label: "Stored bearer token", Value: "stored_bearer"},
			}},
			{Key: "username", Label: "Username", Type: plugin.FieldText, Required: true, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "basic"}}}},
			{Key: "password", Label: "Password", Type: plugin.FieldPassword, Secret: true, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "basic"}}}},
			{Key: "bearer_token", Label: "Bearer token", Type: plugin.FieldPassword, Required: true, Secret: true, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "bearer"}}}},
			{Key: basicCredentialField, Label: "Stored basic auth", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialBasicAuth, Protocols: []string{protocolName},
			}, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "stored_basic"}}}},
			{Key: bearerCredentialField, Label: "Stored bearer token", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialBearerToken, Protocols: []string{protocolName},
			}, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "stored_bearer"}}}},
		}},
		{Name: "TLS", Fields: []plugin.Field{
			{Key: "tls_mode", Label: "TLS mode", Type: plugin.FieldSelect, Required: true, Default: "disable", Options: []plugin.Option{
				{Label: "Disable", Value: "disable"},
				{Label: "Require", Value: "require"},
				{Label: "Verify CA", Value: "verify-ca"},
				{Label: "Verify full", Value: "verify-full"},
			}},
			{Key: "ca_certificate", Label: "CA certificate", Type: plugin.FieldTextarea, Secret: true, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "tls_mode", Op: plugin.OpIn, Value: []any{"verify-ca", "verify-full"}}}}},
		}},
		{Name: "Prometheus", Fields: []plugin.Field{
			{Key: "timeout", Label: "Request timeout", Type: plugin.FieldDuration, Default: "15s"},
			{Key: "poll_interval", Label: "Live metrics interval", Type: plugin.FieldDuration, Default: "5s"},
			{Key: "page_limit", Label: "Page limit", Type: plugin.FieldNumber, Default: defaultPageLimit, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: plugin.MaxPageLimit}}},
			{Key: "admin_api", Label: "Admin API enabled", Type: plugin.FieldToggle, Help: "Enable actions that require Prometheus --web.enable-admin-api."},
			{Key: "lifecycle_api", Label: "Lifecycle API enabled", Type: plugin.FieldToggle, Help: "Enable reload action for Prometheus servers started with --web.enable-lifecycle."},
		}},
	}}
}

func unwrap(sess plugin.Session) (*Session, error) {
	if s, ok := sess.(*Session); ok {
		return s, nil
	}
	// rc.Session is the core's borrowed Handle, which exposes the live session.
	if h, ok := sess.(interface{ Session() plugin.Session }); ok {
		if s, ok := h.Session().(*Session); ok {
			return s, nil
		}
	}
	return nil, plugin.ErrInvalidInput
}

type tlsOptions struct {
	Mode          string
	Host          string
	CACertificate string
}

func tlsConfig(opts tlsOptions) (*tls.Config, error) {
	mode := strings.TrimSpace(opts.Mode)
	if mode == "" || mode == "disable" {
		return nil, nil
	}
	cfg := &tls.Config{ServerName: opts.Host, MinVersion: tls.VersionTLS12}
	switch mode {
	case "require":
		cfg.InsecureSkipVerify = true
	case "verify-ca", "verify-full":
		if opts.CACertificate != "" {
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM([]byte(opts.CACertificate)) {
				return nil, fmt.Errorf("%w: invalid CA certificate", plugin.ErrInvalidInput)
			}
			cfg.RootCAs = pool
		}
		if mode == "verify-ca" {
			cfg.InsecureSkipVerify = true
			cfg.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
				roots := cfg.RootCAs
				if roots == nil {
					roots, _ = x509.SystemCertPool()
				}
				cert, err := x509.ParseCertificate(rawCerts[0])
				if err != nil {
					return err
				}
				_, err = cert.Verify(x509.VerifyOptions{Roots: roots, CurrentTime: time.Now()})
				return err
			}
		}
	default:
		return nil, fmt.Errorf("%w: unsupported TLS mode %q", plugin.ErrInvalidInput, mode)
	}
	return cfg, nil
}

func newClient(opts Options, dialer func(context.Context, string, string) (net.Conn, error)) *client {
	transport := &http.Transport{TLSClientConfig: opts.TLSConfig}
	if dialer != nil {
		transport.DialContext = dialer
	}
	return &client{
		http:     &http.Client{Transport: transport, Timeout: opts.Timeout},
		endpoint: opts.Endpoint,
		auth:     opts.Auth,
	}
}
