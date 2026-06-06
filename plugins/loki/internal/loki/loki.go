// Package loki implements the Grafana Loki protocol plugin.
package loki

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn-contrib/shared/dbcred"
	"github.com/charlesng35/shellcn-contrib/shared/searchrest"
	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	protocolName     = "loki"
	defaultTimeout   = 15 * time.Second
	defaultPageLimit = 100
	credentialField  = "credential_id"
)

const iconSVG = `<svg xmlns="http://www.w3.org/2000/svg" xml:space="preserve" viewBox="0 0 512 512"><linearGradient id="a" x1="485.057" x2="485.057" y1="-705.376" y2="-74.565" gradientTransform="scale(1 -1)rotate(8.748 -271.56 -2903.464)" gradientUnits="userSpaceOnUse"><stop offset="0" style="stop-color:#faed1e"/><stop offset="1" style="stop-color:#f15b2b"/></linearGradient><path d="m139.6 464.9-40.8 6.3 6.3 40.8 40.8-6.3z" style="fill:url(#a)"/><linearGradient id="b" x1="749.438" x2="749.438" y1="-705.376" y2="-74.565" gradientTransform="scale(1 -1)rotate(8.748 -271.56 -2903.464)" gradientUnits="userSpaceOnUse"><stop offset="0" style="stop-color:#faed1e"/><stop offset="1" style="stop-color:#f15b2b"/></linearGradient><path d="M286.9 418.6 467 390.9l-6.3-40.8-180.1 27.7z" style="fill:url(#b)"/><linearGradient id="c" x1="614.329" x2="614.329" y1="-705.376" y2="-74.565" gradientTransform="scale(1 -1)rotate(8.748 -271.56 -2903.464)" gradientUnits="userSpaceOnUse"><stop offset="0" style="stop-color:#faed1e"/><stop offset="1" style="stop-color:#f15b2b"/></linearGradient><path d="m216.7 387.6 6.3 40.8 40.8-6.2-6.3-40.9z" style="fill:url(#c)"/><linearGradient id="d" x1="549.698" x2="549.698" y1="-705.376" y2="-74.565" gradientTransform="scale(1 -1)rotate(8.748 -271.56 -2903.464)" gradientUnits="userSpaceOnUse"><stop offset="0" style="stop-color:#faed1e"/><stop offset="1" style="stop-color:#f15b2b"/></linearGradient><path d="m209.8 495.9-6.3-40.8-40.8 6.3 6.2 40.8z" style="fill:url(#d)"/><linearGradient id="e" x1="485.065" x2="485.065" y1="-705.376" y2="-74.565" gradientTransform="scale(1 -1)rotate(8.748 -271.56 -2903.464)" gradientUnits="userSpaceOnUse"><stop offset="0" style="stop-color:#faed1e"/><stop offset="1" style="stop-color:#f15b2b"/></linearGradient><path d="m95.2 448.1 40.8-6.3-6.2-40.8-40.8 6.3z" style="fill:url(#e)"/><linearGradient id="f" x1="749.433" x2="749.433" y1="-705.376" y2="-74.565" gradientTransform="scale(1 -1)rotate(8.748 -271.56 -2903.464)" gradientUnits="userSpaceOnUse"><stop offset="0" style="stop-color:#faed1e"/><stop offset="1" style="stop-color:#f15b2b"/></linearGradient><path d="m470.6 414-180.2 27.7 6.3 40.8 180.1-27.7z" style="fill:url(#f)"/><linearGradient id="g" x1="614.338" x2="614.338" y1="-705.376" y2="-74.565" gradientTransform="scale(1 -1)rotate(8.748 -271.56 -2903.464)" gradientUnits="userSpaceOnUse"><stop offset="0" style="stop-color:#faed1e"/><stop offset="1" style="stop-color:#f15b2b"/></linearGradient><path d="m226.6 451.5 6.2 40.8 40.8-6.2-6.2-40.9z" style="fill:url(#g)"/><linearGradient id="h" x1="549.702" x2="549.702" y1="-705.376" y2="-74.565" gradientTransform="scale(1 -1)rotate(8.748 -271.56 -2903.464)" gradientUnits="userSpaceOnUse"><stop offset="0" style="stop-color:#faed1e"/><stop offset="1" style="stop-color:#f15b2b"/></linearGradient><path d="m159.1 438.3 40.8-6.3-6.2-40.8-40.9 6.3z" style="fill:url(#h)"/><linearGradient id="i" x1="473.159" x2="473.159" y1="-693.123" y2="-94.853" gradientTransform="scale(1 -1)rotate(8.748 -271.56 -2903.464)" gradientUnits="userSpaceOnUse"><stop offset="0" style="stop-color:#faed1e"/><stop offset="1" style="stop-color:#f15b2b"/></linearGradient><path d="m85.4 384.2 17.2-2.6L52.4 55l-17.2 2.7z" style="fill:url(#i)"/><linearGradient id="j" x1="497.114" x2="497.114" y1="-709.717" y2="-67.381" gradientTransform="scale(1 -1)rotate(8.748 -271.56 -2903.464)" gradientUnits="userSpaceOnUse"><stop offset="0" style="stop-color:#faed1e"/><stop offset="1" style="stop-color:#f15b2b"/></linearGradient><path d="m109.1 380.6 17.2-2.7L72.4 27.3 55.2 30z" style="fill:url(#j)"/><linearGradient id="k" x1="538.062" x2="538.062" y1="-724.295" y2="-43.24" gradientTransform="scale(1 -1)rotate(8.748 -271.56 -2903.464)" gradientUnits="userSpaceOnUse"><stop offset="0" style="stop-color:#faed1e"/><stop offset="1" style="stop-color:#f15b2b"/></linearGradient><path d="m149.6 374.3 17.2-2.6L109.6 0 92.4 2.7z" style="fill:url(#k)"/><linearGradient id="l" x1="562.005" x2="562.005" y1="-701.646" y2="-80.743" gradientTransform="scale(1 -1)rotate(8.748 -271.56 -2903.464)" gradientUnits="userSpaceOnUse"><stop offset="0" style="stop-color:#faed1e"/><stop offset="1" style="stop-color:#f15b2b"/></linearGradient><path d="m173.2 370.7 17.3-2.7-52.2-338.8-17.2 2.6z" style="fill:url(#l)"/><linearGradient id="m" x1="602.42" x2="602.42" y1="-675.539" y2="-123.976" gradientTransform="scale(1 -1)rotate(8.748 -271.56 -2903.464)" gradientUnits="userSpaceOnUse"><stop offset="0" style="stop-color:#faed1e"/><stop offset="1" style="stop-color:#f15b2b"/></linearGradient><path d="m213.2 364.6 17.2-2.7-46.3-301-17.2 2.6z" style="fill:url(#m)"/><linearGradient id="n" x1="626.396" x2="626.396" y1="-683.496" y2="-110.797" gradientTransform="scale(1 -1)rotate(8.748 -271.56 -2903.464)" gradientUnits="userSpaceOnUse"><stop offset="0" style="stop-color:#faed1e"/><stop offset="1" style="stop-color:#f15b2b"/></linearGradient><path d="m236.9 360.9 17.2-2.6L206 45.7l-17.2 2.6z" style="fill:url(#n)"/></svg>`

type Plugin struct{}

type Options struct {
	Endpoint  string
	Auth      searchrest.Auth
	TLSConfig *tls.Config
	Timeout   time.Duration
	PageLimit int
	TenantID  string
	ReadOnly  bool
}

type Session struct {
	client *searchrest.Client
	opts   Options
}

type row map[string]any

func New() plugin.Plugin { return Plugin{} }

func (Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:          plugin.CurrentAPIVersion,
		Name:                protocolName,
		Version:             "0.1.0",
		Title:               "Grafana Loki",
		Description:         "Loki log cockpit with labels, label values, streams, LogQL range queries, and build/status details.",
		Icon:                plugin.Icon{Type: plugin.IconSVG, Value: iconSVG},
		Category:            plugin.CategoryObservability,
		Config:              configSchema(),
		Capabilities:        []plugin.Capability{"logs", "logql", "labels", "streams"},
		SupportedTransports: []plugin.Transport{plugin.TransportDirect},
		Layout:              plugin.LayoutSidebarTree,
		Tree:                tree(),
		Resources:           resources(),
		Actions:             actions(),
		Streams:             []plugin.Stream{{ID: rid("query"), Kind: plugin.StreamLogs, RouteID: rid("query")}},
	}
}

func (Plugin) Routes() []plugin.Route { return Routes() }

func (Plugin) Connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	opts, err := parseOptions(cfg)
	if err != nil {
		return nil, err
	}
	s := &Session{
		client: searchrest.New(searchrest.Options{
			Endpoint:  opts.Endpoint,
			Auth:      opts.Auth,
			TLSConfig: opts.TLSConfig,
			Timeout:   opts.Timeout,
			Dialer:    cfg.Net.DialContext,
			Headers:   tenantHeaders(opts.TenantID),
		}),
		opts: opts,
	}
	return s, s.HealthCheck(ctx)
}

func tenantHeaders(tenantID string) map[string]string {
	if strings.TrimSpace(tenantID) == "" {
		return nil
	}
	return map[string]string{"X-Scope-OrgID": tenantID}
}

func (s *Session) HealthCheck(ctx context.Context) error {
	_, err := s.client.Raw(ctx, "GET", "/ready", nil, "", nil)
	return err
}

func (s *Session) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}

func (s *Session) Close() error {
	s.client.Close()
	return nil
}

func parseOptions(cfg plugin.ConnectConfig) (Options, error) {
	rawURL := strings.TrimSpace(cfg.String("endpoint"))
	if rawURL == "" {
		rawURL = "http://localhost:3100"
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return Options{}, fmt.Errorf("%w: endpoint must be an absolute URL", plugin.ErrInvalidInput)
	}
	opts := Options{
		Endpoint:  strings.TrimRight(u.String(), "/"),
		Timeout:   broker.DurationValue(cfg.Config, "timeout", defaultTimeout),
		PageLimit: broker.IntValue(cfg.Config, "page_limit", defaultPageLimit, 1, plugin.MaxPageLimit),
		TenantID:  strings.TrimSpace(cfg.String("tenant_id")),
		ReadOnly:  broker.BoolValue(cfg.Config, "read_only", true),
	}
	auth, err := parseAuth(cfg)
	if err != nil {
		return Options{}, err
	}
	opts.Auth = auth
	tlsConfig, err := sqldb.TLSConfig(sqldb.TLSOptions{
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

func parseAuth(cfg plugin.ConnectConfig) (searchrest.Auth, error) {
	switch auth := broker.StringValue(cfg.Config, "auth", "none"); auth {
	case "none":
		return searchrest.Auth{}, nil
	case "basic":
		username := strings.TrimSpace(cfg.String("username"))
		if username == "" {
			return searchrest.Auth{}, fmt.Errorf("%w: username is required for basic authentication", plugin.ErrInvalidInput)
		}
		return basicAuth(username, cfg.String("password")), nil
	case "bearer":
		token := cfg.String("bearer_token")
		if token == "" {
			return searchrest.Auth{}, fmt.Errorf("%w: bearer token is required", plugin.ErrInvalidInput)
		}
		return searchrest.Auth{Header: "Authorization", Value: "Bearer " + token}, nil
	case "credential":
		switch kind := cfg.CredentialKindFor(plugin.CredentialField); kind {
		case plugin.CredentialBasicAuth:
			username := cfg.CredentialIdentityFor(plugin.CredentialField)
			if username == "" {
				return searchrest.Auth{}, fmt.Errorf("%w: Loki basic credentials require a username", plugin.ErrInvalidInput)
			}
			return basicAuth(username, cfg.CredentialSecretFor(plugin.CredentialField)), nil
		case plugin.CredentialBearerToken, plugin.CredentialAPIToken:
			token := dbcred.ResolvedSecret(cfg, plugin.CredentialField)
			if token == "" {
				return searchrest.Auth{}, fmt.Errorf("%w: Loki token credentials require a token", plugin.ErrInvalidInput)
			}
			return searchrest.Auth{Header: "Authorization", Value: "Bearer " + token}, nil
		case "":
			return searchrest.Auth{}, nil
		default:
			return searchrest.Auth{}, fmt.Errorf("%w: Loki stored credentials must be basic auth or token credentials", plugin.ErrInvalidInput)
		}
	default:
		return searchrest.Auth{}, fmt.Errorf("%w: unsupported authentication mode %q", plugin.ErrInvalidInput, auth)
	}
}

func basicAuth(username, password string) searchrest.Auth {
	raw := username + ":" + password
	return searchrest.Auth{Header: "Authorization", Value: "Basic " + base64.StdEncoding.EncodeToString([]byte(raw))}
}

func configSchema() plugin.Schema {
	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Server", Fields: []plugin.Field{
			{Key: "endpoint", Label: "Endpoint", Type: plugin.FieldText, Required: true, Default: "http://localhost:3100", Placeholder: "https://loki.example.internal"},
			{Key: "tenant_id", Label: "Tenant ID", Type: plugin.FieldText, Help: "Optional X-Scope-OrgID value for multi-tenant Loki deployments."},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: "none", Options: []plugin.Option{
				{Label: "None", Value: "none"},
				{Label: "Basic auth", Value: "basic"},
				{Label: "Bearer token", Value: "bearer"},
				{Label: "Stored credential", Value: "credential"},
			}},
			{Key: "username", Label: "Username", Type: plugin.FieldText, Required: true, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "basic"}}}},
			{Key: "password", Label: "Password", Type: plugin.FieldPassword, Secret: true, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "basic"}}}},
			{Key: "bearer_token", Label: "Bearer token", Type: plugin.FieldPassword, Required: true, Secret: true, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "bearer"}}}},
			{Key: credentialField, Label: "Stored credential", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kinds: []plugin.CredentialKind{plugin.CredentialBasicAuth, plugin.CredentialBearerToken, plugin.CredentialAPIToken}, Protocols: []string{protocolName},
			}, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "credential"}}}},
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
		{Name: "Safety", Fields: []plugin.Field{
			{Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true, Help: "Blocks log deletion requests."},
			{Key: "timeout", Label: "Request timeout", Type: plugin.FieldDuration, Default: "15s"},
			{Key: "page_limit", Label: "Page limit", Type: plugin.FieldNumber, Default: defaultPageLimit, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: plugin.MaxPageLimit}}},
		}},
	}}
}

func unwrap(sess plugin.Session) (*Session, error) {
	if s, ok := sess.(*Session); ok {
		return s, nil
	}
	if h, ok := sess.(interface{ Session() plugin.Session }); ok {
		if s, ok := h.Session().(*Session); ok {
			return s, nil
		}
	}
	return nil, plugin.ErrInvalidInput
}
