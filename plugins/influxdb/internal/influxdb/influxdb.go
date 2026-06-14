// Package influxdb implements the InfluxDB protocol plugin.
package influxdb

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
	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	protocolName     = "influxdb"
	defaultTimeout   = 15 * time.Second
	defaultPageLimit = 100
	tokenFieldV3     = "api_token_v3"
	tokenFieldV2     = "api_token_v2"
	tokenCredV3      = "token_credential_v3_id"
	tokenCredV2      = "token_credential_v2_id"
	usernameFieldV3  = "username_v3"
	usernameFieldV1  = "username_v1"
	passwordFieldV3  = "password_v3"
	passwordFieldV1  = "password_v1"
	basicCredV3      = "basic_credential_v3_id"
	basicCredV1      = "basic_credential_v1_id"
)

const (
	modeV3 = "v3"
	modeV2 = "v2"
	modeV1 = "v1"
)

const iconSVG = `<svg xmlns="http://www.w3.org/2000/svg" width="128" height="128" viewBox="0 0 128 128"><path style="fill-rule:evenodd;fill:#f14668;fill-opacity:1" d="m94.543 87.625 29.379-6.75a3.35 3.35 0 0 0 1.258-.543 3.358 3.358 0 0 0 1.383-2.305c.058-.46.019-.925-.114-1.37L113.957 22.34a3.499 3.499 0 0 0-1.59-2.14 3.49 3.49 0 0 0-2.633-.391l-29.37 6.75c-.887.23-1.65.8-2.118 1.593a3.452 3.452 0 0 0-.383 2.625L90.32 85.094a3.499 3.499 0 0 0 1.59 2.14c.79.477 1.738.618 2.633.391Zm-10.125 33.566 35.621-33.054c1.344-1.36 1.004-2.196-.844-1.528l-24.484 5.575a6.222 6.222 0 0 0-2.715 1.46 6.221 6.221 0 0 0-1.676 2.586l-7.425 23.954c-.508 1.855.168 2.363 1.523 1.007Zm-64.992-10.789 53.344 16.52c.91.172 1.851.012 2.656-.45a3.947 3.947 0 0 0 1.734-2.07l8.938-28.68a3.48 3.48 0 0 0 .117-1.378 3.492 3.492 0 0 0-.418-1.317 3.473 3.473 0 0 0-.89-1.058 3.562 3.562 0 0 0-1.227-.633L30.336 74.973a3.545 3.545 0 0 0-2.695.304 3.57 3.57 0 0 0-1.696 2.118l-8.879 28.62a3.556 3.556 0 0 0 .278 2.68 3.547 3.547 0 0 0 2.082 1.707ZM2.2 51.7l10.816 47.452c.336 1.856 1.207 1.856 1.68 0l7.425-23.949a6.709 6.709 0 0 0 .031-3.113 6.783 6.783 0 0 0-1.37-2.793L3.721 50.852c-1.183-1.395-2.066-1.008-1.523.847ZM43.906.973 3.046 38.875a3.47 3.47 0 0 0-.168 4.848l20.43 22.144a3.483 3.483 0 0 0 2.415 1.098 3.48 3.48 0 0 0 2.484-.926l40.848-37.965a3.446 3.446 0 0 0 .172-4.847L48.832 1.094A3.48 3.48 0 0 0 47.722.3a3.467 3.467 0 0 0-1.326-.3 3.419 3.419 0 0 0-1.34.238 3.435 3.435 0 0 0-1.149.735Zm39.496 85.804c1.864.508 3.035-.496 2.54-2.422L74.124 33.082c-.508-1.855-2.035-2.363-3.375-1.02L32.258 67.895c-1.352 1.343-1.016 2.859.836 3.367Zm20.09-71.515L56.898.972c-1.851-.511-2.187.169-.675 1.684l17.054 18.387a6.549 6.549 0 0 0 2.7 1.527 6.58 6.58 0 0 0 3.093.11l24.485-5.563c1.8-.508 1.8-1.355-.063-1.855Zm0 0"/></svg>`

type Plugin struct{}

type Options struct {
	Mode            string
	Endpoint        string
	Auth            authHeader
	TLSConfig       *tls.Config
	Timeout         time.Duration
	PageLimit       int
	Org             string
	DefaultDatabase string
	QueryLanguage   string
	ReadOnly        bool
	ConfirmWrites   bool
	Lookback        string
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

type actionResult struct {
	OK bool `json:"ok"`
}

func New() plugin.Plugin { return Plugin{} }

func (Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:          plugin.CurrentAPIVersion,
		Name:                protocolName,
		Version:             "0.1.0",
		Title:               "InfluxDB",
		Description:         "InfluxDB cockpit for v3, v2, and v1 time-series APIs with database/bucket browsing, measurements, schema, data preview, queries, and line protocol writes.",
		Icon:                plugin.Icon{Type: plugin.IconSVG, Value: iconSVG},
		Category:            plugin.CategoryObservability,
		Config:              configSchema(),
		Capabilities:        []plugin.Capability{"timeseries", "query", "measurements", "line_protocol"},
		SupportedTransports: []plugin.Transport{plugin.TransportDirect},
		Layout:              plugin.LayoutSidebarTree,
		Tree:                tree(),
		Resources:           resources(),
		Actions:             actions(),
		Streams:             []plugin.Stream{{ID: rid("query"), Kind: plugin.StreamLogs, RouteID: rid("query")}},
	}
}

func (Plugin) Routes() []plugin.Route { return routes() }

func (Plugin) Connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	opts, err := parseOptions(cfg)
	if err != nil {
		return nil, err
	}
	s := &Session{
		client: newClient(clientOptions{
			Endpoint:  opts.Endpoint,
			Auth:      opts.Auth,
			TLSConfig: opts.TLSConfig,
			Timeout:   opts.Timeout,
			Dialer:    cfg.Net.DialContext,
		}),
		opts: opts,
	}
	return s, s.HealthCheck(ctx)
}

func (s *Session) HealthCheck(ctx context.Context) error {
	return s.client.health(ctx, s.opts.Mode)
}

func (s *Session) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}

func (s *Session) Close() error {
	s.client.close()
	return nil
}

func parseOptions(cfg plugin.ConnectConfig) (Options, error) {
	mode := broker.StringValue(cfg.Config, "api_mode", modeV3)
	endpoint := strings.TrimSpace(cfg.String("endpoint"))
	if endpoint == "" {
		if mode == modeV3 {
			endpoint = "http://localhost:8181"
		} else {
			endpoint = "http://localhost:8086"
		}
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return Options{}, fmt.Errorf("%w: endpoint must be an absolute URL", plugin.ErrInvalidInput)
	}
	opts := Options{
		Mode:            mode,
		Endpoint:        strings.TrimRight(u.String(), "/"),
		Timeout:         broker.DurationValue(cfg.Config, "timeout", defaultTimeout),
		PageLimit:       broker.IntValue(cfg.Config, "page_limit", defaultPageLimit, 1, plugin.MaxPageLimit),
		Org:             strings.TrimSpace(cfg.String("org")),
		DefaultDatabase: strings.TrimSpace(cfg.String("database")),
		QueryLanguage:   queryLanguage(cfg.Config, mode),
		ReadOnly:        broker.BoolValue(cfg.Config, "read_only", true),
		ConfirmWrites:   broker.BoolValue(cfg.Config, "confirm_writes", true),
		Lookback:        broker.StringValue(cfg.Config, "lookback", "-1h"),
	}
	switch opts.Mode {
	case modeV3, modeV2, modeV1:
	default:
		return Options{}, fmt.Errorf("%w: unsupported InfluxDB API mode %q", plugin.ErrInvalidInput, opts.Mode)
	}
	auth, err := parseAuth(cfg, opts.Mode)
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

func parseAuth(cfg plugin.ConnectConfig, mode string) (authHeader, error) {
	switch mode {
	case modeV3:
		return parseTokenOrBasicAuth(cfg, broker.StringValue(cfg.Config, "auth_v3", "token"), "InfluxDB 3", tokenFieldV3, tokenCredV3, usernameFieldV3, passwordFieldV3, basicCredV3)
	case modeV2:
		switch auth := broker.StringValue(cfg.Config, "auth_v2", "token"); auth {
		case "none":
			return authHeader{}, nil
		case "token":
			token := strings.TrimSpace(cfg.String(tokenFieldV2))
			if token == "" {
				return authHeader{}, fmt.Errorf("%w: API token is required", plugin.ErrInvalidInput)
			}
			return authHeader{Header: "Authorization", Value: "Token " + token}, nil
		case "credential":
			token := resolvedSecretAny(cfg, tokenCredV2)
			if token == "" {
				return authHeader{}, fmt.Errorf("%w: stored token credential is required", plugin.ErrInvalidInput)
			}
			return authHeader{Header: "Authorization", Value: "Token " + token}, nil
		default:
			return authHeader{}, fmt.Errorf("%w: unsupported InfluxDB 2 authentication mode %q", plugin.ErrInvalidInput, auth)
		}
	case modeV1:
		switch auth := broker.StringValue(cfg.Config, "auth_v1", "none"); auth {
		case "none":
			return authHeader{}, nil
		case "basic":
			username := strings.TrimSpace(cfg.String(usernameFieldV1))
			if username == "" {
				return authHeader{}, fmt.Errorf("%w: username is required", plugin.ErrInvalidInput)
			}
			return basicAuth(username, cfg.String(passwordFieldV1)), nil
		case "credential":
			if kind := resolvedKindAny(cfg, basicCredV1); kind != "" && kind != plugin.CredentialBasicAuth {
				return authHeader{}, fmt.Errorf("%w: InfluxDB 1 stored credentials must be basic auth", plugin.ErrInvalidInput)
			}
			username := resolvedIdentityAny(cfg, basicCredV1)
			if username == "" {
				return authHeader{}, fmt.Errorf("%w: basic auth credential identity is required", plugin.ErrInvalidInput)
			}
			return basicAuth(username, resolvedSecretAny(cfg, basicCredV1)), nil
		default:
			return authHeader{}, fmt.Errorf("%w: unsupported InfluxDB 1 authentication mode %q", plugin.ErrInvalidInput, auth)
		}
	default:
		return authHeader{}, fmt.Errorf("%w: unsupported InfluxDB API mode %q", plugin.ErrInvalidInput, mode)
	}
}

func parseTokenOrBasicAuth(cfg plugin.ConnectConfig, auth string, label string, tokenField string, tokenCredentialField string, usernameField string, passwordField string, basicCredentialField string) (authHeader, error) {
	switch auth {
	case "none":
		return authHeader{}, nil
	case "token":
		token := strings.TrimSpace(cfg.String(tokenField))
		if token == "" {
			return authHeader{}, fmt.Errorf("%w: API token is required", plugin.ErrInvalidInput)
		}
		return authHeader{Header: "Authorization", Value: "Bearer " + token}, nil
	case "basic":
		username := strings.TrimSpace(cfg.String(usernameField))
		if username == "" {
			return authHeader{}, fmt.Errorf("%w: username is required", plugin.ErrInvalidInput)
		}
		return basicAuth(username, cfg.String(passwordField)), nil
	case "token_credential":
		kind := resolvedKindAny(cfg, tokenCredentialField)
		switch kind {
		case plugin.CredentialAPIToken:
			token := resolvedSecretAny(cfg, tokenCredentialField)
			if token == "" {
				return authHeader{}, fmt.Errorf("%w: stored token credential is required", plugin.ErrInvalidInput)
			}
			return authHeader{Header: "Authorization", Value: "Bearer " + token}, nil
		default:
			return authHeader{}, fmt.Errorf("%w: %s stored token credentials must be API tokens", plugin.ErrInvalidInput, label)
		}
	case "basic_credential":
		if kind := resolvedKindAny(cfg, basicCredentialField); kind != "" && kind != plugin.CredentialBasicAuth {
			return authHeader{}, fmt.Errorf("%w: %s stored basic credentials must be basic auth", plugin.ErrInvalidInput, label)
		}
		username := resolvedIdentityAny(cfg, basicCredentialField)
		if username == "" {
			return authHeader{}, fmt.Errorf("%w: basic auth credential identity is required", plugin.ErrInvalidInput)
		}
		return basicAuth(username, resolvedSecretAny(cfg, basicCredentialField)), nil
	default:
		return authHeader{}, fmt.Errorf("%w: unsupported %s authentication mode %q", plugin.ErrInvalidInput, label, auth)
	}
}

func resolvedSecretAny(cfg plugin.ConnectConfig, keys ...string) string {
	for _, key := range keys {
		if secret := dbcred.ResolvedSecret(cfg, key); secret != "" {
			return secret
		}
	}
	return dbcred.ResolvedSecret(cfg, plugin.CredentialIDField)
}

func resolvedIdentityAny(cfg plugin.ConnectConfig, keys ...string) string {
	for _, key := range keys {
		if identity := dbcred.ResolvedIdentity(cfg, key); identity != "" {
			return identity
		}
	}
	return dbcred.ResolvedIdentity(cfg, plugin.CredentialIDField)
}

func resolvedKindAny(cfg plugin.ConnectConfig, keys ...string) plugin.CredentialKind {
	for _, key := range keys {
		if kind := dbcred.ResolvedKind(cfg, key); kind != "" {
			return kind
		}
	}
	return cfg.CredentialKindFor(plugin.CredentialIDField)
}

func basicAuth(username, password string) authHeader {
	raw := username + ":" + password
	return authHeader{Header: "Authorization", Value: "Basic " + base64.StdEncoding.EncodeToString([]byte(raw))}
}

func queryLanguage(cfg map[string]any, mode string) string {
	switch mode {
	case modeV3:
		return broker.StringValue(cfg, "query_language_v3", "sql")
	case modeV2:
		return "flux"
	default:
		return "influxql"
	}
}

func configSchema() plugin.Schema {
	v3 := &plugin.Condition{AllOf: []plugin.Rule{{Field: "api_mode", Op: plugin.OpEq, Value: modeV3}}}
	v2 := &plugin.Condition{AllOf: []plugin.Rule{{Field: "api_mode", Op: plugin.OpEq, Value: modeV2}}}
	v1 := &plugin.Condition{AllOf: []plugin.Rule{{Field: "api_mode", Op: plugin.OpEq, Value: modeV1}}}
	v3Token := &plugin.Condition{AllOf: []plugin.Rule{{Field: "api_mode", Op: plugin.OpEq, Value: modeV3}, {Field: "auth_v3", Op: plugin.OpEq, Value: "token"}}}
	v3StoredToken := &plugin.Condition{AllOf: []plugin.Rule{{Field: "api_mode", Op: plugin.OpEq, Value: modeV3}, {Field: "auth_v3", Op: plugin.OpEq, Value: "token_credential"}}}
	v3Basic := &plugin.Condition{AllOf: []plugin.Rule{{Field: "api_mode", Op: plugin.OpEq, Value: modeV3}, {Field: "auth_v3", Op: plugin.OpEq, Value: "basic"}}}
	v3StoredBasic := &plugin.Condition{AllOf: []plugin.Rule{{Field: "api_mode", Op: plugin.OpEq, Value: modeV3}, {Field: "auth_v3", Op: plugin.OpEq, Value: "basic_credential"}}}
	v2Token := &plugin.Condition{AllOf: []plugin.Rule{{Field: "api_mode", Op: plugin.OpEq, Value: modeV2}, {Field: "auth_v2", Op: plugin.OpEq, Value: "token"}}}
	v2StoredToken := &plugin.Condition{AllOf: []plugin.Rule{{Field: "api_mode", Op: plugin.OpEq, Value: modeV2}, {Field: "auth_v2", Op: plugin.OpEq, Value: "credential"}}}
	v1Basic := &plugin.Condition{AllOf: []plugin.Rule{{Field: "api_mode", Op: plugin.OpEq, Value: modeV1}, {Field: "auth_v1", Op: plugin.OpEq, Value: "basic"}}}
	v1StoredBasic := &plugin.Condition{AllOf: []plugin.Rule{{Field: "api_mode", Op: plugin.OpEq, Value: modeV1}, {Field: "auth_v1", Op: plugin.OpEq, Value: "credential"}}}
	verifyTLS := &plugin.Condition{AllOf: []plugin.Rule{{Field: "tls_mode", Op: plugin.OpIn, Value: []any{"verify-ca", "verify-full"}}}}
	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Server", Fields: []plugin.Field{
			{Key: "api_mode", Label: "API mode", Type: plugin.FieldSelect, Required: true, Default: modeV3, Options: []plugin.Option{
				{Label: "InfluxDB 3", Value: modeV3},
				{Label: "InfluxDB 2", Value: modeV2},
				{Label: "InfluxDB 1.x", Value: modeV1},
			}},
			{Key: "endpoint", Label: "Endpoint", Type: plugin.FieldText, Required: true, Placeholder: "https://influxdb.example.internal:8086"},
			{Key: "org", Label: "Organization", Type: plugin.FieldText, Required: true, Placeholder: "production", VisibleWhen: v2},
			{Key: "database", Label: "Default database", Type: plugin.FieldText, Placeholder: "metrics", VisibleWhen: &plugin.Condition{AnyOf: []plugin.Rule{{Field: "api_mode", Op: plugin.OpEq, Value: modeV3}, {Field: "api_mode", Op: plugin.OpEq, Value: modeV1}}}},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{Key: "auth_v3", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: "token", VisibleWhen: v3, Options: []plugin.Option{
				{Label: "Token", Value: "token"},
				{Label: "Stored token", Value: "token_credential"},
				{Label: "Basic auth", Value: "basic"},
				{Label: "Stored basic auth", Value: "basic_credential"},
				{Label: "None", Value: "none"},
			}},
			{Key: "auth_v2", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: "token", VisibleWhen: v2, Options: []plugin.Option{
				{Label: "Token", Value: "token"},
				{Label: "Stored token", Value: "credential"},
				{Label: "None", Value: "none"},
			}},
			{Key: "auth_v1", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: "none", VisibleWhen: v1, Options: []plugin.Option{
				{Label: "None", Value: "none"},
				{Label: "Basic auth", Value: "basic"},
				{Label: "Stored basic auth", Value: "credential"},
			}},
			{Key: tokenFieldV3, Label: "Token", Type: plugin.FieldPassword, Required: true, Secret: true, VisibleWhen: v3Token},
			{Key: tokenCredV3, Label: "Stored token", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialAPIToken, Protocols: []string{protocolName},
			}, VisibleWhen: v3StoredToken},
			{Key: usernameFieldV3, Label: "Username", Type: plugin.FieldText, Required: true, VisibleWhen: v3Basic},
			{Key: passwordFieldV3, Label: "Password", Type: plugin.FieldPassword, Secret: true, VisibleWhen: v3Basic},
			{Key: basicCredV3, Label: "Stored basic auth", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialBasicAuth, Protocols: []string{protocolName},
			}, VisibleWhen: v3StoredBasic},
			{Key: tokenFieldV2, Label: "Token", Type: plugin.FieldPassword, Required: true, Secret: true, VisibleWhen: v2Token},
			{Key: tokenCredV2, Label: "Stored token", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialAPIToken, Protocols: []string{protocolName},
			}, VisibleWhen: v2StoredToken},
			{Key: usernameFieldV1, Label: "Username", Type: plugin.FieldText, Required: true, VisibleWhen: v1Basic},
			{Key: passwordFieldV1, Label: "Password", Type: plugin.FieldPassword, Secret: true, VisibleWhen: v1Basic},
			{Key: basicCredV1, Label: "Stored basic auth", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialBasicAuth, Protocols: []string{protocolName},
			}, VisibleWhen: v1StoredBasic},
		}},
		{Name: "TLS", Fields: []plugin.Field{
			{Key: "tls_mode", Label: "TLS mode", Type: plugin.FieldSelect, Required: true, Default: "disable", Options: []plugin.Option{
				{Label: "Disable", Value: "disable"},
				{Label: "Require", Value: "require"},
				{Label: "Verify CA", Value: "verify-ca"},
				{Label: "Verify full", Value: "verify-full"},
			}},
			{Key: "ca_certificate", Label: "CA certificate", Type: plugin.FieldTextarea, Secret: true, VisibleWhen: verifyTLS},
		}},
		{Name: "InfluxDB", Fields: []plugin.Field{
			{Key: "query_language_v3", Label: "Query language", Type: plugin.FieldSelect, Required: true, Default: "sql", VisibleWhen: v3, Options: []plugin.Option{
				{Label: "SQL", Value: "sql"},
				{Label: "InfluxQL", Value: "influxql"},
			}},
			{Key: "lookback", Label: "Default lookback", Type: plugin.FieldText, Default: "-1h", Placeholder: "-24h", VisibleWhen: v2, Help: "Default Flux range for data previews and schema discovery."},
			{Key: "timeout", Label: "Request timeout", Type: plugin.FieldDuration, Default: defaultTimeout.String()},
			{Key: "page_limit", Label: "Page limit", Type: plugin.FieldNumber, Default: defaultPageLimit, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: plugin.MaxPageLimit}}},
			{Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true, Help: "Blocks line protocol writes and non-read query statements."},
			{Key: "confirm_writes", Label: "Confirm writes", Type: plugin.FieldToggle, Default: true, Help: "Requires confirmation before writing line protocol or running write-capable statements."},
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
