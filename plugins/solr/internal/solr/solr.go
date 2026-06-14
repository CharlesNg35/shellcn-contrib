// Package solr implements the Apache Solr protocol plugin.
package solr

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
	defaultTimeout        = 10 * time.Second
	defaultPageLimit      = 100
	basicCredentialField  = "basic_credential_id"
	bearerCredentialField = "bearer_credential_id"
	protocolName          = "solr"
)

const iconSVG = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 48 48"><defs><clipPath id="a" clipPathUnits="userSpaceOnUse"><path d="M0 102.654h203.005V0H0Z"/></clipPath></defs><g clip-path="url(#a)" transform="matrix(1.33333 0 0 -1.33333 0 136.872)"><path d="m0 0-33-35.677L8.473-16.543A33.5 33.5 0 0 1 0 0" style="fill:#da3522;fill-opacity:1;fill-rule:nonzero;stroke:none" transform="translate(31.112 96.955)scale(.53706)"/><path d="M0 0c-4.572 0-8.928-.917-12.9-2.572l-4.428-37.314L4.799-.347A34 34 0 0 1 0 0" style="fill:#da3522;fill-opacity:1;fill-rule:nonzero;stroke:none" transform="translate(17.962 102.654)scale(.53706)"/><path d="m0 0-39.298-21.992 36.87 4.375A33.46 33.46 0 0 1 0 0" style="fill:#da3522;fill-opacity:1;fill-rule:nonzero;stroke:none" transform="translate(35.839 86.966)scale(.53706)"/><path d="M0 0a33.75 33.75 0 0 1 10.612 11.619l-34.559-6.863Z" style="fill:#da3522;fill-opacity:1;fill-rule:nonzero;stroke:none" transform="translate(28.058 69.675)scale(.53706)"/><path d="m0 0-19.237-41.695L16.448-8.689C11.973-4.384 6.313-1.303 0 0" style="fill:#da3522;fill-opacity:1;fill-rule:nonzero;stroke:none" transform="translate(21.626 102.28)scale(.53706)"/><path d="M0 0a33.4 33.4 0 0 1 10.54 2.638L-8.818 4.935Z" style="fill:#da3522;fill-opacity:1;fill-rule:nonzero;stroke:none" transform="translate(19.47 66.654)scale(.53706)"/><path d="M0 0a33.4 33.4 0 0 1-2.829-10.792l5.215-9.32z" style="fill:#da3522;fill-opacity:1;fill-rule:nonzero;stroke:none" transform="translate(1.524 92.034)scale(.53706)"/><path d="M0 0a33.74 33.74 0 0 1-11.832-10.567l4.867-24.504z" style="fill:#da3522;fill-opacity:1;fill-rule:nonzero;stroke:none" transform="translate(9.471 100.531)scale(.53706)"/></g></svg>`

type Plugin struct{}

type Options struct {
	Endpoint  string
	TLSConfig *tls.Config
	Auth      searchrest.Auth
	Timeout   time.Duration
	PageLimit int
	ReadOnly  bool
}

type Session struct {
	client *searchrest.Client
	opts   Options
	mode   string
}

type row = plugin.TableRow

func New() plugin.Plugin { return Plugin{} }

func (Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:          plugin.CurrentAPIVersion,
		Name:                protocolName,
		Version:             "0.1.0",
		Title:               "Apache Solr",
		Description:         "Apache Solr cockpit with cores, documents, schema fields, JSON queries, config, ping, commit, optimize, and CoreAdmin operations.",
		Icon:                plugin.Icon{Type: plugin.IconSVG, Value: iconSVG},
		Category:            plugin.CategorySearch,
		Config:              configSchema(),
		Capabilities:        []plugin.Capability{"cores", "documents", "search", "schema", "config"},
		SupportedTransports: []plugin.Transport{plugin.TransportDirect},
		Layout:              plugin.LayoutSidebarTree,
		Tree:                tree(),
		Resources:           resources(),
		Actions:             actions(),
		Streams:             []plugin.Stream{{ID: rid("search.query"), Kind: plugin.StreamLogs, RouteID: rid("search.query")}},
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
		}),
		opts: opts,
	}
	return s, s.HealthCheck(ctx)
}

func (s *Session) HealthCheck(ctx context.Context) error {
	var out map[string]any
	if err := s.client.Do(ctx, "GET", "/admin/info/system", url.Values{"wt": []string{"json"}}, nil, &out); err != nil {
		return err
	}
	s.mode = strings.TrimSpace(fmt.Sprint(out["mode"]))
	return nil
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
		rawURL = "http://localhost:8983/solr"
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return Options{}, fmt.Errorf("%w: endpoint must be an absolute URL", plugin.ErrInvalidInput)
	}
	opts := Options{
		Endpoint:  strings.TrimRight(u.String(), "/"),
		Timeout:   broker.DurationValue(cfg.Config, "timeout", defaultTimeout),
		PageLimit: broker.IntValue(cfg.Config, "page_limit", defaultPageLimit, 1, plugin.MaxPageLimit),
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
		password := cfg.String("password")
		if username == "" {
			return searchrest.Auth{}, fmt.Errorf("%w: username is required for basic authentication", plugin.ErrInvalidInput)
		}
		return basicAuth(username, password), nil
	case "bearer":
		token := cfg.String("bearer_token")
		if token == "" {
			return searchrest.Auth{}, fmt.Errorf("%w: bearer token is required", plugin.ErrInvalidInput)
		}
		return searchrest.Auth{Header: "Authorization", Value: "Bearer " + token}, nil
	case "stored_basic":
		username := dbcred.ResolvedIdentity(cfg, basicCredentialField)
		if username == "" {
			return searchrest.Auth{}, fmt.Errorf("%w: Solr basic credentials require a username", plugin.ErrInvalidInput)
		}
		return basicAuth(username, dbcred.ResolvedSecret(cfg, basicCredentialField)), nil
	case "stored_bearer":
		token := dbcred.ResolvedSecret(cfg, bearerCredentialField)
		if token == "" {
			return searchrest.Auth{}, fmt.Errorf("%w: Solr bearer credentials require a token", plugin.ErrInvalidInput)
		}
		return searchrest.Auth{Header: "Authorization", Value: "Bearer " + token}, nil
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
			{Key: "endpoint", Label: "Endpoint", Type: plugin.FieldText, Required: true, Default: "http://localhost:8983/solr", Placeholder: "https://solr.example.internal/solr"},
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
				Kind: plugin.CredentialKindBasicAuth, Protocols: []string{protocolName},
			}, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "stored_basic"}}}},
			{Key: bearerCredentialField, Label: "Stored bearer token", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindBearerToken, Protocols: []string{protocolName},
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
		{Name: "Safety", Fields: []plugin.Field{
			{Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true, Help: "Blocks collection/core, document, schema, commit, and optimize writes."},
			{Key: "timeout", Label: "Request timeout", Type: plugin.FieldDuration, Default: "10s"},
			{Key: "page_limit", Label: "Page limit", Type: plugin.FieldNumber, Default: defaultPageLimit, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: plugin.MaxPageLimit}}},
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

func ensureWritable(s *Session) error {
	if s.opts.ReadOnly {
		return fmt.Errorf("%w: connection is read-only", plugin.ErrForbidden)
	}
	return nil
}
