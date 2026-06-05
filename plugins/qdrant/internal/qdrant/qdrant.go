// Package qdrant implements the Qdrant protocol plugin.
package qdrant

import (
	"context"
	"crypto/tls"
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
	protocolName     = "qdrant"
	defaultTimeout   = 10 * time.Second
	defaultPageLimit = 100
	credentialField  = "credential_id"
)

const iconSVG = `<svg data-name="Capa 2"id=Capa_2 viewBox="0 0 346.42 400"xmlns=http://www.w3.org/2000/svg><defs><style>.cls-1{fill:#9e0d38}.cls-2{fill:#dc244c}.cls-3{fill:#ff516b}</style></defs><g id=Vectors><g><g><polygon class=cls-2 points="173.21 0 0 100 0 300 173.21 400 238.16 362.5 238.16 287.5 173.21 325 64.96 262.5 64.96 137.5 173.21 75 281.46 137.5 281.46 387.5 346.42 350 346.42 100 173.21 0"/><polygon class=cls-2 points="108.26 162.5 108.26 237.5 173.21 275 238.16 237.5 238.16 162.5 173.21 125 108.26 162.5"/></g><g><polygon class=cls-1 points="238.16 287.5 238.16 362.5 173.21 400 173.21 325 238.16 287.5"/><polygon class=cls-1 points="346.42 100 346.42 350 281.46 387.5 281.46 137.5 346.42 100"/><polygon class=cls-3 points="346.42 100 281.46 137.5 173.21 75 64.96 137.5 0 100 173.21 0 346.42 100"/><polygon class=cls-2 points="173.21 325 173.21 400 0 300 0 100 64.96 137.5 64.96 262.5 173.21 325"/><polygon class=cls-3 points="238.16 162.5 173.21 200 108.26 162.5 173.21 125 238.16 162.5"/><polygon class=cls-2 points="173.21 200 173.21 275 108.26 237.5 108.26 162.5 173.21 200"/><polygon class=cls-1 points="238.16 162.5 238.16 237.5 173.21 275 173.21 200 238.16 162.5"/></g></g></g></svg>`

type Plugin struct{}

type Options struct {
	Endpoint  string
	APIKey    string
	TLSConfig *tls.Config
	Timeout   time.Duration
	PageLimit int
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
		Version:             "0.1.1",
		Title:               "Qdrant",
		Description:         "Qdrant vector database cockpit with collections, collection details, point browsing, payload inspection, and JSON vector queries.",
		Icon:                plugin.Icon{Type: plugin.IconSVG, Value: iconSVG},
		Category:            plugin.CategorySearch,
		Config:              configSchema(),
		Capabilities:        []plugin.Capability{"collections", "points", "vector-search"},
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
	auth := searchrest.Auth{}
	if opts.APIKey != "" {
		auth = searchrest.Auth{Header: "api-key", Value: opts.APIKey}
	}
	s := &Session{
		client: searchrest.New(searchrest.Options{
			Endpoint:  opts.Endpoint,
			Auth:      auth,
			TLSConfig: opts.TLSConfig,
			Timeout:   opts.Timeout,
			Dialer:    cfg.Net.DialContext,
		}),
		opts: opts,
	}
	return s, s.HealthCheck(ctx)
}

func (s *Session) HealthCheck(ctx context.Context) error {
	var out any
	return s.client.Do(ctx, "GET", "/collections", nil, nil, &out)
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
		rawURL = "http://localhost:6333"
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
	switch auth := broker.StringValue(cfg.Config, "auth", "none"); auth {
	case "none":
	case "api_key":
		opts.APIKey = cfg.String("api_key")
	case "credential":
		if kind := cfg.CredentialKindFor(plugin.CredentialField); kind != "" && kind != plugin.CredentialAPIToken {
			return Options{}, fmt.Errorf("%w: Qdrant stored credentials must be API tokens", plugin.ErrInvalidInput)
		}
		opts.APIKey = dbcred.ResolvedSecret(cfg, plugin.CredentialField)
	default:
		return Options{}, fmt.Errorf("%w: unsupported authentication mode %q", plugin.ErrInvalidInput, auth)
	}
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

func configSchema() plugin.Schema {
	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Server", Fields: []plugin.Field{
			{Key: "endpoint", Label: "Endpoint", Type: plugin.FieldText, Required: true, Default: "http://localhost:6333", Placeholder: "https://qdrant.example.internal"},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: "none", Options: []plugin.Option{
				{Label: "None", Value: "none"},
				{Label: "API key", Value: "api_key"},
				{Label: "Stored API key", Value: "credential"},
			}},
			{Key: "api_key", Label: "API key", Type: plugin.FieldPassword, Secret: true, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "api_key"}}}},
			{Key: credentialField, Label: "Stored API key", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kinds: []plugin.CredentialKind{plugin.CredentialAPIToken}, Protocols: []string{protocolName},
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
			{Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true, Help: "Blocks collection and point mutations."},
			{Key: "timeout", Label: "Request timeout", Type: plugin.FieldDuration, Default: "10s"},
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
