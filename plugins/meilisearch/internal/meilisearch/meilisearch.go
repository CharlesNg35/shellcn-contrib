// Package meilisearch implements the Meilisearch protocol plugin.
package meilisearch

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
	defaultTimeout   = 10 * time.Second
	defaultPageLimit = 100
	credentialField  = "credential_id"
)

const iconSVG = `<svg fill=none height=24 id=Meilisearch--Streamline-Svg-Logos viewBox="0 0 96 96"width=24 xmlns=http://www.w3.org/2000/svg><desc>Meilisearch Streamline Icon: https://streamlinehq.com</desc><path d="m1 75.4083 17.3668-44.4339c2.4473-6.2617 8.4829-10.3828 15.2057-10.3828h10.4703L26.676 65.0255c-2.4473 6.2616-8.4829 10.3828-15.2058 10.3828H1Z"fill=url(#a)></path><path d="m26.478 75.4083 17.3668-44.4339c2.4473-6.2617 8.4829-10.3828 15.2058-10.3828h10.4702L52.154 65.0255c-2.4473 6.2616-8.4829 10.3828-15.2058 10.3828H26.478Z"fill=url(#b)></path><path d="m51.9575 75.4083 17.3668-44.4339c2.4473-6.2617 8.4825-10.3828 15.2054-10.3828h10.4707L77.6335 65.0255c-2.4477 6.2616-8.4829 10.3828-15.2058 10.3828H51.9575Z"fill=url(#c)></path><defs><linearGradient gradientUnits=userSpaceOnUse id=a x1=6621.87 x2=-45.829 y1=-398.113 y2=3368.77><stop stop-color=#ff5caa></stop><stop stop-color=#ff4e62 offset=1></stop></linearGradient><linearGradient gradientUnits=userSpaceOnUse id=b x1=5076.48 x2=-1591.21 y1=-398.126 y2=3368.75><stop stop-color=#ff5caa></stop><stop stop-color=#ff4e62 offset=1></stop></linearGradient><linearGradient gradientUnits=userSpaceOnUse id=c x1=3531.03 x2=-3136.69 y1=-398.126 y2=3368.77><stop stop-color=#ff5caa></stop><stop stop-color=#ff4e62 offset=1></stop></linearGradient></defs></svg>`

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

type actionResult struct {
	OK bool `json:"ok"`
}

func New() plugin.Plugin { return Plugin{} }

func (Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:          plugin.CurrentAPIVersion,
		Name:                "meilisearch",
		Version:             "0.1.0",
		Title:               "Meilisearch",
		Description:         "Meilisearch cockpit with indexes, documents, JSON search, settings, tasks, keys, dumps, and snapshots.",
		Icon:                plugin.Icon{Type: plugin.IconSVG, Value: iconSVG},
		Category:            plugin.CategorySearch,
		Config:              configSchema(),
		Capabilities:        []plugin.Capability{"indexes", "documents", "search", "settings", "tasks", "keys"},
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
	auth := searchrest.Auth{}
	if opts.APIKey != "" {
		auth = searchrest.Auth{Header: "Authorization", Value: "Bearer " + opts.APIKey}
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
	var health map[string]any
	return s.client.Do(ctx, "GET", "/health", nil, nil, &health)
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
		rawURL = "http://localhost:7700"
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
	switch auth := broker.StringValue(cfg.Config, "auth", "api_key"); auth {
	case "none":
	case "api_key":
		opts.APIKey = cfg.String("api_key")
	case "credential":
		if kind := cfg.CredentialKindFor(plugin.CredentialRefField); kind != "" && kind != plugin.CredentialKindAPIToken {
			return Options{}, fmt.Errorf("%w: Meilisearch stored credentials must be API tokens", plugin.ErrInvalidInput)
		}
		opts.APIKey = dbcred.ResolvedSecret(cfg, plugin.CredentialRefField)
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
			{Key: "endpoint", Label: "Endpoint", Type: plugin.FieldText, Required: true, Default: "http://localhost:7700", Placeholder: "https://meilisearch.example.internal:7700"},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: "api_key", Options: []plugin.Option{
				{Label: "API key", Value: "api_key"},
				{Label: "Stored API key", Value: "credential"},
				{Label: "None", Value: "none"},
			}},
			{Key: "api_key", Label: "API key", Type: plugin.FieldPassword, Secret: true, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "api_key"}}}},
			{Key: credentialField, Label: "Stored API key", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindAPIToken, Protocols: []string{"meilisearch"},
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
			{Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true, Help: "Blocks index, document, settings, task, key, dump, and snapshot writes."},
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
