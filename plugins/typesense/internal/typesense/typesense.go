// Package typesense implements the Typesense protocol plugin.
package typesense

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

const iconSVG = `<svg xmlns="http://www.w3.org/2000/svg" width="256" height="256" viewBox="0 0 256 255"><path fill="#1035BC" d="M75.104 80.303c.551 2.751.826 5.41.826 7.979c0 2.384-.275 4.951-.826 7.702l-34.938-.275v92.437c0 7.703 3.576 11.554 10.729 11.554h20.908c1.284 3.118 1.926 6.236 1.926 9.354c0 3.118-.184 5.044-.55 5.777c-8.437 1.1-17.149 1.65-26.135 1.65c-17.79 0-26.686-7.61-26.686-22.833V95.709l-19.533.275C.275 93.234 0 90.666 0 88.282c0-2.568.275-5.228.825-7.979l19.533.275V51.692c0-4.952.734-8.437 2.2-10.454c1.468-2.201 4.31-3.302 8.53-3.302h7.427l1.65 1.651v41.267l34.94-.551Zm10.477 125.255c.178-4.02 1.275-8.405 3.286-13.156c2.194-4.934 4.661-8.771 7.401-11.512c14.436 7.857 27.134 11.786 38.1 11.786c6.026 0 10.87-1.188 14.524-3.563c3.837-2.376 5.759-5.573 5.759-9.594c0-6.395-4.935-11.511-14.803-15.349l-15.35-5.755c-23.022-8.406-34.534-21.836-34.534-40.292c0-6.578 1.186-12.425 3.564-17.541c2.557-5.3 6.026-9.776 10.415-13.43c4.567-3.838 9.958-6.761 16.173-8.771c6.21-2.01 13.154-3.016 20.829-3.016c3.47 0 7.307.275 11.511.823c4.384.548 8.772 1.37 13.155 2.467c4.388.913 8.588 2.01 12.609 3.289c4.02 1.279 7.49 2.65 10.415 4.111c0 4.568-.914 9.319-2.74 14.253c-1.827 4.934-4.295 8.588-7.402 10.963c-14.436-6.395-26.95-9.593-37.548-9.593c-4.75 0-8.499 1.188-11.239 3.564c-2.74 2.192-4.11 5.116-4.11 8.77c0 5.665 4.567 10.142 13.706 13.43l16.719 6.03c12.057 4.203 21.013 9.96 26.86 17.268c5.848 7.31 8.772 15.806 8.772 25.49c0 12.974-4.845 23.39-14.53 31.246c-9.685 7.675-23.57 11.513-41.659 11.513c-17.726 0-34.356-4.478-49.883-13.431Zm150.807 48.031V.83c2.762-.554 5.894-.83 9.396-.83c3.682 0 7.09.276 10.216.829v252.76c-3.127.552-6.534.83-10.216.83c-3.502 0-6.634-.278-9.396-.83Z"/></svg>`

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

type row = plugin.TableRow

type actionResult struct {
	OK bool `json:"ok"`
}

func New() plugin.Plugin { return Plugin{} }

func (Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:          plugin.CurrentAPIVersion,
		Name:                "typesense",
		Version:             "0.1.0",
		Title:               "Typesense",
		Description:         "Typesense cockpit with collections, schemas, documents, search, aliases, synonyms, overrides, keys, metrics, and stats.",
		Icon:                plugin.Icon{Type: plugin.IconSVG, Value: iconSVG},
		Category:            plugin.CategorySearch,
		Config:              configSchema(),
		Capabilities:        []plugin.Capability{"collections", "documents", "search", "schema", "aliases", "synonyms", "overrides", "keys"},
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
		auth = searchrest.Auth{Header: "X-TYPESENSE-API-KEY", Value: opts.APIKey}
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
		rawURL = "http://localhost:8108"
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
	case "api_key":
		opts.APIKey = cfg.String("api_key")
	case "credential":
		if kind := cfg.CredentialKindFor(plugin.CredentialRefField); kind != "" && kind != plugin.CredentialKindAPIToken {
			return Options{}, fmt.Errorf("%w: Typesense stored credentials must be API tokens", plugin.ErrInvalidInput)
		}
		opts.APIKey = dbcred.ResolvedSecret(cfg, plugin.CredentialRefField)
	default:
		return Options{}, fmt.Errorf("%w: unsupported authentication mode %q", plugin.ErrInvalidInput, auth)
	}
	if opts.APIKey == "" {
		return Options{}, fmt.Errorf("%w: Typesense API key is required", plugin.ErrInvalidInput)
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
			{Key: "endpoint", Label: "Endpoint", Type: plugin.FieldText, Required: true, Default: "http://localhost:8108", Placeholder: "https://typesense.example.internal:8108"},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: "api_key", Options: []plugin.Option{
				{Label: "API key", Value: "api_key"},
				{Label: "Stored API key", Value: "credential"},
			}},
			{Key: "api_key", Label: "API key", Type: plugin.FieldPassword, Required: true, Secret: true, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "api_key"}}}},
			{Key: credentialField, Label: "Stored API key", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindAPIToken, Protocols: []string{"typesense"},
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
			{Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true, Help: "Blocks collection, document, alias, synonym, override, and API key writes."},
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
