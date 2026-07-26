// Package chroma implements the Chroma vector database plugin over the REST v2 API.
package chroma

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
	protocolName    = "chroma"
	apiPrefix       = "/api/v2"
	defaultEndpoint = "http://localhost:8000"
	defaultTenant   = "default_tenant"
	defaultDatabase = "default_database"

	defaultTimeout   = 15 * time.Second
	defaultPageLimit = 100
	defaultSample    = 512
	minSample        = 32
	maxSample        = 2000

	tokenCredentialField = plugin.CredentialRefField
	basicCredentialField = "basic_credential_id"
)

const iconSVG = `<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" id="Chroma--Streamline-Svg-Logos" height="24" width="24"><desc>Chroma Streamline Icon: https://streamlinehq.com</desc><path fill="#ffde2d" d="M15.916575 19.52c4.326225 0 7.833325 -3.3668 7.833325 -7.519975 0 -4.153175 -3.5071 -7.519975 -7.833325 -7.519975 -4.326225 0 -7.833325 3.3668 -7.833325 7.519975 0 4.153175 3.5071 7.519975 7.833325 7.519975Z" stroke-width="0.25"></path><path fill="#327eff" d="M8.083325 19.52c4.326225 0 7.833325 -3.3668 7.833325 -7.519975 0 -4.153175 -3.5071 -7.519975 -7.833325 -7.519975C3.7571 4.48005 0.25 7.84685 0.25 12.000025 0.25 16.1532 3.7571 19.52 8.083325 19.52Z" stroke-width="0.25"></path><path fill="#ff6446" d="M15.916625 12.000025c0 4.1532 -3.507125 7.519925 -7.833375 7.519925V12.000025h7.833375Zm-7.833375 0c0 -4.153175 3.5071 -7.519975 7.833375 -7.519975v7.519975H8.08325Z" stroke-width="0.25"></path></svg>`

type Plugin struct{}

type Options struct {
	Endpoint  string
	Tenant    string
	Database  string
	Headers   map[string]string
	TLSConfig *tls.Config
	Timeout   time.Duration
	PageLimit int
	Sample    int
	ReadOnly  bool
}

type Session struct {
	client *searchrest.Client
	opts   Options
}

type row = plugin.TableRow

func New() plugin.Plugin { return Plugin{} }

func (Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:  plugin.CurrentAPIVersion,
		Name:        protocolName,
		Version:     "0.1.0",
		Title:       "Chroma",
		Description: "Chroma vector database cockpit: tenants, databases, collections, record CRUD, similarity search, embedding projection, and distance-space comparison.",
		Icon:        plugin.Icon{Type: plugin.IconSVG, Value: iconSVG},
		Category:    plugin.CategoryDatabases,
		Config:      configSchema(),
		Capabilities: []plugin.Capability{
			"tenants", "databases", "collections", "records",
			"vector-search", "full-text-filter", "embedding-projection", "metrics", "saved-queries",
		},
		SupportedTransports: []plugin.Transport{plugin.TransportDirect},
		Layout:              plugin.LayoutSidebarTree,
		Tree:                tree(),
		Resources:           resources(),
		Actions:             actions(),
		Streams:             streams(),
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
			Headers:   opts.Headers,
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
	return s.client.Do(ctx, "GET", apiPrefix+"/heartbeat", nil, nil, &out)
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
		rawURL = defaultEndpoint
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return Options{}, fmt.Errorf("%w: endpoint must be an absolute URL", plugin.ErrInvalidInput)
	}
	opts := Options{
		Endpoint:  strings.TrimRight(u.String(), "/"),
		Tenant:    broker.StringValue(cfg.Config, "tenant", defaultTenant),
		Database:  broker.StringValue(cfg.Config, "database", defaultDatabase),
		Timeout:   broker.DurationValue(cfg.Config, "timeout", defaultTimeout),
		PageLimit: broker.IntValue(cfg.Config, "page_limit", defaultPageLimit, 1, plugin.MaxPageLimit),
		Sample:    broker.IntValue(cfg.Config, "sample_limit", defaultSample, minSample, maxSample),
		ReadOnly:  broker.BoolValue(cfg.Config, "read_only", true),
	}
	if err := validateName("tenant", opts.Tenant); err != nil {
		return Options{}, err
	}
	if err := validateName("database", opts.Database); err != nil {
		return Options{}, err
	}
	headers, err := authHeaders(cfg)
	if err != nil {
		return Options{}, err
	}
	opts.Headers = headers
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

func authHeaders(cfg plugin.ConnectConfig) (map[string]string, error) {
	switch mode := broker.StringValue(cfg.Config, "auth", "none"); mode {
	case "none":
		return nil, nil
	case "token":
		return tokenHeader("x-chroma-token", strings.TrimSpace(cfg.String("token")))
	case "bearer":
		token := strings.TrimSpace(cfg.String("token"))
		return tokenHeader("Authorization", bearerValue(token))
	case "basic":
		return basicHeader(strings.TrimSpace(cfg.String("username")), cfg.String("password"))
	case "credential_token":
		if kind := cfg.CredentialKindFor(tokenCredentialField); kind != "" && kind != plugin.CredentialKindAPIToken {
			return nil, fmt.Errorf("%w: stored Chroma tokens must be API tokens", plugin.ErrInvalidInput)
		}
		return tokenHeader("x-chroma-token", dbcred.ResolvedSecret(cfg, tokenCredentialField))
	case "credential_basic":
		if kind := cfg.CredentialKindFor(basicCredentialField); kind != "" && kind != plugin.CredentialKindBasicAuth {
			return nil, fmt.Errorf("%w: stored Chroma basic auth must be a basic-auth credential", plugin.ErrInvalidInput)
		}
		return basicHeader(dbcred.ResolvedIdentity(cfg, basicCredentialField), dbcred.ResolvedSecret(cfg, basicCredentialField))
	default:
		return nil, fmt.Errorf("%w: unsupported authentication mode %q", plugin.ErrInvalidInput, mode)
	}
}

func tokenHeader(header, value string) (map[string]string, error) {
	if strings.TrimSpace(value) == "" {
		return nil, fmt.Errorf("%w: an API token is required for the selected authentication mode", plugin.ErrInvalidInput)
	}
	return map[string]string{header: value}, nil
}

func bearerValue(token string) string {
	if token == "" {
		return ""
	}
	return "Bearer " + token
}

func basicHeader(username, password string) (map[string]string, error) {
	if strings.TrimSpace(username) == "" || password == "" {
		return nil, fmt.Errorf("%w: a username and password are required for basic authentication", plugin.ErrInvalidInput)
	}
	encoded := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
	return map[string]string{"Authorization": "Basic " + encoded}, nil
}

// validateName rejects values that would change the meaning of a Chroma REST
// path (Chroma names are alphanumeric plus `_.-`).
func validateName(field, value string) error {
	if value == "" {
		return fmt.Errorf("%w: %s is required", plugin.ErrInvalidInput, field)
	}
	if value == "." || value == ".." {
		return fmt.Errorf("%w: %s %q is not a name", plugin.ErrInvalidInput, field, value)
	}
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '_', r == '-', r == '.':
		default:
			return fmt.Errorf("%w: %s %q may only contain letters, digits, underscore, hyphen, or dot", plugin.ErrInvalidInput, field, value)
		}
	}
	return nil
}

func configSchema() plugin.Schema {
	authIs := func(values ...any) *plugin.Condition {
		return &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpIn, Value: values}}}
	}
	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Server", Fields: []plugin.Field{
			{Key: "endpoint", Label: "Endpoint", Type: plugin.FieldURL, Required: true, Default: defaultEndpoint, Placeholder: "https://chroma.example.internal:8000", Help: "Base URL of the Chroma server; the plugin calls its /api/v2 surface."},
			{Key: "tenant", Label: "Tenant", Type: plugin.FieldText, Required: true, Default: defaultTenant},
			{Key: "database", Label: "Database", Type: plugin.FieldText, Required: true, Default: defaultDatabase},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: "none", Options: []plugin.Option{
				{Label: "None", Value: "none"},
				{Label: "Token (x-chroma-token)", Value: "token"},
				{Label: "Bearer token", Value: "bearer"},
				{Label: "Basic auth", Value: "basic"},
				{Label: "Stored API token", Value: "credential_token"},
				{Label: "Stored basic auth", Value: "credential_basic"},
			}},
			{Key: "token", Label: "Token", Type: plugin.FieldPassword, Secret: true, Required: true, VisibleWhen: authIs("token", "bearer")},
			{Key: "username", Label: "Username", Type: plugin.FieldText, Required: true, VisibleWhen: authIs("basic")},
			{Key: "password", Label: "Password", Type: plugin.FieldPassword, Secret: true, Required: true, VisibleWhen: authIs("basic")},
			{Key: tokenCredentialField, Label: "Stored API token", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindAPIToken, Protocols: []string{protocolName},
			}, VisibleWhen: authIs("credential_token")},
			{Key: basicCredentialField, Label: "Stored basic auth", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindBasicAuth, Protocols: []string{protocolName},
			}, VisibleWhen: authIs("credential_basic")},
		}},
		{Name: "TLS", Fields: []plugin.Field{
			{Key: "tls_mode", Label: "TLS mode", Type: plugin.FieldSelect, Required: true, Default: "disable", Options: []plugin.Option{
				{Label: "Disable", Value: "disable"},
				{Label: "Require", Value: "require"},
				{Label: "Verify CA", Value: "verify-ca"},
				{Label: "Verify full", Value: "verify-full"},
			}},
			{Key: "ca_certificate", Label: "CA certificate", Type: plugin.FieldTextarea, Secret: true, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{
				{Field: "tls_mode", Op: plugin.OpIn, Value: []any{"verify-ca", "verify-full"}},
			}}},
		}},
		{Name: "Safety", Fields: []plugin.Field{
			{Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true, Help: "Blocks every collection, database, tenant, and record mutation."},
			{Key: "timeout", Label: "Request timeout", Type: plugin.FieldDuration, Default: "15s"},
			{Key: "page_limit", Label: "Page limit", Type: plugin.FieldNumber, Default: defaultPageLimit, Validators: []plugin.Validator{
				{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: plugin.MaxPageLimit},
			}},
			{Key: "sample_limit", Label: "Visualization sample", Type: plugin.FieldNumber, Default: defaultSample, Help: "Vectors loaded for the embedding map and distance-space comparison.", Validators: []plugin.Validator{
				{Type: plugin.ValidatorMin, Value: minSample}, {Type: plugin.ValidatorMax, Value: maxSample},
			}},
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
	return nil, fmt.Errorf("%w: no Chroma session on this request", plugin.ErrInvalidInput)
}
