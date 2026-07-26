package arangodb

import (
	"crypto/tls"
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
	protocolName     = "arangodb"
	defaultEndpoint  = "http://localhost:8529"
	defaultDatabase  = "_system"
	defaultTimeout   = 30 * time.Second
	defaultPageLimit = 100
	defaultDepth     = 2
	maxDepth         = 6

	credentialIDField = "credential_id"
	jwtCredentialID   = "jwt_credential_id"

	authNone       = "none"
	authPassword   = "password"
	authCredential = "credential"
	authJWT        = "jwt"
	authStoredJWT  = "stored_jwt"
)

type Options struct {
	Endpoint       string
	Host           string
	Database       string
	Auth           string
	Username       string
	Password       string
	Token          string
	TLSConfig      *tls.Config
	ReadOnly       bool
	RequireConfirm bool
	Timeout        time.Duration
	PageLimit      int
	Depth          int
	RedactPatterns []string
}

func configSchema() plugin.Schema {
	inlinePassword := &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: authPassword}}}
	storedPassword := &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: authCredential}}}
	inlineJWT := &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: authJWT}}}
	storedJWT := &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: authStoredJWT}}}
	verifiedTLS := &plugin.Condition{AllOf: []plugin.Rule{{Field: "tls_mode", Op: plugin.OpIn, Value: []any{"verify-ca", "verify-full"}}}}

	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Server", Fields: []plugin.Field{
			{Key: "endpoint", Label: "Coordinator endpoint", Type: plugin.FieldURL, Required: true, Default: defaultEndpoint,
				Placeholder: "https://arangodb.example.internal:8529",
				Help:        "Base URL of a single server or cluster coordinator."},
			{Key: "database", Label: "Default database", Type: plugin.FieldText, Required: true, Default: defaultDatabase,
				Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: databasePattern, Message: "most characters are allowed; not / : % ? # + or control characters"}}},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: authPassword, Options: []plugin.Option{
				{Label: "Username & password", Value: authPassword},
				{Label: "Stored database password", Value: authCredential},
				{Label: "JWT bearer token", Value: authJWT},
				{Label: "Stored JWT bearer token", Value: authStoredJWT},
				{Label: "None (auth disabled server)", Value: authNone},
			}},
			{Key: "username", Label: "Username", Type: plugin.FieldText, Required: true, Default: "root", VisibleWhen: inlinePassword},
			{Key: "password", Label: "Password", Type: plugin.FieldPassword, Secret: true, VisibleWhen: inlinePassword},
			{Key: credentialIDField, Label: "Stored password", Type: plugin.FieldCredentialRef, Required: true, VisibleWhen: storedPassword,
				Credential: &plugin.CredentialSelector{Kind: plugin.CredentialKindDBPassword, Protocols: []string{protocolName}},
				Help:       "Reusable database credential. Its username field is used when present."},
			{Key: "jwt_token", Label: "JWT token", Type: plugin.FieldPassword, Secret: true, Required: true, VisibleWhen: inlineJWT},
			{Key: jwtCredentialID, Label: "Stored JWT token", Type: plugin.FieldCredentialRef, Required: true, VisibleWhen: storedJWT,
				Credential: &plugin.CredentialSelector{Kind: plugin.CredentialKindBearerToken, Protocols: []string{protocolName}}},
		}},
		{Name: "TLS", Fields: []plugin.Field{
			{Key: "tls_mode", Label: "TLS mode", Type: plugin.FieldSelect, Required: true, Default: "disable", Options: []plugin.Option{
				{Label: "Disable", Value: "disable"},
				{Label: "Require (no verification)", Value: "require"},
				{Label: "Verify CA", Value: "verify-ca"},
				{Label: "Verify full", Value: "verify-full"},
			}, Help: "Applies to https:// endpoints."},
			{Key: "ca_certificate", Label: "CA certificate", Type: plugin.FieldTextarea, Secret: true, VisibleWhen: verifiedTLS,
				Placeholder: "-----BEGIN CERTIFICATE-----"},
		}},
		{Name: "Safety", Fields: []plugin.Field{
			{Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true,
				Help: "Blocks every mutating route and any AQL containing INSERT, UPDATE, REPLACE, UPSERT or REMOVE."},
			{Key: "require_write_confirmation", Label: "Confirm writing AQL", Type: plugin.FieldToggle, Default: true,
				Help: "Requires an explicit confirmation before data-modifying AQL executes."},
			{Key: "timeout", Label: "Request timeout", Type: plugin.FieldDuration, Default: defaultTimeout.String()},
			{Key: "page_limit", Label: "Page limit", Type: plugin.FieldNumber, Default: defaultPageLimit,
				Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: plugin.MaxPageLimit}}},
			{Key: "traversal_depth", Label: "Graph expand depth", Type: plugin.FieldStepper, Default: defaultDepth, Step: 1,
				Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: maxDepth}},
				Help:       "Hops fetched when seeding or expanding the graph explorer."},
			{Key: "redact_attributes", Label: "Redacted attributes", Type: plugin.FieldTextarea,
				Help: "Comma or newline separated regular expressions for document attributes and result columns that must be masked."},
		}},
	}}
}

func parseOptions(cfg plugin.ConnectConfig) (Options, error) {
	raw := strings.TrimSpace(cfg.String("endpoint"))
	if raw == "" {
		raw = defaultEndpoint
	}
	endpoint, err := url.Parse(connectionURL(raw))
	if err != nil || endpoint.Host == "" {
		return Options{}, fmt.Errorf("%w: endpoint must be an absolute URL such as %s", plugin.ErrInvalidInput, defaultEndpoint)
	}
	switch endpoint.Scheme {
	case "http", "https":
	default:
		return Options{}, fmt.Errorf("%w: endpoint scheme %q is not supported", plugin.ErrInvalidInput, endpoint.Scheme)
	}

	database, err := safeDatabaseName(broker.StringValue(cfg.Config, "database", defaultDatabase))
	if err != nil {
		return Options{}, err
	}

	opts := Options{
		Endpoint:       strings.TrimRight(endpoint.String(), "/"),
		Host:           endpoint.Hostname(),
		Database:       database,
		Auth:           broker.StringValue(cfg.Config, "auth", authPassword),
		ReadOnly:       broker.BoolValue(cfg.Config, "read_only", true),
		RequireConfirm: broker.BoolValue(cfg.Config, "require_write_confirmation", true),
		Timeout:        broker.DurationValue(cfg.Config, "timeout", defaultTimeout),
		PageLimit:      broker.IntValue(cfg.Config, "page_limit", defaultPageLimit, 1, plugin.MaxPageLimit),
		Depth:          broker.IntValue(cfg.Config, "traversal_depth", defaultDepth, 1, maxDepth),
		RedactPatterns: sqldb.ParsePatterns(cfg.String("redact_attributes"), sqldb.DefaultRedactColumnPatterns()),
	}

	switch opts.Auth {
	case authNone:
	case authPassword:
		opts.Username = strings.TrimSpace(cfg.String("username"))
		opts.Password = cfg.String("password")
	case authCredential:
		if kind := dbcred.ResolvedKind(cfg, credentialIDField); kind != "" && kind != plugin.CredentialKindDBPassword {
			return Options{}, fmt.Errorf("%w: stored ArangoDB credentials must be database passwords", plugin.ErrInvalidInput)
		}
		material := dbcred.ApplyPasswordCredential(cfg, cfg.String("username"), "")
		opts.Username = material.Username
		opts.Password = material.Password
		if opts.Username == "" {
			opts.Username = cfg.CredentialValueFor(credentialIDField, "username")
		}
		if opts.Password == "" {
			opts.Password = dbcred.ResolvedSecret(cfg, credentialIDField)
		}
	case authJWT:
		opts.Token = strings.TrimSpace(cfg.String("jwt_token"))
	case authStoredJWT:
		if kind := dbcred.ResolvedKind(cfg, jwtCredentialID); kind != "" && kind != plugin.CredentialKindBearerToken {
			return Options{}, fmt.Errorf("%w: stored ArangoDB JWT credentials must be bearer tokens", plugin.ErrInvalidInput)
		}
		opts.Token = dbcred.ResolvedSecret(cfg, jwtCredentialID)
	default:
		return Options{}, fmt.Errorf("%w: unsupported authentication method %q", plugin.ErrInvalidInput, opts.Auth)
	}

	tlsMode := broker.StringValue(cfg.Config, "tls_mode", "disable")
	if endpoint.Scheme == "http" {
		tlsMode = "disable"
	}
	opts.TLSConfig, err = sqldb.TLSConfig(sqldb.TLSOptions{Mode: tlsMode, Host: opts.Host, CACertificate: cfg.String("ca_certificate")})
	if err != nil {
		return Options{}, err
	}
	return opts, nil
}

// connectionURL accepts the arangod-native tcp:// and ssl:// endpoint spellings
// that appear in cluster configuration alongside plain http(s) URLs.
func connectionURL(raw string) string {
	if !strings.Contains(raw, "://") {
		return "http://" + raw
	}
	return strings.NewReplacer("tcp://", "http://", "ssl://", "https://").Replace(raw)
}
