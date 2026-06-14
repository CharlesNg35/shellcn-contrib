package neo4j

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/dbcred"
	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	protocolName          = "neo4j"
	defaultPort           = 7687
	defaultDatabase       = "neo4j"
	defaultQueryTimeout   = 30 * time.Second
	defaultConnectTime    = 10 * time.Second
	defaultRetryTime      = 15 * time.Second
	defaultPoolSize       = 16
	defaultFetchSize      = 500
	defaultPageLimit      = 100
	credentialIDField     = "credential_id"
	bearerCredentialField = "bearer_credential_id"
	authNone              = "none"
	authPassword          = "password"
	authCredential        = "credential"
	authBearer            = "bearer"
	authStoredBearer      = "stored_bearer"
)

type options struct {
	Scheme         string
	Host           string
	Port           int
	Database       string
	Auth           string
	Username       string
	Password       string
	Realm          string
	BearerToken    string
	CACertificate  string
	TLSConfig      *tls.Config
	ReadOnly       bool
	RequireConfirm bool
	QueryTimeout   time.Duration
	ConnectTimeout time.Duration
	RetryTime      time.Duration
	PoolSize       int
	FetchSize      int
	PageLimit      int
	RedactPatterns []string
}

func (o options) URI() string {
	return o.Scheme + "://" + hostPort(o.Host, o.Port)
}

func configSchema() plugin.Schema {
	passwordAuth := &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: authPassword}, {Field: credentialIDField, Op: plugin.OpEmpty}}}
	credentialAuth := &plugin.Condition{
		AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpNin, Value: []string{authNone, authBearer, authStoredBearer}}},
		AnyOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: authCredential}, {Field: credentialIDField, Op: plugin.OpNotEmpty}},
	}
	bearerAuth := &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: authBearer}, {Field: bearerCredentialField, Op: plugin.OpEmpty}}}
	storedBearerAuth := &plugin.Condition{
		AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpNin, Value: []string{authNone, authPassword, authCredential}}},
		AnyOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: authStoredBearer}, {Field: bearerCredentialField, Op: plugin.OpNotEmpty}},
	}
	caVisible := &plugin.Condition{AllOf: []plugin.Rule{{Field: "scheme", Op: plugin.OpIn, Value: []any{"bolt+s", "neo4j+s"}}}}
	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Server", Fields: []plugin.Field{
			{Key: "scheme", Label: "Connection scheme", Type: plugin.FieldSelect, Required: true, Default: "bolt", Options: []plugin.Option{
				{Label: "Bolt", Value: "bolt"},
				{Label: "Neo4j routing", Value: "neo4j"},
				{Label: "Bolt + verified TLS", Value: "bolt+s"},
				{Label: "Neo4j routing + verified TLS", Value: "neo4j+s"},
				{Label: "Bolt + self-signed TLS", Value: "bolt+ssc"},
				{Label: "Neo4j routing + self-signed TLS", Value: "neo4j+ssc"},
			}},
			{Key: "host", Label: "Host", Type: plugin.FieldText, Required: true, Placeholder: "neo4j.example.internal"},
			{Key: "port", Label: "Bolt port", Type: plugin.FieldNumber, Required: true, Default: defaultPort, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 65535}}},
			{Key: "database", Label: "Default database", Type: plugin.FieldText, Required: true, Default: defaultDatabase},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: authPassword, Options: []plugin.Option{
				{Label: "Password", Value: authPassword},
				{Label: "Stored password", Value: authCredential},
				{Label: "Bearer token", Value: authBearer},
				{Label: "Stored bearer token", Value: authStoredBearer},
				{Label: "None", Value: authNone},
			}},
			{Key: "username", Label: "Username", Type: plugin.FieldText, Default: "neo4j", VisibleWhen: passwordAuth},
			{Key: credentialIDField, Label: "Stored password", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindDBPassword, Protocols: []string{protocolName},
			}, VisibleWhen: credentialAuth, Help: "Reusable Neo4j password. The credential identity can also supply the username."},
			{Key: "password", Label: "Password", Type: plugin.FieldPassword, Secret: true, VisibleWhen: passwordAuth},
			{Key: "realm", Label: "Realm", Type: plugin.FieldText, VisibleWhen: passwordAuth},
			{Key: "bearer_token", Label: "Bearer token", Type: plugin.FieldPassword, Secret: true, VisibleWhen: bearerAuth},
			{Key: bearerCredentialField, Label: "Stored bearer token", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindBearerToken, Protocols: []string{protocolName},
			}, VisibleWhen: storedBearerAuth},
		}},
		{Name: "TLS", Fields: []plugin.Field{
			{Key: "ca_certificate", Label: "CA certificate", Type: plugin.FieldTextarea, Secret: true, VisibleWhen: caVisible, Help: "Optional PEM CA bundle used with verified TLS schemes."},
		}},
		{Name: "Safety", Fields: []plugin.Field{
			{Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true, Help: "Blocks CREATE, MERGE, SET, DELETE, REMOVE, DROP, ALTER, privilege changes, and write procedures."},
			{Key: "require_write_confirmation", Label: "Confirm write queries", Type: plugin.FieldToggle, Default: true, Help: "Requires confirmation before write or administrative Cypher executes."},
			{Key: "query_timeout", Label: "Query timeout", Type: plugin.FieldDuration, Default: defaultQueryTimeout.String()},
			{Key: "connect_timeout", Label: "Connect timeout", Type: plugin.FieldDuration, Default: defaultConnectTime.String()},
			{Key: "retry_time", Label: "Retry time", Type: plugin.FieldDuration, Default: defaultRetryTime.String()},
			{Key: "pool_size", Label: "Pool size", Type: plugin.FieldNumber, Default: defaultPoolSize, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 100}}},
			{Key: "fetch_size", Label: "Fetch size", Type: plugin.FieldNumber, Default: defaultFetchSize, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: plugin.MaxPageLimit}}},
			{Key: "page_limit", Label: "Page limit", Type: plugin.FieldNumber, Default: defaultPageLimit, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: plugin.MaxPageLimit}}},
			{Key: "redact_properties", Label: "Redacted properties", Type: plugin.FieldTextarea, Help: "Comma or newline separated regular expressions for result columns/properties that must be masked."},
		}},
	}}
}

func parseOptions(cfg plugin.ConnectConfig) (options, error) {
	scheme := strings.TrimSpace(cfg.String("scheme"))
	if scheme == "" {
		scheme = "bolt"
	}
	switch scheme {
	case "bolt", "neo4j", "bolt+s", "neo4j+s", "bolt+ssc", "neo4j+ssc":
	default:
		return options{}, fmt.Errorf("%w: unsupported Neo4j scheme %q", plugin.ErrInvalidInput, scheme)
	}
	host := strings.TrimSpace(cfg.String("host"))
	if host == "" {
		return options{}, fmt.Errorf("%w: host is required", plugin.ErrInvalidInput)
	}
	if _, err := url.Parse(scheme + "://" + host); err != nil {
		return options{}, fmt.Errorf("%w: invalid host", plugin.ErrInvalidInput)
	}
	port, ok := cfg.Int("port")
	if !ok || port == 0 {
		port = defaultPort
	}
	if port < 1 || port > 65535 {
		return options{}, fmt.Errorf("%w: port must be between 1 and 65535", plugin.ErrInvalidInput)
	}
	authMode := strings.TrimSpace(cfg.String("auth"))
	if authMode == "" {
		authMode = authPassword
	}
	auth := dbcred.AuthMaterial{}
	bearer := ""
	switch authMode {
	case authNone:
	case authPassword, authCredential:
		auth = dbcred.ApplyPasswordCredential(cfg, cfg.String("username"), cfg.String("password"))
	case authBearer:
		bearer = cfg.String("bearer_token")
	case authStoredBearer:
		if kind := dbcred.ResolvedKind(cfg, bearerCredentialField); kind != "" && kind != plugin.CredentialKindBearerToken {
			return options{}, fmt.Errorf("%w: Neo4j bearer credentials must be bearer tokens", plugin.ErrInvalidInput)
		}
		bearer = dbcred.ResolvedSecret(cfg, bearerCredentialField)
		if bearer == "" {
			bearer = dbcred.ResolvedSecret(cfg, plugin.CredentialRefField)
		}
	default:
		return options{}, fmt.Errorf("%w: unsupported authentication method %q", plugin.ErrInvalidInput, authMode)
	}
	tlsConfig, err := neo4jTLSConfig(scheme, host, cfg.String("ca_certificate"))
	if err != nil {
		return options{}, err
	}
	return options{
		Scheme:         scheme,
		Host:           host,
		Port:           port,
		Database:       stringDefault(cfg.String("database"), defaultDatabase),
		Auth:           authMode,
		Username:       auth.Username,
		Password:       auth.Password,
		Realm:          strings.TrimSpace(cfg.String("realm")),
		BearerToken:    bearer,
		CACertificate:  cfg.String("ca_certificate"),
		TLSConfig:      tlsConfig,
		ReadOnly:       sqldb.BoolValue(cfg.Config["read_only"], true),
		RequireConfirm: sqldb.BoolValue(cfg.Config["require_write_confirmation"], true),
		QueryTimeout:   sqldb.DurationValue(cfg.Config["query_timeout"], defaultQueryTimeout),
		ConnectTimeout: sqldb.DurationValue(cfg.Config["connect_timeout"], defaultConnectTime),
		RetryTime:      sqldb.DurationValue(cfg.Config["retry_time"], defaultRetryTime),
		PoolSize:       intValue(cfg.Config["pool_size"], defaultPoolSize, 1, 100),
		FetchSize:      intValue(cfg.Config["fetch_size"], defaultFetchSize, 1, plugin.MaxPageLimit),
		PageLimit:      intValue(cfg.Config["page_limit"], defaultPageLimit, 1, plugin.MaxPageLimit),
		RedactPatterns: sqldb.ParsePatterns(cfg.String("redact_properties"), sqldb.DefaultRedactColumnPatterns()),
	}, nil
}

func neo4jTLSConfig(scheme, host, ca string) (*tls.Config, error) {
	if !strings.HasSuffix(scheme, "+s") && !strings.HasSuffix(scheme, "+ssc") {
		if strings.TrimSpace(ca) != "" {
			return nil, fmt.Errorf("%w: CA certificate requires a verified TLS scheme", plugin.ErrInvalidInput)
		}
		return nil, nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: host}
	if strings.TrimSpace(ca) != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(ca)) {
			return nil, fmt.Errorf("%w: CA certificate is not valid PEM", plugin.ErrInvalidInput)
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}

func intValue(v any, def, low, high int) int {
	n := def
	switch x := v.(type) {
	case int:
		n = x
	case int64:
		n = int(x)
	case float64:
		n = int(x)
	}
	if n < low {
		return low
	}
	if n > high {
		return high
	}
	return n
}

func stringDefault(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return strings.TrimSpace(v)
}
