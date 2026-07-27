package trino

import (
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	trinodriver "github.com/trinodb/trino-go-client/trino"

	"github.com/charlesng35/shellcn-contrib/shared/dbcred"
	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	defaultPort       = 8080
	defaultRowLimit   = 500
	defaultTimeout    = 60 * time.Second
	defaultMaxConns   = 4
	protocolName      = "trino"
	credentialIDField = "credential_id"
	tokenCertField    = "token_credential_id"
	authCertField     = "auth_client_cert_id"
	clientCertField   = "client_cert_id"
	authUser          = "user"
	authPassword      = "password"
	authCredential    = "credential"
	authToken         = "token"
	authClientCert    = "client_certificate"
)

// identifierPattern accepts the catalog/schema/table names Trino connectors
// actually produce (Hive and Iceberg allow digits, "$" and "-"), which the
// stricter shared SQL identifier pattern rejects.
const identifierPattern = `^[A-Za-z0-9_][A-Za-z0-9_$-]{0,127}$`

var identifierRE = regexp.MustCompile(identifierPattern)

type options struct {
	Host              string
	Port              int
	Catalog           string
	Schema            string
	Username          string
	Password          string
	AccessToken       string
	TLSMode           string
	CACertificate     string
	ClientCertificate string
	Source            string
	ClientTags        []string
	SessionProperties map[string]string
	ReadOnly          bool
	RequireConfirm    bool
	QueryTimeout      time.Duration
	RowLimit          int
	MaxConns          int
	RedactPatterns    []string
}

func (o options) scheme() string {
	if o.TLSMode == "" || o.TLSMode == "disable" {
		return "http"
	}
	return "https"
}

func (o options) address() string {
	return net.JoinHostPort(o.Host, strconv.Itoa(o.Port))
}

func configSchema() plugin.Schema {
	passwordAuth := plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: authPassword}}}
	credentialAuth := plugin.Condition{AnyOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: authCredential}, {Field: credentialIDField, Op: plugin.OpNotEmpty}}}
	tokenAuth := plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: authToken}}}
	certAuth := plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: authClientCert}}}
	usernameAuth := plugin.Condition{AnyOf: []plugin.Rule{
		{Field: "auth", Op: plugin.OpEq, Value: authUser},
		{Field: "auth", Op: plugin.OpEq, Value: authPassword},
		{Field: "auth", Op: plugin.OpEq, Value: authClientCert},
	}}
	optionalClientCertificate := plugin.Condition{AllOf: []plugin.Rule{
		{Field: "tls_mode", Op: plugin.OpNeq, Value: "disable"},
		{Field: "auth", Op: plugin.OpNeq, Value: authClientCert},
	}}
	verifyTLS := plugin.Condition{AnyOf: []plugin.Rule{
		{Field: "tls_mode", Op: plugin.OpEq, Value: "verify-ca"},
		{Field: "tls_mode", Op: plugin.OpEq, Value: "verify-full"},
	}}
	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Coordinator", Fields: []plugin.Field{
			{Key: "host", Label: "Host", Type: plugin.FieldText, Required: true, Placeholder: "trino.example.internal", Help: "Trino coordinator host."},
			{Key: "port", Label: "Port", Type: plugin.FieldNumber, Required: true, Default: defaultPort, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 65535}}},
			{Key: "catalog", Label: "Default catalog", Type: plugin.FieldText, Placeholder: "tpch", Help: "Catalog used when a query or panel does not name one."},
			{Key: "schema", Label: "Default schema", Type: plugin.FieldText, Placeholder: "tiny"},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: authUser, Options: []plugin.Option{
				{Label: "User only", Value: authUser},
				{Label: "Password", Value: authPassword},
				{Label: "Stored password", Value: authCredential},
				{Label: "Access token (JWT)", Value: authToken},
				{Label: "Client certificate", Value: authClientCert},
			}},
			{Key: "username", Label: "Username", Type: plugin.FieldText, Required: true, Placeholder: "trino", VisibleWhen: &usernameAuth, Help: "Sent as X-Trino-User and used for query attribution and access control."},
			{Key: credentialIDField, Label: "Stored password", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindDBPassword, Protocols: []string{protocolName},
			}, VisibleWhen: &credentialAuth, Help: "Reusable password for Trino PASSWORD authentication. The credential identity can also supply the username."},
			{Key: "password", Label: "Password", Type: plugin.FieldPassword, Secret: true, VisibleWhen: &passwordAuth, Help: "Trino only accepts password authentication over TLS."},
			{Key: tokenCertField, Label: "Access token", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindBearerToken, Protocols: []string{protocolName},
			}, VisibleWhen: &tokenAuth, Help: "Reusable JWT sent as the Authorization bearer token."},
			{Key: authCertField, Label: "Client certificate", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindTLSClientCert, Protocols: []string{protocolName},
			}, VisibleWhen: &certAuth, Help: "Reusable certificate and private key used for Trino CERTIFICATE authentication."},
		}},
		{Name: "TLS", Fields: []plugin.Field{
			{Key: "tls_mode", Label: "TLS mode", Type: plugin.FieldSelect, Required: true, Default: "disable", Options: []plugin.Option{
				{Label: "Disable", Value: "disable"},
				{Label: "Require encryption", Value: "require"},
				{Label: "Verify CA", Value: "verify-ca"},
				{Label: "Verify full", Value: "verify-full"},
			}, Help: "Anything other than disable makes the coordinator URL https."},
			{Key: "ca_certificate", Label: "CA certificate", Type: plugin.FieldTextarea, Secret: true, VisibleWhen: &verifyTLS, Help: "PEM CA bundle used for verify-ca and verify-full."},
			{Key: clientCertField, Label: "Client certificate", Type: plugin.FieldCredentialRef, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindTLSClientCert, Protocols: []string{protocolName},
			}, VisibleWhen: &optionalClientCertificate, Help: "Optional PEM certificate and private key for mTLS when another authentication mode is used."},
		}},
		{Name: "Session", Fields: []plugin.Field{
			{Key: "source", Label: "Source", Type: plugin.FieldText, Default: plugin.DefaultClientName, Help: "Reported as X-Trino-Source; resource groups can match on it."},
			{Key: "client_tags", Label: "Client tags", Type: plugin.FieldText, Help: "Comma separated tags reported to the coordinator for resource group selection."},
			{Key: "session_properties", Label: "Session properties", Type: plugin.FieldTextarea, Help: "One name=value per line, e.g. query_max_run_time=10m."},
		}},
		{Name: "Safety", Fields: []plugin.Field{
			{Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true, Help: "Blocks INSERT, UPDATE, DELETE, MERGE, DDL, CALL, GRANT, SET SESSION, and EXPLAIN ANALYZE."},
			{Key: "require_destructive_confirmation", Label: "Confirm destructive statements", Type: plugin.FieldToggle, Default: true, Help: "Requires explicit confirmation before writes, DDL, procedure calls, and privilege changes execute."},
			{Key: "query_timeout", Label: "Query timeout", Type: plugin.FieldDuration, Default: defaultTimeout.String()},
			{Key: "row_limit", Label: "Row limit", Type: plugin.FieldNumber, Default: defaultRowLimit, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: plugin.MaxPageLimit}}},
			{Key: "max_connections", Label: "Pool size", Type: plugin.FieldNumber, Default: defaultMaxConns, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 20}}},
			{Key: "redact_columns", Label: "Redacted columns", Type: plugin.FieldTextarea, Help: "Comma or newline separated regular expressions for result columns that must be masked."},
		}},
	}}
}

func parseOptions(cfg plugin.ConnectConfig) (options, error) {
	host := strings.TrimSpace(cfg.String("host"))
	if host == "" {
		return options{}, fmt.Errorf("%w: host is required", plugin.ErrInvalidInput)
	}
	port, ok := cfg.Int("port")
	if !ok || port == 0 {
		port = defaultPort
	}
	if port < 1 || port > 65535 {
		return options{}, fmt.Errorf("%w: port must be between 1 and 65535", plugin.ErrInvalidInput)
	}
	catalog, err := optionalIdentifier(cfg.String("catalog"))
	if err != nil {
		return options{}, fmt.Errorf("%w: default catalog is not a valid identifier", plugin.ErrInvalidInput)
	}
	schema, err := optionalIdentifier(cfg.String("schema"))
	if err != nil {
		return options{}, fmt.Errorf("%w: default schema is not a valid identifier", plugin.ErrInvalidInput)
	}
	mode := strings.TrimSpace(cfg.String("auth"))
	if mode == "" {
		mode = authUser
	}
	tlsMode := stringDefault(cfg.String("tls_mode"), "disable")
	auth := dbcred.ApplyPasswordCredential(cfg, cfg.String("username"), cfg.String("password"))
	clientCertificate := dbcred.ResolvedClientCertificate(cfg, clientCertField)
	accessToken := ""
	switch mode {
	case authUser:
		auth.Password = ""
	case authPassword, authCredential:
	case authToken:
		accessToken = dbcred.ResolvedSecret(cfg, tokenCertField)
		if accessToken == "" {
			return options{}, fmt.Errorf("%w: access token credential is required", plugin.ErrInvalidInput)
		}
		auth.Password = ""
		if auth.Username == "" {
			auth.Username = dbcred.ResolvedIdentity(cfg, tokenCertField)
		}
	case authClientCert:
		certAuth := dbcred.ApplyClientCertificateCredential(cfg, authCertField, auth.Username, tlsMode, "")
		if certAuth.ClientCertificate == "" {
			return options{}, fmt.Errorf("%w: client certificate credential is required", plugin.ErrInvalidInput)
		}
		auth.Username = certAuth.Username
		auth.Password = ""
		tlsMode = certAuth.TLSMode
		clientCertificate = certAuth.ClientCertificate
	default:
		return options{}, fmt.Errorf("%w: unsupported authentication mode %q", plugin.ErrInvalidInput, mode)
	}
	if auth.Username == "" && accessToken == "" {
		return options{}, fmt.Errorf("%w: username is required", plugin.ErrInvalidInput)
	}
	// Trino only accepts a password or a bearer token over TLS, and the plugin
	// will not put either on a plaintext hop just to watch the coordinator
	// reject it.
	if tlsMode == "" || tlsMode == "disable" {
		switch {
		case auth.Password != "":
			return options{}, fmt.Errorf("%w: password authentication requires TLS; choose a TLS mode other than disable", plugin.ErrInvalidInput)
		case accessToken != "":
			return options{}, fmt.Errorf("%w: access token authentication requires TLS; choose a TLS mode other than disable", plugin.ErrInvalidInput)
		}
	}
	rowLimit, ok := cfg.Int("row_limit")
	if !ok || rowLimit <= 0 {
		rowLimit = defaultRowLimit
	}
	if rowLimit > plugin.MaxPageLimit {
		rowLimit = plugin.MaxPageLimit
	}
	maxConns, ok := cfg.Int("max_connections")
	if !ok || maxConns <= 0 {
		maxConns = defaultMaxConns
	}
	if maxConns > 20 {
		maxConns = 20
	}
	properties, err := parseSessionProperties(cfg.String("session_properties"))
	if err != nil {
		return options{}, err
	}
	return options{
		Host:              host,
		Port:              port,
		Catalog:           catalog,
		Schema:            schema,
		Username:          auth.Username,
		Password:          auth.Password,
		AccessToken:       accessToken,
		TLSMode:           tlsMode,
		CACertificate:     cfg.String("ca_certificate"),
		ClientCertificate: clientCertificate,
		Source:            stringDefault(cfg.String("source"), plugin.DefaultClientName),
		ClientTags:        splitList(cfg.String("client_tags")),
		SessionProperties: properties,
		ReadOnly:          sqldb.BoolValue(cfg.Config["read_only"], true),
		RequireConfirm:    sqldb.BoolValue(cfg.Config["require_destructive_confirmation"], true),
		QueryTimeout:      sqldb.DurationValue(cfg.Config["query_timeout"], defaultTimeout),
		RowLimit:          rowLimit,
		MaxConns:          maxConns,
		RedactPatterns:    sqldb.ParsePatterns(cfg.String("redact_columns"), sqldb.DefaultRedactColumnPatterns()),
	}, nil
}

// buildDSN renders the driver DSN. No secret reaches it: the password and the
// access token are attached by the registered http.Client, and the TLS material
// goes on its transport, because the driver rejects a custom client combined
// with an SSL certificate parameter. A non-empty upstream (an agent-supplied L7
// endpoint) replaces the configured coordinator address.
func buildDSN(opts options, clientName, upstream string) (string, error) {
	server := url.URL{Scheme: opts.scheme(), Host: opts.address()}
	if upstream != "" {
		parsed, err := url.Parse(upstream)
		if err != nil || parsed.Host == "" {
			return "", fmt.Errorf("%w: gateway upstream %q is not an absolute URL", plugin.ErrUnavailable, upstream)
		}
		server.Scheme, server.Host = parsed.Scheme, parsed.Host
	}
	if opts.Username != "" {
		server.User = url.User(opts.Username)
	}
	cfg := &trinodriver.Config{
		ServerURI:         server.String(),
		Source:            opts.Source,
		Catalog:           opts.Catalog,
		Schema:            opts.Schema,
		SessionProperties: opts.SessionProperties,
		ClientTags:        opts.ClientTags,
		CustomClientName:  clientName,
		QueryTimeout:      &opts.QueryTimeout,
	}
	dsn, err := cfg.FormatDSN()
	if err != nil {
		return "", fmt.Errorf("%w: %v", plugin.ErrInvalidInput, err)
	}
	return dsn, nil
}

func parseSessionProperties(raw string) (map[string]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	out := map[string]string{}
	for _, line := range strings.FieldsFunc(raw, func(r rune) bool { return r == '\n' || r == '\r' }) {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, value, ok := strings.Cut(line, "=")
		name, value = strings.TrimSpace(name), strings.TrimSpace(value)
		if !ok || name == "" {
			return nil, fmt.Errorf("%w: session property %q must be name=value", plugin.ErrInvalidInput, line)
		}
		// The driver joins properties with ";" and splits pairs on ":", so a
		// property carrying either separator would silently corrupt the header.
		if strings.ContainsAny(name, ":;") || strings.ContainsAny(value, ":;") {
			return nil, fmt.Errorf("%w: session property %q must not contain ':' or ';'", plugin.ErrInvalidInput, name)
		}
		out[name] = value
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func splitList(raw string) []string {
	parts := strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || r == '\n' || r == '\r' || r == '\t' })
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func safeIdentifier(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if !identifierRE.MatchString(trimmed) {
		return "", fmt.Errorf("%w: %q is not a valid Trino identifier", plugin.ErrInvalidInput, raw)
	}
	return trimmed, nil
}

func optionalIdentifier(raw string) (string, error) {
	if strings.TrimSpace(raw) == "" {
		return "", nil
	}
	return safeIdentifier(raw)
}

func stringDefault(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return strings.TrimSpace(v)
}
