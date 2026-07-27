package snowflake

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	sf "github.com/snowflakedb/gosnowflake/v2"

	"github.com/charlesng35/shellcn-contrib/shared/dbcred"
	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	protocolName      = "snowflake"
	credentialIDField = "credential_id"
	keyPairField      = "key_pair_id"

	credentialKindKeyPair = plugin.CredentialKind("snowflake_key_pair")

	authPassword   = "password"
	authCredential = "credential"
	authKeyPair    = "key_pair"

	defaultPort            = 443
	defaultSchema          = "PUBLIC"
	defaultRowLimit        = 500
	defaultMaxConns        = 4
	defaultTimeout         = 60 * time.Second
	defaultHistoryWindow   = 1000
	maxHistoryWindow       = 10000
	defaultMetricsInterval = 30 * time.Second
	minMetricsInterval     = 10 * time.Second
)

type options struct {
	Account         string
	Host            string
	Port            int
	Warehouse       string
	Role            string
	Database        string
	Schema          string
	Username        string
	Password        string
	PrivateKey      *rsa.PrivateKey
	ReadOnly        bool
	RequireConfirm  bool
	QueryTimeout    time.Duration
	RowLimit        int
	MaxConns        int
	HistoryWindow   int
	MetricsInterval time.Duration
	ApplicationName string
	RedactPatterns  []string
}

func credentialKinds() []plugin.CredentialKindInfo {
	return []plugin.CredentialKindInfo{{
		Kind:  credentialKindKeyPair,
		Label: "Snowflake key pair",
		Fields: []plugin.Field{
			plugin.CredentialPublicField(plugin.Field{Key: "username", Label: "Snowflake user", Type: plugin.FieldText, Required: true}),
			plugin.CredentialSecretField(plugin.Field{Key: "private_key", Label: "RSA private key (PEM)", Type: plugin.FieldTextarea, Required: true}),
		},
	}}
}

func configSchema() plugin.Schema {
	passwordAuth := plugin.Condition{AllOf: []plugin.Rule{
		{Field: "auth", Op: plugin.OpEq, Value: authPassword},
		{Field: credentialIDField, Op: plugin.OpEmpty},
		{Field: keyPairField, Op: plugin.OpEmpty},
	}}
	credentialAuth := plugin.Condition{AnyOf: []plugin.Rule{
		{Field: "auth", Op: plugin.OpEq, Value: authCredential},
		{Field: credentialIDField, Op: plugin.OpNotEmpty},
	}}
	keyPairAuth := plugin.Condition{AnyOf: []plugin.Rule{
		{Field: "auth", Op: plugin.OpEq, Value: authKeyPair},
		{Field: keyPairField, Op: plugin.OpNotEmpty},
	}}
	usernameAuth := plugin.Condition{AnyOf: []plugin.Rule{
		{Field: "auth", Op: plugin.OpEq, Value: authPassword},
		{Field: "auth", Op: plugin.OpEq, Value: authKeyPair},
		{Field: keyPairField, Op: plugin.OpNotEmpty},
	}}
	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Account", Fields: []plugin.Field{
			{Key: "account", Label: "Account identifier", Type: plugin.FieldText, Required: true, Placeholder: "myorg-myaccount", Help: "Organization and account name, or the legacy locator such as xy12345.us-east-1."},
			{Key: "host", Label: "Account hostname", Type: plugin.FieldText, Required: true, Placeholder: "myorg-myaccount.snowflakecomputing.com", Help: "Hostname the gateway connects to. Usually the account identifier plus snowflakecomputing.com; use the privatelink or region hostname when yours differs."},
			{Key: "port", Label: "Port", Type: plugin.FieldNumber, Default: defaultPort, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 65535}}},
		}},
		{Name: "Session", Fields: []plugin.Field{
			{Key: "database", Label: "Database", Type: plugin.FieldText, Required: true, Help: "Default database. Its INFORMATION_SCHEMA also backs the account, warehouse, and query-history views."},
			{Key: "schema", Label: "Schema", Type: plugin.FieldText, Default: defaultSchema},
			{Key: "warehouse", Label: "Warehouse", Type: plugin.FieldText, Help: "Virtual warehouse used to run queries. Metadata browsing works without one."},
			{Key: "role", Label: "Role", Type: plugin.FieldText, Help: "Role activated for the session. Leave blank to use the user's default role."},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: authPassword, Options: []plugin.Option{
				{Label: "Password", Value: authPassword},
				{Label: "Stored password", Value: authCredential},
				{Label: "Key pair", Value: authKeyPair},
			}},
			{Key: "username", Label: "User", Type: plugin.FieldText, Required: true, VisibleWhen: &usernameAuth},
			{Key: "password", Label: "Password", Type: plugin.FieldPassword, Secret: true, VisibleWhen: &passwordAuth},
			{Key: credentialIDField, Label: "Stored password", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindDBPassword, Protocols: []string{protocolName},
			}, VisibleWhen: &credentialAuth, Help: "Reusable Snowflake password. The credential identity can also supply the user name."},
			{Key: keyPairField, Label: "Key pair", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: credentialKindKeyPair, Protocols: []string{protocolName},
			}, VisibleWhen: &keyPairAuth, Help: "Unencrypted PKCS#8 or PKCS#1 RSA private key whose public key is set on the Snowflake user with ALTER USER ... SET RSA_PUBLIC_KEY."},
		}},
		{Name: "Safety", Fields: []plugin.Field{
			{Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true, Help: "Blocks INSERT, UPDATE, DELETE, MERGE, COPY, DDL, GRANT, and other write statements."},
			{Key: "require_destructive_confirmation", Label: "Confirm destructive statements", Type: plugin.FieldToggle, Default: true, Help: "Requires explicit confirmation before writes, DDL, grants, and session-state statements execute."},
			{Key: "query_timeout", Label: "Query timeout", Type: plugin.FieldDuration, Default: defaultTimeout.String(), Help: "Also applied as the session STATEMENT_TIMEOUT_IN_SECONDS."},
			{Key: "row_limit", Label: "Row limit", Type: plugin.FieldNumber, Default: defaultRowLimit, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: plugin.MaxPageLimit}}},
			{Key: "max_connections", Label: "Pool size", Type: plugin.FieldNumber, Default: defaultMaxConns, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 20}}},
			{Key: "history_window", Label: "Query history window", Type: plugin.FieldNumber, Default: defaultHistoryWindow, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: maxHistoryWindow}}, Help: "How many recent queries INFORMATION_SCHEMA.QUERY_HISTORY returns before filtering, sorting, and paging."},
			{Key: "metrics_interval", Label: "Metrics interval", Type: plugin.FieldDuration, Default: defaultMetricsInterval.String(), Help: "Poll cadence for the live credit and warehouse-load panels."},
			{Key: "redact_columns", Label: "Redacted columns", Type: plugin.FieldTextarea, Help: "Comma or newline separated regular expressions for result columns that must be masked."},
		}},
	}}
}

func parseOptions(cfg plugin.ConnectConfig) (options, error) {
	account := strings.TrimSpace(cfg.String("account"))
	if account == "" {
		return options{}, fmt.Errorf("%w: account is required", plugin.ErrInvalidInput)
	}
	host := strings.TrimSpace(cfg.String("host"))
	if host == "" {
		return options{}, fmt.Errorf("%w: account hostname is required", plugin.ErrInvalidInput)
	}
	if strings.Contains(host, "://") {
		return options{}, fmt.Errorf("%w: account hostname must not include a scheme", plugin.ErrInvalidInput)
	}
	port, ok := cfg.Int("port")
	if !ok || port == 0 {
		port = defaultPort
	}
	if port < 1 || port > 65535 {
		return options{}, fmt.Errorf("%w: port must be between 1 and 65535", plugin.ErrInvalidInput)
	}
	database, err := safeIdentifier(cfg.String("database"))
	if err != nil {
		return options{}, fmt.Errorf("%w: database is required", plugin.ErrInvalidInput)
	}
	schema, err := optionalIdentifier(stringDefault(cfg.String("schema"), defaultSchema))
	if err != nil {
		return options{}, err
	}
	warehouse, err := optionalIdentifier(cfg.String("warehouse"))
	if err != nil {
		return options{}, err
	}
	role, err := optionalIdentifier(cfg.String("role"))
	if err != nil {
		return options{}, err
	}

	opts := options{
		Account:         account,
		Host:            host,
		Port:            port,
		Warehouse:       warehouse,
		Role:            role,
		Database:        database,
		Schema:          schema,
		ReadOnly:        sqldb.BoolValue(cfg.Config["read_only"], true),
		RequireConfirm:  sqldb.BoolValue(cfg.Config["require_destructive_confirmation"], true),
		QueryTimeout:    sqldb.DurationValue(cfg.Config["query_timeout"], defaultTimeout),
		RowLimit:        boundedInt(cfg, "row_limit", defaultRowLimit, 1, plugin.MaxPageLimit),
		MaxConns:        boundedInt(cfg, "max_connections", defaultMaxConns, 1, 20),
		HistoryWindow:   boundedInt(cfg, "history_window", defaultHistoryWindow, 1, maxHistoryWindow),
		MetricsInterval: sqldb.DurationValue(cfg.Config["metrics_interval"], defaultMetricsInterval),
		ApplicationName: plugin.DefaultClientName + "_snowflake",
		RedactPatterns:  sqldb.ParsePatterns(cfg.String("redact_columns"), sqldb.DefaultRedactColumnPatterns()),
	}
	if opts.MetricsInterval < minMetricsInterval {
		opts.MetricsInterval = minMetricsInterval
	}
	if err := applyAuth(cfg, &opts); err != nil {
		return options{}, err
	}
	return opts, nil
}

// applyAuth resolves the selected authentication mode. Key pair material comes
// from a reusable credential and is parsed here so a bad key fails at connect
// time rather than mid-session.
func applyAuth(cfg plugin.ConnectConfig, opts *options) error {
	keyPairMode := cfg.String("auth") == authKeyPair || dbcred.ResolvedSecret(cfg, keyPairField) != ""
	if keyPairMode {
		username := strings.TrimSpace(cfg.String("username"))
		if identity := dbcred.ResolvedIdentity(cfg, keyPairField); identity != "" {
			username = identity
		}
		material := dbcred.ResolvedSecret(cfg, keyPairField)
		if material == "" {
			return fmt.Errorf("%w: key pair credential is required", plugin.ErrInvalidInput)
		}
		key, err := parsePrivateKey(material)
		if err != nil {
			return err
		}
		opts.Username = username
		opts.PrivateKey = key
	} else {
		auth := dbcred.ApplyPasswordCredential(cfg, cfg.String("username"), cfg.String("password"))
		opts.Username = auth.Username
		opts.Password = auth.Password
	}
	if strings.TrimSpace(opts.Username) == "" {
		return fmt.Errorf("%w: user is required", plugin.ErrInvalidInput)
	}
	if opts.PrivateKey == nil && opts.Password == "" {
		return fmt.Errorf("%w: password is required", plugin.ErrInvalidInput)
	}
	return nil
}

// parsePrivateKey decodes an RSA private key from a PEM bundle. Snowflake signs
// its JWT with an RSA key registered on the user; encrypted PEM blocks are
// rejected because neither this plugin nor the driver can unwrap PKCS#8
// encryption.
func parsePrivateKey(material string) (*rsa.PrivateKey, error) {
	rest := []byte(strings.TrimSpace(material))
	for {
		block, remainder := pem.Decode(rest)
		if block == nil {
			return nil, fmt.Errorf("%w: key pair credential is not PEM encoded", plugin.ErrInvalidInput)
		}
		rest = remainder
		switch block.Type {
		case "ENCRYPTED PRIVATE KEY":
			return nil, fmt.Errorf("%w: encrypted private keys are not supported; store a decrypted PKCS#8 key", plugin.ErrInvalidInput)
		case "RSA PRIVATE KEY":
			key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
			if err != nil {
				return nil, fmt.Errorf("%w: private key is not a valid PKCS#1 RSA key", plugin.ErrInvalidInput)
			}
			return key, nil
		case "PRIVATE KEY":
			parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
			if err != nil {
				return nil, fmt.Errorf("%w: private key is not a valid PKCS#8 key", plugin.ErrInvalidInput)
			}
			key, ok := parsed.(*rsa.PrivateKey)
			if !ok {
				return nil, fmt.Errorf("%w: Snowflake key pair authentication requires an RSA private key", plugin.ErrInvalidInput)
			}
			return key, nil
		}
		if len(rest) == 0 {
			return nil, fmt.Errorf("%w: key pair credential contains no private key block", plugin.ErrInvalidInput)
		}
	}
}

// driverConfig gives the driver the gateway transport, so every HTTP request,
// including the login and result fetches, egresses through the gateway rather
// than a dialer the plugin owns.
func driverConfig(opts options, netTransport plugin.NetTransport) (sf.Config, error) {
	if netTransport == nil {
		return sf.Config{}, fmt.Errorf("%w: network transport is unavailable", plugin.ErrUnavailable)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = netTransport.DialContext
	transport.Proxy = nil

	// Snowflake reads STATEMENT_TIMEOUT_IN_SECONDS as "no timeout" at zero, so a
	// sub-second query timeout must still floor at one second.
	statementTimeout := strconv.Itoa(max(1, int(opts.QueryTimeout.Seconds())))
	cfg := sf.Config{
		Account:     opts.Account,
		Host:        opts.Host,
		Port:        opts.Port,
		Protocol:    "https",
		User:        opts.Username,
		Database:    opts.Database,
		Schema:      opts.Schema,
		Warehouse:   opts.Warehouse,
		Role:        opts.Role,
		Application: opts.ApplicationName,
		Transporter: transport,
		Params: map[string]*string{
			"statement_timeout_in_seconds": &statementTimeout,
		},
		LoginTimeout:   opts.QueryTimeout,
		RequestTimeout: opts.QueryTimeout,
		ClientTimeout:  opts.QueryTimeout,
	}
	if opts.PrivateKey != nil {
		cfg.Authenticator = sf.AuthTypeJwt
		cfg.PrivateKey = opts.PrivateKey
	} else {
		cfg.Authenticator = sf.AuthTypeSnowflake
		cfg.Password = opts.Password
	}
	return cfg, nil
}

func boundedInt(cfg plugin.ConnectConfig, key string, fallback, low, high int) int {
	value, ok := cfg.Int(key)
	if !ok || value <= 0 {
		value = fallback
	}
	if value < low {
		value = low
	}
	if value > high {
		value = high
	}
	return value
}

func stringDefault(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return strings.TrimSpace(v)
}
