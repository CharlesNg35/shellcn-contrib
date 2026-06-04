package mssql

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"time"

	mssqldriver "github.com/microsoft/go-mssqldb"
	"github.com/microsoft/go-mssqldb/msdsn"

	"github.com/charlesng35/shellcn-contrib/shared/dbcred"
	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	defaultPort       = 1433
	defaultRowLimit   = 500
	defaultTimeout    = 30 * time.Second
	defaultMaxConns   = 4
	protocolName      = "mssql"
	credentialIDField = "credential_id"
	clientCertField   = "client_cert_id"
	authPassword      = "password"
	authCredential    = "credential"
)

type optionsData struct {
	Host              string
	Port              int
	Database          string
	Username          string
	Password          string
	EncryptMode       string
	CACertificate     string
	ClientCertificate string
	ReadOnly          bool
	RequireConfirm    bool
	QueryTimeout      time.Duration
	RowLimit          int
	MaxConns          int
	RedactPatterns    []string
}

func configSchema() plugin.Schema {
	passwordAuth := plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: authPassword}, {Field: credentialIDField, Op: plugin.OpEmpty}}}
	credentialAuth := plugin.Condition{AnyOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: authCredential}, {Field: credentialIDField, Op: plugin.OpNotEmpty}}}
	tlsEnabled := plugin.Condition{AllOf: []plugin.Rule{{Field: "encrypt", Op: plugin.OpNeq, Value: "disable"}}}
	verifyTLS := plugin.Condition{AnyOf: []plugin.Rule{
		{Field: "encrypt", Op: plugin.OpEq, Value: "verify-ca"},
		{Field: "encrypt", Op: plugin.OpEq, Value: "verify-full"},
	}}
	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Server", Fields: []plugin.Field{
			{Key: "host", Label: "Host", Type: plugin.FieldText, Required: true, Placeholder: "sqlserver.example.internal"},
			{Key: "port", Label: "Port", Type: plugin.FieldNumber, Required: true, Default: defaultPort, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 65535}}},
			{Key: "database", Label: "Default database", Type: plugin.FieldText, Required: true, Default: "master"},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: authPassword, Options: []plugin.Option{
				{Label: "Password", Value: authPassword},
				{Label: "Stored password", Value: authCredential},
			}},
			{Key: "username", Label: "Username", Type: plugin.FieldText, Required: true, Placeholder: "sa", VisibleWhen: &passwordAuth},
			{Key: credentialIDField, Label: "Stored password", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kinds: []plugin.CredentialKind{plugin.CredentialDBPassword}, Protocols: []string{protocolName},
			}, VisibleWhen: &credentialAuth, Help: "Reusable SQL Server password. The credential identity can also supply the username."},
			{Key: "password", Label: "Password", Type: plugin.FieldPassword, Secret: true, VisibleWhen: &passwordAuth},
		}},
		{Name: "TLS", Fields: []plugin.Field{
			{Key: "encrypt", Label: "Encryption", Type: plugin.FieldSelect, Required: true, Default: "require", Options: []plugin.Option{
				{Label: "Disable", Value: "disable"},
				{Label: "Require encryption", Value: "require"},
				{Label: "Verify CA", Value: "verify-ca"},
				{Label: "Verify full", Value: "verify-full"},
			}},
			{Key: "ca_certificate", Label: "CA certificate", Type: plugin.FieldTextarea, Secret: true, VisibleWhen: &verifyTLS, Help: "PEM CA bundle used for verify-ca and verify-full."},
			{Key: clientCertField, Label: "Client certificate", Type: plugin.FieldCredentialRef, Credential: &plugin.CredentialSelector{
				Kinds: []plugin.CredentialKind{plugin.CredentialTLSClientCert}, Protocols: []string{protocolName},
			}, VisibleWhen: &tlsEnabled, Help: "Optional PEM containing the client certificate and private key for TLS client authentication."},
		}},
		{Name: "Safety", Fields: []plugin.Field{
			{Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true, Help: "Blocks INSERT, UPDATE, DELETE, DDL, EXEC, TRUNCATE, GRANT, and other write statements."},
			{Key: "require_destructive_confirmation", Label: "Confirm destructive statements", Type: plugin.FieldToggle, Default: true, Help: "Requires explicit confirmation before write, DDL, EXEC, and privileged statements execute."},
			{Key: "query_timeout", Label: "Query timeout", Type: plugin.FieldDuration, Default: defaultTimeout.String()},
			{Key: "row_limit", Label: "Row limit", Type: plugin.FieldNumber, Default: defaultRowLimit, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: plugin.MaxPageLimit}}},
			{Key: "max_connections", Label: "Pool size", Type: plugin.FieldNumber, Default: defaultMaxConns, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 20}}},
			{Key: "redact_columns", Label: "Redacted columns", Type: plugin.FieldTextarea, Help: "Comma or newline separated regular expressions for result columns that must be masked."},
		}},
	}}
}

func parseOptions(cfg plugin.ConnectConfig) (optionsData, error) {
	host := strings.TrimSpace(cfg.String("host"))
	if host == "" {
		return optionsData{}, fmt.Errorf("%w: host is required", plugin.ErrInvalidInput)
	}
	port, ok := cfg.Int("port")
	if !ok || port == 0 {
		port = defaultPort
	}
	if port < 1 || port > 65535 {
		return optionsData{}, fmt.Errorf("%w: port must be between 1 and 65535", plugin.ErrInvalidInput)
	}
	database := strings.TrimSpace(cfg.String("database"))
	if database == "" {
		database = "master"
	}
	auth := dbcred.ApplyPasswordCredential(cfg, cfg.String("username"), cfg.String("password"))
	if auth.Username == "" {
		return optionsData{}, fmt.Errorf("%w: username is required", plugin.ErrInvalidInput)
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
	return optionsData{
		Host:              host,
		Port:              port,
		Database:          database,
		Username:          auth.Username,
		Password:          auth.Password,
		EncryptMode:       stringDefault(cfg.String("encrypt"), "require"),
		CACertificate:     cfg.String("ca_certificate"),
		ClientCertificate: dbcred.ResolvedSecret(cfg, clientCertField),
		ReadOnly:          sqldb.BoolValue(cfg.Config["read_only"], true),
		RequireConfirm:    sqldb.BoolValue(cfg.Config["require_destructive_confirmation"], true),
		QueryTimeout:      sqldb.DurationValue(cfg.Config["query_timeout"], defaultTimeout),
		RowLimit:          rowLimit,
		MaxConns:          maxConns,
		RedactPatterns:    sqldb.ParsePatterns(cfg.String("redact_columns"), sqldb.DefaultRedactColumnPatterns()),
	}, nil
}

func connector(opts optionsData, netTransport plugin.NetTransport) (*mssqldriver.Connector, error) {
	tlsConfig, encryption, trust, err := mssqlTLSConfig(opts)
	if err != nil {
		return nil, err
	}
	cfg := msdsn.Config{
		Host:                   opts.Host,
		Port:                   uint64(opts.Port),
		Database:               opts.Database,
		User:                   opts.Username,
		Password:               opts.Password,
		Encryption:             encryption,
		TLSConfig:              tlsConfig,
		TrustServerCertificate: trust,
		DialTimeout:            opts.QueryTimeout,
		ConnTimeout:            opts.QueryTimeout,
		AppName:                "ShellCN",
		Protocols:              []string{"tcp"},
	}
	c := mssqldriver.NewConnectorConfig(cfg)
	c.Dialer = netDialer{net: netTransport}
	return c, nil
}

type netDialer struct {
	net plugin.NetTransport
}

func (d netDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	return d.net.DialContext(ctx, network, addr)
}

func (d netDialer) HostName() string { return "" }

func mssqlTLSConfig(opts optionsData) (*tls.Config, msdsn.Encryption, bool, error) {
	switch opts.EncryptMode {
	case "", "disable":
		if opts.ClientCertificate != "" {
			return nil, 0, false, fmt.Errorf("%w: client certificate requires encryption", plugin.ErrInvalidInput)
		}
		return nil, msdsn.EncryptionDisabled, true, nil
	case "require", "verify-ca", "verify-full":
	default:
		return nil, 0, false, fmt.Errorf("%w: unsupported encryption mode %q", plugin.ErrInvalidInput, opts.EncryptMode)
	}
	cfg, err := sqldb.TLSConfig(sqldb.TLSOptions{
		Mode:              opts.EncryptMode,
		Host:              opts.Host,
		CACertificate:     opts.CACertificate,
		ClientCertificate: opts.ClientCertificate,
	})
	if err != nil {
		return nil, 0, false, err
	}
	trust := opts.EncryptMode == "require"
	return cfg, msdsn.EncryptionRequired, trust, nil
}

func stringDefault(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return strings.TrimSpace(v)
}
