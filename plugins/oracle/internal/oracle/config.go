package oracle

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	go_ora "github.com/sijms/go-ora/v2"

	"github.com/charlesng35/shellcn-contrib/shared/dbcred"
	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	defaultPort       = 1521
	defaultRowLimit   = 500
	defaultTimeout    = 30 * time.Second
	defaultMaxConns   = 4
	protocolName      = "oracle"
	credentialIDField = "credential_id"
	authCertField     = "auth_client_cert_id"
	clientCertField   = "client_cert_id"
	authPassword      = "password"
	authCredential    = "credential"
	authClientCert    = "client_certificate"
)

type optionsData struct {
	Host              string
	Port              int
	Service           string
	SID               string
	Username          string
	Password          string
	TLSMode           string
	CACertificate     string
	ClientCertificate string
	TCPSAuth          bool
	DBAPrivilege      string
	ReadOnly          bool
	RequireConfirm    bool
	QueryTimeout      time.Duration
	RowLimit          int
	MaxConns          int
	RedactPatterns    []string
}

func configSchema() plugin.Schema {
	passwordAuth := plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: authPassword}, {Field: credentialIDField, Op: plugin.OpEmpty}, {Field: authCertField, Op: plugin.OpEmpty}}}
	credentialAuth := plugin.Condition{AnyOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: authCredential}, {Field: credentialIDField, Op: plugin.OpNotEmpty}}}
	optionalClientCertificate := plugin.Condition{AllOf: []plugin.Rule{{Field: "tls_mode", Op: plugin.OpNeq, Value: "disable"}, {Field: "auth", Op: plugin.OpNeq, Value: authClientCert}}}
	verifyTLS := plugin.Condition{AnyOf: []plugin.Rule{
		{Field: "tls_mode", Op: plugin.OpEq, Value: "verify-ca"},
		{Field: "tls_mode", Op: plugin.OpEq, Value: "verify-full"},
	}}
	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Server", Fields: []plugin.Field{
			{Key: "host", Label: "Host", Type: plugin.FieldText, Required: true, Placeholder: "oracle.example.internal"},
			{Key: "port", Label: "Port", Type: plugin.FieldNumber, Required: true, Default: defaultPort, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 65535}}},
			{Key: "service", Label: "Service name", Type: plugin.FieldText, Required: true, Default: "FREEPDB1"},
			{Key: "sid", Label: "SID", Type: plugin.FieldText, Help: "Optional SID. When set it is used instead of service name."},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: authPassword, Options: []plugin.Option{
				{Label: "Password", Value: authPassword},
				{Label: "Stored password", Value: authCredential},
				{Label: "Client certificate", Value: authClientCert},
			}},
			{Key: "username", Label: "Username", Type: plugin.FieldText, Required: true, Placeholder: "SYSTEM", VisibleWhen: &passwordAuth},
			{Key: credentialIDField, Label: "Stored password", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialDBPassword, Protocols: []string{protocolName},
			}, VisibleWhen: &credentialAuth, Help: "Reusable Oracle password. The credential identity can also supply the username."},
			{Key: authCertField, Label: "Client certificate", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialTLSClientCert, Protocols: []string{protocolName},
			}, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: authClientCert}}}, Help: "Reusable client certificate and private key used for TCPS external authentication."},
			{Key: "password", Label: "Password", Type: plugin.FieldPassword, Secret: true, VisibleWhen: &passwordAuth},
			{Key: "dba_privilege", Label: "DBA privilege", Type: plugin.FieldSelect, Default: "", Options: []plugin.Option{
				{Label: "None", Value: ""},
				{Label: "SYSDBA", Value: "SYSDBA"},
				{Label: "SYSOPER", Value: "SYSOPER"},
			}, Help: "Only use privileged modes for dedicated administrative connections."},
		}},
		{Name: "TLS", Fields: []plugin.Field{
			{Key: "tls_mode", Label: "TLS mode", Type: plugin.FieldSelect, Required: true, Default: "disable", Options: []plugin.Option{
				{Label: "Disable", Value: "disable"},
				{Label: "Require encryption", Value: "require"},
				{Label: "Verify CA", Value: "verify-ca"},
				{Label: "Verify full", Value: "verify-full"},
			}},
			{Key: "ca_certificate", Label: "CA certificate", Type: plugin.FieldTextarea, Secret: true, VisibleWhen: &verifyTLS, Help: "PEM CA bundle used for verify-ca and verify-full."},
			{Key: clientCertField, Label: "Client certificate", Type: plugin.FieldCredentialRef, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialTLSClientCert, Protocols: []string{protocolName},
			}, VisibleWhen: &optionalClientCertificate, Help: "Optional PEM containing the client certificate and private key for TCPS client authentication."},
		}},
		{Name: "Safety", Fields: []plugin.Field{
			{Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true, Help: "Blocks INSERT, UPDATE, DELETE, MERGE, PL/SQL blocks, DDL, TRUNCATE, GRANT, and other write statements."},
			{Key: "require_destructive_confirmation", Label: "Confirm destructive statements", Type: plugin.FieldToggle, Default: true, Help: "Requires explicit confirmation before write, DDL, PL/SQL, and privileged statements execute."},
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
	serviceName := strings.TrimSpace(cfg.String("service"))
	sid := strings.TrimSpace(cfg.String("sid"))
	if serviceName == "" && sid == "" {
		serviceName = "FREEPDB1"
	}
	tlsMode := stringDefault(cfg.String("tls_mode"), "disable")
	auth := dbcred.ApplyPasswordCredential(cfg, cfg.String("username"), cfg.String("password"))
	clientCertificate := dbcred.ResolvedClientCertificate(cfg, clientCertField)
	tcpsAuth := cfg.String("auth") == authClientCert || dbcred.ResolvedClientCertificate(cfg, authCertField) != ""
	if tcpsAuth {
		certAuth := dbcred.ApplyClientCertificateCredential(cfg, authCertField, "", tlsMode, "")
		auth.Username = ""
		auth.Password = ""
		tlsMode = certAuth.TLSMode
		clientCertificate = certAuth.ClientCertificate
	}
	if tcpsAuth && clientCertificate == "" {
		return optionsData{}, fmt.Errorf("%w: client certificate is required", plugin.ErrInvalidInput)
	}
	if !tcpsAuth && auth.Username == "" {
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
		Service:           serviceName,
		SID:               sid,
		Username:          auth.Username,
		Password:          auth.Password,
		TLSMode:           tlsMode,
		CACertificate:     cfg.String("ca_certificate"),
		ClientCertificate: clientCertificate,
		TCPSAuth:          tcpsAuth,
		DBAPrivilege:      strings.ToUpper(strings.TrimSpace(cfg.String("dba_privilege"))),
		ReadOnly:          sqldb.BoolValue(cfg.Config["read_only"], true),
		RequireConfirm:    sqldb.BoolValue(cfg.Config["require_destructive_confirmation"], true),
		QueryTimeout:      sqldb.DurationValue(cfg.Config["query_timeout"], defaultTimeout),
		RowLimit:          rowLimit,
		MaxConns:          maxConns,
		RedactPatterns:    sqldb.ParsePatterns(cfg.String("redact_columns"), sqldb.DefaultRedactColumnPatterns()),
	}, nil
}

func openDB(opts optionsData, netTransport plugin.NetTransport) (*sql.DB, error) {
	urlOptions := map[string]string{
		"CONNECT TIMEOUT": strconv.Itoa(int(opts.QueryTimeout.Seconds())),
		"TIMEOUT":         strconv.Itoa(int(opts.QueryTimeout.Seconds())),
		"PREFETCH_ROWS":   strconv.Itoa(opts.RowLimit),
	}
	if opts.SID != "" {
		urlOptions["SID"] = opts.SID
	}
	if opts.DBAPrivilege != "" {
		switch opts.DBAPrivilege {
		case "SYSDBA", "SYSOPER":
			urlOptions["DBA PRIVILEGE"] = opts.DBAPrivilege
		default:
			return nil, fmt.Errorf("%w: unsupported DBA privilege %q", plugin.ErrInvalidInput, opts.DBAPrivilege)
		}
	}
	if opts.TCPSAuth {
		urlOptions["AUTH TYPE"] = "TCPS"
	}
	tlsConfig, ssl, verify, err := oracleTLSConfig(opts)
	if err != nil {
		return nil, err
	}
	if ssl {
		urlOptions["SSL"] = "true"
	}
	if verify {
		urlOptions["SSL VERIFY"] = "true"
	}
	connector, ok := go_ora.NewConnector(go_ora.BuildUrl(opts.Host, opts.Port, opts.Service, opts.Username, opts.Password, urlOptions)).(*go_ora.OracleConnector)
	if !ok {
		return nil, fmt.Errorf("%w: Oracle connector type is unavailable", plugin.ErrUnavailable)
	}
	connector.Dialer(netDialer{net: netTransport})
	if tlsConfig != nil {
		connector.WithTLSConfig(tlsConfig)
	}
	return sql.OpenDB(connector), nil
}

type netDialer struct {
	net plugin.NetTransport
}

func (d netDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	return d.net.DialContext(ctx, network, addr)
}

func oracleTLSConfig(opts optionsData) (*tls.Config, bool, bool, error) {
	switch opts.TLSMode {
	case "", "disable":
		if opts.ClientCertificate != "" {
			return nil, false, false, fmt.Errorf("%w: client certificate requires TLS", plugin.ErrInvalidInput)
		}
		return nil, false, false, nil
	case "require", "verify-ca", "verify-full":
	default:
		return nil, false, false, fmt.Errorf("%w: unsupported TLS mode %q", plugin.ErrInvalidInput, opts.TLSMode)
	}
	cfg, err := sqldb.TLSConfig(sqldb.TLSOptions{
		Mode:              opts.TLSMode,
		Host:              opts.Host,
		CACertificate:     opts.CACertificate,
		ClientCertificate: opts.ClientCertificate,
	})
	if err != nil {
		return nil, false, false, err
	}
	verify := opts.TLSMode == "verify-ca" || opts.TLSMode == "verify-full"
	return cfg, true, verify, nil
}

func stringDefault(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return strings.TrimSpace(v)
}
