package cassandra

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"

	"github.com/charlesng35/shellcn-contrib/shared/dbcred"
	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	protocolName      = "cassandra"
	defaultPort       = 9042
	defaultTimeout    = 30 * time.Second
	defaultPageSize   = 200
	defaultRowLimit   = 500
	defaultNumConns   = 2
	credentialIDField = "credential_id"
	clientCertField   = "client_cert_id"
	authNone          = "none"
	authPassword      = "password"
	authCredential    = "credential"
)

type options struct {
	Hosts             []string
	Port              int
	Keyspace          string
	Username          string
	Password          string
	Consistency       gocql.Consistency
	LocalDC           string
	TLSMode           string
	CACertificate     string
	ClientCertificate string
	ReadOnly          bool
	RequireConfirm    bool
	QueryTimeout      time.Duration
	PageSize          int
	RowLimit          int
	NumConns          int
	RedactPatterns    []string
}

func configSchema() plugin.Schema {
	passwordAuth := plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: authPassword}, {Field: credentialIDField, Op: plugin.OpEmpty}}}
	credentialAuth := plugin.Condition{AnyOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: authCredential}, {Field: credentialIDField, Op: plugin.OpNotEmpty}}}
	tlsEnabled := plugin.Condition{AllOf: []plugin.Rule{{Field: "tls_mode", Op: plugin.OpNeq, Value: "disable"}}}
	verifyTLS := plugin.Condition{AnyOf: []plugin.Rule{
		{Field: "tls_mode", Op: plugin.OpEq, Value: "verify-ca"},
		{Field: "tls_mode", Op: plugin.OpEq, Value: "verify-full"},
	}}
	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Cluster", Fields: []plugin.Field{
			{Key: "hosts", Label: "Contact points", Type: plugin.FieldTextarea, Required: true, Placeholder: "10.0.0.10\n10.0.0.11", Help: "Comma or newline separated Cassandra contact points."},
			{Key: "port", Label: "Port", Type: plugin.FieldNumber, Required: true, Default: defaultPort, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 65535}}},
			{Key: "keyspace", Label: "Default keyspace", Type: plugin.FieldText, Placeholder: "app"},
			{Key: "local_dc", Label: "Local datacenter", Type: plugin.FieldText, Help: "Enables DC-aware token-aware routing when set."},
			{Key: "consistency", Label: "Consistency", Type: plugin.FieldSelect, Required: true, Default: "LOCAL_QUORUM", Options: []plugin.Option{
				{Label: "LOCAL_QUORUM", Value: "LOCAL_QUORUM"},
				{Label: "LOCAL_ONE", Value: "LOCAL_ONE"},
				{Label: "ONE", Value: "ONE"},
				{Label: "QUORUM", Value: "QUORUM"},
				{Label: "EACH_QUORUM", Value: "EACH_QUORUM"},
				{Label: "ALL", Value: "ALL"},
			}},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: authNone, Options: []plugin.Option{
				{Label: "None", Value: authNone},
				{Label: "Password", Value: authPassword},
				{Label: "Stored password", Value: authCredential},
			}},
			{Key: "username", Label: "Username", Type: plugin.FieldText, VisibleWhen: &passwordAuth},
			{Key: credentialIDField, Label: "Stored password", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialDBPassword, Protocols: []string{protocolName},
			}, VisibleWhen: &credentialAuth, Help: "Reusable Cassandra password. The credential identity can also supply the username."},
			{Key: "password", Label: "Password", Type: plugin.FieldPassword, Secret: true, VisibleWhen: &passwordAuth},
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
			}, VisibleWhen: &tlsEnabled, Help: "Optional PEM containing the client certificate and private key for mTLS."},
		}},
		{Name: "Safety", Fields: []plugin.Field{
			{Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true, Help: "Blocks INSERT, UPDATE, DELETE, BATCH, DDL, TRUNCATE, and privilege changes."},
			{Key: "require_destructive_confirmation", Label: "Confirm destructive statements", Type: plugin.FieldToggle, Default: true, Help: "Requires explicit confirmation before writes, DDL, batches, truncates, and privilege changes execute."},
			{Key: "query_timeout", Label: "Query timeout", Type: plugin.FieldDuration, Default: defaultTimeout.String()},
			{Key: "page_size", Label: "Page size", Type: plugin.FieldNumber, Default: defaultPageSize, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: plugin.MaxPageLimit}}},
			{Key: "row_limit", Label: "Row limit", Type: plugin.FieldNumber, Default: defaultRowLimit, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: plugin.MaxPageLimit}}},
			{Key: "connections_per_host", Label: "Connections per host", Type: plugin.FieldNumber, Default: defaultNumConns, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 16}}},
			{Key: "redact_columns", Label: "Redacted columns", Type: plugin.FieldTextarea, Help: "Comma or newline separated regular expressions for result columns that must be masked."},
		}},
	}}
}

func parseOptions(cfg plugin.ConnectConfig) (options, error) {
	hosts := parseHosts(cfg.String("hosts"))
	if len(hosts) == 0 {
		return options{}, fmt.Errorf("%w: at least one contact point is required", plugin.ErrInvalidInput)
	}
	port, ok := cfg.Int("port")
	if !ok || port == 0 {
		port = defaultPort
	}
	if port < 1 || port > 65535 {
		return options{}, fmt.Errorf("%w: port must be between 1 and 65535", plugin.ErrInvalidInput)
	}
	keyspace := strings.TrimSpace(cfg.String("keyspace"))
	if keyspace != "" {
		if _, err := sqldb.SafeIdentifier(keyspace); err != nil {
			return options{}, err
		}
	}
	auth := dbcred.AuthMaterial{}
	switch strings.TrimSpace(cfg.String("auth")) {
	case "", authNone:
	case authPassword, authCredential:
		auth = dbcred.ApplyPasswordCredential(cfg, cfg.String("username"), cfg.String("password"))
		if auth.Username == "" {
			return options{}, fmt.Errorf("%w: username is required", plugin.ErrInvalidInput)
		}
	default:
		return options{}, fmt.Errorf("%w: unsupported authentication method", plugin.ErrInvalidInput)
	}
	consistency, err := parseConsistency(cfg.String("consistency"))
	if err != nil {
		return options{}, err
	}
	tlsMode := stringDefault(cfg.String("tls_mode"), "disable")
	clientCertificate := dbcred.ResolvedClientCertificate(cfg, clientCertField)
	if clientCertificate != "" && tlsMode == "disable" {
		return options{}, fmt.Errorf("%w: client certificate requires TLS", plugin.ErrInvalidInput)
	}
	pageSize, ok := cfg.Int("page_size")
	if !ok || pageSize <= 0 {
		pageSize = defaultPageSize
	}
	if pageSize > plugin.MaxPageLimit {
		pageSize = plugin.MaxPageLimit
	}
	rowLimit, ok := cfg.Int("row_limit")
	if !ok || rowLimit <= 0 {
		rowLimit = defaultRowLimit
	}
	if rowLimit > plugin.MaxPageLimit {
		rowLimit = plugin.MaxPageLimit
	}
	numConns, ok := cfg.Int("connections_per_host")
	if !ok || numConns <= 0 {
		numConns = defaultNumConns
	}
	if numConns > 16 {
		numConns = 16
	}
	return options{
		Hosts:             hosts,
		Port:              port,
		Keyspace:          keyspace,
		Username:          auth.Username,
		Password:          auth.Password,
		Consistency:       consistency,
		LocalDC:           strings.TrimSpace(cfg.String("local_dc")),
		TLSMode:           tlsMode,
		CACertificate:     cfg.String("ca_certificate"),
		ClientCertificate: clientCertificate,
		ReadOnly:          sqldb.BoolValue(cfg.Config["read_only"], true),
		RequireConfirm:    sqldb.BoolValue(cfg.Config["require_destructive_confirmation"], true),
		QueryTimeout:      sqldb.DurationValue(cfg.Config["query_timeout"], defaultTimeout),
		PageSize:          pageSize,
		RowLimit:          rowLimit,
		NumConns:          numConns,
		RedactPatterns:    sqldb.ParsePatterns(cfg.String("redact_columns"), sqldb.DefaultRedactColumnPatterns()),
	}, nil
}

func clusterConfig(opts options, netTransport plugin.NetTransport) (*gocql.ClusterConfig, error) {
	hosts := make([]string, 0, len(opts.Hosts))
	for _, host := range opts.Hosts {
		if _, _, err := net.SplitHostPort(host); err == nil {
			hosts = append(hosts, host)
			continue
		}
		hosts = append(hosts, net.JoinHostPort(host, strconv.Itoa(opts.Port)))
	}
	cluster := gocql.NewCluster(hosts...)
	cluster.Port = opts.Port
	cluster.Keyspace = opts.Keyspace
	cluster.Consistency = opts.Consistency
	cluster.Timeout = opts.QueryTimeout
	cluster.ConnectTimeout = opts.QueryTimeout
	cluster.WriteTimeout = opts.QueryTimeout
	cluster.NumConns = opts.NumConns
	cluster.PageSize = opts.PageSize
	cluster.Dialer = netTransport
	cluster.DisableInitialHostLookup = false
	cluster.DefaultTimestamp = true
	fallback := gocql.RoundRobinHostPolicy()
	if opts.LocalDC != "" {
		fallback = gocql.DCAwareRoundRobinPolicy(opts.LocalDC)
	}
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)
	if opts.Username != "" || opts.Password != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{Username: opts.Username, Password: opts.Password}
	}
	tlsConfig, err := sqldb.TLSConfig(sqldb.TLSOptions{
		Mode:              opts.TLSMode,
		Host:              firstHost(opts.Hosts),
		CACertificate:     opts.CACertificate,
		ClientCertificate: opts.ClientCertificate,
	})
	if err != nil {
		return nil, err
	}
	if tlsConfig != nil {
		cluster.SslOpts = &gocql.SslOptions{Config: tlsConfig, EnableHostVerification: opts.TLSMode == "verify-full"}
	}
	return cluster, nil
}

func parseHosts(raw string) []string {
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == '\n' || r == '\r' || r == '\t' || r == ' '
	})
	hosts := make([]string, 0, len(parts))
	seen := map[string]bool{}
	for _, part := range parts {
		host := strings.TrimSpace(part)
		if host == "" || seen[host] {
			continue
		}
		seen[host] = true
		hosts = append(hosts, host)
	}
	return hosts
}

func parseConsistency(raw string) (gocql.Consistency, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		value = "LOCAL_QUORUM"
	}
	consistency, err := gocql.ParseConsistencyWrapper(value)
	if err != nil {
		return 0, fmt.Errorf("%w: unsupported consistency %q", plugin.ErrInvalidInput, value)
	}
	return consistency, nil
}

func firstHost(hosts []string) string {
	if len(hosts) == 0 {
		return ""
	}
	host := hosts[0]
	if h, _, err := net.SplitHostPort(host); err == nil {
		return h
	}
	return host
}

func stringDefault(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return strings.TrimSpace(v)
}
