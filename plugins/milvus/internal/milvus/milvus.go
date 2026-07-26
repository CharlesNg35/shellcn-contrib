// Package milvus implements the Milvus vector database protocol plugin.
package milvus

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"google.golang.org/grpc"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn-contrib/shared/dbcred"
	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	protocolName      = "milvus"
	defaultPort       = 19530
	defaultDatabase   = "default"
	defaultTimeout    = 15 * time.Second
	defaultPageLimit  = 100
	defaultSampleSize = 512
	maxSampleSize     = 4096

	credentialIDField = "credential_id"
	tokenIDField      = "token_credential_id"
	clientCertField   = "client_cert_id"

	authNone       = "none"
	authPassword   = "password"
	authToken      = "token"
	authCredential = "credential"
	authAPIToken   = "api_token"
)

// identifierPattern matches the Milvus naming rule for databases, collections,
// partitions, fields, indexes, users and roles.
var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,254}$`)

const svgIcon = `<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 600 600"><path fill="#4FC4F9" d="M135.8 155.5a203 203 0 0 1 288.1 0 204.6 204.6 0 0 1 0 289 203.3 203.3 0 0 1-288.1 0L5.8 313.8a19.7 19.7 0 0 1 0-27.8zM393.1 194a149.3 149.3 0 0 0-211.7 0l-95.6 95.8c-5.6 5.8-5.6 15 0 20.6l95.7 96a149.3 149.3 0 0 0 211.7 0 150.4 150.4 0 0 0-.1-212.4M288 197.7c55.4 0 100.3 45.9 100.3 102.5S343.4 402.7 288 402.7a101.4 101.4 0 0 1-100.3-102.5c0-56.6 44.9-102.5 100.3-102.5m306.3 88.5-57.2-58.5a4.8 4.8 0 0 0-8.1 4.5 315 315 0 0 1 0 136c-1 4.8 4.7 8 8 4.5l57.3-58.5a20 20 0 0 0 1.3-26.6z"/></svg>`

type Plugin struct{}

type Options struct {
	Host        string
	Port        int
	Database    string
	Username    string
	Password    string
	Token       string
	TLSConfig   *tls.Config
	Timeout     time.Duration
	PageLimit   int
	SampleLimit int
	ReadOnly    bool
}

type Session struct {
	opts     Options
	net      plugin.NetTransport
	activity *activityLog

	mu      sync.Mutex
	clients map[string]*milvusclient.Client
	closed  bool
}

type row = plugin.TableRow

func New() plugin.Plugin { return Plugin{} }

func (Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:  plugin.CurrentAPIVersion,
		Name:        protocolName,
		Version:     "0.1.0",
		Title:       "Milvus",
		Description: "Milvus vector database cockpit: collections, partitions, schema design, similarity search, embedding projection, index builds and RBAC.",
		Icon:        plugin.Icon{Type: plugin.IconSVG, Value: svgIcon},
		Category:    plugin.CategoryDatabases,
		Config:      configSchema(),
		Capabilities: []plugin.Capability{
			"collections", "partitions", "schema", "vector-search", "full-text-search",
			"indexes", "segments", "metrics", "rbac", "saved-queries", "embedding-projection",
		},
		SupportedTransports: []plugin.Transport{plugin.TransportDirect, plugin.TransportAgent},
		Agent: &plugin.AgentProfile{
			Proxy: plugin.ProxyTarget{Mode: plugin.AgentTCP, Address: "127.0.0.1:19530", Risk: plugin.RiskPrivileged},
			Install: []plugin.InstallArtifact{{
				Label:    "Docker",
				Kind:     "docker-run",
				Template: "docker run -d --restart=always --network host shellcn/agent:latest connect {{shellquote .ConnectURL}} {{shellquote .Token}}",
			}},
		},
		Layout:    plugin.LayoutSidebarTree,
		Scope:     scopeFilters(),
		Tree:      tree(),
		Resources: resources(),
		Actions:   actions(),
		Streams:   streams(),
	}
}

func (Plugin) Routes() []plugin.Route { return Routes() }

func (Plugin) Connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	opts, err := parseOptions(cfg)
	if err != nil {
		return nil, err
	}
	s := &Session{
		opts:     opts,
		net:      cfg.Net,
		activity: newActivityLog(200),
		clients:  map[string]*milvusclient.Client{},
	}
	if _, err := s.clientFor(ctx, opts.Database); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Session) HealthCheck(ctx context.Context) error {
	c, err := s.clientFor(ctx, s.opts.Database)
	if err != nil {
		return err
	}
	if _, err := c.GetServerVersion(ctx, milvusclient.NewGetServerVersionOption()); err != nil {
		return mapError(err)
	}
	return nil
}

func (s *Session) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}

func (s *Session) Close() error {
	s.mu.Lock()
	clients := s.clients
	s.clients = map[string]*milvusclient.Client{}
	s.closed = true
	s.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, c := range clients {
		_ = c.Close(ctx)
	}
	return nil
}

// clientFor returns the per-database client, opening it on first use so a
// connection can span every database without reconnecting eagerly.
func (s *Session) clientFor(ctx context.Context, database string) (*milvusclient.Client, error) {
	database = strings.TrimSpace(database)
	if database == "" {
		database = s.opts.Database
	}
	if !identifierPattern.MatchString(database) {
		return nil, fmt.Errorf("%w: invalid database name %q", plugin.ErrInvalidInput, database)
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, fmt.Errorf("%w: session is closed", plugin.ErrUnavailable)
	}
	if c, ok := s.clients[database]; ok {
		s.mu.Unlock()
		return c, nil
	}
	s.mu.Unlock()

	dialCtx, cancel := context.WithTimeout(ctx, s.opts.Timeout)
	defer cancel()
	c, err := milvusclient.New(dialCtx, s.clientConfig(database))
	if err != nil {
		return nil, mapError(err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		go func() { _ = c.Close(context.Background()) }()
		return nil, fmt.Errorf("%w: session is closed", plugin.ErrUnavailable)
	}
	if existing, ok := s.clients[database]; ok {
		go func() { _ = c.Close(context.Background()) }()
		return existing, nil
	}
	s.clients[database] = c
	return c, nil
}

// forget drops a cached client so the next request reconnects; used when a
// database is dropped or a connection turns out to be dead.
func (s *Session) forget(database string) {
	s.mu.Lock()
	c := s.clients[database]
	delete(s.clients, database)
	s.mu.Unlock()
	if c != nil {
		go func() { _ = c.Close(context.Background()) }()
	}
}

func (s *Session) clientConfig(database string) *milvusclient.ClientConfig {
	cfg := &milvusclient.ClientConfig{
		Address:  net.JoinHostPort(s.opts.Host, strconv.Itoa(s.opts.Port)),
		Username: s.opts.Username,
		Password: s.opts.Password,
		APIKey:   s.opts.Token,
		DBName:   database,
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
				return s.net.DialContext(ctx, "tcp", addr)
			}),
		},
	}
	if s.opts.TLSConfig != nil {
		cfg = cfg.WithTLSConfig(s.opts.TLSConfig.Clone())
	}
	return cfg
}

func parseOptions(cfg plugin.ConnectConfig) (Options, error) {
	host := strings.TrimSpace(cfg.String("host"))
	if cfg.Transport == plugin.TransportAgent && host == "" {
		host = "127.0.0.1"
	}
	if host == "" {
		return Options{}, fmt.Errorf("%w: host is required", plugin.ErrInvalidInput)
	}
	port, ok := cfg.Int("port")
	if !ok || port == 0 {
		port = defaultPort
	}
	if port < 1 || port > 65535 {
		return Options{}, fmt.Errorf("%w: port must be between 1 and 65535", plugin.ErrInvalidInput)
	}
	database := strings.TrimSpace(cfg.String("database"))
	if database == "" {
		database = defaultDatabase
	}
	if !identifierPattern.MatchString(database) {
		return Options{}, fmt.Errorf("%w: invalid database name %q", plugin.ErrInvalidInput, database)
	}

	opts := Options{
		Host:        host,
		Port:        port,
		Database:    database,
		Timeout:     broker.DurationValue(cfg.Config, "timeout", defaultTimeout),
		PageLimit:   broker.IntValue(cfg.Config, "page_limit", defaultPageLimit, 10, plugin.MaxPageLimit),
		SampleLimit: broker.IntValue(cfg.Config, "sample_limit", defaultSampleSize, 32, maxSampleSize),
		ReadOnly:    broker.BoolValue(cfg.Config, "read_only", true),
	}

	switch auth := broker.StringValue(cfg.Config, "auth", authNone); auth {
	case authNone:
	case authPassword:
		opts.Username = strings.TrimSpace(cfg.String("username"))
		opts.Password = cfg.String("password")
		if opts.Username == "" {
			return Options{}, fmt.Errorf("%w: username is required for password authentication", plugin.ErrInvalidInput)
		}
	case authToken:
		opts.Token = strings.TrimSpace(cfg.String("token"))
		if opts.Token == "" {
			return Options{}, fmt.Errorf("%w: token is required for token authentication", plugin.ErrInvalidInput)
		}
	case authCredential:
		material := dbcred.ApplyPasswordCredential(cfg, "", "")
		opts.Username, opts.Password = material.Username, material.Password
		if opts.Username == "" || opts.Password == "" {
			return Options{}, fmt.Errorf("%w: stored credential must carry a username and password", plugin.ErrInvalidInput)
		}
	case authAPIToken:
		if kind := cfg.CredentialKindFor(tokenIDField); kind != "" && kind != plugin.CredentialKindAPIToken {
			return Options{}, fmt.Errorf("%w: Milvus stored tokens must be API tokens", plugin.ErrInvalidInput)
		}
		opts.Token = dbcred.ResolvedSecret(cfg, tokenIDField)
		if opts.Token == "" {
			return Options{}, fmt.Errorf("%w: stored API token is empty", plugin.ErrInvalidInput)
		}
	default:
		return Options{}, fmt.Errorf("%w: unsupported authentication mode %q", plugin.ErrInvalidInput, auth)
	}

	tlsMode := broker.StringValue(cfg.Config, "tls_mode", "disable")
	tlsConfig, err := sqldb.TLSConfig(sqldb.TLSOptions{
		Mode:              tlsMode,
		Host:              host,
		CACertificate:     cfg.String("ca_certificate"),
		ClientCertificate: dbcred.ApplyClientCertificateCredential(cfg, clientCertField, "", tlsMode, "").ClientCertificate,
	})
	if err != nil {
		return Options{}, err
	}
	opts.TLSConfig = tlsConfig
	return opts, nil
}

func configSchema() plugin.Schema {
	directOnly := plugin.Condition{AllOf: []plugin.Rule{
		{Field: plugin.SchemaContextTransport, Op: plugin.OpEq, Value: string(plugin.TransportDirect)},
	}}
	passwordAuth := plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: authPassword}}}
	tokenAuth := plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: authToken}}}
	credentialAuth := plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: authCredential}}}
	apiTokenAuth := plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: authAPIToken}}}
	tlsEnabled := plugin.Condition{AllOf: []plugin.Rule{{Field: "tls_mode", Op: plugin.OpNeq, Value: "disable"}}}
	verifyTLS := plugin.Condition{AnyOf: []plugin.Rule{
		{Field: "tls_mode", Op: plugin.OpEq, Value: "verify-ca"},
		{Field: "tls_mode", Op: plugin.OpEq, Value: "verify-full"},
	}}

	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Server", Fields: []plugin.Field{
			{Key: "host", Label: "Host", Type: plugin.FieldText, Required: true, Placeholder: "milvus.example.internal", VisibleWhen: &directOnly},
			{Key: "port", Label: "Port", Type: plugin.FieldNumber, Required: true, Default: defaultPort, VisibleWhen: &directOnly, Validators: []plugin.Validator{
				{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 65535},
			}},
			{Key: "database", Label: "Default database", Type: plugin.FieldText, Default: defaultDatabase, Help: "Database selected when the workspace opens.", Validators: []plugin.Validator{
				{Type: plugin.ValidatorRegex, Value: identifierPattern.String(), Message: "letters, digits and underscores, starting with a letter or underscore"},
			}},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: authNone, Options: []plugin.Option{
				{Label: "None", Value: authNone},
				{Label: "Username and password", Value: authPassword},
				{Label: "API token", Value: authToken},
				{Label: "Stored password", Value: authCredential},
				{Label: "Stored API token", Value: authAPIToken},
			}},
			{Key: "username", Label: "Username", Type: plugin.FieldText, Required: true, Placeholder: "root", VisibleWhen: &passwordAuth},
			{Key: "password", Label: "Password", Type: plugin.FieldPassword, Secret: true, Required: true, VisibleWhen: &passwordAuth},
			{Key: "token", Label: "API token", Type: plugin.FieldPassword, Secret: true, Required: true, VisibleWhen: &tokenAuth, Help: "Zilliz Cloud style token, or username:password."},
			{Key: credentialIDField, Label: "Stored password", Type: plugin.FieldCredentialRef, Required: true, VisibleWhen: &credentialAuth, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindDBPassword, Protocols: []string{protocolName},
			}, Help: "Reusable credential providing the Milvus username and password."},
			{Key: tokenIDField, Label: "Stored API token", Type: plugin.FieldCredentialRef, Required: true, VisibleWhen: &apiTokenAuth, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindAPIToken, Protocols: []string{protocolName},
			}},
		}},
		{Name: "TLS", Fields: []plugin.Field{
			{Key: "tls_mode", Label: "TLS mode", Type: plugin.FieldSelect, Required: true, Default: "disable", Options: []plugin.Option{
				{Label: "Disable", Value: "disable"},
				{Label: "Require encryption", Value: "require"},
				{Label: "Verify CA", Value: "verify-ca"},
				{Label: "Verify full", Value: "verify-full"},
			}},
			{Key: "ca_certificate", Label: "CA certificate", Type: plugin.FieldTextarea, Secret: true, VisibleWhen: &verifyTLS, Help: "PEM CA bundle used for verify-ca and verify-full."},
			{Key: clientCertField, Label: "Client certificate", Type: plugin.FieldCredentialRef, VisibleWhen: &tlsEnabled, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindTLSClientCert, Protocols: []string{protocolName},
			}, Help: "Optional mTLS certificate and private key."},
		}},
		{Name: "Safety", Fields: []plugin.Field{
			{Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true, Help: "Blocks every schema, entity, index and RBAC mutation."},
			{Key: "timeout", Label: "Request timeout", Type: plugin.FieldDuration, Default: defaultTimeout.String()},
			{Key: "page_limit", Label: "Page size", Type: plugin.FieldNumber, Default: defaultPageLimit, Validators: []plugin.Validator{
				{Type: plugin.ValidatorMin, Value: 10}, {Type: plugin.ValidatorMax, Value: plugin.MaxPageLimit},
			}},
			{Key: "sample_limit", Label: "Projection sample size", Type: plugin.FieldNumber, Default: defaultSampleSize, Help: "Vectors sampled by the embedding projection explorer.", Validators: []plugin.Validator{
				{Type: plugin.ValidatorMin, Value: 32}, {Type: plugin.ValidatorMax, Value: maxSampleSize},
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
	return nil, fmt.Errorf("%w: no Milvus session", plugin.ErrInvalidInput)
}
