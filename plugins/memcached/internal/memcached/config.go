package memcached

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/dbcred"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	protocolName = "memcached"
	defaultPort  = 11211

	defaultTimeout       = 5 * time.Second
	defaultIdleConns     = 4
	defaultPageLimit     = 200
	defaultScanLimit     = 5000
	defaultValueLimit    = 64 * 1024
	defaultEventBuffer   = 500
	keyDelimiter         = ":"
	credentialIDField    = "credential_id"
	clientCertField      = "client_cert_id"
	authNone             = "none"
	authPassword         = "password"
	authCredential       = "credential"
	classScopeParam      = "class"
	classScopeAll        = "all"
	savedCommandsBucket  = "saved_commands"
	maxSavedCommandBytes = 8 * 1024
)

// watchStreams are the memcached watcher feeds this plugin can subscribe to.
// "fetchers" and the proxy feeds are deliberately opt-in: fetchers logs every
// internal item lookup and will swamp the activity buffer on a busy server.
var watchStreams = []string{"mutations", "evictions", "deletions", "connevents", "fetchers", "proxyevents", "proxyuser"}

type options struct {
	Host              string
	Port              int
	Username          string
	Password          string
	TLSMode           string
	CACertificate     string
	ClientCertificate string
	ReadOnly          bool
	Timeout           time.Duration
	IdleConns         int
	PageLimit         int
	ScanLimit         int
	ValueLimit        int
	EventBuffer       int
	WatchStreams      []string
}

func (o options) address() string {
	return net.JoinHostPort(o.Host, strconv.Itoa(o.Port))
}

// scanTimeout budgets the LRU crawler dump, which walks up to ScanLimit items in
// one reply and so needs more headroom than a single-line command.
func (o options) scanTimeout() time.Duration { return o.Timeout * 4 }

func configSchema() plugin.Schema {
	passwordAuth := plugin.Condition{AllOf: []plugin.Rule{
		{Field: "auth", Op: plugin.OpEq, Value: authPassword},
		{Field: credentialIDField, Op: plugin.OpEmpty},
	}}
	credentialAuth := plugin.Condition{AnyOf: []plugin.Rule{
		{Field: "auth", Op: plugin.OpEq, Value: authCredential},
		{Field: credentialIDField, Op: plugin.OpNotEmpty},
	}}
	tlsEnabled := plugin.Condition{AllOf: []plugin.Rule{{Field: "tls_mode", Op: plugin.OpNeq, Value: "disable"}}}
	verifyTLS := plugin.Condition{AnyOf: []plugin.Rule{
		{Field: "tls_mode", Op: plugin.OpEq, Value: "verify-ca"},
		{Field: "tls_mode", Op: plugin.OpEq, Value: "verify-full"},
	}}
	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Server", Fields: []plugin.Field{
			{Key: "host", Label: "Host", Type: plugin.FieldText, Required: true, Placeholder: "cache.example.internal"},
			{Key: "port", Label: "Port", Type: plugin.FieldNumber, Required: true, Default: defaultPort, Validators: []plugin.Validator{
				{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 65535},
			}},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: authNone, Options: []plugin.Option{
				{Label: "None", Value: authNone},
				{Label: "Token (username and password)", Value: authPassword},
				{Label: "Stored password", Value: authCredential},
			}, Help: "Token authentication requires memcached started with an -Y authfile."},
			{Key: "username", Label: "Username", Type: plugin.FieldText, VisibleWhen: &passwordAuth},
			{Key: "password", Label: "Password", Type: plugin.FieldPassword, Secret: true, VisibleWhen: &passwordAuth},
			{Key: credentialIDField, Label: "Stored password", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindDBPassword, Protocols: []string{protocolName},
			}, VisibleWhen: &credentialAuth, Help: "Reusable credential; its stored username is used as the memcached auth token username."},
		}},
		{Name: "TLS", Fields: []plugin.Field{
			{Key: "tls_mode", Label: "TLS mode", Type: plugin.FieldSelect, Required: true, Default: "disable", Options: []plugin.Option{
				{Label: "Disable", Value: "disable"},
				{Label: "Require encryption", Value: "require"},
				{Label: "Verify CA", Value: "verify-ca"},
				{Label: "Verify full", Value: "verify-full"},
			}, Help: "Requires memcached built and started with --enable-ssl."},
			{Key: "ca_certificate", Label: "CA certificate", Type: plugin.FieldTextarea, Secret: true, VisibleWhen: &verifyTLS, Help: "PEM CA bundle used for verify-ca and verify-full."},
			{Key: clientCertField, Label: "Client certificate", Type: plugin.FieldCredentialRef, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindTLSClientCert, Protocols: []string{protocolName},
			}, VisibleWhen: &tlsEnabled, Help: "Optional PEM containing the client certificate and private key for mTLS."},
		}},
		{Name: "Safety", Fields: []plugin.Field{
			{Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true, Help: "Blocks stores, deletes, flushes, and every runtime tuning command."},
			{Key: "timeout", Label: "Command timeout", Type: plugin.FieldDuration, Default: defaultTimeout.String()},
			{Key: "idle_conns", Label: "Idle connections", Type: plugin.FieldNumber, Default: defaultIdleConns, Validators: []plugin.Validator{
				{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 64},
			}},
			{Key: "page_limit", Label: "Key page size", Type: plugin.FieldNumber, Default: defaultPageLimit, Validators: []plugin.Validator{
				{Type: plugin.ValidatorMin, Value: 10}, {Type: plugin.ValidatorMax, Value: plugin.MaxPageLimit},
			}},
			{Key: "scan_limit", Label: "Key scan cap", Type: plugin.FieldNumber, Default: defaultScanLimit, Validators: []plugin.Validator{
				{Type: plugin.ValidatorMin, Value: 100}, {Type: plugin.ValidatorMax, Value: 200000},
			}, Help: "Maximum items the LRU crawler dump walks before the key browser stops and reports a truncated scan."},
			{Key: "value_limit", Label: "Value preview limit", Type: plugin.FieldNumber, Default: defaultValueLimit, Validators: []plugin.Validator{
				{Type: plugin.ValidatorMin, Value: 1024}, {Type: plugin.ValidatorMax, Value: 4 * 1024 * 1024},
			}, Help: "Bytes of a value returned to the browser before it is truncated."},
		}},
		{Name: "Activity", Fields: []plugin.Field{
			{Key: "watch_streams", Label: "Watcher feeds", Type: plugin.FieldMultiSelect, Default: []string{"mutations", "evictions", "deletions", "connevents"}, Options: []plugin.Option{
				{Label: "Mutations", Value: "mutations"},
				{Label: "Evictions", Value: "evictions"},
				{Label: "Deletions", Value: "deletions"},
				{Label: "Connection events", Value: "connevents"},
				{Label: "Fetchers (very noisy)", Value: "fetchers"},
				{Label: "Proxy events", Value: "proxyevents"},
				{Label: "Proxy user log", Value: "proxyuser"},
			}, Help: "Feeds subscribed on the dedicated watcher connection that backs the Activity timeline."},
			{Key: "event_buffer", Label: "Activity buffer", Type: plugin.FieldNumber, Default: defaultEventBuffer, Validators: []plugin.Validator{
				{Type: plugin.ValidatorMin, Value: 50}, {Type: plugin.ValidatorMax, Value: 5000},
			}, Help: "Number of watcher events kept in memory for the Activity timeline."},
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

	auth := dbcred.AuthMaterial{}
	switch strings.TrimSpace(cfg.String("auth")) {
	case "", authNone:
	case authPassword, authCredential:
		auth = dbcred.ApplyPasswordCredential(cfg, cfg.String("username"), cfg.String("password"))
		if auth.Username == "" || auth.Password == "" {
			return options{}, fmt.Errorf("%w: token authentication needs both a username and a password", plugin.ErrInvalidInput)
		}
		if strings.ContainsAny(auth.Username+auth.Password, " \r\n") {
			return options{}, fmt.Errorf("%w: authentication tokens cannot contain spaces or newlines", plugin.ErrInvalidInput)
		}
	default:
		return options{}, fmt.Errorf("%w: unsupported authentication method", plugin.ErrInvalidInput)
	}

	tlsMode := stringDefault(cfg.String("tls_mode"), "disable")
	certMaterial := dbcred.ApplyClientCertificateCredential(cfg, clientCertField, "", tlsMode, "")

	opts := options{
		Host:              host,
		Port:              port,
		Username:          auth.Username,
		Password:          auth.Password,
		TLSMode:           certMaterial.TLSMode,
		CACertificate:     cfg.String("ca_certificate"),
		ClientCertificate: certMaterial.ClientCertificate,
		ReadOnly:          boolValue(cfg.Config["read_only"], true),
		Timeout:           durationValue(cfg.Config["timeout"], defaultTimeout),
		IdleConns:         clampInt(intValue(cfg.Config["idle_conns"], defaultIdleConns), 1, 64),
		PageLimit:         clampInt(intValue(cfg.Config["page_limit"], defaultPageLimit), 10, plugin.MaxPageLimit),
		ScanLimit:         clampInt(intValue(cfg.Config["scan_limit"], defaultScanLimit), 100, 200000),
		ValueLimit:        clampInt(intValue(cfg.Config["value_limit"], defaultValueLimit), 1024, 4*1024*1024),
		EventBuffer:       clampInt(intValue(cfg.Config["event_buffer"], defaultEventBuffer), 50, 5000),
		WatchStreams:      parseWatchStreams(cfg.Config["watch_streams"]),
	}
	if _, err := tlsConfig(opts); err != nil {
		return options{}, err
	}
	return opts, nil
}

func parseWatchStreams(raw any) []string {
	allowed := map[string]bool{}
	for _, name := range watchStreams {
		allowed[name] = true
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(watchStreams))
	for _, value := range stringSlice(raw) {
		name := strings.TrimSpace(value)
		if !allowed[name] || seen[name] {
			continue
		}
		seen[name] = true
		out = append(out, name)
	}
	if len(out) == 0 {
		return []string{"mutations", "evictions", "deletions", "connevents"}
	}
	return out
}

func stringSlice(raw any) []string {
	switch v := raw.(type) {
	case []string:
		return v
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	case string:
		if strings.TrimSpace(v) == "" {
			return nil
		}
		return strings.Split(v, ",")
	default:
		return nil
	}
}

// dialer wires every socket this plugin opens through the gateway transport and
// performs the ASCII auth handshake on each fresh connection, so both the
// gomemcache data plane and the raw admin connections are authenticated.
func dialer(opts options, transport plugin.NetTransport) func(ctx context.Context, network, addr string) (net.Conn, error) {
	tlsCfg, _ := tlsConfig(opts)
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := transport.DialContext(ctx, network, addr)
		if err != nil {
			return nil, fmt.Errorf("%w: dial %s: %v", plugin.ErrUnavailable, addr, err)
		}
		if tlsCfg != nil {
			tlsConn := tls.Client(conn, tlsCfg)
			handshakeCtx, cancel := context.WithTimeout(ctx, opts.Timeout)
			defer cancel()
			if err := tlsConn.HandshakeContext(handshakeCtx); err != nil {
				_ = conn.Close()
				return nil, fmt.Errorf("%w: TLS handshake with %s: %v", plugin.ErrUnavailable, addr, err)
			}
			conn = tlsConn
		}
		if opts.Username != "" {
			if err := newTextConn(conn).authenticate(ctx, opts.Username, opts.Password, opts.Timeout); err != nil {
				_ = conn.Close()
				return nil, err
			}
		}
		return conn, nil
	}
}

func tlsConfig(opts options) (*tls.Config, error) {
	switch opts.TLSMode {
	case "", "disable":
		return nil, nil
	case "require", "verify-ca", "verify-full":
	default:
		return nil, fmt.Errorf("%w: unsupported TLS mode %q", plugin.ErrInvalidInput, opts.TLSMode)
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: opts.Host}
	if opts.TLSMode == "require" {
		cfg.InsecureSkipVerify = true //nolint:gosec // explicit TLS mode matching sslmode=require semantics.
	}
	if opts.CACertificate != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(opts.CACertificate)) {
			return nil, fmt.Errorf("%w: CA certificate is not valid PEM", plugin.ErrInvalidInput)
		}
		cfg.RootCAs = pool
	}
	if opts.ClientCertificate != "" {
		cert, err := tls.X509KeyPair([]byte(opts.ClientCertificate), []byte(opts.ClientCertificate))
		if err != nil {
			return nil, fmt.Errorf("%w: client certificate credential must contain certificate and private key PEM", plugin.ErrInvalidInput)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

func boolValue(v any, def bool) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	return def
}

func intValue(v any, def int) int {
	switch n := v.(type) {
	case int:
		if n > 0 {
			return n
		}
	case int64:
		if n > 0 {
			return int(n)
		}
	case float64:
		if n > 0 {
			return int(n)
		}
	}
	return def
}

func clampInt(v, lo, hi int) int {
	switch {
	case v < lo:
		return lo
	case v > hi:
		return hi
	default:
		return v
	}
}

func durationValue(v any, def time.Duration) time.Duration {
	switch t := v.(type) {
	case string:
		if d, err := time.ParseDuration(strings.TrimSpace(t)); err == nil && d > 0 {
			return d
		}
	case float64:
		if t > 0 {
			return time.Duration(t) * time.Second
		}
	case int:
		if t > 0 {
			return time.Duration(t) * time.Second
		}
	}
	return def
}

func stringDefault(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return strings.TrimSpace(v)
}
