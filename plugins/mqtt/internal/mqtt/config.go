package mqtt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	mqttclient "github.com/eclipse/paho.mqtt.golang"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn-contrib/shared/dbcred"
	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	protocolName         = "mqtt"
	basicCredentialField = "basic_credential_id"
	emqxCredentialField  = "emqx_credential_id"

	defaultPort          = 1883
	defaultTimeout       = 5 * time.Second
	defaultKeepAlive     = 30 * time.Second
	defaultMessageLimit  = 200
	defaultTopicLimit    = 2000
	defaultObserveFilter = "#"

	// sysFilter is a separate subscription because the MQTT spec keeps topics
	// starting with "$" out of a root wildcard match.
	sysFilter = "$SYS/#"
	// sysLimit caps the retained $SYS key space a hostile or chatty broker can
	// push into the session.
	sysLimit = 1000
)

type options struct {
	Address       string
	Scheme        string
	ClientID      string
	CleanSession  bool
	KeepAlive     time.Duration
	Username      string
	Password      string
	TLSConfig     *tls.Config
	Timeout       time.Duration
	Observe       bool
	ObserveFilter string
	SysMetrics    bool
	TopicLimit    int
	MessageLimit  int
	ReadOnly      bool
	ConfirmWrites bool
	EMQXURL       string
	EMQXKey       string
	EMQXSecret    string
}

func (o options) brokerURL() string {
	return o.Scheme + "://" + o.Address
}

func configSchema() plugin.Schema {
	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Broker", Fields: []plugin.Field{
			{Key: "host", Label: "Host", Type: plugin.FieldText, Required: true, Default: "localhost", Placeholder: "mqtt.example.internal"},
			{Key: "port", Label: "Port", Type: plugin.FieldNumber, Required: true, Default: defaultPort, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 65535}}},
			{Key: "client_id", Label: "Client ID", Type: plugin.FieldText, Default: plugin.DefaultClientName, Help: "Must be unique on the broker; a duplicate ID disconnects the other client. Left at the default, each session gets its own suffix."},
			{Key: "clean_session", Label: "Clean session", Type: plugin.FieldToggle, Default: true},
			{Key: "keep_alive", Label: "Keep alive", Type: plugin.FieldDuration, Default: "30s"},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: "none", Options: []plugin.Option{
				{Label: "None", Value: "none"},
				{Label: "Username and password", Value: "password"},
				{Label: "Stored username and password", Value: "stored_password"},
			}},
			{Key: "username", Label: "Username", Type: plugin.FieldText, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "password"}}}},
			{Key: "password", Label: "Password", Type: plugin.FieldPassword, Secret: true, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "password"}}}},
			{Key: basicCredentialField, Label: "Stored username and password", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindBasicAuth, Protocols: []string{protocolName},
			}, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "stored_password"}}}},
		}},
		{Name: "TLS", Fields: []plugin.Field{
			{Key: "tls_mode", Label: "TLS mode", Type: plugin.FieldSelect, Required: true, Default: "disable", Options: []plugin.Option{
				{Label: "Disable", Value: "disable"},
				{Label: "Require", Value: "require"},
				{Label: "Verify CA", Value: "verify-ca"},
				{Label: "Verify full", Value: "verify-full"},
			}},
			{
				Key: "ca_certificate", Label: "CA certificate", Type: plugin.FieldTextarea, Secret: true, Placeholder: "-----BEGIN CERTIFICATE-----",
				VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "tls_mode", Op: plugin.OpIn, Value: []any{"verify-ca", "verify-full"}}}},
			},
		}},
		{Name: "Observation", Fields: []plugin.Field{
			{Key: "observe", Label: "Observe topics", Type: plugin.FieldToggle, Default: true, Help: "Keeps a background subscription so the topic tree, retained snapshot, and recent messages have data."},
			{Key: "observe_filter", Label: "Observe filter", Type: plugin.FieldText, Default: defaultObserveFilter, Placeholder: "sensors/#", VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "observe", Op: plugin.OpEq, Value: true}}}},
			{Key: "sys_metrics", Label: "Collect $SYS metrics", Type: plugin.FieldToggle, Default: true},
			{Key: "topic_limit", Label: "Topic limit", Type: plugin.FieldNumber, Default: defaultTopicLimit, Help: "Maximum distinct topics held in the observation cache.", Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 50000}}},
			{Key: "message_limit", Label: "Message buffer", Type: plugin.FieldNumber, Default: defaultMessageLimit, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: plugin.MaxPageLimit}}},
		}},
		{Name: "EMQX management API", Fields: []plugin.Field{
			{Key: "emqx_url", Label: "EMQX dashboard URL", Type: plugin.FieldText, Placeholder: "http://localhost:18083", Help: "Optional. Enables the clients, subscriptions, and node tables; plain brokers can leave this empty."},
			{Key: "emqx_auth", Label: "EMQX authentication", Type: plugin.FieldSelect, Required: true, Default: "none", Options: []plugin.Option{
				{Label: "None", Value: "none"},
				{Label: "API key", Value: "api_key"},
				{Label: "Stored API key", Value: "stored_api_key"},
			}, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "emqx_url", Op: plugin.OpNotEmpty}}}},
			{Key: "emqx_api_key", Label: "API key", Type: plugin.FieldText, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "emqx_auth", Op: plugin.OpEq, Value: "api_key"}}}},
			{Key: "emqx_api_secret", Label: "Secret key", Type: plugin.FieldPassword, Secret: true, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "emqx_auth", Op: plugin.OpEq, Value: "api_key"}}}},
			{Key: emqxCredentialField, Label: "Stored API key", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindAPIToken, Protocols: []string{protocolName},
			}, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "emqx_auth", Op: plugin.OpEq, Value: "stored_api_key"}}}},
		}},
		{Name: "Safety", Fields: []plugin.Field{
			{Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true, Help: "Blocks publish, retained-message removal, and client kicks."},
			{Key: "confirm_writes", Label: "Confirm write operations", Type: plugin.FieldToggle, Default: true},
			{Key: "timeout", Label: "Request timeout", Type: plugin.FieldDuration, Default: "5s"},
		}},
	}}
}

func parseOptions(cfg plugin.ConnectConfig) (options, error) {
	host := strings.TrimSpace(cfg.String("host"))
	if host == "" {
		return options{}, fmt.Errorf("%w: broker host is required", plugin.ErrInvalidInput)
	}
	port, ok := cfg.Int("port")
	if !ok || port <= 0 || port > 65535 {
		port = defaultPort
	}
	opts := options{
		Address:       net.JoinHostPort(host, strconv.Itoa(port)),
		Scheme:        "tcp",
		ClientID:      sessionClientID(broker.StringValue(cfg.Config, "client_id", plugin.DefaultClientName)),
		CleanSession:  broker.BoolValue(cfg.Config, "clean_session", true),
		KeepAlive:     broker.DurationValue(cfg.Config, "keep_alive", defaultKeepAlive),
		Timeout:       broker.DurationValue(cfg.Config, "timeout", defaultTimeout),
		Observe:       broker.BoolValue(cfg.Config, "observe", true),
		ObserveFilter: broker.StringValue(cfg.Config, "observe_filter", defaultObserveFilter),
		SysMetrics:    broker.BoolValue(cfg.Config, "sys_metrics", true),
		TopicLimit:    broker.IntValue(cfg.Config, "topic_limit", defaultTopicLimit, 1, 50000),
		MessageLimit:  broker.IntValue(cfg.Config, "message_limit", defaultMessageLimit, 1, plugin.MaxPageLimit),
		ReadOnly:      broker.BoolValue(cfg.Config, "read_only", true),
		ConfirmWrites: broker.BoolValue(cfg.Config, "confirm_writes", true),
	}
	if err := validateFilter(opts.ObserveFilter); err != nil {
		return options{}, err
	}
	switch auth := broker.StringValue(cfg.Config, "auth", "none"); auth {
	case "none":
	case "password":
		opts.Username, opts.Password = cfg.String("username"), cfg.String("password")
	case "stored_password":
		opts.Username = dbcred.ResolvedIdentity(cfg, basicCredentialField)
		opts.Password = dbcred.ResolvedSecret(cfg, basicCredentialField)
	default:
		return options{}, fmt.Errorf("%w: unsupported authentication mode %q", plugin.ErrInvalidInput, auth)
	}
	tlsMode := broker.StringValue(cfg.Config, "tls_mode", "disable")
	tlsConfig, err := sqldb.TLSConfig(sqldb.TLSOptions{
		Mode:          tlsMode,
		Host:          host,
		CACertificate: cfg.String("ca_certificate"),
	})
	if err != nil {
		return options{}, err
	}
	if tlsConfig != nil {
		opts.TLSConfig, opts.Scheme = tlsConfig, "ssl"
	}
	if err := parseEMQXOptions(cfg, &opts); err != nil {
		return options{}, err
	}
	return opts, nil
}

func parseEMQXOptions(cfg plugin.ConnectConfig, opts *options) error {
	raw := strings.TrimSpace(cfg.String("emqx_url"))
	if raw == "" {
		return nil
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("%w: EMQX dashboard URL must be an absolute URL", plugin.ErrInvalidInput)
	}
	opts.EMQXURL = strings.TrimRight(u.String(), "/")
	switch mode := broker.StringValue(cfg.Config, "emqx_auth", "none"); mode {
	case "none":
	case "api_key":
		opts.EMQXKey, opts.EMQXSecret = cfg.String("emqx_api_key"), cfg.String("emqx_api_secret")
	case "stored_api_key":
		opts.EMQXKey = dbcred.ResolvedIdentity(cfg, emqxCredentialField)
		opts.EMQXSecret = dbcred.ResolvedSecret(cfg, emqxCredentialField)
	default:
		return fmt.Errorf("%w: unsupported EMQX authentication mode %q", plugin.ErrInvalidInput, mode)
	}
	return nil
}

// clientOptions builds a paho client that dials only through the gateway
// transport: SetCustomOpenConnectionFn replaces the whole dial, so TLS is
// negotiated here rather than by paho's own dialer.
func clientOptions(transport plugin.NetTransport, opts options, clientID string, onConnect mqttclient.OnConnectHandler) *mqttclient.ClientOptions {
	out := mqttclient.NewClientOptions()
	out.AddBroker(opts.brokerURL())
	out.SetClientID(clientID)
	out.SetProtocolVersion(4)
	out.SetCleanSession(opts.CleanSession)
	out.SetOrderMatters(false)
	out.SetKeepAlive(opts.KeepAlive)
	out.SetConnectTimeout(opts.Timeout)
	out.SetWriteTimeout(opts.Timeout)
	out.SetPingTimeout(opts.Timeout)
	out.SetAutoReconnect(true)
	out.SetConnectRetry(false)
	out.SetResumeSubs(false)
	out.SetMaxReconnectInterval(30 * time.Second)
	if opts.Username != "" {
		out.SetUsername(opts.Username)
	}
	if opts.Password != "" {
		out.SetPassword(opts.Password)
	}
	if opts.TLSConfig != nil {
		out.SetTLSConfig(opts.TLSConfig)
	}
	out.SetCustomOpenConnectionFn(func(uri *url.URL, _ mqttclient.ClientOptions) (net.Conn, error) {
		return dialBroker(transport, opts, uri)
	})
	if onConnect != nil {
		out.SetOnConnectHandler(onConnect)
	}
	return out
}

func dialBroker(transport plugin.NetTransport, opts options, uri *url.URL) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), opts.Timeout)
	defer cancel()
	conn, err := transport.DialContext(ctx, "tcp", uri.Host)
	if err != nil {
		return nil, err
	}
	if opts.TLSConfig == nil {
		return conn, nil
	}
	tlsConn := tls.Client(conn, opts.TLSConfig)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return tlsConn, nil
}

func emqxHTTPClient(cfg plugin.ConnectConfig, opts options) (*http.Client, error) {
	tlsConfig, err := emqxTLSConfig(cfg, opts)
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{DialContext: cfg.Net.DialContext, TLSClientConfig: tlsConfig}
	return &http.Client{Transport: transport, Timeout: opts.Timeout}, nil
}

// emqxTLSConfig deliberately does not reuse the broker's TLS config: that one
// pins ServerName to the broker host and skips verification entirely in
// "require" mode. The API key rides every management request as basic auth, so
// verification is only relaxed when the dashboard is the same host the user
// already chose to trust that way.
func emqxTLSConfig(cfg plugin.ConnectConfig, opts options) (*tls.Config, error) {
	out := &tls.Config{MinVersion: tls.VersionTLS12}
	if ca := strings.TrimSpace(cfg.String("ca_certificate")); ca != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(ca)) {
			return nil, fmt.Errorf("%w: CA certificate is not valid PEM", plugin.ErrInvalidInput)
		}
		out.RootCAs = pool
	}
	if opts.TLSConfig != nil && opts.TLSConfig.InsecureSkipVerify && sameHost(opts.EMQXURL, opts.Address) {
		out.InsecureSkipVerify = true //nolint:gosec // inherited only for the broker host the connection already trusts.
	}
	return out, nil
}

func sameHost(rawURL, address string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		host = address
	}
	return u.Hostname() == host
}

// validateFilter enforces the MQTT 3.1.1 topic-filter rules the broker would
// otherwise reject with an opaque protocol error: "+" owns a whole level and
// "#" is only legal as the final level.
func validateFilter(filter string) error {
	if filter == "" {
		return fmt.Errorf("%w: topic filter is required", plugin.ErrInvalidInput)
	}
	if len(filter) > 65535 || strings.ContainsRune(filter, 0) {
		return fmt.Errorf("%w: topic filter is not a valid MQTT filter", plugin.ErrInvalidInput)
	}
	levels := strings.Split(filter, "/")
	for i, level := range levels {
		switch {
		case level == "+" || level == "#":
			if level == "#" && i != len(levels)-1 {
				return fmt.Errorf("%w: %q is only valid as the last level of a topic filter", plugin.ErrInvalidInput, "#")
			}
		case strings.ContainsAny(level, "+#"):
			return fmt.Errorf("%w: wildcards must occupy a whole topic level", plugin.ErrInvalidInput)
		}
	}
	return nil
}

// validateTopic rejects the wildcards that are legal in a filter but never in a
// published topic name.
func validateTopic(topic string) error {
	if strings.TrimSpace(topic) == "" {
		return fmt.Errorf("%w: topic is required", plugin.ErrInvalidInput)
	}
	if strings.ContainsAny(topic, "+#") || strings.ContainsRune(topic, 0) {
		return fmt.Errorf("%w: a publish topic cannot contain wildcards", plugin.ErrInvalidInput)
	}
	if len(topic) > 65535 {
		return fmt.Errorf("%w: topic is too long", plugin.ErrInvalidInput)
	}
	return nil
}

func parseQoS(raw string, fallback byte) (byte, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(trimmed)
	if err != nil || value < 0 || value > 2 {
		return 0, fmt.Errorf("%w: QoS must be 0, 1, or 2", plugin.ErrInvalidInput)
	}
	return byte(value), nil
}
