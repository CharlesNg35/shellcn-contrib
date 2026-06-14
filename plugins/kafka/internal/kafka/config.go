package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/IBM/sarama"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn-contrib/shared/dbcred"
	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	protocolName        = "kafka"
	credentialIDField   = "credential_id"
	defaultTimeout      = 8 * time.Second
	defaultMessageLimit = 100
)

type options struct {
	Brokers       []string
	ClientID      string
	TLSConfig     *tls.Config
	SASLMechanism string
	Username      string
	Password      string
	Timeout       time.Duration
	MessageLimit  int
	ReadOnly      bool
	ConfirmWrites bool
}

func configSchema() plugin.Schema {
	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Cluster", Fields: []plugin.Field{
			{Key: "brokers", Label: "Bootstrap brokers", Type: plugin.FieldTextarea, Required: true, Default: "localhost:9092", Placeholder: "kafka-1:9092, kafka-2:9092", Help: "One or more host:port brokers, comma-separated."},
			{Key: "client_id", Label: "Client ID", Type: plugin.FieldText, Default: plugin.DefaultClientName},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: "none", Options: []plugin.Option{
				{Label: "None", Value: "none"},
				{Label: "SASL/PLAIN", Value: "plain"},
				{Label: "Stored SASL/PLAIN credential", Value: "credential"},
			}},
			{Key: "username", Label: "Username", Type: plugin.FieldText, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "plain"}}}},
			{Key: "password", Label: "Password", Type: plugin.FieldPassword, Secret: true, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "plain"}}}},
			{Key: credentialIDField, Label: "Stored SASL/PLAIN credential", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindBasicAuth, Protocols: []string{protocolName},
			}, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "credential"}}}},
		}},
		{Name: "TLS", Fields: []plugin.Field{
			{Key: "tls_mode", Label: "TLS mode", Type: plugin.FieldSelect, Required: true, Default: "disable", Options: []plugin.Option{
				{Label: "Disable", Value: "disable"},
				{Label: "Require", Value: "require"},
				{Label: "Verify CA", Value: "verify-ca"},
				{Label: "Verify full", Value: "verify-full"},
			}},
			{Key: "ca_certificate", Label: "CA certificate", Type: plugin.FieldTextarea, Secret: true, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "tls_mode", Op: plugin.OpIn, Value: []any{"verify-ca", "verify-full"}}}}},
		}},
		{Name: "Safety", Fields: []plugin.Field{
			{Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true, Help: "Blocks produce, create, delete, and offset changes."},
			{Key: "confirm_writes", Label: "Confirm write operations", Type: plugin.FieldToggle, Default: true},
			{Key: "timeout", Label: "Request timeout", Type: plugin.FieldDuration, Default: "8s"},
			{Key: "message_limit", Label: "Message limit", Type: plugin.FieldNumber, Default: defaultMessageLimit, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: plugin.MaxPageLimit}}},
		}},
	}}
}

func parseOptions(cfg plugin.ConnectConfig) (options, error) {
	addrs, err := broker.SplitAddresses(cfg.String("brokers"), 9092)
	if err != nil {
		return options{}, err
	}
	opts := options{
		Brokers:       addrs,
		ClientID:      broker.StringValue(cfg.Config, "client_id", plugin.DefaultClientName),
		Timeout:       broker.DurationValue(cfg.Config, "timeout", defaultTimeout),
		MessageLimit:  broker.IntValue(cfg.Config, "message_limit", defaultMessageLimit, 1, plugin.MaxPageLimit),
		ReadOnly:      broker.BoolValue(cfg.Config, "read_only", true),
		ConfirmWrites: broker.BoolValue(cfg.Config, "confirm_writes", true),
	}
	auth := broker.StringValue(cfg.Config, "auth", "none")
	switch auth {
	case "none":
	case "plain", "credential":
		material := dbcred.ApplyPasswordCredential(cfg, cfg.String("username"), cfg.String("password"))
		opts.SASLMechanism = string(sarama.SASLTypePlaintext)
		opts.Username, opts.Password = material.Username, material.Password
	default:
		return options{}, fmt.Errorf("%w: unsupported authentication mode %q", plugin.ErrInvalidInput, auth)
	}
	host := opts.Brokers[0]
	if i := strings.LastIndex(host, ":"); i > 0 {
		host = host[:i]
	}
	tlsConfig, err := sqldb.TLSConfig(sqldb.TLSOptions{
		Mode:          broker.StringValue(cfg.Config, "tls_mode", "disable"),
		Host:          host,
		CACertificate: cfg.String("ca_certificate"),
	})
	if err != nil {
		return options{}, err
	}
	opts.TLSConfig = tlsConfig
	return opts, nil
}

type saramaNetDialer struct {
	net     plugin.NetTransport
	timeout time.Duration
}

func (d saramaNetDialer) Dial(network, address string) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), d.timeout)
	defer cancel()
	return d.DialContext(ctx, network, address)
}

func (d saramaNetDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if d.net == nil {
		return nil, fmt.Errorf("%w: network transport is unavailable", plugin.ErrUnavailable)
	}
	return d.net.DialContext(ctx, network, address)
}

func saramaConfig(opts options, netTransport plugin.NetTransport) *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.ClientID = opts.ClientID
	cfg.Version = sarama.V2_8_0_0
	cfg.Admin.Timeout = opts.Timeout
	cfg.Net.DialTimeout = opts.Timeout
	cfg.Net.ReadTimeout = opts.Timeout
	cfg.Net.WriteTimeout = opts.Timeout
	cfg.Metadata.Timeout = opts.Timeout
	cfg.Metadata.AllowAutoTopicCreation = false
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Net.Proxy.Enable = true
	cfg.Net.Proxy.Dialer = saramaNetDialer{net: netTransport, timeout: opts.Timeout}
	if opts.TLSConfig != nil {
		cfg.Net.TLS.Enable = true
		cfg.Net.TLS.Config = opts.TLSConfig
	}
	if opts.SASLMechanism != "" {
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.Mechanism = sarama.SASLMechanism(opts.SASLMechanism)
		cfg.Net.SASL.User = opts.Username
		cfg.Net.SASL.Password = opts.Password
		cfg.Net.SASL.Handshake = true
	}
	return cfg
}
