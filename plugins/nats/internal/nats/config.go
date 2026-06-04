package nats

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"time"

	natsclient "github.com/nats-io/nats.go"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn-contrib/shared/dbcred"
	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	protocolName        = "nats"
	credentialIDField   = "credential_id"
	defaultTimeout      = 5 * time.Second
	defaultMessageLimit = 100
)

type options struct {
	URLs          []string
	Name          string
	Username      string
	Password      string
	Token         string
	TLSConfig     *tls.Config
	Timeout       time.Duration
	MessageLimit  int
	ReadOnly      bool
	ConfirmWrites bool
}

func configSchema() plugin.Schema {
	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Server", Fields: []plugin.Field{
			{Key: "urls", Label: "Servers", Type: plugin.FieldTextarea, Required: true, Default: "nats://localhost:4222", Placeholder: "nats://nats-1:4222, nats://nats-2:4222", Help: "One or more nats:// URLs, comma-separated for a cluster."},
			{Key: "name", Label: "Client name", Type: plugin.FieldText, Default: plugin.DefaultClientName},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: "none", Options: []plugin.Option{
				{Label: "None", Value: "none"},
				{Label: "Username and password", Value: "password"},
				{Label: "Token", Value: "token"},
				{Label: "Stored credential", Value: "credential"},
			}},
			{Key: "username", Label: "Username", Type: plugin.FieldText, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "password"}}}},
			{Key: "password", Label: "Password", Type: plugin.FieldPassword, Secret: true, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "password"}}}},
			{Key: "token", Label: "Token", Type: plugin.FieldPassword, Secret: true, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "token"}}}},
			{Key: credentialIDField, Label: "Stored credential", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kinds: []plugin.CredentialKind{plugin.CredentialBasicAuth, plugin.CredentialBearerToken}, Protocols: []string{protocolName},
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
			{Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true, Help: "Blocks publish, stream create, purge, and delete operations."},
			{Key: "confirm_writes", Label: "Confirm write operations", Type: plugin.FieldToggle, Default: true},
			{Key: "timeout", Label: "Request timeout", Type: plugin.FieldDuration, Default: "5s"},
			{Key: "message_limit", Label: "Message limit", Type: plugin.FieldNumber, Default: defaultMessageLimit, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: plugin.MaxPageLimit}}},
		}},
	}}
}

func parseOptions(cfg plugin.ConnectConfig) (options, error) {
	urls := splitURLs(cfg.String("urls"))
	if len(urls) == 0 {
		return options{}, fmt.Errorf("%w: at least one NATS server is required", plugin.ErrInvalidInput)
	}
	opts := options{
		URLs:          urls,
		Name:          broker.StringValue(cfg.Config, "name", plugin.DefaultClientName),
		Timeout:       broker.DurationValue(cfg.Config, "timeout", defaultTimeout),
		MessageLimit:  broker.IntValue(cfg.Config, "message_limit", defaultMessageLimit, 1, plugin.MaxPageLimit),
		ReadOnly:      broker.BoolValue(cfg.Config, "read_only", true),
		ConfirmWrites: broker.BoolValue(cfg.Config, "confirm_writes", true),
	}
	switch auth := broker.StringValue(cfg.Config, "auth", "none"); auth {
	case "none":
	case "password":
		opts.Username, opts.Password = cfg.String("username"), cfg.String("password")
	case "token":
		opts.Token = cfg.String("token")
	case "credential":
		if cfg.CredentialKindFor(plugin.CredentialField) == plugin.CredentialBearerToken {
			opts.Token = dbcred.ResolvedSecret(cfg, plugin.CredentialField)
		} else {
			material := dbcred.ApplyPasswordCredential(cfg, cfg.String("username"), cfg.String("password"))
			opts.Username, opts.Password = material.Username, material.Password
		}
	default:
		return options{}, fmt.Errorf("%w: unsupported authentication mode %q", plugin.ErrInvalidInput, auth)
	}
	tlsConfig, err := sqldb.TLSConfig(sqldb.TLSOptions{
		Mode:          broker.StringValue(cfg.Config, "tls_mode", "disable"),
		CACertificate: cfg.String("ca_certificate"),
	})
	if err != nil {
		return options{}, err
	}
	opts.TLSConfig = tlsConfig
	return opts, nil
}

type pluginDialer struct {
	timeout time.Duration
	net     plugin.NetTransport
}

func (d pluginDialer) Dial(network, address string) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), d.timeout)
	defer cancel()
	return d.net.DialContext(ctx, network, address)
}

func connectOptions(cfg plugin.ConnectConfig, opts options) []natsclient.Option {
	out := []natsclient.Option{
		natsclient.Name(opts.Name),
		natsclient.Timeout(opts.Timeout),
		natsclient.NoReconnect(),
		natsclient.SetCustomDialer(pluginDialer{timeout: opts.Timeout, net: cfg.Net}),
	}
	if opts.Username != "" || opts.Password != "" {
		out = append(out, natsclient.UserInfo(opts.Username, opts.Password))
	}
	if opts.Token != "" {
		out = append(out, natsclient.Token(opts.Token))
	}
	if opts.TLSConfig != nil {
		out = append(out, natsclient.Secure(opts.TLSConfig))
	}
	return out
}

func splitURLs(raw string) []string {
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == '\n' || r == '\r' || r == '\t'
	})
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if s := strings.TrimSpace(part); s != "" {
			out = append(out, s)
		}
	}
	return out
}
