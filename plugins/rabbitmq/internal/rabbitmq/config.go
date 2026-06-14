package rabbitmq

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn-contrib/shared/dbcred"
	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	protocolName        = "rabbitmq"
	credentialIDField   = "credential_id"
	defaultTimeout      = 5 * time.Second
	defaultMessageLimit = 100
)

type options struct {
	ManagementURL string
	VHost         string
	Username      string
	Password      string
	TLSConfig     *tls.Config
	Timeout       time.Duration
	MessageLimit  int
	ReadOnly      bool
	ConfirmWrites bool
}

func configSchema() plugin.Schema {
	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Management API", Fields: []plugin.Field{
			{Key: "management_url", Label: "Management URL", Type: plugin.FieldText, Required: true, Default: "http://localhost:15672", Placeholder: "https://rabbitmq.example.internal:15672"},
			{Key: "vhost", Label: "Virtual host", Type: plugin.FieldText, Default: "/", Placeholder: "/"},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: "password", Options: []plugin.Option{
				{Label: "None", Value: "none"},
				{Label: "Username and password", Value: "password"},
				{Label: "Stored username & password", Value: "credential"},
			}},
			{Key: "username", Label: "Username", Type: plugin.FieldText, Default: "guest", VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "password"}}}},
			{Key: "password", Label: "Password", Type: plugin.FieldPassword, Secret: true, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "password"}}}},
			{Key: credentialIDField, Label: "Stored username & password", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialBasicAuth, Protocols: []string{protocolName},
			}, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "credential"}}}},
		}},
		{Name: "TLS", Fields: []plugin.Field{
			{Key: "tls_mode", Label: "TLS mode", Type: plugin.FieldSelect, Required: true, Default: "disable", Options: []plugin.Option{
				{Label: "Disable", Value: "disable"},
				{Label: "Require", Value: "require"},
				{Label: "Verify CA", Value: "verify-ca"},
				{Label: "Verify full", Value: "verify-full"},
			}},
			{
				Key: "ca_certificate", Label: "CA certificate", Type: plugin.FieldTextarea, Secret: true, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "tls_mode", Op: plugin.OpIn, Value: []any{"verify-ca", "verify-full"}}}},
				Placeholder: "-----BEGIN CERTIFICATE-----",
			},
		}},
		{Name: "Safety", Fields: []plugin.Field{
			{Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true, Help: "Blocks publish, create, purge, and delete operations."},
			{Key: "confirm_writes", Label: "Confirm write operations", Type: plugin.FieldToggle, Default: true},
			{Key: "timeout", Label: "Request timeout", Type: plugin.FieldDuration, Default: "5s"},
			{Key: "message_limit", Label: "Message limit", Type: plugin.FieldNumber, Default: defaultMessageLimit, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: plugin.MaxPageLimit}}},
		}},
	}}
}

func parseOptions(cfg plugin.ConnectConfig) (options, error) {
	rawURL := strings.TrimSpace(cfg.String("management_url"))
	if rawURL == "" {
		return options{}, fmt.Errorf("%w: management URL is required", plugin.ErrInvalidInput)
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return options{}, fmt.Errorf("%w: management URL must be an absolute URL", plugin.ErrInvalidInput)
	}
	auth := broker.StringValue(cfg.Config, "auth", "password")
	opts := options{
		ManagementURL: strings.TrimRight(u.String(), "/"),
		VHost:         broker.StringValue(cfg.Config, "vhost", "/"),
		Timeout:       broker.DurationValue(cfg.Config, "timeout", defaultTimeout),
		MessageLimit:  broker.IntValue(cfg.Config, "message_limit", defaultMessageLimit, 1, plugin.MaxPageLimit),
		ReadOnly:      broker.BoolValue(cfg.Config, "read_only", true),
		ConfirmWrites: broker.BoolValue(cfg.Config, "confirm_writes", true),
	}
	if opts.VHost == "" {
		opts.VHost = "/"
	}
	switch auth {
	case "none":
	case "password", "credential":
		material := dbcred.ApplyPasswordCredential(cfg, cfg.String("username"), cfg.String("password"))
		opts.Username, opts.Password = material.Username, material.Password
	default:
		return options{}, fmt.Errorf("%w: unsupported authentication mode %q", plugin.ErrInvalidInput, auth)
	}
	tlsConfig, err := sqldb.TLSConfig(sqldb.TLSOptions{
		Mode:          broker.StringValue(cfg.Config, "tls_mode", "disable"),
		Host:          u.Hostname(),
		CACertificate: cfg.String("ca_certificate"),
	})
	if err != nil {
		return options{}, err
	}
	opts.TLSConfig = tlsConfig
	return opts, nil
}

func httpClient(cfg plugin.ConnectConfig, opts options) *http.Client {
	transport := &http.Transport{
		DialContext:     cfg.Net.DialContext,
		TLSClientConfig: opts.TLSConfig,
	}
	return &http.Client{Transport: transport, Timeout: opts.Timeout}
}

func commandContext(ctx context.Context, s *Session) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.opts.Timeout)
}
