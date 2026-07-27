package pulsar

import (
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
	protocolName         = "pulsar"
	tokenCredentialField = "token_credential_id"
	clientCertField      = "client_cert_id"

	defaultAdminURL     = "http://localhost:8080"
	defaultServiceURL   = "pulsar://localhost:6650"
	defaultTenant       = "public"
	defaultNamespace    = "default"
	defaultTimeout      = 10 * time.Second
	defaultMessageLimit = 25
)

type options struct {
	AdminURL     string
	ServiceURL   string
	Tenant       string
	Namespace    string
	Token        string
	Username     string
	Password     string
	TLSConfig    *tls.Config
	Timeout      time.Duration
	MessageLimit int
	ReadOnly     bool
	SystemTopics bool
}

func configSchema() plugin.Schema {
	authIs := func(mode string) *plugin.Condition {
		return &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: mode}}}
	}
	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Cluster", Fields: []plugin.Field{
			{Key: "admin_url", Label: "Admin API URL", Type: plugin.FieldURL, Required: true, Default: defaultAdminURL, Placeholder: "https://pulsar.example.internal:8080", Help: "Broker or proxy web service URL that serves the /admin/v2 REST API."},
			{Key: "service_url", Label: "Broker service URL", Type: plugin.FieldText, Default: defaultServiceURL, Placeholder: "pulsar+ssl://pulsar.example.internal:6651", Help: "Binary protocol URL used to produce and tail messages. Browsing works without it."},
			{Key: "tenant", Label: "Default tenant", Type: plugin.FieldText, Default: defaultTenant, Placeholder: defaultTenant},
			{Key: "namespace", Label: "Default namespace", Type: plugin.FieldText, Default: defaultNamespace, Placeholder: defaultNamespace},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: "none", Options: []plugin.Option{
				{Label: "None", Value: "none"},
				{Label: "JWT token", Value: "token"},
				{Label: "Stored bearer token", Value: "token_credential"},
				{Label: "Username and password", Value: "basic"},
				{Label: "Stored username & password", Value: "basic_credential"},
			}},
			{Key: "token", Label: "JWT token", Type: plugin.FieldPassword, Secret: true, Required: true, VisibleWhen: authIs("token")},
			{Key: tokenCredentialField, Label: "Stored bearer token", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindBearerToken, Protocols: []string{protocolName},
			}, VisibleWhen: authIs("token_credential")},
			{Key: "username", Label: "Username", Type: plugin.FieldText, Required: true, VisibleWhen: authIs("basic")},
			{Key: "password", Label: "Password", Type: plugin.FieldPassword, Secret: true, Required: true, VisibleWhen: authIs("basic")},
			{Key: plugin.CredentialRefField, Label: "Stored username & password", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindBasicAuth, Protocols: []string{protocolName},
			}, VisibleWhen: authIs("basic_credential")},
		}},
		{Name: "TLS", Fields: []plugin.Field{
			{Key: "tls_mode", Label: "TLS mode", Type: plugin.FieldSelect, Required: true, Default: "disable", Options: []plugin.Option{
				{Label: "Disable", Value: "disable"},
				{Label: "Require", Value: "require"},
				{Label: "Verify CA", Value: "verify-ca"},
				{Label: "Verify full", Value: "verify-full"},
			}},
			{Key: "ca_certificate", Label: "CA certificate", Type: plugin.FieldTextarea, Secret: true, Placeholder: "-----BEGIN CERTIFICATE-----",
				VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "tls_mode", Op: plugin.OpIn, Value: []any{"verify-ca", "verify-full"}}}}},
			{Key: clientCertField, Label: "Client certificate", Type: plugin.FieldCredentialRef, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindTLSClientCert, Protocols: []string{protocolName},
			}, Help: "Optional mutual-TLS certificate presented to the broker.",
				VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "tls_mode", Op: plugin.OpNeq, Value: "disable"}}}},
		}},
		{Name: "Safety", Fields: []plugin.Field{
			{Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true, Help: "Blocks produce, create, cursor changes, and delete operations."},
			{Key: "include_system_topics", Label: "Show system topics", Type: plugin.FieldToggle, Default: false, Help: "Include the broker's internal topics (transaction, change events, health check) in topic listings."},
			{Key: "timeout", Label: "Request timeout", Type: plugin.FieldDuration, Default: "10s"},
			{Key: "message_limit", Label: "Message page size", Type: plugin.FieldNumber, Default: defaultMessageLimit, Help: "Upper bound on how many messages one browse request reads from a topic.",
				Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: plugin.MaxPageLimit}}},
		}},
	}}
}

func parseOptions(cfg plugin.ConnectConfig) (options, error) {
	rawURL := strings.TrimSpace(cfg.String("admin_url"))
	if rawURL == "" {
		return options{}, fmt.Errorf("%w: admin API URL is required", plugin.ErrInvalidInput)
	}
	adminURL, err := url.Parse(rawURL)
	if err != nil || adminURL.Scheme == "" || adminURL.Host == "" {
		return options{}, fmt.Errorf("%w: admin API URL must be an absolute URL", plugin.ErrInvalidInput)
	}
	opts := options{
		AdminURL:     strings.TrimRight(adminURL.String(), "/"),
		ServiceURL:   broker.StringValue(cfg.Config, "service_url", defaultServiceURL),
		Tenant:       broker.StringValue(cfg.Config, "tenant", defaultTenant),
		Namespace:    broker.StringValue(cfg.Config, "namespace", defaultNamespace),
		Timeout:      broker.DurationValue(cfg.Config, "timeout", defaultTimeout),
		MessageLimit: broker.IntValue(cfg.Config, "message_limit", defaultMessageLimit, 1, plugin.MaxPageLimit),
		ReadOnly:     broker.BoolValue(cfg.Config, "read_only", true),
		SystemTopics: broker.BoolValue(cfg.Config, "include_system_topics", false),
	}
	switch auth := broker.StringValue(cfg.Config, "auth", "none"); auth {
	case "none":
	case "token":
		opts.Token = strings.TrimSpace(cfg.String("token"))
	case "token_credential":
		opts.Token = dbcred.ResolvedSecret(cfg, tokenCredentialField)
	case "basic":
		opts.Username, opts.Password = strings.TrimSpace(cfg.String("username")), cfg.String("password")
	case "basic_credential":
		material := dbcred.ApplyPasswordCredential(cfg, "", "")
		opts.Username, opts.Password = material.Username, material.Password
	default:
		return options{}, fmt.Errorf("%w: unsupported authentication mode %q", plugin.ErrInvalidInput, auth)
	}
	tlsMode := broker.StringValue(cfg.Config, "tls_mode", "disable")
	tlsConfig, err := sqldb.TLSConfig(sqldb.TLSOptions{
		Mode:              tlsMode,
		Host:              adminURL.Hostname(),
		CACertificate:     cfg.String("ca_certificate"),
		ClientCertificate: dbcred.ResolvedClientCertificate(cfg, clientCertField),
	})
	if err != nil {
		return options{}, err
	}
	opts.TLSConfig = tlsConfig
	return opts, nil
}

func httpClient(cfg plugin.ConnectConfig, opts options) *http.Client {
	transport := &http.Transport{TLSClientConfig: opts.TLSConfig}
	if cfg.Net != nil {
		transport.DialContext = cfg.Net.DialContext
	}
	return &http.Client{Transport: transport, Timeout: opts.Timeout}
}
