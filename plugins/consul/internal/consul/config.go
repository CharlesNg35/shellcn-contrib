package consul

import (
	"fmt"
	"strings"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn-contrib/shared/dbcred"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	protocolName     = "consul"
	defaultPort      = 8500
	defaultTimeout   = 15 * time.Second
	defaultPageLimit = 100

	credentialIDField = plugin.CredentialRefField
	clientCertField   = "client_cert_id"

	authNone       = "none"
	authToken      = "token"
	authCredential = "credential"

	tlsDisable = "disable"
)

type options struct {
	Host              string
	Port              int
	PathPrefix        string
	TLSMode           string
	CACertificate     string
	ClientCertificate string
	Token             string
	Datacenter        string
	Namespace         string
	Partition         string
	AllowStale        bool
	ReadOnly          bool
	Timeout           time.Duration
	PageLimit         int
	KVPrefix          string
}

func configSchema() plugin.Schema {
	inlineToken := plugin.Condition{AllOf: []plugin.Rule{
		{Field: "auth", Op: plugin.OpEq, Value: authToken},
		{Field: credentialIDField, Op: plugin.OpEmpty},
	}}
	storedToken := plugin.Condition{AnyOf: []plugin.Rule{
		{Field: "auth", Op: plugin.OpEq, Value: authCredential},
		{Field: credentialIDField, Op: plugin.OpNotEmpty},
	}}
	tlsEnabled := plugin.Condition{AllOf: []plugin.Rule{{Field: "tls_mode", Op: plugin.OpNeq, Value: tlsDisable}}}
	verifyTLS := plugin.Condition{AnyOf: []plugin.Rule{
		{Field: "tls_mode", Op: plugin.OpEq, Value: "verify-ca"},
		{Field: "tls_mode", Op: plugin.OpEq, Value: "verify-full"},
	}}
	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Agent", Fields: []plugin.Field{
			{Key: "host", Label: "Host", Type: plugin.FieldText, Required: true, Placeholder: "consul.service.consul", Help: "Address of a Consul agent or server HTTP API."},
			{Key: "port", Label: "Port", Type: plugin.FieldNumber, Required: true, Default: defaultPort, Validators: []plugin.Validator{
				{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 65535},
			}},
			{Key: "path_prefix", Label: "Path prefix", Type: plugin.FieldText, Placeholder: "/consul", Help: "Set when the HTTP API is served behind a reverse proxy that strips a prefix."},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: authNone, Options: []plugin.Option{
				{Label: "None (ACLs disabled)", Value: authNone},
				{Label: "ACL token", Value: authToken},
				{Label: "Stored ACL token", Value: authCredential},
			}},
			{Key: "token", Label: "ACL token", Type: plugin.FieldPassword, Secret: true, VisibleWhen: &inlineToken},
			{Key: credentialIDField, Label: "Stored ACL token", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindAPIToken, Protocols: []string{protocolName},
			}, VisibleWhen: &storedToken, Help: "Reusable Consul ACL token secret."},
		}},
		{Name: "TLS", Fields: []plugin.Field{
			{Key: "tls_mode", Label: "TLS mode", Type: plugin.FieldSelect, Required: true, Default: tlsDisable, Options: []plugin.Option{
				{Label: "Disable", Value: tlsDisable},
				{Label: "Require encryption", Value: "require"},
				{Label: "Verify CA", Value: "verify-ca"},
				{Label: "Verify full", Value: "verify-full"},
			}},
			{Key: "ca_certificate", Label: "CA certificate", Type: plugin.FieldTextarea, Secret: true, VisibleWhen: &verifyTLS, Help: "PEM CA bundle used for verify-ca and verify-full."},
			{Key: clientCertField, Label: "Client certificate", Type: plugin.FieldCredentialRef, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindTLSClientCert, Protocols: []string{protocolName},
			}, VisibleWhen: &tlsEnabled, Help: "Optional certificate and private key for mutual TLS."},
		}},
		{Name: "Scope", Fields: []plugin.Field{
			{Key: "datacenter", Label: "Datacenter", Type: plugin.FieldText, Placeholder: "dc1", Help: "Default datacenter for every query. Leave empty to use the agent's own datacenter."},
			{Key: "namespace", Label: "Namespace", Type: plugin.FieldText, Help: "Consul Enterprise namespace applied to every request."},
			{Key: "partition", Label: "Admin partition", Type: plugin.FieldText, Help: "Consul Enterprise admin partition applied to every request."},
			{Key: "kv_prefix", Label: "KV root prefix", Type: plugin.FieldText, Placeholder: "apps/", Help: "Root the key browser starts from. Leave empty to browse the whole key space."},
		}},
		{Name: "Safety", Fields: []plugin.Field{
			{Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true, Help: "Blocks key writes, deregistration, intention changes, and session destruction."},
			{Key: "allow_stale", Label: "Allow stale reads", Type: plugin.FieldToggle, Default: false, Help: "Lets any server answer reads instead of the leader; faster but possibly out of date."},
			{Key: "timeout", Label: "Request timeout", Type: plugin.FieldDuration, Default: defaultTimeout.String()},
			{Key: "page_limit", Label: "Page size", Type: plugin.FieldNumber, Default: defaultPageLimit, Validators: []plugin.Validator{
				{Type: plugin.ValidatorMin, Value: 10}, {Type: plugin.ValidatorMax, Value: plugin.MaxPageLimit},
			}, Help: "Caps rows fetched per page and nodes shown per key-tree level."},
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
	token := ""
	switch strings.TrimSpace(cfg.String("auth")) {
	case "", authNone:
	case authToken:
		token = strings.TrimSpace(cfg.String("token"))
		if secret := dbcred.ResolvedSecret(cfg, credentialIDField); secret != "" {
			token = secret
		}
	case authCredential:
		token = dbcred.ResolvedSecret(cfg, credentialIDField)
		if token == "" {
			return options{}, fmt.Errorf("%w: the selected credential carries no token", plugin.ErrInvalidInput)
		}
	default:
		return options{}, fmt.Errorf("%w: unsupported authentication method", plugin.ErrInvalidInput)
	}
	tlsMode := broker.StringValue(cfg.Config, "tls_mode", tlsDisable)
	switch tlsMode {
	case tlsDisable, "require", "verify-ca", "verify-full":
	default:
		return options{}, fmt.Errorf("%w: unsupported TLS mode %q", plugin.ErrInvalidInput, tlsMode)
	}
	prefix := strings.TrimSpace(cfg.String("path_prefix"))
	if prefix != "" && !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}
	return options{
		Host:              host,
		Port:              port,
		PathPrefix:        strings.TrimSuffix(prefix, "/"),
		TLSMode:           tlsMode,
		CACertificate:     cfg.String("ca_certificate"),
		ClientCertificate: dbcred.ResolvedClientCertificate(cfg, clientCertField),
		Token:             token,
		Datacenter:        strings.TrimSpace(cfg.String("datacenter")),
		Namespace:         strings.TrimSpace(cfg.String("namespace")),
		Partition:         strings.TrimSpace(cfg.String("partition")),
		AllowStale:        broker.BoolValue(cfg.Config, "allow_stale", false),
		ReadOnly:          broker.BoolValue(cfg.Config, "read_only", true),
		Timeout:           broker.DurationValue(cfg.Config, "timeout", defaultTimeout),
		PageLimit:         broker.IntValue(cfg.Config, "page_limit", defaultPageLimit, 10, plugin.MaxPageLimit),
		KVPrefix:          strings.TrimPrefix(strings.TrimSpace(cfg.String("kv_prefix")), "/"),
	}, nil
}

func (o options) scheme() string {
	if o.TLSMode == "" || o.TLSMode == tlsDisable {
		return "http"
	}
	return "https"
}
