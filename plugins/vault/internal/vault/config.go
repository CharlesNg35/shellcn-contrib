package vault

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn-contrib/shared/dbcred"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	protocolName          = "vault"
	defaultPort           = 8200
	defaultTimeout        = 15 * time.Second
	defaultAppRolePath    = "approle"
	tokenCredField        = "token_credential_id"
	approleCredField      = "approle_credential_id"
	credentialKindAppRole = plugin.CredentialKind("vault_approle")

	authToken           = "token"
	authTokenCredential = "token_credential"
	authAppRole         = "approle"

	tlsDisable    = "disable"
	tlsRequire    = "require"
	tlsVerifyCA   = "verify-ca"
	tlsVerifyFull = "verify-full"
)

type options struct {
	Host          string
	Port          int
	TLSMode       string
	CACertificate string
	Namespace     string
	AuthMethod    string
	Token         string
	RoleID        string
	SecretID      string
	AppRolePath   string
	ReadOnly      bool
	Timeout       time.Duration
}

func configSchema() plugin.Schema {
	inlineToken := plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: authToken}}}
	storedToken := plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: authTokenCredential}}}
	approle := plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: authAppRole}}}
	tlsVerified := plugin.Condition{AnyOf: []plugin.Rule{
		{Field: "tls_mode", Op: plugin.OpEq, Value: tlsVerifyCA},
		{Field: "tls_mode", Op: plugin.OpEq, Value: tlsVerifyFull},
	}}
	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Server", Fields: []plugin.Field{
			{Key: "host", Label: "Host", Type: plugin.FieldText, Required: true, Placeholder: "vault.example.internal"},
			{Key: "port", Label: "Port", Type: plugin.FieldNumber, Required: true, Default: defaultPort, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 65535}}},
			{Key: "namespace", Label: "Namespace", Type: plugin.FieldText, Placeholder: "admin/team", Help: "Vault Enterprise namespace this connection starts in. Leave empty for the root namespace."},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: authTokenCredential, Options: []plugin.Option{
				{Label: "Stored token", Value: authTokenCredential},
				{Label: "Token", Value: authToken},
				{Label: "AppRole", Value: authAppRole},
			}},
			{Key: tokenCredField, Label: "Stored token", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindAPIToken, Protocols: []string{protocolName},
			}, VisibleWhen: &storedToken, Help: "Reusable Vault token."},
			{Key: "token", Label: "Token", Type: plugin.FieldPassword, Secret: true, Required: true, VisibleWhen: &inlineToken},
			{Key: approleCredField, Label: "AppRole credential", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: credentialKindAppRole, Protocols: []string{protocolName},
			}, VisibleWhen: &approle, Help: "Reusable AppRole role ID and secret ID; the plugin logs in at connect time."},
			{Key: "approle_path", Label: "AppRole mount", Type: plugin.FieldText, Default: defaultAppRolePath, VisibleWhen: &approle, Help: "Auth mount path the AppRole method is enabled at."},
		}},
		{Name: "TLS", Fields: []plugin.Field{
			{Key: "tls_mode", Label: "TLS mode", Type: plugin.FieldSelect, Required: true, Default: tlsVerifyFull, Options: []plugin.Option{
				{Label: "Disable (http)", Value: tlsDisable},
				{Label: "Require encryption", Value: tlsRequire},
				{Label: "Verify CA", Value: tlsVerifyCA},
				{Label: "Verify full", Value: tlsVerifyFull},
			}},
			{Key: "ca_certificate", Label: "CA certificate", Type: plugin.FieldTextarea, Secret: true, VisibleWhen: &tlsVerified, Help: "PEM CA bundle used for verify-ca and verify-full."},
		}},
		{Name: "Safety", Fields: []plugin.Field{
			{Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true, Help: "Blocks secret writes, policy changes, token creation, and every revoke or destroy operation."},
			{Key: "timeout", Label: "Request timeout", Type: plugin.FieldDuration, Default: defaultTimeout.String()},
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
	tlsMode := broker.StringValue(cfg.Config, "tls_mode", tlsVerifyFull)
	switch tlsMode {
	case tlsDisable, tlsRequire, tlsVerifyCA, tlsVerifyFull:
	default:
		return options{}, fmt.Errorf("%w: unsupported TLS mode %q", plugin.ErrInvalidInput, tlsMode)
	}

	opts := options{
		Host:          host,
		Port:          port,
		TLSMode:       tlsMode,
		CACertificate: cfg.String("ca_certificate"),
		Namespace:     normalizeNamespace(cfg.String("namespace")),
		AuthMethod:    broker.StringValue(cfg.Config, "auth", authTokenCredential),
		AppRolePath:   strings.Trim(broker.StringValue(cfg.Config, "approle_path", defaultAppRolePath), "/"),
		ReadOnly:      broker.BoolValue(cfg.Config, "read_only", true),
		Timeout:       broker.DurationValue(cfg.Config, "timeout", defaultTimeout),
	}
	switch opts.AuthMethod {
	case authToken:
		opts.Token = strings.TrimSpace(cfg.String("token"))
	case authTokenCredential:
		opts.Token = dbcred.ResolvedSecret(cfg, tokenCredField)
	case authAppRole:
		opts.RoleID = strings.TrimSpace(cfg.CredentialValueFor(approleCredField, "role_id"))
		opts.SecretID = strings.TrimSpace(cfg.CredentialValueFor(approleCredField, "secret_id"))
	default:
		return options{}, fmt.Errorf("%w: unsupported authentication method %q", plugin.ErrInvalidInput, opts.AuthMethod)
	}
	if err := validateAuth(opts); err != nil {
		return options{}, err
	}
	return opts, nil
}

func validateAuth(opts options) error {
	switch opts.AuthMethod {
	case authToken, authTokenCredential:
		if opts.Token == "" {
			return fmt.Errorf("%w: a Vault token is required", plugin.ErrInvalidInput)
		}
	case authAppRole:
		if opts.RoleID == "" || opts.SecretID == "" {
			return fmt.Errorf("%w: AppRole authentication needs both a role ID and a secret ID", plugin.ErrInvalidInput)
		}
		if opts.AppRolePath == "" {
			return fmt.Errorf("%w: AppRole mount path is required", plugin.ErrInvalidInput)
		}
	}
	return nil
}

func (o options) address() string {
	scheme := "https"
	if o.TLSMode == tlsDisable {
		scheme = "http"
	}
	return scheme + "://" + net.JoinHostPort(o.Host, strconv.Itoa(o.Port))
}

func clientTLSConfig(opts options) (*tls.Config, error) {
	if opts.TLSMode == tlsDisable {
		return nil, nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: opts.Host}
	if opts.TLSMode == tlsRequire {
		cfg.InsecureSkipVerify = true //nolint:gosec // explicit "encrypt but do not verify" mode, mirroring sslmode=require.
	}
	if strings.TrimSpace(opts.CACertificate) != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(opts.CACertificate)) {
			return nil, fmt.Errorf("%w: CA certificate is not valid PEM", plugin.ErrInvalidInput)
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}

// normalizeNamespace trims the leading and trailing slashes Vault omits in the
// X-Vault-Namespace header while keeping the inner separators of a nested path.
func normalizeNamespace(raw string) string {
	return strings.Trim(strings.TrimSpace(raw), "/")
}
