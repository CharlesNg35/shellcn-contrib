package pinecone

import (
	"crypto/tls"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn-contrib/shared/dbcred"
	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	defaultTimeout   = 20 * time.Second
	defaultPageLimit = 100
	defaultAPIVer    = "2026-04"

	credentialField = plugin.CredentialRefField

	// maxNameLength matches Pinecone's index and collection name limit.
	maxNameLength = 45
	// maxNamespaceLength matches Pinecone's namespace identifier limit.
	maxNamespaceLength = 512
)

type Options struct {
	Endpoint   string
	Scheme     string
	APIKey     string
	APIVersion string
	Namespace  string
	PrivateNet bool
	TLSConfig  *tls.Config
	Timeout    time.Duration
	PageLimit  int
	ReadOnly   bool
}

func parseOptions(cfg plugin.ConnectConfig) (Options, error) {
	rawURL := strings.TrimSpace(cfg.String("endpoint"))
	if rawURL == "" {
		rawURL = controlPlaneDefault
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return Options{}, fmt.Errorf("%w: endpoint must be an absolute URL", plugin.ErrInvalidInput)
	}
	opts := Options{
		Endpoint: strings.TrimRight(u.String(), "/"),
		// Index hosts are returned without a scheme; a plaintext control plane
		// (a local mock or a proxy) must not be upgraded to TLS behind the user.
		Scheme:     u.Scheme,
		APIVersion: broker.StringValue(cfg.Config, "api_version", defaultAPIVer),
		Namespace:  strings.TrimSpace(cfg.String("namespace")),
		PrivateNet: broker.BoolValue(cfg.Config, "private_host", false),
		Timeout:    broker.DurationValue(cfg.Config, "timeout", defaultTimeout),
		PageLimit:  broker.IntValue(cfg.Config, "page_limit", defaultPageLimit, 1, plugin.MaxPageLimit),
		ReadOnly:   broker.BoolValue(cfg.Config, "read_only", true),
	}
	if opts.Namespace == "" {
		opts.Namespace = defaultNamespace
	}
	if err := validateNamespace(opts.Namespace); err != nil {
		return Options{}, err
	}
	if err := validateAPIVersion(opts.APIVersion); err != nil {
		return Options{}, err
	}
	switch auth := broker.StringValue(cfg.Config, "auth", "credential"); auth {
	case "api_key":
		opts.APIKey = strings.TrimSpace(cfg.String("api_key"))
	case "credential":
		if kind := cfg.CredentialKindFor(credentialField); kind != "" && kind != plugin.CredentialKindAPIToken {
			return Options{}, fmt.Errorf("%w: stored Pinecone credentials must be API tokens", plugin.ErrInvalidInput)
		}
		opts.APIKey = dbcred.ResolvedSecret(cfg, credentialField)
	default:
		return Options{}, fmt.Errorf("%w: unsupported authentication mode %q", plugin.ErrInvalidInput, auth)
	}
	if opts.APIKey == "" {
		return Options{}, fmt.Errorf("%w: a Pinecone API key is required", plugin.ErrInvalidInput)
	}
	tlsConfig, err := sqldb.TLSConfig(sqldb.TLSOptions{
		Mode:          broker.StringValue(cfg.Config, "tls_mode", "verify-full"),
		Host:          u.Hostname(),
		CACertificate: cfg.String("ca_certificate"),
	})
	if err != nil {
		return Options{}, err
	}
	opts.TLSConfig = tlsConfig
	return opts, nil
}

// validateName rejects values that would change the meaning of a REST path.
// Pinecone index and collection names are lower-case alphanumerics and hyphens.
func validateName(field, value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("%w: %s is required", plugin.ErrInvalidInput, field)
	}
	if len(value) > maxNameLength {
		return fmt.Errorf("%w: %s may be at most %d characters", plugin.ErrInvalidInput, field, maxNameLength)
	}
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
		default:
			return fmt.Errorf("%w: %s %q may only contain lower-case letters, digits, or hyphens", plugin.ErrInvalidInput, field, value)
		}
	}
	return nil
}

// validateNamespace keeps a namespace out of the URL path structure and the
// header block; Pinecone itself allows most other printable characters.
func validateNamespace(value string) error {
	if len(value) > maxNamespaceLength {
		return fmt.Errorf("%w: namespace may be at most %d characters", plugin.ErrInvalidInput, maxNamespaceLength)
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f || r == '/' || r == '\\' {
			return fmt.Errorf("%w: namespace %q contains an unsupported character", plugin.ErrInvalidInput, value)
		}
	}
	return nil
}

func validateAPIVersion(value string) error {
	if value == "" {
		return fmt.Errorf("%w: an API version is required", plugin.ErrInvalidInput)
	}
	for _, r := range value {
		switch {
		case r >= '0' && r <= '9', r == '-':
		default:
			return fmt.Errorf("%w: API version %q must look like 2025-10", plugin.ErrInvalidInput, value)
		}
	}
	return nil
}

func configSchema() plugin.Schema {
	authIs := func(values ...any) *plugin.Condition {
		return &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpIn, Value: values}}}
	}
	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Project", Fields: []plugin.Field{
			{Key: "endpoint", Label: "Control plane", Type: plugin.FieldURL, Required: true, Default: controlPlaneDefault,
				Placeholder: controlPlaneDefault, Help: "Pinecone control plane. Index data-plane hosts are discovered from it."},
			{Key: "api_version", Label: "API version", Type: plugin.FieldAutocomplete, Required: true, Default: defaultAPIVer,
				Options: []plugin.Option{
					{Label: "2026-04 (latest)", Value: "2026-04"},
					{Label: "2025-10", Value: "2025-10"},
					{Label: "2025-04", Value: "2025-04"},
				},
				Help: "Sent as the X-Pinecone-Api-Version header on every request."},
			{Key: "namespace", Label: "Default namespace", Type: plugin.FieldText, Default: defaultNamespace,
				Help: "Namespace used when a panel does not carry one."},
			{Key: "private_host", Label: "Use private endpoints", Type: plugin.FieldToggle,
				Help: "Prefer an index's private host when the project exposes one."},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: "credential", Options: []plugin.Option{
				{Label: "Stored API key", Value: "credential"},
				{Label: "API key", Value: "api_key"},
			}},
			{Key: "api_key", Label: "API key", Type: plugin.FieldPassword, Secret: true, Required: true, VisibleWhen: authIs("api_key")},
			{Key: credentialField, Label: "Stored API key", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindAPIToken, Protocols: []string{protocolName},
			}, VisibleWhen: authIs("credential")},
		}},
		{Name: "TLS", Fields: []plugin.Field{
			{Key: "tls_mode", Label: "TLS mode", Type: plugin.FieldSelect, Required: true, Default: "verify-full", Options: []plugin.Option{
				{Label: "Verify full", Value: "verify-full"},
				{Label: "Verify CA", Value: "verify-ca"},
				{Label: "Require", Value: "require"},
				{Label: "Disable", Value: "disable"},
			}},
			{Key: "ca_certificate", Label: "CA certificate", Type: plugin.FieldTextarea, Secret: true, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{
				{Field: "tls_mode", Op: plugin.OpIn, Value: []any{"verify-ca", "verify-full"}},
			}}},
		}},
		{Name: "Safety", Fields: []plugin.Field{
			{Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true, Help: "Blocks every index, collection, namespace, and vector mutation."},
			{Key: "timeout", Label: "Request timeout", Type: plugin.FieldDuration, Default: "20s"},
			{Key: "page_limit", Label: "Page limit", Type: plugin.FieldNumber, Default: defaultPageLimit, Validators: []plugin.Validator{
				{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: plugin.MaxPageLimit},
			}},
		}},
	}}
}
