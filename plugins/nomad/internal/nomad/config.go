package nomad

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
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
	protocolName          = "nomad"
	tokenCredentialField  = "token_credential_id"
	clientCertCredential  = "client_certificate_id"
	defaultAddress        = "http://localhost:4646"
	defaultNamespace      = "default"
	defaultTimeout        = 15 * time.Second
	defaultLogLines       = 200
	defaultScanLimit      = 1000
	defaultMetricInterval = 5 * time.Second
)

type options struct {
	Address    string
	Host       string
	Region     string
	Namespace  string
	Token      string
	TLSConfig  *tls.Config
	Timeout    time.Duration
	LogLines   int
	ScanLimit  int
	MetricTick time.Duration
	ReadOnly   bool
	AllowExec  bool
}

func configSchema() plugin.Schema {
	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Cluster", Fields: []plugin.Field{
			{Key: "address", Label: "HTTP API address", Type: plugin.FieldText, Required: true, Default: defaultAddress, Placeholder: "https://nomad.example.internal:4646", Help: "Base URL of a Nomad server or client agent."},
			{Key: "region", Label: "Region", Type: plugin.FieldText, Placeholder: "global", Help: "Leave empty to use the agent's own region."},
			{Key: "namespace", Label: "Default namespace", Type: plugin.FieldText, Default: defaultNamespace, Placeholder: defaultNamespace},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: "none", Options: []plugin.Option{
				{Label: "None (ACLs disabled)", Value: "none"},
				{Label: "ACL token", Value: "token"},
				{Label: "Stored ACL token", Value: "stored_token"},
			}},
			{Key: "token", Label: "ACL token", Type: plugin.FieldPassword, Secret: true, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "token"}}}},
			{Key: tokenCredentialField, Label: "Stored ACL token", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindAPIToken, Protocols: []string{protocolName},
			}, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "stored_token"}}}},
		}},
		{Name: "TLS", Fields: []plugin.Field{
			{Key: "tls_mode", Label: "TLS mode", Type: plugin.FieldSelect, Required: true, Default: "disable", Options: []plugin.Option{
				{Label: "Disable", Value: "disable"},
				{Label: "Require", Value: "require"},
				{Label: "Verify CA", Value: "verify-ca"},
				{Label: "Verify full", Value: "verify-full"},
			}},
			{Key: "ca_certificate", Label: "CA certificate", Type: plugin.FieldTextarea, Secret: true, Placeholder: "-----BEGIN CERTIFICATE-----", VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "tls_mode", Op: plugin.OpIn, Value: []any{"verify-ca", "verify-full"}}}}},
			{Key: clientCertCredential, Label: "Client certificate", Type: plugin.FieldCredentialRef, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindTLSClientCert, Protocols: []string{protocolName},
			}, Help: "Required when the cluster enforces mutual TLS.", VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "tls_mode", Op: plugin.OpNeq, Value: "disable"}}}},
		}},
		{Name: "Safety", Fields: []plugin.Field{
			{Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true, Help: "Blocks job submit, stop, restart, drain, and every other write."},
			{Key: "allow_exec", Label: "Allow allocation exec", Type: plugin.FieldToggle, Default: false, Help: "Opens interactive shells inside running tasks."},
			{Key: "timeout", Label: "Request timeout", Type: plugin.FieldDuration, Default: "15s"},
			{Key: "log_lines", Label: "Log tail size", Type: plugin.FieldNumber, Default: defaultLogLines, Help: "Bytes-equivalent tail depth used when a log stream starts.", Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 10000}}},
			{Key: "scan_limit", Label: "Search scan limit", Type: plugin.FieldNumber, Default: defaultScanLimit, Help: "How many rows one sorted or searched listing walks before it reports a truncated scan.", Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: plugin.MaxPageLimit}, {Type: plugin.ValidatorMax, Value: 20000}}},
		}},
	}}
}

func parseOptions(cfg plugin.ConnectConfig) (options, error) {
	raw := strings.TrimSpace(cfg.String("address"))
	if raw == "" {
		raw = defaultAddress
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return options{}, fmt.Errorf("%w: address must be an http:// or https:// URL", plugin.ErrInvalidInput)
	}
	opts := options{
		Address:    strings.TrimRight(parsed.String(), "/"),
		Host:       hostPort(parsed),
		Region:     broker.StringValue(cfg.Config, "region", ""),
		Namespace:  broker.StringValue(cfg.Config, "namespace", defaultNamespace),
		Timeout:    broker.DurationValue(cfg.Config, "timeout", defaultTimeout),
		LogLines:   broker.IntValue(cfg.Config, "log_lines", defaultLogLines, 1, 10000),
		ScanLimit:  broker.IntValue(cfg.Config, "scan_limit", defaultScanLimit, plugin.MaxPageLimit, 20000),
		MetricTick: defaultMetricInterval,
		ReadOnly:   broker.BoolValue(cfg.Config, "read_only", true),
		AllowExec:  broker.BoolValue(cfg.Config, "allow_exec", false),
	}
	switch auth := broker.StringValue(cfg.Config, "auth", "none"); auth {
	case "none":
	case "token":
		opts.Token = strings.TrimSpace(cfg.String("token"))
	case "stored_token":
		opts.Token = dbcred.ResolvedSecret(cfg, tokenCredentialField)
	default:
		return options{}, fmt.Errorf("%w: unsupported authentication mode %q", plugin.ErrInvalidInput, auth)
	}
	tlsConfig, err := sqldb.TLSConfig(sqldb.TLSOptions{
		Mode:              broker.StringValue(cfg.Config, "tls_mode", "disable"),
		Host:              parsed.Hostname(),
		CACertificate:     cfg.String("ca_certificate"),
		ClientCertificate: dbcred.ResolvedClientCertificate(cfg, clientCertCredential),
	})
	if err != nil {
		return options{}, err
	}
	opts.TLSConfig = tlsConfig
	return opts, nil
}

func hostPort(u *url.URL) string {
	if u.Port() != "" {
		return u.Host
	}
	if u.Scheme == "https" {
		return net.JoinHostPort(u.Hostname(), "443")
	}
	return net.JoinHostPort(u.Hostname(), "80")
}

// pinnedDialer keeps every socket the Nomad client opens on the connection's own
// endpoint. The client library tries a direct dial to an allocation's client node
// before falling back to server-side forwarding, and its node clients swap in a
// plain OS dialer, which would bypass the gateway transport entirely. Refusing a
// foreign address with a net.Error makes that attempt fail immediately and take
// the server-forwarded path.
type pinnedDialer struct {
	host string
	net  plugin.NetTransport
}

func (d pinnedDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if !strings.EqualFold(addr, d.host) {
		return nil, &net.OpError{Op: "dial", Net: network, Err: fmt.Errorf("address %q is outside the connection endpoint", addr)}
	}
	if d.net == nil {
		var dialer net.Dialer
		return dialer.DialContext(ctx, network, addr)
	}
	return d.net.DialContext(ctx, network, addr)
}

func httpTransport(cfg plugin.ConnectConfig, opts options) *http.Transport {
	return &http.Transport{
		DialContext:         pinnedDialer{host: opts.Host, net: cfg.Net}.DialContext,
		TLSClientConfig:     opts.TLSConfig,
		MaxIdleConnsPerHost: 4,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: opts.Timeout,
	}
}

func commandContext(ctx context.Context, s *Session) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.opts.Timeout)
}
