package cloudflare

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	protocolName     = "cloudflare"
	credentialField  = "credential_id"
	defaultEndpoint  = "https://api.cloudflare.com/client/v4"
	defaultTimeout   = 20 * time.Second
	defaultPageLimit = 100
)

type Cloudflare struct{}

type Options struct {
	Endpoint      string
	Token         string
	AccountID     string
	ZoneFilter    string
	Timeout       time.Duration
	PageLimit     int
	ReadOnly      bool
	IncludeLegacy bool
}

type Session struct {
	client *cfClient
	opts   Options
}

func (Cloudflare) Connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	opts, err := parseOptions(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.Net == nil {
		return nil, fmt.Errorf("%w: network transport is unavailable", plugin.ErrUnavailable)
	}
	s := &Session{opts: opts, client: newCFClient(opts, cfg.Net.DialContext)}
	return s, s.HealthCheck(ctx)
}

func (s *Session) HealthCheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.opts.Timeout)
	defer cancel()
	var out map[string]any
	return s.client.do(ctx, http.MethodGet, "/user/tokens/verify", nil, &out)
}

func (s *Session) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}

func (s *Session) Close() error {
	s.client.http.CloseIdleConnections()
	return nil
}

func parseOptions(cfg plugin.ConnectConfig) (Options, error) {
	endpoint := strings.TrimSpace(cfg.String("endpoint"))
	if endpoint == "" {
		endpoint = defaultEndpoint
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return Options{}, fmt.Errorf("%w: endpoint must be an absolute URL", plugin.ErrInvalidInput)
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return Options{}, fmt.Errorf("%w: endpoint scheme must be http or https", plugin.ErrInvalidInput)
	}

	opts := Options{
		Endpoint:      strings.TrimRight(u.String(), "/"),
		AccountID:     strings.TrimSpace(cfg.String("account_id")),
		ZoneFilter:    strings.TrimSpace(cfg.String("zone_filter")),
		Timeout:       durationValue(cfg.Config, "timeout", defaultTimeout),
		PageLimit:     intValue(cfg.Config, "page_limit", defaultPageLimit, 1, plugin.MaxPageLimit),
		ReadOnly:      boolValue(cfg.Config, "read_only", true),
		IncludeLegacy: boolValue(cfg.Config, "include_legacy_firewall", true),
	}

	switch auth := stringValue(cfg.Config, "auth", "stored_token"); auth {
	case "stored_token":
		cred, err := cfg.RequiredCredentialFor(credentialField, plugin.CredentialKindAPIToken)
		if err != nil {
			return Options{}, err
		}
		opts.Token, err = cred.RequiredValue("token")
		if err != nil {
			return Options{}, err
		}
	case "token":
		opts.Token = cfg.String("api_token")
	default:
		return Options{}, fmt.Errorf("%w: unsupported auth mode %q", plugin.ErrInvalidInput, auth)
	}
	if strings.TrimSpace(opts.Token) == "" {
		return Options{}, fmt.Errorf("%w: Cloudflare API token is required", plugin.ErrInvalidInput)
	}
	return opts, nil
}

func cfSession(rc *plugin.RequestContext) (*Session, error) {
	if s, ok := rc.Session.(*Session); ok {
		return s, nil
	}
	if h, ok := rc.Session.(interface{ Session() plugin.Session }); ok {
		if s, ok := h.Session().(*Session); ok {
			return s, nil
		}
	}
	return nil, plugin.ErrInvalidInput
}

func stringValue(m map[string]any, key, fallback string) string {
	if v, ok := m[key].(string); ok && strings.TrimSpace(v) != "" {
		return v
	}
	return fallback
}

func boolValue(m map[string]any, key string, fallback bool) bool {
	if v, ok := m[key].(bool); ok {
		return v
	}
	return fallback
}

func intValue(m map[string]any, key string, fallback, min, max int) int {
	switch v := m[key].(type) {
	case int:
		return clamp(v, fallback, min, max)
	case int64:
		return clamp(int(v), fallback, min, max)
	case float64:
		return clamp(int(v), fallback, min, max)
	default:
		return fallback
	}
}

func durationValue(m map[string]any, key string, fallback time.Duration) time.Duration {
	if v, ok := m[key].(string); ok && strings.TrimSpace(v) != "" {
		d, err := time.ParseDuration(v)
		if err == nil && d > 0 {
			return d
		}
	}
	return fallback
}

func clamp(v, fallback, min, max int) int {
	if v < min {
		return fallback
	}
	if v > max {
		return max
	}
	return v
}
