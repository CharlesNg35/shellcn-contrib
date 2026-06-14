// Package escompat contains shared REST implementation for Elasticsearch-compatible plugins.
package escompat

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn-contrib/shared/dbcred"
	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	basicCredentialField  = "basic_credential_id"
	apiKeyCredentialField = "api_key_credential_id"
	bearerCredentialField = "bearer_credential_id"
	defaultTimeout        = 10 * time.Second
	defaultPageLimit      = 100
)

type Product string

const (
	ProductElasticsearch Product = "elasticsearch"
	ProductOpenSearch    Product = "opensearch"
)

type Provider struct {
	Protocol    string
	Title       string
	Description string
	DefaultURL  string
	Product     Product
	Icon        plugin.Icon
}

type Plugin struct {
	provider Provider
}

func New(provider Provider) *Plugin {
	return &Plugin{provider: provider}
}

type Options struct {
	Endpoint      string
	Username      string
	Password      string
	Authorization string
	TLSConfig     *tls.Config
	Timeout       time.Duration
	PageLimit     int
	ReadOnly      bool
	Product       Product
}

type Session struct {
	client *Client
	opts   Options
}

type Client struct {
	http     *http.Client
	endpoint string
	auth     string
	username string
	password string
}

type row = plugin.TableRow

type actionResult struct {
	OK bool `json:"ok"`
}

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:          plugin.CurrentAPIVersion,
		Name:                p.provider.Protocol,
		Version:             "0.1.0",
		Title:               p.provider.Title,
		Description:         p.provider.Description,
		Icon:                p.provider.Icon,
		Category:            plugin.CategorySearch,
		Config:              configSchema(p.provider),
		Capabilities:        []plugin.Capability{"indexes", "documents", "search", "mappings", "cluster_health"},
		SupportedTransports: []plugin.Transport{plugin.TransportDirect},
		Layout:              plugin.LayoutSidebarTree,
		Tree:                tree(p.provider),
		Resources:           resources(p.provider),
		Actions:             actions(p.provider),
		Streams:             []plugin.Stream{{ID: routeID(p.provider, "search.query"), Kind: plugin.StreamLogs, RouteID: routeID(p.provider, "search.query")}},
	}
}

func (p *Plugin) Routes() []plugin.Route { return Routes(p.provider) }

func (p *Plugin) Connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	return Connect(ctx, cfg, p.provider)
}

func Connect(ctx context.Context, cfg plugin.ConnectConfig, provider Provider) (plugin.Session, error) {
	opts, err := ParseOptions(cfg, provider)
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{DialContext: cfg.Net.DialContext, TLSClientConfig: opts.TLSConfig}
	s := &Session{
		client: &Client{
			http:     &http.Client{Transport: transport, Timeout: opts.Timeout},
			endpoint: opts.Endpoint,
			auth:     opts.Authorization,
			username: opts.Username,
			password: opts.Password,
		},
		opts: opts,
	}
	return s, s.HealthCheck(ctx)
}

func (s *Session) HealthCheck(ctx context.Context) error {
	var root map[string]any
	if err := s.client.Do(ctx, http.MethodGet, "/", nil, nil, &root); err != nil {
		return err
	}
	if err := verifyProduct(root, s.opts.Product); err != nil {
		return err
	}
	return nil
}

func (s *Session) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}

func (s *Session) Close() error {
	s.client.http.CloseIdleConnections()
	return nil
}

func ParseOptions(cfg plugin.ConnectConfig, provider Provider) (Options, error) {
	rawURL := strings.TrimSpace(cfg.String("endpoint"))
	if rawURL == "" {
		rawURL = provider.DefaultURL
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return Options{}, fmt.Errorf("%w: endpoint must be an absolute URL", plugin.ErrInvalidInput)
	}
	opts := Options{
		Endpoint:  strings.TrimRight(u.String(), "/"),
		Timeout:   broker.DurationValue(cfg.Config, "timeout", defaultTimeout),
		PageLimit: broker.IntValue(cfg.Config, "page_limit", defaultPageLimit, 1, plugin.MaxPageLimit),
		ReadOnly:  broker.BoolValue(cfg.Config, "read_only", true),
		Product:   provider.Product,
	}
	switch auth := broker.StringValue(cfg.Config, "auth", "none"); auth {
	case "none":
	case "basic":
		opts.Username, opts.Password = cfg.String("username"), cfg.String("password")
	case "stored_basic":
		opts.Username = dbcred.ResolvedIdentity(cfg, basicCredentialField)
		opts.Password = dbcred.ResolvedSecret(cfg, basicCredentialField)
	case "api_key":
		opts.Authorization = "ApiKey " + cfg.String("api_key")
	case "stored_api_key":
		opts.Authorization = "ApiKey " + dbcred.ResolvedSecret(cfg, apiKeyCredentialField)
	case "bearer":
		opts.Authorization = "Bearer " + cfg.String("bearer_token")
	case "stored_bearer":
		opts.Authorization = "Bearer " + dbcred.ResolvedSecret(cfg, bearerCredentialField)
	default:
		return Options{}, fmt.Errorf("%w: unsupported authentication mode %q", plugin.ErrInvalidInput, auth)
	}
	tlsConfig, err := sqldb.TLSConfig(sqldb.TLSOptions{
		Mode:          broker.StringValue(cfg.Config, "tls_mode", "disable"),
		Host:          u.Hostname(),
		CACertificate: cfg.String("ca_certificate"),
	})
	if err != nil {
		return Options{}, err
	}
	opts.TLSConfig = tlsConfig
	return opts, nil
}

func (c *Client) Do(ctx context.Context, method, path string, query url.Values, body any, out any) error {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(data)
	}
	endpoint := c.endpoint + path
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.auth != "" {
		req.Header.Set("Authorization", c.auth)
	} else if c.username != "" || c.password != "" {
		req.SetBasicAuth(c.username, c.password)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		return httpError(resp.StatusCode, data)
	}
	if out == nil || len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return err
	}
	return nil
}

func configSchema(provider Provider) plugin.Schema {
	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Server", Fields: []plugin.Field{
			{Key: "endpoint", Label: "Endpoint", Type: plugin.FieldText, Required: true, Default: provider.DefaultURL, Placeholder: "https://search.example.internal:9200"},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: "none", Options: []plugin.Option{
				{Label: "None", Value: "none"},
				{Label: "Username and password", Value: "basic"},
				{Label: "API key", Value: "api_key"},
				{Label: "Bearer token", Value: "bearer"},
				{Label: "Stored username and password", Value: "stored_basic"},
				{Label: "Stored API key", Value: "stored_api_key"},
				{Label: "Stored bearer token", Value: "stored_bearer"},
			}},
			{Key: "username", Label: "Username", Type: plugin.FieldText, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "basic"}}}},
			{Key: "password", Label: "Password", Type: plugin.FieldPassword, Secret: true, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "basic"}}}},
			{Key: "api_key", Label: "API key", Type: plugin.FieldPassword, Secret: true, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "api_key"}}}},
			{Key: "bearer_token", Label: "Bearer token", Type: plugin.FieldPassword, Secret: true, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "bearer"}}}},
			{Key: basicCredentialField, Label: "Stored username and password", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindBasicAuth, Protocols: []string{provider.Protocol},
			}, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "stored_basic"}}}},
			{Key: apiKeyCredentialField, Label: "Stored API key", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindAPIToken, Protocols: []string{provider.Protocol},
			}, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "stored_api_key"}}}},
			{Key: bearerCredentialField, Label: "Stored bearer token", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindBearerToken, Protocols: []string{provider.Protocol},
			}, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "stored_bearer"}}}},
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
			{Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true, Help: "Blocks document writes, index creation, close/open, reindex, and deletes."},
			{Key: "timeout", Label: "Request timeout", Type: plugin.FieldDuration, Default: "10s"},
			{Key: "page_limit", Label: "Page limit", Type: plugin.FieldNumber, Default: defaultPageLimit, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: plugin.MaxPageLimit}}},
		}},
	}}
}

func verifyProduct(root map[string]any, product Product) error {
	tagline := strings.ToLower(fmt.Sprint(root["tagline"]))
	version, _ := root["version"].(map[string]any)
	distribution := strings.ToLower(fmt.Sprint(version["distribution"]))
	if product == ProductElasticsearch && (strings.Contains(tagline, "opensearch") || distribution == "opensearch") {
		return fmt.Errorf("%w: endpoint is OpenSearch; use the opensearch plugin", plugin.ErrInvalidInput)
	}
	if product == ProductOpenSearch && !strings.Contains(tagline, "opensearch") && distribution != "opensearch" {
		return fmt.Errorf("%w: endpoint is Elasticsearch; use the elasticsearch plugin", plugin.ErrInvalidInput)
	}
	return nil
}

func httpError(status int, data []byte) error {
	message := strings.TrimSpace(string(data))
	var body map[string]any
	if json.Unmarshal(data, &body) == nil {
		if errObj, ok := body["error"].(map[string]any); ok {
			if reason := strings.TrimSpace(fmt.Sprint(errObj["reason"])); reason != "" {
				message = reason
			}
			if typ := strings.TrimSpace(fmt.Sprint(errObj["type"])); typ != "" {
				message = typ + ": " + message
			}
		} else if errText := strings.TrimSpace(fmt.Sprint(body["error"])); errText != "" {
			message = errText
		}
	}
	if message == "" {
		message = http.StatusText(status)
	}
	switch status {
	case http.StatusNotFound:
		return fmt.Errorf("%w: %s", plugin.ErrNotFound, message)
	case http.StatusConflict:
		return fmt.Errorf("%w: %s", plugin.ErrConflict, message)
	case http.StatusUnauthorized:
		return fmt.Errorf("%w: %s", plugin.ErrUnauthorized, message)
	case http.StatusForbidden:
		return fmt.Errorf("%w: %s", plugin.ErrForbidden, message)
	case http.StatusBadRequest:
		return fmt.Errorf("%w: %s", plugin.ErrInvalidInput, message)
	default:
		return fmt.Errorf("%w: search API returned %d: %s", plugin.ErrUnavailable, status, message)
	}
}

func unwrap(sess plugin.Session) (*Session, error) {
	if s, ok := sess.(*Session); ok {
		return s, nil
	}
	// rc.Session is the core's borrowed Handle, which exposes the live session.
	if h, ok := sess.(interface{ Session() plugin.Session }); ok {
		if s, ok := h.Session().(*Session); ok {
			return s, nil
		}
	}
	return nil, plugin.ErrInvalidInput
}

func ensureWritable(s *Session) error {
	if s.opts.ReadOnly {
		return fmt.Errorf("%w: connection is read-only", plugin.ErrForbidden)
	}
	return nil
}

func pathIndex(index string) string {
	return "/" + url.PathEscape(index)
}

func pathDoc(index, id string) string {
	return "/" + url.PathEscape(index) + "/_doc/" + url.PathEscape(id)
}

// validateIndex rejects empty names and the wildcard/all selectors so a
// destructive operation can never fan out across every index by mistake.
func validateIndex(index string) (string, error) {
	index = strings.TrimSpace(index)
	if index == "" {
		return "", fmt.Errorf("%w: index is required", plugin.ErrInvalidInput)
	}
	if strings.ContainsAny(index, "*,") || index == "_all" {
		return "", fmt.Errorf("%w: index must name a single index", plugin.ErrInvalidInput)
	}
	return index, nil
}

func numericString(v any) int64 {
	switch t := v.(type) {
	case int64:
		return t
	case int:
		return int64(t)
	case float64:
		return int64(t)
	case string:
		n, _ := strconv.ParseInt(strings.TrimSpace(t), 10, 64)
		return n
	default:
		return 0
	}
}

func isMissing(err error) bool {
	return errors.Is(err, plugin.ErrNotFound) || strings.Contains(strings.ToLower(err.Error()), "not_found")
}
