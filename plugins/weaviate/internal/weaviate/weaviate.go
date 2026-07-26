// Package weaviate implements the Weaviate vector database plugin.
package weaviate

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn-contrib/shared/dbcred"
	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/alias"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/backup"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/batch"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/cluster"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/connection"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/data"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/db"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/misc"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/schema"
)

const (
	protocolName             = "weaviate"
	defaultEndpoint          = "http://localhost:8080"
	defaultTimeout           = 30 * time.Second
	defaultPageLimit         = 100
	defaultVectorSample      = 400
	maxVectorSample          = 2000
	inferenceCredentialField = "inference_credential_id"
)

const iconSVG = `<svg xmlns="http://www.w3.org/2000/svg" xml:space="preserve" viewBox="0 0 512 512"><linearGradient id="weaviate_svg__a" x1="275.435" x2="235.143" y1="2510.546" y2="2905.937" gradientTransform="matrix(1 0 0 -1 0 2947.92)" gradientUnits="userSpaceOnUse"><stop offset="0" style="stop-color:#75be2c"/><stop offset=".86" style="stop-color:#9dc03b"/></linearGradient><path d="m495.4 156.1-88.6-51.5c-14.9-8.6-33.6 2.1-33.6 19.3v130.2l-62.9-36.2c-33.7-19.4-75.2-19.4-108.8.1l-62.6 36.2V123.8c0-17.2-18.6-27.9-33.5-19.3L16.7 156C6.3 162 0 173.1 0 185v122.4c0 8.5 2.1 16.7 6.1 23.9 4 7.4 9.8 13.7 17.1 18.4l45.7 29.2 35 22.3c21 13.4 48.1 12.3 67.9-2.7l2.6-2c.2-.2.5-.4.8-.6l52.8-40.3c15.4-11.8 40.7-11.8 56.1 0l52.6 40.1s.2.1.2.2l3.3 2.6c19.9 15 46.9 16 67.9 2.7l35-22.3 45.8-29.2c7.2-4.7 13-11 17-18.3s6.1-15.4 6.1-24V185c0-11.9-6.4-22.9-16.6-28.9" style="fill:url(#weaviate_svg__a)"/><linearGradient id="weaviate_svg__b" x1="256.2" x2="256.2" y1="2675.345" y2="2545.72" gradientTransform="matrix(1 0 0 -1 0 2947.92)" gradientUnits="userSpaceOnUse"><stop offset="0" style="stop-color:#238d37"/><stop offset=".94" style="stop-color:#35537f"/></linearGradient><path d="M373.9 333.6v44c0 13.6-9.7 24.6-21.5 24.6-4.6 0-10.4-2.3-15.8-6.4L284 355.7c-15.4-11.8-40.7-11.8-56.1 0L175.1 396c-5.2 4-8.6 5.1-13.6 5.1-12.3.1-23.2-9-23.1-23.5v-43.8l75.4-48.7a77.86 77.86 0 0 1 84.7 0l75.5 47.5z" style="fill:url(#weaviate_svg__b)"/><linearGradient id="weaviate_svg__c" x1="242.531" x2="262.625" y1="2742.493" y2="2610.545" gradientTransform="matrix(1 0 0 -1 0 2947.92)" gradientUnits="userSpaceOnUse"><stop offset="0" style="stop-color:#67d84d"/><stop offset="1" style="stop-color:#348522"/></linearGradient><path d="m373.3 254.1.2 80.1-74.9-47.9c-25.8-16.5-58.8-16.5-84.7 0l-75.1 48 .1-80.1 62.7-36.3c33.6-19.5 75.1-19.5 108.8-.1z" style="fill:url(#weaviate_svg__c)"/><linearGradient id="weaviate_svg__d" x1="442.6" x2="442.6" y1="2846.454" y2="2616.62" gradientTransform="matrix(1 0 0 -1 0 2947.92)" gradientUnits="userSpaceOnUse"><stop offset="0" style="stop-color:#e4d00a"/><stop offset=".56" style="stop-color:#c4d132"/></linearGradient><path d="M512 185v122.3c0 8.6-2.2 16.8-6.1 24l-132.7-77.2V123.8c0-17.2 18.7-27.9 33.6-19.3l88.6 51.5c10.3 6.1 16.6 17.1 16.6 29" style="fill:url(#weaviate_svg__d)"/><linearGradient id="weaviate_svg__e" x1="69.4" x2="69.4" y1="2846.397" y2="2616.52" gradientTransform="matrix(1 0 0 -1 0 2947.92)" gradientUnits="userSpaceOnUse"><stop offset="0" style="stop-color:#e4d00a"/><stop offset=".56" style="stop-color:#c4d132"/></linearGradient><path d="M138.8 123.8v130.4L6.1 331.4c-4-7.2-6.1-15.4-6.1-24V185c0-11.9 6.3-22.9 16.6-28.9l88.6-51.5c14.9-8.7 33.6 2.1 33.6 19.2" style="fill:url(#weaviate_svg__e)"/><linearGradient id="weaviate_svg__f" x1="390.1" x2="390.1" y1="2614.02" y2="2537.337" gradientTransform="matrix(1 0 0 -1 0 2947.92)" gradientUnits="userSpaceOnUse"><stop offset="0" style="stop-color:#8ab11b"/><stop offset="1" style="stop-color:#6eaf02"/></linearGradient><path d="M350.6 400.6c11.8 0 22.8-9.3 22.8-22.7l-.1-44 70.1 44.7-35.4 22.6c-21 13.4-48.1 12.4-67.9-2.7l-3.3-2.6c5 3.4 9.2 4.7 13.8 4.7" style="fill:url(#weaviate_svg__f)"/><linearGradient id="weaviate_svg__g" x1="445.9" x2="429.832" y1="2706.563" y2="2577.53" gradientTransform="matrix(1 0 0 -1 0 2947.92)" gradientUnits="userSpaceOnUse"><stop offset="0" style="stop-color:#75be2c"/><stop offset=".86" style="stop-color:#9dc03b"/></linearGradient><path d="M505.9 331.4c-4 7.3-9.8 13.6-17 18.3L443 378.8l-69.7-44.6-.2-80.1z" style="fill:url(#weaviate_svg__g)"/><linearGradient id="weaviate_svg__h" x1="121.95" x2="121.95" y1="2613.724" y2="2537.416" gradientTransform="matrix(1 0 0 -1 0 2947.92)" gradientUnits="userSpaceOnUse"><stop offset="0" style="stop-color:#8ab11b"/><stop offset="1" style="stop-color:#6eaf02"/></linearGradient><path d="M138.8 377.9c0 13.4 11.1 24 22.8 22.7 5.2-.6 9.2-1.4 13.4-4.5l-3.2 2.5c-19.9 15-46.9 16-67.9 2.7l-35-22.3v-.1l69.9-44.6z" style="fill:url(#weaviate_svg__h)"/><linearGradient id="weaviate_svg__i" x1="81.714" x2="65.889" y1="2705.934" y2="2550.802" gradientTransform="matrix(1 0 0 -1 0 2947.92)" gradientUnits="userSpaceOnUse"><stop offset="0" style="stop-color:#75be2c"/><stop offset=".86" style="stop-color:#9dc03b"/></linearGradient><path d="M138.8 254.2v80l-69.9 44.6v.1l-45.7-29.1c-7.3-4.7-13.2-11-17.1-18.4z" style="fill:url(#weaviate_svg__i)"/></svg>`

type Plugin struct{}

type Options struct {
	Endpoint        string
	Scheme          string
	Host            string
	APIKey          string
	InferenceHeader string
	InferenceKey    string
	TLSConfig       *tls.Config
	Timeout         time.Duration
	PageLimit       int
	VectorSample    int
	ReadOnly        bool
	Consistency     string
}

// client bundles the weaviate-go-client API groups this plugin uses. The groups
// are built from one connection the plugin owns so its finalizer can be cleared:
// v5's connection finalizer sends on an unbuffered channel that only an OIDC
// refresh goroutine ever reads, so letting it run wedges the process-wide
// finalizer goroutine.
type client struct {
	misc    *misc.API
	schema  *schema.API
	data    *data.API
	batch   *batch.API
	graphql *graphql.API
	cluster *cluster.API
	alias   *alias.API
	backup  *backup.API
}

type Session struct {
	client *client
	http   *http.Client
	opts   Options
}

type row = plugin.TableRow

func New() plugin.Plugin { return Plugin{} }

func (Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:  plugin.CurrentAPIVersion,
		Name:        protocolName,
		Version:     "0.1.0",
		Title:       "Weaviate",
		Description: "Weaviate cockpit: collection schema, object CRUD, GraphQL and vector/hybrid search, cross-reference graphs, embedding projection, shards, tenants, and backups.",
		Icon:        plugin.Icon{Type: plugin.IconSVG, Value: iconSVG},
		Category:    plugin.CategorySearch,
		Config:      configSchema(),
		Capabilities: []plugin.Capability{
			"collections", "objects", "graphql", "vector-search", "hybrid-search",
			"cross-references", "multi-tenancy", "backups", "metrics",
		},
		SupportedTransports: []plugin.Transport{plugin.TransportDirect},
		Layout:              plugin.LayoutSidebarTree,
		Tree:                tree(),
		Resources:           resources(),
		Actions:             actions(),
		Streams:             streams(),
	}
}

func (Plugin) Routes() []plugin.Route { return Routes() }

func (Plugin) Connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	opts, err := parseOptions(cfg)
	if err != nil {
		return nil, err
	}
	httpClient := &http.Client{
		Timeout:   opts.Timeout,
		Transport: &http.Transport{TLSClientConfig: opts.TLSConfig, DialContext: cfg.Net.DialContext},
	}
	conn := newConnection(opts, httpClient)
	meta, err := misc.New(conn, nil).MetaGetter().Do(ctx)
	if err != nil {
		return nil, mapError(err)
	}
	s := &Session{client: newClient(conn, meta.Version), http: httpClient, opts: opts}
	return s, s.HealthCheck(ctx)
}

func newConnection(opts Options, httpClient *http.Client) *connection.Connection {
	headers := map[string]string{}
	if opts.APIKey != "" {
		headers["Authorization"] = "Bearer " + opts.APIKey
	}
	if opts.InferenceHeader != "" && opts.InferenceKey != "" {
		headers[opts.InferenceHeader] = opts.InferenceKey
	}
	conn := connection.NewConnection(opts.Scheme, opts.Host, httpClient, opts.Timeout, headers)
	runtime.SetFinalizer(conn, nil)
	return conn
}

// newClient pins the server version resolved during Connect. The client's own
// provider would otherwise re-dial on every request with a background context
// whenever the first lookup came back empty.
func newClient(conn *connection.Connection, version string) *client {
	provider := db.NewVersionProvider(func() string { return version })
	support := db.NewDBVersionSupport(provider)
	return &client{
		misc:    misc.New(conn, provider),
		schema:  schema.New(conn, provider),
		data:    data.New(conn, support),
		batch:   batch.New(conn, nil, support),
		graphql: graphql.New(conn),
		cluster: cluster.New(conn),
		alias:   alias.New(conn),
		backup:  backup.New(conn),
	}
}

func (s *Session) HealthCheck(ctx context.Context) error {
	ready, err := s.client.misc.ReadyChecker().Do(ctx)
	if err != nil {
		return mapError(err)
	}
	if !ready {
		return fmt.Errorf("%w: Weaviate at %s is not ready", plugin.ErrUnavailable, s.opts.Endpoint)
	}
	return nil
}

func (s *Session) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}

func (s *Session) Close() error {
	s.http.CloseIdleConnections()
	return nil
}

func parseOptions(cfg plugin.ConnectConfig) (Options, error) {
	rawURL := strings.TrimSpace(cfg.String("endpoint"))
	if rawURL == "" {
		rawURL = defaultEndpoint
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return Options{}, fmt.Errorf("%w: endpoint must be an http or https URL", plugin.ErrInvalidInput)
	}
	opts := Options{
		Endpoint:     u.Scheme + "://" + u.Host,
		Scheme:       u.Scheme,
		Host:         u.Host,
		Timeout:      broker.DurationValue(cfg.Config, "timeout", defaultTimeout),
		PageLimit:    broker.IntValue(cfg.Config, "page_limit", defaultPageLimit, 1, plugin.MaxPageLimit),
		VectorSample: broker.IntValue(cfg.Config, "vector_sample", defaultVectorSample, 10, maxVectorSample),
		ReadOnly:     broker.BoolValue(cfg.Config, "read_only", true),
	}
	switch consistency := broker.StringValue(cfg.Config, "consistency_level", "QUORUM"); consistency {
	case "ONE", "QUORUM", "ALL":
		opts.Consistency = consistency
	default:
		return Options{}, fmt.Errorf("%w: unsupported consistency level %q", plugin.ErrInvalidInput, consistency)
	}
	switch auth := broker.StringValue(cfg.Config, "auth", "none"); auth {
	case "none":
	case "api_key":
		opts.APIKey = strings.TrimSpace(cfg.String("api_key"))
		if opts.APIKey == "" {
			return Options{}, fmt.Errorf("%w: API key is required", plugin.ErrInvalidInput)
		}
	case "credential":
		if kind := cfg.CredentialKindFor(plugin.CredentialRefField); kind != "" && kind != plugin.CredentialKindAPIToken {
			return Options{}, fmt.Errorf("%w: Weaviate stored credentials must be API tokens", plugin.ErrInvalidInput)
		}
		opts.APIKey = dbcred.ResolvedSecret(cfg, plugin.CredentialRefField)
		if opts.APIKey == "" {
			return Options{}, fmt.Errorf("%w: stored credential has no API key", plugin.ErrInvalidInput)
		}
	default:
		return Options{}, fmt.Errorf("%w: unsupported authentication mode %q", plugin.ErrInvalidInput, auth)
	}

	header := strings.TrimSpace(cfg.String("inference_header"))
	if header != "" {
		if !validHeaderName(header) {
			return Options{}, fmt.Errorf("%w: inference header %q is not a valid header name", plugin.ErrInvalidInput, header)
		}
		opts.InferenceHeader = header
		opts.InferenceKey = dbcred.ResolvedSecret(cfg, inferenceCredentialField)
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

func validHeaderName(name string) bool {
	for _, r := range name {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			continue
		}
		return false
	}
	return name != ""
}

func configSchema() plugin.Schema {
	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Server", Fields: []plugin.Field{
			{Key: "endpoint", Label: "Endpoint", Type: plugin.FieldURL, Required: true, Default: defaultEndpoint, Placeholder: "https://weaviate.example.internal:8080", Help: "REST and GraphQL base URL. The gateway owns egress; the plugin never dials directly."},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: "none", Options: []plugin.Option{
				{Label: "Anonymous", Value: "none"},
				{Label: "API key", Value: "api_key"},
				{Label: "Stored API key", Value: "credential"},
			}},
			{Key: "api_key", Label: "API key", Type: plugin.FieldPassword, Secret: true, Required: true, Help: "Sent as an Authorization bearer token.", VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "api_key"}}}},
			{Key: plugin.CredentialRefField, Label: "Stored API key", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindAPIToken, Protocols: []string{protocolName},
			}, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "credential"}}}},
		}},
		{Name: "Vectorizer modules", Fields: []plugin.Field{
			{Key: "inference_header", Label: "Inference API header", Type: plugin.FieldAutocomplete, Help: "Header a vectorizer or reranker module needs for nearText and generative queries.", Options: []plugin.Option{
				{Label: "X-OpenAI-Api-Key", Value: "X-OpenAI-Api-Key"},
				{Label: "X-Cohere-Api-Key", Value: "X-Cohere-Api-Key"},
				{Label: "X-HuggingFace-Api-Key", Value: "X-HuggingFace-Api-Key"},
				{Label: "X-Google-Api-Key", Value: "X-Google-Api-Key"},
				{Label: "X-Anthropic-Api-Key", Value: "X-Anthropic-Api-Key"},
				{Label: "X-Jinaai-Api-Key", Value: "X-Jinaai-Api-Key"},
				{Label: "X-VoyageAI-Api-Key", Value: "X-VoyageAI-Api-Key"},
				{Label: "X-Mistral-Api-Key", Value: "X-Mistral-Api-Key"},
			}},
			{Key: inferenceCredentialField, Label: "Inference API key", Type: plugin.FieldCredentialRef, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindAPIToken, Protocols: []string{protocolName},
			}, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "inference_header", Op: plugin.OpNotEmpty}}}},
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
			{Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true, Help: "Blocks schema, object, tenant, alias, shard, and backup mutations."},
			{Key: "consistency_level", Label: "Consistency level", Type: plugin.FieldSelect, Required: true, Default: "QUORUM", Options: []plugin.Option{
				{Label: "One", Value: "ONE"},
				{Label: "Quorum", Value: "QUORUM"},
				{Label: "All", Value: "ALL"},
			}},
			{Key: "timeout", Label: "Request timeout", Type: plugin.FieldDuration, Default: "30s"},
			{Key: "page_limit", Label: "Page limit", Type: plugin.FieldNumber, Default: defaultPageLimit, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: plugin.MaxPageLimit}}},
			{Key: "vector_sample", Label: "Embedding sample size", Type: plugin.FieldNumber, Default: defaultVectorSample, Help: "Objects fetched for the 2D embedding projection.", Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 10}, {Type: plugin.ValidatorMax, Value: maxVectorSample}}},
		}},
	}}
}

func unwrap(sess plugin.Session) (*Session, error) {
	if s, ok := sess.(*Session); ok {
		return s, nil
	}
	if h, ok := sess.(interface{ Session() plugin.Session }); ok {
		if s, ok := h.Session().(*Session); ok {
			return s, nil
		}
	}
	return nil, fmt.Errorf("%w: no Weaviate session", plugin.ErrInvalidInput)
}

func session(rc *plugin.RequestContext) (*Session, error) { return unwrap(rc.Session) }

func ensureWritable(s *Session) error {
	if s.opts.ReadOnly {
		return fmt.Errorf("%w: connection is read-only", plugin.ErrForbidden)
	}
	return nil
}

// mapError translates weaviate-go-client failures into SDK sentinels, pulling the
// human message out of Weaviate's {"error":[{"message":...}]} bodies.
func mapError(err error) error {
	if err == nil {
		return nil
	}
	werr := weaviateFault(err)
	if werr == nil {
		return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	message := weaviateMessage(werr)
	switch werr.StatusCode {
	case http.StatusNotFound:
		return fmt.Errorf("%w: %s", plugin.ErrNotFound, message)
	case http.StatusUnauthorized:
		return fmt.Errorf("%w: %s", plugin.ErrUnauthorized, message)
	case http.StatusForbidden:
		return fmt.Errorf("%w: %s", plugin.ErrForbidden, message)
	case http.StatusConflict:
		return fmt.Errorf("%w: %s", plugin.ErrConflict, message)
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		return fmt.Errorf("%w: %s", plugin.ErrInvalidInput, message)
	default:
		return fmt.Errorf("%w: %s", plugin.ErrUnavailable, message)
	}
}
