// Package dynamodb implements the Amazon DynamoDB protocol plugin.
package dynamodb

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn-contrib/shared/dbcred"
	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	protocolName     = "dynamodb"
	defaultRegion    = "us-east-1"
	defaultTimeout   = 15 * time.Second
	defaultPageLimit = 100
	credentialField  = "credential_id"
)

const iconSVG = `<svg height=800px preserveAspectRatio=xMidYMid version=1.1 viewBox="-16.5 0 289 289"width=800px xmlns=http://www.w3.org/2000/svg xmlns:xlink=http://www.w3.org/1999/xlink><g><path d="M165.258,288.501 L168.766,288.501 L226.027,259.867 L226.98,258.52 L226.98,29.964 L226.027,28.61 L168.766,0 L165.215,0 L165.258,288.501"fill=#5294CF></path><path d="M90.741,288.501 L87.184,288.501 L29.972,259.867 L28.811,257.87 L28.222,31.128 L29.972,28.61 L87.184,0 L90.785,0 L90.741,288.501"fill=#1F5B98></path><path d="M87.285,0 L168.711,0 L168.711,288.501 L87.285,288.501 L87.285,0 Z"fill=#2D72B8></path><path d="M256,137.769 L254.065,137.34 L226.437,134.764 L226.027,134.968 L168.715,132.676 L87.285,132.676 L29.972,134.968 L29.972,91.264 L29.912,91.296 L29.972,91.168 L87.285,77.888 L168.715,77.888 L226.027,91.168 L247.096,102.367 L247.096,95.167 L256,94.193 L255.078,92.395 L226.886,72.236 L226.027,72.515 L168.715,54.756 L87.285,54.756 L29.972,72.515 L29.972,28.61 L0,63.723 L0,94.389 L0.232,94.221 L8.904,95.167 L8.904,102.515 L0,107.28 L0,137.793 L0.232,137.769 L8.904,137.897 L8.904,150.704 L1.422,150.816 L0,150.68 L0,181.205 L8.904,185.993 L8.904,193.426 L0.373,194.368 L0,194.088 L0,224.749 L29.972,259.867 L29.972,215.966 L87.285,233.725 L168.715,233.725 L226.196,215.914 L226.96,216.249 L254.781,196.387 L256,194.408 L247.096,193.426 L247.096,186.142 L245.929,185.676 L226.886,195.941 L226.196,197.381 L168.715,210.584 L168.715,210.6 L87.285,210.6 L87.285,210.584 L29.972,197.325 L29.972,153.461 L87.285,155.745 L87.285,155.801 L168.715,155.801 L226.027,153.461 L227.332,154.061 L254.111,151.755 L256,150.832 L247.096,150.704 L247.096,137.897 L256,137.769"fill=#1A476F></path><path d="M226.027,215.966 L226.027,259.867 L256,224.749 L256,194.288 L226.2,215.914 L226.027,215.966"fill=#2D72B8></path><path d="M226.027,197.421 L226.2,197.381 L256,181.353 L256,150.704 L226.027,153.461 L226.027,197.421"fill=#2D72B8></path><path d="M226.2,91.208 L226.027,91.168 L226.027,134.968 L256,137.769 L256,107.135 L226.2,91.208"fill=#2D72B8></path><path d="M226.2,72.687 L256,94.193 L256,63.731 L226.027,28.61 L226.027,72.515 L226.2,72.575 L226.2,72.687"fill=#2D72B8></path></g></svg>`

type Plugin struct{}

type Options struct {
	Endpoint      string
	Region        string
	Auth          string
	AccessKeyID   string
	SecretKey     string
	SessionToken  string
	TablePrefix   string
	TLSConfig     *tls.Config
	Timeout       time.Duration
	PageLimit     int
	ReadOnly      bool
	ConfirmWrites bool
}

type Session struct {
	client *awsdynamodb.Client
	opts   Options
}

type row map[string]any

type actionResult struct {
	OK bool `json:"ok"`
}

func New() plugin.Plugin { return Plugin{} }

func (Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:          plugin.CurrentAPIVersion,
		Name:                protocolName,
		Version:             "0.1.0",
		Title:               "DynamoDB",
		Description:         "DynamoDB cockpit with tables, indexes, items, backups, TTL, tags, PartiQL, and guarded item/table operations.",
		Icon:                plugin.Icon{Type: plugin.IconSVG, Value: iconSVG},
		Category:            plugin.CategoryDatabases,
		Config:              configSchema(),
		Capabilities:        []plugin.Capability{"tables", "items", "indexes", "partiql", "backups"},
		SupportedTransports: []plugin.Transport{plugin.TransportDirect},
		Layout:              plugin.LayoutSidebarTree,
		Tree:                tree(),
		Resources:           resources(),
		Actions:             actions(),
		Streams:             []plugin.Stream{{ID: rid("partiql"), Kind: plugin.StreamLogs, RouteID: rid("partiql")}},
	}
}

func (Plugin) Routes() []plugin.Route { return routes() }

func (Plugin) Connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	opts, err := parseOptions(cfg)
	if err != nil {
		return nil, err
	}
	client, err := newDynamoClient(ctx, cfg, opts)
	if err != nil {
		return nil, err
	}
	s := &Session{client: client, opts: opts}
	return s, s.HealthCheck(ctx)
}

func (s *Session) HealthCheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.opts.Timeout)
	defer cancel()
	_, err := s.client.ListTables(ctx, &awsdynamodb.ListTablesInput{Limit: aws.Int32(1)})
	return ddbErr(err)
}

func (s *Session) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}

func (s *Session) Close() error { return nil }

func newDynamoClient(ctx context.Context, cfg plugin.ConnectConfig, opts Options) (*awsdynamodb.Client, error) {
	httpClient := &http.Client{
		Timeout: opts.Timeout,
		Transport: &http.Transport{
			DialContext:     cfg.Net.DialContext,
			TLSClientConfig: opts.TLSConfig,
		},
	}
	var awsCfg aws.Config
	var err error
	if opts.Auth == "default_chain" {
		awsCfg, err = awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(opts.Region), awsconfig.WithHTTPClient(httpClient))
		if err != nil {
			return nil, fmt.Errorf("%w: load AWS configuration: %v", plugin.ErrInvalidInput, err)
		}
	} else {
		awsCfg = aws.Config{
			Region:      opts.Region,
			HTTPClient:  httpClient,
			Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(opts.AccessKeyID, opts.SecretKey, opts.SessionToken)),
		}
	}
	return awsdynamodb.NewFromConfig(awsCfg, func(o *awsdynamodb.Options) {
		if opts.Endpoint != "" {
			o.BaseEndpoint = aws.String(opts.Endpoint)
		}
	}), nil
}

func parseOptions(cfg plugin.ConnectConfig) (Options, error) {
	opts := Options{
		Endpoint:      strings.TrimSpace(cfg.String("endpoint")),
		Region:        broker.StringValue(cfg.Config, "region", defaultRegion),
		Auth:          broker.StringValue(cfg.Config, "auth", "access_key"),
		TablePrefix:   strings.TrimSpace(cfg.String("table_prefix")),
		Timeout:       broker.DurationValue(cfg.Config, "timeout", defaultTimeout),
		PageLimit:     broker.IntValue(cfg.Config, "page_limit", defaultPageLimit, 1, plugin.MaxPageLimit),
		ReadOnly:      broker.BoolValue(cfg.Config, "read_only", true),
		ConfirmWrites: broker.BoolValue(cfg.Config, "confirm_writes", true),
	}
	if opts.Region == "" {
		return Options{}, fmt.Errorf("%w: region is required", plugin.ErrInvalidInput)
	}
	if opts.Endpoint != "" {
		u, err := url.Parse(opts.Endpoint)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return Options{}, fmt.Errorf("%w: endpoint must be an absolute URL", plugin.ErrInvalidInput)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return Options{}, fmt.Errorf("%w: endpoint scheme must be http or https", plugin.ErrInvalidInput)
		}
		opts.Endpoint = strings.TrimRight(u.String(), "/")
	}
	switch opts.Auth {
	case "access_key":
		opts.AccessKeyID = strings.TrimSpace(cfg.String("access_key_id"))
		opts.SecretKey = cfg.String("secret_access_key")
		opts.SessionToken = cfg.String("session_token")
	case "credential":
		if kind := dbcred.ResolvedKind(cfg, credentialField); kind != "" && kind != plugin.CredentialKindCloudAccessKey {
			return Options{}, fmt.Errorf("%w: DynamoDB stored credentials must be cloud access keys", plugin.ErrInvalidInput)
		}
		opts.AccessKeyID = dbcred.ResolvedIdentity(cfg, credentialField)
		opts.SecretKey = dbcred.ResolvedSecret(cfg, credentialField)
		opts.SessionToken = cfg.String("session_token")
	case "default_chain":
	default:
		return Options{}, fmt.Errorf("%w: unsupported authentication method %q", plugin.ErrInvalidInput, opts.Auth)
	}
	if opts.Auth != "default_chain" {
		if opts.AccessKeyID == "" {
			return Options{}, fmt.Errorf("%w: access key id is required", plugin.ErrInvalidInput)
		}
		if opts.SecretKey == "" {
			return Options{}, fmt.Errorf("%w: secret access key is required", plugin.ErrInvalidInput)
		}
	}
	host := ""
	if opts.Endpoint != "" {
		if u, err := url.Parse(opts.Endpoint); err == nil {
			host = u.Hostname()
		}
	}
	tlsConfig, err := sqldb.TLSConfig(sqldb.TLSOptions{
		Mode:          broker.StringValue(cfg.Config, "tls_mode", "verify-full"),
		Host:          host,
		CACertificate: cfg.String("ca_certificate"),
	})
	if err != nil {
		return Options{}, err
	}
	opts.TLSConfig = tlsConfig
	return opts, nil
}

func configSchema() plugin.Schema {
	accessKey := &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "access_key"}}}
	staticCredentials := &plugin.Condition{AnyOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "access_key"}, {Field: "auth", Op: plugin.OpEq, Value: "credential"}}}
	stored := &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "credential"}}}
	verifyTLS := &plugin.Condition{AllOf: []plugin.Rule{{Field: "tls_mode", Op: plugin.OpIn, Value: []any{"verify-ca", "verify-full"}}}}
	return plugin.Schema{Groups: []plugin.Group{
		{Name: "AWS", Fields: []plugin.Field{
			{Key: "region", Label: "Region", Type: plugin.FieldText, Required: true, Default: defaultRegion, Placeholder: "us-east-1"},
			{Key: "endpoint", Label: "Endpoint override", Type: plugin.FieldText, Placeholder: "http://localhost:8000", Help: "Optional DynamoDB-compatible endpoint, for example DynamoDB Local."},
			{Key: "table_prefix", Label: "Table prefix filter", Type: plugin.FieldText, Placeholder: "prod_", Help: "Optional prefix used to limit listed tables."},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: "access_key", Options: []plugin.Option{
				{Label: "Access key", Value: "access_key"},
				{Label: "Stored access key", Value: "credential"},
				{Label: "AWS default provider chain", Value: "default_chain"},
			}},
			{Key: "access_key_id", Label: "Access key ID", Type: plugin.FieldText, Required: true, VisibleWhen: accessKey},
			{Key: "secret_access_key", Label: "Secret access key", Type: plugin.FieldPassword, Required: true, Secret: true, VisibleWhen: accessKey},
			{Key: "session_token", Label: "Session token", Type: plugin.FieldPassword, Secret: true, VisibleWhen: staticCredentials},
			{Key: credentialField, Label: "Stored access key", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindCloudAccessKey, Protocols: []string{protocolName},
			}, VisibleWhen: stored},
		}},
		{Name: "TLS", Fields: []plugin.Field{
			{Key: "tls_mode", Label: "TLS mode", Type: plugin.FieldSelect, Required: true, Default: "verify-full", Options: []plugin.Option{
				{Label: "Disable", Value: "disable"},
				{Label: "Require", Value: "require"},
				{Label: "Verify CA", Value: "verify-ca"},
				{Label: "Verify full", Value: "verify-full"},
			}},
			{Key: "ca_certificate", Label: "CA certificate", Type: plugin.FieldTextarea, Secret: true, VisibleWhen: verifyTLS},
		}},
		{Name: "Safety", Fields: []plugin.Field{
			{Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true, Help: "Blocks table, item, backup, TTL, and non-read PartiQL operations."},
			{Key: "confirm_writes", Label: "Confirm writes", Type: plugin.FieldToggle, Default: true, Help: "Requires confirmation before write or destructive PartiQL operations."},
			{Key: "timeout", Label: "Request timeout", Type: plugin.FieldDuration, Default: defaultTimeout.String()},
			{Key: "page_limit", Label: "Page limit", Type: plugin.FieldNumber, Default: defaultPageLimit, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: plugin.MaxPageLimit}}},
		}},
	}}
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

func rid(suffix string) string { return protocolName + "." + suffix }

func icon(name string) plugin.Icon { return plugin.Icon{Type: plugin.IconLucide, Value: name} }
