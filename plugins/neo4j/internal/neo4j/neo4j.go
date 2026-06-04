// Package neo4j implements the Neo4j protocol plugin.
package neo4j

import (
	"context"
	"fmt"
	"net"
	"strconv"

	driver "github.com/neo4j/neo4j-go-driver/v6/neo4j"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j/config"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

const neo4jIconSVG = `<svg xmlns="http://www.w3.org/2000/svg" width="256" height="256" viewBox="0 0 128 128"><g fill="none"><path fill="#000" d="M63.333 32.567c-5.2.866-9.566 3-12.833 6.266c-3.867 3.867-5.833 8.5-6.5 15.367c-.3 3.133-.467 15.467-.2 15.467c.067 0 .7-.234 1.4-.534c1.633-.7 5.167-.7 7-.033l1.4.5l.167-8.033c.166-8.567.366-9.867 1.966-13.067c1.1-2.133 3.767-4.633 6.034-5.667c2.6-1.2 6.4-1.666 9.333-1.2c6.267 1.034 10 4.434 11.567 10.5c.633 2.434.666 3.7.666 17.1v14.434H93.4L93.233 67.9c-.1-14.9-.166-15.9-.866-18.567c-1.9-7.4-6.5-12.766-12.934-15.2c-3.433-1.3-6.7-1.8-11.2-1.766c-2.233.033-4.433.133-4.9.2z"/><path fill="#018BFF" d="M22.733 57.2c-2.866 1.433-4.4 4-4.4 7.467c0 1.1.2 2.5.467 3.133c.633 1.567 2.433 3.467 4 4.3c1.9 1 5.5 1 7.367.033l1.366-.7l4.267 2.9l4.267 2.934V81.7L35.8 84.633l-4.3 2.934l-1.1-.667c-1.6-.933-4.7-1.133-6.6-.4c-2 .767-4.067 2.6-4.833 4.333c-.834 1.767-.834 5.234 0 7c.7 1.567 2.333 3.3 3.8 4.067c.6.3 2.033.6 3.233.7c2.8.2 5.167-.733 6.867-2.733c1.366-1.6 2.266-4.4 2.033-6.334l-.167-1.366l4.3-2.9l4.3-2.9l1.534.7c2.333 1 5.8.766 8-.567c2.4-1.5 3.6-3.633 3.733-6.633c.1-2.1 0-2.567-.833-4.2c-2.167-4.134-7-5.7-11.134-3.634l-1.233.6l-4.233-2.9l-4.234-2.9l-.1-2.333c-.066-2.8-.866-4.6-2.833-6.233c-2.5-2.134-6.233-2.567-9.267-1.067z"/></g></svg>`

type Plugin struct{}

type Session struct {
	driver driver.Driver
	opts   options
}

type row map[string]any

type actionResult struct {
	OK bool `json:"ok"`
}

func New() *Plugin { return &Plugin{} }

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:          plugin.CurrentAPIVersion,
		Name:                protocolName,
		Version:             "0.1.0",
		Title:               "Neo4j",
		Description:         "Neo4j graph cockpit with databases, labels, relationship types, graph visualization, schema, Cypher editor, and guarded graph mutations.",
		Icon:                plugin.Icon{Type: plugin.IconSVG, Value: neo4jIconSVG},
		Category:            plugin.CategoryDatabases,
		Config:              configSchema(),
		Capabilities:        []plugin.Capability{"graph", "cypher", "schema", "nodes", "relationships"},
		SupportedTransports: []plugin.Transport{plugin.TransportDirect},
		Layout:              plugin.LayoutSidebarTree,
		Tree:                tree(),
		Resources:           resources(),
		Actions:             actions(),
		Streams: []plugin.Stream{
			{ID: rid("query"), Kind: plugin.StreamLogs, RouteID: rid("query")},
		},
	}
}

func (p *Plugin) Routes() []plugin.Route { return routes() }

func (p *Plugin) Connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	opts, err := parseOptions(cfg)
	if err != nil {
		return nil, err
	}
	d, err := newDriver(opts)
	if err != nil {
		return nil, err
	}
	s := &Session{driver: d, opts: opts}
	if err := s.HealthCheck(ctx); err != nil {
		_ = d.Close(context.Background())
		return nil, err
	}
	return s, nil
}

func (s *Session) HealthCheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.opts.QueryTimeout)
	defer cancel()
	return neo4jErr(s.driver.VerifyConnectivity(ctx))
}

func (s *Session) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}

func (s *Session) Close() error {
	return s.driver.Close(context.Background())
}

func newDriver(opts options) (driver.Driver, error) {
	auth, err := authToken(opts)
	if err != nil {
		return nil, err
	}
	d, err := driver.NewDriver(opts.URI(), auth, func(cfg *config.Config) {
		cfg.MaxConnectionPoolSize = opts.PoolSize
		cfg.SocketConnectTimeout = opts.ConnectTimeout
		cfg.ConnectionAcquisitionTimeout = opts.QueryTimeout
		cfg.MaxTransactionRetryTime = opts.RetryTime
		cfg.FetchSize = opts.FetchSize
		cfg.TelemetryDisabled = true
		if opts.TLSConfig != nil {
			cfg.TlsConfig = opts.TLSConfig
		}
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", plugin.ErrInvalidInput, err)
	}
	return d, nil
}

func authToken(opts options) (driver.AuthToken, error) {
	switch opts.Auth {
	case authNone:
		return driver.NoAuth(), nil
	case authPassword, authCredential:
		if opts.Username == "" {
			return driver.AuthToken{}, fmt.Errorf("%w: username is required", plugin.ErrInvalidInput)
		}
		if opts.Password == "" {
			return driver.AuthToken{}, fmt.Errorf("%w: password is required", plugin.ErrInvalidInput)
		}
		return driver.BasicAuth(opts.Username, opts.Password, opts.Realm), nil
	case authBearer, authStoredBearer:
		if opts.BearerToken == "" {
			return driver.AuthToken{}, fmt.Errorf("%w: bearer token is required", plugin.ErrInvalidInput)
		}
		return driver.BearerAuth(opts.BearerToken), nil
	default:
		return driver.AuthToken{}, fmt.Errorf("%w: unsupported authentication method %q", plugin.ErrInvalidInput, opts.Auth)
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

func icon(name string) plugin.Icon {
	return plugin.Icon{Type: plugin.IconLucide, Value: name}
}

func rid(name string) string { return protocolName + "." + name }

func hostPort(host string, port int) string {
	return net.JoinHostPort(host, strconv.Itoa(port))
}
