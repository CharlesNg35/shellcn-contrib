package surrealdb

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/connection"
	shttp "github.com/surrealdb/surrealdb.go/pkg/connection/http"
	"github.com/surrealdb/surrealdb.go/pkg/logger"
)

// session is the per-connection runtime: one authenticated SurrealDB client plus
// the gateway-supplied transport. All connection state lives here; the Plugin
// value holds none.
type session struct {
	connID string
	opts   options
	net    plugin.NetTransport

	mu sync.Mutex
	db *surrealdb.DB
}

// connect opens the SurrealDB HTTP client, routing every byte through the
// gateway's transport (cfg.Net), then authenticates and selects ns/db. Using
// cfg.Net.DialContext means direct and agent connections share this one path and
// the gateway stays the audited egress choke point.
func newSession(ctx context.Context, cfg plugin.ConnectConfig) (*session, error) {
	opts, err := parseOptions(cfg)
	if err != nil {
		return nil, err
	}
	s := &session{connID: cfg.ConnectionID, opts: opts, net: cfg.Net}

	db, err := s.dial(ctx)
	if err != nil {
		return nil, err
	}
	s.db = db
	return s, nil
}

func (s *session) dial(ctx context.Context) (*surrealdb.DB, error) {
	conf := connection.NewConfig(s.opts.baseURL())
	// The default driver logger writes to stdout, which go-plugin reserves for the
	// handshake — silence it so plugin logs never corrupt the control channel.
	conf.Logger = logger.New(slog.DiscardHandler)

	conn := shttp.New(conf)
	conn.SetHTTPClient(&http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext:           s.net.DialContext, // egress via the gateway
			MaxIdleConns:          8,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
		},
	})

	db, err := surrealdb.FromConnection(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("%w: connect SurrealDB: %v", plugin.ErrUnavailable, err)
	}
	// Use must precede SignIn: over HTTP every RPC (signin included) requires the
	// Surreal-NS/Surreal-DB headers, which Use stores client-side.
	if err := db.Use(ctx, s.opts.namespace, s.opts.database); err != nil {
		return nil, fmt.Errorf("%w: use ns/db: %v", plugin.ErrUnavailable, err)
	}
	if s.opts.username != "" {
		if err := s.signIn(ctx, db); err != nil {
			return nil, err
		}
	}
	return db, nil
}

// signIn tries the credential at root, then namespace, then database level — the
// config doesn't say which kind of user it is, and SurrealDB scopes users to
// exactly one level.
func (s *session) signIn(ctx context.Context, db *surrealdb.DB) error {
	attempts := []surrealdb.Auth{
		{Username: s.opts.username, Password: s.opts.password},
		{Namespace: s.opts.namespace, Username: s.opts.username, Password: s.opts.password},
		{Namespace: s.opts.namespace, Database: s.opts.database, Username: s.opts.username, Password: s.opts.password},
	}
	var lastErr error
	for _, auth := range attempts {
		if _, err := db.SignIn(ctx, auth); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}
	return fmt.Errorf("%w: sign in: %v", plugin.ErrUnauthorized, lastErr)
}

// client returns the live DB handle, re-dialing once if a previous health check
// tore it down.
func (s *session) client(ctx context.Context) (*surrealdb.DB, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db != nil {
		return s.db, nil
	}
	db, err := s.dial(ctx)
	if err != nil {
		return nil, err
	}
	s.db = db
	return s.db, nil
}

// HealthCheck pings SurrealDB so the gateway can probe an idle session.
func (s *session) HealthCheck(ctx context.Context) error {
	db, err := s.client(ctx)
	if err != nil {
		return err
	}
	if _, err := surrealdb.Query[any](ctx, db, "RETURN true", nil); err != nil {
		return fmt.Errorf("%w: health: %v", plugin.ErrUnavailable, err)
	}
	return nil
}

// OpenChannel backs the interactive REPL terminal. The "channel" is a pseudo
// byte-stream: lines written by the browser are executed as SurrealQL and the
// formatted results are read back. The gateway pins the session while it's open
// and records the stream.
func (s *session) OpenChannel(ctx context.Context, req plugin.ChannelRequest) (plugin.Channel, error) {
	if req.Kind != plugin.StreamTerminal {
		return nil, plugin.ErrNotSupported
	}
	db, err := s.client(ctx)
	if err != nil {
		return nil, err
	}
	return newREPL(db, s.opts), nil
}

// Close tears the SurrealDB client down.
func (s *session) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db != nil {
		_ = s.db.Close(context.Background())
		s.db = nil
	}
	return nil
}

// proxyRoundTripper builds an HTTP transport for the open-in-browser reverse
// proxy, also routed through the gateway.
func (s *session) proxyTransport() http.RoundTripper {
	return &http.Transport{DialContext: s.net.DialContext}
}

var _ io.Closer = (*session)(nil)
