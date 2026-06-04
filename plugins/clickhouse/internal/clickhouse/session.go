package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type Session struct {
	db   *sql.DB
	opts options

	mu      sync.Mutex
	running map[string]context.CancelFunc
}

func connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	opts, err := parseOptions(cfg)
	if err != nil {
		return nil, err
	}
	driverCfg, err := driverOptions(opts, cfg.Net)
	if err != nil {
		return nil, err
	}
	db := clickhousedriver.OpenDB(driverCfg)
	db.SetMaxOpenConns(opts.MaxConns)
	db.SetMaxIdleConns(opts.MaxConns)
	db.SetConnMaxIdleTime(5 * time.Minute)
	db.SetConnMaxLifetime(30 * time.Minute)
	s := &Session{db: db, opts: opts, running: map[string]context.CancelFunc{}}
	if err := s.HealthCheck(ctx); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

func unwrap(sess plugin.Session) (*Session, error) {
	if s, ok := sess.(*Session); ok {
		return s, nil
	}
	type sessionGetter interface {
		Session() plugin.Session
	}
	if h, ok := sess.(sessionGetter); ok {
		if s, ok := h.Session().(*Session); ok {
			return s, nil
		}
	}
	return nil, fmt.Errorf("%w: ClickHouse session unavailable", plugin.ErrUnavailable)
}

func (s *Session) HealthCheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.opts.QueryTimeout)
	defer cancel()
	if err := s.db.PingContext(ctx); err != nil {
		return fmt.Errorf("%w: ClickHouse ping: %v", plugin.ErrUnavailable, err)
	}
	return nil
}

func (s *Session) Close() error {
	s.mu.Lock()
	for id, cancel := range s.running {
		cancel()
		delete(s.running, id)
	}
	s.mu.Unlock()
	return s.db.Close()
}

func (s *Session) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}

func (s *Session) track(id string, cancel context.CancelFunc) {
	s.mu.Lock()
	s.running[id] = cancel
	s.mu.Unlock()
}

func (s *Session) untrack(id string) {
	s.mu.Lock()
	delete(s.running, id)
	s.mu.Unlock()
}

func (s *Session) cancelAll() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	cancelled := false
	for id, cancel := range s.running {
		cancel()
		delete(s.running, id)
		cancelled = true
	}
	return cancelled
}
