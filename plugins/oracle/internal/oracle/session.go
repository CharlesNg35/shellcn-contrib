package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type Session struct {
	db   *sql.DB
	opts optionsData

	mu      sync.Mutex
	running map[string]context.CancelFunc
}

func connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	opts, err := parseOptions(cfg)
	if err != nil {
		return nil, err
	}
	db, err := openDB(opts, cfg.Net)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(opts.MaxConns)
	db.SetMaxIdleConns(opts.MaxConns)
	db.SetConnMaxIdleTime(opts.QueryTimeout)
	pingCtx, cancel := context.WithTimeout(ctx, opts.QueryTimeout)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, oracleErr(err)
	}
	return &Session{db: db, opts: opts, running: map[string]context.CancelFunc{}}, nil
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
	return nil, fmt.Errorf("%w: Oracle session is not available", plugin.ErrUnavailable)
}

func (s *Session) HealthCheck(ctx context.Context) error {
	if s == nil || s.db == nil {
		return plugin.ErrUnavailable
	}
	return oracleErr(s.db.PingContext(ctx))
}

func (s *Session) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	s.cancelAll()
	return s.db.Close()
}

func (s *Session) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}

func (s *Session) track(id string, cancel context.CancelFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running[id] = cancel
}

func (s *Session) untrack(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.running, id)
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
