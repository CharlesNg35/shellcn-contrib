package etcd

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync/atomic"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type Session struct {
	client *clientv3.Client
	opts   options
	closed atomic.Bool
}

func connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	opts, err := parseOptions(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.Net == nil {
		return nil, fmt.Errorf("%w: network transport is required", plugin.ErrUnavailable)
	}
	clientCfg, err := clientConfig(opts, cfg.Net)
	if err != nil {
		return nil, err
	}
	client, err := clientv3.New(clientCfg)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	s := &Session{client: client, opts: opts}
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
	return nil, fmt.Errorf("%w: etcd session unavailable", plugin.ErrUnavailable)
}

func (s *Session) endpoint() string {
	return net.JoinHostPort(s.opts.Host, strconv.Itoa(s.opts.Port))
}

func (s *Session) HealthCheck(ctx context.Context) error {
	if err := s.ensureOpen(); err != nil {
		return err
	}
	reqCtx, cancel := context.WithTimeout(ctx, s.opts.Timeout)
	defer cancel()
	if _, err := s.client.Status(reqCtx, s.endpoint()); err != nil {
		return fmt.Errorf("%w: etcd status: %v", plugin.ErrUnavailable, err)
	}
	return nil
}

func (s *Session) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}

func (s *Session) Close() error {
	s.closed.Store(true)
	if s.client == nil {
		return nil
	}
	return s.client.Close()
}

func (s *Session) ensureOpen() error {
	if s == nil || s.closed.Load() {
		return fmt.Errorf("%w: etcd session closed", plugin.ErrUnavailable)
	}
	return nil
}
