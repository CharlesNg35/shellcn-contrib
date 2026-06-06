package kafka

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type Session struct {
	client sarama.Client
	admin  sarama.ClusterAdmin
	opts   options
	net    plugin.NetTransport
}

func connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	if cfg.Net == nil {
		return nil, fmt.Errorf("%w: network transport is unavailable", plugin.ErrUnavailable)
	}
	opts, err := parseOptions(cfg)
	if err != nil {
		return nil, err
	}
	saramaCfg := saramaConfig(opts, cfg.Net)
	client, err := sarama.NewClient(opts.Brokers, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	s := &Session{client: client, admin: admin, opts: opts, net: cfg.Net}
	return s, s.HealthCheck(ctx)
}

func (s *Session) HealthCheck(context.Context) error {
	err := s.client.RefreshMetadata()
	if err != nil {
		return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	return nil
}

func (s *Session) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}

func (s *Session) Close() error {
	if s.admin != nil {
		_ = s.admin.Close()
	}
	if s.client != nil {
		return s.client.Close()
	}
	return nil
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
