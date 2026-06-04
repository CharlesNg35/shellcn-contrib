package nats

import (
	"context"
	"fmt"
	"time"

	natsclient "github.com/nats-io/nats.go"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type Session struct {
	conn *natsclient.Conn
	js   natsclient.JetStreamContext
	opts options
}

func connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	opts, err := parseOptions(cfg)
	if err != nil {
		return nil, err
	}
	conn, err := natsclient.Connect(stringsJoin(opts.URLs), connectOptions(cfg, opts)...)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	js, err := conn.JetStream(natsclient.MaxWait(opts.Timeout))
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	s := &Session{conn: conn, js: js, opts: opts}
	return s, s.HealthCheck(ctx)
}

func (s *Session) HealthCheck(ctx context.Context) error {
	if !s.conn.IsConnected() {
		return plugin.ErrUnavailable
	}
	flushCtx, cancel := healthCheckContext(ctx, s.opts.Timeout)
	defer cancel()
	if err := s.conn.FlushWithContext(flushCtx); err != nil {
		return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	return nil
}

func healthCheckContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	return context.WithTimeout(ctx, timeout)
}

func (s *Session) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}

func (s *Session) Close() error {
	if s.conn != nil {
		s.conn.Close()
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

func stringsJoin(in []string) string {
	if len(in) == 0 {
		return ""
	}
	out := in[0]
	for _, item := range in[1:] {
		out += "," + item
	}
	return out
}
