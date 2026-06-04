package telnet

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"

	gotelnet "github.com/reiver/go-telnet"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type telnetConn interface {
	io.ReadWriteCloser
}

type dialFunc func(context.Context, string) (telnetConn, error)

var dialTelnet dialFunc = func(ctx context.Context, addr string) (telnetConn, error) {
	type result struct {
		conn telnetConn
		err  error
	}
	done := make(chan result, 1)
	go func() {
		conn, err := gotelnet.DialTo(addr)
		if ctx.Err() != nil && conn != nil {
			_ = conn.Close()
		}
		done <- result{conn: conn, err: err}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-done:
		return res.conn, res.err
	}
}

// Session holds Telnet connection settings and active terminal channels.
type Session struct {
	addr string
	dial dialFunc

	mu       sync.Mutex
	closed   bool
	channels map[*terminalChannel]struct{}
}

func Connect(_ context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	host := cfg.String("host")
	if host == "" {
		return nil, fmt.Errorf("%w: host is required", plugin.ErrInvalidInput)
	}
	port, ok := cfg.Int("port")
	if !ok || port == 0 {
		port = 23
	}
	if port < 1 || port > 65535 {
		return nil, fmt.Errorf("%w: port must be between 1 and 65535", plugin.ErrInvalidInput)
	}
	return NewSession(net.JoinHostPort(host, strconv.Itoa(port)), dialTelnet), nil
}

func NewSession(addr string, dial dialFunc) *Session {
	if dial == nil {
		dial = dialTelnet
	}
	return &Session{addr: addr, dial: dial, channels: map[*terminalChannel]struct{}{}}
}

func (s *Session) HealthCheck(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return plugin.ErrUnavailable
	}
	addr := s.addr
	dial := s.dial
	s.mu.Unlock()

	conn, err := dial(ctx, addr)
	if err != nil {
		return fmt.Errorf("%w: telnet connect %s: %v", plugin.ErrUnavailable, addr, err)
	}
	_ = conn.Close()
	return nil
}

func (s *Session) OpenChannel(ctx context.Context, req plugin.ChannelRequest) (plugin.Channel, error) {
	if req.Kind != plugin.StreamTerminal {
		return nil, plugin.ErrNotSupported
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, plugin.ErrUnavailable
	}
	s.mu.Unlock()

	conn, err := s.dial(ctx, s.addr)
	if err != nil {
		return nil, fmt.Errorf("%w: telnet connect %s: %v", plugin.ErrUnavailable, s.addr, err)
	}
	ch := &terminalChannel{conn: conn}
	ch.release = func() {
		s.remove(ch)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		_ = conn.Close()
		return nil, plugin.ErrUnavailable
	}
	s.channels[ch] = struct{}{}
	return ch, nil
}

func (s *Session) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	channels := make([]*terminalChannel, 0, len(s.channels))
	for ch := range s.channels {
		channels = append(channels, ch)
	}
	s.channels = map[*terminalChannel]struct{}{}
	s.mu.Unlock()

	var err error
	for _, ch := range channels {
		if cerr := ch.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}

func (s *Session) remove(ch *terminalChannel) {
	s.mu.Lock()
	delete(s.channels, ch)
	s.mu.Unlock()
}

type terminalChannel struct {
	conn    telnetConn
	once    sync.Once
	release func()
}

func (c *terminalChannel) Kind() plugin.StreamKind { return plugin.StreamTerminal }

func (c *terminalChannel) Read(p []byte) (int, error) {
	return c.conn.Read(p)
}

func (c *terminalChannel) Write(p []byte) (int, error) {
	return c.conn.Write(p)
}

func (c *terminalChannel) Close() error {
	var err error
	c.once.Do(func() {
		err = c.conn.Close()
		if c.release != nil {
			c.release()
		}
	})
	return err
}
