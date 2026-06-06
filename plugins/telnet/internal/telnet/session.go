package telnet

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type telnetConn interface {
	io.ReadWriteCloser
}

type dialFunc func(context.Context, string) (telnetConn, error)

type directNetTransport struct{}

func (directNetTransport) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, network, addr)
}

func (directNetTransport) HTTP() (string, http.RoundTripper, bool) {
	return "", nil, false
}

func dialTelnet(netTransport plugin.NetTransport) dialFunc {
	if netTransport == nil {
		netTransport = directNetTransport{}
	}
	return func(ctx context.Context, addr string) (telnetConn, error) {
		conn, err := netTransport.DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}
		return newTelnetDataConn(conn), nil
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
	host := strings.TrimSpace(cfg.String("host"))
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
	return NewSession(net.JoinHostPort(host, strconv.Itoa(port)), dialTelnet(cfg.Net)), nil
}

func NewSession(addr string, dial dialFunc) *Session {
	if dial == nil {
		dial = dialTelnet(nil)
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

type telnetDataConn struct {
	conn   net.Conn
	reader *bufio.Reader
}

func newTelnetDataConn(conn net.Conn) *telnetDataConn {
	return &telnetDataConn{conn: conn, reader: bufio.NewReader(conn)}
}

func (c *telnetDataConn) Read(p []byte) (int, error) {
	const (
		iac  = 255
		se   = 240
		sb   = 250
		will = 251
		wont = 252
		do   = 253
		dont = 254
	)

	n := 0
	for n < len(p) {
		b, err := c.reader.ReadByte()
		if err != nil {
			if n > 0 {
				return n, nil
			}
			return n, err
		}
		if b != iac {
			p[n] = b
			n++
			continue
		}

		cmd, err := c.reader.ReadByte()
		if err != nil {
			if n > 0 {
				return n, nil
			}
			return n, err
		}
		switch cmd {
		case iac:
			p[n] = iac
			n++
		case will, wont, do, dont:
			if _, err := c.reader.ReadByte(); err != nil {
				if n > 0 {
					return n, nil
				}
				return n, err
			}
		case sb:
			if err := c.discardSubnegotiation(iac, se); err != nil {
				if n > 0 {
					return n, nil
				}
				return n, err
			}
		default:
		}
	}
	return n, nil
}

func (c *telnetDataConn) discardSubnegotiation(iac, se byte) error {
	for {
		b, err := c.reader.ReadByte()
		if err != nil {
			return err
		}
		if b != iac {
			continue
		}
		next, err := c.reader.ReadByte()
		if err != nil {
			return err
		}
		if next == se {
			return nil
		}
	}
}

func (c *telnetDataConn) Write(p []byte) (int, error) {
	const iac = 255
	escaped := bytes.NewBuffer(make([]byte, 0, len(p)))
	for _, b := range p {
		escaped.WriteByte(b)
		if b == iac {
			escaped.WriteByte(iac)
		}
	}
	if err := writeAll(c.conn, escaped.Bytes()); err != nil {
		return 0, err
	}
	return len(p), nil
}

func writeAll(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		p = p[n:]
	}
	return nil
}

func (c *telnetDataConn) Close() error {
	return c.conn.Close()
}
