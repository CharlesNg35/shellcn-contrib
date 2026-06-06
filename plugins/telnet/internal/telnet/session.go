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
	if cols, rows := terminalSize(req.Params); cols > 0 && rows > 0 {
		_ = ch.Resize(cols, rows)
	}
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

func terminalSize(params map[string]string) (int, int) {
	if len(params) == 0 {
		return 0, 0
	}
	cols, _ := strconv.Atoi(strings.TrimSpace(params["cols"]))
	rows, _ := strconv.Atoi(strings.TrimSpace(params["rows"]))
	if cols <= 0 || rows <= 0 {
		return 0, 0
	}
	return cols, rows
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

func (c *terminalChannel) Resize(cols, rows int) error {
	resizer, ok := c.conn.(interface{ Resize(int, int) error })
	if !ok {
		return nil
	}
	return resizer.Resize(cols, rows)
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
	mu     sync.Mutex
	cols   int
	rows   int
	naws   bool
	ttype  bool
}

func newTelnetDataConn(conn net.Conn) *telnetDataConn {
	return &telnetDataConn{conn: conn, reader: bufio.NewReader(conn)}
}

func (c *telnetDataConn) Read(p []byte) (int, error) {
	const (
		iac = 255
		se  = 240
		sb  = 250

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
			if c.reader.Buffered() == 0 {
				return n, nil
			}
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
			opt, err := c.reader.ReadByte()
			if err != nil {
				if n > 0 {
					return n, nil
				}
				return n, err
			}
			if err := c.negotiate(cmd, opt); err != nil {
				if n > 0 {
					return n, nil
				}
				return n, err
			}
		case sb:
			data, err := c.readSubnegotiation(iac, se)
			if err != nil {
				if n > 0 {
					return n, nil
				}
				return n, err
			}
			if err := c.handleSubnegotiation(data); err != nil {
				if n > 0 {
					return n, nil
				}
				return n, err
			}
		default:
		}
		if n > 0 && c.reader.Buffered() == 0 {
			return n, nil
		}
	}
	return n, nil
}

func (c *telnetDataConn) negotiate(cmd, opt byte) error {
	const (
		will = 251
		wont = 252
		do   = 253
		dont = 254

		echo            = 1
		suppressGoAhead = 3
		terminalType    = 24
		windowSize      = 31
	)

	switch cmd {
	case will:
		if opt == echo || opt == suppressGoAhead {
			return c.sendCommand(do, opt)
		}
		return c.sendCommand(dont, opt)
	case wont:
		return c.sendCommand(dont, opt)
	case do:
		switch opt {
		case suppressGoAhead:
			return c.sendCommand(will, opt)
		case terminalType:
			c.mu.Lock()
			defer c.mu.Unlock()
			c.ttype = true
			return c.writeRawLocked([]byte{255, will, opt})
		case windowSize:
			c.mu.Lock()
			defer c.mu.Unlock()
			c.naws = true
			if err := c.writeRawLocked([]byte{255, will, opt}); err != nil {
				return err
			}
			return c.sendWindowSizeLocked()
		default:
			return c.sendCommand(wont, opt)
		}
	case dont:
		c.mu.Lock()
		if opt == terminalType {
			c.ttype = false
		}
		if opt == windowSize {
			c.naws = false
		}
		c.mu.Unlock()
		return c.sendCommand(wont, opt)
	}
	return nil
}

func (c *telnetDataConn) readSubnegotiation(iac, se byte) ([]byte, error) {
	var data []byte
	for {
		b, err := c.reader.ReadByte()
		if err != nil {
			return nil, err
		}
		if b != iac {
			data = append(data, b)
			continue
		}
		next, err := c.reader.ReadByte()
		if err != nil {
			return nil, err
		}
		if next == se {
			return data, nil
		}
		if next == iac {
			data = append(data, iac)
		}
	}
}

func (c *telnetDataConn) handleSubnegotiation(data []byte) error {
	const (
		iac          = 255
		se           = 240
		sb           = 250
		terminalType = 24
		ttypeIs      = 0
		ttypeSend    = 1
	)
	c.mu.Lock()
	ttype := c.ttype
	c.mu.Unlock()
	if len(data) < 2 || data[0] != terminalType || data[1] != ttypeSend || !ttype {
		return nil
	}
	payload := []byte{iac, sb, terminalType, ttypeIs}
	payload = append(payload, []byte("xterm-256color")...)
	payload = append(payload, iac, se)
	return c.writeRaw(payload)
}

func (c *telnetDataConn) Resize(cols, rows int) error {
	if cols <= 0 || rows <= 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cols, c.rows = cols, rows
	if !c.naws {
		return nil
	}
	return c.sendWindowSizeLocked()
}

func (c *telnetDataConn) sendCommand(cmd, opt byte) error {
	const iac = 255
	return c.writeRaw([]byte{iac, cmd, opt})
}

func (c *telnetDataConn) sendWindowSizeLocked() error {
	const (
		iac        = 255
		se         = 240
		sb         = 250
		windowSize = 31
	)
	if c.cols <= 0 || c.rows <= 0 {
		return nil
	}
	cols, rows := uint16(c.cols), uint16(c.rows)
	return c.writeRawLocked([]byte{
		iac, sb, windowSize,
		byte(cols >> 8), byte(cols),
		byte(rows >> 8), byte(rows),
		iac, se,
	})
}

func (c *telnetDataConn) Write(p []byte) (int, error) {
	const iac = 255
	escaped := bytes.NewBuffer(make([]byte, 0, len(p)))
	for i, b := range p {
		switch b {
		case iac:
			escaped.WriteByte(iac)
			escaped.WriteByte(iac)
		case '\r':
			escaped.WriteByte('\r')
			if i+1 >= len(p) || (p[i+1] != '\n' && p[i+1] != 0) {
				escaped.WriteByte('\n')
			}
		default:
			escaped.WriteByte(b)
		}
	}
	if err := c.writeRaw(escaped.Bytes()); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *telnetDataConn) writeRaw(p []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writeRawLocked(p)
}

func (c *telnetDataConn) writeRawLocked(p []byte) error {
	return writeAll(c.conn, p)
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
