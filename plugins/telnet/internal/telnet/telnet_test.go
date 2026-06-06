package telnet

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestManifestValidates(t *testing.T) {
	p := New()
	plugintest.ValidatePlugin(t, p)
}

func TestManifestExposesTerminalRecording(t *testing.T) {
	m := New().Manifest()
	if m.Name != "telnet" || m.Category != plugin.CategoryShell {
		t.Fatalf("unexpected manifest identity: %+v", m)
	}
	if len(m.CredentialKinds) != 0 {
		t.Fatalf("telnet should not declare credentials: %+v", m.CredentialKinds)
	}
	if len(m.Tabs) != 1 || m.Tabs[0].Type != plugin.PanelTerminalGrid || m.Tabs[0].Source.RouteID != "telnet.shell" {
		t.Fatalf("terminal tab not wired to telnet.shell: %+v", m.Tabs)
	}
	if cfg, ok := m.Tabs[0].Config.(plugin.TerminalGridConfig); !ok || cfg.MaxPanes != 6 || cfg.DefaultPanes != 1 || !cfg.Zoom || !cfg.Search {
		t.Fatalf("terminal grid config not declared: %+v", m.Tabs[0].Config)
	}
	if len(m.Streams) != 1 || m.Streams[0].Kind != plugin.StreamTerminal || m.Streams[0].RouteID != "telnet.shell" {
		t.Fatalf("terminal stream not declared: %+v", m.Streams)
	}
	if len(m.Recording) != 1 || m.Recording[0].Class != plugin.RecordingTerminal || !m.Recording[0].Authoritative {
		t.Fatalf("telnet should declare authoritative terminal recording: %+v", m.Recording)
	}
}

func TestConnectBuildsDefaultAddress(t *testing.T) {
	sess, err := Connect(context.Background(), plugin.ConnectConfig{Config: map[string]any{"host": " example.com "}})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	got := sess.(*Session).addr
	want := net.JoinHostPort("example.com", "23")
	if got != want {
		t.Fatalf("addr: got %q want %q", got, want)
	}
}

func TestConnectUsesConfiguredTransport(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	transport := &fakeNetTransport{conn: client}
	sess, err := Connect(context.Background(), plugin.ConnectConfig{
		Config: map[string]any{"host": "example.com", "port": 2323},
		Net:    transport,
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := sess.HealthCheck(context.Background()); err != nil {
		t.Fatalf("health check: %v", err)
	}
	if transport.network != "tcp" || transport.addr != "example.com:2323" {
		t.Fatalf("dialed %s %s, want tcp example.com:2323", transport.network, transport.addr)
	}
}

func TestSessionDialsPerTerminalChannelAndCloses(t *testing.T) {
	var dialed []string
	var conns []*fakeConn
	sess := NewSession("127.0.0.1:23", func(_ context.Context, addr string) (telnetConn, error) {
		dialed = append(dialed, addr)
		conn := &fakeConn{}
		conns = append(conns, conn)
		return conn, nil
	})

	ch, err := sess.OpenChannel(context.Background(), plugin.ChannelRequest{Kind: plugin.StreamTerminal})
	if err != nil {
		t.Fatalf("open first channel: %v", err)
	}
	if _, err := sess.OpenChannel(context.Background(), plugin.ChannelRequest{Kind: plugin.StreamLogs}); !errors.Is(err, plugin.ErrNotSupported) {
		t.Fatalf("non-terminal channel error: got %v want %v", err, plugin.ErrNotSupported)
	}
	if err := ch.Close(); err != nil {
		t.Fatalf("close first channel: %v", err)
	}

	ch, err = sess.OpenChannel(context.Background(), plugin.ChannelRequest{Kind: plugin.StreamTerminal})
	if err != nil {
		t.Fatalf("open second channel: %v", err)
	}
	if err := sess.Close(); err != nil {
		t.Fatalf("close session: %v", err)
	}
	if len(dialed) != 2 {
		t.Fatalf("dial count: got %d want 2", len(dialed))
	}
	if !conns[0].closed || !conns[1].closed {
		t.Fatalf("expected both telnet connections closed: %+v", conns)
	}
	if err := ch.Close(); err != nil {
		t.Fatalf("second channel close should be idempotent after session close: %v", err)
	}
}

func TestHealthCheckDialsAndClosesProbe(t *testing.T) {
	var dialed []string
	probe := &fakeConn{}
	sess := NewSession("127.0.0.1:23", func(_ context.Context, addr string) (telnetConn, error) {
		dialed = append(dialed, addr)
		return probe, nil
	})

	if err := sess.HealthCheck(context.Background()); err != nil {
		t.Fatalf("health check: %v", err)
	}
	if len(dialed) != 1 || dialed[0] != "127.0.0.1:23" {
		t.Fatalf("health check dialed %+v", dialed)
	}
	if !probe.closed {
		t.Fatal("health check probe connection should be closed")
	}
}

func TestHealthCheckReportsDialFailure(t *testing.T) {
	want := errors.New("refused")
	sess := NewSession("127.0.0.1:23", func(context.Context, string) (telnetConn, error) {
		return nil, want
	})

	if err := sess.HealthCheck(context.Background()); !errors.Is(err, plugin.ErrUnavailable) {
		t.Fatalf("health check error = %v, want ErrUnavailable", err)
	}
}

func TestTelnetDataConnEscapesAndFiltersProtocolBytes(t *testing.T) {
	raw := &scriptedNetConn{reader: bytes.NewReader([]byte{'a', 255, 251, 1, 'b', 255, 255, 'c', 255, 250, 1, 'x', 255, 240, 'd'})}
	conn := newTelnetDataConn(raw)
	defer conn.Close()

	buf := make([]byte, 5)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read telnet data: %v", err)
	}
	if want := []byte{'a', 'b', 255, 'c', 'd'}; !bytes.Equal(buf, want) {
		t.Fatalf("read bytes = %v, want %v", buf, want)
	}

	if n, err := conn.Write([]byte{'x', 255, 'y'}); err != nil || n != 3 {
		t.Fatalf("write telnet data: n=%d err=%v", n, err)
	}
	if want := []byte{'x', 255, 255, 'y'}; !bytes.Equal(raw.writer.Bytes(), want) {
		t.Fatalf("escaped bytes = %v, want %v", raw.writer.Bytes(), want)
	}
}

type scriptedNetConn struct {
	reader *bytes.Reader
	writer bytes.Buffer
	closed bool
}

func (c *scriptedNetConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func (c *scriptedNetConn) Write(p []byte) (int, error) {
	return c.writer.Write(p)
}

func (c *scriptedNetConn) Close() error {
	c.closed = true
	return nil
}

func (c *scriptedNetConn) LocalAddr() net.Addr {
	return dummyAddr("local")
}

func (c *scriptedNetConn) RemoteAddr() net.Addr {
	return dummyAddr("remote")
}

func (c *scriptedNetConn) SetDeadline(time.Time) error {
	return nil
}

func (c *scriptedNetConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *scriptedNetConn) SetWriteDeadline(time.Time) error {
	return nil
}

type dummyAddr string

func (d dummyAddr) Network() string { return "test" }
func (d dummyAddr) String() string  { return string(d) }

type fakeNetTransport struct {
	network string
	addr    string
	conn    net.Conn
}

func (f *fakeNetTransport) DialContext(_ context.Context, network, addr string) (net.Conn, error) {
	f.network = network
	f.addr = addr
	return f.conn, nil
}

func (f *fakeNetTransport) HTTP() (string, http.RoundTripper, bool) {
	return "", nil, false
}

type fakeConn struct {
	closed bool
}

func (f *fakeConn) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (f *fakeConn) Write(p []byte) (int, error) {
	return len(p), nil
}

func (f *fakeConn) Close() error {
	f.closed = true
	return nil
}
