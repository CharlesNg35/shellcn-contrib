package telnet

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

func TestManifestValidates(t *testing.T) {
	p := New()
	if err := plugin.Validate(p.Manifest(), p.Routes()); err != nil {
		t.Fatalf("telnet manifest invalid: %v", err)
	}
}

func TestManifestExposesTerminalRecording(t *testing.T) {
	m := New().Manifest()
	if m.Name != "telnet" || m.Category != plugin.CategoryShell {
		t.Fatalf("unexpected manifest identity: %+v", m)
	}
	if len(m.CredentialKinds) != 0 {
		t.Fatalf("telnet should not declare credentials: %+v", m.CredentialKinds)
	}
	if len(m.Tabs) != 1 || m.Tabs[0].Type != plugin.PanelTerminal || m.Tabs[0].Source.RouteID != "telnet.shell" {
		t.Fatalf("terminal tab not wired to telnet.shell: %+v", m.Tabs)
	}
	if len(m.Streams) != 1 || m.Streams[0].Kind != plugin.StreamTerminal || m.Streams[0].RouteID != "telnet.shell" {
		t.Fatalf("terminal stream not declared: %+v", m.Streams)
	}
	if len(m.Recording) != 1 || m.Recording[0].Class != plugin.RecordingTerminal || !m.Recording[0].Authoritative {
		t.Fatalf("telnet should declare authoritative terminal recording: %+v", m.Recording)
	}
}

func TestConnectBuildsDefaultAddress(t *testing.T) {
	sess, err := Connect(context.Background(), plugin.ConnectConfig{Config: map[string]any{"host": "example.com"}})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	got := sess.(*Session).addr
	want := net.JoinHostPort("example.com", "23")
	if got != want {
		t.Fatalf("addr: got %q want %q", got, want)
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
