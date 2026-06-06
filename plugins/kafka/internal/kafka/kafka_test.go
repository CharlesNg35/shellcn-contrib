package kafka

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestKafkaManifestValidates(t *testing.T) {
	p := New()
	m := p.Manifest()
	proj := plugintest.Projection(t, p)
	if proj.Category.Key != plugin.CategoryMessaging {
		t.Fatalf("category: got %q want %q", proj.Category.Key, plugin.CategoryMessaging)
	}
	if proj.Layout != plugin.LayoutSidebarTree {
		t.Fatalf("layout: got %q", proj.Layout)
	}
	if len(proj.Resources) != 2 {
		t.Fatalf("resources: got %d", len(proj.Resources))
	}
	if len(m.SupportedTransports) != 2 || m.SupportedTransports[0] != plugin.TransportDirect || m.SupportedTransports[1] != plugin.TransportAgent {
		t.Fatalf("unexpected transports: %+v", m.SupportedTransports)
	}
	if m.Agent == nil || m.Agent.Proxy.Mode != plugin.AgentTCP || !m.Agent.Proxy.Forward {
		t.Fatalf("kafka agent profile must use forwarded TCP proxy: %+v", m.Agent)
	}
	if m.Agent.Proxy.Address != "" {
		t.Fatalf("forwarded kafka agent profile should not pin a fixed address: %q", m.Agent.Proxy.Address)
	}
}

func TestKafkaConfigSchemaIsSpecific(t *testing.T) {
	m := New().Manifest()
	fields := fieldMap(m.Config)
	for _, key := range []string{"brokers", "client_id", "auth", "username", "password", "credential_id"} {
		if !fields[key] {
			t.Fatalf("missing field %q", key)
		}
	}
	for _, key := range []string{"management_url", "urls", "token"} {
		if fields[key] {
			t.Fatalf("kafka should not expose %q", key)
		}
	}
}

func TestValidatePartitionCount(t *testing.T) {
	cases := []struct {
		name           string
		topic          string
		count, current int32
		wantErr        bool
	}{
		{name: "increase ok", topic: "t", count: 6, current: 3},
		{name: "equal rejected", topic: "t", count: 3, current: 3, wantErr: true},
		{name: "decrease rejected", topic: "t", count: 2, current: 3, wantErr: true},
		{name: "empty topic rejected", topic: " ", count: 6, current: 3, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validatePartitionCount(tc.topic, tc.count, tc.current)
			if tc.wantErr {
				if !errors.Is(err, plugin.ErrInvalidInput) {
					t.Fatalf("want ErrInvalidInput, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateConfigEntry(t *testing.T) {
	cases := []struct {
		name, key, value string
		wantErr          bool
	}{
		{name: "ok", key: "retention.ms", value: "60000"},
		{name: "empty key", key: " ", value: "60000", wantErr: true},
		{name: "empty value", key: "retention.ms", value: "", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateConfigEntry(tc.key, tc.value)
			if tc.wantErr {
				if !errors.Is(err, plugin.ErrInvalidInput) {
					t.Fatalf("want ErrInvalidInput, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestStructuredMapFields(t *testing.T) {
	p := New()
	for _, tc := range []struct{ routeID, fieldKey string }{
		{"kafka.topic.create", "config"},
		{"kafka.message.produce", "headers"},
	} {
		field := routeField(t, p, tc.routeID, tc.fieldKey)
		if field.Type != plugin.FieldMap {
			t.Fatalf("%s.%s is %q, want map", tc.routeID, tc.fieldKey, field.Type)
		}
		if field.Item == nil || field.Item.Type != plugin.FieldText {
			t.Fatalf("%s.%s value item is not text", tc.routeID, tc.fieldKey)
		}
	}
}

func TestSaramaConfigUsesConnectConfigNet(t *testing.T) {
	transport := &recordingNetTransport{}
	cfg := saramaConfig(options{Timeout: time.Second}, transport)
	if !cfg.Net.Proxy.Enable {
		t.Fatal("sarama proxy dialer is not enabled")
	}
	ctxDialer, ok := cfg.Net.Proxy.Dialer.(interface {
		DialContext(context.Context, string, string) (net.Conn, error)
	})
	if !ok {
		t.Fatalf("sarama proxy dialer does not support context-aware dialing: %T", cfg.Net.Proxy.Dialer)
	}
	conn, err := cfg.Net.Proxy.Dialer.Dial("tcp", "broker-1:9092")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = conn.Close()
	conn, err = ctxDialer.DialContext(context.Background(), "tcp", "broker-2:9092")
	if err != nil {
		t.Fatalf("dial context: %v", err)
	}
	_ = conn.Close()
	if got := strings.Join(transport.callSnapshot(), ","); got != "tcp broker-1:9092,tcp broker-2:9092" {
		t.Fatalf("transport calls: got %q", got)
	}
}

func TestConnectUsesConnectConfigNet(t *testing.T) {
	transport := &recordingNetTransport{err: errors.New("blocked by test transport")}
	_, err := connect(context.Background(), plugin.ConnectConfig{
		Config: map[string]any{
			"brokers":   "broker-1:9092",
			"auth":      "none",
			"tls_mode":  "disable",
			"timeout":   "50ms",
			"read_only": true,
		},
		Net: transport,
	})
	if !errors.Is(err, plugin.ErrUnavailable) {
		t.Fatalf("connect error: got %v want ErrUnavailable", err)
	}
	if len(transport.callSnapshot()) == 0 {
		t.Fatal("connect did not use cfg.Net")
	}
}

func TestConnectRequiresNetTransport(t *testing.T) {
	_, err := connect(context.Background(), plugin.ConnectConfig{
		Config: map[string]any{
			"brokers":  "broker-1:9092",
			"auth":     "none",
			"tls_mode": "disable",
		},
	})
	if !errors.Is(err, plugin.ErrUnavailable) {
		t.Fatalf("connect error: got %v want ErrUnavailable", err)
	}
}

func routeField(t *testing.T, p plugin.Plugin, routeID, fieldKey string) *plugin.Field {
	t.Helper()
	for _, r := range p.Routes() {
		if r.ID != routeID || r.Input == nil {
			continue
		}
		for _, g := range r.Input.Groups {
			for i := range g.Fields {
				if g.Fields[i].Key == fieldKey {
					return &g.Fields[i]
				}
			}
		}
	}
	t.Fatalf("route %q has no field %q", routeID, fieldKey)
	return nil
}

func fieldMap(schema plugin.Schema) map[string]bool {
	out := map[string]bool{}
	for _, group := range schema.Groups {
		for _, field := range group.Fields {
			out[field.Key] = true
		}
	}
	return out
}

type recordingNetTransport struct {
	mu    sync.Mutex
	calls []string
	err   error
}

func (t *recordingNetTransport) DialContext(_ context.Context, network, addr string) (net.Conn, error) {
	t.mu.Lock()
	t.calls = append(t.calls, network+" "+addr)
	t.mu.Unlock()
	if t.err != nil {
		return nil, t.err
	}
	client, server := net.Pipe()
	_ = server.Close()
	return client, nil
}

func (t *recordingNetTransport) HTTP() (string, http.RoundTripper, bool) {
	return "", nil, false
}

func (t *recordingNetTransport) callSnapshot() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.calls...)
}
