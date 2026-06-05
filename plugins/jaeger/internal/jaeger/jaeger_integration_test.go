package jaeger

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	contribtest "github.com/charlesng35/shellcn-contrib/shared/plugintest"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestJaegerPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_JAEGER_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_JAEGER_INTEGRATION=1 to run against Jaeger")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cfg, zipkinEndpoint := jaegerIntegrationConfig(ctx, t)
	pushZipkinSpan(ctx, t, zipkinEndpoint)

	p := New()
	sess, err := p.Connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	h := contribtest.NewHarness(t, p.Routes())

	waitForService(ctx, t, h, sess, "checkout")
	h.Call(ctx, rid("overview"), sess, nil, nil, nil)
	h.Call(ctx, rid("services.tree"), sess, nil, nil, nil)
	operations := pageItems(h.Call(ctx, rid("operations.list"), sess, map[string]string{"service": "checkout"}, nil, nil))
	if !hasName(operations, "GET /checkout") {
		t.Fatalf("expected operation, got %#v", operations)
	}
	traces := pageItems(h.Call(ctx, rid("traces.list"), sess, map[string]string{"service": "checkout"}, url.Values{"limit": []string{"20"}}, nil))
	if len(traces) == 0 {
		t.Fatal("expected trace rows")
	}
	traceID := toString(traces[0]["traceID"])
	trace := h.Call(ctx, rid("trace.read"), sess, map[string]string{"trace": traceID}, nil, nil)
	if field(trace, "traceID") != traceID {
		t.Fatalf("unexpected trace: %#v", trace)
	}
	spans := pageItems(h.Call(ctx, rid("spans.list"), sess, map[string]string{"trace": traceID}, nil, nil))
	if !hasName(spans, "GET /checkout") {
		t.Fatalf("expected span row, got %#v", spans)
	}
	h.AssertAllCovered()
}

func jaegerIntegrationConfig(ctx context.Context, t *testing.T) (map[string]any, string) {
	t.Helper()
	endpoint := os.Getenv("SHELLCN_JAEGER_ENDPOINT")
	zipkinEndpoint := os.Getenv("SHELLCN_JAEGER_ZIPKIN_ENDPOINT")
	if endpoint == "" {
		endpoint, zipkinEndpoint = startJaegerContainer(ctx, t)
	}
	if zipkinEndpoint == "" {
		t.Skip("SHELLCN_JAEGER_ZIPKIN_ENDPOINT is required when using an external Jaeger endpoint")
	}
	return map[string]any{"endpoint": endpoint, "auth": "none", "tls_mode": "disable", "page_limit": 100, "timeout": "15s"}, zipkinEndpoint
}

func startJaegerContainer(ctx context.Context, t *testing.T) (string, string) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_JAEGER_ENDPOINT is not set")
	}
	name := "shellcn-jaeger-it-" + time.Now().UTC().Format("20060102150405")
	run(ctx, t, "docker", "run", "-d", "--rm", "--name", name,
		"-e", "COLLECTOR_ZIPKIN_HOST_PORT=:9411",
		"-p", "127.0.0.1::16686",
		"-p", "127.0.0.1::9411",
		"jaegertracing/all-in-one:1.76.0")
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})
	queryEndpoint := mappedEndpoint(ctx, t, name, "16686/tcp")
	zipkinEndpoint := mappedEndpoint(ctx, t, name, "9411/tcp")
	deadline := time.Now().Add(60 * time.Second)
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, queryEndpoint+"/api/services", nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil && resp.StatusCode < 500 {
			_ = resp.Body.Close()
			return queryEndpoint, zipkinEndpoint
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("Jaeger container did not become ready: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func mappedEndpoint(ctx context.Context, t *testing.T, container, port string) string {
	t.Helper()
	out := run(ctx, t, "docker", "port", container, port)
	host, mapped, err := net.SplitHostPort(strings.TrimSpace(out))
	if err != nil {
		t.Fatalf("unexpected docker port output: %q", out)
	}
	return "http://" + net.JoinHostPort(host, mapped)
}

func pushZipkinSpan(ctx context.Context, t *testing.T, endpoint string) {
	t.Helper()
	body := []map[string]any{{
		"traceId":       "11111111111111111111111111111111",
		"id":            "1111111111111111",
		"name":          "GET /checkout",
		"timestamp":     time.Now().Add(-time.Second).UnixMicro(),
		"duration":      5000,
		"localEndpoint": map[string]string{"serviceName": "checkout"},
		"tags":          map[string]string{"shellcn": "true"},
	}}
	data, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/api/v2/spans", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("push span: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		t.Fatalf("push span status: %s", resp.Status)
	}
}

func waitForService(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, service string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		services := pageItems(h.Call(ctx, rid("services.list"), sess, nil, nil, nil))
		if hasName(services, service) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Jaeger service %q did not become ready: %#v", service, services)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func call(ctx context.Context, t *testing.T, route plugin.Route, sess plugin.Session, params map[string]string, query url.Values, body []byte) any {
	t.Helper()
	out, err := route.Handle(plugin.NewRequestContext(ctx, plugin.User{}, sess, params, query, body))
	if err != nil {
		t.Fatalf("%s: %v", route.ID, err)
	}
	return out
}

func pageItems(page any) []map[string]any {
	data, _ := json.Marshal(page)
	var decoded struct {
		Items []map[string]any `json:"items"`
	}
	_ = json.Unmarshal(data, &decoded)
	return decoded.Items
}

func hasName(items []map[string]any, name string) bool {
	for _, item := range items {
		for _, key := range []string{"name", "operationName"} {
			if toString(item[key]) == name {
				return true
			}
		}
	}
	return false
}

func run(ctx context.Context, t *testing.T, name string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
	return string(out)
}

func toString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	default:
		return strings.TrimSpace(strings.Trim(fmt.Sprint(v), `"`))
	}
}

func field(v any, key string) any {
	switch t := v.(type) {
	case map[string]any:
		return t[key]
	case row:
		return t[key]
	default:
		return nil
	}
}
