package loki

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

func TestLokiPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_LOKI_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_LOKI_INTEGRATION=1 to run against Loki")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cfg := lokiIntegrationConfig(ctx, t)
	pushLokiLog(ctx, t, cfg["endpoint"].(string))

	p := New()
	sess, err := p.Connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	h := contribtest.NewHarness(t, p.Routes())

	waitForLokiLabels(ctx, t, h, sess)
	h.Call(ctx, rid("overview"), sess, nil, nil, nil)
	h.Call(ctx, rid("labels.tree"), sess, nil, nil, nil)
	labels := pageItems(h.Call(ctx, rid("labels.list"), sess, nil, nil, nil))
	if !hasName(labels, "app") {
		t.Fatalf("expected app label, got %#v", labels)
	}
	values := pageItems(h.Call(ctx, rid("label.values"), sess, map[string]string{"label": "app"}, nil, nil))
	if !hasValue(values, "shellcn") {
		t.Fatalf("expected shellcn app label value, got %#v", values)
	}
	streams := pageItems(h.Call(ctx, rid("streams.list"), sess, nil, nil, nil))
	if len(streams) == 0 {
		t.Fatal("expected stream rows")
	}
	logs := pageItems(h.Call(ctx, rid("stream.logs"), sess, map[string]string{"stream": `{app="shellcn",job="integration"}`}, url.Values{"limit": []string{"10"}}, nil))
	if len(logs) == 0 || !strings.Contains(toString(logs[0]["line"]), "hello from shellcn") {
		t.Fatalf("expected pushed log line, got %#v", logs)
	}
	streamOut := h.Stream(ctx, rid("query"), sess, nil, nil, []byte(`{"query":"{app=\"shellcn\",job=\"integration\"}","limit":10,"since":"1h"}`+"\n"))
	if !strings.Contains(string(streamOut), "hello from shellcn") {
		t.Fatalf("unexpected query stream output: %s", streamOut)
	}
	h.AssertCovered(
		rid("overview"),
		rid("labels.tree"),
		rid("labels.list"),
		rid("label.values"),
		rid("streams.list"),
		rid("stream.logs"),
		rid("query"),
	)
}

func lokiIntegrationConfig(ctx context.Context, t *testing.T) map[string]any {
	t.Helper()
	endpoint := os.Getenv("SHELLCN_LOKI_ENDPOINT")
	if endpoint == "" {
		endpoint = startLokiContainer(ctx, t)
	}
	return map[string]any{"endpoint": endpoint, "auth": "none", "tls_mode": "disable", "page_limit": 100, "timeout": "15s"}
}

func startLokiContainer(ctx context.Context, t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_LOKI_ENDPOINT is not set")
	}
	name := "shellcn-loki-it-" + time.Now().UTC().Format("20060102150405")
	run(ctx, t, "docker", "run", "-d", "--rm", "--name", name, "-p", "127.0.0.1::3100", "grafana/loki:3.5.7", "-config.file=/etc/loki/local-config.yaml")
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})
	out := run(ctx, t, "docker", "port", name, "3100/tcp")
	host, port, err := net.SplitHostPort(strings.TrimSpace(out))
	if err != nil {
		t.Fatalf("unexpected docker port output: %q", out)
	}
	endpoint := "http://" + net.JoinHostPort(host, port)
	deadline := time.Now().Add(60 * time.Second)
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/ready", nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil && resp.StatusCode < 500 {
			_ = resp.Body.Close()
			return endpoint
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("Loki container did not become ready: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func pushLokiLog(ctx context.Context, t *testing.T, endpoint string) {
	t.Helper()
	body := map[string]any{"streams": []any{map[string]any{
		"stream": map[string]string{"job": "integration", "app": "shellcn"},
		"values": [][]string{{fmt.Sprintf("%d", time.Now().Add(-time.Second).UnixNano()), "hello from shellcn"}},
	}}}
	data, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/loki/api/v1/push", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("push log: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		t.Fatalf("push log status: %s", resp.Status)
	}
}

func waitForLokiLabels(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		labels := pageItems(h.Call(ctx, rid("labels.list"), sess, nil, nil, nil))
		if hasName(labels, "app") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Loki labels did not become ready: %#v", labels)
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
		if toString(item["name"]) == name {
			return true
		}
	}
	return false
}

func hasValue(items []map[string]any, value string) bool {
	for _, item := range items {
		if toString(item["value"]) == value {
			return true
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
