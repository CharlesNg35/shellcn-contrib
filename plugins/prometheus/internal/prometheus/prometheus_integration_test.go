package prometheus

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestPrometheusPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_PROMETHEUS_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_PROMETHEUS_INTEGRATION=1 to run against Prometheus")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	cfg := prometheusIntegrationConfig(ctx, t)
	p := New()
	sess, err := p.Connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	s := sess.(*Session)
	waitForPrometheusData(ctx, t, s)

	routes := routeMap(p.Routes())
	call(ctx, t, routes["prometheus.overview"], sess, nil, nil, nil)
	call(ctx, t, routes["prometheus.status.tree"], sess, nil, nil, nil)
	statuses := pageItems(call(ctx, t, routes["prometheus.status.list"], sess, nil, nil, nil))
	for _, status := range statuses {
		call(ctx, t, routes["prometheus.status.read"], sess, map[string]string{"status": toString(status["name"])}, nil, nil)
	}

	targets := pageItems(call(ctx, t, routes["prometheus.targets.list"], sess, nil, nil, nil))
	if len(targets) == 0 {
		t.Fatal("expected at least one target")
	}
	targetID := toString(targets[0]["uid"])
	call(ctx, t, routes["prometheus.targets.tree"], sess, nil, nil, nil)
	call(ctx, t, routes["prometheus.target.read"], sess, map[string]string{"target": targetID}, nil, nil)
	call(ctx, t, routes["prometheus.target.metadata"], sess, map[string]string{"target": targetID}, nil, nil)

	rules := pageItems(call(ctx, t, routes["prometheus.rules.list"], sess, nil, nil, nil))
	if len(rules) == 0 {
		t.Fatal("expected rule rows")
	}
	call(ctx, t, routes["prometheus.rules.tree"], sess, nil, nil, nil)
	call(ctx, t, routes["prometheus.rule.read"], sess, map[string]string{"rule": toString(rules[0]["uid"])}, nil, nil)

	alerts := pageItems(call(ctx, t, routes["prometheus.alerts.list"], sess, nil, nil, nil))
	if len(alerts) == 0 {
		t.Fatal("expected alert rows")
	}
	call(ctx, t, routes["prometheus.alerts.tree"], sess, nil, nil, nil)
	call(ctx, t, routes["prometheus.alert.read"], sess, map[string]string{"alert": toString(alerts[0]["uid"])}, nil, nil)

	metrics := pageItems(call(ctx, t, routes["prometheus.metrics.list"], sess, nil, url.Values{"limit": []string{"50"}, "filter": []string{"up"}}, nil))
	if len(metrics) == 0 {
		t.Fatal("expected metric rows")
	}
	call(ctx, t, routes["prometheus.metrics.tree"], sess, nil, url.Values{"limit": []string{"50"}}, nil)
	call(ctx, t, routes["prometheus.metric.read"], sess, map[string]string{"metric": "up"}, nil, nil)
	series := pageItems(call(ctx, t, routes["prometheus.metric.series"], sess, map[string]string{"metric": "up"}, url.Values{"limit": []string{"10"}}, nil))
	if len(series) == 0 {
		t.Fatal("expected series rows")
	}

	labels := pageItems(call(ctx, t, routes["prometheus.labels.list"], sess, nil, nil, nil))
	if len(labels) == 0 {
		t.Fatal("expected label rows")
	}
	call(ctx, t, routes["prometheus.labels.tree"], sess, nil, nil, nil)
	call(ctx, t, routes["prometheus.label.values"], sess, map[string]string{"label": "__name__"}, nil, nil)

	call(ctx, t, routes["prometheus.completion"], sess, nil, nil, nil)
	if result, err := executeQuery(ctx, s, "up"); err != nil || result.RowCount < 1 {
		t.Fatalf("instant query: result=%#v err=%v", result, err)
	}
	rangeQuery := `{"type":"range","query":"up","start":"-2m","end":"now","step":"15s"}`
	if result, err := executeQuery(ctx, s, rangeQuery); err != nil || result.RowCount < 1 {
		t.Fatalf("range query: result=%#v err=%v", result, err)
	}
	frame := liveFrame(ctx, s)
	if frame["targets"] == nil {
		t.Fatalf("unexpected live frame: %#v", frame)
	}

	call(ctx, t, routes["prometheus.snapshot.create"], sess, nil, nil, mustJSON(t, map[string]any{"skip_head": true}))
	call(ctx, t, routes["prometheus.series.delete"], sess, map[string]string{"metric": "shellcn:constant"}, nil, mustJSON(t, map[string]any{"match": "shellcn:constant"}))
	call(ctx, t, routes["prometheus.tombstones.clean"], sess, nil, nil, nil)
	call(ctx, t, routes["prometheus.config.reload"], sess, nil, nil, nil)
}

func prometheusIntegrationConfig(ctx context.Context, t *testing.T) map[string]any {
	t.Helper()
	endpoint := os.Getenv("SHELLCN_PROMETHEUS_ENDPOINT")
	if endpoint == "" {
		endpoint = startPrometheusContainer(ctx, t)
	}
	return map[string]any{"endpoint": endpoint, "auth": "none", "tls_mode": "disable", "timeout": "15s", "poll_interval": "1s", "page_limit": 100, "admin_api": true, "lifecycle_api": true}
}

func startPrometheusContainer(ctx context.Context, t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_PROMETHEUS_ENDPOINT is not set")
	}
	dir := t.TempDir()
	config := `
global:
  scrape_interval: 1s
  evaluation_interval: 1s
rule_files:
  - /etc/prometheus/rules.yml
scrape_configs:
  - job_name: prometheus
    static_configs:
      - targets: ["localhost:9090"]
`
	rules := `
groups:
  - name: shellcn
    rules:
      - alert: ShellCNAlwaysFiring
        expr: vector(1)
        for: 0s
        labels:
          severity: test
        annotations:
          summary: ShellCN integration alert
      - record: shellcn:constant
        expr: vector(7)
`
	if err := os.WriteFile(filepath.Join(dir, "prometheus.yml"), []byte(config), 0o644); err != nil {
		t.Fatalf("write prometheus config: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "rules.yml"), []byte(rules), 0o644); err != nil {
		t.Fatalf("write prometheus rules: %v", err)
	}
	name := "shellcn-prometheus-it-" + time.Now().UTC().Format("20060102150405")
	run(ctx, t, "docker", "run", "-d", "--rm", "--name", name,
		"-p", "127.0.0.1::9090",
		"-v", filepath.Join(dir, "prometheus.yml")+":/etc/prometheus/prometheus.yml:ro",
		"-v", filepath.Join(dir, "rules.yml")+":/etc/prometheus/rules.yml:ro",
		"prom/prometheus:v3.11.3",
		"--config.file=/etc/prometheus/prometheus.yml",
		"--storage.tsdb.path=/prometheus",
		"--web.enable-admin-api",
		"--web.enable-lifecycle")
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})
	out := run(ctx, t, "docker", "port", name, "9090/tcp")
	host, port, err := net.SplitHostPort(strings.TrimSpace(out))
	if err != nil {
		t.Fatalf("unexpected docker port output: %q", out)
	}
	endpoint := "http://" + net.JoinHostPort(host, port)
	cfg := map[string]any{"endpoint": endpoint, "auth": "none", "tls_mode": "disable", "timeout": "15s", "poll_interval": "1s", "page_limit": 100, "admin_api": true, "lifecycle_api": true}
	deadline := time.Now().Add(90 * time.Second)
	for {
		p := New()
		sess, err := p.Connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
		if err == nil {
			_ = sess.Close()
			return endpoint
		}
		if time.Now().After(deadline) {
			t.Fatalf("Prometheus container did not become ready: %v", err)
		}
		time.Sleep(1 * time.Second)
	}
}

func waitForPrometheusData(ctx context.Context, t *testing.T, s *Session) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for {
		result, err := executeQuery(ctx, s, "up")
		alertRows, _ := alerts(ctx, s)
		ruleRows, _ := rules(ctx, s)
		if err == nil && result.RowCount > 0 && len(alertRows) > 0 && len(ruleRows) > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Prometheus data did not become ready: query=%#v err=%v alerts=%d rules=%d", result, err, len(alertRows), len(ruleRows))
		}
		time.Sleep(1 * time.Second)
	}
}

func routeMap(routes []plugin.Route) map[string]plugin.Route {
	out := map[string]plugin.Route{}
	for _, route := range routes {
		out[route.ID] = route
	}
	return out
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

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
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
