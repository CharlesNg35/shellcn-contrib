package influxdb

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

const (
	influxIntegrationOrg    = "shellcn"
	influxIntegrationBucket = "shellcn"
	influxIntegrationToken  = "shellcn-token"
)

func TestInfluxDBPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_INFLUXDB_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_INFLUXDB_INTEGRATION=1 to run against InfluxDB")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	cfg, bucket := influxDBIntegrationConfig(ctx, t)
	p := New()
	sess, err := p.Connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	s := sess.(*Session)

	routes := routeMap(p.Routes())
	measurement := "shellcn_cpu"
	line := fmt.Sprintf("%s,host=web01 usage=0.64,status=\"ok\" %d", measurement, time.Now().UnixNano())
	call(ctx, t, routes["influxdb.write"], sess, map[string]string{"namespace": bucket}, nil, testJSON(t, map[string]any{"line_protocol": line, "precision": "ns"}))
	waitForInfluxMeasurement(ctx, t, s, bucket, measurement)

	call(ctx, t, routes["influxdb.status.tree"], sess, nil, nil, nil)
	statuses := pageItems(call(ctx, t, routes["influxdb.status.list"], sess, nil, nil, nil))
	if len(statuses) < 2 {
		t.Fatalf("expected status rows, got %#v", statuses)
	}
	call(ctx, t, routes["influxdb.status.read"], sess, map[string]string{"status": "health"}, nil, nil)
	call(ctx, t, routes["influxdb.status.read"], sess, map[string]string{"status": "config"}, nil, nil)

	namespaces := pageItems(call(ctx, t, routes["influxdb.namespaces.list"], sess, nil, nil, nil))
	if !hasRowName(namespaces, bucket) {
		t.Fatalf("expected bucket %q in %#v", bucket, namespaces)
	}
	call(ctx, t, routes["influxdb.namespaces.tree"], sess, nil, nil, nil)
	call(ctx, t, routes["influxdb.namespace.read"], sess, map[string]string{"namespace": bucket}, nil, nil)

	newBucket := "shellcn_it_bucket"
	call(ctx, t, routes["influxdb.namespace.create"], sess, nil, nil, testJSON(t, map[string]any{"name": newBucket, "retention_period": "1h"}))
	created := pageItems(call(ctx, t, routes["influxdb.namespaces.list"], sess, nil, nil, nil))
	if !hasRowName(created, newBucket) {
		t.Fatalf("expected created bucket %q in %#v", newBucket, created)
	}
	call(ctx, t, routes["influxdb.namespace.delete"], sess, map[string]string{"namespace": newBucket}, nil, nil)
	afterDelete := pageItems(call(ctx, t, routes["influxdb.namespaces.list"], sess, nil, nil, nil))
	if hasRowName(afterDelete, newBucket) {
		t.Fatalf("bucket %q still present after delete: %#v", newBucket, afterDelete)
	}

	measurements := pageItems(call(ctx, t, routes["influxdb.measurements.list"], sess, map[string]string{"namespace": bucket}, nil, nil))
	if !hasRowName(measurements, measurement) {
		t.Fatalf("expected measurement %q in %#v", measurement, measurements)
	}
	call(ctx, t, routes["influxdb.measurements.tree"], sess, map[string]string{"namespace": bucket}, nil, nil)
	rows := pageItems(call(ctx, t, routes["influxdb.measurement.rows"], sess, map[string]string{"namespace": bucket, "measurement": measurement}, url.Values{"limit": []string{"25"}}, nil))
	if len(rows) == 0 {
		t.Fatal("expected point rows")
	}
	fields := pageItems(call(ctx, t, routes["influxdb.measurement.fields"], sess, map[string]string{"namespace": bucket, "measurement": measurement}, nil, nil))
	if !hasRowName(fields, "usage") && !hasRowValue(fields, "usage") {
		t.Fatalf("expected usage field in %#v", fields)
	}
	tags := pageItems(call(ctx, t, routes["influxdb.measurement.tags"], sess, map[string]string{"namespace": bucket, "measurement": measurement}, nil, nil))
	if !hasRowName(tags, "host") && !hasRowValue(tags, "host") {
		t.Fatalf("expected host tag in %#v", tags)
	}

	call(ctx, t, routes["influxdb.completion"], sess, nil, nil, nil)
	query := fmt.Sprintf(`from(bucket: %q) |> range(start: -1h) |> filter(fn: (r) => r._measurement == %q) |> limit(n: 10)`, bucket, measurement)
	result, err := executeQuery(ctx, s, bucket, sqldb.QueryRequest{Query: query})
	if err != nil || result.RowCount < 1 {
		t.Fatalf("flux query: result=%#v err=%v", result, err)
	}
}

func influxDBIntegrationConfig(ctx context.Context, t *testing.T) (map[string]any, string) {
	t.Helper()
	endpoint := os.Getenv("SHELLCN_INFLUXDB_ENDPOINT")
	token := os.Getenv("SHELLCN_INFLUXDB_TOKEN")
	org := os.Getenv("SHELLCN_INFLUXDB_ORG")
	bucket := os.Getenv("SHELLCN_INFLUXDB_BUCKET")
	if token == "" {
		token = influxIntegrationToken
	}
	if org == "" {
		org = influxIntegrationOrg
	}
	if bucket == "" {
		bucket = influxIntegrationBucket
	}
	if endpoint == "" {
		endpoint = startInfluxDBContainer(ctx, t, org, bucket, token)
	}
	return map[string]any{
		"api_mode":       modeV2,
		"endpoint":       endpoint,
		"org":            org,
		"auth_v2":        "token",
		tokenFieldV2:     token,
		"tls_mode":       "disable",
		"read_only":      false,
		"confirm_writes": false,
		"page_limit":     100,
		"timeout":        "15s",
		"lookback":       "-1h",
	}, bucket
}

func startInfluxDBContainer(ctx context.Context, t *testing.T, org, bucket, token string) string {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_INFLUXDB_ENDPOINT is not set")
	}
	name := "shellcn-influxdb-it-" + time.Now().UTC().Format("20060102150405")
	run(ctx, t, "docker", "run", "-d", "--rm", "--name", name,
		"-p", "127.0.0.1::8086",
		"-e", "DOCKER_INFLUXDB_INIT_MODE=setup",
		"-e", "DOCKER_INFLUXDB_INIT_USERNAME=shellcn",
		"-e", "DOCKER_INFLUXDB_INIT_PASSWORD=shellcnpass123",
		"-e", "DOCKER_INFLUXDB_INIT_ORG="+org,
		"-e", "DOCKER_INFLUXDB_INIT_BUCKET="+bucket,
		"-e", "DOCKER_INFLUXDB_INIT_ADMIN_TOKEN="+token,
		"influxdb:2.7")
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})
	out := run(ctx, t, "docker", "port", name, "8086/tcp")
	host, port, err := net.SplitHostPort(strings.TrimSpace(out))
	if err != nil {
		t.Fatalf("unexpected docker port output: %q", out)
	}
	endpoint := "http://" + net.JoinHostPort(host, port)
	cfg := map[string]any{"api_mode": modeV2, "endpoint": endpoint, "org": org, "auth_v2": "token", tokenFieldV2: token, "tls_mode": "disable", "timeout": "15s", "page_limit": 100, "lookback": "-1h"}
	deadline := time.Now().Add(90 * time.Second)
	for {
		p := New()
		sess, err := p.Connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
		if err == nil {
			_ = sess.Close()
			return endpoint
		}
		if time.Now().After(deadline) {
			t.Fatalf("InfluxDB container did not become ready: %v", err)
		}
		time.Sleep(1 * time.Second)
	}
}

func waitForInfluxMeasurement(ctx context.Context, t *testing.T, s *Session, bucket, measurement string) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for {
		result, err := executeQuery(ctx, s, bucket, sqldb.QueryRequest{Query: fmt.Sprintf(`from(bucket: %q) |> range(start: -1h) |> filter(fn: (r) => r._measurement == %q) |> limit(n: 10)`, bucket, measurement)})
		if err == nil && result.RowCount > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("InfluxDB measurement did not become queryable: result=%#v err=%v", result, err)
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

func testJSON(t *testing.T, value any) []byte {
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

func hasRowName(rows []map[string]any, name string) bool {
	for _, row := range rows {
		if fmt.Sprint(row["name"]) == name {
			return true
		}
	}
	return false
}

func hasRowValue(rows []map[string]any, value string) bool {
	for _, row := range rows {
		if fmt.Sprint(row["_value"]) == value || fmt.Sprint(row["value"]) == value {
			return true
		}
	}
	return false
}
