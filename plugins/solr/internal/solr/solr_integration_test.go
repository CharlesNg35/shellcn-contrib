package solr

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

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestSolrPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_SOLR_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_SOLR_INTEGRATION=1 to run against Solr")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	cfg := solrIntegrationConfig(ctx, t)
	runSolrScenario(ctx, t, cfg)
}

func TestSolrPluginCloudIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_SOLR_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_SOLR_INTEGRATION=1 to run against Solr")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	endpoint := os.Getenv("SHELLCN_SOLR_CLOUD_ENDPOINT")
	if endpoint == "" {
		endpoint = startSolrCloudContainer(ctx, t)
	}
	runSolrScenario(ctx, t, map[string]any{"endpoint": endpoint, "auth": "none", "tls_mode": "disable", "read_only": false, "page_limit": 100, "timeout": "15s"})
}

func runSolrScenario(ctx context.Context, t *testing.T, cfg map[string]any) {
	t.Helper()
	p := New()
	sess, err := p.Connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()

	routes := plugintest.RouteMap(p.Routes())
	core := "shellcn_it_" + time.Now().UTC().Format("20060102150405")
	createBody, _ := json.Marshal(map[string]any{"name": core, "config_set": "_default"})
	call(ctx, t, routes["solr.core.create"], sess, nil, nil, createBody)
	defer callNoFail(context.Background(), routes["solr.core.delete"], sess, map[string]string{"core": core})

	waitForSearchSet(ctx, t, sess.(*Session), core)
	call(ctx, t, routes["solr.overview"], sess, nil, nil, nil)
	call(ctx, t, routes["solr.cores.tree"], sess, nil, nil, nil)
	call(ctx, t, routes["solr.cores.list"], sess, nil, nil, nil)
	call(ctx, t, routes["solr.core.overview"], sess, map[string]string{"core": core}, nil, nil)
	call(ctx, t, routes["solr.core.ping"], sess, map[string]string{"core": core}, nil, nil)
	call(ctx, t, routes["solr.config.read"], sess, map[string]string{"core": core}, nil, nil)
	call(ctx, t, routes["solr.schema.read"], sess, map[string]string{"core": core}, nil, nil)

	field := "shellcn_name_s"
	fieldBody, _ := json.Marshal(map[string]any{"name": field, "type": "string", "indexed": true, "stored": true})
	call(ctx, t, routes["solr.schema.field.add"], sess, map[string]string{"core": core}, nil, fieldBody)
	call(ctx, t, routes["solr.schema.fields"], sess, map[string]string{"core": core}, nil, nil)
	added := call(ctx, t, routes["solr.schema.field.read"], sess, map[string]string{"core": core, "field": field}, nil, nil)
	if got := schemaFieldProp(added, "stored"); got != true {
		t.Fatalf("added field stored: got %#v want true", got)
	}

	docBody, _ := json.Marshal(map[string]any{"document": map[string]any{"id": "ada", field: "Ada Lovelace", "age_i": 37}, "commit": true})
	call(ctx, t, routes["solr.document.upsert"], sess, map[string]string{"core": core}, nil, docBody)

	docs := call(ctx, t, routes["solr.documents.list"], sess, map[string]string{"core": core}, url.Values{"limit": []string{"10"}}, nil)
	if items := pageItems(docs); len(items) != 1 || items[0]["_id"] != "ada" {
		t.Fatalf("expected one document, got %#v", items)
	}
	read := call(ctx, t, routes["solr.document.read"], sess, map[string]string{"core": core, "id": "ada"}, nil, nil)
	if fieldValue(read, field) != "Ada Lovelace" {
		t.Fatalf("unexpected document: %#v", read)
	}
	updateBody, _ := json.Marshal(map[string]any{"content": fmt.Sprintf(`{"id":"ada","%s":"Ada King","age_i":37}`, field)})
	call(ctx, t, routes["solr.document.update"], sess, map[string]string{"core": core, "id": "ada"}, nil, updateBody)
	call(ctx, t, routes["solr.completion"], sess, nil, nil, nil)
	result, err := executeSearch(ctx, sess.(*Session), core, map[string]any{"q": "id:ada", "rows": 10})
	if err != nil || result.RowCount < 1 {
		t.Fatalf("execute search: result=%#v err=%v", result, err)
	}

	call(ctx, t, routes["solr.core.commit"], sess, map[string]string{"core": core}, nil, nil)
	call(ctx, t, routes["solr.core.optimize"], sess, map[string]string{"core": core}, nil, nil)

	tempBody, _ := json.Marshal(map[string]any{"document": map[string]any{"id": "temp", field: "temporary"}, "commit": true})
	call(ctx, t, routes["solr.document.upsert"], sess, map[string]string{"core": core}, nil, tempBody)
	deleteQueryBody, _ := json.Marshal(map[string]any{"query": "id:temp", "commit": true})
	call(ctx, t, routes["solr.documents.delete_query"], sess, map[string]string{"core": core}, nil, deleteQueryBody)
	call(ctx, t, routes["solr.document.delete"], sess, map[string]string{"core": core, "id": "ada"}, nil, nil)

	replaceBody, _ := json.Marshal(map[string]any{"type": "strings", "indexed": true, "stored": false, "multi_valued": true})
	call(ctx, t, routes["solr.schema.field.replace"], sess, map[string]string{"core": core, "field": field}, nil, replaceBody)
	replaced := call(ctx, t, routes["solr.schema.field.read"], sess, map[string]string{"core": core, "field": field}, nil, nil)
	if got := schemaFieldProp(replaced, "stored"); got != false {
		t.Fatalf("replaced field stored: got %#v want false", got)
	}
	if got := schemaFieldProp(replaced, "multiValued"); got != true {
		t.Fatalf("replaced field multiValued: got %#v want true", got)
	}
	if got := schemaFieldProp(replaced, "type"); got != "strings" {
		t.Fatalf("replaced field type: got %#v want strings", got)
	}

	call(ctx, t, routes["solr.schema.field.delete"], sess, map[string]string{"core": core, "field": field}, nil, nil)
	call(ctx, t, routes["solr.core.reload"], sess, map[string]string{"core": core}, nil, nil)
}

func solrIntegrationConfig(ctx context.Context, t *testing.T) map[string]any {
	t.Helper()
	endpoint := os.Getenv("SHELLCN_SOLR_ENDPOINT")
	if endpoint == "" {
		endpoint = startSolrContainer(ctx, t)
	}
	return map[string]any{"endpoint": endpoint, "auth": "none", "tls_mode": "disable", "read_only": false, "page_limit": 100, "timeout": "15s"}
}

func startSolrContainer(ctx context.Context, t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_SOLR_ENDPOINT is not set")
	}
	name := "shellcn-solr-it-" + time.Now().UTC().Format("20060102150405")
	run(ctx, t, "docker", "run", "-d", "--rm", "--name", name, "-p", "127.0.0.1::8983", "solr:10.0.0", "solr-precreate", "bootstrap")
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})
	endpoint := waitForSolrEndpoint(ctx, t, name)
	run(ctx, t, "docker", "exec", name, "bash", "-lc", "mkdir -p /var/solr/data/configsets && rm -rf /var/solr/data/configsets/_default && cp -R /opt/solr/server/solr/configsets/_default /var/solr/data/configsets/_default")
	return endpoint
}

func waitForSolrEndpoint(ctx context.Context, t *testing.T, name string) string {
	t.Helper()
	out := run(ctx, t, "docker", "port", name, "8983/tcp")
	host, port, err := net.SplitHostPort(strings.TrimSpace(out))
	if err != nil {
		t.Fatalf("unexpected docker port output: %q", out)
	}
	endpoint := "http://" + net.JoinHostPort(host, port) + "/solr"
	cfg := map[string]any{"endpoint": endpoint, "auth": "none", "tls_mode": "disable", "read_only": false, "page_limit": 100, "timeout": "15s"}
	deadline := time.Now().Add(90 * time.Second)
	for {
		p := New()
		sess, err := p.Connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
		if err == nil {
			_ = sess.Close()
			return endpoint
		}
		if time.Now().After(deadline) {
			t.Fatalf("Solr container did not become ready: %v", err)
		}
		time.Sleep(1 * time.Second)
	}
}

func startSolrCloudContainer(ctx context.Context, t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_SOLR_CLOUD_ENDPOINT is not set")
	}
	name := "shellcn-solr-cloud-it-" + time.Now().UTC().Format("20060102150405")
	run(ctx, t, "docker", "run", "-d", "--rm", "--name", name, "-p", "127.0.0.1::8983", "solr:10.0.0")
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})
	return waitForSolrEndpoint(ctx, t, name)
}

func waitForSearchSet(ctx context.Context, t *testing.T, s *Session, core string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		status, err := adminStatus(ctx, s)
		if err == nil {
			if _, ok := status[core]; ok {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("core %s did not become ready: %v", core, err)
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

func callNoFail(ctx context.Context, route plugin.Route, sess plugin.Session, params map[string]string) {
	_, _ = route.Handle(plugin.NewRequestContext(ctx, plugin.User{}, sess, params, nil, nil))
}

func pageItems(page any) []map[string]any {
	data, _ := json.Marshal(page)
	var decoded struct {
		Items []map[string]any `json:"items"`
	}
	_ = json.Unmarshal(data, &decoded)
	return decoded.Items
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

func schemaFieldProp(v any, key string) any {
	data, _ := json.Marshal(v)
	var decoded struct {
		Field map[string]any `json:"field"`
	}
	_ = json.Unmarshal(data, &decoded)
	return decoded.Field[key]
}

func fieldValue(v any, key string) any {
	switch t := v.(type) {
	case map[string]any:
		return t[key]
	case row:
		return t[key]
	default:
		return nil
	}
}
