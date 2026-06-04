package opensearch

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

	"github.com/charlesng35/shellcn-contrib/shared/escompat"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestOpenSearchPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_OPENSEARCH_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_OPENSEARCH_INTEGRATION=1 to run against OpenSearch")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	cfg := openSearchIntegrationConfig(ctx, t)
	p := New()
	sess, err := p.Connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()

	routes := routeMap(p.Routes())
	index := "shellcn-it-" + time.Now().UTC().Format("20060102150405")
	createBody, _ := json.Marshal(map[string]any{
		"name": index,
		"settings": map[string]any{
			"number_of_replicas": 0,
		},
		"mappings": map[string]any{"properties": map[string]any{
			"name": map[string]any{"type": "keyword"},
			"age":  map[string]any{"type": "integer"},
		}},
	})
	call(ctx, t, routes["opensearch.index.create"], sess, nil, nil, createBody)
	defer callNoFail(context.Background(), routes["opensearch.index.delete"], sess, map[string]string{"index": index})

	docBody, _ := json.Marshal(map[string]any{"id": "ada", "document": map[string]any{"name": "ada", "age": 37}})
	call(ctx, t, routes["opensearch.document.create"], sess, map[string]string{"index": index}, nil, docBody)
	call(ctx, t, routes["opensearch.index.refresh"], sess, map[string]string{"index": index}, nil, nil)
	call(ctx, t, routes["opensearch.index.overview"], sess, map[string]string{"index": index}, nil, nil)
	call(ctx, t, routes["opensearch.mapping.read"], sess, map[string]string{"index": index}, nil, nil)
	call(ctx, t, routes["opensearch.settings.read"], sess, map[string]string{"index": index}, nil, nil)
	call(ctx, t, routes["opensearch.aliases.list"], sess, map[string]string{"index": index}, nil, nil)
	call(ctx, t, routes["opensearch.shards.list"], sess, map[string]string{"index": index}, nil, nil)

	docs := call(ctx, t, routes["opensearch.documents.list"], sess, map[string]string{"index": index}, url.Values{"limit": []string{"10"}}, nil)
	items := pageItems(docs)
	if len(items) != 1 || items[0]["_id"] != "ada" {
		t.Fatalf("expected indexed document, got %#v", items)
	}
	read := call(ctx, t, routes["opensearch.document.read"], sess, map[string]string{"index": index, "id": "ada"}, nil, nil).(map[string]any)
	if read["_id"] != "ada" {
		t.Fatalf("unexpected document read: %#v", read)
	}
	updateBody, _ := json.Marshal(map[string]any{"content": `{"name":"ada","age":38}`})
	call(ctx, t, routes["opensearch.document.update"], sess, map[string]string{"index": index, "id": "ada"}, nil, updateBody)
	call(ctx, t, routes["opensearch.document.delete"], sess, map[string]string{"index": index, "id": "ada"}, nil, nil)

	settingsBody, _ := json.Marshal(map[string]any{"settings": map[string]any{"index": map[string]any{"refresh_interval": "30s"}}})
	call(ctx, t, routes["opensearch.settings.update"], sess, map[string]string{"index": index}, nil, settingsBody)
	if got := indexSetting(call(ctx, t, routes["opensearch.settings.read"], sess, map[string]string{"index": index}, nil, nil), index, "refresh_interval"); got != "30s" {
		t.Fatalf("refresh_interval after update: got %q want %q", got, "30s")
	}

	for _, id := range []string{"keep-1", "drop-1", "drop-2", "drop-3"} {
		group := "keep"
		if strings.HasPrefix(id, "drop") {
			group = "drop"
		}
		seedBody, _ := json.Marshal(map[string]any{"id": id, "document": map[string]any{"group": group}})
		call(ctx, t, routes["opensearch.document.create"], sess, map[string]string{"index": index}, nil, seedBody)
	}
	call(ctx, t, routes["opensearch.index.refresh"], sess, map[string]string{"index": index}, nil, nil)
	dbqBody, _ := json.Marshal(map[string]any{"query": map[string]any{"term": map[string]any{"group": "drop"}}})
	dbqRes := call(ctx, t, routes["opensearch.documents.delete_by_query"], sess, map[string]string{"index": index}, nil, dbqBody).(map[string]any)
	if deleted := numericValue(dbqRes["deleted"]); deleted != 3 {
		t.Fatalf("delete_by_query deleted: got %v want 3 (%#v)", dbqRes["deleted"], dbqRes)
	}
	call(ctx, t, routes["opensearch.index.refresh"], sess, map[string]string{"index": index}, nil, nil)
	remaining := pageItems(call(ctx, t, routes["opensearch.documents.list"], sess, map[string]string{"index": index}, url.Values{"limit": []string{"10"}}, nil))
	if len(remaining) != 1 || remaining[0]["_id"] != "keep-1" {
		t.Fatalf("after delete_by_query expected only keep-1, got %#v", remaining)
	}
}

func indexSetting(raw any, index, key string) string {
	data, _ := json.Marshal(raw)
	var settings map[string]struct {
		Settings struct {
			Index map[string]any `json:"index"`
		} `json:"settings"`
	}
	if json.Unmarshal(data, &settings) != nil {
		return ""
	}
	return fmt.Sprint(settings[index].Settings.Index[key])
}

func numericValue(v any) float64 {
	if f, ok := v.(float64); ok {
		return f
	}
	return -1
}

func openSearchIntegrationConfig(ctx context.Context, t *testing.T) map[string]any {
	t.Helper()
	endpoint := os.Getenv("SHELLCN_OPENSEARCH_ENDPOINT")
	if endpoint == "" {
		endpoint = startOpenSearchContainer(ctx, t)
	}
	return map[string]any{"endpoint": endpoint, "auth": "none", "tls_mode": "disable", "read_only": false, "page_limit": 100, "timeout": "60s"}
}

func startOpenSearchContainer(ctx context.Context, t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_OPENSEARCH_ENDPOINT is not set")
	}
	name := "shellcn-opensearch-it-" + time.Now().UTC().Format("20060102150405")
	run(ctx, t, "docker", "run", "-d", "--rm", "--name", name,
		"-e", "discovery.type=single-node",
		"-e", "plugins.security.disabled=true",
		"-e", "DISABLE_INSTALL_DEMO_CONFIG=true",
		"-e", "OPENSEARCH_INITIAL_ADMIN_PASSWORD=ShellcnAdmin1!",
		"-e", "OPENSEARCH_JAVA_OPTS=-Xms1g -Xmx1g",
		"-p", "127.0.0.1::9200",
		"opensearchproject/opensearch:3.6.0")
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})
	out := run(ctx, t, "docker", "port", name, "9200/tcp")
	host, port, err := net.SplitHostPort(strings.TrimSpace(out))
	if err != nil {
		t.Fatalf("unexpected docker port output: %q", out)
	}
	endpoint := "http://" + net.JoinHostPort(host, port)
	cfg := map[string]any{"endpoint": endpoint, "auth": "none", "tls_mode": "disable", "read_only": false, "page_limit": 100, "timeout": "60s"}
	deadline := time.Now().Add(150 * time.Second)
	for {
		sess, err := escompat.Connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()}, escompat.Provider{Protocol: "opensearch", DefaultURL: endpoint, Product: escompat.ProductOpenSearch})
		if err == nil {
			_ = sess.Close()
			return endpoint
		}
		if time.Now().After(deadline) {
			t.Fatalf("OpenSearch container did not become ready: %v", err)
		}
		time.Sleep(750 * time.Millisecond)
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
