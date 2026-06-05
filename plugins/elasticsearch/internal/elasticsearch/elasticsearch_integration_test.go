package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
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

func TestElasticsearchPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_ELASTICSEARCH_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_ELASTICSEARCH_INTEGRATION=1 to run against Elasticsearch")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	cfg := elasticsearchIntegrationConfig(ctx, t)
	p := New()
	sess, err := p.Connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()

	routes := plugintest.RouteMap(p.Routes())
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
	call(ctx, t, routes["elasticsearch.index.create"], sess, nil, nil, createBody)
	defer callNoFail(context.Background(), routes["elasticsearch.index.delete"], sess, map[string]string{"index": index})

	docBody, _ := json.Marshal(map[string]any{"id": "ada", "document": map[string]any{"name": "ada", "age": 37}})
	call(ctx, t, routes["elasticsearch.document.create"], sess, map[string]string{"index": index}, nil, docBody)
	call(ctx, t, routes["elasticsearch.index.refresh"], sess, map[string]string{"index": index}, nil, nil)
	call(ctx, t, routes["elasticsearch.index.overview"], sess, map[string]string{"index": index}, nil, nil)
	call(ctx, t, routes["elasticsearch.mapping.read"], sess, map[string]string{"index": index}, nil, nil)
	call(ctx, t, routes["elasticsearch.settings.read"], sess, map[string]string{"index": index}, nil, nil)
	call(ctx, t, routes["elasticsearch.aliases.list"], sess, map[string]string{"index": index}, nil, nil)
	call(ctx, t, routes["elasticsearch.shards.list"], sess, map[string]string{"index": index}, nil, nil)

	aliasName := index + "-alias"
	aliasBody, _ := json.Marshal(map[string]any{"name": aliasName})
	call(ctx, t, routes["elasticsearch.alias.create"], sess, map[string]string{"index": index}, nil, aliasBody)
	aliases := pageItems(call(ctx, t, routes["elasticsearch.aliases.list"], sess, map[string]string{"index": index}, nil, nil))
	found := false
	for _, a := range aliases {
		if a["alias"] == aliasName {
			found = true
		}
	}
	if !found {
		t.Fatalf("alias %q not listed after create: %#v", aliasName, aliases)
	}
	call(ctx, t, routes["elasticsearch.alias.delete"], sess, map[string]string{"index": index, "alias": aliasName}, nil, nil)

	docs := call(ctx, t, routes["elasticsearch.documents.list"], sess, map[string]string{"index": index}, url.Values{"limit": []string{"10"}}, nil)
	items := pageItems(docs)
	if len(items) != 1 || items[0]["_id"] != "ada" {
		t.Fatalf("expected indexed document, got %#v", items)
	}
	read := call(ctx, t, routes["elasticsearch.document.read"], sess, map[string]string{"index": index, "id": "ada"}, nil, nil).(map[string]any)
	if read["_id"] != "ada" {
		t.Fatalf("unexpected document read: %#v", read)
	}
	updateBody, _ := json.Marshal(map[string]any{"content": `{"name":"ada","age":38}`})
	call(ctx, t, routes["elasticsearch.document.update"], sess, map[string]string{"index": index, "id": "ada"}, nil, updateBody)
	call(ctx, t, routes["elasticsearch.document.delete"], sess, map[string]string{"index": index, "id": "ada"}, nil, nil)

	settingsBody, _ := json.Marshal(map[string]any{"settings": map[string]any{"index": map[string]any{"refresh_interval": "30s"}}})
	call(ctx, t, routes["elasticsearch.settings.update"], sess, map[string]string{"index": index}, nil, settingsBody)
	if got := indexSetting(call(ctx, t, routes["elasticsearch.settings.read"], sess, map[string]string{"index": index}, nil, nil), index, "refresh_interval"); got != "30s" {
		t.Fatalf("refresh_interval after update: got %q want %q", got, "30s")
	}

	for _, id := range []string{"keep-1", "drop-1", "drop-2", "drop-3"} {
		group := "keep"
		if strings.HasPrefix(id, "drop") {
			group = "drop"
		}
		seedBody, _ := json.Marshal(map[string]any{"id": id, "document": map[string]any{"group": group}})
		call(ctx, t, routes["elasticsearch.document.create"], sess, map[string]string{"index": index}, nil, seedBody)
	}
	call(ctx, t, routes["elasticsearch.index.refresh"], sess, map[string]string{"index": index}, nil, nil)
	dbqBody, _ := json.Marshal(map[string]any{"query": map[string]any{"term": map[string]any{"group": "drop"}}})
	dbqRes := call(ctx, t, routes["elasticsearch.documents.delete_by_query"], sess, map[string]string{"index": index}, nil, dbqBody).(map[string]any)
	if deleted := numericValue(dbqRes["deleted"]); deleted != 3 {
		t.Fatalf("delete_by_query deleted: got %v want 3 (%#v)", dbqRes["deleted"], dbqRes)
	}
	call(ctx, t, routes["elasticsearch.index.refresh"], sess, map[string]string{"index": index}, nil, nil)
	remaining := pageItems(call(ctx, t, routes["elasticsearch.documents.list"], sess, map[string]string{"index": index}, url.Values{"limit": []string{"10"}}, nil))
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

func elasticsearchIntegrationConfig(ctx context.Context, t *testing.T) map[string]any {
	t.Helper()
	endpoint := os.Getenv("SHELLCN_ELASTICSEARCH_ENDPOINT")
	if endpoint == "" {
		endpoint = startElasticsearchContainer(ctx, t)
	}
	return map[string]any{"endpoint": endpoint, "auth": "none", "tls_mode": "disable", "read_only": false, "page_limit": 100, "timeout": "60s"}
}

func startElasticsearchContainer(ctx context.Context, t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_ELASTICSEARCH_ENDPOINT is not set")
	}
	name := "shellcn-elasticsearch-it-" + time.Now().UTC().Format("20060102150405")
	run(ctx, t, "docker", "run", "-d", "--rm", "--name", name,
		"-e", "discovery.type=single-node",
		"-e", "xpack.security.enabled=false",
		"-e", "xpack.ml.enabled=false",
		"-e", "xpack.watcher.enabled=false",
		"-e", "ingest.geoip.downloader.enabled=false",
		"-e", "cluster.routing.allocation.disk.threshold_enabled=false",
		"-e", "ES_JAVA_OPTS=-Xms1g -Xmx1g",
		"-p", "127.0.0.1::9200",
		"docker.elastic.co/elasticsearch/elasticsearch:9.4.1")
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
	deadline := time.Now().Add(240 * time.Second)
	for {
		sess, err := escompat.Connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()}, escompat.Provider{Protocol: "elasticsearch", DefaultURL: endpoint, Product: escompat.ProductElasticsearch})
		if err == nil {
			_ = sess.Close()
			healthCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
			healthErr := elasticsearchRaw(healthCtx, http.MethodGet, endpoint+"/_cluster/health?wait_for_status=yellow&timeout=120s", nil)
			cancel()
			if healthErr == nil {
				readyCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
				readyErr := elasticsearchReady(readyCtx, endpoint, ".shellcn-ready-"+time.Now().UTC().Format("20060102150405.000000000"))
				cancel()
				if readyErr == nil {
					return endpoint
				}
				err = readyErr
			} else {
				err = healthErr
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("Elasticsearch container did not become ready: %v", err)
		}
		time.Sleep(750 * time.Millisecond)
	}
}

func elasticsearchReady(ctx context.Context, endpoint, index string) error {
	body := []byte(`{"settings":{"number_of_replicas":0}}`)
	if err := elasticsearchRaw(ctx, http.MethodPut, endpoint+"/"+index+"?wait_for_active_shards=1", body); err != nil {
		return err
	}
	if err := elasticsearchRaw(ctx, http.MethodGet, endpoint+"/_cluster/health/"+index+"?wait_for_status=yellow&timeout=120s", nil); err != nil {
		return err
	}
	deleteCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = elasticsearchRaw(deleteCtx, http.MethodDelete, endpoint+"/"+index, nil)
	return nil
}

func elasticsearchRaw(ctx context.Context, method, url string, body []byte) error {
	var reader io.Reader
	if len(body) > 0 {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return fmt.Errorf("%s %s returned %d: %s", method, url, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return nil
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
