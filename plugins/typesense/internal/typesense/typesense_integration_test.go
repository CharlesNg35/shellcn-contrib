package typesense

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

const integrationKey = "shellcn_typesense_key"

func TestTypesensePluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_TYPESENSE_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_TYPESENSE_INTEGRATION=1 to run against Typesense")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cfg := typesenseIntegrationConfig(ctx, t)
	p := New()
	sess, err := p.Connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()

	routes := plugintest.RouteMap(p.Routes())
	collection := "shellcn_it_" + time.Now().UTC().Format("20060102150405")
	createBody, _ := json.Marshal(map[string]any{
		"name": collection,
		"fields": []any{
			map[string]any{"name": "name", "type": "string"},
			map[string]any{"name": "age", "type": "int32", "facet": true},
		},
	})
	call(ctx, t, routes["typesense.collection.create"], sess, nil, nil, createBody)
	defer callNoFail(context.Background(), routes["typesense.collection.delete"], sess, map[string]string{"collection": collection})
	clone := collection + "_clone"
	cloneBody, _ := json.Marshal(map[string]any{"source": collection, "name": clone, "copy_documents": false})
	call(ctx, t, routes["typesense.collection.clone"], sess, nil, nil, cloneBody)
	defer callNoFail(context.Background(), routes["typesense.collection.delete"], sess, map[string]string{"collection": clone})
	updateBody, _ := json.Marshal(map[string]any{"schema": map[string]any{"fields": []any{map[string]any{"name": "tag", "type": "string", "optional": true}}}})
	call(ctx, t, routes["typesense.collection.update"], sess, map[string]string{"collection": collection}, nil, updateBody)

	docBody, _ := json.Marshal(map[string]any{"document": map[string]any{"id": "ada", "name": "Ada Lovelace", "age": 37}, "action": "upsert"})
	call(ctx, t, routes["typesense.document.upsert"], sess, map[string]string{"collection": collection}, nil, docBody)

	call(ctx, t, routes["typesense.overview"], sess, nil, nil, nil)
	call(ctx, t, routes["typesense.collections.tree"], sess, nil, nil, nil)
	call(ctx, t, routes["typesense.collection.overview"], sess, map[string]string{"collection": collection}, nil, nil)
	docs := call(ctx, t, routes["typesense.documents.list"], sess, map[string]string{"collection": collection}, url.Values{"limit": []string{"10"}}, nil)
	if items := pageItems(docs); len(items) != 1 || items[0]["_id"] != "ada" {
		t.Fatalf("expected one document, got %#v", items)
	}
	read := call(ctx, t, routes["typesense.document.read"], sess, map[string]string{"collection": collection, "id": "ada"}, nil, nil)
	if field(read, "name") != "Ada Lovelace" {
		t.Fatalf("unexpected document: %#v", read)
	}
	docUpdateBody, _ := json.Marshal(map[string]any{"content": `{"tag":"math"}`})
	call(ctx, t, routes["typesense.document.update"], sess, map[string]string{"collection": collection, "id": "ada"}, nil, docUpdateBody)
	call(ctx, t, routes["typesense.completion"], sess, nil, nil, nil)
	if result, err := executeSearch(ctx, sess.(*Session), collection, map[string]any{"q": "Ada", "query_by": "name", "per_page": 10}); err != nil || result.RowCount < 1 {
		t.Fatalf("execute search: result=%#v err=%v", result, err)
	}

	alias := collection + "_alias"
	aliasBody, _ := json.Marshal(map[string]any{"name": alias, "collection_name": collection})
	call(ctx, t, routes["typesense.alias.upsert"], sess, nil, nil, aliasBody)
	call(ctx, t, routes["typesense.alias.read"], sess, map[string]string{"alias": alias}, nil, nil)
	call(ctx, t, routes["typesense.aliases.list"], sess, nil, nil, nil)
	call(ctx, t, routes["typesense.aliases.tree"], sess, nil, nil, nil)

	synBody, _ := json.Marshal(map[string]any{"id": "shellcn-synonyms", "synonym": map[string]any{"items": []any{map[string]any{"id": "ada", "synonyms": []any{"ada", "lovelace"}}}}})
	call(ctx, t, routes["typesense.synonym.upsert"], sess, nil, nil, synBody)
	call(ctx, t, routes["typesense.synonyms.list"], sess, nil, nil, nil)

	overrideBody, _ := json.Marshal(map[string]any{"id": "shellcn-curations", "override": map[string]any{
		"items": []any{map[string]any{"id": "pin-ada", "rule": map[string]any{"query": "ada", "match": "exact"}, "includes": []any{map[string]any{"id": "ada", "position": 1}}}},
	}})
	call(ctx, t, routes["typesense.override.upsert"], sess, nil, nil, overrideBody)
	call(ctx, t, routes["typesense.overrides.list"], sess, nil, nil, nil)

	importBody, _ := json.Marshal(map[string]any{"action": "upsert", "documents": "{\"id\":\"grace\",\"name\":\"Grace Hopper\",\"age\":85}\n"})
	call(ctx, t, routes["typesense.documents.import"], sess, map[string]string{"collection": collection}, nil, importBody)
	call(ctx, t, routes["typesense.documents.export"], sess, map[string]string{"collection": collection}, nil, nil)

	keyBody, _ := json.Marshal(map[string]any{"description": "integration", "actions": []any{"documents:search"}, "collections": []any{collection}})
	key := call(ctx, t, routes["typesense.key.create"], sess, nil, nil, keyBody)
	keyID := strings.TrimSpace(toString(field(key, "id")))
	if keyID != "" {
		call(ctx, t, routes["typesense.key.read"], sess, map[string]string{"key": keyID}, nil, nil)
		call(ctx, t, routes["typesense.keys.tree"], sess, nil, nil, nil)
		call(ctx, t, routes["typesense.key.delete"], sess, map[string]string{"key": keyID}, nil, nil)
	}

	call(ctx, t, routes["typesense.override.delete"], sess, map[string]string{"override": "shellcn-curations"}, nil, nil)
	call(ctx, t, routes["typesense.synonym.delete"], sess, map[string]string{"synonym": "shellcn-synonyms"}, nil, nil)
	call(ctx, t, routes["typesense.alias.delete"], sess, map[string]string{"alias": alias}, nil, nil)
	call(ctx, t, routes["typesense.document.delete"], sess, map[string]string{"collection": collection, "id": "ada"}, nil, nil)
}

func typesenseIntegrationConfig(ctx context.Context, t *testing.T) map[string]any {
	t.Helper()
	endpoint := os.Getenv("SHELLCN_TYPESENSE_ENDPOINT")
	key := os.Getenv("SHELLCN_TYPESENSE_API_KEY")
	if key == "" {
		key = integrationKey
	}
	if endpoint == "" {
		endpoint = startTypesenseContainer(ctx, t, key)
	}
	return map[string]any{"endpoint": endpoint, "auth": "api_key", "api_key": key, "tls_mode": "disable", "read_only": false, "page_limit": 100, "timeout": "10s"}
}

func startTypesenseContainer(ctx context.Context, t *testing.T, key string) string {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_TYPESENSE_ENDPOINT is not set")
	}
	name := "shellcn-typesense-it-" + time.Now().UTC().Format("20060102150405")
	run(ctx, t, "docker", "run", "-d", "--rm", "--name", name,
		"-p", "127.0.0.1::8108",
		"typesense/typesense:30.2",
		"--data-dir", "/tmp",
		"--api-key", key,
		"--enable-cors")
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})
	out := run(ctx, t, "docker", "port", name, "8108/tcp")
	host, port, err := net.SplitHostPort(strings.TrimSpace(out))
	if err != nil {
		t.Fatalf("unexpected docker port output: %q", out)
	}
	endpoint := "http://" + net.JoinHostPort(host, port)
	cfg := map[string]any{"endpoint": endpoint, "auth": "api_key", "api_key": key, "tls_mode": "disable", "read_only": false, "page_limit": 100, "timeout": "10s"}
	deadline := time.Now().Add(60 * time.Second)
	for {
		p := New()
		sess, err := p.Connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
		if err == nil {
			_ = sess.Close()
			return endpoint
		}
		if time.Now().After(deadline) {
			t.Fatalf("Typesense container did not become ready: %v", err)
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

func toString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		return strconv.FormatInt(int64(t), 10)
	case int:
		return strconv.Itoa(t)
	default:
		return strings.TrimSpace(strings.Trim(fmt.Sprint(v), "\""))
	}
}

func field(v any, key string) any {
	switch t := v.(type) {
	case map[string]any:
		return t[key]
	default:
		return nil
	}
}
