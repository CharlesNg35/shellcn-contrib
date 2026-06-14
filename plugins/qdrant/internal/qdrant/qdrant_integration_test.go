package qdrant

import (
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

func TestQdrantPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_QDRANT_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_QDRANT_INTEGRATION=1 to run against Qdrant")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cfg := qdrantIntegrationConfig(ctx, t)
	p := New()
	sess, err := p.Connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	h := contribtest.NewHarness(t, p.Routes())

	collection := "shellcn_it_" + time.Now().UTC().Format("20060102150405")
	createBody := mustJSON(t, map[string]any{"name": collection, "vector_size": 3, "distance": "Cosine"})
	h.Call(ctx, rid("collection.create"), sess, nil, nil, createBody)
	defer h.CallNoFail(context.Background(), rid("collection.delete"), sess, map[string]string{"collection": collection})

	upsertBody := mustJSON(t, map[string]any{"points": []any{
		map[string]any{"id": 1, "vector": []float64{0.1, 0.2, 0.3}, "payload": map[string]any{"title": "Ada"}},
	}})
	h.Call(ctx, rid("point.upsert"), sess, map[string]string{"collection": collection}, nil, upsertBody)
	h.Call(ctx, rid("payload.index.create"), sess, map[string]string{"collection": collection}, nil, mustJSON(t, map[string]any{"field_name": "title", "field_schema": "keyword"}))

	h.Call(ctx, rid("overview"), sess, nil, nil, nil)
	collections := pageItems(h.Call(ctx, rid("collections.list"), sess, nil, nil, nil))
	if !hasName(collections, collection) {
		t.Fatalf("created collection not listed: %#v", collections)
	}
	h.Call(ctx, rid("collections.tree"), sess, nil, nil, nil)
	h.Call(ctx, rid("collection.read"), sess, map[string]string{"collection": collection}, nil, nil)
	alias := collection + "_alias"
	h.Call(ctx, rid("alias.create"), sess, map[string]string{"collection": collection}, nil, mustJSON(t, map[string]any{"alias_name": alias}))
	aliases := pageItems(h.Call(ctx, rid("collection.aliases"), sess, map[string]string{"collection": collection}, nil, nil))
	if !hasAlias(aliases, alias) {
		t.Fatalf("expected alias %q, got %#v", alias, aliases)
	}
	h.Call(ctx, rid("alias.delete"), sess, map[string]string{"alias": alias}, nil, nil)
	points := pageItems(h.Call(ctx, rid("points.list"), sess, map[string]string{"collection": collection}, url.Values{"limit": []string{"10"}}, nil))
	if len(points) != 1 || toString(points[0]["id"]) != "1" {
		t.Fatalf("expected one point, got %#v", points)
	}
	point := h.Call(ctx, rid("point.read"), sess, map[string]string{"collection": collection, "point": "1"}, nil, nil)
	if payload, ok := field(point, "payload").(map[string]any); !ok || payload["title"] != "Ada" {
		t.Fatalf("unexpected point payload: %#v", point)
	}
	h.Call(ctx, rid("snapshots.list"), sess, map[string]string{"collection": collection}, nil, nil)
	h.Call(ctx, rid("snapshot.create"), sess, map[string]string{"collection": collection}, nil, nil)
	streamOut := h.Stream(ctx, rid("query"), sess, map[string]string{"collection": collection}, nil, []byte(`{"query":[0.1,0.2,0.3],"limit":10,"with_payload":true}`+"\n"))
	if !strings.Contains(string(streamOut), "Ada") {
		t.Fatalf("unexpected query stream output: %s", streamOut)
	}
	h.Call(ctx, rid("point.delete"), sess, map[string]string{"collection": collection, "point": "1"}, nil, nil)
	h.Call(ctx, rid("collection.delete"), sess, map[string]string{"collection": collection}, nil, nil)
	h.AssertAllCovered()
}

func qdrantIntegrationConfig(ctx context.Context, t *testing.T) map[string]any {
	t.Helper()
	endpoint := os.Getenv("SHELLCN_QDRANT_ENDPOINT")
	if endpoint == "" {
		endpoint = startQdrantContainer(ctx, t)
	}
	return map[string]any{"endpoint": endpoint, "auth": "none", "tls_mode": "disable", "read_only": false, "page_limit": 100, "timeout": "10s"}
}

func startQdrantContainer(ctx context.Context, t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_QDRANT_ENDPOINT is not set")
	}
	name := "shellcn-qdrant-it-" + time.Now().UTC().Format("20060102150405")
	run(ctx, t, "docker", "run", "-d", "--rm", "--name", name, "-p", "127.0.0.1::6333", "qdrant/qdrant:v1.15.4")
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})
	out := run(ctx, t, "docker", "port", name, "6333/tcp")
	host, port, err := net.SplitHostPort(strings.TrimSpace(out))
	if err != nil {
		t.Fatalf("unexpected docker port output: %q", out)
	}
	endpoint := "http://" + net.JoinHostPort(host, port)
	deadline := time.Now().Add(60 * time.Second)
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/collections", nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil && resp.StatusCode < 500 {
			_ = resp.Body.Close()
			return endpoint
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("Qdrant container did not become ready: %v", err)
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

func hasName(items []map[string]any, name string) bool {
	for _, item := range items {
		if toString(item["name"]) == name {
			return true
		}
	}
	return false
}

func hasAlias(items []map[string]any, name string) bool {
	for _, item := range items {
		if toString(item["alias_name"]) == name {
			return true
		}
	}
	return false
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
	case float64:
		return fmt.Sprint(int64(t))
	default:
		return strings.TrimSpace(strings.Trim(fmt.Sprint(v), `"`))
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
