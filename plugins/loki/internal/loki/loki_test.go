package loki

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	contribtest "github.com/charlesng35/shellcn-contrib/shared/plugintest"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestManifestValidates(t *testing.T) {
	p := New()
	plugintest.ValidatePlugin(t, p)
	if !plugintest.CredentialKindSupported(p.Manifest().Config, plugin.CredentialBearerToken) {
		t.Fatal("Loki should support stored bearer token credentials")
	}
}

func TestTenantHeaderAndRules(t *testing.T) {
	var tenant string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenant = r.Header.Get("X-Scope-OrgID")
		switch r.URL.Path {
		case "/ready":
			_, _ = w.Write([]byte("ready"))
		case "/loki/api/v1/rules":
			_, _ = w.Write([]byte(`{"status":"success","data":{"groups":[{"name":"ops","file_path":"prod","rules":[{"alert":"HighErrors","expr":"sum(rate({app=\"api\"}[5m])) > 0"}]}]}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	p := New()
	cfg := map[string]any{"endpoint": srv.URL, "tenant_id": "acme"}
	sess, err := p.Connect(context.Background(), plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	h := contribtest.NewHarness(t, p.Routes())
	out := h.Call(context.Background(), rid("rules.list"), sess, nil, nil, nil)
	if tenant != "acme" {
		t.Fatalf("tenant header not sent: %q", tenant)
	}
	page := out.(plugin.Page[row])
	if len(page.Items) != 1 || page.Items[0]["name"] != "HighErrors" || page.Items[0]["type"] != "alert" {
		t.Fatalf("unexpected rules: %#v", page.Items)
	}
	h.AssertCovered(rid("rules.list"))
}

func TestDeleteCreateHonorsReadOnly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ready":
			_, _ = w.Write([]byte("ready"))
		default:
			t.Fatalf("unexpected request: %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	p := New()
	sess, err := p.Connect(context.Background(), plugin.ConnectConfig{Config: map[string]any{"endpoint": srv.URL, "read_only": true}, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	h := contribtest.NewHarness(t, p.Routes())
	_, err = h.Route(rid("delete.create")).Handle(plugin.NewRequestContext(
		context.Background(),
		plugin.User{},
		sess,
		nil,
		nil,
		[]byte(`{"query":"{app=\"api\"}","start":"2026-06-05T00:00:00Z","end":"2026-06-05T01:00:00Z"}`),
	))
	if !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("expected ErrForbidden, got %v", err)
	}
}

func TestOperationalRoutes(t *testing.T) {
	seen := map[string]bool{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen[r.Method+" "+r.URL.Path] = true
		switch {
		case r.URL.Path == "/ready":
			_, _ = w.Write([]byte("ready"))
		case r.URL.Path == "/loki/api/v1/index/stats":
			_, _ = w.Write([]byte(`{"status":"success","data":{"streams":2,"chunks":3,"entries":4,"bytes":512}}`))
		case r.URL.Path == "/loki/api/v1/index/volume":
			_, _ = w.Write([]byte(`{"status":"success","data":{"result":[{"metric":{"app":"api"},"value":[1717588800,"2048"]}]}}`))
		case r.URL.Path == "/loki/api/v1/format_query":
			if r.URL.Query().Get("query") == "" {
				t.Fatal("missing query")
			}
			_, _ = w.Write([]byte(`{"status":"success","data":"{app=\"api\"}"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/loki/api/v1/delete":
			_, _ = w.Write([]byte(`{"status":"success","data":[{"request_id":"delete-1","query":"{app=\"api\"}","status":"received","start_time":"2026-06-05T00:00:00Z","end_time":"2026-06-05T01:00:00Z"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/loki/api/v1/delete":
			if r.URL.Query().Get("query") == "" || r.URL.Query().Get("start") == "" || r.URL.Query().Get("end") == "" {
				t.Fatalf("bad delete create query: %s", r.URL.RawQuery)
			}
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodDelete && r.URL.Path == "/loki/api/v1/delete":
			if r.URL.Query().Get("request_id") != "delete-1" {
				t.Fatalf("bad delete cancel query: %s", r.URL.RawQuery)
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	p := New()
	sess, err := p.Connect(context.Background(), plugin.ConnectConfig{Config: map[string]any{"endpoint": srv.URL, "read_only": false}, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	h := contribtest.NewHarness(t, p.Routes())

	stats := h.Call(context.Background(), rid("stats.read"), sess, nil, url.Values{"query": []string{`{app="api"}`}}, nil)
	if field(stats, "bytes") != float64(512) {
		t.Fatalf("unexpected stats: %#v", stats)
	}
	volume := h.Call(context.Background(), rid("volume.list"), sess, nil, nil, nil)
	if items := pageItems(volume); len(items) != 1 || items[0]["value"] != float64(2048) {
		t.Fatalf("unexpected volume: %#v", volume)
	}
	formatted := h.Call(context.Background(), rid("query.format"), sess, nil, nil, mustJSON(t, map[string]any{"query": `{ app = "api" }`}))
	if field(formatted, "query") != `{app="api"}` {
		t.Fatalf("unexpected formatted query: %#v", formatted)
	}
	deletes := h.Call(context.Background(), rid("deletes.list"), sess, nil, nil, nil)
	if items := pageItems(deletes); len(items) != 1 || items[0]["request_id"] != "delete-1" {
		t.Fatalf("unexpected deletes: %#v", deletes)
	}
	h.Call(context.Background(), rid("delete.create"), sess, nil, nil, mustJSON(t, map[string]any{"query": `{app="api"}`, "start": "2026-06-05T00:00:00Z", "end": "2026-06-05T01:00:00Z"}))
	h.Call(context.Background(), rid("delete.cancel"), sess, map[string]string{"request": "delete-1"}, nil, mustJSON(t, map[string]any{"force": true}))
	h.AssertCovered(rid("stats.read"), rid("volume.list"), rid("query.format"), rid("deletes.list"), rid("delete.create"), rid("delete.cancel"))
	for _, key := range []string{
		"GET /loki/api/v1/index/stats",
		"GET /loki/api/v1/index/volume",
		"GET /loki/api/v1/format_query",
		"GET /loki/api/v1/delete",
		"POST /loki/api/v1/delete",
		"DELETE /loki/api/v1/delete",
	} {
		if !seen[key] {
			t.Fatalf("route did not call %s", key)
		}
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
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

func TestLabelsList(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ready":
			_, _ = w.Write([]byte("ready"))
		case "/loki/api/v1/labels":
			_, _ = w.Write([]byte(`{"status":"success","data":["app","job"]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	p := New()
	sess, err := p.Connect(context.Background(), plugin.ConnectConfig{Config: map[string]any{"endpoint": srv.URL}, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	out, err := plugintest.RouteMap(p.Routes())[rid("labels.list")].Handle(plugin.NewRequestContext(context.Background(), plugin.User{}, sess, nil, nil, nil))
	if err != nil {
		t.Fatalf("labels: %v", err)
	}
	page := out.(plugin.Page[row])
	if len(page.Items) != 2 || page.Items[0]["name"] != "app" {
		t.Fatalf("unexpected labels: %#v", page.Items)
	}
}
