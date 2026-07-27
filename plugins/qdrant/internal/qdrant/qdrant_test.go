package qdrant

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestManifestValidates(t *testing.T) {
	p := New()
	plugintest.ValidatePlugin(t, p)
	if !plugintest.CredentialKindSupported(p.Manifest().Config, plugin.CredentialKindAPIToken) {
		t.Fatal("Qdrant should support stored API token credentials")
	}
}

func TestCollectionsList(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/collections":
			_, _ = w.Write([]byte(`{"status":"ok","result":{"collections":[{"name":"docs"}]}}`))
		case "/collections/docs":
			_, _ = w.Write([]byte(`{"status":"ok","result":{"status":"green","points_count":3}}`))
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
	routes := plugintest.RouteMap(p.Routes())
	out, err := routes[rid("collections.list")].Handle(plugin.NewRequestContext(context.Background(), plugin.User{}, sess, nil, nil, nil))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	page := out.(plugin.Page[row])
	if len(page.Items) != 1 || page.Items[0]["name"] != "docs" || page.Items[0]["points_count"] == nil {
		t.Fatalf("unexpected page: %#v", page.Items)
	}
}

func TestCollectionsListDescribesOnlyThePage(t *testing.T) {
	var mu sync.Mutex
	details := 0
	names := make([]string, 0, 200)
	for i := range 200 {
		names = append(names, fmt.Sprintf(`{"name":"c%03d"}`, i))
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/collections" {
			_, _ = w.Write([]byte(`{"status":"ok","result":{"collections":[` + strings.Join(names, ",") + `]}}`))
			return
		}
		mu.Lock()
		details++
		mu.Unlock()
		_, _ = w.Write([]byte(`{"status":"ok","result":{"status":"green","points_count":3}}`))
	}))
	defer srv.Close()

	p := New()
	sess, err := p.Connect(context.Background(), plugin.ConnectConfig{Config: map[string]any{"endpoint": srv.URL}, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(p.Routes())
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess, nil, url.Values{"limit": []string{"10"}}, nil)
	out, err := routes[rid("collections.list")].Handle(rc)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	page := out.(plugin.Page[row])
	if len(page.Items) != 10 || page.NextCursor == "" {
		t.Fatalf("expected one bounded page, got %d items (next %q)", len(page.Items), page.NextCursor)
	}
	mu.Lock()
	defer mu.Unlock()
	if details != 10 {
		t.Fatalf("expected 10 detail fetches, got %d", details)
	}
}

func TestCollectionsListSortsAndSearchesDescribedColumns(t *testing.T) {
	names := make([]string, 0, 200)
	for i := range 200 {
		names = append(names, fmt.Sprintf(`{"name":"c%03d"}`, i))
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/collections" {
			_, _ = w.Write([]byte(`{"status":"ok","result":{"collections":[` + strings.Join(names, ",") + `]}}`))
			return
		}
		points := 0
		_, _ = fmt.Sscanf(strings.TrimPrefix(r.URL.Path, "/collections/"), "c%d", &points)
		status := "green"
		if points == 42 {
			status = "yellow"
		}
		_, _ = fmt.Fprintf(w, `{"status":"ok","result":{"status":%q,"points_count":%d}}`, status, points)
	}))
	defer srv.Close()

	p := New()
	sess, err := p.Connect(context.Background(), plugin.ConnectConfig{Config: map[string]any{"endpoint": srv.URL}, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(p.Routes())

	list := func(query url.Values) plugin.Page[row] {
		t.Helper()
		out, err := routes[rid("collections.list")].Handle(plugin.NewRequestContext(context.Background(), plugin.User{}, sess, nil, query, nil))
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		return out.(plugin.Page[row])
	}

	sorted := list(url.Values{"limit": []string{"10"}, "sort": []string{"-points_count"}})
	if len(sorted.Items) != 10 || fmt.Sprint(sorted.Items[0]["name"]) != "c199" {
		t.Fatalf("a detail-column sort must order every collection, got %#v", sorted.Items)
	}
	searched := list(url.Values{"limit": []string{"10"}, "filter": []string{"yellow"}})
	if len(searched.Items) != 1 || fmt.Sprint(searched.Items[0]["name"]) != "c042" {
		t.Fatalf("the search box must match described columns, got %#v", searched.Items)
	}
}

func TestRequestBodyAcceptsCodeEditorPayloads(t *testing.T) {
	for _, body := range [][]byte{
		[]byte(`{"body":{"points":[{"id":1,"vector":[0.1,0.2,0.3,0.4]}]}}`),
		[]byte(`{"content":"{\"points\":[{\"id\":1,\"vector\":[0.1,0.2,0.3,0.4]}]}"}`),
	} {
		got, err := requestBody(plugin.NewRequestContext(context.Background(), plugin.User{}, nil, nil, nil, body))
		if err != nil {
			t.Fatalf("requestBody: %v", err)
		}
		m, ok := got.(map[string]any)
		if !ok || m["points"] == nil {
			t.Fatalf("unexpected body: %#v", got)
		}
	}
}

func TestQueryBodyAcceptsQueryEditorFrame(t *testing.T) {
	body, err := queryBody(map[string]any{
		"query":   `{"query":[0.1,0.2,0.3,0.4],"limit":2,"with_payload":true}`,
		"confirm": false,
	})
	if err != nil {
		t.Fatalf("queryBody: %v", err)
	}
	m, ok := body.(map[string]any)
	if !ok || m["limit"] != float64(2) {
		t.Fatalf("unexpected query body: %#v", body)
	}
}

func TestQueryResultReturnsTableShape(t *testing.T) {
	var raw any
	if err := json.Unmarshal([]byte(`{"points":[{"id":1,"score":0.99,"payload":{"title":"Ada"},"vector":[0.1,0.2,0.3,0.4]}]}`), &raw); err != nil {
		t.Fatal(err)
	}
	got := queryResult(raw)
	if got["rowCount"] != 1 {
		t.Fatalf("unexpected result: %#v", got)
	}
	rows := got["rows"].([][]any)
	if rows[0][2] != `{"title":"Ada"}` {
		t.Fatalf("unexpected rows: %#v", rows)
	}
}

func TestShouldRetryQdrant(t *testing.T) {
	transient := errors.New(`unavailable: Put "http://localhost:6333/collections/docs/points": EOF`)
	if !shouldRetryQdrant(http.MethodPut, "/collections/docs/points", transient) {
		t.Fatal("expected put points retry on transient unavailable")
	}
	if shouldRetryQdrant(http.MethodPost, "/collections/docs/points/delete", transient) {
		t.Fatal("delete should not retry")
	}
	if shouldRetryQdrant(http.MethodPost, "/collections/docs/points", plugin.ErrInvalidInput) {
		t.Fatal("non-transient error should not retry")
	}
}
