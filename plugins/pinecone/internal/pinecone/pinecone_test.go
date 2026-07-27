package pinecone

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	contribtest "github.com/charlesng35/shellcn-contrib/shared/plugintest"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

const (
	testAPIKey    = "pcsk_test_key"
	testIndex     = "docs"
	testNamespace = "docs-ns"
	testVectors   = 250
)

func TestManifestValidates(t *testing.T) {
	p := New()
	plugintest.ValidatePlugin(t, p)
	if !plugintest.CredentialKindSupported(p.Manifest().Config, plugin.CredentialKindAPIToken) {
		t.Fatal("Pinecone should accept stored API token credentials")
	}
}

func TestManifestPanelsCoverTheDomain(t *testing.T) {
	manifest := New().Manifest()
	kinds := map[string]map[string]plugin.PanelType{}
	for _, resource := range manifest.Resources {
		tabs := map[string]plugin.PanelType{}
		for _, tab := range resource.Detail.Tabs {
			tabs[tab.Key] = tab.Type
		}
		kinds[resource.Kind] = tabs
	}
	for _, want := range []struct {
		kind, tab string
		panel     plugin.PanelType
	}{
		{"project", "indexes", plugin.PanelTable},
		{"index", "overview", plugin.PanelObjectDetail},
		{"index", "namespaces", plugin.PanelTable},
		{"index", "vectors", plugin.PanelTable},
		{"index", "search", plugin.PanelQueryEditor},
		{"index", "metrics", plugin.PanelMetrics},
		{"namespace", "vectors", plugin.PanelTable},
		{"vector", "overview", plugin.PanelObjectDetail},
		{"vector", "neighbors", plugin.PanelTable},
		{"collection", "overview", plugin.PanelObjectDetail},
	} {
		if got := kinds[want.kind][want.tab]; got != want.panel {
			t.Errorf("resource %q tab %q: want panel %q, got %q", want.kind, want.tab, want.panel, got)
		}
	}
}

func TestActionsResolveTheirIndexFromTheRightResource(t *testing.T) {
	manifest := New().Manifest()
	byID := make(map[string]plugin.Action, len(manifest.Actions))
	for _, action := range manifest.Actions {
		byID[action.ID] = action
	}
	for _, resource := range manifest.Resources {
		ids := append(append([]string{}, resource.Actions.Detail...), resource.Actions.Row...)
		for _, id := range ids {
			action, ok := byID[id]
			if !ok {
				t.Fatalf("resource %q references unknown action %q", resource.Kind, id)
			}
			// ${resource.name} is the open resource, so only an index resource may
			// use it to address an index.
			if action.Params["index"] == "${resource.name}" && resource.Kind != "index" {
				t.Errorf("action %q on resource %q takes its index from the wrong resource", id, resource.Kind)
			}
			if editor, ok := action.Config.(plugin.CodeEditorConfig); ok {
				if fmt.Sprint(editor.SaveParams) != fmt.Sprint(action.Params) {
					t.Errorf("action %q saves with %v but opens with %v", id, editor.SaveParams, action.Params)
				}
			}
		}
	}
}

func TestRoutesAreNamespacedAndAudited(t *testing.T) {
	for _, route := range New().Routes() {
		if !strings.HasPrefix(route.ID, protocolName+".") {
			t.Errorf("route %q is outside the plugin namespace", route.ID)
		}
		if route.AuditEvent != route.ID {
			t.Errorf("route %q audits as %q", route.ID, route.AuditEvent)
		}
		if route.Permission == "" {
			t.Errorf("route %q has no permission", route.ID)
		}
		if route.Method == plugin.MethodWS && route.Stream == nil {
			t.Errorf("route %q is a websocket without a stream handler", route.ID)
		}
	}
}

func TestEveryRouteIsCoveredAgainstTheMockAPI(t *testing.T) {
	srv, mock := newMockAPI(t)
	defer srv.Close()
	ctx := context.Background()
	p, sess := connect(t, srv.URL, false)
	defer func() { _ = sess.Close() }()
	h := contribtest.NewHarness(t, p.Routes())

	index := map[string]string{"index": testIndex}
	scoped := map[string]string{"index": testIndex, "namespace": testNamespace}

	h.Call(ctx, rid("overview"), sess, nil, nil, nil)
	h.Call(ctx, rid("indexes.tree"), sess, nil, nil, nil)
	h.Call(ctx, rid("indexes.list"), sess, nil, nil, nil)
	h.Call(ctx, rid("index.read"), sess, index, nil, nil)
	h.Call(ctx, rid("index.stats"), sess, index, nil, nil)
	h.Call(ctx, rid("index.create"), sess, nil, nil, mustJSON(t, map[string]any{
		"name": "new-index", "deployment": "serverless", "vector_type": "dense",
		"dimension": 3, "metric": "cosine", "cloud": "aws", "region": "us-east-1",
	}))
	h.Call(ctx, rid("index.configure"), sess, index, nil, mustJSON(t, map[string]any{
		"deletion_protection": "enabled", "tags": map[string]string{"env": "test"},
	}))
	h.Call(ctx, rid("collections.tree"), sess, nil, nil, nil)
	h.Call(ctx, rid("collections.list"), sess, nil, nil, nil)
	h.Call(ctx, rid("collection.read"), sess, map[string]string{"collection": "docs-backup"}, nil, nil)
	h.Call(ctx, rid("collection.create"), sess, nil, nil, mustJSON(t, map[string]any{"name": "docs-copy", "source": testIndex}))
	h.Call(ctx, rid("collection.delete"), sess, map[string]string{"collection": "docs-copy"}, nil, nil)
	h.Call(ctx, rid("namespaces.list"), sess, index, nil, nil)
	h.Call(ctx, rid("namespace.read"), sess, scoped, nil, nil)
	h.Call(ctx, rid("namespace.delete"), sess, map[string]string{"index": testIndex, "namespace": "scratch"}, nil, nil)
	h.Call(ctx, rid("vectors.list"), sess, scoped, url.Values{"limit": []string{"5"}}, nil)
	h.Call(ctx, rid("vector.read"), sess, map[string]string{"index": testIndex, "namespace": testNamespace, "vector": "v001"}, nil, nil)
	h.Call(ctx, rid("vector.neighbors"), sess, map[string]string{"index": testIndex, "namespace": testNamespace, "vector": "v001"}, url.Values{"limit": []string{"3"}}, nil)
	h.Call(ctx, rid("vectors.upsert"), sess, scoped, nil, mustJSON(t, map[string]any{
		"vectors": []any{map[string]any{"id": "v999", "values": []float64{0.1, 0.2, 0.3}}},
	}))
	h.Call(ctx, rid("vectors.delete"), sess, scoped, nil, mustJSON(t, map[string]any{"ids": []string{"v999"}}))
	h.Call(ctx, rid("query.complete"), sess, scoped, nil, nil)
	h.Call(ctx, rid("index.delete"), sess, map[string]string{"index": "new-index"}, nil, nil)

	out := h.Stream(ctx, rid("query"), sess, scoped, nil, []byte(`{"query":"{\"id\":\"v001\",\"topK\":3}","confirm":false}`+"\n"))
	if !strings.Contains(string(out), `"rowCount"`) {
		t.Fatalf("query stream returned no result table: %s", out)
	}

	metricsCtx, cancel := context.WithCancel(ctx)
	time.AfterFunc(100*time.Millisecond, cancel)
	frames := h.Stream(metricsCtx, rid("index.metrics"), sess, index, nil, nil)
	if !strings.Contains(string(frames), `"vectors"`) {
		t.Fatalf("metrics stream returned no frame: %s", frames)
	}

	h.AssertAllCovered()
	if mock.upserted == 0 {
		t.Fatal("upsert never reached the mock API")
	}
}

func TestConnectSendsCredentialsAndAPIVersion(t *testing.T) {
	srv, mock := newMockAPI(t)
	defer srv.Close()
	_, sess := connect(t, srv.URL, true)
	defer func() { _ = sess.Close() }()
	if mock.lastKey != testAPIKey {
		t.Fatalf("Api-Key header was %q", mock.lastKey)
	}
	if mock.lastVersion != defaultAPIVer {
		t.Fatalf("API version header was %q", mock.lastVersion)
	}
}

func TestConnectRejectsABadKey(t *testing.T) {
	srv, _ := newMockAPI(t)
	defer srv.Close()
	_, err := New().Connect(context.Background(), plugin.ConnectConfig{
		Config: map[string]any{"endpoint": srv.URL, "auth": "api_key", "api_key": "wrong"},
		Net:    plugintest.DirectTransport(),
	})
	if !errors.Is(err, plugin.ErrUnauthorized) {
		t.Fatalf("want ErrUnauthorized, got %v", err)
	}
}

func TestConnectRequiresAnAPIKey(t *testing.T) {
	_, err := New().Connect(context.Background(), plugin.ConnectConfig{
		Config: map[string]any{"endpoint": "https://api.pinecone.io", "auth": "api_key"},
		Net:    plugintest.DirectTransport(),
	})
	if !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("want ErrInvalidInput, got %v", err)
	}
}

func TestReadOnlyBlocksMutations(t *testing.T) {
	srv, _ := newMockAPI(t)
	defer srv.Close()
	p, sess := connect(t, srv.URL, true)
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(p.Routes())
	for _, id := range []string{rid("index.delete"), rid("index.create"), rid("collection.delete"), rid("namespace.delete"), rid("vectors.upsert"), rid("vectors.delete")} {
		rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess,
			map[string]string{"index": testIndex, "namespace": testNamespace, "collection": "docs-backup"}, nil,
			[]byte(`{"ids":["v001"],"name":"x","deployment":"serverless","vectors":[]}`))
		if _, err := routes[id].Handle(rc); !errors.Is(err, plugin.ErrForbidden) {
			t.Fatalf("%s: want ErrForbidden, got %v", id, err)
		}
	}
}

func TestVectorsListPagesTheWholeNamespaceExactlyOnce(t *testing.T) {
	srv, mock := newMockAPI(t)
	defer srv.Close()
	p, sess := connect(t, srv.URL, false)
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(p.Routes())

	seen := make([]string, 0, testVectors)
	cursor, pages := "", 0
	for {
		query := url.Values{"limit": []string{"10"}}
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		page := listVectors(t, routes, sess, query)
		pages++
		if pages > testVectors {
			t.Fatal("vectors.list never terminated")
		}
		for _, item := range page.Items {
			seen = append(seen, fmt.Sprint(item["id"]))
		}
		if page.NextCursor == "" {
			break
		}
		if len(page.Items) == 0 {
			t.Fatal("a page with a continuation cursor must not be empty")
		}
		cursor = page.NextCursor
	}
	if len(seen) != testVectors {
		t.Fatalf("expected %d ids across all pages, got %d", testVectors, len(seen))
	}
	unique := map[string]bool{}
	for _, id := range seen {
		if unique[id] {
			t.Fatalf("id %q was returned twice", id)
		}
		unique[id] = true
	}
	if seen[0] != mock.vectorIDs[0] || seen[len(seen)-1] != mock.vectorIDs[testVectors-1] {
		t.Fatalf("paging changed the server order: %q..%q", seen[0], seen[len(seen)-1])
	}
}

func TestVectorsListFiltersAcrossEveryPage(t *testing.T) {
	srv, mock := newMockAPI(t)
	defer srv.Close()
	p, sess := connect(t, srv.URL, false)
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(p.Routes())

	want := 0
	for _, id := range mock.vectorIDs {
		if strings.Contains(id, "7") {
			want++
		}
	}
	seen, cursor := 0, ""
	for {
		query := url.Values{"limit": []string{"4"}, "filter": []string{"7"}}
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		page := listVectors(t, routes, sess, query)
		for _, item := range page.Items {
			if !strings.Contains(fmt.Sprint(item["id"]), "7") {
				t.Fatalf("row %v does not match the filter", item["id"])
			}
		}
		seen += len(page.Items)
		if page.NextCursor == "" {
			break
		}
		if len(page.Items) == 0 {
			t.Fatal("a filtered page with a continuation cursor must not be empty")
		}
		cursor = page.NextCursor
	}
	if seen != want {
		t.Fatalf("filter spans every page: want %d rows, got %d", want, seen)
	}
}

func TestVectorsListSortsAcrossTheDatasetNotThePage(t *testing.T) {
	srv, mock := newMockAPI(t)
	defer srv.Close()
	p, sess := connect(t, srv.URL, false)
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(p.Routes())

	page := listVectors(t, routes, sess, url.Values{"limit": []string{"5"}, "sort": []string{"-id"}})
	if len(page.Items) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(page.Items))
	}
	if got := fmt.Sprint(page.Items[0]["id"]); got != mock.vectorIDs[testVectors-1] {
		t.Fatalf("a descending sort must span the dataset, got %q first", got)
	}
}

func TestVectorsListHydratesMetadataForThePage(t *testing.T) {
	srv, _ := newMockAPI(t)
	defer srv.Close()
	p, sess := connect(t, srv.URL, false)
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(p.Routes())

	page := listVectors(t, routes, sess, url.Values{"limit": []string{"3"}})
	first := page.Items[0]
	metadata, ok := first["metadata"].(map[string]any)
	if !ok || metadata["slot"] == nil {
		t.Fatalf("page rows were not hydrated: %#v", first)
	}
	if first["dimension"] != 3 {
		t.Fatalf("expected the hydrated dimension, got %#v", first["dimension"])
	}
	ref, ok := first["ref"].(plugin.ResourceIdentity)
	if !ok || ref.Scope != testIndex || ref.Namespace != testNamespace {
		t.Fatalf("row ref lost its index/namespace scope: %#v", first["ref"])
	}
}

func TestVectorsListKeepsTheNamespaceScopeOnEveryPage(t *testing.T) {
	srv, mock := newMockAPI(t)
	defer srv.Close()
	p, sess := connect(t, srv.URL, false)
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(p.Routes())

	page := listVectors(t, routes, sess, url.Values{"limit": []string{"10"}})
	next := listVectors(t, routes, sess, url.Values{"limit": []string{"10"}, "cursor": []string{page.NextCursor}})
	if len(next.Items) == 0 {
		t.Fatal("the second page was empty")
	}
	mock.mu.Lock()
	defer mock.mu.Unlock()
	for _, ns := range mock.listNamespaces {
		if ns != testNamespace {
			t.Fatalf("a listing dropped the namespace scope: %q", ns)
		}
	}
	if len(mock.listNamespaces) < 2 {
		t.Fatalf("expected both pages to reach the API, got %d calls", len(mock.listNamespaces))
	}
}

func TestNamespacesListPagesWithoutClaimingATotalItCannotKnow(t *testing.T) {
	srv, _ := newMockAPI(t)
	defer srv.Close()
	p, sess := connect(t, srv.URL, false)
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(p.Routes())

	// The listing endpoint reports no count, so a token-paged answer must not
	// invent one; the grid pages by cursor instead.
	seen, cursor, pages := []string{}, "", 0
	for {
		query := url.Values{"limit": []string{"2"}}
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		page := listNamespaces(t, routes, sess, query)
		if page.Total != nil {
			t.Fatalf("a token-paged listing must not claim a total, got %d", *page.Total)
		}
		for _, item := range page.Items {
			seen = append(seen, fmt.Sprint(item["name"]))
		}
		if pages++; pages > len(mockNamespaces)+2 {
			t.Fatal("namespaces.list never terminated")
		}
		if page.NextCursor == "" {
			break
		}
		if len(page.Items) == 0 {
			t.Fatal("a page with a continuation cursor must not be empty")
		}
		cursor = page.NextCursor
	}
	if len(seen) != len(mockNamespaces) {
		t.Fatalf("expected every namespace exactly once, got %v", seen)
	}
	filtered := listNamespaces(t, routes, sess, url.Values{"limit": []string{"10"}, "filter": []string{"docs"}})
	if len(filtered.Items) != 1 || fmt.Sprint(filtered.Items[0]["name"]) != testNamespace {
		t.Fatalf("unexpected filtered namespaces: %#v", filtered.Items)
	}
}

func TestNamespacesFallBackToIndexStatsWhenListingIsUnsupported(t *testing.T) {
	srv, mock := newMockAPI(t)
	defer srv.Close()
	p, sess := connect(t, srv.URL, false)
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(p.Routes())

	out, err := routes[rid("namespaces.list")].Handle(plugin.NewRequestContext(context.Background(), plugin.User{}, sess,
		map[string]string{"index": podIndex}, url.Values{"limit": []string{"10"}}, nil))
	if err != nil {
		t.Fatalf("a pod-based index must still list its namespaces: %v", err)
	}
	page := out.(pagedResult).Page
	if len(page.Items) != len(mockNamespaces) {
		t.Fatalf("expected every namespace from describe_index_stats, got %#v", page.Items)
	}
	// describe_index_stats returns the whole set at once, so the total is real.
	if page.Total == nil || *page.Total != len(mockNamespaces) {
		t.Fatalf("the fallback set has an authoritative total, got %#v", page.Total)
	}
	if fmt.Sprint(page.Items[0]["name"]) != "alpha" {
		t.Fatalf("the fallback must page in a stable name order, got %#v", page.Items[0])
	}
	if fmt.Sprint(page.Items[0]["index"]) != podIndex {
		t.Fatalf("the fallback lost the index scope: %#v", page.Items[0])
	}
	mock.mu.Lock()
	defer mock.mu.Unlock()
	if mock.namespaceListCalls == 0 {
		t.Fatal("the fallback must only run after the listing endpoint was tried")
	}
}

func TestNamespacesFallbackPreservesFilterAndPaging(t *testing.T) {
	srv, _ := newMockAPI(t)
	defer srv.Close()
	p, sess := connect(t, srv.URL, false)
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(p.Routes())

	call := func(query url.Values) plugin.Page[row] {
		t.Helper()
		out, err := routes[rid("namespaces.list")].Handle(plugin.NewRequestContext(context.Background(), plugin.User{}, sess,
			map[string]string{"index": podIndex}, query, nil))
		if err != nil {
			t.Fatalf("namespaces.list: %v", err)
		}
		return out.(pagedResult).Page
	}
	filtered := call(url.Values{"limit": []string{"10"}, "filter": []string{"scratch"}})
	if len(filtered.Items) != 1 || fmt.Sprint(filtered.Items[0]["name"]) != "scratch" {
		t.Fatalf("the fallback dropped the search term: %#v", filtered.Items)
	}
	sorted := call(url.Values{"limit": []string{"1"}, "sort": []string{"-record_count"}})
	if len(sorted.Items) != 1 || fmt.Sprint(sorted.Items[0]["name"]) != "zeta" {
		t.Fatalf("the fallback must sort across the whole set: %#v", sorted.Items)
	}
	if sorted.NextCursor == "" {
		t.Fatal("the fallback must hand back a continuation cursor")
	}
	next := call(url.Values{"limit": []string{"1"}, "sort": []string{"-record_count"}, "cursor": []string{sorted.NextCursor}})
	if len(next.Items) != 1 || fmt.Sprint(next.Items[0]["name"]) == "zeta" {
		t.Fatalf("the continuation repeated the first row: %#v", next.Items)
	}
}

func TestNamespaceReadFallsBackToIndexStats(t *testing.T) {
	srv, _ := newMockAPI(t)
	defer srv.Close()
	p, sess := connect(t, srv.URL, false)
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(p.Routes())

	out, err := routes[rid("namespace.read")].Handle(plugin.NewRequestContext(context.Background(), plugin.User{}, sess,
		map[string]string{"index": podIndex, "namespace": "zeta"}, nil, nil))
	if err != nil {
		t.Fatalf("namespace.read on a pod index: %v", err)
	}
	item := out.(row)
	if item["record_count"] != int64(9999) || fmt.Sprint(item["index"]) != podIndex {
		t.Fatalf("unexpected namespace record: %#v", item)
	}
	_, err = routes[rid("namespace.read")].Handle(plugin.NewRequestContext(context.Background(), plugin.User{}, sess,
		map[string]string{"index": podIndex, "namespace": "absent"}, nil, nil))
	if !errors.Is(err, plugin.ErrNotFound) {
		t.Fatalf("a namespace missing from both sources must stay not-found, got %v", err)
	}
}

func TestVectorsListSurfacesAnUnsupportedListing(t *testing.T) {
	srv, _ := newMockAPI(t)
	defer srv.Close()
	p, sess := connect(t, srv.URL, false)
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(p.Routes())

	// Pinecone lists record ids for serverless indexes only and there is no
	// substitute, so the route reports the refusal rather than an empty page.
	_, err := routes[rid("vectors.list")].Handle(plugin.NewRequestContext(context.Background(), plugin.User{}, sess,
		map[string]string{"index": podIndex, "namespace": testNamespace}, url.Values{"limit": []string{"5"}}, nil))
	if !errors.Is(err, plugin.ErrNotFound) {
		t.Fatalf("want the upstream refusal, got %v", err)
	}
}

func TestNamespacesListSortsAcrossTheDataset(t *testing.T) {
	srv, _ := newMockAPI(t)
	defer srv.Close()
	p, sess := connect(t, srv.URL, false)
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(p.Routes())

	page := listNamespaces(t, routes, sess, url.Values{"limit": []string{"2"}, "sort": []string{"-record_count"}})
	if len(page.Items) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(page.Items))
	}
	if fmt.Sprint(page.Items[0]["name"]) != "zeta" {
		t.Fatalf("a descending sort must span every namespace page, got %#v", page.Items[0])
	}
}

func TestIndexesListSortsOnStatsAcrossTheDataset(t *testing.T) {
	srv, mock := newMockAPI(t)
	defer srv.Close()
	p, sess := connect(t, srv.URL, false)
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(p.Routes())

	out, err := routes[rid("indexes.list")].Handle(plugin.NewRequestContext(context.Background(), plugin.User{}, sess, nil,
		url.Values{"limit": []string{"1"}, "sort": []string{"-vectors"}}, nil))
	if err != nil {
		t.Fatalf("indexes.list: %v", err)
	}
	page := out.(pagedResult)
	if len(page.Items) != 1 || fmt.Sprint(page.Items[0]["name"]) != "archive" {
		t.Fatalf("stats sort must describe every index before paging: %#v", page.Items)
	}
	if page.Total == nil || *page.Total != len(mockIndexes) {
		t.Fatalf("expected an authoritative total of %d, got %#v", len(mockIndexes), page.Total)
	}
	mock.mu.Lock()
	defer mock.mu.Unlock()
	if mock.statsCalls != len(mockIndexes) {
		t.Fatalf("a stats sort must describe every index, got %d calls", mock.statsCalls)
	}
}

func TestIndexesListFillsStatsForThePageOnly(t *testing.T) {
	srv, mock := newMockAPI(t)
	defer srv.Close()
	p, sess := connect(t, srv.URL, false)
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(p.Routes())

	out, err := routes[rid("indexes.list")].Handle(plugin.NewRequestContext(context.Background(), plugin.User{}, sess, nil,
		url.Values{"limit": []string{"1"}}, nil))
	if err != nil {
		t.Fatalf("indexes.list: %v", err)
	}
	page := out.(pagedResult)
	if len(page.Items) != 1 || page.Items[0]["vectors"] == nil {
		t.Fatalf("the page row was not described: %#v", page.Items)
	}
	mock.mu.Lock()
	defer mock.mu.Unlock()
	if mock.statsCalls != 1 {
		t.Fatalf("a plain listing must describe only the page, got %d calls", mock.statsCalls)
	}
}

func TestIndexRowMapsTheControlPlaneShape(t *testing.T) {
	var idx indexInfo
	raw := `{"name":"docs","dimension":1536,"metric":"cosine","host":"docs-abc.svc.pinecone.io","vector_type":"dense",
		"deletion_protection":"enabled","tags":{"env":"prod"},"status":{"ready":true,"state":"Ready"},
		"spec":{"pod":{"environment":"us-east-1-aws","pod_type":"p1.x2","pods":2,"replicas":2,"shards":1}}}`
	if err := json.Unmarshal([]byte(raw), &idx); err != nil {
		t.Fatal(err)
	}
	item := idx.row()
	for key, want := range map[string]any{
		"name": "docs", "dimension": 1536, "metric": "cosine", "deployment": "pod",
		"location": "us-east-1-aws", "state": "Ready", "ready": true,
		"deletion_protection": "enabled", "pod_type": "p1.x2", "replicas": 2,
	} {
		if got := item[key]; got != want {
			t.Errorf("row[%q] = %#v, want %#v", key, got, want)
		}
	}
}

func TestCursorRoundTripsAndToleratesNumericOffsets(t *testing.T) {
	encoded := encodeCursor(cursor{Token: "tok-9", Skip: 4})
	if encoded == "" || strings.Contains(encoded, "tok-9") {
		t.Fatalf("the cursor must be opaque, got %q", encoded)
	}
	decoded, err := decodeCursor(encoded)
	if err != nil || decoded.Token != "tok-9" || decoded.Skip != 4 {
		t.Fatalf("round trip failed: %#v (%v)", decoded, err)
	}
	numeric, err := decodeCursor("120")
	if err != nil || numeric.Skip != 120 || numeric.Token != "" {
		t.Fatalf("the grid's numeric offset fallback must decode, got %#v (%v)", numeric, err)
	}
	if empty, err := decodeCursor(""); err != nil || empty != (cursor{}) {
		t.Fatalf("an empty cursor must decode to the start: %#v (%v)", empty, err)
	}
	if _, err := decodeCursor("not a cursor!!"); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("want ErrInvalidInput for a corrupt cursor, got %v", err)
	}
	if encodeCursor(cursor{}) != "" {
		t.Fatal("the start position must encode to an empty cursor")
	}
}

func TestStreamPageStopsAtTheScanCapWithARealCursor(t *testing.T) {
	// One matching row far past the cap: the scan must stop, hand back a real
	// continuation, and never load the whole listing into memory.
	total := scanLimit * 4
	fetch := func(_ context.Context, token string, limit int) ([]row, string, error) {
		start := 0
		if token != "" {
			start, _ = strconv.Atoi(token)
		}
		rows := make([]row, 0, limit)
		for i := start; i < min(start+limit, total); i++ {
			rows = append(rows, row{"id": strconv.Itoa(i)})
		}
		next := ""
		if start+limit < total {
			next = strconv.Itoa(start + limit)
		}
		return rows, next, nil
	}
	keep := func(item row) bool { return item["id"] == strconv.Itoa(total-1) }
	page, truncated, err := streamPage(context.Background(), plugin.PageRequest{Limit: 10}, fetch, keep)
	if err != nil {
		t.Fatal(err)
	}
	if !truncated {
		t.Fatal("hitting the scan cap must be reported")
	}
	if page.NextCursor == "" {
		t.Fatal("the cap must still hand back a reachable continuation")
	}
	// Walking the continuation eventually reaches the row past the cap.
	found, guard := false, 0
	for cur := page.NextCursor; cur != "" && !found; guard++ {
		if guard > 10 {
			t.Fatal("the continuation never converged")
		}
		next, _, err := streamPage(context.Background(), plugin.PageRequest{Limit: 10, Cursor: cur}, fetch, keep)
		if err != nil {
			t.Fatal(err)
		}
		found = len(next.Items) > 0
		cur = next.NextCursor
	}
	if !found {
		t.Fatal("data past the scan cap must stay reachable")
	}
}

func TestVectorsListHonorsADeepNumericOffset(t *testing.T) {
	srv, mock := newMockAPI(t)
	defer srv.Close()
	p, sess := connect(t, srv.URL, false)
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(p.Routes())

	// 120 spans more than one server page, so the fallback offset only lands on
	// the right row if the scan carries the remainder across pages.
	page := listVectors(t, routes, sess, url.Values{"limit": []string{"3"}, "cursor": []string{"120"}})
	if len(page.Items) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(page.Items))
	}
	if got := fmt.Sprint(page.Items[0]["id"]); got != mock.vectorIDs[120] {
		t.Fatalf("numeric offset landed on %q, want %q", got, mock.vectorIDs[120])
	}
}

func TestPageRowsOwnsItsSliceAndReportsTotals(t *testing.T) {
	rows := []row{{"id": "c"}, {"id": "a"}, {"id": "b"}}
	page, err := pageRows(plugin.PageRequest{Limit: 2, Sort: []plugin.SortKey{{Field: "id"}}}, rows, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 2 || page.Items[0]["id"] != "a" {
		t.Fatalf("unexpected page: %#v", page.Items)
	}
	if page.Total == nil || *page.Total != 3 {
		t.Fatalf("a fully materialized set has an authoritative total: %#v", page.Total)
	}
	if page.NextCursor == "" {
		t.Fatal("expected a continuation cursor")
	}
	truncated, err := pageRows(plugin.PageRequest{Limit: 2}, rows, true)
	if err != nil {
		t.Fatal(err)
	}
	if truncated.Total != nil {
		t.Fatal("a truncated scan must not claim a total")
	}
}

func TestQueryBodyAcceptsTheQueryEditorFrame(t *testing.T) {
	body, err := queryBody(map[string]any{
		"query":   `{"vector":[0.1,0.2,0.3],"topK":5,"filter":{"genre":{"$eq":"drama"}}}`,
		"confirm": false,
	}, testNamespace)
	if err != nil {
		t.Fatalf("queryBody: %v", err)
	}
	if body["topK"] != 5 {
		t.Fatalf("topK was %#v", body["topK"])
	}
	if body["namespace"] != testNamespace {
		t.Fatalf("the connection namespace must scope the query, got %#v", body["namespace"])
	}
	if body["includeMetadata"] != true {
		t.Fatalf("metadata should be requested by default, got %#v", body["includeMetadata"])
	}
	if _, ok := body["confirm"]; ok {
		t.Fatal("the editor's control keys must not reach Pinecone")
	}
	if body["filter"] == nil {
		t.Fatal("the metadata filter was dropped")
	}
}

func TestQueryBodyRejectsIncompleteQueries(t *testing.T) {
	for name, frame := range map[string]any{
		"no anchor":     map[string]any{"topK": float64(5)},
		"blank id":      map[string]any{"query": `{"id":"  ","topK":10}`},
		"empty vector":  map[string]any{"query": `{"vector":[],"topK":10}`},
		"empty sparse":  map[string]any{"query": `{"sparseVector":{"indices":[],"values":[]}}`},
		"bad json":      map[string]any{"query": "{"},
		"bad topK":      map[string]any{"id": "v1", "topK": "many"},
		"zero topK":     map[string]any{"id": "v1", "topK": float64(0)},
		"huge topK":     map[string]any{"id": "v1", "topK": float64(maxTopK + 1)},
		"not object":    []any{1, 2},
		"bad namespace": map[string]any{"id": "v1", "namespace": "a/b"},
	} {
		if _, err := queryBody(frame, testNamespace); !errors.Is(err, plugin.ErrInvalidInput) {
			t.Errorf("%s: want ErrInvalidInput, got %v", name, err)
		}
	}
}

func TestQueryTableShape(t *testing.T) {
	result := queryResult{Matches: []queryMatch{{
		vectorRecord: vectorRecord{ID: "v1", Values: []float64{0.1, 0.2}, Metadata: map[string]any{"title": "Ada"}},
		Score:        0.987654321,
	}}, Namespace: testNamespace, Usage: &usage{ReadUnits: 6}}
	table := queryTable(result, testIndex, testNamespace)
	if table["rowCount"] != 1 || table["readUnits"] != 6 {
		t.Fatalf("unexpected table: %#v", table)
	}
	rows := table["rows"].([][]any)
	if rows[0][0] != "v1" || rows[0][1] != 0.987654 || rows[0][3] != `{"title":"Ada"}` {
		t.Fatalf("unexpected row: %#v", rows[0])
	}
}

func TestUpsertPayloadAcceptsCodeEditorEnvelopes(t *testing.T) {
	document := `{"vectors":[{"id":"v1","values":[0.1,0.2,0.3],"metadata":{"k":"v"}}],"namespace":"docs-ns"}`
	for name, body := range map[string][]byte{
		"raw":         []byte(document),
		"save body":   []byte(`{"vectors":` + document + `}`),
		"editor text": mustJSONBytes(map[string]any{"content": document}),
	} {
		payload, err := upsertPayload(body)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(payload.Vectors) != 1 || payload.Vectors[0].ID != "v1" || payload.Namespace != "docs-ns" {
			t.Fatalf("%s: unexpected payload %#v", name, payload)
		}
	}
	if _, err := upsertPayload([]byte("nope")); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("want ErrInvalidInput, got %v", err)
	}
}

func TestUpsertRejectsRecordsWithoutValues(t *testing.T) {
	srv, _ := newMockAPI(t)
	defer srv.Close()
	p, sess := connect(t, srv.URL, false)
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(p.Routes())
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess,
		map[string]string{"index": testIndex}, nil, []byte(`{"vectors":[{"id":"v1"}]}`))
	if _, err := routes[rid("vectors.upsert")].Handle(rc); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("want ErrInvalidInput, got %v", err)
	}
}

func TestIndexCreateRejectsUnsupportedShapes(t *testing.T) {
	srv, _ := newMockAPI(t)
	defer srv.Close()
	p, sess := connect(t, srv.URL, false)
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(p.Routes())

	for name, body := range map[string]map[string]any{
		"sparse on pods":       {"name": "sp", "deployment": "pod", "vector_type": "sparse", "environment": "e", "pod_type": "p1.x1"},
		"dense no dimension":   {"name": "dn", "deployment": "serverless", "vector_type": "dense", "cloud": "aws", "region": "us-east-1"},
		"serverless no region": {"name": "nr", "deployment": "serverless", "vector_type": "dense", "dimension": 3, "cloud": "aws"},
		"unknown vector type":  {"name": "uv", "deployment": "serverless", "vector_type": "hybrid", "cloud": "aws", "region": "us-east-1"},
		"unknown deployment":   {"name": "ud", "deployment": "moon", "vector_type": "dense", "dimension": 3},
	} {
		rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess, nil, nil, mustJSON(t, body))
		if _, err := routes[rid("index.create")].Handle(rc); !errors.Is(err, plugin.ErrInvalidInput) {
			t.Errorf("%s: want ErrInvalidInput, got %v", name, err)
		}
	}
}

func TestUpsertSplitsAtPineconesPerRequestCeiling(t *testing.T) {
	srv, mock := newMockAPI(t)
	defer srv.Close()
	p, sess := connect(t, srv.URL, false)
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(p.Routes())

	vectors := make([]any, 0, writeBatchSize+7)
	for i := range writeBatchSize + 7 {
		vectors = append(vectors, map[string]any{"id": fmt.Sprintf("b%04d", i), "values": []float64{0.1, 0.2, 0.3}})
	}
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess,
		map[string]string{"index": testIndex, "namespace": testNamespace}, nil,
		mustJSON(t, map[string]any{"vectors": vectors}))
	out, err := routes[rid("vectors.upsert")].Handle(rc)
	if err != nil {
		t.Fatalf("vectors.upsert: %v", err)
	}
	if got := out.(row)["upserted"]; got != writeBatchSize+7 {
		t.Fatalf("the response must sum every batch, got %#v", got)
	}
	mock.mu.Lock()
	defer mock.mu.Unlock()
	if len(mock.upsertCalls) != 2 || mock.upsertCalls[0] != writeBatchSize || mock.upsertCalls[1] != 7 {
		t.Fatalf("expected batches of %d then 7, got %v", writeBatchSize, mock.upsertCalls)
	}
}

func TestVectorsDeleteSplitsIDsAtPineconesCeiling(t *testing.T) {
	srv, mock := newMockAPI(t)
	defer srv.Close()
	p, sess := connect(t, srv.URL, false)
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(p.Routes())

	ids := make([]string, 0, writeBatchSize+3)
	for i := range writeBatchSize + 3 {
		ids = append(ids, fmt.Sprintf("d%04d", i))
	}
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess,
		map[string]string{"index": testIndex, "namespace": testNamespace}, nil,
		mustJSON(t, map[string]any{"ids": ids}))
	out, err := routes[rid("vectors.delete")].Handle(rc)
	if err != nil {
		t.Fatalf("vectors.delete: %v", err)
	}
	if got := out.(row)["deleted"]; got != writeBatchSize+3 {
		t.Fatalf("unexpected deleted count %#v", got)
	}
	mock.mu.Lock()
	defer mock.mu.Unlock()
	if len(mock.deleteCalls) != 2 || mock.deleteCalls[0] != writeBatchSize {
		t.Fatalf("expected batched deletes, got %v", mock.deleteCalls)
	}
}

func TestVectorsDeleteByFilterDoesNotClaimACount(t *testing.T) {
	srv, _ := newMockAPI(t)
	defer srv.Close()
	p, sess := connect(t, srv.URL, false)
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(p.Routes())

	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess,
		map[string]string{"index": testIndex, "namespace": testNamespace}, nil,
		[]byte(`{"filter":{"genre":{"$eq":"drama"}}}`))
	out, err := routes[rid("vectors.delete")].Handle(rc)
	if err != nil {
		t.Fatalf("vectors.delete: %v", err)
	}
	item := out.(row)
	if _, ok := item["deleted"]; ok {
		t.Fatalf("a filtered delete cannot report a count: %#v", item)
	}
	if item["target"] != "filter" {
		t.Fatalf("unexpected delete result: %#v", item)
	}
}

func TestUpsertOmitsAbsentVectorMembers(t *testing.T) {
	// A sparse-only record must not send "values": null; Pinecone rejects it.
	data, err := json.Marshal(vectorRecord{ID: "s1", SparseValues: &sparseValues{Indices: []int64{3}, Values: []float64{0.5}}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), `"values":null`) || strings.Contains(string(data), `"metadata":null`) {
		t.Fatalf("absent members must be omitted, got %s", data)
	}
	dense, err := json.Marshal(vectorRecord{ID: "d1", Values: []float64{0.1}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(dense), "sparseValues") {
		t.Fatalf("a dense record must not carry a null sparseValues: %s", dense)
	}
}

func TestVectorsListHydratesOnlyThePageWhenSortingOnAListedColumn(t *testing.T) {
	srv, mock := newMockAPI(t)
	defer srv.Close()
	p, sess := connect(t, srv.URL, false)
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(p.Routes())

	page := listVectors(t, routes, sess, url.Values{"limit": []string{"5"}, "sort": []string{"-id"}})
	if len(page.Items) != 5 || page.Items[0]["dimension"] != 3 {
		t.Fatalf("the page must still be hydrated: %#v", page.Items)
	}
	mock.mu.Lock()
	byID := mock.fetchCalls
	mock.fetchCalls = 0
	mock.mu.Unlock()
	if byID != 1 {
		t.Fatalf("sorting on a listed column must fetch the page only, got %d fetches", byID)
	}

	// dimension only exists after a fetch, so ordering by it has to hydrate the
	// whole scan before it can page.
	listVectors(t, routes, sess, url.Values{"limit": []string{"5"}, "sort": []string{"-dimension"}})
	mock.mu.Lock()
	defer mock.mu.Unlock()
	if mock.fetchCalls <= 1 {
		t.Fatalf("ordering by a fetched column must hydrate the scan, got %d fetches", mock.fetchCalls)
	}
}

func TestQueryAuditDescribesTheSearchWithoutLeakingIt(t *testing.T) {
	body, err := queryBody(map[string]any{
		"query": `{"vector":[0.1,0.2],"topK":7,"filter":{"genre":{"$eq":"drama"}},"namespace":"other"}`,
	}, testNamespace)
	if err != nil {
		t.Fatal(err)
	}
	params := queryAudit(testIndex, testNamespace, body)
	for key, want := range map[string]string{
		"index": testIndex, "namespace": "other", "topK": "7", "anchor": "vector", "filtered": "true",
	} {
		if params[key] != want {
			t.Errorf("audit[%q] = %q, want %q", key, params[key], want)
		}
	}
	for _, value := range params {
		if strings.Contains(value, "0.1") || strings.Contains(value, "drama") {
			t.Fatalf("the audit trail must not carry query payloads: %#v", params)
		}
	}
	byID, err := queryBody(map[string]any{"id": "v001"}, testNamespace)
	if err != nil {
		t.Fatal(err)
	}
	if got := queryAudit(testIndex, testNamespace, byID); got["anchor"] != "id" || got["filtered"] != "false" {
		t.Fatalf("unexpected audit params: %#v", got)
	}
	if got := queryAudit(testIndex, testNamespace, nil); got["namespace"] != testNamespace {
		t.Fatalf("a rejected frame must still audit its scope: %#v", got)
	}
}

func TestIndexStatsReadExposesOnlyReportedFields(t *testing.T) {
	srv, _ := newMockAPI(t)
	defer srv.Close()
	p, sess := connect(t, srv.URL, false)
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(p.Routes())

	out, err := routes[rid("index.stats")].Handle(plugin.NewRequestContext(context.Background(), plugin.User{}, sess,
		map[string]string{"index": testIndex}, nil, nil))
	if err != nil {
		t.Fatalf("index.stats: %v", err)
	}
	item := out.(row)
	// Pinecone reports one fullness figure; the others were never in the API.
	for _, key := range []string{"memory_fullness", "storage_fullness"} {
		if _, ok := item[key]; ok {
			t.Errorf("%q is not reported by describe_index_stats", key)
		}
	}
	if item["fullness"] != 42.0 || item["namespaces"] != len(mockNamespaces) {
		t.Fatalf("unexpected stats: %#v", item)
	}
	counts, ok := item["namespace_counts"].([]row)
	if !ok || len(counts) != len(mockNamespaces) || fmt.Sprint(counts[0]["namespace"]) != "alpha" {
		t.Fatalf("per-namespace counts must be listed in name order: %#v", item["namespace_counts"])
	}
}

func TestVectorsDeleteNeedsATarget(t *testing.T) {
	srv, _ := newMockAPI(t)
	defer srv.Close()
	p, sess := connect(t, srv.URL, false)
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(p.Routes())
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess,
		map[string]string{"index": testIndex}, nil, []byte(`{}`))
	if _, err := routes[rid("vectors.delete")].Handle(rc); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("want ErrInvalidInput, got %v", err)
	}
}

func TestVectorReadReportsAMissingRecord(t *testing.T) {
	srv, _ := newMockAPI(t)
	defer srv.Close()
	p, sess := connect(t, srv.URL, false)
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(p.Routes())
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess,
		map[string]string{"index": testIndex, "namespace": testNamespace, "vector": "missing"}, nil, nil)
	if _, err := routes[rid("vector.read")].Handle(rc); !errors.Is(err, plugin.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestNeighborsDropTheAnchorAndRank(t *testing.T) {
	srv, _ := newMockAPI(t)
	defer srv.Close()
	p, sess := connect(t, srv.URL, false)
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(p.Routes())
	out, err := routes[rid("vector.neighbors")].Handle(plugin.NewRequestContext(context.Background(), plugin.User{}, sess,
		map[string]string{"index": testIndex, "namespace": testNamespace, "vector": "v001"}, url.Values{"limit": []string{"3"}}, nil))
	if err != nil {
		t.Fatalf("neighbors: %v", err)
	}
	page := out.(plugin.Page[row])
	if len(page.Items) != 3 {
		t.Fatalf("expected 3 neighbors, got %d", len(page.Items))
	}
	for i, item := range page.Items {
		if item["id"] == "v001" {
			t.Fatal("the anchor must not be returned as its own neighbor")
		}
		if item["rank"] != i+1 {
			t.Fatalf("rank %d was %#v", i+1, item["rank"])
		}
	}
}

func TestAPIErrorMapsBothPineconeShapes(t *testing.T) {
	cases := []struct {
		status int
		body   string
		want   error
		text   string
	}{
		{404, `{"error":{"code":"NOT_FOUND","message":"Index not found"},"status":404}`, plugin.ErrNotFound, "Index not found"},
		{409, `{"error":{"code":"ALREADY_EXISTS","message":"Index already exists"},"status":409}`, plugin.ErrConflict, "Index already exists"},
		{403, `{"error":{"code":"QUOTA_EXCEEDED","message":"Max serverless indexes"},"status":403}`, plugin.ErrForbidden, "Max serverless indexes"},
		{401, `{"error":{"code":"UNAUTHORIZED","message":"Invalid API key"},"status":401}`, plugin.ErrUnauthorized, "Invalid API key"},
		{400, `{"code":3,"message":"topK must be positive"}`, plugin.ErrInvalidInput, "topK must be positive"},
		{503, `upstream down`, plugin.ErrUnavailable, "upstream down"},
	}
	for _, tc := range cases {
		err := apiError(tc.status, []byte(tc.body))
		if !errors.Is(err, tc.want) {
			t.Errorf("status %d: want %v, got %v", tc.status, tc.want, err)
		}
		if !strings.Contains(err.Error(), tc.text) {
			t.Errorf("status %d: message %q lost %q", tc.status, err, tc.text)
		}
	}
}

func TestDataPlaneURLFollowsTheControlPlaneScheme(t *testing.T) {
	idx := indexInfo{Host: "docs-abc.svc.pinecone.io", PrivateHost: "docs-abc.private.pinecone.io"}
	if got := dataPlaneURL(Options{Scheme: "https"}, idx); got != "https://docs-abc.svc.pinecone.io" {
		t.Fatalf("got %q", got)
	}
	if got := dataPlaneURL(Options{Scheme: "http"}, idx); got != "http://docs-abc.svc.pinecone.io" {
		t.Fatalf("a plaintext control plane must not be silently upgraded: %q", got)
	}
	if got := dataPlaneURL(Options{Scheme: "https", PrivateNet: true}, idx); got != "https://docs-abc.private.pinecone.io" {
		t.Fatalf("got %q", got)
	}
	if got := dataPlaneURL(Options{Scheme: "https"}, indexInfo{Host: "https://already.example/"}); got != "https://already.example" {
		t.Fatalf("got %q", got)
	}
}

func TestValidateNameRejectsPathTricks(t *testing.T) {
	for _, bad := range []string{"", "..", "a/b", "UPPER", strings.Repeat("a", maxNameLength+1), "with space"} {
		if err := validateName("index", bad); !errors.Is(err, plugin.ErrInvalidInput) {
			t.Errorf("name %q should be rejected", bad)
		}
	}
	if err := validateName("index", "docs-2"); err != nil {
		t.Errorf("docs-2 should be accepted: %v", err)
	}
	for _, bad := range []string{"a/b", "a\\b", "tab\there"} {
		if err := validateNamespace(bad); !errors.Is(err, plugin.ErrInvalidInput) {
			t.Errorf("namespace %q should be rejected", bad)
		}
	}
	if err := validateNamespace(defaultNamespace); err != nil {
		t.Errorf("%s should be accepted: %v", defaultNamespace, err)
	}
}

func TestParseOptionsDefaults(t *testing.T) {
	opts, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{
		"endpoint": "https://api.pinecone.io/", "auth": "api_key", "api_key": testAPIKey,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if opts.Endpoint != "https://api.pinecone.io" || opts.Scheme != "https" {
		t.Fatalf("unexpected endpoint: %#v", opts)
	}
	if opts.Namespace != defaultNamespace || opts.APIVersion != defaultAPIVer {
		t.Fatalf("unexpected defaults: %#v", opts)
	}
	if !opts.ReadOnly || opts.PageLimit != defaultPageLimit {
		t.Fatalf("safety defaults changed: %#v", opts)
	}
	if _, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{"endpoint": "not-a-url", "auth": "api_key", "api_key": "k"}}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("want ErrInvalidInput, got %v", err)
	}
	if _, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{"auth": "nope"}}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("want ErrInvalidInput, got %v", err)
	}
}

func connect(t *testing.T, endpoint string, readOnly bool) (plugin.Plugin, plugin.Session) {
	t.Helper()
	p := New()
	sess, err := p.Connect(context.Background(), plugin.ConnectConfig{
		Config: map[string]any{
			"endpoint":  endpoint,
			"auth":      "api_key",
			"api_key":   testAPIKey,
			"namespace": testNamespace,
			"read_only": readOnly,
		},
		Net: plugintest.DirectTransport(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return p, sess
}

func listVectors(t *testing.T, routes map[string]plugin.Route, sess plugin.Session, query url.Values) plugin.Page[row] {
	t.Helper()
	out, err := routes[rid("vectors.list")].Handle(plugin.NewRequestContext(context.Background(), plugin.User{}, sess,
		map[string]string{"index": testIndex, "namespace": testNamespace}, query, nil))
	if err != nil {
		t.Fatalf("vectors.list: %v", err)
	}
	return out.(pagedResult).Page
}

func listNamespaces(t *testing.T, routes map[string]plugin.Route, sess plugin.Session, query url.Values) plugin.Page[row] {
	t.Helper()
	out, err := routes[rid("namespaces.list")].Handle(plugin.NewRequestContext(context.Background(), plugin.User{}, sess,
		map[string]string{"index": testIndex}, query, nil))
	if err != nil {
		t.Fatalf("namespaces.list: %v", err)
	}
	return out.(pagedResult).Page
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func mustJSONBytes(value any) []byte {
	data, _ := json.Marshal(value)
	return data
}

// mockAPI serves the control plane and every index's data plane from one
// httptest server; describe_index hands back that server's own host, which is
// exactly how a plugin discovers a real data-plane endpoint.
type mockAPI struct {
	mu   sync.Mutex
	host string

	vectorIDs          []string
	statsCalls         int
	upserted           int
	upsertCalls        []int
	deleteCalls        []int
	fetchCalls         int
	namespaceListCalls int
	listNamespaces     []string
	lastKey            string
	lastVersion        string
}

func newMockAPI(t *testing.T) (*httptest.Server, *mockAPI) {
	t.Helper()
	mock := &mockAPI{vectorIDs: make([]string, 0, testVectors)}
	for i := range testVectors {
		mock.vectorIDs = append(mock.vectorIDs, fmt.Sprintf("v%03d", i))
	}
	srv := httptest.NewServer(http.HandlerFunc(mock.serve))
	t.Cleanup(srv.Close)
	mock.host = strings.TrimPrefix(srv.URL, "http://")
	return srv, mock
}

// podIndex is served as a pod-based index, so the mock refuses the
// serverless-only data-plane listings for it exactly as Pinecone does.
const podIndex = "legacy"

var mockIndexes = []struct {
	name    string
	metric  string
	vectors int64
}{
	{"docs", "cosine", 250},
	{"archive", "dotproduct", 9000},
	{"scratch", "euclidean", 12},
	{podIndex, "cosine", 40},
}

var mockNamespaces = []struct {
	name    string
	records int64
}{
	{"docs-ns", 250},
	{"alpha", 10},
	{"scratch", 1},
	{"zeta", 9999},
}

func (m *mockAPI) serve(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	m.lastKey = r.Header.Get(apiKeyHeader)
	m.lastVersion = r.Header.Get(apiVersionHeader)
	m.mu.Unlock()
	if r.Header.Get(apiKeyHeader) != testAPIKey {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"code":"UNAUTHORIZED","message":"Invalid API key"},"status":401}`))
		return
	}
	if r.Header.Get(apiVersionHeader) == "" {
		http.Error(w, "missing api version", http.StatusBadRequest)
		return
	}
	path, index := splitIndexPath(r.URL.Path)
	switch {
	case path == "/indexes" && r.Method == http.MethodGet:
		m.writeIndexes(w)
	case path == "/indexes" && r.Method == http.MethodPost:
		m.writeIndex(w, "new-index", "cosine")
	case strings.HasPrefix(path, "/indexes/"):
		name := strings.TrimPrefix(path, "/indexes/")
		switch r.Method {
		case http.MethodDelete:
			_, _ = w.Write([]byte(`{}`))
		default:
			m.writeIndex(w, name, metricFor(name))
		}
	case path == "/collections" && r.Method == http.MethodGet:
		_, _ = w.Write([]byte(`{"collections":[
			{"name":"docs-backup","size":1048576,"status":"Ready","dimension":3,"vector_count":250,"environment":"us-east-1-aws"},
			{"name":"cold-backup","size":2048,"status":"Initializing","dimension":3,"vector_count":8,"environment":"us-east-1-aws"}]}`))
	case path == "/collections" && r.Method == http.MethodPost:
		_, _ = w.Write([]byte(`{"name":"docs-copy","status":"Initializing","dimension":3,"vector_count":0,"environment":"us-east-1-aws"}`))
	case strings.HasPrefix(path, "/collections/"):
		if r.Method == http.MethodDelete {
			_, _ = w.Write([]byte(`{}`))
			return
		}
		name := strings.TrimPrefix(path, "/collections/")
		_, _ = fmt.Fprintf(w, `{"name":%q,"size":1048576,"status":"Ready","dimension":3,"vector_count":250,"environment":"us-east-1-aws"}`, name)
	case path == "/describe_index_stats":
		m.writeStats(w, index)
	case path == "/namespaces" && r.Method == http.MethodGet:
		m.writeNamespaces(w, r, index)
	case strings.HasPrefix(path, "/namespaces/"):
		if r.Method == http.MethodDelete {
			_, _ = w.Write([]byte(`{}`))
			return
		}
		if index == podIndex {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"code":5,"message":"describe namespace is not supported for pod-based indexes"}`))
			return
		}
		name := strings.TrimPrefix(path, "/namespaces/")
		_, _ = fmt.Fprintf(w, `{"name":%q,"record_count":250}`, name)
	case path == "/vectors/list":
		m.writeVectorIDs(w, r, index)
	case path == "/vectors/fetch":
		m.writeFetch(w, r)
	case path == "/vectors/upsert":
		m.writeUpsert(w, r)
	case path == "/vectors/delete":
		m.writeDelete(w, r)
	case path == "/query":
		m.writeQuery(w, r)
	default:
		http.NotFound(w, r)
	}
}

func metricFor(name string) string {
	for _, idx := range mockIndexes {
		if idx.name == name {
			return idx.metric
		}
	}
	return "cosine"
}

func (m *mockAPI) writeIndexes(w http.ResponseWriter) {
	parts := make([]string, 0, len(mockIndexes))
	for _, idx := range mockIndexes {
		parts = append(parts, m.indexJSON(idx.name, idx.metric))
	}
	_, _ = fmt.Fprintf(w, `{"indexes":[%s]}`, strings.Join(parts, ","))
}

func (m *mockAPI) writeIndex(w http.ResponseWriter, name, metric string) {
	_, _ = w.Write([]byte(m.indexJSON(name, metric)))
}

// indexJSON hands back a scheme-less, per-index data-plane host, exactly as the
// control plane does; every index therefore gets its own base URL.
func (m *mockAPI) indexJSON(name, metric string) string {
	spec := `{"serverless":{"cloud":"aws","region":"us-east-1"}}`
	if name == podIndex {
		spec = `{"pod":{"environment":"us-east-1-aws","pod_type":"p1.x1","pods":1,"replicas":1,"shards":1}}`
	}
	return fmt.Sprintf(`{"name":%q,"dimension":3,"metric":%q,"host":"%s/i/%s","vector_type":"dense",
		"deletion_protection":"disabled","tags":{"env":"test"},"status":{"ready":true,"state":"Ready"},
		"spec":%s}`, name, metric, m.host, name, spec)
}

// splitIndexPath peels the per-index data-plane prefix off a request path.
func splitIndexPath(path string) (string, string) {
	if !strings.HasPrefix(path, "/i/") {
		return path, ""
	}
	rest := strings.TrimPrefix(path, "/i/")
	name, tail, ok := strings.Cut(rest, "/")
	if !ok {
		return "/", name
	}
	return "/" + tail, name
}

// writeStats mirrors describe_index_stats, which reports every namespace for
// pod and serverless indexes alike and carries no memory/storage fullness.
func (m *mockAPI) writeStats(w http.ResponseWriter, index string) {
	m.mu.Lock()
	m.statsCalls++
	m.mu.Unlock()
	var vectors int64
	for _, idx := range mockIndexes {
		if idx.name == index {
			vectors = idx.vectors
		}
	}
	parts := make([]string, 0, len(mockNamespaces))
	for _, ns := range mockNamespaces {
		parts = append(parts, fmt.Sprintf(`%q:{"vectorCount":%d}`, ns.name, ns.records))
	}
	_, _ = fmt.Fprintf(w, `{"namespaces":{%s},"dimension":3,"indexFullness":0.42,
		"totalVectorCount":%d,"vectorType":"dense","metric":"cosine"}`, strings.Join(parts, ","), vectors)
}

// writeNamespaces mirrors the real listing: name + record_count per row, an
// opaque continuation token, and no total. It is a serverless-only endpoint, so
// it 404s for the index the mock treats as pod-based.
func (m *mockAPI) writeNamespaces(w http.ResponseWriter, r *http.Request, index string) {
	m.mu.Lock()
	m.namespaceListCalls++
	m.mu.Unlock()
	if index == podIndex {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"code":5,"message":"list namespaces is not supported for pod-based indexes"}`))
		return
	}
	limit := queryInt(r, "limit", 100)
	start := queryInt(r, "paginationToken", 0)
	end := min(start+limit, len(mockNamespaces))
	parts := make([]string, 0, end-start)
	for _, ns := range mockNamespaces[start:end] {
		parts = append(parts, fmt.Sprintf(`{"name":%q,"record_count":%d}`, ns.name, ns.records))
	}
	next := ""
	if end < len(mockNamespaces) {
		next = strconv.Itoa(end)
	}
	_, _ = fmt.Fprintf(w, `{"namespaces":[%s],"pagination":{"next":%q}}`, strings.Join(parts, ","), next)
}

func (m *mockAPI) writeUpsert(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Vectors []json.RawMessage `json:"vectors"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	m.mu.Lock()
	m.upserted += len(body.Vectors)
	m.upsertCalls = append(m.upsertCalls, len(body.Vectors))
	m.mu.Unlock()
	_, _ = fmt.Fprintf(w, `{"upsertedCount":%d}`, len(body.Vectors))
}

func (m *mockAPI) writeDelete(w http.ResponseWriter, r *http.Request) {
	var body struct {
		IDs []string `json:"ids"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	m.mu.Lock()
	m.deleteCalls = append(m.deleteCalls, len(body.IDs))
	m.mu.Unlock()
	_, _ = w.Write([]byte(`{}`))
}

func (m *mockAPI) writeVectorIDs(w http.ResponseWriter, r *http.Request, index string) {
	m.mu.Lock()
	m.listNamespaces = append(m.listNamespaces, r.URL.Query().Get("namespace"))
	m.mu.Unlock()
	if index == podIndex {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"code":5,"message":"list is not supported for pod-based indexes"}`))
		return
	}
	limit := queryInt(r, "limit", 100)
	start := queryInt(r, "paginationToken", 0)
	end := min(start+limit, len(m.vectorIDs))
	parts := make([]string, 0, end-start)
	for _, id := range m.vectorIDs[start:end] {
		parts = append(parts, fmt.Sprintf(`{"id":%q}`, id))
	}
	next := ""
	if end < len(m.vectorIDs) {
		next = strconv.Itoa(end)
	}
	_, _ = fmt.Fprintf(w, `{"vectors":[%s],"pagination":{"next":%q},"namespace":%q,"usage":{"readUnits":1}}`,
		strings.Join(parts, ","), next, r.URL.Query().Get("namespace"))
}

func (m *mockAPI) writeFetch(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	m.fetchCalls++
	m.mu.Unlock()
	ids := r.URL.Query()["ids"]
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		slot := 0
		_, _ = fmt.Sscanf(id, "v%d", &slot)
		if !strings.HasPrefix(id, "v") || slot >= len(m.vectorIDs) {
			continue
		}
		parts = append(parts, fmt.Sprintf(`%q:{"id":%q,"values":[0.1,0.2,0.3],"metadata":{"slot":%d}}`, id, id, slot))
	}
	_, _ = fmt.Fprintf(w, `{"vectors":{%s},"namespace":%q,"usage":{"readUnits":1}}`,
		strings.Join(parts, ","), r.URL.Query().Get("namespace"))
}

func (m *mockAPI) writeQuery(w http.ResponseWriter, r *http.Request) {
	var body struct {
		TopK int `json:"topK"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if body.TopK <= 0 {
		body.TopK = defaultTopK
	}
	parts := make([]string, 0, body.TopK)
	for i := range min(body.TopK, len(m.vectorIDs)) {
		id := m.vectorIDs[i]
		parts = append(parts, fmt.Sprintf(`{"id":%q,"score":%v,"metadata":{"slot":%d}}`, id, 1.0-float64(i)/100, i))
	}
	_, _ = fmt.Fprintf(w, `{"matches":[%s],"namespace":%q,"usage":{"readUnits":5}}`,
		strings.Join(parts, ","), "docs-ns")
}

func queryInt(r *http.Request, key string, fallback int) int {
	if raw := r.URL.Query().Get(key); raw != "" {
		if value, err := strconv.Atoi(raw); err == nil {
			return value
		}
	}
	return fallback
}
