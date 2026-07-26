package chroma

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugin/canvas"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestManifestValidates(t *testing.T) {
	p := New()
	plugintest.ValidatePlugin(t, p)
	proj := plugintest.Projection(t, p)
	plugintest.ValidateProjectionPanelConfigs(t, proj)
	for _, kind := range []plugin.CredentialKind{plugin.CredentialKindAPIToken, plugin.CredentialKindBasicAuth} {
		if !plugintest.CredentialKindSupported(p.Manifest().Config, kind) {
			t.Fatalf("Chroma should accept stored %s credentials", kind)
		}
	}
}

func TestRouteContract(t *testing.T) {
	routes := plugintest.RouteMap(New().Routes())
	for id, route := range routes {
		if route.AuditEvent != id {
			t.Errorf("route %s: audit event %q must equal the route id", id, route.AuditEvent)
		}
		if !strings.HasPrefix(route.Permission, protocolName+".") {
			t.Errorf("route %s: permission %q must be namespaced", id, route.Permission)
		}
	}
	destructive := []string{rid("collection.delete"), rid("database.delete"), rid("records.delete"), rid("history.clear"), rid("queries.delete")}
	for _, id := range destructive {
		if routes[id].Risk != plugin.RiskDestructive {
			t.Errorf("route %s must be destructive, got %q", id, routes[id].Risk)
		}
	}
}

func TestConfigSchemaVisibility(t *testing.T) {
	schema := configSchema()
	cases := []struct {
		mode    string
		visible []string
		hidden  []string
	}{
		{"none", nil, []string{"token", "username", "password", tokenCredentialField, basicCredentialField}},
		{"token", []string{"token"}, []string{"username", tokenCredentialField}},
		{"basic", []string{"username", "password"}, []string{"token", basicCredentialField}},
		{"credential_token", []string{tokenCredentialField}, []string{"token", basicCredentialField}},
		{"credential_basic", []string{basicCredentialField}, []string{tokenCredentialField, "password"}},
	}
	allValues := func(mode string) map[string]any {
		values := map[string]any{}
		for _, group := range schema.Groups {
			for _, field := range group.Fields {
				values[field.Key] = "set"
			}
		}
		values["auth"] = mode
		return values
	}
	for _, tc := range cases {
		visible := schema.VisibleValues(allValues(tc.mode), nil)
		for _, key := range tc.visible {
			if _, ok := visible[key]; !ok {
				t.Errorf("auth %q: expected %q to be visible", tc.mode, key)
			}
		}
		for _, key := range tc.hidden {
			if _, ok := visible[key]; ok {
				t.Errorf("auth %q: expected %q to be hidden", tc.mode, key)
			}
		}
	}
	values := allValues("basic")
	values["tls_mode"] = "verify-ca"
	secrets := schema.VisibleSecretKeys(values, nil)
	if !slices.Contains(secrets, "password") || !slices.Contains(secrets, "ca_certificate") {
		t.Fatalf("expected password and ca_certificate to be visible secrets, got %v", secrets)
	}
	if slices.Contains(secrets, "token") {
		t.Fatalf("basic auth should not expose the token field: %v", secrets)
	}
	if defaults := schema.Defaults(); defaults["tenant"] != defaultTenant || defaults["read_only"] != true {
		t.Fatalf("unexpected schema defaults: %#v", defaults)
	}
}

func TestAuthHeaders(t *testing.T) {
	cases := []struct {
		name   string
		config map[string]any
		header string
		value  string
	}{
		{"token", map[string]any{"auth": "token", "token": "abc"}, "x-chroma-token", "abc"},
		{"bearer", map[string]any{"auth": "bearer", "token": "abc"}, "Authorization", "Bearer abc"},
		{"basic", map[string]any{"auth": "basic", "username": "u", "password": "p"}, "Authorization", "Basic dTpw"},
	}
	for _, tc := range cases {
		headers, err := authHeaders(plugin.ConnectConfig{Config: tc.config})
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if headers[tc.header] != tc.value {
			t.Fatalf("%s: got %q, want %q", tc.name, headers[tc.header], tc.value)
		}
	}
	if _, err := authHeaders(plugin.ConnectConfig{Config: map[string]any{"auth": "token"}}); err == nil {
		t.Fatal("expected a missing token to fail")
	}
	if _, err := authHeaders(plugin.ConnectConfig{Config: map[string]any{"auth": "wat"}}); err == nil {
		t.Fatal("expected an unknown auth mode to fail")
	}
}

func TestParseOptionsRejectsPathInjection(t *testing.T) {
	if _, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{"tenant": "../admin"}}); err == nil {
		t.Fatal("expected tenant path traversal to be rejected")
	}
	opts, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{"endpoint": "http://host:8000/", "sample_limit": 5}})
	if err != nil {
		t.Fatalf("parseOptions: %v", err)
	}
	if opts.Endpoint != "http://host:8000" || opts.Sample != minSample || !opts.ReadOnly {
		t.Fatalf("unexpected options: %#v", opts)
	}
}

// fakeChroma answers the subset of the v2 API the handlers use.
type fakeChroma struct {
	t        *testing.T
	requests []string
}

func (f *fakeChroma) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.requests = append(f.requests, r.Method+" "+r.URL.Path)
	write := func(body string) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}
	const collectionJSON = `{"id":"11111111-2222-3333-4444-555555555555","name":"docs","tenant":"default_tenant","database":"default_database",` +
		`"dimension":3,"metadata":{"kind":"demo"},"log_position":7,"version":2,` +
		`"configuration_json":{"hnsw":{"space":"cosine","ef_construction":100,"ef_search":64,"max_neighbors":16},"spann":null,"embedding_function":null},` +
		`"schema":{"defaults":{"string":{"fts_index":{"enabled":false,"config":{}}}},"keys":{"#embedding":{"float_list":{"vector_index":{"enabled":true,"config":{"space":"cosine"}}}}}}}`
	switch {
	case r.URL.Path == "/api/v2/heartbeat":
		write(`{"nanosecond heartbeat":1}`)
	case r.URL.Path == "/api/v2/version":
		write(`"1.5.9"`)
	case r.URL.Path == "/api/v2/healthcheck":
		write(`{"is_executor_ready":true,"is_log_client_ready":true}`)
	case r.URL.Path == "/api/v2/pre-flight-checks":
		write(`{"max_batch_size":5461,"supports_base64_encoding":true}`)
	case r.URL.Path == "/api/v2/auth/identity":
		write(`{"user_id":"","tenant":"default_tenant","databases":["default_database"]}`)
	case r.URL.Path == "/api/v2/tenants/default_tenant":
		write(`{"name":"default_tenant","resource_name":null}`)
	case r.URL.Path == "/api/v2/tenants/default_tenant/databases":
		write(`[{"id":"00000000-0000-0000-0000-000000000000","name":"default_database","tenant":"default_tenant"}]`)
	case strings.HasSuffix(r.URL.Path, "/collections_count"):
		write(`1`)
	case r.URL.Path == "/api/v2/tenants/default_tenant/databases/default_database/collections":
		write(`[` + collectionJSON + `]`)
	case strings.HasSuffix(r.URL.Path, "/count"):
		write(`3`)
	case strings.HasSuffix(r.URL.Path, "/get"):
		write(`{"ids":["a","b","c"],"documents":["alpha","beta","gamma"],"metadatas":[{"tag":"x"},{"tag":"y"},{"tag":"x"}],` +
			`"embeddings":[[1,0,0],[0,1,0],[0.9,0.1,0]],"uris":null,"include":["documents","metadatas","embeddings"]}`)
	case strings.HasSuffix(r.URL.Path, "/query"):
		write(`{"ids":[["a","c"]],"documents":[["alpha","gamma"]],"metadatas":[[{"tag":"x"},{"tag":"x"}]],"distances":[[0,0.02]],"include":["documents","metadatas","distances"]}`)
	case strings.HasSuffix(r.URL.Path, "/indexing_status"):
		http.Error(w, `{"error":"InternalError","message":"not implemented"}`, http.StatusInternalServerError)
	case strings.HasSuffix(r.URL.Path, "/collections/docs"),
		strings.HasSuffix(r.URL.Path, "/by-id/11111111-2222-3333-4444-555555555555"):
		write(collectionJSON)
	default:
		write(`{}`)
	}
}

func newTestSession(t *testing.T) (plugin.Session, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(&fakeChroma{t: t})
	sess, err := New().Connect(context.Background(), plugin.ConnectConfig{
		Config: map[string]any{"endpoint": srv.URL, "read_only": false},
		Net:    plugintest.DirectTransport(),
	})
	if err != nil {
		srv.Close()
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() {
		_ = sess.Close()
		srv.Close()
	})
	return sess, srv
}

func call(t *testing.T, sess plugin.Session, id string, params map[string]string, query url.Values, body []byte) any {
	t.Helper()
	route, ok := plugintest.RouteMap(New().Routes())[id]
	if !ok {
		t.Fatalf("unknown route %q", id)
	}
	out, err := route.Handle(plugin.NewRequestContext(context.Background(), plugin.User{}, sess, params, query, body).WithStorage(newFakeStorage()))
	if err != nil {
		t.Fatalf("%s: %v", id, err)
	}
	return out
}

func TestOverviewReportsServerFacts(t *testing.T) {
	sess, _ := newTestSession(t)
	out := call(t, sess, rid("overview"), nil, nil, nil).(row)
	if out["version"] != "1.5.9" || out["status"] != "ready" || out["maxBatchSize"] != 5461 {
		t.Fatalf("unexpected overview: %#v", out)
	}
	if out["collections"] != 1 {
		t.Fatalf("expected the collection count, got %#v", out["collections"])
	}
}

func TestCollectionsListCarriesRefAndCounts(t *testing.T) {
	sess, _ := newTestSession(t)
	page := call(t, sess, rid("collections.list"), nil, nil, nil).(collectionsPage)
	if len(page.Items) != 1 {
		t.Fatalf("expected one collection, got %#v", page.Items)
	}
	item := page.Items[0]
	if item["name"] != "docs" || item["space"] != "cosine" || item["records"] != 3 || item["dimension"] != 3 {
		t.Fatalf("unexpected collection row: %#v", item)
	}
	ref, ok := item["ref"].(plugin.ResourceIdentity)
	if !ok || ref.Kind != "collection" || ref.UID != "11111111-2222-3333-4444-555555555555" || ref.Namespace != "default_database" {
		t.Fatalf("unexpected ref: %#v", item["ref"])
	}
}

func TestCollectionsListPagesOnTheServer(t *testing.T) {
	base := &fakeChroma{t: t}
	var mu sync.Mutex
	counts := 0
	limit := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v2/tenants/default_tenant/databases/default_database/collections":
			mu.Lock()
			limit = r.URL.Query().Get("limit")
			mu.Unlock()
			asked, _ := strconv.Atoi(r.URL.Query().Get("limit"))
			items := make([]string, 0, asked)
			for i := range asked {
				items = append(items, `{"id":"id-`+strconv.Itoa(i)+`","name":"c`+strconv.Itoa(i)+`","dimension":3}`)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[` + strings.Join(items, ",") + `]`))
		case strings.HasSuffix(r.URL.Path, "/count"):
			mu.Lock()
			counts++
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`3`))
		default:
			base.ServeHTTP(w, r)
		}
	}))
	defer srv.Close()

	sess, err := New().Connect(context.Background(), plugin.ConnectConfig{
		Config: map[string]any{"endpoint": srv.URL},
		Net:    plugintest.DirectTransport(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()

	out := call(t, sess, rid("collections.list"), nil, url.Values{"limit": []string{"5"}}, nil).(collectionsPage)
	if len(out.Items) != 5 || out.NextCursor != "5" {
		t.Fatalf("expected one bounded page, got %d items (next %q)", len(out.Items), out.NextCursor)
	}
	mu.Lock()
	defer mu.Unlock()
	if limit != "6" {
		t.Fatalf("the server should page the fetch, asked for limit %q", limit)
	}
	if counts != 5 {
		t.Fatalf("expected 5 record counts, got %d", counts)
	}
}

func TestRecordsListSearchesDocuments(t *testing.T) {
	sess, srv := newTestSession(t)
	_ = srv
	page := call(t, sess, rid("records.list"), map[string]string{"collection": "docs"}, url.Values{"filter": []string{"alpha"}}, nil).(plugin.Page[row])
	if len(page.Items) != 3 {
		t.Fatalf("expected the fake set, got %#v", page.Items)
	}
	ref := page.Items[0]["ref"].(plugin.ResourceIdentity)
	if ref.Kind != "record" || ref.Scope != "default_tenant/default_database" || ref.Name != "a" {
		t.Fatalf("unexpected record ref: %#v", ref)
	}
}

func TestRecordReadAndNeighbors(t *testing.T) {
	sess, _ := newTestSession(t)
	params := map[string]string{"scope": "default_tenant/default_database", "collection": "docs", "record": "a"}
	record := call(t, sess, rid("record.read"), params, nil, nil).(row)
	if record["dimension"] != 3 || record["space"] != "cosine" {
		t.Fatalf("unexpected record: %#v", record)
	}
	if keys, ok := record["metadata_keys"].([]string); !ok || len(keys) != 1 || keys[0] != "tag" {
		t.Fatalf("unexpected metadata keys: %#v", record["metadata_keys"])
	}
	page := call(t, sess, rid("record.neighbors"), params, nil, nil).(plugin.Page[row])
	if len(page.Items) != 1 || page.Items[0]["id"] != "c" || page.Items[0]["rank"] != 1 {
		t.Fatalf("unexpected neighbors: %#v", page.Items)
	}
}

func TestCollectionSchemaRows(t *testing.T) {
	sess, _ := newTestSession(t)
	page := call(t, sess, rid("collection.schema"), map[string]string{"collection": "docs"}, nil, nil).(plugin.Page[row])
	if len(page.Items) != 2 {
		t.Fatalf("expected default and keyed index rows, got %#v", page.Items)
	}
	states := map[string]string{}
	for _, item := range page.Items {
		states[item["key"].(string)] = item["state"].(string)
	}
	if states["(default)"] != "disabled" || states["#embedding"] != "enabled" {
		t.Fatalf("unexpected schema states: %#v", states)
	}
}

func TestReadOnlyBlocksMutations(t *testing.T) {
	srv := httptest.NewServer(&fakeChroma{t: t})
	defer srv.Close()
	sess, err := New().Connect(context.Background(), plugin.ConnectConfig{
		Config: map[string]any{"endpoint": srv.URL},
		Net:    plugintest.DirectTransport(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(New().Routes())
	// Every route that writes to Chroma, storage-only routes excluded.
	for _, id := range []string{
		rid("tenant.create"), rid("database.create"), rid("database.delete"),
		rid("collection.create"), rid("collection.update"), rid("collection.delete"),
		rid("records.add"), rid("record.update"), rid("records.delete"),
	} {
		body := []byte(`{"name":"x","id":"a","new_name":"y","ids":["a"],"embeddings":[[1,2,3]]}`)
		rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess, map[string]string{"collection": "docs", "database": "default_database"}, nil, body)
		if _, err := routes[id].Handle(rc); err == nil || !strings.Contains(err.Error(), "read-only") {
			t.Fatalf("%s: expected a read-only rejection, got %v", id, err)
		}
	}
}

func TestQueryStreamShapes(t *testing.T) {
	sess, _ := newTestSession(t)
	params := map[string]string{"collection": "docs"}

	similarity := runQueryFrame(t, sess, params, `{"query_embeddings":[[1,0,0]],"n_results":2}`)
	if similarity["mode"] != "similarity" || similarity["rowCount"] != float64(2) {
		t.Fatalf("unexpected similarity result: %#v", similarity)
	}
	columns := similarity["columns"].([]any)
	if columns[0] != "query" || columns[3] != "distance" {
		t.Fatalf("unexpected columns: %#v", columns)
	}
	first := similarity["rows"].([]any)[0].([]any)
	if first[2] != "a" || first[4] != float64(1) {
		t.Fatalf("unexpected first row: %#v", first)
	}

	fetch := runQueryFrame(t, sess, params, `{"where":{"tag":"x"},"limit":10}`)
	if fetch["mode"] != "fetch" || fetch["rowCount"] != float64(3) {
		t.Fatalf("unexpected fetch result: %#v", fetch)
	}

	byID := runQueryFrame(t, sess, params, `{"query_id":"a","n_results":2}`)
	if byID["mode"] != "similarity" {
		t.Fatalf("expected query_id to run a similarity search: %#v", byID)
	}

	text := runQueryFrame(t, sess, params, `{"query_text":"alpha"}`)
	if text["mode"] != "fetch" {
		t.Fatalf("expected query_text to run a document fetch: %#v", text)
	}
}

func TestQueryStreamErrorsKeepTheStreamOpen(t *testing.T) {
	sess, _ := newTestSession(t)
	frames := runQueryStream(t, sess, map[string]string{"collection": "docs"},
		`{"query":""}`, `{"query":"not json"}`, `{"query":"{\"limit\":1}"}`)
	if len(frames) != 3 {
		t.Fatalf("expected three frames, got %d: %#v", len(frames), frames)
	}
	if frames[0]["error"] == nil || frames[1]["error"] == nil {
		t.Fatalf("expected error frames: %#v", frames)
	}
	if frames[2]["rowCount"] == nil {
		t.Fatalf("expected the stream to survive errors: %#v", frames[2])
	}
}

func TestQueryStreamClosesOnMalformedFrame(t *testing.T) {
	sess, _ := newTestSession(t)
	route := plugintest.RouteMap(New().Routes())[rid("query")]
	stream := newMemoryStream("{oops}\n" + `{"query":"{\"limit\":1}"}` + "\n")
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess, map[string]string{"collection": "docs"}, nil, nil).WithStorage(newFakeStorage())

	done := make(chan error, 1)
	go func() { done <- route.Stream(rc, stream) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("query stream: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a malformed frame must close the stream instead of looping on the stuck decoder")
	}
	if count := strings.Count(stream.out.String(), "\n"); count != 1 {
		t.Fatalf("expected exactly one error frame, got %d: %q", count, stream.out.String())
	}
}

func TestValidateNameRejectsDotSegments(t *testing.T) {
	for _, value := range []string{".", "..", "a/b", ""} {
		if err := validateName("database", value); err == nil {
			t.Fatalf("expected %q to be rejected", value)
		}
	}
	if err := validateName("database", "default_database"); err != nil {
		t.Fatalf("valid name rejected: %v", err)
	}
}

func TestFetchOmitsDimensionUnlessEmbeddingsRequested(t *testing.T) {
	sess, _ := newTestSession(t)
	params := map[string]string{"collection": "docs"}

	plain := runQueryFrame(t, sess, params, `{"limit":10}`)
	if got := plain["columns"].([]any); len(got) != 4 || slices.Contains(got, any("dimension")) {
		t.Fatalf("unexpected fetch columns: %#v", got)
	}
	withVectors := runQueryFrame(t, sess, params, `{"limit":10,"include":["documents","embeddings"]}`)
	columns := withVectors["columns"].([]any)
	if !slices.Contains(columns, any("dimension")) {
		t.Fatalf("expected a dimension column: %#v", columns)
	}
	first := withVectors["rows"].([]any)[0].([]any)
	if first[len(first)-1] != float64(3) {
		t.Fatalf("expected the embedding width, got %#v", first)
	}
}

func TestUnboundedFetchRequiresConfirmation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/count"):
			_, _ = w.Write([]byte(`5000`))
		case strings.HasSuffix(r.URL.Path, "/get"):
			_, _ = w.Write([]byte(`{"ids":["a"],"documents":["alpha"],"metadatas":[{}],"include":["documents"]}`))
		case strings.HasSuffix(r.URL.Path, "/collections/docs"):
			_, _ = w.Write([]byte(`{"id":"11111111-2222-3333-4444-555555555555","name":"docs","tenant":"default_tenant","database":"default_database","log_position":0,"version":0,"configuration_json":{}}`))
		default:
			_, _ = w.Write([]byte(`{"nanosecond heartbeat":1}`))
		}
	}))
	defer srv.Close()
	sess, err := New().Connect(context.Background(), plugin.ConnectConfig{
		Config: map[string]any{"endpoint": srv.URL},
		Net:    plugintest.DirectTransport(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()

	frames := runQueryStream(t, sess, map[string]string{"collection": "docs"}, `{"query":"{}"}`)
	if frames[0]["confirmRequired"] != true {
		t.Fatalf("expected a confirmation challenge, got %#v", frames[0])
	}
	frames = runQueryStream(t, sess, map[string]string{"collection": "docs"}, `{"query":"{}","confirm":true}`)
	if frames[0]["rowCount"] == nil {
		t.Fatalf("expected the confirmed run to execute: %#v", frames[0])
	}
}

func runQueryFrame(t *testing.T, sess plugin.Session, params map[string]string, query string) map[string]any {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"query": query})
	if err != nil {
		t.Fatal(err)
	}
	frames := runQueryStream(t, sess, params, string(payload))
	if len(frames) != 1 {
		t.Fatalf("expected one frame, got %#v", frames)
	}
	return frames[0]
}

func runQueryStream(t *testing.T, sess plugin.Session, params map[string]string, frames ...string) []map[string]any {
	t.Helper()
	route := plugintest.RouteMap(New().Routes())[rid("query")]
	stream := newMemoryStream(strings.Join(frames, "\n") + "\n")
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess, params, nil, nil).WithStorage(newFakeStorage())
	if err := route.Stream(rc, stream); err != nil {
		t.Fatalf("query stream: %v", err)
	}
	out := make([]map[string]any, 0, len(frames))
	dec := json.NewDecoder(strings.NewReader(stream.out.String()))
	for dec.More() {
		var frame map[string]any
		if err := dec.Decode(&frame); err != nil {
			t.Fatalf("decode frame: %v", err)
		}
		out = append(out, frame)
	}
	return out
}

func TestSavedQueriesRoundTrip(t *testing.T) {
	sess, _ := newTestSession(t)
	routes := plugintest.RouteMap(New().Routes())
	storage := newFakeStorage()
	ctx := context.Background()

	save := func(body string) any {
		rc := plugin.NewRequestContext(ctx, plugin.User{}, sess, nil, nil, []byte(body)).WithStorage(storage)
		out, err := routes[rid("queries.save")].Handle(rc)
		if err != nil {
			t.Fatalf("save: %v", err)
		}
		return out
	}
	save(`{"name":"Top matches","query":"{\"limit\":5}","collection":"docs"}`)

	rc := plugin.NewRequestContext(ctx, plugin.User{}, sess, nil, nil, nil).WithStorage(storage)
	page, err := routes[rid("queries.list")].Handle(rc)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	items := page.(plugin.Page[row]).Items
	if len(items) != 1 || items[0]["name"] != "Top matches" {
		t.Fatalf("unexpected saved queries: %#v", items)
	}

	rc = plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"id": items[0]["id"].(string)}, nil, nil).WithStorage(storage)
	if _, err := routes[rid("queries.delete")].Handle(rc); err != nil {
		t.Fatalf("delete: %v", err)
	}
	rc = plugin.NewRequestContext(ctx, plugin.User{}, sess, nil, nil, nil).WithStorage(storage)
	page, _ = routes[rid("queries.list")].Handle(rc)
	if len(page.(plugin.Page[row]).Items) != 0 {
		t.Fatalf("expected the saved query to be gone")
	}

	rc = plugin.NewRequestContext(ctx, plugin.User{}, sess, nil, nil, []byte(`{"name":"bad","query":"nope"}`)).WithStorage(storage)
	if _, err := routes[rid("queries.save")].Handle(rc); err == nil {
		t.Fatal("expected non-JSON saved queries to be rejected")
	}
}

func TestQueryHistoryIsRecordedAndCleared(t *testing.T) {
	sess, _ := newTestSession(t)
	routes := plugintest.RouteMap(New().Routes())
	storage := newFakeStorage()
	params := map[string]string{"collection": "docs"}

	stream := newMemoryStream(`{"query":"{\"limit\":2}"}` + "\n")
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess, params, nil, nil).WithStorage(storage)
	if err := routes[rid("query")].Stream(rc, stream); err != nil {
		t.Fatalf("query stream: %v", err)
	}

	rc = plugin.NewRequestContext(context.Background(), plugin.User{}, sess, params, nil, nil).WithStorage(storage)
	page, err := routes[rid("history.list")].Handle(rc)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	items := page.(plugin.Page[row]).Items
	if len(items) != 1 || items[0]["severity"] != "info" {
		t.Fatalf("unexpected history: %#v", items)
	}

	rc = plugin.NewRequestContext(context.Background(), plugin.User{}, sess, params, nil, nil).WithStorage(storage)
	out, err := routes[rid("history.clear")].Handle(rc)
	if err != nil {
		t.Fatalf("clear: %v", err)
	}
	if out.(row)["deleted"] != 1 {
		t.Fatalf("unexpected clear result: %#v", out)
	}
}

func TestRecordPayloadAcceptsEditorEnvelopes(t *testing.T) {
	bodies := []string{
		`{"ids":["a"],"embeddings":[[1,2,3]]}`,
		`{"records":{"ids":["a"],"embeddings":[[1,2,3]]}}`,
		`{"content":"{\"ids\":[\"a\"],\"embeddings\":[[1,2,3]]}"}`,
	}
	for _, body := range bodies {
		payload, err := recordPayload([]byte(body))
		if err != nil {
			t.Fatalf("%s: %v", body, err)
		}
		if len(payload.IDs) != 1 || len(payload.Embeddings) != 1 {
			t.Fatalf("%s: unexpected payload %#v", body, payload)
		}
	}
	if _, err := recordPayload([]byte(`[1,2]`)); err == nil {
		t.Fatal("expected a non-object body to be rejected")
	}
}

func TestScopeResolution(t *testing.T) {
	s := &Session{opts: Options{Tenant: "t", Database: "d"}}
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, s, map[string]string{"scope": "other/db2"}, nil, nil)
	sc, err := scopeOf(rc, s)
	if err != nil {
		t.Fatal(err)
	}
	if sc.Tenant != "other" || sc.Database != "db2" {
		t.Fatalf("unexpected scope: %#v", sc)
	}
	rc = plugin.NewRequestContext(context.Background(), plugin.User{}, s, nil, nil, nil)
	if sc, _ = scopeOf(rc, s); sc.Tenant != "t" || sc.Database != "d" {
		t.Fatalf("expected connection defaults, got %#v", sc)
	}
	rc = plugin.NewRequestContext(context.Background(), plugin.User{}, s, map[string]string{"database": "../etc"}, nil, nil)
	if _, err := scopeOf(rc, s); err == nil {
		t.Fatal("expected an invalid database name to be rejected")
	}
}

func TestProjectionIsDeterministicAndSeparates(t *testing.T) {
	vectors := [][]float64{{1, 0, 0}, {0.95, 0.1, 0}, {0, 1, 0}, {0.05, 0.98, 0}}
	first := project2D(vectors)
	second := project2D(vectors)
	if len(first.Points) != 4 {
		t.Fatalf("unexpected point count: %d", len(first.Points))
	}
	for i := range first.Points {
		if first.Points[i] != second.Points[i] {
			t.Fatalf("projection is not deterministic at %d: %#v vs %#v", i, first.Points[i], second.Points[i])
		}
	}
	if first.Explained(0) <= 0.5 {
		t.Fatalf("expected the first axis to dominate, got %.3f", first.Explained(0))
	}
	sameGroup := (first.Points[0].X < 0) == (first.Points[1].X < 0)
	crossGroup := (first.Points[0].X < 0) == (first.Points[2].X < 0)
	if !sameGroup || crossGroup {
		t.Fatalf("expected the two clusters on opposite sides: %#v", first.Points)
	}
}

func TestDistanceSemantics(t *testing.T) {
	a, b := []float64{1, 0}, []float64{0, 1}
	if got := distance(spaceL2, a, b); got != 2 {
		t.Fatalf("l2: got %v", got)
	}
	if got := distance(spaceCosine, a, b); got != 1 {
		t.Fatalf("cosine: got %v", got)
	}
	if got := distance(spaceIP, a, b); got != 1 {
		t.Fatalf("ip: got %v", got)
	}
	if got := distance(spaceCosine, a, a); got != 0 {
		t.Fatalf("cosine self: got %v", got)
	}
}

func TestRankAndSpearman(t *testing.T) {
	vectors := [][]float64{{0, 0}, {1, 0}, {2, 0}, {3, 0}}
	got := rank(spaceL2, vectors, vectors[0], 0, 2)
	if len(got) != 2 || got[0].Index != 1 || got[1].Index != 2 {
		t.Fatalf("unexpected ranking: %#v", got)
	}
	if value := spearman([]string{"a", "b", "c"}, []string{"a", "b", "c"}); value != 1 {
		t.Fatalf("identical rankings should agree: %v", value)
	}
	if value := spearman([]string{"a", "b", "c"}, []string{"c", "b", "a"}); value != -1 {
		t.Fatalf("reversed rankings should disagree: %v", value)
	}
}

func TestProjectionSceneInteraction(t *testing.T) {
	data := testSample()
	sc := newProjectionScene(data)
	ready := &canvas.ReadyEvent{Type: canvas.EventReady, Width: 1024, Height: 640, DPR: 2, Theme: plugin.PanelThemeDark}
	if !sc.Handle(ready) {
		t.Fatal("ready event should mark the scene dirty")
	}
	frame := sc.Frame()
	if len(frame.Regions) != data.len() {
		t.Fatalf("expected one region per point, got %d", len(frame.Regions))
	}
	if !sc.Handle(pointerDown("pt:2")) {
		t.Fatal("selecting a new point should redraw")
	}
	if sc.selected != 2 || len(sc.neighbors) == 0 {
		t.Fatalf("unexpected selection state: %d", sc.selected)
	}
	if sc.Handle(pointerDown("pt:2")) {
		t.Fatal("re-selecting the same point should not redraw")
	}
	if !sc.Handle(&canvas.KeyEvent{Type: canvas.EventKey, Event: "down", Key: "Escape"}) || sc.selected != -1 {
		t.Fatal("escape should clear the selection")
	}
	if sc.Handle(pointerDown("")) {
		t.Fatal("a pointer press outside every region should not select")
	}
	empty := newProjectionScene(sample{Collection: data.Collection})
	if len(empty.Frame().Regions) != 0 {
		t.Fatal("an empty collection should render no regions")
	}
}

func TestProjectionRegionsCoverEverySelectablePoint(t *testing.T) {
	data := largeSample(900)
	sc := newProjectionScene(data)
	sc.Handle(&canvas.ReadyEvent{Type: canvas.EventReady, Width: 1024, Height: 640, DPR: 2, Theme: plugin.PanelThemeDark})
	if frame := sc.Frame(); len(frame.Regions) != data.len() {
		t.Fatalf("expected a region for each of the %d sampled points, got %d", data.len(), len(frame.Regions))
	}
	if !sc.Handle(&canvas.KeyEvent{Type: canvas.EventKey, Event: "down", Key: "End"}) {
		t.Fatal("End should move the selection to the last point")
	}
	assertFocusResolves(t, sc.Frame())
	if !sc.Handle(&canvas.WheelEvent{Type: canvas.EventWheel, DeltaY: -1200}) {
		t.Fatal("wheel should zoom")
	}
	if !sc.Handle(&canvas.KeyEvent{Type: canvas.EventKey, Event: "down", Key: "Home"}) {
		t.Fatal("Home should move the selection to the first point")
	}
	assertFocusResolves(t, sc.Frame())
}

// assertFocusResolves checks the frame focuses a region it actually publishes;
// the renderer drops the accessible description when the id is missing.
func assertFocusResolves(t *testing.T, frame canvas.Frame) {
	t.Helper()
	labels := make(map[string]string, len(frame.Regions))
	for _, region := range frame.Regions {
		labels[region.ID] = region.Label
	}
	focused := ""
	for _, command := range frame.Commands {
		if focus, ok := command.(canvas.FocusRegion); ok {
			focused = focus.ID
		}
	}
	if focused == "" {
		t.Fatal("expected the frame to focus the selected point")
	}
	if label, ok := labels[focused]; !ok || label == "" {
		t.Fatalf("focused region %q has no labelled region in the frame", focused)
	}
}

func TestSpacesSceneReanchors(t *testing.T) {
	sc := newSpacesScene(testSample())
	sc.Handle(&canvas.ResizeEvent{Type: canvas.EventResize, Width: 1200, Height: 700, Theme: plugin.PanelThemeLight})
	frame := sc.Frame()
	if len(frame.Regions) == 0 {
		t.Fatal("expected neighbor cards")
	}
	for _, region := range frame.Regions {
		if region.Label == "" {
			t.Fatalf("region %q needs an accessible label", region.ID)
		}
	}
	before := sc.anchor
	if !sc.Handle(pointerDown(frame.Regions[0].ID)) {
		t.Fatal("clicking a card should re-anchor")
	}
	if sc.anchor == before {
		t.Fatal("anchor did not move")
	}
	if len(sc.agree) != len(spaces()) {
		t.Fatalf("expected one agreement score per space, got %d", len(sc.agree))
	}
}

func testSample() sample {
	documents := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	set := recordSet{IDs: []string{"a", "b", "c", "d", "e"}}
	vectors := [][]float64{{1, 0, 0}, {0.9, 0.2, 0}, {0, 1, 0}, {0.1, 0.95, 0}, {0, 0, 1}}
	for i := range set.IDs {
		doc := documents[i]
		set.Documents = append(set.Documents, &doc)
		set.Metadatas = append(set.Metadatas, map[string]any{"tag": doc[:1]})
		set.Embeddings = append(set.Embeddings, vectors[i])
	}
	col := collection{ID: "col", Name: "docs", Tenant: "t", Database: "d",
		Configuration: configuration{HNSW: &indexParams{Space: spaceCosine}}}
	out := sample{Collection: col, Records: set}
	for i := range set.IDs {
		out.Vectors = append(out.Vectors, set.Embeddings[i])
		out.Indexes = append(out.Indexes, i)
	}
	return out
}

func largeSample(n int) sample {
	set := recordSet{}
	for i := 0; i < n; i++ {
		id := "vec-" + strconv.Itoa(i)
		set.IDs = append(set.IDs, id)
		set.Documents = append(set.Documents, &id)
		set.Metadatas = append(set.Metadatas, map[string]any{"row": i})
		set.Embeddings = append(set.Embeddings, []float64{float64(i % 37), float64(i / 37), float64(i%11) / 3})
	}
	col := collection{ID: "big", Name: "big", Tenant: "t", Database: "d",
		Configuration: configuration{HNSW: &indexParams{Space: spaceL2}}}
	out := sample{Collection: col, Records: set}
	for i := range set.IDs {
		out.Vectors = append(out.Vectors, set.Embeddings[i])
		out.Indexes = append(out.Indexes, i)
	}
	return out
}

func pointerDown(regionID string) *canvas.PointerEvent {
	return &canvas.PointerEvent{Type: canvas.EventPointer, Event: "down", RegionID: regionID, X: 10, Y: 10}
}

// fakeStorage is the scoped plugin.Storage contract, in memory.
type fakeStorage struct {
	items map[string]plugin.StorageItem
}

func newFakeStorage() *fakeStorage { return &fakeStorage{items: map[string]plugin.StorageItem{}} }

func (s *fakeStorage) Get(_ context.Context, sc plugin.StorageScope, key string) (plugin.StorageItem, error) {
	item, ok := s.items[sc.Collection+"/"+key]
	if !ok {
		return plugin.StorageItem{}, plugin.ErrNotFound
	}
	return item, nil
}

func (s *fakeStorage) Put(_ context.Context, collection string, item plugin.StorageItem) (plugin.StorageItem, error) {
	s.items[collection+"/"+item.Key] = item
	return item, nil
}

func (s *fakeStorage) Delete(_ context.Context, sc plugin.StorageScope, key string) error {
	full := sc.Collection + "/" + key
	if _, ok := s.items[full]; !ok {
		return plugin.ErrNotFound
	}
	delete(s.items, full)
	return nil
}

func (s *fakeStorage) List(_ context.Context, sc plugin.StorageScope, filter *plugin.StorageListFilter) ([]plugin.StorageItem, error) {
	if filter == nil {
		filter = &plugin.StorageListFilter{}
	}
	out := make([]plugin.StorageItem, 0, len(s.items))
	for full, item := range s.items {
		if !strings.HasPrefix(full, sc.Collection+"/") {
			continue
		}
		if filter.KeyPrefix != "" && !strings.HasPrefix(item.Key, filter.KeyPrefix) {
			continue
		}
		if filter.ContentType != "" && item.ContentType != filter.ContentType {
			continue
		}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// memoryStream is a plugin.ClientStream backed by fixed input and a buffer.
type memoryStream struct {
	in  *strings.Reader
	out strings.Builder
	ctx context.Context
}

func newMemoryStream(input string) *memoryStream {
	return &memoryStream{in: strings.NewReader(input), ctx: context.Background()}
}

func (s *memoryStream) Read(p []byte) (int, error)  { return s.in.Read(p) }
func (s *memoryStream) Write(p []byte) (int, error) { return s.out.Write(p) }
func (s *memoryStream) Close() error                { return nil }
func (s *memoryStream) Context() context.Context    { return s.ctx }
