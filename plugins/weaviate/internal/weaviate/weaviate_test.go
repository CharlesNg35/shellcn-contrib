package weaviate

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
	"github.com/weaviate/weaviate/entities/models"
)

func TestManifestValidates(t *testing.T) {
	p := New()
	plugintest.ValidatePlugin(t, p)
	plugintest.ValidateProjectionPanelConfigs(t, plugintest.Projection(t, p))
	if !plugintest.CredentialKindSupported(p.Manifest().Config, plugin.CredentialKindAPIToken) {
		t.Fatal("Weaviate should accept stored API token credentials")
	}
}

func TestRouteMetadataIsNamespaced(t *testing.T) {
	for _, route := range New().Routes() {
		if !strings.HasPrefix(route.ID, protocolName+".") {
			t.Errorf("route %q is not namespaced", route.ID)
		}
		if route.AuditEvent != route.ID {
			t.Errorf("route %q audit event %q should equal its ID", route.ID, route.AuditEvent)
		}
		if !strings.HasPrefix(route.Permission, protocolName+".") {
			t.Errorf("route %q permission %q is not namespaced", route.ID, route.Permission)
		}
	}
}

func TestConfigSchemaHidesUnusedSecrets(t *testing.T) {
	schema := New().Manifest().Config
	apiKey := schema.VisibleValues(map[string]any{"auth": "api_key", "api_key": "s3cret", "tls_mode": "disable"}, nil)
	if apiKey["api_key"] != "s3cret" {
		t.Fatalf("api key should be visible in api_key mode: %#v", apiKey)
	}
	anonymous := schema.VisibleValues(map[string]any{"auth": "none", "api_key": "s3cret", "tls_mode": "disable"}, nil)
	if _, ok := anonymous["api_key"]; ok {
		t.Fatalf("api key must be hidden in anonymous mode: %#v", anonymous)
	}
	secrets := schema.VisibleSecretKeys(map[string]any{"auth": "api_key", "tls_mode": "verify-ca"}, nil)
	sort.Strings(secrets)
	if !slices.Contains(secrets, "api_key") || !slices.Contains(secrets, "ca_certificate") {
		t.Fatalf("expected api_key and ca_certificate secrets, got %v", secrets)
	}
	if err := schema.ValidateValues(map[string]any{"endpoint": "http://localhost:8080", "auth": "none", "tls_mode": "disable", "consistency_level": "QUORUM"}, nil); err != nil {
		t.Fatalf("minimal config should validate: %v", err)
	}
	if err := schema.ValidateValues(map[string]any{"endpoint": "http://localhost:8080", "auth": "api_key", "tls_mode": "disable", "consistency_level": "QUORUM"}, nil); err == nil {
		t.Fatal("api_key mode must require an api key")
	}
}

func TestParseOptionsRejectsBadInput(t *testing.T) {
	if _, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{"endpoint": "weaviate.example.com"}}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("expected invalid endpoint error, got %v", err)
	}
	if _, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{"auth": "oidc"}}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("expected unsupported auth error, got %v", err)
	}
	if _, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{"inference_header": "X-Bad Header"}}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("expected invalid header error, got %v", err)
	}
	opts, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{
		"endpoint": "https://weaviate.example.com:8443/", "auth": "api_key", "api_key": "token", "consistency_level": "ALL",
	}})
	if err != nil {
		t.Fatalf("parseOptions: %v", err)
	}
	if opts.Scheme != "https" || opts.Host != "weaviate.example.com:8443" || opts.Consistency != "ALL" {
		t.Fatalf("unexpected options: %#v", opts)
	}
}

func newTestSession(t *testing.T, handler http.HandlerFunc) (plugin.Session, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(handler)
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

const schemaFixture = `{"classes":[
  {"class":"Article","vectorizer":"none","vectorIndexType":"hnsw","vectorIndexConfig":{"distance":"cosine","ef":-1},
   "properties":[{"name":"title","dataType":["text"]},{"name":"author","dataType":["Author"]}]},
  {"class":"Author","vectorizer":"none","vectorIndexType":"hnsw","properties":[{"name":"name","dataType":["text"]}]}
]}`

const nodesFixture = `{"nodes":[{"name":"node1","status":"HEALTHY","version":"1.38.6","stats":{"objectCount":7,"shardCount":2},
 "batchStats":{"queueLength":0,"ratePerSecond":3},
 "shards":[{"name":"abc","class":"Article","objectCount":5,"vectorIndexingStatus":"READY"},
           {"name":"def","class":"Author","objectCount":2,"vectorIndexingStatus":"READY"}]}]}`

func fixtureHandler(t *testing.T) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/.well-known/ready":
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/v1/meta":
			_, _ = w.Write([]byte(`{"version":"1.38.6","hostname":"http://[::]:8080","modules":{"backup-filesystem":{"backupsPath":"/tmp"},"text2vec-openai":{"name":"OpenAI Module"}}}`))
		case r.URL.Path == "/v1/schema":
			_, _ = w.Write([]byte(schemaFixture))
		case r.URL.Path == "/v1/schema/Article":
			_, _ = w.Write([]byte(`{"class":"Article","vectorizer":"none","vectorIndexType":"hnsw","vectorIndexConfig":{"distance":"cosine"},"properties":[{"name":"title","dataType":["text"]},{"name":"author","dataType":["Author"]}]}`))
		case r.URL.Path == "/v1/nodes":
			_, _ = w.Write([]byte(nodesFixture))
		case r.URL.Path == "/v1/graphql":
			_, _ = w.Write([]byte(`{"data":{"Get":{"Article":[{"title":"Ada","_additional":{"id":"11111111-1111-1111-1111-111111111111","creationTimeUnix":1700000000000,"lastUpdateTimeUnix":1700000001000}}]}}}`))
		default:
			http.NotFound(w, r)
		}
	}
}

func TestCollectionsListMergesClusterCounts(t *testing.T) {
	sess, _ := newTestSession(t, fixtureHandler(t))
	routes := plugintest.RouteMap(New().Routes())
	out, err := routes[rid("collections.list")].Handle(plugin.NewRequestContext(context.Background(), plugin.User{}, sess, nil, nil, nil))
	if err != nil {
		t.Fatalf("collections.list: %v", err)
	}
	page := out.(plugin.Page[row])
	if len(page.Items) != 2 {
		t.Fatalf("expected two collections, got %#v", page.Items)
	}
	article := page.Items[0]
	if article["name"] != "Article" || article["objects"] != int64(5) || article["references"] != 1 {
		t.Fatalf("unexpected Article row: %#v", article)
	}
	if article["status"] != "READY" {
		t.Fatalf("expected READY index status, got %#v", article["status"])
	}
}

func TestObjectsListUsesGraphQLPaging(t *testing.T) {
	sess, _ := newTestSession(t, fixtureHandler(t))
	routes := plugintest.RouteMap(New().Routes())
	out, err := routes[rid("objects.list")].Handle(plugin.NewRequestContext(
		context.Background(), plugin.User{}, sess, map[string]string{"collection": "Article"}, nil, nil))
	if err != nil {
		t.Fatalf("objects.list: %v", err)
	}
	page := out.(plugin.Page[row])
	if len(page.Items) != 1 {
		t.Fatalf("expected one object, got %#v", page.Items)
	}
	item := page.Items[0]
	if item["id"] != "11111111-1111-1111-1111-111111111111" || item["title"] != "Ada" {
		t.Fatalf("unexpected object row: %#v", item)
	}
	if item["_creationTime"] != "2023-11-14T22:13:20Z" {
		t.Fatalf("creation time should be RFC3339: %#v", item["_creationTime"])
	}
	ref, ok := item["ref"].(plugin.ResourceIdentity)
	if !ok || ref.Kind != "object" || ref.Namespace != "Article" {
		t.Fatalf("unexpected ref: %#v", item["ref"])
	}
}

func TestSchemaGraphLinksCrossReferences(t *testing.T) {
	sess, _ := newTestSession(t, fixtureHandler(t))
	routes := plugintest.RouteMap(New().Routes())
	out, err := routes[rid("schema.graph")].Handle(plugin.NewRequestContext(context.Background(), plugin.User{}, sess, nil, nil, nil))
	if err != nil {
		t.Fatalf("schema.graph: %v", err)
	}
	graph := out.(graphResult)
	if len(graph.Nodes) != 2 {
		t.Fatalf("expected two class nodes, got %#v", graph.Nodes)
	}
	if len(graph.Edges) != 1 || graph.Edges[0].Source != "class:Article" || graph.Edges[0].Target != "class:Author" || graph.Edges[0].Label != "author" {
		t.Fatalf("unexpected edges: %#v", graph.Edges)
	}
}

func TestCollectionColumnsDeclareEditors(t *testing.T) {
	sess, _ := newTestSession(t, fixtureHandler(t))
	routes := plugintest.RouteMap(New().Routes())
	out, err := routes[rid("collection.columns")].Handle(plugin.NewRequestContext(
		context.Background(), plugin.User{}, sess, map[string]string{"collection": "Article"}, nil, nil))
	if err != nil {
		t.Fatalf("collection.columns: %v", err)
	}
	byName := map[string]row{}
	for _, item := range out.(plugin.Page[row]).Items {
		byName[text(item["name"])] = item
	}
	if byName["id"]["readOnly"] != true {
		t.Fatalf("id column must be read-only: %#v", byName["id"])
	}
	if byName["title"]["editor"] != string(plugin.ColumnEditorText) {
		t.Fatalf("title should use the text editor: %#v", byName["title"])
	}
	if _, ok := byName["author"]; ok {
		t.Fatalf("cross-reference properties are not grid columns; objects.list never selects them: %#v", byName["author"])
	}
}

func TestObjectGridColumnsMatchListedRows(t *testing.T) {
	sess, _ := newTestSession(t, fixtureHandler(t))
	routes := plugintest.RouteMap(New().Routes())
	params := map[string]string{"collection": "Article"}
	columns, err := routes[rid("collection.columns")].Handle(plugin.NewRequestContext(context.Background(), plugin.User{}, sess, params, nil, nil))
	if err != nil {
		t.Fatalf("collection.columns: %v", err)
	}
	listed, err := routes[rid("objects.list")].Handle(plugin.NewRequestContext(context.Background(), plugin.User{}, sess, params, nil, nil))
	if err != nil {
		t.Fatalf("objects.list: %v", err)
	}
	item := listed.(plugin.Page[row]).Items[0]
	for _, column := range columns.(plugin.Page[row]).Items {
		name := text(column["name"])
		if column["editable"] != true {
			continue
		}
		if _, ok := item[name]; !ok {
			t.Fatalf("editable column %q is never returned by objects.list: %#v", name, item)
		}
	}
}

func TestObjectsPanelPatchesFromTheResourceWatch(t *testing.T) {
	config, ok := collectionPanel(t, "objects").Config.(plugin.TableConfig)
	if !ok {
		t.Fatalf("the objects tab should be a table panel: %#v", collectionPanel(t, "objects").Config)
	}
	if config.Watch == nil && config.RefreshIntervalMs == 0 {
		t.Fatal("the objects grid must stay live like the collections, shards, and node-shard tables")
	}
	if config.Watch.RouteID != rid("resource.watch") || config.Watch.Method != plugin.MethodWS {
		t.Fatalf("unexpected watch source: %#v", config.Watch)
	}
	if config.Watch.Params["kind"] != "object" || config.Watch.Params["collection"] != "${resource.name}" {
		t.Fatalf("the objects watch must be scoped to the collection: %#v", config.Watch.Params)
	}
	if _, ok := watchKinds[config.Watch.Params["kind"]]; !ok {
		t.Fatalf("watch kind %q has no snapshot loader", config.Watch.Params["kind"])
	}
}

func collectionPanel(t *testing.T, key string) plugin.Panel {
	t.Helper()
	for _, panel := range collectionResource().Detail.Tabs {
		if panel.Key == key {
			return panel
		}
	}
	t.Fatalf("collection detail has no %q tab", key)
	return plugin.Panel{}
}

const objectFixture = `{"class":"Article","id":"11111111-1111-1111-1111-111111111111",
 "properties":{"title":"Ada","subtitle":"draft"},"vector":[0.1,0.2,0.3]}`

type objectWrite struct {
	method string
	body   map[string]any
}

func TestObjectUpdateMergesGridEditsAndReplacesDocuments(t *testing.T) {
	var writes []objectWrite
	sess, _ := newTestSession(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/schema/Vectorized":
			_, _ = w.Write([]byte(`{"class":"Vectorized","vectorizer":"text2vec-openai","properties":[{"name":"title","dataType":["text"]}]}`))
		case strings.HasPrefix(r.URL.Path, "/v1/objects/"):
			if r.Method == http.MethodGet {
				_, _ = w.Write([]byte(objectFixture))
				return
			}
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			writes = append(writes, objectWrite{method: r.Method, body: body})
			if r.Method == http.MethodPatch {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			_, _ = w.Write([]byte(objectFixture))
		default:
			fixtureHandler(t)(w, r)
		}
	})
	routes := plugintest.RouteMap(New().Routes())
	update := func(collection string, body string) {
		t.Helper()
		params := map[string]string{"collection": collection, "id": "11111111-1111-1111-1111-111111111111"}
		if _, err := routes[rid("object.update")].Handle(
			plugin.NewRequestContext(context.Background(), plugin.User{}, sess, params, nil, []byte(body))); err != nil {
			t.Fatalf("object.update: %v", err)
		}
	}

	update("Article", `{"title":"Grace"}`)
	update("Article", `{"properties":{"title":"Grace"}}`)
	update("Vectorized", `{"content":"{\"title\":\"Grace\"}"}`)

	if len(writes) != 3 {
		t.Fatalf("expected three writes, got %#v", writes)
	}
	if writes[0].method != http.MethodPatch {
		t.Fatalf("a single-cell grid edit must merge: %#v", writes[0])
	}
	if writes[1].method != http.MethodPut {
		t.Fatalf("a JSON document save must replace so deleting a key deletes the property: %#v", writes[1])
	}
	properties, _ := writes[1].body["properties"].(map[string]any)
	if _, ok := properties["subtitle"]; ok || properties["title"] != "Grace" {
		t.Fatalf("the replacing body must carry exactly the edited document: %#v", writes[1].body)
	}
	vector, _ := writes[1].body["vector"].([]any)
	if len(vector) != 3 {
		t.Fatalf("replacing a vectorizer:none object must carry its stored vector: %#v", writes[1].body)
	}
	if _, ok := writes[2].body["vector"]; ok {
		t.Fatalf("a vectorized collection must re-embed instead of pinning the old vector: %#v", writes[2].body)
	}
}

func TestBatchDeleteRequiresAWhereFilter(t *testing.T) {
	var captured map[string]any
	sess, _ := newTestSession(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/batch/objects" && r.Method == http.MethodDelete {
			_ = json.NewDecoder(r.Body).Decode(&captured)
			_, _ = w.Write([]byte(`{"results":{"matches":2,"successful":2,"failed":0}}`))
			return
		}
		fixtureHandler(t)(w, r)
	})
	routes := plugintest.RouteMap(New().Routes())
	params := map[string]string{"collection": "Article"}
	newRC := func(body string) *plugin.RequestContext {
		return plugin.NewRequestContext(context.Background(), plugin.User{}, sess, params, nil, []byte(body))
	}

	if _, err := routes[rid("objects.delete")].Handle(newRC(`{}`)); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("an unfiltered batch delete must be refused, got %v", err)
	}
	if _, err := routes[rid("objects.delete")].Handle(newRC(`{"where":{"operator":"Sneaky","path":["title"]}}`)); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("unknown operators must be refused, got %v", err)
	}

	out, err := routes[rid("objects.delete")].Handle(newRC(`{"where":"{\"operator\":\"Equal\",\"path\":[\"title\"],\"valueText\":\"Ada\"}","dry_run":true}`))
	if err != nil {
		t.Fatalf("objects.delete: %v", err)
	}
	match, _ := captured["match"].(map[string]any)
	if match["class"] != "Article" {
		t.Fatalf("batch delete must scope the match to the route's collection: %#v", captured)
	}
	where, _ := match["where"].(map[string]any)
	if where["operator"] != "Equal" || captured["dryRun"] != true {
		t.Fatalf("unexpected batch delete body: %#v", captured)
	}
	result := out.(row)
	if result["matches"] != int64(2) || result["successful"] != int64(2) || result["dryRun"] != true {
		t.Fatalf("unexpected batch delete result: %#v", result)
	}
}

func TestSearchStreamHandlesQueryEditorFrames(t *testing.T) {
	var captured string
	sess, _ := newTestSession(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/graphql" {
			var body struct {
				Query string `json:"query"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			captured = body.Query
			_, _ = w.Write([]byte(`{"data":{"Get":{"Article":[{"title":"Ada","_additional":{"id":"11111111-1111-1111-1111-111111111111","score":"0.8"}}]}}}`))
			return
		}
		fixtureHandler(t)(w, r)
	})
	routes := plugintest.RouteMap(New().Routes())
	frames := "{\"query\":\"{\\\"mode\\\":\\\"hybrid\\\",\\\"query\\\":\\\"ada\\\",\\\"alpha\\\":0.6,\\\"limit\\\":5}\",\"confirm\":false}\n{\"query\":\"\"}\n"
	stream := newMemoryStream(frames)
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess, map[string]string{"collection": "Article"}, nil, nil)
	if err := routes[rid("search")].Stream(rc, stream); err != nil {
		t.Fatalf("search stream: %v", err)
	}
	if !strings.Contains(captured, `hybrid:{query: "ada"`) || !strings.Contains(captured, "alpha: 0.6") || !strings.Contains(captured, "limit: 5") {
		t.Fatalf("hybrid query was not built: %s", captured)
	}
	decoded := stream.frames(t)
	if len(decoded) != 2 {
		t.Fatalf("expected two response frames, got %#v", decoded)
	}
	columns, _ := decoded[0]["columns"].([]any)
	if len(columns) == 0 || columns[0] != "id" {
		t.Fatalf("id should be the first column: %#v", decoded[0])
	}
	if decoded[1]["error"] != "Query is required." {
		t.Fatalf("empty query should return an error frame: %#v", decoded[1])
	}
}

func TestSearchSpecValidation(t *testing.T) {
	if _, err := parseSearchSpec(`{"mode":"telepathy"}`); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("unknown mode should be rejected, got %v", err)
	}
	spec, err := parseSearchSpec(`{"vector":[0.1,0.2]}`)
	if err != nil || spec.Mode != "nearVector" || spec.Limit != defaultSearchLimit {
		t.Fatalf("mode inference failed: %#v %v", spec, err)
	}
	if _, err := parseSearchSpec(`not json`); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("invalid JSON should be rejected, got %v", err)
	}
}

func TestBuildWhereRejectsUnknownOperators(t *testing.T) {
	if _, err := buildWhere(json.RawMessage(`{"operator":"Sneaky","path":["title"],"valueText":"x"}`)); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("expected operator rejection, got %v", err)
	}
	where, err := buildWhere(json.RawMessage(`{"operator":"And","operands":[{"operator":"Like","path":["title"],"valueText":"*ada*"},{"operator":"GreaterThan","path":["rank"],"valueInt":3}]}`))
	if err != nil {
		t.Fatalf("buildWhere: %v", err)
	}
	built := where.String()
	if !strings.Contains(built, "operator: And") || !strings.Contains(built, "*ada*") {
		t.Fatalf("unexpected where clause: %s", built)
	}
}

func TestParseObjectRequestAcceptsGridAndEditorBodies(t *testing.T) {
	grid := plugin.NewRequestContext(context.Background(), plugin.User{}, nil, nil, nil,
		[]byte(`{"id":"11111111-1111-1111-1111-111111111111","title":"Ada","_creationTime":"x","ref":{"kind":"object"}}`))
	req, err := parseObjectRequest(grid)
	if err != nil {
		t.Fatalf("grid body: %v", err)
	}
	if req.ID != "11111111-1111-1111-1111-111111111111" || len(req.Properties) != 1 || req.Properties["title"] != "Ada" {
		t.Fatalf("unexpected grid request: %#v", req)
	}
	editor := plugin.NewRequestContext(context.Background(), plugin.User{}, nil, nil, nil, []byte(`{"properties":{"title":"Grace"}}`))
	req, err = parseObjectRequest(editor)
	if err != nil || req.Properties["title"] != "Grace" {
		t.Fatalf("unexpected editor request: %#v %v", req, err)
	}
	raw := plugin.NewRequestContext(context.Background(), plugin.User{}, nil, nil, nil, []byte(`{"content":"{\"title\":\"Hopper\"}"}`))
	req, err = parseObjectRequest(raw)
	if err != nil || req.Properties["title"] != "Hopper" {
		t.Fatalf("unexpected raw request: %#v %v", req, err)
	}
	bad := plugin.NewRequestContext(context.Background(), plugin.User{}, nil, nil, nil, []byte(`{"id":"not-a-uuid"}`))
	if _, err := parseObjectRequest(bad); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("expected uuid rejection, got %v", err)
	}
}

func TestReadOnlyBlocksMutations(t *testing.T) {
	srv := httptest.NewServer(fixtureHandler(t))
	defer srv.Close()
	sess, err := New().Connect(context.Background(), plugin.ConnectConfig{
		Config: map[string]any{"endpoint": srv.URL, "read_only": true},
		Net:    plugintest.DirectTransport(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(New().Routes())
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess, map[string]string{"collection": "Article"}, nil, []byte(`{"name":"Article"}`))
	if _, err := routes[rid("collection.delete")].Handle(rc); !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("read-only connection must refuse deletes, got %v", err)
	}
	batch := plugin.NewRequestContext(context.Background(), plugin.User{}, sess, map[string]string{"collection": "Article"}, nil,
		[]byte(`{"where":{"operator":"Equal","path":["title"],"valueText":"Ada"}}`))
	if _, err := routes[rid("objects.delete")].Handle(batch); !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("read-only connection must refuse batch deletes, got %v", err)
	}
}

func TestIdentifierValidationRejectsInjection(t *testing.T) {
	sess, _ := newTestSession(t, fixtureHandler(t))
	routes := plugintest.RouteMap(New().Routes())
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess, map[string]string{"collection": "Article/../../schema"}, nil, nil)
	if _, err := routes[rid("collection.read")].Handle(rc); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("path traversal must be rejected, got %v", err)
	}
	rc = plugin.NewRequestContext(context.Background(), plugin.User{}, sess, map[string]string{"collection": "Article", "id": "nope"}, nil, nil)
	if _, err := routes[rid("object.read")].Handle(rc); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("non-uuid object id must be rejected, got %v", err)
	}
}

func TestMetricsFrameSummarisesCluster(t *testing.T) {
	sess, _ := newTestSession(t, fixtureHandler(t))
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess, nil, nil, nil)
	frame := metricsFrame(rc, "")
	if frame["nodes"] != 1 || frame["healthyNodes"] != 1 || frame["objects"] != int64(7) || frame["collections"] != 2 {
		t.Fatalf("unexpected cluster frame: %#v", frame)
	}
	scoped := metricsFrame(rc, "node1")
	if scoped["shards"] != int64(2) || scoped["rate"] != int64(3) {
		t.Fatalf("unexpected node frame: %#v", scoped)
	}
	if missing := metricsFrame(rc, "ghost"); missing["error"] == nil {
		t.Fatalf("unknown node should report an error frame: %#v", missing)
	}
}

func TestProjectPCASeparatesClusters(t *testing.T) {
	vectors := [][]float32{
		{1, 0, 0}, {1.05, 0.02, 0}, {0.98, -0.01, 0},
		{-1, 0, 0}, {-1.02, 0.01, 0}, {-0.97, -0.02, 0},
	}
	coords, variance := projectPCA(vectors)
	if len(coords) != len(vectors) {
		t.Fatalf("expected one coordinate per vector, got %d", len(coords))
	}
	if variance[0] < 0.9 {
		t.Fatalf("the first component should explain most variance, got %v", variance)
	}
	if (coords[0][0] > 0) == (coords[3][0] > 0) {
		t.Fatalf("opposite vectors should land on opposite sides: %v", coords)
	}
	assignments, clusters := kmeans(coords, 2)
	if clusters != 2 || assignments[0] == assignments[3] {
		t.Fatalf("k-means should separate the two groups: %v", assignments)
	}
}

func TestEmbeddingSceneRendersEmptyState(t *testing.T) {
	scene := &embeddingScene{className: "Article", theme: plugin.PanelThemeLight, width: 800, height: 500, selected: -1, hovered: -1, zoom: 1, status: "no data"}
	frame := scene.frame()
	if len(frame.Commands) == 0 {
		t.Fatal("empty scene should still draw a frame")
	}
	if len(frame.Regions) != 0 {
		t.Fatalf("empty scene should not declare regions: %#v", frame.Regions)
	}
	scene.points = []embeddingPoint{{ID: "11111111-1111-1111-1111-111111111111", Label: "Ada", X: 0.2, Y: -0.4}}
	scene.clusters = 1
	frame = scene.frame()
	if len(frame.Regions) != 1 || frame.Regions[0].ID != "pt:0" || frame.Regions[0].Label == "" {
		t.Fatalf("points should map to labelled regions: %#v", frame.Regions)
	}
	if pointIndex("pt:0") != 0 || pointIndex("legend") != -1 {
		t.Fatal("region ids should decode back to point indexes")
	}
}

func TestWatchEventsUseTheRendererVocabulary(t *testing.T) {
	ref := plugin.ResourceIdentity{Kind: "collection", Name: "Article", UID: "Article"}
	before := map[string]watchEntry{"/Article": {ref: ref, item: row{"objects": 1}, fingerprint: "a"}}
	after := map[string]watchEntry{"/Article": {ref: ref, item: row{"objects": 2}, fingerprint: "b"}}
	events := watchEvents(before, after)
	if len(events) != 1 || events[0].Type != "modified" {
		t.Fatalf("changed rows must patch with a modified event: %#v", events)
	}
	if events := watchEvents(after, nil); len(events) != 1 || events[0].Type != "deleted" || events[0].Resource != nil {
		t.Fatalf("dropped rows must emit a bare deleted event: %#v", events)
	}
	if events := watchEvents(nil, after); len(events) != 1 || events[0].Type != "added" {
		t.Fatalf("first snapshot must arrive as adds: %#v", events)
	}
}

func TestGraphqlTableFlattensAdditional(t *testing.T) {
	var resp models.GraphQLResponse
	if err := json.Unmarshal([]byte(`{"data":{"Get":{"Article":[{"title":"Ada","_additional":{"id":"x","score":"0.5"}}]}}}`), &resp); err != nil {
		t.Fatal(err)
	}
	table := graphqlTable(&resp)
	columns := table["columns"].([]string)
	if columns[0] != "id" || !slices.Contains(columns, "score") || !slices.Contains(columns, "title") {
		t.Fatalf("unexpected columns: %v", columns)
	}
	if table["rowCount"] != 1 {
		t.Fatalf("unexpected row count: %#v", table)
	}
}

func TestSavedQueryNormalisation(t *testing.T) {
	if _, err := normalizeSavedQuery("", "graphql", "", "{}"); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("name is required, got %v", err)
	}
	if _, err := normalizeSavedQuery("q", "sql", "", "{}"); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("kind must be constrained, got %v", err)
	}
	if _, err := normalizeSavedQuery("q", "search", "bad name", "{}"); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("collection must be a valid class name, got %v", err)
	}
	value, err := normalizeSavedQuery("  Recent  ", "SEARCH", "Article", "  {}  ")
	if err != nil || value.Name != "Recent" || value.Kind != "search" || value.Query != "{}" {
		t.Fatalf("unexpected saved query: %#v %v", value, err)
	}
}

func TestSavedQueryStorageRoundTrip(t *testing.T) {
	sess, _ := newTestSession(t, fixtureHandler(t))
	routes := plugintest.RouteMap(New().Routes())
	store := &fakeStorage{}
	newRC := func(params map[string]string, body []byte) *plugin.RequestContext {
		return plugin.NewRequestContext(context.Background(), plugin.User{}, sess, params, nil, body).WithStorage(store)
	}
	saved, err := routes[rid("query.save")].Handle(newRC(nil, []byte(`{"name":"Recent","kind":"graphql","query":"{ Aggregate { Article { meta { count } } } }"}`)))
	if err != nil {
		t.Fatalf("query.save: %v", err)
	}
	id := text(saved.(row)["id"])
	if !strings.HasPrefix(id, savedQueryPrefix) {
		t.Fatalf("unexpected saved query id %q", id)
	}
	listed, err := routes[rid("queries.list")].Handle(newRC(nil, nil))
	if err != nil || len(listed.(plugin.Page[row]).Items) != 1 {
		t.Fatalf("query list: %#v %v", listed, err)
	}
	updated, err := routes[rid("query.update")].Handle(newRC(map[string]string{"id": id}, []byte(`{"content":"{ Aggregate { Author { meta { count } } } }"}`)))
	if err != nil {
		t.Fatalf("query.update: %v", err)
	}
	if !strings.Contains(text(updated.(row)["content"]), "Author") {
		t.Fatalf("update should return canonical content: %#v", updated)
	}
	if _, err := routes[rid("query.read")].Handle(newRC(map[string]string{"id": id}, nil)); err != nil {
		t.Fatalf("query.read: %v", err)
	}
	if _, err := routes[rid("query.delete")].Handle(newRC(map[string]string{"id": id}, nil)); err != nil {
		t.Fatalf("query.delete: %v", err)
	}
	if _, err := routes[rid("query.read")].Handle(newRC(map[string]string{"id": id}, nil)); !errors.Is(err, plugin.ErrNotFound) {
		t.Fatalf("deleted query should be gone, got %v", err)
	}
	if _, err := routes[rid("query.read")].Handle(newRC(map[string]string{"id": "../etc"}, nil)); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("saved query ids must be namespaced, got %v", err)
	}
}

func TestActivityFeedRecordsMutations(t *testing.T) {
	store := &fakeStorage{}
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, nil, nil, nil, nil).WithStorage(store)
	record(rc, "schema", plugin.SeveritySuccess, "Collection created", "Article")
	record(rc, "query", plugin.SeverityInfo, "GraphQL query executed", "{ Get { Article } }")
	out, err := listActivity(rc)
	if err != nil {
		t.Fatalf("activity.list: %v", err)
	}
	items := out.(plugin.Page[row]).Items
	if len(items) != 2 {
		t.Fatalf("expected two events, got %#v", items)
	}
	if items[0]["title"] != "GraphQL query executed" {
		t.Fatalf("events should be newest first: %#v", items)
	}
	if items[0]["icon"] != "search" {
		t.Fatalf("category icon missing: %#v", items[0])
	}
}

func TestMapErrorTranslatesWeaviateBodies(t *testing.T) {
	sess, _ := newTestSession(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/.well-known/ready":
			w.WriteHeader(http.StatusOK)
			return
		case "/v1/meta":
			_, _ = w.Write([]byte(`{"version":"1.38.6"}`))
			return
		}
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"error":[{"message":"class name Article already exists"}]}`))
	})
	routes := plugintest.RouteMap(New().Routes())
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess, nil, nil, []byte(`{"name":"Article","vectorizer":"none"}`))
	_, err := routes[rid("collection.create")].Handle(rc)
	if !errors.Is(err, plugin.ErrInvalidInput) || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("unexpected error mapping: %v", err)
	}
}

func TestMapErrorUnwrapsGraphQLStatusCodes(t *testing.T) {
	sess, _ := newTestSession(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/graphql" {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":[{"message":"forbidden"}]}`))
			return
		}
		fixtureHandler(t)(w, r)
	})
	routes := plugintest.RouteMap(New().Routes())
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess, map[string]string{"collection": "Article"}, nil, nil)
	if _, err := routes[rid("objects.list")].Handle(rc); !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("a 403 from /v1/graphql must map to ErrForbidden, got %v", err)
	}
}
