package weaviate

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

const weaviateImage = "cr.weaviate.io/semitechnologies/weaviate:1.38.6"

const (
	authorID = "aaaaaaaa-0000-4000-8000-000000000001"
	docOneID = "dddddddd-0000-4000-8000-000000000001"
	docTwoID = "dddddddd-0000-4000-8000-000000000002"
	docTreID = "dddddddd-0000-4000-8000-000000000003"
)

type integrationFixture struct {
	doc     string
	author  string
	tenants string
	backup  string
	backupI string
}

func TestWeaviatePluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_WEAVIATE_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_WEAVIATE_INTEGRATION=1 to run against a real Weaviate")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	endpoint := weaviateEndpoint(ctx, t)
	p := New()
	sess, err := p.Connect(ctx, plugin.ConnectConfig{
		Config: map[string]any{
			"endpoint":          endpoint,
			"auth":              "none",
			"tls_mode":          "disable",
			"read_only":         false,
			"consistency_level": "ONE",
			"page_limit":        100,
			"vector_sample":     50,
			"timeout":           "60s",
		},
		Net: plugintest.DirectTransport(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()

	h := contribtest.NewHarness(t, p.Routes())
	store := &fakeStorage{}
	suffix := time.Now().UTC().Format("20060102150405")
	fx := integrationFixture{
		doc:     "ShellcnDoc" + suffix,
		author:  "ShellcnAuthor" + suffix,
		tenants: "ShellcnTenant" + suffix,
		backup:  "ShellcnArchive" + suffix,
		backupI: "shellcn-it-" + suffix,
	}
	t.Cleanup(func() {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancelCleanup()
		for _, class := range []string{fx.doc, fx.author, fx.tenants, fx.backup} {
			h.CallNoFail(cleanupCtx, rid("collection.delete"), sess, map[string]string{"collection": class})
		}
	})

	integrationSchema(ctx, t, h, sess, fx)
	integrationObjects(ctx, t, h, sess, fx)
	integrationReferences(ctx, t, h, sess, fx)
	integrationQueries(ctx, t, h, sess, fx)
	integrationEditorDefaults(ctx, t, h, sess, fx)
	integrationCluster(ctx, t, h, sess, fx)
	integrationTenants(ctx, t, h, sess, fx)
	integrationAliases(ctx, t, h, sess, fx)
	integrationBackups(ctx, t, h, sess, fx)
	integrationStorage(ctx, t, h, sess, store, fx)
	integrationStreams(ctx, t, h, sess, fx)
	integrationObjectDelete(ctx, t, h, sess, fx)

	h.AssertAllCovered()
}

func integrationSchema(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, fx integrationFixture) {
	t.Helper()
	for _, class := range []string{fx.doc, fx.author, fx.backup} {
		h.Call(ctx, rid("collection.create"), sess, nil, nil, mustJSON(t, map[string]any{
			"name": class, "vectorizer": "none", "vector_index_type": "hnsw", "distance": "cosine",
			"description": "ShellCN integration fixture",
		}))
	}
	h.Call(ctx, rid("collection.create"), sess, nil, nil, mustJSON(t, map[string]any{
		"name": fx.tenants, "vectorizer": "none", "multi_tenancy": true, "auto_tenant_creation": false,
	}))

	h.Call(ctx, rid("property.create"), sess, map[string]string{"collection": fx.author}, nil,
		mustJSON(t, map[string]any{"name": "name", "data_type": "text"}))
	h.Call(ctx, rid("property.create"), sess, map[string]string{"collection": fx.doc}, nil,
		mustJSON(t, map[string]any{"name": "title", "data_type": "text", "tokenization": "word"}))
	h.Call(ctx, rid("property.create"), sess, map[string]string{"collection": fx.doc}, nil,
		mustJSON(t, map[string]any{"name": "rank", "data_type": "int"}))
	h.Call(ctx, rid("property.create"), sess, map[string]string{"collection": fx.doc}, nil,
		mustJSON(t, map[string]any{"name": "author", "data_type": fx.author}))

	properties := pageItems(t, h.Call(ctx, rid("properties.list"), sess, map[string]string{"collection": fx.doc}, nil, nil))
	kinds := map[string]string{}
	for _, item := range properties {
		kinds[text(item["name"])] = text(item["kind"])
	}
	if kinds["title"] != "primitive" || kinds["author"] != "reference" {
		t.Fatalf("unexpected property kinds: %#v", kinds)
	}

	columns := pageItems(t, h.Call(ctx, rid("collection.columns"), sess, map[string]string{"collection": fx.doc}, nil, nil))
	editors := map[string]any{}
	for _, item := range columns {
		editors[text(item["name"])] = item["editor"]
	}
	if editors["rank"] != string(plugin.ColumnEditorNumber) {
		t.Fatalf("int property should use the number editor: %#v", editors)
	}

	detail := asRow(t, h.Call(ctx, rid("collection.read"), sess, map[string]string{"collection": fx.doc}, nil, nil))
	if detail["vectorizer"] != "none" || detail["references"] != 1 {
		t.Fatalf("unexpected collection detail: %#v", detail)
	}

	definition := asRow(t, h.Call(ctx, rid("collection.definition"), sess, map[string]string{"collection": fx.doc}, nil, nil))
	var class map[string]any
	if err := json.Unmarshal([]byte(text(definition["content"])), &class); err != nil {
		t.Fatalf("definition is not JSON: %v", err)
	}
	class["description"] = "updated by the ShellCN integration test"
	updated := asRow(t, h.Call(ctx, rid("collection.update"), sess, map[string]string{"collection": fx.doc}, nil,
		mustJSON(t, map[string]any{"class": class})))
	if !strings.Contains(text(updated["content"]), "updated by the ShellCN integration test") {
		t.Fatalf("collection.update did not return the canonical definition: %#v", updated)
	}

	tree := pageItems(t, h.Call(ctx, rid("collections.tree"), sess, nil, nil, nil))
	if !containsField(tree, "label", fx.doc) {
		t.Fatalf("collection tree is missing %s: %#v", fx.doc, tree)
	}
	collections := pageItems(t, h.Call(ctx, rid("collections.list"), sess, nil, nil, nil))
	if !containsField(collections, "name", fx.doc) {
		t.Fatalf("collection list is missing %s", fx.doc)
	}

	graph := asGraph(t, h.Call(ctx, rid("schema.graph"), sess, nil, nil, nil))
	if !hasEdge(graph, classNodeID(fx.doc), classNodeID(fx.author), "author") {
		t.Fatalf("schema graph is missing the cross-reference edge: %#v", graph.Edges)
	}
	expanded := asGraph(t, h.Call(ctx, rid("schema.graph.expand"), sess, map[string]string{"node": classNodeID(fx.doc)}, nil, nil))
	if len(expanded.Nodes) < 2 {
		t.Fatalf("expanded schema graph should include the reference target: %#v", expanded.Nodes)
	}
}

func integrationObjects(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, fx integrationFixture) {
	t.Helper()
	h.Call(ctx, rid("object.create"), sess, map[string]string{"collection": fx.author}, nil,
		mustJSON(t, map[string]any{"id": authorID, "vector": []float64{1, 0, 0}, "properties": map[string]any{"name": "Ada Lovelace"}}))
	seeds := []struct {
		id     string
		title  string
		rank   int
		vector []float64
	}{
		{docOneID, "Vector databases", 1, []float64{0.9, 0.1, 0.05}},
		{docTwoID, "Hybrid retrieval", 2, []float64{0.85, 0.2, 0.1}},
		{docTreID, "Graph traversal", 3, []float64{-0.8, 0.15, -0.1}},
	}
	for _, seed := range seeds {
		h.Call(ctx, rid("object.create"), sess, map[string]string{"collection": fx.doc}, nil, mustJSON(t, map[string]any{
			"id": seed.id, "vector": seed.vector, "title": seed.title, "rank": seed.rank,
		}))
	}
	waitForObjects(ctx, t, h, sess, fx.doc, len(seeds))

	sorted := pageItems(t, h.Call(ctx, rid("objects.list"), sess, map[string]string{"collection": fx.doc},
		url.Values{"sort": []string{"-rank"}, "limit": []string{"10"}}, nil))
	if len(sorted) != 3 || text(sorted[0]["title"]) != "Graph traversal" {
		t.Fatalf("server-side descending sort failed: %#v", sorted)
	}
	filtered := pageItems(t, h.Call(ctx, rid("objects.list"), sess, map[string]string{"collection": fx.doc},
		url.Values{"filter": []string{"hybrid"}}, nil))
	if len(filtered) != 1 || text(filtered[0]["title"]) != "Hybrid retrieval" {
		t.Fatalf("free-text filter failed: %#v", filtered)
	}
	paged := h.Call(ctx, rid("objects.list"), sess, map[string]string{"collection": fx.doc}, url.Values{"limit": []string{"2"}}, nil)
	page := paged.(plugin.Page[row])
	if page.NextCursor != "2" || len(page.Items) != 2 {
		t.Fatalf("cursor paging failed: %q %d", page.NextCursor, len(page.Items))
	}

	object := asRow(t, h.Call(ctx, rid("object.read"), sess, map[string]string{"collection": fx.doc, "id": docOneID}, nil, nil))
	if object["vectorDims"] != 3 || object["id"] != docOneID {
		t.Fatalf("unexpected object detail: %#v", object)
	}
	document := asRow(t, h.Call(ctx, rid("object.document"), sess, map[string]string{"collection": fx.doc, "id": docOneID}, nil, nil))
	if !strings.Contains(text(document["content"]), "Vector databases") {
		t.Fatalf("object document missing properties: %#v", document)
	}
	// A properties document is a replace: the omitted rank goes away, the stored
	// vector of a vectorizer:none collection does not.
	updated := asRow(t, h.Call(ctx, rid("object.update"), sess, map[string]string{"collection": fx.doc, "id": docOneID}, nil,
		mustJSON(t, map[string]any{"properties": map[string]any{"title": "Vector databases, 2nd edition"}})))
	if !strings.Contains(text(updated["content"]), "2nd edition") {
		t.Fatalf("object.update did not return the canonical document: %#v", updated)
	}
	replaced := asRow(t, h.Call(ctx, rid("object.read"), sess, map[string]string{"collection": fx.doc, "id": docOneID}, nil, nil))
	if replaced["vectorDims"] != 3 {
		t.Fatalf("a replacing update must keep the stored vector: %#v", replaced)
	}
	properties, _ := replaced["properties"].(map[string]any)
	if _, ok := properties["rank"]; ok {
		t.Fatalf("a replacing update must drop the properties the document omits: %#v", properties)
	}

	// A grid cell edit is a merge: the sibling properties survive.
	merged := asRow(t, h.Call(ctx, rid("object.update"), sess, map[string]string{"collection": fx.doc, "id": docTwoID}, nil,
		mustJSON(t, map[string]any{"id": docTwoID, "title": "Hybrid retrieval, revised"})))
	if !strings.Contains(text(merged["content"]), `"rank"`) {
		t.Fatalf("a single-cell grid edit must merge, not replace: %#v", merged)
	}
}

func integrationReferences(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, fx integrationFixture) {
	t.Helper()
	params := map[string]string{"collection": fx.doc, "id": docOneID}
	body := mustJSON(t, map[string]any{"property": "author", "target_id": authorID})
	h.Call(ctx, rid("reference.create"), sess, params, nil, body)

	graph := asGraph(t, h.Call(ctx, rid("object.references"), sess, params, nil, nil))
	if !hasEdge(graph, objectNodeID(fx.doc, docOneID), objectNodeID(fx.author, authorID), "author") {
		t.Fatalf("object reference graph is missing the outbound edge: %#v", graph.Edges)
	}
	inbound := asGraph(t, h.Call(ctx, rid("object.references.expand"), sess,
		map[string]string{"collection": fx.author, "id": authorID, "node": objectNodeID(fx.author, authorID)}, nil, nil))
	if !hasEdge(inbound, objectNodeID(fx.doc, docOneID), objectNodeID(fx.author, authorID), "author") {
		t.Fatalf("expanded graph is missing the inbound edge: %#v", inbound.Edges)
	}
	h.Call(ctx, rid("reference.delete"), sess, params, nil, body)
	after := asGraph(t, h.Call(ctx, rid("object.references"), sess, params, nil, nil))
	if hasEdge(after, objectNodeID(fx.doc, docOneID), objectNodeID(fx.author, authorID), "author") {
		t.Fatalf("reference should be gone: %#v", after.Edges)
	}
}

func integrationQueries(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, fx integrationFixture) {
	t.Helper()
	graphqlOut := h.Stream(ctx, rid("graphql"), sess, nil, nil, mustJSON(t, map[string]any{
		"query": fmt.Sprintf("{ Get { %s(limit: 5) { title rank } } }", fx.doc),
	}))
	if !strings.Contains(string(graphqlOut), "Graph traversal") {
		t.Fatalf("unexpected GraphQL stream output: %s", graphqlOut)
	}

	frames := strings.Join([]string{
		string(mustJSON(t, map[string]any{"query": `{"mode":"nearVector","vector":[0.9,0.1,0.05],"limit":3}`})),
		string(mustJSON(t, map[string]any{"query": `{"mode":"bm25","query":"hybrid","properties":["title"],"limit":3}`})),
		string(mustJSON(t, map[string]any{"query": `{"mode":"hybrid","query":"graph","vector":[-0.8,0.15,-0.1],"alpha":0.4,"limit":3}`})),
		string(mustJSON(t, map[string]any{"query": `{"mode":"fetch","limit":2,"where":{"operator":"GreaterThanEqual","path":["rank"],"valueInt":2},"sort":[{"path":["rank"],"order":"asc"}]}`})),
		string(mustJSON(t, map[string]any{"query": `{"mode":"nearVector"}`})),
	}, "\n") + "\n"
	searchOut := h.Stream(ctx, rid("search"), sess, map[string]string{"collection": fx.doc}, nil, []byte(frames))
	results := decodeFrames(t, searchOut)
	if len(results) != 5 {
		t.Fatalf("expected five search frames, got %d: %s", len(results), searchOut)
	}
	if rowCount(results[0]) != 3 {
		t.Fatalf("nearVector should return every seeded doc: %#v", results[0])
	}
	if rowCount(results[1]) != 1 {
		t.Fatalf("bm25 should match one document: %#v", results[1])
	}
	if rowCount(results[2]) == 0 {
		t.Fatalf("hybrid search returned nothing: %#v", results[2])
	}
	if rowCount(results[3]) != 2 {
		t.Fatalf("filtered fetch should return two documents: %#v", results[3])
	}
	if results[4]["error"] == nil {
		t.Fatalf("nearVector without a vector must return an error frame: %#v", results[4])
	}

	completions := pageItems(t, h.Call(ctx, rid("graphql.complete"), sess, map[string]string{"collection": fx.doc}, nil, nil))
	if !containsField(completions, "label", fx.doc) || !containsField(completions, "label", "title") {
		t.Fatalf("completion route should suggest the collection and its properties")
	}
}

// integrationEditorDefaults runs the query editors' shipped defaults the way the
// panel does on open: resource tokens interpolated, one frame in, one frame out.
func integrationEditorDefaults(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, fx integrationFixture) {
	t.Helper()
	interpolate := strings.NewReplacer(
		"${resource.name}", fx.doc,
		"${resource.uid}", fx.doc,
		"${resource.kind}", "collection",
		"${resource.namespace}", "",
		"${resource.scope}", "",
	).Replace

	run := func(label, route string, params map[string]string, query string) map[string]any {
		t.Helper()
		out := h.Stream(ctx, route, sess, params, nil, mustJSON(t, map[string]any{"query": query, "confirm": false}))
		frames := decodeFrames(t, out)
		if len(frames) != 1 {
			t.Fatalf("%s default should produce one frame: %s", label, out)
		}
		if frames[0]["error"] != nil {
			t.Fatalf("%s default must run cleanly: %s", label, out)
		}
		if rowCount(frames[0]) == 0 {
			t.Fatalf("%s default should return rows: %s", label, out)
		}
		return frames[0]
	}

	graphqlFrame := run("GraphQL editor", rid("graphql"), nil, interpolate(graphqlEditorConfig().InitialQuery))
	if !strings.Contains(fmt.Sprint(graphqlFrame["rows"]), fx.doc) {
		t.Fatalf("GraphQL editor default should list %s: %#v", fx.doc, graphqlFrame)
	}
	searchFrame := run("Vector search editor", rid("search"), map[string]string{"collection": fx.doc},
		interpolate(searchEditorConfig().InitialQuery))
	if rowCount(searchFrame) != 3 {
		t.Fatalf("vector search default should return every seeded object: %#v", searchFrame)
	}
}

func integrationCluster(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, fx integrationFixture) {
	t.Helper()
	overview := asRow(t, h.Call(ctx, rid("overview"), sess, nil, nil, nil))
	if text(overview["version"]) == "" || overview["nodes"] == nil {
		t.Fatalf("unexpected overview: %#v", overview)
	}
	modules := pageItems(t, h.Call(ctx, rid("modules.list"), sess, nil, nil, nil))
	if !containsField(modules, "name", "backup-filesystem") {
		t.Fatalf("backup-filesystem module should be enabled: %#v", modules)
	}

	nodes := pageItems(t, h.Call(ctx, rid("nodes.list"), sess, nil, nil, nil))
	if len(nodes) == 0 {
		t.Fatal("cluster reported no nodes")
	}
	nodeName := text(nodes[0]["name"])
	node := asRow(t, h.Call(ctx, rid("node.read"), sess, map[string]string{"node": nodeName}, nil, nil))
	if text(node["status"]) != "HEALTHY" {
		t.Fatalf("node should be healthy: %#v", node)
	}
	nodeShards := pageItems(t, h.Call(ctx, rid("node.shards"), sess, map[string]string{"node": nodeName}, nil, nil))
	if len(nodeShards) == 0 {
		t.Fatal("node should host shards")
	}

	shards := pageItems(t, h.Call(ctx, rid("shards.list"), sess, map[string]string{"collection": fx.doc}, nil, nil))
	if len(shards) == 0 {
		t.Fatalf("collection %s has no shards", fx.doc)
	}
	shardName := text(shards[0]["name"])
	readonly := asRow(t, h.Call(ctx, rid("shard.update"), sess, map[string]string{"collection": fx.doc, "shard": shardName}, nil,
		mustJSON(t, map[string]any{"status": "READONLY"})))
	if text(readonly["status"]) != "READONLY" {
		t.Fatalf("shard should be READONLY: %#v", readonly)
	}
	h.Call(ctx, rid("shard.update"), sess, map[string]string{"collection": fx.doc, "shard": shardName}, nil,
		mustJSON(t, map[string]any{"status": "READY"}))
}

func integrationTenants(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, fx integrationFixture) {
	t.Helper()
	params := map[string]string{"collection": fx.tenants}
	h.Call(ctx, rid("tenant.create"), sess, params, nil, mustJSON(t, map[string]any{"name": "acme", "activity_status": "ACTIVE"}))
	tenants := pageItems(t, h.Call(ctx, rid("tenants.list"), sess, params, nil, nil))
	if !containsField(tenants, "name", "acme") {
		t.Fatalf("tenant list is missing acme: %#v", tenants)
	}
	updated := asRow(t, h.Call(ctx, rid("tenant.update"), sess,
		map[string]string{"collection": fx.tenants, "tenant": "acme"}, nil,
		mustJSON(t, map[string]any{"activity_status": "INACTIVE"})))
	if text(updated["activityStatus"]) != "INACTIVE" {
		t.Fatalf("tenant should be INACTIVE: %#v", updated)
	}
	h.Call(ctx, rid("tenant.delete"), sess, map[string]string{"collection": fx.tenants, "tenant": "acme"}, nil, nil)
	remaining := pageItems(t, h.Call(ctx, rid("tenants.list"), sess, params, nil, nil))
	if containsField(remaining, "name", "acme") {
		t.Fatalf("tenant should be deleted: %#v", remaining)
	}
}

func integrationAliases(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, fx integrationFixture) {
	t.Helper()
	alias := "ShellcnAliasFor" + strings.TrimPrefix(fx.doc, "Shellcn")
	h.Call(ctx, rid("alias.create"), sess, nil, nil, mustJSON(t, map[string]any{"name": alias, "collection": fx.doc}))
	aliases := pageItems(t, h.Call(ctx, rid("aliases.list"), sess, nil, nil, nil))
	if !containsField(aliases, "name", alias) {
		t.Fatalf("alias list is missing %s: %#v", alias, aliases)
	}
	read := asRow(t, h.Call(ctx, rid("alias.read"), sess, map[string]string{"alias": alias}, nil, nil))
	if text(read["collection"]) != fx.doc {
		t.Fatalf("alias should target %s: %#v", fx.doc, read)
	}
	h.Call(ctx, rid("alias.update"), sess, map[string]string{"alias": alias}, nil, mustJSON(t, map[string]any{"collection": fx.author}))
	retargeted := asRow(t, h.Call(ctx, rid("alias.read"), sess, map[string]string{"alias": alias}, nil, nil))
	if text(retargeted["collection"]) != fx.author {
		t.Fatalf("alias should now target %s: %#v", fx.author, retargeted)
	}
	h.Call(ctx, rid("alias.delete"), sess, map[string]string{"alias": alias}, nil, nil)
}

func integrationBackups(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, fx integrationFixture) {
	t.Helper()
	created := asRow(t, h.Call(ctx, rid("backup.create"), sess, nil, nil, mustJSON(t, map[string]any{
		"id": fx.backupI, "backend": "filesystem", "collections": []string{fx.backup}, "wait": true,
	})))
	if text(created["status"]) != "SUCCESS" {
		t.Fatalf("backup should complete: %#v", created)
	}
	backups := pageItems(t, h.Call(ctx, rid("backups.list"), sess, nil, nil, nil))
	if !containsField(backups, "id", fx.backupI) {
		t.Fatalf("backup list is missing %s: %#v", fx.backupI, backups)
	}
	params := map[string]string{"backend": "filesystem", "backup": fx.backupI}
	status := asRow(t, h.Call(ctx, rid("backup.read"), sess, params, nil, nil))
	if text(status["status"]) != "SUCCESS" {
		t.Fatalf("backup status should be SUCCESS: %#v", status)
	}
	// Cancelling a finished backup is rejected upstream; the call still exercises the route.
	h.CallNoFail(ctx, rid("backup.cancel"), sess, params)

	h.Call(ctx, rid("collection.delete"), sess, map[string]string{"collection": fx.backup}, nil, nil)
	restored := asRow(t, h.Call(ctx, rid("backup.restore"), sess, params, nil, nil))
	if text(restored["status"]) != "SUCCESS" {
		t.Fatalf("restore should complete: %#v", restored)
	}
	collections := pageItems(t, h.Call(ctx, rid("collections.list"), sess, nil, nil, nil))
	if !containsField(collections, "name", fx.backup) {
		t.Fatalf("restored collection %s is missing: %#v", fx.backup, collections)
	}
}

func integrationStorage(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, store *fakeStorage, fx integrationFixture) {
	t.Helper()
	withStorage := func(id string, params map[string]string, body []byte) any {
		t.Helper()
		route := h.Route(id)
		out, err := route.Handle(plugin.NewRequestContext(ctx, plugin.User{}, sess, params, nil, body).WithStorage(store))
		if err != nil {
			t.Fatalf("%s: %v", id, err)
		}
		return out
	}

	saved := asRow(t, withStorage(rid("query.save"), nil, mustJSON(t, map[string]any{
		"name": "Recent documents", "kind": "search", "collection": fx.doc,
		"query": `{"mode":"fetch","limit":5}`,
	})))
	id := text(saved["id"])
	queries := pageItems(t, withStorage(rid("queries.list"), nil, nil))
	if !containsField(queries, "name", "Recent documents") {
		t.Fatalf("saved query list is missing the record: %#v", queries)
	}
	read := asRow(t, withStorage(rid("query.read"), map[string]string{"id": id}, nil))
	if !strings.Contains(text(read["content"]), `"mode":"fetch"`) {
		t.Fatalf("saved query content is wrong: %#v", read)
	}
	updated := asRow(t, withStorage(rid("query.update"), map[string]string{"id": id},
		mustJSON(t, map[string]any{"content": `{"mode":"hybrid","query":"vector","limit":5}`})))
	if !strings.Contains(text(updated["content"]), "hybrid") {
		t.Fatalf("saved query update failed: %#v", updated)
	}
	withStorage(rid("query.delete"), map[string]string{"id": id}, nil)

	// A mutation through a storage-backed context must land in the activity feed.
	withStorage(rid("property.create"), map[string]string{"collection": fx.doc},
		mustJSON(t, map[string]any{"name": "summary", "data_type": "text"}))
	activity := pageItems(t, withStorage(rid("activity.list"), nil, nil))
	if len(activity) == 0 || text(activity[0]["category"]) != "schema" {
		t.Fatalf("activity feed should record the schema change: %#v", activity)
	}
}

func integrationStreams(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, fx integrationFixture) {
	t.Helper()
	metricsCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	metricsOut := h.Stream(metricsCtx, rid("metrics"), sess, nil, nil, nil)
	frames := decodeFrames(t, metricsOut)
	if len(frames) == 0 || frames[0]["nodes"] == nil {
		t.Fatalf("metrics stream should emit a first frame immediately: %s", metricsOut)
	}

	watchCtx, cancelWatch := context.WithTimeout(ctx, 2*time.Second)
	defer cancelWatch()
	watchOut := h.Stream(watchCtx, rid("resource.watch"), sess, map[string]string{"kind": "collection"}, nil, nil)
	events := decodeFrames(t, watchOut)
	if len(events) == 0 {
		t.Fatalf("resource watch emitted no events: %s", watchOut)
	}
	seen := false
	for _, event := range events {
		ref, _ := event["ref"].(map[string]any)
		if event["type"] == "added" && text(ref["uid"]) == fx.doc {
			seen = true
		}
	}
	if !seen {
		t.Fatalf("resource watch did not announce %s: %s", fx.doc, watchOut)
	}

	ready := mustJSON(t, map[string]any{"type": "ready", "width": 900, "height": 600, "dpr": 1, "theme": "dark"})
	canvasOut := h.Stream(ctx, rid("embedding"), sess, map[string]string{"collection": fx.doc}, nil, append(ready, '\n'))
	canvasFrames := decodeFrames(t, canvasOut)
	if len(canvasFrames) == 0 {
		t.Fatalf("embedding canvas emitted no frames: %s", canvasOut)
	}
	last := canvasFrames[len(canvasFrames)-1]
	regions, _ := last["regions"].([]any)
	if len(regions) != 3 {
		t.Fatalf("embedding projection should map three objects to regions: %s", canvasOut)
	}
	if commands, _ := last["commands"].([]any); len(commands) == 0 {
		t.Fatalf("embedding canvas frame has no draw commands: %#v", last)
	}
}

func integrationObjectDelete(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, fx integrationFixture) {
	t.Helper()
	params := map[string]string{"collection": fx.doc}
	h.Call(ctx, rid("object.delete"), sess, map[string]string{"collection": fx.doc, "id": docTreID}, nil, nil)
	waitForObjectGone(ctx, t, h, sess, fx.doc, docTreID, 2)

	filter := map[string]any{"operator": "GreaterThanEqual", "path": []string{"rank"}, "valueInt": 2}
	dry := asRow(t, h.Call(ctx, rid("objects.delete"), sess, params, nil,
		mustJSON(t, map[string]any{"where": filter, "dry_run": true})))
	if dry["matches"] != int64(1) {
		t.Fatalf("dry run should match the one remaining ranked object: %#v", dry)
	}
	if items := pageItems(t, h.Call(ctx, rid("objects.list"), sess, params, nil, nil)); !containsField(items, "id", docTwoID) {
		t.Fatalf("a dry run must not delete anything: %#v", items)
	}
	deleted := asRow(t, h.Call(ctx, rid("objects.delete"), sess, params, nil, mustJSON(t, map[string]any{"where": filter})))
	if deleted["matches"] != int64(1) || deleted["successful"] != int64(1) || deleted["failed"] != int64(0) {
		t.Fatalf("batch delete should remove exactly the matching object: %#v", deleted)
	}
	waitForObjectGone(ctx, t, h, sess, fx.doc, docTwoID, 1)
}

func waitForObjectGone(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, class, id string, want int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		items := pageItems(t, h.Call(ctx, rid("objects.list"), sess, map[string]string{"collection": class}, nil, nil))
		if !containsField(items, "id", id) {
			if len(items) != want {
				t.Fatalf("expected %d remaining objects in %s, got %d", want, class, len(items))
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("deleted object %s is still listed", id)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func waitForObjects(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, class string, want int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		items := pageItems(t, h.Call(ctx, rid("objects.list"), sess, map[string]string{"collection": class}, nil, nil))
		if len(items) >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d/%d objects became visible in %s", len(items), want, class)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func weaviateEndpoint(ctx context.Context, t *testing.T) string {
	t.Helper()
	if endpoint := os.Getenv("SHELLCN_WEAVIATE_ENDPOINT"); endpoint != "" {
		return endpoint
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_WEAVIATE_ENDPOINT is not set")
	}
	name := "shellcn-weaviate-it-" + time.Now().UTC().Format("20060102150405")
	args := []string{
		"run", "-d", "--rm", "--name", name, "-p", "127.0.0.1::8080",
		"-e", "AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true",
		"-e", "PERSISTENCE_DATA_PATH=/var/lib/weaviate",
		"-e", "DEFAULT_VECTORIZER_MODULE=none",
		"-e", "ENABLE_MODULES=backup-filesystem",
		"-e", "BACKUP_FILESYSTEM_PATH=/var/lib/weaviate/backups",
		"-e", "CLUSTER_HOSTNAME=node1",
		weaviateImage,
		"--host", "0.0.0.0", "--port", "8080", "--scheme", "http",
	}
	if out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput(); err != nil {
		t.Skipf("could not start %s: %v\n%s", weaviateImage, err, out)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})
	out, err := exec.CommandContext(ctx, "docker", "port", name, "8080/tcp").CombinedOutput()
	if err != nil {
		t.Fatalf("docker port: %v\n%s", err, out)
	}
	host, port, err := net.SplitHostPort(strings.TrimSpace(strings.Split(strings.TrimSpace(string(out)), "\n")[0]))
	if err != nil {
		t.Fatalf("unexpected docker port output %q: %v", out, err)
	}
	endpoint := "http://" + net.JoinHostPort(host, port)
	waitForReady(ctx, t, endpoint)
	return endpoint
}

func waitForReady(ctx context.Context, t *testing.T, endpoint string) {
	t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/v1/.well-known/ready", nil)
		if err != nil {
			t.Fatalf("build readiness request: %v", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("Weaviate at %s did not become ready: %v", endpoint, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return data
}

func asRow(t *testing.T, value any) row {
	t.Helper()
	item, ok := value.(row)
	if !ok {
		t.Fatalf("expected an object result, got %T", value)
	}
	return item
}

func asGraph(t *testing.T, value any) graphResult {
	t.Helper()
	graph, ok := value.(graphResult)
	if !ok {
		t.Fatalf("expected a graph result, got %T", value)
	}
	return graph
}

func pageItems(t *testing.T, value any) []map[string]any {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal page: %v", err)
	}
	var decoded struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("decode page: %v", err)
	}
	return decoded.Items
}

func containsField(items []map[string]any, key, want string) bool {
	for _, item := range items {
		if text(item[key]) == want {
			return true
		}
	}
	return false
}

func hasEdge(graph graphResult, source, target, label string) bool {
	for _, edge := range graph.Edges {
		if edge.Source == source && edge.Target == target && edge.Label == label {
			return true
		}
	}
	return false
}

func decodeFrames(t *testing.T, raw []byte) []map[string]any {
	t.Helper()
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	out := make([]map[string]any, 0, 4)
	for decoder.More() {
		var frame map[string]any
		if err := decoder.Decode(&frame); err != nil {
			t.Fatalf("decode frame: %v (raw: %s)", err, raw)
		}
		out = append(out, frame)
	}
	return out
}

func rowCount(frame map[string]any) int {
	count, _ := numberOf(frame["rowCount"])
	return int(count)
}
