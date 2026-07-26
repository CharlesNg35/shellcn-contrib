package couchdb

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

	contribtest "github.com/charlesng35/shellcn-contrib/shared/plugintest"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

const (
	integrationImage    = "couchdb:3.5.2"
	integrationUser     = "admin"
	integrationPassword = "shellcn-integration"
)

// TestCouchDBPluginIntegration drives every declared route against a real
// CouchDB container through the same handlers the gateway calls.
func TestCouchDBPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_COUCHDB_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_COUCHDB_INTEGRATION=1 to run against CouchDB")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	endpoint := couchEndpoint(ctx, t)
	host, port := splitHostPort(t, endpoint)

	p := New()
	sess, err := p.Connect(ctx, plugin.ConnectConfig{
		ConnectionID: "integration",
		Transport:    plugin.TransportDirect,
		Net:          plugintest.DirectTransport(),
		Config: map[string]any{
			"host": host, "port": float64(port),
			"auth": "basic", "username": integrationUser, "password": integrationPassword,
			"tls_mode": "disable", "read_only": false, "page_limit": 100, "timeout": "30s",
		},
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	if err := sess.HealthCheck(ctx); err != nil {
		t.Fatalf("health check: %v", err)
	}

	h := contribtest.NewHarness(t, p.Routes())
	db := "shellcn_it_" + time.Now().UTC().Format("20060102150405")
	backup := db + "_backup"

	integrationServer(ctx, t, h, sess)
	integrationDatabase(ctx, t, h, sess, db)
	defer h.CallNoFail(context.Background(), rid("database.delete"), sess, map[string]string{"db": db})

	integrationDocuments(ctx, t, h, sess, db, endpoint)
	integrationMango(ctx, t, h, sess, db)
	integrationQueryEditorDefaults(ctx, t, h, sess, db)
	integrationDesign(ctx, t, h, sess, db)
	integrationReplication(ctx, t, h, sess, db, backup)
	integrationStreams(ctx, t, h, sess, db)
	integrationSavedQueries(ctx, t, h, sess, db)

	h.Call(ctx, rid("database.delete"), sess, map[string]string{"db": backup}, nil, nil)
	h.Call(ctx, rid("database.delete"), sess, map[string]string{"db": db}, nil, nil)
	h.AssertAllCovered()
}

func integrationServer(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session) {
	t.Helper()
	overview := h.Call(ctx, rid("server.overview"), sess, nil, nil, nil)
	if version := stringOf(field(overview, "version")); !strings.HasPrefix(version, "3.") {
		t.Fatalf("unexpected CouchDB version %q", version)
	}
	servers := pageItems(h.Call(ctx, rid("server.list"), sess, nil, nil, nil))
	if len(servers) != 1 || servers[0]["status"] != "ok" {
		t.Fatalf("unexpected server row: %#v", servers)
	}
	config := mustJSON(t, h.Call(ctx, rid("server.config"), sess, nil, nil, nil))
	if !strings.Contains(string(config), redactedValue) || strings.Contains(string(config), "pbkdf2") {
		t.Fatalf("the node configuration must not expose credential material: %s", config)
	}
	h.Call(ctx, rid("fauxton.url"), sess, nil, nil, nil)
	h.Call(ctx, rid("tasks.list"), sess, nil, nil, nil)
	h.Call(ctx, rid("nodes.tree"), sess, nil, nil, nil)

	nodes := pageItems(h.Call(ctx, rid("nodes.list"), sess, nil, nil, nil))
	if len(nodes) == 0 {
		t.Fatal("_membership reported no cluster nodes")
	}
	node := stringOf(nodes[0]["name"])
	read := h.Call(ctx, rid("node.read"), sess, map[string]string{"node": node}, nil, nil)
	if stringOf(field(read, "membership")) != "cluster" {
		t.Fatalf("unexpected node membership: %#v", read)
	}
	stats := h.Call(ctx, rid("node.stats"), sess, map[string]string{"node": node}, nil, nil)
	if mapOf(field(stats, "couchdb")) == nil {
		t.Fatalf("unexpected node statistics: %#v", stats)
	}
}

func integrationDatabase(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, db string) {
	t.Helper()
	h.Call(ctx, rid("database.create"), sess, nil, nil, mustJSON(t, map[string]any{
		"name": db, "shards": 1, "replicas": 1,
	}))

	databases := pageItems(h.Call(ctx, rid("databases.list"), sess, nil, nil, nil))
	if !containsName(databases, db) {
		t.Fatalf("the created database is not listed: %#v", databases)
	}
	tree := pageItems(h.Call(ctx, rid("databases.tree"), sess, nil, nil, nil))
	if !containsLabel(tree, db) {
		t.Fatalf("the created database is missing from the tree: %#v", tree)
	}
	info := h.Call(ctx, rid("database.read"), sess, map[string]string{"db": db}, nil, nil)
	if stringOf(field(info, "name")) != db {
		t.Fatalf("unexpected database info: %#v", info)
	}

	security := h.Call(ctx, rid("database.security.read"), sess, map[string]string{"db": db}, nil, nil)
	if mapOf(field(security, "members")) == nil {
		t.Fatalf("the security document should expose a members object: %#v", security)
	}
	updated := h.Call(ctx, rid("database.security.update"), sess, map[string]string{"db": db}, nil,
		mustJSON(t, map[string]any{"content": `{"admins":{"names":[],"roles":["_admin"]},"members":{"names":[],"roles":["reader"]}}`}))
	if !strings.Contains(stringOf(field(updated, "content")), "reader") {
		t.Fatalf("the security document was not stored: %#v", updated)
	}

	h.Call(ctx, rid("database.compact"), sess, map[string]string{"db": db}, nil, nil)
	h.Call(ctx, rid("database.viewcleanup"), sess, map[string]string{"db": db}, nil, nil)
}

func integrationDocuments(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, db, endpoint string) {
	t.Helper()
	params := map[string]string{"db": db}

	created := h.Call(ctx, rid("document.create"), sess, params, nil,
		mustJSON(t, map[string]any{"content": `{"_id":"order-1","kind":"order","total":12,"customer":"ada"}`}))
	if stringOf(field(created, "id")) != "order-1" {
		t.Fatalf("unexpected create result: %#v", created)
	}
	h.Call(ctx, rid("documents.insert"), sess, params, nil,
		mustJSON(t, map[string]any{"id": "order-2", "kind": "order", "total": 5, "customer": "grace"}))
	h.Call(ctx, rid("documents.bulk"), sess, params, nil, mustJSON(t, map[string]any{
		"content": `{"docs":[{"_id":"order-3","kind":"order","total":9,"customer":"hopper"}]}`,
	}))

	docs := pageItems(h.Call(ctx, rid("documents.list"), sess, params, nil, nil))
	if len(docs) != 3 {
		t.Fatalf("expected three documents, got %#v", docs)
	}
	columns := pageItems(h.Call(ctx, rid("documents.columns"), sess, params, nil, nil))
	if !containsName(columns, "customer") || !containsName(columns, "total") {
		t.Fatalf("discovered columns are missing document fields: %#v", columns)
	}

	order2 := findRow(docs, "id", "order-2")
	if order2 == nil {
		t.Fatalf("order-2 is missing from the grid: %#v", docs)
	}
	h.Call(ctx, rid("documents.update"), sess, params, nil, mustJSON(t, map[string]any{
		"id": "order-2", "rev": order2["rev"], "total": 21,
	}))
	merged := h.Call(ctx, rid("document.read"), sess, map[string]string{"db": db, "docid": "order-2"}, nil, nil)
	if numberOf(field(merged, "total")) != 21 || stringOf(field(merged, "customer")) != "grace" {
		t.Fatalf("the grid update must merge into the stored document: %#v", merged)
	}

	saved := h.Call(ctx, rid("document.update"), sess, map[string]string{"db": db, "docid": "order-1"}, nil,
		mustJSON(t, map[string]any{"content": `{"kind":"order","total":13,"customer":"ada"}`}))
	if !strings.Contains(stringOf(field(saved, "content")), `"total": 13`) {
		t.Fatalf("the editor baseline was not refreshed: %#v", saved)
	}

	revisions := pageItems(h.Call(ctx, rid("document.revisions"), sess, map[string]string{"db": db, "docid": "order-1"}, nil, nil))
	if len(revisions) < 2 {
		t.Fatalf("expected a multi-revision history, got %#v", revisions)
	}
	if empty := pageItems(h.Call(ctx, rid("document.conflicts"), sess, map[string]string{"db": db, "docid": "order-1"}, nil, nil)); len(empty) != 0 {
		t.Fatalf("order-1 should not be conflicted: %#v", empty)
	}

	// CouchDB only produces conflicts through replication, so the fixture is
	// written with new_edits=false, exactly how a replica would deliver it.
	seedConflict(ctx, t, endpoint, db)
	conflicts := pageItems(h.Call(ctx, rid("conflicts.list"), sess, params, nil, nil))
	if len(conflicts) != 1 || conflicts[0]["id"] != "conflicted" {
		t.Fatalf("the conflict scan did not find the seeded document: %#v", conflicts)
	}
	docConflicts := pageItems(h.Call(ctx, rid("document.conflicts"), sess, map[string]string{"db": db, "docid": "conflicted"}, nil, nil))
	if len(docConflicts) != 2 {
		t.Fatalf("expected two conflicting revisions, got %#v", docConflicts)
	}
	resolved := h.Call(ctx, rid("document.resolve"), sess, map[string]string{"db": db, "docid": "conflicted"}, nil,
		mustJSON(t, map[string]any{"keep": "winner"}))
	if len(sliceOf(field(resolved, "deleted"))) != 1 {
		t.Fatalf("conflict resolution should delete one revision: %#v", resolved)
	}
	if after := pageItems(h.Call(ctx, rid("conflicts.list"), sess, params, nil, nil)); len(after) != 0 {
		t.Fatalf("the database should be conflict-free after resolution: %#v", after)
	}

	h.Call(ctx, rid("documents.remove"), sess, params, nil, mustJSON(t, map[string]any{"id": "order-3"}))
	h.Call(ctx, rid("document.delete"), sess, map[string]string{"db": db, "docid": "order-2"}, nil, nil)

	// The attachment carries no "kind", so it stays out of the map/reduce view
	// and the Mango assertions that follow.
	attachParams := map[string]string{"db": db, "docid": "receipt"}
	h.Call(ctx, rid("document.create"), sess, params, nil, mustJSON(t, map[string]any{
		"content": `{"_id":"receipt","_attachments":{"note.txt":{"content_type":"text/plain","data":"aGVsbG8gc2hlbGxjbg=="}}}`,
	}))
	attachments := pageItems(h.Call(ctx, rid("attachments.list"), sess, attachParams, nil, nil))
	if len(attachments) != 1 || attachments[0]["name"] != "note.txt" || numberOf(attachments[0]["length"]) == 0 {
		t.Fatalf("unexpected attachment listing: %#v", attachments)
	}
	if stringOf(attachments[0]["content_type"]) != "text/plain" {
		t.Fatalf("the attachment content type was not reported: %#v", attachments[0])
	}
	h.Call(ctx, rid("attachment.delete"), sess, map[string]string{
		"db": db, "docid": "receipt", "name": "note.txt",
	}, nil, nil)
	if left := pageItems(h.Call(ctx, rid("attachments.list"), sess, attachParams, nil, nil)); len(left) != 0 {
		t.Fatalf("the attachment survived deletion: %#v", left)
	}

	h.Call(ctx, rid("document.create"), sess, params, nil,
		mustJSON(t, map[string]any{"content": `{"_id":"purge-me","kind":"scratch"}`}))
	purged := h.Call(ctx, rid("document.purge"), sess, map[string]string{"db": db, "docid": "purge-me"}, nil, nil)
	if numberOf(field(purged, "purged")) < 1 {
		t.Fatalf("unexpected purge result: %#v", purged)
	}
	if _, err := h.Route(rid("document.read")).Handle(plugin.NewRequestContext(ctx, plugin.User{}, sess,
		map[string]string{"db": db, "docid": "purge-me"}, nil, nil)); err == nil {
		t.Fatal("the purged document is still readable")
	}
}

func integrationMango(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, db string) {
	t.Helper()
	params := map[string]string{"db": db}

	h.Call(ctx, rid("index.create"), sess, params, nil, mustJSON(t, map[string]any{
		"name": "by-kind", "ddoc": "mango-indexes", "fields": []string{"kind"}, "type": "json",
	}))
	indexes := pageItems(h.Call(ctx, rid("indexes.list"), sess, params, nil, nil))
	if !containsName(indexes, "by-kind") {
		t.Fatalf("the created index is not listed: %#v", indexes)
	}

	explained := h.Call(ctx, rid("mango.explain"), sess, params, nil, mustJSON(t, map[string]any{
		"content": `{"selector":{"kind":"order"},"limit":10}`,
	}))
	if mapOf(field(explained, "index")) == nil {
		t.Fatalf("_explain did not report an index: %#v", explained)
	}
	completions := pageItems(h.Call(ctx, rid("mango.complete"), sess, params, nil, nil))
	if !containsLabel(completions, "$elemMatch") || !containsLabel(completions, "customer") {
		t.Fatalf("completions are missing operators or document fields: %#v", completions)
	}

	out := h.Stream(ctx, rid("mango.query"), sess, params, nil,
		[]byte(`{"query":"{\"selector\":{\"kind\":\"order\"},\"limit\":10}"}`+"\n"))
	var result mangoResult
	if err := json.Unmarshal([]byte(strings.SplitN(strings.TrimSpace(string(out)), "\n", 2)[0]), &result); err != nil {
		t.Fatalf("decode mango frame: %v (%s)", err, out)
	}
	if result.RowCount != 1 || result.Index == "" {
		t.Fatalf("unexpected mango result: %#v", result)
	}
	if !strings.Contains(result.Index, "by-kind") {
		t.Fatalf("the Mango query did not use the created index: %#v", result)
	}

	h.Call(ctx, rid("index.delete"), sess, map[string]string{"db": db, "ddoc": "mango-indexes", "name": "by-kind"}, nil, nil)
}

// integrationQueryEditorDefaults runs every InitialQuery the manifest ships
// through its own stream route, with the ${resource.*} tokens resolved exactly
// as the frontend resolves them. A default must execute without an error frame
// and return rows on a seeded database, with no index the user has to build.
func integrationQueryEditorDefaults(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, db string) {
	t.Helper()
	identity := plugin.ResourceIdentity{Kind: "database", Name: db, UID: db}
	panels := queryEditorPanels(New().Manifest())
	if len(panels) == 0 {
		t.Fatal("the manifest declares no query editor panel")
	}
	for _, panel := range panels {
		config, ok := panel.Config.(plugin.QueryEditorConfig)
		if !ok || strings.TrimSpace(config.InitialQuery) == "" {
			t.Fatalf("query editor %q ships no initial query", panel.Key)
		}
		if panel.Source == nil {
			t.Fatalf("query editor %q has no source route", panel.Key)
		}
		params := map[string]string{}
		for key, value := range panel.Source.Params {
			params[key] = resolveResourceTokens(value, identity)
		}
		frame := mustJSON(t, mangoRequest{Query: resolveResourceTokens(config.InitialQuery, identity)})
		out := h.Stream(ctx, panel.Source.RouteID, sess, params, nil, append(frame, '\n'))
		first := strings.SplitN(strings.TrimSpace(string(out)), "\n", 2)[0]
		if first == "" {
			t.Fatalf("query editor %q default produced no frame", panel.Key)
		}
		var failure struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal([]byte(first), &failure); err != nil {
			t.Fatalf("decode %q default frame: %v (%s)", panel.Key, err, first)
		}
		if failure.Error != "" {
			t.Fatalf("query editor %q default errored: %s", panel.Key, failure.Error)
		}
		var result mangoResult
		if err := json.Unmarshal([]byte(first), &result); err != nil {
			t.Fatalf("decode %q default result: %v (%s)", panel.Key, err, first)
		}
		if result.RowCount == 0 {
			t.Fatalf("query editor %q default returned no rows: %#v", panel.Key, result)
		}
		if result.Index == "" {
			t.Fatalf("query editor %q default resolved no index: %#v", panel.Key, result)
		}
	}
}

func queryEditorPanels(manifest plugin.Manifest) []plugin.Panel {
	var panels []plugin.Panel
	var walk func(items []plugin.Panel)
	walk = func(items []plugin.Panel) {
		for _, item := range items {
			if item.Type == plugin.PanelQueryEditor {
				panels = append(panels, item)
			}
			if dashboard, ok := item.Config.(plugin.DashboardConfig); ok {
				walk(dashboard.Cells)
			}
		}
	}
	for _, resource := range manifest.Resources {
		walk(resource.Detail.Tabs)
	}
	return panels
}

// resolveResourceTokens mirrors the frontend's ${resource.*} interpolation.
func resolveResourceTokens(text string, identity plugin.ResourceIdentity) string {
	return strings.NewReplacer(
		"${resource.name}", identity.Name,
		"${resource.scope}", identity.Scope,
		"${resource.namespace}", identity.Namespace,
		"${resource.uid}", identity.UID,
		"${resource.kind}", identity.Kind,
	).Replace(text)
}

func integrationDesign(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, db string) {
	t.Helper()
	params := map[string]string{"db": db}
	ddocParams := map[string]string{"db": db, "ddoc": "reports"}
	viewParams := map[string]string{"db": db, "ddoc": "reports", "view": "by_kind"}

	h.Call(ctx, rid("designdoc.create"), sess, params, nil, mustJSON(t, map[string]any{
		"content": `{"_id":"_design/reports","language":"javascript","views":{` +
			`"by_kind":{"map":"function (doc) { if (doc.kind) { emit(doc.kind, 1); } }","reduce":"_count"}}}`,
	}))
	ddocs := pageItems(h.Call(ctx, rid("designdocs.list"), sess, params, nil, nil))
	if !containsName(ddocs, "reports") {
		t.Fatalf("the created design document is not listed: %#v", ddocs)
	}
	read := h.Call(ctx, rid("designdoc.read"), sess, ddocParams, nil, nil)
	if mapOf(field(read, "views")) == nil {
		t.Fatalf("unexpected design document: %#v", read)
	}
	savedDoc := h.Call(ctx, rid("designdoc.save"), sess, ddocParams, nil, mustJSON(t, map[string]any{
		"content": `{"_id":"_design/reports","language":"javascript","views":{` +
			`"by_kind":{"map":"function (doc) { if (doc.kind) { emit(doc.kind, doc.total || 0); } }","reduce":"_sum"}}}`,
	}))
	if !strings.Contains(stringOf(field(savedDoc, "content")), "_sum") {
		t.Fatalf("the design document save did not return the canonical content: %#v", savedDoc)
	}

	views := pageItems(h.Call(ctx, rid("views.list"), sess, ddocParams, nil, nil))
	if len(views) != 1 || views[0]["reduce_kind"] != "builtin" {
		t.Fatalf("unexpected view listing: %#v", views)
	}
	rows := pageItems(h.Call(ctx, rid("view.rows"), sess, viewParams, nil, nil))
	if len(rows) == 0 || rows[0]["key"] != "order" {
		t.Fatalf("the map view emitted nothing useful: %#v", rows)
	}
	reduced := pageItems(h.Call(ctx, rid("view.reduce"), sess, viewParams, nil, nil))
	if len(reduced) != 1 || numberOf(reduced[0]["value"]) != 13 {
		t.Fatalf("the reduce did not sum the remaining order: %#v", reduced)
	}
	definition := h.Call(ctx, rid("view.definition"), sess, viewParams, nil, nil)
	if !strings.Contains(stringOf(field(definition, "content")), "_sum") {
		t.Fatalf("unexpected view definition: %#v", definition)
	}

	h.Call(ctx, rid("designdoc.delete"), sess, ddocParams, nil, nil)
}

func integrationReplication(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, db, backup string) {
	t.Helper()
	id := "shellcn-it-" + backup
	h.Call(ctx, rid("replication.create"), sess, nil, nil, mustJSON(t, map[string]any{
		"id": id, "source": db, "target": backup, "create_target": true, "continuous": false,
		"use_connection_credentials": true,
	}))

	replications := pageItems(h.Call(ctx, rid("replications.list"), sess, nil, nil, nil))
	if !containsName(replications, id) {
		t.Fatalf("the created replication is not listed: %#v", replications)
	}
	tree := pageItems(h.Call(ctx, rid("replications.tree"), sess, nil, nil, nil))
	if !containsLabel(tree, id) {
		t.Fatalf("the replication is missing from the tree: %#v", tree)
	}
	read := h.Call(ctx, rid("replication.read"), sess, map[string]string{"id": id}, nil, nil)
	if !strings.Contains(stringOf(field(read, "source")), db) {
		t.Fatalf("unexpected replication document: %#v", read)
	}
	h.Call(ctx, rid("replication.events"), sess, map[string]string{"id": id}, nil, nil)
	h.Call(ctx, rid("scheduler.jobs"), sess, nil, nil, nil)

	graph := h.Call(ctx, rid("replication.graph"), sess, nil, nil, nil)
	if len(field(graph, "edges").([]row)) == 0 {
		t.Fatalf("the replication map has no edges: %#v", graph)
	}

	waitFor(ctx, t, 60*time.Second, "replication to copy documents", func() bool {
		info, err := h.Route(rid("database.read")).Handle(plugin.NewRequestContext(ctx, plugin.User{}, sess,
			map[string]string{"db": backup}, nil, nil))
		return err == nil && numberOf(field(info, "doc_count")) > 0
	})
	h.Call(ctx, rid("replication.delete"), sess, map[string]string{"id": id}, nil, nil)
}

func integrationStreams(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, db string) {
	t.Helper()
	params := map[string]string{"db": db}

	metrics := streamFrames(ctx, t, h, rid("server.metrics"), sess, nil, nil)
	if len(metrics) == 0 || metrics[0]["requests"] == nil {
		t.Fatalf("unexpected metrics frames: %#v", metrics)
	}

	// An idle node has no compaction or indexing task, so the watch legitimately
	// emits nothing; the assertion is that it stays a well-formed frame stream.
	streamFrames(ctx, t, h, rid("tasks.watch"), sess, nil, nil)

	databases := streamFrames(ctx, t, h, rid("databases.watch"), sess, nil, nil)
	if len(databases) == 0 {
		t.Fatal("the databases watch emitted no events")
	}
	dbEvents := streamFrames(ctx, t, h, rid("database.watch"), sess, params, nil)
	if len(dbEvents) == 0 || stringOf(dbEvents[0]["name"]) != db {
		t.Fatalf("unexpected database watch frames: %#v", dbEvents)
	}
	docEvents := streamFrames(ctx, t, h, rid("document.watch"), sess, map[string]string{"db": db, "docid": "order-1"}, nil)
	if len(docEvents) == 0 || stringOf(docEvents[0]["_id"]) != "order-1" {
		t.Fatalf("unexpected document watch frames: %#v", docEvents)
	}
	// The replication document was already removed, so this watch is expected to
	// be quiet; it must still open, poll and shut down cleanly.
	streamFrames(ctx, t, h, rid("replications.watch"), sess, nil, nil)

	origins := h.Call(ctx, rid("changes.origins"), sess, params, nil, nil)
	options, ok := origins.([]plugin.Option)
	if !ok || len(options) != 2 {
		t.Fatalf("unexpected change origins: %#v", origins)
	}

	changes := streamFrames(ctx, t, h, rid("changes.stream"), sess, map[string]string{"db": db, "since": "0"}, nil)
	if len(changes) < 2 {
		t.Fatalf("the changes feed produced no events: %#v", changes)
	}
	// documents.watch follows the live feed from "now", so an idle database emits
	// nothing; the assertion is that it opens and closes without error.
	streamFrames(ctx, t, h, rid("documents.watch"), sess, params, nil)
}

func integrationSavedQueries(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, db string) {
	t.Helper()
	store := &fakeStorage{}
	params := map[string]string{"db": db}

	saveRC := plugin.NewRequestContext(ctx, plugin.User{}, sess, params, nil,
		mustJSON(t, map[string]any{"name": "orders", "query": map[string]any{"selector": map[string]any{"kind": "order"}}})).WithStorage(store)
	created, err := h.Route(rid("query.save")).Handle(saveRC)
	if err != nil {
		t.Fatalf("save query: %v", err)
	}
	key := stringOf(field(created, "key"))

	listRC := plugin.NewRequestContext(ctx, plugin.User{}, sess, params, nil, nil).WithStorage(store)
	listed, err := h.Route(rid("queries.list")).Handle(listRC)
	if err != nil {
		t.Fatalf("list queries: %v", err)
	}
	if items := pageItems(listed); len(items) != 1 || items[0]["name"] != "orders" {
		t.Fatalf("unexpected saved queries: %#v", items)
	}

	deleteRC := plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"key": key}, nil, nil).WithStorage(store)
	if _, err := h.Route(rid("query.delete")).Handle(deleteRC); err != nil {
		t.Fatalf("delete query: %v", err)
	}
}

// streamFrames runs a server-push stream for a bounded window and decodes the
// newline-delimited frames it wrote.
func streamFrames(ctx context.Context, t *testing.T, h *contribtest.Harness, id string, sess plugin.Session, params map[string]string, input []byte) []row {
	t.Helper()
	streamCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	out := h.Stream(streamCtx, id, sess, params, nil, input)

	frames := make([]row, 0, 8)
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var frame row
		if err := json.Unmarshal([]byte(line), &frame); err != nil {
			t.Fatalf("%s: decode frame %q: %v", id, line, err)
		}
		if resource := mapOf(frame["resource"]); resource != nil {
			frame = resource
		}
		frames = append(frames, frame)
	}
	return frames
}

func seedConflict(ctx context.Context, t *testing.T, endpoint, db string) {
	t.Helper()
	body := map[string]any{
		"new_edits": false,
		"docs": []any{
			map[string]any{"_id": "conflicted", "_rev": "1-" + strings.Repeat("a", 32), "branch": "left"},
			map[string]any{"_id": "conflicted", "_rev": "1-" + strings.Repeat("b", 32), "branch": "right"},
		},
	}
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("encode conflict fixture: %v", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/"+url.PathEscape(db)+"/_bulk_docs", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("build conflict fixture request: %v", err)
	}
	req.SetBasicAuth(integrationUser, integrationPassword)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("seed conflict: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		t.Fatalf("seed conflict: %s %s", resp.Status, data)
	}
}

func couchEndpoint(ctx context.Context, t *testing.T) string {
	t.Helper()
	if endpoint := os.Getenv("SHELLCN_COUCHDB_ENDPOINT"); endpoint != "" {
		return strings.TrimRight(endpoint, "/")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_COUCHDB_ENDPOINT is not set")
	}
	name := "shellcn-couchdb-it-" + time.Now().UTC().Format("20060102150405")
	if _, err := dockerRun(ctx, "run", "-d", "--rm", "--name", name,
		"-e", "COUCHDB_USER="+integrationUser,
		"-e", "COUCHDB_PASSWORD="+integrationPassword,
		"-p", "127.0.0.1::5984", integrationImage); err != nil {
		t.Skipf("could not start %s: %v", integrationImage, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = dockerRun(cleanupCtx, "rm", "-f", name)
	})

	out, err := dockerRun(ctx, "port", name, "5984/tcp")
	if err != nil {
		t.Fatalf("docker port: %v", err)
	}
	host, port, err := net.SplitHostPort(strings.TrimSpace(strings.Split(strings.TrimSpace(out), "\n")[0]))
	if err != nil {
		t.Fatalf("unexpected docker port output %q: %v", out, err)
	}
	endpoint := "http://" + net.JoinHostPort(host, port)

	waitFor(ctx, t, 120*time.Second, "CouchDB to accept requests", func() bool {
		return couchGET(ctx, endpoint+"/_up") == http.StatusOK
	})
	enableSingleNode(ctx, t, endpoint)
	return endpoint
}

// enableSingleNode runs the documented single-node setup so _users, _replicator
// and _global_changes exist before the plugin touches them.
func enableSingleNode(ctx context.Context, t *testing.T, endpoint string) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"action": "enable_single_node", "bind_address": "0.0.0.0",
		"username": integrationUser, "password": integrationPassword, "singlenode": true,
	})
	if err != nil {
		t.Fatalf("encode cluster setup: %v", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/_cluster_setup", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("build cluster setup request: %v", err)
	}
	req.SetBasicAuth(integrationUser, integrationPassword)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("cluster setup: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, _ := io.ReadAll(resp.Body)
	// A node that is already configured answers 400; anything else is fatal.
	if resp.StatusCode >= 500 {
		t.Fatalf("cluster setup: %s %s", resp.Status, data)
	}
	waitFor(ctx, t, 60*time.Second, "the _replicator database", func() bool {
		return couchGET(ctx, endpoint+"/_replicator") == http.StatusOK
	})
}

func couchGET(ctx context.Context, target string) int {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return 0
	}
	req.SetBasicAuth(integrationUser, integrationPassword)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func waitFor(ctx context.Context, t *testing.T, timeout time.Duration, what string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if ready() {
			return
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func dockerRun(ctx context.Context, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("docker %s: %w\n%s", strings.Join(args, " "), err, out)
	}
	return string(out), nil
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	return data
}

func containsName(rows []row, name string) bool {
	return findRow(rows, "name", name) != nil
}

func containsLabel(rows []row, label string) bool {
	return findRow(rows, "label", label) != nil
}

func findRow(rows []row, key, value string) row {
	for _, item := range rows {
		if stringOf(item[key]) == value {
			return item
		}
	}
	return nil
}
