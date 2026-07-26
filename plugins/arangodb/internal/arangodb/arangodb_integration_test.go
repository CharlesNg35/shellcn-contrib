package arangodb

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	driver "github.com/arangodb/go-driver/v2/arangodb"

	contribtest "github.com/charlesng35/shellcn-contrib/shared/plugintest"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

const (
	integrationImage    = "arangodb/arangodb:3.12"
	integrationPassword = "shellcn-test-password"
	foxxMount           = "/shellcn-it"
)

func TestArangoDBPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_ARANGODB_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_ARANGODB_INTEGRATION=1 to run against ArangoDB")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	endpoint, password := arangoEndpoint(ctx, t)
	database := "shellcn_it_" + time.Now().UTC().Format("20060102150405")

	p := New()
	sess, err := p.Connect(ctx, plugin.ConnectConfig{
		Config: connectionConfig(endpoint, password, defaultDatabase),
		Net:    plugintest.DirectTransport(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()

	h := contribtest.NewHarness(t, p.Routes())
	store := &fakeStorage{}

	// Databases.
	h.Call(ctx, rid("database.create"), sess, nil, nil, mustJSON(t, map[string]any{
		"name": database, "replication_factor": 1,
	}))
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		h.CallNoFail(cleanup, rid("database.drop"), sess, map[string]string{"database": database})
	})

	databases := rowsOf(t, h.Call(ctx, rid("databases.list"), sess, nil, nil, nil))
	if !hasField(databases, "name", database) {
		t.Fatalf("created database missing from list: %#v", databases)
	}
	h.Call(ctx, rid("tree.databases"), sess, nil, nil, nil)
	categories := treeNodes(t, h.Call(ctx, rid("tree.database"), sess, map[string]string{"database": database}, nil, nil))
	if len(categories) < 6 {
		t.Fatalf("expected a full category tree, got %#v", categories)
	}
	h.Call(ctx, rid("database.read"), sess, map[string]string{"database": database}, nil, nil)

	dbParams := map[string]string{"database": database}

	// Collections and documents.
	h.Call(ctx, rid("collection.create"), sess, dbParams, nil, mustJSON(t, map[string]any{
		"name": "people", "type": "document", "number_of_shards": 1, "replication_factor": 1,
	}))
	h.Call(ctx, rid("collection.create"), sess, dbParams, nil, mustJSON(t, map[string]any{
		"name": "companies", "type": "document",
	}))
	h.Call(ctx, rid("collection.create"), sess, dbParams, nil, mustJSON(t, map[string]any{
		"name": "works_at", "type": "edge",
	}))
	h.Call(ctx, rid("collection.create"), sess, dbParams, nil, mustJSON(t, map[string]any{
		"name": "scratch", "type": "document",
	}))

	collections := rowsOf(t, h.Call(ctx, rid("collections.list"), sess, dbParams, nil, nil))
	if !hasField(collections, "name", "people") || !hasField(collections, "name", "works_at") {
		t.Fatalf("created collections missing: %#v", collections)
	}
	for _, item := range collections {
		if item["system"] == true {
			t.Fatalf("system collections must stay hidden by default: %#v", item)
		}
	}

	// ArangoDB's extended naming constraints: dots, spaces and UTF-8 letters. The
	// name travels as a URL path segment and as an AQL bind parameter, so both
	// paths are exercised end to end.
	extended := "München コレクション.v1"
	extendedParams := map[string]string{"database": database, "collection": extended}
	h.Call(ctx, rid("collection.create"), sess, dbParams, nil, mustJSON(t, map[string]any{
		"name": extended, "type": "document",
	}))
	if !hasField(rowsOf(t, h.Call(ctx, rid("collections.list"), sess, dbParams, nil, nil)), "name", extended) {
		t.Fatalf("extended-name collection missing from the list")
	}
	h.Call(ctx, rid("document.insert"), sess, extendedParams, nil, mustJSON(t, map[string]any{
		"values": map[string]any{"_key": "one", "name": "Erste"},
	}))
	extendedDocs := rowsOf(t, h.Call(ctx, rid("documents.list"), sess, extendedParams, nil, nil))
	if len(extendedDocs) != 1 || extendedDocs[0]["name"] != "Erste" {
		t.Fatalf("extended-name collection is unreachable: %#v", extendedDocs)
	}
	if detail := mapOf(t, h.Call(ctx, rid("collection.read"), sess, extendedParams, nil, nil)); detail["type"] != "document" {
		t.Fatalf("collection.read did not reach the extended-name collection: %#v", detail)
	}
	h.Call(ctx, rid("collection.drop"), sess, extendedParams, nil, nil)
	if hasField(rowsOf(t, h.Call(ctx, rid("collections.list"), sess, dbParams, nil, nil)), "name", extended) {
		t.Fatalf("extended-name collection still present after drop")
	}

	peopleParams := map[string]string{"database": database, "collection": "people"}
	edgeParams := map[string]string{"database": database, "collection": "works_at"}

	ada := mapOf(t, h.Call(ctx, rid("document.insert"), sess, peopleParams, nil, mustJSON(t, map[string]any{
		"values": map[string]any{"_key": "ada", "name": "Ada", "city": "London", "age": 36, "password": "hunter2"},
	})))
	if ada["_key"] != "ada" {
		t.Fatalf("unexpected insert result: %#v", ada)
	}
	// Redacted attributes never reach the server through the grid.
	if _, leaked := ada["password"]; leaked {
		t.Fatalf("redacted attribute must not be stored: %#v", ada)
	}
	h.Call(ctx, rid("document.insert"), sess, peopleParams, nil, mustJSON(t, map[string]any{
		"values": map[string]any{"_key": "bob", "name": "Bob", "city": "Berlin", "age": 41},
	}))
	h.Call(ctx, rid("document.insert"), sess, map[string]string{"database": database, "collection": "companies"}, nil,
		mustJSON(t, map[string]any{"values": map[string]any{"_key": "acme", "name": "ACME"}}))
	h.Call(ctx, rid("document.insert"), sess, edgeParams, nil, mustJSON(t, map[string]any{
		"values": map[string]any{"_key": "e1", "_from": "people/ada", "_to": "companies/acme", "since": 2019},
	}))
	h.Call(ctx, rid("document.insert"), sess, map[string]string{"database": database, "collection": "scratch"}, nil,
		mustJSON(t, map[string]any{"values": map[string]any{"_key": "tmp", "name": "temporary"}}))

	docs := rowsOf(t, h.Call(ctx, rid("documents.list"), sess, peopleParams, nil, nil))
	if len(docs) != 2 {
		t.Fatalf("expected two people, got %#v", docs)
	}
	searchPage := pageOf(t, h.Call(ctx, rid("documents.list"), sess, peopleParams, queryValues("filter", "berlin"), nil))
	if len(searchPage.Items) != 1 || searchPage.Items[0]["_key"] != "bob" {
		t.Fatalf("free-text search did not narrow the page: %#v", searchPage.Items)
	}
	// A filtered set has no stored count, and scanning the collection to invent
	// one is exactly what the bounded reader refuses to do: the total is omitted.
	if searchPage.Total != nil {
		t.Fatalf("search page must not carry a full-scan total: %d", *searchPage.Total)
	}
	sorted := rowsOf(t, h.Call(ctx, rid("documents.list"), sess, peopleParams, queryValues("sort", "-name"), nil))
	if len(sorted) != 2 || sorted[0]["name"] != "Bob" {
		t.Fatalf("server-side sort not applied: %#v", sorted)
	}

	columns := rowsOf(t, h.Call(ctx, rid("documents.columns"), sess, peopleParams, nil, nil))
	byName := indexRows(columns, "name")
	if byName["_key"]["readOnly"] != true || byName["_key"]["editable"] != false {
		t.Fatalf("_key must be read-only: %#v", byName["_key"])
	}
	if byName["age"]["editor"] != string(plugin.ColumnEditorNumber) {
		t.Fatalf("numeric attributes should get a number editor: %#v", byName["age"])
	}
	edgeColumns := indexRows(rowsOf(t, h.Call(ctx, rid("documents.columns"), sess, edgeParams, nil, nil)), "name")
	if _, ok := edgeColumns["_from"]; !ok {
		t.Fatalf("edge collections must expose _from/_to: %#v", edgeColumns)
	}

	h.Call(ctx, rid("document.update"), sess, peopleParams, nil, mustJSON(t, map[string]any{
		"key": map[string]any{"_key": "ada"}, "values": map[string]any{"city": "Cambridge"},
	}))
	updated := docContent(t, h.Call(ctx, rid("document.read"), sess,
		map[string]string{"database": database, "collection": "people", "key": "ada"}, nil, nil))
	if updated["city"] != "Cambridge" {
		t.Fatalf("update not applied: %#v", updated)
	}

	replaced := docContent(t, h.Call(ctx, rid("document.replace"), sess,
		map[string]string{"database": database, "collection": "people", "key": "ada"}, nil,
		mustJSON(t, map[string]any{"content": `{"name":"Ada Lovelace","age":36}`})))
	if replaced["name"] != "Ada Lovelace" {
		t.Fatalf("replace not applied: %#v", replaced)
	}
	if _, ok := replaced["city"]; ok {
		t.Fatalf("replace must drop removed attributes: %#v", replaced)
	}

	h.Call(ctx, rid("collection.read"), sess, peopleParams, nil, nil)
	h.Call(ctx, rid("collection.shards"), sess, peopleParams, nil, nil)

	// Schema validation editor.
	h.Call(ctx, rid("collection.schema.read"), sess, peopleParams, nil, nil)
	schema := docContent(t, h.Call(ctx, rid("collection.schema.update"), sess, peopleParams, nil, mustJSON(t, map[string]any{
		"schema": map[string]any{
			"rule":    map[string]any{"type": "object", "properties": map[string]any{"name": map[string]any{"type": "string"}}},
			"level":   "moderate",
			"message": "name must be a string",
		},
	})))
	if schema["level"] != "moderate" {
		t.Fatalf("schema level not applied: %#v", schema)
	}

	// Indexes.
	h.Call(ctx, rid("index.create"), sess, peopleParams, nil, mustJSON(t, map[string]any{
		"name": "idx_city", "type": "persistent", "fields": "city", "unique": false, "sparse": false,
	}))
	indexes := rowsOf(t, h.Call(ctx, rid("indexes.list"), sess, peopleParams, nil, nil))
	if !hasField(indexes, "name", "idx_city") {
		t.Fatalf("created index missing: %#v", indexes)
	}
	h.Call(ctx, rid("index.delete"), sess, map[string]string{"database": database, "collection": "people", "index": "idx_city"}, nil, nil)
	indexes = rowsOf(t, h.Call(ctx, rid("indexes.list"), sess, peopleParams, nil, nil))
	if hasField(indexes, "name", "idx_city") {
		t.Fatalf("index still present after delete: %#v", indexes)
	}

	// Graphs.
	h.Call(ctx, rid("graph.create"), sess, dbParams, nil, mustJSON(t, map[string]any{
		"name": "employment", "edge_collection": "works_at", "from": "people", "to": "companies",
	}))
	graphParams := map[string]string{"database": database, "graph": "employment"}
	graphs := rowsOf(t, h.Call(ctx, rid("graphs.list"), sess, dbParams, nil, nil))
	if !hasField(graphs, "name", "employment") {
		t.Fatalf("created graph missing: %#v", graphs)
	}
	graphDetail := mapOf(t, h.Call(ctx, rid("graph.read"), sess, graphParams, nil, nil))
	if fmt.Sprint(graphDetail["edge_count"]) != "1" {
		t.Fatalf("graph should report one edge: %#v", graphDetail)
	}
	h.Call(ctx, rid("graph.edges"), sess, graphParams, nil, nil)
	vertices := rowsOf(t, h.Call(ctx, rid("graph.vertices"), sess, graphParams, nil, nil))
	if len(vertices) != 2 {
		t.Fatalf("expected two vertex collections, got %#v", vertices)
	}

	explored := graphPayloadOf(t, h.Call(ctx, rid("graph.explore"), sess, graphParams, nil, nil))
	if len(explored.Nodes) < 2 || len(explored.Edges) < 1 {
		t.Fatalf("explorer should seed nodes and edges: %#v", explored)
	}
	expanded := graphPayloadOf(t, h.Call(ctx, rid("graph.expand"), sess, graphParams, queryValues("node", "people/ada"), nil))
	if len(expanded.Nodes) < 2 || len(expanded.Edges) < 1 {
		t.Fatalf("expanding a vertex should return its neighbourhood: %#v", expanded)
	}
	schemaMap := graphPayloadOf(t, h.Call(ctx, rid("database.schema"), sess, dbParams, nil, nil))
	if len(schemaMap.Nodes) < 3 || len(schemaMap.Edges) < 2 {
		t.Fatalf("schema map should link edge collections to both endpoints: %#v", schemaMap)
	}

	topology := runStreamRaw(t, h, ctx, rid("graph.topology"), sess, graphParams,
		`{"type":"ready","width":900,"height":600,"dpr":1,"theme":"dark"}`+"\n", 30*time.Second)
	if !strings.Contains(topology, "works_at") || !strings.Contains(topology, "\"regions\"") {
		t.Fatalf("topology canvas frame missing content: %s", topology)
	}

	// ArangoSearch.
	h.Call(ctx, rid("analyzer.create"), sess, dbParams, nil, mustJSON(t, map[string]any{
		"name": "it_text", "type": "text", "locale": "en", "features": []string{"frequency", "norm", "position"},
	}))
	analyzers := rowsOf(t, h.Call(ctx, rid("analyzers.list"), sess, dbParams, nil, nil))
	if !hasField(analyzers, "name", "it_text") {
		t.Fatalf("created analyzer missing: %#v", analyzers)
	}
	h.Call(ctx, rid("analyzer.read"), sess, map[string]string{"database": database, "analyzer": "it_text"}, nil, nil)

	h.Call(ctx, rid("view.create"), sess, dbParams, nil, mustJSON(t, map[string]any{
		"name": "people_search", "collection": "people", "analyzer": "identity",
	}))
	viewParams := map[string]string{"database": database, "view": "people_search"}
	views := rowsOf(t, h.Call(ctx, rid("views.list"), sess, dbParams, nil, nil))
	if !hasField(views, "name", "people_search") {
		t.Fatalf("created view missing: %#v", views)
	}
	h.Call(ctx, rid("view.read"), sess, viewParams, nil, nil)
	links := rowsOf(t, h.Call(ctx, rid("view.links"), sess, viewParams, nil, nil))
	if len(links) != 1 || links[0]["collection"] != "people" {
		t.Fatalf("view should link the people collection: %#v", links)
	}
	h.Call(ctx, rid("view.definition"), sess, viewParams, nil, nil)
	definition := docContent(t, h.Call(ctx, rid("view.update"), sess, viewParams, nil, mustJSON(t, map[string]any{
		"properties": map[string]any{"cleanupIntervalStep": 3},
	})))
	if fmt.Sprint(definition["cleanupIntervalStep"]) != "3" {
		t.Fatalf("view property update not applied: %#v", definition)
	}

	// AQL console, activity and saved queries.
	frames := decodeFrames(t, runStreamRaw(t, h, ctx, rid("query"), sess, dbParams,
		`{"query":"FOR doc IN people SORT doc._key RETURN doc"}`+"\n"+
			`{"query":"EXPLAIN FOR doc IN people RETURN doc"}`+"\n"+
			`{"query":"INSERT {name: \"temp\"} INTO scratch RETURN NEW","confirm":true}`+"\n"+
			`{"query":"FOR doc IN nope RETURN doc"}`+"\n", 60*time.Second))
	if len(frames) != 4 {
		t.Fatalf("expected four result frames, got %d: %#v", len(frames), frames)
	}
	if fmt.Sprint(frames[0]["rowCount"]) != "2" {
		t.Fatalf("select frame should return both people: %#v", frames[0])
	}
	if frames[1]["commandTag"] != "explain" {
		t.Fatalf("EXPLAIN prefix should return a plan: %#v", frames[1])
	}
	if !strings.Contains(fmt.Sprint(frames[2]["commandTag"]), "writes executed 1") {
		t.Fatalf("write frame should report its counters: %#v", frames[2])
	}
	if _, ok := frames[3]["error"].(string); !ok {
		t.Fatalf("a failing query must return an error frame, not close the socket: %#v", frames[3])
	}

	completion := completionOf(t, h.Call(ctx, rid("query.complete"), sess, dbParams, nil, nil))
	if !completion["people"] || !completion["employment"] || !completion["FOR"] {
		t.Fatalf("completion should offer collections, graphs and keywords: %#v", completion)
	}
	h.Call(ctx, rid("query.cancel"), sess, dbParams, nil, nil)
	h.Call(ctx, rid("queries.running"), sess, dbParams, nil, nil)
	h.Call(ctx, rid("queries.slow"), sess, dbParams, nil, nil)
	h.Call(ctx, rid("queries.slow.clear"), sess, dbParams, nil, nil)
	// A finished query id is no longer tracked; the detail route says so clearly.
	h.CallNoFail(ctx, rid("query.read"), sess, map[string]string{"id": encodeID(kindAQLQuery, database, "0")})
	h.CallNoFail(ctx, rid("query.kill"), sess, map[string]string{"id": encodeID(kindAQLQuery, database, "0")})

	saved := savedOf(t, callWithStorage(t, h, ctx, rid("saved.create"), sess, store, nil, mustJSON(t, map[string]any{
		"name": "People by city", "database": database, "query": "FOR doc IN people RETURN doc",
	})))
	savedList := rowsOf(t, callWithStorage(t, h, ctx, rid("saved.list"), sess, store, nil, nil))
	if len(savedList) != 1 || savedList[0]["name"] != "People by city" {
		t.Fatalf("saved query not listed: %#v", savedList)
	}
	callWithStorage(t, h, ctx, rid("saved.read"), sess, store, map[string]string{"id": saved.ID}, nil)
	callWithStorage(t, h, ctx, rid("saved.delete"), sess, store, map[string]string{"id": saved.ID}, nil)

	// Metrics streams.
	dbFrame := firstFrame(t, runStreamRaw(t, h, ctx, rid("database.metrics"), sess, dbParams, "", 3*time.Second))
	if fmt.Sprint(dbFrame["collections"]) != "4" {
		t.Fatalf("database metrics should count user collections: %#v", dbFrame)
	}
	colFrame := firstFrame(t, runStreamRaw(t, h, ctx, rid("collection.metrics"), sess, peopleParams, "", 3*time.Second))
	if fmt.Sprint(colFrame["documents"]) != "2" {
		t.Fatalf("collection metrics should count documents: %#v", colFrame)
	}
	clusterFrame := firstFrame(t, runStreamRaw(t, h, ctx, rid("cluster.metrics"), sess, nil, "", 3*time.Second))
	if fmt.Sprint(clusterFrame["serversTotal"]) == "0" {
		t.Fatalf("cluster metrics should report at least one server: %#v", clusterFrame)
	}

	// Cluster / server.
	clusterRows := rowsOf(t, h.Call(ctx, rid("cluster.list"), sess, nil, nil, nil))
	if len(clusterRows) != 1 {
		t.Fatalf("cluster overview should be a single row: %#v", clusterRows)
	}
	overview := mapOf(t, h.Call(ctx, rid("cluster.read"), sess, nil, nil, nil))
	if overview["version"] == "" {
		t.Fatalf("cluster overview should report the server version: %#v", overview)
	}
	servers := rowsOf(t, h.Call(ctx, rid("cluster.servers"), sess, nil, nil, nil))
	if len(servers) == 0 {
		t.Fatalf("expected at least one server row")
	}

	// Foxx.
	installFoxxService(ctx, t, sess.(*Session), database)
	services := rowsOf(t, h.Call(ctx, rid("foxx.list"), sess, dbParams, nil, nil))
	if !hasField(services, "mount", foxxMount) {
		t.Fatalf("installed Foxx service missing: %#v", services)
	}
	serviceID := encodeID(kindFoxx, database, foxxMount)
	serviceParams := map[string]string{"service": serviceID}
	service := mapOf(t, h.Call(ctx, rid("foxx.read"), sess, serviceParams, nil, nil))
	if service["name"] != "shellcn-it" {
		t.Fatalf("unexpected Foxx manifest: %#v", service)
	}
	foxxRouteRows := rowsOf(t, h.Call(ctx, rid("foxx.routes"), sess, serviceParams, nil, nil))
	if len(foxxRouteRows) == 0 {
		t.Fatalf("Foxx swagger should expose at least one route")
	}
	readme, ok := h.Call(ctx, rid("foxx.readme"), sess, serviceParams, nil, nil).(string)
	if !ok || !strings.Contains(readme, "ShellCN") {
		t.Fatalf("readme must be raw markdown the editor can render: %#v", readme)
	}
	h.Call(ctx, rid("foxx.configuration"), sess, serviceParams, nil, nil)
	h.Call(ctx, rid("foxx.dependencies"), sess, serviceParams, nil, nil)
	development := mapOf(t, h.Call(ctx, rid("foxx.development"), sess, serviceParams, nil, mustJSON(t, map[string]any{"enabled": true})))
	if development["development"] != true {
		t.Fatalf("development mode not enabled: %#v", development)
	}
	h.Call(ctx, rid("foxx.development"), sess, serviceParams, nil, mustJSON(t, map[string]any{"enabled": false}))
	h.Call(ctx, rid("foxx.uninstall"), sess, serviceParams, nil, nil)
	services = rowsOf(t, h.Call(ctx, rid("foxx.list"), sess, dbParams, nil, nil))
	if hasField(services, "mount", foxxMount) {
		t.Fatalf("Foxx service still mounted after uninstall: %#v", services)
	}

	// Destructive collection lifecycle.
	scratchParams := map[string]string{"database": database, "collection": "scratch"}
	h.Call(ctx, rid("collection.truncate"), sess, scratchParams, nil, nil)
	remaining := rowsOf(t, h.Call(ctx, rid("documents.list"), sess, scratchParams, nil, nil))
	if len(remaining) != 0 {
		t.Fatalf("truncate should empty the collection: %#v", remaining)
	}
	h.Call(ctx, rid("document.delete"), sess, peopleParams, nil, mustJSON(t, map[string]any{
		"key": map[string]any{"_key": "bob"},
	}))
	docs = rowsOf(t, h.Call(ctx, rid("documents.list"), sess, peopleParams, nil, nil))
	if len(docs) != 1 {
		t.Fatalf("delete should remove exactly one document: %#v", docs)
	}
	h.Call(ctx, rid("collection.drop"), sess, scratchParams, nil, nil)
	h.Call(ctx, rid("view.drop"), sess, viewParams, nil, nil)
	h.Call(ctx, rid("analyzer.delete"), sess, map[string]string{"database": database, "analyzer": "it_text"}, nil, nil)
	h.Call(ctx, rid("graph.drop"), sess, graphParams, nil, nil)
	h.Call(ctx, rid("database.drop"), sess, dbParams, nil, nil)

	h.AssertAllCovered()
}

// TestArangoDBReadOnlyIntegration proves the read-only switch is enforced by the
// plugin before any statement reaches the server.
func TestArangoDBReadOnlyIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_ARANGODB_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_ARANGODB_INTEGRATION=1 to run against ArangoDB")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	endpoint, password := arangoEndpoint(ctx, t)
	p := New()
	routes := plugintest.RouteMap(p.Routes())
	collection := "shellcn_ro_" + time.Now().UTC().Format("20060102150405")
	params := map[string]string{"database": defaultDatabase, "collection": collection}

	writable, err := p.Connect(ctx, plugin.ConnectConfig{
		Config: connectionConfig(endpoint, password, defaultDatabase), Net: plugintest.DirectTransport(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = writable.Close() }()
	callRoute(t, routes[rid("collection.create")], ctx, writable, map[string]string{"database": defaultDatabase}, nil,
		mustJSON(t, map[string]any{"name": collection, "type": "document"}))
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = routes[rid("collection.drop")].Handle(plugin.NewRequestContext(cleanup, plugin.User{}, writable, params, nil, nil))
	})

	cfg := connectionConfig(endpoint, password, defaultDatabase)
	cfg["read_only"] = true
	sess, err := p.Connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect read-only: %v", err)
	}
	defer func() { _ = sess.Close() }()

	body := mustJSON(t, map[string]any{"name": "should_not_exist", "type": "document"})
	if _, err := routes[rid("collection.create")].Handle(
		plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"database": defaultDatabase}, nil, body),
	); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("read-only connections must refuse writes, got %v", err)
	}
	insert := mustJSON(t, map[string]any{"values": map[string]any{"name": "nope"}})
	if _, err := routes[rid("document.insert")].Handle(
		plugin.NewRequestContext(ctx, plugin.User{}, sess, params, nil, insert),
	); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("read-only connections must refuse document writes, got %v", err)
	}

	// Privileged AQL administration is a mutation of server state, so read-only
	// connections must refuse it too.
	if _, err := routes[rid("queries.slow.clear")].Handle(
		plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"database": defaultDatabase}, nil, nil),
	); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("read-only connections must refuse clearing the slow log, got %v", err)
	}
	if _, err := routes[rid("query.kill")].Handle(
		plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"id": encodeID(kindAQLQuery, defaultDatabase, "1")}, nil, nil),
	); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("read-only connections must refuse killing queries, got %v", err)
	}

	columns := rowsOf(t, callRoute(t, routes[rid("documents.columns")], ctx, sess, params, nil, nil))
	if len(columns) == 0 {
		t.Fatal("expected the system attribute columns")
	}
	for _, column := range columns {
		if column["editable"] == true {
			t.Fatalf("read-only connections must not expose editable columns: %#v", column)
		}
	}
}

// TestArangoDBPanelDefaultsIntegration runs the pre-filled query of every AQL
// editor exactly as the browser sends it: ${resource.*} resolved from the
// identity the panel was opened with, then through the query stream.
func TestArangoDBPanelDefaultsIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_ARANGODB_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_ARANGODB_INTEGRATION=1 to run against ArangoDB")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	endpoint, password := arangoEndpoint(ctx, t)
	database := "shellcn_defaults_" + time.Now().UTC().Format("20060102150405")

	p := New()
	sess, err := p.Connect(ctx, plugin.ConnectConfig{
		Config: connectionConfig(endpoint, password, defaultDatabase), Net: plugintest.DirectTransport(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()

	h := contribtest.NewHarness(t, p.Routes())
	dbParams := map[string]string{"database": database}
	h.Call(ctx, rid("database.create"), sess, nil, nil, mustJSON(t, map[string]any{"name": database, "replication_factor": 1}))
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		h.CallNoFail(cleanup, rid("database.drop"), sess, dbParams)
	})
	for _, spec := range []map[string]any{
		{"name": "people", "type": "document"},
		{"name": "companies", "type": "document"},
		{"name": "works_at", "type": "edge"},
	} {
		h.Call(ctx, rid("collection.create"), sess, dbParams, nil, mustJSON(t, spec))
	}
	for collection, values := range map[string][]map[string]any{
		"people":    {{"_key": "ada", "name": "Ada"}, {"_key": "bob", "name": "Bob"}},
		"companies": {{"_key": "acme", "name": "Acme"}},
		"works_at":  {{"_from": "people/ada", "_to": "companies/acme"}, {"_from": "people/bob", "_to": "companies/acme"}},
	} {
		for _, value := range values {
			h.Call(ctx, rid("document.insert"), sess, map[string]string{"database": database, "collection": collection}, nil,
				mustJSON(t, map[string]any{"values": value}))
		}
	}
	h.Call(ctx, rid("graph.create"), sess, dbParams, nil, mustJSON(t, map[string]any{
		"name": "employment", "edge_collection": "works_at", "from": "people", "to": "companies",
	}))

	identities := map[string]plugin.ResourceIdentity{
		kindDatabase:   {Kind: kindDatabase, Name: database, UID: database},
		kindCollection: {Kind: kindCollection, Namespace: database, Name: "people", UID: encodeID(kindCollection, database, "people")},
		kindGraph:      {Kind: kindGraph, Namespace: database, Name: "employment", UID: encodeID(kindGraph, database, "employment")},
	}
	panels := queryEditorPanels(t, p.Manifest())
	if len(panels) != len(identities) {
		t.Fatalf("expected %d AQL editors, found %d", len(identities), len(panels))
	}
	for _, panel := range panels {
		identity, ok := identities[panel.kind]
		if !ok {
			t.Fatalf("no seeded resource for kind %q", panel.kind)
		}
		query := resolveResourceTokens(panel.query, identity)
		params := make(map[string]string, len(panel.params))
		for key, value := range panel.params {
			params[key] = resolveResourceTokens(value, identity)
		}
		frames := decodeFrames(t, runStreamRaw(t, h, ctx, rid("query"), sess, params,
			string(mustJSON(t, map[string]any{"query": query, "confirm": false}))+"\n", 60*time.Second))
		if len(frames) != 1 {
			t.Fatalf("%s default: expected one frame, got %#v", panel.kind, frames)
		}
		if message, failed := frames[0]["error"]; failed {
			t.Fatalf("%s default failed: %v\n%s", panel.kind, message, query)
		}
		if fmt.Sprint(frames[0]["rowCount"]) == "0" {
			t.Fatalf("%s default returned no rows:\n%s", panel.kind, query)
		}
	}
}

// TestArangoDBBoundedPagingIntegration seeds ten pages of documents and more
// collections than fit on one page, then walks both lists with a small limit:
// every call answers with exactly one bounded page, the cursor advances to the
// next distinct page without overlaps or gaps, and the walk ends on its own.
func TestArangoDBBoundedPagingIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_ARANGODB_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_ARANGODB_INTEGRATION=1 to run against ArangoDB")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	const (
		seededDocs        = 250
		docLimit          = 25
		seededCollections = 12
		collectionLimit   = 5
	)

	endpoint, password := arangoEndpoint(ctx, t)
	database := "shellcn_paging_" + time.Now().UTC().Format("20060102150405")

	p := New()
	sess, err := p.Connect(ctx, plugin.ConnectConfig{
		Config: connectionConfig(endpoint, password, defaultDatabase), Net: plugintest.DirectTransport(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()

	h := contribtest.NewHarness(t, p.Routes())
	dbParams := map[string]string{"database": database}
	h.Call(ctx, rid("database.create"), sess, nil, nil, mustJSON(t, map[string]any{"name": database, "replication_factor": 1}))
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		h.CallNoFail(cleanup, rid("database.drop"), sess, dbParams)
	})

	names := make([]string, 0, seededCollections)
	for i := range seededCollections - 1 {
		names = append(names, fmt.Sprintf("bulk_%02d", i))
	}
	names = append(names, "events")
	for _, name := range names {
		h.Call(ctx, rid("collection.create"), sess, dbParams, nil, mustJSON(t, map[string]any{
			"name": name, "type": "document", "number_of_shards": 1, "replication_factor": 1,
		}))
	}
	slices.Sort(names)

	keys := make([]string, 0, seededDocs)
	for i := range seededDocs {
		keys = append(keys, fmt.Sprintf("doc%04d", i))
	}
	seedDocuments(ctx, t, sess.(*Session), database, "events", keys)

	docParams := map[string]string{"database": database, "collection": "events"}
	first := pageOf(t, h.Call(ctx, rid("documents.list"), sess, docParams, queryValues("limit", strconv.Itoa(docLimit)), nil))
	if len(first.Items) != docLimit {
		t.Fatalf("documents.list must answer with one page of %d, got %d of %d seeded", docLimit, len(first.Items), seededDocs)
	}
	if first.NextCursor == "" {
		t.Fatal("a full page must carry a cursor to the next one")
	}
	if first.Total == nil || *first.Total != seededDocs {
		t.Fatalf("total must be the collection's stored count: %#v", first.Total)
	}

	seen := make([]string, 0, seededDocs)
	pages, cursor := 0, ""
	for {
		query := queryValues("limit", strconv.Itoa(docLimit))
		if cursor != "" {
			query["cursor"] = []string{cursor}
		}
		page := pageOf(t, h.Call(ctx, rid("documents.list"), sess, docParams, query, nil))
		pages++
		if len(page.Items) > docLimit {
			t.Fatalf("page %d returned %d documents for a limit of %d", pages, len(page.Items), docLimit)
		}
		for _, item := range page.Items {
			seen = append(seen, fmt.Sprint(item["_key"]))
		}
		if page.NextCursor == "" {
			break
		}
		if len(page.Items) != docLimit {
			t.Fatalf("page %d is short but still advertises a next page", pages)
		}
		if pages > seededDocs {
			t.Fatal("document paging never terminated")
		}
		cursor = page.NextCursor
	}
	if want := seededDocs / docLimit; pages != want {
		t.Fatalf("walked %d pages, want %d: a trailing empty page means the cursor is guessed instead of probed", pages, want)
	}
	if len(seen) != seededDocs {
		t.Fatalf("paging covered %d of %d documents", len(seen), seededDocs)
	}
	for i, key := range seen {
		if key != keys[i] {
			t.Fatalf("the walk overlaps or skips at position %d: got %q, want %q", i, key, keys[i])
		}
	}

	filtered := pageOf(t, h.Call(ctx, rid("documents.list"), sess, docParams,
		queryValues("limit", strconv.Itoa(docLimit), "filter", "doc01"), nil))
	if len(filtered.Items) != docLimit || filtered.NextCursor == "" {
		t.Fatalf("a filtered list must page like any other: %d items, cursor %q", len(filtered.Items), filtered.NextCursor)
	}
	if filtered.Total != nil {
		t.Fatalf("a filtered page must not carry a full-scan total, got %d", *filtered.Total)
	}

	rows := make([]map[string]any, 0, seededCollections)
	pages, cursor = 0, ""
	for {
		query := queryValues("limit", strconv.Itoa(collectionLimit))
		if cursor != "" {
			query["cursor"] = []string{cursor}
		}
		page := pageOf(t, h.Call(ctx, rid("collections.list"), sess, dbParams, query, nil))
		pages++
		if len(page.Items) > collectionLimit {
			t.Fatalf("collections page %d returned %d rows for a limit of %d", pages, len(page.Items), collectionLimit)
		}
		if page.Total == nil || *page.Total != seededCollections {
			t.Fatalf("collections total must be the catalogue size: %#v", page.Total)
		}
		for _, item := range page.Items {
			if _, ok := item["documents"]; !ok {
				t.Fatalf("collection row without a document count: %#v", item)
			}
			rows = append(rows, item)
		}
		if page.NextCursor == "" {
			break
		}
		if len(page.Items) != collectionLimit {
			t.Fatalf("collections page %d is short but still advertises a next page", pages)
		}
		if pages > seededCollections {
			t.Fatal("collection paging never terminated")
		}
		cursor = page.NextCursor
	}
	if want := (seededCollections + collectionLimit - 1) / collectionLimit; pages != want {
		t.Fatalf("walked %d collection pages, want %d", pages, want)
	}
	walked := make([]string, 0, len(rows))
	for _, item := range rows {
		walked = append(walked, fmt.Sprint(item["name"]))
	}
	if !slices.Equal(walked, names) {
		t.Fatalf("collection walk is not gap-free: %v", walked)
	}
	if count := fmt.Sprint(indexRows(rows, "name")["events"]["documents"]); count != strconv.Itoa(seededDocs) {
		t.Fatalf("paged collection rows must still carry their counts: got %q", count)
	}
}

func seedDocuments(ctx context.Context, t *testing.T, s *Session, database, collection string, keys []string) {
	t.Helper()
	if _, err := s.queryRows(ctx, database,
		`FOR k IN @keys INSERT {_key: k, label: CONCAT("row-", k)} INTO @@col`,
		map[string]any{"@col": collection, "keys": keys}, 1); err != nil {
		t.Fatalf("seed %s: %v", collection, err)
	}
}

type editorPanel struct {
	kind   string
	query  string
	params map[string]string
}

func queryEditorPanels(t *testing.T, manifest plugin.Manifest) []editorPanel {
	t.Helper()
	panels := make([]editorPanel, 0, 3)
	for _, resource := range manifest.Resources {
		for _, tab := range resource.Detail.Tabs {
			if tab.Type != plugin.PanelQueryEditor {
				continue
			}
			config, ok := tab.Config.(plugin.QueryEditorConfig)
			if !ok {
				t.Fatalf("%s/%s: unexpected editor config %T", resource.Kind, tab.Key, tab.Config)
			}
			if strings.TrimSpace(config.InitialQuery) == "" {
				t.Fatalf("%s/%s: query editor without a pre-filled query", resource.Kind, tab.Key)
			}
			params := map[string]string{}
			if tab.Source != nil {
				for key, value := range tab.Source.Params {
					params[key] = value
				}
			}
			panels = append(panels, editorPanel{kind: resource.Kind, query: config.InitialQuery, params: params})
		}
	}
	return panels
}

func resolveResourceTokens(value string, identity plugin.ResourceIdentity) string {
	return strings.NewReplacer(
		"${resource.kind}", identity.Kind,
		"${resource.scope}", identity.Scope,
		"${resource.namespace}", identity.Namespace,
		"${resource.name}", identity.Name,
		"${resource.uid}", identity.UID,
	).Replace(value)
}

func connectionConfig(endpoint, password, database string) map[string]any {
	return map[string]any{
		"endpoint": endpoint, "database": database,
		"auth": authPassword, "username": "root", "password": password,
		"tls_mode": "disable", "read_only": false, "require_write_confirmation": true,
		"timeout": "60s", "page_limit": 100, "traversal_depth": 2,
	}
}

func arangoEndpoint(ctx context.Context, t *testing.T) (string, string) {
	t.Helper()
	if endpoint := os.Getenv("SHELLCN_ARANGODB_ENDPOINT"); endpoint != "" {
		password := os.Getenv("SHELLCN_ARANGODB_PASSWORD")
		if password == "" {
			password = integrationPassword
		}
		return endpoint, password
	}
	return startArangoContainer(ctx, t), integrationPassword
}

func startArangoContainer(ctx context.Context, t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_ARANGODB_ENDPOINT is not set")
	}
	name := "shellcn-arangodb-it-" + time.Now().UTC().Format("20060102150405")
	if out, err := runDocker(ctx, "run", "-d", "--rm", "--name", name,
		"-p", "127.0.0.1::8529",
		"-e", "ARANGO_ROOT_PASSWORD="+integrationPassword,
		integrationImage); err != nil {
		t.Skipf("cannot start %s: %v\n%s", integrationImage, err, out)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = runDocker(cleanup, "rm", "-f", name)
	})
	out, err := runDocker(ctx, "port", name, "8529/tcp")
	if err != nil {
		t.Fatalf("docker port: %v\n%s", err, out)
	}
	host, port, err := net.SplitHostPort(strings.TrimSpace(strings.Split(out, "\n")[0]))
	if err != nil {
		t.Fatalf("unexpected docker port output %q: %v", out, err)
	}
	endpoint := "http://" + net.JoinHostPort(host, port)
	waitForArango(ctx, t, endpoint, name)
	return endpoint
}

func waitForArango(ctx context.Context, t *testing.T, endpoint, container string) {
	t.Helper()
	deadline := time.Now().Add(180 * time.Second)
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/_api/version", nil)
		req.SetBasicAuth("root", integrationPassword)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			logs, _ := runDocker(context.Background(), "logs", "--tail", "80", container)
			t.Fatalf("ArangoDB did not become ready: %v\n%s", err, logs)
		}
		time.Sleep(time.Second)
	}
}

func runDocker(ctx context.Context, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	return string(out), err
}

// installFoxxService builds a minimal service bundle so the Foxx panels are
// exercised against a real mounted microservice.
func installFoxxService(ctx context.Context, t *testing.T, s *Session, database string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "service.zip")
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create bundle: %v", err)
	}
	archive := zip.NewWriter(file)
	files := map[string]string{
		"manifest.json": `{
  "$schema": "http://json.schemastore.org/foxx-manifest",
  "name": "shellcn-it",
  "version": "1.0.0",
  "license": "Apache-2.0",
  "author": "ShellCN",
  "description": "Integration test service",
  "main": "index.js",
  "engines": {"arangodb": "^3.0.0"},
  "configuration": {"greeting": {"type": "string", "default": "hello", "description": "Greeting"}}
}`,
		"index.js": `'use strict';
const createRouter = require('@arangodb/foxx/router');
const router = createRouter();
router.get('/hello', function (req, res) { res.json({ok: true}); })
  .summary('Say hello')
  .description('Returns a static payload.');
module.context.use(router);`,
		"README.md": "# ShellCN integration service\n\nUsed by the ShellCN ArangoDB plugin integration test.\n",
	}
	for name, content := range files {
		w, err := archive.Create(name)
		if err != nil {
			t.Fatalf("add %s: %v", name, err)
		}
		if _, err := w.Write([]byte(content)); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	if err := archive.Close(); err != nil {
		t.Fatalf("close archive: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close bundle: %v", err)
	}
	mount := foxxMount
	if err := s.client.InstallFoxxService(ctx, database, path, &driver.FoxxDeploymentOptions{Mount: &mount}); err != nil {
		t.Fatalf("install Foxx service: %v", err)
	}
}

func callRoute(t *testing.T, route plugin.Route, ctx context.Context, sess plugin.Session, params map[string]string, query map[string][]string, body []byte) any {
	t.Helper()
	out, err := route.Handle(plugin.NewRequestContext(ctx, plugin.User{}, sess, params, query, body))
	if err != nil {
		t.Fatalf("%s: %v", route.ID, err)
	}
	return out
}

func callWithStorage(t *testing.T, h *contribtest.Harness, ctx context.Context, id string, sess plugin.Session, store plugin.Storage, params map[string]string, body []byte) any {
	t.Helper()
	route := h.Route(id)
	rc := plugin.NewRequestContext(ctx, plugin.User{}, sess, params, nil, body)
	rc.Storage = store
	out, err := route.Handle(rc)
	if err != nil {
		t.Fatalf("%s: %v", id, err)
	}
	return out
}

func runStreamRaw(t *testing.T, h *contribtest.Harness, ctx context.Context, id string, sess plugin.Session, params map[string]string, input string, timeout time.Duration) string {
	t.Helper()
	streamCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return string(h.Stream(streamCtx, id, sess, params, nil, []byte(input)))
}

func decodeFrames(t *testing.T, raw string) []map[string]any {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(raw))
	frames := make([]map[string]any, 0, 4)
	for dec.More() {
		frame := map[string]any{}
		if err := dec.Decode(&frame); err != nil {
			t.Fatalf("decode frame: %v (in %s)", err, raw)
		}
		frames = append(frames, frame)
	}
	return frames
}

func firstFrame(t *testing.T, raw string) map[string]any {
	t.Helper()
	frames := decodeFrames(t, raw)
	if len(frames) == 0 {
		t.Fatalf("expected at least one frame, got %q", raw)
	}
	return frames[0]
}

func rowsOf(t *testing.T, value any) []map[string]any {
	t.Helper()
	return pageOf(t, value).Items
}

func pageOf(t *testing.T, value any) plugin.Page[map[string]any] {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal page: %v", err)
	}
	var decoded plugin.Page[map[string]any]
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("decode page: %v", err)
	}
	return decoded
}

func mapOf(t *testing.T, value any) map[string]any {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal object: %v", err)
	}
	out := map[string]any{}
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("decode object: %v", err)
	}
	return out
}

func docContent(t *testing.T, value any) map[string]any {
	t.Helper()
	wrapper := mapOf(t, value)
	content, ok := wrapper["content"].(string)
	if !ok {
		t.Fatalf("expected a code-editor content payload, got %#v", wrapper)
	}
	out := map[string]any{}
	if err := json.Unmarshal([]byte(content), &out); err != nil {
		t.Fatalf("content is not JSON: %v (%s)", err, content)
	}
	return out
}

func graphPayloadOf(t *testing.T, value any) graphPayload {
	t.Helper()
	data, _ := json.Marshal(value)
	var out graphPayload
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("decode graph payload: %v", err)
	}
	return out
}

func savedOf(t *testing.T, value any) savedQueryRow {
	t.Helper()
	saved, ok := value.(savedQueryRow)
	if !ok {
		t.Fatalf("expected a saved query row, got %#v", value)
	}
	return saved
}

func completionOf(t *testing.T, value any) map[string]bool {
	t.Helper()
	data, _ := json.Marshal(value)
	var items []struct {
		Label string `json:"label"`
	}
	if err := json.Unmarshal(data, &items); err != nil {
		t.Fatalf("decode completion: %v", err)
	}
	out := map[string]bool{}
	for _, item := range items {
		out[item.Label] = true
	}
	return out
}

func treeNodes(t *testing.T, value any) []plugin.TreeNode {
	t.Helper()
	page, ok := value.(plugin.Page[plugin.TreeNode])
	if !ok {
		t.Fatalf("expected a tree page, got %#v", value)
	}
	return page.Items
}

func hasField(rows []map[string]any, key, want string) bool {
	for _, item := range rows {
		if fmt.Sprint(item[key]) == want {
			return true
		}
	}
	return false
}

func indexRows(rows []map[string]any, key string) map[string]map[string]any {
	out := make(map[string]map[string]any, len(rows))
	for _, item := range rows {
		out[fmt.Sprint(item[key])] = item
	}
	return out
}

func queryValues(pairs ...string) map[string][]string {
	out := map[string][]string{}
	for i := 0; i+1 < len(pairs); i += 2 {
		key := pairs[i]
		if key != "filter" && key != "sort" && key != "limit" && key != "cursor" {
			key = "p." + key
		}
		out[key] = []string{pairs[i+1]}
	}
	return out
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return data
}
