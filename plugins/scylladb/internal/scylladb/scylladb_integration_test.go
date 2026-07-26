package scylladb

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

	contribtest "github.com/charlesng35/shellcn-contrib/shared/plugintest"
	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

const (
	itKeyspace = "shellcn_scylla_it"
	itTable    = "people"
	itRowID    = "11111111-1111-1111-1111-111111111111"
)

func TestScyllaDBPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_SCYLLADB_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_SCYLLADB_INTEGRATION=1 to run against ScyllaDB")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	cfg := integrationConfig(ctx, t)
	cfg["read_only"] = false
	cfg["require_destructive_confirmation"] = true
	cfg["row_limit"] = 50
	cfg["page_size"] = 50
	cfg["query_timeout"] = "30s"
	cfg["consistency"] = "LOCAL_ONE"

	p := New()
	sess, err := p.Connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	s := sess.(*Session)
	h := contribtest.NewHarness(t, p.Routes())

	dc := localDatacenter(ctx, t, s)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cleanupCancel()
		_ = execCQL(cleanupCtx, s, `DROP KEYSPACE IF EXISTS "`+itKeyspace+`"`)
	})

	// --- keyspace lifecycle -------------------------------------------------
	h.Call(ctx, "scylladb.keyspace.create", sess, nil, nil, mustJSON(t, map[string]any{
		"name":                   itKeyspace,
		"replication_class":      "NetworkTopologyStrategy",
		"datacenter_replication": map[string]any{dc: 1},
		"durable_writes":         true,
		"tablets":                "default",
		"if_not_exists":          true,
	}))
	keyspaces := pageItems(t, h.Call(ctx, "scylladb.keyspaces.list", sess, nil, nil, nil))
	if !hasField(keyspaces, "name", itKeyspace) {
		t.Fatalf("created keyspace was not listed: %#v", keyspaces)
	}
	// A detail lookup must not inherit the caller's list paging or filtering, or
	// a record past the first page would 404.
	detailQuery := url.Values{"limit": {"1"}, "cursor": {"1"}, "filter": {"no-such-keyspace"}}
	overview := h.Call(ctx, "scylladb.keyspace.overview", sess, map[string]string{"keyspace": itKeyspace}, detailQuery, nil).(row)
	if overview["name"] != itKeyspace {
		t.Fatalf("unexpected keyspace overview: %#v", overview)
	}

	// A throwaway keyspace proves the drop route really removes it.
	dropKS := itKeyspace + "_drop"
	h.Call(ctx, "scylladb.keyspace.create", sess, nil, nil, mustJSON(t, map[string]any{
		"name":                   dropKS,
		"replication_class":      "NetworkTopologyStrategy",
		"datacenter_replication": map[string]any{dc: 1},
		"durable_writes":         true,
		"tablets":                "disabled",
		"if_not_exists":          true,
	}))
	h.Call(ctx, "scylladb.keyspace.drop", sess, map[string]string{"keyspace": dropKS}, nil, nil)
	if hasField(pageItems(t, h.Call(ctx, "scylladb.keyspaces.list", sess, nil, nil, nil)), "name", dropKS) {
		t.Fatalf("keyspace %q is still listed after drop", dropKS)
	}

	// --- table + column + index DDL ----------------------------------------
	ksParams := map[string]string{"keyspace": itKeyspace}
	h.Call(ctx, "scylladb.table.create", sess, ksParams, nil, mustJSON(t, map[string]any{
		"name": itTable,
		"columns": []map[string]any{
			{"name": "id", "type": "uuid"},
			{"name": "name", "type": "text"},
			{"name": "access_token", "type": "text"},
		},
		"primary_key":         "id",
		"compaction_strategy": "IncrementalCompactionStrategy",
		"if_not_exists":       true,
	}))
	waitForTable(ctx, t, h, sess)

	tableParamValues := map[string]string{"keyspace": itKeyspace, "table": itTable}
	h.Call(ctx, "scylladb.column.add", sess, tableParamValues, nil, mustJSON(t, map[string]any{"name": "region", "type": "text"}))
	columns := pageItems(t, h.Call(ctx, "scylladb.table.columns", sess, tableParamValues, nil, nil))
	if !hasField(columns, "column_name", "region") {
		t.Fatalf("added column was not listed: %#v", columns)
	}
	if columns[0]["column_name"] != "id" || columns[0]["kind"] != "partition_key" || columns[0]["editable"] != false {
		t.Fatalf("partition key must sort first and stay read-only: %#v", columns[0])
	}
	h.Call(ctx, "scylladb.index.create", sess, tableParamValues, nil, mustJSON(t, map[string]any{"name": "ix_people_name", "column": "name"}))
	indexes := pageItems(t, h.Call(ctx, "scylladb.table.indexes", sess, tableParamValues, nil, nil))
	if !hasField(indexes, "index_name", "ix_people_name") {
		t.Fatalf("created index was not listed: %#v", indexes)
	}
	h.Call(ctx, "scylladb.index.drop", sess, map[string]string{"keyspace": itKeyspace, "name": "ix_people_name"}, nil, nil)

	definition := h.Call(ctx, "scylladb.table.definition", sess, tableParamValues, nil, nil).(row)
	if cql, _ := definition["cql"].(string); !strings.Contains(cql, `PRIMARY KEY ("id")`) {
		t.Fatalf("definition tab did not render the CREATE TABLE statement: %#v", definition["cql"])
	}

	// --- editable data grid -------------------------------------------------
	if err := execCQL(ctx, s, fmt.Sprintf(`INSERT INTO %s (id, name, access_token, region) VALUES (%s, 'alice', 'secret-token', 'eu')`,
		qualified(itKeyspace, itTable), itRowID)); err != nil {
		t.Fatalf("seed row: %v", err)
	}
	rows := pageItems(t, h.Call(ctx, "scylladb.table.rows", sess, tableParamValues, nil, nil))
	if len(rows) != 1 {
		t.Fatalf("expected one row, got %#v", rows)
	}
	if rows[0]["access_token"] != sqldb.RedactedValue {
		t.Fatalf("sensitive column was not redacted: %#v", rows[0])
	}
	seedToken, ok := rows[0]["_token"]
	if !ok {
		t.Fatalf("the row grid must project the partition token: %#v", rows[0])
	}
	rowKey, ok := rows[0]["_key"].(map[string]any)
	if !ok || fmt.Sprint(rowKey["id"]) != itRowID {
		t.Fatalf("expected _key carrying the primary key, got %#v", rows[0]["_key"])
	}

	newID := "22222222-2222-2222-2222-222222222222"
	h.Call(ctx, "scylladb.table.row.insert", sess, tableParamValues, nil, mustJSON(t, map[string]any{
		"values": map[string]any{"id": newID, "name": "bob", "region": "us"},
	}))
	if findRow(ctx, t, h, sess, "name", "bob") == nil {
		t.Fatal("inserted row was not found")
	}
	h.Call(ctx, "scylladb.table.row.update", sess, tableParamValues, nil, mustJSON(t, map[string]any{
		"key": map[string]any{"id": newID}, "values": map[string]any{"name": "robert"},
	}))
	if findRow(ctx, t, h, sess, "name", "robert") == nil {
		t.Fatal("updated row was not found")
	}
	if findRow(ctx, t, h, sess, "name", "bob") != nil {
		t.Fatal("row kept its old name after update")
	}
	h.Call(ctx, "scylladb.table.row.delete", sess, tableParamValues, nil, mustJSON(t, map[string]any{
		"key": map[string]any{"id": newID},
	}))
	if findRow(ctx, t, h, sess, "name", "robert") != nil {
		t.Fatal("row survived delete")
	}

	// The grid pages on driver page state: a CQL LIMIT would exhaust the query on
	// the first page and the coordinator would stop handing back a cursor.
	for i := range 60 {
		if err := execCQL(ctx, s, fmt.Sprintf(`INSERT INTO %s (id, name) VALUES (uuid(), 'page-%d')`,
			qualified(itKeyspace, itTable), i)); err != nil {
			t.Fatalf("seed paging rows: %v", err)
		}
	}
	firstPage := h.Call(ctx, "scylladb.table.rows", sess, tableParamValues, url.Values{"limit": {"20"}}, nil).(plugin.Page[row])
	if len(firstPage.Items) != 20 || firstPage.NextCursor == "" {
		t.Fatalf("the grid must page beyond its first %d rows: cursor=%q", len(firstPage.Items), firstPage.NextCursor)
	}
	secondPage := h.Call(ctx, "scylladb.table.rows", sess, tableParamValues,
		url.Values{"limit": {"20"}, "cursor": {firstPage.NextCursor}}, nil).(plugin.Page[row])
	if len(secondPage.Items) == 0 {
		t.Fatal("the second grid page is empty")
	}
	seenIDs := map[string]bool{}
	for _, r := range firstPage.Items {
		seenIDs[fmt.Sprint(r["id"])] = true
	}
	for _, r := range secondPage.Items {
		if seenIDs[fmt.Sprint(r["id"])] {
			t.Fatalf("the second grid page repeats row %v", r["id"])
		}
	}

	// The grid's free-text term is honored: the scan walks pages and only rows
	// that match come back.
	matched, cursor := 0, ""
	for range 10 {
		params := url.Values{"limit": {"20"}, "filter": {"alice"}}
		if cursor != "" {
			params.Set("cursor", cursor)
		}
		page := h.Call(ctx, "scylladb.table.rows", sess, tableParamValues, params, nil).(plugin.Page[row])
		for _, r := range page.Items {
			if r["name"] != "alice" {
				t.Fatalf("the grid search returned a row that does not match: %#v", r)
			}
			matched++
		}
		if cursor = page.NextCursor; cursor == "" {
			break
		}
	}
	if cursor != "" {
		t.Fatal("the grid search never reached the end of the table")
	}
	if matched != 1 {
		t.Fatalf("the grid search found %d rows, want the single matching row", matched)
	}

	// A mutation key that is not the primary key must be refused server-side.
	deleteRoute := h.Route("scylladb.table.row.delete")
	if _, err := deleteRoute.Handle(plugin.NewRequestContext(ctx, plugin.User{}, sess, tableParamValues, nil,
		mustJSON(t, map[string]any{"key": map[string]any{"name": "alice"}}))); err == nil {
		t.Fatal("a non-primary-key row key must be rejected")
	}

	// --- partition locator --------------------------------------------------
	located := h.Call(ctx, "scylladb.partition.locate", sess, tableParamValues, nil, mustJSON(t, map[string]any{
		"partition_key": map[string]any{"id": itRowID},
	})).(row)
	if fmt.Sprint(located["token"]) != fmt.Sprint(seedToken) {
		t.Fatalf("locator token %v does not match the grid token %v", located["token"], seedToken)
	}
	owner := h.Call(ctx, "scylladb.partition.owner", sess, map[string]string{
		"keyspace": itKeyspace, "table": itTable, "token": fmt.Sprint(located["token"]),
	}, nil, nil).(row)
	if fmt.Sprint(owner["primary_replica"]) == "" || owner["shard"] == nil {
		t.Fatalf("partition owner must name a replica and a shard: %#v", owner)
	}

	// --- views, types, functions -------------------------------------------
	viewName := itTable + "_by_name"
	mvErr := execCQL(ctx, s, fmt.Sprintf(
		`CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE name IS NOT NULL AND id IS NOT NULL PRIMARY KEY (name, id)`,
		qualified(itKeyspace, viewName), qualified(itKeyspace, itTable)))
	views := pageItems(t, h.Call(ctx, "scylladb.views.list", sess, nil, url.Values{"p.keyspace": {itKeyspace}}, nil))
	if mvErr == nil {
		if !hasField(views, "name", viewName) {
			t.Fatalf("materialized view was not listed: %#v", views)
		}
		h.Call(ctx, "scylladb.view.definition", sess, map[string]string{"keyspace": itKeyspace, "table": viewName}, nil, nil)
		h.Call(ctx, "scylladb.view.drop", sess, map[string]string{"keyspace": itKeyspace, "view": viewName}, nil, nil)
		if hasField(pageItems(t, h.Call(ctx, "scylladb.views.list", sess, nil, url.Values{"p.keyspace": {itKeyspace}}, nil)), "name", viewName) {
			t.Fatal("materialized view is still listed after drop")
		}
	} else {
		t.Logf("materialized views unavailable on this build (%v); covering the routes without one", mvErr)
		h.CallNoFail(ctx, "scylladb.view.definition", sess, map[string]string{"keyspace": itKeyspace, "table": viewName})
		h.CallNoFail(ctx, "scylladb.view.drop", sess, map[string]string{"keyspace": itKeyspace, "view": viewName})
	}

	h.Call(ctx, "scylladb.type.create", sess, ksParams, nil, mustJSON(t, map[string]any{
		"name": "address",
		"fields": []map[string]any{
			{"name": "street", "type": "text"},
			{"name": "zip", "type": "text"},
		},
		"if_not_exists": true,
	}))
	typeQuery := url.Values{"p.keyspace": {itKeyspace}}
	if !hasField(pageItems(t, h.Call(ctx, "scylladb.types.list", sess, nil, typeQuery, nil)), "name", "address") {
		t.Fatal("created type was not listed")
	}
	h.Call(ctx, "scylladb.type.overview", sess, map[string]string{"keyspace": itKeyspace, "name": "address"}, nil, nil)
	h.Call(ctx, "scylladb.type.drop", sess, map[string]string{"keyspace": itKeyspace, "name": "address"}, nil, nil)
	if hasField(pageItems(t, h.Call(ctx, "scylladb.types.list", sess, nil, typeQuery, nil)), "name", "address") {
		t.Fatal("type is still listed after drop")
	}

	h.Call(ctx, "scylladb.functions.list", sess, nil, nil, nil)
	// User-defined functions need to be enabled per node, so the overview route
	// is exercised without assuming one exists.
	h.CallNoFail(ctx, "scylladb.function.overview", sess, map[string]string{"id": itKeyspace + ".missing()"})

	// --- navigation trees ---------------------------------------------------
	tree := treeItems(t, h.Call(ctx, "scylladb.keyspaces.tree", sess, nil, nil, nil))
	if !hasTreeLabel(tree, itKeyspace) {
		t.Fatalf("keyspace tree is missing the test keyspace: %#v", tree)
	}
	relations := treeItems(t, h.Call(ctx, "scylladb.relations.tree", sess, nil, url.Values{"p.keyspace": {itKeyspace}}, nil))
	if !hasTreeLabel(relations, itTable) {
		t.Fatalf("relations tree is missing the test table: %#v", relations)
	}
	h.Call(ctx, "scylladb.types.tree", sess, nil, typeQuery, nil)
	h.Call(ctx, "scylladb.functions.tree", sess, nil, nil, nil)
	nodeTree := treeItems(t, h.Call(ctx, "scylladb.nodes.tree", sess, nil, nil, nil))
	if len(nodeTree) == 0 {
		t.Fatal("node tree is empty")
	}

	// --- cluster, nodes, topology ------------------------------------------
	clusterPage := pageItems(t, h.Call(ctx, "scylladb.cluster.list", sess, nil, nil, nil))
	if len(clusterPage) != 1 {
		t.Fatalf("cluster list must return exactly one row: %#v", clusterPage)
	}
	cluster := h.Call(ctx, "scylladb.cluster.overview", sess, nil, nil, nil).(row)
	if intValue(cluster["nodes_total"]) < 1 || intValue(cluster["shards_total"]) < 1 {
		t.Fatalf("cluster overview must report nodes and shards: %#v", cluster)
	}
	if cluster["health"] != "healthy" {
		t.Fatalf("single-node cluster should be healthy: %#v", cluster["health"])
	}
	nodes := pageItems(t, h.Call(ctx, "scylladb.nodes.list", sess, nil, nil, nil))
	if len(nodes) == 0 {
		t.Fatal("node list is empty")
	}
	address := fmt.Sprint(nodes[0]["address"])
	if fmt.Sprint(nodes[0]["status"]) != "UN" {
		t.Fatalf("a live single node must report UN, got %#v", nodes[0]["status"])
	}
	if intValue(nodes[0]["tokens"]) == 0 {
		t.Fatalf("node must report its vnode tokens: %#v", nodes[0])
	}
	nodeDetail := h.Call(ctx, "scylladb.node.overview", sess, map[string]string{"address": address},
		url.Values{"limit": {"1"}, "filter": {"no-such-node"}}, nil).(row)
	if intValue(nodeDetail["shards"]) < 1 {
		t.Fatalf("node overview must report the shard count: %#v", nodeDetail)
	}
	runtime := pageItems(t, h.Call(ctx, "scylladb.node.runtime", sess, map[string]string{"address": address}, nil, nil))
	if len(runtime) == 0 {
		t.Fatal("system.runtime_info returned no counters")
	}
	// system.clients is node-local: the cluster list unions the ring and the node
	// panel pins the query to the node it is about.
	clients := pageItems(t, h.Call(ctx, "scylladb.clients.list", sess, nil, nil, nil))
	if len(clients) == 0 {
		t.Fatal("this driver's own connections should appear in system.clients")
	}
	if fmt.Sprint(clients[0]["node"]) == "" {
		t.Fatalf("cluster-wide client rows must name their node: %#v", clients[0])
	}
	nodeClients := pageItems(t, h.Call(ctx, "scylladb.clients.list", sess, map[string]string{"address": address}, nil, nil))
	if len(nodeClients) == 0 {
		t.Fatalf("the node client list must be pinned to %s, not the default coordinator", address)
	}
	if _, err := h.Route("scylladb.clients.list").Handle(plugin.NewRequestContext(ctx, plugin.User{}, sess,
		map[string]string{"address": "203.0.113.9"}, nil, nil)); err == nil {
		t.Fatal("an address outside the ring must be rejected rather than silently answered by the coordinator")
	}
	config := pageItems(t, h.Call(ctx, "scylladb.config.list", sess, nil, url.Values{"limit": {"500"}}, nil))
	if len(config) == 0 {
		t.Fatal("system.config returned no parameters")
	}
	ring := pageItems(t, h.Call(ctx, "scylladb.tokenring.list", sess, ksParams, url.Values{"limit": {"20"}}, nil))
	if len(ring) == 0 {
		t.Fatal("token ring is empty for the test keyspace")
	}
	if _, err := strconv.ParseInt(fmt.Sprint(ring[0]["end_token"]), 10, 64); err != nil {
		t.Fatalf("token ring rows must carry parseable tokens: %#v", ring[0])
	}
	h.Call(ctx, "scylladb.compactions.list", sess, nil, nil, nil)
	h.Call(ctx, "scylladb.table.estimates", sess, tableParamValues, nil, nil)
	h.Call(ctx, "scylladb.table.large", sess, tableParamValues, nil, nil)
	h.Call(ctx, "scylladb.slowlog.list", sess, nil, nil, nil)
	h.Call(ctx, "scylladb.completion", sess, nil, nil, nil)

	// --- CQL console --------------------------------------------------------
	consoleOut := string(h.Stream(ctx, "scylladb.query", sess, nil, nil, []byte(
		`{"query":"SELECT release_version FROM system.local;"}`+"\n"+
			`{"query":"TRUNCATE `+itKeyspace+`.does_not_exist;"}`+"\n")))
	if !strings.Contains(consoleOut, "release_version") {
		t.Fatalf("console did not return the query result: %s", truncate(consoleOut))
	}
	if !strings.Contains(consoleOut, "requiresConfirmation") {
		t.Fatalf("a destructive statement must ask for confirmation: %s", truncate(consoleOut))
	}
	// A malformed frame leaves the JSON decoder unable to resynchronise, so the
	// console has to end the stream instead of spinning on the same bytes.
	malformedCtx, malformedCancel := context.WithTimeout(ctx, 15*time.Second)
	malformed := string(h.Stream(malformedCtx, "scylladb.query", sess, nil, nil, []byte("{not json\n")))
	malformedCancel()
	if !strings.Contains(malformed, "Invalid CQL request.") {
		t.Fatalf("a malformed console frame must be reported: %s", truncate(malformed))
	}
	if strings.Count(malformed, "Invalid CQL request.") != 1 {
		t.Fatalf("the console must report a malformed frame once, not in a loop: %s", truncate(malformed))
	}
	cancelled := h.Call(ctx, "scylladb.query.cancel", sess, nil, nil, nil).(actionResult)
	if cancelled.OK {
		t.Fatal("no query is running, so cancel should report nothing cancelled")
	}

	// --- tracing ------------------------------------------------------------
	traced := h.Call(ctx, "scylladb.trace.run", sess, nil, nil, mustJSON(t, map[string]any{
		"query": fmt.Sprintf("SELECT * FROM %s LIMIT 10", qualified(itKeyspace, itTable)),
	})).(row)
	sessionID := fmt.Sprint(traced["session_id"])
	if sessionID == "" {
		t.Fatalf("trace run did not return a session id: %#v", traced)
	}
	waitForTrace(ctx, t, h, sess, sessionID)
	traceDetail := h.Call(ctx, "scylladb.trace.overview", sess, map[string]string{"id": sessionID}, nil, nil).(row)
	if int64Value(traceDetail["duration_us"]) <= 0 {
		t.Fatalf("trace session has no coordinator duration: %#v", traceDetail)
	}
	events := pageItems(t, h.Call(ctx, "scylladb.trace.events", sess, map[string]string{"id": sessionID}, nil, nil))
	if len(events) == 0 {
		t.Fatal("trace has no events")
	}
	if events[0]["time"] == "" || events[0]["activity"] == "" {
		t.Fatalf("trace events must carry a timestamp and an activity: %#v", events[0])
	}
	if !hasField(pageItems(t, h.Call(ctx, "scylladb.traces.list", sess, nil, url.Values{"limit": {"200"}}, nil)), "session_id", sessionID) {
		t.Fatalf("trace %s was not listed", sessionID)
	}

	// --- streams ------------------------------------------------------------
	// Server-push streams run until their context ends, so each one gets its own
	// budget rather than sharing a single deadline.
	clusterFrame := decodeFrame(t, streamOnce(ctx, t, h, "scylladb.cluster.metrics", sess, nil))
	if clusterFrame["shardsTotal"] == nil || clusterFrame["memoryTotal"] == nil {
		t.Fatalf("cluster metrics frame is missing live counters: %#v", clusterFrame)
	}
	if clusterFrame["storageTotal"] == nil || clusterFrame["clientConnections"] == nil {
		t.Fatalf("cluster metrics frame is missing capacity counters: %#v", clusterFrame)
	}
	tableFrame := decodeFrame(t, streamOnce(ctx, t, h, "scylladb.table.metrics", sess, tableParamValues))
	if tableFrame["readLatencyMs"] == nil || tableFrame["cacheHitRate"] == nil {
		t.Fatalf("table metrics frame is missing latency/cache counters: %#v", tableFrame)
	}
	if tableFrame["largePartitions"] == nil || tableFrame["compactionsLastHour"] == nil {
		t.Fatalf("table metrics frame is missing compaction counters: %#v", tableFrame)
	}
	watchFrame := decodeFrame(t, streamOnce(ctx, t, h, "scylladb.cluster.watch", sess, nil))
	if watchFrame["type"] != "modified" {
		t.Fatalf("cluster watch must push resource events: %#v", watchFrame)
	}
	nodeEvent := decodeFrame(t, streamOnce(ctx, t, h, "scylladb.nodes.watch", sess, nil))
	if nodeEvent["type"] != "added" {
		t.Fatalf("the first node watch frame must announce the node: %#v", nodeEvent)
	}
	if ref, _ := nodeEvent["ref"].(map[string]any); ref == nil || ref["kind"] != "node" {
		t.Fatalf("node watch events must carry a node ref: %#v", nodeEvent)
	}
	streamOnce(ctx, t, h, "scylladb.compactions.watch", sess, nil)

	canvasOut := string(h.Stream(ctx, "scylladb.topology", sess, nil, nil,
		[]byte(`{"type":"ready","width":1280,"height":720,"dpr":2,"theme":"dark"}`+"\n")))
	if !strings.Contains(canvasOut, `"type":"clear"`) || !strings.Contains(canvasOut, `"type":"arc"`) {
		t.Fatalf("topology canvas did not draw a ring: %s", truncate(canvasOut))
	}
	if !strings.Contains(canvasOut, `"regions"`) || !strings.Contains(canvasOut, `"shard:0"`) {
		t.Fatalf("topology canvas did not expose shard hit targets: %s", truncate(canvasOut))
	}

	// --- saved queries (storage-scoped) -------------------------------------
	storage := newFakeStorage()
	saveRoute := h.Route("scylladb.query.save")
	saved, err := saveRoute.Handle(plugin.NewRequestContext(ctx, plugin.User{ID: "u1"}, sess, nil, nil, mustJSON(t, map[string]any{
		"name": "people sample", "query": "SELECT * FROM " + itKeyspace + "." + itTable + " LIMIT 10;", "scope": storageScopeConnection,
	})).WithStorage(storage))
	if err != nil {
		t.Fatalf("save CQL: %v", err)
	}
	savedKey := fmt.Sprint(saved.(row)["key"])
	listRoute := h.Route("scylladb.queries.list")
	listed, err := listRoute.Handle(plugin.NewRequestContext(ctx, plugin.User{ID: "u1"}, sess, nil, url.Values{}, nil).WithStorage(storage))
	if err != nil {
		t.Fatalf("list saved CQL: %v", err)
	}
	if !hasField(pageItems(t, listed), "name", "people sample") {
		t.Fatalf("saved CQL was not listed: %#v", listed)
	}
	deleteQueryRoute := h.Route("scylladb.query.delete")
	if _, err := deleteQueryRoute.Handle(plugin.NewRequestContext(ctx, plugin.User{ID: "u1"}, sess, map[string]string{"key": savedKey}, nil, nil).WithStorage(storage)); err != nil {
		t.Fatalf("delete saved CQL: %v", err)
	}

	// --- table teardown -----------------------------------------------------
	h.Call(ctx, "scylladb.column.drop", sess, map[string]string{"keyspace": itKeyspace, "table": itTable, "name": "access_token"}, nil, nil)
	h.Call(ctx, "scylladb.table.truncate", sess, tableParamValues, nil, nil)
	if remaining := pageItems(t, h.Call(ctx, "scylladb.table.rows", sess, tableParamValues, nil, nil)); len(remaining) != 0 {
		t.Fatalf("truncate left rows behind: %#v", remaining)
	}
	if !hasScopedField(pageItems(t, h.Call(ctx, "scylladb.tables.list", sess, nil, url.Values{"p.keyspace": {itKeyspace}}, nil)), itKeyspace, itTable) {
		t.Fatal("table disappeared before the drop step")
	}
	h.Call(ctx, "scylladb.table.drop", sess, tableParamValues, nil, nil)
	if hasScopedField(pageItems(t, h.Call(ctx, "scylladb.tables.list", sess, nil, url.Values{"p.keyspace": {itKeyspace}}, nil)), itKeyspace, itTable) {
		t.Fatal("table is still listed after drop")
	}

	h.AssertAllCovered()
}

func integrationConfig(ctx context.Context, t *testing.T) map[string]any {
	t.Helper()
	if raw := os.Getenv("SHELLCN_SCYLLADB_ADDR"); raw != "" {
		host, portText, err := net.SplitHostPort(raw)
		if err != nil {
			t.Fatalf("parse SHELLCN_SCYLLADB_ADDR: %v", err)
		}
		port, err := strconv.Atoi(portText)
		if err != nil {
			t.Fatalf("parse ScyllaDB port: %v", err)
		}
		return map[string]any{"hosts": host, "port": port, "auth": authNone, "tls_mode": "disable"}
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_SCYLLADB_ADDR is not set")
	}
	name := "shellcn-scylladb-it-" + time.Now().UTC().Format("20060102150405")
	run(ctx, t, "docker", "run", "-d", "--rm", "--name", name,
		"-p", "127.0.0.1::9042",
		"scylladb/scylla:2026.2",
		"--smp", "1", "--memory", "1G", "--developer-mode", "1", "--overprovisioned", "1")
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})
	out := run(ctx, t, "docker", "port", name, "9042/tcp")
	published := strings.TrimSpace(strings.Split(out, "\n")[0])
	if _, _, err := net.SplitHostPort(published); err != nil {
		t.Fatalf("unexpected docker port output: %q", out)
	}
	// The contact point is the published host port, while the driver reaches the
	// node's own address on the container's CQL port after ring discovery.
	cfg := map[string]any{
		"hosts": published, "port": 9042, "auth": authNone, "tls_mode": "disable",
		"read_only": false, "consistency": "LOCAL_ONE",
	}
	waitForCQLPort(ctx, t, published)
	deadline := time.Now().Add(4 * time.Minute)
	var lastErr error
	for {
		sess, err := connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
		if err == nil {
			_ = sess.Close()
			return cfg
		}
		lastErr = err
		if time.Now().After(deadline) {
			logs, _ := exec.CommandContext(ctx, "docker", "logs", "--tail", "120", name).CombinedOutput()
			t.Fatalf("ScyllaDB container did not become ready: %v\n%s", lastErr, logs)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("ScyllaDB container did not become ready: %v", ctx.Err())
		case <-time.After(5 * time.Second):
		}
	}
}

func waitForCQLPort(ctx context.Context, t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Minute)
	for {
		conn, err := (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, "tcp", addr)
		if err == nil {
			_ = conn.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("ScyllaDB never accepted CQL connections on %s: %v", addr, err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("ScyllaDB never accepted CQL connections on %s: %v", addr, ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
}

func localDatacenter(ctx context.Context, t *testing.T, s *Session) string {
	t.Helper()
	rows, err := queryRows(ctx, s, `SELECT data_center FROM system.local`, nil)
	if err != nil || len(rows) == 0 {
		t.Fatalf("read local datacenter: %v", err)
	}
	return fmt.Sprint(rows[0]["data_center"])
}

func waitForTable(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		listed := pageItems(t, h.Call(ctx, "scylladb.tables.list", sess, nil, url.Values{"p.keyspace": {itKeyspace}}, nil))
		if hasScopedField(listed, itKeyspace, itTable) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("table %s.%s was not listed", itKeyspace, itTable)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("table %s.%s was not listed: %v", itKeyspace, itTable, ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// waitForTrace lets the coordinator flush the trace session before assertions.
func waitForTrace(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, sessionID string) {
	t.Helper()
	route := h.Route("scylladb.trace.overview")
	deadline := time.Now().Add(20 * time.Second)
	for {
		out, err := route.Handle(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"id": sessionID}, nil, nil))
		if err == nil {
			if detail, ok := out.(row); ok && int64Value(detail["duration_us"]) > 0 {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("trace %s never appeared in system_traces: %v", sessionID, err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("trace %s never appeared: %v", sessionID, ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func findRow(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, column, value string) row {
	t.Helper()
	for _, r := range pageItems(t, h.Call(ctx, "scylladb.table.rows", sess, map[string]string{"keyspace": itKeyspace, "table": itTable}, nil, nil)) {
		if fmt.Sprint(r[column]) == value {
			return r
		}
	}
	return nil
}

// streamOnce drives a server-push stream long enough for its first frame, then
// lets its context expire so the handler tears down cleanly.
func streamOnce(parent context.Context, t *testing.T, h *contribtest.Harness, id string, sess plugin.Session, params map[string]string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 8*time.Second)
	defer cancel()
	return h.Stream(ctx, id, sess, params, nil, nil)
}

func pageItems(t *testing.T, value any) []row {
	t.Helper()
	page, ok := value.(plugin.Page[row])
	if !ok {
		t.Fatalf("expected a row page, got %T", value)
	}
	return page.Items
}

func treeItems(t *testing.T, value any) []plugin.TreeNode {
	t.Helper()
	page, ok := value.(plugin.Page[plugin.TreeNode])
	if !ok {
		t.Fatalf("expected a tree page, got %T", value)
	}
	return page.Items
}

func hasField(rows []row, key, value string) bool {
	for _, r := range rows {
		if fmt.Sprint(r[key]) == value {
			return true
		}
	}
	return false
}

func hasScopedField(rows []row, keyspace, name string) bool {
	for _, r := range rows {
		if fmt.Sprint(r["keyspace"]) == keyspace && fmt.Sprint(r["name"]) == name {
			return true
		}
	}
	return false
}

func hasTreeLabel(nodes []plugin.TreeNode, label string) bool {
	for _, n := range nodes {
		if n.Label == label {
			return true
		}
	}
	return false
}

func decodeFrame(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	line := strings.TrimSpace(strings.Split(string(raw), "\n")[0])
	if line == "" {
		t.Fatal("stream produced no frames")
	}
	var frame map[string]any
	if err := json.Unmarshal([]byte(line), &frame); err != nil {
		t.Fatalf("decode stream frame %q: %v", line, err)
	}
	return frame
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal JSON: %v", err)
	}
	return raw
}

func run(ctx context.Context, t *testing.T, name string, args ...string) string {
	t.Helper()
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
	return string(out)
}

func truncate(s string) string {
	if len(s) > 600 {
		return s[:600] + "..."
	}
	return s
}
