package trino

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	contribtest "github.com/charlesng35/shellcn-contrib/shared/plugintest"
	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

// mockResult is one canned answer from the fake coordinator.
type mockResult struct {
	Columns     []string
	Types       []string
	Rows        [][]any
	UpdateCount int64
	Error       string
}

// mockTrino speaks enough of the Trino client protocol to drive the real
// database/sql driver: POST /v1/statement returns a queued handle, and the
// nextUri it points at returns the columns and rows.
type mockTrino struct {
	server  *httptest.Server
	respond func(statement string) mockResult

	mu      sync.Mutex
	seq     int
	pending map[string]mockResult
	seen    []string
	headers []http.Header
}

func newMockTrino(t *testing.T, respond func(statement string) mockResult) *mockTrino {
	t.Helper()
	m := &mockTrino{respond: respond, pending: map[string]mockResult{}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/statement", m.start)
	mux.HandleFunc("GET /v1/statement/{id}/{token}", m.next)
	mux.HandleFunc("DELETE /v1/statement/{id}/{token}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	m.server = httptest.NewServer(mux)
	t.Cleanup(m.server.Close)
	return m
}

func (m *mockTrino) start(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	statement := preparedStatementText(string(body), r.Header)
	m.mu.Lock()
	m.seq++
	id := fmt.Sprintf("20260101_000000_%05d_mock", m.seq)
	m.seen = append(m.seen, statement)
	m.headers = append(m.headers, r.Header.Clone())
	m.pending[id] = m.respond(statement)
	m.mu.Unlock()
	writeJSON(w, map[string]any{
		"id":      id,
		"infoUri": m.server.URL + "/ui/query.html?" + id,
		"nextUri": m.server.URL + "/v1/statement/" + id + "/1",
		"stats":   map[string]any{"state": "QUEUED", "queuedSplits": 1},
	})
}

func (m *mockTrino) next(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	m.mu.Lock()
	res, ok := m.pending[id]
	delete(m.pending, id)
	m.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	payload := map[string]any{
		"id":      id,
		"infoUri": m.server.URL + "/ui/query.html?" + id,
		"stats": map[string]any{
			"state": "FINISHED", "scheduled": true, "nodes": 1,
			"totalSplits": 3, "completedSplits": 3,
			"processedRows": len(res.Rows), "processedBytes": 128,
			"elapsedTimeMillis": 12, "cpuTimeMillis": 4,
		},
	}
	if res.Error != "" {
		payload["error"] = map[string]any{
			"message": res.Error, "errorCode": 1, "errorName": "MOCK_ERROR", "errorType": "USER_ERROR",
		}
		writeJSON(w, payload)
		return
	}
	if len(res.Columns) > 0 {
		columns := make([]map[string]any, len(res.Columns))
		for i, name := range res.Columns {
			dataType := "varchar"
			if i < len(res.Types) && res.Types[i] != "" {
				dataType = res.Types[i]
			}
			columns[i] = map[string]any{
				"name": name, "type": dataType,
				"typeSignature": map[string]any{"rawType": dataType, "arguments": []any{}},
			}
		}
		payload["columns"] = columns
		if len(res.Rows) > 0 {
			payload["data"] = res.Rows
		}
	}
	if res.UpdateCount > 0 {
		payload["updateCount"] = res.UpdateCount
		payload["updateType"] = "MOCK"
	}
	writeJSON(w, payload)
}

func (m *mockTrino) statements() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.seen...)
}

func (m *mockTrino) requestHeaders() []http.Header {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]http.Header(nil), m.headers...)
}

func (m *mockTrino) sawStatement(t *testing.T, substring string) string {
	t.Helper()
	for _, statement := range m.statements() {
		if strings.Contains(statement, substring) {
			return statement
		}
	}
	t.Fatalf("no statement contained %q; saw %v", substring, m.statements())
	return ""
}

func (m *mockTrino) address(t *testing.T) (string, int) {
	t.Helper()
	u, err := url.Parse(m.server.URL)
	if err != nil {
		t.Fatalf("parse mock URL: %v", err)
	}
	host, portText, err := net.SplitHostPort(u.Host)
	if err != nil {
		t.Fatalf("split mock host: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse mock port: %v", err)
	}
	return host, port
}

// preparedStatementText recovers the user statement when the driver used
// explicit PREPARE: the body becomes "EXECUTE _trino_go USING ..." and the real
// text travels in the prepared-statement header.
func preparedStatementText(body string, headers http.Header) string {
	if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(body)), "EXECUTE ") {
		return body
	}
	for _, value := range headers.Values("X-Trino-Prepared-Statement") {
		name, encoded, ok := strings.Cut(value, "=")
		if !ok || name != "_trino_go" {
			continue
		}
		if decoded, err := url.QueryUnescape(encoded); err == nil {
			return decoded
		}
	}
	return body
}

func writeJSON(w http.ResponseWriter, payload map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}

// mockCoordinator answers every statement the plugin issues. Matching is by the
// distinctive fragment of each statement so a change in projection or paging is
// visible in the assertions rather than silently accepted.
func mockCoordinator(statement string) mockResult {
	normalized := strings.Join(strings.Fields(statement), " ")
	switch {
	case strings.Contains(normalized, "node_id, http_uri, node_version FROM system.runtime.nodes WHERE coordinator"):
		return mockResult{Columns: []string{"node_id", "http_uri", "node_version"}, Rows: [][]any{{"n1", "http://n1:8080", "483"}}}
	case strings.Contains(normalized, "SELECT node_version FROM system.runtime.nodes WHERE coordinator"):
		return mockResult{Columns: []string{"node_version"}, Rows: [][]any{{"483"}}}
	case strings.Contains(normalized, "count(*) AS total_nodes"):
		return mockResult{Columns: []string{"total_nodes", "active_nodes"}, Types: []string{"bigint", "bigint"}, Rows: [][]any{{2, 2}}}
	case strings.Contains(normalized, "running_queries"):
		return mockResult{Columns: []string{"running_queries", "queued_queries", "failed_queries"}, Types: []string{"bigint", "bigint", "bigint"}, Rows: [][]any{{1, 0, 3}}}
	case strings.Contains(normalized, `count(*) AS "schemas"`):
		return mockResult{Columns: []string{"schemas"}, Types: []string{"bigint"}, Rows: [][]any{{4}}}
	case strings.Contains(normalized, "count(*) AS catalogs"):
		return mockResult{Columns: []string{"catalogs"}, Types: []string{"bigint"}, Rows: [][]any{{2}}}
	case strings.Contains(normalized, "FROM system.metadata.catalogs WHERE catalog_name"):
		return mockResult{Columns: []string{"catalog", "connector"}, Rows: [][]any{{"tpch", "tpch"}}}
	case strings.Contains(normalized, "SELECT catalog_name FROM system.metadata.catalogs"):
		return mockResult{Columns: []string{"catalog_name"}, Rows: [][]any{{"tpch"}, {"memory"}}}
	case strings.Contains(normalized, "FROM system.metadata.catalogs"):
		return mockResult{Columns: []string{"catalog", "connector"}, Rows: [][]any{{"memory", "memory"}, {"tpch", "tpch"}}}
	case strings.Contains(normalized, "SELECT schema_name FROM"):
		return mockResult{Columns: []string{"schema_name"}, Rows: [][]any{{"tiny"}}}
	case strings.Contains(normalized, "information_schema.schemata"):
		return mockResult{Columns: []string{"schema"}, Rows: [][]any{{"tiny"}, {"sf1"}}}
	case strings.Contains(normalized, "information_schema.tables"):
		return mockResult{Columns: []string{"name", "schema", "type"}, Rows: [][]any{
			{"nation", "tiny", "BASE TABLE"},
			{"nation_view", "tiny", "VIEW"},
		}}
	case strings.Contains(normalized, "SELECT table_name, column_name FROM"):
		return mockResult{Columns: []string{"table_name", "column_name"}, Rows: [][]any{{"nation", "nationkey"}}}
	case strings.Contains(normalized, "information_schema.columns"):
		return mockResult{
			Columns: []string{"column_name", "data_type", "is_nullable", "column_default", "ordinal_position", "comment"},
			Types:   []string{"varchar", "varchar", "varchar", "varchar", "bigint", "varchar"},
			Rows: [][]any{
				{"nationkey", "bigint", "NO", nil, 1, nil},
				{"name", "varchar(25)", "YES", nil, 2, "the name"},
				{"access_token", "varchar", "YES", nil, 3, nil},
			},
		}
	case strings.HasPrefix(normalized, "SHOW STATS FOR"):
		return mockResult{
			Columns: []string{"column_name", "data_size", "distinct_values_count", "nulls_fraction", "row_count", "low_value", "high_value"},
			Types:   []string{"varchar", "double", "double", "double", "double", "varchar", "varchar"},
			Rows:    [][]any{{"nationkey", nil, 25.0, 0.0, nil, "0", "24"}},
		}
	case strings.HasPrefix(normalized, "SHOW CREATE TABLE"):
		return mockResult{Columns: []string{"Create Table"}, Rows: [][]any{{"CREATE TABLE tpch.tiny.nation (\n  nationkey bigint\n)"}}}
	case strings.HasPrefix(normalized, "SHOW CREATE VIEW"):
		return mockResult{Columns: []string{"Create View"}, Rows: [][]any{{"CREATE VIEW tpch.tiny.nation_view AS SELECT 1"}}}
	case strings.HasPrefix(normalized, "SHOW SESSION"):
		return mockResult{
			Columns: []string{"Name", "Value", "Default", "Type", "Description"},
			Rows: [][]any{
				{"query_priority", "1", "1", "integer", "Priority of queries"},
				{"join_distribution_type", "AUTOMATIC", "AUTOMATIC", "varchar", "Join distribution type"},
			},
		}
	case strings.Contains(normalized, "FROM system.runtime.nodes WHERE node_id"):
		return mockResult{Columns: []string{"node_id", "http_uri", "node_version", "coordinator", "state"}, Types: []string{"varchar", "varchar", "varchar", "boolean", "varchar"}, Rows: [][]any{{"n1", "http://n1:8080", "483", true, "active"}}}
	case strings.Contains(normalized, "FROM system.runtime.nodes"):
		return mockResult{Columns: []string{"node_id", "http_uri", "node_version", "coordinator", "state"}, Types: []string{"varchar", "varchar", "varchar", "boolean", "varchar"}, Rows: [][]any{
			{"n1", "http://n1:8080", "483", true, "active"},
			{"n2", "http://n2:8080", "483", false, "active"},
		}}
	case strings.Contains(normalized, "FROM system.runtime.queries"):
		return mockResult{
			Columns: []string{"query_id", "state", "user", "source", "query", "resource_group", "created", "started", "end", "queued_time_ms", "analysis_time_ms", "planning_time_ms", "elapsed_ms"},
			Types:   []string{"varchar", "varchar", "varchar", "varchar", "varchar", "varchar", "varchar", "varchar", "varchar", "bigint", "bigint", "bigint", "bigint"},
			Rows: [][]any{{
				"20260101_000000_00001_mock", "RUNNING", "analyst", "shellcn", "SELECT 1",
				"global", "2026-01-01 00:00:00.000 UTC", "2026-01-01 00:00:00.100 UTC", nil, 1, 2, 3, 400,
			}},
		}
	case strings.HasPrefix(normalized, "CALL system.runtime.kill_query"),
		strings.HasPrefix(normalized, "CREATE SCHEMA"), strings.HasPrefix(normalized, "DROP SCHEMA"),
		strings.HasPrefix(normalized, "CREATE TABLE"), strings.HasPrefix(normalized, "DROP TABLE"),
		strings.HasPrefix(normalized, "DROP VIEW"), strings.HasPrefix(normalized, "ALTER TABLE"):
		return mockResult{}
	case strings.HasPrefix(normalized, "INSERT INTO"):
		return mockResult{Columns: []string{"rows"}, Types: []string{"bigint"}, Rows: [][]any{{1}}, UpdateCount: 1}
	case strings.HasPrefix(normalized, "SELECT * FROM"):
		return mockResult{Columns: []string{"nationkey", "name", "access_token"}, Types: []string{"bigint", "varchar", "varchar"}, Rows: [][]any{
			{0, "ALGERIA", "secret"},
			{1, "ARGENTINA", "secret"},
		}}
	case strings.Contains(normalized, "SELECT 1"):
		return mockResult{Columns: []string{"_col0"}, Types: []string{"bigint"}, Rows: [][]any{{1}}}
	}
	return mockResult{Error: "mock coordinator has no answer for: " + normalized}
}

func newMockSession(t *testing.T, respond func(string) mockResult) (*Session, *mockTrino) {
	t.Helper()
	if respond == nil {
		respond = mockCoordinator
	}
	mock := newMockTrino(t, respond)
	host, port := mock.address(t)
	sess, err := connect(t.Context(), plugin.ConnectConfig{
		Config: map[string]any{
			"host": host, "port": port, "username": "analyst",
			"catalog": "tpch", "schema": "tiny",
			"read_only": false, "require_destructive_confirmation": false,
			"query_timeout": "20s",
		},
		Net: plugintest.DirectTransport(),
	})
	if err != nil {
		t.Fatalf("connect to mock coordinator: %v", err)
	}
	t.Cleanup(func() { _ = sess.Close() })
	return sess.(*Session), mock
}

func TestEveryRouteRunsAgainstTheMockCoordinator(t *testing.T) {
	sess, mock := newMockSession(t, nil)
	ctx := t.Context()
	h := contribtest.NewHarness(t, New().Routes())

	table := map[string]string{"catalog": "tpch", "schema": "tiny", "table": "nation"}

	catalogs := asRowPage(t, h.Call(ctx, "trino.catalogs.list", sess, nil, nil, nil))
	if len(catalogs.Items) != 2 || catalogs.Items[0]["catalog"] != "memory" {
		t.Fatalf("unexpected catalogs: %#v", catalogs.Items)
	}
	ref, ok := catalogs.Items[0]["ref"].(plugin.ResourceIdentity)
	if !ok || ref.Kind != "catalog" || ref.UID != "memory" {
		t.Fatalf("catalog rows must carry a catalog ref: %#v", catalogs.Items[0])
	}

	tree := asTreePage(t, h.Call(ctx, "trino.catalogs.tree", sess, nil, nil, nil))
	if len(tree.Items) != 2 || tree.Items[0].ChildrenSource == nil || tree.Items[0].ChildrenSource.RouteID != "trino.schemas.tree" {
		t.Fatalf("catalog nodes must expand into schemas: %#v", tree.Items)
	}
	schemaNodes := asTreePage(t, h.Call(ctx, "trino.schemas.tree", sess, map[string]string{"catalog": "tpch"}, nil, nil))
	if len(schemaNodes.Items) != 2 || schemaNodes.Items[0].ChildrenSource.RouteID != "trino.relations.tree" {
		t.Fatalf("schema nodes must expand into relations: %#v", schemaNodes.Items)
	}
	relations := asTreePage(t, h.Call(ctx, "trino.relations.tree", sess, map[string]string{"catalog": "tpch", "schema": "tiny"}, nil, nil))
	if len(relations.Items) != 2 || !relations.Items[0].Leaf {
		t.Fatalf("relation nodes must be leaves: %#v", relations.Items)
	}
	if relations.Items[0].Ref.Kind != "table" || relations.Items[1].Ref.Kind != "view" {
		t.Fatalf("relation kinds must follow table_type: %#v", relations.Items)
	}
	h.Call(ctx, "trino.nodes.tree", sess, nil, nil, nil)
	h.Call(ctx, "trino.queries.tree", sess, nil, nil, nil)

	overview := asRow(t, h.Call(ctx, "trino.cluster.overview", sess, nil, nil, nil))
	if overview["version"] != "483" || overview["default_catalog"] != "tpch" {
		t.Fatalf("unexpected cluster overview: %#v", overview)
	}

	catalog := asRow(t, h.Call(ctx, "trino.catalog.overview", sess, map[string]string{"catalog": "tpch"}, nil, nil))
	if catalog["connector"] != "tpch" || catalog["schemas"] == nil {
		t.Fatalf("unexpected catalog overview: %#v", catalog)
	}

	schemas := asRowPage(t, h.Call(ctx, "trino.schemas.list", sess, map[string]string{"catalog": "tpch"}, nil, nil))
	if len(schemas.Items) != 2 || schemas.Items[0]["catalog"] != "tpch" {
		t.Fatalf("schema rows must be scoped by catalog: %#v", schemas.Items)
	}

	tables := asRowPage(t, h.Call(ctx, "trino.tables.list", sess, map[string]string{"catalog": "tpch", "schema": "tiny"}, nil, nil))
	if len(tables.Items) != 2 {
		t.Fatalf("unexpected tables: %#v", tables.Items)
	}
	h.Call(ctx, "trino.views.list", sess, map[string]string{"catalog": "tpch", "schema": "tiny"}, nil, nil)
	if !strings.Contains(mock.sawStatement(t, "table_type = ?"), "table_schema = ?") {
		t.Fatal("relation listings must filter by schema and type server-side")
	}

	rows := asRowPage(t, h.Call(ctx, "trino.table.rows", sess, table, nil, nil))
	if len(rows.Items) != 2 || rows.Items[0]["name"] != "ALGERIA" {
		t.Fatalf("unexpected table rows: %#v", rows.Items)
	}
	if rows.Items[0]["access_token"] != sqldb.RedactedValue {
		t.Fatal("sensitive columns must be redacted in the data grid")
	}
	if rows.Total != nil {
		t.Fatal("a Trino data grid must not claim an authoritative row count")
	}

	columns := asRowPage(t, h.Call(ctx, "trino.table.columns", sess, table, nil, nil))
	if len(columns.Items) != 3 || columns.Items[0]["name"] != "nationkey" {
		t.Fatalf("unexpected columns: %#v", columns.Items)
	}
	if columns.Items[0]["nullable"] != false || columns.Items[1]["nullable"] != true {
		t.Fatalf("is_nullable must be normalized to a boolean: %#v", columns.Items)
	}
	if columns.Items[0]["editable"] != true || columns.Items[2]["editable"] != false {
		t.Fatalf("redacted columns must not be editable: %#v", columns.Items)
	}

	stats := asRowPage(t, h.Call(ctx, "trino.table.stats", sess, table, nil, nil))
	if len(stats.Items) != 1 || stats.Items[0]["column_name"] != "nationkey" {
		t.Fatalf("unexpected statistics: %#v", stats.Items)
	}
	ddl := asRow(t, h.Call(ctx, "trino.table.ddl", sess, table, nil, nil))
	if !strings.HasPrefix(fmt.Sprint(ddl["ddl"]), "CREATE TABLE") {
		t.Fatalf("unexpected DDL payload: %#v", ddl)
	}
	h.Call(ctx, "trino.view.rows", sess, table, nil, nil)
	h.Call(ctx, "trino.view.columns", sess, table, nil, nil)
	definition := asRow(t, h.Call(ctx, "trino.view.definition", sess, table, nil, nil))
	if !strings.HasPrefix(fmt.Sprint(definition["ddl"]), "CREATE VIEW") {
		t.Fatalf("unexpected view definition: %#v", definition)
	}

	nodes := asRowPage(t, h.Call(ctx, "trino.nodes.list", sess, nil, nil, nil))
	if len(nodes.Items) != 2 || nodes.Items[0]["coordinator"] != true {
		t.Fatalf("unexpected nodes: %#v", nodes.Items)
	}
	node := asRow(t, h.Call(ctx, "trino.node.overview", sess, map[string]string{"node": "n1"}, nil, nil))
	if node["state"] != "active" {
		t.Fatalf("unexpected node overview: %#v", node)
	}

	queries := asRowPage(t, h.Call(ctx, "trino.queries.list", sess, nil, nil, nil))
	if len(queries.Items) != 1 || queries.Items[0]["state"] != "RUNNING" {
		t.Fatalf("unexpected queries: %#v", queries.Items)
	}
	queryID := fmt.Sprint(queries.Items[0]["query_id"])
	detail := asRow(t, h.Call(ctx, "trino.query.overview", sess, map[string]string{"query": queryID}, nil, nil))
	if detail["resource_group"] != "global" {
		t.Fatalf("unexpected query overview: %#v", detail)
	}
	statement := asRow(t, h.Call(ctx, "trino.query.statement", sess, map[string]string{"query": queryID}, nil, nil))
	if statement["sql"] != "SELECT 1" {
		t.Fatalf("unexpected query statement: %#v", statement)
	}
	h.Call(ctx, "trino.query.kill", sess, map[string]string{"query": queryID}, nil, nil)
	if !strings.Contains(mock.sawStatement(t, "kill_query"), "'"+queryID+"'") {
		t.Fatal("kill must target the selected query id")
	}

	session := asRowPage(t, h.Call(ctx, "trino.session.list", sess, nil, nil, nil))
	if len(session.Items) != 2 || session.Items[0]["name"] == nil {
		t.Fatalf("SHOW SESSION columns must be lower-cased: %#v", session.Items)
	}

	h.Call(ctx, "trino.schema.create", sess, map[string]string{"catalog": "memory"}, nil, []byte(`{"name":"demo","if_not_exists":true}`))
	mock.sawStatement(t, `CREATE SCHEMA IF NOT EXISTS "memory"."demo"`)
	h.Call(ctx, "trino.schema.drop", sess, map[string]string{"catalog": "memory", "schema": "demo"}, nil, nil)
	mock.sawStatement(t, `DROP SCHEMA IF EXISTS "memory"."demo"`)
	h.Call(ctx, "trino.table.create", sess, map[string]string{"catalog": "memory", "schema": "demo"},
		nil, []byte(`{"name":"events","columns":[{"name":"id","type":"bigint"}],"comment":"demo","if_not_exists":true}`))
	mock.sawStatement(t, `CREATE TABLE IF NOT EXISTS "memory"."demo"."events" ("id" bigint NOT NULL) COMMENT 'demo'`)
	h.Call(ctx, "trino.table.rename", sess, map[string]string{"catalog": "memory", "schema": "demo", "table": "events"}, nil, []byte(`{"name":"events2"}`))
	mock.sawStatement(t, `ALTER TABLE "memory"."demo"."events" RENAME TO "memory"."demo"."events2"`)
	h.Call(ctx, "trino.table.row.insert", sess, map[string]string{"catalog": "tpch", "schema": "tiny", "table": "nation"},
		nil, []byte(`{"values":{"nationkey":7,"name":"ATLANTIS"}}`))
	mock.sawStatement(t, `INSERT INTO "tpch"."tiny"."nation" ("name", "nationkey")`)
	h.Call(ctx, "trino.table.drop", sess, map[string]string{"catalog": "memory", "schema": "demo", "table": "events2"}, nil, nil)
	mock.sawStatement(t, `DROP TABLE IF EXISTS "memory"."demo"."events2"`)
	h.Call(ctx, "trino.view.drop", sess, map[string]string{"catalog": "tpch", "schema": "tiny", "table": "nation_view"}, nil, nil)
	mock.sawStatement(t, `DROP VIEW IF EXISTS "tpch"."tiny"."nation_view"`)

	completion := h.Call(ctx, "trino.completion", sess, map[string]string{"catalog": "tpch", "schema": "tiny"}, nil, nil)
	items, ok := completion.([]sqldb.CompletionItem)
	if !ok || len(items) == 0 {
		t.Fatalf("unexpected completion payload: %#v", completion)
	}

	frame := lastFrame(t, h.Stream(ctx, "trino.query", sess, table, nil, []byte(`{"query":"SELECT 1"}`)))
	if frame["error"] != nil {
		t.Fatalf("query stream returned an error frame: %#v", frame)
	}
	if cols, _ := frame["columns"].([]any); len(cols) != 1 {
		t.Fatalf("unexpected query columns: %#v", frame["columns"])
	}
	if frame["queryId"] == "" || frame["stats"] == nil {
		t.Fatalf("query results must carry the Trino query id and stats: %#v", frame)
	}

	metricsCtx, cancelMetrics := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancelMetrics()
	metrics := lastFrame(t, h.Stream(metricsCtx, "trino.cluster.metrics", sess, nil, nil, nil))
	if metrics["totalNodes"] == nil || metrics["runningQueries"] == nil {
		t.Fatalf("unexpected metrics frame: %#v", metrics)
	}

	cancelled := h.Call(ctx, "trino.query.cancel", sess, nil, nil, nil)
	if result, ok := cancelled.(actionResult); !ok || result.OK {
		t.Fatalf("cancel with no running query should report nothing to cancel: %#v", cancelled)
	}

	h.AssertAllCovered()
}

// The grid's search and sort must reach Trino, not just the page in hand.
func TestTableRowsPushesSearchSortAndCursorToTrino(t *testing.T) {
	sess, mock := newMockSession(t, nil)
	ctx := t.Context()
	table := map[string]string{"catalog": "tpch", "schema": "tiny", "table": "nation"}

	page, err := tableRows(plugin.NewRequestContext(ctx, plugin.User{}, sess, table,
		url.Values{"limit": {"1"}, "filter": {"alg"}, "sort": {"-name"}}, nil))
	if err != nil {
		t.Fatalf("table rows: %v", err)
	}
	rows := page.(plugin.Page[row])
	if len(rows.Items) != 1 {
		t.Fatalf("limit must bound the page, got %d rows", len(rows.Items))
	}
	if rows.NextCursor == "" {
		t.Fatal("a full page with more rows behind it must advertise a cursor")
	}
	statement := mock.sawStatement(t, "SELECT * FROM")
	for _, want := range []string{`FROM "tpch"."tiny"."nation"`, "LIKE UPPER(?)", `ORDER BY "name" DESC`, "OFFSET 0 LIMIT 2"} {
		if !strings.Contains(statement, want) {
			t.Fatalf("statement %q is missing %q", statement, want)
		}
	}
	// The emitted cursor must resume where the page stopped.
	if _, err := tableRows(plugin.NewRequestContext(ctx, plugin.User{}, sess, table,
		url.Values{"limit": {"1"}, "cursor": {rows.NextCursor}}, nil)); err != nil {
		t.Fatalf("continuation page: %v", err)
	}
	mock.sawStatement(t, "OFFSET 1 LIMIT 2")

	if _, err := tableRows(plugin.NewRequestContext(ctx, plugin.User{}, sess, table, url.Values{"sort": {"nope"}}, nil)); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("sorting by an unknown column must be rejected, got %v", err)
	}
	// The grid masks access_token, so it must not be usable to probe the value.
	if _, err := tableRows(plugin.NewRequestContext(ctx, plugin.User{}, sess, table, url.Values{"sort": {"access_token"}}, nil)); !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("ordering by a masked column must be refused, got %v", err)
	}
	filtered, err := tableRows(plugin.NewRequestContext(ctx, plugin.User{}, sess, table, url.Values{"filter": {"secret"}}, nil))
	if err != nil {
		t.Fatalf("filtered rows: %v", err)
	}
	if _, ok := filtered.(plugin.Page[row]); !ok {
		t.Fatalf("unexpected filtered page: %T", filtered)
	}
	if strings.Contains(mock.sawStatement(t, "LIKE UPPER(?)"), `"access_token"`) {
		t.Fatal("the free-text filter must not reach a masked column")
	}
}

// A table whose every column is uncastable cannot be filtered server-side, so
// the route reports no matches instead of quietly returning the whole page.
func TestTableRowsReturnsNothingWhenNoColumnCanBeFiltered(t *testing.T) {
	sess, mock := newMockSession(t, func(statement string) mockResult {
		if strings.Contains(statement, "information_schema.columns") {
			return mockResult{
				Columns: []string{"column_name", "data_type", "is_nullable", "column_default", "ordinal_position", "comment"},
				Types:   []string{"varchar", "varchar", "varchar", "varchar", "bigint", "varchar"},
				Rows:    [][]any{{"tags", "array(varchar)", "YES", nil, 1, nil}},
			}
		}
		return mockCoordinator(statement)
	})
	page, err := tableRows(plugin.NewRequestContext(t.Context(), plugin.User{}, sess,
		map[string]string{"catalog": "tpch", "schema": "tiny", "table": "nation"}, url.Values{"filter": {"alg"}}, nil))
	if err != nil {
		t.Fatalf("filtered rows: %v", err)
	}
	rows := page.(plugin.Page[row])
	if len(rows.Items) != 0 || rows.NextCursor != "" || rows.Total != nil {
		t.Fatalf("expected an empty, terminal page: %#v", rows)
	}
	for _, statement := range mock.statements() {
		if strings.HasPrefix(strings.TrimSpace(statement), "SELECT * FROM") {
			t.Fatalf("an unfilterable table must not be scanned: %q", statement)
		}
	}
}

func TestListRoutesRejectUnknownSortAndPageServerSide(t *testing.T) {
	sess, mock := newMockSession(t, nil)
	ctx := t.Context()

	if _, err := listNodes(plugin.NewRequestContext(ctx, plugin.User{}, sess, nil, url.Values{"sort": {"http_uri"}}, nil)); err != nil {
		t.Fatalf("known sort column rejected: %v", err)
	}
	mock.sawStatement(t, `ORDER BY "http_uri" OFFSET 0`)
	if _, err := listNodes(plugin.NewRequestContext(ctx, plugin.User{}, sess, nil, url.Values{"sort": {"secret"}}, nil)); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("unknown sort column must be rejected, got %v", err)
	}
	if _, err := listNodes(plugin.NewRequestContext(ctx, plugin.User{}, sess, nil, url.Values{"cursor": {"bogus"}}, nil)); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("a malformed cursor must be rejected, got %v", err)
	}
	if _, err := listQueries(plugin.NewRequestContext(ctx, plugin.User{}, sess, nil, url.Values{"filter": {"analyst"}}, nil)); err != nil {
		t.Fatalf("filtered queries: %v", err)
	}
	mock.sawStatement(t, `UPPER(CAST("user" AS VARCHAR)) LIKE`)
}

// The last page must not advertise a cursor, or the grid loops forever.
func TestPageSQLStopsAtTheEndOfTheDataset(t *testing.T) {
	sess, _ := newMockSession(t, nil)
	page, err := listNodes(plugin.NewRequestContext(t.Context(), plugin.User{}, sess, nil, url.Values{"limit": {"10"}}, nil))
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}
	nodes := page.(plugin.Page[row])
	if len(nodes.Items) != 2 || nodes.NextCursor != "" {
		t.Fatalf("a short page must end the listing: %d items, cursor %q", len(nodes.Items), nodes.NextCursor)
	}
}

// A panel opened on a catalog/schema must scope execution through the driver's
// per-request headers rather than mutating shared session state.
func TestQueryStreamSendsScopeHeaders(t *testing.T) {
	sess, mock := newMockSession(t, nil)
	h := contribtest.NewHarness(t, New().Routes())
	h.Stream(t.Context(), "trino.query", sess, map[string]string{"catalog": "memory", "schema": "demo"}, nil, []byte(`{"query":"SELECT 1"}`))
	found := false
	for _, headers := range mock.requestHeaders() {
		if headers.Get(catalogHeaderParam) == "memory" && headers.Get(schemaHeaderParam) == "demo" {
			found = true
		}
	}
	if !found {
		t.Fatal("the query editor must scope execution with X-Trino-Catalog/X-Trino-Schema")
	}
}

func TestQueryStreamReportsErrorsWithoutClosing(t *testing.T) {
	sess, _ := newMockSession(t, func(statement string) mockResult {
		if strings.Contains(statement, "system.runtime.nodes") {
			return mockCoordinator(statement)
		}
		return mockResult{Error: "Schema must be specified when session schema is not set"}
	})
	h := contribtest.NewHarness(t, New().Routes())
	out := h.Stream(t.Context(), "trino.query", sess, nil, nil, []byte("{\"query\":\"SELECT 1\"}\n{\"query\":\"SELECT 2\"}\n"))
	frames := decodeFrames(t, out)
	if len(frames) != 2 {
		t.Fatalf("an execution error must keep the stream open, got %d frames", len(frames))
	}
	for _, frame := range frames {
		if frame["error"] == nil {
			t.Fatalf("expected an error frame, got %#v", frame)
		}
	}
}

func TestQueryStreamChallengesDestructiveStatements(t *testing.T) {
	mock := newMockTrino(t, mockCoordinator)
	host, port := mock.address(t)
	sess, err := connect(t.Context(), plugin.ConnectConfig{
		Config: map[string]any{"host": host, "port": port, "username": "analyst", "read_only": false, "require_destructive_confirmation": true},
		Net:    plugintest.DirectTransport(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = sess.Close() })
	h := contribtest.NewHarness(t, New().Routes())
	frame := lastFrame(t, h.Stream(t.Context(), "trino.query", sess, nil, nil, []byte(`{"query":"DROP TABLE memory.demo.events"}`)))
	if frame["requiresConfirmation"] != true {
		t.Fatalf("a destructive statement must be challenged: %#v", frame)
	}
}

func TestConnectRejectsAnUnreachableCoordinator(t *testing.T) {
	mock := newMockTrino(t, func(string) mockResult { return mockResult{Error: "boom"} })
	host, port := mock.address(t)
	_, err := connect(t.Context(), plugin.ConnectConfig{
		Config: map[string]any{"host": host, "port": port, "username": "analyst"},
		Net:    plugintest.DirectTransport(),
	})
	if !errors.Is(err, plugin.ErrUnavailable) {
		t.Fatalf("a failing health probe must surface as unavailable, got %v", err)
	}
}

func asRowPage(t *testing.T, value any) plugin.Page[row] {
	t.Helper()
	page, ok := value.(plugin.Page[row])
	if !ok {
		t.Fatalf("expected a row page, got %T", value)
	}
	return page
}

func asTreePage(t *testing.T, value any) plugin.Page[plugin.TreeNode] {
	t.Helper()
	page, ok := value.(plugin.Page[plugin.TreeNode])
	if !ok {
		t.Fatalf("expected a tree page, got %T", value)
	}
	return page
}

func asRow(t *testing.T, value any) map[string]any {
	t.Helper()
	object, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("expected an object, got %T", value)
	}
	return object
}

func decodeFrames(t *testing.T, raw []byte) []map[string]any {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	var frames []map[string]any
	for {
		var frame map[string]any
		if err := dec.Decode(&frame); err != nil {
			if errors.Is(err, io.EOF) {
				return frames
			}
			t.Fatalf("decode stream frame: %v (raw %q)", err, raw)
		}
		frames = append(frames, frame)
	}
}

func lastFrame(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	frames := decodeFrames(t, raw)
	if len(frames) == 0 {
		t.Fatalf("stream produced no frames: %q", raw)
	}
	return frames[len(frames)-1]
}

// A malformed frame cannot be resynchronised by the JSON decoder, so the stream
// reports it once and closes rather than looping on the same bytes.
func TestQueryStreamClosesOnAMalformedFrame(t *testing.T) {
	sess, _ := newMockSession(t, nil)
	h := contribtest.NewHarness(t, New().Routes())
	frames := decodeFrames(t, h.Stream(t.Context(), "trino.query", sess, nil, nil, []byte("not json\n{\"query\":\"SELECT 1\"}\n")))
	if len(frames) != 1 || frames[0]["error"] == nil {
		t.Fatalf("expected exactly one error frame, got %#v", frames)
	}
}
