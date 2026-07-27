package trino

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

// TestTrinoPluginIntegration drives the plugin against a real coordinator. The
// stock image ships the tpch, tpcds, memory, jmx, and system catalogs, so the
// test needs no external data: tpch is read, memory is written.
func TestTrinoPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_TRINO_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_TRINO_INTEGRATION=1 to run against Trino")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cfg := integrationConfig(ctx, t)
	cfg["read_only"] = false
	cfg["require_destructive_confirmation"] = true
	cfg["row_limit"] = 50
	cfg["query_timeout"] = "60s"

	sess, err := connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	s := sess.(*Session)
	h := contribtest.NewHarness(t, New().Routes())

	catalogs := asRowPage(t, h.Call(ctx, "trino.catalogs.list", s, nil, nil, nil))
	if !pageHas(catalogs, "catalog", "tpch") || !pageHas(catalogs, "catalog", "memory") {
		t.Fatalf("built-in catalogs missing: %#v", catalogs.Items)
	}
	h.Call(ctx, "trino.catalogs.tree", s, nil, nil, nil)
	h.Call(ctx, "trino.schemas.tree", s, map[string]string{"catalog": "tpch"}, nil, nil)
	h.Call(ctx, "trino.relations.tree", s, map[string]string{"catalog": "tpch", "schema": "tiny"}, nil, nil)
	h.Call(ctx, "trino.nodes.tree", s, nil, nil, nil)
	h.Call(ctx, "trino.queries.tree", s, nil, nil, nil)

	overview := asRow(t, h.Call(ctx, "trino.cluster.overview", s, nil, nil, nil))
	if fmt.Sprint(overview["version"]) == "" || overview["active_nodes"] == nil {
		t.Fatalf("unexpected cluster overview: %#v", overview)
	}
	catalog := asRow(t, h.Call(ctx, "trino.catalog.overview", s, map[string]string{"catalog": "tpch"}, nil, nil))
	if catalog["connector"] != "tpch" {
		t.Fatalf("unexpected catalog overview: %#v", catalog)
	}

	schemas := asRowPage(t, h.Call(ctx, "trino.schemas.list", s, map[string]string{"catalog": "tpch"}, nil, nil))
	if !pageHas(schemas, "schema", "tiny") {
		t.Fatalf("tpch.tiny missing: %#v", schemas.Items)
	}
	tpchTiny := map[string]string{"catalog": "tpch", "schema": "tiny"}
	tables := asRowPage(t, h.Call(ctx, "trino.tables.list", s, tpchTiny, nil, nil))
	if !pageHas(tables, "name", "nation") {
		t.Fatalf("tpch.tiny.nation missing: %#v", tables.Items)
	}
	h.Call(ctx, "trino.views.list", s, tpchTiny, nil, nil)

	nation := map[string]string{"catalog": "tpch", "schema": "tiny", "table": "nation"}
	rows := asRowPage(t, h.Call(ctx, "trino.table.rows", s, nation, url.Values{"limit": {"5"}}, nil))
	if len(rows.Items) != 5 || rows.NextCursor == "" {
		t.Fatalf("expected a bounded first page with a cursor, got %d rows / %q", len(rows.Items), rows.NextCursor)
	}
	// The cursor must resume the scan rather than replay the first page.
	next := asRowPage(t, h.Call(ctx, "trino.table.rows", s, nation, url.Values{"limit": {"5"}, "cursor": {rows.NextCursor}}, nil))
	if len(next.Items) != 5 || fmt.Sprint(next.Items[0]["nationkey"]) == fmt.Sprint(rows.Items[0]["nationkey"]) {
		t.Fatalf("second page repeated the first: %#v", next.Items)
	}
	// Sorting is pushed to Trino, so it orders the whole table, not the page.
	desc := asRowPage(t, h.Call(ctx, "trino.table.rows", s, nation, url.Values{"limit": {"3"}, "sort": {"-nationkey"}}, nil))
	if len(desc.Items) != 3 || fmt.Sprint(desc.Items[0]["nationkey"]) != "24" {
		t.Fatalf("descending sort must span the table: %#v", desc.Items)
	}
	// The free-text filter is pushed down too.
	filtered := asRowPage(t, h.Call(ctx, "trino.table.rows", s, nation, url.Values{"filter": {"ALGERIA"}}, nil))
	if len(filtered.Items) != 1 {
		t.Fatalf("filter must match one nation, got %#v", filtered.Items)
	}
	if empty := asRowPage(t, h.Call(ctx, "trino.table.rows", s, nation, url.Values{"filter": {"zzz-nomatch"}}, nil)); len(empty.Items) != 0 {
		t.Fatalf("a non-matching filter must return nothing, got %#v", empty.Items)
	}

	columns := asRowPage(t, h.Call(ctx, "trino.table.columns", s, nation, nil, nil))
	if !pageHas(columns, "name", "nationkey") {
		t.Fatalf("unexpected columns: %#v", columns.Items)
	}
	stats := asRowPage(t, h.Call(ctx, "trino.table.stats", s, nation, nil, nil))
	if len(stats.Items) == 0 {
		t.Fatal("tpch reports column statistics")
	}
	ddl := asRow(t, h.Call(ctx, "trino.table.ddl", s, nation, nil, nil))
	if !strings.Contains(fmt.Sprint(ddl["ddl"]), "CREATE TABLE") {
		t.Fatalf("unexpected DDL: %#v", ddl)
	}

	nodes := asRowPage(t, h.Call(ctx, "trino.nodes.list", s, nil, nil, nil))
	if len(nodes.Items) == 0 {
		t.Fatal("the coordinator must appear in system.runtime.nodes")
	}
	nodeID := fmt.Sprint(nodes.Items[0]["node_id"])
	if node := asRow(t, h.Call(ctx, "trino.node.overview", s, map[string]string{"node": nodeID}, nil, nil)); node["state"] != "active" {
		t.Fatalf("unexpected node overview: %#v", node)
	}
	queries := asRowPage(t, h.Call(ctx, "trino.queries.list", s, nil, nil, nil))
	if len(queries.Items) == 0 {
		t.Fatal("the queries this test ran must appear in system.runtime.queries")
	}
	queryID := fmt.Sprint(queries.Items[0]["query_id"])
	h.Call(ctx, "trino.query.overview", s, map[string]string{"query": queryID}, nil, nil)
	h.Call(ctx, "trino.query.statement", s, map[string]string{"query": queryID}, nil, nil)
	// Killing an already finished query is rejected by the coordinator, which is
	// the route's real behaviour; only the transport path is asserted here.
	h.CallNoFail(ctx, "trino.query.kill", s, map[string]string{"query": queryID})

	session := asRowPage(t, h.Call(ctx, "trino.session.list", s, nil, nil, nil))
	if len(session.Items) == 0 || session.Items[0]["name"] == nil {
		t.Fatalf("SHOW SESSION must return lower-cased rows: %#v", session.Items)
	}
	if session.Total == nil {
		t.Fatal("a fully scanned SHOW SESSION should report a total")
	}

	schema := "shellcn_it_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	memory := map[string]string{"catalog": "memory", "schema": schema}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_, _ = s.db.ExecContext(cleanupCtx, `DROP TABLE IF EXISTS "memory".`+quoteIdent(schema)+`."events"`)
		_, _ = s.db.ExecContext(cleanupCtx, `DROP SCHEMA IF EXISTS "memory".`+quoteIdent(schema))
	})
	h.Call(ctx, "trino.schema.create", s, map[string]string{"catalog": "memory"}, nil, mustJSON(map[string]any{"name": schema, "if_not_exists": true}))
	h.Call(ctx, "trino.table.create", s, memory, nil, mustJSON(map[string]any{
		"name": "events", "if_not_exists": true,
		"columns": []map[string]any{
			{"name": "id", "type": "bigint", "nullable": true},
			{"name": "name", "type": "varchar", "nullable": true},
			{"name": "access_token", "type": "varchar", "nullable": true},
		},
	}))
	events := map[string]string{"catalog": "memory", "schema": schema, "table": "events"}
	h.Call(ctx, "trino.table.row.insert", s, events, nil, mustJSON(map[string]any{
		"values": map[string]any{"id": 1, "name": "alice"},
	}))
	// Seeded outside the route so the grid has a sensitive value to mask; the
	// route itself refuses to write a redacted column.
	if _, err := s.db.ExecContext(ctx, `INSERT INTO "memory".`+quoteIdent(schema)+`."events" VALUES (2, 'bob', 'secret-token')`); err != nil {
		t.Fatalf("seed sensitive row: %v", err)
	}
	if _, err := insertRow(plugin.NewRequestContext(ctx, plugin.User{}, s, events, nil,
		mustJSON(map[string]any{"values": map[string]any{"access_token": "nope"}}))); err == nil {
		t.Fatal("writing a redacted column must be refused")
	}
	inserted := asRowPage(t, h.Call(ctx, "trino.table.rows", s, events, url.Values{"sort": {"id"}}, nil))
	if len(inserted.Items) != 2 || inserted.Items[0]["name"] != "alice" {
		t.Fatalf("inserted row not visible: %#v", inserted.Items)
	}
	if inserted.Items[1]["access_token"] != sqldb.RedactedValue {
		t.Fatalf("sensitive columns must be redacted: %#v", inserted.Items[1])
	}

	// The editor result carries the coordinator's query id and live statistics.
	frame := lastFrame(t, h.Stream(ctx, "trino.query", s, tpchTiny, nil, mustJSON(map[string]any{"query": "SELECT name FROM nation ORDER BY name LIMIT 3"})))
	if frame["error"] != nil {
		t.Fatalf("query stream error: %#v", frame)
	}
	if rowsAny, _ := frame["rows"].([]any); len(rowsAny) != 3 {
		t.Fatalf("unexpected query rows: %#v", frame["rows"])
	}
	if !strings.HasPrefix(fmt.Sprint(frame["queryId"]), "2") {
		t.Fatalf("query result must carry the coordinator query id: %#v", frame["queryId"])
	}
	statsFrame, ok := frame["stats"].(map[string]any)
	if !ok || statsFrame["state"] == nil {
		t.Fatalf("query result must carry live statistics: %#v", frame["stats"])
	}
	// The panel's catalog/schema scope reaches the coordinator as request
	// headers, so an unqualified table name resolves.
	if frame["catalog"] != "tpch" || frame["schema"] != "tiny" {
		t.Fatalf("the result must echo the executing scope: %#v", frame)
	}

	confirm := lastFrame(t, h.Stream(ctx, "trino.query", s, memory, nil, mustJSON(map[string]any{"query": "DROP TABLE events"})))
	if confirm["requiresConfirmation"] != true {
		t.Fatalf("a destructive statement must be challenged first: %#v", confirm)
	}

	completion := h.Call(ctx, "trino.completion", s, tpchTiny, nil, nil)
	items, ok := completion.([]sqldb.CompletionItem)
	if !ok || len(items) == 0 {
		t.Fatalf("unexpected completion payload: %#v", completion)
	}

	metricsCtx, metricsCancel := context.WithTimeout(ctx, 2*time.Second)
	defer metricsCancel()
	metrics := lastFrame(t, h.Stream(metricsCtx, "trino.cluster.metrics", s, nil, nil, nil))
	if metrics["totalNodes"] == nil {
		t.Fatalf("unexpected metrics frame: %#v", metrics)
	}
	h.Call(ctx, "trino.query.cancel", s, nil, nil, nil)

	h.Call(ctx, "trino.view.rows", s, nation, url.Values{"limit": {"1"}}, nil)
	h.Call(ctx, "trino.view.columns", s, nation, nil, nil)
	seedView(ctx, t, s, schema)
	view := map[string]string{"catalog": "memory", "schema": schema, "table": "events_view"}
	h.CallNoFail(ctx, "trino.view.definition", s, view)
	h.CallNoFail(ctx, "trino.view.drop", s, view)

	h.Call(ctx, "trino.table.rename", s, events, nil, mustJSON(map[string]any{"name": "events2"}))
	renamed := map[string]string{"catalog": "memory", "schema": schema, "table": "events2"}
	h.Call(ctx, "trino.table.drop", s, renamed, nil, nil)
	h.Call(ctx, "trino.schema.drop", s, memory, nil, nil)

	h.AssertAllCovered()
}

// seedView creates a view when the connector supports one; the memory connector
// does not, so a failure here is not fatal to the run.
func seedView(ctx context.Context, t *testing.T, s *Session, schema string) {
	t.Helper()
	stmt := `CREATE VIEW "memory".` + quoteIdent(schema) + `."events_view" AS SELECT 1 AS x`
	if _, err := s.db.ExecContext(ctx, stmt); err != nil {
		t.Logf("memory connector does not support views: %v", err)
	}
}

func integrationConfig(ctx context.Context, t *testing.T) map[string]any {
	t.Helper()
	if raw := os.Getenv("SHELLCN_TRINO_URL"); raw != "" {
		return configFromURL(t, raw)
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_TRINO_URL is not set")
	}
	name := "shellcn-trino-it-" + time.Now().UTC().Format("20060102150405")
	run(ctx, t, "docker", "run", "-d", "--rm", "--name", name, "-p", "127.0.0.1::8080", "trinodb/trino:latest")
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})
	out := run(ctx, t, "docker", "port", name, "8080/tcp")
	host, portText, err := net.SplitHostPort(strings.TrimSpace(strings.Split(out, "\n")[0]))
	if err != nil {
		t.Fatalf("unexpected docker port output: %q", out)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse docker port %q: %v", portText, err)
	}
	cfg := map[string]any{
		"host": host, "port": port, "username": "shellcn",
		"catalog": "tpch", "schema": "tiny", "tls_mode": "disable", "read_only": false,
	}
	deadline := time.Now().Add(3 * time.Minute)
	var lastErr error
	for {
		probe, err := connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
		if err == nil {
			_ = probe.Close()
			return cfg
		}
		lastErr = err
		if time.Now().After(deadline) {
			logs, _ := exec.CommandContext(ctx, "docker", "logs", "--tail", "120", name).CombinedOutput()
			t.Fatalf("trino container did not become ready: %v\n%s", lastErr, logs)
		}
		time.Sleep(2 * time.Second)
	}
}

func configFromURL(t *testing.T, raw string) map[string]any {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse SHELLCN_TRINO_URL: %v", err)
	}
	port := defaultPort
	if u.Port() != "" {
		if port, err = strconv.Atoi(u.Port()); err != nil {
			t.Fatalf("parse URL port: %v", err)
		}
	}
	tlsMode := "disable"
	if u.Scheme == "https" {
		tlsMode = "verify-full"
	}
	password, _ := u.User.Password()
	auth := authUser
	if password != "" {
		auth = authPassword
	}
	return map[string]any{
		"host": u.Hostname(), "port": port,
		"catalog":  stringDefault(u.Query().Get("catalog"), "tpch"),
		"schema":   stringDefault(u.Query().Get("schema"), "tiny"),
		"auth":     auth,
		"username": stringDefault(u.User.Username(), "shellcn"),
		"password": password,
		"tls_mode": tlsMode,
	}
}

func run(ctx context.Context, t *testing.T, name string, args ...string) string {
	t.Helper()
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
	return string(out)
}

func pageHas(page plugin.Page[row], key, value string) bool {
	for _, item := range page.Items {
		if fmt.Sprint(item[key]) == value {
			return true
		}
	}
	return false
}

func mustJSON(body map[string]any) []byte {
	raw, _ := json.Marshal(body)
	return raw
}
