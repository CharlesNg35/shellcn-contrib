package cassandra

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

	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestCassandraPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_CASSANDRA_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_CASSANDRA_INTEGRATION=1 to run against Cassandra")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	cfg := integrationConfig(ctx, t)
	cfg["read_only"] = false
	cfg["require_destructive_confirmation"] = true
	cfg["row_limit"] = 50
	cfg["page_size"] = 50
	cfg["query_timeout"] = "20s"
	cfg["consistency"] = "LOCAL_ONE"

	sess, err := connect(ctx, plugin.ConnectConfig{
		Config: cfg,
		Net:    plugintest.DirectTransport(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	s := sess.(*Session)

	rc := plugin.NewRequestContext(ctx, plugin.User{ID: "u1", Username: "admin"}, s, nil, nil, mustJSON(t, map[string]any{
		"name":               "shellcn_it",
		"replication_class":  "SimpleStrategy",
		"replication_factor": 1,
		"durable_writes":     true,
		"if_not_exists":      true,
	}))
	if _, err := createKeyspace(rc); err != nil {
		t.Fatalf("create keyspace route: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		_ = execCQL(cleanupCtx, s, `DROP KEYSPACE IF EXISTS "shellcn_it"`)
	})

	createTableRC := plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"keyspace": "shellcn_it"}, nil, mustJSON(t, map[string]any{
		"name": "people",
		"columns": []map[string]any{
			{"name": "id", "type": "uuid"},
			{"name": "name", "type": "text"},
			{"name": "access_token", "type": "text"},
		},
		"primary_key":   "id",
		"if_not_exists": true,
	}))
	if _, err := createTable(createTableRC); err != nil {
		t.Fatalf("create table route: %v", err)
	}
	if err := execCQL(ctx, s, `TRUNCATE "shellcn_it"."people"`); err != nil {
		t.Fatalf("truncate table: %v", err)
	}
	if err := execCQL(ctx, s, `INSERT INTO "shellcn_it"."people" (id, name, access_token) VALUES (11111111-1111-1111-1111-111111111111, 'alice', 'secret-token')`); err != nil {
		t.Fatalf("insert row: %v", err)
	}

	baseRC := plugin.NewRequestContext(ctx, plugin.User{ID: "u1", Username: "admin"}, s, nil, nil, nil)
	waitForTable(ctx, t, baseRC, "shellcn_it", "people")

	keyspaces, err := listKeyspaces(baseRC)
	if err != nil {
		t.Fatalf("list keyspaces: %v", err)
	}
	if !pageHasName(keyspaces.(plugin.Page[row]), "shellcn_it") {
		t.Fatalf("created keyspace was not listed: %#v", keyspaces)
	}

	// Keyspace drop round-trip: create a SEPARATE throwaway keyspace via the
	// handler, confirm it lists, then drop it via the handler and confirm it's gone.
	// Kept distinct from shellcn_it so later sub-steps still have their keyspace.
	dropKS := "shellcn_it_drop"
	createDropKSRC := plugin.NewRequestContext(ctx, plugin.User{}, s, nil, nil, mustJSON(t, map[string]any{
		"name":               dropKS,
		"replication_class":  "SimpleStrategy",
		"replication_factor": 1,
		"durable_writes":     true,
		"if_not_exists":      true,
	}))
	if _, err := createKeyspace(createDropKSRC); err != nil {
		t.Fatalf("create throwaway keyspace: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		_ = execCQL(cleanupCtx, s, `DROP KEYSPACE IF EXISTS "`+dropKS+`"`)
	})
	if list, err := listKeyspaces(baseRC); err != nil {
		t.Fatalf("list keyspaces before drop: %v", err)
	} else if !pageHasName(list.(plugin.Page[row]), dropKS) {
		t.Fatalf("throwaway keyspace %q was not listed before drop", dropKS)
	}
	if _, err := dropKeyspace(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"keyspace": dropKS}, nil, nil)); err != nil {
		t.Fatalf("drop keyspace: %v", err)
	}
	if list, err := listKeyspaces(baseRC); err != nil {
		t.Fatalf("list keyspaces after drop: %v", err)
	} else if pageHasName(list.(plugin.Page[row]), dropKS) {
		t.Fatalf("keyspace %q still listed after drop", dropKS)
	}

	// UDT create/drop round-trip in shellcn_it: create a type via the handler,
	// confirm it lists, drop it via the handler, confirm it's gone.
	if _, err := createType(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"keyspace": "shellcn_it"}, nil, mustJSON(t, map[string]any{
		"name": "address",
		"fields": []map[string]any{
			{"name": "street", "type": "text"},
			{"name": "zip", "type": "text"},
		},
		"if_not_exists": true,
	}))); err != nil {
		t.Fatalf("create type: %v", err)
	}
	typeListRC := plugin.NewRequestContext(ctx, plugin.User{}, s, nil, url.Values{"p.keyspace": {"shellcn_it"}}, nil)
	if types, err := listTypes(typeListRC); err != nil {
		t.Fatalf("list types after create: %v", err)
	} else if !pageHasName(types.(plugin.Page[row]), "address") {
		t.Fatalf("created type was not listed")
	}
	if _, err := dropType(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"keyspace": "shellcn_it", "name": "address"}, nil, nil)); err != nil {
		t.Fatalf("drop type: %v", err)
	}
	if types, err := listTypes(typeListRC); err != nil {
		t.Fatalf("list types after drop: %v", err)
	} else if pageHasName(types.(plugin.Page[row]), "address") {
		t.Fatalf("type still listed after drop")
	}

	// Materialized view drop round-trip: create an MV, drop it via the handler.
	// MVs are disabled by default in some Cassandra builds; skip this sub-check there.
	if err := execCQL(ctx, s, `CREATE MATERIALIZED VIEW "shellcn_it"."people_by_name" AS SELECT * FROM "shellcn_it"."people" WHERE name IS NOT NULL AND id IS NOT NULL PRIMARY KEY (name, id)`); err != nil {
		if strings.Contains(err.Error(), "Materialized views are disabled") {
			t.Log("materialized views disabled in this Cassandra build; skipping MV drop round-trip")
		} else {
			t.Fatalf("seed materialized view: %v", err)
		}
	} else {
		if _, err := dropView(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"keyspace": "shellcn_it", "view": "people_by_name"}, nil, nil)); err != nil {
			t.Fatalf("drop materialized view: %v", err)
		}
		views, err := listViews(plugin.NewRequestContext(ctx, plugin.User{}, s, nil, url.Values{"p.keyspace": {"shellcn_it"}}, nil))
		if err != nil {
			t.Fatalf("list views: %v", err)
		}
		if pageHasName(views.(plugin.Page[row]), "people_by_name") {
			t.Fatalf("materialized view still listed after drop")
		}
	}

	tableRC := plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"keyspace": "shellcn_it", "table": "people"}, nil, nil)
	rows, err := tableRows(tableRC)
	if err != nil {
		t.Fatalf("table rows: %v", err)
	}
	page := rows.(plugin.Page[row])
	if len(page.Items) != 1 || page.Items[0]["access_token"] != sqldb.RedactedValue {
		t.Fatalf("expected redacted table data, got %#v", page.Items)
	}

	result, err := executeQueryRequest(ctx, s, sqldb.QueryRequest{Query: `SELECT name, access_token FROM "shellcn_it"."people"`})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	tokenIndex := columnIndex(result.Columns, "access_token")
	if len(result.Rows) != 1 || tokenIndex < 0 || result.Rows[0][tokenIndex] != sqldb.RedactedValue {
		t.Fatalf("expected redacted query result, got columns=%#v rows=%#v", result.Columns, result.Rows)
	}

	for name, fn := range map[string]func(*plugin.RequestContext) (any, error){
		"columns":    tableColumnsRoute,
		"indexes":    tableIndexes,
		"definition": tableDefinition,
	} {
		if _, err := fn(tableRC); err != nil {
			t.Fatalf("table %s route: %v", name, err)
		}
	}

	// Editable data grid round-trip: insert a row, read it back, update it, delete
	// it, asserting at each step. Identity is the primary key (id), echoed as _key.
	rowID := "22222222-2222-2222-2222-222222222222"
	mutationRC := func(body map[string]any) *plugin.RequestContext {
		return plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"keyspace": "shellcn_it", "table": "people"}, nil, mustJSON(t, body))
	}
	if _, err := insertRow(mutationRC(map[string]any{"values": map[string]any{"id": rowID, "name": "bob", "access_token": "bob-token"}})); err != nil {
		t.Fatalf("insert row: %v", err)
	}
	bob := findRowByName(ctx, t, s, "name", "bob")
	if bob == nil {
		t.Fatalf("inserted row was not found")
	}
	rowKey, ok := bob["_key"].(map[string]any)
	if !ok || fmt.Sprint(rowKey["id"]) != rowID {
		t.Fatalf("expected _key carrying id %s, got %#v", rowID, bob["_key"])
	}
	if _, err := updateRow(mutationRC(map[string]any{"key": map[string]any{"id": rowID}, "values": map[string]any{"name": "robert"}})); err != nil {
		t.Fatalf("update row: %v", err)
	}
	if updated := findRowByName(ctx, t, s, "name", "robert"); updated == nil {
		t.Fatalf("updated row was not found after update")
	}
	if stale := findRowByName(ctx, t, s, "name", "bob"); stale != nil {
		t.Fatalf("row still has old name after update: %#v", stale)
	}
	if _, err := deleteRow(mutationRC(map[string]any{"key": map[string]any{"id": rowID}})); err != nil {
		t.Fatalf("delete row: %v", err)
	}
	if gone := findRowByName(ctx, t, s, "name", "robert"); gone != nil {
		t.Fatalf("row still present after delete: %#v", gone)
	}
	for name, fn := range map[string]func(*plugin.RequestContext) (any, error){
		"views":      listViews,
		"types":      listTypes,
		"functions":  listFunctions,
		"nodes":      listNodes,
		"completion": completionRoute,
	} {
		if _, err := fn(baseRC); err != nil {
			t.Fatalf("%s route: %v", name, err)
		}
	}

	// Column/index management via declarative DDL actions (CQL).
	ddlRC := func(body map[string]any) *plugin.RequestContext {
		return plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"keyspace": "shellcn_it", "table": "people"}, nil, mustJSON(t, body))
	}
	// Drops read the target name from a query param (the manifest actions pass
	// name: ${resource.name}), not the body.
	dropRC := func(name string) *plugin.RequestContext {
		return plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"keyspace": "shellcn_it", "table": "people", "name": name}, nil, nil)
	}
	if _, err := createIndex(ddlRC(map[string]any{"name": "ix_people_name", "column": "name"})); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if _, err := dropIndex(dropRC("ix_people_name")); err != nil {
		t.Fatalf("drop index: %v", err)
	}
	if _, err := dropColumn(dropRC("access_token")); err != nil {
		t.Fatalf("drop column: %v", err)
	}
}

func integrationConfig(ctx context.Context, t *testing.T) map[string]any {
	t.Helper()
	if raw := os.Getenv("SHELLCN_CASSANDRA_ADDR"); raw != "" {
		host, portText, err := net.SplitHostPort(raw)
		if err != nil {
			t.Fatalf("parse SHELLCN_CASSANDRA_ADDR: %v", err)
		}
		port, err := strconv.Atoi(portText)
		if err != nil {
			t.Fatalf("parse Cassandra port: %v", err)
		}
		return map[string]any{
			"hosts":     host,
			"port":      port,
			"auth":      authNone,
			"tls_mode":  "disable",
			"read_only": false,
		}
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_CASSANDRA_ADDR is not set")
	}
	name := "shellcn-cassandra-it-" + time.Now().UTC().Format("20060102150405")
	run(ctx, t, "docker", "run", "-d", "--rm", "--name", name,
		"-e", "CASSANDRA_CLUSTER_NAME=shellcn",
		"-e", "MAX_HEAP_SIZE=512M",
		"-e", "HEAP_NEWSIZE=128M",
		"-p", "127.0.0.1::9042",
		"cassandra:5")
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})
	out := run(ctx, t, "docker", "port", name, "9042/tcp")
	host, portText, err := net.SplitHostPort(strings.TrimSpace(out))
	if err != nil {
		t.Fatalf("unexpected docker port output: %q", out)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse docker port %q: %v", portText, err)
	}
	cfg := map[string]any{
		"hosts":       host,
		"port":        port,
		"auth":        authNone,
		"tls_mode":    "disable",
		"read_only":   false,
		"consistency": "LOCAL_ONE",
	}
	deadline := time.Now().Add(3 * time.Minute)
	var lastErr error
	for {
		sess, err := connect(ctx, plugin.ConnectConfig{
			Config: cfg,
			Net:    plugintest.DirectTransport(),
		})
		if err == nil {
			_ = sess.Close()
			return cfg
		}
		lastErr = err
		if time.Now().After(deadline) {
			logs := exec.CommandContext(ctx, "docker", "logs", "--tail", "160", name)
			out, _ := logs.CombinedOutput()
			t.Fatalf("cassandra container did not become ready: %v\n%s", lastErr, out)
		}
		time.Sleep(2 * time.Second)
	}
}

func waitForTable(ctx context.Context, t *testing.T, rc *plugin.RequestContext, keyspace, table string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		list, err := listTables(rc)
		if err == nil && pageHasScopedName(list.(plugin.Page[row]), keyspace, table) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("table %s.%s was not listed: %v", keyspace, table, err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("table %s.%s was not listed: %v", keyspace, table, ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
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
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
	return string(out)
}

func pageHasName(page plugin.Page[row], name string) bool {
	for _, item := range page.Items {
		if item["name"] == name {
			return true
		}
	}
	return false
}

func pageHasScopedName(page plugin.Page[row], namespace, name string) bool {
	for _, item := range page.Items {
		if item["keyspace"] == namespace && item["name"] == name {
			return true
		}
	}
	return false
}

// findRowByName scans the table via the data-grid handler (which attaches _key)
// and returns the first row whose column equals value, or nil when absent.
func findRowByName(ctx context.Context, t *testing.T, s *Session, column, value string) row {
	t.Helper()
	rc := plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"keyspace": "shellcn_it", "table": "people"}, nil, nil)
	res, err := tableRows(rc)
	if err != nil {
		t.Fatalf("table rows: %v", err)
	}
	for _, r := range res.(plugin.Page[row]).Items {
		if fmt.Sprint(r[column]) == value {
			return r
		}
	}
	return nil
}

func columnIndex(columns []string, column string) int {
	for i, name := range columns {
		if name == column {
			return i
		}
	}
	return -1
}
