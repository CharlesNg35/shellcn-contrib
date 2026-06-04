package clickhouse

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

func TestClickHousePluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_CLICKHOUSE_INTEGRATION=1 to run against ClickHouse")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	cfg := integrationConfig(ctx, t)
	cfg["read_only"] = false
	cfg["require_destructive_confirmation"] = true
	cfg["row_limit"] = 50
	cfg["query_timeout"] = "10s"

	sess, err := connect(ctx, plugin.ConnectConfig{
		Config: cfg,
		Net:    plugintest.DirectTransport(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	s := sess.(*Session)

	createdDatabase := "shellcn_it_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if _, err := createDatabase(rowMutationRC(ctx, s, nil, map[string]any{"name": createdDatabase, "if_not_exists": true})); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = s.db.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS "+quoteIdent(createdDatabase))
	})
	databases, err := listDatabases(plugin.NewRequestContext(ctx, plugin.User{}, s, nil, nil, nil))
	if err != nil {
		t.Fatalf("list databases after create: %v", err)
	}
	if !pageHasName(databases.(plugin.Page[row]), createdDatabase) {
		t.Fatalf("created database was not listed: %#v", databases)
	}

	// View drop round-trip: create a view, drop it through the handler, verify gone.
	if _, err := s.db.ExecContext(ctx, "CREATE VIEW "+qualified(createdDatabase, "shellcn_v")+" AS SELECT 1 AS x"); err != nil {
		t.Fatalf("seed view: %v", err)
	}
	if _, err := dropView(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"database": createdDatabase, "view": "shellcn_v"}, nil, nil)); err != nil {
		t.Fatalf("drop view: %v", err)
	}
	var vcount uint64
	if err := s.db.QueryRowContext(ctx, "SELECT count() FROM system.tables WHERE database = ? AND name = ?", createdDatabase, "shellcn_v").Scan(&vcount); err != nil || vcount != 0 {
		t.Fatalf("expected view dropped, got %d err=%v", vcount, err)
	}

	seedStatements := []string{
		`CREATE TABLE IF NOT EXISTS shellcn_people (
  id UInt64,
  name String,
  access_token String
) ENGINE = MergeTree ORDER BY id`,
		`TRUNCATE TABLE shellcn_people`,
		`INSERT INTO shellcn_people (id, name, access_token) VALUES (1, 'alice', 'secret-token')`,
		`CREATE VIEW IF NOT EXISTS shellcn_people_view AS SELECT name FROM shellcn_people`,
	}
	for _, statement := range seedStatements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			t.Fatalf("seed database: %v", err)
		}
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = s.db.ExecContext(cleanupCtx, `DROP VIEW IF EXISTS shellcn_people_view`)
		_, _ = s.db.ExecContext(cleanupCtx, `DROP TABLE IF EXISTS shellcn_people`)
	})

	rc := plugin.NewRequestContext(ctx, plugin.User{ID: "u1", Username: "admin"}, s, nil, nil, nil)
	list, err := listTables(rc)
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	if !pageHasName(list.(plugin.Page[row]), "shellcn_people") {
		t.Fatalf("created table was not listed: %#v", list)
	}

	rows, err := tableRows(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"database": cfg["database"].(string), "table": "shellcn_people"}, nil, nil))
	if err != nil {
		t.Fatalf("table rows: %v", err)
	}
	page := rows.(plugin.Page[row])
	if len(page.Items) != 1 || page.Items[0]["access_token"] != sqldb.RedactedValue {
		t.Fatalf("expected redacted table data, got %#v", page.Items)
	}

	// Free-text search filters the data grid server-side (per-column).
	chPeople := map[string]string{"database": cfg["database"].(string), "table": "shellcn_people"}
	chMatch, err := tableRows(plugin.NewRequestContext(ctx, plugin.User{}, s, chPeople, url.Values{"filter": {"alice"}}, nil))
	if err != nil {
		t.Fatalf("filtered rows: %v", err)
	}
	if len(chMatch.(plugin.Page[row]).Items) != 1 {
		t.Fatalf("filter 'alice' should match 1 row, got %#v", chMatch.(plugin.Page[row]).Items)
	}
	chMiss, err := tableRows(plugin.NewRequestContext(ctx, plugin.User{}, s, chPeople, url.Values{"filter": {"zzz-nomatch"}}, nil))
	if err != nil {
		t.Fatalf("filtered rows (miss): %v", err)
	}
	if len(chMiss.(plugin.Page[row]).Items) != 0 {
		t.Fatalf("filter 'zzz-nomatch' should match 0 rows, got %#v", chMiss.(plugin.Page[row]).Items)
	}

	result, err := executeQueryRequest(ctx, s, sqldb.QueryRequest{Query: `SELECT name, access_token FROM shellcn_people`})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0][1] != sqldb.RedactedValue {
		t.Fatalf("expected redacted query result, got %#v", result.Rows)
	}

	tableRC := plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"database": cfg["database"].(string), "table": "shellcn_people"}, nil, nil)
	for name, fn := range map[string]func(*plugin.RequestContext) (any, error){
		"columns":     tableColumnsRoute,
		"indexes":     tableIndexes,
		"constraints": tableConstraints,
		"definition":  tableDefinition,
	} {
		if _, err := fn(tableRC); err != nil {
			t.Fatalf("table %s route: %v", name, err)
		}
	}
	viewRC := plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"database": cfg["database"].(string), "table": "shellcn_people_view"}, nil, nil)
	if _, err := tableDefinition(viewRC); err != nil {
		t.Fatalf("view definition route: %v", err)
	}

	for name, fn := range map[string]func(*plugin.RequestContext) (any, error){
		"databases":    listDatabases,
		"views":        listViews,
		"dictionaries": listDictionaries,
		"mutations":    listMutations,
		"merges":       listMerges,
		"processes":    listProcesses,
		"users":        listUsers,
		"completion":   completionRoute,
	} {
		if _, err := fn(rc); err != nil {
			t.Fatalf("%s route: %v", name, err)
		}
	}

	// Editable data grid round-trip. INSERT is synchronous, so the row reads back
	// immediately and carries its sorting-key (id) as _key. UPDATE/DELETE are
	// asynchronous ALTER ... mutations, so assert the handlers schedule them rather
	// than that the row changes synchronously.
	dataParams := map[string]string{"database": cfg["database"].(string), "table": "shellcn_people"}
	if _, err := insertRow(rowMutationRC(ctx, s, dataParams, map[string]any{"values": map[string]any{"id": uint64(2), "name": "bob", "access_token": "bob-token"}})); err != nil {
		t.Fatalf("insert row: %v", err)
	}
	inserted := findRow(ctx, t, s, dataParams, "name", "bob")
	if inserted == nil {
		t.Fatalf("inserted row was not found")
	}
	insertedKey, ok := inserted["_key"].(map[string]any)
	if !ok || fmt.Sprint(insertedKey["id"]) != "2" {
		t.Fatalf("expected _key carrying id 2, got %#v", inserted["_key"])
	}
	if _, err := updateRow(rowMutationRC(ctx, s, dataParams, map[string]any{"key": map[string]any{"id": uint64(2)}, "values": map[string]any{"name": "robert"}})); err != nil {
		t.Fatalf("update row mutation: %v", err)
	}
	if _, err := deleteRow(rowMutationRC(ctx, s, dataParams, map[string]any{"key": map[string]any{"id": uint64(2)}})); err != nil {
		t.Fatalf("delete row mutation: %v", err)
	}

	// Column/index management via declarative DDL actions. The drop handlers read
	// the target identifier from params (the action sends it as the "name" param),
	// not the body.
	db := cfg["database"].(string)
	ddlRC := func(params map[string]string) *plugin.RequestContext {
		full := map[string]string{"database": db, "table": "shellcn_people"}
		for k, v := range params {
			full[k] = v
		}
		return plugin.NewRequestContext(ctx, plugin.User{}, s, full, nil, nil)
	}
	// Data-skipping index create + drop round-trip via the declarative handlers.
	if _, err := createIndex(rowMutationRC(ctx, s, ddlParams(db), map[string]any{
		"name": "ix_name", "expression": "name", "type": "set(0)", "granularity": 4,
	})); err != nil {
		t.Fatalf("create index: %v", err)
	}
	var ixCount int
	if err := s.db.QueryRowContext(ctx, "SELECT count() FROM system.data_skipping_indices WHERE database = ? AND table = 'shellcn_people' AND name = 'ix_name'", db).Scan(&ixCount); err != nil || ixCount != 1 {
		t.Fatalf("expected ix_name index created, got %d err=%v", ixCount, err)
	}
	if _, err := dropIndex(ddlRC(map[string]string{"name": "ix_name"})); err != nil {
		t.Fatalf("drop index: %v", err)
	}
	if err := s.db.QueryRowContext(ctx, "SELECT count() FROM system.data_skipping_indices WHERE database = ? AND table = 'shellcn_people' AND name = 'ix_name'", db).Scan(&ixCount); err != nil || ixCount != 0 {
		t.Fatalf("expected ix_name index dropped, got %d err=%v", ixCount, err)
	}

	// MODIFY COLUMN round-trip: widen access_token to Nullable(String) before it is
	// dropped below, and verify the new type lands in system.columns.
	if _, err := alterColumn(rowMutationRC(ctx, s, ddlParams(db), map[string]any{
		"name": "access_token", "type": "String", "nullable": true,
	})); err != nil {
		t.Fatalf("alter column: %v", err)
	}
	var colType string
	if err := s.db.QueryRowContext(ctx, "SELECT type FROM system.columns WHERE database = ? AND table = 'shellcn_people' AND name = 'access_token'", db).Scan(&colType); err != nil {
		t.Fatalf("read modified column type: %v", err)
	}
	if colType != "Nullable(String)" {
		t.Fatalf("expected access_token modified to Nullable(String), got %q", colType)
	}

	if _, err := dropColumn(ddlRC(map[string]string{"name": "access_token"})); err != nil {
		t.Fatalf("drop column: %v", err)
	}
	var cols int
	if err := s.db.QueryRowContext(ctx, "SELECT count() FROM system.columns WHERE database = ? AND table = 'shellcn_people' AND name = 'access_token'", db).Scan(&cols); err != nil || cols != 0 {
		t.Fatalf("expected access_token column dropped, got %d err=%v", cols, err)
	}

	// RENAME TABLE round-trip: rename within the same database, verify the new name
	// is present and the old name is gone, then rename it back so later cleanup and
	// assertions that depend on shellcn_people still hold.
	if _, err := renameTable(rowMutationRC(ctx, s, ddlParams(db), map[string]any{"name": "shellcn_people_renamed"})); err != nil {
		t.Fatalf("rename table: %v", err)
	}
	if !tableExists(ctx, t, s, db, "shellcn_people_renamed") || tableExists(ctx, t, s, db, "shellcn_people") {
		t.Fatalf("expected table renamed to shellcn_people_renamed")
	}
	if _, err := renameTable(rowMutationRC(ctx, s, map[string]string{"database": db, "table": "shellcn_people_renamed"}, map[string]any{"name": "shellcn_people"})); err != nil {
		t.Fatalf("rename table back: %v", err)
	}

	// User create -> grant -> drop round-trip via the declarative handlers.
	itUser := "shellcn_it_user_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = s.db.ExecContext(cleanupCtx, "DROP USER IF EXISTS "+quoteIdent(itUser))
	})
	if _, err := createUser(rowMutationRC(ctx, s, nil, map[string]any{
		"name": itUser, "auth_type": "sha256_password", "password": "S3cr3t!Pass", "if_not_exists": true,
	})); err != nil {
		t.Fatalf("create user: %v", err)
	}
	var userCount int
	if err := s.db.QueryRowContext(ctx, "SELECT count() FROM system.users WHERE name = ?", itUser).Scan(&userCount); err != nil || userCount != 1 {
		t.Fatalf("expected user created, got %d err=%v", userCount, err)
	}
	if _, err := grantUser(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"user": itUser}, nil, mustJSON(map[string]any{
		"privilege": "SELECT", "on": db + ".*",
	}))); err != nil {
		t.Fatalf("grant user: %v", err)
	}
	var grantCount int
	if err := s.db.QueryRowContext(ctx, "SELECT count() FROM system.grants WHERE user_name = ? AND access_type = 'SELECT'", itUser).Scan(&grantCount); err != nil || grantCount == 0 {
		t.Fatalf("expected SELECT grant, got %d err=%v", grantCount, err)
	}
	if _, err := dropUser(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"user": itUser}, nil, nil)); err != nil {
		t.Fatalf("drop user: %v", err)
	}
	if err := s.db.QueryRowContext(ctx, "SELECT count() FROM system.users WHERE name = ?", itUser).Scan(&userCount); err != nil || userCount != 0 {
		t.Fatalf("expected user dropped, got %d err=%v", userCount, err)
	}

	// Merge STOP/START round-trip on a real table. SYSTEM STOP/START MERGES return no
	// rows; assert the handlers run cleanly and the table still accepts a merge cycle.
	if _, err := stopMerges(plugin.NewRequestContext(ctx, plugin.User{}, s, ddlParams(db), nil, nil)); err != nil {
		t.Fatalf("stop merges: %v", err)
	}
	if _, err := startMerges(plugin.NewRequestContext(ctx, plugin.User{}, s, ddlParams(db), nil, nil)); err != nil {
		t.Fatalf("start merges: %v", err)
	}

	// Kill-query handler path. Killing a real in-flight query is racy, so target a
	// query_id that is not running: KILL QUERY succeeds with zero matched queries,
	// exercising the handler, statement generation, and audit path end to end.
	if _, err := killProcess(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"id": "shellcn-it-not-running"}, nil, nil)); err != nil {
		t.Fatalf("kill process (no match): %v", err)
	}

	// Kill-mutation handler path. Likewise drive it against a non-existent mutation so
	// the handler/SQL/validate path runs without depending on a live mutation.
	if _, err := killMutation(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{
		"database": db, "table": "shellcn_people", "id": "0000000000",
	}, nil, nil)); err != nil {
		t.Fatalf("kill mutation (no match): %v", err)
	}

	// Database create + drop round-trip via the declarative handlers.
	dropDB := "shellcn_it_drop_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if _, err := createDatabase(rowMutationRC(ctx, s, nil, map[string]any{"name": dropDB, "if_not_exists": true})); err != nil {
		t.Fatalf("create database (drop round-trip): %v", err)
	}
	if _, err := dropDatabase(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"database": dropDB}, nil, nil)); err != nil {
		t.Fatalf("drop database: %v", err)
	}
	var dbCount int
	if err := s.db.QueryRowContext(ctx, "SELECT count() FROM system.databases WHERE name = ?", dropDB).Scan(&dbCount); err != nil || dbCount != 0 {
		t.Fatalf("expected database dropped, got %d err=%v", dbCount, err)
	}
}

func ddlParams(database string) map[string]string {
	return map[string]string{"database": database, "table": "shellcn_people"}
}

func tableExists(ctx context.Context, t *testing.T, s *Session, database, name string) bool {
	t.Helper()
	var count int
	if err := s.db.QueryRowContext(ctx, "SELECT count() FROM system.tables WHERE database = ? AND name = ?", database, name).Scan(&count); err != nil {
		t.Fatalf("table exists check: %v", err)
	}
	return count > 0
}

func integrationConfig(ctx context.Context, t *testing.T) map[string]any {
	t.Helper()
	if raw := os.Getenv("SHELLCN_CLICKHOUSE_DSN"); raw != "" {
		return configFromDSN(t, raw)
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_CLICKHOUSE_DSN is not set")
	}
	name := "shellcn-clickhouse-it-" + time.Now().UTC().Format("20060102150405")
	run(ctx, t, "docker", "run", "-d", "--rm", "--name", name,
		"--ulimit", "nofile=262144:262144",
		"-e", "CLICKHOUSE_DB=shellcn",
		"-e", "CLICKHOUSE_USER=shellcn",
		"-e", "CLICKHOUSE_PASSWORD=shellcn",
		"-e", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
		"-p", "127.0.0.1::9000",
		"clickhouse/clickhouse-server:latest")
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})
	out := run(ctx, t, "docker", "port", name, "9000/tcp")
	host, portText, err := net.SplitHostPort(strings.TrimSpace(out))
	if err != nil {
		t.Fatalf("unexpected docker port output: %q", out)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse docker port %q: %v", portText, err)
	}
	cfg := map[string]any{
		"host":      host,
		"port":      port,
		"database":  "shellcn",
		"username":  "shellcn",
		"password":  "shellcn",
		"tls_mode":  "disable",
		"read_only": false,
	}
	deadline := time.Now().Add(60 * time.Second)
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
			logs := exec.CommandContext(ctx, "docker", "logs", "--tail", "120", name)
			out, _ := logs.CombinedOutput()
			t.Fatalf("clickhouse container did not become ready: %v\n%s", lastErr, out)
		}
		time.Sleep(750 * time.Millisecond)
	}
}

func configFromDSN(t *testing.T, raw string) map[string]any {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse SHELLCN_CLICKHOUSE_DSN: %v", err)
	}
	port := defaultPort
	if u.Port() != "" {
		if port, err = strconv.Atoi(u.Port()); err != nil {
			t.Fatalf("parse DSN port: %v", err)
		}
	}
	password, _ := u.User.Password()
	return map[string]any{
		"host":      u.Hostname(),
		"port":      port,
		"database":  stringDefault(strings.TrimPrefix(u.Path, "/"), "default"),
		"username":  stringDefault(u.User.Username(), "default"),
		"password":  password,
		"tls_mode":  stringDefault(u.Query().Get("tls"), "disable"),
		"read_only": false,
	}
}

func rowMutationRC(ctx context.Context, s *Session, params map[string]string, body map[string]any) *plugin.RequestContext {
	return plugin.NewRequestContext(ctx, plugin.User{ID: "u1"}, s, params, nil, mustJSON(body))
}

func mustJSON(body map[string]any) []byte {
	raw, _ := json.Marshal(body)
	return raw
}

// findRow scans the table via the data-grid handler (which attaches _key) and
// returns the first row whose column equals value, or nil when absent.
func findRow(ctx context.Context, t *testing.T, s *Session, params map[string]string, column, value string) row {
	t.Helper()
	res, err := tableRows(plugin.NewRequestContext(ctx, plugin.User{}, s, params, nil, nil))
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
