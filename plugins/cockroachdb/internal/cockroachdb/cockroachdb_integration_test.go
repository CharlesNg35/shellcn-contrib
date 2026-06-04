package cockroachdb

import (
	"context"
	"encoding/json"
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

func TestCockroachDBPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_COCKROACHDB_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_COCKROACHDB_INTEGRATION=1 to run against CockroachDB")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
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
		_, _ = s.pool.Exec(cleanupCtx, "DROP DATABASE IF EXISTS "+sqldb.QuoteIdent(createdDatabase))
	})
	databases, err := listDatabases(plugin.NewRequestContext(ctx, plugin.User{}, s, nil, nil, nil))
	if err != nil {
		t.Fatalf("list databases after create: %v", err)
	}
	if !pageHasName(databases.(plugin.Page[row]), createdDatabase) {
		t.Fatalf("created database was not listed: %#v", databases)
	}

	// View drop round-trip: create a view, drop it through the handler, verify gone.
	if _, err := s.pool.Exec(ctx, `CREATE VIEW public.shellcn_v AS SELECT 1 AS x`); err != nil {
		t.Fatalf("seed view: %v", err)
	}
	if _, err := dropView(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"schema": "public", "view": "shellcn_v"}, nil, nil)); err != nil {
		t.Fatalf("drop view: %v", err)
	}
	var vcount int
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM pg_views WHERE schemaname='public' AND viewname='shellcn_v'`).Scan(&vcount); err != nil || vcount != 0 {
		t.Fatalf("expected view dropped, got %d err=%v", vcount, err)
	}

	// Schema create round-trip.
	defer func() { _, _ = s.pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS shellcn_sc CASCADE`) }()
	if _, err := createSchema(rowMutationRC(ctx, s, nil, map[string]any{"name": "shellcn_sc"})); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	schemas, err := listSchemas(plugin.NewRequestContext(ctx, plugin.User{}, s, nil, nil, nil))
	if err != nil {
		t.Fatalf("list schemas: %v", err)
	}
	if !pageHasName(schemas.(plugin.Page[row]), "shellcn_sc") {
		t.Fatalf("created schema was not listed: %#v", schemas)
	}

	if _, err := s.pool.Exec(ctx, `
CREATE TABLE IF NOT EXISTS public.shellcn_people (
  id INT8 PRIMARY KEY,
  name STRING NOT NULL,
  access_token STRING NOT NULL
);
TRUNCATE public.shellcn_people;
INSERT INTO public.shellcn_people (id, name, access_token) VALUES (1, 'alice', 'secret-token')`); err != nil {
		t.Fatalf("seed database: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = s.pool.Exec(cleanupCtx, `DROP TABLE IF EXISTS public.shellcn_people`)
	})

	// Foreign-key relationship graph (ERD) round-trip.
	if _, err := s.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS public.shellcn_orders (id INT8 PRIMARY KEY, person_id INT8 REFERENCES public.shellcn_people(id))`); err != nil {
		t.Fatalf("create child table: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = s.pool.Exec(cleanupCtx, `DROP TABLE IF EXISTS public.shellcn_orders`)
	})
	graph, err := relationGraph(plugin.NewRequestContext(ctx, plugin.User{}, s, nil, nil, nil))
	if err != nil {
		t.Fatalf("relation graph: %v", err)
	}
	if !hasEdge(graph.(sqldb.GraphPayload), "public.shellcn_orders", "public.shellcn_people") {
		t.Fatalf("expected FK edge orders -> people, got %#v", graph)
	}

	rc := plugin.NewRequestContext(ctx, plugin.User{ID: "u1", Username: "admin"}, s, nil, nil, nil)
	list, err := listTables(rc)
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	if !pageHasName(list.(plugin.Page[row]), "shellcn_people") {
		t.Fatalf("created table was not listed: %#v", list)
	}

	// A schema tree node expands to its real tables (not detail-tab categories).
	treeRC := plugin.NewRequestContext(ctx, plugin.User{}, s, nil, url.Values{"p.schema": {"public"}}, nil)
	treeNodes, err := treeTables(treeRC)
	if err != nil {
		t.Fatalf("schema tree children: %v", err)
	}
	foundTable := false
	for _, n := range treeNodes.(plugin.Page[plugin.TreeNode]).Items {
		if n.Ref != nil && n.Ref.Kind == "table" && strings.Contains(n.Label, "shellcn_people") {
			foundTable = true
		}
	}
	if !foundTable {
		t.Fatalf("schema tree should list real tables, got %#v", treeNodes)
	}

	rows, err := tableRows(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"schema": "public", "table": "shellcn_people"}, nil, nil))
	if err != nil {
		t.Fatalf("table rows: %v", err)
	}
	page := rows.(plugin.Page[row])
	if len(page.Items) != 1 || page.Items[0]["access_token"] != sqldb.RedactedValue {
		t.Fatalf("expected redacted table data, got %#v", page.Items)
	}

	// Free-text search filters the data grid server-side.
	matched, err := tableRows(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"schema": "public", "table": "shellcn_people"}, url.Values{"filter": {"alice"}}, nil))
	if err != nil {
		t.Fatalf("filtered rows: %v", err)
	}
	if len(matched.(plugin.Page[row]).Items) != 1 {
		t.Fatalf("filter 'alice' should match 1 row, got %#v", matched.(plugin.Page[row]).Items)
	}
	missed, err := tableRows(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"schema": "public", "table": "shellcn_people"}, url.Values{"filter": {"zzz-nomatch"}}, nil))
	if err != nil {
		t.Fatalf("filtered rows (miss): %v", err)
	}
	if len(missed.(plugin.Page[row]).Items) != 0 {
		t.Fatalf("filter 'zzz-nomatch' should match 0 rows, got %#v", missed.(plugin.Page[row]).Items)
	}

	result, err := executeQueryRequest(ctx, s, sqldb.QueryRequest{Query: `SELECT name, access_token FROM public.shellcn_people`})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0][1] != sqldb.RedactedValue {
		t.Fatalf("expected redacted query result, got %#v", result.Rows)
	}

	// Editable data grid: rows carry _key from the primary key.
	if key, ok := page.Items[0]["_key"].(map[string]any); !ok || key["id"] == nil {
		t.Fatalf("table rows must carry a _key from the primary key: %#v", page.Items[0])
	}
	params := map[string]string{"schema": "public", "table": "shellcn_people"}
	if _, err := insertRow(rowMutationRC(ctx, s, params, map[string]any{"values": map[string]any{"id": 2, "name": "bob", "access_token": "tok"}})); err != nil {
		t.Fatalf("insert row: %v", err)
	}
	var bobID int64
	if err := s.pool.QueryRow(ctx, `SELECT id FROM public.shellcn_people WHERE name = 'bob'`).Scan(&bobID); err != nil {
		t.Fatalf("read inserted row: %v", err)
	}
	if _, err := updateRow(rowMutationRC(ctx, s, params, map[string]any{"key": map[string]any{"id": bobID}, "values": map[string]any{"name": "bob2"}})); err != nil {
		t.Fatalf("update row: %v", err)
	}
	var name string
	if err := s.pool.QueryRow(ctx, `SELECT name FROM public.shellcn_people WHERE id = $1`, bobID).Scan(&name); err != nil || name != "bob2" {
		t.Fatalf("expected updated name bob2, got %q err=%v", name, err)
	}
	if _, err := deleteRow(rowMutationRC(ctx, s, params, map[string]any{"key": map[string]any{"id": bobID}})); err != nil {
		t.Fatalf("delete row: %v", err)
	}
	var remaining int
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM public.shellcn_people`).Scan(&remaining); err != nil || remaining != 1 {
		t.Fatalf("expected 1 row after delete, got %d err=%v", remaining, err)
	}
	if _, err := updateRow(rowMutationRC(ctx, s, params, map[string]any{"key": map[string]any{"name": "alice"}, "values": map[string]any{"access_token": "x"}})); err == nil {
		t.Fatal("update with a non-primary-key key must be rejected")
	}

	// Column/index management via declarative DDL actions (create then drop the
	// index proves both work; CockroachDB drops indexes with table@index).
	if _, err := createIndex(rowMutationRC(ctx, s, params, map[string]any{"name": "ix_people_name", "columns": "name", "unique": false})); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if _, err := dropIndex(rowMutationRC(ctx, s, map[string]string{"schema": "public", "table": "shellcn_people", "name": "ix_people_name"}, nil)); err != nil {
		t.Fatalf("drop index: %v", err)
	}
	if _, err := dropColumn(rowMutationRC(ctx, s, map[string]string{"schema": "public", "table": "shellcn_people", "name": "access_token"}, nil)); err != nil {
		t.Fatalf("drop column: %v", err)
	}
	var cols int
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='shellcn_people' AND column_name='access_token'`).Scan(&cols); err != nil || cols != 0 {
		t.Fatalf("expected access_token column dropped, got %d err=%v", cols, err)
	}

	// Column rename + alter type round-trip.
	if _, err := renameColumn(rowMutationRC(ctx, s, map[string]string{"schema": "public", "table": "shellcn_people", "name": "name"}, map[string]any{"newName": "full_name"})); err != nil {
		t.Fatalf("rename column: %v", err)
	}
	if _, err := alterColumn(rowMutationRC(ctx, s, map[string]string{"schema": "public", "table": "shellcn_people", "name": "full_name"}, map[string]any{"type": "VARCHAR(64)"})); err != nil {
		t.Fatalf("alter column: %v", err)
	}
	var renamedCols int
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='shellcn_people' AND column_name='full_name'`).Scan(&renamedCols); err != nil || renamedCols != 1 {
		t.Fatalf("expected renamed column full_name, got %d err=%v", renamedCols, err)
	}

	// Constraint add + drop round-trip (CHECK is the simplest to add/drop in place).
	if _, err := addConstraint(rowMutationRC(ctx, s, map[string]string{"schema": "public", "table": "shellcn_people"}, map[string]any{"name": "people_id_positive", "type": constraintCheck, "check": "id > 0"})); err != nil {
		t.Fatalf("add constraint: %v", err)
	}
	var conCount int
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM information_schema.table_constraints WHERE table_schema='public' AND table_name='shellcn_people' AND constraint_name='people_id_positive'`).Scan(&conCount); err != nil || conCount != 1 {
		t.Fatalf("expected constraint added, got %d err=%v", conCount, err)
	}
	if _, err := dropConstraint(rowMutationRC(ctx, s, map[string]string{"schema": "public", "table": "shellcn_people", "name": "people_id_positive"}, nil)); err != nil {
		t.Fatalf("drop constraint: %v", err)
	}
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM information_schema.table_constraints WHERE table_schema='public' AND table_name='shellcn_people' AND constraint_name='people_id_positive'`).Scan(&conCount); err != nil || conCount != 0 {
		t.Fatalf("expected constraint dropped, got %d err=%v", conCount, err)
	}

	// Table rename round-trip (renamed back so later cleanup still finds it).
	if _, err := renameTable(rowMutationRC(ctx, s, map[string]string{"schema": "public", "table": "shellcn_people"}, map[string]any{"newName": "shellcn_humans"})); err != nil {
		t.Fatalf("rename table: %v", err)
	}
	var renamedTables int
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name='shellcn_humans'`).Scan(&renamedTables); err != nil || renamedTables != 1 {
		t.Fatalf("expected renamed table shellcn_humans, got %d err=%v", renamedTables, err)
	}
	if _, err := renameTable(rowMutationRC(ctx, s, map[string]string{"schema": "public", "table": "shellcn_humans"}, map[string]any{"newName": "shellcn_people"})); err != nil {
		t.Fatalf("rename table back: %v", err)
	}

	// Drop schema round-trip: the empty schema created earlier drops cleanly.
	if _, err := dropSchema(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"schema": "shellcn_sc"}, nil, nil)); err != nil {
		t.Fatalf("drop schema: %v", err)
	}
	var schemaCount int
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name='shellcn_sc'`).Scan(&schemaCount); err != nil || schemaCount != 0 {
		t.Fatalf("expected schema dropped, got %d err=%v", schemaCount, err)
	}

	// Drop database round-trip: the database created earlier drops through the
	// handler (it is not the connected database).
	if _, err := dropDatabase(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"database": createdDatabase}, nil, nil)); err != nil {
		t.Fatalf("drop database: %v", err)
	}
	databasesAfterDrop, err := listDatabases(plugin.NewRequestContext(ctx, plugin.User{}, s, nil, nil, nil))
	if err != nil {
		t.Fatalf("list databases after drop: %v", err)
	}
	if pageHasName(databasesAfterDrop.(plugin.Page[row]), createdDatabase) {
		t.Fatalf("dropped database is still listed: %#v", databasesAfterDrop)
	}
	// Dropping the connected database must be refused.
	if _, err := dropDatabase(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"database": s.opts.Database}, nil, nil)); err == nil {
		t.Fatal("dropping the connected database must be rejected")
	}

	// User management round-trip: create -> grant -> drop.
	createdUser := "shellcn_it_user"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = s.pool.Exec(cleanupCtx, "DROP USER IF EXISTS "+sqldb.QuoteIdent(createdUser))
	})
	// The self-provisioned cluster runs --insecure, which rejects WITH PASSWORD;
	// exercise the (optional) no-password path.
	if _, err := createUser(rowMutationRC(ctx, s, nil, map[string]any{"name": createdUser})); err != nil {
		t.Fatalf("create user: %v", err)
	}
	users, err := listUsers(plugin.NewRequestContext(ctx, plugin.User{}, s, nil, nil, nil))
	if err != nil {
		t.Fatalf("list users: %v", err)
	}
	if !pageHasName(users.(plugin.Page[row]), createdUser) {
		t.Fatalf("created user was not listed: %#v", users)
	}
	if _, err := grantUser(rowMutationRC(ctx, s, map[string]string{"user": createdUser}, map[string]any{"target": grantTargetDatabase, "privilege": "ALL", "object": "defaultdb"})); err != nil {
		t.Fatalf("grant user: %v", err)
	}
	// A user holding grants cannot be dropped; revoke first.
	if _, err := s.pool.Exec(ctx, "REVOKE ALL ON DATABASE defaultdb FROM "+sqldb.QuoteIdent(createdUser)); err != nil {
		t.Fatalf("revoke grant: %v", err)
	}
	if _, err := dropUser(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"user": createdUser}, nil, nil)); err != nil {
		t.Fatalf("drop user: %v", err)
	}
	usersAfterDrop, err := listUsers(plugin.NewRequestContext(ctx, plugin.User{}, s, nil, nil, nil))
	if err != nil {
		t.Fatalf("list users after drop: %v", err)
	}
	if pageHasName(usersAfterDrop.(plugin.Page[row]), createdUser) {
		t.Fatalf("dropped user is still listed: %#v", usersAfterDrop)
	}

	// Cancel handlers: reject malformed ids before touching the cluster, and
	// drive a real CANCEL QUERY against a live (own) session's query id. The id
	// is read from SHOW QUERIES; cancelling is racy — by the time CANCEL runs the
	// query (the SHOW QUERIES read itself) has usually finished, so a "not found"
	// from the cluster is an accepted outcome as long as the handler reaches it.
	if _, err := cancelSession(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"id": "not-a-hex-token"}, nil, nil)); err == nil {
		t.Fatal("cancelSession must reject a malformed session id")
	}
	if _, err := cancelQueryByID(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"id": "'; SELECT 1"}, nil, nil)); err == nil {
		t.Fatal("cancelQueryByID must reject a malformed query id")
	}
	var queryID string
	if err := s.pool.QueryRow(ctx, `SELECT query_id FROM [SHOW QUERIES] LIMIT 1`).Scan(&queryID); err == nil && queryID != "" {
		if _, err := cancelQueryByID(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"id": queryID}, nil, nil)); err != nil && !strings.Contains(err.Error(), "not found") {
			t.Fatalf("cancel query by id: %v", err)
		}
	}

	for name, fn := range map[string]func(*plugin.RequestContext) (any, error){
		"databases": listDatabases,
		"users":     listUsers,
		"schemas":   listSchemas,
		"nodes":     listNodes,
		"jobs":      listJobs,
		"sessions":  listSessions,
		"queries":   listQueries,
		"ranges":    listRanges,
	} {
		if _, err := fn(rc); err != nil {
			t.Fatalf("%s route: %v", name, err)
		}
	}
}

func integrationConfig(ctx context.Context, t *testing.T) map[string]any {
	t.Helper()
	if raw := os.Getenv("SHELLCN_COCKROACHDB_DSN"); raw != "" {
		return configFromDSN(t, raw)
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_COCKROACHDB_DSN is not set")
	}
	name := "shellcn-cockroach-it-" + time.Now().UTC().Format("20060102150405")
	run(ctx, t, "docker", "run", "-d", "--name", name,
		"-p", "127.0.0.1::26257",
		"cockroachdb/cockroach:latest",
		"start",
		"--insecure",
		"--store=type=mem,size=1GiB",
		"--listen-addr=0.0.0.0:26257",
		"--advertise-addr=localhost:26257",
		"--join=localhost:26257",
		"--http-addr=0.0.0.0:8080")
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})
	out := run(ctx, t, "docker", "port", name, "26257/tcp")
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
		"database":  "defaultdb",
		"username":  "root",
		"password":  "",
		"tls_mode":  "disable",
		"read_only": false,
	}
	deadline := time.Now().Add(90 * time.Second)
	initialized := false
	var lastErr error
	for {
		if !initialized {
			if err := exec.CommandContext(ctx, "docker", "exec", name, "./cockroach", "init", "--insecure", "--host=localhost:26257").Run(); err == nil {
				initialized = true
			}
		}
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
			t.Fatalf("cockroach container did not become ready: %v\n%s", lastErr, out)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func configFromDSN(t *testing.T, raw string) map[string]any {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse SHELLCN_COCKROACHDB_DSN: %v", err)
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
		"database":  strings.TrimPrefix(u.Path, "/"),
		"username":  u.User.Username(),
		"password":  password,
		"tls_mode":  stringDefault(u.Query().Get("sslmode"), "disable"),
		"read_only": false,
	}
}

func rowMutationRC(ctx context.Context, s *Session, params map[string]string, body map[string]any) *plugin.RequestContext {
	raw, _ := json.Marshal(body)
	return plugin.NewRequestContext(ctx, plugin.User{ID: "u1"}, s, params, nil, raw)
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

func hasEdge(g sqldb.GraphPayload, source, target string) bool {
	for _, e := range g.Edges {
		if e.Source == source && e.Target == target {
			return true
		}
	}
	return false
}
