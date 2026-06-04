package oracle

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

func TestOraclePluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_ORACLE_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_ORACLE_INTEGRATION=1 to run against Oracle Database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cfg := integrationConfig(ctx, t)
	cfg["read_only"] = false
	cfg["require_destructive_confirmation"] = true
	cfg["row_limit"] = 50
	cfg["query_timeout"] = "15s"

	sess, err := connect(ctx, plugin.ConnectConfig{
		Config: cfg,
		Net:    plugintest.DirectTransport(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	s := sess.(*Session)

	seed(ctx, t, s)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		_, _ = s.db.ExecContext(cleanupCtx, `DROP USER SHELLCN_TEST CASCADE`)
	})

	list, err := listTables(plugin.NewRequestContext(ctx, plugin.User{ID: "u1", Username: "admin"}, s, nil, url.Values{"p.schema": {"SHELLCN_TEST"}}, nil))
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	if !pageHasName(list.(plugin.Page[row]), "PEOPLE") {
		t.Fatalf("created table was not listed: %#v", list)
	}

	rows, err := tableRows(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"id": objectID("SHELLCN_TEST", "PEOPLE")}, nil, nil))
	if err != nil {
		t.Fatalf("table rows: %v", err)
	}
	page := rows.(plugin.Page[row])
	// The editable Data grid keeps Oracle's real (uppercase) column names so its
	// quoted UPDATE/DELETE identifiers match.
	if len(page.Items) != 1 || page.Items[0]["ACCESS_TOKEN"] != sqldb.RedactedValue {
		t.Fatalf("expected redacted table data, got %#v", page.Items)
	}

	// Free-text search filters the data grid server-side (per-column).
	orPeople := map[string]string{"id": objectID("SHELLCN_TEST", "PEOPLE")}
	orMatch, err := tableRows(plugin.NewRequestContext(ctx, plugin.User{}, s, orPeople, url.Values{"filter": {"alice"}}, nil))
	if err != nil {
		t.Fatalf("filtered rows: %v", err)
	}
	if len(orMatch.(plugin.Page[row]).Items) != 1 {
		t.Fatalf("filter 'alice' should match 1 row, got %#v", orMatch.(plugin.Page[row]).Items)
	}
	orMiss, err := tableRows(plugin.NewRequestContext(ctx, plugin.User{}, s, orPeople, url.Values{"filter": {"zzz-nomatch"}}, nil))
	if err != nil {
		t.Fatalf("filtered rows (miss): %v", err)
	}
	if len(orMiss.(plugin.Page[row]).Items) != 0 {
		t.Fatalf("filter 'zzz-nomatch' should match 0 rows, got %#v", orMiss.(plugin.Page[row]).Items)
	}
	if key, ok := page.Items[0]["_key"].(map[string]any); !ok || key["ID"] == nil {
		t.Fatalf("table rows must carry a _key from the primary key: %#v", page.Items[0])
	}

	result, err := executeQueryRequest(ctx, s, "SHELLCN_TEST", sqldb.QueryRequest{Query: `SELECT NAME, ACCESS_TOKEN FROM PEOPLE`})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0][1] != sqldb.RedactedValue {
		t.Fatalf("expected redacted query result, got %#v", result.Rows)
	}

	params := map[string]string{"id": objectID("SHELLCN_TEST", "PEOPLE")}
	if _, err := insertRow(rowMutationRC(ctx, s, params, map[string]any{"values": map[string]any{"NAME": "bob", "ACCESS_TOKEN": "tok"}})); err != nil {
		t.Fatalf("insert row: %v", err)
	}
	var bobID int64
	if err := s.db.QueryRowContext(ctx, `SELECT ID FROM SHELLCN_TEST.PEOPLE WHERE NAME = 'bob'`).Scan(&bobID); err != nil {
		t.Fatalf("read inserted row: %v", err)
	}
	if _, err := updateRow(rowMutationRC(ctx, s, params, map[string]any{"key": map[string]any{"ID": bobID}, "values": map[string]any{"NAME": "bob2"}})); err != nil {
		t.Fatalf("update row: %v", err)
	}
	var name string
	if err := s.db.QueryRowContext(ctx, `SELECT NAME FROM SHELLCN_TEST.PEOPLE WHERE ID = :1`, bobID).Scan(&name); err != nil || name != "bob2" {
		t.Fatalf("expected updated name bob2, got %q err=%v", name, err)
	}
	if _, err := deleteRow(rowMutationRC(ctx, s, params, map[string]any{"key": map[string]any{"ID": bobID}})); err != nil {
		t.Fatalf("delete row: %v", err)
	}
	var remaining int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM SHELLCN_TEST.PEOPLE`).Scan(&remaining); err != nil || remaining != 1 {
		t.Fatalf("expected 1 row after delete, got %d err=%v", remaining, err)
	}
	if _, err := updateRow(rowMutationRC(ctx, s, params, map[string]any{"key": map[string]any{"NAME": "alice"}, "values": map[string]any{"ACCESS_TOKEN": "x"}})); err == nil {
		t.Fatal("update with a non-primary-key key must be rejected")
	}

	// Column/index management via declarative DDL actions.
	if _, err := createIndex(rowMutationRC(ctx, s, params, map[string]any{"name": "IX_PEOPLE_NAME", "columns": "NAME", "unique": false})); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if _, err := dropIndex(rowMutationRC(ctx, s, map[string]string{"id": objectID("SHELLCN_TEST", "PEOPLE"), "name": "IX_PEOPLE_NAME"}, nil)); err != nil {
		t.Fatalf("drop index: %v", err)
	}
	if _, err := dropColumn(rowMutationRC(ctx, s, map[string]string{"id": objectID("SHELLCN_TEST", "PEOPLE"), "name": "ACCESS_TOKEN"}, nil)); err != nil {
		t.Fatalf("drop column: %v", err)
	}
	var cols int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM all_tab_columns WHERE owner = 'SHELLCN_TEST' AND table_name = 'PEOPLE' AND column_name = 'ACCESS_TOKEN'`).Scan(&cols); err != nil || cols != 0 {
		t.Fatalf("expected ACCESS_TOKEN column dropped, got %d err=%v", cols, err)
	}

	// Column alter (MODIFY) widens a column's type and toggles nullability.
	if _, err := alterColumn(rowMutationRC(ctx, s, map[string]string{"id": objectID("SHELLCN_TEST", "PEOPLE"), "name": "NAME"}, map[string]any{"type": "VARCHAR2(500)", "nullable": true})); err != nil {
		t.Fatalf("alter column: %v", err)
	}
	var charLen int
	var oraNullable string
	if err := s.db.QueryRowContext(ctx, `SELECT char_length, nullable FROM all_tab_columns WHERE owner = 'SHELLCN_TEST' AND table_name = 'PEOPLE' AND column_name = 'NAME'`).Scan(&charLen, &oraNullable); err != nil {
		t.Fatalf("read altered column: %v", err)
	}
	if charLen != 500 || oraNullable != "Y" {
		t.Fatalf("expected NAME widened to 500 and nullable, got len=%d nullable=%q", charLen, oraNullable)
	}

	// Column rename round-trips through RENAME COLUMN (and back).
	renameColParams := map[string]string{"id": objectID("SHELLCN_TEST", "PEOPLE"), "name": "NAME"}
	if _, err := renameColumn(rowMutationRC(ctx, s, renameColParams, map[string]any{"to": "FULL_NAME"})); err != nil {
		t.Fatalf("rename column: %v", err)
	}
	var renamed int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM all_tab_columns WHERE owner = 'SHELLCN_TEST' AND table_name = 'PEOPLE' AND column_name = 'FULL_NAME'`).Scan(&renamed); err != nil || renamed != 1 {
		t.Fatalf("expected column renamed to FULL_NAME, got %d err=%v", renamed, err)
	}
	if _, err := renameColumn(rowMutationRC(ctx, s, map[string]string{"id": objectID("SHELLCN_TEST", "PEOPLE"), "name": "FULL_NAME"}, map[string]any{"to": "NAME"})); err != nil {
		t.Fatalf("rename column back: %v", err)
	}

	// Constraint add (UNIQUE) then drop.
	constraintParams := map[string]string{"id": objectID("SHELLCN_TEST", "PEOPLE")}
	if _, err := addConstraint(rowMutationRC(ctx, s, constraintParams, map[string]any{"name": "UQ_PEOPLE_NAME", "type": "UNIQUE", "columns": "NAME"})); err != nil {
		t.Fatalf("add constraint: %v", err)
	}
	var constraints int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM all_constraints WHERE owner = 'SHELLCN_TEST' AND table_name = 'PEOPLE' AND constraint_name = 'UQ_PEOPLE_NAME'`).Scan(&constraints); err != nil || constraints != 1 {
		t.Fatalf("expected UQ_PEOPLE_NAME constraint, got %d err=%v", constraints, err)
	}
	if _, err := dropConstraint(rowMutationRC(ctx, s, map[string]string{"id": objectID("SHELLCN_TEST", "PEOPLE"), "name": "UQ_PEOPLE_NAME"}, nil)); err != nil {
		t.Fatalf("drop constraint: %v", err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM all_constraints WHERE owner = 'SHELLCN_TEST' AND table_name = 'PEOPLE' AND constraint_name = 'UQ_PEOPLE_NAME'`).Scan(&constraints); err != nil || constraints != 0 {
		t.Fatalf("expected UQ_PEOPLE_NAME dropped, got %d err=%v", constraints, err)
	}

	// Schema (user) create + drop: CREATE USER ... then DROP USER ... CASCADE.
	if _, err := createSchema(rowMutationRC(ctx, s, nil, map[string]any{"name": "SHELLCN_TEST2", "password": "ShellCN123"})); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	var users int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM all_users WHERE username = 'SHELLCN_TEST2'`).Scan(&users); err != nil || users != 1 {
		t.Fatalf("expected SHELLCN_TEST2 user created, got %d err=%v", users, err)
	}
	if _, err := dropSchema(plugin.NewRequestContext(ctx, plugin.User{ID: "u1"}, s, map[string]string{"schema": "SHELLCN_TEST2"}, nil, nil)); err != nil {
		t.Fatalf("drop schema: %v", err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM all_users WHERE username = 'SHELLCN_TEST2'`).Scan(&users); err != nil || users != 0 {
		t.Fatalf("expected SHELLCN_TEST2 user dropped, got %d err=%v", users, err)
	}

	// Table rename round-trips (rename a throwaway table, then back).
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE SHELLCN_TEST.TEMP_RENAME (ID NUMBER)`); err != nil {
		t.Fatalf("create rename table: %v", err)
	}
	if _, err := renameTable(rowMutationRC(ctx, s, map[string]string{"id": objectID("SHELLCN_TEST", "TEMP_RENAME")}, map[string]any{"name": "TEMP_RENAMED"})); err != nil {
		t.Fatalf("rename table: %v", err)
	}
	var renamedTables int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM all_tables WHERE owner = 'SHELLCN_TEST' AND table_name = 'TEMP_RENAMED'`).Scan(&renamedTables); err != nil || renamedTables != 1 {
		t.Fatalf("expected table renamed to TEMP_RENAMED, got %d err=%v", renamedTables, err)
	}

	// Foreign-key cells carry generic _links to the referenced table.
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE SHELLCN_TEST.ORDERS (ID NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, PERSON_ID NUMBER REFERENCES SHELLCN_TEST.PEOPLE(ID))`); err != nil {
		t.Fatalf("create child table: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO SHELLCN_TEST.ORDERS (PERSON_ID) VALUES (1)`); err != nil {
		t.Fatalf("seed child row: %v", err)
	}
	orderRows, err := tableRows(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"id": objectID("SHELLCN_TEST", "ORDERS")}, nil, nil))
	if err != nil {
		t.Fatalf("child table rows: %v", err)
	}
	if links, ok := orderRows.(plugin.Page[row]).Items[0]["_links"].(map[string]plugin.ResourceRef); !ok || links["PERSON_ID"].UID != objectID("SHELLCN_TEST", "PEOPLE") {
		t.Fatalf("expected _links[PERSON_ID] -> PEOPLE, got %#v", orderRows.(plugin.Page[row]).Items[0]["_links"])
	}

	// Foreign-key relationship graph (ERD), owner-scoped on Oracle.
	graph, err := relationGraph(plugin.NewRequestContext(ctx, plugin.User{}, s, nil, url.Values{"p.schema": {"SHELLCN_TEST"}}, nil))
	if err != nil {
		t.Fatalf("relation graph: %v", err)
	}
	if !hasEdge(graph.(sqldb.GraphPayload), "SHELLCN_TEST.ORDERS", "SHELLCN_TEST.PEOPLE") {
		t.Fatalf("expected FK edge orders -> people, got %#v", graph)
	}

	// User management: create (via schema.create) -> lock -> unlock -> grant ->
	// drop, asserting account_status and granted role at each step.
	if _, err := createSchema(rowMutationRC(ctx, s, nil, map[string]any{"name": "SHELLCN_USER", "password": "ShellCN123"})); err != nil {
		t.Fatalf("create user: %v", err)
	}
	userParams := map[string]string{"user": "SHELLCN_USER"}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		_, _ = s.db.ExecContext(cleanupCtx, `DROP USER SHELLCN_USER CASCADE`)
	})

	if _, err := lockUser(plugin.NewRequestContext(ctx, plugin.User{ID: "u1"}, s, userParams, nil, nil)); err != nil {
		t.Fatalf("lock user: %v", err)
	}
	if got := userAccountStatus(ctx, t, s, "SHELLCN_USER"); !strings.Contains(got, "LOCKED") {
		t.Fatalf("expected locked account, got %q", got)
	}
	if _, err := unlockUser(plugin.NewRequestContext(ctx, plugin.User{ID: "u1"}, s, userParams, nil, nil)); err != nil {
		t.Fatalf("unlock user: %v", err)
	}
	if got := userAccountStatus(ctx, t, s, "SHELLCN_USER"); strings.Contains(got, "LOCKED") {
		t.Fatalf("expected unlocked account, got %q", got)
	}

	if _, err := grantUser(rowMutationRC(ctx, s, userParams, map[string]any{"privileges": []any{"CREATE SESSION", "CREATE TABLE"}})); err != nil {
		t.Fatalf("grant user: %v", err)
	}
	var grants int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dba_sys_privs WHERE grantee = 'SHELLCN_USER' AND privilege IN ('CREATE SESSION','CREATE TABLE')`).Scan(&grants); err != nil || grants != 2 {
		t.Fatalf("expected 2 system privileges granted, got %d err=%v", grants, err)
	}

	if _, err := dropUser(plugin.NewRequestContext(ctx, plugin.User{ID: "u1"}, s, userParams, nil, nil)); err != nil {
		t.Fatalf("drop user: %v", err)
	}
	var present int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM all_users WHERE username = 'SHELLCN_USER'`).Scan(&present); err != nil || present != 0 {
		t.Fatalf("expected SHELLCN_USER dropped, got %d err=%v", present, err)
	}

	// Session kill: reject malformed ids, then best-effort kill the current
	// session's own SID/SERIAL# (KILL marks it; the running connection may report
	// ORA-00027 when killing itself, which is acceptable here).
	if _, err := killSession(plugin.NewRequestContext(ctx, plugin.User{ID: "u1"}, s, map[string]string{"id": "bad:id"}, nil, nil)); err == nil {
		t.Fatal("kill session accepted a non-numeric id")
	}
	var sid, serial int64
	if err := s.db.QueryRowContext(ctx, `SELECT sid, serial# FROM v$session WHERE sid = SYS_CONTEXT('USERENV','SID')`).Scan(&sid, &serial); err != nil {
		t.Skipf("v$session unavailable for session.kill exercise: %v", err)
	}
	killID := strconv.FormatInt(sid, 10) + ":" + strconv.FormatInt(serial, 10)
	_, killErr := killSession(plugin.NewRequestContext(ctx, plugin.User{ID: "u1"}, s, map[string]string{"id": killID}, nil, nil))
	if killErr != nil && !strings.Contains(killErr.Error(), "ORA-00027") {
		t.Fatalf("kill session (self) unexpected error: %v", killErr)
	}
}

func userAccountStatus(ctx context.Context, t *testing.T, s *Session, user string) string {
	t.Helper()
	var status string
	if err := s.db.QueryRowContext(ctx, `SELECT account_status FROM dba_users WHERE username = :1`, user).Scan(&status); err != nil {
		t.Fatalf("read account status for %s: %v", user, err)
	}
	return status
}

func rowMutationRC(ctx context.Context, s *Session, params map[string]string, body map[string]any) *plugin.RequestContext {
	raw, _ := json.Marshal(body)
	return plugin.NewRequestContext(ctx, plugin.User{ID: "u1"}, s, params, nil, raw)
}

func integrationConfig(ctx context.Context, t *testing.T) map[string]any {
	t.Helper()
	if raw := os.Getenv("SHELLCN_ORACLE_DSN"); raw != "" {
		return configFromDSN(t, raw)
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_ORACLE_DSN is not set")
	}
	name := "shellcn-oracle-it-" + time.Now().UTC().Format("20060102150405")
	password := "ShellCN123"
	run(ctx, t, "docker", "run", "-d", "--rm", "--name", name,
		"-e", "ORACLE_PASSWORD="+password,
		"-p", "127.0.0.1::1521",
		"gvenzl/oracle-free:slim-faststart")
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})
	out := run(ctx, t, "docker", "port", name, "1521/tcp")
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
		"service":   "FREEPDB1",
		"username":  "SYSTEM",
		"password":  password,
		"tls_mode":  "disable",
		"read_only": false,
	}
	deadline := time.Now().Add(4 * time.Minute)
	for {
		sess, err := connect(ctx, plugin.ConnectConfig{
			Config: cfg,
			Net:    plugintest.DirectTransport(),
		})
		if err == nil {
			_ = sess.Close()
			return cfg
		}
		if time.Now().After(deadline) {
			t.Fatalf("oracle container did not become ready: %v", err)
		}
		time.Sleep(2 * time.Second)
	}
}

func configFromDSN(t *testing.T, raw string) map[string]any {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse SHELLCN_ORACLE_DSN: %v", err)
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
		"service":   stringDefault(strings.TrimPrefix(u.Path, "/"), "FREEPDB1"),
		"username":  u.User.Username(),
		"password":  password,
		"tls_mode":  stringDefault(u.Query().Get("tls_mode"), "disable"),
		"read_only": false,
	}
}

func seed(ctx context.Context, t *testing.T, s *Session) {
	t.Helper()
	statements := []string{
		`BEGIN
  EXECUTE IMMEDIATE 'DROP USER SHELLCN_TEST CASCADE';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -1918 THEN RAISE; END IF;
END;`,
		`CREATE USER SHELLCN_TEST IDENTIFIED BY "ShellCN123"`,
		`GRANT CONNECT, RESOURCE TO SHELLCN_TEST`,
		`ALTER USER SHELLCN_TEST QUOTA UNLIMITED ON USERS`,
		`CREATE TABLE SHELLCN_TEST.PEOPLE (
  ID NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  NAME VARCHAR2(255) NOT NULL,
  ACCESS_TOKEN VARCHAR2(255) NOT NULL
)`,
		`INSERT INTO SHELLCN_TEST.PEOPLE (NAME, ACCESS_TOKEN) VALUES ('alice', 'secret-token')`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			t.Fatalf("seed database: %v", err)
		}
	}
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
