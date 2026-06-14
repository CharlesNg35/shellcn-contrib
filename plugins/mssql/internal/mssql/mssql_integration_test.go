package mssql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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

func TestMSSQLPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_MSSQL_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_MSSQL_INTEGRATION=1 to run against Microsoft SQL Server")
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
	if _, err := createDatabase(rowMutationRC(ctx, s, nil, map[string]any{"name": createdDatabase})); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = s.db.ExecContext(cleanupCtx, "DROP DATABASE "+quoteIdent(createdDatabase))
	})
	databases, err := listDatabases(plugin.NewRequestContext(ctx, plugin.User{}, s, nil, nil, nil))
	if err != nil {
		t.Fatalf("list databases after create: %v", err)
	}
	if !pageHasName(databases.(plugin.Page[row]), createdDatabase) {
		t.Fatalf("created database was not listed: %#v", databases)
	}

	// Drop the database through the handler (forces SINGLE_USER first).
	if _, err := dropDatabase(plugin.NewRequestContext(ctx, plugin.User{ID: "u1"}, s, map[string]string{"database": createdDatabase}, nil, nil)); err != nil {
		t.Fatalf("drop database: %v", err)
	}
	var dbExists int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sys.databases WHERE name = @p1`, createdDatabase).Scan(&dbExists); err != nil || dbExists != 0 {
		t.Fatalf("expected database dropped, got %d err=%v", dbExists, err)
	}
	// Dropping the connected database must be refused.
	if _, err := dropDatabase(plugin.NewRequestContext(ctx, plugin.User{ID: "u1"}, s, map[string]string{"database": s.opts.Database}, nil, nil)); err == nil {
		t.Fatal("dropping the connected database must be rejected")
	}

	seed(ctx, t, s)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = s.db.ExecContext(cleanupCtx, `DROP TABLE IF EXISTS [shellcn].[dbo].[people]`)
		_, _ = s.db.ExecContext(cleanupCtx, `DROP DATABASE [shellcn]`)
	})

	list, err := listTables(plugin.NewRequestContext(ctx, plugin.User{ID: "u1", Username: "admin"}, s, nil, url.Values{"p.database": {"shellcn"}}, nil))
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	if !pageHasName(list.(plugin.Page[row]), "people") {
		t.Fatalf("created table was not listed: %#v", list)
	}

	rows, err := tableRows(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"id": objectID("shellcn", "dbo", "people")}, nil, nil))
	if err != nil {
		t.Fatalf("table rows: %v", err)
	}
	page := rows.(plugin.Page[row])
	if len(page.Items) != 1 || page.Items[0]["access_token"] != sqldb.RedactedValue {
		t.Fatalf("expected redacted table data, got %#v", page.Items)
	}

	// Free-text search filters the data grid server-side (per-column).
	msPeople := map[string]string{"id": objectID("shellcn", "dbo", "people")}
	msMatch, err := tableRows(plugin.NewRequestContext(ctx, plugin.User{}, s, msPeople, url.Values{"filter": {"alice"}}, nil))
	if err != nil {
		t.Fatalf("filtered rows: %v", err)
	}
	if len(msMatch.(plugin.Page[row]).Items) != 1 {
		t.Fatalf("filter 'alice' should match 1 row, got %#v", msMatch.(plugin.Page[row]).Items)
	}
	msMiss, err := tableRows(plugin.NewRequestContext(ctx, plugin.User{}, s, msPeople, url.Values{"filter": {"zzz-nomatch"}}, nil))
	if err != nil {
		t.Fatalf("filtered rows (miss): %v", err)
	}
	if len(msMiss.(plugin.Page[row]).Items) != 0 {
		t.Fatalf("filter 'zzz-nomatch' should match 0 rows, got %#v", msMiss.(plugin.Page[row]).Items)
	}

	result, err := executeQueryRequest(ctx, s, "shellcn", sqldb.QueryRequest{Query: `SELECT name, access_token FROM dbo.people`})
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
	params := map[string]string{"id": objectID("shellcn", "dbo", "people")}
	if _, err := insertRow(rowMutationRC(ctx, s, params, map[string]any{"values": map[string]any{"name": "bob", "access_token": "tok"}})); err != nil {
		t.Fatalf("insert row: %v", err)
	}
	var bobID int64
	if err := s.db.QueryRowContext(ctx, `SELECT id FROM [shellcn].[dbo].[people] WHERE name = N'bob'`).Scan(&bobID); err != nil {
		t.Fatalf("read inserted row: %v", err)
	}
	if _, err := updateRow(rowMutationRC(ctx, s, params, map[string]any{"key": map[string]any{"id": bobID}, "values": map[string]any{"name": "bob2"}})); err != nil {
		t.Fatalf("update row: %v", err)
	}
	var name string
	if err := s.db.QueryRowContext(ctx, `SELECT name FROM [shellcn].[dbo].[people] WHERE id = @p1`, bobID).Scan(&name); err != nil || name != "bob2" {
		t.Fatalf("expected updated name bob2, got %q err=%v", name, err)
	}
	if _, err := deleteRow(rowMutationRC(ctx, s, params, map[string]any{"key": map[string]any{"id": bobID}})); err != nil {
		t.Fatalf("delete row: %v", err)
	}
	var remaining int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM [shellcn].[dbo].[people]`).Scan(&remaining); err != nil || remaining != 1 {
		t.Fatalf("expected 1 row after delete, got %d err=%v", remaining, err)
	}
	if _, err := updateRow(rowMutationRC(ctx, s, params, map[string]any{"key": map[string]any{"name": "alice"}, "values": map[string]any{"access_token": "x"}})); err == nil {
		t.Fatal("update with a non-primary-key key must be rejected")
	}

	// Hierarchical tree: database -> schema -> table (3-level drill-down).
	dbTree, err := treeDatabases(plugin.NewRequestContext(ctx, plugin.User{}, s, nil, nil, nil))
	if err != nil {
		t.Fatalf("tree databases: %v", err)
	}
	if !hasBranch(dbTree, "shellcn") {
		t.Fatalf("database branch missing: %#v", dbTree)
	}
	schemaTree, err := treeSchemas(plugin.NewRequestContext(ctx, plugin.User{}, s, nil, url.Values{"p.database": {"shellcn"}}, nil))
	if err != nil {
		t.Fatalf("tree schemas: %v", err)
	}
	if !hasBranch(schemaTree, "dbo") {
		t.Fatalf("schema branch dbo missing: %#v", schemaTree)
	}
	relTree, err := treeRelations(plugin.NewRequestContext(ctx, plugin.User{}, s, nil, url.Values{"p.database": {"shellcn"}, "p.schema": {"dbo"}}, nil))
	if err != nil {
		t.Fatalf("tree relations: %v", err)
	}
	leaf := false
	for _, n := range relTree.(plugin.Page[plugin.TreeNode]).Items {
		if n.Label == "people" && n.Leaf {
			leaf = true
		}
	}
	if !leaf {
		t.Fatalf("table leaf 'people' missing under dbo: %#v", relTree)
	}

	// Column/index management via declarative DDL actions.
	if _, err := createIndex(rowMutationRC(ctx, s, params, map[string]any{"name": "ix_people_name", "columns": "name", "unique": false})); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if _, err := dropIndex(rowMutationRC(ctx, s, map[string]string{"id": objectID("shellcn", "dbo", "people"), "name": "ix_people_name"}, nil)); err != nil {
		t.Fatalf("drop index: %v", err)
	}
	if _, err := dropColumn(rowMutationRC(ctx, s, map[string]string{"id": objectID("shellcn", "dbo", "people"), "name": "access_token"}, nil)); err != nil {
		t.Fatalf("drop column: %v", err)
	}
	var cols int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM [shellcn].INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'people' AND COLUMN_NAME = 'access_token'`).Scan(&cols); err != nil || cols != 0 {
		t.Fatalf("expected access_token column dropped, got %d err=%v", cols, err)
	}

	// Schema create + drop within the database.
	if _, err := createSchema(rowMutationRC(ctx, s, map[string]string{"database": "shellcn"}, map[string]any{"name": "app"})); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	var schemaCount int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM [shellcn].sys.schemas WHERE name = 'app'`).Scan(&schemaCount); err != nil || schemaCount != 1 {
		t.Fatalf("expected schema app created, got %d err=%v", schemaCount, err)
	}
	if _, err := dropSchema(rowMutationRC(ctx, s, map[string]string{"database": "shellcn", "schema": "app"}, nil)); err != nil {
		t.Fatalf("drop schema: %v", err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM [shellcn].sys.schemas WHERE name = 'app'`).Scan(&schemaCount); err != nil || schemaCount != 0 {
		t.Fatalf("expected schema app dropped, got %d err=%v", schemaCount, err)
	}

	// Alter column: widen and toggle nullability of the people.name column.
	if _, err := alterColumn(rowMutationRC(ctx, s, params, map[string]any{"name": "name", "type": "nvarchar(512)", "nullable": true})); err != nil {
		t.Fatalf("alter column: %v", err)
	}
	var maxLen int
	var isNullable bool
	if err := s.db.QueryRowContext(ctx, `SELECT c.max_length, c.is_nullable FROM [shellcn].sys.columns c JOIN [shellcn].sys.objects o ON o.object_id = c.object_id JOIN [shellcn].sys.schemas sc ON sc.schema_id = o.schema_id WHERE sc.name = 'dbo' AND o.name = 'people' AND c.name = 'name'`).Scan(&maxLen, &isNullable); err != nil {
		t.Fatalf("read altered column: %v", err)
	}
	if maxLen != 1024 || !isNullable {
		t.Fatalf("expected nvarchar(512) NULL (max_length 1024, nullable), got max_length=%d nullable=%v", maxLen, isNullable)
	}

	// Constraint add (UNIQUE) + drop on the people table.
	if _, err := addConstraint(rowMutationRC(ctx, s, params, map[string]any{"name": "uq_people_name", "type": "UNIQUE", "columns": "name"})); err != nil {
		t.Fatalf("add constraint: %v", err)
	}
	var constraintCount int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM [shellcn].sys.key_constraints WHERE name = 'uq_people_name'`).Scan(&constraintCount); err != nil || constraintCount != 1 {
		t.Fatalf("expected constraint uq_people_name created, got %d err=%v", constraintCount, err)
	}
	if _, err := dropConstraint(rowMutationRC(ctx, s, map[string]string{"id": objectID("shellcn", "dbo", "people"), "name": "uq_people_name"}, nil)); err != nil {
		t.Fatalf("drop constraint: %v", err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM [shellcn].sys.key_constraints WHERE name = 'uq_people_name'`).Scan(&constraintCount); err != nil || constraintCount != 0 {
		t.Fatalf("expected constraint uq_people_name dropped, got %d err=%v", constraintCount, err)
	}

	// Rename table people -> humans, then back so later assertions still match.
	if _, err := renameTable(rowMutationRC(ctx, s, params, map[string]any{"name": "humans"})); err != nil {
		t.Fatalf("rename table: %v", err)
	}
	var renamed int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM [shellcn].sys.objects o JOIN [shellcn].sys.schemas sc ON sc.schema_id = o.schema_id WHERE sc.name = 'dbo' AND o.name = 'humans'`).Scan(&renamed); err != nil || renamed != 1 {
		t.Fatalf("expected table renamed to humans, got %d err=%v", renamed, err)
	}
	if _, err := renameTable(rowMutationRC(ctx, s, map[string]string{"id": objectID("shellcn", "dbo", "humans")}, map[string]any{"name": "people"})); err != nil {
		t.Fatalf("rename table back: %v", err)
	}

	// Foreign-key cells carry generic _links to the referenced table.
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE [shellcn].[dbo].[orders] (id bigint IDENTITY(1,1) PRIMARY KEY, person_id bigint REFERENCES [shellcn].[dbo].[people](id))`); err != nil {
		t.Fatalf("create child table: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO [shellcn].[dbo].[orders] (person_id) VALUES (1)`); err != nil {
		t.Fatalf("seed child row: %v", err)
	}
	orderRows, err := tableRows(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"id": objectID("shellcn", "dbo", "orders")}, nil, nil))
	if err != nil {
		t.Fatalf("child table rows: %v", err)
	}
	if links, ok := orderRows.(plugin.Page[row]).Items[0]["_links"].(map[string]plugin.ResourceIdentity); !ok || links["person_id"].UID != objectID("shellcn", "dbo", "people") {
		t.Fatalf("expected _links[person_id] -> people, got %#v", orderRows.(plugin.Page[row]).Items[0]["_links"])
	}

	// Foreign-key relationship graph (ERD) over the FK created above.
	graph, err := relationGraph(plugin.NewRequestContext(ctx, plugin.User{}, s, nil, url.Values{"p.database": {"shellcn"}}, nil))
	if err != nil {
		t.Fatalf("relation graph: %v", err)
	}
	if !hasEdge(graph.(sqldb.GraphPayload), "dbo.orders", "dbo.people") {
		t.Fatalf("expected FK edge orders -> people, got %#v", graph)
	}

	exerciseUserManagement(ctx, t, s)
	exerciseJobControl(ctx, t, s)
}

// exerciseUserManagement round-trips create -> grant -> drop for a contained
// database user in the seeded shellcn database.
func exerciseUserManagement(ctx context.Context, t *testing.T, s *Session) {
	t.Helper()
	userName := "shellcn_user_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = s.db.ExecContext(cleanupCtx, "USE [shellcn]; EXEC("+quoteLiteral("DROP USER IF EXISTS "+quoteIdent(userName))+")")
	})

	if _, err := createUser(rowMutationRC(ctx, s, nil, map[string]any{"database": "shellcn", "name": userName})); err != nil {
		t.Fatalf("create user: %v", err)
	}
	var userCount int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM [shellcn].sys.database_principals WHERE name = @p1`, userName).Scan(&userCount); err != nil || userCount != 1 {
		t.Fatalf("expected user %s created, got %d err=%v", userName, userCount, err)
	}

	grantParams := map[string]string{"database": "shellcn", "user": userName}
	if _, err := grantUser(rowMutationRC(ctx, s, grantParams, map[string]any{"permission": "SELECT"})); err != nil {
		t.Fatalf("grant user: %v", err)
	}
	var granted int
	if err := s.db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM [shellcn].sys.database_permissions p
JOIN [shellcn].sys.database_principals dp ON dp.principal_id = p.grantee_principal_id
WHERE dp.name = @p1 AND p.permission_name = 'SELECT' AND p.state_desc = 'GRANT'`, userName).Scan(&granted); err != nil || granted == 0 {
		t.Fatalf("expected SELECT granted to %s, got %d err=%v", userName, granted, err)
	}

	if _, err := dropUser(plugin.NewRequestContext(ctx, plugin.User{ID: "u1"}, s, grantParams, nil, nil)); err != nil {
		t.Fatalf("drop user: %v", err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM [shellcn].sys.database_principals WHERE name = @p1`, userName).Scan(&userCount); err != nil || userCount != 0 {
		t.Fatalf("expected user %s dropped, got %d err=%v", userName, userCount, err)
	}
}

// exerciseJobControl creates a throwaway SQL Agent job, then runs the
// enable/disable/start handlers against it. SQL Agent may be unavailable on
// some images; in that case the job-create step is skipped rather than failed.
func exerciseJobControl(ctx context.Context, t *testing.T, s *Session) {
	t.Helper()
	jobNameValue := "shellcn_job_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if _, err := s.db.ExecContext(ctx, "EXEC msdb.dbo.sp_add_job @job_name = @job_name, @enabled = 1", sql.Named("job_name", jobNameValue)); err != nil {
		t.Skipf("SQL Agent unavailable, skipping job control: %v", err)
		return
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = s.db.ExecContext(cleanupCtx, "EXEC msdb.dbo.sp_delete_job @job_name = @job_name", sql.Named("job_name", jobNameValue))
	})

	enabledOf := func() bool {
		var enabled bool
		if err := s.db.QueryRowContext(ctx, `SELECT enabled FROM msdb.dbo.sysjobs WHERE name = @p1`, jobNameValue).Scan(&enabled); err != nil {
			t.Fatalf("read job enabled: %v", err)
		}
		return enabled
	}

	jobParams := map[string]string{"name": jobNameValue}
	if _, err := disableJob(plugin.NewRequestContext(ctx, plugin.User{ID: "u1"}, s, jobParams, nil, nil)); err != nil {
		t.Fatalf("disable job: %v", err)
	}
	if enabledOf() {
		t.Fatal("expected job disabled")
	}
	if _, err := enableJob(plugin.NewRequestContext(ctx, plugin.User{ID: "u1"}, s, jobParams, nil, nil)); err != nil {
		t.Fatalf("enable job: %v", err)
	}
	if !enabledOf() {
		t.Fatal("expected job enabled")
	}
	// The job has no steps/server target, so a start request may legitimately
	// fail; assert only that the handler reaches the procedure without a
	// parameter/validation error.
	if _, err := startJob(plugin.NewRequestContext(ctx, plugin.User{ID: "u1"}, s, jobParams, nil, nil)); err != nil && !errors.Is(err, plugin.ErrUnavailable) {
		t.Fatalf("start job: unexpected error class: %v", err)
	}

	// The job list surfaces the created job and scans enabled as a bool.
	jobs, err := listJobs(plugin.NewRequestContext(ctx, plugin.User{}, s, nil, nil, nil))
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if !pageHasName(jobs.(plugin.Page[row]), jobNameValue) {
		t.Fatalf("created job not listed: %#v", jobs)
	}
}

func hasBranch(tree any, label string) bool {
	for _, n := range tree.(plugin.Page[plugin.TreeNode]).Items {
		if n.Label == label && n.ChildrenSource != nil && !n.Leaf {
			return true
		}
	}
	return false
}

func rowMutationRC(ctx context.Context, s *Session, params map[string]string, body map[string]any) *plugin.RequestContext {
	raw, _ := json.Marshal(body)
	return plugin.NewRequestContext(ctx, plugin.User{ID: "u1"}, s, params, nil, raw)
}

func integrationConfig(ctx context.Context, t *testing.T) map[string]any {
	t.Helper()
	if raw := os.Getenv("SHELLCN_MSSQL_DSN"); raw != "" {
		return configFromDSN(t, raw)
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_MSSQL_DSN is not set")
	}
	name := "shellcn-mssql-it-" + time.Now().UTC().Format("20060102150405")
	password := "ShellCN!23456"
	run(ctx, t, "docker", "run", "-d", "--rm", "--name", name,
		"-e", "ACCEPT_EULA=Y",
		"-e", "MSSQL_SA_PASSWORD="+password,
		"-p", "127.0.0.1::1433",
		"mcr.microsoft.com/mssql/server:2022-latest")
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})
	out := run(ctx, t, "docker", "port", name, "1433/tcp")
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
		"database":  "master",
		"username":  "sa",
		"password":  password,
		"encrypt":   "require",
		"read_only": false,
	}
	deadline := time.Now().Add(90 * time.Second)
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
			t.Fatalf("mssql container did not become ready: %v", err)
		}
		time.Sleep(time.Second)
	}
}

func configFromDSN(t *testing.T, raw string) map[string]any {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse SHELLCN_MSSQL_DSN: %v", err)
	}
	port := defaultPort
	if u.Port() != "" {
		if port, err = strconv.Atoi(u.Port()); err != nil {
			t.Fatalf("parse DSN port: %v", err)
		}
	}
	password, _ := u.User.Password()
	encrypt := stringDefault(u.Query().Get("encrypt"), "require")
	if trust := u.Query().Get("TrustServerCertificate"); strings.EqualFold(trust, "true") {
		encrypt = "require"
	}
	return map[string]any{
		"host":      u.Hostname(),
		"port":      port,
		"database":  stringDefault(strings.TrimPrefix(u.Path, "/"), "master"),
		"username":  u.User.Username(),
		"password":  password,
		"encrypt":   encrypt,
		"read_only": false,
	}
}

func seed(ctx context.Context, t *testing.T, s *Session) {
	t.Helper()
	statements := []string{
		`IF DB_ID(N'shellcn') IS NULL CREATE DATABASE [shellcn]`,
		`DROP TABLE IF EXISTS [shellcn].[dbo].[people]`,
		`CREATE TABLE [shellcn].[dbo].[people] (
  id bigint IDENTITY(1,1) PRIMARY KEY,
  name nvarchar(255) NOT NULL,
  access_token nvarchar(255) NOT NULL
)`,
		`INSERT INTO [shellcn].[dbo].[people] (name, access_token) VALUES (N'alice', N'secret-token')`,
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
