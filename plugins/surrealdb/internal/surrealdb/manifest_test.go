package surrealdb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

func TestManifestValidates(t *testing.T) {
	p := &Plugin{}
	plugintest.ValidatePlugin(t, p)
}

func TestParseOptions(t *testing.T) {
	cfg := plugin.ConnectConfig{Config: map[string]any{
		"host":      "db.internal",
		"port":      float64(9000), // JSON numbers decode to float64
		"tls":       true,
		"namespace": "ns",
		"database":  "app",
		"username":  "root",
		"password":  "secret",
	}}
	o, err := parseOptions(cfg)
	if err != nil {
		t.Fatalf("parseOptions: %v", err)
	}
	if o.scheme != "https" || o.addr() != "db.internal:9000" {
		t.Fatalf("unexpected target: %s %s", o.scheme, o.addr())
	}
	if o.namespace != "ns" || o.database != "app" {
		t.Fatalf("unexpected ns/db: %s/%s", o.namespace, o.database)
	}
	if !o.readOnly || o.timeout != defaultQueryTimeout || o.rowLimit != defaultRowLimit {
		t.Fatalf("unexpected safety defaults: readOnly=%v timeout=%s rowLimit=%d", o.readOnly, o.timeout, o.rowLimit)
	}
}

func TestParseOptionsSafetyConfig(t *testing.T) {
	cfg := plugin.ConnectConfig{Config: map[string]any{
		"namespace":     "ns",
		"database":      "app",
		"read_only":     false,
		"query_timeout": "5s",
		"row_limit":     float64(12),
	}}
	o, err := parseOptions(cfg)
	if err != nil {
		t.Fatalf("parseOptions: %v", err)
	}
	if o.readOnly || o.timeout != 5*time.Second || o.rowLimit != 12 {
		t.Fatalf("unexpected safety config: readOnly=%v timeout=%s rowLimit=%d", o.readOnly, o.timeout, o.rowLimit)
	}
}

func TestParseOptionsRequiresNamespaceAndDatabase(t *testing.T) {
	_, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{"host": "h"}})
	if err == nil {
		t.Fatal("expected error when namespace/database missing")
	}
}

func TestConnectRequiresNetworkTransport(t *testing.T) {
	_, err := (&Plugin{}).Connect(context.Background(), plugin.ConnectConfig{Config: map[string]any{
		"namespace": "ns",
		"database":  "app",
	}})
	if !errors.Is(err, plugin.ErrUnavailable) {
		t.Fatalf("expected unavailable transport error, got %v", err)
	}
}

func TestSplitRecordID(t *testing.T) {
	tb, key, ok := splitRecordID("person:alice")
	if !ok || tb != "person" || key != "alice" {
		t.Fatalf("split failed: %q %q %v", tb, key, ok)
	}
	if _, _, ok := splitRecordID("noseparator"); ok {
		t.Fatal("expected failure without separator")
	}
}

func TestSurrealReadOnlyClassifier(t *testing.T) {
	for _, q := range []string{
		"SELECT * FROM person;",
		"INFO FOR DB;",
		"RETURN true;",
		"SHOW CHANGES FOR TABLE person;",
		"-- comment\nSELECT * FROM person;",
		"/* comment */ INFO FOR TABLE person;",
	} {
		if !isReadOnlySurrealQL(q) {
			t.Fatalf("expected read-only query: %s", q)
		}
	}
	for _, q := range []string{
		"CREATE person CONTENT {name: 'alice'};",
		"UPDATE person:alice SET name = 'Alice';",
		"DELETE person:alice;",
		"DEFINE TABLE person;",
		"REMOVE TABLE person;",
		"RELATE person:alice->knows->person:bob;",
		"LET $x = 1;",
		"LIVE SELECT * FROM person;",
	} {
		if isReadOnlySurrealQL(q) {
			t.Fatalf("expected mutating/unknown query to be blocked: %s", q)
		}
	}
}

func TestSplitSurrealStatementsRespectsStringsAndComments(t *testing.T) {
	statements := splitSurrealStatements("SELECT 'a;b'; -- ignored ;\nINFO FOR DB; /* ignored ; */ RETURN true")
	if len(statements) != 3 {
		t.Fatalf("expected 3 statements, got %#v", statements)
	}
}

func TestQueryAuditParamsDoNotIncludeRawQuery(t *testing.T) {
	query := "SELECT * FROM user WHERE password = 'secret';"
	params := queryAuditParams(query, splitSurrealStatements(query), true, 2, 15)
	if params["query_sha256"] == "" || params["statement_count"] != "1" || params["first_statement"] != "SELECT" {
		t.Fatalf("unexpected audit params: %#v", params)
	}
	if _, ok := params["query"]; ok {
		t.Fatalf("raw query leaked in audit params: %#v", params)
	}
}

func TestResultsToGridAppliesRowLimit(t *testing.T) {
	res := []surrealdb.QueryResult[any]{{
		Status: "OK",
		Result: []any{
			map[string]any{"id": "person:1", "name": "alice"},
			map[string]any{"id": "person:2", "name": "bob"},
			map[string]any{"id": "person:3", "name": "carol"},
		},
	}}
	out := resultsToGrid(&res, 2)
	if !out.Truncated || out.RowCount != 3 || len(out.Rows) != 2 {
		t.Fatalf("expected truncated grid, got %#v", out)
	}
}

func TestReadOnlyGuardBlocksWrites(t *testing.T) {
	called := false
	rc := plugin.NewRequestContext(
		t.Context(),
		plugin.User{ID: "u"},
		&session{opts: options{readOnly: true}},
		nil,
		nil,
		nil,
	)
	_, err := readOnlyGuard(func(*plugin.RequestContext) (any, error) {
		called = true
		return map[string]any{"ok": true}, nil
	})(rc)
	if !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("expected forbidden error, got %v", err)
	}
	if called {
		t.Fatal("guard called wrapped handler in read-only mode")
	}
}
