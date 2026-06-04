package clickhouse

import (
	"context"
	"errors"
	"testing"

	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

func TestManifestRegistersAndStaysDirectOnly(t *testing.T) {
	reg := plugin.NewRegistry()
	if err := reg.Register(New()); err != nil {
		t.Fatalf("register ClickHouse plugin: %v", err)
	}
	m, ok := reg.Manifest(protocolName)
	if !ok {
		t.Fatal("manifest not registered")
	}
	if m.Agent != nil {
		t.Fatal("ClickHouse must not declare agent transport")
	}
	if len(m.SupportedTransports) != 1 || m.SupportedTransports[0] != plugin.TransportDirect {
		t.Fatalf("unexpected transports: %+v", m.SupportedTransports)
	}
	if !reg.CredentialKindSupportsProtocol(plugin.CredentialDBPassword, protocolName) {
		t.Fatal("database password credential should support ClickHouse")
	}
	if !reg.CredentialKindSupportsProtocol(plugin.CredentialTLSClientCert, protocolName) {
		t.Fatal("TLS client certificate credential should support ClickHouse")
	}
}

func TestQuerySafetyStopsBeforeDatabase(t *testing.T) {
	_, err := executeQueryRequest(context.Background(), &Session{opts: options{ReadOnly: true}}, sqldb.QueryRequest{Query: "insert into events values (1)"})
	if !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("expected read-only forbidden error, got %v", err)
	}
	_, err = executeQueryRequest(context.Background(), &Session{opts: options{RequireConfirm: true}}, sqldb.QueryRequest{Query: "system reload dictionaries"})
	var confirmErr confirmationError
	if !errors.As(err, &confirmErr) {
		t.Fatalf("expected confirmation error, got %v", err)
	}
	if got := queryAuditResult(err); got != plugin.AuditDenied {
		t.Fatalf("confirmation should audit as denied, got %s", got)
	}
}

func TestClickHouseDDLColumnValidation(t *testing.T) {
	col, err := ddlColumn(sqldb.ColumnSpec{Name: "event_time", Type: "DateTime"})
	if err != nil {
		t.Fatalf("valid column rejected: %v", err)
	}
	if col != "`event_time` DateTime" {
		t.Fatalf("unexpected column: %q", col)
	}
	col, err = ddlColumn(sqldb.ColumnSpec{Name: "email", Type: "String", Nullable: true, Default: "''"})
	if err != nil {
		t.Fatalf("valid nullable column rejected: %v", err)
	}
	if col != "`email` Nullable(String) DEFAULT ''" {
		t.Fatalf("unexpected nullable column: %q", col)
	}
	if _, err := ddlColumn(sqldb.ColumnSpec{Name: "bad-name", Type: "String"}); err == nil {
		t.Fatal("invalid identifier accepted")
	}
	if _, err := ddlColumn(sqldb.ColumnSpec{Name: "name", Type: "String; drop table users"}); err == nil {
		t.Fatal("unsafe type accepted")
	}
}

func TestRedactRowsMasksConfiguredColumns(t *testing.T) {
	rows := []row{{"id": uint64(1), "access_token": "plain", "name": "alice"}}
	redactRows(rows, sqldb.DefaultRedactColumnPatterns())
	if rows[0]["access_token"] != sqldb.RedactedValue || rows[0]["name"] != "alice" {
		t.Fatalf("unexpected row redaction: %#v", rows)
	}
}

func TestInsertRowStatement(t *testing.T) {
	stmt, args, err := dialect.Insert(qualified("analytics", "events"), map[string]any{"id": int64(7), "name": "click"})
	if err != nil {
		t.Fatalf("insert build failed: %v", err)
	}
	want := "INSERT INTO `analytics`.`events` (`id`, `name`) VALUES (?, ?)"
	if stmt != want {
		t.Fatalf("unexpected insert statement:\n got %q\nwant %q", stmt, want)
	}
	if len(args) != 2 || args[0] != int64(7) || args[1] != "click" {
		t.Fatalf("unexpected insert args: %#v", args)
	}
}

func TestAlterUpdateMutationStatement(t *testing.T) {
	stmt, args, err := buildAlterUpdate(qualified("analytics", "events"),
		map[string]any{"id": int64(7)}, map[string]any{"name": "view", "count": int64(3)})
	if err != nil {
		t.Fatalf("update build failed: %v", err)
	}
	want := "ALTER TABLE `analytics`.`events` UPDATE `count` = ?, `name` = ? WHERE `id` = ?"
	if stmt != want {
		t.Fatalf("unexpected update statement:\n got %q\nwant %q", stmt, want)
	}
	if len(args) != 3 || args[0] != int64(3) || args[1] != "view" || args[2] != int64(7) {
		t.Fatalf("unexpected update args: %#v", args)
	}
}

func TestAlterDeleteMutationStatement(t *testing.T) {
	stmt, args, err := buildAlterDelete(qualified("analytics", "events"), map[string]any{"id": int64(7), "shard": "a"})
	if err != nil {
		t.Fatalf("delete build failed: %v", err)
	}
	want := "ALTER TABLE `analytics`.`events` DELETE WHERE `id` = ? AND `shard` = ?"
	if stmt != want {
		t.Fatalf("unexpected delete statement:\n got %q\nwant %q", stmt, want)
	}
	if len(args) != 2 || args[0] != int64(7) || args[1] != "a" {
		t.Fatalf("unexpected delete args: %#v", args)
	}
}

func TestMutationNullKeyMatchesIsNull(t *testing.T) {
	stmt, args, err := buildAlterDelete(qualified("db", "t"), map[string]any{"k": nil})
	if err != nil {
		t.Fatalf("delete build failed: %v", err)
	}
	if stmt != "ALTER TABLE `db`.`t` DELETE WHERE `k` IS NULL" {
		t.Fatalf("unexpected null-key statement: %q", stmt)
	}
	if len(args) != 0 {
		t.Fatalf("null-key match must bind no args, got %#v", args)
	}
}

func TestMutationRejectsEmptyKeyAndValues(t *testing.T) {
	if _, _, err := buildAlterDelete(qualified("db", "t"), nil); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("delete without key must be rejected, got %v", err)
	}
	if _, _, err := buildAlterUpdate(qualified("db", "t"), nil, map[string]any{"a": 1}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("update without key must be rejected, got %v", err)
	}
	if _, _, err := buildAlterUpdate(qualified("db", "t"), map[string]any{"id": 1}, nil); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("update without values must be rejected, got %v", err)
	}
}

func TestMutationRejectsUnsafeIdentifier(t *testing.T) {
	if _, _, err := buildAlterUpdate(qualified("db", "t"), map[string]any{"id": 1}, map[string]any{"bad-col": 1}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("unsafe set column must be rejected, got %v", err)
	}
	if _, _, err := buildAlterDelete(qualified("db", "t"), map[string]any{"bad key": 1}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("unsafe key column must be rejected, got %v", err)
	}
}

func TestAddIndexStatement(t *testing.T) {
	clause, err := buildAddIndex("ix_value", "value", "minmax", 4)
	if err != nil {
		t.Fatalf("add index build failed: %v", err)
	}
	if clause != "ADD INDEX `ix_value` value TYPE minmax GRANULARITY 4" {
		t.Fatalf("unexpected add index clause: %q", clause)
	}
	// Non-positive granularity defaults to 1; set(0) is a valid index type.
	clause, err = buildAddIndex("ix_name", "lower(name)", "set(0)", 0)
	if err != nil {
		t.Fatalf("add index build failed: %v", err)
	}
	if clause != "ADD INDEX `ix_name` lower(name) TYPE set(0) GRANULARITY 1" {
		t.Fatalf("unexpected add index clause: %q", clause)
	}
}

func TestAddIndexRejectsUnsafeInput(t *testing.T) {
	if _, err := buildAddIndex("bad-name", "value", "minmax", 1); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("unsafe index name must be rejected, got %v", err)
	}
	if _, err := buildAddIndex("ix", "value; DROP TABLE t", "minmax", 1); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("unsafe index expression must be rejected, got %v", err)
	}
	if _, err := buildAddIndex("ix", "value", "minmax; DROP TABLE t", 1); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("unsafe index type must be rejected, got %v", err)
	}
	if _, err := buildAddIndex("ix", "", "minmax", 1); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("empty index expression must be rejected, got %v", err)
	}
}

func TestAddConstraintStatement(t *testing.T) {
	clause, err := buildAddConstraint("age_positive", "age >= 0")
	if err != nil {
		t.Fatalf("add constraint build failed: %v", err)
	}
	if clause != "ADD CONSTRAINT `age_positive` CHECK age >= 0" {
		t.Fatalf("unexpected add constraint clause: %q", clause)
	}
}

func TestAddConstraintRejectsUnsafeInput(t *testing.T) {
	if _, err := buildAddConstraint("bad name", "age >= 0"); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("unsafe constraint name must be rejected, got %v", err)
	}
	if _, err := buildAddConstraint("c", "age >= 0; DROP TABLE t"); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("unsafe constraint expression must be rejected, got %v", err)
	}
	if _, err := buildAddConstraint("c", ""); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("empty constraint expression must be rejected, got %v", err)
	}
}

func TestModifyColumnReusesDDLColumn(t *testing.T) {
	// ALTER ... MODIFY COLUMN reuses ddlColumn, so the rendered column definition
	// matches the ADD COLUMN form, including the Nullable() wrap and DEFAULT.
	col, err := ddlColumn(sqldb.ColumnSpec{Name: "score", Type: "Float64", Default: "0"})
	if err != nil {
		t.Fatalf("valid modify column rejected: %v", err)
	}
	if col != "`score` Float64 DEFAULT 0" {
		t.Fatalf("unexpected modify column: %q", col)
	}
}

func TestQuoteLiteralEscapes(t *testing.T) {
	cases := map[string]string{
		"abc":            "'abc'",
		"a'b":            `'a\'b'`,
		`a\b`:            `'a\\b'`,
		`'; DROP USER x`: `'\'; DROP USER x'`,
	}
	for in, want := range cases {
		if got := quoteLiteral(in); got != want {
			t.Fatalf("quoteLiteral(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestBuildKillQuery(t *testing.T) {
	if got := buildKillQuery("abc-123"); got != "KILL QUERY WHERE query_id = 'abc-123'" {
		t.Fatalf("unexpected kill query: %q", got)
	}
	// A query_id carrying a quote is escaped into the string literal, never breaking out.
	if got := buildKillQuery("a' OR '1'='1"); got != `KILL QUERY WHERE query_id = 'a\' OR \'1\'=\'1'` {
		t.Fatalf("unexpected escaped kill query: %q", got)
	}
}

func TestBuildKillMutation(t *testing.T) {
	stmt, err := buildKillMutation("analytics", "events", "0000000001")
	if err != nil {
		t.Fatalf("kill mutation build failed: %v", err)
	}
	want := "KILL MUTATION WHERE database = 'analytics' AND table = 'events' AND mutation_id = '0000000001'"
	if stmt != want {
		t.Fatalf("unexpected kill mutation:\n got %q\nwant %q", stmt, want)
	}
	if _, err := buildKillMutation("analytics", "events", "   "); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("empty mutation id must be rejected, got %v", err)
	}
}

func TestBuildCreateUser(t *testing.T) {
	stmt, err := buildCreateUser("reporter", "sha256_password", "s3cr3t", true)
	if err != nil {
		t.Fatalf("create user build failed: %v", err)
	}
	want := "CREATE USER IF NOT EXISTS `reporter` IDENTIFIED WITH sha256_password BY 's3cr3t'"
	if stmt != want {
		t.Fatalf("unexpected create user:\n got %q\nwant %q", stmt, want)
	}
	stmt, err = buildCreateUser("svc", "no_password", "", false)
	if err != nil {
		t.Fatalf("no_password user build failed: %v", err)
	}
	if stmt != "CREATE USER `svc` IDENTIFIED WITH no_password" {
		t.Fatalf("unexpected no_password user: %q", stmt)
	}
	// A password carrying a quote is escaped into the literal.
	stmt, err = buildCreateUser("svc", "plaintext_password", "a'b", false)
	if err != nil {
		t.Fatalf("plaintext user build failed: %v", err)
	}
	if stmt != `CREATE USER `+"`svc`"+` IDENTIFIED WITH plaintext_password BY 'a\'b'` {
		t.Fatalf("unexpected escaped password user: %q", stmt)
	}
}

func TestBuildCreateUserRejectsBadInput(t *testing.T) {
	if _, err := buildCreateUser("bad-name", "sha256_password", "x", false); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("unsafe user name must be rejected, got %v", err)
	}
	if _, err := buildCreateUser("u", "sha256_password", "", false); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("missing password must be rejected, got %v", err)
	}
	if _, err := buildCreateUser("u", "ldap", "x", false); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("unsupported auth type must be rejected, got %v", err)
	}
}

func TestBuildGrant(t *testing.T) {
	stmt, err := buildGrant("reporter", "SELECT", "analytics.*", false)
	if err != nil {
		t.Fatalf("grant build failed: %v", err)
	}
	if stmt != "GRANT SELECT ON analytics.* TO `reporter`" {
		t.Fatalf("unexpected grant: %q", stmt)
	}
	stmt, err = buildGrant("admin", "ALL", "*.*", true)
	if err != nil {
		t.Fatalf("grant with option build failed: %v", err)
	}
	if stmt != "GRANT ALL ON *.* TO `admin` WITH GRANT OPTION" {
		t.Fatalf("unexpected grant with option: %q", stmt)
	}
}

func TestBuildGrantRejectsUnsafeInput(t *testing.T) {
	if _, err := buildGrant("bad name", "SELECT", "*.*", false); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("unsafe user must be rejected, got %v", err)
	}
	if _, err := buildGrant("u", "SELECT; DROP USER admin", "*.*", false); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("unsafe privilege must be rejected, got %v", err)
	}
	if _, err := buildGrant("u", "SELECT", "db.*; GRANT ALL ON *.* TO u", false); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("unsafe target must be rejected, got %v", err)
	}
	if _, err := buildGrant("u", "", "*.*", false); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("empty privilege must be rejected, got %v", err)
	}
}

func TestReadOnlyBlocksRowMutation(t *testing.T) {
	if err := ensureWritable(&Session{opts: options{ReadOnly: true}}); !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("read-only mode must block row mutations, got %v", err)
	}
}

func TestNoSortingKeyKeepsGridReadOnly(t *testing.T) {
	rows := []row{{"name": "click", "value": int64(1)}}
	attachRowKeys(rows, nil, nil) // no sorting key -> no _key attached
	if _, ok := rows[0]["_key"]; ok {
		t.Fatal("rows without a sorting key must not carry _key")
	}
	// A key column that is itself sensitive also keeps the grid read-only.
	rows = []row{{"token": "abc", "value": int64(1)}}
	attachRowKeys(rows, []string{"token"}, sqldb.DefaultRedactColumnPatterns())
	if _, ok := rows[0]["_key"]; ok {
		t.Fatal("rows with a sensitive key column must not carry _key")
	}
	// A real sorting key produces a _key map mutations can echo back.
	rows = []row{{"id": int64(9), "value": int64(1)}}
	attachRowKeys(rows, []string{"id"}, sqldb.DefaultRedactColumnPatterns())
	key, ok := rows[0]["_key"].(map[string]any)
	if !ok || key["id"] != int64(9) {
		t.Fatalf("expected _key with sorting key value, got %#v", rows[0]["_key"])
	}
	// Such a key must pass row-key validation against the sorting key.
	if err := sqldb.ValidateRowKey([]string{"id"}, key); err != nil {
		t.Fatalf("sorting-key row key should validate: %v", err)
	}
	if err := sqldb.ValidateRowKey(nil, key); !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("validation against an empty sorting key must be forbidden, got %v", err)
	}
}

func TestTableCreateColumnsIsStructuredArray(t *testing.T) {
	assertColumnsArray(t, New(), "clickhouse.table.create", []string{"name", "type", "nullable", "default"})
}

func assertColumnsArray(t *testing.T, p plugin.Plugin, routeID string, wantKeys []string) {
	t.Helper()
	var schema *plugin.Schema
	for _, r := range p.Routes() {
		if r.ID == routeID {
			schema = r.Input
			break
		}
	}
	if schema == nil {
		t.Fatalf("route %q has no input schema", routeID)
	}
	var columns *plugin.Field
	for _, g := range schema.Groups {
		for i := range g.Fields {
			if g.Fields[i].Key == "columns" {
				columns = &g.Fields[i]
			}
		}
	}
	if columns == nil {
		t.Fatalf("%s: no columns field", routeID)
	}
	if columns.Type != plugin.FieldArray {
		t.Fatalf("%s: columns is %q, want array", routeID, columns.Type)
	}
	if columns.Item == nil || columns.Item.Type != plugin.FieldObject {
		t.Fatalf("%s: columns item is not an object", routeID)
	}
	got := make([]string, 0, len(columns.Item.Fields))
	for _, f := range columns.Item.Fields {
		got = append(got, f.Key)
	}
	if len(got) != len(wantKeys) {
		t.Fatalf("%s: columns item keys = %v, want %v", routeID, got, wantKeys)
	}
	for i, k := range wantKeys {
		if got[i] != k {
			t.Fatalf("%s: columns item keys = %v, want %v", routeID, got, wantKeys)
		}
	}
}
