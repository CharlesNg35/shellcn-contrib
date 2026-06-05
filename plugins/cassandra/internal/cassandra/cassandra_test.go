package cassandra

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/gocql/gocql"

	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestManifestRegistersAndStaysDirectOnly(t *testing.T) {
	p := New()
	m := p.Manifest()
	plugintest.ValidatePlugin(t, p)
	if m.Agent != nil {
		t.Fatal("Cassandra must not declare agent transport")
	}
	if len(m.SupportedTransports) != 1 || m.SupportedTransports[0] != plugin.TransportDirect {
		t.Fatalf("unexpected transports: %+v", m.SupportedTransports)
	}
	if !plugintest.CredentialKindSupported(m.Config, plugin.CredentialDBPassword) {
		t.Fatal("database password credential should support Cassandra")
	}
	if !plugintest.CredentialKindSupported(m.Config, plugin.CredentialTLSClientCert) {
		t.Fatal("TLS client certificate credential should support Cassandra")
	}
}

func TestParseOptionsDefaultsToNoAuth(t *testing.T) {
	opts, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{"hosts": "127.0.0.1"}})
	if err != nil {
		t.Fatalf("parse options: %v", err)
	}
	if opts.Username != "" || opts.Password != "" || opts.Port != defaultPort || opts.Consistency != gocql.LocalQuorum {
		t.Fatalf("unexpected defaults: %+v", opts)
	}
}

func TestParseOptionsUsesPasswordCredentialAndTLSCredential(t *testing.T) {
	opts, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{
		"hosts":                   "db1, db2",
		"auth":                    authCredential,
		plugin.CredentialIdentity: "cassandra",
		plugin.CredentialSecret:   "secret",
		"tls_mode":                "require",
		"_client_cert_id_secret":  "pem-material",
	}})
	if err != nil {
		t.Fatalf("parse options: %v", err)
	}
	if opts.Username != "cassandra" || opts.Password != "secret" || opts.ClientCertificate != "pem-material" || len(opts.Hosts) != 2 {
		t.Fatalf("unexpected credential material: %+v", opts)
	}
}

func TestQuerySafetyStopsBeforeDatabase(t *testing.T) {
	_, err := executeQueryRequest(context.Background(), &Session{opts: options{ReadOnly: true}}, sqldb.QueryRequest{Query: "insert into events (id) values (1)"})
	if !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("expected read-only forbidden error, got %v", err)
	}
	_, err = executeQueryRequest(context.Background(), &Session{opts: options{RequireConfirm: true}}, sqldb.QueryRequest{Query: "truncate events"})
	var confirmErr confirmationError
	if !errors.As(err, &confirmErr) {
		t.Fatalf("expected confirmation error, got %v", err)
	}
	if got := queryAuditResult(err); got != plugin.AuditDenied {
		t.Fatalf("confirmation should audit as denied, got %s", got)
	}
}

func TestCQLDDLValidation(t *testing.T) {
	cols, err := parseColumns([]any{
		map[string]any{"name": "id", "type": "uuid"},
		map[string]any{"name": "payload", "type": "frozen<map<text, text>>"},
	})
	if err != nil {
		t.Fatalf("valid columns rejected: %v", err)
	}
	if len(cols) != 2 || cols[0] != `"id" uuid` {
		t.Fatalf("unexpected columns: %#v", cols)
	}
	if _, err := parseColumns([]any{map[string]any{"name": "bad-name", "type": "text"}}); err == nil {
		t.Fatal("invalid identifier accepted")
	}
	if safePrimaryKey("id); DROP TABLE users") {
		t.Fatal("unsafe primary key accepted")
	}
}

func TestDropKeyspaceCQLGeneration(t *testing.T) {
	if got := "DROP KEYSPACE " + quoteIdent("shellcn_drop_it"); got != `DROP KEYSPACE "shellcn_drop_it"` {
		t.Fatalf("unexpected drop keyspace CQL: %s", got)
	}
	// Identifier quoting must escape embedded quotes so a crafted name can't break out.
	if got := quoteIdent(`a"b`); got != `"a""b"` {
		t.Fatalf("unexpected quoting: %s", got)
	}
}

func TestCreateTypeCQLGeneration(t *testing.T) {
	fields, err := parseColumns([]any{
		map[string]any{"name": "street", "type": "text"},
		map[string]any{"name": "zip", "type": "frozen<list<text>>"},
	})
	if err != nil {
		t.Fatalf("valid UDT fields rejected: %v", err)
	}
	cql := "CREATE TYPE IF NOT EXISTS " + qualified("shop", "address") + " (" + strings.Join(fields, ", ") + ")"
	want := `CREATE TYPE IF NOT EXISTS "shop"."address" ("street" text, "zip" frozen<list<text>>)`
	if cql != want {
		t.Fatalf("unexpected create type CQL:\n got: %s\nwant: %s", cql, want)
	}
	if _, err := parseColumns([]any{map[string]any{"name": "bad-field", "type": "text"}}); err == nil {
		t.Fatal("invalid UDT field identifier accepted")
	}
	if _, err := parseColumns([]any{map[string]any{"name": "x", "type": "text; drop table users"}}); err == nil {
		t.Fatal("unsafe UDT field type accepted")
	}
}

func TestDropTypeCQLGeneration(t *testing.T) {
	if got := "DROP TYPE " + qualified("shop", "address"); got != `DROP TYPE "shop"."address"` {
		t.Fatalf("unexpected drop type CQL: %s", got)
	}
}

func TestRowMutationCQLGeneration(t *testing.T) {
	table := qualified("shop", "orders")

	insertCQL, insertArgs, err := dialect.Insert(table, map[string]any{"id": "u1", "total": float64(42)})
	if err != nil {
		t.Fatalf("insert build: %v", err)
	}
	if insertCQL != `INSERT INTO "shop"."orders" ("id", "total") VALUES (?, ?)` {
		t.Fatalf("unexpected insert CQL: %s", insertCQL)
	}
	if len(insertArgs) != 2 || insertArgs[0] != "u1" || insertArgs[1] != int64(42) {
		t.Fatalf("unexpected insert args: %#v", insertArgs)
	}

	updateCQL, updateArgs, err := dialect.Update(table, map[string]any{"id": "u1"}, map[string]any{"total": float64(7)})
	if err != nil {
		t.Fatalf("update build: %v", err)
	}
	if updateCQL != `UPDATE "shop"."orders" SET "total" = ? WHERE "id" = ?` {
		t.Fatalf("unexpected update CQL: %s", updateCQL)
	}
	if len(updateArgs) != 2 || updateArgs[0] != int64(7) || updateArgs[1] != "u1" {
		t.Fatalf("unexpected update args: %#v", updateArgs)
	}

	deleteCQL, deleteArgs, err := dialect.Delete(table, map[string]any{"tenant": "t1", "id": "u1"})
	if err != nil {
		t.Fatalf("delete build: %v", err)
	}
	if deleteCQL != `DELETE FROM "shop"."orders" WHERE "id" = ? AND "tenant" = ?` {
		t.Fatalf("unexpected delete CQL: %s", deleteCQL)
	}
	if len(deleteArgs) != 2 || deleteArgs[0] != "u1" || deleteArgs[1] != "t1" {
		t.Fatalf("unexpected delete args: %#v", deleteArgs)
	}

	if _, _, err := dialect.Insert(table, nil); err == nil {
		t.Fatal("insert with no values should be rejected")
	}
	if _, _, err := dialect.Delete(table, nil); err == nil {
		t.Fatal("delete with no key should be rejected (would sweep the table)")
	}
}

func TestRowMutationCQLRejectsUnsafeIdentifier(t *testing.T) {
	table := qualified("shop", "orders")
	if _, _, err := dialect.Insert(table, map[string]any{"bad-col": 1}); err == nil {
		t.Fatal("insert with unsafe column identifier should be rejected")
	}
	if _, _, err := dialect.Update(table, map[string]any{"id": "x"}, map[string]any{"bad col": 1}); err == nil {
		t.Fatal("update with unsafe set identifier should be rejected")
	}
}

func TestAttachRowKeysGuards(t *testing.T) {
	// No primary key: rows stay read-only (no _key attached).
	rows := []row{{"id": "u1"}}
	attachRowKeys(rows, nil, nil)
	if _, ok := rows[0]["_key"]; ok {
		t.Fatal("keyless table must not receive a _key (stays read-only)")
	}

	// Sensitive key column: refuse to ship the raw key value to the client.
	rows = []row{{"api_key": "secret", "value": 1}}
	attachRowKeys(rows, []string{"api_key"}, sqldb.DefaultRedactColumnPatterns())
	if _, ok := rows[0]["_key"]; ok {
		t.Fatal("sensitive key column must keep the grid read-only")
	}

	// Usable composite key: _key carries exactly the key columns.
	rows = []row{{"tenant": "t1", "id": "u1", "total": 9}}
	attachRowKeys(rows, []string{"tenant", "id"}, nil)
	key, ok := rows[0]["_key"].(map[string]any)
	if !ok {
		t.Fatalf("expected _key map, got %#v", rows[0]["_key"])
	}
	if len(key) != 2 || key["tenant"] != "t1" || key["id"] != "u1" {
		t.Fatalf("unexpected _key: %#v", key)
	}
}

func TestValidateRowKeyRejectsNonPrimaryKey(t *testing.T) {
	// A key that is not exactly the primary key must be rejected so a mutation
	// cannot widen its WHERE clause beyond a single identified row.
	if err := sqldb.ValidateRowKey([]string{"id"}, map[string]any{"name": "x"}); err == nil {
		t.Fatal("non-primary-key column accepted as row key")
	}
	if err := sqldb.ValidateRowKey(nil, map[string]any{"id": "x"}); !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("keyless table should forbid mutations, got %v", err)
	}
	if err := sqldb.ValidateRowKey([]string{"id"}, map[string]any{"id": "x"}); err != nil {
		t.Fatalf("exact primary key rejected: %v", err)
	}
}

func TestStructuredArrayFields(t *testing.T) {
	p := New()
	assertArrayItemKeys(t, p, "cassandra.table.create", "columns", []string{"name", "type"})
	assertArrayItemKeys(t, p, "cassandra.type.create", "fields", []string{"name", "type"})
}

func TestReplicationMapNetworkTopologyAcceptsNumberMap(t *testing.T) {
	// The bound body is `any`; after JSON binding a {dc: number} object is a
	// map[string]any with float64 values — the shape the FieldNumber map submits.
	got, err := replicationMap("NetworkTopologyStrategy", 1, map[string]any{"dc1": float64(3), "dc2": float64(2)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "{'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 2}"
	if got != want {
		t.Fatalf("replicationMap = %q, want %q", got, want)
	}
	if _, err := replicationMap("NetworkTopologyStrategy", 1, map[string]any{"dc1": float64(21)}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("out-of-range factor: want ErrInvalidInput, got %v", err)
	}
}

func TestKeyspaceDatacenterReplicationIsNumberMap(t *testing.T) {
	var schema *plugin.Schema
	for _, r := range New().Routes() {
		if r.ID == "cassandra.keyspace.create" {
			schema = r.Input
		}
	}
	if schema == nil {
		t.Fatal("cassandra.keyspace.create has no input schema")
	}
	var field *plugin.Field
	for _, g := range schema.Groups {
		for i := range g.Fields {
			if g.Fields[i].Key == "datacenter_replication" {
				field = &g.Fields[i]
			}
		}
	}
	if field == nil {
		t.Fatal("no datacenter_replication field")
	}
	if field.Type != plugin.FieldMap {
		t.Fatalf("datacenter_replication is %q, want map", field.Type)
	}
	if field.Item == nil || field.Item.Type != plugin.FieldNumber {
		t.Fatalf("datacenter_replication value item is not a number")
	}
}

func assertArrayItemKeys(t *testing.T, p plugin.Plugin, routeID, fieldKey string, wantKeys []string) {
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
	var field *plugin.Field
	for _, g := range schema.Groups {
		for i := range g.Fields {
			if g.Fields[i].Key == fieldKey {
				field = &g.Fields[i]
			}
		}
	}
	if field == nil {
		t.Fatalf("%s: no %q field", routeID, fieldKey)
	}
	if field.Type != plugin.FieldArray {
		t.Fatalf("%s.%s is %q, want array", routeID, fieldKey, field.Type)
	}
	if field.Item == nil {
		t.Fatalf("%s.%s has no item", routeID, fieldKey)
	}
	if len(wantKeys) == 0 {
		if field.Item.Type != plugin.FieldText {
			t.Fatalf("%s.%s item is %q, want text", routeID, fieldKey, field.Item.Type)
		}
		return
	}
	if field.Item.Type != plugin.FieldObject {
		t.Fatalf("%s.%s item is %q, want object", routeID, fieldKey, field.Item.Type)
	}
	got := make([]string, 0, len(field.Item.Fields))
	for _, f := range field.Item.Fields {
		got = append(got, f.Key)
	}
	if len(got) != len(wantKeys) {
		t.Fatalf("%s.%s item keys = %v, want %v", routeID, fieldKey, got, wantKeys)
	}
	for i, k := range wantKeys {
		if got[i] != k {
			t.Fatalf("%s.%s item keys = %v, want %v", routeID, fieldKey, got, wantKeys)
		}
	}
}
