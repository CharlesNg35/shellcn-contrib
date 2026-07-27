package snowflake

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	sf "github.com/snowflakedb/gosnowflake/v2"

	contribtest "github.com/charlesng35/shellcn-contrib/shared/plugintest"
	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

const testQueryID = "3f1a25f6-0000-4000-8000-000000000001"

// ---------------------------------------------------------------- manifest

func TestManifestRegistersAndStaysDirectOnly(t *testing.T) {
	p := New()
	m := p.Manifest()
	plugintest.ValidatePlugin(t, p)
	if m.Agent != nil {
		t.Fatal("Snowflake must not declare agent transport")
	}
	if len(m.SupportedTransports) != 1 || m.SupportedTransports[0] != plugin.TransportDirect {
		t.Fatalf("unexpected transports: %+v", m.SupportedTransports)
	}
	if !plugintest.CredentialKindSupported(m.Config, plugin.CredentialKindDBPassword) {
		t.Fatal("database password credential should support Snowflake")
	}
	if !plugintest.CredentialKindSupported(m.Config, credentialKindKeyPair) {
		t.Fatal("key pair credential should support Snowflake")
	}
}

// TestKeyPairCredentialKindKeepsMaterialSecret guards the credential contract:
// the private key must be stored as secret material, never as listable metadata.
func TestKeyPairCredentialKindKeepsMaterialSecret(t *testing.T) {
	kinds := credentialKinds()
	if len(kinds) != 1 || kinds[0].Kind != credentialKindKeyPair {
		t.Fatalf("unexpected credential kinds: %+v", kinds)
	}
	for _, field := range kinds[0].Fields {
		switch field.Key {
		case "private_key":
			if !field.Secret || field.Public {
				t.Fatalf("private key must be secret material: %+v", field)
			}
		case "username":
			if field.Secret {
				t.Fatalf("user name must not be stored as secret material: %+v", field)
			}
		default:
			t.Fatalf("unexpected credential field %q", field.Key)
		}
	}
}

func TestStreamKindsMatchPanels(t *testing.T) {
	want := map[string]plugin.StreamKind{
		"snowflake.query":             plugin.StreamQuery,
		"snowflake.account.metrics":   plugin.StreamMetrics,
		"snowflake.warehouse.metrics": plugin.StreamMetrics,
	}
	for _, stream := range New().Manifest().Streams {
		kind, ok := want[stream.ID]
		if !ok {
			t.Fatalf("undeclared stream %q", stream.ID)
		}
		if stream.Kind != kind {
			t.Fatalf("stream %q is %q, want %q", stream.ID, stream.Kind, kind)
		}
		delete(want, stream.ID)
	}
	if len(want) > 0 {
		t.Fatalf("missing streams: %v", want)
	}
}

func TestTableDataGridIsEditableThroughDeclaredRoutes(t *testing.T) {
	p := New()
	routeIDs := map[string]bool{}
	for _, r := range p.Routes() {
		routeIDs[r.ID] = true
	}
	var data plugin.Panel
	for _, res := range p.Manifest().Resources {
		if res.Kind != "table" {
			continue
		}
		for _, tab := range res.Detail.Tabs {
			if tab.Key == "data" {
				data = tab
			}
		}
	}
	cfg, ok := data.Config.(plugin.TableConfig)
	if data.Key == "" || !ok || !cfg.Editable {
		t.Fatalf("table Data tab must be an editable grid: %#v", data.Config)
	}
	for key, ds := range map[string]*plugin.DataSource{"insert": cfg.Insert, "update": cfg.Update, "delete": cfg.Delete, "columns": cfg.ColumnsSource} {
		if ds == nil {
			t.Fatalf("Data tab missing %q source", key)
		}
		if !routeIDs[ds.RouteID] {
			t.Fatalf("Data tab %q points at missing route %q", key, ds.RouteID)
		}
	}
}

func TestTableCreateColumnsIsStructuredArray(t *testing.T) {
	var schema *plugin.Schema
	for _, r := range New().Routes() {
		if r.ID == "snowflake.table.create" {
			schema = r.Input
		}
	}
	if schema == nil {
		t.Fatal("snowflake.table.create has no input schema")
	}
	var columns *plugin.Field
	for _, g := range schema.Groups {
		for i := range g.Fields {
			if g.Fields[i].Key == "columns" {
				columns = &g.Fields[i]
			}
		}
	}
	if columns == nil || columns.Type != plugin.FieldArray || columns.Item == nil || columns.Item.Type != plugin.FieldObject {
		t.Fatalf("columns must be an array of column objects: %#v", columns)
	}
	want := []string{"name", "type", "nullable", "primary", "unique", "default"}
	got := make([]string, 0, len(columns.Item.Fields))
	for _, f := range columns.Item.Fields {
		got = append(got, f.Key)
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("columns item keys = %v, want %v", got, want)
	}
}

// ---------------------------------------------------------------- options

func TestParseOptionsUsesStoredPasswordCredential(t *testing.T) {
	opts, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{
		"account":  "myorg-myaccount",
		"host":     "myorg-myaccount.snowflakecomputing.com",
		"database": "TESTDB",
		"auth":     authCredential,
	}, Credentials: plugin.NewResolvedCredentials(plugin.CredentialBinding{
		Field:      credentialIDField,
		Credential: plugin.ResolvedCredential{Values: map[string]string{"username": "svc", "password": "s3cr3t"}},
	})})
	if err != nil {
		t.Fatalf("parse options: %v", err)
	}
	if opts.Username != "svc" || opts.Password != "s3cr3t" || opts.PrivateKey != nil {
		t.Fatalf("unexpected credential material: user=%q keyed=%v", opts.Username, opts.PrivateKey != nil)
	}
	if opts.Schema != defaultSchema || opts.Port != defaultPort {
		t.Fatalf("defaults not applied: %+v", opts)
	}
}

func TestParseOptionsUsesKeyPairCredential(t *testing.T) {
	key, encoded := testPrivateKey(t)
	opts, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{
		"account":  "myorg-myaccount",
		"host":     "myorg-myaccount.snowflakecomputing.com",
		"database": "TESTDB",
		"auth":     authKeyPair,
	}, Credentials: plugin.NewResolvedCredentials(plugin.CredentialBinding{
		Field:      keyPairField,
		Credential: plugin.ResolvedCredential{Values: map[string]string{"username": "svc", "private_key": encoded}},
	})})
	if err != nil {
		t.Fatalf("parse options: %v", err)
	}
	if opts.Username != "svc" || opts.Password != "" {
		t.Fatalf("key pair auth must not carry a password: %+v", opts)
	}
	if opts.PrivateKey == nil || !opts.PrivateKey.Equal(key) {
		t.Fatal("private key was not resolved from the credential")
	}
	cfg, err := driverConfig(opts, stubTransport{})
	if err != nil {
		t.Fatalf("driver config: %v", err)
	}
	if cfg.Password != "" || cfg.PrivateKey == nil {
		t.Fatalf("key pair driver config must sign with the key only: %+v", cfg)
	}
	if cfg.Transporter == nil {
		t.Fatal("driver must egress through the gateway transport")
	}
}

func TestParseOptionsRejectsIncompleteConfig(t *testing.T) {
	base := map[string]any{
		"account":  "myorg-myaccount",
		"host":     "myorg-myaccount.snowflakecomputing.com",
		"database": "TESTDB",
		"username": "svc",
		"password": "secret",
	}
	cases := map[string]func(map[string]any){
		"missing account":  func(m map[string]any) { delete(m, "account") },
		"missing host":     func(m map[string]any) { delete(m, "host") },
		"host with scheme": func(m map[string]any) { m["host"] = "https://x.snowflakecomputing.com" },
		"missing database": func(m map[string]any) { delete(m, "database") },
		"bad database":     func(m map[string]any) { m["database"] = "bad-db" },
		"missing user":     func(m map[string]any) { delete(m, "username") },
		"missing password": func(m map[string]any) { delete(m, "password") },
	}
	for name, mutate := range cases {
		cfg := map[string]any{}
		for k, v := range base {
			cfg[k] = v
		}
		mutate(cfg)
		if _, err := parseOptions(plugin.ConnectConfig{Config: cfg}); !errors.Is(err, plugin.ErrInvalidInput) {
			t.Fatalf("%s: expected invalid input, got %v", name, err)
		}
	}
}

func TestParseOptionsClampsBounds(t *testing.T) {
	opts, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{
		"account":          "a",
		"host":             "a.snowflakecomputing.com",
		"database":         "DB",
		"username":         "u",
		"password":         "p",
		"row_limit":        float64(100000),
		"max_connections":  float64(999),
		"history_window":   float64(999999),
		"metrics_interval": "1s",
	}})
	if err != nil {
		t.Fatalf("parse options: %v", err)
	}
	if opts.RowLimit != plugin.MaxPageLimit || opts.MaxConns != 20 || opts.HistoryWindow != maxHistoryWindow {
		t.Fatalf("bounds not clamped: %+v", opts)
	}
	if opts.MetricsInterval != minMetricsInterval {
		t.Fatalf("metrics interval not clamped: %v", opts.MetricsInterval)
	}
}

// TestDriverConfigFloorsTheStatementTimeout guards a Snowflake foot-gun:
// STATEMENT_TIMEOUT_IN_SECONDS = 0 means "no timeout", so a sub-second query
// timeout must not round down into an unbounded server-side statement.
func TestDriverConfigFloorsTheStatementTimeout(t *testing.T) {
	opts, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{
		"account":       "a",
		"host":          "a.snowflakecomputing.com",
		"database":      "DB",
		"username":      "u",
		"password":      "p",
		"query_timeout": "500ms",
	}})
	if err != nil {
		t.Fatalf("parse options: %v", err)
	}
	cfg, err := driverConfig(opts, stubTransport{})
	if err != nil {
		t.Fatalf("driver config: %v", err)
	}
	got := cfg.Params["statement_timeout_in_seconds"]
	if got == nil || *got != "1" {
		t.Fatalf("statement timeout was not floored: %v", got)
	}
}

func TestParsePrivateKeyRejectsUnusableMaterial(t *testing.T) {
	if _, err := parsePrivateKey("not a pem"); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("non-PEM material must be rejected, got %v", err)
	}
	encrypted := pem.EncodeToMemory(&pem.Block{Type: "ENCRYPTED PRIVATE KEY", Bytes: []byte("x")})
	if _, err := parsePrivateKey(string(encrypted)); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("encrypted key must be rejected, got %v", err)
	}
	if _, err := parsePrivateKey(string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("x")}))); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("non-key PEM must be rejected, got %v", err)
	}
}

func TestParsePrivateKeyAcceptsPKCS1(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	block := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	parsed, err := parsePrivateKey(string(block))
	if err != nil {
		t.Fatalf("PKCS#1 key rejected: %v", err)
	}
	if !parsed.Equal(key) {
		t.Fatal("parsed PKCS#1 key does not match")
	}
}

func testPrivateKey(t *testing.T) (*rsa.PrivateKey, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	return key, string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
}

type stubTransport struct{}

func (stubTransport) DialContext(context.Context, string, string) (net.Conn, error) {
	return nil, errors.New("unused")
}

func (stubTransport) HTTP() (string, http.RoundTripper, bool) { return "", nil, false }

// ---------------------------------------------------------------- dialect

func TestQuoteIdentAndLiteralEscape(t *testing.T) {
	if got := quoteIdent(`we"ird`); got != `"we""ird"` {
		t.Fatalf("quoteIdent = %q", got)
	}
	cases := map[string]string{
		"abc":            "'abc'",
		"a'b":            `'a\'b'`,
		`a\b`:            `'a\\b'`,
		`'; DROP ROLE x`: `'\'; DROP ROLE x'`,
	}
	for in, want := range cases {
		if got := quoteLiteral(in); got != want {
			t.Fatalf("quoteLiteral(%q) = %q, want %q", in, got, want)
		}
	}
	if got := qualifiedQuoted("DB", "S", "T"); got != `"DB"."S"."T"` {
		t.Fatalf("qualifiedQuoted = %q", got)
	}
}

func TestSafeIdentifierAcceptsSnowflakeGrammar(t *testing.T) {
	for _, ok := range []string{"PEOPLE", "_x", "a$b", "Mixed_Case9"} {
		if _, err := safeIdentifier(ok); err != nil {
			t.Fatalf("safeIdentifier(%q) rejected: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "1abc", "has space", `has"quote`, "drop;", "a.b"} {
		if _, err := safeIdentifier(bad); !errors.Is(err, plugin.ErrInvalidInput) {
			t.Fatalf("safeIdentifier(%q) must be rejected, got %v", bad, err)
		}
	}
	if got, err := optionalIdentifier("  "); err != nil || got != "" {
		t.Fatalf("optionalIdentifier(blank) = %q, %v", got, err)
	}
}

func TestSearchClauseCoversEveryProjectedExpression(t *testing.T) {
	clause, args := searchClause([]string{`"name"`, `UPPER("state")`}, "prod")
	want := `(UPPER(TO_VARCHAR("name")) LIKE UPPER(?) ESCAPE '\\' OR UPPER(TO_VARCHAR(UPPER("state"))) LIKE UPPER(?) ESCAPE '\\')`
	if clause != want {
		t.Fatalf("search clause:\n got %q\nwant %q", clause, want)
	}
	if len(args) != 2 || args[0] != "%prod%" || args[1] != "%prod%" {
		t.Fatalf("unexpected search args: %#v", args)
	}
	if clause, args := searchClause([]string{`"name"`}, "   "); clause != "" || args != nil {
		t.Fatalf("blank term must not filter: %q %#v", clause, args)
	}
}

func TestEscapeLikeNeutralisesWildcards(t *testing.T) {
	if got := escapeLike(`50%_a\b`); got != `50\%\_a\\b` {
		t.Fatalf("escapeLike = %q", got)
	}
}

func TestSelectSpecPushesSortAndSearchToTheServer(t *testing.T) {
	spec := selectSpec{
		From:    `"DB".INFORMATION_SCHEMA.TABLES`,
		Where:   "TABLE_SCHEMA <> 'INFORMATION_SCHEMA'",
		Columns: []outColumn{{Alias: "name", Expr: "TABLE_NAME"}, {Alias: "bytes", Expr: "BYTES"}},
		Order:   "TABLE_NAME",
	}
	stmt, args, err := spec.build(plugin.PageRequest{Sort: []plugin.SortKey{{Field: "bytes", Desc: true}}, Filter: map[string]string{"q": "orders"}}, 26, 50)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if !strings.Contains(stmt, `ORDER BY BYTES DESC NULLS LAST`) {
		t.Fatalf("sort was not pushed down: %s", stmt)
	}
	if !strings.Contains(stmt, "(TABLE_SCHEMA <> 'INFORMATION_SCHEMA') AND (UPPER(TO_VARCHAR(TABLE_NAME))") {
		t.Fatalf("search was not combined with the base predicate: %s", stmt)
	}
	if !strings.HasSuffix(stmt, "LIMIT 26 OFFSET 50") {
		t.Fatalf("paging was not pushed down: %s", stmt)
	}
	if len(args) != 2 {
		t.Fatalf("only the search term should be bound: %#v", args)
	}
}

func TestSelectSpecRejectsUnknownSortField(t *testing.T) {
	spec := selectSpec{From: "T", Columns: []outColumn{{Alias: "name"}}}
	if _, _, err := spec.build(plugin.PageRequest{Sort: []plugin.SortKey{{Field: "nope"}}}, 10, 0); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("unknown sort field must be rejected, got %v", err)
	}
}

// ---------------------------------------------------------------- cursors

func TestCursorRoundTripIsOpaqueAndTolerant(t *testing.T) {
	cursor := encodeCursor(120)
	if cursor == "120" {
		t.Fatal("cursor must be opaque, not a bare offset")
	}
	if got, err := cursorOffset(cursor); err != nil || got != 120 {
		t.Fatalf("cursorOffset(%q) = %d, %v", cursor, got, err)
	}
	// The grid falls back to numeric offsets when it resumes without a cursor.
	if got, err := cursorOffset("40"); err != nil || got != 40 {
		t.Fatalf("numeric fallback failed: %d %v", got, err)
	}
	if got, err := cursorOffset(""); err != nil || got != 0 {
		t.Fatalf("empty cursor must start at zero: %d %v", got, err)
	}
	for _, bad := range []string{"-1", "not-a-cursor", encodeCursorRaw("v0:5"), encodeCursorRaw("sf1:-3")} {
		if _, err := cursorOffset(bad); !errors.Is(err, plugin.ErrInvalidInput) {
			t.Fatalf("cursor %q must be rejected, got %v", bad, err)
		}
	}
}

func encodeCursorRaw(text string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(text))
}

// ---------------------------------------------------------------- statements

func TestBuildRoleGrantAndRevoke(t *testing.T) {
	stmt, err := buildRoleGrant(false, "REPORTER", "select", "TABLE", "SALES.PUBLIC.ORDERS", true)
	if err != nil {
		t.Fatalf("grant build failed: %v", err)
	}
	want := `GRANT SELECT ON TABLE "SALES"."PUBLIC"."ORDERS" TO ROLE "REPORTER" WITH GRANT OPTION`
	if stmt != want {
		t.Fatalf("grant:\n got %q\nwant %q", stmt, want)
	}
	stmt, err = buildRoleGrant(true, "REPORTER", "MATERIALIZED_VIEW_PRIV", "MATERIALIZED_VIEW", "SALES.PUBLIC.MV", false)
	if err != nil {
		t.Fatalf("revoke build failed: %v", err)
	}
	if !strings.HasPrefix(stmt, `REVOKE MATERIALIZED VIEW PRIV ON MATERIALIZED VIEW "SALES"."PUBLIC"."MV" FROM ROLE "REPORTER"`) {
		t.Fatalf("revoke: %q", stmt)
	}
	stmt, err = buildRoleGrant(false, "ADMIN", "CREATE DATABASE", "ACCOUNT", "", false)
	if err != nil {
		t.Fatalf("account grant failed: %v", err)
	}
	if stmt != `GRANT CREATE DATABASE ON ACCOUNT TO ROLE "ADMIN"` {
		t.Fatalf("account grant: %q", stmt)
	}
}

func TestBuildRoleGrantRejectsUnsafeInput(t *testing.T) {
	cases := []struct {
		name                               string
		role, privilege, grantedOn, object string
	}{
		{"bad role", "bad role", "SELECT", "TABLE", "A.B"},
		{"privilege injection", "R", "SELECT; DROP ROLE X", "TABLE", "A.B"},
		{"object type injection", "R", "SELECT", "TABLE; DROP", "A.B"},
		{"object injection", "R", "SELECT", "TABLE", `A"."B"; DROP TABLE X; --`},
		{"too many parts", "R", "SELECT", "TABLE", "A.B.C.D"},
		{"account with object", "R", "SELECT", "ACCOUNT", "A"},
	}
	for _, tc := range cases {
		if _, err := buildRoleGrant(false, tc.role, tc.privilege, tc.grantedOn, tc.object, false); !errors.Is(err, plugin.ErrInvalidInput) {
			t.Fatalf("%s must be rejected, got %v", tc.name, err)
		}
	}
}

// TestColumnCommentOmitsTheEqualsSign guards the one place Snowflake's COMMENT
// grammar differs: object properties take "COMMENT = 'x'", a column takes
// "COMMENT 'x'", and ALTER TABLE ... ADD COLUMN rejects the "=" form.
func TestColumnCommentOmitsTheEqualsSign(t *testing.T) {
	if got := comment("made by a test"); got != ` COMMENT = 'made by a test'` {
		t.Fatalf("object comment = %q", got)
	}
	if got := columnComment("made by a test"); got != ` COMMENT 'made by a test'` {
		t.Fatalf("column comment = %q", got)
	}
	if columnComment("  ") != "" {
		t.Fatal("a blank column comment must render nothing")
	}
	target := scriptedTarget()
	s := newSession(t, target, nil)
	object := map[string]string{"database": "TESTDB", "schema": "PUBLIC", "name": "PEOPLE"}
	body := mustJSON(t, map[string]any{"name": "EMAIL", "type": "VARCHAR(255)", "nullable": true, "comment": "contact"})
	if _, err := addColumn(plugin.NewRequestContext(context.Background(), plugin.User{}, s, object, nil, body)); err != nil {
		t.Fatalf("add column: %v", err)
	}
	if !target.sawStatement(`ADD COLUMN "EMAIL" VARCHAR(255) COMMENT 'contact'`) {
		t.Fatalf("column comment was not rendered without an equals sign: %v", target.statements())
	}
}

// TestObjectDDLAddressesCaseSensitiveNames guards the GET_DDL argument: an
// unquoted name inside the literal is uppercased by Snowflake, so an object
// created with a quoted lower or mixed case name would never resolve.
func TestObjectDDLAddressesCaseSensitiveNames(t *testing.T) {
	target := scriptedTarget()
	s := newSession(t, target, nil)
	object := map[string]string{"database": "TESTDB", "schema": "PUBLIC", "name": "People"}
	if _, err := tableDDL(plugin.NewRequestContext(context.Background(), plugin.User{}, s, object, nil, nil)); err != nil {
		t.Fatalf("table ddl: %v", err)
	}
	if !target.sawStatement(`GET_DDL('TABLE', '"TESTDB"."PUBLIC"."People"', TRUE)`) {
		t.Fatalf("GET_DDL did not quote the object name: %v", target.statements())
	}
}

func TestBuildWarehouseResize(t *testing.T) {
	stmt, err := buildWarehouseResize("COMPUTE_WH", "x-small", true)
	if err != nil {
		t.Fatalf("resize build failed: %v", err)
	}
	if stmt != `ALTER WAREHOUSE "COMPUTE_WH" SET WAREHOUSE_SIZE = 'XSMALL' WAIT_FOR_COMPLETION = TRUE` {
		t.Fatalf("resize: %q", stmt)
	}
	if _, err := buildWarehouseResize("COMPUTE_WH", "HUGE", false); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("unknown size must be rejected, got %v", err)
	}
}

func TestRowDMLUsesSnowflakePlaceholdersAndQuoting(t *testing.T) {
	stmt, args, err := dialect.Insert(qualifiedQuoted("DB", "PUBLIC", "PEOPLE"), map[string]any{"ID": int64(7), "NAME": "alice"})
	if err != nil {
		t.Fatalf("insert build failed: %v", err)
	}
	if stmt != `INSERT INTO "DB"."PUBLIC"."PEOPLE" ("ID", "NAME") VALUES (?, ?)` {
		t.Fatalf("insert: %q", stmt)
	}
	if len(args) != 2 || args[0] != int64(7) || args[1] != "alice" {
		t.Fatalf("insert args: %#v", args)
	}
	stmt, _, err = dialect.Update(qualifiedQuoted("DB", "PUBLIC", "PEOPLE"), map[string]any{"ID": int64(7)}, map[string]any{"NAME": "bob"})
	if err != nil {
		t.Fatalf("update build failed: %v", err)
	}
	if stmt != `UPDATE "DB"."PUBLIC"."PEOPLE" SET "NAME" = ? WHERE "ID" = ?` {
		t.Fatalf("update: %q", stmt)
	}
	if _, _, err := dialect.Delete(qualifiedQuoted("DB", "PUBLIC", "PEOPLE"), nil); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("keyless delete must be rejected, got %v", err)
	}
}

func TestColumnTypeRendersDeclaredPrecision(t *testing.T) {
	cases := []struct {
		in   row
		want string
	}{
		{row{"data_type": "TEXT", "length": int64(255)}, "TEXT(255)"},
		{row{"data_type": "NUMBER", "precision": int64(38), "scale": int64(2)}, "NUMBER(38,2)"},
		{row{"data_type": "TIMESTAMP_NTZ"}, "TIMESTAMP_NTZ"},
		{row{"data_type": "TEXT"}, "TEXT"},
	}
	for _, tc := range cases {
		if got := columnType(tc.in); got != tc.want {
			t.Fatalf("columnType(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestPrimaryKeyGatesTheEditableGrid(t *testing.T) {
	rows := []row{{"ID": int64(1), "NAME": "alice"}}
	attachRowKeys(rows, nil, nil)
	if _, ok := rows[0]["_key"]; ok {
		t.Fatal("a table without a primary key must stay read-only")
	}
	rows = []row{{"TOKEN": "abc"}}
	attachRowKeys(rows, []string{"TOKEN"}, sqldb.DefaultRedactColumnPatterns())
	if _, ok := rows[0]["_key"]; ok {
		t.Fatal("a sensitive key column must keep the grid read-only")
	}
	rows = []row{{"ID": int64(9), "NAME": "alice"}}
	attachRowKeys(rows, []string{"ID"}, sqldb.DefaultRedactColumnPatterns())
	key, ok := rows[0]["_key"].(map[string]any)
	if !ok || key["ID"] != int64(9) {
		t.Fatalf("expected _key with primary key value, got %#v", rows[0]["_key"])
	}
	if err := sqldb.ValidateRowKey([]string{"ID"}, key); err != nil {
		t.Fatalf("primary-key row key should validate: %v", err)
	}
	if err := sqldb.ValidateRowKey(nil, key); !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("validation without a primary key must be forbidden, got %v", err)
	}
}

func TestRedactRowsMasksConfiguredColumns(t *testing.T) {
	rows := []row{{"ID": int64(1), "ACCESS_TOKEN": "plain", "NAME": "alice"}}
	redactRows(rows, sqldb.DefaultRedactColumnPatterns())
	if rows[0]["ACCESS_TOKEN"] != sqldb.RedactedValue || rows[0]["NAME"] != "alice" {
		t.Fatalf("unexpected redaction: %#v", rows)
	}
}

func TestQuerySafetyStopsBeforeSnowflake(t *testing.T) {
	_, err := executeQueryRequest(context.Background(), &Session{opts: options{ReadOnly: true}}, sqldb.QueryRequest{Query: "insert into t values (1)"})
	if !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("expected read-only forbidden error, got %v", err)
	}
	_, err = executeQueryRequest(context.Background(), &Session{opts: options{RequireConfirm: true}}, sqldb.QueryRequest{Query: "alter warehouse compute_wh suspend"})
	var confirmErr confirmationError
	if !errors.As(err, &confirmErr) {
		t.Fatalf("expected confirmation error, got %v", err)
	}
	if got := queryAuditResult(err); got != plugin.AuditDenied {
		t.Fatalf("confirmation should audit as denied, got %s", got)
	}
	if _, err := executeQueryRequest(context.Background(), &Session{opts: options{}}, sqldb.QueryRequest{Query: "  "}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("empty query must be rejected, got %v", err)
	}
}

func TestStatementClassification(t *testing.T) {
	for _, readOnly := range []string{"select 1", "SHOW WAREHOUSES", "describe table t", "with x as (select 1) select * from x", "list @stage"} {
		if !isReadOnlyStatement(readOnly) {
			t.Fatalf("%q should be read-only", readOnly)
		}
		if isDestructiveStatement(readOnly) {
			t.Fatalf("%q should not be destructive", readOnly)
		}
	}
	for _, destructive := range []string{"merge into t using s on 1=1", "copy into t from @s", "use warehouse x", "grant select on table t to role r", "call p()"} {
		if !isDestructiveStatement(destructive) {
			t.Fatalf("%q should be destructive", destructive)
		}
	}
	if !statementReturnsRows("CALL my_proc()") || statementReturnsRows("INSERT INTO t VALUES (1)") {
		t.Fatal("row-returning classification is wrong")
	}
}

func TestTakeQueryIDIsNonBlocking(t *testing.T) {
	ids := make(chan string, 1)
	if got := takeQueryID(ids); got != "" {
		t.Fatalf("unreported query id must be empty, got %q", got)
	}
	ids <- testQueryID
	close(ids)
	if got := takeQueryID(ids); got != testQueryID {
		t.Fatalf("query id = %q", got)
	}
}

func TestSnowflakeErrMapsServerErrorsToSentinels(t *testing.T) {
	cases := []struct {
		err  error
		want error
	}{
		{&sf.SnowflakeError{Number: 390100, SQLState: "28000", Message: "incorrect username or password"}, plugin.ErrUnauthorized},
		{&sf.SnowflakeError{Number: 3001, SQLState: "42501", Message: "insufficient privileges"}, plugin.ErrForbidden},
		{&sf.SnowflakeError{Number: objectNotFoundCode, Message: "object does not exist"}, plugin.ErrNotFound},
		{&sf.SnowflakeError{Number: 2003, SQLState: "42S02", Message: "table does not exist"}, plugin.ErrNotFound},
		{&sf.SnowflakeError{Number: 1, SQLState: "57014", Message: "statement aborted"}, plugin.ErrUnavailable},
		{errors.New("dial tcp: connection refused"), plugin.ErrUnavailable},
		{sql.ErrNoRows, plugin.ErrNotFound},
	}
	for _, tc := range cases {
		if got := snowflakeErr(tc.err); !errors.Is(got, tc.want) {
			t.Fatalf("snowflakeErr(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
	if snowflakeErr(nil) != nil {
		t.Fatal("a nil error must stay nil")
	}
}

func TestEnsureWritableBlocksReadOnlySessions(t *testing.T) {
	if err := ensureWritable(&Session{opts: options{ReadOnly: true}}); !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("read-only mode must block writes, got %v", err)
	}
	if err := ensureWritable(&Session{opts: options{}}); err != nil {
		t.Fatalf("writable session rejected: %v", err)
	}
}

func TestQueryIDValidation(t *testing.T) {
	if got, err := queryID("  " + testQueryID + " "); err != nil || got != testQueryID {
		t.Fatalf("queryID = %q, %v", got, err)
	}
	if _, err := queryID("01a'; DROP TABLE x"); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("non-UUID query id must be rejected, got %v", err)
	}
}

// TestMetricsFrameSurfacesSourceFailures keeps a live panel honest: a role that
// cannot read the metering views must see the error rather than an empty frame
// that reads as "no activity".
func TestMetricsFrameSurfacesSourceFailures(t *testing.T) {
	denied := fmt.Errorf("%w: insufficient privileges", plugin.ErrForbidden)
	if err := emptyFrameErr(map[string]any{}, denied); !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("an empty frame must report the source failure, got %v", err)
	}
	if err := emptyFrameErr(map[string]any{"queries": 1.0}, denied); err != nil {
		t.Fatalf("a partial frame must still be sent, got %v", err)
	}
	if err := emptyFrameErr(map[string]any{}, nil); err != nil {
		t.Fatalf("an empty frame without a failure is not an error, got %v", err)
	}
}

func TestTaskRunSeverity(t *testing.T) {
	for state, want := range map[string]string{"SUCCEEDED": "success", "FAILED": "danger", "CANCELLED": "danger", "SKIPPED": "warn", "SCHEDULED": "info"} {
		if got := taskRunSeverity(state); got != want {
			t.Fatalf("taskRunSeverity(%q) = %q, want %q", state, got, want)
		}
	}
}

// ---------------------------------------------------------------- pagination

func TestTableRowsPagesWithoutPhantomPages(t *testing.T) {
	target := scriptedTarget()
	s := newSession(t, target, nil)
	params := map[string]string{"database": "TESTDB", "schema": "PUBLIC", "name": "PEOPLE"}

	seen := 0
	cursor := ""
	for page := 0; page < 5; page++ {
		query := url.Values{"limit": {"2"}}
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		out, err := tableRows(plugin.NewRequestContext(context.Background(), plugin.User{}, s, params, query, nil))
		if err != nil {
			t.Fatalf("table rows page %d: %v", page, err)
		}
		p := out.(plugin.Page[row])
		if len(p.Items) == 0 {
			t.Fatalf("page %d is a phantom empty page", page)
		}
		if p.Total == nil || *p.Total != 5 {
			t.Fatalf("unfiltered total should be authoritative, got %v", p.Total)
		}
		seen += len(p.Items)
		cursor = p.NextCursor
		if cursor == "" {
			break
		}
	}
	if seen != 5 {
		t.Fatalf("paging returned %d of 5 rows", seen)
	}
	if cursor != "" {
		t.Fatal("the final page must not advertise another cursor")
	}
}

func TestTableRowsFiltersAndSortsAcrossTheDataset(t *testing.T) {
	target := scriptedTarget()
	s := newSession(t, target, nil)
	params := map[string]string{"database": "TESTDB", "schema": "PUBLIC", "name": "PEOPLE"}
	query := url.Values{"limit": {"2"}, "filter": {"ali"}, "sort": {"-NAME"}}
	out, err := tableRows(plugin.NewRequestContext(context.Background(), plugin.User{}, s, params, query, nil))
	if err != nil {
		t.Fatalf("table rows: %v", err)
	}
	if page := out.(plugin.Page[row]); page.Total != nil {
		t.Fatalf("a filtered page must not claim an authoritative total: %v", *page.Total)
	}
	if !target.sawStatement(`SELECT * FROM "TESTDB"."PUBLIC"."PEOPLE"`, `LIKE UPPER(?)`, `ORDER BY "NAME" DESC`, "LIMIT 3 OFFSET 0") {
		t.Fatalf("filter and sort were not pushed to Snowflake: %v", target.statements())
	}
}

// TestTableRowsOrderIsDeterministic guards the paging invariant: Snowflake gives
// no implicit row order, so an offset page without a total order can repeat or
// drop rows between pages.
func TestTableRowsOrderIsDeterministic(t *testing.T) {
	target := scriptedTarget()
	s := newSession(t, target, nil)
	params := map[string]string{"database": "TESTDB", "schema": "PUBLIC", "name": "PEOPLE"}
	if _, err := tableRows(plugin.NewRequestContext(context.Background(), plugin.User{}, s, params, nil, nil)); err != nil {
		t.Fatalf("table rows: %v", err)
	}
	if !target.sawStatement(`SELECT * FROM "TESTDB"."PUBLIC"."PEOPLE" ORDER BY "ID" LIMIT`) {
		t.Fatalf("unsorted paging must fall back to the primary key: %v", target.statements())
	}
	sorted := scriptedTarget()
	s = newSession(t, sorted, nil)
	if _, err := tableRows(plugin.NewRequestContext(context.Background(), plugin.User{}, s, params, url.Values{"sort": {"NAME"}}, nil)); err != nil {
		t.Fatalf("sorted table rows: %v", err)
	}
	if !sorted.sawStatement(`ORDER BY "NAME" ASC NULLS LAST, "ID"`) {
		t.Fatalf("a requested sort must keep the primary key as a tiebreaker: %v", sorted.statements())
	}
}

func TestSelectSpecKeepsNaturalOrderAsTiebreaker(t *testing.T) {
	spec := selectSpec{
		From:    "T",
		Columns: []outColumn{{Alias: "name", Expr: "TABLE_NAME"}, {Alias: "bytes", Expr: "BYTES"}},
		Order:   "TABLE_SCHEMA, TABLE_NAME",
	}
	stmt, _, err := spec.build(plugin.PageRequest{Sort: []plugin.SortKey{{Field: "bytes"}}}, 10, 0)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if !strings.Contains(stmt, "ORDER BY BYTES ASC NULLS LAST, TABLE_SCHEMA, TABLE_NAME") {
		t.Fatalf("catalog sort lost its tiebreaker: %s", stmt)
	}
}

func TestTableRowsRejectsSortOnAnUnknownColumn(t *testing.T) {
	s := newSession(t, scriptedTarget(), nil)
	params := map[string]string{"database": "TESTDB", "schema": "PUBLIC", "name": "PEOPLE"}
	query := url.Values{"sort": {"NOT_A_COLUMN"}}
	if _, err := tableRows(plugin.NewRequestContext(context.Background(), plugin.User{}, s, params, query, nil)); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("unknown sort column must be rejected, got %v", err)
	}
}

func TestTableRowsRedactsSensitiveColumns(t *testing.T) {
	s := newSession(t, scriptedTarget(), nil)
	params := map[string]string{"database": "TESTDB", "schema": "PUBLIC", "name": "PEOPLE"}
	out, err := tableRows(plugin.NewRequestContext(context.Background(), plugin.User{}, s, params, nil, nil))
	if err != nil {
		t.Fatalf("table rows: %v", err)
	}
	page := out.(plugin.Page[row])
	if page.Items[0]["ACCESS_TOKEN"] != sqldb.RedactedValue {
		t.Fatalf("sensitive column was not redacted: %#v", page.Items[0])
	}
	key, ok := page.Items[0]["_key"].(map[string]any)
	if !ok || key["ID"] == nil {
		t.Fatalf("declared primary key should drive the editable grid: %#v", page.Items[0]["_key"])
	}
}

// TestTableRowsSearchSkipsRedactedColumns keeps the grid filter honest: a
// redacted cell is only ever shown masked, so matching rows on its real value
// would both leak it and look like a broken filter.
func TestTableRowsSearchSkipsRedactedColumns(t *testing.T) {
	target := scriptedTarget()
	s := newSession(t, target, nil)
	params := map[string]string{"database": "TESTDB", "schema": "PUBLIC", "name": "PEOPLE"}
	query := url.Values{"filter": {"tok"}}
	if _, err := tableRows(plugin.NewRequestContext(context.Background(), plugin.User{}, s, params, query, nil)); err != nil {
		t.Fatalf("table rows: %v", err)
	}
	if !target.sawStatement(`TO_VARCHAR("NAME")`) {
		t.Fatalf("visible columns must stay searchable: %v", target.statements())
	}
	for _, statement := range target.statements() {
		if strings.Contains(statement, `TO_VARCHAR("ACCESS_TOKEN")`) {
			t.Fatalf("a redacted column must not be searched: %s", statement)
		}
	}
}

func TestRowMutationsRefuseRedactedColumns(t *testing.T) {
	s := newSession(t, scriptedTarget(), nil)
	object := map[string]string{"database": "TESTDB", "schema": "PUBLIC", "name": "PEOPLE"}
	body := mustJSON(t, map[string]any{"values": map[string]any{"NAME": "carol", "ACCESS_TOKEN": "••••"}})
	if _, err := insertRow(plugin.NewRequestContext(context.Background(), plugin.User{}, s, object, nil, body)); !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("writing a redacted column must be forbidden, got %v", err)
	}
	update := mustJSON(t, map[string]any{"key": map[string]any{"ID": 1}, "values": map[string]any{"ACCESS_TOKEN": "••••"}})
	if _, err := updateRow(plugin.NewRequestContext(context.Background(), plugin.User{}, s, object, nil, update)); !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("updating a redacted column must be forbidden, got %v", err)
	}
}

func TestCatalogListPagesWithAnOpaqueCursor(t *testing.T) {
	target := scriptedTarget()
	s := newSession(t, target, nil)
	out, err := listSchemas(plugin.NewRequestContext(context.Background(), plugin.User{}, s, nil, url.Values{"limit": {"1"}}, nil))
	if err != nil {
		t.Fatalf("list schemas: %v", err)
	}
	page := out.(plugin.Page[row])
	if len(page.Items) != 1 || page.NextCursor == "" {
		t.Fatalf("first page should carry a continue cursor: %+v", page)
	}
	if page.Total != nil {
		t.Fatal("a catalog listing must not report a total it did not compute")
	}
	next, err := listSchemas(plugin.NewRequestContext(context.Background(), plugin.User{}, s, nil, url.Values{"limit": {"1"}, "cursor": {page.NextCursor}}, nil))
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	second := next.(plugin.Page[row])
	if len(second.Items) != 1 || second.Items[0]["name"] == page.Items[0]["name"] {
		t.Fatalf("cursor did not advance: %+v", second)
	}
	if second.NextCursor != "" {
		t.Fatal("the last page must not advertise another cursor")
	}
}

func TestShowBackedListPagesThroughResultScan(t *testing.T) {
	target := scriptedTarget()
	s := newSession(t, target, nil)
	out, err := listWarehouses(plugin.NewRequestContext(context.Background(), plugin.User{}, s, nil, url.Values{"limit": {"1"}}, nil))
	if err != nil {
		t.Fatalf("list warehouses: %v", err)
	}
	page := out.(plugin.Page[row])
	if len(page.Items) != 1 || page.NextCursor == "" {
		t.Fatalf("SHOW output should page: %+v", page)
	}
	if !target.sawStatement("SHOW WAREHOUSES") || !target.sawStatement(resultScanFrom, "LIMIT 2 OFFSET 0") {
		t.Fatalf("SHOW output was not paged through RESULT_SCAN: %v", target.statements())
	}
	if ref, ok := page.Items[0]["ref"].(plugin.ResourceIdentity); !ok || ref.Kind != "warehouse" {
		t.Fatalf("warehouse rows must carry a resource ref: %#v", page.Items[0])
	}
}

func TestListsStayScopedToTheConnectionDatabase(t *testing.T) {
	target := scriptedTarget()
	s := newSession(t, target, nil)
	if _, err := listStages(plugin.NewRequestContext(context.Background(), plugin.User{}, s, nil, nil, nil)); err != nil {
		t.Fatalf("list stages: %v", err)
	}
	if !target.sawStatement(`FROM "TESTDB".INFORMATION_SCHEMA.STAGES`) {
		t.Fatalf("listing escaped the connection database: %v", target.statements())
	}
	if _, err := listTasks(plugin.NewRequestContext(context.Background(), plugin.User{}, s, map[string]string{"database": "ANALYTICS", "schema": "PUBLIC"}, nil, nil)); err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if !target.sawStatement(`SHOW TASKS IN SCHEMA "ANALYTICS"."PUBLIC"`) {
		t.Fatalf("SHOW was not scoped to the requested schema: %v", target.statements())
	}
}

func TestTreeRelationsWalksTablesAndViewsWithOneCursor(t *testing.T) {
	s := newSession(t, scriptedTarget(), nil)
	params := map[string]string{"database": "TESTDB", "schema": "PUBLIC"}
	out, err := treeRelations(plugin.NewRequestContext(context.Background(), plugin.User{}, s, params, nil, nil))
	if err != nil {
		t.Fatalf("relations tree: %v", err)
	}
	page := out.(plugin.Page[plugin.TreeNode])
	if len(page.Items) != 2 {
		t.Fatalf("expected both relations in one page: %+v", page.Items)
	}
	kinds := map[string]bool{}
	for _, node := range page.Items {
		if !node.Leaf || node.Ref == nil {
			t.Fatalf("relation nodes must be navigable leaves: %+v", node)
		}
		kinds[node.Ref.Kind] = true
	}
	if !kinds["table"] || !kinds["view"] {
		t.Fatalf("relation kinds were not derived from TABLE_TYPE: %v", kinds)
	}
}

// ---------------------------------------------------------------- route coverage

func TestEveryRouteIsExercised(t *testing.T) {
	target := scriptedTarget()
	s := newSession(t, target, nil)
	h := contribtest.NewHarness(t, routes())
	ctx := context.Background()

	object := map[string]string{"database": "TESTDB", "schema": "PUBLIC", "name": "PEOPLE"}
	schema := map[string]string{"database": "TESTDB", "schema": "PUBLIC"}
	database := map[string]string{"database": "TESTDB"}

	call := func(id string, params map[string]string, body []byte) any {
		t.Helper()
		return h.Call(ctx, id, plugin.Session(s), params, nil, body)
	}

	call("snowflake.databases.tree", nil, nil)
	call("snowflake.schemas.tree", database, nil)
	call("snowflake.relations.tree", schema, nil)
	call("snowflake.account.overview", nil, nil)
	call("snowflake.databases.list", nil, nil)
	call("snowflake.database.overview", database, nil)
	call("snowflake.schemas.list", database, nil)
	call("snowflake.schema.overview", schema, nil)
	call("snowflake.tables.list", schema, nil)
	call("snowflake.views.list", schema, nil)
	call("snowflake.table.overview", object, nil)
	call("snowflake.table.columns", object, nil)
	call("snowflake.table.ddl", object, nil)
	call("snowflake.view.ddl", object, nil)
	call("snowflake.table.rows", object, nil)
	call("snowflake.view.rows", object, nil)
	call("snowflake.warehouses.list", nil, nil)
	call("snowflake.warehouse.overview", map[string]string{"warehouse": "COMPUTE_WH"}, nil)
	call("snowflake.roles.list", nil, nil)
	call("snowflake.role.overview", map[string]string{"role": "REPORTER"}, nil)
	call("snowflake.role.grants", map[string]string{"role": "REPORTER"}, nil)
	call("snowflake.users.list", nil, nil)
	call("snowflake.user.overview", map[string]string{"user": "SVC"}, nil)
	call("snowflake.user.grants", map[string]string{"user": "SVC"}, nil)
	call("snowflake.stages.list", database, nil)
	call("snowflake.stage.overview", map[string]string{"database": "TESTDB", "schema": "PUBLIC", "name": "LANDING"}, nil)
	call("snowflake.stage.files", map[string]string{"database": "TESTDB", "schema": "PUBLIC", "name": "LANDING"}, nil)
	call("snowflake.formats.list", database, nil)
	call("snowflake.format.overview", map[string]string{"database": "TESTDB", "schema": "PUBLIC", "name": "CSV_FMT"}, nil)
	call("snowflake.pipes.list", database, nil)
	call("snowflake.pipe.overview", map[string]string{"database": "TESTDB", "schema": "PUBLIC", "name": "ORDERS_PIPE"}, nil)
	call("snowflake.tasks.list", database, nil)
	call("snowflake.task.overview", map[string]string{"database": "TESTDB", "schema": "PUBLIC", "name": "NIGHTLY"}, nil)
	call("snowflake.task.history", map[string]string{"database": "TESTDB", "schema": "PUBLIC", "name": "NIGHTLY"}, nil)
	call("snowflake.streams.list", database, nil)
	call("snowflake.stream.overview", map[string]string{"database": "TESTDB", "schema": "PUBLIC", "name": "ORDERS_STREAM"}, nil)
	call("snowflake.queries.list", nil, nil)
	call("snowflake.query.overview", map[string]string{"id": testQueryID}, nil)
	call("snowflake.completion", nil, nil)

	call("snowflake.database.create", nil, mustJSON(t, map[string]any{"name": "NEWDB", "comment": "made by a test", "if_not_exists": true}))
	call("snowflake.database.drop", map[string]string{"database": "NEWDB"}, nil)
	call("snowflake.schema.create", database, mustJSON(t, map[string]any{"name": "STAGING", "managed_access": true}))
	call("snowflake.schema.drop", map[string]string{"database": "TESTDB", "schema": "STAGING"}, nil)
	call("snowflake.table.create", schema, mustJSON(t, map[string]any{
		"name":       "EVENTS",
		"columns":    []map[string]any{{"name": "ID", "type": "NUMBER(38,0)", "primary": true}, {"name": "PAYLOAD", "type": "VARIANT", "nullable": true}},
		"cluster_by": "ID",
	}))
	call("snowflake.table.rename", object, mustJSON(t, map[string]any{"name": "PEOPLE_OLD"}))
	call("snowflake.table.truncate", object, nil)
	call("snowflake.table.drop", object, nil)
	call("snowflake.view.drop", map[string]string{"database": "TESTDB", "schema": "PUBLIC", "name": "ORDERS_VIEW"}, nil)
	call("snowflake.column.add", object, mustJSON(t, map[string]any{"name": "EMAIL", "type": "VARCHAR(255)", "nullable": true}))
	call("snowflake.column.drop", map[string]string{"database": "TESTDB", "schema": "PUBLIC", "name": "PEOPLE", "column": "EMAIL"}, nil)
	call("snowflake.table.row.insert", object, mustJSON(t, map[string]any{"values": map[string]any{"NAME": "carol"}}))
	call("snowflake.table.row.update", object, mustJSON(t, map[string]any{"key": map[string]any{"ID": 1}, "values": map[string]any{"NAME": "carol"}}))
	call("snowflake.table.row.delete", object, mustJSON(t, map[string]any{"key": map[string]any{"ID": 1}}))
	call("snowflake.warehouse.resume", map[string]string{"warehouse": "COMPUTE_WH"}, nil)
	call("snowflake.warehouse.suspend", map[string]string{"warehouse": "COMPUTE_WH"}, nil)
	call("snowflake.warehouse.resize", map[string]string{"warehouse": "COMPUTE_WH"}, mustJSON(t, map[string]any{"size": "SMALL"}))
	call("snowflake.task.resume", map[string]string{"database": "TESTDB", "schema": "PUBLIC", "name": "NIGHTLY"}, nil)
	call("snowflake.task.suspend", map[string]string{"database": "TESTDB", "schema": "PUBLIC", "name": "NIGHTLY"}, nil)
	call("snowflake.task.execute", map[string]string{"database": "TESTDB", "schema": "PUBLIC", "name": "NIGHTLY"}, nil)
	call("snowflake.role.create", nil, mustJSON(t, map[string]any{"name": "REPORTER"}))
	call("snowflake.role.drop", map[string]string{"role": "REPORTER"}, nil)
	call("snowflake.role.grant", map[string]string{"role": "REPORTER"}, mustJSON(t, map[string]any{"privilege": "USAGE", "granted_on": "DATABASE", "object": "TESTDB"}))
	call("snowflake.role.revoke", map[string]string{"role": "REPORTER", "privilege": "USAGE", "granted_on": "DATABASE", "object": "TESTDB"}, nil)
	call("snowflake.user.role.grant", map[string]string{"user": "SVC"}, mustJSON(t, map[string]any{"role": "REPORTER"}))
	call("snowflake.user.role.revoke", map[string]string{"user": "SVC", "role": "REPORTER"}, nil)
	call("snowflake.query.abort", map[string]string{"id": testQueryID}, nil)
	call("snowflake.query.cancel", nil, nil)

	frames := h.Stream(ctx, "snowflake.query", s, nil, nil, []byte(`{"query":"SELECT 1;"}`))
	var result queryResult
	if err := json.Unmarshal(firstFrame(t, frames), &result); err != nil {
		t.Fatalf("decode query frame: %v", err)
	}
	if len(result.Columns) != 1 || result.RowCount != 1 || len(result.Statements) != 1 {
		t.Fatalf("unexpected query result: %+v", result)
	}

	metricsCtx, cancel := context.WithTimeout(ctx, 60*time.Millisecond)
	defer cancel()
	accountFrame := firstFrame(t, h.Stream(metricsCtx, "snowflake.account.metrics", s, nil, nil, nil))
	assertMetric(t, accountFrame, "queries", "credits", "warehouses", "cachePct")
	warehouseCtx, cancelWarehouse := context.WithTimeout(ctx, 60*time.Millisecond)
	defer cancelWarehouse()
	warehouseFrame := firstFrame(t, h.Stream(warehouseCtx, "snowflake.warehouse.metrics", s, map[string]string{"warehouse": "COMPUTE_WH"}, nil, nil))
	assertMetric(t, warehouseFrame, "running", "clusterPct", "avgRunning", "credits")

	h.AssertAllCovered()
}

func TestWriteRoutesRefuseReadOnlySessions(t *testing.T) {
	s := newSession(t, scriptedTarget(), func(o *options) { o.ReadOnly = true })
	object := map[string]string{"database": "TESTDB", "schema": "PUBLIC", "name": "PEOPLE"}
	cases := map[string]func(*plugin.RequestContext) (any, error){
		"drop table":     dropTable,
		"truncate table": truncateTable,
		"drop column":    dropColumn,
		"abort query":    abortQuery,
	}
	for name, handle := range cases {
		params := object
		if name == "drop column" {
			params = map[string]string{"database": "TESTDB", "schema": "PUBLIC", "name": "PEOPLE", "column": "NAME"}
		}
		if name == "abort query" {
			params = map[string]string{"id": testQueryID}
		}
		if _, err := handle(plugin.NewRequestContext(context.Background(), plugin.User{}, s, params, nil, nil)); !errors.Is(err, plugin.ErrForbidden) {
			t.Fatalf("%s must be blocked in read-only mode, got %v", name, err)
		}
	}
}

func TestRowMutationsRequireTheDeclaredPrimaryKey(t *testing.T) {
	s := newSession(t, scriptedTarget(), nil)
	object := map[string]string{"database": "TESTDB", "schema": "PUBLIC", "name": "PEOPLE"}
	body := mustJSON(t, map[string]any{"key": map[string]any{"NAME": "alice"}, "values": map[string]any{"NAME": "bob"}})
	if _, err := updateRow(plugin.NewRequestContext(context.Background(), plugin.User{}, s, object, nil, body)); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("a key that is not the primary key must be rejected, got %v", err)
	}
	if _, err := deleteRow(plugin.NewRequestContext(context.Background(), plugin.User{}, s, object, nil, mustJSON(t, map[string]any{}))); err == nil {
		t.Fatal("a keyless delete must be rejected")
	}
}

// ---------------------------------------------------------------- fixtures

func firstFrame(t *testing.T, out []byte) []byte {
	t.Helper()
	line, _, found := strings.Cut(strings.TrimSpace(string(out)), "\n")
	if !found && line == "" {
		t.Fatalf("stream produced no frame: %q", out)
	}
	return []byte(line)
}

func assertMetric(t *testing.T, frame []byte, keys ...string) {
	t.Helper()
	var decoded map[string]any
	if err := json.Unmarshal(frame, &decoded); err != nil {
		t.Fatalf("decode metrics frame: %v", err)
	}
	for _, key := range keys {
		if _, ok := decoded[key]; !ok {
			t.Fatalf("metrics frame is missing %q: %s", key, frame)
		}
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

// scriptedTarget wires one canned response per statement the handlers issue.
// Rules are matched in order, so the more specific projections are registered
// before the broader table sources they read from.
func scriptedTarget() *fakeTarget {
	now := "2026-07-01T00:00:00Z"
	return newTarget().
		on(`AS "schemas"`,
			[]string{"name", "owner", "is_transient", "retention_days", "created", "last_altered", "comment", "schemas"},
			[]driver.Value{"TESTDB", "SYSADMIN", false, int64(1), now, now, "primary", int64(2)}).
		on(`AS "columns"`,
			[]string{"name", "schema", "database", "kind", "rows", "bytes", "clustering_key", "is_transient", "retention_days", "created", "last_altered", "comment", "columns"},
			[]driver.Value{"PEOPLE", "PUBLIC", "TESTDB", "base table", int64(5), int64(4096), nil, false, int64(1), now, now, "people", int64(3)}).
		on(`AS "data_type"`,
			[]string{"name", "data_type", "nullable", "default", "position", "comment", "length", "precision", "scale"},
			[]driver.Value{"ID", "NUMBER", false, nil, int64(1), "", nil, int64(38), int64(0)},
			[]driver.Value{"NAME", "TEXT", true, nil, int64(2), "", int64(255), nil, nil},
			[]driver.Value{"ACCESS_TOKEN", "TEXT", true, nil, int64(3), "", int64(64), nil, nil}).
		on(`AS "column"`,
			[]string{"schema", "table", "kind", "column"},
			[]driver.Value{"PUBLIC", "PEOPLE", "BASE TABLE", "ID"},
			[]driver.Value{"PUBLIC", "ORDERS_VIEW", "VIEW", "TOTAL"}).
		on("INFORMATION_SCHEMA.DATABASES",
			[]string{"name", "owner", "is_transient", "retention_days", "created", "last_altered", "comment"},
			[]driver.Value{"TESTDB", "SYSADMIN", false, int64(1), now, now, "primary"},
			[]driver.Value{"ANALYTICS", "SYSADMIN", true, int64(0), now, now, ""}).
		on("INFORMATION_SCHEMA.SCHEMATA",
			[]string{"name", "database", "owner", "is_transient", "managed_access", "retention_days", "created", "last_altered", "comment"},
			[]driver.Value{"PUBLIC", "TESTDB", "SYSADMIN", false, false, int64(1), now, now, ""},
			[]driver.Value{"STAGING", "TESTDB", "SYSADMIN", true, true, int64(0), now, now, ""}).
		on("INFORMATION_SCHEMA.TABLES",
			[]string{"name", "schema", "database", "kind", "rows", "bytes", "clustering_key", "is_transient", "retention_days", "created", "last_altered", "comment"},
			[]driver.Value{"PEOPLE", "PUBLIC", "TESTDB", "base table", int64(5), int64(4096), nil, false, int64(1), now, now, "people"},
			[]driver.Value{"ORDERS_VIEW", "PUBLIC", "TESTDB", "view", nil, nil, nil, false, int64(1), now, now, ""}).
		on("INFORMATION_SCHEMA.STAGES",
			[]string{"name", "schema", "database", "kind", "url", "region", "owner", "created", "last_altered", "comment"},
			[]driver.Value{"LANDING", "PUBLIC", "TESTDB", "EXTERNAL", "s3://bucket/landing", "us-east-1", "SYSADMIN", now, now, ""}).
		on("INFORMATION_SCHEMA.FILE_FORMATS",
			[]string{"name", "schema", "database", "kind", "owner", "created", "last_altered", "comment"},
			[]driver.Value{"CSV_FMT", "PUBLIC", "TESTDB", "CSV", "SYSADMIN", now, now, ""}).
		on("INFORMATION_SCHEMA.PIPES",
			[]string{"name", "schema", "database", "definition", "auto_ingest", "notification_channel", "invalid_reason", "owner", "created", "last_altered", "comment"},
			[]driver.Value{"ORDERS_PIPE", "PUBLIC", "TESTDB", "COPY INTO orders FROM @landing", true, "arn:aws:sqs:queue", nil, "SYSADMIN", now, now, ""}).
		on("TASK_HISTORY(",
			[]string{"name", "schema", "database", "state", "scheduled_time", "completed_time", "query_id", "error_code", "error_message"},
			[]driver.Value{"NIGHTLY", "PUBLIC", "TESTDB", "SUCCEEDED", now, now, testQueryID, nil, nil}).
		on("COUNT_IF(EXECUTION_STATUS",
			[]string{"queries", "failed", "running", "bytes_scanned", "rows_produced", "avg_elapsed_ms", "cloud_credits", "cache_pct"},
			[]driver.Value{int64(42), int64(1), int64(2), int64(1048576), int64(900), 132.5, 0.004, 61.25}).
		on("QUERY_HISTORY(",
			[]string{"query_id", "query_text", "query_type", "status", "user", "role", "warehouse", "warehouse_size", "database", "schema", "query_tag", "start_time", "end_time", "elapsed_ms", "compilation_ms", "execution_ms", "queued_ms", "bytes_scanned", "bytes_written", "rows_produced", "cache_pct", "credits", "error_code", "error_message"},
			[]driver.Value{testQueryID, "SELECT 1", "SELECT", "SUCCESS", "SVC", "SYSADMIN", "COMPUTE_WH", "X-Small", "TESTDB", "PUBLIC", nil, now, now, int64(120), int64(20), int64(90), int64(10), int64(2048), int64(0), int64(1), 12.5, 0.0001, nil, nil}).
		on("WAREHOUSE_METERING_HISTORY",
			[]string{"credits", "compute_credits"},
			[]driver.Value{1.75, 1.5}).
		on("WAREHOUSE_LOAD_HISTORY",
			[]string{"avg_running", "avg_queued_load", "avg_queued_provisioning", "avg_blocked"},
			[]driver.Value{1.25, 0.0, 0.0, 0.0}).
		on(`COUNT_IF(UPPER("state")`, []string{"started"}, []driver.Value{int64(1)}).
		on("GET_DDL('TABLE'", []string{"definition"}, []driver.Value{"create or replace TABLE PEOPLE (ID NUMBER(38,0));"}).
		on("GET_DDL('VIEW'", []string{"definition"}, []driver.Value{"create or replace view ORDERS_VIEW as select 1;"}).
		on("CURRENT_ACCOUNT()",
			[]string{"account", "region", "version", "user", "role", "warehouse", "database", "schema", "session_id"},
			[]driver.Value{"MYACCOUNT", "AWS_US_EAST_1", "9.1.0", "SVC", "SYSADMIN", "COMPUTE_WH", "TESTDB", "PUBLIC", "112233"}).
		on("SYSTEM$CANCEL_QUERY", []string{"message"}, []driver.Value{"query aborted"}).
		on("SHOW PRIMARY KEYS IN TABLE",
			[]string{"column_name", "key_sequence"},
			[]driver.Value{"ID", int64(1)}).
		on(`AS "auto_suspend"`,
			[]string{"name", "state", "size", "kind", "running", "queued", "clusters", "started_clusters", "min_clusters", "max_clusters", "auto_suspend", "auto_resume", "resource_monitor", "scaling_policy", "owner", "comment", "created", "resumed"},
			[]driver.Value{"COMPUTE_WH", "STARTED", "X-Small", "STANDARD", int64(2), int64(0), int64(1), int64(1), int64(1), int64(4), int64(600), true, "null", "STANDARD", "SYSADMIN", "", now, now},
			[]driver.Value{"LOAD_WH", "SUSPENDED", "Large", "STANDARD", int64(0), int64(0), int64(0), int64(0), int64(1), int64(2), int64(60), true, "null", "STANDARD", "SYSADMIN", "", now, nil}).
		on(`AS "assigned_to_users"`,
			[]string{"name", "owner", "assigned_to_users", "granted_to_roles", "granted_roles", "created", "comment"},
			[]driver.Value{"REPORTER", "USERADMIN", int64(3), int64(1), int64(2), now, ""}).
		on(`AS "granted_on"`,
			[]string{"privilege", "granted_on", "name", "grant_option", "granted_by", "created"},
			[]driver.Value{"USAGE", "DATABASE", "TESTDB", false, "SYSADMIN", now}).
		on(`"role" AS "role"`,
			[]string{"role", "granted_by", "created"},
			[]driver.Value{"REPORTER", "SECURITYADMIN", now}).
		on(`AS "login_name"`,
			[]string{"name", "login_name", "display_name", "email", "disabled", "must_change_password", "has_rsa_public_key", "default_role", "default_warehouse", "default_namespace", "last_success_login", "created", "owner", "comment"},
			[]driver.Value{"SVC", "svc@example.com", "Service", "svc@example.com", false, false, true, "SYSADMIN", "COMPUTE_WH", "TESTDB.PUBLIC", now, now, "USERADMIN", ""}).
		on(`AS "md5"`,
			[]string{"name", "size", "md5", "last_modified"},
			[]driver.Value{"landing/orders.csv", int64(2048), "d41d8cd98f00b204e9800998ecf8427e", now}).
		on(`AS "predecessors"`,
			[]string{"name", "schema", "database", "state", "warehouse", "schedule", "predecessors", "condition", "definition", "last_committed", "owner", "comment"},
			[]driver.Value{"NIGHTLY", "PUBLIC", "TESTDB", "STARTED", "COMPUTE_WH", "USING CRON 0 2 * * * UTC", "[]", nil, "INSERT INTO t SELECT 1", now, "SYSADMIN", ""}).
		on(`AS "source_type"`,
			[]string{"name", "schema", "database", "table_name", "source_type", "mode", "stale", "stale_after", "owner", "created", "comment"},
			[]driver.Value{"ORDERS_STREAM", "PUBLIC", "TESTDB", "TESTDB.PUBLIC.ORDERS", "Table", "DEFAULT", false, now, "SYSADMIN", now, ""}).
		on("SHOW WAREHOUSES", []string{"name"}, []driver.Value{"COMPUTE_WH"}).
		on("SHOW ROLES", []string{"name"}, []driver.Value{"REPORTER"}).
		on("SHOW USERS", []string{"name"}, []driver.Value{"SVC"}).
		on("SHOW GRANTS TO ROLE", []string{"name"}, []driver.Value{"TESTDB"}).
		on("SHOW GRANTS TO USER", []string{"role"}, []driver.Value{"REPORTER"}).
		on("SHOW TASKS", []string{"name"}, []driver.Value{"NIGHTLY"}).
		on("SHOW STREAMS", []string{"name"}, []driver.Value{"ORDERS_STREAM"}).
		on("LIST @", []string{"name"}, []driver.Value{"landing/orders.csv"}).
		on("SELECT COUNT(*) FROM ", []string{"count"}, []driver.Value{int64(5)}).
		on(`SELECT * FROM "TESTDB"."PUBLIC".`,
			[]string{"ID", "NAME", "ACCESS_TOKEN"},
			[]driver.Value{int64(1), "alice", "tok-1"},
			[]driver.Value{int64(2), "bob", "tok-2"},
			[]driver.Value{int64(3), "carol", "tok-3"},
			[]driver.Value{int64(4), "dave", "tok-4"},
			[]driver.Value{int64(5), "erin", "tok-5"}).
		on("SELECT 1", []string{"1"}, []driver.Value{int64(1)})
}
