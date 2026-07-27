package trino

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestManifestValidatesAndStaysDirectOnly(t *testing.T) {
	p := New()
	m := p.Manifest()
	plugintest.ValidatePlugin(t, p)
	if m.Agent != nil {
		t.Fatal("Trino must not declare agent transport")
	}
	if len(m.SupportedTransports) != 1 || m.SupportedTransports[0] != plugin.TransportDirect {
		t.Fatalf("unexpected transports: %+v", m.SupportedTransports)
	}
	if m.Layout != plugin.LayoutSidebarTree {
		t.Fatalf("unexpected layout: %q", m.Layout)
	}
	for _, kind := range []plugin.CredentialKind{plugin.CredentialKindDBPassword, plugin.CredentialKindBearerToken, plugin.CredentialKindTLSClientCert} {
		if !plugintest.CredentialKindSupported(m.Config, kind) {
			t.Fatalf("credential kind %q should be selectable for Trino", kind)
		}
	}
}

func TestManifestDeclaresEveryResourceAndStream(t *testing.T) {
	m := New().Manifest()
	kinds := map[string]bool{}
	for _, r := range m.Resources {
		kinds[r.Kind] = true
	}
	for _, want := range []string{"cluster", "catalog", "schema", "table", "view", "node", "query"} {
		if !kinds[want] {
			t.Fatalf("manifest is missing resource kind %q", want)
		}
	}
	streams := map[string]plugin.StreamKind{}
	for _, s := range m.Streams {
		streams[s.RouteID] = s.Kind
	}
	if streams["trino.query"] != plugin.StreamQuery {
		t.Fatalf("query editor stream must be %q, got %q", plugin.StreamQuery, streams["trino.query"])
	}
	if streams["trino.cluster.metrics"] != plugin.StreamMetrics {
		t.Fatalf("cluster metrics stream must be %q, got %q", plugin.StreamMetrics, streams["trino.cluster.metrics"])
	}
}

// Trino rows carry no key an UPDATE or DELETE could target.
func TestDataGridIsAppendOnly(t *testing.T) {
	cfg := dataGridConfig()
	if !cfg.Editable || cfg.Insert == nil {
		t.Fatal("data grid should allow inserts")
	}
	if cfg.Update != nil || cfg.Delete != nil {
		t.Fatal("Trino rows have no identity, so update/delete must not be declared")
	}
	if cfg.ColumnsSource == nil {
		t.Fatal("data grid needs a runtime columns source")
	}
}

func TestRouteIDsAreNamespacedAndAudited(t *testing.T) {
	for _, r := range New().Routes() {
		if !strings.HasPrefix(r.ID, protocolName+".") {
			t.Fatalf("route %q is not namespaced under %q", r.ID, protocolName)
		}
		if r.AuditEvent != r.ID {
			t.Fatalf("route %q audit event %q should equal the route id", r.ID, r.AuditEvent)
		}
		if r.Permission == "" {
			t.Fatalf("route %q has no permission", r.ID)
		}
	}
}

func TestParseOptionsRequiresHostAndResolvesCredentials(t *testing.T) {
	if _, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{}}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("missing host must be rejected, got %v", err)
	}
	opts, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{
		"host": "trino.internal", "port": 8443, "catalog": "tpch", "schema": "tiny",
		"auth": authPassword, "username": "analyst", "password": "s3cr3t", "tls_mode": "verify-full",
	}})
	if err != nil {
		t.Fatalf("valid password config rejected: %v", err)
	}
	if opts.Username != "analyst" || opts.Password != "s3cr3t" || opts.scheme() != "https" {
		t.Fatalf("unexpected options: %+v", opts)
	}
	if _, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{"host": "h", "auth": authToken, "tls_mode": "verify-full"}}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("token auth without a credential must be rejected, got %v", err)
	}
	if _, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{"host": "h", "auth": authClientCert, "username": "u"}}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("certificate auth without a credential must be rejected, got %v", err)
	}
	if _, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{"host": "h", "auth": "ldap", "username": "u"}}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("unknown auth mode must be rejected, got %v", err)
	}
	if _, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{"host": "h", "catalog": "bad catalog"}}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("invalid default catalog must be rejected, got %v", err)
	}
}

// Trino drops basic auth and a bearer token on a plaintext hop, so the plugin
// refuses the configuration instead of connecting without the secret.
func TestParseOptionsRefusesSecretsWithoutTLS(t *testing.T) {
	if _, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{
		"host": "h", "auth": authPassword, "username": "u", "password": "p", "tls_mode": "disable",
	}}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("password over plaintext must be rejected, got %v", err)
	}
	if _, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{
		"host": "h", "auth": authUser, "username": "u", "tls_mode": "disable",
	}}); err != nil {
		t.Fatalf("user-only auth needs no TLS: %v", err)
	}
}

func TestParseOptionsClampsLimits(t *testing.T) {
	opts, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{
		"host": "h", "username": "u", "row_limit": plugin.MaxPageLimit * 10, "max_connections": 500,
	}})
	if err != nil {
		t.Fatalf("config rejected: %v", err)
	}
	if opts.RowLimit != plugin.MaxPageLimit {
		t.Fatalf("row limit should clamp to %d, got %d", plugin.MaxPageLimit, opts.RowLimit)
	}
	if opts.MaxConns != 20 {
		t.Fatalf("pool size should clamp to 20, got %d", opts.MaxConns)
	}
}

// The DSN must never carry a secret or TLS material: the driver rejects a
// custom HTTP client combined with an SSL certificate, and everything sensitive
// belongs on the gateway-provided transport anyway.
func TestBuildDSNCarriesSessionButNoSecrets(t *testing.T) {
	opts := options{
		Host: "trino.internal", Port: 8443, Catalog: "tpch", Schema: "tiny",
		Username: "analyst", Password: "p@ss", AccessToken: "jwt", TLSMode: "verify-full",
		CACertificate: "-----BEGIN CERTIFICATE-----", ClientCertificate: "-----BEGIN CERTIFICATE-----",
		Source: "shellcn", ClientTags: []string{"a", "b"},
		SessionProperties: map[string]string{"query_priority": "1"},
		QueryTimeout:      defaultTimeout,
	}
	dsn, err := buildDSN(opts, "client-1", "")
	if err != nil {
		t.Fatalf("build DSN: %v", err)
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("DSN is not a URL: %v", err)
	}
	if u.Scheme != "https" || u.Host != "trino.internal:8443" {
		t.Fatalf("unexpected server URI: %q", dsn)
	}
	if pass, ok := u.User.Password(); u.User.Username() != "analyst" || ok || pass != "" {
		t.Fatalf("the password must stay out of the DSN: %q", u.User.String())
	}
	q := u.Query()
	if q.Get("catalog") != "tpch" || q.Get("schema") != "tiny" || q.Get("custom_client") != "client-1" {
		t.Fatalf("unexpected DSN parameters: %v", q)
	}
	if q.Get("session_properties") != "query_priority:1" || q.Get("clientTags") != "a,b" {
		t.Fatalf("session properties/tags missing: %v", q)
	}
	if q.Get("SSLCert") != "" || q.Get("SSLCertPath") != "" {
		t.Fatalf("TLS material must not reach the DSN: %v", q)
	}
	if q.Get("accessToken") != "" || strings.Contains(dsn, "jwt") || strings.Contains(dsn, "p@ss") {
		t.Fatalf("no secret may appear in the DSN: %q", dsn)
	}
}

// The secrets the DSN no longer carries must still reach the coordinator.
func TestAuthTransportAttachesTheConfiguredCredential(t *testing.T) {
	if got := withAuth(http.DefaultTransport, options{Username: "u"}); got != http.RoundTripper(http.DefaultTransport) {
		t.Fatal("a credential-free connection must not be wrapped")
	}
	basic := captureAuth(t, options{Username: "analyst", Password: "p@ss"})
	user, pass, ok := basic.BasicAuth()
	if !ok || user != "analyst" || pass != "p@ss" {
		t.Fatalf("basic auth missing: %q", basic.Header.Get("Authorization"))
	}
	// A token wins: parseOptions never resolves both, and a stale password must
	// not shadow the bearer credential if it ever did.
	bearer := captureAuth(t, options{Username: "analyst", Password: "p@ss", AccessToken: "jwt"})
	if got := bearer.Header.Get("Authorization"); got != "Bearer jwt" {
		t.Fatalf("unexpected authorization header: %q", got)
	}
}

func captureAuth(t *testing.T, opts options) *http.Request {
	t.Helper()
	recorder := &recordingTransport{}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://trino.internal/v1/statement", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if _, err := withAuth(recorder, opts).RoundTrip(req); err != nil {
		t.Fatalf("round trip: %v", err)
	}
	if req.Header.Get("Authorization") != "" {
		t.Fatal("a RoundTripper must not mutate the request it was given")
	}
	return recorder.seen
}

type recordingTransport struct{ seen *http.Request }

func (r *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r.seen = req
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}")), Header: http.Header{}, Request: req}, nil
}

func TestBuildDSNPrefersTheGatewayUpstream(t *testing.T) {
	dsn, err := buildDSN(options{Host: "ignored", Port: 1, Username: "u", QueryTimeout: defaultTimeout}, "c", "http://127.0.0.1:9999/tunnel")
	if err != nil {
		t.Fatalf("build DSN: %v", err)
	}
	u, _ := url.Parse(dsn)
	if u.Host != "127.0.0.1:9999" {
		t.Fatalf("an agent-supplied upstream must win: %q", dsn)
	}
	if _, err := buildDSN(options{Host: "h", Port: 1, Username: "u", QueryTimeout: defaultTimeout}, "c", "not a url"); !errors.Is(err, plugin.ErrUnavailable) {
		t.Fatalf("a malformed upstream must be rejected, got %v", err)
	}
}

func TestBuildDSNOmitsIdentityForTokenOnlyAuth(t *testing.T) {
	dsn, err := buildDSN(options{Host: "h", Port: 8080, AccessToken: "jwt", QueryTimeout: defaultTimeout}, "c", "")
	if err != nil {
		t.Fatalf("build DSN: %v", err)
	}
	u, _ := url.Parse(dsn)
	if u.User != nil {
		t.Fatalf("token auth should not embed a userinfo section: %q", dsn)
	}
}

func TestParseSessionPropertiesRejectsSeparators(t *testing.T) {
	props, err := parseSessionProperties("query_priority=1\n# comment\n\nquery_max_run_time=10m")
	if err != nil {
		t.Fatalf("valid properties rejected: %v", err)
	}
	if len(props) != 2 || props["query_priority"] != "1" || props["query_max_run_time"] != "10m" {
		t.Fatalf("unexpected properties: %#v", props)
	}
	if _, err := parseSessionProperties("broken"); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("a property without '=' must be rejected, got %v", err)
	}
	if _, err := parseSessionProperties("a=b;c=d"); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("a property carrying the header separator must be rejected, got %v", err)
	}
	if _, err := parseSessionProperties("a=b:c"); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("a property carrying the pair separator must be rejected, got %v", err)
	}
}

func TestIdentifierQuoting(t *testing.T) {
	if got := quoteIdent(`we"ird`); got != `"we""ird"` {
		t.Fatalf("quoteIdent escaped wrongly: %q", got)
	}
	if got := qualified("tpch", "tiny", "nation"); got != `"tpch"."tiny"."nation"` {
		t.Fatalf("unexpected qualified name: %q", got)
	}
	if got := quoteLiteral("a'b"); got != `'a''b'` {
		t.Fatalf("quoteLiteral escaped wrongly: %q", got)
	}
	for _, bad := range []string{"", "bad name", "drop.table", `x"y`, "a;b"} {
		if _, err := safeIdentifier(bad); !errors.Is(err, plugin.ErrInvalidInput) {
			t.Fatalf("identifier %q must be rejected, got %v", bad, err)
		}
	}
	for _, ok := range []string{"tpch", "my-table", "t$1", "_x", "9lives"} {
		if _, err := safeIdentifier(ok); err != nil {
			t.Fatalf("identifier %q should be accepted: %v", ok, err)
		}
	}
}

func TestBuildKillQueryEscapesID(t *testing.T) {
	want := "CALL system.runtime.kill_query(query_id => '20260101_000000_00000_abcde', message => 'Terminated from " + plugin.DefaultClientName + "')"
	if got := buildKillQuery("20260101_000000_00000_abcde"); got != want {
		t.Fatalf("unexpected kill call:\n got %q\nwant %q", got, want)
	}
	if got := buildKillQuery("a' OR '1'='1"); !strings.Contains(got, `'a'' OR ''1''=''1'`) {
		t.Fatalf("query id must be escaped into the literal: %q", got)
	}
}

func TestCursorRoundTripsAndAcceptsNumericFallback(t *testing.T) {
	token := encodeCursor(150)
	if _, err := strconv.Atoi(token); err == nil {
		t.Fatalf("cursor must be opaque, got a bare number: %q", token)
	}
	offset, err := cursorOffset(token)
	if err != nil || offset != 150 {
		t.Fatalf("cursor round-trip failed: %d %v", offset, err)
	}
	// The grid falls back to a plain numeric offset; it must keep working.
	if offset, err = cursorOffset("42"); err != nil || offset != 42 {
		t.Fatalf("numeric cursor fallback failed: %d %v", offset, err)
	}
	if offset, err = cursorOffset(""); err != nil || offset != 0 {
		t.Fatalf("empty cursor should start at 0: %d %v", offset, err)
	}
	for _, bad := range []string{"-1", "not-a-cursor", base64.RawURLEncoding.EncodeToString([]byte("x9"))} {
		if _, err := cursorOffset(bad); !errors.Is(err, plugin.ErrInvalidInput) {
			t.Fatalf("cursor %q must be rejected, got %v", bad, err)
		}
	}
}

func TestPageScannedFiltersAndSortsAcrossTheWholeSet(t *testing.T) {
	rows := []row{
		{"name": "gamma", "value": int64(3)},
		{"name": "alpha", "value": int64(1)},
		{"name": "beta", "value": int64(2)},
	}
	page, err := pageScanned(listRC(t, url.Values{"limit": {"2"}, "sort": {"name"}}), clone(rows), false)
	if err != nil {
		t.Fatalf("page: %v", err)
	}
	if len(page.Items) != 2 || page.Items[0]["name"] != "alpha" || page.Items[1]["name"] != "beta" {
		t.Fatalf("sort must span the dataset, got %#v", page.Items)
	}
	if page.NextCursor == "" || page.Total == nil || *page.Total != 3 {
		t.Fatalf("unexpected page envelope: next=%q total=%v", page.NextCursor, page.Total)
	}
	// The continuation must return the remaining row and stop cleanly.
	next, err := pageScanned(listRC(t, url.Values{"limit": {"2"}, "sort": {"name"}, "cursor": {page.NextCursor}}), clone(rows), false)
	if err != nil {
		t.Fatalf("page 2: %v", err)
	}
	if len(next.Items) != 1 || next.Items[0]["name"] != "gamma" {
		t.Fatalf("unexpected second page: %#v", next.Items)
	}
	if next.NextCursor != "" {
		t.Fatalf("last page must not advertise more rows: %q", next.NextCursor)
	}
	filtered, err := pageScanned(listRC(t, url.Values{"filter": {"bet"}}), clone(rows), false)
	if err != nil {
		t.Fatalf("filtered page: %v", err)
	}
	if len(filtered.Items) != 1 || filtered.Items[0]["name"] != "beta" {
		t.Fatalf("filter must span the dataset, got %#v", filtered.Items)
	}
}

// A truncated scan cannot report an authoritative count, so Total stays unset.
func TestPageScannedDropsTotalWhenTruncated(t *testing.T) {
	page, err := pageScanned(listRC(t, nil), []row{{"name": "a"}}, true)
	if err != nil {
		t.Fatalf("page: %v", err)
	}
	if page.Total != nil {
		t.Fatalf("truncated scan must not report a total, got %d", *page.Total)
	}
}

func TestQuerySafetyStopsBeforeTheCluster(t *testing.T) {
	readOnly := &Session{opts: options{ReadOnly: true}}
	if _, err := executeQueryRequest(context.Background(), readOnly, queryScope{}, sqldb.QueryRequest{Query: "insert into t values (1)"}); !errors.Is(err, plugin.ErrForbidden) {
		t.Fatal("read-only mode must block INSERT")
	}
	// EXPLAIN ANALYZE executes the analysed statement, so it is not read-only.
	if _, err := executeQueryRequest(context.Background(), readOnly, queryScope{}, sqldb.QueryRequest{Query: "explain analyze insert into t values (1)"}); !errors.Is(err, plugin.ErrForbidden) {
		t.Fatal("read-only mode must block EXPLAIN ANALYZE")
	}
	if _, err := executeQueryRequest(context.Background(), readOnly, queryScope{}, sqldb.QueryRequest{Query: ""}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatal("an empty query must be rejected")
	}
	confirming := &Session{opts: options{RequireConfirm: true}}
	_, err := executeQueryRequest(context.Background(), confirming, queryScope{}, sqldb.QueryRequest{Query: "call system.runtime.kill_query(query_id => 'x')"})
	var confirmErr confirmationError
	if !errors.As(err, &confirmErr) {
		t.Fatalf("expected a confirmation challenge, got %v", err)
	}
	if got := queryAuditResult(err); got != plugin.AuditDenied {
		t.Fatalf("confirmation should audit as denied, got %s", got)
	}
	if got := queryAuditResult(nil); got != plugin.AuditAllowed {
		t.Fatalf("success should audit as allowed, got %s", got)
	}
}

func TestStatementClassification(t *testing.T) {
	for _, st := range []string{"SELECT 1", "show catalogs", "explain select 1", "WITH x AS (SELECT 1) SELECT * FROM x", "VALUES 1", "DESCRIBE t"} {
		if !isReadOnlyStatement(st) {
			t.Fatalf("%q should be read-only", st)
		}
	}
	for _, st := range []string{"INSERT INTO t VALUES (1)", "set session x = 1", "CALL a.b()", "explain analyze select 1", "USE tpch.tiny"} {
		if isReadOnlyStatement(st) {
			t.Fatalf("%q must not be read-only", st)
		}
		if !isDestructiveStatement(st) {
			t.Fatalf("%q should require confirmation", st)
		}
	}
	if isDestructiveStatement("SELECT 1") {
		t.Fatal("SELECT must not require confirmation")
	}
	if !statementReturnsRows("SHOW SESSION") || statementReturnsRows("INSERT INTO t VALUES (1)") {
		t.Fatal("statementReturnsRows misclassified a statement")
	}
}

func TestQueryScopeRendersHeaderArguments(t *testing.T) {
	if args := (queryScope{Schema: "tiny"}).args(); args != nil {
		t.Fatalf("a schema without a catalog is meaningless to Trino, got %#v", args)
	}
	args := (queryScope{Catalog: "tpch", Schema: "tiny"}).args()
	if len(args) != 2 {
		t.Fatalf("expected catalog and schema headers, got %#v", args)
	}
	want := []sql.NamedArg{
		{Name: catalogHeaderParam, Value: "tpch"},
		{Name: schemaHeaderParam, Value: "tiny"},
	}
	for i, arg := range args {
		named, ok := arg.(sql.NamedArg)
		if !ok || named != want[i] {
			t.Fatalf("arg %d = %#v, want %#v", i, arg, want[i])
		}
	}
}

func TestDDLColumnRendering(t *testing.T) {
	col, err := ddlColumn(sqldb.ColumnSpec{Name: "id", Type: "bigint"})
	if err != nil {
		t.Fatalf("valid column rejected: %v", err)
	}
	if col != `"id" bigint NOT NULL` {
		t.Fatalf("unexpected column: %q", col)
	}
	col, err = ddlColumn(sqldb.ColumnSpec{Name: "name", Type: "varchar(25)", Nullable: true})
	if err != nil {
		t.Fatalf("valid nullable column rejected: %v", err)
	}
	if col != `"name" varchar(25)` {
		t.Fatalf("unexpected nullable column: %q", col)
	}
	if _, err := ddlColumn(sqldb.ColumnSpec{Name: "bad name", Type: "bigint"}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatal("unsafe identifier accepted")
	}
	if _, err := ddlColumn(sqldb.ColumnSpec{Name: "x", Type: "bigint); DROP TABLE t --"}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatal("unsafe type accepted")
	}
	if _, err := parseDDLColumns("[]"); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatal("an empty column list must be rejected")
	}
	cols, err := parseDDLColumns(`[{"name":"id","type":"bigint"},{"name":"n","type":"varchar","nullable":true}]`)
	if err != nil {
		t.Fatalf("valid column array rejected: %v", err)
	}
	if len(cols) != 2 || cols[0] != `"id" bigint NOT NULL` || cols[1] != `"n" varchar` {
		t.Fatalf("unexpected columns: %#v", cols)
	}
}

func TestInsertStatementBuild(t *testing.T) {
	stmt, args, err := dialect.Insert(qualified("memory", "demo", "events"), map[string]any{"id": float64(7), "name": "click"})
	if err != nil {
		t.Fatalf("insert build failed: %v", err)
	}
	want := `INSERT INTO "memory"."demo"."events" ("id", "name") VALUES (?, ?)`
	if stmt != want {
		t.Fatalf("unexpected insert:\n got %q\nwant %q", stmt, want)
	}
	if len(args) != 2 || args[0] != int64(7) || args[1] != "click" {
		t.Fatalf("unexpected insert args: %#v", args)
	}
}

func TestWritableColumnGuards(t *testing.T) {
	columns := []columnMeta{{Name: "id", DataType: "bigint"}, {Name: "access_token", DataType: "varchar"}}
	patterns := sqldb.DefaultRedactColumnPatterns()
	if err := ensureWritableColumns(columns, map[string]any{"id": 1}, patterns); err != nil {
		t.Fatalf("known column rejected: %v", err)
	}
	if err := ensureWritableColumns(columns, map[string]any{"nope": 1}, patterns); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("unknown column must be rejected, got %v", err)
	}
	if err := ensureWritableColumns(columns, map[string]any{"access_token": "x"}, patterns); !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("redacted column must not be writable, got %v", err)
	}
	if err := ensureWritable(&Session{opts: options{ReadOnly: true}}); !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("read-only mode must block writes, got %v", err)
	}
}

// A masked column must be invisible to the filter too: LIKE over a redacted
// value would let the grid confirm the secret it refuses to render.
func TestSearchableColumnsSkipUncastableAndMaskedColumns(t *testing.T) {
	columns := []columnMeta{
		{Name: "id", DataType: "bigint"},
		{Name: "tags", DataType: "array(varchar)"},
		{Name: "attrs", DataType: "map(varchar, varchar)"},
		{Name: "nested", DataType: "row(a bigint)"},
		{Name: "blob", DataType: "varbinary"},
		{Name: "sketch", DataType: "HyperLogLog"},
		{Name: "doc", DataType: "json"},
		{Name: "access_token", DataType: "varchar"},
		{Name: "name", DataType: "varchar(25)"},
	}
	got := searchableColumns(columns, sqldb.DefaultRedactColumnPatterns())
	want := []string{"id", "doc", "name"}
	if len(got) != len(want) {
		t.Fatalf("searchable columns = %v, want %v", got, want)
	}
	for i, name := range want {
		if got[i] != name {
			t.Fatalf("searchable columns = %v, want %v", got, want)
		}
	}
}

// The shared clause builder validates against a stricter identifier rule than
// Trino connectors use, which would drop these columns from the filter.
func TestRowSearchClauseCoversTrinoIdentifiers(t *testing.T) {
	clause, args := rowSearchClause([]string{"sales-2026", "col$1", "9lives", "bad name"}, " alg ")
	if len(args) != 3 {
		t.Fatalf("expected one argument per usable column, got %#v", args)
	}
	for _, want := range []string{`UPPER(CAST("sales-2026" AS VARCHAR)) LIKE UPPER(?)`, `"col$1"`, `"9lives"`} {
		if !strings.Contains(clause, want) {
			t.Fatalf("clause %q is missing %q", clause, want)
		}
	}
	if strings.Contains(clause, "bad name") {
		t.Fatalf("an unusable identifier must be dropped: %q", clause)
	}
	if args[0] != "%alg%" {
		t.Fatalf("the term must be trimmed and wrapped: %#v", args[0])
	}
	if clause, args := rowSearchClause([]string{"bad name"}, "alg"); clause != "" || args != nil {
		t.Fatalf("no usable column must yield no clause: %q %#v", clause, args)
	}
	if clause, _ := rowSearchClause([]string{"id"}, "   "); clause != "" {
		t.Fatalf("an empty term must yield no clause: %q", clause)
	}
}

func TestSortColumnMustExistAndStayUnmasked(t *testing.T) {
	patterns := sqldb.DefaultRedactColumnPatterns()
	columns := []columnMeta{{Name: "id"}, {Name: "name"}, {Name: "access_token"}}
	if got, err := sortColumn(columns, "name", patterns); err != nil || got != "name" {
		t.Fatalf("known sort column rejected: %q %v", got, err)
	}
	if _, err := sortColumn(columns, "missing", patterns); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("unknown sort column must be rejected, got %v", err)
	}
	if _, err := sortColumn(columns, "id; DROP TABLE t", patterns); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("unsafe sort column must be rejected, got %v", err)
	}
	if _, err := sortColumn(columns, "access_token", patterns); !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("ordering by a masked column leaks it, got %v", err)
	}
}

func TestRedactRowsMasksConfiguredColumns(t *testing.T) {
	rows := []row{{"id": int64(1), "access_token": "plain", "name": "alice"}}
	redactRows(rows, sqldb.DefaultRedactColumnPatterns())
	if rows[0]["access_token"] != sqldb.RedactedValue || rows[0]["name"] != "alice" {
		t.Fatalf("unexpected redaction: %#v", rows)
	}
}

func TestScopeResolutionPrefersParamsThenDefaults(t *testing.T) {
	s := &Session{opts: options{Catalog: "tpch", Schema: "tiny"}}
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, s, map[string]string{"catalog": "memory"}, nil, nil)
	catalog, err := catalogScope(rc, s)
	if err != nil || catalog != "memory" {
		t.Fatalf("param should win over the default: %q %v", catalog, err)
	}
	rc = plugin.NewRequestContext(context.Background(), plugin.User{}, s, nil, url.Values{"p.schema": {"sf1"}}, nil)
	catalog, schema, err := schemaScope(rc, s)
	if err != nil || catalog != "tpch" || schema != "sf1" {
		t.Fatalf("renderer param should scope the schema: %q %q %v", catalog, schema, err)
	}
	empty := &Session{opts: options{}}
	rc = plugin.NewRequestContext(context.Background(), plugin.User{}, empty, nil, nil, nil)
	if _, err := catalogScope(rc, empty); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("an unscoped catalog must be an explicit error, got %v", err)
	}
	if _, _, _, err := tableScope(rc, s); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("a missing table must be rejected, got %v", err)
	}
}

func TestAndClauseComposition(t *testing.T) {
	if got := andClause("", "b"); got != "b" {
		t.Fatalf("unexpected clause: %q", got)
	}
	if got := andClause("a", ""); got != "a" {
		t.Fatalf("unexpected clause: %q", got)
	}
	if got := andClause("a OR b", "c"); got != "(a OR b) AND c" {
		t.Fatalf("a disjunction must stay parenthesised: %q", got)
	}
}

func TestProgressCollectorIsEmptyUntilTheDriverReports(t *testing.T) {
	p := &progressCollector{}
	if id, stats := p.snapshot(); id != "" || stats != nil {
		t.Fatalf("an unused collector must report nothing: %q %#v", id, stats)
	}
	var nilCollector *progressCollector
	if id, stats := nilCollector.snapshot(); id != "" || stats != nil {
		t.Fatalf("a nil collector must report nothing: %q %#v", id, stats)
	}
}

func TestTruthyReadsBothInformationSchemaShapes(t *testing.T) {
	for value, want := range map[any]bool{"YES": true, "yes": true, "NO": false, true: true, false: false, nil: false} {
		if got := truthy(value); got != want {
			t.Fatalf("truthy(%#v) = %v, want %v", value, got, want)
		}
	}
}

func listRC(t *testing.T, query url.Values) *plugin.RequestContext {
	t.Helper()
	return plugin.NewRequestContext(context.Background(), plugin.User{}, &Session{opts: options{RowLimit: defaultRowLimit}}, nil, query, nil)
}

func clone(rows []row) []row {
	out := make([]row, 0, len(rows))
	for _, r := range rows {
		copied := row{}
		for k, v := range r {
			copied[k] = v
		}
		out = append(out, copied)
	}
	return out
}
