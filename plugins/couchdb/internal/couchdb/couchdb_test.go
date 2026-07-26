package couchdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	contribtest "github.com/charlesng35/shellcn-contrib/shared/plugintest"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestManifestValidates(t *testing.T) {
	p := New()
	plugintest.ValidatePlugin(t, p)
	plugintest.ValidateProjectionPanelConfigs(t, plugintest.Projection(t, p))
	if !plugintest.CredentialKindSupported(p.Manifest().Config, plugin.CredentialKindDBPassword) {
		t.Fatal("CouchDB should accept stored database-password credentials")
	}
}

func TestRouteMetadataIsNamespacedAndAudited(t *testing.T) {
	for _, route := range New().Routes() {
		if !strings.HasPrefix(route.ID, protocolName+".") {
			t.Fatalf("route %q is not namespaced", route.ID)
		}
		if route.AuditEvent != route.ID {
			t.Fatalf("route %q audit event %q must equal the route id", route.ID, route.AuditEvent)
		}
		if !strings.HasPrefix(route.Permission, protocolName+".") || strings.Count(route.Permission, ".") != 2 {
			t.Fatalf("route %q permission %q must look like couchdb.<resource>.<verb>", route.ID, route.Permission)
		}
	}
}

func TestParseOptionsDefaultsAndBasicAuth(t *testing.T) {
	opts, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{
		"auth": "basic", "username": "admin", "password": "secret",
	}})
	if err != nil {
		t.Fatalf("parseOptions: %v", err)
	}
	if opts.baseURL() != "http://127.0.0.1:5984" {
		t.Fatalf("unexpected base URL %q", opts.baseURL())
	}
	if !opts.readOnly || opts.timeout != defaultTimeout || opts.pageLimit != defaultPageLimit {
		t.Fatalf("unexpected safety defaults: %+v", opts)
	}
}

func TestParseOptionsTLSSwitchesScheme(t *testing.T) {
	opts, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{
		"host": "couch.internal", "port": float64(6984), "tls_mode": "verify-full",
		"auth": "basic", "username": "admin", "password": "x",
	}})
	if err != nil {
		t.Fatalf("parseOptions: %v", err)
	}
	if opts.baseURL() != "https://couch.internal:6984" {
		t.Fatalf("unexpected base URL %q", opts.baseURL())
	}
	if opts.tls == nil || opts.tls.ServerName != "couch.internal" {
		t.Fatalf("verify-full should pin the server name, got %+v", opts.tls)
	}
}

func TestParseOptionsResolvesStoredCredential(t *testing.T) {
	cfg := plugin.ConnectConfig{
		Config: map[string]any{"auth": "credential_session"},
		Credentials: plugin.NewResolvedCredentials(plugin.CredentialBinding{
			Field: credentialField,
			Credential: plugin.ResolvedCredential{
				Kind:   plugin.CredentialKindDBPassword,
				Values: map[string]string{"username": "svc", "password": "stored"},
			},
		}),
	}
	opts, err := parseOptions(cfg)
	if err != nil {
		t.Fatalf("parseOptions: %v", err)
	}
	if opts.authMode != "session" || opts.username != "svc" || opts.password != "stored" {
		t.Fatalf("unexpected credential material: %+v", opts)
	}
}

func TestParseOptionsRejectsWrongCredentialKind(t *testing.T) {
	_, err := parseOptions(plugin.ConnectConfig{
		Config: map[string]any{"auth": "credential"},
		Credentials: plugin.NewResolvedCredentials(plugin.CredentialBinding{
			Field:      credentialField,
			Credential: plugin.ResolvedCredential{Kind: plugin.CredentialKindAPIToken, Values: map[string]string{"token": "t"}},
		}),
	})
	if !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("expected invalid input for the wrong credential kind, got %v", err)
	}
}

func TestParseOptionsRejectsUnknownAuthMode(t *testing.T) {
	if _, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{"auth": "kerberos"}}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("expected invalid input, got %v", err)
	}
}

// TestConfigSchemaHidesSecretsUntilSelected pins the connection form contract:
// inline secrets and the credential reference are mutually exclusive, and the
// direct-only endpoint fields disappear behind an agent.
func TestConfigSchemaHidesSecretsUntilSelected(t *testing.T) {
	fields := map[string]plugin.Field{}
	for _, group := range configSchema().Groups {
		for _, field := range group.Fields {
			fields[field.Key] = field
		}
	}
	password := fields["password"]
	if !password.Secret || password.VisibleWhen == nil {
		t.Fatalf("the inline password must be secret and conditional: %+v", password)
	}
	credential := fields[credentialField]
	if credential.Type != plugin.FieldCredentialRef || credential.Credential == nil || credential.VisibleWhen == nil {
		t.Fatalf("the credential reference is not wired: %+v", credential)
	}
	if fields["ca_certificate"].Secret != true {
		t.Fatal("the CA certificate must be stored as a secret")
	}
	for _, key := range []string{"host", "port"} {
		if fields[key].VisibleWhen == nil {
			t.Fatalf("%q must be hidden for agent connections", key)
		}
	}
}

func TestConnectRequiresNetworkTransport(t *testing.T) {
	_, err := New().Connect(context.Background(), plugin.ConnectConfig{Config: map[string]any{"auth": "none"}})
	if !errors.Is(err, plugin.ErrUnavailable) {
		t.Fatalf("expected unavailable transport error, got %v", err)
	}
}

func TestReadOnlyGuardBlocksMutations(t *testing.T) {
	called := false
	rc := plugin.NewRequestContext(t.Context(), plugin.User{}, &Session{opts: options{readOnly: true}}, nil, nil, nil)
	_, err := readOnlyGuard(func(*plugin.RequestContext) (any, error) {
		called = true
		return nil, nil
	})(rc)
	if !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("expected forbidden, got %v", err)
	}
	if called {
		t.Fatal("the guard must not reach the wrapped handler")
	}
	for _, route := range New().Routes() {
		if route.Handle == nil || route.Risk == plugin.RiskSafe || pluginOwnedRoutes[route.ID] {
			continue
		}
		if _, err := route.Handle(rc); !errors.Is(err, plugin.ErrForbidden) {
			t.Fatalf("route %q is not guarded in read-only mode: %v", route.ID, err)
		}
	}
	// Saved queries live in ShellCN storage, so read-only mode must not block them.
	saveRC := plugin.NewRequestContext(t.Context(), plugin.User{}, &Session{opts: options{readOnly: true}},
		map[string]string{"db": "orders"}, nil,
		[]byte(`{"name":"n","query":{"selector":{}}}`)).WithStorage(&fakeStorage{})
	if _, err := saveQuery(saveRC); err != nil {
		t.Fatalf("saving a query must work on a read-only connection: %v", err)
	}
}

func TestHTTPErrorMapsCouchSentinels(t *testing.T) {
	cases := []struct {
		status int
		body   string
		want   error
	}{
		{http.StatusConflict, `{"error":"conflict","reason":"Document update conflict."}`, plugin.ErrConflict},
		{http.StatusNotFound, `{"error":"not_found","reason":"missing"}`, plugin.ErrNotFound},
		{http.StatusUnauthorized, `{"error":"unauthorized","reason":"Name or password is incorrect."}`, plugin.ErrUnauthorized},
		{http.StatusForbidden, `{"error":"forbidden","reason":"nope"}`, plugin.ErrForbidden},
		{http.StatusBadRequest, `{"error":"illegal_database_name","reason":"bad"}`, plugin.ErrInvalidInput},
		{http.StatusInternalServerError, `boom`, plugin.ErrUnavailable},
	}
	for _, tc := range cases {
		err := httpError(tc.status, []byte(tc.body))
		if !errors.Is(err, tc.want) {
			t.Fatalf("status %d mapped to %v, want %v", tc.status, err, tc.want)
		}
	}
	if msg := httpError(http.StatusConflict, []byte(`{"error":"conflict","reason":"Document update conflict."}`)).Error(); !strings.Contains(msg, "Document update conflict.") {
		t.Fatalf("the CouchDB reason must survive: %s", msg)
	}
}

func TestDocPathKeepsDesignPrefixLiteral(t *testing.T) {
	if got := docPath("app", "_design/by type"); got != "/app/_design/by%20type" {
		t.Fatalf("unexpected design path %q", got)
	}
	if got := docPath("app/orders", "a/b"); got != "/app%2Forders/a%2Fb" {
		t.Fatalf("unexpected document path %q", got)
	}
}

func TestParseMangoQueryClampsLimit(t *testing.T) {
	body, err := parseMangoQuery(`{"selector":{},"limit":100000}`, 25)
	if err != nil {
		t.Fatalf("parseMangoQuery: %v", err)
	}
	if body["limit"] != 25 {
		t.Fatalf("limit was not clamped: %v", body["limit"])
	}
	if _, err := parseMangoQuery(`{"fields":["_id"]}`, 25); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("a selector-less query must be rejected, got %v", err)
	}
	if _, err := parseMangoQuery(`not json`, 25); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("invalid JSON must be rejected, got %v", err)
	}
}

func TestMangoAuditParamsExcludeRawQuery(t *testing.T) {
	params := mangoAuditParams("app", `{"selector":{"password":"hunter2"}}`, 3, 12)
	if params["query_sha256"] == "" || params["row_count"] != "3" || params["database"] != "app" {
		t.Fatalf("unexpected audit params: %#v", params)
	}
	for _, value := range params {
		if strings.Contains(value, "hunter2") {
			t.Fatalf("the raw query leaked into audit params: %#v", params)
		}
	}
}

func TestChooseRevisionStrategies(t *testing.T) {
	revs := []string{"2-b", "3-c", "1-a"}
	if got, _ := chooseRevision("winner", "", "2-b", revs); got != "2-b" {
		t.Fatalf("winner strategy returned %q", got)
	}
	if got, _ := chooseRevision("highest", "", "2-b", revs); got != "3-c" {
		t.Fatalf("highest strategy returned %q", got)
	}
	if got, _ := chooseRevision("explicit", "1-a", "2-b", revs); got != "1-a" {
		t.Fatalf("explicit strategy returned %q", got)
	}
	if _, err := chooseRevision("explicit", "9-z", "2-b", revs); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("an unknown explicit revision must be rejected, got %v", err)
	}
	if _, err := chooseRevision("newest", "", "2-b", revs); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("an unknown strategy must be rejected, got %v", err)
	}
}

func TestEndpointLabelRedactsCredentials(t *testing.T) {
	if got := endpointLabel("https://admin:hunter2@remote:5984/orders"); got != "https://admin@remote:5984/orders" {
		t.Fatalf("the password was not redacted: %q", got)
	}
	if got := endpointLabel(map[string]any{"url": "https://u:p@remote/db?a=b"}); got != "https://u@remote/db" {
		t.Fatalf("the object endpoint was not redacted: %q", got)
	}
	if got := endpointLabel("orders"); got != "orders" {
		t.Fatalf("a local database name must pass through: %q", got)
	}
}

func TestValidateDesignDocRejectsBrokenViews(t *testing.T) {
	if err := validateDesignDoc(row{"views": row{"a": row{"map": "function(doc){emit(doc._id,1)}"}}}); err != nil {
		t.Fatalf("a valid design document was rejected: %v", err)
	}
	if err := validateDesignDoc(row{"views": row{"a": row{}}}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("a view without map must be rejected, got %v", err)
	}
	if err := validateDesignDoc(row{"views": row{"a": row{"map": "f", "reduce": "_median"}}}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("an unknown built-in reducer must be rejected, got %v", err)
	}
	if err := validateDesignDoc(row{"views": "oops"}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("a non-object views field must be rejected, got %v", err)
	}
}

func TestDatabaseRowComputesFragmentation(t *testing.T) {
	item := databaseRow("orders", row{
		"doc_count": float64(4), "doc_del_count": float64(1),
		"sizes": map[string]any{"file": float64(1000), "active": float64(250)},
		"props": map[string]any{"partitioned": true},
	})
	if item["fragmentation"] != 75.0 {
		t.Fatalf("unexpected fragmentation: %#v", item["fragmentation"])
	}
	if item["partitioned"] != true {
		t.Fatalf("partitioned flag not read from props: %#v", item)
	}
}

func TestGridFieldsDropsManagedKeys(t *testing.T) {
	got := gridFields(row{"id": "a", "rev": "1-x", "ref": "r", "_rev": "1-x", "name": "ada", "tags": []any{"x"}})
	if len(got) != 2 || got["name"] != "ada" {
		t.Fatalf("unexpected grid fields: %#v", got)
	}
}

func TestHandlersAgainstFakeServer(t *testing.T) {
	fake := newFakeCouch()
	srv := httptest.NewServer(fake)
	defer srv.Close()

	p := New()
	sess := connectFake(t, p, srv.URL, plugintest.DirectTransport())
	defer func() { _ = sess.Close() }()

	h := contribtest.NewHarness(t, p.Routes())
	ctx := t.Context()

	page := pageItems(h.Call(ctx, rid("databases.list"), sess, nil, nil, nil))
	if len(page) != 1 || page[0]["name"] != "orders" {
		t.Fatalf("unexpected database page: %#v", page)
	}
	if page[0]["fragmentation"] == nil {
		t.Fatalf("database rows must carry fragmentation: %#v", page[0])
	}

	docs := pageItems(h.Call(ctx, rid("documents.list"), sess, map[string]string{"db": "orders"}, nil, nil))
	if len(docs) != 2 || docs[0]["id"] != "doc-a" || docs[0]["customer"] != "ada" {
		t.Fatalf("unexpected documents page: %#v", docs)
	}

	columns := pageItems(h.Call(ctx, rid("documents.columns"), sess, map[string]string{"db": "orders"}, nil, nil))
	names := make([]string, 0, len(columns))
	for _, column := range columns {
		names = append(names, stringOf(column["name"]))
	}
	for _, want := range []string{"id", "rev", "customer", "total"} {
		if !slices.Contains(names, want) {
			t.Fatalf("column %q missing from %v", want, names)
		}
	}
	for _, column := range columns {
		if column["name"] == "total" && column["editor"] != string(plugin.ColumnEditorNumber) {
			t.Fatalf("numeric columns must use the number editor: %#v", column)
		}
		if column["name"] == "id" && column["readOnly"] != true {
			t.Fatalf("_id must stay read-only: %#v", column)
		}
	}

	// A stale revision must surface as a conflict, not a silent overwrite.
	_, err := h.Route(rid("documents.update")).Handle(plugin.NewRequestContext(ctx, plugin.User{}, sess,
		map[string]string{"db": "orders"}, nil, []byte(`{"id":"doc-a","rev":"1-stale","customer":"grace"}`)))
	if !errors.Is(err, plugin.ErrConflict) {
		t.Fatalf("a stale grid revision must conflict, got %v", err)
	}

	updated := h.Call(ctx, rid("documents.update"), sess, map[string]string{"db": "orders"}, nil,
		[]byte(`{"row":{"id":"doc-a","rev":"1-a","customer":"grace"}}`))
	if field(updated, "rev") == "1-a" {
		t.Fatalf("the update must produce a new revision: %#v", updated)
	}
	if fake.doc("orders", "doc-a")["total"] == nil {
		t.Fatal("a grid update must merge, not replace, the stored document")
	}

	inserted := h.Call(ctx, rid("documents.insert"), sess, map[string]string{"db": "orders"}, nil,
		[]byte(`{"id":"doc-c","customer":"hopper"}`))
	if field(inserted, "id") != "doc-c" {
		t.Fatalf("unexpected insert result: %#v", inserted)
	}
	h.Call(ctx, rid("documents.remove"), sess, map[string]string{"db": "orders"}, nil, []byte(`{"id":"doc-c"}`))
	if fake.doc("orders", "doc-c") != nil {
		t.Fatal("the grid delete did not remove the document")
	}

	created := h.Call(ctx, rid("document.create"), sess, map[string]string{"db": "orders"}, nil,
		[]byte(`{"content":"{\"_id\":\"doc-d\",\"customer\":\"turing\"}"}`))
	if field(created, "id") != "doc-d" {
		t.Fatalf("unexpected create result: %#v", created)
	}
	saved := h.Call(ctx, rid("document.update"), sess, map[string]string{"db": "orders", "docid": "doc-d"}, nil,
		[]byte(`{"content":"{\"customer\":\"lovelace\"}"}`))
	if !strings.Contains(stringOf(field(saved, "content")), "lovelace") {
		t.Fatalf("the editor baseline must be returned: %#v", saved)
	}
	h.Call(ctx, rid("document.delete"), sess, map[string]string{"db": "orders", "docid": "doc-d"}, nil, nil)

	revisions := pageItems(h.Call(ctx, rid("document.revisions"), sess, map[string]string{"db": "orders", "docid": "doc-b"}, nil, nil))
	if len(revisions) != 2 || numberOf(revisions[0]["generation"]) != 2 || revisions[0]["severity"] != "success" {
		t.Fatalf("unexpected revision timeline: %#v", revisions)
	}

	conflicts := pageItems(h.Call(ctx, rid("conflicts.list"), sess, map[string]string{"db": "orders"}, nil, nil))
	if len(conflicts) != 1 || conflicts[0]["id"] != "doc-b" {
		t.Fatalf("unexpected conflict scan: %#v", conflicts)
	}
	docConflicts := pageItems(h.Call(ctx, rid("document.conflicts"), sess, map[string]string{"db": "orders", "docid": "doc-b"}, nil, nil))
	if len(docConflicts) != 2 {
		t.Fatalf("unexpected document conflicts: %#v", docConflicts)
	}
	resolved := h.Call(ctx, rid("document.resolve"), sess, map[string]string{"db": "orders", "docid": "doc-b"}, nil,
		[]byte(`{"keep":"explicit","rev":"2-b2"}`))
	if field(resolved, "kept") != "2-b2" {
		t.Fatalf("the explicitly kept revision should survive: %#v", resolved)
	}
	if deleted := sliceOf(field(resolved, "deleted")); len(deleted) != 1 || stringOf(deleted[0]) != "2-b1" {
		t.Fatalf("every other revision must be deleted: %#v", resolved)
	}

	indexes := pageItems(h.Call(ctx, rid("indexes.list"), sess, map[string]string{"db": "orders"}, nil, nil))
	if len(indexes) != 1 || indexes[0]["name"] != "by-customer" {
		t.Fatalf("unexpected indexes: %#v", indexes)
	}

	replications := pageItems(h.Call(ctx, rid("replications.list"), sess, nil, nil, nil))
	if len(replications) != 1 || replications[0]["state"] != "running" {
		t.Fatalf("unexpected replications: %#v", replications)
	}
	if strings.Contains(stringOf(replications[0]["target"]), "hunter2") {
		t.Fatalf("replication credentials leaked: %#v", replications[0])
	}

	graph := h.Call(ctx, rid("replication.graph"), sess, nil, nil, nil)
	nodes, _ := field(graph, "nodes").([]row)
	edges, _ := field(graph, "edges").([]row)
	if len(nodes) != 2 || len(edges) != 1 {
		t.Fatalf("unexpected replication graph: %#v", graph)
	}
	if nodes[0]["group"] != "local" || nodes[1]["group"] != "remote" {
		t.Fatalf("the graph must distinguish local and remote endpoints: %#v", nodes)
	}
}

func TestMangoStreamReportsIndexAndErrors(t *testing.T) {
	fake := newFakeCouch()
	srv := httptest.NewServer(fake)
	defer srv.Close()

	p := New()
	sess := connectFake(t, p, srv.URL, plugintest.DirectTransport())
	defer func() { _ = sess.Close() }()
	h := contribtest.NewHarness(t, p.Routes())

	input := `{"query":"{\"selector\":{}}"}` + "\n" + `{"query":"{\"fields\":[]}"}` + "\n"
	out := h.Stream(t.Context(), rid("mango.query"), sess, map[string]string{"db": "orders"}, nil, []byte(input))

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected one frame per request, got %q", out)
	}
	var result mangoResult
	if err := json.Unmarshal([]byte(lines[0]), &result); err != nil {
		t.Fatalf("decode result frame: %v", err)
	}
	if result.RowCount != 2 || len(result.Columns) == 0 || result.Columns[0] != "_id" {
		t.Fatalf("unexpected mango result: %#v", result)
	}
	if result.Index != "mango-indexes/by-customer" || result.IndexType != "json" {
		t.Fatalf("the chosen index must be reported: %#v", result)
	}
	if !strings.Contains(lines[1], "selector") {
		t.Fatalf("a selector-less query must return an error frame: %s", lines[1])
	}
}

// TestMangoStreamClosesOnUnparsableFrame guards the frame loop: encoding/json
// never resynchronises after a syntax error, so retrying the same bytes would
// spin the handler forever.
func TestMangoStreamClosesOnUnparsableFrame(t *testing.T) {
	fake := newFakeCouch()
	srv := httptest.NewServer(fake)
	defer srv.Close()

	p := New()
	sess := connectFake(t, p, srv.URL, plugintest.DirectTransport())
	defer func() { _ = sess.Close() }()
	h := contribtest.NewHarness(t, p.Routes())

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan []byte, 1)
	go func() {
		done <- h.Stream(ctx, rid("mango.query"), sess, map[string]string{"db": "orders"}, nil, []byte("not json\n"))
	}()
	select {
	case out := <-done:
		lines := strings.Split(strings.TrimSpace(string(out)), "\n")
		if len(lines) != 1 || !strings.Contains(lines[0], "JSON object") {
			t.Fatalf("expected exactly one error frame, got %q", out)
		}
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("the mango stream did not close after an unparsable frame")
	}
}

func TestChangesStreamEmitsLogFrames(t *testing.T) {
	fake := newFakeCouch()
	srv := httptest.NewServer(fake)
	defer srv.Close()

	p := New()
	sess := connectFake(t, p, srv.URL, plugintest.DirectTransport())
	defer func() { _ = sess.Close() }()
	h := contribtest.NewHarness(t, p.Routes())

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	out := h.Stream(ctx, rid("changes.stream"), sess, map[string]string{"db": "orders", "since": "0"}, nil, nil)
	if !strings.Contains(string(out), `"action":"conflicted"`) {
		t.Fatalf("a multi-leaf change must be flagged as conflicted: %s", out)
	}
	if !strings.Contains(string(out), "Feed closed at sequence") {
		t.Fatalf("the closing sequence must be reported: %s", out)
	}
}

// TestChangesFeedOutlivesTheRequestTimeout pins the continuous feed against the
// per-request timeout: http.Client.Timeout also bounds body reads, so a shared
// client would cut every tail after the configured timeout.
func TestChangesFeedOutlivesTheRequestTimeout(t *testing.T) {
	fake := newFakeCouch()
	fake.changesDelay = 300 * time.Millisecond
	srv := httptest.NewServer(fake)
	defer srv.Close()

	host, port := splitHostPort(t, srv.URL)
	p := New()
	sess, err := p.Connect(t.Context(), plugin.ConnectConfig{
		Config: map[string]any{
			"host": host, "port": float64(port), "auth": "none",
			"read_only": false, "timeout": "250ms",
		},
		Net: plugintest.DirectTransport(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()

	h := contribtest.NewHarness(t, p.Routes())
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	out := string(h.Stream(ctx, rid("changes.stream"), sess, map[string]string{"db": "orders", "since": "0"}, nil, nil))
	if !strings.Contains(out, "Feed closed at sequence") {
		t.Fatalf("the feed was cut before it finished: %s", out)
	}
	if strings.Contains(out, "The changes feed stopped") {
		t.Fatalf("the feed reported a transport error: %s", out)
	}
}

// TestFeedReadersExitWhenTheFeedEndsOnItsOwn pins the teardown of the _changes
// readers: a feed that reaches last_seq must release its close watcher instead of
// parking it until the request itself is cancelled.
func TestFeedReadersExitWhenTheFeedEndsOnItsOwn(t *testing.T) {
	fake := newFakeCouch()
	srv := httptest.NewServer(fake)
	defer srv.Close()

	p := New()
	sess := connectFake(t, p, srv.URL, plugintest.DirectTransport())
	defer func() { _ = sess.Close() }()
	h := contribtest.NewHarness(t, p.Routes())

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	out := string(h.Stream(ctx, rid("changes.stream"), sess, map[string]string{"db": "orders", "since": "0"}, nil, nil))
	if !strings.Contains(out, "Feed closed at sequence") {
		t.Fatalf("the feed did not end on its own: %s", out)
	}
	if events := h.Stream(ctx, rid("documents.watch"), sess, map[string]string{"db": "orders"}, nil, nil); len(events) == 0 {
		t.Fatal("the document watch emitted no events")
	}
	assertNoParkedFeedReaders(t)
}

func assertNoParkedFeedReaders(t *testing.T) {
	t.Helper()
	markers := []string{"couchdb.changesStream", "couchdb.watchDocuments", "couchdb.closeFeedOnDisconnect"}
	deadline := time.Now().Add(5 * time.Second)
	for {
		buf := make([]byte, 1<<20)
		stacks := string(buf[:runtime.Stack(buf, true)])
		parked := ""
		for _, marker := range markers {
			if strings.Contains(stacks, marker) {
				parked = marker
			}
		}
		if parked == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("a goroutine is still parked in %s while the request is alive:\n%s", parked, stacks)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestServerConfigRedactsCredentialMaterial(t *testing.T) {
	fake := newFakeCouch()
	srv := httptest.NewServer(fake)
	defer srv.Close()

	p := New()
	sess := connectFake(t, p, srv.URL, plugintest.DirectTransport())
	defer func() { _ = sess.Close() }()
	h := contribtest.NewHarness(t, p.Routes())

	config := h.Call(t.Context(), rid("server.config"), sess, nil, nil, nil)
	encoded, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("encode config: %v", err)
	}
	for _, secret := range []string{"pbkdf2", "cookie-signing-secret", "hunter2"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("credential material leaked from the node configuration: %s", encoded)
		}
	}
	if !strings.Contains(string(encoded), `"uuid":"abc"`) || !strings.Contains(string(encoded), `"timeout":"600"`) {
		t.Fatalf("non-secret configuration must survive redaction: %s", encoded)
	}
}

func TestNodeRoutesRejectPathEscapes(t *testing.T) {
	rc := plugin.NewRequestContext(t.Context(), plugin.User{}, &Session{opts: options{}},
		map[string]string{"node": ".."}, nil, nil)
	if _, err := nodeStats(rc); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("a dot segment must be rejected before it reaches /_node, got %v", err)
	}
	if _, err := readNode(rc); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("a dot segment must be rejected before it reaches /_node, got %v", err)
	}
	if _, err := nodeParam(plugin.NewRequestContext(t.Context(), plugin.User{}, nil,
		map[string]string{"node": "couchdb@127.0.0.1"}, nil, nil)); err != nil {
		t.Fatalf("a real Erlang node name must be accepted: %v", err)
	}
}

func TestAttachmentsAreListedAndDeleted(t *testing.T) {
	fake := newFakeCouch()
	srv := httptest.NewServer(fake)
	defer srv.Close()

	p := New()
	sess := connectFake(t, p, srv.URL, plugintest.DirectTransport())
	defer func() { _ = sess.Close() }()
	h := contribtest.NewHarness(t, p.Routes())
	ctx := t.Context()
	params := map[string]string{"db": "orders", "docid": "doc-a"}

	items := pageItems(h.Call(ctx, rid("attachments.list"), sess, params, nil, nil))
	if len(items) != 1 || items[0]["name"] != "invoice.pdf" || numberOf(items[0]["length"]) != 2048 {
		t.Fatalf("unexpected attachment listing: %#v", items)
	}
	h.Call(ctx, rid("attachment.delete"), sess, map[string]string{
		"db": "orders", "docid": "doc-a", "name": "invoice.pdf",
	}, nil, nil)
	if left := pageItems(h.Call(ctx, rid("attachments.list"), sess, params, nil, nil)); len(left) != 0 {
		t.Fatalf("the attachment was not removed: %#v", left)
	}
}

func TestWatchStreamsEmitResourceEvents(t *testing.T) {
	fake := newFakeCouch()
	srv := httptest.NewServer(fake)
	defer srv.Close()

	p := New()
	sess := connectFake(t, p, srv.URL, plugintest.DirectTransport())
	defer func() { _ = sess.Close() }()
	h := contribtest.NewHarness(t, p.Routes())

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	out := h.Stream(ctx, rid("databases.watch"), sess, nil, nil, nil)
	var event plugin.ResourceEvent
	if err := json.Unmarshal([]byte(strings.SplitN(strings.TrimSpace(string(out)), "\n", 2)[0]), &event); err != nil {
		t.Fatalf("decode resource event: %v", err)
	}
	if event.Type != "modified" || event.Ref.Kind != "database" || event.Ref.Name != "orders" {
		t.Fatalf("unexpected watch event: %#v", event)
	}
}

func TestMetricsStreamComputesRequestRate(t *testing.T) {
	fake := newFakeCouch()
	srv := httptest.NewServer(fake)
	defer srv.Close()

	p := New()
	sess := connectFake(t, p, srv.URL, plugintest.DirectTransport())
	defer func() { _ = sess.Close() }()
	h := contribtest.NewHarness(t, p.Routes())

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	out := h.Stream(ctx, rid("server.metrics"), sess, nil, nil, nil)
	var frame map[string]any
	if err := json.Unmarshal([]byte(strings.SplitN(strings.TrimSpace(string(out)), "\n", 2)[0]), &frame); err != nil {
		t.Fatalf("decode metrics frame: %v", err)
	}
	for _, key := range []string{"requests", "reads", "writes", "memPct", "activeTasks", "uptime"} {
		if frame[key] == nil {
			t.Fatalf("metric %q missing from frame %#v", key, frame)
		}
	}
}

func TestSavedQueriesUseScopedStorage(t *testing.T) {
	fake := newFakeCouch()
	srv := httptest.NewServer(fake)
	defer srv.Close()

	p := New()
	sess := connectFake(t, p, srv.URL, plugintest.DirectTransport())
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(p.Routes())
	store := &fakeStorage{}
	ctx := t.Context()

	rc := plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"db": "orders"}, nil,
		[]byte(`{"name":"open orders","query":{"selector":{"status":"open"}}}`)).WithStorage(store)
	created, err := routes[rid("query.save")].Handle(rc)
	if err != nil {
		t.Fatalf("save query: %v", err)
	}
	key := stringOf(field(created, "key"))
	if !strings.HasPrefix(key, savedQueryPrefix) {
		t.Fatalf("unexpected saved query key %q", key)
	}

	listRC := plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"db": "orders"}, nil, nil).WithStorage(store)
	listed := pageItems(mustCall(t, routes[rid("queries.list")], listRC))
	if len(listed) != 1 || listed[0]["name"] != "open orders" {
		t.Fatalf("unexpected saved queries: %#v", listed)
	}

	otherRC := plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"db": "other"}, nil, nil).WithStorage(store)
	if other := pageItems(mustCall(t, routes[rid("queries.list")], otherRC)); len(other) != 0 {
		t.Fatalf("saved queries must be scoped to their database: %#v", other)
	}

	delRC := plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"key": key}, nil, nil).WithStorage(store)
	if _, err := routes[rid("query.delete")].Handle(delRC); err != nil {
		t.Fatalf("delete query: %v", err)
	}
	if len(store.items) != 0 {
		t.Fatalf("the saved query was not removed: %#v", store.items)
	}
}

func TestSaveQueryRejectsSelectorlessBody(t *testing.T) {
	rc := plugin.NewRequestContext(t.Context(), plugin.User{}, &Session{opts: options{}},
		map[string]string{"db": "orders"}, nil, []byte(`{"name":"x","query":{"limit":1}}`)).WithStorage(&fakeStorage{})
	if _, err := saveQuery(rc); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("expected invalid input, got %v", err)
	}
}

// TestL7TransportIsUsedWhenOffered proves the plugin honours the gateway's HTTP
// transport (agent http_proxy mode) instead of dialing anything itself.
func TestL7TransportIsUsedWhenOffered(t *testing.T) {
	fake := newFakeCouch()
	srv := httptest.NewServer(fake)
	defer srv.Close()

	p := New()
	sess, err := p.Connect(t.Context(), plugin.ConnectConfig{
		Config: map[string]any{"host": "unused.invalid", "port": float64(1), "auth": "none", "read_only": false},
		Net:    plugintest.HTTPTransport(srv.URL, http.DefaultTransport),
	})
	if err != nil {
		t.Fatalf("connect through the L7 transport: %v", err)
	}
	defer func() { _ = sess.Close() }()

	rc := plugin.NewRequestContext(t.Context(), plugin.User{}, sess, nil, nil, nil)
	out, err := listDatabases(rc)
	if err != nil {
		t.Fatalf("list databases: %v", err)
	}
	if len(pageItems(out)) != 1 {
		t.Fatalf("unexpected page over the L7 transport: %#v", out)
	}
}

// TestDialerTransportIsUsedForL4 proves the L4 fallback dials only through the
// transport the gateway supplies.
func TestDialerTransportIsUsedForL4(t *testing.T) {
	fake := newFakeCouch()
	srv := httptest.NewServer(fake)
	defer srv.Close()
	upstream := strings.TrimPrefix(srv.URL, "http://")

	var dialed []string
	var mu sync.Mutex
	transport := plugintest.TransportFunc(func(ctx context.Context, network, addr string) (net.Conn, error) {
		mu.Lock()
		dialed = append(dialed, addr)
		mu.Unlock()
		return (&net.Dialer{}).DialContext(ctx, network, upstream)
	})

	p := New()
	sess, err := p.Connect(t.Context(), plugin.ConnectConfig{
		Config: map[string]any{"host": "couch.internal", "port": float64(5984), "auth": "none", "read_only": false},
		Net:    transport,
	})
	if err != nil {
		t.Fatalf("connect through the dialer transport: %v", err)
	}
	defer func() { _ = sess.Close() }()

	mu.Lock()
	defer mu.Unlock()
	if len(dialed) == 0 || dialed[0] != "couch.internal:5984" {
		t.Fatalf("the plugin must dial the configured address through cfg.Net, got %v", dialed)
	}
}

func TestSessionAuthenticatesWithCookieSession(t *testing.T) {
	fake := newFakeCouch()
	fake.requireCookie = true
	srv := httptest.NewServer(fake)
	defer srv.Close()

	host, port := splitHostPort(t, srv.URL)
	p := New()
	sess, err := p.Connect(t.Context(), plugin.ConnectConfig{
		Config: map[string]any{
			"host": host, "port": float64(port), "auth": "session",
			"username": "admin", "password": "secret", "read_only": false,
		},
		Net: plugintest.DirectTransport(),
	})
	if err != nil {
		t.Fatalf("cookie session connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	if _, err := listDatabases(plugin.NewRequestContext(t.Context(), plugin.User{}, sess, nil, nil, nil)); err != nil {
		t.Fatalf("cookie-authenticated request failed: %v", err)
	}
	if fake.sessionLogins == 0 {
		t.Fatal("the plugin never exchanged the password for a session cookie")
	}
}

func connectFake(t *testing.T, p plugin.Plugin, endpoint string, net plugin.NetTransport) plugin.Session {
	t.Helper()
	host, port := splitHostPort(t, endpoint)
	sess, err := p.Connect(t.Context(), plugin.ConnectConfig{
		Config: map[string]any{
			"host": host, "port": float64(port), "auth": "none",
			"read_only": false, "page_limit": 50, "timeout": "10s",
		},
		Net: net,
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return sess
}

func splitHostPort(t *testing.T, endpoint string) (string, int) {
	t.Helper()
	parsed, err := url.Parse(endpoint)
	if err != nil {
		t.Fatalf("parse endpoint: %v", err)
	}
	host, rawPort, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatalf("split endpoint: %v", err)
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil {
		t.Fatalf("parse port: %v", err)
	}
	return host, port
}

func mustCall(t *testing.T, route plugin.Route, rc *plugin.RequestContext) any {
	t.Helper()
	out, err := route.Handle(rc)
	if err != nil {
		t.Fatalf("%s: %v", route.ID, err)
	}
	return out
}

func pageItems(page any) []row {
	data, _ := json.Marshal(page)
	var decoded struct {
		Items []row `json:"items"`
	}
	_ = json.Unmarshal(data, &decoded)
	return decoded.Items
}

func field(value any, key string) any {
	data, _ := json.Marshal(value)
	var decoded map[string]any
	if json.Unmarshal(data, &decoded) != nil {
		return nil
	}
	if key == "nodes" || key == "edges" {
		entries := sliceOf(decoded[key])
		rows := make([]row, 0, len(entries))
		for _, entry := range entries {
			rows = append(rows, mapOf(entry))
		}
		return rows
	}
	return decoded[key]
}

// fakeStorage mirrors the gateway's scoped storage semantics closely enough to
// validate handler behaviour.
type fakeStorage struct {
	mu    sync.Mutex
	items map[string]plugin.StorageItem
}

func (s *fakeStorage) Get(_ context.Context, scope plugin.StorageScope, key string) (plugin.StorageItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.items[scope.Collection+"/"+key]
	if !ok {
		return plugin.StorageItem{}, plugin.ErrNotFound
	}
	return item, nil
}

func (s *fakeStorage) Put(_ context.Context, collection string, item plugin.StorageItem) (plugin.StorageItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.items == nil {
		s.items = map[string]plugin.StorageItem{}
	}
	item.UpdatedAt = time.Now()
	item.CreatedAt = item.UpdatedAt
	s.items[collection+"/"+item.Key] = item
	return item, nil
}

func (s *fakeStorage) Delete(_ context.Context, scope plugin.StorageScope, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	full := scope.Collection + "/" + key
	if _, ok := s.items[full]; !ok {
		return plugin.ErrNotFound
	}
	delete(s.items, full)
	return nil
}

func (s *fakeStorage) List(_ context.Context, scope plugin.StorageScope, filter *plugin.StorageListFilter) ([]plugin.StorageItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if filter == nil {
		filter = &plugin.StorageListFilter{}
	}
	out := make([]plugin.StorageItem, 0, len(s.items))
	for full, item := range s.items {
		if !strings.HasPrefix(full, scope.Collection+"/") {
			continue
		}
		if filter.KeyPrefix != "" && !strings.HasPrefix(item.Key, filter.KeyPrefix) {
			continue
		}
		if filter.ContentType != "" && item.ContentType != filter.ContentType {
			continue
		}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// fakeCouch implements just enough of the CouchDB HTTP API to exercise the
// handlers deterministically, including revisions and conflicts.
type fakeCouch struct {
	mu            sync.Mutex
	dbs           map[string]map[string]row
	requireCookie bool
	sessionLogins int
	requests      int
	changesDelay  time.Duration
}

func newFakeCouch() *fakeCouch {
	return &fakeCouch{dbs: map[string]map[string]row{
		"orders": {
			"doc-a": {"_id": "doc-a", "_rev": "1-a", "customer": "ada", "total": float64(12),
				"_attachments": row{"invoice.pdf": row{
					"content_type": "application/pdf", "length": float64(2048),
					"revpos": float64(1), "digest": "md5-deadbeef", "stub": true}}},
			"doc-b": {"_id": "doc-b", "_rev": "2-b1", "customer": "bob", "total": float64(7),
				"_conflicts": []any{"2-b2"}},
		},
		"_replicator": {
			"orders-backup": {"_id": "orders-backup", "_rev": "1-r", "source": "orders",
				"target": "https://admin:hunter2@remote:5984/orders", "continuous": true},
		},
	}}
}

func (f *fakeCouch) doc(db, id string) row {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dbs[db][id]
}

func (f *fakeCouch) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests++
	f.mu.Unlock()

	if r.URL.Path == "/_session" && r.Method == http.MethodPost {
		f.mu.Lock()
		f.sessionLogins++
		f.mu.Unlock()
		http.SetCookie(w, &http.Cookie{Name: "AuthSession", Value: "fake-cookie"})
		writeJSON(w, http.StatusOK, row{"ok": true, "name": "admin"})
		return
	}
	if f.requireCookie {
		if cookie, err := r.Cookie("AuthSession"); err != nil || cookie.Value == "" {
			writeJSON(w, http.StatusUnauthorized, row{"error": "unauthorized", "reason": "Authentication required."})
			return
		}
	}

	segments := splitPath(r.URL.Path)
	switch {
	case len(segments) == 0:
		writeJSON(w, http.StatusOK, row{"couchdb": "Welcome", "version": "3.5.2", "git_sha": "deadbeef",
			"vendor": row{"name": "The Apache Software Foundation"}, "features": []any{"scheduler"}})
	case segments[0] == "_up":
		writeJSON(w, http.StatusOK, row{"status": "ok"})
	case segments[0] == "_all_dbs":
		writeJSON(w, http.StatusOK, f.databaseNames())
	case segments[0] == "_dbs_info":
		writeJSON(w, http.StatusNotFound, row{"error": "not_found", "reason": "no such endpoint"})
	case segments[0] == "_membership":
		writeJSON(w, http.StatusOK, row{"all_nodes": []any{"couchdb@127.0.0.1"}, "cluster_nodes": []any{"couchdb@127.0.0.1"}})
	case segments[0] == "_active_tasks":
		writeJSON(w, http.StatusOK, []any{row{"type": "indexer", "database": "orders", "pid": "<0.1.0>",
			"design_document": "_design/orders", "changes_done": float64(1), "total_changes": float64(2),
			"started_on": float64(time.Now().Add(-time.Minute).Unix())}})
	case segments[0] == "_scheduler" && len(segments) > 1 && segments[1] == "docs":
		writeJSON(w, http.StatusOK, row{"docs": []any{row{"database": "_replicator", "doc_id": "orders-backup",
			"id": "job-1", "node": "couchdb@127.0.0.1", "state": "running", "error_count": float64(0),
			"source": "orders", "target": "https://admin:hunter2@remote:5984/orders",
			"start_time": "2026-01-01T00:00:00Z",
			"info":       row{"docs_written": float64(4), "docs_read": float64(4), "changes_pending": float64(0)}}}})
	case segments[0] == "_scheduler" && len(segments) > 1 && segments[1] == "jobs":
		writeJSON(w, http.StatusOK, row{"jobs": []any{row{"id": "job-1", "doc_id": "orders-backup",
			"database": "_replicator", "node": "couchdb@127.0.0.1", "source": "orders",
			"target": "https://admin:hunter2@remote:5984/orders", "start_time": "2026-01-01T00:00:00Z",
			"history": []any{row{"timestamp": "2026-01-01T00:00:00Z", "type": "started"}}}}})
	case segments[0] == "_node":
		f.serveNode(w, segments)
	default:
		f.serveDatabase(w, r, segments)
	}
}

func (f *fakeCouch) serveNode(w http.ResponseWriter, segments []string) {
	switch segments[len(segments)-1] {
	case "_system":
		writeJSON(w, http.StatusOK, row{"uptime": float64(1200), "run_queue": float64(1),
			"process_count": float64(400), "memory": row{"total": float64(2000), "processes": float64(500)}})
	case "_stats":
		writeJSON(w, http.StatusOK, row{"couchdb": row{
			"httpd":          row{"requests": row{"value": float64(120), "type": "counter"}},
			"database_reads": row{"value": float64(30)}, "database_writes": row{"value": float64(12)},
			"open_databases": row{"value": float64(2)}, "open_os_files": row{"value": float64(9)},
		}})
	case "_config":
		writeJSON(w, http.StatusOK, row{
			"couchdb":     row{"uuid": "abc"},
			"admins":      row{"admin": "-pbkdf2-1c0ff33,5a17,10"},
			"chttpd_auth": row{"secret": "cookie-signing-secret", "timeout": "600"},
			"replicator":  row{"password": "hunter2", "max_http_sessions": "10"},
		})
	default:
		writeJSON(w, http.StatusNotFound, row{"error": "not_found", "reason": "missing"})
	}
}

func (f *fakeCouch) databaseNames() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	names := make([]string, 0, len(f.dbs))
	for name := range f.dbs {
		if !strings.HasPrefix(name, "_") {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func (f *fakeCouch) serveDatabase(w http.ResponseWriter, r *http.Request, segments []string) {
	db := segments[0]
	f.mu.Lock()
	docs, ok := f.dbs[db]
	f.mu.Unlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, row{"error": "not_found", "reason": "Database does not exist."})
		return
	}
	rest := segments[1:]
	switch {
	case len(rest) == 0 && r.Method == http.MethodGet:
		f.mu.Lock()
		count := len(docs)
		f.mu.Unlock()
		writeJSON(w, http.StatusOK, row{"db_name": db, "doc_count": float64(count), "doc_del_count": float64(0),
			"update_seq": "5-x", "purge_seq": "0", "sizes": row{"file": float64(1000), "active": float64(400)},
			"compact_running": false, "cluster": row{"q": float64(2), "n": float64(1)}})
	case rest[0] == "_all_docs":
		f.serveAllDocs(w, r, db)
	case rest[0] == "_design_docs":
		writeJSON(w, http.StatusOK, row{"total_rows": float64(0), "offset": float64(0), "rows": []any{}})
	case rest[0] == "_index":
		writeJSON(w, http.StatusOK, row{"total_rows": float64(1), "indexes": []any{row{
			"ddoc": "_design/mango-indexes", "name": "by-customer", "type": "json",
			"def": row{"fields": []any{row{"customer": "asc"}}}}}})
	case rest[0] == "_find":
		f.serveFind(w, db)
	case rest[0] == "_explain":
		writeJSON(w, http.StatusOK, row{"index": row{"ddoc": "_design/mango-indexes", "name": "by-customer", "type": "json"}})
	case rest[0] == "_changes":
		f.serveChanges(w, db)
	case rest[0] == "_bulk_docs":
		f.serveBulk(w, r, db)
	case rest[0] == "_security":
		writeJSON(w, http.StatusOK, row{})
	case len(rest) == 2 && !strings.HasPrefix(rest[0], "_"):
		f.serveAttachment(w, r, db, rest[0], rest[1])
	default:
		f.serveDocument(w, r, db, strings.Join(rest, "/"))
	}
}

func (f *fakeCouch) serveAttachment(w http.ResponseWriter, r *http.Request, db, id, name string) {
	if r.Method != http.MethodDelete {
		writeJSON(w, http.StatusMethodNotAllowed, row{"error": "method_not_allowed", "reason": r.Method})
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	doc := f.dbs[db][id]
	attachments := mapOf(doc["_attachments"])
	if doc == nil || attachments[name] == nil {
		writeJSON(w, http.StatusNotFound, row{"error": "not_found", "reason": "Document is missing attachment"})
		return
	}
	delete(attachments, name)
	doc["_rev"] = bumpRev(stringOf(doc["_rev"]))
	writeJSON(w, http.StatusOK, row{"ok": true, "id": id, "rev": doc["_rev"]})
}

func (f *fakeCouch) serveAllDocs(w http.ResponseWriter, r *http.Request, db string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make([]string, 0, len(f.dbs[db]))
	for id := range f.dbs[db] {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	skip, _ := strconv.Atoi(r.URL.Query().Get("skip"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = len(ids)
	}
	rows := make([]any, 0, len(ids))
	for i, id := range ids {
		if i < skip || len(rows) >= limit {
			continue
		}
		doc := f.dbs[db][id]
		entry := row{"id": id, "key": id, "value": row{"rev": doc["_rev"]}}
		if r.URL.Query().Get("include_docs") == "true" {
			entry["doc"] = doc
		}
		rows = append(rows, entry)
	}
	writeJSON(w, http.StatusOK, row{"total_rows": float64(len(ids)), "offset": float64(skip), "rows": rows})
}

func (f *fakeCouch) serveFind(w http.ResponseWriter, db string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	docs := make([]any, 0, len(f.dbs[db]))
	ids := make([]string, 0, len(f.dbs[db]))
	for id := range f.dbs[db] {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		docs = append(docs, f.dbs[db][id])
	}
	writeJSON(w, http.StatusOK, row{"docs": docs, "bookmark": "nil"})
}

func (f *fakeCouch) serveChanges(w http.ResponseWriter, db string) {
	f.mu.Lock()
	ids := make([]string, 0, len(f.dbs[db]))
	for id := range f.dbs[db] {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	lines := make([]string, 0, len(ids)+1)
	for i, id := range ids {
		doc := f.dbs[db][id]
		changes := []any{row{"rev": doc["_rev"]}}
		for _, conflict := range sliceOf(doc["_conflicts"]) {
			changes = append(changes, row{"rev": conflict})
		}
		payload, _ := json.Marshal(row{"seq": strconv.Itoa(i + 1), "id": id, "changes": changes, "doc": doc})
		lines = append(lines, string(payload))
	}
	delay := f.changesDelay
	f.mu.Unlock()
	last, _ := json.Marshal(row{"last_seq": strconv.Itoa(len(ids))})
	lines = append(lines, "", string(last))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	if flusher != nil {
		flusher.Flush()
	}
	for _, line := range lines {
		if delay > 0 {
			time.Sleep(delay)
		}
		_, _ = fmt.Fprint(w, line+"\n")
		if flusher != nil {
			flusher.Flush()
		}
	}
}

func (f *fakeCouch) serveBulk(w http.ResponseWriter, r *http.Request, db string) {
	var body struct {
		Docs []row `json:"docs"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	f.mu.Lock()
	defer f.mu.Unlock()
	results := make([]any, 0, len(body.Docs))
	for _, doc := range body.Docs {
		id := stringOf(doc["_id"])
		if doc["_deleted"] == true {
			stored := f.dbs[db][id]
			if stored != nil {
				remaining := make([]any, 0)
				for _, conflict := range sliceOf(stored["_conflicts"]) {
					if stringOf(conflict) != stringOf(doc["_rev"]) {
						remaining = append(remaining, conflict)
					}
				}
				stored["_conflicts"] = remaining
			}
			results = append(results, row{"ok": true, "id": id, "rev": "9-deleted"})
			continue
		}
		doc["_rev"] = bumpRev(stringOf(doc["_rev"]))
		f.dbs[db][id] = doc
		results = append(results, row{"ok": true, "id": id, "rev": doc["_rev"]})
	}
	writeJSON(w, http.StatusOK, results)
}

func (f *fakeCouch) serveDocument(w http.ResponseWriter, r *http.Request, db, id string) {
	id, _ = url.PathUnescape(id)
	f.mu.Lock()
	defer f.mu.Unlock()
	docs := f.dbs[db]

	switch r.Method {
	case http.MethodGet:
		doc, ok := docs[id]
		if !ok {
			writeJSON(w, http.StatusNotFound, row{"error": "not_found", "reason": "missing"})
			return
		}
		out := row{}
		for key, value := range doc {
			out[key] = value
		}
		if r.URL.Query().Get("conflicts") != "true" {
			delete(out, "_conflicts")
		}
		if r.URL.Query().Get("revs_info") == "true" {
			infos := []any{row{"rev": doc["_rev"], "status": "available"}}
			if id == "doc-b" {
				infos = append(infos, row{"rev": "1-b0", "status": "missing"})
			}
			out["_revs_info"] = infos
		}
		writeJSON(w, http.StatusOK, out)
	case http.MethodPut:
		var incoming row
		_ = json.NewDecoder(r.Body).Decode(&incoming)
		if existing, ok := docs[id]; ok && stringOf(incoming["_rev"]) != stringOf(existing["_rev"]) {
			writeJSON(w, http.StatusConflict, row{"error": "conflict", "reason": "Document update conflict."})
			return
		}
		incoming["_id"] = id
		incoming["_rev"] = bumpRev(stringOf(incoming["_rev"]))
		docs[id] = incoming
		writeJSON(w, http.StatusCreated, row{"ok": true, "id": id, "rev": incoming["_rev"]})
	case http.MethodDelete:
		if _, ok := docs[id]; !ok {
			writeJSON(w, http.StatusNotFound, row{"error": "not_found", "reason": "missing"})
			return
		}
		delete(docs, id)
		writeJSON(w, http.StatusOK, row{"ok": true, "id": id, "rev": "9-deleted"})
	case http.MethodPost:
		var incoming row
		_ = json.NewDecoder(r.Body).Decode(&incoming)
		newDocID := "generated-" + strconv.Itoa(len(docs)+1)
		incoming["_id"] = newDocID
		incoming["_rev"] = "1-new"
		docs[newDocID] = incoming
		writeJSON(w, http.StatusCreated, row{"ok": true, "id": newDocID, "rev": "1-new"})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, row{"error": "method_not_allowed", "reason": r.Method})
	}
}

func bumpRev(rev string) string {
	return strconv.Itoa(revGeneration(rev)+1) + "-next"
}

func splitPath(path string) []string {
	parts := make([]string, 0, 4)
	for _, segment := range strings.Split(strings.Trim(path, "/"), "/") {
		if segment != "" {
			decoded, err := url.PathUnescape(segment)
			if err != nil {
				decoded = segment
			}
			parts = append(parts, decoded)
		}
	}
	return parts
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
