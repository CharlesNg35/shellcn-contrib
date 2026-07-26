package arangodb

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestManifestValidates(t *testing.T) {
	p := New()
	plugintest.ValidatePlugin(t, p)
	plugintest.ValidateProjectionPanelConfigs(t, plugintest.Projection(t, p))

	m := p.Manifest()
	if m.Agent != nil {
		t.Fatal("ArangoDB must not declare agent transport")
	}
	if len(m.SupportedTransports) != 1 || m.SupportedTransports[0] != plugin.TransportDirect {
		t.Fatalf("unexpected transports: %+v", m.SupportedTransports)
	}
	if !plugintest.CredentialKindSupported(m.Config, plugin.CredentialKindDBPassword) {
		t.Fatal("stored database passwords should be selectable")
	}
	if !plugintest.CredentialKindSupported(m.Config, plugin.CredentialKindBearerToken) {
		t.Fatal("stored JWT bearer tokens should be selectable")
	}
	if plugintest.CredentialKindSupported(m.Config, plugin.CredentialKindTLSClientCert) {
		t.Fatal("ArangoDB should not advertise TLS client certificate credentials")
	}
}

func TestManifestReferencesResolve(t *testing.T) {
	p := New()
	m := p.Manifest()
	routeIDs := map[string]bool{}
	for _, route := range p.Routes() {
		routeIDs[route.ID] = true
	}
	actions := map[string]plugin.Action{}
	for _, action := range m.Actions {
		actions[action.ID] = action
		if !routeIDs[action.RouteID] {
			t.Fatalf("action %q points at missing route %q", action.ID, action.RouteID)
		}
	}
	kinds := map[string]bool{}
	for _, resource := range m.Resources {
		kinds[resource.Kind] = true
	}
	for _, group := range m.Tree {
		if group.Source.RouteID != "" && !routeIDs[group.Source.RouteID] {
			t.Fatalf("tree group %q points at missing route %q", group.Key, group.Source.RouteID)
		}
		if group.ResourceKind != "" && !kinds[group.ResourceKind] {
			t.Fatalf("tree group %q points at missing resource %q", group.Key, group.ResourceKind)
		}
		if group.Ref != nil && !kinds[group.Ref.Kind] {
			t.Fatalf("tree group %q ref points at missing resource %q", group.Key, group.Ref.Kind)
		}
	}
	for _, resource := range m.Resources {
		if !routeIDs[resource.List.RouteID] {
			t.Fatalf("resource %q list points at missing route %q", resource.Kind, resource.List.RouteID)
		}
		ids := append(append([]string{}, resource.Actions.Detail...), append(resource.Actions.Toolbar, resource.Actions.Row...)...)
		for _, id := range ids {
			if _, ok := actions[id]; !ok {
				t.Fatalf("resource %q references missing action %q", resource.Kind, id)
			}
		}
		for _, tab := range resource.Detail.Tabs {
			if tab.Source != nil && !routeIDs[tab.Source.RouteID] {
				t.Fatalf("resource %q tab %q points at missing route %q", resource.Kind, tab.Key, tab.Source.RouteID)
			}
			table, ok := tab.Config.(plugin.TableConfig)
			if !ok {
				continue
			}
			for _, id := range append(append([]string{}, table.ActionIDs...), table.RowActionIDs...) {
				if _, ok := actions[id]; !ok {
					t.Fatalf("resource %q tab %q references missing action %q", resource.Kind, tab.Key, id)
				}
			}
		}
	}
}

func TestTreeStopsAtCollections(t *testing.T) {
	// The sidebar is navigation only: a database expands to a fixed category list
	// and every category is a leaf that opens a paginated resource list.
	nodes := treeCategories(t, "_system")
	if len(nodes) < 6 {
		t.Fatalf("expected the full category set, got %d", len(nodes))
	}
	for _, node := range nodes[1:] {
		if !node.Leaf {
			t.Fatalf("category %q must be a leaf", node.Key)
		}
		if node.ResourceKind == "" {
			t.Fatalf("category %q must open a resource list", node.Key)
		}
		if node.ChildrenSource != nil {
			t.Fatalf("category %q must not expand data into the sidebar", node.Key)
		}
	}
}

func treeCategories(t *testing.T, database string) []plugin.TreeNode {
	t.Helper()
	sess := &Session{opts: Options{Database: database}}
	out, err := treeDatabase(plugin.NewRequestContext(context.Background(), plugin.User{}, sess, map[string]string{"database": database}, nil, nil))
	if err != nil {
		t.Fatalf("treeDatabase: %v", err)
	}
	return out.(plugin.Page[plugin.TreeNode]).Items
}

func TestConfigSchemaExposesOnlyArangoFields(t *testing.T) {
	fields := fieldMap(configSchema())
	for _, key := range []string{
		"endpoint", "database", "auth", "username", "password", credentialIDField,
		"jwt_token", jwtCredentialID, "tls_mode", "ca_certificate", "read_only",
		"require_write_confirmation", "timeout", "page_limit", "traversal_depth", "redact_attributes",
	} {
		if !fields[key] {
			t.Fatalf("schema should expose %q", key)
		}
	}
	for _, key := range []string{"host", "port", "keyspace", "bucket", "access_key_id", "client_cert_id"} {
		if fields[key] {
			t.Fatalf("schema should not expose unrelated field %q", key)
		}
	}
}

func TestConfigSchemaVisibilityIsAuthSpecific(t *testing.T) {
	schema := configSchema()
	tests := []struct {
		name   string
		values map[string]any
		want   []string
		reject []string
	}{
		{name: "password", values: map[string]any{"auth": authPassword, "tls_mode": "disable"},
			want: []string{"username", "password"}, reject: []string{credentialIDField, "jwt_token", jwtCredentialID, "ca_certificate"}},
		{name: "stored password", values: map[string]any{"auth": authCredential, "tls_mode": "disable"},
			want: []string{credentialIDField}, reject: []string{"username", "password", "jwt_token", jwtCredentialID}},
		{name: "jwt", values: map[string]any{"auth": authJWT, "tls_mode": "verify-full"},
			want: []string{"jwt_token", "ca_certificate"}, reject: []string{"username", "password", credentialIDField, jwtCredentialID}},
		{name: "stored jwt", values: map[string]any{"auth": authStoredJWT, "tls_mode": "require"},
			want: []string{jwtCredentialID}, reject: []string{"username", "password", credentialIDField, "jwt_token", "ca_certificate"}},
		{name: "none", values: map[string]any{"auth": authNone, "tls_mode": "disable"},
			want: []string{"auth", "endpoint"}, reject: []string{"username", "password", credentialIDField, "jwt_token", jwtCredentialID}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			visible := visibleFields(schema, tt.values)
			for _, key := range tt.want {
				if !visible[key] {
					t.Fatalf("expected %q to be visible in %#v", key, visible)
				}
			}
			for _, key := range tt.reject {
				if visible[key] {
					t.Fatalf("expected %q to stay hidden in %#v", key, visible)
				}
			}
		})
	}
}

func TestConfigSchemaSecretsAreNeverPublic(t *testing.T) {
	schema := configSchema()
	secrets := schema.VisibleSecretKeys(map[string]any{"auth": authPassword, "tls_mode": "verify-ca"}, nil)
	if len(secrets) == 0 {
		t.Fatal("password auth should declare at least one secret field")
	}
	for _, key := range secrets {
		if key != "password" && key != "ca_certificate" {
			t.Fatalf("unexpected secret field %q", key)
		}
	}
}

func TestParseOptionsNormalisesEndpointAndAuth(t *testing.T) {
	opts, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{
		"endpoint": "tcp://arango.internal:8529/", "database": "shop", "auth": authPassword,
		"username": "root", "password": "secret", "tls_mode": "verify-full",
		"page_limit": 25, "traversal_depth": 9, "timeout": "3s",
	}})
	if err != nil {
		t.Fatalf("parseOptions: %v", err)
	}
	if opts.Endpoint != "http://arango.internal:8529" {
		t.Fatalf("endpoint not normalised: %q", opts.Endpoint)
	}
	// A plain-HTTP endpoint cannot be TLS-verified, so the TLS mode is dropped.
	if opts.TLSConfig != nil {
		t.Fatal("http endpoints must not build a TLS config")
	}
	if opts.Depth != maxDepth {
		t.Fatalf("traversal depth not clamped: %d", opts.Depth)
	}
	if opts.PageLimit != 25 || opts.Database != "shop" || opts.Username != "root" {
		t.Fatalf("unexpected options: %+v", opts)
	}

	if _, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{"endpoint": "ftp://x:1"}}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("unsupported scheme should be rejected, got %v", err)
	}
	if _, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{"database": "bad/name"}}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("invalid database name should be rejected, got %v", err)
	}
}

func TestParseOptionsBuildsTLSForHTTPS(t *testing.T) {
	opts, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{
		"endpoint": "https://arango.internal:8529", "tls_mode": "verify-full", "auth": authNone,
	}})
	if err != nil {
		t.Fatalf("parseOptions: %v", err)
	}
	if opts.TLSConfig == nil || opts.TLSConfig.ServerName != "arango.internal" {
		t.Fatalf("expected verified TLS for the endpoint host, got %+v", opts.TLSConfig)
	}
}

func TestConnectRejectsAgentTransportBeforeDial(t *testing.T) {
	dialed := false
	_, err := New().Connect(context.Background(), plugin.ConnectConfig{
		Transport: plugin.TransportAgent,
		Net: plugintest.TransportFunc(func(context.Context, string, string) (net.Conn, error) {
			dialed = true
			return nil, errors.New("dial must not happen")
		}),
	})
	if !errors.Is(err, plugin.ErrNotSupported) {
		t.Fatalf("expected unsupported transport, got %v", err)
	}
	if dialed {
		t.Fatal("agent transport must be rejected before any dial")
	}
}

// TestConnectUsesGatewayDialer proves egress leaves through cfg.Net: the fake
// transport is the only way the driver can reach the test server.
func TestConnectUsesGatewayDialer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasPrefix(r.URL.Path, "/_api/version"):
			_, _ = w.Write([]byte(`{"server":"arango","version":"3.12.4","license":"community"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":true,"code":404,"errorNum":404,"errorMessage":"not found"}`))
		}
	}))
	defer srv.Close()

	dials := 0
	sess, err := New().Connect(context.Background(), plugin.ConnectConfig{
		Config: map[string]any{"endpoint": srv.URL, "auth": authNone, "database": defaultDatabase},
		Net: plugintest.TransportFunc(func(ctx context.Context, network, addr string) (net.Conn, error) {
			dials++
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		}),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	if dials == 0 {
		t.Fatal("the driver must dial through cfg.Net")
	}
	if err := sess.HealthCheck(context.Background()); err != nil {
		t.Fatalf("health check: %v", err)
	}
}

func TestClusterOverviewDegradesToSingleServer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasPrefix(r.URL.Path, "/_api/version"):
			_, _ = w.Write([]byte(`{"server":"arango","version":"3.12.4","license":"community"}`))
		case strings.HasSuffix(r.URL.Path, "/_admin/server/role"):
			_, _ = w.Write([]byte(`{"role":"SINGLE"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":true,"code":404,"errorNum":404,"errorMessage":"not found"}`))
		}
	}))
	defer srv.Close()

	sess := connectTo(t, srv.URL)
	defer func() { _ = sess.Close() }()

	out, err := clusterRead(plugin.NewRequestContext(context.Background(), plugin.User{}, sess, nil, nil, nil))
	if err != nil {
		t.Fatalf("clusterRead: %v", err)
	}
	overview := out.(row)
	if overview["deployment"] != "single" {
		t.Fatalf("expected a single-server deployment, got %#v", overview["deployment"])
	}
	if overview["servers_total"] != 1 {
		t.Fatalf("single server should report one member: %#v", overview)
	}

	servers, err := clusterServers(plugin.NewRequestContext(context.Background(), plugin.User{}, sess, nil, nil, nil))
	if err != nil {
		t.Fatalf("clusterServers: %v", err)
	}
	page := servers.(plugin.Page[row])
	if len(page.Items) != 1 || page.Items[0]["status"] != "GOOD" {
		t.Fatalf("unexpected single-server row: %#v", page.Items)
	}
}

func TestAQLWriteDetection(t *testing.T) {
	for _, query := range []string{
		"FOR doc IN people RETURN doc",
		"RETURN \"INSERT\"",
		"// REMOVE doc IN people\nRETURN 1",
		"/* UPDATE */ RETURN 1",
		"FOR d IN v SEARCH ANALYZER(d.text == 'x', 'text_en') RETURN d",
	} {
		if aqlWrites(query) {
			t.Fatalf("query should be read-only: %s", query)
		}
	}
	for _, query := range []string{
		"INSERT {name: 'a'} INTO people",
		"FOR doc IN people REMOVE doc IN people",
		"UPSERT {_key: 'a'} INSERT {} UPDATE {} IN people",
		"FOR doc IN people REPLACE doc WITH {} IN people",
	} {
		if !aqlWrites(query) {
			t.Fatalf("query should be flagged as a write: %s", query)
		}
	}
}

func TestSplitExplain(t *testing.T) {
	query, explain := splitExplain("EXPLAIN FOR doc IN people RETURN doc")
	if !explain || query != "FOR doc IN people RETURN doc" {
		t.Fatalf("unexpected explain split: %q %v", query, explain)
	}
	if _, explain := splitExplain("EXPLAINER"); explain {
		t.Fatal("EXPLAINER must not be treated as an explain prefix")
	}
	if _, explain := splitExplain("FOR doc IN people RETURN doc"); explain {
		t.Fatal("plain queries must not be treated as explain")
	}
}

func TestConsoleQueryGuardsStopBeforeNetwork(t *testing.T) {
	readOnly := &Session{opts: Options{ReadOnly: true, PageLimit: 10}}
	if _, err := readOnly.runConsoleQuery(context.Background(), "shop", sqldb.QueryRequest{Query: "INSERT {} INTO people"}); !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("read-only mode should forbid writes, got %v", err)
	}
	confirming := &Session{opts: Options{RequireConfirm: true, PageLimit: 10}}
	_, err := confirming.runConsoleQuery(context.Background(), "shop", sqldb.QueryRequest{Query: "REMOVE 'a' IN people"})
	var confirm confirmationError
	if !errors.As(err, &confirm) {
		t.Fatalf("expected a confirmation challenge, got %v", err)
	}
	if got := auditResult(err); got != plugin.AuditDenied {
		t.Fatalf("confirmation should audit as denied, got %v", got)
	}
	empty := &Session{opts: Options{PageLimit: 10}}
	if _, err := empty.runConsoleQuery(context.Background(), "shop", sqldb.QueryRequest{Query: "   "}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("empty query should be rejected, got %v", err)
	}
}

// TestQueryStreamKeepsSocketOpenOnError mirrors the PanelQueryEditor contract:
// the browser sends {"query":...,"confirm":...} frames and errors come back as
// frames rather than closing the socket.
func TestQueryStreamReportsErrorsAsFrames(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasPrefix(r.URL.Path, "/_api/version") {
			_, _ = w.Write([]byte(`{"server":"arango","version":"3.12.4"}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":true,"code":404,"errorNum":404,"errorMessage":"not found"}`))
	}))
	defer srv.Close()
	sess := connectTo(t, srv.URL)
	defer func() { _ = sess.Close() }()
	sess.opts.RequireConfirm = true

	frames := runStream(t, sess, map[string]string{"database": defaultDatabase},
		`{"query":"INSERT {} INTO people","confirm":false}`+"\n"+`{"query":"","confirm":false}`+"\n")
	if len(frames) != 2 {
		t.Fatalf("expected one frame per request, got %d: %#v", len(frames), frames)
	}
	if frames[0]["confirmRequired"] != true {
		t.Fatalf("write query should ask for confirmation: %#v", frames[0])
	}
	if _, ok := frames[1]["error"].(string); !ok {
		t.Fatalf("empty query should return an error frame: %#v", frames[1])
	}
}

func TestTabulateBuildsQueryEditorColumns(t *testing.T) {
	columns, rows := tabulate([]any{
		map[string]any{"name": "ada", "_key": "1", "_id": "people/1"},
		map[string]any{"name": "bob", "_key": "2", "_id": "people/2", "city": "Berlin"},
	})
	want := []string{"_key", "_id", "city", "name"}
	if strings.Join(columns, ",") != strings.Join(want, ",") {
		t.Fatalf("system attributes should sort first: %v", columns)
	}
	if len(rows) != 2 || len(rows[0]) != len(columns) {
		t.Fatalf("rows must be rectangular: %#v", rows)
	}
	if rows[0][2] != nil {
		t.Fatalf("missing attributes must be null: %#v", rows[0])
	}

	scalarColumns, scalarRows := tabulate([]any{1.0, 2.0})
	if len(scalarColumns) != 1 || scalarColumns[0] != "result" || len(scalarRows) != 2 {
		t.Fatalf("scalar results should use a single value column: %v %v", scalarColumns, scalarRows)
	}
}

func TestDocumentKeyRejectsWidenedMutations(t *testing.T) {
	if _, err := documentKey(map[string]any{"_key": "abc"}); err != nil {
		t.Fatalf("valid key rejected: %v", err)
	}
	for _, key := range []map[string]any{
		{},
		{"name": "abc"},
		{"_key": "abc", "name": "x"},
		{"_key": 7},
		{"_key": "bad key"},
	} {
		if _, err := documentKey(key); err == nil {
			t.Fatalf("key %#v must be rejected", key)
		}
	}
}

func TestWritableAttributesStripsServerManagedFields(t *testing.T) {
	s := &Session{opts: Options{RedactPatterns: []string{`(?i)password`}}}
	values, err := writableAttributes(s, map[string]any{
		"_key": "abc", "_id": "people/abc", "_rev": "_g1", "name": "Ada", "password": "***",
	}, true)
	if err != nil {
		t.Fatalf("writableAttributes: %v", err)
	}
	if _, ok := values["_id"]; ok {
		t.Fatalf("_id must never be written back: %#v", values)
	}
	if _, ok := values["_rev"]; ok {
		t.Fatalf("_rev must never be written back: %#v", values)
	}
	if _, ok := values["password"]; ok {
		t.Fatalf("redacted attributes must not be written back: %#v", values)
	}
	if values["_key"] != "abc" || values["name"] != "Ada" {
		t.Fatalf("unexpected writable values: %#v", values)
	}

	update, err := writableAttributes(s, map[string]any{"_key": "abc", "name": "Ada"}, false)
	if err != nil {
		t.Fatalf("writableAttributes: %v", err)
	}
	if _, ok := update["_key"]; ok {
		t.Fatalf("updates must not rewrite the key: %#v", update)
	}
	if _, err := writableAttributes(s, map[string]any{"bad.name": 1}, true); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("dotted attribute names must be rejected, got %v", err)
	}
}

func TestIdentifierValidation(t *testing.T) {
	// ArangoDB's extended naming constraints allow most UTF-8 characters,
	// leading digits, dots and inner spaces.
	for _, name := range []string{"people", "_users", "my-col_1", "1people", "a.b", "@abc123", "my collection",
		"España", "犬", "Бишкек", "😀", strings.Repeat("é", 128)} {
		if _, err := safeName(name, "collection"); err != nil {
			t.Fatalf("%q should be accepted: %v", name, err)
		}
	}
	// "/" and ":" are refused by ArangoDB itself; "%", "?", "#" and "+" would be
	// rewritten by the driver's URL round trip and address a different object.
	for _, name := range []string{"", "people/x", "people:x", "people%2Fx", "people?x=1", "people#x", "people+x",
		"people\x01", "people\x7f", "\xff\xfe", strings.Repeat("a", 257)} {
		if _, err := safeName(name, "collection"); err == nil {
			t.Fatalf("%q must be rejected", name)
		}
	}
	if _, err := safeDatabaseName(strings.Repeat("a", 129)); err == nil {
		t.Fatal("database names longer than 128 bytes must be rejected")
	}
	if _, err := safeDatabaseName("données"); err != nil {
		t.Fatalf("extended database name rejected: %v", err)
	}
	if _, err := safeAnalyzerName("_system::text_en"); err != nil {
		t.Fatalf("qualified analyzer name rejected: %v", err)
	}
	if _, err := safeMount("/my-service"); err != nil {
		t.Fatalf("mount rejected: %v", err)
	}
	for _, mount := range []string{"my-service", "/../etc", "/a//b", "/a b"} {
		if _, err := safeMount(mount); err == nil {
			t.Fatalf("mount %q must be rejected", mount)
		}
	}
	if err := validateDocumentID("people/alice"); err != nil {
		t.Fatalf("document id rejected: %v", err)
	}
	for _, id := range []string{"alice", "people", "people/al ice", "people/a/b"} {
		if err := validateDocumentID(id); err == nil {
			t.Fatalf("document id %q must be rejected", id)
		}
	}
}

func TestAQLIdentifierQuoting(t *testing.T) {
	if got := quote("my collection"); got != "`my collection`" {
		t.Fatalf("unexpected quoting: %s", got)
	}
	// A backtick has no escape inside a backtick-quoted name, so an extended
	// name containing one must not be able to close the quote.
	if got := quote("people`; RETURN 1"); got != "´people`; RETURN 1´" {
		t.Fatalf("backtick escaped the identifier quote: %s", got)
	}
	if got := aqlString("gra\"ph\\"); got != `"gra\"ph\\"` {
		t.Fatalf("graph name literal not escaped: %s", got)
	}
}

func TestFoxxReadmeSurfacesRealErrors(t *testing.T) {
	readme := func(t *testing.T, status int, body string) (any, error) {
		t.Helper()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if strings.HasPrefix(r.URL.Path, "/_api/version") {
				_, _ = w.Write([]byte(`{"server":"arango","version":"3.12.4","license":"community"}`))
				return
			}
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
		}))
		t.Cleanup(srv.Close)
		sess := connectTo(t, srv.URL)
		t.Cleanup(func() { _ = sess.Close() })
		return foxxReadme(plugin.NewRequestContext(context.Background(), plugin.User{}, sess,
			map[string]string{"database": defaultDatabase, "mount": "/my-service"}, nil, nil))
	}

	_, err := readme(t, http.StatusForbidden, `{"error":true,"code":403,"errorNum":11,"errorMessage":"Foxx API is disabled"}`)
	if !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("a disabled or denied Foxx API must surface, got %v", err)
	}
	if !strings.Contains(err.Error(), "Foxx API is disabled") {
		t.Fatalf("server message should survive: %v", err)
	}
	if _, err := readme(t, http.StatusNotFound, `{"error":true,"code":404,"errorNum":3009,"errorMessage":"service not found"}`); !errors.Is(err, plugin.ErrNotFound) {
		t.Fatalf("an unmounted service must surface as not found, got %v", err)
	}
	// 204 is the only "this service ships no README" answer.
	out, err := readme(t, http.StatusNoContent, "")
	if err != nil {
		t.Fatalf("foxxReadme: %v", err)
	}
	if text, _ := out.(string); !strings.Contains(text, "does not ship a README") {
		t.Fatalf("unexpected readme placeholder: %#v", out)
	}
}

func TestResourceIDRoundTrip(t *testing.T) {
	id := encodeID(kindFoxx, "_system", "/my-service")
	parts, err := decodeID(id, 3)
	if err != nil {
		t.Fatalf("decodeID: %v", err)
	}
	if parts[0] != kindFoxx || parts[1] != "_system" || parts[2] != "/my-service" {
		t.Fatalf("unexpected round trip: %#v", parts)
	}
	if _, err := decodeID("not-base64!!", 3); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("bad id should be rejected, got %v", err)
	}
	if _, err := decodeID(encodeID("a", "b"), 3); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("wrong arity should be rejected, got %v", err)
	}
}

func TestGraphBuilderPrunesDanglingEdges(t *testing.T) {
	builder := newGraphBuilder()
	builder.addVertex(map[string]any{"_id": "people/1", "_key": "1", "name": "Ada", "_rev": "x"})
	builder.addVertex(map[string]any{"_id": "people/1", "_key": "1", "name": "Ada"})
	builder.addEdge(map[string]any{"_id": "knows/1", "_from": "people/1", "_to": "people/2"})
	payload := builder.prune()
	if len(payload.Nodes) != 1 {
		t.Fatalf("vertices must be de-duplicated: %#v", payload.Nodes)
	}
	if _, ok := payload.Nodes[0].Properties["_rev"]; ok {
		t.Fatalf("_rev should not reach the graph panel: %#v", payload.Nodes[0].Properties)
	}
	if payload.Nodes[0].Label != "Ada" {
		t.Fatalf("label should prefer a name attribute: %#v", payload.Nodes[0])
	}
	if len(payload.Edges) != 0 {
		t.Fatalf("edges without both endpoints must be pruned: %#v", payload.Edges)
	}

	builder.addVertex(map[string]any{"_id": "people/2", "_key": "2"})
	builder.addEdge(map[string]any{"_id": "knows/2", "_from": "people/1", "_to": "people/2"})
	if len(builder.prune().Edges) != 1 {
		t.Fatalf("edge with both endpoints should survive: %#v", builder.payload.Edges)
	}
}

func TestTopologySceneIsKeyboardNavigableAndThemed(t *testing.T) {
	scene := &topologyScene{title: "social · _system", width: 800, height: 600}
	scene.load(
		[]topoNode{{Name: "people", Count: 3}, {Name: "companies", Count: 2}},
		[]topoEdge{{Collection: "works_at", From: "people", To: "companies", Count: 4}},
		"2 vertex collections",
	)
	if !scene.Handle(readyEvent(800, 600, plugin.PanelThemeDark)) {
		t.Fatal("ready event should mark the scene dirty")
	}
	dark := scene.Frame()
	if len(dark.Regions) != 2 {
		t.Fatalf("every vertex collection needs a hit region: %#v", dark.Regions)
	}
	for _, region := range dark.Regions {
		if region.Label == "" {
			t.Fatalf("regions must carry accessible labels: %#v", region)
		}
	}
	before := scene.selected
	if !scene.key(keyEvent("ArrowRight")) {
		t.Fatal("arrow keys should move the selection")
	}
	if scene.selected == before {
		t.Fatal("selection did not move")
	}
	if scene.announce == "" {
		t.Fatal("selection changes must be announced")
	}
	darkJSON := frameJSON(t, dark)
	scene.theme = plugin.PanelThemeLight
	lightJSON := frameJSON(t, scene.Frame())
	if darkJSON == lightJSON {
		t.Fatal("light and dark themes must render different palettes")
	}
	if !strings.Contains(darkJSON, "works_at") {
		t.Fatalf("edge definitions must be labelled: %s", darkJSON)
	}
}

func TestSettingRowsRedactSecrets(t *testing.T) {
	rows := settingRows(map[string]any{
		"apiToken": map[string]any{"title": "API token", "type": "string", "current": "super-secret", "required": true},
		"limit":    map[string]any{"title": "Limit", "type": "integer", "current": 10.0},
	}, []string{`(?i)token`})
	if len(rows) != 2 {
		t.Fatalf("expected two settings, got %#v", rows)
	}
	if rows[0]["current"] != sqldb.RedactedValue {
		t.Fatalf("secret-looking settings must be masked: %#v", rows[0])
	}
	if rows[1]["current"] != "10" {
		t.Fatalf("plain settings must survive: %#v", rows[1])
	}
}

func TestSavedQueryStorageRoundTrip(t *testing.T) {
	store := &fakeStorage{}
	ctx := context.Background()
	created := callStorageRoute(t, store, createSavedQuery, nil,
		[]byte(`{"name":"Top people","database":"shop","query":"FOR d IN people RETURN d","notes":"demo"}`))
	saved := created.(savedQueryRow)
	if saved.Writes {
		t.Fatalf("read-only AQL must not be flagged as writing: %#v", saved)
	}
	if !strings.HasPrefix(saved.ID, "query/") {
		t.Fatalf("saved queries should use a namespaced key: %q", saved.ID)
	}

	listed := callStorageRoute(t, store, listSavedQueries, nil, nil).(plugin.Page[row])
	if len(listed.Items) != 1 || listed.Items[0]["name"] != "Top people" {
		t.Fatalf("unexpected saved list: %#v", listed.Items)
	}

	read := callStorageRoute(t, store, readSavedQuery, map[string]string{"id": saved.ID}, nil).(savedQueryRow)
	if read.Query != "FOR d IN people RETURN d" {
		t.Fatalf("unexpected saved query: %#v", read)
	}

	// The saved grid declares sortable columns, so the list route must honour the
	// grid's sort instead of always ordering by name.
	callStorageRoute(t, store, createSavedQuery, nil, []byte(`{"name":"Aging stock","query":"REMOVE 'a' IN stock"}`))
	out, err := listSavedQueries(storageQueryContext(store, nil, map[string][]string{"sort": {"-name"}}, nil))
	if err != nil {
		t.Fatalf("listSavedQueries: %v", err)
	}
	sorted := out.(plugin.Page[row])
	if len(sorted.Items) != 2 || sorted.Items[0]["name"] != "Top people" {
		t.Fatalf("descending name sort not applied: %#v", sorted.Items)
	}
	if sorted.Items[1]["writes"] != true {
		t.Fatalf("a data-modifying saved query must be flagged: %#v", sorted.Items[1])
	}

	callStorageRoute(t, store, deleteSavedQuery, map[string]string{"id": saved.ID}, nil)
	if _, err := store.Get(ctx, plugin.UserStorage(savedCollection), saved.ID); !errors.Is(err, plugin.ErrNotFound) {
		t.Fatalf("saved query should be gone, got %v", err)
	}

	if _, err := createSavedQuery(storageContext(store, nil, []byte(`{"name":"","query":""}`))); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("empty saved query should be rejected, got %v", err)
	}
}

func TestParseEditorBodyAcceptsBothShapes(t *testing.T) {
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, nil, nil, nil, []byte(`{"content":"{\"level\":\"strict\"}"}`))
	body, err := parseEditorBody(rc, "schema")
	if err != nil || body["level"] != "strict" {
		t.Fatalf("code editor body not parsed: %#v %v", body, err)
	}
	rc = plugin.NewRequestContext(context.Background(), plugin.User{}, nil, nil, nil, []byte(`{"schema":{"level":"moderate"}}`))
	body, err = parseEditorBody(rc, "schema")
	if err != nil || body["level"] != "moderate" {
		t.Fatalf("typed body not parsed: %#v %v", body, err)
	}
	rc = plugin.NewRequestContext(context.Background(), plugin.User{}, nil, nil, nil, []byte(`{}`))
	if _, err := parseEditorBody(rc, "schema"); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("empty body should be rejected, got %v", err)
	}
}

func TestArangoErrorMapping(t *testing.T) {
	cases := map[int]error{
		401: plugin.ErrUnauthorized,
		403: plugin.ErrForbidden,
		404: plugin.ErrNotFound,
		409: plugin.ErrConflict,
		400: plugin.ErrInvalidInput,
		500: plugin.ErrUnavailable,
	}
	for code, want := range cases {
		err := arangoErr(arangoError(code, "boom"))
		if !errors.Is(err, want) {
			t.Fatalf("code %d mapped to %v, want %v", code, err, want)
		}
		if !strings.Contains(err.Error(), "boom") {
			t.Fatalf("server message should survive: %v", err)
		}
	}
	if arangoErr(nil) != nil {
		t.Fatal("nil must stay nil")
	}
}

func fieldMap(schema plugin.Schema) map[string]bool {
	out := map[string]bool{}
	for _, group := range schema.Groups {
		for _, field := range group.Fields {
			out[field.Key] = true
		}
	}
	return out
}

func visibleFields(schema plugin.Schema, overrides map[string]any) map[string]bool {
	values := schema.Defaults()
	for _, group := range schema.Groups {
		for _, field := range group.Fields {
			if _, ok := values[field.Key]; !ok {
				values[field.Key] = ""
			}
		}
	}
	for key, value := range overrides {
		values[key] = value
	}
	out := map[string]bool{}
	for key := range schema.VisibleValues(values, nil) {
		out[key] = true
	}
	return out
}

func connectTo(t *testing.T, endpoint string) *Session {
	t.Helper()
	sess, err := New().Connect(context.Background(), plugin.ConnectConfig{
		Config: map[string]any{"endpoint": endpoint, "auth": authNone, "database": defaultDatabase, "read_only": false},
		Net:    plugintest.DirectTransport(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return sess.(*Session)
}

func frameJSON(t *testing.T, frame any) string {
	t.Helper()
	data, err := json.Marshal(frame)
	if err != nil {
		t.Fatalf("marshal frame: %v", err)
	}
	return string(data)
}
