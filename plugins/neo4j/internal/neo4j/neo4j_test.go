package neo4j

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestManifestRegistersDirectOnlyAndCredentialKinds(t *testing.T) {
	p := New()
	m := p.Manifest()
	plugintest.ValidatePlugin(t, p)
	if m.Agent != nil {
		t.Fatal("Neo4j must not declare agent transport")
	}
	if len(m.SupportedTransports) != 1 || m.SupportedTransports[0] != plugin.TransportDirect {
		t.Fatalf("unexpected transports: %+v", m.SupportedTransports)
	}
	if !plugintest.CredentialKindSupported(m.Config, plugin.CredentialDBPassword) {
		t.Fatal("database password credential should support Neo4j")
	}
	if !plugintest.CredentialKindSupported(m.Config, plugin.CredentialBearerToken) {
		t.Fatal("bearer token credential should support Neo4j")
	}
	if plugintest.CredentialKindSupported(m.Config, plugin.CredentialTLSClientCert) {
		t.Fatal("Neo4j should not advertise TLS client certificate credentials")
	}
}

func TestConfigSchemaHasOnlyNeo4jFields(t *testing.T) {
	fields := fieldMap(configSchema())
	for _, key := range []string{"scheme", "host", "port", "database", "auth", "username", "credential_id", "password", "realm", "bearer_token", "bearer_credential_id", "ca_certificate", "read_only", "require_write_confirmation", "query_timeout", "connect_timeout", "retry_time", "pool_size", "fetch_size", "page_limit", "redact_properties"} {
		if !fields[key] {
			t.Fatalf("schema should expose %q", key)
		}
	}
	for _, key := range []string{"tls_mode", "client_cert_id", "auth_client_cert_id", "access_key_id", "secret_access_key", "endpoint", "keyspace"} {
		if fields[key] {
			t.Fatalf("schema should not expose unrelated field %q", key)
		}
	}
}

func TestConfigSchemaVisibleValuesAreAuthSpecific(t *testing.T) {
	schema := configSchema()
	tests := []struct {
		name   string
		values map[string]any
		want   []string
		reject []string
	}{
		{name: "password", values: map[string]any{"auth": authPassword, "scheme": "bolt"}, want: []string{"username", "password", "realm"}, reject: []string{"credential_id", "bearer_token", "bearer_credential_id", "ca_certificate"}},
		{name: "stored password", values: map[string]any{"auth": authCredential, "scheme": "bolt"}, want: []string{"credential_id"}, reject: []string{"username", "password", "realm", "bearer_token", "bearer_credential_id", "ca_certificate"}},
		{name: "bearer", values: map[string]any{"auth": authBearer, "scheme": "neo4j+s"}, want: []string{"bearer_token", "ca_certificate"}, reject: []string{"username", "password", "credential_id", "bearer_credential_id"}},
		{name: "stored bearer", values: map[string]any{"auth": authStoredBearer, "scheme": "bolt"}, want: []string{"bearer_credential_id"}, reject: []string{"username", "password", "credential_id", "bearer_token", "ca_certificate"}},
		{name: "none", values: map[string]any{"auth": authNone, "scheme": "bolt+ssc"}, want: []string{"auth", "scheme"}, reject: []string{"username", "password", "credential_id", "bearer_token", "bearer_credential_id", "ca_certificate"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			visible := visibleFields(schema, tt.values)
			for _, key := range tt.want {
				if !visible[key] {
					t.Fatalf("visible values should include %q in %#v", key, visible)
				}
			}
			for _, key := range tt.reject {
				if visible[key] {
					t.Fatalf("visible values should not include %q in %#v", key, visible)
				}
			}
		})
	}
}

func TestConfigSchemaHidesStaleCredentialRefsFromOtherAuthModes(t *testing.T) {
	schema := configSchema()
	tests := []struct {
		name   string
		values map[string]any
		want   []string
		reject []string
	}{
		{
			name:   "bearer ignores stale stored password",
			values: map[string]any{"auth": authBearer, "scheme": "bolt", credentialIDField: "cred-db"},
			want:   []string{"bearer_token"},
			reject: []string{"credential_id", "username", "password", "bearer_credential_id"},
		},
		{
			name:   "password ignores stale stored bearer",
			values: map[string]any{"auth": authPassword, "scheme": "bolt", bearerCredentialField: "cred-bearer"},
			want:   []string{"username", "password", "realm"},
			reject: []string{"credential_id", "bearer_token", "bearer_credential_id"},
		},
		{
			name:   "none ignores stale stored credentials",
			values: map[string]any{"auth": authNone, "scheme": "bolt", credentialIDField: "cred-db", bearerCredentialField: "cred-bearer"},
			want:   []string{"auth", "scheme"},
			reject: []string{"username", "password", "credential_id", "bearer_token", "bearer_credential_id"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			visible := visibleFields(schema, tt.values)
			for _, key := range tt.want {
				if !visible[key] {
					t.Fatalf("visible values should include %q in %#v", key, visible)
				}
			}
			for _, key := range tt.reject {
				if visible[key] {
					t.Fatalf("visible values should not include %q in %#v", key, visible)
				}
			}
		})
	}
}

func TestCypherSafety(t *testing.T) {
	for _, query := range []string{"MATCH (n) RETURN n", "SHOW INDEXES", "EXPLAIN CREATE (n)", "RETURN 'CREATE (n)' AS text"} {
		if cypherNeedsReview(query) {
			t.Fatalf("query should be read-only: %s", query)
		}
	}
	for _, query := range []string{"CREATE (n)", "MATCH (n) SET n.name = 'Ada'", "MATCH (n) DETACH DELETE n", "CALL dbms.listConfig()"} {
		if !cypherNeedsReview(query) {
			t.Fatalf("query should require review: %s", query)
		}
	}
}

func TestCypherSafetyStopsBeforeNetwork(t *testing.T) {
	_, err := executeCypher(context.Background(), &Session{opts: options{ReadOnly: true}}, defaultDatabase, sqldb.QueryRequest{Query: "CREATE (n)"})
	if !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("expected read-only forbidden error, got %v", err)
	}
	_, err = executeCypher(context.Background(), &Session{opts: options{RequireConfirm: true}}, defaultDatabase, sqldb.QueryRequest{Query: "MERGE (n:Person {id: 1})"})
	var confirmErr confirmationError
	if !errors.As(err, &confirmErr) {
		t.Fatalf("expected confirmation error, got %v", err)
	}
}

func TestResourceIDRoundTrip(t *testing.T) {
	id := mustEncodeID("node", "neo4j", "4:abc")
	kind, db, elementID, err := decodeID3(id)
	if err != nil {
		t.Fatalf("decode id: %v", err)
	}
	if kind != "node" || db != "neo4j" || elementID != "4:abc" {
		t.Fatalf("unexpected decoded id: %s %s %s", kind, db, elementID)
	}
}

func TestManifestReferencesResolve(t *testing.T) {
	p := New()
	m := p.Manifest()
	routeIDs := map[string]bool{}
	for _, r := range p.Routes() {
		routeIDs[r.ID] = true
	}
	actions := map[string]plugin.Action{}
	for _, action := range m.Actions {
		actions[action.ID] = action
		if !routeIDs[action.RouteID] {
			t.Fatalf("action %q points at missing route %q", action.ID, action.RouteID)
		}
	}
	for _, group := range m.Tree {
		if !routeIDs[group.Source.RouteID] {
			t.Fatalf("tree group %q points at missing route %q", group.Key, group.Source.RouteID)
		}
	}
	for _, res := range m.Resources {
		if !routeIDs[res.List.RouteID] {
			t.Fatalf("resource %q list points at missing route %q", res.Kind, res.List.RouteID)
		}
		for _, id := range append(append([]string{}, res.Actions.Detail...), append(res.Actions.Toolbar, res.Actions.Row...)...) {
			if _, ok := actions[id]; !ok {
				t.Fatalf("resource %q references missing action %q", res.Kind, id)
			}
		}
		for _, tab := range res.Detail.Tabs {
			if tab.Source != nil && !routeIDs[tab.Source.RouteID] {
				t.Fatalf("resource %q tab %q points at missing route %q", res.Kind, tab.Key, tab.Source.RouteID)
			}
		}
	}
}

func TestSetPropertiesQuery(t *testing.T) {
	props := map[string]any{"name": "Ada", "code": "MATCH (n) DETACH DELETE n"}

	q, params := setPropertiesQuery("n", "MATCH (n) WHERE elementId(n) = $id", "4:abc", props)
	if q != "MATCH (n) WHERE elementId(n) = $id SET n = $props" {
		t.Fatalf("unexpected node update query: %q", q)
	}
	if params["id"] != "4:abc" {
		t.Fatalf("id param not carried: %#v", params)
	}
	// Property values are parameterised, never interpolated into the statement —
	// a value that itself contains Cypher must not leak into the query text.
	got, ok := params["props"].(map[string]any)
	if !ok || got["code"] != props["code"] {
		t.Fatalf("props must be passed as a parameter map: %#v", params["props"])
	}
	if strings.Contains(q, "Ada") || strings.Contains(q, "DETACH") {
		t.Fatalf("query must not interpolate property values: %q", q)
	}

	rq, rparams := setPropertiesQuery("r", "MATCH ()-[r]->() WHERE elementId(r) = $id", "5:rel", props)
	if rq != "MATCH ()-[r]->() WHERE elementId(r) = $id SET r = $props" {
		t.Fatalf("unexpected relationship update query: %q", rq)
	}
	if rparams["id"] != "5:rel" {
		t.Fatalf("relationship id param not carried: %#v", rparams)
	}
}

func TestParsePropertiesContent(t *testing.T) {
	got, err := parsePropertiesContent(`{"name":"Ada","age":36}`)
	if err != nil {
		t.Fatalf("parse properties: %v", err)
	}
	if got["name"] != "Ada" {
		t.Fatalf("unexpected parsed properties: %#v", got)
	}

	empty, err := parsePropertiesContent("   ")
	if err != nil || len(empty) != 0 {
		t.Fatalf("blank content should yield empty map: %#v %v", empty, err)
	}

	if _, err := parsePropertiesContent(`[1,2,3]`); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("non-object JSON should be rejected, got %v", err)
	}
	if _, err := parsePropertiesContent(`{not json}`); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("invalid JSON should be rejected, got %v", err)
	}
}

func TestConstraintCreateQuery(t *testing.T) {
	tests := []struct {
		name   string
		entity string
		ctype  string
		label  string
		props  []string
		want   string
	}{
		{name: "unique single", entity: "node", ctype: "unique", label: "Person", props: []string{"email"}, want: "CREATE CONSTRAINT `c1` FOR (n:`Person`) REQUIRE n.`email` IS UNIQUE"},
		{name: "exists", entity: "node", ctype: "exists", label: "Person", props: []string{"name"}, want: "CREATE CONSTRAINT `c1` FOR (n:`Person`) REQUIRE n.`name` IS NOT NULL"},
		{name: "node key multi", entity: "node", ctype: "node_key", label: "Person", props: []string{"first", "last"}, want: "CREATE CONSTRAINT `c1` FOR (n:`Person`) REQUIRE (n.`first`, n.`last`) IS NODE KEY"},
		{name: "relationship", entity: "relationship", ctype: "exists", label: "KNOWS", props: []string{"since"}, want: "CREATE CONSTRAINT `c1` FOR ()-[r:`KNOWS`]-() REQUIRE r.`since` IS NOT NULL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := constraintCreateQuery("c1", tt.entity, tt.ctype, tt.label, tt.props)
			if err != nil {
				t.Fatalf("build constraint query: %v", err)
			}
			if q != tt.want {
				t.Fatalf("constraint query mismatch:\n got: %q\nwant: %q", q, tt.want)
			}
		})
	}

	if _, err := constraintCreateQuery("c1", "node", "unique", "Person", nil); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("missing properties should error, got %v", err)
	}
	if _, err := constraintCreateQuery("c1", "node", "bogus", "Person", []string{"x"}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("unknown constraint type should error, got %v", err)
	}
	if _, err := constraintCreateQuery("bad name", "node", "unique", "Person", []string{"x"}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("unsafe constraint name should error, got %v", err)
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
	visible := schema.VisibleValues(values, nil)
	out := map[string]bool{}
	for key := range visible {
		out[key] = true
	}
	return out
}
