package dynamodb

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

func TestManifestRegistersAndStaysDirectOnly(t *testing.T) {
	reg := plugin.NewRegistry()
	if err := reg.Register(New()); err != nil {
		t.Fatalf("register DynamoDB plugin: %v", err)
	}
	m, ok := reg.Manifest(protocolName)
	if !ok {
		t.Fatal("manifest not registered")
	}
	if m.Agent != nil {
		t.Fatal("DynamoDB must not declare agent transport")
	}
	if len(m.SupportedTransports) != 1 || m.SupportedTransports[0] != plugin.TransportDirect {
		t.Fatalf("unexpected transports: %+v", m.SupportedTransports)
	}
	if !reg.CredentialKindSupportsProtocol(plugin.CredentialCloudAccessKey, protocolName) {
		t.Fatal("cloud access key credential should support DynamoDB")
	}
	for _, kind := range []plugin.CredentialKind{plugin.CredentialDBPassword, plugin.CredentialTLSClientCert, plugin.CredentialBasicAuth, plugin.CredentialBearerToken} {
		if reg.CredentialKindSupportsProtocol(kind, protocolName) {
			t.Fatalf("DynamoDB should not advertise %s credentials", kind)
		}
	}
}

func TestConfigSchemaHasOnlyDynamoDBFields(t *testing.T) {
	fields := fieldMap(configSchema())
	for _, key := range []string{"region", "endpoint", "table_prefix", "auth", "access_key_id", "secret_access_key", "session_token", "credential_id", "tls_mode", "ca_certificate", "read_only", "confirm_writes", "timeout", "page_limit"} {
		if !fields[key] {
			t.Fatalf("schema should expose %q", key)
		}
	}
	for _, key := range []string{"username", "password", "database", "host", "port", "api_key", "bearer_token", "query_language", "client_cert_id", "auth_client_cert_id"} {
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
		{
			name:   "access key",
			values: map[string]any{"region": "us-east-1", "auth": "access_key", "access_key_id": "akid", "secret_access_key": "secret", "session_token": ""},
			want:   []string{"region", "auth", "access_key_id", "secret_access_key", "session_token", "tls_mode", "read_only", "confirm_writes", "timeout", "page_limit"},
			reject: []string{"credential_id"},
		},
		{
			name:   "stored credential",
			values: map[string]any{"region": "us-east-1", "auth": "credential", "credential_id": "cred-1", "session_token": ""},
			want:   []string{"region", "auth", "credential_id", "session_token", "tls_mode", "read_only", "confirm_writes", "timeout", "page_limit"},
			reject: []string{"access_key_id", "secret_access_key"},
		},
		{
			name:   "provider chain",
			values: map[string]any{"region": "us-east-1", "auth": "default_chain"},
			want:   []string{"region", "auth", "tls_mode", "read_only", "confirm_writes", "timeout", "page_limit"},
			reject: []string{"access_key_id", "secret_access_key", "session_token", "credential_id"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			visible := schema.VisibleValues(schema.ValuesWithDefaults(tt.values), nil)
			for _, key := range tt.want {
				if _, ok := visible[key]; !ok {
					t.Fatalf("visible values should include %q in %#v", key, visible)
				}
			}
			for _, key := range tt.reject {
				if _, ok := visible[key]; ok {
					t.Fatalf("visible values should not include %q in %#v", key, visible)
				}
			}
		})
	}
}

func TestPartiQLSafetyStopsBeforeNetwork(t *testing.T) {
	_, err := executePartiQL(context.Background(), &Session{opts: Options{ReadOnly: true}}, sqldb.QueryRequest{Query: `DELETE FROM "users" WHERE pk='1'`})
	if !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("expected read-only forbidden error, got %v", err)
	}
	_, err = executePartiQL(context.Background(), &Session{opts: Options{ConfirmWrites: true}}, sqldb.QueryRequest{Query: `INSERT INTO "users" VALUE {'pk':'1'}`})
	var confirmErr confirmationError
	if !errors.As(err, &confirmErr) {
		t.Fatalf("expected confirmation error, got %v", err)
	}
}

func TestNormalizePartiQLStatementLimit(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		wantSQL   string
		wantLimit int32
		wantErr   bool
	}{
		{name: "no limit", statement: `SELECT * FROM "users"`, wantSQL: `SELECT * FROM "users"`, wantLimit: 100},
		{name: "trailing limit", statement: `SELECT * FROM "users" LIMIT 25`, wantSQL: `SELECT * FROM "users"`, wantLimit: 25},
		{name: "trailing semicolon", statement: `SELECT * FROM "users" LIMIT 25;`, wantSQL: `SELECT * FROM "users"`, wantLimit: 25},
		{name: "quoted text", statement: `SELECT * FROM "users" WHERE note='keep LIMIT 25'`, wantSQL: `SELECT * FROM "users" WHERE note='keep LIMIT 25'`, wantLimit: 100},
		{name: "bad limit", statement: `SELECT * FROM "users" LIMIT 0`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSQL, gotLimit, err := normalizePartiQLStatement(tt.statement, 100)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("normalize statement: %v", err)
			}
			if gotSQL != tt.wantSQL || gotLimit != tt.wantLimit {
				t.Fatalf("got (%q, %d), want (%q, %d)", gotSQL, gotLimit, tt.wantSQL, tt.wantLimit)
			}
		})
	}
}

func TestAttributeValueKeyRoundTrip(t *testing.T) {
	key := map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "user#1"},
		"sk": &types.AttributeValueMemberN{Value: "42"},
	}
	id, err := encodeItemID("users", key)
	if err != nil {
		t.Fatalf("encode item id: %v", err)
	}
	table, decoded, err := decodeItemID(id)
	if err != nil {
		t.Fatalf("decode item id: %v", err)
	}
	if table != "users" || keyDisplay(decoded, []types.KeySchemaElement{{AttributeName: strptr("pk"), KeyType: types.KeyTypeHash}, {AttributeName: strptr("sk"), KeyType: types.KeyTypeRange}}) != "pk=user#1 · sk=42" {
		t.Fatalf("unexpected decoded key: table=%s key=%#v", table, decoded)
	}
}

func TestManifestReferencesResolve(t *testing.T) {
	p := New()
	m := p.Manifest()
	routeIDs := map[string]bool{}
	for _, r := range p.Routes() {
		routeIDs[r.ID] = true
	}
	actionByID := map[string]plugin.Action{}
	for _, action := range m.Actions {
		actionByID[action.ID] = action
		if !routeIDs[action.RouteID] {
			t.Fatalf("action %q points at missing route %q", action.ID, action.RouteID)
		}
	}
	resourceByKind := map[string]plugin.ResourceType{}
	for _, res := range m.Resources {
		resourceByKind[res.Kind] = res
	}
	for _, group := range m.Tree {
		if group.Source.RouteID != "" && !routeIDs[group.Source.RouteID] {
			t.Fatalf("tree group %q points at missing route %q", group.Key, group.Source.RouteID)
		}
		if group.Source.RouteID == "" {
			res, ok := resourceByKind[group.ResourceKind]
			if !ok || !routeIDs[res.List.RouteID] {
				t.Fatalf("tree group %q points at missing resource kind %q", group.Key, group.ResourceKind)
			}
		}
	}
	for _, res := range m.Resources {
		if !routeIDs[res.List.RouteID] {
			t.Fatalf("resource %q list points at missing route %q", res.Kind, res.List.RouteID)
		}
		for _, id := range append(append([]string{}, res.Actions.Detail...), append(res.Actions.Toolbar, res.Actions.Row...)...) {
			if _, ok := actionByID[id]; !ok {
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

func fieldMap(schema plugin.Schema) map[string]bool {
	out := map[string]bool{}
	for _, group := range schema.Groups {
		for _, field := range group.Fields {
			out[field.Key] = true
		}
	}
	return out
}

func strptr(s string) *string { return &s }
