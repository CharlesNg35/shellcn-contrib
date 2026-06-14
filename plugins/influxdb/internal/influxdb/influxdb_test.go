package influxdb

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestManifestRegistersAndStaysDirectOnly(t *testing.T) {
	p := New()
	m := p.Manifest()
	plugintest.ValidatePlugin(t, p)
	if m.Agent != nil {
		t.Fatal("InfluxDB must not declare agent transport")
	}
	if len(m.SupportedTransports) != 1 || m.SupportedTransports[0] != plugin.TransportDirect {
		t.Fatalf("unexpected transports: %+v", m.SupportedTransports)
	}
	for _, kind := range []plugin.CredentialKind{plugin.CredentialKindAPIToken, plugin.CredentialKindBasicAuth} {
		if !plugintest.CredentialKindSupported(m.Config, kind) {
			t.Fatalf("%s credential should support InfluxDB", kind)
		}
	}
	if plugintest.CredentialKindSupported(m.Config, plugin.CredentialKindBearerToken) {
		t.Fatal("InfluxDB should use API token credentials instead of bearer token credentials")
	}
	if plugintest.CredentialKindSupported(m.Config, plugin.CredentialKindDBPassword) {
		t.Fatal("InfluxDB should not advertise database password credentials")
	}
}

func TestConfigSchemaIsVersionAware(t *testing.T) {
	fields := fieldMap(configSchema())
	for _, key := range []string{"api_mode", "endpoint", "org", "database", "auth_v3", "auth_v2", "auth_v1", tokenFieldV3, tokenCredV3, usernameFieldV3, passwordFieldV3, basicCredV3, tokenFieldV2, tokenCredV2, usernameFieldV1, passwordFieldV1, basicCredV1, "query_language_v3", "tls_mode", "read_only", "confirm_writes"} {
		if !fields[key] {
			t.Fatalf("schema should expose field %q", key)
		}
	}
	for _, key := range []string{"brokers", "keyspace", "management_url", "bucket", "query_language", "api_token", "token_credential_id", "basic_credential_id"} {
		if fields[key] {
			t.Fatalf("schema should not expose unrelated or ambiguous field %q", key)
		}
	}
}

func TestConfigSchemaVisibleValuesAreModeSpecific(t *testing.T) {
	schema := configSchema()
	tests := []struct {
		name   string
		values map[string]any
		want   []string
		reject []string
	}{
		{
			name:   "v3 token",
			values: map[string]any{"api_mode": modeV3, "endpoint": "http://localhost:8181", "database": "metrics", "auth_v3": "token", tokenFieldV3: "secret"},
			want:   []string{"api_mode", "endpoint", "database", "auth_v3", tokenFieldV3, "tls_mode", "query_language_v3", "timeout", "page_limit", "read_only", "confirm_writes"},
			reject: []string{"org", "auth_v2", "auth_v1", tokenFieldV2, tokenCredV2, usernameFieldV1, passwordFieldV1, basicCredV1, "lookback"},
		},
		{
			name:   "v2 token",
			values: map[string]any{"api_mode": modeV2, "endpoint": "http://localhost:8086", "org": "shellcn", "auth_v2": "token", tokenFieldV2: "secret"},
			want:   []string{"api_mode", "endpoint", "org", "auth_v2", tokenFieldV2, "tls_mode", "lookback", "timeout", "page_limit", "read_only", "confirm_writes"},
			reject: []string{"database", "auth_v3", "auth_v1", tokenFieldV3, tokenCredV3, usernameFieldV3, passwordFieldV3, basicCredV3, usernameFieldV1, passwordFieldV1, basicCredV1, "query_language_v3"},
		},
		{
			name:   "v1 basic",
			values: map[string]any{"api_mode": modeV1, "endpoint": "http://localhost:8086", "database": "metrics", "auth_v1": "basic", usernameFieldV1: "root", passwordFieldV1: "secret"},
			want:   []string{"api_mode", "endpoint", "database", "auth_v1", usernameFieldV1, passwordFieldV1, "tls_mode", "timeout", "page_limit", "read_only", "confirm_writes"},
			reject: []string{"org", "auth_v3", "auth_v2", tokenFieldV3, tokenCredV3, usernameFieldV3, passwordFieldV3, basicCredV3, tokenFieldV2, tokenCredV2, "query_language_v3", "lookback"},
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

func TestQuerySafetyStopsBeforeNetwork(t *testing.T) {
	_, err := executeQuery(context.Background(), &Session{opts: Options{Mode: modeV3, ReadOnly: true, QueryLanguage: "sql"}}, "metrics", sqldb.QueryRequest{Query: "delete from cpu"})
	if !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("expected read-only forbidden error, got %v", err)
	}
	_, err = executeQuery(context.Background(), &Session{opts: Options{Mode: modeV3, ConfirmWrites: true, QueryLanguage: "sql"}}, "metrics", sqldb.QueryRequest{Query: "drop table cpu"})
	var confirmErr confirmationError
	if !errors.As(err, &confirmErr) {
		t.Fatalf("expected confirmation error, got %v", err)
	}
	if got := queryAuditResult(err); got != plugin.AuditDenied {
		t.Fatalf("confirmation should audit as denied, got %s", got)
	}
}

func TestParseAnnotatedCSVRows(t *testing.T) {
	rows, err := parseCSVRows([]byte(`#datatype,string,long,dateTime:RFC3339,string,string,double
#group,false,false,false,true,true,false
#default,_result,,,,,
,result,table,_time,_measurement,host,_value
,,0,2026-05-27T00:00:00Z,cpu,web01,0.64
`))
	if err != nil {
		t.Fatalf("parse csv: %v", err)
	}
	if len(rows) != 1 || rows[0]["_measurement"] != "cpu" || rows[0]["host"] != "web01" {
		t.Fatalf("unexpected rows: %#v", rows)
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
	for _, a := range m.Actions {
		actionByID[a.ID] = a
		if !routeIDs[a.RouteID] {
			t.Fatalf("action %q points at missing route %q", a.ID, a.RouteID)
		}
	}
	for _, g := range m.Tree {
		if !routeIDs[g.Source.RouteID] {
			t.Fatalf("tree group %q points at missing route %q", g.Key, g.Source.RouteID)
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
			for _, id := range actionIDs(tab.Config) {
				if _, ok := actionByID[id]; !ok {
					t.Fatalf("resource %q tab %q references missing action %q", res.Kind, tab.Key, id)
				}
			}
		}
	}
	if strings.TrimSpace(m.Icon.Value) == "" || m.Icon.Type != plugin.IconSVG {
		t.Fatalf("InfluxDB should use an inline SVG icon: %#v", m.Icon)
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

func actionIDs(config plugin.PanelConfig) []string {
	if tc, ok := config.(plugin.TableConfig); ok {
		return append(append([]string{}, tc.ActionIDs...), tc.RowActionIDs...)
	}
	return nil
}
