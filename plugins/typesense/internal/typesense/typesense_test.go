package typesense

import (
	"testing"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

func TestManifest(t *testing.T) {
	p := New()
	m := p.Manifest()
	if err := plugin.Validate(m, p.Routes()); err != nil {
		t.Fatalf("manifest should validate: %v", err)
	}
	if m.Category != plugin.CategorySearch {
		t.Fatalf("category: got %q want %q", m.Category, plugin.CategorySearch)
	}
	if len(m.SupportedTransports) != 1 || m.SupportedTransports[0] != plugin.TransportDirect {
		t.Fatalf("typesense should be direct-only: %+v", m.SupportedTransports)
	}
	fields := fieldMap(m.Config)
	for _, key := range []string{"endpoint", "auth", "api_key", "credential_id", "tls_mode", "read_only", "page_limit"} {
		if !fields[key] {
			t.Fatalf("missing field %q", key)
		}
	}
	for _, key := range []string{"username", "password", "bearer_token"} {
		if fields[key] {
			t.Fatalf("unexpected field %q", key)
		}
	}
}

func TestSynonymAndCurationRoutesAreGlobal(t *testing.T) {
	routes := routeMap(New().Routes())
	for id, path := range map[string]string{
		rid("synonyms.list"):   "/synonym_sets",
		rid("synonym.upsert"):  "/synonym_sets/{synonym}",
		rid("synonym.delete"):  "/synonym_sets/{synonym}",
		rid("overrides.list"):  "/curation_sets",
		rid("override.upsert"): "/curation_sets/{override}",
		rid("override.delete"): "/curation_sets/{override}",
	} {
		route, ok := routes[id]
		if !ok {
			t.Fatalf("missing route %q", id)
		}
		if route.Path != path {
			t.Fatalf("%s path: got %q want %q", id, route.Path, path)
		}
	}
}

func fieldMap(schema plugin.Schema) map[string]bool {
	fields := map[string]bool{}
	for _, group := range schema.Groups {
		for _, field := range group.Fields {
			fields[field.Key] = true
		}
	}
	return fields
}

func TestStructuredArrayFields(t *testing.T) {
	p := New()
	assertArrayItemKeys(t, p, "typesense.collection.create", "fields", []string{"name", "type", "facet", "optional", "sort", "index"})
	assertArrayItemKeys(t, p, "typesense.key.create", "actions", nil)
	assertArrayItemKeys(t, p, "typesense.key.create", "collections", nil)
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
