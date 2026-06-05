package opensearch

import (
	"testing"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestOpenSearchManifestValidates(t *testing.T) {
	p := New()
	proj := plugintest.Projection(t, p)
	if proj.Category.Key != plugin.CategorySearch {
		t.Fatalf("category: got %q want %q", proj.Category.Key, plugin.CategorySearch)
	}
	if proj.Layout != plugin.LayoutSidebarTree {
		t.Fatalf("layout: got %q", proj.Layout)
	}
	if len(proj.Resources) != 2 {
		t.Fatalf("resources: got %d", len(proj.Resources))
	}
}

func TestOpenSearchConfigSchemaIsSearchSpecific(t *testing.T) {
	fields := fieldMap(New().Manifest().Config)
	for _, key := range []string{"endpoint", "auth", "username", "password", "api_key", "bearer_token", "credential_id", "tls_mode"} {
		if !fields[key] {
			t.Fatalf("missing field %q", key)
		}
	}
	for _, key := range []string{"brokers", "urls", "management_url"} {
		if fields[key] {
			t.Fatalf("opensearch should not expose %q", key)
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
