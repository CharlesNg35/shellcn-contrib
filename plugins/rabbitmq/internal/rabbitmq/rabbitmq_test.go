package rabbitmq

import (
	"testing"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestRabbitMQManifestValidates(t *testing.T) {
	p := New()
	proj := plugintest.Projection(t, p)
	if proj.Category.Key != plugin.CategoryMessaging {
		t.Fatalf("category: got %q want %q", proj.Category.Key, plugin.CategoryMessaging)
	}
	if proj.Layout != plugin.LayoutSidebarTree {
		t.Fatalf("layout: got %q", proj.Layout)
	}
	if len(proj.Resources) != 2 {
		t.Fatalf("resources: got %d", len(proj.Resources))
	}
}

func TestRabbitMQConfigSchemaIsSpecific(t *testing.T) {
	m := New().Manifest()
	fields := fieldMap(m.Config)
	for _, key := range []string{"management_url", "vhost", "auth", "username", "password", "credential_id"} {
		if !fields[key] {
			t.Fatalf("missing field %q", key)
		}
	}
	for _, key := range []string{"brokers", "urls", "token"} {
		if fields[key] {
			t.Fatalf("rabbitmq should not expose %q", key)
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
