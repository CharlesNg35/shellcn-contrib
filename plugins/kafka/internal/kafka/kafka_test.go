package kafka

import (
	"errors"
	"testing"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

func TestKafkaManifestValidates(t *testing.T) {
	reg := plugin.NewRegistry()
	reg.MustRegister(New())

	proj, ok := reg.Projection(protocolName)
	if !ok {
		t.Fatal("projection missing")
	}
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

func TestKafkaConfigSchemaIsSpecific(t *testing.T) {
	m := New().Manifest()
	fields := fieldMap(m.Config)
	for _, key := range []string{"brokers", "client_id", "auth", "username", "password", "credential_id"} {
		if !fields[key] {
			t.Fatalf("missing field %q", key)
		}
	}
	for _, key := range []string{"management_url", "urls", "token"} {
		if fields[key] {
			t.Fatalf("kafka should not expose %q", key)
		}
	}
}

func TestValidatePartitionCount(t *testing.T) {
	cases := []struct {
		name           string
		topic          string
		count, current int32
		wantErr        bool
	}{
		{name: "increase ok", topic: "t", count: 6, current: 3},
		{name: "equal rejected", topic: "t", count: 3, current: 3, wantErr: true},
		{name: "decrease rejected", topic: "t", count: 2, current: 3, wantErr: true},
		{name: "empty topic rejected", topic: " ", count: 6, current: 3, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validatePartitionCount(tc.topic, tc.count, tc.current)
			if tc.wantErr {
				if !errors.Is(err, plugin.ErrInvalidInput) {
					t.Fatalf("want ErrInvalidInput, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateConfigEntry(t *testing.T) {
	cases := []struct {
		name, key, value string
		wantErr          bool
	}{
		{name: "ok", key: "retention.ms", value: "60000"},
		{name: "empty key", key: " ", value: "60000", wantErr: true},
		{name: "empty value", key: "retention.ms", value: "", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateConfigEntry(tc.key, tc.value)
			if tc.wantErr {
				if !errors.Is(err, plugin.ErrInvalidInput) {
					t.Fatalf("want ErrInvalidInput, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestStructuredMapFields(t *testing.T) {
	p := New()
	for _, tc := range []struct{ routeID, fieldKey string }{
		{"kafka.topic.create", "config"},
		{"kafka.message.produce", "headers"},
	} {
		field := routeField(t, p, tc.routeID, tc.fieldKey)
		if field.Type != plugin.FieldMap {
			t.Fatalf("%s.%s is %q, want map", tc.routeID, tc.fieldKey, field.Type)
		}
		if field.Item == nil || field.Item.Type != plugin.FieldText {
			t.Fatalf("%s.%s value item is not text", tc.routeID, tc.fieldKey)
		}
	}
}

func routeField(t *testing.T, p plugin.Plugin, routeID, fieldKey string) *plugin.Field {
	t.Helper()
	for _, r := range p.Routes() {
		if r.ID != routeID || r.Input == nil {
			continue
		}
		for _, g := range r.Input.Groups {
			for i := range g.Fields {
				if g.Fields[i].Key == fieldKey {
					return &g.Fields[i]
				}
			}
		}
	}
	t.Fatalf("route %q has no field %q", routeID, fieldKey)
	return nil
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
