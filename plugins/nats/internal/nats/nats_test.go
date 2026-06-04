package nats

import (
	"context"
	"testing"
	"time"

	natsclient "github.com/nats-io/nats.go"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

func ptr[T any](v T) *T { return &v }

func TestApplyStreamUpdatePreservesUnsetFields(t *testing.T) {
	current := natsclient.StreamConfig{
		Name:     "ORDERS",
		Subjects: []string{"orders.*"},
		Storage:  natsclient.MemoryStorage,
		Replicas: 1,
		MaxMsgs:  100,
		MaxBytes: -1,
		MaxAge:   time.Hour,
	}
	got, err := applyStreamUpdate(current, streamUpdateRequest{MaxMsgs: ptr(int64(500))})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got.MaxMsgs != 500 {
		t.Fatalf("max_msgs: got %d want 500", got.MaxMsgs)
	}
	if got.Name != "ORDERS" || got.Storage != natsclient.MemoryStorage {
		t.Fatalf("immutable fields changed: %#v", got)
	}
	if got.MaxBytes != -1 || got.MaxAge != time.Hour || got.Replicas != 1 {
		t.Fatalf("unset fields changed: %#v", got)
	}
	if len(got.Subjects) != 1 || got.Subjects[0] != "orders.*" {
		t.Fatalf("subjects changed: %#v", got.Subjects)
	}
}

func TestApplyStreamUpdateOverlaysProvidedFields(t *testing.T) {
	current := natsclient.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.*"}, Replicas: 1}
	got, err := applyStreamUpdate(current, streamUpdateRequest{
		Subjects: ptr("orders.*, payments.>"),
		Replicas: ptr(3),
		MaxBytes: ptr(int64(2048)),
		MaxAge:   ptr("30m"),
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(got.Subjects) != 2 {
		t.Fatalf("subjects: %#v", got.Subjects)
	}
	if got.Replicas != 3 || got.MaxBytes != 2048 || got.MaxAge != 30*time.Minute {
		t.Fatalf("overlay failed: %#v", got)
	}
}

func TestApplyStreamUpdateRejectsInvalid(t *testing.T) {
	current := natsclient.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.*"}}
	cases := []streamUpdateRequest{
		{Subjects: ptr("   ")},
		{Replicas: ptr(0)},
		{Replicas: ptr(6)},
		{MaxAge: ptr("not-a-duration")},
		{MaxAge: ptr("-5m")},
	}
	for i, req := range cases {
		if _, err := applyStreamUpdate(current, req); err == nil {
			t.Fatalf("case %d: expected error", i)
		}
	}
}

func TestParseMaxAge(t *testing.T) {
	for _, raw := range []string{"", "0", "  "} {
		if d, err := parseMaxAge(raw); err != nil || d != 0 {
			t.Fatalf("parseMaxAge(%q): got %v, %v", raw, d, err)
		}
	}
	if d, err := parseMaxAge("2h30m"); err != nil || d != 2*time.Hour+30*time.Minute {
		t.Fatalf("parseMaxAge duration: got %v, %v", d, err)
	}
}

func TestNATSManifestValidates(t *testing.T) {
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

func TestNATSConfigSchemaIsSpecific(t *testing.T) {
	m := New().Manifest()
	fields := fieldMap(m.Config)
	for _, key := range []string{"urls", "name", "auth", "username", "password", "token", "credential_id"} {
		if !fields[key] {
			t.Fatalf("missing field %q", key)
		}
	}
	for _, key := range []string{"management_url", "brokers"} {
		if fields[key] {
			t.Fatalf("nats should not expose %q", key)
		}
	}
}

func TestHealthCheckContextAddsDeadline(t *testing.T) {
	ctx, cancel := healthCheckContext(context.Background(), 25*time.Millisecond)
	defer cancel()
	if _, ok := ctx.Deadline(); !ok {
		t.Fatal("health check context should have a deadline")
	}
}

func TestHealthCheckContextKeepsExistingDeadline(t *testing.T) {
	base, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	want, _ := base.Deadline()
	ctx, done := healthCheckContext(base, 25*time.Millisecond)
	defer done()
	got, ok := ctx.Deadline()
	if !ok {
		t.Fatal("health check context should keep the caller deadline")
	}
	if !got.Equal(want) {
		t.Fatalf("deadline changed: got %v want %v", got, want)
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
