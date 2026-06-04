package meilisearch

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/charlesng35/shellcn-contrib/shared/searchrest"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

// wrappedSession mimics the core's borrowed session.Handle: a plugin.Session
// that exposes the live session via Session().
type wrappedSession struct{ inner plugin.Session }

func (w wrappedSession) Session() plugin.Session           { return w.inner }
func (w wrappedSession) HealthCheck(context.Context) error { return nil }
func (w wrappedSession) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}
func (w wrappedSession) Close() error { return nil }

func TestUnwrapResolvesThroughHandleWrapper(t *testing.T) {
	inner := &Session{}
	if got, err := unwrap(inner); err != nil || got != inner {
		t.Fatalf("bare session: got %v, err %v", got, err)
	}
	if got, err := unwrap(wrappedSession{inner: inner}); err != nil || got != inner {
		t.Fatalf("wrapped session must resolve to the inner session: got %v, err %v", got, err)
	}
}

// newMockSession points a real Session at an httptest server so route handlers
// run against representative API JSON without a live Meilisearch.
func newMockSession(t *testing.T, h http.HandlerFunc) (*Session, func()) {
	t.Helper()
	srv := httptest.NewServer(h)
	s := &Session{
		client: searchrest.New(searchrest.Options{Endpoint: srv.URL}),
		opts:   Options{PageLimit: 100},
	}
	return s, srv.Close
}

// Meilisearch returns /tasks "next" as a number (or null); decoding it must not
// fail, which previously 500'd the tasks list/tree.
func TestListTasksDecodesNumericNextCursor(t *testing.T) {
	s, closeSrv := newMockSession(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/tasks" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"results":[{"uid":3,"type":"indexCreation","status":"succeeded"}],"total":1,"limit":20,"from":3,"next":2}`))
	})
	defer closeSrv()

	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, s, nil, url.Values{}, nil)
	res, err := treeTasks(rc)
	if err != nil {
		t.Fatalf("treeTasks: %v", err)
	}
	page := res.(plugin.Page[plugin.TreeNode])
	if page.NextCursor != "2" {
		t.Fatalf("next cursor = %q, want %q", page.NextCursor, "2")
	}
	if len(page.Items) != 1 {
		t.Fatalf("items = %#v", page.Items)
	}
}

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
		t.Fatalf("meilisearch should be direct-only: %+v", m.SupportedTransports)
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

func TestValidTaskUID(t *testing.T) {
	for _, in := range []string{"0", "42", " 7 "} {
		if _, err := validTaskUID(in); err != nil {
			t.Fatalf("validTaskUID(%q) unexpected error: %v", in, err)
		}
	}
	for _, in := range []string{"", "-1", "abc", "1.5", "0x10"} {
		if _, err := validTaskUID(in); err == nil {
			t.Fatalf("validTaskUID(%q) should reject", in)
		}
	}
}

func TestValidKeyUID(t *testing.T) {
	if got, err := validKeyUID(" 6062abda-a5aa-4414-ac91-ecd7944c0f8d "); err != nil || got != "6062abda-a5aa-4414-ac91-ecd7944c0f8d" {
		t.Fatalf("validKeyUID valid: got %q err %v", got, err)
	}
	for _, in := range []string{"", "not-a-uuid", "6062abda-a5aa-4414-ac91", "default"} {
		if _, err := validKeyUID(in); err == nil {
			t.Fatalf("validKeyUID(%q) should reject", in)
		}
	}
}

func TestKeyUpdateBody(t *testing.T) {
	name := " Renamed "
	desc := " new desc "
	body, err := keyUpdateBody(&name, &desc)
	if err != nil {
		t.Fatalf("keyUpdateBody: %v", err)
	}
	if body["name"] != "Renamed" || body["description"] != "new desc" {
		t.Fatalf("body trimmed wrong: %#v", body)
	}
	only := "x"
	if body, err := keyUpdateBody(&only, nil); err != nil || len(body) != 1 || body["name"] != "x" {
		t.Fatalf("name-only body: %#v err %v", body, err)
	}
	if _, err := keyUpdateBody(nil, nil); err == nil {
		t.Fatalf("empty payload should be rejected")
	}
}

func TestUpdateKeyPatchesNameAndDescription(t *testing.T) {
	var gotMethod, gotPath, gotBody string
	s, closeSrv := newMockSession(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		buf := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(buf)
		gotBody = string(buf)
		_, _ = w.Write([]byte(`{"uid":"6062abda-a5aa-4414-ac91-ecd7944c0f8d","name":"Renamed"}`))
	})
	defer closeSrv()

	body := []byte(`{"name":"Renamed","description":"d"}`)
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, s, map[string]string{"key": "6062abda-a5aa-4414-ac91-ecd7944c0f8d"}, url.Values{}, body)
	if _, err := updateKey(rc); err != nil {
		t.Fatalf("updateKey: %v", err)
	}
	if gotMethod != http.MethodPatch || gotPath != "/keys/6062abda-a5aa-4414-ac91-ecd7944c0f8d" {
		t.Fatalf("request = %s %s", gotMethod, gotPath)
	}
	if !strings.Contains(gotBody, `"name":"Renamed"`) || !strings.Contains(gotBody, `"description":"d"`) {
		t.Fatalf("body = %s", gotBody)
	}
}

func TestUpdateKeyRejectsBadUID(t *testing.T) {
	s, closeSrv := newMockSession(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Error("should not reach server")
		w.WriteHeader(http.StatusOK)
	})
	defer closeSrv()
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, s, map[string]string{"key": "bad"}, url.Values{}, []byte(`{"name":"x"}`))
	if _, err := updateKey(rc); err == nil {
		t.Fatalf("expected invalid uid error")
	}
}

func TestDeleteTaskFiltersByUID(t *testing.T) {
	var gotMethod, gotPath, gotQuery string
	s, closeSrv := newMockSession(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath, gotQuery = r.Method, r.URL.Path, r.URL.RawQuery
		_, _ = w.Write([]byte(`{"taskUid":99,"status":"enqueued","type":"taskDeletion"}`))
	})
	defer closeSrv()
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, s, map[string]string{"task": "12"}, url.Values{}, nil)
	if _, err := deleteTask(rc); err != nil {
		t.Fatalf("deleteTask: %v", err)
	}
	if gotMethod != http.MethodDelete || gotPath != "/tasks" || gotQuery != "uids=12" {
		t.Fatalf("request = %s %s ?%s", gotMethod, gotPath, gotQuery)
	}
}

func TestDeleteTaskRejectsBadUID(t *testing.T) {
	s, closeSrv := newMockSession(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Error("should not reach server")
		w.WriteHeader(http.StatusOK)
	})
	defer closeSrv()
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, s, map[string]string{"task": "abc"}, url.Values{}, nil)
	if _, err := deleteTask(rc); err == nil {
		t.Fatalf("expected invalid uid error")
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
	assertArrayItemKeys(t, p, "meilisearch.key.create", "actions", nil)
	assertArrayItemKeys(t, p, "meilisearch.key.create", "indexes", nil)
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
