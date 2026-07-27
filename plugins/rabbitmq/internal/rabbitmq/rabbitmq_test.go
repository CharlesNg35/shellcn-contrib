package rabbitmq

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func testSession(managementURL string) *Session {
	return &Session{client: &http.Client{}, opts: options{ManagementURL: managementURL, VHost: "/"}}
}

func TestListPagedKeepsOffsetCursorsAndPushesSort(t *testing.T) {
	var got url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.URL.Query()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items":          []map[string]any{{"name": "q1", "vhost": "/", "messages": 1}},
			"page":           4,
			"page_count":     10,
			"filtered_count": 500,
		})
	}))
	defer srv.Close()
	s := testSession(srv.URL)

	query := url.Values{"limit": {"50"}, "cursor": {"150"}, "sort": {"-messages"}}
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, s, nil, query, nil)
	page, err := listPaged(rc, s, "/api/queues/"+apiVHost("/"), queueListColumns)
	if err != nil {
		t.Fatalf("listPaged: %v", err)
	}
	if got.Get("page") != "4" || got.Get("page_size") != "50" {
		t.Fatalf("row offset 150 asked for page=%q page_size=%q, want 4/50", got.Get("page"), got.Get("page_size"))
	}
	if got.Get("sort") != "messages" || got.Get("sort_reverse") != "true" {
		t.Fatalf("sort was not pushed to the broker: sort=%q sort_reverse=%q", got.Get("sort"), got.Get("sort_reverse"))
	}
	if page.NextCursor != "200" {
		t.Fatalf("next cursor: got %q want the next row offset %q", page.NextCursor, "200")
	}
	if page.Total == nil || *page.Total != 500 {
		t.Fatalf("total: got %v want 500", page.Total)
	}
}

func TestListPagedAcceptsZeroCursorAndFiltersUnnamedItems(t *testing.T) {
	var got url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.URL.Query()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{
				{"consumer_tag": "ctag-1", "queue": map[string]any{"name": "orders"}},
				{"consumer_tag": "ctag-2", "queue": map[string]any{"name": "billing"}},
			},
			"page":           1,
			"page_count":     1,
			"filtered_count": 2,
		})
	}))
	defer srv.Close()
	s := testSession(srv.URL)

	query := url.Values{"cursor": {"0"}, "filter": {"BILLING"}}
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, s, nil, query, nil)
	page, err := listPaged(rc, s, "/api/consumers/"+apiVHost("/"), consumerListColumns)
	if err != nil {
		t.Fatalf("listPaged: %v", err)
	}
	if got.Get("page") != "1" {
		t.Fatalf("cursor 0 is the first page, got page=%q", got.Get("page"))
	}
	if got.Has("name") {
		t.Fatalf("consumers carry no name field, but name=%q was sent", got.Get("name"))
	}
	if len(page.Items) != 1 || page.Items[0]["consumer_tag"] != "ctag-2" {
		t.Fatalf("search kept %#v, want only ctag-2", page.Items)
	}
}

func TestExchangesPageDropsDefaultExchangeFromTheTotal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{
				{"name": "", "vhost": "/"},
				{"name": "amq.direct", "vhost": "/"},
			},
			"page":           1,
			"page_count":     1,
			"filtered_count": 2,
		})
	}))
	defer srv.Close()
	s := testSession(srv.URL)

	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, s, nil, nil, nil)
	page, err := exchangesPage(rc)
	if err != nil {
		t.Fatalf("exchangesPage: %v", err)
	}
	if len(page.Items) != 1 {
		t.Fatalf("items: %#v, want the default exchange dropped", page.Items)
	}
	if page.Total == nil || *page.Total != 1 {
		t.Fatalf("total: got %v want 1 once the default exchange is dropped", page.Total)
	}
}
