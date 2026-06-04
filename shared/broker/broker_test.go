package broker

import (
	"context"
	"net/url"
	"testing"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type row map[string]any

func pageRC(t *testing.T, filter string) *plugin.RequestContext {
	t.Helper()
	q := url.Values{}
	if filter != "" {
		q.Set("filter", filter)
	}
	return plugin.NewRequestContext(context.Background(), plugin.User{ID: "u1"}, nil, nil, q, nil)
}

func sortRC(t *testing.T, sort string) *plugin.RequestContext {
	t.Helper()
	q := url.Values{}
	q.Set("sort", sort)
	return plugin.NewRequestContext(context.Background(), plugin.User{ID: "u1"}, nil, nil, q, nil)
}

func TestPageRowsSorts(t *testing.T) {
	rows := []row{{"name": "orders", "n": 3}, {"name": "events", "n": 1}, {"name": "Alerts", "n": 2}}

	asc, _ := PageRows(sortRC(t, "name"), rows)
	if asc.Items[0]["name"] != "Alerts" || asc.Items[2]["name"] != "orders" {
		t.Fatalf("name asc = %v", asc.Items)
	}
	desc, _ := PageRows(sortRC(t, "-n"), rows)
	if desc.Items[0]["n"] != 3 || desc.Items[2]["n"] != 1 {
		t.Fatalf("numeric desc = %v", desc.Items)
	}
}

func TestPageRowsFiltersMapRows(t *testing.T) {
	rows := []row{{"name": "orders"}, {"name": "events"}, {"name": "order-dlq"}}

	page, err := PageRows(pageRC(t, "order"), rows)
	if err != nil {
		t.Fatalf("PageRows: %v", err)
	}
	if len(page.Items) != 2 || *page.Total != 2 {
		t.Fatalf("filter order = %d items (total %d), want 2", len(page.Items), *page.Total)
	}

	// Blank filter returns everything.
	all, _ := PageRows(pageRC(t, ""), rows)
	if len(all.Items) != 3 {
		t.Fatalf("no filter = %d items, want 3", len(all.Items))
	}
}
