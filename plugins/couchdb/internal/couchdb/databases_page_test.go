package couchdb

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

// pagedCouch serves /_all_dbs the way CouchDB does, honouring limit and
// startkey, and records how many database info documents were ever requested.
type pagedCouch struct {
	names []string

	mu       sync.Mutex
	allDBs   []url.Values
	infoReqs int
}

func (p *pagedCouch) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(r.URL.Path, "/")
	switch {
	case path == "_all_dbs":
		query := r.URL.Query()
		p.mu.Lock()
		p.allDBs = append(p.allDBs, query)
		p.mu.Unlock()

		names := append([]string{}, p.names...)
		sort.Strings(names)
		desc := query.Get("descending") == "true"
		if desc {
			slices.Reverse(names)
		}
		if raw := query.Get("startkey"); raw != "" {
			var start string
			_ = json.Unmarshal([]byte(raw), &start)
			at := slices.IndexFunc(names, func(name string) bool {
				if desc {
					return name <= start
				}
				return name >= start
			})
			if at < 0 {
				at = len(names)
			}
			names = names[at:]
		}
		if raw := query.Get("skip"); raw != "" {
			if skip, err := strconv.Atoi(raw); err == nil {
				names = names[min(skip, len(names)):]
			}
		}
		if raw := query.Get("limit"); raw != "" {
			if limit, err := strconv.Atoi(raw); err == nil && limit < len(names) {
				names = names[:limit]
			}
		}
		writeTestJSON(w, names)
	case path == "_dbs_info":
		var body struct {
			Keys []string `json:"keys"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		p.mu.Lock()
		p.infoReqs += len(body.Keys)
		p.mu.Unlock()
		out := make([]any, 0, len(body.Keys))
		for _, key := range body.Keys {
			out = append(out, map[string]any{"key": key, "info": map[string]any{
				"db_name": key, "doc_count": float64(1), "sizes": map[string]any{"file": float64(10), "active": float64(5)},
			}})
		}
		writeTestJSON(w, out)
	default:
		writeTestJSON(w, map[string]any{"couchdb": "Welcome", "version": "3.5.2"})
	}
}

func writeTestJSON(w http.ResponseWriter, body any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}

// TestListDatabasesPagesInsteadOfEnumerating proves one page costs one bounded
// /_all_dbs read plus one /_dbs_info batch, never an info read per database.
func TestListDatabasesPagesInsteadOfEnumerating(t *testing.T) {
	fake := &pagedCouch{}
	for i := 0; i < 500; i++ {
		fake.names = append(fake.names, "db-"+strconv.Itoa(1000+i))
	}
	srv := httptest.NewServer(fake)
	defer srv.Close()

	sess := connectFake(t, New(), srv.URL, plugintest.DirectTransport())
	defer func() { _ = sess.Close() }()

	query := url.Values{"limit": []string{"10"}}
	rc := plugin.NewRequestContext(t.Context(), plugin.User{}, sess, nil, query, nil)
	first, err := listDatabases(rc)
	if err != nil {
		t.Fatalf("list databases: %v", err)
	}
	page, ok := first.(plugin.Page[row])
	if !ok {
		t.Fatalf("unexpected page type %T", first)
	}
	if len(page.Items) != 10 {
		t.Fatalf("first page = %d rows, want 10", len(page.Items))
	}
	if page.NextCursor != "db-1010" {
		t.Fatalf("next cursor = %q, want db-1010", page.NextCursor)
	}
	if stringOf(page.Items[0]["name"]) != "db-1000" {
		t.Fatalf("first row = %v, want db-1000", page.Items[0]["name"])
	}

	query.Set("cursor", page.NextCursor)
	second, err := listDatabases(plugin.NewRequestContext(t.Context(), plugin.User{}, sess, nil, query, nil))
	if err != nil {
		t.Fatalf("list databases page 2: %v", err)
	}
	rows := second.(plugin.Page[row]).Items
	if len(rows) != 10 || stringOf(rows[0]["name"]) != "db-1010" {
		t.Fatalf("second page = %d rows starting at %v", len(rows), rows[0]["name"])
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.infoReqs != 20 {
		t.Fatalf("info documents read = %d, want 20 (the two pages only)", fake.infoReqs)
	}
	for _, values := range fake.allDBs {
		if values.Get("limit") != "11" {
			t.Fatalf("/_all_dbs must be bounded, got limit=%q", values.Get("limit"))
		}
	}
}

func databaseNames(t *testing.T, page any) []string {
	t.Helper()
	rows, ok := page.(plugin.Page[row])
	if !ok {
		t.Fatalf("unexpected page type %T", page)
	}
	out := make([]string, 0, len(rows.Items))
	for _, item := range rows.Items {
		out = append(out, stringOf(item["name"]))
	}
	return out
}

// TestListDatabasesSearchesEveryName proves the grid's filter box reaches a
// database that lives far past the first fetched window, and that the matches
// page by offset with a real total once the walk has seen every name.
func TestListDatabasesSearchesEveryName(t *testing.T) {
	fake := &pagedCouch{}
	for i := 0; i < 500; i++ {
		fake.names = append(fake.names, "db-"+strconv.Itoa(1000+i))
	}
	srv := httptest.NewServer(fake)
	defer srv.Close()

	sess := connectFake(t, New(), srv.URL, plugintest.DirectTransport())
	defer func() { _ = sess.Close() }()

	query := url.Values{"limit": []string{"10"}, "filter": []string{"db-1499"}}
	found, err := listDatabases(plugin.NewRequestContext(t.Context(), plugin.User{}, sess, nil, query, nil))
	if err != nil {
		t.Fatalf("filtered list: %v", err)
	}
	if names := databaseNames(t, found); len(names) != 1 || names[0] != "db-1499" {
		t.Fatalf("filter db-1499 = %v, want [db-1499]", names)
	}
	page := found.(plugin.Page[row])
	if page.NextCursor != "" {
		t.Fatalf("single match must end the page, got cursor %q", page.NextCursor)
	}
	if page.Total == nil || *page.Total != 1 {
		t.Fatalf("filtered total = %v, want 1", page.Total)
	}

	query.Set("filter", "DB-14")
	first, err := listDatabases(plugin.NewRequestContext(t.Context(), plugin.User{}, sess, nil, query, nil))
	if err != nil {
		t.Fatalf("case-insensitive list: %v", err)
	}
	page = first.(plugin.Page[row])
	if page.Total == nil || *page.Total != 100 {
		t.Fatalf("filtered total = %v, want 100", page.Total)
	}
	if page.NextCursor != "10" {
		t.Fatalf("filtered cursor = %q, want the row offset 10", page.NextCursor)
	}
	query.Set("cursor", page.NextCursor)
	second, err := listDatabases(plugin.NewRequestContext(t.Context(), plugin.User{}, sess, nil, query, nil))
	if err != nil {
		t.Fatalf("filtered page 2: %v", err)
	}
	if names := databaseNames(t, second); len(names) != 10 || names[0] != "db-1410" {
		t.Fatalf("filtered page 2 = %v, want 10 rows from db-1410", names)
	}
}

// TestListDatabasesHonoursNameSort proves the one column CouchDB can order is
// ordered by the server, not within the page that happened to be fetched.
func TestListDatabasesHonoursNameSort(t *testing.T) {
	fake := &pagedCouch{}
	for i := 0; i < 500; i++ {
		fake.names = append(fake.names, "db-"+strconv.Itoa(1000+i))
	}
	srv := httptest.NewServer(fake)
	defer srv.Close()

	sess := connectFake(t, New(), srv.URL, plugintest.DirectTransport())
	defer func() { _ = sess.Close() }()

	query := url.Values{"limit": []string{"10"}, "sort": []string{"-name"}}
	first, err := listDatabases(plugin.NewRequestContext(t.Context(), plugin.User{}, sess, nil, query, nil))
	if err != nil {
		t.Fatalf("descending list: %v", err)
	}
	names := databaseNames(t, first)
	if len(names) != 10 || names[0] != "db-1499" || names[9] != "db-1490" {
		t.Fatalf("descending page = %v, want db-1499..db-1490", names)
	}
	page := first.(plugin.Page[row])
	if page.NextCursor != "db-1489" {
		t.Fatalf("descending cursor = %q, want db-1489", page.NextCursor)
	}
	query.Set("cursor", page.NextCursor)
	second, err := listDatabases(plugin.NewRequestContext(t.Context(), plugin.User{}, sess, nil, query, nil))
	if err != nil {
		t.Fatalf("descending page 2: %v", err)
	}
	if names := databaseNames(t, second); len(names) != 10 || names[0] != "db-1489" {
		t.Fatalf("descending page 2 = %v, want 10 rows from db-1489", names)
	}
}

// TestListDatabasesAcceptsOffsetCursor pins the grid's fallback cursor: when the
// panel has no remembered key it sends the row offset, which must still serve
// that row window rather than the head of the list.
func TestListDatabasesAcceptsOffsetCursor(t *testing.T) {
	fake := &pagedCouch{}
	for i := 0; i < 500; i++ {
		fake.names = append(fake.names, "db-"+strconv.Itoa(1000+i))
	}
	srv := httptest.NewServer(fake)
	defer srv.Close()

	sess := connectFake(t, New(), srv.URL, plugintest.DirectTransport())
	defer func() { _ = sess.Close() }()

	query := url.Values{"limit": []string{"10"}, "cursor": []string{"50"}}
	page, err := listDatabases(plugin.NewRequestContext(t.Context(), plugin.User{}, sess, nil, query, nil))
	if err != nil {
		t.Fatalf("offset cursor: %v", err)
	}
	if names := databaseNames(t, page); len(names) != 10 || names[0] != "db-1050" {
		t.Fatalf("offset cursor 50 = %v, want 10 rows from db-1050", names)
	}
}

// TestListDatabasesReportsTotalWhenTheServerFits keeps the paginator's row count
// and page jumps alive on deployments that fit in a single window.
func TestListDatabasesReportsTotalWhenTheServerFits(t *testing.T) {
	fake := &pagedCouch{names: []string{"alpha", "beta", "gamma"}}
	srv := httptest.NewServer(fake)
	defer srv.Close()

	sess := connectFake(t, New(), srv.URL, plugintest.DirectTransport())
	defer func() { _ = sess.Close() }()

	query := url.Values{"limit": []string{"10"}}
	page, err := listDatabases(plugin.NewRequestContext(t.Context(), plugin.User{}, sess, nil, query, nil))
	if err != nil {
		t.Fatalf("list databases: %v", err)
	}
	total := page.(plugin.Page[row]).Total
	if total == nil || *total != 3 {
		t.Fatalf("total = %v, want 3", total)
	}
}
