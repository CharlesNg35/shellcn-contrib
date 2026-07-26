package couchdb

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
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
		if raw := query.Get("startkey"); raw != "" {
			var start string
			_ = json.Unmarshal([]byte(raw), &start)
			names = names[sort.SearchStrings(names, start):]
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
