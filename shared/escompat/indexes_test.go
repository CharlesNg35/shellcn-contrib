package escompat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

// catServer answers _cat/indices with a cluster of the requested size and
// records the request the plugin actually issued.
type catServer struct {
	indexes int
	path    string
	query   url.Values
	calls   int
}

func (c *catServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.calls++
	c.path, c.query = r.URL.Path, r.URL.Query()
	rows := make([]map[string]any, 0, c.indexes)
	for i := range c.indexes {
		rows = append(rows, map[string]any{
			"index":        fmt.Sprintf("logs-%05d", i),
			"health":       "green",
			"status":       "open",
			"uuid":         fmt.Sprintf("uuid-%05d", i),
			"pri":          "1",
			"rep":          "1",
			"docs.count":   "12",
			"docs.deleted": "0",
			"store.size":   "2048",
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(rows)
}

func newCatSession(t *testing.T, indexes int) (*Session, *catServer) {
	t.Helper()
	fake := &catServer{indexes: indexes}
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	return &Session{
		client: &Client{http: srv.Client(), endpoint: srv.URL},
		opts:   Options{Timeout: 5 * time.Second, PageLimit: defaultPageLimit},
	}, fake
}

func indexRequest(t *testing.T, s *Session, query url.Values) *plugin.RequestContext {
	t.Helper()
	return plugin.NewRequestContext(t.Context(), plugin.User{}, s, nil, query, nil)
}

func TestListIndexesReturnsOneBoundedPage(t *testing.T) {
	s, fake := newCatSession(t, 5000)
	out, err := listIndexes(indexRequest(t, s, url.Values{"limit": {"25"}}))
	if err != nil {
		t.Fatalf("listIndexes: %v", err)
	}
	page, ok := out.(plugin.Page[row])
	if !ok {
		t.Fatalf("listIndexes returned %T, want plugin.Page[row]", out)
	}
	if len(page.Items) != 25 {
		t.Fatalf("page has %d rows, want 25", len(page.Items))
	}
	if page.NextCursor != "25" {
		t.Fatalf("nextCursor = %q, want 25", page.NextCursor)
	}
	if page.Total != nil {
		t.Fatalf("index listings must not count the cluster, got total %d", *page.Total)
	}
	if page.Items[0]["docs_count"] != int64(12) || page.Items[0]["store_size"] != int64(2048) {
		t.Fatalf("row rewrite lost numeric columns: %#v", page.Items[0])
	}
	if got := fake.query.Get("h"); got != indexListColumns {
		t.Fatalf("h = %q, want %q", got, indexListColumns)
	}
	if got := fake.query.Get("s"); got != "index" {
		t.Fatalf("s = %q, want index", got)
	}
	if fake.path != "/_cat/indices" {
		t.Fatalf("path = %q, want /_cat/indices", fake.path)
	}
}

func TestListIndexesResumesFromCursor(t *testing.T) {
	s, _ := newCatSession(t, 120)
	out, err := listIndexes(indexRequest(t, s, url.Values{"limit": {"10"}, "cursor": {"110"}}))
	if err != nil {
		t.Fatalf("listIndexes: %v", err)
	}
	page := out.(plugin.Page[row])
	if len(page.Items) != 10 {
		t.Fatalf("page has %d rows, want 10", len(page.Items))
	}
	if page.Items[0]["index"] != "logs-00110" {
		t.Fatalf("cursor ignored, first row is %v", page.Items[0]["index"])
	}
	if page.NextCursor != "" {
		t.Fatalf("last page should end the listing: %#v", page)
	}
}

// TestListIndexesHonorsTheRequestedLimit pins that the connection's page_limit is
// not a ceiling on an explicit client request: a table asking for 250 rows must
// not silently receive a short page it cannot tell apart from the last one.
func TestListIndexesHonorsTheRequestedLimit(t *testing.T) {
	s, _ := newCatSession(t, 5000)
	out, err := listIndexes(indexRequest(t, s, url.Values{"limit": {"250"}}))
	if err != nil {
		t.Fatalf("listIndexes: %v", err)
	}
	page := out.(plugin.Page[row])
	if len(page.Items) != 250 {
		t.Fatalf("page has %d rows, want the requested 250", len(page.Items))
	}
	if page.NextCursor != "250" {
		t.Fatalf("nextCursor = %q, want 250", page.NextCursor)
	}
}

// TestListIndexesPagesPastAnyScanDepth pins that a deep offset keeps returning
// rows and a continue cursor: a large cluster must stay walkable to its end.
func TestListIndexesPagesPastAnyScanDepth(t *testing.T) {
	s, _ := newCatSession(t, 6000)
	out, err := listIndexes(indexRequest(t, s, url.Values{"limit": {"50"}, "cursor": {"4000"}}))
	if err != nil {
		t.Fatalf("listIndexes: %v", err)
	}
	page := out.(plugin.Page[row])
	if len(page.Items) != 50 || page.Items[0]["index"] != "logs-04000" {
		t.Fatalf("deep page truncated: %d rows starting at %v", len(page.Items), page.Items[0]["index"])
	}
	if page.NextCursor != "4050" {
		t.Fatalf("nextCursor = %q, want 4050", page.NextCursor)
	}
}

func TestListIndexesPushesFilterAndSortToTheServer(t *testing.T) {
	s, fake := newCatSession(t, 3)
	query := url.Values{"filter": {"payments"}, "sort": {"-docs_count"}}
	if _, err := listIndexes(indexRequest(t, s, query)); err != nil {
		t.Fatalf("listIndexes: %v", err)
	}
	if fake.path != "/_cat/indices/*payments*" {
		t.Fatalf("path = %q, want the filter as an index pattern", fake.path)
	}
	if got := fake.query.Get("s"); got != "docs.count:desc" {
		t.Fatalf("s = %q, want docs.count:desc", got)
	}
}

func TestTreeIndexesFetchesOnlyTheNameColumn(t *testing.T) {
	s, fake := newCatSession(t, 4)
	out, err := treeIndexes(indexRequest(t, s, url.Values{"limit": {"2"}}))
	if err != nil {
		t.Fatalf("treeIndexes: %v", err)
	}
	page := out.(plugin.Page[plugin.TreeNode])
	if len(page.Items) != 2 || page.Items[0].Label != "logs-00000" {
		t.Fatalf("unexpected tree page: %#v", page.Items)
	}
	if page.NextCursor != "2" {
		t.Fatalf("nextCursor = %q, want 2", page.NextCursor)
	}
	if got := fake.query.Get("h"); got != indexTreeColumns {
		t.Fatalf("h = %q, want %q", got, indexTreeColumns)
	}
	if fake.calls != 1 {
		t.Fatalf("tree made %d cluster calls, want 1", fake.calls)
	}
}

func TestIndexPatternSanitizesTheFilterTerm(t *testing.T) {
	cases := map[string]string{
		"":            "",
		"*":           "",
		"logs":        "*logs*",
		"Logs":        "*logs*",
		"logs-*":      "*logs-*",
		"a,b":         "*ab*",
		"we ird/name": "*weirdname*",
	}
	for in, want := range cases {
		if got := indexPattern(in); got != want {
			t.Errorf("indexPattern(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestIndexSortParamIgnoresUnknownColumns(t *testing.T) {
	if got := indexSortParam([]plugin.SortKey{{Field: "nope"}}); got != "index" {
		t.Fatalf("unknown sort key = %q, want index", got)
	}
	got := indexSortParam([]plugin.SortKey{{Field: "health"}, {Field: "store_size", Desc: true}})
	if got != "health,store.size:desc" {
		t.Fatalf("sort = %q", got)
	}
}
