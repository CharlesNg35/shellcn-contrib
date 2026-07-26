package chroma

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	contribtest "github.com/charlesng35/shellcn-contrib/shared/plugintest"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

// TestChromaBoundedPagingIntegration seeds several pages worth of collections and
// records on a real server and proves the listing routes return one bounded page
// per call: the outbound Chroma request is capped at the asked-for page, the
// cursor walks distinct pages until it terminates, and nothing pulls the whole
// collection set or record set in one fetch.
func TestChromaBoundedPagingIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_CHROMA_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_CHROMA_INTEGRATION=1 to run against Chroma")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	const (
		collectionCount = 25
		collectionLimit = 10
		recordCount     = 240
		recordLimit     = 25
	)

	suffix := strings.ReplaceAll(time.Now().UTC().Format("20060102150405.000000"), ".", "")
	database := "shellcn_pg_db_" + suffix

	config := chromaConfig(ctx, t)
	endpoint, _ := config["endpoint"].(string)
	if endpoint == "" {
		t.Fatalf("no Chroma endpoint resolved: %#v", config)
	}
	rec := recordChroma(t, endpoint)
	config["endpoint"] = rec.URL()

	p := New()
	sess, err := p.Connect(ctx, plugin.ConnectConfig{Config: config, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	h := contribtest.NewHarness(t, p.Routes())

	h.Call(ctx, rid("database.create"), sess, nil, nil, mustJSON(t, map[string]any{"name": database}))
	defer h.CallNoFail(context.Background(), rid("database.delete"), sess, map[string]string{"database": database})

	scoped := map[string]string{"tenant": defaultTenant, "database": database}
	seededCollections := make(map[string]bool, collectionCount)
	firstCollectionID := ""
	for i := range collectionCount {
		name := fmt.Sprintf("shellcn_pg_col_%s_%03d", suffix, i)
		created := asMap(t, h.Call(ctx, rid("collection.create"), sess, nil, nil, mustJSON(t, map[string]any{
			"name": name, "space": spaceCosine, "database": database,
		})))
		id, _ := created["id"].(string)
		if id == "" {
			t.Fatalf("collection create returned no id: %#v", created)
		}
		seededCollections[name] = true
		if firstCollectionID == "" {
			firstCollectionID = id
		}
		defer h.CallNoFail(context.Background(), rid("collection.delete"), sess,
			map[string]string{"tenant": defaultTenant, "database": database, "collection": id})
	}

	// Collections: one bounded page per call, cursor-walked to exhaustion.
	rec.reset()
	seen := map[string]bool{}
	cursor := ""
	pages := 0
	for {
		query := url.Values{"limit": []string{strconv.Itoa(collectionLimit)}}
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		page := decodePage(t, h.Call(ctx, rid("collections.list"), sess, scoped, query, nil))
		pages++
		if pages > collectionCount {
			t.Fatalf("collection paging did not terminate after %d pages", pages)
		}
		remaining := collectionCount - len(seen)
		want := min(remaining, collectionLimit)
		if len(page.Items) != want {
			t.Fatalf("page %d: expected a bounded page of %d collections, got %d", pages, want, len(page.Items))
		}
		if page.Total == nil || *page.Total != collectionCount {
			t.Fatalf("page %d: expected the server collection count %d, got %v", pages, collectionCount, page.Total)
		}
		for _, item := range page.Items {
			name, _ := item["name"].(string)
			if !seededCollections[name] {
				t.Fatalf("page %d returned an unseeded collection %q", pages, name)
			}
			if seen[name] {
				t.Fatalf("collection %q was returned on two pages", name)
			}
			seen[name] = true
		}
		if page.NextCursor == "" {
			break
		}
		if want := strconv.Itoa(len(seen)); page.NextCursor != want {
			t.Fatalf("page %d: expected cursor %q, got %q", pages, want, page.NextCursor)
		}
		cursor = page.NextCursor
	}
	if pages != 3 {
		t.Fatalf("expected %d collections to page in 3 calls of %d, got %d", collectionCount, collectionLimit, pages)
	}
	if len(seen) != collectionCount {
		t.Fatalf("paging covered %d of %d collections", len(seen), collectionCount)
	}

	// Each page fetched exactly one bounded window from Chroma: limit+1 to probe
	// for a next page, offset walking the cursor. A whole-list fetch would show a
	// single request with a limit far above the page size.
	fetches := rec.collectionFetches(database)
	if len(fetches) != pages {
		t.Fatalf("expected one collections fetch per page, got %d for %d pages", len(fetches), pages)
	}
	for i, call := range fetches {
		if got := call.Query.Get("limit"); got != strconv.Itoa(collectionLimit+1) {
			t.Fatalf("fetch %d asked Chroma for limit %q, want %q (unbounded fetch)", i, got, strconv.Itoa(collectionLimit+1))
		}
		wantOffset := ""
		if i > 0 {
			wantOffset = strconv.Itoa(i * collectionLimit)
		}
		if got := call.Query.Get("offset"); got != wantOffset {
			t.Fatalf("fetch %d asked Chroma for offset %q, want %q", i, got, wantOffset)
		}
	}

	// Search and sort span every page, so that branch scans once up to the cap and
	// still hands back a single bounded page.
	rec.reset()
	sorted := decodePage(t, h.Call(ctx, rid("collections.list"), sess, scoped, url.Values{
		"limit": []string{strconv.Itoa(collectionLimit)}, "sort": []string{"-name"}, "filter": []string{suffix},
	}, nil))
	if len(sorted.Items) != collectionLimit || sorted.NextCursor != strconv.Itoa(collectionLimit) {
		t.Fatalf("sorted listing is not bounded: %d items, cursor %q", len(sorted.Items), sorted.NextCursor)
	}
	if sorted.Truncated {
		t.Fatalf("%d collections must not trip the %d scan cap", collectionCount, collectionScanLimit)
	}
	if name, _ := sorted.Items[0]["name"].(string); !strings.HasSuffix(name, fmt.Sprintf("%03d", collectionCount-1)) {
		t.Fatalf("descending sort should start at the last collection, got %q", name)
	}
	sortedFetches := rec.collectionFetches(database)
	if len(sortedFetches) != 1 {
		t.Fatalf("expected one scan fetch for a sorted listing, got %d", len(sortedFetches))
	}
	if got := sortedFetches[0].Query.Get("limit"); got != strconv.Itoa(collectionScanLimit+1) {
		t.Fatalf("sorted scan asked for limit %q, want the %d scan cap", got, collectionScanLimit)
	}

	// Records: seed several pages into the first collection.
	records := map[string]string{"tenant": defaultTenant, "database": database, "collection": firstCollectionID}
	seededRecords := make(map[string]bool, recordCount)
	for start := 0; start < recordCount; start += 80 {
		ids := make([]string, 0, 80)
		embeddings := make([][]float64, 0, 80)
		documents := make([]string, 0, 80)
		for i := start; i < start+80 && i < recordCount; i++ {
			id := fmt.Sprintf("rec-%04d", i)
			ids = append(ids, id)
			embeddings = append(embeddings, []float64{float64(i), float64(i % 7), 1})
			documents = append(documents, "document "+id)
			seededRecords[id] = true
		}
		h.Call(ctx, rid("records.add"), sess, records, nil, mustJSON(t, map[string]any{"records": map[string]any{
			"ids": ids, "embeddings": embeddings, "documents": documents, "upsert": true,
		}}))
	}

	rec.reset()
	seenRecords := map[string]bool{}
	cursor = ""
	pages = 0
	for {
		query := url.Values{"limit": []string{strconv.Itoa(recordLimit)}}
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		page := decodePage(t, h.Call(ctx, rid("records.list"), sess, records, query, nil))
		pages++
		if pages > recordCount {
			t.Fatalf("record paging did not terminate after %d pages", pages)
		}
		remaining := recordCount - len(seenRecords)
		want := min(remaining, recordLimit)
		if len(page.Items) != want {
			t.Fatalf("page %d: expected a bounded page of %d records, got %d", pages, want, len(page.Items))
		}
		// A record page never carries a total: counting every record would be the
		// full scan this route no longer does.
		if _, ok := page.raw["total"]; ok {
			t.Fatalf("record page %d reported a total: %#v", pages, page.raw["total"])
		}
		for _, item := range page.Items {
			id, _ := item["id"].(string)
			if !seededRecords[id] {
				t.Fatalf("page %d returned an unseeded record %q", pages, id)
			}
			if seenRecords[id] {
				t.Fatalf("record %q was returned on two pages", id)
			}
			seenRecords[id] = true
		}
		if page.NextCursor == "" {
			break
		}
		if want := strconv.Itoa(len(seenRecords)); page.NextCursor != want {
			t.Fatalf("page %d: expected cursor %q, got %q", pages, want, page.NextCursor)
		}
		cursor = page.NextCursor
	}
	if len(seenRecords) != recordCount {
		t.Fatalf("paging covered %d of %d records", len(seenRecords), recordCount)
	}
	if wantPages := (recordCount + recordLimit - 1) / recordLimit; pages != wantPages {
		t.Fatalf("expected %d record pages, got %d", wantPages, pages)
	}

	gets := rec.recordGets()
	if len(gets) != pages {
		t.Fatalf("expected one Chroma /get per record page, got %d for %d pages", len(gets), pages)
	}
	for i, call := range gets {
		if limit, _ := call.Body["limit"].(float64); int(limit) != recordLimit {
			t.Fatalf("record fetch %d asked Chroma for limit %v, want %d (unbounded fetch)", i, call.Body["limit"], recordLimit)
		}
		if offset, _ := call.Body["offset"].(float64); int(offset) != i*recordLimit {
			t.Fatalf("record fetch %d asked Chroma for offset %v, want %d", i, call.Body["offset"], i*recordLimit)
		}
	}

	// Over-asking is clamped to the connection's page limit, and the clamp reaches
	// the outbound fetch rather than only trimming the response.
	rec.reset()
	over := decodePage(t, h.Call(ctx, rid("records.list"), sess, records, url.Values{
		"limit": []string{strconv.Itoa(plugin.MaxPageLimit)},
	}, nil))
	if len(over.Items) != defaultPageLimit {
		t.Fatalf("over-asking should clamp to the %d page limit, got %d items", defaultPageLimit, len(over.Items))
	}
	if over.NextCursor != strconv.Itoa(defaultPageLimit) {
		t.Fatalf("clamped page should continue at %d, got cursor %q", defaultPageLimit, over.NextCursor)
	}
	clamped := rec.recordGets()
	if len(clamped) != 1 {
		t.Fatalf("expected one clamped record fetch, got %d", len(clamped))
	}
	if limit, _ := clamped[0].Body["limit"].(float64); int(limit) != defaultPageLimit {
		t.Fatalf("clamped fetch asked Chroma for limit %v, want %d", clamped[0].Body["limit"], defaultPageLimit)
	}

	h.AssertCovered(rid("collections.list"), rid("records.list"))
}

// outboundCall is one HTTP request the plugin made to Chroma.
type outboundCall struct {
	Method string
	Path   string
	Query  url.Values
	Body   map[string]any
}

// chromaRecorder proxies the plugin's traffic to a real Chroma server and keeps
// the requests, so a test can assert what the plugin actually asked for.
type chromaRecorder struct {
	server *httptest.Server

	mu    sync.Mutex
	calls []outboundCall
}

func recordChroma(t *testing.T, endpoint string) *chromaRecorder {
	t.Helper()
	target, err := url.Parse(endpoint)
	if err != nil {
		t.Fatalf("parse endpoint %q: %v", endpoint, err)
	}
	rec := &chromaRecorder{}
	proxy := httputil.NewSingleHostReverseProxy(target)
	rec.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := outboundCall{Method: r.Method, Path: r.URL.Path, Query: r.URL.Query()}
		if r.Body != nil && strings.HasSuffix(r.URL.Path, "/get") {
			body, err := io.ReadAll(r.Body)
			_ = r.Body.Close()
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			_ = json.Unmarshal(body, &call.Body)
			r.Body = io.NopCloser(bytes.NewReader(body))
			r.ContentLength = int64(len(body))
		}
		rec.mu.Lock()
		rec.calls = append(rec.calls, call)
		rec.mu.Unlock()
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(rec.server.Close)
	return rec
}

func (r *chromaRecorder) URL() string { return r.server.URL }

func (r *chromaRecorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = nil
}

func (r *chromaRecorder) filter(keep func(outboundCall) bool) []outboundCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]outboundCall, 0, len(r.calls))
	for _, call := range r.calls {
		if keep(call) {
			out = append(out, call)
		}
	}
	return out
}

func (r *chromaRecorder) collectionFetches(database string) []outboundCall {
	path := apiPrefix + scope{Tenant: defaultTenant, Database: database}.collectionsPath()
	return r.filter(func(call outboundCall) bool {
		return call.Method == http.MethodGet && call.Path == path
	})
}

func (r *chromaRecorder) recordGets() []outboundCall {
	return r.filter(func(call outboundCall) bool {
		return call.Method == http.MethodPost && strings.HasSuffix(call.Path, "/get")
	})
}

type pagedResult struct {
	Items      []map[string]any `json:"items"`
	NextCursor string           `json:"nextCursor"`
	Total      *int             `json:"total"`
	Truncated  bool             `json:"truncated"`

	raw map[string]any
}

func decodePage(t *testing.T, value any) pagedResult {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var out pagedResult
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("expected a page, got %s", data)
	}
	if err := json.Unmarshal(data, &out.raw); err != nil {
		t.Fatalf("expected a page object, got %s", data)
	}
	return out
}
