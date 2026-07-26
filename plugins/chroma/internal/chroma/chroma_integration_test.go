package chroma

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	contribtest "github.com/charlesng35/shellcn-contrib/shared/plugintest"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

const chromaImage = "chromadb/chroma:1.5.9"

func TestChromaPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_CHROMA_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_CHROMA_INTEGRATION=1 to run against Chroma")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	suffix := time.Now().UTC().Format("20060102150405")
	database := "shellcn_it_db_" + suffix
	tenant := "shellcn_it_tenant_" + suffix
	collectionName := "shellcn_it_col_" + suffix

	p := New()
	sess, err := p.Connect(ctx, plugin.ConnectConfig{
		Config: chromaConfig(ctx, t),
		Net:    plugintest.DirectTransport(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	h := contribtest.NewHarness(t, p.Routes())
	storage := newFakeStorage()

	overviewOut := asMap(t, h.Call(ctx, rid("overview"), sess, nil, nil, nil))
	if overviewOut["version"] == nil || overviewOut["status"] != "ready" {
		t.Fatalf("unexpected overview: %#v", overviewOut)
	}
	tenants := pageItems(t, h.Call(ctx, rid("tenants.tree"), sess, nil, nil, nil))
	if len(tenants) == 0 {
		t.Fatalf("expected at least one tenant node")
	}
	h.Call(ctx, rid("tenant.read"), sess, nil, nil, nil)
	h.Call(ctx, rid("tenant.create"), sess, nil, nil, mustJSON(t, map[string]any{"name": tenant}))
	if created := asMap(t, h.Call(ctx, rid("tenant.read"), sess, map[string]string{"tenant": tenant}, nil, nil)); created["name"] != tenant {
		t.Fatalf("created tenant not readable: %#v", created)
	}

	h.Call(ctx, rid("database.create"), sess, nil, nil, mustJSON(t, map[string]any{"name": database}))
	defer h.CallNoFail(context.Background(), rid("database.delete"), sess, map[string]string{"database": database})
	databases := pageItems(t, h.Call(ctx, rid("databases.list"), sess, nil, nil, nil))
	if !hasField(databases, "name", database) {
		t.Fatalf("created database not listed: %#v", databases)
	}
	dbNodes := pageItems(t, h.Call(ctx, rid("databases.tree"), sess, nil, nil, nil))
	if !hasField(dbNodes, "label", database) {
		t.Fatalf("created database missing from the tree: %#v", dbNodes)
	}

	created := asMap(t, h.Call(ctx, rid("collection.create"), sess, nil, nil, mustJSON(t, map[string]any{
		"name": collectionName, "space": spaceCosine, "database": database,
		"metadata": map[string]any{"owner": "shellcn"}, "ef_construction": 128, "max_neighbors": 24,
	})))
	collectionID, _ := created["id"].(string)
	if collectionID == "" {
		t.Fatalf("collection create returned no id: %#v", created)
	}
	scoped := map[string]string{"database": database, "collection": collectionID}
	defer h.CallNoFail(context.Background(), rid("collection.delete"), sess, scoped)

	collections := pageItems(t, h.Call(ctx, rid("collections.list"), sess, map[string]string{"database": database}, nil, nil))
	if !hasField(collections, "name", collectionName) {
		t.Fatalf("created collection not listed: %#v", collections)
	}
	detail := asMap(t, h.Call(ctx, rid("collection.read"), sess, scoped, nil, nil))
	if detail["space"] != spaceCosine || detail["ef_construction"] != float64(128) {
		t.Fatalf("unexpected collection detail: %#v", detail)
	}

	h.Call(ctx, rid("records.add"), sess, scoped, nil, mustJSON(t, map[string]any{"records": map[string]any{
		"ids":        []string{"alpha", "beta", "gamma", "delta"},
		"embeddings": [][]float64{{1, 0, 0}, {0.92, 0.2, 0}, {0, 1, 0}, {0, 0.1, 1}},
		"documents":  []string{"alpha vector", "beta vector", "gamma vector", "delta vector"},
		"metadatas":  []map[string]any{{"tag": "x"}, {"tag": "x"}, {"tag": "y"}, {"tag": "z"}},
		"upsert":     true,
	}}))

	records := pageItems(t, h.Call(ctx, rid("records.list"), sess, scoped, url.Values{"limit": []string{"50"}}, nil))
	if len(records) != 4 {
		t.Fatalf("expected four records, got %#v", records)
	}
	filtered := pageItems(t, h.Call(ctx, rid("records.list"), sess, scoped, url.Values{"filter": []string{"gamma"}}, nil))
	if len(filtered) != 1 || filtered[0]["id"] != "gamma" {
		t.Fatalf("document search did not filter: %#v", filtered)
	}

	recordParams := map[string]string{"scope": defaultTenant + "/" + database, "collection": collectionID, "record": "alpha"}
	record := asMap(t, h.Call(ctx, rid("record.read"), sess, recordParams, nil, nil))
	if record["dimension"] != float64(3) || record["document"] != "alpha vector" {
		t.Fatalf("unexpected record: %#v", record)
	}
	neighbors := pageItems(t, h.Call(ctx, rid("record.neighbors"), sess, recordParams, nil, nil))
	if len(neighbors) != 3 || neighbors[0]["id"] != "beta" {
		t.Fatalf("unexpected neighbors: %#v", neighbors)
	}

	updated := asMap(t, h.Call(ctx, rid("record.update"), sess, scoped, nil, mustJSON(t, map[string]any{
		"id": "delta", "document": "delta vector v2", "metadata": map[string]any{"tag": "z", "revised": true},
	})))
	if updated["document"] != "delta vector v2" {
		t.Fatalf("record update did not round-trip: %#v", updated)
	}

	schemaRows := pageItems(t, h.Call(ctx, rid("collection.schema"), sess, scoped, nil, nil))
	if len(schemaRows) == 0 {
		t.Fatalf("expected schema rows")
	}
	completions := pageItems(t, h.Call(ctx, rid("query.complete"), sess, scoped, url.Values{"limit": []string{"200"}}, nil))
	if !hasField(completions, "label", "tag") {
		t.Fatalf("completions should surface the sampled metadata key: %#v", completions)
	}

	frames := streamFrames(t, h.Stream(ctx, rid("query"), sess, scoped, nil, mustLines(
		`{"query":"{\"query_embeddings\":[[1,0,0]],\"n_results\":3}"}`,
		`{"query":"{\"query_id\":\"alpha\",\"n_results\":2}"}`,
		`{"query":"{\"query_text\":\"gamma\"}"}`,
		`{"query":"{\"where\":{\"tag\":{\"$eq\":\"x\"}},\"limit\":10}"}`,
		`{"query":"not json"}`,
	)))
	if len(frames) != 5 {
		t.Fatalf("expected five query frames, got %d: %#v", len(frames), frames)
	}
	if frames[0]["mode"] != "similarity" || frames[0]["rowCount"] != float64(3) {
		t.Fatalf("unexpected similarity frame: %#v", frames[0])
	}
	if frames[2]["mode"] != "fetch" || frames[2]["rowCount"] != float64(1) {
		t.Fatalf("query_text should match one document: %#v", frames[2])
	}
	if frames[3]["rowCount"] != float64(2) {
		t.Fatalf("metadata filter should match two records: %#v", frames[3])
	}
	if frames[4]["error"] == nil {
		t.Fatalf("expected an error frame for invalid JSON: %#v", frames[4])
	}

	// Canvas streams: one ready event, then the client goes away.
	ready := mustLines(`{"type":"ready","width":1200,"height":720,"dpr":2,"theme":"dark"}`)
	projectionFrame := firstFrame(t, h.Stream(ctx, rid("projection"), sess, scoped, nil, ready))
	if len(projectionFrame["regions"].([]any)) != 4 {
		t.Fatalf("expected one canvas region per vector: %#v", projectionFrame["regions"])
	}
	spacesFrame := firstFrame(t, h.Stream(ctx, rid("spaces"), sess, scoped, nil, ready))
	if len(spacesFrame["regions"].([]any)) == 0 {
		t.Fatalf("expected neighbor cards on the distance-space canvas")
	}

	// Metric streams write immediately, then stop with their own context.
	collectionMetric := firstFrame(t, streamBriefly(ctx, t, h, rid("collection.metrics"), sess, scoped))
	if collectionMetric["records"] != float64(4) || collectionMetric["dimension"] != float64(3) {
		t.Fatalf("unexpected collection metrics: %#v", collectionMetric)
	}
	serverMetric := firstFrame(t, streamBriefly(ctx, t, h, rid("server.metrics"), sess, map[string]string{"database": database}))
	if serverMetric["collections"] != float64(1) || serverMetric["records"] != float64(4) {
		t.Fatalf("unexpected server metrics: %#v", serverMetric)
	}

	// Saved queries and history need the scoped storage surface.
	callStored(t, h, ctx, rid("queries.save"), sess, storage, nil, mustJSON(t, map[string]any{
		"name": "Nearest to alpha", "query": `{"query_id":"alpha","n_results":5}`, "collection": collectionName,
	}))
	saved := pageItems(t, callStored(t, h, ctx, rid("queries.list"), sess, storage, nil, nil))
	if len(saved) != 1 || saved[0]["name"] != "Nearest to alpha" {
		t.Fatalf("unexpected saved queries: %#v", saved)
	}
	callStored(t, h, ctx, rid("queries.delete"), sess, storage, map[string]string{"id": saved[0]["id"].(string)}, nil)

	runStoredQuery(t, h, ctx, sess, storage, scoped, `{"query":"{\"limit\":5}"}`)
	history := pageItems(t, callStored(t, h, ctx, rid("history.list"), sess, storage, scoped, nil))
	if len(history) != 1 || history[0]["title"] == nil {
		t.Fatalf("unexpected history: %#v", history)
	}
	cleared := asMap(t, callStored(t, h, ctx, rid("history.clear"), sess, storage, scoped, nil))
	if cleared["deleted"] != float64(1) {
		t.Fatalf("unexpected history clear: %#v", cleared)
	}

	// Mutations that unwind the fixture.
	h.Call(ctx, rid("records.delete"), sess, scoped, nil, mustJSON(t, map[string]any{"ids": []string{"delta"}}))
	if left := pageItems(t, h.Call(ctx, rid("records.list"), sess, scoped, nil, nil)); len(left) != 3 {
		t.Fatalf("expected three records after delete, got %#v", left)
	}
	renamed := collectionName + "_renamed"
	h.Call(ctx, rid("collection.update"), sess, scoped, nil, mustJSON(t, map[string]any{
		"new_name": renamed, "new_metadata": map[string]any{"owner": "shellcn", "stage": "verified"},
	}))
	if after := asMap(t, h.Call(ctx, rid("collection.read"), sess, scoped, nil, nil)); after["name"] != renamed {
		t.Fatalf("rename did not apply: %#v", after)
	}
	h.Call(ctx, rid("collection.delete"), sess, scoped, nil, nil)
	if after := pageItems(t, h.Call(ctx, rid("collections.list"), sess, map[string]string{"database": database}, nil, nil)); len(after) != 0 {
		t.Fatalf("expected the collection to be gone, got %#v", after)
	}
	h.Call(ctx, rid("database.delete"), sess, map[string]string{"database": database}, nil, nil)

	h.AssertAllCovered()
}

func TestChromaReadOnlyIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_CHROMA_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_CHROMA_INTEGRATION=1 to run against Chroma")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	config := chromaConfig(ctx, t)
	config["read_only"] = true
	sess, err := New().Connect(ctx, plugin.ConnectConfig{Config: config, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()

	routes := plugintest.RouteMap(New().Routes())
	body := mustJSON(t, map[string]any{"name": "blocked"})
	rc := plugin.NewRequestContext(ctx, plugin.User{}, sess, nil, nil, body)
	if _, err := routes[rid("collection.create")].Handle(rc); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("expected the read-only guard to reject a create, got %v", err)
	}
}

func chromaConfig(ctx context.Context, t *testing.T) map[string]any {
	t.Helper()
	endpoint := os.Getenv("SHELLCN_CHROMA_ENDPOINT")
	if endpoint == "" {
		endpoint = startChromaContainer(ctx, t)
	}
	return map[string]any{
		"endpoint": endpoint, "tenant": defaultTenant, "database": defaultDatabase,
		"auth": "none", "tls_mode": "disable", "read_only": false,
		"page_limit": 100, "sample_limit": 512, "timeout": "20s",
	}
}

func startChromaContainer(ctx context.Context, t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_CHROMA_ENDPOINT is not set")
	}
	name := "shellcn-chroma-it-" + time.Now().UTC().Format("20060102150405")
	if out, err := exec.CommandContext(ctx, "docker", "run", "-d", "--rm", "--name", name,
		"-p", "127.0.0.1::8000", chromaImage).CombinedOutput(); err != nil {
		t.Skipf("cannot start %s: %v\n%s", chromaImage, err, out)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = exec.CommandContext(stopCtx, "docker", "rm", "-f", name).Run()
	})

	out, err := exec.CommandContext(ctx, "docker", "port", name, "8000/tcp").CombinedOutput()
	if err != nil {
		t.Fatalf("docker port: %v\n%s", err, out)
	}
	host, port, err := net.SplitHostPort(strings.TrimSpace(strings.Split(string(out), "\n")[0]))
	if err != nil {
		t.Fatalf("unexpected docker port output: %q", out)
	}
	endpoint := "http://" + net.JoinHostPort(host, port)

	deadline := time.Now().Add(90 * time.Second)
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/api/v2/heartbeat", nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode < 400 {
				return endpoint
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("Chroma container did not become ready: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func callStored(t *testing.T, h *contribtest.Harness, ctx context.Context, id string, sess plugin.Session, storage plugin.Storage, params map[string]string, body []byte) any {
	t.Helper()
	route := h.Route(id)
	rc := plugin.NewRequestContext(ctx, plugin.User{}, sess, params, nil, body).WithStorage(storage)
	out, err := route.Handle(rc)
	if err != nil {
		t.Fatalf("%s: %v", id, err)
	}
	return out
}

func runStoredQuery(t *testing.T, h *contribtest.Harness, ctx context.Context, sess plugin.Session, storage plugin.Storage, params map[string]string, frame string) {
	t.Helper()
	route := h.Route(rid("query"))
	stream := newMemoryStream(frame + "\n")
	rc := plugin.NewRequestContext(ctx, plugin.User{}, sess, params, nil, nil).WithStorage(storage)
	if err := route.Stream(rc, stream); err != nil {
		t.Fatalf("query stream: %v", err)
	}
}

// streamBriefly gives a metric stream its own deadline so the first snapshot is
// captured and the pump then exits on context cancellation.
func streamBriefly(ctx context.Context, t *testing.T, h *contribtest.Harness, id string, sess plugin.Session, params map[string]string) []byte {
	t.Helper()
	streamCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return h.Stream(streamCtx, id, sess, params, nil, nil)
}

func mustLines(frames ...string) []byte { return []byte(strings.Join(frames, "\n") + "\n") }

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func asMap(t *testing.T, value any) map[string]any {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("expected an object, got %s", data)
	}
	return out
}

func pageItems(t *testing.T, page any) []map[string]any {
	t.Helper()
	data, err := json.Marshal(page)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("expected a page, got %s", data)
	}
	return decoded.Items
}

func streamFrames(t *testing.T, out []byte) []map[string]any {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(string(out)))
	frames := make([]map[string]any, 0, 4)
	for dec.More() {
		var frame map[string]any
		if err := dec.Decode(&frame); err != nil {
			t.Fatalf("decode stream frame: %v (%s)", err, out)
		}
		frames = append(frames, frame)
	}
	return frames
}

func firstFrame(t *testing.T, out []byte) map[string]any {
	t.Helper()
	frames := streamFrames(t, out)
	if len(frames) == 0 {
		t.Fatalf("expected at least one stream frame, got %q", out)
	}
	return frames[0]
}

func hasField(items []map[string]any, key, want string) bool {
	for _, item := range items {
		if fmt.Sprint(item[key]) == want {
			return true
		}
	}
	return false
}
