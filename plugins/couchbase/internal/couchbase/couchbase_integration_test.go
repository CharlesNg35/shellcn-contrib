package couchbase

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	contribtest "github.com/charlesng35/shellcn-contrib/shared/plugintest"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

const (
	integrationImage    = "couchbase:community-7.6.2"
	integrationUser     = "Administrator"
	integrationPassword = "password"
)

// TestCouchbasePluginIntegration drives every route against a real Couchbase
// Server. The container is provisioned through the documented cluster
// initialization REST calls before the plugin connects.
func TestCouchbasePluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_COUCHBASE_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_COUCHBASE_INTEGRATION=1 to run against Couchbase Server")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	host, managementPort, queryPort := couchbaseEndpoint(ctx, t)
	p := New()
	sess, err := p.Connect(ctx, plugin.ConnectConfig{
		Net: plugintest.DirectTransport(),
		Config: map[string]any{
			"host": host, "port": managementPort, "query_port": queryPort,
			"username": integrationUser, "password": integrationPassword,
			"read_only": false, "require_write_confirmation": true, "timeout": "60s",
		},
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	if err := sess.HealthCheck(ctx); err != nil {
		t.Fatalf("health check: %v", err)
	}

	h := contribtest.NewHarness(t, p.Routes())
	bucket := "shellcn_it_" + time.Now().UTC().Format("20060102150405")
	storage := &fakeStorage{}

	// --- bucket, scope, collection lifecycle --------------------------------
	h.Call(ctx, rid("bucket.create"), sess, nil, nil, mustMarshal(t, map[string]any{
		"name": bucket, "bucket_type": "couchbase", "ram_quota_mb": 128,
		"replica_number": 0, "flush_enabled": true,
	}))
	defer h.CallNoFail(context.Background(), rid("bucket.delete"), sess, map[string]string{"bucket": bucket})
	waitForBucket(ctx, t, h, sess, bucket)

	bucketParams := map[string]string{"bucket": bucket}
	h.Call(ctx, rid("scope.create"), sess, bucketParams, nil, mustMarshal(t, map[string]any{"name": "app"}))
	scopeKey := keyspace{Bucket: bucket, Scope: "app"}
	h.Call(ctx, rid("collection.create"), sess, map[string]string{"keyspace": scopeKey.uid()}, nil,
		mustMarshal(t, map[string]any{"name": "orders", "max_ttl": 0}))
	collectionKey := keyspace{Bucket: bucket, Scope: "app", Collection: "orders"}
	collectionParams := map[string]string{"keyspace": collectionKey.uid()}
	waitForCollection(ctx, t, sess.(*Session), collectionKey)

	// --- navigation ---------------------------------------------------------
	if !hasField(pageOf(t, h.Call(ctx, rid("tree.buckets"), sess, nil, nil, nil)), "label", bucket) {
		t.Fatal("the new bucket is missing from the sidebar tree")
	}
	if !hasField(pageOf(t, h.Call(ctx, rid("tree.scopes"), sess, bucketParams, nil, nil)), "label", "app") {
		t.Fatal("the new scope is missing from the tree")
	}
	if !hasField(pageOf(t, h.Call(ctx, rid("tree.collections"), sess, map[string]string{"bucket": bucket, "scope": "app"}, nil, nil)), "label", "orders") {
		t.Fatal("the new collection is missing from the tree")
	}

	// --- cluster ------------------------------------------------------------
	overview := objectOf(t, h.Call(ctx, rid("cluster.overview"), sess, nil, nil, nil))
	if overview["edition"] == "" || overview["version"] == "" {
		t.Fatalf("cluster overview is incomplete: %#v", overview)
	}
	if len(pageOf(t, h.Call(ctx, rid("cluster.list"), sess, nil, nil, nil))) != 1 {
		t.Fatal("the cluster resource must expose exactly one row")
	}
	if len(pageOf(t, h.Call(ctx, rid("nodes.list"), sess, nil, nil, nil))) == 0 {
		t.Fatal("expected at least one cluster node")
	}
	if len(pageOf(t, h.Call(ctx, rid("events.list"), sess, nil, nil, nil))) == 0 {
		t.Fatal("expected cluster log events")
	}
	h.Call(ctx, rid("events.list"), sess, map[string]string{"filter": "replication"}, nil, nil)
	h.Call(ctx, rid("users.list"), sess, nil, nil, nil)

	// --- buckets ------------------------------------------------------------
	if !hasField(pageOf(t, h.Call(ctx, rid("buckets.list"), sess, nil, nil, nil)), "name", bucket) {
		t.Fatal("the new bucket is missing from the bucket list")
	}
	detail := objectOf(t, h.Call(ctx, rid("bucket.read"), sess, bucketParams, nil, nil))
	if detail["name"] != bucket || numberValue(detail["scopes"]) < 2 {
		t.Fatalf("unexpected bucket detail: %#v", detail)
	}
	h.Call(ctx, rid("bucket.update"), sess, bucketParams, nil, mustMarshal(t, map[string]any{"ram_quota_mb": 160}))
	edited := objectOf(t, h.Call(ctx, rid("bucket.read"), sess, bucketParams, nil, nil))
	if numberValue(edited["ram_quota_mb"]) != 160 || edited["flush_enabled"] != true {
		t.Fatalf("the bucket edit was not applied: %#v", edited)
	}
	// The edit form prefills from the bucket row, so submitting it untouched has
	// to leave the live bucket exactly as the user saw it.
	h.Call(ctx, rid("bucket.update"), sess, bucketParams, nil, mustMarshal(t, bucketEditPrefill(t, edited)))
	resubmitted := objectOf(t, h.Call(ctx, rid("bucket.read"), sess, bucketParams, nil, nil))
	if numberValue(resubmitted["ram_quota_mb"]) != 160 || numberValue(resubmitted["replicas"]) != 0 || resubmitted["flush_enabled"] != true {
		t.Fatalf("resubmitting the prefilled edit form changed the bucket: %#v", resubmitted)
	}
	h.Call(ctx, rid("bucket.dcp"), sess, bucketParams, nil, nil)

	scopes := pageOf(t, h.Call(ctx, rid("scopes.list"), sess, bucketParams, nil, nil))
	if !hasField(scopes, "name", "app") {
		t.Fatalf("unexpected scopes: %#v", scopes)
	}
	scopeDetail := objectOf(t, h.Call(ctx, rid("scope.read"), sess, map[string]string{"keyspace": scopeKey.uid()}, nil, nil))
	if scopeDetail["name"] != "app" {
		t.Fatalf("unexpected scope detail: %#v", scopeDetail)
	}
	collections := pageOf(t, h.Call(ctx, rid("collections.list"), sess, map[string]string{"keyspace": scopeKey.uid()}, nil, nil))
	if !hasField(collections, "name", "orders") {
		t.Fatalf("unexpected collections: %#v", collections)
	}
	collectionDetail := objectOf(t, h.Call(ctx, rid("collection.read"), sess, collectionParams, nil, nil))
	if collectionDetail["keyspace"] != collectionKey.label() {
		t.Fatalf("unexpected collection detail: %#v", collectionDetail)
	}

	// --- indexes ------------------------------------------------------------
	h.Call(ctx, rid("index.create"), sess, collectionParams, nil, mustMarshal(t, map[string]any{
		"name": "it_primary", "primary": true,
	}))
	waitForIndex(ctx, t, h, sess, collectionKey, "it_primary")
	h.Call(ctx, rid("index.create"), sess, collectionParams, nil, mustMarshal(t, map[string]any{
		"name": "it_customer", "fields": []any{"customer"}, "deferred": true,
	}))
	deferred := collectionKey
	deferred.Document = "it_customer"
	h.Call(ctx, rid("index.build"), sess, map[string]string{"id": deferred.uid()}, nil, nil)
	waitForIndex(ctx, t, h, sess, collectionKey, "it_customer")
	indexDetail := objectOf(t, h.Call(ctx, rid("index.read"), sess, map[string]string{"id": deferred.uid()}, nil, nil))
	if !strings.Contains(fmt.Sprint(indexDetail["definition"]), "it_customer") {
		t.Fatalf("unexpected index detail: %#v", indexDetail)
	}
	if indexDetail["index_keys"] == nil {
		t.Fatalf("the catalog entry was not merged into the index detail: %#v", indexDetail)
	}
	// system:indexes omits bucket_id for the default scope and collection, so the
	// catalog lookup has to match that shape too.
	defaultKey := keyspace{Bucket: bucket, Scope: "_default", Collection: "_default"}
	h.Call(ctx, rid("index.create"), sess, map[string]string{"keyspace": defaultKey.uid()}, nil,
		mustMarshal(t, map[string]any{"name": "it_default", "primary": true}))
	waitForIndex(ctx, t, h, sess, defaultKey, "it_default")
	defaultIndex := defaultKey
	defaultIndex.Document = "it_default"
	defaultDetail := objectOf(t, h.Call(ctx, rid("index.read"), sess, map[string]string{"id": defaultIndex.uid()}, nil, nil))
	if defaultDetail["state"] == nil {
		t.Fatalf("the catalog entry for a default-collection index was not merged: %#v", defaultDetail)
	}

	// --- documents ----------------------------------------------------------
	h.Call(ctx, rid("document.insert"), sess, collectionParams, nil, mustMarshal(t, map[string]any{
		"key": "order::1", "value": map[string]any{"customer": "ada", "total": 42, "status": "paid"},
	}))
	h.Call(ctx, rid("document.insert"), sess, collectionParams, nil, mustMarshal(t, map[string]any{
		"values": map[string]any{"id": "order::2", "customer": "grace", "total": 7, "status": "open"},
	}))
	documents := pageOf(t, h.Call(ctx, rid("documents.list"), sess, collectionParams, nil, nil))
	if len(documents) != 2 {
		t.Fatalf("expected two documents, got %#v", documents)
	}
	columns := pageOf(t, h.Call(ctx, rid("documents.columns"), sess, collectionParams, nil, nil))
	if !hasField(columns, "name", "customer") {
		t.Fatalf("inferred columns are missing document fields: %#v", columns)
	}

	document := collectionKey
	document.Document = "order::1"
	documentParams := map[string]string{"id": document.uid()}
	read := objectOf(t, h.Call(ctx, rid("document.read"), sess, documentParams, nil, nil))
	if read["key"] != "order::1" || numberValue(read["cas"]) == 0 {
		t.Fatalf("unexpected document read: %#v", read)
	}
	content := objectOf(t, h.Call(ctx, rid("document.content"), sess, documentParams, nil, nil))
	if !strings.Contains(fmt.Sprint(content["content"]), "\"customer\": \"ada\"") {
		t.Fatalf("unexpected document content: %#v", content)
	}
	h.Call(ctx, rid("document.update"), sess, collectionParams, nil, mustMarshal(t, map[string]any{
		"key": map[string]any{"id": "order::1"}, "values": map[string]any{"total": 99},
	}))
	saved := objectOf(t, h.Call(ctx, rid("document.save"), sess, documentParams, nil, mustMarshal(t, map[string]any{
		"content": `{"customer":"ada","total":123,"status":"shipped"}`,
	})))
	if !strings.Contains(fmt.Sprint(saved["content"]), "123") {
		t.Fatalf("document save did not return the canonical content: %#v", saved)
	}

	schema := pageOf(t, h.Call(ctx, rid("collection.schema"), sess, collectionParams, nil, nil))
	if !hasField(schema, "field", "customer") {
		t.Fatalf("INFER did not report the document fields: %#v", schema)
	}

	// --- SQL++ --------------------------------------------------------------
	queryFrame := streamFrames(ctx, t, h, sess, rid("query"), collectionParams,
		`{"query":"SELECT META(d).id AS id, d.customer FROM `+collectionKey.path()+` d ORDER BY META(d).id"}`)
	if !strings.Contains(queryFrame, "order::1") || !strings.Contains(queryFrame, "\"columns\"") {
		t.Fatalf("unexpected SQL++ result frame: %s", queryFrame)
	}
	confirmFrame := streamFrames(ctx, t, h, sess, rid("query"), collectionParams,
		`{"query":"UPSERT INTO `+collectionKey.path()+` (KEY, VALUE) VALUES (\"order::3\", {\"customer\":\"linus\"})"}`)
	if !strings.Contains(confirmFrame, "confirmRequired") {
		t.Fatalf("a mutating statement must ask for confirmation: %s", confirmFrame)
	}
	confirmedFrame := streamFrames(ctx, t, h, sess, rid("query"), collectionParams,
		`{"query":"UPSERT INTO `+collectionKey.path()+` (KEY, VALUE) VALUES (\"order::3\", {\"customer\":\"linus\"})","confirm":true}`)
	if strings.Contains(confirmedFrame, "error") {
		t.Fatalf("the confirmed statement failed: %s", confirmedFrame)
	}
	adviceFrame := streamFrames(ctx, t, h, sess, rid("advisor"), collectionParams,
		`{"query":"SELECT * FROM `+collectionKey.path()+` WHERE status = \"paid\""}`)
	if !strings.Contains(adviceFrame, "\"kind\"") {
		t.Fatalf("the advisor returned no advice: %s", adviceFrame)
	}
	completions := pageOf(t, h.Call(ctx, rid("query.complete"), sess, collectionParams, nil, nil))
	if len(completions) == 0 {
		t.Fatal("expected SQL++ completions")
	}
	cancelled := objectOf(t, h.Call(ctx, rid("query.cancel"), sess, nil, nil, nil))
	if cancelled["ok"] != true {
		t.Fatalf("unexpected cancel result: %#v", cancelled)
	}

	// --- saved queries (plugin storage) -------------------------------------
	storageRC := func(params map[string]string, body []byte) *plugin.RequestContext {
		return plugin.NewRequestContext(ctx, plugin.User{}, sess, params, url.Values{}, body).WithStorage(storage)
	}
	if _, err := h.Route(rid("query.save")).Handle(storageRC(nil, mustMarshal(t, map[string]any{
		"name": "Recent orders", "statement": "SELECT * FROM " + collectionKey.path() + " LIMIT 10",
		"keyspace": collectionKey.label(),
	}))); err != nil {
		t.Fatalf("query.save: %v", err)
	}
	listed, err := h.Route(rid("queries.list")).Handle(storageRC(nil, nil))
	if err != nil {
		t.Fatalf("queries.list: %v", err)
	}
	if !hasField(pageOf(t, listed), "name", "Recent orders") {
		t.Fatalf("the saved statement is missing: %#v", pageOf(t, listed))
	}
	if _, err := h.Route(rid("query.delete")).Handle(storageRC(map[string]string{"id": "query/recent-orders"}, nil)); err != nil {
		t.Fatalf("query.delete: %v", err)
	}

	// --- live surfaces ------------------------------------------------------
	clusterFrame := streamFrames(ctx, t, h, sess, rid("cluster.metrics"), nil, "")
	if !strings.Contains(clusterFrame, "\"quotaPct\"") {
		t.Fatalf("unexpected cluster metrics frame: %s", clusterFrame)
	}
	bucketFrame := streamFrames(ctx, t, h, sess, rid("bucket.metrics"), bucketParams, "")
	if !strings.Contains(bucketFrame, "\"items\"") {
		t.Fatalf("unexpected bucket metrics frame: %s", bucketFrame)
	}
	watchFrame := streamFrames(ctx, t, h, sess, rid("cluster.watch"), nil, "")
	if !strings.Contains(watchFrame, "\"kind\":\"cluster\"") {
		t.Fatalf("unexpected cluster watch frame: %s", watchFrame)
	}
	streamFrames(ctx, t, h, sess, rid("events.watch"), nil, "")
	streamFrames(ctx, t, h, sess, rid("resource.watch"), map[string]string{"kind": "bucket"}, "")
	mapFrame := streamFrames(ctx, t, h, sess, rid("datamap"), bucketParams, "")
	if !strings.Contains(mapFrame, "collection:app/orders") {
		t.Fatalf("the data map is missing the collection region: %s", mapFrame)
	}

	// --- XDCR and console ---------------------------------------------------
	if len(pageOf(t, h.Call(ctx, rid("replications.list"), sess, nil, nil, nil))) != 0 {
		t.Fatal("a fresh cluster should have no XDCR replications")
	}
	h.CallNoFail(ctx, rid("replication.read"), sess, map[string]string{"id": replicationUID("missing/x/y")})
	h.Call(ctx, rid("remotes.list"), sess, nil, nil, nil)
	consoleRC := plugin.NewRequestContext(ctx, plugin.User{}, sess, nil, url.Values{}, nil).
		WithProxyPrefix("/api/connections/it/proxy")
	consoleOut, err := h.Route(rid("console.url")).Handle(consoleRC)
	if err != nil {
		t.Fatalf("console.url: %v", err)
	}
	if objectOf(t, consoleOut)["url"] != "/api/connections/it/proxy/" {
		t.Fatalf("unexpected console URL: %#v", consoleOut)
	}

	// --- teardown -----------------------------------------------------------
	h.Call(ctx, rid("document.remove"), sess, documentParams, nil, nil)
	h.Call(ctx, rid("bucket.compact"), sess, bucketParams, nil, nil)
	h.Call(ctx, rid("bucket.flush"), sess, bucketParams, nil, nil)
	h.Call(ctx, rid("index.drop"), sess, map[string]string{"id": deferred.uid()}, nil, nil)
	h.Call(ctx, rid("collection.delete"), sess, collectionParams, nil, nil)
	h.Call(ctx, rid("scope.delete"), sess, map[string]string{"keyspace": scopeKey.uid()}, nil, nil)
	h.Call(ctx, rid("bucket.delete"), sess, bucketParams, nil, nil)

	h.AssertAllCovered()
}

// --- helpers ----------------------------------------------------------------

func mustMarshal(t *testing.T, value any) []byte {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return body
}

func pageOf(t *testing.T, value any) []map[string]any {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal page: %v", err)
	}
	var decoded struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode page: %v", err)
	}
	return decoded.Items
}

func objectOf(t *testing.T, value any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal object: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode object: %v", err)
	}
	return decoded
}

func hasField(items []map[string]any, key, want string) bool {
	for _, item := range items {
		if fmt.Sprint(item[key]) == want {
			return true
		}
	}
	return false
}

// streamFrames runs a stream handler with a bounded lifetime; server-push
// streams end when the context expires, request/response streams when the input
// is consumed.
func streamFrames(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, id string, params map[string]string, input string) string {
	t.Helper()
	streamCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	payload := []byte(nil)
	if input != "" {
		payload = []byte(input + "\n")
	}
	return string(h.Stream(streamCtx, id, sess, params, url.Values{}, payload))
}

func waitForBucket(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, bucket string) {
	t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	for {
		out := h.Call(ctx, rid("bucket.read"), sess, map[string]string{"bucket": bucket}, nil, nil)
		if objectOf(t, out)["health"] == "healthy" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("bucket %q did not become healthy", bucket)
		}
		time.Sleep(2 * time.Second)
	}
}

// waitForCollection waits until the Query service sees the new collection; the
// collections manifest propagates asynchronously.
func waitForCollection(ctx context.Context, t *testing.T, s *Session, k keyspace) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for {
		if _, err := s.exec(ctx, "SELECT RAW COUNT(*) FROM "+k.path()); err == nil {
			return
		} else if time.Now().After(deadline) {
			t.Fatalf("collection %s never became queryable: %v", k.label(), err)
		}
		time.Sleep(2 * time.Second)
	}
}

func waitForIndex(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, k keyspace, name string) {
	t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	for {
		for _, item := range pageOf(t, h.Call(ctx, rid("indexes.list"), sess, map[string]string{"keyspace": k.uid()}, nil, nil)) {
			if item["name"] == name && item["status"] == "ready" {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("index %q never became ready", name)
		}
		time.Sleep(2 * time.Second)
	}
}

// couchbaseEndpoint returns a provisioned single-node cluster, starting the
// official image when no endpoint is supplied.
func couchbaseEndpoint(ctx context.Context, t *testing.T) (host string, managementPort, queryPort int) {
	t.Helper()
	if endpoint := os.Getenv("SHELLCN_COUCHBASE_ENDPOINT"); endpoint != "" {
		parsedHost, portText, err := net.SplitHostPort(endpoint)
		if err != nil {
			t.Fatalf("SHELLCN_COUCHBASE_ENDPOINT must be host:port, got %q", endpoint)
		}
		port, err := strconv.Atoi(portText)
		if err != nil {
			t.Fatalf("invalid management port in %q", endpoint)
		}
		query := port + 2
		if text := os.Getenv("SHELLCN_COUCHBASE_QUERY_PORT"); text != "" {
			if parsed, err := strconv.Atoi(text); err == nil {
				query = parsed
			}
		}
		provisionCluster(ctx, t, parsedHost, port, query)
		return parsedHost, port, query
	}
	return startCouchbaseContainer(ctx, t)
}

func startCouchbaseContainer(ctx context.Context, t *testing.T) (string, int, int) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_COUCHBASE_ENDPOINT is not set")
	}
	name := "shellcn-couchbase-it-" + time.Now().UTC().Format("20060102150405")
	if out, err := exec.CommandContext(ctx, "docker", "run", "-d", "--rm", "--name", name,
		"-p", "127.0.0.1::8091", "-p", "127.0.0.1::8093", integrationImage).CombinedOutput(); err != nil {
		t.Skipf("cannot start %s: %v\n%s", integrationImage, err, out)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})
	host, managementPort := dockerPort(ctx, t, name, "8091/tcp")
	_, queryPort := dockerPort(ctx, t, name, "8093/tcp")
	provisionCluster(ctx, t, host, managementPort, queryPort)
	return host, managementPort, queryPort
}

func dockerPort(ctx context.Context, t *testing.T, name, port string) (string, int) {
	t.Helper()
	out, err := exec.CommandContext(ctx, "docker", "port", name, port).CombinedOutput()
	if err != nil {
		t.Fatalf("docker port %s %s: %v\n%s", name, port, err, out)
	}
	mapping := strings.TrimSpace(strings.SplitN(string(out), "\n", 2)[0])
	host, portText, err := net.SplitHostPort(mapping)
	if err != nil {
		t.Fatalf("unexpected docker port output %q", mapping)
	}
	parsed, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("unexpected docker port %q", portText)
	}
	return host, parsed
}

// provisionCluster performs the documented single-node initialization sequence:
// assign services, set the memory quotas, create the administrator, and choose
// the index storage mode. It is idempotent enough to run against an already
// provisioned cluster.
func provisionCluster(ctx context.Context, t *testing.T, host string, managementPort, queryPort int) {
	t.Helper()
	base := "http://" + net.JoinHostPort(host, strconv.Itoa(managementPort))
	waitForHTTP(ctx, t, base+"/pools", 180*time.Second)

	// The node restarts its HTTP listener while it is being provisioned, so each
	// call is retried through transient transport failures.
	post := func(path string, form url.Values, auth bool) (int, string) {
		var lastErr error
		for attempt := 0; attempt < 10; attempt++ {
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, strings.NewReader(form.Encode()))
			if err != nil {
				t.Fatalf("build %s request: %v", path, err)
			}
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.Close = true
			if auth {
				req.SetBasicAuth(integrationUser, integrationPassword)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				lastErr = err
				time.Sleep(2 * time.Second)
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			return resp.StatusCode, strings.TrimSpace(string(body))
		}
		t.Fatalf("%s: %v", path, lastErr)
		return 0, ""
	}

	if status, body := post("/node/controller/setupServices", url.Values{"services": {"kv,n1ql,index"}}, false); status >= 400 &&
		!strings.Contains(body, "cannot change node services") {
		t.Fatalf("setupServices: %d %s", status, body)
	}
	if status, body := post("/pools/default", url.Values{"memoryQuota": {"512"}, "indexMemoryQuota": {"512"}}, false); status >= 400 {
		if status, body = post("/pools/default", url.Values{"memoryQuota": {"512"}, "indexMemoryQuota": {"512"}}, true); status >= 400 {
			t.Fatalf("memory quotas: %d %s", status, body)
		}
	}
	if status, body := post("/settings/web", url.Values{
		"username": {integrationUser}, "password": {integrationPassword}, "port": {"SAME"},
	}, true); status >= 400 {
		t.Fatalf("settings/web: %d %s", status, body)
	}
	waitForHTTP(ctx, t, base+"/pools/default", 120*time.Second)
	// Community Edition only offers forestdb; Enterprise adds plasma.
	for _, mode := range []string{"plasma", "forestdb"} {
		if status, _ := post("/settings/indexes", url.Values{"storageMode": {mode}}, true); status < 400 {
			break
		}
	}
	waitForQueryService(ctx, t, host, queryPort)
}

func waitForHTTP(ctx context.Context, t *testing.T, endpoint string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode < 500 {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not become reachable: %v", endpoint, err)
		}
		time.Sleep(2 * time.Second)
	}
}

func waitForQueryService(ctx context.Context, t *testing.T, host string, port int) {
	t.Helper()
	endpoint := "http://" + net.JoinHostPort(host, strconv.Itoa(port)) + "/query/service"
	deadline := time.Now().Add(180 * time.Second)
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint,
			strings.NewReader(`{"statement":"SELECT RAW 1"}`))
		if err != nil {
			t.Fatalf("build query request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.SetBasicAuth(integrationUser, integrationPassword)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK && strings.Contains(string(body), "success") {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("the query service on port %d never became ready: %v", port, err)
		}
		time.Sleep(2 * time.Second)
	}
}
