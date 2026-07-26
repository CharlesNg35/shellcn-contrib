package milvus

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	contribtest "github.com/charlesng35/shellcn-contrib/shared/plugintest"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

const (
	integrationImage = "milvusdb/milvus:v2.6.21"
	integrationUser  = "root"
	integrationPass  = "Milvus"
)

func TestMilvusPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_MILVUS_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_MILVUS_INTEGRATION=1 to run against Milvus")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	host, port := milvusEndpoint(ctx, t)
	p := New()
	sess := connectWithRetry(ctx, t, p, host, port)
	defer func() { _ = sess.Close() }()
	if err := sess.HealthCheck(ctx); err != nil {
		t.Fatalf("health check: %v", err)
	}

	h := contribtest.NewHarness(t, p.Routes())
	suffix := time.Now().UTC().Format("20060102150405")
	collection := "shellcn_it_" + suffix
	renamed := collection + "_renamed"
	database := "shellcn_db_" + suffix
	username := "shellcn_user_" + suffix
	rolename := "shellcn_role_" + suffix
	scope := map[string]string{"database": defaultDatabase}
	collScope := map[string]string{"database": defaultDatabase, "collection": collection}

	// Cluster-level reads.
	assertField(t, h.Call(ctx, rid("overview"), sess, scope, nil, nil), "version")
	h.Call(ctx, rid("resourcegroups.list"), sess, scope, nil, nil)
	h.Call(ctx, rid("activity.list"), sess, scope, nil, nil)

	// Databases.
	h.Call(ctx, rid("database.create"), sess, scope, nil, mustJSON(t, map[string]any{"name": database}))
	defer h.CallNoFail(context.Background(), rid("database.drop"), sess, map[string]string{"database": defaultDatabase, "name": database})
	if !hasRow(pageItems(h.Call(ctx, rid("databases.list"), sess, scope, nil, nil)), "name", database) {
		t.Fatalf("created database %q not listed", database)
	}
	assertField(t, h.Call(ctx, rid("database.read"), sess, map[string]string{"database": defaultDatabase, "name": database}, nil, nil), "collection_count")

	// Collection with a designed schema, AUTOINDEX and an eager load.
	createBody := mustJSON(t, map[string]any{
		"name": collection, "description": "ShellCN integration collection",
		"consistency_level": "Strong", "shard_num": 1, "enable_dynamic_field": true,
		"auto_index": true, "metric_type": "COSINE", "load_after_create": true,
		"fields": []any{
			map[string]any{"name": "id", "type": "Int64", "primary": true},
			map[string]any{"name": "title", "type": "VarChar", "max_length": 128},
			map[string]any{"name": "bucket", "type": "Int64"},
			map[string]any{"name": "vector", "type": "FloatVector", "dim": 8},
		},
	})
	h.Call(ctx, rid("collection.create"), sess, scope, nil, createBody)
	defer h.CallNoFail(context.Background(), rid("collection.drop"), sess, collScope)

	listed := findRow(pageItems(h.Call(ctx, rid("collections.list"), sess, scope, nil, nil)), "name", collection)
	if listed == nil {
		t.Fatalf("created collection %q not listed", collection)
	}
	if listed["state"] != "loaded" || listed["field_count"] == nil {
		t.Fatalf("collection row was not enriched with load state and schema facts: %#v", listed)
	}
	// The Data grid picks its writable or read-only variant from this field.
	if listed["read_only"] != false {
		t.Fatalf("collection rows must carry the connection's read-only state: %#v", listed)
	}
	detail := asRow(t, h.Call(ctx, rid("collection.read"), sess, collScope, nil, nil))
	if detail["primary_key"] != "id" || detail["state"] != "loaded" {
		t.Fatalf("unexpected collection detail: %#v", detail)
	}
	schemaRows := pageItems(h.Call(ctx, rid("collection.schema"), sess, collScope, nil, nil))
	if !hasRow(schemaRows, "name", "vector") {
		t.Fatalf("schema rows missing the vector field: %#v", schemaRows)
	}
	graph := asRow(t, h.Call(ctx, rid("collection.graph"), sess, collScope, nil, nil))
	if len(graph["nodes"].([]any)) < 5 {
		t.Fatalf("collection graph is too small: %#v", graph)
	}
	nodes := treeNodes(h.Call(ctx, rid("tree.collections"), sess, scope, nil, nil))
	node := findRow(nodes, "label", collection)
	if node == nil {
		t.Fatalf("collection %q is missing from the tree: %#v", collection, nodes)
	}
	if data, _ := node["data"].(map[string]any); data["read_only"] != false {
		t.Fatalf("tree nodes must carry the connection's read-only state: %#v", node)
	}
	h.Call(ctx, rid("tree.partitions"), sess, collScope, nil, nil)

	// Entities: columns, insert, list, upsert, delete.
	columns := pageItems(h.Call(ctx, rid("entities.columns"), sess, collScope, nil, nil))
	if !hasRow(columns, "name", "title") {
		t.Fatalf("entity columns missing title: %#v", columns)
	}
	h.Call(ctx, rid("entity.insert"), sess, collScope, nil, mustJSON(t, map[string]any{"rows": sampleRows(24)}))
	entities := pageItems(h.Call(ctx, rid("entities.list"), sess, collScope, nil, nil))
	if len(entities) < 24 {
		t.Fatalf("expected the inserted entities back, got %d", len(entities))
	}
	if entities[0]["vector"] == nil || entities[0]["title"] == nil {
		t.Fatalf("entity rows must carry every output field: %#v", entities[0])
	}
	h.Call(ctx, rid("entity.update"), sess, collScope, nil, mustJSON(t, map[string]any{
		"rows": []any{map[string]any{"id": 1, "title": "updated", "bucket": 9, "vector": vectorFor(1)}},
	}))
	updated := pageItems(h.Call(ctx, rid("entities.list"), sess, collScope, nil, nil))
	if !hasRow(updated, "title", "updated") {
		t.Fatalf("upsert did not take effect: %#v", updated)
	}
	h.Call(ctx, rid("entity.delete"), sess, collScope, nil, mustJSON(t, map[string]any{"rows": []any{map[string]any{"_id": 23}}}))
	if hasRow(pageItems(h.Call(ctx, rid("entities.list"), sess, collScope, nil, nil)), "id", "23") {
		t.Fatal("deleted entity is still listed")
	}

	// Query console: ANN search, scalar filter, and completions.
	searchOut := h.Stream(ctx, rid("query"), sess, collScope, nil, mustJSONLine(t, map[string]any{
		"query": `{"field":"vector","vector":` + jsonText(vectorFor(3)) + `,"limit":5,"output":["*"]}`,
	}))
	if search := firstFrame(t, searchOut); toInt(search["rowCount"]) < 1 {
		t.Fatalf("ANN search returned no rows: %#v", search)
	}
	filterOut := h.Stream(ctx, rid("query"), sess, collScope, nil, mustJSONLine(t, map[string]any{
		"query": `{"filter":"bucket >= 0","limit":5,"output":["id","title"]}`,
	}))
	if filtered := firstFrame(t, filterOut); toInt(filtered["rowCount"]) < 1 {
		t.Fatalf("filter query returned no rows: %#v", filtered)
	}
	completions := pageItems(h.Call(ctx, rid("query.complete"), sess, collScope, nil, nil))
	if !hasRow(completions, "label", "vector") {
		t.Fatalf("completions missing the vector field: %#v", completions)
	}

	// The console ships with a pre-filled query and a set of templates; both run
	// unedited against whatever schema the opened collection has.
	canned := []string{queryEditorConfig().InitialQuery}
	for _, item := range completions {
		if item["type"] == "template" {
			canned = append(canned, fmt.Sprint(item["apply"]))
		}
	}
	if len(canned) < 5 {
		t.Fatalf("expected the query templates in the completions: %#v", completions)
	}
	for _, query := range canned {
		frame := firstFrame(t, h.Stream(ctx, rid("query"), sess, collScope, nil,
			mustJSONLine(t, map[string]any{"query": query, "confirm": false})))
		if frame["error"] != nil {
			t.Fatalf("default query %s failed: %v", query, frame["error"])
		}
		if frame["confirmRequired"] != nil {
			t.Fatalf("default query %s must not need a confirmation: %#v", query, frame)
		}
	}
	if rows := toInt(firstFrame(t, h.Stream(ctx, rid("query"), sess, collScope, nil,
		mustJSONLine(t, map[string]any{"query": queryEditorConfig().InitialQuery})))["rowCount"]); rows < 1 {
		t.Fatal("the pre-filled query must return rows on a seeded collection")
	}

	// Saved queries live in scoped plugin storage, so they need a storage bridge.
	store := &fakeStorage{}
	saved := callWithStorage(ctx, t, h.Route(rid("query.save")), sess, store, nil, mustJSON(t, map[string]any{
		"name": "integration", "collection": collection, "query": `{"filter":"id > 0","limit":1}`,
	}))
	savedID, _ := asRow(t, saved)["id"].(string)
	if items := pageItems(callWithStorage(ctx, t, h.Route(rid("queries.list")), sess, store, nil, nil)); len(items) != 1 {
		t.Fatalf("expected one saved query, got %#v", items)
	}
	callWithStorage(ctx, t, h.Route(rid("query.delete")), sess, store, map[string]string{"id": savedID}, nil)

	// Live panels: metrics, watch, canvas projection, task progress.
	if cluster := firstFrame(t, h.Stream(shortCtx(ctx, 6*time.Second), rid("overview.metrics"), sess, scope, nil, nil)); toInt(cluster["collections"]) < 1 {
		t.Fatalf("cluster metrics must count the live collections: %#v", cluster)
	}
	assertStreamFrame(t, h.Stream(shortCtx(ctx, 6*time.Second), rid("collection.metrics"), sess, collScope, nil, nil), "entities")
	assertStreamFrame(t, h.Stream(shortCtx(ctx, 6*time.Second), rid("collection.watch"), sess, collScope, nil, nil), "primary_key")
	projectionOut := h.Stream(shortCtx(ctx, 30*time.Second), rid("collection.projection"), sess, collScope, nil,
		mustJSONLine(t, map[string]any{"type": "ready", "width": 1200, "height": 720, "dpr": 1, "theme": "dark"}))
	// The status line proves the explorer sampled real vectors and reduced them,
	// not just that it painted an empty plot.
	for _, want := range []string{`"commands"`, "action:resample", " dims ", "PC1"} {
		if !strings.Contains(string(projectionOut), want) {
			t.Fatalf("projection canvas frame is missing %q: %s", want, truncate(string(projectionOut), 4000))
		}
	}

	// Indexes.
	indexes := pageItems(h.Call(ctx, rid("indexes.list"), sess, collScope, nil, nil))
	if !hasRow(indexes, "name", "vector") {
		t.Fatalf("AUTOINDEX on the vector field is missing: %#v", indexes)
	}
	indexScope := map[string]string{"database": defaultDatabase, "collection": collection, "index": "vector"}
	assertField(t, h.Call(ctx, rid("index.read"), sess, indexScope, nil, nil), "index_type")
	assertStreamFrame(t, h.Stream(shortCtx(ctx, 30*time.Second), rid("index.progress"), sess, indexScope, nil, nil), "status")

	// Maintenance.
	h.Call(ctx, rid("collection.flush"), sess, collScope, nil, nil)
	h.Call(ctx, rid("collection.compact"), sess, collScope, nil, nil)
	segments := pageItems(h.Call(ctx, rid("segments.list"), sess, collScope, nil, nil))
	if len(segments) == 0 {
		t.Fatal("expected at least one persistent segment after flushing")
	}
	for _, segment := range segments {
		if state := fmt.Sprint(segment["state"]); segmentSeverities[state] == "" {
			t.Fatalf("segment state %q has no badge severity: %#v", state, segment)
		}
	}

	// Aliases.
	alias := collection + "_alias"
	h.Call(ctx, rid("alias.create"), sess, collScope, nil, mustJSON(t, map[string]any{"name": alias}))
	if !hasRow(pageItems(h.Call(ctx, rid("aliases.list"), sess, collScope, nil, nil)), "alias", alias) {
		t.Fatalf("alias %q not listed", alias)
	}
	h.Call(ctx, rid("alias.drop"), sess, map[string]string{"database": defaultDatabase, "alias": alias}, nil, nil)

	// Release, then exercise partitions and a scalar index that needs an unloaded collection.
	h.Call(ctx, rid("collection.release"), sess, collScope, nil, nil)
	partition := "p_" + suffix
	partScope := map[string]string{"database": defaultDatabase, "collection": collection, "partition": partition}
	h.Call(ctx, rid("partition.create"), sess, collScope, nil, mustJSON(t, map[string]any{"name": partition}))
	if !hasRow(pageItems(h.Call(ctx, rid("partitions.list"), sess, collScope, nil, nil)), "name", partition) {
		t.Fatalf("partition %q not listed", partition)
	}
	assertField(t, h.Call(ctx, rid("partition.read"), sess, partScope, nil, nil), "state")
	h.Call(ctx, rid("partition.load"), sess, partScope, nil, nil)
	h.Call(ctx, rid("partition.release"), sess, partScope, nil, nil)
	h.Call(ctx, rid("partition.drop"), sess, partScope, nil, nil)

	h.Call(ctx, rid("index.create"), sess, collScope, nil, mustJSON(t, map[string]any{
		"field_name": "bucket", "index_name": "bucket_idx", "index_type": "INVERTED",
	}))
	scalarScope := map[string]string{"database": defaultDatabase, "collection": collection, "index": "bucket_idx"}
	if owner := asRow(t, h.Call(ctx, rid("index.read"), sess, scalarScope, nil, nil))["field_name"]; owner != "bucket" {
		t.Fatalf("index.read must resolve the indexed field, got %v", owner)
	}
	h.Call(ctx, rid("index.drop"), sess, scalarScope, nil, nil)
	h.Call(ctx, rid("collection.field.add"), sess, collScope, nil, mustJSON(t, map[string]any{
		"name": "note", "type": "VarChar", "max_length": 64, "nullable": true,
	}))
	h.Call(ctx, rid("collection.load"), sess, collScope, nil, nil)

	// RBAC.
	h.Call(ctx, rid("user.create"), sess, scope, nil, mustJSON(t, map[string]any{"name": username, "password": "Shellcn123"}))
	defer h.CallNoFail(context.Background(), rid("user.delete"), sess, map[string]string{"database": defaultDatabase, "user": username})
	if !hasRow(pageItems(h.Call(ctx, rid("users.list"), sess, scope, nil, nil)), "name", username) {
		t.Fatalf("user %q not listed", username)
	}
	userScope := map[string]string{"database": defaultDatabase, "user": username}
	assertField(t, h.Call(ctx, rid("user.read"), sess, userScope, nil, nil), "roles")
	h.Call(ctx, rid("user.password"), sess, userScope, nil, mustJSON(t, map[string]any{
		"old_password": "Shellcn123", "new_password": "Shellcn456",
	}))

	h.Call(ctx, rid("role.create"), sess, scope, nil, mustJSON(t, map[string]any{"name": rolename}))
	defer h.CallNoFail(context.Background(), rid("role.delete"), sess, map[string]string{"database": defaultDatabase, "role": rolename})
	if !hasRow(pageItems(h.Call(ctx, rid("roles.list"), sess, scope, nil, nil)), "name", rolename) {
		t.Fatalf("role %q not listed", rolename)
	}
	roleScope := map[string]string{"database": defaultDatabase, "role": rolename}
	h.Call(ctx, rid("role.privilege.grant"), sess, roleScope, nil, mustJSON(t, map[string]any{
		"privilege": "Search", "collection": collection, "db": defaultDatabase,
	}))
	privileges := pageItems(h.Call(ctx, rid("role.privileges"), sess, roleScope, nil, nil))
	if !hasRow(privileges, "privilege", "Search") {
		t.Fatalf("granted privilege missing: %#v", privileges)
	}
	h.Call(ctx, rid("user.role.grant"), sess, userScope, nil, mustJSON(t, map[string]any{"role": rolename}))
	if users, _ := asRow(t, h.Call(ctx, rid("role.read"), sess, roleScope, nil, nil))["users"].([]any); len(users) == 0 {
		t.Fatal("role.read should report the granted user")
	}
	if !hasRow(pageItems(h.Call(ctx, rid("user.roles"), sess, userScope, nil, nil)), "role", rolename) {
		t.Fatalf("granted role %q missing from the user roles table", rolename)
	}
	h.Call(ctx, rid("user.role.revoke"), sess, map[string]string{"database": defaultDatabase, "user": username, "role": rolename}, nil, nil)
	h.Call(ctx, rid("role.privilege.revoke"), sess, roleScope, nil, mustJSON(t, map[string]any{
		"privilege": "Search", "collection": collection, "db": defaultDatabase,
	}))
	h.Call(ctx, rid("role.delete"), sess, roleScope, nil, nil)
	h.Call(ctx, rid("user.delete"), sess, userScope, nil, nil)

	// The activity feed records everything this session performed.
	activity := pageItems(h.Call(ctx, rid("activity.list"), sess, scope, nil, nil))
	if !hasRow(activity, "operation", "collection.create") {
		t.Fatalf("activity feed is missing the collection creation: %#v", activity)
	}

	// Rename, then drop everything the test created.
	h.Call(ctx, rid("collection.rename"), sess, collScope, nil, mustJSON(t, map[string]any{"name": renamed}))
	h.Call(ctx, rid("collection.drop"), sess, map[string]string{"database": defaultDatabase, "collection": renamed}, nil, nil)
	h.Call(ctx, rid("database.drop"), sess, map[string]string{"database": defaultDatabase, "name": database}, nil, nil)

	h.AssertAllCovered()
}

// connectWithRetry tolerates the window where the health endpoint is already
// serving but the Milvus proxy has not finished registering.
func connectWithRetry(ctx context.Context, t *testing.T, p plugin.Plugin, host string, port int) plugin.Session {
	t.Helper()
	cfg := plugin.ConnectConfig{
		Config: map[string]any{
			"host": host, "port": port, "database": defaultDatabase,
			"auth": authPassword, "username": integrationUser, "password": integrationPass,
			"tls_mode": "disable", "read_only": false,
			"timeout": "60s", "page_limit": 200, "sample_limit": 128,
		},
		Net: plugintest.DirectTransport(),
	}
	deadline := time.Now().Add(4 * time.Minute)
	for {
		sess, err := p.Connect(ctx, cfg)
		if err == nil {
			return sess
		}
		if time.Now().After(deadline) {
			t.Fatalf("connect: %v", err)
		}
		time.Sleep(3 * time.Second)
	}
}

func milvusEndpoint(ctx context.Context, t *testing.T) (string, int) {
	t.Helper()
	if address := strings.TrimSpace(os.Getenv("SHELLCN_MILVUS_ADDRESS")); address != "" {
		host, portText, err := net.SplitHostPort(address)
		if err != nil {
			t.Fatalf("SHELLCN_MILVUS_ADDRESS must be host:port: %v", err)
		}
		port, ok := parseInt(portText)
		if !ok {
			t.Fatalf("SHELLCN_MILVUS_ADDRESS has an invalid port %q", portText)
		}
		return host, port
	}
	return startMilvusContainer(ctx, t)
}

// startMilvusContainer runs the official standalone image with embedded etcd and
// local storage, which is the single-container deployment Milvus documents.
func startMilvusContainer(ctx context.Context, t *testing.T) (string, int) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_MILVUS_ADDRESS is not set")
	}
	// Only the two config files are bind-mounted; Milvus keeps its data inside
	// the container so the temp dir stays owned by the test user.
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("chmod temp dir: %v", err)
	}
	etcdConfig := filepath.Join(dir, "embedEtcd.yaml")
	userConfig := filepath.Join(dir, "user.yaml")
	writeFile(t, etcdConfig, "listen-client-urls: http://0.0.0.0:2379\nadvertise-client-urls: http://0.0.0.0:2379\nquota-backend-bytes: 4294967296\nauto-compaction-mode: revision\nauto-compaction-retention: '1000'\n")
	writeFile(t, userConfig, "common:\n  security:\n    authorizationEnabled: true\n")

	name := "shellcn-milvus-it-" + time.Now().UTC().Format("20060102150405")
	run(ctx, t, "docker", "run", "-d", "--name", name,
		"--security-opt", "seccomp:unconfined",
		"-e", "ETCD_USE_EMBED=true",
		"-e", "ETCD_DATA_DIR=/var/lib/milvus/etcd",
		"-e", "ETCD_CONFIG_PATH=/milvus/configs/embedEtcd.yaml",
		"-e", "COMMON_STORAGETYPE=local",
		"-e", "DEPLOY_MODE=STANDALONE",
		"-v", etcdConfig+":/milvus/configs/embedEtcd.yaml:ro",
		"-v", userConfig+":/milvus/configs/user.yaml:ro",
		"-p", "127.0.0.1::19530", "-p", "127.0.0.1::9091",
		integrationImage, "milvus", "run", "standalone")
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})

	host, port := containerPort(ctx, t, name, "19530/tcp")
	healthHost, healthPort := containerPort(ctx, t, name, "9091/tcp")
	healthURL := fmt.Sprintf("http://%s/healthz", net.JoinHostPort(healthHost, fmt.Sprint(healthPort)))

	// Milvus standalone bootstraps etcd, storage, and every coordinator, so the
	// readiness window is minutes rather than seconds.
	deadline := time.Now().Add(6 * time.Minute)
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			healthy := resp.StatusCode == http.StatusOK
			_ = resp.Body.Close()
			if healthy {
				return host, port
			}
		}
		if time.Now().After(deadline) {
			logs, _ := exec.CommandContext(ctx, "docker", "logs", "--tail", "40", name).CombinedOutput()
			t.Fatalf("Milvus container did not become healthy: %v\n%s", err, logs)
		}
		time.Sleep(2 * time.Second)
	}
}

func containerPort(ctx context.Context, t *testing.T, name, target string) (string, int) {
	t.Helper()
	out := strings.TrimSpace(run(ctx, t, "docker", "port", name, target))
	if idx := strings.IndexByte(out, '\n'); idx >= 0 {
		out = out[:idx]
	}
	host, portText, err := net.SplitHostPort(strings.TrimSpace(out))
	if err != nil {
		t.Fatalf("unexpected docker port output %q: %v", out, err)
	}
	port, ok := parseInt(portText)
	if !ok {
		t.Fatalf("unexpected docker port %q", portText)
	}
	return host, port
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func run(ctx context.Context, t *testing.T, name string, args ...string) string {
	t.Helper()
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
	return string(out)
}

func shortCtx(parent context.Context, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(parent, d)
	go func() {
		<-ctx.Done()
		cancel()
	}()
	return ctx
}

func callWithStorage(ctx context.Context, t *testing.T, route plugin.Route, sess plugin.Session, store plugin.Storage, params map[string]string, body []byte) any {
	t.Helper()
	rc := plugin.NewRequestContext(ctx, plugin.User{}, sess, params, nil, body).WithStorage(store)
	out, err := route.Handle(rc)
	if err != nil {
		t.Fatalf("%s: %v", route.ID, err)
	}
	return out
}

func sampleRows(count int) []any {
	rows := make([]any, 0, count)
	for i := 0; i < count; i++ {
		rows = append(rows, map[string]any{
			"id": i, "title": fmt.Sprintf("doc-%02d", i), "bucket": i % 4, "vector": vectorFor(i),
		})
	}
	return rows
}

func vectorFor(i int) []float64 {
	vector := make([]float64, 8)
	for j := range vector {
		vector[j] = float64((i%5)+1)*0.13 + float64(j)*0.017
	}
	return vector
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func mustJSONLine(t *testing.T, value any) []byte {
	return append(mustJSON(t, value), '\n')
}

func jsonText(value any) string {
	data, _ := json.Marshal(value)
	return string(data)
}

func pageItems(page any) []map[string]any {
	data, _ := json.Marshal(page)
	var decoded struct {
		Items []map[string]any `json:"items"`
	}
	_ = json.Unmarshal(data, &decoded)
	return decoded.Items
}

func treeNodes(page any) []map[string]any { return pageItems(page) }

func asRow(t *testing.T, value any) map[string]any {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("result is not an object: %v", err)
	}
	return decoded
}

func assertField(t *testing.T, value any, key string) {
	t.Helper()
	if _, ok := asRow(t, value)[key]; !ok {
		t.Fatalf("result is missing %q: %#v", key, value)
	}
}

func assertStreamFrame(t *testing.T, out []byte, key string) {
	t.Helper()
	if !strings.Contains(string(out), `"`+key+`"`) {
		t.Fatalf("stream frame is missing %q: %s", key, truncate(string(out), 400))
	}
}

func hasRow(items []map[string]any, key, want string) bool {
	return findRow(items, key, want) != nil
}

func findRow(items []map[string]any, key, want string) map[string]any {
	for _, item := range items {
		if fmt.Sprint(item[key]) == want {
			return item
		}
	}
	return nil
}

func firstFrame(t *testing.T, out []byte) map[string]any {
	t.Helper()
	frame := strings.TrimSpace(string(out))
	if idx := strings.IndexByte(frame, '\n'); idx >= 0 {
		frame = frame[:idx]
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(frame), &decoded); err != nil {
		t.Fatalf("stream frame is not JSON: %v (%s)", err, truncate(string(out), 400))
	}
	return decoded
}

func toInt(value any) int {
	number, _ := value.(float64)
	return int(number)
}
