package neo4j

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestNeo4jPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_NEO4J_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_NEO4J_INTEGRATION=1 to run against Neo4j")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	cfg := neo4jIntegrationConfig(ctx, t)
	p := New()
	sess, err := p.Connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	s := sess.(*Session)
	routes := plugintest.RouteMap(p.Routes())
	db := fmt.Sprint(cfg["database"])

	_, _ = executeCypher(ctx, s, db, sqldb.QueryRequest{Query: "MATCH (n:ShellCNIT) DETACH DELETE n", Confirm: true})
	defer func() {
		_, _ = executeCypher(context.Background(), s, db, sqldb.QueryRequest{Query: "MATCH (n:ShellCNIT) DETACH DELETE n", Confirm: true})
	}()

	a := call(ctx, t, routes[rid("node.create")], sess, map[string]string{"database": db}, nil, testJSON(t, map[string]any{
		"labels": "ShellCNIT Person", "properties": map[string]any{"name": "Ada", "kind": "engineer"},
	})).(row)
	b := call(ctx, t, routes[rid("node.create")], sess, map[string]string{"database": db}, nil, testJSON(t, map[string]any{
		"labels": "ShellCNIT Person", "properties": map[string]any{"name": "Charles", "kind": "operator"},
	})).(row)
	rel := call(ctx, t, routes[rid("relationship.create")], sess, map[string]string{"database": db}, nil, testJSON(t, map[string]any{
		"start_element_id": fmt.Sprint(a["element_id"]),
		"end_element_id":   fmt.Sprint(b["element_id"]),
		"type":             "KNOWS",
		"properties":       map[string]any{"since": int64(2026)},
	})).(row)

	call(ctx, t, routes[rid("databases.tree")], sess, nil, nil, nil)
	// The administrative `system` database is excluded so its data tabs never run
	// MATCH (which it rejects).
	dbList := pageItems(call(ctx, t, routes[rid("databases.list")], sess, nil, nil, nil))
	if hasRowName(dbList, "system") {
		t.Fatalf("system database must not be listed: %#v", dbList)
	}
	call(ctx, t, routes[rid("database.overview")], sess, map[string]string{"database": db}, nil, nil)
	call(ctx, t, routes[rid("labels.tree")], sess, nil, nil, nil)
	labels := pageItems(call(ctx, t, routes[rid("labels.list")], sess, nil, url.Values{"p.database": []string{db}}, nil))
	if !hasRowName(labels, "ShellCNIT") {
		t.Fatalf("expected ShellCNIT label in %#v", labels)
	}
	call(ctx, t, routes[rid("label.overview")], sess, map[string]string{"database": db, "label": "ShellCNIT"}, nil, nil)
	call(ctx, t, routes[rid("relationship_types.tree")], sess, nil, nil, nil)
	types := pageItems(call(ctx, t, routes[rid("relationship_types.list")], sess, nil, url.Values{"p.database": []string{db}}, nil))
	if !hasRowName(types, "KNOWS") {
		t.Fatalf("expected KNOWS type in %#v", types)
	}
	call(ctx, t, routes[rid("relationship_type.overview")], sess, map[string]string{"database": db, "type": "KNOWS"}, nil, nil)
	call(ctx, t, routes[rid("indexes.list")], sess, nil, url.Values{"p.database": []string{db}}, nil)
	call(ctx, t, routes[rid("constraints.list")], sess, nil, url.Values{"p.database": []string{db}}, nil)
	call(ctx, t, routes[rid("schema.list")], sess, nil, url.Values{"p.database": []string{db}}, nil)
	call(ctx, t, routes[rid("schema.tree")], sess, nil, url.Values{"p.database": []string{db}}, nil)

	indexName := "shellcn_it_idx"
	call(ctx, t, routes[rid("index.create")], sess, map[string]string{"database": db}, nil, testJSON(t, map[string]any{
		"name": indexName, "entity_type": "node", "label": "ShellCNIT", "properties": "name",
	}))
	indexes := pageItems(call(ctx, t, routes[rid("indexes.list")], sess, nil, url.Values{"p.database": []string{db}}, nil))
	if !hasRowName(indexes, indexName) {
		t.Fatalf("expected created index %q in %#v", indexName, indexes)
	}
	call(ctx, t, routes[rid("schema.drop")], sess, map[string]string{"id": mustEncodeID("index", db, indexName)}, nil, nil)
	indexes = pageItems(call(ctx, t, routes[rid("indexes.list")], sess, nil, url.Values{"p.database": []string{db}}, nil))
	if hasRowName(indexes, indexName) {
		t.Fatalf("index %q still present after drop: %#v", indexName, indexes)
	}

	nodes := pageItems(call(ctx, t, routes[rid("nodes.list")], sess, nil, url.Values{"p.database": []string{db}, "p.label": []string{"ShellCNIT"}}, nil))
	if len(nodes) < 2 {
		t.Fatalf("expected nodes, got %#v", nodes)
	}
	nodeRef := nodes[0]["ref"].(map[string]any)
	nodeID := fmt.Sprint(nodeRef["uid"])
	call(ctx, t, routes[rid("node.read")], sess, map[string]string{"id": nodeID}, nil, nil)
	call(ctx, t, routes[rid("node.relationships")], sess, map[string]string{"id": nodeID}, nil, nil)

	// Node property update: the {"properties":{...}} body replaces the property map,
	// read it back via node.properties and assert the change.
	updated := call(ctx, t, routes[rid("node.update")], sess, map[string]string{"id": nodeID}, nil, testJSON(t, map[string]any{
		"properties": map[string]any{"name": "Ada", "kind": "engineer", "team": "core"},
	})).(row)
	if props := asMap(updated["properties"]); fmt.Sprint(props["team"]) != "core" {
		t.Fatalf("expected updated node properties to include team=core, got %#v", updated["properties"])
	}
	readback := asMap(call(ctx, t, routes[rid("node.properties")], sess, map[string]string{"id": nodeID}, nil, nil))
	if fmt.Sprint(readback["team"]) != "core" {
		t.Fatalf("node.properties did not reflect update: %#v", readback)
	}
	// The code-editor body {"content":"<json>"} is accepted as well; this replaces
	// the map, so the previously-set team key is cleared.
	editor := call(ctx, t, routes[rid("node.update")], sess, map[string]string{"id": nodeID}, nil, testJSON(t, map[string]any{
		"content": `{"name":"Ada","kind":"engineer"}`,
	})).(row)
	if props := asMap(editor["properties"]); fmt.Sprint(props["team"]) != "<nil>" {
		t.Fatalf("expected content-editor update to clear team, got %#v", editor["properties"])
	}

	// Relationship property update mirrors the node path.
	relList := pageItems(call(ctx, t, routes[rid("relationships.list")], sess, nil, url.Values{"p.database": []string{db}, "p.type": []string{"KNOWS"}}, nil))
	if len(relList) != 1 {
		t.Fatalf("expected one KNOWS relationship, got %#v", relList)
	}
	relID := fmt.Sprint(relList[0]["ref"].(map[string]any)["uid"])
	relUpdated := call(ctx, t, routes[rid("relationship.update")], sess, map[string]string{"id": relID}, nil, testJSON(t, map[string]any{
		"properties": map[string]any{"since": int64(2026), "weight": int64(5)},
	})).(row)
	if props := asMap(relUpdated["properties"]); fmt.Sprint(props["weight"]) != "5" {
		t.Fatalf("expected relationship update to set weight=5, got %#v", relUpdated["properties"])
	}

	// Constraint create round-trip: create a uniqueness constraint, confirm it is
	// listed, then drop it via schema.drop and confirm it is gone.
	constraintName := "shellcn_it_unique"
	call(ctx, t, routes[rid("constraint.create")], sess, map[string]string{"database": db}, nil, testJSON(t, map[string]any{
		"name": constraintName, "entity_type": "node", "type": "unique", "label": "ShellCNIT", "properties": "name",
	}))
	constraints := pageItems(call(ctx, t, routes[rid("constraints.list")], sess, nil, url.Values{"p.database": []string{db}}, nil))
	if !hasRowName(constraints, constraintName) {
		t.Fatalf("expected created constraint %q in %#v", constraintName, constraints)
	}
	call(ctx, t, routes[rid("schema.drop")], sess, map[string]string{"id": mustEncodeID("constraint", db, constraintName)}, nil, nil)
	constraints = pageItems(call(ctx, t, routes[rid("constraints.list")], sess, nil, url.Values{"p.database": []string{db}}, nil))
	if hasRowName(constraints, constraintName) {
		t.Fatalf("constraint %q still present after drop: %#v", constraintName, constraints)
	}

	rels := pageItems(call(ctx, t, routes[rid("relationships.list")], sess, nil, url.Values{"p.database": []string{db}, "p.type": []string{"KNOWS"}}, nil))
	if len(rels) != 1 {
		t.Fatalf("expected relationship, got %#v", rels)
	}
	relRef := rels[0]["ref"].(map[string]any)
	call(ctx, t, routes[rid("relationship.read")], sess, map[string]string{"id": fmt.Sprint(relRef["uid"])}, nil, nil)
	graph := call(ctx, t, routes[rid("graph")], sess, nil, url.Values{"p.database": []string{db}}, nil)
	if payload := graphPayloadFromAny(graph); len(payload.Nodes) < 2 || len(payload.Edges) < 1 {
		t.Fatalf("expected graph data, got %#v", graph)
	}
	call(ctx, t, routes[rid("label.graph")], sess, map[string]string{"database": db, "label": "ShellCNIT"}, nil, nil)
	call(ctx, t, routes[rid("relationship_type.graph")], sess, map[string]string{"database": db, "type": "KNOWS"}, nil, nil)

	// Click-to-expand: a node's neighbourhood includes its KNOWS edge.
	expanded := call(ctx, t, routes[rid("node.graph")], sess, nil, url.Values{"p.database": []string{db}, "p.node": []string{fmt.Sprint(a["element_id"])}}, nil)
	if payload := graphPayloadFromAny(expanded); len(payload.Edges) < 1 || len(payload.Nodes) < 2 {
		t.Fatalf("expected node expansion to return neighbours, got %#v", expanded)
	}

	call(ctx, t, routes[rid("completion")], sess, nil, nil, nil)

	result, err := executeCypher(ctx, s, db, sqldb.QueryRequest{Query: "MATCH (n:ShellCNIT) RETURN count(n) AS nodes"})
	if err != nil || result.RowCount != 1 {
		t.Fatalf("cypher query: result=%#v err=%v", result, err)
	}
	call(ctx, t, routes[rid("relationship.delete")], sess, map[string]string{"id": mustEncodeID("relationship", db, fmt.Sprint(rel["element_id"]))}, nil, nil)
	for _, item := range nodes {
		ref := item["ref"].(map[string]any)
		call(ctx, t, routes[rid("node.delete")], sess, map[string]string{"id": fmt.Sprint(ref["uid"])}, nil, nil)
	}
}

func neo4jIntegrationConfig(ctx context.Context, t *testing.T) map[string]any {
	t.Helper()
	host := os.Getenv("SHELLCN_NEO4J_HOST")
	port := os.Getenv("SHELLCN_NEO4J_PORT")
	password := os.Getenv("SHELLCN_NEO4J_PASSWORD")
	if host == "" || port == "" {
		host, port, password = startNeo4jContainer(ctx, t)
	}
	if password == "" {
		password = "shellcn-test-password"
	}
	db := os.Getenv("SHELLCN_NEO4J_DATABASE")
	if db == "" {
		db = defaultDatabase
	}
	return map[string]any{
		"scheme":                     "bolt",
		"host":                       host,
		"port":                       mustAtoi(port),
		"database":                   db,
		"auth":                       authPassword,
		"username":                   "neo4j",
		"password":                   password,
		"read_only":                  false,
		"require_write_confirmation": false,
		"query_timeout":              "30s",
		"connect_timeout":            "20s",
		"retry_time":                 "10s",
		"pool_size":                  8,
		"fetch_size":                 100,
		"page_limit":                 100,
	}
}

func startNeo4jContainer(ctx context.Context, t *testing.T) (string, string, string) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_NEO4J_HOST/SHELLCN_NEO4J_PORT are not set")
	}
	password := "shellcn-test-password"
	name := "shellcn-neo4j-it-" + time.Now().UTC().Format("20060102150405")
	run(ctx, t, "docker", "run", "-d", "--rm", "--name", name,
		"-p", "127.0.0.1::7687",
		"-e", "NEO4J_AUTH=neo4j/"+password,
		"-e", "NEO4J_server_memory_heap_initial__size=256m",
		"-e", "NEO4J_server_memory_heap_max__size=512m",
		"neo4j:latest")
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})
	out := run(ctx, t, "docker", "port", name, "7687/tcp")
	host, port, err := net.SplitHostPort(strings.TrimSpace(out))
	if err != nil {
		t.Fatalf("unexpected docker port output: %q", out)
	}
	cfg := map[string]any{"scheme": "bolt", "host": host, "port": mustAtoi(port), "database": defaultDatabase, "auth": authPassword, "username": "neo4j", "password": password, "query_timeout": "20s", "connect_timeout": "10s", "pool_size": 4, "fetch_size": 100, "page_limit": 100}
	deadline := time.Now().Add(120 * time.Second)
	for {
		p := New()
		sess, err := p.Connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
		if err == nil {
			_ = sess.Close()
			return host, port, password
		}
		if time.Now().After(deadline) {
			logs := exec.CommandContext(ctx, "docker", "logs", name)
			out, _ := logs.CombinedOutput()
			t.Fatalf("Neo4j container did not become ready: %v\n%s", err, out)
		}
		time.Sleep(2 * time.Second)
	}
}

func call(ctx context.Context, t *testing.T, route plugin.Route, sess plugin.Session, params map[string]string, query url.Values, body []byte) any {
	t.Helper()
	out, err := route.Handle(plugin.NewRequestContext(ctx, plugin.User{}, sess, params, query, body))
	if err != nil {
		t.Fatalf("%s: %v", route.ID, err)
	}
	return out
}

func pageItems(page any) []map[string]any {
	data, _ := json.Marshal(page)
	var decoded struct {
		Items []map[string]any `json:"items"`
	}
	_ = json.Unmarshal(data, &decoded)
	return decoded.Items
}

func graphPayloadFromAny(value any) graphPayload {
	data, _ := json.Marshal(value)
	var out graphPayload
	_ = json.Unmarshal(data, &out)
	return out
}

func testJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func run(ctx context.Context, t *testing.T, name string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
	return string(out)
}

func hasRowName(rows []map[string]any, name string) bool {
	for _, item := range rows {
		if fmt.Sprint(item["name"]) == name {
			return true
		}
	}
	return false
}

func mustAtoi(raw string) int {
	n, _ := strconv.Atoi(strings.TrimSpace(raw))
	return n
}

func TestNeo4jListBoundedPagingIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_NEO4J_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_NEO4J_INTEGRATION=1 to run against Neo4j")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	cfg := neo4jIntegrationConfig(ctx, t)
	p := New()
	sess, err := p.Connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	s := sess.(*Session)
	routes := plugintest.RouteMap(p.Routes())
	db := fmt.Sprint(cfg["database"])

	const (
		nodeCount = 310
		relCount  = 130
		limit     = 25
	)

	purge := func(ctx context.Context) {
		_, _ = executeCypher(ctx, s, db, sqldb.QueryRequest{Query: "MATCH (n:ShellCNPagingIT) DETACH DELETE n", Confirm: true})
	}
	purge(ctx)
	defer purge(context.Background())

	seedCypher(ctx, t, s, db, fmt.Sprintf(
		"UNWIND range(1, %d) AS i CREATE (:ShellCNPagingIT:ShellCNPagingNode {seq: i})", nodeCount))
	seedCypher(ctx, t, s, db, fmt.Sprintf(
		"UNWIND range(1, %d) AS i CREATE (:ShellCNPagingIT:ShellCNPagingEnd {seq: i})-[:SHELLCN_PAGING_REL {seq: i}]->(:ShellCNPagingIT:ShellCNPagingEnd {seq: i})", relCount))

	assertBoundedPaging(ctx, t, routes[rid("nodes.list")], sess,
		url.Values{"p.database": []string{db}, "p.label": []string{"ShellCNPagingNode"}}, limit, nodeCount)
	assertBoundedPaging(ctx, t, routes[rid("relationships.list")], sess,
		url.Values{"p.database": []string{db}, "p.type": []string{"SHELLCN_PAGING_REL"}}, limit, relCount)
}

// assertBoundedPaging drives a list route across its cursor pages and asserts the
// handler returns one bounded page per call instead of the whole collection: the
// first page is exactly limit long, every page carries no exact Total, cursors walk
// disjoint pages, and the walk terminates having covered every seeded seq once.
func assertBoundedPaging(ctx context.Context, t *testing.T, route plugin.Route, sess plugin.Session, base url.Values, limit, seeded int) {
	t.Helper()
	if seeded <= limit {
		t.Fatalf("%s: seeded %d items must exceed the page limit %d", route.ID, seeded, limit)
	}

	query := func(cursor string, limit int) url.Values {
		q := url.Values{}
		for key, values := range base {
			q[key] = values
		}
		if limit > 0 {
			q.Set("limit", strconv.Itoa(limit))
		}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		return q
	}

	first, firstNext, firstRaw := decodePage(call(ctx, t, route, sess, nil, query("", limit), nil))
	if len(first) != limit {
		t.Fatalf("%s: first page returned %d items, want exactly the %d-item page (seeded %d)", route.ID, len(first), limit, seeded)
	}
	if _, ok := firstRaw["total"]; ok {
		t.Fatalf("%s: page must omit an exact total, got total=%v", route.ID, firstRaw["total"])
	}
	if firstNext == "" {
		t.Fatalf("%s: expected a non-empty nextCursor with %d items seeded", route.ID, seeded)
	}

	second, secondNext, _ := decodePage(call(ctx, t, route, sess, nil, query(firstNext, limit), nil))
	if len(second) != limit {
		t.Fatalf("%s: second page returned %d items, want %d", route.ID, len(second), limit)
	}
	if secondNext == "" || secondNext == firstNext {
		t.Fatalf("%s: cursor did not advance: first=%q second=%q", route.ID, firstNext, secondNext)
	}
	for _, id := range pageElementIDs(t, route.ID, first) {
		for _, other := range pageElementIDs(t, route.ID, second) {
			if id == other {
				t.Fatalf("%s: page 2 overlaps page 1 on element %q", route.ID, id)
			}
		}
	}

	seen := map[string]int{}
	seqs := map[int]int{}
	cursor := ""
	pages := 0
	for {
		items, next, raw := decodePage(call(ctx, t, route, sess, nil, query(cursor, limit), nil))
		pages++
		if len(items) > limit {
			t.Fatalf("%s: page %d returned %d items, above the requested limit %d", route.ID, pages, len(items), limit)
		}
		if _, ok := raw["total"]; ok {
			t.Fatalf("%s: page %d must omit an exact total, got total=%v", route.ID, pages, raw["total"])
		}
		for _, id := range pageElementIDs(t, route.ID, items) {
			seen[id]++
			if seen[id] > 1 {
				t.Fatalf("%s: element %q returned on more than one page", route.ID, id)
			}
		}
		for _, seq := range pageSeqs(t, route.ID, items) {
			seqs[seq]++
		}
		if next == "" {
			break
		}
		cursor = next
		if pages > seeded/limit+4 {
			t.Fatalf("%s: paging did not terminate after %d pages", route.ID, pages)
		}
	}

	wantPages := seeded / limit
	if seeded%limit != 0 {
		wantPages++
	}
	if pages != wantPages {
		t.Fatalf("%s: walked %d pages for %d items at limit %d, want %d", route.ID, pages, seeded, limit, wantPages)
	}
	if len(seen) != seeded {
		t.Fatalf("%s: paging covered %d distinct elements, want %d", route.ID, len(seen), seeded)
	}
	for i := 1; i <= seeded; i++ {
		if seqs[i] != 1 {
			t.Fatalf("%s: seq %d appeared %d times across pages, want exactly once", route.ID, i, seqs[i])
		}
	}
	if len(seqs) != seeded {
		t.Fatalf("%s: paging returned %d distinct seq values, want %d", route.ID, len(seqs), seeded)
	}

	unbounded, _, _ := decodePage(call(ctx, t, route, sess, nil, query("", 0), nil))
	if len(unbounded) != plugin.DefaultPageLimit {
		t.Fatalf("%s: request without a limit returned %d items, want the default page of %d (seeded %d)", route.ID, len(unbounded), plugin.DefaultPageLimit, seeded)
	}
}

func seedCypher(ctx context.Context, t *testing.T, s *Session, db, query string) {
	t.Helper()
	if _, err := executeCypher(ctx, s, db, sqldb.QueryRequest{Query: query, Confirm: true}); err != nil {
		t.Fatalf("seed %q: %v", query, err)
	}
}

func decodePage(page any) ([]map[string]any, string, map[string]any) {
	data, _ := json.Marshal(page)
	var decoded struct {
		Items      []map[string]any `json:"items"`
		NextCursor string           `json:"nextCursor"`
	}
	_ = json.Unmarshal(data, &decoded)
	raw := map[string]any{}
	_ = json.Unmarshal(data, &raw)
	return decoded.Items, decoded.NextCursor, raw
}

func pageElementIDs(t *testing.T, routeID string, items []map[string]any) []string {
	t.Helper()
	out := make([]string, 0, len(items))
	for _, item := range items {
		id, _ := item["element_id"].(string)
		if id == "" {
			t.Fatalf("%s: item without element_id: %#v", routeID, item)
		}
		out = append(out, id)
	}
	return out
}

func pageSeqs(t *testing.T, routeID string, items []map[string]any) []int {
	t.Helper()
	out := make([]int, 0, len(items))
	for _, item := range items {
		value, ok := asMap(item["properties"])["seq"].(float64)
		if !ok {
			t.Fatalf("%s: item without numeric seq property: %#v", routeID, item)
		}
		out = append(out, int(value))
	}
	return out
}
