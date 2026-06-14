package meilisearch

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

const integrationKey = "shellcn_meilisearch_master_key"

func TestMeilisearchPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_MEILISEARCH_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_MEILISEARCH_INTEGRATION=1 to run against Meilisearch")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cfg := meilisearchIntegrationConfig(ctx, t)
	p := New()
	sess, err := p.Connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()

	routes := plugintest.RouteMap(p.Routes())
	index := "shellcn_it_" + time.Now().UTC().Format("20060102150405")
	createBody, _ := json.Marshal(map[string]any{"uid": index, "primaryKey": "id"})
	created := call(ctx, t, routes["meilisearch.index.create"], sess, nil, nil, createBody)
	waitTask(ctx, t, routes, sess, field(created, "taskUid"))
	defer callNoFail(context.Background(), routes["meilisearch.index.delete"], sess, map[string]string{"index": index})

	settingsBody, _ := json.Marshal(map[string]any{"settings": map[string]any{"filterableAttributes": []any{"age"}, "sortableAttributes": []any{"age"}}})
	waitTask(ctx, t, routes, sess, field(call(ctx, t, routes["meilisearch.settings.update"], sess, map[string]string{"index": index}, nil, settingsBody), "taskUid"))
	updateBody, _ := json.Marshal(map[string]any{"primaryKey": "id"})
	waitTask(ctx, t, routes, sess, field(call(ctx, t, routes["meilisearch.index.update"], sess, map[string]string{"index": index}, nil, updateBody), "taskUid"))

	docBody, _ := json.Marshal(map[string]any{"document": map[string]any{"id": "ada", "name": "Ada Lovelace", "age": 37}})
	waitTask(ctx, t, routes, sess, field(call(ctx, t, routes["meilisearch.document.upsert"], sess, map[string]string{"index": index}, nil, docBody), "taskUid"))

	call(ctx, t, routes["meilisearch.overview"], sess, nil, nil, nil)
	call(ctx, t, routes["meilisearch.indexes.tree"], sess, nil, nil, nil)
	call(ctx, t, routes["meilisearch.index.overview"], sess, map[string]string{"index": index}, nil, nil)
	call(ctx, t, routes["meilisearch.index.stats"], sess, map[string]string{"index": index}, nil, nil)
	call(ctx, t, routes["meilisearch.settings.read"], sess, map[string]string{"index": index}, nil, nil)
	docs := call(ctx, t, routes["meilisearch.documents.list"], sess, map[string]string{"index": index}, url.Values{"limit": []string{"10"}}, nil)
	if items := pageItems(docs); len(items) != 1 || items[0]["_id"] != "ada" {
		t.Fatalf("expected one document, got %#v", items)
	}
	read := call(ctx, t, routes["meilisearch.document.read"], sess, map[string]string{"index": index, "id": "ada"}, nil, nil)
	if field(read, "name") != "Ada Lovelace" {
		t.Fatalf("unexpected document: %#v", read)
	}
	call(ctx, t, routes["meilisearch.tasks.list"], sess, nil, url.Values{"limit": []string{"10"}}, nil)
	call(ctx, t, routes["meilisearch.tasks.tree"], sess, nil, url.Values{"limit": []string{"10"}}, nil)
	call(ctx, t, routes["meilisearch.keys.list"], sess, nil, url.Values{"limit": []string{"10"}}, nil)
	call(ctx, t, routes["meilisearch.keys.tree"], sess, nil, url.Values{"limit": []string{"10"}}, nil)
	call(ctx, t, routes["meilisearch.completion"], sess, nil, nil, nil)
	if result, err := executeSearch(ctx, sess.(*Session), index, map[string]any{"q": "Ada", "limit": 10}); err != nil || result.RowCount < 1 {
		t.Fatalf("execute search: result=%#v err=%v", result, err)
	}
	waitTask(ctx, t, routes, sess, field(call(ctx, t, routes["meilisearch.dump.create"], sess, nil, nil, nil), "taskUid"))
	waitTask(ctx, t, routes, sess, field(call(ctx, t, routes["meilisearch.snapshot.create"], sess, nil, nil, nil), "taskUid"))
	keyBody, _ := json.Marshal(map[string]any{"name": "shellcn-it", "description": "integration", "actions": []any{"search"}, "indexes": []any{index}, "expiresAt": nil})
	key := call(ctx, t, routes["meilisearch.key.create"], sess, nil, nil, keyBody)
	if uid := strings.TrimSpace(toString(field(key, "uid"))); uid != "" {
		call(ctx, t, routes["meilisearch.key.read"], sess, map[string]string{"key": uid}, nil, nil)
		keyUpdateBody, _ := json.Marshal(map[string]any{"name": "shellcn-it-renamed", "description": "updated"})
		updated := call(ctx, t, routes["meilisearch.key.update"], sess, map[string]string{"key": uid}, nil, keyUpdateBody)
		if toString(field(updated, "name")) != "shellcn-it-renamed" || toString(field(updated, "description")) != "updated" {
			t.Fatalf("key update did not persist: %#v", updated)
		}
		readBack := call(ctx, t, routes["meilisearch.key.read"], sess, map[string]string{"key": uid}, nil, nil)
		if toString(field(readBack, "name")) != "shellcn-it-renamed" || toString(field(readBack, "description")) != "updated" {
			t.Fatalf("key update not reflected on read: %#v", readBack)
		}
		call(ctx, t, routes["meilisearch.key.delete"], sess, map[string]string{"key": uid}, nil, nil)
	}

	// Cancel an enqueued/processing task, then delete a finished one by uid.
	cancelDoc, _ := json.Marshal(map[string]any{"document": map[string]any{"id": "cancel-target", "name": "Cancel", "age": 2}})
	enqueued := call(ctx, t, routes["meilisearch.document.upsert"], sess, map[string]string{"index": index}, nil, cancelDoc)
	cancelUID := toString(field(enqueued, "taskUid"))
	cancelRes := call(ctx, t, routes["meilisearch.task.cancel"], sess, map[string]string{"task": cancelUID}, nil, nil)
	waitTask(ctx, t, routes, sess, field(cancelRes, "taskUid"))
	waitTaskFinished(ctx, t, routes, sess, cancelUID)
	delTask := call(ctx, t, routes["meilisearch.task.delete"], sess, map[string]string{"task": cancelUID}, nil, nil)
	delUID := toString(field(delTask, "taskUid"))
	waitTask(ctx, t, routes, sess, delUID)
	gone := call(ctx, t, routes["meilisearch.task.read"], sess, map[string]string{"task": delUID}, nil, nil)
	if details, ok := field(gone, "details").(map[string]any); ok {
		if toString(details["deletedTasks"]) == "0" {
			t.Fatalf("task deletion removed nothing: %#v", gone)
		}
	}
	waitTask(ctx, t, routes, sess, field(call(ctx, t, routes["meilisearch.document.delete"], sess, map[string]string{"index": index, "id": "ada"}, nil, nil), "taskUid"))
	docBody, _ = json.Marshal(map[string]any{"document": map[string]any{"id": "temp", "name": "Temporary", "age": 1}})
	waitTask(ctx, t, routes, sess, field(call(ctx, t, routes["meilisearch.document.upsert"], sess, map[string]string{"index": index}, nil, docBody), "taskUid"))
	waitTask(ctx, t, routes, sess, field(call(ctx, t, routes["meilisearch.documents.delete_all"], sess, map[string]string{"index": index}, nil, nil), "taskUid"))
}

func meilisearchIntegrationConfig(ctx context.Context, t *testing.T) map[string]any {
	t.Helper()
	endpoint := os.Getenv("SHELLCN_MEILISEARCH_ENDPOINT")
	key := os.Getenv("SHELLCN_MEILISEARCH_API_KEY")
	if key == "" {
		key = integrationKey
	}
	if endpoint == "" {
		endpoint = startMeilisearchContainer(ctx, t, key)
	}
	return map[string]any{"endpoint": endpoint, "auth": "api_key", "api_key": key, "tls_mode": "disable", "read_only": false, "page_limit": 100, "timeout": "10s"}
}

func startMeilisearchContainer(ctx context.Context, t *testing.T, key string) string {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_MEILISEARCH_ENDPOINT is not set")
	}
	name := "shellcn-meilisearch-it-" + time.Now().UTC().Format("20060102150405")
	run(ctx, t, "docker", "run", "-d", "--rm", "--name", name,
		"-e", "MEILI_MASTER_KEY="+key,
		"-p", "127.0.0.1::7700",
		"getmeili/meilisearch:latest")
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})
	out := run(ctx, t, "docker", "port", name, "7700/tcp")
	host, port, err := net.SplitHostPort(strings.TrimSpace(out))
	if err != nil {
		t.Fatalf("unexpected docker port output: %q", out)
	}
	endpoint := "http://" + net.JoinHostPort(host, port)
	deadline := time.Now().Add(60 * time.Second)
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/health", nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil && resp.StatusCode < 500 {
			_ = resp.Body.Close()
			return endpoint
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("Meilisearch container did not become ready: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func waitTask(ctx context.Context, t *testing.T, routes map[string]plugin.Route, sess plugin.Session, uid any) {
	t.Helper()
	id := toString(uid)
	deadline := time.Now().Add(20 * time.Second)
	for {
		task := call(ctx, t, routes["meilisearch.task.read"], sess, map[string]string{"task": id}, nil, nil)
		status := toString(field(task, "status"))
		if status == "succeeded" {
			return
		}
		if status == "failed" || status == "canceled" {
			t.Fatalf("task %s ended with %s: %#v", id, status, task)
		}
		if time.Now().After(deadline) {
			t.Fatalf("task %s did not finish: %#v", id, task)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// waitTaskFinished waits until a task reaches any terminal state (it may be
// canceled rather than succeeded), so it can then be deleted from history.
func waitTaskFinished(ctx context.Context, t *testing.T, routes map[string]plugin.Route, sess plugin.Session, uid string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		task := call(ctx, t, routes["meilisearch.task.read"], sess, map[string]string{"task": uid}, nil, nil)
		switch toString(field(task, "status")) {
		case "succeeded", "failed", "canceled":
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("task %s did not finish: %#v", uid, task)
		}
		time.Sleep(250 * time.Millisecond)
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

func callNoFail(ctx context.Context, route plugin.Route, sess plugin.Session, params map[string]string) {
	_, _ = route.Handle(plugin.NewRequestContext(ctx, plugin.User{}, sess, params, nil, nil))
}

func pageItems(page any) []map[string]any {
	data, _ := json.Marshal(page)
	var decoded struct {
		Items []map[string]any `json:"items"`
	}
	_ = json.Unmarshal(data, &decoded)
	return decoded.Items
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

func toString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		return strconv.FormatInt(int64(t), 10)
	case int:
		return strconv.Itoa(t)
	default:
		return strings.TrimSpace(strings.Trim(fmt.Sprint(v), "\""))
	}
}

func field(v any, key string) any {
	switch t := v.(type) {
	case map[string]any:
		return t[key]
	default:
		return nil
	}
}
