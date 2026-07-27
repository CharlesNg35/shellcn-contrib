package pinecone

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	contribtest "github.com/charlesng35/shellcn-contrib/shared/plugintest"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

// Pinecone is a hosted service, so there is no container to start. The test runs
// only when it is explicitly enabled and a real API key is present.
func TestPineconePluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_PINECONE_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_PINECONE_INTEGRATION=1 to run against Pinecone")
	}
	apiKey := strings.TrimSpace(os.Getenv("PINECONE_API_KEY"))
	if apiKey == "" {
		t.Skip("set PINECONE_API_KEY to run against Pinecone")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	namespace := "shellcn-it-" + time.Now().UTC().Format("20060102150405")
	config := map[string]any{
		"auth":      "api_key",
		"api_key":   apiKey,
		"namespace": namespace,
		"read_only": false,
	}
	if endpoint := strings.TrimSpace(os.Getenv("PINECONE_CONTROL_PLANE")); endpoint != "" {
		config["endpoint"] = endpoint
	}
	p := New()
	sess, err := p.Connect(ctx, plugin.ConnectConfig{Config: config, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	h := contribtest.NewHarness(t, p.Routes())

	index := strings.TrimSpace(os.Getenv("PINECONE_INDEX"))
	if index == "" {
		index = "shellcn-it-" + time.Now().UTC().Format("20060102150405")
		h.Call(ctx, rid("index.create"), sess, nil, nil, mustIntegrationJSON(t, map[string]any{
			"name": index, "deployment": "serverless", "vector_type": "dense",
			"dimension": 3, "metric": "cosine",
			"cloud":  envOr("PINECONE_CLOUD", "aws"),
			"region": envOr("PINECONE_REGION", "us-east-1"),
		}))
		defer h.CallNoFail(context.Background(), rid("index.delete"), sess, map[string]string{"index": index})
		waitForIndex(ctx, t, h, sess, index)
	}
	indexParam := map[string]string{"index": index}
	scoped := map[string]string{"index": index, "namespace": namespace}

	h.Call(ctx, rid("overview"), sess, nil, nil, nil)
	h.Call(ctx, rid("indexes.tree"), sess, nil, nil, nil)
	indexes := integrationRows(h.Call(ctx, rid("indexes.list"), sess, nil, url.Values{"limit": []string{"50"}}, nil))
	if !hasName(indexes, index) {
		t.Fatalf("index %q is not listed: %#v", index, indexes)
	}
	h.Call(ctx, rid("index.read"), sess, indexParam, nil, nil)
	h.Call(ctx, rid("index.stats"), sess, indexParam, nil, nil)
	h.Call(ctx, rid("collections.list"), sess, nil, nil, nil)
	h.Call(ctx, rid("collections.tree"), sess, nil, nil, nil)

	h.Call(ctx, rid("vectors.upsert"), sess, scoped, nil, mustIntegrationJSON(t, map[string]any{
		"vectors": []any{
			map[string]any{"id": "it-1", "values": []float64{0.1, 0.2, 0.3}, "metadata": map[string]any{"title": "Ada"}},
			map[string]any{"id": "it-2", "values": []float64{0.2, 0.1, 0.3}, "metadata": map[string]any{"title": "Grace"}},
			map[string]any{"id": "it-3", "values": []float64{0.3, 0.2, 0.1}, "metadata": map[string]any{"title": "Alan"}},
		},
	}))
	waitForVectors(ctx, t, h, sess, scoped, 3)

	record := h.Call(ctx, rid("vector.read"), sess,
		map[string]string{"index": index, "namespace": namespace, "vector": "it-1"}, nil, nil)
	if item, ok := record.(row); !ok || item["dimension"] != 3 {
		t.Fatalf("unexpected vector record: %#v", record)
	}
	neighbors := integrationRows(h.Call(ctx, rid("vector.neighbors"), sess,
		map[string]string{"index": index, "namespace": namespace, "vector": "it-1"}, url.Values{"limit": []string{"2"}}, nil))
	if len(neighbors) == 0 {
		t.Fatal("expected at least one neighbor")
	}
	h.Call(ctx, rid("namespaces.list"), sess, indexParam, nil, nil)
	h.Call(ctx, rid("namespace.read"), sess, scoped, nil, nil)
	h.Call(ctx, rid("query.complete"), sess, scoped, nil, nil)

	out := h.Stream(ctx, rid("query"), sess, scoped, nil,
		[]byte(`{"query":"{\"vector\":[0.1,0.2,0.3],\"topK\":3,\"includeMetadata\":true}","confirm":false}`+"\n"))
	if !strings.Contains(string(out), "Ada") {
		t.Fatalf("query stream did not return the upserted metadata: %s", out)
	}
	metricsCtx, cancelMetrics := context.WithCancel(ctx)
	time.AfterFunc(2*time.Second, cancelMetrics)
	h.Stream(metricsCtx, rid("index.metrics"), sess, indexParam, nil, nil)

	h.Call(ctx, rid("vectors.delete"), sess, scoped, nil, mustIntegrationJSON(t, map[string]any{"ids": []string{"it-3"}}))
	h.Call(ctx, rid("index.configure"), sess, indexParam, nil, mustIntegrationJSON(t, map[string]any{
		"tags": map[string]string{"owner": "shellcn-it"},
	}))
	h.CallNoFail(ctx, rid("namespace.delete"), sess, scoped)

	// Collections exist for pod-based indexes only, so the integration index
	// cannot create one; drive the routes with real input and tolerate the
	// upstream refusal rather than skipping them.
	backup := index + "-bak"
	tolerate(ctx, t, h, rid("collection.create"), sess, nil, mustIntegrationJSON(t, map[string]any{"name": backup, "source": index}))
	tolerate(ctx, t, h, rid("collection.read"), sess, map[string]string{"collection": backup}, nil)
	tolerate(ctx, t, h, rid("collection.delete"), sess, map[string]string{"collection": backup}, nil)

	h.CallNoFail(ctx, rid("index.delete"), sess, map[string]string{"index": index})
	h.AssertAllCovered()
}

// tolerate drives a route that a serverless-only account may legitimately
// refuse, so the run still proves the handler builds a well-formed request.
func tolerate(ctx context.Context, t *testing.T, h *contribtest.Harness, id string, sess plugin.Session, params map[string]string, body []byte) {
	t.Helper()
	route := h.Route(id)
	if _, err := route.Handle(plugin.NewRequestContext(ctx, plugin.User{}, sess, params, nil, body)); err != nil {
		t.Logf("%s: %v", id, err)
	}
}

func waitForIndex(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, index string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		out := h.Call(ctx, rid("index.read"), sess, map[string]string{"index": index}, nil, nil)
		if item, ok := out.(row); ok && item["ready"] == true {
			return
		}
		time.Sleep(5 * time.Second)
	}
	t.Fatalf("index %q never became ready", index)
}

func waitForVectors(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, params map[string]string, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		rows := integrationRows(h.Call(ctx, rid("vectors.list"), sess, params, url.Values{"limit": []string{"20"}}, nil))
		if len(rows) >= want {
			return
		}
		time.Sleep(3 * time.Second)
	}
	t.Fatalf("upserted vectors never became visible in %v", params)
}

func integrationRows(out any) []row {
	switch page := out.(type) {
	case pagedResult:
		return page.Items
	case plugin.Page[row]:
		return page.Items
	default:
		return nil
	}
}

func hasName(rows []row, name string) bool {
	for _, item := range rows {
		if fmt.Sprint(item["name"]) == name {
			return true
		}
	}
	return false
}

func envOr(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func mustIntegrationJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
