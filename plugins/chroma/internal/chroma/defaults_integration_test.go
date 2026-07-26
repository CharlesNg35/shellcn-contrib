package chroma

import (
	"context"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	contribtest "github.com/charlesng35/shellcn-contrib/shared/plugintest"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

// TestDefaultQueryIntegration runs the manifest's pre-filled editor query exactly
// as the frontend would send it, so a default that cannot execute on a freshly
// seeded collection fails here instead of in the user's first tab.
func TestDefaultQueryIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_CHROMA_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_CHROMA_INTEGRATION=1 to run against Chroma")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	suffix := time.Now().UTC().Format("20060102150405.000000")
	suffix = strings.ReplaceAll(suffix, ".", "")
	database := "shellcn_dq_db_" + suffix
	collectionName := "shellcn_dq_col_" + suffix

	p := New()
	sess, err := p.Connect(ctx, plugin.ConnectConfig{Config: chromaConfig(ctx, t), Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	h := contribtest.NewHarness(t, p.Routes())

	h.Call(ctx, rid("database.create"), sess, nil, nil, mustJSON(t, map[string]any{"name": database}))
	defer h.CallNoFail(context.Background(), rid("database.delete"), sess, map[string]string{"database": database})

	created := asMap(t, h.Call(ctx, rid("collection.create"), sess, nil, nil, mustJSON(t, map[string]any{
		"name": collectionName, "space": spaceCosine, "database": database,
	})))
	collectionID, _ := created["id"].(string)
	if collectionID == "" {
		t.Fatalf("collection create returned no id: %#v", created)
	}
	scoped := map[string]string{"tenant": defaultTenant, "database": database, "collection": collectionID}
	defer h.CallNoFail(context.Background(), rid("collection.delete"), sess, scoped)

	h.Call(ctx, rid("records.add"), sess, scoped, nil, mustJSON(t, map[string]any{"records": map[string]any{
		"ids":        []string{"alpha", "beta", "gamma"},
		"embeddings": [][]float64{{1, 0, 0}, {0.92, 0.2, 0}, {0, 1, 0}},
		"documents":  []string{"alpha vector", "beta vector", "gamma vector"},
		"metadatas":  []map[string]any{{"tag": "x"}, {"tag": "x"}, {"tag": "y"}},
		"upsert":     true,
	}}))

	// The editor interpolates ${resource.*} from the opened collection before it
	// sends the first frame; the collection identity supplies those values.
	query := interpolateResource(queryConfig().InitialQuery, map[string]string{
		"name": collectionName, "uid": collectionID,
		"scope": defaultTenant, "namespace": database, "kind": "collection",
	})
	if strings.Contains(query, "${") {
		t.Fatalf("initial query still holds an uninterpolated token: %s", query)
	}

	frames := streamFrames(t, h.Stream(ctx, rid("query"), sess, scoped, nil,
		mustLines(string(mustJSON(t, queryFrame{Query: query})))))
	if len(frames) != 1 {
		t.Fatalf("expected one frame for the default query, got %#v", frames)
	}
	frame := frames[0]
	if frame["error"] != nil {
		t.Fatalf("the default query must not error: %v (%s)", frame["error"], query)
	}
	if frame["confirmRequired"] != nil {
		t.Fatalf("the default query must not need confirmation: %v", frame["confirmMessage"])
	}
	if count, _ := frame["rowCount"].(float64); count < 3 {
		t.Fatalf("the default query should return the seeded records, got %#v", frame)
	}
	for _, want := range []any{"id", "document", "metadata", "dimension"} {
		if !slices.Contains(frame["columns"].([]any), want) {
			t.Fatalf("the default query should surface a %v column: %#v", want, frame["columns"])
		}
	}
	if row := frame["rows"].([]any)[0].([]any); row[len(row)-1] != float64(3) {
		t.Fatalf("expected the seeded dimension on the first row: %#v", row)
	}
}

func interpolateResource(query string, values map[string]string) string {
	for key, value := range values {
		query = strings.ReplaceAll(query, "${resource."+key+"}", value)
	}
	return query
}
