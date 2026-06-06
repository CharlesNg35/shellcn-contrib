package surrealdb

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"testing"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

// TestIntegrationDirect drives the real handlers against a live SurrealDB
// (SURREAL_IT=1; expects root/root on 127.0.0.1:8000) through the direct
// transport, the same path the gateway uses minus the gRPC hop.
func TestIntegrationDirect(t *testing.T) {
	if os.Getenv("SURREAL_IT") == "" {
		t.Skip("set SURREAL_IT=1 with a local SurrealDB on :8000")
	}
	ctx := context.Background()
	p := &Plugin{}
	ensureSurrealTestDatabase(ctx, t)
	cfg := plugin.ConnectConfig{
		ConnectionID: "it",
		Transport:    plugin.TransportDirect,
		Net:          plugintest.DirectTransport(),
		Config: map[string]any{
			"host": "127.0.0.1", "port": float64(8000),
			"namespace": "test", "database": "test",
			"username": "root", "password": "root",
			"read_only": false,
		},
	}
	sess, err := p.Connect(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer sess.Close()

	if err := sess.HealthCheck(ctx); err != nil {
		t.Fatalf("health: %v", err)
	}

	rcOf := func(params map[string]string, body any) *plugin.RequestContext {
		var raw []byte
		if body != nil {
			raw, _ = json.Marshal(body)
		}
		return plugin.NewRequestContext(ctx, plugin.User{ID: "u"}, sess, params, url.Values{}, raw)
	}

	if _, err := defineTable(rcOf(nil, map[string]any{"name": "it_people"})); err != nil {
		t.Fatalf("defineTable: %v", err)
	}
	out, err := listTables(rcOf(nil, nil))
	if err != nil {
		t.Fatalf("listTables: %v", err)
	}
	t.Logf("tables: %+v", out)

	created, err := createRecord(rcOf(map[string]string{"table": "it_people"},
		map[string]any{"data": map[string]any{"name": "alice", "age": 30}}))
	if err != nil {
		t.Fatalf("createRecord: %v", err)
	}
	t.Logf("created: %+v", created)

	recs, err := listRecords(rcOf(map[string]string{"table": "it_people"}, nil))
	if err != nil {
		t.Fatalf("listRecords: %v", err)
	}
	t.Logf("records: %+v", recs)
}

func ensureSurrealTestDatabase(ctx context.Context, t *testing.T) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://127.0.0.1:8000/sql", bytes.NewBufferString("DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test;"))
	if err != nil {
		t.Fatalf("create setup request: %v", err)
	}
	req.SetBasicAuth("root", "root")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "text/plain")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("initialize test namespace/database: %v", err)
	}
	defer func() { _ = res.Body.Close() }()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		t.Fatalf("initialize test namespace/database: status=%s body=%s", res.Status, body)
	}
	var out []struct {
		Status string `json:"status"`
		Result any    `json:"result"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode setup response: %v\n%s", err, body)
	}
	for _, r := range out {
		if r.Status == "ERR" {
			t.Fatalf("initialize test namespace/database failed: %s", body)
		}
	}
}
