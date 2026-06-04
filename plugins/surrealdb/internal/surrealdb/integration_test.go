package surrealdb

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"testing"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

// TestIntegrationDirect drives the real handlers against a live SurrealDB
// (SURREAL_IT=1; expects root/root on 127.0.0.1:8000) through the direct
// transport — the same path the gateway uses minus the gRPC hop.
func TestIntegrationDirect(t *testing.T) {
	if os.Getenv("SURREAL_IT") == "" {
		t.Skip("set SURREAL_IT=1 with a local SurrealDB on :8000")
	}
	ctx := context.Background()
	p := &Plugin{}
	cfg := plugin.ConnectConfig{
		ConnectionID: "it",
		Transport:    plugin.TransportDirect,
		Net:          plugintest.DirectTransport(),
		Config: map[string]any{
			"host": "127.0.0.1", "port": float64(8000),
			"namespace": "test", "database": "test",
			"username": "root", "password": "root",
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
