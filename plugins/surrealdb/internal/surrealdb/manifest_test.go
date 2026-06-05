package surrealdb

import (
	"testing"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestManifestValidates(t *testing.T) {
	p := &Plugin{}
	plugintest.ValidatePlugin(t, p)
}

func TestParseOptions(t *testing.T) {
	cfg := plugin.ConnectConfig{Config: map[string]any{
		"host":      "db.internal",
		"port":      float64(9000), // JSON numbers decode to float64
		"tls":       true,
		"namespace": "ns",
		"database":  "app",
		"username":  "root",
		"password":  "secret",
	}}
	o, err := parseOptions(cfg)
	if err != nil {
		t.Fatalf("parseOptions: %v", err)
	}
	if o.scheme != "https" || o.addr() != "db.internal:9000" {
		t.Fatalf("unexpected target: %s %s", o.scheme, o.addr())
	}
	if o.namespace != "ns" || o.database != "app" {
		t.Fatalf("unexpected ns/db: %s/%s", o.namespace, o.database)
	}
}

func TestParseOptionsRequiresNamespaceAndDatabase(t *testing.T) {
	_, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{"host": "h"}})
	if err == nil {
		t.Fatal("expected error when namespace/database missing")
	}
}

func TestSplitRecordID(t *testing.T) {
	tb, key, ok := splitRecordID("person:alice")
	if !ok || tb != "person" || key != "alice" {
		t.Fatalf("split failed: %q %q %v", tb, key, ok)
	}
	if _, _, ok := splitRecordID("noseparator"); ok {
		t.Fatal("expected failure without separator")
	}
}
