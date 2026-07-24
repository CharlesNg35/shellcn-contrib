package duckdb

import (
	"context"
	"testing"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestManifestValidates(t *testing.T) {
	plugintest.ValidatePlugin(t, New())
}

func newAssetContext(p string) *plugin.RequestContext {
	return plugin.NewRequestContext(context.Background(), plugin.User{}, nil, map[string]string{"path": p}, nil, nil)
}

func TestAssetRouteRejectsTraversal(t *testing.T) {
	for _, bad := range []string{"../go.mod", "..", "/../secret", "a/../../x"} {
		if _, err := assetRoute(newAssetContext(bad)); err == nil {
			t.Errorf("assetRoute(%q) = nil error; want rejection", bad)
		}
	}
}

func TestAssetRouteServesEngine(t *testing.T) {
	for _, name := range []string{"app.js", "duckdb.mjs", "duckdb-browser-eh.worker.js", "duckdb-eh.wasm"} {
		if _, err := assetRoute(newAssetContext(name)); err != nil {
			t.Errorf("assetRoute(%q) failed: %v", name, err)
		}
	}
}
