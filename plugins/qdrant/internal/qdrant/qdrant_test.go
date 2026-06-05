package qdrant

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestManifestValidates(t *testing.T) {
	p := New()
	plugintest.ValidatePlugin(t, p)
	if !plugintest.CredentialKindSupported(p.Manifest().Config, plugin.CredentialAPIToken) {
		t.Fatal("Qdrant should support stored API token credentials")
	}
}

func TestCollectionsList(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/collections":
			_, _ = w.Write([]byte(`{"status":"ok","result":{"collections":[{"name":"docs"}]}}`))
		case "/collections/docs":
			_, _ = w.Write([]byte(`{"status":"ok","result":{"status":"green","points_count":3}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	p := New()
	sess, err := p.Connect(context.Background(), plugin.ConnectConfig{Config: map[string]any{"endpoint": srv.URL}, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	routes := plugintest.RouteMap(p.Routes())
	out, err := routes[rid("collections.list")].Handle(plugin.NewRequestContext(context.Background(), plugin.User{}, sess, nil, nil, nil))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	page := out.(plugin.Page[row])
	if len(page.Items) != 1 || page.Items[0]["name"] != "docs" || page.Items[0]["points_count"] == nil {
		t.Fatalf("unexpected page: %#v", page.Items)
	}
}
