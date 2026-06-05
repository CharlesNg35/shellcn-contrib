package jaeger

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
	if !plugintest.CredentialKindSupported(p.Manifest().Config, plugin.CredentialBearerToken) {
		t.Fatal("Jaeger should support stored bearer token credentials")
	}
}

func TestServicesList(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/services":
			_, _ = w.Write([]byte(`{"data":["checkout","payments"]}`))
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
	out, err := plugintest.RouteMap(p.Routes())[rid("services.list")].Handle(plugin.NewRequestContext(context.Background(), plugin.User{}, sess, nil, nil, nil))
	if err != nil {
		t.Fatalf("services: %v", err)
	}
	page := out.(plugin.Page[row])
	if len(page.Items) != 2 || page.Items[0]["name"] != "checkout" {
		t.Fatalf("unexpected services: %#v", page.Items)
	}
}
