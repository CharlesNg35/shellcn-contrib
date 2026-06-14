package jaeger

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestManifestValidates(t *testing.T) {
	p := New()
	plugintest.ValidatePlugin(t, p)
	if !plugintest.CredentialKindSupported(p.Manifest().Config, plugin.CredentialKindBearerToken) {
		t.Fatal("Jaeger should support stored bearer token credentials")
	}
}

func TestServicesList(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v3/services":
			_, _ = w.Write([]byte(`{"services":["checkout","payments"]}`))
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

func TestTraceRoutesUseAPIV3(t *testing.T) {
	seenTraceQuery := url.Values{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v3/services":
			_, _ = w.Write([]byte(`{"services":["checkout"]}`))
		case "/api/v3/operations":
			if r.URL.Query().Get("service") != "checkout" {
				t.Fatalf("unexpected operations query: %s", r.URL.RawQuery)
			}
			_, _ = w.Write([]byte(`{"operations":[{"name":"GET /checkout","spanKind":"server"}]}`))
		case "/api/v3/traces":
			seenTraceQuery = r.URL.Query()
			_, _ = w.Write([]byte(traceFixture()))
		case "/api/v3/traces/11111111111111111111111111111111":
			_, _ = w.Write([]byte(traceFixture()))
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
	ctx := context.Background()
	operations, err := routes[rid("operations.list")].Handle(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"service": "checkout"}, nil, nil))
	if err != nil {
		t.Fatalf("operations: %v", err)
	}
	if page := operations.(plugin.Page[row]); len(page.Items) != 1 || page.Items[0]["name"] != "GET /checkout" {
		t.Fatalf("unexpected operations: %#v", operations)
	}

	traces, err := routes[rid("traces.list")].Handle(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"service": "checkout"}, url.Values{"limit": []string{"10"}, "lookback": []string{"2h"}}, nil))
	if err != nil {
		t.Fatalf("traces: %v", err)
	}
	if seenTraceQuery.Get("query.service_name") != "checkout" || seenTraceQuery.Get("query.num_traces") != "10" {
		t.Fatalf("unexpected trace query: %s", seenTraceQuery.Encode())
	}
	tracePage := traces.(plugin.Page[row])
	if len(tracePage.Items) != 1 || tracePage.Items[0]["traceID"] != "11111111111111111111111111111111" {
		t.Fatalf("unexpected traces: %#v", tracePage.Items)
	}

	spans, err := routes[rid("spans.list")].Handle(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"trace": "11111111111111111111111111111111"}, nil, nil))
	if err != nil {
		t.Fatalf("spans: %v", err)
	}
	spanPage := spans.(plugin.Page[row])
	if len(spanPage.Items) != 1 || spanPage.Items[0]["serviceName"] != "checkout" {
		t.Fatalf("unexpected spans: %#v", spanPage.Items)
	}
}

func traceFixture() string {
	return `{"result":{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"checkout"}}]},"scopeSpans":[{"spans":[{"traceId":"11111111111111111111111111111111","spanId":"1111111111111111","name":"GET /checkout","startTimeUnixNano":"1700000000000000000","endTimeUnixNano":"1700000000005000000","attributes":[{"key":"shellcn","value":{"stringValue":"true"}}]}]}]}]}}`
}
