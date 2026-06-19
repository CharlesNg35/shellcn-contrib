package escompat

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

func TestEditorContentWrapsIndentedJSON(t *testing.T) {
	out, err := editorContent(map[string]any{"_id": "1", "_source": map[string]any{"n": 2}})
	if err != nil {
		t.Fatalf("editorContent: %v", err)
	}
	content, ok := out.(map[string]any)["content"].(string)
	if !ok {
		t.Fatalf("expected string content, got %#v", out)
	}
	if !strings.Contains(content, "\n  ") {
		t.Fatalf("content not indented: %q", content)
	}
	var back map[string]any
	if err := json.Unmarshal([]byte(content), &back); err != nil {
		t.Fatalf("content not valid JSON: %v", err)
	}
}

func TestDocumentEditorDeclaresRefreshField(t *testing.T) {
	m := New(Provider{Protocol: "elasticsearch", Product: ProductElasticsearch}).Manifest()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if !strings.Contains(string(b), `"refreshField":"content"`) {
		t.Fatal("document editor manifest missing refreshField=content")
	}
}

// wrappedSession mimics the core's borrowed session.Handle: a plugin.Session
// that exposes the live session via Session().
type wrappedSession struct{ inner plugin.Session }

func (w wrappedSession) Session() plugin.Session           { return w.inner }
func (w wrappedSession) HealthCheck(context.Context) error { return nil }
func (w wrappedSession) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}
func (w wrappedSession) Close() error { return nil }

func TestValidateIndex(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want string
		ok   bool
	}{
		{name: "plain", in: "orders", want: "orders", ok: true},
		{name: "trims", in: "  orders  ", want: "orders", ok: true},
		{name: "empty", in: "", ok: false},
		{name: "blank", in: "   ", ok: false},
		{name: "wildcard", in: "orders-*", ok: false},
		{name: "comma", in: "orders,users", ok: false},
		{name: "all", in: "_all", ok: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := validateIndex(tc.in)
			if tc.ok {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if got != tc.want {
					t.Fatalf("got %q want %q", got, tc.want)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error for %q", tc.in)
			}
			if !errors.Is(err, plugin.ErrInvalidInput) {
				t.Fatalf("error %v is not ErrInvalidInput", err)
			}
		})
	}
}

func TestUnwrapResolvesThroughHandleWrapper(t *testing.T) {
	inner := &Session{}
	if got, err := unwrap(inner); err != nil || got != inner {
		t.Fatalf("bare session: got %v, err %v", got, err)
	}
	if got, err := unwrap(wrappedSession{inner: inner}); err != nil || got != inner {
		t.Fatalf("wrapped session must resolve to the inner session: got %v, err %v", got, err)
	}
}
