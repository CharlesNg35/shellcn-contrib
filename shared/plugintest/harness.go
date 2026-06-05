// Package plugintest contains test helpers shared by ShellCN contrib plugins.
package plugintest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"slices"
	"testing"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type Harness struct {
	t       testing.TB
	routes  map[string]plugin.Route
	covered map[string]bool
}

func NewHarness(t testing.TB, routes []plugin.Route) *Harness {
	t.Helper()
	byID := make(map[string]plugin.Route, len(routes))
	for _, route := range routes {
		byID[route.ID] = route
	}
	return &Harness{t: t, routes: byID, covered: map[string]bool{}}
}

func (h *Harness) Call(ctx context.Context, id string, sess plugin.Session, params map[string]string, query url.Values, body []byte) any {
	h.t.Helper()
	route := h.route(id)
	if route.Handle == nil {
		h.t.Fatalf("%s has no HTTP handler", id)
	}
	h.covered[id] = true
	out, err := route.Handle(plugin.NewRequestContext(ctx, plugin.User{}, sess, params, query, body))
	if err != nil {
		h.t.Fatalf("%s: %v", id, err)
	}
	return out
}

func (h *Harness) CallNoFail(ctx context.Context, id string, sess plugin.Session, params map[string]string) {
	h.t.Helper()
	route, ok := h.routes[id]
	if !ok || route.Handle == nil {
		return
	}
	h.covered[id] = true
	_, _ = route.Handle(plugin.NewRequestContext(ctx, plugin.User{}, sess, params, nil, nil))
}

func (h *Harness) Stream(ctx context.Context, id string, sess plugin.Session, params map[string]string, query url.Values, input []byte) []byte {
	h.t.Helper()
	route := h.route(id)
	if route.Stream == nil {
		h.t.Fatalf("%s has no stream handler", id)
	}
	h.covered[id] = true
	stream := &memoryStream{ctx: ctx, reader: bytes.NewReader(input)}
	if err := route.Stream(plugin.NewRequestContext(ctx, plugin.User{}, sess, params, query, nil), stream); err != nil {
		h.t.Fatalf("%s: %v", id, err)
	}
	return stream.output.Bytes()
}

func (h *Harness) AssertAllCovered() {
	h.t.Helper()
	missing := make([]string, 0)
	for id := range h.routes {
		if !h.covered[id] {
			missing = append(missing, id)
		}
	}
	slices.Sort(missing)
	if len(missing) > 0 {
		h.t.Fatalf("routes not covered: %v", missing)
	}
}

func (h *Harness) AssertCovered(ids ...string) {
	h.t.Helper()
	missing := make([]string, 0)
	for _, id := range ids {
		if !h.covered[id] {
			missing = append(missing, id)
		}
	}
	slices.Sort(missing)
	if len(missing) > 0 {
		h.t.Fatalf("routes not covered: %v", missing)
	}
}

func (h *Harness) Route(id string) plugin.Route {
	h.covered[id] = true
	return h.route(id)
}

func (h *Harness) route(id string) plugin.Route {
	h.t.Helper()
	route, ok := h.routes[id]
	if !ok {
		h.t.Fatalf("route %q not found", id)
	}
	return route
}

type memoryStream struct {
	ctx    context.Context
	reader *bytes.Reader
	output bytes.Buffer
}

func (s *memoryStream) Read(p []byte) (int, error) {
	return s.reader.Read(p)
}

func (s *memoryStream) Write(p []byte) (int, error) {
	return s.output.Write(p)
}

func (s *memoryStream) Close() error {
	_, err := io.Copy(io.Discard, s.reader)
	if err != nil {
		return fmt.Errorf("close stream: %w", err)
	}
	return nil
}

func (s *memoryStream) Context() context.Context {
	return s.ctx
}
