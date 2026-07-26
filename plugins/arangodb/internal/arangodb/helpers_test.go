package arangodb

import (
	"bytes"
	"context"
	"encoding/json"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/arangodb/go-driver/v2/arangodb/shared"

	contribtest "github.com/charlesng35/shellcn-contrib/shared/plugintest"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugin/canvas"
)

func arangoError(code int, message string) error {
	return shared.ArangoError{HasError: true, Code: code, ErrorNum: code, ErrorMessage: message}
}

func readyEvent(width, height float64, theme plugin.PanelTheme) *canvas.ReadyEvent {
	return &canvas.ReadyEvent{Type: canvas.EventReady, Width: width, Height: height, DPR: 1, Theme: theme}
}

func keyEvent(key string) *canvas.KeyEvent {
	return &canvas.KeyEvent{Type: canvas.EventKey, Event: "keydown", Key: key}
}

// runStream drives the query stream through the shared contrib harness so the
// test exercises the same route wiring the gateway uses.
func runStream(t *testing.T, sess plugin.Session, params map[string]string, input string) []map[string]any {
	t.Helper()
	h := contribtest.NewHarness(t, New().Routes())
	out := h.Stream(context.Background(), rid("query"), sess, params, nil, []byte(input))
	frames := make([]map[string]any, 0, 2)
	dec := json.NewDecoder(bytes.NewReader(out))
	for dec.More() {
		frame := map[string]any{}
		if err := dec.Decode(&frame); err != nil {
			t.Fatalf("decode stream frame: %v", err)
		}
		frames = append(frames, frame)
	}
	return frames
}

func storageContext(store plugin.Storage, params map[string]string, body []byte) *plugin.RequestContext {
	return storageQueryContext(store, params, nil, body)
}

func storageQueryContext(store plugin.Storage, params map[string]string, query map[string][]string, body []byte) *plugin.RequestContext {
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, nil, params, query, body)
	rc.Storage = store
	return rc
}

func callStorageRoute(t *testing.T, store plugin.Storage, handle func(*plugin.RequestContext) (any, error), params map[string]string, body []byte) any {
	t.Helper()
	out, err := handle(storageContext(store, params, body))
	if err != nil {
		t.Fatalf("storage route: %v", err)
	}
	return out
}

// fakeStorage mirrors the real scoped store: Put writes by collection/key and
// reads are filtered by the requested scope's collection.
type fakeStorage struct {
	items map[string]plugin.StorageItem
}

func (s *fakeStorage) Get(_ context.Context, scope plugin.StorageScope, key string) (plugin.StorageItem, error) {
	item, ok := s.items[scope.Collection+"/"+key]
	if !ok {
		return plugin.StorageItem{}, plugin.ErrNotFound
	}
	return item, nil
}

func (s *fakeStorage) Put(_ context.Context, collection string, item plugin.StorageItem) (plugin.StorageItem, error) {
	if s.items == nil {
		s.items = map[string]plugin.StorageItem{}
	}
	s.items[collection+"/"+item.Key] = item
	return item, nil
}

func (s *fakeStorage) Delete(_ context.Context, scope plugin.StorageScope, key string) error {
	full := scope.Collection + "/" + key
	if _, ok := s.items[full]; !ok {
		return plugin.ErrNotFound
	}
	delete(s.items, full)
	return nil
}

func (s *fakeStorage) List(_ context.Context, scope plugin.StorageScope, filter *plugin.StorageListFilter) ([]plugin.StorageItem, error) {
	if filter == nil {
		filter = &plugin.StorageListFilter{}
	}
	out := make([]plugin.StorageItem, 0, len(s.items))
	for full, item := range s.items {
		if !strings.HasPrefix(full, scope.Collection+"/") {
			continue
		}
		if len(filter.Keys) > 0 && !slices.Contains(filter.Keys, item.Key) {
			continue
		}
		if filter.KeyPrefix != "" && !strings.HasPrefix(item.Key, filter.KeyPrefix) {
			continue
		}
		if filter.ContentType != "" && item.ContentType != filter.ContentType {
			continue
		}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}
