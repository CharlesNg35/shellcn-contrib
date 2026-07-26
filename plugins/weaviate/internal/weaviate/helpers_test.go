package weaviate

import (
	"bytes"
	"context"
	"encoding/json"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

// memoryStream is a plugin.ClientStream backed by in-memory buffers so stream
// handlers can be driven without a WebSocket.
type memoryStream struct {
	ctx    context.Context
	reader *bytes.Reader
	output bytes.Buffer
}

func newMemoryStream(input string) *memoryStream {
	return &memoryStream{ctx: context.Background(), reader: bytes.NewReader([]byte(input))}
}

func (s *memoryStream) Read(p []byte) (int, error)  { return s.reader.Read(p) }
func (s *memoryStream) Write(p []byte) (int, error) { return s.output.Write(p) }
func (s *memoryStream) Close() error                { return nil }
func (s *memoryStream) Context() context.Context    { return s.ctx }

func (s *memoryStream) frames(t testing.TB) []map[string]any {
	t.Helper()
	out := make([]map[string]any, 0, 4)
	decoder := json.NewDecoder(strings.NewReader(s.output.String()))
	for decoder.More() {
		var frame map[string]any
		if err := decoder.Decode(&frame); err != nil {
			t.Fatalf("decode stream frame: %v", err)
		}
		out = append(out, frame)
	}
	return out
}

// fakeStorage mirrors the scoping rules of the gateway store closely enough to
// exercise saved-query and activity handlers.
type fakeStorage struct {
	mu    sync.Mutex
	items map[string]plugin.StorageItem
}

func (s *fakeStorage) key(collection, key string) string { return collection + "\x00" + key }

func (s *fakeStorage) Get(_ context.Context, scope plugin.StorageScope, key string) (plugin.StorageItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.items[s.key(scope.Collection, key)]
	if !ok {
		return plugin.StorageItem{}, plugin.ErrNotFound
	}
	return item, nil
}

func (s *fakeStorage) Put(_ context.Context, collection string, item plugin.StorageItem) (plugin.StorageItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.items == nil {
		s.items = map[string]plugin.StorageItem{}
	}
	now := time.Now().UTC()
	if existing, ok := s.items[s.key(collection, item.Key)]; ok {
		item.CreatedAt = existing.CreatedAt
	} else {
		item.CreatedAt = now
	}
	item.UpdatedAt = now
	s.items[s.key(collection, item.Key)] = item
	return item, nil
}

func (s *fakeStorage) Delete(_ context.Context, scope plugin.StorageScope, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	full := s.key(scope.Collection, key)
	if _, ok := s.items[full]; !ok {
		return plugin.ErrNotFound
	}
	delete(s.items, full)
	return nil
}

func (s *fakeStorage) List(_ context.Context, scope plugin.StorageScope, filter *plugin.StorageListFilter) ([]plugin.StorageItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if filter == nil {
		filter = &plugin.StorageListFilter{}
	}
	prefix := scope.Collection + "\x00"
	out := make([]plugin.StorageItem, 0, len(s.items))
	for full, item := range s.items {
		if !strings.HasPrefix(full, prefix) {
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
	if filter.Offset > 0 {
		if filter.Offset >= len(out) {
			return nil, nil
		}
		out = out[filter.Offset:]
	}
	if filter.Limit > 0 && filter.Limit < len(out) {
		out = out[:filter.Limit]
	}
	return out, nil
}
