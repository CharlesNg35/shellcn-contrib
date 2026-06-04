package filesystem

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"os"
	"path"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

// memFS is an in-memory filesystem.Client used to exercise the shared file
// browser handlers end to end without a live remote server.
type memFS struct {
	files map[string][]byte
	dirs  map[string]bool
}

func newMemFS() *memFS {
	return &memFS{files: map[string][]byte{}, dirs: map[string]bool{"/": true}}
}

func (m *memFS) Filesystem() (Client, error) { return m, nil }

func (m *memFS) HealthCheck(context.Context) error { return nil }
func (m *memFS) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}
func (m *memFS) Close() error { return nil }

func (m *memFS) Home(context.Context) (string, error) { return "/", nil }

func (m *memFS) ReadDir(_ context.Context, p string) ([]os.FileInfo, error) {
	if !m.dirs[p] {
		return nil, os.ErrNotExist
	}
	seen := map[string]os.FileInfo{}
	for f := range m.files {
		if parent := path.Dir(f); parent == p {
			seen[path.Base(f)] = memInfo{name: path.Base(f), size: int64(len(m.files[f]))}
		}
	}
	for d := range m.dirs {
		if d == "/" {
			continue
		}
		if parent := path.Dir(d); parent == p {
			seen[path.Base(d)] = memInfo{name: path.Base(d), dir: true}
		}
	}
	out := make([]os.FileInfo, 0, len(seen))
	for _, info := range seen {
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name() < out[j].Name() })
	return out, nil
}

func (m *memFS) Stat(_ context.Context, p string) (os.FileInfo, error) {
	if m.dirs[p] {
		return memInfo{name: path.Base(p), dir: true}, nil
	}
	if data, ok := m.files[p]; ok {
		return memInfo{name: path.Base(p), size: int64(len(data))}, nil
	}
	return nil, os.ErrNotExist
}

func (m *memFS) Open(_ context.Context, p string) (io.ReadCloser, error) {
	data, ok := m.files[p]
	if !ok {
		return nil, os.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *memFS) Write(_ context.Context, p string, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.files[p] = data
	return nil
}

func (m *memFS) Mkdir(_ context.Context, p string) error {
	m.dirs[p] = true
	return nil
}

func (m *memFS) Rename(_ context.Context, from, to string) error {
	if data, ok := m.files[from]; ok {
		m.files[to] = data
		delete(m.files, from)
		return nil
	}
	if m.dirs[from] {
		m.dirs[to] = true
		delete(m.dirs, from)
		return nil
	}
	return os.ErrNotExist
}

func (m *memFS) Remove(_ context.Context, p string, isDir bool) error {
	if isDir {
		delete(m.dirs, p)
		for f := range m.files {
			if strings.HasPrefix(f, p+"/") {
				delete(m.files, f)
			}
		}
		return nil
	}
	if _, ok := m.files[p]; !ok {
		return os.ErrNotExist
	}
	delete(m.files, p)
	return nil
}

type memInfo struct {
	name string
	size int64
	dir  bool
}

func (i memInfo) Name() string { return i.name }
func (i memInfo) Size() int64  { return i.size }
func (i memInfo) Mode() os.FileMode {
	if i.dir {
		return os.ModeDir | 0o755
	}
	return 0o644
}
func (i memInfo) ModTime() time.Time { return time.Unix(0, 0) }
func (i memInfo) IsDir() bool        { return i.dir }
func (i memInfo) Sys() any           { return nil }

func TestFilesystemHandlersRoundTrip(t *testing.T) {
	fs := newMemFS()
	routes := map[string]plugin.Route{}
	for _, r := range Routes("test", "test") {
		routes[r.ID] = r
	}

	run := func(id string, params map[string]string, body []byte) any {
		t.Helper()
		out, err := routes[id].Handle(plugin.NewRequestContext(context.Background(), plugin.User{}, fs, params, nil, body))
		if err != nil {
			t.Fatalf("%s: %v", id, err)
		}
		return out
	}

	run("test.files.mkdir", map[string]string{"path": "/"}, mustJSON(t, map[string]any{"name": "docs"}))
	if !fs.dirs["/docs"] {
		t.Fatal("mkdir did not create /docs")
	}

	run("test.files.write", map[string]string{"path": "/docs/readme.txt"}, mustJSON(t, map[string]any{"content": "hello world"}))
	content := run("test.files.read", map[string]string{"path": "/docs/readme.txt"}, nil).(FileContent)
	if content.Content != "hello world" {
		t.Fatalf("read returned %q", content.Content)
	}

	uploadRC := plugin.NewMultipartRequestContext(context.Background(), plugin.User{}, fs,
		map[string]string{"path": "/docs"}, nil, nil, map[string][]plugin.UploadedFile{"files": {makeUpload(t, "data.bin", []byte("binary"))}})
	if _, err := routes["test.files.upload"].Handle(uploadRC); err != nil {
		t.Fatalf("upload: %v", err)
	}

	names := listNames(t, run("test.files.list", map[string]string{"path": "/docs"}, nil))
	if !contains(names, "readme.txt") || !contains(names, "data.bin") {
		t.Fatalf("expected uploaded + written files, got %v", names)
	}

	run("test.files.rename", map[string]string{"path": "/docs/readme.txt"}, mustJSON(t, map[string]any{"name": "renamed.txt"}))
	if _, ok := fs.files["/docs/renamed.txt"]; !ok {
		t.Fatal("rename did not move the file")
	}

	run("test.files.delete", map[string]string{"path": "/docs/renamed.txt"}, nil)
	if _, ok := fs.files["/docs/renamed.txt"]; ok {
		t.Fatal("delete did not remove the file")
	}

	run("test.files.delete", map[string]string{"path": "/docs"}, nil)
	if fs.dirs["/docs"] {
		t.Fatal("delete did not remove the directory")
	}
}

func makeUpload(t *testing.T, name string, content []byte) plugin.UploadedFile {
	t.Helper()
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	part, err := w.CreateFormFile("files", name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	form, err := multipart.NewReader(&buf, w.Boundary()).ReadForm(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	headers := form.File["files"]
	if len(headers) != 1 {
		t.Fatalf("expected one parsed file, got %d", len(headers))
	}
	return plugin.NewUploadedFile("files", headers[0])
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func listNames(t *testing.T, page any) []string {
	t.Helper()
	fp, ok := page.(FilePage)
	if !ok {
		t.Fatalf("list did not return FilePage, got %T", page)
	}
	names := make([]string, 0, len(fp.Items))
	for _, item := range fp.Items {
		names = append(names, item.Name)
	}
	return names
}

func contains(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}
