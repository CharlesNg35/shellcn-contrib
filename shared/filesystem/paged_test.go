package filesystem

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"testing"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

// pagedFS is a cursor-native Client: it fails the test if the browser ever falls
// back to draining the whole directory through ReadDir.
type pagedFS struct {
	t      *testing.T
	cursor string
	limit  int
}

func (p *pagedFS) Filesystem() (Client, error) { return p, nil }

func (p *pagedFS) HealthCheck(context.Context) error { return nil }

func (p *pagedFS) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}

func (p *pagedFS) Close() error { return nil }

func (p *pagedFS) Home(context.Context) (string, error) { return "/", nil }

func (p *pagedFS) ReadDir(context.Context, string) ([]os.FileInfo, error) {
	p.t.Fatal("ReadDir called on a PagedReader backend")
	return nil, nil
}

func (p *pagedFS) ReadDirPage(_ context.Context, _, cursor string, limit int) ([]os.FileInfo, string, bool, error) {
	p.cursor, p.limit = cursor, limit
	infos := make([]os.FileInfo, 0, limit)
	for i := range limit {
		infos = append(infos, memInfo{name: fmt.Sprintf("obj-%03d", i)})
	}
	infos = append(infos, memInfo{name: "sub", dir: true})
	return infos, "next-token", true, nil
}

func (p *pagedFS) Stat(context.Context, string) (os.FileInfo, error) { return nil, os.ErrNotExist }

func (p *pagedFS) Open(context.Context, string) (io.ReadCloser, error) { return nil, os.ErrNotExist }

func (p *pagedFS) Write(context.Context, string, io.Reader) error { return os.ErrPermission }

func (p *pagedFS) Mkdir(context.Context, string) error { return os.ErrPermission }

func (p *pagedFS) Rename(context.Context, string, string) error { return os.ErrPermission }

func (p *pagedFS) Remove(context.Context, string, bool) error { return os.ErrPermission }

func TestListUsesPagedReaderAndBoundsTheRequest(t *testing.T) {
	fs := &pagedFS{t: t}
	rc := plugin.NewRequestContext(t.Context(), plugin.User{}, fs, map[string]string{"path": "/"},
		url.Values{"limit": {"3"}, "cursor": {"tok"}}, nil)

	out, err := list(rc)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	page, ok := out.(FilePage)
	if !ok {
		t.Fatalf("list returned %T, want FilePage", out)
	}
	if fs.cursor != "tok" || fs.limit != 3 {
		t.Fatalf("backend got cursor %q limit %d; want tok/3", fs.cursor, fs.limit)
	}
	if len(page.Items) != 4 {
		t.Fatalf("page has %d items, want 4", len(page.Items))
	}
	if !page.Items[0].IsDir || page.Items[0].Name != "sub" {
		t.Fatalf("page is not dirs-first: %#v", page.Items[0])
	}
	if page.NextCursor != "next-token" || !page.Truncated {
		t.Fatalf("page dropped the backend cursor: %#v", page)
	}
	if page.Total != nil {
		t.Fatalf("paged listings must not report a total, got %d", *page.Total)
	}
}

func TestListClampsPagedLimitToMaxPageLimit(t *testing.T) {
	fs := &pagedFS{t: t}
	rc := plugin.NewRequestContext(t.Context(), plugin.User{}, fs, map[string]string{"path": "/"},
		url.Values{"limit": {"100000"}}, nil)
	if _, err := list(rc); err != nil {
		t.Fatalf("list: %v", err)
	}
	if fs.limit != plugin.MaxPageLimit {
		t.Fatalf("limit = %d, want %d", fs.limit, plugin.MaxPageLimit)
	}
}
