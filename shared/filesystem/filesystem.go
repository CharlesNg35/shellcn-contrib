// Package filesystem contains shared manifest and route helpers for file
// browser plugins backed by different remote filesystem protocols.
package filesystem

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"mime"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

const previewLimit = 1 << 20

type Client interface {
	Home(ctx context.Context) (string, error)
	ReadDir(ctx context.Context, p string) ([]os.FileInfo, error)
	Stat(ctx context.Context, p string) (os.FileInfo, error)
	Open(ctx context.Context, p string) (io.ReadCloser, error)
	Write(ctx context.Context, p string, r io.Reader) error
	Mkdir(ctx context.Context, p string) error
	Rename(ctx context.Context, from, to string) error
	Remove(ctx context.Context, p string, isDir bool) error
}

type ErrorMapper interface {
	MapError(error) error
}

// Seekable is an optional Client capability: a seekable handle enables full HTTP
// Range support for downloads/previews.
type Seekable interface {
	OpenSeeker(ctx context.Context, p string) (io.ReadSeekCloser, error)
}

// RangeOpener is an optional Client capability for backends that read from an
// offset but cannot seek. length <= 0 means to EOF.
type RangeOpener interface {
	OpenRange(ctx context.Context, p string, offset, length int64) (io.ReadCloser, error)
}

type Session interface {
	Filesystem() (Client, error)
}

type FileEntry struct {
	Name    string    `json:"name"`
	Path    string    `json:"path"`
	IsDir   bool      `json:"isDir"`
	Size    int64     `json:"size,omitempty"`
	MIME    string    `json:"mime,omitempty"`
	ModTime time.Time `json:"modTime,omitzero"`
	Mode    string    `json:"mode,omitempty"`
}

type FileContent struct {
	Path      string `json:"path"`
	MIME      string `json:"mime,omitempty"`
	Encoding  string `json:"encoding,omitempty"`
	Content   string `json:"content,omitempty"`
	Size      int64  `json:"size,omitempty"`
	Truncated bool   `json:"truncated,omitempty"`
}

type FilePage struct {
	Items      []FileEntry `json:"items"`
	NextCursor string      `json:"nextCursor"`
	Total      *int        `json:"total,omitempty"`
	Path       string      `json:"path"`
}

func Routes(prefix, protocol string) []plugin.Route {
	return []plugin.Route{
		{ID: prefix + ".files.list", Method: plugin.MethodGet, Path: "/files/list/{path}", Permission: protocol + ".files.read", Risk: plugin.RiskSafe, AuditEvent: protocol + ".files.list", Handle: list},
		{ID: prefix + ".files.read", Method: plugin.MethodGet, Path: "/files/read/{path}", Permission: protocol + ".files.read", Risk: plugin.RiskSafe, AuditEvent: protocol + ".files.read", Handle: read},
		{ID: prefix + ".files.download", Method: plugin.MethodGet, Path: "/files/download/{path}", Permission: protocol + ".files.read", Risk: plugin.RiskSafe, AuditEvent: protocol + ".files.download", Handle: download},
		{ID: prefix + ".files.stat", Method: plugin.MethodGet, Path: "/files/stat/{path}", Permission: protocol + ".files.read", Risk: plugin.RiskSafe, AuditEvent: protocol + ".files.stat", Handle: stat},
		{ID: prefix + ".files.write", Method: plugin.MethodPut, Path: "/files/write/{path}", Permission: protocol + ".files.write", Risk: plugin.RiskWrite, AuditEvent: protocol + ".files.write", Input: writeSchema(), Handle: writeFile},
		{ID: prefix + ".files.upload", Method: plugin.MethodPost, Path: "/files/upload/{path}", Permission: protocol + ".files.write", Risk: plugin.RiskWrite, AuditEvent: protocol + ".files.upload", Input: uploadSchema(), Handle: upload},
		{ID: prefix + ".files.mkdir", Method: plugin.MethodPost, Path: "/files/mkdir/{path}", Permission: protocol + ".files.write", Risk: plugin.RiskWrite, AuditEvent: protocol + ".files.mkdir", Input: nameSchema("Folder"), Handle: mkdir},
		{ID: prefix + ".files.rename", Method: plugin.MethodPatch, Path: "/files/rename/{path}", Permission: protocol + ".files.write", Risk: plugin.RiskWrite, AuditEvent: protocol + ".files.rename", Input: nameSchema("Name"), Handle: renameEntry},
		{ID: prefix + ".files.delete", Method: plugin.MethodDelete, Path: "/files/delete/{path}", Permission: protocol + ".files.write", Risk: plugin.RiskDestructive, AuditEvent: protocol + ".files.delete", Handle: deleteEntry},
		{ID: prefix + ".files.move", Method: plugin.MethodPost, Path: "/files/move", Permission: protocol + ".files.write", Risk: plugin.RiskWrite, AuditEvent: protocol + ".files.move", Handle: move},
		{ID: prefix + ".files.copy", Method: plugin.MethodPost, Path: "/files/copy", Permission: protocol + ".files.write", Risk: plugin.RiskWrite, AuditEvent: protocol + ".files.copy", Handle: copyFiles},
		{ID: prefix + ".files.chmod", Method: plugin.MethodPost, Path: "/files/chmod", Permission: protocol + ".files.write", Risk: plugin.RiskWrite, AuditEvent: protocol + ".files.chmod", Handle: chmod},
		{ID: prefix + ".files.archive", Method: plugin.MethodPost, Path: "/files/archive", Permission: protocol + ".files.read", Risk: plugin.RiskSafe, AuditEvent: protocol + ".files.archive", Handle: archive},
	}
}

// FilesOption opts a FilesTab into the bulk-operation slots a backend supports.
type FilesOption func(*plugin.FileBrowserConfig)

// WithMove populates the move bulk slot so the renderer surfaces that action.
// A backend opts in only for ops it implements; sibling options cover copy,
// chmod, and archive.
func WithMove(prefix string) FilesOption {
	return func(c *plugin.FileBrowserConfig) { c.MoveRouteID = prefix + ".files.move" }
}

func WithCopy(prefix string) FilesOption {
	return func(c *plugin.FileBrowserConfig) { c.CopyRouteID = prefix + ".files.copy" }
}

func WithChmod(prefix string) FilesOption {
	return func(c *plugin.FileBrowserConfig) { c.ChmodRouteID = prefix + ".files.chmod" }
}

func WithArchive(prefix string) FilesOption {
	return func(c *plugin.FileBrowserConfig) { c.ArchiveRouteID = prefix + ".files.archive" }
}

func FilesTab(prefix string, opts ...FilesOption) plugin.Panel {
	cfg := plugin.FileBrowserConfig{
		PathParam:       "path",
		ReadRouteID:     prefix + ".files.read",
		DownloadRouteID: prefix + ".files.download",
		WriteRouteID:    prefix + ".files.write",
		UploadRouteID:   prefix + ".files.upload",
		MkdirRouteID:    prefix + ".files.mkdir",
		RenameRouteID:   prefix + ".files.rename",
		DeleteRouteID:   prefix + ".files.delete",
		Writable:        true,
		MultipleUpload:  true,
		MaxUploadBytes:  52428800,
		UploadFieldName: "files",
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return plugin.Panel{
		Key: "files", Label: "Files", Icon: plugin.Icon{Type: plugin.IconLucide, Value: "folder"},
		Type:   plugin.PanelFileBrowser,
		Source: &plugin.DataSource{RouteID: prefix + ".files.list", Params: map[string]string{"path": "."}},
		Config: cfg,
	}
}

func uploadSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Upload", Fields: []plugin.Field{{Key: "files", Label: "Files", Type: plugin.FieldFile, Required: true}}}}}
}

func writeSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Content", Fields: []plugin.Field{{Key: "content", Label: "Content", Type: plugin.FieldTextarea, Required: true}}}}}
}

func nameSchema(label string) *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: label, Fields: []plugin.Field{{Key: "name", Label: label, Type: plugin.FieldText, Required: true}}}}}
}

func fsSession(rc *plugin.RequestContext) (Client, error) {
	s, ok := rc.Session.(Session)
	if !ok {
		type sessionGetter interface {
			Session() plugin.Session
		}
		if h, wrapped := rc.Session.(sessionGetter); wrapped {
			s, ok = h.Session().(Session)
		}
	}
	if !ok {
		return nil, fmt.Errorf("%w: filesystem session unavailable", plugin.ErrUnavailable)
	}
	return s.Filesystem()
}

func list(rc *plugin.RequestContext) (any, error) {
	fs, err := fsSession(rc)
	if err != nil {
		return nil, err
	}
	p, err := resolveRemotePath(rc.Ctx, fs, rc.Param("path"))
	if err != nil {
		return nil, err
	}
	infos, err := fs.ReadDir(rc.Ctx, p)
	if err != nil {
		return nil, mapClientError(fs, err)
	}
	entries := make([]FileEntry, 0, len(infos))
	for _, info := range infos {
		entries = append(entries, fileEntry(joinRemote(p, info.Name()), info))
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].IsDir != entries[j].IsDir {
			return entries[i].IsDir
		}
		return strings.ToLower(entries[i].Name) < strings.ToLower(entries[j].Name)
	})
	req, err := rc.Page()
	if err != nil {
		return nil, err
	}
	return pageEntries(p, entries, req), nil
}

func stat(rc *plugin.RequestContext) (any, error) {
	fs, err := fsSession(rc)
	if err != nil {
		return nil, err
	}
	p, err := resolveRemotePath(rc.Ctx, fs, rc.Param("path"))
	if err != nil {
		return nil, err
	}
	info, err := fs.Stat(rc.Ctx, p)
	if err != nil {
		return nil, mapClientError(fs, err)
	}
	return fileEntry(p, info), nil
}

func read(rc *plugin.RequestContext) (any, error) {
	fs, err := fsSession(rc)
	if err != nil {
		return nil, err
	}
	p, err := resolveRemotePath(rc.Ctx, fs, rc.Param("path"))
	if err != nil {
		return nil, err
	}
	info, err := fs.Stat(rc.Ctx, p)
	if err != nil {
		return nil, mapClientError(fs, err)
	}
	if info.IsDir() {
		return nil, plugin.ErrInvalidInput
	}
	f, err := fs.Open(rc.Ctx, p)
	if err != nil {
		return nil, mapClientError(fs, err)
	}
	defer func() { _ = f.Close() }()
	limit := previewLimit
	if info.Size() < int64(limit) {
		limit = int(info.Size())
	}
	buf := make([]byte, limit)
	n, rerr := io.ReadFull(f, buf)
	if rerr != nil && !errors.Is(rerr, io.ErrUnexpectedEOF) && !errors.Is(rerr, io.EOF) {
		return nil, mapClientError(fs, rerr)
	}
	buf = buf[:n]
	mimeType := mimeFor(p)
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}
	content := FileContent{Path: p, MIME: mimeType, Size: info.Size()}
	if isText(mimeType, buf) {
		content.Encoding = "utf8"
		content.Content = string(buf)
		content.Truncated = info.Size() > int64(n)
		return content, nil
	}
	content.Encoding = "binary"
	return content, nil
}

func download(rc *plugin.RequestContext) (any, error) {
	fs, err := fsSession(rc)
	if err != nil {
		return nil, err
	}
	p, err := resolveRemotePath(rc.Ctx, fs, rc.Param("path"))
	if err != nil {
		return nil, err
	}
	info, err := fs.Stat(rc.Ctx, p)
	if err != nil {
		return nil, mapClientError(fs, err)
	}
	if info.IsDir() {
		return nil, plugin.ErrInvalidInput
	}
	dl := &plugin.Download{
		Name:    path.Base(p),
		MIME:    mimeFor(p),
		Size:    info.Size(),
		ModTime: info.ModTime(),
		Inline:  rc.Param("inline") == "1",
	}
	switch fsc := fs.(type) {
	case Seekable:
		sk, err := fsc.OpenSeeker(rc.Ctx, p)
		if err != nil {
			return nil, mapClientError(fs, err)
		}
		dl.Seeker = sk
	case RangeOpener:
		dl.OpenRange = func(off, length int64) (io.ReadCloser, error) {
			r, err := fsc.OpenRange(rc.Ctx, p, off, length)
			if err != nil {
				return nil, mapClientError(fs, err)
			}
			return r, nil
		}
	default:
		f, err := fs.Open(rc.Ctx, p)
		if err != nil {
			return nil, mapClientError(fs, err)
		}
		dl.Body = f
	}
	return dl, nil
}

func upload(rc *plugin.RequestContext) (any, error) {
	fs, err := fsSession(rc)
	if err != nil {
		return nil, err
	}
	dir, err := resolveRemotePath(rc.Ctx, fs, rc.Param("path"))
	if err != nil {
		return nil, err
	}
	files := rc.Uploads("files")
	if len(files) == 0 {
		return nil, fmt.Errorf("%w: no files uploaded", plugin.ErrInvalidInput)
	}
	for _, file := range files {
		name, err := cleanName(file.Filename)
		if err != nil {
			return nil, err
		}
		src, err := file.Open()
		if err != nil {
			return nil, mapClientError(fs, err)
		}
		err = fs.Write(rc.Ctx, joinRemote(dir, name), src)
		_ = src.Close()
		if err != nil {
			return nil, mapClientError(fs, err)
		}
	}
	return map[string]bool{"ok": true}, nil
}

type writeRequest struct {
	Content string `json:"content"`
}

func writeFile(rc *plugin.RequestContext) (any, error) {
	fs, err := fsSession(rc)
	if err != nil {
		return nil, err
	}
	p, err := resolveRemotePath(rc.Ctx, fs, rc.Param("path"))
	if err != nil {
		return nil, err
	}
	var req writeRequest
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	if err := fs.Write(rc.Ctx, p, strings.NewReader(req.Content)); err != nil {
		return nil, mapClientError(fs, err)
	}
	return map[string]bool{"ok": true}, nil
}

type nameRequest struct {
	Name string `json:"name" validate:"required"`
}

func mkdir(rc *plugin.RequestContext) (any, error) {
	fs, err := fsSession(rc)
	if err != nil {
		return nil, err
	}
	dir, err := resolveRemotePath(rc.Ctx, fs, rc.Param("path"))
	if err != nil {
		return nil, err
	}
	var req nameRequest
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name, err := cleanName(req.Name)
	if err != nil {
		return nil, err
	}
	if err := fs.Mkdir(rc.Ctx, joinRemote(dir, name)); err != nil {
		return nil, mapClientError(fs, err)
	}
	return map[string]bool{"ok": true}, nil
}

func renameEntry(rc *plugin.RequestContext) (any, error) {
	fs, err := fsSession(rc)
	if err != nil {
		return nil, err
	}
	p, err := resolveRemotePath(rc.Ctx, fs, rc.Param("path"))
	if err != nil {
		return nil, err
	}
	var req nameRequest
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name, err := cleanName(req.Name)
	if err != nil {
		return nil, err
	}
	if err := fs.Rename(rc.Ctx, p, joinRemote(path.Dir(p), name)); err != nil {
		return nil, mapClientError(fs, err)
	}
	return map[string]bool{"ok": true}, nil
}

func deleteEntry(rc *plugin.RequestContext) (any, error) {
	fs, err := fsSession(rc)
	if err != nil {
		return nil, err
	}
	p, err := resolveRemotePath(rc.Ctx, fs, rc.Param("path"))
	if err != nil {
		return nil, err
	}
	info, err := fs.Stat(rc.Ctx, p)
	if err != nil {
		return nil, mapClientError(fs, err)
	}
	if err := fs.Remove(rc.Ctx, p, info.IsDir()); err != nil {
		return nil, mapClientError(fs, err)
	}
	return map[string]bool{"ok": true}, nil
}

func resolveRemotePath(ctx context.Context, fs Client, raw string) (string, error) {
	if strings.ContainsRune(raw, 0) {
		return "", fmt.Errorf("%w: invalid path", plugin.ErrInvalidInput)
	}
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "." || raw == "~" {
		return homePath(ctx, fs)
	}
	if strings.HasPrefix(raw, "~/") {
		home, err := homePath(ctx, fs)
		if err != nil {
			return "", err
		}
		raw = joinRemote(home, strings.TrimPrefix(raw, "~/"))
	}
	return cleanRemotePath(raw)
}

func homePath(ctx context.Context, fs Client) (string, error) {
	home, err := fs.Home(ctx)
	if err != nil {
		return "", mapClientError(fs, err)
	}
	return cleanRemotePath(home)
}

func cleanRemotePath(raw string) (string, error) {
	if strings.ContainsRune(raw, 0) {
		return "", fmt.Errorf("%w: invalid path", plugin.ErrInvalidInput)
	}
	if strings.TrimSpace(raw) == "" {
		raw = "/"
	}
	clean := path.Clean("/" + strings.TrimPrefix(raw, "/"))
	if clean == "." {
		clean = "/"
	}
	return clean, nil
}

func cleanName(raw string) (string, error) {
	name := path.Base(strings.TrimSpace(raw))
	if name == "." || name == "/" || name == "" || strings.ContainsRune(name, 0) {
		return "", fmt.Errorf("%w: invalid name", plugin.ErrInvalidInput)
	}
	return name, nil
}

func joinRemote(dir, name string) string {
	if dir == "/" {
		return "/" + name
	}
	return path.Join(dir, name)
}

func fileEntry(p string, info os.FileInfo) FileEntry {
	return FileEntry{
		Name:    info.Name(),
		Path:    p,
		IsDir:   info.IsDir(),
		Size:    info.Size(),
		MIME:    mimeFor(p),
		ModTime: info.ModTime(),
		Mode:    info.Mode().String(),
	}
}

func pageEntries(currentPath string, entries []FileEntry, req plugin.PageRequest) FilePage {
	offset := cursorOffset(req.Cursor)
	if offset < 0 || offset > len(entries) {
		offset = 0
	}
	limit := req.Limit
	if limit <= 0 {
		limit = plugin.DefaultPageLimit
	}
	end := offset + limit
	if end > len(entries) {
		end = len(entries)
	}
	next := ""
	if end < len(entries) {
		next = base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(end)))
	}
	total := len(entries)
	return FilePage{Items: entries[offset:end], NextCursor: next, Total: &total, Path: currentPath}
}

func cursorOffset(cursor string) int {
	if cursor == "" {
		return 0
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0
	}
	n, err := strconv.Atoi(string(raw))
	if err != nil {
		return 0
	}
	return n
}

func mimeFor(p string) string {
	return mime.TypeByExtension(strings.ToLower(path.Ext(p)))
}

func isText(mimeType string, buf []byte) bool {
	return strings.HasPrefix(mimeType, "text/") ||
		strings.Contains(mimeType, "json") ||
		strings.Contains(mimeType, "xml") ||
		strings.Contains(mimeType, "yaml") ||
		utf8.Valid(buf)
}

func mapClientError(fs Client, err error) error {
	if err == nil {
		return nil
	}
	if mapper, ok := fs.(ErrorMapper); ok {
		if mapped := mapper.MapError(err); mapped != nil {
			return mapped
		}
	}
	if errors.Is(err, plugin.ErrNotFound) || errors.Is(err, plugin.ErrForbidden) || errors.Is(err, plugin.ErrInvalidInput) {
		return err
	}
	if os.IsNotExist(err) {
		return plugin.ErrNotFound
	}
	if os.IsPermission(err) {
		return plugin.ErrForbidden
	}
	return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
}
