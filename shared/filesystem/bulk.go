package filesystem

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strconv"
	"strings"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

// Optional Client capabilities. The base Client interface stays minimal so every
// backend keeps working; handlers type-assert these and report a clean
// unsupported error when a backend doesn't implement one.

// Mover relocates a path within the backend.
type Mover interface {
	Move(ctx context.Context, src, dst string) error
}

// Copier duplicates a path within the backend.
type Copier interface {
	Copy(ctx context.Context, src, dst string) error
}

// Chmodder changes a path's permission bits.
type Chmodder interface {
	Chmod(ctx context.Context, p string, mode fs.FileMode) error
}

// Bounds for the generic archive walk so a bad selection can't exhaust memory or
// run unbounded.
const (
	archiveMaxEntries = 50000
	archiveMaxBytes   = int64(2) << 30 // 2 GiB
)

func errUnsupported(op string) error {
	return fmt.Errorf("%w: %s is not supported by this backend", plugin.ErrInvalidInput, op)
}

type pathsRequest struct {
	Paths []string `json:"paths"`
}

type destRequest struct {
	Paths []string `json:"paths"`
	Dest  string   `json:"dest"`
}

type chmodRequest struct {
	Paths []string `json:"paths"`
	Mode  string   `json:"mode"`
}

// resolvePaths validates and cleans each requested path, rejecting an empty
// selection or any malformed (e.g. traversal) entry.
func resolvePaths(raw []string) ([]string, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("%w: no paths provided", plugin.ErrInvalidInput)
	}
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		clean, err := cleanRemotePath(r)
		if err != nil {
			return nil, err
		}
		if clean == "/" {
			return nil, fmt.Errorf("%w: refusing to operate on root", plugin.ErrInvalidInput)
		}
		out = append(out, clean)
	}
	return out, nil
}

// parseMode parses an octal permission string (e.g. "0644" or "755").
func parseMode(raw string) (fs.FileMode, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("%w: mode is required", plugin.ErrInvalidInput)
	}
	v, err := strconv.ParseUint(raw, 8, 32)
	if err != nil || v > 0o7777 {
		return 0, fmt.Errorf("%w: invalid octal mode %q", plugin.ErrInvalidInput, raw)
	}
	return fs.FileMode(v), nil
}

func move(rc *plugin.RequestContext) (any, error) {
	c, err := fsSession(rc)
	if err != nil {
		return nil, err
	}
	mover, ok := c.(Mover)
	if !ok {
		return nil, errUnsupported("move")
	}
	srcs, dest, err := bindDest(rc)
	if err != nil {
		return nil, err
	}
	for _, src := range srcs {
		if err := mover.Move(rc.Ctx, src, joinRemote(dest, path.Base(src))); err != nil {
			return nil, mapClientError(c, err)
		}
	}
	return map[string]bool{"ok": true}, nil
}

func copyFiles(rc *plugin.RequestContext) (any, error) {
	c, err := fsSession(rc)
	if err != nil {
		return nil, err
	}
	copier, ok := c.(Copier)
	if !ok {
		return nil, errUnsupported("copy")
	}
	srcs, dest, err := bindDest(rc)
	if err != nil {
		return nil, err
	}
	for _, src := range srcs {
		if err := copier.Copy(rc.Ctx, src, joinRemote(dest, path.Base(src))); err != nil {
			return nil, mapClientError(c, err)
		}
	}
	return map[string]bool{"ok": true}, nil
}

func chmod(rc *plugin.RequestContext) (any, error) {
	c, err := fsSession(rc)
	if err != nil {
		return nil, err
	}
	chmodder, ok := c.(Chmodder)
	if !ok {
		return nil, errUnsupported("chmod")
	}
	var req chmodRequest
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	mode, err := parseMode(req.Mode)
	if err != nil {
		return nil, err
	}
	paths, err := resolvePaths(req.Paths)
	if err != nil {
		return nil, err
	}
	for _, p := range paths {
		if err := chmodder.Chmod(rc.Ctx, p, mode); err != nil {
			return nil, mapClientError(c, err)
		}
	}
	return map[string]bool{"ok": true}, nil
}

func bindDest(rc *plugin.RequestContext) (paths []string, dest string, err error) {
	var req destRequest
	if err = rc.Bind(&req); err != nil {
		return nil, "", err
	}
	dest, err = cleanRemotePath(req.Dest)
	if err != nil {
		return nil, "", err
	}
	paths, err = resolvePaths(req.Paths)
	if err != nil {
		return nil, "", err
	}
	return paths, dest, nil
}

// archive streams a zip built generically over the base Client (Stat/ReadDir/
// Open), so it works for any backend without a capability.
func archive(rc *plugin.RequestContext) (any, error) {
	c, err := fsSession(rc)
	if err != nil {
		return nil, err
	}
	var req pathsRequest
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	paths, err := resolvePaths(req.Paths)
	if err != nil {
		return nil, err
	}
	pr, pw := io.Pipe()
	name := archiveName(paths)
	go func() {
		zw := zip.NewWriter(pw)
		w := &archiveWalker{ctx: rc.Ctx, client: c, zw: zw}
		var werr error
		for _, p := range paths {
			if werr = w.add(p, path.Dir(p)); werr != nil {
				break
			}
		}
		if werr == nil {
			werr = zw.Close()
		} else {
			_ = zw.Close()
		}
		_ = pw.CloseWithError(werr)
	}()
	return &plugin.Download{
		Name: name,
		MIME: "application/zip",
		Size: -1,
		Body: pr,
	}, nil
}

func archiveName(paths []string) string {
	if len(paths) == 1 {
		return path.Base(paths[0]) + ".zip"
	}
	return "archive.zip"
}

type archiveWalker struct {
	ctx     context.Context
	client  Client
	zw      *zip.Writer
	entries int
	bytes   int64
}

// add zips p (a file or directory tree). base is stripped so the zip holds names
// relative to the selection root.
func (w *archiveWalker) add(p, base string) error {
	if err := w.ctx.Err(); err != nil {
		return err
	}
	info, err := w.client.Stat(w.ctx, p)
	if err != nil {
		return mapClientError(w.client, err)
	}
	rel := zipName(p, base)
	if rel == "" {
		return nil
	}
	w.entries++
	if w.entries > archiveMaxEntries {
		return fmt.Errorf("%w: archive exceeds %d entries", plugin.ErrInvalidInput, archiveMaxEntries)
	}
	if info.IsDir() {
		if _, err := w.zw.Create(rel + "/"); err != nil {
			return err
		}
		children, err := w.client.ReadDir(w.ctx, p)
		if err != nil {
			return mapClientError(w.client, err)
		}
		for _, child := range children {
			if err := w.add(joinRemote(p, child.Name()), base); err != nil {
				return err
			}
		}
		return nil
	}
	w.bytes += info.Size()
	if w.bytes > archiveMaxBytes {
		return fmt.Errorf("%w: archive exceeds size limit", plugin.ErrInvalidInput)
	}
	f, err := w.client.Open(w.ctx, p)
	if err != nil {
		return mapClientError(w.client, err)
	}
	defer func() { _ = f.Close() }()
	hw, err := w.zw.CreateHeader(&zip.FileHeader{Name: rel, Method: zip.Deflate, Modified: info.ModTime()})
	if err != nil {
		return err
	}
	_, err = io.Copy(hw, f)
	return err
}

func zipName(p, base string) string {
	rel := strings.TrimPrefix(p, strings.TrimSuffix(base, "/")+"/")
	rel = strings.TrimPrefix(rel, "/")
	return strings.TrimPrefix(rel, "./")
}
