// Package nfs implements the NFSv3 filesystem plugin.
package nfs

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	nfsclient "github.com/willscott/go-nfs-client/nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"

	"github.com/charlesng35/shellcn-contrib/shared/filesystem"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const protocolName = "nfs"

type Plugin struct{}

func New() *Plugin { return &Plugin{} }

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:          plugin.CurrentAPIVersion,
		Name:                protocolName,
		Version:             "0.1.0",
		Title:               "NFS",
		Description:         "File browser for NFSv3 exports.",
		Icon:                plugin.Icon{Type: plugin.IconLucide, Value: "network"},
		Category:            plugin.CategoryFiles,
		Config:              configSchema(),
		Capabilities:        []plugin.Capability{"filesystem"},
		SupportedTransports: []plugin.Transport{plugin.TransportDirect},
		Layout:              plugin.LayoutSingle,
		Tabs: []plugin.Panel{filesystem.FilesTab(
			protocolName,
			filesystem.WithMove(protocolName),
			filesystem.WithCopy(protocolName),
			filesystem.WithArchive(protocolName),
		)},
	}
}

func (p *Plugin) Routes() []plugin.Route {
	return filesystem.Routes(protocolName, protocolName)
}

func (p *Plugin) Connect(_ context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	opts, err := parseOptions(cfg)
	if err != nil {
		return nil, err
	}
	mount, err := nfsclient.DialMount(opts.Host, 30*time.Second)
	if err != nil {
		return nil, fmt.Errorf("%w: dial nfs mount service: %v", plugin.ErrUnavailable, err)
	}
	auth := rpc.NewAuthUnix(opts.MachineName, opts.UID, opts.GID).Auth()
	target, err := mount.Mount(opts.ExportPath, auth)
	if err != nil {
		mount.Close()
		return nil, fmt.Errorf("%w: mount nfs export: %v", plugin.ErrUnauthorized, err)
	}
	return &Session{mount: mount, target: target, fs: &Client{target: target, root: opts.RootPath}}, nil
}

func configSchema() plugin.Schema {
	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Server", Fields: []plugin.Field{
			{Key: "host", Label: "Host", Type: plugin.FieldText, Required: true, Placeholder: "nfs.example.internal"},
			{Key: "export_path", Label: "Export path", Type: plugin.FieldText, Required: true, Placeholder: "/srv/share"},
			{Key: "root_path", Label: "Root path", Type: plugin.FieldText, Default: "/", Placeholder: "/"},
		}},
		{Name: "AUTH_SYS", Fields: []plugin.Field{
			{Key: "machine_name", Label: "Machine name", Type: plugin.FieldText, Default: plugin.DefaultClientName},
			{Key: "uid", Label: "UID", Type: plugin.FieldNumber, Default: 0, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 0}}},
			{Key: "gid", Label: "GID", Type: plugin.FieldNumber, Default: 0, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 0}}},
		}},
	}}
}

type options struct {
	Host        string
	ExportPath  string
	RootPath    string
	MachineName string
	UID         uint32
	GID         uint32
}

func parseOptions(cfg plugin.ConnectConfig) (options, error) {
	host := strings.TrimSpace(cfg.String("host"))
	if host == "" {
		return options{}, fmt.Errorf("%w: host is required", plugin.ErrInvalidInput)
	}
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	exportPath := strings.TrimSpace(cfg.String("export_path"))
	if exportPath == "" {
		return options{}, fmt.Errorf("%w: export path is required", plugin.ErrInvalidInput)
	}
	opts := options{
		Host:        host,
		ExportPath:  exportPath,
		RootPath:    strings.TrimSpace(cfg.String("root_path")),
		MachineName: strings.TrimSpace(cfg.String("machine_name")),
		UID:         uint32Value(cfg, "uid", 0),
		GID:         uint32Value(cfg, "gid", 0),
	}
	if opts.RootPath == "" {
		opts.RootPath = "/"
	}
	if opts.MachineName == "" {
		opts.MachineName = plugin.DefaultClientName
	}
	return opts, nil
}

type Session struct {
	mount  *nfsclient.Mount
	target *nfsclient.Target
	fs     *Client
}

func (s *Session) Filesystem() (filesystem.Client, error) {
	return s.fs, nil
}

func (s *Session) HealthCheck(context.Context) error {
	_, err := s.target.FSInfo()
	return err
}

func (s *Session) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}

func (s *Session) Close() error {
	var err error
	if s.mount != nil {
		err = s.mount.Unmount()
		s.mount.Close()
	}
	if s.target != nil {
		s.target.Close()
	}
	return err
}

type Client struct {
	target *nfsclient.Target
	root   string
}

func (c *Client) Home(context.Context) (string, error) {
	return c.root, nil
}

func (c *Client) ReadDir(_ context.Context, p string) ([]os.FileInfo, error) {
	entries, err := c.target.ReadDirPlus(p)
	if err != nil {
		return nil, err
	}
	infos := make([]os.FileInfo, 0, len(entries))
	for _, entry := range entries {
		infos = append(infos, nfsInfo{entry: entry})
	}
	return infos, nil
}

func (c *Client) Stat(_ context.Context, p string) (os.FileInfo, error) {
	info, _, err := c.target.Lookup(p)
	return namedInfo{name: pathBase(p), FileInfo: info}, err
}

func (c *Client) Open(_ context.Context, p string) (io.ReadCloser, error) {
	return c.target.Open(p)
}

func (c *Client) OpenSeeker(_ context.Context, p string) (io.ReadSeekCloser, error) {
	f, err := c.target.Open(p)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (c *Client) Write(_ context.Context, p string, r io.Reader) error {
	f, err := c.target.OpenFile(p, 0o644)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(f, r)
	closeErr := f.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func (c *Client) Mkdir(_ context.Context, p string) error {
	_, err := c.target.Mkdir(p, 0o755)
	return err
}

func (c *Client) Rename(_ context.Context, from, to string) error {
	return c.target.Rename(from, to)
}

func (c *Client) Remove(_ context.Context, p string, isDir bool) error {
	if isDir {
		return c.target.RmDir(p)
	}
	return c.target.Remove(p)
}

func (c *Client) Move(_ context.Context, src, dst string) error {
	return c.target.Rename(src, dst)
}

func (c *Client) Copy(ctx context.Context, src, dst string) error {
	r, err := c.Open(ctx, src)
	if err != nil {
		return err
	}
	defer func() { _ = r.Close() }()
	return c.Write(ctx, dst, r)
}

type nfsInfo struct {
	entry *nfsclient.EntryPlus
}

func (i nfsInfo) Name() string       { return i.entry.Name() }
func (i nfsInfo) Size() int64        { return i.entry.Size() }
func (i nfsInfo) Mode() os.FileMode  { return i.entry.Mode() }
func (i nfsInfo) ModTime() time.Time { return i.entry.ModTime() }
func (i nfsInfo) IsDir() bool        { return i.entry.IsDir() }
func (i nfsInfo) Sys() any           { return i.entry.Sys() }

type namedInfo struct {
	name string
	os.FileInfo
}

func (i namedInfo) Name() string {
	if i.name != "" {
		return i.name
	}
	return i.FileInfo.Name()
}

func uint32Value(cfg plugin.ConnectConfig, key string, fallback uint32) uint32 {
	if n, ok := cfg.Int(key); ok && n >= 0 {
		return uint32(n)
	}
	if raw := strings.TrimSpace(cfg.String(key)); raw != "" {
		if n, err := strconv.ParseUint(raw, 10, 32); err == nil {
			return uint32(n)
		}
	}
	return fallback
}

func pathBase(p string) string {
	p = strings.TrimRight(p, "/")
	if p == "" {
		return "/"
	}
	idx := strings.LastIndex(p, "/")
	if idx == -1 {
		return p
	}
	return p[idx+1:]
}
