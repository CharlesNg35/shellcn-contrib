package notebook

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/api/types/network"
	dockerclient "github.com/moby/moby/client"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	defaultNotebookImage = "quay.io/jupyter/minimal-notebook:python-3.13.5"
	defaultWorkspaceName = "workspace"
	sandboxDocker        = "docker"
	agentDockerSocket    = "/var/docker/docker.sock"
	jupyterPort          = "8888/tcp"
	containerHome        = "/home/jovyan"
	containerWorkspace   = "/home/jovyan/work"
	containerJupyter     = "/home/jovyan/.jupyter"
	containerLocal       = "/home/jovyan/.local"
	containerIPython     = "/home/jovyan/.ipython"
	notebookSourcePrefix = "/shellcn-notebook"
	legacySWFile         = "__shellcn_sw.js"
	startupTimeout       = 60 * time.Second
)

var removeDockerContainerFunc = func(ctx context.Context, rt *dockerRuntime, containerID string) error {
	return rt.removeContainer(ctx, containerID)
}

type Options struct {
	ConnectionID string
	UserID       string
	ActorScope   string
	Image        string
}

type Session struct {
	baseURL     *url.URL
	transport   http.RoundTripper
	runtime     *dockerRuntime
	containerID string
	once        sync.Once
}

type dockerRuntime struct {
	cli       *dockerclient.Client
	dialApp   func(context.Context, string, string) (net.Conn, error)
	closeOnce sync.Once
}

func (Notebook) Connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	opts, err := parseOptions(cfg)
	if err != nil {
		return nil, err
	}
	rt, err := newDockerRuntime(cfg)
	if err != nil {
		return nil, err
	}
	s, err := startSession(ctx, rt, opts)
	if err != nil {
		_ = rt.Close()
		return nil, err
	}
	return s, nil
}

func parseOptions(cfg plugin.ConnectConfig) (Options, error) {
	if cfg.Transport != plugin.TransportAgent {
		return Options{}, fmt.Errorf("%w: notebook requires agent transport", plugin.ErrInvalidInput)
	}
	opts := Options{
		ConnectionID: strings.TrimSpace(cfg.ConnectionID),
		UserID:       strings.TrimSpace(cfg.UserID),
		ActorScope:   strings.TrimSpace(cfg.ActorScope),
		Image:        strings.TrimSpace(cfg.String("image")),
	}
	if opts.Image == "" {
		opts.Image = defaultNotebookImage
	}
	sandbox := strings.TrimSpace(cfg.String("sandbox"))
	if sandbox == "" {
		sandbox = sandboxDocker
	}
	if sandbox != sandboxDocker {
		return Options{}, fmt.Errorf("%w: unsupported sandbox %q", plugin.ErrInvalidInput, sandbox)
	}
	if opts.ConnectionID == "" {
		return Options{}, fmt.Errorf("%w: connection id is required", plugin.ErrInvalidInput)
	}
	if opts.ActorScope == "" && opts.UserID == "" {
		return Options{}, fmt.Errorf("%w: actor scope is required", plugin.ErrInvalidInput)
	}
	return opts, nil
}

func newDockerRuntime(cfg plugin.ConnectConfig) (*dockerRuntime, error) {
	daemonDial, appDial, err := dockerDialers(cfg)
	if err != nil {
		return nil, err
	}
	cli, err := dockerclient.New(
		dockerclient.WithHost("http://docker"),
		dockerclient.WithDialContext(func(ctx context.Context, _, _ string) (net.Conn, error) {
			return daemonDial(ctx)
		}),
		dockerclient.WithAPIVersionNegotiation(),
	)
	if err != nil {
		return nil, fmt.Errorf("%w: create docker client: %v", plugin.ErrUnavailable, err)
	}
	return &dockerRuntime{cli: cli, dialApp: appDial}, nil
}

func dockerDialers(cfg plugin.ConnectConfig) (
	func(context.Context) (net.Conn, error),
	func(context.Context, string, string) (net.Conn, error),
	error,
) {
	if cfg.Net == nil {
		return nil, nil, fmt.Errorf("%w: agent transport is unavailable", plugin.ErrUnavailable)
	}
	return func(ctx context.Context) (net.Conn, error) {
			return cfg.Net.DialContext(ctx, "unix", agentDockerSocket)
		},
		cfg.Net.DialContext,
		nil
}

func startSession(ctx context.Context, rt *dockerRuntime, opts Options) (*Session, error) {
	if err := rt.HealthCheck(ctx); err != nil {
		return nil, err
	}
	containerID, err := rt.startJupyter(ctx, opts)
	if err != nil {
		return nil, err
	}
	host, err := rt.publishedAddress(ctx, containerID)
	if err != nil {
		_ = rt.removeContainer(context.Background(), containerID)
		return nil, err
	}
	transport := &http.Transport{DialContext: rt.dialApp, ForceAttemptHTTP2: false}
	s := &Session{
		baseURL:     &url.URL{Scheme: "http", Host: host},
		transport:   transport,
		runtime:     rt,
		containerID: containerID,
	}
	if err := s.waitReady(ctx); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

func (rt *dockerRuntime) HealthCheck(ctx context.Context) error {
	_, err := rt.cli.Ping(ctx, dockerclient.PingOptions{})
	if err != nil {
		return fmt.Errorf("%w: docker daemon unavailable: %v", plugin.ErrUnavailable, err)
	}
	return nil
}

func (rt *dockerRuntime) startJupyter(ctx context.Context, opts Options) (string, error) {
	name := dockerContainerName(opts)
	_ = rt.removeContainer(ctx, name)
	if err := rt.pullImage(ctx, opts.Image); err != nil {
		return "", err
	}
	port := network.MustParsePort(jupyterPort)
	created, err := rt.cli.ContainerCreate(ctx, dockerclient.ContainerCreateOptions{
		Name: name,
		Config: &container.Config{
			Image:        opts.Image,
			User:         "1000:1000",
			Entrypoint:   []string{"jupyter"},
			Cmd:          jupyterArgs(),
			Env:          jupyterEnv(),
			WorkingDir:   containerWorkspace,
			ExposedPorts: network.PortSet{port: struct{}{}},
			Labels:       dockerLabels(opts),
		},
		HostConfig: &container.HostConfig{
			AutoRemove:     false,
			CapDrop:        []string{"ALL"},
			PortBindings:   network.PortMap{port: []network.PortBinding{{HostIP: netip.MustParseAddr("127.0.0.1")}}},
			ReadonlyRootfs: true,
			SecurityOpt:    []string{"no-new-privileges:true"},
			Tmpfs:          map[string]string{"/tmp": "rw,nosuid,nodev,noexec,size=256m"},
			Mounts: []mount.Mount{
				{Type: mount.TypeVolume, Source: dockerVolumeName(opts, "home"), Target: containerHome},
				{Type: mount.TypeVolume, Source: dockerVolumeName(opts, "workspace"), Target: containerWorkspace},
			},
		},
	})
	if err != nil {
		return "", dockerErr("create docker sandbox", err)
	}
	if _, err := rt.cli.ContainerStart(ctx, created.ID, dockerclient.ContainerStartOptions{}); err != nil {
		_ = rt.removeContainer(context.Background(), created.ID)
		return "", dockerErr("start docker sandbox", err)
	}
	return created.ID, nil
}

func (rt *dockerRuntime) pullImage(ctx context.Context, image string) error {
	pull, err := rt.cli.ImagePull(ctx, image, dockerclient.ImagePullOptions{})
	if err != nil {
		return dockerErr("pull docker sandbox image", err)
	}
	defer func() { _ = pull.Close() }()
	if err := pull.Wait(ctx); err != nil {
		return dockerErr("pull docker sandbox image", err)
	}
	return nil
}

func (rt *dockerRuntime) publishedAddress(ctx context.Context, containerID string) (string, error) {
	inspect, err := rt.cli.ContainerInspect(ctx, containerID, dockerclient.ContainerInspectOptions{})
	if err != nil {
		return "", dockerErr("inspect docker sandbox", err)
	}
	if inspect.Container.NetworkSettings == nil {
		return "", fmt.Errorf("%w: docker sandbox has no network settings", plugin.ErrUnavailable)
	}
	port := network.MustParsePort(jupyterPort)
	bindings := inspect.Container.NetworkSettings.Ports[port]
	if len(bindings) == 0 || bindings[0].HostPort == "" {
		return "", fmt.Errorf("%w: docker sandbox has no published notebook port", plugin.ErrUnavailable)
	}
	host := bindings[0].HostIP
	if !host.IsValid() || host.IsUnspecified() {
		host = netip.MustParseAddr("127.0.0.1")
	}
	return net.JoinHostPort(host.String(), bindings[0].HostPort), nil
}

func (rt *dockerRuntime) containerRunning(ctx context.Context, containerID string) bool {
	inspect, err := rt.cli.ContainerInspect(ctx, containerID, dockerclient.ContainerInspectOptions{})
	return err == nil && inspect.Container.State != nil && inspect.Container.State.Running
}

func (rt *dockerRuntime) runtimeLogs(ctx context.Context, containerID string) string {
	logs, err := rt.cli.ContainerLogs(ctx, containerID, dockerclient.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Tail:       "80",
	})
	if err != nil {
		return strings.TrimSpace(err.Error())
	}
	defer func() { _ = logs.Close() }()
	body, err := io.ReadAll(io.LimitReader(logs, 64<<10))
	if err != nil {
		return strings.TrimSpace(err.Error())
	}
	return strings.TrimSpace(string(body))
}

func (rt *dockerRuntime) removeContainer(ctx context.Context, containerID string) error {
	if strings.TrimSpace(containerID) == "" {
		return nil
	}
	_, err := rt.cli.ContainerRemove(ctx, containerID, dockerclient.ContainerRemoveOptions{Force: true})
	if err != nil && !cerrdefs.IsNotFound(err) {
		return dockerErr("remove docker sandbox", err)
	}
	return nil
}

func (rt *dockerRuntime) Close() error {
	if rt == nil || rt.cli == nil {
		return nil
	}
	var err error
	rt.closeOnce.Do(func() {
		err = rt.cli.Close()
	})
	return err
}

func jupyterArgs() []string {
	return []string{
		"lab",
		"--ip=0.0.0.0",
		"--port=8888",
		"--no-browser",
		"--IdentityProvider.token=",
		"--ServerApp.password=",
		"--ServerApp.allow_remote_access=True",
		"--ServerApp.root_dir=" + containerWorkspace,
		"--ServerApp.base_url=" + notebookSourcePrefix + "/",
		"--ServerApp.default_url=/lab",
		"--ServerApp.quit_button=False",
	}
}

func jupyterEnv() []string {
	return []string{
		"HOME=" + containerHome,
		"JUPYTER_CONFIG_DIR=" + containerJupyter,
		"JUPYTER_DATA_DIR=" + containerLocal + "/share/jupyter",
		"JUPYTER_RUNTIME_DIR=" + containerLocal + "/share/jupyter/runtime",
		"IPYTHONDIR=" + containerIPython,
	}
}

func dockerLabels(opts Options) map[string]string {
	return map[string]string{
		"shellcn.managed":           "true",
		"shellcn.plugin":            "notebook",
		"shellcn.connection":        safeLabelValue(opts.ConnectionID),
		"shellcn.actor_scope":       safeLabelValue(scopeSegment(opts)),
		"shellcn.notebook_base_url": notebookSourcePrefix + "/",
	}
}

func dockerContainerName(opts Options) string {
	seed := scopeSegment(opts) + "-" + connectionSegment(opts)
	sum := sha256.Sum256([]byte(seed))
	return "shellcn-notebook-" + safeDockerName(seed, 32) + "-" + hex.EncodeToString(sum[:])[:12]
}

func dockerVolumeName(opts Options, suffix string) string {
	seed := scopeSegment(opts) + "-" + connectionSegment(opts) + "-" + suffix
	sum := sha256.Sum256([]byte(seed))
	return "shellcn-notebook-" + safeDockerName(seed, 42) + "-" + hex.EncodeToString(sum[:])[:12]
}

func safeDockerName(name string, max int) string {
	out := safeName(name)
	if len(out) <= max {
		return out
	}
	return strings.Trim(out[:max], "-_")
}

func safeLabelValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	return value
}

func scopeSegment(opts Options) string {
	scope := strings.TrimSpace(opts.ActorScope)
	if scope == "" {
		scope = strings.TrimSpace(opts.UserID)
	}
	if scope == "" {
		scope = "unknown-actor"
	}
	return safeName(scope)
}

func connectionSegment(opts Options) string {
	connectionID := strings.TrimSpace(opts.ConnectionID)
	if connectionID == "" {
		connectionID = "unknown-connection"
	}
	return safeName(connectionID)
}

func safeName(name string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(name) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
		} else if b.Len() == 0 || !strings.HasSuffix(b.String(), "-") {
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), "-_")
	if out == "" {
		return defaultWorkspaceName
	}
	return out
}

func dockerErr(action string, err error) error {
	if err == nil {
		return nil
	}
	switch {
	case cerrdefs.IsNotFound(err):
		return fmt.Errorf("%w: %s: %v", plugin.ErrNotFound, action, err)
	case cerrdefs.IsInvalidArgument(err):
		return fmt.Errorf("%w: %s: %v", plugin.ErrInvalidInput, action, err)
	case cerrdefs.IsConflict(err):
		return fmt.Errorf("%w: %s: %v", plugin.ErrConflict, action, err)
	case cerrdefs.IsUnavailable(err):
		return fmt.Errorf("%w: %s: %v", plugin.ErrUnavailable, action, err)
	default:
		return fmt.Errorf("%s: %w", action, err)
	}
}

func (s *Session) waitReady(ctx context.Context) error {
	deadline := time.Now().Add(startupTimeout)
	for {
		if err := s.HealthCheck(ctx); err == nil {
			return nil
		}
		if s.exited(ctx) {
			return fmt.Errorf("%w: JupyterLab exited: %s", plugin.ErrUnavailable, s.runtimeLogs(ctx))
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%w: JupyterLab did not become ready: %s", plugin.ErrUnavailable, s.runtimeLogs(ctx))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func (s *Session) exited(ctx context.Context) bool {
	if s.runtime == nil || s.containerID == "" {
		return false
	}
	return !s.runtime.containerRunning(ctx, s.containerID)
}

func (s *Session) runtimeLogs(ctx context.Context) string {
	if s.runtime == nil || s.containerID == "" {
		return ""
	}
	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return s.runtime.runtimeLogs(cctx, s.containerID)
}

func (s *Session) HealthCheck(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.baseURL.String()+notebookSourcePrefix+"/lab", nil)
	if err != nil {
		return err
	}
	client := http.Client{Transport: s.transport, Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 500 {
		return fmt.Errorf("%w: notebook returned %s", plugin.ErrUnavailable, resp.Status)
	}
	return nil
}

func (s *Session) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}

func (s *Session) Close() error {
	var err error
	s.once.Do(func() {
		if s.runtime != nil && s.containerID != "" {
			err = removeDockerContainerFunc(context.Background(), s.runtime, s.containerID)
		}
		if s.runtime != nil {
			if closeErr := s.runtime.Close(); err == nil {
				err = closeErr
			}
		}
	})
	return err
}

func (s *Session) ServeHTTPProxy(w http.ResponseWriter, r *http.Request) {
	serveProxy(w, r, s.baseURL, s.transport)
}

func serveProxy(w http.ResponseWriter, r *http.Request, base *url.URL, transport http.RoundTripper) {
	prefix := plugin.RequestProxyPrefix(r)
	path := stripPublicPrefix(r.URL.Path, prefix)
	if path == "/"+legacySWFile {
		serveLegacyWorkerCleanup(w, prefix)
		return
	}
	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = base.Scheme
			req.URL.Host = base.Host
			req.URL.Path = notebookUpstreamPath(path)
			req.URL.RawPath = ""
			req.Host = base.Host
			req.Header.Set("Accept-Encoding", "identity")
			req.Header.Set("X-Forwarded-Host", r.Host)
			req.Header.Set("X-Forwarded-Prefix", prefix)
			req.Header.Set("X-Forwarded-Proto", forwardedProto(r))
			req.Header.Set("X-Forwarded-Uri", r.URL.RequestURI())
			req.Header.Set("Forwarded", forwardedHeader(r))
			rewriteNotebookOrigin(req)
			applyNotebookWebSocketHeaders(req)
		},
		Transport:     notebookProxyTransport(transport),
		FlushInterval: -1,
		ModifyResponse: func(resp *http.Response) error {
			return rewriteNotebookResponse(resp, base, prefix, r.Host)
		},
	}
	proxy.ServeHTTP(w, r)
}

func stripPublicPrefix(path, prefix string) string {
	if prefix != "" {
		switch {
		case path == prefix:
			return "/"
		case strings.HasPrefix(path, prefix+"/"):
			return strings.TrimPrefix(path, prefix)
		}
	}
	if path == "" {
		return "/"
	}
	return path
}

func rewriteNotebookOrigin(req *http.Request) {
	if req.URL == nil || req.URL.Host == "" || req.Header.Get("Origin") == "" {
		return
	}
	scheme := req.URL.Scheme
	if scheme == "" {
		scheme = "http"
	}
	req.Header.Set("Origin", scheme+"://"+req.URL.Host)
}

func serveLegacyWorkerCleanup(w http.ResponseWriter, prefix string) {
	w.Header().Set("Content-Type", "text/javascript")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Service-Worker-Allowed", prefix+"/")
	_, _ = io.WriteString(w, `self.addEventListener("install",function(){self.skipWaiting();});self.addEventListener("activate",function(e){e.waitUntil(self.registration.unregister().then(function(){return self.clients.matchAll();}).then(function(clients){clients.forEach(function(client){client.navigate(client.url);});}));});`)
}

func notebookUpstreamPath(path string) string {
	if path == "" || path == "/" {
		return notebookSourcePrefix + "/"
	}
	if strings.HasPrefix(path, notebookSourcePrefix+"/") || path == notebookSourcePrefix {
		return path
	}
	return notebookSourcePrefix + path
}

func applyNotebookWebSocketHeaders(req *http.Request) {
	if !strings.EqualFold(req.Header.Get("Upgrade"), "websocket") || req.URL == nil || req.URL.Host == "" {
		return
	}
	req.Host = req.URL.Host
	req.Header.Del("Forwarded")
	req.Header["X-Forwarded-For"] = nil
	req.Header.Del("X-Forwarded-Host")
	req.Header.Del("X-Forwarded-Prefix")
	req.Header.Del("X-Forwarded-Proto")
	req.Header.Del("X-Forwarded-Uri")
}

func rewriteNotebookResponse(resp *http.Response, base *url.URL, prefix, publicHost string) error {
	resp.Header.Set("Alt-Svc", "clear")
	resp.Header.Del("Alt-Used")
	if loc := resp.Header.Get("Location"); loc != "" {
		resp.Header.Set("Location", mapNotebookLocation(loc, base, prefix, publicHost))
	}
	rewriteNotebookCookiePaths(resp.Header, prefix)
	resp.Header.Del("Content-Security-Policy")
	resp.Header.Del("Content-Security-Policy-Report-Only")
	resp.Header.Del("X-Frame-Options")

	ct := resp.Header.Get("Content-Type")
	if !shouldRewriteNotebookBody(ct) {
		return nil
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	_ = resp.Body.Close()
	if err != nil {
		return err
	}
	out := string(body)
	if base != nil {
		out = strings.ReplaceAll(out, base.Scheme+"://"+base.Host, prefix)
	}
	out = strings.ReplaceAll(out, notebookSourcePrefix, prefix)
	resp.Body = io.NopCloser(strings.NewReader(out))
	resp.ContentLength = int64(len(out))
	resp.Header.Set("Content-Length", strconv.Itoa(len(out)))
	return nil
}

func shouldRewriteNotebookBody(contentType string) bool {
	contentType = strings.ToLower(contentType)
	return strings.Contains(contentType, "text/html") ||
		strings.Contains(contentType, "text/css") ||
		strings.Contains(contentType, "text/javascript") ||
		strings.Contains(contentType, "application/javascript") ||
		strings.Contains(contentType, "application/json")
}

func mapNotebookLocation(loc string, base *url.URL, prefix, publicHost string) string {
	if loc == "" {
		return loc
	}
	if strings.HasPrefix(loc, notebookSourcePrefix) {
		return prefix + strings.TrimPrefix(loc, notebookSourcePrefix)
	}
	u, err := url.Parse(loc)
	if err != nil {
		return loc
	}
	upstreamHost := ""
	if base != nil {
		upstreamHost = base.Host
	}
	if u.IsAbs() {
		if u.Host != upstreamHost && u.Host != publicHost {
			return loc
		}
		if strings.HasPrefix(u.Path, notebookSourcePrefix) {
			u.Scheme = ""
			u.Host = ""
			u.Path = prefix + strings.TrimPrefix(u.Path, notebookSourcePrefix)
			return u.String()
		}
	}
	if strings.HasPrefix(u.Path, notebookSourcePrefix) {
		u.Path = prefix + strings.TrimPrefix(u.Path, notebookSourcePrefix)
		return u.String()
	}
	return loc
}

func rewriteNotebookCookiePaths(header http.Header, prefix string) {
	cookies := header.Values("Set-Cookie")
	if len(cookies) == 0 {
		return
	}
	header.Del("Set-Cookie")
	for _, cookie := range cookies {
		parts := strings.Split(cookie, ";")
		out := make([]string, 0, len(parts)+1)
		hasPath := false
		for i, part := range parts {
			trimmed := strings.TrimSpace(part)
			if i == 0 {
				out = append(out, trimmed)
				continue
			}
			lower := strings.ToLower(trimmed)
			switch {
			case strings.HasPrefix(lower, "domain="):
				continue
			case strings.HasPrefix(lower, "path="):
				hasPath = true
				out = append(out, "Path="+prefix)
			default:
				out = append(out, trimmed)
			}
		}
		if !hasPath {
			out = append(out, "Path="+prefix)
		}
		header.Add("Set-Cookie", strings.Join(out, "; "))
	}
}

func forwardedProto(r *http.Request) string {
	if proto := strings.TrimSpace(r.Header.Get("X-Forwarded-Proto")); proto != "" {
		return proto
	}
	if r.TLS != nil {
		return "https"
	}
	return "http"
}

func forwardedHeader(r *http.Request) string {
	return "proto=" + forwardedProto(r) + `;host="` + strings.ReplaceAll(r.Host, `"`, "") + `"`
}

func notebookProxyTransport(rt http.RoundTripper) http.RoundTripper {
	if rt == nil {
		return http.DefaultTransport
	}
	return rt
}
