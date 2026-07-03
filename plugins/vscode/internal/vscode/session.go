package vscode

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"path/filepath"
	"regexp"
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
	"github.com/charlesng35/shellcn/sdk/plugin/webproxy"
)

const (
	defaultWorkspacePath  = "/workspace"
	sandboxDocker         = "docker"
	agentDockerSocket     = "/var/docker/docker.sock"
	sandboxUser           = "1000:1000"
	sandboxHomeDir        = "/home/coder"
	sandboxWorkspacePath  = "/workspace"
	sandboxUserDataDir    = "/user-data"
	sandboxExtensionsDir  = "/extensions"
	dockerCodeServerImage = "codercom/code-server:4.127.0"
	codeServerEntrypoint  = "/usr/bin/entrypoint.sh"
	dockerCodeServerPort  = "8080/tcp"
	startupTimeout        = 45 * time.Second
	gitTimeout            = 2 * time.Minute
)

type VSCode struct{}

var removeDockerContainerFunc = func(ctx context.Context, rt *dockerRuntime, containerID string) error {
	return rt.removeContainer(ctx, containerID)
}

type Options struct {
	RepositoryURL   string
	RepositoryRef   string
	RepositoryToken string
	ConnectionID    string
	UserID          string
	ActorScope      string
	WorkspacePath   string
}

type Session struct {
	baseURL       *url.URL
	transport     http.RoundTripper
	runtime       *dockerRuntime
	containerID   string
	workspacePath string
	once          sync.Once
}

type dockerRuntime struct {
	cli       *dockerclient.Client
	dialApp   func(context.Context, string, string) (net.Conn, error)
	closeOnce sync.Once
}

func (VSCode) Connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
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
		return Options{}, fmt.Errorf("%w: vscode requires agent transport", plugin.ErrInvalidInput)
	}
	opts := Options{
		RepositoryURL: strings.TrimSpace(cfg.String("repository_url")),
		RepositoryRef: strings.TrimSpace(cfg.String("repository_ref")),
		ConnectionID:  strings.TrimSpace(cfg.ConnectionID),
		UserID:        strings.TrimSpace(cfg.UserID),
		ActorScope:    strings.TrimSpace(cfg.ActorScope),
		WorkspacePath: strings.TrimSpace(cfg.String("workspace_path")),
	}
	if opts.WorkspacePath == "" {
		opts.WorkspacePath = defaultWorkspacePath
	}
	sandbox := strings.TrimSpace(cfg.String("sandbox"))
	if sandbox == "" {
		sandbox = sandboxDocker
	}
	if sandbox != sandboxDocker {
		return Options{}, fmt.Errorf("%w: unsupported sandbox %q", plugin.ErrInvalidInput, sandbox)
	}
	if !strings.HasPrefix(opts.WorkspacePath, "/") {
		return Options{}, fmt.Errorf("%w: workspace path must be absolute", plugin.ErrInvalidInput)
	}
	if opts.ConnectionID == "" {
		return Options{}, fmt.Errorf("%w: connection id is required", plugin.ErrInvalidInput)
	}
	if opts.ActorScope == "" && opts.UserID == "" {
		return Options{}, fmt.Errorf("%w: actor scope is required", plugin.ErrInvalidInput)
	}
	authMode := strings.TrimSpace(cfg.String("repository_auth"))
	if authMode == "" {
		authMode = "none"
	}
	if opts.RepositoryURL != "" {
		u, err := url.Parse(opts.RepositoryURL)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return Options{}, fmt.Errorf("%w: repository URL must be absolute", plugin.ErrInvalidInput)
		}
		switch authMode {
		case "none":
		case "stored_token":
			cred, err := cfg.RequiredCredentialFor("repository_token", plugin.CredentialKindAPIToken)
			if err != nil {
				return Options{}, err
			}
			token, err := cred.RequiredValue("token")
			if err != nil {
				return Options{}, err
			}
			opts.RepositoryToken = token
		case "inline_token":
			token := strings.TrimSpace(cfg.String("repository_token_value"))
			if token == "" {
				return Options{}, fmt.Errorf("%w: repository token is required", plugin.ErrInvalidInput)
			}
			opts.RepositoryToken = token
		default:
			return Options{}, fmt.Errorf("%w: unsupported repository auth mode %q", plugin.ErrInvalidInput, authMode)
		}
		opts.WorkspacePath = filepath.Join(sandboxWorkspacePath, repoSlug(opts.RepositoryURL))
	} else if authMode != "none" {
		return Options{}, fmt.Errorf("%w: repository auth requires a repository URL", plugin.ErrInvalidInput)
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
	if err := rt.prepareVolumesAndRepo(ctx, opts); err != nil {
		return nil, err
	}
	containerID, err := rt.startCodeServer(ctx, opts)
	if err != nil {
		return nil, err
	}
	host, err := rt.publishedAddress(ctx, containerID)
	if err != nil {
		_ = rt.removeContainer(context.Background(), containerID)
		return nil, err
	}
	transport := &http.Transport{DialContext: rt.dialApp}
	s := &Session{
		baseURL:       &url.URL{Scheme: "http", Host: host},
		transport:     transport,
		runtime:       rt,
		containerID:   containerID,
		workspacePath: opts.WorkspacePath,
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

func (rt *dockerRuntime) startCodeServer(ctx context.Context, opts Options) (string, error) {
	name := dockerContainerName(opts)
	_ = rt.removeContainer(ctx, name)
	if err := rt.pullImage(ctx, dockerCodeServerImage); err != nil {
		return "", err
	}
	port := network.MustParsePort(dockerCodeServerPort)
	created, err := rt.cli.ContainerCreate(ctx, dockerclient.ContainerCreateOptions{
		Name: name,
		Config: &container.Config{
			Image:        dockerCodeServerImage,
			User:         sandboxUser,
			Entrypoint:   []string{codeServerEntrypoint},
			Cmd:          codeServerArgs(opts.WorkspacePath),
			Env:          codeServerEnv(),
			WorkingDir:   sandboxWorkspacePath,
			ExposedPorts: network.PortSet{port: struct{}{}},
			Labels:       dockerLabels(opts),
		},
		HostConfig: &container.HostConfig{
			AutoRemove:     false,
			CapDrop:        []string{"ALL"},
			PortBindings:   network.PortMap{port: []network.PortBinding{{HostIP: netip.MustParseAddr("127.0.0.1")}}},
			ReadonlyRootfs: true,
			SecurityOpt:    []string{"no-new-privileges:true"},
			Tmpfs:          map[string]string{"/tmp": "rw,nosuid,nodev,noexec,size=64m"},
			Mounts: []mount.Mount{
				{Type: mount.TypeVolume, Source: workspaceVolumeName(opts), Target: sandboxWorkspacePath},
				{Type: mount.TypeVolume, Source: dockerVolumeName(opts, "home"), Target: sandboxHomeDir},
				{Type: mount.TypeVolume, Source: dockerVolumeName(opts, "user-data"), Target: sandboxUserDataDir},
				{Type: mount.TypeVolume, Source: dockerVolumeName(opts, "extensions"), Target: sandboxExtensionsDir},
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

func (rt *dockerRuntime) prepareVolumesAndRepo(ctx context.Context, opts Options) error {
	if err := rt.pullImage(ctx, dockerCodeServerImage); err != nil {
		return err
	}
	gitCtx, cancel := context.WithTimeout(ctx, gitTimeout)
	defer cancel()
	name := dockerContainerName(opts) + "-git"
	_ = rt.removeContainer(gitCtx, name)
	env := []string{
		"REPOSITORY_URL=" + opts.RepositoryURL,
		"REPOSITORY_REF=" + opts.RepositoryRef,
		"REPOSITORY_DEST=" + opts.WorkspacePath,
		"HOME=/tmp",
	}
	if opts.RepositoryToken != "" {
		env = append(env,
			"GIT_CONFIG_COUNT=1",
			"GIT_CONFIG_KEY_0=http.extraHeader",
			"GIT_CONFIG_VALUE_0=Authorization: Bearer "+opts.RepositoryToken,
		)
	}
	created, err := rt.cli.ContainerCreate(gitCtx, dockerclient.ContainerCreateOptions{
		Name: name,
		Config: &container.Config{
			Image:      dockerCodeServerImage,
			User:       "0:0",
			Entrypoint: []string{"sh"},
			Cmd:        []string{"-ec", workspacePrepareScript},
			Env:        env,
			WorkingDir: sandboxWorkspacePath,
			Labels: map[string]string{
				"shellcn.managed":     "true",
				"shellcn.plugin":      "vscode",
				"shellcn.purpose":     "repository",
				"shellcn.connection":  safeLabelValue(opts.ConnectionID),
				"shellcn.actor_scope": safeLabelValue(scopeSegment(opts)),
			},
		},
		HostConfig: &container.HostConfig{
			AutoRemove:     false,
			ReadonlyRootfs: true,
			SecurityOpt:    []string{"no-new-privileges:true"},
			Tmpfs:          map[string]string{"/tmp": "rw,nosuid,nodev,noexec,size=64m"},
			Mounts: []mount.Mount{
				{Type: mount.TypeVolume, Source: workspaceVolumeName(opts), Target: sandboxWorkspacePath},
				{Type: mount.TypeVolume, Source: dockerVolumeName(opts, "home"), Target: sandboxHomeDir},
				{Type: mount.TypeVolume, Source: dockerVolumeName(opts, "user-data"), Target: sandboxUserDataDir},
				{Type: mount.TypeVolume, Source: dockerVolumeName(opts, "extensions"), Target: sandboxExtensionsDir},
			},
		},
	})
	if err != nil {
		return dockerErr("create repository checkout", err)
	}
	defer func() { _ = rt.removeContainer(context.Background(), created.ID) }()
	wait := rt.cli.ContainerWait(gitCtx, created.ID, dockerclient.ContainerWaitOptions{Condition: container.WaitConditionNotRunning})
	if _, err := rt.cli.ContainerStart(gitCtx, created.ID, dockerclient.ContainerStartOptions{}); err != nil {
		return dockerErr("start repository checkout", err)
	}
	select {
	case err := <-wait.Error:
		return dockerErr("wait repository checkout", err)
	case result := <-wait.Result:
		if result.StatusCode != 0 {
			return fmt.Errorf("%w: workspace preparation failed: %s", plugin.ErrUnavailable, rt.runtimeLogs(gitCtx, created.ID))
		}
	case <-gitCtx.Done():
		return gitCtx.Err()
	}
	return rt.probeWorkspacePermissions(gitCtx, opts)
}

const workspacePrepareScript = `
set -eu
mkdir -p /workspace /home/coder /user-data /extensions
if [ -n "${REPOSITORY_URL:-}" ]; then
  dest="${REPOSITORY_DEST:-/workspace}"
  mkdir -p "$(dirname "$dest")"
  git config --global --add safe.directory '*'
  if [ -d "$dest/.git" ] &&
    [ "$(git -C "$dest" config --get remote.origin.url || true)" = "$REPOSITORY_URL" ] &&
    git -C "$dest" rev-parse --verify HEAD >/dev/null 2>&1; then
    git -C "$dest" config remote.origin.fetch '+refs/heads/*:refs/remotes/origin/*'
    git -C "$dest" fetch origin --tags --prune
  else
    rm -rf "$dest"
    git clone -- "$REPOSITORY_URL" "$dest"
  fi
  if [ -n "${REPOSITORY_REF:-}" ]; then
    git -C "$dest" checkout "$REPOSITORY_REF"
  elif ! git -C "$dest" rev-parse --verify HEAD >/dev/null 2>&1; then
    default_ref="$(git -C "$dest" symbolic-ref --quiet --short refs/remotes/origin/HEAD || true)"
    if [ -z "$default_ref" ]; then
      default_ref="origin/main"
    fi
    git -C "$dest" checkout -B "${default_ref#origin/}" "$default_ref"
  fi
fi
chown -R 1000:1000 /workspace /home/coder /user-data /extensions
chmod -R u+rwX,g+rwX /workspace /home/coder /user-data /extensions
`

func (rt *dockerRuntime) probeWorkspacePermissions(ctx context.Context, opts Options) error {
	name := dockerContainerName(opts) + "-probe"
	_ = rt.removeContainer(ctx, name)
	created, err := rt.cli.ContainerCreate(ctx, dockerclient.ContainerCreateOptions{
		Name: name,
		Config: &container.Config{
			Image:      dockerCodeServerImage,
			User:       sandboxUser,
			Entrypoint: []string{"sh"},
			Cmd:        []string{"-ec", workspaceProbeScript},
			Env:        []string{"HOME=" + sandboxHomeDir},
			WorkingDir: sandboxWorkspacePath,
			Labels: map[string]string{
				"shellcn.managed":     "true",
				"shellcn.plugin":      "vscode",
				"shellcn.purpose":     "permissions",
				"shellcn.connection":  safeLabelValue(opts.ConnectionID),
				"shellcn.actor_scope": safeLabelValue(scopeSegment(opts)),
			},
		},
		HostConfig: &container.HostConfig{
			AutoRemove:     false,
			CapDrop:        []string{"ALL"},
			ReadonlyRootfs: true,
			SecurityOpt:    []string{"no-new-privileges:true"},
			Tmpfs:          map[string]string{"/tmp": "rw,nosuid,nodev,noexec,size=64m"},
			Mounts: []mount.Mount{
				{Type: mount.TypeVolume, Source: workspaceVolumeName(opts), Target: sandboxWorkspacePath},
				{Type: mount.TypeVolume, Source: dockerVolumeName(opts, "home"), Target: sandboxHomeDir},
				{Type: mount.TypeVolume, Source: dockerVolumeName(opts, "user-data"), Target: sandboxUserDataDir},
				{Type: mount.TypeVolume, Source: dockerVolumeName(opts, "extensions"), Target: sandboxExtensionsDir},
			},
		},
	})
	if err != nil {
		return dockerErr("create workspace permission probe", err)
	}
	defer func() { _ = rt.removeContainer(context.Background(), created.ID) }()
	wait := rt.cli.ContainerWait(ctx, created.ID, dockerclient.ContainerWaitOptions{Condition: container.WaitConditionNotRunning})
	if _, err := rt.cli.ContainerStart(ctx, created.ID, dockerclient.ContainerStartOptions{}); err != nil {
		return dockerErr("start workspace permission probe", err)
	}
	select {
	case err := <-wait.Error:
		return dockerErr("wait workspace permission probe", err)
	case result := <-wait.Result:
		if result.StatusCode != 0 {
			return fmt.Errorf("%w: workspace permissions are not writable by sandbox user: %s", plugin.ErrUnavailable, rt.runtimeLogs(ctx, created.ID))
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

const workspaceProbeScript = `
set -eu
mkdir -p /user-data/User /extensions /home/coder/.config /workspace/.shellcn-probe
touch /user-data/User/.shellcn-probe
touch /extensions/.shellcn-probe
touch /home/coder/.config/.shellcn-probe
touch /workspace/.shellcn-probe/file
rm -f /user-data/User/.shellcn-probe /extensions/.shellcn-probe /home/coder/.config/.shellcn-probe
rm -rf /workspace/.shellcn-probe
`

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
	port := network.MustParsePort(dockerCodeServerPort)
	if inspect.Container.NetworkSettings == nil {
		return "", fmt.Errorf("%w: docker sandbox has no network settings", plugin.ErrUnavailable)
	}
	bindings := inspect.Container.NetworkSettings.Ports[port]
	if len(bindings) == 0 || bindings[0].HostPort == "" {
		return "", fmt.Errorf("%w: docker sandbox has no published editor port", plugin.ErrUnavailable)
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

func codeServerArgs(workspacePath string) []string {
	if strings.TrimSpace(workspacePath) == "" {
		workspacePath = sandboxWorkspacePath
	}
	return []string{
		"--bind-addr", "0.0.0.0:8080",
		"--auth", "none",
		"--disable-telemetry",
		"--disable-update-check",
		"--user-data-dir", sandboxUserDataDir,
		"--extensions-dir", sandboxExtensionsDir,
		workspacePath,
	}
}

func codeServerEnv() []string {
	return []string{
		"HOME=" + sandboxHomeDir,
		"XDG_CONFIG_HOME=" + sandboxHomeDir + "/.config",
		"XDG_CACHE_HOME=" + sandboxHomeDir + "/.cache",
		"XDG_DATA_HOME=" + sandboxHomeDir + "/.local/share",
	}
}

func dockerLabels(opts Options) map[string]string {
	return map[string]string{
		"shellcn.managed":     "true",
		"shellcn.plugin":      "vscode",
		"shellcn.connection":  safeLabelValue(opts.ConnectionID),
		"shellcn.actor_scope": safeLabelValue(scopeSegment(opts)),
	}
}

func dockerContainerName(opts Options) string {
	seed := scopeSegment(opts) + "-" + connectionSegment(opts)
	sum := sha256.Sum256([]byte(seed))
	return "shellcn-vscode-" + safeDockerName(seed, 32) + "-" + hex.EncodeToString(sum[:])[:12]
}

func dockerVolumeName(opts Options, suffix string) string {
	seed := scopeSegment(opts) + "-" + connectionSegment(opts) + "-" + suffix
	sum := sha256.Sum256([]byte(seed))
	return "shellcn-vscode-" + safeDockerName(seed, 42) + "-" + hex.EncodeToString(sum[:])[:12]
}

func workspaceVolumeName(opts Options) string {
	return dockerVolumeName(opts, "workspace")
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
		return "workspace"
	}
	return out
}

func repoSlug(raw string) string {
	u, err := url.Parse(raw)
	name := "workspace"
	if err == nil {
		base := strings.TrimSuffix(filepath.Base(u.Path), ".git")
		if base != "." && base != "/" && base != "" {
			name = base
		}
	}
	sum := sha256.Sum256([]byte(raw))
	return safeName(name) + "-" + hex.EncodeToString(sum[:])[:12]
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
			return fmt.Errorf("%w: code-server exited: %s", plugin.ErrUnavailable, s.runtimeLogs(ctx))
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%w: code-server did not become ready: %s", plugin.ErrUnavailable, s.runtimeLogs(ctx))
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
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.baseURL.String()+"/", nil)
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
		return fmt.Errorf("%w: editor returned %s", plugin.ErrUnavailable, resp.Status)
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
	prefix := plugin.RequestProxyPrefix(r)
	if r.URL.Path == "/"+webproxy.SWFile {
		webproxy.ServeWorker(w, prefix)
		return
	}
	req := r
	if s.workspacePath != "" && r.URL.Path == "/" && r.URL.RawQuery == "" {
		q := url.Values{}
		q.Set("folder", s.workspacePath)
		location := strings.TrimRight(prefix, "/") + "/?" + q.Encode()
		if prefix == "" {
			location = "/?" + q.Encode()
		}
		http.Redirect(w, r, location, http.StatusFound)
		return
	}
	webproxy.Serve(w, req, webproxy.Options{
		Base:            s.baseURL,
		Transport:       codeServerTransport{base: s.transport, publicPrefix: prefix},
		UpstreamPath:    req.URL.Path,
		UpstreamRawPath: req.URL.RawPath,
		PublicPrefix:    prefix,
		WebSocket:       codeServerWebSocketOptions(),
	})
}

var workbenchConfigMeta = regexp.MustCompile(`(<meta[^>]+id="vscode-workbench-web-configuration"[^>]+data-settings=")([^"]*)(")`)

type codeServerTransport struct {
	base         http.RoundTripper
	publicPrefix string
}

func (t codeServerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	resp, err := base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	if !strings.Contains(resp.Header.Get("Content-Type"), "text/html") || t.publicPrefix == "" {
		return resp, nil
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	_ = resp.Body.Close()
	if err != nil {
		return nil, err
	}
	out := rewriteCodeServerWorkbenchConfig(string(body), t.publicPrefix)
	resp.Body = io.NopCloser(strings.NewReader(out))
	resp.ContentLength = int64(len(out))
	resp.Header.Set("Content-Length", strconv.Itoa(len(out)))
	return resp, nil
}

func codeServerWebSocketOptions() webproxy.WebSocketOptions {
	return webproxy.WebSocketOptions{
		RewriteOrigin:         true,
		StripForwardedHeaders: true,
	}
}

func rewriteCodeServerWorkbenchConfig(page, prefix string) string {
	return workbenchConfigMeta.ReplaceAllStringFunc(page, func(match string) string {
		g := workbenchConfigMeta.FindStringSubmatch(match)
		if len(g) != 4 {
			return match
		}
		var cfg map[string]any
		if err := json.Unmarshal([]byte(html.UnescapeString(g[2])), &cfg); err != nil {
			return match
		}
		normalizeCodeServerPath(cfg, "webviewEndpoint")
		normalizeCodeServerPath(cfg, "callbackRoute")
		out, err := json.Marshal(cfg)
		if err != nil {
			return match
		}
		return g[1] + html.EscapeString(string(out)) + g[3]
	})
}

func normalizeCodeServerPath(cfg map[string]any, key string) {
	v, ok := cfg[key].(string)
	if !ok {
		return
	}
	cfg[key] = strings.TrimPrefix(strings.TrimSpace(v), "./")
}
