package vscode

import (
	"context"
	"encoding/json"
	"errors"
	"html"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestManifest(t *testing.T) {
	plugintest.ValidatePlugin(t, VSCode{})
	m := VSCode{}.Manifest()
	if m.SupportsTransport(plugin.TransportDirect) || !m.SupportsTransport(plugin.TransportAgent) {
		t.Fatalf("VS Code should support only agent transport: %+v", m.SupportedTransports)
	}
	if m.Agent == nil {
		t.Fatal("VS Code should declare an agent profile")
	}
	if m.Agent.Proxy.Mode != plugin.AgentUnix || m.Agent.Proxy.Address != agentDockerSocket || !m.Agent.Proxy.Forward {
		t.Fatalf("unexpected agent proxy: %+v", m.Agent.Proxy)
	}
	if len(m.Agent.Install) != 2 {
		t.Fatalf("expected docker run and compose artifacts, got %+v", m.Agent.Install)
	}
	if m.Agent.Install[0].Kind != "docker-run" || !strings.Contains(m.Agent.Install[0].Template, "/var/run/docker.sock:/var/docker/docker.sock") {
		t.Fatalf("unexpected docker-run artifact: %+v", m.Agent.Install[0])
	}
	if m.Agent.Install[1].Kind != "docker-compose" {
		t.Fatalf("expected compose artifact, got %+v", m.Agent.Install[1])
	}
	compose := m.Agent.Install[1].Content
	for _, want := range []string{
		"network_mode: host",
		"read_only: true",
		`"/var/run/docker.sock:/var/docker/docker.sock"`,
	} {
		if !strings.Contains(compose, want) {
			t.Fatalf("agent compose missing %q in:\n%s", want, compose)
		}
	}
	if strings.Contains(compose, "code-server:") || strings.Contains(compose, "vscode-workspace") {
		t.Fatalf("agent compose should not declare static code-server service:\n%s", compose)
	}
	if len(m.Tabs) != 1 || m.Tabs[0].Type != plugin.PanelWebProxy {
		t.Fatalf("expected one web proxy panel, got %+v", m.Tabs)
	}
	cfg, ok := m.Tabs[0].Config.(plugin.WebProxyConfig)
	if !ok {
		t.Fatalf("expected web proxy config, got %T", m.Tabs[0].Config)
	}
	if cfg.InlineToolbar == nil || *cfg.InlineToolbar {
		t.Fatalf("expected vscode to disable inline web proxy toolbar, got %+v", cfg.InlineToolbar)
	}
}

func TestParseOptions(t *testing.T) {
	_, err := parseOptions(plugin.ConnectConfig{ConnectionID: "c1", ActorScope: "u1"})
	if err == nil || !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("expected missing agent transport error, got %v", err)
	}

	_, err = parseOptions(plugin.ConnectConfig{ConnectionID: "c1", ActorScope: "u1", Transport: plugin.TransportDirect})
	if err == nil || !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("expected direct transport error, got %v", err)
	}

	opts, err := parseOptions(plugin.ConnectConfig{ConnectionID: "c1", ActorScope: "u1", Transport: plugin.TransportAgent})
	if err != nil {
		t.Fatalf("parse defaults: %v", err)
	}
	if opts.WorkspacePath != defaultWorkspacePath {
		t.Fatalf("workspace path = %q, want %q", opts.WorkspacePath, defaultWorkspacePath)
	}

	_, err = parseOptions(plugin.ConnectConfig{ConnectionID: "c1", ActorScope: "u1", Transport: plugin.TransportAgent, Config: map[string]any{
		"sandbox": "none",
	}})
	if err == nil || !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("expected invalid sandbox error, got %v", err)
	}

	opts, err = parseOptions(plugin.ConnectConfig{ConnectionID: "c1", ActorScope: "u1", Transport: plugin.TransportAgent, Config: map[string]any{
		"workspace_path": "/src/app",
	}})
	if err != nil {
		t.Fatalf("parse workspace: %v", err)
	}
	if opts.WorkspacePath != "/src/app" {
		t.Fatalf("workspace path = %q", opts.WorkspacePath)
	}

	opts, err = parseOptions(plugin.ConnectConfig{ConnectionID: "c1", ActorScope: "u1", Transport: plugin.TransportAgent, Config: map[string]any{
		"repository_url": "https://github.com/acme/app.git",
		"repository_ref": "main",
	}})
	if err != nil {
		t.Fatalf("parse repository: %v", err)
	}
	if opts.RepositoryURL != "https://github.com/acme/app.git" || opts.RepositoryRef != "main" {
		t.Fatalf("repository options = %+v", opts)
	}
	if !strings.HasPrefix(opts.WorkspacePath, sandboxWorkspacePath+"/app-") {
		t.Fatalf("repository workspace path = %q", opts.WorkspacePath)
	}

	opts, err = parseOptions(plugin.ConnectConfig{ConnectionID: "c1", ActorScope: "u1", Transport: plugin.TransportAgent, Config: map[string]any{
		"repository_url":            "https://gitlab.com/acme/app.git",
		"repository_auth":           "inline_basic",
		"repository_basic_username": "oauth2",
		"repository_basic_password": "glpat_example",
	}})
	if err != nil {
		t.Fatalf("parse inline basic repository auth: %v", err)
	}
	if opts.RepositoryAuthHeader != gitBasicAuthHeader("oauth2", "glpat_example") {
		t.Fatalf("inline basic auth header = %q", opts.RepositoryAuthHeader)
	}

	opts, err = parseOptions(plugin.ConnectConfig{
		ConnectionID: "c1",
		ActorScope:   "u1",
		Transport:    plugin.TransportAgent,
		Config: map[string]any{
			"repository_url":  "https://git.example.com/acme/app.git",
			"repository_auth": "stored_basic",
		},
		Credentials: plugin.NewResolvedCredentials(plugin.CredentialBinding{
			Field: "repository_basic",
			Credential: plugin.ResolvedCredential{
				Kind:   plugin.CredentialKindBasicAuth,
				Values: map[string]string{"username": "alice", "password": "app-password"},
			},
		}),
	})
	if err != nil {
		t.Fatalf("parse stored basic repository auth: %v", err)
	}
	if opts.RepositoryAuthHeader != gitBasicAuthHeader("alice", "app-password") {
		t.Fatalf("stored basic auth header = %q", opts.RepositoryAuthHeader)
	}

	_, err = parseOptions(plugin.ConnectConfig{ConnectionID: "c1", ActorScope: "u1", Transport: plugin.TransportAgent, Config: map[string]any{
		"workspace_path": "relative",
	}})
	if err == nil || !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("expected invalid workspace path error, got %v", err)
	}

	_, err = parseOptions(plugin.ConnectConfig{ConnectionID: "c1", Transport: plugin.TransportAgent})
	if err == nil || !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("expected missing actor scope error, got %v", err)
	}

	_, err = parseOptions(plugin.ConnectConfig{ConnectionID: "c1", ActorScope: "u1", Transport: "other"})
	if err == nil || !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("expected invalid transport error, got %v", err)
	}
}

func TestDockerContainerNameIsStablePerActorAndConnection(t *testing.T) {
	opts := Options{ActorScope: "user@example.com", ConnectionID: "conn/one"}
	first := dockerContainerName(opts)
	second := dockerContainerName(opts)
	if first != second {
		t.Fatalf("container names should be stable, got %q and %q", first, second)
	}
	if !strings.HasPrefix(first, "shellcn-vscode-user-example-com-conn-one-") {
		t.Fatalf("container name %q missing actor/connection prefix", first)
	}
	otherActor := dockerContainerName(Options{ActorScope: "other@example.com", ConnectionID: "conn/one"})
	if otherActor == first {
		t.Fatalf("container name should change across actor scopes")
	}
	otherConnection := dockerContainerName(Options{ActorScope: "user@example.com", ConnectionID: "conn/two"})
	if otherConnection == first {
		t.Fatalf("container name should change across connections")
	}
}

func TestDockerVolumeNameIsScopedPerActorConnectionAndPurpose(t *testing.T) {
	opts := Options{ActorScope: "user@example.com", ConnectionID: "conn/one"}
	workspace := dockerVolumeName(opts, "workspace")
	home := dockerVolumeName(opts, "home")
	if workspace == home {
		t.Fatalf("volume names should vary by purpose")
	}
	if !strings.HasPrefix(workspace, "shellcn-vscode-user-example-com-conn-one-workspace-") {
		t.Fatalf("workspace volume %q missing scope", workspace)
	}
}

func TestCodeServerArgsUseDockerSandboxPaths(t *testing.T) {
	joined := "\x00" + strings.Join(codeServerArgs(sandboxWorkspacePath), "\x00") + "\x00"
	for _, want := range []string{
		"\x00--bind-addr\x000.0.0.0:8080\x00",
		"\x00--auth\x00none\x00",
		"\x00--user-data-dir\x00" + sandboxUserDataDir + "\x00",
		"\x00--extensions-dir\x00" + sandboxExtensionsDir + "\x00",
		"\x00" + sandboxWorkspacePath + "\x00",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("code-server args missing %q in %#v", want, codeServerArgs(sandboxWorkspacePath))
		}
	}
}

func TestCodeServerUsesOfficialEntrypoint(t *testing.T) {
	if codeServerEntrypoint != "/usr/bin/entrypoint.sh" {
		t.Fatalf("code-server entrypoint = %q", codeServerEntrypoint)
	}
}

func TestSandboxTmpfsAllowsEditorToolingTemps(t *testing.T) {
	tmpfs := sandboxTmpfs()
	opts := tmpfs["/tmp"]
	if !strings.Contains(opts, "size=1g") {
		t.Fatalf("sandbox tmpfs should leave room for editor tooling, got %q", opts)
	}
	if strings.Contains(opts, "noexec") {
		t.Fatalf("sandbox tmpfs should not block tooling that executes temp files: %q", opts)
	}
}

func TestWorkspacePrepareScriptFixesVolumeOwnership(t *testing.T) {
	for _, want := range []string{
		"mkdir -p /workspace /home/coder/.config /user-data/User /user-data/Machine /extensions",
		"git clone --",
		"remote.origin.url",
		"rev-parse --verify HEAD",
		"git -C \"$dest\" fetch origin --tags --prune",
		"git -C \"$dest\" config remote.origin.fetch '+refs/heads/*:refs/remotes/origin/*'",
		"chown -R 1000:1000 /workspace/. /home/coder/. /user-data/. /extensions/.",
		"chmod -R u+rwX,g+rwX /workspace/. /home/coder/. /user-data/. /extensions/.",
	} {
		if !strings.Contains(workspacePrepareScript, want) {
			t.Fatalf("workspace prep script missing %q:\n%s", want, workspacePrepareScript)
		}
	}
}

func TestWorkspaceProbeScriptChecksVSCodeWritableDirs(t *testing.T) {
	for _, want := range []string{
		"mkdir -p /user-data/User",
		"touch /user-data/User/.shellcn-probe",
		"touch /extensions/.shellcn-probe",
		"touch /home/coder/.config/.shellcn-probe",
		"touch /workspace/.shellcn-probe/file",
	} {
		if !strings.Contains(workspaceProbeScript, want) {
			t.Fatalf("workspace probe script missing %q:\n%s", want, workspaceProbeScript)
		}
	}
}

func TestRepositoryAuthHeadersUseBasicEncoding(t *testing.T) {
	tokenHeader := gitTokenAuthHeader("ghp_example")
	if !strings.HasPrefix(tokenHeader, "Authorization: Basic ") {
		t.Fatalf("token auth header = %q", tokenHeader)
	}
	if strings.Contains(tokenHeader, "ghp_example") || strings.Contains(tokenHeader, "Bearer") {
		t.Fatalf("token auth header should be basic encoded without a bearer token: %q", tokenHeader)
	}

	basicHeader := gitBasicAuthHeader("oauth2", "glpat_example")
	if !strings.HasPrefix(basicHeader, "Authorization: Basic ") {
		t.Fatalf("basic auth header = %q", basicHeader)
	}
	if strings.Contains(basicHeader, "oauth2") || strings.Contains(basicHeader, "glpat_example") {
		t.Fatalf("basic auth header should be encoded without visible credentials: %q", basicHeader)
	}
}

func TestSessionCloseDockerContainerCleanup(t *testing.T) {
	removed := ""
	orig := removeDockerContainerFunc
	removeDockerContainerFunc = func(_ context.Context, _ *dockerRuntime, containerID string) error {
		removed = containerID
		return nil
	}
	t.Cleanup(func() { removeDockerContainerFunc = orig })

	s := &Session{runtime: &dockerRuntime{}, containerID: "container-123"}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if removed != "container-123" {
		t.Fatalf("removed container = %q", removed)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if removed != "container-123" {
		t.Fatalf("second close removed unexpected container = %q", removed)
	}
}

func TestRewriteCodeServerWorkbenchConfig(t *testing.T) {
	prefix := "/api/connections/c1/proxy"
	cfg := `{"serverBasePath":".","webviewEndpoint":"./stable-x/static/out/vs/workbench/contrib/webview/browser/pre","callbackRoute":"./stable-x/callback","productConfiguration":{"rootEndpoint":".","proxyEndpointTemplate":"./proxy/{{port}}/","serviceWorker":{"scope":"./","path":"./_static/out/browser/serviceWorker.js"}}}`
	page := `<html><head><meta id="vscode-workbench-web-configuration" data-settings="` + html.EscapeString(cfg) + `"></head></html>`

	out := rewriteCodeServerWorkbenchConfig(page, prefix)
	got := decodeWorkbenchConfig(t, out)

	if got["serverBasePath"] != "." {
		t.Fatalf("serverBasePath = %v, want relative root", got["serverBasePath"])
	}
	if got["webviewEndpoint"] != "stable-x/static/out/vs/workbench/contrib/webview/browser/pre" {
		t.Fatalf("webviewEndpoint = %v", got["webviewEndpoint"])
	}
	if got["callbackRoute"] != "stable-x/callback" {
		t.Fatalf("callbackRoute = %v", got["callbackRoute"])
	}
	product, ok := got["productConfiguration"].(map[string]any)
	if !ok {
		t.Fatalf("productConfiguration has type %T", got["productConfiguration"])
	}
	if product["rootEndpoint"] != "." {
		t.Fatalf("rootEndpoint = %v", product["rootEndpoint"])
	}
	if product["proxyEndpointTemplate"] != "./proxy/{{port}}/" {
		t.Fatalf("proxyEndpointTemplate = %v", product["proxyEndpointTemplate"])
	}
	sw, ok := product["serviceWorker"].(map[string]any)
	if !ok {
		t.Fatalf("serviceWorker has type %T", product["serviceWorker"])
	}
	if sw["scope"] != "./" {
		t.Fatalf("serviceWorker.scope = %v", sw["scope"])
	}
	if sw["path"] != "./_static/out/browser/serviceWorker.js" {
		t.Fatalf("serviceWorker.path = %v", sw["path"])
	}
}

func TestServeHTTPProxyRedirectsToWorkspaceFolder(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected upstream request: %s", r.URL.RequestURI())
	}))
	defer upstream.Close()
	u, err := urlParse(upstream.URL)
	if err != nil {
		t.Fatalf("parse upstream URL: %v", err)
	}
	sess := &Session{baseURL: u, transport: http.DefaultTransport, workspacePath: "/src/app"}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(plugin.ProxyPrefixHeader, "/api/connections/c1/proxy")
	rec := httptest.NewRecorder()

	sess.ServeHTTPProxy(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("status = %d, body = %q", rec.Code, rec.Body.String())
	}
	if loc := rec.Header().Get("Location"); loc != "/api/connections/c1/proxy/?folder=%2Fsrc%2Fapp" {
		t.Fatalf("Location = %q", loc)
	}
}

func TestStartSessionIntegration(t *testing.T) {
	_, err := newDockerRuntime(plugin.ConnectConfig{Transport: plugin.TransportAgent})
	if err == nil || !errors.Is(err, plugin.ErrUnavailable) {
		t.Fatalf("expected missing agent net error, got %v", err)
	}
}

func decodeWorkbenchConfig(t *testing.T, page string) map[string]any {
	t.Helper()
	g := workbenchConfigMeta.FindStringSubmatch(page)
	if len(g) != 4 {
		t.Fatalf("workbench config meta not found in %q", page)
	}
	var cfg map[string]any
	if err := json.Unmarshal([]byte(html.UnescapeString(g[2])), &cfg); err != nil {
		t.Fatalf("decode workbench config: %v", err)
	}
	return cfg
}

func urlParse(raw string) (*url.URL, error) {
	return url.Parse(raw)
}

type failingNet struct{}

func (failingNet) DialContext(context.Context, string, string) (net.Conn, error) {
	return nil, errors.New("dial disabled")
}

func (failingNet) HTTP() (string, http.RoundTripper, bool) {
	return "", nil, false
}
