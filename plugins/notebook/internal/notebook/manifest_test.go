package notebook

import (
	"context"
	"errors"
	"io"
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
	plugintest.ValidatePlugin(t, Notebook{})
	m := Notebook{}.Manifest()
	if m.SupportsTransport(plugin.TransportDirect) || !m.SupportsTransport(plugin.TransportAgent) {
		t.Fatalf("Notebook should support only agent transport: %+v", m.SupportedTransports)
	}
	if m.Agent == nil {
		t.Fatal("Notebook should declare an agent profile")
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
	if strings.Contains(compose, "jupyter:") || strings.Contains(compose, "notebook-workspace") {
		t.Fatalf("agent compose should not declare static Jupyter service:\n%s", compose)
	}
	if len(m.Tabs) != 1 || m.Tabs[0].Type != plugin.PanelWebProxy {
		t.Fatalf("expected one web proxy panel, got %+v", m.Tabs)
	}
	cfg, ok := m.Tabs[0].Config.(plugin.WebProxyConfig)
	if !ok {
		t.Fatalf("expected web proxy config, got %T", m.Tabs[0].Config)
	}
	if cfg.Path != "/lab" {
		t.Fatalf("web proxy path = %q, want /lab", cfg.Path)
	}
	if cfg.InlineToolbar == nil || *cfg.InlineToolbar {
		t.Fatalf("expected notebook to disable inline web proxy toolbar, got %+v", cfg.InlineToolbar)
	}
	if !hasWebProxyCapability(cfg.Capabilities, plugin.WebProxyCapabilitySameOrigin) {
		t.Fatal("notebook iframe should request same-origin sandbox capability for JupyterLab")
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
	if opts.Image != defaultNotebookImage {
		t.Fatalf("image = %q, want %q", opts.Image, defaultNotebookImage)
	}

	_, err = parseOptions(plugin.ConnectConfig{ConnectionID: "c1", ActorScope: "u1", Transport: plugin.TransportAgent, Config: map[string]any{
		"sandbox": "none",
	}})
	if err == nil || !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("expected invalid sandbox error, got %v", err)
	}

	opts, err = parseOptions(plugin.ConnectConfig{ConnectionID: "c1", ActorScope: "u1", Transport: plugin.TransportAgent, Config: map[string]any{
		"image": "example/notebook:latest",
	}})
	if err != nil {
		t.Fatalf("parse image: %v", err)
	}
	if opts.Image != "example/notebook:latest" {
		t.Fatalf("image = %q", opts.Image)
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

func hasWebProxyCapability(capabilities []plugin.WebProxyCapability, want plugin.WebProxyCapability) bool {
	for _, capability := range capabilities {
		if capability == want {
			return true
		}
	}
	return false
}

func TestDockerContainerNameIsStablePerActorAndConnection(t *testing.T) {
	opts := Options{ActorScope: "user@example.com", ConnectionID: "conn/one"}
	first := dockerContainerName(opts)
	second := dockerContainerName(opts)
	if first != second {
		t.Fatalf("container names should be stable, got %q and %q", first, second)
	}
	if !strings.HasPrefix(first, "shellcn-notebook-user-example-com-conn-one-") {
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
	if !strings.HasPrefix(workspace, "shellcn-notebook-user-example-com-conn-one-workspace-") {
		t.Fatalf("workspace volume %q missing scope", workspace)
	}
}

func TestJupyterArgsUseSandboxPaths(t *testing.T) {
	joined := "\x00" + strings.Join(jupyterArgs(), "\x00") + "\x00"
	for _, want := range []string{
		"\x00lab\x00",
		"\x00--ip=0.0.0.0\x00",
		"\x00--port=8888\x00",
		"\x00--IdentityProvider.token=\x00",
		"\x00--ServerApp.root_dir=" + containerWorkspace + "\x00",
		"\x00--ServerApp.base_url=/shellcn-notebook/\x00",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("Jupyter args missing %q in %#v", want, jupyterArgs())
		}
	}
}

func TestNotebookPrepareScriptFixesVolumeOwnership(t *testing.T) {
	for _, want := range []string{
		"mkdir -p /home/jovyan/work /home/jovyan/.jupyter /home/jovyan/.local/share/jupyter/runtime /home/jovyan/.ipython",
		"chown -R 1000:1000 /home/jovyan/. /home/jovyan/work/.",
		"chmod -R u+rwX,g+rwX /home/jovyan/. /home/jovyan/work/.",
	} {
		if !strings.Contains(notebookPrepareScript, want) {
			t.Fatalf("notebook prep script missing %q:\n%s", want, notebookPrepareScript)
		}
	}
}

func TestNotebookProbeScriptChecksWritableDirs(t *testing.T) {
	for _, want := range []string{
		`mkdir -p "$JUPYTER_CONFIG_DIR" "$JUPYTER_DATA_DIR" "$JUPYTER_RUNTIME_DIR" "$IPYTHONDIR" /home/jovyan/work/.shellcn-probe`,
		`touch "$JUPYTER_CONFIG_DIR/.shellcn-probe"`,
		`touch "$JUPYTER_DATA_DIR/.shellcn-probe"`,
		`touch "$JUPYTER_RUNTIME_DIR/.shellcn-probe"`,
		`touch "$IPYTHONDIR/.shellcn-probe"`,
		"touch /home/jovyan/work/.shellcn-probe/file",
	} {
		if !strings.Contains(notebookProbeScript, want) {
			t.Fatalf("notebook probe script missing %q:\n%s", want, notebookProbeScript)
		}
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

func TestServeProxyStripsAltSvc(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/shellcn-notebook/api/status" {
			t.Fatalf("upstream path = %q, want /shellcn-notebook/api/status", r.URL.Path)
		}
		if got, want := r.Header.Get("Origin"), upstreamOrigin(r); got != want {
			t.Fatalf("Origin = %q, want %q", got, want)
		}
		w.Header().Set("Alt-Svc", `h2=":443"`)
		w.Header().Set("Alt-Used", "upstream.local")
		_, _ = io.WriteString(w, "ok")
	}))
	defer upstream.Close()
	base, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatalf("parse upstream URL: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/connections/c1/proxy/api/status", nil)
	req.Header.Set(plugin.ProxyPrefixHeader, "/api/connections/c1/proxy")
	req.Header.Set("Origin", "http://gateway.local")
	rec := httptest.NewRecorder()

	serveProxy(rec, req, base, http.DefaultTransport)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Alt-Svc"); got != "clear" {
		t.Fatalf("Alt-Svc = %q, want clear", got)
	}
	if got := rec.Header().Get("Alt-Used"); got != "" {
		t.Fatalf("Alt-Used leaked through proxy: %q", got)
	}
}

func upstreamOrigin(r *http.Request) string {
	return "http://" + r.Host
}

func TestServeProxyRewritesNotebookHTMLWithoutShim(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/shellcn-notebook/lab" {
			t.Fatalf("upstream path = %q, want /shellcn-notebook/lab", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Location", "/shellcn-notebook/lab")
		w.Header().Add("Set-Cookie", "sid=abc; Path=/shellcn-notebook; Domain=upstream.local")
		_, _ = io.WriteString(w, `<html><head><script id="jupyter-config-data" type="application/json">{"baseUrl": "/shellcn-notebook/", "settingsUrl": "/lab/api/settings", "staticUrl": "/static/lab"}</script></head><body data-base="/shellcn-notebook/">ok</body></html>`)
	}))
	defer upstream.Close()
	base, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatalf("parse upstream URL: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/connections/c1/proxy/lab", nil)
	req.Host = "gateway.local"
	req.Header.Set(plugin.ProxyPrefixHeader, "/api/connections/c1/proxy")
	rec := httptest.NewRecorder()

	serveProxy(rec, req, base, http.DefaultTransport)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Location"); got != "/api/connections/c1/proxy/lab" {
		t.Fatalf("Location = %q", got)
	}
	if got := rec.Header().Values("Set-Cookie"); len(got) != 1 || !strings.Contains(got[0], "Path=/api/connections/c1/proxy") || strings.Contains(strings.ToLower(got[0]), "domain=") {
		t.Fatalf("Set-Cookie was not scoped to proxy prefix: %+v", got)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `data-base="/api/connections/c1/proxy/"`) {
		t.Fatalf("body was not rewritten under proxy prefix: %s", body)
	}
	for _, want := range []string{
		`"baseUrl": "/api/connections/c1/proxy/`,
		`"settingsUrl": "/lab/api/settings"`,
		`"staticUrl": "/static/lab"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("body missing rewritten URL %q: %s", want, body)
		}
	}
	if strings.Contains(body, "window.fetch") || strings.Contains(body, "__shellcn_sw") {
		t.Fatalf("notebook proxy should not inject the generic webproxy shim: %s", body)
	}
}

func TestServeProxyRewritesNotebookJSONAssets(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/shellcn-notebook/api/kernelspecs" {
			t.Fatalf("upstream path = %q, want /shellcn-notebook/api/kernelspecs", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"kernelspecs":{"python3":{"resources":{"logo-svg":"/shellcn-notebook/kernelspecs/python3/logo-svg.svg"}}}}`)
	}))
	defer upstream.Close()
	base, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatalf("parse upstream URL: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/connections/c1/proxy/api/kernelspecs", nil)
	req.Header.Set(plugin.ProxyPrefixHeader, "/api/connections/c1/proxy")
	rec := httptest.NewRecorder()

	serveProxy(rec, req, base, http.DefaultTransport)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"/api/connections/c1/proxy/kernelspecs/python3/logo-svg.svg"`) {
		t.Fatalf("JSON asset URL was not rewritten under proxy prefix: %s", body)
	}
	if strings.Contains(body, notebookSourcePrefix) {
		t.Fatalf("JSON leaked notebook source prefix: %s", body)
	}
}

func TestServeProxyServesLegacyWorkerCleanup(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/connections/c1/proxy/__shellcn_sw.js", nil)
	req.Header.Set(plugin.ProxyPrefixHeader, "/api/connections/c1/proxy")
	rec := httptest.NewRecorder()

	serveProxy(rec, req, &url.URL{Scheme: "http", Host: "127.0.0.1:1"}, http.DefaultTransport)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Service-Worker-Allowed"); got != "/api/connections/c1/proxy/" {
		t.Fatalf("Service-Worker-Allowed = %q", got)
	}
	if !strings.Contains(rec.Body.String(), "unregister") {
		t.Fatalf("legacy worker cleanup did not unregister: %s", rec.Body.String())
	}
}

func TestStartSessionIntegration(t *testing.T) {
	_, err := newDockerRuntime(plugin.ConnectConfig{Transport: plugin.TransportAgent})
	if err == nil || !errors.Is(err, plugin.ErrUnavailable) {
		t.Fatalf("expected missing agent net error, got %v", err)
	}
}

type failingNet struct{}

func (failingNet) DialContext(context.Context, string, string) (net.Conn, error) {
	return nil, errors.New("dial disabled")
}

func (failingNet) HTTP() (string, http.RoundTripper, bool) {
	return "", nil, false
}
