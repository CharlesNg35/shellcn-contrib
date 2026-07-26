package memcached

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math"
	"math/big"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	contribtest "github.com/charlesng35/shellcn-contrib/shared/plugintest"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugin/canvas"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestManifestValidates(t *testing.T) {
	p := New()
	plugintest.ValidatePlugin(t, p)
	plugintest.ValidateProjectionPanelConfigs(t, plugintest.Projection(t, p))

	m := p.Manifest()
	if m.Name != protocolName {
		t.Fatalf("unexpected plugin name %q", m.Name)
	}
	if m.Agent == nil || len(m.SupportedTransports) != 2 {
		t.Fatalf("memcached should offer direct and agent transports: %+v", m.SupportedTransports)
	}
	if !plugintest.CredentialKindSupported(m.Config, plugin.CredentialKindDBPassword) {
		t.Fatal("database password credential should support memcached")
	}
	if !plugintest.CredentialKindSupported(m.Config, plugin.CredentialKindTLSClientCert) {
		t.Fatal("TLS client certificate credential should support memcached")
	}
}

func TestEveryRouteIsReachableFromTheManifest(t *testing.T) {
	p := New()
	referenced := map[string]bool{}
	for _, action := range p.Manifest().Actions {
		referenced[action.RouteID] = true
	}
	for _, stream := range p.Manifest().Streams {
		referenced[stream.RouteID] = true
	}
	var walk func(panels []plugin.Panel)
	walk = func(panels []plugin.Panel) {
		for _, panel := range panels {
			if panel.Source != nil {
				referenced[panel.Source.RouteID] = true
			}
			switch cfg := panel.Config.(type) {
			case plugin.TableConfig:
				if cfg.Watch != nil {
					referenced[cfg.Watch.RouteID] = true
				}
			case plugin.KVConfig:
				referenced[cfg.CreateRouteID] = true
				referenced[cfg.ReadRouteID] = true
				referenced[cfg.WriteRouteID] = true
				referenced[cfg.DeleteRouteID] = true
			case plugin.ObjectDetailConfig:
				if cfg.Watch != nil {
					referenced[cfg.Watch.RouteID] = true
				}
			case plugin.TimelineConfig:
				if cfg.Watch != nil {
					referenced[cfg.Watch.RouteID] = true
				}
			case plugin.QueryEditorConfig:
				referenced[cfg.CompletionRouteID] = true
			case plugin.DashboardConfig:
				walk(cfg.Cells)
			case plugin.SplitConfig:
				for _, child := range cfg.Panels {
					walk([]plugin.Panel{child.Panel})
				}
			}
		}
	}
	walk(p.Manifest().Tabs)
	for _, scope := range p.Manifest().Scope {
		if scope.OptionsSource != nil {
			referenced[scope.OptionsSource.RouteID] = true
		}
	}
	for _, route := range p.Routes() {
		if !referenced[route.ID] {
			t.Errorf("route %s is not referenced by any panel, action, stream, or scope", route.ID)
		}
	}
}

func TestParseOptionsDefaults(t *testing.T) {
	opts, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{"host": "cache.internal"}})
	if err != nil {
		t.Fatalf("parse options: %v", err)
	}
	if opts.Port != defaultPort || opts.Username != "" || opts.TLSMode != "disable" {
		t.Fatalf("unexpected defaults: %+v", opts)
	}
	if !opts.ReadOnly {
		t.Fatal("read-only must default on")
	}
	if opts.ScanLimit != defaultScanLimit || opts.PageLimit != defaultPageLimit {
		t.Fatalf("unexpected scan limits: %+v", opts)
	}
	if strings.Join(opts.WatchStreams, ",") != "mutations,evictions,deletions,connevents" {
		t.Fatalf("unexpected watcher feeds: %v", opts.WatchStreams)
	}
}

func TestParseOptionsAppliesStoredCredentialAndClientCert(t *testing.T) {
	certPEM, keyPEM := selfSignedPEM(t)
	opts, err := parseOptions(plugin.ConnectConfig{
		Config: map[string]any{"host": "cache.internal", "auth": authCredential, "tls_mode": "verify-full", "ca_certificate": certPEM},
		Credentials: plugin.NewResolvedCredentials(
			plugin.CredentialBinding{
				Field:      plugin.CredentialRefField,
				Credential: plugin.ResolvedCredential{Values: map[string]string{"username": "svc", "password": "s3cret"}},
			},
			plugin.CredentialBinding{
				Field:      clientCertField,
				Credential: plugin.ResolvedCredential{Values: map[string]string{"certificate": certPEM, "private_key": keyPEM}},
			},
		),
	})
	if err != nil {
		t.Fatalf("parse options: %v", err)
	}
	if opts.Username != "svc" || opts.Password != "s3cret" {
		t.Fatalf("stored credential not applied: %+v", opts)
	}
	if opts.TLSMode != "verify-full" || !strings.Contains(opts.ClientCertificate, "PRIVATE KEY") {
		t.Fatalf("TLS material not applied: %+v", opts)
	}
	tlsCfg, err := tlsConfig(opts)
	if err != nil {
		t.Fatalf("tls config: %v", err)
	}
	if len(tlsCfg.Certificates) != 1 || tlsCfg.RootCAs == nil || tlsCfg.ServerName != "cache.internal" {
		t.Fatalf("unexpected TLS config: %+v", tlsCfg)
	}
}

// selfSignedPEM builds a throwaway certificate so the TLS wiring is exercised
// with material the standard library actually accepts.
func selfSignedPEM(t *testing.T) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "shellcn-memcached-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		IsCA:         true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
}

func TestParseOptionsRejectsBadInput(t *testing.T) {
	cases := map[string]map[string]any{
		"missing host":     {},
		"bad port":         {"host": "h", "port": 70000},
		"unknown auth":     {"host": "h", "auth": "kerberos"},
		"partial token":    {"host": "h", "auth": authPassword, "username": "u"},
		"spaced token":     {"host": "h", "auth": authPassword, "username": "u u", "password": "p"},
		"bad tls mode":     {"host": "h", "tls_mode": "mutual"},
		"invalid ca chain": {"host": "h", "tls_mode": "verify-ca", "ca_certificate": "not-pem"},
	}
	for name, cfg := range cases {
		if _, err := parseOptions(plugin.ConnectConfig{Config: cfg}); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestConfigSchemaHidesSecretsUntilTheAuthModeSelectsThem(t *testing.T) {
	schema := configSchema()
	direct := map[string]any{plugin.SchemaContextTransport: string(plugin.TransportDirect)}
	none := schema.VisibleSecretKeys(map[string]any{"auth": authNone, "tls_mode": "disable"}, direct)
	if containsString(none, "password") {
		t.Fatalf("the inline password must stay hidden when authentication is disabled: %v", none)
	}
	withPassword := schema.VisibleSecretKeys(map[string]any{"auth": authPassword, "tls_mode": "disable"}, direct)
	if !containsString(withPassword, "password") {
		t.Fatalf("password should be a visible secret in password mode: %v", withPassword)
	}
	visible := schema.VisibleValues(map[string]any{
		"auth": authCredential, "tls_mode": "verify-ca",
		credentialIDField: "cred-1", "password": "inline",
	}, direct)
	if _, ok := visible[credentialIDField]; !ok {
		t.Fatal("the credential reference must be visible in stored-password mode")
	}
	if _, ok := visible["password"]; ok {
		t.Fatal("the inline password must be hidden in stored-password mode")
	}
	if err := schema.ValidateValues(map[string]any{"host": "h", "port": 11211, "auth": authNone, "tls_mode": "disable"}, nil); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if err := schema.ValidateValues(map[string]any{"port": 11211, "auth": authNone, "tls_mode": "disable"}, nil); err == nil {
		t.Fatal("a config without a host must be rejected")
	}
	if err := schema.ValidateValues(map[string]any{"host": "h", "port": 0, "auth": authNone, "tls_mode": "disable"}, nil); err == nil {
		t.Fatal("port 0 must be rejected by the schema validators")
	}
	if err := schema.ValidateValues(map[string]any{"host": "h", "port": 11211, "auth": "kerberos", "tls_mode": "disable"}, nil); err == nil {
		t.Fatal("an unknown authentication mode must be rejected by the schema options")
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestResolveCommandClassifiesAndFramesCommands(t *testing.T) {
	spec, payload, err := resolveCommand("stats slabs")
	if err != nil {
		t.Fatalf("stats slabs: %v", err)
	}
	if spec.Class != classRead || spec.Reply != replyStats || payload != "stats slabs\r\n" {
		t.Fatalf("unexpected spec/payload: %+v %q", spec, payload)
	}
	if spec, _, err := resolveCommand("stats reset"); err != nil || spec.Class != classAdmin {
		t.Fatalf("stats reset should be administrative: %+v %v", spec, err)
	}
	if _, payload, err := resolveCommand("set foo 0 0 3\nbar"); err != nil || payload != "set foo 0 0 3\r\nbar\r\n" {
		t.Fatalf("storage framing wrong: %q %v", payload, err)
	}
	for _, bad := range []string{"", "watch mutations", "shutdown", "quit", "set foo 0 0 5\nbar", "set foo 0 0", "delete foo noreply", "get foo\nextra"} {
		if _, _, err := resolveCommand(bad); err == nil {
			t.Errorf("%q should be rejected", bad)
		}
	}
}

func TestParseActivityLine(t *testing.T) {
	event, ok := parseActivityLine("ts=1700000000.123456 gid=7 type=eviction key=user%3A42 status=hit clsid=3")
	if !ok {
		t.Fatal("watcher line should parse")
	}
	if event.Reason != "eviction" || event.GID != 7 || event.Severity != plugin.SeverityWarn {
		t.Fatalf("unexpected event: %+v", event)
	}
	if event.Resource != "user:42" {
		t.Fatalf("URI escaping not decoded: %q", event.Resource)
	}
	if !strings.HasPrefix(event.Time, "2023-11-14T") {
		t.Fatalf("unexpected timestamp: %q", event.Time)
	}
	if !strings.Contains(event.Message, "clsid=3") {
		t.Fatalf("message should keep the remaining fields: %q", event.Message)
	}
	if _, ok := parseActivityLine("OK"); ok {
		t.Fatal("a non key=value line should not become an event")
	}
}

func TestBuildSlabRowsComputesChunkSlack(t *testing.T) {
	rows := buildSlabRows(
		map[int]map[string]string{
			1: {"chunk_size": "96", "chunks_per_page": "10922", "total_pages": "1", "total_chunks": "10922", "used_chunks": "10", "free_chunks": "10912"},
		},
		map[int]map[string]string{
			1: {"number": "10", "mem_requested": "600", "age": "12", "evicted": "2"},
		},
	)
	if len(rows) != 1 {
		t.Fatalf("expected one slab row, got %d", len(rows))
	}
	row := rows[0]
	if row.AllocatedBytes != 96*10922 || row.UsedBytes != 960 {
		t.Fatalf("unexpected byte totals: %+v", row)
	}
	if row.WastedBytes != 360 || row.WastedPct != 37.5 {
		t.Fatalf("chunk slack miscalculated: %+v", row)
	}
	if row.Items != 10 || row.Evicted != 2 || row.Age != 12 {
		t.Fatalf("item statistics not joined: %+v", row)
	}
}

func TestBuildOverviewDerivesRatiosAndCapacity(t *testing.T) {
	out := buildOverview(
		options{Host: "cache", Port: 11211, ReadOnly: true},
		map[string]string{
			"version": "1.6.45", "uptime": "120", "curr_items": "3", "bytes": "1000",
			"limit_maxbytes": "4000", "curr_connections": "5", "get_hits": "30", "get_misses": "10",
		},
		map[string]string{"maxconns": "10", "lru_crawler": "yes", "cas_enabled": "1"},
		map[string]string{"active_slabs": "2", "total_malloced": "2048"},
		map[int]map[string]string{1: {"mem_requested": "700"}},
	)
	if out.Address != "cache:11211" || out.Version != "1.6.45" {
		t.Fatalf("unexpected identity: %+v", out)
	}
	if out.MemoryPct != 25 || out.ConnectionsPct != 50 || out.HitRatio != 75 {
		t.Fatalf("unexpected ratios: %+v", out)
	}
	if out.WastedBytes != 300 || out.WastedPct != 30 {
		t.Fatalf("unexpected slack: %+v", out)
	}
	if !out.LRUCrawlerEnabled || !out.CASEnabled || !out.ReadOnly {
		t.Fatalf("unexpected flags: %+v", out)
	}
}

func TestMetricFrameDerivesRates(t *testing.T) {
	overview := serverOverview{CurrItems: 4, GetHits: 30, GetMisses: 10, CmdGet: 40, CmdSet: 10, HitRatio: 75}
	prev := metricSample{At: time.Now().Add(-10 * time.Second), CmdGet: 20, CmdSet: 5, GetHits: 15, GetMisses: 5}
	frame := metricFrame(overview, prev, sampleFrom(overview))
	if frame["getsPerSec"].(float64) != 2 || frame["setsPerSec"].(float64) != 0.5 {
		t.Fatalf("unexpected rates: %+v", frame)
	}
	if frame["liveHitRatio"].(float64) != 75 {
		t.Fatalf("unexpected windowed hit ratio: %+v", frame)
	}
	if _, ok := metricFrame(overview, metricSample{}, sampleFrom(overview))["getsPerSec"]; ok {
		t.Fatal("the first frame cannot report a rate")
	}
}

func TestWatchersReportAnUnreachableServerAndRecover(t *testing.T) {
	overview := serverOverview{Address: "cache:11211", Status: statusOK, CurrItems: 4, CmdGet: 40}
	unreachable := errors.New("dial tcp: connection refused")

	var metrics metricsWatcher
	if metrics.frame(overview, nil)["metricsAvailable"] != true {
		t.Fatal("a healthy poll must mark the metrics feed available")
	}
	down := metrics.frame(serverOverview{}, unreachable)
	if down["metricsAvailable"] != false || !strings.Contains(down["message"].(string), unreachable.Error()) {
		t.Fatalf("a failed poll must report the feed as unavailable: %+v", down)
	}
	if _, ok := down["currItems"]; ok {
		t.Fatalf("an unavailable frame must not repeat stale values: %+v", down)
	}
	if back := metrics.frame(overview, nil); back["metricsAvailable"] != true || back["currItems"] != int64(4) {
		t.Fatalf("the feed must recover once the server answers again: %+v", back)
	}

	watcher := newOverviewWatcher("cache:11211")
	up := watcher.event(overview, nil)
	if up.Type != "modified" || up.Ref.UID != "cache:11211" {
		t.Fatalf("the watch stream must emit identified resource events: %+v", up)
	}
	if up.Resource.(serverOverview).Status != statusOK {
		t.Fatalf("a healthy snapshot must report ok: %+v", up.Resource)
	}
	lost := watcher.event(serverOverview{}, unreachable).Resource.(serverOverview)
	if lost.Status != statusUnreachable || lost.StatusError != unreachable.Error() {
		t.Fatalf("a failed poll must be pushed as unreachable: %+v", lost)
	}
	if lost.Address != "cache:11211" || lost.CurrItems != 4 {
		t.Fatalf("an unreachable snapshot keeps the last known values: %+v", lost)
	}
	recovered := watcher.event(overview, nil).Resource.(serverOverview)
	if recovered.Status != statusOK || recovered.StatusError != "" {
		t.Fatalf("the snapshot must clear the failure once the server answers: %+v", recovered)
	}
}

func TestWatchStreamsKeepEmittingWhenPollsFail(t *testing.T) {
	// The fake server only answers "version", so every stats poll fails the way an
	// unreachable server does once the session is already open.
	s := fakeSession(t, map[string]string{"version": "VERSION 1.6.45\r\n"}, nil)
	h := contribtest.NewHarness(t, New().Routes())

	metricsCtx, cancelMetrics := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancelMetrics()
	metrics := decodeFrames(t, h.Stream(metricsCtx, "memcached.metrics", s, nil, nil, nil))
	if len(metrics) == 0 {
		t.Fatal("a failed poll must still push a metrics frame")
	}
	if metrics[0]["metricsAvailable"] != false || metrics[0]["message"] == nil {
		t.Fatalf("the metrics panel must be told the feed is unavailable: %+v", metrics[0])
	}

	watchCtx, cancelWatch := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancelWatch()
	frames := decodeFrames(t, h.Stream(watchCtx, "memcached.overview.watch", s, nil, nil, nil))
	if len(frames) == 0 {
		t.Fatal("a failed poll must still push an overview snapshot")
	}
	resource, ok := frames[0]["resource"].(map[string]any)
	if !ok {
		t.Fatalf("overview watch frames must be resource events: %+v", frames[0])
	}
	if resource["status"] != statusUnreachable || resource["statusError"] == nil {
		t.Fatalf("the property sheet must be told the server is unreachable: %+v", resource)
	}
	if resource["address"] != s.opts.address() {
		t.Fatalf("an unreachable snapshot must still identify the server: %+v", resource)
	}
}

func TestEncodeAndDecodeValueRoundTrip(t *testing.T) {
	kind, encoded := encodeValue([]byte(`{"a":1}`))
	if kind != valueJSON {
		t.Fatalf("expected JSON detection, got %q", kind)
	}
	decoded, err := decodeValue(kind, encoded)
	if err != nil || string(decoded) != `{"a":1}` {
		t.Fatalf("json round trip failed: %q %v", decoded, err)
	}
	kind, encoded = encodeValue([]byte{0xff, 0xfe})
	if kind != valueBinary {
		t.Fatalf("expected binary detection, got %q", kind)
	}
	decoded, err = decodeValue(kind, encoded)
	if err != nil || len(decoded) != 2 || decoded[0] != 0xff {
		t.Fatalf("binary round trip failed: %v %v", decoded, err)
	}
	if _, err := decodeValue(valueJSON, "{oops"); err == nil {
		t.Fatal("invalid JSON should be rejected")
	}
	if _, err := decodeValue(valueBinary, "!!!!"); err == nil {
		t.Fatal("invalid base64 should be rejected")
	}
}

func TestValidateKeyRejectsProtocolBreakers(t *testing.T) {
	if err := validateKey("ok:key"); err != nil {
		t.Fatalf("valid key rejected: %v", err)
	}
	for _, bad := range []string{"", "with space", "with\nnewline", strings.Repeat("k", 251)} {
		if err := validateKey(bad); err == nil {
			t.Errorf("%q should be rejected", bad)
		}
	}
}

func TestSlabSceneSelectionAndFrame(t *testing.T) {
	scene := newSlabScene()
	scene.handle(&canvas.ReadyEvent{Width: 800, Height: 600, Theme: plugin.PanelThemeLight})
	if !scene.setRows([]slabRow{
		{Class: 1, ChunkSize: 96, TotalChunks: 100, UsedChunks: 40, AllocatedBytes: 9600, UsedBytes: 3840, MemRequested: 2400, WastedBytes: 1440, WastedPct: 37.5, FillPct: 40},
		{Class: 2, ChunkSize: 120, TotalChunks: 80, UsedChunks: 10, AllocatedBytes: 9600, UsedBytes: 1200, MemRequested: 1000, WastedBytes: 200, WastedPct: 16.6, FillPct: 12.5},
	}, nil) {
		t.Fatal("new slab data should mark the scene dirty")
	}
	if scene.setRows(scene.rows, nil) {
		t.Fatal("identical slab data should not redraw")
	}
	if !scene.handle(&canvas.KeyEvent{Event: "keydown", Key: "ArrowDown"}) || scene.selected != 1 {
		t.Fatalf("arrow key should move the selection, got %d", scene.selected)
	}
	if !scene.handle(&canvas.PointerEvent{Event: canvas.PointerDown, RegionID: "class:0"}) || scene.selected != 0 {
		t.Fatalf("pointer should select a class region, got %d", scene.selected)
	}
	if scene.handle(&canvas.PointerEvent{Event: canvas.PointerDown}) {
		t.Fatal("a pointer event without a region must be ignored")
	}
	frame := scene.frame()
	if len(frame.Regions) != 2 {
		t.Fatalf("expected one region per visible class, got %d", len(frame.Regions))
	}
	for _, region := range frame.Regions {
		if region.Label == "" || region.ID == "" {
			t.Fatalf("canvas regions must stay labelled: %+v", region)
		}
	}
	raw, err := json.Marshal(frame)
	if err != nil {
		t.Fatalf("frame must encode: %v", err)
	}
	if !strings.Contains(string(raw), `"type":"clear"`) {
		t.Fatalf("frame should start with a clear command: %s", raw)
	}
}

func TestSlabSceneRendersErrorAndEmptyStates(t *testing.T) {
	scene := newSlabScene()
	scene.setRows(nil, nil)
	raw, _ := json.Marshal(scene.frame())
	if !strings.Contains(string(raw), "No slab classes are allocated yet") {
		t.Fatalf("empty state missing: %s", raw)
	}
	scene.setRows(nil, context.DeadlineExceeded)
	raw, _ = json.Marshal(scene.frame())
	if !strings.Contains(string(raw), "Slab statistics unavailable") {
		t.Fatalf("error state missing: %s", raw)
	}
}

// fakeServer speaks just enough of the memcached ASCII protocol to drive the
// handlers through a fake transport.
type fakeServer struct {
	listener net.Listener
	replies  map[string]string
}

func newFakeServer(t *testing.T, replies map[string]string) *fakeServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := &fakeServer{listener: listener, replies: replies}
	go server.serve()
	t.Cleanup(func() { _ = listener.Close() })
	return server
}

func (f *fakeServer) serve() {
	for {
		conn, err := f.listener.Accept()
		if err != nil {
			return
		}
		go f.handle(conn)
	}
}

func (f *fakeServer) handle(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		command := strings.TrimRight(line, "\r\n")
		reply, ok := f.replies[command]
		if !ok {
			reply = "ERROR\r\n"
		}
		if _, err := conn.Write([]byte(reply)); err != nil {
			return
		}
	}
}

func (f *fakeServer) transport() plugin.NetTransport {
	addr := f.listener.Addr().String()
	return plugintest.TransportFunc(func(ctx context.Context, network, _ string) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, network, addr)
	})
}

func fakeSession(t *testing.T, replies map[string]string, config map[string]any) *Session {
	t.Helper()
	server := newFakeServer(t, replies)
	host, port, err := net.SplitHostPort(server.listener.Addr().String())
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil {
		t.Fatalf("parse port %q: %v", port, err)
	}
	cfg := map[string]any{"host": host, "port": portNumber, "auth": authNone, "tls_mode": "disable", "read_only": false}
	for key, value := range config {
		cfg[key] = value
	}
	sess, err := connect(context.Background(), plugin.ConnectConfig{Config: cfg, Net: server.transport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = sess.Close() })
	return sess.(*Session)
}

func TestHandlersAgainstAFakeServer(t *testing.T) {
	replies := map[string]string{
		"version":        "VERSION 1.6.45\r\n",
		"stats":          "STAT version 1.6.45\r\nSTAT uptime 90\r\nSTAT curr_items 2\r\nSTAT bytes 200\r\nSTAT limit_maxbytes 1000\r\nSTAT curr_connections 3\r\nSTAT get_hits 8\r\nSTAT get_misses 2\r\nEND\r\n",
		"stats settings": "STAT maxconns 20\r\nSTAT lru_crawler yes\r\nSTAT item_size_max 1048576\r\nEND\r\n",
		"stats slabs":    "STAT 1:chunk_size 96\r\nSTAT 1:chunks_per_page 10922\r\nSTAT 1:total_pages 1\r\nSTAT 1:total_chunks 10922\r\nSTAT 1:used_chunks 2\r\nSTAT 1:free_chunks 10920\r\nSTAT active_slabs 1\r\nSTAT total_malloced 1048576\r\nEND\r\n",
		"stats items":    "STAT items:1:number 2\r\nSTAT items:1:age 5\r\nSTAT items:1:mem_requested 120\r\nSTAT items:1:evicted 0\r\nEND\r\n",
		"stats conns":    "STAT 26:addr 127.0.0.1:52000\r\nSTAT 26:state conn_new_cmd\r\nSTAT 26:secs_since_last_cmd 1\r\nEND\r\n",
		"lru_crawler metadump all": "key=alpha exp=-1 la=1700000000 cas=1 fetch=no cls=1 size=70\r\n" +
			"key=beta%3Aone exp=1900000000 la=1700000001 cas=2 fetch=yes cls=1 size=80\r\nEND\r\n",
		"flush_all 0": "OK\r\n",
	}
	s := fakeSession(t, replies, nil)
	h := contribtest.NewHarness(t, New().Routes())
	ctx := context.Background()

	overview := h.Call(ctx, "memcached.overview", s, nil, nil, nil).(serverOverview)
	if overview.Version != "1.6.45" || overview.MemoryPct != 20 || overview.HitRatio != 80 {
		t.Fatalf("unexpected overview: %+v", overview)
	}

	slabs := pageRowItems(t, h.Call(ctx, "memcached.slabs.list", s, nil, nil, nil))
	if len(slabs) != 1 || slabs[0]["chunkSize"].(float64) != 96 {
		t.Fatalf("unexpected slab rows: %+v", slabs)
	}
	scoped := pageRowItems(t, h.Call(ctx, "memcached.slabs.list", s, map[string]string{classScopeParam: "9"}, nil, nil))
	if len(scoped) != 0 {
		t.Fatalf("the slab class scope should filter rows: %+v", scoped)
	}
	if _, err := h.Route("memcached.slabs.list").Handle(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{classScopeParam: "abc"}, nil, nil)); err == nil {
		t.Fatal("a non-numeric slab class must be rejected")
	}

	items := pageRowItems(t, h.Call(ctx, "memcached.items.list", s, nil, nil, nil))
	if len(items) != 1 || items[0]["number"].(float64) != 2 {
		t.Fatalf("unexpected LRU rows: %+v", items)
	}
	conns := pageRowItems(t, h.Call(ctx, "memcached.conns.list", s, nil, nil, nil))
	if len(conns) != 1 || conns[0]["state"] != "conn_new_cmd" {
		t.Fatalf("unexpected connection rows: %+v", conns)
	}
	settings := pageRowItems(t, h.Call(ctx, "memcached.settings.list", s, nil, nil, nil))
	if len(settings) != 3 {
		t.Fatalf("unexpected settings rows: %+v", settings)
	}

	keys := pageRowItems(t, h.Call(ctx, "memcached.keys.list", s, nil, nil, nil))
	if len(keys) != 2 || keys[0]["key"] != "alpha" || keys[1]["key"] != "beta:one" {
		t.Fatalf("unexpected key rows: %+v", keys)
	}
	if keys[0]["ttl"].(float64) != -1 {
		t.Fatalf("an item without an expiry should report ttl -1: %+v", keys[0])
	}
	filtered := pageRowItems(t, h.Call(ctx, "memcached.keys.list", s, nil, url.Values{"filter": []string{"beta"}}, nil))
	if len(filtered) != 1 || filtered[0]["key"] != "beta:one" {
		t.Fatalf("key search not applied: %+v", filtered)
	}

	options := h.Call(ctx, "memcached.slabs.options", s, nil, nil, nil).(plugin.Page[plugin.FilterOption])
	if len(options.Items) != 2 || options.Items[0].Value != classScopeAll {
		t.Fatalf("unexpected scope options: %+v", options.Items)
	}

	completions := h.Call(ctx, "memcached.console.complete", s, nil, nil, nil).(plugin.Page[completionEntry])
	if len(completions.Items) < len(commandCatalog) {
		t.Fatalf("completion list should cover the catalog: %d", len(completions.Items))
	}

	flushed := h.Call(ctx, "memcached.server.flush", s, nil, nil, []byte(`{"delay":0}`)).(actionResult)
	if !flushed.OK || flushed.Message != "OK" {
		t.Fatalf("unexpected flush result: %+v", flushed)
	}
}

func TestReadOnlySessionBlocksEveryWriteRoute(t *testing.T) {
	s := fakeSession(t, map[string]string{"version": "VERSION 1.6.45\r\n"}, map[string]any{"read_only": true})
	routes := plugintest.RouteMap(New().Routes())
	ctx := context.Background()
	blocked := []struct {
		id   string
		body string
	}{
		{"memcached.key.write", `{"value":"x"}`},
		{"memcached.key.delete", ``},
		{"memcached.key.store", `{"key":"k","value":"x"}`},
		{"memcached.key.touch", `{"key":"k","ttl":10}`},
		{"memcached.key.incr", `{"key":"k","delta":1}`},
		{"memcached.key.decr", `{"key":"k","delta":1}`},
		{"memcached.server.flush", `{"delay":0}`},
		{"memcached.server.stats_reset", ``},
		{"memcached.server.memlimit", `{"megabytes":64}`},
		{"memcached.server.verbosity", `{"level":1}`},
		{"memcached.server.crawl", `{"classes":"all"}`},
	}
	for _, tc := range blocked {
		rc := plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"key": "k"}, nil, []byte(tc.body))
		if _, err := routes[tc.id].Handle(rc); err == nil {
			t.Errorf("%s must be blocked in read-only mode", tc.id)
		}
	}
}

func TestConsoleStreamHonoursConfirmationAndReadOnly(t *testing.T) {
	replies := map[string]string{
		"version":     "VERSION 1.6.45\r\n",
		"stats":       "STAT uptime 5\r\nEND\r\n",
		"flush_all 0": "OK\r\n",
	}
	s := fakeSession(t, replies, map[string]any{"read_only": true})
	h := contribtest.NewHarness(t, New().Routes())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	out := h.Stream(ctx, "memcached.console", s, nil, nil, []byte(
		"{\"query\":\"stats\"}\n"+
			"{\"query\":\"flush_all 0\"}\n"+
			"{\"query\":\"nonsense\"}\n"+
			"{\"query\":\"  \"}\n"))
	frames := decodeFrames(t, out)
	if len(frames) != 4 {
		t.Fatalf("expected one frame per submission, got %d: %s", len(frames), out)
	}
	if frames[0]["rowCount"].(float64) != 1 {
		t.Fatalf("stats should return rows: %+v", frames[0])
	}
	if _, ok := frames[1]["error"]; !ok {
		t.Fatalf("read-only mode must refuse flush_all before asking for confirmation: %+v", frames[1])
	}
	if _, ok := frames[2]["error"]; !ok {
		t.Fatalf("an unknown command should return an error frame: %+v", frames[2])
	}
	if _, ok := frames[3]["error"]; !ok {
		t.Fatalf("an empty command should return an error frame: %+v", frames[3])
	}

	writable := fakeSession(t, replies, nil)
	out = h.Stream(ctx, "memcached.console", writable, nil, nil, []byte(
		"{\"query\":\"flush_all 0\"}\n{\"query\":\"flush_all 0\",\"confirm\":true}\n"))
	frames = decodeFrames(t, out)
	if len(frames) != 2 {
		t.Fatalf("expected two frames, got %d: %s", len(frames), out)
	}
	if frames[0]["confirmRequired"] != true {
		t.Fatalf("flush_all should demand confirmation first: %+v", frames[0])
	}
	if frames[1]["message"] != "OK" {
		t.Fatalf("confirmed flush_all should execute: %+v", frames[1])
	}
}

func TestSavedCommandsUseScopedStorage(t *testing.T) {
	s := fakeSession(t, map[string]string{"version": "VERSION 1.6.45\r\n"}, nil)
	store := newMemoryStorage()
	routes := plugintest.RouteMap(New().Routes())
	ctx := context.Background()

	rc := plugin.NewRequestContext(ctx, plugin.User{}, s, nil, nil, []byte(`{"name":"Slab report","command":"stats slabs"}`)).WithStorage(store)
	saved, err := routes["memcached.commands.save"].Handle(rc)
	if err != nil {
		t.Fatalf("save command: %v", err)
	}
	record := saved.(savedCommand)
	if record.ID != "slab-report" {
		t.Fatalf("unexpected storage key: %+v", record)
	}
	if store.scope.Collection != savedCommandsBucket {
		t.Fatalf("saved commands must live in their own collection: %+v", store.scope)
	}

	listed, err := routes["memcached.commands.list"].Handle(plugin.NewRequestContext(ctx, plugin.User{}, s, nil, nil, nil).WithStorage(store))
	if err != nil {
		t.Fatalf("list commands: %v", err)
	}
	page := listed.(plugin.Page[savedCommand])
	if len(page.Items) != 1 || page.Items[0].Command != "stats slabs" {
		t.Fatalf("unexpected saved commands: %+v", page.Items)
	}
	if store.scope.Level != plugin.StorageScopeConnection {
		t.Fatalf("saved commands must be connection scoped, got %q", store.scope.Level)
	}

	rejectRC := plugin.NewRequestContext(ctx, plugin.User{}, s, nil, nil, []byte(`{"name":"bad","command":"shutdown"}`)).WithStorage(store)
	if _, err := routes["memcached.commands.save"].Handle(rejectRC); err == nil {
		t.Fatal("a command outside the allow-list must not be saved")
	}

	delRC := plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"id": "slab-report"}, nil, nil).WithStorage(store)
	if _, err := routes["memcached.commands.delete"].Handle(delRC); err != nil {
		t.Fatalf("delete command: %v", err)
	}
	if len(store.items) != 0 {
		t.Fatalf("the record should be gone: %+v", store.items)
	}
}

type memoryStorage struct {
	items map[string]plugin.StorageItem
	scope plugin.StorageScope
}

func newMemoryStorage() *memoryStorage {
	return &memoryStorage{items: map[string]plugin.StorageItem{}}
}

func (m *memoryStorage) Get(_ context.Context, scope plugin.StorageScope, key string) (plugin.StorageItem, error) {
	m.scope = scope
	item, ok := m.items[key]
	if !ok {
		return plugin.StorageItem{}, plugin.ErrNotFound
	}
	return item, nil
}

func (m *memoryStorage) Put(_ context.Context, collection string, item plugin.StorageItem) (plugin.StorageItem, error) {
	m.scope = plugin.StorageScope{Collection: collection, Level: plugin.StorageScopeConnection}
	item.UpdatedAt = time.Now()
	m.items[item.Key] = item
	return item, nil
}

func (m *memoryStorage) Delete(_ context.Context, scope plugin.StorageScope, key string) error {
	m.scope = scope
	delete(m.items, key)
	return nil
}

func (m *memoryStorage) List(_ context.Context, scope plugin.StorageScope, _ *plugin.StorageListFilter) ([]plugin.StorageItem, error) {
	m.scope = scope
	out := make([]plugin.StorageItem, 0, len(m.items))
	for _, item := range m.items {
		out = append(out, item)
	}
	return out, nil
}

func pageRowItems(t *testing.T, value any) []map[string]any {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal page: %v", err)
	}
	var decoded struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode page: %v", err)
	}
	return decoded.Items
}

func decodeFrames(t *testing.T, raw []byte) []map[string]any {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	out := []map[string]any{}
	for dec.More() {
		frame := map[string]any{}
		if err := dec.Decode(&frame); err != nil {
			t.Fatalf("decode frame: %v", err)
		}
		out = append(out, frame)
	}
	return out
}

func TestStatsResetIsAnAdminLineReply(t *testing.T) {
	spec, payload, err := resolveCommand("stats reset")
	if err != nil {
		t.Fatalf("stats reset: %v", err)
	}
	if spec.Reply != replyLine || spec.Class != classAdmin || payload != "stats reset\r\n" {
		t.Fatalf("stats reset must be read as a single RESET line: %+v %q", spec, payload)
	}
	if spec, _, err := resolveCommand("lru_crawler crawl all"); err != nil || spec.Reply != replyLine || spec.Class != classAdmin {
		t.Fatalf("lru_crawler crawl must be read as a single line: %+v %v", spec, err)
	}
	if spec, _, err := resolveCommand("lru_crawler metadump all"); err != nil || spec.Reply != replyStats || spec.Class != classRead {
		t.Fatalf("lru_crawler metadump must stay a read that ends at END: %+v %v", spec, err)
	}
}

func TestApplyMetadataUsesRelativeExpiry(t *testing.T) {
	detail := keyDetail{TTL: -1}
	applyMetadata(&detail, map[string]string{"exp": "583", "la": "17", "cls": "4", "fetch": "yes"})
	if detail.TTL != 583 {
		t.Fatalf("me reports the remaining TTL directly: %+v", detail)
	}
	if detail.Class != 4 || !detail.Fetched || detail.LastAccess == "" {
		t.Fatalf("meta debug fields not applied: %+v", detail)
	}
	detail = keyDetail{TTL: 0}
	applyMetadata(&detail, map[string]string{"exp": "-1"})
	if detail.TTL != -1 {
		t.Fatalf("an item without an expiry must report -1: %+v", detail)
	}
}

func TestMetaItemToRowUsesAbsoluteTimestamps(t *testing.T) {
	now := int64(1_700_000_000)
	row := metaItemToRow(metaItem{Key: "k", Expires: now + 120, LastRead: now - 30, Class: 2, Size: 71, Flags: 7}, now)
	if row.TTL != 120 || row.Flags != 7 || row.Class != 2 {
		t.Fatalf("crawler timestamps are absolute: %+v", row)
	}
	if row.Expires == "" || row.LastAccess == "" {
		t.Fatalf("expiry and last access should be rendered: %+v", row)
	}
	if expired := metaItemToRow(metaItem{Key: "k", Expires: now - 5}, now); expired.TTL != 0 {
		t.Fatalf("an item past its expiry should report ttl 0: %+v", expired)
	}
	if never := metaItemToRow(metaItem{Key: "k"}, now); never.TTL != -1 {
		t.Fatalf("an item without an expiry should report ttl -1: %+v", never)
	}
}

func TestKeyScanStopsAtTheConfiguredCap(t *testing.T) {
	var dump strings.Builder
	dump.WriteString("OK\r\n")
	for i := 0; i < 400; i++ {
		dump.WriteString("key=k" + strconv.Itoa(i) + " exp=-1 la=1700000000 cas=1 fetch=no cls=1 size=70\n")
	}
	dump.WriteString("END\r\n")
	s := fakeSession(t, map[string]string{
		"version":                  "VERSION 1.6.45\r\n",
		"lru_crawler metadump all": dump.String(),
	}, map[string]any{"scan_limit": 100})

	result, err := s.scanKeys(context.Background(), classScopeAll)
	if err != nil {
		t.Fatalf("scan keys: %v", err)
	}
	if len(result.Items) != 100 || !result.Truncated || !result.Retire {
		t.Fatalf("the scan cap must stop the dump and retire the connection: %d %+v", len(result.Items), result)
	}
	// The abandoned connection must not poison the pool for the next caller.
	if err := s.HealthCheck(context.Background()); err != nil {
		t.Fatalf("session unusable after a truncated scan: %v", err)
	}
}

// The gomemcache client must never resolve the target itself: the address has to
// reach cfg.Net verbatim, or an agent connection to a name that only resolves on
// the far side loses the whole data plane.
func TestDataPlaneDialsTheConfiguredAddressThroughTheGateway(t *testing.T) {
	server := newFakeServer(t, map[string]string{
		"version":     "VERSION 1.6.45\r\n",
		"gets absent": "END\r\n",
	})
	var mu sync.Mutex
	var dialed []string
	transport := plugintest.TransportFunc(func(ctx context.Context, network, addr string) (net.Conn, error) {
		mu.Lock()
		dialed = append(dialed, addr)
		mu.Unlock()
		var dialer net.Dialer
		return dialer.DialContext(ctx, network, server.listener.Addr().String())
	})

	sess, err := connect(context.Background(), plugin.ConnectConfig{Config: map[string]any{
		"host": "memcached.invalid", "port": 11211, "auth": authNone, "tls_mode": "disable", "read_only": false,
	}, Net: transport})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = sess.Close() })

	if _, err := sess.(*Session).readKey(context.Background(), "absent"); !errors.Is(err, plugin.ErrNotFound) {
		t.Fatalf("the data plane must reach the server through the gateway, got %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(dialed) < 2 {
		t.Fatalf("expected both an admin and a data-plane dial, got %v", dialed)
	}
	for _, addr := range dialed {
		if addr != "memcached.invalid:11211" {
			t.Fatalf("every dial must carry the configured address, got %q", addr)
		}
	}
}

func TestExpirationForCrossesTheThirtyDayBoundary(t *testing.T) {
	if got := expirationFor(600); got != 600 {
		t.Fatalf("a short TTL stays relative: %d", got)
	}
	if got := expirationFor(relativeTTLMax); got != relativeTTLMax {
		t.Fatalf("the boundary itself stays relative: %d", got)
	}
	long := int64(relativeTTLMax + 1)
	got := expirationFor(long)
	if delta := int64(got) - (time.Now().Unix() + long); delta < -2 || delta > 2 {
		t.Fatalf("a TTL past 30 days must be sent as an absolute timestamp: %d", got)
	}
	if _, _, err := parseTTL(int64(math.MaxInt32) + 1); err == nil {
		t.Fatal("a TTL that cannot fit the protocol field must be rejected")
	}
}

func TestConsoleReplyStopsAtTheRowCap(t *testing.T) {
	var dump strings.Builder
	dump.WriteString("OK\r\n")
	for i := 0; i < maxConsoleReplyRows+50; i++ {
		dump.WriteString("key=k" + strconv.Itoa(i) + " exp=-1 la=1700000000 cas=1 fetch=no cls=1 size=70\r\n")
	}
	dump.WriteString("END\r\n")
	s := fakeSession(t, map[string]string{
		"version":                  "VERSION 1.6.45\r\n",
		"lru_crawler metadump all": dump.String(),
		"stats":                    "STAT uptime 5\r\nEND\r\n",
	}, nil)
	h := contribtest.NewHarness(t, New().Routes())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	out := h.Stream(ctx, "memcached.console", s, nil, nil,
		[]byte("{\"query\":\"lru_crawler metadump all\"}\n{\"query\":\"stats\"}\n"))
	frames := decodeFrames(t, out)
	if len(frames) != 2 {
		t.Fatalf("expected two frames, got %d: %s", len(frames), out)
	}
	if frames[0]["truncated"] != true {
		t.Fatalf("an unbounded dump must be reported as truncated: %+v", frames[0])
	}
	if frames[0]["rowCount"].(float64) != maxConsoleReplyRows {
		t.Fatalf("the reply must stop at the row cap: %+v", frames[0]["rowCount"])
	}
	// The abandoned connection must not poison the pool for the next command.
	if frames[1]["rowCount"].(float64) != 1 {
		t.Fatalf("the next console command must run on a clean connection: %+v", frames[1])
	}
}

func TestReadMetaIgnoresAMalformedValueHeader(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	go func() {
		_, _ = server.Write([]byte("VA\r\n"))
	}()
	out, err := newTextConn(client).readMeta()
	if err != nil {
		t.Fatalf("a malformed meta header must not fail the read: %v", err)
	}
	if len(out.Lines) != 1 || out.Lines[0] != "VA" {
		t.Fatalf("unexpected meta result: %+v", out)
	}
}

func TestMetadumpReportsCrawlerContention(t *testing.T) {
	s := fakeSession(t, map[string]string{
		"version":                  "VERSION 1.6.45\r\n",
		"lru_crawler metadump all": "BUSY currently processing crawler request\r\n",
	}, nil)
	if _, err := s.scanKeys(context.Background(), classScopeAll); err == nil {
		t.Fatal("a busy crawler must surface as an error")
	}
}
