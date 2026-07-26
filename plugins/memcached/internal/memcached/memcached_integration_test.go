package memcached

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	contribtest "github.com/charlesng35/shellcn-contrib/shared/plugintest"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

const memcachedImage = "memcached:1.6-alpine"

func TestMemcachedPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_MEMCACHED_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_MEMCACHED_INTEGRATION=1 to run against memcached")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	cfg := integrationConfig(ctx, t)
	p := New()
	sess, err := p.Connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()

	h := contribtest.NewHarness(t, p.Routes())
	prefix := "shellcn:it:" + strconv.FormatInt(time.Now().UnixNano(), 36)
	key := prefix + ":alpha"
	counter := prefix + ":counter"

	// Overview and server-wide read routes.
	var overview serverOverview
	decodeInto(t, h.Call(ctx, "memcached.overview", sess, nil, nil, nil), &overview)
	if overview.Version == "" || overview.LimitMaxBytes == 0 {
		t.Fatalf("unexpected overview: %+v", overview)
	}
	settings := pageRowItems(t, h.Call(ctx, "memcached.settings.list", sess, nil, nil, nil))
	if len(settings) == 0 {
		t.Fatal("stats settings returned nothing")
	}
	if !hasSetting(settings, "lru_crawler", "yes") {
		t.Fatal("the container must run with the LRU crawler enabled for the key browser")
	}

	// Store, read, touch, and overwrite an item.
	h.Call(ctx, "memcached.key.store", sess, nil, nil, mustJSONBytes(t, map[string]any{
		"key": key, "mode": "add", "type": valueJSON, "value": `{"hello":"world"}`, "ttl": 600, "flags": 42,
	}))
	var detail keyDetail
	decodeInto(t, h.Call(ctx, "memcached.key.read", sess, map[string]string{"key": key}, nil, nil), &detail)
	if detail.Value != `{"hello":"world"}` || detail.Type != valueJSON {
		t.Fatalf("unexpected item detail: %+v", detail)
	}
	if detail.Flags != 42 || detail.TTL <= 0 || detail.Class <= 0 {
		t.Fatalf("item metadata not read back from the meta debug command: %+v", detail)
	}

	if _, err := h.Route("memcached.key.store").Handle(plugin.NewRequestContext(ctx, plugin.User{}, sess, nil, nil, mustJSONBytes(t, map[string]any{
		"key": key, "mode": "add", "value": "again",
	}))); err == nil {
		t.Fatal("add must fail once the key exists")
	}

	h.Call(ctx, "memcached.key.touch", sess, nil, nil, mustJSONBytes(t, map[string]any{"key": key, "ttl": 900}))
	h.Call(ctx, "memcached.key.write", sess, map[string]string{"key": key}, nil, mustJSONBytes(t, map[string]any{
		"key": key, "type": valueText, "value": "edited",
	}))
	decodeInto(t, h.Call(ctx, "memcached.key.read", sess, map[string]string{"key": key}, nil, nil), &detail)
	if detail.Value != "edited" {
		t.Fatalf("value edit not applied: %+v", detail)
	}
	if detail.Flags != 42 {
		t.Fatalf("a value edit must preserve the item's client flags: %+v", detail)
	}
	if detail.TTL <= 0 {
		t.Fatalf("a value edit must preserve the item's TTL: %+v", detail)
	}

	// A TTL past 30 days is stored as an absolute timestamp but reported back as a
	// remaining TTL, so a value edit has to convert it back before it is resent.
	longKey := prefix + ":long"
	h.Call(ctx, "memcached.key.store", sess, nil, nil, mustJSONBytes(t, map[string]any{
		"key": longKey, "mode": "set", "value": "first", "ttl": time.Now().Add(60 * 24 * time.Hour).Unix(),
	}))
	h.Call(ctx, "memcached.key.write", sess, map[string]string{"key": longKey}, nil, mustJSONBytes(t, map[string]any{
		"key": longKey, "type": valueText, "value": "second",
	}))
	var longDetail keyDetail
	decodeInto(t, h.Call(ctx, "memcached.key.read", sess, map[string]string{"key": longKey}, nil, nil), &longDetail)
	if longDetail.Value != "second" || longDetail.TTL < 59*24*3600 {
		t.Fatalf("editing a long-lived item must keep its expiry: %+v", longDetail)
	}

	// Counters.
	h.Call(ctx, "memcached.key.store", sess, nil, nil, mustJSONBytes(t, map[string]any{"key": counter, "mode": "set", "value": "10"}))
	var incremented actionResult
	decodeInto(t, h.Call(ctx, "memcached.key.incr", sess, nil, nil, mustJSONBytes(t, map[string]any{"key": counter, "delta": 5})), &incremented)
	if incremented.Value != "15" {
		t.Fatalf("increment returned %+v", incremented)
	}
	var decremented actionResult
	decodeInto(t, h.Call(ctx, "memcached.key.decr", sess, nil, nil, mustJSONBytes(t, map[string]any{"key": counter, "delta": 3})), &decremented)
	if decremented.Value != "12" {
		t.Fatalf("decrement returned %+v", decremented)
	}

	// The LRU crawler key scanner.
	keys := pageRowItems(t, h.Call(ctx, "memcached.keys.list", sess, nil, nil, nil))
	if !hasKey(keys, key) || !hasKey(keys, counter) {
		t.Fatalf("stored keys missing from the scan: %+v", keys)
	}

	// Slab and LRU statistics.
	slabs := pageRowItems(t, h.Call(ctx, "memcached.slabs.list", sess, nil, nil, nil))
	if len(slabs) == 0 {
		t.Fatal("expected at least one slab class after storing items")
	}
	class := int(slabs[0]["class"].(float64))
	scoped := pageRowItems(t, h.Call(ctx, "memcached.slabs.list", sess, map[string]string{classScopeParam: strconv.Itoa(class)}, nil, nil))
	if len(scoped) != 1 {
		t.Fatalf("the slab class scope should return exactly one row, got %d", len(scoped))
	}
	options := h.Call(ctx, "memcached.slabs.options", sess, nil, nil, nil).(plugin.Page[plugin.FilterOption])
	if len(options.Items) < 2 {
		t.Fatalf("scope options should list the active slab classes: %+v", options.Items)
	}
	items := pageRowItems(t, h.Call(ctx, "memcached.items.list", sess, nil, nil, nil))
	if len(items) == 0 {
		t.Fatal("expected LRU statistics for the populated slab class")
	}
	conns := pageRowItems(t, h.Call(ctx, "memcached.conns.list", sess, nil, nil, nil))
	if len(conns) == 0 {
		t.Fatal("expected at least this plugin's own connections")
	}

	// Streams.
	canvasOut := h.Stream(streamCtx(ctx, 10*time.Second), "memcached.slabs.canvas", sess, nil, nil,
		[]byte(`{"type":"ready","width":900,"height":600,"dpr":1,"theme":"dark"}`+"\n"+
			`{"type":"key","event":"keydown","key":"ArrowDown"}`+"\n"))
	if !strings.Contains(string(canvasOut), `"type":"clear"`) || !strings.Contains(string(canvasOut), "class:") {
		t.Fatalf("canvas stream did not draw slab regions: %s", truncate(string(canvasOut)))
	}

	metricsOut := h.Stream(streamCtx(ctx, 3*time.Second), "memcached.metrics", sess, nil, nil, nil)
	metricFrames := decodeFrames(t, metricsOut)
	if len(metricFrames) == 0 || metricFrames[0]["currItems"] == nil {
		t.Fatalf("metrics stream produced no usable frame: %s", truncate(string(metricsOut)))
	}
	if metricFrames[0]["memoryTotal"] == nil || metricFrames[0]["hitRatio"] == nil {
		t.Fatalf("metrics frame is missing capacity keys: %+v", metricFrames[0])
	}

	watchOut := h.Stream(streamCtx(ctx, 3*time.Second), "memcached.overview.watch", sess, nil, nil, nil)
	watchFrames := decodeFrames(t, watchOut)
	if len(watchFrames) == 0 || watchFrames[0]["type"] != "modified" {
		t.Fatalf("overview watch produced no resource event: %s", truncate(string(watchOut)))
	}
	watched, ok := watchFrames[0]["resource"].(map[string]any)
	if !ok || watched["address"] == nil {
		t.Fatalf("overview watch produced no snapshot: %s", truncate(string(watchOut)))
	}
	if watched["status"] != statusOK {
		t.Fatalf("a reachable server must be watched as ok: %+v", watched)
	}

	// Activity: the watcher only reports events that happen while it is attached.
	h.Call(ctx, "memcached.events.list", sess, nil, nil, nil)
	events := waitForEvents(ctx, t, h, sess, prefix)
	if len(events) == 0 {
		t.Fatal("the watcher reported no events after storing items")
	}
	eventsOut := h.Stream(streamCtx(ctx, 3*time.Second), "memcached.events.watch", sess, nil, nil, nil)
	if len(eventsOut) > 0 {
		frames := decodeFrames(t, eventsOut)
		if frames[0]["type"] != "added" {
			t.Fatalf("watch frames must be resource events: %+v", frames[0])
		}
	}

	// Console.
	consoleOut := h.Stream(streamCtx(ctx, 20*time.Second), "memcached.console", sess, nil, nil, []byte(
		"{\"query\":\"stats slabs\"}\n"+
			"{\"query\":\"set "+prefix+":console 0 0 5\\nhello\"}\n"+
			"{\"query\":\"get "+prefix+":console\"}\n"+
			"{\"query\":\"me "+prefix+":console\"}\n"+
			"{\"query\":\"stats reset\"}\n"+
			"{\"query\":\"stats reset\",\"confirm\":true}\n"))
	frames := decodeFrames(t, consoleOut)
	if len(frames) != 6 {
		t.Fatalf("expected six console frames, got %d: %s", len(frames), truncate(string(consoleOut)))
	}
	if frames[0]["rowCount"].(float64) == 0 {
		t.Fatalf("stats slabs returned no rows: %+v", frames[0])
	}
	if frames[1]["message"] != "STORED" {
		t.Fatalf("console set failed: %+v", frames[1])
	}
	if !strings.Contains(mustJSONString(t, frames[2]), "hello") {
		t.Fatalf("console get did not return the value: %+v", frames[2])
	}
	if !strings.Contains(mustJSONString(t, frames[3]), "cls=") {
		t.Fatalf("meta debug reply missing internal metadata: %+v", frames[3])
	}
	if frames[4]["confirmRequired"] != true {
		t.Fatalf("stats reset must ask for confirmation: %+v", frames[4])
	}
	if frames[5]["message"] == nil {
		t.Fatalf("confirmed stats reset should execute: %+v", frames[5])
	}
	completions := h.Call(ctx, "memcached.console.complete", sess, nil, nil, nil).(plugin.Page[completionEntry])
	if len(completions.Items) == 0 {
		t.Fatal("console completions are empty")
	}

	// Saved commands run through scoped plugin storage.
	store := newMemoryStorage()
	saveRoute := h.Route("memcached.commands.save")
	saved, err := saveRoute.Handle(plugin.NewRequestContext(ctx, plugin.User{}, sess, nil, nil,
		mustJSONBytes(t, map[string]any{"name": "Slab report", "command": "stats slabs"})).WithStorage(store))
	if err != nil {
		t.Fatalf("save command: %v", err)
	}
	record := saved.(savedCommand)
	listed, err := h.Route("memcached.commands.list").Handle(plugin.NewRequestContext(ctx, plugin.User{}, sess, nil, nil, nil).WithStorage(store))
	if err != nil {
		t.Fatalf("list commands: %v", err)
	}
	if len(listed.(plugin.Page[savedCommand]).Items) != 1 {
		t.Fatalf("expected one saved command: %+v", listed)
	}
	if _, err := h.Route("memcached.commands.delete").Handle(plugin.NewRequestContext(ctx, plugin.User{}, sess,
		map[string]string{"id": record.ID}, nil, nil).WithStorage(store)); err != nil {
		t.Fatalf("delete command: %v", err)
	}

	// Administrative operations.
	h.Call(ctx, "memcached.server.crawl", sess, nil, nil, mustJSONBytes(t, map[string]any{"classes": classScopeAll}))
	h.Call(ctx, "memcached.server.memlimit", sess, nil, nil, mustJSONBytes(t, map[string]any{"megabytes": 96}))
	h.Call(ctx, "memcached.server.verbosity", sess, nil, nil, mustJSONBytes(t, map[string]any{"level": 1}))
	h.Call(ctx, "memcached.server.stats_reset", sess, nil, nil, nil)
	decodeInto(t, h.Call(ctx, "memcached.overview", sess, nil, nil, nil), &overview)
	if overview.LimitMaxBytes != 96*1024*1024 {
		t.Fatalf("cache_memlimit did not take effect: %d", overview.LimitMaxBytes)
	}

	// Deletion and the destructive flush.
	h.Call(ctx, "memcached.key.delete", sess, map[string]string{"key": key}, nil, nil)
	if _, err := h.Route("memcached.key.read").Handle(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"key": key}, nil, nil)); err == nil {
		t.Fatal("reading a deleted key must fail")
	}
	h.Call(ctx, "memcached.server.flush", sess, nil, nil, mustJSONBytes(t, map[string]any{"delay": 0}))
	if _, err := h.Route("memcached.key.read").Handle(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"key": counter}, nil, nil)); err == nil {
		t.Fatal("flush_all must invalidate every remaining item")
	}

	h.AssertAllCovered()
}

func TestMemcachedReadOnlyIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_MEMCACHED_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_MEMCACHED_INTEGRATION=1 to run against memcached")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cfg := integrationConfig(ctx, t)
	cfg["read_only"] = true
	p := New()
	sess, err := p.Connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()

	routes := plugintest.RouteMap(p.Routes())
	if _, err := routes["memcached.server.flush"].Handle(plugin.NewRequestContext(ctx, plugin.User{}, sess, nil, nil, []byte(`{"delay":0}`))); err == nil {
		t.Fatal("flush must be refused on a read-only connection")
	}
	if _, err := routes["memcached.overview"].Handle(plugin.NewRequestContext(ctx, plugin.User{}, sess, nil, nil, nil)); err != nil {
		t.Fatalf("reads must still work on a read-only connection: %v", err)
	}
}

func waitForEvents(ctx context.Context, t *testing.T, h *contribtest.Harness, sess plugin.Session, prefix string) []map[string]any {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for i := 0; ; i++ {
		body := mustJSONBytes(t, map[string]any{"key": prefix + ":event" + strconv.Itoa(i), "mode": "set", "value": "ping"})
		h.Call(ctx, "memcached.key.store", sess, nil, nil, body)
		time.Sleep(300 * time.Millisecond)
		events := pageRowItems(t, h.Call(ctx, "memcached.events.list", sess, nil, nil, nil))
		if len(events) > 0 {
			return events
		}
		if time.Now().After(deadline) {
			return nil
		}
	}
}

func integrationConfig(ctx context.Context, t *testing.T) map[string]any {
	t.Helper()
	addr := os.Getenv("SHELLCN_MEMCACHED_ADDR")
	if addr == "" {
		addr = startMemcachedContainer(ctx, t)
	}
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("parse memcached address %q: %v", addr, err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse memcached port %q: %v", portText, err)
	}
	return map[string]any{
		"host": host, "port": port, "auth": authNone, "tls_mode": "disable",
		"read_only": false, "timeout": "10s", "scan_limit": 2000, "page_limit": 200,
		"watch_streams": []string{"mutations", "evictions", "deletions", "connevents"},
	}
}

func startMemcachedContainer(ctx context.Context, t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_MEMCACHED_ADDR is not set")
	}
	name := "shellcn-memcached-it-" + time.Now().UTC().Format("20060102150405")
	runCmd(ctx, t, "docker", "run", "-d", "--rm", "--name", name,
		"-p", "127.0.0.1::11211", memcachedImage, "memcached", "-m", "64", "-o", "lru_crawler")
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})
	out := runCmd(ctx, t, "docker", "port", name, "11211/tcp")
	addr := strings.TrimSpace(strings.Split(strings.TrimSpace(out), "\n")[0])

	deadline := time.Now().Add(90 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			_ = conn.Close()
			return addr
		}
		if time.Now().After(deadline) {
			t.Fatalf("memcached container did not become ready: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func runCmd(ctx context.Context, t *testing.T, name string, args ...string) string {
	t.Helper()
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
	return string(out)
}

func streamCtx(parent context.Context, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(parent, d)
	go func() {
		<-ctx.Done()
		cancel()
	}()
	return ctx
}

func decodeInto(t *testing.T, value any, dst any) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		t.Fatalf("decode into %T: %v", dst, err)
	}
}

func mustJSONBytes(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

func mustJSONString(t *testing.T, value any) string {
	return string(mustJSONBytes(t, value))
}

func hasKey(rows []map[string]any, key string) bool {
	for _, row := range rows {
		if row["key"] == key {
			return true
		}
	}
	return false
}

func hasSetting(rows []map[string]any, name, value string) bool {
	for _, row := range rows {
		if row["name"] == name && row["value"] == value {
			return true
		}
	}
	return false
}

func truncate(text string) string {
	if len(text) > 800 {
		return text[:800] + "…"
	}
	return text
}
