package mqtt

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
	sdktest "github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestMQTTPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_MQTT_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_MQTT_INTEGRATION=1 to run against an MQTT broker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	cfg := mqttIntegrationConfig(ctx, t)
	sess, err := connect(ctx, plugin.ConnectConfig{Config: cfg, Net: sdktest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	s := sess.(*Session)

	prefix := integrationPrefix + time.Now().UTC().Format("20060102150405")
	retainedTopic, liveTopic := prefix+"/retained", prefix+"/live"

	publish(ctx, t, s, retainedTopic, "kept", true)
	publish(ctx, t, s, liveTopic, "transient", false)
	waitForTopic(ctx, t, s, retainedTopic)
	waitForTopic(ctx, t, s, liveTopic)

	topics := listRows(t, listTopics, rc(t, s, nil, url.Values{"limit": {"200"}}, nil))
	if !containsTopic(topics, retainedTopic) || !containsTopic(topics, liveTopic) {
		t.Fatalf("observed topics are missing the published topics: %v", rowTopics(topics))
	}
	retained := listRows(t, listRetained, rc(t, s, nil, url.Values{"limit": {"200"}}, nil))
	if !containsTopic(retained, retainedTopic) {
		t.Fatalf("retained snapshot is missing %q: %v", retainedTopic, rowTopics(retained))
	}
	if containsTopic(retained, liveTopic) {
		t.Fatalf("a non-retained publish leaked into the retained snapshot: %v", rowTopics(retained))
	}
	messages := listRows(t, listMessages, rc(t, s, map[string]string{"topic": liveTopic}, nil, nil))
	if len(messages) == 0 || messages[0]["payload"] != "transient" {
		t.Fatalf("message buffer: %#v", messages)
	}
	if _, err := topicOverview(rc(t, s, map[string]string{"topic": retainedTopic}, nil, nil)); err != nil {
		t.Fatalf("topic overview: %v", err)
	}
	if _, err := treeTopics(rc(t, s, map[string]string{"prefix": "shellcn"}, nil, nil)); err != nil {
		t.Fatalf("topic tree: %v", err)
	}
	if _, err := overview(rc(t, s, nil, nil, nil)); err != nil {
		t.Fatalf("overview: %v", err)
	}

	streamTopic := prefix + "/stream"
	assertStreamDelivers(ctx, t, s, streamTopic)

	// A retained message is removed by publishing an empty retained payload; the
	// rescan then proves the broker no longer replays it.
	if _, err := clearRetained(rc(t, s, map[string]string{"topic": retainedTopic}, nil, nil)); err != nil {
		t.Fatalf("clear retained: %v", err)
	}
	// The rescan is retried, not just the read: a broker whose retainer removes
	// the message asynchronously can still replay it into the subscription that
	// the previous rescan re-established.
	deadline := time.Now().Add(20 * time.Second)
	for {
		if _, err := rescanRetained(rc(t, s, nil, nil, nil)); err != nil {
			t.Fatalf("rescan: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
		rows := listRows(t, listRetained, rc(t, s, nil, url.Values{"limit": {"200"}}, nil))
		if !containsTopic(rows, retainedTopic) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("retained message %q survived the clear", retainedTopic)
		}
	}

	if emqxURL := os.Getenv("SHELLCN_MQTT_EMQX_URL"); emqxURL != "" {
		emqxCfg := maps.Clone(cfg)
		// A distinct client id: the first session is still connected, and a broker
		// kicks the older holder of a duplicate id.
		emqxCfg["client_id"] = "shellcn-it-mgmt"
		emqxCfg["emqx_url"] = emqxURL
		emqxCfg["emqx_auth"] = "api_key"
		emqxCfg["emqx_api_key"] = os.Getenv("SHELLCN_MQTT_EMQX_KEY")
		emqxCfg["emqx_api_secret"] = os.Getenv("SHELLCN_MQTT_EMQX_SECRET")
		managed, err := connect(ctx, plugin.ConnectConfig{Config: emqxCfg, Net: sdktest.DirectTransport()})
		if err != nil {
			t.Fatalf("connect with EMQX API: %v", err)
		}
		defer func() { _ = managed.Close() }()
		for name, list := range map[string]func(*plugin.RequestContext) (any, error){
			"clients": listEMQXClients, "subscriptions": listEMQXSubscriptions,
			"topics": listEMQXTopics, "nodes": listEMQXNodes,
		} {
			out, err := list(rc(t, managed, nil, url.Values{"limit": {"200"}}, nil))
			if err != nil {
				t.Fatalf("EMQX %s: %v", name, err)
			}
			page := out.(scanPage)
			if page.Unavailable || len(page.Items) == 0 {
				t.Fatalf("EMQX %s returned nothing: %#v", name, page)
			}
		}
		clients := scanRows(t, listEMQXClients, rc(t, managed, nil, url.Values{"limit": {"200"}}, nil))
		if !containsField(clients, "clientid", "shellcn-it-mgmt") {
			t.Fatalf("the management session is missing from the EMQX client list: %#v", clients)
		}
		nodes := scanRows(t, listEMQXNodes, rc(t, managed, nil, nil, nil))
		if _, ok := nodes[0]["uptime"].(float64); !ok {
			t.Fatalf("node uptime should be a converted number: %#v", nodes[0])
		}
	}
}

func scanRows(t *testing.T, list func(*plugin.RequestContext) (any, error), ctx *plugin.RequestContext) []row {
	t.Helper()
	out, err := list(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	return out.(scanPage).Items
}

func containsField(rows []row, key, want string) bool {
	for _, item := range rows {
		if fmt.Sprint(item[key]) == want {
			return true
		}
	}
	return false
}

func publish(ctx context.Context, t *testing.T, s *Session, topic, payload string, retain bool) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"topic": topic, "payload": payload, "encoding": "string", "qos": "1", "retain": retain})
	if _, err := publishMessage(plugin.NewRequestContext(ctx, plugin.User{}, s, nil, nil, body)); err != nil {
		t.Fatalf("publish %s: %v", topic, err)
	}
}

func waitForTopic(ctx context.Context, t *testing.T, s *Session, topic string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, ok := s.topic(topic); ok {
			return
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			t.Fatalf("topic %q was never observed", topic)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func assertStreamDelivers(ctx context.Context, t *testing.T, s *Session, topic string) {
	t.Helper()
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream := &memoryStream{ctx: streamCtx}
	done := make(chan error, 1)
	go func() {
		done <- subscribeStream(plugin.NewRequestContext(streamCtx, plugin.User{}, s, map[string]string{"filter": topic, "qos": "1"}, nil, nil), stream)
	}()
	// Give the subscription time to reach the broker before publishing.
	time.Sleep(time.Second)
	publish(ctx, t, s, topic, "streamed", false)

	deadline := time.Now().Add(15 * time.Second)
	for !strings.Contains(stream.output(), "streamed") {
		if time.Now().After(deadline) {
			t.Fatalf("live stream never delivered the published message: %q", stream.output())
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("stream: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("stream did not exit after the client disconnected")
	}
}

func listRows(t *testing.T, list func(*plugin.RequestContext) (any, error), ctx *plugin.RequestContext) []row {
	t.Helper()
	out, err := list(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	return out.(plugin.Page[row]).Items
}

func containsTopic(rows []row, topic string) bool {
	for _, item := range rows {
		if fmt.Sprint(item["topic"]) == topic {
			return true
		}
	}
	return false
}

func mqttIntegrationConfig(ctx context.Context, t *testing.T) map[string]any {
	t.Helper()
	address := os.Getenv("SHELLCN_MQTT_ADDRESS")
	if address == "" {
		address = startMosquittoContainer(ctx, t)
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatalf("SHELLCN_MQTT_ADDRESS must be host:port, got %q", address)
	}
	cfg := integrationConfig(host, port)
	if user := os.Getenv("SHELLCN_MQTT_USERNAME"); user != "" {
		cfg["auth"] = "password"
		cfg["username"] = user
		cfg["password"] = os.Getenv("SHELLCN_MQTT_PASSWORD")
	}
	return cfg
}

// integrationPrefix scopes the run to its own topic space. It is also the
// observe filter, because a broker may refuse a root wildcard: EMQX's default
// ACL denies subscribing to "#" and "$SYS/#".
const integrationPrefix = "shellcn/it/"

func integrationConfig(host, port string) map[string]any {
	portNumber, _ := strconv.Atoi(port)
	return map[string]any{
		"host": host, "port": float64(portNumber),
		"client_id": "shellcn-it", "clean_session": true, "keep_alive": "30s",
		"auth": "none", "tls_mode": "disable",
		"observe": true, "observe_filter": integrationPrefix + "#", "sys_metrics": true,
		"topic_limit": float64(2000), "message_limit": float64(200),
		"read_only": false, "timeout": "5s",
	}
}

func startMosquittoContainer(ctx context.Context, t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_MQTT_ADDRESS is not set")
	}
	probe, cancelProbe := context.WithTimeout(ctx, 15*time.Second)
	defer cancelProbe()
	if out, err := exec.CommandContext(probe, "docker", "info").CombinedOutput(); err != nil {
		t.Skipf("docker daemon unreachable and SHELLCN_MQTT_ADDRESS is not set: %v\n%s", err, out)
	}
	name := "shellcn-mqtt-it-" + time.Now().UTC().Format("20060102150405")
	// The image's default config only listens on loopback inside the container;
	// /mosquitto-no-auth.conf ships an anonymous listener on 1883.
	run(ctx, t, "docker", "run", "-d", "--rm", "--name", name, "-p", "127.0.0.1::1883",
		"eclipse-mosquitto:2", "mosquitto", "-c", "/mosquitto-no-auth.conf")
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})
	out := run(ctx, t, "docker", "port", name, "1883/tcp")
	host, port, err := net.SplitHostPort(strings.TrimSpace(strings.Split(out, "\n")[0]))
	if err != nil {
		t.Fatalf("unexpected docker port output: %q", out)
	}
	address := net.JoinHostPort(host, port)
	deadline := time.Now().Add(45 * time.Second)
	for {
		sess, err := connect(ctx, plugin.ConnectConfig{Config: integrationConfig(host, port), Net: sdktest.DirectTransport()})
		if err == nil {
			_ = sess.Close()
			return address
		}
		if time.Now().After(deadline) {
			t.Fatalf("mosquitto container did not become ready: %v", err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func run(ctx context.Context, t *testing.T, name string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
	return string(out)
}
