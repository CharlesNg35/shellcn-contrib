package pulsar

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
	sdktest "github.com/charlesng35/shellcn/sdk/plugintest"
)

// pulsarImage is pinned so the integration run does not silently move between
// broker releases.
const pulsarImage = "apachepulsar/pulsar:4.0.3"

func TestPulsarPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_PULSAR_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_PULSAR_INTEGRATION=1 to run against Pulsar")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	adminURL, serviceURL := pulsarEndpoints(ctx, t)
	sess, err := connect(ctx, plugin.ConnectConfig{
		Config: map[string]any{
			"admin_url":     adminURL,
			"service_url":   serviceURL,
			"tenant":        defaultTenant,
			"namespace":     defaultNamespace,
			"auth":          "none",
			"tls_mode":      "disable",
			"read_only":     false,
			"timeout":       "30s",
			"message_limit": 20,
		},
		Net: sdktest.DirectTransport(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()

	suffix := time.Now().UTC().Format("20060102150405")
	tenant := "shellcn-it-" + suffix
	namespace := "events"
	topicName := "orders"
	topic := fmt.Sprintf("persistent://%s/%s/%s", tenant, namespace, topicName)
	subscription := "shellcn-it-sub"
	scope := map[string]string{"tenant": tenant, "namespace": namespace}
	topicArgs := map[string]string{"topic": topic}
	subScope := map[string]string{"topic": topic, "subscription": subscription}

	clusters, err := listClusters(request(ctx, sess, nil, nil, nil))
	if err != nil {
		t.Fatalf("list clusters: %v", err)
	}
	names := []string{}
	for _, item := range clusters.(plugin.Page[row]).Items {
		names = append(names, fmt.Sprint(item["name"]))
	}
	if len(names) == 0 {
		t.Fatal("the broker reported no clusters")
	}

	body, _ := json.Marshal(map[string]any{"name": tenant, "allowed_clusters": names})
	if _, err := createTenant(request(ctx, sess, nil, nil, body)); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 60*time.Second)
		defer stop()
		_, _ = deleteNamespace(request(cleanup, sess, scope, nil, nil))
		_, _ = deleteTenant(request(cleanup, sess, map[string]string{"tenant": tenant}, nil, nil))
	}()

	body, _ = json.Marshal(map[string]any{"name": namespace})
	if _, err := createNamespace(request(ctx, sess, map[string]string{"tenant": tenant}, nil, body)); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	body, _ = json.Marshal(map[string]any{"time_minutes": 30, "size_mb": 64})
	if _, err := setNamespaceRetention(request(ctx, sess, scope, nil, body)); err != nil {
		t.Fatalf("set retention: %v", err)
	}
	policies, err := namespacePolicies(request(ctx, sess, scope, nil, nil))
	if err != nil {
		t.Fatalf("namespace policies: %v", err)
	}
	if policies.(row)["retention_policies"] == nil {
		t.Fatalf("retention was not applied: %#v", policies)
	}

	body, _ = json.Marshal(map[string]any{"name": topicName, "domain": persistentDomain, "partitions": 0})
	if _, err := createTopic(request(ctx, sess, scope, nil, body)); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	body, _ = json.Marshal(map[string]any{"name": subscription, "position": positionEarliest})
	if _, err := createSubscription(request(ctx, sess, topicArgs, nil, body)); err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	topics, err := listTopics(request(ctx, sess, scope, url.Values{"limit": {"50"}}, nil))
	if err != nil {
		t.Fatalf("list topics: %v", err)
	}
	found := false
	for _, item := range topics.(plugin.Page[row]).Items {
		if item["topic"] == topic {
			found = true
		}
	}
	if !found {
		t.Fatalf("created topic is missing from the listing: %#v", topics)
	}

	body, _ = json.Marshal(map[string]any{"payload": "hello-pulsar", "encoding": "string", "key": "k1", "properties": map[string]string{"source": "integration"}})
	produced, err := produceMessage(request(ctx, sess, topicArgs, nil, body))
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if produced.(row)["message_id"] == "" {
		t.Fatalf("produce returned no message id: %#v", produced)
	}

	messages, err := listMessages(request(ctx, sess, topicArgs, url.Values{"limit": {"5"}}, nil))
	if err != nil {
		t.Fatalf("browse messages: %v", err)
	}
	items := messages.(plugin.Page[row]).Items
	if len(items) == 0 || !strings.Contains(fmt.Sprint(items[0]["payload"]), "hello-pulsar") {
		t.Fatalf("examine did not return the published message: %#v", items)
	}

	peeked, err := listMessages(request(ctx, sess, subScope, url.Values{"limit": {"5"}}, nil))
	if err != nil {
		t.Fatalf("peek messages: %v", err)
	}
	if len(peeked.(plugin.Page[row]).Items) == 0 {
		t.Fatalf("peek through the subscription returned nothing")
	}

	subs, err := listSubscriptions(request(ctx, sess, topicArgs, nil, nil))
	if err != nil {
		t.Fatalf("list subscriptions: %v", err)
	}
	if len(subs.(plugin.Page[row]).Items) == 0 {
		t.Fatalf("no subscriptions on %s", topic)
	}
	if _, err := listProducers(request(ctx, sess, topicArgs, nil, nil)); err != nil {
		t.Fatalf("list producers: %v", err)
	}
	if _, err := listConsumers(request(ctx, sess, topicArgs, nil, nil)); err != nil {
		t.Fatalf("list consumers: %v", err)
	}
	if _, err := readTopicStats(request(ctx, sess, topicArgs, nil, nil)); err != nil {
		t.Fatalf("topic stats: %v", err)
	}
	if _, err := readTopicInternalStats(request(ctx, sess, topicArgs, nil, nil)); err != nil {
		t.Fatalf("topic internal stats: %v", err)
	}
	schema, err := readTopicSchema(request(ctx, sess, topicArgs, nil, nil))
	if err != nil {
		t.Fatalf("topic schema: %v", err)
	}
	if schema.(row)["registered"] != false {
		t.Fatalf("a schemaless topic should report registered=false: %#v", schema)
	}

	if _, err := clearBacklog(request(ctx, sess, subScope, nil, nil)); err != nil {
		t.Fatalf("clear backlog: %v", err)
	}
	body, _ = json.Marshal(map[string]any{"target": positionEarliest})
	if _, err := resetCursor(request(ctx, sess, subScope, nil, body)); err != nil {
		t.Fatalf("reset cursor: %v", err)
	}
	if _, err := deleteSubscription(request(ctx, sess, subScope, nil, nil)); err != nil {
		t.Fatalf("delete subscription: %v", err)
	}
	if _, err := deleteTopic(request(ctx, sess, topicArgs, nil, nil)); err != nil {
		t.Fatalf("delete topic: %v", err)
	}
}

func request(ctx context.Context, sess plugin.Session, params map[string]string, query url.Values, body []byte) *plugin.RequestContext {
	return plugin.NewRequestContext(ctx, plugin.User{}, sess, params, query, body)
}

// pulsarEndpoints returns the admin and broker URLs, starting a standalone
// container when the environment does not point at an existing broker.
func pulsarEndpoints(ctx context.Context, t *testing.T) (string, string) {
	t.Helper()
	if adminURL := strings.TrimSpace(os.Getenv("SHELLCN_PULSAR_ADMIN_URL")); adminURL != "" {
		serviceURL := strings.TrimSpace(os.Getenv("SHELLCN_PULSAR_SERVICE_URL"))
		if serviceURL == "" {
			serviceURL = defaultServiceURL
		}
		return adminURL, serviceURL
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_PULSAR_ADMIN_URL is not set")
	}
	name := "shellcn-pulsar-it-" + time.Now().UTC().Format("20060102150405")
	// The broker port must stay fixed because standalone advertises 127.0.0.1:6650
	// for topic lookups; the admin port is passed explicitly, so it takes an
	// ephemeral host port to avoid colliding with anything already on 8080.
	dockerRun(ctx, t, "run", "-d", "--rm", "--name", name,
		"-p", "127.0.0.1::8080", "-p", "127.0.0.1:6650:6650",
		pulsarImage,
		"bin/pulsar", "standalone", "--no-functions-worker", "--no-stream-storage", "-a", "127.0.0.1",
	)
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		_ = exec.CommandContext(cleanup, "docker", "rm", "-f", name).Run()
	})

	adminHost := strings.TrimSpace(dockerRun(ctx, t, "port", name, "8080/tcp"))
	if i := strings.IndexByte(adminHost, '\n'); i >= 0 {
		adminHost = strings.TrimSpace(adminHost[:i])
	}
	if !strings.Contains(adminHost, ":") {
		t.Fatalf("unexpected docker port output: %q", adminHost)
	}
	adminURL, serviceURL := "http://"+adminHost, "pulsar://127.0.0.1:6650"
	probe := &Session{
		http: httpClient(plugin.ConnectConfig{}, options{Timeout: 5 * time.Second}),
		opts: options{AdminURL: adminURL, Timeout: 5 * time.Second},
	}
	deadline := time.Now().Add(4 * time.Minute)
	for {
		if err := probe.HealthCheck(ctx); err == nil {
			return adminURL, serviceURL
		} else if time.Now().After(deadline) {
			t.Fatalf("Pulsar standalone did not become ready: %v", err)
		}
		time.Sleep(2 * time.Second)
	}
}

func dockerRun(ctx context.Context, t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}
