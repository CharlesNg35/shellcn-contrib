package kafka

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

func TestKafkaPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_KAFKA_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_KAFKA_INTEGRATION=1 to run against Kafka")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg := kafkaIntegrationConfig(ctx, t)
	sess, err := connect(ctx, plugin.ConnectConfig{Config: cfg, Net: kafkaDirectNetTransport{}})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	s := sess.(*Session)

	topic := "shellcn_it_" + time.Now().UTC().Format("20060102150405")
	create, _ := json.Marshal(map[string]any{"name": topic, "partitions": 1, "replication_factor": 1})
	if _, err := createTopic(plugin.NewRequestContext(ctx, plugin.User{}, sess, nil, nil, create)); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	defer func() {
		_, _ = deleteTopic(plugin.NewRequestContext(context.Background(), plugin.User{}, sess, map[string]string{"topic": topic}, nil, nil))
	}()
	if _, err := topicOverview(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"topic": topic}, nil, nil)); err != nil {
		t.Fatalf("topic overview: %v", err)
	}
	if _, err := listPartitions(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"topic": topic}, nil, nil)); err != nil {
		t.Fatalf("partitions: %v", err)
	}
	if _, err := topicConfig(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"topic": topic}, nil, nil)); err != nil {
		t.Fatalf("topic config: %v", err)
	}

	alter, _ := json.Marshal(map[string]any{"key": "retention.ms", "value": "86400000"})
	if _, err := alterTopicConfig(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"topic": topic}, nil, alter)); err != nil {
		t.Fatalf("alter topic config: %v", err)
	}
	if got := kafkaConfigValue(ctx, t, sess, topic, "retention.ms"); got != "86400000" {
		t.Fatalf("retention.ms after alter: got %q want %q", got, "86400000")
	}

	addParts, _ := json.Marshal(map[string]any{"count": 3})
	if _, err := addPartitions(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"topic": topic}, nil, addParts)); err != nil {
		t.Fatalf("add partitions: %v", err)
	}
	if n := kafkaPartitionCount(ctx, t, sess, topic); n != 3 {
		t.Fatalf("partition count after add: got %d want 3", n)
	}

	decrease, _ := json.Marshal(map[string]any{"count": 2})
	if _, err := addPartitions(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"topic": topic}, nil, decrease)); err == nil {
		t.Fatal("expected error decreasing partition count")
	}

	produce, _ := json.Marshal(map[string]any{"key": "k1", "value": "hello", "encoding": "string"})
	if _, err := produceMessage(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"topic": topic}, nil, produce)); err != nil {
		t.Fatalf("produce: %v", err)
	}
	messages := waitKafkaMessages(ctx, t, sess, topic)
	if len(messages) == 0 || messages[0]["value"] != "hello" {
		t.Fatalf("expected produced record, got %#v", messages)
	}

	group := "shellcn_it_group_" + time.Now().UTC().Format("20060102150405")
	offsets := map[string]map[int32]sarama.OffsetAndMetadata{topic: {0: {Offset: 1}}}
	if _, err := s.admin.AlterConsumerGroupOffsets(group, offsets, nil); err != nil {
		t.Fatalf("create group offset: %v", err)
	}
	if _, err := listGroups(plugin.NewRequestContext(ctx, plugin.User{}, sess, nil, nil, nil)); err != nil {
		t.Fatalf("list groups: %v", err)
	}
	if _, err := groupOverview(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"group": group}, nil, nil)); err != nil {
		t.Fatalf("group overview: %v", err)
	}
	if _, err := groupOffsets(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"group": group}, nil, nil)); err != nil {
		t.Fatalf("group offsets: %v", err)
	}

	reset, _ := json.Marshal(map[string]any{"target": "earliest"})
	if _, err := resetGroupOffsets(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"group": group}, nil, reset)); err != nil {
		t.Fatalf("reset offsets: %v", err)
	}
	res, err := groupOffsets(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"group": group}, nil, nil))
	if err != nil {
		t.Fatalf("group offsets after reset: %v", err)
	}
	for _, r := range res.(plugin.Page[row]).Items {
		if r["partition"].(int32) == 0 && r["committed_offset"].(int64) != 0 {
			t.Fatalf("expected committed offset reset to 0 (oldest), got %v", r["committed_offset"])
		}
	}
}

func kafkaIntegrationConfig(ctx context.Context, t *testing.T) map[string]any {
	t.Helper()
	brokers := os.Getenv("SHELLCN_KAFKA_BROKERS")
	if brokers == "" {
		brokers = startRedpandaContainer(ctx, t)
	}
	cfg := map[string]any{
		"brokers":       brokers,
		"client_id":     "shellcn-integration",
		"auth":          "none",
		"tls_mode":      "disable",
		"read_only":     false,
		"message_limit": 100,
		"timeout":       "10s",
	}
	if user := os.Getenv("SHELLCN_KAFKA_USERNAME"); user != "" {
		cfg["auth"] = "plain"
		cfg["username"] = user
		cfg["password"] = os.Getenv("SHELLCN_KAFKA_PASSWORD")
	}
	return cfg
}

func waitKafkaMessages(ctx context.Context, t *testing.T, sess plugin.Session, topic string) []row {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		res, err := listMessages(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"topic": topic}, url.Values{"limit": []string{"10"}}, nil))
		if err != nil {
			t.Fatalf("list messages: %v", err)
		}
		items := res.(plugin.Page[row]).Items
		if len(items) > 0 {
			return items
		}
		if time.Now().After(deadline) {
			return items
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func kafkaConfigValue(ctx context.Context, t *testing.T, sess plugin.Session, topic, key string) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	last := ""
	for {
		res, err := topicConfig(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"topic": topic}, nil, nil))
		if err != nil {
			t.Fatalf("topic config: %v", err)
		}
		for _, r := range res.(plugin.Page[row]).Items {
			if r["name"] == key {
				last, _ = r["value"].(string)
			}
		}
		if last == "86400000" || time.Now().After(deadline) {
			return last
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func kafkaPartitionCount(ctx context.Context, t *testing.T, sess plugin.Session, topic string) int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		s := sess.(*Session)
		_ = s.client.RefreshMetadata(topic)
		res, err := listPartitions(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"topic": topic}, nil, nil))
		if err != nil {
			t.Fatalf("partitions: %v", err)
		}
		n := len(res.(plugin.Page[row]).Items)
		if n >= 3 || time.Now().After(deadline) {
			return n
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func startRedpandaContainer(ctx context.Context, t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_KAFKA_BROKERS is not set")
	}
	port := freePort(t)
	name := "shellcn-redpanda-it-" + time.Now().UTC().Format("20060102150405")
	run(ctx, t, "docker", "run", "-d", "--rm", "--name", name,
		"-p", "127.0.0.1:"+port+":9092",
		"redpandadata/redpanda:v24.3.6",
		"redpanda", "start",
		"--overprovisioned",
		"--smp", "1",
		"--memory", "512M",
		"--reserve-memory", "0M",
		"--node-id", "0",
		"--check=false",
		"--kafka-addr", "PLAINTEXT://0.0.0.0:9092",
		"--advertise-kafka-addr", "PLAINTEXT://127.0.0.1:"+port,
	)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})
	brokers := "127.0.0.1:" + port
	cfg := map[string]any{"brokers": brokers, "client_id": "shellcn-integration", "auth": "none", "tls_mode": "disable", "read_only": false, "message_limit": 100, "timeout": "10s"}
	deadline := time.Now().Add(60 * time.Second)
	for {
		sess, err := connect(ctx, plugin.ConnectConfig{Config: cfg, Net: kafkaDirectNetTransport{}})
		if err == nil {
			_ = sess.Close()
			return brokers
		}
		if time.Now().After(deadline) {
			t.Fatalf("Redpanda container did not become ready: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

type kafkaDirectNetTransport struct{}

func (kafkaDirectNetTransport) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, network, addr)
}

func (kafkaDirectNetTransport) HTTP() (string, http.RoundTripper, bool) {
	return "", nil, false
}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate port: %v", err)
	}
	defer func() { _ = ln.Close() }()
	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatalf("parse port: %v", err)
	}
	return port
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
