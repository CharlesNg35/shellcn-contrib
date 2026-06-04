package nats

import (
	"context"
	"encoding/json"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestNATSPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_NATS_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_NATS_INTEGRATION=1 to run against NATS")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	cfg := natsIntegrationConfig(ctx, t)
	sess, err := connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()

	stream := "SHELLCN_IT_" + time.Now().UTC().Format("20060102150405")
	subject := "shellcn.it." + stream
	create, _ := json.Marshal(map[string]any{"name": stream, "subjects": subject, "storage": "memory", "replicas": 1, "max_msgs": 100, "max_bytes": -1})
	if _, err := createStream(plugin.NewRequestContext(ctx, plugin.User{}, sess, nil, nil, create)); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer func() {
		_, _ = deleteStream(plugin.NewRequestContext(context.Background(), plugin.User{}, sess, map[string]string{"stream": stream}, nil, nil))
	}()
	if _, err := listStreams(plugin.NewRequestContext(ctx, plugin.User{}, sess, nil, nil, nil)); err != nil {
		t.Fatalf("list streams: %v", err)
	}
	if _, err := streamOverview(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"stream": stream}, nil, nil)); err != nil {
		t.Fatalf("stream overview: %v", err)
	}

	update, _ := json.Marshal(map[string]any{"max_msgs": 250})
	if _, err := updateStream(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"stream": stream}, nil, update)); err != nil {
		t.Fatalf("update stream: %v", err)
	}
	updatedInfo, err := sess.(*Session).js.StreamInfo(stream)
	if err != nil {
		t.Fatalf("read back stream: %v", err)
	}
	if updatedInfo.Config.MaxMsgs != 250 {
		t.Fatalf("max_msgs after update: got %d want 250", updatedInfo.Config.MaxMsgs)
	}

	pub, _ := json.Marshal(map[string]any{"subject": subject, "data": "hello", "encoding": "string", "jetstream": true})
	if _, err := publishMessage(plugin.NewRequestContext(ctx, plugin.User{}, sess, nil, nil, pub)); err != nil {
		t.Fatalf("publish: %v", err)
	}
	res, err := listMessages(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"stream": stream}, url.Values{"limit": []string{"10"}}, nil))
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	messages := res.(plugin.Page[row]).Items
	if len(messages) == 0 || messages[0]["data"] != "hello" {
		t.Fatalf("expected published message, got %#v", messages)
	}
	createConsumerBody, _ := json.Marshal(map[string]any{"name": "shellcn-it-consumer", "filter_subject": subject, "ack_policy": "explicit"})
	if _, err := createConsumer(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"stream": stream}, nil, createConsumerBody)); err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	if _, err := listConsumers(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"stream": stream}, nil, nil)); err != nil {
		t.Fatalf("list consumers: %v", err)
	}
	if _, err := consumerOverview(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"stream": stream, "consumer": "shellcn-it-consumer"}, nil, nil)); err != nil {
		t.Fatalf("consumer overview: %v", err)
	}
	if _, err := deleteConsumer(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"stream": stream, "consumer": "shellcn-it-consumer"}, nil, nil)); err != nil {
		t.Fatalf("delete consumer: %v", err)
	}
	if _, err := deleteMessage(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"stream": stream, "sequence": "1"}, nil, nil)); err != nil {
		t.Fatalf("delete message: %v", err)
	}
	if _, err := purgeStream(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"stream": stream}, nil, nil)); err != nil {
		t.Fatalf("purge stream: %v", err)
	}
}

func natsIntegrationConfig(ctx context.Context, t *testing.T) map[string]any {
	t.Helper()
	urls := os.Getenv("SHELLCN_NATS_URLS")
	if urls == "" {
		urls = startNATSContainer(ctx, t)
	}
	cfg := map[string]any{
		"urls":          urls,
		"name":          "shellcn-integration",
		"auth":          "none",
		"tls_mode":      "disable",
		"read_only":     false,
		"message_limit": 100,
		"timeout":       "5s",
	}
	if token := os.Getenv("SHELLCN_NATS_TOKEN"); token != "" {
		cfg["auth"] = "token"
		cfg["token"] = token
	}
	return cfg
}

func startNATSContainer(ctx context.Context, t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_NATS_URLS is not set")
	}
	name := "shellcn-nats-it-" + time.Now().UTC().Format("20060102150405")
	run(ctx, t, "docker", "run", "-d", "--rm", "--name", name, "-p", "127.0.0.1::4222", "nats:2-alpine", "-js")
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})
	out := run(ctx, t, "docker", "port", name, "4222/tcp")
	host, port, err := net.SplitHostPort(strings.TrimSpace(out))
	if err != nil {
		t.Fatalf("unexpected docker port output: %q", out)
	}
	urls := "nats://" + net.JoinHostPort(host, port)
	cfg := map[string]any{"urls": urls, "name": "shellcn-integration", "auth": "none", "tls_mode": "disable", "read_only": false, "message_limit": 100, "timeout": "5s"}
	deadline := time.Now().Add(30 * time.Second)
	for {
		sess, err := connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
		if err == nil {
			_ = sess.Close()
			return urls
		}
		if time.Now().After(deadline) {
			t.Fatalf("NATS container did not become ready: %v", err)
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
