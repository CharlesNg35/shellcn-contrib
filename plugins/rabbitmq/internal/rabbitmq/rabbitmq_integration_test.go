package rabbitmq

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

func TestRabbitMQPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_RABBITMQ_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_RABBITMQ_INTEGRATION=1 to run against RabbitMQ")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	cfg := rabbitIntegrationConfig(ctx, t)
	sess, err := connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()

	name := "shellcn-it-" + time.Now().UTC().Format("20060102150405")
	body, _ := json.Marshal(map[string]any{"vhost": cfg["vhost"], "name": name, "durable": false, "auto_delete": true})
	if _, err := createQueue(plugin.NewRequestContext(ctx, plugin.User{}, sess, nil, nil, body)); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	defer func() {
		_, _ = deleteQueue(plugin.NewRequestContext(context.Background(), plugin.User{}, sess, map[string]string{"vhost": cfg["vhost"].(string), "queue": name}, nil, nil))
	}()
	if _, err := purgeQueue(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"vhost": cfg["vhost"].(string), "queue": name}, nil, nil)); err != nil {
		t.Fatalf("purge empty queue: %v", err)
	}

	pub, _ := json.Marshal(map[string]any{"payload": "hello", "payload_encoding": "string"})
	if _, err := publishQueueMessage(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"vhost": cfg["vhost"].(string), "queue": name}, nil, pub)); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, err := queueOverview(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"vhost": cfg["vhost"].(string), "queue": name}, nil, nil)); err != nil {
		t.Fatalf("queue overview: %v", err)
	}
	if _, err := queueBindings(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"vhost": cfg["vhost"].(string), "queue": name}, nil, nil)); err != nil {
		t.Fatalf("queue bindings: %v", err)
	}

	bindBody, _ := json.Marshal(map[string]any{"source": "amq.direct", "routing_key": "shellcn-rk"})
	if _, err := createBinding(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"vhost": cfg["vhost"].(string), "queue": name}, nil, bindBody)); err != nil {
		t.Fatalf("create binding: %v", err)
	}
	bres, err := queueBindings(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"vhost": cfg["vhost"].(string), "queue": name}, nil, nil))
	if err != nil {
		t.Fatalf("list bindings after create: %v", err)
	}
	var spec string
	for _, b := range bres.(plugin.Page[row]).Items {
		if b["source"] == "amq.direct" {
			spec = b["ref"].(plugin.ResourceRef).UID
		}
	}
	if spec == "" {
		t.Fatalf("created binding not listed: %#v", bres.(plugin.Page[row]).Items)
	}
	if _, err := deleteBinding(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"spec": spec}, nil, nil)); err != nil {
		t.Fatalf("delete binding: %v", err)
	}
	res, err := queueMessages(plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"vhost": cfg["vhost"].(string), "queue": name}, url.Values{"limit": []string{"10"}}, nil))
	if err != nil {
		t.Fatalf("messages: %v", err)
	}
	messages := res.(plugin.Page[row]).Items
	if len(messages) == 0 || messages[0]["payload"] != "hello" {
		t.Fatalf("expected published message, got %#v", messages)
	}
}

func rabbitIntegrationConfig(ctx context.Context, t *testing.T) map[string]any {
	t.Helper()
	managementURL := os.Getenv("SHELLCN_RABBITMQ_MANAGEMENT_URL")
	if managementURL == "" {
		managementURL = startRabbitMQContainer(ctx, t)
	}
	vhost := os.Getenv("SHELLCN_RABBITMQ_VHOST")
	if vhost == "" {
		vhost = "/"
	}
	cfg := map[string]any{
		"management_url": managementURL,
		"vhost":          vhost,
		"auth":           "password",
		"username":       os.Getenv("SHELLCN_RABBITMQ_USERNAME"),
		"password":       os.Getenv("SHELLCN_RABBITMQ_PASSWORD"),
		"tls_mode":       "disable",
		"read_only":      false,
		"message_limit":  100,
		"timeout":        "5s",
	}
	if cfg["username"] == "" {
		cfg["username"] = "guest"
	}
	if cfg["password"] == "" {
		cfg["password"] = "guest"
	}
	return cfg
}

func startRabbitMQContainer(ctx context.Context, t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_RABBITMQ_MANAGEMENT_URL is not set")
	}
	name := "shellcn-rabbitmq-it-" + time.Now().UTC().Format("20060102150405")
	run(ctx, t, "docker", "run", "-d", "--rm", "--name", name, "-p", "127.0.0.1::15672", "rabbitmq:3-management-alpine")
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})
	out := run(ctx, t, "docker", "port", name, "15672/tcp")
	host, port, err := net.SplitHostPort(strings.TrimSpace(out))
	if err != nil {
		t.Fatalf("unexpected docker port output: %q", out)
	}
	managementURL := "http://" + net.JoinHostPort(host, port)
	cfg := map[string]any{"management_url": managementURL, "vhost": "/", "auth": "password", "username": "guest", "password": "guest", "tls_mode": "disable", "timeout": "5s", "message_limit": 100}
	deadline := time.Now().Add(45 * time.Second)
	for {
		sess, err := connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
		if err == nil {
			_ = sess.Close()
			return managementURL
		}
		if time.Now().After(deadline) {
			t.Fatalf("RabbitMQ container did not become ready: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
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
