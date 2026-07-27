package consul

import (
	"context"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
	sdktest "github.com/charlesng35/shellcn/sdk/plugintest"
)

const consulImage = "hashicorp/consul:1.20"

func TestConsulPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_CONSUL_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_CONSUL_INTEGRATION=1 to run against consul")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	config := integrationConfig(ctx, t)
	config["read_only"] = false

	sess, err := connect(ctx, plugin.ConnectConfig{Config: config, Net: sdktest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()

	user := plugin.User{ID: "u1", Username: "operator"}
	rc := func(params map[string]string, body []byte) *plugin.RequestContext {
		return plugin.NewRequestContext(ctx, user, sess, params, nil, body)
	}

	const key = "shellcn/it/alpha"
	if _, err := kvPut(rc(nil, mustJSON(t, map[string]any{"key": key, "value": "v1"}))); err != nil {
		t.Fatalf("kv.put: %v", err)
	}

	listed, err := kvList(rc(map[string]string{"prefix": "shellcn/"}, nil))
	if err != nil {
		t.Fatalf("kv.list: %v", err)
	}
	if !containsKey(listed.(plugin.Page[row]).Items, key) {
		t.Fatalf("listed keys missing %q: %#v", key, listed)
	}

	detail, err := kvRead(rc(map[string]string{"key": key}, nil))
	if err != nil {
		t.Fatalf("kv.read: %v", err)
	}
	if detail.(row)["value"] != "v1" {
		t.Fatalf("unexpected value: %#v", detail)
	}

	entries, err := kvEntries(rc(map[string]string{"prefix": "shellcn/it/"}, nil))
	if err != nil {
		t.Fatalf("kv.entries: %v", err)
	}
	if page := entries.(plugin.Page[row]); len(page.Items) == 0 || page.Items[0]["size"] != 2 {
		t.Fatalf("entry metadata was not resolved: %#v", page.Items)
	}

	index := detail.(row)["modify_index"].(uint64)
	if _, err := kvWrite(rc(map[string]string{"key": key}, mustJSON(t, map[string]any{"value": "v2", "cas": index}))); err != nil {
		t.Fatalf("check-and-set write: %v", err)
	}
	if _, err := kvWrite(rc(map[string]string{"key": key}, mustJSON(t, map[string]any{"value": "v3", "cas": index}))); err == nil {
		t.Fatal("a stale check-and-set write should conflict")
	}

	services, err := servicesList(rc(nil, nil))
	if err != nil {
		t.Fatalf("services.list: %v", err)
	}
	if !containsField(services.(plugin.Page[row]).Items, "name", "consul") {
		t.Fatalf("the consul service should be registered: %#v", services)
	}

	nodes, err := nodesList(rc(nil, nil))
	if err != nil {
		t.Fatalf("nodes.list: %v", err)
	}
	nodeRows := nodes.(plugin.Page[row]).Items
	if len(nodeRows) == 0 {
		t.Fatal("expected at least one catalog node")
	}
	node, _ := nodeRows[0]["name"].(string)

	if _, err := nodeChecks(rc(map[string]string{"node": node}, nil)); err != nil {
		t.Fatalf("node.checks: %v", err)
	}
	if _, err := serviceRead(rc(map[string]string{"service": "consul"}, nil)); err != nil {
		t.Fatalf("service.read: %v", err)
	}
	if _, err := serviceInstances(rc(map[string]string{"service": "consul"}, nil)); err != nil {
		t.Fatalf("service.instances: %v", err)
	}
	if _, err := checksList(rc(nil, nil)); err != nil {
		t.Fatalf("checks.list: %v", err)
	}
	if _, err := agentSelf(rc(nil, nil)); err != nil {
		t.Fatalf("agent.self: %v", err)
	}
	settings, err := agentConfig(rc(nil, nil))
	if err != nil {
		t.Fatalf("agent.config: %v", err)
	}
	if len(settings.(plugin.Page[row]).Items) == 0 {
		t.Fatal("the agent should report its runtime configuration")
	}
	// A -dev agent runs without ACLs, so the ACL panels must degrade instead of failing.
	if _, err := aclTokensList(rc(nil, nil)); err != nil {
		t.Fatalf("acl.tokens.list should degrade when ACLs are disabled: %v", err)
	}
	if _, err := aclPoliciesList(rc(nil, nil)); err != nil {
		t.Fatalf("acl.policies.list should degrade when ACLs are disabled: %v", err)
	}
	if _, err := membersList(rc(nil, nil)); err != nil {
		t.Fatalf("members.list: %v", err)
	}
	if _, err := datacentersList(rc(nil, nil)); err != nil {
		t.Fatalf("datacenters.list: %v", err)
	}
	if _, err := sessionsList(rc(nil, nil)); err != nil {
		t.Fatalf("sessions.list: %v", err)
	}
	if _, err := kvTree(rc(nil, nil)); err != nil {
		t.Fatalf("kv.tree: %v", err)
	}

	if _, err := kvDelete(rc(map[string]string{"key": key}, nil)); err != nil {
		t.Fatalf("kv.delete: %v", err)
	}
	if _, err := kvRead(rc(map[string]string{"key": key}, nil)); err == nil {
		t.Fatal("expected the key to be gone after delete")
	}
}

func containsKey(items []row, key string) bool {
	return containsField(items, "key", key)
}

func containsField(items []row, field, want string) bool {
	for _, item := range items {
		if value, _ := item[field].(string); value == want {
			return true
		}
	}
	return false
}

func integrationConfig(ctx context.Context, t *testing.T) map[string]any {
	t.Helper()
	if raw := os.Getenv("SHELLCN_CONSUL_ADDR"); raw != "" {
		host, portText, err := net.SplitHostPort(raw)
		if err != nil {
			t.Fatalf("parse SHELLCN_CONSUL_ADDR: %v", err)
		}
		port, err := strconv.Atoi(portText)
		if err != nil {
			t.Fatalf("parse consul port: %v", err)
		}
		return map[string]any{"host": host, "port": port, "auth": authNone, "tls_mode": tlsDisable}
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_CONSUL_ADDR is not set")
	}
	name := "shellcn-consul-it-" + time.Now().UTC().Format("20060102150405")
	runCommand(ctx, t, "docker", "run", "-d", "--rm", "--name", name,
		"-p", "127.0.0.1::8500", consulImage,
		"agent", "-dev", "-client", "0.0.0.0",
	)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})
	out := runCommand(ctx, t, "docker", "port", name, "8500/tcp")
	host, portText, err := net.SplitHostPort(strings.TrimSpace(strings.Split(out, "\n")[0]))
	if err != nil {
		t.Fatalf("unexpected docker port output: %q", out)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse docker port %q: %v", portText, err)
	}
	config := map[string]any{"host": host, "port": port, "auth": authNone, "tls_mode": tlsDisable}
	deadline := time.Now().Add(120 * time.Second)
	for {
		sess, err := connect(ctx, plugin.ConnectConfig{Config: config, Net: sdktest.DirectTransport()})
		if err == nil {
			_ = sess.Close()
			return config
		}
		if time.Now().After(deadline) {
			t.Fatalf("consul did not become ready: %v", err)
		}
		time.Sleep(time.Second)
	}
}

func runCommand(ctx context.Context, t *testing.T, name string, args ...string) string {
	t.Helper()
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
	return string(out)
}
