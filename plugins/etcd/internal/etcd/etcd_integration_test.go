package etcd

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

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestEtcdPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_ETCD_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_ETCD_INTEGRATION=1 to run against etcd")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	cfg := integrationConfig(ctx, t)
	cfg["read_only"] = false

	sess, err := connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	s := sess.(*Session)

	user := plugin.User{ID: "u1", Username: "admin"}
	const key = "/shellcn/it/alpha"

	writeRC := plugin.NewRequestContext(ctx, user, s, map[string]string{"key": key}, nil, mustJSON(t, map[string]any{"value": "v1"}))
	if _, err := writeKey(writeRC); err != nil {
		t.Fatalf("write key: %v", err)
	}

	listRC := plugin.NewRequestContext(ctx, user, s, nil, nil, nil)
	listed, err := listKeys(listRC)
	if err != nil {
		t.Fatalf("list keys: %v", err)
	}
	pageResult := listed.(plugin.Page[keyEntry])
	if !containsKey(pageResult.Items, key) {
		t.Fatalf("listed keys missing %q: %+v", key, pageResult.Items)
	}

	readRC := plugin.NewRequestContext(ctx, user, s, map[string]string{"key": key}, nil, nil)
	read, err := readKey(readRC)
	if err != nil {
		t.Fatalf("read key: %v", err)
	}
	if detail := read.(keyDetail); detail.Value != "v1" {
		t.Fatalf("unexpected value: %+v", detail)
	}

	if _, err := statusRoute(plugin.NewRequestContext(ctx, user, s, nil, nil, nil)); err != nil {
		t.Fatalf("status: %v", err)
	}

	members, err := listMembers(plugin.NewRequestContext(ctx, user, s, nil, nil, nil))
	if err != nil {
		t.Fatalf("members: %v", err)
	}
	if len(members.(plugin.Page[memberEntry]).Items) == 0 {
		t.Fatal("expected at least one member")
	}

	granted, err := grantLease(plugin.NewRequestContext(ctx, user, s, nil, nil, mustJSON(t, map[string]any{"ttl": 60})))
	if err != nil {
		t.Fatalf("grant lease: %v", err)
	}
	leaseID, _ := granted.(map[string]any)["id"].(string)
	if leaseID == "" {
		t.Fatalf("grant lease returned no id: %+v", granted)
	}
	leases, err := listLeases(plugin.NewRequestContext(ctx, user, s, nil, nil, nil))
	if err != nil {
		t.Fatalf("list leases: %v", err)
	}
	if !containsLease(leases.(plugin.Page[leaseEntry]).Items, leaseID) {
		t.Fatalf("granted lease %q not listed", leaseID)
	}
	if _, err := revokeLease(plugin.NewRequestContext(ctx, user, s, map[string]string{"id": leaseID}, nil, nil)); err != nil {
		t.Fatalf("revoke lease: %v", err)
	}

	if _, err := deleteKey(plugin.NewRequestContext(ctx, user, s, map[string]string{"key": key}, nil, nil)); err != nil {
		t.Fatalf("delete key: %v", err)
	}
	if _, err := readKey(plugin.NewRequestContext(ctx, user, s, map[string]string{"key": key}, nil, nil)); err == nil {
		t.Fatal("expected read to fail after delete")
	}
}

func containsKey(items []keyEntry, key string) bool {
	for _, item := range items {
		if item.Key == key {
			return true
		}
	}
	return false
}

func containsLease(items []leaseEntry, id string) bool {
	for _, item := range items {
		if item.ID == id {
			return true
		}
	}
	return false
}

func integrationConfig(ctx context.Context, t *testing.T) map[string]any {
	t.Helper()
	if raw := os.Getenv("SHELLCN_ETCD_ADDR"); raw != "" {
		host, portText, err := net.SplitHostPort(raw)
		if err != nil {
			t.Fatalf("parse SHELLCN_ETCD_ADDR: %v", err)
		}
		port, err := strconv.Atoi(portText)
		if err != nil {
			t.Fatalf("parse etcd port: %v", err)
		}
		return map[string]any{"host": host, "port": port, "auth": authNone, "tls_mode": "disable"}
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_ETCD_ADDR is not set")
	}
	name := "shellcn-etcd-it-" + time.Now().UTC().Format("20060102150405")
	run(ctx, t, "docker", "run", "-d", "--rm", "--name", name,
		"-p", "127.0.0.1::2379",
		"quay.io/coreos/etcd:v3.5.17",
		"/usr/local/bin/etcd", "--name", "it",
		"--advertise-client-urls", "http://0.0.0.0:2379",
		"--listen-client-urls", "http://0.0.0.0:2379",
	)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})
	out := run(ctx, t, "docker", "port", name, "2379/tcp")
	host, portText, err := net.SplitHostPort(strings.TrimSpace(strings.Split(out, "\n")[0]))
	if err != nil {
		t.Fatalf("unexpected docker port output: %q", out)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse docker port %q: %v", portText, err)
	}
	cfg := map[string]any{"host": host, "port": port, "auth": authNone, "tls_mode": "disable"}
	deadline := time.Now().Add(90 * time.Second)
	for {
		sess, err := connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
		if err == nil {
			_ = sess.Close()
			return cfg
		}
		if time.Now().After(deadline) {
			t.Fatalf("etcd did not become ready: %v", err)
		}
		time.Sleep(time.Second)
	}
}

func run(ctx context.Context, t *testing.T, name string, args ...string) string {
	t.Helper()
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
	return string(out)
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}
