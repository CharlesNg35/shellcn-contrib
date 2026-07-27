package vault

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestVaultPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_VAULT_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_VAULT_INTEGRATION=1 to run against Vault")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	cfg := vaultIntegrationConfig(ctx, t)
	sess, err := connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()

	call := func(handler plugin.Handler, params map[string]string, body any) any {
		t.Helper()
		var raw []byte
		if body != nil {
			raw, err = json.Marshal(body)
			if err != nil {
				t.Fatalf("marshal body: %v", err)
			}
		}
		out, callErr := handler(plugin.NewRequestContext(ctx, plugin.User{}, sess, params, url.Values{"limit": {"100"}}, raw))
		if callErr != nil {
			t.Fatalf("call: %v", callErr)
		}
		return out
	}

	system := call(systemList, nil, nil).(plugin.Page[row])
	if len(system.Items) != 1 || system.Items[0]["state"] != "active" {
		t.Fatalf("dev-mode vault should be active: %#v", system.Items)
	}
	call(systemHealth, nil, nil)
	call(systemSeal, nil, nil)

	mounts := call(listMounts, nil, nil).(plugin.Page[row])
	kv := ""
	for _, item := range mounts.Items {
		if item["kv"] == true && item["kv_version"] == 2 {
			kv = fmt.Sprint(item["path"])
			break
		}
	}
	if kv == "" {
		t.Fatalf("dev mode should mount a KV v2 engine: %#v", mounts.Items)
	}
	call(readMount, map[string]string{"mount": kv}, nil)
	call(readMountConfig, map[string]string{"mount": kv}, nil)
	call(listAuth, nil, nil)
	call(readAuth, map[string]string{"mount": "token"}, nil)
	call(treeMounts, nil, nil)
	call(treeAuth, nil, nil)
	call(treePaths, map[string]string{"mount": kv}, nil)

	folder := "shellcn-it-" + time.Now().UTC().Format("20060102150405")
	key := kv + folder + "/app"
	keyParams := map[string]string{"key": key}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = purgeSecret(plugin.NewRequestContext(cleanupCtx, plugin.User{}, sess, keyParams, nil, nil))
	}()

	call(putSecret, nil, map[string]any{"key": key, "data": map[string]any{"username": "root", "password": "first"}})
	call(writeSecret, keyParams, map[string]any{"value": `{"username":"root","password":"second"}`})

	secret := call(readSecret, keyParams, nil).(row)
	if secret["version"] != 2 {
		t.Fatalf("the second write should create version 2: %#v", secret)
	}
	if value, ok := secret["value"].(map[string]any); !ok || value["password"] != "second" {
		t.Fatalf("unexpected secret value: %#v", secret["value"])
	}

	listed := call(listSecrets, map[string]string{"mount": kv, "path": folder}, nil).(scanPage)
	if len(listed.Items) != 1 || listed.Items[0]["key"] != key {
		t.Fatalf("folder listing should find the new secret: %#v", listed.Items)
	}

	versions := call(listSecretVersions, keyParams, nil).(plugin.Page[row])
	if len(versions.Items) != 2 {
		t.Fatalf("expected two versions: %#v", versions.Items)
	}
	metadata := call(readSecretMetadata, keyParams, nil).(row)
	if metadata["current_version"] != 2 {
		t.Fatalf("unexpected metadata: %#v", metadata)
	}

	call(deleteSecret, keyParams, nil)
	if _, err := readSecret(plugin.NewRequestContext(ctx, plugin.User{}, sess, keyParams, nil, nil)); err != nil {
		t.Fatalf("a soft-deleted secret still resolves its metadata: %v", err)
	}
	call(undeleteSecret, keyParams, map[string]any{"versions": "2"})
	restored := call(readSecret, keyParams, nil).(row)
	if value, ok := restored["value"].(map[string]any); !ok || value["password"] != "second" {
		t.Fatalf("undelete did not restore the data: %#v", restored["value"])
	}
	call(destroySecret, keyParams, map[string]any{"versions": "1"})
	call(purgeSecret, keyParams, nil)

	// cubbyhole is the unversioned engine every dev server mounts, so it covers
	// the KV v1 code path against a real server.
	v1Key := "cubbyhole/" + folder
	v1Params := map[string]string{"key": v1Key}
	call(writeSecret, v1Params, map[string]any{"value": map[string]any{"token": "legacy"}})
	v1Secret := call(readSecret, v1Params, nil).(row)
	if value, ok := v1Secret["value"].(map[string]any); !ok || value["token"] != "legacy" {
		t.Fatalf("unexpected kv v1 value: %#v", v1Secret["value"])
	}
	if items := call(listSecretVersions, v1Params, nil).(plugin.Page[row]).Items; len(items) != 0 {
		t.Fatalf("kv v1 keeps no versions: %#v", items)
	}
	if _, err := readSecretMetadata(plugin.NewRequestContext(ctx, plugin.User{}, sess, v1Params, nil, nil)); err == nil {
		t.Fatal("kv v1 should report that secret metadata is unsupported")
	}
	call(deleteSecret, v1Params, nil)

	policy := "shellcn-it-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	call(createPolicy, nil, map[string]any{"name": policy, "policy": "path \"" + kv + "data/*\" {\n  capabilities = [\"read\"]\n}\n"})
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = deletePolicy(plugin.NewRequestContext(cleanupCtx, plugin.User{}, sess, map[string]string{"name": policy}, nil, nil))
	}()
	policies := call(listPolicies, nil, nil).(plugin.Page[row])
	if !hasRow(policies.Items, "name", policy) {
		t.Fatalf("the new policy should be listed: %#v", policies.Items)
	}
	hcl := call(readPolicy, map[string]string{"name": policy}, nil).(string)
	if !strings.Contains(hcl, "capabilities") {
		t.Fatalf("unexpected policy document: %q", hcl)
	}
	call(writePolicy, map[string]string{"name": policy}, map[string]any{"content": "path \"" + kv + "data/*\" {\n  capabilities = [\"list\", \"read\"]\n}\n"})
	call(deletePolicy, map[string]string{"name": policy}, nil)

	created := call(createToken, nil, map[string]any{"display_name": "shellcn-it", "policies": "default", "ttl": "10m", "num_uses": 3}).(row)
	accessor := fmt.Sprint(created["accessor"])
	if accessor == "" {
		t.Fatalf("token creation returned no accessor: %#v", created)
	}
	tokens := call(listTokens, nil, nil).(plugin.Page[row])
	if !hasRow(tokens.Items, "accessor", accessor) {
		t.Fatalf("the new accessor should be listed: %#v", tokens.Items)
	}
	token := call(readToken, map[string]string{"accessor": accessor}, nil).(row)
	if token["display_name"] == "" {
		t.Fatalf("unexpected token detail: %#v", token)
	}
	call(revokeToken, map[string]string{"accessor": accessor}, nil)

	call(listLeases, nil, nil)
	call(listNamespaces, nil, nil)
	call(listAudit, nil, nil)
}

func vaultIntegrationConfig(ctx context.Context, t *testing.T) map[string]any {
	t.Helper()
	address := os.Getenv("SHELLCN_VAULT_ADDR")
	token := os.Getenv("SHELLCN_VAULT_TOKEN")
	if address == "" {
		address, token = startVaultContainer(ctx, t)
	}
	if token == "" {
		token = "root"
	}
	host, port := splitVaultAddress(t, address)
	return map[string]any{
		"host": host, "port": port, "tls_mode": tlsDisable,
		"auth": authToken, "token": token, "read_only": false, "timeout": "15s",
	}
}

func startVaultContainer(ctx context.Context, t *testing.T) (string, string) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_VAULT_ADDR is not set")
	}
	name := "shellcn-vault-it-" + time.Now().UTC().Format("20060102150405")
	runDocker(ctx, t, "run", "-d", "--rm", "--name", name, "--cap-add=IPC_LOCK",
		"-e", "VAULT_DEV_ROOT_TOKEN_ID=root", "-e", "VAULT_DEV_LISTEN_ADDRESS=0.0.0.0:8200",
		"-p", "127.0.0.1::8200", "hashicorp/vault")
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})
	address := strings.TrimSpace(runDocker(ctx, t, "port", name, "8200/tcp"))
	if i := strings.IndexByte(address, '\n'); i >= 0 {
		address = strings.TrimSpace(address[:i])
	}
	host, port := splitVaultAddress(t, address)
	cfg := map[string]any{"host": host, "port": port, "tls_mode": tlsDisable, "auth": authToken, "token": "root", "read_only": false, "timeout": "5s"}
	deadline := time.Now().Add(60 * time.Second)
	for {
		sess, err := connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
		if err == nil {
			_ = sess.Close()
			return address, "root"
		}
		if time.Now().After(deadline) {
			t.Fatalf("vault container did not become ready: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func splitVaultAddress(t *testing.T, address string) (string, int) {
	t.Helper()
	address = strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(address), "http://"), "https://")
	host, rawPort, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatalf("unexpected vault address %q: %v", address, err)
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil {
		t.Fatalf("unexpected vault port in %q: %v", address, err)
	}
	return host, port
}

func runDocker(ctx context.Context, t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}
