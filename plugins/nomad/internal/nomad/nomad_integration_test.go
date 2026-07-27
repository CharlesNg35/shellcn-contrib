package nomad

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
	sdktest "github.com/charlesng35/shellcn/sdk/plugintest"
)

const integrationJobHCL = `job "shellcn-it" {
  type = "batch"

  group "app" {
    count = 1

    task "hello" {
      driver = "raw_exec"

      config {
        command = "/bin/sh"
        args    = ["-c", "echo shellcn"]
      }

      resources {
        cpu    = 100
        memory = 64
      }
    }
  }
}
`

func TestNomadPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_NOMAD_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_NOMAD_INTEGRATION=1 to run against Nomad")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	cfg := nomadIntegrationConfig(ctx, t)
	sess, err := connect(ctx, plugin.ConnectConfig{Config: cfg, Net: sdktest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()

	call := func(name string, handler plugin.Handler, params map[string]string, body []byte) any {
		t.Helper()
		out, err := handler(plugin.NewRequestContext(ctx, plugin.User{}, sess, params, nil, body))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return out
	}

	call("cluster overview", clusterOverview, nil, nil)
	call("members", listMembers, nil, nil)
	call("regions", listRegions, nil, nil)
	call("namespaces", listNamespaces, nil, nil)
	call("namespace scope", namespaceScope, nil, nil)
	call("tree namespaces", treeNamespaces, nil, nil)

	nodes := call("nodes", listNodes, nil, nil).(listPage)
	if len(nodes.Items) == 0 {
		t.Fatal("a dev agent should register one client")
	}
	nodeID := fmt.Sprint(nodes.Items[0]["id"])
	call("node overview", nodeOverview, map[string]string{"node": nodeID}, nil)
	call("node allocations", listAllocations, map[string]string{"node": nodeID}, nil)

	spec, _ := json.Marshal(map[string]any{"content": integrationJobHCL})
	call("plan", planJob, nil, spec)
	call("submit", submitJob, nil, spec)
	jobParams := map[string]string{"job": "shellcn-it"}
	defer func() {
		cleanupCtx, done := context.WithTimeout(context.Background(), 30*time.Second)
		defer done()
		_, _ = purgeJob(plugin.NewRequestContext(cleanupCtx, plugin.User{}, sess, jobParams, nil, nil))
	}()

	waitFor(ctx, t, "job to appear", func() bool {
		page := call("jobs", listJobs, nil, nil).(listPage)
		for _, item := range page.Items {
			if item["id"] == "shellcn-it" {
				return true
			}
		}
		return false
	})

	call("job overview", jobOverview, jobParams, nil)
	call("job groups", jobGroups, jobParams, nil)
	call("job versions", jobVersions, jobParams, nil)
	call("job spec", jobSpec, jobParams, nil)
	call("job allocations", listAllocations, jobParams, nil)
	call("job deployments", listDeployments, jobParams, nil)
	call("job evaluations", listEvaluations, jobParams, nil)
	call("evaluations", listEvaluations, nil, nil)
	call("deployments", listDeployments, nil, nil)
	call("volumes", listVolumes, nil, nil)
	call("host volumes", listHostVolumes, nil, nil)
	call("force evaluate", evaluateJob, jobParams, nil)

	allocs := call("allocations", listAllocations, jobParams, nil).(listPage)
	if len(allocs.Items) > 0 {
		allocParams := map[string]string{"alloc": fmt.Sprint(allocs.Items[0]["id"])}
		call("alloc overview", allocOverview, allocParams, nil)
		call("alloc events", allocEvents, allocParams, nil)
		call("alloc tasks", allocTasks, allocParams, nil)
	}

	sorted := call("sorted jobs", listJobs, nil, nil).(listPage)
	if sorted.Total == nil {
		t.Log("job listing reported a truncated scan")
	}
	if _, err := listJobs(plugin.NewRequestContext(ctx, plugin.User{}, sess, nil, url.Values{"sort": {"-name"}, "limit": {"1"}}, nil)); err != nil {
		t.Fatalf("sorted listing: %v", err)
	}

	call("stop", stopJob, jobParams, nil)
}

func waitFor(ctx context.Context, t *testing.T, what string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		if done() {
			return
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func nomadIntegrationConfig(ctx context.Context, t *testing.T) map[string]any {
	t.Helper()
	address := os.Getenv("SHELLCN_NOMAD_ADDR")
	if address == "" {
		address = startNomadContainer(ctx, t)
	}
	cfg := map[string]any{
		"address":    address,
		"namespace":  defaultNamespace,
		"auth":       "none",
		"tls_mode":   "disable",
		"read_only":  false,
		"allow_exec": true,
		"timeout":    "20s",
		"log_lines":  100,
		"scan_limit": plugin.MaxPageLimit,
	}
	if token := os.Getenv("SHELLCN_NOMAD_TOKEN"); token != "" {
		cfg["auth"] = "token"
		cfg["token"] = token
	}
	return cfg
}

func startNomadContainer(ctx context.Context, t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_NOMAD_ADDR is not set")
	}
	name := "shellcn-nomad-it-" + time.Now().UTC().Format("20060102150405")
	out, err := run(ctx, "docker", "run", "-d", "--rm", "--privileged", "--name", name,
		"-p", "127.0.0.1::4646", "hashicorp/nomad:latest",
		"agent", "-dev", "-bind=0.0.0.0", "-log-level=WARN")
	if err != nil {
		t.Skipf("cannot start a Nomad dev agent: %v\n%s", err, out)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_, _ = run(cleanupCtx, "docker", "rm", "-f", name)
	})

	ports, err := run(ctx, "docker", "port", name, "4646/tcp")
	if err != nil {
		t.Skipf("cannot read the agent port: %v\n%s", err, ports)
	}
	host, port, err := net.SplitHostPort(strings.TrimSpace(strings.Split(strings.TrimSpace(ports), "\n")[0]))
	if err != nil {
		t.Skipf("unexpected docker port output: %q", ports)
	}
	address := "http://" + net.JoinHostPort(host, port)

	cfg := map[string]any{"address": address, "namespace": defaultNamespace, "auth": "none", "tls_mode": "disable", "read_only": false, "timeout": "20s"}
	deadline := time.Now().Add(90 * time.Second)
	for {
		sess, err := connect(ctx, plugin.ConnectConfig{Config: cfg, Net: sdktest.DirectTransport()})
		if err == nil {
			_ = sess.Close()
			return address
		}
		if time.Now().After(deadline) {
			t.Skipf("Nomad container did not become ready: %v", err)
		}
		time.Sleep(time.Second)
	}
}

func run(ctx context.Context, name string, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	return string(out), err
}
