package nomad

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hashicorp/nomad/api"

	harness "github.com/charlesng35/shellcn-contrib/shared/plugintest"
	"github.com/charlesng35/shellcn/sdk/plugin"
	sdktest "github.com/charlesng35/shellcn/sdk/plugintest"
)

const (
	testJobID  = "web"
	testAlloc  = "11111111-2222-3333-4444-555555555555"
	testNode   = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	testDeploy = "99999999-8888-7777-6666-555555555555"
	testEval   = "12121212-3434-5656-7878-909090909090"
	testVolume = "csi-vol-1"
	testHostV  = "hv-1"
)

// cluster is a stand-in Nomad HTTP API: it answers the exact endpoints the
// official client calls and records every request path so scoping assertions can
// prove which upstream endpoint a handler chose.
type cluster struct {
	server *httptest.Server

	mu       sync.Mutex
	requests []string
	urls     []string

	jobCount       int
	namespaceCount int
}

func newCluster(t *testing.T) *cluster {
	t.Helper()
	c := &cluster{jobCount: 3, namespaceCount: 2}
	mux := http.NewServeMux()
	mux.HandleFunc("/", c.route)
	c.server = httptest.NewServer(mux)
	t.Cleanup(c.server.Close)
	return c
}

func (c *cluster) paths() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.requests...)
}

func (c *cluster) sawPath(want string) bool {
	for _, path := range c.paths() {
		if path == want {
			return true
		}
	}
	return false
}

// sawQuery reports whether a request to path carried key=value, which is how the
// scoping assertions prove a region or namespace actually reached the cluster.
func (c *cluster) sawQuery(path, key, value string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, raw := range c.urls {
		parsed, err := url.Parse(raw)
		if err != nil || parsed.Path != path {
			continue
		}
		if parsed.Query().Get(key) == value {
			return true
		}
	}
	return false
}

func (c *cluster) route(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	c.requests = append(c.requests, r.URL.Path)
	c.urls = append(c.urls, r.URL.String())
	c.mu.Unlock()

	path := r.URL.Path
	switch {
	case path == "/v1/client/allocation/"+testAlloc+"/exec":
		c.exec(w, r)
		return
	case path == "/v1/client/fs/logs/"+testAlloc:
		c.logs(w)
		return
	case path == "/v1/event/stream":
		c.events(w)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Nomad-Index", "42")
	w.Header().Set("X-Nomad-LastContact", "0")
	w.Header().Set("X-Nomad-KnownLeader", "true")

	body, ok := c.body(w, r)
	if !ok {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}
	_ = json.NewEncoder(w).Encode(body)
}

func (c *cluster) body(w http.ResponseWriter, r *http.Request) (any, bool) {
	path := r.URL.Path
	switch path {
	case "/v1/status/leader":
		return "127.0.0.1:4647", true
	case "/v1/status/peers":
		return []string{"127.0.0.1:4647"}, true
	case "/v1/regions":
		return []string{"global"}, true
	case "/v1/namespaces":
		return c.namespacesPage(w, r), true
	case "/v1/agent/self":
		return api.AgentSelf{
			Member: api.AgentMember{Name: "server-1", Tags: map[string]string{"dc": "dc1", "region": "global"}},
			Stats:  map[string]map[string]string{"nomad": {"version": "1.10.0"}},
		}, true
	case "/v1/agent/members":
		return api.ServerMembers{ServerName: "server-1", ServerRegion: "global", Members: []*api.AgentMember{
			{
				Name: "server-1.global", Addr: "127.0.0.1", Port: 4648, Status: "alive",
				Tags: map[string]string{"region": "global", "dc": "dc1", "build": "1.10.0", "role": "nomad", "port": "4647"},
			},
			{
				Name: "server-2.global", Addr: "127.0.0.2", Port: 4648, Status: "alive",
				Tags: map[string]string{"region": "global", "dc": "dc1", "build": "1.10.0", "role": "nomad", "port": "4647"},
			},
		}}, true
	case "/v1/jobs":
		if r.Method != http.MethodGet {
			return api.JobRegisterResponse{EvalID: "eval-1", JobModifyIndex: 9}, true
		}
		return c.jobsPage(w, r), true
	case "/v1/jobs/parse":
		return parsedJob(), true
	case "/v1/allocations":
		return []*api.AllocationListStub{allocStub()}, true
	case "/v1/nodes":
		return []*api.NodeListStub{nodeStub()}, true
	case "/v1/deployments":
		return []*api.Deployment{deployment()}, true
	case "/v1/evaluations":
		return []*api.Evaluation{evaluation()}, true
	case "/v1/volumes":
		if r.URL.Query().Get("type") == "host" {
			return []*api.HostVolumeStub{{ID: testHostV, Name: "data", Namespace: "default", NodeID: testNode, State: api.HostVolumeStateReady, CapacityBytes: 1 << 30}}, true
		}
		return []*api.CSIVolumeListStub{{ID: testVolume, Name: "shared", Namespace: "default", PluginID: "aws-ebs", Provider: "ebs", Schedulable: true}}, true
	case "/v1/volume/csi/" + testVolume:
		return api.CSIVolume{ID: testVolume, Name: "shared", Namespace: "default", PluginID: "aws-ebs", Provider: "ebs", Schedulable: true, Capacity: 1 << 30}, true
	case "/v1/volume/host/" + testHostV:
		return api.HostVolume{ID: testHostV, Name: "data", Namespace: "default", NodeID: testNode, State: api.HostVolumeStateReady, CapacityBytes: 1 << 30}, true
	case "/v1/job/" + testJobID:
		return parsedJob(), true
	case "/v1/job/" + testJobID + "/summary":
		return api.JobSummary{JobID: testJobID, Namespace: "default", Summary: map[string]api.TaskGroupSummary{"app": {Running: 2, Queued: 1}}}, true
	case "/v1/job/" + testJobID + "/deployment":
		return deployment(), true
	case "/v1/job/" + testJobID + "/deployments":
		return []*api.Deployment{deployment()}, true
	case "/v1/job/" + testJobID + "/evaluations":
		return []*api.Evaluation{evaluation()}, true
	case "/v1/job/" + testJobID + "/allocations":
		return []*api.AllocationListStub{allocStub()}, true
	case "/v1/job/" + testJobID + "/versions":
		return api.JobVersionsResponse{Versions: []*api.Job{parsedJob()}, Diffs: []*api.JobDiff{{Type: "Edited", ID: testJobID}}}, true
	case "/v1/job/" + testJobID + "/submission":
		return api.JobSubmission{Source: "job \"web\" {}\n", Format: "hcl2"}, true
	case "/v1/job/" + testJobID + "/plan":
		return api.JobPlanResponse{JobModifyIndex: 7, Diff: &api.JobDiff{Type: "Edited", TaskGroups: []*api.TaskGroupDiff{{Name: "app", Type: "Edited"}}}}, true
	case "/v1/job/" + testJobID + "/scale", "/v1/job/" + testJobID + "/evaluate",
		"/v1/job/" + testJobID + "/periodic/force", "/v1/job/" + testJobID + "/revert":
		return api.JobRegisterResponse{EvalID: "eval-1"}, true
	case "/v1/job/" + testJobID + "/purge":
		return api.JobDeregisterResponse{EvalID: "eval-1"}, true
	case "/v1/allocation/" + testAlloc:
		return allocation(), true
	case "/v1/allocation/" + testAlloc + "/stop":
		return api.AllocStopResponse{EvalID: "eval-1"}, true
	case "/v1/client/allocation/" + testAlloc + "/restart",
		"/v1/client/allocation/" + testAlloc + "/signal":
		return struct{}{}, true
	case "/v1/client/allocation/" + testAlloc + "/stats":
		return api.AllocResourceUsage{ResourceUsage: &api.ResourceUsage{
			CpuStats:    &api.CpuStats{Percent: 12.5, TotalTicks: 250},
			MemoryStats: &api.MemoryStats{Usage: 64 << 20, RSS: 60 << 20},
		}}, true
	case "/v1/client/stats":
		return api.HostStats{
			Uptime: 3600,
			Memory: &api.HostMemoryStats{Total: 8 << 30, Used: 2 << 30},
			CPU:    []*api.HostCPUStats{{CPU: "cpu0", Idle: 80}},
			AllocDirStats: &api.HostDiskStats{
				Size: 100 << 30, Used: 20 << 30, UsedPercent: 20,
			},
		}, true
	case "/v1/node/" + testNode:
		return node(), true
	case "/v1/node/" + testNode + "/allocations":
		return []*api.Allocation{allocation()}, true
	case "/v1/node/" + testNode + "/drain":
		return api.NodeDrainUpdateResponse{NodeModifyIndex: 3, EvalIDs: []string{"eval-1"}}, true
	case "/v1/node/" + testNode + "/eligibility":
		return api.NodeEligibilityUpdateResponse{NodeModifyIndex: 4}, true
	case "/v1/deployment/" + testDeploy:
		return deployment(), true
	case "/v1/deployment/allocations/" + testDeploy:
		return []*api.AllocationListStub{allocStub()}, true
	case "/v1/deployment/promote/" + testDeploy, "/v1/deployment/fail/" + testDeploy,
		"/v1/deployment/pause/" + testDeploy, "/v1/deployment/unblock/" + testDeploy:
		return api.DeploymentUpdateResponse{EvalID: "eval-1"}, true
	case "/v1/evaluation/" + testEval:
		return evaluation(), true
	case "/v1/evaluation/" + testEval + "/allocations":
		return []*api.AllocationListStub{allocStub()}, true
	}
	if path == "/v1/job/"+testJobID && r.Method == http.MethodDelete {
		return api.JobDeregisterResponse{EvalID: "eval-1"}, true
	}
	return nil, false
}

// jobsPage answers /v1/jobs with real per_page + next_token semantics so cursor
// behaviour is exercised against the contract the cluster actually implements.
func (c *cluster) jobsPage(w http.ResponseWriter, r *http.Request) []*api.JobListStub {
	perPage, _ := strconv.Atoi(r.URL.Query().Get("per_page"))
	if perPage <= 0 {
		perPage = c.jobCount
	}
	start, _ := strconv.Atoi(r.URL.Query().Get("next_token"))
	out := []*api.JobListStub{}
	for i := start; i < c.jobCount && len(out) < perPage; i++ {
		out = append(out, jobStub(i))
	}
	if next := start + len(out); next < c.jobCount {
		w.Header().Set("X-Nomad-NextToken", strconv.Itoa(next))
	}
	return out
}

// namespacesPage answers /v1/namespaces with per_page + next_token so the
// namespace walk is exercised against a cluster that really does page.
func (c *cluster) namespacesPage(w http.ResponseWriter, r *http.Request) []*api.Namespace {
	names := []string{"default", "team-a", "team-b", "team-c", "team-d", "team-e"}
	// The fake caps its own page size, so a caller that ignores next_token can
	// only ever see the first two namespaces.
	perPage := 2
	start, _ := strconv.Atoi(r.URL.Query().Get("next_token"))
	out := []*api.Namespace{}
	for i := start; i < c.namespaceCount && len(out) < perPage; i++ {
		out = append(out, &api.Namespace{Name: names[i%len(names)], Description: "namespace " + names[i%len(names)]})
	}
	if next := start + len(out); next < c.namespaceCount {
		w.Header().Set("X-Nomad-NextToken", strconv.Itoa(next))
	}
	return out
}

func (c *cluster) logs(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	_ = enc.Encode(api.StreamFrame{Data: []byte("hello "), File: "stdout"})
	_ = enc.Encode(api.StreamFrame{Data: []byte("world\n"), File: "stdout"})
}

func (c *cluster) events(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	job := parsedJob()
	payload, _ := json.Marshal(map[string]any{"Job": job})
	var decoded map[string]any
	_ = json.Unmarshal(payload, &decoded)
	_ = json.NewEncoder(w).Encode(api.Events{Index: 7, Events: []api.Event{{
		Topic: api.TopicJob, Type: "JobRegistered", Key: testJobID, Index: 7, Payload: decoded,
	}}})
}

func (c *cluster) exec(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()
	_ = conn.WriteJSON(api.ExecStreamingOutput{Stdout: &api.ExecStreamingIOOperation{Data: []byte("root@nomad:/# ")}})
	_ = conn.WriteJSON(api.ExecStreamingOutput{Exited: true, Result: &api.ExecStreamingExitResult{ExitCode: 0}})
	// Hold the socket open the way a real exec endpoint does, so stdin and
	// resize frames have somewhere to land until the client hangs up.
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}

func ptr[T any](v T) *T { return &v }

func parsedJob() *api.Job {
	return &api.Job{
		ID: ptr(testJobID), Name: ptr("web"), Namespace: ptr("default"), Type: ptr("service"),
		Status: ptr("running"), Priority: ptr(50), Region: ptr("global"), Version: ptr(uint64(3)),
		Stable: ptr(true), Stop: ptr(false), SubmitTime: ptr(time.Now().UnixNano()),
		ModifyIndex: ptr(uint64(9)), JobModifyIndex: ptr(uint64(9)), Datacenters: []string{"dc1"},
		TaskGroups: []*api.TaskGroup{{
			Name: ptr("app"), Count: ptr(2),
			EphemeralDisk: &api.EphemeralDisk{SizeMB: ptr(300)},
			Tasks: []*api.Task{{
				Name: "server", Driver: "docker",
				Resources: &api.Resources{CPU: ptr(100), MemoryMB: ptr(128)},
			}},
		}},
	}
}

func jobStub(i int) *api.JobListStub {
	names := []string{"charlie", "alpha", "bravo"}
	name := names[i%len(names)]
	return &api.JobListStub{
		ID: name, Name: name, Namespace: "default", Type: "service", Status: "running",
		Priority: 50 + i, Datacenters: []string{"dc1"}, SubmitTime: time.Now().UnixNano(),
		JobSummary: &api.JobSummary{Summary: map[string]api.TaskGroupSummary{"app": {Running: 1}}},
	}
}

func allocStub() *api.AllocationListStub {
	return &api.AllocationListStub{
		ID: testAlloc, Name: "web.app[0]", Namespace: "default", JobID: testJobID, JobType: "service",
		JobVersion: 3, TaskGroup: "app", NodeID: testNode, NodeName: "client-1",
		ClientStatus: "running", DesiredStatus: "run",
		AllocatedResources: &api.AllocatedResources{Tasks: map[string]*api.AllocatedTaskResources{
			"server": {Cpu: api.AllocatedCpuResources{CpuShares: 100}, Memory: api.AllocatedMemoryResources{MemoryMB: 128}},
		}},
		CreateTime: time.Now().UnixNano(), ModifyTime: time.Now().UnixNano(),
	}
}

func allocation() *api.Allocation {
	return &api.Allocation{
		ID: testAlloc, Name: "web.app[0]", Namespace: "default", JobID: testJobID, TaskGroup: "app",
		NodeID: testNode, NodeName: "client-1", ClientStatus: "running", DesiredStatus: "run",
		AllocatedResources: &api.AllocatedResources{
			Tasks:  map[string]*api.AllocatedTaskResources{"server": {Cpu: api.AllocatedCpuResources{CpuShares: 100}, Memory: api.AllocatedMemoryResources{MemoryMB: 128}}},
			Shared: api.AllocatedSharedResources{DiskMB: 300},
		},
		TaskStates: map[string]*api.TaskState{"server": {State: "running", Events: []*api.TaskEvent{
			{Type: api.TaskReceived, Time: time.Now().Add(-time.Minute).UnixNano(), DisplayMessage: "Task received"},
			{Type: api.TaskStarted, Time: time.Now().UnixNano(), DisplayMessage: "Task started"},
		}}},
		CreateTime: time.Now().UnixNano(), ModifyTime: time.Now().UnixNano(),
	}
}

func nodeStub() *api.NodeListStub {
	return &api.NodeListStub{
		ID: testNode, Name: "client-1", Datacenter: "dc1", NodePool: "default", Status: "ready",
		SchedulingEligibility: "eligible", Version: "1.10.0", Address: "127.0.0.1",
		NodeResources: &api.NodeResources{
			Cpu: api.NodeCpuResources{CpuShares: 4000}, Memory: api.NodeMemoryResources{MemoryMB: 8192},
			Disk: api.NodeDiskResources{DiskMB: 102400},
		},
		Drivers: map[string]*api.DriverInfo{"docker": {Detected: true, Healthy: true}},
	}
}

func node() *api.Node {
	return &api.Node{
		ID: testNode, Name: "client-1", Datacenter: "dc1", NodePool: "default", Status: "ready",
		SchedulingEligibility: "eligible", HTTPAddr: "127.0.0.1:4646",
		Attributes: map[string]string{"nomad.version": "1.10.0", "os.name": "ubuntu", "cpu.arch": "amd64"},
		NodeResources: &api.NodeResources{
			Cpu: api.NodeCpuResources{CpuShares: 4000}, Memory: api.NodeMemoryResources{MemoryMB: 8192},
			Disk: api.NodeDiskResources{DiskMB: 102400},
		},
		Drivers: map[string]*api.DriverInfo{"docker": {Detected: true, Healthy: true}},
	}
}

func deployment() *api.Deployment {
	return &api.Deployment{
		ID: testDeploy, Namespace: "default", JobID: testJobID, JobVersion: 3, Status: "successful",
		StatusDescription: "Deployment completed successfully",
		TaskGroups: map[string]*api.DeploymentState{"app": {
			DesiredTotal: 2, PlacedAllocs: 2, HealthyAllocs: 2, Promoted: true,
		}},
		CreateTime: time.Now().UnixNano(), ModifyTime: time.Now().UnixNano(),
	}
}

func evaluation() *api.Evaluation {
	return &api.Evaluation{
		ID: testEval, Namespace: "default", JobID: testJobID, Priority: 50, Type: "service",
		TriggeredBy: "job-register", Status: "complete",
		CreateTime: time.Now().UnixNano(), ModifyTime: time.Now().UnixNano(),
	}
}

func testConfig(c *cluster, overrides map[string]any) map[string]any {
	cfg := map[string]any{
		"address":    c.server.URL,
		"namespace":  "default",
		"auth":       "none",
		"tls_mode":   "disable",
		"read_only":  false,
		"allow_exec": true,
		"timeout":    "10s",
		"log_lines":  10,
		"scan_limit": plugin.MaxPageLimit,
	}
	for key, value := range overrides {
		cfg[key] = value
	}
	return cfg
}

func newSession(t *testing.T, c *cluster, overrides map[string]any) *Session {
	t.Helper()
	sess, err := connect(context.Background(), plugin.ConnectConfig{
		Config: testConfig(c, overrides),
		Net:    sdktest.DirectTransport(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = sess.Close() })
	return sess.(*Session)
}

func TestNomadManifestValidates(t *testing.T) {
	proj := sdktest.Projection(t, New())
	if proj.Category.Key != plugin.CategoryOrchestration {
		t.Fatalf("category: got %q want %q", proj.Category.Key, plugin.CategoryOrchestration)
	}
	if proj.Layout != plugin.LayoutSidebarTree {
		t.Fatalf("layout: got %q", proj.Layout)
	}
	kinds := map[string]bool{}
	for _, resource := range proj.Resources {
		kinds[resource.Kind] = true
	}
	for _, want := range []string{"cluster", "job", "allocation", "node", "deployment", "evaluation", "volume", "host_volume"} {
		if !kinds[want] {
			t.Fatalf("missing resource kind %q", want)
		}
	}
}

func TestNomadConfigSchemaIsSpecific(t *testing.T) {
	fields := map[string]plugin.Field{}
	for _, group := range New().Manifest().Config.Groups {
		for _, field := range group.Fields {
			fields[field.Key] = field
		}
	}
	for _, key := range []string{"address", "region", "namespace", "auth", "token", tokenCredentialField, clientCertCredential, "read_only", "allow_exec", "scan_limit"} {
		if _, ok := fields[key]; !ok {
			t.Fatalf("missing config field %q", key)
		}
	}
	for _, key := range []string{"management_url", "brokers", "urls"} {
		if _, ok := fields[key]; ok {
			t.Fatalf("nomad should not expose %q", key)
		}
	}
	if !fields["token"].Secret {
		t.Fatal("token must be stored as a secret")
	}
	if fields[tokenCredentialField].Credential == nil || fields[tokenCredentialField].Credential.Kind != plugin.CredentialKindAPIToken {
		t.Fatal("stored token must reference the API token credential kind")
	}
}

func TestNomadRouteMetadata(t *testing.T) {
	byID := map[string]plugin.Route{}
	for _, route := range routes() {
		if !strings.HasPrefix(route.ID, protocolName+".") {
			t.Fatalf("route %q is not namespaced", route.ID)
		}
		if route.AuditEvent != route.ID {
			t.Fatalf("route %q audit event %q should equal the route id", route.ID, route.AuditEvent)
		}
		byID[route.ID] = route
	}
	if got := byID["nomad.alloc.exec"].Risk; got != plugin.RiskPrivileged {
		t.Fatalf("exec risk: got %q want privileged", got)
	}
	for _, id := range []string{"nomad.job.stop", "nomad.job.purge", "nomad.alloc.stop", "nomad.node.drain", "nomad.deployment.fail"} {
		if byID[id].Risk != plugin.RiskDestructive {
			t.Fatalf("route %q should be destructive", id)
		}
	}
	for _, id := range []string{"nomad.jobs.list", "nomad.allocs.list", "nomad.nodes.list", "nomad.job.plan"} {
		if byID[id].Risk != plugin.RiskSafe {
			t.Fatalf("route %q should be safe", id)
		}
	}
}

func TestNomadRoutesCoverEveryHandler(t *testing.T) {
	c := newCluster(t)
	sess := newSession(t, c, nil)
	h := harness.NewHarness(t, routes())
	ctx := context.Background()

	job := map[string]string{"ns": "default", "job": testJobID}
	alloc := map[string]string{"ns": "default", "alloc": testAlloc}
	nodeParams := map[string]string{"node": testNode}
	deploy := map[string]string{"ns": "default", "deployment": testDeploy}
	eval := map[string]string{"ns": "default", "eval": testEval}

	for _, id := range []string{"nomad.cluster.list", "nomad.cluster.overview", "nomad.members.list", "nomad.regions.list", "nomad.datacenters.list", "nomad.namespaces.list", "nomad.scope.namespaces", "nomad.tree.namespaces", "nomad.jobs.list", "nomad.tree.jobs", "nomad.allocs.list", "nomad.nodes.list", "nomad.deployments.list", "nomad.evals.list", "nomad.volumes.list", "nomad.hostvolumes.list", "nomad.log.types"} {
		h.Call(ctx, id, sess, nil, nil, nil)
	}
	for _, id := range []string{"nomad.tree.groups", "nomad.job.overview", "nomad.job.spec", "nomad.job.groups", "nomad.job.versions"} {
		h.Call(ctx, id, sess, job, nil, nil)
	}
	for _, id := range []string{"nomad.alloc.overview", "nomad.alloc.events", "nomad.alloc.tasks"} {
		h.Call(ctx, id, sess, alloc, nil, nil)
	}
	h.Call(ctx, "nomad.node.overview", sess, nodeParams, nil, nil)
	h.Call(ctx, "nomad.deployment.overview", sess, deploy, nil, nil)
	h.Call(ctx, "nomad.eval.overview", sess, eval, nil, nil)
	h.Call(ctx, "nomad.volume.overview", sess, map[string]string{"ns": "default", "volume": testVolume}, nil, nil)
	h.Call(ctx, "nomad.hostvolume.overview", sess, map[string]string{"ns": "default", "volume": testHostV}, nil, nil)

	spec, _ := json.Marshal(map[string]any{"content": "job \"web\" {}\n", "ns": "default"})
	h.Call(ctx, "nomad.job.plan", sess, nil, nil, spec)
	h.Call(ctx, "nomad.job.submit", sess, nil, nil, spec)
	h.Call(ctx, "nomad.job.spec.save", sess, job, nil, spec)
	h.Call(ctx, "nomad.job.restart", sess, job, nil, nil)
	h.Call(ctx, "nomad.job.revert", sess, job, nil, []byte(`{"version":2}`))
	h.Call(ctx, "nomad.job.scale", sess, job, nil, []byte(`{"group":"app","count":3,"message":"scale"}`))
	h.Call(ctx, "nomad.job.evaluate", sess, job, nil, nil)
	h.Call(ctx, "nomad.job.periodic", sess, job, nil, nil)
	h.Call(ctx, "nomad.job.stop", sess, job, nil, nil)
	h.Call(ctx, "nomad.job.purge", sess, job, nil, nil)

	h.Call(ctx, "nomad.alloc.restart", sess, alloc, nil, []byte(`{"task":"server"}`))
	h.Call(ctx, "nomad.alloc.signal", sess, alloc, nil, []byte(`{"signal":"SIGHUP","task":"server"}`))
	h.Call(ctx, "nomad.alloc.stop", sess, alloc, nil, nil)

	h.Call(ctx, "nomad.node.drain", sess, nodeParams, nil, []byte(`{"deadline":"1h","ignore_system_jobs":false}`))
	h.Call(ctx, "nomad.node.drain.cancel", sess, nodeParams, nil, nil)
	h.Call(ctx, "nomad.node.eligibility", sess, nodeParams, nil, []byte(`{"eligible":true}`))

	h.Call(ctx, "nomad.deployment.promote", sess, deploy, nil, nil)
	h.Call(ctx, "nomad.deployment.pause", sess, deploy, nil, []byte(`{"pause":true}`))
	h.Call(ctx, "nomad.deployment.unblock", sess, deploy, nil, nil)
	h.Call(ctx, "nomad.deployment.fail", sess, deploy, nil, nil)

	// Each stream gets its own short deadline: the tick-driven handlers only exit
	// when the browser goes away, and a shared context would starve later ones.
	stream := func(id string, params map[string]string, input []byte) []byte {
		streamCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer cancel()
		return h.Stream(streamCtx, id, sess, params, nil, input)
	}
	stream("nomad.job.watch", job, nil)
	stream("nomad.alloc.watch", alloc, nil)
	stream("nomad.node.watch", nodeParams, nil)
	stream("nomad.alloc.metrics", alloc, nil)
	stream("nomad.node.metrics", nodeParams, nil)
	stream("nomad.deployment.progress", deploy, nil)
	stream("nomad.resources.watch", map[string]string{"kind": "job"}, nil)

	logs := stream("nomad.alloc.logs", map[string]string{"ns": "default", "alloc": testAlloc, "task": "server", "type": "stdout"}, nil)
	if !strings.Contains(string(logs), "hello world") {
		t.Fatalf("log stream: got %q", logs)
	}
	stream("nomad.alloc.exec", map[string]string{"ns": "default", "alloc": testAlloc, "task": "server", "cols": "80", "rows": "24"}, []byte("id\n"))

	h.AssertAllCovered()
}

func TestExecChannelSpeaksTheStreamingProtocol(t *testing.T) {
	c := newCluster(t)
	sess := newSession(t, c, nil)
	ch, err := sess.OpenChannel(context.Background(), plugin.ChannelRequest{
		Kind:   plugin.StreamTerminal,
		Params: map[string]string{"ns": "default", "alloc": testAlloc, "task": "server", "cols": "80", "rows": "24"},
	})
	if err != nil {
		t.Fatalf("open exec channel: %v", err)
	}
	defer func() { _ = ch.Close() }()
	if ch.Kind() != plugin.StreamTerminal {
		t.Fatalf("channel kind: %q", ch.Kind())
	}
	if _, err := ch.Write([]byte("id\n")); err != nil {
		t.Fatalf("stdin: %v", err)
	}
	if resizer, ok := ch.(plugin.Resizer); !ok {
		t.Fatal("an exec channel must accept terminal resizes")
	} else if err := resizer.Resize(120, 40); err != nil {
		t.Fatalf("resize: %v", err)
	}

	var out strings.Builder
	buf := make([]byte, 256)
	for {
		n, err := ch.Read(buf)
		out.Write(buf[:n])
		if err != nil {
			break
		}
		if strings.Contains(out.String(), "exit status") {
			break
		}
	}
	if !strings.Contains(out.String(), "root@nomad") {
		t.Fatalf("stdout was not forwarded: %q", out.String())
	}
	if !strings.Contains(out.String(), "exit status 0") {
		t.Fatalf("exit status was not reported: %q", out.String())
	}
}

func TestJobsListPagesThroughUpstreamCursor(t *testing.T) {
	c := newCluster(t)
	c.jobCount = 5
	sess := newSession(t, c, nil)
	ctx := context.Background()

	first := listPageOf(t, ctx, sess, url.Values{"limit": {"2"}})
	if len(first.Items) != 2 {
		t.Fatalf("first page size: got %d want 2", len(first.Items))
	}
	if first.NextCursor == "" {
		t.Fatal("first page should continue")
	}
	if first.Total != nil {
		t.Fatal("cluster-ordered paging cannot report an authoritative total")
	}

	second := listPageOf(t, ctx, sess, url.Values{"limit": {"2"}, "cursor": {first.NextCursor}})
	if len(second.Items) != 2 {
		t.Fatalf("second page size: got %d", len(second.Items))
	}
	if fmt.Sprint(second.Items[0]["id"]) == fmt.Sprint(first.Items[0]["id"]) {
		t.Fatal("second page repeated the first page")
	}

	last := listPageOf(t, ctx, sess, url.Values{"limit": {"2"}, "cursor": {second.NextCursor}})
	if len(last.Items) != 1 {
		t.Fatalf("last page size: got %d want 1", len(last.Items))
	}
	if last.NextCursor != "" {
		t.Fatal("last page must not advertise another page")
	}
}

func TestJobsListSortsAcrossTheWholeDataset(t *testing.T) {
	c := newCluster(t)
	c.jobCount = 3
	sess := newSession(t, c, nil)
	ctx := context.Background()

	// One row per page: a per-page sort would return "charlie" first.
	page := listPageOf(t, ctx, sess, url.Values{"limit": {"1"}, "sort": {"name"}})
	if len(page.Items) != 1 || fmt.Sprint(page.Items[0]["name"]) != "alpha" {
		t.Fatalf("sorted first page: got %#v", page.Items)
	}
	if page.Total == nil || *page.Total != 3 {
		t.Fatalf("a complete walk should report the total: %v", page.Total)
	}
	next := listPageOf(t, ctx, sess, url.Values{"limit": {"1"}, "sort": {"name"}, "cursor": {page.NextCursor}})
	if fmt.Sprint(next.Items[0]["name"]) != "bravo" {
		t.Fatalf("sorted second page: got %#v", next.Items)
	}
}

func TestJobsListFiltersAcrossTheWholeDataset(t *testing.T) {
	c := newCluster(t)
	c.jobCount = 3
	sess := newSession(t, c, nil)

	page := listPageOf(t, context.Background(), sess, url.Values{"limit": {"1"}, "filter": {"bravo"}})
	if len(page.Items) != 1 || fmt.Sprint(page.Items[0]["name"]) != "bravo" {
		t.Fatalf("filtered page: got %#v", page.Items)
	}
	if page.Total == nil || *page.Total != 1 {
		t.Fatalf("filtered total: %v", page.Total)
	}
	if page.NextCursor != "" {
		t.Fatal("a fully matched filter must not advertise another page")
	}
}

func TestJobsListReportsATruncatedScan(t *testing.T) {
	c := newCluster(t)
	c.jobCount = 40
	sess := newSession(t, c, nil)
	sess.opts.ScanLimit = 10

	page := listPageOf(t, context.Background(), sess, url.Values{"limit": {"2"}, "sort": {"name"}})
	if !page.Truncated || page.ScanLimit != 10 {
		t.Fatalf("expected a truncated scan, got truncated=%v limit=%d", page.Truncated, page.ScanLimit)
	}
	if page.Total != nil {
		t.Fatal("a capped walk must not report a total it cannot vouch for")
	}
	if page.NextCursor == "" {
		t.Fatal("a capped walk must still hand back a reachable continue cursor")
	}
}

func TestScanBudgetKeepsDeepPagesReachable(t *testing.T) {
	c := newCluster(t)
	c.jobCount = 40
	sess := newSession(t, c, nil)
	sess.opts.ScanLimit = 10

	// Offset 30 sits past the cap; the walk has to grow to cover the page.
	page := listPageOf(t, context.Background(), sess, url.Values{"limit": {"5"}, "sort": {"name"}, "cursor": {"30"}})
	if len(page.Items) != 5 {
		t.Fatalf("deep page size: got %d want 5", len(page.Items))
	}
}

func TestListsToleratePlainOffsetCursors(t *testing.T) {
	c := newCluster(t)
	c.jobCount = 5
	sess := newSession(t, c, nil)

	page := listPageOf(t, context.Background(), sess, url.Values{"limit": {"2"}, "cursor": {"2"}})
	if len(page.Items) != 2 {
		t.Fatalf("offset page size: got %d", len(page.Items))
	}
	if page.Total == nil || *page.Total != 5 {
		t.Fatalf("an offset cursor walks the whole set: %v", page.Total)
	}
}

func TestListsRejectMalformedCursors(t *testing.T) {
	c := newCluster(t)
	sess := newSession(t, c, nil)
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess, nil, url.Values{"cursor": {"!!not-a-cursor"}}, nil)
	if _, err := listJobs(rc); err == nil {
		t.Fatal("expected an invalid cursor error")
	}
}

func listPageOf(t *testing.T, ctx context.Context, sess *Session, query url.Values) listPage {
	t.Helper()
	out, err := listJobs(plugin.NewRequestContext(ctx, plugin.User{}, sess, nil, query, nil))
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	page, ok := out.(listPage)
	if !ok {
		t.Fatalf("unexpected list payload %T", out)
	}
	return page
}

func TestAllocationListScopesToItsParent(t *testing.T) {
	c := newCluster(t)
	sess := newSession(t, c, nil)
	ctx := context.Background()

	cases := []struct {
		params map[string]string
		want   string
	}{
		{map[string]string{"ns": "default", "job": testJobID}, "/v1/job/" + testJobID + "/allocations"},
		{map[string]string{"node": testNode}, "/v1/node/" + testNode + "/allocations"},
		{map[string]string{"ns": "default", "deployment": testDeploy}, "/v1/deployment/allocations/" + testDeploy},
		{map[string]string{"ns": "default", "eval": testEval}, "/v1/evaluation/" + testEval + "/allocations"},
		{nil, "/v1/allocations"},
	}
	for _, tc := range cases {
		if _, err := listAllocations(plugin.NewRequestContext(ctx, plugin.User{}, sess, tc.params, nil, nil)); err != nil {
			t.Fatalf("list allocations %v: %v", tc.params, err)
		}
		if !c.sawPath(tc.want) {
			t.Fatalf("params %v should have queried %s, saw %v", tc.params, tc.want, c.paths())
		}
	}
}

func TestAllocationListFiltersByTaskGroup(t *testing.T) {
	c := newCluster(t)
	sess := newSession(t, c, nil)
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess, map[string]string{"ns": "default", "job": testJobID, "group": "other"}, nil, nil)
	out, err := listAllocations(rc)
	if err != nil {
		t.Fatalf("list allocations: %v", err)
	}
	if items := out.(listPage).Items; len(items) != 0 {
		t.Fatalf("group scope should have excluded every row, got %#v", items)
	}
}

func TestNamespaceResolution(t *testing.T) {
	c := newCluster(t)
	sess := newSession(t, c, map[string]any{"namespace": "connection-ns"})
	ctx := context.Background()

	newRC := func(params map[string]string) *plugin.RequestContext {
		return plugin.NewRequestContext(ctx, plugin.User{}, sess, params, nil, nil)
	}
	if got := listNamespace(newRC(nil), sess); got != "connection-ns" {
		t.Fatalf("default namespace: got %q", got)
	}
	if got := listNamespace(newRC(map[string]string{"namespace": "*"}), sess); got != "*" {
		t.Fatalf("scope wildcard should reach a list: got %q", got)
	}
	if got := objectNamespace(newRC(map[string]string{"namespace": "*"}), sess); got != "connection-ns" {
		t.Fatalf("single-object reads cannot use a wildcard: got %q", got)
	}
	if got := listNamespace(newRC(map[string]string{"ns": "team-a", "namespace": "team-b"}), sess); got != "team-a" {
		t.Fatalf("resource scope should beat the picker: got %q", got)
	}
}

func TestListQueryCarriesScope(t *testing.T) {
	c := newCluster(t)
	sess := newSession(t, c, map[string]any{"region": "eu", "namespace": "default"})
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess, map[string]string{"ns": "team-a"}, nil, nil)
	q := sess.listQuery(rc)
	if q.Namespace != "team-a" || q.Region != "eu" {
		t.Fatalf("list query scope: %#v", q)
	}
	rc = plugin.NewRequestContext(context.Background(), plugin.User{}, sess, map[string]string{"region": "us"}, nil, nil)
	if got := sess.listQuery(rc).Region; got != "us" {
		t.Fatalf("region scope: got %q", got)
	}
}

func TestParamFallsBackToStreamQueryArgs(t *testing.T) {
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, nil, nil, url.Values{"p.alloc": {testAlloc}}, nil)
	if got := param(rc, "alloc"); got != testAlloc {
		t.Fatalf("stream param: got %q", got)
	}
}

func TestCursorRoundTrip(t *testing.T) {
	encoded := encodeCursor(cursor{Token: "abc"})
	if encoded == "" {
		t.Fatal("a real continuation token must encode")
	}
	decoded, err := decodeCursor(encoded)
	if err != nil || decoded.Token != "abc" {
		t.Fatalf("round trip: %#v %v", decoded, err)
	}
	if encodeCursor(cursor{}) != "" {
		t.Fatal("an exhausted listing must not emit a cursor")
	}
	offset, err := decodeCursor("120")
	if err != nil || offset.Offset != 120 {
		t.Fatalf("numeric fallback: %#v %v", offset, err)
	}
	if _, err := decodeCursor("-1"); err == nil {
		t.Fatal("negative offsets are invalid")
	}
	if _, err := decodeCursor("~~~"); err == nil {
		t.Fatal("malformed cursors are invalid")
	}
}

func TestSliceRowsOrdersBeforeCutting(t *testing.T) {
	rows := []row{{"name": "c"}, {"name": "a"}, {"name": "b"}}
	page := sliceRows(rows, plugin.PageRequest{Limit: 2, Sort: []plugin.SortKey{{Field: "name"}}}, 0, false, 100)
	if len(page.Items) != 2 || page.Items[0]["name"] != "a" || page.Items[1]["name"] != "b" {
		t.Fatalf("sorted slice: %#v", page.Items)
	}
	if page.Total == nil || *page.Total != 3 {
		t.Fatalf("total: %v", page.Total)
	}
	if page.NextCursor != "2" {
		t.Fatalf("next cursor: %q", page.NextCursor)
	}
}

func TestSliceRowsClampsPastTheEnd(t *testing.T) {
	page := sliceRows([]row{{"name": "a"}}, plugin.PageRequest{Limit: 10}, 50, false, 100)
	if len(page.Items) != 0 {
		t.Fatalf("out-of-range offset: %#v", page.Items)
	}
	if page.Items == nil {
		t.Fatal("an empty page must still encode as a list")
	}
	if page.NextCursor != "" {
		t.Fatal("an out-of-range page must not advertise another page")
	}
}

func TestParseJobSpecAcceptsJSONAndHCL(t *testing.T) {
	c := newCluster(t)
	sess := newSession(t, c, nil)

	wrapped, submission, err := sess.parseJobSpec(`{"Job":{"ID":"web","Name":"web"}}`, "team-a")
	if err != nil {
		t.Fatalf("wrapped JSON: %v", err)
	}
	if deref(wrapped.ID) != "web" || deref(wrapped.Namespace) != "team-a" {
		t.Fatalf("wrapped JSON job: %#v", wrapped)
	}
	if submission == nil || submission.Format != "json" {
		t.Fatalf("JSON submission: %#v", submission)
	}
	direct, _, err := sess.parseJobSpec(`{"ID":"web"}`, "")
	if err != nil || deref(direct.ID) != "web" {
		t.Fatalf("bare JSON job: %#v %v", direct, err)
	}
	hcl, hclSubmission, err := sess.parseJobSpec("job \"web\" {}\n", "default")
	if err != nil || deref(hcl.ID) != testJobID {
		t.Fatalf("HCL job: %#v %v", hcl, err)
	}
	if hclSubmission == nil || hclSubmission.Format != "hcl2" || !strings.Contains(hclSubmission.Source, "job") {
		t.Fatalf("HCL submission: %#v", hclSubmission)
	}
	if _, _, err := sess.parseJobSpec("   ", ""); err == nil {
		t.Fatal("an empty specification is invalid")
	}
	if _, _, err := sess.parseJobSpec(`{"Job":{"Name":"web"}}`, ""); err == nil {
		t.Fatal("a job without an ID is invalid")
	}
}

func TestSaveJobSpecRefusesARenamedJob(t *testing.T) {
	c := newCluster(t)
	sess := newSession(t, c, nil)
	body, _ := json.Marshal(map[string]any{"content": `{"Job":{"ID":"other"}}`})
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess, map[string]string{"ns": "default", "job": testJobID}, nil, body)
	if _, err := saveJobSpec(rc); err == nil {
		t.Fatal("editing a job spec must not silently register a different job")
	}
}

func TestReadOnlyBlocksWrites(t *testing.T) {
	c := newCluster(t)
	sess := newSession(t, c, map[string]any{"read_only": true})
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess, map[string]string{"ns": "default", "job": testJobID}, nil, nil)
	for name, handler := range map[string]plugin.Handler{
		"stop":     stopJob,
		"purge":    purgeJob,
		"restart":  restartJob,
		"evaluate": evaluateJob,
	} {
		if _, err := handler(rc); err == nil {
			t.Fatalf("%s should be refused on a read-only connection", name)
		}
	}
}

func TestExecRequiresOptIn(t *testing.T) {
	c := newCluster(t)
	sess := newSession(t, c, map[string]any{"allow_exec": false})
	_, err := sess.OpenChannel(context.Background(), plugin.ChannelRequest{
		Kind:   plugin.StreamTerminal,
		Params: map[string]string{"alloc": testAlloc, "task": "server"},
	})
	if err == nil {
		t.Fatal("exec must stay closed until the connection opts in")
	}
}

func TestOpenChannelRejectsUnknownKinds(t *testing.T) {
	c := newCluster(t)
	sess := newSession(t, c, nil)
	if _, err := sess.OpenChannel(context.Background(), plugin.ChannelRequest{Kind: plugin.StreamQuery}); err == nil {
		t.Fatal("unsupported channel kinds must be refused")
	}
}

func TestPinnedDialerRefusesForeignAddresses(t *testing.T) {
	d := pinnedDialer{host: "127.0.0.1:4646"}
	if _, err := d.DialContext(context.Background(), "tcp", "10.0.0.9:4646"); err == nil {
		t.Fatal("a client-node address must not be dialled directly")
	}
}

func TestExecCommand(t *testing.T) {
	if got := execCommand(""); len(got) != 1 || got[0] != "/bin/sh" {
		t.Fatalf("default command: %#v", got)
	}
	if got := execCommand(`["/bin/bash","-l"]`); len(got) != 2 || got[1] != "-l" {
		t.Fatalf("JSON command: %#v", got)
	}
	if got := execCommand("ps aux"); len(got) != 2 || got[0] != "ps" {
		t.Fatalf("plain command: %#v", got)
	}
}

func TestParseDeadline(t *testing.T) {
	for _, raw := range []string{"", "0", "  "} {
		if d, err := parseDeadline(raw); err != nil || d != 0 {
			t.Fatalf("parseDeadline(%q) = %v, %v", raw, d, err)
		}
	}
	if d, err := parseDeadline("90m"); err != nil || d != 90*time.Minute {
		t.Fatalf("parseDeadline duration: %v %v", d, err)
	}
	for _, raw := range []string{"soon", "-5m"} {
		if _, err := parseDeadline(raw); err == nil {
			t.Fatalf("parseDeadline(%q) should fail", raw)
		}
	}
}

func TestNomadErrMapsUpstreamStatus(t *testing.T) {
	c := newCluster(t)
	sess := newSession(t, c, nil)
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess, map[string]string{"job": "missing"}, nil, nil)
	_, err := jobOverview(rc)
	if err == nil || !strings.Contains(err.Error(), plugin.ErrNotFound.Error()) {
		t.Fatalf("missing job should map to not found, got %v", err)
	}
}

func TestStatusErrMapping(t *testing.T) {
	cases := map[int]error{
		http.StatusBadRequest:   plugin.ErrInvalidInput,
		http.StatusUnauthorized: plugin.ErrUnauthorized,
		http.StatusForbidden:    plugin.ErrForbidden,
		http.StatusNotFound:     plugin.ErrNotFound,
		http.StatusConflict:     plugin.ErrConflict,
		http.StatusBadGateway:   plugin.ErrUnavailable,
	}
	for status, want := range cases {
		if err := statusErr(status, "boom"); !strings.Contains(err.Error(), want.Error()) {
			t.Fatalf("status %d mapped to %v, want %v", status, err, want)
		}
	}
}

func TestParseOptionsRejectsBadAddresses(t *testing.T) {
	for _, address := range []string{"nomad.example.internal", "ftp://nomad:4646", "://"} {
		if _, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{"address": address}}); err == nil {
			t.Fatalf("address %q should be rejected", address)
		}
	}
	opts, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{"address": "https://nomad.example.internal/", "auth": "token", "token": "s3cr3t"}})
	if err != nil {
		t.Fatalf("parse options: %v", err)
	}
	if opts.Address != "https://nomad.example.internal" || opts.Host != "nomad.example.internal:443" {
		t.Fatalf("address normalisation: %#v", opts)
	}
	if opts.Namespace != defaultNamespace || opts.Token != "s3cr3t" {
		t.Fatalf("defaults: %#v", opts)
	}
	if _, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{"address": "http://x:4646", "auth": "kerberos"}}); err == nil {
		t.Fatal("unknown auth modes should be rejected")
	}
}

// TestRowsCoverDeclaredColumns keeps the manifest and the row builders honest:
// every column the grid renders has to exist in the payload the route returns.
func TestRowsCoverDeclaredColumns(t *testing.T) {
	cases := []struct {
		name    string
		columns []plugin.Column
		row     row
	}{
		{"job", jobColumns(), jobRow(jobStub(0))},
		{"allocation", allocColumns(), allocRow(allocStub())},
		{"node", nodeColumns(), nodeRow(nodeStub())},
		{"deployment", deploymentColumns(), deploymentRow(deployment())},
		{"evaluation", evalColumns(), evaluationRow(evaluation())},
		{"host volume", hostVolumeColumns(), hostVolumeRow(&api.HostVolumeStub{ID: testHostV, Name: "data", State: api.HostVolumeStateReady})},
		{"member", memberColumns(), memberRow(&api.AgentMember{Name: "s1", Tags: map[string]string{}}, nil)},
		{"datacenter", datacenterColumns(), datacenterRows([]*api.NodeListStub{nodeStub()})[0]},
		{"task group", taskGroupColumns(), taskGroupRow(parsedJob(), parsedJob().TaskGroups[0])},
		{"job version", jobVersionColumns(), jobVersionRow(parsedJob(), &api.JobDiff{})},
	}
	for _, tc := range cases {
		for _, column := range tc.columns {
			if _, ok := tc.row[column.Key]; !ok {
				t.Errorf("%s row is missing column %q", tc.name, column.Key)
			}
		}
	}
}

// TestWatchRowsMatchListRows guards the live grid: a patched row has to carry the
// same keys as the row the list route produced, or the cells it omits go blank.
func TestWatchRowsMatchListRows(t *testing.T) {
	listRow := jobRow(jobStub(0))
	eventRow := jobEventRow(parsedJob())
	applySummary(eventRow, &api.JobSummary{Summary: map[string]api.TaskGroupSummary{"app": {Running: 1}}})
	for key := range listRow {
		if _, ok := eventRow[key]; !ok {
			t.Errorf("job watch row is missing %q", key)
		}
	}
	nodeList := nodeRow(nodeStub())
	nodeEvent := nodeEventRow(node())
	for key := range nodeList {
		if _, ok := nodeEvent[key]; !ok {
			t.Errorf("node watch row is missing %q", key)
		}
	}
}

func TestResourceEventCarriesTheRowIdentity(t *testing.T) {
	c := newCluster(t)
	sess := newSession(t, c, nil)
	payload, _ := json.Marshal(map[string]any{"Job": parsedJob()})
	var decoded map[string]any
	_ = json.Unmarshal(payload, &decoded)
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess, nil, nil, nil)
	event, ok := sess.resourceEvent(rc, "job", &api.Event{Topic: api.TopicJob, Type: "JobRegistered", Payload: decoded})
	if !ok {
		t.Fatal("a job event should map to a resource event")
	}
	if event.Type != "modified" || event.Ref.Kind != "job" || event.Ref.Name != testJobID {
		t.Fatalf("resource event: %#v", event)
	}
	if _, ok := sess.resourceEvent(rc, "namespace", &api.Event{}); ok {
		t.Fatal("kinds without a change feed must not emit events")
	}
}

func TestAllocEventsAreNewestFirst(t *testing.T) {
	c := newCluster(t)
	sess := newSession(t, c, nil)
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess, map[string]string{"ns": "default", "alloc": testAlloc}, nil, nil)
	out, err := allocEvents(rc)
	if err != nil {
		t.Fatalf("alloc events: %v", err)
	}
	items := out.(listPage).Items
	if len(items) != 2 {
		t.Fatalf("event count: %d", len(items))
	}
	if items[0]["reason"] != api.TaskStarted {
		t.Fatalf("events should be newest first: %#v", items)
	}
}

// TestAllocMetricsPercentMatchesTheReservation keeps the gauge self-consistent:
// the panel renders cpuPercent as cpuUsed of cpuTotal, so a host-wide percentage
// would contradict the numbers printed beside it.
func TestAllocMetricsPercentMatchesTheReservation(t *testing.T) {
	c := newCluster(t)
	sess := newSession(t, c, nil)
	h := harness.NewHarness(t, routes())
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	out := h.Stream(ctx, "nomad.alloc.metrics", sess, map[string]string{"ns": "default", "alloc": testAlloc}, nil, nil)
	var frame struct {
		CPUPercent  float64 `json:"cpuPercent"`
		CPUUsed     float64 `json:"cpuUsed"`
		CPUTotal    float64 `json:"cpuTotal"`
		MemoryUsed  float64 `json:"memoryUsed"`
		MemoryTotal float64 `json:"memoryTotal"`
	}
	first := strings.SplitN(strings.TrimSpace(string(out)), "\n", 2)[0]
	if err := json.Unmarshal([]byte(first), &frame); err != nil {
		t.Fatalf("metrics frame %q: %v", first, err)
	}
	if frame.CPUTotal != 100 || frame.CPUUsed != 250 {
		t.Fatalf("metrics frame: %#v", frame)
	}
	if want := percent(frame.CPUUsed, frame.CPUTotal); frame.CPUPercent != want {
		t.Fatalf("cpuPercent %v does not match cpuUsed of cpuTotal (%v)", frame.CPUPercent, want)
	}
	if want := percent(frame.MemoryUsed, frame.MemoryTotal); frame.MemoryUsed == 0 || want == 0 {
		t.Fatalf("memory usage was not reported: %#v", frame)
	}
}

func TestClusterOverviewSummarisesTheAgent(t *testing.T) {
	c := newCluster(t)
	sess := newSession(t, c, nil)
	out, err := clusterOverview(plugin.NewRequestContext(context.Background(), plugin.User{}, sess, nil, nil, nil))
	if err != nil {
		t.Fatalf("cluster overview: %v", err)
	}
	got := out.(row)
	if got["leader"] != "127.0.0.1:4647" || got["version"] != "1.10.0" {
		t.Fatalf("cluster overview: %#v", got)
	}
	if got["clientCount"] != 1 || got["clientsReady"] != 1 {
		t.Fatalf("client counts: %#v", got)
	}
}

func TestNamespaceScopeLeadsWithTheWildcard(t *testing.T) {
	c := newCluster(t)
	sess := newSession(t, c, nil)
	out, err := namespaceScope(plugin.NewRequestContext(context.Background(), plugin.User{}, sess, nil, nil, nil))
	if err != nil {
		t.Fatalf("namespace scope: %v", err)
	}
	items := out.(listPage).Items
	if len(items) == 0 || items[0]["value"] != api.AllNamespacesNamespace {
		t.Fatalf("namespace scope options: %#v", items)
	}
	list, err := listNamespaces(plugin.NewRequestContext(context.Background(), plugin.User{}, sess, nil, nil, nil))
	if err != nil {
		t.Fatalf("namespaces: %v", err)
	}
	for _, item := range list.(listPage).Items {
		if item["name"] == api.AllNamespacesNamespace {
			t.Fatal("the namespace table must not carry the picker's wildcard entry")
		}
	}
}

// TestDatacentersRollUpTheClientSet checks the derived datacenter view: Nomad
// has no datacenter endpoint, so the numbers have to come from the clients.
func TestDatacentersRollUpTheClientSet(t *testing.T) {
	drained := nodeStub()
	drained.ID, drained.Name, drained.Drain = "dddddddd-0000-0000-0000-000000000000", "client-2", true
	drained.SchedulingEligibility = "ineligible"
	drained.Status = "down"
	other := nodeStub()
	other.ID, other.Name, other.Datacenter = "eeeeeeee-0000-0000-0000-000000000000", "client-3", "dc2"

	rows := datacenterRows([]*api.NodeListStub{nodeStub(), drained, other})
	if len(rows) != 2 {
		t.Fatalf("datacenter count: %#v", rows)
	}
	if rows[0]["name"] != "dc1" || rows[1]["name"] != "dc2" {
		t.Fatalf("datacenters should be ordered by name: %#v", rows)
	}
	dc1 := rows[0]
	if dc1["clients"] != 2 || dc1["ready"] != 1 || dc1["eligible"] != 1 || dc1["draining"] != 1 {
		t.Fatalf("dc1 rollup: %#v", dc1)
	}
	if dc1["cpu"] != int64(8000) || dc1["memory"] != int64(16384) {
		t.Fatalf("dc1 capacity should sum its clients: %#v", dc1)
	}
	if pools, _ := dc1["nodePools"].([]string); len(pools) != 1 || pools[0] != "default" {
		t.Fatalf("dc1 node pools: %#v", dc1["nodePools"])
	}
}

// TestServerMembersMarkTheRealLeader guards against deriving leadership from the
// serf "role" tag, which every Nomad server carries.
func TestServerMembersMarkTheRealLeader(t *testing.T) {
	c := newCluster(t)
	sess := newSession(t, c, nil)
	out, err := listMembers(plugin.NewRequestContext(context.Background(), plugin.User{}, sess, nil, nil, nil))
	if err != nil {
		t.Fatalf("members: %v", err)
	}
	items := out.(listPage).Items
	if len(items) != 2 {
		t.Fatalf("member count: %d", len(items))
	}
	leaders := 0
	for _, item := range items {
		if item["leader"] == true {
			leaders++
			if item["name"] != "server-1.global" {
				t.Fatalf("the wrong server was marked leader: %#v", item)
			}
		}
	}
	if leaders != 1 {
		t.Fatalf("exactly one server leads a region, got %d", leaders)
	}
}

// TestLeaderFollowsTheScopedRegion keeps the region scope on the leader lookup;
// the unscoped call always answers for the agent's own region.
func TestLeaderFollowsTheScopedRegion(t *testing.T) {
	c := newCluster(t)
	sess := newSession(t, c, map[string]any{"region": "eu"})
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess, map[string]string{"region": "us"}, nil, nil)
	if _, err := clusterOverview(rc); err != nil {
		t.Fatalf("cluster overview: %v", err)
	}
	if !c.sawQuery("/v1/status/leader", "region", "us") {
		t.Fatalf("the scoped region never reached the leader lookup: %v", c.urls)
	}
	if !c.sawQuery("/v1/nodes", "region", "us") {
		t.Fatalf("the scoped region never reached the node count: %v", c.urls)
	}
}

// TestNamespacesWalkEveryPage catches the truncated-picker regression: a single
// upstream page would silently hide every namespace past the first response.
func TestNamespacesWalkEveryPage(t *testing.T) {
	c := newCluster(t)
	c.namespaceCount = 5
	sess := newSession(t, c, nil)
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess, nil, url.Values{"limit": {"100"}}, nil)

	out, err := listNamespaces(rc)
	if err != nil {
		t.Fatalf("namespaces: %v", err)
	}
	page := out.(listPage)
	if len(page.Items) != 5 {
		t.Fatalf("namespace walk stopped early: got %d of 5", len(page.Items))
	}
	if page.Total == nil || *page.Total != 5 {
		t.Fatalf("a complete walk reports the total: %v", page.Total)
	}

	scope, err := namespaceScope(plugin.NewRequestContext(context.Background(), plugin.User{}, sess, nil, url.Values{"limit": {"100"}}, nil))
	if err != nil {
		t.Fatalf("namespace scope: %v", err)
	}
	if items := scope.(listPage).Items; len(items) != 6 {
		t.Fatalf("the picker should carry the wildcard plus every namespace, got %d", len(items))
	}
}

// TestNamespaceWalkStopsAtTheScanLimit proves the walk stays bounded.
func TestNamespaceWalkStopsAtTheScanLimit(t *testing.T) {
	c := newCluster(t)
	c.namespaceCount = 6
	sess := newSession(t, c, nil)
	sess.opts.ScanLimit = 2

	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess, nil, url.Values{"limit": {"100"}}, nil)
	namespaces, whole, err := sess.namespaces(rc)
	if err != nil {
		t.Fatalf("namespaces: %v", err)
	}
	if len(namespaces) != 2 {
		t.Fatalf("the walk must stop at the scan limit, got %d", len(namespaces))
	}
	if whole {
		t.Fatal("a capped walk must not claim it read the whole set")
	}

	out, err := listNamespaces(rc)
	if err != nil {
		t.Fatalf("namespaces: %v", err)
	}
	page := out.(listPage)
	if !page.Truncated || page.ScanLimit != 2 {
		t.Fatalf("a capped listing must say so: truncated=%v limit=%d", page.Truncated, page.ScanLimit)
	}
	if page.Total != nil {
		t.Fatalf("a capped listing cannot report a total: %v", page.Total)
	}
}

// TestJobWatchRowKeepsSummaryKeys guards the live grid: an unreachable summary
// must not produce a patch that blanks the allocation columns.
func TestJobWatchRowKeepsSummaryKeys(t *testing.T) {
	c := newCluster(t)
	sess := newSession(t, c, nil)
	missing := parsedJob()
	missing.ID = ptr("gone")
	payload, _ := json.Marshal(map[string]any{"Job": missing})
	var decoded map[string]any
	_ = json.Unmarshal(payload, &decoded)

	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess, nil, nil, nil)
	event, ok := sess.resourceEvent(rc, "job", &api.Event{Topic: api.TopicJob, Type: "JobRegistered", Payload: decoded})
	if !ok {
		t.Fatal("a job event should map to a resource event")
	}
	patched, _ := event.Resource.(row)
	for _, key := range []string{"running", "queued", "failed", "complete", "lost", "starting"} {
		if _, present := patched[key]; !present {
			t.Errorf("watch row dropped %q when the summary was unreachable", key)
		}
	}
}

func TestTreeGroupsOpenAPagedAllocationList(t *testing.T) {
	c := newCluster(t)
	sess := newSession(t, c, nil)
	out, err := treeGroups(plugin.NewRequestContext(context.Background(), plugin.User{}, sess, map[string]string{"ns": "default", "job": testJobID}, nil, nil))
	if err != nil {
		t.Fatalf("tree groups: %v", err)
	}
	nodes := out.(plugin.Page[plugin.TreeNode]).Items
	if len(nodes) != 1 {
		t.Fatalf("group node count: %d", len(nodes))
	}
	group := nodes[0]
	if !group.Leaf || group.ResourceKind != "allocation" {
		t.Fatalf("a task group should open the allocation grid: %#v", group)
	}
	if group.ListParams["job"] != testJobID || group.ListParams["group"] != "app" || group.ListParams["ns"] != "default" {
		t.Fatalf("group list params: %#v", group.ListParams)
	}
}

func TestJobSpecPrefersTheSubmittedSource(t *testing.T) {
	c := newCluster(t)
	sess := newSession(t, c, nil)
	out, err := jobSpec(plugin.NewRequestContext(context.Background(), plugin.User{}, sess, map[string]string{"ns": "default", "job": testJobID}, nil, nil))
	if err != nil {
		t.Fatalf("job spec: %v", err)
	}
	if source, ok := out.(string); !ok || !strings.Contains(source, "job \"web\"") {
		t.Fatalf("job spec should return the submitted HCL, got %#v", out)
	}
}
