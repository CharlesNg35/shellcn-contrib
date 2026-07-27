package nomad

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/hashicorp/nomad/api"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type actionResult struct {
	OK      bool   `json:"ok"`
	EvalID  string `json:"evalId,omitempty"`
	Message string `json:"message,omitempty"`
}

// maxBulkTargets caps a fan-out write (restarting every allocation of a job) so
// one click cannot turn into an unbounded number of upstream calls.
const maxBulkTargets = 200

func routes() []plugin.Route {
	return []plugin.Route{
		{ID: "nomad.cluster.list", Method: plugin.MethodGet, Path: "/cluster", Permission: "nomad.cluster.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.cluster.list", Handle: listCluster},
		{ID: "nomad.cluster.overview", Method: plugin.MethodGet, Path: "/cluster/overview", Permission: "nomad.cluster.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.cluster.overview", Handle: clusterOverview},
		{ID: "nomad.members.list", Method: plugin.MethodGet, Path: "/members", Permission: "nomad.cluster.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.members.list", Handle: listMembers},
		{ID: "nomad.regions.list", Method: plugin.MethodGet, Path: "/regions", Permission: "nomad.cluster.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.regions.list", Handle: listRegions},
		{ID: "nomad.datacenters.list", Method: plugin.MethodGet, Path: "/datacenters", Permission: "nomad.nodes.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.datacenters.list", Handle: listDatacenters},
		{ID: "nomad.namespaces.list", Method: plugin.MethodGet, Path: "/namespaces", Permission: "nomad.cluster.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.namespaces.list", Handle: listNamespaces},
		{ID: "nomad.scope.namespaces", Method: plugin.MethodGet, Path: "/scope/namespaces", Permission: "nomad.cluster.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.scope.namespaces", Handle: namespaceScope},

		{ID: "nomad.resources.watch", Method: plugin.MethodWS, Path: "/watch/{kind}", Permission: "nomad.cluster.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.resources.watch", Stream: watchResources},

		{ID: "nomad.tree.namespaces", Method: plugin.MethodGet, Path: "/tree/namespaces", Permission: "nomad.cluster.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.tree.namespaces", Handle: treeNamespaces},
		{ID: "nomad.tree.jobs", Method: plugin.MethodGet, Path: "/tree/jobs", Permission: "nomad.jobs.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.tree.jobs", Handle: treeJobs},
		{ID: "nomad.tree.groups", Method: plugin.MethodGet, Path: "/tree/groups", Permission: "nomad.jobs.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.tree.groups", Handle: treeGroups},

		{ID: "nomad.jobs.list", Method: plugin.MethodGet, Path: "/jobs", Permission: "nomad.jobs.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.jobs.list", Handle: listJobs},
		{ID: "nomad.job.overview", Method: plugin.MethodGet, Path: "/jobs/{job}", Permission: "nomad.jobs.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.job.overview", Handle: jobOverview},
		{ID: "nomad.job.watch", Method: plugin.MethodWS, Path: "/jobs/{job}/watch", Permission: "nomad.jobs.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.job.watch", Stream: watchJob},
		{ID: "nomad.job.spec", Method: plugin.MethodGet, Path: "/jobs/{job}/spec", Permission: "nomad.jobs.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.job.spec", Handle: jobSpec},
		{ID: "nomad.job.groups", Method: plugin.MethodGet, Path: "/jobs/{job}/groups", Permission: "nomad.jobs.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.job.groups", Handle: jobGroups},
		{ID: "nomad.job.versions", Method: plugin.MethodGet, Path: "/jobs/{job}/versions", Permission: "nomad.jobs.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.job.versions", Handle: jobVersions},
		{ID: "nomad.job.plan", Method: plugin.MethodPost, Path: "/jobs/plan", Permission: "nomad.jobs.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.job.plan", Input: jobSpecSchema(), Handle: planJob},
		{ID: "nomad.job.submit", Method: plugin.MethodPost, Path: "/jobs", Permission: "nomad.jobs.write", Risk: plugin.RiskWrite, AuditEvent: "nomad.job.submit", Handle: submitJob},
		{ID: "nomad.job.spec.save", Method: plugin.MethodPut, Path: "/jobs/{job}/spec", Permission: "nomad.jobs.write", Risk: plugin.RiskWrite, AuditEvent: "nomad.job.spec.save", Handle: saveJobSpec},
		{ID: "nomad.job.restart", Method: plugin.MethodPost, Path: "/jobs/{job}/restart", Permission: "nomad.jobs.write", Risk: plugin.RiskWrite, AuditEvent: "nomad.job.restart", Handle: restartJob},
		{ID: "nomad.job.revert", Method: plugin.MethodPost, Path: "/jobs/{job}/revert", Permission: "nomad.jobs.write", Risk: plugin.RiskWrite, AuditEvent: "nomad.job.revert", Input: jobRevertSchema(), Handle: revertJob},
		{ID: "nomad.job.scale", Method: plugin.MethodPost, Path: "/jobs/{job}/scale", Permission: "nomad.jobs.write", Risk: plugin.RiskWrite, AuditEvent: "nomad.job.scale", Input: jobScaleSchema(), Handle: scaleJob},
		{ID: "nomad.job.evaluate", Method: plugin.MethodPost, Path: "/jobs/{job}/evaluate", Permission: "nomad.jobs.write", Risk: plugin.RiskWrite, AuditEvent: "nomad.job.evaluate", Handle: evaluateJob},
		{ID: "nomad.job.periodic", Method: plugin.MethodPost, Path: "/jobs/{job}/periodic", Permission: "nomad.jobs.write", Risk: plugin.RiskWrite, AuditEvent: "nomad.job.periodic", Handle: forcePeriodicJob},
		{ID: "nomad.job.stop", Method: plugin.MethodDelete, Path: "/jobs/{job}", Permission: "nomad.jobs.delete", Risk: plugin.RiskDestructive, AuditEvent: "nomad.job.stop", Handle: stopJob},
		{ID: "nomad.job.purge", Method: plugin.MethodDelete, Path: "/jobs/{job}/purge", Permission: "nomad.jobs.delete", Risk: plugin.RiskDestructive, AuditEvent: "nomad.job.purge", Handle: purgeJob},

		{ID: "nomad.allocs.list", Method: plugin.MethodGet, Path: "/allocations", Permission: "nomad.allocations.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.allocs.list", Handle: listAllocations},
		{ID: "nomad.alloc.overview", Method: plugin.MethodGet, Path: "/allocations/{alloc}", Permission: "nomad.allocations.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.alloc.overview", Handle: allocOverview},
		{ID: "nomad.alloc.watch", Method: plugin.MethodWS, Path: "/allocations/{alloc}/watch", Permission: "nomad.allocations.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.alloc.watch", Stream: watchAllocation},
		{ID: "nomad.alloc.events", Method: plugin.MethodGet, Path: "/allocations/{alloc}/events", Permission: "nomad.allocations.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.alloc.events", Handle: allocEvents},
		{ID: "nomad.alloc.tasks", Method: plugin.MethodGet, Path: "/allocations/{alloc}/tasks", Permission: "nomad.allocations.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.alloc.tasks", Handle: allocTasks},
		{ID: "nomad.log.types", Method: plugin.MethodGet, Path: "/log-types", Permission: "nomad.allocations.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.log.types", Handle: logTypes},
		{ID: "nomad.alloc.logs", Method: plugin.MethodWS, Path: "/allocations/{alloc}/logs", Permission: "nomad.allocations.logs", Risk: plugin.RiskSafe, AuditEvent: "nomad.alloc.logs", Stream: streamAllocLogs},
		{ID: "nomad.alloc.metrics", Method: plugin.MethodWS, Path: "/allocations/{alloc}/metrics", Permission: "nomad.allocations.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.alloc.metrics", Stream: streamAllocMetrics},
		{ID: "nomad.alloc.exec", Method: plugin.MethodWS, Path: "/allocations/{alloc}/exec", Permission: "nomad.allocations.exec", Risk: plugin.RiskPrivileged, AuditEvent: "nomad.alloc.exec", Stream: streamAllocExec},
		{ID: "nomad.alloc.restart", Method: plugin.MethodPost, Path: "/allocations/{alloc}/restart", Permission: "nomad.allocations.write", Risk: plugin.RiskWrite, AuditEvent: "nomad.alloc.restart", Input: allocRestartSchema(), Handle: restartAllocation},
		{ID: "nomad.alloc.signal", Method: plugin.MethodPost, Path: "/allocations/{alloc}/signal", Permission: "nomad.allocations.write", Risk: plugin.RiskWrite, AuditEvent: "nomad.alloc.signal", Input: allocSignalSchema(), Handle: signalAllocation},
		{ID: "nomad.alloc.stop", Method: plugin.MethodPost, Path: "/allocations/{alloc}/stop", Permission: "nomad.allocations.delete", Risk: plugin.RiskDestructive, AuditEvent: "nomad.alloc.stop", Handle: stopAllocation},

		{ID: "nomad.nodes.list", Method: plugin.MethodGet, Path: "/nodes", Permission: "nomad.nodes.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.nodes.list", Handle: listNodes},
		{ID: "nomad.node.overview", Method: plugin.MethodGet, Path: "/nodes/{node}", Permission: "nomad.nodes.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.node.overview", Handle: nodeOverview},
		{ID: "nomad.node.watch", Method: plugin.MethodWS, Path: "/nodes/{node}/watch", Permission: "nomad.nodes.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.node.watch", Stream: watchNode},
		{ID: "nomad.node.metrics", Method: plugin.MethodWS, Path: "/nodes/{node}/metrics", Permission: "nomad.nodes.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.node.metrics", Stream: streamNodeMetrics},
		{ID: "nomad.node.eligibility", Method: plugin.MethodPost, Path: "/nodes/{node}/eligibility", Permission: "nomad.nodes.write", Risk: plugin.RiskWrite, AuditEvent: "nomad.node.eligibility", Input: nodeEligibilitySchema(), Handle: setNodeEligibility},
		{ID: "nomad.node.drain.cancel", Method: plugin.MethodDelete, Path: "/nodes/{node}/drain", Permission: "nomad.nodes.write", Risk: plugin.RiskWrite, AuditEvent: "nomad.node.drain.cancel", Handle: cancelNodeDrain},
		{ID: "nomad.node.drain", Method: plugin.MethodPost, Path: "/nodes/{node}/drain", Permission: "nomad.nodes.drain", Risk: plugin.RiskDestructive, AuditEvent: "nomad.node.drain", Input: nodeDrainSchema(), Handle: drainNode},

		{ID: "nomad.deployments.list", Method: plugin.MethodGet, Path: "/deployments", Permission: "nomad.deployments.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.deployments.list", Handle: listDeployments},
		{ID: "nomad.deployment.overview", Method: plugin.MethodGet, Path: "/deployments/{deployment}", Permission: "nomad.deployments.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.deployment.overview", Handle: deploymentOverview},
		{ID: "nomad.deployment.progress", Method: plugin.MethodWS, Path: "/deployments/{deployment}/progress", Permission: "nomad.deployments.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.deployment.progress", Stream: streamDeploymentProgress},
		{ID: "nomad.deployment.promote", Method: plugin.MethodPost, Path: "/deployments/{deployment}/promote", Permission: "nomad.deployments.write", Risk: plugin.RiskWrite, AuditEvent: "nomad.deployment.promote", Handle: promoteDeployment},
		{ID: "nomad.deployment.pause", Method: plugin.MethodPost, Path: "/deployments/{deployment}/pause", Permission: "nomad.deployments.write", Risk: plugin.RiskWrite, AuditEvent: "nomad.deployment.pause", Input: deploymentPauseSchema(), Handle: pauseDeployment},
		{ID: "nomad.deployment.unblock", Method: plugin.MethodPost, Path: "/deployments/{deployment}/unblock", Permission: "nomad.deployments.write", Risk: plugin.RiskWrite, AuditEvent: "nomad.deployment.unblock", Handle: unblockDeployment},
		{ID: "nomad.deployment.fail", Method: plugin.MethodPost, Path: "/deployments/{deployment}/fail", Permission: "nomad.deployments.delete", Risk: plugin.RiskDestructive, AuditEvent: "nomad.deployment.fail", Handle: failDeployment},

		{ID: "nomad.evals.list", Method: plugin.MethodGet, Path: "/evaluations", Permission: "nomad.evaluations.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.evals.list", Handle: listEvaluations},
		{ID: "nomad.eval.overview", Method: plugin.MethodGet, Path: "/evaluations/{eval}", Permission: "nomad.evaluations.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.eval.overview", Handle: evaluationOverview},

		{ID: "nomad.volumes.list", Method: plugin.MethodGet, Path: "/volumes", Permission: "nomad.volumes.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.volumes.list", Handle: listVolumes},
		{ID: "nomad.volume.overview", Method: plugin.MethodGet, Path: "/volumes/{volume}", Permission: "nomad.volumes.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.volume.overview", Handle: volumeOverview},
		{ID: "nomad.hostvolumes.list", Method: plugin.MethodGet, Path: "/host-volumes", Permission: "nomad.volumes.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.hostvolumes.list", Handle: listHostVolumes},
		{ID: "nomad.hostvolume.overview", Method: plugin.MethodGet, Path: "/host-volumes/{volume}", Permission: "nomad.volumes.read", Risk: plugin.RiskSafe, AuditEvent: "nomad.hostvolume.overview", Handle: hostVolumeOverview},
	}
}

const sampleJobHCL = `job "example" {
  type = "service"

  group "app" {
    count = 1

    task "server" {
      driver = "docker"

      config {
        image = "nginx:alpine"
      }

      resources {
        cpu    = 100
        memory = 128
      }
    }
  }
}
`

func jobSpecSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Job", Fields: []plugin.Field{
		{Key: "content", Label: "Job specification", Type: plugin.FieldTextarea, Required: true, Default: sampleJobHCL, Help: "HCL2 or JSON job specification."},
		{Key: "ns", Label: "Namespace", Type: plugin.FieldSelect, OptionsSource: &plugin.DataSource{RouteID: "nomad.namespaces.list"}, Help: "Leave empty to use the connection namespace."},
	}}}}
}

func jobRevertSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Revert", Fields: []plugin.Field{
		{Key: "version", Label: "Target version", Type: plugin.FieldNumber, Required: true, Default: "${record.version}", Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 0}}},
	}}}}
}

func jobScaleSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Scale", Fields: []plugin.Field{
		{Key: "group", Label: "Task group", Type: plugin.FieldText, Required: true, Default: "${record.name}"},
		{Key: "count", Label: "Count", Type: plugin.FieldNumber, Required: true, Default: "${record.count}", Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 0}, {Type: plugin.ValidatorMax, Value: 10000}}},
		{Key: "message", Label: "Reason", Type: plugin.FieldText, Placeholder: "Scaled from ShellCN"},
	}}}}
}

func allocRestartSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Restart", Fields: []plugin.Field{
		{Key: "task", Label: "Task", Type: plugin.FieldText, Help: "Leave empty to restart every task in the allocation."},
	}}}}
}

func allocSignalSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Signal", Fields: []plugin.Field{
		{Key: "signal", Label: "Signal", Type: plugin.FieldSelect, Required: true, Default: "SIGHUP", Options: []plugin.Option{
			{Label: "SIGHUP", Value: "SIGHUP"},
			{Label: "SIGINT", Value: "SIGINT"},
			{Label: "SIGTERM", Value: "SIGTERM"},
			{Label: "SIGUSR1", Value: "SIGUSR1"},
			{Label: "SIGUSR2", Value: "SIGUSR2"},
		}},
		{Key: "task", Label: "Task", Type: plugin.FieldText, Help: "Leave empty to signal every task in the allocation."},
	}}}}
}

func nodeDrainSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Drain", Fields: []plugin.Field{
		{Key: "deadline", Label: "Deadline", Type: plugin.FieldDuration, Default: "1h", Help: "Force-stop remaining allocations after this long. 0 waits forever."},
		{Key: "ignore_system_jobs", Label: "Keep system jobs", Type: plugin.FieldToggle, Default: false},
	}}}}
}

func nodeEligibilitySchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Scheduling", Fields: []plugin.Field{
		{Key: "eligible", Label: "Eligible for scheduling", Type: plugin.FieldToggle, Default: true},
	}}}}
}

func deploymentPauseSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Rollout", Fields: []plugin.Field{
		{Key: "pause", Label: "Pause the rollout", Type: plugin.FieldToggle, Default: true, Help: "Turn off to resume a paused rollout."},
	}}}}
}

func nomadSession(rc *plugin.RequestContext) (*Session, error) {
	return unwrap(rc.Session)
}

// param reads a renderer-supplied value from either the resolved route params or
// the p.* query args a stream source carries.
func param(rc *plugin.RequestContext, name string) string {
	if v := strings.TrimSpace(rc.Param(name)); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p." + name))
}

// listNamespace resolves the namespace a listing runs in: a resource-scoped "ns"
// wins, then the workspace namespace picker, then the connection default.
func listNamespace(rc *plugin.RequestContext, s *Session) string {
	if v := param(rc, "ns"); v != "" {
		return v
	}
	if v := param(rc, "namespace"); v != "" {
		return v
	}
	return s.opts.Namespace
}

// objectNamespace is listNamespace without the wildcard, because single-object
// endpoints address exactly one namespace.
func objectNamespace(rc *plugin.RequestContext, s *Session) string {
	if ns := listNamespace(rc, s); ns != api.AllNamespacesNamespace {
		return ns
	}
	return s.opts.Namespace
}

func (s *Session) listQuery(rc *plugin.RequestContext) *api.QueryOptions {
	q := &api.QueryOptions{Region: regionOf(rc, s), Namespace: listNamespace(rc, s)}
	return q.WithContext(rc.Ctx)
}

func (s *Session) objectQuery(rc *plugin.RequestContext) *api.QueryOptions {
	q := &api.QueryOptions{Region: regionOf(rc, s), Namespace: objectNamespace(rc, s)}
	return q.WithContext(rc.Ctx)
}

func regionOf(rc *plugin.RequestContext, s *Session) string {
	if v := param(rc, "region"); v != "" {
		return v
	}
	return s.opts.Region
}

func requiredParam(rc *plugin.RequestContext, name, label string) (string, error) {
	value := param(rc, name)
	if value == "" {
		return "", fmt.Errorf("%w: %s is required", plugin.ErrInvalidInput, label)
	}
	return value, nil
}

// leader resolves the Raft leader of the region the request is scoped to; the
// unscoped call would always answer for the agent's own region.
func (s *Session) leader(rc *plugin.RequestContext) (string, error) {
	if region := regionOf(rc, s); region != "" {
		return s.client.Status().RegionLeader(region)
	}
	return s.client.Status().Leader()
}

func listCluster(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	leader, _ := s.leader(rc)
	return staticPage(rc, []row{{
		"name":    s.opts.Address,
		"region":  regionOf(rc, s),
		"leader":  leader,
		"address": s.opts.Address,
		"ref":     plugin.ResourceIdentity{Kind: "cluster", Name: "cluster", UID: "cluster"},
	}})
}

func clusterOverview(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	leader, err := s.leader(rc)
	if err != nil {
		return nil, nomadErr(err)
	}
	out := row{
		"address":     s.opts.Address,
		"leader":      leader,
		"region":      regionOf(rc, s),
		"namespace":   s.opts.Namespace,
		"readOnly":    s.opts.ReadOnly,
		"execAllowed": s.opts.AllowExec,
	}
	if peers, err := s.client.Status().Peers(); err == nil {
		out["peers"] = peers
		out["serverCount"] = len(peers)
	}
	if self, err := s.client.Agent().Self(); err == nil {
		out["version"] = agentStat(self, "nomad", "version")
		out["nodeName"] = self.Member.Name
		out["datacenter"] = self.Member.Tags["dc"]
		if out["region"] == "" {
			out["region"] = self.Member.Tags["region"]
		}
	}
	if regions, err := s.client.Regions().List(); err == nil {
		out["regions"] = regions
	}
	if nodes, whole, err := walkAll(s, (&api.QueryOptions{Region: regionOf(rc, s)}).WithContext(rc.Ctx), s.client.Nodes().List); err == nil {
		ready := 0
		for _, node := range nodes {
			if node.Status == api.NodeStatusReady {
				ready++
			}
		}
		out["clientCount"] = len(nodes)
		out["clientsReady"] = ready
		// A capped walk cannot claim an exact count, so the counts are labelled
		// as a floor rather than quietly under-reporting the cluster.
		out["clientCountPartial"] = !whole
	}
	return out, nil
}

func listMembers(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	members, err := s.client.Agent().MembersOpts((&api.QueryOptions{Region: regionOf(rc, s)}).WithContext(rc.Ctx))
	if err != nil {
		return nil, nomadErr(err)
	}
	leaders := s.regionLeaders(members.Members)
	rows := make([]row, 0, len(members.Members))
	for _, member := range members.Members {
		rows = append(rows, memberRow(member, leaders))
	}
	return staticPage(rc, rows)
}

// regionLeaders maps each region present in the member set to its leader's RPC
// address. Serf membership carries no leadership flag, so the only way to tell
// which server is leading is to compare against the region's reported leader.
func (s *Session) regionLeaders(members []*api.AgentMember) map[string]string {
	out := map[string]string{}
	for _, member := range members {
		region := member.Tags["region"]
		if region == "" {
			continue
		}
		if _, ok := out[region]; ok {
			continue
		}
		leader, err := s.client.Status().RegionLeader(region)
		if err != nil {
			continue
		}
		out[region] = leader
	}
	return out
}

func listRegions(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	regions, err := s.client.Regions().List()
	if err != nil {
		return nil, nomadErr(err)
	}
	rows := make([]row, 0, len(regions))
	for _, region := range regions {
		rows = append(rows, row{"name": region, "value": region, "label": region})
	}
	return staticPage(rc, rows)
}

func listDatacenters(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	base := (&api.QueryOptions{Region: regionOf(rc, s), Params: map[string]string{"resources": "true"}}).WithContext(rc.Ctx)
	nodes, whole, err := walkAll(s, base, s.client.Nodes().List)
	if err != nil {
		return nil, err
	}
	page, err := staticPage(rc, datacenterRows(nodes))
	if err != nil {
		return nil, err
	}
	return markTruncated(page, whole, s.opts.ScanLimit), nil
}

func listNamespaces(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	namespaces, whole, err := s.namespaces(rc)
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(namespaces))
	for _, ns := range namespaces {
		rows = append(rows, namespaceRow(ns))
	}
	page, err := staticPage(rc, rows)
	if err != nil {
		return nil, err
	}
	return markTruncated(page, whole, s.opts.ScanLimit), nil
}

// namespaceScope backs the workspace namespace picker, which needs the wildcard
// entry the namespace listing itself must not carry.
func namespaceScope(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	namespaces, whole, err := s.namespaces(rc)
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(namespaces)+1)
	rows = append(rows, row{"value": api.AllNamespacesNamespace, "label": "All namespaces"})
	for _, ns := range namespaces {
		rows = append(rows, row{"value": ns.Name, "label": ns.Name})
	}
	page, err := staticPage(rc, rows)
	if err != nil {
		return nil, err
	}
	return markTruncated(page, whole, s.opts.ScanLimit), nil
}

func (s *Session) namespaces(rc *plugin.RequestContext) ([]*api.Namespace, bool, error) {
	base := (&api.QueryOptions{Region: regionOf(rc, s)}).WithContext(rc.Ctx)
	return walkAll(s, base, s.client.Namespaces().List)
}

func treeNamespaces(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	namespaces, whole, err := s.namespaces(rc)
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(namespaces))
	for _, ns := range namespaces {
		rows = append(rows, namespaceRow(ns))
	}
	page, err := staticPage(rc, rows)
	if err != nil {
		return nil, err
	}
	page = markTruncated(page, whole, s.opts.ScanLimit)
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name := fmt.Sprint(item["name"])
		nodes = append(nodes, plugin.TreeNode{
			Key:            "namespace:" + name,
			Label:          name,
			Icon:           icon("folder"),
			ChildrenSource: &plugin.DataSource{RouteID: "nomad.tree.jobs", Params: map[string]string{"ns": name}},
		})
	}
	return treePage(page, nodes), nil
}

func treeJobs(rc *plugin.RequestContext) (any, error) {
	page, err := jobsPage(rc)
	if err != nil {
		return nil, err
	}
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		id, ns := fmt.Sprint(item["id"]), fmt.Sprint(item["namespace"])
		ref := jobIdentity(ns, id)
		nodes = append(nodes, plugin.TreeNode{
			Key:            "job:" + ref.UID,
			Label:          fmt.Sprint(item["name"]),
			Icon:           icon("briefcase"),
			Ref:            &ref,
			ChildrenSource: &plugin.DataSource{RouteID: "nomad.tree.groups", Params: map[string]string{"ns": ns, "job": id}},
			Data:           map[string]any{"status": item["status"], "type": item["type"]},
		})
	}
	return treePage(page, nodes), nil
}

func treeGroups(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	jobID, err := requiredParam(rc, "job", "job")
	if err != nil {
		return nil, err
	}
	ns := objectNamespace(rc, s)
	job, _, err := s.client.Jobs().Info(jobID, s.objectQuery(rc))
	if err != nil {
		return nil, nomadErr(err)
	}
	rows := make([]row, 0, len(job.TaskGroups))
	for _, group := range job.TaskGroups {
		rows = append(rows, taskGroupRow(job, group))
	}
	page, err := staticPage(rc, rows)
	if err != nil {
		return nil, err
	}
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name := fmt.Sprint(item["name"])
		nodes = append(nodes, plugin.TreeNode{
			Key:          "group:" + ns + "/" + jobID + "/" + name,
			Label:        name,
			Icon:         icon("layers"),
			Leaf:         true,
			ResourceKind: "allocation",
			ListParams:   map[string]string{"ns": ns, "job": jobID, "group": name},
			Data:         map[string]any{"count": item["count"]},
		})
	}
	return treePage(page, nodes), nil
}

func jobsPage(rc *plugin.RequestContext) (listPage, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return listPage{}, err
	}
	return s.pageTokens(rc, s.listQuery(rc), func(q *api.QueryOptions) ([]row, string, error) {
		stubs, meta, err := s.client.Jobs().List(q)
		if err != nil {
			return nil, "", err
		}
		rows := make([]row, 0, len(stubs))
		for _, stub := range stubs {
			rows = append(rows, jobRow(stub))
		}
		return rows, nextToken(meta), nil
	})
}

func listJobs(rc *plugin.RequestContext) (any, error) {
	return jobsPage(rc)
}

func (s *Session) job(rc *plugin.RequestContext) (*api.Job, error) {
	jobID, err := requiredParam(rc, "job", "job")
	if err != nil {
		return nil, err
	}
	job, _, err := s.client.Jobs().Info(jobID, s.objectQuery(rc))
	if err != nil {
		return nil, nomadErr(err)
	}
	return job, nil
}

func jobOverview(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	job, err := s.job(rc)
	if err != nil {
		return nil, err
	}
	out := jobDetail(job)
	if summary, _, err := s.client.Jobs().Summary(deref(job.ID), s.objectQuery(rc)); err == nil {
		applySummary(out, summary)
	}
	if deployment, _, err := s.client.Jobs().LatestDeployment(deref(job.ID), s.objectQuery(rc)); err == nil && deployment != nil {
		out["latestDeployment"] = deployment.ID
		out["deploymentStatus"] = deployment.Status
	}
	return out, nil
}

func jobSpec(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	job, err := s.job(rc)
	if err != nil {
		return nil, err
	}
	version := 0
	if job.Version != nil {
		version = int(*job.Version)
	}
	if submission, _, err := s.client.Jobs().Submission(deref(job.ID), version, s.objectQuery(rc)); err == nil && submission != nil && strings.TrimSpace(submission.Source) != "" {
		return submission.Source, nil
	}
	encoded, err := json.MarshalIndent(job, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	return string(encoded), nil
}

func jobGroups(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	job, err := s.job(rc)
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(job.TaskGroups))
	for _, group := range job.TaskGroups {
		rows = append(rows, taskGroupRow(job, group))
	}
	return staticPage(rc, rows)
}

func jobVersions(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	jobID, err := requiredParam(rc, "job", "job")
	if err != nil {
		return nil, err
	}
	versions, diffs, _, err := s.client.Jobs().Versions(jobID, true, s.objectQuery(rc))
	if err != nil {
		return nil, nomadErr(err)
	}
	rows := make([]row, 0, len(versions))
	for i, version := range versions {
		var diff *api.JobDiff
		if i < len(diffs) {
			diff = diffs[i]
		}
		rows = append(rows, jobVersionRow(version, diff))
	}
	return staticPage(rc, rows)
}

// parseJobSpec turns the editor buffer into a job, accepting both a JSON job
// document and HCL2 that only the server can parse. The submission travels with
// the job on registration so the spec panel reads back the source the operator
// wrote instead of the server's canonical JSON.
func (s *Session) parseJobSpec(content, namespace string) (*api.Job, *api.JobSubmission, error) {
	content = strings.TrimSpace(content)
	if content == "" {
		return nil, nil, fmt.Errorf("%w: job specification is required", plugin.ErrInvalidInput)
	}
	submission := &api.JobSubmission{Source: content, Format: "hcl2"}
	var job *api.Job
	if strings.HasPrefix(content, "{") {
		submission.Format = "json"
		var wrapper struct {
			Job *api.Job `json:"Job"`
		}
		if err := json.Unmarshal([]byte(content), &wrapper); err != nil {
			return nil, nil, fmt.Errorf("%w: job specification is not valid JSON: %v", plugin.ErrInvalidInput, err)
		}
		job = wrapper.Job
		if job == nil {
			var direct api.Job
			if err := json.Unmarshal([]byte(content), &direct); err != nil {
				return nil, nil, fmt.Errorf("%w: job specification is not valid JSON: %v", plugin.ErrInvalidInput, err)
			}
			job = &direct
		}
	} else {
		parsed, err := s.client.Jobs().ParseHCLOpts(&api.JobsParseRequest{JobHCL: content, Canonicalize: true})
		if err != nil {
			return nil, nil, fmt.Errorf("%w: %v", plugin.ErrInvalidInput, err)
		}
		job = parsed
	}
	if job == nil || deref(job.ID) == "" {
		return nil, nil, fmt.Errorf("%w: job specification has no job ID", plugin.ErrInvalidInput)
	}
	if namespace != "" {
		job.Namespace = &namespace
	}
	return job, submission, nil
}

type specRequest struct {
	Content string `json:"content"`
	NS      string `json:"ns"`
}

func planJob(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	var req specRequest
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	ns := namespaceOrDefault(req.NS, objectNamespace(rc, s))
	job, _, err := s.parseJobSpec(req.Content, ns)
	if err != nil {
		return nil, err
	}
	ctx, cancel := commandContext(rc.Ctx, s)
	defer cancel()
	resp, _, err := s.client.Jobs().PlanOpts(job, &api.PlanOptions{Diff: true}, s.writeOptions(ctx, ns))
	if err != nil {
		return nil, nomadErr(err)
	}
	return planResult(job, resp), nil
}

func submitJob(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req specRequest
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	ns := namespaceOrDefault(req.NS, objectNamespace(rc, s))
	return s.registerSpec(rc, req.Content, ns, "")
}

func saveJobSpec(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	jobID, err := requiredParam(rc, "job", "job")
	if err != nil {
		return nil, err
	}
	var req specRequest
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	return s.registerSpec(rc, req.Content, objectNamespace(rc, s), jobID)
}

// registerSpec submits a parsed spec, refusing an edit whose job ID no longer
// matches the job the panel was opened on.
func (s *Session) registerSpec(rc *plugin.RequestContext, content, namespace, expectID string) (any, error) {
	job, submission, err := s.parseJobSpec(content, namespace)
	if err != nil {
		return nil, err
	}
	if expectID != "" && deref(job.ID) != expectID {
		return nil, fmt.Errorf("%w: specification declares job %q but this panel edits %q", plugin.ErrInvalidInput, deref(job.ID), expectID)
	}
	ctx, cancel := commandContext(rc.Ctx, s)
	defer cancel()
	resp, _, err := s.client.Jobs().RegisterOpts(job, &api.RegisterOptions{Submission: submission}, s.writeOptions(ctx, namespace))
	if err != nil {
		return nil, nomadErr(err)
	}
	return row{
		"ok":       true,
		"job":      deref(job.ID),
		"evalId":   resp.EvalID,
		"warnings": resp.Warnings,
		"content":  content,
	}, nil
}

func stopJob(rc *plugin.RequestContext) (any, error) {
	return deregisterJob(rc, false)
}

func purgeJob(rc *plugin.RequestContext) (any, error) {
	return deregisterJob(rc, true)
}

func deregisterJob(rc *plugin.RequestContext, purge bool) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	jobID, err := requiredParam(rc, "job", "job")
	if err != nil {
		return nil, err
	}
	ctx, cancel := commandContext(rc.Ctx, s)
	defer cancel()
	evalID, _, err := s.client.Jobs().DeregisterOpts(jobID, &api.DeregisterOptions{Purge: purge}, s.writeOptions(ctx, objectNamespace(rc, s)))
	if err != nil {
		return nil, nomadErr(err)
	}
	return actionResult{OK: true, EvalID: evalID}, nil
}

func restartJob(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	jobID, err := requiredParam(rc, "job", "job")
	if err != nil {
		return nil, err
	}
	q := s.objectQuery(rc)
	stubs, _, err := s.client.Jobs().Allocations(jobID, false, q)
	if err != nil {
		return nil, nomadErr(err)
	}
	restarted, skipped := 0, 0
	for _, stub := range stubs {
		if stub.ClientStatus != api.AllocClientStatusRunning {
			continue
		}
		if restarted >= maxBulkTargets {
			skipped++
			continue
		}
		if err := s.client.Allocations().Restart(&api.Allocation{ID: stub.ID}, "", q); err != nil {
			return nil, nomadErr(err)
		}
		restarted++
	}
	return row{"ok": true, "restarted": restarted, "skipped": skipped}, nil
}

func revertJob(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	jobID, err := requiredParam(rc, "job", "job")
	if err != nil {
		return nil, err
	}
	var req struct {
		Version uint64 `json:"version"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	ctx, cancel := commandContext(rc.Ctx, s)
	defer cancel()
	resp, _, err := s.client.Jobs().Revert(jobID, req.Version, nil, s.writeOptions(ctx, objectNamespace(rc, s)), "", "")
	if err != nil {
		return nil, nomadErr(err)
	}
	return actionResult{OK: true, EvalID: resp.EvalID, Message: resp.Warnings}, nil
}

func scaleJob(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	jobID, err := requiredParam(rc, "job", "job")
	if err != nil {
		return nil, err
	}
	var req struct {
		Group   string `json:"group" validate:"required"`
		Count   int    `json:"count"`
		Message string `json:"message"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	if req.Count < 0 {
		return nil, fmt.Errorf("%w: count cannot be negative", plugin.ErrInvalidInput)
	}
	message := strings.TrimSpace(req.Message)
	if message == "" {
		message = "scaled from ShellCN"
	}
	ctx, cancel := commandContext(rc.Ctx, s)
	defer cancel()
	resp, _, err := s.client.Jobs().Scale(jobID, req.Group, &req.Count, message, false, nil, s.writeOptions(ctx, objectNamespace(rc, s)))
	if err != nil {
		return nil, nomadErr(err)
	}
	return actionResult{OK: true, EvalID: resp.EvalID}, nil
}

func evaluateJob(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	jobID, err := requiredParam(rc, "job", "job")
	if err != nil {
		return nil, err
	}
	ctx, cancel := commandContext(rc.Ctx, s)
	defer cancel()
	evalID, _, err := s.client.Jobs().ForceEvaluate(jobID, s.writeOptions(ctx, objectNamespace(rc, s)))
	if err != nil {
		return nil, nomadErr(err)
	}
	return actionResult{OK: true, EvalID: evalID}, nil
}

func forcePeriodicJob(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	jobID, err := requiredParam(rc, "job", "job")
	if err != nil {
		return nil, err
	}
	ctx, cancel := commandContext(rc.Ctx, s)
	defer cancel()
	evalID, _, err := s.client.Jobs().PeriodicForce(jobID, s.writeOptions(ctx, objectNamespace(rc, s)))
	if err != nil {
		return nil, nomadErr(err)
	}
	return actionResult{OK: true, EvalID: evalID}, nil
}

// listAllocations serves every allocation grid. The parent the panel was opened
// from picks the upstream endpoint, so a job, node, deployment, or evaluation
// tab never enumerates the cluster and filters afterwards.
func listAllocations(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	q := s.listQuery(rc)
	group := param(rc, "group")
	switch {
	case param(rc, "deployment") != "":
		stubs, _, err := s.client.Deployments().Allocations(param(rc, "deployment"), s.objectQuery(rc))
		if err != nil {
			return nil, nomadErr(err)
		}
		return staticPage(rc, allocStubRows(stubs, group))
	case param(rc, "eval") != "":
		stubs, _, err := s.client.Evaluations().Allocations(param(rc, "eval"), s.objectQuery(rc))
		if err != nil {
			return nil, nomadErr(err)
		}
		return staticPage(rc, allocStubRows(stubs, group))
	case param(rc, "job") != "":
		stubs, _, err := s.client.Jobs().Allocations(param(rc, "job"), true, s.objectQuery(rc))
		if err != nil {
			return nil, nomadErr(err)
		}
		return staticPage(rc, allocStubRows(stubs, group))
	case param(rc, "node") != "":
		allocs, _, err := s.client.Nodes().Allocations(param(rc, "node"), q)
		if err != nil {
			return nil, nomadErr(err)
		}
		rows := make([]row, 0, len(allocs))
		for _, alloc := range allocs {
			if group != "" && alloc.TaskGroup != group {
				continue
			}
			rows = append(rows, allocRow(alloc.Stub()))
		}
		return staticPage(rc, rows)
	}
	return s.pageTokens(rc, q, func(q *api.QueryOptions) ([]row, string, error) {
		stubs, meta, err := s.client.Allocations().List(q)
		if err != nil {
			return nil, "", err
		}
		return allocStubRows(stubs, group), nextToken(meta), nil
	})
}

func allocStubRows(stubs []*api.AllocationListStub, group string) []row {
	rows := make([]row, 0, len(stubs))
	for _, stub := range stubs {
		if group != "" && stub.TaskGroup != group {
			continue
		}
		rows = append(rows, allocRow(stub))
	}
	return rows
}

func (s *Session) allocation(rc *plugin.RequestContext) (*api.Allocation, error) {
	allocID, err := requiredParam(rc, "alloc", "allocation")
	if err != nil {
		return nil, err
	}
	alloc, _, err := s.client.Allocations().Info(allocID, s.objectQuery(rc))
	if err != nil {
		return nil, nomadErr(err)
	}
	return alloc, nil
}

func allocOverview(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	alloc, err := s.allocation(rc)
	if err != nil {
		return nil, err
	}
	return allocDetail(alloc), nil
}

func allocEvents(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	alloc, err := s.allocation(rc)
	if err != nil {
		return nil, err
	}
	rows := []row{}
	for task, state := range alloc.TaskStates {
		for _, event := range state.Events {
			rows = append(rows, taskEventRow(alloc, task, event))
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		return fmt.Sprint(rows[i]["time"]) > fmt.Sprint(rows[j]["time"])
	})
	return staticPage(rc, rows)
}

func allocTasks(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	alloc, err := s.allocation(rc)
	if err != nil {
		return nil, err
	}
	names := taskNames(alloc)
	options := make([]plugin.Option, 0, len(names))
	for _, name := range names {
		options = append(options, plugin.Option{Label: name, Value: name})
	}
	return options, nil
}

func logTypes(rc *plugin.RequestContext) (any, error) {
	if _, err := nomadSession(rc); err != nil {
		return nil, err
	}
	return []plugin.Option{
		{Label: "stdout", Value: api.FSLogNameStdout},
		{Label: "stderr", Value: api.FSLogNameStderr},
	}, nil
}

func restartAllocation(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	allocID, err := requiredParam(rc, "alloc", "allocation")
	if err != nil {
		return nil, err
	}
	var req struct {
		Task string `json:"task"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	if err := s.client.Allocations().Restart(&api.Allocation{ID: allocID}, strings.TrimSpace(req.Task), s.objectQuery(rc)); err != nil {
		return nil, nomadErr(err)
	}
	return actionResult{OK: true}, nil
}

func signalAllocation(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	allocID, err := requiredParam(rc, "alloc", "allocation")
	if err != nil {
		return nil, err
	}
	var req struct {
		Signal string `json:"signal" validate:"required"`
		Task   string `json:"task"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	if err := s.client.Allocations().Signal(&api.Allocation{ID: allocID}, s.objectQuery(rc), strings.TrimSpace(req.Task), req.Signal); err != nil {
		return nil, nomadErr(err)
	}
	return actionResult{OK: true}, nil
}

func stopAllocation(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	allocID, err := requiredParam(rc, "alloc", "allocation")
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Allocations().Stop(&api.Allocation{ID: allocID}, s.objectQuery(rc))
	if err != nil {
		return nil, nomadErr(err)
	}
	return actionResult{OK: true, EvalID: resp.EvalID}, nil
}

func listNodes(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	// Nodes are cluster-scoped, so the listing carries no namespace.
	base := (&api.QueryOptions{Region: regionOf(rc, s), Params: map[string]string{"resources": "true"}}).WithContext(rc.Ctx)
	return s.pageTokens(rc, base, func(q *api.QueryOptions) ([]row, string, error) {
		stubs, meta, err := s.client.Nodes().List(q)
		if err != nil {
			return nil, "", err
		}
		rows := make([]row, 0, len(stubs))
		for _, stub := range stubs {
			rows = append(rows, nodeRow(stub))
		}
		return rows, nextToken(meta), nil
	})
}

func (s *Session) node(rc *plugin.RequestContext) (*api.Node, error) {
	nodeID, err := requiredParam(rc, "node", "node")
	if err != nil {
		return nil, err
	}
	node, _, err := s.client.Nodes().Info(nodeID, (&api.QueryOptions{Region: regionOf(rc, s)}).WithContext(rc.Ctx))
	if err != nil {
		return nil, nomadErr(err)
	}
	return node, nil
}

func nodeOverview(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	node, err := s.node(rc)
	if err != nil {
		return nil, err
	}
	out := nodeDetail(node)
	if stats, err := s.client.Nodes().Stats(node.ID, (&api.QueryOptions{Region: regionOf(rc, s)}).WithContext(rc.Ctx)); err == nil {
		applyHostStats(out, stats)
	}
	return out, nil
}

func drainNode(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	nodeID, err := requiredParam(rc, "node", "node")
	if err != nil {
		return nil, err
	}
	var req struct {
		Deadline         string `json:"deadline"`
		IgnoreSystemJobs bool   `json:"ignore_system_jobs"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	deadline, err := parseDeadline(req.Deadline)
	if err != nil {
		return nil, err
	}
	ctx, cancel := commandContext(rc.Ctx, s)
	defer cancel()
	resp, err := s.client.Nodes().UpdateDrainOpts(nodeID, &api.DrainOptions{
		DrainSpec: &api.DrainSpec{Deadline: deadline, IgnoreSystemJobs: req.IgnoreSystemJobs},
	}, s.writeOptions(ctx, ""))
	if err != nil {
		return nil, nomadErr(err)
	}
	return row{"ok": true, "evalIds": resp.EvalIDs, "nodeModifyIndex": resp.NodeModifyIndex}, nil
}

func cancelNodeDrain(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	nodeID, err := requiredParam(rc, "node", "node")
	if err != nil {
		return nil, err
	}
	ctx, cancel := commandContext(rc.Ctx, s)
	defer cancel()
	resp, err := s.client.Nodes().UpdateDrainOpts(nodeID, &api.DrainOptions{MarkEligible: true}, s.writeOptions(ctx, ""))
	if err != nil {
		return nil, nomadErr(err)
	}
	return row{"ok": true, "nodeModifyIndex": resp.NodeModifyIndex}, nil
}

func setNodeEligibility(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	nodeID, err := requiredParam(rc, "node", "node")
	if err != nil {
		return nil, err
	}
	var req struct {
		Eligible bool `json:"eligible"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	ctx, cancel := commandContext(rc.Ctx, s)
	defer cancel()
	resp, err := s.client.Nodes().ToggleEligibility(nodeID, req.Eligible, s.writeOptions(ctx, ""))
	if err != nil {
		return nil, nomadErr(err)
	}
	return row{"ok": true, "eligible": req.Eligible, "nodeModifyIndex": resp.NodeModifyIndex}, nil
}

func listDeployments(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	if jobID := param(rc, "job"); jobID != "" {
		deployments, _, err := s.client.Jobs().Deployments(jobID, true, s.objectQuery(rc))
		if err != nil {
			return nil, nomadErr(err)
		}
		return staticPage(rc, deploymentRows(deployments))
	}
	return s.pageTokens(rc, s.listQuery(rc), func(q *api.QueryOptions) ([]row, string, error) {
		deployments, meta, err := s.client.Deployments().List(q)
		if err != nil {
			return nil, "", err
		}
		return deploymentRows(deployments), nextToken(meta), nil
	})
}

func deploymentRows(deployments []*api.Deployment) []row {
	rows := make([]row, 0, len(deployments))
	for _, deployment := range deployments {
		rows = append(rows, deploymentRow(deployment))
	}
	return rows
}

func (s *Session) deployment(rc *plugin.RequestContext) (*api.Deployment, error) {
	id, err := requiredParam(rc, "deployment", "deployment")
	if err != nil {
		return nil, err
	}
	deployment, _, err := s.client.Deployments().Info(id, s.objectQuery(rc))
	if err != nil {
		return nil, nomadErr(err)
	}
	return deployment, nil
}

func deploymentOverview(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	deployment, err := s.deployment(rc)
	if err != nil {
		return nil, err
	}
	return deploymentDetail(deployment), nil
}

func promoteDeployment(rc *plugin.RequestContext) (any, error) {
	return deploymentAction(rc, func(s *Session, id string, w *api.WriteOptions) (*api.DeploymentUpdateResponse, error) {
		resp, _, err := s.client.Deployments().PromoteAll(id, w)
		return resp, err
	})
}

func failDeployment(rc *plugin.RequestContext) (any, error) {
	return deploymentAction(rc, func(s *Session, id string, w *api.WriteOptions) (*api.DeploymentUpdateResponse, error) {
		resp, _, err := s.client.Deployments().Fail(id, w)
		return resp, err
	})
}

func unblockDeployment(rc *plugin.RequestContext) (any, error) {
	return deploymentAction(rc, func(s *Session, id string, w *api.WriteOptions) (*api.DeploymentUpdateResponse, error) {
		resp, _, err := s.client.Deployments().Unblock(id, w)
		return resp, err
	})
}

func pauseDeployment(rc *plugin.RequestContext) (any, error) {
	var req struct {
		Pause bool `json:"pause"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	return deploymentAction(rc, func(s *Session, id string, w *api.WriteOptions) (*api.DeploymentUpdateResponse, error) {
		resp, _, err := s.client.Deployments().Pause(id, req.Pause, w)
		return resp, err
	})
}

func deploymentAction(rc *plugin.RequestContext, run func(*Session, string, *api.WriteOptions) (*api.DeploymentUpdateResponse, error)) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	id, err := requiredParam(rc, "deployment", "deployment")
	if err != nil {
		return nil, err
	}
	ctx, cancel := commandContext(rc.Ctx, s)
	defer cancel()
	resp, err := run(s, id, s.writeOptions(ctx, objectNamespace(rc, s)))
	if err != nil {
		return nil, nomadErr(err)
	}
	return actionResult{OK: true, EvalID: resp.EvalID}, nil
}

func listEvaluations(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	if jobID := param(rc, "job"); jobID != "" {
		evals, _, err := s.client.Jobs().Evaluations(jobID, s.objectQuery(rc))
		if err != nil {
			return nil, nomadErr(err)
		}
		return staticPage(rc, evaluationRows(evals))
	}
	return s.pageTokens(rc, s.listQuery(rc), func(q *api.QueryOptions) ([]row, string, error) {
		evals, meta, err := s.client.Evaluations().List(q)
		if err != nil {
			return nil, "", err
		}
		return evaluationRows(evals), nextToken(meta), nil
	})
}

func evaluationRows(evals []*api.Evaluation) []row {
	rows := make([]row, 0, len(evals))
	for _, eval := range evals {
		rows = append(rows, evaluationRow(eval))
	}
	return rows
}

func evaluationOverview(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	id, err := requiredParam(rc, "eval", "evaluation")
	if err != nil {
		return nil, err
	}
	eval, _, err := s.client.Evaluations().Info(id, s.objectQuery(rc))
	if err != nil {
		return nil, nomadErr(err)
	}
	return evaluationDetail(eval), nil
}

func listVolumes(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	return s.pageTokens(rc, s.listQuery(rc), func(q *api.QueryOptions) ([]row, string, error) {
		volumes, meta, err := s.client.CSIVolumes().List(q)
		if err != nil {
			return nil, "", err
		}
		rows := make([]row, 0, len(volumes))
		for _, volume := range volumes {
			rows = append(rows, csiVolumeRow(volume))
		}
		return rows, nextToken(meta), nil
	})
}

func volumeOverview(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	id, err := requiredParam(rc, "volume", "volume")
	if err != nil {
		return nil, err
	}
	volume, _, err := s.client.CSIVolumes().Info(id, s.objectQuery(rc))
	if err != nil {
		return nil, nomadErr(err)
	}
	return csiVolumeDetail(volume), nil
}

func listHostVolumes(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	return s.pageTokens(rc, s.listQuery(rc), func(q *api.QueryOptions) ([]row, string, error) {
		volumes, meta, err := s.client.HostVolumes().List(&api.HostVolumeListRequest{NodeID: param(rc, "node")}, q)
		if err != nil {
			return nil, "", err
		}
		rows := make([]row, 0, len(volumes))
		for _, volume := range volumes {
			rows = append(rows, hostVolumeRow(volume))
		}
		return rows, nextToken(meta), nil
	})
}

func hostVolumeOverview(rc *plugin.RequestContext) (any, error) {
	s, err := nomadSession(rc)
	if err != nil {
		return nil, err
	}
	id, err := requiredParam(rc, "volume", "volume")
	if err != nil {
		return nil, err
	}
	volume, _, err := s.client.HostVolumes().Get(id, s.objectQuery(rc))
	if err != nil {
		return nil, nomadErr(err)
	}
	return hostVolumeDetail(volume), nil
}

func nextToken(meta *api.QueryMeta) string {
	if meta == nil {
		return ""
	}
	return meta.NextToken
}

func parseDeadline(raw string) (time.Duration, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed == "0" {
		return 0, nil
	}
	deadline, err := time.ParseDuration(trimmed)
	if err != nil {
		return 0, fmt.Errorf("%w: deadline must be a duration (e.g. 1h)", plugin.ErrInvalidInput)
	}
	if deadline < 0 {
		return 0, fmt.Errorf("%w: deadline cannot be negative", plugin.ErrInvalidInput)
	}
	return deadline, nil
}

func agentStat(self *api.AgentSelf, section, key string) string {
	if self == nil {
		return ""
	}
	if values, ok := self.Stats[section]; ok {
		return values[key]
	}
	return ""
}

func deref[T any](v *T) T {
	if v == nil {
		var zero T
		return zero
	}
	return *v
}
