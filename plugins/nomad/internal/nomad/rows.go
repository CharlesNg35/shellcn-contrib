package nomad

import (
	"fmt"
	"net"
	"sort"
	"time"

	"github.com/hashicorp/nomad/api"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

func jobIdentity(namespace, id string) plugin.ResourceIdentity {
	return plugin.ResourceIdentity{Kind: "job", Namespace: namespace, Name: id, UID: namespace + "/" + id}
}

func jobRow(stub *api.JobListStub) row {
	namespace := namespaceOrDefault(stub.Namespace, defaultNamespace)
	out := row{
		"id":            stub.ID,
		"name":          stub.Name,
		"namespace":     namespace,
		"type":          stub.Type,
		"status":        stub.Status,
		"priority":      stub.Priority,
		"datacenters":   stub.Datacenters,
		"periodic":      stub.Periodic,
		"parameterized": stub.ParameterizedJob,
		"stopped":       stub.Stop,
		"submitTime":    unixNano(stub.SubmitTime),
		"modifyIndex":   stub.ModifyIndex,
		"ref":           jobIdentity(namespace, stub.ID),
	}
	applySummary(out, stub.JobSummary)
	return out
}

// jobEventRow shapes a job change feed payload like the list row it patches; the
// event carries no summary, so the caller fills the allocation counts in.
func jobEventRow(job *api.Job) row {
	namespace := namespaceOrDefault(deref(job.Namespace), defaultNamespace)
	return row{
		"id":            deref(job.ID),
		"name":          deref(job.Name),
		"namespace":     namespace,
		"type":          deref(job.Type),
		"status":        deref(job.Status),
		"priority":      deref(job.Priority),
		"datacenters":   job.Datacenters,
		"periodic":      job.IsPeriodic(),
		"parameterized": job.IsParameterized(),
		"stopped":       deref(job.Stop),
		"submitTime":    unixNano(deref(job.SubmitTime)),
		"modifyIndex":   deref(job.ModifyIndex),
		"ref":           jobIdentity(namespace, deref(job.ID)),
	}
}

func nodeEventRow(node *api.Node) row {
	cpu, memory, disk := nodeCapacity(node.NodeResources)
	return row{
		"id":          node.ID,
		"shortId":     shortID(node.ID),
		"name":        node.Name,
		"datacenter":  node.Datacenter,
		"nodeClass":   node.NodeClass,
		"nodePool":    node.NodePool,
		"status":      node.Status,
		"eligibility": node.SchedulingEligibility,
		"drain":       node.Drain,
		"version":     node.Attributes["nomad.version"],
		"address":     node.HTTPAddr,
		"drivers":     healthyDrivers(node.Drivers),
		"cpu":         cpu,
		"memory":      memory,
		"disk":        disk,
		"modifyIndex": node.ModifyIndex,
		"ref":         plugin.ResourceIdentity{Kind: "node", Name: node.Name, UID: node.ID},
	}
}

func jobDetail(job *api.Job) row {
	namespace := namespaceOrDefault(deref(job.Namespace), defaultNamespace)
	groups := make([]string, 0, len(job.TaskGroups))
	tasks := 0
	for _, group := range job.TaskGroups {
		groups = append(groups, deref(group.Name))
		tasks += len(group.Tasks)
	}
	return row{
		"id":                deref(job.ID),
		"name":              deref(job.Name),
		"namespace":         namespace,
		"type":              deref(job.Type),
		"status":            deref(job.Status),
		"statusDescription": deref(job.StatusDescription),
		"priority":          deref(job.Priority),
		"region":            deref(job.Region),
		"nodePool":          deref(job.NodePool),
		"datacenters":       job.Datacenters,
		"version":           deref(job.Version),
		"stable":            deref(job.Stable),
		"stopped":           deref(job.Stop),
		"periodic":          job.IsPeriodic(),
		"parameterized":     job.IsParameterized(),
		"multiregion":       job.IsMultiregion(),
		"taskGroups":        groups,
		"groupCount":        len(job.TaskGroups),
		"taskCount":         tasks,
		"meta":              job.Meta,
		"submitTime":        unixNano(deref(job.SubmitTime)),
		"modifyIndex":       deref(job.ModifyIndex),
		"jobModifyIndex":    deref(job.JobModifyIndex),
		"ref":               jobIdentity(namespace, deref(job.ID)),
	}
}

func applySummary(out row, summary *api.JobSummary) {
	running, queued, failed, complete, lost, starting := 0, 0, 0, 0, 0, 0
	if summary != nil {
		for _, group := range summary.Summary {
			running += group.Running
			queued += group.Queued
			failed += group.Failed
			complete += group.Complete
			lost += group.Lost
			starting += group.Starting
		}
	}
	out["running"] = running
	out["queued"] = queued
	out["failed"] = failed
	out["complete"] = complete
	out["lost"] = lost
	out["starting"] = starting
}

func taskGroupRow(job *api.Job, group *api.TaskGroup) row {
	tasks := make([]string, 0, len(group.Tasks))
	drivers := map[string]bool{}
	cpu, memory := 0, 0
	for _, task := range group.Tasks {
		tasks = append(tasks, task.Name)
		drivers[task.Driver] = true
		if task.Resources != nil {
			cpu += deref(task.Resources.CPU)
			memory += deref(task.Resources.MemoryMB)
		}
	}
	driverNames := make([]string, 0, len(drivers))
	for driver := range drivers {
		if driver != "" {
			driverNames = append(driverNames, driver)
		}
	}
	sort.Strings(driverNames)
	disk := 0
	if group.EphemeralDisk != nil {
		disk = deref(group.EphemeralDisk.SizeMB)
	}
	return row{
		"name":     deref(group.Name),
		"job":      deref(job.ID),
		"count":    deref(group.Count),
		"tasks":    tasks,
		"drivers":  driverNames,
		"cpu":      cpu,
		"memory":   memory,
		"disk":     disk,
		"services": len(group.Services),
		"volumes":  len(group.Volumes),
	}
}

func jobVersionRow(job *api.Job, diff *api.JobDiff) row {
	changed := 0
	if diff != nil {
		changed = len(diff.Fields) + len(diff.Objects) + len(diff.TaskGroups)
	}
	tag := ""
	if job.VersionTag != nil {
		tag = job.VersionTag.Name
	}
	return row{
		"version":     deref(job.Version),
		"stable":      deref(job.Stable),
		"status":      deref(job.Status),
		"tag":         tag,
		"submitTime":  unixNano(deref(job.SubmitTime)),
		"modifyIndex": deref(job.ModifyIndex),
		"changes":     changed,
	}
}

func allocRow(stub *api.AllocationListStub) row {
	namespace := namespaceOrDefault(stub.Namespace, defaultNamespace)
	cpu, memory := allocatedResources(stub.AllocatedResources)
	return row{
		"id":                stub.ID,
		"shortId":           shortID(stub.ID),
		"name":              stub.Name,
		"namespace":         namespace,
		"jobId":             stub.JobID,
		"jobType":           stub.JobType,
		"jobVersion":        stub.JobVersion,
		"taskGroup":         stub.TaskGroup,
		"nodeId":            stub.NodeID,
		"nodeName":          stub.NodeName,
		"clientStatus":      stub.ClientStatus,
		"desiredStatus":     stub.DesiredStatus,
		"clientDescription": stub.ClientDescription,
		"cpu":               cpu,
		"memory":            memory,
		"createTime":        unixNano(stub.CreateTime),
		"modifyTime":        unixNano(stub.ModifyTime),
		"ref":               plugin.ResourceIdentity{Kind: "allocation", Namespace: namespace, Name: stub.Name, UID: stub.ID},
	}
}

func allocDetail(alloc *api.Allocation) row {
	namespace := namespaceOrDefault(alloc.Namespace, defaultNamespace)
	cpu, memory := allocatedResources(alloc.AllocatedResources)
	disk := int64(0)
	if alloc.AllocatedResources != nil {
		disk = alloc.AllocatedResources.Shared.DiskMB
	}
	out := row{
		"id":                 alloc.ID,
		"shortId":            shortID(alloc.ID),
		"name":               alloc.Name,
		"namespace":          namespace,
		"jobId":              alloc.JobID,
		"taskGroup":          alloc.TaskGroup,
		"nodeId":             alloc.NodeID,
		"nodeName":           alloc.NodeName,
		"evalId":             alloc.EvalID,
		"deploymentId":       alloc.DeploymentID,
		"clientStatus":       alloc.ClientStatus,
		"clientDescription":  alloc.ClientDescription,
		"desiredStatus":      alloc.DesiredStatus,
		"desiredDescription": alloc.DesiredDescription,
		"previousAllocation": alloc.PreviousAllocation,
		"nextAllocation":     alloc.NextAllocation,
		"tasks":              taskNames(alloc),
		"cpu":                cpu,
		"memory":             memory,
		"disk":               disk,
		"createTime":         unixNano(alloc.CreateTime),
		"modifyTime":         unixNano(alloc.ModifyTime),
		"ref":                plugin.ResourceIdentity{Kind: "allocation", Namespace: namespace, Name: alloc.Name, UID: alloc.ID},
	}
	if alloc.DeploymentStatus != nil {
		out["deploymentHealthy"] = deref(alloc.DeploymentStatus.Healthy)
		out["canary"] = alloc.DeploymentStatus.Canary
	}
	if alloc.RescheduleTracker != nil {
		out["rescheduleAttempts"] = len(alloc.RescheduleTracker.Events)
	}
	if alloc.NetworkStatus != nil {
		out["address"] = alloc.NetworkStatus.Address
	}
	failed := 0
	restarts := uint64(0)
	for _, state := range alloc.TaskStates {
		if state.Failed {
			failed++
		}
		restarts += state.Restarts
	}
	out["failedTasks"] = failed
	out["restarts"] = restarts
	return out
}

func taskNames(alloc *api.Allocation) []string {
	names := make([]string, 0, len(alloc.TaskStates))
	for name := range alloc.TaskStates {
		names = append(names, name)
	}
	if len(names) == 0 && alloc.Job != nil {
		for _, group := range alloc.Job.TaskGroups {
			if deref(group.Name) != alloc.TaskGroup {
				continue
			}
			for _, task := range group.Tasks {
				names = append(names, task.Name)
			}
		}
	}
	sort.Strings(names)
	return names
}

func taskEventRow(alloc *api.Allocation, task string, event *api.TaskEvent) row {
	message := event.DisplayMessage
	if message == "" {
		message = event.Message
	}
	return row{
		"time":     time.Unix(0, event.Time),
		"task":     task,
		"reason":   event.Type,
		"message":  message,
		"severity": eventSeverity(event),
		"icon":     eventIcon(event),
		"resource": alloc.Name + " / " + task,
	}
}

func eventSeverity(event *api.TaskEvent) string {
	switch event.Type {
	case api.TaskSetupFailure, api.TaskDriverFailure, api.TaskFailedValidation, api.TaskArtifactDownloadFailed, api.TaskSiblingFailed, api.TaskNotRestarting:
		return string(plugin.SeverityDanger)
	case api.TaskRestarting, api.TaskKilling, api.TaskTerminated, api.TaskSignaling:
		return string(plugin.SeverityWarn)
	case api.TaskStarted:
		return string(plugin.SeveritySuccess)
	default:
		return string(plugin.SeverityInfo)
	}
}

func eventIcon(event *api.TaskEvent) string {
	switch event.Type {
	case api.TaskStarted:
		return "play"
	case api.TaskTerminated, api.TaskKilled:
		return "square"
	case api.TaskRestarting:
		return "rotate-ccw"
	default:
		return "circle-dot"
	}
}

func nodeRow(stub *api.NodeListStub) row {
	cpu, memory, disk := nodeCapacity(stub.NodeResources)
	return row{
		"id":          stub.ID,
		"shortId":     shortID(stub.ID),
		"name":        stub.Name,
		"datacenter":  stub.Datacenter,
		"nodeClass":   stub.NodeClass,
		"nodePool":    stub.NodePool,
		"status":      stub.Status,
		"eligibility": stub.SchedulingEligibility,
		"drain":       stub.Drain,
		"version":     stub.Version,
		"address":     stub.Address,
		"drivers":     healthyDrivers(stub.Drivers),
		"cpu":         cpu,
		"memory":      memory,
		"disk":        disk,
		"modifyIndex": stub.ModifyIndex,
		"ref":         plugin.ResourceIdentity{Kind: "node", Name: stub.Name, UID: stub.ID},
	}
}

func nodeDetail(node *api.Node) row {
	cpu, memory, disk := nodeCapacity(node.NodeResources)
	out := row{
		"id":                node.ID,
		"shortId":           shortID(node.ID),
		"name":              node.Name,
		"datacenter":        node.Datacenter,
		"nodeClass":         node.NodeClass,
		"nodePool":          node.NodePool,
		"status":            node.Status,
		"statusDescription": node.StatusDescription,
		"eligibility":       node.SchedulingEligibility,
		"drain":             node.Drain,
		"address":           node.HTTPAddr,
		"tls":               node.TLSEnabled,
		"version":           node.Attributes["nomad.version"],
		"os":                node.Attributes["os.name"],
		"kernel":            node.Attributes["kernel.name"],
		"arch":              node.Attributes["cpu.arch"],
		"drivers":           healthyDrivers(node.Drivers),
		"hostVolumes":       sortedKeys(node.HostVolumes),
		"cpu":               cpu,
		"memory":            memory,
		"disk":              disk,
		"maxAllocs":         node.NodeMaxAllocs,
		"meta":              node.Meta,
		"modifyIndex":       node.ModifyIndex,
		"ref":               plugin.ResourceIdentity{Kind: "node", Name: node.Name, UID: node.ID},
	}
	if node.ReservedResources != nil {
		out["reservedCpu"] = node.ReservedResources.Cpu.CpuShares
		out["reservedMemory"] = node.ReservedResources.Memory.MemoryMB
	}
	if node.DrainStrategy != nil {
		out["drainDeadline"] = node.DrainStrategy.ForceDeadline
		out["drainIgnoresSystemJobs"] = node.DrainStrategy.IgnoreSystemJobs
	}
	if node.LastDrain != nil {
		out["lastDrainStatus"] = string(node.LastDrain.Status)
		out["lastDrainAt"] = node.LastDrain.UpdatedAt
	}
	return out
}

func applyHostStats(out row, stats *api.HostStats) {
	if stats == nil {
		return
	}
	out["uptime"] = stats.Uptime
	if stats.Memory != nil && stats.Memory.Total > 0 {
		out["memoryUsed"] = stats.Memory.Used
		out["memoryTotal"] = stats.Memory.Total
		out["memoryPercent"] = percent(float64(stats.Memory.Used), float64(stats.Memory.Total))
	}
	if idle, ok := averageIdle(stats.CPU); ok {
		out["cpuPercent"] = round2(100 - idle)
	}
	if stats.AllocDirStats != nil && stats.AllocDirStats.Size > 0 {
		out["diskUsed"] = stats.AllocDirStats.Used
		out["diskTotal"] = stats.AllocDirStats.Size
		out["diskPercent"] = round2(stats.AllocDirStats.UsedPercent)
	}
}

func averageIdle(cpus []*api.HostCPUStats) (float64, bool) {
	if len(cpus) == 0 {
		return 0, false
	}
	total := 0.0
	for _, cpu := range cpus {
		total += cpu.Idle
	}
	return total / float64(len(cpus)), true
}

func memberRow(member *api.AgentMember, leaders map[string]string) row {
	region := member.Tags["region"]
	rpcAddr := ""
	if port := member.Tags["port"]; port != "" {
		rpcAddr = net.JoinHostPort(member.Addr, port)
	}
	return row{
		"name":       member.Name,
		"address":    member.Addr,
		"port":       member.Port,
		"status":     member.Status,
		"region":     region,
		"datacenter": member.Tags["dc"],
		"build":      member.Tags["build"],
		"raftVsn":    member.Tags["raft_vsn"],
		"leader":     rpcAddr != "" && leaders[region] == rpcAddr,
	}
}

// datacenterRows rolls a client listing up per datacenter. Nomad has no
// datacenter endpoint: a datacenter exists exactly as long as a client declares
// it, so the client set is the authoritative source.
func datacenterRows(nodes []*api.NodeListStub) []row {
	type bucket struct {
		clients, ready, eligible, draining int
		cpu, memory                        int64
		pools                              map[string]bool
	}
	buckets := map[string]*bucket{}
	for _, node := range nodes {
		name := node.Datacenter
		if name == "" {
			continue
		}
		b := buckets[name]
		if b == nil {
			b = &bucket{pools: map[string]bool{}}
			buckets[name] = b
		}
		cpu, memory, _ := nodeCapacity(node.NodeResources)
		b.clients++
		b.cpu += cpu
		b.memory += memory
		if node.Status == api.NodeStatusReady {
			b.ready++
		}
		if node.SchedulingEligibility == api.NodeSchedulingEligible {
			b.eligible++
		}
		if node.Drain {
			b.draining++
		}
		if node.NodePool != "" {
			b.pools[node.NodePool] = true
		}
	}
	rows := make([]row, 0, len(buckets))
	for name, b := range buckets {
		rows = append(rows, row{
			"name":      name,
			"clients":   b.clients,
			"ready":     b.ready,
			"eligible":  b.eligible,
			"draining":  b.draining,
			"nodePools": sortedKeys(b.pools),
			"cpu":       b.cpu,
			"memory":    b.memory,
		})
	}
	sortRowsByName(rows, "name")
	return rows
}

func namespaceRow(ns *api.Namespace) row {
	return row{
		"name":        ns.Name,
		"value":       ns.Name,
		"label":       ns.Name,
		"description": ns.Description,
		"quota":       ns.Quota,
		"modifyIndex": ns.ModifyIndex,
	}
}

func deploymentRow(deployment *api.Deployment) row {
	namespace := namespaceOrDefault(deployment.Namespace, defaultNamespace)
	desired, placed, healthy, unhealthy, canaries := deploymentTotals(deployment)
	return row{
		"id":                deployment.ID,
		"shortId":           shortID(deployment.ID),
		"namespace":         namespace,
		"jobId":             deployment.JobID,
		"jobVersion":        deployment.JobVersion,
		"status":            deployment.Status,
		"statusDescription": deployment.StatusDescription,
		"desired":           desired,
		"placed":            placed,
		"healthy":           healthy,
		"unhealthy":         unhealthy,
		"canaries":          canaries,
		"progress":          percent(float64(healthy), float64(desired)),
		"createTime":        unixNano(deployment.CreateTime),
		"modifyTime":        unixNano(deployment.ModifyTime),
		"ref":               plugin.ResourceIdentity{Kind: "deployment", Namespace: namespace, Name: shortID(deployment.ID), UID: deployment.ID},
	}
}

func deploymentDetail(deployment *api.Deployment) row {
	out := deploymentRow(deployment)
	groups := make([]row, 0, len(deployment.TaskGroups))
	for name, state := range deployment.TaskGroups {
		groups = append(groups, row{
			"name":            name,
			"desiredTotal":    state.DesiredTotal,
			"desiredCanaries": state.DesiredCanaries,
			"placed":          state.PlacedAllocs,
			"healthy":         state.HealthyAllocs,
			"unhealthy":       state.UnhealthyAllocs,
			"promoted":        state.Promoted,
			"autoRevert":      state.AutoRevert,
		})
	}
	sortRowsByName(groups, "name")
	out["groups"] = groups
	out["multiregion"] = deployment.IsMultiregion
	out["jobModifyIndex"] = deployment.JobModifyIndex
	return out
}

func deploymentTotals(deployment *api.Deployment) (desired, placed, healthy, unhealthy, canaries int) {
	for _, state := range deployment.TaskGroups {
		desired += state.DesiredTotal
		placed += state.PlacedAllocs
		healthy += state.HealthyAllocs
		unhealthy += state.UnhealthyAllocs
		canaries += len(state.PlacedCanaries)
	}
	return desired, placed, healthy, unhealthy, canaries
}

func evaluationRow(eval *api.Evaluation) row {
	namespace := namespaceOrDefault(eval.Namespace, defaultNamespace)
	return row{
		"id":                eval.ID,
		"shortId":           shortID(eval.ID),
		"namespace":         namespace,
		"jobId":             eval.JobID,
		"nodeId":            eval.NodeID,
		"deploymentId":      eval.DeploymentID,
		"priority":          eval.Priority,
		"type":              eval.Type,
		"triggeredBy":       eval.TriggeredBy,
		"status":            eval.Status,
		"statusDescription": eval.StatusDescription,
		"blockedEval":       eval.BlockedEval,
		"failedGroups":      len(eval.FailedTGAllocs),
		"createTime":        unixNano(eval.CreateTime),
		"modifyTime":        unixNano(eval.ModifyTime),
		"ref":               plugin.ResourceIdentity{Kind: "evaluation", Namespace: namespace, Name: shortID(eval.ID), UID: eval.ID},
	}
}

func evaluationDetail(eval *api.Evaluation) row {
	out := evaluationRow(eval)
	out["nextEval"] = eval.NextEval
	out["previousEval"] = eval.PreviousEval
	out["waitUntil"] = eval.WaitUntil
	out["quotaLimitReached"] = eval.QuotaLimitReached
	out["snapshotIndex"] = eval.SnapshotIndex
	queued := 0
	for _, count := range eval.QueuedAllocations {
		queued += count
	}
	out["queuedAllocations"] = queued
	blocked := make([]row, 0, len(eval.FailedTGAllocs))
	for name, metric := range eval.FailedTGAllocs {
		if metric == nil {
			continue
		}
		blocked = append(blocked, row{
			"group":     name,
			"evaluated": metric.NodesEvaluated,
			"filtered":  metric.NodesFiltered,
			"exhausted": metric.NodesExhausted,
		})
	}
	sortRowsByName(blocked, "group")
	out["placementFailures"] = blocked
	return out
}

func csiVolumeRow(stub *api.CSIVolumeListStub) row {
	namespace := namespaceOrDefault(stub.Namespace, defaultNamespace)
	return row{
		"id":             stub.ID,
		"name":           stub.Name,
		"namespace":      namespace,
		"externalId":     stub.ExternalID,
		"pluginId":       stub.PluginID,
		"provider":       stub.Provider,
		"accessMode":     string(stub.AccessMode),
		"attachmentMode": string(stub.AttachmentMode),
		"schedulable":    stub.Schedulable,
		"readers":        stub.CurrentReaders,
		"writers":        stub.CurrentWriters,
		"controllers":    stub.ControllersHealthy,
		"nodes":          stub.NodesHealthy,
		"createTime":     unixNano(stub.CreateTime),
		"ref":            plugin.ResourceIdentity{Kind: "volume", Namespace: namespace, Name: stub.ID, UID: stub.ID},
	}
}

func csiVolumeDetail(volume *api.CSIVolume) row {
	namespace := namespaceOrDefault(volume.Namespace, defaultNamespace)
	return row{
		"id":                  volume.ID,
		"name":                volume.Name,
		"namespace":           namespace,
		"externalId":          volume.ExternalID,
		"pluginId":            volume.PluginID,
		"provider":            volume.Provider,
		"providerVersion":     volume.ProviderVersion,
		"accessMode":          string(volume.AccessMode),
		"attachmentMode":      string(volume.AttachmentMode),
		"schedulable":         volume.Schedulable,
		"capacity":            volume.Capacity,
		"controllerRequired":  volume.ControllerRequired,
		"controllersHealthy":  volume.ControllersHealthy,
		"controllersExpected": volume.ControllersExpected,
		"nodesHealthy":        volume.NodesHealthy,
		"nodesExpected":       volume.NodesExpected,
		"readers":             len(volume.ReadAllocs),
		"writers":             len(volume.WriteAllocs),
		"createTime":          unixNano(volume.CreateTime),
		"modifyTime":          unixNano(volume.ModifyTime),
		"ref":                 plugin.ResourceIdentity{Kind: "volume", Namespace: namespace, Name: volume.ID, UID: volume.ID},
	}
}

func hostVolumeRow(stub *api.HostVolumeStub) row {
	namespace := namespaceOrDefault(stub.Namespace, defaultNamespace)
	return row{
		"id":         stub.ID,
		"name":       stub.Name,
		"namespace":  namespace,
		"pluginId":   stub.PluginID,
		"nodeId":     stub.NodeID,
		"nodePool":   stub.NodePool,
		"capacity":   stub.CapacityBytes,
		"state":      string(stub.State),
		"createTime": unixNano(stub.CreateTime),
		"ref":        plugin.ResourceIdentity{Kind: "host_volume", Namespace: namespace, Name: stub.Name, UID: stub.ID},
	}
}

func hostVolumeDetail(volume *api.HostVolume) row {
	namespace := namespaceOrDefault(volume.Namespace, defaultNamespace)
	return row{
		"id":          volume.ID,
		"name":        volume.Name,
		"namespace":   namespace,
		"pluginId":    volume.PluginID,
		"nodeId":      volume.NodeID,
		"nodePool":    volume.NodePool,
		"hostPath":    volume.HostPath,
		"capacity":    volume.CapacityBytes,
		"state":       string(volume.State),
		"parameters":  volume.Parameters,
		"allocations": len(volume.Allocations),
		"createTime":  unixNano(volume.CreateTime),
		"modifyTime":  unixNano(volume.ModifyTime),
		"ref":         plugin.ResourceIdentity{Kind: "host_volume", Namespace: namespace, Name: volume.Name, UID: volume.ID},
	}
}

func planResult(job *api.Job, resp *api.JobPlanResponse) row {
	groups := []row{}
	if resp.Diff != nil {
		for _, group := range resp.Diff.TaskGroups {
			groups = append(groups, row{"name": group.Name, "type": group.Type, "changes": len(group.Fields) + len(group.Objects) + len(group.Tasks)})
		}
	}
	failures := make([]row, 0, len(resp.FailedTGAllocs))
	for name, metric := range resp.FailedTGAllocs {
		if metric == nil {
			continue
		}
		failures = append(failures, row{"group": name, "evaluated": metric.NodesEvaluated, "filtered": metric.NodesFiltered, "exhausted": metric.NodesExhausted})
	}
	sortRowsByName(failures, "group")
	out := row{
		"job":            deref(job.ID),
		"namespace":      namespaceOrDefault(deref(job.Namespace), defaultNamespace),
		"jobModifyIndex": resp.JobModifyIndex,
		"warnings":       resp.Warnings,
		"createdEvals":   len(resp.CreatedEvals),
		"groups":         groups,
		"failures":       failures,
	}
	if resp.Diff != nil {
		out["changeType"] = resp.Diff.Type
	}
	if resp.Annotations != nil {
		out["desiredUpdates"] = resp.Annotations.DesiredTGUpdates
	}
	if !resp.NextPeriodicLaunch.IsZero() {
		out["nextPeriodicLaunch"] = resp.NextPeriodicLaunch
	}
	return out
}

func allocatedResources(resources *api.AllocatedResources) (cpu, memory int64) {
	if resources == nil {
		return 0, 0
	}
	for _, task := range resources.Tasks {
		cpu += task.Cpu.CpuShares
		memory += task.Memory.MemoryMB
	}
	return cpu, memory
}

func nodeCapacity(resources *api.NodeResources) (cpu int64, memory int64, disk int64) {
	if resources == nil {
		return 0, 0, 0
	}
	return resources.Cpu.CpuShares, resources.Memory.MemoryMB, resources.Disk.DiskMB
}

func healthyDrivers(drivers map[string]*api.DriverInfo) []string {
	out := make([]string, 0, len(drivers))
	for name, info := range drivers {
		if info != nil && info.Detected && info.Healthy {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

func sortedKeys[V any](in map[string]V) []string {
	out := make([]string, 0, len(in))
	for key := range in {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func unixNano(value int64) any {
	if value <= 0 {
		return nil
	}
	return time.Unix(0, value)
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

func percent(used, total float64) float64 {
	if total <= 0 {
		return 0
	}
	return round2(used / total * 100)
}

func round2(value float64) float64 {
	return float64(int64(value*100+0.5)) / 100
}

func sortRowsByName(rows []row, key string) {
	sort.Slice(rows, func(i, j int) bool { return fmt.Sprint(rows[i][key]) < fmt.Sprint(rows[j][key]) })
}
