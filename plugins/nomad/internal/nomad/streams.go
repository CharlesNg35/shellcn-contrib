package nomad

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/hashicorp/nomad/api"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

func streamAllocLogs(rc *plugin.RequestContext, client plugin.ClientStream) error {
	ch, err := rc.Session.OpenChannel(rc.Ctx, plugin.ChannelRequest{Kind: plugin.StreamLogs, Params: streamParams(rc)})
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()

	output := make(chan error, 1)
	go func() {
		_, copyErr := io.Copy(client, ch)
		output <- copyErr
	}()
	select {
	case <-client.Context().Done():
		_ = ch.Close()
		<-output
		return nil
	case err := <-output:
		return endOfStream(err)
	}
}

func streamAllocExec(rc *plugin.RequestContext, client plugin.ClientStream) error {
	auditParams := map[string]string{"alloc": param(rc, "alloc"), "task": param(rc, "task")}
	ch, err := rc.Session.OpenChannel(rc.Ctx, plugin.ChannelRequest{Kind: plugin.StreamTerminal, Params: streamParams(rc)})
	if err != nil {
		rc.Audit(plugin.AuditDenied, auditParams, err)
		return err
	}
	defer func() { _ = ch.Close() }()
	rc.Audit(plugin.AuditAllowed, auditParams, nil)

	output := make(chan error, 1)
	go func() {
		_, copyErr := io.Copy(client, ch)
		output <- copyErr
	}()
	input := make(chan error, 1)
	go func() { input <- plugin.CopyTerminalInput(ch, client) }()

	select {
	case <-client.Context().Done():
		_ = ch.Close()
		<-output
		return nil
	case err := <-output:
		return endOfStream(err)
	case err := <-input:
		// The browser stopped sending. Close the task side and let the output
		// copy finish so already-produced bytes still reach the terminal.
		_ = ch.Close()
		<-output
		return endOfStream(err)
	}
}

func endOfStream(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

// streamParams collects the values a stream source carries, which arrive as p.*
// query args rather than resolved path params.
func streamParams(rc *plugin.RequestContext) map[string]string {
	out := rc.Params()
	if out == nil {
		out = map[string]string{}
	}
	for _, name := range []string{"ns", "alloc", "task", "type", "tail", "command", "cols", "rows"} {
		if value := param(rc, name); value != "" {
			out[name] = value
		}
	}
	return out
}

func streamAllocMetrics(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := nomadSession(rc)
	if err != nil {
		return err
	}
	alloc, err := s.allocation(rc)
	if err != nil {
		return err
	}
	cpuTotal, memoryTotal := allocatedResources(alloc.AllocatedResources)
	q := s.objectQuery(rc)
	return tickStream(rc, client, s.opts.MetricTick, func() (any, bool) {
		frame := row{
			"cpuTotal":    cpuTotal,
			"memoryTotal": memoryTotal << 20,
			"tasks":       len(alloc.TaskStates),
		}
		stats, err := s.client.Allocations().Stats(alloc, q)
		if err != nil || stats == nil || stats.ResourceUsage == nil {
			return frame, true
		}
		if cpu := stats.ResourceUsage.CpuStats; cpu != nil {
			frame["cpuTicks"] = round2(cpu.TotalTicks)
			frame["cpuUsed"] = round2(cpu.TotalTicks)
			// The panel renders cpuPercent as cpuUsed of cpuTotal, so the gauge has
			// to be ticks against the allocation's reservation. CpuStats.Percent is
			// a share of the whole host and would contradict the numbers beside it.
			if cpuTotal > 0 {
				frame["cpuPercent"] = percent(cpu.TotalTicks, float64(cpuTotal))
			} else {
				frame["cpuPercent"] = round2(cpu.Percent)
			}
		}
		if memory := stats.ResourceUsage.MemoryStats; memory != nil {
			used := memory.Usage
			if used == 0 {
				used = memory.RSS
			}
			frame["memoryUsed"] = used
			frame["memoryPercent"] = percent(float64(used), float64(memoryTotal)*(1<<20))
		}
		return frame, true
	})
}

func streamNodeMetrics(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := nomadSession(rc)
	if err != nil {
		return err
	}
	node, err := s.node(rc)
	if err != nil {
		return err
	}
	cpuTotal, memoryTotal, diskTotal := nodeCapacity(node.NodeResources)
	q := (&api.QueryOptions{Region: regionOf(rc, s)}).WithContext(rc.Ctx)
	return tickStream(rc, client, s.opts.MetricTick, func() (any, bool) {
		frame := row{
			"cpuTotal":    cpuTotal,
			"memoryTotal": memoryTotal << 20,
			"diskTotal":   diskTotal << 20,
		}
		stats, err := s.client.Nodes().Stats(node.ID, q)
		if err != nil || stats == nil {
			return frame, true
		}
		frame["uptime"] = stats.Uptime
		frame["cpuTicks"] = round2(stats.CPUTicksConsumed)
		frame["cpuUsed"] = round2(stats.CPUTicksConsumed)
		if idle, ok := averageIdle(stats.CPU); ok {
			frame["cpuPercent"] = round2(100 - idle)
		}
		if stats.Memory != nil && stats.Memory.Total > 0 {
			frame["memoryUsed"] = stats.Memory.Used
			frame["memoryTotal"] = stats.Memory.Total
			frame["memoryPercent"] = percent(float64(stats.Memory.Used), float64(stats.Memory.Total))
		}
		if stats.AllocDirStats != nil && stats.AllocDirStats.Size > 0 {
			frame["diskUsed"] = stats.AllocDirStats.Used
			frame["diskTotal"] = stats.AllocDirStats.Size
			frame["diskPercent"] = round2(stats.AllocDirStats.UsedPercent)
		}
		return frame, true
	})
}

func watchJob(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := nomadSession(rc)
	if err != nil {
		return err
	}
	return tickStream(rc, client, s.opts.MetricTick, func() (any, bool) {
		out, err := jobOverview(rc)
		if err != nil {
			return nil, false
		}
		return out, true
	})
}

func watchAllocation(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := nomadSession(rc)
	if err != nil {
		return err
	}
	return tickStream(rc, client, s.opts.MetricTick, func() (any, bool) {
		out, err := allocOverview(rc)
		if err != nil {
			return nil, false
		}
		return out, true
	})
}

func watchNode(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := nomadSession(rc)
	if err != nil {
		return err
	}
	return tickStream(rc, client, s.opts.MetricTick, func() (any, bool) {
		out, err := nodeOverview(rc)
		if err != nil {
			return nil, false
		}
		return out, true
	})
}

// watchTopics maps a manifest resource kind onto the Nomad event-stream topic
// that reports its changes. A kind that is absent has no change feed.
var watchTopics = map[string]api.Topic{
	"job":        api.TopicJob,
	"allocation": api.TopicAllocation,
	"node":       api.TopicNode,
	"deployment": api.TopicDeployment,
	"evaluation": api.TopicEvaluation,
}

// watchResources turns one Nomad event-stream subscription into the resource
// deltas a live grid patches itself from. One route serves every watchable kind
// because the topic is the only thing that varies.
func watchResources(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := nomadSession(rc)
	if err != nil {
		return err
	}
	kind := param(rc, "kind")
	topic, ok := watchTopics[kind]
	if !ok {
		return fmt.Errorf("%w: %q has no change feed", plugin.ErrNotSupported, kind)
	}
	ctx, cancel := context.WithCancel(rc.Ctx)
	defer cancel()
	go func() {
		select {
		case <-client.Context().Done():
		case <-ctx.Done():
		}
		cancel()
	}()

	q := &api.QueryOptions{Region: regionOf(rc, s), Namespace: listNamespace(rc, s)}
	events, err := s.watch.EventStream().Stream(ctx, map[api.Topic][]string{topic: {"*"}}, 0, q)
	if err != nil {
		return nomadErr(err)
	}
	enc := json.NewEncoder(client)
	for batch := range events {
		if batch.Err != nil {
			// A closed subscription is how the cluster ends a watch; only a real
			// failure should surface to the browser as an error.
			if ctx.Err() != nil || errors.Is(batch.Err, io.EOF) || errors.Is(batch.Err, io.ErrUnexpectedEOF) {
				return nil
			}
			return nomadErr(batch.Err)
		}
		for i := range batch.Events {
			event, ok := s.resourceEvent(rc, kind, &batch.Events[i])
			if !ok {
				continue
			}
			if err := enc.Encode(event); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Session) resourceEvent(rc *plugin.RequestContext, kind string, event *api.Event) (plugin.ResourceEvent, bool) {
	switch kind {
	case "job":
		job, deleted, err := event.DeregisteredJob()
		if err != nil || job == nil {
			return plugin.ResourceEvent{}, false
		}
		out := jobEventRow(job)
		// The event carries no summary and a cluster-wide watch spans namespaces,
		// so the counts are re-read in the job's own namespace. applySummary runs
		// either way: a patched row that dropped these keys would blank the
		// allocation columns of the row it replaces.
		var summary *api.JobSummary
		if !deleted {
			q := (&api.QueryOptions{
				Region:    regionOf(rc, s),
				Namespace: namespaceOrDefault(deref(job.Namespace), s.opts.Namespace),
			}).WithContext(rc.Ctx)
			summary, _, _ = s.client.Jobs().Summary(deref(job.ID), q)
		}
		applySummary(out, summary)
		return resourceEventFor(out, eventType(deleted)), true
	case "allocation":
		alloc, err := event.Allocation()
		if err != nil || alloc == nil {
			return plugin.ResourceEvent{}, false
		}
		return resourceEventFor(allocRow(alloc.Stub()), eventType(false)), true
	case "node":
		node, err := event.Node()
		if err != nil || node == nil {
			return plugin.ResourceEvent{}, false
		}
		return resourceEventFor(nodeEventRow(node), eventType(false)), true
	case "deployment":
		deployment, err := event.Deployment()
		if err != nil || deployment == nil {
			return plugin.ResourceEvent{}, false
		}
		return resourceEventFor(deploymentRow(deployment), eventType(false)), true
	case "evaluation":
		eval, err := event.Evaluation()
		if err != nil || eval == nil {
			return plugin.ResourceEvent{}, false
		}
		return resourceEventFor(evaluationRow(eval), eventType(false)), true
	default:
		return plugin.ResourceEvent{}, false
	}
}

func resourceEventFor(item row, eventType string) plugin.ResourceEvent {
	ref, _ := item["ref"].(plugin.ResourceIdentity)
	return plugin.ResourceEvent{Type: eventType, Ref: ref, Resource: item}
}

func eventType(deleted bool) string {
	if deleted {
		return "deleted"
	}
	return "modified"
}

// deploymentTerminal marks the rollout states that will never progress again, so
// the task panel stops polling once the deployment settles.
var deploymentTerminal = map[string]string{
	"successful": "succeeded",
	"failed":     "failed",
	"cancelled":  "cancelled",
}

func streamDeploymentProgress(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := nomadSession(rc)
	if err != nil {
		return err
	}
	started := time.Now().UTC()
	return tickStream(rc, client, s.opts.MetricTick, func() (any, bool) {
		deployment, err := s.deployment(rc)
		if err != nil {
			return row{"status": "failed", "message": err.Error(), "startedAt": started, "finishedAt": time.Now().UTC()}, false
		}
		desired, placed, healthy, unhealthy, canaries := deploymentTotals(deployment)
		status, terminal := deploymentTerminal[deployment.Status]
		if !terminal {
			status = "running"
			if deployment.Status == "paused" {
				status = "paused"
			}
		}
		frame := row{
			"status":    status,
			"message":   deployment.StatusDescription,
			"progress":  percent(float64(healthy), float64(desired)),
			"current":   healthy,
			"total":     desired,
			"placed":    placed,
			"unhealthy": unhealthy,
			"canaries":  canaries,
			"startedAt": unixNano(deployment.CreateTime),
		}
		if terminal {
			frame["finishedAt"] = unixNano(deployment.ModifyTime)
		}
		return frame, !terminal
	})
}

// tickStream writes one frame immediately, then on every tick, until the browser
// leaves or the producer reports it has nothing left to send.
func tickStream(rc *plugin.RequestContext, client plugin.ClientStream, interval time.Duration, next func() (any, bool)) error {
	enc := json.NewEncoder(client)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		frame, more := next()
		if frame != nil {
			if err := enc.Encode(frame); err != nil {
				return err
			}
		}
		if !more {
			return nil
		}
		select {
		case <-client.Context().Done():
			return nil
		case <-rc.Ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
