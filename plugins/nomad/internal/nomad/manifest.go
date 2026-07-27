package nomad

import "github.com/charlesng35/shellcn/sdk/plugin"

func icon(name string) plugin.Icon {
	return plugin.Icon{Type: plugin.IconLucide, Value: name}
}

func scope() []plugin.ScopeFilter {
	return []plugin.ScopeFilter{
		{
			Param: "namespace", Label: "Namespace", Icon: icon("folder"), Control: plugin.ScopeSelect,
			OptionsSource: &plugin.DataSource{RouteID: "nomad.scope.namespaces"},
			ValueField:    "value", LabelField: "label",
		},
		{
			Param: "region", Label: "Region", Icon: icon("globe"), Control: plugin.ScopeSelect,
			OptionsSource: &plugin.DataSource{RouteID: "nomad.regions.list"},
			ValueField:    "name", LabelField: "label", AllLabel: "Connection region",
		},
	}
}

func streams() []plugin.Stream {
	return []plugin.Stream{
		{ID: "nomad.job.watch", Kind: plugin.StreamResource, RouteID: "nomad.job.watch"},
		{ID: "nomad.alloc.watch", Kind: plugin.StreamResource, RouteID: "nomad.alloc.watch"},
		{ID: "nomad.node.watch", Kind: plugin.StreamResource, RouteID: "nomad.node.watch"},
		{ID: "nomad.alloc.logs", Kind: plugin.StreamLogs, RouteID: "nomad.alloc.logs"},
		{ID: "nomad.alloc.exec", Kind: plugin.StreamTerminal, RouteID: "nomad.alloc.exec"},
		{ID: "nomad.alloc.metrics", Kind: plugin.StreamMetrics, RouteID: "nomad.alloc.metrics"},
		{ID: "nomad.node.metrics", Kind: plugin.StreamMetrics, RouteID: "nomad.node.metrics"},
		{ID: "nomad.deployment.progress", Kind: plugin.StreamTask, RouteID: "nomad.deployment.progress"},
		{ID: "nomad.resources.watch", Kind: plugin.StreamResource, RouteID: "nomad.resources.watch"},
	}
}

// resourceWatch binds a resource list to the Nomad event stream for its kind.
func resourceWatch(kind string) *plugin.DataSource {
	return &plugin.DataSource{RouteID: "nomad.resources.watch", Method: plugin.MethodWS, Params: map[string]string{"kind": kind}}
}

func recording() []plugin.RecordingCapability {
	return []plugin.RecordingCapability{{
		Class:         plugin.RecordingTerminal,
		Formats:       []plugin.RecordingFormat{plugin.FormatAsciicastV2},
		StreamIDs:     []string{"nomad.alloc.exec"},
		Authoritative: true,
		InputCapture:  true,
	}}
}

func tree() []plugin.TreeGroup {
	return []plugin.TreeGroup{
		{Key: "cluster", Label: "Cluster", Icon: icon("ship-wheel"), Ref: &plugin.ResourceIdentity{Kind: "cluster", Name: "cluster", UID: "cluster"}},
		{Key: "namespaces", Label: "Namespaces", Icon: icon("folders"), Source: plugin.DataSource{RouteID: "nomad.tree.namespaces"}},
		{Key: "jobs", Label: "Jobs", Icon: icon("briefcase"), ResourceKind: "job"},
		{Key: "allocations", Label: "Allocations", Icon: icon("boxes"), ResourceKind: "allocation"},
		{Key: "clients", Label: "Clients", Icon: icon("hard-drive"), ResourceKind: "node"},
		{Key: "deployments", Label: "Deployments", Icon: icon("rocket"), ResourceKind: "deployment"},
		{Key: "evaluations", Label: "Evaluations", Icon: icon("list-checks"), ResourceKind: "evaluation"},
		{Key: "csi-volumes", Label: "CSI volumes", Icon: icon("database"), ResourceKind: "volume"},
		{Key: "host-volumes", Label: "Host volumes", Icon: icon("folder-tree"), ResourceKind: "host_volume"},
	}
}

func jobSeverities() map[string]plugin.Severity {
	return map[string]plugin.Severity{
		"running": plugin.SeveritySuccess,
		"pending": plugin.SeverityWarn,
		"dead":    plugin.SeveritySecondary,
	}
}

func allocSeverities() map[string]plugin.Severity {
	return map[string]plugin.Severity{
		"running":  plugin.SeveritySuccess,
		"pending":  plugin.SeverityWarn,
		"starting": plugin.SeverityWarn,
		"complete": plugin.SeveritySecondary,
		"failed":   plugin.SeverityDanger,
		"lost":     plugin.SeverityDanger,
		"unknown":  plugin.SeverityWarn,
	}
}

func nodeSeverities() map[string]plugin.Severity {
	return map[string]plugin.Severity{
		"ready":        plugin.SeveritySuccess,
		"initializing": plugin.SeverityWarn,
		"down":         plugin.SeverityDanger,
		"disconnected": plugin.SeverityWarn,
	}
}

func deploymentSeverities() map[string]plugin.Severity {
	return map[string]plugin.Severity{
		"running":    plugin.SeverityInfo,
		"paused":     plugin.SeverityWarn,
		"successful": plugin.SeveritySuccess,
		"failed":     plugin.SeverityDanger,
		"cancelled":  plugin.SeveritySecondary,
	}
}

func evalSeverities() map[string]plugin.Severity {
	return map[string]plugin.Severity{
		"pending":  plugin.SeverityWarn,
		"blocked":  plugin.SeverityWarn,
		"complete": plugin.SeveritySuccess,
		"failed":   plugin.SeverityDanger,
		"canceled": plugin.SeveritySecondary,
	}
}

func volumeSeverities() map[string]plugin.Severity {
	return map[string]plugin.Severity{
		"ready":       plugin.SeveritySuccess,
		"pending":     plugin.SeverityWarn,
		"unavailable": plugin.SeverityDanger,
	}
}

func memberSeverities() map[string]plugin.Severity {
	return map[string]plugin.Severity{
		"alive":   plugin.SeveritySuccess,
		"leaving": plugin.SeverityWarn,
		"left":    plugin.SeveritySecondary,
		"failed":  plugin.SeverityDanger,
	}
}

func jobColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Job", Sortable: true},
		{Key: "namespace", Label: "Namespace", Sortable: true},
		{Key: "type", Label: "Type", Type: plugin.ColumnBadge, Severities: map[string]plugin.Severity{
			"service": plugin.SeverityInfo, "batch": plugin.SeveritySecondary, "system": plugin.SeverityWarn, "sysbatch": plugin.SeveritySecondary,
		}},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: jobSeverities()},
		{Key: "priority", Label: "Priority", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "running", Label: "Running", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "queued", Label: "Queued", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "failed", Label: "Failed", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "submitTime", Label: "Submitted", Type: plugin.ColumnRelativeTime, Sortable: true},
	}
}

func allocColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "shortId", Label: "ID", Width: "9rem"},
		{Key: "name", Label: "Allocation", Sortable: true},
		{Key: "namespace", Label: "Namespace", Sortable: true},
		{Key: "taskGroup", Label: "Task group", Sortable: true},
		{Key: "clientStatus", Label: "Client status", Type: plugin.ColumnBadge, Severities: allocSeverities()},
		{Key: "desiredStatus", Label: "Desired", Type: plugin.ColumnBadge, Severities: map[string]plugin.Severity{
			"run": plugin.SeveritySuccess, "stop": plugin.SeveritySecondary, "evict": plugin.SeverityDanger,
		}},
		{Key: "nodeName", Label: "Client", Sortable: true},
		{Key: "jobVersion", Label: "Version", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "cpu", Label: "CPU", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "memory", Label: "Memory (MB)", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "modifyTime", Label: "Modified", Type: plugin.ColumnRelativeTime, Sortable: true},
	}
}

func nodeColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Client", Sortable: true},
		{Key: "shortId", Label: "ID", Width: "9rem"},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: nodeSeverities()},
		{Key: "eligibility", Label: "Eligibility", Type: plugin.ColumnBadge, Severities: map[string]plugin.Severity{
			"eligible": plugin.SeveritySuccess, "ineligible": plugin.SeverityWarn,
		}},
		{Key: "drain", Label: "Draining", Type: plugin.ColumnBool},
		{Key: "datacenter", Label: "Datacenter", Sortable: true},
		{Key: "nodePool", Label: "Node pool", Sortable: true},
		{Key: "nodeClass", Label: "Class", Sortable: true},
		{Key: "cpu", Label: "CPU (MHz)", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "memory", Label: "Memory (MB)", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "version", Label: "Version", Sortable: true},
	}
}

func deploymentColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "shortId", Label: "ID", Width: "9rem"},
		{Key: "jobId", Label: "Job", Sortable: true},
		{Key: "namespace", Label: "Namespace", Sortable: true},
		{Key: "jobVersion", Label: "Version", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: deploymentSeverities()},
		{Key: "progress", Label: "Healthy", Type: plugin.ColumnPercent, Sortable: true},
		{Key: "desired", Label: "Desired", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "placed", Label: "Placed", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "unhealthy", Label: "Unhealthy", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "createTime", Label: "Created", Type: plugin.ColumnRelativeTime, Sortable: true},
	}
}

func evalColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "shortId", Label: "ID", Width: "9rem"},
		{Key: "jobId", Label: "Job", Sortable: true},
		{Key: "namespace", Label: "Namespace", Sortable: true},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: evalSeverities()},
		{Key: "type", Label: "Type", Type: plugin.ColumnBadge},
		{Key: "triggeredBy", Label: "Triggered by", Sortable: true},
		{Key: "priority", Label: "Priority", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "failedGroups", Label: "Blocked groups", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "createTime", Label: "Created", Type: plugin.ColumnRelativeTime, Sortable: true},
	}
}

func volumeColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "id", Label: "Volume", Sortable: true},
		{Key: "name", Label: "Name", Sortable: true},
		{Key: "namespace", Label: "Namespace", Sortable: true},
		{Key: "pluginId", Label: "Plugin", Sortable: true},
		{Key: "provider", Label: "Provider", Sortable: true},
		{Key: "accessMode", Label: "Access mode", Type: plugin.ColumnBadge},
		{Key: "schedulable", Label: "Schedulable", Type: plugin.ColumnBool},
		{Key: "readers", Label: "Readers", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "writers", Label: "Writers", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "createTime", Label: "Created", Type: plugin.ColumnRelativeTime, Sortable: true},
	}
}

func hostVolumeColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Volume", Sortable: true},
		{Key: "namespace", Label: "Namespace", Sortable: true},
		{Key: "state", Label: "State", Type: plugin.ColumnBadge, Severities: volumeSeverities()},
		{Key: "pluginId", Label: "Plugin", Sortable: true},
		{Key: "nodePool", Label: "Node pool", Sortable: true},
		{Key: "nodeId", Label: "Client", Width: "18rem"},
		{Key: "capacity", Label: "Capacity", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "createTime", Label: "Created", Type: plugin.ColumnRelativeTime, Sortable: true},
	}
}

func taskGroupColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Task group", Sortable: true},
		{Key: "count", Label: "Count", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "tasks", Label: "Tasks", Type: plugin.ColumnJSON},
		{Key: "drivers", Label: "Drivers", Type: plugin.ColumnJSON},
		{Key: "cpu", Label: "CPU (MHz)", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "memory", Label: "Memory (MB)", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "disk", Label: "Disk (MB)", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "services", Label: "Services", Type: plugin.ColumnNumber},
	}
}

func jobVersionColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "version", Label: "Version", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "stable", Label: "Stable", Type: plugin.ColumnBool},
		{Key: "tag", Label: "Tag"},
		{Key: "changes", Label: "Changes", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "submitTime", Label: "Submitted", Type: plugin.ColumnRelativeTime, Sortable: true},
	}
}

func memberColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Server", Sortable: true},
		{Key: "leader", Label: "Leader", Type: plugin.ColumnBool},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: memberSeverities()},
		{Key: "address", Label: "Address"},
		{Key: "port", Label: "Port", Type: plugin.ColumnNumber},
		{Key: "region", Label: "Region", Sortable: true},
		{Key: "datacenter", Label: "Datacenter", Sortable: true},
		{Key: "build", Label: "Build"},
	}
}

func namespaceColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Namespace", Sortable: true},
		{Key: "description", Label: "Description"},
		{Key: "quota", Label: "Quota"},
	}
}

func regionColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Region", Sortable: true}}
}

func datacenterColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Datacenter", Sortable: true},
		{Key: "clients", Label: "Clients", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "ready", Label: "Ready", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "eligible", Label: "Eligible", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "draining", Label: "Draining", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "nodePools", Label: "Node pools", Type: plugin.ColumnJSON},
		{Key: "cpu", Label: "CPU (MHz)", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "memory", Label: "Memory (MB)", Type: plugin.ColumnNumber, Sortable: true},
	}
}

func actions() []plugin.Action {
	jobParams := map[string]string{"ns": "${resource.namespace}", "job": "${resource.name}"}
	allocParams := map[string]string{"ns": "${resource.namespace}", "alloc": "${resource.uid}"}
	nodeParams := map[string]string{"node": "${resource.uid}"}
	deploymentParams := map[string]string{"ns": "${resource.namespace}", "deployment": "${resource.uid}"}
	return []plugin.Action{
		{
			ID: "nomad.job.submit", Label: "Submit job", Icon: icon("upload"), RouteID: "nomad.job.submit",
			Open: plugin.OpenDialog, Panel: plugin.PanelCodeEditor,
			Config: plugin.CodeEditorConfig{
				Language: "hcl", InitialContent: sampleJobHCL,
				SaveRouteID: "nomad.job.submit", SaveMethod: plugin.MethodPost,
				SaveDismiss: plugin.SaveDismissClose,
				SaveToast:   &plugin.SaveToast{Summary: "Job submitted", Detail: "${response.job} registered as evaluation ${response.evalId}", Severity: plugin.SeveritySuccess},
			},
			OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList},
		},
		{ID: "nomad.job.plan", Label: "Plan job", Icon: icon("clipboard-check"), RouteID: "nomad.job.plan"},
		{ID: "nomad.job.evaluate", Label: "Force evaluate", Icon: icon("refresh-cw"), RouteID: "nomad.job.evaluate", Params: jobParams, Group: "Lifecycle"},
		{
			ID: "nomad.job.periodic", Label: "Force periodic run", Icon: icon("timer"), RouteID: "nomad.job.periodic", Params: jobParams, Group: "Lifecycle",
			VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "periodic", Op: plugin.OpEq, Value: true}}},
		},
		{
			ID: "nomad.job.scale", Label: "Scale group", Icon: icon("scaling"), RouteID: "nomad.job.scale", Params: jobParams, Group: "Lifecycle",
			VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "type", Op: plugin.OpIn, Value: []any{"service", "batch"}}}},
		},
		{
			ID: "nomad.job.restart", Label: "Restart allocations", Icon: icon("rotate-ccw"), RouteID: "nomad.job.restart", Params: jobParams, Group: "Lifecycle",
			Confirm: true, ConfirmText: "Restart every running allocation of this job?",
		},
		{
			ID: "nomad.job.revert", Label: "Revert to this version", Icon: icon("undo-2"), RouteID: "nomad.job.revert",
			Params:  jobParams,
			Confirm: true, ConfirmText: "Roll the job back to this version? Running allocations are replaced.",
		},
		{
			ID: "nomad.job.stop", Label: "Stop", Icon: icon("square"), RouteID: "nomad.job.stop", Params: jobParams,
			Confirm: true, ConfirmText: "Stop this job? Every allocation is shut down.", Bulk: true,
		},
		{
			ID: "nomad.job.purge", Label: "Purge", Icon: icon("trash-2"), RouteID: "nomad.job.purge", Params: jobParams,
			Confirm: true, ConfirmText: "Purge this job? It is removed from state and cannot be inspected afterwards.", Bulk: true,
		},

		{ID: "nomad.alloc.restart", Label: "Restart", Icon: icon("rotate-ccw"), RouteID: "nomad.alloc.restart", Params: allocParams},
		{ID: "nomad.alloc.signal", Label: "Send signal", Icon: icon("zap"), RouteID: "nomad.alloc.signal", Params: allocParams},
		{
			ID: "nomad.alloc.stop", Label: "Stop", Icon: icon("square"), RouteID: "nomad.alloc.stop", Params: allocParams,
			Confirm: true, ConfirmText: "Stop this allocation? It is rescheduled according to the job policy.", Bulk: true,
		},

		{
			ID: "nomad.node.drain", Label: "Drain", Icon: icon("download"), RouteID: "nomad.node.drain", Params: nodeParams,
			Confirm: true, ConfirmText: "Drain this client? Every allocation migrates off it.",
			VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "drain", Op: plugin.OpNeq, Value: true}}},
		},
		{
			ID: "nomad.node.drain.cancel", Label: "Cancel drain", Icon: icon("circle-stop"), RouteID: "nomad.node.drain.cancel", Params: nodeParams,
			VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "drain", Op: plugin.OpEq, Value: true}}},
		},
		{ID: "nomad.node.eligibility", Label: "Scheduling eligibility", Icon: icon("toggle-left"), RouteID: "nomad.node.eligibility", Params: nodeParams},

		{ID: "nomad.deployment.promote", Label: "Promote canaries", Icon: icon("badge-check"), RouteID: "nomad.deployment.promote", Params: deploymentParams},
		{ID: "nomad.deployment.pause", Label: "Pause or resume", Icon: icon("pause"), RouteID: "nomad.deployment.pause", Params: deploymentParams},
		{ID: "nomad.deployment.unblock", Label: "Unblock", Icon: icon("unlock"), RouteID: "nomad.deployment.unblock", Params: deploymentParams},
		{
			ID: "nomad.deployment.fail", Label: "Fail rollout", Icon: icon("octagon-x"), RouteID: "nomad.deployment.fail", Params: deploymentParams,
			Confirm: true, ConfirmText: "Fail this rollout? Nomad reverts to the previous stable version when auto-revert is on.",
		},
	}
}

func objectDetail(sections []plugin.ObjectDetailSection, watch *plugin.DataSource) plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{Sections: sections, RawToggle: true, Watch: watch}
}

func jobSections() []plugin.ObjectDetailSection {
	return []plugin.ObjectDetailSection{
		{Title: "Identity", Fields: []plugin.ObjectDetailField{
			{Key: "id", Label: "Job ID", Copy: true},
			{Key: "name", Label: "Name"},
			{Key: "namespace", Label: "Namespace"},
			{Key: "type", Label: "Type", Type: plugin.ColumnBadge},
			{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: jobSeverities()},
			{Key: "statusDescription", Label: "Status detail"},
			{Key: "priority", Label: "Priority", Type: plugin.ColumnNumber},
			{Key: "region", Label: "Region"},
			{Key: "nodePool", Label: "Node pool"},
			{Key: "datacenters", Label: "Datacenters", Type: plugin.ColumnJSON},
		}},
		{Title: "Allocations", Fields: []plugin.ObjectDetailField{
			{Key: "running", Label: "Running", Type: plugin.ColumnNumber},
			{Key: "starting", Label: "Starting", Type: plugin.ColumnNumber},
			{Key: "queued", Label: "Queued", Type: plugin.ColumnNumber},
			{Key: "complete", Label: "Complete", Type: plugin.ColumnNumber},
			{Key: "failed", Label: "Failed", Type: plugin.ColumnNumber},
			{Key: "lost", Label: "Lost", Type: plugin.ColumnNumber},
		}},
		{Title: "Version", Fields: []plugin.ObjectDetailField{
			{Key: "version", Label: "Version", Type: plugin.ColumnNumber},
			{Key: "stable", Label: "Stable", Type: plugin.ColumnBool},
			{Key: "stopped", Label: "Stopped", Type: plugin.ColumnBool},
			{Key: "periodic", Label: "Periodic", Type: plugin.ColumnBool},
			{Key: "parameterized", Label: "Parameterized", Type: plugin.ColumnBool},
			{Key: "multiregion", Label: "Multiregion", Type: plugin.ColumnBool},
			{Key: "latestDeployment", Label: "Latest deployment", Copy: true},
			{Key: "deploymentStatus", Label: "Deployment status", Type: plugin.ColumnBadge, Severities: deploymentSeverities()},
			{Key: "submitTime", Label: "Submitted", Type: plugin.ColumnDateTime},
			{Key: "jobModifyIndex", Label: "Job modify index", Type: plugin.ColumnNumber},
			{Key: "meta", Label: "Meta", Type: plugin.ColumnJSON},
		}},
	}
}

func allocSections() []plugin.ObjectDetailSection {
	return []plugin.ObjectDetailSection{
		{Title: "Identity", Fields: []plugin.ObjectDetailField{
			{Key: "id", Label: "Allocation ID", Copy: true},
			{Key: "name", Label: "Name"},
			{Key: "namespace", Label: "Namespace"},
			{Key: "jobId", Label: "Job", Copy: true},
			{Key: "taskGroup", Label: "Task group"},
			{Key: "tasks", Label: "Tasks", Type: plugin.ColumnJSON},
		}},
		{Title: "Status", Fields: []plugin.ObjectDetailField{
			{Key: "clientStatus", Label: "Client status", Type: plugin.ColumnBadge, Severities: allocSeverities()},
			{Key: "clientDescription", Label: "Client detail"},
			{Key: "desiredStatus", Label: "Desired status", Type: plugin.ColumnBadge},
			{Key: "desiredDescription", Label: "Desired detail"},
			{Key: "deploymentHealthy", Label: "Deployment healthy", Type: plugin.ColumnBool},
			{Key: "canary", Label: "Canary", Type: plugin.ColumnBool},
			{Key: "failedTasks", Label: "Failed tasks", Type: plugin.ColumnNumber},
			{Key: "restarts", Label: "Restarts", Type: plugin.ColumnNumber},
			{Key: "rescheduleAttempts", Label: "Reschedule attempts", Type: plugin.ColumnNumber},
		}},
		{Title: "Placement", Fields: []plugin.ObjectDetailField{
			{Key: "nodeName", Label: "Client"},
			{Key: "nodeId", Label: "Client ID", Copy: true},
			{Key: "address", Label: "Address", Copy: true},
			{Key: "evalId", Label: "Evaluation", Copy: true},
			{Key: "deploymentId", Label: "Deployment", Copy: true},
			{Key: "previousAllocation", Label: "Previous allocation", Copy: true},
			{Key: "nextAllocation", Label: "Next allocation", Copy: true},
		}},
		{Title: "Resources", Fields: []plugin.ObjectDetailField{
			{Key: "cpu", Label: "CPU reserved", Type: plugin.ColumnNumber},
			{Key: "memory", Label: "Memory reserved (MB)", Type: plugin.ColumnNumber},
			{Key: "disk", Label: "Ephemeral disk (MB)", Type: plugin.ColumnNumber},
			{Key: "createTime", Label: "Created", Type: plugin.ColumnDateTime},
			{Key: "modifyTime", Label: "Modified", Type: plugin.ColumnRelativeTime},
		}},
	}
}

func nodeSections() []plugin.ObjectDetailSection {
	return []plugin.ObjectDetailSection{
		{Title: "Identity", Fields: []plugin.ObjectDetailField{
			{Key: "name", Label: "Client"},
			{Key: "id", Label: "Client ID", Copy: true},
			{Key: "address", Label: "HTTP address", Copy: true},
			{Key: "datacenter", Label: "Datacenter"},
			{Key: "nodePool", Label: "Node pool"},
			{Key: "nodeClass", Label: "Class"},
			{Key: "version", Label: "Nomad version"},
			{Key: "os", Label: "Operating system"},
			{Key: "kernel", Label: "Kernel"},
			{Key: "arch", Label: "Architecture"},
		}},
		{Title: "Scheduling", Fields: []plugin.ObjectDetailField{
			{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: nodeSeverities()},
			{Key: "statusDescription", Label: "Status detail"},
			{Key: "eligibility", Label: "Eligibility", Type: plugin.ColumnBadge},
			{Key: "drain", Label: "Draining", Type: plugin.ColumnBool},
			{Key: "drainDeadline", Label: "Drain deadline", Type: plugin.ColumnDateTime},
			{Key: "drainIgnoresSystemJobs", Label: "Drain keeps system jobs", Type: plugin.ColumnBool},
			{Key: "lastDrainStatus", Label: "Last drain", Type: plugin.ColumnBadge},
			{Key: "lastDrainAt", Label: "Last drain at", Type: plugin.ColumnRelativeTime},
			{Key: "maxAllocs", Label: "Max allocations", Type: plugin.ColumnNumber},
		}},
		{Title: "Capacity", Fields: []plugin.ObjectDetailField{
			{Key: "cpuPercent", Label: "CPU usage", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
				PercentKey: "cpuPercent", TotalKey: "cpu", TotalType: plugin.ColumnNumber, TotalLabel: "of", Unit: "MHz", WarnAt: 75, CriticalAt: 90,
			}},
			{Key: "memoryPercent", Label: "Memory usage", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
				PercentKey: "memoryPercent", UsedKey: "memoryUsed", TotalKey: "memoryTotal",
				UsedType: plugin.ColumnBytes, TotalType: plugin.ColumnBytes, WarnAt: 80, CriticalAt: 95,
			}},
			{Key: "diskPercent", Label: "Alloc disk usage", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
				PercentKey: "diskPercent", UsedKey: "diskUsed", TotalKey: "diskTotal",
				UsedType: plugin.ColumnBytes, TotalType: plugin.ColumnBytes, WarnAt: 80, CriticalAt: 95,
			}},
			{Key: "memory", Label: "Schedulable memory (MB)", Type: plugin.ColumnNumber},
			{Key: "disk", Label: "Schedulable disk (MB)", Type: plugin.ColumnNumber},
			{Key: "reservedCpu", Label: "Reserved CPU (MHz)", Type: plugin.ColumnNumber},
			{Key: "reservedMemory", Label: "Reserved memory (MB)", Type: plugin.ColumnNumber},
			{Key: "drivers", Label: "Healthy drivers", Type: plugin.ColumnJSON},
			{Key: "hostVolumes", Label: "Host volumes", Type: plugin.ColumnJSON},
			{Key: "meta", Label: "Meta", Type: plugin.ColumnJSON},
		}},
	}
}

func deploymentSections() []plugin.ObjectDetailSection {
	return []plugin.ObjectDetailSection{
		{Title: "Rollout", Fields: []plugin.ObjectDetailField{
			{Key: "id", Label: "Deployment ID", Copy: true},
			{Key: "jobId", Label: "Job", Copy: true},
			{Key: "namespace", Label: "Namespace"},
			{Key: "jobVersion", Label: "Job version", Type: plugin.ColumnNumber},
			{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: deploymentSeverities()},
			{Key: "statusDescription", Label: "Status detail"},
			{Key: "multiregion", Label: "Multiregion", Type: plugin.ColumnBool},
		}},
		{Title: "Progress", Fields: []plugin.ObjectDetailField{
			{Key: "progress", Label: "Healthy allocations", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
				PercentKey: "progress", UsedKey: "healthy", TotalKey: "desired",
				UsedType: plugin.ColumnNumber, TotalType: plugin.ColumnNumber, TotalLabel: "of", Unit: "allocation(s)", WarnAt: 50, CriticalAt: 25,
			}},
			{Key: "placed", Label: "Placed", Type: plugin.ColumnNumber},
			{Key: "unhealthy", Label: "Unhealthy", Type: plugin.ColumnNumber},
			{Key: "canaries", Label: "Canaries", Type: plugin.ColumnNumber},
			{Key: "groups", Label: "Task groups", Type: plugin.ColumnJSON},
			{Key: "createTime", Label: "Created", Type: plugin.ColumnDateTime},
			{Key: "modifyTime", Label: "Modified", Type: plugin.ColumnRelativeTime},
		}},
	}
}

func evalSections() []plugin.ObjectDetailSection {
	return []plugin.ObjectDetailSection{
		{Title: "Evaluation", Fields: []plugin.ObjectDetailField{
			{Key: "id", Label: "Evaluation ID", Copy: true},
			{Key: "jobId", Label: "Job", Copy: true},
			{Key: "namespace", Label: "Namespace"},
			{Key: "nodeId", Label: "Client", Copy: true},
			{Key: "deploymentId", Label: "Deployment", Copy: true},
			{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: evalSeverities()},
			{Key: "statusDescription", Label: "Status detail"},
			{Key: "type", Label: "Type", Type: plugin.ColumnBadge},
			{Key: "triggeredBy", Label: "Triggered by"},
			{Key: "priority", Label: "Priority", Type: plugin.ColumnNumber},
		}},
		{Title: "Scheduling", Fields: []plugin.ObjectDetailField{
			{Key: "queuedAllocations", Label: "Queued allocations", Type: plugin.ColumnNumber},
			{Key: "placementFailures", Label: "Placement failures", Type: plugin.ColumnJSON},
			{Key: "quotaLimitReached", Label: "Quota limit reached"},
			{Key: "blockedEval", Label: "Blocked evaluation", Copy: true},
			{Key: "nextEval", Label: "Next evaluation", Copy: true},
			{Key: "previousEval", Label: "Previous evaluation", Copy: true},
			{Key: "waitUntil", Label: "Wait until", Type: plugin.ColumnDateTime},
			{Key: "createTime", Label: "Created", Type: plugin.ColumnDateTime},
			{Key: "modifyTime", Label: "Modified", Type: plugin.ColumnRelativeTime},
		}},
	}
}

func clusterSections() []plugin.ObjectDetailSection {
	return []plugin.ObjectDetailSection{
		{Title: "Cluster", Fields: []plugin.ObjectDetailField{
			{Key: "address", Label: "API address", Copy: true},
			{Key: "leader", Label: "Leader", Copy: true},
			{Key: "version", Label: "Nomad version"},
			{Key: "region", Label: "Region"},
			{Key: "datacenter", Label: "Datacenter"},
			{Key: "namespace", Label: "Default namespace"},
			{Key: "nodeName", Label: "Agent node"},
		}},
		{Title: "Topology", Fields: []plugin.ObjectDetailField{
			{Key: "serverCount", Label: "Servers", Type: plugin.ColumnNumber},
			{Key: "peers", Label: "Server peers", Type: plugin.ColumnJSON},
			{Key: "clientCount", Label: "Clients", Type: plugin.ColumnNumber},
			{Key: "clientsReady", Label: "Clients ready", Type: plugin.ColumnNumber},
			{Key: "clientCountPartial", Label: "Client counts capped by scan limit", Type: plugin.ColumnBool},
			{Key: "regions", Label: "Regions", Type: plugin.ColumnJSON},
		}},
		{Title: "Connection", Fields: []plugin.ObjectDetailField{
			{Key: "readOnly", Label: "Read-only", Type: plugin.ColumnBool},
			{Key: "execAllowed", Label: "Exec allowed", Type: plugin.ColumnBool},
		}},
	}
}

func volumeSections() []plugin.ObjectDetailSection {
	return []plugin.ObjectDetailSection{
		{Title: "Volume", Fields: []plugin.ObjectDetailField{
			{Key: "id", Label: "Volume ID", Copy: true},
			{Key: "name", Label: "Name"},
			{Key: "namespace", Label: "Namespace"},
			{Key: "externalId", Label: "External ID", Copy: true},
			{Key: "pluginId", Label: "Plugin"},
			{Key: "provider", Label: "Provider"},
			{Key: "providerVersion", Label: "Provider version"},
			{Key: "capacity", Label: "Capacity", Type: plugin.ColumnBytes},
		}},
		{Title: "Health", Fields: []plugin.ObjectDetailField{
			{Key: "schedulable", Label: "Schedulable", Type: plugin.ColumnBool},
			{Key: "accessMode", Label: "Access mode", Type: plugin.ColumnBadge},
			{Key: "attachmentMode", Label: "Attachment mode", Type: plugin.ColumnBadge},
			{Key: "controllersHealthy", Label: "Healthy controllers", Type: plugin.ColumnNumber},
			{Key: "controllersExpected", Label: "Expected controllers", Type: plugin.ColumnNumber},
			{Key: "nodesHealthy", Label: "Healthy nodes", Type: plugin.ColumnNumber},
			{Key: "nodesExpected", Label: "Expected nodes", Type: plugin.ColumnNumber},
			{Key: "readers", Label: "Readers", Type: plugin.ColumnNumber},
			{Key: "writers", Label: "Writers", Type: plugin.ColumnNumber},
			{Key: "createTime", Label: "Created", Type: plugin.ColumnDateTime},
		}},
	}
}

func hostVolumeSections() []plugin.ObjectDetailSection {
	return []plugin.ObjectDetailSection{
		{Title: "Volume", Fields: []plugin.ObjectDetailField{
			{Key: "name", Label: "Name"},
			{Key: "id", Label: "Volume ID", Copy: true},
			{Key: "namespace", Label: "Namespace"},
			{Key: "state", Label: "State", Type: plugin.ColumnBadge, Severities: volumeSeverities()},
			{Key: "pluginId", Label: "Plugin"},
			{Key: "nodeId", Label: "Client", Copy: true},
			{Key: "nodePool", Label: "Node pool"},
			{Key: "hostPath", Label: "Host path", Copy: true},
			{Key: "capacity", Label: "Capacity", Type: plugin.ColumnBytes},
			{Key: "allocations", Label: "Claiming allocations", Type: plugin.ColumnNumber},
			{Key: "parameters", Label: "Parameters", Type: plugin.ColumnJSON},
			{Key: "createTime", Label: "Created", Type: plugin.ColumnDateTime},
		}},
	}
}

func allocTable(params map[string]string) plugin.Panel {
	return plugin.Panel{
		Key: "allocations", Label: "Allocations", Icon: icon("boxes"), Type: plugin.PanelTable,
		Source: &plugin.DataSource{RouteID: "nomad.allocs.list", Params: params},
		Config: plugin.TableConfig{
			Columns: allocColumns(), RowActionIDs: []string{"nomad.alloc.stop"},
			DefaultSort: &plugin.SortKey{Field: "modifyTime", Desc: true},
			EmptyText:   "No allocations here yet.", RefreshIntervalMs: 10000, Exportable: true,
		},
	}
}

func resources() []plugin.ResourceType {
	jobParams := map[string]string{"ns": "${resource.namespace}", "job": "${resource.name}"}
	allocParams := map[string]string{"ns": "${resource.namespace}", "alloc": "${resource.uid}"}
	nodeParams := map[string]string{"node": "${resource.uid}"}
	deploymentParams := map[string]string{"ns": "${resource.namespace}", "deployment": "${resource.uid}"}

	return []plugin.ResourceType{
		{
			Kind: "cluster", Title: "Cluster", List: plugin.DataSource{RouteID: "nomad.cluster.list"},
			Columns: []plugin.Column{{Key: "name", Label: "Cluster"}, {Key: "leader", Label: "Leader"}},
			Detail: plugin.DetailView{
				Header: plugin.HeaderSpec{Title: "Cluster"}, DefaultTab: "overview",
				Tabs: []plugin.Panel{
					{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
						Source: &plugin.DataSource{RouteID: "nomad.cluster.overview"},
						Config: objectDetail(clusterSections(), nil)},
					{Key: "servers", Label: "Servers", Icon: icon("server"), Type: plugin.PanelTable,
						Source: &plugin.DataSource{RouteID: "nomad.members.list"},
						Config: plugin.TableConfig{Columns: memberColumns(), DefaultSort: &plugin.SortKey{Field: "name"}, EmptyText: "No server members reported.", RefreshIntervalMs: 15000, Exportable: true}},
					{Key: "cluster-namespaces", Label: "Namespaces", Icon: icon("folders"), Type: plugin.PanelTable,
						Source: &plugin.DataSource{RouteID: "nomad.namespaces.list"},
						Config: plugin.TableConfig{Columns: namespaceColumns(), DefaultSort: &plugin.SortKey{Field: "name"}, EmptyText: "No namespaces are readable with this token.", RefreshIntervalMs: 30000, Exportable: true}},
					{Key: "cluster-regions", Label: "Regions", Icon: icon("globe"), Type: plugin.PanelTable,
						Source: &plugin.DataSource{RouteID: "nomad.regions.list"},
						Config: plugin.TableConfig{Columns: regionColumns(), DefaultSort: &plugin.SortKey{Field: "name"}, EmptyText: "No federated regions.", RefreshIntervalMs: 30000}},
					{Key: "cluster-datacenters", Label: "Datacenters", Icon: icon("map-pin"), Type: plugin.PanelTable,
						Source: &plugin.DataSource{RouteID: "nomad.datacenters.list"},
						Config: plugin.TableConfig{Columns: datacenterColumns(), DefaultSort: &plugin.SortKey{Field: "name"}, EmptyText: "No client has declared a datacenter.", RefreshIntervalMs: 30000, Exportable: true}},
				},
			},
		},
		{
			Kind: "job", Title: "Jobs", List: plugin.DataSource{RouteID: "nomad.jobs.list"}, Watch: resourceWatch("job"), Columns: jobColumns(),
			Actions: plugin.ResourceActions{
				Toolbar: []string{"nomad.job.submit", "nomad.job.plan"},
				Row:     []string{"nomad.job.stop", "nomad.job.purge"},
				Detail:  []string{"nomad.job.evaluate", "nomad.job.periodic", "nomad.job.restart", "nomad.job.stop", "nomad.job.purge"},
			},
			Detail: plugin.DetailView{
				Header: plugin.HeaderSpec{Title: "${resource.name}", StatusField: "status", Severities: jobSeverities()}, DefaultTab: "overview",
				Tabs: []plugin.Panel{
					{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
						Source: &plugin.DataSource{RouteID: "nomad.job.overview", Params: jobParams},
						Config: objectDetail(jobSections(), &plugin.DataSource{RouteID: "nomad.job.watch", Method: plugin.MethodWS, Params: jobParams})},
					{Key: "groups", Label: "Task groups", Icon: icon("layers"), Type: plugin.PanelTable,
						Source: &plugin.DataSource{RouteID: "nomad.job.groups", Params: jobParams},
						Config: plugin.TableConfig{Columns: taskGroupColumns(), RowActionIDs: []string{"nomad.job.scale"}, DefaultSort: &plugin.SortKey{Field: "name"}, EmptyText: "This job declares no task groups.", RefreshIntervalMs: 15000, Exportable: true}},
					allocTable(jobParams),
					{Key: "deployments", Label: "Deployments", Icon: icon("rocket"), Type: plugin.PanelTable,
						Source: &plugin.DataSource{RouteID: "nomad.deployments.list", Params: jobParams},
						Config: plugin.TableConfig{Columns: deploymentColumns(), DefaultSort: &plugin.SortKey{Field: "createTime", Desc: true}, EmptyText: "This job has never rolled out.", RefreshIntervalMs: 10000, Exportable: true}},
					{Key: "evaluations", Label: "Evaluations", Icon: icon("list-checks"), Type: plugin.PanelTable,
						Source: &plugin.DataSource{RouteID: "nomad.evals.list", Params: jobParams},
						Config: plugin.TableConfig{Columns: evalColumns(), DefaultSort: &plugin.SortKey{Field: "createTime", Desc: true}, EmptyText: "No evaluations for this job.", RefreshIntervalMs: 10000, Exportable: true}},
					{Key: "versions", Label: "Versions", Icon: icon("history"), Type: plugin.PanelTable,
						Source: &plugin.DataSource{RouteID: "nomad.job.versions", Params: jobParams},
						Config: plugin.TableConfig{Columns: jobVersionColumns(), RowActionIDs: []string{"nomad.job.revert"}, DefaultSort: &plugin.SortKey{Field: "version", Desc: true}, EmptyText: "Only one version has been submitted.", RefreshIntervalMs: 30000, Exportable: true}},
					{Key: "spec", Label: "Specification", Icon: icon("code"), Type: plugin.PanelCodeEditor,
						Source: &plugin.DataSource{RouteID: "nomad.job.spec", Params: jobParams},
						Config: plugin.CodeEditorConfig{
							Language: "hcl", SaveRouteID: "nomad.job.spec.save", SaveMethod: plugin.MethodPut, SaveParams: jobParams,
							RefreshField: "content",
							SaveToast:    &plugin.SaveToast{Summary: "Job updated", Detail: "Evaluation ${response.evalId} created", Severity: plugin.SeveritySuccess},
						}},
				},
			},
		},
		{
			Kind: "allocation", Title: "Allocations", List: plugin.DataSource{RouteID: "nomad.allocs.list"}, Watch: resourceWatch("allocation"), Columns: allocColumns(),
			Actions: plugin.ResourceActions{
				Row:    []string{"nomad.alloc.stop"},
				Detail: []string{"nomad.alloc.restart", "nomad.alloc.signal", "nomad.alloc.stop"},
			},
			Detail: plugin.DetailView{
				Header: plugin.HeaderSpec{Title: "${resource.name}", StatusField: "clientStatus", Severities: allocSeverities()}, DefaultTab: "alloc-overview",
				Tabs: []plugin.Panel{
					{Key: "alloc-overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
						Source: &plugin.DataSource{RouteID: "nomad.alloc.overview", Params: allocParams},
						Config: objectDetail(allocSections(), &plugin.DataSource{RouteID: "nomad.alloc.watch", Method: plugin.MethodWS, Params: allocParams})},
					{Key: "alloc-events", Label: "Task events", Icon: icon("history"), Type: plugin.PanelTimeline,
						Source: &plugin.DataSource{RouteID: "nomad.alloc.events", Params: allocParams},
						Config: plugin.TimelineConfig{
							TimestampField: "time", TitleField: "reason", BodyField: "message", SeverityField: "severity",
							IconField: "icon", ResourceField: "resource", EmptyText: "No task events recorded yet.", RefreshIntervalMs: 10000,
						}},
					{Key: "alloc-logs", Label: "Logs", Icon: icon("scroll-text"), Type: plugin.PanelLogStream,
						Source: &plugin.DataSource{RouteID: "nomad.alloc.logs", Method: plugin.MethodWS, Params: allocParams},
						Config: plugin.LogStreamConfig{Controls: []plugin.StreamControl{
							{Param: "task", Label: "Task", OptionsSource: &plugin.DataSource{RouteID: "nomad.alloc.tasks", Params: allocParams}},
							{Param: "type", Label: "Stream", OptionsSource: &plugin.DataSource{RouteID: "nomad.log.types"}},
						}}},
					{Key: "alloc-exec", Label: "Exec", Icon: icon("terminal"), Type: plugin.PanelTerminal,
						Source: &plugin.DataSource{RouteID: "nomad.alloc.exec", Method: plugin.MethodWS, Params: allocParams},
						Config: plugin.TerminalConfig{Zoom: true, Search: true, Controls: []plugin.StreamControl{
							{Param: "task", Label: "Task", OptionsSource: &plugin.DataSource{RouteID: "nomad.alloc.tasks", Params: allocParams}},
						}},
						VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "clientStatus", Op: plugin.OpEq, Value: "running"}}}},
					{Key: "alloc-metrics", Label: "Metrics", Icon: icon("activity"), Type: plugin.PanelMetrics,
						Source: &plugin.DataSource{RouteID: "nomad.alloc.metrics", Method: plugin.MethodWS, Params: allocParams},
						Config: plugin.MetricsConfig{
							Stats: []plugin.MetricStat{
								{Key: "cpuTicks", Label: "CPU", Unit: "MHz"},
								{Key: "memoryUsed", Label: "Memory", Unit: "bytes"},
								{Key: "tasks", Label: "Tasks"},
							},
							Usage: []plugin.MetricUsage{
								{Key: "cpuPercent", Label: "CPU usage", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
									PercentKey: "cpuPercent", UsedKey: "cpuUsed", TotalKey: "cpuTotal",
									UsedType: plugin.ColumnNumber, TotalType: plugin.ColumnNumber, TotalLabel: "of", Unit: "MHz", WarnAt: 75, CriticalAt: 90,
								}},
								{Key: "memoryPercent", Label: "Memory usage", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
									PercentKey: "memoryPercent", UsedKey: "memoryUsed", TotalKey: "memoryTotal",
									UsedType: plugin.ColumnBytes, TotalType: plugin.ColumnBytes, WarnAt: 80, CriticalAt: 95,
								}},
							},
							Series:  []plugin.MetricSeries{{Key: "cpuPercent", Label: "CPU", Unit: "%"}, {Key: "memoryPercent", Label: "Memory", Unit: "%"}},
							History: 60,
						},
						VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "clientStatus", Op: plugin.OpEq, Value: "running"}}}},
				},
			},
		},
		{
			Kind: "node", Title: "Clients", List: plugin.DataSource{RouteID: "nomad.nodes.list"}, Watch: resourceWatch("node"), Columns: nodeColumns(),
			Actions: plugin.ResourceActions{
				Detail: []string{"nomad.node.drain", "nomad.node.drain.cancel", "nomad.node.eligibility"},
			},
			Detail: plugin.DetailView{
				Header: plugin.HeaderSpec{Title: "${resource.name}", StatusField: "status", Severities: nodeSeverities()}, DefaultTab: "node-overview",
				Tabs: []plugin.Panel{
					{Key: "node-overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
						Source: &plugin.DataSource{RouteID: "nomad.node.overview", Params: nodeParams},
						Config: objectDetail(nodeSections(), &plugin.DataSource{RouteID: "nomad.node.watch", Method: plugin.MethodWS, Params: nodeParams})},
					allocTable(nodeParams),
					{Key: "node-metrics", Label: "Metrics", Icon: icon("activity"), Type: plugin.PanelMetrics,
						Source: &plugin.DataSource{RouteID: "nomad.node.metrics", Method: plugin.MethodWS, Params: nodeParams},
						Config: plugin.MetricsConfig{
							Stats: []plugin.MetricStat{
								{Key: "cpuTicks", Label: "CPU", Unit: "MHz"},
								{Key: "memoryUsed", Label: "Memory", Unit: "bytes"},
								{Key: "uptime", Label: "Uptime", Unit: "s"},
							},
							Usage: []plugin.MetricUsage{
								{Key: "cpuPercent", Label: "CPU usage", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
									PercentKey: "cpuPercent", UsedKey: "cpuUsed", TotalKey: "cpuTotal",
									UsedType: plugin.ColumnNumber, TotalType: plugin.ColumnNumber, TotalLabel: "of", Unit: "MHz", WarnAt: 75, CriticalAt: 90,
								}},
								{Key: "memoryPercent", Label: "Memory usage", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
									PercentKey: "memoryPercent", UsedKey: "memoryUsed", TotalKey: "memoryTotal",
									UsedType: plugin.ColumnBytes, TotalType: plugin.ColumnBytes, WarnAt: 80, CriticalAt: 95,
								}},
								{Key: "diskPercent", Label: "Alloc disk usage", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
									PercentKey: "diskPercent", UsedKey: "diskUsed", TotalKey: "diskTotal",
									UsedType: plugin.ColumnBytes, TotalType: plugin.ColumnBytes, WarnAt: 80, CriticalAt: 95,
								}},
							},
							Series:  []plugin.MetricSeries{{Key: "cpuPercent", Label: "CPU", Unit: "%"}, {Key: "memoryPercent", Label: "Memory", Unit: "%"}},
							History: 60,
						}},
					{Key: "node-volumes", Label: "Host volumes", Icon: icon("folder-tree"), Type: plugin.PanelTable,
						Source: &plugin.DataSource{RouteID: "nomad.hostvolumes.list", Params: nodeParams},
						Config: plugin.TableConfig{Columns: hostVolumeColumns(), DefaultSort: &plugin.SortKey{Field: "name"}, EmptyText: "No dynamic host volumes on this client.", RefreshIntervalMs: 30000, Exportable: true}},
				},
			},
		},
		{
			Kind: "deployment", Title: "Deployments", List: plugin.DataSource{RouteID: "nomad.deployments.list"}, Watch: resourceWatch("deployment"), Columns: deploymentColumns(),
			Actions: plugin.ResourceActions{
				Detail: []string{"nomad.deployment.promote", "nomad.deployment.pause", "nomad.deployment.unblock", "nomad.deployment.fail"},
			},
			Detail: plugin.DetailView{
				Header: plugin.HeaderSpec{Title: "${resource.name}", StatusField: "status", Severities: deploymentSeverities()}, DefaultTab: "rollout",
				Tabs: []plugin.Panel{
					{Key: "rollout", Label: "Rollout", Icon: icon("rocket"), Type: plugin.PanelTaskProgress,
						Source: &plugin.DataSource{RouteID: "nomad.deployment.progress", Method: plugin.MethodWS, Params: deploymentParams},
						Config: plugin.TaskProgressConfig{Title: "Deployment rollout", CancelRouteID: "nomad.deployment.fail"}},
					{Key: "deployment-overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
						Source: &plugin.DataSource{RouteID: "nomad.deployment.overview", Params: deploymentParams},
						Config: objectDetail(deploymentSections(), nil)},
					allocTable(deploymentParams),
				},
			},
		},
		{
			Kind: "evaluation", Title: "Evaluations", List: plugin.DataSource{RouteID: "nomad.evals.list"}, Watch: resourceWatch("evaluation"), Columns: evalColumns(),
			Detail: plugin.DetailView{
				Header: plugin.HeaderSpec{Title: "${resource.name}", StatusField: "status", Severities: evalSeverities()}, DefaultTab: "eval-overview",
				Tabs: []plugin.Panel{
					{Key: "eval-overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
						Source: &plugin.DataSource{RouteID: "nomad.eval.overview", Params: map[string]string{"ns": "${resource.namespace}", "eval": "${resource.uid}"}},
						Config: objectDetail(evalSections(), nil)},
					allocTable(map[string]string{"ns": "${resource.namespace}", "eval": "${resource.uid}"}),
				},
			},
		},
		{
			Kind: "volume", Title: "CSI volumes", List: plugin.DataSource{RouteID: "nomad.volumes.list"}, Columns: volumeColumns(),
			Detail: plugin.DetailView{
				Header: plugin.HeaderSpec{Title: "${resource.name}"}, DefaultTab: "volume-overview",
				Tabs: []plugin.Panel{
					{Key: "volume-overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
						Source: &plugin.DataSource{RouteID: "nomad.volume.overview", Params: map[string]string{"ns": "${resource.namespace}", "volume": "${resource.uid}"}},
						Config: objectDetail(volumeSections(), nil)},
				},
			},
		},
		{
			Kind: "host_volume", Title: "Host volumes", List: plugin.DataSource{RouteID: "nomad.hostvolumes.list"}, Columns: hostVolumeColumns(),
			Detail: plugin.DetailView{
				Header: plugin.HeaderSpec{Title: "${resource.name}", StatusField: "state", Severities: volumeSeverities()}, DefaultTab: "host-volume-overview",
				Tabs: []plugin.Panel{
					{Key: "host-volume-overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
						Source: &plugin.DataSource{RouteID: "nomad.hostvolume.overview", Params: map[string]string{"ns": "${resource.namespace}", "volume": "${resource.uid}"}},
						Config: objectDetail(hostVolumeSections(), nil)},
				},
			},
		},
	}
}
