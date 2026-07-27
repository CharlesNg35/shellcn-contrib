package consul

import "github.com/charlesng35/shellcn/sdk/plugin"

func icon(name string) plugin.Icon { return plugin.Icon{Type: plugin.IconLucide, Value: name} }

func rid(suffix string) string { return protocolName + "." + suffix }

var (
	healthSeverities = map[string]plugin.Severity{
		"passing":     plugin.SeveritySuccess,
		"warning":     plugin.SeverityWarn,
		"critical":    plugin.SeverityDanger,
		"maintenance": plugin.SeveritySecondary,
		"unknown":     plugin.SeveritySecondary,
	}
	memberSeverities = map[string]plugin.Severity{
		"alive":   plugin.SeveritySuccess,
		"leaving": plugin.SeverityWarn,
		"left":    plugin.SeveritySecondary,
		"failed":  plugin.SeverityDanger,
		"none":    plugin.SeveritySecondary,
	}
	intentionSeverities = map[string]plugin.Severity{
		"allow": plugin.SeveritySuccess,
		"deny":  plugin.SeverityDanger,
	}
	clusterSeverities = map[string]plugin.Severity{
		"ready":     plugin.SeveritySuccess,
		"degraded":  plugin.SeverityWarn,
		"no-leader": plugin.SeverityDanger,
	}
)

func scopeFilters() []plugin.ScopeFilter {
	return []plugin.ScopeFilter{{
		Param:         "datacenter",
		Label:         "Datacenter",
		Icon:          icon("globe"),
		Control:       plugin.ScopeSelect,
		OptionsSource: &plugin.DataSource{RouteID: rid("datacenters.list")},
		ValueField:    "value",
		LabelField:    "label",
		AllLabel:      "Connection datacenter",
	}}
}

func streams() []plugin.Stream {
	return []plugin.Stream{{ID: rid("kv.watch"), Kind: plugin.StreamLogs, RouteID: rid("kv.watch")}}
}

func tree() []plugin.TreeGroup {
	clusterRef := plugin.ResourceIdentity{Kind: kindCluster, Name: "Cluster", UID: kindCluster}
	return []plugin.TreeGroup{
		{Key: "cluster", Label: "Cluster", Icon: icon("server-cog"), Ref: &clusterRef},
		{Key: "kv", Label: "Key/value", Icon: icon("folder-tree"), Source: plugin.DataSource{RouteID: rid("kv.tree")}},
		{Key: "catalog", Label: "Catalog", Icon: icon("boxes"), Source: plugin.DataSource{RouteID: rid("catalog.tree")}},
		{Key: "mesh", Label: "Mesh & coordination", Icon: icon("share-2"), Source: plugin.DataSource{RouteID: rid("mesh.tree")}},
		{Key: "acl", Label: "Access control", Icon: icon("shield"), Source: plugin.DataSource{RouteID: rid("acl.tree")}},
	}
}

func kvParams() map[string]string      { return map[string]string{"prefix": "${resource.uid}"} }
func serviceParams() map[string]string { return map[string]string{"service": "${resource.uid}"} }
func nodeParams() map[string]string    { return map[string]string{"node": "${resource.uid}"} }
func checkParams() map[string]string {
	return map[string]string{"node": "${resource.namespace}", "check": "${resource.name}"}
}
func instanceParams() map[string]string {
	return map[string]string{"service": "${resource.scope}", "node": "${resource.namespace}", "instance": "${resource.name}"}
}
func sessionParams() map[string]string { return map[string]string{"session": "${resource.uid}"} }
func queryIDParams() map[string]string { return map[string]string{"query": "${resource.uid}"} }
func intentionParams() map[string]string {
	return map[string]string{"source": "${resource.scope}", "destination": "${resource.name}"}
}
func tokenParams() map[string]string  { return map[string]string{"token": "${resource.uid}"} }
func policyParams() map[string]string { return map[string]string{"policy": "${resource.uid}"} }
func roleParams() map[string]string   { return map[string]string{"role": "${resource.uid}"} }

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		clusterResource(),
		kvResource(),
		serviceResource(),
		instanceResource(),
		nodeResource(),
		checkResource(),
		sessionResource(),
		preparedQueryResource(),
		intentionResource(),
		aclTokenResource(),
		aclPolicyResource(),
		aclRoleResource(),
	}
}

func clusterResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: kindCluster, Title: "Cluster",
		List:    plugin.DataSource{RouteID: rid("cluster.list")},
		Columns: clusterColumns(),
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.name}", StatusField: "status", Severities: clusterSeverities},
			DefaultTab: "agent",
			Tabs: []plugin.Panel{
				{Key: "agent", Label: "Agent", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("agent.self")}, Config: agentDetailConfig()},
				{Key: "config", Label: "Configuration", Icon: icon("settings"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("agent.config")}, Config: agentConfigTableConfig()},
				{Key: "members", Label: "Members", Icon: icon("users"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("members.list")}, Config: memberTableConfig()},
				{Key: "raft", Label: "Raft peers", Icon: icon("network"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("raft.list")}, Config: raftTableConfig()},
				{Key: "datacenters", Label: "Datacenters", Icon: icon("globe"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("datacenters.list")}, Config: datacenterTableConfig()},
			},
		},
	}
}

func kvResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: kindKV, Title: "Key/value",
		List:    plugin.DataSource{RouteID: rid("kv.entries")},
		Columns: kvColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{rid("kv.put")},
			Detail:  []string{rid("kv.put"), rid("kv.prefix.delete")},
		},
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.name}"},
			DefaultTab: "browse",
			Tabs: []plugin.Panel{
				{Key: "browse", Label: "Keys", Icon: icon("key-round"), Type: plugin.PanelKV,
					Source: &plugin.DataSource{RouteID: rid("kv.list"), Params: kvParams()}, Config: kvPanelConfig()},
				{Key: "entries", Label: "Entries", Icon: icon("table"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("kv.entries"), Params: kvParams()}, Config: kvTableConfig()},
				{Key: "changes", Label: "Changes", Icon: icon("activity"), Type: plugin.PanelLogStream,
					Source: &plugin.DataSource{RouteID: rid("kv.watch"), Method: plugin.MethodWS, Params: kvParams()}},
			},
		},
	}
}

func serviceResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: kindService, Title: "Services",
		List:    plugin.DataSource{RouteID: rid("services.list")},
		Columns: serviceColumns(),
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.name}", StatusField: "status", Severities: healthSeverities},
			DefaultTab: "overview",
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("service.read"), Params: serviceParams()}, Config: serviceDetailConfig()},
				{Key: "instances", Label: "Instances", Icon: icon("boxes"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("service.instances"), Params: serviceParams()}, Config: instanceTableConfig()},
				{Key: "service-checks", Label: "Health checks", Icon: icon("heart-pulse"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("service.checks"), Params: serviceParams()}, Config: checkTableConfig()},
				{Key: "service-intentions", Label: "Intentions", Icon: icon("shield-check"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("intentions.list"), Params: map[string]string{"destination": "${resource.uid}"}},
					Config: intentionTableConfig()},
			},
		},
	}
}

func instanceResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: kindInstance, Title: "Service instances",
		List:    plugin.DataSource{RouteID: rid("service.instances")},
		Columns: instanceColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{rid("instance.deregister")},
			Detail: []string{rid("instance.deregister")},
		},
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.name}", StatusField: "status", Severities: healthSeverities},
			DefaultTab: "instance-overview",
			Tabs: []plugin.Panel{
				{Key: "instance-overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("instance.read"), Params: instanceParams()}, Config: instanceDetailConfig()},
			},
		},
	}
}

func nodeResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: kindNode, Title: "Nodes",
		List:    plugin.DataSource{RouteID: rid("nodes.list")},
		Columns: nodeColumns(),
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.name}", StatusField: "status", Severities: healthSeverities},
			DefaultTab: "node-overview",
			Tabs: []plugin.Panel{
				{Key: "node-overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("node.read"), Params: nodeParams()}, Config: nodeDetailConfig()},
				{Key: "node-services", Label: "Services", Icon: icon("boxes"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("node.services"), Params: nodeParams()}, Config: nodeServiceTableConfig()},
				{Key: "node-checks", Label: "Health checks", Icon: icon("heart-pulse"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("node.checks"), Params: nodeParams()}, Config: checkTableConfig()},
			},
		},
	}
}

func checkResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: kindCheck, Title: "Health checks",
		List:    plugin.DataSource{RouteID: rid("checks.list")},
		Columns: checkColumns(),
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.name}", StatusField: "status", Severities: healthSeverities},
			DefaultTab: "check-overview",
			Tabs: []plugin.Panel{
				{Key: "check-overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("check.read"), Params: checkParams()}, Config: checkDetailConfig()},
			},
		},
	}
}

func sessionResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: kindSession, Title: "Sessions",
		List:    plugin.DataSource{RouteID: rid("sessions.list")},
		Columns: sessionColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{rid("session.destroy")},
			Detail: []string{rid("session.destroy")},
		},
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.name}"},
			DefaultTab: "session-overview",
			Tabs: []plugin.Panel{
				{Key: "session-overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("session.read"), Params: sessionParams()}, Config: sessionDetailConfig()},
			},
		},
	}
}

func preparedQueryResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: kindPreparedQuery, Title: "Prepared queries",
		List:    plugin.DataSource{RouteID: rid("queries.list")},
		Columns: preparedQueryColumns(),
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.name}"},
			DefaultTab: "query-overview",
			Tabs: []plugin.Panel{
				{Key: "query-overview", Label: "Definition", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("query.read"), Params: queryIDParams()}, Config: preparedQueryDetailConfig()},
				{Key: "query-results", Label: "Results", Icon: icon("play"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("query.execute"), Params: queryIDParams()}, Config: queryResultTableConfig()},
			},
		},
	}
}

func intentionResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: kindIntention, Title: "Intentions",
		List:    plugin.DataSource{RouteID: rid("intentions.list")},
		Columns: intentionColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{rid("intention.delete")},
			Detail: []string{rid("intention.toggle"), rid("intention.delete")},
		},
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.name}", StatusField: "action", Severities: intentionSeverities},
			DefaultTab: "intention-overview",
			Tabs: []plugin.Panel{
				{Key: "intention-overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("intention.read"), Params: intentionParams()}, Config: intentionDetailConfig()},
			},
		},
	}
}

func aclTokenResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: kindACLToken, Title: "ACL tokens",
		List:    plugin.DataSource{RouteID: rid("acl.tokens.list")},
		Columns: aclTokenColumns(),
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.name}"},
			DefaultTab: "token-overview",
			Tabs: []plugin.Panel{
				{Key: "token-overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("acl.token.read"), Params: tokenParams()}, Config: aclTokenDetailConfig()},
			},
		},
	}
}

func aclPolicyResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: kindACLPolicy, Title: "ACL policies",
		List:    plugin.DataSource{RouteID: rid("acl.policies.list")},
		Columns: aclPolicyColumns(),
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.name}"},
			DefaultTab: "policy-overview",
			Tabs: []plugin.Panel{
				{Key: "policy-overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("acl.policy.read"), Params: policyParams()}, Config: aclPolicyDetailConfig()},
			},
		},
	}
}

func aclRoleResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: kindACLRole, Title: "ACL roles",
		List:    plugin.DataSource{RouteID: rid("acl.roles.list")},
		Columns: aclRoleColumns(),
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.name}"},
			DefaultTab: "role-overview",
			Tabs: []plugin.Panel{
				{Key: "role-overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("acl.role.read"), Params: roleParams()}, Config: aclRoleDetailConfig()},
			},
		},
	}
}

func actions() []plugin.Action {
	return []plugin.Action{
		{ID: rid("kv.put"), Label: "Set key", Icon: icon("file-plus"), RouteID: rid("kv.put"),
			OnSuccess: &plugin.ActionSuccess{SelectTab: "entries"}},
		{ID: rid("kv.entry.delete"), Label: "Delete", Icon: icon("trash-2"), RouteID: rid("kv.delete"),
			Params: map[string]string{"key": "${record.key}"}, Confirm: true, Bulk: true,
			ConfirmText: "Delete the selected key(s)? The stored value is removed from every Consul server."},
		{ID: rid("kv.prefix.delete"), Label: "Delete folder", Icon: icon("folder-x"), RouteID: rid("kv.tree.delete"),
			Params: kvParams(), Confirm: true,
			ConfirmText: "Recursively delete every key under this prefix? The values cannot be recovered.",
			OnSuccess:   &plugin.ActionSuccess{Navigate: plugin.NavigateList}},
		{ID: rid("instance.deregister"), Label: "Deregister", Icon: icon("power-off"), RouteID: rid("instance.deregister"),
			Params: instanceParams(), Confirm: true, Bulk: true,
			ConfirmText: "Deregister the selected instance(s) from the catalog? The node must re-register to appear again.",
			OnSuccess:   &plugin.ActionSuccess{Navigate: plugin.NavigateList}},
		{ID: rid("session.destroy"), Label: "Destroy", Icon: icon("trash-2"), RouteID: rid("session.destroy"),
			Params: sessionParams(), Confirm: true, Bulk: true,
			ConfirmText: "Destroy the selected session(s)? Locks held by them are released or their keys deleted.",
			OnSuccess:   &plugin.ActionSuccess{Navigate: plugin.NavigateList}},
		{ID: rid("intention.toggle"), Label: "Toggle allow/deny", Icon: icon("toggle-right"), RouteID: rid("intention.toggle"),
			Params: intentionParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "intention-overview"}},
		{ID: rid("intention.delete"), Label: "Delete", Icon: icon("trash-2"), RouteID: rid("intention.delete"),
			Params: intentionParams(), Confirm: true, Bulk: true,
			ConfirmText: "Delete the selected intention(s)? Traffic then falls back to the default mesh policy.",
			OnSuccess:   &plugin.ActionSuccess{Navigate: plugin.NavigateList}},
	}
}

func clusterColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Cluster", Sortable: true},
		{Key: "datacenter", Label: "Datacenter", Sortable: true},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Sortable: true, Severities: clusterSeverities},
		{Key: "leader", Label: "Leader"},
		{Key: "version", Label: "Version", Sortable: true},
		{Key: "nodes", Label: "Nodes", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "services", Label: "Services", Type: plugin.ColumnNumber, Sortable: true},
	}
}

func kvColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "key", Label: "Key", Sortable: true},
		{Key: "size", Label: "Size", Type: plugin.ColumnBytes},
		{Key: "flags", Label: "Flags", Type: plugin.ColumnNumber},
		{Key: "session", Label: "Lock session"},
		{Key: "modify_index", Label: "Modify index", Type: plugin.ColumnNumber},
	}
}

func serviceColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Service", Sortable: true},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Sortable: true, Severities: healthSeverities},
		{Key: "tags", Label: "Tags"},
		{Key: "passing", Label: "Passing", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "warning", Label: "Warning", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "critical", Label: "Critical", Type: plugin.ColumnNumber, Sortable: true},
	}
}

func instanceColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Instance", Sortable: true},
		{Key: "node", Label: "Node", Sortable: true},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Sortable: true, Severities: healthSeverities},
		{Key: "address", Label: "Address"},
		{Key: "port", Label: "Port", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "tags", Label: "Tags"},
		{Key: "kind", Label: "Kind", Type: plugin.ColumnBadge, Sortable: true},
	}
}

func nodeColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Node", Sortable: true},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Sortable: true, Severities: healthSeverities},
		{Key: "address", Label: "Address", Sortable: true},
		{Key: "datacenter", Label: "Datacenter", Sortable: true},
		{Key: "id", Label: "Node ID"},
		{Key: "meta", Label: "Meta", Type: plugin.ColumnJSON},
	}
}

func checkColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Check", Sortable: true},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Sortable: true, Severities: healthSeverities},
		{Key: "node", Label: "Node", Sortable: true},
		{Key: "service", Label: "Service", Sortable: true},
		{Key: "type", Label: "Type", Type: plugin.ColumnBadge, Sortable: true},
		{Key: "output", Label: "Output"},
	}
}

func nodeServiceColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Instance", Sortable: true},
		{Key: "service", Label: "Service", Sortable: true},
		{Key: "address", Label: "Address"},
		{Key: "port", Label: "Port", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "tags", Label: "Tags"},
		{Key: "kind", Label: "Kind", Type: plugin.ColumnBadge, Sortable: true},
	}
}

func sessionColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Session", Sortable: true},
		{Key: "id", Label: "ID"},
		{Key: "node", Label: "Node", Sortable: true},
		{Key: "behavior", Label: "Behavior", Type: plugin.ColumnBadge, Sortable: true},
		{Key: "ttl", Label: "TTL", Sortable: true},
		{Key: "lock_delay", Label: "Lock delay", Type: plugin.ColumnDuration, Sortable: true},
		{Key: "checks", Label: "Checks", Type: plugin.ColumnNumber, Sortable: true},
	}
}

func preparedQueryColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Query", Sortable: true},
		{Key: "id", Label: "ID"},
		{Key: "service", Label: "Service", Sortable: true},
		{Key: "only_passing", Label: "Only passing", Type: plugin.ColumnBool},
		{Key: "near", Label: "Near"},
		{Key: "session", Label: "Session"},
	}
}

func queryResultColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "node", Label: "Node", Sortable: true},
		{Key: "address", Label: "Address"},
		{Key: "port", Label: "Port", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Sortable: true, Severities: healthSeverities},
		{Key: "tags", Label: "Tags"},
	}
}

func intentionColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "source", Label: "Source", Sortable: true},
		{Key: "destination", Label: "Destination", Sortable: true},
		{Key: "action", Label: "Action", Type: plugin.ColumnBadge, Sortable: true, Severities: intentionSeverities},
		{Key: "precedence", Label: "Precedence", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "permissions", Label: "L7 rules", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "updated_at", Label: "Updated", Type: plugin.ColumnRelativeTime, Sortable: true},
	}
}

func aclTokenColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "description", Label: "Description", Sortable: true},
		{Key: "accessor_id", Label: "Accessor ID"},
		{Key: "policies", Label: "Policies"},
		{Key: "roles", Label: "Roles"},
		{Key: "local", Label: "Local", Type: plugin.ColumnBool, Sortable: true},
		{Key: "created_at", Label: "Created", Type: plugin.ColumnRelativeTime, Sortable: true},
		{Key: "expires_at", Label: "Expires", Type: plugin.ColumnRelativeTime, Sortable: true},
	}
}

func aclPolicyColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Policy", Sortable: true},
		{Key: "id", Label: "ID"},
		{Key: "description", Label: "Description"},
		{Key: "datacenters", Label: "Datacenters"},
	}
}

func aclRoleColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Role", Sortable: true},
		{Key: "id", Label: "ID"},
		{Key: "description", Label: "Description"},
		{Key: "policies", Label: "Policies"},
	}
}

func memberColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Member", Sortable: true},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Sortable: true, Severities: memberSeverities},
		{Key: "address", Label: "Address"},
		{Key: "role", Label: "Role", Type: plugin.ColumnBadge, Sortable: true},
		{Key: "datacenter", Label: "Datacenter", Sortable: true},
		{Key: "build", Label: "Build", Sortable: true},
	}
}

func raftColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "node", Label: "Node", Sortable: true},
		{Key: "address", Label: "Address"},
		{Key: "leader", Label: "Leader", Type: plugin.ColumnBool, Sortable: true},
		{Key: "voter", Label: "Voter", Type: plugin.ColumnBool, Sortable: true},
		{Key: "last_index", Label: "Last index", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "protocol", Label: "Raft protocol"},
	}
}

func agentConfigColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "setting", Label: "Setting", Sortable: true},
		{Key: "value", Label: "Value"},
	}
}

func datacenterColumns() []plugin.Column {
	return []plugin.Column{{Key: "label", Label: "Datacenter", Sortable: true}}
}

func kvPanelConfig() plugin.KVConfig {
	return plugin.KVConfig{
		CreateRouteID: rid("kv.write"),
		ReadRouteID:   rid("kv.read"),
		WriteRouteID:  rid("kv.write"),
		DeleteRouteID: rid("kv.delete"),
		KeyParam:      "key",
		Writable:      true,
		Delimiter:     "/",
		ValueTypes:    []string{"string", "json", "binary"},
	}
}

func kvTableConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Columns:           kvColumns(),
		ActionIDs:         []string{rid("kv.put")},
		RowActionIDs:      []string{rid("kv.entry.delete")},
		DefaultSort:       &plugin.SortKey{Field: "key"},
		RefreshIntervalMs: 20000,
		EmptyText:         "No keys directly under this prefix. Open a folder in the sidebar for nested keys, or use Set key to create one.",
		Exportable:        true,
	}
}

func memberTableConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Columns:           memberColumns(),
		DefaultSort:       &plugin.SortKey{Field: "name"},
		RefreshIntervalMs: 15000,
		EmptyText:         "The agent reported no gossip members.",
		Exportable:        true,
	}
}

func raftTableConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Columns:           raftColumns(),
		DefaultSort:       &plugin.SortKey{Field: "node"},
		RefreshIntervalMs: 30000,
		EmptyText:         "Raft configuration is unavailable; the token needs operator:read.",
		Exportable:        true,
	}
}

func agentConfigTableConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Columns:     agentConfigColumns(),
		DefaultSort: &plugin.SortKey{Field: "setting"},
		EmptyText:   "The agent reported no runtime configuration; the token needs agent:read.",
		Exportable:  true,
	}
}

func datacenterTableConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Columns:           datacenterColumns(),
		DefaultSort:       &plugin.SortKey{Field: "label"},
		RefreshIntervalMs: 60000,
		EmptyText:         "No federated datacenters were reported.",
	}
}

func instanceTableConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Columns:           instanceColumns(),
		RowActionIDs:      []string{rid("instance.deregister")},
		DefaultSort:       &plugin.SortKey{Field: "name"},
		RefreshIntervalMs: 15000,
		EmptyText:         "No instances of this service are registered in the selected datacenter.",
		Exportable:        true,
	}
}

// checkTableConfig sorts ascending by status so critical checks open at the top;
// Consul's status values happen to be alphabetical in severity order.
func checkTableConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Columns:           checkColumns(),
		DefaultSort:       &plugin.SortKey{Field: "status"},
		RefreshIntervalMs: 10000,
		EmptyText:         "No health checks match this scope.",
		Exportable:        true,
	}
}

func nodeServiceTableConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Columns:           nodeServiceColumns(),
		DefaultSort:       &plugin.SortKey{Field: "name"},
		RefreshIntervalMs: 15000,
		EmptyText:         "This node has no registered services.",
		Exportable:        true,
	}
}

func intentionTableConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Columns:           intentionColumns(),
		RowActionIDs:      []string{rid("intention.delete")},
		DefaultSort:       &plugin.SortKey{Field: "precedence", Desc: true},
		RefreshIntervalMs: 30000,
		EmptyText:         "No intentions are defined; mesh traffic follows the default policy.",
		Exportable:        true,
	}
}

func queryResultTableConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Columns:           queryResultColumns(),
		DefaultSort:       &plugin.SortKey{Field: "node"},
		RefreshIntervalMs: 30000,
		EmptyText:         "Executing this prepared query returned no service instances.",
		Exportable:        true,
	}
}

func agentDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Sections: []plugin.ObjectDetailSection{
			{Title: "Agent", Fields: []plugin.ObjectDetailField{
				{Key: "node_name", Label: "Node name", Copy: true},
				{Key: "node_id", Label: "Node ID", Copy: true},
				{Key: "datacenter", Label: "Datacenter", Copy: true},
				{Key: "primary_datacenter", Label: "Primary datacenter"},
				{Key: "server", Label: "Server", Type: plugin.ColumnBool},
				{Key: "version", Label: "Version", Copy: true},
				{Key: "revision", Label: "Revision"},
				{Key: "endpoint", Label: "Endpoint", Copy: true},
			}},
			{Title: "Networking", Fields: []plugin.ObjectDetailField{
				{Key: "advertise_addr", Label: "Advertise address", Copy: true},
				{Key: "bind_addr", Label: "Bind address"},
				{Key: "tagged_addresses", Label: "Tagged addresses", Type: plugin.ColumnJSON},
				{Key: "gossip_tags", Label: "Gossip tags", Type: plugin.ColumnJSON},
				{Key: "meta", Label: "Node meta", Type: plugin.ColumnJSON},
			}},
			{Title: "Connection", Fields: []plugin.ObjectDetailField{
				{Key: "read_only", Label: "Read-only connection", Type: plugin.ColumnBool},
				{Key: "allow_stale", Label: "Stale reads allowed", Type: plugin.ColumnBool},
				{Key: "namespace", Label: "Namespace"},
				{Key: "partition", Label: "Admin partition"},
				{Key: "kv_prefix", Label: "KV root prefix", Copy: true},
			}},
		},
	}
}

func serviceDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Sections: []plugin.ObjectDetailSection{
			{Title: "Service", Fields: []plugin.ObjectDetailField{
				{Key: "name", Label: "Name", Copy: true},
				{Key: "status", Label: "Aggregate status", Type: plugin.ColumnBadge, Severities: healthSeverities},
				{Key: "instances", Label: "Instances", Type: plugin.ColumnNumber},
				{Key: "nodes", Label: "Nodes", Type: plugin.ColumnNumber},
				{Key: "kinds", Label: "Kinds"},
			}},
			{Title: "Health", Fields: []plugin.ObjectDetailField{
				{Key: "passing", Label: "Passing checks", Type: plugin.ColumnNumber},
				{Key: "warning", Label: "Warning checks", Type: plugin.ColumnNumber},
				{Key: "critical", Label: "Critical checks", Type: plugin.ColumnNumber},
			}},
			{Title: "Registration", Fields: []plugin.ObjectDetailField{
				{Key: "tags", Label: "Tags"},
				{Key: "ports", Label: "Ports"},
				{Key: "datacenter", Label: "Datacenter"},
				{Key: "meta", Label: "Service meta", Type: plugin.ColumnJSON},
			}},
		},
	}
}

func instanceDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Sections: []plugin.ObjectDetailSection{
			{Title: "Instance", Fields: []plugin.ObjectDetailField{
				{Key: "name", Label: "Instance ID", Copy: true},
				{Key: "service", Label: "Service", Copy: true},
				{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: healthSeverities},
				{Key: "kind", Label: "Kind", Type: plugin.ColumnBadge},
				{Key: "node", Label: "Node", Copy: true},
			}},
			{Title: "Addressing", Fields: []plugin.ObjectDetailField{
				{Key: "address", Label: "Address", Copy: true},
				{Key: "port", Label: "Port", Type: plugin.ColumnNumber},
				{Key: "socket_path", Label: "Socket path"},
				{Key: "node_address", Label: "Node address"},
				{Key: "tagged_addresses", Label: "Tagged addresses", Type: plugin.ColumnJSON},
			}},
			{Title: "Mesh", Fields: []plugin.ObjectDetailField{
				{Key: "proxy_destination", Label: "Proxy destination"},
				{Key: "connect_native", Label: "Connect native", Type: plugin.ColumnBool},
				{Key: "upstreams", Label: "Upstreams", Type: plugin.ColumnJSON},
			}},
			{Title: "Metadata", Fields: []plugin.ObjectDetailField{
				{Key: "tags", Label: "Tags"},
				{Key: "meta", Label: "Meta", Type: plugin.ColumnJSON},
				{Key: "weights", Label: "Weights", Type: plugin.ColumnJSON},
				{Key: "passing", Label: "Passing checks", Type: plugin.ColumnNumber},
				{Key: "critical", Label: "Critical checks", Type: plugin.ColumnNumber},
			}},
		},
	}
}

func nodeDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Sections: []plugin.ObjectDetailSection{
			{Title: "Node", Fields: []plugin.ObjectDetailField{
				{Key: "name", Label: "Name", Copy: true},
				{Key: "id", Label: "Node ID", Copy: true},
				{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: healthSeverities},
				{Key: "address", Label: "Address", Copy: true},
				{Key: "datacenter", Label: "Datacenter"},
				{Key: "partition", Label: "Admin partition"},
			}},
			{Title: "Registration", Fields: []plugin.ObjectDetailField{
				{Key: "services", Label: "Services", Type: plugin.ColumnNumber},
				{Key: "passing", Label: "Passing checks", Type: plugin.ColumnNumber},
				{Key: "warning", Label: "Warning checks", Type: plugin.ColumnNumber},
				{Key: "critical", Label: "Critical checks", Type: plugin.ColumnNumber},
			}},
			{Title: "Metadata", Fields: []plugin.ObjectDetailField{
				{Key: "tagged_addresses", Label: "Tagged addresses", Type: plugin.ColumnJSON},
				{Key: "meta", Label: "Meta", Type: plugin.ColumnJSON},
			}},
		},
	}
}

func checkDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Sections: []plugin.ObjectDetailSection{
			{Title: "Check", Fields: []plugin.ObjectDetailField{
				{Key: "name", Label: "Check ID", Copy: true},
				{Key: "title", Label: "Name"},
				{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: healthSeverities},
				{Key: "type", Label: "Type", Type: plugin.ColumnBadge},
				{Key: "node", Label: "Node", Copy: true},
				{Key: "service", Label: "Service"},
			}},
			{Title: "Definition", Fields: []plugin.ObjectDetailField{
				{Key: "http", Label: "HTTP endpoint", Copy: true},
				{Key: "tcp", Label: "TCP endpoint"},
				{Key: "grpc", Label: "gRPC endpoint"},
				{Key: "interval", Label: "Interval"},
				{Key: "timeout", Label: "Timeout"},
				{Key: "deregister_after", Label: "Deregister critical after"},
			}},
			{Title: "Last result", Fields: []plugin.ObjectDetailField{
				{Key: "notes", Label: "Notes"},
				{Key: "output", Label: "Output"},
			}},
		},
	}
}

func sessionDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Sections: []plugin.ObjectDetailSection{
			{Title: "Session", Fields: []plugin.ObjectDetailField{
				{Key: "id", Label: "ID", Copy: true},
				{Key: "name", Label: "Name"},
				{Key: "node", Label: "Node", Copy: true},
				{Key: "behavior", Label: "Invalidation behavior", Type: plugin.ColumnBadge},
				{Key: "ttl", Label: "TTL"},
				{Key: "lock_delay", Label: "Lock delay", Type: plugin.ColumnDuration},
			}},
			{Title: "Attached checks", Fields: []plugin.ObjectDetailField{
				{Key: "node_checks", Label: "Node checks", Type: plugin.ColumnJSON},
				{Key: "service_checks", Label: "Service checks", Type: plugin.ColumnJSON},
				{Key: "create_index", Label: "Create index", Type: plugin.ColumnNumber},
			}},
		},
	}
}

func preparedQueryDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Sections: []plugin.ObjectDetailSection{
			{Title: "Query", Fields: []plugin.ObjectDetailField{
				{Key: "id", Label: "ID", Copy: true},
				{Key: "name", Label: "Name", Copy: true},
				{Key: "session", Label: "Session"},
				{Key: "template_type", Label: "Template type"},
				{Key: "template_regexp", Label: "Template regexp"},
			}},
			{Title: "Service selection", Fields: []plugin.ObjectDetailField{
				{Key: "service", Label: "Service", Copy: true},
				{Key: "near", Label: "Near"},
				{Key: "only_passing", Label: "Only passing", Type: plugin.ColumnBool},
				{Key: "tags", Label: "Tags"},
				{Key: "node_meta", Label: "Node meta", Type: plugin.ColumnJSON},
				{Key: "failover", Label: "Failover", Type: plugin.ColumnJSON},
			}},
			{Title: "DNS", Fields: []plugin.ObjectDetailField{
				{Key: "dns_ttl", Label: "DNS TTL"},
			}},
		},
	}
}

func intentionDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Sections: []plugin.ObjectDetailSection{
			{Title: "Intention", Fields: []plugin.ObjectDetailField{
				{Key: "source", Label: "Source", Copy: true},
				{Key: "destination", Label: "Destination", Copy: true},
				{Key: "action", Label: "Action", Type: plugin.ColumnBadge, Severities: intentionSeverities},
				{Key: "precedence", Label: "Precedence", Type: plugin.ColumnNumber},
				{Key: "source_type", Label: "Source type"},
				{Key: "description", Label: "Description"},
			}},
			{Title: "Layer 7", Fields: []plugin.ObjectDetailField{
				{Key: "permissions", Label: "Permission rules", Type: plugin.ColumnNumber},
				{Key: "permission_detail", Label: "Permissions", Type: plugin.ColumnJSON},
			}},
			{Title: "History", Fields: []plugin.ObjectDetailField{
				{Key: "created_at", Label: "Created", Type: plugin.ColumnDateTime},
				{Key: "updated_at", Label: "Updated", Type: plugin.ColumnRelativeTime},
				{Key: "meta", Label: "Meta", Type: plugin.ColumnJSON},
			}},
		},
	}
}

func aclTokenDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Sections: []plugin.ObjectDetailSection{
			{Title: "Token", Fields: []plugin.ObjectDetailField{
				{Key: "accessor_id", Label: "Accessor ID", Copy: true},
				{Key: "description", Label: "Description"},
				{Key: "secret_id", Label: "Secret ID", Redacted: true},
				{Key: "local", Label: "Local token", Type: plugin.ColumnBool},
				{Key: "auth_method", Label: "Auth method"},
			}},
			{Title: "Grants", Fields: []plugin.ObjectDetailField{
				{Key: "policies", Label: "Policies"},
				{Key: "roles", Label: "Roles"},
				{Key: "service_identities", Label: "Service identities"},
				{Key: "node_identities", Label: "Node identities"},
				{Key: "templated_policies", Label: "Templated policies"},
			}},
			{Title: "Lifetime", Fields: []plugin.ObjectDetailField{
				{Key: "created_at", Label: "Created", Type: plugin.ColumnDateTime},
				{Key: "expires_at", Label: "Expires", Type: plugin.ColumnRelativeTime},
				{Key: "create_index", Label: "Create index", Type: plugin.ColumnNumber},
				{Key: "modify_index", Label: "Modify index", Type: plugin.ColumnNumber},
			}},
		},
	}
}

func aclPolicyDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Sections: []plugin.ObjectDetailSection{
			{Title: "Policy", Fields: []plugin.ObjectDetailField{
				{Key: "id", Label: "ID", Copy: true},
				{Key: "name", Label: "Name", Copy: true},
				{Key: "description", Label: "Description"},
				{Key: "datacenters", Label: "Datacenters"},
			}},
			{Title: "Rules", Fields: []plugin.ObjectDetailField{
				{Key: "rules", Label: "HCL rules", Copy: true},
			}},
		},
	}
}

func aclRoleDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Sections: []plugin.ObjectDetailSection{
			{Title: "Role", Fields: []plugin.ObjectDetailField{
				{Key: "id", Label: "ID", Copy: true},
				{Key: "name", Label: "Name", Copy: true},
				{Key: "description", Label: "Description"},
			}},
			{Title: "Grants", Fields: []plugin.ObjectDetailField{
				{Key: "policies", Label: "Policies"},
				{Key: "service_identities", Label: "Service identities"},
				{Key: "node_identities", Label: "Node identities"},
				{Key: "templated_policies", Label: "Templated policies"},
			}},
		},
	}
}
