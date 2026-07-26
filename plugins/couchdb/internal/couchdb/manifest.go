package couchdb

import "github.com/charlesng35/shellcn/sdk/plugin"

const iconSVG = `<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" id="Couchdb-Icon--Streamline-Svg-Logos" height="24" width="24"><desc>Couchdb Icon Streamline Icon: https://streamlinehq.com</desc><path fill="#e42528" d="M19.34365 14.5703c0 0.973925 -0.512975 1.45115 -1.4687 1.468075v0.00065H6.12505v-0.00065c-0.955725 -0.016925 -1.4687 -0.49415 -1.4687 -1.468075 0 -0.9737 0.512975 -1.45115 1.4687 -1.46785v-0.00085h11.7499v0.00085c0.955725 0.0167 1.4687 0.49415 1.4687 1.46785Zm-1.4687 2.204025v-0.00085H6.12505v0.00085c-0.955725 0.016725 -1.4687 0.49415 -1.4687 1.468075s0.512975 1.450925 1.4687 1.46785v0.00065h11.7499v-0.00085c0.955725 -0.016725 1.4687 -0.493925 1.4687 -1.46785 0 -0.97395 -0.512975 -1.45115 -1.4687 -1.467875Zm3.671875 -8.07735v-0.000875c-0.955725 0.016925 -1.4687 0.49415 -1.4687 1.468075v8.078025c0 0.973925 0.512975 1.450925 1.4687 1.46785v-0.0015C22.9804 19.6582 23.75 18.22635 23.75 15.304775V11.632875c0 -1.947625 -0.7696 -2.902075 -2.203175 -2.9359Zm-19.0936525 -0.000875v0.000875C1.01959 8.7308 0.25 9.68525 0.25 11.632875v3.6719C0.25 18.22635 1.01959 19.658 2.4531725 19.70855v0.0015c0.9557275 -0.016725 1.4687025 -0.493925 1.4687025 -1.46785V10.164175c0 -0.973925 -0.512975 -1.45115 -1.4687025 -1.468075ZM21.546825 7.961c0 -2.4347 -1.282575 -3.62775 -3.671875 -3.66995v-0.001925H6.12505v0.001925c-2.389075 0.0422 -3.6718775 1.23525 -3.6718775 3.66995v0.0013c1.4335775 0.025275 2.2031775 0.7411 2.2031775 2.201875s0.769575 2.176625 2.203175 2.2019v0.00105h10.281175v-0.00105c1.433375 -0.025275 2.20295 -0.741125 2.20295 -2.2019 0 -1.460775 0.7696 -2.1766 2.203175 -2.201875v-0.0013Z" stroke-width="0.25"></path></svg>`

func icon(name string) plugin.Icon { return plugin.Icon{Type: plugin.IconLucide, Value: name} }

func rid(suffix string) string { return protocolName + "." + suffix }

func (Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:  plugin.CurrentAPIVersion,
		Name:        protocolName,
		Version:     "0.1.0",
		Title:       "CouchDB",
		Description: "Apache CouchDB cockpit: databases, document CRUD with revision and conflict tooling, Mango queries, map/reduce views, replication and the live changes feed.",
		Icon:        plugin.Icon{Type: plugin.IconSVG, Value: iconSVG},
		Category:    plugin.CategoryDatabases,
		Layout:      plugin.LayoutSidebarTree,

		Config: configSchema(),
		Capabilities: []plugin.Capability{
			"documents", "attachments", "mango", "map-reduce", "replication", "changes-feed", "conflicts", "metrics",
		},

		SupportedTransports: []plugin.Transport{plugin.TransportDirect, plugin.TransportAgent},
		Agent: &plugin.AgentProfile{
			Proxy: plugin.ProxyTarget{
				Mode:    plugin.AgentTCP,
				Address: "127.0.0.1:5984",
				Risk:    plugin.RiskPrivileged,
			},
			Install: []plugin.InstallArtifact{{
				Label:    "Docker",
				Kind:     "docker",
				Template: "docker run -d --network host " + plugin.AgentImageLatest + " --connect {{shellquote .ConnectURL}} --token {{shellquote .Token}}",
			}},
		},

		Tree:          tree(),
		Resources:     resources(),
		Actions:       actions(),
		Streams:       streams(),
		HeaderActions: []string{rid("fauxton.open")},
	}
}

func tree() []plugin.TreeGroup {
	return []plugin.TreeGroup{
		{
			Key: "server", Label: "Server", Icon: icon("server"),
			Ref: &plugin.ResourceIdentity{Kind: "server", Name: "Server", UID: "server"},
		},
		{
			Key: "databases", Label: "Databases", Icon: icon("database"),
			Source: plugin.DataSource{RouteID: rid("databases.tree")}, ResourceKind: "database",
		},
		{
			Key: "replication", Label: "Replication", Icon: icon("refresh-cw"),
			Source: plugin.DataSource{RouteID: rid("replications.tree")}, ResourceKind: "replication",
		},
		{
			Key: "nodes", Label: "Cluster", Icon: icon("network"),
			Source: plugin.DataSource{RouteID: rid("nodes.tree")}, ResourceKind: "node",
		},
	}
}

func streams() []plugin.Stream {
	return []plugin.Stream{
		{ID: rid("mango.query"), Kind: plugin.StreamQuery, RouteID: rid("mango.query")},
		{ID: rid("changes.stream"), Kind: plugin.StreamLogs, RouteID: rid("changes.stream")},
		{ID: rid("server.metrics"), Kind: plugin.StreamMetrics, RouteID: rid("server.metrics")},
		{ID: rid("databases.watch"), Kind: plugin.StreamResource, RouteID: rid("databases.watch")},
		{ID: rid("database.watch"), Kind: plugin.StreamResource, RouteID: rid("database.watch")},
		{ID: rid("documents.watch"), Kind: plugin.StreamResource, RouteID: rid("documents.watch")},
		{ID: rid("document.watch"), Kind: plugin.StreamResource, RouteID: rid("document.watch")},
		{ID: rid("tasks.watch"), Kind: plugin.StreamResource, RouteID: rid("tasks.watch")},
		{ID: rid("replications.watch"), Kind: plugin.StreamResource, RouteID: rid("replications.watch")},
	}
}

// --- shared param maps -------------------------------------------------------

func dbParams() map[string]string { return map[string]string{"db": "${resource.name}"} }

func docParams() map[string]string {
	return map[string]string{"db": "${resource.namespace}", "docid": "${resource.name}"}
}

func ddocParams() map[string]string {
	return map[string]string{"db": "${resource.namespace}", "ddoc": "${resource.name}"}
}

func viewParams() map[string]string {
	return map[string]string{"db": "${resource.scope}", "ddoc": "${resource.namespace}", "view": "${resource.name}"}
}

func replicationParams() map[string]string { return map[string]string{"id": "${resource.name}"} }

func nodeParams() map[string]string { return map[string]string{"node": "${resource.name}"} }

// --- columns -----------------------------------------------------------------

func databaseColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Database", Sortable: true},
		{Key: "doc_count", Label: "Documents", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "doc_del_count", Label: "Deleted", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "disk_size", Label: "Disk", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "data_size", Label: "Active", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "fragmentation", Label: "Fragmentation", Type: plugin.ColumnPercent, Sortable: true},
		{Key: "update_seq", Label: "Update seq"},
		{Key: "partitioned", Label: "Partitioned", Type: plugin.ColumnBool},
	}
}

func conflictColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "id", Label: "Document", Sortable: true},
		{Key: "rev", Label: "Winning revision"},
		{Key: "conflicts", Label: "Conflicts", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "conflict_revs", Label: "Conflicting revisions", Type: plugin.ColumnJSON},
	}
}

func revisionColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "rev", Label: "Revision", Sortable: true},
		{Key: "generation", Label: "Generation", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: map[string]plugin.Severity{
			"available": plugin.SeveritySuccess,
			"missing":   plugin.SeveritySecondary,
			"deleted":   plugin.SeverityWarn,
		}},
	}
}

func indexColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Index", Sortable: true},
		{Key: "type", Label: "Type", Type: plugin.ColumnBadge, Sortable: true, Severities: map[string]plugin.Severity{
			"json":    plugin.SeverityInfo,
			"text":    plugin.SeveritySuccess,
			"special": plugin.SeveritySecondary,
		}},
		{Key: "ddoc", Label: "Design document"},
		{Key: "fields", Label: "Fields", Type: plugin.ColumnJSON},
		{Key: "partitioned", Label: "Partitioned", Type: plugin.ColumnBool},
	}
}

func designDocColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Design document", Sortable: true},
		{Key: "language", Label: "Language", Type: plugin.ColumnBadge, Severities: map[string]plugin.Severity{
			"javascript": plugin.SeverityInfo,
			"query":      plugin.SeveritySuccess,
			"erlang":     plugin.SeverityWarn,
		}},
		{Key: "views", Label: "Views", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "has_reduce", Label: "Reduce", Type: plugin.ColumnBool},
		{Key: "rev", Label: "Revision"},
	}
}

func viewColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "View", Sortable: true},
		{Key: "reduce", Label: "Reduce", Type: plugin.ColumnBool},
		{Key: "reduce_kind", Label: "Reducer", Type: plugin.ColumnBadge, Severities: map[string]plugin.Severity{
			"builtin": plugin.SeveritySuccess,
			"custom":  plugin.SeverityWarn,
			"none":    plugin.SeveritySecondary,
		}},
	}
}

func viewRowColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "key", Label: "Key", Type: plugin.ColumnJSON, Sortable: true},
		{Key: "value", Label: "Value", Type: plugin.ColumnJSON},
		{Key: "id", Label: "Document", Sortable: true},
	}
}

func replicationColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Replication", Sortable: true},
		{Key: "state", Label: "State", Type: plugin.ColumnBadge, Sortable: true, Severities: replicationSeverities()},
		{Key: "source", Label: "Source"},
		{Key: "target", Label: "Target"},
		{Key: "continuous", Label: "Continuous", Type: plugin.ColumnBool},
		{Key: "start_time", Label: "Started", Type: plugin.ColumnRelativeTime, Sortable: true},
		{Key: "error_count", Label: "Errors", Type: plugin.ColumnNumber, Sortable: true},
	}
}

func replicationSeverities() map[string]plugin.Severity {
	return map[string]plugin.Severity{
		"running":   plugin.SeveritySuccess,
		"completed": plugin.SeverityInfo,
		"pending":   plugin.SeverityWarn,
		"crashing":  plugin.SeverityDanger,
		"failed":    plugin.SeverityDanger,
		"error":     plugin.SeverityDanger,
	}
}

func schedulerColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "id", Label: "Job", Sortable: true},
		{Key: "database", Label: "Replicator DB"},
		{Key: "state", Label: "State", Type: plugin.ColumnBadge, Sortable: true, Severities: replicationSeverities()},
		{Key: "source", Label: "Source"},
		{Key: "target", Label: "Target"},
		{Key: "node", Label: "Node"},
		{Key: "start_time", Label: "Started", Type: plugin.ColumnRelativeTime, Sortable: true},
	}
}

func nodeColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Node", Sortable: true},
		{Key: "membership", Label: "Membership", Type: plugin.ColumnBadge, Sortable: true, Severities: map[string]plugin.Severity{
			"cluster": plugin.SeveritySuccess,
			"known":   plugin.SeveritySecondary,
		}},
		{Key: "uptime", Label: "Uptime", Type: plugin.ColumnDuration, Sortable: true},
		{Key: "memory_used", Label: "Memory", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "process_count", Label: "Processes", Type: plugin.ColumnNumber, Sortable: true},
	}
}

func attachmentColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Attachment", Sortable: true},
		{Key: "content_type", Label: "Content type", Sortable: true},
		{Key: "length", Label: "Size", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "revpos", Label: "Added in generation", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "digest", Label: "Digest"},
	}
}

func savedQueryColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Name", Sortable: true},
		{Key: "database", Label: "Database", Sortable: true},
		{Key: "updated_at", Label: "Updated", Type: plugin.ColumnRelativeTime, Sortable: true},
		{Key: "query", Label: "Selector", Type: plugin.ColumnJSON},
	}
}

func serverColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Server", Sortable: true},
		{Key: "version", Label: "Version"},
		{Key: "vendor", Label: "Vendor"},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: map[string]plugin.Severity{
			"ok":              plugin.SeveritySuccess,
			"maintenance":     plugin.SeverityWarn,
			"nolb":            plugin.SeverityWarn,
			"unknown":         plugin.SeveritySecondary,
			"not_implemented": plugin.SeveritySecondary,
		}},
		{Key: "databases", Label: "Databases", Type: plugin.ColumnNumber},
	}
}

// --- resources ---------------------------------------------------------------

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		serverResource(),
		databaseResource(),
		documentResource(),
		designDocResource(),
		viewResource(),
		replicationResource(),
		nodeResource(),
	}
}

func serverResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "server", Title: "Server",
		List:    plugin.DataSource{RouteID: rid("server.list")},
		Columns: serverColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{rid("database.create"), rid("replication.create")},
			Detail:  []string{rid("database.create"), rid("replication.create"), rid("fauxton.open")},
		},
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "CouchDB", StatusField: "status", Severities: serverColumns()[3].Severities},
			DefaultTab: "overview",
			Tabs: []plugin.Panel{
				{
					Key: "overview", Label: "Overview", Icon: icon("layout-dashboard"), Type: plugin.PanelDashboard,
					Config: plugin.DashboardConfig{Cells: []plugin.Panel{
						{
							Key: "identity", Label: "Server", Icon: icon("info"), Span: 1, Type: plugin.PanelObjectDetail,
							Source: &plugin.DataSource{RouteID: rid("server.overview")},
							Config: serverDetailConfig(),
						},
						{
							Key: "live", Label: "Live", Icon: icon("activity"), Span: 1, Type: plugin.PanelMetrics,
							Source: &plugin.DataSource{RouteID: rid("server.metrics"), Method: plugin.MethodWS},
							Config: metricsConfig(),
						},
						{
							Key: "dblist", Label: "Databases", Icon: icon("database"), Span: 2, Type: plugin.PanelTable,
							Source: &plugin.DataSource{RouteID: rid("databases.list")},
							Config: databaseTableConfig(),
						},
					}},
				},
				{
					Key: "metrics", Label: "Metrics", Icon: icon("activity"), Type: plugin.PanelMetrics,
					Source: &plugin.DataSource{RouteID: rid("server.metrics"), Method: plugin.MethodWS},
					Config: metricsConfig(),
				},
				{
					Key: "tasks", Label: "Active tasks", Icon: icon("loader"), Type: plugin.PanelTimeline,
					Source: &plugin.DataSource{RouteID: rid("tasks.list")},
					Config: plugin.TimelineConfig{
						TimestampField: "time", TitleField: "title", BodyField: "message",
						SeverityField: "severity", ResourceField: "resource",
						EmptyText: "No compaction, indexing or replication tasks are running.",
						Watch:     &plugin.DataSource{RouteID: rid("tasks.watch")},
					},
				},
				{
					Key: "topology", Label: "Replication map", Icon: icon("workflow"), Type: plugin.PanelGraph,
					Source: &plugin.DataSource{RouteID: rid("replication.graph")},
					Config: plugin.GraphConfig{Layout: plugin.GraphLayoutGrid, FitView: true},
				},
				{
					Key: "scheduler", Label: "Scheduler", Icon: icon("calendar-clock"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("scheduler.jobs")},
					Config: plugin.TableConfig{
						Columns: schedulerColumns(), DefaultSort: &plugin.SortKey{Field: "start_time", Desc: true},
						RefreshIntervalMs: 5000, Exportable: true,
						EmptyText: "The replication scheduler has no jobs.",
					},
				},
				{
					Key: "nodes", Label: "Cluster", Icon: icon("network"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("nodes.list")},
					Config: plugin.TableConfig{
						Columns: nodeColumns(), DefaultSort: &plugin.SortKey{Field: "name"},
						RefreshIntervalMs: 15000, Exportable: true,
						EmptyText: "No cluster members were reported by /_membership.",
					},
				},
				{
					Key: "config", Label: "Configuration", Icon: icon("sliders-horizontal"), Type: plugin.PanelDocument,
					Source: &plugin.DataSource{RouteID: rid("server.config")},
				},
				{
					Key: "fauxton", Label: "Fauxton", Icon: icon("app-window"), Type: plugin.PanelWebProxy,
					Config: plugin.WebProxyConfig{
						Path:         "/_utils/",
						Capabilities: []plugin.WebProxyCapability{plugin.WebProxyCapabilityClipboard, plugin.WebProxyCapabilityDownloads},
						OpenExternal: true,
						AriaLabel:    "CouchDB Fauxton administration UI",
					},
				},
			},
		},
	}
}

func serverDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		Sections: []plugin.ObjectDetailSection{
			{Title: "Server", Fields: []plugin.ObjectDetailField{
				{Key: "version", Label: "Version", Copy: true},
				{Key: "vendor", Label: "Vendor"},
				{Key: "git_sha", Label: "Build", Copy: true},
				{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: serverColumns()[3].Severities},
				{Key: "features", Label: "Features", Type: plugin.ColumnJSON},
			}},
			{Title: "Cluster", Fields: []plugin.ObjectDetailField{
				{Key: "cluster_nodes", Label: "Cluster nodes", Type: plugin.ColumnNumber},
				{Key: "all_nodes", Label: "Known nodes", Type: plugin.ColumnNumber},
				{Key: "local_node", Label: "Local node", Copy: true},
				{Key: "uptime", Label: "Uptime", Type: plugin.ColumnDuration},
			}},
			{Title: "Content", Fields: []plugin.ObjectDetailField{
				{Key: "databases", Label: "Databases", Type: plugin.ColumnNumber},
				{Key: "documents", Label: "Documents", Type: plugin.ColumnNumber},
				{Key: "deleted_documents", Label: "Deleted documents", Type: plugin.ColumnNumber},
				{Key: "disk_size", Label: "On disk", Type: plugin.ColumnBytes},
				{Key: "active_tasks", Label: "Active tasks", Type: plugin.ColumnNumber},
				{Key: "replications", Label: "Replications", Type: plugin.ColumnNumber},
			}},
		},
		RawToggle: true,
	}
}

func metricsConfig() plugin.MetricsConfig {
	return plugin.MetricsConfig{
		Stats: []plugin.MetricStat{
			{Key: "requestsPerSec", Label: "Requests/s"},
			{Key: "requests", Label: "Requests"},
			{Key: "reads", Label: "Doc reads"},
			{Key: "writes", Label: "Doc writes"},
			{Key: "activeTasks", Label: "Active tasks"},
			{Key: "openDatabases", Label: "Open databases"},
			{Key: "openFiles", Label: "Open files"},
			{Key: "uptime", Label: "Uptime", Unit: "s"},
		},
		Usage: []plugin.MetricUsage{{
			Key: "memPct", Label: "Erlang process memory", Type: plugin.ColumnPercent,
			Usage: &plugin.UsageSpec{
				PercentKey: "memPct", UsedKey: "memUsed", TotalKey: "memTotal",
				UsedType: plugin.ColumnBytes, TotalType: plugin.ColumnBytes,
				TotalLabel: "of", WarnAt: 80, CriticalAt: 95,
			},
		}},
		Series: []plugin.MetricSeries{
			{Key: "requestsPerSec", Label: "Requests/s"},
			{Key: "activeTasks", Label: "Active tasks"},
			{Key: "runQueue", Label: "Run queue"},
		},
		History: 90,
	}
}

func databaseTableConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Columns:      databaseColumns(),
		DefaultSort:  &plugin.SortKey{Field: "name"},
		ActionIDs:    []string{rid("database.create")},
		RowActionIDs: []string{rid("database.delete")},
		Watch:        &plugin.DataSource{RouteID: rid("databases.watch")},
		Exportable:   true,
		EmptyText:    "No databases yet. Create one to start storing documents.",
	}
}

func databaseResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "database", Title: "Databases",
		List:    plugin.DataSource{RouteID: rid("databases.list")},
		Watch:   &plugin.DataSource{RouteID: rid("databases.watch")},
		Columns: databaseColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{rid("database.create")},
			Row:     []string{rid("database.delete")},
			Detail: []string{
				rid("document.create"), rid("documents.bulk"), rid("designdoc.create"),
				rid("index.create"), rid("mango.explain"), rid("query.save"),
				rid("database.compact"), rid("database.viewcleanup"), rid("database.delete"),
			},
		},
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.name}"},
			DefaultTab: "documents",
			Tabs: []plugin.Panel{
				{
					Key: "db_overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("database.read"), Params: dbParams()},
					Config: plugin.ObjectDetailConfig{
						Sections: []plugin.ObjectDetailSection{
							{Title: "Content", Fields: []plugin.ObjectDetailField{
								{Key: "name", Label: "Name", Copy: true},
								{Key: "doc_count", Label: "Documents", Type: plugin.ColumnNumber},
								{Key: "doc_del_count", Label: "Deleted documents", Type: plugin.ColumnNumber},
								{Key: "update_seq", Label: "Update sequence", Copy: true},
								{Key: "purge_seq", Label: "Purge sequence"},
								{Key: "partitioned", Label: "Partitioned", Type: plugin.ColumnBool},
							}},
							{Title: "Storage", Fields: []plugin.ObjectDetailField{
								{
									Key: "fragmentation", Label: "Fragmentation", Type: plugin.ColumnPercent,
									Usage: &plugin.UsageSpec{
										PercentKey: "fragmentation", UsedKey: "data_size", TotalKey: "disk_size",
										UsedType: plugin.ColumnBytes, TotalType: plugin.ColumnBytes,
										TotalLabel: "of", WarnAt: 40, CriticalAt: 70,
									},
								},
								{Key: "disk_size", Label: "File size", Type: plugin.ColumnBytes},
								{Key: "data_size", Label: "Active data", Type: plugin.ColumnBytes},
								{Key: "compact_running", Label: "Compacting", Type: plugin.ColumnBool},
								{Key: "cluster", Label: "Shards (q/n/r/w)", Type: plugin.ColumnJSON},
							}},
						},
						RawToggle: true,
						Watch:     &plugin.DataSource{RouteID: rid("database.watch"), Params: dbParams()},
					},
				},
				{
					Key: "documents", Label: "Documents", Icon: icon("file-json"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("documents.list"), Params: dbParams()},
					Config: plugin.TableConfig{
						ColumnsSource: &plugin.DataSource{RouteID: rid("documents.columns"), Params: dbParams()},
						Watch:         &plugin.DataSource{RouteID: rid("documents.watch"), Params: dbParams()},
						Editable:      true,
						StagedEdits:   true,
						RowKey:        []string{"id"},
						Insert:        &plugin.DataSource{RouteID: rid("documents.insert"), Method: plugin.MethodPost, Params: dbParams()},
						Update:        &plugin.DataSource{RouteID: rid("documents.update"), Method: plugin.MethodPut, Params: dbParams()},
						Delete:        &plugin.DataSource{RouteID: rid("documents.remove"), Method: plugin.MethodDelete, Params: dbParams()},
						ActionIDs:     []string{rid("document.create"), rid("documents.bulk")},
						RowActionIDs:  []string{rid("document.delete")},
						HiddenColumns: []string{"ref"},
						Exportable:    true,
						EmptyText:     "This database has no documents yet.",
					},
				},
				{
					Key: "mango", Label: "Mango", Icon: icon("search"), Type: plugin.PanelQueryEditor,
					Source: &plugin.DataSource{RouteID: rid("mango.query"), Method: plugin.MethodWS, Params: dbParams()},
					Config: plugin.QueryEditorConfig{
						Language: "json", Label: "Mango selector",
						ExecuteLabel: "Find", RunningLabel: "Finding...",
						EmptyText: "Run a Mango query. The body is POSTed to /{db}/_find and the chosen index is reported with every result. " +
							"The default selector matches every document through the built-in _all_docs index.",
						InitialQuery:      "{\n  \"selector\": {\n    \"_id\": { \"$gt\": null }\n  },\n  \"limit\": 25\n}",
						CompletionRouteID: rid("mango.complete"),
						CompletionParams:  dbParams(),
						Exportable:        true,
					},
				},
				{
					Key: "saved", Label: "Saved queries", Icon: icon("bookmark"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("queries.list"), Params: dbParams()},
					Config: plugin.TableConfig{
						Columns: savedQueryColumns(), DefaultSort: &plugin.SortKey{Field: "updated_at", Desc: true},
						ActionIDs: []string{rid("query.save")}, RowActionIDs: []string{rid("query.delete")},
						RefreshIntervalMs: 30000,
						EmptyText:         "No saved Mango queries for this connection yet.",
					},
				},
				{
					Key: "indexes", Label: "Indexes", Icon: icon("list-tree"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("indexes.list"), Params: dbParams()},
					Config: plugin.TableConfig{
						Columns: indexColumns(), DefaultSort: &plugin.SortKey{Field: "name"},
						ActionIDs: []string{rid("index.create")}, RowActionIDs: []string{rid("index.delete")},
						RefreshIntervalMs: 30000, Exportable: true,
						EmptyText: "Only the built-in _all_docs index exists. Create a Mango index to avoid full scans.",
					},
				},
				{
					Key: "design", Label: "Design docs", Icon: icon("file-code"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("designdocs.list"), Params: dbParams()},
					Config: plugin.TableConfig{
						Columns: designDocColumns(), DefaultSort: &plugin.SortKey{Field: "name"},
						ActionIDs: []string{rid("designdoc.create")}, RowActionIDs: []string{rid("designdoc.delete")},
						RefreshIntervalMs: 30000, Exportable: true,
						EmptyText: "No design documents. Add one to define map/reduce views.",
					},
				},
				{
					Key: "conflicts", Label: "Conflicts", Icon: icon("git-merge"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("conflicts.list"), Params: dbParams()},
					Config: plugin.TableConfig{
						Columns: conflictColumns(), DefaultSort: &plugin.SortKey{Field: "conflicts", Desc: true},
						RowActionIDs:      []string{rid("document.resolve")},
						RefreshIntervalMs: 20000, Exportable: true,
						HiddenColumns: []string{"ref"},
						EmptyText:     "No conflicting documents. Replication has converged.",
					},
				},
				{
					Key: "changes", Label: "Changes feed", Icon: icon("activity"), Type: plugin.PanelLogStream,
					Source: &plugin.DataSource{RouteID: rid("changes.stream"), Method: plugin.MethodWS, Params: dbParams()},
					Config: plugin.LogStreamConfig{Controls: []plugin.StreamControl{{
						Param: "since", Label: "Start from",
						OptionsSource: &plugin.DataSource{RouteID: rid("changes.origins"), Params: dbParams()},
					}}},
				},
				{
					Key: "security", Label: "Security", Icon: icon("shield"), Type: plugin.PanelCodeEditor,
					Source: &plugin.DataSource{RouteID: rid("database.security.read"), Params: dbParams()},
					Config: plugin.CodeEditorConfig{
						Language:     "json",
						SaveRouteID:  rid("database.security.update"),
						SaveMethod:   plugin.MethodPut,
						SaveParams:   dbParams(),
						RefreshField: "content",
						SaveToast:    &plugin.SaveToast{Summary: "Security document saved", Severity: plugin.SeveritySuccess},
					},
				},
			},
		},
	}
}

func documentResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "document", Title: "Documents",
		List:    plugin.DataSource{RouteID: rid("documents.list"), Params: map[string]string{"db": "${resource.namespace}"}},
		Watch:   &plugin.DataSource{RouteID: rid("documents.watch"), Params: map[string]string{"db": "${resource.namespace}"}},
		Columns: []plugin.Column{{Key: "id", Label: "Document", Sortable: true}, {Key: "rev", Label: "Revision"}},
		Actions: plugin.ResourceActions{
			Detail: []string{rid("document.resolve.current"), rid("document.purge"), rid("document.delete.current")},
		},
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.name}"},
			DefaultTab: "doc_editor",
			Tabs: []plugin.Panel{
				{
					Key: "doc_editor", Label: "Document", Icon: icon("file-json"), Type: plugin.PanelCodeEditor,
					Source: &plugin.DataSource{RouteID: rid("document.read"), Params: docParams()},
					Config: plugin.CodeEditorConfig{
						Language:     "json",
						SaveRouteID:  rid("document.update"),
						SaveMethod:   plugin.MethodPut,
						SaveParams:   docParams(),
						RefreshField: "content",
						Watch:        &plugin.DataSource{RouteID: rid("document.watch"), Params: docParams()},
						SaveToast:    &plugin.SaveToast{Summary: "Document saved", Detail: "New revision ${response.rev}", Severity: plugin.SeveritySuccess},
					},
				},
				{
					Key: "doc_revisions", Label: "Revisions", Icon: icon("history"), Type: plugin.PanelTimeline,
					Source: &plugin.DataSource{RouteID: rid("document.revisions"), Params: docParams()},
					Config: plugin.TimelineConfig{
						TimestampField: "time", TitleField: "rev", BodyField: "message",
						SeverityField: "severity", ResourceField: "resource",
						EmptyText: "CouchDB reported no revision history for this document.",
					},
				},
				{
					Key: "doc_conflicts", Label: "Conflicts", Icon: icon("git-merge"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("document.conflicts"), Params: docParams()},
					Config: plugin.TableConfig{
						Columns: revisionColumns(), DefaultSort: &plugin.SortKey{Field: "generation", Desc: true},
						RefreshIntervalMs: 20000, Exportable: true,
						EmptyText: "This document has no conflicting revisions.",
					},
				},
				{
					Key: "doc_attachments", Label: "Attachments", Icon: icon("paperclip"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("attachments.list"), Params: docParams()},
					Config: plugin.TableConfig{
						Columns: attachmentColumns(), DefaultSort: &plugin.SortKey{Field: "name"},
						RowActionIDs:      []string{rid("attachment.delete")},
						RefreshIntervalMs: 30000, Exportable: true,
						EmptyText: "This document carries no attachments.",
					},
				},
			},
		},
	}
}

func designDocResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "designdoc", Title: "Design documents",
		List:    plugin.DataSource{RouteID: rid("designdocs.list"), Params: map[string]string{"db": "${resource.namespace}"}},
		Columns: designDocColumns(),
		Actions: plugin.ResourceActions{Detail: []string{rid("designdoc.delete.current")}},
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.name}"},
			DefaultTab: "ddoc_source",
			Tabs: []plugin.Panel{
				{
					Key: "ddoc_source", Label: "Map / reduce", Icon: icon("code"), Type: plugin.PanelCodeEditor,
					Source: &plugin.DataSource{RouteID: rid("designdoc.read"), Params: ddocParams()},
					Config: plugin.CodeEditorConfig{
						Language:     "json",
						SaveRouteID:  rid("designdoc.save"),
						SaveMethod:   plugin.MethodPut,
						SaveParams:   ddocParams(),
						RefreshField: "content",
						SaveToast:    &plugin.SaveToast{Summary: "Design document saved", Detail: "New revision ${response.rev}", Severity: plugin.SeveritySuccess},
					},
				},
				{
					Key: "ddoc_views", Label: "Views", Icon: icon("table-2"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("views.list"), Params: ddocParams()},
					Config: plugin.TableConfig{
						Columns: viewColumns(), DefaultSort: &plugin.SortKey{Field: "name"},
						RefreshIntervalMs: 30000, Exportable: true,
						EmptyText: "This design document declares no views.",
					},
				},
			},
		},
	}
}

func viewResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "view", Title: "Views",
		List:    plugin.DataSource{RouteID: rid("views.list"), Params: map[string]string{"db": "${resource.scope}", "ddoc": "${resource.namespace}"}},
		Columns: viewColumns(),
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.namespace}/${resource.name}"},
			DefaultTab: "view_rows",
			Tabs: []plugin.Panel{
				{
					Key: "view_rows", Label: "Result", Icon: icon("rows-3"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("view.rows"), Params: viewParams()},
					Config: plugin.TableConfig{
						Columns: viewRowColumns(), RefreshIntervalMs: 30000, Exportable: true,
						EmptyText: "The view emitted no rows.",
					},
				},
				{
					Key: "view_reduce", Label: "Reduced", Icon: icon("sigma"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("view.reduce"), Params: viewParams()},
					Config: plugin.TableConfig{
						Columns: []plugin.Column{
							{Key: "key", Label: "Group key", Type: plugin.ColumnJSON, Sortable: true},
							{Key: "value", Label: "Reduced value", Type: plugin.ColumnJSON},
						},
						RefreshIntervalMs: 30000, Exportable: true,
						EmptyText: "This view has no reduce function, so there is nothing to group.",
					},
				},
				{
					Key: "view_definition", Label: "Definition", Icon: icon("file-code"), Type: plugin.PanelDocument,
					Source: &plugin.DataSource{RouteID: rid("view.definition"), Params: viewParams()},
				},
			},
		},
	}
}

func replicationResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "replication", Title: "Replications",
		List:    plugin.DataSource{RouteID: rid("replications.list")},
		Watch:   &plugin.DataSource{RouteID: rid("replications.watch")},
		Columns: replicationColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{rid("replication.create")},
			Row:     []string{rid("replication.delete")},
			Detail:  []string{rid("replication.delete")},
		},
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.name}", StatusField: "state", Severities: replicationSeverities()},
			DefaultTab: "rep_overview",
			Tabs: []plugin.Panel{
				{
					Key: "rep_overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("replication.read"), Params: replicationParams()},
					Config: plugin.ObjectDetailConfig{
						Sections: []plugin.ObjectDetailSection{
							{Title: "Replication", Fields: []plugin.ObjectDetailField{
								{Key: "name", Label: "Document id", Copy: true},
								{Key: "state", Label: "State", Type: plugin.ColumnBadge, Severities: replicationSeverities()},
								{Key: "source", Label: "Source", Copy: true},
								{Key: "target", Label: "Target", Copy: true},
								{Key: "continuous", Label: "Continuous", Type: plugin.ColumnBool},
								{Key: "create_target", Label: "Creates target", Type: plugin.ColumnBool},
								{Key: "owner", Label: "Owner"},
							}},
							{Title: "Progress", Fields: []plugin.ObjectDetailField{
								{Key: "start_time", Label: "Started", Type: plugin.ColumnDateTime},
								{Key: "last_updated", Label: "Last update", Type: plugin.ColumnRelativeTime},
								{Key: "progress", Label: "Progress", Type: plugin.ColumnPercent},
								{Key: "docs_written", Label: "Documents written", Type: plugin.ColumnNumber},
								{Key: "docs_read", Label: "Documents read", Type: plugin.ColumnNumber},
								{Key: "doc_write_failures", Label: "Write failures", Type: plugin.ColumnNumber},
								{Key: "error_count", Label: "Errors", Type: plugin.ColumnNumber},
								{Key: "info", Label: "Scheduler info", Type: plugin.ColumnJSON},
							}},
						},
						RawToggle: true,
					},
				},
				{
					Key: "rep_events", Label: "History", Icon: icon("history"), Type: plugin.PanelTimeline,
					Source: &plugin.DataSource{RouteID: rid("replication.events"), Params: replicationParams()},
					Config: plugin.TimelineConfig{
						TimestampField: "time", TitleField: "title", BodyField: "message",
						SeverityField: "severity", ResourceField: "resource",
						EmptyText: "The scheduler has no recorded history for this replication.",
						Watch:     &plugin.DataSource{RouteID: rid("replications.watch")},
					},
				},
			},
		},
	}
}

func nodeResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "node", Title: "Cluster nodes",
		List:    plugin.DataSource{RouteID: rid("nodes.list")},
		Columns: nodeColumns(),
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.name}"},
			DefaultTab: "node_overview",
			Tabs: []plugin.Panel{
				{
					Key: "node_overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("node.read"), Params: nodeParams()},
					Config: plugin.ObjectDetailConfig{
						Sections: []plugin.ObjectDetailSection{
							{Title: "Node", Fields: []plugin.ObjectDetailField{
								{Key: "name", Label: "Name", Copy: true},
								{Key: "membership", Label: "Membership", Type: plugin.ColumnBadge, Severities: nodeColumns()[1].Severities},
								{Key: "uptime", Label: "Uptime", Type: plugin.ColumnDuration},
								{Key: "process_count", Label: "Processes", Type: plugin.ColumnNumber},
								{Key: "run_queue", Label: "Run queue", Type: plugin.ColumnNumber},
							}},
							{Title: "Memory", Fields: []plugin.ObjectDetailField{{
								Key: "memory_pct", Label: "Process memory", Type: plugin.ColumnPercent,
								Usage: &plugin.UsageSpec{
									PercentKey: "memory_pct", UsedKey: "memory_used", TotalKey: "memory_total",
									UsedType: plugin.ColumnBytes, TotalType: plugin.ColumnBytes,
									TotalLabel: "of", WarnAt: 80, CriticalAt: 95,
								},
							}}},
						},
						RawToggle: true,
					},
				},
				{
					Key: "node_stats", Label: "Statistics", Icon: icon("chart-line"), Type: plugin.PanelDocument,
					Source: &plugin.DataSource{RouteID: rid("node.stats"), Params: nodeParams()},
				},
			},
		},
	}
}

// --- actions -----------------------------------------------------------------

func actions() []plugin.Action {
	return []plugin.Action{
		{
			ID: rid("database.create"), Label: "Create database", Icon: icon("plus"),
			RouteID: rid("database.create"),
		},
		{
			ID: rid("database.delete"), Label: "Delete", Icon: icon("trash-2"), RouteID: rid("database.delete"),
			Params: dbParams(), Confirm: true,
			ConfirmText: "Delete this database and every document, index and design document in it?",
			OnSuccess:   &plugin.ActionSuccess{Navigate: plugin.NavigateList}, Bulk: true,
		},
		{
			ID: rid("database.compact"), Label: "Compact", Icon: icon("archive-restore"), RouteID: rid("database.compact"),
			Params: dbParams(), Confirm: true, Group: "Maintenance",
			ConfirmText: "Start compaction? CouchDB rewrites the database file in the background.",
			OnSuccess:   &plugin.ActionSuccess{SelectTab: "db_overview"},
		},
		{
			ID: rid("database.viewcleanup"), Label: "Clean up views", Icon: icon("brush-cleaning"), RouteID: rid("database.viewcleanup"),
			Params: dbParams(), Confirm: true, Group: "Maintenance",
			ConfirmText: "Delete index files for design documents that no longer exist?",
		},
		{
			ID: rid("document.create"), Label: "New document", Icon: icon("file-plus"), RouteID: rid("document.create"),
			Params: dbParams(), Open: plugin.OpenDialog, Panel: plugin.PanelCodeEditor,
			OnSuccess: &plugin.ActionSuccess{SelectTab: "documents"},
			Config: plugin.CodeEditorConfig{
				Language:       "json",
				InitialContent: "{\n  \"_id\": \"\",\n  \"type\": \"example\"\n}",
				SaveRouteID:    rid("document.create"), SaveMethod: plugin.MethodPost, SaveParams: dbParams(),
				SaveDismiss: plugin.SaveDismissClose,
				SaveToast:   &plugin.SaveToast{Summary: "Document created", Detail: "${response.id}", Severity: plugin.SeveritySuccess},
			},
		},
		{
			ID: rid("documents.bulk"), Label: "Bulk update", Icon: icon("layers"), RouteID: rid("documents.bulk"),
			Params: dbParams(), Open: plugin.OpenDialog, Panel: plugin.PanelCodeEditor,
			OnSuccess: &plugin.ActionSuccess{SelectTab: "documents"},
			Config: plugin.CodeEditorConfig{
				Language:       "json",
				InitialContent: "{\n  \"docs\": [\n    {\"_id\": \"example\", \"value\": 1}\n  ]\n}",
				SaveRouteID:    rid("documents.bulk"), SaveMethod: plugin.MethodPost, SaveParams: dbParams(),
				SaveDismiss: plugin.SaveDismissClose,
				SaveToast:   &plugin.SaveToast{Summary: "Bulk update submitted", Severity: plugin.SeveritySuccess},
			},
		},
		{
			ID: rid("document.delete"), Label: "Delete", Icon: icon("trash-2"), RouteID: rid("document.delete"),
			Params:  map[string]string{"db": "${resource.name}", "docid": "${record.id}"},
			Body:    map[string]any{"rev": "${record.rev}"},
			Confirm: true, ConfirmText: "Delete the selected document(s)? CouchDB writes a deletion tombstone.",
			Bulk: true,
		},
		{
			ID: rid("document.delete.current"), Label: "Delete", Icon: icon("trash-2"), RouteID: rid("document.delete"),
			Params:  docParams(),
			Confirm: true, ConfirmText: "Delete this document? CouchDB writes a deletion tombstone.",
			OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList},
		},
		{
			ID: rid("document.purge"), Label: "Purge", Icon: icon("eraser"), RouteID: rid("document.purge"),
			Params: docParams(), Confirm: true, Group: "Danger zone",
			ConfirmText: "Purge removes the document and all of its revisions from this node. Replicas that already saw it may resurrect it. Continue?",
			OnSuccess:   &plugin.ActionSuccess{Navigate: plugin.NavigateList},
		},
		{
			ID: rid("document.resolve"), Label: "Resolve conflicts", Icon: icon("git-merge"), RouteID: rid("document.resolve"),
			Params:  map[string]string{"db": "${resource.name}", "docid": "${record.id}"},
			Confirm: true, ConfirmText: "Keep one revision and delete every other conflicting revision of this document?",
		},
		{
			ID: rid("document.resolve.current"), Label: "Resolve conflicts", Icon: icon("git-merge"), RouteID: rid("document.resolve"),
			Params:  docParams(),
			Confirm: true, ConfirmText: "Keep one revision and delete every other conflicting revision of this document?",
			OnSuccess: &plugin.ActionSuccess{SelectTab: "doc_conflicts"},
		},
		{
			ID: rid("attachment.delete"), Label: "Delete", Icon: icon("trash-2"), RouteID: rid("attachment.delete"),
			Params: map[string]string{
				"db": "${resource.namespace}", "docid": "${resource.name}", "name": "${record.name}",
			},
			Confirm: true, ConfirmText: "Delete this attachment? The document keeps every other field and gains a new revision.",
			Bulk: true,
		},
		{
			ID: rid("index.create"), Label: "Create index", Icon: icon("list-plus"), RouteID: rid("index.create"),
			Params: dbParams(),
		},
		{
			ID: rid("index.delete"), Label: "Delete", Icon: icon("trash-2"), RouteID: rid("index.delete"),
			Params:  map[string]string{"db": "${resource.name}", "ddoc": "${record.ddoc}", "name": "${record.name}"},
			Confirm: true, ConfirmText: "Delete this Mango index? Queries relying on it will fall back to a full scan.",
			Bulk: true,
		},
		{
			ID: rid("mango.explain"), Label: "Explain query", Icon: icon("scan-search"), RouteID: rid("mango.explain"),
			Params: dbParams(), Group: "Analyze",
		},
		{
			ID: rid("designdoc.create"), Label: "New design doc", Icon: icon("file-plus-2"), RouteID: rid("designdoc.create"),
			Params: dbParams(), Open: plugin.OpenDialog, Panel: plugin.PanelCodeEditor,
			OnSuccess: &plugin.ActionSuccess{SelectTab: "design"},
			Config: plugin.CodeEditorConfig{
				Language: "json",
				InitialContent: "{\n  \"_id\": \"_design/example\",\n  \"language\": \"javascript\",\n  \"views\": {\n" +
					"    \"by_type\": {\n      \"map\": \"function (doc) { if (doc.type) { emit(doc.type, 1); } }\",\n      \"reduce\": \"_count\"\n    }\n  }\n}",
				SaveRouteID: rid("designdoc.create"), SaveMethod: plugin.MethodPost, SaveParams: dbParams(),
				SaveDismiss: plugin.SaveDismissClose,
				SaveToast:   &plugin.SaveToast{Summary: "Design document created", Detail: "${response.id}", Severity: plugin.SeveritySuccess},
			},
		},
		{
			ID: rid("designdoc.delete"), Label: "Delete", Icon: icon("trash-2"), RouteID: rid("designdoc.delete"),
			Params:  map[string]string{"db": "${resource.name}", "ddoc": "${record.name}"},
			Confirm: true, ConfirmText: "Delete this design document and drop its view indexes?",
			Bulk: true,
		},
		{
			ID: rid("designdoc.delete.current"), Label: "Delete", Icon: icon("trash-2"), RouteID: rid("designdoc.delete"),
			Params:  ddocParams(),
			Confirm: true, ConfirmText: "Delete this design document and drop its view indexes?",
			OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList},
		},
		{
			ID: rid("replication.create"), Label: "New replication", Icon: icon("refresh-cw"),
			RouteID: rid("replication.create"),
		},
		{
			ID: rid("replication.delete"), Label: "Delete", Icon: icon("trash-2"), RouteID: rid("replication.delete"),
			Params: replicationParams(), Confirm: true,
			ConfirmText: "Delete this replication document? A running job stops immediately.",
			OnSuccess:   &plugin.ActionSuccess{Navigate: plugin.NavigateList}, Bulk: true,
		},
		{
			ID: rid("query.save"), Label: "Save query", Icon: icon("bookmark-plus"), RouteID: rid("query.save"),
			Params: dbParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "saved"},
		},
		{
			ID: rid("query.delete"), Label: "Delete", Icon: icon("trash-2"), RouteID: rid("query.delete"),
			Params:  map[string]string{"key": "${record.key}"},
			Confirm: true, ConfirmText: "Delete this saved query?", Bulk: true,
		},
		{
			ID: rid("fauxton.open"), Label: "Open Fauxton", Icon: icon("external-link"),
			RouteID: rid("fauxton.url"), Open: plugin.OpenURL,
		},
	}
}
