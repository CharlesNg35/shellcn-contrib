// Package scylladb implements the ScyllaDB protocol plugin.
package scylladb

import (
	"context"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type Plugin struct{}

func New() *Plugin { return &Plugin{} }

const scyllaIconSVG = `<svg height="2500" viewBox="53.6 9 219.8 272.2" width="1579" xmlns="http://www.w3.org/2000/svg"><path d="m162.6 11.3c-62.7 0-107.3 39.6-107.3 95.7 0 26.9 28 158 28 158 1.8 7.8 9.4 13.1 17.4 11.8 1.8-.3 3.4-.9 4.9-1.7l1.2-.1c2.8 1.6 6 2.4 9.5 2 2.8-.3 5.3-1.4 7.4-3l3.4-.2c2.8 2.2 6.4 3.4 10.2 3.2 3.9-.2 7.3-1.9 9.9-4.4l6-.4c2.7 2.8 6.4 4.7 10.6 4.8 4.5.1 8.5-1.7 11.4-4.7l6.2.4c2.3 2.3 5.4 3.8 8.9 4.2 4.1.5 8-.7 11-3.1l3.8.2c1.8 1.3 3.9 2.3 6.2 2.7 3.3.6 6.5.1 9.3-1.3 1.2.6 2.6 1 4 1.2 8 1.3 15.6-3.9 17.4-11.8 0 0 28-131.2 28-158-.2-55.8-44.7-95.5-107.4-95.5z" fill="#57d1e5"/><circle cx="127.9" cy="97.3" fill="#fff" r="47.9"/><path d="m127.9 147.7c-27.8 0-50.5-22.6-50.5-50.5s22.6-50.5 50.5-50.5 50.5 22.6 50.5 50.5-22.7 50.5-50.5 50.5zm0-95.7c-25 0-45.3 20.3-45.3 45.3s20.3 45.3 45.3 45.3 45.3-20.3 45.3-45.3-20.4-45.3-45.3-45.3z" fill="#3a2d55"/><path d="m137.8 65.9c-8.3 1.7-9.9 19.8-5 24.8 3.3 3.3 9.9 4.3 9.9 6.6s-6.6 3.3-9.9 6.6c-5 5-3.3 23.1 5 24.8 9.8 2 24.8-9.9 24.8-31.4s-15-33.4-24.8-31.4z" fill="#3a2d55"/><path d="m104.4 162.5c1.7 5 7 14.4 13.6 12.7 3.2-.8 5-5 5-11.6m24.7-5.3c1.7 6.6 3.3 11.6 8.3 11.6s11.6-6.6 9.9-18.2" fill="#fff"/><g fill="#3a2d55"><path d="m189.3 135.5c-2.1-2.3-5-4-8.2-4.6h-.4c-1.4-.1-2.7 1-2.8 2.4s1 2.7 2.4 2.8 2.8.5 4 1.2a126 126 0 0 1 -41.8 18.3c-15.5 3.7-31.6 4.4-47.4 2.1-1.2-.2-2.3.6-2.5 1.8s.6 2.3 1.8 2.5c2.4.4 4.9.7 7.3.9 0 .1 0 .2.1.3 2.1 6.3 7.5 14.7 14.5 14.7.7 0 1.4-.1 2.2-.3 2.6-.6 6.9-3.3 6.9-14.1 0-.2-.1-.3-.1-.5 6.1-.5 12.2-1.5 18.2-2.8.6-.1 1.2-.3 1.7-.4 1.4 5.6 3.5 12.6 10.5 12.6 2.9 0 6-1.6 8.4-4.3 2-2.3 5.2-7.5 4.2-16.2 6.9-3.1 13.5-6.8 19.7-11 .6 1 1.1 2.1 1.3 3.3.2.9 1 1.5 1.9 1.4 1-.1 1.7-.9 1.6-1.9-.1-2.9-1.4-5.8-3.5-8.2zm-69 28.1c0 5-1.2 8.6-3 9-3.9 1-7.9-4.8-9.8-9.2 4.3.2 8.5.2 12.8 0 .1.1 0 .2 0 .2zm40.1 1.2c-1.3 1.5-3 2.5-4.4 2.5-2.4 0-3.8-2.1-5.5-8.7 4.4-1.3 8.8-2.8 13-4.5.2 4.3-.9 8.1-3.1 10.7z"/><path d="m242.4 36.5c-19.9-17.7-47.9-27.5-78.9-27.5s-59 9.8-78.9 27.5c-20 17.8-31 43-31 70.8s26.9 153.2 28 158.6c1.9 8.3 9.2 14 17.4 14 1 0 1.9-.1 2.9-.2 1.7-.3 3.3-.8 4.9-1.6 3.4 1.5 7.2 2.1 10.9 1.4 2.8-.5 5.4-1.7 7.6-3.5 1.9 1.5 4.2 2.7 6.6 3.3 3.9 1 8.1.7 11.8-1 2.6-1.2 4.8-3 6.5-5.1 1.3 1.6 2.8 3 4.6 4.1 1.4.9 2.9 1.5 4.5 2s3.2.7 4.8.7 3.3-.3 4.8-.7c1.6-.5 3.1-1.1 4.5-2s2.7-1.9 3.8-3.1c.3-.3.6-.6.8-.9 2.8 3.4 6.9 5.8 11.3 6.4 4.4.7 9-.4 12.6-2.9.4-.2.7-.5 1-.8 2.1 1.7 4.6 2.9 7.2 3.4 1.9.4 3.8.5 5.7.3 1.7-.2 3.3-.6 4.9-1.3 1.4.6 2.8 1 4.3 1.3 9.3 1.5 18.3-4.5 20.3-13.8 1.1-5.4 28-131.7 28-158.6.1-27.8-10.9-52.9-30.9-70.8zm-2.1 228.3c-1.4 6.3-7.4 10.5-13.8 9.8 1.2-1.2 2.3-2.6 3.1-4.1.9-1.7 1.6-3.5 1.9-5.3l.8-4.8 3.1-19.1c1-6.4 2-12.8 2.8-19.2.1-.9-.5-1.7-1.3-1.9-.9-.2-1.9.4-2.1 1.3-1.4 6.3-2.7 12.7-4 19l-3.7 19-.9 4.7c-.3 1.3-.8 2.6-1.4 3.7-.6 1.2-1.5 2.2-2.5 3.1s-2.1 1.7-3.3 2.2-2.5.9-3.8 1.1-2.7 0-4-.2c-1.7-.3-3.3-1.1-4.7-2 1.2-1.8 2.2-3.8 2.7-6 .2-.5.2-1.1.3-1.6 0-.3.1-.6.1-.8l.1-.7.3-2.8.6-5.6 1.1-11.2c.7-7.5 1.4-15 1.9-22.5.1-.9-.6-1.7-1.5-1.8s-1.8.5-2 1.5c-1.1 7.4-2.1 14.9-3.1 22.3l-1.4 11.2-.7 5.6c-.2 1.8-.4 3.9-.8 5.2-.7 3-2.7 5.7-5.3 7.4s-5.8 2.4-8.8 1.9c-3.1-.4-5.9-2.1-7.9-4.5-.5-.6-.9-1.2-1.3-1.9.3-.8.5-1.6.7-2.3.4-1.6.4-3.3.4-4.6l.1-8.3c.1-5.5.2-11.1.2-16.6.1-5.5.1-11.1 0-16.6 0-.9-.7-1.7-1.6-1.7-1-.1-1.8.7-1.8 1.6-.3 5.5-.7 11.1-.9 16.6-.3 5.5-.5 11.1-.8 16.6l-.4 8.3c0 .7 0 1.4-.1 2 0 .6-.1 1.1-.2 1.7-.3 1.1-.6 2.2-1.2 3.2-2.1 4.1-6.6 6.8-11.2 6.7-1.1 0-2.3-.1-3.4-.5-1.1-.3-2.1-.8-3.1-1.3-2-1.2-3.6-2.9-4.7-4.9-.6-1-.9-2.1-1.2-3.2s-.3-2.2-.4-3.7l-.4-8.3c-.2-5.5-.5-11.1-.8-16.6s-.6-11.1-.9-16.6c-.1-.9-.8-1.6-1.7-1.6-1 0-1.7.8-1.7 1.7v16.6c0 5.5.1 11.1.2 16.6l.1 8.3c0 1.3.1 3 .4 4.6.2.8.4 1.6.7 2.4-1.4 2.3-3.4 4.2-5.9 5.3-2.6 1.2-5.5 1.4-8.2.7s-5.2-2.3-6.9-4.6c-.9-1.1-1.6-2.4-2-3.7-.2-.7-.4-1.4-.5-2.1-.1-.3-.1-.8-.2-1.2l-.2-1.3-2.6-20.3c-.9-6.8-1.8-13.6-2.9-20.3-.1-.9-.9-1.5-1.8-1.5-1 .1-1.7.9-1.6 1.9.5 6.8 1.1 13.6 1.7 20.4l2 20.4.1 1.3c0 .4.1.8.2 1.4.1 1 .4 2 .7 2.9.5 1.7 1.4 3.3 2.4 4.8-1.5 1-3.2 1.8-5 2.1-2.6.5-5.3.1-7.7-1-2.4-1.2-4.4-3-5.7-5.4-.6-1.2-1.1-2.4-1.4-3.7l-.9-4.7-3.7-18.9c-1.3-6.3-2.6-12.6-4-18.9-.2-.9-1-1.5-1.9-1.3-.9.1-1.6 1-1.5 2 .9 6.4 1.8 12.7 2.8 19.1l3.1 19 .8 4.8c.3 1.9 1 3.7 1.8 5.3.8 1.5 1.8 2.9 3.1 4.1-.2 0-.3.1-.5.1-6.6 1.1-12.9-3.2-14.4-9.7-.3-1.3-27.9-131.2-27.9-157.5s10.4-50.1 29.2-67c18.9-16.9 45.7-26.2 75.4-26.2s56.5 9.3 75.4 26.2c18.9 16.8 29.2 40.6 29.2 67 .6 26.3-27 156.2-27.3 157.4z"/></g></svg>`

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:          plugin.CurrentAPIVersion,
		Name:                protocolName,
		Version:             "0.1.0",
		Title:               "ScyllaDB",
		Description:         "ScyllaDB cockpit: keyspaces and tables, editable CQL grids, a CQL console, a live token-ring and shard-per-core topology map, per-table compaction/cache/latency metrics, nodetool-style cluster status, and a CQL tracing viewer.",
		Icon:                plugin.Icon{Type: plugin.IconSVG, Value: scyllaIconSVG},
		Category:            plugin.CategoryDatabases,
		Config:              configSchema(),
		Capabilities:        []plugin.Capability{"cql", "schema", "tables", "query_editor", "cluster", "topology", "metrics", "tracing"},
		SupportedTransports: []plugin.Transport{plugin.TransportDirect},
		Layout:              plugin.LayoutSidebarTree,
		Tree:                tree(),
		Resources:           resources(),
		Actions:             actions(),
		Streams: []plugin.Stream{
			{ID: "scylladb.query", Kind: plugin.StreamQuery, RouteID: "scylladb.query"},
			{ID: "scylladb.cluster.metrics", Kind: plugin.StreamMetrics, RouteID: "scylladb.cluster.metrics"},
			{ID: "scylladb.table.metrics", Kind: plugin.StreamMetrics, RouteID: "scylladb.table.metrics"},
			{ID: "scylladb.topology", Kind: plugin.StreamCanvas, RouteID: "scylladb.topology"},
			{ID: "scylladb.cluster.watch", Kind: plugin.StreamResource, RouteID: "scylladb.cluster.watch"},
			{ID: "scylladb.nodes.watch", Kind: plugin.StreamResource, RouteID: "scylladb.nodes.watch"},
			{ID: "scylladb.compactions.watch", Kind: plugin.StreamResource, RouteID: "scylladb.compactions.watch"},
		},
	}
}

func (p *Plugin) Routes() []plugin.Route { return routes() }

func (p *Plugin) Connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	return connect(ctx, cfg)
}

func icon(name string) plugin.Icon {
	return plugin.Icon{Type: plugin.IconLucide, Value: name}
}

var nodeStatusSeverities = map[string]plugin.Severity{
	"UN": plugin.SeveritySuccess,
	"UJ": plugin.SeverityWarn,
	"UL": plugin.SeverityWarn,
	"UM": plugin.SeverityWarn,
	"DN": plugin.SeverityDanger,
	"DL": plugin.SeverityDanger,
	"DJ": plugin.SeverityDanger,
	"?N": plugin.SeveritySecondary,
}

func tree() []plugin.TreeGroup {
	return []plugin.TreeGroup{
		{Key: "keyspaces", Label: "Keyspaces", Icon: icon("database"), Source: plugin.DataSource{RouteID: "scylladb.keyspaces.tree"}, Ref: &plugin.ResourceIdentity{Kind: "server", Name: "Keyspaces", UID: "server"}},
		{Key: "cluster", Label: "Cluster", Icon: icon("orbit"), Ref: &plugin.ResourceIdentity{Kind: "cluster", Name: "Cluster", UID: "cluster"}},
		{Key: "nodes", Label: "Nodes", Icon: icon("server"), Source: plugin.DataSource{RouteID: "scylladb.nodes.tree"}, ResourceKind: "node"},
		{Key: "traces", Label: "Tracing", Icon: icon("route"), ResourceKind: "trace"},
		{Key: "types", Label: "Types", Icon: icon("braces"), Source: plugin.DataSource{RouteID: "scylladb.types.tree"}, ResourceKind: "type"},
		{Key: "functions", Label: "Functions", Icon: icon("function-square"), Source: plugin.DataSource{RouteID: "scylladb.functions.tree"}, ResourceKind: "function"},
	}
}

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		serverResource(),
		clusterResource(),
		keyspaceResource(),
		tableResource(),
		viewResource(),
		typeResource(),
		functionResource(),
		nodeResource(),
		traceResource(),
	}
}

func objectDetail(sections ...plugin.ObjectDetailSection) plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{Sections: sections, RawToggle: true}
}

func field(key, label string) plugin.ObjectDetailField {
	return plugin.ObjectDetailField{Key: key, Label: label}
}

func typedField(key, label string, t plugin.ColumnType) plugin.ObjectDetailField {
	return plugin.ObjectDetailField{Key: key, Label: label, Type: t}
}

// serverResource is the connection-level view opened by the Keyspaces tree
// group: the keyspace list, a CQL console, and the user's saved CQL.
func serverResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "server", Title: "Keyspaces",
		List:    plugin.DataSource{RouteID: "scylladb.keyspaces.list"},
		Columns: keyspaceColumns(),
		Actions: plugin.ResourceActions{Toolbar: []string{"scylladb.keyspace.create"}},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "Keyspaces"},
			Tabs: []plugin.Panel{
				{Key: "keyspaces", Label: "Keyspaces", Icon: icon("database"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "scylladb.keyspaces.list"}, Config: plugin.TableConfig{Columns: keyspaceColumns(), ActionIDs: []string{"scylladb.keyspace.create"}, RowActionIDs: []string{"scylladb.keyspace.drop"}, DefaultSort: &plugin.SortKey{Field: "name"}, EmptyText: "No user keyspaces yet.", RefreshIntervalMs: 30000, Exportable: true}},
				{Key: "console", Label: "CQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "scylladb.query", Method: plugin.MethodWS}, Config: queryConfig("SELECT keyspace_name, durable_writes, replication FROM system_schema.keyspaces LIMIT 25;")},
				{Key: "saved", Label: "Saved CQL", Icon: icon("bookmark"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "scylladb.queries.list"}, Config: plugin.TableConfig{Columns: savedQueryColumns(), ActionIDs: []string{"scylladb.query.save"}, RowActionIDs: []string{"scylladb.query.delete"}, DefaultSort: &plugin.SortKey{Field: "name"}, EmptyText: "No saved CQL yet. Use Save CQL to keep a statement for this connection.", RefreshIntervalMs: 60000}},
			},
		},
	}
}

func clusterResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind:  "cluster",
		Title: "Cluster",
		List:  plugin.DataSource{RouteID: "scylladb.cluster.list"},
		Watch: &plugin.DataSource{RouteID: "scylladb.cluster.watch", Method: plugin.MethodWS},
		Columns: []plugin.Column{
			{Key: "cluster_name", Label: "Cluster", Sortable: true},
			{Key: "release_version", Label: "ScyllaDB"},
			{Key: "nodes_up", Label: "Nodes up", Type: plugin.ColumnNumber},
			{Key: "nodes_total", Label: "Nodes", Type: plugin.ColumnNumber},
			{Key: "shards_total", Label: "Shards", Type: plugin.ColumnNumber},
		},
		Actions: plugin.ResourceActions{Detail: []string{"scylladb.trace.run"}},
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.name}", StatusField: "health", Severities: map[string]plugin.Severity{"healthy": plugin.SeveritySuccess, "degraded": plugin.SeverityWarn, "down": plugin.SeverityDanger}},
			DefaultTab: "overview",
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("layout-dashboard"), Type: plugin.PanelDashboard, Config: plugin.DashboardConfig{Cells: []plugin.Panel{
					{Key: "cluster-live", Label: "Live", Icon: icon("activity"), Span: 2, Type: plugin.PanelMetrics, Source: &plugin.DataSource{RouteID: "scylladb.cluster.metrics", Method: plugin.MethodWS}, Config: clusterMetricsConfig()},
					{Key: "cluster-status", Label: "Cluster status", Icon: icon("server"), Span: 2, Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "scylladb.nodes.list"}, Config: plugin.TableConfig{Columns: nodeColumns(), DefaultSort: &plugin.SortKey{Field: "address"}, EmptyText: "No nodes reported by system.cluster_status.", Watch: &plugin.DataSource{RouteID: "scylladb.nodes.watch", Method: plugin.MethodWS}}},
					{Key: "cluster-facts", Label: "Cluster", Icon: icon("info"), Span: 2, Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "scylladb.cluster.overview"}, Config: clusterOverviewConfig()},
				}}},
				{Key: "topology", Label: "Token ring", Icon: icon("orbit"), Type: plugin.PanelCanvas, Source: &plugin.DataSource{RouteID: "scylladb.topology", Method: plugin.MethodWS}, Config: topologyCanvasConfig()},
				{Key: "nodes", Label: "Nodes", Icon: icon("server"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "scylladb.nodes.list"}, Config: plugin.TableConfig{Columns: nodeColumns(), DefaultSort: &plugin.SortKey{Field: "address"}, EmptyText: "No nodes reported by system.cluster_status.", Watch: &plugin.DataSource{RouteID: "scylladb.nodes.watch", Method: plugin.MethodWS}, Exportable: true}},
				{Key: "compactions", Label: "Compactions", Icon: icon("layers"), Type: plugin.PanelTimeline, Source: &plugin.DataSource{RouteID: "scylladb.compactions.list"}, Config: compactionTimelineConfig()},
				{Key: "clients", Label: "Clients", Icon: icon("plug"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "scylladb.clients.list"}, Config: plugin.TableConfig{Columns: clientColumns(), DefaultSort: &plugin.SortKey{Field: "address"}, EmptyText: "No CQL clients connected.", RefreshIntervalMs: 10000}},
				{Key: "slowlog", Label: "Slow queries", Icon: icon("timer"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "scylladb.slowlog.list"}, Config: plugin.TableConfig{Columns: slowLogColumns(), DefaultSort: &plugin.SortKey{Field: "duration_us", Desc: true}, EmptyText: "No slow queries recorded. Enable the slow-query log on the node to populate system_traces.node_slow_log.", RefreshIntervalMs: 30000, Exportable: true}},
				{Key: "config", Label: "Configuration", Icon: icon("sliders-horizontal"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "scylladb.config.list"}, Config: plugin.TableConfig{Columns: configColumns(), DefaultSort: &plugin.SortKey{Field: "name"}, EmptyText: "No configuration reported by system.config.", RefreshIntervalMs: 60000, Exportable: true}},
				{Key: "console", Label: "CQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "scylladb.query", Method: plugin.MethodWS}, Config: queryConfig("SELECT peer, dc, rack, status, up, load, owns, tokens FROM system.cluster_status LIMIT 25;")},
			},
		},
	}
}

func keyspaceResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind:    "keyspace",
		Title:   "Keyspaces",
		List:    plugin.DataSource{RouteID: "scylladb.keyspaces.list"},
		Columns: keyspaceColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{"scylladb.keyspace.create"},
			Row:     []string{"scylladb.keyspace.drop"},
			Detail:  []string{"scylladb.keyspace.drop"},
		},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "scylladb.keyspace.overview", Params: map[string]string{"keyspace": "${resource.uid}"}}, Config: keyspaceOverviewConfig()},
			{Key: "tables", Label: "Tables", Icon: icon("table-2"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "scylladb.tables.list", Params: map[string]string{"keyspace": "${resource.uid}"}}, Config: plugin.TableConfig{Columns: tableColumns(), ActionIDs: []string{"scylladb.table.create"}, RowActionIDs: []string{"scylladb.table.truncate", "scylladb.table.drop"}, DefaultSort: &plugin.SortKey{Field: "name"}, EmptyText: "No tables in this keyspace.", RefreshIntervalMs: 30000, Exportable: true}},
			{Key: "views", Label: "Materialized views", Icon: icon("panel-top"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "scylladb.views.list", Params: map[string]string{"keyspace": "${resource.uid}"}}, Config: plugin.TableConfig{Columns: viewColumns(), RowActionIDs: []string{"scylladb.view.drop"}, DefaultSort: &plugin.SortKey{Field: "name"}, EmptyText: "No materialized views in this keyspace.", RefreshIntervalMs: 30000}},
			{Key: "ring", Label: "Token ranges", Icon: icon("circle-dot"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "scylladb.tokenring.list", Params: map[string]string{"keyspace": "${resource.uid}"}}, Config: plugin.TableConfig{Columns: tokenRingColumns(), DefaultSort: &plugin.SortKey{Field: "start_token"}, EmptyText: "No token ranges reported for this keyspace.", RefreshIntervalMs: 60000, Exportable: true}},
			{Key: "types", Label: "Types", Icon: icon("braces"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "scylladb.types.list", Params: map[string]string{"keyspace": "${resource.uid}"}}, Config: plugin.TableConfig{Columns: typeColumns(), ActionIDs: []string{"scylladb.type.create"}, RowActionIDs: []string{"scylladb.type.drop"}, DefaultSort: &plugin.SortKey{Field: "name"}, EmptyText: "No user-defined types in this keyspace.", RefreshIntervalMs: 60000}},
			{Key: "functions", Label: "Functions", Icon: icon("function-square"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "scylladb.functions.list", Params: map[string]string{"keyspace": "${resource.uid}"}}, Config: plugin.TableConfig{Columns: functionColumns(), DefaultSort: &plugin.SortKey{Field: "name"}, EmptyText: "No user-defined functions in this keyspace.", RefreshIntervalMs: 60000}},
			{Key: "query", Label: "CQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "scylladb.query", Method: plugin.MethodWS}, Config: queryConfig("SELECT table_name, comment, compaction, default_time_to_live FROM system_schema.tables WHERE keyspace_name = '${resource.name}' LIMIT 25;")},
		}},
	}
}

func tableResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "table", Title: "Tables",
		List:    plugin.DataSource{RouteID: "scylladb.tables.list"},
		Columns: tableColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{"scylladb.table.truncate", "scylladb.table.drop"},
			Detail: []string{"scylladb.partition.locate", "scylladb.trace.run", "scylladb.table.truncate", "scylladb.table.drop"},
		},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.namespace}.${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "data", Label: "Data", Icon: icon("table-properties"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "scylladb.table.rows", Params: tableParams()}, Config: dataGridConfig()},
			{Key: "columns", Label: "Columns", Icon: icon("columns-3"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "scylladb.table.columns", Params: tableParams()}, Config: plugin.TableConfig{Columns: columnColumns(), ActionIDs: []string{"scylladb.column.add"}, RowActionIDs: []string{"scylladb.column.drop"}, EmptyText: "No columns.", RefreshIntervalMs: 60000}},
			{Key: "indexes", Label: "Indexes", Icon: icon("list-tree"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "scylladb.table.indexes", Params: tableParams()}, Config: plugin.TableConfig{Columns: indexColumns(), ActionIDs: []string{"scylladb.index.create"}, RowActionIDs: []string{"scylladb.index.drop"}, DefaultSort: &plugin.SortKey{Field: "index_name"}, EmptyText: "No secondary indexes on this table.", RefreshIntervalMs: 60000}},
			{Key: "metrics", Label: "Live", Icon: icon("activity"), Type: plugin.PanelMetrics, Source: &plugin.DataSource{RouteID: "scylladb.table.metrics", Method: plugin.MethodWS, Params: tableParams()}, Config: tableMetricsConfig()},
			{Key: "storage", Label: "Storage", Icon: icon("hard-drive"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "scylladb.table.estimates", Params: tableParams()}, Config: plugin.TableConfig{Columns: estimateColumns(), DefaultSort: &plugin.SortKey{Field: "partitions_count", Desc: true}, EmptyText: "No size estimates for this table yet; they are refreshed periodically by the node.", RefreshIntervalMs: 60000, Exportable: true}},
			{Key: "large", Label: "Large partitions", Icon: icon("triangle-alert"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "scylladb.table.large", Params: tableParams()}, Config: plugin.TableConfig{Columns: largePartitionColumns(), DefaultSort: &plugin.SortKey{Field: "partition_size", Desc: true}, EmptyText: "No partitions crossed the large-partition threshold.", RefreshIntervalMs: 60000, Exportable: true}},
			{Key: "compactions", Label: "Compactions", Icon: icon("layers"), Type: plugin.PanelTimeline, Source: &plugin.DataSource{RouteID: "scylladb.compactions.list", Params: tableParams()}, Config: compactionTimelineConfig()},
			{Key: "definition", Label: "Definition", Icon: icon("code"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: "scylladb.table.definition", Params: tableParams()}},
			{Key: "query", Label: "CQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "scylladb.query", Method: plugin.MethodWS}, Config: queryConfig("SELECT * FROM \"${resource.namespace}\".\"${resource.name}\" LIMIT 25;")},
		}},
	}
}

func viewResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "view", Title: "Materialized views",
		List:    plugin.DataSource{RouteID: "scylladb.views.list"},
		Columns: viewColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{"scylladb.view.drop"},
			Detail: []string{"scylladb.view.drop"},
		},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.namespace}.${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "columns", Label: "Columns", Icon: icon("columns-3"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "scylladb.table.columns", Params: tableParams()}, Config: plugin.TableConfig{Columns: columnColumns(), EmptyText: "No columns.", RefreshIntervalMs: 60000}},
			{Key: "definition", Label: "Definition", Icon: icon("code"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: "scylladb.view.definition", Params: tableParams()}},
			{Key: "query", Label: "CQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "scylladb.query", Method: plugin.MethodWS}, Config: queryConfig("SELECT * FROM \"${resource.namespace}\".\"${resource.name}\" LIMIT 25;")},
		}},
	}
}

func typeResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "type", Title: "Types",
		List:    plugin.DataSource{RouteID: "scylladb.types.list"},
		Columns: typeColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{"scylladb.type.drop"},
			Detail: []string{"scylladb.type.drop"},
		},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.namespace}.${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "type-overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "scylladb.type.overview", Params: map[string]string{"keyspace": "${resource.namespace}", "name": "${resource.name}"}}, Config: objectDetail(plugin.ObjectDetailSection{Title: "Type", Fields: []plugin.ObjectDetailField{
				{Key: "name", Label: "Name", Copy: true},
				field("keyspace", "Keyspace"),
				field("fields", "Fields"),
			}})},
		}},
	}
}

func functionResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "function", Title: "Functions",
		List:    plugin.DataSource{RouteID: "scylladb.functions.list"},
		Columns: functionColumns(),
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.namespace}.${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "function-overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "scylladb.function.overview", Params: map[string]string{"id": "${resource.uid}"}}, Config: objectDetail(plugin.ObjectDetailSection{Title: "Function", Fields: []plugin.ObjectDetailField{
				{Key: "name", Label: "Name", Copy: true},
				field("keyspace", "Keyspace"),
				field("arguments", "Arguments"),
				field("return_type", "Returns"),
				field("language", "Language"),
				field("body", "Body"),
			}})},
		}},
	}
}

func nodeResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "node", Title: "Nodes",
		List:    plugin.DataSource{RouteID: "scylladb.nodes.list"},
		Watch:   &plugin.DataSource{RouteID: "scylladb.nodes.watch", Method: plugin.MethodWS},
		Columns: nodeColumns(),
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}", StatusField: "status", Severities: nodeStatusSeverities}, Tabs: []plugin.Panel{
			{Key: "node-overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "scylladb.node.overview", Params: map[string]string{"address": "${resource.uid}"}}, Config: nodeOverviewConfig()},
			{Key: "node-runtime", Label: "Runtime", Icon: icon("gauge"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "scylladb.node.runtime", Params: map[string]string{"address": "${resource.uid}"}}, Config: plugin.TableConfig{Columns: runtimeColumns(), DefaultSort: &plugin.SortKey{Field: "group"}, EmptyText: "No runtime counters reported by system.runtime_info.", RefreshIntervalMs: 5000, Exportable: true}},
			{Key: "node-clients", Label: "Clients", Icon: icon("plug"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "scylladb.clients.list", Params: map[string]string{"address": "${resource.uid}"}}, Config: plugin.TableConfig{Columns: clientColumns(), DefaultSort: &plugin.SortKey{Field: "shard_id"}, EmptyText: "No CQL clients connected to this node.", RefreshIntervalMs: 10000}},
		}},
	}
}

func traceResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "trace", Title: "CQL traces",
		List:    plugin.DataSource{RouteID: "scylladb.traces.list"},
		Columns: traceColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{"scylladb.trace.run"},
			Detail:  []string{"scylladb.trace.run"},
		},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, DefaultTab: "trace-events", Tabs: []plugin.Panel{
			{Key: "trace-events", Label: "Timeline", Icon: icon("route"), Type: plugin.PanelTimeline, Source: &plugin.DataSource{RouteID: "scylladb.trace.events", Params: map[string]string{"id": "${resource.uid}"}}, Config: plugin.TimelineConfig{
				TimestampField: "time",
				TitleField:     "activity",
				BodyField:      "detail",
				SeverityField:  "severity",
				IconField:      "icon",
				ResourceField:  "source",
				EmptyText:      "No trace events recorded for this session.",
			}},
			{Key: "trace-overview", Label: "Session", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "scylladb.trace.overview", Params: map[string]string{"id": "${resource.uid}"}}, Config: traceOverviewConfig()},
		}},
	}
}

func tableParams() map[string]string {
	return map[string]string{"keyspace": "${resource.namespace}", "table": "${resource.name}"}
}

// dataGridConfig makes the table Data tab an editable grid. RowKey is left empty
// so the renderer targets rows by the per-row _key (primary-key) map the rows
// handler attaches; tables without a usable key carry no _key and stay read-only.
func dataGridConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Editable:      true,
		StagedEdits:   true,
		Exportable:    true,
		EmptyText:     "No rows.",
		HiddenColumns: []string{"_key", tokenColumn},
		Insert:        &plugin.DataSource{RouteID: "scylladb.table.row.insert", Method: plugin.MethodPost, Params: tableParams()},
		Update:        &plugin.DataSource{RouteID: "scylladb.table.row.update", Method: plugin.MethodPatch, Params: tableParams()},
		Delete:        &plugin.DataSource{RouteID: "scylladb.table.row.delete", Method: plugin.MethodDelete, Params: tableParams()},
		ColumnsSource: &plugin.DataSource{RouteID: "scylladb.table.columns", Params: tableParams()},
	}
}

func queryConfig(initial string) plugin.QueryEditorConfig {
	return plugin.QueryEditorConfig{
		Language:          "sql",
		Label:             "CQL",
		ExecuteLabel:      "Run CQL",
		CancelLabel:       "Cancel",
		RunningLabel:      "Running...",
		EmptyText:         "Run a CQL statement to see results. Prefix a statement with TRACING ON; to capture a server-side trace.",
		InitialQuery:      initial,
		CancelRouteID:     "scylladb.query.cancel",
		CompletionRouteID: "scylladb.completion",
		Exportable:        true,
	}
}

func topologyCanvasConfig() plugin.CanvasConfig {
	return plugin.CanvasConfig{
		ScaleMode:      plugin.CanvasScaleResize,
		ResizeEvents:   true,
		Interactive:    true,
		Pointer:        true,
		Keyboard:       true,
		WheelMode:      plugin.CanvasWheelNone,
		HiDPI:          true,
		FocusOnPointer: true,
		AriaLabel:      "ScyllaDB token ring and shard-per-core map",
		Instructions:   "The outer ring shows token ranges coloured by owning node; the inner wheel shows the selected node's shards with their live client connections. Click a ring segment or press the left and right arrow keys to select a node; press Home to select the coordinator.",
	}
}

func clusterMetricsConfig() plugin.MetricsConfig {
	return plugin.MetricsConfig{
		Stats: []plugin.MetricStat{
			{Key: "nodesUp", Label: "Nodes up"},
			{Key: "shardsTotal", Label: "Shards"},
			{Key: "driverConnections", Label: "Driver connections"},
			{Key: "clientConnections", Label: "CQL clients"},
			{Key: "cacheRequestsPerSec", Label: "Cache reads", Unit: "/s"},
			{Key: "compactionBytesPerSec", Label: "Compaction write", Unit: "bytes/s"},
		},
		Usage: []plugin.MetricUsage{
			{Key: "memoryPct", Label: "Node memory", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
				PercentKey: "memoryPct", UsedKey: "memoryUsed", TotalKey: "memoryTotal",
				UsedType: plugin.ColumnBytes, TotalType: plugin.ColumnBytes, WarnAt: 80, CriticalAt: 95,
			}},
			{Key: "cachePct", Label: "Row cache", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
				PercentKey: "cachePct", UsedKey: "cacheUsed", TotalKey: "cacheTotal",
				UsedType: plugin.ColumnBytes, TotalType: plugin.ColumnBytes, WarnAt: 85, CriticalAt: 95,
			}},
			{Key: "storagePct", Label: "Storage", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
				PercentKey: "storagePct", UsedKey: "storageUsed", TotalKey: "storageTotal",
				UsedType: plugin.ColumnBytes, TotalType: plugin.ColumnBytes, WarnAt: 70, CriticalAt: 90,
			}},
		},
		Series: []plugin.MetricSeries{
			{Key: "cacheHitRate", Label: "Cache hit rate", Unit: "%"},
			{Key: "memoryPct", Label: "Memory", Unit: "%"},
			{Key: "probeLatencyMs", Label: "Client round-trip", Unit: "ms"},
			{Key: "coordinatorLatencyMs", Label: "Coordinator latency", Unit: "ms"},
		},
		History: 90,
	}
}

func tableMetricsConfig() plugin.MetricsConfig {
	return plugin.MetricsConfig{
		Stats: []plugin.MetricStat{
			{Key: "partitions", Label: "Partitions (est.)"},
			{Key: "meanPartitionSize", Label: "Mean partition", Unit: "bytes"},
			{Key: "estimatedBytes", Label: "Estimated data", Unit: "bytes"},
			{Key: "largePartitions", Label: "Large partitions"},
			{Key: "compactionsLastHour", Label: "Compactions / hour"},
			{Key: "compactionBytesPerSec", Label: "Compaction write", Unit: "bytes/s"},
		},
		Gauges: []plugin.MetricGauge{
			{Key: "compactionRatio", Label: "Compaction size ratio", Unit: "%"},
		},
		Series: []plugin.MetricSeries{
			{Key: "readLatencyMs", Label: "Client read latency", Unit: "ms"},
			{Key: "coordinatorLatencyMs", Label: "Coordinator latency", Unit: "ms"},
			{Key: "cacheHitRate", Label: "Node cache hit rate", Unit: "%"},
		},
		History: 90,
	}
}

func compactionTimelineConfig() plugin.TimelineConfig {
	return plugin.TimelineConfig{
		TimestampField: "compacted_at",
		TitleField:     "title",
		BodyField:      "detail",
		SeverityField:  "severity",
		IconField:      "icon",
		ResourceField:  "target",
		EmptyText:      "No compactions recorded yet.",
		Watch:          &plugin.DataSource{RouteID: "scylladb.compactions.watch", Method: plugin.MethodWS},
	}
}

func clusterOverviewConfig() plugin.ObjectDetailConfig {
	cfg := objectDetail(
		plugin.ObjectDetailSection{Title: "Cluster", Fields: []plugin.ObjectDetailField{
			{Key: "cluster_name", Label: "Cluster name", Copy: true},
			field("release_version", "ScyllaDB version"),
			field("build_mode", "Build"),
			field("cql_version", "CQL version"),
			field("partitioner", "Partitioner"),
			field("snitch", "Snitch"),
			typedField("tablets_enabled", "Tablets", plugin.ColumnBool),
		}},
		plugin.ObjectDetailSection{Title: "Topology", Fields: []plugin.ObjectDetailField{
			typedField("nodes_total", "Nodes", plugin.ColumnNumber),
			typedField("nodes_up", "Nodes up", plugin.ColumnNumber),
			typedField("shards_total", "Shards (cores)", plugin.ColumnNumber),
			typedField("datacenters", "Datacenters", plugin.ColumnNumber),
			typedField("racks", "Racks", plugin.ColumnNumber),
			typedField("tokens_total", "Tokens", plugin.ColumnNumber),
			field("sharding_algorithm", "Sharding algorithm"),
			typedField("schema_agreement", "Schema agreement", plugin.ColumnBool),
		}},
		plugin.ObjectDetailSection{Title: "Driver", Fields: []plugin.ObjectDetailField{
			typedField("driver_connections", "Connections", plugin.ColumnNumber),
			typedField("driver_shard_connections", "Shards covered", plugin.ColumnNumber),
			typedField("shard_aware_port", "Shard-aware port", plugin.ColumnNumber),
			typedField("shard_aware_enabled", "Shard-aware dialing", plugin.ColumnBool),
			typedField("in_flight", "In-flight requests", plugin.ColumnNumber),
		}},
	)
	cfg.Watch = &plugin.DataSource{RouteID: "scylladb.cluster.watch", Method: plugin.MethodWS}
	return cfg
}

func keyspaceOverviewConfig() plugin.ObjectDetailConfig {
	return objectDetail(plugin.ObjectDetailSection{Title: "Keyspace", Fields: []plugin.ObjectDetailField{
		{Key: "name", Label: "Name", Copy: true},
		field("replication", "Replication"),
		typedField("durable_writes", "Durable writes", plugin.ColumnBool),
		typedField("tables", "Tables", plugin.ColumnNumber),
		typedField("views", "Materialized views", plugin.ColumnNumber),
		typedField("types", "User types", plugin.ColumnNumber),
		typedField("partitions", "Partitions (est.)", plugin.ColumnNumber),
		typedField("estimated_bytes", "Estimated data", plugin.ColumnBytes),
		typedField("token_ranges", "Token ranges", plugin.ColumnNumber),
	}})
}

func nodeOverviewConfig() plugin.ObjectDetailConfig {
	return objectDetail(
		plugin.ObjectDetailSection{Title: "Node", Fields: []plugin.ObjectDetailField{
			{Key: "address", Label: "Address", Copy: true},
			{Key: "host_id", Label: "Host ID", Copy: true},
			{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: nodeStatusSeverities},
			field("state", "State"),
			typedField("up", "Up", plugin.ColumnBool),
			field("data_center", "Datacenter"),
			field("rack", "Rack"),
			field("release_version", "Version"),
			field("schema_version", "Schema version"),
		}},
		plugin.ObjectDetailSection{Title: "Shards and ring", Fields: []plugin.ObjectDetailField{
			typedField("shards", "Shards (cores)", plugin.ColumnNumber),
			typedField("tokens", "Tokens", plugin.ColumnNumber),
			typedField("owns_pct", "Ring ownership", plugin.ColumnPercent),
			typedField("shard_aware_port", "Shard-aware port", plugin.ColumnNumber),
			field("sharding_algorithm", "Sharding algorithm"),
			typedField("driver_connections", "Driver connections", plugin.ColumnNumber),
		}},
		plugin.ObjectDetailSection{Title: "Load", Fields: []plugin.ObjectDetailField{
			typedField("load_bytes", "Load", plugin.ColumnBytes),
			{Key: "storage_pct", Label: "Storage", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
				PercentKey: "storage_pct", UsedKey: "storage_used", TotalKey: "storage_total",
				UsedType: plugin.ColumnBytes, TotalType: plugin.ColumnBytes, WarnAt: 70, CriticalAt: 90,
			}},
			typedField("tablets_allocated", "Tablets", plugin.ColumnNumber),
		}},
	)
}

func traceOverviewConfig() plugin.ObjectDetailConfig {
	return objectDetail(
		plugin.ObjectDetailSection{Title: "Session", Fields: []plugin.ObjectDetailField{
			{Key: "session_id", Label: "Session", Copy: true},
			field("request", "Request"),
			field("command", "Command"),
			{Key: "coordinator", Label: "Coordinator", Copy: true},
			typedField("started_at", "Started", plugin.ColumnDateTime),
			typedField("duration_us", "Duration", plugin.ColumnDuration),
		}},
		plugin.ObjectDetailSection{Title: "Payload", Fields: []plugin.ObjectDetailField{
			typedField("request_size", "Request size", plugin.ColumnBytes),
			typedField("response_size", "Response size", plugin.ColumnBytes),
			field("client", "Client"),
			field("username", "User"),
			typedField("events", "Events", plugin.ColumnNumber),
			typedField("parameters", "Parameters", plugin.ColumnJSON),
		}},
	)
}

func keyspaceColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Keyspace", Sortable: true},
		{Key: "replication", Label: "Replication"},
		{Key: "durable_writes", Label: "Durable writes", Type: plugin.ColumnBool, Sortable: true},
		{Key: "tables", Label: "Tables", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "views", Label: "Views", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "estimated_bytes", Label: "Data (est.)", Type: plugin.ColumnBytes, Sortable: true},
	}
}

func tableColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Table", Sortable: true},
		{Key: "keyspace", Label: "Keyspace", Sortable: true},
		{Key: "compaction_strategy", Label: "Compaction", Sortable: true},
		{Key: "caching", Label: "Caching"},
		{Key: "partitions", Label: "Partitions (est.)", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "estimated_bytes", Label: "Data (est.)", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "gc_grace_seconds", Label: "GC grace", Type: plugin.ColumnDuration, Sortable: true},
		{Key: "bloom_filter_fp_chance", Label: "Bloom FP", Type: plugin.ColumnNumber},
	}
}

func viewColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "View", Sortable: true},
		{Key: "keyspace", Label: "Keyspace", Sortable: true},
		{Key: "base_table", Label: "Base table", Sortable: true},
		{Key: "where_clause", Label: "Where"},
	}
}

func columnColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "column_name", Label: "Column", Sortable: true},
		{Key: "kind", Label: "Kind", Type: plugin.ColumnBadge, Sortable: true, Severities: map[string]plugin.Severity{
			"partition_key": plugin.SeverityDanger,
			"clustering":    plugin.SeverityWarn,
			"static":        plugin.SeverityInfo,
			"regular":       plugin.SeveritySecondary,
		}},
		{Key: "position", Label: "Position", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "type", Label: "Type"},
	}
}

func indexColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "index_name", Label: "Index", Sortable: true},
		{Key: "kind", Label: "Kind", Sortable: true},
		{Key: "target", Label: "Target"},
		{Key: "options", Label: "Options"},
	}
}

func typeColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Type", Sortable: true},
		{Key: "keyspace", Label: "Keyspace", Sortable: true},
		{Key: "fields", Label: "Fields"},
	}
}

func functionColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Function", Sortable: true},
		{Key: "keyspace", Label: "Keyspace", Sortable: true},
		{Key: "arguments", Label: "Arguments"},
		{Key: "return_type", Label: "Returns"},
		{Key: "language", Label: "Language", Sortable: true},
	}
}

func nodeColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "address", Label: "Address", Sortable: true},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Sortable: true, Severities: nodeStatusSeverities},
		{Key: "data_center", Label: "Datacenter", Sortable: true},
		{Key: "rack", Label: "Rack", Sortable: true},
		{Key: "shards", Label: "Shards", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "tokens", Label: "Tokens", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "owns_pct", Label: "Owns", Type: plugin.ColumnPercent, Sortable: true},
		{Key: "load_bytes", Label: "Load", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "release_version", Label: "Version"},
	}
}

func runtimeColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "group", Label: "Group", Sortable: true},
		{Key: "item", Label: "Counter", Sortable: true},
		{Key: "value", Label: "Value"},
	}
}

func clientColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "address", Label: "Client", Sortable: true},
		{Key: "node", Label: "Node", Sortable: true},
		{Key: "port", Label: "Port", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "shard_id", Label: "Shard", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "connection_stage", Label: "Stage", Type: plugin.ColumnBadge, Sortable: true, Severities: map[string]plugin.Severity{
			"READY":          plugin.SeveritySuccess,
			"AUTHENTICATING": plugin.SeverityWarn,
			"ESTABLISHED":    plugin.SeverityInfo,
		}},
		{Key: "driver_name", Label: "Driver", Sortable: true},
		{Key: "driver_version", Label: "Driver version"},
		{Key: "username", Label: "User", Sortable: true},
		{Key: "ssl_enabled", Label: "TLS", Type: plugin.ColumnBool},
		{Key: "scheduling_group", Label: "Scheduling group"},
	}
}

func configColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Parameter", Sortable: true},
		{Key: "value", Label: "Value"},
		{Key: "type", Label: "Type", Sortable: true},
		{Key: "source", Label: "Source", Type: plugin.ColumnBadge, Sortable: true, Severities: map[string]plugin.Severity{
			"default":  plugin.SeveritySecondary,
			"config":   plugin.SeverityInfo,
			"cli":      plugin.SeverityWarn,
			"cql":      plugin.SeverityWarn,
			"internal": plugin.SeveritySecondary,
		}},
	}
}

func tokenRingColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "start_token", Label: "Start token", Sortable: true},
		{Key: "end_token", Label: "End token", Sortable: true},
		{Key: "endpoint", Label: "Replica", Sortable: true},
		{Key: "shard", Label: "Shard", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "data_center", Label: "Datacenter", Sortable: true},
		{Key: "rack", Label: "Rack"},
	}
}

func estimateColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "range_start", Label: "Range start", Sortable: true},
		{Key: "range_end", Label: "Range end", Sortable: true},
		{Key: "partitions_count", Label: "Partitions", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "mean_partition_size", Label: "Mean partition", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "estimated_bytes", Label: "Estimated data", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "shard", Label: "Shard", Type: plugin.ColumnNumber, Sortable: true},
	}
}

func largePartitionColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "partition_key", Label: "Partition key", Sortable: true},
		{Key: "partition_size", Label: "Size", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "rows", Label: "Rows", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "dead_rows", Label: "Dead rows", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "range_tombstones", Label: "Range tombstones", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "sstable_name", Label: "SSTable"},
		{Key: "compaction_time", Label: "Detected", Type: plugin.ColumnDateTime, Sortable: true},
	}
}

func slowLogColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "start_time", Label: "Started", Type: plugin.ColumnDateTime, Sortable: true},
		{Key: "duration_us", Label: "Duration", Type: plugin.ColumnDuration, Sortable: true},
		{Key: "command", Label: "Statement"},
		{Key: "tables", Label: "Tables"},
		{Key: "node_ip", Label: "Node", Sortable: true},
		{Key: "shard", Label: "Shard", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "username", Label: "User", Sortable: true},
	}
}

func traceColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "started_at", Label: "Started", Type: plugin.ColumnDateTime, Sortable: true},
		{Key: "duration_us", Label: "Duration", Type: plugin.ColumnDuration, Sortable: true},
		{Key: "request", Label: "Request"},
		{Key: "command", Label: "Command", Sortable: true},
		{Key: "coordinator", Label: "Coordinator", Sortable: true},
		{Key: "request_size", Label: "Request", Type: plugin.ColumnBytes},
		{Key: "response_size", Label: "Response", Type: plugin.ColumnBytes},
		{Key: "username", Label: "User", Sortable: true},
	}
}

func savedQueryColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Name", Sortable: true},
		{Key: "query", Label: "CQL"},
		{Key: "scope", Label: "Scope", Type: plugin.ColumnBadge, Sortable: true, Severities: map[string]plugin.Severity{
			"connection": plugin.SeverityInfo,
			"user":       plugin.SeveritySecondary,
		}},
		{Key: "updated_at", Label: "Updated", Type: plugin.ColumnRelativeTime, Sortable: true},
	}
}

func actions() []plugin.Action {
	return []plugin.Action{
		{ID: "scylladb.keyspace.create", Label: "Create keyspace", Icon: icon("plus"), RouteID: "scylladb.keyspace.create", OnSuccess: &plugin.ActionSuccess{SelectTab: "keyspaces"}},
		{ID: "scylladb.keyspace.drop", Label: "Drop", Icon: icon("trash-2"), RouteID: "scylladb.keyspace.drop", Params: map[string]string{"keyspace": "${resource.uid}"}, Confirm: true, ConfirmText: "Drop this keyspace? Every table, view, type, and all data within it will be permanently deleted.", Bulk: true},
		{ID: "scylladb.type.create", Label: "Create type", Icon: icon("plus"), RouteID: "scylladb.type.create", Params: map[string]string{"keyspace": "${resource.uid}"}, OnSuccess: &plugin.ActionSuccess{SelectTab: "types"}},
		{ID: "scylladb.type.drop", Label: "Drop", Icon: icon("trash-2"), RouteID: "scylladb.type.drop", Params: map[string]string{"keyspace": "${resource.namespace}", "name": "${resource.name}"}, Confirm: true, ConfirmText: "Drop this type? Tables and columns that use it must already be gone.", Bulk: true},
		{ID: "scylladb.table.create", Label: "Create table", Icon: icon("plus"), RouteID: "scylladb.table.create", Params: map[string]string{"keyspace": "${resource.uid}"}, OnSuccess: &plugin.ActionSuccess{SelectTab: "tables"}},
		{ID: "scylladb.column.add", Label: "Add column", Icon: icon("columns-3"), RouteID: "scylladb.column.add", Params: tableParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "columns"}},
		{ID: "scylladb.column.drop", Label: "Drop column", Icon: icon("trash"), RouteID: "scylladb.column.drop", Params: map[string]string{"keyspace": "${record.keyspace}", "table": "${record.table}", "name": "${record.column_name}"}, Confirm: true, ConfirmText: "Drop this column? Its data is permanently removed.", OnSuccess: &plugin.ActionSuccess{SelectTab: "columns"}, Bulk: true},
		{ID: "scylladb.index.create", Label: "Create index", Icon: icon("plus"), RouteID: "scylladb.index.create", Params: tableParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "indexes"}},
		{ID: "scylladb.index.drop", Label: "Drop index", Icon: icon("trash"), RouteID: "scylladb.index.drop", Params: map[string]string{"keyspace": "${record.keyspace}", "name": "${record.index_name}"}, Confirm: true, ConfirmText: "Drop this index? Queries relying on it will start failing.", OnSuccess: &plugin.ActionSuccess{SelectTab: "indexes"}, Bulk: true},
		{ID: "scylladb.table.truncate", Label: "Truncate", Icon: icon("eraser"), RouteID: "scylladb.table.truncate", Params: tableParams(), Confirm: true, ConfirmText: "Truncate this table? Every partition and row will be deleted on every replica.", Bulk: true},
		{ID: "scylladb.table.drop", Label: "Drop", Icon: icon("trash-2"), RouteID: "scylladb.table.drop", Params: tableParams(), Confirm: true, ConfirmText: "Drop this table? The table definition and data will be permanently deleted.", Bulk: true},
		{ID: "scylladb.view.drop", Label: "Drop", Icon: icon("trash-2"), RouteID: "scylladb.view.drop", Params: map[string]string{"keyspace": "${resource.namespace}", "view": "${resource.name}"}, Confirm: true, ConfirmText: "Drop this materialized view? It will stop being maintained and its data is removed.", Bulk: true},
		{
			ID: "scylladb.partition.locate", Label: "Locate partition", Icon: icon("crosshair"), RouteID: "scylladb.partition.locate",
			Params: tableParams(),
			OnSuccess: &plugin.ActionSuccess{Effects: []plugin.ActionEffect{{
				Type: plugin.ActionEffectOpenPanel,
				OpenPanel: &plugin.OpenPanelEffect{
					Open:  plugin.OpenDialog,
					Panel: plugin.PanelObjectDetail,
					Title: "Partition ${response.token}",
					Icon:  icon("crosshair"),
					Source: &plugin.DataSource{RouteID: "scylladb.partition.owner", Params: map[string]string{
						"keyspace": "${response.keyspace}",
						"table":    "${response.table}",
						"token":    "${response.token}",
					}},
					Config: partitionOwnerConfig(),
				},
			}}},
		},
		{
			ID: "scylladb.trace.run", Label: "Trace a statement", Icon: icon("route"), RouteID: "scylladb.trace.run",
			Confirm: true, ConfirmText: "Run this statement on the cluster with server-side tracing enabled? Tracing adds load and writes rows to system_traces.",
			OnSuccess: &plugin.ActionSuccess{Effects: []plugin.ActionEffect{{
				Type: plugin.ActionEffectOpenPanel,
				OpenPanel: &plugin.OpenPanelEffect{
					Open:   plugin.OpenDialog,
					Panel:  plugin.PanelTimeline,
					Title:  "Trace ${response.session_id}",
					Icon:   icon("route"),
					Source: &plugin.DataSource{RouteID: "scylladb.trace.events", Params: map[string]string{"id": "${response.session_id}"}},
					Config: plugin.TimelineConfig{
						TimestampField: "time",
						TitleField:     "activity",
						BodyField:      "detail",
						SeverityField:  "severity",
						IconField:      "icon",
						ResourceField:  "source",
						EmptyText:      "The coordinator has not flushed this trace yet.",
					},
				},
			}}},
		},
		{ID: "scylladb.query.save", Label: "Save CQL", Icon: icon("bookmark-plus"), RouteID: "scylladb.query.save", OnSuccess: &plugin.ActionSuccess{SelectTab: "saved"}},
		{ID: "scylladb.query.delete", Label: "Delete", Icon: icon("trash"), RouteID: "scylladb.query.delete", Params: map[string]string{"key": "${record.key}"}, Confirm: true, ConfirmText: "Delete this saved CQL statement?", OnSuccess: &plugin.ActionSuccess{SelectTab: "saved"}, Bulk: true},
	}
}

func partitionOwnerConfig() plugin.ObjectDetailConfig {
	return objectDetail(
		plugin.ObjectDetailSection{Title: "Partition", Fields: []plugin.ObjectDetailField{
			{Key: "token", Label: "Token", Copy: true},
			field("keyspace", "Keyspace"),
			field("table", "Table"),
			field("range_start", "Range start"),
			field("range_end", "Range end"),
		}},
		plugin.ObjectDetailSection{Title: "Placement", Fields: []plugin.ObjectDetailField{
			{Key: "primary_replica", Label: "Primary replica", Copy: true},
			typedField("shard", "Shard (core)", plugin.ColumnNumber),
			field("data_center", "Datacenter"),
			field("rack", "Rack"),
			typedField("replicas", "All replicas", plugin.ColumnJSON),
			typedField("replica_count", "Replica count", plugin.ColumnNumber),
		}},
	)
}
