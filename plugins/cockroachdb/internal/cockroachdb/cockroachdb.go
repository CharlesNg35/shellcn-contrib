// Package cockroachdb implements the CockroachDB protocol plugin.
package cockroachdb

import (
	"context"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type Plugin struct{}

func New() *Plugin { return &Plugin{} }

const cockroachdbSvgIcon = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 31.82 32" width="2486" height="2500"><title>CL</title><path d="M19.42 9.17a15.39 15.39 0 0 1-3.51.4 15.46 15.46 0 0 1-3.51-.4 15.63 15.63 0 0 1 3.51-3.91 15.71 15.71 0 0 1 3.51 3.91zM30 .57A17.22 17.22 0 0 0 25.59 0a17.4 17.4 0 0 0-9.68 2.93A17.38 17.38 0 0 0 6.23 0a17.22 17.22 0 0 0-4.44.57A16.22 16.22 0 0 0 0 1.13a.07.07 0 0 0 0 .09 17.32 17.32 0 0 0 .83 1.57.07.07 0 0 0 .08 0 16.39 16.39 0 0 1 1.81-.54 15.65 15.65 0 0 1 11.59 1.88 17.52 17.52 0 0 0-3.78 4.48c-.2.32-.37.65-.55 1s-.22.45-.33.69-.31.72-.44 1.08a17.46 17.46 0 0 0 4.29 18.7c.26.25.53.49.81.73s.44.37.67.54.59.44.89.64a.07.07 0 0 0 .08 0c.3-.21.6-.42.89-.64s.45-.35.67-.54.55-.48.81-.73a17.45 17.45 0 0 0 5.38-12.61 17.39 17.39 0 0 0-1.09-6.09c-.14-.37-.29-.73-.45-1.09s-.22-.47-.33-.69-.35-.66-.55-1a17.61 17.61 0 0 0-3.78-4.48 15.65 15.65 0 0 1 11.6-1.84 16.13 16.13 0 0 1 1.81.54.07.07 0 0 0 .08 0q.44-.76.82-1.56a.07.07 0 0 0 0-.09A16.89 16.89 0 0 0 30 .57z" fill="#151f34"/><path d="M21.82 17.47a15.51 15.51 0 0 1-4.25 10.69 15.66 15.66 0 0 1-.72-4.68 15.5 15.5 0 0 1 4.25-10.69 15.62 15.62 0 0 1 .72 4.68" fill="#348540"/><path d="M15 23.48a15.55 15.55 0 0 1-.72 4.68 15.54 15.54 0 0 1-3.53-15.37A15.5 15.5 0 0 1 15 23.48" fill="#7dbc42"/></svg>`

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:          plugin.CurrentAPIVersion,
		Name:                protocolName,
		Version:             "0.1.0",
		Title:               "CockroachDB",
		Description:         "CockroachDB cockpit with databases, schemas, table data, ranges, nodes, jobs, sessions, queries, SQL editor, DDL helpers, and safety controls.",
		Icon:                plugin.Icon{Type: plugin.IconSVG, Value: cockroachdbSvgIcon},
		Category:            plugin.CategoryDatabases,
		Config:              configSchema(),
		Capabilities:        []plugin.Capability{"sql", "schema", "tables", "query_editor", "cluster", "jobs"},
		SupportedTransports: []plugin.Transport{plugin.TransportDirect},
		Layout:              plugin.LayoutSidebarTree,
		Tree:                tree(),
		Resources:           resources(),
		Actions:             actions(),
		Streams: []plugin.Stream{
			{ID: "cockroachdb.query", Kind: plugin.StreamLogs, RouteID: "cockroachdb.query"},
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

func objectDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{RawToggle: true}
}

func boolPtr(v bool) *bool {
	return &v
}

func tree() []plugin.TreeGroup {
	return []plugin.TreeGroup{
		{Key: "databases", Label: "Databases", Icon: icon("database"), Source: plugin.DataSource{RouteID: "cockroachdb.databases.tree"}, Ref: &plugin.ResourceRef{Kind: "server", Name: "Databases", UID: "server"}},
		{Key: "nodes", Label: "Nodes", Icon: icon("server"), Source: plugin.DataSource{RouteID: "cockroachdb.nodes.tree"}, ResourceKind: "node"},
		{Key: "ranges", Label: "Ranges", Icon: icon("blocks"), Source: plugin.DataSource{RouteID: "cockroachdb.ranges.tree"}, ResourceKind: "range"},
		{Key: "jobs", Label: "Jobs", Icon: icon("briefcase-business"), Source: plugin.DataSource{RouteID: "cockroachdb.jobs.tree"}, ResourceKind: "job"},
		{Key: "sessions", Label: "Sessions", Icon: icon("activity"), Source: plugin.DataSource{RouteID: "cockroachdb.sessions.tree"}, ResourceKind: "session"},
		{Key: "queries", Label: "Queries", Icon: icon("search-code"), Source: plugin.DataSource{RouteID: "cockroachdb.queries.tree"}, ResourceKind: "query"},
		{Key: "users", Label: "Users", Icon: icon("users"), Source: plugin.DataSource{RouteID: "cockroachdb.users.tree"}, ResourceKind: "user"},
		{Key: "schemas", Label: "Schemas", Icon: icon("folder-tree"), Source: plugin.DataSource{RouteID: "cockroachdb.schemas.tree"}, ResourceKind: "schema"},
		{Key: "functions", Label: "Functions", Icon: icon("function-square"), Source: plugin.DataSource{RouteID: "cockroachdb.functions.tree"}, ResourceKind: "function"},
	}
}

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		serverResource(),
		databaseResource(),
		nodeResource(),
		rangeResource(),
		jobResource(),
		sessionResource(),
		queryResource(),
		userResource(),
		schemaResource(),
		tableResource(),
		viewResource(),
		functionResource(),
		sequenceResource(),
	}
}

// serverResource is the connection-level view opened by clicking the Databases
// tree group: the database list plus a SQL console.
func serverResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "server", Title: "Databases",
		List: plugin.DataSource{RouteID: "cockroachdb.databases.list"},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "Databases"},
			Tabs: []plugin.Panel{
				{Key: "databases", Label: "Databases", Icon: icon("database"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "cockroachdb.databases.list"}, Config: plugin.TableConfig{ActionIDs: []string{"cockroachdb.database.create"}}},
				{Key: "users", Label: "Users", Icon: icon("users"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "cockroachdb.users.list"}, Config: plugin.TableConfig{Columns: userColumns(), ActionIDs: []string{"cockroachdb.user.create"}, RowActionIDs: []string{"cockroachdb.user.grant", "cockroachdb.user.drop"}}},
				{Key: "console", Label: "SQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "cockroachdb.query", Method: plugin.MethodWS}, Config: queryConfig("SELECT now();")},
			},
		},
	}
}

func databaseResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "database", Title: "Databases",
		List: plugin.DataSource{RouteID: "cockroachdb.databases.list"},
		Actions: plugin.ResourceActions{
			Toolbar: []string{"cockroachdb.database.create"},
			Row:     []string{"cockroachdb.database.drop"},
			Detail:  []string{"cockroachdb.schema.create", "cockroachdb.database.drop"},
		},
		Columns: []plugin.Column{
			{Key: "name", Label: "Database", Sortable: true},
			{Key: "owner", Label: "Owner", Sortable: true},
			{Key: "size", Label: "Size", Type: plugin.ColumnBytes, Sortable: true},
			{Key: "schemas", Label: "Schemas", Type: plugin.ColumnNumber, Sortable: true},
			{Key: "encoding", Label: "Encoding"},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "cockroachdb.database.overview", Params: map[string]string{"database": "${resource.uid}"}}, Config: objectDetailConfig()},
				{Key: "schemas", Label: "Schemas", Icon: icon("folder-tree"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "cockroachdb.schemas.list"}, Config: plugin.TableConfig{Columns: schemaColumns(), ActionIDs: []string{"cockroachdb.schema.create"}, RowActionIDs: []string{"cockroachdb.schema.drop"}}},
				{Key: "relations", Label: "Relationships", Icon: icon("workflow"), Type: plugin.PanelGraph, Source: &plugin.DataSource{RouteID: "cockroachdb.relations.graph"}, Config: plugin.GraphConfig{Layout: plugin.GraphLayoutGrid, FitView: true, Exportable: boolPtr(true)}},
				{Key: "nodes", Label: "Nodes", Icon: icon("server"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "cockroachdb.nodes.list"}, Config: plugin.TableConfig{Columns: nodeColumns()}},
				{Key: "jobs", Label: "Jobs", Icon: icon("briefcase-business"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "cockroachdb.jobs.list"}, Config: plugin.TableConfig{Columns: jobColumns()}},
				{Key: "ranges", Label: "Ranges", Icon: icon("blocks"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "cockroachdb.ranges.list"}, Config: plugin.TableConfig{Columns: rangeColumns()}},
				{Key: "query", Label: "Query", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "cockroachdb.query", Method: plugin.MethodWS}, Config: queryConfig("SELECT now();")},
			},
		},
	}
}

func nodeResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "node", Title: "Nodes",
		List:    plugin.DataSource{RouteID: "cockroachdb.nodes.list"},
		Columns: nodeColumns(),
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "Node ${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "cockroachdb.node.overview", Params: map[string]string{"node": "${resource.uid}"}}, Config: objectDetailConfig()},
		}},
	}
}

func rangeResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "range", Title: "Ranges",
		List:    plugin.DataSource{RouteID: "cockroachdb.ranges.list"},
		Columns: rangeColumns(),
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "Range ${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "cockroachdb.range.overview", Params: map[string]string{"range": "${resource.uid}"}}, Config: objectDetailConfig()},
		}},
	}
}

func jobResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "job", Title: "Jobs",
		List:    plugin.DataSource{RouteID: "cockroachdb.jobs.list"},
		Columns: jobColumns(),
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "Job ${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "cockroachdb.job.overview", Params: map[string]string{"job": "${resource.uid}"}}, Config: objectDetailConfig()},
		}},
	}
}

func sessionResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "session", Title: "Sessions",
		List:    plugin.DataSource{RouteID: "cockroachdb.sessions.list"},
		Columns: sessionColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{"cockroachdb.session.cancel"},
			Detail: []string{"cockroachdb.session.cancel"},
		},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "Session ${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "cockroachdb.session.overview", Params: map[string]string{"session": "${resource.uid}"}}, Config: objectDetailConfig()},
		}},
	}
}

func queryResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "query", Title: "Queries",
		List:    plugin.DataSource{RouteID: "cockroachdb.queries.list"},
		Columns: queryColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{"cockroachdb.query.cancel.id"},
			Detail: []string{"cockroachdb.query.cancel.id"},
		},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "Query ${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "cockroachdb.query.overview", Params: map[string]string{"query": "${resource.uid}"}}, Config: objectDetailConfig()},
		}},
	}
}

func userResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "user", Title: "Users",
		List:    plugin.DataSource{RouteID: "cockroachdb.users.list"},
		Columns: userColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{"cockroachdb.user.create"},
			Row:     []string{"cockroachdb.user.drop"},
			Detail:  []string{"cockroachdb.user.grant", "cockroachdb.user.drop"},
		},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "cockroachdb.user.overview", Params: map[string]string{"user": "${resource.uid}"}}, Config: objectDetailConfig()},
		}},
	}
}

func schemaResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "schema", Title: "Schemas",
		List: plugin.DataSource{RouteID: "cockroachdb.schemas.list"},
		Actions: plugin.ResourceActions{
			Toolbar: []string{"cockroachdb.schema.create"},
			Row:     []string{"cockroachdb.schema.drop"},
			Detail:  []string{"cockroachdb.schema.drop"},
		},
		Columns: schemaColumns(),
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "cockroachdb.schema.overview", Params: map[string]string{"schema": "${resource.uid}"}}, Config: objectDetailConfig()},
				{Key: "tables", Label: "Tables", Icon: icon("table-2"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "cockroachdb.tables.list", Params: map[string]string{"schema": "${resource.uid}"}}, Config: plugin.TableConfig{Columns: tableColumns(), ActionIDs: []string{"cockroachdb.table.create"}, RowActionIDs: []string{"cockroachdb.table.truncate", "cockroachdb.table.drop"}}},
				{Key: "views", Label: "Views", Icon: icon("panel-top"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "cockroachdb.views.list", Params: map[string]string{"schema": "${resource.uid}"}}, Config: plugin.TableConfig{Columns: viewColumns(), RowActionIDs: []string{"cockroachdb.view.drop"}}},
				{Key: "functions", Label: "Functions", Icon: icon("function-square"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "cockroachdb.functions.list", Params: map[string]string{"schema": "${resource.uid}"}}, Config: plugin.TableConfig{Columns: functionColumns()}},
				{Key: "sequences", Label: "Sequences", Icon: icon("list-ordered"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "cockroachdb.sequences.list", Params: map[string]string{"schema": "${resource.uid}"}}, Config: plugin.TableConfig{Columns: sequenceColumns()}},
			},
		},
	}
}

func tableResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "table", Title: "Tables",
		List:    plugin.DataSource{RouteID: "cockroachdb.tables.list"},
		Columns: tableColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{"cockroachdb.table.truncate", "cockroachdb.table.drop"},
			Detail: []string{"cockroachdb.table.rename", "cockroachdb.table.truncate", "cockroachdb.table.drop"},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.namespace}.${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "data", Label: "Data", Icon: icon("table"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "cockroachdb.table.rows", Params: tableParams()}, Config: dataGridConfig()},
				{Key: "columns", Label: "Columns", Icon: icon("columns-3"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "cockroachdb.table.columns", Params: tableParams()}, Config: plugin.TableConfig{Columns: columnColumns(), ActionIDs: []string{"cockroachdb.column.add"}, RowActionIDs: []string{"cockroachdb.column.rename", "cockroachdb.column.alter", "cockroachdb.column.drop"}}},
				{Key: "indexes", Label: "Indexes", Icon: icon("key-round"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "cockroachdb.table.indexes", Params: tableParams()}, Config: plugin.TableConfig{Columns: indexColumns(), ActionIDs: []string{"cockroachdb.index.create"}, RowActionIDs: []string{"cockroachdb.index.drop"}}},
				{Key: "constraints", Label: "Constraints", Icon: icon("shield-check"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "cockroachdb.table.constraints", Params: tableParams()}, Config: plugin.TableConfig{Columns: constraintColumns(), ActionIDs: []string{"cockroachdb.constraint.add"}, RowActionIDs: []string{"cockroachdb.constraint.drop"}}},
				{Key: "query", Label: "SQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "cockroachdb.query", Method: plugin.MethodWS}, Config: queryConfig("SELECT * FROM ${resource.namespace}.${resource.name} LIMIT 100;")},
			},
		},
	}
}

func viewResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "view", Title: "Views",
		List: plugin.DataSource{RouteID: "cockroachdb.views.list"}, Columns: viewColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{"cockroachdb.view.drop"},
			Detail: []string{"cockroachdb.view.drop"},
		},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.namespace}.${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "data", Label: "Data", Icon: icon("table-properties"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "cockroachdb.view.rows", Params: tableParams()}},
			{Key: "definition", Label: "Definition", Icon: icon("code"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: "cockroachdb.view.definition", Params: tableParams()}},
			{Key: "query", Label: "SQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "cockroachdb.query", Method: plugin.MethodWS}, Config: queryConfig("SELECT * FROM ${resource.namespace}.${resource.name} LIMIT 100;")},
		}},
	}
}

func functionResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "function", Title: "Functions",
		List: plugin.DataSource{RouteID: "cockroachdb.functions.list"}, Columns: functionColumns(),
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.namespace}.${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "definition", Label: "Definition", Icon: icon("code"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: "cockroachdb.function.definition", Params: map[string]string{"id": "${resource.uid}"}}},
		}},
	}
}

func sequenceResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "sequence", Title: "Sequences",
		List: plugin.DataSource{RouteID: "cockroachdb.sequences.list"}, Columns: sequenceColumns(),
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.namespace}.${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "cockroachdb.sequence.overview", Params: tableParams()}, Config: objectDetailConfig()},
		}},
	}
}

func tableParams() map[string]string {
	return map[string]string{"schema": "${resource.namespace}", "table": "${resource.name}"}
}

func dataGridConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Editable:      true,
		StagedEdits:   true,
		Exportable:    true,
		EmptyText:     "No rows.",
		Insert:        &plugin.DataSource{RouteID: "cockroachdb.table.row.insert", Method: plugin.MethodPost, Params: tableParams()},
		Update:        &plugin.DataSource{RouteID: "cockroachdb.table.row.update", Method: plugin.MethodPatch, Params: tableParams()},
		Delete:        &plugin.DataSource{RouteID: "cockroachdb.table.row.delete", Method: plugin.MethodDelete, Params: tableParams()},
		ColumnsSource: &plugin.DataSource{RouteID: "cockroachdb.table.columns", Params: tableParams()},
	}
}

func queryConfig(initial string) plugin.QueryEditorConfig {
	return plugin.QueryEditorConfig{
		Language:          "sql",
		Label:             "SQL",
		ExecuteLabel:      "Run query",
		CancelLabel:       "Cancel query",
		RunningLabel:      "Running…",
		EmptyText:         "Run a query to see results.",
		InitialQuery:      initial,
		CancelRouteID:     "cockroachdb.query.cancel",
		CompletionRouteID: "cockroachdb.completion",
		Exportable:        true,
	}
}

func schemaColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Schema", Sortable: true}, {Key: "owner", Label: "Owner", Sortable: true}, {Key: "tables", Label: "Tables", Type: plugin.ColumnNumber, Sortable: true}, {Key: "views", Label: "Views", Type: plugin.ColumnNumber, Sortable: true}, {Key: "functions", Label: "Functions", Type: plugin.ColumnNumber, Sortable: true}}
}

func tableColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Table", Sortable: true}, {Key: "schema", Label: "Schema", Sortable: true}, {Key: "rows", Label: "Rows", Type: plugin.ColumnNumber, Sortable: true}, {Key: "size", Label: "Size", Type: plugin.ColumnBytes, Sortable: true}, {Key: "owner", Label: "Owner", Sortable: true}}
}

func viewColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "View", Sortable: true}, {Key: "schema", Label: "Schema", Sortable: true}, {Key: "owner", Label: "Owner", Sortable: true}, {Key: "updatable", Label: "Updatable", Type: plugin.ColumnBool}}
}

func functionColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Function", Sortable: true}, {Key: "schema", Label: "Schema", Sortable: true}, {Key: "arguments", Label: "Arguments"}, {Key: "returns", Label: "Returns"}, {Key: "language", Label: "Language", Sortable: true}}
}

func sequenceColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Sequence", Sortable: true}, {Key: "schema", Label: "Schema", Sortable: true}, {Key: "dataType", Label: "Type"}, {Key: "start", Label: "Start", Type: plugin.ColumnNumber}, {Key: "increment", Label: "Increment", Type: plugin.ColumnNumber}}
}

func nodeColumns() []plugin.Column {
	return []plugin.Column{{Key: "node_id", Label: "Node", Type: plugin.ColumnNumber, Sortable: true}, {Key: "version", Label: "Version"}, {Key: "cluster_id", Label: "Cluster"}, {Key: "platform", Label: "Platform"}}
}

func rangeColumns() []plugin.Column {
	return []plugin.Column{{Key: "range_id", Label: "Range", Type: plugin.ColumnNumber, Sortable: true}, {Key: "start_key", Label: "Start"}, {Key: "end_key", Label: "End"}, {Key: "lease_holder", Label: "Leaseholder"}, {Key: "replicas", Label: "Replicas"}}
}

func jobColumns() []plugin.Column {
	return []plugin.Column{{Key: "job_id", Label: "Job", Sortable: true}, {Key: "job_type", Label: "Type", Sortable: true}, {Key: "status", Label: "Status", Sortable: true}, {Key: "created", Label: "Created", Type: plugin.ColumnRelativeTime}, {Key: "fraction_completed", Label: "Progress", Type: plugin.ColumnNumber}, {Key: "description", Label: "Description"}}
}

func sessionColumns() []plugin.Column {
	return []plugin.Column{{Key: "session_id", Label: "Session", Sortable: true}, {Key: "node_id", Label: "Node", Type: plugin.ColumnNumber}, {Key: "username", Label: "User"}, {Key: "client_address", Label: "Client"}, {Key: "application_name", Label: "Application"}, {Key: "active_queries", Label: "Active", Type: plugin.ColumnNumber}, {Key: "start", Label: "Started", Type: plugin.ColumnRelativeTime}}
}

func queryColumns() []plugin.Column {
	return []plugin.Column{{Key: "query_id", Label: "Query", Sortable: true}, {Key: "node_id", Label: "Node", Type: plugin.ColumnNumber}, {Key: "username", Label: "User"}, {Key: "start", Label: "Started", Type: plugin.ColumnRelativeTime}, {Key: "status", Label: "Status"}, {Key: "query", Label: "SQL"}}
}

func userColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "User", Sortable: true}, {Key: "member_of", Label: "Member of"}, {Key: "options", Label: "Options"}}
}

func columnColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Column", Sortable: true}, {Key: "type", Label: "Type"}, {Key: "nullable", Label: "Nullable", Type: plugin.ColumnBool}, {Key: "default", Label: "Default"}, {Key: "identity", Label: "Identity"}, {Key: "position", Label: "Position", Type: plugin.ColumnNumber, Sortable: true}}
}

func indexColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Index", Sortable: true}, {Key: "unique", Label: "Unique", Type: plugin.ColumnBool}, {Key: "primary", Label: "Primary", Type: plugin.ColumnBool}, {Key: "definition", Label: "Definition"}}
}

func constraintColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Constraint", Sortable: true}, {Key: "type", Label: "Type", Sortable: true}, {Key: "definition", Label: "Definition"}}
}

func actions() []plugin.Action {
	return []plugin.Action{
		{ID: "cockroachdb.database.create", Label: "Create database", Icon: icon("plus"), RouteID: "cockroachdb.database.create"},
		{ID: "cockroachdb.database.drop", Label: "Drop database", Icon: icon("trash-2"), RouteID: "cockroachdb.database.drop", Params: map[string]string{"database": "${resource.uid}"}, Confirm: true, ConfirmText: "Drop this database? All of its schemas and data will be permanently deleted."},
		{ID: "cockroachdb.schema.create", Label: "Create schema", Icon: icon("folder-plus"), RouteID: "cockroachdb.schema.create"},
		{ID: "cockroachdb.schema.drop", Label: "Drop schema", Icon: icon("trash-2"), RouteID: "cockroachdb.schema.drop", Params: map[string]string{"schema": "${resource.uid}"}, Confirm: true, ConfirmText: "Drop this schema? It must be empty."},
		{ID: "cockroachdb.table.create", Label: "Create table", Icon: icon("plus"), RouteID: "cockroachdb.table.create", Params: map[string]string{"schema": "${resource.uid}"}, OnSuccess: &plugin.ActionSuccess{SelectTab: "tables"}},
		{ID: "cockroachdb.table.rename", Label: "Rename", Icon: icon("pencil"), RouteID: "cockroachdb.table.rename", Params: tableParams()},
		{ID: "cockroachdb.column.add", Label: "Add column", Icon: icon("columns-3"), RouteID: "cockroachdb.column.add", Params: tableParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "columns"}},
		{ID: "cockroachdb.column.rename", Label: "Rename column", Icon: icon("pencil"), RouteID: "cockroachdb.column.rename", Params: map[string]string{"schema": "${resource.scope}", "table": "${resource.namespace}", "name": "${resource.name}"}, OnSuccess: &plugin.ActionSuccess{SelectTab: "columns"}},
		{ID: "cockroachdb.column.alter", Label: "Change type", Icon: icon("wand-2"), RouteID: "cockroachdb.column.alter", Params: map[string]string{"schema": "${resource.scope}", "table": "${resource.namespace}", "name": "${resource.name}"}, OnSuccess: &plugin.ActionSuccess{SelectTab: "columns"}},
		{ID: "cockroachdb.column.drop", Label: "Drop column", Icon: icon("trash"), RouteID: "cockroachdb.column.drop", Params: map[string]string{"schema": "${resource.scope}", "table": "${resource.namespace}", "name": "${resource.name}"}, Confirm: true, ConfirmText: "Drop this column? Its data is permanently removed.", OnSuccess: &plugin.ActionSuccess{SelectTab: "columns"}},
		{ID: "cockroachdb.constraint.add", Label: "Add constraint", Icon: icon("plus"), RouteID: "cockroachdb.constraint.add", Params: tableParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "constraints"}},
		{ID: "cockroachdb.constraint.drop", Label: "Drop constraint", Icon: icon("trash"), RouteID: "cockroachdb.constraint.drop", Params: map[string]string{"schema": "${resource.scope}", "table": "${resource.namespace}", "name": "${resource.name}"}, Confirm: true, ConfirmText: "Drop this constraint?", OnSuccess: &plugin.ActionSuccess{SelectTab: "constraints"}},
		{ID: "cockroachdb.index.create", Label: "Create index", Icon: icon("plus"), RouteID: "cockroachdb.index.create", Params: tableParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "indexes"}},
		{ID: "cockroachdb.index.drop", Label: "Drop index", Icon: icon("trash"), RouteID: "cockroachdb.index.drop", Params: map[string]string{"schema": "${resource.scope}", "table": "${resource.namespace}", "name": "${resource.name}"}, Confirm: true, ConfirmText: "Drop this index?", OnSuccess: &plugin.ActionSuccess{SelectTab: "indexes"}},
		{ID: "cockroachdb.table.truncate", Label: "Truncate", Icon: icon("trash"), RouteID: "cockroachdb.table.truncate", Params: tableParams(), Confirm: true, ConfirmText: "Truncate this table? Every row will be deleted."},
		{ID: "cockroachdb.table.drop", Label: "Drop", Icon: icon("trash-2"), RouteID: "cockroachdb.table.drop", Params: tableParams(), Confirm: true, ConfirmText: "Drop this table? The table definition and data will be permanently deleted."},
		{ID: "cockroachdb.view.drop", Label: "Drop", Icon: icon("trash-2"), RouteID: "cockroachdb.view.drop", Params: map[string]string{"schema": "${resource.namespace}", "view": "${resource.name}"}, Confirm: true, ConfirmText: "Drop this view?"},
		{ID: "cockroachdb.session.cancel", Label: "Cancel session", Icon: icon("circle-stop"), RouteID: "cockroachdb.session.cancel", Params: map[string]string{"id": "${resource.uid}"}, Confirm: true, ConfirmText: "Cancel this session? Its active query is stopped and the session ends."},
		{ID: "cockroachdb.query.cancel.id", Label: "Cancel query", Icon: icon("circle-stop"), RouteID: "cockroachdb.query.cancel.id", Params: map[string]string{"id": "${resource.uid}"}, Confirm: true, ConfirmText: "Cancel this query? It will be stopped."},
		{ID: "cockroachdb.user.create", Label: "Create user", Icon: icon("user-plus"), RouteID: "cockroachdb.user.create"},
		{ID: "cockroachdb.user.grant", Label: "Grant", Icon: icon("shield-plus"), RouteID: "cockroachdb.user.grant", Params: map[string]string{"user": "${resource.uid}"}, Confirm: true, ConfirmText: "Grant privileges to this user?"},
		{ID: "cockroachdb.user.drop", Label: "Drop user", Icon: icon("user-minus"), RouteID: "cockroachdb.user.drop", Params: map[string]string{"user": "${resource.uid}"}, Confirm: true, ConfirmText: "Drop this user? They lose access to the cluster."},
	}
}
