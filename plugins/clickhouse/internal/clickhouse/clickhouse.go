// Package clickhouse implements the ClickHouse protocol plugin.
package clickhouse

import (
	"context"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type Plugin struct{}

func New() *Plugin { return &Plugin{} }

const clickHouseIconSVG = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 150 150"><rect y="0" width="150" height="150" rx="18" fill="#ffcc01"/><path fill="#161616" d="M30,28.3c0-.6.5-1.1,1.1-1.1h8.4c.6,0,1.1.5,1.1,1.1v93.3c0,.6-.5,1.1-1.1,1.1h-8.4c-.6,0-1.1-.5-1.1-1.1V28.3Z"/><path fill="#161616" d="M51.2,28.3c0-.6.5-1.1,1.1-1.1h8.4c.6,0,1.1.5,1.1,1.1v93.3c0,.6-.5,1.1-1.1,1.1h-8.4c-.6,0-1.1-.5-1.1-1.1V28.3Z"/><path fill="#161616" d="M72.4,28.3c0-.6.5-1.1,1.1-1.1h8.4c.6,0,1.1.5,1.1,1.1v93.3c0,.6-.5,1.1-1.1,1.1h-8.4c-.6,0-1.1-.5-1.1-1.1V28.3Z"/><path fill="#161616" d="M93.7,28.3c0-.6.5-1.1,1.1-1.1h8.4c.6,0,1.1.5,1.1,1.1v93.3c0,.6-.5,1.1-1.1,1.1h-8.4c-.6,0-1.1-.5-1.1-1.1V28.3Z"/><path fill="#161616" d="M114.9,65.5c0-.6.5-1.1,1.1-1.1h8.4c.6,0,1.1.5,1.1,1.1v19c0,.6-.5,1.1-1.1,1.1h-8.4c-.6,0-1.1-.5-1.1-1.1v-19Z"/></svg>`

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:          plugin.CurrentAPIVersion,
		Name:                protocolName,
		Version:             "0.1.0",
		Title:               "ClickHouse",
		Description:         "ClickHouse cockpit with databases, tables, views, dictionaries, mutations, merges, processes, users, SQL editor, DDL helpers, and safety controls.",
		Icon:                plugin.Icon{Type: plugin.IconSVG, Value: clickHouseIconSVG},
		Category:            plugin.CategoryDatabases,
		Config:              configSchema(),
		Capabilities:        []plugin.Capability{"sql", "schema", "tables", "query_editor", "analytics", "cluster"},
		SupportedTransports: []plugin.Transport{plugin.TransportDirect},
		Layout:              plugin.LayoutSidebarTree,
		Tree:                tree(),
		Resources:           resources(),
		Actions:             actions(),
		Streams: []plugin.Stream{
			{ID: "clickhouse.query", Kind: plugin.StreamLogs, RouteID: "clickhouse.query"},
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

func tree() []plugin.TreeGroup {
	return []plugin.TreeGroup{
		{Key: "databases", Label: "Databases", Icon: icon("database"), Source: plugin.DataSource{RouteID: "clickhouse.databases.tree"}, Ref: &plugin.ResourceRef{Kind: "server", Name: "Databases", UID: "server"}},
		{Key: "dictionaries", Label: "Dictionaries", Icon: icon("book-open"), Source: plugin.DataSource{RouteID: "clickhouse.dictionaries.tree"}, ResourceKind: "dictionary"},
		{Key: "mutations", Label: "Mutations", Icon: icon("git-compare-arrows"), ResourceKind: "mutation"},
		{Key: "merges", Label: "Merges", Icon: icon("merge"), ResourceKind: "merge"},
		{Key: "processes", Label: "Processes", Icon: icon("activity"), ResourceKind: "process"},
		{Key: "users", Label: "Users", Icon: icon("users"), Source: plugin.DataSource{RouteID: "clickhouse.users.tree"}, ResourceKind: "user"},
	}
}

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		serverResource(),
		databaseResource(),
		tableResource(),
		viewResource(),
		dictionaryResource(),
		mutationResource(),
		mergeResource(),
		processResource(),
		userResource(),
	}
}

// serverResource is the connection-level view opened by clicking the Databases
// tree group: the database list plus a SQL console.
func serverResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "server", Title: "Databases",
		List: plugin.DataSource{RouteID: "clickhouse.databases.list"},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "Databases"},
			Tabs: []plugin.Panel{
				{Key: "databases", Label: "Databases", Icon: icon("database"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "clickhouse.databases.list"}, Config: plugin.TableConfig{ActionIDs: []string{"clickhouse.database.create"}, RowActionIDs: []string{"clickhouse.database.drop"}}},
				{Key: "console", Label: "SQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "clickhouse.query", Method: plugin.MethodWS}, Config: queryConfig("SELECT version();")},
			},
		},
	}
}

func databaseResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "database", Title: "Databases",
		List: plugin.DataSource{RouteID: "clickhouse.databases.list"},
		Actions: plugin.ResourceActions{
			Toolbar: []string{"clickhouse.database.create"},
			Row:     []string{"clickhouse.database.drop"},
			Detail:  []string{"clickhouse.database.drop"},
		},
		Columns: []plugin.Column{
			{Key: "name", Label: "Database", Sortable: true},
			{Key: "engine", Label: "Engine", Sortable: true},
			{Key: "tables", Label: "Tables", Type: plugin.ColumnNumber, Sortable: true},
			{Key: "views", Label: "Views", Type: plugin.ColumnNumber, Sortable: true},
			{Key: "size", Label: "Size", Type: plugin.ColumnBytes, Sortable: true},
			{Key: "comment", Label: "Comment"},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "clickhouse.database.overview", Params: map[string]string{"database": "${resource.uid}"}}, Config: objectDetailConfig()},
				{Key: "tables", Label: "Tables", Icon: icon("table-2"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "clickhouse.tables.list", Params: map[string]string{"database": "${resource.uid}"}}, Config: plugin.TableConfig{Columns: tableColumns(), ActionIDs: []string{"clickhouse.table.create"}, RowActionIDs: []string{"clickhouse.table.truncate", "clickhouse.table.drop"}}},
				{Key: "views", Label: "Views", Icon: icon("panel-top"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "clickhouse.views.list", Params: map[string]string{"database": "${resource.uid}"}}, Config: plugin.TableConfig{Columns: viewColumns(), RowActionIDs: []string{"clickhouse.view.drop"}}},
				{Key: "dictionaries", Label: "Dictionaries", Icon: icon("book-open"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "clickhouse.dictionaries.list", Params: map[string]string{"database": "${resource.uid}"}}, Config: plugin.TableConfig{Columns: dictionaryColumns()}},
				{Key: "mutations", Label: "Mutations", Icon: icon("git-compare-arrows"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "clickhouse.mutations.list", Params: map[string]string{"database": "${resource.uid}"}}, Config: plugin.TableConfig{Columns: mutationColumns()}},
				{Key: "query", Label: "SQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "clickhouse.query", Method: plugin.MethodWS}, Config: queryConfig("SELECT version();")},
			},
		},
	}
}

func tableResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "table", Title: "Tables",
		List:    plugin.DataSource{RouteID: "clickhouse.tables.list"},
		Columns: tableColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{"clickhouse.table.truncate", "clickhouse.table.drop"},
			Detail: []string{"clickhouse.table.rename", "clickhouse.table.truncate", "clickhouse.table.drop"},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.namespace}.${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "data", Label: "Data", Icon: icon("table-properties"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "clickhouse.table.rows", Params: tableParams()}, Config: dataGridConfig()},
				{Key: "columns", Label: "Columns", Icon: icon("columns-3"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "clickhouse.table.columns", Params: tableParams()}, Config: plugin.TableConfig{Columns: columnColumns(), ActionIDs: []string{"clickhouse.column.add"}, RowActionIDs: []string{"clickhouse.column.alter", "clickhouse.column.drop"}}},
				{Key: "indexes", Label: "Indexes", Icon: icon("key-round"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "clickhouse.table.indexes", Params: tableParams()}, Config: plugin.TableConfig{Columns: indexColumns(), ActionIDs: []string{"clickhouse.index.create"}, RowActionIDs: []string{"clickhouse.index.drop"}}},
				{Key: "constraints", Label: "Constraints", Icon: icon("shield-check"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "clickhouse.table.constraints", Params: tableParams()}, Config: plugin.TableConfig{Columns: constraintColumns(), ActionIDs: []string{"clickhouse.constraint.add"}, RowActionIDs: []string{"clickhouse.constraint.drop"}}},
				{Key: "mutations", Label: "Mutations", Icon: icon("git-compare-arrows"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "clickhouse.mutations.list", Params: tableParams()}, Config: plugin.TableConfig{Columns: mutationColumns()}},
				{Key: "definition", Label: "Definition", Icon: icon("code"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: "clickhouse.table.definition", Params: tableParams()}},
				{Key: "query", Label: "SQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "clickhouse.query", Method: plugin.MethodWS}, Config: queryConfig("SELECT * FROM `${resource.namespace}`.`${resource.name}` LIMIT 100;")},
			},
		},
	}
}

func viewResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "view", Title: "Views",
		List: plugin.DataSource{RouteID: "clickhouse.views.list"}, Columns: viewColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{"clickhouse.view.drop"},
			Detail: []string{"clickhouse.view.drop"},
		},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.namespace}.${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "data", Label: "Data", Icon: icon("table-properties"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "clickhouse.view.rows", Params: tableParams()}, Config: plugin.TableConfig{Exportable: true}},
			{Key: "definition", Label: "Definition", Icon: icon("code"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: "clickhouse.view.definition", Params: tableParams()}},
			{Key: "query", Label: "SQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "clickhouse.query", Method: plugin.MethodWS}, Config: queryConfig("SELECT * FROM `${resource.namespace}`.`${resource.name}` LIMIT 100;")},
		}},
	}
}

func dictionaryResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "dictionary", Title: "Dictionaries",
		List:    plugin.DataSource{RouteID: "clickhouse.dictionaries.list"},
		Columns: dictionaryColumns(),
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.namespace}.${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "clickhouse.dictionary.overview", Params: tableParams()}, Config: objectDetailConfig()},
		}},
	}
}

func mutationResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "mutation", Title: "Mutations",
		List:    plugin.DataSource{RouteID: "clickhouse.mutations.list"},
		Columns: mutationColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{"clickhouse.mutation.kill"},
			Detail: []string{"clickhouse.mutation.kill"},
		},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "clickhouse.mutation.overview", Params: map[string]string{"id": "${resource.uid}"}}, Config: objectDetailConfig()},
		}},
	}
}

func mergeResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "merge", Title: "Merges",
		List:    plugin.DataSource{RouteID: "clickhouse.merges.list"},
		Columns: mergeColumns(),
		Actions: plugin.ResourceActions{
			Detail: []string{"clickhouse.merge.stop", "clickhouse.merge.start"},
		},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "clickhouse.merge.overview", Params: map[string]string{"id": "${resource.uid}"}}, Config: objectDetailConfig()},
		}},
	}
}

func processResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "process", Title: "Processes",
		List:    plugin.DataSource{RouteID: "clickhouse.processes.list"},
		Columns: processColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{"clickhouse.process.kill"},
			Detail: []string{"clickhouse.process.kill"},
		},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "clickhouse.process.overview", Params: map[string]string{"id": "${resource.uid}"}}, Config: objectDetailConfig()},
		}},
	}
}

func userResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "user", Title: "Users",
		List: plugin.DataSource{RouteID: "clickhouse.users.list"},
		Columns: []plugin.Column{
			{Key: "user", Label: "User", Sortable: true},
			{Key: "auth_type", Label: "Auth"},
			{Key: "storage", Label: "Storage"},
		},
		Actions: plugin.ResourceActions{
			Toolbar: []string{"clickhouse.user.create"},
			Row:     []string{"clickhouse.user.drop"},
			Detail:  []string{"clickhouse.user.grant", "clickhouse.user.drop"},
		},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "clickhouse.user.overview", Params: map[string]string{"user": "${resource.name}"}}, Config: objectDetailConfig()},
		}},
	}
}

func tableParams() map[string]string {
	return map[string]string{"database": "${resource.namespace}", "table": "${resource.name}"}
}

// dataGridConfig makes the table Data tab editable. ClickHouse INSERTs are cheap,
// but UPDATE/DELETE become asynchronous ALTER mutations, so the renderer's
// confirmation flow guards them via the destructive route risk. Rows are targeted
// by the table's sorting key, which the rows handler attaches as _key; tables
// without a sorting key (ORDER BY tuple()) ship no key and stay read-only.
func dataGridConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Editable:      true,
		StagedEdits:   true,
		Exportable:    true,
		EmptyText:     "No rows.",
		Insert:        &plugin.DataSource{RouteID: "clickhouse.table.row.insert", Method: plugin.MethodPost, Params: tableParams()},
		Update:        &plugin.DataSource{RouteID: "clickhouse.table.row.update", Method: plugin.MethodPatch, Params: tableParams()},
		Delete:        &plugin.DataSource{RouteID: "clickhouse.table.row.delete", Method: plugin.MethodDelete, Params: tableParams()},
		ColumnsSource: &plugin.DataSource{RouteID: "clickhouse.table.columns", Params: tableParams()},
	}
}

func queryConfig(initial string) plugin.QueryEditorConfig {
	return plugin.QueryEditorConfig{
		Language:          "sql",
		Label:             "SQL",
		ExecuteLabel:      "Run query",
		CancelLabel:       "Cancel query",
		RunningLabel:      "Running...",
		EmptyText:         "Run a query to see results.",
		InitialQuery:      initial,
		CancelRouteID:     "clickhouse.query.cancel",
		CompletionRouteID: "clickhouse.completion",
		Exportable:        true,
	}
}

func tableColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Table", Sortable: true}, {Key: "database", Label: "Database", Sortable: true}, {Key: "engine", Label: "Engine"}, {Key: "rows", Label: "Rows", Type: plugin.ColumnNumber, Sortable: true}, {Key: "size", Label: "Size", Type: plugin.ColumnBytes, Sortable: true}, {Key: "modified", Label: "Modified", Type: plugin.ColumnDateTime}, {Key: "comment", Label: "Comment"}}
}

func viewColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "View", Sortable: true}, {Key: "database", Label: "Database", Sortable: true}, {Key: "engine", Label: "Engine"}, {Key: "modified", Label: "Modified", Type: plugin.ColumnDateTime}, {Key: "comment", Label: "Comment"}}
}

func dictionaryColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Dictionary", Sortable: true}, {Key: "database", Label: "Database", Sortable: true}, {Key: "status", Label: "Status"}, {Key: "type", Label: "Type"}, {Key: "origin", Label: "Origin"}, {Key: "bytes_allocated", Label: "Bytes", Type: plugin.ColumnBytes}, {Key: "element_count", Label: "Elements", Type: plugin.ColumnNumber}}
}

func mutationColumns() []plugin.Column {
	return []plugin.Column{{Key: "mutation_id", Label: "Mutation", Sortable: true}, {Key: "database", Label: "Database"}, {Key: "table", Label: "Table"}, {Key: "command", Label: "Command"}, {Key: "create_time", Label: "Created", Type: plugin.ColumnRelativeTime}, {Key: "is_done", Label: "Done", Type: plugin.ColumnBool}, {Key: "latest_fail_reason", Label: "Last failure"}}
}

func mergeColumns() []plugin.Column {
	return []plugin.Column{{Key: "id", Label: "Merge"}, {Key: "database", Label: "Database"}, {Key: "table", Label: "Table"}, {Key: "elapsed", Label: "Elapsed", Type: plugin.ColumnNumber}, {Key: "progress", Label: "Progress", Type: plugin.ColumnNumber}, {Key: "num_parts", Label: "Parts", Type: plugin.ColumnNumber}}
}

func processColumns() []plugin.Column {
	return []plugin.Column{{Key: "query_id", Label: "Query", Sortable: true}, {Key: "user", Label: "User"}, {Key: "address", Label: "Address"}, {Key: "elapsed", Label: "Elapsed", Type: plugin.ColumnNumber}, {Key: "read_rows", Label: "Read rows", Type: plugin.ColumnNumber}, {Key: "memory_usage", Label: "Memory", Type: plugin.ColumnBytes}, {Key: "query", Label: "SQL"}}
}

func columnColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Column", Sortable: true}, {Key: "type", Label: "Type"}, {Key: "default_kind", Label: "Default kind"}, {Key: "default_expression", Label: "Default"}, {Key: "position", Label: "Position", Type: plugin.ColumnNumber, Sortable: true}, {Key: "comment", Label: "Comment"}}
}

func indexColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Index", Sortable: true}, {Key: "expression", Label: "Expression"}, {Key: "type", Label: "Type"}, {Key: "granularity", Label: "Granularity", Type: plugin.ColumnNumber}}
}

func constraintColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Constraint", Sortable: true}, {Key: "type", Label: "Type"}, {Key: "expression", Label: "Expression"}}
}

func actions() []plugin.Action {
	return []plugin.Action{
		{ID: "clickhouse.database.create", Label: "Create database", Icon: icon("plus"), RouteID: "clickhouse.database.create"},
		{ID: "clickhouse.database.drop", Label: "Drop", Icon: icon("trash-2"), RouteID: "clickhouse.database.drop", Params: map[string]string{"database": "${resource.uid}"}, Confirm: true, ConfirmText: "Drop this database? Every table, view, and dictionary it contains is permanently deleted.", OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList}},
		{ID: "clickhouse.table.create", Label: "Create table", Icon: icon("plus"), RouteID: "clickhouse.table.create", Params: map[string]string{"database": "${resource.uid}"}, OnSuccess: &plugin.ActionSuccess{SelectTab: "tables"}},
		{ID: "clickhouse.table.rename", Label: "Rename", Icon: icon("pencil"), RouteID: "clickhouse.table.rename", Params: tableParams(), OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList}},
		{ID: "clickhouse.column.add", Label: "Add column", Icon: icon("columns-3"), RouteID: "clickhouse.column.add", Params: tableParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "columns"}},
		{ID: "clickhouse.column.alter", Label: "Modify column", Icon: icon("pencil"), RouteID: "clickhouse.column.alter", Params: map[string]string{"database": "${resource.scope}", "table": "${resource.namespace}", "name": "${resource.name}"}, Confirm: true, ConfirmText: "Modify this column? Changing its type rewrites the affected data via a mutation.", OnSuccess: &plugin.ActionSuccess{SelectTab: "columns"}},
		{ID: "clickhouse.column.drop", Label: "Drop column", Icon: icon("trash"), RouteID: "clickhouse.column.drop", Params: map[string]string{"database": "${resource.scope}", "table": "${resource.namespace}", "name": "${resource.name}"}, Confirm: true, ConfirmText: "Drop this column? Its data is permanently removed.", OnSuccess: &plugin.ActionSuccess{SelectTab: "columns"}},
		{ID: "clickhouse.index.create", Label: "Add index", Icon: icon("plus"), RouteID: "clickhouse.index.create", Params: tableParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "indexes"}},
		{ID: "clickhouse.index.drop", Label: "Drop index", Icon: icon("trash"), RouteID: "clickhouse.index.drop", Params: map[string]string{"database": "${resource.scope}", "table": "${resource.namespace}", "name": "${resource.name}"}, Confirm: true, ConfirmText: "Drop this data-skipping index?", OnSuccess: &plugin.ActionSuccess{SelectTab: "indexes"}},
		{ID: "clickhouse.constraint.add", Label: "Add constraint", Icon: icon("plus"), RouteID: "clickhouse.constraint.add", Params: tableParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "constraints"}},
		{ID: "clickhouse.constraint.drop", Label: "Drop constraint", Icon: icon("trash"), RouteID: "clickhouse.constraint.drop", Params: map[string]string{"database": "${resource.scope}", "table": "${resource.namespace}", "name": "${resource.name}"}, Confirm: true, ConfirmText: "Drop this constraint?", OnSuccess: &plugin.ActionSuccess{SelectTab: "constraints"}},
		{ID: "clickhouse.table.truncate", Label: "Truncate", Icon: icon("trash"), RouteID: "clickhouse.table.truncate", Params: tableParams(), Confirm: true, ConfirmText: "Truncate this table? Every row will be deleted."},
		{ID: "clickhouse.table.drop", Label: "Drop", Icon: icon("trash-2"), RouteID: "clickhouse.table.drop", Params: tableParams(), Confirm: true, ConfirmText: "Drop this table? The table definition and data will be permanently deleted."},
		{ID: "clickhouse.view.drop", Label: "Drop", Icon: icon("trash-2"), RouteID: "clickhouse.view.drop", Params: map[string]string{"database": "${resource.namespace}", "view": "${resource.name}"}, Confirm: true, ConfirmText: "Drop this view?"},
		{ID: "clickhouse.process.kill", Label: "Kill query", Icon: icon("circle-stop"), RouteID: "clickhouse.process.kill", Params: map[string]string{"id": "${resource.uid}"}, Confirm: true, ConfirmText: "Kill this running query? It is terminated on the server."},
		{ID: "clickhouse.mutation.kill", Label: "Kill mutation", Icon: icon("circle-stop"), RouteID: "clickhouse.mutation.kill", Params: map[string]string{"database": "${resource.namespace}", "table": "${resource.scope}", "id": "${resource.name}"}, Confirm: true, ConfirmText: "Kill this mutation? Cancelling a partially applied mutation can leave parts in a mixed state."},
		{ID: "clickhouse.merge.stop", Label: "Stop merges", Icon: icon("pause"), RouteID: "clickhouse.merge.stop", Params: map[string]string{"database": "${resource.namespace}", "table": "${resource.scope}"}, Confirm: true, ConfirmText: "Stop background merges for this table? Parts stop merging until merges are started again."},
		{ID: "clickhouse.merge.start", Label: "Start merges", Icon: icon("play"), RouteID: "clickhouse.merge.start", Params: map[string]string{"database": "${resource.namespace}", "table": "${resource.scope}"}},
		{ID: "clickhouse.user.create", Label: "Create user", Icon: icon("user-plus"), RouteID: "clickhouse.user.create"},
		{ID: "clickhouse.user.grant", Label: "Grant privilege", Icon: icon("key-round"), RouteID: "clickhouse.user.grant", Params: map[string]string{"user": "${resource.name}"}, Confirm: true, ConfirmText: "Grant privileges to this user? This can expand access to databases, tables, or cluster-level operations."},
		{ID: "clickhouse.user.drop", Label: "Drop user", Icon: icon("trash-2"), RouteID: "clickhouse.user.drop", Params: map[string]string{"user": "${resource.name}"}, Confirm: true, ConfirmText: "Drop this user? They can no longer connect.", OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList}},
	}
}
