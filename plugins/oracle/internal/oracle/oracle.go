// Package oracle implements the Oracle Database protocol plugin.
package oracle

import (
	"context"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type Plugin struct{}

func New() *Plugin { return &Plugin{} }

const oracleSvgIcon = `<svg width="800px" height="800px" viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg" fill="none"><path fill="#EA1B22" fill-rule="evenodd" d="M.1 8c0 2.761 2.237 5 4.997 5h5.806A4.999 4.999 0 0015.9 8c0-2.761-2.237-5-4.997-5H5.097A4.999 4.999 0 00.1 8zm13.911 0a3.235 3.235 0 01-3.234 3.237h-5.55A3.235 3.235 0 011.991 8a3.235 3.235 0 013.234-3.236h5.551A3.235 3.235 0 0114.011 8z" clip-rule="evenodd"/></svg>`

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:          plugin.CurrentAPIVersion,
		Name:                protocolName,
		Version:             "0.1.0",
		Title:               "Oracle Database",
		Description:         "Oracle cockpit with schemas, tables, views, procedures, packages, sequences, users, tablespaces, sessions, SQL editor, DDL helpers, and safety controls.",
		Icon:                plugin.Icon{Type: plugin.IconSVG, Value: oracleSvgIcon},
		Category:            plugin.CategoryDatabases,
		Config:              configSchema(),
		Capabilities:        []plugin.Capability{"sql", "schema", "tables", "query_editor", "users", "sessions"},
		SupportedTransports: []plugin.Transport{plugin.TransportDirect},
		Layout:              plugin.LayoutSidebarTree,
		Tree:                tree(),
		Resources:           resources(),
		Actions:             actions(),
		Streams: []plugin.Stream{
			{ID: "oracle.query", Kind: plugin.StreamLogs, RouteID: "oracle.query"},
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
		{Key: "schemas", Label: "Schemas", Icon: icon("folder-tree"), Source: plugin.DataSource{RouteID: "oracle.schemas.tree"}, Ref: &plugin.ResourceRef{Kind: "server", Name: "Schemas", UID: "server"}},
		{Key: "users", Label: "Users", Icon: icon("users"), Source: plugin.DataSource{RouteID: "oracle.users.tree"}, ResourceKind: "user"},
		{Key: "tablespaces", Label: "Tablespaces", Icon: icon("hard-drive"), Source: plugin.DataSource{RouteID: "oracle.tablespaces.tree"}, ResourceKind: "tablespace"},
		{Key: "sessions", Label: "Sessions", Icon: icon("activity"), ResourceKind: "session"},
	}
}

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		serverResource(),
		schemaResource(),
		tableResource(),
		viewResource(),
		procedureResource(),
		packageResource(),
		sequenceResource(),
		userResource(),
		tablespaceResource(),
		sessionResource(),
	}
}

// serverResource is the connection-level view opened by clicking the Schemas
// tree group: the schema list plus a SQL console.
func serverResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "server", Title: "Schemas",
		List: plugin.DataSource{RouteID: "oracle.schemas.list"},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "Schemas"},
			Tabs: []plugin.Panel{
				{Key: "schemas", Label: "Schemas", Icon: icon("folder-tree"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "oracle.schemas.list"}, Config: plugin.TableConfig{Columns: schemaColumns(), ActionIDs: []string{"oracle.schema.create"}, RowActionIDs: []string{"oracle.schema.drop"}}},
				{Key: "console", Label: "SQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "oracle.query", Method: plugin.MethodWS}, Config: queryConfig("SELECT SYSDATE AS now FROM dual")},
			},
		},
	}
}

func schemaResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "schema", Title: "Schemas",
		List:    plugin.DataSource{RouteID: "oracle.schemas.list"},
		Columns: schemaColumns(),
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "oracle.schema.overview", Params: map[string]string{"schema": "${resource.name}"}}, Config: objectDetailConfig()},
			{Key: "tables", Label: "Tables", Icon: icon("table-2"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "oracle.tables.list", Params: map[string]string{"schema": "${resource.name}"}}, Config: plugin.TableConfig{Columns: tableColumns(), ActionIDs: []string{"oracle.table.create"}, RowActionIDs: []string{"oracle.table.truncate", "oracle.table.drop"}}},
			{Key: "relations", Label: "Relationships", Icon: icon("workflow"), Type: plugin.PanelGraph, Source: &plugin.DataSource{RouteID: "oracle.relations.graph", Params: map[string]string{"schema": "${resource.name}"}}, Config: plugin.GraphConfig{Layout: plugin.GraphLayoutGrid, FitView: true, Exportable: boolPtr(true)}},
			{Key: "views", Label: "Views", Icon: icon("panel-top"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "oracle.views.list", Params: map[string]string{"schema": "${resource.name}"}}, Config: plugin.TableConfig{Columns: viewColumns(), RowActionIDs: []string{"oracle.view.drop"}}},
			{Key: "procedures", Label: "Procedures", Icon: icon("function-square"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "oracle.procedures.list", Params: map[string]string{"schema": "${resource.name}"}}, Config: plugin.TableConfig{Columns: procedureColumns()}},
			{Key: "packages", Label: "Packages", Icon: icon("package"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "oracle.packages.list", Params: map[string]string{"schema": "${resource.name}"}}, Config: plugin.TableConfig{Columns: packageColumns()}},
			{Key: "query", Label: "SQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "oracle.query", Method: plugin.MethodWS, Params: map[string]string{"schema": "${resource.name}"}}, Config: queryConfig("SELECT SYSDATE AS now FROM dual")},
		}},
	}
}

func tableResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "table", Title: "Tables",
		List:    plugin.DataSource{RouteID: "oracle.tables.list"},
		Columns: tableColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{"oracle.table.truncate", "oracle.table.drop"},
			Detail: []string{"oracle.table.rename", "oracle.table.truncate", "oracle.table.drop"},
		},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "data", Label: "Data", Icon: icon("table"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "oracle.table.rows", Params: objectParams()}, Config: dataGridConfig()},
			{Key: "columns", Label: "Columns", Icon: icon("columns-3"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "oracle.table.columns", Params: objectParams()}, Config: plugin.TableConfig{Columns: columnColumns(), ActionIDs: []string{"oracle.column.add"}, RowActionIDs: []string{"oracle.column.alter", "oracle.column.rename", "oracle.column.drop"}}},
			{Key: "indexes", Label: "Indexes", Icon: icon("key-round"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "oracle.table.indexes", Params: objectParams()}, Config: plugin.TableConfig{Columns: indexColumns(), ActionIDs: []string{"oracle.index.create"}, RowActionIDs: []string{"oracle.index.drop"}}},
			{Key: "constraints", Label: "Constraints", Icon: icon("shield-check"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "oracle.table.constraints", Params: objectParams()}, Config: plugin.TableConfig{Columns: constraintColumns(), ActionIDs: []string{"oracle.constraint.add"}, RowActionIDs: []string{"oracle.constraint.drop"}}},
			{Key: "sql", Label: "SQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "oracle.query", Method: plugin.MethodWS, Params: map[string]string{"schema": "${resource.namespace}"}}, Config: queryConfig("SELECT * FROM ${resource.name} FETCH FIRST 100 ROWS ONLY")},
		}},
	}
}

func viewResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "view", Title: "Views",
		List: plugin.DataSource{RouteID: "oracle.views.list"}, Columns: viewColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{"oracle.view.drop"},
			Detail: []string{"oracle.view.drop"},
		},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "data", Label: "Data", Icon: icon("table-properties"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "oracle.view.rows", Params: objectParams()}},
			{Key: "definition", Label: "Definition", Icon: icon("code"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: "oracle.view.definition", Params: objectParams()}},
			{Key: "sql", Label: "SQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "oracle.query", Method: plugin.MethodWS, Params: map[string]string{"schema": "${resource.namespace}"}}, Config: queryConfig("SELECT * FROM ${resource.name} FETCH FIRST 100 ROWS ONLY")},
		}},
	}
}

func procedureResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "procedure", Title: "Procedures",
		List: plugin.DataSource{RouteID: "oracle.procedures.list"}, Columns: procedureColumns(),
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "definition", Label: "Definition", Icon: icon("code"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: "oracle.object.definition", Params: objectParams()}},
		}},
	}
}

func packageResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "package", Title: "Packages",
		List: plugin.DataSource{RouteID: "oracle.packages.list"}, Columns: packageColumns(),
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "spec", Label: "Spec", Icon: icon("code"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: "oracle.package.spec", Params: objectParams()}},
			{Key: "body", Label: "Body", Icon: icon("braces"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: "oracle.package.body", Params: objectParams()}},
		}},
	}
}

func sequenceResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "sequence", Title: "Sequences",
		List:    plugin.DataSource{RouteID: "oracle.sequences.list"},
		Columns: []plugin.Column{{Key: "name", Label: "Sequence", Sortable: true}, {Key: "owner", Label: "Owner", Sortable: true}, {Key: "min_value", Label: "Min", Type: plugin.ColumnNumber}, {Key: "max_value", Label: "Max", Type: plugin.ColumnNumber}, {Key: "increment_by", Label: "Increment", Type: plugin.ColumnNumber}, {Key: "last_number", Label: "Last", Type: plugin.ColumnNumber}, {Key: "cache_size", Label: "Cache", Type: plugin.ColumnNumber}},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "oracle.sequence.overview", Params: objectParams()}, Config: objectDetailConfig()},
		}},
	}
}

func userResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "user", Title: "Users",
		List:    plugin.DataSource{RouteID: "oracle.users.list"},
		Columns: []plugin.Column{{Key: "name", Label: "User", Sortable: true}, {Key: "account_status", Label: "Status"}, {Key: "default_tablespace", Label: "Default tablespace"}, {Key: "temporary_tablespace", Label: "Temporary tablespace"}, {Key: "created", Label: "Created", Type: plugin.ColumnDateTime}},
		Actions: plugin.ResourceActions{
			Row:    []string{"oracle.user.drop"},
			Detail: []string{"oracle.user.grant", "oracle.user.lock", "oracle.user.unlock", "oracle.user.drop"},
		},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "oracle.user.overview", Params: map[string]string{"user": "${resource.name}"}}, Config: objectDetailConfig()},
		}},
	}
}

func tablespaceResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "tablespace", Title: "Tablespaces",
		List:    plugin.DataSource{RouteID: "oracle.tablespaces.list"},
		Columns: []plugin.Column{{Key: "name", Label: "Tablespace", Sortable: true}, {Key: "status", Label: "Status"}, {Key: "contents", Label: "Contents"}, {Key: "extent_management", Label: "Extents"}, {Key: "bigfile", Label: "Bigfile", Type: plugin.ColumnBool}},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "oracle.tablespace.overview", Params: map[string]string{"tablespace": "${resource.name}"}}, Config: objectDetailConfig()},
		}},
	}
}

func sessionResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "session", Title: "Sessions",
		List:    plugin.DataSource{RouteID: "oracle.sessions.list"},
		Columns: []plugin.Column{{Key: "sid", Label: "SID", Type: plugin.ColumnNumber, Sortable: true}, {Key: "serial", Label: "Serial", Type: plugin.ColumnNumber}, {Key: "username", Label: "User"}, {Key: "status", Label: "Status"}, {Key: "machine", Label: "Machine"}, {Key: "program", Label: "Program"}, {Key: "logon_time", Label: "Logon", Type: plugin.ColumnDateTime}},
		Actions: plugin.ResourceActions{
			Row:    []string{"oracle.session.kill"},
			Detail: []string{"oracle.session.kill"},
		},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "Session ${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "oracle.session.overview", Params: map[string]string{"id": "${resource.uid}"}}, Config: objectDetailConfig()},
		}},
	}
}

func objectParams() map[string]string {
	return map[string]string{"id": "${resource.uid}"}
}

func dataGridConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Editable:      true,
		StagedEdits:   true,
		Exportable:    true,
		EmptyText:     "No rows.",
		Insert:        &plugin.DataSource{RouteID: "oracle.table.row.insert", Method: plugin.MethodPost, Params: objectParams()},
		Update:        &plugin.DataSource{RouteID: "oracle.table.row.update", Method: plugin.MethodPatch, Params: objectParams()},
		Delete:        &plugin.DataSource{RouteID: "oracle.table.row.delete", Method: plugin.MethodDelete, Params: objectParams()},
		ColumnsSource: &plugin.DataSource{RouteID: "oracle.table.columns", Params: objectParams()},
	}
}

func queryConfig(initial string) plugin.QueryEditorConfig {
	return plugin.QueryEditorConfig{
		Language:          "sql",
		Label:             "Oracle SQL",
		ExecuteLabel:      "Run query",
		CancelLabel:       "Cancel query",
		RunningLabel:      "Running...",
		EmptyText:         "Run a query to see results.",
		InitialQuery:      initial,
		CancelRouteID:     "oracle.query.cancel",
		CompletionRouteID: "oracle.completion",
		Exportable:        true,
	}
}

func schemaColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Schema", Sortable: true}, {Key: "account_status", Label: "Status"}, {Key: "default_tablespace", Label: "Default tablespace"}, {Key: "temporary_tablespace", Label: "Temporary tablespace"}, {Key: "created", Label: "Created", Type: plugin.ColumnDateTime}}
}

func tableColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Table", Sortable: true}, {Key: "owner", Label: "Owner", Sortable: true}, {Key: "tablespace", Label: "Tablespace"}, {Key: "rows", Label: "Rows", Type: plugin.ColumnNumber}, {Key: "blocks", Label: "Blocks", Type: plugin.ColumnNumber}, {Key: "created", Label: "Created", Type: plugin.ColumnDateTime}, {Key: "last_analyzed", Label: "Analyzed", Type: plugin.ColumnDateTime}}
}

func viewColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "View", Sortable: true}, {Key: "owner", Label: "Owner", Sortable: true}, {Key: "created", Label: "Created", Type: plugin.ColumnDateTime}, {Key: "modified", Label: "Modified", Type: plugin.ColumnDateTime}}
}

func procedureColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Procedure", Sortable: true}, {Key: "owner", Label: "Owner", Sortable: true}, {Key: "type", Label: "Type"}, {Key: "status", Label: "Status"}, {Key: "created", Label: "Created", Type: plugin.ColumnDateTime}, {Key: "modified", Label: "Modified", Type: plugin.ColumnDateTime}}
}

func packageColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Package", Sortable: true}, {Key: "owner", Label: "Owner", Sortable: true}, {Key: "status", Label: "Status"}, {Key: "created", Label: "Created", Type: plugin.ColumnDateTime}, {Key: "modified", Label: "Modified", Type: plugin.ColumnDateTime}}
}

func columnColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Column", Sortable: true}, {Key: "type", Label: "Type"}, {Key: "nullable", Label: "Nullable", Type: plugin.ColumnBool}, {Key: "default", Label: "Default"}, {Key: "position", Label: "Position", Type: plugin.ColumnNumber}, {Key: "comments", Label: "Comment"}}
}

func indexColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Index", Sortable: true}, {Key: "columns", Label: "Columns"}, {Key: "unique", Label: "Unique", Type: plugin.ColumnBool}, {Key: "type", Label: "Type"}, {Key: "status", Label: "Status"}}
}

func constraintColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Constraint", Sortable: true}, {Key: "type", Label: "Type"}, {Key: "columns", Label: "Columns"}, {Key: "referenced", Label: "Referenced"}, {Key: "status", Label: "Status"}}
}

func actions() []plugin.Action {
	return []plugin.Action{
		{ID: "oracle.schema.create", Label: "Create schema", Icon: icon("folder-plus"), RouteID: "oracle.schema.create", Confirm: true, ConfirmText: "Create this schema user? It can own objects and may receive database privileges.", OnSuccess: &plugin.ActionSuccess{SelectTab: "schemas"}},
		{ID: "oracle.schema.drop", Label: "Drop schema", Icon: icon("trash-2"), RouteID: "oracle.schema.drop", Params: map[string]string{"schema": "${resource.name}"}, Confirm: true, ConfirmText: "Drop this schema (user)? The user and every object it owns are permanently deleted.", OnSuccess: &plugin.ActionSuccess{SelectTab: "schemas"}},
		{ID: "oracle.table.create", Label: "Create table", Icon: icon("plus"), RouteID: "oracle.table.create", Params: map[string]string{"schema": "${resource.name}"}, OnSuccess: &plugin.ActionSuccess{SelectTab: "tables"}},
		{ID: "oracle.table.rename", Label: "Rename table", Icon: icon("pencil"), RouteID: "oracle.table.rename", Params: objectParams()},
		{ID: "oracle.column.add", Label: "Add column", Icon: icon("columns-3"), RouteID: "oracle.column.add", Params: objectParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "columns"}},
		{ID: "oracle.column.alter", Label: "Modify column", Icon: icon("pencil"), RouteID: "oracle.column.alter", Params: map[string]string{"id": "${record.id}", "name": "${record.name}"}, OnSuccess: &plugin.ActionSuccess{SelectTab: "columns"}},
		{ID: "oracle.column.rename", Label: "Rename column", Icon: icon("pencil-line"), RouteID: "oracle.column.rename", Params: map[string]string{"id": "${record.id}", "name": "${record.name}"}, OnSuccess: &plugin.ActionSuccess{SelectTab: "columns"}},
		{ID: "oracle.column.drop", Label: "Drop column", Icon: icon("trash"), RouteID: "oracle.column.drop", Params: map[string]string{"id": "${record.id}", "name": "${record.name}"}, Confirm: true, ConfirmText: "Drop this column? Its data is permanently removed.", OnSuccess: &plugin.ActionSuccess{SelectTab: "columns"}},
		{ID: "oracle.index.create", Label: "Create index", Icon: icon("plus"), RouteID: "oracle.index.create", Params: objectParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "indexes"}},
		{ID: "oracle.index.drop", Label: "Drop index", Icon: icon("trash"), RouteID: "oracle.index.drop", Params: map[string]string{"id": "${record.id}", "name": "${record.name}"}, Confirm: true, ConfirmText: "Drop this index?", OnSuccess: &plugin.ActionSuccess{SelectTab: "indexes"}},
		{ID: "oracle.constraint.add", Label: "Add constraint", Icon: icon("plus"), RouteID: "oracle.constraint.add", Params: objectParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "constraints"}},
		{ID: "oracle.constraint.drop", Label: "Drop constraint", Icon: icon("trash"), RouteID: "oracle.constraint.drop", Params: map[string]string{"id": "${record.id}", "name": "${record.name}"}, Confirm: true, ConfirmText: "Drop this constraint?", OnSuccess: &plugin.ActionSuccess{SelectTab: "constraints"}},
		{ID: "oracle.table.truncate", Label: "Truncate", Icon: icon("trash"), RouteID: "oracle.table.truncate", Params: objectParams(), Confirm: true, ConfirmText: "Truncate this table? Every row will be deleted."},
		{ID: "oracle.table.drop", Label: "Drop", Icon: icon("trash-2"), RouteID: "oracle.table.drop", Params: objectParams(), Confirm: true, ConfirmText: "Drop this table? The table definition and data will be permanently deleted."},
		{ID: "oracle.view.drop", Label: "Drop", Icon: icon("trash-2"), RouteID: "oracle.view.drop", Params: objectParams(), Confirm: true, ConfirmText: "Drop this view?"},
		{ID: "oracle.session.kill", Label: "Kill session", Icon: icon("circle-x"), RouteID: "oracle.session.kill", Params: map[string]string{"id": "${resource.uid}"}, Confirm: true, ConfirmText: "Kill this session? In-flight work is rolled back and the connection is terminated immediately."},
		{ID: "oracle.user.lock", Label: "Lock account", Icon: icon("lock"), RouteID: "oracle.user.lock", Params: map[string]string{"user": "${resource.name}"}},
		{ID: "oracle.user.unlock", Label: "Unlock account", Icon: icon("lock-open"), RouteID: "oracle.user.unlock", Params: map[string]string{"user": "${resource.name}"}},
		{ID: "oracle.user.grant", Label: "Grant roles/privileges", Icon: icon("shield-plus"), RouteID: "oracle.user.grant", Params: map[string]string{"user": "${resource.name}"}, Confirm: true, ConfirmText: "Grant roles or privileges to this user? This can expand access across schemas, objects, or administrative operations."},
		{ID: "oracle.user.drop", Label: "Drop user", Icon: icon("trash-2"), RouteID: "oracle.user.drop", Params: map[string]string{"user": "${resource.name}"}, Confirm: true, ConfirmText: "Drop this user? The user and every object it owns are permanently deleted."},
	}
}
