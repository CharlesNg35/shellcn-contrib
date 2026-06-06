// Package mssql implements the Microsoft SQL Server protocol plugin.
package mssql

import (
	"context"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type Plugin struct{}

func New() *Plugin { return &Plugin{} }

const mssqlIconSvg = `<svg xmlns="http://www.w3.org/2000/svg"  viewBox="0 0 48 48" width="48px" height="48px"><path fill="#cfd8dc" d="M23.084,11.277c-1.633-2.449-1.986-5.722-2.063-7.067c-4.148,0.897-8.269,2.506-8.031,3.691 c0.03,0.149,0.218,0.328,0.53,0.502l-0.488,0.873c-0.596-0.334-0.931-0.719-1.022-1.179c-0.269-1.341,1.25-2.554,4.642-3.709 c2.316-0.789,4.652-1.26,4.751-1.279l0.597-0.12L22,3.6c0,0.042,0.026,4.288,1.916,7.123L23.084,11.277z"/><path fill="#cfd8dc" d="M24.751,43H24.5c-8.192,0-17.309-2.573-18.386-6.879c-0.657-2.63,1.492-5.536,6.214-8.401 l0.52,0.854c-4.249,2.579-6.296,5.172-5.763,7.305c0.935,3.738,9.575,6.068,17.153,6.12c0.901-1.347,5.742-9.26,2.979-19.873 l0.967-0.252c3.149,12.092-3.218,20.837-3.282,20.924L24.751,43z"/><path fill="#cfd8dc" d="M9.931,39.306c-0.539,0-0.806-0.059-0.85-0.07c-0.176-0.043-0.314-0.178-0.362-0.352 c-0.049-0.174,0.001-0.361,0.129-0.488c0.072-0.072,7.197-7.208,8.159-12.978l0.986,0.164c-0.827,4.964-5.715,10.623-7.656,12.707 c1.939-0.111,6.835-1.019,16.234-6.28c-7.335-0.804-8.495-6.676-8.507-6.739l0.983-0.181c0.047,0.246,1.226,6.011,9.244,6.011 c0.003,0,0.005,0,0.008,0l0,0c0.227,0,0.424,0.152,0.482,0.37c0.06,0.218-0.036,0.449-0.231,0.563 C17.315,38.542,11.867,39.305,9.931,39.306z"/><path fill="#cfd8dc" d="M14.524,41.7c-0.207,0-0.395-0.128-0.468-0.325c-0.079-0.211-0.007-0.45,0.177-0.582 c0.034-0.025,1.813-1.338,3.706-4.228c-0.728-0.322-1.465-0.698-2.196-1.137c-0.888-0.533-1.559-1.105-2.06-1.691 c-2.57,0.678-4.942,0.946-7.025,0.769l0.084-0.996c1.876,0.159,4.009-0.063,6.321-0.64c-1.573-2.688-0.129-5.356-0.109-5.392 l0.874,0.487c-0.067,0.122-1.265,2.37,0.249,4.633c2.201-0.632,4.549-1.567,6.979-2.782c0.559-1.835,0.996-3.922,1.225-6.276 c0.016-0.161,0.108-0.304,0.248-0.385s0.311-0.088,0.458-0.021c0.032,0.015,3.264,1.491,5.604,2.454 c0.17,0.07,0.288,0.228,0.307,0.411c0.02,0.183-0.063,0.361-0.216,0.465c-2.289,1.56-4.563,2.913-6.778,4.042 c-0.702,2.225-1.571,4.077-2.459,5.591c3.702,1.383,6.915,1.404,6.956,1.404c0.228,0,0.427,0.154,0.484,0.375 c0.057,0.221-0.042,0.452-0.241,0.563c-4.54,2.522-11.767,3.232-12.072,3.261C14.556,41.699,14.54,41.7,14.524,41.7z M18.909,36.967c-1.04,1.614-2.062,2.773-2.826,3.53c1.998-0.294,5.501-0.938,8.408-2.139 C23.099,38.187,21.084,37.807,18.909,36.967z M14.767,33.431c0.393,0.392,0.883,0.775,1.49,1.14 c0.736,0.442,1.483,0.817,2.22,1.135c0.754-1.264,1.501-2.781,2.142-4.568C18.598,32.1,16.636,32.868,14.767,33.431z M23.202,24.329c-0.205,1.768-0.521,3.381-0.913,4.85c1.66-0.885,3.354-1.896,5.062-3.026 C25.802,25.497,24.099,24.734,23.202,24.329z"/><path fill="#cfd8dc" d="M17.924,10.6c-0.117,0-0.233-0.042-0.325-0.12c-1.61-1.378-3.505-4.182-3.585-4.301 c-0.129-0.191-0.109-0.446,0.046-0.616c0.154-0.171,0.408-0.211,0.608-0.102c0.011,0.003,0.938,0.385,7.217,1.431 c0.181,0.03,0.33,0.156,0.39,0.328c0.061,0.172,0.022,0.364-0.1,0.5c-1.758,1.953-3.979,2.813-4.073,2.848 C18.044,10.589,17.983,10.6,17.924,10.6z M15.647,6.746c0.631,0.849,1.54,1.996,2.372,2.769c0.511-0.233,1.657-0.818,2.744-1.798 C18.18,7.276,16.604,6.962,15.647,6.746z"/><path fill="#b71c1c" d="M21.843,24.4c-0.068,0-0.137-0.014-0.201-0.042c-0.199-0.088-0.319-0.294-0.296-0.51 c0.292-2.749-3.926-3.852-3.969-3.862c-0.174-0.044-0.312-0.179-0.359-0.352s0.002-0.359,0.129-0.486 c0.207-0.207,5.139-5.098,11.327-7.784c0.173-0.075,0.369-0.047,0.515,0.07c0.145,0.118,0.212,0.307,0.174,0.489 c-1.186,5.744-6.71,12.044-6.944,12.309C22.12,24.341,21.982,24.4,21.843,24.4z M18.455,19.285 c1.184,0.445,3.258,1.475,3.783,3.356c1.449-1.808,4.542-5.973,5.697-9.934C23.548,14.817,19.854,17.999,18.455,19.285z"/><path fill="#b71c1c" d="M13.079,28.36l-0.475-0.88c1.883-1.015,4.04-2.883,5.807-5.054c-1.504,1.03-2.365,1.735-2.392,1.758 l-0.639-0.77c0.039-0.032,1.764-1.447,4.631-3.22c0.787-1.266,1.392-2.568,1.703-3.816c0.053-0.212,0.099-0.417,0.136-0.615 c-1.925-0.687-3.701-1.094-4.921-1.269c-0.185-0.026-0.339-0.153-0.401-0.328c-0.062-0.175-0.021-0.371,0.104-0.507 c0.085-0.092,2.116-2.268,4.654-3.463c0.197-0.093,0.433-0.047,0.581,0.114c0.067,0.073,1.44,1.615,1.091,4.805 c1.155,0.45,2.345,0.997,3.491,1.648c2.759-1.24,5.892-2.356,9.229-3.03c0.172-0.034,0.363,0.028,0.481,0.168 c0.117,0.14,0.149,0.333,0.083,0.503c-1.3,3.332-4.786,6.891-4.934,7.041c-0.101,0.102-0.239,0.153-0.383,0.148 c-0.143-0.008-0.275-0.076-0.365-0.188c-1.12-1.408-2.584-2.574-4.163-3.523c-2.175,1.004-4.101,2.078-5.684,3.049 C18.693,24.084,15.644,26.979,13.079,28.36z M27.492,17.396c1.29,0.832,2.491,1.81,3.484,2.948 c0.828-0.898,2.815-3.168,3.942-5.422C32.268,15.532,29.76,16.415,27.492,17.396z M22.799,16.122 c-0.033,0.163-0.071,0.33-0.113,0.5c-0.21,0.839-0.544,1.701-0.972,2.561c1.096-0.626,2.309-1.272,3.618-1.898 C24.494,16.841,23.639,16.455,22.799,16.122z M18.048,13.672c1.111,0.218,2.48,0.574,3.941,1.086 c0.152-1.843-0.346-2.972-0.647-3.472C19.966,12.004,18.761,13.014,18.048,13.672z"/><path fill="#b71c1c" d="M18.05,18.5c0,4.38-3.65,7.86-6.28,10.4c-0.44,0.43-1.93,0.5-1.93,0.5 c0.37-0.38,0.79-0.78,1.24-1.21c2.5-2.42,5.97-5.73,5.97-9.69c0-4.69-1.89-6.54-3.38-8.02c-0.66-0.67-1.22-1.31-1.56-2.09 l0.31-0.13c0.34,0.15,0.73,0.32,1.03,0.45c0.24,0.35,0.56,0.69,0.93,1.06C15.91,11.3,18.05,13.4,18.05,18.5z"/><path fill="#b71c1c" d="M42.935,19.794c0,0-0.605,0.086-0.775,0.106c-8.76,0.97-17.8,3.49-22.97,5.56 c-1.87,0.75-3.81,1.66-5.58,2.68c-0.01,0.01-0.02,0.01-0.04,0.02C12.53,28.76,10,30,7.95,31.09c3-3.19,8.62-5.65,10.86-6.55 c5.07-2.03,13.78-4.48,22.35-5.53c-1.01-1.18-3.48-3.68-8.34-5.54c-2.84-1.1-7.16-1.72-10.97-2.27c-6.06-0.87-9.51-1.45-9.84-3.1 c-0.07-0.33-0.02-0.66,0.13-0.98c0.33,0.54,0.8,0.92,1.11,1.14c0.15,0.1,0.26,0.16,0.3,0.18l0.01,0.01 c1.42,0.75,5.25,1.3,8.44,1.76c3.86,0.56,8.23,1.19,11.18,2.32c6.87,2.65,9.24,6.44,9.34,6.6 C42.61,19.28,42.935,19.794,42.935,19.794z"/></svg>`

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:          plugin.CurrentAPIVersion,
		Name:                protocolName,
		Version:             "0.1.0",
		Title:               "Microsoft SQL Server",
		Description:         "SQL Server cockpit with databases, schemas, table data, views, procedures, users, jobs, T-SQL editor, DDL helpers, and safety controls.",
		Icon:                plugin.Icon{Type: plugin.IconSVG, Value: mssqlIconSvg},
		Category:            plugin.CategoryDatabases,
		Config:              configSchema(),
		Capabilities:        []plugin.Capability{"sql", "schema", "tables", "query_editor", "users", "jobs"},
		SupportedTransports: []plugin.Transport{plugin.TransportDirect},
		Layout:              plugin.LayoutSidebarTree,
		Tree:                tree(),
		Resources:           resources(),
		Actions:             actions(),
		Streams: []plugin.Stream{
			{ID: "mssql.query", Kind: plugin.StreamLogs, RouteID: "mssql.query"},
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
		{Key: "databases", Label: "Databases", Icon: icon("database"), Source: plugin.DataSource{RouteID: "mssql.databases.tree"}, Ref: &plugin.ResourceRef{Kind: "server", Name: "Databases", UID: "server"}},
		{Key: "users", Label: "Users", Icon: icon("users"), Source: plugin.DataSource{RouteID: "mssql.users.tree"}, ResourceKind: "user"},
		{Key: "jobs", Label: "Jobs", Icon: icon("calendar-clock"), Source: plugin.DataSource{RouteID: "mssql.jobs.tree"}, ResourceKind: "job"},
	}
}

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		serverResource(),
		databaseResource(),
		schemaResource(),
		tableResource(),
		viewResource(),
		procedureResource(),
		userResource(),
		jobResource(),
	}
}

// serverResource is the connection-level view opened by clicking the Databases
// tree group: the database list plus a SQL console.
func serverResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "server", Title: "Databases",
		List: plugin.DataSource{RouteID: "mssql.databases.list"},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "Databases"},
			Tabs: []plugin.Panel{
				{Key: "databases", Label: "Databases", Icon: icon("database"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "mssql.databases.list"}, Config: plugin.TableConfig{ActionIDs: []string{"mssql.database.create"}}},
				{Key: "console", Label: "SQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "mssql.query", Method: plugin.MethodWS}, Config: queryConfig("SELECT SYSDATETIMEOFFSET() AS now;")},
			},
		},
	}
}

func databaseResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "database", Title: "Databases",
		List: plugin.DataSource{RouteID: "mssql.databases.list"},
		Actions: plugin.ResourceActions{
			Toolbar: []string{"mssql.database.create"},
			Row:     []string{"mssql.database.drop"},
			Detail:  []string{"mssql.schema.create", "mssql.database.drop"},
		},
		Columns: []plugin.Column{
			{Key: "name", Label: "Database", Sortable: true},
			{Key: "state", Label: "State", Sortable: true},
			{Key: "recovery", Label: "Recovery"},
			{Key: "compatibility", Label: "Compat", Type: plugin.ColumnNumber},
			{Key: "created", Label: "Created", Type: plugin.ColumnDateTime, Sortable: true},
		},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "mssql.database.overview", Params: map[string]string{"database": "${resource.uid}"}}, Config: objectDetailConfig()},
			{Key: "schemas", Label: "Schemas", Icon: icon("folder-tree"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "mssql.schemas.list", Params: map[string]string{"database": "${resource.uid}"}}, Config: plugin.TableConfig{Columns: schemaColumns(), ActionIDs: []string{"mssql.schema.create"}, RowActionIDs: []string{"mssql.schema.drop"}}},
			{Key: "relations", Label: "Relationships", Icon: icon("workflow"), Type: plugin.PanelGraph, Source: &plugin.DataSource{RouteID: "mssql.relations.graph", Params: map[string]string{"database": "${resource.uid}"}}, Config: plugin.GraphConfig{Layout: plugin.GraphLayoutGrid, FitView: true, Exportable: boolPtr(true)}},
			{Key: "query", Label: "Query", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "mssql.query", Method: plugin.MethodWS, Params: map[string]string{"database": "${resource.uid}"}}, Config: queryConfig("SELECT SYSDATETIMEOFFSET() AS now;")},
		}},
	}
}

func schemaResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "schema", Title: "Schemas",
		List:    plugin.DataSource{RouteID: "mssql.schemas.list"},
		Columns: schemaColumns(),
		Actions: plugin.ResourceActions{Row: []string{"mssql.schema.drop"}},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "mssql.schema.overview", Params: map[string]string{"database": "${resource.namespace}", "schema": "${resource.name}"}}, Config: objectDetailConfig()},
			{Key: "tables", Label: "Tables", Icon: icon("table-2"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "mssql.tables.list", Params: map[string]string{"database": "${resource.namespace}", "schema": "${resource.name}"}}, Config: plugin.TableConfig{Columns: tableColumns(), ActionIDs: []string{"mssql.table.create"}, RowActionIDs: []string{"mssql.table.truncate", "mssql.table.drop"}}},
			{Key: "views", Label: "Views", Icon: icon("panel-top"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "mssql.views.list", Params: map[string]string{"database": "${resource.namespace}", "schema": "${resource.name}"}}, Config: plugin.TableConfig{Columns: viewColumns(), RowActionIDs: []string{"mssql.view.drop"}}},
			{Key: "procedures", Label: "Procedures", Icon: icon("function-square"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "mssql.procedures.list", Params: map[string]string{"database": "${resource.namespace}", "schema": "${resource.name}"}}, Config: plugin.TableConfig{Columns: procedureColumns()}},
		}},
	}
}

func tableResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "table", Title: "Tables",
		List:    plugin.DataSource{RouteID: "mssql.tables.list"},
		Columns: tableColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{"mssql.table.truncate", "mssql.table.drop"},
			Detail: []string{"mssql.table.rename", "mssql.table.truncate", "mssql.table.drop"},
		},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.namespace}.${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "data", Label: "Data", Icon: icon("table"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "mssql.table.rows", Params: objectParams()}, Config: dataGridConfig()},
			{Key: "columns", Label: "Columns", Icon: icon("columns-3"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "mssql.table.columns", Params: objectParams()}, Config: plugin.TableConfig{Columns: columnColumns(), ActionIDs: []string{"mssql.column.add", "mssql.column.alter"}, RowActionIDs: []string{"mssql.column.drop"}}},
			{Key: "indexes", Label: "Indexes", Icon: icon("key-round"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "mssql.table.indexes", Params: objectParams()}, Config: plugin.TableConfig{Columns: indexColumns(), ActionIDs: []string{"mssql.index.create"}, RowActionIDs: []string{"mssql.index.drop"}}},
			{Key: "constraints", Label: "Constraints", Icon: icon("shield-check"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "mssql.table.constraints", Params: objectParams()}, Config: plugin.TableConfig{Columns: constraintColumns(), ActionIDs: []string{"mssql.constraint.add"}, RowActionIDs: []string{"mssql.constraint.drop"}}},
			{Key: "query", Label: "SQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "mssql.query", Method: plugin.MethodWS, Params: map[string]string{"database": "${resource.namespace}"}}, Config: queryConfig("SELECT TOP (100) * FROM ${resource.name};")},
		}},
	}
}

func viewResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "view", Title: "Views",
		List: plugin.DataSource{RouteID: "mssql.views.list"}, Columns: viewColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{"mssql.view.drop"},
			Detail: []string{"mssql.view.drop"},
		},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.namespace}.${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "data", Label: "Data", Icon: icon("table-properties"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "mssql.view.rows", Params: objectParams()}},
			{Key: "definition", Label: "Definition", Icon: icon("code"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: "mssql.view.definition", Params: objectParams()}},
			{Key: "query", Label: "SQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "mssql.query", Method: plugin.MethodWS, Params: map[string]string{"database": "${resource.namespace}"}}, Config: queryConfig("SELECT TOP (100) * FROM ${resource.name};")},
		}},
	}
}

func procedureResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "procedure", Title: "Procedures",
		List: plugin.DataSource{RouteID: "mssql.procedures.list"}, Columns: procedureColumns(),
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.namespace}.${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "definition", Label: "Definition", Icon: icon("code"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: "mssql.procedure.definition", Params: objectParams()}},
		}},
	}
}

func userResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "user", Title: "Users",
		List:    plugin.DataSource{RouteID: "mssql.users.list"},
		Columns: []plugin.Column{{Key: "name", Label: "User", Sortable: true}, {Key: "database", Label: "Database", Sortable: true}, {Key: "type", Label: "Type"}, {Key: "login", Label: "Login"}, {Key: "created", Label: "Created", Type: plugin.ColumnDateTime}},
		Actions: plugin.ResourceActions{
			Toolbar: []string{"mssql.user.create"},
			Row:     []string{"mssql.user.drop"},
			Detail:  []string{"mssql.user.grant", "mssql.user.drop"},
		},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.namespace}.${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "mssql.user.overview", Params: map[string]string{"database": "${resource.namespace}", "user": "${resource.name}"}}, Config: objectDetailConfig()},
		}},
	}
}

func jobResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "job", Title: "Jobs",
		List:    plugin.DataSource{RouteID: "mssql.jobs.list"},
		Columns: []plugin.Column{{Key: "name", Label: "Job", Sortable: true}, {Key: "enabled", Label: "Enabled", Type: plugin.ColumnBool}, {Key: "owner", Label: "Owner"}, {Key: "created", Label: "Created", Type: plugin.ColumnDateTime}},
		Actions: plugin.ResourceActions{
			Detail: []string{"mssql.job.start", "mssql.job.enable", "mssql.job.disable"},
		},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "mssql.job.overview", Params: map[string]string{"id": "${resource.uid}"}}, Config: objectDetailConfig()},
		}},
	}
}

func objectParams() map[string]string {
	return map[string]string{"id": "${resource.uid}"}
}

func tableParams() map[string]string {
	return objectParams()
}

func dataGridConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Editable:      true,
		StagedEdits:   true,
		Exportable:    true,
		EmptyText:     "No rows.",
		Insert:        &plugin.DataSource{RouteID: "mssql.table.row.insert", Method: plugin.MethodPost, Params: objectParams()},
		Update:        &plugin.DataSource{RouteID: "mssql.table.row.update", Method: plugin.MethodPatch, Params: objectParams()},
		Delete:        &plugin.DataSource{RouteID: "mssql.table.row.delete", Method: plugin.MethodDelete, Params: objectParams()},
		ColumnsSource: &plugin.DataSource{RouteID: "mssql.table.columns", Params: objectParams()},
	}
}

func queryConfig(initial string) plugin.QueryEditorConfig {
	return plugin.QueryEditorConfig{
		Language:          "sql",
		Label:             "T-SQL",
		ExecuteLabel:      "Run query",
		CancelLabel:       "Cancel query",
		RunningLabel:      "Running...",
		EmptyText:         "Run a query to see results.",
		InitialQuery:      initial,
		CancelRouteID:     "mssql.query.cancel",
		CompletionRouteID: "mssql.completion",
		Exportable:        true,
	}
}

func schemaColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Schema", Sortable: true}, {Key: "database", Label: "Database", Sortable: true}, {Key: "owner", Label: "Owner"}, {Key: "tables", Label: "Tables", Type: plugin.ColumnNumber}, {Key: "views", Label: "Views", Type: plugin.ColumnNumber}}
}

func tableColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Table", Sortable: true}, {Key: "schema", Label: "Schema", Sortable: true}, {Key: "database", Label: "Database"}, {Key: "rows", Label: "Rows", Type: plugin.ColumnNumber}, {Key: "size", Label: "Size", Type: plugin.ColumnBytes}, {Key: "created", Label: "Created", Type: plugin.ColumnDateTime}}
}

func viewColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "View", Sortable: true}, {Key: "schema", Label: "Schema", Sortable: true}, {Key: "database", Label: "Database"}, {Key: "created", Label: "Created", Type: plugin.ColumnDateTime}, {Key: "modified", Label: "Modified", Type: plugin.ColumnDateTime}}
}

func procedureColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Procedure", Sortable: true}, {Key: "schema", Label: "Schema", Sortable: true}, {Key: "database", Label: "Database"}, {Key: "created", Label: "Created", Type: plugin.ColumnDateTime}, {Key: "modified", Label: "Modified", Type: plugin.ColumnDateTime}}
}

func columnColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Column", Sortable: true}, {Key: "type", Label: "Type"}, {Key: "nullable", Label: "Nullable", Type: plugin.ColumnBool}, {Key: "identity", Label: "Identity", Type: plugin.ColumnBool}, {Key: "default", Label: "Default"}, {Key: "position", Label: "Position", Type: plugin.ColumnNumber}}
}

func indexColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Index", Sortable: true}, {Key: "columns", Label: "Columns"}, {Key: "unique", Label: "Unique", Type: plugin.ColumnBool}, {Key: "primary", Label: "Primary", Type: plugin.ColumnBool}, {Key: "type", Label: "Type"}}
}

func constraintColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Constraint", Sortable: true}, {Key: "type", Label: "Type"}, {Key: "column", Label: "Column"}, {Key: "referenced", Label: "Referenced"}}
}

func actions() []plugin.Action {
	return []plugin.Action{
		{ID: "mssql.database.create", Label: "Create database", Icon: icon("plus"), RouteID: "mssql.database.create"},
		{ID: "mssql.database.drop", Label: "Drop database", Icon: icon("trash-2"), RouteID: "mssql.database.drop", Params: map[string]string{"database": "${resource.uid}"}, Confirm: true, ConfirmText: "Drop this database? All of its schemas and data will be permanently deleted."},
		{ID: "mssql.schema.create", Label: "Create schema", Icon: icon("folder-plus"), RouteID: "mssql.schema.create", Params: map[string]string{"database": "${resource.uid}"}, OnSuccess: &plugin.ActionSuccess{SelectTab: "schemas"}},
		{ID: "mssql.schema.drop", Label: "Drop schema", Icon: icon("trash-2"), RouteID: "mssql.schema.drop", Params: map[string]string{"database": "${resource.namespace}", "schema": "${resource.name}"}, Confirm: true, ConfirmText: "Drop this schema? It must be empty."},
		{ID: "mssql.table.create", Label: "Create table", Icon: icon("plus"), RouteID: "mssql.table.create", Params: map[string]string{"database": "${resource.namespace}", "schema": "${resource.name}"}, OnSuccess: &plugin.ActionSuccess{SelectTab: "tables"}},
		{ID: "mssql.table.rename", Label: "Rename", Icon: icon("pencil"), RouteID: "mssql.table.rename", Params: objectParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "data"}},
		{ID: "mssql.column.add", Label: "Add column", Icon: icon("columns-3"), RouteID: "mssql.column.add", Params: objectParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "columns"}},
		{ID: "mssql.column.alter", Label: "Alter column", Icon: icon("pencil"), RouteID: "mssql.column.alter", Params: objectParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "columns"}},
		{ID: "mssql.column.drop", Label: "Drop column", Icon: icon("trash"), RouteID: "mssql.column.drop", Params: map[string]string{"id": "${resource.scope}", "name": "${resource.name}"}, Confirm: true, ConfirmText: "Drop this column? Its data is permanently removed.", OnSuccess: &plugin.ActionSuccess{SelectTab: "columns"}},
		{ID: "mssql.constraint.add", Label: "Add constraint", Icon: icon("plus"), RouteID: "mssql.constraint.add", Params: objectParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "constraints"}},
		{ID: "mssql.constraint.drop", Label: "Drop constraint", Icon: icon("trash"), RouteID: "mssql.constraint.drop", Params: map[string]string{"id": "${resource.scope}", "name": "${resource.name}"}, Confirm: true, ConfirmText: "Drop this constraint?", OnSuccess: &plugin.ActionSuccess{SelectTab: "constraints"}},
		{ID: "mssql.index.create", Label: "Create index", Icon: icon("plus"), RouteID: "mssql.index.create", Params: objectParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "indexes"}},
		{ID: "mssql.index.drop", Label: "Drop index", Icon: icon("trash"), RouteID: "mssql.index.drop", Params: map[string]string{"id": "${resource.scope}", "name": "${resource.name}"}, Confirm: true, ConfirmText: "Drop this index?", OnSuccess: &plugin.ActionSuccess{SelectTab: "indexes"}},
		{ID: "mssql.table.truncate", Label: "Truncate", Icon: icon("trash"), RouteID: "mssql.table.truncate", Params: tableParams(), Confirm: true, ConfirmText: "Truncate this table? Every row will be deleted."},
		{ID: "mssql.table.drop", Label: "Drop", Icon: icon("trash-2"), RouteID: "mssql.table.drop", Params: tableParams(), Confirm: true, ConfirmText: "Drop this table? The table definition and data will be permanently deleted."},
		{ID: "mssql.view.drop", Label: "Drop", Icon: icon("trash-2"), RouteID: "mssql.view.drop", Params: objectParams(), Confirm: true, ConfirmText: "Drop this view?"},
		{ID: "mssql.job.start", Label: "Start job", Icon: icon("play"), RouteID: "mssql.job.start", Params: map[string]string{"name": "${resource.name}"}, Confirm: true, ConfirmText: "Start this SQL Agent job now?"},
		{ID: "mssql.job.enable", Label: "Enable job", Icon: icon("toggle-right"), RouteID: "mssql.job.enable", Params: map[string]string{"name": "${resource.name}"}},
		{ID: "mssql.job.disable", Label: "Disable job", Icon: icon("toggle-left"), RouteID: "mssql.job.disable", Params: map[string]string{"name": "${resource.name}"}},
		{ID: "mssql.user.create", Label: "Create user", Icon: icon("user-plus"), RouteID: "mssql.user.create"},
		{ID: "mssql.user.grant", Label: "Grant permission", Icon: icon("shield-check"), RouteID: "mssql.user.grant", Params: map[string]string{"database": "${resource.namespace}", "user": "${resource.name}"}, Confirm: true, ConfirmText: "Grant this permission to the user?"},
		{ID: "mssql.user.drop", Label: "Drop user", Icon: icon("user-minus"), RouteID: "mssql.user.drop", Params: map[string]string{"database": "${resource.namespace}", "user": "${resource.name}"}, Confirm: true, ConfirmText: "Drop this database user?"},
	}
}
