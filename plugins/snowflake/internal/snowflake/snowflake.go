// Package snowflake implements the Snowflake protocol plugin.
package snowflake

import (
	"context"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type Plugin struct{}

func New() *Plugin { return &Plugin{} }

const snowflakeIconSVG = `<svg height="1em" style="flex:none;line-height:1" viewBox="0 0 24 24" width="1em" xmlns="http://www.w3.org/2000/svg"><title>Snowflake</title><path clip-rule="evenodd" d="M23.252 10.365l-2.843 1.636 2.843 1.631a1.47 1.47 0 01.697.903 1.492 1.492 0 01-.15 1.135c-.202.342-.53.591-.912.693a1.498 1.498 0 01-1.132-.15l-5.09-2.924a1.473 1.473 0 01-.68-.851 1.446 1.446 0 01-.068-.485 1.5 1.5 0 01.745-1.248l5.09-2.921a1.496 1.496 0 012.044.547 1.479 1.479 0 01-.544 2.034zm-2.692 7.927l-5.087-2.92a1.477 1.477 0 00-.867-.195 1.478 1.478 0 00-.982.468c-.257.276-.4.639-.403 1.017v5.847A1.49 1.49 0 0014.718 24c.828 0 1.497-.668 1.497-1.491v-3.27l2.849 1.636a1.493 1.493 0 002.044-.544 1.49 1.49 0 00-.548-2.04zm-5.87-5.719l-2.116 2.102a.42.42 0 01-.265.112h-.621a.427.427 0 01-.265-.112l-2.115-2.102a.42.42 0 01-.11-.262v-.62a.43.43 0 01.11-.265l2.114-2.102a.426.426 0 01.264-.11h.623a.422.422 0 01.265.11l2.116 2.102a.43.43 0 01.109.265v.62a.428.428 0 01-.11.262zM13 11.99a.442.442 0 00-.113-.266l-.612-.607a.431.431 0 00-.266-.11h-.024a.426.426 0 00-.264.11l-.612.607a.436.436 0 00-.107.266v.024c0 .085.047.202.107.262l.612.61c.061.06.179.11.264.11h.024a.434.434 0 00.266-.11l.612-.61a.429.429 0 00.112-.262v-.024zM3.436 5.704l5.089 2.924c.274.157.578.219.868.195.375-.026.726-.194.983-.47.256-.275.4-.64.403-1.017V1.489C10.78.667 10.11 0 9.284 0c-.829 0-1.498.667-1.498 1.49v3.27l-2.85-1.639a1.496 1.496 0 00-2.045.546 1.489 1.489 0 00.546 2.037zm11.17 3.119c.29.024.594-.038.866-.195l5.087-2.923a1.474 1.474 0 00.697-.903 1.496 1.496 0 00-.149-1.135 1.496 1.496 0 00-2.044-.545L16.215 4.76V1.489C16.215.667 15.546 0 14.718 0c-.83 0-1.497.667-1.497 1.49v5.845a1.491 1.491 0 001.385 1.487zm-5.213 6.354a1.479 1.479 0 00-.868.194l-5.089 2.92a1.476 1.476 0 00-.696.905 1.498 1.498 0 00.148 1.135 1.496 1.496 0 002.044.543l2.851-1.636v3.27c0 .825.67 1.491 1.498 1.491.826 0 1.496-.667 1.496-1.49v-5.847a1.5 1.5 0 00-.401-1.017 1.477 1.477 0 00-.982-.468zm-1.38-2.74c.05-.156.072-.32.068-.484a1.497 1.497 0 00-.751-1.248l-5.084-2.92a1.499 1.499 0 00-2.045.547 1.481 1.481 0 00.549 2.034l2.841 1.636L.75 13.633a1.47 1.47 0 00-.698.903 1.492 1.492 0 00.15 1.135c.202.343.53.592.912.693.382.102.789.048 1.132-.15l5.086-2.924c.345-.195.577-.505.684-.852z" fill="#249EDC" fill-rule="evenodd"></path></svg>`

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:          plugin.CurrentAPIVersion,
		Name:                protocolName,
		Version:             "0.1.0",
		Title:               "Snowflake",
		Description:         "Snowflake cockpit with databases, schemas, tables, views, warehouses, roles, grants, users, stages, pipes, tasks, streams, query history, credit metrics, and a SQL editor.",
		Icon:                plugin.Icon{Type: plugin.IconSVG, Value: snowflakeIconSVG},
		Category:            plugin.CategoryDatabases,
		Config:              configSchema(),
		CredentialKinds:     credentialKinds(),
		Capabilities:        []plugin.Capability{"sql", "schema", "tables", "query_editor", "analytics", "warehouse", "rbac", "metrics"},
		SupportedTransports: []plugin.Transport{plugin.TransportDirect},
		Layout:              plugin.LayoutSidebarTree,
		Tree:                tree(),
		Resources:           resources(),
		Actions:             actions(),
		Streams: []plugin.Stream{
			{ID: "snowflake.query", Kind: plugin.StreamQuery, RouteID: "snowflake.query"},
			{ID: "snowflake.account.metrics", Kind: plugin.StreamMetrics, RouteID: "snowflake.account.metrics"},
			{ID: "snowflake.warehouse.metrics", Kind: plugin.StreamMetrics, RouteID: "snowflake.warehouse.metrics"},
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

func tree() []plugin.TreeGroup {
	return []plugin.TreeGroup{
		{Key: "databases", Label: "Databases", Icon: icon("database"), Source: plugin.DataSource{RouteID: "snowflake.databases.tree"}, Ref: &plugin.ResourceIdentity{Kind: "account", Name: "Account", UID: "account"}},
		{Key: "warehouses", Label: "Warehouses", Icon: icon("server-cog"), ResourceKind: "warehouse"},
		{Key: "roles", Label: "Roles", Icon: icon("shield"), ResourceKind: "role"},
		{Key: "users", Label: "Users", Icon: icon("users"), ResourceKind: "user"},
		{Key: "tasks", Label: "Tasks", Icon: icon("calendar-clock"), ResourceKind: "task"},
		{Key: "streams", Label: "Streams", Icon: icon("waves"), ResourceKind: "stream"},
		{Key: "stages", Label: "Stages", Icon: icon("hard-drive-upload"), ResourceKind: "stage"},
		{Key: "queries", Label: "Query history", Icon: icon("history"), ResourceKind: "query"},
	}
}

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		accountResource(),
		databaseResource(),
		schemaResource(),
		tableResource(),
		viewResource(),
		warehouseResource(),
		roleResource(),
		userResource(),
		stageResource(),
		fileFormatResource(),
		pipeResource(),
		taskResource(),
		streamResource(),
		queryResource(),
	}
}

// accountResource is the connection-level view the Databases tree group opens.
func accountResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "account", Title: "Account",
		List:    plugin.DataSource{RouteID: "snowflake.databases.list"},
		Columns: databaseColumns(),
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "Account"},
			DefaultTab: "databases",
			Tabs: []plugin.Panel{
				{Key: "databases", Label: "Databases", Icon: icon("database"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "snowflake.databases.list"}, Config: plugin.TableConfig{Columns: databaseColumns(), ActionIDs: []string{"snowflake.database.create"}, RowActionIDs: []string{"snowflake.database.drop"}, DefaultSort: &plugin.SortKey{Field: "name"}, EmptyText: "No databases are visible to this role.", Exportable: true}},
				{Key: "session", Label: "Session", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "snowflake.account.overview"}, Config: accountOverviewConfig()},
				{Key: "warehouses", Label: "Warehouses", Icon: icon("server-cog"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "snowflake.warehouses.list"}, Config: warehouseTableConfig()},
				{Key: "credits", Label: "Credits", Icon: icon("activity"), Type: plugin.PanelMetrics, Source: &plugin.DataSource{RouteID: "snowflake.account.metrics", Method: plugin.MethodWS}, Config: accountMetricsConfig()},
				{Key: "history", Label: "Query history", Icon: icon("history"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "snowflake.queries.list"}, Config: queryTableConfig()},
				{Key: "console", Label: "SQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "snowflake.query", Method: plugin.MethodWS}, Config: queryConfig("SELECT CURRENT_VERSION();")},
			},
		},
	}
}

func databaseResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "database", Title: "Databases",
		List:    plugin.DataSource{RouteID: "snowflake.databases.list"},
		Columns: databaseColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{"snowflake.database.create"},
			Row:     []string{"snowflake.database.drop"},
			Detail:  []string{"snowflake.schema.create", "snowflake.database.drop"},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "snowflake.database.overview", Params: databaseParams()}, Config: databaseOverviewConfig()},
				{Key: "schemas", Label: "Schemas", Icon: icon("folder-tree"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "snowflake.schemas.list", Params: databaseParams()}, Config: plugin.TableConfig{Columns: schemaColumns(), ActionIDs: []string{"snowflake.schema.create"}, RowActionIDs: []string{"snowflake.schema.drop"}, DefaultSort: &plugin.SortKey{Field: "name"}, EmptyText: "No schemas in this database.", Exportable: true}},
				{Key: "tables", Label: "Tables", Icon: icon("table-2"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "snowflake.tables.list", Params: databaseParams()}, Config: plugin.TableConfig{Columns: tableColumns(), RowActionIDs: []string{"snowflake.table.truncate", "snowflake.table.drop"}, DefaultSort: &plugin.SortKey{Field: "name"}, EmptyText: "No tables in this database.", Exportable: true}},
				{Key: "views", Label: "Views", Icon: icon("panel-top"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "snowflake.views.list", Params: databaseParams()}, Config: plugin.TableConfig{Columns: viewColumns(), RowActionIDs: []string{"snowflake.view.drop"}, DefaultSort: &plugin.SortKey{Field: "name"}, EmptyText: "No views in this database."}},
				{Key: "stages", Label: "Stages", Icon: icon("hard-drive-upload"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "snowflake.stages.list", Params: databaseParams()}, Config: plugin.TableConfig{Columns: stageColumns(), DefaultSort: &plugin.SortKey{Field: "name"}, EmptyText: "No stages in this database."}},
				{Key: "pipes", Label: "Pipes", Icon: icon("git-branch"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "snowflake.pipes.list", Params: databaseParams()}, Config: plugin.TableConfig{Columns: pipeColumns(), DefaultSort: &plugin.SortKey{Field: "name"}, EmptyText: "No pipes in this database."}},
				{Key: "formats", Label: "File formats", Icon: icon("file-code"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "snowflake.formats.list", Params: databaseParams()}, Config: plugin.TableConfig{Columns: fileFormatColumns(), DefaultSort: &plugin.SortKey{Field: "name"}, EmptyText: "No file formats in this database."}},
				{Key: "tasks", Label: "Tasks", Icon: icon("calendar-clock"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "snowflake.tasks.list", Params: databaseParams()}, Config: taskTableConfig()},
				{Key: "streams", Label: "Streams", Icon: icon("waves"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "snowflake.streams.list", Params: databaseParams()}, Config: plugin.TableConfig{Columns: streamColumns(), DefaultSort: &plugin.SortKey{Field: "name"}, EmptyText: "No streams in this database."}},
				{Key: "query", Label: "SQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "snowflake.query", Method: plugin.MethodWS}, Config: queryConfig("SHOW SCHEMAS IN DATABASE \"${resource.name}\";")},
			},
		},
	}
}

func schemaResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "schema", Title: "Schemas",
		List:    plugin.DataSource{RouteID: "snowflake.schemas.list"},
		Columns: schemaColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{"snowflake.schema.create"},
			Row:     []string{"snowflake.schema.drop"},
			Detail:  []string{"snowflake.table.create", "snowflake.schema.drop"},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.namespace}.${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "snowflake.schema.overview", Params: schemaParams()}, Config: schemaOverviewConfig()},
				{Key: "tables", Label: "Tables", Icon: icon("table-2"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "snowflake.tables.list", Params: schemaParams()}, Config: plugin.TableConfig{Columns: tableColumns(), ActionIDs: []string{"snowflake.table.create"}, RowActionIDs: []string{"snowflake.table.truncate", "snowflake.table.drop"}, DefaultSort: &plugin.SortKey{Field: "name"}, EmptyText: "No tables in this schema.", Exportable: true}},
				{Key: "views", Label: "Views", Icon: icon("panel-top"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "snowflake.views.list", Params: schemaParams()}, Config: plugin.TableConfig{Columns: viewColumns(), RowActionIDs: []string{"snowflake.view.drop"}, DefaultSort: &plugin.SortKey{Field: "name"}, EmptyText: "No views in this schema."}},
				{Key: "stages", Label: "Stages", Icon: icon("hard-drive-upload"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "snowflake.stages.list", Params: schemaParams()}, Config: plugin.TableConfig{Columns: stageColumns(), DefaultSort: &plugin.SortKey{Field: "name"}, EmptyText: "No stages in this schema."}},
				{Key: "pipes", Label: "Pipes", Icon: icon("git-branch"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "snowflake.pipes.list", Params: schemaParams()}, Config: plugin.TableConfig{Columns: pipeColumns(), DefaultSort: &plugin.SortKey{Field: "name"}, EmptyText: "No pipes in this schema."}},
				{Key: "formats", Label: "File formats", Icon: icon("file-code"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "snowflake.formats.list", Params: schemaParams()}, Config: plugin.TableConfig{Columns: fileFormatColumns(), DefaultSort: &plugin.SortKey{Field: "name"}, EmptyText: "No file formats in this schema."}},
				{Key: "tasks", Label: "Tasks", Icon: icon("calendar-clock"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "snowflake.tasks.list", Params: schemaParams()}, Config: taskTableConfig()},
				{Key: "streams", Label: "Streams", Icon: icon("waves"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "snowflake.streams.list", Params: schemaParams()}, Config: plugin.TableConfig{Columns: streamColumns(), DefaultSort: &plugin.SortKey{Field: "name"}, EmptyText: "No streams in this schema."}},
				{Key: "query", Label: "SQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "snowflake.query", Method: plugin.MethodWS}, Config: queryConfig("SHOW TABLES IN SCHEMA \"${resource.namespace}\".\"${resource.name}\";")},
			},
		},
	}
}

func tableResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "table", Title: "Tables",
		List:    plugin.DataSource{RouteID: "snowflake.tables.list"},
		Columns: tableColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{"snowflake.table.truncate", "snowflake.table.drop"},
			Detail: []string{"snowflake.column.add", "snowflake.table.rename", "snowflake.table.truncate", "snowflake.table.drop"},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.namespace}.${resource.scope}.${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "data", Label: "Data", Icon: icon("table-properties"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "snowflake.table.rows", Params: objectParams()}, Config: dataGridConfig()},
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "snowflake.table.overview", Params: objectParams()}, Config: tableOverviewConfig()},
				{Key: "columns", Label: "Columns", Icon: icon("columns-3"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "snowflake.table.columns", Params: objectParams()}, Config: plugin.TableConfig{Columns: columnColumns(), ActionIDs: []string{"snowflake.column.add"}, RowActionIDs: []string{"snowflake.column.drop"}, EmptyText: "No columns."}},
				{Key: "ddl", Label: "DDL", Icon: icon("code"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: "snowflake.table.ddl", Params: objectParams()}},
				{Key: "query", Label: "SQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "snowflake.query", Method: plugin.MethodWS}, Config: queryConfig("SELECT * FROM \"${resource.namespace}\".\"${resource.scope}\".\"${resource.name}\" LIMIT 100;")},
			},
		},
	}
}

func viewResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "view", Title: "Views",
		List:    plugin.DataSource{RouteID: "snowflake.views.list"},
		Columns: viewColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{"snowflake.view.drop"},
			Detail: []string{"snowflake.view.drop"},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.namespace}.${resource.scope}.${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "data", Label: "Data", Icon: icon("table-properties"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "snowflake.view.rows", Params: objectParams()}, Config: plugin.TableConfig{Exportable: true, EmptyText: "No rows.", ColumnsSource: &plugin.DataSource{RouteID: "snowflake.table.columns", Params: objectParams()}}},
				{Key: "columns", Label: "Columns", Icon: icon("columns-3"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "snowflake.table.columns", Params: objectParams()}, Config: plugin.TableConfig{Columns: columnColumns(), EmptyText: "No columns."}},
				{Key: "ddl", Label: "DDL", Icon: icon("code"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: "snowflake.view.ddl", Params: objectParams()}},
				{Key: "query", Label: "SQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "snowflake.query", Method: plugin.MethodWS}, Config: queryConfig("SELECT * FROM \"${resource.namespace}\".\"${resource.scope}\".\"${resource.name}\" LIMIT 100;")},
			},
		},
	}
}

func warehouseResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "warehouse", Title: "Warehouses",
		List:    plugin.DataSource{RouteID: "snowflake.warehouses.list"},
		Columns: warehouseColumns(),
		Actions: plugin.ResourceActions{
			Detail: []string{"snowflake.warehouse.resume", "snowflake.warehouse.suspend", "snowflake.warehouse.resize"},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}", StatusField: "state", Severities: warehouseSeverities()},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "snowflake.warehouse.overview", Params: map[string]string{"warehouse": "${resource.name}"}}, Config: warehouseOverviewConfig()},
				{Key: "load", Label: "Load", Icon: icon("activity"), Type: plugin.PanelMetrics, Source: &plugin.DataSource{RouteID: "snowflake.warehouse.metrics", Method: plugin.MethodWS, Params: map[string]string{"warehouse": "${resource.name}"}}, Config: warehouseMetricsConfig()},
				{Key: "history", Label: "Queries", Icon: icon("history"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "snowflake.queries.list", Params: map[string]string{"warehouse": "${resource.name}"}}, Config: queryTableConfig()},
			},
		},
	}
}

func roleResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "role", Title: "Roles",
		List:    plugin.DataSource{RouteID: "snowflake.roles.list"},
		Columns: roleColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{"snowflake.role.create"},
			Row:     []string{"snowflake.role.drop"},
			Detail:  []string{"snowflake.role.grant", "snowflake.role.drop"},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "snowflake.role.overview", Params: map[string]string{"role": "${resource.name}"}}, Config: roleOverviewConfig()},
				{Key: "grants", Label: "Grants", Icon: icon("key-round"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "snowflake.role.grants", Params: map[string]string{"role": "${resource.name}"}}, Config: plugin.TableConfig{Columns: grantColumns(), ActionIDs: []string{"snowflake.role.grant"}, RowActionIDs: []string{"snowflake.role.revoke"}, DefaultSort: &plugin.SortKey{Field: "privilege"}, EmptyText: "This role holds no privileges.", Exportable: true}},
			},
		},
	}
}

func userResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "user", Title: "Users",
		List:    plugin.DataSource{RouteID: "snowflake.users.list"},
		Columns: userColumns(),
		Actions: plugin.ResourceActions{
			Detail: []string{"snowflake.user.role.grant"},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "snowflake.user.overview", Params: map[string]string{"user": "${resource.name}"}}, Config: userOverviewConfig()},
				{Key: "roles", Label: "Roles", Icon: icon("shield"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "snowflake.user.grants", Params: map[string]string{"user": "${resource.name}"}}, Config: plugin.TableConfig{Columns: userGrantColumns(), ActionIDs: []string{"snowflake.user.role.grant"}, RowActionIDs: []string{"snowflake.user.role.revoke"}, DefaultSort: &plugin.SortKey{Field: "role"}, EmptyText: "This user holds no roles."}},
				{Key: "history", Label: "Queries", Icon: icon("history"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "snowflake.queries.list", Params: map[string]string{"user": "${resource.name}"}}, Config: queryTableConfig()},
			},
		},
	}
}

func stageResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "stage", Title: "Stages",
		List:    plugin.DataSource{RouteID: "snowflake.stages.list"},
		Columns: stageColumns(),
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.namespace}.${resource.scope}.${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "snowflake.stage.overview", Params: objectParams()}, Config: stageOverviewConfig()},
				{Key: "files", Label: "Files", Icon: icon("files"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "snowflake.stage.files", Params: objectParams()}, Config: plugin.TableConfig{Columns: stageFileColumns(), DefaultSort: &plugin.SortKey{Field: "name"}, EmptyText: "No files staged.", RefreshIntervalMs: 30000, Exportable: true}},
			},
		},
	}
}

func fileFormatResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "file_format", Title: "File formats",
		List:    plugin.DataSource{RouteID: "snowflake.formats.list"},
		Columns: fileFormatColumns(),
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.namespace}.${resource.scope}.${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "snowflake.format.overview", Params: objectParams()}, Config: fileFormatOverviewConfig()},
			},
		},
	}
}

func pipeResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "pipe", Title: "Pipes",
		List:    plugin.DataSource{RouteID: "snowflake.pipes.list"},
		Columns: pipeColumns(),
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.namespace}.${resource.scope}.${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "snowflake.pipe.overview", Params: objectParams()}, Config: pipeOverviewConfig()},
			},
		},
	}
}

func taskResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "task", Title: "Tasks",
		List:    plugin.DataSource{RouteID: "snowflake.tasks.list"},
		Columns: taskColumns(),
		Actions: plugin.ResourceActions{
			Detail: []string{"snowflake.task.resume", "snowflake.task.suspend", "snowflake.task.execute"},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.namespace}.${resource.scope}.${resource.name}", StatusField: "state", Severities: taskSeverities()},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "snowflake.task.overview", Params: objectParams()}, Config: taskOverviewConfig()},
				{Key: "runs", Label: "Runs", Icon: icon("history"), Type: plugin.PanelTimeline, Source: &plugin.DataSource{RouteID: "snowflake.task.history", Params: objectParams()}, Config: taskTimelineConfig()},
			},
		},
	}
}

func streamResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "stream", Title: "Streams",
		List:    plugin.DataSource{RouteID: "snowflake.streams.list"},
		Columns: streamColumns(),
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.namespace}.${resource.scope}.${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "snowflake.stream.overview", Params: objectParams()}, Config: streamOverviewConfig()},
			},
		},
	}
}

func queryResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "query", Title: "Query history",
		List:    plugin.DataSource{RouteID: "snowflake.queries.list"},
		Columns: queryColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{"snowflake.query.abort"},
			Detail: []string{"snowflake.query.abort"},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}", StatusField: "status", Severities: querySeverities()},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "snowflake.query.overview", Params: map[string]string{"id": "${resource.uid}"}}, Config: queryOverviewConfig()},
			},
		},
	}
}

func databaseParams() map[string]string {
	return map[string]string{"database": "${resource.uid}"}
}

func schemaParams() map[string]string {
	return map[string]string{"database": "${resource.namespace}", "schema": "${resource.name}"}
}

// objectParams addresses a schema-scoped object from the row's resource ref, so
// a list panel and a detail tab target the same object without the handler
// guessing.
func objectParams() map[string]string {
	return map[string]string{"database": "${resource.namespace}", "schema": "${resource.scope}", "name": "${resource.name}"}
}

// dataGridConfig addresses rows by the table's declared primary key, which the
// rows handler attaches as _key; tables without one ship no key and stay
// read-only.
func dataGridConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Editable:      true,
		StagedEdits:   true,
		Exportable:    true,
		EmptyText:     "No rows.",
		RowClick:      plugin.RowClickNone,
		Insert:        &plugin.DataSource{RouteID: "snowflake.table.row.insert", Method: plugin.MethodPost, Params: objectParams()},
		Update:        &plugin.DataSource{RouteID: "snowflake.table.row.update", Method: plugin.MethodPatch, Params: objectParams()},
		Delete:        &plugin.DataSource{RouteID: "snowflake.table.row.delete", Method: plugin.MethodDelete, Params: objectParams()},
		ColumnsSource: &plugin.DataSource{RouteID: "snowflake.table.columns", Params: objectParams()},
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
		CancelRouteID:     "snowflake.query.cancel",
		CompletionRouteID: "snowflake.completion",
		Exportable:        true,
	}
}

func warehouseTableConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Columns:           warehouseColumns(),
		RowActionIDs:      []string{"snowflake.warehouse.resume", "snowflake.warehouse.suspend"},
		DefaultSort:       &plugin.SortKey{Field: "name"},
		EmptyText:         "No warehouses are visible to this role.",
		RefreshIntervalMs: 15000,
		Exportable:        true,
	}
}

func taskTableConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Columns:           taskColumns(),
		RowActionIDs:      []string{"snowflake.task.resume", "snowflake.task.suspend"},
		DefaultSort:       &plugin.SortKey{Field: "name"},
		EmptyText:         "No tasks defined.",
		RefreshIntervalMs: 30000,
	}
}

func queryTableConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Columns:           queryColumns(),
		RowActionIDs:      []string{"snowflake.query.abort"},
		DefaultSort:       &plugin.SortKey{Field: "start_time", Desc: true},
		EmptyText:         "No queries in the history window.",
		RefreshIntervalMs: 30000,
		Exportable:        true,
	}
}

func warehouseSeverities() map[string]plugin.Severity {
	return map[string]plugin.Severity{
		"started":    plugin.SeveritySuccess,
		"resuming":   plugin.SeverityInfo,
		"suspending": plugin.SeverityWarn,
		"suspended":  plugin.SeveritySecondary,
	}
}

func taskSeverities() map[string]plugin.Severity {
	return map[string]plugin.Severity{
		"started":   plugin.SeveritySuccess,
		"suspended": plugin.SeveritySecondary,
	}
}

func querySeverities() map[string]plugin.Severity {
	return map[string]plugin.Severity{
		"success":           plugin.SeveritySuccess,
		"running":           plugin.SeverityInfo,
		"queued":            plugin.SeverityWarn,
		"blocked":           plugin.SeverityWarn,
		"fail":              plugin.SeverityDanger,
		"incident":          plugin.SeverityDanger,
		"failed_with_error": plugin.SeverityDanger,
	}
}

func databaseColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Database", Sortable: true},
		{Key: "owner", Label: "Owner", Sortable: true},
		{Key: "is_transient", Label: "Transient", Type: plugin.ColumnBool},
		{Key: "retention_days", Label: "Time travel", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "created", Label: "Created", Type: plugin.ColumnDateTime, Sortable: true},
		{Key: "last_altered", Label: "Altered", Type: plugin.ColumnRelativeTime, Sortable: true},
		{Key: "comment", Label: "Comment"},
	}
}

func schemaColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Schema", Sortable: true},
		{Key: "database", Label: "Database", Sortable: true},
		{Key: "owner", Label: "Owner", Sortable: true},
		{Key: "is_transient", Label: "Transient", Type: plugin.ColumnBool},
		{Key: "managed_access", Label: "Managed access", Type: plugin.ColumnBool},
		{Key: "retention_days", Label: "Time travel", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "created", Label: "Created", Type: plugin.ColumnDateTime, Sortable: true},
		{Key: "comment", Label: "Comment"},
	}
}

func tableColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Table", Sortable: true},
		{Key: "schema", Label: "Schema", Sortable: true},
		{Key: "database", Label: "Database", Sortable: true},
		{Key: "kind", Label: "Kind", Type: plugin.ColumnBadge, Severities: map[string]plugin.Severity{"base table": plugin.SeveritySecondary, "external table": plugin.SeverityInfo, "temporary table": plugin.SeverityWarn}},
		{Key: "rows", Label: "Rows", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "bytes", Label: "Size", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "clustering_key", Label: "Clustering key"},
		{Key: "last_altered", Label: "Altered", Type: plugin.ColumnRelativeTime, Sortable: true},
		{Key: "comment", Label: "Comment"},
	}
}

func viewColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "View", Sortable: true},
		{Key: "schema", Label: "Schema", Sortable: true},
		{Key: "database", Label: "Database", Sortable: true},
		{Key: "kind", Label: "Kind", Type: plugin.ColumnBadge, Severities: map[string]plugin.Severity{"view": plugin.SeveritySecondary, "materialized view": plugin.SeverityInfo}},
		{Key: "last_altered", Label: "Altered", Type: plugin.ColumnRelativeTime, Sortable: true},
		{Key: "comment", Label: "Comment"},
	}
}

func columnColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Column", Sortable: true},
		{Key: "type", Label: "Type"},
		{Key: "nullable", Label: "Nullable", Type: plugin.ColumnBool},
		{Key: "primary_key", Label: "Primary key", Type: plugin.ColumnBool},
		{Key: "default", Label: "Default"},
		{Key: "position", Label: "Position", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "comment", Label: "Comment"},
	}
}

func warehouseColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Warehouse", Sortable: true},
		{Key: "state", Label: "State", Type: plugin.ColumnBadge, Sortable: true, Severities: warehouseSeverities()},
		{Key: "size", Label: "Size", Sortable: true},
		{Key: "kind", Label: "Type"},
		{Key: "running", Label: "Running", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "queued", Label: "Queued", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "clusters", Label: "Clusters", Type: plugin.ColumnNumber},
		{Key: "auto_suspend", Label: "Auto suspend", Type: plugin.ColumnDuration, Sortable: true},
		{Key: "owner", Label: "Owner"},
		{Key: "comment", Label: "Comment"},
	}
}

func roleColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Role", Sortable: true},
		{Key: "owner", Label: "Owner", Sortable: true},
		{Key: "assigned_to_users", Label: "Users", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "granted_to_roles", Label: "Granted to roles", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "granted_roles", Label: "Granted roles", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "created", Label: "Created", Type: plugin.ColumnDateTime, Sortable: true},
		{Key: "comment", Label: "Comment"},
	}
}

func userColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "User", Sortable: true},
		{Key: "login_name", Label: "Login name", Sortable: true},
		{Key: "display_name", Label: "Display name"},
		{Key: "disabled", Label: "Disabled", Type: plugin.ColumnBool, Sortable: true},
		{Key: "default_role", Label: "Default role"},
		{Key: "default_warehouse", Label: "Default warehouse"},
		{Key: "has_rsa_public_key", Label: "Key pair", Type: plugin.ColumnBool},
		{Key: "last_success_login", Label: "Last login", Type: plugin.ColumnRelativeTime, Sortable: true},
		{Key: "owner", Label: "Owner"},
	}
}

func grantColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "privilege", Label: "Privilege", Sortable: true},
		{Key: "granted_on", Label: "On", Sortable: true},
		{Key: "name", Label: "Object", Sortable: true},
		{Key: "grant_option", Label: "Grant option", Type: plugin.ColumnBool},
		{Key: "granted_by", Label: "Granted by"},
		{Key: "created", Label: "Created", Type: plugin.ColumnDateTime, Sortable: true},
	}
}

func userGrantColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "role", Label: "Role", Sortable: true},
		{Key: "granted_by", Label: "Granted by"},
		{Key: "created", Label: "Created", Type: plugin.ColumnDateTime, Sortable: true},
	}
}

func stageColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Stage", Sortable: true},
		{Key: "schema", Label: "Schema", Sortable: true},
		{Key: "database", Label: "Database", Sortable: true},
		{Key: "kind", Label: "Type"},
		{Key: "url", Label: "URL"},
		{Key: "region", Label: "Region"},
		{Key: "owner", Label: "Owner"},
		{Key: "comment", Label: "Comment"},
	}
}

func stageFileColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "File", Sortable: true},
		{Key: "size", Label: "Size", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "md5", Label: "MD5"},
		{Key: "last_modified", Label: "Modified", Type: plugin.ColumnRelativeTime, Sortable: true},
	}
}

func fileFormatColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "File format", Sortable: true},
		{Key: "schema", Label: "Schema", Sortable: true},
		{Key: "database", Label: "Database", Sortable: true},
		{Key: "kind", Label: "Type", Sortable: true},
		{Key: "owner", Label: "Owner"},
		{Key: "created", Label: "Created", Type: plugin.ColumnDateTime, Sortable: true},
		{Key: "comment", Label: "Comment"},
	}
}

func pipeColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Pipe", Sortable: true},
		{Key: "schema", Label: "Schema", Sortable: true},
		{Key: "database", Label: "Database", Sortable: true},
		{Key: "auto_ingest", Label: "Auto ingest", Type: plugin.ColumnBool},
		{Key: "notification_channel", Label: "Notification channel"},
		{Key: "invalid_reason", Label: "Invalid reason"},
		{Key: "owner", Label: "Owner"},
		{Key: "created", Label: "Created", Type: plugin.ColumnDateTime, Sortable: true},
	}
}

func taskColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Task", Sortable: true},
		{Key: "schema", Label: "Schema", Sortable: true},
		{Key: "database", Label: "Database", Sortable: true},
		{Key: "state", Label: "State", Type: plugin.ColumnBadge, Sortable: true, Severities: taskSeverities()},
		{Key: "warehouse", Label: "Warehouse"},
		{Key: "schedule", Label: "Schedule"},
		{Key: "predecessors", Label: "Predecessors"},
		{Key: "comment", Label: "Comment"},
	}
}

func streamColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Stream", Sortable: true},
		{Key: "schema", Label: "Schema", Sortable: true},
		{Key: "database", Label: "Database", Sortable: true},
		{Key: "table_name", Label: "Source object", Sortable: true},
		{Key: "source_type", Label: "Source type"},
		{Key: "mode", Label: "Mode"},
		{Key: "stale", Label: "Stale", Type: plugin.ColumnBool, Sortable: true},
		{Key: "stale_after", Label: "Stale after", Type: plugin.ColumnDateTime},
		{Key: "owner", Label: "Owner"},
	}
}

func queryColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "start_time", Label: "Started", Type: plugin.ColumnRelativeTime, Sortable: true},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Sortable: true, Severities: querySeverities()},
		{Key: "query_type", Label: "Type", Sortable: true},
		{Key: "user", Label: "User", Sortable: true},
		{Key: "warehouse", Label: "Warehouse", Sortable: true},
		{Key: "elapsed_ms", Label: "Elapsed", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "bytes_scanned", Label: "Scanned", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "rows_produced", Label: "Rows", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "credits", Label: "Cloud credits", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "query_text", Label: "SQL"},
	}
}

func accountOverviewConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		Sections: []plugin.ObjectDetailSection{{
			Title: "Session",
			Fields: []plugin.ObjectDetailField{
				{Key: "account", Label: "Account", Copy: true},
				{Key: "region", Label: "Region"},
				{Key: "version", Label: "Snowflake version"},
				{Key: "user", Label: "User", Copy: true},
				{Key: "role", Label: "Role"},
				{Key: "warehouse", Label: "Warehouse"},
				{Key: "database", Label: "Database"},
				{Key: "schema", Label: "Schema"},
				{Key: "session_id", Label: "Session", Copy: true},
			},
		}},
		RawToggle: true,
	}
}

func databaseOverviewConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		Sections: []plugin.ObjectDetailSection{{
			Title: "Database",
			Fields: []plugin.ObjectDetailField{
				{Key: "name", Label: "Name", Copy: true},
				{Key: "owner", Label: "Owner"},
				{Key: "is_transient", Label: "Transient", Type: plugin.ColumnBool},
				{Key: "retention_days", Label: "Time travel (days)", Type: plugin.ColumnNumber},
				{Key: "schemas", Label: "Schemas", Type: plugin.ColumnNumber},
				{Key: "created", Label: "Created", Type: plugin.ColumnDateTime},
				{Key: "last_altered", Label: "Last altered", Type: plugin.ColumnRelativeTime},
				{Key: "comment", Label: "Comment"},
			},
		}},
		RawToggle: true,
	}
}

func schemaOverviewConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		Sections: []plugin.ObjectDetailSection{{
			Title: "Schema",
			Fields: []plugin.ObjectDetailField{
				{Key: "name", Label: "Name", Copy: true},
				{Key: "database", Label: "Database"},
				{Key: "owner", Label: "Owner"},
				{Key: "is_transient", Label: "Transient", Type: plugin.ColumnBool},
				{Key: "managed_access", Label: "Managed access", Type: plugin.ColumnBool},
				{Key: "retention_days", Label: "Time travel (days)", Type: plugin.ColumnNumber},
				{Key: "created", Label: "Created", Type: plugin.ColumnDateTime},
				{Key: "last_altered", Label: "Last altered", Type: plugin.ColumnRelativeTime},
				{Key: "comment", Label: "Comment"},
			},
		}},
		RawToggle: true,
	}
}

func tableOverviewConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		Sections: []plugin.ObjectDetailSection{{
			Title: "Table",
			Fields: []plugin.ObjectDetailField{
				{Key: "name", Label: "Name", Copy: true},
				{Key: "schema", Label: "Schema"},
				{Key: "database", Label: "Database"},
				{Key: "kind", Label: "Kind", Type: plugin.ColumnBadge},
				{Key: "rows", Label: "Rows", Type: plugin.ColumnNumber},
				{Key: "bytes", Label: "Size", Type: plugin.ColumnBytes},
				{Key: "columns", Label: "Columns", Type: plugin.ColumnNumber},
				{Key: "primary_key", Label: "Primary key"},
				{Key: "clustering_key", Label: "Clustering key"},
				{Key: "is_transient", Label: "Transient", Type: plugin.ColumnBool},
				{Key: "retention_days", Label: "Time travel (days)", Type: plugin.ColumnNumber},
				{Key: "created", Label: "Created", Type: plugin.ColumnDateTime},
				{Key: "last_altered", Label: "Last altered", Type: plugin.ColumnRelativeTime},
				{Key: "comment", Label: "Comment"},
			},
		}},
		RawToggle: true,
	}
}

func warehouseOverviewConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		Sections: []plugin.ObjectDetailSection{
			{
				Title: "Warehouse",
				Fields: []plugin.ObjectDetailField{
					{Key: "name", Label: "Name", Copy: true},
					{Key: "state", Label: "State", Type: plugin.ColumnBadge, Severities: warehouseSeverities()},
					{Key: "size", Label: "Size"},
					{Key: "kind", Label: "Type"},
					{Key: "scaling_policy", Label: "Scaling policy"},
					{Key: "owner", Label: "Owner"},
					{Key: "resource_monitor", Label: "Resource monitor"},
					{Key: "comment", Label: "Comment"},
				},
			},
			{
				Title: "Load",
				Fields: []plugin.ObjectDetailField{
					{Key: "cluster_pct", Label: "Clusters started", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
						PercentKey: "cluster_pct", UsedKey: "started_clusters", TotalKey: "max_clusters",
						UsedType: plugin.ColumnNumber, TotalType: plugin.ColumnNumber, TotalLabel: "of", Unit: "cluster(s)", WarnAt: 70, CriticalAt: 90,
					}},
					{Key: "running", Label: "Running queries", Type: plugin.ColumnNumber},
					{Key: "queued", Label: "Queued queries", Type: plugin.ColumnNumber},
					{Key: "auto_suspend", Label: "Auto suspend", Type: plugin.ColumnDuration},
					{Key: "auto_resume", Label: "Auto resume", Type: plugin.ColumnBool},
					{Key: "resumed", Label: "Resumed", Type: plugin.ColumnRelativeTime},
					{Key: "created", Label: "Created", Type: plugin.ColumnDateTime},
				},
			},
		},
		RawToggle: true,
	}
}

func roleOverviewConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		Sections: []plugin.ObjectDetailSection{{
			Title: "Role",
			Fields: []plugin.ObjectDetailField{
				{Key: "name", Label: "Name", Copy: true},
				{Key: "owner", Label: "Owner"},
				{Key: "assigned_to_users", Label: "Assigned to users", Type: plugin.ColumnNumber},
				{Key: "granted_to_roles", Label: "Granted to roles", Type: plugin.ColumnNumber},
				{Key: "granted_roles", Label: "Granted roles", Type: plugin.ColumnNumber},
				{Key: "created", Label: "Created", Type: plugin.ColumnDateTime},
				{Key: "comment", Label: "Comment"},
			},
		}},
		RawToggle: true,
	}
}

func userOverviewConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		Sections: []plugin.ObjectDetailSection{{
			Title: "User",
			Fields: []plugin.ObjectDetailField{
				{Key: "name", Label: "Name", Copy: true},
				{Key: "login_name", Label: "Login name", Copy: true},
				{Key: "display_name", Label: "Display name"},
				{Key: "email", Label: "Email", Redacted: true},
				{Key: "disabled", Label: "Disabled", Type: plugin.ColumnBool},
				{Key: "must_change_password", Label: "Must change password", Type: plugin.ColumnBool},
				{Key: "has_rsa_public_key", Label: "Key pair configured", Type: plugin.ColumnBool},
				{Key: "default_role", Label: "Default role"},
				{Key: "default_warehouse", Label: "Default warehouse"},
				{Key: "default_namespace", Label: "Default namespace"},
				{Key: "last_success_login", Label: "Last login", Type: plugin.ColumnRelativeTime},
				{Key: "created", Label: "Created", Type: plugin.ColumnDateTime},
				{Key: "owner", Label: "Owner"},
				{Key: "comment", Label: "Comment"},
			},
		}},
		RawToggle: true,
	}
}

func stageOverviewConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		Sections: []plugin.ObjectDetailSection{{
			Title: "Stage",
			Fields: []plugin.ObjectDetailField{
				{Key: "name", Label: "Name", Copy: true},
				{Key: "schema", Label: "Schema"},
				{Key: "database", Label: "Database"},
				{Key: "kind", Label: "Type"},
				{Key: "url", Label: "URL", Copy: true},
				{Key: "region", Label: "Region"},
				{Key: "owner", Label: "Owner"},
				{Key: "created", Label: "Created", Type: plugin.ColumnDateTime},
				{Key: "comment", Label: "Comment"},
			},
		}},
		RawToggle: true,
	}
}

func fileFormatOverviewConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		Sections: []plugin.ObjectDetailSection{{
			Title: "File format",
			Fields: []plugin.ObjectDetailField{
				{Key: "name", Label: "Name", Copy: true},
				{Key: "schema", Label: "Schema"},
				{Key: "database", Label: "Database"},
				{Key: "kind", Label: "Type"},
				{Key: "owner", Label: "Owner"},
				{Key: "created", Label: "Created", Type: plugin.ColumnDateTime},
				{Key: "last_altered", Label: "Last altered", Type: plugin.ColumnRelativeTime},
				{Key: "comment", Label: "Comment"},
			},
		}},
		RawToggle: true,
	}
}

func pipeOverviewConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		Sections: []plugin.ObjectDetailSection{{
			Title: "Pipe",
			Fields: []plugin.ObjectDetailField{
				{Key: "name", Label: "Name", Copy: true},
				{Key: "schema", Label: "Schema"},
				{Key: "database", Label: "Database"},
				{Key: "auto_ingest", Label: "Auto ingest", Type: plugin.ColumnBool},
				{Key: "notification_channel", Label: "Notification channel", Copy: true},
				{Key: "invalid_reason", Label: "Invalid reason"},
				{Key: "definition", Label: "COPY statement"},
				{Key: "owner", Label: "Owner"},
				{Key: "created", Label: "Created", Type: plugin.ColumnDateTime},
				{Key: "comment", Label: "Comment"},
			},
		}},
		RawToggle: true,
	}
}

func taskOverviewConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		Sections: []plugin.ObjectDetailSection{{
			Title: "Task",
			Fields: []plugin.ObjectDetailField{
				{Key: "name", Label: "Name", Copy: true},
				{Key: "schema", Label: "Schema"},
				{Key: "database", Label: "Database"},
				{Key: "state", Label: "State", Type: plugin.ColumnBadge, Severities: taskSeverities()},
				{Key: "warehouse", Label: "Warehouse"},
				{Key: "schedule", Label: "Schedule"},
				{Key: "predecessors", Label: "Predecessors"},
				{Key: "condition", Label: "Condition"},
				{Key: "definition", Label: "Definition"},
				{Key: "last_committed", Label: "Last run", Type: plugin.ColumnRelativeTime},
				{Key: "owner", Label: "Owner"},
				{Key: "comment", Label: "Comment"},
			},
		}},
		RawToggle: true,
	}
}

func streamOverviewConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		Sections: []plugin.ObjectDetailSection{{
			Title: "Stream",
			Fields: []plugin.ObjectDetailField{
				{Key: "name", Label: "Name", Copy: true},
				{Key: "schema", Label: "Schema"},
				{Key: "database", Label: "Database"},
				{Key: "table_name", Label: "Source object", Copy: true},
				{Key: "source_type", Label: "Source type"},
				{Key: "mode", Label: "Mode"},
				{Key: "stale", Label: "Stale", Type: plugin.ColumnBool},
				{Key: "stale_after", Label: "Stale after", Type: plugin.ColumnDateTime},
				{Key: "owner", Label: "Owner"},
				{Key: "created", Label: "Created", Type: plugin.ColumnDateTime},
				{Key: "comment", Label: "Comment"},
			},
		}},
		RawToggle: true,
	}
}

func queryOverviewConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		Sections: []plugin.ObjectDetailSection{
			{
				Title: "Query",
				Fields: []plugin.ObjectDetailField{
					{Key: "query_id", Label: "Query ID", Copy: true},
					{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: querySeverities()},
					{Key: "query_type", Label: "Type"},
					{Key: "user", Label: "User"},
					{Key: "role", Label: "Role"},
					{Key: "warehouse", Label: "Warehouse"},
					{Key: "warehouse_size", Label: "Warehouse size"},
					{Key: "database", Label: "Database"},
					{Key: "schema", Label: "Schema"},
					{Key: "query_tag", Label: "Query tag"},
					{Key: "error_code", Label: "Error code"},
					{Key: "error_message", Label: "Error"},
					{Key: "query_text", Label: "SQL", Copy: true},
				},
			},
			{
				Title: "Cost and timing",
				Fields: []plugin.ObjectDetailField{
					{Key: "start_time", Label: "Started", Type: plugin.ColumnDateTime},
					{Key: "end_time", Label: "Ended", Type: plugin.ColumnDateTime},
					{Key: "elapsed_ms", Label: "Total elapsed (ms)", Type: plugin.ColumnNumber},
					{Key: "compilation_ms", Label: "Compilation (ms)", Type: plugin.ColumnNumber},
					{Key: "execution_ms", Label: "Execution (ms)", Type: plugin.ColumnNumber},
					{Key: "queued_ms", Label: "Queued (ms)", Type: plugin.ColumnNumber},
					{Key: "bytes_scanned", Label: "Bytes scanned", Type: plugin.ColumnBytes},
					{Key: "bytes_written", Label: "Bytes written", Type: plugin.ColumnBytes},
					{Key: "rows_produced", Label: "Rows produced", Type: plugin.ColumnNumber},
					{Key: "cache_pct", Label: "Scanned from cache", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{PercentKey: "cache_pct"}},
					{Key: "credits", Label: "Cloud services credits", Type: plugin.ColumnNumber},
				},
			},
		},
		RawToggle: true,
	}
}

func accountMetricsConfig() plugin.MetricsConfig {
	return plugin.MetricsConfig{
		Stats: []plugin.MetricStat{
			{Key: "queries", Label: "Queries / hour"},
			{Key: "failed", Label: "Failed / hour"},
			{Key: "running", Label: "Running now"},
			{Key: "bytesScanned", Label: "Scanned / hour", Unit: "bytes"},
			{Key: "rowsProduced", Label: "Rows / hour"},
			{Key: "warehouses", Label: "Warehouses started"},
		},
		Gauges: []plugin.MetricGauge{
			{Key: "cachePct", Label: "Scanned from cache", Unit: "%"},
		},
		Series: []plugin.MetricSeries{
			{Key: "credits", Label: "Warehouse credits / hour"},
			{Key: "cloudCredits", Label: "Cloud services credits / hour"},
			{Key: "avgElapsedMs", Label: "Average elapsed", Unit: "ms"},
			{Key: "queries", Label: "Queries / hour"},
		},
		History: 90,
	}
}

func warehouseMetricsConfig() plugin.MetricsConfig {
	return plugin.MetricsConfig{
		Stats: []plugin.MetricStat{
			{Key: "running", Label: "Running queries"},
			{Key: "queued", Label: "Queued queries"},
			{Key: "startedClusters", Label: "Started clusters"},
			{Key: "credits", Label: "Credits / hour"},
		},
		Usage: []plugin.MetricUsage{
			{Key: "clusterPct", Label: "Cluster capacity", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
				PercentKey: "clusterPct", UsedKey: "startedClusters", TotalKey: "maxClusters",
				UsedType: plugin.ColumnNumber, TotalType: plugin.ColumnNumber, TotalLabel: "of", Unit: "cluster(s)", WarnAt: 70, CriticalAt: 90,
			}},
		},
		Series: []plugin.MetricSeries{
			{Key: "avgRunning", Label: "Average running"},
			{Key: "avgQueuedLoad", Label: "Average queued (overload)"},
			{Key: "avgQueuedProvisioning", Label: "Average queued (provisioning)"},
			{Key: "avgBlocked", Label: "Average blocked"},
		},
		History: 90,
	}
}

func taskTimelineConfig() plugin.TimelineConfig {
	return plugin.TimelineConfig{
		TimestampField:    "scheduled_time",
		TitleField:        "title",
		BodyField:         "detail",
		SeverityField:     "severity",
		IconField:         "icon",
		ResourceField:     "target",
		EmptyText:         "No task runs in the retention window.",
		RefreshIntervalMs: 30000,
	}
}

func actions() []plugin.Action {
	return []plugin.Action{
		{ID: "snowflake.database.create", Label: "Create database", Icon: icon("plus"), RouteID: "snowflake.database.create"},
		{ID: "snowflake.database.drop", Label: "Drop database", Icon: icon("trash-2"), RouteID: "snowflake.database.drop", Params: map[string]string{"database": "${resource.uid}"}, Confirm: true, ConfirmText: "Drop this database? Every schema, table, view, stage, and task it contains is removed and only recoverable within the time-travel retention window.", OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList}, Bulk: true},
		{ID: "snowflake.schema.create", Label: "Create schema", Icon: icon("plus"), RouteID: "snowflake.schema.create", Params: map[string]string{"database": "${resource.uid}"}, OnSuccess: &plugin.ActionSuccess{SelectTab: "schemas"}},
		{ID: "snowflake.schema.drop", Label: "Drop schema", Icon: icon("trash-2"), RouteID: "snowflake.schema.drop", Params: schemaParams(), Confirm: true, ConfirmText: "Drop this schema? Every table and view it contains is removed and only recoverable within the time-travel retention window.", OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList}, Bulk: true},
		{ID: "snowflake.table.create", Label: "Create table", Icon: icon("plus"), RouteID: "snowflake.table.create", Params: schemaParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "tables"}},
		{ID: "snowflake.table.rename", Label: "Rename table", Icon: icon("pencil"), RouteID: "snowflake.table.rename", Params: objectParams(), OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList}},
		{ID: "snowflake.table.truncate", Label: "Truncate", Icon: icon("eraser"), RouteID: "snowflake.table.truncate", Params: objectParams(), Confirm: true, ConfirmText: "Truncate this table? Every row is deleted and only recoverable within the time-travel retention window.", Bulk: true},
		{ID: "snowflake.table.drop", Label: "Drop table", Icon: icon("trash-2"), RouteID: "snowflake.table.drop", Params: objectParams(), Confirm: true, ConfirmText: "Drop this table? Its definition and data are removed and only recoverable within the time-travel retention window.", Bulk: true},
		{ID: "snowflake.view.drop", Label: "Drop view", Icon: icon("trash-2"), RouteID: "snowflake.view.drop", Params: objectParams(), Confirm: true, ConfirmText: "Drop this view? Anything reading through it starts failing immediately.", Bulk: true},
		{ID: "snowflake.column.add", Label: "Add column", Icon: icon("columns-3"), RouteID: "snowflake.column.add", Params: objectParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "columns"}},
		{ID: "snowflake.column.drop", Label: "Drop column", Icon: icon("trash"), RouteID: "snowflake.column.drop", Params: map[string]string{"database": "${record.database}", "schema": "${record.schema}", "name": "${record.table}", "column": "${record.name}"}, Confirm: true, ConfirmText: "Drop this column? Its data is removed and only recoverable within the time-travel retention window.", OnSuccess: &plugin.ActionSuccess{SelectTab: "columns"}, Bulk: true},
		{ID: "snowflake.warehouse.resume", Label: "Resume", Icon: icon("play"), RouteID: "snowflake.warehouse.resume", Params: map[string]string{"warehouse": "${resource.name}"}, EnabledWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "state", Op: plugin.OpNeq, Value: "STARTED"}}}},
		{ID: "snowflake.warehouse.suspend", Label: "Suspend", Icon: icon("pause"), RouteID: "snowflake.warehouse.suspend", Params: map[string]string{"warehouse": "${resource.name}"}, EnabledWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "state", Op: plugin.OpEq, Value: "STARTED"}}}, Confirm: true, ConfirmText: "Suspend this warehouse? Queries already running finish, but new ones queue until it resumes."},
		{ID: "snowflake.warehouse.resize", Label: "Resize", Icon: icon("scaling"), RouteID: "snowflake.warehouse.resize", Params: map[string]string{"warehouse": "${resource.name}"}, Confirm: true, ConfirmText: "Resize this warehouse? Credit consumption changes immediately with the new size."},
		{ID: "snowflake.task.resume", Label: "Resume task", Icon: icon("play"), RouteID: "snowflake.task.resume", Params: objectParams(), EnabledWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "state", Op: plugin.OpNeq, Value: "STARTED"}}}},
		{ID: "snowflake.task.suspend", Label: "Suspend task", Icon: icon("pause"), RouteID: "snowflake.task.suspend", Params: objectParams(), EnabledWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "state", Op: plugin.OpEq, Value: "STARTED"}}}, Confirm: true, ConfirmText: "Suspend this task? Scheduled runs stop until it is resumed."},
		{ID: "snowflake.task.execute", Label: "Run now", Icon: icon("circle-play"), RouteID: "snowflake.task.execute", Params: objectParams(), Confirm: true, ConfirmText: "Run this task now? It executes outside its schedule and consumes warehouse credits."},
		{ID: "snowflake.role.create", Label: "Create role", Icon: icon("plus"), RouteID: "snowflake.role.create"},
		{ID: "snowflake.role.drop", Label: "Drop role", Icon: icon("trash-2"), RouteID: "snowflake.role.drop", Params: map[string]string{"role": "${resource.name}"}, Confirm: true, ConfirmText: "Drop this role? Every user and role that inherits it loses the access it granted.", OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList}, Bulk: true},
		{ID: "snowflake.role.grant", Label: "Grant privilege", Icon: icon("key-round"), RouteID: "snowflake.role.grant", Params: map[string]string{"role": "${resource.name}"}, Confirm: true, ConfirmText: "Grant this privilege? Every user holding the role gains the access immediately.", OnSuccess: &plugin.ActionSuccess{SelectTab: "grants"}},
		{ID: "snowflake.role.revoke", Label: "Revoke privilege", Icon: icon("key"), RouteID: "snowflake.role.revoke", Params: map[string]string{"role": "${resource.name}", "privilege": "${record.privilege}", "granted_on": "${record.granted_on}", "object": "${record.name}"}, Confirm: true, ConfirmText: "Revoke this privilege? Every user holding the role loses the access immediately.", OnSuccess: &plugin.ActionSuccess{SelectTab: "grants"}, Bulk: true},
		{ID: "snowflake.user.role.grant", Label: "Grant role", Icon: icon("user-plus"), RouteID: "snowflake.user.role.grant", Params: map[string]string{"user": "${resource.name}"}, Confirm: true, ConfirmText: "Grant this role to the user? They gain every privilege the role holds.", OnSuccess: &plugin.ActionSuccess{SelectTab: "roles"}},
		{ID: "snowflake.user.role.revoke", Label: "Revoke role", Icon: icon("user-minus"), RouteID: "snowflake.user.role.revoke", Params: map[string]string{"user": "${resource.name}", "role": "${record.role}"}, Confirm: true, ConfirmText: "Revoke this role from the user? They lose every privilege it holds.", OnSuccess: &plugin.ActionSuccess{SelectTab: "roles"}, Bulk: true},
		{ID: "snowflake.query.abort", Label: "Abort query", Icon: icon("circle-stop"), RouteID: "snowflake.query.abort", Params: map[string]string{"id": "${resource.uid}"}, Confirm: true, ConfirmText: "Abort this query? Its work is discarded and the client receives an error.", Bulk: true},
	}
}
