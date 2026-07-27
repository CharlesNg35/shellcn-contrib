// Package trino implements the Trino distributed SQL query engine plugin.
package trino

import (
	"context"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type Plugin struct{}

func New() *Plugin { return &Plugin{} }

const trinoIconSVG = `<svg id="Layer_1" data-name="Layer 1" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 42.1 62.45"><defs><style>.cls-1{fill:#fff}.cls-2{fill:#dd00a1}.cls-3{fill:#f9d8d2}.cls-4{fill:#10110e}.cls-5{fill:#e5e5e5}.cls-6{fill:#8accce;opacity:.2;isolation:isolate}.cls-7{fill:#515151}</style></defs><path class="cls-1" d="M14.24,55.36c.92-.61-1.49-.38-1.69-.79a15,15,0,0,1-2.82-.51C8,53.62,3.9,50.85,3.47,48.25s0-7.65,1.83-10.61A17,17,0,0,1,9.73,33S6.16,24.51,5.85,18.1,6,.21,10.24,0s3.49,10.48,2.82,17.43a89.47,89.47,0,0,0-.21,13.89,19.45,19.45,0,0,1,5.51-.92,21.2,21.2,0,0,1,4.9.46S24.58,21,29,14.86,38,2.4,41,4.19s-.81,8.19-3.46,11.52-7.25,8.33-8.78,11a47.26,47.26,0,0,0-2.65,5.51,15,15,0,0,1,6.4,6.32c2.07,4.28,2.27,9.59,1.15,11.83a8.1,8.1,0,0,1-5.61,4c-1.32.1-6.91,1.48-6.91,1.48Z" transform="translate(0 0)"/><path class="cls-2" d="M24,30.94s3.33-8.7,6.32-13.6,7-8.59,8.26-7.84S36,14.85,32.66,19.32a112.3,112.3,0,0,0-7.44,12.37Z" transform="translate(0 0)"/><path class="cls-2" d="M10.64,32.44l.95-.75s-.13-8.7,0-14S12,5.78,10.23,5.85,7.23,11,7.92,18.23A86.92,86.92,0,0,0,10.64,32.44Z" transform="translate(0 0)"/><circle class="cls-3" cx="7.65" cy="49.17" r="1.94"/><circle class="cls-3" cx="30.57" cy="49.17" r="1.94"/><path class="cls-4" d="M22.33,49a.19.19,0,0,0-.25,0h0s-.57,1-1.41,1.06a2.11,2.11,0,0,1-1.5-.7V48c.54-.29,1.41-1.19,1.41-1.5a1.4,1.4,0,0,0-1.5-1.09c-1,0-1.7.65-1.7,1.19s1.1,1.23,1.41,1.44v1.45a1.77,1.77,0,0,1-1.3.7c-.8,0-1.4-1.08-1.4-1.09a.19.19,0,0,0-.25-.08h0a.18.18,0,0,0-.08.23h0c0,.06.71,1.29,1.73,1.29a2.07,2.07,0,0,0,1.5-.72,2.46,2.46,0,0,0,1.59.73h.14a2.47,2.47,0,0,0,1.71-1.25.18.18,0,0,0,0-.25A.24.24,0,0,0,22.33,49Z" transform="translate(0 0)"/><path class="cls-5" d="M9.73,33S7,26,6.42,22.29s-1-9.78-.3-13.85A46.3,46.3,0,0,1,7.31,2.85S6,12.34,6.49,16.49,9.73,33,9.73,33Z" transform="translate(0 0)"/><path class="cls-5" d="M23.26,30.85s2-10.59,4.91-14.76S34.73,6.9,36.52,5.62l1.82-1.28S31,12,28.17,18.34,23.26,30.85,23.26,30.85Z" transform="translate(0 0)"/><path class="cls-4" d="M11.6,42.85a1.52,1.52,0,1,0,1.52,1.52h0a1.52,1.52,0,0,0-1.5-1.52Zm.4,1.41a.42.42,0,1,1,.42-.42h0a.42.42,0,0,1-.41.43h0Z" transform="translate(0 0)"/><path class="cls-4" d="M26.55,42.85a1.52,1.52,0,1,0,1.51,1.53h0a1.52,1.52,0,0,0-1.51-1.53ZM27,44.26a.42.42,0,0,1-.43-.41h0a.43.43,0,0,1,.41-.43.42.42,0,0,1,.43.41h0a.41.41,0,0,1-.4.42h0Z" transform="translate(0 0)"/><ellipse class="cls-6" cx="19.36" cy="44.95" rx="14.1" ry="10.16"/><path class="cls-1" d="M36.43,39.3V32.58a2.46,2.46,0,0,0,1.91-2.29,2.65,2.65,0,0,0-5.27,0A2.48,2.48,0,0,0,35,32.58v6.55h-.2c-2-6.94-7.91-11.28-15.63-11.28S5.51,32.15,3.55,39.13H2.88A2.76,2.76,0,0,0,0,41.71v5.14a2.75,2.75,0,0,0,2.88,2.59h.91a13,13,0,0,0,3.59,5.11L6.09,55.72A.78.78,0,0,0,6,56.82l0,0H6c2.93,3.53,7.72,5.6,13.13,5.6s10.21-2,13.14-5.6a.79.79,0,0,0-.09-1.11h0l-1.3-1.19a13,13,0,0,0,3.59-5.08h1a2.74,2.74,0,0,0,2.87-2.6V41.71A2.58,2.58,0,0,0,36.43,39.3Zm-17.26-4c7.14,0,12.94,4.7,12.94,10.47,0,6.25-6.51,9-12.94,9S6.23,52,6.23,45.72C6.23,40,12,35.25,19.17,35.25Z" transform="translate(0 0)"/><path class="cls-7" d="M36.91,45.16H35v-1a18.61,18.61,0,0,0-.39-3.79h.82a1.35,1.35,0,0,1,1.44,1.25h0Z" transform="translate(0 0)"/><path class="cls-7" d="M35.47,48.18h-1A16.13,16.13,0,0,0,34.89,46h2v.85a1.34,1.34,0,0,1-1.36,1.32h-.06Z" transform="translate(0 0)"/><path class="cls-7" d="M19.17,61.1c-4.79,0-9.06-1.72-11.74-4.78l1.3-1.21a17.87,17.87,0,0,0,10.4,3,17.73,17.73,0,0,0,10.4-3l1.35,1.21C28.23,59.38,24,61.1,19.17,61.1Z" transform="translate(0 0)"/><path class="cls-7" d="M2.88,40.42h.85a19.33,19.33,0,0,0-.38,3.79,8.34,8.34,0,0,0,0,1H1.49v-3.5a1.27,1.27,0,0,1,1.24-1.29Z" transform="translate(0 0)"/><path d="M19.17,34.39a17.66,17.66,0,0,0-4.79.69V29.85a17.23,17.23,0,0,1,4.79-.64,17.3,17.3,0,0,1,4.8.64v5.26A15.91,15.91,0,0,0,19.17,34.39Z" transform="translate(0 0)"/><path class="cls-7" d="M1.44,46h2a17.56,17.56,0,0,0,.43,2.16h-1a1.35,1.35,0,0,1-1.44-1.25h0V46Z" transform="translate(0 0)"/><ellipse class="cls-7" cx="35.71" cy="30.29" rx="1.2" ry="1.08"/></svg>`

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:          plugin.CurrentAPIVersion,
		Name:                protocolName,
		Version:             "0.1.0",
		Title:               "Trino",
		Description:         "Trino cockpit with catalogs, schemas, tables and views, bounded data grids, SQL editor with live query stats, cluster nodes, running queries with kill, and session properties.",
		Icon:                plugin.Icon{Type: plugin.IconSVG, Value: trinoIconSVG},
		Category:            plugin.CategoryDatabases,
		Config:              configSchema(),
		Capabilities:        []plugin.Capability{"sql", "schema", "tables", "query_editor", "analytics", "cluster"},
		SupportedTransports: []plugin.Transport{plugin.TransportDirect},
		Layout:              plugin.LayoutSidebarTree,
		Tree:                tree(),
		Resources:           resources(),
		Actions:             actions(),
		Streams: []plugin.Stream{
			{ID: "trino.query", Kind: plugin.StreamQuery, RouteID: "trino.query"},
			{ID: "trino.cluster.metrics", Kind: plugin.StreamMetrics, RouteID: "trino.cluster.metrics"},
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
		{Key: "catalogs", Label: "Catalogs", Icon: icon("library-big"), Source: plugin.DataSource{RouteID: "trino.catalogs.tree"}, Ref: &plugin.ResourceIdentity{Kind: "cluster", Name: "Cluster", UID: "cluster"}},
		{Key: "nodes", Label: "Nodes", Icon: icon("server"), Source: plugin.DataSource{RouteID: "trino.nodes.tree"}, ResourceKind: "node"},
		{Key: "queries", Label: "Queries", Icon: icon("activity"), Source: plugin.DataSource{RouteID: "trino.queries.tree"}, ResourceKind: "query"},
	}
}

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		clusterResource(),
		catalogResource(),
		schemaResource(),
		tableResource(),
		viewResource(),
		nodeResource(),
		queryResource(),
	}
}

func clusterResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "cluster", Title: "Cluster",
		List: plugin.DataSource{RouteID: "trino.catalogs.list"},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "Cluster"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "trino.cluster.overview"}, Config: clusterDetailConfig()},
				{Key: "metrics", Label: "Metrics", Icon: icon("gauge"), Type: plugin.PanelMetrics, Source: &plugin.DataSource{RouteID: "trino.cluster.metrics", Method: plugin.MethodWS}, Config: metricsConfig()},
				{Key: "catalogs", Label: "Catalogs", Icon: icon("library-big"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "trino.catalogs.list"}, Config: plugin.TableConfig{Columns: catalogColumns(), DefaultSort: &plugin.SortKey{Field: "catalog"}}},
				{Key: "nodes", Label: "Nodes", Icon: icon("server"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "trino.nodes.list"}, Config: plugin.TableConfig{Columns: nodeColumns(), DefaultSort: &plugin.SortKey{Field: "node_id"}}},
				{Key: "queries", Label: "Queries", Icon: icon("activity"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "trino.queries.list"}, Config: plugin.TableConfig{Columns: queryColumns(), RowActionIDs: []string{"trino.query.kill"}, RefreshIntervalMs: 5000}},
				{Key: "session", Label: "Session", Icon: icon("sliders-horizontal"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "trino.session.list"}, Config: plugin.TableConfig{Columns: sessionColumns(), DefaultSort: &plugin.SortKey{Field: "name"}, EmptyText: "No session properties."}},
				{Key: "sql", Label: "SQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "trino.query", Method: plugin.MethodWS}, Config: queryConfig("SELECT node_version FROM system.runtime.nodes WHERE coordinator;", nil)},
			},
		},
	}
}

func catalogResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "catalog", Title: "Catalogs",
		List:    plugin.DataSource{RouteID: "trino.catalogs.list"},
		Columns: catalogColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{"trino.schema.create"},
			Detail:  []string{"trino.schema.create"},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "trino.catalog.overview", Params: catalogParams()}, Config: catalogDetailConfig()},
				{Key: "schemas", Label: "Schemas", Icon: icon("folder-tree"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "trino.schemas.list", Params: catalogParams()}, Config: plugin.TableConfig{Columns: schemaColumns(), ActionIDs: []string{"trino.schema.create"}, RowActionIDs: []string{"trino.schema.drop"}}},
				{Key: "sql", Label: "SQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "trino.query", Method: plugin.MethodWS, Params: catalogParams()}, Config: queryConfig("SHOW SCHEMAS;", catalogParams())},
			},
		},
	}
}

func schemaResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "schema", Title: "Schemas",
		List:    plugin.DataSource{RouteID: "trino.schemas.list"},
		Columns: schemaColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{"trino.table.create"},
			Row:     []string{"trino.schema.drop"},
			Detail:  []string{"trino.table.create", "trino.schema.drop"},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.namespace}.${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "tables", Label: "Tables", Icon: icon("table-2"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "trino.tables.list", Params: schemaParams()}, Config: plugin.TableConfig{Columns: relationColumns("Table"), ActionIDs: []string{"trino.table.create"}, RowActionIDs: []string{"trino.table.drop"}}},
				{Key: "views", Label: "Views", Icon: icon("panel-top"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "trino.views.list", Params: schemaParams()}, Config: plugin.TableConfig{Columns: relationColumns("View"), RowActionIDs: []string{"trino.view.drop"}}},
				{Key: "sql", Label: "SQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "trino.query", Method: plugin.MethodWS, Params: schemaParams()}, Config: queryConfig("SHOW TABLES;", schemaParams())},
			},
		},
	}
}

func tableResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "table", Title: "Tables",
		List:    plugin.DataSource{RouteID: "trino.tables.list"},
		Columns: relationColumns("Table"),
		Actions: plugin.ResourceActions{
			Row:    []string{"trino.table.drop"},
			Detail: []string{"trino.table.rename", "trino.table.drop"},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.scope}.${resource.namespace}.${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "data", Label: "Data", Icon: icon("table-properties"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "trino.table.rows", Params: tableParams()}, Config: dataGridConfig()},
				{Key: "columns", Label: "Columns", Icon: icon("columns-3"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "trino.table.columns", Params: tableParams()}, Config: plugin.TableConfig{Columns: columnColumns()}},
				{Key: "stats", Label: "Statistics", Icon: icon("chart-column"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "trino.table.stats", Params: tableParams()}, Config: plugin.TableConfig{Columns: statsColumns(), EmptyText: "This connector reports no table statistics."}},
				{Key: "ddl", Label: "DDL", Icon: icon("code"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: "trino.table.ddl", Params: tableParams()}},
				{Key: "sql", Label: "SQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "trino.query", Method: plugin.MethodWS, Params: tableParams()}, Config: queryConfig(`SELECT * FROM "${resource.scope}"."${resource.namespace}"."${resource.name}" LIMIT 100;`, tableParams())},
			},
		},
	}
}

func viewResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "view", Title: "Views",
		List:    plugin.DataSource{RouteID: "trino.views.list"},
		Columns: relationColumns("View"),
		Actions: plugin.ResourceActions{
			Row:    []string{"trino.view.drop"},
			Detail: []string{"trino.view.drop"},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.scope}.${resource.namespace}.${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "data", Label: "Data", Icon: icon("table-properties"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "trino.view.rows", Params: tableParams()}, Config: plugin.TableConfig{
					Exportable:    true,
					RowClick:      plugin.RowClickNone,
					EmptyText:     "No rows.",
					ColumnsSource: &plugin.DataSource{RouteID: "trino.view.columns", Params: tableParams()},
				}},
				{Key: "columns", Label: "Columns", Icon: icon("columns-3"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "trino.view.columns", Params: tableParams()}, Config: plugin.TableConfig{Columns: columnColumns()}},
				{Key: "definition", Label: "Definition", Icon: icon("code"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: "trino.view.definition", Params: tableParams()}},
				{Key: "sql", Label: "SQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "trino.query", Method: plugin.MethodWS, Params: tableParams()}, Config: queryConfig(`SELECT * FROM "${resource.scope}"."${resource.namespace}"."${resource.name}" LIMIT 100;`, tableParams())},
			},
		},
	}
}

func nodeResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "node", Title: "Nodes",
		List:    plugin.DataSource{RouteID: "trino.nodes.list"},
		Columns: nodeColumns(),
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "trino.node.overview", Params: map[string]string{"node": "${resource.uid}"}}, Config: nodeDetailConfig()},
			},
		},
	}
}

func queryResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "query", Title: "Queries",
		List:    plugin.DataSource{RouteID: "trino.queries.list"},
		Columns: queryColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{"trino.query.kill"},
			Detail: []string{"trino.query.kill"},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "trino.query.overview", Params: map[string]string{"query": "${resource.uid}"}}, Config: queryDetailConfig()},
				{Key: "sql", Label: "Statement", Icon: icon("code"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: "trino.query.statement", Params: map[string]string{"query": "${resource.uid}"}}},
			},
		},
	}
}

func catalogParams() map[string]string {
	return map[string]string{"catalog": "${resource.name}"}
}

func schemaParams() map[string]string {
	return map[string]string{"catalog": "${resource.namespace}", "schema": "${resource.name}"}
}

func tableParams() map[string]string {
	return map[string]string{"catalog": "${resource.scope}", "schema": "${resource.namespace}", "table": "${resource.name}"}
}

// dataGridConfig keeps the data grid append-only: Trino tables carry no row
// identity, so an UPDATE or DELETE built from a displayed row could not be
// targeted safely. Inserts need no key, so they stay available.
func dataGridConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Editable:      true,
		Exportable:    true,
		RowClick:      plugin.RowClickNone,
		EmptyText:     "No rows.",
		Insert:        &plugin.DataSource{RouteID: "trino.table.row.insert", Method: plugin.MethodPost, Params: tableParams()},
		ColumnsSource: &plugin.DataSource{RouteID: "trino.table.columns", Params: tableParams()},
	}
}

func queryConfig(initial string, params map[string]string) plugin.QueryEditorConfig {
	return plugin.QueryEditorConfig{
		Language:          "sql",
		Label:             "SQL",
		ExecuteLabel:      "Run query",
		CancelLabel:       "Cancel query",
		RunningLabel:      "Running...",
		EmptyText:         "Run a query to see results and cluster statistics.",
		InitialQuery:      initial,
		CancelRouteID:     "trino.query.cancel",
		CompletionRouteID: "trino.completion",
		CompletionParams:  params,
		Exportable:        true,
	}
}

func metricsConfig() plugin.MetricsConfig {
	return plugin.MetricsConfig{
		Stats: []plugin.MetricStat{
			{Key: "runningQueries", Label: "Running queries"},
			{Key: "queuedQueries", Label: "Queued queries"},
			{Key: "failedQueries", Label: "Failed queries"},
			{Key: "activeNodes", Label: "Active nodes"},
			{Key: "totalNodes", Label: "Known nodes"},
			{Key: "catalogs", Label: "Catalogs"},
		},
		Series: []plugin.MetricSeries{
			{Key: "runningQueries", Label: "Running"},
			{Key: "queuedQueries", Label: "Queued"},
		},
		History: 60,
	}
}

func clusterDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		Sections: []plugin.ObjectDetailSection{
			{Title: "Coordinator", Fields: []plugin.ObjectDetailField{
				{Key: "coordinator", Label: "Coordinator", Copy: true},
				{Key: "version", Label: "Version", Copy: true},
			}},
			{Title: "Capacity", Fields: []plugin.ObjectDetailField{
				{Key: "total_nodes", Label: "Known nodes", Type: plugin.ColumnNumber},
				{Key: "active_nodes", Label: "Active nodes", Type: plugin.ColumnNumber},
				{Key: "catalogs", Label: "Catalogs", Type: plugin.ColumnNumber},
			}},
			{Title: "Queries", Fields: []plugin.ObjectDetailField{
				{Key: "running_queries", Label: "Running", Type: plugin.ColumnNumber},
				{Key: "queued_queries", Label: "Queued", Type: plugin.ColumnNumber},
				{Key: "failed_queries", Label: "Failed", Type: plugin.ColumnNumber},
			}},
			{Title: "Session defaults", Fields: []plugin.ObjectDetailField{
				{Key: "default_catalog", Label: "Catalog"},
				{Key: "default_schema", Label: "Schema"},
				{Key: "read_only", Label: "Read-only mode", Type: plugin.ColumnBool},
			}},
		},
		RawToggle: true,
	}
}

func catalogDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		Sections: []plugin.ObjectDetailSection{{Title: "Catalog", Fields: []plugin.ObjectDetailField{
			{Key: "catalog", Label: "Catalog", Copy: true},
			{Key: "connector", Label: "Connector"},
			{Key: "schemas", Label: "Schemas", Type: plugin.ColumnNumber},
		}}},
		RawToggle: true,
	}
}

func nodeDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		Sections: []plugin.ObjectDetailSection{{Title: "Node", Fields: []plugin.ObjectDetailField{
			{Key: "node_id", Label: "Node", Copy: true},
			{Key: "http_uri", Label: "HTTP URI", Copy: true},
			{Key: "node_version", Label: "Version"},
			{Key: "coordinator", Label: "Coordinator", Type: plugin.ColumnBool},
			{Key: "state", Label: "State", Type: plugin.ColumnBadge, Severities: nodeStateSeverities()},
		}}},
		RawToggle: true,
	}
}

func queryDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		Sections: []plugin.ObjectDetailSection{
			{Title: "Query", Fields: []plugin.ObjectDetailField{
				{Key: "query_id", Label: "Query ID", Copy: true},
				{Key: "state", Label: "State", Type: plugin.ColumnBadge, Severities: queryStateSeverities()},
				{Key: "user", Label: "User"},
				{Key: "source", Label: "Source"},
				{Key: "resource_group", Label: "Resource group"},
			}},
			{Title: "Timing", Fields: []plugin.ObjectDetailField{
				{Key: "created", Label: "Created", Type: plugin.ColumnDateTime},
				{Key: "started", Label: "Started", Type: plugin.ColumnDateTime},
				{Key: "end", Label: "Ended", Type: plugin.ColumnDateTime},
				{Key: "queued_time_ms", Label: "Queued", Type: plugin.ColumnNumber},
				{Key: "analysis_time_ms", Label: "Analysis", Type: plugin.ColumnNumber},
				{Key: "planning_time_ms", Label: "Planning", Type: plugin.ColumnNumber},
			}},
		},
		RawToggle: true,
	}
}

func catalogColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "catalog", Label: "Catalog", Sortable: true},
		{Key: "connector", Label: "Connector", Sortable: true},
	}
}

func schemaColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "schema", Label: "Schema", Sortable: true},
		{Key: "catalog", Label: "Catalog"},
	}
}

// relationColumns mirrors information_schema.tables, which carries no comment
// column in Trino; a Comment column here would render permanently blank.
func relationColumns(label string) []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: label, Sortable: true},
		{Key: "schema", Label: "Schema", Sortable: true},
		{Key: "catalog", Label: "Catalog"},
		{Key: "type", Label: "Type", Sortable: true},
	}
}

func columnColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Column", Sortable: true},
		{Key: "type", Label: "Type"},
		{Key: "nullable", Label: "Nullable", Type: plugin.ColumnBool},
		{Key: "default", Label: "Default"},
		{Key: "position", Label: "Position", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "comment", Label: "Comment"},
	}
}

func statsColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "column_name", Label: "Column", Sortable: true},
		{Key: "data_size", Label: "Data size", Type: plugin.ColumnBytes},
		{Key: "distinct_values_count", Label: "Distinct", Type: plugin.ColumnNumber},
		{Key: "nulls_fraction", Label: "Null fraction", Type: plugin.ColumnNumber},
		{Key: "row_count", Label: "Rows", Type: plugin.ColumnNumber},
		{Key: "low_value", Label: "Low"},
		{Key: "high_value", Label: "High"},
	}
}

func nodeColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "node_id", Label: "Node", Sortable: true},
		{Key: "http_uri", Label: "HTTP URI", Sortable: true},
		{Key: "node_version", Label: "Version", Sortable: true},
		{Key: "coordinator", Label: "Coordinator", Type: plugin.ColumnBool},
		{Key: "state", Label: "State", Type: plugin.ColumnBadge, Sortable: true, Severities: nodeStateSeverities()},
	}
}

func queryColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "query_id", Label: "Query", Sortable: true},
		{Key: "state", Label: "State", Type: plugin.ColumnBadge, Sortable: true, Severities: queryStateSeverities()},
		{Key: "user", Label: "User", Sortable: true},
		{Key: "source", Label: "Source", Sortable: true},
		{Key: "created", Label: "Created", Type: plugin.ColumnRelativeTime, Sortable: true},
		{Key: "elapsed_ms", Label: "Elapsed", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "query", Label: "Statement"},
	}
}

func sessionColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Property", Sortable: true},
		{Key: "value", Label: "Value"},
		{Key: "default", Label: "Default"},
		{Key: "type", Label: "Type"},
		{Key: "description", Label: "Description"},
	}
}

func nodeStateSeverities() map[string]plugin.Severity {
	return map[string]plugin.Severity{
		"active":        plugin.SeveritySuccess,
		"shutting_down": plugin.SeverityWarn,
		"inactive":      plugin.SeverityDanger,
	}
}

func queryStateSeverities() map[string]plugin.Severity {
	return map[string]plugin.Severity{
		"running":               plugin.SeveritySuccess,
		"finished":              plugin.SeverityInfo,
		"planning":              plugin.SeverityInfo,
		"starting":              plugin.SeverityInfo,
		"queued":                plugin.SeverityWarn,
		"waiting_for_resources": plugin.SeverityWarn,
		"failed":                plugin.SeverityDanger,
	}
}

func actions() []plugin.Action {
	return []plugin.Action{
		{ID: "trino.schema.create", Label: "Create schema", Icon: icon("folder-plus"), RouteID: "trino.schema.create", Params: catalogParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "schemas"}},
		{ID: "trino.schema.drop", Label: "Drop schema", Icon: icon("trash-2"), RouteID: "trino.schema.drop", Params: schemaParams(), Confirm: true, ConfirmText: "Drop this schema? Most connectors refuse while it still holds tables or views.", OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList}, Bulk: true},
		{ID: "trino.table.create", Label: "Create table", Icon: icon("plus"), RouteID: "trino.table.create", Params: schemaParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "tables"}},
		{ID: "trino.table.rename", Label: "Rename", Icon: icon("pencil"), RouteID: "trino.table.rename", Params: tableParams(), OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList}},
		{ID: "trino.table.drop", Label: "Drop", Icon: icon("trash-2"), RouteID: "trino.table.drop", Params: tableParams(), Confirm: true, ConfirmText: "Drop this table? The definition and its data are permanently deleted.", Bulk: true},
		{ID: "trino.view.drop", Label: "Drop", Icon: icon("trash-2"), RouteID: "trino.view.drop", Params: tableParams(), Confirm: true, ConfirmText: "Drop this view?", Bulk: true},
		{ID: "trino.query.kill", Label: "Kill query", Icon: icon("circle-stop"), RouteID: "trino.query.kill", Params: map[string]string{"query": "${resource.uid}"}, Confirm: true, ConfirmText: "Kill this query? It is terminated on the coordinator and its client receives an error.", Bulk: true},
	}
}
