package arangodb

import "github.com/charlesng35/shellcn/sdk/plugin"

var (
	collectionStatusSeverities = map[string]plugin.Severity{
		"loaded":    plugin.SeveritySuccess,
		"loading":   plugin.SeverityWarn,
		"unloading": plugin.SeverityWarn,
		"unloaded":  plugin.SeveritySecondary,
		"deleted":   plugin.SeverityDanger,
		"unknown":   plugin.SeveritySecondary,
	}
	serverStatusSeverities = map[string]plugin.Severity{
		"good":     plugin.SeveritySuccess,
		"degraded": plugin.SeverityWarn,
		"bad":      plugin.SeverityWarn,
		"failed":   plugin.SeverityDanger,
		"unknown":  plugin.SeveritySecondary,
		"serving":  plugin.SeveritySuccess,
	}
	queryStateSeverities = map[string]plugin.Severity{
		"executing": plugin.SeveritySuccess,
		"finished":  plugin.SeveritySecondary,
		"killed":    plugin.SeverityDanger,
		"waiting":   plugin.SeverityWarn,
	}
	collectionTypeSeverities = map[string]plugin.Severity{
		"document": plugin.SeverityInfo,
		"edge":     plugin.SeverityWarn,
	}
	viewTypeSeverities = map[string]plugin.Severity{
		"arangosearch": plugin.SeverityInfo,
		"search-alias": plugin.SeverityWarn,
	}
)

func databaseParams() map[string]string {
	return map[string]string{"database": "${resource.uid}"}
}

func namedParams(key string) map[string]string {
	return map[string]string{"database": "${resource.namespace}", key: "${resource.name}"}
}

func tree() []plugin.TreeGroup {
	clusterRef := plugin.ResourceIdentity{Kind: kindCluster, Name: "overview", UID: "overview"}
	return []plugin.TreeGroup{
		{Key: "databases", Label: "Databases", Icon: icon("database"),
			Source: plugin.DataSource{RouteID: rid("tree.databases")}, ResourceKind: kindDatabase},
		{Key: "cluster", Label: "Cluster & server", Icon: icon("server"), Ref: &clusterRef},
		{Key: "saved", Label: "Saved AQL", Icon: icon("bookmark"), ResourceKind: kindSavedQuery},
	}
}

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		databaseResource(),
		collectionResource(),
		graphResource(),
		viewResource(),
		analyzerResource(),
		foxxResource(),
		clusterResource(),
		aqlQueryResource(),
		savedQueryResource(),
	}
}

func databaseResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: kindDatabase, Title: "Databases",
		List: plugin.DataSource{RouteID: rid("databases.list")},
		Columns: []plugin.Column{
			{Key: "name", Label: "Database", Sortable: true},
			{Key: "system", Label: "System", Type: plugin.ColumnBool, Sortable: true},
			{Key: "sharding", Label: "Sharding", Type: plugin.ColumnBadge, Sortable: true},
			{Key: "replication_factor", Label: "Replication", Type: plugin.ColumnBadge},
			{Key: "write_concern", Label: "Write concern", Type: plugin.ColumnNumber},
			{Key: "replication_version", Label: "Replication version", Type: plugin.ColumnBadge},
		},
		Actions: plugin.ResourceActions{
			Toolbar: []string{rid("database.create")},
			Row:     []string{rid("database.drop")},
			Detail:  []string{rid("queries.slow.clear"), rid("database.drop")},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("database.read"), Params: databaseParams()},
					Config: databaseOverviewConfig()},
				{Key: "metrics", Label: "Metrics", Icon: icon("activity"), Type: plugin.PanelMetrics,
					Source: &plugin.DataSource{RouteID: rid("database.metrics"), Method: plugin.MethodWS, Params: databaseParams()},
					Config: databaseMetricsConfig()},
				{Key: "collections", Label: "Collections", Icon: icon("table"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("collections.list"), Params: databaseParams()},
					Config: plugin.TableConfig{Columns: collectionColumns(), ActionIDs: []string{rid("collection.create")},
						DefaultSort: &plugin.SortKey{Field: "name"}, RefreshIntervalMs: 30000, Exportable: true,
						EmptyText: "No collections in this database yet."}},
				{Key: "graphs", Label: "Graphs", Icon: icon("workflow"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("graphs.list"), Params: databaseParams()},
					Config: plugin.TableConfig{Columns: graphColumns(), ActionIDs: []string{rid("graph.create")},
						DefaultSort: &plugin.SortKey{Field: "name"}, RefreshIntervalMs: 60000, Exportable: true,
						EmptyText: "No named graphs defined in this database."}},
				{Key: "views", Label: "Search views", Icon: icon("search"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("views.list"), Params: databaseParams()},
					Config: plugin.TableConfig{Columns: viewColumns(), ActionIDs: []string{rid("view.create")},
						DefaultSort: &plugin.SortKey{Field: "name"}, RefreshIntervalMs: 60000, Exportable: true,
						EmptyText: "No ArangoSearch views defined in this database."}},
				{Key: "analyzers", Label: "Analyzers", Icon: icon("scan-text"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("analyzers.list"), Params: databaseParams()},
					Config: plugin.TableConfig{Columns: analyzerColumns(), ActionIDs: []string{rid("analyzer.create")},
						DefaultSort: &plugin.SortKey{Field: "name"}, RefreshIntervalMs: 60000, Exportable: true,
						EmptyText: "No analyzers are visible from this database."}},
				{Key: "foxx", Label: "Foxx", Icon: icon("boxes"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("foxx.list"), Params: databaseParams()},
					Config: plugin.TableConfig{Columns: foxxColumns(), DefaultSort: &plugin.SortKey{Field: "mount"},
						RefreshIntervalMs: 60000, Exportable: true,
						EmptyText: "No Foxx microservices are mounted in this database."}},
				{Key: "schema", Label: "Schema map", Icon: icon("share-2"), Type: plugin.PanelGraph,
					Source: &plugin.DataSource{RouteID: rid("database.schema"), Params: databaseParams()},
					Config: plugin.GraphConfig{Layout: plugin.GraphLayoutGrid, FitView: true}},
				{Key: "aql", Label: "AQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor,
					Source: &plugin.DataSource{RouteID: rid("query"), Method: plugin.MethodWS, Params: databaseParams()},
					Config: queryEditorConfig(databaseInitialAQL, databaseParams())},
				{Key: "slow", Label: "Slow AQL", Icon: icon("history"), Type: plugin.PanelTimeline,
					Source: &plugin.DataSource{RouteID: rid("queries.slow"), Params: databaseParams()},
					Config: plugin.TimelineConfig{TimestampField: "started", TitleField: "state", BodyField: "query",
						SeverityField: "severity", ResourceField: "database", RefreshIntervalMs: 30000,
						EmptyText: "No slow AQL recorded for this database."}},
				{Key: "saved", Label: "Saved AQL", Icon: icon("bookmark"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("saved.list")},
					Config: plugin.TableConfig{Columns: savedColumns(), ActionIDs: []string{rid("saved.create")},
						DefaultSort: &plugin.SortKey{Field: "name"}, Exportable: true,
						EmptyText: "No saved AQL yet. Save a query to reuse it across connections."}},
			},
		},
	}
}

func collectionResource() plugin.ResourceType {
	params := namedParams("collection")
	return plugin.ResourceType{
		Kind: kindCollection, Title: "Collections",
		List:    plugin.DataSource{RouteID: rid("collections.list")},
		Columns: collectionColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{rid("collection.drop")},
			Detail: []string{rid("collection.truncate"), rid("collection.drop")},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}", StatusField: "status", Severities: collectionStatusSeverities},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("collection.read"), Params: params},
					Config: collectionOverviewConfig()},
				{Key: "documents", Label: "Documents", Icon: icon("file-json"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("documents.list"), Params: params},
					Config: documentsTableConfig(params)},
				{Key: "indexes", Label: "Indexes", Icon: icon("list-tree"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("indexes.list"), Params: params},
					Config: plugin.TableConfig{Columns: indexColumns(), ActionIDs: []string{rid("index.create")},
						RowActionIDs: []string{rid("index.delete")}, DefaultSort: &plugin.SortKey{Field: "name"},
						RefreshIntervalMs: 60000, Exportable: true, EmptyText: "This collection has no secondary indexes."}},
				{Key: "metrics", Label: "Metrics", Icon: icon("activity"), Type: plugin.PanelMetrics,
					Source: &plugin.DataSource{RouteID: rid("collection.metrics"), Method: plugin.MethodWS, Params: params},
					Config: collectionMetricsConfig()},
				{Key: "shards", Label: "Shards", Icon: icon("layers"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("collection.shards"), Params: params},
					Config: plugin.TableConfig{Columns: shardColumns(), DefaultSort: &plugin.SortKey{Field: "shard"},
						RefreshIntervalMs: 30000, EmptyText: "No shard placement reported for this collection."}},
				{Key: "schema", Label: "Validation", Icon: icon("shield-check"), Type: plugin.PanelCodeEditor,
					Source: &plugin.DataSource{RouteID: rid("collection.schema.read"), Params: params},
					Config: plugin.CodeEditorConfig{Language: "json", SaveRouteID: rid("collection.schema.update"),
						SaveMethod: plugin.MethodPut, SaveParams: params, SaveBodyKey: "schema", RefreshField: "content"}},
				{Key: "aql", Label: "AQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor,
					Source: &plugin.DataSource{RouteID: rid("query"), Method: plugin.MethodWS, Params: map[string]string{"database": "${resource.namespace}"}},
					Config: queryEditorConfig(collectionInitialAQL, map[string]string{"database": "${resource.namespace}"})},
			},
		},
	}
}

func graphResource() plugin.ResourceType {
	params := namedParams("graph")
	return plugin.ResourceType{
		Kind: kindGraph, Title: "Graphs",
		List:    plugin.DataSource{RouteID: rid("graphs.list")},
		Columns: graphColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{rid("graph.drop")},
			Detail: []string{rid("graph.drop")},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("graph.read"), Params: params},
					Config: graphOverviewConfig()},
				{Key: "explorer", Label: "Explorer", Icon: icon("git-fork"), Type: plugin.PanelGraph,
					Source: &plugin.DataSource{RouteID: rid("graph.explore"), Params: params},
					Config: plugin.GraphConfig{FitView: true, ExpandRouteID: rid("graph.expand"), ExpandParam: "node"}},
				{Key: "topology", Label: "Topology", Icon: icon("scan"), Type: plugin.PanelCanvas,
					Source: &plugin.DataSource{RouteID: rid("graph.topology"), Method: plugin.MethodWS, Params: params},
					Config: topologyCanvasConfig()},
				{Key: "edges", Label: "Edge definitions", Icon: icon("git-branch"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("graph.edges"), Params: params},
					Config: plugin.TableConfig{Columns: edgeDefinitionColumns(), DefaultSort: &plugin.SortKey{Field: "collection"},
						RefreshIntervalMs: 60000, Exportable: true, EmptyText: "This graph has no edge definitions."}},
				{Key: "vertices", Label: "Vertex collections", Icon: icon("circle-dot"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("graph.vertices"), Params: params},
					Config: plugin.TableConfig{Columns: vertexCollectionColumns(), DefaultSort: &plugin.SortKey{Field: "name"},
						RefreshIntervalMs: 60000, Exportable: true, EmptyText: "This graph has no vertex collections."}},
				{Key: "aql", Label: "AQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor,
					Source: &plugin.DataSource{RouteID: rid("query"), Method: plugin.MethodWS, Params: map[string]string{"database": "${resource.namespace}"}},
					Config: queryEditorConfig(graphInitialAQL, map[string]string{"database": "${resource.namespace}"})},
			},
		},
	}
}

func viewResource() plugin.ResourceType {
	params := namedParams("view")
	return plugin.ResourceType{
		Kind: kindView, Title: "Search views",
		List:    plugin.DataSource{RouteID: rid("views.list")},
		Columns: viewColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{rid("view.drop")},
			Detail: []string{rid("view.drop")},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}", StatusField: "type", Severities: viewTypeSeverities},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("view.read"), Params: params},
					Config: viewOverviewConfig()},
				{Key: "links", Label: "Links", Icon: icon("link"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("view.links"), Params: params},
					Config: plugin.TableConfig{Columns: viewLinkColumns(), DefaultSort: &plugin.SortKey{Field: "collection"},
						RefreshIntervalMs: 60000, Exportable: true, EmptyText: "This view indexes no collections yet."}},
				{Key: "definition", Label: "Definition", Icon: icon("braces"), Type: plugin.PanelCodeEditor,
					Source: &plugin.DataSource{RouteID: rid("view.definition"), Params: params},
					Config: plugin.CodeEditorConfig{Language: "json", SaveRouteID: rid("view.update"),
						SaveMethod: plugin.MethodPut, SaveParams: params, SaveBodyKey: "properties", RefreshField: "content"}},
			},
		},
	}
}

func analyzerResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: kindAnalyzer, Title: "Analyzers",
		List:    plugin.DataSource{RouteID: rid("analyzers.list")},
		Columns: analyzerColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{rid("analyzer.delete")},
			Detail: []string{rid("analyzer.delete")},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("analyzer.read"), Params: namedParams("analyzer")},
					Config: plugin.ObjectDetailConfig{RawToggle: true, Sections: []plugin.ObjectDetailSection{{
						Title: "Analyzer",
						Fields: []plugin.ObjectDetailField{
							{Key: "name", Label: "Name", Copy: true},
							{Key: "unique_name", Label: "Qualified name", Copy: true},
							{Key: "database", Label: "Database"},
							{Key: "type", Label: "Type", Type: plugin.ColumnBadge},
							{Key: "features", Label: "Features"},
							{Key: "properties", Label: "Properties", Type: plugin.ColumnJSON},
						},
					}}}},
			},
		},
	}
}

func foxxResource() plugin.ResourceType {
	params := map[string]string{"service": "${resource.uid}"}
	return plugin.ResourceType{
		Kind: kindFoxx, Title: "Foxx services",
		List:    plugin.DataSource{RouteID: rid("foxx.list")},
		Columns: foxxColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{rid("foxx.uninstall")},
			Detail: []string{rid("foxx.development"), rid("foxx.uninstall")},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("foxx.read"), Params: params},
					Config: foxxOverviewConfig()},
				{Key: "routes", Label: "Routes", Icon: icon("route"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("foxx.routes"), Params: params},
					Config: plugin.TableConfig{Columns: foxxRouteColumns(), DefaultSort: &plugin.SortKey{Field: "path"},
						Exportable: true, EmptyText: "This service publishes no documented routes."}},
				{Key: "configuration", Label: "Configuration", Icon: icon("sliders-horizontal"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("foxx.configuration"), Params: params},
					Config: plugin.TableConfig{Columns: foxxSettingColumns(), DefaultSort: &plugin.SortKey{Field: "name"},
						EmptyText: "This service declares no configuration."}},
				{Key: "dependencies", Label: "Dependencies", Icon: icon("package"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("foxx.dependencies"), Params: params},
					Config: plugin.TableConfig{Columns: foxxSettingColumns(), DefaultSort: &plugin.SortKey{Field: "name"},
						EmptyText: "This service declares no dependencies."}},
				{Key: "readme", Label: "Readme", Icon: icon("book-open"), Type: plugin.PanelCodeEditor,
					Source: &plugin.DataSource{RouteID: rid("foxx.readme"), Params: params},
					Config: plugin.CodeEditorConfig{Language: "markdown"}},
			},
		},
	}
}

func clusterResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: kindCluster, Title: "Cluster & server",
		List: plugin.DataSource{RouteID: rid("cluster.list")},
		Columns: []plugin.Column{
			{Key: "name", Label: "Deployment"},
			{Key: "deployment", Label: "Topology", Type: plugin.ColumnBadge},
			{Key: "role", Label: "Role", Type: plugin.ColumnBadge},
			{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: serverStatusSeverities},
			{Key: "version", Label: "Version"},
			{Key: "servers_total", Label: "Servers", Type: plugin.ColumnNumber},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "ArangoDB deployment", StatusField: "status", Severities: serverStatusSeverities},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("cluster.read")},
					Config: clusterOverviewConfig()},
				{Key: "servers", Label: "Servers", Icon: icon("server"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("cluster.servers")},
					Config: plugin.TableConfig{Columns: serverColumns(), DefaultSort: &plugin.SortKey{Field: "short_name"},
						RefreshIntervalMs: 10000, Exportable: true,
						EmptyText: "No servers reported by the health endpoint."}},
				{Key: "metrics", Label: "Metrics", Icon: icon("activity"), Type: plugin.PanelMetrics,
					Source: &plugin.DataSource{RouteID: rid("cluster.metrics"), Method: plugin.MethodWS},
					Config: clusterMetricsConfig()},
			},
		},
	}
}

func aqlQueryResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: kindAQLQuery, Title: "AQL activity",
		List:    plugin.DataSource{RouteID: rid("queries.running")},
		Columns: aqlQueryColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{rid("query.kill")},
			Detail: []string{rid("query.kill")},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}", StatusField: "state", Severities: queryStateSeverities},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Query", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("query.read"), Params: map[string]string{"id": "${resource.uid}"}},
					Config: aqlQueryOverviewConfig()},
			},
		},
	}
}

func savedQueryResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: kindSavedQuery, Title: "Saved AQL",
		List:    plugin.DataSource{RouteID: rid("saved.list")},
		Columns: savedColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{rid("saved.create")},
			Row:     []string{rid("saved.delete")},
			Detail:  []string{rid("saved.delete")},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Query", Icon: icon("bookmark"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("saved.read"), Params: map[string]string{"id": "${resource.uid}"}},
					Config: plugin.ObjectDetailConfig{Sections: []plugin.ObjectDetailSection{{
						Title: "Saved AQL",
						Fields: []plugin.ObjectDetailField{
							{Key: "name", Label: "Name", Copy: true},
							{Key: "database", Label: "Database"},
							{Key: "writes", Label: "Modifies data", Type: plugin.ColumnBool},
							{Key: "updated_at", Label: "Updated", Type: plugin.ColumnRelativeTime},
							{Key: "query", Label: "AQL", Copy: true},
							{Key: "notes", Label: "Notes"},
						},
					}}}},
			},
		},
	}
}

func actions() []plugin.Action {
	return []plugin.Action{
		{ID: rid("database.create"), Label: "Create database", Icon: icon("plus"), RouteID: rid("database.create"),
			OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList}},
		{ID: rid("database.drop"), Label: "Drop database", Icon: icon("trash-2"), RouteID: rid("database.drop"),
			Params: databaseParams(), Confirm: true, Bulk: true,
			ConfirmText: "Drop this database with every collection, graph, view and Foxx service inside it. This cannot be undone.",
			VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "system", Op: plugin.OpNeq, Value: true}}},
			OnSuccess:   &plugin.ActionSuccess{Navigate: plugin.NavigateList}},

		{ID: rid("collection.create"), Label: "Create collection", Icon: icon("plus"), RouteID: rid("collection.create"),
			Params: databaseParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "collections"}},
		{ID: rid("collection.truncate"), Label: "Truncate", Icon: icon("eraser"), RouteID: rid("collection.truncate"),
			Params: namedParams("collection"), Confirm: true, Group: "Danger zone",
			ConfirmText: "Delete every document in this collection. Indexes and the collection itself are kept.",
			OnSuccess:   &plugin.ActionSuccess{SelectTab: "documents"}},
		{ID: rid("collection.drop"), Label: "Drop collection", Icon: icon("trash-2"), RouteID: rid("collection.drop"),
			Params: namedParams("collection"), Confirm: true, Bulk: true, Group: "Danger zone",
			ConfirmText: "Drop this collection and every document and index it holds. This cannot be undone.",
			VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "system", Op: plugin.OpNeq, Value: true}}},
			OnSuccess:   &plugin.ActionSuccess{Navigate: plugin.NavigateList}},

		{ID: rid("index.create"), Label: "Create index", Icon: icon("plus"), RouteID: rid("index.create"),
			Params: namedParams("collection"), OnSuccess: &plugin.ActionSuccess{SelectTab: "indexes"}},
		{ID: rid("index.delete"), Label: "Drop index", Icon: icon("trash"), RouteID: rid("index.delete"),
			Params:  map[string]string{"database": "${resource.namespace}", "collection": "${resource.name}", "index": "${record.name}"},
			Confirm: true, Bulk: true,
			ConfirmText: "Drop this index. Queries relying on it will fall back to full collection scans.",
			EnabledWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "system", Op: plugin.OpNeq, Value: true}}}},

		{ID: rid("document.edit"), Label: "Edit JSON", Icon: icon("file-json"), RouteID: rid("document.read"),
			Params: map[string]string{"database": "${resource.namespace}", "collection": "${resource.name}", "key": "${record._key}"},
			Open:   plugin.OpenDialog, Panel: plugin.PanelCodeEditor,
			Config: plugin.CodeEditorConfig{
				Language:    "json",
				SaveRouteID: rid("document.replace"), SaveMethod: plugin.MethodPut,
				SaveParams:  map[string]string{"database": "${resource.namespace}", "collection": "${resource.name}", "key": "${record._key}"},
				SaveBodyKey: "document", RefreshField: "content", SaveDismiss: plugin.SaveDismissClose,
				SaveToast: &plugin.SaveToast{Summary: "Document replaced", Severity: plugin.SeveritySuccess},
			},
			OnSuccess: &plugin.ActionSuccess{SelectTab: "documents"}},

		{ID: rid("graph.create"), Label: "Create graph", Icon: icon("plus"), RouteID: rid("graph.create"),
			Params: databaseParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "graphs"}},
		{ID: rid("graph.drop"), Label: "Drop graph", Icon: icon("trash-2"), RouteID: rid("graph.drop"),
			Params: namedParams("graph"), Confirm: true, Bulk: true,
			ConfirmText: "Drop this named graph definition. Vertex and edge collections are kept.",
			OnSuccess:   &plugin.ActionSuccess{Navigate: plugin.NavigateList}},

		{ID: rid("view.create"), Label: "Create view", Icon: icon("plus"), RouteID: rid("view.create"),
			Params: databaseParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "views"}},
		{ID: rid("view.drop"), Label: "Drop view", Icon: icon("trash-2"), RouteID: rid("view.drop"),
			Params: namedParams("view"), Confirm: true, Bulk: true,
			ConfirmText: "Drop this search view. Its index is deleted and SEARCH queries against it will fail.",
			OnSuccess:   &plugin.ActionSuccess{Navigate: plugin.NavigateList}},

		{ID: rid("analyzer.create"), Label: "Create analyzer", Icon: icon("plus"), RouteID: rid("analyzer.create"),
			Params: databaseParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "analyzers"}},
		{ID: rid("analyzer.delete"), Label: "Delete analyzer", Icon: icon("trash-2"), RouteID: rid("analyzer.delete"),
			Params: namedParams("analyzer"), Confirm: true, Bulk: true,
			ConfirmText: "Delete this analyzer. Views that reference it stop indexing until they are updated.",
			OnSuccess:   &plugin.ActionSuccess{Navigate: plugin.NavigateList}},

		{ID: rid("foxx.development"), Label: "Development mode", Icon: icon("hammer"), RouteID: rid("foxx.development"),
			Params:    map[string]string{"service": "${resource.uid}"},
			OnSuccess: &plugin.ActionSuccess{SelectTab: "overview"}},
		{ID: rid("foxx.uninstall"), Label: "Uninstall service", Icon: icon("trash-2"), RouteID: rid("foxx.uninstall"),
			Params: map[string]string{"service": "${resource.uid}"}, Confirm: true, Bulk: true,
			ConfirmText: "Uninstall this Foxx service and run its teardown script. Collections it created may be removed.",
			OnSuccess:   &plugin.ActionSuccess{Navigate: plugin.NavigateList}},

		{ID: rid("query.kill"), Label: "Kill query", Icon: icon("octagon-x"), RouteID: rid("query.kill"),
			Params: map[string]string{"id": "${resource.uid}"}, Confirm: true, Bulk: true,
			ConfirmText: "Kill this running AQL query. Partial writes from a modifying query are rolled back."},
		{ID: rid("queries.slow.clear"), Label: "Clear slow AQL log", Icon: icon("eraser"), RouteID: rid("queries.slow.clear"),
			Params: databaseParams(), Confirm: true, Group: "Danger zone",
			ConfirmText: "Clear the recorded slow AQL queries for this database.",
			OnSuccess:   &plugin.ActionSuccess{SelectTab: "slow"}},

		{ID: rid("saved.create"), Label: "Save AQL", Icon: icon("bookmark-plus"), RouteID: rid("saved.create"),
			OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList}},
		{ID: rid("saved.delete"), Label: "Delete saved AQL", Icon: icon("trash-2"), RouteID: rid("saved.delete"),
			Params: map[string]string{"id": "${resource.uid}"}, Confirm: true, Bulk: true,
			ConfirmText: "Delete this saved query.",
			OnSuccess:   &plugin.ActionSuccess{Navigate: plugin.NavigateList}},
	}
}

// The three AQL editors open pre-filled. Every default runs on any freshly
// created database without bind parameters, indexes or system collections that
// only exist in _system, and returns rows that describe what is actually there.
const (
	databaseInitialAQL = "FOR c IN COLLECTIONS()\n" +
		"  FILTER !STARTS_WITH(c.name, \"_\")\n" +
		"  SORT c.name\n" +
		"  LIMIT 25\n" +
		"  RETURN { collection: c.name, documents: COLLECTION_COUNT(c.name) }"

	collectionInitialAQL = "FOR doc IN `${resource.name}`\n" +
		"  LIMIT 25\n" +
		"  RETURN doc"

	graphInitialAQL = "/* Traverse the graph by copying a vertex _id into the start position:\n" +
		"   FOR v, e, p IN 1..2 ANY \"collection/key\" GRAPH `${resource.name}` LIMIT 25 RETURN p */\n" +
		"FOR g IN _graphs\n" +
		"  FILTER g._key == \"${resource.name}\"\n" +
		"  FOR def IN g.edgeDefinitions\n" +
		"    RETURN { edge: def.collection, from: def.from, to: def.to, edges: COLLECTION_COUNT(def.collection) }"
)

func queryEditorConfig(initial string, params map[string]string) plugin.QueryEditorConfig {
	return plugin.QueryEditorConfig{
		Language:          "aql",
		Label:             "AQL",
		ExecuteLabel:      "Run AQL",
		RunningLabel:      "Running…",
		CancelLabel:       "Cancel",
		EmptyText:         "Run AQL to see results. Prefix a query with EXPLAIN to inspect its execution plan instead.",
		InitialQuery:      initial,
		CancelRouteID:     rid("query.cancel"),
		CancelParams:      params,
		CompletionRouteID: rid("query.complete"),
		CompletionParams:  params,
		Exportable:        true,
	}
}

func topologyCanvasConfig() plugin.CanvasConfig {
	return plugin.CanvasConfig{
		ScaleMode:      plugin.CanvasScaleResize,
		Interactive:    true,
		Pointer:        true,
		Keyboard:       true,
		ResizeEvents:   true,
		WheelMode:      plugin.CanvasWheelNone,
		HiDPI:          true,
		FocusOnPointer: true,
		AriaLabel:      "Named graph topology",
		Instructions:   "Arrow keys move between vertex collections; Enter announces the selected collection's edge definitions.",
	}
}

func documentsTableConfig(params map[string]string) plugin.TableConfig {
	return plugin.TableConfig{
		Editable:      true,
		StagedEdits:   true,
		Exportable:    true,
		RowKey:        []string{"_key"},
		RowClick:      plugin.RowClickNone,
		ColumnsSource: &plugin.DataSource{RouteID: rid("documents.columns"), Params: params},
		Insert:        &plugin.DataSource{RouteID: rid("document.insert"), Method: plugin.MethodPost, Params: params},
		Update:        &plugin.DataSource{RouteID: rid("document.update"), Method: plugin.MethodPatch, Params: params},
		Delete:        &plugin.DataSource{RouteID: rid("document.delete"), Method: plugin.MethodDelete, Params: params},
		RowActionIDs:  []string{rid("document.edit")},
		EmptyText:     "This collection has no documents yet.",
	}
}

func databaseOverviewConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Sections: []plugin.ObjectDetailSection{
			{Title: "Database", Fields: []plugin.ObjectDetailField{
				{Key: "name", Label: "Name", Copy: true},
				{Key: "id", Label: "ID", Copy: true},
				{Key: "system", Label: "System database", Type: plugin.ColumnBool},
				{Key: "endpoint", Label: "Endpoint", Copy: true},
				{Key: "server_version", Label: "Server version"},
				{Key: "license", Label: "License", Type: plugin.ColumnBadge},
				{Key: "read_only", Label: "Read-only connection", Type: plugin.ColumnBool},
			}},
			{Title: "Placement", Fields: []plugin.ObjectDetailField{
				{Key: "sharding", Label: "Sharding", Type: plugin.ColumnBadge},
				{Key: "replication_factor", Label: "Replication factor", Type: plugin.ColumnBadge},
				{Key: "write_concern", Label: "Write concern", Type: plugin.ColumnNumber},
				{Key: "replication_version", Label: "Replication version", Type: plugin.ColumnBadge},
				{Key: "path", Label: "Path"},
			}},
			{Title: "Contents", Fields: []plugin.ObjectDetailField{
				{Key: "collections", Label: "Collections", Type: plugin.ColumnNumber},
				{Key: "documents", Label: "Documents", Type: plugin.ColumnNumber},
				{Key: "edges", Label: "Edge documents", Type: plugin.ColumnNumber},
				{Key: "graphs", Label: "Named graphs", Type: plugin.ColumnNumber},
				{Key: "views", Label: "Search views", Type: plugin.ColumnNumber},
				{Key: "analyzers", Label: "Analyzers", Type: plugin.ColumnNumber},
				{Key: "runningQueries", Label: "Running AQL", Type: plugin.ColumnNumber},
				{Key: "slowQueries", Label: "Slow AQL", Type: plugin.ColumnNumber},
			}},
		},
	}
}

func databaseMetricsConfig() plugin.MetricsConfig {
	return plugin.MetricsConfig{
		Stats: []plugin.MetricStat{
			{Key: "collections", Label: "Collections"},
			{Key: "documents", Label: "Documents"},
			{Key: "edges", Label: "Edge documents"},
			{Key: "graphs", Label: "Named graphs"},
			{Key: "views", Label: "Search views"},
			{Key: "analyzers", Label: "Analyzers"},
		},
		Series: []plugin.MetricSeries{
			{Key: "documents", Label: "Documents"},
			{Key: "runningQueries", Label: "Running AQL"},
			{Key: "slowQueries", Label: "Slow AQL"},
		},
		History: 60,
	}
}

func collectionOverviewConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Sections: []plugin.ObjectDetailSection{
			{Title: "Collection", Fields: []plugin.ObjectDetailField{
				{Key: "name", Label: "Name", Copy: true},
				{Key: "id", Label: "ID", Copy: true},
				{Key: "globally_unique_id", Label: "Global ID", Copy: true},
				{Key: "type", Label: "Type", Type: plugin.ColumnBadge, Severities: collectionTypeSeverities},
				{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: collectionStatusSeverities},
				{Key: "system", Label: "System collection", Type: plugin.ColumnBool},
				{Key: "schema_level", Label: "Schema validation", Type: plugin.ColumnBadge},
			}},
			{Title: "Storage", Fields: []plugin.ObjectDetailField{
				{Key: "documents", Label: "Documents", Type: plugin.ColumnNumber},
				{Key: "indexes", Label: "Indexes", Type: plugin.ColumnNumber},
				{Key: "documents_size", Label: "Document data", Type: plugin.ColumnBytes},
				{Key: "indexes_size", Label: "Index data", Type: plugin.ColumnBytes},
				{Key: "revision", Label: "Revision", Copy: true},
				{Key: "cache_enabled", Label: "Cache enabled", Type: plugin.ColumnBool},
			}},
			{Title: "Placement", Fields: []plugin.ObjectDetailField{
				{Key: "shards", Label: "Shards", Type: plugin.ColumnNumber},
				{Key: "shard_keys", Label: "Shard keys"},
				{Key: "replication_factor", Label: "Replication factor", Type: plugin.ColumnBadge},
				{Key: "write_concern", Label: "Write concern", Type: plugin.ColumnNumber},
				{Key: "wait_for_sync", Label: "Wait for sync", Type: plugin.ColumnBool},
				{Key: "key_type", Label: "Key generator", Type: plugin.ColumnBadge},
				{Key: "user_keys", Label: "User keys allowed", Type: plugin.ColumnBool},
			}},
		},
	}
}

func collectionMetricsConfig() plugin.MetricsConfig {
	return plugin.MetricsConfig{
		Stats: []plugin.MetricStat{
			{Key: "documents", Label: "Documents"},
			{Key: "indexes", Label: "Indexes"},
			{Key: "documentsSize", Label: "Document data", Unit: "bytes"},
			{Key: "indexesSize", Label: "Index data", Unit: "bytes"},
		},
		Usage: []plugin.MetricUsage{{
			Key: "cachePct", Label: "Index cache", Type: plugin.ColumnPercent,
			Usage: &plugin.UsageSpec{
				PercentKey: "cachePct", UsedKey: "cacheUsed", TotalKey: "cacheSize",
				UsedType: plugin.ColumnBytes, TotalType: plugin.ColumnBytes,
				TotalLabel: "of", WarnAt: 80, CriticalAt: 95,
			},
		}},
		Series: []plugin.MetricSeries{
			{Key: "documents", Label: "Documents"},
			{Key: "documentsSize", Label: "Document data", Unit: "bytes"},
		},
		History: 60,
	}
}

func graphOverviewConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Sections: []plugin.ObjectDetailSection{
			{Title: "Graph", Fields: []plugin.ObjectDetailField{
				{Key: "name", Label: "Name", Copy: true},
				{Key: "database", Label: "Database"},
				{Key: "kind", Label: "Kind", Type: plugin.ColumnBadge},
				{Key: "smart_graph_attribute", Label: "Smart graph attribute"},
			}},
			{Title: "Structure", Fields: []plugin.ObjectDetailField{
				{Key: "edge_definitions", Label: "Edge definitions", Type: plugin.ColumnNumber},
				{Key: "edge_collections", Label: "Edge collections"},
				{Key: "vertex_collections", Label: "Vertex collections"},
				{Key: "orphan_collections", Label: "Orphan collections"},
				{Key: "vertex_count", Label: "Vertices", Type: plugin.ColumnNumber},
				{Key: "edge_count", Label: "Edges", Type: plugin.ColumnNumber},
			}},
			{Title: "Placement", Fields: []plugin.ObjectDetailField{
				{Key: "number_of_shards", Label: "Shards", Type: plugin.ColumnNumber},
				{Key: "replication_factor", Label: "Replication factor", Type: plugin.ColumnNumber},
				{Key: "write_concern", Label: "Write concern", Type: plugin.ColumnNumber},
			}},
		},
	}
}

func viewOverviewConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Sections: []plugin.ObjectDetailSection{
			{Title: "View", Fields: []plugin.ObjectDetailField{
				{Key: "name", Label: "Name", Copy: true},
				{Key: "id", Label: "ID", Copy: true},
				{Key: "database", Label: "Database"},
				{Key: "type", Label: "Type", Type: plugin.ColumnBadge, Severities: viewTypeSeverities},
			}},
			{Title: "Indexing", Fields: []plugin.ObjectDetailField{
				{Key: "links", Label: "Links", Type: plugin.ColumnNumber},
				{Key: "linked_collections", Label: "Linked collections"},
				{Key: "analyzers", Label: "Analyzers"},
				{Key: "primarySortCompression", Label: "Primary sort compression", Type: plugin.ColumnBadge},
				{Key: "consolidationIntervalMsec", Label: "Consolidation interval", Type: plugin.ColumnNumber},
				{Key: "commitIntervalMsec", Label: "Commit interval", Type: plugin.ColumnNumber},
				{Key: "cleanupIntervalStep", Label: "Cleanup interval step", Type: plugin.ColumnNumber},
			}},
		},
	}
}

func foxxOverviewConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Sections: []plugin.ObjectDetailSection{
			{Title: "Service", Fields: []plugin.ObjectDetailField{
				{Key: "mount", Label: "Mount", Copy: true},
				{Key: "name", Label: "Name"},
				{Key: "version", Label: "Version", Type: plugin.ColumnBadge},
				{Key: "database", Label: "Database"},
				{Key: "development", Label: "Development mode", Type: plugin.ColumnBool},
				{Key: "legacy", Label: "Legacy compatibility", Type: plugin.ColumnBool},
			}},
			{Title: "Manifest", Fields: []plugin.ObjectDetailField{
				{Key: "description", Label: "Description"},
				{Key: "author", Label: "Author"},
				{Key: "license", Label: "License", Type: plugin.ColumnBadge},
				{Key: "main", Label: "Entry point"},
				{Key: "engines", Label: "Engines"},
				{Key: "scripts", Label: "Scripts"},
				{Key: "base_path", Label: "Base path", Copy: true},
				{Key: "routes", Label: "Documented routes", Type: plugin.ColumnNumber},
				{Key: "path", Label: "Server path"},
			}},
		},
	}
}

func clusterOverviewConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Sections: []plugin.ObjectDetailSection{
			{Title: "Deployment", Fields: []plugin.ObjectDetailField{
				{Key: "deployment", Label: "Topology", Type: plugin.ColumnBadge},
				{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: serverStatusSeverities},
				{Key: "role", Label: "Role", Type: plugin.ColumnBadge},
				{Key: "server", Label: "Server"},
				{Key: "version", Label: "Version"},
				{Key: "license", Label: "License", Type: plugin.ColumnBadge},
				{Key: "endpoint", Label: "Endpoint", Copy: true},
				{Key: "cluster_id", Label: "Cluster ID", Copy: true},
				{Key: "server_id", Label: "Server ID", Copy: true},
			}},
			{Title: "Health", Fields: []plugin.ObjectDetailField{
				{Key: "servers_percent", Label: "Healthy servers", Type: plugin.ColumnPercent,
					Usage: &plugin.UsageSpec{
						PercentKey: "servers_percent", UsedKey: "servers_good", TotalKey: "servers_total",
						UsedType: plugin.ColumnNumber, TotalType: plugin.ColumnNumber,
						TotalLabel: "of", Unit: "server(s)", WarnAt: 99, CriticalAt: 60,
					}},
				{Key: "coordinators", Label: "Coordinators", Type: plugin.ColumnNumber},
				{Key: "db_servers", Label: "DB servers", Type: plugin.ColumnNumber},
				{Key: "cleaned_servers", Label: "Cleaned out servers", Type: plugin.ColumnNumber},
				{Key: "endpoints", Label: "Coordinator endpoints"},
			}},
			{Title: "Runtime", Fields: []plugin.ObjectDetailField{
				{Key: "host", Label: "Host"},
				{Key: "pid", Label: "PID", Type: plugin.ColumnNumber},
				{Key: "mode", Label: "Mode", Type: plugin.ColumnBadge},
				{Key: "server_mode", Label: "Server mode", Type: plugin.ColumnBadge},
				{Key: "maintenance", Label: "Maintenance", Type: plugin.ColumnBool},
				{Key: "server_read_only", Label: "Server read-only", Type: plugin.ColumnBool},
				{Key: "foxx_api", Label: "Foxx API enabled", Type: plugin.ColumnBool},
				{Key: "databases", Label: "Databases", Type: plugin.ColumnNumber},
				{Key: "read_only", Label: "Read-only connection", Type: plugin.ColumnBool},
			}},
		},
	}
}

func clusterMetricsConfig() plugin.MetricsConfig {
	return plugin.MetricsConfig{
		Stats: []plugin.MetricStat{
			{Key: "databases", Label: "Databases"},
			{Key: "coordinators", Label: "Coordinators"},
			{Key: "dbServers", Label: "DB servers"},
		},
		Usage: []plugin.MetricUsage{{
			Key: "serversPct", Label: "Healthy servers", Type: plugin.ColumnPercent,
			Usage: &plugin.UsageSpec{
				PercentKey: "serversPct", UsedKey: "serversGood", TotalKey: "serversTotal",
				UsedType: plugin.ColumnNumber, TotalType: plugin.ColumnNumber,
				TotalLabel: "of", Unit: "server(s)", WarnAt: 99, CriticalAt: 60,
			},
		}},
		Series:  []plugin.MetricSeries{{Key: "serversPct", Label: "Healthy servers", Unit: "%"}},
		History: 60,
	}
}

func aqlQueryOverviewConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Sections: []plugin.ObjectDetailSection{{
			Title: "AQL query",
			Fields: []plugin.ObjectDetailField{
				{Key: "id", Label: "Query ID", Copy: true},
				{Key: "state", Label: "State", Type: plugin.ColumnBadge, Severities: queryStateSeverities},
				{Key: "database", Label: "Database"},
				{Key: "user", Label: "User"},
				{Key: "runtime", Label: "Runtime", Type: plugin.ColumnDuration},
				{Key: "peak_memory", Label: "Peak memory", Type: plugin.ColumnBytes},
				{Key: "modification", Label: "Modifies data", Type: plugin.ColumnBool},
				{Key: "stream", Label: "Streaming", Type: plugin.ColumnBool},
				{Key: "started", Label: "Started", Type: plugin.ColumnDateTime},
				{Key: "query", Label: "AQL", Copy: true},
			},
		}},
	}
}

func collectionColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Collection", Sortable: true},
		{Key: "type", Label: "Type", Type: plugin.ColumnBadge, Sortable: true, Severities: collectionTypeSeverities},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Sortable: true, Severities: collectionStatusSeverities},
		{Key: "documents", Label: "Documents", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "shards", Label: "Shards", Type: plugin.ColumnNumber},
		{Key: "replication_factor", Label: "Replication", Type: plugin.ColumnBadge},
		{Key: "wait_for_sync", Label: "Sync", Type: plugin.ColumnBool},
		{Key: "system", Label: "System", Type: plugin.ColumnBool, Sortable: true},
	}
}

func graphColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Graph", Sortable: true},
		{Key: "kind", Label: "Kind", Type: plugin.ColumnBadge, Sortable: true},
		{Key: "edge_definitions", Label: "Edge definitions", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "edge_collections", Label: "Edge collections"},
		{Key: "orphan_collections", Label: "Orphans"},
	}
}

func viewColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "View", Sortable: true},
		{Key: "type", Label: "Type", Type: plugin.ColumnBadge, Sortable: true, Severities: viewTypeSeverities},
		{Key: "links", Label: "Links", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "linked_collections", Label: "Indexed collections"},
	}
}

func analyzerColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Analyzer", Sortable: true},
		{Key: "type", Label: "Type", Type: plugin.ColumnBadge, Sortable: true},
		{Key: "features", Label: "Features"},
		{Key: "unique_name", Label: "Qualified name"},
		{Key: "builtin", Label: "Inherited", Type: plugin.ColumnBool},
	}
}

func foxxColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "mount", Label: "Mount", Sortable: true},
		{Key: "name", Label: "Name", Sortable: true},
		{Key: "version", Label: "Version", Type: plugin.ColumnBadge},
		{Key: "development", Label: "Development", Type: plugin.ColumnBool, Sortable: true},
		{Key: "provides", Label: "Provides"},
	}
}

func indexColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Index", Sortable: true},
		{Key: "type", Label: "Type", Type: plugin.ColumnBadge, Sortable: true},
		{Key: "fields", Label: "Attributes"},
		{Key: "unique", Label: "Unique", Type: plugin.ColumnBool},
		{Key: "sparse", Label: "Sparse", Type: plugin.ColumnBool},
		{Key: "selectivity", Label: "Selectivity", Type: plugin.ColumnPercent},
		{Key: "system", Label: "Built-in", Type: plugin.ColumnBool},
	}
}

func shardColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "shard", Label: "Shard", Sortable: true},
		{Key: "leader", Label: "Leader"},
		{Key: "followers", Label: "Followers"},
		{Key: "replicas", Label: "Replicas", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "state", Label: "State", Type: plugin.ColumnBadge, Severities: map[string]plugin.Severity{
			"replicated": plugin.SeveritySuccess, "unreplicated": plugin.SeverityWarn,
			"unassigned": plugin.SeverityDanger, "single-server": plugin.SeveritySecondary,
		}},
	}
}

func edgeDefinitionColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "collection", Label: "Edge collection", Sortable: true},
		{Key: "from", Label: "From"},
		{Key: "to", Label: "To"},
		{Key: "edges", Label: "Edges", Type: plugin.ColumnNumber, Sortable: true},
	}
}

func vertexCollectionColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Vertex collection", Sortable: true},
		{Key: "documents", Label: "Documents", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "role", Label: "Role", Type: plugin.ColumnBadge, Severities: map[string]plugin.Severity{
			"connected": plugin.SeveritySuccess, "orphan": plugin.SeveritySecondary,
		}},
	}
}

func viewLinkColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "collection", Label: "Collection", Sortable: true},
		{Key: "analyzers", Label: "Analyzers"},
		{Key: "include_all", Label: "All fields", Type: plugin.ColumnBool},
		{Key: "track_positions", Label: "Positions", Type: plugin.ColumnBool},
		{Key: "store_values", Label: "Store values", Type: plugin.ColumnBadge},
		{Key: "fields", Label: "Fields"},
		{Key: "documents", Label: "Documents", Type: plugin.ColumnNumber, Sortable: true},
	}
}

func foxxRouteColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "method", Label: "Method", Type: plugin.ColumnBadge, Sortable: true, Severities: map[string]plugin.Severity{
			"get": plugin.SeverityInfo, "post": plugin.SeveritySuccess,
			"put": plugin.SeverityWarn, "patch": plugin.SeverityWarn, "delete": plugin.SeverityDanger,
		}},
		{Key: "path", Label: "Path", Sortable: true},
		{Key: "summary", Label: "Summary"},
		{Key: "tags", Label: "Tags"},
	}
}

func foxxSettingColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Name", Sortable: true},
		{Key: "title", Label: "Title"},
		{Key: "type", Label: "Type", Type: plugin.ColumnBadge},
		{Key: "required", Label: "Required", Type: plugin.ColumnBool},
		{Key: "default", Label: "Default"},
		{Key: "current", Label: "Current"},
	}
}

func serverColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "short_name", Label: "Server", Sortable: true},
		{Key: "role", Label: "Role", Type: plugin.ColumnBadge, Sortable: true},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Sortable: true, Severities: serverStatusSeverities},
		{Key: "sync_status", Label: "Sync", Type: plugin.ColumnBadge, Severities: serverStatusSeverities},
		{Key: "endpoint", Label: "Endpoint"},
		{Key: "version", Label: "Version"},
		{Key: "last_heartbeat", Label: "Last heartbeat", Type: plugin.ColumnRelativeTime},
		{Key: "can_be_deleted", Label: "Removable", Type: plugin.ColumnBool},
	}
}

func aqlQueryColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "id", Label: "Query ID", Sortable: true},
		{Key: "state", Label: "State", Type: plugin.ColumnBadge, Sortable: true, Severities: queryStateSeverities},
		{Key: "runtime", Label: "Runtime", Type: plugin.ColumnDuration, Sortable: true},
		{Key: "peak_memory", Label: "Peak memory", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "user", Label: "User", Sortable: true},
		{Key: "modification", Label: "Writes", Type: plugin.ColumnBool},
		{Key: "query", Label: "AQL"},
	}
}

func savedColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Name", Sortable: true},
		{Key: "database", Label: "Database", Sortable: true},
		{Key: "writes", Label: "Modifies data", Type: plugin.ColumnBool, Sortable: true},
		{Key: "updated_at", Label: "Updated", Type: plugin.ColumnRelativeTime, Sortable: true},
		{Key: "query", Label: "AQL"},
	}
}
