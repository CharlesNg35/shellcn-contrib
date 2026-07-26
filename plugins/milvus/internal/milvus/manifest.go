package milvus

import "github.com/charlesng35/shellcn/sdk/plugin"

var loadSeverities = map[string]plugin.Severity{
	"loaded":      plugin.SeveritySuccess,
	"loading":     plugin.SeverityWarn,
	"not_loaded":  plugin.SeveritySecondary,
	"unavailable": plugin.SeverityDanger,
	"unknown":     plugin.SeveritySecondary,
}

var indexSeverities = map[string]plugin.Severity{
	"finished":   plugin.SeveritySuccess,
	"inprogress": plugin.SeverityWarn,
	"unissued":   plugin.SeverityInfo,
	"retry":      plugin.SeverityWarn,
	"failed":     plugin.SeverityDanger,
	"none":       plugin.SeveritySecondary,
}

var segmentSeverities = map[string]plugin.Severity{
	"flushed":   plugin.SeveritySuccess,
	"growing":   plugin.SeverityInfo,
	"flushing":  plugin.SeverityWarn,
	"importing": plugin.SeverityInfo,
	"sealed":    plugin.SeverityWarn,
	"dropped":   plugin.SeveritySecondary,
	"notexist":  plugin.SeveritySecondary,
	"none":      plugin.SeveritySecondary,
}

func collectionParams() map[string]string {
	return map[string]string{"database": "${resource.namespace}", "collection": "${resource.name}"}
}

func partitionParams() map[string]string {
	return map[string]string{"database": "${resource.namespace}", "collection": "${resource.scope}", "partition": "${resource.name}"}
}

func indexParams() map[string]string {
	return map[string]string{"database": "${resource.namespace}", "collection": "${resource.scope}", "index": "${resource.name}"}
}

func databaseParams() map[string]string {
	return map[string]string{"name": "${resource.name}"}
}

func userParams() map[string]string { return map[string]string{"user": "${resource.name}"} }
func roleParams() map[string]string { return map[string]string{"role": "${resource.name}"} }

func scopeFilters() []plugin.ScopeFilter {
	return []plugin.ScopeFilter{{
		Param:         "database",
		Label:         "Database",
		Icon:          icon("database"),
		Control:       plugin.ScopeSelect,
		DisableSearch: true,
		OptionsSource: &plugin.DataSource{RouteID: rid("databases.list")},
		ValueField:    "value",
		LabelField:    "label",
		DefaultValue:  defaultDatabase,
	}}
}

func streams() []plugin.Stream {
	return []plugin.Stream{
		{ID: rid("overview.metrics"), Kind: plugin.StreamMetrics, RouteID: rid("overview.metrics")},
		{ID: rid("collection.metrics"), Kind: plugin.StreamMetrics, RouteID: rid("collection.metrics")},
		{ID: rid("collection.watch"), Kind: plugin.StreamResource, RouteID: rid("collection.watch")},
		{ID: rid("collection.projection"), Kind: plugin.StreamCanvas, RouteID: rid("collection.projection")},
		{ID: rid("index.progress"), Kind: plugin.StreamTask, RouteID: rid("index.progress")},
		{ID: rid("query"), Kind: plugin.StreamQuery, RouteID: rid("query")},
	}
}

func tree() []plugin.TreeGroup {
	return []plugin.TreeGroup{
		{Key: "cluster", Label: "Cluster", Icon: icon("server"), Ref: &plugin.ResourceIdentity{Kind: "server", Name: "Milvus", UID: "server"}},
		{Key: "collections", Label: "Collections", Icon: icon("table-2"), Source: plugin.DataSource{RouteID: rid("tree.collections")}, ResourceKind: "collection"},
		{Key: "databases", Label: "Databases", Icon: icon("database"), ResourceKind: "database"},
		{Key: "users", Label: "Users", Icon: icon("users"), ResourceKind: "user"},
		{Key: "roles", Label: "Roles", Icon: icon("shield"), ResourceKind: "role"},
	}
}

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		serverResource(),
		databaseResource(),
		collectionResource(),
		partitionResource(),
		indexResource(),
		userResource(),
		roleResource(),
	}
}

func serverResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "server", Title: "Cluster",
		List:    plugin.DataSource{RouteID: rid("overview")},
		Columns: []plugin.Column{{Key: "name", Label: "Cluster"}, {Key: "version", Label: "Version"}},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "Milvus"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("layout-dashboard"), Type: plugin.PanelDashboard, Config: plugin.DashboardConfig{Cells: []plugin.Panel{
					{Key: "stats", Label: "Cluster", Span: 2, Type: plugin.PanelMetrics,
						Source: &plugin.DataSource{RouteID: rid("overview.metrics"), Method: plugin.MethodWS},
						Config: clusterMetrics()},
					{Key: "collections", Label: "Collections", Span: 2, Type: plugin.PanelTable,
						Source: &plugin.DataSource{RouteID: rid("collections.list")},
						Config: plugin.TableConfig{Columns: collectionColumns(), EmptyText: "No collections in this database yet.", DefaultSort: &plugin.SortKey{Field: "name"}, RefreshIntervalMs: 15000, Exportable: true}},
				}}},
				{Key: "server", Label: "Server", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("overview")}, Config: serverDetail()},
				{Key: "databases", Label: "Databases", Icon: icon("database"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("databases.list")},
					Config: plugin.TableConfig{Columns: databaseColumns(), ActionIDs: []string{rid("database.create")}, RowActionIDs: []string{rid("database.drop")}, EmptyText: "No databases.", DefaultSort: &plugin.SortKey{Field: "name"}, RefreshIntervalMs: 30000, Exportable: true}},
				{Key: "resource_groups", Label: "Resource groups", Icon: icon("boxes"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("resourcegroups.list")},
					Config: plugin.TableConfig{Columns: resourceGroupColumns(), EmptyText: "No resource groups.", DefaultSort: &plugin.SortKey{Field: "name"}, RefreshIntervalMs: 30000}},
				{Key: "saved_queries", Label: "Saved queries", Icon: icon("bookmark"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("queries.list")},
					Config: plugin.TableConfig{Columns: savedQueryColumns(), ActionIDs: []string{rid("query.save")}, RowActionIDs: []string{rid("query.delete")}, EmptyText: "Save a query from the console to reuse it here.", DefaultSort: &plugin.SortKey{Field: "name"}}},
				{Key: "activity", Label: "Activity", Icon: icon("history"), Type: plugin.PanelTimeline,
					Source: &plugin.DataSource{RouteID: rid("activity.list")},
					Config: plugin.TimelineConfig{
						TimestampField: "time", TitleField: "operation", BodyField: "message",
						SeverityField: "severity", IconField: "icon", ResourceField: "target",
						EmptyText: "No operations performed on this connection yet.", RefreshIntervalMs: 5000,
					}},
			},
		},
	}
}

func databaseResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "database", Title: "Databases",
		List:    plugin.DataSource{RouteID: rid("databases.list")},
		Columns: databaseColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{rid("database.create")},
			Row:     []string{rid("database.drop")},
			Detail:  []string{rid("database.drop")},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("database.read"), Params: databaseParams()},
					Config: plugin.ObjectDetailConfig{RawToggle: true, Sections: []plugin.ObjectDetailSection{{
						Title: "Database", Fields: []plugin.ObjectDetailField{
							{Key: "name", Label: "Name", Copy: true},
							{Key: "collection_count", Label: "Collections", Type: plugin.ColumnNumber},
							{Key: "collections", Label: "Collection names", Type: plugin.ColumnJSON},
							{Key: "properties", Label: "Properties", Type: plugin.ColumnJSON},
						},
					}}}},
			},
		},
	}
}

func collectionResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "collection", Title: "Collections",
		List:    plugin.DataSource{RouteID: rid("collections.list")},
		Columns: collectionColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{rid("collection.create")},
			Row:     []string{rid("collection.drop")},
			Detail: []string{
				rid("collection.load"), rid("collection.release"), rid("collection.flush"), rid("collection.compact"),
				rid("index.create"), rid("partition.create"), rid("alias.create"), rid("collection.field.add"),
				rid("collection.rename"), rid("collection.drop"),
			},
		},
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.name}", StatusField: "state", Severities: loadSeverities},
			DefaultTab: "overview",
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("collection.read"), Params: collectionParams()},
					Config: collectionDetailConfig()},
				{Key: "metrics", Label: "Load & segments", Icon: icon("activity"), Type: plugin.PanelMetrics,
					Source: &plugin.DataSource{RouteID: rid("collection.metrics"), Method: plugin.MethodWS, Params: collectionParams()},
					Config: collectionMetricsConfig()},
				{Key: "data", Label: "Data", Icon: icon("table"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("entities.list"), Params: collectionParams()},
					Config: entityGridConfig(true),
					Variants: []plugin.PanelVariant{{
						Type:        plugin.PanelTable,
						Config:      entityGridConfig(false),
						VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "read_only", Op: plugin.OpEq, Value: true}}},
					}}},
				{Key: "schema", Label: "Schema", Icon: icon("list-tree"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("collection.schema"), Params: collectionParams()},
					Config: plugin.TableConfig{Columns: schemaColumns(), ActionIDs: []string{rid("collection.field.add")}, EmptyText: "This collection declares no fields.", DefaultSort: &plugin.SortKey{Field: "id"}, Exportable: true}},
				{Key: "search", Label: "Search", Icon: icon("search"), Type: plugin.PanelQueryEditor,
					Source: &plugin.DataSource{RouteID: rid("query"), Method: plugin.MethodWS, Params: collectionParams()},
					Config: queryEditorConfig()},
				{Key: "projection", Label: "Vector space", Icon: icon("scatter-chart"), Type: plugin.PanelCanvas,
					Source: &plugin.DataSource{RouteID: rid("collection.projection"), Method: plugin.MethodWS, Params: collectionParams()},
					Config: projectionCanvasConfig()},
				{Key: "indexes", Label: "Indexes", Icon: icon("list-ordered"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("indexes.list"), Params: collectionParams()},
					Config: plugin.TableConfig{Columns: indexColumns(), ActionIDs: []string{rid("index.create")}, RowActionIDs: []string{rid("index.drop")}, EmptyText: "No indexes. Vector search needs at least one vector index.", DefaultSort: &plugin.SortKey{Field: "name"}, RefreshIntervalMs: 10000, Exportable: true}},
				{Key: "partitions", Label: "Partitions", Icon: icon("split"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("partitions.list"), Params: collectionParams()},
					Config: plugin.TableConfig{Columns: partitionColumns(), ActionIDs: []string{rid("partition.create")}, RowActionIDs: []string{rid("partition.drop")}, EmptyText: "Only the default partition exists.", DefaultSort: &plugin.SortKey{Field: "name"}, RefreshIntervalMs: 15000, Exportable: true}},
				{Key: "segments", Label: "Segments", Icon: icon("layers"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("segments.list"), Params: collectionParams()},
					Config: plugin.TableConfig{Columns: segmentColumns(), EmptyText: "No persistent segments yet. Flush the collection to seal growing data.", DefaultSort: &plugin.SortKey{Field: "id"}, RefreshIntervalMs: 15000, Exportable: true}},
				{Key: "aliases", Label: "Aliases", Icon: icon("tags"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("aliases.list"), Params: collectionParams()},
					Config: plugin.TableConfig{Columns: aliasColumns(), ActionIDs: []string{rid("alias.create")}, RowActionIDs: []string{rid("alias.drop")}, EmptyText: "No aliases point at this collection.", DefaultSort: &plugin.SortKey{Field: "alias"}}},
				{Key: "graph", Label: "Map", Icon: icon("workflow"), Type: plugin.PanelGraph,
					Source: &plugin.DataSource{RouteID: rid("collection.graph"), Params: collectionParams()},
					Config: plugin.GraphConfig{Layout: plugin.GraphLayoutGrid, FitView: true}},
			},
		},
	}
}

func partitionResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "partition", Title: "Partitions",
		List:    plugin.DataSource{RouteID: rid("partitions.list")},
		Columns: partitionColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{rid("partition.create")},
			Row:     []string{rid("partition.drop")},
			Detail:  []string{rid("partition.load"), rid("partition.release"), rid("partition.drop")},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.scope} / ${resource.name}", StatusField: "state", Severities: loadSeverities},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("partition.read"), Params: partitionParams()},
					Config: plugin.ObjectDetailConfig{RawToggle: true, Sections: []plugin.ObjectDetailSection{{
						Title: "Partition", Fields: []plugin.ObjectDetailField{
							{Key: "name", Label: "Name", Copy: true},
							{Key: "collection", Label: "Collection", Copy: true},
							{Key: "state", Label: "Load state", Type: plugin.ColumnBadge, Severities: loadSeverities},
							{Key: "load_progress", Label: "Load progress", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
								PercentKey: "load_progress", TotalLabel: "of", Unit: "%", WarnAt: 0, CriticalAt: 0,
							}},
							{Key: "entities", Label: "Entities", Type: plugin.ColumnNumber},
						},
					}}}},
				{Key: "data", Label: "Data", Icon: icon("table"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("entities.list"), Params: partitionParams()},
					Config: plugin.TableConfig{
						ColumnsSource: &plugin.DataSource{RouteID: rid("entities.columns"), Params: partitionParams()},
						EmptyText:     "No entities in this partition.", Exportable: true,
					}},
			},
		},
	}
}

func indexResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "index", Title: "Indexes",
		List:    plugin.DataSource{RouteID: rid("indexes.list")},
		Columns: indexColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{rid("index.create")},
			Row:     []string{rid("index.drop")},
			Detail:  []string{rid("index.drop")},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.scope} / ${resource.name}", StatusField: "state", Severities: indexSeverities},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("index.read"), Params: indexParams()},
					Config: plugin.ObjectDetailConfig{RawToggle: true, Sections: []plugin.ObjectDetailSection{{
						Title: "Index", Fields: []plugin.ObjectDetailField{
							{Key: "name", Label: "Name", Copy: true},
							{Key: "field_name", Label: "Field", Copy: true},
							{Key: "index_type", Label: "Index type"},
							{Key: "metric_type", Label: "Metric type"},
							{Key: "state", Label: "State", Type: plugin.ColumnBadge, Severities: indexSeverities},
							{Key: "progress", Label: "Indexed rows", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
								PercentKey: "progress", UsedKey: "indexed_rows", TotalKey: "total_rows",
								UsedType: plugin.ColumnNumber, TotalType: plugin.ColumnNumber, TotalLabel: "of", Unit: "rows",
							}},
							{Key: "pending_rows", Label: "Pending rows", Type: plugin.ColumnNumber},
							{Key: "params", Label: "Parameters", Type: plugin.ColumnJSON},
						},
					}}}},
				{Key: "build", Label: "Build progress", Icon: icon("loader"), Type: plugin.PanelTaskProgress,
					Source: &plugin.DataSource{RouteID: rid("index.progress"), Method: plugin.MethodWS, Params: indexParams()},
					Config: plugin.TaskProgressConfig{Title: "Index build", CancelRouteID: rid("index.drop")}},
			},
		},
	}
}

func userResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "user", Title: "Users",
		List:    plugin.DataSource{RouteID: rid("users.list")},
		Columns: userColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{rid("user.create")},
			Row:     []string{rid("user.delete")},
			Detail:  []string{rid("user.role.grant"), rid("user.password"), rid("user.delete")},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("user"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("user.read"), Params: userParams()},
					Config: plugin.ObjectDetailConfig{RawToggle: true, Sections: []plugin.ObjectDetailSection{{
						Title: "User", Fields: []plugin.ObjectDetailField{
							{Key: "name", Label: "Username", Copy: true},
							{Key: "role_count", Label: "Roles", Type: plugin.ColumnNumber},
							{Key: "roles", Label: "Granted roles", Type: plugin.ColumnJSON},
						},
					}}}},
				{Key: "roles", Label: "Roles", Icon: icon("shield"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("user.roles"), Params: userParams()},
					Config: plugin.TableConfig{Columns: userRoleColumns(), ActionIDs: []string{rid("user.role.grant")}, RowActionIDs: []string{rid("user.role.revoke")}, EmptyText: "This user holds no roles.", DefaultSort: &plugin.SortKey{Field: "role"}}},
			},
		},
	}
}

func roleResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "role", Title: "Roles",
		List:    plugin.DataSource{RouteID: rid("roles.list")},
		Columns: roleColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{rid("role.create")},
			Row:     []string{rid("role.delete")},
			Detail:  []string{rid("role.privilege.grant"), rid("role.privilege.revoke"), rid("role.delete")},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("shield"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("role.read"), Params: roleParams()},
					Config: plugin.ObjectDetailConfig{RawToggle: true, Sections: []plugin.ObjectDetailSection{{
						Title: "Role", Fields: []plugin.ObjectDetailField{
							{Key: "name", Label: "Role", Copy: true},
							{Key: "privilege_count", Label: "Privileges", Type: plugin.ColumnNumber},
							{Key: "users", Label: "Granted to", Type: plugin.ColumnJSON},
						},
					}}}},
				{Key: "privileges", Label: "Privileges", Icon: icon("key"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("role.privileges"), Params: roleParams()},
					Config: plugin.TableConfig{Columns: privilegeColumns(), ActionIDs: []string{rid("role.privilege.grant")}, EmptyText: "This role holds no privileges.", DefaultSort: &plugin.SortKey{Field: "privilege"}, Exportable: true}},
			},
		},
	}
}

func actions() []plugin.Action {
	return []plugin.Action{
		{ID: rid("database.create"), Label: "Create database", Icon: icon("plus"), RouteID: rid("database.create"),
			OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList}},
		{ID: rid("database.drop"), Label: "Drop database", Icon: icon("trash-2"), RouteID: rid("database.drop"), Params: databaseParams(),
			Confirm: true, ConfirmText: "Drop this database and every collection inside it? Vectors and indexes are deleted permanently.", Bulk: true,
			OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList}},

		{ID: rid("collection.create"), Label: "Create collection", Icon: icon("plus"), RouteID: rid("collection.create"),
			OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList}},
		{ID: rid("collection.field.add"), Label: "Add field", Icon: icon("list-plus"), RouteID: rid("collection.field.add"), Params: collectionParams(),
			Group: "Schema", OnSuccess: &plugin.ActionSuccess{SelectTab: "schema"}},
		{ID: rid("collection.rename"), Label: "Rename", Icon: icon("pencil"), RouteID: rid("collection.rename"), Params: collectionParams(),
			Group: "Schema", OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList}},
		{ID: rid("collection.load"), Label: "Load", Icon: icon("download"), RouteID: rid("collection.load"), Params: collectionParams(),
			Group: "Lifecycle", EnabledWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "state", Op: plugin.OpNeq, Value: "loaded"}}},
			OnSuccess: &plugin.ActionSuccess{SelectTab: "metrics"}},
		{ID: rid("collection.release"), Label: "Release", Icon: icon("eject"), RouteID: rid("collection.release"), Params: collectionParams(),
			Group: "Lifecycle", EnabledWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "state", Op: plugin.OpEq, Value: "loaded"}}},
			OnSuccess: &plugin.ActionSuccess{SelectTab: "metrics"}},
		{ID: rid("collection.flush"), Label: "Flush", Icon: icon("save"), RouteID: rid("collection.flush"), Params: collectionParams(),
			Group: "Maintenance", Confirm: true, ConfirmText: "Seal growing segments and persist them? Writes pause briefly while the flush runs.",
			OnSuccess: &plugin.ActionSuccess{SelectTab: "segments"}},
		{ID: rid("collection.compact"), Label: "Compact", Icon: icon("layers"), RouteID: rid("collection.compact"), Params: collectionParams(),
			Group: "Maintenance", Confirm: true, ConfirmText: "Start a compaction? It rewrites segments and consumes cluster I/O until it finishes.",
			OnSuccess: &plugin.ActionSuccess{SelectTab: "segments"}},
		{ID: rid("collection.drop"), Label: "Drop collection", Icon: icon("trash-2"), RouteID: rid("collection.drop"), Params: collectionParams(),
			Confirm: true, ConfirmText: "Drop this collection? Every entity, index and partition is deleted permanently.", Bulk: true,
			OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList}},

		{ID: rid("partition.create"), Label: "Create partition", Icon: icon("plus"), RouteID: rid("partition.create"), Params: collectionParams(),
			OnSuccess: &plugin.ActionSuccess{SelectTab: "partitions"}},
		{ID: rid("partition.load"), Label: "Load partition", Icon: icon("download"), RouteID: rid("partition.load"), Params: partitionParams(),
			Group: "Lifecycle", EnabledWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "state", Op: plugin.OpNeq, Value: "loaded"}}},
			OnSuccess: &plugin.ActionSuccess{SelectTab: "overview"}},
		{ID: rid("partition.release"), Label: "Release partition", Icon: icon("eject"), RouteID: rid("partition.release"), Params: partitionParams(),
			Group: "Lifecycle", EnabledWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "state", Op: plugin.OpEq, Value: "loaded"}}},
			OnSuccess: &plugin.ActionSuccess{SelectTab: "overview"}},
		{ID: rid("partition.drop"), Label: "Drop partition", Icon: icon("trash"), RouteID: rid("partition.drop"), Params: partitionParams(),
			Confirm: true, ConfirmText: "Drop this partition? Its entities are deleted permanently.", Bulk: true,
			OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList}},

		{ID: rid("index.create"), Label: "Create index", Icon: icon("plus"), RouteID: rid("index.create"), Params: collectionParams(),
			OnSuccess: &plugin.ActionSuccess{SelectTab: "indexes"}},
		{ID: rid("index.drop"), Label: "Drop index", Icon: icon("trash"), RouteID: rid("index.drop"), Params: indexParams(),
			Confirm: true, ConfirmText: "Drop this index? Vector search falls back to brute force until it is rebuilt.", Bulk: true,
			OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList}},

		{ID: rid("alias.create"), Label: "Create alias", Icon: icon("tag"), RouteID: rid("alias.create"), Params: collectionParams(),
			OnSuccess: &plugin.ActionSuccess{SelectTab: "aliases"}},
		{ID: rid("alias.drop"), Label: "Drop alias", Icon: icon("trash"), RouteID: rid("alias.drop"),
			Params:  map[string]string{"database": "${record.database}", "alias": "${record.alias}"},
			Confirm: true, ConfirmText: "Drop this alias? Clients using it will stop resolving.", Bulk: true},

		{ID: rid("query.save"), Label: "Save query", Icon: icon("bookmark"), RouteID: rid("query.save"),
			OnSuccess: &plugin.ActionSuccess{SelectTab: "saved_queries"}},
		{ID: rid("query.delete"), Label: "Delete saved query", Icon: icon("trash"), RouteID: rid("query.delete"),
			Params: map[string]string{"id": "${record.id}"}, Confirm: true, ConfirmText: "Delete this saved query?", Bulk: true},

		{ID: rid("user.create"), Label: "Create user", Icon: icon("user-plus"), RouteID: rid("user.create"),
			OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList}},
		{ID: rid("user.password"), Label: "Change password", Icon: icon("key"), RouteID: rid("user.password"), Params: userParams(),
			OnSuccess: &plugin.ActionSuccess{SelectTab: "overview"}},
		{ID: rid("user.role.grant"), Label: "Grant role", Icon: icon("shield-plus"), RouteID: rid("user.role.grant"), Params: userParams(),
			Confirm: true, ConfirmText: "Grant this role? The user immediately gains every privilege the role holds.",
			OnSuccess: &plugin.ActionSuccess{SelectTab: "roles"}},
		{ID: rid("user.role.revoke"), Label: "Revoke role", Icon: icon("shield-minus"), RouteID: rid("user.role.revoke"),
			Params:  map[string]string{"user": "${resource.name}", "role": "${record.role}"},
			Confirm: true, ConfirmText: "Revoke this role from the user?", Bulk: true,
			OnSuccess: &plugin.ActionSuccess{SelectTab: "roles"}},
		{ID: rid("user.delete"), Label: "Delete user", Icon: icon("user-minus"), RouteID: rid("user.delete"), Params: userParams(),
			Confirm: true, ConfirmText: "Delete this user? Their sessions stop working immediately.", Bulk: true,
			OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList}},

		{ID: rid("role.create"), Label: "Create role", Icon: icon("plus"), RouteID: rid("role.create"),
			OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList}},
		{ID: rid("role.privilege.grant"), Label: "Grant privilege", Icon: icon("key"), RouteID: rid("role.privilege.grant"), Params: roleParams(),
			Confirm: true, ConfirmText: "Grant this privilege? Every user holding the role gains it immediately.",
			OnSuccess: &plugin.ActionSuccess{SelectTab: "privileges"}},
		{ID: rid("role.privilege.revoke"), Label: "Revoke privilege", Icon: icon("shield-minus"), RouteID: rid("role.privilege.revoke"), Params: roleParams(),
			Confirm: true, ConfirmText: "Revoke this privilege from the role?",
			OnSuccess: &plugin.ActionSuccess{SelectTab: "privileges"}},
		{ID: rid("role.delete"), Label: "Delete role", Icon: icon("trash-2"), RouteID: rid("role.delete"), Params: roleParams(),
			Confirm: true, ConfirmText: "Delete this role? Users lose every privilege it granted.", Bulk: true,
			OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList}},
	}
}

func serverDetail() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Sections: []plugin.ObjectDetailSection{
			{Title: "Server", Fields: []plugin.ObjectDetailField{
				{Key: "version", Label: "Version", Copy: true},
				{Key: "address", Label: "Address", Copy: true},
				{Key: "database", Label: "Active database", Copy: true},
				{Key: "read_only", Label: "Read-only connection", Type: plugin.ColumnBool},
			}},
			{Title: "Inventory", Fields: []plugin.ObjectDetailField{
				{Key: "databases", Label: "Databases", Type: plugin.ColumnNumber},
				{Key: "collections", Label: "Collections", Type: plugin.ColumnNumber},
				{Key: "loaded_pct", Label: "Loaded collections", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
					PercentKey: "loaded_pct", UsedKey: "loaded_collections", TotalKey: "collections",
					UsedType: plugin.ColumnNumber, TotalType: plugin.ColumnNumber, TotalLabel: "of", Unit: "collection(s)",
				}},
				{Key: "entities", Label: "Entities", Type: plugin.ColumnNumber},
				{Key: "partitions", Label: "Partitions", Type: plugin.ColumnNumber},
				{Key: "indexes", Label: "Indexes", Type: plugin.ColumnNumber},
			}},
			{Title: "Access control", Fields: []plugin.ObjectDetailField{
				{Key: "users", Label: "Users", Type: plugin.ColumnNumber},
				{Key: "roles", Label: "Roles", Type: plugin.ColumnNumber},
				{Key: "resource_groups", Label: "Resource groups", Type: plugin.ColumnJSON},
			}},
		},
	}
}

func collectionDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Watch:     &plugin.DataSource{RouteID: rid("collection.watch"), Method: plugin.MethodWS, Params: collectionParams()},
		Sections: []plugin.ObjectDetailSection{
			{Title: "Identity", Fields: []plugin.ObjectDetailField{
				{Key: "name", Label: "Collection", Copy: true},
				{Key: "database", Label: "Database", Copy: true},
				{Key: "id", Label: "Collection id", Copy: true},
				{Key: "description", Label: "Description"},
				{Key: "state", Label: "Load state", Type: plugin.ColumnBadge, Severities: loadSeverities},
			}},
			{Title: "Schema", Fields: []plugin.ObjectDetailField{
				{Key: "primary_key", Label: "Primary key", Copy: true},
				{Key: "auto_id", Label: "Auto id", Type: plugin.ColumnBool},
				{Key: "dynamic_field", Label: "Dynamic field", Type: plugin.ColumnBool},
				{Key: "field_count", Label: "Fields", Type: plugin.ColumnNumber},
				{Key: "vector_fields", Label: "Vector fields", Type: plugin.ColumnJSON},
				{Key: "functions", Label: "Functions", Type: plugin.ColumnJSON},
				{Key: "consistency_level", Label: "Consistency level"},
				{Key: "shards", Label: "Shards", Type: plugin.ColumnNumber},
			}},
			{Title: "Storage", Fields: []plugin.ObjectDetailField{
				{Key: "entities", Label: "Entities", Type: plugin.ColumnNumber},
				{Key: "partitions", Label: "Partitions", Type: plugin.ColumnNumber},
				{Key: "indexes", Label: "Indexes", Type: plugin.ColumnNumber},
				{Key: "aliases", Label: "Aliases", Type: plugin.ColumnJSON},
				{Key: "virtual_channels", Label: "Virtual channels", Type: plugin.ColumnJSON},
				{Key: "properties", Label: "Properties", Type: plugin.ColumnJSON},
			}},
		},
	}
}

func clusterMetrics() plugin.MetricsConfig {
	return plugin.MetricsConfig{
		Stats: []plugin.MetricStat{
			{Key: "collections", Label: "Collections"},
			{Key: "entities", Label: "Entities"},
			{Key: "partitions", Label: "Partitions"},
			{Key: "indexes", Label: "Indexes"},
			{Key: "databases", Label: "Databases"},
			{Key: "users", Label: "Users"},
		},
		Usage: []plugin.MetricUsage{{
			Key: "loaded_pct", Label: "Loaded collections", Type: plugin.ColumnPercent,
			Usage: &plugin.UsageSpec{
				PercentKey: "loaded_pct", UsedKey: "loaded_collections", TotalKey: "collections",
				UsedType: plugin.ColumnNumber, TotalType: plugin.ColumnNumber, TotalLabel: "of", Unit: "collection(s)",
				WarnAt: 0, CriticalAt: 0,
			},
		}},
		Series:  []plugin.MetricSeries{{Key: "entities", Label: "Entities"}, {Key: "loaded_pct", Label: "Loaded", Unit: "%"}},
		History: 60,
	}
}

func collectionMetricsConfig() plugin.MetricsConfig {
	return plugin.MetricsConfig{
		Stats: []plugin.MetricStat{
			{Key: "entities", Label: "Entities"},
			{Key: "segments", Label: "Segments"},
			{Key: "flushedSegments", Label: "Flushed segments"},
			{Key: "partitions", Label: "Partitions"},
			{Key: "indexes", Label: "Indexes"},
		},
		Usage: []plugin.MetricUsage{
			{Key: "loadProgress", Label: "Load progress", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
				PercentKey: "loadProgress", TotalLabel: "of", Unit: "%",
			}},
			{Key: "indexedPct", Label: "Indexed rows", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
				PercentKey: "indexedPct", UsedKey: "indexedRows", TotalKey: "indexTotalRows",
				UsedType: plugin.ColumnNumber, TotalType: plugin.ColumnNumber, TotalLabel: "of", Unit: "rows",
			}},
			{Key: "flushedPct", Label: "Flushed rows", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
				PercentKey: "flushedPct", UsedKey: "flushedRows", TotalKey: "segmentRows",
				UsedType: plugin.ColumnNumber, TotalType: plugin.ColumnNumber, TotalLabel: "of", Unit: "rows",
			}},
		},
		Series:  []plugin.MetricSeries{{Key: "entities", Label: "Entities"}, {Key: "indexedPct", Label: "Indexed", Unit: "%"}},
		History: 60,
	}
}

// entityGridConfig drives the Data grid. A read-only connection gets the same
// grid without the mutation routes, so the UI offers nothing the session refuses.
func entityGridConfig(writable bool) plugin.TableConfig {
	cfg := plugin.TableConfig{
		ColumnsSource: &plugin.DataSource{RouteID: rid("entities.columns"), Params: collectionParams()},
		RowKey:        []string{entityKeyField},
		HiddenColumns: []string{entityKeyField},
		EmptyText:     "No entities. Load the collection, then query rows.",
		Exportable:    true,
	}
	if !writable {
		return cfg
	}
	cfg.Editable = true
	cfg.StagedEdits = true
	cfg.Insert = &plugin.DataSource{RouteID: rid("entity.insert"), Method: plugin.MethodPost, Params: collectionParams()}
	cfg.Update = &plugin.DataSource{RouteID: rid("entity.update"), Method: plugin.MethodPatch, Params: collectionParams()}
	cfg.Delete = &plugin.DataSource{RouteID: rid("entity.delete"), Method: plugin.MethodDelete, Params: collectionParams()}
	cfg.EmptyText = "No entities. Load the collection, then insert or query rows."
	return cfg
}

func queryEditorConfig() plugin.QueryEditorConfig {
	return plugin.QueryEditorConfig{
		Language:          "json",
		Label:             "Milvus search",
		ExecuteLabel:      "Run search",
		RunningLabel:      "Searching…",
		CancelLabel:       "Cancel",
		EmptyText:         "Run an ANN search, a full-text search, a scalar filter query, or a primary-key lookup.",
		InitialQuery:      `{"vector":[0.1,0.2,0.3,0.4],"limit":10,"output":["*"]}`,
		CompletionRouteID: rid("query.complete"),
		CompletionParams:  collectionParams(),
		Exportable:        true,
	}
}

func projectionCanvasConfig() plugin.CanvasConfig {
	return plugin.CanvasConfig{
		ScaleMode:      plugin.CanvasScaleResize,
		Interactive:    true,
		Pointer:        true,
		Keyboard:       true,
		ResizeEvents:   true,
		WheelMode:      plugin.CanvasWheelModified,
		HiDPI:          true,
		FocusOnPointer: true,
		AriaLabel:      "Embedding projection explorer",
		Instructions:   "Two-dimensional projection of sampled vectors. Drag to lasso-select points, hover for id and payload. Press R to resample, P to switch between PCA and random projection, F to project the next vector field, C to clear the selection, 0 to reset the view, plus and minus to zoom, arrow keys to pan.",
	}
}

func collectionColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Collection", Sortable: true},
		{Key: "state", Label: "Load state", Type: plugin.ColumnBadge, Sortable: true, Severities: loadSeverities},
		{Key: "entities", Label: "Entities", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "field_count", Label: "Fields", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "partitions", Label: "Partitions", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "indexes", Label: "Indexes", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "consistency_level", Label: "Consistency", Sortable: true},
		{Key: "vector_fields", Label: "Vector fields", Type: plugin.ColumnJSON},
	}
}

func databaseColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Database", Sortable: true},
		{Key: "property_count", Label: "Properties", Type: plugin.ColumnNumber, Sortable: true},
	}
}

func schemaColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Field", Sortable: true},
		{Key: "type", Label: "Type", Sortable: true},
		{Key: "dim", Label: "Dim", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "max_length", Label: "Max length", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "primary", Label: "Primary", Type: plugin.ColumnBool, Sortable: true},
		{Key: "auto_id", Label: "Auto id", Type: plugin.ColumnBool},
		{Key: "nullable", Label: "Nullable", Type: plugin.ColumnBool},
		{Key: "partition_key", Label: "Partition key", Type: plugin.ColumnBool},
		{Key: "clustering_key", Label: "Clustering key", Type: plugin.ColumnBool},
		{Key: "index", Label: "Index", Sortable: true},
		{Key: "description", Label: "Description"},
	}
}

func partitionColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Partition", Sortable: true},
		{Key: "state", Label: "Load state", Type: plugin.ColumnBadge, Sortable: true, Severities: loadSeverities},
		{Key: "entities", Label: "Entities", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "load_progress", Label: "Load progress", Type: plugin.ColumnPercent, Sortable: true},
	}
}

func indexColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Index", Sortable: true},
		{Key: "field_name", Label: "Field", Sortable: true},
		{Key: "index_type", Label: "Type", Sortable: true},
		{Key: "metric_type", Label: "Metric", Sortable: true},
		{Key: "state", Label: "State", Type: plugin.ColumnBadge, Sortable: true, Severities: indexSeverities},
		{Key: "progress", Label: "Built", Type: plugin.ColumnPercent, Sortable: true},
		{Key: "indexed_rows", Label: "Indexed rows", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "pending_rows", Label: "Pending rows", Type: plugin.ColumnNumber, Sortable: true},
	}
}

func segmentColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "id", Label: "Segment", Sortable: true},
		{Key: "state", Label: "State", Type: plugin.ColumnBadge, Sortable: true, Severities: segmentSeverities},
		{Key: "rows", Label: "Rows", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "partition_id", Label: "Partition id", Sortable: true},
		{Key: "flushed", Label: "Flushed", Type: plugin.ColumnBool, Sortable: true},
	}
}

func aliasColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "alias", Label: "Alias", Sortable: true},
		{Key: "collection", Label: "Collection", Sortable: true},
		{Key: "database", Label: "Database", Sortable: true},
	}
}

func resourceGroupColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Resource group", Sortable: true},
		{Key: "capacity", Label: "Capacity", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "available_nodes", Label: "Available nodes", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "nodes", Label: "Nodes", Type: plugin.ColumnNumber, Sortable: true},
	}
}

func userColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "User", Sortable: true},
		{Key: "role_count", Label: "Roles", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "roles", Label: "Granted roles", Type: plugin.ColumnJSON},
	}
}

func userRoleColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "role", Label: "Role", Sortable: true},
		{Key: "user", Label: "Granted to", Sortable: true},
	}
}

func roleColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Role", Sortable: true},
		{Key: "privilege_count", Label: "Privileges", Type: plugin.ColumnNumber, Sortable: true},
	}
}

func privilegeColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "privilege", Label: "Privilege", Sortable: true},
		{Key: "object", Label: "Object", Sortable: true},
		{Key: "object_name", Label: "Object name", Sortable: true},
		{Key: "database", Label: "Database", Sortable: true},
		{Key: "grantor", Label: "Grantor", Sortable: true},
	}
}

func savedQueryColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Name", Sortable: true},
		{Key: "collection", Label: "Collection", Sortable: true},
		{Key: "query", Label: "Query"},
		{Key: "updated_at", Label: "Updated", Type: plugin.ColumnRelativeTime, Sortable: true},
	}
}
