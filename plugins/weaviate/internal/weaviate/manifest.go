package weaviate

import "github.com/charlesng35/shellcn/sdk/plugin"

func icon(name string) plugin.Icon { return plugin.Icon{Type: plugin.IconLucide, Value: name} }

func rid(suffix string) string { return protocolName + "." + suffix }

func collectionParams() map[string]string { return map[string]string{"collection": "${resource.name}"} }

func objectParams() map[string]string {
	return map[string]string{"collection": "${resource.namespace}", "id": "${resource.name}"}
}

func nodeParams() map[string]string { return map[string]string{"node": "${resource.name}"} }

func aliasParams() map[string]string { return map[string]string{"alias": "${resource.name}"} }

func backupParams() map[string]string {
	return map[string]string{"backend": "${resource.namespace}", "backup": "${resource.name}"}
}

func savedQueryParams() map[string]string { return map[string]string{"id": "${resource.uid}"} }

func watchSource(kind string, extra map[string]string) *plugin.DataSource {
	params := map[string]string{"kind": kind}
	for key, value := range extra {
		params[key] = value
	}
	return &plugin.DataSource{RouteID: rid("resource.watch"), Method: plugin.MethodWS, Params: params}
}

var (
	indexingSeverities = map[string]plugin.Severity{
		"ready": plugin.SeveritySuccess, "indexing": plugin.SeverityWarn, "": plugin.SeveritySecondary,
	}
	nodeSeverities = map[string]plugin.Severity{
		"healthy": plugin.SeveritySuccess, "unhealthy": plugin.SeverityDanger,
		"unavailable": plugin.SeverityDanger, "indexing": plugin.SeverityWarn, "timeout": plugin.SeverityWarn,
	}
	shardSeverities = map[string]plugin.Severity{
		"ready": plugin.SeveritySuccess, "readonly": plugin.SeverityWarn,
	}
	tenantSeverities = map[string]plugin.Severity{
		"active": plugin.SeveritySuccess, "inactive": plugin.SeveritySecondary, "offloaded": plugin.SeverityWarn,
	}
	backupSeverities = map[string]plugin.Severity{
		"success": plugin.SeveritySuccess, "started": plugin.SeverityInfo, "transferring": plugin.SeverityInfo,
		"transferred": plugin.SeverityInfo, "failed": plugin.SeverityDanger, "canceled": plugin.SeveritySecondary,
	}
	moduleSeverities = map[string]plugin.Severity{
		"vectorizer": plugin.SeverityInfo, "generative": plugin.SeveritySuccess, "reranker": plugin.SeverityWarn,
		"backup": plugin.SeveritySecondary, "reader": plugin.SeverityInfo, "other": plugin.SeveritySecondary,
	}
)

func streams() []plugin.Stream {
	return []plugin.Stream{
		{ID: rid("graphql"), Kind: plugin.StreamQuery, RouteID: rid("graphql")},
		{ID: rid("search"), Kind: plugin.StreamQuery, RouteID: rid("search")},
		{ID: rid("metrics"), Kind: plugin.StreamMetrics, RouteID: rid("metrics")},
		{ID: rid("embedding"), Kind: plugin.StreamCanvas, RouteID: rid("embedding")},
		{ID: rid("resource.watch"), Kind: plugin.StreamResource, RouteID: rid("resource.watch")},
	}
}

func tree() []plugin.TreeGroup {
	return []plugin.TreeGroup{
		{Key: "overview", Label: "Overview", Icon: icon("layout-dashboard"), Ref: &plugin.ResourceIdentity{Kind: "server", Name: "Weaviate", UID: "server"}},
		{Key: "collections", Label: "Collections", Icon: icon("layers"), Source: plugin.DataSource{RouteID: rid("collections.tree")}, ResourceKind: "collection"},
		{Key: "nodes", Label: "Cluster nodes", Icon: icon("server"), ResourceKind: "node"},
		{Key: "aliases", Label: "Aliases", Icon: icon("tag"), ResourceKind: "alias"},
		{Key: "backups", Label: "Backups", Icon: icon("archive"), ResourceKind: "backup"},
		{Key: "saved_queries", Label: "Saved queries", Icon: icon("bookmark"), ResourceKind: "saved_query"},
	}
}

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		serverResource(),
		collectionResource(),
		objectResource(),
		nodeResource(),
		aliasResource(),
		backupResource(),
		savedQueryResource(),
	}
}

func serverResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "server", Title: "Weaviate",
		List:    plugin.DataSource{RouteID: rid("overview")},
		Columns: []plugin.Column{{Key: "hostname", Label: "Host"}, {Key: "version", Label: "Version"}},
		Actions: plugin.ResourceActions{Toolbar: []string{rid("collection.create"), rid("backup.create")}},
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "Weaviate"},
			DefaultTab: "overview",
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("overview")}, Config: serverDetailConfig()},
				{Key: "live", Label: "Live", Icon: icon("activity"), Type: plugin.PanelMetrics,
					Source: &plugin.DataSource{RouteID: rid("metrics"), Method: plugin.MethodWS}, Config: clusterMetricsConfig()},
				{Key: "collections", Label: "Collections", Icon: icon("layers"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("collections.list")}, Config: plugin.TableConfig{
						Columns:     collectionColumns(),
						Watch:       watchSource("collection", nil),
						ActionIDs:   []string{rid("collection.create")},
						DefaultSort: &plugin.SortKey{Field: "name"},
						EmptyText:   "No collections yet. Create one to define a schema and start importing objects.",
						Exportable:  true,
						RowClick:    plugin.RowClickNavigate,
					}},
				{Key: "schema", Label: "Schema graph", Icon: icon("workflow"), Type: plugin.PanelGraph,
					Source: &plugin.DataSource{RouteID: rid("schema.graph")}, Config: plugin.GraphConfig{
						Layout: plugin.GraphLayoutGrid, FitView: true,
						ExpandRouteID: rid("schema.graph.expand"), ExpandParam: "node",
					}},
				{Key: "graphql", Label: "GraphQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor,
					Source: &plugin.DataSource{RouteID: rid("graphql"), Method: plugin.MethodWS}, Config: graphqlEditorConfig()},
				{Key: "modules", Label: "Modules", Icon: icon("puzzle"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("modules.list")}, Config: plugin.TableConfig{
						Columns:     moduleColumns(),
						DefaultSort: &plugin.SortKey{Field: "name"},
						EmptyText:   "This server runs without optional modules.",
						Exportable:  true,
					}},
				{Key: "activity", Label: "Activity", Icon: icon("history"), Type: plugin.PanelTimeline,
					Source: &plugin.DataSource{RouteID: rid("activity.list")}, Config: plugin.TimelineConfig{
						TimestampField:    "time",
						TitleField:        "title",
						BodyField:         "message",
						SeverityField:     "severity",
						IconField:         "icon",
						ResourceField:     "category",
						EmptyText:         "Schema changes, writes, and queries you run from this connection appear here.",
						RefreshIntervalMs: 10000,
					}},
			},
		},
	}
}

func collectionResource() plugin.ResourceType {
	multiTenant := &plugin.Condition{AllOf: []plugin.Rule{{Field: "multiTenancy", Op: plugin.OpEq, Value: true}}}
	return plugin.ResourceType{
		Kind: "collection", Title: "Collections",
		List:    plugin.DataSource{RouteID: rid("collections.list")},
		Watch:   watchSource("collection", nil),
		Columns: collectionColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{rid("collection.create")},
			Row:     []string{rid("collection.delete")},
			Detail:  []string{rid("property.create"), rid("object.create"), rid("objects.delete"), rid("alias.create"), rid("backup.create"), rid("collection.delete")},
		},
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.name}", StatusField: "status", Severities: indexingSeverities},
			DefaultTab: "overview",
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("collection.read"), Params: collectionParams()}, Config: collectionDetailConfig()},
				{Key: "properties", Label: "Properties", Icon: icon("list"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("properties.list"), Params: collectionParams()}, Config: plugin.TableConfig{
						Columns:     propertyColumns(),
						ActionIDs:   []string{rid("property.create")},
						DefaultSort: &plugin.SortKey{Field: "name"},
						EmptyText:   "This collection has no properties. Add one to make objects searchable.",
						Exportable:  true,
					}},
				{Key: "objects", Label: "Objects", Icon: icon("braces"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("objects.list"), Params: collectionParams()}, Config: objectsTableConfig()},
				{Key: "search", Label: "Vector search", Icon: icon("search"), Type: plugin.PanelQueryEditor,
					Source: &plugin.DataSource{RouteID: rid("search"), Method: plugin.MethodWS, Params: collectionParams()}, Config: searchEditorConfig()},
				{Key: "embedding", Label: "Embedding map", Icon: icon("scatter-chart"), Type: plugin.PanelCanvas,
					Source: &plugin.DataSource{RouteID: rid("embedding"), Method: plugin.MethodWS, Params: collectionParams()}, Config: embeddingCanvasConfig()},
				{Key: "references", Label: "References", Icon: icon("workflow"), Type: plugin.PanelGraph,
					Source: &plugin.DataSource{RouteID: rid("schema.graph.expand"), Params: map[string]string{"node": "class:${resource.name}"}},
					Config: plugin.GraphConfig{Layout: plugin.GraphLayoutGrid, FitView: true, ExpandRouteID: rid("schema.graph.expand"), ExpandParam: "node"}},
				{Key: "definition", Label: "Definition", Icon: icon("code"), Type: plugin.PanelCodeEditor,
					Source: &plugin.DataSource{RouteID: rid("collection.definition"), Params: collectionParams()}, Config: plugin.CodeEditorConfig{
						Language:     "json",
						SaveRouteID:  rid("collection.update"),
						SaveMethod:   plugin.MethodPut,
						SaveParams:   collectionParams(),
						SaveBodyKey:  "class",
						RefreshField: "content",
						SaveToast:    &plugin.SaveToast{Summary: "Definition applied", Detail: "${response.name} updated", Severity: plugin.SeveritySuccess},
					}},
				{Key: "shards", Label: "Shards", Icon: icon("server"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("shards.list"), Params: collectionParams()}, Config: plugin.TableConfig{
						Columns:           shardColumns(),
						RowActionIDs:      []string{rid("shard.update")},
						DefaultSort:       &plugin.SortKey{Field: "name"},
						RefreshIntervalMs: 15000,
						EmptyText:         "No shards reported for this collection.",
						Exportable:        true,
					}},
				{Key: "tenants", Label: "Tenants", Icon: icon("users"), Type: plugin.PanelTable,
					VisibleWhen: multiTenant,
					Source:      &plugin.DataSource{RouteID: rid("tenants.list"), Params: collectionParams()}, Config: plugin.TableConfig{
						Columns:      tenantColumns(),
						ActionIDs:    []string{rid("tenant.create")},
						RowActionIDs: []string{rid("tenant.update"), rid("tenant.delete")},
						DefaultSort:  &plugin.SortKey{Field: "name"},
						EmptyText:    "No tenants yet. Create one to isolate this collection's data.",
						Exportable:   true,
					}},
				{Key: "aliases", Label: "Aliases", Icon: icon("tag"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("aliases.list"), Params: collectionParams()}, Config: plugin.TableConfig{
						Columns:     aliasColumns(),
						ActionIDs:   []string{rid("alias.create")},
						DefaultSort: &plugin.SortKey{Field: "name"},
						EmptyText:   "No aliases point at this collection.",
						Exportable:  true,
					}},
			},
		},
	}
}

func objectResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "object", Title: "Objects",
		List:    plugin.DataSource{RouteID: rid("objects.list"), Params: map[string]string{"collection": "${resource.namespace}"}},
		Watch:   watchSource("object", map[string]string{"collection": "${resource.namespace}"}),
		Columns: []plugin.Column{{Key: "id", Label: "ID", Sortable: false}},
		Actions: plugin.ResourceActions{
			Detail: []string{rid("reference.create"), rid("reference.delete"), rid("object.delete")},
		},
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.namespace} · ${resource.name}"},
			DefaultTab: "overview",
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("object.read"), Params: objectParams()}, Config: objectDetailConfig()},
				{Key: "properties", Label: "Properties", Icon: icon("code"), Type: plugin.PanelCodeEditor,
					Source: &plugin.DataSource{RouteID: rid("object.document"), Params: objectParams()}, Config: plugin.CodeEditorConfig{
						Language:     "json",
						SaveRouteID:  rid("object.update"),
						SaveMethod:   plugin.MethodPut,
						SaveParams:   objectParams(),
						SaveBodyKey:  "properties",
						RefreshField: "content",
						SaveToast:    &plugin.SaveToast{Summary: "Object updated", Detail: "${response.id}", Severity: plugin.SeveritySuccess},
					}},
				{Key: "references", Label: "References", Icon: icon("link"), Type: plugin.PanelGraph,
					Source: &plugin.DataSource{RouteID: rid("object.references"), Params: objectParams()}, Config: plugin.GraphConfig{
						Layout: plugin.GraphLayoutGrid, FitView: true,
						ExpandRouteID: rid("object.references.expand"), ExpandParam: "node",
					}},
			},
		},
	}
}

func nodeResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "node", Title: "Cluster nodes",
		List:    plugin.DataSource{RouteID: rid("nodes.list")},
		Watch:   watchSource("node", nil),
		Columns: nodeColumns(),
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.name}", StatusField: "status", Severities: nodeSeverities},
			DefaultTab: "overview",
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("node.read"), Params: nodeParams()}, Config: nodeDetailConfig()},
				{Key: "live", Label: "Live", Icon: icon("activity"), Type: plugin.PanelMetrics,
					Source: &plugin.DataSource{RouteID: rid("metrics"), Method: plugin.MethodWS, Params: nodeParams()}, Config: nodeMetricsConfig()},
				{Key: "shards", Label: "Shards", Icon: icon("database"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("node.shards"), Params: nodeParams()}, Config: plugin.TableConfig{
						Columns:           nodeShardColumns(),
						DefaultSort:       &plugin.SortKey{Field: "name"},
						RefreshIntervalMs: 15000,
						EmptyText:         "This node hosts no shards.",
						Exportable:        true,
					}},
			},
		},
	}
}

func aliasResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "alias", Title: "Aliases",
		List:    plugin.DataSource{RouteID: rid("aliases.list")},
		Columns: aliasColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{rid("alias.create")},
			Row:     []string{rid("alias.delete")},
			Detail:  []string{rid("alias.update"), rid("alias.delete")},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("tag"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("alias.read"), Params: aliasParams()}, Config: plugin.ObjectDetailConfig{
						Sections: []plugin.ObjectDetailSection{{Title: "Alias", Fields: []plugin.ObjectDetailField{
							{Key: "name", Label: "Alias", Copy: true},
							{Key: "collection", Label: "Target collection", Copy: true},
						}}},
						RawToggle: true,
					}},
			},
		},
	}
}

func backupResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "backup", Title: "Backups",
		List:    plugin.DataSource{RouteID: rid("backups.list")},
		Watch:   watchSource("backup", nil),
		Columns: backupColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{rid("backup.create")},
			Row:     []string{rid("backup.cancel")},
			Detail:  []string{rid("backup.restore"), rid("backup.cancel")},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}", StatusField: "status", Severities: backupSeverities},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("archive"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("backup.read"), Params: backupParams()}, Config: backupDetailConfig()},
			},
		},
	}
}

func savedQueryResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "saved_query", Title: "Saved queries",
		List:    plugin.DataSource{RouteID: rid("queries.list")},
		Columns: savedQueryColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{rid("query.save")},
			Row:     []string{rid("query.delete")},
			Detail:  []string{rid("query.delete")},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "editor", Label: "Query", Icon: icon("code"), Type: plugin.PanelCodeEditor,
					Source: &plugin.DataSource{RouteID: rid("query.read"), Params: savedQueryParams()},
					Config: savedQueryEditorConfig("graphql"),
					Variants: []plugin.PanelVariant{{
						Type:        plugin.PanelCodeEditor,
						Config:      savedQueryEditorConfig("json"),
						VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "kind", Op: plugin.OpEq, Value: "search"}}},
					}},
				},
			},
		},
	}
}

func savedQueryEditorConfig(language string) plugin.CodeEditorConfig {
	return plugin.CodeEditorConfig{
		Language:     language,
		SaveRouteID:  rid("query.update"),
		SaveMethod:   plugin.MethodPut,
		SaveParams:   savedQueryParams(),
		RefreshField: "content",
		SaveToast:    &plugin.SaveToast{Summary: "Saved query updated", Detail: "${response.name}", Severity: plugin.SeveritySuccess},
	}
}

func actions() []plugin.Action {
	return []plugin.Action{
		{ID: rid("collection.create"), Label: "Create collection", Icon: icon("plus"), RouteID: rid("collection.create"),
			OnSuccess: &plugin.ActionSuccess{SelectTab: "collections"}},
		{ID: rid("collection.delete"), Label: "Delete collection", Icon: icon("trash-2"), RouteID: rid("collection.delete"),
			Params: collectionParams(), Confirm: true, Bulk: true,
			ConfirmText: "Delete this collection with every object, vector, and shard it holds? This cannot be undone.",
			OnSuccess:   &plugin.ActionSuccess{Navigate: plugin.NavigateList}},
		{ID: rid("property.create"), Label: "Add property", Icon: icon("list-plus"), RouteID: rid("property.create"),
			Params: collectionParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "properties"}},

		{ID: rid("object.create"), Label: "Insert object", Icon: icon("plus"), RouteID: rid("object.create"),
			Params: collectionParams(), Open: plugin.OpenDialog, Panel: plugin.PanelCodeEditor,
			Config: plugin.CodeEditorConfig{
				Language:       "json",
				InitialContent: "{\n  \"title\": \"example\"\n}",
				SaveRouteID:    rid("object.create"),
				SaveMethod:     plugin.MethodPost,
				SaveParams:     collectionParams(),
				SaveBodyKey:    "properties",
				SaveDismiss:    plugin.SaveDismissClose,
				SaveToast:      &plugin.SaveToast{Summary: "Object inserted", Detail: "${response.id}", Severity: plugin.SeveritySuccess},
			},
			OnSuccess: &plugin.ActionSuccess{SelectTab: "objects"}},
		{ID: rid("object.delete"), Label: "Delete object", Icon: icon("trash"), RouteID: rid("object.delete"),
			Params: objectParams(), Confirm: true,
			ConfirmText: "Delete this object and its vectors? This cannot be undone.",
			OnSuccess:   &plugin.ActionSuccess{Navigate: plugin.NavigateList}},
		{ID: rid("objects.delete"), Label: "Delete by filter", Icon: icon("list-x"), RouteID: rid("objects.delete"),
			Params: collectionParams(), Confirm: true,
			ConfirmText: "Delete every object matching this filter, with its vectors? Run it as a dry run first to see how many match.",
			OnSuccess:   &plugin.ActionSuccess{SelectTab: "objects"}},

		{ID: rid("reference.create"), Label: "Add reference", Icon: icon("link"), RouteID: rid("reference.create"),
			Params: objectParams(), Group: "References", OnSuccess: &plugin.ActionSuccess{SelectTab: "references"}},
		{ID: rid("reference.delete"), Label: "Remove reference", Icon: icon("unlink"), RouteID: rid("reference.delete"),
			Params: objectParams(), Group: "References", Confirm: true,
			ConfirmText: "Remove this cross-reference? The target object is kept.",
			OnSuccess:   &plugin.ActionSuccess{SelectTab: "references"}},

		{ID: rid("shard.update"), Label: "Set shard status", Icon: icon("toggle-left"), RouteID: rid("shard.update"),
			Params:      map[string]string{"collection": "${resource.name}", "shard": "${record.name}"},
			Confirm:     true,
			ConfirmText: "Change this shard's status? READONLY stops accepting writes on this shard."},

		{ID: rid("tenant.create"), Label: "Create tenant", Icon: icon("user-plus"), RouteID: rid("tenant.create"),
			Params: collectionParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "tenants"}},
		{ID: rid("tenant.update"), Label: "Set tenant status", Icon: icon("toggle-left"), RouteID: rid("tenant.update"),
			Params: map[string]string{"collection": "${resource.name}", "tenant": "${record.name}"}, Confirm: true,
			ConfirmText: "Change this tenant's activity status? INACTIVE and OFFLOADED take its objects out of service."},
		{ID: rid("tenant.delete"), Label: "Delete tenant", Icon: icon("trash"), RouteID: rid("tenant.delete"),
			Params:  map[string]string{"collection": "${resource.name}", "tenant": "${record.name}"},
			Confirm: true, Bulk: true, ConfirmText: "Delete the selected tenant(s) and every object stored under them?"},

		{ID: rid("alias.create"), Label: "Create alias", Icon: icon("tag"), RouteID: rid("alias.create")},
		{ID: rid("alias.update"), Label: "Retarget alias", Icon: icon("shuffle"), RouteID: rid("alias.update"), Params: aliasParams()},
		{ID: rid("alias.delete"), Label: "Delete alias", Icon: icon("trash"), RouteID: rid("alias.delete"),
			Params: aliasParams(), Confirm: true, Bulk: true, ConfirmText: "Delete the selected alias(es)? The target collection is kept."},

		{ID: rid("backup.create"), Label: "Create backup", Icon: icon("archive"), RouteID: rid("backup.create"),
			OnSuccess: &plugin.ActionSuccess{Effects: []plugin.ActionEffect{{
				Type: plugin.ActionEffectOpenPanel,
				OpenPanel: &plugin.OpenPanelEffect{
					Open: plugin.OpenDialog, Panel: plugin.PanelObjectDetail,
					Title:  "Backup · ${response.id}",
					Icon:   icon("archive"),
					Source: &plugin.DataSource{RouteID: rid("backup.read"), Params: map[string]string{"backend": "${response.backend}", "backup": "${response.id}"}},
					Config: backupDetailConfig(),
				},
			}}}},
		{ID: rid("backup.restore"), Label: "Restore backup", Icon: icon("rotate-ccw"), RouteID: rid("backup.restore"),
			Params: backupParams(), Confirm: true,
			ConfirmText: "Restore this backup? Collections in the backup must not already exist on this cluster."},
		{ID: rid("backup.cancel"), Label: "Cancel backup", Icon: icon("circle-stop"), RouteID: rid("backup.cancel"),
			Params: backupParams(), Confirm: true, Bulk: true,
			ConfirmText: "Cancel the selected backup(s)? Partially written data is discarded."},

		{ID: rid("query.save"), Label: "Save query", Icon: icon("bookmark-plus"), RouteID: rid("query.save")},
		{ID: rid("query.delete"), Label: "Delete saved query", Icon: icon("trash"), RouteID: rid("query.delete"),
			Params: savedQueryParams(), Confirm: true, Bulk: true, ConfirmText: "Delete the selected saved query(s)?"},
	}
}

func serverDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		Sections: []plugin.ObjectDetailSection{
			{Title: "Server", Fields: []plugin.ObjectDetailField{
				{Key: "endpoint", Label: "Endpoint", Copy: true},
				{Key: "version", Label: "Version", Copy: true},
				{Key: "hostname", Label: "Hostname", Copy: true},
				{Key: "consistency", Label: "Consistency level", Type: plugin.ColumnBadge},
				{Key: "readOnly", Label: "Read-only connection", Type: plugin.ColumnBool},
			}},
			{Title: "Data", Fields: []plugin.ObjectDetailField{
				{Key: "collections", Label: "Collections", Type: plugin.ColumnNumber},
				{Key: "objects", Label: "Objects", Type: plugin.ColumnNumber},
				{Key: "shards", Label: "Shards", Type: plugin.ColumnNumber},
				{Key: "vectorizers", Label: "Vectorizers in use", Type: plugin.ColumnJSON},
			}},
			{Title: "Cluster", Fields: []plugin.ObjectDetailField{
				{Key: "nodesPct", Label: "Healthy nodes", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
					PercentKey: "nodesPct", UsedKey: "healthyNodes", TotalKey: "nodes",
					UsedType: plugin.ColumnNumber, TotalType: plugin.ColumnNumber, TotalLabel: "of", Unit: "node(s)",
					WarnAt: 99, CriticalAt: 60,
				}},
				{Key: "batchQueue", Label: "Batch queue", Type: plugin.ColumnNumber},
				{Key: "rate", Label: "Import rate", Type: plugin.ColumnNumber},
			}},
			{Title: "Modules", Fields: []plugin.ObjectDetailField{
				{Key: "moduleCount", Label: "Enabled modules", Type: plugin.ColumnNumber},
				{Key: "modules", Label: "Modules", Type: plugin.ColumnJSON},
			}},
		},
		RawToggle: true,
	}
}

func collectionDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		Sections: []plugin.ObjectDetailSection{
			{Title: "Identity", Fields: []plugin.ObjectDetailField{
				{Key: "name", Label: "Collection", Copy: true},
				{Key: "description", Label: "Description"},
				{Key: "status", Label: "Vector index status", Type: plugin.ColumnBadge, Severities: indexingSeverities},
				{Key: "objects", Label: "Objects", Type: plugin.ColumnNumber},
			}},
			{Title: "Vectors", Fields: []plugin.ObjectDetailField{
				{Key: "vectorizer", Label: "Vectorizer", Type: plugin.ColumnBadge},
				{Key: "namedVectors", Label: "Named vectors", Type: plugin.ColumnJSON},
				{Key: "vectorIndexType", Label: "Index type", Type: plugin.ColumnBadge},
				{Key: "distance", Label: "Distance metric", Type: plugin.ColumnBadge},
				{Key: "quantization", Label: "Quantization", Type: plugin.ColumnBadge},
				{Key: "ef", Label: "ef", Type: plugin.ColumnNumber},
				{Key: "efConstruction", Label: "efConstruction", Type: plugin.ColumnNumber},
				{Key: "maxConnections", Label: "maxConnections", Type: plugin.ColumnNumber},
			}},
			{Title: "Schema", Fields: []plugin.ObjectDetailField{
				{Key: "properties", Label: "Properties", Type: plugin.ColumnNumber},
				{Key: "references", Label: "Cross-references", Type: plugin.ColumnNumber},
				{Key: "moduleConfig", Label: "Module config", Type: plugin.ColumnJSON},
				{Key: "invertedIndex", Label: "Inverted index", Type: plugin.ColumnJSON},
			}},
			{Title: "Distribution", Fields: []plugin.ObjectDetailField{
				{Key: "shards", Label: "Shards", Type: plugin.ColumnNumber},
				{Key: "shardingDesired", Label: "Desired shard count", Type: plugin.ColumnNumber},
				{Key: "replication", Label: "Replication factor", Type: plugin.ColumnNumber},
				{Key: "asyncReplication", Label: "Async replication", Type: plugin.ColumnBool},
				{Key: "multiTenancy", Label: "Multi-tenancy", Type: plugin.ColumnBool},
				{Key: "autoTenantCreation", Label: "Auto-create tenants", Type: plugin.ColumnBool},
				{Key: "autoTenantActivation", Label: "Auto-activate tenants", Type: plugin.ColumnBool},
			}},
		},
		RawToggle: true,
	}
}

func objectDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		Sections: []plugin.ObjectDetailSection{
			{Title: "Identity", Fields: []plugin.ObjectDetailField{
				{Key: "id", Label: "UUID", Copy: true},
				{Key: "collection", Label: "Collection", Copy: true},
				{Key: "tenant", Label: "Tenant"},
				{Key: "created", Label: "Created", Type: plugin.ColumnDateTime},
				{Key: "updated", Label: "Updated", Type: plugin.ColumnRelativeTime},
			}},
			{Title: "Vector", Fields: []plugin.ObjectDetailField{
				{Key: "vectorDims", Label: "Dimensions", Type: plugin.ColumnNumber},
				{Key: "vectorNorm", Label: "L2 norm", Type: plugin.ColumnNumber},
				{Key: "namedVectors", Label: "Named vectors", Type: plugin.ColumnJSON},
			}},
			{Title: "Properties", Fields: []plugin.ObjectDetailField{
				{Key: "propertyCount", Label: "Property count", Type: plugin.ColumnNumber},
				{Key: "properties", Label: "Properties", Type: plugin.ColumnJSON},
			}},
		},
		RawToggle: true,
	}
}

func nodeDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		Sections: []plugin.ObjectDetailSection{
			{Title: "Node", Fields: []plugin.ObjectDetailField{
				{Key: "name", Label: "Name", Copy: true},
				{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: nodeSeverities},
				{Key: "version", Label: "Version"},
				{Key: "gitHash", Label: "Git hash", Copy: true},
				{Key: "operationalMode", Label: "Operational mode", Type: plugin.ColumnBadge},
			}},
			{Title: "Load", Fields: []plugin.ObjectDetailField{
				{Key: "objects", Label: "Objects", Type: plugin.ColumnNumber},
				{Key: "shards", Label: "Shards", Type: plugin.ColumnNumber},
				{Key: "batchQueue", Label: "Batch queue", Type: plugin.ColumnNumber},
				{Key: "rate", Label: "Import rate", Type: plugin.ColumnNumber},
				{Key: "classes", Label: "Collections", Type: plugin.ColumnJSON},
			}},
		},
		RawToggle: true,
	}
}

func backupDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		Sections: []plugin.ObjectDetailSection{
			{Title: "Backup", Fields: []plugin.ObjectDetailField{
				{Key: "id", Label: "Backup ID", Copy: true},
				{Key: "backend", Label: "Backend", Type: plugin.ColumnBadge},
				{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: backupSeverities},
				{Key: "path", Label: "Path", Copy: true},
				{Key: "size", Label: "Size", Type: plugin.ColumnBytes},
			}},
			{Title: "Timing", Fields: []plugin.ObjectDetailField{
				{Key: "startedAt", Label: "Started", Type: plugin.ColumnDateTime},
				{Key: "completedAt", Label: "Completed", Type: plugin.ColumnDateTime},
				{Key: "error", Label: "Error"},
			}},
			{Title: "Restore", Fields: []plugin.ObjectDetailField{
				{Key: "restoreStatus", Label: "Restore status", Type: plugin.ColumnBadge, Severities: backupSeverities},
				{Key: "restoreError", Label: "Restore error"},
			}},
		},
		RawToggle: true,
	}
}

func clusterMetricsConfig() plugin.MetricsConfig {
	return plugin.MetricsConfig{
		Stats: []plugin.MetricStat{
			{Key: "collections", Label: "Collections"},
			{Key: "objects", Label: "Objects"},
			{Key: "shards", Label: "Shards"},
			{Key: "rate", Label: "Import rate", Unit: "obj/s"},
		},
		Usage: []plugin.MetricUsage{{
			Key: "nodesPct", Label: "Healthy nodes", Type: plugin.ColumnPercent,
			Usage: &plugin.UsageSpec{
				PercentKey: "nodesPct", UsedKey: "healthyNodes", TotalKey: "nodes",
				UsedType: plugin.ColumnNumber, TotalType: plugin.ColumnNumber, TotalLabel: "of", Unit: "node(s)",
				WarnAt: 99, CriticalAt: 60,
			},
		}},
		Series:  []plugin.MetricSeries{{Key: "objects", Label: "Objects"}, {Key: "batchQueue", Label: "Batch queue"}, {Key: "rate", Label: "Import rate", Unit: "obj/s"}},
		History: 60,
	}
}

func nodeMetricsConfig() plugin.MetricsConfig {
	return plugin.MetricsConfig{
		Stats: []plugin.MetricStat{
			{Key: "objects", Label: "Objects"},
			{Key: "shards", Label: "Shards"},
			{Key: "collections", Label: "Collections"},
			{Key: "rate", Label: "Import rate", Unit: "obj/s"},
		},
		Series:  []plugin.MetricSeries{{Key: "objects", Label: "Objects"}, {Key: "batchQueue", Label: "Batch queue"}},
		History: 60,
	}
}

func graphqlEditorConfig() plugin.QueryEditorConfig {
	return plugin.QueryEditorConfig{
		Language:          "graphql",
		Label:             "GraphQL",
		ExecuteLabel:      "Run",
		RunningLabel:      "Running…",
		EmptyText:         "Run a Weaviate GraphQL query to see results. Get, Aggregate, and Explore are supported.",
		InitialQuery:      "{\n  Aggregate {\n    # replace with a collection name\n  }\n}",
		CompletionRouteID: rid("graphql.complete"),
		Exportable:        true,
	}
}

func searchEditorConfig() plugin.QueryEditorConfig {
	return plugin.QueryEditorConfig{
		Language:          "json",
		Label:             "Vector search",
		ExecuteLabel:      "Search",
		RunningLabel:      "Searching…",
		EmptyText:         "Describe a search: mode is fetch, nearVector, nearText, hybrid, or bm25. Add where, sort, properties, limit, and target_vector as needed.",
		InitialQuery:      "{\n  \"mode\": \"hybrid\",\n  \"query\": \"example\",\n  \"alpha\": 0.5,\n  \"limit\": 10\n}",
		CompletionRouteID: rid("graphql.complete"),
		CompletionParams:  collectionParams(),
		Exportable:        true,
	}
}

func embeddingCanvasConfig() plugin.CanvasConfig {
	return plugin.CanvasConfig{
		ScaleMode:      plugin.CanvasScaleResize,
		Interactive:    true,
		Pointer:        true,
		Keyboard:       true,
		ResizeEvents:   true,
		WheelMode:      plugin.CanvasWheelModified,
		HiDPI:          true,
		FocusOnPointer: true,
		AriaLabel:      "Two-dimensional PCA projection of this collection's vectors",
		Instructions:   "Hover or click a point to inspect an object. Arrow keys move the selection, R reloads the sample, plus and minus zoom, 0 resets.",
	}
}

func objectsTableConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Columns:       objectColumns(),
		ColumnsSource: &plugin.DataSource{RouteID: rid("collection.columns"), Params: collectionParams()},
		Watch:         watchSource("object", collectionParams()),
		Editable:      true,
		StagedEdits:   true,
		RowKey:        []string{"id"},
		Insert:        &plugin.DataSource{RouteID: rid("object.create"), Method: plugin.MethodPost, Params: collectionParams()},
		Update:        &plugin.DataSource{RouteID: rid("object.update"), Method: plugin.MethodPut, Params: map[string]string{"collection": "${resource.name}", "id": "${record.id}"}},
		Delete:        &plugin.DataSource{RouteID: rid("object.delete"), Method: plugin.MethodDelete, Params: map[string]string{"collection": "${resource.name}", "id": "${record.id}"}},
		ActionIDs:     []string{rid("object.create"), rid("objects.delete")},
		HiddenColumns: []string{"ref"},
		EmptyText:     "No objects yet. Insert one or import a batch to populate this collection.",
		Exportable:    true,
		RowClick:      plugin.RowClickNavigate,
	}
}

func collectionColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Collection", Sortable: true},
		{Key: "status", Label: "Index", Type: plugin.ColumnBadge, Sortable: true, Severities: indexingSeverities},
		{Key: "objects", Label: "Objects", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "vectorizer", Label: "Vectorizer", Type: plugin.ColumnBadge, Sortable: true},
		{Key: "vectorIndexType", Label: "Index type", Sortable: true},
		{Key: "distance", Label: "Distance", Sortable: true},
		{Key: "properties", Label: "Properties", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "references", Label: "Refs", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "shards", Label: "Shards", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "multiTenancy", Label: "Multi-tenant", Type: plugin.ColumnBool, Sortable: true},
	}
}

func propertyColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Property", Sortable: true},
		{Key: "dataType", Label: "Data type", Sortable: true},
		{Key: "kind", Label: "Kind", Type: plugin.ColumnBadge, Sortable: true, Severities: map[string]plugin.Severity{
			"primitive": plugin.SeveritySecondary, "reference": plugin.SeverityInfo,
		}},
		{Key: "tokenization", Label: "Tokenization", Sortable: true},
		{Key: "indexFilterable", Label: "Filterable", Type: plugin.ColumnBool, Sortable: true},
		{Key: "indexSearchable", Label: "Searchable", Type: plugin.ColumnBool, Sortable: true},
		{Key: "indexRangeFilters", Label: "Range", Type: plugin.ColumnBool},
		{Key: "description", Label: "Description"},
	}
}

func objectColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "id", Label: "ID", ReadOnly: true},
		{Key: "_creationTime", Label: "Created", Type: plugin.ColumnDateTime, ReadOnly: true},
		{Key: "_updateTime", Label: "Updated", Type: plugin.ColumnRelativeTime, ReadOnly: true},
	}
}

func moduleColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Module", Sortable: true},
		{Key: "kind", Label: "Kind", Type: plugin.ColumnBadge, Sortable: true, Severities: moduleSeverities},
		{Key: "title", Label: "Title"},
		{Key: "documentation", Label: "Documentation"},
		{Key: "config", Label: "Config", Type: plugin.ColumnJSON},
	}
}

func shardColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Shard", Sortable: true},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Sortable: true, Severities: shardSeverities},
		{Key: "node", Label: "Node", Sortable: true},
		{Key: "objects", Label: "Objects", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "vectorQueueSize", Label: "Vector queue", Type: plugin.ColumnNumber, Sortable: true},
	}
}

func nodeShardColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Shard", Sortable: true},
		{Key: "collection", Label: "Collection", Sortable: true},
		{Key: "objects", Label: "Objects", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "vectorIndexingStatus", Label: "Indexing", Type: plugin.ColumnBadge, Sortable: true, Severities: indexingSeverities},
		{Key: "compressed", Label: "Compressed", Type: plugin.ColumnBool},
		{Key: "loaded", Label: "Loaded", Type: plugin.ColumnBool},
		{Key: "replicationFactor", Label: "Replication", Type: plugin.ColumnNumber},
	}
}

func nodeColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Node", Sortable: true},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Sortable: true, Severities: nodeSeverities},
		{Key: "version", Label: "Version", Sortable: true},
		{Key: "objects", Label: "Objects", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "shards", Label: "Shards", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "collections", Label: "Collections", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "rate", Label: "Import rate", Type: plugin.ColumnNumber},
	}
}

func tenantColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Tenant", Sortable: true},
		{Key: "activityStatus", Label: "Activity", Type: plugin.ColumnBadge, Sortable: true, Severities: tenantSeverities},
		{Key: "collection", Label: "Collection", Sortable: true},
	}
}

func aliasColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Alias", Sortable: true},
		{Key: "collection", Label: "Target collection", Sortable: true},
	}
}

func backupColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "id", Label: "Backup", Sortable: true},
		{Key: "backend", Label: "Backend", Type: plugin.ColumnBadge, Sortable: true},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Sortable: true, Severities: backupSeverities},
		{Key: "startedAt", Label: "Started", Type: plugin.ColumnDateTime, Sortable: true},
		{Key: "completedAt", Label: "Completed", Type: plugin.ColumnRelativeTime, Sortable: true},
		{Key: "size", Label: "Size", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "collections", Label: "Collections", Type: plugin.ColumnJSON},
	}
}

func savedQueryColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Name", Sortable: true},
		{Key: "kind", Label: "Kind", Type: plugin.ColumnBadge, Sortable: true, Severities: map[string]plugin.Severity{
			"graphql": plugin.SeverityInfo, "search": plugin.SeveritySuccess,
		}},
		{Key: "collection", Label: "Collection", Sortable: true},
		{Key: "updated", Label: "Updated", Type: plugin.ColumnRelativeTime, Sortable: true},
	}
}
