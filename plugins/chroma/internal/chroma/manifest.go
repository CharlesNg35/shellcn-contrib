package chroma

import "github.com/charlesng35/shellcn/sdk/plugin"

func icon(name string) plugin.Icon { return plugin.Icon{Type: plugin.IconLucide, Value: name} }

func rid(suffix string) string { return protocolName + "." + suffix }

func serverRef() plugin.ResourceIdentity {
	return plugin.ResourceIdentity{Kind: "server", Name: "Chroma", UID: "server"}
}

var (
	spaceSeverities = map[string]plugin.Severity{
		"cosine": plugin.SeverityInfo,
		"l2":     plugin.SeveritySecondary,
		"ip":     plugin.SeveritySuccess,
	}
	indexSeverities = map[string]plugin.Severity{
		"hnsw":    plugin.SeverityInfo,
		"spann":   plugin.SeveritySuccess,
		"unknown": plugin.SeveritySecondary,
	}
	stateSeverities = map[string]plugin.Severity{
		"enabled":  plugin.SeveritySuccess,
		"disabled": plugin.SeveritySecondary,
	}
	statusSeverities = map[string]plugin.Severity{
		"ready":       plugin.SeveritySuccess,
		"degraded":    plugin.SeverityWarn,
		"unreachable": plugin.SeverityDanger,
	}
)

func streams() []plugin.Stream {
	return []plugin.Stream{
		{ID: rid("server.metrics"), Kind: plugin.StreamMetrics, RouteID: rid("server.metrics")},
		{ID: rid("collection.metrics"), Kind: plugin.StreamMetrics, RouteID: rid("collection.metrics")},
		{ID: rid("query"), Kind: plugin.StreamQuery, RouteID: rid("query")},
		{ID: rid("projection"), Kind: plugin.StreamCanvas, RouteID: rid("projection")},
		{ID: rid("spaces"), Kind: plugin.StreamCanvas, RouteID: rid("spaces")},
	}
}

func tree() []plugin.TreeGroup {
	ref := serverRef()
	return []plugin.TreeGroup{
		{Key: "server", Label: "Server", Icon: icon("layout-dashboard"), Ref: &ref},
		{Key: "tenants", Label: "Tenants", Icon: icon("building-2"), Source: plugin.DataSource{RouteID: rid("tenants.tree")}},
	}
}

func collectionParams() map[string]string {
	return map[string]string{
		"tenant":     "${resource.scope}",
		"database":   "${resource.namespace}",
		"collection": "${resource.uid}",
	}
}

func recordParams() map[string]string {
	return map[string]string{
		"scope":      "${resource.scope}",
		"collection": "${resource.namespace}",
		"record":     "${resource.name}",
	}
}

func recordScopeParams() map[string]string {
	return map[string]string{
		"scope":      "${resource.scope}",
		"collection": "${resource.namespace}",
	}
}

func databaseRecordParams() map[string]string {
	return map[string]string{"tenant": "${record.tenant}", "database": "${record.name}"}
}

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		{
			Kind: "server", Title: "Chroma server",
			List:    plugin.DataSource{RouteID: rid("overview")},
			Columns: serverColumns(),
			Actions: plugin.ResourceActions{
				Detail: []string{rid("tenant.create"), rid("database.create"), rid("collection.create")},
			},
			Detail: plugin.DetailView{
				Header:     plugin.HeaderSpec{Title: "${resource.name}", StatusField: "status", Severities: statusSeverities},
				DefaultTab: "overview",
				Tabs: []plugin.Panel{
					{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
						Source: &plugin.DataSource{RouteID: rid("overview")}, Config: serverDetailConfig()},
					{Key: "metrics", Label: "Metrics", Icon: icon("activity"), Type: plugin.PanelMetrics,
						Source: &plugin.DataSource{RouteID: rid("server.metrics"), Method: plugin.MethodWS}, Config: serverMetricsConfig()},
					{Key: "databases", Label: "Databases", Icon: icon("database"), Type: plugin.PanelTable,
						Source: &plugin.DataSource{RouteID: rid("databases.list")}, Config: databaseTableConfig()},
					{Key: "collections", Label: "Collections", Icon: icon("layers"), Type: plugin.PanelTable,
						Source: &plugin.DataSource{RouteID: rid("collections.list")}, Config: collectionTableConfig()},
					{Key: "tenant", Label: "Tenant", Icon: icon("building-2"), Type: plugin.PanelObjectDetail,
						Source: &plugin.DataSource{RouteID: rid("tenant.read")}, Config: tenantDetailConfig()},
					{Key: "saved", Label: "Saved queries", Icon: icon("bookmark"), Type: plugin.PanelTable,
						Source: &plugin.DataSource{RouteID: rid("queries.list")}, Config: savedQueryTableConfig()},
				},
			},
		},
		{
			Kind: "collection", Title: "Collections",
			List:    plugin.DataSource{RouteID: rid("collections.list")},
			Columns: collectionColumns(),
			Actions: plugin.ResourceActions{
				Toolbar: []string{rid("collection.create")},
				Row:     []string{rid("collection.delete")},
				Detail: []string{
					rid("records.add"), rid("collection.update"), rid("query.save"),
					rid("history.clear"), rid("collection.delete"),
				},
			},
			Detail: plugin.DetailView{
				Header:     plugin.HeaderSpec{Title: "${resource.name}", StatusField: "space", Severities: spaceSeverities},
				DefaultTab: "overview",
				Tabs: []plugin.Panel{
					{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
						Source: &plugin.DataSource{RouteID: rid("collection.read"), Params: collectionParams()}, Config: collectionDetailConfig()},
					{Key: "records", Label: "Records", Icon: icon("table"), Type: plugin.PanelTable,
						Source: &plugin.DataSource{RouteID: rid("records.list"), Params: collectionParams()}, Config: recordTableConfig()},
					{Key: "query", Label: "Search", Icon: icon("search"), Type: plugin.PanelQueryEditor,
						Source: &plugin.DataSource{RouteID: rid("query"), Method: plugin.MethodWS, Params: collectionParams()}, Config: queryConfig()},
					{Key: "map", Label: "Embedding map", Icon: icon("scatter-chart"), Type: plugin.PanelCanvas,
						Source: &plugin.DataSource{RouteID: rid("projection"), Method: plugin.MethodWS, Params: collectionParams()}, Config: projectionConfig()},
					{Key: "spaces", Label: "Distance spaces", Icon: icon("git-compare"), Type: plugin.PanelCanvas,
						Source: &plugin.DataSource{RouteID: rid("spaces"), Method: plugin.MethodWS, Params: collectionParams()}, Config: spacesConfig()},
					{Key: "schema", Label: "Schema", Icon: icon("list-tree"), Type: plugin.PanelTable,
						Source: &plugin.DataSource{RouteID: rid("collection.schema"), Params: collectionParams()}, Config: schemaTableConfig()},
					{Key: "metrics", Label: "Metrics", Icon: icon("activity"), Type: plugin.PanelMetrics,
						Source: &plugin.DataSource{RouteID: rid("collection.metrics"), Method: plugin.MethodWS, Params: collectionParams()}, Config: collectionMetricsConfig()},
					{Key: "history", Label: "History", Icon: icon("history"), Type: plugin.PanelTimeline,
						Source: &plugin.DataSource{RouteID: rid("history.list"), Params: collectionParams()}, Config: historyConfig()},
				},
			},
		},
		{
			Kind: "record", Title: "Records",
			List:    plugin.DataSource{RouteID: rid("records.list")},
			Columns: recordColumns(),
			Actions: plugin.ResourceActions{
				Row:    []string{rid("record.delete")},
				Detail: []string{rid("record.delete")},
			},
			Detail: plugin.DetailView{
				Header:     plugin.HeaderSpec{Title: "${resource.name}"},
				DefaultTab: "overview",
				Tabs: []plugin.Panel{
					{Key: "overview", Label: "Record", Icon: icon("file-text"), Type: plugin.PanelObjectDetail,
						Source: &plugin.DataSource{RouteID: rid("record.read"), Params: recordParams()}, Config: recordDetailConfig()},
					{Key: "neighbors", Label: "Nearest neighbors", Icon: icon("radar"), Type: plugin.PanelTable,
						Source: &plugin.DataSource{RouteID: rid("record.neighbors"), Params: recordParams()}, Config: neighborTableConfig()},
				},
			},
		},
	}
}

func actions() []plugin.Action {
	return []plugin.Action{
		{ID: rid("tenant.create"), Label: "Create tenant", Icon: icon("building"), RouteID: rid("tenant.create"), Group: "Create",
			OnSuccess: &plugin.ActionSuccess{SelectTab: "tenant"}},
		{ID: rid("database.create"), Label: "Create database", Icon: icon("database"), RouteID: rid("database.create"), Group: "Create",
			OnSuccess: &plugin.ActionSuccess{SelectTab: "databases"}},
		{ID: rid("database.delete"), Label: "Delete database", Icon: icon("trash-2"), RouteID: rid("database.delete"),
			Params: databaseRecordParams(), Confirm: true, Bulk: true,
			ConfirmText: "Delete this database and every collection inside it? Vectors, documents, and metadata are removed permanently."},
		{ID: rid("collection.create"), Label: "Create collection", Icon: icon("plus"), RouteID: rid("collection.create"), Group: "Create",
			OnSuccess: &plugin.ActionSuccess{SelectTab: "collections"}},
		{ID: rid("collection.update"), Label: "Edit collection", Icon: icon("pencil"), RouteID: rid("collection.update"), Params: collectionParams(),
			OnSuccess: &plugin.ActionSuccess{SelectTab: "overview"}},
		{ID: rid("collection.delete"), Label: "Delete", Icon: icon("trash-2"), RouteID: rid("collection.delete"), Params: collectionParams(),
			Confirm: true, Bulk: true, ConfirmText: "Delete this collection and every record in it? Embeddings cannot be recovered.",
			OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList}},
		{ID: rid("records.add"), Label: "Add records", Icon: icon("file-plus"), RouteID: rid("records.add"), Params: collectionParams(),
			Open: plugin.OpenDialog, Panel: plugin.PanelCodeEditor, Config: recordEditorConfig(),
			OnSuccess: &plugin.ActionSuccess{SelectTab: "records"}},
		{ID: rid("record.delete"), Label: "Delete", Icon: icon("trash"), RouteID: rid("records.delete"),
			Params: recordScopeParams(), Body: map[string]any{"id": "${resource.name}"},
			Confirm: true, Bulk: true, ConfirmText: "Delete the selected record(s) from this collection?"},
		{ID: rid("query.save"), Label: "Save query", Icon: icon("bookmark-plus"), RouteID: rid("queries.save"), Group: "Queries",
			OnSuccess: &plugin.ActionSuccess{SelectTab: "query"}},
		{ID: rid("query.delete"), Label: "Delete", Icon: icon("trash"), RouteID: rid("queries.delete"),
			Params: map[string]string{"id": "${record.id}"}, Confirm: true, Bulk: true,
			ConfirmText: "Delete this saved query?"},
		{ID: rid("history.clear"), Label: "Clear history", Icon: icon("eraser"), RouteID: rid("history.clear"), Params: collectionParams(),
			Confirm: true, ConfirmText: "Delete the recorded search history for this collection?", Group: "Queries",
			OnSuccess: &plugin.ActionSuccess{SelectTab: "history"}},
	}
}

func serverColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "endpoint", Label: "Endpoint", Sortable: true},
		{Key: "version", Label: "Version", Sortable: true},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Sortable: true, Severities: statusSeverities},
		{Key: "collections", Label: "Collections", Type: plugin.ColumnNumber, Sortable: true},
	}
}

func collectionColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Collection", Sortable: true},
		{Key: "records", Label: "Records", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "dimension", Label: "Dimensions", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "space", Label: "Space", Type: plugin.ColumnBadge, Sortable: true, Severities: spaceSeverities},
		{Key: "index", Label: "Index", Type: plugin.ColumnBadge, Sortable: true, Severities: indexSeverities},
		{Key: "database", Label: "Database", Sortable: true},
		{Key: "version", Label: "Version", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "metadata", Label: "Metadata", Type: plugin.ColumnJSON},
	}
}

func recordColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "id", Label: "ID", ReadOnly: true},
		{Key: "document", Label: "Document", Editable: true, Editor: plugin.ColumnEditorTextarea},
		{Key: "metadata", Label: "Metadata", Type: plugin.ColumnJSON, Editable: true, Editor: plugin.ColumnEditorJSON, Nullable: true},
		{Key: "uri", Label: "URI", ReadOnly: true},
	}
}

func neighborColumns() []plugin.Column {
	precision := 5
	return []plugin.Column{
		{Key: "rank", Label: "Rank", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "id", Label: "ID", Sortable: true},
		{Key: "distance", Label: "Distance", Type: plugin.ColumnNumber, Sortable: true, Precision: &precision},
		{Key: "similarity", Label: "Similarity", Type: plugin.ColumnNumber, Sortable: true, Precision: &precision},
		{Key: "document", Label: "Document"},
		{Key: "metadata", Label: "Metadata", Type: plugin.ColumnJSON},
	}
}

func databaseColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Database", Sortable: true},
		{Key: "collections", Label: "Collections", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "id", Label: "ID"},
	}
}

func schemaColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "key", Label: "Key", Sortable: true},
		{Key: "type", Label: "Value type", Type: plugin.ColumnBadge, Sortable: true},
		{Key: "index", Label: "Index", Sortable: true},
		{Key: "state", Label: "State", Type: plugin.ColumnBadge, Sortable: true, Severities: stateSeverities},
		{Key: "space", Label: "Space", Type: plugin.ColumnBadge, Severities: spaceSeverities},
		{Key: "source", Label: "Source key"},
		{Key: "config", Label: "Config", Type: plugin.ColumnJSON},
	}
}

func savedQueryColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Name", Sortable: true},
		{Key: "collection", Label: "Collection", Sortable: true},
		{Key: "query", Label: "Query", Type: plugin.ColumnJSON},
		{Key: "updated_at", Label: "Updated", Type: plugin.ColumnRelativeTime, Sortable: true},
	}
}

func collectionTableConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Columns:           collectionColumns(),
		ActionIDs:         []string{rid("collection.create")},
		RowActionIDs:      []string{rid("collection.delete")},
		DefaultSort:       &plugin.SortKey{Field: "name"},
		RefreshIntervalMs: 15000,
		EmptyText:         "No collections in this database yet. Create one to start storing embeddings.",
		Exportable:        true,
	}
}

func databaseTableConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Columns:           databaseColumns(),
		ActionIDs:         []string{rid("database.create")},
		RowActionIDs:      []string{rid("database.delete")},
		DefaultSort:       &plugin.SortKey{Field: "name"},
		RefreshIntervalMs: 30000,
		EmptyText:         "This tenant has no databases.",
		Exportable:        true,
	}
}

func recordTableConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Columns:      recordColumns(),
		Editable:     true,
		StagedEdits:  true,
		RowKey:       []string{"id"},
		Update:       &plugin.DataSource{RouteID: rid("record.update"), Method: plugin.MethodPatch, Params: collectionParams()},
		Delete:       &plugin.DataSource{RouteID: rid("records.delete"), Method: plugin.MethodDelete, Params: collectionParams()},
		ActionIDs:    []string{rid("records.add")},
		RowActionIDs: []string{rid("record.delete")},
		EmptyText:    "No records match. Chroma returns records in server order; use the search box to filter by document text.",
		Exportable:   true,
		RowClick:     plugin.RowClickNavigate,
	}
}

func neighborTableConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Columns:           neighborColumns(),
		DefaultSort:       &plugin.SortKey{Field: "rank"},
		RefreshIntervalMs: 30000,
		EmptyText:         "No other records with embeddings to compare against.",
		Exportable:        true,
	}
}

func schemaTableConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Columns:           schemaColumns(),
		DefaultSort:       &plugin.SortKey{Field: "key"},
		RefreshIntervalMs: 60000,
		EmptyText:         "This Chroma server did not report an index schema for the collection.",
		Exportable:        true,
	}
}

func savedQueryTableConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Columns:           savedQueryColumns(),
		ActionIDs:         []string{rid("query.save")},
		RowActionIDs:      []string{rid("query.delete")},
		DefaultSort:       &plugin.SortKey{Field: "name"},
		RefreshIntervalMs: 60000,
		EmptyText:         "No saved queries yet. Save a search to reuse it on any Chroma connection.",
	}
}

func serverDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Sections: []plugin.ObjectDetailSection{
			{Title: "Server", Fields: []plugin.ObjectDetailField{
				{Key: "endpoint", Label: "Endpoint", Copy: true},
				{Key: "version", Label: "Version", Copy: true},
				{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: statusSeverities},
				{Key: "executorReady", Label: "Executor ready", Type: plugin.ColumnBool},
				{Key: "logClientReady", Label: "Log client ready", Type: plugin.ColumnBool},
				{Key: "heartbeat", Label: "Heartbeat (ns)", Type: plugin.ColumnNumber},
			}},
			{Title: "Scope", Fields: []plugin.ObjectDetailField{
				{Key: "tenant", Label: "Tenant", Copy: true},
				{Key: "database", Label: "Database", Copy: true},
				{Key: "identityTenant", Label: "Authenticated tenant"},
				{Key: "userId", Label: "User"},
				{Key: "databases", Label: "Databases", Type: plugin.ColumnJSON},
				{Key: "collections", Label: "Collections", Type: plugin.ColumnNumber},
			}},
			{Title: "Limits", Fields: []plugin.ObjectDetailField{
				{Key: "maxBatchSize", Label: "Max batch size", Type: plugin.ColumnNumber},
				{Key: "base64Embeddings", Label: "Base64 embeddings", Type: plugin.ColumnBool},
				{Key: "readOnly", Label: "Read-only connection", Type: plugin.ColumnBool},
			}},
		},
	}
}

func tenantDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Sections: []plugin.ObjectDetailSection{{Title: "Tenant", Fields: []plugin.ObjectDetailField{
			{Key: "name", Label: "Name", Copy: true},
			{Key: "resource_name", Label: "Resource name"},
			{Key: "database_count", Label: "Databases", Type: plugin.ColumnNumber},
			{Key: "databases", Label: "Database names", Type: plugin.ColumnJSON},
		}}},
	}
}

func collectionDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Sections: []plugin.ObjectDetailSection{
			{Title: "Identity", Fields: []plugin.ObjectDetailField{
				{Key: "name", Label: "Name", Copy: true},
				{Key: "id", Label: "Collection ID", Copy: true},
				{Key: "tenant", Label: "Tenant"},
				{Key: "database", Label: "Database"},
				{Key: "version", Label: "Version", Type: plugin.ColumnNumber},
				{Key: "log_position", Label: "Log position", Type: plugin.ColumnNumber},
			}},
			{Title: "Vectors", Fields: []plugin.ObjectDetailField{
				{Key: "records", Label: "Records", Type: plugin.ColumnNumber},
				{Key: "dimension", Label: "Dimensions", Type: plugin.ColumnNumber},
				{Key: "space", Label: "Distance space", Type: plugin.ColumnBadge, Severities: spaceSeverities},
				{Key: "index", Label: "Index", Type: plugin.ColumnBadge, Severities: indexSeverities},
				{Key: "estimated_vector_bytes", Label: "Vector payload", Type: plugin.ColumnBytes},
				{Key: "embedded", Label: "Server-side embedding function", Type: plugin.ColumnBool},
			}},
			{Title: "Index tuning", Fields: []plugin.ObjectDetailField{
				{Key: "ef_construction", Label: "efConstruction", Type: plugin.ColumnNumber},
				{Key: "ef_search", Label: "efSearch", Type: plugin.ColumnNumber},
				{Key: "max_neighbors", Label: "Max neighbors", Type: plugin.ColumnNumber},
				{Key: "sync_threshold", Label: "Sync threshold", Type: plugin.ColumnNumber},
			}},
			{Title: "Metadata", Fields: []plugin.ObjectDetailField{
				{Key: "metadata", Label: "Metadata", Type: plugin.ColumnJSON},
				{Key: "embedding_function", Label: "Embedding function", Type: plugin.ColumnJSON},
			}},
		},
	}
}

func recordDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Sections: []plugin.ObjectDetailSection{
			{Title: "Record", Fields: []plugin.ObjectDetailField{
				{Key: "id", Label: "ID", Copy: true},
				{Key: "collection", Label: "Collection"},
				{Key: "uri", Label: "URI", Copy: true},
				{Key: "document_length", Label: "Document length", Type: plugin.ColumnNumber},
				{Key: "document", Label: "Document"},
			}},
			{Title: "Embedding", Fields: []plugin.ObjectDetailField{
				{Key: "dimension", Label: "Dimensions", Type: plugin.ColumnNumber},
				{Key: "norm", Label: "L2 norm", Type: plugin.ColumnNumber},
				{Key: "space", Label: "Distance space", Type: plugin.ColumnBadge, Severities: spaceSeverities},
				{Key: "embedding", Label: "Vector", Type: plugin.ColumnJSON},
			}},
			{Title: "Metadata", Fields: []plugin.ObjectDetailField{
				{Key: "metadata_keys", Label: "Keys", Type: plugin.ColumnJSON},
				{Key: "metadata", Label: "Metadata", Type: plugin.ColumnJSON},
			}},
		},
	}
}

func serverMetricsConfig() plugin.MetricsConfig {
	return plugin.MetricsConfig{
		Stats: []plugin.MetricStat{
			{Key: "collections", Label: "Collections"},
			{Key: "databases", Label: "Databases"},
			{Key: "records", Label: "Records"},
			{Key: "vectorBytes", Label: "Vector payload", Unit: "bytes"},
			{Key: "maxBatchSize", Label: "Max batch size"},
			{Key: "maxDimension", Label: "Largest dimension"},
		},
		Series: []plugin.MetricSeries{
			{Key: "heartbeatMs", Label: "Heartbeat", Unit: "ms"},
			{Key: "records", Label: "Records"},
		},
		History: 90,
	}
}

func collectionMetricsConfig() plugin.MetricsConfig {
	return plugin.MetricsConfig{
		Stats: []plugin.MetricStat{
			{Key: "records", Label: "Records"},
			{Key: "dimension", Label: "Dimensions"},
			{Key: "vectorBytes", Label: "Vector payload", Unit: "bytes"},
			{Key: "version", Label: "Version"},
			{Key: "logPosition", Label: "Log position"},
			{Key: "efSearch", Label: "efSearch"},
		},
		Usage: []plugin.MetricUsage{{
			Key: "indexPct", Label: "Index catch-up", Type: plugin.ColumnPercent,
			Usage: &plugin.UsageSpec{
				PercentKey: "indexPct", UsedKey: "indexedOps", TotalKey: "totalOps",
				UsedType: plugin.ColumnNumber, TotalType: plugin.ColumnNumber,
				TotalLabel: "of", Unit: "ops", WarnAt: 90, CriticalAt: 99,
			},
		}},
		Series: []plugin.MetricSeries{
			{Key: "records", Label: "Records"},
			{Key: "logPosition", Label: "Log position"},
		},
		History: 90,
	}
}

func historyConfig() plugin.TimelineConfig {
	return plugin.TimelineConfig{
		TimestampField:    "time",
		TitleField:        "title",
		BodyField:         "message",
		SeverityField:     "severity",
		IconField:         "icon",
		ResourceField:     "resource",
		EmptyText:         "No searches recorded for this collection yet.",
		RefreshIntervalMs: 10000,
	}
}

func queryConfig() plugin.QueryEditorConfig {
	return plugin.QueryEditorConfig{
		Language:          "json",
		Label:             "Chroma search",
		ExecuteLabel:      "Search",
		RunningLabel:      "Searching...",
		EmptyText:         "Send a JSON body. Use query_embeddings or query_id for similarity search, query_text/where_document for document filters, or where/ids for a metadata fetch.",
		InitialQuery:      "{\n  \"query_text\": \"\",\n  \"where\": {},\n  \"limit\": 25,\n  \"include\": [\"documents\", \"metadatas\"]\n}",
		CompletionRouteID: rid("query.complete"),
		CompletionParams:  collectionParams(),
		Exportable:        true,
	}
}

func projectionConfig() plugin.CanvasConfig {
	return plugin.CanvasConfig{
		ScaleMode:      plugin.CanvasScaleResize,
		ResizeEvents:   true,
		Interactive:    true,
		Pointer:        true,
		Keyboard:       true,
		WheelMode:      plugin.CanvasWheelModified,
		HiDPI:          true,
		FocusOnPointer: true,
		AriaLabel:      "Two-dimensional PCA projection of this collection's embeddings",
		Instructions:   "Arrow keys move the selection, Home and End jump to the edges, plus and minus zoom, 0 resets, Escape clears. Click a point to select it; drag to pan; hold Alt, Ctrl, or Cmd with the wheel to zoom.",
	}
}

func spacesConfig() plugin.CanvasConfig {
	return plugin.CanvasConfig{
		ScaleMode:      plugin.CanvasScaleResize,
		ResizeEvents:   true,
		Interactive:    true,
		Pointer:        true,
		Keyboard:       true,
		WheelMode:      plugin.CanvasWheelNone,
		HiDPI:          true,
		FocusOnPointer: true,
		AriaLabel:      "Neighbor rankings for one record compared across the l2, cosine, and inner-product distance spaces",
		Instructions:   "Left and right arrows move between distance columns, up and down change the anchor record, Enter anchors on the top neighbor of the focused column, and r resets. Click any card to re-anchor.",
	}
}

func recordEditorConfig() plugin.CodeEditorConfig {
	return plugin.CodeEditorConfig{
		Language: "json",
		InitialContent: "{\n  \"ids\": [\"record-1\"],\n  \"embeddings\": [[0.1, 0.2, 0.3]],\n" +
			"  \"documents\": [\"example document\"],\n  \"metadatas\": [{\"source\": \"shellcn\"}],\n  \"upsert\": true\n}",
		SaveRouteID: rid("records.add"),
		SaveMethod:  plugin.MethodPost,
		SaveParams:  collectionParams(),
		SaveBodyKey: "records",
		SaveDismiss: plugin.SaveDismissClose,
		SaveToast: &plugin.SaveToast{
			Summary:  "Records written",
			Detail:   "${response.count} record(s) written to ${response.collection}",
			Severity: plugin.SeveritySuccess,
		},
	}
}
