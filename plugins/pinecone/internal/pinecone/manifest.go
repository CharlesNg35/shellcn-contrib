package pinecone

import "github.com/charlesng35/shellcn/sdk/plugin"

func icon(name string) plugin.Icon { return plugin.Icon{Type: plugin.IconLucide, Value: name} }

func rid(suffix string) string { return protocolName + "." + suffix }

func serverRef() plugin.ResourceIdentity {
	return plugin.ResourceIdentity{Kind: "project", Name: "Pinecone", UID: "project"}
}

var (
	// Keys are the lower-cased forms of Pinecone's IndexModelStatus state enum;
	// the badge renderer lower-cases the cell before it looks a value up.
	stateSeverities = map[string]plugin.Severity{
		"ready":                plugin.SeveritySuccess,
		"initializing":         plugin.SeverityInfo,
		"scalingup":            plugin.SeverityInfo,
		"scalingdown":          plugin.SeverityInfo,
		"scalinguppodsize":     plugin.SeverityInfo,
		"scalingdownpodsize":   plugin.SeverityInfo,
		"terminating":          plugin.SeverityWarn,
		"disabled":             plugin.SeverityWarn,
		"initializationfailed": plugin.SeverityDanger,
		"unknown":              plugin.SeveritySecondary,
	}
	deploymentSeverities = map[string]plugin.Severity{
		"serverless": plugin.SeveritySuccess,
		"pod":        plugin.SeverityInfo,
		"byoc":       plugin.SeverityWarn,
		"unknown":    plugin.SeveritySecondary,
	}
	metricSeverities = map[string]plugin.Severity{
		"cosine":     plugin.SeverityInfo,
		"euclidean":  plugin.SeveritySecondary,
		"dotproduct": plugin.SeveritySuccess,
	}
	vectorTypeSeverities = map[string]plugin.Severity{
		"dense":  plugin.SeverityInfo,
		"sparse": plugin.SeveritySecondary,
	}
	protectionSeverities = map[string]plugin.Severity{
		"enabled":  plugin.SeverityWarn,
		"disabled": plugin.SeveritySecondary,
	}
	statusSeverities = map[string]plugin.Severity{
		"ready":        plugin.SeveritySuccess,
		"degraded":     plugin.SeverityWarn,
		"unreachable":  plugin.SeverityDanger,
		"initializing": plugin.SeverityInfo,
		"terminating":  plugin.SeverityWarn,
	}
)

func streams() []plugin.Stream {
	return []plugin.Stream{
		{ID: rid("query"), Kind: plugin.StreamQuery, RouteID: rid("query")},
		{ID: rid("index.metrics"), Kind: plugin.StreamMetrics, RouteID: rid("index.metrics")},
	}
}

func tree() []plugin.TreeGroup {
	ref := serverRef()
	return []plugin.TreeGroup{
		{Key: "project", Label: "Project", Icon: icon("layout-dashboard"), Ref: &ref},
		{Key: "indexes", Label: "Indexes", Icon: icon("box"), Source: plugin.DataSource{RouteID: rid("indexes.tree")}, ResourceKind: "index"},
		{Key: "collections", Label: "Collections", Icon: icon("archive"), Source: plugin.DataSource{RouteID: rid("collections.tree")}, ResourceKind: "collection"},
	}
}

func indexParams() map[string]string {
	return map[string]string{"index": "${resource.name}"}
}

func namespaceParams() map[string]string {
	return map[string]string{"index": "${resource.scope}", "namespace": "${resource.name}"}
}

func vectorParams() map[string]string {
	return map[string]string{
		"index":     "${resource.scope}",
		"namespace": "${resource.namespace}",
		"vector":    "${resource.name}",
	}
}

func vectorScopeParams() map[string]string {
	return map[string]string{"index": "${resource.scope}", "namespace": "${resource.namespace}"}
}

func collectionParams() map[string]string {
	return map[string]string{"collection": "${resource.name}"}
}

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		{
			Kind: "project", Title: "Pinecone project",
			List:    plugin.DataSource{RouteID: rid("overview")},
			Columns: projectColumns(),
			Actions: plugin.ResourceActions{Detail: []string{rid("index.create"), rid("collection.create")}},
			Detail: plugin.DetailView{
				Header:     plugin.HeaderSpec{Title: "${resource.name}", StatusField: "status", Severities: statusSeverities},
				DefaultTab: "overview",
				Tabs: []plugin.Panel{
					{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
						Source: &plugin.DataSource{RouteID: rid("overview")}, Config: projectDetailConfig()},
					{Key: "indexes", Label: "Indexes", Icon: icon("box"), Type: plugin.PanelTable,
						Source: &plugin.DataSource{RouteID: rid("indexes.list")}, Config: indexTableConfig()},
					{Key: "collections", Label: "Collections", Icon: icon("archive"), Type: plugin.PanelTable,
						Source: &plugin.DataSource{RouteID: rid("collections.list")}, Config: collectionTableConfig()},
				},
			},
		},
		{
			Kind: "index", Title: "Indexes",
			List:    plugin.DataSource{RouteID: rid("indexes.list")},
			Columns: indexColumns(),
			Actions: plugin.ResourceActions{
				Toolbar: []string{rid("index.create")},
				Row:     []string{rid("index.delete")},
				Detail: []string{
					rid("vectors.upsert"), rid("index.configure"), rid("collection.create"),
					rid("vectors.purge"), rid("index.delete"),
				},
			},
			Detail: plugin.DetailView{
				Header:     plugin.HeaderSpec{Title: "${resource.name}", StatusField: "state", Severities: stateSeverities},
				DefaultTab: "overview",
				Tabs: []plugin.Panel{
					{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
						Source: &plugin.DataSource{RouteID: rid("index.read"), Params: indexParams()}, Config: indexDetailConfig()},
					{Key: "namespaces", Label: "Namespaces", Icon: icon("folder-tree"), Type: plugin.PanelTable,
						Source: &plugin.DataSource{RouteID: rid("namespaces.list"), Params: indexParams()}, Config: namespaceTableConfig()},
					{Key: "vectors", Label: "Vectors", Icon: icon("braces"), Type: plugin.PanelTable,
						Source: &plugin.DataSource{RouteID: rid("vectors.list"), Params: indexParams()}, Config: vectorTableConfig()},
					{Key: "search", Label: "Search", Icon: icon("search"), Type: plugin.PanelQueryEditor,
						Source: &plugin.DataSource{RouteID: rid("query"), Method: plugin.MethodWS, Params: indexParams()}, Config: queryConfig(indexParams())},
					{Key: "stats", Label: "Stats", Icon: icon("gauge"), Type: plugin.PanelObjectDetail,
						Source: &plugin.DataSource{RouteID: rid("index.stats"), Params: indexParams()}, Config: statsDetailConfig()},
					{Key: "metrics", Label: "Metrics", Icon: icon("activity"), Type: plugin.PanelMetrics,
						Source: &plugin.DataSource{RouteID: rid("index.metrics"), Method: plugin.MethodWS, Params: indexParams()}, Config: metricsConfig()},
				},
			},
		},
		{
			Kind: "namespace", Title: "Namespaces",
			List:    plugin.DataSource{RouteID: rid("namespaces.list")},
			Columns: namespaceColumns(),
			Actions: plugin.ResourceActions{
				Row:    []string{rid("namespace.delete")},
				Detail: []string{rid("namespace.vectors.upsert"), rid("namespace.delete")},
			},
			Detail: plugin.DetailView{
				Header:     plugin.HeaderSpec{Title: "${resource.name}"},
				DefaultTab: "overview",
				Tabs: []plugin.Panel{
					{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
						Source: &plugin.DataSource{RouteID: rid("namespace.read"), Params: namespaceParams()}, Config: namespaceDetailConfig()},
					{Key: "vectors", Label: "Vectors", Icon: icon("braces"), Type: plugin.PanelTable,
						Source: &plugin.DataSource{RouteID: rid("vectors.list"), Params: namespaceParams()}, Config: vectorTableConfig()},
					{Key: "search", Label: "Search", Icon: icon("search"), Type: plugin.PanelQueryEditor,
						Source: &plugin.DataSource{RouteID: rid("query"), Method: plugin.MethodWS, Params: namespaceParams()}, Config: queryConfig(namespaceParams())},
				},
			},
		},
		{
			Kind: "vector", Title: "Vectors",
			List:    plugin.DataSource{RouteID: rid("vectors.list")},
			Columns: vectorColumns(),
			Actions: plugin.ResourceActions{
				Row:    []string{rid("vector.delete")},
				Detail: []string{rid("vector.delete")},
			},
			Detail: plugin.DetailView{
				Header:     plugin.HeaderSpec{Title: "${resource.name}"},
				DefaultTab: "overview",
				Tabs: []plugin.Panel{
					{Key: "overview", Label: "Record", Icon: icon("file-text"), Type: plugin.PanelObjectDetail,
						Source: &plugin.DataSource{RouteID: rid("vector.read"), Params: vectorParams()}, Config: vectorDetailConfig()},
					{Key: "neighbors", Label: "Nearest neighbors", Icon: icon("radar"), Type: plugin.PanelTable,
						Source: &plugin.DataSource{RouteID: rid("vector.neighbors"), Params: vectorParams()}, Config: neighborTableConfig()},
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
				Detail:  []string{rid("collection.delete")},
			},
			Detail: plugin.DetailView{
				Header:     plugin.HeaderSpec{Title: "${resource.name}", StatusField: "status", Severities: statusSeverities},
				DefaultTab: "overview",
				Tabs: []plugin.Panel{
					{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
						Source: &plugin.DataSource{RouteID: rid("collection.read"), Params: collectionParams()}, Config: collectionDetailConfig()},
				},
			},
		},
	}
}

func actions() []plugin.Action {
	return []plugin.Action{
		{ID: rid("index.create"), Label: "Create index", Icon: icon("plus"), RouteID: rid("index.create"), Group: "Create",
			OnSuccess: &plugin.ActionSuccess{SelectTab: "indexes"}},
		{ID: rid("index.configure"), Label: "Configure", Icon: icon("settings"), RouteID: rid("index.configure"), Params: indexParams(),
			OnSuccess: &plugin.ActionSuccess{SelectTab: "overview"}},
		{ID: rid("index.delete"), Label: "Delete", Icon: icon("trash-2"), RouteID: rid("index.delete"), Params: indexParams(),
			Confirm: true, Bulk: true,
			ConfirmText: "Delete this index and every vector in it? Embeddings and metadata cannot be recovered without a collection backup.",
			OnSuccess:   &plugin.ActionSuccess{Navigate: plugin.NavigateList}},
		{ID: rid("collection.create"), Label: "Create collection", Icon: icon("archive"), RouteID: rid("collection.create"), Group: "Create",
			OnSuccess: &plugin.ActionSuccess{SelectTab: "collections"}},
		{ID: rid("collection.delete"), Label: "Delete", Icon: icon("trash-2"), RouteID: rid("collection.delete"), Params: collectionParams(),
			Confirm: true, Bulk: true,
			ConfirmText: "Delete this backup collection? Indexes restored from it are unaffected, but the backup is gone.",
			OnSuccess:   &plugin.ActionSuccess{Navigate: plugin.NavigateList}},
		{ID: rid("namespace.delete"), Label: "Delete", Icon: icon("trash-2"), RouteID: rid("namespace.delete"), Params: namespaceParams(),
			Confirm: true, Bulk: true,
			ConfirmText: "Delete this namespace and every record inside it? Deleting a namespace is irreversible.",
			OnSuccess:   &plugin.ActionSuccess{Navigate: plugin.NavigateList}},
		{ID: rid("vectors.upsert"), Label: "Upsert vectors", Icon: icon("file-plus"), RouteID: rid("vectors.upsert"), Params: indexParams(),
			Open: plugin.OpenDialog, Panel: plugin.PanelCodeEditor, Config: upsertEditorConfig(indexParams()),
			OnSuccess: &plugin.ActionSuccess{SelectTab: "vectors"}},
		{ID: rid("namespace.vectors.upsert"), Label: "Upsert vectors", Icon: icon("file-plus"), RouteID: rid("vectors.upsert"), Params: namespaceParams(),
			Open: plugin.OpenDialog, Panel: plugin.PanelCodeEditor, Config: upsertEditorConfig(namespaceParams()),
			OnSuccess: &plugin.ActionSuccess{SelectTab: "vectors"}},
		{ID: rid("vector.delete"), Label: "Delete", Icon: icon("trash"), RouteID: rid("vectors.delete"),
			Params: vectorScopeParams(), Body: map[string]any{"id": "${resource.name}"},
			Confirm: true, Bulk: true, ConfirmText: "Delete the selected record(s) from this namespace?"},
		{ID: rid("vectors.purge"), Label: "Delete all vectors", Icon: icon("eraser"), RouteID: rid("vectors.delete"), Params: indexParams(),
			Body: map[string]any{"deleteAll": true}, Group: "Danger zone",
			Confirm: true, ConfirmText: "Delete every record in this index's namespace? The vectors cannot be recovered.",
			OnSuccess: &plugin.ActionSuccess{SelectTab: "vectors"}},
	}
}

func projectColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "endpoint", Label: "Control plane", Sortable: true},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Sortable: true, Severities: statusSeverities},
		{Key: "indexes", Label: "Indexes", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "collections", Label: "Collections", Type: plugin.ColumnNumber, Sortable: true},
	}
}

func indexColumns() []plugin.Column {
	precision := 2
	return []plugin.Column{
		{Key: "name", Label: "Index", Sortable: true},
		{Key: "state", Label: "State", Type: plugin.ColumnBadge, Sortable: true, Severities: stateSeverities},
		{Key: "deployment", Label: "Deployment", Type: plugin.ColumnBadge, Sortable: true, Severities: deploymentSeverities},
		{Key: "vectors", Label: "Vectors", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "namespaces", Label: "Namespaces", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "dimension", Label: "Dimensions", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "metric", Label: "Metric", Type: plugin.ColumnBadge, Sortable: true, Severities: metricSeverities},
		{Key: "vector_type", Label: "Vectors type", Type: plugin.ColumnBadge, Sortable: true, Severities: vectorTypeSeverities},
		{Key: "fullness", Label: "Fullness", Type: plugin.ColumnPercent, Sortable: true, Precision: &precision},
		{Key: "location", Label: "Location", Sortable: true},
		{Key: "deletion_protection", Label: "Protected", Type: plugin.ColumnBadge, Sortable: true, Severities: protectionSeverities},
		{Key: "tags", Label: "Tags", Type: plugin.ColumnJSON},
	}
}

func namespaceColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Namespace", Sortable: true},
		{Key: "record_count", Label: "Records", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "index", Label: "Index", Sortable: true},
	}
}

func vectorColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "id", Label: "ID", ReadOnly: true},
		{Key: "metadata", Label: "Metadata", Type: plugin.ColumnJSON},
		{Key: "dimension", Label: "Dimensions", Type: plugin.ColumnNumber},
		{Key: "namespace", Label: "Namespace"},
	}
}

func neighborColumns() []plugin.Column {
	precision := 5
	return []plugin.Column{
		{Key: "rank", Label: "Rank", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "id", Label: "ID", Sortable: true},
		{Key: "score", Label: "Score", Type: plugin.ColumnNumber, Sortable: true, Precision: &precision},
		{Key: "metadata", Label: "Metadata", Type: plugin.ColumnJSON},
	}
}

func collectionColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Collection", Sortable: true},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Sortable: true, Severities: statusSeverities},
		{Key: "vector_count", Label: "Vectors", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "dimension", Label: "Dimensions", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "size", Label: "Size", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "environment", Label: "Environment", Sortable: true},
	}
}

func indexTableConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Columns:           indexColumns(),
		ActionIDs:         []string{rid("index.create")},
		RowActionIDs:      []string{rid("index.delete")},
		DefaultSort:       &plugin.SortKey{Field: "name"},
		RefreshIntervalMs: 20000,
		EmptyText:         "This project has no indexes yet. Create one to start storing embeddings.",
		Exportable:        true,
		RowClick:          plugin.RowClickNavigate,
	}
}

func collectionTableConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Columns:           collectionColumns(),
		ActionIDs:         []string{rid("collection.create")},
		RowActionIDs:      []string{rid("collection.delete")},
		DefaultSort:       &plugin.SortKey{Field: "name"},
		RefreshIntervalMs: 60000,
		EmptyText:         "No backup collections. Create one from a pod-based index to snapshot its vectors.",
		Exportable:        true,
		RowClick:          plugin.RowClickNavigate,
	}
}

func namespaceTableConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Columns:           namespaceColumns(),
		RowActionIDs:      []string{rid("namespace.delete")},
		RefreshIntervalMs: 30000,
		EmptyText:         "No namespaces in this index yet. A namespace appears once a record is written to it.",
		Exportable:        true,
		RowClick:          plugin.RowClickNavigate,
	}
}

func vectorTableConfig() plugin.TableConfig {
	return plugin.TableConfig{
		// Upsert lives in the detail header so it is scoped by the open resource;
		// this table renders under both an index and a namespace.
		Columns:           vectorColumns(),
		RowActionIDs:      []string{rid("vector.delete")},
		RefreshIntervalMs: 30000,
		EmptyText:         "No records listed. Pinecone lists record IDs for serverless indexes only, in sorted order; the search box matches IDs.",
		Exportable:        true,
		RowClick:          plugin.RowClickNavigate,
	}
}

func neighborTableConfig() plugin.TableConfig {
	return plugin.TableConfig{
		Columns:           neighborColumns(),
		DefaultSort:       &plugin.SortKey{Field: "rank"},
		RefreshIntervalMs: 60000,
		EmptyText:         "No other records in this namespace to compare against.",
		Exportable:        true,
	}
}

func projectDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Sections: []plugin.ObjectDetailSection{
			{Title: "Project", Fields: []plugin.ObjectDetailField{
				{Key: "endpoint", Label: "Control plane", Copy: true},
				{Key: "apiVersion", Label: "API version", Copy: true},
				{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: statusSeverities},
				{Key: "namespace", Label: "Default namespace", Copy: true},
			}},
			{Title: "Inventory", Fields: []plugin.ObjectDetailField{
				{Key: "indexes", Label: "Indexes", Type: plugin.ColumnNumber},
				{Key: "readyIndexes", Label: "Ready", Type: plugin.ColumnNumber},
				{Key: "serverlessIndexes", Label: "Serverless", Type: plugin.ColumnNumber},
				{Key: "podIndexes", Label: "Pod-based", Type: plugin.ColumnNumber},
				{Key: "collections", Label: "Collections", Type: plugin.ColumnNumber},
			}},
			{Title: "Connection", Fields: []plugin.ObjectDetailField{
				{Key: "readOnly", Label: "Read-only connection", Type: plugin.ColumnBool},
				{Key: "privateNet", Label: "Private endpoints", Type: plugin.ColumnBool},
			}},
		},
	}
}

func indexDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Sections: []plugin.ObjectDetailSection{
			{Title: "Identity", Fields: []plugin.ObjectDetailField{
				{Key: "name", Label: "Name", Copy: true},
				{Key: "host", Label: "Data-plane host", Copy: true},
				{Key: "private_host", Label: "Private host", Copy: true},
				{Key: "state", Label: "State", Type: plugin.ColumnBadge, Severities: stateSeverities},
				{Key: "ready", Label: "Ready", Type: plugin.ColumnBool},
				{Key: "deletion_protection", Label: "Deletion protection", Type: plugin.ColumnBadge, Severities: protectionSeverities},
			}},
			{Title: "Vectors", Fields: []plugin.ObjectDetailField{
				{Key: "vectors", Label: "Records", Type: plugin.ColumnNumber},
				{Key: "namespaces", Label: "Namespaces", Type: plugin.ColumnNumber},
				{Key: "dimension", Label: "Dimensions", Type: plugin.ColumnNumber},
				{Key: "metric", Label: "Metric", Type: plugin.ColumnBadge, Severities: metricSeverities},
				{Key: "vector_type", Label: "Vector type", Type: plugin.ColumnBadge, Severities: vectorTypeSeverities},
				{Key: "estimated_vector_bytes", Label: "Vector payload", Type: plugin.ColumnBytes},
			}},
			{Title: "Capacity", Fields: []plugin.ObjectDetailField{
				{Key: "fullness", Label: "Index fullness", Type: plugin.ColumnPercent},
				{Key: "read_capacity", Label: "Read capacity", Type: plugin.ColumnJSON},
			}},
			{Title: "Placement", Fields: []plugin.ObjectDetailField{
				{Key: "deployment", Label: "Deployment", Type: plugin.ColumnBadge, Severities: deploymentSeverities},
				{Key: "location", Label: "Location"},
				{Key: "pod_type", Label: "Pod type"},
				{Key: "pods", Label: "Pods", Type: plugin.ColumnNumber},
				{Key: "replicas", Label: "Replicas", Type: plugin.ColumnNumber},
				{Key: "shards", Label: "Shards", Type: plugin.ColumnNumber},
				{Key: "source_collection", Label: "Restored from"},
			}},
			{Title: "Metadata", Fields: []plugin.ObjectDetailField{
				{Key: "tags", Label: "Tags", Type: plugin.ColumnJSON},
				{Key: "embed", Label: "Integrated embedding", Type: plugin.ColumnJSON},
				{Key: "spec", Label: "Spec", Type: plugin.ColumnJSON},
			}},
		},
	}
}

func statsDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Sections: []plugin.ObjectDetailSection{
			{Title: "Totals", Fields: []plugin.ObjectDetailField{
				{Key: "vectors", Label: "Records", Type: plugin.ColumnNumber},
				{Key: "namespaces", Label: "Namespaces", Type: plugin.ColumnNumber},
				{Key: "dimension", Label: "Dimensions", Type: plugin.ColumnNumber},
				{Key: "metric", Label: "Metric", Type: plugin.ColumnBadge, Severities: metricSeverities},
				{Key: "vector_type", Label: "Vector type", Type: plugin.ColumnBadge, Severities: vectorTypeSeverities},
				{Key: "estimated_vector_bytes", Label: "Vector payload", Type: plugin.ColumnBytes},
			}},
			{Title: "Capacity", Fields: []plugin.ObjectDetailField{
				{Key: "fullness", Label: "Index fullness", Type: plugin.ColumnPercent},
			}},
			{Title: "Per namespace", Fields: []plugin.ObjectDetailField{
				{Key: "namespace_counts", Label: "Record counts", Type: plugin.ColumnJSON},
			}},
		},
	}
}

func namespaceDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Sections: []plugin.ObjectDetailSection{{Title: "Namespace", Fields: []plugin.ObjectDetailField{
			{Key: "name", Label: "Name", Copy: true},
			{Key: "index", Label: "Index", Copy: true},
			{Key: "record_count", Label: "Records", Type: plugin.ColumnNumber},
			{Key: "read_only", Label: "Read-only connection", Type: plugin.ColumnBool},
		}}},
	}
}

func vectorDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Sections: []plugin.ObjectDetailSection{
			{Title: "Record", Fields: []plugin.ObjectDetailField{
				{Key: "id", Label: "ID", Copy: true},
				{Key: "index", Label: "Index"},
				{Key: "namespace", Label: "Namespace"},
			}},
			{Title: "Vector", Fields: []plugin.ObjectDetailField{
				{Key: "dimension", Label: "Dimensions", Type: plugin.ColumnNumber},
				{Key: "norm", Label: "L2 norm", Type: plugin.ColumnNumber},
				{Key: "sparse_terms", Label: "Sparse terms", Type: plugin.ColumnNumber},
				{Key: "values", Label: "Values", Type: plugin.ColumnJSON},
				{Key: "sparse_values", Label: "Sparse values", Type: plugin.ColumnJSON},
			}},
			{Title: "Metadata", Fields: []plugin.ObjectDetailField{
				{Key: "metadata_keys", Label: "Keys", Type: plugin.ColumnJSON},
				{Key: "metadata", Label: "Metadata", Type: plugin.ColumnJSON},
			}},
		},
	}
}

func collectionDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Sections: []plugin.ObjectDetailSection{{Title: "Collection", Fields: []plugin.ObjectDetailField{
			{Key: "name", Label: "Name", Copy: true},
			{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: statusSeverities},
			{Key: "vector_count", Label: "Records", Type: plugin.ColumnNumber},
			{Key: "dimension", Label: "Dimensions", Type: plugin.ColumnNumber},
			{Key: "size", Label: "Size", Type: plugin.ColumnBytes},
			{Key: "environment", Label: "Environment"},
		}}},
	}
}

func metricsConfig() plugin.MetricsConfig {
	return plugin.MetricsConfig{
		Stats: []plugin.MetricStat{
			{Key: "vectors", Label: "Records"},
			{Key: "namespaces", Label: "Namespaces"},
			{Key: "dimension", Label: "Dimensions"},
			{Key: "vectorBytes", Label: "Vector payload", Unit: "bytes"},
			{Key: "latencyMs", Label: "Stats latency", Unit: "ms"},
		},
		Usage: []plugin.MetricUsage{{
			Key: "fullnessPct", Label: "Index fullness", Type: plugin.ColumnPercent,
			Usage: &plugin.UsageSpec{PercentKey: "fullnessPct", WarnAt: 80, CriticalAt: 95},
		}},
		Series: []plugin.MetricSeries{
			{Key: "vectors", Label: "Records"},
			{Key: "latencyMs", Label: "Stats latency", Unit: "ms"},
		},
		History: 90,
	}
}

func queryConfig(params map[string]string) plugin.QueryEditorConfig {
	return plugin.QueryEditorConfig{
		Language:     "json",
		Label:        "Pinecone query",
		ExecuteLabel: "Search",
		RunningLabel: "Searching...",
		EmptyText: "Send a Pinecone /query body. Use vector for a dense search, sparseVector for a sparse one, " +
			"or id to search by an existing record. Add filter for metadata narrowing and namespace to target another namespace.",
		InitialQuery:      "{\n  \"id\": \"record-1\",\n  \"topK\": 10,\n  \"includeMetadata\": true,\n  \"includeValues\": false\n}",
		CompletionRouteID: rid("query.complete"),
		CompletionParams:  params,
		Exportable:        true,
	}
}

func upsertEditorConfig(params map[string]string) plugin.CodeEditorConfig {
	return plugin.CodeEditorConfig{
		Language: "json",
		InitialContent: "{\n  \"vectors\": [\n    {\n      \"id\": \"record-1\",\n" +
			"      \"values\": [0.1, 0.2, 0.3],\n      \"metadata\": {\"source\": \"shellcn\"}\n    }\n  ]\n}",
		SaveRouteID: rid("vectors.upsert"),
		SaveMethod:  plugin.MethodPost,
		SaveParams:  params,
		SaveBodyKey: "vectors",
		SaveDismiss: plugin.SaveDismissClose,
		SaveToast: &plugin.SaveToast{
			Summary:  "Vectors upserted",
			Detail:   "${response.upserted} record(s) written to ${response.namespace}",
			Severity: plugin.SeveritySuccess,
		},
	}
}
