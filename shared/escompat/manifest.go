package escompat

import "github.com/charlesng35/shellcn/sdk/plugin"

// Badge color maps: a lower-cased cell value to its severity.
var (
	healthSeverities = map[string]plugin.Severity{
		"green": plugin.SeveritySuccess, "yellow": plugin.SeverityWarn, "red": plugin.SeverityDanger,
	}
	indexStatusSeverities = map[string]plugin.Severity{
		"open": plugin.SeveritySuccess, "close": plugin.SeveritySecondary, "closed": plugin.SeveritySecondary,
	}
	shardStateSeverities = map[string]plugin.Severity{
		"started": plugin.SeveritySuccess, "relocating": plugin.SeverityWarn,
		"initializing": plugin.SeverityWarn, "unassigned": plugin.SeverityDanger,
	}
)

func icon(name string) plugin.Icon {
	return plugin.Icon{Type: plugin.IconLucide, Value: name}
}

func routeID(provider Provider, suffix string) string {
	return provider.Protocol + "." + suffix
}

func objectDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{RawToggle: true}
}

func tree(provider Provider) []plugin.TreeGroup {
	return []plugin.TreeGroup{
		{Key: "indexes", Label: "Indexes", Icon: icon("database"), Source: plugin.DataSource{RouteID: routeID(provider, "indexes.tree")}, ResourceKind: "index"},
	}
}

func resources(provider Provider) []plugin.ResourceType {
	return []plugin.ResourceType{
		{
			Kind: "index", Title: "Indexes", List: plugin.DataSource{RouteID: routeID(provider, "indexes.list")},
			Columns: indexColumns(),
			Actions: plugin.ResourceActions{
				Toolbar: []string{routeID(provider, "index.create")},
				Row:     []string{routeID(provider, "index.delete")},
				Detail: []string{
					routeID(provider, "mapping.update"),
					routeID(provider, "settings.update"),
					routeID(provider, "index.refresh"),
					routeID(provider, "index.flush"),
					routeID(provider, "index.close"),
					routeID(provider, "index.open"),
					routeID(provider, "reindex"),
					routeID(provider, "documents.delete_by_query"),
					routeID(provider, "index.delete"),
				},
			},
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}", StatusField: "health", Severities: healthSeverities}, Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: routeID(provider, "index.overview"), Params: map[string]string{"index": "${resource.name}"}}, Config: objectDetailConfig()},
				{Key: "documents", Label: "Documents", Icon: icon("file-json"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: routeID(provider, "documents.list"), Params: map[string]string{"index": "${resource.name}"}}, Config: plugin.TableConfig{Columns: documentColumns(), ActionIDs: []string{routeID(provider, "document.create")}, RowActionIDs: []string{routeID(provider, "document.delete")}, Exportable: true}},
				{Key: "search", Label: "Search", Icon: icon("search"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: routeID(provider, "search.query"), Method: plugin.MethodWS, Params: map[string]string{"index": "${resource.name}"}}, Config: searchConfig(provider)},
				{Key: "mapping", Label: "Mapping", Icon: icon("braces"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: routeID(provider, "mapping.read"), Params: map[string]string{"index": "${resource.name}"}}},
				{Key: "settings", Label: "Settings", Icon: icon("settings"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: routeID(provider, "settings.read"), Params: map[string]string{"index": "${resource.name}"}}},
				{Key: "aliases", Label: "Aliases", Icon: icon("tag"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: routeID(provider, "aliases.list"), Params: map[string]string{"index": "${resource.name}"}}, Config: plugin.TableConfig{Columns: aliasColumns(), ActionIDs: []string{routeID(provider, "alias.create")}, RowActionIDs: []string{routeID(provider, "alias.delete")}, Exportable: true}},
				{Key: "shards", Label: "Shards", Icon: icon("split"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: routeID(provider, "shards.list"), Params: map[string]string{"index": "${resource.name}"}}, Config: plugin.TableConfig{Columns: shardColumns(), Exportable: true}},
			}},
		},
		{
			Kind: "document", Title: "Documents", List: plugin.DataSource{RouteID: routeID(provider, "documents.list")},
			Columns: documentColumns(),
			Actions: plugin.ResourceActions{
				Row:    []string{routeID(provider, "document.delete")},
				Detail: []string{routeID(provider, "document.delete")},
			},
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.namespace}/${resource.name}"}, DefaultTab: "editor", Tabs: []plugin.Panel{
				{Key: "document", Label: "Document", Icon: icon("file-json"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: routeID(provider, "document.read"), Params: documentParams()}},
				{Key: "editor", Label: "Editor", Icon: icon("code"), Type: plugin.PanelCodeEditor, Source: &plugin.DataSource{RouteID: routeID(provider, "document.read"), Params: documentParams()}, Config: plugin.CodeEditorConfig{Language: "json", SaveRouteID: routeID(provider, "document.update"), SaveMethod: plugin.MethodPut, SaveParams: documentParams()}},
			}},
		},
	}
}

// whenIndexOpen gates an action that only applies to an open index (its
// `_cat/indices` status is "open"); closed indices can't be refreshed/flushed.
func whenIndexOpen() *plugin.Condition {
	return &plugin.Condition{AllOf: []plugin.Rule{{Field: "status", Op: plugin.OpEq, Value: "open"}}}
}

func actions(provider Provider) []plugin.Action {
	return []plugin.Action{
		{ID: routeID(provider, "index.create"), Label: "Create index", Icon: icon("plus"), RouteID: routeID(provider, "index.create")},
		{ID: routeID(provider, "index.refresh"), Label: "Refresh", Icon: icon("refresh-cw"), RouteID: routeID(provider, "index.refresh"), Params: indexParams(), EnabledWhen: whenIndexOpen()},
		{ID: routeID(provider, "index.flush"), Label: "Flush", Icon: icon("hard-drive-download"), RouteID: routeID(provider, "index.flush"), Params: indexParams(), Confirm: true, ConfirmText: "Flush this index?", EnabledWhen: whenIndexOpen()},
		{ID: routeID(provider, "index.close"), Label: "Close", Icon: icon("lock"), RouteID: routeID(provider, "index.close"), Params: indexParams(), Confirm: true, ConfirmText: "Close this index?", EnabledWhen: whenIndexOpen()},
		{ID: routeID(provider, "index.open"), Label: "Open", Icon: icon("lock-open"), RouteID: routeID(provider, "index.open"), Params: indexParams(), EnabledWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "status", Op: plugin.OpIn, Value: []string{"close", "closed"}}}}},
		{ID: routeID(provider, "index.delete"), Label: "Delete", Icon: icon("trash-2"), RouteID: routeID(provider, "index.delete"), Params: indexParams(), Confirm: true, ConfirmText: "Delete this index and all documents?"},
		{ID: routeID(provider, "mapping.update"), Label: "Update mapping", Icon: icon("braces"), RouteID: routeID(provider, "mapping.update"), Params: indexParams(), Open: plugin.OpenDialog, Panel: plugin.PanelCodeEditor, Config: plugin.CodeEditorConfig{Language: "json", InitialContent: "{\n  \"properties\": {\n    \"new_field\": { \"type\": \"text\" }\n  }\n}", SaveRouteID: routeID(provider, "mapping.update"), SaveMethod: plugin.MethodPut, SaveParams: indexParams(), SaveBodyKey: "mapping"}},
		{ID: routeID(provider, "settings.update"), Label: "Update settings", Icon: icon("settings"), RouteID: routeID(provider, "settings.update"), Params: indexParams(), Open: plugin.OpenDialog, Panel: plugin.PanelCodeEditor, Config: plugin.CodeEditorConfig{Language: "json", InitialContent: "{\n  \"index\": {\n    \"number_of_replicas\": 1,\n    \"refresh_interval\": \"30s\"\n  }\n}", SaveRouteID: routeID(provider, "settings.update"), SaveMethod: plugin.MethodPut, SaveParams: indexParams(), SaveBodyKey: "settings"}},
		{ID: routeID(provider, "documents.delete_by_query"), Label: "Delete by query", Icon: icon("eraser"), RouteID: routeID(provider, "documents.delete_by_query"), Params: indexParams(), Open: plugin.OpenDialog, Panel: plugin.PanelCodeEditor, Confirm: true, ConfirmText: "Delete all documents matching this query? This cannot be undone.", Config: plugin.CodeEditorConfig{Language: "json", InitialContent: "{\n  \"match_all\": {}\n}", SaveRouteID: routeID(provider, "documents.delete_by_query"), SaveMethod: plugin.MethodPost, SaveParams: indexParams(), SaveBodyKey: "query"}},
		{ID: routeID(provider, "document.create"), Label: "Create document", Icon: icon("plus"), RouteID: routeID(provider, "document.create"), Params: indexParams(), Open: plugin.OpenDialog, Panel: plugin.PanelCodeEditor, Config: plugin.CodeEditorConfig{Language: "json", InitialContent: "{\n  \"title\": \"Example\"\n}", SaveRouteID: routeID(provider, "document.create"), SaveMethod: plugin.MethodPost, SaveParams: indexParams(), SaveBodyKey: "document"}},
		{ID: routeID(provider, "document.delete"), Label: "Delete", Icon: icon("trash"), RouteID: routeID(provider, "document.delete"), Params: documentParams(), Confirm: true, ConfirmText: "Delete this document?"},
		{ID: routeID(provider, "reindex"), Label: "Reindex", Icon: icon("copy"), RouteID: routeID(provider, "reindex"), Params: map[string]string{"source": "${resource.name}"}, Confirm: true, ConfirmText: "Start a reindex operation?"},
		{ID: routeID(provider, "alias.create"), Label: "Add alias", Icon: icon("plus"), RouteID: routeID(provider, "alias.create"), Params: indexParams()},
		{ID: routeID(provider, "alias.delete"), Label: "Remove", Icon: icon("trash"), RouteID: routeID(provider, "alias.delete"), Params: aliasParams(), Confirm: true, ConfirmText: "Remove this alias?"},
	}
}

func aliasParams() map[string]string {
	return map[string]string{"index": "${record.index}", "alias": "${record.alias}"}
}

func searchConfig(provider Provider) plugin.QueryEditorConfig {
	return plugin.QueryEditorConfig{
		Language:          "json",
		Label:             provider.Title + " query",
		ExecuteLabel:      "Search",
		RunningLabel:      "Searching...",
		EmptyText:         "Run a JSON DSL search to see hits.",
		InitialQuery:      `{"query":{"match_all":{}},"size":50}`,
		CompletionRouteID: routeID(provider, "completion"),
		Exportable:        true,
	}
}

func indexParams() map[string]string {
	return map[string]string{"index": "${resource.name}"}
}

func documentParams() map[string]string {
	return map[string]string{"index": "${resource.namespace}", "id": "${resource.name}"}
}

func indexColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "health", Label: "Health", Type: plugin.ColumnBadge, Sortable: true, Severities: healthSeverities},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Sortable: true, Severities: indexStatusSeverities},
		{Key: "index", Label: "Index", Sortable: true},
		{Key: "uuid", Label: "UUID"},
		{Key: "pri", Label: "Primaries", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "rep", Label: "Replicas", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "docs_count", Label: "Documents", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "docs_deleted", Label: "Deleted", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "store_size", Label: "Store", Type: plugin.ColumnBytes, Sortable: true},
	}
}

func documentColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "_id", Label: "ID", Sortable: true},
		{Key: "_index", Label: "Index", Sortable: true},
		{Key: "_score", Label: "Score", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "_version", Label: "Version", Type: plugin.ColumnNumber},
		{Key: "_source", Label: "Source", Type: plugin.ColumnJSON},
	}
}

func aliasColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "alias", Label: "Alias", Sortable: true},
		{Key: "index", Label: "Index", Sortable: true},
		{Key: "filter", Label: "Filter"},
		{Key: "routing.index", Label: "Index routing"},
		{Key: "routing.search", Label: "Search routing"},
		{Key: "is_write_index", Label: "Write", Type: plugin.ColumnBool},
	}
}

func shardColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "index", Label: "Index", Sortable: true},
		{Key: "shard", Label: "Shard", Sortable: true},
		{Key: "prirep", Label: "Type", Type: plugin.ColumnBadge},
		{Key: "state", Label: "State", Type: plugin.ColumnBadge, Sortable: true, Severities: shardStateSeverities},
		{Key: "docs", Label: "Docs", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "store", Label: "Store", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "ip", Label: "IP"},
		{Key: "node", Label: "Node"},
	}
}
