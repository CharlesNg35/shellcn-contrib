package solr

import "github.com/charlesng35/shellcn/sdk/plugin"

func icon(name string) plugin.Icon { return plugin.Icon{Type: plugin.IconLucide, Value: name} }

// healthSeverities colors a collection's health badge by value.
var healthSeverities = map[string]plugin.Severity{
	"green": plugin.SeveritySuccess, "yellow": plugin.SeverityWarn, "red": plugin.SeverityDanger,
}

func rid(suffix string) string { return protocolName + "." + suffix }

func tree() []plugin.TreeGroup {
	return []plugin.TreeGroup{
		{Key: "cores", Label: "Collections / Cores", Icon: icon("database"), Source: plugin.DataSource{RouteID: rid("cores.tree")}, ResourceKind: "core"},
	}
}

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		{
			Kind: "core", Title: "Collections / Cores", List: plugin.DataSource{RouteID: rid("cores.list")},
			Columns: coreColumns(),
			Actions: plugin.ResourceActions{
				Toolbar: []string{rid("core.create")},
				Row:     []string{rid("core.delete")},
				Detail: []string{
					rid("core.commit"), rid("core.optimize"), rid("core.reload"), rid("core.delete"),
				},
			},
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: rid("core.overview"), Params: coreParams()}},
				{Key: "documents", Label: "Documents", Icon: icon("file-json"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("documents.list"), Params: coreParams()}, Config: plugin.TableConfig{Columns: documentColumns(), ActionIDs: []string{rid("document.upsert"), rid("documents.delete_query")}, RowActionIDs: []string{rid("document.delete")}, Exportable: true}},
				{Key: "search", Label: "Search", Icon: icon("search"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: rid("search.query"), Method: plugin.MethodWS, Params: coreParams()}, Config: searchConfig()},
				{Key: "schema", Label: "Schema", Icon: icon("braces"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: rid("schema.read"), Params: coreParams()}},
				{Key: "fields", Label: "Fields", Icon: icon("columns-3"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("schema.fields"), Params: coreParams()}, Config: plugin.TableConfig{Columns: fieldColumns(), ActionIDs: []string{rid("schema.field.add")}, RowActionIDs: []string{rid("schema.field.replace"), rid("schema.field.delete")}, Exportable: true}},
				{Key: "config", Label: "Config", Icon: icon("settings"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: rid("config.read"), Params: coreParams()}},
				{Key: "ping", Label: "Ping", Icon: icon("activity"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: rid("core.ping"), Params: coreParams()}},
			}},
		},
		{
			Kind: "document", Title: "Documents", List: plugin.DataSource{RouteID: rid("documents.list")},
			Columns: documentColumns(),
			Actions: plugin.ResourceActions{
				Row:    []string{rid("document.delete")},
				Detail: []string{rid("document.delete")},
			},
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.namespace}/${resource.name}"}, DefaultTab: "editor", Tabs: []plugin.Panel{
				{Key: "document", Label: "Document", Icon: icon("file-json"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: rid("document.read"), Params: documentParams()}},
				{Key: "editor", Label: "Editor", Icon: icon("code"), Type: plugin.PanelCodeEditor, Source: &plugin.DataSource{RouteID: rid("document.read"), Params: documentParams()}, Config: plugin.CodeEditorConfig{Language: "json", SaveRouteID: rid("document.update"), SaveMethod: plugin.MethodPatch, SaveParams: documentParams()}},
			}},
		},
		{
			Kind: "field", Title: "Fields", List: plugin.DataSource{RouteID: rid("schema.fields")},
			Columns: fieldColumns(),
			Actions: plugin.ResourceActions{
				Row:    []string{rid("schema.field.delete")},
				Detail: []string{rid("schema.field.replace"), rid("schema.field.delete")},
			},
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.namespace}/${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("columns-3"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: rid("schema.field.read"), Params: fieldParams()}},
			}},
		},
	}
}

func actions() []plugin.Action {
	return []plugin.Action{
		{ID: rid("core.create"), Label: "Create collection/core", Icon: icon("plus"), RouteID: rid("core.create")},
		{ID: rid("core.reload"), Label: "Reload", Icon: icon("refresh-cw"), RouteID: rid("core.reload"), Params: coreParams(), Confirm: true, ConfirmText: "Reload this collection or core?"},
		{ID: rid("core.commit"), Label: "Commit", Icon: icon("check"), RouteID: rid("core.commit"), Params: coreParams(), Confirm: true, ConfirmText: "Commit pending updates for this collection or core?"},
		{ID: rid("core.optimize"), Label: "Optimize", Icon: icon("gauge"), RouteID: rid("core.optimize"), Params: coreParams(), Confirm: true, ConfirmText: "Optimize this collection or core now?"},
		{ID: rid("core.delete"), Label: "Delete", Icon: icon("trash-2"), RouteID: rid("core.delete"), Params: coreParams(), Confirm: true, ConfirmText: "Delete this collection or unload this core with its index data?"},
		{ID: rid("document.upsert"), Label: "Upsert document", Icon: icon("plus"), RouteID: rid("document.upsert"), Params: coreParams(), Open: plugin.OpenDialog, Panel: plugin.PanelCodeEditor, Config: plugin.CodeEditorConfig{Language: "json", InitialContent: "{\n  \"id\": \"example\"\n}", SaveRouteID: rid("document.upsert"), SaveMethod: plugin.MethodPost, SaveParams: coreParams(), SaveBodyKey: "document", SaveExtra: map[string]any{"commit": true}}},
		{ID: rid("document.delete"), Label: "Delete", Icon: icon("trash"), RouteID: rid("document.delete"), Params: documentParams(), Confirm: true, ConfirmText: "Delete this document?"},
		{ID: rid("documents.delete_query"), Label: "Delete by query", Icon: icon("eraser"), RouteID: rid("documents.delete_query"), Params: coreParams(), Confirm: true, ConfirmText: "Delete all Solr documents matching this query?"},
		{ID: rid("schema.field.add"), Label: "Add field", Icon: icon("columns-3"), RouteID: rid("schema.field.add"), Params: coreParams(), Confirm: true, ConfirmText: "Add this field to the managed schema?"},
		{ID: rid("schema.field.replace"), Label: "Edit field", Icon: icon("pencil"), RouteID: rid("schema.field.replace"), Params: fieldParams(), Confirm: true, ConfirmText: "Replace this field's definition? Solr requires the full definition and reindexing may be needed for existing documents."},
		{ID: rid("schema.field.delete"), Label: "Delete field", Icon: icon("trash"), RouteID: rid("schema.field.delete"), Params: fieldParams(), Confirm: true, ConfirmText: "Delete this field from the managed schema?"},
	}
}

func searchConfig() plugin.QueryEditorConfig {
	return plugin.QueryEditorConfig{
		Language:          "json",
		Label:             "Solr query",
		ExecuteLabel:      "Search",
		RunningLabel:      "Searching...",
		EmptyText:         "Run a Solr JSON query to see documents.",
		InitialQuery:      `{"q":"*:*","rows":50}`,
		CompletionRouteID: rid("completion"),
		Exportable:        true,
	}
}

func coreParams() map[string]string { return map[string]string{"core": "${resource.name}"} }

func documentParams() map[string]string {
	return map[string]string{"core": "${resource.namespace}", "id": "${resource.name}"}
}

func fieldParams() map[string]string {
	return map[string]string{"core": "${resource.namespace}", "field": "${resource.name}"}
}

func coreColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Collection/Core", Sortable: true},
		{Key: "mode", Label: "Mode", Type: plugin.ColumnBadge, Sortable: true},
		{Key: "health", Label: "Health", Type: plugin.ColumnBadge, Sortable: true, Severities: healthSeverities},
		{Key: "numDocs", Label: "Documents", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "maxDoc", Label: "Max doc", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "deletedDocs", Label: "Deleted", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "size", Label: "Size"},
		{Key: "uptime", Label: "Uptime"},
	}
}

func documentColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "_id", Label: "ID", Sortable: true},
		{Key: "_core", Label: "Core", Sortable: true},
		{Key: "_score", Label: "Score", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "_source", Label: "Document", Type: plugin.ColumnJSON},
	}
}

func fieldColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Field", Sortable: true},
		{Key: "type", Label: "Type", Sortable: true},
		{Key: "indexed", Label: "Indexed", Type: plugin.ColumnBool, Sortable: true},
		{Key: "stored", Label: "Stored", Type: plugin.ColumnBool, Sortable: true},
		{Key: "multiValued", Label: "Multi-valued", Type: plugin.ColumnBool, Sortable: true},
		{Key: "required", Label: "Required", Type: plugin.ColumnBool, Sortable: true},
	}
}
