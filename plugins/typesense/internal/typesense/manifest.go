package typesense

import "github.com/charlesng35/shellcn/sdk/plugin"

func icon(name string) plugin.Icon { return plugin.Icon{Type: plugin.IconLucide, Value: name} }

func rid(suffix string) string { return "typesense." + suffix }

func objectDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{RawToggle: true}
}

func tree() []plugin.TreeGroup {
	return []plugin.TreeGroup{
		{Key: "collections", Label: "Collections", Icon: icon("database"), Source: plugin.DataSource{RouteID: rid("collections.tree")}, ResourceKind: "collection"},
		{Key: "aliases", Label: "Aliases", Icon: icon("tag"), Source: plugin.DataSource{RouteID: rid("aliases.tree")}, ResourceKind: "alias"},
		{Key: "keys", Label: "API keys", Icon: icon("key-round"), Source: plugin.DataSource{RouteID: rid("keys.tree")}, ResourceKind: "key"},
	}
}

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		{
			Kind: "collection", Title: "Collections", List: plugin.DataSource{RouteID: rid("collections.list")},
			Columns: collectionColumns(),
			Actions: plugin.ResourceActions{
				Toolbar: []string{rid("collection.create"), rid("collection.clone")},
				Row:     []string{rid("collection.delete")},
				Detail: []string{
					rid("collection.update"), rid("alias.upsert"), rid("collection.delete"),
				},
			},
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("collection.overview"), Params: collectionParams()}, Config: objectDetailConfig()},
				{Key: "documents", Label: "Documents", Icon: icon("file-json"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("documents.list"), Params: collectionParams()}, Config: plugin.TableConfig{Columns: documentColumns(), ActionIDs: []string{rid("document.upsert"), rid("documents.import")}, RowActionIDs: []string{rid("document.delete")}, Exportable: true}},
				{Key: "search", Label: "Search", Icon: icon("search"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: rid("search.query"), Method: plugin.MethodWS, Params: collectionParams()}, Config: searchConfig()},
				{Key: "synonyms", Label: "Synonym sets", Icon: icon("replace"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("synonyms.list")}, Config: plugin.TableConfig{Columns: synonymColumns(), ActionIDs: []string{rid("synonym.upsert")}, RowActionIDs: []string{rid("synonym.delete")}, Exportable: true}},
				{Key: "overrides", Label: "Curation sets", Icon: icon("pin"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("overrides.list")}, Config: plugin.TableConfig{Columns: overrideColumns(), ActionIDs: []string{rid("override.upsert")}, RowActionIDs: []string{rid("override.delete")}, Exportable: true}},
				{Key: "export", Label: "Export", Icon: icon("download"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: rid("documents.export"), Params: collectionParams()}},
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
			Kind: "alias", Title: "Aliases", List: plugin.DataSource{RouteID: rid("aliases.list")},
			Columns: aliasColumns(),
			Actions: plugin.ResourceActions{
				Toolbar: []string{rid("alias.upsert")},
				Row:     []string{rid("alias.delete")},
				Detail:  []string{rid("alias.delete")},
			},
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("tag"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("alias.read"), Params: aliasParams()}, Config: objectDetailConfig()},
			}},
		},
		{
			Kind: "key", Title: "API keys", List: plugin.DataSource{RouteID: rid("keys.list")},
			Columns: keyColumns(),
			Actions: plugin.ResourceActions{
				Toolbar: []string{rid("key.create")},
				Row:     []string{rid("key.delete")},
				Detail:  []string{rid("key.delete")},
			},
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("key-round"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("key.read"), Params: keyParams()}, Config: objectDetailConfig()},
			}},
		},
	}
}

func actions() []plugin.Action {
	return []plugin.Action{
		{ID: rid("collection.create"), Label: "Create collection", Icon: icon("plus"), RouteID: rid("collection.create")},
		{ID: rid("collection.clone"), Label: "Clone collection", Icon: icon("copy"), RouteID: rid("collection.clone"), Confirm: true, ConfirmText: "Create a collection from an existing schema?"},
		{ID: rid("collection.update"), Label: "Update schema", Icon: icon("columns-3"), RouteID: rid("collection.update"), Params: collectionParams(), Confirm: true, ConfirmText: "Update this collection schema?"},
		{ID: rid("collection.delete"), Label: "Delete", Icon: icon("trash-2"), RouteID: rid("collection.delete"), Params: collectionParams(), Confirm: true, ConfirmText: "Delete this collection and all documents?"},
		{ID: rid("document.upsert"), Label: "Upsert document", Icon: icon("plus"), RouteID: rid("document.upsert"), Params: collectionParams(), Open: plugin.OpenDialog, Panel: plugin.PanelCodeEditor, Config: plugin.CodeEditorConfig{Language: "json", InitialContent: "{\n  \"id\": \"example\"\n}", SaveRouteID: rid("document.upsert"), SaveMethod: plugin.MethodPost, SaveParams: collectionParams(), SaveBodyKey: "document", SaveExtra: map[string]any{"action": "upsert"}}},
		{ID: rid("documents.import"), Label: "Import JSONL", Icon: icon("upload"), RouteID: rid("documents.import"), Params: collectionParams(), Confirm: true, ConfirmText: "Import these documents into the collection?"},
		{ID: rid("document.delete"), Label: "Delete", Icon: icon("trash"), RouteID: rid("document.delete"), Params: documentParams(), Confirm: true, ConfirmText: "Delete this document?"},
		{ID: rid("alias.upsert"), Label: "Upsert alias", Icon: icon("tag"), RouteID: rid("alias.upsert")},
		{ID: rid("alias.delete"), Label: "Delete", Icon: icon("trash"), RouteID: rid("alias.delete"), Params: aliasParams(), Confirm: true, ConfirmText: "Delete this alias?"},
		{ID: rid("synonym.upsert"), Label: "Upsert synonym set", Icon: icon("replace"), RouteID: rid("synonym.upsert")},
		{ID: rid("synonym.delete"), Label: "Delete", Icon: icon("trash"), RouteID: rid("synonym.delete"), Params: synonymParams(), Confirm: true, ConfirmText: "Delete this synonym?"},
		{ID: rid("override.upsert"), Label: "Upsert curation set", Icon: icon("pin"), RouteID: rid("override.upsert")},
		{ID: rid("override.delete"), Label: "Delete", Icon: icon("trash"), RouteID: rid("override.delete"), Params: overrideParams(), Confirm: true, ConfirmText: "Delete this override?"},
		{ID: rid("key.create"), Label: "Create key", Icon: icon("plus"), RouteID: rid("key.create")},
		{ID: rid("key.delete"), Label: "Delete", Icon: icon("trash"), RouteID: rid("key.delete"), Params: keyParams(), Confirm: true, ConfirmText: "Delete this API key?"},
	}
}

func searchConfig() plugin.QueryEditorConfig {
	return plugin.QueryEditorConfig{
		Language:          "json",
		Label:             "Typesense query",
		ExecuteLabel:      "Search",
		RunningLabel:      "Searching...",
		EmptyText:         "Run a Typesense JSON search to see hits.",
		InitialQuery:      `{"q":"*","per_page":50}`,
		CompletionRouteID: rid("completion"),
		Exportable:        true,
	}
}

func collectionParams() map[string]string { return map[string]string{"collection": "${resource.name}"} }
func aliasParams() map[string]string      { return map[string]string{"alias": "${resource.name}"} }
func keyParams() map[string]string        { return map[string]string{"key": "${resource.name}"} }
func documentParams() map[string]string {
	return map[string]string{"collection": "${resource.namespace}", "id": "${resource.name}"}
}

func synonymParams() map[string]string {
	return map[string]string{"synonym": "${resource.name}"}
}

func overrideParams() map[string]string {
	return map[string]string{"override": "${resource.name}"}
}

func collectionColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Collection", Sortable: true},
		{Key: "num_documents", Label: "Documents", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "fields", Label: "Fields", Type: plugin.ColumnJSON},
		{Key: "default_sorting_field", Label: "Default sort"},
		{Key: "created_at", Label: "Created", Type: plugin.ColumnNumber, Sortable: true},
	}
}

func documentColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "_id", Label: "ID", Sortable: true},
		{Key: "_collection", Label: "Collection", Sortable: true},
		{Key: "_text_match", Label: "Score", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "_source", Label: "Document", Type: plugin.ColumnJSON},
	}
}

func aliasColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Alias", Sortable: true}, {Key: "collection_name", Label: "Collection", Sortable: true}}
}

func synonymColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Set", Sortable: true}, {Key: "items", Label: "Items", Type: plugin.ColumnJSON}}
}

func overrideColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Set", Sortable: true}, {Key: "items", Label: "Items", Type: plugin.ColumnJSON}}
}

func keyColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "id", Label: "ID", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "description", Label: "Description"},
		{Key: "actions", Label: "Actions", Type: plugin.ColumnJSON},
		{Key: "collections", Label: "Collections", Type: plugin.ColumnJSON},
		{Key: "expires_at", Label: "Expires", Type: plugin.ColumnNumber, Sortable: true},
	}
}
