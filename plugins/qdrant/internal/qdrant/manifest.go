package qdrant

import "github.com/charlesng35/shellcn/sdk/plugin"

func icon(name string) plugin.Icon { return plugin.Icon{Type: plugin.IconLucide, Value: name} }

func rid(suffix string) string { return protocolName + "." + suffix }

func objectDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{RawToggle: true}
}

func tree() []plugin.TreeGroup {
	return []plugin.TreeGroup{
		{Key: "overview", Label: "Overview", Icon: icon("layout-dashboard"), Ref: &plugin.ResourceIdentity{Kind: "server", Name: "Qdrant", UID: "server"}},
		{Key: "collections", Label: "Collections", Icon: icon("database"), Source: plugin.DataSource{RouteID: rid("collections.tree")}, ResourceKind: "collection"},
	}
}

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		{
			Kind: "server", Title: "Qdrant", List: plugin.DataSource{RouteID: rid("overview")},
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("layout-dashboard"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("overview")}, Config: objectDetailConfig()},
				{Key: "collections", Label: "Collections", Icon: icon("database"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("collections.list")}, Config: plugin.TableConfig{Columns: collectionColumns(), Exportable: true}},
			}},
		},
		{
			Kind: "collection", Title: "Collections", List: plugin.DataSource{RouteID: rid("collections.list")},
			Columns: collectionColumns(),
			Actions: plugin.ResourceActions{
				Toolbar: []string{rid("collection.create")},
				Row:     []string{rid("collection.delete")},
				Detail:  []string{rid("point.upsert"), rid("payload.index.create"), rid("alias.create"), rid("snapshot.create"), rid("collection.delete")},
			},
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("collection.read"), Params: collectionParams()}, Config: objectDetailConfig()},
				{Key: "points", Label: "Points", Icon: icon("braces"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("points.list"), Params: collectionParams()}, Config: plugin.TableConfig{Columns: pointColumns(), RowActionIDs: []string{rid("point.delete")}, Exportable: true}},
				{Key: "query", Label: "Query", Icon: icon("search"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: rid("query"), Method: plugin.MethodWS, Params: collectionParams()}, Config: queryConfig()},
				{Key: "aliases", Label: "Aliases", Icon: icon("tags"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("collection.aliases"), Params: collectionParams()}, Config: plugin.TableConfig{Columns: aliasColumns(), RowActionIDs: []string{rid("alias.delete")}, Exportable: true}},
				{Key: "snapshots", Label: "Snapshots", Icon: icon("archive"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("snapshots.list"), Params: collectionParams()}, Config: plugin.TableConfig{Columns: snapshotColumns(), Exportable: true}},
			}},
		},
		{
			Kind: "point", Title: "Points", List: plugin.DataSource{RouteID: rid("points.list")},
			Columns: pointColumns(),
			Actions: plugin.ResourceActions{Detail: []string{rid("point.delete")}},
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.namespace}/${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "document", Label: "Point", Icon: icon("braces"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: rid("point.read"), Params: pointParams()}},
			}},
		},
	}
}

func actions() []plugin.Action {
	return []plugin.Action{
		{ID: rid("collection.create"), Label: "Create collection", Icon: icon("plus"), RouteID: rid("collection.create")},
		{ID: rid("collection.delete"), Label: "Delete", Icon: icon("trash-2"), RouteID: rid("collection.delete"), Params: collectionParams(), Confirm: true, ConfirmText: "Delete this Qdrant collection and all points?"},
		{ID: rid("point.upsert"), Label: "Upsert points", Icon: icon("plus"), RouteID: rid("point.upsert"), Params: collectionParams(), Open: plugin.OpenDialog, Panel: plugin.PanelCodeEditor, Config: plugin.CodeEditorConfig{Language: "json", InitialContent: "{\n  \"points\": [\n    {\"id\": 1, \"vector\": [0.1, 0.2, 0.3, 0.4], \"payload\": {\"text\": \"example\"}}\n  ]\n}", SaveRouteID: rid("point.upsert"), SaveMethod: plugin.MethodPut, SaveParams: collectionParams(), SaveBodyKey: "body"}},
		{ID: rid("point.delete"), Label: "Delete", Icon: icon("trash"), RouteID: rid("point.delete"), Params: pointParams(), Confirm: true, ConfirmText: "Delete this point?"},
		{ID: rid("payload.index.create"), Label: "Create payload index", Icon: icon("list-plus"), RouteID: rid("payload.index.create"), Params: collectionParams()},
		{ID: rid("alias.create"), Label: "Create alias", Icon: icon("tag"), RouteID: rid("alias.create"), Params: collectionParams()},
		{ID: rid("alias.delete"), Label: "Delete", Icon: icon("trash"), RouteID: rid("alias.delete"), Params: aliasParams(), Confirm: true, ConfirmText: "Delete this alias?"},
		{ID: rid("snapshot.create"), Label: "Create snapshot", Icon: icon("archive"), RouteID: rid("snapshot.create"), Params: collectionParams(), Confirm: true, ConfirmText: "Create a snapshot for this collection?"},
	}
}

func collectionParams() map[string]string { return map[string]string{"collection": "${resource.name}"} }
func aliasParams() map[string]string {
	return map[string]string{"collection": "${record.collection_name}", "alias": "${record.alias_name}"}
}
func pointParams() map[string]string {
	return map[string]string{"collection": "${resource.namespace}", "point": "${resource.name}"}
}

func queryConfig() plugin.QueryEditorConfig {
	return plugin.QueryEditorConfig{
		Language:     "json",
		Label:        "Qdrant query",
		ExecuteLabel: "Query",
		RunningLabel: "Querying...",
		EmptyText:    "Run a Qdrant JSON query. The body is sent to /collections/{collection}/points/query.",
		InitialQuery: `{"query":[0.1,0.2,0.3,0.4],"limit":10,"with_payload":true}`,
		Exportable:   true,
	}
}

func collectionColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Collection", Sortable: true},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Sortable: true, Severities: map[string]plugin.Severity{"green": plugin.SeveritySuccess, "yellow": plugin.SeverityWarn, "red": plugin.SeverityDanger}},
		{Key: "vectors_count", Label: "Vectors", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "points_count", Label: "Points", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "indexed_vectors_count", Label: "Indexed", Type: plugin.ColumnNumber, Sortable: true},
	}
}

func pointColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "id", Label: "ID", Sortable: true},
		{Key: "payload", Label: "Payload", Type: plugin.ColumnJSON},
		{Key: "vector", Label: "Vector", Type: plugin.ColumnJSON},
	}
}

func aliasColumns() []plugin.Column {
	return []plugin.Column{{Key: "alias_name", Label: "Alias", Sortable: true}, {Key: "collection_name", Label: "Collection", Sortable: true}}
}

func snapshotColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Snapshot", Sortable: true},
		{Key: "creation_time", Label: "Created", Type: plugin.ColumnRelativeTime, Sortable: true},
		{Key: "size", Label: "Size", Type: plugin.ColumnBytes, Sortable: true},
	}
}
