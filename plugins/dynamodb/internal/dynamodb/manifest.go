package dynamodb

import "github.com/charlesng35/shellcn/sdk/plugin"

// statusSeverities colors table/index/backup status badges by value.
var statusSeverities = map[string]plugin.Severity{
	"active": plugin.SeveritySuccess, "available": plugin.SeveritySuccess, "enabled": plugin.SeveritySuccess,
	"creating": plugin.SeverityWarn, "updating": plugin.SeverityWarn, "archiving": plugin.SeverityWarn,
	"deleting": plugin.SeverityDanger, "inaccessible_encryption_credentials": plugin.SeverityDanger,
	"archived": plugin.SeveritySecondary, "deleted": plugin.SeveritySecondary, "disabled": plugin.SeveritySecondary,
}

func objectDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{RawToggle: true}
}

func tree() []plugin.TreeGroup {
	return []plugin.TreeGroup{
		{Key: "tables", Label: "Tables", Icon: icon("table-2"), Source: plugin.DataSource{RouteID: rid("tables.tree")}, ResourceKind: "table"},
		{Key: "backups", Label: "Backups", Icon: icon("archive"), ResourceKind: "backup"},
	}
}

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		tableResource(),
		indexResource(),
		itemResource(),
		backupResource(),
	}
}

func tableResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "table", Title: "Tables",
		List:    plugin.DataSource{RouteID: rid("tables.list")},
		Columns: tableColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{rid("table.create")},
			Row:     []string{rid("table.delete")},
			Detail:  []string{rid("backup.create"), rid("ttl.update"), rid("table.delete")},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("table.read"), Params: tableParams()}, Config: objectDetailConfig()},
				{Key: "items", Label: "Items", Icon: icon("braces"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("items.list"), Params: tableParams()}, Config: plugin.TableConfig{Exportable: true, ActionIDs: []string{rid("item.put")}, RowActionIDs: []string{rid("item.delete")}, EmptyText: "No items found."}},
				{Key: "indexes", Label: "Indexes", Icon: icon("list-tree"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("indexes.list"), Params: tableParams()}, Config: plugin.TableConfig{Columns: indexColumns(), Exportable: true, ActionIDs: []string{rid("gsi.create")}, RowActionIDs: []string{rid("gsi.delete")}}},
				{Key: "capacity", Label: "Capacity", Icon: icon("gauge"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("table.capacity"), Params: tableParams()}, Config: objectDetailConfig()},
				{Key: "ttl", Label: "TTL", Icon: icon("timer"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("ttl.read"), Params: tableParams()}, Config: objectDetailConfig()},
				{Key: "tags", Label: "Tags", Icon: icon("tags"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("tags.list"), Params: tableParams()}, Config: plugin.TableConfig{Columns: tagColumns(), Exportable: true}},
				{Key: "backups", Label: "Backups", Icon: icon("archive"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("backups.list"), Params: tableParams()}, Config: plugin.TableConfig{Columns: backupColumns(), Exportable: true, ActionIDs: []string{rid("backup.create")}, RowActionIDs: []string{rid("backup.delete")}}},
				{Key: "partiql", Label: "PartiQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: rid("partiql"), Method: plugin.MethodWS, Params: tableParams()}, Config: queryConfig(`SELECT * FROM "${resource.name}"`)},
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
			Row:    []string{rid("gsi.delete")},
			Detail: []string{rid("gsi.delete")},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.namespace}.${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("index.read"), Params: indexParams()}, Config: objectDetailConfig()},
			},
		},
	}
}

func itemResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "item", Title: "Items",
		List: plugin.DataSource{RouteID: rid("items.list")},
		Actions: plugin.ResourceActions{
			Row:    []string{rid("item.delete")},
			Detail: []string{rid("item.delete")},
		},
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.name}"},
			DefaultTab: "editor",
			Tabs: []plugin.Panel{
				{Key: "document", Label: "Item", Icon: icon("braces"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: rid("item.read"), Params: map[string]string{"id": "${resource.uid}"}}},
				{Key: "editor", Label: "Editor", Icon: icon("code"), Type: plugin.PanelCodeEditor, Source: &plugin.DataSource{RouteID: rid("item.read"), Params: map[string]string{"id": "${resource.uid}"}}, Config: plugin.CodeEditorConfig{Language: "json", SaveRouteID: rid("item.update"), SaveMethod: plugin.MethodPut, SaveParams: map[string]string{"id": "${resource.uid}"}}},
			},
		},
	}
}

func backupResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "backup", Title: "Backups",
		List:    plugin.DataSource{RouteID: rid("backups.list")},
		Columns: backupColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{rid("backup.delete")},
			Detail: []string{rid("backup.delete")},
		},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("backup.read"), Params: map[string]string{"backup": "${resource.uid}"}}, Config: objectDetailConfig()},
			},
		},
	}
}

func actions() []plugin.Action {
	return []plugin.Action{
		{ID: rid("table.create"), Label: "Create table", Icon: icon("plus"), RouteID: rid("table.create"), Confirm: true},
		{ID: rid("table.delete"), Label: "Delete table", Icon: icon("trash-2"), RouteID: rid("table.delete"), Params: tableParams(), Confirm: true, ConfirmText: "Delete this DynamoDB table and all items?"},
		{ID: rid("item.put"), Label: "Put item", Icon: icon("plus"), RouteID: rid("item.put"), Params: tableParams(), Open: plugin.OpenDialog, Panel: plugin.PanelCodeEditor, Config: plugin.CodeEditorConfig{Language: "json", InitialContent: "{\n  \"pk\": \"example\"\n}", SaveRouteID: rid("item.put"), SaveMethod: plugin.MethodPost, SaveParams: tableParams(), SaveBodyKey: "item"}, OnSuccess: &plugin.ActionSuccess{SelectTab: "items"}},
		{ID: rid("item.delete"), Label: "Delete item", Icon: icon("trash"), RouteID: rid("item.delete"), Params: map[string]string{"id": "${resource.uid}"}, Confirm: true, ConfirmText: "Delete this item?"},
		{ID: rid("gsi.create"), Label: "Create GSI", Icon: icon("plus"), RouteID: rid("gsi.create"), Params: tableParams(), Confirm: true, OnSuccess: &plugin.ActionSuccess{SelectTab: "indexes"}},
		{ID: rid("gsi.delete"), Label: "Delete GSI", Icon: icon("trash"), RouteID: rid("gsi.delete"), Params: indexParams(), Confirm: true, ConfirmText: "Delete this global secondary index?"},
		{ID: rid("backup.create"), Label: "Create backup", Icon: icon("archive"), RouteID: rid("backup.create"), Params: tableParams(), Confirm: true, OnSuccess: &plugin.ActionSuccess{SelectTab: "backups"}},
		{ID: rid("backup.delete"), Label: "Delete backup", Icon: icon("trash"), RouteID: rid("backup.delete"), Params: map[string]string{"backup": "${resource.uid}"}, Confirm: true, ConfirmText: "Delete this backup?"},
		{ID: rid("ttl.update"), Label: "Update TTL", Icon: icon("timer-reset"), RouteID: rid("ttl.update"), Params: tableParams(), Confirm: true, OnSuccess: &plugin.ActionSuccess{SelectTab: "ttl"}},
	}
}

func queryConfig(initial string) plugin.QueryEditorConfig {
	return plugin.QueryEditorConfig{
		Language:          "sql",
		Label:             "PartiQL",
		ExecuteLabel:      "Run",
		RunningLabel:      "Running...",
		EmptyText:         "Run a PartiQL statement to see results.",
		InitialQuery:      initial,
		CompletionRouteID: rid("completion"),
		Exportable:        true,
	}
}

func tableParams() map[string]string { return map[string]string{"table": "${resource.name}"} }
func indexParams() map[string]string {
	return map[string]string{"table": "${resource.namespace}", "index": "${resource.name}"}
}

func tableColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Table", Sortable: true},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Sortable: true, Severities: statusSeverities},
		{Key: "billing_mode", Label: "Billing", Type: plugin.ColumnBadge, Sortable: true},
		{Key: "items", Label: "Items", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "size", Label: "Size", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "created", Label: "Created", Type: plugin.ColumnRelativeTime, Sortable: true},
	}
}

func indexColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Index", Sortable: true},
		{Key: "table", Label: "Table", Sortable: true},
		{Key: "kind", Label: "Kind", Type: plugin.ColumnBadge, Sortable: true},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Sortable: true, Severities: statusSeverities},
		{Key: "key_schema", Label: "Key schema"},
		{Key: "projection", Label: "Projection", Type: plugin.ColumnBadge},
		{Key: "items", Label: "Items", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "size", Label: "Size", Type: plugin.ColumnBytes, Sortable: true},
	}
}

func backupColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Backup", Sortable: true},
		{Key: "table", Label: "Table", Sortable: true},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Sortable: true, Severities: statusSeverities},
		{Key: "size", Label: "Size", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "created", Label: "Created", Type: plugin.ColumnRelativeTime, Sortable: true},
	}
}

func tagColumns() []plugin.Column {
	return []plugin.Column{{Key: "key", Label: "Key", Sortable: true}, {Key: "value", Label: "Value"}}
}
