package loki

import "github.com/charlesng35/shellcn/sdk/plugin"

func icon(name string) plugin.Icon { return plugin.Icon{Type: plugin.IconLucide, Value: name} }

func rid(suffix string) string { return protocolName + "." + suffix }

func objectDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{RawToggle: true}
}

func tree() []plugin.TreeGroup {
	return []plugin.TreeGroup{
		{Key: "overview", Label: "Overview", Icon: icon("layout-dashboard"), Ref: &plugin.ResourceRef{Kind: "server", Name: "Loki", UID: "server"}},
		{Key: "labels", Label: "Labels", Icon: icon("tag"), Source: plugin.DataSource{RouteID: rid("labels.tree")}, ResourceKind: "label"},
		{Key: "streams", Label: "Streams", Icon: icon("list-tree"), ResourceKind: "stream"},
	}
}

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		{
			Kind: "server", Title: "Loki", List: plugin.DataSource{RouteID: rid("overview")},
			Actions: plugin.ResourceActions{Detail: []string{rid("query.format"), rid("delete.create")}},
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "query", Label: "LogQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: rid("query"), Method: plugin.MethodWS}, Config: queryConfig()},
				{Key: "overview", Label: "Overview", Icon: icon("layout-dashboard"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("overview")}, Config: objectDetailConfig()},
				{Key: "labels", Label: "Labels", Icon: icon("tag"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("labels.list")}, Config: plugin.TableConfig{Columns: labelColumns(), Exportable: true}},
				{Key: "streams", Label: "Streams", Icon: icon("list-tree"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("streams.list")}, Config: plugin.TableConfig{Columns: streamColumns(), Exportable: true}},
				{Key: "stats", Label: "Stats", Icon: icon("chart-no-axes-combined"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("stats.read")}, Config: objectDetailConfig()},
				{Key: "volume", Label: "Volume", Icon: icon("chart-column"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("volume.list")}, Config: plugin.TableConfig{Columns: volumeColumns(), Exportable: true}},
				{Key: "rules", Label: "Rules", Icon: icon("list-checks"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("rules.list")}, Config: plugin.TableConfig{Columns: ruleColumns(), Exportable: true}},
				{Key: "deletes", Label: "Deletes", Icon: icon("eraser"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("deletes.list")}, Config: plugin.TableConfig{Columns: deleteColumns(), RowActionIDs: []string{rid("delete.cancel")}, Exportable: true}},
			}},
		},
		{
			Kind: "label", Title: "Labels", List: plugin.DataSource{RouteID: rid("labels.list")},
			Columns: labelColumns(),
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "values", Label: "Values", Icon: icon("tags"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("label.values"), Params: labelParams()}, Config: plugin.TableConfig{Columns: valueColumns(), Exportable: true}},
			}},
		},
		{
			Kind: "stream", Title: "Streams", List: plugin.DataSource{RouteID: rid("streams.list")},
			Columns: streamColumns(),
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "logs", Label: "Logs", Icon: icon("file-text"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("stream.logs"), Params: streamParams()}, Config: plugin.TableConfig{Columns: logColumns(), Exportable: true}},
			}},
		},
	}
}

func actions() []plugin.Action {
	return []plugin.Action{
		{ID: rid("query.format"), Label: "Format LogQL", Icon: icon("wand-sparkles"), RouteID: rid("query.format")},
		{ID: rid("delete.create"), Label: "Delete logs", Icon: icon("eraser"), RouteID: rid("delete.create"), Confirm: true, ConfirmText: "Schedule a Loki delete request for matching logs?"},
		{ID: rid("delete.cancel"), Label: "Cancel", Icon: icon("circle-x"), RouteID: rid("delete.cancel"), Params: map[string]string{"request": "${resource.uid}"}, Confirm: true, ConfirmText: "Cancel this Loki delete request?"},
	}
}

func queryConfig() plugin.QueryEditorConfig {
	return plugin.QueryEditorConfig{
		Language:     "plaintext",
		Label:        "LogQL",
		ExecuteLabel: "Query",
		RunningLabel: "Querying...",
		EmptyText:    "Run a LogQL range query. You can also send JSON with query, since, limit, and direction.",
		InitialQuery: `{job=~".+"}`,
		Exportable:   true,
	}
}

func labelParams() map[string]string  { return map[string]string{"label": "${resource.name}"} }
func streamParams() map[string]string { return map[string]string{"stream": "${resource.uid}"} }

func labelColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Label", Sortable: true}}
}

func valueColumns() []plugin.Column {
	return []plugin.Column{{Key: "value", Label: "Value", Sortable: true}}
}

func streamColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Stream", Sortable: true}, {Key: "labels", Label: "Labels", Type: plugin.ColumnJSON}}
}

func logColumns() []plugin.Column {
	return []plugin.Column{{Key: "timestamp", Label: "Time", Type: plugin.ColumnRelativeTime, Sortable: true}, {Key: "line", Label: "Line"}, {Key: "labels", Label: "Labels", Type: plugin.ColumnJSON}}
}

func volumeColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "metric", Label: "Labels", Type: plugin.ColumnJSON},
		{Key: "value", Label: "Bytes", Type: plugin.ColumnBytes, Sortable: true},
	}
}

func ruleColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "namespace", Label: "Namespace", Sortable: true},
		{Key: "group", Label: "Group", Sortable: true},
		{Key: "name", Label: "Rule", Sortable: true},
		{Key: "type", Label: "Type", Sortable: true},
		{Key: "query", Label: "Query"},
	}
}

func deleteColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "request_id", Label: "Request ID", Sortable: true},
		{Key: "query", Label: "Query"},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Sortable: true},
		{Key: "start_time", Label: "Start", Type: plugin.ColumnRelativeTime, Sortable: true},
		{Key: "end_time", Label: "End", Type: plugin.ColumnRelativeTime, Sortable: true},
	}
}
