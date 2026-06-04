package influxdb

import "github.com/charlesng35/shellcn/sdk/plugin"

func icon(name string) plugin.Icon { return plugin.Icon{Type: plugin.IconLucide, Value: name} }

func rid(suffix string) string { return protocolName + "." + suffix }

func tree() []plugin.TreeGroup {
	return []plugin.TreeGroup{
		{Key: "status", Label: "Status", Icon: icon("activity"), Source: plugin.DataSource{RouteID: rid("status.tree")}, ResourceKind: "status"},
		{Key: "namespaces", Label: "Databases / Buckets", Icon: icon("database"), Source: plugin.DataSource{RouteID: rid("namespaces.tree")}, ResourceKind: "namespace"},
	}
}

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		{
			Kind: "status", Title: "Status", List: plugin.DataSource{RouteID: rid("status.list")},
			Columns: statusColumns(),
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: rid("status.read"), Params: statusParams()}},
				{Key: "query", Label: "Query", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: rid("query"), Method: plugin.MethodWS}, Config: queryConfig()},
			}},
		},
		{
			Kind: "namespace", Title: "Databases / Buckets", List: plugin.DataSource{RouteID: rid("namespaces.list")},
			Columns: namespaceColumns(),
			Actions: plugin.ResourceActions{
				Toolbar: []string{rid("namespace.create")},
				Row:     []string{rid("namespace.delete")},
				Detail:  []string{rid("write.namespace"), rid("namespace.delete")},
			},
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: rid("namespace.read"), Params: namespaceParams()}},
				{Key: "measurements", Label: "Measurements", Icon: icon("table-2"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("measurements.list"), Params: namespaceParams()}, Config: plugin.TableConfig{Columns: measurementColumns(), Exportable: true}},
				{Key: "query", Label: "Query", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: rid("query"), Method: plugin.MethodWS, Params: namespaceParams()}, Config: queryConfig()},
			}},
		},
		{
			Kind: "measurement", Title: "Measurements", List: plugin.DataSource{RouteID: rid("measurements.list")},
			Columns: measurementColumns(),
			Actions: plugin.ResourceActions{
				Detail: []string{rid("write.measurement")},
			},
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.namespace}.${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "data", Label: "Data", Icon: icon("table-properties"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("measurement.rows"), Params: measurementParams()}, Config: plugin.TableConfig{Exportable: true, EmptyText: "No points in the selected range."}},
				{Key: "fields", Label: "Fields", Icon: icon("columns-3"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("measurement.fields"), Params: measurementParams()}, Config: plugin.TableConfig{Columns: fieldColumns(), Exportable: true}},
				{Key: "tags", Label: "Tags", Icon: icon("tags"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("measurement.tags"), Params: measurementParams()}, Config: plugin.TableConfig{Columns: tagColumns(), Exportable: true}},
				{Key: "query", Label: "Query", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: rid("query"), Method: plugin.MethodWS, Params: namespaceParams()}, Config: measurementQueryConfig()},
			}},
		},
	}
}

func actions() []plugin.Action {
	return []plugin.Action{
		{ID: rid("write.namespace"), Label: "Write line protocol", Icon: icon("send"), RouteID: rid("write"), Params: namespaceParams(), Confirm: true, ConfirmText: "Write line protocol to this InfluxDB container?"},
		{ID: rid("write.measurement"), Label: "Write line protocol", Icon: icon("send"), RouteID: rid("write"), Params: map[string]string{"namespace": "${resource.namespace}"}, Confirm: true, ConfirmText: "Write line protocol to this InfluxDB container?"},
		{ID: rid("namespace.create"), Label: "Create database / bucket", Icon: icon("plus"), RouteID: rid("namespace.create")},
		{ID: rid("namespace.delete"), Label: "Delete", Icon: icon("trash-2"), RouteID: rid("namespace.delete"), Params: namespaceParams(), Confirm: true, ConfirmText: "Delete this database / bucket and all of its data?"},
	}
}

func queryConfig() plugin.QueryEditorConfig {
	return plugin.QueryEditorConfig{
		Language:          "plaintext",
		Label:             "Query",
		ExecuteLabel:      "Run",
		RunningLabel:      "Running...",
		EmptyText:         "Run a query to see results.",
		CompletionRouteID: rid("completion"),
		Exportable:        true,
	}
}

func measurementQueryConfig() plugin.QueryEditorConfig {
	return queryConfig()
}

func statusParams() map[string]string    { return map[string]string{"status": "${resource.name}"} }
func namespaceParams() map[string]string { return map[string]string{"namespace": "${resource.name}"} }
func measurementParams() map[string]string {
	return map[string]string{"namespace": "${resource.namespace}", "measurement": "${resource.name}"}
}

func statusColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Status", Sortable: true}, {Key: "description", Label: "Description"}}
}

func namespaceColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Name", Sortable: true},
		{Key: "kind", Label: "Kind", Type: plugin.ColumnBadge, Sortable: true},
		{Key: "retention", Label: "Retention"},
		{Key: "created", Label: "Created", Type: plugin.ColumnDateTime, Sortable: true},
	}
}

func measurementColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Measurement / Table", Sortable: true},
		{Key: "namespace", Label: "Database / Bucket", Sortable: true},
		{Key: "type", Label: "Type", Type: plugin.ColumnBadge, Sortable: true},
	}
}

func fieldColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Field", Sortable: true}, {Key: "type", Label: "Type", Sortable: true}}
}

func tagColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Tag", Sortable: true}, {Key: "type", Label: "Type", Sortable: true}}
}
