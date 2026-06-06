package jaeger

import "github.com/charlesng35/shellcn/sdk/plugin"

func icon(name string) plugin.Icon { return plugin.Icon{Type: plugin.IconLucide, Value: name} }

func rid(suffix string) string { return protocolName + "." + suffix }

func objectDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{RawToggle: true}
}

func tree() []plugin.TreeGroup {
	return []plugin.TreeGroup{
		{Key: "overview", Label: "Overview", Icon: icon("layout-dashboard"), Ref: &plugin.ResourceRef{Kind: "server", Name: "Jaeger", UID: "server"}},
		{Key: "services", Label: "Services", Icon: icon("boxes"), Source: plugin.DataSource{RouteID: rid("services.tree")}, ResourceKind: "service"},
	}
}

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		{
			Kind: "server", Title: "Jaeger", List: plugin.DataSource{RouteID: rid("overview")},
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("layout-dashboard"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("overview")}, Config: objectDetailConfig()},
				{Key: "services", Label: "Services", Icon: icon("boxes"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("services.list")}, Config: plugin.TableConfig{Columns: serviceColumns(), Exportable: true}},
			}},
		},
		{
			Kind: "service", Title: "Services", List: plugin.DataSource{RouteID: rid("services.list")},
			Columns: serviceColumns(),
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "operations", Label: "Operations", Icon: icon("workflow"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("operations.list"), Params: serviceParams()}, Config: plugin.TableConfig{Columns: operationColumns(), Exportable: true}},
				{Key: "traces", Label: "Traces", Icon: icon("git-branch"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("traces.list"), Params: serviceParams()}, Config: plugin.TableConfig{Columns: traceColumns(), Exportable: true}},
			}},
		},
		{
			Kind: "trace", Title: "Traces", List: plugin.DataSource{RouteID: rid("traces.list")},
			Columns: traceColumns(),
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "overview", Label: "Trace", Icon: icon("git-branch"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: rid("trace.read"), Params: traceParams()}},
				{Key: "spans", Label: "Spans", Icon: icon("list-tree"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("spans.list"), Params: traceParams()}, Config: plugin.TableConfig{Columns: spanColumns(), Exportable: true}},
			}},
		},
	}
}

func serviceParams() map[string]string { return map[string]string{"service": "${resource.name}"} }
func traceParams() map[string]string   { return map[string]string{"trace": "${resource.name}"} }

func serviceColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Service", Sortable: true}}
}

func operationColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Operation", Sortable: true}, {Key: "spanKind", Label: "Span kind", Sortable: true}}
}

func traceColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "traceID", Label: "Trace ID", Sortable: true},
		{Key: "operationName", Label: "Operation", Sortable: true},
		{Key: "serviceName", Label: "Service", Sortable: true},
		{Key: "duration", Label: "Duration us", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "startTime", Label: "Start", Type: plugin.ColumnDateTime, Sortable: true},
		{Key: "spans", Label: "Spans", Type: plugin.ColumnNumber, Sortable: true},
	}
}

func spanColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "spanID", Label: "Span ID", Sortable: true},
		{Key: "operationName", Label: "Operation", Sortable: true},
		{Key: "serviceName", Label: "Service", Sortable: true},
		{Key: "duration", Label: "Duration us", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "startTime", Label: "Start", Type: plugin.ColumnDateTime, Sortable: true},
		{Key: "tags", Label: "Tags", Type: plugin.ColumnJSON},
	}
}
