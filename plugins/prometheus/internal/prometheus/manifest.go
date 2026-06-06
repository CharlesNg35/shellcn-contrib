package prometheus

import "github.com/charlesng35/shellcn/sdk/plugin"

// Badge color maps: a lower-cased cell value to its severity.
var (
	healthSeverities = map[string]plugin.Severity{
		"up": plugin.SeveritySuccess, "ok": plugin.SeveritySuccess,
		"down": plugin.SeverityDanger, "err": plugin.SeverityDanger, "error": plugin.SeverityDanger,
		"unknown": plugin.SeveritySecondary,
	}
	targetStateSeverities = map[string]plugin.Severity{
		"active": plugin.SeveritySuccess, "dropped": plugin.SeveritySecondary,
	}
	alertStateSeverities = map[string]plugin.Severity{
		"firing": plugin.SeverityDanger, "pending": plugin.SeverityWarn,
		"inactive": plugin.SeveritySecondary, "ok": plugin.SeveritySuccess,
	}
)

func icon(name string) plugin.Icon { return plugin.Icon{Type: plugin.IconLucide, Value: name} }

func rid(suffix string) string { return protocolName + "." + suffix }

func objectDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{RawToggle: true}
}

func tree() []plugin.TreeGroup {
	return []plugin.TreeGroup{
		{Key: "overview", Label: "Overview", Icon: icon("layout-dashboard"), Ref: &plugin.ResourceRef{Kind: "server", Name: "Prometheus", UID: "server"}},
		{Key: "status", Label: "Status", Icon: icon("activity"), Source: plugin.DataSource{RouteID: rid("status.tree")}, ResourceKind: "status"},
		{Key: "targets", Label: "Targets", Icon: icon("crosshair"), Source: plugin.DataSource{RouteID: rid("targets.tree")}, ResourceKind: "target"},
		{Key: "alerts", Label: "Alerts", Icon: icon("bell"), Source: plugin.DataSource{RouteID: rid("alerts.tree")}, ResourceKind: "alert"},
		{Key: "rules", Label: "Rules", Icon: icon("list-checks"), Source: plugin.DataSource{RouteID: rid("rules.tree")}, ResourceKind: "rule"},
		{Key: "metrics", Label: "Metrics", Icon: icon("chart-line"), Source: plugin.DataSource{RouteID: rid("metrics.tree")}, ResourceKind: "metric"},
		{Key: "labels", Label: "Labels", Icon: icon("tag"), Source: plugin.DataSource{RouteID: rid("labels.tree")}, ResourceKind: "label"},
	}
}

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		{
			Kind: "server", Title: "Prometheus",
			// List satisfies the manifest contract; the server is only ever opened
			// via the Overview tree group's Ref, which goes straight to Detail.
			List:    plugin.DataSource{RouteID: rid("overview")},
			Actions: plugin.ResourceActions{Detail: []string{rid("snapshot.create"), rid("tombstones.clean"), rid("config.reload")}},
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "query", Label: "PromQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: rid("query"), Method: plugin.MethodWS}, Config: queryConfig()},
				{Key: "overview", Label: "Overview", Icon: icon("layout-dashboard"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("overview")}, Config: objectDetailConfig()},
				{Key: "live", Label: "Live", Icon: icon("activity"), Type: plugin.PanelMetrics, Source: &plugin.DataSource{RouteID: rid("metrics.live"), Method: plugin.MethodWS}, Config: liveMetricsConfig()},
				{Key: "targets", Label: "Targets", Icon: icon("crosshair"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("targets.list")}, Config: plugin.TableConfig{Columns: targetColumns(), Exportable: true}},
				{Key: "alerts", Label: "Alerts", Icon: icon("bell"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("alerts.list")}, Config: plugin.TableConfig{Columns: alertColumns(), Exportable: true}},
				{Key: "rules", Label: "Rules", Icon: icon("list-checks"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("rules.list")}, Config: plugin.TableConfig{Columns: ruleColumns(), Exportable: true}},
			}},
		},
		{
			Kind: "status", Title: "Status", List: plugin.DataSource{RouteID: rid("status.list")},
			Columns: statusColumns(),
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("status.read"), Params: statusParams()}, Config: objectDetailConfig()},
			}},
		},
		{
			Kind: "target", Title: "Targets", List: plugin.DataSource{RouteID: rid("targets.list")},
			Columns: targetColumns(),
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("crosshair"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("target.read"), Params: targetParams()}, Config: objectDetailConfig()},
				{Key: "metadata", Label: "Metadata", Icon: icon("database-zap"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("target.metadata"), Params: targetParams()}, Config: plugin.TableConfig{Columns: targetMetadataColumns(), Exportable: true}},
			}},
		},
		{
			Kind: "alert", Title: "Alerts", List: plugin.DataSource{RouteID: rid("alerts.list")},
			Columns: alertColumns(),
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("bell"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("alert.read"), Params: alertParams()}, Config: objectDetailConfig()},
			}},
		},
		{
			Kind: "rule", Title: "Rules", List: plugin.DataSource{RouteID: rid("rules.list")},
			Columns: ruleColumns(),
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("list-checks"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("rule.read"), Params: ruleParams()}, Config: objectDetailConfig()},
			}},
		},
		{
			Kind: "metric", Title: "Metrics", List: plugin.DataSource{RouteID: rid("metrics.list")},
			Columns: metricColumns(),
			Actions: plugin.ResourceActions{Detail: []string{rid("series.delete")}},
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "metadata", Label: "Metadata", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("metric.read"), Params: metricParams()}, Config: objectDetailConfig()},
				{Key: "series", Label: "Series", Icon: icon("list-tree"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("metric.series"), Params: metricParams()}, Config: plugin.TableConfig{Columns: seriesColumns(), Exportable: true}},
				{Key: "query", Label: "PromQL", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: rid("query"), Method: plugin.MethodWS}, Config: metricQueryConfig()},
			}},
		},
		{
			Kind: "label", Title: "Labels", List: plugin.DataSource{RouteID: rid("labels.list")},
			Columns: labelColumns(),
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "values", Label: "Values", Icon: icon("tags"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("label.values"), Params: labelParams()}, Config: plugin.TableConfig{Columns: labelValueColumns(), Exportable: true}},
			}},
		},
	}
}

func actions() []plugin.Action {
	return []plugin.Action{
		{ID: rid("snapshot.create"), Label: "Snapshot", Icon: icon("archive"), RouteID: rid("snapshot.create"), Confirm: true, ConfirmText: "Create a Prometheus TSDB snapshot?"},
		{ID: rid("series.delete"), Label: "Delete series", Icon: icon("eraser"), RouteID: rid("series.delete"), Params: metricParams(), Confirm: true, ConfirmText: "Delete matching Prometheus series data?"},
		{ID: rid("tombstones.clean"), Label: "Clean tombstones", Icon: icon("trash"), RouteID: rid("tombstones.clean"), Confirm: true, ConfirmText: "Clean deleted-series tombstones now?"},
		{ID: rid("config.reload"), Label: "Reload config", Icon: icon("refresh-cw"), RouteID: rid("config.reload"), Confirm: true, ConfirmText: "Reload Prometheus configuration?"},
	}
}

func liveMetricsConfig() plugin.MetricsConfig {
	return plugin.MetricsConfig{
		Stats: []plugin.MetricStat{
			{Key: "targets", Label: "Targets"},
			{Key: "targets_up", Label: "Up"},
			{Key: "head_series", Label: "Head series"},
			{Key: "queries", Label: "Queries"},
		},
		Gauges: []plugin.MetricGauge{{Key: "target_health", Label: "Target health", Unit: "%", Max: 100}},
		Series: []plugin.MetricSeries{
			{Key: "targets_up", Label: "Up targets"},
			{Key: "head_series", Label: "Head series"},
			{Key: "queries", Label: "Queries"},
		},
		History: 120,
	}
}

func queryConfig() plugin.QueryEditorConfig {
	return plugin.QueryEditorConfig{
		Language:          "plaintext",
		Label:             "PromQL",
		ExecuteLabel:      "Query",
		RunningLabel:      "Querying...",
		EmptyText:         "Run a PromQL instant query, or a JSON range query.",
		InitialQuery:      "up",
		CompletionRouteID: rid("completion"),
		Exportable:        true,
	}
}

func metricQueryConfig() plugin.QueryEditorConfig {
	return plugin.QueryEditorConfig{
		Language:          "plaintext",
		Label:             "PromQL",
		ExecuteLabel:      "Query",
		RunningLabel:      "Querying...",
		EmptyText:         "Run a PromQL instant query, or a JSON range query.",
		InitialQuery:      "${resource.name}",
		CompletionRouteID: rid("completion"),
		Exportable:        true,
	}
}

func statusParams() map[string]string { return map[string]string{"status": "${resource.name}"} }
func targetParams() map[string]string { return map[string]string{"target": "${resource.uid}"} }
func alertParams() map[string]string  { return map[string]string{"alert": "${resource.uid}"} }
func ruleParams() map[string]string   { return map[string]string{"rule": "${resource.uid}"} }
func metricParams() map[string]string { return map[string]string{"metric": "${resource.name}"} }
func labelParams() map[string]string  { return map[string]string{"label": "${resource.name}"} }

func statusColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Status", Sortable: true}, {Key: "description", Label: "Description"}}
}

func targetColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "job", Label: "Job", Sortable: true},
		{Key: "instance", Label: "Instance", Sortable: true},
		{Key: "health", Label: "Health", Type: plugin.ColumnBadge, Sortable: true, Severities: healthSeverities},
		{Key: "state", Label: "State", Type: plugin.ColumnBadge, Sortable: true, Severities: targetStateSeverities},
		{Key: "scrapePool", Label: "Pool", Sortable: true},
		{Key: "lastScrape", Label: "Last scrape", Type: plugin.ColumnRelativeTime, Sortable: true},
		{Key: "lastError", Label: "Last error"},
	}
}

func targetMetadataColumns() []plugin.Column {
	return []plugin.Column{{Key: "target", Label: "Target"}, {Key: "metric", Label: "Metric", Sortable: true}, {Key: "type", Label: "Type"}, {Key: "unit", Label: "Unit"}, {Key: "help", Label: "Help"}}
}

func alertColumns() []plugin.Column {
	return []plugin.Column{{Key: "alertname", Label: "Alert", Sortable: true}, {Key: "state", Label: "State", Type: plugin.ColumnBadge, Sortable: true, Severities: alertStateSeverities}, {Key: "activeAt", Label: "Active at", Type: plugin.ColumnRelativeTime, Sortable: true}, {Key: "value", Label: "Value"}, {Key: "labels", Label: "Labels", Type: plugin.ColumnJSON}}
}

func ruleColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Rule", Sortable: true}, {Key: "group", Label: "Group", Sortable: true}, {Key: "type", Label: "Type", Type: plugin.ColumnBadge, Sortable: true}, {Key: "health", Label: "Health", Type: plugin.ColumnBadge, Sortable: true, Severities: healthSeverities}, {Key: "state", Label: "State", Type: plugin.ColumnBadge, Severities: alertStateSeverities}, {Key: "query", Label: "Query"}}
}

func metricColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Metric", Sortable: true}, {Key: "type", Label: "Type", Sortable: true}, {Key: "unit", Label: "Unit"}, {Key: "help", Label: "Help"}}
}

func seriesColumns() []plugin.Column {
	return []plugin.Column{{Key: "metric", Label: "Metric", Sortable: true}, {Key: "labels", Label: "Labels", Type: plugin.ColumnJSON}}
}

func labelColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Label", Sortable: true}}
}

func labelValueColumns() []plugin.Column {
	return []plugin.Column{{Key: "value", Label: "Value", Sortable: true}}
}
