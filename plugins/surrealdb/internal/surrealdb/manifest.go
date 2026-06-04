package surrealdb

import (
	"context"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

// Plugin is the stateless SurrealDB plugin singleton.
type Plugin struct{}

const surrealIconSvg = `<svg fill=none height=1728 viewBox="0 0 1486 1728"width=1486 xmlns=http://www.w3.org/2000/svg><path d="M743 454.616L1155.72 682.453V591.107L743 363.799C681.614 397.657 384.945 561.128 330.281 591.107C381.053 619.146 914.244 912.759 1238.33 1091.22V1182.21C1194.28 1206.55 743 1455.02 743 1455.02C619.52 1387.13 370.969 1250.28 247.667 1182.21V909.409L743 1182.21L825.615 1136.72L165.052 773.094V1227.89L743 1546.01C799.963 1514.62 1278.67 1250.99 1320.77 1227.71V1045.9L495.333 591.107L743 454.616ZM165.052 500.113V682.101L990.49 1136.89L742.823 1273.38L330.104 1045.55V1136.89L742.823 1364.2C804.209 1330.34 1100.88 1166.87 1155.54 1136.89C1104.77 1108.85 571.756 815.241 247.667 636.604V545.61C291.716 521.274 743 272.805 743 272.805C866.303 340.874 1114.85 477.717 1238.33 545.61V818.415L743 545.61L660.385 591.107L1320.77 954.906V500.113L743 181.811C685.86 213.377 207.332 477.012 165.052 500.113ZM743 0L0 409.296V1318.7L743 1728L1486 1318.88V409.296L743 0ZM1403.21 1273.21L743 1637.01L82.6145 1273.21V454.793L743 90.9938L1403.39 454.793L1403.21 1273.21Z"fill=white /><path d="M743 454.616L1155.72 682.453V591.107L743 363.799C681.614 397.657 384.945 561.128 330.281 591.107C381.053 619.146 914.244 912.759 1238.33 1091.22V1182.21C1194.28 1206.55 743 1455.02 743 1455.02C619.52 1387.13 370.969 1250.28 247.667 1182.21V909.409L743 1182.21L825.615 1136.72L165.052 773.094V1227.89L743 1546.01C799.963 1514.62 1278.67 1250.99 1320.77 1227.71V1045.9L495.333 591.107L743 454.616ZM165.052 500.113V682.101L990.49 1136.89L742.823 1273.38L330.104 1045.55V1136.89L742.823 1364.2C804.209 1330.34 1100.88 1166.87 1155.54 1136.89C1104.77 1108.85 571.756 815.241 247.667 636.604V545.61C291.716 521.274 743 272.805 743 272.805C866.303 340.874 1114.85 477.717 1238.33 545.61V818.415L743 545.61L660.385 591.107L1320.77 954.906V500.113L743 181.811C685.86 213.377 207.332 477.012 165.052 500.113ZM743 0L0 409.296V1318.7L743 1728L1486 1318.88V409.296L743 0ZM1403.21 1273.21L743 1637.01L82.6145 1273.21V454.793L743 90.9938L1403.39 454.793L1403.21 1273.21Z"fill=url(#paint0_linear_29402_5666) /><defs><linearGradient gradientUnits=userSpaceOnUse id=paint0_linear_29402_5666 x1=292.729 x2=1399.49 y1=-330.5 y2=1931.47><stop offset=0.274038 stop-color=#D254FE /><stop offset=1 stop-color=#3B0CA6 /></linearGradient></defs></svg>`

func icon(name string) plugin.Icon { return plugin.Icon{Type: plugin.IconLucide, Value: name} }

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:  plugin.CurrentAPIVersion,
		Name:        "surrealdb",
		Version:     "0.2.0",
		Title:       "SurrealDB",
		Description: "Explore, query, and manage a SurrealDB namespace/database.",
		Icon:        plugin.Icon{Type: plugin.IconSVG, Value: surrealIconSvg},
		Category:    plugin.CategoryDatabases,
		Layout:      plugin.LayoutSidebarTree,

		Config: configSchema(),

		SupportedTransports: []plugin.Transport{plugin.TransportDirect, plugin.TransportAgent},
		Agent: &plugin.AgentProfile{
			Proxy: plugin.ProxyTarget{
				Mode:    plugin.AgentTCP,
				Address: "127.0.0.1:8000",
				Risk:    plugin.RiskPrivileged,
			},
			Install: []plugin.InstallArtifact{{
				Label:    "Docker",
				Kind:     "docker",
				Template: "docker run -d --network host shellcn/agent --connect {{.ConnectURL}} --token {{.Token}}",
			}},
		},

		Tree: []plugin.TreeGroup{
			{Key: "database", Label: "Database", Icon: icon("database"), Ref: &plugin.ResourceRef{Kind: "database", Name: "Database", UID: "database"}},
			{Key: "tables", Label: "Tables", Icon: icon("table-2"), Source: plugin.DataSource{RouteID: "surrealdb.tree.tables"}, Ref: &plugin.ResourceRef{Kind: "database", Name: "Tables", UID: "database"}},
		},
		Resources: resources(),
		Actions:   actions(),

		HeaderActions: []string{"surrealdb.open"},

		Streams: []plugin.Stream{
			{ID: "surrealdb.query", Kind: plugin.StreamLogs, RouteID: "surrealdb.query"},
			{ID: "surrealdb.repl", Kind: plugin.StreamTerminal, RouteID: "surrealdb.repl"},
			{ID: "surrealdb.table.tail", Kind: plugin.StreamLogs, RouteID: "surrealdb.table.tail"},
		},

		Recording: []plugin.RecordingCapability{{
			Class:         plugin.RecordingTerminal,
			Formats:       []plugin.RecordingFormat{plugin.FormatAsciicastV2},
			StreamIDs:     []string{"surrealdb.repl"},
			Authoritative: true,
			InputCapture:  true,
		}},
	}
}

func (p *Plugin) Routes() []plugin.Route { return routes() }

func (p *Plugin) Connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	return newSession(ctx, cfg)
}

func tableColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Name", Sortable: true},
		{Key: "mode", Label: "Mode", Type: plugin.ColumnBadge, Severities: map[string]plugin.Severity{
			"schemafull": plugin.SeverityInfo, "schemaless": plugin.SeveritySecondary,
		}},
		{Key: "records", Label: "Records", Type: plugin.ColumnNumber},
	}
}

func defColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Name", Sortable: true},
		{Key: "definition", Label: "Definition"},
	}
}

func tableParam() map[string]string { return map[string]string{"table": "${resource.uid}"} }

func queryConfig(initial string) plugin.QueryEditorConfig {
	return plugin.QueryEditorConfig{Language: "sql", Label: "SurrealQL", InitialQuery: initial, Exportable: true}
}

func resources() []plugin.ResourceType {
	res := []plugin.ResourceType{databaseResource(), tableResource(), recordResource()}
	for _, k := range objectKindSpecs {
		res = append(res, objectResource(k))
	}
	return res
}

// databaseResource is the connection-level view: overview, table and DB-object
// lists, and a console — opened from the tree roots.
func databaseResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "database", Title: "Database",
		List:    plugin.DataSource{RouteID: "surrealdb.tables.list"},
		Columns: tableColumns(),
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "Database"},
			DefaultTab: "overview",
			Tabs: append([]plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: "surrealdb.db.overview"}},
				{Key: "tables", Label: "Tables", Icon: icon("table-2"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "surrealdb.tables.list"}, Config: plugin.TableConfig{
					Columns: tableColumns(), ActionIDs: []string{"surrealdb.table.define"}, RowActionIDs: []string{"surrealdb.table.remove"},
					EmptyText: "No tables yet — define one to get started.",
				}},
			}, append(objectPanels(),
				plugin.Panel{Key: "query", Label: "Query", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "surrealdb.query", Method: plugin.MethodWS}, Config: queryConfig("INFO FOR DB;")},
				plugin.Panel{Key: "repl", Label: "REPL", Icon: icon("terminal"), Type: plugin.PanelTerminal, Source: &plugin.DataSource{RouteID: "surrealdb.repl"}, Config: plugin.TerminalConfig{Zoom: true, Search: true}},
			)...),
		},
	}
}

func tableResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "table", Title: "Tables",
		List:    plugin.DataSource{RouteID: "surrealdb.tables.list"},
		Columns: tableColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{"surrealdb.table.define"},
			Row:     []string{"surrealdb.table.remove"},
			Detail:  []string{"surrealdb.table.remove"},
		},
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.name}"},
			DefaultTab: "data",
			Tabs: []plugin.Panel{
				{Key: "data", Label: "Data", Icon: icon("rows-3"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "surrealdb.records.list", Params: tableParam()}, Config: plugin.TableConfig{
					ColumnsSource: &plugin.DataSource{RouteID: "surrealdb.table.columns", Params: tableParam()},
					Editable:      true,
					RowKey:        []string{"id"},
					Insert:        &plugin.DataSource{RouteID: "surrealdb.record.insert", Params: tableParam()},
					Update:        &plugin.DataSource{RouteID: "surrealdb.record.update", Params: tableParam()},
					Delete:        &plugin.DataSource{RouteID: "surrealdb.record.delete", Params: tableParam()},
					ActionIDs:     []string{"surrealdb.record.create"},
					HiddenColumns: []string{"record", "ref"},
					Exportable:    true,
					EmptyText:     "No records yet.",
				}},
				{Key: "fields", Label: "Fields", Icon: icon("list-tree"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "surrealdb.table.fields", Params: tableParam()}, Config: plugin.TableConfig{
					Columns: []plugin.Column{
						{Key: "name", Label: "Name", Sortable: true},
						{Key: "type", Label: "Type"},
						{Key: "definition", Label: "Definition"},
					},
					ActionIDs: []string{"surrealdb.field.define"}, RowActionIDs: []string{"surrealdb.field.remove"},
					EmptyText: "No field definitions (schemaless).",
				}},
				{Key: "indexes", Label: "Indexes", Icon: icon("key-round"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "surrealdb.table.indexes", Params: tableParam()}, Config: plugin.TableConfig{
					Columns:   defColumns(),
					ActionIDs: []string{"surrealdb.index.define"}, RowActionIDs: []string{"surrealdb.index.remove"},
					EmptyText: "No indexes.",
				}},
				{Key: "events", Label: "Events", Icon: icon("zap"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "surrealdb.table.events", Params: tableParam()}, Config: plugin.TableConfig{
					Columns: defColumns(), EmptyText: "No events.",
				}},
				{Key: "definition", Label: "Definition", Icon: icon("file-code"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: "surrealdb.table.definition", Params: tableParam()}},
				{Key: "tail", Label: "Live tail", Icon: icon("activity"), Type: plugin.PanelLogStream, Source: &plugin.DataSource{RouteID: "surrealdb.table.tail", Params: tableParam()}},
				{Key: "query", Label: "Query", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: "surrealdb.query", Method: plugin.MethodWS}, Config: queryConfig("SELECT * FROM ${resource.name} LIMIT 100;")},
			},
		},
	}
}

func recordResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "record", Title: "Records",
		List:    plugin.DataSource{RouteID: "surrealdb.records.list", Params: map[string]string{"table": "${resource.scope}"}},
		Columns: []plugin.Column{{Key: "id", Label: "ID", Sortable: true}},
		Actions: plugin.ResourceActions{Detail: []string{"surrealdb.record.edit", "surrealdb.record.remove"}},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "document", Label: "Document", Icon: icon("file-json"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: "surrealdb.record.get", Params: map[string]string{"id": "${resource.uid}"}}},
			},
		},
	}
}

// objectKindSpecs drives the DB-object resources/panels/actions (functions,
// params, analyzers, users) from one table.
var objectKindSpecs = []struct {
	kind, section, title, icon string
}{
	{"function", "functions", "Functions", "function-square"},
	{"param", "params", "Params", "variable"},
	{"analyzer", "analyzers", "Analyzers", "scan-text"},
	{"user", "users", "Users", "users"},
}

func objectPanels() []plugin.Panel {
	panels := make([]plugin.Panel, 0, len(objectKindSpecs))
	for _, k := range objectKindSpecs {
		panels = append(panels, plugin.Panel{
			Key: k.section, Label: k.title, Icon: icon(k.icon), Type: plugin.PanelTable,
			Source: &plugin.DataSource{RouteID: "surrealdb." + k.section + ".list"},
			Config: plugin.TableConfig{
				Columns:      defColumns(),
				RowActionIDs: []string{"surrealdb." + k.kind + ".remove"},
				EmptyText:    "None.",
			},
		})
	}
	return panels
}

func objectResource(k struct{ kind, section, title, icon string }) plugin.ResourceType {
	return plugin.ResourceType{
		Kind: k.kind, Title: k.title,
		List:    plugin.DataSource{RouteID: "surrealdb." + k.section + ".list"},
		Columns: defColumns(),
		Actions: plugin.ResourceActions{Row: []string{"surrealdb." + k.kind + ".remove"}, Detail: []string{"surrealdb." + k.kind + ".remove"}},
		Detail: plugin.DetailView{
			Header: plugin.HeaderSpec{Title: "${resource.name}"},
			Tabs: []plugin.Panel{
				{Key: "definition", Label: "Definition", Icon: icon("file-code"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: "surrealdb.object.definition", Params: map[string]string{"kind": k.kind, "name": "${resource.name}"}}},
			},
		},
	}
}

func actions() []plugin.Action {
	acts := []plugin.Action{
		{ID: "surrealdb.table.define", Label: "Define table", Icon: icon("plus"), RouteID: "surrealdb.table.define"},
		{
			ID: "surrealdb.table.remove", Label: "Remove", Icon: icon("trash-2"), RouteID: "surrealdb.table.remove",
			Params: map[string]string{"table": "${resource.uid}"}, Confirm: true,
			ConfirmText: "Remove this table and all its records?",
			OnSuccess:   &plugin.ActionSuccess{Navigate: plugin.NavigateList},
		},
		{
			ID: "surrealdb.record.create", Label: "New record", Icon: icon("plus"), RouteID: "surrealdb.record.create",
			Params: map[string]string{"table": "${resource.uid}"},
		},
		{
			ID: "surrealdb.record.edit", Label: "Edit", Icon: icon("pencil"), RouteID: "surrealdb.record.edit",
			Params: map[string]string{"id": "${resource.uid}"},
		},
		{
			ID: "surrealdb.record.remove", Label: "Delete", Icon: icon("trash-2"), RouteID: "surrealdb.record.remove",
			Params: map[string]string{"id": "${resource.uid}"}, Confirm: true, ConfirmText: "Delete this record?",
			OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList},
		},
		{
			ID: "surrealdb.field.define", Label: "Define field", Icon: icon("plus"), RouteID: "surrealdb.field.define",
			Params: map[string]string{"table": "${resource.uid}"},
		},
		{
			ID: "surrealdb.field.remove", Label: "Remove", Icon: icon("trash-2"), RouteID: "surrealdb.field.remove",
			Params: map[string]string{"table": "${resource.scope}", "name": "${resource.name}"}, Confirm: true,
			ConfirmText: "Remove this field definition?",
		},
		{
			ID: "surrealdb.index.define", Label: "Define index", Icon: icon("plus"), RouteID: "surrealdb.index.define",
			Params: map[string]string{"table": "${resource.uid}"},
		},
		{
			ID: "surrealdb.index.remove", Label: "Remove", Icon: icon("trash-2"), RouteID: "surrealdb.index.remove",
			Params: map[string]string{"table": "${resource.scope}", "name": "${resource.name}"}, Confirm: true,
			ConfirmText: "Remove this index?",
		},
		{
			ID: "surrealdb.open", Label: "Open in browser", Icon: icon("external-link"),
			RouteID: "surrealdb.proxy.url", Open: plugin.OpenURL,
		},
	}
	for _, k := range objectKindSpecs {
		acts = append(acts, plugin.Action{
			ID: "surrealdb." + k.kind + ".remove", Label: "Remove", Icon: icon("trash-2"), RouteID: "surrealdb.object.remove",
			Params: map[string]string{"kind": k.kind, "name": "${resource.name}"}, Confirm: true,
			ConfirmText: "Remove this " + k.kind + "?",
		})
	}
	return acts
}
