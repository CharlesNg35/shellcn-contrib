package neo4j

import "github.com/charlesng35/shellcn/sdk/plugin"

// statusSeverities colors a database status / index state badge by value.
var statusSeverities = map[string]plugin.Severity{
	"online":   plugin.SeveritySuccess,
	"offline":  plugin.SeveritySecondary,
	"starting": plugin.SeverityWarn, "stopping": plugin.SeverityWarn, "populating": plugin.SeverityWarn, "store copying": plugin.SeverityWarn,
	"failed": plugin.SeverityDanger, "dirty": plugin.SeverityDanger, "quarantined": plugin.SeverityDanger,
}

func objectDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{RawToggle: true}
}

func boolPtr(v bool) *bool {
	return &v
}

func tree() []plugin.TreeGroup {
	return []plugin.TreeGroup{
		{Key: "databases", Label: "Databases", Icon: icon("database"), Source: plugin.DataSource{RouteID: rid("databases.tree")}, ResourceKind: "database"},
		{Key: "labels", Label: "Labels", Icon: icon("tags"), Source: plugin.DataSource{RouteID: rid("labels.tree")}, ResourceKind: "label"},
		{Key: "relationships", Label: "Relationship Types", Icon: icon("git-branch"), Source: plugin.DataSource{RouteID: rid("relationship_types.tree")}, ResourceKind: "relationship_type"},
		{Key: "schema", Label: "Schema", Icon: icon("list-tree"), Source: plugin.DataSource{RouteID: rid("schema.tree")}, ResourceKind: "schema_item"},
	}
}

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		databaseResource(),
		labelResource(),
		relationshipTypeResource(),
		nodeResource(),
		relationshipResource(),
		schemaItemResource(),
	}
}

func databaseResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "database", Title: "Databases",
		List: plugin.DataSource{RouteID: rid("databases.list")},
		Columns: []plugin.Column{
			{Key: "name", Label: "Database", Sortable: true},
			{Key: "current_status", Label: "Status", Type: plugin.ColumnBadge, Sortable: true, Severities: statusSeverities},
			{Key: "requested_status", Label: "Requested", Type: plugin.ColumnBadge, Sortable: true, Severities: statusSeverities},
			{Key: "role", Label: "Role", Type: plugin.ColumnBadge, Sortable: true},
			{Key: "address", Label: "Address"},
		},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("database.overview"), Params: map[string]string{"database": "${resource.uid}"}}, Config: objectDetailConfig()},
			{Key: "graph", Label: "Graph", Icon: icon("workflow"), Type: plugin.PanelGraph, Source: &plugin.DataSource{RouteID: rid("graph"), Params: map[string]string{"database": "${resource.uid}"}}, Config: plugin.GraphConfig{FitView: true, ExpandRouteID: rid("node.graph"), Exportable: boolPtr(true)}},
			{Key: "labels", Label: "Labels", Icon: icon("tags"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("labels.list"), Params: map[string]string{"database": "${resource.uid}"}}, Config: plugin.TableConfig{Columns: labelColumns(), ActionIDs: []string{rid("node.create")}, Exportable: true}},
			{Key: "relationship_types", Label: "Relationship Types", Icon: icon("git-branch"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("relationship_types.list"), Params: map[string]string{"database": "${resource.uid}"}}, Config: plugin.TableConfig{Columns: relationshipTypeColumns(), ActionIDs: []string{rid("relationship.create")}, Exportable: true}},
			{Key: "indexes", Label: "Indexes", Icon: icon("list-tree"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("indexes.list"), Params: map[string]string{"database": "${resource.uid}"}}, Config: plugin.TableConfig{Columns: indexColumns(), ActionIDs: []string{rid("index.create")}, RowActionIDs: []string{rid("schema.drop")}, Exportable: true}},
			{Key: "constraints", Label: "Constraints", Icon: icon("shield-check"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("constraints.list"), Params: map[string]string{"database": "${resource.uid}"}}, Config: plugin.TableConfig{Columns: constraintColumns(), ActionIDs: []string{rid("constraint.create")}, RowActionIDs: []string{rid("schema.drop")}, Exportable: true}},
			{Key: "cypher", Label: "Cypher", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: rid("query"), Method: plugin.MethodWS, Params: map[string]string{"database": "${resource.uid}"}}, Config: queryConfig("MATCH (n) RETURN n LIMIT 25")},
		}},
	}
}

func labelResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "label", Title: "Labels",
		List:    plugin.DataSource{RouteID: rid("labels.list")},
		Columns: labelColumns(),
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: ":${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("label.overview"), Params: map[string]string{"database": "${resource.namespace}", "label": "${resource.name}"}}, Config: objectDetailConfig()},
			{Key: "graph", Label: "Graph", Icon: icon("workflow"), Type: plugin.PanelGraph, Source: &plugin.DataSource{RouteID: rid("label.graph"), Params: map[string]string{"database": "${resource.namespace}", "label": "${resource.name}"}}, Config: plugin.GraphConfig{FitView: true, ExpandRouteID: rid("node.graph"), Exportable: boolPtr(true)}},
			{Key: "nodes", Label: "Nodes", Icon: icon("circle-dot"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("nodes.list"), Params: map[string]string{"database": "${resource.namespace}", "label": "${resource.name}"}}, Config: plugin.TableConfig{Columns: nodeColumns(), ActionIDs: []string{rid("node.create")}, RowActionIDs: []string{rid("node.delete")}, Exportable: true}},
			{Key: "cypher", Label: "Cypher", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: rid("query"), Method: plugin.MethodWS, Params: map[string]string{"database": "${resource.namespace}"}}, Config: queryConfig("MATCH (n:" + "`${resource.name}`" + ") RETURN n LIMIT 25")},
		}},
	}
}

func relationshipTypeResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "relationship_type", Title: "Relationship Types",
		List:    plugin.DataSource{RouteID: rid("relationship_types.list")},
		Columns: relationshipTypeColumns(),
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "[:${resource.name}]"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("relationship_type.overview"), Params: map[string]string{"database": "${resource.namespace}", "type": "${resource.name}"}}, Config: objectDetailConfig()},
			{Key: "graph", Label: "Graph", Icon: icon("workflow"), Type: plugin.PanelGraph, Source: &plugin.DataSource{RouteID: rid("relationship_type.graph"), Params: map[string]string{"database": "${resource.namespace}", "type": "${resource.name}"}}, Config: plugin.GraphConfig{FitView: true, ExpandRouteID: rid("node.graph"), Exportable: boolPtr(true)}},
			{Key: "relationships", Label: "Relationships", Icon: icon("git-branch"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("relationships.list"), Params: map[string]string{"database": "${resource.namespace}", "type": "${resource.name}"}}, Config: plugin.TableConfig{Columns: relationshipColumns(), ActionIDs: []string{rid("relationship.create")}, RowActionIDs: []string{rid("relationship.delete")}, Exportable: true}},
			{Key: "cypher", Label: "Cypher", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor, Source: &plugin.DataSource{RouteID: rid("query"), Method: plugin.MethodWS, Params: map[string]string{"database": "${resource.namespace}"}}, Config: queryConfig("MATCH p=()-[r:" + "`${resource.name}`" + "]->() RETURN p LIMIT 25")},
		}},
	}
}

func nodeResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "node", Title: "Nodes",
		List:    plugin.DataSource{RouteID: rid("nodes.list")},
		Columns: nodeColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{rid("node.delete")},
			Detail: []string{rid("node.delete")},
		},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("node.read"), Params: map[string]string{"id": "${resource.uid}"}}, Config: objectDetailConfig()},
			{Key: "properties", Label: "Properties", Icon: icon("braces"), Type: plugin.PanelCodeEditor, Source: &plugin.DataSource{RouteID: rid("node.properties"), Params: map[string]string{"id": "${resource.uid}"}}, Config: propertiesEditorConfig(rid("node.update"))},
			{Key: "relationships", Label: "Relationships", Icon: icon("git-branch"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("node.relationships"), Params: map[string]string{"id": "${resource.uid}"}}, Config: plugin.TableConfig{Columns: relationshipColumns(), RowActionIDs: []string{rid("relationship.delete")}, Exportable: true}},
		}},
	}
}

func relationshipResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "relationship", Title: "Relationships",
		List:    plugin.DataSource{RouteID: rid("relationships.list")},
		Columns: relationshipColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{rid("relationship.delete")},
			Detail: []string{rid("relationship.delete")},
		},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("relationship.read"), Params: map[string]string{"id": "${resource.uid}"}}, Config: objectDetailConfig()},
			{Key: "properties", Label: "Properties", Icon: icon("braces"), Type: plugin.PanelCodeEditor, Source: &plugin.DataSource{RouteID: rid("relationship.properties"), Params: map[string]string{"id": "${resource.uid}"}}, Config: propertiesEditorConfig(rid("relationship.update"))},
		}},
	}
}

func schemaItemResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "schema_item", Title: "Schema",
		List: plugin.DataSource{RouteID: rid("schema.list")},
		Columns: []plugin.Column{
			{Key: "kind", Label: "Kind", Type: plugin.ColumnBadge, Sortable: true},
			{Key: "name", Label: "Name", Sortable: true},
			{Key: "type", Label: "Type", Type: plugin.ColumnBadge, Sortable: true},
			{Key: "entity_type", Label: "Entity", Type: plugin.ColumnBadge, Sortable: true},
			{Key: "labels_or_types", Label: "Labels / Types"},
			{Key: "properties", Label: "Properties"},
		},
		Actions: plugin.ResourceActions{
			Row:    []string{rid("schema.drop")},
			Detail: []string{rid("schema.drop")},
		},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("schema.read"), Params: map[string]string{"id": "${resource.uid}"}}, Config: objectDetailConfig()},
		}},
	}
}

func actions() []plugin.Action {
	return []plugin.Action{
		{ID: rid("node.create"), Label: "Create node", Icon: icon("plus"), RouteID: rid("node.create"), Params: map[string]string{"database": "${resource.namespace}"}, OnSuccess: &plugin.ActionSuccess{SelectTab: "nodes"}},
		{ID: rid("node.delete"), Label: "Delete node", Icon: icon("trash"), RouteID: rid("node.delete"), Params: map[string]string{"id": "${resource.uid}"}, Confirm: true, ConfirmText: "Delete this node and detach its relationships?"},
		{ID: rid("relationship.create"), Label: "Create relationship", Icon: icon("git-branch-plus"), RouteID: rid("relationship.create"), Params: map[string]string{"database": "${resource.namespace}"}, OnSuccess: &plugin.ActionSuccess{SelectTab: "relationships"}},
		{ID: rid("relationship.delete"), Label: "Delete relationship", Icon: icon("trash"), RouteID: rid("relationship.delete"), Params: map[string]string{"id": "${resource.uid}"}, Confirm: true, ConfirmText: "Delete this relationship?"},
		{ID: rid("index.create"), Label: "Create index", Icon: icon("plus"), RouteID: rid("index.create"), Params: map[string]string{"database": "${resource.uid}"}, OnSuccess: &plugin.ActionSuccess{SelectTab: "indexes"}},
		{ID: rid("constraint.create"), Label: "Create constraint", Icon: icon("plus"), RouteID: rid("constraint.create"), Params: map[string]string{"database": "${resource.uid}"}, OnSuccess: &plugin.ActionSuccess{SelectTab: "constraints"}},
		{ID: rid("schema.drop"), Label: "Drop", Icon: icon("trash"), RouteID: rid("schema.drop"), Params: map[string]string{"id": "${resource.uid}"}, Confirm: true, ConfirmText: "Drop this index or constraint?"},
	}
}

func propertiesEditorConfig(saveRouteID string) plugin.CodeEditorConfig {
	return plugin.CodeEditorConfig{
		Language:    "json",
		SaveRouteID: saveRouteID,
		SaveMethod:  plugin.MethodPut,
		SaveParams:  map[string]string{"id": "${resource.uid}"},
	}
}

func queryConfig(initial string) plugin.QueryEditorConfig {
	return plugin.QueryEditorConfig{
		Language:          "cypher",
		Label:             "Cypher",
		ExecuteLabel:      "Run query",
		RunningLabel:      "Running...",
		EmptyText:         "Run a Cypher query to see results.",
		InitialQuery:      initial,
		CompletionRouteID: rid("completion"),
		Exportable:        true,
	}
}

func labelColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Label", Sortable: true}, {Key: "nodes", Label: "Nodes", Type: plugin.ColumnNumber, Sortable: true}, {Key: "properties", Label: "Properties"}}
}

func relationshipTypeColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Type", Sortable: true}, {Key: "relationships", Label: "Relationships", Type: plugin.ColumnNumber, Sortable: true}, {Key: "properties", Label: "Properties"}}
}

func nodeColumns() []plugin.Column {
	return []plugin.Column{{Key: "element_id", Label: "Element ID", Sortable: true}, {Key: "labels", Label: "Labels"}, {Key: "properties", Label: "Properties", Type: plugin.ColumnJSON}, {Key: "degree", Label: "Degree", Type: plugin.ColumnNumber, Sortable: true}}
}

func relationshipColumns() []plugin.Column {
	return []plugin.Column{{Key: "element_id", Label: "Element ID", Sortable: true}, {Key: "type", Label: "Type", Type: plugin.ColumnBadge, Sortable: true}, {Key: "start", Label: "Start"}, {Key: "end", Label: "End"}, {Key: "properties", Label: "Properties", Type: plugin.ColumnJSON}}
}

func indexColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Index", Sortable: true}, {Key: "type", Label: "Type", Type: plugin.ColumnBadge}, {Key: "entity_type", Label: "Entity", Type: plugin.ColumnBadge}, {Key: "labels_or_types", Label: "Labels / Types"}, {Key: "properties", Label: "Properties"}, {Key: "state", Label: "State", Type: plugin.ColumnBadge, Severities: statusSeverities}}
}

func constraintColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Constraint", Sortable: true}, {Key: "type", Label: "Type", Type: plugin.ColumnBadge}, {Key: "entity_type", Label: "Entity", Type: plugin.ColumnBadge}, {Key: "labels_or_types", Label: "Labels / Types"}, {Key: "properties", Label: "Properties"}}
}
