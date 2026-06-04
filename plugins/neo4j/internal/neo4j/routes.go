package neo4j

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	driver "github.com/neo4j/neo4j-go-driver/v6/neo4j"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

type confirmationError struct{ message string }

func (e confirmationError) Error() string { return e.message }

func routes() []plugin.Route {
	return []plugin.Route{
		{ID: rid("databases.tree"), Method: plugin.MethodGet, Path: "/tree/databases", Permission: "neo4j.databases.read", Risk: plugin.RiskSafe, AuditEvent: rid("databases.tree"), Handle: databasesTree},
		{ID: rid("databases.list"), Method: plugin.MethodGet, Path: "/databases", Permission: "neo4j.databases.read", Risk: plugin.RiskSafe, AuditEvent: rid("databases.list"), Handle: databasesList},
		{ID: rid("database.overview"), Method: plugin.MethodGet, Path: "/databases/{database}/overview", Permission: "neo4j.databases.read", Risk: plugin.RiskSafe, AuditEvent: rid("database.overview"), Handle: databaseOverview},
		{ID: rid("labels.tree"), Method: plugin.MethodGet, Path: "/tree/labels", Permission: "neo4j.labels.read", Risk: plugin.RiskSafe, AuditEvent: rid("labels.tree"), Handle: labelsTree},
		{ID: rid("labels.list"), Method: plugin.MethodGet, Path: "/labels", Permission: "neo4j.labels.read", Risk: plugin.RiskSafe, AuditEvent: rid("labels.list"), Handle: labelsList},
		{ID: rid("label.overview"), Method: plugin.MethodGet, Path: "/labels/{database}/{label}/overview", Permission: "neo4j.labels.read", Risk: plugin.RiskSafe, AuditEvent: rid("label.overview"), Handle: labelOverview},
		{ID: rid("relationship_types.tree"), Method: plugin.MethodGet, Path: "/tree/relationship-types", Permission: "neo4j.relationships.read", Risk: plugin.RiskSafe, AuditEvent: rid("relationship_types.tree"), Handle: relationshipTypesTree},
		{ID: rid("relationship_types.list"), Method: plugin.MethodGet, Path: "/relationship-types", Permission: "neo4j.relationships.read", Risk: plugin.RiskSafe, AuditEvent: rid("relationship_types.list"), Handle: relationshipTypesList},
		{ID: rid("relationship_type.overview"), Method: plugin.MethodGet, Path: "/relationship-types/{database}/{type}/overview", Permission: "neo4j.relationships.read", Risk: plugin.RiskSafe, AuditEvent: rid("relationship_type.overview"), Handle: relationshipTypeOverview},
		{ID: rid("schema.tree"), Method: plugin.MethodGet, Path: "/tree/schema", Permission: "neo4j.schema.read", Risk: plugin.RiskSafe, AuditEvent: rid("schema.tree"), Handle: schemaTree},
		{ID: rid("schema.list"), Method: plugin.MethodGet, Path: "/schema", Permission: "neo4j.schema.read", Risk: plugin.RiskSafe, AuditEvent: rid("schema.list"), Handle: schemaList},
		{ID: rid("schema.read"), Method: plugin.MethodGet, Path: "/schema/{id}", Permission: "neo4j.schema.read", Risk: plugin.RiskSafe, AuditEvent: rid("schema.read"), Handle: schemaRead},
		{ID: rid("indexes.list"), Method: plugin.MethodGet, Path: "/indexes", Permission: "neo4j.schema.read", Risk: plugin.RiskSafe, AuditEvent: rid("indexes.list"), Handle: indexesList},
		{ID: rid("constraints.list"), Method: plugin.MethodGet, Path: "/constraints", Permission: "neo4j.schema.read", Risk: plugin.RiskSafe, AuditEvent: rid("constraints.list"), Handle: constraintsList},
		{ID: rid("index.create"), Method: plugin.MethodPost, Path: "/databases/{database}/indexes", Permission: "neo4j.schema.write", Risk: plugin.RiskWrite, AuditEvent: rid("index.create"), Input: indexCreateSchema(), Handle: indexCreate},
		{ID: rid("constraint.create"), Method: plugin.MethodPost, Path: "/databases/{database}/constraints", Permission: "neo4j.schema.write", Risk: plugin.RiskWrite, AuditEvent: rid("constraint.create"), Input: constraintCreateSchema(), Handle: constraintCreate},
		{ID: rid("schema.drop"), Method: plugin.MethodDelete, Path: "/schema/{id}", Permission: "neo4j.schema.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("schema.drop"), Handle: schemaDrop},
		{ID: rid("nodes.list"), Method: plugin.MethodGet, Path: "/nodes", Permission: "neo4j.nodes.read", Risk: plugin.RiskSafe, AuditEvent: rid("nodes.list"), Handle: nodesList},
		{ID: rid("node.read"), Method: plugin.MethodGet, Path: "/nodes/{id}", Permission: "neo4j.nodes.read", Risk: plugin.RiskSafe, AuditEvent: rid("node.read"), Handle: nodeRead},
		{ID: rid("node.properties"), Method: plugin.MethodGet, Path: "/nodes/{id}/properties", Permission: "neo4j.nodes.read", Risk: plugin.RiskSafe, AuditEvent: rid("node.properties"), Handle: nodeProperties},
		{ID: rid("node.update"), Method: plugin.MethodPut, Path: "/nodes/{id}", Permission: "neo4j.nodes.write", Risk: plugin.RiskWrite, AuditEvent: rid("node.update"), Handle: nodeUpdate},
		{ID: rid("node.relationships"), Method: plugin.MethodGet, Path: "/nodes/{id}/relationships", Permission: "neo4j.relationships.read", Risk: plugin.RiskSafe, AuditEvent: rid("node.relationships"), Handle: nodeRelationships},
		{ID: rid("relationships.list"), Method: plugin.MethodGet, Path: "/relationships", Permission: "neo4j.relationships.read", Risk: plugin.RiskSafe, AuditEvent: rid("relationships.list"), Handle: relationshipsList},
		{ID: rid("relationship.read"), Method: plugin.MethodGet, Path: "/relationships/{id}", Permission: "neo4j.relationships.read", Risk: plugin.RiskSafe, AuditEvent: rid("relationship.read"), Handle: relationshipRead},
		{ID: rid("relationship.properties"), Method: plugin.MethodGet, Path: "/relationships/{id}/properties", Permission: "neo4j.relationships.read", Risk: plugin.RiskSafe, AuditEvent: rid("relationship.properties"), Handle: relationshipProperties},
		{ID: rid("relationship.update"), Method: plugin.MethodPut, Path: "/relationships/{id}", Permission: "neo4j.relationships.write", Risk: plugin.RiskWrite, AuditEvent: rid("relationship.update"), Handle: relationshipUpdate},
		{ID: rid("graph"), Method: plugin.MethodGet, Path: "/graph", Permission: "neo4j.graph.read", Risk: plugin.RiskSafe, AuditEvent: rid("graph"), Handle: graphRoute},
		{ID: rid("label.graph"), Method: plugin.MethodGet, Path: "/labels/{database}/{label}/graph", Permission: "neo4j.graph.read", Risk: plugin.RiskSafe, AuditEvent: rid("label.graph"), Handle: labelGraph},
		{ID: rid("relationship_type.graph"), Method: plugin.MethodGet, Path: "/relationship-types/{database}/{type}/graph", Permission: "neo4j.graph.read", Risk: plugin.RiskSafe, AuditEvent: rid("relationship_type.graph"), Handle: relationshipTypeGraph},
		{ID: rid("node.graph"), Method: plugin.MethodGet, Path: "/graph/node", Permission: "neo4j.graph.read", Risk: plugin.RiskSafe, AuditEvent: rid("node.graph"), Handle: nodeGraph},
		{ID: rid("node.create"), Method: plugin.MethodPost, Path: "/databases/{database}/nodes", Permission: "neo4j.nodes.write", Risk: plugin.RiskWrite, AuditEvent: rid("node.create"), Input: nodeCreateSchema(), Handle: nodeCreate},
		{ID: rid("node.delete"), Method: plugin.MethodDelete, Path: "/nodes/{id}", Permission: "neo4j.nodes.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("node.delete"), Handle: nodeDelete},
		{ID: rid("relationship.create"), Method: plugin.MethodPost, Path: "/databases/{database}/relationships", Permission: "neo4j.relationships.write", Risk: plugin.RiskWrite, AuditEvent: rid("relationship.create"), Input: relationshipCreateSchema(), Handle: relationshipCreate},
		{ID: rid("relationship.delete"), Method: plugin.MethodDelete, Path: "/relationships/{id}", Permission: "neo4j.relationships.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("relationship.delete"), Handle: relationshipDelete},
		{ID: rid("query"), Method: plugin.MethodWS, Path: "/query", Permission: "neo4j.cypher.execute", Risk: plugin.RiskPrivileged, AuditEvent: rid("query"), Stream: queryStream},
		{ID: rid("completion"), Method: plugin.MethodGet, Path: "/completion", Permission: "neo4j.schema.read", Risk: plugin.RiskSafe, AuditEvent: rid("completion"), Handle: completionRoute},
	}
}

func nodeCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Node", Fields: []plugin.Field{
		{Key: "labels", Label: "Labels", Type: plugin.FieldText, Required: true, Placeholder: "Person, Customer"},
		{Key: "properties", Label: "Properties", Type: plugin.FieldJSON, Default: map[string]any{}},
	}}}}
}

func relationshipCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Relationship", Fields: []plugin.Field{
		{Key: "start_element_id", Label: "Start node element ID", Type: plugin.FieldText, Required: true},
		{Key: "end_element_id", Label: "End node element ID", Type: plugin.FieldText, Required: true},
		{Key: "type", Label: "Type", Type: plugin.FieldText, Required: true, Placeholder: "KNOWS"},
		{Key: "properties", Label: "Properties", Type: plugin.FieldJSON, Default: map[string]any{}},
	}}}}
}

func neo4jSession(rc *plugin.RequestContext) (*Session, error) { return unwrap(rc.Session) }

func databasesTree(rc *plugin.RequestContext) (any, error) {
	res, err := databasesList(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name := fmt.Sprint(item["name"])
		ref := plugin.ResourceRef{Kind: "database", Name: name, UID: name}
		nodes = append(nodes, plugin.TreeNode{Key: "database:" + name, Label: name, Icon: icon("database"), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

func databasesList(rc *plugin.RequestContext) (any, error) {
	s, err := neo4jSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, "system", "SHOW DATABASES YIELD name, address, role, requestedStatus, currentStatus RETURN name, address, role, requestedStatus, currentStatus ORDER BY name", nil)
	if err != nil {
		rows = []row{{"name": s.opts.Database, "current_status": "unknown", "requested_status": "unknown", "role": "", "address": "", "ref": plugin.ResourceRef{Kind: "database", Name: s.opts.Database, UID: s.opts.Database}}}
		return broker.PageRows(rc, rows)
	}
	// The administrative `system` database rejects data queries (MATCH), so it is
	// not a browsable data database — leave it out of the catalogue.
	out := make([]row, 0, len(rows))
	for _, r := range rows {
		name := fmt.Sprint(r["name"])
		if name == "system" {
			continue
		}
		r["requested_status"] = r["requestedStatus"]
		r["current_status"] = r["currentStatus"]
		delete(r, "requestedStatus")
		delete(r, "currentStatus")
		r["ref"] = plugin.ResourceRef{Kind: "database", Name: name, UID: name}
		out = append(out, r)
	}
	return broker.PageRows(rc, out)
}

func databaseOverview(rc *plugin.RequestContext) (any, error) {
	db := databaseName(rc)
	s, err := neo4jSession(rc)
	if err != nil {
		return nil, err
	}
	info := row{"database": db, "uri": s.opts.URI(), "read_only": s.opts.ReadOnly}
	if rows, err := queryRows(rc.Ctx, s, db, "MATCH (n) RETURN count(n) AS nodes", nil); err == nil && len(rows) > 0 {
		info["nodes"] = rows[0]["nodes"]
	}
	if rows, err := queryRows(rc.Ctx, s, db, "MATCH ()-[r]->() RETURN count(r) AS relationships", nil); err == nil && len(rows) > 0 {
		info["relationships"] = rows[0]["relationships"]
	}
	if rows, err := queryRows(rc.Ctx, s, db, "CALL db.labels() YIELD label RETURN count(label) AS labels", nil); err == nil && len(rows) > 0 {
		info["labels"] = rows[0]["labels"]
	}
	if rows, err := queryRows(rc.Ctx, s, db, "CALL db.relationshipTypes() YIELD relationshipType RETURN count(relationshipType) AS relationship_types", nil); err == nil && len(rows) > 0 {
		info["relationship_types"] = rows[0]["relationship_types"]
	}
	return info, nil
}

func labelsTree(rc *plugin.RequestContext) (any, error) {
	res, err := labelsList(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name, db := fmt.Sprint(item["name"]), fmt.Sprint(item["database"])
		ref := plugin.ResourceRef{Kind: "label", Namespace: db, Name: name, UID: db + ":" + name}
		nodes = append(nodes, plugin.TreeNode{Key: "label:" + ref.UID, Label: name, Icon: icon("tag"), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

func labelsList(rc *plugin.RequestContext) (any, error) {
	s, err := neo4jSession(rc)
	if err != nil {
		return nil, err
	}
	db := databaseName(rc)
	rows, err := queryRows(rc.Ctx, s, db, `
MATCH (n)
UNWIND labels(n) AS label
WITH label, count(n) AS nodes
OPTIONAL MATCH (m)
WHERE label IN labels(m)
UNWIND keys(m) AS prop
RETURN label AS name, nodes, collect(DISTINCT prop)[0..25] AS properties
ORDER BY name`, nil)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		name := fmt.Sprint(r["name"])
		r["database"] = db
		r["properties"] = strings.Join(stringSlice(r["properties"]), ", ")
		r["ref"] = plugin.ResourceRef{Kind: "label", Namespace: db, Name: name, UID: db + ":" + name}
	}
	return broker.PageRows(rc, rows)
}

func labelOverview(rc *plugin.RequestContext) (any, error) {
	db, label := databaseName(rc), rc.Param("label")
	s, err := neo4jSession(rc)
	if err != nil {
		return nil, err
	}
	out := row{"database": db, "label": label}
	q := "MATCH (n:" + quoteCypherName(label) + ") RETURN count(n) AS nodes, collect(DISTINCT keys(n))[0..100] AS property_sets"
	if rows, err := queryRows(rc.Ctx, s, db, q, nil); err == nil && len(rows) > 0 {
		for k, v := range rows[0] {
			out[k] = v
		}
	}
	return out, nil
}

func relationshipTypesTree(rc *plugin.RequestContext) (any, error) {
	res, err := relationshipTypesList(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name, db := fmt.Sprint(item["name"]), fmt.Sprint(item["database"])
		ref := plugin.ResourceRef{Kind: "relationship_type", Namespace: db, Name: name, UID: db + ":" + name}
		nodes = append(nodes, plugin.TreeNode{Key: "relationship_type:" + ref.UID, Label: name, Icon: icon("git-branch"), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

func relationshipTypesList(rc *plugin.RequestContext) (any, error) {
	s, err := neo4jSession(rc)
	if err != nil {
		return nil, err
	}
	db := databaseName(rc)
	rows, err := queryRows(rc.Ctx, s, db, `
MATCH ()-[r]->()
WITH type(r) AS name, count(r) AS relationships, collect(DISTINCT keys(r))[0..100] AS property_sets
RETURN name, relationships, property_sets AS properties
ORDER BY name`, nil)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		name := fmt.Sprint(r["name"])
		r["database"] = db
		r["properties"] = compactValue(r["properties"])
		r["ref"] = plugin.ResourceRef{Kind: "relationship_type", Namespace: db, Name: name, UID: db + ":" + name}
	}
	return broker.PageRows(rc, rows)
}

func relationshipTypeOverview(rc *plugin.RequestContext) (any, error) {
	db, typ := databaseName(rc), rc.Param("type")
	s, err := neo4jSession(rc)
	if err != nil {
		return nil, err
	}
	out := row{"database": db, "type": typ}
	q := "MATCH ()-[r:" + quoteCypherName(typ) + "]->() RETURN count(r) AS relationships, collect(DISTINCT keys(r))[0..100] AS property_sets"
	if rows, err := queryRows(rc.Ctx, s, db, q, nil); err == nil && len(rows) > 0 {
		for k, v := range rows[0] {
			out[k] = v
		}
	}
	return out, nil
}

func indexesList(rc *plugin.RequestContext) (any, error) {
	rows, err := schemaRows(rc, "SHOW INDEXES YIELD name, type, entityType, labelsOrTypes, properties, state RETURN name, type, entityType, labelsOrTypes, properties, state")
	if err != nil {
		return nil, err
	}
	return broker.PageRows(rc, rows)
}

func constraintsList(rc *plugin.RequestContext) (any, error) {
	rows, err := schemaRows(rc, "SHOW CONSTRAINTS YIELD name, type, entityType, labelsOrTypes, properties RETURN name, type, entityType, labelsOrTypes, properties")
	if err != nil {
		return nil, err
	}
	return broker.PageRows(rc, rows)
}

func schemaRows(rc *plugin.RequestContext, query string) ([]row, error) {
	s, err := neo4jSession(rc)
	if err != nil {
		return nil, err
	}
	db := databaseName(rc)
	rows, err := queryRows(rc.Ctx, s, db, query, nil)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		kind := "index"
		if _, ok := r["state"]; !ok {
			kind = "constraint"
		}
		name := fmt.Sprint(r["name"])
		r["kind"] = kind
		r["database"] = db
		r["entity_type"] = r["entityType"]
		r["labels_or_types"] = strings.Join(stringSlice(r["labelsOrTypes"]), ", ")
		r["properties"] = strings.Join(stringSlice(r["properties"]), ", ")
		delete(r, "entityType")
		delete(r, "labelsOrTypes")
		r["ref"] = plugin.ResourceRef{Kind: "schema_item", Namespace: db, Name: name, UID: mustEncodeID(kind, db, name)}
	}
	return rows, nil
}

func schemaList(rc *plugin.RequestContext) (any, error) {
	indexes, err := schemaRows(rc, "SHOW INDEXES YIELD name, type, entityType, labelsOrTypes, properties, state RETURN name, type, entityType, labelsOrTypes, properties, state")
	if err != nil {
		return nil, err
	}
	constraints, err := schemaRows(rc, "SHOW CONSTRAINTS YIELD name, type, entityType, labelsOrTypes, properties RETURN name, type, entityType, labelsOrTypes, properties")
	if err != nil {
		return nil, err
	}
	rows := append(indexes, constraints...)
	sortRows(rows, "name")
	return broker.PageRows(rc, rows)
}

func schemaTree(rc *plugin.RequestContext) (any, error) {
	res, err := schemaList(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name, kind := fmt.Sprint(item["name"]), fmt.Sprint(item["kind"])
		ref := item["ref"].(plugin.ResourceRef)
		nodes = append(nodes, plugin.TreeNode{Key: "schema:" + ref.UID, Label: name, Icon: schemaIcon(kind), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

func schemaRead(rc *plugin.RequestContext) (any, error) {
	kind, db, name, err := decodeID3(rc.Param("id"))
	if err != nil {
		return nil, err
	}
	rows, err := schemaRowsForDB(rc.Ctx, rc.Session, db, kind)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		if fmt.Sprint(r["name"]) == name {
			return r, nil
		}
	}
	return nil, plugin.ErrNotFound
}

func indexCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Index", Fields: []plugin.Field{
		{Key: "name", Label: "Index name", Type: plugin.FieldText, Required: true},
		{Key: "entity_type", Label: "Entity", Type: plugin.FieldSelect, Required: true, Default: "node", Options: []plugin.Option{{Label: "Node", Value: "node"}, {Label: "Relationship", Value: "relationship"}}},
		{Key: "label", Label: "Label / type", Type: plugin.FieldText, Required: true, Placeholder: "Person"},
		{Key: "properties", Label: "Properties", Type: plugin.FieldText, Required: true, Placeholder: "name, email"},
	}}}}
}

func indexCreate(rc *plugin.RequestContext) (any, error) {
	s, err := neo4jSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Name       string `json:"name" validate:"required"`
		EntityType string `json:"entity_type"`
		Label      string `json:"label" validate:"required"`
		Properties string `json:"properties" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	props := splitCommaList(req.Properties)
	if len(props) == 0 {
		return nil, fmt.Errorf("%w: at least one property is required", plugin.ErrInvalidInput)
	}
	varName, pattern := "n", "(n:"+quoteCypherName(req.Label)+")"
	if strings.EqualFold(req.EntityType, "relationship") {
		varName, pattern = "r", "()-[r:"+quoteCypherName(req.Label)+"]-()"
	}
	quoted := make([]string, 0, len(props))
	for _, p := range props {
		quoted = append(quoted, varName+"."+quoteCypherName(p))
	}
	query := "CREATE INDEX " + quoteCypherName(req.Name) + " FOR " + pattern + " ON (" + strings.Join(quoted, ", ") + ")"
	_, err = writeRows(rc.Ctx, s, databaseName(rc), query, nil)
	return actionResult{OK: err == nil}, err
}

func constraintCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Constraint", Fields: []plugin.Field{
		{Key: "name", Label: "Constraint name", Type: plugin.FieldText, Required: true},
		{Key: "entity_type", Label: "Entity", Type: plugin.FieldSelect, Required: true, Default: "node", Options: []plugin.Option{{Label: "Node", Value: "node"}, {Label: "Relationship", Value: "relationship"}}},
		{Key: "type", Label: "Type", Type: plugin.FieldSelect, Required: true, Default: "unique", Options: []plugin.Option{
			{Label: "Unique", Value: "unique"},
			{Label: "Property existence", Value: "exists"},
			{Label: "Node key", Value: "node_key"},
		}},
		{Key: "label", Label: "Label / type", Type: plugin.FieldText, Required: true, Placeholder: "Person"},
		{Key: "properties", Label: "Properties", Type: plugin.FieldText, Required: true, Placeholder: "name, email"},
	}}}}
}

func constraintCreate(rc *plugin.RequestContext) (any, error) {
	s, err := neo4jSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Name       string `json:"name" validate:"required"`
		EntityType string `json:"entity_type"`
		Type       string `json:"type"`
		Label      string `json:"label" validate:"required"`
		Properties string `json:"properties" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	query, err := constraintCreateQuery(req.Name, req.EntityType, req.Type, req.Label, splitCommaList(req.Properties))
	if err != nil {
		return nil, err
	}
	_, err = writeRows(rc.Ctx, s, databaseName(rc), query, nil)
	return actionResult{OK: err == nil}, err
}

// constraintCreateQuery builds a `CREATE CONSTRAINT` statement. Identifiers are
// quoted (never interpolated as values), so the generated Cypher is injection-safe.
func constraintCreateQuery(name, entityType, constraintType, label string, props []string) (string, error) {
	safeName, err := safeGraphName(name, "constraint name")
	if err != nil {
		return "", err
	}
	safeLabel, err := safeGraphName(label, "label / type")
	if err != nil {
		return "", err
	}
	if len(props) == 0 {
		return "", fmt.Errorf("%w: at least one property is required", plugin.ErrInvalidInput)
	}
	varName, pattern := "n", "(n:"+quoteCypherName(safeLabel)+")"
	if strings.EqualFold(entityType, "relationship") {
		varName, pattern = "r", "()-[r:"+quoteCypherName(safeLabel)+"]-()"
	}
	refs := make([]string, 0, len(props))
	for _, p := range props {
		name, err := safeGraphName(p, "property")
		if err != nil {
			return "", err
		}
		refs = append(refs, varName+"."+quoteCypherName(name))
	}
	joined := strings.Join(refs, ", ")
	expr := joined
	if len(refs) > 1 || strings.EqualFold(constraintType, "node_key") {
		expr = "(" + joined + ")"
	}
	head := "CREATE CONSTRAINT " + quoteCypherName(safeName) + " FOR " + pattern + " REQUIRE "
	switch strings.ToLower(strings.TrimSpace(constraintType)) {
	case "", "unique":
		return head + expr + " IS UNIQUE", nil
	case "exists":
		return head + expr + " IS NOT NULL", nil
	case "node_key":
		return head + expr + " IS NODE KEY", nil
	default:
		return "", fmt.Errorf("%w: unsupported constraint type %q", plugin.ErrInvalidInput, constraintType)
	}
}

func schemaDrop(rc *plugin.RequestContext) (any, error) {
	kind, db, name, err := decodeID3(rc.Param("id"))
	if err != nil {
		return nil, err
	}
	s, err := neo4jSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	verb := "DROP INDEX "
	if kind == "constraint" {
		verb = "DROP CONSTRAINT "
	}
	_, err = writeRows(rc.Ctx, s, db, verb+quoteCypherName(name), nil)
	return actionResult{OK: err == nil}, err
}

func splitCommaList(in string) []string {
	out := []string{}
	for _, part := range strings.Split(in, ",") {
		if v := strings.TrimSpace(part); v != "" {
			out = append(out, v)
		}
	}
	return out
}

func schemaRowsForDB(ctx context.Context, sess plugin.Session, db, kind string) ([]row, error) {
	s, err := unwrap(sess)
	if err != nil {
		return nil, err
	}
	if kind == "constraint" {
		rows, err := queryRows(ctx, s, db, "SHOW CONSTRAINTS YIELD name, type, entityType, labelsOrTypes, properties RETURN name, type, entityType, labelsOrTypes, properties", nil)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			r["kind"] = "constraint"
		}
		return rows, nil
	}
	rows, err := queryRows(ctx, s, db, "SHOW INDEXES YIELD name, type, entityType, labelsOrTypes, properties, state RETURN name, type, entityType, labelsOrTypes, properties, state", nil)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		r["kind"] = "index"
	}
	return rows, nil
}

func nodesList(rc *plugin.RequestContext) (any, error) {
	s, err := neo4jSession(rc)
	if err != nil {
		return nil, err
	}
	page, err := rc.Page()
	if err != nil {
		return nil, err
	}
	db := databaseName(rc)
	label := paramOrQuery(rc, "label")
	skip := cursorOffset(page.Cursor)
	query := "MATCH (n"
	if label != "" {
		query += ":" + quoteCypherName(label)
	}
	query += ") RETURN elementId(n) AS element_id, labels(n) AS labels, properties(n) AS properties, count { (n)--() } AS degree ORDER BY element_id SKIP $skip LIMIT $limit"
	rows, err := queryRows(rc.Ctx, s, db, query, map[string]any{"skip": skip, "limit": page.Limit})
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		id := fmt.Sprint(r["element_id"])
		r["labels"] = strings.Join(stringSlice(r["labels"]), ", ")
		r["properties"] = redactMap(asMap(r["properties"]), s.opts.RedactPatterns)
		r["ref"] = plugin.ResourceRef{Kind: "node", Namespace: db, Name: nodeName(r), UID: mustEncodeID("node", db, id)}
	}
	next := ""
	if len(rows) == page.Limit {
		next = strconv.Itoa(skip + len(rows))
	}
	return plugin.Page[row]{Items: rows, NextCursor: next}, nil
}

func nodeRead(rc *plugin.RequestContext) (any, error) {
	_, db, id, err := decodeID3(rc.Param("id"))
	if err != nil {
		return nil, err
	}
	s, err := neo4jSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, db, "MATCH (n) WHERE elementId(n) = $id RETURN elementId(n) AS element_id, labels(n) AS labels, properties(n) AS properties, count { (n)--() } AS degree", map[string]any{"id": id})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, plugin.ErrNotFound
	}
	rows[0]["properties"] = redactMap(asMap(rows[0]["properties"]), s.opts.RedactPatterns)
	return rows[0], nil
}

func nodeProperties(rc *plugin.RequestContext) (any, error) {
	_, db, id, err := decodeID3(rc.Param("id"))
	if err != nil {
		return nil, err
	}
	s, err := neo4jSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, db, "MATCH (n) WHERE elementId(n) = $id RETURN properties(n) AS properties", map[string]any{"id": id})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, plugin.ErrNotFound
	}
	return redactMap(asMap(rows[0]["properties"]), s.opts.RedactPatterns), nil
}

func nodeUpdate(rc *plugin.RequestContext) (any, error) {
	_, db, id, err := decodeID3(rc.Param("id"))
	if err != nil {
		return nil, err
	}
	s, err := neo4jSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	props, err := bindProperties(rc)
	if err != nil {
		return nil, err
	}
	query, params := setPropertiesQuery("n", "MATCH (n) WHERE elementId(n) = $id", id, props)
	rows, err := writeRows(rc.Ctx, s, db, query+" RETURN properties(n) AS properties", params)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, plugin.ErrNotFound
	}
	rows[0]["properties"] = redactMap(asMap(rows[0]["properties"]), s.opts.RedactPatterns)
	return rows[0], nil
}

func nodeRelationships(rc *plugin.RequestContext) (any, error) {
	_, db, id, err := decodeID3(rc.Param("id"))
	if err != nil {
		return nil, err
	}
	return relationshipRows(rc, db, "MATCH (a)-[r]-(b) WHERE elementId(a) = $id RETURN elementId(r) AS element_id, type(r) AS type, elementId(startNode(r)) AS start, elementId(endNode(r)) AS end, properties(r) AS properties ORDER BY element_id SKIP $skip LIMIT $limit", map[string]any{"id": id})
}

func relationshipsList(rc *plugin.RequestContext) (any, error) {
	db := databaseName(rc)
	typ := paramOrQuery(rc, "type")
	query := "MATCH ()-[r"
	if typ != "" {
		query += ":" + quoteCypherName(typ)
	}
	query += "]->() RETURN elementId(r) AS element_id, type(r) AS type, elementId(startNode(r)) AS start, elementId(endNode(r)) AS end, properties(r) AS properties ORDER BY element_id SKIP $skip LIMIT $limit"
	return relationshipRows(rc, db, query, nil)
}

func relationshipRows(rc *plugin.RequestContext, db, query string, params map[string]any) (any, error) {
	s, err := neo4jSession(rc)
	if err != nil {
		return nil, err
	}
	page, err := rc.Page()
	if err != nil {
		return nil, err
	}
	if params == nil {
		params = map[string]any{}
	}
	skip := cursorOffset(page.Cursor)
	params["skip"] = skip
	params["limit"] = page.Limit
	rows, err := queryRows(rc.Ctx, s, db, query, params)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		id := fmt.Sprint(r["element_id"])
		r["properties"] = redactMap(asMap(r["properties"]), s.opts.RedactPatterns)
		r["ref"] = plugin.ResourceRef{Kind: "relationship", Namespace: db, Name: fmt.Sprint(r["type"]) + " " + id, UID: mustEncodeID("relationship", db, id)}
	}
	next := ""
	if len(rows) == page.Limit {
		next = strconv.Itoa(skip + len(rows))
	}
	return plugin.Page[row]{Items: rows, NextCursor: next}, nil
}

func relationshipRead(rc *plugin.RequestContext) (any, error) {
	_, db, id, err := decodeID3(rc.Param("id"))
	if err != nil {
		return nil, err
	}
	s, err := neo4jSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, db, "MATCH ()-[r]->() WHERE elementId(r) = $id RETURN elementId(r) AS element_id, type(r) AS type, elementId(startNode(r)) AS start, elementId(endNode(r)) AS end, properties(r) AS properties", map[string]any{"id": id})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, plugin.ErrNotFound
	}
	rows[0]["properties"] = redactMap(asMap(rows[0]["properties"]), s.opts.RedactPatterns)
	return rows[0], nil
}

func relationshipProperties(rc *plugin.RequestContext) (any, error) {
	_, db, id, err := decodeID3(rc.Param("id"))
	if err != nil {
		return nil, err
	}
	s, err := neo4jSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, db, "MATCH ()-[r]->() WHERE elementId(r) = $id RETURN properties(r) AS properties", map[string]any{"id": id})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, plugin.ErrNotFound
	}
	return redactMap(asMap(rows[0]["properties"]), s.opts.RedactPatterns), nil
}

func relationshipUpdate(rc *plugin.RequestContext) (any, error) {
	_, db, id, err := decodeID3(rc.Param("id"))
	if err != nil {
		return nil, err
	}
	s, err := neo4jSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	props, err := bindProperties(rc)
	if err != nil {
		return nil, err
	}
	query, params := setPropertiesQuery("r", "MATCH ()-[r]->() WHERE elementId(r) = $id", id, props)
	rows, err := writeRows(rc.Ctx, s, db, query+" RETURN properties(r) AS properties", params)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, plugin.ErrNotFound
	}
	rows[0]["properties"] = redactMap(asMap(rows[0]["properties"]), s.opts.RedactPatterns)
	return rows[0], nil
}

func graphRoute(rc *plugin.RequestContext) (any, error) {
	return graphQuery(rc, databaseName(rc), `
MATCH (a)-[r]->(b)
WITH a, r, b, count { (a)--() } + count { (b)--() } AS degree
ORDER BY degree DESC
RETURN a, r, b LIMIT $limit`, nil)
}

func labelGraph(rc *plugin.RequestContext) (any, error) {
	db, label := databaseName(rc), rc.Param("label")
	return graphQuery(rc, db, `
MATCH (a:`+quoteCypherName(label)+`)-[r]-(b)
WITH a, r, b, count { (a)--() } + count { (b)--() } AS degree
ORDER BY degree DESC
RETURN a, r, b LIMIT $limit`, nil)
}

func relationshipTypeGraph(rc *plugin.RequestContext) (any, error) {
	db, typ := databaseName(rc), rc.Param("type")
	return graphQuery(rc, db, `
MATCH (a)-[r:`+quoteCypherName(typ)+`]->(b)
WITH a, r, b, count { (a)--() } + count { (b)--() } AS degree
ORDER BY degree DESC
RETURN a, r, b LIMIT $limit`, nil)
}

// nodeGraph returns a node's immediate neighbourhood for click-to-expand: the
// clicked node id is the element id carried by the graph node payload.
func nodeGraph(rc *plugin.RequestContext) (any, error) {
	id := strings.TrimSpace(paramOrQuery(rc, "node"))
	if id == "" {
		return nil, fmt.Errorf("%w: node is required", plugin.ErrInvalidInput)
	}
	return graphQuery(rc, databaseName(rc), "MATCH p=(a)-[r]-(b) WHERE elementId(a) = $id RETURN p LIMIT $limit", map[string]any{"id": id})
}

func graphQuery(rc *plugin.RequestContext, db, query string, params map[string]any) (any, error) {
	s, err := neo4jSession(rc)
	if err != nil {
		return nil, err
	}
	if params == nil {
		params = map[string]any{}
	}
	params["limit"] = s.opts.PageLimit
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.QueryTimeout)
	defer cancel()
	session := s.driver.NewSession(ctx, driver.SessionConfig{DatabaseName: db, AccessMode: driver.AccessModeRead, FetchSize: s.opts.FetchSize})
	defer func() { _ = session.Close(context.Background()) }()
	result, err := session.Run(ctx, query, params)
	if err != nil {
		return nil, neo4jErr(err)
	}
	records, err := result.Collect(ctx)
	if err != nil {
		return nil, neo4jErr(err)
	}
	graph := graphPayload{Nodes: []graphNode{}, Edges: []graphEdge{}}
	seenNodes, seenEdges := map[string]bool{}, map[string]bool{}
	for _, record := range records {
		for _, value := range record.Values {
			collectGraphValue(value, &graph, seenNodes, seenEdges, s.opts.RedactPatterns)
		}
	}
	return graph, nil
}

func nodeCreate(rc *plugin.RequestContext) (any, error) {
	s, err := neo4jSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Labels     string         `json:"labels" validate:"required"`
		Properties map[string]any `json:"properties"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	labels, err := labelList(req.Labels)
	if err != nil {
		return nil, err
	}
	query := "CREATE (n" + labels + " $props) RETURN elementId(n) AS element_id, labels(n) AS labels, properties(n) AS properties"
	rows, err := writeRows(rc.Ctx, s, databaseName(rc), query, map[string]any{"props": req.Properties})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return actionResult{OK: true}, nil
	}
	return rows[0], nil
}

func nodeDelete(rc *plugin.RequestContext) (any, error) {
	_, db, id, err := decodeID3(rc.Param("id"))
	if err != nil {
		return nil, err
	}
	s, err := neo4jSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	_, err = writeRows(rc.Ctx, s, db, "MATCH (n) WHERE elementId(n) = $id DETACH DELETE n", map[string]any{"id": id})
	return actionResult{OK: err == nil}, err
}

func relationshipCreate(rc *plugin.RequestContext) (any, error) {
	s, err := neo4jSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		StartElementID string         `json:"start_element_id" validate:"required"`
		EndElementID   string         `json:"end_element_id" validate:"required"`
		Type           string         `json:"type" validate:"required"`
		Properties     map[string]any `json:"properties"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	typ, err := safeGraphName(req.Type, "relationship type")
	if err != nil {
		return nil, err
	}
	query := "MATCH (a), (b) WHERE elementId(a) = $start AND elementId(b) = $end CREATE (a)-[r:" + quoteCypherName(typ) + " $props]->(b) RETURN elementId(r) AS element_id, type(r) AS type, elementId(startNode(r)) AS start, elementId(endNode(r)) AS end, properties(r) AS properties"
	rows, err := writeRows(rc.Ctx, s, databaseName(rc), query, map[string]any{"start": req.StartElementID, "end": req.EndElementID, "props": req.Properties})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, plugin.ErrNotFound
	}
	return rows[0], nil
}

func relationshipDelete(rc *plugin.RequestContext) (any, error) {
	_, db, id, err := decodeID3(rc.Param("id"))
	if err != nil {
		return nil, err
	}
	s, err := neo4jSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	_, err = writeRows(rc.Ctx, s, db, "MATCH ()-[r]->() WHERE elementId(r) = $id DELETE r", map[string]any{"id": id})
	return actionResult{OK: err == nil}, err
}

func queryStream(rc *plugin.RequestContext, stream plugin.ClientStream) error {
	s, err := neo4jSession(rc)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(stream)
	dec := json.NewDecoder(stream)
	db := databaseName(rc)
	for {
		var req sqldb.QueryRequest
		if err := dec.Decode(&req); err != nil {
			return nil
		}
		result, err := executeCypher(stream.Context(), s, db, req)
		params := sqldb.AuditParams(sqldb.QueryAudit{Query: req.Query, Statements: []string{req.Query}, Confirmed: req.Confirm, ReadOnlyMode: s.opts.ReadOnly, RequiresReview: cypherNeedsReview(req.Query), RowCount: result.RowCount, ElapsedMS: result.ElapsedMS})
		rc.Audit(queryAuditResult(err), params, err)
		if err != nil {
			payload := map[string]any{"error": err.Error()}
			var confirmErr confirmationError
			if errors.As(err, &confirmErr) {
				payload["requiresConfirmation"] = true
				payload["confirmMessage"] = "This Cypher can write data or perform administrative work. Review it before running."
			}
			if err := enc.Encode(payload); err != nil {
				return err
			}
			continue
		}
		if err := enc.Encode(result); err != nil {
			return err
		}
	}
}

func executeCypher(ctx context.Context, s *Session, db string, req sqldb.QueryRequest) (sqldb.QueryResult, error) {
	query := strings.TrimSpace(req.Query)
	if query == "" {
		return sqldb.QueryResult{}, fmt.Errorf("%w: query is empty", plugin.ErrInvalidInput)
	}
	if s.opts.ReadOnly && cypherNeedsReview(query) {
		return sqldb.QueryResult{}, fmt.Errorf("%w: read-only mode blocks write and administrative Cypher", plugin.ErrForbidden)
	}
	if s.opts.RequireConfirm && !req.Confirm && cypherNeedsReview(query) {
		return sqldb.QueryResult{}, confirmationError{message: "query requires confirmation"}
	}
	start := time.Now()
	accessMode := driver.AccessModeRead
	if cypherNeedsReview(query) {
		accessMode = driver.AccessModeWrite
	}
	ctx, cancel := context.WithTimeout(ctx, s.opts.QueryTimeout)
	defer cancel()
	session := s.driver.NewSession(ctx, driver.SessionConfig{DatabaseName: db, AccessMode: accessMode, FetchSize: s.opts.FetchSize})
	defer func() { _ = session.Close(context.Background()) }()
	result, err := session.Run(ctx, query, nil)
	if err != nil {
		return sqldb.QueryResult{}, neo4jErr(err)
	}
	keys, _ := result.Keys()
	records, err := result.Collect(ctx)
	if err != nil {
		return sqldb.QueryResult{}, neo4jErr(err)
	}
	summary, err := result.Consume(ctx)
	if err != nil {
		return sqldb.QueryResult{}, neo4jErr(err)
	}
	rows := make([][]any, 0, len(records))
	for _, record := range records {
		line := make([]any, len(keys))
		for i, value := range record.Values {
			if i < len(line) {
				line[i] = normalizeValue(value, s.opts.RedactPatterns)
			}
		}
		rows = append(rows, line)
	}
	rows = sqldb.RedactRows(keys, rows, s.opts.RedactPatterns)
	tag := queryType(summary)
	if summary != nil && summary.Counters() != nil && summary.Counters().ContainsUpdates() {
		tag = "updates"
	}
	return sqldb.QueryResult{Columns: keys, Rows: rows, RowCount: int64(len(rows)), ElapsedMS: time.Since(start).Milliseconds(), Statement: query, CommandTag: tag}, nil
}

func completionRoute(_ *plugin.RequestContext) (any, error) {
	return []sqldb.CompletionItem{
		{Label: "MATCH", Type: "keyword", Apply: "MATCH (n) RETURN n LIMIT 25"},
		{Label: "CREATE", Type: "keyword", Apply: "CREATE (n:Label {name: 'value'}) RETURN n"},
		{Label: "MERGE", Type: "keyword", Apply: "MERGE (n:Label {id: $id}) RETURN n"},
		{Label: "SHOW INDEXES", Type: "keyword", Apply: "SHOW INDEXES"},
		{Label: "SHOW CONSTRAINTS", Type: "keyword", Apply: "SHOW CONSTRAINTS"},
		{Label: "CALL db.labels", Type: "function", Apply: "CALL db.labels() YIELD label RETURN label"},
	}, nil
}

func databaseName(rc *plugin.RequestContext) string {
	if db := strings.TrimSpace(rc.Param("database")); db != "" {
		return db
	}
	if db := strings.TrimSpace(rc.Query().Get("p.database")); db != "" {
		return db
	}
	if s, err := neo4jSession(rc); err == nil && s.opts.Database != "" {
		return s.opts.Database
	}
	return defaultDatabase
}

func paramOrQuery(rc *plugin.RequestContext, key string) string {
	if value := strings.TrimSpace(rc.Param(key)); value != "" {
		return value
	}
	return strings.TrimSpace(rc.Query().Get("p." + key))
}

func queryRows(ctx context.Context, s *Session, db, query string, params map[string]any) ([]row, error) {
	ctx, cancel := context.WithTimeout(ctx, s.opts.QueryTimeout)
	defer cancel()
	session := s.driver.NewSession(ctx, driver.SessionConfig{DatabaseName: db, AccessMode: driver.AccessModeRead, FetchSize: s.opts.FetchSize})
	defer func() { _ = session.Close(context.Background()) }()
	result, err := session.Run(ctx, query, params)
	if err != nil {
		return nil, neo4jErr(err)
	}
	records, err := result.Collect(ctx)
	if err != nil {
		return nil, neo4jErr(err)
	}
	rows := make([]row, 0, len(records))
	for _, record := range records {
		item := row{}
		for i, key := range record.Keys {
			if i < len(record.Values) {
				item[key] = normalizeValue(record.Values[i], s.opts.RedactPatterns)
			}
		}
		rows = append(rows, item)
	}
	return rows, nil
}

func writeRows(ctx context.Context, s *Session, db, query string, params map[string]any) ([]row, error) {
	ctx, cancel := context.WithTimeout(ctx, s.opts.QueryTimeout)
	defer cancel()
	session := s.driver.NewSession(ctx, driver.SessionConfig{DatabaseName: db, AccessMode: driver.AccessModeWrite, FetchSize: s.opts.FetchSize})
	defer func() { _ = session.Close(context.Background()) }()
	result, err := session.Run(ctx, query, params)
	if err != nil {
		return nil, neo4jErr(err)
	}
	records, err := result.Collect(ctx)
	if err != nil {
		return nil, neo4jErr(err)
	}
	rows := make([]row, 0, len(records))
	for _, record := range records {
		item := row{}
		for i, key := range record.Keys {
			if i < len(record.Values) {
				item[key] = normalizeValue(record.Values[i], s.opts.RedactPatterns)
			}
		}
		rows = append(rows, item)
	}
	return rows, nil
}

// bindProperties reads the uniform property-editor body. The generic code
// editor posts {"content": "<json>"}; a {"properties": {...}} object body is
// also accepted so callers can target the route directly.
func bindProperties(rc *plugin.RequestContext) (map[string]any, error) {
	var req struct {
		Content    string         `json:"content"`
		Properties map[string]any `json:"properties"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	if req.Properties != nil {
		return req.Properties, nil
	}
	return parsePropertiesContent(req.Content)
}

func parsePropertiesContent(content string) (map[string]any, error) {
	trimmed := strings.TrimSpace(content)
	if trimmed == "" {
		return map[string]any{}, nil
	}
	var props map[string]any
	if err := json.Unmarshal([]byte(trimmed), &props); err != nil {
		return nil, fmt.Errorf("%w: properties must be a JSON object: %v", plugin.ErrInvalidInput, err)
	}
	if props == nil {
		props = map[string]any{}
	}
	return props, nil
}

// setPropertiesQuery builds a property replace statement. The property map is
// passed as a parameter ($props) and never interpolated, so arbitrary values
// (including ones holding Cypher) are safe. `=` replaces the property map so
// removed keys are cleared, matching the read-edit-save round trip.
func setPropertiesQuery(varName, match, id string, props map[string]any) (string, map[string]any) {
	return match + " SET " + varName + " = $props", map[string]any{"id": id, "props": props}
}

func ensureWritable(s *Session) error {
	if s.opts.ReadOnly {
		return fmt.Errorf("%w: connection is read-only", plugin.ErrForbidden)
	}
	return nil
}

func queryAuditResult(err error) plugin.AuditResult {
	if err == nil {
		return plugin.AuditAllowed
	}
	var confirmErr confirmationError
	if errors.As(err, &confirmErr) {
		return plugin.AuditDenied
	}
	return plugin.AuditError
}
