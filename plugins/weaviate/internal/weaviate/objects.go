package weaviate

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/crossref"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
)

const (
	maxReferenceFanout = 25
	maxInboundPerClass = 10
)

// reservedRowKeys are grid columns the plugin owns; everything else in an
// editable-grid row body is treated as an object property.
var reservedRowKeys = map[string]bool{
	"id": true, "ref": true, "vector": true, "vectors": true, "tenant": true,
	"properties": true, "_creationTime": true, "_updateTime": true, "_additional": true,
}

type objectRequest struct {
	ID         string
	Properties map[string]any
	Vector     []float32
	Tenant     string
	// Replace is set when the body carried a whole properties document rather
	// than the changed cells of a grid row.
	Replace bool
}

func listObjects(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	class, err := loadClass(rc)
	if err != nil {
		return nil, err
	}
	req, err := rc.Page()
	if err != nil {
		return nil, err
	}
	limit := req.Limit
	if limit <= 0 || limit > s.opts.PageLimit {
		limit = s.opts.PageLimit
	}
	offset := 0
	if req.Cursor != "" {
		parsed, err := strconv.Atoi(req.Cursor)
		if err != nil || parsed < 0 {
			return nil, fmt.Errorf("%w: invalid page cursor", plugin.ErrInvalidInput)
		}
		offset = parsed
	}

	fields, columns := graphqlFields(class)
	builder := s.client.graphql.Get().
		WithClassName(class.Class).
		WithFields(append(fields, additionalField("id", "creationTimeUnix", "lastUpdateTimeUnix"))...).
		WithLimit(limit).
		WithOffset(offset)
	if tenant := strings.TrimSpace(rc.Param("tenant")); tenant != "" {
		builder = builder.WithTenant(tenant)
	}
	if len(req.Sort) > 0 {
		field, err := validName(req.Sort[0].Field, propertyNamePattern, "sort field")
		if err != nil || !columnExists(columns, field) {
			return nil, fmt.Errorf("%w: cannot sort by %q", plugin.ErrInvalidInput, req.Sort[0].Field)
		}
		order := graphql.Asc
		if req.Sort[0].Desc {
			order = graphql.Desc
		}
		builder = builder.WithSort(graphql.Sort{Path: []string{field}, Order: order})
	}
	if term := strings.TrimSpace(req.Search()); term != "" {
		if where := searchFilter(class, term); where != nil {
			builder = builder.WithWhere(where)
		}
	}

	resp, err := builder.Do(rc.Ctx)
	if err != nil {
		return nil, mapError(err)
	}
	if err := graphqlErrors(resp); err != nil {
		return nil, err
	}
	items := getResultItems(resp, class.Class)
	rows := make([]row, 0, len(items))
	for _, item := range items {
		rows = append(rows, objectRow(class.Class, item))
	}
	next := ""
	if len(rows) == limit {
		next = strconv.Itoa(offset + limit)
	}
	return plugin.Page[row]{Items: rows, NextCursor: next}, nil
}

func columnExists(columns []string, name string) bool {
	for _, column := range columns {
		if column == name {
			return true
		}
	}
	return false
}

func objectRow(className string, item map[string]any) row {
	out := row{}
	for key, value := range item {
		if key == "_additional" {
			continue
		}
		out[key] = value
	}
	additional, _ := item["_additional"].(map[string]any)
	id := text(additional["id"])
	out["id"] = id
	out["_creationTime"] = unixMillis(additional["creationTimeUnix"])
	out["_updateTime"] = unixMillis(additional["lastUpdateTimeUnix"])
	out["ref"] = plugin.ResourceIdentity{Kind: "object", Namespace: className, Name: id, UID: id}
	return out
}

// graphqlFields builds the selection set for a class and the flat column names it
// produces. Nested and geo properties need explicit sub-selections; blobs are left
// out of list queries because they are large and never useful in a grid.
func graphqlFields(class *models.Class) ([]graphql.Field, []string) {
	fields := make([]graphql.Field, 0, len(class.Properties))
	columns := make([]string, 0, len(class.Properties))
	for _, property := range class.Properties {
		if property == nil || len(property.DataType) == 0 {
			continue
		}
		dataType := property.DataType[0]
		if isReferenceType(dataType) || dataType == "blob" {
			continue
		}
		field := graphql.Field{Name: property.Name}
		switch strings.TrimSuffix(dataType, "[]") {
		case "object":
			nested := make([]graphql.Field, 0, len(property.NestedProperties))
			for _, child := range property.NestedProperties {
				if child != nil {
					nested = append(nested, graphql.Field{Name: child.Name})
				}
			}
			if len(nested) == 0 {
				continue
			}
			field.Fields = nested
		case "geoCoordinates":
			field.Fields = []graphql.Field{{Name: "latitude"}, {Name: "longitude"}}
		case "phoneNumber":
			field.Fields = []graphql.Field{{Name: "input"}, {Name: "internationalFormatted"}, {Name: "countryCode"}}
		}
		fields = append(fields, field)
		columns = append(columns, property.Name)
	}
	return fields, columns
}

func additionalField(names ...string) graphql.Field {
	nested := make([]graphql.Field, 0, len(names))
	for _, name := range names {
		nested = append(nested, graphql.Field{Name: name})
	}
	return graphql.Field{Name: "_additional", Fields: nested}
}

func searchFilter(class *models.Class, term string) *filters.WhereBuilder {
	operands := make([]*filters.WhereBuilder, 0, len(class.Properties))
	for _, property := range class.Properties {
		if property == nil || len(property.DataType) == 0 {
			continue
		}
		if property.DataType[0] != "text" && property.DataType[0] != "text[]" {
			continue
		}
		operands = append(operands, filters.Where().
			WithPath([]string{property.Name}).
			WithOperator(filters.Like).
			WithValueText("*"+term+"*"))
	}
	switch len(operands) {
	case 0:
		return nil
	case 1:
		return operands[0]
	default:
		return filters.Where().WithOperator(filters.Or).WithOperands(operands)
	}
}

func listCollectionColumns(rc *plugin.RequestContext) (any, error) {
	class, err := loadClass(rc)
	if err != nil {
		return nil, err
	}
	rows := []row{{
		"name": "id", "label": "ID", "type": string(plugin.ColumnText), "readOnly": true, "editable": false,
	}}
	// Only properties the list route actually selects become grid columns;
	// cross-references live in the References graph, not in an empty cell.
	_, selectable := graphqlFields(class)
	for _, property := range class.Properties {
		if property == nil || len(property.DataType) == 0 || !columnExists(selectable, property.Name) {
			continue
		}
		column := row{"name": property.Name, "label": property.Name}
		columnType, editor := columnTypeFor(property.DataType[0])
		column["type"] = string(columnType)
		column["editor"] = string(editor)
		column["editable"] = true
		column["nullable"] = true
		rows = append(rows, column)
	}
	rows = append(rows,
		row{"name": "_creationTime", "label": "Created", "type": string(plugin.ColumnDateTime), "readOnly": true, "editable": false},
		row{"name": "_updateTime", "label": "Updated", "type": string(plugin.ColumnDateTime), "readOnly": true, "editable": false},
	)
	return plugin.Page[row]{Items: rows}, nil
}

func columnTypeFor(dataType string) (plugin.ColumnType, plugin.ColumnEditor) {
	switch dataType {
	case "text", "string", "uuid":
		return plugin.ColumnText, plugin.ColumnEditorText
	case "int", "number":
		return plugin.ColumnNumber, plugin.ColumnEditorNumber
	case "boolean":
		return plugin.ColumnBool, plugin.ColumnEditorToggle
	case "date":
		return plugin.ColumnDateTime, plugin.ColumnEditorText
	default:
		return plugin.ColumnJSON, plugin.ColumnEditorJSON
	}
}

func readObject(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	className, id, err := objectTarget(rc)
	if err != nil {
		return nil, err
	}
	object, err := fetchObject(rc.Ctx, s, className, id, strings.TrimSpace(rc.Param("tenant")))
	if err != nil {
		return nil, err
	}
	vector := objectVector(object)
	out := row{
		"id":            string(object.ID),
		"collection":    object.Class,
		"tenant":        object.Tenant,
		"created":       unixMillis(float64(object.CreationTimeUnix)),
		"updated":       unixMillis(float64(object.LastUpdateTimeUnix)),
		"vectorDims":    len(vector),
		"vectorNorm":    round(norm(vector), 6),
		"namedVectors":  vectorNames(object),
		"properties":    object.Properties,
		"propertyCount": propertyCount(object),
	}
	return out, nil
}

func readObjectDocument(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	className, id, err := objectTarget(rc)
	if err != nil {
		return nil, err
	}
	object, err := fetchObject(rc.Ctx, s, className, id, strings.TrimSpace(rc.Param("tenant")))
	if err != nil {
		return nil, err
	}
	content, err := propertiesDocument(object)
	if err != nil {
		return nil, err
	}
	return row{"content": content, "id": string(object.ID), "collection": object.Class}, nil
}

// propertiesDocument renders an object's properties the way PanelCodeEditor
// displays them, so a save response can reset the editor baseline.
func propertiesDocument(object *models.Object) (string, error) {
	properties := object.Properties
	if properties == nil {
		properties = map[string]any{}
	}
	data, err := json.MarshalIndent(properties, "", "  ")
	if err != nil {
		return "", fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	return string(data), nil
}

func objectTarget(rc *plugin.RequestContext) (string, string, error) {
	className, err := collectionParam(rc)
	if err != nil {
		return "", "", err
	}
	id, err := objectIDParam(rc)
	if err != nil {
		return "", "", err
	}
	return className, id, nil
}

func fetchObject(ctx context.Context, s *Session, className, id, tenant string) (*models.Object, error) {
	getter := s.client.data.ObjectsGetter().
		WithClassName(className).
		WithID(id).
		WithVector().
		WithConsistencyLevel(s.opts.Consistency)
	if tenant != "" {
		getter = getter.WithTenant(tenant)
	}
	objects, err := getter.Do(ctx)
	if err != nil {
		return nil, mapError(err)
	}
	if len(objects) == 0 || objects[0] == nil {
		return nil, fmt.Errorf("%w: object %s in %s", plugin.ErrNotFound, id, className)
	}
	return objects[0], nil
}

func objectVector(object *models.Object) []float32 {
	if len(object.Vector) > 0 {
		return object.Vector
	}
	for _, name := range vectorNames(object) {
		if vector, ok := toFloat32Slice(object.Vectors[name]); ok {
			return vector
		}
	}
	return nil
}

func vectorNames(object *models.Object) []string {
	names := make([]string, 0, len(object.Vectors))
	for name := range object.Vectors {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func toFloat32Slice(value any) ([]float32, bool) {
	switch typed := value.(type) {
	case []float32:
		return typed, true
	case models.C11yVector:
		return typed, true
	case []float64:
		out := make([]float32, len(typed))
		for i, v := range typed {
			out[i] = float32(v)
		}
		return out, true
	case []any:
		out := make([]float32, 0, len(typed))
		for _, v := range typed {
			number, ok := numberOf(v)
			if !ok {
				return nil, false
			}
			out = append(out, float32(number))
		}
		return out, true
	default:
		return nil, false
	}
}

func propertyCount(object *models.Object) int {
	properties, ok := object.Properties.(map[string]any)
	if !ok {
		return 0
	}
	return len(properties)
}

func createObject(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	className, err := collectionParam(rc)
	if err != nil {
		return nil, err
	}
	req, err := parseObjectRequest(rc)
	if err != nil {
		return nil, err
	}
	creator := s.client.data.Creator().
		WithClassName(className).
		WithProperties(req.Properties).
		WithConsistencyLevel(s.opts.Consistency)
	if req.ID != "" {
		creator = creator.WithID(req.ID)
	}
	if len(req.Vector) > 0 {
		creator = creator.WithVector(req.Vector)
	}
	if req.Tenant != "" {
		creator = creator.WithTenant(req.Tenant)
	}
	created, err := creator.Do(rc.Ctx)
	if err != nil {
		return nil, mapError(err)
	}
	id := req.ID
	if created != nil && created.Object != nil {
		id = string(created.Object.ID)
	}
	record(rc, "objects", plugin.SeveritySuccess, "Object created", className+"/"+id)
	return row{"ok": true, "id": id, "collection": className}, nil
}

// updateObject merges when the editable grid sends the cells a user changed and
// replaces when the JSON document editor sends the whole properties object, so
// deleting a key there deletes the property on the server.
func updateObject(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	className, id, err := objectTarget(rc)
	if err != nil {
		return nil, err
	}
	req, err := parseObjectRequest(rc)
	if err != nil {
		return nil, err
	}
	updater := s.client.data.Updater().
		WithClassName(className).
		WithID(id).
		WithProperties(req.Properties).
		WithConsistencyLevel(s.opts.Consistency)
	if req.Tenant != "" {
		updater = updater.WithTenant(req.Tenant)
	}
	switch {
	case !req.Replace:
		// Single-cell grid edits carry only the changed columns, so they merge.
		updater = updater.WithMerge()
		if len(req.Vector) > 0 {
			updater = updater.WithVector(req.Vector)
		}
	case len(req.Vector) > 0:
		updater = updater.WithVector(req.Vector)
	default:
		vector, vectors, err := storedVectors(rc.Ctx, s, className, id, req.Tenant)
		if err != nil {
			return nil, err
		}
		if len(vector) > 0 {
			updater = updater.WithVector(vector)
		}
		if len(vectors) > 0 {
			updater = updater.WithVectors(vectors)
		}
	}
	if err := updater.Do(rc.Ctx); err != nil {
		return nil, mapError(err)
	}
	record(rc, "objects", plugin.SeverityInfo, "Object updated", className+"/"+id)
	object, err := fetchObject(rc.Ctx, s, className, id, req.Tenant)
	if err != nil {
		return nil, err
	}
	content, err := propertiesDocument(object)
	if err != nil {
		return nil, err
	}
	return row{"ok": true, "id": id, "collection": className, "content": content}, nil
}

// storedVectors returns the embedding a replacing update must carry forward. A
// PUT drops everything the body omits, and a collection that vectorizes nothing
// itself can never rebuild the vector the object was imported with.
func storedVectors(ctx context.Context, s *Session, className, id, tenant string) ([]float32, models.Vectors, error) {
	class, err := s.client.schema.ClassGetter().WithClassName(className).Do(ctx)
	if err != nil {
		return nil, nil, mapError(err)
	}
	if primaryVectorizer(class) != "none" {
		return nil, nil, nil
	}
	object, err := fetchObject(ctx, s, className, id, tenant)
	if err != nil {
		return nil, nil, err
	}
	return object.Vector, object.Vectors, nil
}

func deleteObject(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	className, id, err := objectTarget(rc)
	if err != nil {
		return nil, err
	}
	deleter := s.client.data.Deleter().
		WithClassName(className).
		WithID(id).
		WithConsistencyLevel(s.opts.Consistency)
	if tenant := strings.TrimSpace(rc.Param("tenant")); tenant != "" {
		deleter = deleter.WithTenant(tenant)
	}
	if err := deleter.Do(rc.Ctx); err != nil {
		return nil, mapError(err)
	}
	record(rc, "objects", plugin.SeverityDanger, "Object deleted", className+"/"+id)
	return row{"ok": true, "id": id, "collection": className}, nil
}

func deleteObjectsByFilter(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	className, err := collectionParam(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		Where  json.RawMessage `json:"where"`
		DryRun bool            `json:"dry_run"`
		Tenant string          `json:"tenant"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	where, err := parseWhereFilter(req.Where)
	if err != nil {
		return nil, err
	}
	deleter := s.client.batch.ObjectsBatchDeleter().
		WithClassName(className).
		WithWhere(where).
		WithDryRun(req.DryRun).
		WithConsistencyLevel(s.opts.Consistency)
	tenant := strings.TrimSpace(req.Tenant)
	if tenant == "" {
		tenant = strings.TrimSpace(rc.Param("tenant"))
	}
	if tenant != "" {
		deleter = deleter.WithTenant(tenant)
	}
	resp, err := deleter.Do(rc.Ctx)
	if err != nil {
		return nil, mapError(err)
	}
	out := row{"ok": true, "collection": className, "dryRun": req.DryRun}
	if resp != nil && resp.Results != nil {
		out["matches"] = resp.Results.Matches
		out["successful"] = resp.Results.Successful
		out["failed"] = resp.Results.Failed
	}
	title, severity := "Objects deleted by filter", plugin.SeverityDanger
	if req.DryRun {
		title, severity = "Delete-by-filter dry run", plugin.SeverityInfo
	}
	record(rc, "objects", severity, title, fmt.Sprintf("%s: %v matched", className, out["matches"]))
	return out, nil
}

// parseWhereFilter accepts the filter as a JSON object or as the JSON text a
// textarea field submits.
func parseWhereFilter(raw json.RawMessage) (*filters.WhereBuilder, error) {
	trimmed := json.RawMessage(strings.TrimSpace(string(raw)))
	if len(trimmed) > 0 && trimmed[0] == '"' {
		var content string
		if err := json.Unmarshal(trimmed, &content); err != nil {
			return nil, fmt.Errorf("%w: where filter is not valid JSON", plugin.ErrInvalidInput)
		}
		trimmed = json.RawMessage(strings.TrimSpace(content))
	}
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("%w: a where filter is required; deleting a whole collection is a collection delete", plugin.ErrInvalidInput)
	}
	return buildWhere(trimmed)
}

// parseObjectRequest accepts both editable-grid rows (flat property columns) and
// explicit {"properties": {...}} payloads from the code editor.
func parseObjectRequest(rc *plugin.RequestContext) (objectRequest, error) {
	body := rc.Body()
	if len(strings.TrimSpace(string(body))) == 0 {
		return objectRequest{}, fmt.Errorf("%w: request body is empty", plugin.ErrInvalidInput)
	}
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return objectRequest{}, fmt.Errorf("%w: body must be a JSON object", plugin.ErrInvalidInput)
	}
	out := objectRequest{Properties: map[string]any{}}
	if id := strings.TrimSpace(text(raw["id"])); id != "" {
		if !uuidPattern.MatchString(id) {
			return objectRequest{}, fmt.Errorf("%w: object id must be a UUID", plugin.ErrInvalidInput)
		}
		out.ID = id
	}
	out.Tenant = strings.TrimSpace(text(raw["tenant"]))
	if out.Tenant == "" {
		out.Tenant = strings.TrimSpace(rc.Param("tenant"))
	}
	if vector, ok := toFloat32Slice(raw["vector"]); ok {
		out.Vector = vector
	}
	if properties, ok := raw["properties"].(map[string]any); ok {
		out.Properties = properties
		out.Replace = true
		return out, nil
	}
	if content, ok := raw["content"].(string); ok && len(raw) == 1 {
		var properties map[string]any
		if err := json.Unmarshal([]byte(content), &properties); err != nil {
			return objectRequest{}, fmt.Errorf("%w: properties must be a JSON object", plugin.ErrInvalidInput)
		}
		out.Properties = properties
		out.Replace = true
		return out, nil
	}
	for key, value := range raw {
		if reservedRowKeys[key] || strings.HasPrefix(key, "_") {
			continue
		}
		if _, err := validName(key, propertyNamePattern, "property"); err != nil {
			return objectRequest{}, err
		}
		out.Properties[key] = value
	}
	return out, nil
}

func createReference(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	className, id, property, target, targetClass, err := referenceTarget(rc)
	if err != nil {
		return nil, err
	}
	payload := s.client.data.ReferencePayloadBuilder().WithClassName(targetClass).WithID(target).Payload()
	creator := s.client.data.ReferenceCreator().
		WithClassName(className).
		WithID(id).
		WithReferenceProperty(property).
		WithReference(payload).
		WithConsistencyLevel(s.opts.Consistency)
	if tenant := strings.TrimSpace(rc.Param("tenant")); tenant != "" {
		creator = creator.WithTenant(tenant)
	}
	if err := creator.Do(rc.Ctx); err != nil {
		return nil, mapError(err)
	}
	record(rc, "references", plugin.SeveritySuccess, "Reference added", className+"/"+id+" -> "+targetClass+"/"+target)
	return row{"ok": true, "property": property, "target": target}, nil
}

func deleteReference(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	className, id, property, target, targetClass, err := referenceTarget(rc)
	if err != nil {
		return nil, err
	}
	payload := s.client.data.ReferencePayloadBuilder().WithClassName(targetClass).WithID(target).Payload()
	deleter := s.client.data.ReferenceDeleter().
		WithClassName(className).
		WithID(id).
		WithReferenceProperty(property).
		WithReference(payload).
		WithConsistencyLevel(s.opts.Consistency)
	if tenant := strings.TrimSpace(rc.Param("tenant")); tenant != "" {
		deleter = deleter.WithTenant(tenant)
	}
	if err := deleter.Do(rc.Ctx); err != nil {
		return nil, mapError(err)
	}
	record(rc, "references", plugin.SeverityWarn, "Reference removed", className+"/"+id+" -> "+targetClass+"/"+target)
	return row{"ok": true, "property": property, "target": target}, nil
}

func referenceTarget(rc *plugin.RequestContext) (className, id, property, target, targetClass string, err error) {
	className, id, err = objectTarget(rc)
	if err != nil {
		return "", "", "", "", "", err
	}
	var req struct {
		Property    string `json:"property" validate:"required"`
		TargetID    string `json:"target_id" validate:"required"`
		TargetClass string `json:"target_collection"`
	}
	if err = rc.Bind(&req); err != nil {
		return "", "", "", "", "", err
	}
	property, err = validName(req.Property, propertyNamePattern, "property")
	if err != nil {
		return "", "", "", "", "", err
	}
	target = strings.TrimSpace(req.TargetID)
	if !uuidPattern.MatchString(target) {
		return "", "", "", "", "", fmt.Errorf("%w: target id must be a UUID", plugin.ErrInvalidInput)
	}
	targetClass = strings.TrimSpace(req.TargetClass)
	if targetClass == "" {
		targetClass, err = referencePropertyTarget(rc, className, property)
		if err != nil {
			return "", "", "", "", "", err
		}
	}
	if targetClass, err = validName(targetClass, classNamePattern, "target collection"); err != nil {
		return "", "", "", "", "", err
	}
	return className, id, property, target, targetClass, nil
}

func referencePropertyTarget(rc *plugin.RequestContext, className, property string) (string, error) {
	s, err := session(rc)
	if err != nil {
		return "", err
	}
	class, err := s.client.schema.ClassGetter().WithClassName(className).Do(rc.Ctx)
	if err != nil {
		return "", mapError(err)
	}
	for _, candidate := range class.Properties {
		if candidate != nil && candidate.Name == property && len(candidate.DataType) > 0 && isReferenceType(candidate.DataType[0]) {
			return candidate.DataType[0], nil
		}
	}
	return "", fmt.Errorf("%w: %s.%s is not a cross-reference property", plugin.ErrInvalidInput, className, property)
}

func objectReferenceGraph(rc *plugin.RequestContext) (any, error) {
	className, id, err := objectTarget(rc)
	if err != nil {
		return nil, err
	}
	return buildObjectGraph(rc, className, id)
}

func expandObjectReferenceGraph(rc *plugin.RequestContext) (any, error) {
	node := strings.TrimSpace(rc.Param("node"))
	className, id, ok := parseObjectNodeID(node)
	if !ok {
		return nil, fmt.Errorf("%w: node must look like obj:<Collection>:<uuid>", plugin.ErrInvalidInput)
	}
	if _, err := validName(className, classNamePattern, "collection"); err != nil {
		return nil, err
	}
	return buildObjectGraph(rc, className, id)
}

func parseObjectNodeID(node string) (string, string, bool) {
	parts := strings.SplitN(node, ":", 3)
	if len(parts) != 3 || parts[0] != "obj" || !uuidPattern.MatchString(parts[2]) {
		return "", "", false
	}
	return parts[1], parts[2], true
}

func objectNodeID(className, id string) string { return "obj:" + className + ":" + id }

func buildObjectGraph(rc *plugin.RequestContext, className, id string) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	tenant := strings.TrimSpace(rc.Param("tenant"))
	object, err := fetchObject(rc.Ctx, s, className, id, tenant)
	if err != nil {
		return nil, err
	}
	graph := newGraphBuilder()
	graph.addNode(objectNode(object, true))

	properties, _ := object.Properties.(map[string]any)
	for property, value := range properties {
		for _, beacon := range beaconsOf(value) {
			targetID := crossref.ExtractID(beacon)
			if !uuidPattern.MatchString(targetID) {
				continue
			}
			targetClass := beaconClass(beacon)
			if targetClass == "" {
				targetClass = "Object"
			}
			target := objectNodeID(targetClass, targetID)
			if graph.count() >= maxReferenceFanout {
				continue
			}
			if resolved, err := fetchObject(rc.Ctx, s, targetClass, targetID, tenant); err == nil {
				graph.addNode(objectNode(resolved, false))
			} else {
				graph.addNode(graphNode{ID: target, Label: shortID(targetID), Group: targetClass, Summary: "unresolved reference"})
			}
			graph.addEdge(graphEdge{Source: objectNodeID(className, id), Target: target, Label: property})
		}
	}

	dump, err := s.client.schema.Getter().Do(rc.Ctx)
	if err == nil {
		for _, source := range sortedClasses(dump.Classes) {
			for _, property := range source.Properties {
				if property == nil || len(property.DataType) == 0 || property.DataType[0] != className {
					continue
				}
				for _, inbound := range inboundReferences(rc, source.Class, property.Name, className, id) {
					graph.addNode(inbound)
					graph.addEdge(graphEdge{Source: inbound.ID, Target: objectNodeID(className, id), Label: property.Name})
				}
			}
		}
	}
	return graph.result(), nil
}

func inboundReferences(rc *plugin.RequestContext, sourceClass, property, targetClass, targetID string) []graphNode {
	s, err := session(rc)
	if err != nil {
		return nil
	}
	class, err := s.client.schema.ClassGetter().WithClassName(sourceClass).Do(rc.Ctx)
	if err != nil {
		return nil
	}
	where := filters.Where().
		WithPath([]string{property, targetClass, "id"}).
		WithOperator(filters.Equal).
		WithValueText(targetID)
	fields := []graphql.Field{additionalField("id")}
	if label := labelProperty(class); label != "" {
		fields = append(fields, graphql.Field{Name: label})
	}
	resp, err := s.client.graphql.Get().
		WithClassName(sourceClass).
		WithFields(fields...).
		WithWhere(where).
		WithLimit(maxInboundPerClass).
		Do(rc.Ctx)
	if err != nil || graphqlErrors(resp) != nil {
		return nil
	}
	items := getResultItems(resp, sourceClass)
	nodes := make([]graphNode, 0, len(items))
	for _, item := range items {
		additional, _ := item["_additional"].(map[string]any)
		id := text(additional["id"])
		if id == "" {
			continue
		}
		nodes = append(nodes, graphNode{
			ID:      objectNodeID(sourceClass, id),
			Label:   summaryLabel(item, sourceClass, id),
			Group:   sourceClass,
			Summary: sourceClass,
		})
	}
	return nodes
}

func labelProperty(class *models.Class) string {
	for _, property := range class.Properties {
		if property == nil || len(property.DataType) == 0 {
			continue
		}
		if property.DataType[0] == "text" {
			return property.Name
		}
	}
	return ""
}

func summaryLabel(item map[string]any, className, id string) string {
	for key, value := range item {
		if key == "_additional" {
			continue
		}
		if str, ok := value.(string); ok && strings.TrimSpace(str) != "" {
			return truncate(str, 42)
		}
	}
	return className + " " + shortID(id)
}

func objectNode(object *models.Object, root bool) graphNode {
	properties, _ := object.Properties.(map[string]any)
	label := summaryLabel(properties, object.Class, string(object.ID))
	fields := make([]graphField, 0, len(properties))
	keys := make([]string, 0, len(properties))
	for key := range properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fields = append(fields, graphField{Name: key, Key: truncate(text(properties[key]), 64)})
	}
	summary := object.Class
	if root {
		summary = object.Class + " (selected)"
	}
	return graphNode{
		ID:      objectNodeID(object.Class, string(object.ID)),
		Label:   label,
		Group:   object.Class,
		Summary: summary,
		Fields:  fields,
		Properties: map[string]any{
			"id":         string(object.ID),
			"collection": object.Class,
			"vectorDims": len(objectVector(object)),
		},
	}
}

func beaconsOf(value any) []string {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if beacon := text(entry["beacon"]); beacon != "" {
			out = append(out, beacon)
		}
	}
	return out
}

// beaconClass reads the class segment from weaviate://localhost/<Class>/<uuid>.
// Legacy beacons omit the class, in which case the caller falls back.
func beaconClass(beacon string) string {
	parts := strings.Split(strings.TrimPrefix(beacon, "weaviate://"), "/")
	if len(parts) < 3 {
		return ""
	}
	if classNamePattern.MatchString(parts[1]) {
		return parts[1]
	}
	return ""
}

func shortID(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}

func truncate(value string, max int) string {
	value = strings.TrimSpace(value)
	if len(value) <= max {
		return value
	}
	return value[:max-1] + "…"
}

func round(value float64, digits int) float64 {
	factor := 1.0
	for i := 0; i < digits; i++ {
		factor *= 10
	}
	return float64(int64(value*factor+0.5)) / factor
}
