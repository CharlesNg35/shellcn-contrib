package milvus

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/milvusclient"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

// entityKeyField is the synthetic row key the editable grid addresses rows by;
// the real primary-key name differs per collection.
const entityKeyField = "_id"

func entityColumns(rc *plugin.RequestContext) (any, error) {
	collection, err := collectionParam(rc)
	if err != nil {
		return nil, err
	}
	s, c, err := clientOf(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	coll, err := c.DescribeCollection(ctx, milvusclient.NewDescribeCollectionOption(collection))
	if err != nil {
		return nil, mapError(err)
	}
	rows := make([]row, 0, len(coll.Schema.Fields)+1)
	for _, f := range coll.Schema.Fields {
		editable := !s.opts.ReadOnly && !f.PrimaryKey && !f.IsDynamic
		rows = append(rows, row{
			"name":     f.Name,
			"label":    f.Name,
			"type":     string(gridColumnType(f)),
			"editor":   string(gridColumnEditor(f)),
			"editable": editable,
			"readOnly": !editable,
			"nullable": f.Nullable,
		})
	}
	if coll.Schema.EnableDynamicField {
		rows = append(rows, row{
			"name": "$meta", "label": "$meta", "type": string(plugin.ColumnJSON),
			"editor": string(plugin.ColumnEditorJSON), "editable": false, "readOnly": true, "nullable": true,
		})
	}
	return plugin.Page[row]{Items: rows}, nil
}

func gridColumnType(f *entity.Field) plugin.ColumnType {
	switch {
	case f.DataType.IsVectorType(), f.DataType == entity.FieldTypeJSON, f.DataType == entity.FieldTypeArray:
		return plugin.ColumnJSON
	case f.DataType == entity.FieldTypeBool:
		return plugin.ColumnBool
	case f.DataType == entity.FieldTypeInt8, f.DataType == entity.FieldTypeInt16,
		f.DataType == entity.FieldTypeInt32, f.DataType == entity.FieldTypeInt64,
		f.DataType == entity.FieldTypeFloat, f.DataType == entity.FieldTypeDouble:
		return plugin.ColumnNumber
	default:
		return plugin.ColumnText
	}
}

func gridColumnEditor(f *entity.Field) plugin.ColumnEditor {
	switch gridColumnType(f) {
	case plugin.ColumnJSON:
		return plugin.ColumnEditorJSON
	case plugin.ColumnBool:
		return plugin.ColumnEditorToggle
	case plugin.ColumnNumber:
		return plugin.ColumnEditorNumber
	default:
		return plugin.ColumnEditorText
	}
}

func listEntities(rc *plugin.RequestContext) (any, error) {
	collection, err := collectionParam(rc)
	if err != nil {
		return nil, err
	}
	s, c, err := clientOf(rc)
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
		parsed, okCursor := parseInt(req.Cursor)
		if !okCursor || parsed < 0 {
			return nil, fmt.Errorf("%w: invalid cursor", plugin.ErrInvalidInput)
		}
		offset = parsed
	}

	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	coll, err := c.DescribeCollection(ctx, milvusclient.NewDescribeCollectionOption(collection))
	if err != nil {
		return nil, mapError(err)
	}
	pk := primaryKeyName(coll.Schema)

	opt := milvusclient.NewQueryOption(collection).WithLimit(limit).WithOutputFields("*")
	if offset > 0 {
		opt = opt.WithOffset(offset)
	}
	if filter := strings.TrimSpace(rc.Query().Get("filter")); filter != "" {
		opt = opt.WithFilter(filter)
	}
	if partition := strings.TrimSpace(rc.Param("partition")); partition != "" {
		name, err := identifier("partition", partition)
		if err != nil {
			return nil, err
		}
		opt = opt.WithPartitions(name)
	}
	rs, err := c.Query(ctx, opt)
	if err != nil {
		return nil, mapError(err)
	}
	rows := resultRows(rs, pk)
	next := ""
	if len(rows) == limit {
		next = strconv.Itoa(offset + limit)
	}
	return plugin.Page[row]{Items: plugin.FilterRows(rows, req.Search()), NextCursor: next}, nil
}

func insertEntity(rc *plugin.RequestContext) (any, error) {
	collection, coll, s, c, err := entityTarget(rc)
	if err != nil {
		return nil, err
	}
	values, err := mutationRows(rc)
	if err != nil {
		return nil, err
	}
	columns, err := buildColumns(coll.Schema, values, false)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	res, err := c.Insert(ctx, milvusclient.NewColumnBasedInsertOption(collection, columns...))
	s.activity.record("entity.insert", collection, fmt.Sprintf("Inserted %d entity(ies)", len(values)), err)
	if err != nil {
		return nil, mapError(err)
	}
	return row{"ok": true, "inserted": res.InsertCount}, nil
}

func updateEntity(rc *plugin.RequestContext) (any, error) {
	collection, coll, s, c, err := entityTarget(rc)
	if err != nil {
		return nil, err
	}
	values, err := mutationRows(rc)
	if err != nil {
		return nil, err
	}
	columns, err := buildColumns(coll.Schema, values, true)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	res, err := c.Upsert(ctx, milvusclient.NewColumnBasedInsertOption(collection, columns...))
	s.activity.record("entity.update", collection, fmt.Sprintf("Upserted %d entity(ies)", len(values)), err)
	if err != nil {
		return nil, mapError(err)
	}
	return row{"ok": true, "upserted": res.UpsertCount}, nil
}

func deleteEntity(rc *plugin.RequestContext) (any, error) {
	collection, coll, s, c, err := entityTarget(rc)
	if err != nil {
		return nil, err
	}
	values, err := mutationRows(rc)
	if err != nil {
		return nil, err
	}
	pkField := primaryKeyField(coll.Schema)
	if pkField == nil {
		return nil, fmt.Errorf("%w: collection has no primary key", plugin.ErrConflict)
	}
	ids := make([]any, 0, len(values))
	for _, item := range values {
		value, ok := item[entityKeyField]
		if !ok {
			value, ok = item[pkField.Name]
		}
		if !ok {
			return nil, fmt.Errorf("%w: row key is required", plugin.ErrInvalidInput)
		}
		ids = append(ids, value)
	}

	opt := milvusclient.NewDeleteOption(collection)
	if pkField.DataType == entity.FieldTypeVarChar {
		keys := make([]string, 0, len(ids))
		for _, id := range ids {
			text, err := asString(id)
			if err != nil {
				return nil, err
			}
			keys = append(keys, text)
		}
		opt = opt.WithStringIDs(pkField.Name, keys)
	} else {
		keys := make([]int64, 0, len(ids))
		for _, id := range ids {
			number, err := asInt64(id)
			if err != nil {
				return nil, err
			}
			keys = append(keys, number)
		}
		opt = opt.WithInt64IDs(pkField.Name, keys)
	}
	if partition := strings.TrimSpace(rc.Param("partition")); partition != "" {
		name, err := identifier("partition", partition)
		if err != nil {
			return nil, err
		}
		opt = opt.WithPartition(name)
	}

	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	res, err := c.Delete(ctx, opt)
	s.activity.record("entity.delete", collection, fmt.Sprintf("Deleted %d entity(ies)", len(ids)), err)
	if err != nil {
		return nil, mapError(err)
	}
	return row{"ok": true, "deleted": res.DeleteCount}, nil
}

func entityTarget(rc *plugin.RequestContext) (string, *entity.Collection, *Session, *milvusclient.Client, error) {
	collection, err := collectionParam(rc)
	if err != nil {
		return "", nil, nil, nil, err
	}
	s, c, err := writableClient(rc)
	if err != nil {
		return "", nil, nil, nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	coll, err := c.DescribeCollection(ctx, milvusclient.NewDescribeCollectionOption(collection))
	if err != nil {
		return "", nil, nil, nil, mapError(err)
	}
	return collection, coll, s, c, nil
}

// mutationRows accepts the grid's single-row body, an explicit {"rows": [...]}
// envelope, or a bare array so bulk edits and the JSON editor share one route.
func mutationRows(rc *plugin.RequestContext) ([]map[string]any, error) {
	raw := rc.Body()
	if len(raw) == 0 {
		return nil, fmt.Errorf("%w: request body is required", plugin.ErrInvalidInput)
	}
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, fmt.Errorf("%w: body must be JSON", plugin.ErrInvalidInput)
	}
	switch value := decoded.(type) {
	case []any:
		return objectList(value)
	case map[string]any:
		if rows, ok := value["rows"]; ok {
			list, ok := rows.([]any)
			if !ok {
				return nil, fmt.Errorf("%w: rows must be an array", plugin.ErrInvalidInput)
			}
			return objectList(list)
		}
		if inner, ok := value["row"].(map[string]any); ok {
			return []map[string]any{inner}, nil
		}
		if content, ok := value["content"].(string); ok && len(value) == 1 {
			var parsed any
			if err := json.Unmarshal([]byte(content), &parsed); err != nil {
				return nil, fmt.Errorf("%w: content must be JSON", plugin.ErrInvalidInput)
			}
			switch inner := parsed.(type) {
			case []any:
				return objectList(inner)
			case map[string]any:
				return []map[string]any{inner}, nil
			}
			return nil, fmt.Errorf("%w: content must be a JSON object or array", plugin.ErrInvalidInput)
		}
		if key, ok := value["key"].(map[string]any); ok && len(value) == 1 {
			return []map[string]any{key}, nil
		}
		return []map[string]any{value}, nil
	default:
		return nil, fmt.Errorf("%w: body must be a JSON object or array", plugin.ErrInvalidInput)
	}
}

func objectList(items []any) ([]map[string]any, error) {
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		object, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%w: every row must be a JSON object", plugin.ErrInvalidInput)
		}
		out = append(out, object)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: at least one row is required", plugin.ErrInvalidInput)
	}
	return out, nil
}

func primaryKeyField(schema *entity.Schema) *entity.Field {
	for _, f := range schema.Fields {
		if f.PrimaryKey {
			return f
		}
	}
	return nil
}

// buildColumns converts decoded JSON rows into typed Milvus columns. Insert
// skips an auto-id primary key; upsert always requires it.
func buildColumns(schema *entity.Schema, rows []map[string]any, includeAutoID bool) ([]column.Column, error) {
	columns := make([]column.Column, 0, len(schema.Fields))
	for _, f := range schema.Fields {
		if f.IsDynamic {
			continue
		}
		if f.PrimaryKey && f.AutoID && !includeAutoID {
			continue
		}
		col, err := buildColumn(f, rows)
		if err != nil {
			return nil, err
		}
		columns = append(columns, col)
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("%w: no writable fields", plugin.ErrInvalidInput)
	}
	return columns, nil
}

func buildColumn(f *entity.Field, rows []map[string]any) (column.Column, error) {
	values := make([]any, len(rows))
	for i, item := range rows {
		value, ok := item[f.Name]
		if !ok && f.PrimaryKey {
			value, ok = item[entityKeyField]
		}
		if !ok || value == nil {
			if !f.Nullable {
				return nil, fmt.Errorf("%w: field %q is required", plugin.ErrInvalidInput, f.Name)
			}
			values[i] = nil
			continue
		}
		values[i] = value
	}

	switch f.DataType {
	case entity.FieldTypeBool:
		return typedColumn(f, values, asBool, column.NewColumnBool)
	case entity.FieldTypeInt8:
		return typedColumn(f, values, asInt[int8], column.NewColumnInt8)
	case entity.FieldTypeInt16:
		return typedColumn(f, values, asInt[int16], column.NewColumnInt16)
	case entity.FieldTypeInt32:
		return typedColumn(f, values, asInt[int32], column.NewColumnInt32)
	case entity.FieldTypeInt64:
		return typedColumn(f, values, asInt64, column.NewColumnInt64)
	case entity.FieldTypeFloat:
		return typedColumn(f, values, asFloat[float32], column.NewColumnFloat)
	case entity.FieldTypeDouble:
		return typedColumn(f, values, asFloat[float64], column.NewColumnDouble)
	case entity.FieldTypeVarChar:
		return typedColumn(f, values, asString, column.NewColumnVarChar)
	case entity.FieldTypeJSON:
		return typedColumn(f, values, asJSONBytes, column.NewColumnJSONBytes)
	case entity.FieldTypeFloatVector:
		dim, err := f.GetDim()
		if err != nil {
			return nil, fmt.Errorf("%w: field %q has no dimension", plugin.ErrInvalidInput, f.Name)
		}
		data := make([][]float32, 0, len(values))
		for _, value := range values {
			vector, err := asFloat32Slice(value)
			if err != nil {
				return nil, fmt.Errorf("%w: field %q: %v", plugin.ErrInvalidInput, f.Name, err)
			}
			if int64(len(vector)) != dim {
				return nil, fmt.Errorf("%w: field %q expects %d dimensions, got %d", plugin.ErrInvalidInput, f.Name, dim, len(vector))
			}
			data = append(data, vector)
		}
		return column.NewColumnFloatVector(f.Name, int(dim), data), nil
	default:
		return nil, fmt.Errorf("%w: field %q of type %s cannot be edited from the grid", plugin.ErrNotSupported, f.Name, fieldTypeName(f.DataType))
	}
}

func typedColumn[T any, C column.Column](f *entity.Field, values []any, convert func(any) (T, error), build func(string, []T) C) (column.Column, error) {
	col := build(f.Name, nil)
	for _, value := range values {
		if value == nil {
			col.SetNullable(true)
			break
		}
	}
	for _, value := range values {
		if value == nil {
			if err := col.AppendNull(); err != nil {
				return nil, fmt.Errorf("%w: field %q: %v", plugin.ErrInvalidInput, f.Name, err)
			}
			continue
		}
		converted, err := convert(value)
		if err != nil {
			return nil, fmt.Errorf("%w: field %q: %v", plugin.ErrInvalidInput, f.Name, err)
		}
		if err := col.AppendValue(converted); err != nil {
			return nil, fmt.Errorf("%w: field %q: %v", plugin.ErrInvalidInput, f.Name, err)
		}
	}
	return col, nil
}

func resultRows(rs milvusclient.ResultSet, pk string) []row {
	count := rs.ResultCount
	rows := make([]row, 0, count)
	for i := 0; i < count; i++ {
		item := row{}
		for _, col := range rs.Fields {
			item[col.Name()] = cellValue(col, i)
		}
		if rs.IDs != nil {
			if _, ok := item[rs.IDs.Name()]; !ok {
				item[rs.IDs.Name()] = cellValue(rs.IDs, i)
			}
		}
		if i < len(rs.Scores) {
			item["score"] = round4(float64(rs.Scores[i]))
		}
		if pk != "" {
			if value, ok := item[pk]; ok {
				item[entityKeyField] = value
			}
		}
		rows = append(rows, item)
	}
	return rows
}

func cellValue(col column.Column, i int) any {
	value, err := col.Get(i)
	if err != nil {
		return nil
	}
	switch typed := value.(type) {
	case []byte:
		var decoded any
		if json.Unmarshal(typed, &decoded) == nil {
			return decoded
		}
		return string(typed)
	case entity.SparseEmbedding:
		return sparseValue(typed)
	default:
		return value
	}
}

func sparseValue(embedding entity.SparseEmbedding) map[string]any {
	indices := make([]uint32, 0, embedding.Len())
	values := make([]float32, 0, embedding.Len())
	for i := 0; i < embedding.Len(); i++ {
		idx, value, ok := embedding.Get(i)
		if !ok {
			continue
		}
		indices = append(indices, idx)
		values = append(values, value)
	}
	return map[string]any{"indices": indices, "values": values}
}

func queryCompletions(rc *plugin.RequestContext) (any, error) {
	collection, err := collectionParam(rc)
	if err != nil {
		return nil, err
	}
	s, c, err := clientOf(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	coll, err := c.DescribeCollection(ctx, milvusclient.NewDescribeCollectionOption(collection))
	if err != nil {
		return nil, mapError(err)
	}
	items := make([]row, 0, len(coll.Schema.Fields)+16)
	for _, f := range coll.Schema.Fields {
		items = append(items, row{
			"label": f.Name, "type": "field", "detail": fieldTypeName(f.DataType), "apply": f.Name,
		})
	}
	if names, err := c.ListPartitions(ctx, milvusclient.NewListPartitionOption(collection)); err == nil {
		sort.Strings(names)
		for _, name := range names {
			items = append(items, row{"label": name, "type": "partition", "detail": "partition", "apply": name})
		}
	}
	for _, keyword := range []string{"like", "in", "not in", "json_contains", "array_contains", "TEXT_MATCH"} {
		items = append(items, row{"label": keyword, "type": "keyword", "detail": "filter expression", "apply": keyword})
	}
	for _, tpl := range queryTemplates(coll.Schema) {
		items = append(items, tpl)
	}
	return plugin.Page[row]{Items: items}, nil
}

// queryTemplates offers starting points the console can run unedited: the ANN
// template carries a vector of the collection's own dimension, the scalar
// templates follow the primary key's type, and a template is only offered when
// the schema has the field it needs.
func queryTemplates(schema *entity.Schema) []row {
	templates := []row{{
		"label": "List entities", "type": "template", "detail": "query",
		"apply": `{"filter":"","limit":20,"output":["*"]}`,
	}}
	if dense := floatVectorField(schema); dense != nil {
		templates = append(templates, row{
			"label": "ANN search", "type": "template", "detail": "vector similarity",
			"apply": fmt.Sprintf(`{"field":%q,"vector":%s,"limit":10,"output":["*"]}`, dense.Name, jsonValue(sampleVector(fieldDim(dense)))),
		})
	}
	if sparse := sparseVectorField(schema); sparse != nil {
		templates = append(templates, row{
			"label": "Full-text search", "type": "template", "detail": "BM25 / sparse",
			"apply": fmt.Sprintf(`{"field":%q,"text":"search terms","limit":10,"output":["*"]}`, sparse.Name),
		})
	}
	pk := primaryKeyField(schema)
	if pk == nil {
		return templates
	}
	filter := pk.Name + " >= 0"
	var ids any = []int64{1, 2, 3}
	if pk.DataType == entity.FieldTypeVarChar {
		filter = pk.Name + ` != ""`
		ids = []string{"id-1", "id-2"}
	}
	return append(templates,
		row{"label": "Scalar filter", "type": "template", "detail": "query",
			"apply": fmt.Sprintf(`{"filter":%s,"limit":25,"output":["*"]}`, jsonValue(filter))},
		row{"label": "Primary key lookup", "type": "template", "detail": "get",
			"apply": fmt.Sprintf(`{"ids":%s,"output":["*"]}`, jsonValue(ids))},
	)
}

// floatVectorField is the only dense type a JSON float array can probe, so it
// is the only one the ANN template is offered for.
func floatVectorField(schema *entity.Schema) *entity.Field {
	for _, f := range schema.Fields {
		if f.DataType == entity.FieldTypeFloatVector && fieldDim(f) > 0 {
			return f
		}
	}
	return nil
}

func sparseVectorField(schema *entity.Schema) *entity.Field {
	for _, f := range schema.Fields {
		if f.DataType == entity.FieldTypeSparseVector {
			return f
		}
	}
	return nil
}

func fieldDim(f *entity.Field) int {
	dim, err := strconv.Atoi(f.TypeParams[entity.TypeParamDim])
	if err != nil || dim <= 0 {
		return 0
	}
	return dim
}

// sampleVector is the query vector the ANN template ships with: every component
// is equal, so it is a valid non-zero probe under L2, IP and COSINE alike.
func sampleVector(dim int) []float64 {
	values := make([]float64, dim)
	for i := range values {
		values[i] = 0.1
	}
	return values
}

func jsonValue(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return "null"
	}
	return string(data)
}

func listSavedQueries(rc *plugin.RequestContext) (any, error) {
	if rc.Storage == nil {
		return nil, fmt.Errorf("%w: plugin storage is unavailable", plugin.ErrUnavailable)
	}
	items, err := rc.Storage.List(rc.Ctx, plugin.ConnectionStorage(savedQueryCollection), &plugin.StorageListFilter{
		KeyPrefix: savedQueryPrefix, ContentType: "application/json",
	})
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(items))
	for _, item := range items {
		var value savedQuery
		if err := json.Unmarshal(item.Value, &value); err != nil {
			continue
		}
		rows = append(rows, row{
			"id":         item.Key,
			"name":       value.Name,
			"collection": value.Collection,
			"query":      value.Query,
			"updated_at": item.UpdatedAt,
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		return strings.ToLower(fmt.Sprint(rows[i]["name"])) < strings.ToLower(fmt.Sprint(rows[j]["name"]))
	})
	return broker.PageRows(rc, rows)
}

func saveQuery(rc *plugin.RequestContext) (any, error) {
	if rc.Storage == nil {
		return nil, fmt.Errorf("%w: plugin storage is unavailable", plugin.ErrUnavailable)
	}
	var req savedQuery
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	req.Name = strings.TrimSpace(req.Name)
	req.Query = strings.TrimSpace(req.Query)
	if req.Name == "" || req.Query == "" {
		return nil, fmt.Errorf("%w: name and query are required", plugin.ErrInvalidInput)
	}
	if len(req.Query) > 64*1024 {
		return nil, fmt.Errorf("%w: query is too large", plugin.ErrInvalidInput)
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	key := savedQueryPrefix + strconv.FormatInt(nowUnixNano(), 36)
	item, err := rc.Storage.Put(rc.Ctx, savedQueryCollection, plugin.StorageItem{
		Key:         key,
		Value:       payload,
		ContentType: "application/json",
		Metadata:    map[string]string{"name": req.Name, "collection": req.Collection},
	})
	if err != nil {
		return nil, err
	}
	return row{"ok": true, "id": item.Key, "name": req.Name}, nil
}

func deleteSavedQuery(rc *plugin.RequestContext) (any, error) {
	if rc.Storage == nil {
		return nil, fmt.Errorf("%w: plugin storage is unavailable", plugin.ErrUnavailable)
	}
	id := strings.TrimSpace(rc.Param("id"))
	if id == "" || !strings.HasPrefix(id, savedQueryPrefix) {
		return nil, fmt.Errorf("%w: unknown saved query", plugin.ErrInvalidInput)
	}
	if err := rc.Storage.Delete(rc.Ctx, plugin.ConnectionStorage(savedQueryCollection), id); err != nil {
		return nil, err
	}
	return row{"ok": true, "id": id}, nil
}

type savedQuery struct {
	Name       string `json:"name"`
	Collection string `json:"collection"`
	Query      string `json:"query"`
}

const (
	savedQueryCollection = "saved_queries"
	savedQueryPrefix     = "query/"
)

// collectionSchemaOf is the shared describe used by the query and projection
// streams, which run outside a single request timeout.
func collectionSchemaOf(ctx context.Context, c *milvusclient.Client, collection string) (*entity.Collection, error) {
	coll, err := c.DescribeCollection(ctx, milvusclient.NewDescribeCollectionOption(collection))
	if err != nil {
		return nil, mapError(err)
	}
	return coll, nil
}
