package arangodb

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	driver "github.com/arangodb/go-driver/v2/arangodb"

	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

// systemAttributes are server-managed and never writable through the grid.
var systemAttributes = map[string]bool{"_key": true, "_id": true, "_rev": true}

const columnSampleSize = 200

func documentsList(rc *plugin.RequestContext) (any, error) {
	s, database, collection, err := collectionScope(rc)
	if err != nil {
		return nil, err
	}
	req, err := rc.Page()
	if err != nil {
		return nil, err
	}
	offset, err := cursorOffset(req.Cursor)
	if err != nil {
		return nil, err
	}
	bind := map[string]any{"@col": collection, "offset": offset, "limit": req.Limit}
	filter := ""
	if term := strings.TrimSpace(req.Search()); term != "" {
		filter = " FILTER CONTAINS(LOWER(JSON_STRINGIFY(doc)), LOWER(@q))"
		bind["q"] = term
	}
	aql := "FOR doc IN @@col" + filter
	if len(req.Sort) > 0 {
		attribute, err := safeAttribute(req.Sort[0].Field)
		if err != nil {
			return nil, err
		}
		bind["sortAttr"] = attribute
		if req.Sort[0].Desc {
			aql += " SORT doc.@sortAttr DESC"
		} else {
			aql += " SORT doc.@sortAttr ASC"
		}
	} else {
		aql += " SORT doc._key ASC"
	}
	aql += " LIMIT @offset, @limit RETURN doc"

	rows, err := s.queryRows(rc.Ctx, database, aql, bind, req.Limit)
	if err != nil {
		return nil, err
	}
	// The total must describe the same set the page came from, so a search term
	// counts the filtered documents rather than the whole collection.
	countAQL := "RETURN LENGTH(@@col)"
	countBind := map[string]any{"@col": collection}
	if filter != "" {
		countAQL = "FOR doc IN @@col" + filter + " COLLECT WITH COUNT INTO matches RETURN matches"
		countBind["q"] = bind["q"]
	}
	total := int(s.queryInt(rc.Ctx, database, countAQL, countBind))
	return plugin.Page[row]{Items: rows, NextCursor: nextCursor(offset, len(rows), req.Limit), Total: &total}, nil
}

// documentColumns derives the grid's runtime columns from a bounded sample plus
// the collection's own key attributes, so an edge collection always exposes
// _from/_to even when the sample is empty.
func documentColumns(rc *plugin.RequestContext) (any, error) {
	s, database, collection, err := collectionScope(rc)
	if err != nil {
		return nil, err
	}
	edge, err := s.isEdgeCollection(rc.Ctx, database, collection)
	if err != nil {
		return nil, err
	}
	sample, err := s.queryRows(rc.Ctx, database,
		"FOR doc IN @@col LIMIT @limit RETURN doc",
		map[string]any{"@col": collection, "limit": columnSampleSize}, columnSampleSize)
	if err != nil {
		return nil, err
	}

	types := map[string]string{"_key": "string", "_id": "string", "_rev": "string"}
	order := []string{"_key", "_id", "_rev"}
	if edge {
		types["_from"] = "string"
		types["_to"] = "string"
		order = append(order, "_from", "_to")
	}
	pinned := make(map[string]bool, len(order))
	for _, name := range order {
		pinned[name] = true
	}
	// A null in the first document must not fix the column's type, so a later
	// non-null value still wins.
	for _, item := range sample {
		for name, value := range item {
			if pinned[name] {
				continue
			}
			if kind, seen := types[name]; !seen || kind == "null" {
				types[name] = jsonKind(value)
			}
		}
	}
	names := make([]string, 0, len(types))
	for name := range types {
		if !pinned[name] {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	order = append(order, names...)

	readOnly := s.opts.ReadOnly
	columns := make([]row, 0, len(order))
	for _, name := range order {
		kind := types[name]
		column := row{
			"name":     name,
			"key":      name,
			"label":    name,
			"type":     kind,
			"nullable": !systemAttributes[name],
		}
		locked := readOnly || systemAttributes[name] || sqldb.RedactColumn(name, s.opts.RedactPatterns)
		column["readOnly"] = locked
		column["editable"] = !locked
		column["columnType"] = string(columnTypeFor(kind))
		if !locked {
			column["editor"] = string(columnEditorFor(kind))
		}
		columns = append(columns, column)
	}
	return plugin.Page[row]{Items: columns}, nil
}

func documentInsert(rc *plugin.RequestContext) (any, error) {
	s, database, collection, mutation, err := documentMutation(rc)
	if err != nil {
		return nil, err
	}
	values, err := writableAttributes(s, mutation.Values, true)
	if err != nil {
		return nil, err
	}
	created, err := s.queryOne(rc.Ctx, database, "INSERT @doc INTO @@col RETURN NEW", map[string]any{"@col": collection, "doc": values})
	if err != nil {
		return nil, err
	}
	return asMap(created), nil
}

func documentUpdate(rc *plugin.RequestContext) (any, error) {
	s, database, collection, mutation, err := documentMutation(rc)
	if err != nil {
		return nil, err
	}
	key, err := documentKey(mutation.Key)
	if err != nil {
		return nil, err
	}
	values, err := writableAttributes(s, mutation.Values, false)
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("%w: no writable attributes in the update", plugin.ErrInvalidInput)
	}
	if _, err := s.execute(rc.Ctx, database, "UPDATE @key WITH @values IN @@col",
		map[string]any{"@col": collection, "key": key, "values": values}); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func documentDelete(rc *plugin.RequestContext) (any, error) {
	s, database, collection, mutation, err := documentMutation(rc)
	if err != nil {
		return nil, err
	}
	key, err := documentKey(mutation.Key)
	if err != nil {
		return nil, err
	}
	if _, err := s.execute(rc.Ctx, database, "REMOVE @key IN @@col",
		map[string]any{"@col": collection, "key": key}); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

// documentRead backs the single-document code editor.
func documentRead(rc *plugin.RequestContext) (any, error) {
	s, database, collection, key, err := documentScope(rc)
	if err != nil {
		return nil, err
	}
	doc, err := s.queryOne(rc.Ctx, database, "RETURN DOCUMENT(@@col, @key)", map[string]any{"@col": collection, "key": key})
	if err != nil {
		return nil, err
	}
	if doc == nil {
		return nil, fmt.Errorf("%w: document %q", plugin.ErrNotFound, key)
	}
	return editorContent(doc)
}

// documentReplace applies the code editor buffer as a full document replacement
// and returns the canonical stored document so the editor can reset its baseline.
func documentReplace(rc *plugin.RequestContext) (any, error) {
	s, database, collection, key, err := documentScope(rc)
	if err != nil {
		return nil, err
	}
	body, err := parseEditorBody(rc, "document")
	if err != nil {
		return nil, err
	}
	values, err := writableAttributes(s, body, false)
	if err != nil {
		return nil, err
	}
	if _, err := s.execute(rc.Ctx, database, "REPLACE @key WITH @doc IN @@col",
		map[string]any{"@col": collection, "key": key, "doc": values}); err != nil {
		return nil, err
	}
	doc, err := s.queryOne(rc.Ctx, database, "RETURN DOCUMENT(@@col, @key)", map[string]any{"@col": collection, "key": key})
	if err != nil {
		return nil, err
	}
	return editorContent(doc)
}

func documentMutation(rc *plugin.RequestContext) (*Session, string, string, sqldb.RowMutation, error) {
	s, database, collection, err := collectionScope(rc)
	if err != nil {
		return nil, "", "", sqldb.RowMutation{}, err
	}
	if s.opts.ReadOnly {
		return nil, "", "", sqldb.RowMutation{}, fmt.Errorf("%w: connection is read-only", plugin.ErrForbidden)
	}
	var mutation sqldb.RowMutation
	if err := rc.Bind(&mutation); err != nil {
		return nil, "", "", sqldb.RowMutation{}, err
	}
	return s, database, collection, mutation, nil
}

// documentKey extracts the identifying _key from a grid row key. Any other shape
// is refused so a mutation can never widen into a multi-document statement.
func documentKey(key map[string]any) (string, error) {
	if err := sqldb.ValidateRowKey([]string{"_key"}, key); err != nil {
		return "", err
	}
	value, ok := key["_key"].(string)
	if !ok {
		return "", fmt.Errorf("%w: _key must be a string", plugin.ErrInvalidInput)
	}
	return safeKey(value)
}

// writableAttributes drops server-managed and redacted attributes so an edited
// row can never rewrite _id/_rev or push a masked placeholder back into storage.
func writableAttributes(s *Session, values map[string]any, allowKey bool) (map[string]any, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("%w: document body is required", plugin.ErrInvalidInput)
	}
	out := make(map[string]any, len(values))
	for name, value := range values {
		if !attributeName(name) {
			return nil, fmt.Errorf("%w: invalid attribute name %q", plugin.ErrInvalidInput, name)
		}
		if name == "_key" {
			if !allowKey {
				continue
			}
			key, err := safeKey(fmt.Sprint(value))
			if err != nil {
				return nil, err
			}
			out[name] = key
			continue
		}
		if systemAttributes[name] {
			continue
		}
		if sqldb.RedactColumn(name, s.opts.RedactPatterns) {
			continue
		}
		out[name] = value
	}
	if len(out) == 0 && allowKey {
		return nil, fmt.Errorf("%w: document has no writable attributes", plugin.ErrInvalidInput)
	}
	return out, nil
}

func attributeName(name string) bool {
	if name == "" || len(name) > 256 {
		return false
	}
	for _, r := range name {
		if r < 0x20 || r == '.' || r == '`' || r == '"' || r == '\'' {
			return false
		}
	}
	return true
}

// safeAttribute validates a sort attribute before it is bound as an AQL
// attribute bind parameter.
func safeAttribute(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if !attributeName(raw) {
		return "", fmt.Errorf("%w: invalid sort attribute %q", plugin.ErrInvalidInput, raw)
	}
	return raw, nil
}

func (s *Session) isEdgeCollection(ctx context.Context, database, collection string) (bool, error) {
	col, err := s.collection(ctx, database, collection)
	if err != nil {
		return false, err
	}
	tctx, cancel := context.WithTimeout(ctx, s.opts.Timeout)
	defer cancel()
	props, err := col.Properties(tctx)
	if err != nil {
		return false, arangoErr(err)
	}
	return props.Type == driver.CollectionTypeEdge, nil
}

func jsonKind(value any) string {
	switch value.(type) {
	case nil:
		return "null"
	case bool:
		return "bool"
	case float64, int, int64, json.Number:
		return "number"
	case string:
		return "string"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	default:
		return "string"
	}
}

func columnTypeFor(kind string) plugin.ColumnType {
	switch kind {
	case "bool":
		return plugin.ColumnBool
	case "number":
		return plugin.ColumnNumber
	case "array", "object":
		return plugin.ColumnJSON
	default:
		return plugin.ColumnText
	}
}

func columnEditorFor(kind string) plugin.ColumnEditor {
	switch kind {
	case "bool":
		return plugin.ColumnEditorToggle
	case "number":
		return plugin.ColumnEditorNumber
	case "array", "object":
		return plugin.ColumnEditorJSON
	default:
		return plugin.ColumnEditorText
	}
}
