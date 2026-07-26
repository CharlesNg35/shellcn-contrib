package couchbase

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

// Metadata columns the grid shows read-only; they are projected by META() and
// never written back into the document body.
var documentMetaFields = []string{"id", "key", "cas", "expiration", "flags"}

func isMetaField(name string) bool {
	for _, field := range documentMetaFields {
		if field == name {
			return true
		}
	}
	return false
}

func documentRef(k keyspace, id string) plugin.ResourceIdentity {
	doc := k
	doc.Document = id
	return plugin.ResourceIdentity{
		Kind:      "document",
		Scope:     k.uid(),
		Namespace: k.Scope,
		Name:      id,
		UID:       doc.uid(),
	}
}

func requireCollection(rc *plugin.RequestContext, s *Session) (keyspace, error) {
	k, err := keyspaceFrom(rc, s)
	if err != nil {
		return keyspace{}, err
	}
	if k.Collection == "" {
		return keyspace{}, fmt.Errorf("%w: a collection is required", plugin.ErrInvalidInput)
	}
	return k, nil
}

// documentRows projects documents with their metadata. Document ids are bound as
// query arguments; only the keyspace is interpolated, and it is whitelisted.
func (s *Session) documentRows(rc *plugin.RequestContext, k keyspace, limit, offset int, search string) ([]row, error) {
	statement := "SELECT META(d).id AS __id, META(d).cas AS __cas, META(d).expiration AS __expiration," +
		" META(d).flags AS __flags, d.* FROM " + k.path() + " d"
	args := []any{limit, offset}
	if search != "" {
		statement += " WHERE CONTAINS(LOWER(META(d).id), $3)" +
			" OR ANY v IN OBJECT_VALUES(d) SATISFIES CONTAINS(LOWER(TO_STRING(v)), $3) END"
		args = append(args, strings.ToLower(search))
	}
	statement += " ORDER BY META(d).id LIMIT $1 OFFSET $2"

	raw, err := s.queryRows(rc.Ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(raw))
	for _, item := range raw {
		id := stringValue(item["__id"])
		out := row{
			"ref":        documentRef(k, id),
			"id":         id,
			"key":        id,
			"cas":        item["__cas"],
			"expiration": item["__expiration"],
			"flags":      item["__flags"],
		}
		for _, key := range sortedKeys(item) {
			if strings.HasPrefix(key, "__") {
				continue
			}
			if sqldb.RedactColumn(key, s.opts.RedactPatterns) {
				out[key] = sqldb.RedactedValue
				continue
			}
			out[key] = item[key]
		}
		rows = append(rows, out)
	}
	return rows, nil
}

func documentsList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	k, err := requireCollection(rc, s)
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
			return nil, fmt.Errorf("%w: invalid cursor", plugin.ErrInvalidInput)
		}
		offset = parsed
	}
	// One extra row tells us whether another page exists without a COUNT scan.
	rows, err := s.documentRows(rc, k, limit+1, offset, strings.TrimSpace(req.Search()))
	if err != nil {
		return nil, err
	}
	next := ""
	if len(rows) > limit {
		rows = rows[:limit]
		next = strconv.Itoa(offset + limit)
	}
	return plugin.Page[row]{Items: rows, NextCursor: next}, nil
}

// documentColumns derives the grid's columns from a bounded document sample, so
// a schemaless collection still renders as a typed, editable table.
func documentColumns(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	k, err := requireCollection(rc, s)
	if err != nil {
		return nil, err
	}
	rows, err := s.documentRows(rc, k, defaultSampleSize, 0, "")
	if err != nil {
		return nil, err
	}
	types := map[string]plugin.ColumnType{}
	for _, item := range rows {
		for key, value := range item {
			if key == "ref" || isMetaField(key) {
				continue
			}
			if existing, ok := types[key]; ok && existing != valueColumnType(value) {
				types[key] = plugin.ColumnText
				continue
			}
			types[key] = valueColumnType(value)
		}
	}

	columns := []row{
		{"name": "id", "label": "Document ID", "type": plugin.ColumnText, "editable": false, "readOnly": true},
		{"name": "cas", "label": "CAS", "type": plugin.ColumnNumber, "editable": false, "readOnly": true},
		{"name": "expiration", "label": "Expiry", "type": plugin.ColumnNumber, "editable": false, "readOnly": true},
	}
	writable := !s.opts.ReadOnly
	for _, name := range sortedColumnNames(types) {
		columnType := types[name]
		redacted := sqldb.RedactColumn(name, s.opts.RedactPatterns)
		columns = append(columns, row{
			"name":     name,
			"label":    name,
			"type":     columnType,
			"editable": writable && !redacted,
			"editor":   columnEditor(columnType),
			"readOnly": redacted,
			"nullable": true,
		})
	}
	return plugin.Page[row]{Items: columns}, nil
}

func sortedColumnNames(types map[string]plugin.ColumnType) []string {
	out := make([]string, 0, len(types))
	for name := range types {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func valueColumnType(value any) plugin.ColumnType {
	switch value.(type) {
	case bool:
		return plugin.ColumnBool
	case float64, int, int64:
		return plugin.ColumnNumber
	case map[string]any, []any:
		return plugin.ColumnJSON
	default:
		return plugin.ColumnText
	}
}

func columnEditor(columnType plugin.ColumnType) plugin.ColumnEditor {
	switch columnType {
	case plugin.ColumnBool:
		return plugin.ColumnEditorToggle
	case plugin.ColumnNumber:
		return plugin.ColumnEditorNumber
	case plugin.ColumnJSON:
		return plugin.ColumnEditorJSON
	default:
		return plugin.ColumnEditorText
	}
}

// documentBody fetches one document plus its KV metadata through the Query
// service's USE KEYS fast path, which reads straight from the data service.
func (s *Session) documentBody(rc *plugin.RequestContext, k keyspace) (map[string]any, map[string]any, error) {
	statement := "SELECT META(d).id AS __id, META(d).cas AS __cas, META(d).expiration AS __expiration," +
		" META(d).flags AS __flags, META(d).type AS __type, d AS __doc FROM " + k.path() + " d USE KEYS [$1]"
	rows, err := s.queryRows(rc.Ctx, statement, k.Document)
	if err != nil {
		return nil, nil, err
	}
	if len(rows) == 0 {
		return nil, nil, fmt.Errorf("%w: document %q not found in %s", plugin.ErrNotFound, k.Document, k.label())
	}
	item := rows[0]
	doc := asMap(item["__doc"])
	meta := map[string]any{
		"id":         stringValue(item["__id"]),
		"cas":        item["__cas"],
		"expiration": item["__expiration"],
		"flags":      item["__flags"],
		"type":       stringValue(item["__type"]),
	}
	return doc, meta, nil
}

// restoreRedacted puts the stored value back for every field the editor received
// masked, so saving a document that contains redacted fields cannot overwrite the
// real value with the mask.
func restoreRedacted(doc, current map[string]any, patterns []string) {
	for key, value := range doc {
		if value != sqldb.RedactedValue || !sqldb.RedactColumn(key, patterns) {
			continue
		}
		if stored, ok := current[key]; ok {
			doc[key] = stored
		}
	}
}

func redactDocument(doc map[string]any, patterns []string) map[string]any {
	out := make(map[string]any, len(doc))
	for key, value := range doc {
		if sqldb.RedactColumn(key, patterns) {
			out[key] = sqldb.RedactedValue
			continue
		}
		out[key] = value
	}
	return out
}

func documentRead(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	k, err := documentKeyspace(rc)
	if err != nil {
		return nil, err
	}
	doc, meta, err := s.documentBody(rc, k)
	if err != nil {
		return nil, err
	}
	visible := redactDocument(doc, s.opts.RedactPatterns)
	encoded, err := json.Marshal(visible)
	if err != nil {
		return nil, fmt.Errorf("%w: encode document: %v", plugin.ErrUnavailable, err)
	}
	collection := keyspace{Bucket: k.Bucket, Scope: k.Scope, Collection: k.Collection}
	return row{
		"ref":        documentRef(collection, k.Document),
		"key":        k.Document,
		"id":         k.Document,
		"type":       "json",
		"value":      visible,
		"keyspace":   k.label(),
		"bucket":     k.Bucket,
		"scope":      k.Scope,
		"collection": k.Collection,
		"cas":        meta["cas"],
		"expiration": meta["expiration"],
		"flags":      meta["flags"],
		"encoding":   meta["type"],
		"size":       len(encoded),
		"fields":     len(visible),
	}, nil
}

func documentContent(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	k, err := documentKeyspace(rc)
	if err != nil {
		return nil, err
	}
	doc, _, err := s.documentBody(rc, k)
	if err != nil {
		return nil, err
	}
	return row{"content": prettyJSON(redactDocument(doc, s.opts.RedactPatterns))}, nil
}

func prettyJSON(value any) string {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return "{}"
	}
	return string(encoded)
}

// documentPayload accepts every write shape the renderer produces: the code
// editor's {"content": "..."}, the KV panel's {"key","type","value"}, and a bare
// document object.
func documentPayload(rc *plugin.RequestContext) (string, map[string]any, error) {
	var envelope struct {
		Key     string          `json:"key"`
		ID      string          `json:"id"`
		Type    string          `json:"type"`
		Value   json.RawMessage `json:"value"`
		Content string          `json:"content"`
	}
	body := rc.Body()
	if len(body) == 0 {
		return "", nil, fmt.Errorf("%w: a document body is required", plugin.ErrInvalidInput)
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return "", nil, fmt.Errorf("%w: request body must be JSON", plugin.ErrInvalidInput)
	}
	key := stringDefault(envelope.Key, stringDefault(envelope.ID, strings.TrimSpace(rc.Param("id"))))

	var raw []byte
	switch {
	case len(envelope.Value) > 0:
		raw = envelope.Value
	case strings.TrimSpace(envelope.Content) != "":
		raw = []byte(envelope.Content)
	default:
		raw = body
	}
	// The KV panel serialises the edited value as a JSON string; unwrap it.
	var asString string
	if json.Unmarshal(raw, &asString) == nil && strings.HasPrefix(strings.TrimSpace(asString), "{") {
		raw = []byte(asString)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return "", nil, fmt.Errorf("%w: document must be a JSON object", plugin.ErrInvalidInput)
	}
	for _, field := range documentMetaFields {
		if field == "id" || field == "key" {
			if value, ok := doc[field]; ok && key == "" {
				key = stringValue(value)
			}
		}
		delete(doc, field)
	}
	return key, doc, nil
}

func (s *Session) upsert(rc *plugin.RequestContext, k keyspace, id string, doc map[string]any) (row, error) {
	statement := "UPSERT INTO " + k.path() + " (KEY, VALUE) VALUES ($1, $2) RETURNING META().id AS id, META().cas AS cas"
	rows, err := s.queryRows(rc.Ctx, statement, id, doc)
	if err != nil {
		return nil, err
	}
	out := row{"ok": true, "id": id, "ref": documentRef(k, id)}
	if len(rows) > 0 {
		out["cas"] = rows[0]["cas"]
	}
	return out, nil
}

func documentInsert(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	k, err := requireCollection(rc, s)
	if err != nil {
		return nil, err
	}
	id, doc, err := insertPayload(rc)
	if err != nil {
		return nil, err
	}
	if id == "" {
		return nil, fmt.Errorf("%w: a document id is required", plugin.ErrInvalidInput)
	}
	return s.upsert(rc, k, id, doc)
}

// insertPayload reads either the editable grid's RowMutation or a plain document
// body, so the same route serves the grid's add-row and the create dialog.
func insertPayload(rc *plugin.RequestContext) (string, map[string]any, error) {
	var mutation sqldb.RowMutation
	if err := json.Unmarshal(rc.Body(), &mutation); err == nil && len(mutation.Values) > 0 {
		values := make(map[string]any, len(mutation.Values))
		id := stringValue(mutation.Values["id"])
		if id == "" {
			id = stringValue(mutation.Key["id"])
		}
		for key, value := range mutation.Values {
			if isMetaField(key) {
				continue
			}
			values[key] = value
		}
		return id, values, nil
	}
	return documentPayload(rc)
}

func documentUpdate(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	k, err := requireCollection(rc, s)
	if err != nil {
		return nil, err
	}
	var mutation sqldb.RowMutation
	if err := rc.Bind(&mutation); err != nil {
		return nil, err
	}
	if err := sqldb.ValidateRowKey([]string{"id"}, mutation.Key); err != nil {
		return nil, err
	}
	id := stringValue(mutation.Key["id"])
	if id == "" {
		return nil, fmt.Errorf("%w: a document id is required", plugin.ErrInvalidInput)
	}
	doc := k
	doc.Document = id
	current, _, err := s.documentBody(rc, doc)
	if err != nil {
		return nil, err
	}
	merged := make(map[string]any, len(current)+len(mutation.Values))
	for key, value := range current {
		merged[key] = value
	}
	for key, value := range mutation.Values {
		if isMetaField(key) {
			continue
		}
		if sqldb.RedactColumn(key, s.opts.RedactPatterns) {
			return nil, fmt.Errorf("%w: field %q is redacted and cannot be edited", plugin.ErrForbidden, key)
		}
		merged[key] = value
	}
	return s.upsert(rc, k, id, merged)
}

func documentSave(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	k, err := keyspaceFrom(rc, s)
	if err != nil || k.Collection == "" {
		// A document reference carries the whole keyspace when no explicit
		// bucket/scope/collection params are present.
		k, err = documentKeyspace(rc)
		if err != nil {
			return nil, err
		}
	}
	payloadID, doc, err := documentPayload(rc)
	if err != nil {
		return nil, err
	}
	// The reference in the path names the document being saved; the payload key
	// only applies when the keyspace came from explicit params instead.
	id := k.Document
	if id == "" {
		id = payloadID
	}
	if id == "" {
		return nil, fmt.Errorf("%w: a document id is required", plugin.ErrInvalidInput)
	}
	collection := keyspace{Bucket: k.Bucket, Scope: k.Scope, Collection: k.Collection}
	saved := collection
	saved.Document = id
	if current, _, err := s.documentBody(rc, saved); err == nil {
		restoreRedacted(doc, current, s.opts.RedactPatterns)
	}
	if _, err := s.upsert(rc, collection, id, doc); err != nil {
		return nil, err
	}
	stored, _, err := s.documentBody(rc, saved)
	if err != nil {
		return nil, err
	}
	return row{
		"ok":      true,
		"id":      id,
		"ref":     documentRef(collection, id),
		"content": prettyJSON(redactDocument(stored, s.opts.RedactPatterns)),
	}, nil
}

func documentRemove(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	k, err := documentKeyspace(rc)
	if err != nil {
		return nil, err
	}
	collection := keyspace{Bucket: k.Bucket, Scope: k.Scope, Collection: k.Collection}
	statement := "DELETE FROM " + collection.path() + " USE KEYS [$1] RETURNING META().id AS id"
	rows, err := s.queryRows(rc.Ctx, statement, k.Document)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("%w: document %q not found in %s", plugin.ErrNotFound, k.Document, collection.label())
	}
	return actionResult{OK: true, Message: "document " + k.Document + " deleted"}, nil
}

// --- inferred schema --------------------------------------------------------

// collectionSchema runs INFER, Couchbase's own schema inference over a document
// sample, and flattens the flavours into one row per observed field.
func collectionSchema(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	k, err := requireCollection(rc, s)
	if err != nil {
		return nil, err
	}
	res, err := s.exec(rc.Ctx, "INFER "+k.path()+" WITH {\"sample_size\": 1000, \"num_sample_values\": 3}")
	if err != nil {
		return nil, err
	}
	fields := map[string]row{}
	for _, raw := range res.Results {
		var flavours []map[string]any
		if err := json.Unmarshal(raw, &flavours); err != nil {
			continue
		}
		for _, flavour := range flavours {
			for name, spec := range asMap(flavour["properties"]) {
				property := asMap(spec)
				item, ok := fields[name]
				if !ok {
					item = row{"field": name, "docs": 0.0, "flavours": 0}
					fields[name] = item
				}
				item["type"] = joinTypes(item["type"], property["type"])
				item["docs"] = numberValue(item["docs"]) + numberValue(property["#docs"])
				item["flavours"] = item["flavours"].(int) + 1
				item["coverage"] = round1(numberValue(property["%docs"]))
				if sqldb.RedactColumn(name, s.opts.RedactPatterns) {
					item["samples"] = sqldb.RedactedValue
				} else {
					item["samples"] = strings.Join(stringSlice(property["samples"]), ", ")
				}
			}
		}
	}
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)
	rows := make([]row, 0, len(fields))
	for _, name := range names {
		rows = append(rows, fields[name])
	}
	return plugin.Page[row]{Items: rows}, nil
}

func joinTypes(existing, incoming any) string {
	current := stringValue(existing)
	next := strings.Join(stringSlice(incoming), " | ")
	if next == "" {
		return current
	}
	if current == "" {
		return next
	}
	for _, part := range strings.Split(current, " | ") {
		if part == next {
			return current
		}
	}
	return current + " | " + next
}
