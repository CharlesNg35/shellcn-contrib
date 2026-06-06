package surrealdb

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/charlesng35/shellcn/sdk/plugin"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

// listRecords returns a page of a table's records for the editable data grid. The
// grid edits typed cells for schemafull tables (one column per field) and a
// "record" JSON cell for schemaless ones, so each row carries both the flat
// fields and the whole normalized record.
func listRecords(rc *plugin.RequestContext) (any, error) {
	db, table, err := tableClient(rc)
	if err != nil {
		// An empty/absent table param yields an empty page rather than an error,
		// so the grid renders cleanly before a table is chosen.
		if rc.Param("table") == "" {
			return plugin.Page[map[string]any]{Items: []map[string]any{}}, nil
		}
		return nil, err
	}
	page, err := rc.Page()
	if err != nil {
		return nil, err
	}
	maxLimit := sess(rc).opts.rowLimit
	if maxLimit <= 0 {
		maxLimit = defaultRowLimit
	}
	limit := page.Limit
	if limit <= 0 {
		limit = maxLimit
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	start := 0
	if page.Cursor != "" {
		start, _ = parseCursor(page.Cursor)
	}

	sql := "SELECT * FROM type::table($tb)"
	vars := map[string]any{"tb": table, "limit": limit, "start": start}
	if term := page.Search(); term != "" {
		// Generic free-text search across the stringified record id (see README:
		// schemaless records have no universal text column to scan).
		sql += " WHERE string::contains(string::lowercase(<string>id), $q)"
		vars["q"] = strings.ToLower(term)
	}
	sql += " ORDER BY id LIMIT $limit START $start"

	rows, err := queryOne[[]map[string]any](rc.Ctx, db, sql, vars)
	if err != nil {
		return nil, err
	}

	items := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		norm, _ := normalize(r).(map[string]any)
		id, _ := norm["id"].(string)
		row := map[string]any{}
		for k, v := range norm {
			row[k] = v
		}
		row["id"] = id
		row["record"] = norm
		row["ref"] = plugin.ResourceRef{Kind: "record", Scope: table, Name: id, UID: id}
		items = append(items, row)
	}
	out := plugin.Page[map[string]any]{Items: items}
	if len(items) == limit {
		out.NextCursor = makeCursor(start + limit)
	}
	return out, nil
}

// getRecord returns one record by id as a pretty JSON document for the record
// detail view.
func getRecord(rc *plugin.RequestContext) (any, error) {
	tb, key, ok := splitRecordID(rc.Param("id"))
	if !ok {
		return nil, fmt.Errorf("%w: record id must be table:key", plugin.ErrInvalidInput)
	}
	db, err := sess(rc).client(rc.Ctx)
	if err != nil {
		return nil, err
	}
	rows, err := queryOne[[]map[string]any](rc.Ctx, db,
		"SELECT * FROM type::thing($tb, $id)",
		map[string]any{"tb": tb, "id": key})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("%w: record %s", plugin.ErrNotFound, rc.Param("id"))
	}
	pretty, _ := json.MarshalIndent(normalize(rows[0]), "", "  ")
	return map[string]any{"content": "```json\n" + string(pretty) + "\n```", "format": "markdown"}, nil
}

// createRecord inserts one record from the "New record" form: optional explicit
// id, plus a JSON content object.
func createRecord(rc *plugin.RequestContext) (any, error) {
	var in struct {
		ID   string         `json:"id"`
		Data map[string]any `json:"data" validate:"required"`
	}
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	db, table, err := tableClient(rc)
	if err != nil {
		return nil, err
	}
	target := "type::table($tb)"
	vars := map[string]any{"tb": table, "data": in.Data}
	if id := strings.TrimSpace(in.ID); id != "" {
		target = "type::thing($tb, $id)"
		vars["id"] = id
	}
	created, err := queryOne[[]map[string]any](rc.Ctx, db,
		fmt.Sprintf("CREATE %s CONTENT $data", target), vars)
	if err != nil {
		return nil, err
	}
	return normalize(anySlice(created)), nil
}

// insertRecord is the data grid's Insert slot: the edited row arrives as a flat
// JSON object. The id (if any) is stripped from the content and used as the
// record key.
func insertRecord(rc *plugin.RequestContext) (any, error) {
	db, table, err := tableClient(rc)
	if err != nil {
		return nil, err
	}
	row, err := bindRow(rc)
	if err != nil {
		return nil, err
	}
	content := gridContent(row)
	target := "type::table($tb)"
	vars := map[string]any{"tb": table, "data": content}
	if id, ok := rowKey(row); ok {
		_, key, _ := splitRecordID(id)
		if key == "" {
			key = id
		}
		target = "type::thing($tb, $id)"
		vars["id"] = key
	}
	created, err := queryOne[[]map[string]any](rc.Ctx, db,
		fmt.Sprintf("CREATE %s CONTENT $data", target), vars)
	if err != nil {
		return nil, err
	}
	return normalize(anySlice(created)), nil
}

// updateRecord is the data grid's Update slot: the full edited row (with its id)
// replaces the record's content.
func updateRecord(rc *plugin.RequestContext) (any, error) {
	db, err := sess(rc).client(rc.Ctx)
	if err != nil {
		return nil, err
	}
	row, err := bindRow(rc)
	if err != nil {
		return nil, err
	}
	id, ok := rowKey(row)
	if !ok {
		return nil, fmt.Errorf("%w: row is missing its id", plugin.ErrInvalidInput)
	}
	tb, key, ok := splitRecordID(id)
	if !ok {
		return nil, fmt.Errorf("%w: record id must be table:key", plugin.ErrInvalidInput)
	}
	return replaceRecord(rc, db, tb, key, gridContent(row))
}

// editRecord is the detail-form Edit action: replace one record's content.
func editRecord(rc *plugin.RequestContext) (any, error) {
	tb, key, ok := splitRecordID(rc.Param("id"))
	if !ok {
		return nil, fmt.Errorf("%w: record id must be table:key", plugin.ErrInvalidInput)
	}
	var in struct {
		Data map[string]any `json:"data" validate:"required"`
	}
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	db, err := sess(rc).client(rc.Ctx)
	if err != nil {
		return nil, err
	}
	return replaceRecord(rc, db, tb, key, in.Data)
}

func replaceRecord(rc *plugin.RequestContext, db *surrealdb.DB, tb, key string, data map[string]any) (any, error) {
	updated, err := queryOne[[]map[string]any](rc.Ctx, db,
		"UPDATE type::thing($tb, $id) CONTENT $data",
		map[string]any{"tb": tb, "id": key, "data": data})
	if err != nil {
		return nil, err
	}
	return normalize(anySlice(updated)), nil
}

// deleteRecord is the data grid's Delete slot: the row (with its id) arrives as
// JSON.
func deleteRecord(rc *plugin.RequestContext) (any, error) {
	db, err := sess(rc).client(rc.Ctx)
	if err != nil {
		return nil, err
	}
	row, err := bindRow(rc)
	if err != nil {
		return nil, err
	}
	id, ok := rowKey(row)
	if !ok {
		return nil, fmt.Errorf("%w: row is missing its id", plugin.ErrInvalidInput)
	}
	return removeByID(rc, db, id)
}

// removeRecord is the detail-action Delete: remove one record by its {id} param.
func removeRecord(rc *plugin.RequestContext) (any, error) {
	db, err := sess(rc).client(rc.Ctx)
	if err != nil {
		return nil, err
	}
	return removeByID(rc, db, rc.Param("id"))
}

func removeByID(rc *plugin.RequestContext, db *surrealdb.DB, id string) (any, error) {
	tb, key, ok := splitRecordID(id)
	if !ok {
		return nil, fmt.Errorf("%w: record id must be table:key", plugin.ErrInvalidInput)
	}
	if _, err := queryOne[any](rc.Ctx, db,
		"DELETE type::thing($tb, $id)",
		map[string]any{"tb": tb, "id": key}); err != nil {
		return nil, err
	}
	return map[string]any{"ok": true}, nil
}

// bindRow decodes a grid mutation body. The grid posts the edited row either at
// the top level or under a "row" key; accept both.
func bindRow(rc *plugin.RequestContext) (map[string]any, error) {
	var raw map[string]any
	if err := json.Unmarshal(rc.Body(), &raw); err != nil {
		return nil, fmt.Errorf("%w: invalid row body: %v", plugin.ErrInvalidInput, err)
	}
	if inner, ok := raw["row"].(map[string]any); ok {
		return inner, nil
	}
	return raw, nil
}

func rowKey(row map[string]any) (string, bool) {
	if id, ok := row["id"].(string); ok && id != "" {
		return id, true
	}
	return "", false
}

// gridContent is the record content to persist: the edited row minus the helper
// fields the list route attaches (id is the key, not content; record/ref are UI
// helpers). A schemaless row whose only editable cell is "record" unwraps it.
func gridContent(row map[string]any) map[string]any {
	if rec, ok := row["record"].(map[string]any); ok && len(row) <= 3 {
		out := map[string]any{}
		for k, v := range rec {
			if k != "id" {
				out[k] = v
			}
		}
		return out
	}
	out := map[string]any{}
	for k, v := range row {
		switch k {
		case "id", "record", "ref":
		default:
			out[k] = v
		}
	}
	return out
}
