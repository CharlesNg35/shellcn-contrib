package surrealdb

import (
	"fmt"
	"sort"
	"strings"

	"github.com/charlesng35/shellcn/sdk/plugin"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

// dbInfo returns the parsed INFO FOR DB map (each section is name -> DEFINE
// statement). It is the single source of truth for tables and DB objects.
func dbInfo(rc *plugin.RequestContext, db *surrealdb.DB) (map[string]any, error) {
	info, err := queryOne[map[string]any](rc.Ctx, db, "INFO FOR DB", nil)
	if err != nil {
		return nil, err
	}
	return info, nil
}

// section extracts one name->definition map from an INFO result.
func section(info map[string]any, key string) map[string]any {
	if m, ok := info[key].(map[string]any); ok {
		return m
	}
	return map[string]any{}
}

type tableRow struct {
	Name    string             `json:"name"`
	Mode    string             `json:"mode"`
	Records int64              `json:"records"`
	Ref     plugin.ResourceRef `json:"ref"`
}

// listTables backs the Tables list, the database resource list, and the tree.
// SurrealDB exposes the schema via INFO FOR DB, whose "tables" field maps table
// name -> DEFINE statement; the mode (schemafull/schemaless) and a record count
// come from that definition and a per-table count() query.
func listTables(rc *plugin.RequestContext) (any, error) {
	db, err := sess(rc).client(rc.Ctx)
	if err != nil {
		return nil, err
	}
	page, err := rc.Page()
	if err != nil {
		return nil, err
	}
	info, err := dbInfo(rc, db)
	if err != nil {
		return nil, err
	}

	names := make([]string, 0)
	defs := section(info, "tables")
	term := strings.ToLower(page.Search())
	for name := range defs {
		if term != "" && !strings.Contains(strings.ToLower(name), term) {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)

	rows := make([]tableRow, 0, len(names))
	for _, name := range names {
		mode := "schemaless"
		if def, _ := defs[name].(string); strings.Contains(strings.ToUpper(def), "SCHEMAFULL") {
			mode = "schemafull"
		}
		var count int64
		if c, err := queryOne[[]map[string]any](rc.Ctx, db,
			fmt.Sprintf("SELECT count() FROM %s GROUP ALL", name), nil); err == nil && len(c) > 0 {
			count = toInt64(c[0]["count"])
		}
		rows = append(rows, tableRow{
			Name: name, Mode: mode, Records: count,
			Ref: plugin.ResourceRef{Kind: "table", Name: name, UID: name},
		})
	}
	return plugin.Page[tableRow]{Items: rows}, nil
}

// treeTables returns the Tables sidebar nodes; clicking one opens the table's
// detail (data grid + schema + console).
func treeTables(rc *plugin.RequestContext) (any, error) {
	out, err := listTables(rc)
	if err != nil {
		return nil, err
	}
	page := out.(plugin.Page[tableRow])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, t := range page.Items {
		nodes = append(nodes, plugin.TreeNode{
			Key: "table:" + t.Name, Label: t.Name, Icon: icon("table"),
			Ref:  &plugin.ResourceRef{Kind: "table", Name: t.Name, UID: t.Name},
			Leaf: true,
		})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

// defineTable creates a table in the chosen schema mode.
func defineTable(rc *plugin.RequestContext) (any, error) {
	var in struct {
		Name   string `json:"name" validate:"required"`
		Schema string `json:"schema"`
	}
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	name, err := requireIdent(in.Name)
	if err != nil {
		return nil, err
	}
	mode := "SCHEMALESS"
	if strings.EqualFold(in.Schema, "schemafull") {
		mode = "SCHEMAFULL"
	}
	db, err := sess(rc).client(rc.Ctx)
	if err != nil {
		return nil, err
	}
	if _, err := queryOne[any](rc.Ctx, db, fmt.Sprintf("DEFINE TABLE %s %s", name, mode), nil); err != nil {
		return nil, err
	}
	return map[string]any{"name": name}, nil
}

func removeTable(rc *plugin.RequestContext) (any, error) {
	table, err := requireIdent(rc.Param("table"))
	if err != nil {
		return nil, err
	}
	db, err := sess(rc).client(rc.Ctx)
	if err != nil {
		return nil, err
	}
	if _, err := queryOne[any](rc.Ctx, db, fmt.Sprintf("REMOVE TABLE %s", table), nil); err != nil {
		return nil, err
	}
	return map[string]any{"ok": true}, nil
}

// tableDefinition returns the table's DEFINE statement plus its fields/indexes as
// a markdown document for the Definition tab.
func tableDefinition(rc *plugin.RequestContext) (any, error) {
	db, table, err := tableClient(rc)
	if err != nil {
		return nil, err
	}
	dbDef, _ := dbInfo(rc, db)
	tblDef, _ := section(dbDef, "tables")[table].(string)
	info, err := tableInfo(rc, db, table)
	if err != nil {
		return nil, err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "## %s\n\n```surql\n%s\n```\n", table, tblDef)
	writeDefSection(&b, "Fields", section(info, "fields"))
	writeDefSection(&b, "Indexes", section(info, "indexes"))
	writeDefSection(&b, "Events", section(info, "events"))
	return map[string]any{"content": b.String(), "format": "markdown"}, nil
}

func writeDefSection(b *strings.Builder, label string, defs map[string]any) {
	if len(defs) == 0 {
		return
	}
	names := sortedKeys(defs)
	fmt.Fprintf(b, "\n### %s\n\n```surql\n", label)
	for _, n := range names {
		if def, ok := defs[n].(string); ok {
			fmt.Fprintf(b, "%s\n", def)
		}
	}
	b.WriteString("```\n")
}

// tableInfo returns the parsed INFO FOR TABLE map.
func tableInfo(rc *plugin.RequestContext, db *surrealdb.DB, table string) (map[string]any, error) {
	return queryOne[map[string]any](rc.Ctx, db, fmt.Sprintf("INFO FOR TABLE %s", table), nil)
}

type defRow struct {
	Name       string             `json:"name"`
	Definition string             `json:"definition"`
	Ref        plugin.ResourceRef `json:"ref"`
}

type fieldRow struct {
	Name       string             `json:"name"`
	Type       string             `json:"type"`
	Definition string             `json:"definition"`
	Ref        plugin.ResourceRef `json:"ref"`
}

// tableFields lists a table's defined fields (schemafull) with their parsed type.
func tableFields(rc *plugin.RequestContext) (any, error) {
	db, table, err := tableClient(rc)
	if err != nil {
		return nil, err
	}
	info, err := tableInfo(rc, db, table)
	if err != nil {
		return nil, err
	}
	defs := section(info, "fields")
	rows := make([]fieldRow, 0, len(defs))
	for _, name := range sortedKeys(defs) {
		def, _ := defs[name].(string)
		rows = append(rows, fieldRow{
			Name: name, Type: parseFieldType(def), Definition: def,
			Ref: plugin.ResourceRef{Kind: "field", Scope: table, Name: name, UID: table + "." + name},
		})
	}
	return plugin.Page[fieldRow]{Items: rows}, nil
}

type columnRow struct {
	Name     string `json:"name"`
	Label    string `json:"label"`
	Type     string `json:"type,omitempty"`
	ReadOnly bool   `json:"readOnly,omitempty"`
}

// tableColumnsRoute powers the editable data grid's ColumnsSource: the id column
// plus every defined field, so a schemafull table edits with typed cells. A
// schemaless table falls back to id + record (free JSON), inferred client-side.
func tableColumnsRoute(rc *plugin.RequestContext) (any, error) {
	db, table, err := tableClient(rc)
	if err != nil {
		return nil, err
	}
	info, err := tableInfo(rc, db, table)
	if err != nil {
		return nil, err
	}
	cols := []columnRow{{Name: "id", Label: "ID", ReadOnly: true}}
	defs := section(info, "fields")
	for _, name := range sortedKeys(defs) {
		def, _ := defs[name].(string)
		cols = append(cols, columnRow{Name: name, Label: name, Type: parseFieldType(def)})
	}
	if len(defs) == 0 {
		cols = append(cols, columnRow{Name: "record", Label: "Record", Type: "json"})
	}
	return plugin.Page[columnRow]{Items: cols}, nil
}

func tableIndexes(rc *plugin.RequestContext) (any, error) {
	return tableDefSection(rc, "indexes", "index")
}

func tableEvents(rc *plugin.RequestContext) (any, error) {
	return tableDefSection(rc, "events", "event")
}

// tableDefSection lists one INFO FOR TABLE section (indexes/events) as name +
// definition rows.
func tableDefSection(rc *plugin.RequestContext, sectionKey, refKind string) (any, error) {
	db, table, err := tableClient(rc)
	if err != nil {
		return nil, err
	}
	info, err := tableInfo(rc, db, table)
	if err != nil {
		return nil, err
	}
	defs := section(info, sectionKey)
	rows := make([]defRow, 0, len(defs))
	for _, name := range sortedKeys(defs) {
		def, _ := defs[name].(string)
		rows = append(rows, defRow{
			Name: name, Definition: def,
			Ref: plugin.ResourceRef{Kind: refKind, Scope: table, Name: name, UID: table + "." + name},
		})
	}
	return plugin.Page[defRow]{Items: rows}, nil
}

// defineField defines a field on a table. Name is identifier-validated; the type
// is restricted to a SurrealQL type expression (letters, digits, and the few
// punctuation chars a type uses).
func defineField(rc *plugin.RequestContext) (any, error) {
	var in struct {
		Name string `json:"name" validate:"required"`
		Type string `json:"type"`
	}
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	db, table, err := tableClient(rc)
	if err != nil {
		return nil, err
	}
	name, err := requireIdent(in.Name)
	if err != nil {
		return nil, err
	}
	typ := strings.TrimSpace(in.Type)
	if typ == "" {
		typ = "any"
	}
	if !validType(typ) {
		return nil, fmt.Errorf("%w: invalid field type", plugin.ErrInvalidInput)
	}
	if _, err := queryOne[any](rc.Ctx, db,
		fmt.Sprintf("DEFINE FIELD %s ON %s TYPE %s", name, table, typ), nil); err != nil {
		return nil, err
	}
	return map[string]any{"name": name}, nil
}

func removeField(rc *plugin.RequestContext) (any, error) {
	db, table, err := tableClient(rc)
	if err != nil {
		return nil, err
	}
	name, err := requireIdent(rc.Param("name"))
	if err != nil {
		return nil, err
	}
	if _, err := queryOne[any](rc.Ctx, db,
		fmt.Sprintf("REMOVE FIELD %s ON %s", name, table), nil); err != nil {
		return nil, err
	}
	return map[string]any{"ok": true}, nil
}

// defineIndex creates an index over one or more (validated) fields, optionally
// unique.
func defineIndex(rc *plugin.RequestContext) (any, error) {
	var in struct {
		Name   string `json:"name" validate:"required"`
		Fields string `json:"fields" validate:"required"`
		Unique bool   `json:"unique"`
	}
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	db, table, err := tableClient(rc)
	if err != nil {
		return nil, err
	}
	name, err := requireIdent(in.Name)
	if err != nil {
		return nil, err
	}
	parts := strings.Split(in.Fields, ",")
	fields := make([]string, 0, len(parts))
	for _, f := range parts {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		if !validIdent(f) {
			return nil, fmt.Errorf("%w: invalid index field %q", plugin.ErrInvalidInput, f)
		}
		fields = append(fields, f)
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("%w: at least one field is required", plugin.ErrInvalidInput)
	}
	stmt := fmt.Sprintf("DEFINE INDEX %s ON %s FIELDS %s", name, table, strings.Join(fields, ", "))
	if in.Unique {
		stmt += " UNIQUE"
	}
	if _, err := queryOne[any](rc.Ctx, db, stmt, nil); err != nil {
		return nil, err
	}
	return map[string]any{"name": name}, nil
}

func removeIndex(rc *plugin.RequestContext) (any, error) {
	db, table, err := tableClient(rc)
	if err != nil {
		return nil, err
	}
	name, err := requireIdent(rc.Param("name"))
	if err != nil {
		return nil, err
	}
	if _, err := queryOne[any](rc.Ctx, db,
		fmt.Sprintf("REMOVE INDEX %s ON %s", name, table), nil); err != nil {
		return nil, err
	}
	return map[string]any{"ok": true}, nil
}

// dbOverview renders INFO FOR DB as a markdown document for the Overview tab and
// the header "Database info" dialog.
func dbOverview(rc *plugin.RequestContext) (any, error) {
	db, err := sess(rc).client(rc.Ctx)
	if err != nil {
		return nil, err
	}
	info, err := dbInfo(rc, db)
	if err != nil {
		return nil, err
	}
	var b strings.Builder
	b.WriteString("## Database overview\n")
	for _, sec := range []struct{ key, label string }{
		{"tables", "Tables"}, {"functions", "Functions"}, {"params", "Params"},
		{"analyzers", "Analyzers"}, {"accesses", "Accesses"}, {"users", "Users"},
		{"models", "Models"},
	} {
		defs := section(info, sec.key)
		fmt.Fprintf(&b, "\n### %s (%d)\n", sec.label, len(defs))
		if len(defs) == 0 {
			b.WriteString("\n_None._\n")
			continue
		}
		b.WriteString("\n```surql\n")
		for _, n := range sortedKeys(defs) {
			if def, ok := defs[n].(string); ok {
				fmt.Fprintf(&b, "%s\n", def)
			}
		}
		b.WriteString("```\n")
	}
	return map[string]any{"content": b.String(), "format": "markdown"}, nil
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func toInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case float64:
		return int64(x)
	case uint64:
		return int64(x)
	default:
		return 0
	}
}

// parseFieldType pulls the TYPE clause out of a DEFINE FIELD statement for the
// fields grid; falls back to "any" when none is present.
func parseFieldType(def string) string {
	up := strings.ToUpper(def)
	i := strings.Index(up, " TYPE ")
	if i < 0 {
		return "any"
	}
	rest := strings.TrimSpace(def[i+6:])
	for _, kw := range []string{" PERMISSIONS", " DEFAULT", " VALUE", " ASSERT", " READONLY"} {
		if j := strings.Index(strings.ToUpper(rest), kw); j >= 0 {
			rest = rest[:j]
		}
	}
	return strings.TrimSpace(rest)
}

// validType allows a SurrealQL type expression: identifiers plus the bracket,
// angle, pipe, and comma characters used in composite types (array<int>,
// option<string>, "a"|"b" is not allowed — only structural type syntax).
func validType(s string) bool {
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '_' || r == '<' || r == '>' || r == '|' || r == ',' || r == ' ' || r == '.':
		default:
			return false
		}
	}
	return s != ""
}
