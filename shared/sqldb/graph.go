package sqldb

import (
	"fmt"
	"strings"
)

// GraphPayload is the {nodes, edges} document the generic graph panel renders.
// Relational schemas reuse the same panel the graph databases use; the optional
// Fields turn a node into an ERD table box, and the edge field anchors let the
// panel draw a foreign key from the exact child column to the parent column.
type GraphPayload struct {
	Nodes []GraphNode `json:"nodes"`
	Edges []GraphEdge `json:"edges"`
}

type GraphNode struct {
	ID     string       `json:"id"`
	Label  string       `json:"label"`
	Group  string       `json:"group,omitempty"`
	Fields []GraphField `json:"fields,omitempty"`
}

// GraphField is one column rendered inside an ERD table node. Key marks special
// columns ("fk" for a foreign key) so the panel can badge them.
type GraphField struct {
	Name string `json:"name"`
	Type string `json:"type,omitempty"`
	Key  string `json:"key,omitempty"`
}

type GraphEdge struct {
	ID          string `json:"id,omitempty"`
	Source      string `json:"source"`
	Target      string `json:"target"`
	Label       string `json:"label,omitempty"`
	SourceField string `json:"sourceField,omitempty"`
	TargetField string `json:"targetField,omitempty"`
}

// ForeignKey is one introspected relationship: a child table column that
// references a parent table column.
type ForeignKey struct {
	Constraint   string
	ChildSchema  string
	ChildTable   string
	ChildColumn  string
	ParentSchema string
	ParentTable  string
	ParentColumn string
}

// TableColumn is one column of a table, used to build ERD table nodes.
type TableColumn struct {
	Schema string
	Table  string
	Name   string
	Type   string
}

// ForeignKeyFromRow reads a foreign key from an introspection row whose columns
// are aliased to the standard names, so each dialect only writes its own query.
func ForeignKeyFromRow(r map[string]any) ForeignKey {
	return ForeignKey{
		Constraint:   rowString(r, "constraint_name"),
		ChildSchema:  rowString(r, "child_schema"),
		ChildTable:   rowString(r, "child_table"),
		ChildColumn:  rowString(r, "child_column"),
		ParentSchema: rowString(r, "parent_schema"),
		ParentTable:  rowString(r, "parent_table"),
		ParentColumn: rowString(r, "parent_column"),
	}
}

// TableColumnFromRow reads a column from an introspection row aliased to the
// standard names (table_schema, table_name, column_name, data_type).
func TableColumnFromRow(r map[string]any) TableColumn {
	return TableColumn{
		Schema: rowString(r, "table_schema"),
		Table:  rowString(r, "table_name"),
		Name:   rowString(r, "column_name"),
		Type:   rowString(r, "data_type"),
	}
}

// RelationGraph builds an ERD: one node per table (listing its columns), one edge
// per foreign key, anchored from the child column to the parent column. Tables
// referenced by a foreign key but absent from columns still get a (fieldless)
// node so no edge dangles. Labels are schema-qualified only when more than one
// schema is present, to keep the common single-schema diagram clean.
func RelationGraph(columns []TableColumn, fks []ForeignKey) GraphPayload {
	schemas := map[string]struct{}{}
	for _, c := range columns {
		schemas[c.Schema] = struct{}{}
	}
	for _, fk := range fks {
		schemas[fk.ChildSchema] = struct{}{}
		schemas[fk.ParentSchema] = struct{}{}
	}
	qualify := len(schemas) > 1

	index := map[string]*GraphNode{}
	order := []string{}
	ensure := func(schema, table string) *GraphNode {
		id := tableID(schema, table)
		n, ok := index[id]
		if !ok {
			label := table
			if qualify && schema != "" {
				label = id
			}
			n = &GraphNode{ID: id, Label: label, Group: schema}
			index[id] = n
			order = append(order, id)
		}
		return n
	}

	for _, c := range columns {
		n := ensure(c.Schema, c.Table)
		n.Fields = append(n.Fields, GraphField{Name: c.Name, Type: c.Type})
	}

	edges := []GraphEdge{}
	edgeAt := map[string]int{}
	fkColumns := map[string]map[string]struct{}{}
	for _, fk := range fks {
		child := ensure(fk.ChildSchema, fk.ChildTable)
		target := ensure(fk.ParentSchema, fk.ParentTable).ID
		if fkColumns[child.ID] == nil {
			fkColumns[child.ID] = map[string]struct{}{}
		}
		fkColumns[child.ID][fk.ChildColumn] = struct{}{}

		key := fmt.Sprintf("%s->%s:%s", child.ID, target, fk.Constraint)
		if i, ok := edgeAt[key]; ok {
			edges[i].Label += ", " + fk.ChildColumn
			continue
		}
		edgeAt[key] = len(edges)
		edges = append(edges, GraphEdge{
			ID: key, Source: child.ID, Target: target,
			Label: fk.ChildColumn, SourceField: fk.ChildColumn, TargetField: fk.ParentColumn,
		})
	}

	for id, cols := range fkColumns {
		for i := range index[id].Fields {
			if _, ok := cols[index[id].Fields[i].Name]; ok {
				index[id].Fields[i].Key = "fk"
			}
		}
	}

	payload := GraphPayload{Nodes: make([]GraphNode, 0, len(order)), Edges: edges}
	for _, id := range order {
		payload.Nodes = append(payload.Nodes, *index[id])
	}
	return payload
}

func tableID(schema, table string) string {
	if schema == "" {
		return table
	}
	return schema + "." + table
}

func rowString(r map[string]any, key string) string {
	v, ok := r[key]
	if !ok || v == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(v))
}
