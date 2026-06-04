package sqldb

import "testing"

func fieldKey(n GraphNode, name string) string {
	for _, f := range n.Fields {
		if f.Name == name {
			return f.Key
		}
	}
	return "<missing>"
}

func TestRelationGraphBuildsTableNodesAndEdges(t *testing.T) {
	cols := []TableColumn{
		{Schema: "public", Table: "customers", Name: "id", Type: "bigint"},
		{Schema: "public", Table: "customers", Name: "name", Type: "text"},
		{Schema: "public", Table: "orders", Name: "id", Type: "bigint"},
		{Schema: "public", Table: "orders", Name: "customer_id", Type: "bigint"},
	}
	fks := []ForeignKey{
		{Constraint: "fk_order_customer", ChildSchema: "public", ChildTable: "orders", ChildColumn: "customer_id", ParentSchema: "public", ParentTable: "customers", ParentColumn: "id"},
	}
	g := RelationGraph(cols, fks)

	if len(g.Nodes) != 2 {
		t.Fatalf("expected 2 table nodes, got %d: %#v", len(g.Nodes), g.Nodes)
	}
	var orders GraphNode
	for _, n := range g.Nodes {
		if n.ID == "public.orders" {
			orders = n
		}
		if n.Label != n.ID[len("public."):] {
			t.Fatalf("single-schema label should be the bare table name, got %q (id %q)", n.Label, n.ID)
		}
	}
	if len(orders.Fields) != 2 {
		t.Fatalf("orders should list its 2 columns, got %#v", orders.Fields)
	}
	if fieldKey(orders, "customer_id") != "fk" {
		t.Fatalf("customer_id should be marked fk, got %q", fieldKey(orders, "customer_id"))
	}
	if fieldKey(orders, "id") != "" {
		t.Fatalf("non-fk column should be unmarked, got %q", fieldKey(orders, "id"))
	}
	if len(g.Edges) != 1 || g.Edges[0].SourceField != "customer_id" || g.Edges[0].TargetField != "id" {
		t.Fatalf("edge should anchor customer_id -> id, got %#v", g.Edges)
	}
}

func TestRelationGraphStubsOutOfScopeParent(t *testing.T) {
	// orders is in scope; its parent customers is not (no columns) but must still
	// get a node so the edge does not dangle.
	cols := []TableColumn{{Schema: "public", Table: "orders", Name: "customer_id", Type: "bigint"}}
	fks := []ForeignKey{{Constraint: "fk", ChildSchema: "public", ChildTable: "orders", ChildColumn: "customer_id", ParentSchema: "public", ParentTable: "customers", ParentColumn: "id"}}
	g := RelationGraph(cols, fks)
	if len(g.Nodes) != 2 {
		t.Fatalf("expected orders + stub customers node, got %d", len(g.Nodes))
	}
	if len(g.Edges) != 1 {
		t.Fatalf("expected 1 edge, got %d", len(g.Edges))
	}
}

func TestRelationGraphMergesCompositeForeignKey(t *testing.T) {
	fks := []ForeignKey{
		{Constraint: "fk", ChildSchema: "s", ChildTable: "a", ChildColumn: "x", ParentSchema: "s", ParentTable: "b", ParentColumn: "p"},
		{Constraint: "fk", ChildSchema: "s", ChildTable: "a", ChildColumn: "y", ParentSchema: "s", ParentTable: "b", ParentColumn: "q"},
	}
	g := RelationGraph(nil, fks)
	if len(g.Edges) != 1 {
		t.Fatalf("composite FK should collapse to one edge, got %d", len(g.Edges))
	}
	if g.Edges[0].Label != "x, y" {
		t.Fatalf("composite edge label = %q, want %q", g.Edges[0].Label, "x, y")
	}
}

func TestRelationGraphQualifiesLabelsAcrossSchemas(t *testing.T) {
	g := RelationGraph(nil, []ForeignKey{
		{Constraint: "fk", ChildSchema: "sales", ChildTable: "orders", ChildColumn: "uid", ParentSchema: "auth", ParentTable: "users", ParentColumn: "id"},
	})
	for _, n := range g.Nodes {
		if n.Label != n.ID {
			t.Fatalf("multi-schema label should be schema-qualified, got %q (id %q)", n.Label, n.ID)
		}
	}
}
