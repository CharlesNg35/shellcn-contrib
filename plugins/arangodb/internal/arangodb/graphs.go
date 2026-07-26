package arangodb

import (
	"context"
	"fmt"
	"sort"
	"strings"

	driver "github.com/arangodb/go-driver/v2/arangodb"
	"github.com/arangodb/go-driver/v2/arangodb/shared"

	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

// graphPayload is the document-level {nodes, edges} contract PanelGraph renders.
// It is richer than the collection-level ERD payload: each node carries the
// document properties shown in the panel's detail drawer.
type graphPayload struct {
	Nodes []graphNode `json:"nodes"`
	Edges []graphEdge `json:"edges"`
}

type graphNode struct {
	ID         string         `json:"id"`
	Label      string         `json:"label"`
	Group      string         `json:"group,omitempty"`
	Summary    string         `json:"summary,omitempty"`
	Properties map[string]any `json:"properties,omitempty"`
}

type graphEdge struct {
	ID     string `json:"id,omitempty"`
	Source string `json:"source"`
	Target string `json:"target"`
	Label  string `json:"label,omitempty"`
}

type graphBuilder struct {
	payload graphPayload
	nodes   map[string]bool
	edges   map[string]bool
}

func newGraphBuilder() *graphBuilder {
	return &graphBuilder{
		payload: graphPayload{Nodes: []graphNode{}, Edges: []graphEdge{}},
		nodes:   map[string]bool{},
		edges:   map[string]bool{},
	}
}

func (b *graphBuilder) addVertex(doc map[string]any) string {
	id, _ := doc["_id"].(string)
	if id == "" || b.nodes[id] {
		return id
	}
	b.nodes[id] = true
	collection, _, _ := strings.Cut(id, "/")
	properties := make(map[string]any, len(doc))
	for key, value := range doc {
		if key == "_rev" {
			continue
		}
		properties[key] = value
	}
	b.payload.Nodes = append(b.payload.Nodes, graphNode{
		ID:         id,
		Label:      vertexLabel(doc, id),
		Group:      collection,
		Summary:    vertexSummary(properties),
		Properties: properties,
	})
	return id
}

func (b *graphBuilder) addEdge(doc map[string]any) {
	id, _ := doc["_id"].(string)
	from, _ := doc["_from"].(string)
	to, _ := doc["_to"].(string)
	if id == "" || from == "" || to == "" || b.edges[id] {
		return
	}
	b.edges[id] = true
	collection, _, _ := strings.Cut(id, "/")
	label := collection
	if name, ok := doc["label"].(string); ok && name != "" {
		label = name
	}
	b.payload.Edges = append(b.payload.Edges, graphEdge{ID: id, Source: from, Target: to, Label: label})
}

// prune drops edges whose endpoints were not materialised, so the panel never
// receives a dangling reference.
func (b *graphBuilder) prune() graphPayload {
	edges := b.payload.Edges[:0]
	for _, edge := range b.payload.Edges {
		if b.nodes[edge.Source] && b.nodes[edge.Target] {
			edges = append(edges, edge)
		}
	}
	b.payload.Edges = edges
	return b.payload
}

func vertexLabel(doc map[string]any, id string) string {
	for _, key := range []string{"name", "title", "label", "_key"} {
		if value, ok := doc[key].(string); ok && strings.TrimSpace(value) != "" {
			return value
		}
	}
	return id
}

func vertexSummary(properties map[string]any) string {
	keys := make([]string, 0, len(properties))
	for key := range properties {
		if strings.HasPrefix(key, "_") {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) > 3 {
		keys = keys[:3]
	}
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+compact(properties[key]))
	}
	return strings.Join(parts, ", ")
}

// graphDefinition reads a named graph's edge definitions and orphan collections.
func (s *Session) graphDefinition(ctx context.Context, database, name string) (driver.Graph, error) {
	db, err := s.database(ctx, database)
	if err != nil {
		return nil, err
	}
	if _, err := safeName(name, "graph"); err != nil {
		return nil, err
	}
	tctx, cancel := context.WithTimeout(ctx, s.opts.Timeout)
	defer cancel()
	graph, err := db.Graph(tctx, name, &driver.GetGraphOptions{})
	if err != nil {
		return nil, arangoErr(err)
	}
	return graph, nil
}

func graphsList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	database := databaseParam(rc, s)
	db, err := s.database(rc.Ctx, database)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	reader, err := db.Graphs(ctx)
	if err != nil {
		return nil, arangoErr(err)
	}
	rows := make([]row, 0, 8)
	for {
		graph, err := reader.Read()
		if err != nil {
			if shared.IsNoMoreDocuments(err) {
				break
			}
			return nil, arangoErr(err)
		}
		rows = append(rows, graphRow(graph, database))
	}
	return pageRows(rc, rows)
}

func graphRow(graph driver.Graph, database string) row {
	edges := graph.EdgeDefinitions()
	names := make([]string, 0, len(edges))
	for _, edge := range edges {
		names = append(names, edge.Collection)
	}
	kind := "general"
	switch {
	case graph.IsSatellite():
		kind = "satellite"
	case graph.IsDisjoint():
		kind = "disjoint smart"
	case graph.IsSmart():
		kind = "smart"
	}
	return row{
		"name":               graph.Name(),
		"database":           database,
		"kind":               kind,
		"edge_definitions":   len(edges),
		"edge_collections":   joinStrings(names),
		"orphan_collections": joinStrings(graph.OrphanCollections()),
		"number_of_shards":   intOrZero(graph.NumberOfShards()),
		"replication_factor": graph.ReplicationFactor(),
		"write_concern":      intOrZero(graph.WriteConcern()),
		"ref":                plugin.ResourceIdentity{Kind: kindGraph, Namespace: database, Name: graph.Name(), UID: encodeID(kindGraph, database, graph.Name())},
	}
}

func graphRead(rc *plugin.RequestContext) (any, error) {
	s, database, name, err := graphScope(rc)
	if err != nil {
		return nil, err
	}
	graph, err := s.graphDefinition(rc.Ctx, database, name)
	if err != nil {
		return nil, err
	}
	out := graphRow(graph, database)
	vertices, edges := int64(0), int64(0)
	for _, collection := range vertexCollectionNames(graph) {
		vertices += s.queryInt(rc.Ctx, database, "RETURN LENGTH(@@col)", map[string]any{"@col": collection})
	}
	for _, definition := range graph.EdgeDefinitions() {
		edges += s.queryInt(rc.Ctx, database, "RETURN LENGTH(@@col)", map[string]any{"@col": definition.Collection})
	}
	out["vertex_collections"] = joinStrings(vertexCollectionNames(graph))
	out["vertex_count"] = vertices
	out["edge_count"] = edges
	out["smart_graph_attribute"] = graph.SmartGraphAttribute()
	return out, nil
}

func graphEdgeDefinitions(rc *plugin.RequestContext) (any, error) {
	s, database, name, err := graphScope(rc)
	if err != nil {
		return nil, err
	}
	graph, err := s.graphDefinition(rc.Ctx, database, name)
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(graph.EdgeDefinitions()))
	for _, definition := range graph.EdgeDefinitions() {
		rows = append(rows, row{
			"collection": definition.Collection,
			"from":       joinStrings(definition.From),
			"to":         joinStrings(definition.To),
			"edges":      s.queryInt(rc.Ctx, database, "RETURN LENGTH(@@col)", map[string]any{"@col": definition.Collection}),
		})
	}
	return pageRows(rc, rows)
}

func graphVertexCollections(rc *plugin.RequestContext) (any, error) {
	s, database, name, err := graphScope(rc)
	if err != nil {
		return nil, err
	}
	graph, err := s.graphDefinition(rc.Ctx, database, name)
	if err != nil {
		return nil, err
	}
	orphans := map[string]bool{}
	for _, orphan := range graph.OrphanCollections() {
		orphans[orphan] = true
	}
	rows := make([]row, 0, 8)
	for _, collection := range vertexCollectionNames(graph) {
		rows = append(rows, row{
			"name":      collection,
			"documents": s.queryInt(rc.Ctx, database, "RETURN LENGTH(@@col)", map[string]any{"@col": collection}),
			"role":      map[bool]string{true: "orphan", false: "connected"}[orphans[collection]],
		})
	}
	return pageRows(rc, rows)
}

func vertexCollectionNames(graph driver.Graph) []string {
	seen := map[string]bool{}
	names := make([]string, 0, 8)
	add := func(list []string) {
		for _, name := range list {
			if !seen[name] {
				seen[name] = true
				names = append(names, name)
			}
		}
	}
	for _, definition := range graph.EdgeDefinitions() {
		add(definition.From)
		add(definition.To)
	}
	add(graph.OrphanCollections())
	sort.Strings(names)
	return names
}

// graphExplore seeds the interactive explorer with the densest slice of the
// named graph: real edges plus both endpoint documents.
func graphExplore(rc *plugin.RequestContext) (any, error) {
	s, database, name, err := graphScope(rc)
	if err != nil {
		return nil, err
	}
	graph, err := s.graphDefinition(rc.Ctx, database, name)
	if err != nil {
		return nil, err
	}
	builder := newGraphBuilder()
	budget := s.opts.PageLimit
	definitions := graph.EdgeDefinitions()
	per := budget
	if len(definitions) > 1 {
		per = budget/len(definitions) + 1
	}
	for _, definition := range definitions {
		rows, err := s.queryRows(rc.Ctx, database, `
FOR edge IN @@col
  LET from = DOCUMENT(edge._from)
  LET to = DOCUMENT(edge._to)
  FILTER from != null AND to != null
  LIMIT @limit
  RETURN {edge: edge, from: from, to: to}`,
			map[string]any{"@col": definition.Collection, "limit": per}, per)
		if err != nil {
			return nil, err
		}
		for _, item := range rows {
			builder.addVertex(asMap(item["from"]))
			builder.addVertex(asMap(item["to"]))
			builder.addEdge(asMap(item["edge"]))
		}
	}
	if len(builder.payload.Nodes) == 0 {
		// A graph with no edges yet still shows its vertices.
		for _, collection := range vertexCollectionNames(graph) {
			rows, err := s.queryRows(rc.Ctx, database, "FOR doc IN @@col LIMIT @limit RETURN doc",
				map[string]any{"@col": collection, "limit": budget}, budget)
			if err != nil {
				return nil, err
			}
			for _, doc := range rows {
				builder.addVertex(doc)
			}
		}
	}
	return builder.prune(), nil
}

// graphExpand loads one vertex's neighbourhood for click-to-expand. The clicked
// node id is the document `_id` carried by the explorer payload.
func graphExpand(rc *plugin.RequestContext) (any, error) {
	s, database, name, err := graphScope(rc)
	if err != nil {
		return nil, err
	}
	start := strings.TrimSpace(paramOrQuery(rc, "node"))
	if err := validateDocumentID(start); err != nil {
		return nil, err
	}
	if _, err := safeName(name, "graph"); err != nil {
		return nil, err
	}
	rows, err := s.queryRows(rc.Ctx, database, `
FOR vertex, edge IN 1..@depth ANY @start GRAPH @graph
  LIMIT @limit
  RETURN {vertex: vertex, edge: edge, from: DOCUMENT(edge._from), to: DOCUMENT(edge._to)}`,
		map[string]any{"start": start, "graph": name, "depth": s.opts.Depth, "limit": s.opts.PageLimit},
		s.opts.PageLimit)
	if err != nil {
		return nil, err
	}
	builder := newGraphBuilder()
	if origin, err := s.queryOne(rc.Ctx, database, "RETURN DOCUMENT(@start)", map[string]any{"start": start}); err == nil && origin != nil {
		builder.addVertex(asMap(origin))
	}
	for _, item := range rows {
		builder.addVertex(asMap(item["from"]))
		builder.addVertex(asMap(item["to"]))
		builder.addVertex(asMap(item["vertex"]))
		builder.addEdge(asMap(item["edge"]))
	}
	return builder.prune(), nil
}

// schemaGraph is the database-level map: one ERD node per collection listing its
// sampled attributes, with an edge per observed edge-collection relationship.
func schemaGraph(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	database := databaseParam(rc, s)
	db, err := s.database(rc.Ctx, database)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	collections, err := db.Collections(ctx)
	if err != nil {
		return nil, arangoErr(err)
	}
	payload := sqldb.GraphPayload{Nodes: []sqldb.GraphNode{}, Edges: []sqldb.GraphEdge{}}
	known := map[string]bool{}
	edgeCollections := make([]string, 0, 4)
	for _, collection := range collections {
		name := collection.Name()
		if strings.HasPrefix(name, "_") {
			continue
		}
		props, err := collection.Properties(ctx)
		if err != nil {
			continue
		}
		group := "document"
		if props.Type == driver.CollectionTypeEdge {
			group = "edge"
			edgeCollections = append(edgeCollections, name)
		}
		known[name] = true
		payload.Nodes = append(payload.Nodes, sqldb.GraphNode{
			ID: name, Label: name, Group: group, Fields: s.sampleFields(rc.Ctx, database, name),
		})
	}
	for _, name := range edgeCollections {
		links, err := s.queryRows(rc.Ctx, database, `
FOR edge IN @@col
  LIMIT @limit
  COLLECT from = PARSE_IDENTIFIER(edge._from).collection, to = PARSE_IDENTIFIER(edge._to).collection WITH COUNT INTO edges
  RETURN {from: from, to: to, edges: edges}`,
			map[string]any{"@col": name, "limit": columnSampleSize}, 64)
		if err != nil {
			continue
		}
		for _, link := range links {
			from, _ := link["from"].(string)
			to, _ := link["to"].(string)
			if !known[from] || !known[to] {
				continue
			}
			payload.Edges = append(payload.Edges,
				sqldb.GraphEdge{ID: name + ":" + from + "->" + to, Source: from, Target: name, Label: "_from"},
				sqldb.GraphEdge{ID: name + ":" + to + "<-" + from, Source: name, Target: to, Label: "_to"},
			)
		}
	}
	payload.Edges = dedupeEdges(payload.Edges)
	return payload, nil
}

func dedupeEdges(edges []sqldb.GraphEdge) []sqldb.GraphEdge {
	seen := map[string]bool{}
	out := edges[:0]
	for _, edge := range edges {
		key := edge.Source + "->" + edge.Target + ":" + edge.Label
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, edge)
	}
	return out
}

func (s *Session) sampleFields(ctx context.Context, database, collection string) []sqldb.GraphField {
	rows, err := s.queryRows(ctx, database, "FOR doc IN @@col LIMIT @limit RETURN doc",
		map[string]any{"@col": collection, "limit": 25}, 25)
	if err != nil {
		return nil
	}
	kinds := map[string]string{}
	for _, doc := range rows {
		for name, value := range doc {
			if name == "_rev" || name == "_id" {
				continue
			}
			if existing, ok := kinds[name]; !ok || existing == "null" {
				kinds[name] = jsonKind(value)
			}
		}
	}
	fields := make([]sqldb.GraphField, 0, len(kinds))
	for _, name := range sortedKeys(kinds) {
		field := sqldb.GraphField{Name: name, Type: kinds[name]}
		if name == "_key" {
			field.Key = "pk"
		}
		if name == "_from" || name == "_to" {
			field.Key = "fk"
		}
		fields = append(fields, field)
	}
	return fields
}

func validateDocumentID(id string) error {
	collection, key, ok := strings.Cut(id, "/")
	if !ok {
		return fmt.Errorf("%w: node must be a document id such as people/alice", plugin.ErrInvalidInput)
	}
	if _, err := safeName(collection, "collection"); err != nil {
		return err
	}
	_, err := safeKey(key)
	return err
}

func graphCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Named graph", Fields: []plugin.Field{
		{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true,
			Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: namePattern, Message: "most characters are allowed; not / : % ? # + or control characters"}}},
		{Key: "edge_collection", Label: "Edge collection", Type: plugin.FieldText, Required: true, Placeholder: "knows",
			Help: "Created if it does not exist yet."},
		{Key: "from", Label: "From vertex collections", Type: plugin.FieldText, Required: true, Placeholder: "people"},
		{Key: "to", Label: "To vertex collections", Type: plugin.FieldText, Required: true, Placeholder: "people, companies"},
		{Key: "orphans", Label: "Orphan vertex collections", Type: plugin.FieldText, Placeholder: "tags"},
	}}}}
}

func graphCreate(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Name           string `json:"name" validate:"required"`
		EdgeCollection string `json:"edge_collection" validate:"required"`
		From           string `json:"from" validate:"required"`
		To             string `json:"to" validate:"required"`
		Orphans        string `json:"orphans"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name, err := safeName(req.Name, "graph")
	if err != nil {
		return nil, err
	}
	edgeCollection, err := safeName(req.EdgeCollection, "collection")
	if err != nil {
		return nil, err
	}
	from, err := collectionNameList(req.From)
	if err != nil {
		return nil, err
	}
	to, err := collectionNameList(req.To)
	if err != nil {
		return nil, err
	}
	orphans := []string{}
	if strings.TrimSpace(req.Orphans) != "" {
		if orphans, err = collectionNameList(req.Orphans); err != nil {
			return nil, err
		}
	}
	database := databaseParam(rc, s)
	db, err := s.database(rc.Ctx, database)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	definition := &driver.GraphDefinition{
		Name:              name,
		EdgeDefinitions:   []driver.EdgeDefinition{{Collection: edgeCollection, From: from, To: to}},
		OrphanCollections: orphans,
	}
	if _, err := db.CreateGraph(ctx, name, definition, &driver.CreateGraphOptions{}); err != nil {
		return nil, arangoErr(err)
	}
	return row{"ok": true, "name": name, "database": database}, nil
}

func graphDrop(rc *plugin.RequestContext) (any, error) {
	s, database, name, err := graphScope(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	graph, err := s.graphDefinition(rc.Ctx, database, name)
	if err != nil {
		return nil, err
	}
	var dropCollections bool
	if strings.EqualFold(paramOrQuery(rc, "drop_collections"), "true") {
		dropCollections = true
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	if err := graph.Remove(ctx, &driver.RemoveGraphOptions{DropCollections: dropCollections}); err != nil {
		return nil, arangoErr(err)
	}
	return actionResult{OK: true}, nil
}

func collectionNameList(raw string) ([]string, error) {
	parts := strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || r == ' ' || r == '\n' || r == '\t' })
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		name, err := safeName(part, "collection")
		if err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: at least one collection is required", plugin.ErrInvalidInput)
	}
	return out, nil
}
