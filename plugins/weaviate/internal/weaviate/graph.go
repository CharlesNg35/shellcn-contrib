package weaviate

import (
	"fmt"
	"sort"
	"strings"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/weaviate/weaviate/entities/models"
)

type graphField struct {
	Name string `json:"name"`
	Type string `json:"type,omitempty"`
	Key  string `json:"key,omitempty"`
}

type graphNode struct {
	ID         string         `json:"id"`
	Label      string         `json:"label"`
	Group      string         `json:"group,omitempty"`
	Summary    string         `json:"summary,omitempty"`
	Fields     []graphField   `json:"fields,omitempty"`
	Properties map[string]any `json:"properties,omitempty"`
}

type graphEdge struct {
	Source string `json:"source"`
	Target string `json:"target"`
	Label  string `json:"label,omitempty"`
}

type graphResult struct {
	Nodes []graphNode `json:"nodes"`
	Edges []graphEdge `json:"edges"`
}

type graphBuilder struct {
	nodes     []graphNode
	edges     []graphEdge
	nodeIndex map[string]int
	edgeSeen  map[string]bool
}

func newGraphBuilder() *graphBuilder {
	return &graphBuilder{nodeIndex: map[string]int{}, edgeSeen: map[string]bool{}}
}

func (g *graphBuilder) addNode(node graphNode) {
	if node.ID == "" {
		return
	}
	if index, ok := g.nodeIndex[node.ID]; ok {
		if len(g.nodes[index].Fields) == 0 && len(node.Fields) > 0 {
			g.nodes[index] = node
		}
		return
	}
	g.nodeIndex[node.ID] = len(g.nodes)
	g.nodes = append(g.nodes, node)
}

func (g *graphBuilder) addEdge(edge graphEdge) {
	if edge.Source == "" || edge.Target == "" {
		return
	}
	key := edge.Source + "\x00" + edge.Target + "\x00" + edge.Label
	if g.edgeSeen[key] {
		return
	}
	g.edgeSeen[key] = true
	g.edges = append(g.edges, edge)
}

func (g *graphBuilder) count() int { return len(g.nodes) }

func (g *graphBuilder) result() graphResult {
	nodes := g.nodes
	if nodes == nil {
		nodes = []graphNode{}
	}
	edges := g.edges
	if edges == nil {
		edges = []graphEdge{}
	}
	return graphResult{Nodes: nodes, Edges: edges}
}

func classNodeID(name string) string { return "class:" + name }

func schemaGraph(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	dump, err := s.client.schema.Getter().Do(rc.Ctx)
	if err != nil {
		return nil, mapError(err)
	}
	stats, _ := clusterStats(rc)
	graph := newGraphBuilder()
	classes := sortedClasses(dump.Classes)
	known := map[string]bool{}
	for _, class := range classes {
		known[class.Class] = true
	}
	for _, class := range classes {
		graph.addNode(classGraphNode(class, stats))
	}
	for _, class := range classes {
		for _, property := range class.Properties {
			if property == nil || len(property.DataType) == 0 || !isReferenceType(property.DataType[0]) {
				continue
			}
			for _, target := range property.DataType {
				if !known[target] {
					graph.addNode(graphNode{ID: classNodeID(target), Label: target, Group: "missing", Summary: "target collection is not in the schema"})
				}
				graph.addEdge(graphEdge{Source: classNodeID(class.Class), Target: classNodeID(target), Label: property.Name})
			}
		}
	}
	return graph.result(), nil
}

func expandSchemaGraph(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	node := strings.TrimSpace(rc.Param("node"))
	name := strings.TrimPrefix(node, "class:")
	if name == node || name == "" {
		return nil, fmt.Errorf("%w: node must look like class:<Collection>", plugin.ErrInvalidInput)
	}
	if _, err := validName(name, classNamePattern, "collection"); err != nil {
		return nil, err
	}
	dump, err := s.client.schema.Getter().Do(rc.Ctx)
	if err != nil {
		return nil, mapError(err)
	}
	stats, _ := clusterStats(rc)
	byName := map[string]*models.Class{}
	for _, class := range sortedClasses(dump.Classes) {
		byName[class.Class] = class
	}
	center, ok := byName[name]
	if !ok {
		return nil, fmt.Errorf("%w: collection %q", plugin.ErrNotFound, name)
	}

	graph := newGraphBuilder()
	graph.addNode(classGraphNode(center, stats))
	for _, property := range center.Properties {
		if property == nil || len(property.DataType) == 0 || !isReferenceType(property.DataType[0]) {
			continue
		}
		for _, target := range property.DataType {
			if class, ok := byName[target]; ok {
				graph.addNode(classGraphNode(class, stats))
			} else {
				graph.addNode(graphNode{ID: classNodeID(target), Label: target, Group: "missing"})
			}
			graph.addEdge(graphEdge{Source: classNodeID(name), Target: classNodeID(target), Label: property.Name})
		}
	}
	for _, class := range sortedClasses(dump.Classes) {
		for _, property := range class.Properties {
			if property == nil || len(property.DataType) == 0 {
				continue
			}
			for _, target := range property.DataType {
				if target != name {
					continue
				}
				graph.addNode(classGraphNode(class, stats))
				graph.addEdge(graphEdge{Source: classNodeID(class.Class), Target: classNodeID(name), Label: property.Name})
			}
		}
	}
	return graph.result(), nil
}

func classGraphNode(class *models.Class, stats *clusterSnapshot) graphNode {
	fields := make([]graphField, 0, len(class.Properties))
	properties := sortedProperties(class)
	for _, property := range properties {
		kind := propertyKind(property)
		fields = append(fields, graphField{
			Name: property.Name,
			Type: strings.Join(property.DataType, "|"),
			Key:  map[bool]string{true: "ref", false: ""}[kind == "reference"],
		})
	}
	node := graphNode{
		ID:      classNodeID(class.Class),
		Label:   class.Class,
		Group:   primaryVectorizer(class),
		Summary: graphSummary(class),
		Fields:  fields,
		Properties: map[string]any{
			"vectorizer":      primaryVectorizer(class),
			"vectorIndexType": class.VectorIndexType,
			"properties":      len(class.Properties),
			"references":      countReferences(class),
			"multiTenancy":    multiTenancyEnabled(class),
			"replication":     replicationFactor(class),
		},
	}
	if stats != nil {
		if perClass, ok := stats.Classes[class.Class]; ok {
			node.Properties["objects"] = perClass.Objects
			node.Properties["shards"] = perClass.Shards
		}
	}
	return node
}

func sortedProperties(class *models.Class) []*models.Property {
	out := make([]*models.Property, 0, len(class.Properties))
	for _, property := range class.Properties {
		if property != nil {
			out = append(out, property)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func graphSummary(class *models.Class) string {
	parts := []string{fmt.Sprintf("%d properties", len(class.Properties))}
	if refs := countReferences(class); refs > 0 {
		parts = append(parts, fmt.Sprintf("%d references", refs))
	}
	if multiTenancyEnabled(class) {
		parts = append(parts, "multi-tenant")
	}
	if description := strings.TrimSpace(class.Description); description != "" {
		parts = append(parts, truncate(description, 60))
	}
	return strings.Join(parts, " · ")
}
