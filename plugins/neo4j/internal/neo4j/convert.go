package neo4j

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	driver "github.com/neo4j/neo4j-go-driver/v6/neo4j"

	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

type graphPayload struct {
	Nodes []graphNode `json:"nodes"`
	Edges []graphEdge `json:"edges"`
}

type graphNode struct {
	ID         string         `json:"id"`
	Label      string         `json:"label,omitempty"`
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

func collectGraphValue(value any, graph *graphPayload, seenNodes, seenEdges map[string]bool, redact []string) {
	switch v := value.(type) {
	case driver.Node:
		addGraphNode(v, graph, seenNodes, redact)
	case driver.Relationship:
		addGraphEdge(v, graph, seenEdges)
	case driver.Path:
		for _, node := range v.Nodes {
			addGraphNode(node, graph, seenNodes, redact)
		}
		for _, rel := range v.Relationships {
			addGraphEdge(rel, graph, seenEdges)
		}
	case []any:
		for _, item := range v {
			collectGraphValue(item, graph, seenNodes, seenEdges, redact)
		}
	case map[string]any:
		for _, item := range v {
			collectGraphValue(item, graph, seenNodes, seenEdges, redact)
		}
	}
}

func addGraphNode(node driver.Node, graph *graphPayload, seen map[string]bool, redact []string) {
	id := node.ElementId
	if id == "" {
		id = "node:" + compactValue(redactMap(node.Props, redact))
	}
	if seen[id] {
		return
	}
	seen[id] = true
	props := redactMap(node.Props, redact)
	graph.Nodes = append(graph.Nodes, graphNode{
		ID:         id,
		Label:      graphNodeLabel(node),
		Group:      strings.Join(node.Labels, ", "),
		Summary:    nodeSummary(props),
		Properties: props,
	})
}

func addGraphEdge(rel driver.Relationship, graph *graphPayload, seen map[string]bool) {
	id := rel.ElementId
	if id == "" {
		id = "relationship:" + rel.StartElementId + ":" + rel.Type + ":" + rel.EndElementId
	}
	if seen[id] || rel.StartElementId == "" || rel.EndElementId == "" {
		return
	}
	seen[id] = true
	graph.Edges = append(graph.Edges, graphEdge{ID: id, Source: rel.StartElementId, Target: rel.EndElementId, Label: rel.Type})
}

func graphNodeLabel(node driver.Node) string {
	if name, ok := node.Props["name"]; ok && fmt.Sprint(name) != "" {
		return fmt.Sprint(name)
	}
	if title, ok := node.Props["title"]; ok && fmt.Sprint(title) != "" {
		return fmt.Sprint(title)
	}
	if len(node.Labels) > 0 {
		return ":" + node.Labels[0]
	}
	if node.ElementId != "" {
		return node.ElementId
	}
	return "node"
}

func nodeSummary(props map[string]any) string {
	keys := make([]string, 0, len(props))
	for key := range props {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) > 3 {
		keys = keys[:3]
	}
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+fmt.Sprint(props[key]))
	}
	return strings.Join(parts, ", ")
}

func normalizeValue(value any, redact []string) any {
	switch v := value.(type) {
	case driver.Node:
		return row{"element_id": v.ElementId, "labels": v.Labels, "properties": redactMap(v.Props, redact)}
	case driver.Relationship:
		return row{"element_id": v.ElementId, "type": v.Type, "start": v.StartElementId, "end": v.EndElementId, "properties": redactMap(v.Props, redact)}
	case driver.Path:
		return row{"nodes": len(v.Nodes), "relationships": len(v.Relationships)}
	case time.Time:
		return v.Format(time.RFC3339Nano)
	case []any:
		out := make([]any, 0, len(v))
		for _, item := range v {
			out = append(out, normalizeValue(item, redact))
		}
		return out
	case map[string]any:
		return redactMap(v, redact)
	default:
		return value
	}
}

func redactMap(in map[string]any, patterns []string) map[string]any {
	if in == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		if sqldb.RedactColumn(key, patterns) {
			out[key] = sqldb.RedactedValue
			continue
		}
		out[key] = normalizeValue(value, patterns)
	}
	return out
}

func asMap(value any) map[string]any {
	if m, ok := value.(map[string]any); ok {
		return m
	}
	return map[string]any{}
}

func compactValue(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprint(value)
	}
	return string(data)
}

func stringSlice(value any) []string {
	switch v := value.(type) {
	case []string:
		return v
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			out = append(out, fmt.Sprint(item))
		}
		return out
	default:
		if value == nil {
			return nil
		}
		return []string{fmt.Sprint(value)}
	}
}

func nodeName(item row) string {
	props := asMap(item["properties"])
	for _, key := range []string{"name", "title", "id"} {
		if value, ok := props[key]; ok && fmt.Sprint(value) != "" {
			return fmt.Sprint(value)
		}
	}
	if labels := fmt.Sprint(item["labels"]); labels != "" {
		return labels + " " + fmt.Sprint(item["element_id"])
	}
	return fmt.Sprint(item["element_id"])
}

func mustEncodeID(kind, database, id string) string {
	raw, err := json.Marshal([]string{kind, database, id})
	if err != nil {
		return kind + ":" + database + ":" + id
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeID3(encoded string) (string, string, string, error) {
	data, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return "", "", "", fmt.Errorf("%w: invalid resource id", plugin.ErrInvalidInput)
	}
	var parts []string
	if err := json.Unmarshal(data, &parts); err != nil || len(parts) != 3 {
		return "", "", "", fmt.Errorf("%w: invalid resource id", plugin.ErrInvalidInput)
	}
	return parts[0], parts[1], parts[2], nil
}

func cursorOffset(cursor string) int {
	if cursor == "" {
		return 0
	}
	n, err := strconv.Atoi(cursor)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

func sortRows(rows []row, key string) {
	sort.Slice(rows, func(i, j int) bool {
		return strings.ToLower(fmt.Sprint(rows[i][key])) < strings.ToLower(fmt.Sprint(rows[j][key]))
	})
}

func schemaIcon(kind string) plugin.Icon {
	if kind == "constraint" {
		return icon("shield-check")
	}
	return icon("list-tree")
}
