package couchdb

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

var (
	databaseNamePattern = regexp.MustCompile(`^[a-z_][a-z0-9_$()+/-]*$`)
	designNamePattern   = regexp.MustCompile(`^[^/\x00-\x1f]+$`)
	// nodeNamePattern matches Erlang node names ("couchdb@127.0.0.1") and the
	// "_local" alias while excluding the dot segments that would let a crafted
	// parameter escape the /_node/{node} prefix.
	nodeNamePattern = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.@+-]*$`)
)

func dbParam(rc *plugin.RequestContext) (string, error) {
	name := strings.TrimSpace(rc.Param("db"))
	if !databaseNamePattern.MatchString(name) {
		return "", fmt.Errorf("%w: %q is not a valid CouchDB database name", plugin.ErrInvalidInput, rc.Param("db"))
	}
	return name, nil
}

func docParam(rc *plugin.RequestContext) (string, error) {
	id := strings.TrimSpace(rc.Param("docid"))
	if id == "" {
		return "", fmt.Errorf("%w: a document id is required", plugin.ErrInvalidInput)
	}
	if strings.ContainsAny(id, "\x00\n\r") {
		return "", fmt.Errorf("%w: the document id contains control characters", plugin.ErrInvalidInput)
	}
	return id, nil
}

// ddocParam returns the design document name without its "_design/" prefix; the
// UI addresses design documents by bare name.
func ddocParam(rc *plugin.RequestContext) (string, error) {
	name := strings.TrimSpace(rc.Param("ddoc"))
	name = strings.TrimPrefix(name, "_design/")
	if name == "" || !designNamePattern.MatchString(name) {
		return "", fmt.Errorf("%w: %q is not a valid design document name", plugin.ErrInvalidInput, rc.Param("ddoc"))
	}
	return name, nil
}

func nodeParam(rc *plugin.RequestContext) (string, error) {
	name := strings.TrimSpace(rc.Param("node"))
	if !nodeNamePattern.MatchString(name) {
		return "", fmt.Errorf("%w: %q is not a valid node name", plugin.ErrInvalidInput, rc.Param("node"))
	}
	return name, nil
}

func viewParam(rc *plugin.RequestContext) (string, error) {
	name := strings.TrimSpace(rc.Param("view"))
	if name == "" || !designNamePattern.MatchString(name) {
		return "", fmt.Errorf("%w: %q is not a valid view name", plugin.ErrInvalidInput, rc.Param("view"))
	}
	return name, nil
}

// dbSession resolves the session and the validated database in one step, since
// nearly every database-scoped handler needs both.
func dbSession(rc *plugin.RequestContext) (*Session, string, error) {
	s, err := session(rc)
	if err != nil {
		return nil, "", err
	}
	db, err := dbParam(rc)
	if err != nil {
		return nil, "", err
	}
	return s, db, nil
}

// boundedLimit clamps a page/limit request to the connection's configured cap.
func (s *Session) boundedLimit(requested int) int {
	if requested <= 0 || requested > s.opts.pageLimit {
		return s.opts.pageLimit
	}
	return requested
}

// contentBody decodes the body shared by code-editor saves and JSON actions. The
// editor sends {"content": "<text>"}; forms may send the object directly.
func contentBody(rc *plugin.RequestContext) (map[string]any, error) {
	var raw any
	if err := json.Unmarshal(rc.Body(), &raw); err != nil {
		return nil, fmt.Errorf("%w: the request body must be JSON", plugin.ErrInvalidInput)
	}
	envelope, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: expected a JSON object", plugin.ErrInvalidInput)
	}
	if content, ok := envelope["content"]; ok {
		switch value := content.(type) {
		case string:
			var decoded map[string]any
			if err := json.Unmarshal([]byte(value), &decoded); err != nil {
				return nil, fmt.Errorf("%w: the editor content is not valid JSON: %v", plugin.ErrInvalidInput, err)
			}
			return decoded, nil
		case map[string]any:
			return value, nil
		default:
			return nil, fmt.Errorf("%w: content must be a JSON object or a JSON string", plugin.ErrInvalidInput)
		}
	}
	return envelope, nil
}

// editorContent renders a document the same way the code editor displays it, so
// a save response can reset the editor baseline without a visible diff.
func editorContent(doc any) (string, error) {
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return "", fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	return string(data), nil
}

func numberOf(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case json.Number:
		f, _ := n.Float64()
		return f
	case bool:
		if n {
			return 1
		}
	}
	return 0
}

func stringOf(v any) string {
	switch s := v.(type) {
	case nil:
		return ""
	case string:
		return s
	default:
		return fmt.Sprint(v)
	}
}

func mapOf(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return nil
}

func sliceOf(v any) []any {
	if s, ok := v.([]any); ok {
		return s
	}
	return nil
}

// nested walks a decoded JSON tree, e.g. nested(stats, "couchdb", "httpd", "requests").
func nested(root map[string]any, path ...string) any {
	var current any = root
	for _, key := range path {
		m := mapOf(current)
		if m == nil {
			return nil
		}
		current = m[key]
	}
	return current
}

// statValue reads a CouchDB statistic leaf, which is {"value": n} for counters
// and {"value": {...}} for histograms.
func statValue(root map[string]any, path ...string) float64 {
	leaf := mapOf(nested(root, path...))
	if leaf == nil {
		return 0
	}
	switch value := leaf["value"].(type) {
	case map[string]any:
		return numberOf(value["arithmetic_mean"])
	default:
		return numberOf(value)
	}
}

func jsonText(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(data)
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// revGeneration returns the leading integer of a CouchDB revision ("3-abc" -> 3).
func revGeneration(rev string) int {
	dash := strings.IndexByte(rev, '-')
	if dash <= 0 {
		return 0
	}
	generation := 0
	for _, r := range rev[:dash] {
		if r < '0' || r > '9' {
			return 0
		}
		generation = generation*10 + int(r-'0')
	}
	return generation
}

func timelineRow(when time.Time, title, message string, severity plugin.Severity, ref plugin.ResourceIdentity) row {
	return row{
		"time":     when.UTC().Format(time.RFC3339),
		"title":    title,
		"message":  message,
		"severity": string(severity),
		"resource": ref,
	}
}

// unixTime converts CouchDB's numeric or RFC3339 timestamps into a Go time.
func unixTime(v any) (time.Time, bool) {
	switch value := v.(type) {
	case string:
		for _, layout := range []string{time.RFC3339, time.RFC1123, "2006-01-02T15:04:05Z"} {
			if parsed, err := time.Parse(layout, value); err == nil {
				return parsed, true
			}
		}
	case float64:
		if value > 0 {
			return time.Unix(int64(value), 0), true
		}
	}
	return time.Time{}, false
}
