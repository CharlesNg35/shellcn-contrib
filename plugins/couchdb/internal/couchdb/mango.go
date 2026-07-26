package couchdb

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

type mangoRequest struct {
	Query   string `json:"query"`
	Confirm bool   `json:"confirm,omitempty"`
	Explain bool   `json:"explain,omitempty"`
}

type mangoResult struct {
	Columns   []string `json:"columns"`
	Rows      [][]any  `json:"rows"`
	RowCount  int      `json:"rowCount"`
	ElapsedMS int64    `json:"elapsedMs"`
	Statement string   `json:"statement,omitempty"`
	Bookmark  string   `json:"bookmark,omitempty"`
	Index     string   `json:"index,omitempty"`
	IndexType string   `json:"indexType,omitempty"`
	Warning   string   `json:"warning,omitempty"`
	Truncated bool     `json:"truncated,omitempty"`
}

// mangoStream backs the Mango query editor. Every execution is bounded by the
// connection's page limit, audited individually, and annotated with the index
// CouchDB actually chose so a full scan is visible instead of silent.
func mangoStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, db, err := dbSession(rc)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(client)
	enc := json.NewEncoder(client)
	for {
		var req mangoRequest
		if err := dec.Decode(&req); err != nil {
			if errors.Is(err, io.EOF) || client.Context().Err() != nil || rc.Ctx.Err() != nil {
				return nil
			}
			if encErr := enc.Encode(row{"error": "the query frame must be a JSON object: " + err.Error()}); encErr != nil {
				return encErr
			}
			// encoding/json cannot resynchronise after a syntax error, so the
			// frame stream is closed instead of retrying the same bytes forever.
			var syntax *json.SyntaxError
			if errors.As(err, &syntax) {
				return nil
			}
			continue
		}
		body, err := parseMangoQuery(req.Query, s.opts.pageLimit)
		if err != nil {
			rc.Audit(plugin.AuditDenied, mangoAuditParams(db, req.Query, 0, 0), err)
			if encErr := enc.Encode(row{"error": err.Error()}); encErr != nil {
				return encErr
			}
			continue
		}
		if req.Explain {
			explained, err := s.explain(rc, db, body)
			if err != nil {
				if encErr := enc.Encode(row{"error": err.Error()}); encErr != nil {
					return encErr
				}
				continue
			}
			if err := enc.Encode(row{"message": "explain", "explain": explained}); err != nil {
				return err
			}
			continue
		}

		started := time.Now()
		result, err := s.find(rc, db, body)
		elapsed := time.Since(started).Milliseconds()
		if err != nil {
			rc.Audit(plugin.AuditError, mangoAuditParams(db, req.Query, 0, elapsed), err)
			if encErr := enc.Encode(row{"error": err.Error()}); encErr != nil {
				return encErr
			}
			continue
		}
		result.ElapsedMS = elapsed
		if name, kind, ok := s.chosenIndex(rc, db, body); ok {
			result.Index, result.IndexType = name, kind
		}
		rc.Audit(plugin.AuditAllowed, mangoAuditParams(db, req.Query, result.RowCount, elapsed), nil)
		if err := enc.Encode(result); err != nil {
			return err
		}
	}
}

// parseMangoQuery validates the editor text and clamps the requested limit to
// the connection's page limit so one query cannot pull an entire database.
func parseMangoQuery(text string, pageLimit int) (row, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil, fmt.Errorf("%w: the query is empty", plugin.ErrInvalidInput)
	}
	var body row
	if err := json.Unmarshal([]byte(text), &body); err != nil {
		return nil, fmt.Errorf("%w: the query must be a JSON object: %v", plugin.ErrInvalidInput, err)
	}
	if body["selector"] == nil {
		return nil, fmt.Errorf("%w: a Mango query requires a \"selector\" (use {} to match every document)", plugin.ErrInvalidInput)
	}
	limit := int(numberOf(body["limit"]))
	if limit <= 0 || limit > pageLimit {
		body["limit"] = pageLimit
	}
	return body, nil
}

func (s *Session) find(rc *plugin.RequestContext, db string, body row) (mangoResult, error) {
	var out struct {
		Docs     []row  `json:"docs"`
		Bookmark string `json:"bookmark"`
		Warning  string `json:"warning"`
	}
	if err := s.client.do(rc.Ctx, http.MethodPost, dbPath(db)+"/_find", nil, body, &out); err != nil {
		return mangoResult{}, err
	}
	columns := mangoColumns(out.Docs)
	rows := make([][]any, 0, len(out.Docs))
	for _, doc := range out.Docs {
		cells := make([]any, len(columns))
		for i, column := range columns {
			cells[i] = doc[column]
		}
		rows = append(rows, cells)
	}
	return mangoResult{
		Columns:   columns,
		Rows:      rows,
		RowCount:  len(rows),
		Statement: "_find",
		Bookmark:  out.Bookmark,
		Warning:   out.Warning,
		Truncated: len(rows) == int(numberOf(body["limit"])),
	}, nil
}

// mangoColumns orders result columns deterministically: identity first, then the
// remaining union of document keys sorted.
func mangoColumns(docs []row) []string {
	seen := map[string]bool{}
	for _, doc := range docs {
		for key := range doc {
			seen[key] = true
		}
	}
	rest := make([]string, 0, len(seen))
	for key := range seen {
		if key != "_id" && key != "_rev" {
			rest = append(rest, key)
		}
	}
	sort.Strings(rest)
	columns := make([]string, 0, len(seen))
	for _, key := range []string{"_id", "_rev"} {
		if seen[key] {
			columns = append(columns, key)
		}
	}
	return append(columns, rest...)
}

func (s *Session) explain(rc *plugin.RequestContext, db string, body row) (row, error) {
	var out row
	if err := s.client.do(rc.Ctx, http.MethodPost, dbPath(db)+"/_explain", nil, body, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// chosenIndex reports which index CouchDB planned for a selector. A failure here
// is informational only, so it never fails the query it annotates.
func (s *Session) chosenIndex(rc *plugin.RequestContext, db string, body row) (string, string, bool) {
	explained, err := s.explain(rc, db, body)
	if err != nil {
		return "", "", false
	}
	index := mapOf(explained["index"])
	if index == nil {
		return "", "", false
	}
	name := stringOf(index["name"])
	if ddoc := stringOf(index["ddoc"]); ddoc != "" {
		name = strings.TrimPrefix(ddoc, "_design/") + "/" + name
	}
	return name, stringOf(index["type"]), name != ""
}

func explainMango(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	content, err := contentBody(rc)
	if err != nil {
		return nil, err
	}
	body, err := parseMangoQuery(jsonText(content), s.opts.pageLimit)
	if err != nil {
		return nil, err
	}
	return s.explain(rc, db, body)
}

func mangoAuditParams(db, query string, rows int, elapsed int64) map[string]string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(query)))
	params := map[string]string{
		"database":     db,
		"query_sha256": hex.EncodeToString(sum[:]),
		"query_bytes":  strconv.Itoa(len(query)),
	}
	if rows > 0 {
		params["row_count"] = strconv.Itoa(rows)
	}
	if elapsed > 0 {
		params["elapsed_ms"] = strconv.FormatInt(elapsed, 10)
	}
	return params
}

// completeMango feeds the editor's completion popup with the operators, index
// names and document fields that are valid for this database.
func completeMango(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	items := make([]row, 0, 64)
	for _, operator := range []string{
		"$and", "$or", "$not", "$nor", "$all", "$elemMatch", "$allMatch", "$keyMapMatch",
		"$lt", "$lte", "$eq", "$ne", "$gte", "$gt", "$exists", "$type", "$in", "$nin",
		"$size", "$mod", "$regex", "$beginsWith",
	} {
		items = append(items, row{"label": operator, "type": "operator", "detail": "Mango operator"})
	}
	for _, keyword := range []string{"selector", "fields", "sort", "limit", "skip", "use_index", "bookmark", "conflicts", "execution_stats", "r", "stable", "update"} {
		items = append(items, row{"label": keyword, "type": "keyword", "detail": "_find parameter"})
	}
	if indexes, err := s.indexRows(rc, db); err == nil {
		for _, index := range indexes {
			items = append(items, row{
				"label":  stringOf(index["name"]),
				"type":   "index",
				"detail": stringOf(index["type"]) + " index on " + jsonText(index["fields"]),
			})
		}
	}
	if res, err := s.allDocs(rc, db, s.boundedLimit(0), 0, "", true); err == nil {
		fields := map[string]bool{}
		for _, entry := range res.Rows {
			for key := range entry.Doc {
				if !couchReserved[key] {
					fields[key] = true
				}
			}
		}
		for _, field := range sortedKeys(fields) {
			items = append(items, row{"label": field, "type": "field", "detail": "document field"})
		}
	}
	return plugin.Page[row]{Items: items}, nil
}

// --- Mango indexes ------------------------------------------------------------

func (s *Session) indexRows(rc *plugin.RequestContext, db string) ([]row, error) {
	var out struct {
		Indexes []struct {
			DDoc string `json:"ddoc"`
			Name string `json:"name"`
			Type string `json:"type"`
			Def  row    `json:"def"`
		} `json:"indexes"`
	}
	if err := s.client.do(rc.Ctx, http.MethodGet, dbPath(db)+"/_index", nil, nil, &out); err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(out.Indexes))
	for _, index := range out.Indexes {
		rows = append(rows, row{
			"name":        index.Name,
			"type":        index.Type,
			"ddoc":        strings.TrimPrefix(index.DDoc, "_design/"),
			"fields":      indexFields(index.Def),
			"partitioned": index.Def["partitioned"] == true,
			"selector":    index.Def["partial_filter_selector"],
		})
	}
	return rows, nil
}

// indexFields flattens CouchDB's [{"field":"asc"}] index definition into an
// ordered list the grid can render.
func indexFields(def row) []string {
	entries := sliceOf(def["fields"])
	fields := make([]string, 0, len(entries))
	for _, entry := range entries {
		switch value := entry.(type) {
		case string:
			fields = append(fields, value)
		case map[string]any:
			for _, key := range sortedKeys(value) {
				fields = append(fields, key+" "+stringOf(value[key]))
			}
		}
	}
	return fields
}

func listIndexes(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := s.indexRows(rc, db)
	if err != nil {
		return nil, err
	}
	return broker.PageRows(rc, rows)
}

func createIndex(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	var in struct {
		Name            string   `json:"name"`
		DDoc            string   `json:"ddoc"`
		Fields          []string `json:"fields" validate:"required,min=1,dive,required"`
		Type            string   `json:"type"`
		PartialSelector any      `json:"partial_selector"`
	}
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	index := row{"fields": toAnySlice(in.Fields)}
	if selector := normalizeSelector(in.PartialSelector); selector != nil {
		index["partial_filter_selector"] = selector
	}
	body := row{"index": index}
	if name := strings.TrimSpace(in.Name); name != "" {
		body["name"] = name
	}
	if ddoc := strings.TrimSpace(in.DDoc); ddoc != "" {
		body["ddoc"] = strings.TrimPrefix(ddoc, "_design/")
	}
	switch in.Type {
	case "", "json":
		body["type"] = "json"
	case "text":
		body["type"] = "text"
	default:
		return nil, fmt.Errorf("%w: index type must be json or text", plugin.ErrInvalidInput)
	}

	var out row
	if err := s.client.do(rc.Ctx, http.MethodPost, dbPath(db)+"/_index", nil, body, &out); err != nil {
		return nil, err
	}
	return row{"ok": true, "result": stringOf(out["result"]), "id": stringOf(out["id"]), "name": stringOf(out["name"])}, nil
}

// normalizeSelector accepts either a decoded object or the raw JSON text a
// textarea-backed JSON field submits.
func normalizeSelector(value any) map[string]any {
	switch selector := value.(type) {
	case map[string]any:
		if len(selector) == 0 {
			return nil
		}
		return selector
	case string:
		text := strings.TrimSpace(selector)
		if text == "" {
			return nil
		}
		var decoded map[string]any
		if json.Unmarshal([]byte(text), &decoded) != nil || len(decoded) == 0 {
			return nil
		}
		return decoded
	default:
		return nil
	}
}

func toAnySlice(values []string) []any {
	out := make([]any, 0, len(values))
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// deleteIndex resolves the index's type from CouchDB, because the DELETE path
// embeds it and the grid only carries the design document and name.
func deleteIndex(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	ddoc, err := ddocParam(rc)
	if err != nil {
		return nil, err
	}
	name := strings.TrimSpace(rc.Param("name"))
	if name == "" {
		return nil, fmt.Errorf("%w: an index name is required", plugin.ErrInvalidInput)
	}
	indexes, err := s.indexRows(rc, db)
	if err != nil {
		return nil, err
	}
	kind := ""
	for _, index := range indexes {
		if stringOf(index["ddoc"]) == ddoc && stringOf(index["name"]) == name {
			kind = stringOf(index["type"])
			break
		}
	}
	if kind == "" {
		return nil, fmt.Errorf("%w: index %s/%s", plugin.ErrNotFound, ddoc, name)
	}
	path := dbPath(db) + "/_index/_design/" + url.PathEscape(ddoc) + "/" + url.PathEscape(kind) + "/" + url.PathEscape(name)
	var out row
	if err := s.client.do(rc.Ctx, http.MethodDelete, path, nil, nil, &out); err != nil {
		return nil, err
	}
	return row{"ok": true, "ddoc": ddoc, "name": name}, nil
}
