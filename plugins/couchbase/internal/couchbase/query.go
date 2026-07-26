package couchbase

import (
	"context"
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

	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

// n1qlReadKeywords are the SQL++ statements that cannot change data or metadata.
var n1qlReadKeywords = map[string]bool{
	"SELECT": true, "EXPLAIN": true, "ADVISE": true, "INFER": true, "WITH": true, "VALUES": true,
}

func isReadOnlyN1QL(statement string) bool {
	keyword := sqldb.FirstKeyword(statement)
	if n1qlReadKeywords[keyword] {
		return true
	}
	// EXECUTE/PREPARE run whatever they were given, so they are never read-only.
	return false
}

func streamDecodeError(client plugin.ClientStream, err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) || client.Context().Err() != nil {
		return nil
	}
	return err
}

// contextID registers a cancellable context for one execution so the Cancel
// button can abort both the client wait and the server-side request.
func (s *Session) contextID(ctx context.Context) (context.Context, string, func()) {
	s.mu.Lock()
	s.seq++
	id := "shellcn-" + strconv.FormatUint(s.seq, 10)
	s.mu.Unlock()

	execCtx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.inflight[id] = cancel
	s.mu.Unlock()
	return execCtx, id, func() {
		s.mu.Lock()
		delete(s.inflight, id)
		s.mu.Unlock()
		cancel()
	}
}

func (s *Session) activeContextIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.inflight))
	for id := range s.inflight {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func queryStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := session(rc)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(client)
	enc := json.NewEncoder(client)
	for {
		var req sqldb.QueryRequest
		if err := dec.Decode(&req); err != nil {
			return streamDecodeError(client, err)
		}
		result, frameErr := s.runQuery(rc, req)
		if frameErr != nil {
			if err := enc.Encode(frameErr); err != nil {
				return err
			}
			continue
		}
		if err := enc.Encode(result); err != nil {
			return err
		}
	}
}

// runQuery executes one editor submission. It returns either a result envelope
// or a frame describing an error / confirmation challenge, never both.
func (s *Session) runQuery(rc *plugin.RequestContext, req sqldb.QueryRequest) (any, map[string]any) {
	statements := sqldb.SplitStatements(req.Query)
	if len(statements) == 0 {
		return nil, map[string]any{"error": "a SQL++ statement is required"}
	}
	audit := sqldb.QueryAudit{
		Query:        req.Query,
		Statements:   statements,
		Confirmed:    req.Confirm,
		ReadOnlyMode: s.opts.ReadOnly,
	}
	for _, statement := range statements {
		if isReadOnlyN1QL(statement) {
			continue
		}
		audit.RequiresReview = true
		if s.opts.ReadOnly {
			err := fmt.Errorf("%w: read-only mode blocks %s statements", plugin.ErrForbidden, sqldb.FirstKeyword(statement))
			rc.Audit(plugin.AuditDenied, sqldb.AuditParams(audit), err)
			return nil, map[string]any{"error": err.Error()}
		}
	}
	if audit.RequiresReview && s.opts.RequireConfirm && !req.Confirm {
		rc.Audit(plugin.AuditDenied, sqldb.AuditParams(audit), nil)
		return nil, map[string]any{
			"confirmRequired": true,
			"confirmMessage":  "This statement changes data or metadata in Couchbase. Run it anyway?",
		}
	}

	out := sqldb.QueryResult{Columns: []string{}, Rows: [][]any{}}
	started := time.Now()
	for _, statement := range statements {
		execCtx, contextID, release := s.contextID(rc.Ctx)
		statementStart := time.Now()
		res, err := s.execWithContextID(execCtx, contextID, statement)
		release()
		if err != nil {
			audit.ElapsedMS = time.Since(started).Milliseconds()
			rc.Audit(plugin.AuditError, sqldb.AuditParams(audit), err)
			return nil, map[string]any{"error": err.Error()}
		}
		statementResult := s.resultGrid(res)
		statementResult.Statement = statement
		statementResult.ElapsedMS = time.Since(statementStart).Milliseconds()
		out.Statements = append(out.Statements, statementResult)
		out.Columns = statementResult.Columns
		out.Rows = statementResult.Rows
		out.RowCount = statementResult.RowCount
		out.CommandTag = statementResult.CommandTag
	}
	out.ElapsedMS = time.Since(started).Milliseconds()
	audit.ElapsedMS = out.ElapsedMS
	audit.RowCount = out.RowCount
	audit.CommandTag = out.CommandTag
	rc.Audit(plugin.AuditAllowed, sqldb.AuditParams(audit), nil)
	return out, nil
}

// resultGrid flattens SQL++ documents into an ordered column/row grid. Couchbase
// results are schemaless, so columns are the union of keys across the result set.
func (s *Session) resultGrid(res *n1qlResponse) sqldb.StatementResult {
	objects := make([]map[string]any, 0, len(res.Results))
	scalars := make([]any, 0)
	for _, raw := range res.Results {
		var item any
		if err := json.Unmarshal(raw, &item); err != nil {
			continue
		}
		if obj, ok := item.(map[string]any); ok {
			objects = append(objects, obj)
			continue
		}
		scalars = append(scalars, item)
	}
	out := sqldb.StatementResult{
		Columns:    []string{},
		Rows:       [][]any{},
		RowCount:   res.Metrics.ResultCount,
		CommandTag: commandTag(res),
	}
	if len(objects) == 0 && len(scalars) > 0 {
		out.Columns = []string{"value"}
		for _, value := range scalars {
			out.Rows = append(out.Rows, []any{value})
		}
		return out
	}
	if len(objects) == 0 {
		return out
	}
	out.Columns = unionKeys(objects)
	for _, object := range objects {
		values := make([]any, len(out.Columns))
		for i, column := range out.Columns {
			values[i] = object[column]
		}
		out.Rows = append(out.Rows, values)
	}
	out.Rows = sqldb.RedactRows(out.Columns, out.Rows, s.opts.RedactPatterns)
	return out
}

func commandTag(res *n1qlResponse) string {
	if res.Metrics.MutationCount > 0 {
		return "MUTATED " + strconv.FormatInt(res.Metrics.MutationCount, 10)
	}
	if res.Status != "" && !strings.EqualFold(res.Status, "success") {
		return strings.ToUpper(res.Status)
	}
	return ""
}

// unionKeys keeps document id fields first so a result reads like a table.
func unionKeys(objects []map[string]any) []string {
	priority := map[string]int{"id": 0, "__id": 0, "name": 1, "type": 2}
	seen := map[string]bool{}
	keys := make([]string, 0, 8)
	for _, object := range objects {
		for key := range object {
			if seen[key] {
				continue
			}
			seen[key] = true
			keys = append(keys, key)
		}
	}
	sort.SliceStable(keys, func(i, j int) bool {
		pi, iok := priority[keys[i]]
		pj, jok := priority[keys[j]]
		switch {
		case iok && jok && pi != pj:
			return pi < pj
		case iok != jok:
			return iok
		default:
			return keys[i] < keys[j]
		}
	})
	return keys
}

// queryCancel aborts the in-flight executions of this connection: the local HTTP
// wait first, then the server-side request through the Query admin API.
func queryCancel(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	ids := s.activeContextIDs()
	s.mu.Lock()
	for _, id := range ids {
		if cancel := s.inflight[id]; cancel != nil {
			cancel()
		}
	}
	s.mu.Unlock()

	cancelled := 0
	var active []struct {
		RequestID       string `json:"requestId"`
		ClientContextID string `json:"clientContextID"`
	}
	if status, body, err := s.queryAdmin(rc.Ctx, http.MethodGet, "/admin/active_requests"); err == nil && status < 400 &&
		json.Unmarshal(body, &active) == nil {
		wanted := map[string]bool{}
		for _, id := range ids {
			wanted[id] = true
		}
		for _, request := range active {
			if !wanted[request.ClientContextID] || request.RequestID == "" {
				continue
			}
			if status, _, err := s.queryAdmin(rc.Ctx, http.MethodDelete, "/admin/active_requests/"+url.PathEscape(request.RequestID)); err == nil && status < 400 {
				cancelled++
			}
		}
	}
	return row{"ok": true, "cancelled": cancelled, "aborted": len(ids)}, nil
}

// queryComplete feeds the editor's autocomplete with the cluster's real
// keyspaces, the fields of the active collection, and SQL++ keywords.
func queryComplete(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	items := make([]sqldb.CompletionItem, 0, 64)
	for _, keyword := range n1qlKeywords {
		items = append(items, sqldb.CompletionItem{Label: keyword, Type: "keyword"})
	}
	rows, err := s.queryRows(rc.Ctx, "SELECT RAW k FROM system:keyspaces k")
	if err == nil {
		for _, item := range rows {
			path := stringValue(item["path"])
			if path == "" {
				continue
			}
			name := stringValue(item["name"])
			quoted := quotedKeyspacePath(path)
			items = append(items, sqldb.CompletionItem{
				Label:  path,
				Type:   "keyspace",
				Detail: "collection " + name,
				Apply:  quoted,
			})
		}
	}
	if k, err := keyspaceFrom(rc, s); err == nil && k.Collection != "" {
		if columns, err := s.documentRows(rc, k, defaultSampleSize, 0, ""); err == nil {
			fields := map[string]bool{}
			for _, doc := range columns {
				for key := range doc {
					if key == "ref" || strings.HasPrefix(key, "__") {
						continue
					}
					fields[key] = true
				}
			}
			for _, field := range sortedSet(fields) {
				items = append(items, sqldb.CompletionItem{Label: field, Type: "field", Detail: k.label()})
			}
		}
	}
	return plugin.Page[sqldb.CompletionItem]{Items: items}, nil
}

func quotedKeyspacePath(path string) string {
	parts := strings.Split(strings.TrimPrefix(path, "default:"), ".")
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		quoted = append(quoted, quoteName(part))
	}
	return strings.Join(quoted, ".")
}

var n1qlKeywords = []string{
	"SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT", "OFFSET", "JOIN", "NEST", "UNNEST",
	"LET", "LETTING", "HAVING", "UNION", "INTERSECT", "EXCEPT", "USE KEYS", "USE INDEX",
	"INSERT INTO", "UPSERT INTO", "UPDATE", "DELETE FROM", "MERGE INTO", "RETURNING",
	"CREATE INDEX", "CREATE PRIMARY INDEX", "DROP INDEX", "BUILD INDEX", "ALTER INDEX",
	"CREATE SCOPE", "DROP SCOPE", "CREATE COLLECTION", "DROP COLLECTION",
	"EXPLAIN", "ADVISE", "INFER", "PREPARE", "EXECUTE", "META", "ARRAY", "OBJECT_VALUES",
}

const savedQueryCollection = "saved_queries"

type savedQuery struct {
	Name        string `json:"name"`
	Statement   string `json:"statement"`
	Description string `json:"description,omitempty"`
	Keyspace    string `json:"keyspace,omitempty"`
}

func savedQueryRow(item plugin.StorageItem) (row, error) {
	var value savedQuery
	if err := json.Unmarshal(item.Value, &value); err != nil {
		return nil, fmt.Errorf("%w: stored query %q is corrupt", plugin.ErrUnavailable, item.Key)
	}
	return row{
		"id":          item.Key,
		"name":        value.Name,
		"statement":   value.Statement,
		"description": value.Description,
		"keyspace":    value.Keyspace,
		"updated_at":  item.UpdatedAt.UTC().Format(time.RFC3339),
	}, nil
}

func savedQueriesList(rc *plugin.RequestContext) (any, error) {
	items, err := rc.Storage.List(rc.Ctx, plugin.ConnectionStorage(savedQueryCollection), &plugin.StorageListFilter{
		KeyPrefix:   "query/",
		ContentType: "application/json",
	})
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(items))
	for _, item := range items {
		converted, err := savedQueryRow(item)
		if err != nil {
			return nil, err
		}
		rows = append(rows, converted)
	}
	sort.Slice(rows, func(i, j int) bool {
		return strings.ToLower(stringValue(rows[i]["name"])) < strings.ToLower(stringValue(rows[j]["name"]))
	})
	return plugin.Page[row]{Items: rows}, nil
}

func savedQuerySave(rc *plugin.RequestContext) (any, error) {
	var in savedQuery
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	in.Name = strings.TrimSpace(in.Name)
	in.Statement = strings.TrimSpace(in.Statement)
	if in.Name == "" || in.Statement == "" {
		return nil, fmt.Errorf("%w: name and statement are required", plugin.ErrInvalidInput)
	}
	body, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	key := "query/" + storageKey(in.Name)
	item, err := rc.Storage.Put(rc.Ctx, savedQueryCollection, plugin.StorageItem{
		Key:         key,
		Value:       body,
		ContentType: "application/json",
		Metadata:    map[string]string{"name": in.Name},
	})
	if err != nil {
		return nil, err
	}
	return savedQueryRow(item)
}

func savedQueryDelete(rc *plugin.RequestContext) (any, error) {
	key := strings.TrimSpace(rc.Param("id"))
	if key == "" {
		return nil, fmt.Errorf("%w: a saved query id is required", plugin.ErrInvalidInput)
	}
	if err := rc.Storage.Delete(rc.Ctx, plugin.ConnectionStorage(savedQueryCollection), key); err != nil {
		return nil, err
	}
	return actionResult{OK: true, Message: "saved query deleted"}, nil
}

// storageKey turns a user-chosen name into a stable, safe record key so saving
// the same name twice replaces the record instead of duplicating it.
func storageKey(name string) string {
	out := make([]rune, 0, len(name))
	for _, r := range strings.ToLower(name) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			out = append(out, r)
		default:
			if len(out) > 0 && out[len(out)-1] != '-' {
				out = append(out, '-')
			}
		}
	}
	trimmed := strings.Trim(string(out), "-")
	if trimmed == "" {
		return "query"
	}
	if len(trimmed) > 80 {
		trimmed = trimmed[:80]
	}
	return trimmed
}
