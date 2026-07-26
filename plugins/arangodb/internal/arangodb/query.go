package arangodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	driver "github.com/arangodb/go-driver/v2/arangodb"
	"github.com/arangodb/go-driver/v2/arangodb/shared"

	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

// query runs an AQL statement and collects at most limit results. Every caller
// passes user data through bind parameters; only validated identifiers are ever
// interpolated into the statement text.
func (s *Session) query(ctx context.Context, database, aql string, bind map[string]any, limit int) ([]any, driver.CursorStats, error) {
	db, err := s.database(ctx, database)
	if err != nil {
		return nil, driver.CursorStats{}, err
	}
	if limit <= 0 || limit > plugin.MaxPageLimit {
		limit = s.opts.PageLimit
	}
	ctx, cancel := context.WithTimeout(ctx, s.opts.Timeout)
	defer cancel()
	release := s.trackQuery(database, cancel)
	defer release()

	cursor, err := db.Query(ctx, aql, &driver.QueryOptions{BindVars: bind, BatchSize: limit, Count: false})
	if err != nil {
		return nil, driver.CursorStats{}, arangoErr(err)
	}
	defer func() { _ = cursor.CloseWithContext(context.WithoutCancel(ctx)) }()

	out := make([]any, 0, limit)
	for cursor.HasMore() && len(out) < limit {
		var value any
		if _, err := cursor.ReadDocument(ctx, &value); err != nil {
			if shared.IsNoMoreDocuments(err) {
				break
			}
			return nil, cursor.Statistics(), arangoErr(err)
		}
		out = append(out, normalize(value, s.opts.RedactPatterns))
	}
	return out, cursor.Statistics(), nil
}

// queryRows is the object-shaped variant used by every list route.
func (s *Session) queryRows(ctx context.Context, database, aql string, bind map[string]any, limit int) ([]row, error) {
	values, _, err := s.query(ctx, database, aql, bind, limit)
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(values))
	for _, value := range values {
		rows = append(rows, asMap(value))
	}
	return rows, nil
}

func (s *Session) queryOne(ctx context.Context, database, aql string, bind map[string]any) (any, error) {
	values, _, err := s.query(ctx, database, aql, bind, 1)
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("%w: no result", plugin.ErrNotFound)
	}
	return values[0], nil
}

func (s *Session) queryInt(ctx context.Context, database, aql string, bind map[string]any) int64 {
	value, err := s.queryOne(ctx, database, aql, bind)
	if err != nil {
		return 0
	}
	switch v := value.(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case json.Number:
		n, _ := v.Int64()
		return n
	default:
		return 0
	}
}

// execute runs a data-modifying AQL statement and returns the write counters.
func (s *Session) execute(ctx context.Context, database, aql string, bind map[string]any) (driver.CursorStats, error) {
	if s.opts.ReadOnly {
		return driver.CursorStats{}, fmt.Errorf("%w: connection is read-only", plugin.ErrForbidden)
	}
	_, stats, err := s.query(ctx, database, aql, bind, s.opts.PageLimit)
	return stats, err
}

func queryStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := session(rc)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(client)
	enc := json.NewEncoder(client)
	database := databaseParam(rc, s)
	for {
		var req sqldb.QueryRequest
		if err := dec.Decode(&req); err != nil {
			// The socket is gone or the browser sent a frame this decoder can no
			// longer resynchronise on; either way the editor session is over.
			return nil
		}
		result, err := s.runConsoleQuery(client.Context(), database, req)
		audit := sqldb.AuditParams(sqldb.QueryAudit{
			Query:          req.Query,
			Statements:     []string{req.Query},
			Confirmed:      req.Confirm,
			ReadOnlyMode:   s.opts.ReadOnly,
			RequiresReview: aqlWrites(req.Query),
			RowCount:       result.RowCount,
			ElapsedMS:      result.ElapsedMS,
			CommandTag:     result.CommandTag,
		})
		audit["database"] = database
		rc.Audit(auditResult(err), audit, err)
		if err != nil {
			payload := map[string]any{"error": err.Error()}
			var confirm confirmationError
			if errors.As(err, &confirm) {
				payload["confirmRequired"] = true
				payload["confirmMessage"] = "This AQL modifies data. Run it anyway?"
			}
			if err := enc.Encode(payload); err != nil {
				return err
			}
			continue
		}
		if err := enc.Encode(result); err != nil {
			return err
		}
	}
}

func (s *Session) runConsoleQuery(ctx context.Context, database string, req sqldb.QueryRequest) (sqldb.QueryResult, error) {
	aql, explain := splitExplain(req.Query)
	if aql == "" {
		return sqldb.QueryResult{}, fmt.Errorf("%w: query is empty", plugin.ErrInvalidInput)
	}
	if explain {
		return s.explain(ctx, database, aql)
	}
	writes := aqlWrites(aql)
	if writes && s.opts.ReadOnly {
		return sqldb.QueryResult{}, fmt.Errorf("%w: read-only mode blocks data-modifying AQL", plugin.ErrForbidden)
	}
	if writes && s.opts.RequireConfirm && !req.Confirm {
		return sqldb.QueryResult{}, confirmationError{message: "query requires confirmation"}
	}
	start := time.Now()
	values, stats, err := s.query(ctx, database, aql, nil, s.opts.PageLimit)
	if err != nil {
		return sqldb.QueryResult{}, err
	}
	columns, rows := tabulate(values)
	return sqldb.QueryResult{
		Columns:    columns,
		Rows:       sqldb.RedactRows(columns, rows, s.opts.RedactPatterns),
		RowCount:   int64(len(rows)),
		ElapsedMS:  time.Since(start).Milliseconds(),
		Statement:  aql,
		CommandTag: commandTag(writes, stats),
	}, nil
}

func (s *Session) explain(ctx context.Context, database, aql string) (sqldb.QueryResult, error) {
	db, err := s.database(ctx, database)
	if err != nil {
		return sqldb.QueryResult{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, s.opts.Timeout)
	defer cancel()
	start := time.Now()
	plan, err := db.ExplainQuery(ctx, aql, nil, &driver.ExplainQueryOptions{})
	if err != nil {
		return sqldb.QueryResult{}, arangoErr(err)
	}
	columns := []string{"id", "type", "dependencies", "estimatedCost", "estimatedNrItems"}
	rows := make([][]any, 0, len(plan.Plan.NodesRaw))
	for _, node := range plan.Plan.NodesRaw {
		fields := asMap(node)
		rows = append(rows, []any{
			fields["id"], fields["type"], compact(fields["dependencies"]),
			fields["estimatedCost"], fields["estimatedNrItems"],
		})
	}
	rows = append(rows, []any{"plan", "total", compact(plan.Plan.Rules), plan.Plan.EstimatedCost, plan.Plan.EstimatedNrItems})
	tag := "explain"
	if len(plan.Warnings) > 0 {
		tag = "explain (" + strconv.Itoa(len(plan.Warnings)) + " warnings)"
	}
	return sqldb.QueryResult{
		Columns: columns, Rows: rows, RowCount: int64(len(rows)),
		ElapsedMS: time.Since(start).Milliseconds(), Statement: aql, CommandTag: tag,
	}, nil
}

// tabulate turns AQL results into the query editor's column/row contract. Object
// results become one column per attribute; anything else becomes a value column.
func tabulate(values []any) ([]string, [][]any) {
	columns := make([]string, 0, 8)
	seen := map[string]bool{}
	objects := len(values) > 0
	for _, value := range values {
		fields, ok := value.(map[string]any)
		if !ok {
			objects = false
			break
		}
		for _, key := range sortedKeys(fields) {
			if !seen[key] {
				seen[key] = true
				columns = append(columns, key)
			}
		}
	}
	if !objects {
		rows := make([][]any, 0, len(values))
		for _, value := range values {
			rows = append(rows, []any{value})
		}
		return []string{"result"}, rows
	}
	sort.SliceStable(columns, func(i, j int) bool {
		if a, b := columnRank(columns[i]), columnRank(columns[j]); a != b {
			return a < b
		}
		return columns[i] < columns[j]
	})
	rows := make([][]any, 0, len(values))
	for _, value := range values {
		fields := value.(map[string]any)
		line := make([]any, len(columns))
		for i, column := range columns {
			line[i] = fields[column]
		}
		rows = append(rows, line)
	}
	return columns, rows
}

// columnRank floats the ArangoDB system attributes to the left of the grid.
func columnRank(column string) int {
	switch column {
	case "_key":
		return 0
	case "_id":
		return 1
	case "_rev":
		return 2
	case "_from":
		return 3
	case "_to":
		return 4
	default:
		return 5
	}
}

func commandTag(writes bool, stats driver.CursorStats) string {
	if !writes {
		return "read"
	}
	return fmt.Sprintf("writes executed %d, ignored %d", stats.WritesExecutedInt, stats.WritesIgnoredInt)
}

func auditResult(err error) plugin.AuditResult {
	if err == nil {
		return plugin.AuditAllowed
	}
	var confirm confirmationError
	if errors.As(err, &confirm) || errors.Is(err, plugin.ErrForbidden) || errors.Is(err, plugin.ErrUnauthorized) {
		return plugin.AuditDenied
	}
	return plugin.AuditError
}

func queryCancel(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	return map[string]any{"ok": true, "cancelled": s.cancelDatabase(databaseParam(rc, s))}, nil
}

func queryCompletion(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	items := make([]sqldb.CompletionItem, 0, 64)
	for _, keyword := range aqlKeywords {
		items = append(items, sqldb.CompletionItem{Label: keyword, Type: "keyword", Apply: keyword})
	}
	for _, fn := range aqlFunctions {
		items = append(items, sqldb.CompletionItem{Label: fn.Name, Type: "function", Detail: fn.Detail, Apply: fn.Apply})
	}
	database := databaseParam(rc, s)
	if db, err := s.database(rc.Ctx, database); err == nil {
		ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
		defer cancel()
		if collections, err := db.Collections(ctx); err == nil {
			for _, collection := range collections {
				if strings.HasPrefix(collection.Name(), "_") {
					continue
				}
				items = append(items, sqldb.CompletionItem{
					Label: collection.Name(), Type: "collection", Detail: "collection in " + database,
					Apply: "FOR doc IN " + quote(collection.Name()) + " LIMIT 25 RETURN doc",
				})
			}
		}
		if graphs, err := db.Graphs(ctx); err == nil {
			for {
				graph, err := graphs.Read()
				if err != nil {
					break
				}
				items = append(items, sqldb.CompletionItem{
					Label: graph.Name(), Type: "graph", Detail: "named graph",
					Apply: "FOR v, e, p IN 1..2 ANY @start GRAPH " + aqlString(graph.Name()) + " RETURN p",
				})
			}
		}
		if views, err := db.ViewsAll(ctx); err == nil {
			for _, view := range views {
				items = append(items, sqldb.CompletionItem{
					Label: view.Name(), Type: "view", Detail: string(view.Type()),
					Apply: "FOR doc IN " + quote(view.Name()) + " SEARCH ANALYZER(doc.text == \"value\", \"text_en\") RETURN doc",
				})
			}
		}
	}
	return items, nil
}

func runningQueries(rc *plugin.RequestContext) (any, error) {
	return aqlQueryRows(rc, false)
}

func slowQueries(rc *plugin.RequestContext) (any, error) {
	return aqlQueryRows(rc, true)
}

func aqlQueryRows(rc *plugin.RequestContext, slow bool) (any, error) {
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
	list, err := db.ListOfRunningAQLQueries(ctx, nil)
	if slow {
		list, err = db.ListOfSlowAQLQueries(ctx, nil)
	}
	if err != nil {
		return nil, arangoErr(err)
	}
	rows := make([]row, 0, len(list))
	for _, item := range list {
		id := stringOrEmpty(item.Id)
		runtime := 0.0
		if item.RunTime != nil {
			runtime = *item.RunTime
		}
		rows = append(rows, row{
			"id":       id,
			"database": stringOrEmpty(item.Database),
			"user":     stringOrEmpty(item.User),
			"query":    stringOrEmpty(item.Query),
			"state":    stringOrEmpty(item.State),
			"started":  stringOrEmpty(item.Started),
			"runtime":  runtime,
			"peak_memory": func() int64 {
				if item.PeakMemoryUsage == nil {
					return 0
				}
				return int64(*item.PeakMemoryUsage)
			}(),
			"modification": boolOrFalse(item.ModificationQuery),
			"stream":       boolOrFalse(item.Stream),
			"severity":     querySeverity(stringOrEmpty(item.State), runtime),
			"ref":          plugin.ResourceIdentity{Kind: kindAQLQuery, Namespace: database, Name: id, UID: encodeID(kindAQLQuery, database, id)},
		})
	}
	return pageRows(rc, rows)
}

func querySeverity(state string, runtime float64) string {
	switch {
	case strings.EqualFold(state, "killed"):
		return "danger"
	case runtime >= 10:
		return "warn"
	default:
		return "info"
	}
}

func killQuery(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	parts, err := decodeID(rc.Param("id"), 3)
	if err != nil {
		return nil, err
	}
	db, err := s.database(rc.Ctx, parts[1])
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	if err := db.KillAQLQuery(ctx, parts[2], nil); err != nil {
		return nil, arangoErr(err)
	}
	return actionResult{OK: true}, nil
}

func clearSlowQueries(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	db, err := s.database(rc.Ctx, databaseParam(rc, s))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	if err := db.ClearSlowAQLQueries(ctx, nil); err != nil {
		return nil, arangoErr(err)
	}
	return actionResult{OK: true}, nil
}

// queryRead resolves one AQL query id against the running and slow lists so the
// activity detail view has a real object to render.
func queryRead(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	parts, err := decodeID(rc.Param("id"), 3)
	if err != nil {
		return nil, err
	}
	database, id := parts[1], parts[2]
	db, err := s.database(rc.Ctx, database)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	lists := make([][]driver.RunningAQLQuery, 0, 2)
	if running, err := db.ListOfRunningAQLQueries(ctx, nil); err == nil {
		lists = append(lists, running)
	}
	if slow, err := db.ListOfSlowAQLQueries(ctx, nil); err == nil {
		lists = append(lists, slow)
	}
	for _, list := range lists {
		for _, item := range list {
			if stringOrEmpty(item.Id) != id {
				continue
			}
			runtime := 0.0
			if item.RunTime != nil {
				runtime = *item.RunTime
			}
			peak := int64(0)
			if item.PeakMemoryUsage != nil {
				peak = int64(*item.PeakMemoryUsage)
			}
			return row{
				"id": id, "database": database,
				"user":         stringOrEmpty(item.User),
				"query":        stringOrEmpty(item.Query),
				"state":        stringOrEmpty(item.State),
				"started":      stringOrEmpty(item.Started),
				"runtime":      runtime,
				"peak_memory":  peak,
				"modification": boolOrFalse(item.ModificationQuery),
				"stream":       boolOrFalse(item.Stream),
				// Bind variables carry the literal values a statement writes, so they
				// go through the connection's redaction patterns like any other data.
				"bind_vars": normalize(item.BindVars, s.opts.RedactPatterns),
				"warnings":  intOrZero(item.Warnings),
			}, nil
		}
	}
	return nil, fmt.Errorf("%w: AQL query %q is no longer tracked", plugin.ErrNotFound, id)
}
