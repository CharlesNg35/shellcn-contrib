package scylladb

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"

	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

// traceCollector implements gocql.Tracer by recording the trace session ids the
// coordinator assigns, so a handler can read system_traces afterwards.
type traceCollector struct {
	mu  sync.Mutex
	ids []string
}

func (t *traceCollector) Trace(traceID []byte) {
	id, err := gocql.UUIDFromBytes(traceID)
	if err != nil {
		return
	}
	t.mu.Lock()
	t.ids = append(t.ids, id.String())
	t.mu.Unlock()
}

func (t *traceCollector) IDs() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.ids...)
}

const traceSessionLimit = 500

func listTraces(rc *plugin.RequestContext) (any, error) {
	s, err := scyllaSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, fmt.Sprintf(`
SELECT session_id, command, coordinator, duration, request, started_at, client, username, request_size, response_size
FROM system_traces.sessions LIMIT %d`, traceSessionLimit), nil)
	if err != nil {
		return nil, err
	}
	out := make([]row, 0, len(rows))
	for _, r := range rows {
		id := fmt.Sprint(r["session_id"])
		out = append(out, row{
			"session_id":    id,
			"started_at":    r["started_at"],
			"duration_us":   int64Value(r["duration"]),
			"request":       r["request"],
			"command":       r["command"],
			"coordinator":   r["coordinator"],
			"client":        r["client"],
			"username":      r["username"],
			"request_size":  int64Value(r["request_size"]),
			"response_size": int64Value(r["response_size"]),
			"ref":           plugin.ResourceIdentity{Kind: "trace", Name: id, UID: id},
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return fmt.Sprint(out[i]["started_at"]) > fmt.Sprint(out[j]["started_at"])
	})
	return pageRows(rc, out)
}

func traceOverview(rc *plugin.RequestContext) (any, error) {
	s, err := scyllaSession(rc)
	if err != nil {
		return nil, err
	}
	id, err := gocql.ParseUUID(strings.TrimSpace(rc.Param("id")))
	if err != nil {
		return nil, fmt.Errorf("%w: trace id must be a UUID", plugin.ErrInvalidInput)
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT session_id, command, coordinator, duration, request, started_at, client, username, request_size, response_size, parameters
FROM system_traces.sessions WHERE session_id = ?`, []any{id})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("%w: trace session %s is not in system_traces", plugin.ErrNotFound, id)
	}
	events, err := queryRows(rc.Ctx, s, `SELECT event_id FROM system_traces.events WHERE session_id = ?`, []any{id})
	if err != nil {
		return nil, err
	}
	r := rows[0]
	return row{
		"session_id":    fmt.Sprint(r["session_id"]),
		"started_at":    r["started_at"],
		"duration_us":   int64Value(r["duration"]),
		"request":       r["request"],
		"command":       r["command"],
		"coordinator":   r["coordinator"],
		"client":        r["client"],
		"username":      r["username"],
		"request_size":  int64Value(r["request_size"]),
		"response_size": int64Value(r["response_size"]),
		"parameters":    r["parameters"],
		"events":        len(events),
	}, nil
}

// traceEvents renders one trace session as timeline records. ScyllaDB reports a
// per-event microsecond offset from the coordinator's start, and the activity
// text carries the shard the work ran on.
func traceEvents(rc *plugin.RequestContext) (any, error) {
	s, err := scyllaSession(rc)
	if err != nil {
		return nil, err
	}
	id, err := gocql.ParseUUID(strings.TrimSpace(rc.Param("id")))
	if err != nil {
		return nil, fmt.Errorf("%w: trace id must be a UUID", plugin.ErrInvalidInput)
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT event_id, activity, source, source_elapsed, thread
FROM system_traces.events WHERE session_id = ?`, []any{id})
	if err != nil {
		return nil, err
	}
	events := make([]row, 0, len(rows))
	var previous int
	for i, r := range rows {
		elapsed := intValue(r["source_elapsed"])
		step := elapsed
		if i > 0 {
			step = elapsed - previous
		}
		previous = elapsed
		activity := fmt.Sprint(r["activity"])
		events = append(events, row{
			"time":       eventTime(fmt.Sprint(r["event_id"])),
			"activity":   activity,
			"detail":     fmt.Sprintf("+%dµs (%+dµs) · %s · %s", elapsed, step, fmt.Sprint(r["thread"]), fmt.Sprint(r["source"])),
			"severity":   string(traceSeverity(activity, step)),
			"icon":       traceIcon(activity),
			"source":     fmt.Sprint(r["source"]),
			"elapsed_us": elapsed,
			"step_us":    step,
			"thread":     fmt.Sprint(r["thread"]),
			"ref":        plugin.ResourceIdentity{Kind: "trace_event", Namespace: id.String(), Name: activity, UID: fmt.Sprint(r["event_id"])},
		})
	}
	total := len(events)
	return plugin.Page[row]{Items: events, Total: &total}, nil
}

func eventTime(eventID string) string {
	id, err := gocql.ParseUUID(eventID)
	if err != nil {
		return ""
	}
	return id.Time().UTC().Format(time.RFC3339Nano)
}

// traceSeverity highlights the steps that dominate a request: anything that adds
// more than a millisecond, plus explicit timeout/error activities.
func traceSeverity(activity string, stepMicros int) plugin.Severity {
	lower := strings.ToLower(activity)
	switch {
	case strings.Contains(lower, "timeout"), strings.Contains(lower, "error"), strings.Contains(lower, "failed"):
		return plugin.SeverityDanger
	case stepMicros >= 1000:
		return plugin.SeverityWarn
	case strings.Contains(lower, "request complete"):
		return plugin.SeveritySuccess
	default:
		return plugin.SeverityInfo
	}
}

func traceIcon(activity string) string {
	lower := strings.ToLower(activity)
	switch {
	case strings.Contains(lower, "cache"):
		return "database-zap"
	case strings.Contains(lower, "sstable"), strings.Contains(lower, "disk"):
		return "hard-drive"
	case strings.Contains(lower, "read"), strings.Contains(lower, "querying"):
		return "search"
	case strings.Contains(lower, "write"), strings.Contains(lower, "mutation"):
		return "pencil"
	case strings.Contains(lower, "complete"):
		return "circle-check"
	default:
		return "route"
	}
}

// runTrace executes one statement with server-side tracing on and returns the
// session id so the UI can open its timeline.
func runTrace(rc *plugin.RequestContext) (any, error) {
	s, err := scyllaSession(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		Query string `json:"query" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	statements := sqldb.SplitStatements(req.Query)
	if len(statements) != 1 {
		return nil, fmt.Errorf("%w: trace exactly one statement", plugin.ErrInvalidInput)
	}
	statement := statements[0]
	if s.opts.ReadOnly && !isReadOnlyStatement(statement) {
		return nil, fmt.Errorf("%w: read-only mode blocks write statements", plugin.ErrForbidden)
	}
	collector := &traceCollector{}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.QueryTimeout)
	defer cancel()
	result, err := executeStatement(ctx, s, statement, true, collector)
	if err != nil {
		return nil, err
	}
	ids := collector.IDs()
	if len(ids) == 0 {
		return nil, fmt.Errorf("%w: the coordinator did not return a trace session", plugin.ErrUnavailable)
	}
	sessionID := ids[len(ids)-1]
	out := row{
		"session_id": sessionID,
		"statement":  statementSummary(statement),
		"row_count":  result.RowCount,
		"elapsed_ms": result.ElapsedMS,
	}
	if micros, err := traceDuration(rc.Ctx, s, sessionID); err == nil {
		out["coordinator_us"] = micros
	}
	return out, nil
}

func listSlowLog(rc *plugin.RequestContext) (any, error) {
	s, err := scyllaSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT start_time, node_ip, shard, command, date, duration, session_id, source_ip, table_names, username
FROM system_traces.node_slow_log LIMIT 200`, nil)
	if err != nil {
		// The slow-query log is optional; an operator enables it per node.
		return plugin.Page[row]{Items: []row{}}, nil
	}
	out := make([]row, 0, len(rows))
	for _, r := range rows {
		id := fmt.Sprint(r["session_id"])
		out = append(out, row{
			"start_time":  eventTime(fmt.Sprint(r["start_time"])),
			"duration_us": int64Value(r["duration"]),
			"command":     statementSummary(fmt.Sprint(r["command"])),
			"tables":      strings.Join(stringSlice(r["table_names"]), ", "),
			"node_ip":     r["node_ip"],
			"shard":       intValue(r["shard"]),
			"source_ip":   r["source_ip"],
			"username":    r["username"],
			"session_id":  id,
			"ref":         plugin.ResourceIdentity{Kind: "trace", Name: id, UID: id},
		})
	}
	return pageRows(rc, out)
}

func traceRunSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Traced statement", Fields: []plugin.Field{
		{Key: "query", Label: "CQL", Type: plugin.FieldTextarea, Required: true, Placeholder: "SELECT * FROM ks.table WHERE id = 1;", Help: "The statement runs once on the cluster with server-side tracing enabled."},
	}}}}
}
