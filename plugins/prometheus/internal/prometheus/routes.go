package prometheus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

func Routes() []plugin.Route {
	return []plugin.Route{
		{ID: rid("overview"), Method: plugin.MethodGet, Path: "/overview", Permission: "prometheus.read", Risk: plugin.RiskSafe, AuditEvent: rid("overview"), Handle: overview},
		{ID: rid("status.tree"), Method: plugin.MethodGet, Path: "/tree/status", Permission: "prometheus.status.read", Risk: plugin.RiskSafe, AuditEvent: rid("status.tree"), Handle: statusTree},
		{ID: rid("status.list"), Method: plugin.MethodGet, Path: "/status", Permission: "prometheus.status.read", Risk: plugin.RiskSafe, AuditEvent: rid("status.list"), Handle: statusList},
		{ID: rid("status.read"), Method: plugin.MethodGet, Path: "/status/{status}", Permission: "prometheus.status.read", Risk: plugin.RiskSafe, AuditEvent: rid("status.read"), Handle: statusRead},
		{ID: rid("targets.tree"), Method: plugin.MethodGet, Path: "/tree/targets", Permission: "prometheus.targets.read", Risk: plugin.RiskSafe, AuditEvent: rid("targets.tree"), Handle: targetTree},
		{ID: rid("targets.list"), Method: plugin.MethodGet, Path: "/targets", Permission: "prometheus.targets.read", Risk: plugin.RiskSafe, AuditEvent: rid("targets.list"), Handle: targetList},
		{ID: rid("target.read"), Method: plugin.MethodGet, Path: "/targets/{target}", Permission: "prometheus.targets.read", Risk: plugin.RiskSafe, AuditEvent: rid("target.read"), Handle: targetRead},
		{ID: rid("target.metadata"), Method: plugin.MethodGet, Path: "/targets/{target}/metadata", Permission: "prometheus.targets.read", Risk: plugin.RiskSafe, AuditEvent: rid("target.metadata"), Handle: targetMetadata},
		{ID: rid("alerts.tree"), Method: plugin.MethodGet, Path: "/tree/alerts", Permission: "prometheus.alerts.read", Risk: plugin.RiskSafe, AuditEvent: rid("alerts.tree"), Handle: alertTree},
		{ID: rid("alerts.list"), Method: plugin.MethodGet, Path: "/alerts", Permission: "prometheus.alerts.read", Risk: plugin.RiskSafe, AuditEvent: rid("alerts.list"), Handle: alertList},
		{ID: rid("alert.read"), Method: plugin.MethodGet, Path: "/alerts/{alert}", Permission: "prometheus.alerts.read", Risk: plugin.RiskSafe, AuditEvent: rid("alert.read"), Handle: alertRead},
		{ID: rid("rules.tree"), Method: plugin.MethodGet, Path: "/tree/rules", Permission: "prometheus.rules.read", Risk: plugin.RiskSafe, AuditEvent: rid("rules.tree"), Handle: ruleTree},
		{ID: rid("rules.list"), Method: plugin.MethodGet, Path: "/rules", Permission: "prometheus.rules.read", Risk: plugin.RiskSafe, AuditEvent: rid("rules.list"), Handle: ruleList},
		{ID: rid("rule.read"), Method: plugin.MethodGet, Path: "/rules/{rule}", Permission: "prometheus.rules.read", Risk: plugin.RiskSafe, AuditEvent: rid("rule.read"), Handle: ruleRead},
		{ID: rid("metrics.tree"), Method: plugin.MethodGet, Path: "/tree/metrics", Permission: "prometheus.metrics.read", Risk: plugin.RiskSafe, AuditEvent: rid("metrics.tree"), Handle: metricTree},
		{ID: rid("metrics.list"), Method: plugin.MethodGet, Path: "/metrics", Permission: "prometheus.metrics.read", Risk: plugin.RiskSafe, AuditEvent: rid("metrics.list"), Handle: metricList},
		{ID: rid("metric.read"), Method: plugin.MethodGet, Path: "/metrics/{metric}", Permission: "prometheus.metrics.read", Risk: plugin.RiskSafe, AuditEvent: rid("metric.read"), Handle: metricRead},
		{ID: rid("metric.series"), Method: plugin.MethodGet, Path: "/metrics/{metric}/series", Permission: "prometheus.metrics.read", Risk: plugin.RiskSafe, AuditEvent: rid("metric.series"), Handle: metricSeries},
		{ID: rid("labels.tree"), Method: plugin.MethodGet, Path: "/tree/labels", Permission: "prometheus.labels.read", Risk: plugin.RiskSafe, AuditEvent: rid("labels.tree"), Handle: labelTree},
		{ID: rid("labels.list"), Method: plugin.MethodGet, Path: "/labels", Permission: "prometheus.labels.read", Risk: plugin.RiskSafe, AuditEvent: rid("labels.list"), Handle: labelList},
		{ID: rid("label.values"), Method: plugin.MethodGet, Path: "/labels/{label}/values", Permission: "prometheus.labels.read", Risk: plugin.RiskSafe, AuditEvent: rid("label.values"), Handle: labelValues},
		{ID: rid("query"), Method: plugin.MethodWS, Path: "/query", Permission: "prometheus.query.execute", Risk: plugin.RiskSafe, AuditEvent: rid("query"), Stream: queryStream},
		{ID: rid("metrics.live"), Method: plugin.MethodWS, Path: "/metrics/live", Permission: "prometheus.metrics.read", Risk: plugin.RiskSafe, AuditEvent: rid("metrics.live"), Stream: liveMetricsStream},
		{ID: rid("completion"), Method: plugin.MethodGet, Path: "/completion", Permission: "prometheus.read", Risk: plugin.RiskSafe, AuditEvent: rid("completion"), Handle: completionRoute},
		{ID: rid("snapshot.create"), Method: plugin.MethodPost, Path: "/admin/snapshot", Permission: "prometheus.admin.write", Risk: plugin.RiskWrite, AuditEvent: rid("snapshot.create"), Input: snapshotSchema(), Handle: createSnapshot},
		{ID: rid("series.delete"), Method: plugin.MethodPost, Path: "/admin/delete-series", Permission: "prometheus.admin.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("series.delete"), Input: deleteSeriesSchema(), Handle: deleteSeries},
		{ID: rid("tombstones.clean"), Method: plugin.MethodPost, Path: "/admin/clean-tombstones", Permission: "prometheus.admin.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("tombstones.clean"), Handle: cleanTombstones},
		{ID: rid("config.reload"), Method: plugin.MethodPost, Path: "/admin/reload", Permission: "prometheus.lifecycle.write", Risk: plugin.RiskWrite, AuditEvent: rid("config.reload"), Handle: reloadConfig},
	}
}

func snapshotSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Snapshot", Fields: []plugin.Field{
		{Key: "skip_head", Label: "Skip head block", Type: plugin.FieldToggle},
	}}}}
}

func deleteSeriesSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Delete series", Fields: []plugin.Field{
		{Key: "match", Label: "Series selector", Type: plugin.FieldText, Required: true, Default: "{__name__=~\".+\"}"},
		{Key: "start", Label: "Start", Type: plugin.FieldText, Placeholder: "2026-05-27T00:00:00Z"},
		{Key: "end", Label: "End", Type: plugin.FieldText, Placeholder: "now"},
	}}}}
}

func session(rc *plugin.RequestContext) (*Session, error) { return unwrap(rc.Session) }

func overview(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	out := row{}
	_ = s.client.api(rc.Ctx, http.MethodGet, "/api/v1/status/buildinfo", nil, nil, ptr(out, "build"))
	_ = s.client.api(rc.Ctx, http.MethodGet, "/api/v1/status/runtimeinfo", nil, nil, ptr(out, "runtime"))
	_ = s.client.api(rc.Ctx, http.MethodGet, "/api/v1/status/tsdb", nil, nil, ptr(out, "tsdb"))
	_ = s.client.api(rc.Ctx, http.MethodGet, "/api/v1/targets", nil, nil, ptr(out, "targets"))
	return out, nil
}

var statusItems = []row{
	{"name": "buildinfo", "description": "Build and version information"},
	{"name": "runtimeinfo", "description": "Runtime and command-line information"},
	{"name": "tsdb", "description": "TSDB cardinality and head-block statistics"},
	{"name": "config", "description": "Loaded Prometheus configuration"},
	{"name": "flags", "description": "Runtime flags"},
}

func statusTree(rc *plugin.RequestContext) (any, error) {
	res, err := statusList(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name := fmt.Sprint(item["name"])
		ref := plugin.ResourceRef{Kind: "status", Name: name, UID: name}
		nodes = append(nodes, plugin.TreeNode{Key: "status:" + name, Label: name, Icon: icon("activity"), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, Total: page.Total}, nil
}

func statusList(rc *plugin.RequestContext) (any, error) {
	rows := cloneRows(statusItems)
	for _, item := range rows {
		name := fmt.Sprint(item["name"])
		item["ref"] = plugin.ResourceRef{Kind: "status", Name: name, UID: name}
	}
	return broker.PageRows(rc, rows)
}

func statusRead(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	name := statusParam(rc)
	paths := map[string]string{
		"buildinfo":   "/api/v1/status/buildinfo",
		"runtimeinfo": "/api/v1/status/runtimeinfo",
		"tsdb":        "/api/v1/status/tsdb",
		"config":      "/api/v1/status/config",
		"flags":       "/api/v1/status/flags",
	}
	path, ok := paths[name]
	if !ok {
		return nil, fmt.Errorf("%w: status %q", plugin.ErrNotFound, name)
	}
	var out any
	err = s.client.api(rc.Ctx, http.MethodGet, path, nil, nil, &out)
	return out, err
}

func targetTree(rc *plugin.RequestContext) (any, error) {
	res, err := targetList(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		uid := fmt.Sprint(item["uid"])
		label := fmt.Sprint(item["job"]) + "/" + fmt.Sprint(item["instance"])
		ref := plugin.ResourceRef{Kind: "target", Name: label, UID: uid}
		nodes = append(nodes, plugin.TreeNode{Key: "target:" + uid, Label: label, Icon: targetIcon(item), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, Total: page.Total}, nil
}

func targetList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	rows, err := targets(rc.Ctx, s)
	if err != nil {
		return nil, err
	}
	for _, item := range rows {
		label := fmt.Sprint(item["job"]) + "/" + fmt.Sprint(item["instance"])
		item["ref"] = plugin.ResourceRef{Kind: "target", Name: label, UID: fmt.Sprint(item["uid"])}
	}
	return broker.PageRows(rc, rows)
}

func targetRead(rc *plugin.RequestContext) (any, error) {
	return findByUID(rc, targets, targetParam(rc), "target")
}

func targetMetadata(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	target, err := findTarget(rc)
	if err != nil {
		return nil, err
	}
	labels, _ := target["labels"].(map[string]any)
	selector := labelsSelector(labels)
	var raw []row
	q := url.Values{"limit": []string{strconv.Itoa(s.opts.PageLimit)}}
	if selector != "" {
		q.Set("match_target", selector)
	}
	if err := s.client.api(rc.Ctx, http.MethodGet, "/api/v1/targets/metadata", q, nil, &raw); err != nil {
		return nil, err
	}
	for _, item := range raw {
		item["target"] = fmt.Sprint(target["job"]) + "/" + fmt.Sprint(target["instance"])
	}
	return broker.PageRows(rc, raw)
}

func alertTree(rc *plugin.RequestContext) (any, error) {
	res, err := alertList(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		uid := fmt.Sprint(item["uid"])
		ref := plugin.ResourceRef{Kind: "alert", Name: fmt.Sprint(item["alertname"]), UID: uid}
		nodes = append(nodes, plugin.TreeNode{Key: "alert:" + uid, Label: fmt.Sprint(item["alertname"]), Icon: icon("bell"), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, Total: page.Total}, nil
}

func alertList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	rows, err := alerts(rc.Ctx, s)
	if err != nil {
		return nil, err
	}
	for _, item := range rows {
		item["ref"] = plugin.ResourceRef{Kind: "alert", Name: fmt.Sprint(item["alertname"]), UID: fmt.Sprint(item["uid"])}
	}
	return broker.PageRows(rc, rows)
}

func alertRead(rc *plugin.RequestContext) (any, error) {
	return findByUID(rc, alerts, alertParam(rc), "alert")
}

func ruleTree(rc *plugin.RequestContext) (any, error) {
	res, err := ruleList(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		uid := fmt.Sprint(item["uid"])
		ref := plugin.ResourceRef{Kind: "rule", Name: fmt.Sprint(item["name"]), UID: uid}
		nodes = append(nodes, plugin.TreeNode{Key: "rule:" + uid, Label: fmt.Sprint(item["name"]), Icon: icon("list-checks"), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, Total: page.Total}, nil
}

func ruleList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	rows, err := rules(rc.Ctx, s)
	if err != nil {
		return nil, err
	}
	for _, item := range rows {
		item["ref"] = plugin.ResourceRef{Kind: "rule", Name: fmt.Sprint(item["name"]), UID: fmt.Sprint(item["uid"])}
	}
	return broker.PageRows(rc, rows)
}

func ruleRead(rc *plugin.RequestContext) (any, error) {
	return findByUID(rc, rules, ruleParam(rc), "rule")
}

func metricTree(rc *plugin.RequestContext) (any, error) {
	res, err := metricList(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name := fmt.Sprint(item["name"])
		ref := plugin.ResourceRef{Kind: "metric", Name: name, UID: name}
		nodes = append(nodes, plugin.TreeNode{Key: "metric:" + name, Label: name, Icon: icon("chart-line"), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, Total: page.Total}, nil
}

func metricList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	names, err := metricNames(rc.Ctx, s)
	if err != nil {
		return nil, err
	}
	meta, _ := metadata(rc.Ctx, s, "")
	rows := make([]row, 0, len(names))
	filter := strings.TrimSpace(rc.Query().Get("filter"))
	for _, name := range names {
		if filter != "" && !strings.Contains(name, filter) {
			continue
		}
		item := row{"name": name}
		if entries := meta[name]; len(entries) > 0 {
			item["type"] = entries[0]["type"]
			item["help"] = entries[0]["help"]
			item["unit"] = entries[0]["unit"]
		}
		item["ref"] = plugin.ResourceRef{Kind: "metric", Name: name, UID: name}
		rows = append(rows, item)
	}
	return broker.PageRows(rc, rows)
}

func metricRead(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	meta, err := metadata(rc.Ctx, s, metricParam(rc))
	if err != nil {
		return nil, err
	}
	return row{"metric": metricParam(rc), "metadata": meta[metricParam(rc)]}, nil
}

func metricSeries(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	q := url.Values{"match[]": []string{metricParam(rc)}, "limit": []string{strconv.Itoa(limitFromRequest(rc, s))}}
	var raw []map[string]string
	if err := s.client.api(rc.Ctx, http.MethodGet, "/api/v1/series", q, nil, &raw); err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(raw))
	for _, labels := range raw {
		rows = append(rows, row{"metric": labels["__name__"], "labels": labels})
	}
	return broker.PageRows(rc, rows)
}

func labelTree(rc *plugin.RequestContext) (any, error) {
	res, err := labelList(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name := fmt.Sprint(item["name"])
		ref := plugin.ResourceRef{Kind: "label", Name: name, UID: name}
		nodes = append(nodes, plugin.TreeNode{Key: "label:" + name, Label: name, Icon: icon("tag"), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, Total: page.Total}, nil
}

func labelList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var names []string
	if err := s.client.api(rc.Ctx, http.MethodGet, "/api/v1/labels", nil, nil, &names); err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(names))
	for _, name := range names {
		rows = append(rows, row{"name": name, "ref": plugin.ResourceRef{Kind: "label", Name: name, UID: name}})
	}
	return broker.PageRows(rc, rows)
}

func labelValues(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var values []string
	if err := s.client.api(rc.Ctx, http.MethodGet, "/api/v1/label/"+url.PathEscape(labelParam(rc))+"/values", nil, nil, &values); err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(values))
	for _, value := range values {
		rows = append(rows, row{"value": value})
	}
	return broker.PageRows(rc, rows)
}

func createSnapshot(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureAdmin(s); err != nil {
		return nil, err
	}
	var req struct {
		SkipHead bool `json:"skip_head"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	q := url.Values{}
	if req.SkipHead {
		q.Set("skip_head", "true")
	}
	var out row
	err = s.client.api(rc.Ctx, http.MethodPost, "/api/v1/admin/tsdb/snapshot", q, nil, &out)
	return out, err
}

func deleteSeries(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureAdmin(s); err != nil {
		return nil, err
	}
	var req struct {
		Match string `json:"match"`
		Start string `json:"start"`
		End   string `json:"end"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.Match) == "" {
		req.Match = metricParam(rc)
	}
	q := url.Values{"match[]": []string{req.Match}}
	if req.Start != "" {
		q.Set("start", req.Start)
	}
	if req.End != "" && req.End != "now" {
		q.Set("end", req.End)
	}
	_, err = s.client.raw(rc.Ctx, http.MethodPost, "/api/v1/admin/tsdb/delete_series", q, "", nil)
	return row{"ok": err == nil}, err
}

func cleanTombstones(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureAdmin(s); err != nil {
		return nil, err
	}
	_, err = s.client.raw(rc.Ctx, http.MethodPost, "/api/v1/admin/tsdb/clean_tombstones", nil, "", nil)
	return row{"ok": err == nil}, err
}

func reloadConfig(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if !s.opts.LifecycleAPI {
		return nil, fmt.Errorf("%w: enable lifecycle API actions in the connection config for Prometheus servers started with --web.enable-lifecycle", plugin.ErrForbidden)
	}
	_, err = s.client.raw(rc.Ctx, http.MethodPost, "/-/reload", nil, "", nil)
	return row{"ok": err == nil}, err
}

func ensureAdmin(s *Session) error {
	if !s.opts.AdminAPI {
		return fmt.Errorf("%w: enable admin API actions in the connection config for Prometheus servers started with --web.enable-admin-api", plugin.ErrForbidden)
	}
	return nil
}

type queryResultData struct {
	ResultType string            `json:"resultType"`
	Result     []json.RawMessage `json:"result"`
}

type promSample struct {
	Metric map[string]string `json:"metric"`
	Value  []any             `json:"value"`
	Values [][]any           `json:"values"`
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
			if client.Context().Err() != nil || errors.Is(err, io.EOF) {
				return nil
			}
			if err := enc.Encode(map[string]any{"error": "Invalid query request."}); err != nil {
				return err
			}
			continue
		}
		result, err := executeQuery(client.Context(), s, req.Query)
		rc.Audit(auditResult(err), map[string]string{"query": req.Query}, err)
		if err != nil {
			if err := enc.Encode(map[string]any{"error": err.Error()}); err != nil {
				return err
			}
			continue
		}
		if err := enc.Encode(result); err != nil {
			return err
		}
	}
}

func executeQuery(ctx context.Context, s *Session, raw string) (sqldb.QueryResult, error) {
	started := time.Now()
	q, endpoint, statement, err := queryParams(raw)
	if err != nil {
		return sqldb.QueryResult{}, err
	}
	var data queryResultData
	if err := s.client.api(ctx, http.MethodGet, endpoint, q, nil, &data); err != nil {
		return sqldb.QueryResult{}, err
	}
	columns, rows := queryRows(data)
	return sqldb.QueryResult{
		Columns:    columns,
		Rows:       rows,
		RowCount:   int64(len(rows)),
		ElapsedMS:  time.Since(started).Milliseconds(),
		Statement:  statement,
		CommandTag: strings.ToUpper(strings.TrimPrefix(endpoint, "/api/v1/")),
	}, nil
}

func queryParams(raw string) (url.Values, string, string, error) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return nil, "", "", fmt.Errorf("%w: query is required", plugin.ErrInvalidInput)
	}
	if strings.HasPrefix(text, "{") {
		var body map[string]any
		if err := json.Unmarshal([]byte(text), &body); err != nil {
			return nil, "", "", fmt.Errorf("%w: query JSON must be an object", plugin.ErrInvalidInput)
		}
		query := strings.TrimSpace(fmt.Sprint(body["query"]))
		if query == "" || query == "<nil>" {
			return nil, "", "", fmt.Errorf("%w: query JSON must include query", plugin.ErrInvalidInput)
		}
		q := url.Values{"query": []string{query}}
		endpoint := "/api/v1/query"
		if typ := strings.TrimSpace(fmt.Sprint(body["type"])); typ == "range" {
			endpoint = "/api/v1/query_range"
			now := time.Now().UTC()
			start := now.Add(-1 * time.Hour)
			end := now
			if rawStart, ok := body["start"]; ok {
				if parsed, err := parseQueryTime(fmt.Sprint(rawStart), now); err == nil {
					start = parsed
				}
			}
			if rawEnd, ok := body["end"]; ok {
				if parsed, err := parseQueryTime(fmt.Sprint(rawEnd), now); err == nil {
					end = parsed
				}
			}
			step := "30s"
			if rawStep := strings.TrimSpace(fmt.Sprint(body["step"])); rawStep != "" && rawStep != "<nil>" {
				step = rawStep
			}
			q.Set("start", formatQueryTime(start))
			q.Set("end", formatQueryTime(end))
			q.Set("step", step)
		} else if rawTime := strings.TrimSpace(fmt.Sprint(body["time"])); rawTime != "" && rawTime != "<nil>" {
			q.Set("time", rawTime)
		}
		if timeout := strings.TrimSpace(fmt.Sprint(body["timeout"])); timeout != "" && timeout != "<nil>" {
			q.Set("timeout", timeout)
		}
		return q, endpoint, text, nil
	}
	return url.Values{"query": []string{text}}, "/api/v1/query", text, nil
}

func queryRows(data queryResultData) ([]string, [][]any) {
	switch data.ResultType {
	case "matrix":
		rows := make([][]any, 0, len(data.Result))
		for _, raw := range data.Result {
			var sample promSample
			if json.Unmarshal(raw, &sample) == nil {
				rows = append(rows, []any{metricName(sample.Metric), sample.Metric, sample.Values})
			}
		}
		return []string{"metric", "labels", "values"}, rows
	case "vector":
		rows := make([][]any, 0, len(data.Result))
		for _, raw := range data.Result {
			var sample promSample
			if json.Unmarshal(raw, &sample) == nil {
				ts, value := sampleValue(sample.Value)
				rows = append(rows, []any{metricName(sample.Metric), sample.Metric, value, ts})
			}
		}
		return []string{"metric", "labels", "value", "timestamp"}, rows
	default:
		rows := make([][]any, 0, len(data.Result))
		for _, raw := range data.Result {
			var value any
			if json.Unmarshal(raw, &value) == nil {
				rows = append(rows, []any{value})
			}
		}
		return []string{"value"}, rows
	}
}

func liveMetricsStream(rc *plugin.RequestContext, stream plugin.ClientStream) error {
	s, err := session(rc)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(stream)
	ticker := time.NewTicker(s.opts.PollInterval)
	defer ticker.Stop()
	for {
		frame := liveFrame(stream.Context(), s)
		if err := enc.Encode(frame); err != nil {
			return err
		}
		select {
		case <-stream.Context().Done():
			return nil
		case <-ticker.C:
		}
	}
}

func liveFrame(ctx context.Context, s *Session) row {
	targetRows, _ := targets(ctx, s)
	targetsTotal := float64(len(targetRows))
	upTargets := 0.0
	for _, item := range targetRows {
		if fmt.Sprint(item["health"]) == "up" {
			upTargets++
		}
	}
	return row{
		"targets":       targetsTotal,
		"targets_up":    upTargets,
		"target_health": pct(upTargets, targetsTotal),
		"head_series":   instantValue(ctx, s, "prometheus_tsdb_head_series"),
		"queries":       instantValue(ctx, s, "prometheus_engine_queries"),
	}
}

func completionRoute(*plugin.RequestContext) (any, error) {
	return []sqldb.CompletionItem{
		{Label: "up", Type: "metric", Apply: "up"},
		{Label: "rate(http_requests_total[5m])", Type: "function", Apply: "rate(http_requests_total[5m])"},
		{Label: "sum by (job) (up)", Type: "query", Apply: "sum by (job) (up)"},
		{Label: `{"type":"range","query":"up","start":"-1h","end":"now","step":"30s"}`, Type: "range", Apply: `{"type":"range","query":"up","start":"-1h","end":"now","step":"30s"}`},
	}, nil
}

func targets(ctx context.Context, s *Session) ([]row, error) {
	var out struct {
		Active  []row `json:"activeTargets"`
		Dropped []row `json:"droppedTargets"`
	}
	if err := s.client.api(ctx, http.MethodGet, "/api/v1/targets", url.Values{"state": []string{"any"}}, nil, &out); err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(out.Active)+len(out.Dropped))
	for _, item := range out.Active {
		labels, _ := item["labels"].(map[string]any)
		item["state"] = "active"
		item["job"] = fmt.Sprint(labels["job"])
		item["instance"] = fmt.Sprint(labels["instance"])
		item["uid"] = stableID(item["scrapeUrl"], item["labels"])
		rows = append(rows, item)
	}
	for _, item := range out.Dropped {
		labels, _ := item["labels"].(map[string]any)
		if len(labels) == 0 {
			labels, _ = item["discoveredLabels"].(map[string]any)
		}
		item["state"] = "dropped"
		item["health"] = "dropped"
		item["job"] = fmt.Sprint(labels["job"])
		item["instance"] = fmt.Sprint(labels["instance"])
		item["uid"] = stableID(item["scrapeUrl"], item["labels"], item["discoveredLabels"])
		rows = append(rows, item)
	}
	return rows, nil
}

func alerts(ctx context.Context, s *Session) ([]row, error) {
	var out struct {
		Alerts []row `json:"alerts"`
	}
	if err := s.client.api(ctx, http.MethodGet, "/api/v1/alerts", nil, nil, &out); err != nil {
		return nil, err
	}
	for _, item := range out.Alerts {
		labels, _ := item["labels"].(map[string]any)
		item["alertname"] = fmt.Sprint(labels["alertname"])
		item["uid"] = stableID(item["labels"], item["activeAt"])
	}
	return out.Alerts, nil
}

func rules(ctx context.Context, s *Session) ([]row, error) {
	var out struct {
		Groups []struct {
			Name  string `json:"name"`
			File  string `json:"file"`
			Rules []row  `json:"rules"`
		} `json:"groups"`
	}
	if err := s.client.api(ctx, http.MethodGet, "/api/v1/rules", nil, nil, &out); err != nil {
		return nil, err
	}
	rows := []row{}
	for _, group := range out.Groups {
		for _, item := range group.Rules {
			item["group"] = group.Name
			item["file"] = group.File
			if name := strings.TrimSpace(fmt.Sprint(item["name"])); name == "" || name == "<nil>" {
				if labels, _ := item["labels"].(map[string]any); labels != nil {
					item["name"] = labels["alertname"]
				}
			}
			item["uid"] = stableID(group.Name, item["name"], item["query"], item["type"], item["labels"])
			rows = append(rows, item)
		}
	}
	return rows, nil
}

// metricNames returns the full metric catalogue. Names are bounded and cheap, so
// no server-side limit is applied — the metric browser paginates and filters the
// complete list client-side rather than truncating to one page.
func metricNames(ctx context.Context, s *Session) ([]string, error) {
	var names []string
	if err := s.client.api(ctx, http.MethodGet, "/api/v1/label/__name__/values", nil, nil, &names); err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

func metadata(ctx context.Context, s *Session, metric string) (map[string][]row, error) {
	q := url.Values{}
	if metric != "" {
		q.Set("metric", metric)
	}
	var out map[string][]row
	err := s.client.api(ctx, http.MethodGet, "/api/v1/metadata", q, nil, &out)
	return out, err
}

func findByUID(rc *plugin.RequestContext, load func(context.Context, *Session) ([]row, error), uid, kind string) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	rows, err := load(rc.Ctx, s)
	if err != nil {
		return nil, err
	}
	for _, item := range rows {
		if fmt.Sprint(item["uid"]) == uid {
			return item, nil
		}
	}
	return nil, fmt.Errorf("%w: %s %q", plugin.ErrNotFound, kind, uid)
}

func findTarget(rc *plugin.RequestContext) (row, error) {
	v, err := findByUID(rc, targets, targetParam(rc), "target")
	if err != nil {
		return nil, err
	}
	return v.(row), nil
}

func instantValue(ctx context.Context, s *Session, query string) float64 {
	res, err := executeQuery(ctx, s, query)
	if err != nil || len(res.Rows) == 0 || len(res.Rows[0]) < 3 {
		return 0
	}
	switch v := res.Rows[0][2].(type) {
	case float64:
		return v
	case string:
		f, _ := strconv.ParseFloat(v, 64)
		return f
	default:
		return 0
	}
}

func limitFromRequest(rc *plugin.RequestContext, s *Session) int {
	req, err := rc.Page()
	if err != nil {
		return s.opts.PageLimit
	}
	if req.Limit > s.opts.PageLimit {
		return s.opts.PageLimit
	}
	return req.Limit
}

func targetIcon(item row) plugin.Icon {
	if fmt.Sprint(item["health"]) == "up" {
		return icon("circle-check")
	}
	return icon("circle-alert")
}

func labelsSelector(labels map[string]any) string {
	if len(labels) == 0 {
		return ""
	}
	parts := make([]string, 0, len(labels))
	for k, v := range labels {
		parts = append(parts, k+`="`+strings.ReplaceAll(fmt.Sprint(v), `"`, `\"`)+`"`)
	}
	sort.Strings(parts)
	return "{" + strings.Join(parts, ",") + "}"
}

func metricName(labels map[string]string) string {
	if name := labels["__name__"]; name != "" {
		return name
	}
	return "{}"
}

func sampleValue(value []any) (float64, float64) {
	if len(value) != 2 {
		return 0, 0
	}
	ts := number(value[0])
	val, _ := strconv.ParseFloat(fmt.Sprint(value[1]), 64)
	if math.IsNaN(val) || math.IsInf(val, 0) {
		val = 0
	}
	return ts, val
}

func number(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case int:
		return float64(t)
	case string:
		f, _ := strconv.ParseFloat(t, 64)
		return f
	default:
		return 0
	}
}

func pct(value, total float64) float64 {
	if total <= 0 {
		return 0
	}
	return math.Round(value/total*1000) / 10
}

func parseQueryTime(raw string, now time.Time) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "now" {
		return now, nil
	}
	if strings.HasPrefix(raw, "-") {
		d, err := time.ParseDuration(strings.TrimPrefix(raw, "-"))
		if err != nil {
			return time.Time{}, err
		}
		return now.Add(-d), nil
	}
	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		sec, dec := math.Modf(f)
		return time.Unix(int64(sec), int64(dec*1e9)).UTC(), nil
	}
	return time.Parse(time.RFC3339, raw)
}

func formatQueryTime(t time.Time) string {
	return strconv.FormatFloat(float64(t.UnixNano())/1e9, 'f', 3, 64)
}

func ptr(parent row, key string) *map[string]any {
	child := map[string]any{}
	parent[key] = child
	return &child
}

func cloneRows(in []row) []row {
	out := make([]row, 0, len(in))
	for _, item := range in {
		next := row{}
		for k, v := range item {
			next[k] = v
		}
		out = append(out, next)
	}
	return out
}

func statusParam(rc *plugin.RequestContext) string {
	if v := strings.TrimSpace(rc.Param("status")); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p.status"))
}

func targetParam(rc *plugin.RequestContext) string {
	if v := strings.TrimSpace(rc.Param("target")); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p.target"))
}

func alertParam(rc *plugin.RequestContext) string {
	if v := strings.TrimSpace(rc.Param("alert")); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p.alert"))
}

func ruleParam(rc *plugin.RequestContext) string {
	if v := strings.TrimSpace(rc.Param("rule")); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p.rule"))
}

func metricParam(rc *plugin.RequestContext) string {
	if v := strings.TrimSpace(rc.Param("metric")); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p.metric"))
}

func labelParam(rc *plugin.RequestContext) string {
	if v := strings.TrimSpace(rc.Param("label")); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p.label"))
}

func auditResult(err error) plugin.AuditResult {
	if err != nil {
		return plugin.AuditError
	}
	return plugin.AuditAllowed
}
