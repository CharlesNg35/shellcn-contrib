package loki

import (
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

type envelope struct {
	Status string          `json:"status"`
	Data   json.RawMessage `json:"data"`
	Error  string          `json:"error"`
}

func Routes() []plugin.Route {
	return []plugin.Route{
		{ID: rid("overview"), Method: plugin.MethodGet, Path: "/overview", Permission: "loki.read", Risk: plugin.RiskSafe, AuditEvent: rid("overview"), Handle: overview},
		{ID: rid("labels.tree"), Method: plugin.MethodGet, Path: "/tree/labels", Permission: "loki.labels.read", Risk: plugin.RiskSafe, AuditEvent: rid("labels.tree"), Handle: treeLabels},
		{ID: rid("labels.list"), Method: plugin.MethodGet, Path: "/labels", Permission: "loki.labels.read", Risk: plugin.RiskSafe, AuditEvent: rid("labels.list"), Handle: listLabels},
		{ID: rid("label.values"), Method: plugin.MethodGet, Path: "/labels/{label}/values", Permission: "loki.labels.read", Risk: plugin.RiskSafe, AuditEvent: rid("label.values"), Handle: labelValues},
		{ID: rid("streams.list"), Method: plugin.MethodGet, Path: "/streams", Permission: "loki.streams.read", Risk: plugin.RiskSafe, AuditEvent: rid("streams.list"), Handle: listStreams},
		{ID: rid("stream.logs"), Method: plugin.MethodGet, Path: "/streams/{stream}/logs", Permission: "loki.logs.read", Risk: plugin.RiskSafe, AuditEvent: rid("stream.logs"), Handle: streamLogs},
		{ID: rid("stats.read"), Method: plugin.MethodGet, Path: "/stats", Permission: "loki.stats.read", Risk: plugin.RiskSafe, AuditEvent: rid("stats.read"), Handle: readStats},
		{ID: rid("volume.list"), Method: plugin.MethodGet, Path: "/volume", Permission: "loki.volume.read", Risk: plugin.RiskSafe, AuditEvent: rid("volume.list"), Handle: listVolume},
		{ID: rid("rules.list"), Method: plugin.MethodGet, Path: "/rules", Permission: "loki.rules.read", Risk: plugin.RiskSafe, AuditEvent: rid("rules.list"), Handle: listRules},
		{ID: rid("query.format"), Method: plugin.MethodPost, Path: "/query/format", Permission: "loki.query.execute", Risk: plugin.RiskSafe, AuditEvent: rid("query.format"), Input: formatQuerySchema(), Handle: formatQuery},
		{ID: rid("deletes.list"), Method: plugin.MethodGet, Path: "/deletes", Permission: "loki.deletes.read", Risk: plugin.RiskSafe, AuditEvent: rid("deletes.list"), Handle: listDeletes},
		{ID: rid("delete.create"), Method: plugin.MethodPost, Path: "/deletes", Permission: "loki.deletes.write", Risk: plugin.RiskDestructive, AuditEvent: rid("delete.create"), Input: deleteCreateSchema(), Handle: createDelete},
		{ID: rid("delete.cancel"), Method: plugin.MethodDelete, Path: "/deletes/{request}", Permission: "loki.deletes.write", Risk: plugin.RiskDestructive, AuditEvent: rid("delete.cancel"), Input: deleteCancelSchema(), Handle: cancelDelete},
		{ID: rid("query"), Method: plugin.MethodWS, Path: "/query", Permission: "loki.query.execute", Risk: plugin.RiskSafe, AuditEvent: rid("query"), Stream: queryStream},
	}
}

func formatQuerySchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Format LogQL", Fields: []plugin.Field{
		{Key: "query", Label: "Query", Type: plugin.FieldTextarea, Required: true, Default: `{job=~".+"}`},
	}}}}
}

func deleteCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Delete logs", Fields: []plugin.Field{
		{Key: "query", Label: "LogQL stream selector", Type: plugin.FieldText, Required: true, Default: `{job=~".+"}`},
		{Key: "start", Label: "Start", Type: plugin.FieldText, Required: true, Placeholder: "2026-06-05T00:00:00Z"},
		{Key: "end", Label: "End", Type: plugin.FieldText, Required: true, Placeholder: "2026-06-05T01:00:00Z"},
	}}}}
}

func deleteCancelSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Cancel delete", Fields: []plugin.Field{
		{Key: "force", Label: "Force", Type: plugin.FieldToggle},
	}}}}
}

func session(rc *plugin.RequestContext) (*Session, error) { return unwrap(rc.Session) }

func lokiAPI(rc *plugin.RequestContext, method, path string, query url.Values, out any) error {
	s, err := session(rc)
	if err != nil {
		return err
	}
	data, err := s.client.Raw(rc.Ctx, method, path, query, "", nil)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	var env envelope
	if err := json.Unmarshal(data, &env); err != nil {
		if out == nil {
			return nil
		}
		return json.Unmarshal(data, out)
	}
	if env.Status == "error" {
		return fmt.Errorf("%w: %s", plugin.ErrInvalidInput, env.Error)
	}
	if out == nil || len(env.Data) == 0 {
		return nil
	}
	return json.Unmarshal(env.Data, out)
}

func overview(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	out := row{}
	var build any
	if err := lokiAPI(rc, http.MethodGet, "/loki/api/v1/status/buildinfo", nil, &build); err == nil {
		out["build"] = build
	}
	if cfg, err := s.client.Raw(rc.Ctx, http.MethodGet, "/config", nil, "", nil); err == nil {
		out["config"] = string(cfg)
	}
	return out, nil
}

func treeLabels(rc *plugin.RequestContext) (any, error) {
	res, err := listLabels(rc)
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

func listLabels(rc *plugin.RequestContext) (any, error) {
	var labels []string
	if err := lokiAPI(rc, http.MethodGet, "/loki/api/v1/labels", rangeQuery(rc, ""), &labels); err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(labels))
	for _, label := range labels {
		rows = append(rows, row{"name": label, "ref": plugin.ResourceRef{Kind: "label", Name: label, UID: label}})
	}
	return broker.PageRows(rc, rows)
}

func labelValues(rc *plugin.RequestContext) (any, error) {
	var values []string
	if err := lokiAPI(rc, http.MethodGet, "/loki/api/v1/label/"+url.PathEscape(labelParam(rc))+"/values", rangeQuery(rc, ""), &values); err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(values))
	for _, value := range values {
		rows = append(rows, row{"value": value})
	}
	return broker.PageRows(rc, rows)
}

func listStreams(rc *plugin.RequestContext) (any, error) {
	var streams []map[string]string
	if err := lokiAPI(rc, http.MethodGet, "/loki/api/v1/series", rangeQuery(rc, `{job=~".+"}`), &streams); err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(streams))
	for _, labels := range streams {
		name := labelsToSelector(labels)
		rows = append(rows, row{"name": name, "labels": labels, "ref": plugin.ResourceRef{Kind: "stream", Name: name, UID: name}})
	}
	return broker.PageRows(rc, rows)
}

func streamLogs(rc *plugin.RequestContext) (any, error) {
	return queryRangeRows(rc, streamParam(rc))
}

func readStats(rc *plugin.RequestContext) (any, error) {
	var out any
	if err := lokiAPI(rc, http.MethodGet, "/loki/api/v1/index/stats", rangeQuery(rc, selectorQuery(rc)), &out); err != nil {
		if optionalLokiFeatureUnavailable(err) {
			return row{}, nil
		}
		return nil, err
	}
	if out == nil {
		return row{}, nil
	}
	return out, nil
}

func listVolume(rc *plugin.RequestContext) (any, error) {
	q := rangeQuery(rc, selectorQuery(rc))
	if targetLabels := strings.TrimSpace(rc.Query().Get("targetLabels")); targetLabels != "" {
		q.Set("targetLabels", targetLabels)
	}
	if aggregateBy := strings.TrimSpace(rc.Query().Get("aggregateBy")); aggregateBy != "" {
		q.Set("aggregateBy", aggregateBy)
	}
	var data struct {
		Result []struct {
			Metric map[string]string `json:"metric"`
			Value  []any             `json:"value"`
		} `json:"result"`
	}
	if err := lokiAPI(rc, http.MethodGet, "/loki/api/v1/index/volume", q, &data); err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(data.Result))
	for _, item := range data.Result {
		rows = append(rows, row{"metric": item.Metric, "value": sampleValue(item.Value)})
	}
	return broker.PageRows(rc, rows)
}

func listRules(rc *plugin.RequestContext) (any, error) {
	var data any
	if err := lokiAPI(rc, http.MethodGet, "/loki/api/v1/rules", nil, &data); err != nil {
		if optionalLokiFeatureUnavailable(err) {
			return broker.PageRows(rc, []row{})
		}
		return nil, err
	}
	return broker.PageRows(rc, flattenRules(data))
}

func formatQuery(rc *plugin.RequestContext) (any, error) {
	var req struct {
		Query string `json:"query" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	var out string
	if err := lokiAPI(rc, http.MethodGet, "/loki/api/v1/format_query", url.Values{"query": []string{req.Query}}, &out); err != nil {
		return nil, err
	}
	return row{"query": out}, nil
}

func listDeletes(rc *plugin.RequestContext) (any, error) {
	var data any
	if err := lokiAPI(rc, http.MethodGet, "/loki/api/v1/delete", rangeBounds(rc), &data); err != nil {
		if optionalLokiFeatureUnavailable(err) {
			return broker.PageRows(rc, []row{})
		}
		return nil, err
	}
	rows := normalizeRows(data)
	for _, item := range rows {
		id := firstString(item, "request_id", "requestID", "id")
		if id == "" {
			id = stableRowID(item)
		}
		item["request_id"] = id
	}
	return broker.PageRows(rc, rows)
}

func createDelete(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Query string `json:"query" validate:"required"`
		Start string `json:"start" validate:"required"`
		End   string `json:"end" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	q := url.Values{"query": []string{req.Query}, "start": []string{req.Start}, "end": []string{req.End}}
	data, err := s.client.Raw(rc.Ctx, http.MethodPost, "/loki/api/v1/delete", q, "", nil)
	return row{"ok": err == nil, "response": string(data)}, err
}

func cancelDelete(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	q := url.Values{"request_id": []string{deleteRequestParam(rc)}}
	var req struct {
		Force bool `json:"force"`
	}
	if len(rc.Body()) > 0 {
		if err := rc.Bind(&req); err != nil {
			return nil, err
		}
	}
	if req.Force {
		q.Set("force", "true")
	}
	data, err := s.client.Raw(rc.Ctx, http.MethodDelete, "/loki/api/v1/delete", q, "", nil)
	return row{"ok": err == nil, "response": string(data)}, err
}

func queryStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	dec := json.NewDecoder(client)
	enc := json.NewEncoder(client)
	for {
		var raw any
		if err := dec.Decode(&raw); err != nil {
			if client.Context().Err() != nil || errors.Is(err, io.EOF) {
				return nil
			}
			if err := enc.Encode(row{"error": "Invalid query request."}); err != nil {
				return err
			}
			continue
		}
		query, q := queryInput(raw)
		out, err := queryRangeRowsWith(rc, query, q)
		if err != nil {
			if encErr := enc.Encode(row{"error": err.Error()}); encErr != nil {
				return encErr
			}
			continue
		}
		if err := enc.Encode(out); err != nil {
			return err
		}
	}
}

func queryRangeRows(rc *plugin.RequestContext, query string) (any, error) {
	return queryRangeRowsWith(rc, query, rangeQuery(rc, query))
}

func queryRangeRowsWith(rc *plugin.RequestContext, query string, q url.Values) (any, error) {
	if strings.TrimSpace(query) == "" {
		return nil, fmt.Errorf("%w: LogQL query is required", plugin.ErrInvalidInput)
	}
	q.Set("query", query)
	var data struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Stream map[string]string `json:"stream"`
			Values [][]string        `json:"values"`
		} `json:"result"`
	}
	if err := lokiAPI(rc, http.MethodGet, "/loki/api/v1/query_range", q, &data); err != nil {
		return nil, err
	}
	rows := []row{}
	for _, stream := range data.Result {
		for _, value := range stream.Values {
			if len(value) < 2 {
				continue
			}
			rows = append(rows, row{"timestamp": parseLokiTime(value[0]), "line": value[1], "labels": stream.Stream})
		}
	}
	return broker.PageRows(rc, rows)
}

func queryInput(raw any) (string, url.Values) {
	q := url.Values{}
	switch v := raw.(type) {
	case string:
		return v, defaultRange()
	case map[string]any:
		query := strings.TrimSpace(fmt.Sprint(v["query"]))
		if query == "" {
			query = strings.TrimSpace(fmt.Sprint(v["q"]))
		}
		q = defaultRange()
		for _, key := range []string{"since", "start", "end", "direction", "limit"} {
			if value := strings.TrimSpace(fmt.Sprint(v[key])); value != "" && value != "<nil>" {
				q.Set(key, value)
			}
		}
		return query, q
	default:
		return fmt.Sprint(v), defaultRange()
	}
}

func rangeQuery(rc *plugin.RequestContext, query string) url.Values {
	q := defaultRange()
	if search := strings.TrimSpace(rc.Query().Get("query")); search != "" {
		q.Set("query", search)
	} else if query != "" {
		q.Set("query", query)
	}
	if limit := rc.Query().Get("limit"); limit != "" {
		q.Set("limit", limit)
	}
	return q
}

func defaultRange() url.Values {
	return url.Values{"since": []string{"1h"}, "limit": []string{strconv.Itoa(defaultPageLimit)}, "direction": []string{"backward"}}
}

func selectorQuery(rc *plugin.RequestContext) string {
	if query := strings.TrimSpace(rc.Query().Get("query")); query != "" {
		return query
	}
	return `{job=~".+"}`
}

func rangeBounds(rc *plugin.RequestContext) url.Values {
	q := url.Values{}
	for _, key := range []string{"start", "end"} {
		if v := strings.TrimSpace(rc.Query().Get(key)); v != "" {
			q.Set(key, v)
		}
	}
	return q
}

func parseLokiTime(raw string) string {
	nsec, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return raw
	}
	return time.Unix(0, nsec).UTC().Format(time.RFC3339Nano)
}

func labelsToSelector(labels map[string]string) string {
	parts := make([]string, 0, len(labels))
	for k, v := range labels {
		parts = append(parts, k+`="`+strings.ReplaceAll(v, `"`, `\"`)+`"`)
	}
	sort.Strings(parts)
	return "{" + strings.Join(parts, ",") + "}"
}

func flattenRules(data any) []row {
	root, ok := data.(map[string]any)
	if !ok {
		return normalizeRows(data)
	}
	groups := normalizeRows(root["groups"])
	if len(groups) == 0 {
		return normalizeRows(data)
	}
	rows := []row{}
	for _, group := range groups {
		namespace := firstString(group, "namespace", "file", "file_path")
		groupName := firstString(group, "name")
		for _, rule := range normalizeRows(group["rules"]) {
			item := row{
				"namespace": namespace,
				"group":     groupName,
				"name":      firstString(rule, "name", "alert", "record"),
				"type":      ruleType(rule),
				"query":     firstString(rule, "query", "expr"),
			}
			for k, v := range rule {
				if _, exists := item[k]; !exists {
					item[k] = v
				}
			}
			rows = append(rows, item)
		}
	}
	return rows
}

func normalizeRows(data any) []row {
	switch v := data.(type) {
	case []row:
		return v
	case []map[string]any:
		rows := make([]row, 0, len(v))
		for _, item := range v {
			rows = append(rows, row(item))
		}
		return rows
	case []any:
		rows := make([]row, 0, len(v))
		for _, item := range v {
			if m, ok := item.(map[string]any); ok {
				rows = append(rows, row(m))
			}
		}
		return rows
	case map[string]any:
		return []row{row(v)}
	default:
		return nil
	}
}

func ruleType(rule row) string {
	if firstString(rule, "alert") != "" {
		return "alert"
	}
	if firstString(rule, "record") != "" {
		return "record"
	}
	return firstString(rule, "type")
}

func sampleValue(values []any) float64 {
	if len(values) < 2 {
		return 0
	}
	switch v := values[1].(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case string:
		f, _ := strconv.ParseFloat(v, 64)
		return f
	default:
		f, _ := strconv.ParseFloat(fmt.Sprint(v), 64)
		return f
	}
}

func firstString(item row, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(fmt.Sprint(item[key])); value != "" && value != "<nil>" {
			return value
		}
	}
	return ""
}

func stableRowID(item row) string {
	keys := make([]string, 0, len(item))
	for key := range item {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+fmt.Sprint(item[key]))
	}
	return strings.Join(parts, "|")
}

func optionalLokiFeatureUnavailable(err error) bool {
	if errors.Is(err, plugin.ErrNotFound) {
		return true
	}
	text := strings.ToLower(err.Error())
	for _, marker := range []string{
		"404 page not found",
		"unable to read rule dir",
		"ruler api is not enabled",
		"deletion mode is disabled",
	} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func ensureWritable(s *Session) error {
	if s.opts.ReadOnly {
		return fmt.Errorf("%w: connection is read-only", plugin.ErrForbidden)
	}
	return nil
}

func labelParam(rc *plugin.RequestContext) string         { return rc.Param("label") }
func streamParam(rc *plugin.RequestContext) string        { return rc.Param("stream") }
func deleteRequestParam(rc *plugin.RequestContext) string { return rc.Param("request") }
