package jaeger

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

type servicesResponse struct {
	Services []string `json:"services"`
}

type operationsResponse struct {
	Operations []row `json:"operations"`
}

type tracesResponse struct {
	Result row `json:"result"`
}

func Routes() []plugin.Route {
	return []plugin.Route{
		{ID: rid("overview"), Method: plugin.MethodGet, Path: "/overview", Permission: "jaeger.read", Risk: plugin.RiskSafe, AuditEvent: rid("overview"), Handle: overview},
		{ID: rid("services.tree"), Method: plugin.MethodGet, Path: "/tree/services", Permission: "jaeger.services.read", Risk: plugin.RiskSafe, AuditEvent: rid("services.tree"), Handle: treeServices},
		{ID: rid("services.list"), Method: plugin.MethodGet, Path: "/services", Permission: "jaeger.services.read", Risk: plugin.RiskSafe, AuditEvent: rid("services.list"), Handle: listServices},
		{ID: rid("operations.list"), Method: plugin.MethodGet, Path: "/services/{service}/operations", Permission: "jaeger.operations.read", Risk: plugin.RiskSafe, AuditEvent: rid("operations.list"), Handle: listOperations},
		{ID: rid("traces.list"), Method: plugin.MethodGet, Path: "/services/{service}/traces", Permission: "jaeger.traces.read", Risk: plugin.RiskSafe, AuditEvent: rid("traces.list"), Handle: listTraces},
		{ID: rid("trace.read"), Method: plugin.MethodGet, Path: "/traces/{trace}", Permission: "jaeger.traces.read", Risk: plugin.RiskSafe, AuditEvent: rid("trace.read"), Handle: readTrace},
		{ID: rid("spans.list"), Method: plugin.MethodGet, Path: "/traces/{trace}/spans", Permission: "jaeger.traces.read", Risk: plugin.RiskSafe, AuditEvent: rid("spans.list"), Handle: listSpans},
	}
}

func session(rc *plugin.RequestContext) (*Session, error) { return unwrap(rc.Session) }

func overview(rc *plugin.RequestContext) (any, error) {
	services, err := fetchServices(rc)
	if err != nil {
		return nil, err
	}
	return row{"services": len(services)}, nil
}

func treeServices(rc *plugin.RequestContext) (any, error) {
	res, err := listServices(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name := fmt.Sprint(item["name"])
		ref := plugin.ResourceIdentity{Kind: "service", Name: name, UID: name}
		nodes = append(nodes, plugin.TreeNode{Key: "service:" + name, Label: name, Icon: icon("box"), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, Total: page.Total}, nil
}

func listServices(rc *plugin.RequestContext) (any, error) {
	services, err := fetchServices(rc)
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(services))
	for _, service := range services {
		rows = append(rows, row{"name": service, "ref": plugin.ResourceIdentity{Kind: "service", Name: service, UID: service}})
	}
	return broker.PageRows(rc, rows)
}

func listOperations(rc *plugin.RequestContext) (any, error) {
	q := url.Values{"service": []string{serviceParam(rc)}}
	var resp operationsResponse
	if err := jaegerGet(rc, "/api/v3/operations", q, &resp); err != nil {
		return nil, err
	}
	return broker.PageRows(rc, resp.Operations)
}

func listTraces(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	limit := s.opts.PageLimit
	if qLimit, err := strconv.Atoi(rc.Query().Get("limit")); err == nil && qLimit > 0 && qLimit < limit {
		limit = qLimit
	}
	start, end, err := traceWindow(rc)
	if err != nil {
		return nil, err
	}
	q := url.Values{
		"query.service_name":   []string{serviceParam(rc)},
		"query.start_time_min": []string{start.Format(time.RFC3339Nano)},
		"query.start_time_max": []string{end.Format(time.RFC3339Nano)},
		"query.num_traces":     []string{strconv.Itoa(limit)},
		"query.raw_traces":     []string{"false"},
	}
	if op := strings.TrimSpace(rc.Query().Get("operation")); op != "" {
		q.Set("query.operation_name", op)
	}
	var resp tracesResponse
	if err := jaegerGet(rc, "/api/v3/traces", q, &resp); err != nil {
		return nil, err
	}
	return broker.PageRows(rc, traceRows(resp.Result))
}

func readTrace(rc *plugin.RequestContext) (any, error) {
	var resp tracesResponse
	if err := jaegerGet(rc, "/api/v3/traces/"+url.PathEscape(traceParam(rc)), nil, &resp); err != nil {
		return nil, err
	}
	rows := traceRows(resp.Result)
	if len(rows) == 0 {
		return nil, fmt.Errorf("%w: trace %q", plugin.ErrNotFound, traceParam(rc))
	}
	return rows[0], nil
}

func listSpans(rc *plugin.RequestContext) (any, error) {
	trace, err := readTrace(rc)
	if err != nil {
		return nil, err
	}
	rows := flattenSpans(trace.(row))
	return broker.PageRows(rc, rows)
}

func fetchServices(rc *plugin.RequestContext) ([]string, error) {
	var resp servicesResponse
	if err := jaegerGet(rc, "/api/v3/services", nil, &resp); err != nil {
		return nil, err
	}
	return resp.Services, nil
}

func jaegerGet(rc *plugin.RequestContext, path string, q url.Values, out any) error {
	s, err := session(rc)
	if err != nil {
		return err
	}
	return s.client.Do(rc.Ctx, http.MethodGet, path, q, nil, out)
}

func traceRows(result row) []row {
	resourceSpans := asRows(result["resourceSpans"])
	out := make([]row, 0, len(resourceSpans))
	for _, resourceSpan := range resourceSpans {
		service := resourceServiceName(resourceSpan)
		spans := spansForResource(resourceSpan, service)
		if len(spans) == 0 {
			continue
		}
		first := spans[0]
		id := stringValue(first["traceID"])
		out = append(out, row{
			"traceID":       id,
			"operationName": first["operationName"],
			"serviceName":   first["serviceName"],
			"duration":      first["duration"],
			"startTime":     first["startTime"],
			"spans":         len(spans),
			"resourceSpans": []row{resourceSpan},
			"ref":           plugin.ResourceIdentity{Kind: "trace", Name: id, UID: id},
		})
	}
	return out
}

func flattenSpans(trace row) []row {
	resourceSpans := asRows(trace["resourceSpans"])
	out := []row{}
	for _, resourceSpan := range resourceSpans {
		service := resourceServiceName(resourceSpan)
		for _, span := range spansForResource(resourceSpan, service) {
			out = append(out, row{
				"spanID":        span["spanID"],
				"operationName": span["operationName"],
				"serviceName":   span["serviceName"],
				"duration":      span["duration"],
				"startTime":     span["startTime"],
				"tags":          span["tags"],
			})
		}
	}
	return out
}

func spansForResource(resourceSpan row, service string) []row {
	scopeSpans := asRows(resourceSpan["scopeSpans"])
	out := []row{}
	for _, scopeSpan := range scopeSpans {
		for _, span := range asRows(scopeSpan["spans"]) {
			out = append(out, normalizeSpan(span, service))
		}
	}
	return out
}

func normalizeSpan(span row, service string) row {
	start := unixNano(span["startTimeUnixNano"])
	end := unixNano(span["endTimeUnixNano"])
	duration := ""
	if start > 0 && end > start {
		duration = time.Duration(end - start).String()
	}
	return row{
		"traceID":       stringValue(span["traceID"], span["traceId"]),
		"spanID":        stringValue(span["spanID"], span["spanId"]),
		"operationName": stringValue(span["operationName"], span["name"]),
		"serviceName":   service,
		"duration":      duration,
		"startTime":     unixNanoTime(start),
		"tags":          attributesMap(span["attributes"]),
	}
}

func resourceServiceName(resourceSpan row) string {
	resource, _ := resourceSpan["resource"].(map[string]any)
	return stringValue(attributesMap(resource["attributes"])["service.name"])
}

func asRows(v any) []row {
	switch items := v.(type) {
	case []row:
		return items
	case []any:
		rows := make([]row, 0, len(items))
		for _, item := range items {
			if m, ok := item.(map[string]any); ok {
				rows = append(rows, row(m))
			}
		}
		return rows
	default:
		return nil
	}
}

func attributesMap(v any) map[string]any {
	out := map[string]any{}
	for _, item := range asRows(v) {
		key := stringValue(item["key"])
		if key == "" {
			continue
		}
		out[key] = attributeValue(item["value"])
	}
	return out
}

func attributeValue(v any) any {
	value, _ := v.(map[string]any)
	for _, key := range []string{"stringValue", "intValue", "doubleValue", "boolValue"} {
		if raw, ok := value[key]; ok {
			return raw
		}
	}
	return v
}

func stringValue(values ...any) string {
	for _, v := range values {
		s := strings.TrimSpace(fmt.Sprint(v))
		if s != "" && s != "<nil>" {
			return s
		}
	}
	return ""
}

func unixNano(v any) int64 {
	switch t := v.(type) {
	case string:
		n, _ := strconv.ParseInt(t, 10, 64)
		return n
	case float64:
		return int64(t)
	case int64:
		return t
	case int:
		return int64(t)
	default:
		return 0
	}
}

func unixNanoTime(n int64) string {
	if n <= 0 {
		return ""
	}
	return time.Unix(0, n).UTC().Format(time.RFC3339Nano)
}

func traceWindow(rc *plugin.RequestContext) (time.Time, time.Time, error) {
	end := time.Now().UTC()
	if raw := strings.TrimSpace(rc.Query().Get("end")); raw != "" {
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("%w: end must be RFC3339", plugin.ErrInvalidInput)
		}
		end = parsed.UTC()
	}
	lookback, err := time.ParseDuration(defaultLookback(rc))
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: lookback must be a duration", plugin.ErrInvalidInput)
	}
	return end.Add(-lookback), end, nil
}

func defaultLookback(rc *plugin.RequestContext) string {
	if v := strings.TrimSpace(rc.Query().Get("lookback")); v != "" {
		return v
	}
	return "1h"
}

func serviceParam(rc *plugin.RequestContext) string { return rc.Param("service") }
func traceParam(rc *plugin.RequestContext) string   { return rc.Param("trace") }
