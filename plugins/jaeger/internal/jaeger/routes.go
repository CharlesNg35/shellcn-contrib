package jaeger

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

type response struct {
	Data any `json:"data"`
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
	var services []string
	if err := jaegerData(rc, "/api/services", nil, &services); err != nil {
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
		ref := plugin.ResourceRef{Kind: "service", Name: name, UID: name}
		nodes = append(nodes, plugin.TreeNode{Key: "service:" + name, Label: name, Icon: icon("box"), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, Total: page.Total}, nil
}

func listServices(rc *plugin.RequestContext) (any, error) {
	var services []string
	if err := jaegerData(rc, "/api/services", nil, &services); err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(services))
	for _, service := range services {
		rows = append(rows, row{"name": service, "ref": plugin.ResourceRef{Kind: "service", Name: service, UID: service}})
	}
	return broker.PageRows(rc, rows)
}

func listOperations(rc *plugin.RequestContext) (any, error) {
	q := url.Values{"service": []string{serviceParam(rc)}}
	var operations []row
	if err := jaegerData(rc, "/api/operations", q, &operations); err != nil {
		return nil, err
	}
	for _, item := range operations {
		if item["name"] == nil {
			item["name"] = item["operationName"]
		}
	}
	return broker.PageRows(rc, operations)
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
	q := url.Values{
		"service":  []string{serviceParam(rc)},
		"limit":    []string{strconv.Itoa(limit)},
		"lookback": []string{defaultLookback(rc)},
	}
	if op := strings.TrimSpace(rc.Query().Get("operation")); op != "" {
		q.Set("operation", op)
	}
	var traces []row
	if err := jaegerData(rc, "/api/traces", q, &traces); err != nil {
		return nil, err
	}
	rows := flattenTraces(traces)
	return broker.PageRows(rc, rows)
}

func readTrace(rc *plugin.RequestContext) (any, error) {
	var traces []row
	if err := jaegerData(rc, "/api/traces/"+url.PathEscape(traceParam(rc)), nil, &traces); err != nil {
		return nil, err
	}
	if len(traces) == 0 {
		return nil, fmt.Errorf("%w: trace %q", plugin.ErrNotFound, traceParam(rc))
	}
	return traces[0], nil
}

func listSpans(rc *plugin.RequestContext) (any, error) {
	trace, err := readTrace(rc)
	if err != nil {
		return nil, err
	}
	rows := flattenSpans(trace.(row))
	return broker.PageRows(rc, rows)
}

func jaegerData(rc *plugin.RequestContext, path string, q url.Values, out any) error {
	s, err := session(rc)
	if err != nil {
		return err
	}
	var resp response
	if err := s.client.Do(rc.Ctx, http.MethodGet, path, q, nil, &resp); err != nil {
		return err
	}
	data, err := json.Marshal(resp.Data)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}

func flattenTraces(traces []row) []row {
	rows := make([]row, 0, len(traces))
	for _, trace := range traces {
		spans := asRows(trace["spans"])
		first := row{}
		if len(spans) > 0 {
			first = spans[0]
		}
		processes := asProcessMap(trace["processes"])
		service := serviceName(first, processes)
		id := fmt.Sprint(trace["traceID"])
		rows = append(rows, row{
			"traceID":       id,
			"operationName": first["operationName"],
			"serviceName":   service,
			"duration":      first["duration"],
			"startTime":     unixMicro(first["startTime"]),
			"spans":         len(spans),
			"ref":           plugin.ResourceRef{Kind: "trace", Name: id, UID: id},
		})
	}
	return rows
}

func flattenSpans(trace row) []row {
	processes := asProcessMap(trace["processes"])
	spans := asRows(trace["spans"])
	rows := make([]row, 0, len(spans))
	for _, span := range spans {
		rows = append(rows, row{
			"spanID":        span["spanID"],
			"operationName": span["operationName"],
			"serviceName":   serviceName(span, processes),
			"duration":      span["duration"],
			"startTime":     unixMicro(span["startTime"]),
			"tags":          span["tags"],
		})
	}
	return rows
}

func serviceName(span row, processes map[string]row) string {
	processID := fmt.Sprint(span["processID"])
	process := processes[processID]
	if service := strings.TrimSpace(fmt.Sprint(process["serviceName"])); service != "" && service != "<nil>" {
		return service
	}
	return ""
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

func asProcessMap(v any) map[string]row {
	out := map[string]row{}
	if raw, ok := v.(map[string]any); ok {
		for key, value := range raw {
			if m, ok := value.(map[string]any); ok {
				out[key] = row(m)
			}
		}
	}
	return out
}

func unixMicro(v any) string {
	var micros int64
	switch t := v.(type) {
	case float64:
		micros = int64(t)
	case int64:
		micros = t
	case int:
		micros = int64(t)
	default:
		return fmt.Sprint(v)
	}
	return time.UnixMicro(micros).UTC().Format(time.RFC3339Nano)
}

func defaultLookback(rc *plugin.RequestContext) string {
	if v := strings.TrimSpace(rc.Query().Get("lookback")); v != "" {
		return v
	}
	return "1h"
}

func serviceParam(rc *plugin.RequestContext) string { return rc.Param("service") }
func traceParam(rc *plugin.RequestContext) string   { return rc.Param("trace") }
