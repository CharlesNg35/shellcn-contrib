package influxdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

func routes() []plugin.Route {
	return []plugin.Route{
		{ID: rid("status.tree"), Method: plugin.MethodGet, Path: "/tree/status", Permission: "influxdb.status.read", Risk: plugin.RiskSafe, AuditEvent: rid("status.tree"), Handle: statusTree},
		{ID: rid("status.list"), Method: plugin.MethodGet, Path: "/status", Permission: "influxdb.status.read", Risk: plugin.RiskSafe, AuditEvent: rid("status.list"), Handle: statusList},
		{ID: rid("status.read"), Method: plugin.MethodGet, Path: "/status/{status}", Permission: "influxdb.status.read", Risk: plugin.RiskSafe, AuditEvent: rid("status.read"), Handle: statusRead},
		{ID: rid("namespaces.tree"), Method: plugin.MethodGet, Path: "/tree/namespaces", Permission: "influxdb.namespaces.read", Risk: plugin.RiskSafe, AuditEvent: rid("namespaces.tree"), Handle: namespaceTree},
		{ID: rid("namespaces.list"), Method: plugin.MethodGet, Path: "/namespaces", Permission: "influxdb.namespaces.read", Risk: plugin.RiskSafe, AuditEvent: rid("namespaces.list"), Handle: namespaceList},
		{ID: rid("namespace.read"), Method: plugin.MethodGet, Path: "/namespaces/{namespace}", Permission: "influxdb.namespaces.read", Risk: plugin.RiskSafe, AuditEvent: rid("namespace.read"), Handle: namespaceRead},
		{ID: rid("measurements.tree"), Method: plugin.MethodGet, Path: "/tree/measurements", Permission: "influxdb.measurements.read", Risk: plugin.RiskSafe, AuditEvent: rid("measurements.tree"), Handle: measurementTree},
		{ID: rid("measurements.list"), Method: plugin.MethodGet, Path: "/measurements", Permission: "influxdb.measurements.read", Risk: plugin.RiskSafe, AuditEvent: rid("measurements.list"), Handle: measurementList},
		{ID: rid("measurement.rows"), Method: plugin.MethodGet, Path: "/measurements/{namespace}/{measurement}/rows", Permission: "influxdb.measurements.data.read", Risk: plugin.RiskSafe, AuditEvent: rid("measurement.rows"), Handle: measurementRows},
		{ID: rid("measurement.fields"), Method: plugin.MethodGet, Path: "/measurements/{namespace}/{measurement}/fields", Permission: "influxdb.measurements.read", Risk: plugin.RiskSafe, AuditEvent: rid("measurement.fields"), Handle: measurementFields},
		{ID: rid("measurement.tags"), Method: plugin.MethodGet, Path: "/measurements/{namespace}/{measurement}/tags", Permission: "influxdb.measurements.read", Risk: plugin.RiskSafe, AuditEvent: rid("measurement.tags"), Handle: measurementTags},
		{ID: rid("query"), Method: plugin.MethodWS, Path: "/query", Permission: "influxdb.query.execute", Risk: plugin.RiskPrivileged, AuditEvent: rid("query"), Stream: queryStream},
		{ID: rid("completion"), Method: plugin.MethodGet, Path: "/completion", Permission: "influxdb.query.execute", Risk: plugin.RiskSafe, AuditEvent: rid("completion"), Handle: completionRoute},
		{ID: rid("write"), Method: plugin.MethodPost, Path: "/namespaces/{namespace}/write", Permission: "influxdb.write", Risk: plugin.RiskWrite, AuditEvent: rid("write"), Input: writeSchema(), Handle: writeLineProtocol},
		{ID: rid("namespace.create"), Method: plugin.MethodPost, Path: "/namespaces", Permission: "influxdb.namespaces.write", Risk: plugin.RiskWrite, AuditEvent: rid("namespace.create"), Input: namespaceCreateSchema(), Handle: createNamespace},
		{ID: rid("namespace.delete"), Method: plugin.MethodDelete, Path: "/namespaces/{namespace}", Permission: "influxdb.namespaces.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("namespace.delete"), Handle: deleteNamespace},
	}
}

func session(rc *plugin.RequestContext) (*Session, error) { return unwrap(rc.Session) }

var statusRows = []row{
	{"name": "health", "description": "Health endpoint and connection mode"},
	{"name": "config", "description": "Resolved non-secret plugin options"},
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
	rows := cloneRows(statusRows)
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
	switch rc.Param("status") {
	case "health":
		err := s.HealthCheck(rc.Ctx)
		return row{"ok": err == nil, "mode": s.opts.Mode, "endpoint": s.opts.Endpoint, "error": errorString(err)}, nil
	case "config":
		return row{
			"mode": s.opts.Mode, "endpoint": s.opts.Endpoint, "org": s.opts.Org,
			"default_database": s.opts.DefaultDatabase, "query_language": s.opts.QueryLanguage,
			"page_limit": s.opts.PageLimit, "read_only": s.opts.ReadOnly, "confirm_writes": s.opts.ConfirmWrites,
		}, nil
	default:
		return nil, plugin.ErrNotFound
	}
}

func namespaceTree(rc *plugin.RequestContext) (any, error) {
	res, err := namespaceList(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name := fmt.Sprint(item["name"])
		ref := plugin.ResourceRef{Kind: "namespace", Name: name, UID: name}
		nodes = append(nodes, plugin.TreeNode{
			Key: "namespace:" + name, Label: name, Icon: icon("database"), Ref: &ref,
			Leaf: true, ResourceKind: "measurement", ListParams: map[string]string{"namespace": name},
		})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, Total: page.Total}, nil
}

func namespaceList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	rows, err := namespaces(rc.Ctx, s)
	if err != nil {
		return nil, err
	}
	for _, item := range rows {
		name := fmt.Sprint(item["name"])
		item["ref"] = plugin.ResourceRef{Kind: "namespace", Name: name, UID: name}
	}
	return broker.PageRows(rc, rows)
}

func namespaceRead(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	name := namespaceParam(rc)
	rows, err := namespaces(rc.Ctx, s)
	if err != nil {
		return nil, err
	}
	for _, item := range rows {
		if fmt.Sprint(item["name"]) == name {
			return item, nil
		}
	}
	return nil, plugin.ErrNotFound
}

func measurementTree(rc *plugin.RequestContext) (any, error) {
	res, err := measurementList(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name := fmt.Sprint(item["name"])
		namespace := fmt.Sprint(item["namespace"])
		ref := plugin.ResourceRef{Kind: "measurement", Namespace: namespace, Name: name, UID: namespace + "." + name}
		nodes = append(nodes, plugin.TreeNode{Key: "measurement:" + namespace + ":" + name, Label: name, Icon: icon("table-2"), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, Total: page.Total}, nil
}

func measurementList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	namespace := namespaceParam(rc)
	if namespace == "" {
		namespace = s.opts.DefaultDatabase
	}
	rows, err := measurements(rc.Ctx, s, namespace)
	if err != nil {
		return nil, err
	}
	for _, item := range rows {
		name := fmt.Sprint(item["name"])
		ns := fmt.Sprint(item["namespace"])
		item["ref"] = plugin.ResourceRef{Kind: "measurement", Namespace: ns, Name: name, UID: ns + "." + name}
	}
	return broker.PageRows(rc, rows)
}

func measurementRows(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	rows, err := previewRows(rc.Ctx, s, namespaceParam(rc), measurementParam(rc), pageLimit(rc, s.opts.PageLimit))
	if err != nil {
		return nil, err
	}
	return broker.PageRows(rc, rows)
}

func measurementFields(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	rows, err := fields(rc.Ctx, s, namespaceParam(rc), measurementParam(rc))
	if err != nil {
		return nil, err
	}
	return broker.PageRows(rc, rows)
}

func measurementTags(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	rows, err := tags(rc.Ctx, s, namespaceParam(rc), measurementParam(rc))
	if err != nil {
		return nil, err
	}
	return broker.PageRows(rc, rows)
}

func queryStream(rc *plugin.RequestContext, stream plugin.ClientStream) error {
	s, err := session(rc)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(stream)
	dec := json.NewDecoder(stream)
	for {
		var req sqldb.QueryRequest
		if err := dec.Decode(&req); err != nil {
			return nil
		}
		result, err := executeQuery(stream.Context(), s, queryNamespace(rc, s), req)
		params := sqldb.AuditParams(sqldb.QueryAudit{
			Query: req.Query, Statements: sqldb.SplitStatements(req.Query), Confirmed: req.Confirm,
			ReadOnlyMode: s.opts.ReadOnly, RequiresReview: statementNeedsReview(req.Query),
		})
		rc.Audit(queryAuditResult(err), params, err)
		if err != nil {
			payload := map[string]any{"error": err.Error()}
			var confirmErr confirmationError
			if strings.Contains(err.Error(), "requires confirmation") || errors.As(err, &confirmErr) {
				payload["requiresConfirmation"] = true
				payload["confirmMessage"] = "This InfluxDB operation can write data or change schema. Review it before running."
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

func executeQuery(ctx context.Context, s *Session, namespace string, req sqldb.QueryRequest) (sqldb.QueryResult, error) {
	text := strings.TrimSpace(req.Query)
	if text == "" {
		return sqldb.QueryResult{}, fmt.Errorf("%w: query is empty", plugin.ErrInvalidInput)
	}
	language := s.opts.QueryLanguage
	var body map[string]any
	if strings.HasPrefix(text, "{") && json.Unmarshal([]byte(text), &body) == nil {
		if q := strings.TrimSpace(fmt.Sprint(body["query"])); q != "" && q != "<nil>" {
			text = q
		}
		if lang := strings.TrimSpace(fmt.Sprint(body["language"])); lang != "" && lang != "<nil>" {
			language = strings.ToLower(lang)
		}
		if ns := strings.TrimSpace(fmt.Sprint(body["namespace"])); ns != "" && ns != "<nil>" {
			namespace = ns
		}
	}
	if namespace == "" && s.opts.Mode != modeV2 {
		namespace = s.opts.DefaultDatabase
	}
	if s.opts.ReadOnly && !readOnlyQuery(text, language) {
		return sqldb.QueryResult{}, fmt.Errorf("%w: read-only mode blocks write statements", plugin.ErrForbidden)
	}
	if s.opts.ConfirmWrites && !req.Confirm && statementNeedsReview(text) {
		return sqldb.QueryResult{}, confirmationError{message: "statement requires confirmation"}
	}
	start := time.Now()
	rows, err := queryRows(ctx, s, namespace, language, text)
	if err != nil {
		return sqldb.QueryResult{}, err
	}
	columns, matrix := matrixRows(rows)
	return sqldb.QueryResult{Columns: columns, Rows: matrix, RowCount: int64(len(matrix)), ElapsedMS: time.Since(start).Milliseconds(), Statement: text}, nil
}

func writeLineProtocol(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if s.opts.ReadOnly {
		return nil, fmt.Errorf("%w: read-only mode blocks writes", plugin.ErrForbidden)
	}
	var req struct {
		LineProtocol string `json:"line_protocol" validate:"required"`
		Precision    string `json:"precision"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	precision := strings.TrimSpace(req.Precision)
	if precision == "" {
		precision = "ns"
	}
	namespace := namespaceParam(rc)
	if namespace == "" {
		namespace = s.opts.DefaultDatabase
	}
	q := url.Values{"precision": []string{precision}}
	path := "/write"
	switch s.opts.Mode {
	case modeV3:
		path = "/api/v3/write_lp"
		q.Set("db", namespace)
	case modeV2:
		path = "/api/v2/write"
		q.Set("org", s.opts.Org)
		q.Set("bucket", namespace)
	default:
		q.Set("db", namespace)
	}
	_, err = s.client.text(rc.Ctx, http.MethodPost, path, q, []byte(req.LineProtocol))
	if err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func namespaceCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Database / bucket", Fields: []plugin.Field{
		{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true},
		{Key: "retention_period", Label: "Retention period", Type: plugin.FieldText, Placeholder: "30d", Help: "Optional retention (e.g. 30d, 24h). Leave empty for infinite retention."},
	}}}}
}

func createNamespace(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if s.opts.ReadOnly {
		return nil, fmt.Errorf("%w: read-only mode blocks writes", plugin.ErrForbidden)
	}
	var req struct {
		Name            string `json:"name" validate:"required"`
		RetentionPeriod string `json:"retention_period"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", plugin.ErrInvalidInput)
	}
	retention := strings.TrimSpace(req.RetentionPeriod)
	switch s.opts.Mode {
	case modeV2:
		return createBucketV2(rc.Ctx, s, name, retention)
	case modeV1:
		if err := v1Exec(rc.Ctx, s, "CREATE DATABASE "+quoteV1Ident(name)); err != nil {
			return nil, err
		}
		return actionResult{OK: true}, nil
	default:
		body := row{"db": name}
		if retention != "" {
			body["retention_period"] = retention
		}
		if err := s.client.json(rc.Ctx, http.MethodPost, "/api/v3/configure/database", nil, body, nil); err != nil {
			return nil, err
		}
		return actionResult{OK: true}, nil
	}
}

func deleteNamespace(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if s.opts.ReadOnly {
		return nil, fmt.Errorf("%w: read-only mode blocks writes", plugin.ErrForbidden)
	}
	name := namespaceParam(rc)
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", plugin.ErrInvalidInput)
	}
	switch s.opts.Mode {
	case modeV2:
		id, err := bucketIDV2(rc.Ctx, s, name)
		if err != nil {
			return nil, err
		}
		if err := s.client.json(rc.Ctx, http.MethodDelete, "/api/v2/buckets/"+url.PathEscape(id), nil, nil, nil); err != nil {
			return nil, err
		}
		return actionResult{OK: true}, nil
	case modeV1:
		if err := v1Exec(rc.Ctx, s, "DROP DATABASE "+quoteV1Ident(name)); err != nil {
			return nil, err
		}
		return actionResult{OK: true}, nil
	default:
		q := url.Values{"db": []string{name}}
		if err := s.client.json(rc.Ctx, http.MethodDelete, "/api/v3/configure/database", q, nil, nil); err != nil {
			return nil, err
		}
		return actionResult{OK: true}, nil
	}
}

func createBucketV2(ctx context.Context, s *Session, name, retention string) (any, error) {
	id, err := orgIDV2(ctx, s)
	if err != nil {
		return nil, err
	}
	rules := []any{}
	if secs := parseRetentionSeconds(retention); secs > 0 {
		rules = append(rules, row{"type": "expire", "everySeconds": secs})
	}
	body := row{"orgID": id, "name": name, "retentionRules": rules}
	if err := s.client.json(ctx, http.MethodPost, "/api/v2/buckets", nil, body, nil); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func orgIDV2(ctx context.Context, s *Session) (string, error) {
	if s.opts.Org == "" {
		return "", fmt.Errorf("%w: org is required to manage InfluxDB 2 buckets", plugin.ErrInvalidInput)
	}
	var out struct {
		Orgs []struct {
			ID string `json:"id"`
		} `json:"orgs"`
	}
	if err := s.client.json(ctx, http.MethodGet, "/api/v2/orgs", url.Values{"org": []string{s.opts.Org}}, nil, &out); err != nil {
		return "", err
	}
	if len(out.Orgs) == 0 || out.Orgs[0].ID == "" {
		return "", fmt.Errorf("%w: org %q not found", plugin.ErrNotFound, s.opts.Org)
	}
	return out.Orgs[0].ID, nil
}

func bucketIDV2(ctx context.Context, s *Session, name string) (string, error) {
	var out struct {
		Buckets []struct {
			ID string `json:"id"`
		} `json:"buckets"`
	}
	q := url.Values{"name": []string{name}}
	if s.opts.Org != "" {
		q.Set("org", s.opts.Org)
	}
	if err := s.client.json(ctx, http.MethodGet, "/api/v2/buckets", q, nil, &out); err != nil {
		return "", err
	}
	if len(out.Buckets) == 0 || out.Buckets[0].ID == "" {
		return "", fmt.Errorf("%w: bucket %q not found", plugin.ErrNotFound, name)
	}
	return out.Buckets[0].ID, nil
}

func v1Exec(ctx context.Context, s *Session, stmt string) error {
	var out v1Response
	if err := s.client.json(ctx, http.MethodPost, "/query", url.Values{"q": []string{stmt}}, nil, &out); err != nil {
		return err
	}
	return out.Err()
}

func quoteV1Ident(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `\"`) + `"`
}

// parseRetentionSeconds converts a retention string like "30d"/"24h"/"45m"/"90s"
// into whole seconds. An empty or unparseable value yields 0 (infinite).
func parseRetentionSeconds(in string) int64 {
	in = strings.TrimSpace(in)
	if in == "" {
		return 0
	}
	unit := in[len(in)-1]
	value := strings.TrimSpace(in[:len(in)-1])
	mult := int64(0)
	switch unit {
	case 's':
		mult = 1
	case 'm':
		mult = 60
	case 'h':
		mult = 3600
	case 'd':
		mult = 86400
	case 'w':
		mult = 604800
	default:
		return 0
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n * mult
}

func writeSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Line protocol", Fields: []plugin.Field{
		{Key: "line_protocol", Label: "Line protocol", Type: plugin.FieldTextarea, Required: true, Placeholder: "cpu,host=web01 usage=0.64"},
		{Key: "precision", Label: "Precision", Type: plugin.FieldSelect, Default: "ns", Options: []plugin.Option{
			{Label: "Nanoseconds", Value: "ns"},
			{Label: "Microseconds", Value: "us"},
			{Label: "Milliseconds", Value: "ms"},
			{Label: "Seconds", Value: "s"},
		}},
	}}}}
}

func completionRoute(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	switch s.opts.Mode {
	case modeV2:
		return []sqldb.CompletionItem{
			{Label: "from", Type: "function", Apply: `from(bucket: "bucket") |> range(start: -1h) |> limit(n: 100)`},
			{Label: "measurements", Type: "schema", Apply: `import "influxdata/influxdb/schema"\nschema.measurements(bucket: "bucket")`},
		}, nil
	case modeV3:
		return []sqldb.CompletionItem{
			{Label: "SHOW TABLES", Type: "influxql", Apply: "SHOW TABLES"},
			{Label: "SQL preview", Type: "sql", Apply: `SELECT * FROM "measurement" LIMIT 100`},
			{Label: "InfluxQL preview", Type: "influxql", Apply: `{"language":"influxql","query":"SELECT * FROM \"measurement\" LIMIT 100"}`},
		}, nil
	default:
		return []sqldb.CompletionItem{
			{Label: "SHOW MEASUREMENTS", Type: "influxql", Apply: "SHOW MEASUREMENTS"},
			{Label: "SHOW FIELD KEYS", Type: "influxql", Apply: `SHOW FIELD KEYS FROM "measurement"`},
			{Label: "SELECT", Type: "influxql", Apply: `SELECT * FROM "measurement" LIMIT 100`},
		}, nil
	}
}

func namespaces(ctx context.Context, s *Session) ([]row, error) {
	switch s.opts.Mode {
	case modeV2:
		var out struct {
			Buckets []row `json:"buckets"`
		}
		q := url.Values{}
		if s.opts.Org != "" {
			q.Set("org", s.opts.Org)
		}
		if err := s.client.json(ctx, http.MethodGet, "/api/v2/buckets", q, nil, &out); err != nil {
			return nil, err
		}
		rows := make([]row, 0, len(out.Buckets))
		for _, b := range out.Buckets {
			name := fmt.Sprint(b["name"])
			if name == "" || strings.HasPrefix(name, "_") {
				continue
			}
			rows = append(rows, row{"name": name, "kind": "bucket", "created": b["createdAt"], "retention": retentionText(b["retentionRules"])})
		}
		return rows, nil
	case modeV1:
		return v1Databases(ctx, s)
	default:
		rows, err := v3Databases(ctx, s)
		if err == nil && len(rows) > 0 {
			return rows, nil
		}
		if s.opts.DefaultDatabase != "" {
			return []row{{"name": s.opts.DefaultDatabase, "kind": "database"}}, nil
		}
		return rows, err
	}
}

func measurements(ctx context.Context, s *Session, namespace string) ([]row, error) {
	switch s.opts.Mode {
	case modeV2:
		q := fmt.Sprintf(`import "influxdata/influxdb/schema"
schema.measurements(bucket: %q)`, namespace)
		rows, err := fluxRows(ctx, s, q)
		if err != nil {
			return nil, err
		}
		return valueRows(rows, namespace, "measurement"), nil
	case modeV1:
		return v1NamedRows(ctx, s, namespace, "SHOW MEASUREMENTS", "measurement", "measurement")
	default:
		rows, err := v3SQLRows(ctx, s, namespace, `SELECT table_name AS name, table_type AS type FROM information_schema.tables WHERE table_schema = 'iox' ORDER BY table_name`)
		if err != nil {
			return nil, err
		}
		for _, item := range rows {
			item["namespace"] = namespace
			if fmt.Sprint(item["type"]) == "" {
				item["type"] = "table"
			}
		}
		return rows, nil
	}
}

func fields(ctx context.Context, s *Session, namespace, measurement string) ([]row, error) {
	switch s.opts.Mode {
	case modeV2:
		q := fmt.Sprintf(`import "influxdata/influxdb/schema"
schema.fieldKeys(bucket: %q, predicate: (r) => r._measurement == %q, start: %s)`, namespace, measurement, s.opts.Lookback)
		rows, err := fluxRows(ctx, s, q)
		if err != nil {
			return nil, err
		}
		return valueRows(rows, namespace, "field"), nil
	case modeV1:
		return v1NamedRows(ctx, s, namespace, `SHOW FIELD KEYS FROM `+quoteInfluxQL(measurement), "fieldKey", "field")
	default:
		q := fmt.Sprintf("SELECT column_name AS name, data_type AS type FROM information_schema.columns WHERE table_schema = 'iox' AND table_name = %s ORDER BY ordinal_position", sqlString(measurement))
		rows, err := v3SQLRows(ctx, s, namespace, q)
		if err != nil {
			return nil, err
		}
		return withoutTime(rows), nil
	}
}

func tags(ctx context.Context, s *Session, namespace, measurement string) ([]row, error) {
	switch s.opts.Mode {
	case modeV2:
		q := fmt.Sprintf(`import "influxdata/influxdb/schema"
schema.tagKeys(bucket: %q, predicate: (r) => r._measurement == %q, start: %s)`, namespace, measurement, s.opts.Lookback)
		rows, err := fluxRows(ctx, s, q)
		if err != nil {
			return nil, err
		}
		return valueRows(rows, namespace, "tag"), nil
	case modeV1:
		return v1NamedRows(ctx, s, namespace, `SHOW TAG KEYS FROM `+quoteInfluxQL(measurement), "tagKey", "tag")
	default:
		rows, err := v3InfluxQLRows(ctx, s, namespace, `SHOW TAG KEYS FROM `+quoteInfluxQL(measurement))
		if err != nil {
			return nil, err
		}
		return normalizeNameRows(rows, "tagKey", "tag"), nil
	}
}

func previewRows(ctx context.Context, s *Session, namespace, measurement string, limit int) ([]row, error) {
	switch s.opts.Mode {
	case modeV2:
		q := fmt.Sprintf(`from(bucket: %q) |> range(start: %s) |> filter(fn: (r) => r._measurement == %q) |> limit(n: %d)`, namespace, s.opts.Lookback, measurement, limit)
		return fluxRows(ctx, s, q)
	case modeV1:
		return v1QueryRows(ctx, s, namespace, fmt.Sprintf("SELECT * FROM %s LIMIT %d", quoteInfluxQL(measurement), limit))
	default:
		return v3SQLRows(ctx, s, namespace, fmt.Sprintf("SELECT * FROM %s LIMIT %d", quoteSQLIdent(measurement), limit))
	}
}

func queryRows(ctx context.Context, s *Session, namespace, language, query string) ([]row, error) {
	switch s.opts.Mode {
	case modeV2:
		return fluxRows(ctx, s, query)
	case modeV1:
		return v1QueryRows(ctx, s, namespace, query)
	default:
		if strings.EqualFold(language, "influxql") {
			return v3InfluxQLRows(ctx, s, namespace, query)
		}
		return v3SQLRows(ctx, s, namespace, query)
	}
}

func v3Databases(ctx context.Context, s *Session) ([]row, error) {
	var raw any
	if err := s.client.json(ctx, http.MethodGet, "/api/v3/configure/database", nil, nil, &raw); err != nil {
		return nil, err
	}
	names := collectNames(raw)
	rows := make([]row, 0, len(names))
	for _, name := range names {
		rows = append(rows, row{"name": name, "kind": "database"})
	}
	return rows, nil
}

func v3SQLRows(ctx context.Context, s *Session, db, query string) ([]row, error) {
	q := url.Values{"format": []string{"jsonl"}}
	if db != "" {
		q.Set("db", db)
	}
	resp, err := s.client.raw(ctx, http.MethodPost, "/api/v3/query_sql", q, "application/json", mustJSON(row{"q": query}), "application/json")
	if err != nil {
		return nil, err
	}
	return parseJSONOrJSONLRows(resp)
}

func v3InfluxQLRows(ctx context.Context, s *Session, db, query string) ([]row, error) {
	q := url.Values{"format": []string{"jsonl"}}
	if db != "" {
		q.Set("db", db)
	}
	resp, err := s.client.raw(ctx, http.MethodPost, "/api/v3/query_influxql", q, "application/json", mustJSON(row{"q": query}), "application/json")
	if err != nil {
		return nil, err
	}
	return parseJSONOrJSONLRows(resp)
}

func fluxRows(ctx context.Context, s *Session, query string) ([]row, error) {
	q := url.Values{}
	if s.opts.Org != "" {
		q.Set("org", s.opts.Org)
	}
	return s.client.csv(ctx, http.MethodPost, "/api/v2/query", q, row{"query": query, "type": "flux"})
}

func v1Databases(ctx context.Context, s *Session) ([]row, error) {
	rows, err := v1QueryRows(ctx, s, "", "SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	out := []row{}
	for _, item := range rows {
		name := fmt.Sprint(item["name"])
		if name == "" || strings.HasPrefix(name, "_") {
			continue
		}
		out = append(out, row{"name": name, "kind": "database"})
	}
	return out, nil
}

func v1NamedRows(ctx context.Context, s *Session, db, query, key, typ string) ([]row, error) {
	rows, err := v1QueryRows(ctx, s, db, query)
	if err != nil {
		return nil, err
	}
	return normalizeNameRows(rows, key, typ), nil
}

func v1QueryRows(ctx context.Context, s *Session, db, query string) ([]row, error) {
	q := url.Values{"q": []string{query}}
	if db != "" {
		q.Set("db", db)
	}
	var out v1Response
	if err := s.client.json(ctx, http.MethodGet, "/query", q, nil, &out); err != nil {
		return nil, err
	}
	if err := out.Err(); err != nil {
		return nil, err
	}
	return out.Rows(), nil
}

type v1Response struct {
	Results []struct {
		Error  string `json:"error"`
		Series []struct {
			Name    string   `json:"name"`
			Columns []string `json:"columns"`
			Values  [][]any  `json:"values"`
		} `json:"series"`
	} `json:"results"`
}

func (r v1Response) Err() error {
	for _, res := range r.Results {
		if strings.TrimSpace(res.Error) != "" {
			return fmt.Errorf("%w: %s", plugin.ErrInvalidInput, res.Error)
		}
	}
	return nil
}

func (r v1Response) Rows() []row {
	out := []row{}
	for _, res := range r.Results {
		for _, series := range res.Series {
			for _, values := range series.Values {
				item := row{}
				for i, col := range series.Columns {
					if i < len(values) {
						item[col] = values[i]
					}
				}
				if series.Name != "" {
					item["_series"] = series.Name
				}
				out = append(out, item)
			}
		}
	}
	return out
}

func parseJSONOrJSONLRows(data []byte) ([]row, error) {
	rows, err := parseJSONLRows(data)
	if err == nil && len(rows) > 0 {
		return rows, nil
	}
	var arr []row
	if json.Unmarshal(data, &arr) == nil {
		return arr, nil
	}
	var one row
	if json.Unmarshal(data, &one) == nil {
		if raw, ok := one["results"]; ok {
			return rowsFromAny(raw), nil
		}
		return []row{one}, nil
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return []row{}, nil
	}
	return rows, err
}

func matrixRows(rows []row) ([]string, [][]any) {
	colset := map[string]bool{}
	for _, item := range rows {
		for key := range item {
			colset[key] = true
		}
	}
	columns := make([]string, 0, len(colset))
	for key := range colset {
		columns = append(columns, key)
	}
	sort.Strings(columns)
	matrix := make([][]any, 0, len(rows))
	for _, item := range rows {
		line := make([]any, len(columns))
		for i, key := range columns {
			line[i] = item[key]
		}
		matrix = append(matrix, line)
	}
	return columns, matrix
}

func valueRows(rows []row, namespace, typ string) []row {
	out := []row{}
	for _, item := range rows {
		name := firstString(item, "_value", "value", "name")
		if name == "" {
			continue
		}
		out = append(out, row{"name": name, "namespace": namespace, "type": typ})
	}
	return dedupeRows(out)
}

func normalizeNameRows(rows []row, key, typ string) []row {
	out := []row{}
	for _, item := range rows {
		name := firstString(item, "name", key, "_value")
		if name == "" {
			continue
		}
		out = append(out, row{"name": name, "type": firstNonEmpty(fmt.Sprint(item["type"]), typ)})
	}
	return dedupeRows(out)
}

func withoutTime(rows []row) []row {
	out := []row{}
	for _, item := range rows {
		name := fmt.Sprint(item["name"])
		if name == "" || name == "time" {
			continue
		}
		out = append(out, item)
	}
	return out
}

func dedupeRows(rows []row) []row {
	seen := map[string]bool{}
	out := []row{}
	for _, item := range rows {
		key := fmt.Sprint(item["name"]) + "|" + fmt.Sprint(item["namespace"]) + "|" + fmt.Sprint(item["type"])
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, item)
	}
	return out
}

func queryNamespace(rc *plugin.RequestContext, s *Session) string {
	if ns := namespaceParam(rc); ns != "" {
		return ns
	}
	return s.opts.DefaultDatabase
}

func namespaceParam(rc *plugin.RequestContext) string {
	if v := strings.TrimSpace(rc.Param("namespace")); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p.namespace"))
}

func measurementParam(rc *plugin.RequestContext) string {
	return strings.TrimSpace(rc.Param("measurement"))
}

func readOnlyQuery(query, language string) bool {
	if strings.EqualFold(language, "flux") {
		return !strings.Contains(strings.ToLower(query), "to(")
	}
	return sqldb.IsReadOnlyStatement(query)
}

func statementNeedsReview(query string) bool {
	if strings.Contains(strings.ToLower(query), "to(") {
		return true
	}
	return sqldb.IsDestructiveStatement(query)
}

type confirmationError struct{ message string }

func (e confirmationError) Error() string { return e.message }

func queryAuditResult(err error) plugin.AuditResult {
	if err == nil {
		return plugin.AuditAllowed
	}
	var confirmErr confirmationError
	if errors.As(err, &confirmErr) {
		return plugin.AuditDenied
	}
	return plugin.AuditError
}

func collectNames(v any) []string {
	names := map[string]bool{}
	var walk func(any)
	walk = func(x any) {
		switch t := x.(type) {
		case []any:
			for _, item := range t {
				walk(item)
			}
		case map[string]any:
			for _, key := range []string{"name", "database", "db"} {
				if name := strings.TrimSpace(fmt.Sprint(t[key])); name != "" && name != "<nil>" {
					names[name] = true
				}
			}
			for _, value := range t {
				walk(value)
			}
		case string:
			if t = strings.TrimSpace(t); t != "" {
				names[t] = true
			}
		}
	}
	walk(v)
	out := make([]string, 0, len(names))
	for name := range names {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func rowsFromAny(v any) []row {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	var rows []row
	_ = json.Unmarshal(raw, &rows)
	return rows
}

func cloneRows(in []row) []row {
	out := make([]row, len(in))
	for i, item := range in {
		next := row{}
		for k, v := range item {
			next[k] = v
		}
		out[i] = next
	}
	return out
}

func mustJSON(v any) []byte {
	data, _ := json.Marshal(v)
	return data
}

func quoteSQLIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func quoteInfluxQL(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `\"`) + `"`
}

func sqlString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func firstString(item row, keys ...string) string {
	for _, key := range keys {
		text := strings.TrimSpace(fmt.Sprint(item[key]))
		if text != "" && text != "<nil>" {
			return text
		}
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" && value != "<nil>" {
			return value
		}
	}
	return ""
}

func retentionText(raw any) string {
	data, _ := json.Marshal(raw)
	if string(data) == "null" || len(data) == 0 {
		return ""
	}
	return string(data)
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func pageLimit(rc *plugin.RequestContext, defaultLimit int) int {
	page, err := rc.Page()
	if err != nil || page.Limit <= 0 {
		return defaultLimit
	}
	if page.Limit > plugin.MaxPageLimit {
		return plugin.MaxPageLimit
	}
	return page.Limit
}
