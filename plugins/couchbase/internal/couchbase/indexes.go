package couchbase

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

type indexStatus struct {
	IndexName    string      `json:"indexName"`
	Bucket       string      `json:"bucket"`
	Scope        string      `json:"scope"`
	Collection   string      `json:"collection"`
	Status       string      `json:"status"`
	Definition   string      `json:"definition"`
	Progress     float64     `json:"progress"`
	StorageMode  string      `json:"storageMode"`
	Hosts        []string    `json:"hosts"`
	Stale        bool        `json:"stale"`
	NumReplica   int         `json:"numReplica"`
	ReplicaID    int         `json:"replicaId"`
	LastScanTime string      `json:"lastScanTime"`
	ID           json.Number `json:"id"`
}

func (i indexStatus) keyspace() keyspace {
	return keyspace{Bucket: i.Bucket, Scope: i.Scope, Collection: i.Collection, Document: i.IndexName}
}

func (i indexStatus) row() row {
	k := i.keyspace()
	collection := keyspace{Bucket: i.Bucket, Scope: i.Scope, Collection: i.Collection}
	return row{
		"ref":            plugin.ResourceIdentity{Kind: "index", Scope: i.Bucket, Namespace: i.Scope, Name: i.IndexName, UID: k.uid()},
		"name":           i.IndexName,
		"bucket":         i.Bucket,
		"scope":          i.Scope,
		"collection":     i.Collection,
		"keyspace":       collection.label(),
		"status":         strings.ToLower(i.Status),
		"progress":       round1(i.Progress),
		"definition":     i.Definition,
		"storage_mode":   i.StorageMode,
		"hosts":          strings.Join(i.Hosts, ", "),
		"stale":          i.Stale,
		"replicas":       i.NumReplica,
		"replica_id":     i.ReplicaID,
		"last_scan_time": i.LastScanTime,
		"primary":        strings.Contains(strings.ToUpper(i.Definition), "PRIMARY INDEX"),
	}
}

func (s *Session) indexStatuses(ctx context.Context) ([]indexStatus, error) {
	var out struct {
		Indexes []indexStatus `json:"indexes"`
	}
	if err := s.mgmtGet(ctx, "/indexStatus", &out); err != nil {
		return nil, err
	}
	sort.Slice(out.Indexes, func(i, j int) bool {
		if out.Indexes[i].Bucket != out.Indexes[j].Bucket {
			return out.Indexes[i].Bucket < out.Indexes[j].Bucket
		}
		return out.Indexes[i].IndexName < out.Indexes[j].IndexName
	})
	return out.Indexes, nil
}

func (s *Session) collectionIndexes(ctx context.Context, k keyspace) ([]indexStatus, error) {
	all, err := s.indexStatuses(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]indexStatus, 0, len(all))
	for _, index := range all {
		if index.Bucket != k.Bucket {
			continue
		}
		if k.Scope != "" && index.Scope != k.Scope {
			continue
		}
		if k.Collection != "" && index.Collection != k.Collection {
			continue
		}
		out = append(out, index)
	}
	return out, nil
}

func indexesList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var indexes []indexStatus
	// Without any keyspace hint the panel is the cluster-wide index list.
	scoped := strings.TrimSpace(rc.Param("keyspace")) != "" ||
		strings.TrimSpace(rc.Param("bucket")) != "" || s.opts.Bucket != ""
	if scoped {
		k, kerr := keyspaceFrom(rc, s)
		if kerr != nil {
			return nil, kerr
		}
		indexes, err = s.collectionIndexes(rc.Ctx, k)
	} else {
		indexes, err = s.indexStatuses(rc.Ctx)
	}
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(indexes))
	for _, index := range indexes {
		rows = append(rows, index.row())
	}
	return broker.PageRows(rc, rows)
}

func indexKeyspace(rc *plugin.RequestContext) (keyspace, error) {
	k, err := parseKeyspaceUID(rc.Param("id"))
	if err != nil {
		return keyspace{}, err
	}
	if k.Document == "" {
		return keyspace{}, fmt.Errorf("%w: an index reference is required", plugin.ErrInvalidInput)
	}
	if _, err := safeIndexName(k.Document); err != nil {
		return keyspace{}, err
	}
	return k, nil
}

func indexRead(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	k, err := indexKeyspace(rc)
	if err != nil {
		return nil, err
	}
	indexes, err := s.indexStatuses(rc.Ctx)
	if err != nil {
		return nil, err
	}
	for _, index := range indexes {
		if index.IndexName != k.Document || index.Bucket != k.Bucket || index.Scope != k.Scope || index.Collection != k.Collection {
			continue
		}
		item := index.row()
		if meta, err := s.indexMetadata(rc.Ctx, k); err == nil {
			for key, value := range meta {
				item[key] = value
			}
		}
		return item, nil
	}
	return nil, fmt.Errorf("%w: index %q not found", plugin.ErrNotFound, k.Document)
}

// indexMetadata enriches the management view with the catalog entry the Query
// service keeps, which carries the index keys, partitioning, and condition.
// system:indexes omits bucket_id for indexes on the default scope and collection
// and carries the bucket name in keyspace_id instead, so both shapes are matched.
func (s *Session) indexMetadata(ctx context.Context, k keyspace) (row, error) {
	statement := "SELECT RAW idx FROM system:indexes idx WHERE idx.name = $1" +
		" AND IFMISSING(idx.bucket_id, idx.keyspace_id) = $2" +
		" AND IFMISSINGORNULL(idx.scope_id, '_default') = $3" +
		" AND (CASE WHEN idx.bucket_id IS MISSING THEN '_default' ELSE idx.keyspace_id END) = $4"
	rows, err := s.queryRows(ctx, statement, k.Document, k.Bucket, k.Scope, k.Collection)
	if err != nil || len(rows) == 0 {
		if err == nil {
			err = fmt.Errorf("%w: index metadata unavailable", plugin.ErrNotFound)
		}
		return nil, err
	}
	entry := rows[0]
	return row{
		"index_keys": strings.Join(stringSlice(entry["index_key"]), ", "),
		"condition":  stringValue(entry["condition"]),
		"partition":  stringValue(entry["partition"]),
		"using":      stringValue(entry["using"]),
		"state":      stringValue(entry["state"]),
		"is_primary": entry["is_primary"] == true,
	}, nil
}

func indexCreate(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	k, err := requireCollection(rc, s)
	if err != nil {
		return nil, err
	}
	var in struct {
		Name       string `json:"name" validate:"required"`
		Fields     any    `json:"fields"`
		Primary    bool   `json:"primary"`
		Deferred   bool   `json:"deferred"`
		NumReplica int    `json:"num_replica"`
	}
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	name, err := safeIndexName(in.Name)
	if err != nil {
		return nil, err
	}
	statement := "CREATE INDEX " + quoteName(name) + " ON " + k.path()
	if in.Primary {
		statement = "CREATE PRIMARY INDEX " + quoteName(name) + " ON " + k.path()
	} else {
		fields, err := sqldb.IdentifierListValue(in.Fields, quoteName)
		if err != nil {
			return nil, err
		}
		statement += "(" + strings.Join(fields, ", ") + ")"
	}
	with := map[string]any{}
	if in.Deferred {
		with["defer_build"] = true
	}
	if in.NumReplica > 0 {
		with["num_replica"] = in.NumReplica
	}
	if len(with) > 0 {
		statement += " WITH " + string(mustJSON(with))
	}
	if _, err := s.exec(rc.Ctx, statement); err != nil {
		return nil, err
	}
	created := k
	created.Document = name
	return row{
		"ok":   true,
		"name": name,
		"ref":  plugin.ResourceIdentity{Kind: "index", Scope: k.Bucket, Namespace: k.Scope, Name: name, UID: created.uid()},
	}, nil
}

func indexBuild(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	k, err := indexKeyspace(rc)
	if err != nil {
		return nil, err
	}
	collection := keyspace{Bucket: k.Bucket, Scope: k.Scope, Collection: k.Collection}
	statement := "BUILD INDEX ON " + collection.path() + "(" + quoteName(k.Document) + ")"
	if _, err := s.exec(rc.Ctx, statement); err != nil {
		return nil, err
	}
	return actionResult{OK: true, Message: "build started for index " + k.Document}, nil
}

func indexDrop(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	k, err := indexKeyspace(rc)
	if err != nil {
		return nil, err
	}
	collection := keyspace{Bucket: k.Bucket, Scope: k.Scope, Collection: k.Collection}
	statement := "DROP INDEX " + quoteName(k.Document) + " ON " + collection.path()
	if k.Document == "#primary" {
		statement = "DROP PRIMARY INDEX ON " + collection.path()
	}
	if _, err := s.exec(rc.Ctx, statement); err != nil {
		return nil, err
	}
	return actionResult{OK: true, Message: "index " + k.Document + " dropped"}, nil
}

// --- index advisor ----------------------------------------------------------

// fieldRefRE matches the `alias`.`field` references the Query service emits in
// EXPLAIN filter conditions.
var fieldRefRE = regexp.MustCompile("`([^`]+)`\\.`([^`]+)`")

type adviceRow struct {
	Kind      string `json:"kind"`
	Keyspace  string `json:"keyspace"`
	Detail    string `json:"detail"`
	Statement string `json:"statement"`
	Source    string `json:"source"`
}

var adviceColumns = []string{"kind", "keyspace", "detail", "statement", "source"}

func (a adviceRow) values() []any {
	return []any{a.Kind, a.Keyspace, a.Detail, a.Statement, a.Source}
}

// advisableStatement keeps the advisor to a single analysable statement: EXPLAIN
// and ADVISE prefix whatever they are handed, so a second statement smuggled in
// behind a semicolon would run with the advisor's read-only permission.
func advisableStatement(statement string) error {
	if len(sqldb.SplitStatements(statement)) > 1 {
		return fmt.Errorf("%w: the advisor analyses one statement at a time", plugin.ErrInvalidInput)
	}
	switch sqldb.FirstKeyword(statement) {
	case "SELECT", "UPDATE", "DELETE", "MERGE", "WITH":
		return nil
	default:
		return fmt.Errorf("%w: the advisor only analyses SELECT, UPDATE, DELETE, MERGE, and WITH statements", plugin.ErrInvalidInput)
	}
}

// advise asks the server's ADVISE first and falls back to a plan-derived
// analysis, because ADVISE is an Enterprise-only feature.
func (s *Session) advise(ctx context.Context, statement string) ([]adviceRow, error) {
	if err := advisableStatement(statement); err != nil {
		return nil, err
	}
	// ADVISE is Enterprise-only and reports that differently across releases, so
	// any failure hands over to the plan-derived analysis, which also surfaces the
	// real parse error for a malformed statement.
	if rows, err := s.serverAdvice(ctx, statement); err == nil && len(rows) > 0 {
		return rows, nil
	}
	return s.planAdvice(ctx, statement)
}

func (s *Session) serverAdvice(ctx context.Context, statement string) ([]adviceRow, error) {
	res, err := s.exec(ctx, "ADVISE "+statement)
	if err != nil {
		return nil, err
	}
	out := make([]adviceRow, 0, 4)
	for _, raw := range res.Results {
		var item map[string]any
		if err := json.Unmarshal(raw, &item); err != nil {
			continue
		}
		info := asMap(mapValue(item, "advice"))
		if len(info) == 0 {
			info = asMap(mapValue(item, "adviseinfo"))
		}
		for _, current := range toSlice(mapValue(info, "current_indexes")) {
			entry := asMap(current)
			out = append(out, adviceRow{
				Kind:      "in use",
				Keyspace:  stringValue(entry["keyspace_alias"]),
				Detail:    stringValue(entry["index_status"]),
				Statement: stringValue(entry["index_statement"]),
				Source:    "advise",
			})
		}
		recommended := asMap(mapValue(info, "recommended_indexes"))
		for _, group := range []string{"indexes", "covering_indexes"} {
			for _, candidate := range toSlice(mapValue(recommended, group)) {
				entry := asMap(candidate)
				out = append(out, adviceRow{
					Kind:      strings.TrimSuffix(group, "es"),
					Keyspace:  stringValue(entry["keyspace_alias"]),
					Detail:    stringValue(entry["recommending_rule"]),
					Statement: stringValue(entry["index_statement"]),
					Source:    "advise",
				})
			}
		}
	}
	return out, nil
}

func toSlice(v any) []any {
	switch t := v.(type) {
	case []any:
		return t
	case nil:
		return nil
	default:
		return []any{t}
	}
}

// planAdvice derives recommendations from the query plan: it flags primary scans
// (full keyspace scans) and proposes a covering secondary index built from the
// predicate fields the plan filters on.
func (s *Session) planAdvice(ctx context.Context, statement string) ([]adviceRow, error) {
	res, err := s.exec(ctx, "EXPLAIN "+statement)
	if err != nil {
		return nil, err
	}
	plan := struct {
		primaryScans []keyspace
		indexScans   []adviceRow
		filters      []string
		orderFields  []string
	}{}
	for _, raw := range res.Results {
		var item map[string]any
		if err := json.Unmarshal(raw, &item); err != nil {
			continue
		}
		walkPlan(asMap(item["plan"]), func(op map[string]any) {
			switch stringValue(op["#operator"]) {
			case "PrimaryScan", "PrimaryScan3":
				plan.primaryScans = append(plan.primaryScans, planKeyspace(op))
			case "IndexScan", "IndexScan2", "IndexScan3":
				plan.indexScans = append(plan.indexScans, adviceRow{
					Kind:     "in use",
					Keyspace: planKeyspace(op).label(),
					Detail:   "index scan",
					Source:   "plan",
				})
			case "Filter":
				plan.filters = append(plan.filters, stringValue(op["condition"]))
			case "Order":
				for _, term := range toSlice(op["sort_terms"]) {
					plan.orderFields = append(plan.orderFields, fieldNames(stringValue(asMap(term)["expr"]))...)
				}
			}
		})
	}

	out := make([]adviceRow, 0, 4)
	for i := range plan.indexScans {
		scan := plan.indexScans[i]
		if scan.Keyspace == "" {
			continue
		}
		out = append(out, scan)
	}
	predicate := uniqueStrings(fieldNames(strings.Join(plan.filters, " ")))
	for _, scanned := range plan.primaryScans {
		if scanned.Bucket == "" {
			continue
		}
		fields := append([]string{}, predicate...)
		fields = append(fields, plan.orderFields...)
		fields = uniqueStrings(fields)
		detail := "primary scan reads every document in the keyspace"
		suggestion := ""
		if len(fields) > 0 {
			quoted := make([]string, 0, len(fields))
			for _, field := range fields {
				quoted = append(quoted, quoteName(field))
			}
			suggestion = "CREATE INDEX " + quoteName(indexNameFor(scanned, fields)) + " ON " + scanned.path() + "(" + strings.Join(quoted, ", ") + ")"
			detail += "; the plan filters or sorts on " + strings.Join(fields, ", ")
		} else {
			detail += "; add a predicate the index can serve"
		}
		out = append(out, adviceRow{
			Kind:      "index",
			Keyspace:  scanned.label(),
			Detail:    detail,
			Statement: suggestion,
			Source:    "plan",
		})
	}
	if len(out) == 0 {
		out = append(out, adviceRow{
			Kind:   "ok",
			Detail: "the plan uses existing indexes; no additional index is recommended",
			Source: "plan",
		})
	}
	return out, nil
}

func indexNameFor(k keyspace, fields []string) string {
	parts := []string{"idx", k.Collection}
	parts = append(parts, fields...)
	name := strings.Join(parts, "_")
	sanitized := make([]rune, 0, len(name))
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
			sanitized = append(sanitized, r)
		default:
			sanitized = append(sanitized, '_')
		}
	}
	if len(sanitized) > 100 {
		sanitized = sanitized[:100]
	}
	return string(sanitized)
}

func planKeyspace(op map[string]any) keyspace {
	k := keyspace{
		Bucket:     stringValue(op["bucket"]),
		Scope:      stringValue(op["scope"]),
		Collection: stringValue(op["keyspace"]),
	}
	if k.Bucket == "" {
		k.Bucket = stringValue(op["namespace"])
	}
	return k
}

func walkPlan(node map[string]any, visit func(map[string]any)) {
	if len(node) == 0 {
		return
	}
	if _, ok := node["#operator"]; ok {
		visit(node)
	}
	for _, key := range sortedKeys(node) {
		switch child := node[key].(type) {
		case map[string]any:
			walkPlan(child, visit)
		case []any:
			for _, item := range child {
				if nested, ok := item.(map[string]any); ok {
					walkPlan(nested, visit)
				}
			}
		}
	}
}

func fieldNames(condition string) []string {
	matches := fieldRefRE.FindAllStringSubmatch(condition, -1)
	out := make([]string, 0, len(matches))
	for _, match := range matches {
		field := match[2]
		if field == "" || strings.HasPrefix(field, "#") {
			continue
		}
		out = append(out, field)
	}
	return out
}

func uniqueStrings(in []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, item := range in {
		if item == "" || seen[item] {
			continue
		}
		seen[item] = true
		out = append(out, item)
	}
	return out
}

// advisorStream backs the Index advisor query-editor panel: the operator pastes a
// SQL++ statement and receives index recommendations instead of rows.
func advisorStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
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
		statement := strings.TrimSpace(req.Query)
		if statement == "" {
			if err := enc.Encode(map[string]any{"error": "a SQL++ statement is required"}); err != nil {
				return err
			}
			continue
		}
		rows, err := s.advise(rc.Ctx, strings.TrimSuffix(statement, ";"))
		params := map[string]string{
			"query_sha256":   sqldb.QueryHash(statement),
			"statement_type": sqldb.FirstKeyword(statement),
		}
		if err != nil {
			rc.Audit(plugin.AuditError, params, err)
			if encErr := enc.Encode(map[string]any{"error": err.Error()}); encErr != nil {
				return encErr
			}
			continue
		}
		params["advice_count"] = strconv.Itoa(len(rows))
		rc.Audit(plugin.AuditAllowed, params, nil)
		values := make([][]any, 0, len(rows))
		for _, item := range rows {
			values = append(values, item.values())
		}
		if err := enc.Encode(map[string]any{
			"columns":   adviceColumns,
			"rows":      values,
			"rowCount":  len(values),
			"statement": statement,
		}); err != nil {
			return err
		}
	}
}
