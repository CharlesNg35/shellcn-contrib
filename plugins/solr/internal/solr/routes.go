package solr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

func Routes() []plugin.Route {
	return []plugin.Route{
		{ID: rid("overview"), Method: plugin.MethodGet, Path: "/overview", Permission: "solr.read", Risk: plugin.RiskSafe, AuditEvent: rid("overview"), Handle: overview},
		{ID: rid("cores.tree"), Method: plugin.MethodGet, Path: "/tree/cores", Permission: "solr.cores.read", Risk: plugin.RiskSafe, AuditEvent: rid("cores.tree"), Handle: treeCores},
		{ID: rid("cores.list"), Method: plugin.MethodGet, Path: "/cores", Permission: "solr.cores.read", Risk: plugin.RiskSafe, AuditEvent: rid("cores.list"), Handle: listCores},
		{ID: rid("core.overview"), Method: plugin.MethodGet, Path: "/cores/{core}", Permission: "solr.cores.read", Risk: plugin.RiskSafe, AuditEvent: rid("core.overview"), Handle: coreOverview},
		{ID: rid("core.create"), Method: plugin.MethodPost, Path: "/cores", Permission: "solr.cores.write", Risk: plugin.RiskWrite, AuditEvent: rid("core.create"), Input: coreCreateSchema(), Handle: createCore},
		{ID: rid("core.reload"), Method: plugin.MethodPost, Path: "/cores/{core}/reload", Permission: "solr.cores.write", Risk: plugin.RiskWrite, AuditEvent: rid("core.reload"), Handle: reloadCore},
		{ID: rid("core.delete"), Method: plugin.MethodDelete, Path: "/cores/{core}", Permission: "solr.cores.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("core.delete"), Handle: deleteCore},
		{ID: rid("core.commit"), Method: plugin.MethodPost, Path: "/cores/{core}/commit", Permission: "solr.documents.write", Risk: plugin.RiskWrite, AuditEvent: rid("core.commit"), Handle: commitCore},
		{ID: rid("core.optimize"), Method: plugin.MethodPost, Path: "/cores/{core}/optimize", Permission: "solr.documents.write", Risk: plugin.RiskWrite, AuditEvent: rid("core.optimize"), Handle: optimizeCore},
		{ID: rid("core.ping"), Method: plugin.MethodGet, Path: "/cores/{core}/ping", Permission: "solr.cores.read", Risk: plugin.RiskSafe, AuditEvent: rid("core.ping"), Handle: pingCore},
		{ID: rid("documents.list"), Method: plugin.MethodGet, Path: "/cores/{core}/documents", Permission: "solr.documents.read", Risk: plugin.RiskSafe, AuditEvent: rid("documents.list"), Handle: listDocuments},
		{ID: rid("document.read"), Method: plugin.MethodGet, Path: "/cores/{core}/documents/{id}", Permission: "solr.documents.read", Risk: plugin.RiskSafe, AuditEvent: rid("document.read"), Handle: readDocument},
		{ID: rid("document.upsert"), Method: plugin.MethodPost, Path: "/cores/{core}/documents", Permission: "solr.documents.write", Risk: plugin.RiskWrite, AuditEvent: rid("document.upsert"), Input: documentUpsertSchema(), Handle: upsertDocument},
		{ID: rid("document.update"), Method: plugin.MethodPatch, Path: "/cores/{core}/documents/{id}", Permission: "solr.documents.write", Risk: plugin.RiskWrite, AuditEvent: rid("document.update"), Handle: updateDocument},
		{ID: rid("document.delete"), Method: plugin.MethodDelete, Path: "/cores/{core}/documents/{id}", Permission: "solr.documents.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("document.delete"), Handle: deleteDocument},
		{ID: rid("documents.delete_query"), Method: plugin.MethodPost, Path: "/cores/{core}/documents/delete", Permission: "solr.documents.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("documents.delete_query"), Input: deleteQuerySchema(), Handle: deleteByQuery},
		{ID: rid("schema.read"), Method: plugin.MethodGet, Path: "/cores/{core}/schema", Permission: "solr.schema.read", Risk: plugin.RiskSafe, AuditEvent: rid("schema.read"), Handle: readSchema},
		{ID: rid("schema.fields"), Method: plugin.MethodGet, Path: "/cores/{core}/schema/fields", Permission: "solr.schema.read", Risk: plugin.RiskSafe, AuditEvent: rid("schema.fields"), Handle: listFields},
		{ID: rid("schema.field.read"), Method: plugin.MethodGet, Path: "/cores/{core}/schema/fields/{field}", Permission: "solr.schema.read", Risk: plugin.RiskSafe, AuditEvent: rid("schema.field.read"), Handle: readField},
		{ID: rid("schema.field.add"), Method: plugin.MethodPost, Path: "/cores/{core}/schema/fields", Permission: "solr.schema.write", Risk: plugin.RiskWrite, AuditEvent: rid("schema.field.add"), Input: fieldAddSchema(), Handle: addField},
		{ID: rid("schema.field.replace"), Method: plugin.MethodPut, Path: "/cores/{core}/schema/fields/{field}", Permission: "solr.schema.write", Risk: plugin.RiskWrite, AuditEvent: rid("schema.field.replace"), Input: fieldReplaceSchema(), Handle: replaceField},
		{ID: rid("schema.field.delete"), Method: plugin.MethodDelete, Path: "/cores/{core}/schema/fields/{field}", Permission: "solr.schema.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("schema.field.delete"), Handle: deleteField},
		{ID: rid("config.read"), Method: plugin.MethodGet, Path: "/cores/{core}/config", Permission: "solr.config.read", Risk: plugin.RiskSafe, AuditEvent: rid("config.read"), Handle: readConfig},
		{ID: rid("search.query"), Method: plugin.MethodWS, Path: "/cores/{core}/search", Permission: "solr.search.execute", Risk: plugin.RiskSafe, AuditEvent: rid("search.query"), Stream: searchStream},
		{ID: rid("completion"), Method: plugin.MethodGet, Path: "/completion", Permission: "solr.read", Risk: plugin.RiskSafe, AuditEvent: rid("completion"), Handle: completionRoute},
	}
}

func coreCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Collection / core", Fields: []plugin.Field{
		{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true},
		{Key: "config_set", Label: "Config set", Type: plugin.FieldText, Default: "_default", Placeholder: "_default"},
	}}}}
}

func documentUpsertSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Document", Fields: []plugin.Field{
		{Key: "document", Label: "Document", Type: plugin.FieldJSON, Required: true, Default: map[string]any{"id": "example", "title_s": "Example"}},
		{Key: "commit", Label: "Commit immediately", Type: plugin.FieldToggle, Default: true},
	}}}}
}

func deleteQuerySchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Delete", Fields: []plugin.Field{
		{Key: "query", Label: "Query", Type: plugin.FieldText, Required: true, Default: "*:*"},
		{Key: "commit", Label: "Commit immediately", Type: plugin.FieldToggle, Default: true},
	}}}}
}

func fieldAddSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Field", Fields: []plugin.Field{
		{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true},
		{Key: "type", Label: "Type", Type: plugin.FieldText, Required: true, Default: "string"},
		{Key: "indexed", Label: "Indexed", Type: plugin.FieldToggle, Default: true},
		{Key: "stored", Label: "Stored", Type: plugin.FieldToggle, Default: true},
		{Key: "multi_valued", Label: "Multi-valued", Type: plugin.FieldToggle},
		{Key: "required", Label: "Required", Type: plugin.FieldToggle},
	}}}}
}

func fieldReplaceSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Field", Fields: []plugin.Field{
		{Key: "type", Label: "Type", Type: plugin.FieldText, Required: true, Default: "string"},
		{Key: "indexed", Label: "Indexed", Type: plugin.FieldToggle, Default: true},
		{Key: "stored", Label: "Stored", Type: plugin.FieldToggle, Default: true},
		{Key: "multi_valued", Label: "Multi-valued", Type: plugin.FieldToggle},
		{Key: "required", Label: "Required", Type: plugin.FieldToggle},
	}}}}
}

func session(rc *plugin.RequestContext) (*Session, error) { return unwrap(rc.Session) }

func overview(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	out := row{}
	_ = s.client.Do(rc.Ctx, http.MethodGet, "/admin/info/system", url.Values{"wt": []string{"json"}}, nil, ptrMap(out, "system"))
	_ = s.client.Do(rc.Ctx, http.MethodGet, "/admin/cores", url.Values{"action": []string{"STATUS"}, "wt": []string{"json"}}, nil, ptrMap(out, "cores"))
	return out, nil
}

func treeCores(rc *plugin.RequestContext) (any, error) {
	res, err := listCores(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name := fmt.Sprint(item["name"])
		ref := plugin.ResourceIdentity{Kind: "core", Name: name, UID: name}
		nodes = append(nodes, plugin.TreeNode{Key: "core:" + name, Label: name, Icon: icon("database"), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, Total: page.Total}, nil
}

func listCores(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	status, err := adminStatus(rc.Ctx, s)
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(status))
	for name, item := range status {
		r := coreRow(name, item)
		r["ref"] = plugin.ResourceIdentity{Kind: "core", Name: name, UID: name}
		rows = append(rows, r)
	}
	return broker.PageRows(rc, rows)
}

func coreOverview(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	status, err := adminStatus(rc.Ctx, s)
	if err != nil {
		return nil, err
	}
	core := coreParam(rc)
	if item, ok := status[core]; ok {
		return item, nil
	}
	return nil, fmt.Errorf("%w: core %q", plugin.ErrNotFound, core)
}

func createCore(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Name      string `json:"name"`
		ConfigSet string `json:"config_set"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	if s.isSolrCloud() {
		q := url.Values{
			"action":            []string{"CREATE"},
			"name":              []string{strings.TrimSpace(req.Name)},
			"numShards":         []string{"1"},
			"replicationFactor": []string{"1"},
			"wt":                []string{"json"},
		}
		if req.ConfigSet != "" {
			q.Set("collection.configName", req.ConfigSet)
		}
		var out row
		err = s.client.Do(rc.Ctx, http.MethodPost, "/admin/collections", q, nil, &out)
		return out, err
	}
	q := url.Values{"action": []string{"CREATE"}, "name": []string{strings.TrimSpace(req.Name)}, "wt": []string{"json"}}
	if req.ConfigSet != "" {
		q.Set("configSet", req.ConfigSet)
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodPost, "/admin/cores", q, nil, &out)
	return out, err
}

func reloadCore(rc *plugin.RequestContext) (any, error) {
	return coreAdminWrite(rc, "RELOAD", nil)
}

func deleteCore(rc *plugin.RequestContext) (any, error) {
	return coreAdminWrite(rc, "UNLOAD", url.Values{"deleteIndex": []string{"true"}, "deleteDataDir": []string{"true"}, "deleteInstanceDir": []string{"true"}})
}

func coreAdminWrite(rc *plugin.RequestContext, action string, extra url.Values) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	if s.isSolrCloud() {
		collectionAction := action
		if action == "UNLOAD" {
			collectionAction = "DELETE"
		}
		q := url.Values{"action": []string{collectionAction}, "name": []string{coreParam(rc)}, "wt": []string{"json"}}
		var out row
		err = s.client.Do(rc.Ctx, http.MethodPost, "/admin/collections", q, nil, &out)
		return out, err
	}
	q := url.Values{"action": []string{action}, "core": []string{coreParam(rc)}, "wt": []string{"json"}}
	for k, vals := range extra {
		for _, v := range vals {
			q.Add(k, v)
		}
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodPost, "/admin/cores", q, nil, &out)
	return out, err
}

func commitCore(rc *plugin.RequestContext) (any, error) {
	return updateCommand(rc, url.Values{"commit": []string{"true"}, "wt": []string{"json"}})
}

func optimizeCore(rc *plugin.RequestContext) (any, error) {
	return updateCommand(rc, url.Values{"optimize": []string{"true"}, "wt": []string{"json"}})
}

func updateCommand(rc *plugin.RequestContext, q url.Values) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodPost, corePath(coreParam(rc), "/update"), q, map[string]any{}, &out)
	return out, err
}

func pingCore(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodGet, corePath(coreParam(rc), "/admin/ping"), url.Values{"wt": []string{"json"}}, nil, &out)
	return out, err
}

func listDocuments(rc *plugin.RequestContext) (any, error) {
	s, req, err := paged(rc)
	if err != nil {
		return nil, err
	}
	start := startCursor(req.Cursor)
	limit := limitFor(s, req.Limit)
	q := url.Values{
		"q":     []string{"*:*"},
		"start": []string{strconv.Itoa(start)},
		"rows":  []string{strconv.Itoa(limit)},
		"wt":    []string{"json"},
	}
	if filter := req.Search(); filter != "" {
		q.Set("q", filter)
	}
	result, err := searchDocuments(rc.Ctx, s, coreParam(rc), q)
	if err != nil {
		return nil, err
	}
	next := ""
	if len(result.Rows) == limit && int64(start+limit) < result.Total {
		next = strconv.Itoa(start + limit)
	}
	total := int(result.Total)
	return plugin.Page[row]{Items: result.Rows, NextCursor: next, Total: &total}, nil
}

func readDocument(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	result, err := searchDocuments(rc.Ctx, s, coreParam(rc), url.Values{"q": []string{idQuery(docParam(rc))}, "rows": []string{"1"}, "wt": []string{"json"}})
	if err != nil {
		return nil, err
	}
	if len(result.Rows) == 0 {
		return nil, fmt.Errorf("%w: document %q", plugin.ErrNotFound, docParam(rc))
	}
	return result.Rows[0]["_source"], nil
}

func upsertDocument(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Document map[string]any `json:"document"`
		Commit   bool           `json:"commit"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	q := url.Values{"wt": []string{"json"}}
	if req.Commit {
		q.Set("commit", "true")
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodPost, corePath(coreParam(rc), "/update/json/docs"), q, req.Document, &out)
	return out, err
}

func updateDocument(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Content string `json:"content"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(req.Content), &doc); err != nil {
		return nil, fmt.Errorf("%w: document content must be valid JSON", plugin.ErrInvalidInput)
	}
	if _, ok := doc["id"]; !ok {
		doc["id"] = docParam(rc)
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodPost, corePath(coreParam(rc), "/update/json/docs"), url.Values{"commit": []string{"true"}, "wt": []string{"json"}}, doc, &out)
	return out, err
}

func deleteDocument(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	body := row{"delete": row{"id": docParam(rc)}}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodPost, corePath(coreParam(rc), "/update"), url.Values{"commit": []string{"true"}, "wt": []string{"json"}}, body, &out)
	return out, err
}

func deleteByQuery(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Query  string `json:"query"`
		Commit bool   `json:"commit"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	q := url.Values{"wt": []string{"json"}}
	if req.Commit {
		q.Set("commit", "true")
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodPost, corePath(coreParam(rc), "/update"), q, row{"delete": row{"query": req.Query}}, &out)
	return out, err
}

func readSchema(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodGet, corePath(coreParam(rc), "/schema"), url.Values{"wt": []string{"json"}}, nil, &out)
	return out, err
}

func listFields(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	rows, err := schemaFields(rc.Ctx, s, coreParam(rc))
	if err != nil {
		return nil, err
	}
	for _, item := range rows {
		name := fmt.Sprint(item["name"])
		item["ref"] = plugin.ResourceIdentity{Kind: "field", Namespace: coreParam(rc), Name: name, UID: coreParam(rc) + "/" + name}
	}
	return broker.PageRows(rc, rows)
}

func readField(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodGet, corePath(coreParam(rc), "/schema/fields/"+url.PathEscape(fieldParam(rc))), url.Values{"wt": []string{"json"}}, nil, &out)
	return out, err
}

type fieldSpec struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Indexed     bool   `json:"indexed"`
	Stored      bool   `json:"stored"`
	MultiValued bool   `json:"multi_valued"`
	Required    bool   `json:"required"`
}

// fieldDefinition builds the Solr field definition body for add-field/replace-field.
// Solr requires the complete definition; multiValued/required are emitted only when set.
func fieldDefinition(spec fieldSpec) row {
	field := row{"name": strings.TrimSpace(spec.Name), "type": strings.TrimSpace(spec.Type), "indexed": spec.Indexed, "stored": spec.Stored}
	if spec.MultiValued {
		field["multiValued"] = true
	}
	if spec.Required {
		field["required"] = true
	}
	return field
}

func validFieldDefinition(spec fieldSpec) error {
	if strings.TrimSpace(spec.Name) == "" {
		return fmt.Errorf("%w: field name is required", plugin.ErrInvalidInput)
	}
	if strings.TrimSpace(spec.Type) == "" {
		return fmt.Errorf("%w: field type is required", plugin.ErrInvalidInput)
	}
	return nil
}

func addField(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var spec fieldSpec
	if err := rc.Bind(&spec); err != nil {
		return nil, err
	}
	if err := validFieldDefinition(spec); err != nil {
		return nil, err
	}
	return schemaCommand(rc, s, "add-field", fieldDefinition(spec))
}

func replaceField(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var spec fieldSpec
	if err := rc.Bind(&spec); err != nil {
		return nil, err
	}
	spec.Name = fieldParam(rc)
	if err := validFieldDefinition(spec); err != nil {
		return nil, err
	}
	return schemaCommand(rc, s, "replace-field", fieldDefinition(spec))
}

func deleteField(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	field := strings.TrimSpace(fieldParam(rc))
	if field == "" {
		return nil, fmt.Errorf("%w: field name is required", plugin.ErrInvalidInput)
	}
	return schemaCommand(rc, s, "delete-field", row{"name": field})
}

func schemaCommand(rc *plugin.RequestContext, s *Session, command string, body row) (any, error) {
	core := strings.TrimSpace(coreParam(rc))
	if core == "" {
		return nil, fmt.Errorf("%w: collection or core is required", plugin.ErrInvalidInput)
	}
	var out row
	err := s.client.Do(rc.Ctx, http.MethodPost, corePath(core, "/schema"), url.Values{"wt": []string{"json"}}, row{command: body}, &out)
	return out, err
}

func readConfig(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodGet, corePath(coreParam(rc), "/config"), url.Values{"wt": []string{"json"}}, nil, &out)
	return out, err
}

type searchResult struct {
	Rows  []row
	Total int64
}

func searchDocuments(ctx context.Context, s *Session, core string, query url.Values) (searchResult, error) {
	var out struct {
		Response struct {
			NumFound int64            `json:"numFound"`
			Docs     []map[string]any `json:"docs"`
		} `json:"response"`
	}
	if err := s.client.Do(ctx, http.MethodGet, corePath(core, "/select"), query, nil, &out); err != nil {
		return searchResult{}, err
	}
	rows := make([]row, 0, len(out.Response.Docs))
	for _, doc := range out.Response.Docs {
		id := documentID(doc)
		rows = append(rows, row{
			"_core":   core,
			"_id":     id,
			"_score":  numeric(doc["score"]),
			"_source": doc,
			"ref":     plugin.ResourceIdentity{Kind: "document", Namespace: core, Name: id, UID: core + "/" + id},
		})
	}
	return searchResult{Rows: rows, Total: out.Response.NumFound}, nil
}

func executeSearch(ctx context.Context, s *Session, core string, body map[string]any) (sqldb.QueryResult, error) {
	start := time.Now()
	query := queryValues(body)
	if query.Get("q") == "" {
		query.Set("q", "*:*")
	}
	if query.Get("wt") == "" {
		query.Set("wt", "json")
	}
	result, err := searchDocuments(ctx, s, core, query)
	if err != nil {
		return sqldb.QueryResult{}, err
	}
	rows := make([][]any, 0, len(result.Rows))
	for _, r := range result.Rows {
		rows = append(rows, []any{r["_id"], r["_score"], r["_source"]})
	}
	return sqldb.QueryResult{
		Columns:    []string{"id", "score", "document"},
		Rows:       rows,
		RowCount:   result.Total,
		ElapsedMS:  time.Since(start).Milliseconds(),
		Statement:  mustJSON(body),
		CommandTag: "SEARCH",
	}, nil
}

func searchStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
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
			if err := enc.Encode(map[string]any{"error": "Invalid search request."}); err != nil {
				return err
			}
			continue
		}
		var body map[string]any
		if err := json.Unmarshal([]byte(req.Query), &body); err != nil {
			_ = enc.Encode(map[string]any{"error": "Query must be a JSON object."})
			continue
		}
		result, err := executeSearch(client.Context(), s, coreParam(rc), body)
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

func completionRoute(*plugin.RequestContext) (any, error) {
	return []sqldb.CompletionItem{
		{Label: `{"q":"*:*","rows":50}`, Type: "keyword", Apply: `{"q":"*:*","rows":50}`},
		{Label: `{"q":"title:docker","fq":"status:active","rows":50}`, Type: "keyword", Apply: `{"q":"title:docker","fq":"status:active","rows":50}`},
		{Label: `{"q":"*:*","sort":"id asc","fl":"id,score,*","rows":50}`, Type: "keyword", Apply: `{"q":"*:*","sort":"id asc","fl":"id,score,*","rows":50}`},
		{Label: `{"q":"*:*","facet":"true","facet.field":"category_s","rows":0}`, Type: "keyword", Apply: `{"q":"*:*","facet":"true","facet.field":"category_s","rows":0}`},
	}, nil
}

func adminStatus(ctx context.Context, s *Session) (map[string]map[string]any, error) {
	if s.isSolrCloud() {
		return collectionStatus(ctx, s)
	}
	return coreStatus(ctx, s)
}

func coreStatus(ctx context.Context, s *Session) (map[string]map[string]any, error) {
	var out struct {
		Status map[string]map[string]any `json:"status"`
	}
	if err := s.client.Do(ctx, http.MethodGet, "/admin/cores", url.Values{"action": []string{"STATUS"}, "wt": []string{"json"}}, nil, &out); err != nil {
		return nil, err
	}
	return out.Status, nil
}

func collectionStatus(ctx context.Context, s *Session) (map[string]map[string]any, error) {
	var out struct {
		Cluster struct {
			Collections map[string]map[string]any `json:"collections"`
		} `json:"cluster"`
	}
	if err := s.client.Do(ctx, http.MethodGet, "/admin/collections", url.Values{"action": []string{"CLUSTERSTATUS"}, "wt": []string{"json"}}, nil, &out); err != nil {
		return nil, err
	}
	return out.Cluster.Collections, nil
}

func coreRow(name string, item map[string]any) row {
	index, _ := item["index"].(map[string]any)
	r := row{"name": name}
	if health, ok := item["health"]; ok {
		r["health"] = health
		r["mode"] = "collection"
		shards, _ := item["shards"].(map[string]any)
		r["shards"] = len(shards)
		return r
	}
	r["mode"] = "core"
	for _, key := range []string{"numDocs", "maxDoc", "deletedDocs", "size", "sizeInBytes"} {
		if value, ok := index[key]; ok {
			r[key] = value
		}
	}
	if uptime, ok := item["uptime"]; ok {
		r["uptime"] = uptime
	}
	return r
}

func (s *Session) isSolrCloud() bool {
	return strings.EqualFold(s.mode, "solrcloud")
}

func schemaFields(ctx context.Context, s *Session, core string) ([]row, error) {
	var out struct {
		Fields []row `json:"fields"`
	}
	if err := s.client.Do(ctx, http.MethodGet, corePath(core, "/schema/fields"), url.Values{"wt": []string{"json"}}, nil, &out); err != nil {
		return nil, err
	}
	return out.Fields, nil
}

func paged(rc *plugin.RequestContext) (*Session, plugin.PageRequest, error) {
	s, err := session(rc)
	if err != nil {
		return nil, plugin.PageRequest{}, err
	}
	req, err := rc.Page()
	return s, req, err
}

func limitFor(s *Session, limit int) int {
	if limit > s.opts.PageLimit {
		return s.opts.PageLimit
	}
	return limit
}

func startCursor(cursor string) int {
	n, _ := strconv.Atoi(strings.TrimSpace(cursor))
	if n < 0 {
		return 0
	}
	return n
}

func queryValues(body map[string]any) url.Values {
	q := url.Values{}
	for key, value := range body {
		switch t := value.(type) {
		case nil:
		case string:
			q.Set(key, t)
		case []any:
			for _, item := range t {
				q.Add(key, fmt.Sprint(item))
			}
		default:
			q.Set(key, fmt.Sprint(t))
		}
	}
	return q
}

func ptrMap(parent row, key string) *map[string]any {
	child := map[string]any{}
	parent[key] = child
	return &child
}

func coreParam(rc *plugin.RequestContext) string {
	if v := strings.TrimSpace(rc.Param("core")); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p.core"))
}

func docParam(rc *plugin.RequestContext) string {
	if v := strings.TrimSpace(rc.Param("id")); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p.id"))
}

func fieldParam(rc *plugin.RequestContext) string {
	if v := strings.TrimSpace(rc.Param("field")); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p.field"))
}

func corePath(core, suffix string) string {
	return "/" + url.PathEscape(core) + suffix
}

func idQuery(id string) string {
	return `id:"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(id) + `"`
}

func documentID(doc map[string]any) string {
	for _, key := range []string{"id", "_id"} {
		if value := strings.TrimSpace(fmt.Sprint(doc[key])); value != "" && value != "<nil>" {
			return value
		}
	}
	data, _ := json.Marshal(doc)
	return string(data)
}

func numeric(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int64:
		return float64(t)
	case int:
		return float64(t)
	default:
		return 0
	}
}

func mustJSON(v any) string {
	data, _ := json.Marshal(v)
	return string(data)
}

func auditResult(err error) plugin.AuditResult {
	if err != nil {
		return plugin.AuditError
	}
	return plugin.AuditAllowed
}
