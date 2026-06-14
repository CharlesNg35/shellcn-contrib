package escompat

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

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

func Routes(provider Provider) []plugin.Route {
	return []plugin.Route{
		{ID: routeID(provider, "overview"), Method: plugin.MethodGet, Path: "/overview", Permission: provider.Protocol + ".read", Risk: plugin.RiskSafe, AuditEvent: routeID(provider, "overview"), Handle: overview},
		{ID: routeID(provider, "indexes.tree"), Method: plugin.MethodGet, Path: "/tree/indexes", Permission: provider.Protocol + ".indexes.read", Risk: plugin.RiskSafe, AuditEvent: routeID(provider, "indexes.tree"), Handle: treeIndexes},
		{ID: routeID(provider, "indexes.list"), Method: plugin.MethodGet, Path: "/indexes", Permission: provider.Protocol + ".indexes.read", Risk: plugin.RiskSafe, AuditEvent: routeID(provider, "indexes.list"), Handle: listIndexes},
		{ID: routeID(provider, "index.overview"), Method: plugin.MethodGet, Path: "/indexes/{index}", Permission: provider.Protocol + ".indexes.read", Risk: plugin.RiskSafe, AuditEvent: routeID(provider, "index.overview"), Handle: indexOverview},
		{ID: routeID(provider, "index.create"), Method: plugin.MethodPost, Path: "/indexes", Permission: provider.Protocol + ".indexes.write", Risk: plugin.RiskWrite, AuditEvent: routeID(provider, "index.create"), Input: indexCreateSchema(), Handle: createIndex},
		{ID: routeID(provider, "index.delete"), Method: plugin.MethodDelete, Path: "/indexes/{index}", Permission: provider.Protocol + ".indexes.delete", Risk: plugin.RiskDestructive, AuditEvent: routeID(provider, "index.delete"), Handle: deleteIndex},
		{ID: routeID(provider, "index.refresh"), Method: plugin.MethodPost, Path: "/indexes/{index}/refresh", Permission: provider.Protocol + ".indexes.write", Risk: plugin.RiskWrite, AuditEvent: routeID(provider, "index.refresh"), Handle: refreshIndex},
		{ID: routeID(provider, "index.flush"), Method: plugin.MethodPost, Path: "/indexes/{index}/flush", Permission: provider.Protocol + ".indexes.write", Risk: plugin.RiskWrite, AuditEvent: routeID(provider, "index.flush"), Handle: flushIndex},
		{ID: routeID(provider, "index.close"), Method: plugin.MethodPost, Path: "/indexes/{index}/close", Permission: provider.Protocol + ".indexes.write", Risk: plugin.RiskWrite, AuditEvent: routeID(provider, "index.close"), Handle: closeIndex},
		{ID: routeID(provider, "index.open"), Method: plugin.MethodPost, Path: "/indexes/{index}/open", Permission: provider.Protocol + ".indexes.write", Risk: plugin.RiskWrite, AuditEvent: routeID(provider, "index.open"), Handle: openIndex},
		{ID: routeID(provider, "mapping.read"), Method: plugin.MethodGet, Path: "/indexes/{index}/mapping", Permission: provider.Protocol + ".mappings.read", Risk: plugin.RiskSafe, AuditEvent: routeID(provider, "mapping.read"), Handle: readMapping},
		{ID: routeID(provider, "mapping.update"), Method: plugin.MethodPut, Path: "/indexes/{index}/mapping", Permission: provider.Protocol + ".mappings.write", Risk: plugin.RiskWrite, AuditEvent: routeID(provider, "mapping.update"), Input: mappingUpdateSchema(), Handle: updateMapping},
		{ID: routeID(provider, "settings.read"), Method: plugin.MethodGet, Path: "/indexes/{index}/settings", Permission: provider.Protocol + ".settings.read", Risk: plugin.RiskSafe, AuditEvent: routeID(provider, "settings.read"), Handle: readSettings},
		{ID: routeID(provider, "settings.update"), Method: plugin.MethodPut, Path: "/indexes/{index}/settings", Permission: provider.Protocol + ".settings.write", Risk: plugin.RiskWrite, AuditEvent: routeID(provider, "settings.update"), Input: settingsUpdateSchema(), Handle: updateSettings},
		{ID: routeID(provider, "aliases.list"), Method: plugin.MethodGet, Path: "/indexes/{index}/aliases", Permission: provider.Protocol + ".aliases.read", Risk: plugin.RiskSafe, AuditEvent: routeID(provider, "aliases.list"), Handle: listAliases},
		{ID: routeID(provider, "alias.create"), Method: plugin.MethodPost, Path: "/indexes/{index}/aliases", Permission: provider.Protocol + ".aliases.write", Risk: plugin.RiskWrite, AuditEvent: routeID(provider, "alias.create"), Input: aliasCreateSchema(), Handle: createAlias},
		{ID: routeID(provider, "alias.delete"), Method: plugin.MethodDelete, Path: "/indexes/{index}/aliases/{alias}", Permission: provider.Protocol + ".aliases.delete", Risk: plugin.RiskDestructive, AuditEvent: routeID(provider, "alias.delete"), Handle: deleteAlias},
		{ID: routeID(provider, "shards.list"), Method: plugin.MethodGet, Path: "/indexes/{index}/shards", Permission: provider.Protocol + ".shards.read", Risk: plugin.RiskSafe, AuditEvent: routeID(provider, "shards.list"), Handle: listShards},
		{ID: routeID(provider, "documents.list"), Method: plugin.MethodGet, Path: "/indexes/{index}/documents", Permission: provider.Protocol + ".documents.read", Risk: plugin.RiskSafe, AuditEvent: routeID(provider, "documents.list"), Handle: listDocuments},
		{ID: routeID(provider, "document.read"), Method: plugin.MethodGet, Path: "/indexes/{index}/documents/{id}", Permission: provider.Protocol + ".documents.read", Risk: plugin.RiskSafe, AuditEvent: routeID(provider, "document.read"), Handle: readDocument},
		{ID: routeID(provider, "document.create"), Method: plugin.MethodPost, Path: "/indexes/{index}/documents", Permission: provider.Protocol + ".documents.write", Risk: plugin.RiskWrite, AuditEvent: routeID(provider, "document.create"), Input: documentCreateSchema(), Handle: createDocument},
		{ID: routeID(provider, "document.update"), Method: plugin.MethodPut, Path: "/indexes/{index}/documents/{id}", Permission: provider.Protocol + ".documents.write", Risk: plugin.RiskWrite, AuditEvent: routeID(provider, "document.update"), Input: documentUpdateSchema(), Handle: updateDocument},
		{ID: routeID(provider, "document.delete"), Method: plugin.MethodDelete, Path: "/indexes/{index}/documents/{id}", Permission: provider.Protocol + ".documents.delete", Risk: plugin.RiskDestructive, AuditEvent: routeID(provider, "document.delete"), Handle: deleteDocument},
		{ID: routeID(provider, "documents.delete_by_query"), Method: plugin.MethodPost, Path: "/indexes/{index}/delete_by_query", Permission: provider.Protocol + ".documents.delete", Risk: plugin.RiskDestructive, AuditEvent: routeID(provider, "documents.delete_by_query"), Input: deleteByQuerySchema(), Handle: deleteByQuery},
		{ID: routeID(provider, "reindex"), Method: plugin.MethodPost, Path: "/reindex", Permission: provider.Protocol + ".indexes.write", Risk: plugin.RiskWrite, AuditEvent: routeID(provider, "reindex"), Input: reindexSchema(), Handle: reindex},
		{ID: routeID(provider, "search.query"), Method: plugin.MethodWS, Path: "/search", Permission: provider.Protocol + ".search.execute", Risk: plugin.RiskSafe, AuditEvent: routeID(provider, "search.query"), Stream: searchStream},
		{ID: routeID(provider, "completion"), Method: plugin.MethodGet, Path: "/completion", Permission: provider.Protocol + ".read", Risk: plugin.RiskSafe, AuditEvent: routeID(provider, "completion"), Handle: completionRoute},
	}
}

func indexCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Index", Fields: []plugin.Field{
		{Key: "name", Label: "Index name", Type: plugin.FieldText, Required: true},
		{Key: "settings", Label: "Settings", Type: plugin.FieldJSON},
		{Key: "mappings", Label: "Mappings", Type: plugin.FieldJSON},
		{Key: "aliases", Label: "Aliases", Type: plugin.FieldJSON},
	}}}}
}

func mappingUpdateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Mapping", Fields: []plugin.Field{
		{Key: "mapping", Label: "Mapping", Type: plugin.FieldJSON, Required: true},
	}}}}
}

func documentCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Document", Fields: []plugin.Field{
		{Key: "id", Label: "Document ID", Type: plugin.FieldText},
		{Key: "document", Label: "Document", Type: plugin.FieldJSON, Required: true},
	}}}}
}

func documentUpdateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Document", Fields: []plugin.Field{
		{Key: "content", Label: "Document", Type: plugin.FieldJSON, Required: true},
	}}}}
}

func reindexSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Reindex", Fields: []plugin.Field{
		{Key: "source", Label: "Source index", Type: plugin.FieldText, Required: true},
		{Key: "destination", Label: "Destination index", Type: plugin.FieldText, Required: true},
		{Key: "query", Label: "Query filter", Type: plugin.FieldJSON},
		{Key: "wait_for_completion", Label: "Wait for completion", Type: plugin.FieldToggle, Default: true},
	}}}}
}

func searchSession(rc *plugin.RequestContext) (*Session, error) {
	return unwrap(rc.Session)
}

func overview(rc *plugin.RequestContext) (any, error) {
	s, err := searchSession(rc)
	if err != nil {
		return nil, err
	}
	var root map[string]any
	if err := s.client.Do(rc.Ctx, http.MethodGet, "/", nil, nil, &root); err != nil {
		return nil, err
	}
	var health map[string]any
	if err := s.client.Do(rc.Ctx, http.MethodGet, "/_cluster/health", nil, nil, &health); err == nil {
		root["cluster_health"] = health
	}
	return root, nil
}

func treeIndexes(rc *plugin.RequestContext) (any, error) {
	res, err := listIndexes(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name := fmt.Sprint(item["index"])
		ref := plugin.ResourceIdentity{Kind: "index", Name: name, UID: name}
		nodes = append(nodes, plugin.TreeNode{Key: "index:" + name, Label: name, Icon: icon("database"), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

func listIndexes(rc *plugin.RequestContext) (any, error) {
	s, err := searchSession(rc)
	if err != nil {
		return nil, err
	}
	q := url.Values{"format": []string{"json"}, "bytes": []string{"b"}, "expand_wildcards": []string{"all"}}
	var raw []row
	if err := s.client.Do(rc.Ctx, http.MethodGet, "/_cat/indices", q, nil, &raw); err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(raw))
	for _, r := range raw {
		name := fmt.Sprint(r["index"])
		r["docs_count"] = numericString(r["docs.count"])
		r["docs_deleted"] = numericString(r["docs.deleted"])
		r["store_size"] = numericString(r["store.size"])
		r["pri_store_size"] = numericString(r["pri.store.size"])
		r["ref"] = plugin.ResourceIdentity{Kind: "index", Name: name, UID: name}
		rows = append(rows, r)
	}
	sort.Slice(rows, func(i, j int) bool { return fmt.Sprint(rows[i]["index"]) < fmt.Sprint(rows[j]["index"]) })
	return broker.PageRows(rc, rows)
}

func indexOverview(rc *plugin.RequestContext) (any, error) {
	s, err := searchSession(rc)
	if err != nil {
		return nil, err
	}
	index := indexParam(rc)
	var info map[string]any
	if err := s.client.Do(rc.Ctx, http.MethodGet, pathIndex(index), nil, nil, &info); err != nil {
		return nil, err
	}
	var stats map[string]any
	if err := s.client.Do(rc.Ctx, http.MethodGet, pathIndex(index)+"/_stats/docs,store,indexing,search", nil, nil, &stats); err == nil {
		info["_stats"] = stats
	}
	return info, nil
}

func createIndex(rc *plugin.RequestContext) (any, error) {
	s, err := searchSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Name     string         `json:"name"`
		Settings map[string]any `json:"settings"`
		Mappings map[string]any `json:"mappings"`
		Aliases  map[string]any `json:"aliases"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	body := map[string]any{}
	if len(req.Settings) > 0 {
		body["settings"] = req.Settings
	}
	if len(req.Mappings) > 0 {
		body["mappings"] = req.Mappings
	}
	if len(req.Aliases) > 0 {
		body["aliases"] = req.Aliases
	}
	err = s.client.Do(rc.Ctx, http.MethodPut, pathIndex(req.Name), url.Values{"wait_for_active_shards": []string{"1"}}, body, nil)
	return actionResult{OK: err == nil}, err
}

func deleteIndex(rc *plugin.RequestContext) (any, error) {
	s, err := searchSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	index, err := validateIndex(indexParam(rc))
	if err != nil {
		return nil, err
	}
	err = s.client.Do(rc.Ctx, http.MethodDelete, pathIndex(index), nil, nil, nil)
	return actionResult{OK: err == nil}, err
}

func refreshIndex(rc *plugin.RequestContext) (any, error) {
	s, err := searchSession(rc)
	if err != nil {
		return nil, err
	}
	index, err := validateIndex(indexParam(rc))
	if err != nil {
		return nil, err
	}
	err = s.client.Do(rc.Ctx, http.MethodPost, pathIndex(index)+"/_refresh", nil, nil, nil)
	return actionResult{OK: err == nil}, err
}

func flushIndex(rc *plugin.RequestContext) (any, error) {
	s, err := searchSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	index, err := validateIndex(indexParam(rc))
	if err != nil {
		return nil, err
	}
	err = s.client.Do(rc.Ctx, http.MethodPost, pathIndex(index)+"/_flush", nil, nil, nil)
	return actionResult{OK: err == nil}, err
}

func closeIndex(rc *plugin.RequestContext) (any, error) {
	s, err := searchSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	index, err := validateIndex(indexParam(rc))
	if err != nil {
		return nil, err
	}
	err = s.client.Do(rc.Ctx, http.MethodPost, pathIndex(index)+"/_close", nil, nil, nil)
	return actionResult{OK: err == nil}, err
}

func openIndex(rc *plugin.RequestContext) (any, error) {
	s, err := searchSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	index, err := validateIndex(indexParam(rc))
	if err != nil {
		return nil, err
	}
	err = s.client.Do(rc.Ctx, http.MethodPost, pathIndex(index)+"/_open", nil, nil, nil)
	return actionResult{OK: err == nil}, err
}

func readMapping(rc *plugin.RequestContext) (any, error) {
	s, err := searchSession(rc)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	err = s.client.Do(rc.Ctx, http.MethodGet, pathIndex(indexParam(rc))+"/_mapping", nil, nil, &out)
	return out, err
}

func updateMapping(rc *plugin.RequestContext) (any, error) {
	s, err := searchSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	index, err := validateIndex(indexParam(rc))
	if err != nil {
		return nil, err
	}
	var req struct {
		Mapping map[string]any `json:"mapping"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	if len(req.Mapping) == 0 {
		return nil, fmt.Errorf("%w: mapping body is required", plugin.ErrInvalidInput)
	}
	err = s.client.Do(rc.Ctx, http.MethodPut, pathIndex(index)+"/_mapping", nil, req.Mapping, nil)
	return actionResult{OK: err == nil}, err
}

func readSettings(rc *plugin.RequestContext) (any, error) {
	s, err := searchSession(rc)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	err = s.client.Do(rc.Ctx, http.MethodGet, pathIndex(indexParam(rc))+"/_settings", nil, nil, &out)
	return out, err
}

func settingsUpdateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Settings", Fields: []plugin.Field{
		{Key: "settings", Label: "Settings", Type: plugin.FieldJSON, Required: true, Help: "Dynamic index settings to apply, e.g. {\"index\":{\"number_of_replicas\":1,\"refresh_interval\":\"30s\"}}."},
	}}}}
}

func updateSettings(rc *plugin.RequestContext) (any, error) {
	s, err := searchSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	index, err := validateIndex(indexParam(rc))
	if err != nil {
		return nil, err
	}
	var req struct {
		Settings map[string]any `json:"settings"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	if len(req.Settings) == 0 {
		return nil, fmt.Errorf("%w: settings body is required", plugin.ErrInvalidInput)
	}
	err = s.client.Do(rc.Ctx, http.MethodPut, pathIndex(index)+"/_settings", nil, req.Settings, nil)
	return actionResult{OK: err == nil}, err
}

func deleteByQuerySchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Delete by query", Fields: []plugin.Field{
		{Key: "query", Label: "Query", Type: plugin.FieldJSON, Required: true, Help: "Query DSL selecting the documents to delete, e.g. {\"match\":{\"status\":\"archived\"}}."},
	}}}}
}

func deleteByQuery(rc *plugin.RequestContext) (any, error) {
	s, err := searchSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	index, err := validateIndex(indexParam(rc))
	if err != nil {
		return nil, err
	}
	var req struct {
		Query map[string]any `json:"query"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	if len(req.Query) == 0 {
		return nil, fmt.Errorf("%w: a query is required to delete by query", plugin.ErrInvalidInput)
	}
	body := map[string]any{"query": req.Query}
	q := url.Values{"refresh": []string{"true"}, "conflicts": []string{"proceed"}}
	var out map[string]any
	err = s.client.Do(rc.Ctx, http.MethodPost, pathIndex(index)+"/_delete_by_query", q, body, &out)
	return out, err
}

func listAliases(rc *plugin.RequestContext) (any, error) {
	s, err := searchSession(rc)
	if err != nil {
		return nil, err
	}
	index := indexParam(rc)
	var raw map[string]struct {
		Aliases map[string]aliasDefinition `json:"aliases"`
	}
	if err := s.client.Do(rc.Ctx, http.MethodGet, pathIndex(index)+"/_alias", nil, nil, &raw); err != nil {
		if isMissing(err) {
			return plugin.Page[row]{Items: []row{}, Total: ptr(0)}, nil
		}
		return nil, err
	}
	rows := make([]row, 0)
	for idx, entry := range raw {
		for alias, def := range entry.Aliases {
			rows = append(rows, aliasRow(idx, alias, def))
		}
	}
	sort.Slice(rows, func(i, j int) bool { return fmt.Sprint(rows[i]["alias"]) < fmt.Sprint(rows[j]["alias"]) })
	return broker.PageRows(rc, rows)
}

type aliasDefinition struct {
	Filter        map[string]any `json:"filter"`
	IndexRouting  string         `json:"index_routing"`
	SearchRouting string         `json:"search_routing"`
	IsWriteIndex  bool           `json:"is_write_index"`
}

func aliasRow(index, alias string, def aliasDefinition) row {
	filter := "-"
	if len(def.Filter) > 0 {
		if data, err := json.Marshal(def.Filter); err == nil {
			filter = string(data)
		}
	}
	return row{
		"alias":          alias,
		"index":          index,
		"filter":         filter,
		"routing.index":  def.IndexRouting,
		"routing.search": def.SearchRouting,
		"is_write_index": def.IsWriteIndex,
	}
}

func createAlias(rc *plugin.RequestContext) (any, error) {
	s, err := searchSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	index, err := validateIndex(indexParam(rc))
	if err != nil {
		return nil, err
	}
	var req struct {
		Name   string         `json:"name"`
		Filter map[string]any `json:"filter"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		return nil, fmt.Errorf("%w: alias name is required", plugin.ErrInvalidInput)
	}
	var body any
	if len(req.Filter) > 0 {
		body = map[string]any{"filter": req.Filter}
	}
	err = s.client.Do(rc.Ctx, http.MethodPut, pathIndex(index)+"/_alias/"+url.PathEscape(name), nil, body, nil)
	return actionResult{OK: err == nil}, err
}

func deleteAlias(rc *plugin.RequestContext) (any, error) {
	s, err := searchSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	index, err := validateIndex(indexParam(rc))
	if err != nil {
		return nil, err
	}
	alias := strings.TrimSpace(rc.Param("alias"))
	if alias == "" {
		return nil, fmt.Errorf("%w: alias is required", plugin.ErrInvalidInput)
	}
	err = s.client.Do(rc.Ctx, http.MethodDelete, pathIndex(index)+"/_alias/"+url.PathEscape(alias), nil, nil, nil)
	return actionResult{OK: err == nil}, err
}

func aliasCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Alias", Fields: []plugin.Field{
		{Key: "name", Label: "Alias name", Type: plugin.FieldText, Required: true},
		{Key: "filter", Label: "Filter", Type: plugin.FieldJSON, Help: "Optional query DSL to make this a filtered alias."},
	}}}}
}

func listShards(rc *plugin.RequestContext) (any, error) {
	s, err := searchSession(rc)
	if err != nil {
		return nil, err
	}
	var rows []row
	if err := s.client.Do(rc.Ctx, http.MethodGet, "/_cat/shards/"+url.PathEscape(indexParam(rc)), url.Values{"format": []string{"json"}, "bytes": []string{"b"}}, nil, &rows); err != nil {
		return nil, err
	}
	for _, r := range rows {
		r["docs"] = numericString(r["docs"])
		r["store"] = numericString(r["store"])
	}
	return broker.PageRows(rc, rows)
}

func listDocuments(rc *plugin.RequestContext) (any, error) {
	s, err := searchSession(rc)
	if err != nil {
		return nil, err
	}
	req, err := rc.Page()
	if err != nil {
		return nil, err
	}
	from := 0
	if req.Cursor != "" {
		from, err = strconv.Atoi(req.Cursor)
		if err != nil || from < 0 {
			return nil, fmt.Errorf("%w: invalid cursor", plugin.ErrInvalidInput)
		}
	}
	limit := req.Limit
	if limit > s.opts.PageLimit {
		limit = s.opts.PageLimit
	}
	body := map[string]any{"query": map[string]any{"match_all": map[string]any{}}, "from": from, "size": limit}
	if q := req.Search(); q != "" {
		body["query"] = map[string]any{"query_string": map[string]any{"query": q}}
	}
	result, err := executeSearch(rc.Ctx, s, indexParam(rc), body)
	if err != nil {
		return nil, err
	}
	next := ""
	if len(result.Rows) == limit && from+limit < int(result.Total) {
		next = strconv.Itoa(from + limit)
	}
	total := int(result.Total)
	return plugin.Page[row]{Items: result.Rows, NextCursor: next, Total: &total}, nil
}

func readDocument(rc *plugin.RequestContext) (any, error) {
	s, err := searchSession(rc)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	err = s.client.Do(rc.Ctx, http.MethodGet, pathDoc(indexParam(rc), docIDParam(rc)), nil, nil, &out)
	return out, err
}

func createDocument(rc *plugin.RequestContext) (any, error) {
	s, err := searchSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	index, err := validateIndex(indexParam(rc))
	if err != nil {
		return nil, err
	}
	var req struct {
		ID       string         `json:"id"`
		Document map[string]any `json:"document"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	if len(req.Document) == 0 {
		return nil, fmt.Errorf("%w: document body is required", plugin.ErrInvalidInput)
	}
	path := pathIndex(index) + "/_doc"
	method := http.MethodPost
	if strings.TrimSpace(req.ID) != "" {
		path = pathDoc(index, req.ID)
		method = http.MethodPut
	}
	var out map[string]any
	err = s.client.Do(rc.Ctx, method, path, nil, req.Document, &out)
	return out, err
}

func updateDocument(rc *plugin.RequestContext) (any, error) {
	s, err := searchSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	index, err := validateIndex(indexParam(rc))
	if err != nil {
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
	if src, ok := doc["_source"].(map[string]any); ok {
		doc = src
	}
	if len(doc) == 0 {
		return nil, fmt.Errorf("%w: document body is required", plugin.ErrInvalidInput)
	}
	var out map[string]any
	err = s.client.Do(rc.Ctx, http.MethodPut, pathDoc(index, docIDParam(rc)), nil, doc, &out)
	return out, err
}

func deleteDocument(rc *plugin.RequestContext) (any, error) {
	s, err := searchSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	index, err := validateIndex(indexParam(rc))
	if err != nil {
		return nil, err
	}
	err = s.client.Do(rc.Ctx, http.MethodDelete, pathDoc(index, docIDParam(rc)), nil, nil, nil)
	return actionResult{OK: err == nil}, err
}

func reindex(rc *plugin.RequestContext) (any, error) {
	s, err := searchSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Source            string         `json:"source"`
		Destination       string         `json:"destination"`
		Query             map[string]any `json:"query"`
		WaitForCompletion bool           `json:"wait_for_completion"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	source, err := validateIndex(req.Source)
	if err != nil {
		return nil, err
	}
	destination, err := validateIndex(req.Destination)
	if err != nil {
		return nil, err
	}
	body := map[string]any{"source": map[string]any{"index": source}, "dest": map[string]any{"index": destination}}
	if len(req.Query) > 0 {
		body["source"].(map[string]any)["query"] = req.Query
	}
	q := url.Values{"wait_for_completion": []string{strconv.FormatBool(req.WaitForCompletion)}}
	var out map[string]any
	err = s.client.Do(rc.Ctx, http.MethodPost, "/_reindex", q, body, &out)
	return out, err
}

type searchResult struct {
	Rows  []row
	Total int64
}

func executeSearch(ctx context.Context, s *Session, index string, body map[string]any) (searchResult, error) {
	path := "/_search"
	if strings.TrimSpace(index) != "" {
		path = pathIndex(index) + "/_search"
	}
	var out map[string]any
	if err := s.client.Do(ctx, http.MethodPost, path, nil, body, &out); err != nil {
		return searchResult{}, err
	}
	hits, _ := out["hits"].(map[string]any)
	total := totalHits(hits["total"])
	rawHits, _ := hits["hits"].([]any)
	rows := make([]row, 0, len(rawHits))
	for _, item := range rawHits {
		hit, _ := item.(map[string]any)
		idx, id := fmt.Sprint(hit["_index"]), fmt.Sprint(hit["_id"])
		rows = append(rows, row{
			"_index":   idx,
			"_id":      id,
			"_score":   hit["_score"],
			"_source":  hit["_source"],
			"_version": hit["_version"],
			"ref":      plugin.ResourceIdentity{Kind: "document", Namespace: idx, Name: id, UID: idx + "/" + id},
		})
	}
	return searchResult{Rows: rows, Total: total}, nil
}

func ExecuteSearchRequest(ctx context.Context, s *Session, index string, req sqldb.QueryRequest) (sqldb.QueryResult, error) {
	var body map[string]any
	if err := json.Unmarshal([]byte(req.Query), &body); err != nil {
		return sqldb.QueryResult{}, fmt.Errorf("%w: query must be a JSON object", plugin.ErrInvalidInput)
	}
	start := time.Now()
	result, err := executeSearch(ctx, s, index, body)
	if err != nil {
		return sqldb.QueryResult{}, err
	}
	rows := make([][]any, 0, len(result.Rows))
	for _, r := range result.Rows {
		rows = append(rows, []any{r["_index"], r["_id"], r["_score"], r["_source"]})
	}
	return sqldb.QueryResult{
		Columns:    []string{"_index", "_id", "_score", "_source"},
		Rows:       rows,
		RowCount:   int64(len(rows)),
		ElapsedMS:  time.Since(start).Milliseconds(),
		Statement:  req.Query,
		CommandTag: "SEARCH",
	}, nil
}

func searchStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := searchSession(rc)
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
		result, err := ExecuteSearchRequest(client.Context(), s, indexParam(rc), req)
		rc.Audit(queryAuditResult(err), map[string]string{"query": req.Query}, err)
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
	queries := []string{
		`{"query":{"match_all":{}},"size":50}`,
		`{"query":{"query_string":{"query":"field:value"}},"size":50}`,
		`{"query":{"bool":{"must":[],"filter":[]}},"sort":[{"@timestamp":"desc"}],"size":50}`,
		`{"aggs":{"by_field":{"terms":{"field":"field.keyword"}}},"size":0}`,
	}
	items := make([]sqldb.CompletionItem, 0, len(queries))
	for _, q := range queries {
		items = append(items, sqldb.CompletionItem{Label: q, Type: "keyword", Apply: q})
	}
	return items, nil
}

func indexParam(rc *plugin.RequestContext) string {
	if v := strings.TrimSpace(rc.Param("index")); v != "" {
		return v
	}
	if v := strings.TrimSpace(rc.Query().Get("p.index")); v != "" {
		return v
	}
	if v := strings.TrimSpace(rc.Param("source")); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p.source"))
}

func docIDParam(rc *plugin.RequestContext) string {
	if v := strings.TrimSpace(rc.Param("id")); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p.id"))
}

func totalHits(v any) int64 {
	switch t := v.(type) {
	case map[string]any:
		return numericString(t["value"])
	case float64:
		return int64(t)
	default:
		return 0
	}
}

func ptr(v int) *int { return &v }

func queryAuditResult(err error) plugin.AuditResult {
	if err != nil {
		return plugin.AuditError
	}
	return plugin.AuditAllowed
}
