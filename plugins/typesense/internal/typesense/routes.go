package typesense

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
		{ID: rid("overview"), Method: plugin.MethodGet, Path: "/overview", Permission: "typesense.read", Risk: plugin.RiskSafe, AuditEvent: rid("overview"), Handle: overview},
		{ID: rid("collections.tree"), Method: plugin.MethodGet, Path: "/tree/collections", Permission: "typesense.collections.read", Risk: plugin.RiskSafe, AuditEvent: rid("collections.tree"), Handle: treeCollections},
		{ID: rid("aliases.tree"), Method: plugin.MethodGet, Path: "/tree/aliases", Permission: "typesense.aliases.read", Risk: plugin.RiskSafe, AuditEvent: rid("aliases.tree"), Handle: treeAliases},
		{ID: rid("keys.tree"), Method: plugin.MethodGet, Path: "/tree/keys", Permission: "typesense.keys.read", Risk: plugin.RiskSafe, AuditEvent: rid("keys.tree"), Handle: treeKeys},
		{ID: rid("collections.list"), Method: plugin.MethodGet, Path: "/collections", Permission: "typesense.collections.read", Risk: plugin.RiskSafe, AuditEvent: rid("collections.list"), Handle: listCollections},
		{ID: rid("collection.overview"), Method: plugin.MethodGet, Path: "/collections/{collection}", Permission: "typesense.collections.read", Risk: plugin.RiskSafe, AuditEvent: rid("collection.overview"), Handle: collectionOverview},
		{ID: rid("collection.create"), Method: plugin.MethodPost, Path: "/collections", Permission: "typesense.collections.write", Risk: plugin.RiskWrite, AuditEvent: rid("collection.create"), Input: collectionCreateSchema(), Handle: createCollection},
		{ID: rid("collection.clone"), Method: plugin.MethodPost, Path: "/collections/clone", Permission: "typesense.collections.write", Risk: plugin.RiskWrite, AuditEvent: rid("collection.clone"), Input: collectionCloneSchema(), Handle: cloneCollection},
		{ID: rid("collection.update"), Method: plugin.MethodPatch, Path: "/collections/{collection}", Permission: "typesense.collections.write", Risk: plugin.RiskWrite, AuditEvent: rid("collection.update"), Input: collectionUpdateSchema(), Handle: updateCollection},
		{ID: rid("collection.delete"), Method: plugin.MethodDelete, Path: "/collections/{collection}", Permission: "typesense.collections.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("collection.delete"), Handle: deleteCollection},
		{ID: rid("documents.list"), Method: plugin.MethodGet, Path: "/collections/{collection}/documents", Permission: "typesense.documents.read", Risk: plugin.RiskSafe, AuditEvent: rid("documents.list"), Handle: listDocuments},
		{ID: rid("document.read"), Method: plugin.MethodGet, Path: "/collections/{collection}/documents/{id}", Permission: "typesense.documents.read", Risk: plugin.RiskSafe, AuditEvent: rid("document.read"), Handle: readDocument},
		{ID: rid("document.upsert"), Method: plugin.MethodPost, Path: "/collections/{collection}/documents", Permission: "typesense.documents.write", Risk: plugin.RiskWrite, AuditEvent: rid("document.upsert"), Input: documentUpsertSchema(), Handle: upsertDocument},
		{ID: rid("document.update"), Method: plugin.MethodPatch, Path: "/collections/{collection}/documents/{id}", Permission: "typesense.documents.write", Risk: plugin.RiskWrite, AuditEvent: rid("document.update"), Handle: updateDocument},
		{ID: rid("document.delete"), Method: plugin.MethodDelete, Path: "/collections/{collection}/documents/{id}", Permission: "typesense.documents.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("document.delete"), Handle: deleteDocument},
		{ID: rid("documents.import"), Method: plugin.MethodPost, Path: "/collections/{collection}/documents/import", Permission: "typesense.documents.write", Risk: plugin.RiskWrite, AuditEvent: rid("documents.import"), Input: documentsImportSchema(), Handle: importDocuments},
		{ID: rid("documents.export"), Method: plugin.MethodGet, Path: "/collections/{collection}/documents/export", Permission: "typesense.documents.read", Risk: plugin.RiskSafe, AuditEvent: rid("documents.export"), Handle: exportDocuments},
		{ID: rid("aliases.list"), Method: plugin.MethodGet, Path: "/aliases", Permission: "typesense.aliases.read", Risk: plugin.RiskSafe, AuditEvent: rid("aliases.list"), Handle: listAliases},
		{ID: rid("alias.read"), Method: plugin.MethodGet, Path: "/aliases/{alias}", Permission: "typesense.aliases.read", Risk: plugin.RiskSafe, AuditEvent: rid("alias.read"), Handle: readAlias},
		{ID: rid("alias.upsert"), Method: plugin.MethodPut, Path: "/aliases", Permission: "typesense.aliases.write", Risk: plugin.RiskWrite, AuditEvent: rid("alias.upsert"), Input: aliasUpsertSchema(), Handle: upsertAlias},
		{ID: rid("alias.delete"), Method: plugin.MethodDelete, Path: "/aliases/{alias}", Permission: "typesense.aliases.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("alias.delete"), Handle: deleteAlias},
		{ID: rid("synonyms.list"), Method: plugin.MethodGet, Path: "/synonym_sets", Permission: "typesense.synonyms.read", Risk: plugin.RiskSafe, AuditEvent: rid("synonyms.list"), Handle: listSynonyms},
		{ID: rid("synonym.upsert"), Method: plugin.MethodPut, Path: "/synonym_sets/{synonym}", Permission: "typesense.synonyms.write", Risk: plugin.RiskWrite, AuditEvent: rid("synonym.upsert"), Input: synonymUpsertSchema(), Handle: upsertSynonym},
		{ID: rid("synonym.delete"), Method: plugin.MethodDelete, Path: "/synonym_sets/{synonym}", Permission: "typesense.synonyms.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("synonym.delete"), Handle: deleteSynonym},
		{ID: rid("overrides.list"), Method: plugin.MethodGet, Path: "/curation_sets", Permission: "typesense.overrides.read", Risk: plugin.RiskSafe, AuditEvent: rid("overrides.list"), Handle: listOverrides},
		{ID: rid("override.upsert"), Method: plugin.MethodPut, Path: "/curation_sets/{override}", Permission: "typesense.overrides.write", Risk: plugin.RiskWrite, AuditEvent: rid("override.upsert"), Input: overrideUpsertSchema(), Handle: upsertOverride},
		{ID: rid("override.delete"), Method: plugin.MethodDelete, Path: "/curation_sets/{override}", Permission: "typesense.overrides.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("override.delete"), Handle: deleteOverride},
		{ID: rid("keys.list"), Method: plugin.MethodGet, Path: "/keys", Permission: "typesense.keys.read", Risk: plugin.RiskSafe, AuditEvent: rid("keys.list"), Handle: listKeys},
		{ID: rid("key.read"), Method: plugin.MethodGet, Path: "/keys/{key}", Permission: "typesense.keys.read", Risk: plugin.RiskSafe, AuditEvent: rid("key.read"), Handle: readKey},
		{ID: rid("key.create"), Method: plugin.MethodPost, Path: "/keys", Permission: "typesense.keys.write", Risk: plugin.RiskWrite, AuditEvent: rid("key.create"), Input: keyCreateSchema(), Handle: createKey},
		{ID: rid("key.delete"), Method: plugin.MethodDelete, Path: "/keys/{key}", Permission: "typesense.keys.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("key.delete"), Handle: deleteKey},
		{ID: rid("search.query"), Method: plugin.MethodWS, Path: "/search", Permission: "typesense.search.execute", Risk: plugin.RiskSafe, AuditEvent: rid("search.query"), Stream: searchStream},
		{ID: rid("completion"), Method: plugin.MethodGet, Path: "/completion", Permission: "typesense.read", Risk: plugin.RiskSafe, AuditEvent: rid("completion"), Handle: completionRoute},
	}
}

func collectionCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Collection", Fields: []plugin.Field{
		{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true},
		{Key: "fields", Label: "Fields", Type: plugin.FieldArray, Required: true, MinItems: 1, ItemLabel: "Field", Item: &plugin.Field{Type: plugin.FieldObject, Fields: []plugin.Field{
			{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true},
			{Key: "type", Label: "Type", Type: plugin.FieldSelect, Required: true, Default: "string", Options: []plugin.Option{
				{Label: "string", Value: "string"},
				{Label: "int32", Value: "int32"},
				{Label: "int64", Value: "int64"},
				{Label: "float", Value: "float"},
				{Label: "bool", Value: "bool"},
				{Label: "string[]", Value: "string[]"},
				{Label: "int32[]", Value: "int32[]"},
				{Label: "int64[]", Value: "int64[]"},
				{Label: "float[]", Value: "float[]"},
				{Label: "bool[]", Value: "bool[]"},
				{Label: "object", Value: "object"},
				{Label: "object[]", Value: "object[]"},
				{Label: "geopoint", Value: "geopoint"},
				{Label: "auto", Value: "auto"},
			}},
			{Key: "facet", Label: "Facet", Type: plugin.FieldToggle},
			{Key: "optional", Label: "Optional", Type: plugin.FieldToggle},
			{Key: "sort", Label: "Sortable", Type: plugin.FieldToggle},
			{Key: "index", Label: "Index", Type: plugin.FieldToggle, Default: true},
		}}},
		{Key: "default_sorting_field", Label: "Default sorting field", Type: plugin.FieldText},
		{Key: "enable_nested_fields", Label: "Enable nested fields", Type: plugin.FieldToggle},
	}}}}
}

func collectionCloneSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Clone", Fields: []plugin.Field{
		{Key: "source", Label: "Source collection", Type: plugin.FieldText, Required: true},
		{Key: "name", Label: "New collection", Type: plugin.FieldText, Required: true},
		{Key: "copy_documents", Label: "Copy documents", Type: plugin.FieldToggle},
	}}}}
}

func collectionUpdateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Schema patch", Fields: []plugin.Field{
		{Key: "schema", Label: "Schema patch", Type: plugin.FieldJSON, Required: true},
	}}}}
}

func documentUpsertSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Document", Fields: []plugin.Field{
		{Key: "document", Label: "Document", Type: plugin.FieldJSON, Required: true},
		{Key: "action", Label: "Action", Type: plugin.FieldSelect, Required: true, Default: "upsert", Options: []plugin.Option{
			{Label: "Upsert", Value: "upsert"},
			{Label: "Create", Value: "create"},
			{Label: "Update", Value: "update"},
			{Label: "Emplace", Value: "emplace"},
		}},
	}}}}
}

func documentsImportSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Import", Fields: []plugin.Field{
		{Key: "documents", Label: "JSONL documents", Type: plugin.FieldTextarea, Required: true},
		{Key: "action", Label: "Action", Type: plugin.FieldSelect, Required: true, Default: "upsert", Options: []plugin.Option{
			{Label: "Upsert", Value: "upsert"}, {Label: "Create", Value: "create"}, {Label: "Update", Value: "update"}, {Label: "Emplace", Value: "emplace"}, {Label: "Delete", Value: "delete"},
		}},
	}}}}
}

func aliasUpsertSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Alias", Fields: []plugin.Field{
		{Key: "name", Label: "Alias", Type: plugin.FieldText, Required: true},
		{Key: "collection_name", Label: "Collection", Type: plugin.FieldText, Required: true},
	}}}}
}

func synonymUpsertSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Synonym set", Fields: []plugin.Field{
		{Key: "id", Label: "Set name", Type: plugin.FieldText, Required: true},
		{Key: "synonym", Label: "Synonym set", Type: plugin.FieldJSON, Required: true, Default: map[string]any{"items": []any{map[string]any{"id": "example", "synonyms": []any{"word", "term"}}}}},
	}}}}
}

func overrideUpsertSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Curation set", Fields: []plugin.Field{
		{Key: "id", Label: "Set name", Type: plugin.FieldText, Required: true},
		{Key: "override", Label: "Curation set", Type: plugin.FieldJSON, Required: true, Default: map[string]any{"items": []any{map[string]any{"id": "pin-result", "rule": map[string]any{"query": "query", "match": "exact"}}}}},
	}}}}
}

func keyCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Key", Fields: []plugin.Field{
		{Key: "description", Label: "Description", Type: plugin.FieldText, Required: true},
		{Key: "actions", Label: "Actions", Type: plugin.FieldArray, Required: true, MinItems: 1, ItemLabel: "Action", Default: []any{"documents:search"}, Item: &plugin.Field{Type: plugin.FieldText}},
		{Key: "collections", Label: "Collections", Type: plugin.FieldArray, Required: true, MinItems: 1, ItemLabel: "Collection", Default: []any{"*"}, Item: &plugin.Field{Type: plugin.FieldText}},
		{Key: "expires_at", Label: "Expires at unix timestamp", Type: plugin.FieldNumber},
	}}}}
}

func session(rc *plugin.RequestContext) (*Session, error) { return unwrap(rc.Session) }

func overview(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	_ = s.client.Do(rc.Ctx, http.MethodGet, "/health", nil, nil, ptrMap(out, "health"))
	_ = s.client.Do(rc.Ctx, http.MethodGet, "/stats.json", nil, nil, ptrMap(out, "stats"))
	if data, err := s.client.Raw(rc.Ctx, http.MethodGet, "/metrics.json", nil, "", nil); err == nil {
		var metrics any
		if json.Unmarshal(data, &metrics) == nil {
			out["metrics"] = metrics
		} else {
			out["metrics"] = string(data)
		}
	}
	return out, nil
}

func treeCollections(rc *plugin.RequestContext) (any, error) {
	res, err := listCollections(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name := fmt.Sprint(item["name"])
		ref := plugin.ResourceIdentity{Kind: "collection", Name: name, UID: name}
		nodes = append(nodes, plugin.TreeNode{Key: "collection:" + name, Label: name, Icon: icon("database"), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, Total: page.Total}, nil
}

func listCollections(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var rows []row
	if err := s.client.Do(rc.Ctx, http.MethodGet, "/collections", nil, nil, &rows); err != nil {
		return nil, err
	}
	for _, item := range rows {
		name := fmt.Sprint(item["name"])
		item["ref"] = plugin.ResourceIdentity{Kind: "collection", Name: name, UID: name}
	}
	return broker.PageRows(rc, rows)
}

func collectionOverview(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodGet, pathCollection(collectionParam(rc)), nil, nil, &out)
	return out, err
}

func createCollection(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Name                string           `json:"name"`
		Fields              []map[string]any `json:"fields"`
		DefaultSortingField string           `json:"default_sorting_field"`
		EnableNestedFields  bool             `json:"enable_nested_fields"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	body := row{"name": req.Name, "fields": req.Fields}
	if req.DefaultSortingField != "" {
		body["default_sorting_field"] = req.DefaultSortingField
	}
	if req.EnableNestedFields {
		body["enable_nested_fields"] = true
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodPost, "/collections", nil, body, &out)
	return out, err
}

func cloneCollection(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Source        string `json:"source"`
		Name          string `json:"name"`
		CopyDocuments bool   `json:"copy_documents"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	q := url.Values{"src_name": []string{req.Source}, "copy_documents": []string{strconv.FormatBool(req.CopyDocuments)}}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodPost, "/collections", q, row{"name": req.Name}, &out)
	return out, err
}

func updateCollection(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Schema map[string]any `json:"schema"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodPatch, pathCollection(collectionParam(rc)), nil, req.Schema, &out)
	return out, err
}

func deleteCollection(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodDelete, pathCollection(collectionParam(rc)), nil, nil, &out)
	return out, err
}

func listDocuments(rc *plugin.RequestContext) (any, error) {
	s, req, err := paged(rc)
	if err != nil {
		return nil, err
	}
	collection := collectionParam(rc)
	q := url.Values{"q": []string{"*"}, "page": []string{pageCursor(req.Cursor)}, "per_page": []string{strconv.Itoa(limitFor(s, req.Limit))}}
	if filter := req.Search(); filter != "" {
		q.Set("q", filter)
	}
	if queryBy := defaultQueryBy(rc.Ctx, s, collection); queryBy != "" {
		q.Set("query_by", queryBy)
	}
	result, err := searchDocuments(rc.Ctx, s, collection, q)
	if err != nil {
		return nil, err
	}
	next := ""
	if len(result.Rows) == limitFor(s, req.Limit) && int(result.Total) > pageNumber(req.Cursor)*limitFor(s, req.Limit) {
		next = strconv.Itoa(pageNumber(req.Cursor) + 1)
	}
	total := int(result.Total)
	return plugin.Page[row]{Items: result.Rows, NextCursor: next, Total: &total}, nil
}

func readDocument(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodGet, pathDocument(collectionParam(rc), docParam(rc)), nil, nil, &out)
	return out, err
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
		Action   string         `json:"action"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	action := req.Action
	if action == "" {
		action = "upsert"
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodPost, pathCollection(collectionParam(rc))+"/documents", url.Values{"action": []string{action}}, req.Document, &out)
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
	var out row
	err = s.client.Do(rc.Ctx, http.MethodPatch, pathDocument(collectionParam(rc), docParam(rc)), nil, doc, &out)
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
	var out row
	err = s.client.Do(rc.Ctx, http.MethodDelete, pathDocument(collectionParam(rc), docParam(rc)), nil, nil, &out)
	return out, err
}

func importDocuments(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Documents string `json:"documents"`
		Action    string `json:"action"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	if req.Action == "" {
		req.Action = "upsert"
	}
	data, err := s.client.Raw(rc.Ctx, http.MethodPost, pathCollection(collectionParam(rc))+"/documents/import", url.Values{"action": []string{req.Action}, "return_id": []string{"true"}}, "text/plain", []byte(req.Documents))
	if err != nil {
		return nil, err
	}
	return row{"result": string(data)}, nil
}

func exportDocuments(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	data, err := s.client.Raw(rc.Ctx, http.MethodGet, pathCollection(collectionParam(rc))+"/documents/export", nil, "", nil)
	if err != nil {
		return nil, err
	}
	return row{"content": string(data)}, nil
}

func treeAliases(rc *plugin.RequestContext) (any, error) {
	res, err := listAliases(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name := fmt.Sprint(item["name"])
		ref := plugin.ResourceIdentity{Kind: "alias", Name: name, UID: name}
		nodes = append(nodes, plugin.TreeNode{Key: "alias:" + name, Label: name, Icon: icon("tag"), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, Total: page.Total}, nil
}

func listAliases(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var out struct {
		Aliases []row `json:"aliases"`
	}
	if err := s.client.Do(rc.Ctx, http.MethodGet, "/aliases", nil, nil, &out); err != nil {
		return nil, err
	}
	for _, item := range out.Aliases {
		name := fmt.Sprint(item["name"])
		item["ref"] = plugin.ResourceIdentity{Kind: "alias", Name: name, UID: name}
	}
	return broker.PageRows(rc, out.Aliases)
}

func readAlias(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodGet, "/aliases/"+url.PathEscape(aliasParam(rc)), nil, nil, &out)
	return out, err
}

func upsertAlias(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Name           string `json:"name"`
		CollectionName string `json:"collection_name"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodPut, "/aliases/"+url.PathEscape(req.Name), nil, row{"collection_name": req.CollectionName}, &out)
	return out, err
}

func deleteAlias(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodDelete, "/aliases/"+url.PathEscape(aliasParam(rc)), nil, nil, &out)
	return out, err
}

func listSynonyms(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var decoded any
	if err := s.client.Do(rc.Ctx, http.MethodGet, "/synonym_sets", nil, nil, &decoded); err != nil {
		return nil, err
	}
	items := rowsFrom(decoded, "synonym_sets")
	for _, item := range items {
		id := synonymName(item)
		item["name"] = id
	}
	return broker.PageRows(rc, items)
}

func upsertSynonym(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		ID      string         `json:"id"`
		Synonym map[string]any `json:"synonym"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodPut, "/synonym_sets/"+url.PathEscape(req.ID), nil, req.Synonym, &out)
	return out, err
}

func deleteSynonym(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	err = s.client.Do(rc.Ctx, http.MethodDelete, "/synonym_sets/"+url.PathEscape(synonymParam(rc)), nil, nil, nil)
	return actionResult{OK: err == nil}, err
}

func listOverrides(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var decoded any
	if err := s.client.Do(rc.Ctx, http.MethodGet, "/curation_sets", nil, nil, &decoded); err != nil {
		return nil, err
	}
	items := rowsFrom(decoded, "curation_sets")
	for _, item := range items {
		id := synonymName(item)
		item["name"] = id
	}
	return broker.PageRows(rc, items)
}

func upsertOverride(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		ID       string         `json:"id"`
		Override map[string]any `json:"override"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodPut, "/curation_sets/"+url.PathEscape(req.ID), nil, req.Override, &out)
	return out, err
}

func deleteOverride(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	err = s.client.Do(rc.Ctx, http.MethodDelete, "/curation_sets/"+url.PathEscape(overrideParam(rc)), nil, nil, nil)
	return actionResult{OK: err == nil}, err
}

func treeKeys(rc *plugin.RequestContext) (any, error) {
	res, err := listKeys(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name := fmt.Sprint(item["id"])
		ref := plugin.ResourceIdentity{Kind: "key", Name: name, UID: name}
		nodes = append(nodes, plugin.TreeNode{Key: "key:" + name, Label: name + " " + fmt.Sprint(item["description"]), Icon: icon("key-round"), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, Total: page.Total}, nil
}

func listKeys(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var out struct {
		Keys []row `json:"keys"`
	}
	if err := s.client.Do(rc.Ctx, http.MethodGet, "/keys", nil, nil, &out); err != nil {
		return nil, err
	}
	for _, item := range out.Keys {
		id := fmt.Sprint(item["id"])
		item["ref"] = plugin.ResourceIdentity{Kind: "key", Name: id, UID: id}
	}
	return broker.PageRows(rc, out.Keys)
}

func readKey(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodGet, "/keys/"+url.PathEscape(keyParam(rc)), nil, nil, &out)
	return out, err
}

func createKey(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Description string `json:"description"`
		Actions     []any  `json:"actions"`
		Collections []any  `json:"collections"`
		ExpiresAt   any    `json:"expires_at"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	body := row{"description": req.Description, "actions": req.Actions, "collections": req.Collections}
	if req.ExpiresAt != nil {
		body["expires_at"] = req.ExpiresAt
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodPost, "/keys", nil, body, &out)
	return out, err
}

func deleteKey(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	err = s.client.Do(rc.Ctx, http.MethodDelete, "/keys/"+url.PathEscape(keyParam(rc)), nil, nil, nil)
	return actionResult{OK: err == nil}, err
}

type searchResult struct {
	Rows  []row
	Total int64
}

func searchDocuments(ctx context.Context, s *Session, collection string, query url.Values) (searchResult, error) {
	var out map[string]any
	if err := s.client.Do(ctx, http.MethodGet, pathCollection(collection)+"/documents/search", query, nil, &out); err != nil {
		return searchResult{}, err
	}
	rawHits, _ := out["hits"].([]any)
	rows := make([]row, 0, len(rawHits))
	for _, item := range rawHits {
		hit, _ := item.(map[string]any)
		doc, _ := hit["document"].(map[string]any)
		id := fmt.Sprint(doc["id"])
		rows = append(rows, row{
			"_collection": collection,
			"_id":         id,
			"_text_match": numeric(hit["text_match"]),
			"_source":     doc,
			"highlights":  hit["highlights"],
			"ref":         plugin.ResourceIdentity{Kind: "document", Namespace: collection, Name: id, UID: collection + "/" + id},
		})
	}
	return searchResult{Rows: rows, Total: numeric(out["found"])}, nil
}

func executeSearch(ctx context.Context, s *Session, collection string, body map[string]any) (sqldb.QueryResult, error) {
	start := time.Now()
	query := queryValues(body)
	if query.Get("q") == "" {
		query.Set("q", "*")
	}
	if query.Get("query_by") == "" {
		if queryBy := defaultQueryBy(ctx, s, collection); queryBy != "" {
			query.Set("query_by", queryBy)
		}
	}
	result, err := searchDocuments(ctx, s, collection, query)
	if err != nil {
		return sqldb.QueryResult{}, err
	}
	rows := make([][]any, 0, len(result.Rows))
	for _, r := range result.Rows {
		rows = append(rows, []any{r["_id"], r["_text_match"], r["_source"]})
	}
	return sqldb.QueryResult{
		Columns:    []string{"id", "text_match", "document"},
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
		result, err := executeSearch(client.Context(), s, collectionParam(rc), body)
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
		{Label: `{"q":"*","per_page":50}`, Type: "keyword", Apply: `{"q":"*","per_page":50}`},
		{Label: `{"q":"query","query_by":"name,title","filter_by":"status:=active","sort_by":"created_at:desc","per_page":50}`, Type: "keyword", Apply: `{"q":"query","query_by":"name,title","filter_by":"status:=active","sort_by":"created_at:desc","per_page":50}`},
		{Label: `{"q":"query","query_by":"name","facet_by":"category","max_facet_values":20}`, Type: "keyword", Apply: `{"q":"query","query_by":"name","facet_by":"category","max_facet_values":20}`},
	}, nil
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

func pageCursor(cursor string) string {
	if cursor == "" {
		return "1"
	}
	return cursor
}

func pageNumber(cursor string) int {
	n, _ := strconv.Atoi(pageCursor(cursor))
	if n < 1 {
		return 1
	}
	return n
}

func defaultQueryBy(ctx context.Context, s *Session, collection string) string {
	var schema row
	if err := s.client.Do(ctx, http.MethodGet, pathCollection(collection), nil, nil, &schema); err != nil {
		return ""
	}
	fields, _ := schema["fields"].([]any)
	names := make([]string, 0, len(fields))
	for _, item := range fields {
		field, _ := item.(map[string]any)
		typ := fmt.Sprint(field["type"])
		indexed, hasIndex := field["index"].(bool)
		if (typ == "string" || typ == "string[]") && (!hasIndex || indexed) {
			names = append(names, fmt.Sprint(field["name"]))
		}
	}
	return strings.Join(names, ",")
}

func queryValues(body map[string]any) url.Values {
	q := url.Values{}
	for key, value := range body {
		switch t := value.(type) {
		case nil:
		case string:
			q.Set(key, t)
		case []any:
			parts := make([]string, 0, len(t))
			for _, item := range t {
				parts = append(parts, fmt.Sprint(item))
			}
			q.Set(key, strings.Join(parts, ","))
		default:
			q.Set(key, fmt.Sprint(t))
		}
	}
	return q
}

func rowsFrom(value any, key string) []row {
	switch t := value.(type) {
	case []any:
		rows := make([]row, 0, len(t))
		for _, item := range t {
			if m, ok := item.(map[string]any); ok {
				rows = append(rows, row(m))
			}
		}
		return rows
	case map[string]any:
		if raw, ok := t[key].([]any); ok {
			return rowsFrom(raw, key)
		}
		for _, fallback := range []string{"items", "synonyms", "overrides", "aliases", "keys"} {
			if raw, ok := t[fallback].([]any); ok {
				return rowsFrom(raw, key)
			}
		}
	}
	return []row{}
}

func synonymName(item row) string {
	for _, key := range []string{"name", "id"} {
		if value := strings.TrimSpace(fmt.Sprint(item[key])); value != "" && value != "<nil>" {
			return value
		}
	}
	return ""
}

func ptrMap(parent map[string]any, key string) *map[string]any {
	child := map[string]any{}
	parent[key] = child
	return &child
}

func collectionParam(rc *plugin.RequestContext) string {
	if v := strings.TrimSpace(rc.Param("collection")); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p.collection"))
}

func docParam(rc *plugin.RequestContext) string {
	if v := strings.TrimSpace(rc.Param("id")); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p.id"))
}

func aliasParam(rc *plugin.RequestContext) string {
	if v := strings.TrimSpace(rc.Param("alias")); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p.alias"))
}

func synonymParam(rc *plugin.RequestContext) string {
	if v := strings.TrimSpace(rc.Param("synonym")); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p.synonym"))
}

func overrideParam(rc *plugin.RequestContext) string {
	if v := strings.TrimSpace(rc.Param("override")); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p.override"))
}

func keyParam(rc *plugin.RequestContext) string {
	if v := strings.TrimSpace(rc.Param("key")); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p.key"))
}

func pathCollection(collection string) string { return "/collections/" + url.PathEscape(collection) }

func pathDocument(collection, id string) string {
	return pathCollection(collection) + "/documents/" + url.PathEscape(id)
}

func numeric(v any) int64 {
	switch t := v.(type) {
	case int64:
		return t
	case int:
		return int64(t)
	case float64:
		return int64(t)
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
