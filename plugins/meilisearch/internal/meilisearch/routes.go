package meilisearch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func Routes() []plugin.Route {
	return []plugin.Route{
		{ID: rid("overview"), Method: plugin.MethodGet, Path: "/overview", Permission: "meilisearch.read", Risk: plugin.RiskSafe, AuditEvent: rid("overview"), Handle: overview},
		{ID: rid("indexes.tree"), Method: plugin.MethodGet, Path: "/tree/indexes", Permission: "meilisearch.indexes.read", Risk: plugin.RiskSafe, AuditEvent: rid("indexes.tree"), Handle: treeIndexes},
		{ID: rid("tasks.tree"), Method: plugin.MethodGet, Path: "/tree/tasks", Permission: "meilisearch.tasks.read", Risk: plugin.RiskSafe, AuditEvent: rid("tasks.tree"), Handle: treeTasks},
		{ID: rid("keys.tree"), Method: plugin.MethodGet, Path: "/tree/keys", Permission: "meilisearch.keys.read", Risk: plugin.RiskSafe, AuditEvent: rid("keys.tree"), Handle: treeKeys},
		{ID: rid("indexes.list"), Method: plugin.MethodGet, Path: "/indexes", Permission: "meilisearch.indexes.read", Risk: plugin.RiskSafe, AuditEvent: rid("indexes.list"), Handle: listIndexes},
		{ID: rid("index.overview"), Method: plugin.MethodGet, Path: "/indexes/{index}", Permission: "meilisearch.indexes.read", Risk: plugin.RiskSafe, AuditEvent: rid("index.overview"), Handle: indexOverview},
		{ID: rid("index.stats"), Method: plugin.MethodGet, Path: "/indexes/{index}/stats", Permission: "meilisearch.stats.read", Risk: plugin.RiskSafe, AuditEvent: rid("index.stats"), Handle: indexStats},
		{ID: rid("index.create"), Method: plugin.MethodPost, Path: "/indexes", Permission: "meilisearch.indexes.write", Risk: plugin.RiskWrite, AuditEvent: rid("index.create"), Input: indexCreateSchema(), Handle: createIndex},
		{ID: rid("index.update"), Method: plugin.MethodPatch, Path: "/indexes/{index}", Permission: "meilisearch.indexes.write", Risk: plugin.RiskWrite, AuditEvent: rid("index.update"), Input: indexUpdateSchema(), Handle: updateIndex},
		{ID: rid("index.delete"), Method: plugin.MethodDelete, Path: "/indexes/{index}", Permission: "meilisearch.indexes.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("index.delete"), Handle: deleteIndex},
		{ID: rid("settings.read"), Method: plugin.MethodGet, Path: "/indexes/{index}/settings", Permission: "meilisearch.settings.read", Risk: plugin.RiskSafe, AuditEvent: rid("settings.read"), Handle: readSettings},
		{ID: rid("settings.update"), Method: plugin.MethodPatch, Path: "/indexes/{index}/settings", Permission: "meilisearch.settings.write", Risk: plugin.RiskWrite, AuditEvent: rid("settings.update"), Input: settingsUpdateSchema(), Handle: updateSettings},
		{ID: rid("documents.list"), Method: plugin.MethodGet, Path: "/indexes/{index}/documents", Permission: "meilisearch.documents.read", Risk: plugin.RiskSafe, AuditEvent: rid("documents.list"), Handle: listDocuments},
		{ID: rid("document.read"), Method: plugin.MethodGet, Path: "/indexes/{index}/documents/{id}", Permission: "meilisearch.documents.read", Risk: plugin.RiskSafe, AuditEvent: rid("document.read"), Handle: readDocument},
		{ID: rid("document.upsert"), Method: plugin.MethodPut, Path: "/indexes/{index}/documents", Permission: "meilisearch.documents.write", Risk: plugin.RiskWrite, AuditEvent: rid("document.upsert"), Input: documentUpsertSchema(), Handle: upsertDocument},
		{ID: rid("document.update"), Method: plugin.MethodPut, Path: "/indexes/{index}/documents/{id}", Permission: "meilisearch.documents.write", Risk: plugin.RiskWrite, AuditEvent: rid("document.update"), Handle: updateDocument},
		{ID: rid("document.delete"), Method: plugin.MethodDelete, Path: "/indexes/{index}/documents/{id}", Permission: "meilisearch.documents.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("document.delete"), Handle: deleteDocument},
		{ID: rid("documents.delete_all"), Method: plugin.MethodDelete, Path: "/indexes/{index}/documents", Permission: "meilisearch.documents.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("documents.delete_all"), Handle: deleteAllDocuments},
		{ID: rid("tasks.list"), Method: plugin.MethodGet, Path: "/tasks", Permission: "meilisearch.tasks.read", Risk: plugin.RiskSafe, AuditEvent: rid("tasks.list"), Handle: listTasks},
		{ID: rid("task.read"), Method: plugin.MethodGet, Path: "/tasks/{task}", Permission: "meilisearch.tasks.read", Risk: plugin.RiskSafe, AuditEvent: rid("task.read"), Handle: readTask},
		{ID: rid("task.cancel"), Method: plugin.MethodPost, Path: "/tasks/{task}/cancel", Permission: "meilisearch.tasks.write", Risk: plugin.RiskWrite, AuditEvent: rid("task.cancel"), Handle: cancelTask},
		{ID: rid("task.delete"), Method: plugin.MethodDelete, Path: "/tasks/{task}", Permission: "meilisearch.tasks.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("task.delete"), Handle: deleteTask},
		{ID: rid("keys.list"), Method: plugin.MethodGet, Path: "/keys", Permission: "meilisearch.keys.read", Risk: plugin.RiskSafe, AuditEvent: rid("keys.list"), Handle: listKeys},
		{ID: rid("key.read"), Method: plugin.MethodGet, Path: "/keys/{key}", Permission: "meilisearch.keys.read", Risk: plugin.RiskSafe, AuditEvent: rid("key.read"), Handle: readKey},
		{ID: rid("key.create"), Method: plugin.MethodPost, Path: "/keys", Permission: "meilisearch.keys.write", Risk: plugin.RiskWrite, AuditEvent: rid("key.create"), Input: keyCreateSchema(), Handle: createKey},
		{ID: rid("key.update"), Method: plugin.MethodPatch, Path: "/keys/{key}", Permission: "meilisearch.keys.write", Risk: plugin.RiskWrite, AuditEvent: rid("key.update"), Input: keyUpdateSchema(), Handle: updateKey},
		{ID: rid("key.delete"), Method: plugin.MethodDelete, Path: "/keys/{key}", Permission: "meilisearch.keys.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("key.delete"), Handle: deleteKey},
		{ID: rid("dump.create"), Method: plugin.MethodPost, Path: "/dumps", Permission: "meilisearch.dumps.write", Risk: plugin.RiskWrite, AuditEvent: rid("dump.create"), Handle: createDump},
		{ID: rid("snapshot.create"), Method: plugin.MethodPost, Path: "/snapshots", Permission: "meilisearch.snapshots.write", Risk: plugin.RiskWrite, AuditEvent: rid("snapshot.create"), Handle: createSnapshot},
		{ID: rid("search.query"), Method: plugin.MethodWS, Path: "/search", Permission: "meilisearch.search.execute", Risk: plugin.RiskSafe, AuditEvent: rid("search.query"), Stream: searchStream},
		{ID: rid("completion"), Method: plugin.MethodGet, Path: "/completion", Permission: "meilisearch.read", Risk: plugin.RiskSafe, AuditEvent: rid("completion"), Handle: completionRoute},
	}
}

func indexCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Index", Fields: []plugin.Field{
		{Key: "uid", Label: "UID", Type: plugin.FieldText, Required: true},
		{Key: "primaryKey", Label: "Primary key", Type: plugin.FieldText},
	}}}}
}

func indexUpdateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Index", Fields: []plugin.Field{
		{Key: "primaryKey", Label: "Primary key", Type: plugin.FieldText, Required: true},
	}}}}
}

func settingsUpdateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Settings", Fields: []plugin.Field{
		{Key: "settings", Label: "Settings", Type: plugin.FieldJSON, Required: true},
	}}}}
}

func documentUpsertSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Document", Fields: []plugin.Field{
		{Key: "document", Label: "Document", Type: plugin.FieldJSON, Required: true},
		{Key: "primaryKey", Label: "Primary key", Type: plugin.FieldText},
	}}}}
}

func keyCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Key", Fields: []plugin.Field{
		{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true},
		{Key: "description", Label: "Description", Type: plugin.FieldTextarea},
		{Key: "actions", Label: "Actions", Type: plugin.FieldArray, Required: true, MinItems: 1, ItemLabel: "Action", Default: []any{"search"}, Item: &plugin.Field{Type: plugin.FieldText}},
		{Key: "indexes", Label: "Indexes", Type: plugin.FieldArray, Required: true, MinItems: 1, ItemLabel: "Index", Default: []any{"*"}, Item: &plugin.Field{Type: plugin.FieldText}},
		{Key: "expiresAt", Label: "Expires at", Type: plugin.FieldText, Placeholder: "2027-01-01T00:00:00Z or null"},
	}}}}
}

func keyUpdateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Key", Fields: []plugin.Field{
		{Key: "name", Label: "Name", Type: plugin.FieldText},
		{Key: "description", Label: "Description", Type: plugin.FieldTextarea},
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
	_ = s.client.Do(rc.Ctx, http.MethodGet, "/version", nil, nil, ptrMap(out, "version"))
	_ = s.client.Do(rc.Ctx, http.MethodGet, "/stats", nil, nil, ptrMap(out, "stats"))
	return out, nil
}

func treeIndexes(rc *plugin.RequestContext) (any, error) {
	res, err := listIndexes(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name := fmt.Sprint(item["uid"])
		ref := plugin.ResourceIdentity{Kind: "index", Name: name, UID: name}
		nodes = append(nodes, plugin.TreeNode{Key: "index:" + name, Label: name, Icon: icon("database"), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

func listIndexes(rc *plugin.RequestContext) (any, error) {
	s, req, err := paged(rc)
	if err != nil {
		return nil, err
	}
	q := url.Values{"limit": []string{strconv.Itoa(limitFor(s, req.Limit))}, "offset": []string{cursorOffset(req.Cursor)}}
	var out struct {
		Results []row `json:"results"`
		Total   int   `json:"total"`
		Limit   int   `json:"limit"`
		Offset  int   `json:"offset"`
	}
	if err := s.client.Do(rc.Ctx, http.MethodGet, "/indexes", q, nil, &out); err != nil {
		return nil, err
	}
	for _, r := range out.Results {
		name := fmt.Sprint(r["uid"])
		if stats, err := getIndexStats(rc.Ctx, s, name); err == nil {
			for k, v := range stats {
				r[k] = v
			}
		}
		r["ref"] = plugin.ResourceIdentity{Kind: "index", Name: name, UID: name}
	}
	next := nextOffset(out.Offset, out.Limit, out.Total)
	return plugin.Page[row]{Items: out.Results, NextCursor: next, Total: &out.Total}, nil
}

func indexOverview(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var out row
	if err := s.client.Do(rc.Ctx, http.MethodGet, pathIndex(indexParam(rc)), nil, nil, &out); err != nil {
		return nil, err
	}
	if stats, err := getIndexStats(rc.Ctx, s, indexParam(rc)); err == nil {
		out["stats"] = stats
	}
	return out, nil
}

func indexStats(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	return getIndexStats(rc.Ctx, s, indexParam(rc))
}

func createIndex(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		UID        string `json:"uid"`
		PrimaryKey string `json:"primaryKey"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	body := row{"uid": req.UID}
	if strings.TrimSpace(req.PrimaryKey) != "" {
		body["primaryKey"] = req.PrimaryKey
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodPost, "/indexes", nil, body, &out)
	return out, err
}

func updateIndex(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		PrimaryKey string `json:"primaryKey"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodPatch, pathIndex(indexParam(rc)), nil, row{"primaryKey": req.PrimaryKey}, &out)
	return out, err
}

func deleteIndex(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodDelete, pathIndex(indexParam(rc)), nil, nil, &out)
	return out, err
}

func readSettings(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodGet, pathIndex(indexParam(rc))+"/settings", nil, nil, &out)
	return out, err
}

func updateSettings(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Settings map[string]any `json:"settings"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodPatch, pathIndex(indexParam(rc))+"/settings", nil, req.Settings, &out)
	return out, err
}

func listDocuments(rc *plugin.RequestContext) (any, error) {
	s, req, err := paged(rc)
	if err != nil {
		return nil, err
	}
	index := indexParam(rc)
	limit := limitFor(s, req.Limit)
	q := url.Values{"limit": []string{strconv.Itoa(limit)}, "offset": []string{cursorOffset(req.Cursor)}}
	if filter := req.Search(); filter != "" {
		q.Set("filter", filter)
	}
	var out struct {
		Results []row `json:"results"`
		Total   int   `json:"total"`
		Limit   int   `json:"limit"`
		Offset  int   `json:"offset"`
	}
	if err := s.client.Do(rc.Ctx, http.MethodGet, pathIndex(index)+"/documents", q, nil, &out); err != nil {
		return nil, err
	}
	pk := primaryKey(rc.Ctx, s, index)
	rows := make([]row, 0, len(out.Results))
	for i, doc := range out.Results {
		id := docID(doc, pk, out.Offset+i)
		rows = append(rows, row{"_index": index, "_id": id, "_source": doc, "ref": plugin.ResourceIdentity{Kind: "document", Namespace: index, Name: id, UID: index + "/" + id}})
	}
	next := nextOffset(out.Offset, out.Limit, out.Total)
	return plugin.Page[row]{Items: rows, NextCursor: next, Total: &out.Total}, nil
}

func readDocument(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodGet, pathDoc(indexParam(rc), docParam(rc)), nil, nil, &out)
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
		Document   map[string]any `json:"document"`
		PrimaryKey string         `json:"primaryKey"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	q := url.Values{}
	if strings.TrimSpace(req.PrimaryKey) != "" {
		q.Set("primaryKey", req.PrimaryKey)
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodPut, pathIndex(indexParam(rc))+"/documents", q, []map[string]any{req.Document}, &out)
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
	err = s.client.Do(rc.Ctx, http.MethodPut, pathIndex(indexParam(rc))+"/documents", nil, []map[string]any{doc}, &out)
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
	err = s.client.Do(rc.Ctx, http.MethodDelete, pathDoc(indexParam(rc), docParam(rc)), nil, nil, &out)
	return out, err
}

func deleteAllDocuments(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodDelete, pathIndex(indexParam(rc))+"/documents", nil, nil, &out)
	return out, err
}

func treeTasks(rc *plugin.RequestContext) (any, error) {
	res, err := listTasks(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name := fmt.Sprint(item["uid"])
		ref := plugin.ResourceIdentity{Kind: "task", Name: name, UID: name}
		nodes = append(nodes, plugin.TreeNode{Key: "task:" + name, Label: name + " " + fmt.Sprint(item["type"]), Icon: icon("list-checks"), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

func listTasks(rc *plugin.RequestContext) (any, error) {
	s, req, err := paged(rc)
	if err != nil {
		return nil, err
	}
	q := url.Values{"limit": []string{strconv.Itoa(limitFor(s, req.Limit))}}
	if req.Cursor != "" {
		q.Set("from", req.Cursor)
	}
	if index := indexParam(rc); index != "" {
		q.Set("indexUids", index)
	}
	// Meilisearch paginates /tasks with a numeric "next" (the uid to pass as
	// "from"), or null when there are no more — not a string.
	var out struct {
		Results []row  `json:"results"`
		Next    *int64 `json:"next"`
	}
	if err := s.client.Do(rc.Ctx, http.MethodGet, "/tasks", q, nil, &out); err != nil {
		return nil, err
	}
	for _, item := range out.Results {
		name := fmt.Sprint(item["uid"])
		item["ref"] = plugin.ResourceIdentity{Kind: "task", Name: name, UID: name}
	}
	next := ""
	if out.Next != nil {
		next = strconv.FormatInt(*out.Next, 10)
	}
	return plugin.Page[row]{Items: out.Results, NextCursor: next}, nil
}

func readTask(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodGet, "/tasks/"+url.PathEscape(taskParam(rc)), nil, nil, &out)
	return out, err
}

func cancelTask(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	uid, err := validTaskUID(taskParam(rc))
	if err != nil {
		return nil, err
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodPost, "/tasks/cancel", url.Values{"uids": []string{uid}}, nil, &out)
	return out, err
}

func deleteTask(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	uid, err := validTaskUID(taskParam(rc))
	if err != nil {
		return nil, err
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodDelete, "/tasks", url.Values{"uids": []string{uid}}, nil, &out)
	return out, err
}

func treeKeys(rc *plugin.RequestContext) (any, error) {
	res, err := listKeys(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name := fmt.Sprint(item["uid"])
		ref := plugin.ResourceIdentity{Kind: "key", Name: name, UID: name}
		nodes = append(nodes, plugin.TreeNode{Key: "key:" + name, Label: displayName(item, name), Icon: icon("key-round"), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

func listKeys(rc *plugin.RequestContext) (any, error) {
	s, req, err := paged(rc)
	if err != nil {
		return nil, err
	}
	q := url.Values{"limit": []string{strconv.Itoa(limitFor(s, req.Limit))}, "offset": []string{cursorOffset(req.Cursor)}}
	var out struct {
		Results []row `json:"results"`
		Total   int   `json:"total"`
		Limit   int   `json:"limit"`
		Offset  int   `json:"offset"`
	}
	if err := s.client.Do(rc.Ctx, http.MethodGet, "/keys", q, nil, &out); err != nil {
		return nil, err
	}
	for _, item := range out.Results {
		name := fmt.Sprint(item["uid"])
		item["ref"] = plugin.ResourceIdentity{Kind: "key", Name: name, UID: name}
	}
	next := nextOffset(out.Offset, out.Limit, out.Total)
	return plugin.Page[row]{Items: out.Results, NextCursor: next, Total: &out.Total}, nil
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
		Name        string `json:"name"`
		Description string `json:"description"`
		Actions     []any  `json:"actions"`
		Indexes     []any  `json:"indexes"`
		ExpiresAt   any    `json:"expiresAt"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	body := row{"name": req.Name, "description": req.Description, "actions": req.Actions, "indexes": req.Indexes, "expiresAt": req.ExpiresAt}
	if strings.TrimSpace(fmt.Sprint(req.ExpiresAt)) == "" {
		body["expiresAt"] = nil
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodPost, "/keys", nil, body, &out)
	return out, err
}

func updateKey(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	uid, err := validKeyUID(keyParam(rc))
	if err != nil {
		return nil, err
	}
	var req struct {
		Name        *string `json:"name"`
		Description *string `json:"description"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	body, err := keyUpdateBody(req.Name, req.Description)
	if err != nil {
		return nil, err
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodPatch, "/keys/"+url.PathEscape(uid), nil, body, &out)
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

func createDump(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodPost, "/dumps", nil, nil, &out)
	return out, err
}

func createSnapshot(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var out row
	err = s.client.Do(rc.Ctx, http.MethodPost, "/snapshots", nil, nil, &out)
	return out, err
}

func executeSearch(ctx context.Context, s *Session, index string, body map[string]any) (sqldb.QueryResult, error) {
	start := time.Now()
	var out map[string]any
	if err := s.client.Do(ctx, http.MethodPost, pathIndex(index)+"/search", nil, body, &out); err != nil {
		return sqldb.QueryResult{}, err
	}
	rawHits, _ := out["hits"].([]any)
	rows := make([][]any, 0, len(rawHits))
	pk := primaryKey(ctx, s, index)
	for _, item := range rawHits {
		doc, _ := item.(map[string]any)
		rows = append(rows, []any{docID(doc, pk, len(rows)), doc})
	}
	total := numeric(out["estimatedTotalHits"])
	if total == 0 {
		total = numeric(out["totalHits"])
	}
	return sqldb.QueryResult{
		Columns:    []string{"id", "document"},
		Rows:       rows,
		RowCount:   total,
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
		result, err := executeSearch(client.Context(), s, indexParam(rc), body)
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
		{Label: `{"q":"","limit":50}`, Type: "keyword", Apply: `{"q":"","limit":50}`},
		{Label: `{"q":"query","filter":"status = active","sort":["created_at:desc"],"limit":50}`, Type: "keyword", Apply: `{"q":"query","filter":"status = active","sort":["created_at:desc"],"limit":50}`},
		{Label: `{"q":"query","facets":["*"],"attributesToHighlight":["*"],"limit":20}`, Type: "keyword", Apply: `{"q":"query","facets":["*"],"attributesToHighlight":["*"],"limit":20}`},
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

func cursorOffset(cursor string) string {
	if cursor == "" {
		return "0"
	}
	return cursor
}

func nextOffset(offset, limit, total int) string {
	if limit <= 0 || offset+limit >= total {
		return ""
	}
	return strconv.Itoa(offset + limit)
}

func getIndexStats(ctx context.Context, s *Session, index string) (row, error) {
	var stats row
	err := s.client.Do(ctx, http.MethodGet, pathIndex(index)+"/stats", nil, nil, &stats)
	return stats, err
}

func primaryKey(ctx context.Context, s *Session, index string) string {
	var info row
	if err := s.client.Do(ctx, http.MethodGet, pathIndex(index), nil, nil, &info); err == nil {
		if pk := strings.TrimSpace(fmt.Sprint(info["primaryKey"])); pk != "" && pk != "<nil>" {
			return pk
		}
	}
	return "id"
}

func docID(doc row, primaryKey string, fallback int) string {
	if v, ok := doc[primaryKey]; ok {
		return fmt.Sprint(v)
	}
	if v, ok := doc["id"]; ok {
		return fmt.Sprint(v)
	}
	return strconv.Itoa(fallback)
}

func ptrMap(parent map[string]any, key string) *map[string]any {
	child := map[string]any{}
	parent[key] = child
	return &child
}

func indexParam(rc *plugin.RequestContext) string {
	if v := strings.TrimSpace(rc.Param("index")); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p.index"))
}

func docParam(rc *plugin.RequestContext) string {
	if v := strings.TrimSpace(rc.Param("id")); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p.id"))
}

func taskParam(rc *plugin.RequestContext) string {
	if v := strings.TrimSpace(rc.Param("task")); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p.task"))
}

func keyParam(rc *plugin.RequestContext) string {
	if v := strings.TrimSpace(rc.Param("key")); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p.key"))
}

// validTaskUID accepts Meilisearch task uids, which are non-negative integers.
func validTaskUID(uid string) (string, error) {
	uid = strings.TrimSpace(uid)
	if n, err := strconv.ParseInt(uid, 10, 64); err != nil || n < 0 {
		return "", fmt.Errorf("%w: task uid must be a non-negative integer", plugin.ErrInvalidInput)
	}
	return uid, nil
}

// validKeyUID accepts Meilisearch API key uids, which are UUIDs.
func validKeyUID(uid string) (string, error) {
	uid = strings.TrimSpace(uid)
	if !uuidPattern.MatchString(uid) {
		return "", fmt.Errorf("%w: key uid must be a UUID", plugin.ErrInvalidInput)
	}
	return uid, nil
}

// keyUpdateBody builds the PATCH /keys body; Meilisearch only allows updating
// name and description, and rejects an empty payload.
func keyUpdateBody(name, description *string) (row, error) {
	body := row{}
	if name != nil {
		body["name"] = strings.TrimSpace(*name)
	}
	if description != nil {
		body["description"] = strings.TrimSpace(*description)
	}
	if len(body) == 0 {
		return nil, fmt.Errorf("%w: provide a name or description to update", plugin.ErrInvalidInput)
	}
	return body, nil
}

func pathIndex(index string) string { return "/indexes/" + url.PathEscape(index) }

func pathDoc(index, id string) string {
	return pathIndex(index) + "/documents/" + url.PathEscape(id)
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

func displayName(item row, fallback string) string {
	if name := strings.TrimSpace(fmt.Sprint(item["name"])); name != "" && name != "<nil>" {
		return name
	}
	return fallback
}

func auditResult(err error) plugin.AuditResult {
	if err != nil {
		return plugin.AuditError
	}
	return plugin.AuditAllowed
}
