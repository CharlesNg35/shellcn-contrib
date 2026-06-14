package qdrant

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

type envelope struct {
	Result json.RawMessage `json:"result"`
	Status string          `json:"status"`
	Time   float64         `json:"time"`
}

func Routes() []plugin.Route {
	return []plugin.Route{
		{ID: rid("overview"), Method: plugin.MethodGet, Path: "/overview", Permission: "qdrant.read", Risk: plugin.RiskSafe, AuditEvent: rid("overview"), Handle: overview},
		{ID: rid("collections.tree"), Method: plugin.MethodGet, Path: "/tree/collections", Permission: "qdrant.collections.read", Risk: plugin.RiskSafe, AuditEvent: rid("collections.tree"), Handle: treeCollections},
		{ID: rid("collections.list"), Method: plugin.MethodGet, Path: "/collections", Permission: "qdrant.collections.read", Risk: plugin.RiskSafe, AuditEvent: rid("collections.list"), Handle: listCollections},
		{ID: rid("collection.read"), Method: plugin.MethodGet, Path: "/collections/{collection}", Permission: "qdrant.collections.read", Risk: plugin.RiskSafe, AuditEvent: rid("collection.read"), Handle: readCollection},
		{ID: rid("collection.create"), Method: plugin.MethodPut, Path: "/collections", Permission: "qdrant.collections.write", Risk: plugin.RiskWrite, AuditEvent: rid("collection.create"), Input: collectionCreateSchema(), Handle: createCollection},
		{ID: rid("collection.delete"), Method: plugin.MethodDelete, Path: "/collections/{collection}", Permission: "qdrant.collections.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("collection.delete"), Handle: deleteCollection},
		{ID: rid("collection.aliases"), Method: plugin.MethodGet, Path: "/collections/{collection}/aliases", Permission: "qdrant.aliases.read", Risk: plugin.RiskSafe, AuditEvent: rid("collection.aliases"), Handle: listCollectionAliases},
		{ID: rid("alias.create"), Method: plugin.MethodPost, Path: "/collections/{collection}/aliases", Permission: "qdrant.aliases.write", Risk: plugin.RiskWrite, AuditEvent: rid("alias.create"), Input: aliasCreateSchema(), Handle: createAlias},
		{ID: rid("alias.delete"), Method: plugin.MethodDelete, Path: "/aliases/{alias}", Permission: "qdrant.aliases.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("alias.delete"), Handle: deleteAlias},
		{ID: rid("points.list"), Method: plugin.MethodGet, Path: "/collections/{collection}/points", Permission: "qdrant.points.read", Risk: plugin.RiskSafe, AuditEvent: rid("points.list"), Handle: listPoints},
		{ID: rid("point.read"), Method: plugin.MethodGet, Path: "/collections/{collection}/points/{point}", Permission: "qdrant.points.read", Risk: plugin.RiskSafe, AuditEvent: rid("point.read"), Handle: readPoint},
		{ID: rid("point.upsert"), Method: plugin.MethodPut, Path: "/collections/{collection}/points", Permission: "qdrant.points.write", Risk: plugin.RiskWrite, AuditEvent: rid("point.upsert"), Input: rawJSONSchema("Points"), Handle: upsertPoints},
		{ID: rid("point.delete"), Method: plugin.MethodPost, Path: "/collections/{collection}/points/{point}/delete", Permission: "qdrant.points.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("point.delete"), Handle: deletePoint},
		{ID: rid("payload.index.create"), Method: plugin.MethodPut, Path: "/collections/{collection}/index", Permission: "qdrant.indexes.write", Risk: plugin.RiskWrite, AuditEvent: rid("payload.index.create"), Input: payloadIndexSchema(), Handle: createPayloadIndex},
		{ID: rid("snapshots.list"), Method: plugin.MethodGet, Path: "/collections/{collection}/snapshots", Permission: "qdrant.snapshots.read", Risk: plugin.RiskSafe, AuditEvent: rid("snapshots.list"), Handle: listSnapshots},
		{ID: rid("snapshot.create"), Method: plugin.MethodPost, Path: "/collections/{collection}/snapshots", Permission: "qdrant.snapshots.write", Risk: plugin.RiskWrite, AuditEvent: rid("snapshot.create"), Handle: createSnapshot},
		{ID: rid("query"), Method: plugin.MethodWS, Path: "/collections/{collection}/query", Permission: "qdrant.query.execute", Risk: plugin.RiskSafe, AuditEvent: rid("query"), Stream: queryStream},
	}
}

func collectionCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Collection", Fields: []plugin.Field{
		{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true},
		{Key: "vector_size", Label: "Vector size", Type: plugin.FieldNumber, Required: true, Default: 384, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}}},
		{Key: "distance", Label: "Distance", Type: plugin.FieldSelect, Required: true, Default: "Cosine", Options: []plugin.Option{{Label: "Cosine", Value: "Cosine"}, {Label: "Dot", Value: "Dot"}, {Label: "Euclid", Value: "Euclid"}, {Label: "Manhattan", Value: "Manhattan"}}},
		{Key: "on_disk_payload", Label: "On-disk payload", Type: plugin.FieldToggle},
	}}}}
}

func rawJSONSchema(name string) *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: name, Fields: []plugin.Field{
		{Key: "body", Label: name, Type: plugin.FieldJSON, Required: true},
	}}}}
}

func payloadIndexSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Payload index", Fields: []plugin.Field{
		{Key: "field_name", Label: "Field name", Type: plugin.FieldText, Required: true},
		{Key: "field_schema", Label: "Field schema", Type: plugin.FieldSelect, Required: true, Default: "keyword", Options: []plugin.Option{{Label: "Keyword", Value: "keyword"}, {Label: "Integer", Value: "integer"}, {Label: "Float", Value: "float"}, {Label: "Bool", Value: "bool"}, {Label: "Datetime", Value: "datetime"}, {Label: "UUID", Value: "uuid"}, {Label: "Geo", Value: "geo"}, {Label: "Text", Value: "text"}}},
	}}}}
}

func aliasCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Alias", Fields: []plugin.Field{
		{Key: "alias_name", Label: "Alias name", Type: plugin.FieldText, Required: true},
	}}}}
}

func session(rc *plugin.RequestContext) (*Session, error) { return unwrap(rc.Session) }

func qdrantAPI(ctx *plugin.RequestContext, method, path string, query url.Values, body any, out any) error {
	s, err := session(ctx)
	if err != nil {
		return err
	}
	var env envelope
	do := func() error {
		env = envelope{}
		return s.client.Do(ctx.Ctx, method, path, query, body, &env)
	}
	if err := do(); err != nil {
		if !shouldRetryQdrant(method, path, err) {
			return err
		}
		s.client.Close()
		if retryErr := do(); retryErr != nil {
			return retryErr
		}
	}
	if env.Status != "" && env.Status != "ok" {
		return fmt.Errorf("%w: Qdrant returned status %q", plugin.ErrUnavailable, env.Status)
	}
	if out == nil || len(env.Result) == 0 {
		return nil
	}
	return json.Unmarshal(env.Result, out)
}

func overview(rc *plugin.RequestContext) (any, error) {
	out := row{}
	var collections any
	if err := qdrantAPI(rc, http.MethodGet, "/collections", nil, nil, &collections); err == nil {
		out["collections"] = collections
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
	var out struct {
		Collections []row `json:"collections"`
	}
	if err := qdrantAPI(rc, http.MethodGet, "/collections", nil, nil, &out); err != nil {
		return nil, err
	}
	for _, item := range out.Collections {
		name := fmt.Sprint(item["name"])
		var detail row
		if err := qdrantAPI(rc, http.MethodGet, "/collections/"+url.PathEscape(name), nil, nil, &detail); err == nil {
			merge(item, detail)
		}
		item["ref"] = plugin.ResourceIdentity{Kind: "collection", Name: name, UID: name}
	}
	return broker.PageRows(rc, out.Collections)
}

func readCollection(rc *plugin.RequestContext) (any, error) {
	var out row
	err := qdrantAPI(rc, http.MethodGet, "/collections/"+url.PathEscape(collectionParam(rc)), nil, nil, &out)
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
		Name          string `json:"name" validate:"required"`
		VectorSize    int    `json:"vector_size" validate:"required,min=1"`
		Distance      string `json:"distance" validate:"required"`
		OnDiskPayload bool   `json:"on_disk_payload"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	body := row{"vectors": row{"size": req.VectorSize, "distance": req.Distance}, "on_disk_payload": req.OnDiskPayload}
	var out any
	err = qdrantAPI(rc, http.MethodPut, "/collections/"+url.PathEscape(req.Name), url.Values{"wait": []string{"true"}}, body, &out)
	return row{"ok": err == nil, "result": out}, err
}

func deleteCollection(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var out any
	err = qdrantAPI(rc, http.MethodDelete, "/collections/"+url.PathEscape(collectionParam(rc)), url.Values{"wait": []string{"true"}}, nil, &out)
	return row{"ok": err == nil, "result": out}, err
}

func listCollectionAliases(rc *plugin.RequestContext) (any, error) {
	var out struct {
		Aliases []row `json:"aliases"`
	}
	if err := qdrantAPI(rc, http.MethodGet, "/collections/"+url.PathEscape(collectionParam(rc))+"/aliases", nil, nil, &out); err != nil {
		return nil, err
	}
	for _, item := range out.Aliases {
		if item["collection_name"] == nil {
			item["collection_name"] = collectionParam(rc)
		}
	}
	return broker.PageRows(rc, out.Aliases)
}

func createAlias(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		AliasName string `json:"alias_name" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	body := row{"actions": []any{row{"create_alias": row{"collection_name": collectionParam(rc), "alias_name": req.AliasName}}}}
	var out any
	err = qdrantAPI(rc, http.MethodPost, "/collections/aliases", url.Values{"timeout": []string{"30"}}, body, &out)
	return row{"ok": err == nil, "result": out}, err
}

func deleteAlias(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	body := row{"actions": []any{row{"delete_alias": row{"alias_name": aliasParam(rc)}}}}
	var out any
	err = qdrantAPI(rc, http.MethodPost, "/collections/aliases", url.Values{"timeout": []string{"30"}}, body, &out)
	return row{"ok": err == nil, "result": out}, err
}

func listPoints(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	req, err := rc.Page()
	if err != nil {
		return nil, err
	}
	limit := req.Limit
	if limit <= 0 || limit > s.opts.PageLimit {
		limit = s.opts.PageLimit
	}
	body := row{"limit": limit, "with_payload": true, "with_vector": true}
	if req.Cursor != "" {
		body["offset"] = req.Cursor
	}
	var out struct {
		Points     []row `json:"points"`
		NextOffset any   `json:"next_page_offset"`
	}
	if err := qdrantAPI(rc, http.MethodPost, "/collections/"+url.PathEscape(collectionParam(rc))+"/points/scroll", nil, body, &out); err != nil {
		return nil, err
	}
	for _, item := range out.Points {
		id := fmt.Sprint(item["id"])
		item["ref"] = plugin.ResourceIdentity{Kind: "point", Name: id, UID: id, Namespace: collectionParam(rc)}
	}
	next := ""
	if out.NextOffset != nil {
		next = fmt.Sprint(out.NextOffset)
	}
	return plugin.Page[row]{Items: plugin.FilterRows(out.Points, req.Search()), NextCursor: next}, nil
}

func readPoint(rc *plugin.RequestContext) (any, error) {
	body := row{"ids": []any{pointID(pointParam(rc))}, "with_payload": true, "with_vector": true}
	var out []row
	if err := qdrantAPI(rc, http.MethodPost, "/collections/"+url.PathEscape(collectionParam(rc))+"/points", nil, body, &out); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: point %q", plugin.ErrNotFound, pointParam(rc))
	}
	return out[0], nil
}

func upsertPoints(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	body, err := requestBody(rc)
	if err != nil {
		return nil, err
	}
	var out any
	err = qdrantAPI(rc, http.MethodPut, "/collections/"+url.PathEscape(collectionParam(rc))+"/points", url.Values{"wait": []string{"true"}}, body, &out)
	return row{"ok": err == nil, "result": out}, err
}

func deletePoint(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	body := row{"points": []any{pointID(pointParam(rc))}}
	var out any
	err = qdrantAPI(rc, http.MethodPost, "/collections/"+url.PathEscape(collectionParam(rc))+"/points/delete", url.Values{"wait": []string{"true"}}, body, &out)
	return row{"ok": err == nil, "result": out}, err
}

func createPayloadIndex(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		FieldName   string `json:"field_name" validate:"required"`
		FieldSchema string `json:"field_schema" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	var out any
	err = qdrantAPI(rc, http.MethodPut, "/collections/"+url.PathEscape(collectionParam(rc))+"/index", url.Values{"wait": []string{"true"}}, row{"field_name": req.FieldName, "field_schema": req.FieldSchema}, &out)
	return row{"ok": err == nil, "result": out}, err
}

func listSnapshots(rc *plugin.RequestContext) (any, error) {
	var out []row
	if err := qdrantAPI(rc, http.MethodGet, "/collections/"+url.PathEscape(collectionParam(rc))+"/snapshots", nil, nil, &out); err != nil {
		return nil, err
	}
	return broker.PageRows(rc, out)
}

func createSnapshot(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var out any
	err = qdrantAPI(rc, http.MethodPost, "/collections/"+url.PathEscape(collectionParam(rc))+"/snapshots", nil, nil, &out)
	return row{"ok": err == nil, "result": out}, err
}

func queryStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	dec := json.NewDecoder(client)
	enc := json.NewEncoder(client)
	for {
		var frame any
		if err := dec.Decode(&frame); err != nil {
			if client.Context().Err() != nil || errors.Is(err, io.EOF) {
				return nil
			}
			if err := enc.Encode(row{"error": "Invalid JSON query."}); err != nil {
				return err
			}
			continue
		}
		body, err := queryBody(frame)
		if err != nil {
			if encErr := enc.Encode(row{"error": err.Error()}); encErr != nil {
				return encErr
			}
			continue
		}
		var out any
		err = qdrantAPI(rc, http.MethodPost, "/collections/"+url.PathEscape(collectionParam(rc))+"/points/query", nil, body, &out)
		if err != nil {
			if encErr := enc.Encode(row{"error": err.Error()}); encErr != nil {
				return encErr
			}
			continue
		}
		if err := enc.Encode(queryResult(out)); err != nil {
			return err
		}
	}
}

func pointID(raw string) any {
	if i, err := strconv.ParseUint(raw, 10, 64); err == nil {
		return i
	}
	return raw
}

func collectionParam(rc *plugin.RequestContext) string { return rc.Param("collection") }
func aliasParam(rc *plugin.RequestContext) string      { return rc.Param("alias") }
func pointParam(rc *plugin.RequestContext) string      { return rc.Param("point") }

func requestBody(rc *plugin.RequestContext) (any, error) {
	var raw any
	if err := json.Unmarshal(rc.Body(), &raw); err != nil {
		return nil, fmt.Errorf("%w: body must be JSON", plugin.ErrInvalidInput)
	}
	if m, ok := raw.(map[string]any); ok {
		if body, ok := m["body"]; ok && len(m) == 1 {
			return body, nil
		}
		if content, ok := m["content"].(string); ok && len(m) == 1 {
			var body any
			if err := json.Unmarshal([]byte(content), &body); err != nil {
				return nil, fmt.Errorf("%w: content must be JSON", plugin.ErrInvalidInput)
			}
			return body, nil
		}
	}
	return raw, nil
}

func queryBody(frame any) (any, error) {
	m, ok := frame.(map[string]any)
	if !ok {
		return frame, nil
	}
	rawQuery, hasQuery := m["query"]
	_, hasConfirm := m["confirm"]
	queryText, queryIsText := rawQuery.(string)
	if !hasQuery || (!hasConfirm && !queryIsText) {
		return frame, nil
	}
	if !queryIsText {
		return frame, nil
	}
	var body any
	if err := json.Unmarshal([]byte(queryText), &body); err != nil {
		return nil, fmt.Errorf("%w: query must be valid JSON", plugin.ErrInvalidInput)
	}
	return body, nil
}

func queryResult(v any) row {
	points := queryPoints(v)
	rows := make([][]any, 0, len(points))
	for _, point := range points {
		rows = append(rows, []any{
			point["id"],
			point["score"],
			jsonText(point["payload"]),
			jsonText(point["vector"]),
		})
	}
	return row{
		"columns":  []string{"id", "score", "payload", "vector"},
		"rows":     rows,
		"rowCount": len(rows),
	}
}

func queryPoints(v any) []row {
	switch t := v.(type) {
	case []any:
		return rowsFromAny(t)
	case map[string]any:
		if points, ok := t["points"].([]any); ok {
			return rowsFromAny(points)
		}
	}
	return nil
}

func rowsFromAny(items []any) []row {
	rows := make([]row, 0, len(items))
	for _, item := range items {
		if m, ok := item.(map[string]any); ok {
			rows = append(rows, row(m))
		}
	}
	return rows
}

func jsonText(v any) string {
	if v == nil {
		return ""
	}
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(data)
}

func shouldRetryQdrant(method, path string, err error) bool {
	text := err.Error()
	if !strings.Contains(text, "EOF") &&
		!strings.Contains(text, "connection reset by peer") &&
		!strings.Contains(text, "server closed idle connection") {
		return false
	}
	switch method {
	case http.MethodGet, http.MethodPut:
		return true
	case http.MethodPost:
		return strings.Contains(path, "/points") && !strings.Contains(path, "/delete")
	default:
		return false
	}
}

func ensureWritable(s *Session) error {
	if s.opts.ReadOnly {
		return fmt.Errorf("%w: connection is read-only", plugin.ErrForbidden)
	}
	return nil
}

func merge(dst row, src row) {
	for k, v := range src {
		dst[k] = v
	}
}
