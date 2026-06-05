package qdrant

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

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
	if err := s.client.Do(ctx.Ctx, method, path, query, body, &env); err != nil {
		return err
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
		ref := plugin.ResourceRef{Kind: "collection", Name: name, UID: name}
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
		item["ref"] = plugin.ResourceRef{Kind: "collection", Name: name, UID: name}
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
		name := fmt.Sprint(item["alias_name"])
		item["ref"] = plugin.ResourceRef{Kind: "alias", Name: name, UID: name, Namespace: fmt.Sprint(item["collection_name"])}
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
		item["ref"] = plugin.ResourceRef{Kind: "point", Name: id, UID: id, Namespace: collectionParam(rc)}
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
		var body any
		if err := dec.Decode(&body); err != nil {
			if client.Context().Err() != nil || errors.Is(err, io.EOF) {
				return nil
			}
			if err := enc.Encode(row{"error": "Invalid JSON query."}); err != nil {
				return err
			}
			continue
		}
		var out any
		err := qdrantAPI(rc, http.MethodPost, "/collections/"+url.PathEscape(collectionParam(rc))+"/points/query", nil, body, &out)
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
	}
	return raw, nil
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
