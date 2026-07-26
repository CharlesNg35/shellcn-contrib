package weaviate

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	savedQueryCollection = "saved_queries"
	savedQueryPrefix     = "query/"
	activityCollection   = "activity"
	activityPrefix       = "event/"
	activityRetention    = 250
)

type savedQuery struct {
	Name       string `json:"name"`
	Kind       string `json:"kind"`
	Collection string `json:"collection,omitempty"`
	Query      string `json:"query"`
}

type activityEvent struct {
	Category string `json:"category"`
	Title    string `json:"title"`
	Message  string `json:"message"`
	Severity string `json:"severity"`
	Time     string `json:"time"`
}

func savedQueryKinds() []plugin.Option {
	return []plugin.Option{
		{Label: "GraphQL", Value: "graphql"},
		{Label: "Vector search", Value: "search"},
	}
}

func listSavedQueries(rc *plugin.RequestContext) (any, error) {
	if rc.Storage == nil {
		return plugin.Page[row]{Items: []row{}}, nil
	}
	items, err := rc.Storage.List(rc.Ctx, plugin.ConnectionStorage(savedQueryCollection), &plugin.StorageListFilter{
		KeyPrefix:   savedQueryPrefix,
		ContentType: "application/json",
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	rows := make([]row, 0, len(items))
	for _, item := range items {
		value, err := decodeSavedQuery(item)
		if err != nil {
			return nil, err
		}
		rows = append(rows, value)
	}
	sort.Slice(rows, func(i, j int) bool {
		return strings.ToLower(text(rows[i]["name"])) < strings.ToLower(text(rows[j]["name"]))
	})
	total := len(rows)
	return plugin.Page[row]{Items: rows, Total: &total}, nil
}

func decodeSavedQuery(item plugin.StorageItem) (row, error) {
	var value savedQuery
	if err := json.Unmarshal(item.Value, &value); err != nil {
		return nil, fmt.Errorf("%w: stored query %q is corrupt", plugin.ErrUnavailable, item.Key)
	}
	return row{
		"ref":        plugin.ResourceIdentity{Kind: "saved_query", Name: value.Name, UID: item.Key},
		"id":         item.Key,
		"name":       value.Name,
		"kind":       value.Kind,
		"collection": value.Collection,
		"query":      value.Query,
		"updated":    item.UpdatedAt.UTC().Format(time.RFC3339),
	}, nil
}

func readSavedQuery(rc *plugin.RequestContext) (any, error) {
	if rc.Storage == nil {
		return nil, fmt.Errorf("%w: plugin storage is unavailable", plugin.ErrUnavailable)
	}
	key, err := savedQueryKey(rc)
	if err != nil {
		return nil, err
	}
	item, err := rc.Storage.Get(rc.Ctx, plugin.ConnectionStorage(savedQueryCollection), key)
	if err != nil {
		return nil, err
	}
	value, err := decodeSavedQuery(item)
	if err != nil {
		return nil, err
	}
	value["content"] = value["query"]
	return value, nil
}

func saveQuery(rc *plugin.RequestContext) (any, error) {
	if rc.Storage == nil {
		return nil, fmt.Errorf("%w: plugin storage is unavailable", plugin.ErrUnavailable)
	}
	var req struct {
		Name       string `json:"name" validate:"required"`
		Kind       string `json:"kind" validate:"required"`
		Collection string `json:"collection"`
		Query      string `json:"query" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	value, err := normalizeSavedQuery(req.Name, req.Kind, req.Collection, req.Query)
	if err != nil {
		return nil, err
	}
	id, err := randomID()
	if err != nil {
		return nil, err
	}
	return putSavedQuery(rc, savedQueryPrefix+id, value)
}

func updateSavedQuery(rc *plugin.RequestContext) (any, error) {
	if rc.Storage == nil {
		return nil, fmt.Errorf("%w: plugin storage is unavailable", plugin.ErrUnavailable)
	}
	key, err := savedQueryKey(rc)
	if err != nil {
		return nil, err
	}
	existing, err := rc.Storage.Get(rc.Ctx, plugin.ConnectionStorage(savedQueryCollection), key)
	if err != nil {
		return nil, err
	}
	var current savedQuery
	if err := json.Unmarshal(existing.Value, &current); err != nil {
		return nil, fmt.Errorf("%w: stored query %q is corrupt", plugin.ErrUnavailable, key)
	}
	next, err := applySavedQueryPatch(current, rc.Body())
	if err != nil {
		return nil, err
	}
	value, err := normalizeSavedQuery(next.Name, next.Kind, next.Collection, next.Query)
	if err != nil {
		return nil, err
	}
	return putSavedQuery(rc, key, value)
}

// applySavedQueryPatch merges an update body onto the stored record. The code
// editor sends {"content": "..."} where the content is itself JSON, so the query
// text is never re-parsed as saved-query metadata.
func applySavedQueryPatch(current savedQuery, body []byte) (savedQuery, error) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil {
		return savedQuery{}, fmt.Errorf("%w: body must be a JSON object", plugin.ErrInvalidInput)
	}
	next := current
	text := func(key string, target *string) error {
		raw, ok := envelope[key]
		if !ok {
			return nil
		}
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return fmt.Errorf("%w: %s must be a string", plugin.ErrInvalidInput, key)
		}
		*target = value
		return nil
	}
	if _, ok := envelope["content"]; ok {
		if err := text("content", &next.Query); err != nil {
			return savedQuery{}, err
		}
	} else if err := text("query", &next.Query); err != nil {
		return savedQuery{}, err
	}
	for key, target := range map[string]*string{"name": &next.Name, "kind": &next.Kind, "collection": &next.Collection} {
		if err := text(key, target); err != nil {
			return savedQuery{}, err
		}
	}
	return next, nil
}

func putSavedQuery(rc *plugin.RequestContext, key string, value savedQuery) (any, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	item, err := rc.Storage.Put(rc.Ctx, savedQueryCollection, plugin.StorageItem{
		Key:         key,
		Value:       payload,
		ContentType: "application/json",
		Metadata:    map[string]string{"name": value.Name, "kind": value.Kind},
	})
	if err != nil {
		return nil, err
	}
	out, err := decodeSavedQuery(item)
	if err != nil {
		return nil, err
	}
	out["content"] = value.Query
	return out, nil
}

func normalizeSavedQuery(name, kind, collection, query string) (savedQuery, error) {
	value := savedQuery{
		Name:       strings.TrimSpace(name),
		Kind:       strings.ToLower(strings.TrimSpace(kind)),
		Collection: strings.TrimSpace(collection),
		Query:      strings.TrimSpace(query),
	}
	if value.Name == "" || value.Query == "" {
		return savedQuery{}, fmt.Errorf("%w: name and query are required", plugin.ErrInvalidInput)
	}
	if value.Kind != "graphql" && value.Kind != "search" {
		return savedQuery{}, fmt.Errorf("%w: kind must be graphql or search", plugin.ErrInvalidInput)
	}
	if value.Collection != "" {
		if _, err := validName(value.Collection, classNamePattern, "collection"); err != nil {
			return savedQuery{}, err
		}
	}
	return value, nil
}

func deleteSavedQuery(rc *plugin.RequestContext) (any, error) {
	if rc.Storage == nil {
		return nil, fmt.Errorf("%w: plugin storage is unavailable", plugin.ErrUnavailable)
	}
	key, err := savedQueryKey(rc)
	if err != nil {
		return nil, err
	}
	if err := rc.Storage.Delete(rc.Ctx, plugin.ConnectionStorage(savedQueryCollection), key); err != nil {
		return nil, err
	}
	return row{"ok": true, "id": key}, nil
}

func savedQueryKey(rc *plugin.RequestContext) (string, error) {
	key := strings.TrimSpace(rc.Param("id"))
	if !strings.HasPrefix(key, savedQueryPrefix) || len(key) <= len(savedQueryPrefix) {
		return "", fmt.Errorf("%w: saved query id must look like %s<id>", plugin.ErrInvalidInput, savedQueryPrefix)
	}
	return key, nil
}

func listActivity(rc *plugin.RequestContext) (any, error) {
	if rc.Storage == nil {
		return plugin.Page[row]{Items: []row{}}, nil
	}
	items, err := rc.Storage.List(rc.Ctx, plugin.ConnectionStorage(activityCollection), &plugin.StorageListFilter{
		KeyPrefix:   activityPrefix,
		ContentType: "application/json",
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	rows := make([]row, 0, len(items))
	for _, item := range items {
		var event activityEvent
		if err := json.Unmarshal(item.Value, &event); err != nil {
			continue
		}
		rows = append(rows, row{
			"ref":      plugin.ResourceIdentity{Kind: "activity", Name: item.Key, UID: item.Key},
			"id":       item.Key,
			"time":     event.Time,
			"category": event.Category,
			"title":    event.Title,
			"message":  event.Message,
			"severity": event.Severity,
			"icon":     activityIcons[event.Category],
		})
	}
	sort.Slice(rows, func(i, j int) bool { return text(rows[i]["time"]) > text(rows[j]["time"]) })
	total := len(rows)
	return plugin.Page[row]{Items: rows, Total: &total}, nil
}

var activityIcons = map[string]string{
	"schema":     "layers",
	"objects":    "braces",
	"references": "link",
	"query":      "search",
	"tenants":    "users",
	"aliases":    "tag",
	"shards":     "server",
	"backups":    "archive",
}

// record appends a connection-scoped activity entry. Failures are ignored: the
// operation the caller performed already succeeded and the feed is advisory.
func record(rc *plugin.RequestContext, category string, severity plugin.Severity, title, message string) {
	if rc.Storage == nil {
		return
	}
	id, err := randomID()
	if err != nil {
		return
	}
	stamp := time.Now().UTC()
	payload, err := json.Marshal(activityEvent{
		Category: category,
		Title:    title,
		Message:  message,
		Severity: string(severity),
		Time:     stamp.Format(time.RFC3339Nano),
	})
	if err != nil {
		return
	}
	key := fmt.Sprintf("%s%019d-%s", activityPrefix, stamp.UnixNano(), id)
	if _, err := rc.Storage.Put(rc.Ctx, activityCollection, plugin.StorageItem{
		Key:         key,
		Value:       payload,
		ContentType: "application/json",
		Metadata:    map[string]string{"category": category},
	}); err != nil {
		return
	}
	pruneActivity(rc)
}

func pruneActivity(rc *plugin.RequestContext) {
	scope := plugin.ConnectionStorage(activityCollection)
	items, err := rc.Storage.List(rc.Ctx, scope, &plugin.StorageListFilter{KeyPrefix: activityPrefix})
	if err != nil || len(items) <= activityRetention {
		return
	}
	keys := make([]string, 0, len(items))
	for _, item := range items {
		keys = append(keys, item.Key)
	}
	sort.Strings(keys)
	for _, key := range keys[:len(keys)-activityRetention] {
		_ = rc.Storage.Delete(rc.Ctx, scope, key)
	}
}

func randomID() (string, error) {
	buffer := make([]byte, 12)
	if _, err := rand.Read(buffer); err != nil {
		return "", fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	return hex.EncodeToString(buffer), nil
}
