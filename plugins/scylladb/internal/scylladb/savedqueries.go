package scylladb

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	savedQueryCollection   = "saved_queries"
	savedQueryContentType  = "application/json"
	storageScopeConnection = "connection"
	storageScopeUser       = "user"
)

type savedQuery struct {
	Name  string `json:"name"`
	Query string `json:"query"`
	Scope string `json:"scope"`
}

func savedQueryKey(scope string) string {
	return scope + "/" + uuid.NewString()
}

func savedQueryScopeOf(key string) string {
	if strings.HasPrefix(key, storageScopeUser+"/") {
		return storageScopeUser
	}
	return storageScopeConnection
}

func listSavedQueries(rc *plugin.RequestContext) (any, error) {
	if rc.Storage == nil {
		return plugin.Page[row]{Items: []row{}}, nil
	}
	seen := map[string]bool{}
	out := []row{}
	for _, scope := range []plugin.StorageScope{
		plugin.ConnectionStorage(savedQueryCollection),
		plugin.UserStorage(savedQueryCollection),
	} {
		items, err := rc.Storage.List(rc.Ctx, scope, &plugin.StorageListFilter{ContentType: savedQueryContentType})
		if err != nil {
			return nil, fmt.Errorf("%w: read saved CQL: %v", plugin.ErrUnavailable, err)
		}
		for _, item := range items {
			if seen[item.Key] || (scope.Level == plugin.StorageScopeUser && savedQueryScopeOf(item.Key) != storageScopeUser) {
				continue
			}
			seen[item.Key] = true
			decoded, err := decodeSavedQuery(item)
			if err != nil {
				return nil, err
			}
			out = append(out, decoded)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return strings.ToLower(fmt.Sprint(out[i]["name"])) < strings.ToLower(fmt.Sprint(out[j]["name"]))
	})
	return pageRows(rc, out)
}

func decodeSavedQuery(item plugin.StorageItem) (row, error) {
	var value savedQuery
	if err := json.Unmarshal(item.Value, &value); err != nil {
		return nil, fmt.Errorf("%w: saved CQL %q is corrupt", plugin.ErrUnavailable, item.Key)
	}
	return row{
		"key":        item.Key,
		"name":       value.Name,
		"query":      value.Query,
		"scope":      savedQueryScopeOf(item.Key),
		"updated_at": item.UpdatedAt.Format(time.RFC3339Nano),
	}, nil
}

func saveQuery(rc *plugin.RequestContext) (any, error) {
	if rc.Storage == nil {
		return nil, fmt.Errorf("%w: plugin storage is unavailable", plugin.ErrUnavailable)
	}
	var req savedQuery
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	req.Name = strings.TrimSpace(req.Name)
	req.Query = strings.TrimSpace(req.Query)
	if req.Name == "" || req.Query == "" {
		return nil, fmt.Errorf("%w: name and CQL are required", plugin.ErrInvalidInput)
	}
	switch req.Scope {
	case "", storageScopeConnection:
		req.Scope = storageScopeConnection
	case storageScopeUser:
	default:
		return nil, fmt.Errorf("%w: unsupported scope", plugin.ErrInvalidInput)
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	item, err := rc.Storage.Put(rc.Ctx, savedQueryCollection, plugin.StorageItem{
		Key:         savedQueryKey(req.Scope),
		Value:       body,
		ContentType: savedQueryContentType,
		Metadata:    map[string]string{"name": req.Name},
	})
	if err != nil {
		return nil, fmt.Errorf("%w: save CQL: %v", plugin.ErrUnavailable, err)
	}
	return decodeSavedQuery(item)
}

func deleteSavedQuery(rc *plugin.RequestContext) (any, error) {
	if rc.Storage == nil {
		return nil, fmt.Errorf("%w: plugin storage is unavailable", plugin.ErrUnavailable)
	}
	key := strings.TrimSpace(rc.Param("key"))
	if key == "" {
		return nil, fmt.Errorf("%w: saved CQL key is required", plugin.ErrInvalidInput)
	}
	scope := plugin.ConnectionStorage(savedQueryCollection)
	if savedQueryScopeOf(key) == storageScopeUser {
		scope = plugin.UserStorage(savedQueryCollection)
	}
	if err := rc.Storage.Delete(rc.Ctx, scope, key); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func savedQuerySchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Saved CQL", Fields: []plugin.Field{
		{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorMax, Value: 120}}},
		{Key: "query", Label: "CQL", Type: plugin.FieldTextarea, Required: true, Placeholder: "SELECT * FROM ks.table LIMIT 100;"},
		{Key: "scope", Label: "Scope", Type: plugin.FieldSelect, Required: true, Default: storageScopeConnection, Options: []plugin.Option{
			{Label: "This connection", Value: storageScopeConnection},
			{Label: "All my ScyllaDB connections", Value: storageScopeUser},
		}},
	}}}}
}
