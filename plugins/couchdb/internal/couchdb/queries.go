package couchdb

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

const savedQueryCollection = "saved_queries"

const savedQueryPrefix = "query/"

type savedQuery struct {
	Name     string `json:"name"`
	Database string `json:"database"`
	Query    string `json:"query"`
}

func storageOf(rc *plugin.RequestContext) (plugin.Storage, error) {
	if rc.Storage == nil {
		return nil, fmt.Errorf("%w: plugin storage is unavailable on this request", plugin.ErrUnavailable)
	}
	return rc.Storage, nil
}

func savedQueryRow(item plugin.StorageItem) (row, error) {
	var value savedQuery
	if err := json.Unmarshal(item.Value, &value); err != nil {
		return nil, fmt.Errorf("%w: saved query %q is corrupt", plugin.ErrUnavailable, item.Key)
	}
	return row{
		"key":        item.Key,
		"name":       value.Name,
		"database":   value.Database,
		"query":      value.Query,
		"updated_at": item.UpdatedAt.UTC().Format(time.RFC3339),
	}, nil
}

// listSavedQueries returns this connection's saved Mango queries, narrowed to
// the active database when the panel supplies one.
func listSavedQueries(rc *plugin.RequestContext) (any, error) {
	store, err := storageOf(rc)
	if err != nil {
		return nil, err
	}
	items, err := store.List(rc.Ctx, plugin.ConnectionStorage(savedQueryCollection), &plugin.StorageListFilter{
		KeyPrefix:   savedQueryPrefix,
		ContentType: "application/json",
	})
	if err != nil {
		return nil, err
	}
	db := strings.TrimSpace(rc.Param("db"))
	rows := make([]row, 0, len(items))
	for _, item := range items {
		decoded, err := savedQueryRow(item)
		if err != nil {
			return nil, err
		}
		if db != "" && stringOf(decoded["database"]) != db {
			continue
		}
		rows = append(rows, decoded)
	}
	page, err := rc.Page()
	if err != nil {
		return nil, err
	}
	rows = plugin.FilterRows(rows, page.Search())
	if len(page.Sort) > 0 {
		rows = plugin.SortRows(rows, page.Sort)
	} else {
		sort.SliceStable(rows, func(i, j int) bool {
			return strings.ToLower(stringOf(rows[i]["name"])) < strings.ToLower(stringOf(rows[j]["name"]))
		})
	}
	return plugin.Page[row]{Items: rows}, nil
}

func saveQuery(rc *plugin.RequestContext) (any, error) {
	store, err := storageOf(rc)
	if err != nil {
		return nil, err
	}
	db, err := dbParam(rc)
	if err != nil {
		return nil, err
	}
	var in struct {
		Name  string `json:"name" validate:"required"`
		Query any    `json:"query" validate:"required"`
	}
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	text := jsonText(in.Query)
	var probe map[string]any
	if err := json.Unmarshal([]byte(text), &probe); err != nil {
		return nil, fmt.Errorf("%w: the saved query must be a JSON object", plugin.ErrInvalidInput)
	}
	if probe["selector"] == nil {
		return nil, fmt.Errorf("%w: a saved Mango query needs a \"selector\"", plugin.ErrInvalidInput)
	}
	pretty, err := json.MarshalIndent(probe, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("%w: %v", plugin.ErrInvalidInput, err)
	}

	value, err := json.Marshal(savedQuery{Name: strings.TrimSpace(in.Name), Database: db, Query: string(pretty)})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", plugin.ErrInvalidInput, err)
	}
	item, err := store.Put(rc.Ctx, savedQueryCollection, plugin.StorageItem{
		Key:         savedQueryPrefix + newID(),
		Value:       value,
		ContentType: "application/json",
		Metadata:    map[string]string{"name": strings.TrimSpace(in.Name), "database": db},
	})
	if err != nil {
		return nil, err
	}
	return savedQueryRow(item)
}

func deleteSavedQuery(rc *plugin.RequestContext) (any, error) {
	store, err := storageOf(rc)
	if err != nil {
		return nil, err
	}
	key := strings.TrimSpace(rc.Param("key"))
	if !strings.HasPrefix(key, savedQueryPrefix) {
		return nil, fmt.Errorf("%w: %q is not a saved query key", plugin.ErrInvalidInput, key)
	}
	if err := store.Delete(rc.Ctx, plugin.ConnectionStorage(savedQueryCollection), key); err != nil {
		return nil, err
	}
	return row{"ok": true, "key": key}, nil
}

func newID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return hex.EncodeToString([]byte(time.Now().UTC().Format(time.RFC3339Nano)))
	}
	return hex.EncodeToString(buf)
}
