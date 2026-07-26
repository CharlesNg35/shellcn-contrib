package chroma

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	savedQueryCollection = "saved_queries"
	savedQueryPrefix     = "query/"
	historyCollection    = "query_history"
	historyPrefix        = "history/"
	historyKeep          = 100
)

type savedQuery struct {
	Name       string `json:"name"`
	Query      string `json:"query"`
	Collection string `json:"collection,omitempty"`
}

func queriesList(rc *plugin.RequestContext) (any, error) {
	if rc.Storage == nil {
		return nil, fmt.Errorf("%w: plugin storage is unavailable", plugin.ErrNotSupported)
	}
	items, err := rc.Storage.List(rc.Ctx, plugin.UserStorage(savedQueryCollection), &plugin.StorageListFilter{
		KeyPrefix: savedQueryPrefix, ContentType: "application/json",
	})
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(items))
	for _, item := range items {
		value, err := decodeSavedQuery(item)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row{
			"id":         strings.TrimPrefix(item.Key, savedQueryPrefix),
			"name":       value.Name,
			"query":      value.Query,
			"collection": value.Collection,
			"updated_at": item.UpdatedAt,
		})
	}
	sort.SliceStable(rows, func(i, j int) bool {
		return strings.ToLower(fmt.Sprint(rows[i]["name"])) < strings.ToLower(fmt.Sprint(rows[j]["name"]))
	})
	return broker.PageRows(rc, rows)
}

func queriesSave(rc *plugin.RequestContext) (any, error) {
	if rc.Storage == nil {
		return nil, fmt.Errorf("%w: plugin storage is unavailable", plugin.ErrNotSupported)
	}
	var req savedQuery
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	req.Name = strings.TrimSpace(req.Name)
	req.Query = strings.TrimSpace(req.Query)
	if req.Name == "" || req.Query == "" {
		return nil, fmt.Errorf("%w: name and query are required", plugin.ErrInvalidInput)
	}
	var probe any
	if err := json.Unmarshal([]byte(req.Query), &probe); err != nil {
		return nil, fmt.Errorf("%w: query must be valid JSON", plugin.ErrInvalidInput)
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	id := newQueryID()
	item, err := rc.Storage.Put(rc.Ctx, savedQueryCollection, plugin.StorageItem{
		Key:         savedQueryPrefix + id,
		Value:       body,
		ContentType: "application/json",
		Metadata:    map[string]string{"name": req.Name, "collection": req.Collection},
	})
	if err != nil {
		return nil, err
	}
	return row{"ok": true, "id": id, "name": req.Name, "updated_at": item.UpdatedAt}, nil
}

func queriesDelete(rc *plugin.RequestContext) (any, error) {
	if rc.Storage == nil {
		return nil, fmt.Errorf("%w: plugin storage is unavailable", plugin.ErrNotSupported)
	}
	id := strings.TrimSpace(rc.Param("id"))
	if id == "" {
		return nil, fmt.Errorf("%w: saved query id is required", plugin.ErrInvalidInput)
	}
	if err := rc.Storage.Delete(rc.Ctx, plugin.UserStorage(savedQueryCollection), savedQueryPrefix+id); err != nil {
		return nil, err
	}
	return row{"ok": true, "id": id}, nil
}

func decodeSavedQuery(item plugin.StorageItem) (savedQuery, error) {
	var value savedQuery
	if err := json.Unmarshal(item.Value, &value); err != nil {
		return savedQuery{}, fmt.Errorf("%w: saved query %q is corrupt", plugin.ErrUnavailable, item.Key)
	}
	return value, nil
}

type historyEntry struct {
	Statement string  `json:"statement"`
	Mode      string  `json:"mode"`
	Query     string  `json:"query"`
	RowCount  int     `json:"rowCount"`
	ElapsedMs float64 `json:"elapsedMs"`
	Error     string  `json:"error,omitempty"`
}

// recordHistory keeps a per-collection execution trail for the timeline panel.
// Failures to persist never break a query result.
func recordHistory(rc *plugin.RequestContext, col collection, query string, result row, runErr error) {
	if rc.Storage == nil {
		return
	}
	entry := historyEntry{Query: strings.TrimSpace(query)}
	if runErr != nil {
		entry.Error = runErr.Error()
		entry.Mode = "error"
		entry.Statement = "failed execution"
	} else {
		entry.Statement = fmt.Sprint(result["statement"])
		entry.Mode = fmt.Sprint(result["mode"])
		if count, ok := result["rowCount"].(int); ok {
			entry.RowCount = count
		}
		if ms, ok := result["elapsedMs"].(float64); ok {
			entry.ElapsedMs = ms
		}
	}
	body, err := json.Marshal(entry)
	if err != nil {
		return
	}
	key := historyKey(col.ID, time.Now().UTC())
	if _, err := rc.Storage.Put(rc.Ctx, historyCollection, plugin.StorageItem{
		Key:         key,
		Value:       body,
		ContentType: "application/json",
		Metadata:    map[string]string{"collection": col.Name},
	}); err != nil {
		return
	}
	pruneHistory(rc, col.ID)
}

func historyKey(collectionID string, at time.Time) string {
	return historyPrefix + collectionID + "/" + strconv.FormatInt(at.UnixNano(), 10)
}

func pruneHistory(rc *plugin.RequestContext, collectionID string) {
	items, err := rc.Storage.List(rc.Ctx, plugin.ConnectionStorage(historyCollection), &plugin.StorageListFilter{
		KeyPrefix: historyPrefix + collectionID + "/",
	})
	if err != nil || len(items) <= historyKeep {
		return
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	for _, item := range items[:len(items)-historyKeep] {
		_ = rc.Storage.Delete(rc.Ctx, plugin.ConnectionStorage(historyCollection), item.Key)
	}
}

func historyList(rc *plugin.RequestContext) (any, error) {
	if rc.Storage == nil {
		return nil, fmt.Errorf("%w: plugin storage is unavailable", plugin.ErrNotSupported)
	}
	_, _, col, err := collectionOf(rc)
	if err != nil {
		return nil, err
	}
	items, err := rc.Storage.List(rc.Ctx, plugin.ConnectionStorage(historyCollection), &plugin.StorageListFilter{
		KeyPrefix: historyPrefix + col.ID + "/",
	})
	if err != nil {
		return nil, err
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].Key > items[j].Key })
	rows := make([]row, 0, len(items))
	for _, item := range items {
		var entry historyEntry
		if err := json.Unmarshal(item.Value, &entry); err != nil {
			continue
		}
		severity, iconName := "info", "search"
		body := entry.Query
		if entry.Error != "" {
			severity, iconName = "danger", "circle-alert"
			body = entry.Error + "\n\n" + entry.Query
		} else if entry.RowCount == 0 {
			severity, iconName = "warn", "search-x"
		}
		rows = append(rows, row{
			"id":        item.Key,
			"time":      item.CreatedAt,
			"title":     entry.Statement,
			"message":   body,
			"severity":  severity,
			"icon":      iconName,
			"resource":  col.Name,
			"rows":      entry.RowCount,
			"elapsedMs": entry.ElapsedMs,
		})
	}
	return broker.PageRows(rc, rows)
}

func historyClear(rc *plugin.RequestContext) (any, error) {
	if rc.Storage == nil {
		return nil, fmt.Errorf("%w: plugin storage is unavailable", plugin.ErrNotSupported)
	}
	_, _, col, err := collectionOf(rc)
	if err != nil {
		return nil, err
	}
	items, err := rc.Storage.List(rc.Ctx, plugin.ConnectionStorage(historyCollection), &plugin.StorageListFilter{
		KeyPrefix: historyPrefix + col.ID + "/",
	})
	if err != nil {
		return nil, err
	}
	removed := 0
	for _, item := range items {
		if err := rc.Storage.Delete(rc.Ctx, plugin.ConnectionStorage(historyCollection), item.Key); err == nil {
			removed++
		}
	}
	return row{"ok": true, "deleted": removed}, nil
}

// newQueryID keeps saved-query keys unique across every connection of this user:
// the library is read and deleted with user scope, and a key shared by two
// connections makes those lookups ambiguous.
func newQueryID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return strconv.FormatInt(time.Now().UTC().UnixNano(), 36)
	}
	return hex.EncodeToString(buf)
}
