package arangodb

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

const savedCollection = "saved_queries"

type savedQuery struct {
	Name     string `json:"name"`
	Database string `json:"database,omitempty"`
	Query    string `json:"query"`
	Notes    string `json:"notes,omitempty"`
}

type savedQueryRow struct {
	Ref       plugin.ResourceIdentity `json:"ref"`
	ID        string                  `json:"id"`
	Name      string                  `json:"name"`
	Database  string                  `json:"database,omitempty"`
	Query     string                  `json:"query"`
	Notes     string                  `json:"notes,omitempty"`
	Writes    bool                    `json:"writes"`
	UpdatedAt time.Time               `json:"updated_at"`
}

func savedQuerySchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Saved query", Fields: []plugin.Field{
		{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true, Placeholder: "Top customers by orders"},
		{Key: "database", Label: "Database", Type: plugin.FieldText, Placeholder: defaultDatabase,
			Help: "Optional. Recorded so the query can be replayed against the right database."},
		{Key: "query", Label: "AQL", Type: plugin.FieldTextarea, Required: true, Placeholder: "FOR doc IN customers LIMIT 25 RETURN doc"},
		{Key: "notes", Label: "Notes", Type: plugin.FieldTextarea},
	}}}}
}

func listSavedQueries(rc *plugin.RequestContext) (any, error) {
	items, err := rc.Storage.List(rc.Ctx, plugin.UserStorage(savedCollection), &plugin.StorageListFilter{
		KeyPrefix: "query/", ContentType: "application/json",
	})
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(items))
	for _, item := range items {
		saved, err := savedRowFromItem(item)
		if err != nil {
			return nil, err
		}
		rows = append(rows, structRow(saved))
	}
	sort.Slice(rows, func(i, j int) bool {
		return strings.ToLower(compact(rows[i]["name"])) < strings.ToLower(compact(rows[j]["name"]))
	})
	return pageRows(rc, rows)
}

func readSavedQuery(rc *plugin.RequestContext) (any, error) {
	item, err := rc.Storage.Get(rc.Ctx, plugin.UserStorage(savedCollection), rc.Param("id"))
	if err != nil {
		return nil, err
	}
	return savedRowFromItem(item)
}

func createSavedQuery(rc *plugin.RequestContext) (any, error) {
	var in savedQuery
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	in.Name = strings.TrimSpace(in.Name)
	in.Query = strings.TrimSpace(in.Query)
	if in.Name == "" || in.Query == "" {
		return nil, fmt.Errorf("%w: name and query are required", plugin.ErrInvalidInput)
	}
	if in.Database != "" {
		if _, err := safeDatabaseName(in.Database); err != nil {
			return nil, err
		}
	}
	body, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	item, err := rc.Storage.Put(rc.Ctx, savedCollection, plugin.StorageItem{
		Key:         "query/" + newID(),
		Value:       body,
		ContentType: "application/json",
		Metadata:    map[string]string{"name": in.Name, "database": in.Database},
	})
	if err != nil {
		return nil, err
	}
	return savedRowFromItem(item)
}

func deleteSavedQuery(rc *plugin.RequestContext) (any, error) {
	if err := rc.Storage.Delete(rc.Ctx, plugin.UserStorage(savedCollection), rc.Param("id")); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func savedRowFromItem(item plugin.StorageItem) (savedQueryRow, error) {
	var value savedQuery
	if err := json.Unmarshal(item.Value, &value); err != nil {
		return savedQueryRow{}, fmt.Errorf("%w: stored query is corrupt", plugin.ErrUnavailable)
	}
	return savedQueryRow{
		Ref:       plugin.ResourceIdentity{Kind: kindSavedQuery, Namespace: value.Database, Name: value.Name, UID: item.Key},
		ID:        item.Key,
		Name:      value.Name,
		Database:  value.Database,
		Query:     value.Query,
		Notes:     value.Notes,
		Writes:    aqlWrites(value.Query),
		UpdatedAt: item.UpdatedAt,
	}, nil
}

func newID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return strings.ReplaceAll(time.Now().UTC().Format("20060102150405.000000000"), ".", "")
	}
	return hex.EncodeToString(buf)
}
