package couchdb

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

// dbsInfoBatch is the server-side cap on /_dbs_info keys
// (max_db_number_for_dbs_info_req), so the enrichment is chunked to match.
const dbsInfoBatch = 100

// databaseInfos reads the info document of the names on the current page only.
// CouchDB 2.2+ answers /_dbs_info in one round trip per batch; older nodes fall
// back to one GET per database, still bounded by the page.
func (s *Session) databaseInfos(rc *plugin.RequestContext, names []string) ([]row, error) {
	out := make([]row, 0, len(names))
	for start := 0; start < len(names); start += dbsInfoBatch {
		end := min(start+dbsInfoBatch, len(names))
		out = append(out, s.databaseInfoBatch(rc, names[start:end])...)
	}
	return out, nil
}

func (s *Session) databaseInfoBatch(rc *plugin.RequestContext, names []string) []row {
	var bulk []struct {
		Key  string `json:"key"`
		Info row    `json:"info"`
	}
	if err := s.client.do(rc.Ctx, http.MethodPost, "/_dbs_info", nil, row{"keys": names}, &bulk); err == nil && len(bulk) > 0 {
		out := make([]row, 0, len(bulk))
		for _, entry := range bulk {
			if entry.Info == nil {
				continue
			}
			out = append(out, databaseRow(entry.Key, entry.Info))
		}
		return out
	}

	out := make([]row, 0, len(names))
	for _, name := range names {
		var info row
		if err := s.client.do(rc.Ctx, http.MethodGet, dbPath(name), nil, nil, &info); err != nil {
			continue
		}
		out = append(out, databaseRow(name, info))
	}
	return out
}

// databaseRow flattens a CouchDB db info document and adds the fragmentation
// ratio operators actually compact on.
func databaseRow(name string, info row) row {
	if name == "" {
		name = stringOf(info["db_name"])
	}
	sizes := mapOf(info["sizes"])
	disk := numberOf(sizes["file"])
	active := numberOf(sizes["active"])
	if disk == 0 {
		disk = numberOf(info["disk_size"])
	}
	if active == 0 {
		active = numberOf(info["data_size"])
	}
	item := row{
		"name":            name,
		"doc_count":       numberOf(info["doc_count"]),
		"doc_del_count":   numberOf(info["doc_del_count"]),
		"update_seq":      stringOf(info["update_seq"]),
		"purge_seq":       stringOf(info["purge_seq"]),
		"disk_size":       disk,
		"data_size":       active,
		"partitioned":     databasePartitioned(info),
		"compact_running": info["compact_running"] == true,
		"cluster":         info["cluster"],
		"ref":             plugin.ResourceIdentity{Kind: "database", Name: name, UID: name},
	}
	if disk > 0 {
		item["fragmentation"] = round1((disk - active) / disk * 100)
	} else {
		item["fragmentation"] = 0.0
	}
	return item
}

func databasePartitioned(info row) bool {
	if props := mapOf(info["props"]); props != nil {
		return props["partitioned"] == true
	}
	return false
}

// databaseScanWindows bounds how many /_all_dbs windows one search request
// walks. CouchDB has no server-side name filter, so a search term is matched by
// scanning names, which stay cheap; only the resulting page is ever enriched.
const databaseScanWindows = 20

// databasePage resolves the requested page of database names. An unfiltered page
// rides CouchDB's key cursor; a search scans a bounded prefix of the name space
// and pages the matches by offset, the way every other grid does. Total is set
// only when the walk provably saw every name.
func databasePage(rc *plugin.RequestContext, s *Session) ([]string, string, *int, error) {
	page, err := rc.Page()
	if err != nil {
		return nil, "", nil, err
	}
	desc := databaseSortDesc(page.Sort)
	if q := page.Search(); q != "" {
		return s.databaseSearchPage(rc, page.Cursor, q, page.Limit, desc)
	}
	names, next, err := s.databaseNamePage(rc, page.Cursor, page.Limit, desc)
	if err != nil {
		return nil, "", nil, err
	}
	if page.Cursor == "" && next == "" {
		total := len(names)
		return names, next, &total, nil
	}
	return names, next, nil, nil
}

// databaseSortDesc reports the direction for the only column CouchDB can order
// server-side; the info-derived columns are not advertised as sortable.
func databaseSortDesc(sort []plugin.SortKey) bool {
	for _, key := range sort {
		if key.Field == "name" {
			return key.Desc
		}
	}
	return false
}

func (s *Session) databaseSearchPage(rc *plugin.RequestContext, cursor, q string, limit int, desc bool) ([]string, string, *int, error) {
	offset := 0
	if cursor != "" {
		parsed, err := strconv.Atoi(cursor)
		if err != nil || parsed < 0 {
			return nil, "", nil, fmt.Errorf("%w: invalid cursor", plugin.ErrInvalidInput)
		}
		offset = parsed
	}
	needle := strings.ToLower(q)
	matched := make([]string, 0, min(offset+limit+1, databaseScanWindows*plugin.MaxPageLimit))
	resume, exhausted := "", false
	for range databaseScanWindows {
		names, next, err := s.databaseNamePage(rc, resume, plugin.MaxPageLimit, desc)
		if err != nil {
			return nil, "", nil, err
		}
		for _, name := range names {
			if strings.Contains(strings.ToLower(name), needle) {
				matched = append(matched, name)
			}
		}
		resume = next
		if next == "" {
			exhausted = true
			break
		}
		if len(matched) > offset+limit {
			break
		}
	}
	start := min(offset, len(matched))
	end := min(start+limit, len(matched))
	next := ""
	if end < len(matched) {
		next = strconv.Itoa(end)
	}
	if exhausted {
		total := len(matched)
		return matched[start:end], next, &total, nil
	}
	return matched[start:end], next, nil, nil
}

func listDatabases(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	names, next, total, err := databasePage(rc, s)
	if err != nil {
		return nil, err
	}
	rows, err := s.databaseInfos(rc, names)
	if err != nil {
		return nil, err
	}
	return plugin.Page[row]{Items: rows, NextCursor: next, Total: total}, nil
}

func treeDatabases(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	names, next, total, err := databasePage(rc, s)
	if err != nil {
		return nil, err
	}
	nodes := make([]plugin.TreeNode, 0, len(names))
	for _, name := range names {
		ref := plugin.ResourceIdentity{Kind: "database", Name: name, UID: name}
		nodes = append(nodes, plugin.TreeNode{
			Key: "database:" + name, Label: name, Icon: icon("database"), Ref: &ref, Leaf: true,
		})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: next, Total: total}, nil
}

// watchDatabases refreshes only the page the panel is showing; enumerating every
// database on each tick is unbounded on a per-user-database deployment.
func watchDatabases(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := session(rc)
	if err != nil {
		return err
	}
	return pollWatch(rc, client, 10*time.Second, func() ([]plugin.ResourceEvent, error) {
		names, _, _, err := databasePage(rc, s)
		if err != nil {
			return nil, err
		}
		rows, err := s.databaseInfos(rc, names)
		if err != nil {
			return nil, err
		}
		return modifiedEvents(rows, "database", func(item row) plugin.ResourceIdentity {
			name := stringOf(item["name"])
			return plugin.ResourceIdentity{Name: name, UID: name}
		}), nil
	})
}

func (s *Session) databaseInfo(rc *plugin.RequestContext, db string) (row, error) {
	var info row
	if err := s.client.do(rc.Ctx, http.MethodGet, dbPath(db), nil, nil, &info); err != nil {
		return nil, err
	}
	return databaseRow(db, info), nil
}

func readDatabase(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	return s.databaseInfo(rc, db)
}

func watchDatabase(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, db, err := dbSession(rc)
	if err != nil {
		return err
	}
	return pollObject(rc, client, 5*time.Second, func() (any, string, error) {
		info, err := s.databaseInfo(rc, db)
		if err != nil {
			return nil, "", err
		}
		version := stringOf(info["update_seq"]) + "|" + strconv.FormatBool(info["compact_running"] == true)
		return info, version, nil
	})
}

func createDatabase(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var in struct {
		Name        string `json:"name" validate:"required"`
		Shards      int    `json:"shards"`
		Replicas    int    `json:"replicas"`
		Partitioned bool   `json:"partitioned"`
	}
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	if !databaseNamePattern.MatchString(in.Name) {
		return nil, invalidDatabaseName(in.Name)
	}
	query := url.Values{}
	if in.Shards > 0 {
		query.Set("q", strconv.Itoa(in.Shards))
	}
	if in.Replicas > 0 {
		query.Set("n", strconv.Itoa(in.Replicas))
	}
	if in.Partitioned {
		query.Set("partitioned", "true")
	}
	var out row
	if err := s.client.do(rc.Ctx, http.MethodPut, dbPath(in.Name), query, nil, &out); err != nil {
		return nil, err
	}
	return row{"ok": true, "name": in.Name}, nil
}

func invalidDatabaseName(name string) error {
	return httpError(http.StatusBadRequest, []byte(`{"error":"illegal_database_name","reason":"`+
		name+` is not a valid database name; use a-z 0-9 _ $ ( ) + - / starting with a letter"}`))
}

func deleteDatabase(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	var out row
	if err := s.client.do(rc.Ctx, http.MethodDelete, dbPath(db), nil, nil, &out); err != nil {
		return nil, err
	}
	return row{"ok": true, "name": db}, nil
}

func compactDatabase(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	var out row
	if err := s.client.do(rc.Ctx, http.MethodPost, dbPath(db)+"/_compact", nil, row{}, &out); err != nil {
		return nil, err
	}
	return row{"ok": true, "name": db, "started": true}, nil
}

func cleanupViews(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	var out row
	if err := s.client.do(rc.Ctx, http.MethodPost, dbPath(db)+"/_view_cleanup", nil, row{}, &out); err != nil {
		return nil, err
	}
	return row{"ok": true, "name": db}, nil
}

func readSecurity(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	var security row
	if err := s.client.do(rc.Ctx, http.MethodGet, dbPath(db)+"/_security", nil, nil, &security); err != nil {
		return nil, err
	}
	if security == nil {
		security = row{}
	}
	// CouchDB answers {} for an unrestricted database; showing the empty member
	// and admin shapes makes the editor immediately useful.
	if security["admins"] == nil {
		security["admins"] = row{"names": []any{}, "roles": []any{}}
	}
	if security["members"] == nil {
		security["members"] = row{"names": []any{}, "roles": []any{}}
	}
	return security, nil
}

func updateSecurity(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	document, err := contentBody(rc)
	if err != nil {
		return nil, err
	}
	var out row
	if err := s.client.do(rc.Ctx, http.MethodPut, dbPath(db)+"/_security", nil, document, &out); err != nil {
		return nil, err
	}
	stored, err := readSecurity(rc)
	if err != nil {
		return nil, err
	}
	content, err := editorContent(stored)
	if err != nil {
		return nil, err
	}
	return row{"ok": true, "content": content}, nil
}
