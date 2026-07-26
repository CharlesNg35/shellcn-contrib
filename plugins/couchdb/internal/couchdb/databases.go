package couchdb

import (
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

// databaseInfos reads every database's info document. CouchDB 2.2+ answers
// /_dbs_info in one round trip; older nodes fall back to one GET per database.
func (s *Session) databaseInfos(rc *plugin.RequestContext) ([]row, error) {
	names := s.databaseNames(rc)
	if len(names) == 0 {
		return []row{}, nil
	}
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
		return out, nil
	}

	out := make([]row, 0, len(names))
	for _, name := range names {
		var info row
		if err := s.client.do(rc.Ctx, http.MethodGet, dbPath(name), nil, nil, &info); err != nil {
			continue
		}
		out = append(out, databaseRow(name, info))
	}
	return out, nil
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

func listDatabases(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	rows, err := s.databaseInfos(rc)
	if err != nil {
		return nil, err
	}
	return broker.PageRows(rc, rows)
}

func treeDatabases(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	names := s.databaseNames(rc)
	nodes := make([]plugin.TreeNode, 0, len(names))
	for _, name := range names {
		ref := plugin.ResourceIdentity{Kind: "database", Name: name, UID: name}
		nodes = append(nodes, plugin.TreeNode{
			Key: "database:" + name, Label: name, Icon: icon("database"), Ref: &ref, Leaf: true,
		})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes}, nil
}

func watchDatabases(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := session(rc)
	if err != nil {
		return err
	}
	return pollWatch(rc, client, 10*time.Second, func() ([]plugin.ResourceEvent, error) {
		rows, err := s.databaseInfos(rc)
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
