package arangodb

import (
	"context"
	"fmt"
	"strings"

	driver "github.com/arangodb/go-driver/v2/arangodb"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

func databasesList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	databases, err := s.client.AccessibleDatabases(ctx)
	if err != nil {
		databases, err = s.client.Databases(ctx)
		if err != nil {
			return nil, arangoErr(err)
		}
	}
	rows := make([]row, 0, len(databases))
	for _, db := range databases {
		item := row{
			"name":     db.Name(),
			"system":   db.Name() == defaultDatabase,
			"current":  db.Name() == s.opts.Database,
			"ref":      plugin.ResourceIdentity{Kind: kindDatabase, Name: db.Name(), UID: db.Name()},
			"sharding": "",
		}
		if info, err := db.Info(ctx); err == nil {
			item["id"] = info.ID
			item["sharding"] = string(info.Sharding)
			item["replication_factor"] = replicationFactorText(driver.ReplicationFactor(info.ReplicationFactor))
			item["write_concern"] = info.WriteConcern
			item["replication_version"] = string(info.ReplicationVersion)
			item["system"] = info.IsSystem
		}
		rows = append(rows, item)
	}
	return pageRows(rc, rows)
}

func databaseRead(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	name := databaseParam(rc, s)
	db, err := s.database(rc.Ctx, name)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	info, err := db.Info(ctx)
	if err != nil {
		return nil, arangoErr(err)
	}
	out := row{
		"name":                info.Name,
		"id":                  info.ID,
		"system":              info.IsSystem,
		"path":                info.Path,
		"sharding":            string(info.Sharding),
		"replication_factor":  replicationFactorText(driver.ReplicationFactor(info.ReplicationFactor)),
		"write_concern":       info.WriteConcern,
		"replication_version": string(info.ReplicationVersion),
		"read_only":           s.opts.ReadOnly,
		"endpoint":            s.opts.Endpoint,
	}
	frame := s.databaseFrame(rc.Ctx, name)
	for key, value := range frame {
		out[key] = value
	}
	if version, err := s.client.Version(ctx); err == nil {
		out["server_version"] = string(version.Version)
		out["license"] = version.License
	}
	return out, nil
}

func databaseCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Database", Fields: []plugin.Field{
		{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true,
			Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: databasePattern, Message: "most characters are allowed; not / : % ? # + or control characters"}}},
		{Key: "sharding", Label: "Sharding", Type: plugin.FieldSelect, Default: "", Options: []plugin.Option{
			{Label: "Server default", Value: ""},
			{Label: "Single (OneShard)", Value: "single"},
			{Label: "Flexible", Value: "flexible"},
		}, Help: "Cluster deployments only."},
		{Key: "replication_factor", Label: "Replication factor", Type: plugin.FieldStepper, Default: 1, Step: 1,
			Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 10}}},
	}}}}
}

func databaseCreate(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Name              string `json:"name" validate:"required"`
		Sharding          string `json:"sharding"`
		ReplicationFactor int    `json:"replication_factor"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name, err := safeDatabaseName(req.Name)
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(name, "_") {
		return nil, fmt.Errorf("%w: system database names are reserved", plugin.ErrForbidden)
	}
	options := &driver.CreateDatabaseOptions{}
	switch req.Sharding {
	case "":
	case "single", "flexible":
		options.Options.Sharding = driver.DatabaseSharding(req.Sharding)
	default:
		return nil, fmt.Errorf("%w: unsupported sharding %q", plugin.ErrInvalidInput, req.Sharding)
	}
	if req.ReplicationFactor > 0 {
		options.Options.ReplicationFactor = driver.ReplicationFactor(req.ReplicationFactor)
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	if _, err := s.client.CreateDatabase(ctx, name, options); err != nil {
		return nil, arangoErr(err)
	}
	return row{"ok": true, "name": name}, nil
}

func databaseDrop(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	name, err := safeDatabaseName(databaseParam(rc, s))
	if err != nil {
		return nil, err
	}
	if name == defaultDatabase {
		return nil, fmt.Errorf("%w: the _system database cannot be dropped", plugin.ErrForbidden)
	}
	db, err := s.database(rc.Ctx, name)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	if err := db.Remove(ctx); err != nil {
		return nil, arangoErr(err)
	}
	return actionResult{OK: true}, nil
}

func treeDatabases(rc *plugin.RequestContext) (any, error) {
	result, err := databasesList(rc)
	if err != nil {
		return nil, err
	}
	page := result.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name := fmt.Sprint(item["name"])
		nodes = append(nodes, plugin.TreeNode{
			Key:   "database:" + name,
			Label: name,
			Icon:  icon("database"),
			ChildrenSource: &plugin.DataSource{
				RouteID: rid("tree.database"),
				Params:  map[string]string{"database": name},
			},
			Data: map[string]any{"database": name},
		})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

// treeDatabase returns the fixed, bounded set of categories under a database.
// Collections, graphs, views, analyzers and Foxx services open as lists, so the
// sidebar never grows with the data.
func treeDatabase(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	database, err := safeDatabaseName(databaseParam(rc, s))
	if err != nil {
		return nil, err
	}
	params := map[string]string{"database": database}
	ref := plugin.ResourceIdentity{Kind: kindDatabase, Name: database, UID: database}
	nodes := []plugin.TreeNode{
		{Key: "database:" + database + ":overview", Label: "Overview", Icon: icon("info"), Ref: &ref, Leaf: true},
		{Key: "database:" + database + ":collections", Label: "Collections", Icon: icon("table"), Leaf: true, ResourceKind: kindCollection, ListParams: params},
		{Key: "database:" + database + ":graphs", Label: "Graphs", Icon: icon("workflow"), Leaf: true, ResourceKind: kindGraph, ListParams: params},
		{Key: "database:" + database + ":views", Label: "Search views", Icon: icon("search"), Leaf: true, ResourceKind: kindView, ListParams: params},
		{Key: "database:" + database + ":analyzers", Label: "Analyzers", Icon: icon("scan-text"), Leaf: true, ResourceKind: kindAnalyzer, ListParams: params},
		{Key: "database:" + database + ":foxx", Label: "Foxx services", Icon: icon("boxes"), Leaf: true, ResourceKind: kindFoxx, ListParams: params},
		{Key: "database:" + database + ":queries", Label: "AQL activity", Icon: icon("activity"), Leaf: true, ResourceKind: kindAQLQuery, ListParams: params},
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes}, nil
}
