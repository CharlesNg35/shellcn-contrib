package cassandra

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"

	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

type row map[string]any

type actionResult struct {
	OK bool `json:"ok"`
}

type confirmationError struct {
	message string
}

func (e confirmationError) Error() string { return e.message }

// dialect builds parameterized single-row CQL (quoted identifiers, ? binds) for
// the editable data grid, reusing the driver-neutral SQL DML builder.
var dialect = sqldb.Dialect{QuoteIdent: quoteIdent, Placeholder: sqldb.QuestionPlaceholder}

func routes() []plugin.Route {
	return []plugin.Route{
		{ID: "cassandra.keyspaces.tree", Method: plugin.MethodGet, Path: "/tree/keyspaces", Permission: "cassandra.keyspaces.read", Risk: plugin.RiskSafe, AuditEvent: "cassandra.keyspaces.tree", Handle: treeKeyspaces},
		{ID: "cassandra.keyspaces.list", Method: plugin.MethodGet, Path: "/keyspaces", Permission: "cassandra.keyspaces.read", Risk: plugin.RiskSafe, AuditEvent: "cassandra.keyspaces.list", Handle: listKeyspaces},
		{ID: "cassandra.keyspace.overview", Method: plugin.MethodGet, Path: "/keyspaces/{keyspace}/overview", Permission: "cassandra.keyspaces.read", Risk: plugin.RiskSafe, AuditEvent: "cassandra.keyspace.overview", Handle: keyspaceOverview},
		{ID: "cassandra.relations.tree", Method: plugin.MethodGet, Path: "/tree/relations", Permission: "cassandra.tables.read", Risk: plugin.RiskSafe, AuditEvent: "cassandra.relations.tree", Handle: treeRelations},
		{ID: "cassandra.tables.list", Method: plugin.MethodGet, Path: "/tables", Permission: "cassandra.tables.read", Risk: plugin.RiskSafe, AuditEvent: "cassandra.tables.list", Handle: listTables},
		{ID: "cassandra.views.list", Method: plugin.MethodGet, Path: "/views", Permission: "cassandra.views.read", Risk: plugin.RiskSafe, AuditEvent: "cassandra.views.list", Handle: listViews},
		{ID: "cassandra.view.drop", Method: plugin.MethodDelete, Path: "/views/{keyspace}/{view}", Permission: "cassandra.tables.write", Risk: plugin.RiskDestructive, AuditEvent: "cassandra.view.drop", Handle: dropView},
		{ID: "cassandra.types.tree", Method: plugin.MethodGet, Path: "/tree/types", Permission: "cassandra.types.read", Risk: plugin.RiskSafe, AuditEvent: "cassandra.types.tree", Handle: treeTypes},
		{ID: "cassandra.types.list", Method: plugin.MethodGet, Path: "/types", Permission: "cassandra.types.read", Risk: plugin.RiskSafe, AuditEvent: "cassandra.types.list", Handle: listTypes},
		{ID: "cassandra.type.overview", Method: plugin.MethodGet, Path: "/types/{keyspace}/{name}/overview", Permission: "cassandra.types.read", Risk: plugin.RiskSafe, AuditEvent: "cassandra.type.overview", Handle: typeOverview},
		{ID: "cassandra.functions.tree", Method: plugin.MethodGet, Path: "/tree/functions", Permission: "cassandra.functions.read", Risk: plugin.RiskSafe, AuditEvent: "cassandra.functions.tree", Handle: treeFunctions},
		{ID: "cassandra.functions.list", Method: plugin.MethodGet, Path: "/functions", Permission: "cassandra.functions.read", Risk: plugin.RiskSafe, AuditEvent: "cassandra.functions.list", Handle: listFunctions},
		{ID: "cassandra.function.overview", Method: plugin.MethodGet, Path: "/functions/{id}/overview", Permission: "cassandra.functions.read", Risk: plugin.RiskSafe, AuditEvent: "cassandra.function.overview", Handle: functionOverview},
		{ID: "cassandra.nodes.tree", Method: plugin.MethodGet, Path: "/tree/nodes", Permission: "cassandra.nodes.read", Risk: plugin.RiskSafe, AuditEvent: "cassandra.nodes.tree", Handle: treeNodes},
		{ID: "cassandra.nodes.list", Method: plugin.MethodGet, Path: "/nodes", Permission: "cassandra.nodes.read", Risk: plugin.RiskSafe, AuditEvent: "cassandra.nodes.list", Handle: listNodes},
		{ID: "cassandra.node.overview", Method: plugin.MethodGet, Path: "/nodes/{address}/overview", Permission: "cassandra.nodes.read", Risk: plugin.RiskSafe, AuditEvent: "cassandra.node.overview", Handle: nodeOverview},
		{ID: "cassandra.table.rows", Method: plugin.MethodGet, Path: "/tables/{keyspace}/{table}/rows", Permission: "cassandra.tables.data.read", Risk: plugin.RiskSafe, AuditEvent: "cassandra.table.rows", Handle: tableRows},
		{ID: "cassandra.table.columns", Method: plugin.MethodGet, Path: "/tables/{keyspace}/{table}/columns", Permission: "cassandra.tables.read", Risk: plugin.RiskSafe, AuditEvent: "cassandra.table.columns", Handle: tableColumnsRoute},
		{ID: "cassandra.table.row.insert", Method: plugin.MethodPost, Path: "/tables/{keyspace}/{table}/rows", Permission: "cassandra.tables.data.write", Risk: plugin.RiskWrite, AuditEvent: "cassandra.table.row.insert", Handle: insertRow},
		{ID: "cassandra.table.row.update", Method: plugin.MethodPatch, Path: "/tables/{keyspace}/{table}/rows", Permission: "cassandra.tables.data.write", Risk: plugin.RiskWrite, AuditEvent: "cassandra.table.row.update", Handle: updateRow},
		{ID: "cassandra.table.row.delete", Method: plugin.MethodDelete, Path: "/tables/{keyspace}/{table}/rows", Permission: "cassandra.tables.data.delete", Risk: plugin.RiskDestructive, AuditEvent: "cassandra.table.row.delete", Handle: deleteRow},
		{ID: "cassandra.table.indexes", Method: plugin.MethodGet, Path: "/tables/{keyspace}/{table}/indexes", Permission: "cassandra.indexes.read", Risk: plugin.RiskSafe, AuditEvent: "cassandra.table.indexes", Handle: tableIndexes},
		{ID: "cassandra.table.definition", Method: plugin.MethodGet, Path: "/tables/{keyspace}/{table}/definition", Permission: "cassandra.tables.read", Risk: plugin.RiskSafe, AuditEvent: "cassandra.table.definition", Handle: tableDefinition},
		{ID: "cassandra.view.definition", Method: plugin.MethodGet, Path: "/views/{keyspace}/{table}/definition", Permission: "cassandra.views.read", Risk: plugin.RiskSafe, AuditEvent: "cassandra.view.definition", Handle: viewDefinition},
		{ID: "cassandra.completion", Method: plugin.MethodGet, Path: "/completion", Permission: "cassandra.keyspaces.read", Risk: plugin.RiskSafe, AuditEvent: "cassandra.completion", Handle: completionRoute},
		{ID: "cassandra.keyspace.create", Method: plugin.MethodPost, Path: "/keyspaces", Permission: "cassandra.keyspaces.write", Risk: plugin.RiskWrite, AuditEvent: "cassandra.keyspace.create", Input: keyspaceCreateSchema(), Handle: createKeyspace},
		{ID: "cassandra.keyspace.drop", Method: plugin.MethodDelete, Path: "/keyspaces/{keyspace}", Permission: "cassandra.keyspaces.delete", Risk: plugin.RiskDestructive, AuditEvent: "cassandra.keyspace.drop", Handle: dropKeyspace},
		{ID: "cassandra.type.create", Method: plugin.MethodPost, Path: "/keyspaces/{keyspace}/types", Permission: "cassandra.types.write", Risk: plugin.RiskWrite, AuditEvent: "cassandra.type.create", Input: typeCreateSchema(), Handle: createType},
		{ID: "cassandra.type.drop", Method: plugin.MethodDelete, Path: "/types/{keyspace}/{name}", Permission: "cassandra.types.delete", Risk: plugin.RiskDestructive, AuditEvent: "cassandra.type.drop", Handle: dropType},
		{ID: "cassandra.table.create", Method: plugin.MethodPost, Path: "/keyspaces/{keyspace}/tables", Permission: "cassandra.tables.write", Risk: plugin.RiskWrite, AuditEvent: "cassandra.table.create", Input: tableCreateSchema(), Handle: createTable},
		{ID: "cassandra.column.add", Method: plugin.MethodPost, Path: "/tables/{keyspace}/{table}/columns", Permission: "cassandra.tables.write", Risk: plugin.RiskWrite, AuditEvent: "cassandra.column.add", Input: columnAddSchema(), Handle: addColumn},
		{ID: "cassandra.column.drop", Method: plugin.MethodPost, Path: "/tables/{keyspace}/{table}/columns/drop", Permission: "cassandra.tables.write", Risk: plugin.RiskDestructive, AuditEvent: "cassandra.column.drop", Handle: dropColumn},
		{ID: "cassandra.index.create", Method: plugin.MethodPost, Path: "/tables/{keyspace}/{table}/indexes", Permission: "cassandra.tables.write", Risk: plugin.RiskWrite, AuditEvent: "cassandra.index.create", Input: indexCreateSchema(), Handle: createIndex},
		{ID: "cassandra.index.drop", Method: plugin.MethodPost, Path: "/tables/{keyspace}/{table}/indexes/drop", Permission: "cassandra.tables.write", Risk: plugin.RiskDestructive, AuditEvent: "cassandra.index.drop", Handle: dropIndex},
		{ID: "cassandra.table.truncate", Method: plugin.MethodPost, Path: "/tables/{keyspace}/{table}/truncate", Permission: "cassandra.tables.delete", Risk: plugin.RiskDestructive, AuditEvent: "cassandra.table.truncate", Handle: truncateTable},
		{ID: "cassandra.table.drop", Method: plugin.MethodDelete, Path: "/tables/{keyspace}/{table}", Permission: "cassandra.tables.delete", Risk: plugin.RiskDestructive, AuditEvent: "cassandra.table.drop", Handle: dropTable},
		{ID: "cassandra.query", Method: plugin.MethodWS, Path: "/query", Permission: "cassandra.query.execute", Risk: plugin.RiskPrivileged, AuditEvent: "cassandra.query", Stream: queryStream},
		{ID: "cassandra.query.cancel", Method: plugin.MethodPost, Path: "/query/cancel", Permission: "cassandra.query.cancel", Risk: plugin.RiskWrite, AuditEvent: "cassandra.query.cancel", Handle: cancelQuery},
	}
}

func cassandraSession(rc *plugin.RequestContext) (*Session, error) {
	return unwrap(rc.Session)
}

func keyspaceCreateSchema() *plugin.Schema {
	simpleStrategy := plugin.Condition{AllOf: []plugin.Rule{{Field: "replication_class", Op: plugin.OpEq, Value: "SimpleStrategy"}}}
	networkTopology := plugin.Condition{AllOf: []plugin.Rule{{Field: "replication_class", Op: plugin.OpEq, Value: "NetworkTopologyStrategy"}}}
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Keyspace", Fields: []plugin.Field{
		{Key: "name", Label: "Keyspace name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		{Key: "replication_class", Label: "Replication class", Type: plugin.FieldSelect, Required: true, Default: "SimpleStrategy", Options: []plugin.Option{{Label: "SimpleStrategy", Value: "SimpleStrategy"}, {Label: "NetworkTopologyStrategy", Value: "NetworkTopologyStrategy"}}},
		{Key: "replication_factor", Label: "Replication factor", Type: plugin.FieldNumber, Default: 1, VisibleWhen: &simpleStrategy, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 20}}},
		{Key: "datacenter_replication", Label: "Datacenter replication", Type: plugin.FieldMap, Required: true, VisibleWhen: &networkTopology, KeyPlaceholder: "datacenter", AddLabel: "Add datacenter", Item: &plugin.Field{Type: plugin.FieldNumber, Default: 1, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 20}}}},
		{Key: "durable_writes", Label: "Durable writes", Type: plugin.FieldToggle, Default: true},
		{Key: "if_not_exists", Label: "If not exists", Type: plugin.FieldToggle, Default: true},
	}}}}
}

func tableCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Table", Fields: []plugin.Field{
		{Key: "name", Label: "Table name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		{Key: "columns", Label: "Columns", Type: plugin.FieldArray, Required: true, MinItems: 1, ItemLabel: "Column", Item: &plugin.Field{Type: plugin.FieldObject, Fields: []plugin.Field{
			{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
			{Key: "type", Label: "Type", Type: plugin.FieldText, Required: true, Placeholder: "text"},
		}}},
		{Key: "primary_key", Label: "Primary key", Type: plugin.FieldText, Required: true, Help: "CQL primary key expression, for example id or (tenant_id, id)."},
		{Key: "if_not_exists", Label: "If not exists", Type: plugin.FieldToggle, Default: true},
	}}}}
}

func columnAddSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Column", Fields: []plugin.Field{
		{Key: "name", Label: "Column name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		{Key: "type", Label: "Type", Type: plugin.FieldText, Required: true, Default: "text"},
	}}}}
}

func indexCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Index", Fields: []plugin.Field{
		{Key: "name", Label: "Index name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		{Key: "column", Label: "Column", Type: plugin.FieldSelect, Required: true, OptionsSource: &plugin.DataSource{RouteID: "cassandra.table.columns", Params: tableParams()}, Help: "Cassandra secondary indexes cover a single column."},
	}}}}
}

func typeCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Type", Fields: []plugin.Field{
		{Key: "name", Label: "Type name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		{Key: "fields", Label: "Fields", Type: plugin.FieldArray, Required: true, MinItems: 1, ItemLabel: "Field", Item: &plugin.Field{Type: plugin.FieldObject, Fields: []plugin.Field{
			{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
			{Key: "type", Label: "Type", Type: plugin.FieldText, Required: true, Placeholder: "text"},
		}}},
		{Key: "if_not_exists", Label: "If not exists", Type: plugin.FieldToggle, Default: true},
	}}}}
}

// treeKeyspaces lists keyspaces as expandable branches that drill into their
// tables/materialized views (hierarchical, TablePlus-style).
func treeKeyspaces(rc *plugin.RequestContext) (any, error) {
	res, err := listKeyspaces(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, r := range page.Items {
		name := fmt.Sprint(r["name"])
		nodes = append(nodes, plugin.TreeNode{
			Key:            "ks:" + name,
			Label:          name,
			Icon:           icon("database"),
			Ref:            &plugin.ResourceRef{Kind: "keyspace", Name: name, UID: name},
			ChildrenSource: &plugin.DataSource{RouteID: "cassandra.relations.tree", Params: map[string]string{"keyspace": name}},
		})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

// treeRelations lists a keyspace's tables and materialized views as leaves
// (scoped by the p.keyspace param the parent node supplies).
func treeRelations(rc *plugin.RequestContext) (any, error) {
	tables, err := listTables(rc)
	if err != nil {
		return nil, err
	}
	views, err := listViews(rc)
	if err != nil {
		return nil, err
	}
	nodes := []plugin.TreeNode{}
	add := func(res any, iconName string) {
		for _, r := range res.(plugin.Page[row]).Items {
			ref, ok := r["ref"].(plugin.ResourceRef)
			if !ok || ref.Kind == "" {
				continue
			}
			nodes = append(nodes, plugin.TreeNode{Key: ref.Kind + ":" + ref.UID, Label: fmt.Sprint(r["name"]), Icon: icon(iconName), Ref: &ref, Leaf: true})
		}
	}
	add(tables, "table-2")
	add(views, "panel-top")
	total := len(nodes)
	return plugin.Page[plugin.TreeNode]{Items: nodes, Total: &total}, nil
}

func treeTypes(rc *plugin.RequestContext) (any, error) {
	return treeFromPage(rc, "type", "braces", "name", listTypes)
}

func treeFunctions(rc *plugin.RequestContext) (any, error) {
	return treeFromPage(rc, "function", "function-square", "name", listFunctions)
}

func treeNodes(rc *plugin.RequestContext) (any, error) {
	return treeFromPage(rc, "node", "server", "address", listNodes)
}

func listKeyspaces(rc *plugin.RequestContext) (any, error) {
	s, err := cassandraSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT keyspace_name, durable_writes, replication
FROM system_schema.keyspaces`, nil)
	if err != nil {
		return nil, err
	}
	tableCounts, _ := countByKeyspace(rc.Ctx, s, "system_schema.tables")
	viewCounts, _ := countByKeyspace(rc.Ctx, s, "system_schema.views")
	out := make([]row, 0, len(rows))
	for _, r := range rows {
		name := fmt.Sprint(r["keyspace_name"])
		if isSystemKeyspace(name) {
			continue
		}
		item := row{
			"name":           name,
			"durable_writes": r["durable_writes"],
			"replication":    compactJSON(r["replication"]),
			"tables":         tableCounts[name],
			"views":          viewCounts[name],
			"ref":            plugin.ResourceRef{Kind: "keyspace", Name: name, UID: name},
		}
		out = append(out, item)
	}
	return pageRows(rc, out)
}

func keyspaceOverview(rc *plugin.RequestContext) (any, error) {
	keyspace, err := safeIdent(rc.Param("keyspace"))
	if err != nil {
		return nil, err
	}
	page, err := listKeyspaces(rc)
	if err != nil {
		return nil, err
	}
	for _, r := range page.(plugin.Page[row]).Items {
		if r["name"] == keyspace {
			return r, nil
		}
	}
	return nil, plugin.ErrNotFound
}

func listTables(rc *plugin.RequestContext) (any, error) {
	s, err := cassandraSession(rc)
	if err != nil {
		return nil, err
	}
	keyspace := strings.TrimSpace(rc.Query().Get("p.keyspace"))
	if keyspace != "" {
		if _, err := safeIdent(keyspace); err != nil {
			return nil, err
		}
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT keyspace_name, table_name, comment, gc_grace_seconds, bloom_filter_fp_chance, caching, compaction, compression
FROM system_schema.tables`, nil)
	if err != nil {
		return nil, err
	}
	out := make([]row, 0, len(rows))
	for _, r := range rows {
		ks, name := fmt.Sprint(r["keyspace_name"]), fmt.Sprint(r["table_name"])
		if isSystemKeyspace(ks) || (keyspace != "" && ks != keyspace) {
			continue
		}
		item := row{
			"name":                   name,
			"keyspace":               ks,
			"comment":                r["comment"],
			"gc_grace_seconds":       r["gc_grace_seconds"],
			"bloom_filter_fp_chance": r["bloom_filter_fp_chance"],
			"caching":                compactJSON(r["caching"]),
			"compaction":             compactJSON(r["compaction"]),
			"compression":            compactJSON(r["compression"]),
			"ref":                    plugin.ResourceRef{Kind: "table", Namespace: ks, Name: name, UID: ks + "." + name},
		}
		out = append(out, item)
	}
	return pageRows(rc, out)
}

func listViews(rc *plugin.RequestContext) (any, error) {
	s, err := cassandraSession(rc)
	if err != nil {
		return nil, err
	}
	keyspace := strings.TrimSpace(rc.Query().Get("p.keyspace"))
	if keyspace != "" {
		if _, err := safeIdent(keyspace); err != nil {
			return nil, err
		}
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT keyspace_name, view_name, base_table_name, where_clause
FROM system_schema.views`, nil)
	if err != nil {
		return nil, err
	}
	out := make([]row, 0, len(rows))
	for _, r := range rows {
		ks, name := fmt.Sprint(r["keyspace_name"]), fmt.Sprint(r["view_name"])
		if isSystemKeyspace(ks) || (keyspace != "" && ks != keyspace) {
			continue
		}
		out = append(out, row{
			"name":         name,
			"keyspace":     ks,
			"base_table":   r["base_table_name"],
			"where_clause": r["where_clause"],
			"ref":          plugin.ResourceRef{Kind: "view", Namespace: ks, Name: name, UID: ks + "." + name},
		})
	}
	return pageRows(rc, out)
}

func listTypes(rc *plugin.RequestContext) (any, error) {
	s, err := cassandraSession(rc)
	if err != nil {
		return nil, err
	}
	keyspace := strings.TrimSpace(rc.Query().Get("p.keyspace"))
	if keyspace != "" {
		if _, err := safeIdent(keyspace); err != nil {
			return nil, err
		}
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT keyspace_name, type_name, field_names, field_types
FROM system_schema.types`, nil)
	if err != nil {
		return nil, err
	}
	out := make([]row, 0, len(rows))
	for _, r := range rows {
		ks, name := fmt.Sprint(r["keyspace_name"]), fmt.Sprint(r["type_name"])
		if isSystemKeyspace(ks) || (keyspace != "" && ks != keyspace) {
			continue
		}
		out = append(out, row{
			"name":     name,
			"keyspace": ks,
			"fields":   typeFields(r["field_names"], r["field_types"]),
			"ref":      plugin.ResourceRef{Kind: "type", Namespace: ks, Name: name, UID: ks + "." + name},
		})
	}
	return pageRows(rc, out)
}

func typeOverview(rc *plugin.RequestContext) (any, error) {
	return scopedOverview(rc, "keyspace", "name", "name", listTypes)
}

func listFunctions(rc *plugin.RequestContext) (any, error) {
	s, err := cassandraSession(rc)
	if err != nil {
		return nil, err
	}
	keyspace := strings.TrimSpace(rc.Query().Get("p.keyspace"))
	if keyspace != "" {
		if _, err := safeIdent(keyspace); err != nil {
			return nil, err
		}
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT keyspace_name, function_name, argument_names, argument_types, return_type, language, body
FROM system_schema.functions`, nil)
	if err != nil {
		return nil, err
	}
	out := make([]row, 0, len(rows))
	for _, r := range rows {
		ks, name := fmt.Sprint(r["keyspace_name"]), fmt.Sprint(r["function_name"])
		if isSystemKeyspace(ks) || (keyspace != "" && ks != keyspace) {
			continue
		}
		args := typeFields(r["argument_names"], r["argument_types"])
		id := functionID(ks, name, args)
		out = append(out, row{
			"name":        name,
			"keyspace":    ks,
			"arguments":   args,
			"return_type": r["return_type"],
			"language":    r["language"],
			"body":        r["body"],
			"ref":         plugin.ResourceRef{Kind: "function", Namespace: ks, Name: name, UID: id},
		})
	}
	return pageRows(rc, out)
}

func functionOverview(rc *plugin.RequestContext) (any, error) {
	id := rc.Param("id")
	page, err := listFunctions(rc)
	if err != nil {
		return nil, err
	}
	for _, r := range page.(plugin.Page[row]).Items {
		ref, _ := r["ref"].(plugin.ResourceRef)
		if ref.UID == id {
			return r, nil
		}
	}
	return nil, plugin.ErrNotFound
}

func listNodes(rc *plugin.RequestContext) (any, error) {
	s, err := cassandraSession(rc)
	if err != nil {
		return nil, err
	}
	local, err := queryRows(rc.Ctx, s, `
SELECT listen_address, rpc_address, data_center, rack, release_version, schema_version
FROM system.local`, nil)
	if err != nil {
		return nil, err
	}
	peers, _ := queryRows(rc.Ctx, s, `
SELECT peer, data_center, rack, release_version, schema_version
FROM system.peers`, nil)
	out := make([]row, 0, len(local)+len(peers))
	for _, r := range local {
		addr := fmt.Sprint(firstNonEmpty(r["rpc_address"], r["listen_address"], "local"))
		out = append(out, nodeRow(addr, r))
	}
	for _, r := range peers {
		addr := fmt.Sprint(firstNonEmpty(r["peer"], "peer"))
		out = append(out, nodeRow(addr, r))
	}
	return pageRows(rc, out)
}

func nodeOverview(rc *plugin.RequestContext) (any, error) {
	address := rc.Param("address")
	page, err := listNodes(rc)
	if err != nil {
		return nil, err
	}
	for _, r := range page.(plugin.Page[row]).Items {
		if fmt.Sprint(r["address"]) == address {
			return r, nil
		}
	}
	return nil, plugin.ErrNotFound
}

func tableRows(rc *plugin.RequestContext) (any, error) {
	keyspace, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	req, err := rc.Page()
	if err != nil {
		return nil, err
	}
	s, err := cassandraSession(rc)
	if err != nil {
		return nil, err
	}
	limit := req.Limit
	if limit > s.opts.RowLimit {
		limit = s.opts.RowLimit
	}
	state, err := decodeCursor(req.Cursor)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.QueryTimeout)
	defer cancel()
	// No free-text data filter here: CQL can't LIKE/scan arbitrary columns without
	// per-column SASI indexes, so the grid's search box is a no-op for CQL data.
	iter := s.db.Query(fmt.Sprintf("SELECT * FROM %s LIMIT %d", qualified(keyspace, table), limit)).
		WithContext(ctx).
		Consistency(s.opts.Consistency).
		PageSize(limit).
		PageState(state).
		Iter()
	rows, err := iterRows(iter, limit, s.opts.RedactPatterns)
	if err != nil {
		return nil, err
	}
	pk, err := primaryKeyColumns(rc.Ctx, s, keyspace, table)
	if err != nil {
		return nil, err
	}
	attachRowKeys(rows, pk, s.opts.RedactPatterns)
	next := encodeCursor(iter.PageState())
	return plugin.Page[row]{Items: rows, NextCursor: next}, nil
}

// primaryKeyColumns returns a table's CQL primary key — partition key columns
// followed by clustering columns, in key order — read from the schema catalog.
// These are the columns a row mutation must match to identify exactly one row.
func primaryKeyColumns(ctx context.Context, s *Session, keyspace, table string) ([]string, error) {
	rows, err := queryRows(ctx, s, `
SELECT column_name, kind, position
FROM system_schema.columns
WHERE keyspace_name = ? AND table_name = ?`, []any{keyspace, table})
	if err != nil {
		return nil, err
	}
	keyRows := make([]row, 0, len(rows))
	for _, r := range rows {
		switch fmt.Sprint(r["kind"]) {
		case "partition_key", "clustering":
			keyRows = append(keyRows, r)
		}
	}
	sort.Slice(keyRows, func(i, j int) bool {
		ki, kj := fmt.Sprint(keyRows[i]["kind"]), fmt.Sprint(keyRows[j]["kind"])
		if ki != kj {
			return columnKindRank(ki) < columnKindRank(kj)
		}
		return intValue(keyRows[i]["position"]) < intValue(keyRows[j]["position"])
	})
	out := make([]string, 0, len(keyRows))
	for _, r := range keyRows {
		out = append(out, fmt.Sprint(r["column_name"]))
	}
	return out, nil
}

// attachRowKeys tags each row with the primary-key/value map the editable grid
// echoes back for UPDATE/DELETE. The grid stays read-only when the table has no
// usable primary key, or when a key column is itself sensitive (so a redacted
// value is never shipped raw inside _key).
func attachRowKeys(rows []row, pk, patterns []string) {
	if len(pk) == 0 || sqldb.AnyColumnRedacted(pk, patterns) {
		return
	}
	for _, r := range rows {
		key := map[string]any{}
		for _, col := range pk {
			key[col] = r[col]
		}
		r["_key"] = key
	}
}

func tableColumnsRoute(rc *plugin.RequestContext) (any, error) {
	keyspace, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	s, err := cassandraSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT column_name, kind, position, type
FROM system_schema.columns
WHERE keyspace_name = ? AND table_name = ?`, []any{keyspace, table})
	if err != nil {
		return nil, err
	}
	sort.Slice(rows, func(i, j int) bool {
		ki, kj := fmt.Sprint(rows[i]["kind"]), fmt.Sprint(rows[j]["kind"])
		if ki != kj {
			return columnKindRank(ki) < columnKindRank(kj)
		}
		return intValue(rows[i]["position"]) < intValue(rows[j]["position"])
	})
	for i := range rows {
		rows[i]["keyspace"] = keyspace
		rows[i]["table"] = table
	}
	return pageRows(rc, rows)
}

func tableIndexes(rc *plugin.RequestContext) (any, error) {
	keyspace, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	s, err := cassandraSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT index_name, kind, options
FROM system_schema.indexes
WHERE keyspace_name = ? AND table_name = ?`, []any{keyspace, table})
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		r["options"] = compactJSON(r["options"])
		r["keyspace"] = keyspace
		r["table"] = table
	}
	return pageRows(rc, rows)
}

func tableDefinition(rc *plugin.RequestContext) (any, error) {
	keyspace, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	cols, err := tableColumnsRoute(rc)
	if err != nil {
		return nil, err
	}
	indexes, err := tableIndexes(rc)
	if err != nil {
		return nil, err
	}
	return row{"keyspace": keyspace, "table": table, "columns": cols, "indexes": indexes}, nil
}

func viewDefinition(rc *plugin.RequestContext) (any, error) {
	keyspace, view, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	page, err := listViews(rc)
	if err != nil {
		return nil, err
	}
	for _, r := range page.(plugin.Page[row]).Items {
		if r["keyspace"] == keyspace && r["name"] == view {
			return r, nil
		}
	}
	return nil, plugin.ErrNotFound
}

func completionRoute(rc *plugin.RequestContext) (any, error) {
	s, err := cassandraSession(rc)
	if err != nil {
		return nil, err
	}
	items := []sqldb.CompletionItem{
		{Label: "SELECT", Type: "keyword"},
		{Label: "FROM", Type: "keyword"},
		{Label: "WHERE", Type: "keyword"},
		{Label: "LIMIT", Type: "keyword"},
		{Label: "INSERT INTO", Type: "keyword"},
		{Label: "UPDATE", Type: "keyword"},
		{Label: "DELETE FROM", Type: "keyword"},
		{Label: "CREATE TABLE", Type: "keyword"},
		{Label: "ALTER TABLE", Type: "keyword"},
		{Label: "TRUNCATE", Type: "keyword"},
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT keyspace_name, table_name, column_name
FROM system_schema.columns`, nil)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	add := func(item sqldb.CompletionItem) {
		key := item.Type + ":" + item.Label + ":" + item.Detail
		if seen[key] {
			return
		}
		seen[key] = true
		items = append(items, item)
	}
	for _, r := range rows {
		keyspace := fmt.Sprint(r["keyspace_name"])
		if isSystemKeyspace(keyspace) {
			continue
		}
		table := fmt.Sprint(r["table_name"])
		column := fmt.Sprint(r["column_name"])
		add(sqldb.CompletionItem{Label: keyspace, Type: "namespace", Detail: "keyspace"})
		add(sqldb.CompletionItem{Label: table, Type: "table", Detail: keyspace, Apply: qualified(keyspace, table)})
		add(sqldb.CompletionItem{Label: column, Type: "property", Detail: keyspace + "." + table})
	}
	return items, nil
}

func createKeyspace(rc *plugin.RequestContext) (any, error) {
	s, err := cassandraSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Name                  string `json:"name" validate:"required"`
		ReplicationClass      string `json:"replication_class" validate:"required"`
		ReplicationFactor     int    `json:"replication_factor"`
		DatacenterReplication any    `json:"datacenter_replication"`
		DurableWrites         bool   `json:"durable_writes"`
		IfNotExists           bool   `json:"if_not_exists"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	keyspace, err := safeIdent(req.Name)
	if err != nil {
		return nil, err
	}
	if req.ReplicationFactor <= 0 {
		req.ReplicationFactor = 1
	}
	replication, err := replicationMap(req.ReplicationClass, req.ReplicationFactor, req.DatacenterReplication)
	if err != nil {
		return nil, err
	}
	prefix := "CREATE KEYSPACE "
	if req.IfNotExists {
		prefix += "IF NOT EXISTS "
	}
	cql := fmt.Sprintf("%s%s WITH replication = %s AND durable_writes = %t", prefix, quoteIdent(keyspace), replication, req.DurableWrites)
	if err := execCQL(rc.Ctx, s, cql); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func dropKeyspace(rc *plugin.RequestContext) (any, error) {
	keyspace, err := safeIdent(rc.Param("keyspace"))
	if err != nil {
		return nil, err
	}
	if isSystemKeyspace(keyspace) {
		return nil, fmt.Errorf("%w: system keyspaces cannot be dropped", plugin.ErrForbidden)
	}
	return execDDL(rc, "DROP KEYSPACE "+quoteIdent(keyspace))
}

func createType(rc *plugin.RequestContext) (any, error) {
	s, err := cassandraSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	keyspace, err := safeIdent(rc.Param("keyspace"))
	if err != nil {
		return nil, err
	}
	var req struct {
		Name        string `json:"name" validate:"required"`
		Fields      any    `json:"fields" validate:"required"`
		IfNotExists bool   `json:"if_not_exists"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name, err := safeIdent(req.Name)
	if err != nil {
		return nil, err
	}
	fields, err := parseColumns(req.Fields)
	if err != nil {
		return nil, err
	}
	prefix := "CREATE TYPE "
	if req.IfNotExists {
		prefix += "IF NOT EXISTS "
	}
	cql := prefix + qualified(keyspace, name) + " (" + strings.Join(fields, ", ") + ")"
	if err := execCQL(rc.Ctx, s, cql); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func dropType(rc *plugin.RequestContext) (any, error) {
	keyspace, err := safeIdent(rc.Param("keyspace"))
	if err != nil {
		return nil, err
	}
	name, err := safeIdent(rc.Param("name"))
	if err != nil {
		return nil, err
	}
	return execDDL(rc, "DROP TYPE "+qualified(keyspace, name))
}

func createTable(rc *plugin.RequestContext) (any, error) {
	s, err := cassandraSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	keyspace, err := safeIdent(rc.Param("keyspace"))
	if err != nil {
		return nil, err
	}
	var req struct {
		Name        string `json:"name" validate:"required"`
		Columns     any    `json:"columns" validate:"required"`
		PrimaryKey  string `json:"primary_key" validate:"required"`
		IfNotExists bool   `json:"if_not_exists"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	table, err := safeIdent(req.Name)
	if err != nil {
		return nil, err
	}
	columns, err := parseColumns(req.Columns)
	if err != nil {
		return nil, err
	}
	primaryKey := strings.TrimSpace(req.PrimaryKey)
	if !safePrimaryKey(primaryKey) {
		return nil, fmt.Errorf("%w: unsafe primary key expression", plugin.ErrInvalidInput)
	}
	prefix := "CREATE TABLE "
	if req.IfNotExists {
		prefix += "IF NOT EXISTS "
	}
	cql := prefix + qualified(keyspace, table) + " (" + strings.Join(append(columns, "PRIMARY KEY ("+primaryKey+")"), ", ") + ")"
	if err := execCQL(rc.Ctx, s, cql); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func addColumn(rc *plugin.RequestContext) (any, error) {
	s, err := cassandraSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	keyspace, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		Name string `json:"name" validate:"required"`
		Type string `json:"type" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	column, err := safeIdent(req.Name)
	if err != nil {
		return nil, err
	}
	if !safeCQLType(req.Type) {
		return nil, fmt.Errorf("%w: unsafe column type", plugin.ErrInvalidInput)
	}
	if err := execCQL(rc.Ctx, s, "ALTER TABLE "+qualified(keyspace, table)+" ADD "+quoteIdent(column)+" "+strings.TrimSpace(req.Type)); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func dropColumn(rc *plugin.RequestContext) (any, error) {
	s, err := cassandraSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	keyspace, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	column, err := safeIdent(rc.Param("name"))
	if err != nil {
		return nil, err
	}
	if err := execCQL(rc.Ctx, s, "ALTER TABLE "+qualified(keyspace, table)+" DROP "+quoteIdent(column)); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func createIndex(rc *plugin.RequestContext) (any, error) {
	s, err := cassandraSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	keyspace, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		Name   string `json:"name" validate:"required"`
		Column string `json:"column" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name, err := safeIdent(req.Name)
	if err != nil {
		return nil, err
	}
	column, err := safeIdent(req.Column)
	if err != nil {
		return nil, err
	}
	if err := execCQL(rc.Ctx, s, "CREATE INDEX "+quoteIdent(name)+" ON "+qualified(keyspace, table)+" ("+quoteIdent(column)+")"); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func dropIndex(rc *plugin.RequestContext) (any, error) {
	s, err := cassandraSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	keyspace, _, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	name, err := safeIdent(rc.Param("name"))
	if err != nil {
		return nil, err
	}
	if err := execCQL(rc.Ctx, s, "DROP INDEX "+qualified(keyspace, name)); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func truncateTable(rc *plugin.RequestContext) (any, error) {
	keyspace, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	return execDDL(rc, "TRUNCATE "+qualified(keyspace, table))
}

func dropTable(rc *plugin.RequestContext) (any, error) {
	keyspace, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	return execDDL(rc, "DROP TABLE "+qualified(keyspace, table))
}

func dropView(rc *plugin.RequestContext) (any, error) {
	keyspace, err := sqldb.SafeIdentifier(rc.Param("keyspace"))
	if err != nil {
		return nil, err
	}
	view, err := sqldb.SafeIdentifier(rc.Param("view"))
	if err != nil {
		return nil, err
	}
	return execDDL(rc, "DROP MATERIALIZED VIEW "+qualified(keyspace, view))
}

func execDDL(rc *plugin.RequestContext, cql string) (any, error) {
	s, err := cassandraSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	if err := execCQL(rc.Ctx, s, cql); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

// --- row mutations ------------------------------------------------------

func insertRow(rc *plugin.RequestContext) (any, error) {
	s, keyspace, table, m, err := mutationContext(rc)
	if err != nil {
		return nil, err
	}
	cql, args, err := dialect.Insert(qualified(keyspace, table), m.Values)
	if err != nil {
		return nil, err
	}
	if err := execCQLArgs(rc.Ctx, s, cql, args); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func updateRow(rc *plugin.RequestContext) (any, error) {
	s, keyspace, table, m, err := mutationContext(rc)
	if err != nil {
		return nil, err
	}
	if err := validateRowKey(rc, s, keyspace, table, m.Key); err != nil {
		return nil, err
	}
	cql, args, err := dialect.Update(qualified(keyspace, table), m.Key, m.Values)
	if err != nil {
		return nil, err
	}
	if err := execCQLArgs(rc.Ctx, s, cql, args); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func deleteRow(rc *plugin.RequestContext) (any, error) {
	s, keyspace, table, m, err := mutationContext(rc)
	if err != nil {
		return nil, err
	}
	if err := validateRowKey(rc, s, keyspace, table, m.Key); err != nil {
		return nil, err
	}
	cql, args, err := dialect.Delete(qualified(keyspace, table), m.Key)
	if err != nil {
		return nil, err
	}
	if err := execCQLArgs(rc.Ctx, s, cql, args); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

// mutationContext resolves the session, target table, and decoded mutation body
// shared by the row insert/update/delete handlers, after the read-only gate.
func mutationContext(rc *plugin.RequestContext) (*Session, string, string, sqldb.RowMutation, error) {
	s, err := cassandraSession(rc)
	if err != nil {
		return nil, "", "", sqldb.RowMutation{}, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, "", "", sqldb.RowMutation{}, err
	}
	keyspace, table, err := tableIdent(rc)
	if err != nil {
		return nil, "", "", sqldb.RowMutation{}, err
	}
	var m sqldb.RowMutation
	if err := rc.Bind(&m); err != nil {
		return nil, "", "", sqldb.RowMutation{}, err
	}
	return s, keyspace, table, m, nil
}

// validateRowKey loads the table's primary key and rejects any client key that
// is not exactly that key, so a mutation can only ever target one partition/row.
func validateRowKey(rc *plugin.RequestContext, s *Session, keyspace, table string, key map[string]any) error {
	pk, err := primaryKeyColumns(rc.Ctx, s, keyspace, table)
	if err != nil {
		return err
	}
	return sqldb.ValidateRowKey(pk, key)
}

func execCQLArgs(ctx context.Context, s *Session, cql string, args []any) error {
	ctx, cancel := context.WithTimeout(ctx, s.opts.QueryTimeout)
	defer cancel()
	if err := s.db.Query(cql, args...).WithContext(ctx).Consistency(s.opts.Consistency).Exec(); err != nil {
		return cassandraErr(err)
	}
	return nil
}

func cancelQuery(rc *plugin.RequestContext) (any, error) {
	s, err := cassandraSession(rc)
	if err != nil {
		return nil, err
	}
	return actionResult{OK: s.cancelAll()}, nil
}

func queryStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := cassandraSession(rc)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(client)
	enc := json.NewEncoder(client)
	for {
		var req sqldb.QueryRequest
		if err := dec.Decode(&req); err != nil {
			if client.Context().Err() != nil || errors.Is(err, io.EOF) {
				return nil
			}
			if err := enc.Encode(map[string]any{"error": "Invalid CQL request."}); err != nil {
				return err
			}
			continue
		}
		statements := sqldb.SplitStatements(req.Query)
		result, err := executeQueryRequest(client.Context(), s, req)
		rc.Audit(queryAuditResult(err), sqldb.AuditParams(sqldb.QueryAudit{
			Query:          req.Query,
			Statements:     statements,
			Confirmed:      req.Confirm,
			ReadOnlyMode:   s.opts.ReadOnly,
			RequiresReview: statementsRequireReview(statements),
			RowCount:       result.RowCount,
			ElapsedMS:      result.ElapsedMS,
			CommandTag:     result.CommandTag,
		}), err)
		if err != nil {
			payload := map[string]any{"error": err.Error()}
			var confirmErr confirmationError
			if errors.As(err, &confirmErr) {
				payload["requiresConfirmation"] = true
				payload["confirmMessage"] = "This CQL statement can change data, schema, privileges, or cluster state. Review it before running."
			}
			if err := enc.Encode(payload); err != nil {
				return err
			}
			continue
		}
		if err := enc.Encode(result); err != nil {
			return err
		}
	}
}

func executeQueryRequest(parent context.Context, s *Session, req sqldb.QueryRequest) (sqldb.QueryResult, error) {
	statements := sqldb.SplitStatements(req.Query)
	if len(statements) == 0 {
		return sqldb.QueryResult{}, fmt.Errorf("%w: query is empty", plugin.ErrInvalidInput)
	}
	if s.opts.ReadOnly {
		for _, st := range statements {
			if !isReadOnlyStatement(st) {
				return sqldb.QueryResult{}, fmt.Errorf("%w: read-only mode blocks write statements", plugin.ErrForbidden)
			}
		}
	}
	if s.opts.RequireConfirm && !req.Confirm {
		for _, st := range statements {
			if isDestructiveStatement(st) {
				return sqldb.QueryResult{}, confirmationError{message: "statement requires confirmation"}
			}
		}
	}
	ctx, cancel := context.WithTimeout(parent, s.opts.QueryTimeout)
	id := req.RequestID
	if id == "" {
		id = uuid.NewString()
	}
	s.track(id, cancel)
	defer func() {
		cancel()
		s.untrack(id)
	}()
	results := make([]sqldb.StatementResult, 0, len(statements))
	for _, st := range statements {
		res, err := executeStatement(ctx, s, st)
		if err != nil {
			return sqldb.QueryResult{}, err
		}
		results = append(results, res)
	}
	out := sqldb.QueryResult{Statements: results}
	if len(results) > 0 {
		last := results[len(results)-1]
		out.Columns = last.Columns
		out.Rows = last.Rows
		out.RowCount = last.RowCount
		out.ElapsedMS = last.ElapsedMS
		out.Statement = last.Statement
		out.CommandTag = last.CommandTag
	}
	return out, nil
}

func executeStatement(ctx context.Context, s *Session, statement string) (sqldb.StatementResult, error) {
	start := time.Now()
	if !statementReturnsRows(statement) {
		if err := s.db.Query(statement).WithContext(ctx).Consistency(s.opts.Consistency).Exec(); err != nil {
			return sqldb.StatementResult{}, cassandraErr(err)
		}
		return sqldb.StatementResult{Statement: statement, RowCount: 0, ElapsedMS: time.Since(start).Milliseconds(), CommandTag: sqldb.FirstKeyword(statement)}, nil
	}
	iter := s.db.Query(statement).WithContext(ctx).Consistency(s.opts.Consistency).PageSize(s.opts.RowLimit).Iter()
	iterColumns := iter.Columns()
	rows, err := iterRows(iter, s.opts.RowLimit, s.opts.RedactPatterns)
	if err != nil {
		return sqldb.StatementResult{}, err
	}
	columns := []string{}
	if len(rows) > 0 {
		for name := range rows[0] {
			columns = append(columns, name)
		}
		sort.Strings(columns)
	} else {
		for _, col := range iterColumns {
			columns = append(columns, col.Name)
		}
	}
	values := make([][]any, 0, len(rows))
	for _, r := range rows {
		rowValues := make([]any, 0, len(columns))
		for _, col := range columns {
			rowValues = append(rowValues, r[col])
		}
		values = append(values, rowValues)
	}
	return sqldb.StatementResult{
		Statement:  statement,
		Columns:    columns,
		Rows:       values,
		RowCount:   int64(len(values)),
		ElapsedMS:  time.Since(start).Milliseconds(),
		CommandTag: sqldb.FirstKeyword(statement),
	}, nil
}

func queryRows(ctx context.Context, s *Session, cql string, args []any) ([]row, error) {
	ctx, cancel := context.WithTimeout(ctx, s.opts.QueryTimeout)
	defer cancel()
	q := s.db.Query(cql, args...).WithContext(ctx).Consistency(s.opts.Consistency).PageSize(s.opts.PageSize)
	iter := q.Iter()
	return iterRows(iter, plugin.MaxPageLimit*10, nil)
}

func execCQL(ctx context.Context, s *Session, cql string) error {
	ctx, cancel := context.WithTimeout(ctx, s.opts.QueryTimeout)
	defer cancel()
	if err := s.db.Query(cql).WithContext(ctx).Consistency(s.opts.Consistency).Exec(); err != nil {
		return cassandraErr(err)
	}
	return nil
}

func iterRows(iter *gocql.Iter, limit int, redactions []string) ([]row, error) {
	out := []row{}
	for len(out) < limit {
		m := map[string]any{}
		if !iter.MapScan(m) {
			break
		}
		r := row{}
		for k, v := range m {
			r[k] = jsonValue(k, v)
		}
		redactRow(r, redactions)
		out = append(out, r)
	}
	if err := iter.Close(); err != nil {
		return nil, cassandraErr(err)
	}
	return out, nil
}

func jsonValue(key string, v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case []byte:
		return sqldb.DisplayBytes(key, x)
	case time.Time:
		return x.Format(time.RFC3339Nano)
	case net.IP:
		return x.String()
	case gocql.UUID:
		return x.String()
	default:
		b, err := json.Marshal(x)
		if err == nil {
			var decoded any
			if json.Unmarshal(b, &decoded) == nil {
				return decoded
			}
		}
		return fmt.Sprint(x)
	}
}

func pageRows(rc *plugin.RequestContext, rows []row) (plugin.Page[row], error) {
	req, err := rc.Page()
	if err != nil {
		return plugin.Page[row]{}, err
	}
	rows = filterRows(rows, req.Search())
	sortRows(rows, req.Sort)
	total := len(rows)
	start, err := offsetCursor(req.Cursor)
	if err != nil {
		return plugin.Page[row]{}, err
	}
	if start > len(rows) {
		start = len(rows)
	}
	end := min(start+req.Limit, len(rows))
	next := ""
	if end < len(rows) {
		next = strconv.Itoa(end)
	}
	return plugin.Page[row]{Items: rows[start:end], NextCursor: next, Total: &total}, nil
}

func treeFromPage(rc *plugin.RequestContext, kind string, iconName string, labelKey string, load func(*plugin.RequestContext) (any, error)) (any, error) {
	res, err := load(rc)
	if err != nil {
		return nil, err
	}
	page, ok := res.(plugin.Page[row])
	if !ok {
		return nil, fmt.Errorf("%w: tree source returned invalid page", plugin.ErrUnavailable)
	}
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, r := range page.Items {
		ref, _ := r["ref"].(plugin.ResourceRef)
		if ref.Kind == "" {
			continue
		}
		label := fmt.Sprint(r[labelKey])
		if keyspace := fmt.Sprint(r["keyspace"]); keyspace != "" && keyspace != "<nil>" && kind != "keyspace" {
			label = keyspace + "." + label
		}
		nodes = append(nodes, plugin.TreeNode{Key: kind + ":" + ref.UID, Label: label, Icon: icon(iconName), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

func scopedOverview(rc *plugin.RequestContext, namespaceParam, nameParam, rowNameKey string, load func(*plugin.RequestContext) (any, error)) (any, error) {
	namespace, err := safeIdent(rc.Param(namespaceParam))
	if err != nil {
		return nil, err
	}
	name, err := safeIdent(rc.Param(nameParam))
	if err != nil {
		return nil, err
	}
	page, err := load(rc)
	if err != nil {
		return nil, err
	}
	for _, r := range page.(plugin.Page[row]).Items {
		if r["keyspace"] == namespace && r[rowNameKey] == name {
			return r, nil
		}
	}
	return nil, plugin.ErrNotFound
}

func countByKeyspace(ctx context.Context, s *Session, table string) (map[string]int, error) {
	rows, err := queryRows(ctx, s, "SELECT keyspace_name FROM "+table, nil)
	if err != nil {
		return nil, err
	}
	counts := map[string]int{}
	for _, r := range rows {
		counts[fmt.Sprint(r["keyspace_name"])]++
	}
	return counts, nil
}

func nodeRow(address string, r row) row {
	item := row{
		"address":         address,
		"data_center":     r["data_center"],
		"rack":            r["rack"],
		"release_version": r["release_version"],
		"schema_version":  r["schema_version"],
	}
	item["ref"] = plugin.ResourceRef{Kind: "node", Name: address, UID: address}
	return item
}

func tableIdent(rc *plugin.RequestContext) (string, string, error) {
	keyspace, err := safeIdent(rc.Param("keyspace"))
	if err != nil {
		return "", "", err
	}
	table, err := safeIdent(rc.Param("table"))
	if err != nil {
		return "", "", err
	}
	return keyspace, table, nil
}

func safeIdent(raw string) (string, error) {
	return sqldb.SafeIdentifier(raw)
}

func qualified(keyspace, table string) string {
	return quoteIdent(keyspace) + "." + quoteIdent(table)
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func replicationMap(class string, replicationFactor int, datacenterReplication any) (string, error) {
	switch class {
	case "SimpleStrategy":
		if replicationFactor <= 0 {
			replicationFactor = 1
		}
		if replicationFactor > 20 {
			return "", fmt.Errorf("%w: replication factor must be at most 20", plugin.ErrInvalidInput)
		}
		return fmt.Sprintf("{'class': 'SimpleStrategy', 'replication_factor': %d}", replicationFactor), nil
	case "NetworkTopologyStrategy":
		raw, err := sqldb.NormalizeJSONValue(datacenterReplication)
		if err != nil {
			return "", err
		}
		var values map[string]any
		if err := json.Unmarshal(raw, &values); err != nil || len(values) == 0 {
			return "", fmt.Errorf("%w: datacenter replication must be a non-empty JSON object", plugin.ErrInvalidInput)
		}
		parts := []string{"'class': 'NetworkTopologyStrategy'"}
		datacenters := make([]string, 0, len(values))
		for dc := range values {
			datacenters = append(datacenters, dc)
		}
		sort.Strings(datacenters)
		for _, dc := range datacenters {
			if !safeReplicationName(dc) {
				return "", fmt.Errorf("%w: unsafe datacenter name", plugin.ErrInvalidInput)
			}
			factor, ok := intAny(values[dc])
			if !ok || factor < 1 || factor > 20 {
				return "", fmt.Errorf("%w: datacenter replication factors must be between 1 and 20", plugin.ErrInvalidInput)
			}
			parts = append(parts, fmt.Sprintf("'%s': %d", dc, factor))
		}
		return "{" + strings.Join(parts, ", ") + "}", nil
	default:
		return "", fmt.Errorf("%w: unsupported replication class", plugin.ErrInvalidInput)
	}
}

func parseColumns(value any) ([]string, error) {
	raw, err := sqldb.NormalizeJSONValue(value)
	if err != nil {
		return nil, err
	}
	var specs []sqldb.ColumnSpec
	if err := json.Unmarshal(raw, &specs); err != nil || len(specs) == 0 {
		return nil, fmt.Errorf("%w: columns must be a non-empty JSON array", plugin.ErrInvalidInput)
	}
	out := make([]string, 0, len(specs))
	for _, spec := range specs {
		name, err := safeIdent(spec.Name)
		if err != nil {
			return nil, err
		}
		if !safeCQLType(spec.Type) {
			return nil, fmt.Errorf("%w: unsafe column type", plugin.ErrInvalidInput)
		}
		out = append(out, quoteIdent(name)+" "+strings.TrimSpace(spec.Type))
	}
	return out, nil
}

func safeCQLType(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" || strings.Contains(s, ";") || strings.Contains(s, "--") || strings.Contains(s, "/*") || strings.Contains(s, "*/") {
		return false
	}
	for _, r := range s {
		if !isAlphaNumeric(r) && !strings.ContainsRune("_<>(), .'\"", r) {
			return false
		}
	}
	return true
}

func safePrimaryKey(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" || strings.Contains(s, ";") || strings.Contains(s, "--") || strings.Contains(s, "/*") || strings.Contains(s, "*/") {
		return false
	}
	for _, r := range s {
		if !isAlphaNumeric(r) && !strings.ContainsRune("_(), \"", r) {
			return false
		}
	}
	return true
}

func isReadOnlyStatement(statement string) bool {
	switch sqldb.FirstKeyword(statement) {
	case "SELECT", "WITH", "DESC", "DESCRIBE", "SHOW":
		return true
	default:
		return false
	}
}

func isDestructiveStatement(statement string) bool {
	switch sqldb.FirstKeyword(statement) {
	case "INSERT", "UPDATE", "DELETE", "BEGIN", "APPLY", "BATCH", "CREATE", "ALTER", "DROP", "TRUNCATE", "GRANT", "REVOKE":
		return true
	default:
		return false
	}
}

func statementReturnsRows(statement string) bool {
	switch sqldb.FirstKeyword(statement) {
	case "SELECT":
		return true
	default:
		return false
	}
}

func statementsRequireReview(statements []string) bool {
	for _, st := range statements {
		if isDestructiveStatement(st) {
			return true
		}
	}
	return false
}

func ensureWritable(s *Session) error {
	if s.opts.ReadOnly {
		return fmt.Errorf("%w: read-only mode blocks write actions", plugin.ErrForbidden)
	}
	return nil
}

func queryAuditResult(err error) plugin.AuditResult {
	if err == nil {
		return plugin.AuditAllowed
	}
	var confirmErr confirmationError
	if errors.As(err, &confirmErr) {
		return plugin.AuditDenied
	}
	return plugin.AuditError
}

func cassandraErr(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
}

func filterRows(rows []row, query string) []row {
	return plugin.FilterRows(rows, query)
}

func sortRows(rows []row, sorts []plugin.SortKey) {
	if len(sorts) == 0 {
		return
	}
	sortSpec := sorts[0]
	sort.SliceStable(rows, func(i, j int) bool {
		less := fmt.Sprint(rows[i][sortSpec.Field]) < fmt.Sprint(rows[j][sortSpec.Field])
		if sortSpec.Desc {
			return !less
		}
		return less
	})
}

func offsetCursor(raw string) (int, error) {
	if strings.TrimSpace(raw) == "" {
		return 0, nil
	}
	offset, err := strconv.Atoi(raw)
	if err != nil || offset < 0 {
		return 0, fmt.Errorf("%w: invalid cursor", plugin.ErrInvalidInput)
	}
	return offset, nil
}

func encodeCursor(state []byte) string {
	if len(state) == 0 {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(state)
}

func decodeCursor(raw string) ([]byte, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	state, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid cursor", plugin.ErrInvalidInput)
	}
	return state, nil
}

func redactRow(r row, patterns []string) {
	for key, value := range r {
		if value != nil && sqldb.RedactColumn(key, patterns) {
			r[key] = sqldb.RedactedValue
		}
	}
}

func compactJSON(value any) string {
	if value == nil {
		return ""
	}
	b, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprint(value)
	}
	return string(b)
}

func typeFields(names, types any) string {
	nameList := stringSlice(names)
	typeList := stringSlice(types)
	parts := make([]string, 0, max(len(nameList), len(typeList)))
	for i := 0; i < max(len(nameList), len(typeList)); i++ {
		name, typ := "", ""
		if i < len(nameList) {
			name = nameList[i]
		}
		if i < len(typeList) {
			typ = typeList[i]
		}
		if name == "" {
			parts = append(parts, typ)
		} else {
			parts = append(parts, name+" "+typ)
		}
	}
	return strings.Join(parts, ", ")
}

func stringSlice(value any) []string {
	switch v := value.(type) {
	case []string:
		return v
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			out = append(out, fmt.Sprint(item))
		}
		return out
	default:
		raw := strings.Trim(fmt.Sprint(value), "[]")
		if raw == "" || raw == "<nil>" {
			return nil
		}
		return strings.Fields(raw)
	}
}

func functionID(keyspace, name, args string) string {
	return keyspace + "." + name + "(" + args + ")"
}

func firstNonEmpty(values ...any) any {
	for _, value := range values {
		if strings.TrimSpace(fmt.Sprint(value)) != "" && fmt.Sprint(value) != "<nil>" {
			return value
		}
	}
	return ""
}

func isSystemKeyspace(name string) bool {
	return strings.HasPrefix(name, "system")
}

func columnKindRank(kind string) int {
	switch kind {
	case "partition_key":
		return 0
	case "clustering":
		return 1
	case "regular":
		return 2
	case "static":
		return 3
	default:
		return 4
	}
}

func intValue(v any) int {
	switch x := v.(type) {
	case int:
		return x
	case int64:
		return int(x)
	case float64:
		return int(x)
	default:
		i, _ := strconv.Atoi(fmt.Sprint(x))
		return i
	}
}

func intAny(v any) (int, bool) {
	switch x := v.(type) {
	case int:
		return x, true
	case int64:
		return int(x), true
	case float64:
		if x != float64(int(x)) {
			return 0, false
		}
		return int(x), true
	case json.Number:
		i, err := x.Int64()
		return int(i), err == nil
	default:
		i, err := strconv.Atoi(fmt.Sprint(x))
		return i, err == nil
	}
}

func safeReplicationName(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	for _, r := range s {
		if !isAlphaNumeric(r) && !strings.ContainsRune("_.-", r) {
			return false
		}
	}
	return true
}

func isAlphaNumeric(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}
