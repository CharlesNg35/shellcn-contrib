package scylladb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
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

type row = plugin.TableRow

// tokenColumn is the helper column the data grid carries so a row can be handed
// straight to the partition locator.
const tokenColumn = "_token"

// scanCap bounds every introspection read materialized in memory.
const scanCap = plugin.MaxPageLimit * 20

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
	return append(clusterRoutes(), []plugin.Route{
		{ID: "scylladb.keyspaces.tree", Method: plugin.MethodGet, Path: "/tree/keyspaces", Permission: "scylladb.keyspaces.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.keyspaces.tree", Handle: treeKeyspaces},
		{ID: "scylladb.relations.tree", Method: plugin.MethodGet, Path: "/tree/relations", Permission: "scylladb.tables.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.relations.tree", Handle: treeRelations},
		{ID: "scylladb.types.tree", Method: plugin.MethodGet, Path: "/tree/types", Permission: "scylladb.types.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.types.tree", Handle: treeTypes},
		{ID: "scylladb.functions.tree", Method: plugin.MethodGet, Path: "/tree/functions", Permission: "scylladb.functions.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.functions.tree", Handle: treeFunctions},

		{ID: "scylladb.keyspaces.list", Method: plugin.MethodGet, Path: "/keyspaces", Permission: "scylladb.keyspaces.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.keyspaces.list", Handle: listKeyspaces},
		{ID: "scylladb.keyspace.overview", Method: plugin.MethodGet, Path: "/keyspaces/{keyspace}/overview", Permission: "scylladb.keyspaces.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.keyspace.overview", Handle: keyspaceOverview},
		{ID: "scylladb.keyspace.create", Method: plugin.MethodPost, Path: "/keyspaces", Permission: "scylladb.keyspaces.write", Risk: plugin.RiskWrite, AuditEvent: "scylladb.keyspace.create", Input: keyspaceCreateSchema(), Handle: createKeyspace},
		{ID: "scylladb.keyspace.drop", Method: plugin.MethodDelete, Path: "/keyspaces/{keyspace}", Permission: "scylladb.keyspaces.delete", Risk: plugin.RiskDestructive, AuditEvent: "scylladb.keyspace.drop", Handle: dropKeyspace},

		{ID: "scylladb.tables.list", Method: plugin.MethodGet, Path: "/tables", Permission: "scylladb.tables.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.tables.list", Handle: listTables},
		{ID: "scylladb.table.create", Method: plugin.MethodPost, Path: "/keyspaces/{keyspace}/tables", Permission: "scylladb.tables.write", Risk: plugin.RiskWrite, AuditEvent: "scylladb.table.create", Input: tableCreateSchema(), Handle: createTable},
		{ID: "scylladb.table.truncate", Method: plugin.MethodPost, Path: "/tables/{keyspace}/{table}/truncate", Permission: "scylladb.tables.delete", Risk: plugin.RiskDestructive, AuditEvent: "scylladb.table.truncate", Handle: truncateTable},
		{ID: "scylladb.table.drop", Method: plugin.MethodDelete, Path: "/tables/{keyspace}/{table}", Permission: "scylladb.tables.delete", Risk: plugin.RiskDestructive, AuditEvent: "scylladb.table.drop", Handle: dropTable},
		{ID: "scylladb.table.rows", Method: plugin.MethodGet, Path: "/tables/{keyspace}/{table}/rows", Permission: "scylladb.tables.data.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.table.rows", Handle: tableRows},
		{ID: "scylladb.table.columns", Method: plugin.MethodGet, Path: "/tables/{keyspace}/{table}/columns", Permission: "scylladb.tables.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.table.columns", Handle: tableColumnsRoute},
		{ID: "scylladb.table.row.insert", Method: plugin.MethodPost, Path: "/tables/{keyspace}/{table}/rows", Permission: "scylladb.tables.data.write", Risk: plugin.RiskWrite, AuditEvent: "scylladb.table.row.insert", Handle: insertRow},
		{ID: "scylladb.table.row.update", Method: plugin.MethodPatch, Path: "/tables/{keyspace}/{table}/rows", Permission: "scylladb.tables.data.write", Risk: plugin.RiskWrite, AuditEvent: "scylladb.table.row.update", Handle: updateRow},
		{ID: "scylladb.table.row.delete", Method: plugin.MethodDelete, Path: "/tables/{keyspace}/{table}/rows", Permission: "scylladb.tables.data.delete", Risk: plugin.RiskDestructive, AuditEvent: "scylladb.table.row.delete", Handle: deleteRow},
		{ID: "scylladb.table.indexes", Method: plugin.MethodGet, Path: "/tables/{keyspace}/{table}/indexes", Permission: "scylladb.indexes.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.table.indexes", Handle: tableIndexes},
		{ID: "scylladb.table.definition", Method: plugin.MethodGet, Path: "/tables/{keyspace}/{table}/definition", Permission: "scylladb.tables.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.table.definition", Handle: tableDefinition},
		{ID: "scylladb.column.add", Method: plugin.MethodPost, Path: "/tables/{keyspace}/{table}/columns", Permission: "scylladb.tables.write", Risk: plugin.RiskWrite, AuditEvent: "scylladb.column.add", Input: columnAddSchema(), Handle: addColumn},
		{ID: "scylladb.column.drop", Method: plugin.MethodPost, Path: "/tables/{keyspace}/{table}/columns/drop", Permission: "scylladb.tables.write", Risk: plugin.RiskDestructive, AuditEvent: "scylladb.column.drop", Handle: dropColumn},
		{ID: "scylladb.index.create", Method: plugin.MethodPost, Path: "/tables/{keyspace}/{table}/indexes", Permission: "scylladb.tables.write", Risk: plugin.RiskWrite, AuditEvent: "scylladb.index.create", Input: indexCreateSchema(), Handle: createIndex},
		{ID: "scylladb.index.drop", Method: plugin.MethodPost, Path: "/keyspaces/{keyspace}/indexes/drop", Permission: "scylladb.tables.write", Risk: plugin.RiskDestructive, AuditEvent: "scylladb.index.drop", Handle: dropIndex},

		{ID: "scylladb.views.list", Method: plugin.MethodGet, Path: "/views", Permission: "scylladb.views.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.views.list", Handle: listViews},
		{ID: "scylladb.view.definition", Method: plugin.MethodGet, Path: "/views/{keyspace}/{table}/definition", Permission: "scylladb.views.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.view.definition", Handle: viewDefinition},
		{ID: "scylladb.view.drop", Method: plugin.MethodDelete, Path: "/views/{keyspace}/{view}", Permission: "scylladb.tables.delete", Risk: plugin.RiskDestructive, AuditEvent: "scylladb.view.drop", Handle: dropView},

		{ID: "scylladb.types.list", Method: plugin.MethodGet, Path: "/types", Permission: "scylladb.types.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.types.list", Handle: listTypes},
		{ID: "scylladb.type.overview", Method: plugin.MethodGet, Path: "/types/{keyspace}/{name}/overview", Permission: "scylladb.types.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.type.overview", Handle: typeOverview},
		{ID: "scylladb.type.create", Method: plugin.MethodPost, Path: "/keyspaces/{keyspace}/types", Permission: "scylladb.types.write", Risk: plugin.RiskWrite, AuditEvent: "scylladb.type.create", Input: typeCreateSchema(), Handle: createType},
		{ID: "scylladb.type.drop", Method: plugin.MethodDelete, Path: "/types/{keyspace}/{name}", Permission: "scylladb.types.delete", Risk: plugin.RiskDestructive, AuditEvent: "scylladb.type.drop", Handle: dropType},

		{ID: "scylladb.functions.list", Method: plugin.MethodGet, Path: "/functions", Permission: "scylladb.functions.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.functions.list", Handle: listFunctions},
		{ID: "scylladb.function.overview", Method: plugin.MethodGet, Path: "/functions/{id}/overview", Permission: "scylladb.functions.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.function.overview", Handle: functionOverview},

		{ID: "scylladb.completion", Method: plugin.MethodGet, Path: "/completion", Permission: "scylladb.keyspaces.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.completion", Handle: completionRoute},
		{ID: "scylladb.query", Method: plugin.MethodWS, Path: "/query", Permission: "scylladb.query.execute", Risk: plugin.RiskPrivileged, AuditEvent: "scylladb.query", Stream: queryStream},
		{ID: "scylladb.query.cancel", Method: plugin.MethodPost, Path: "/query/cancel", Permission: "scylladb.query.cancel", Risk: plugin.RiskWrite, AuditEvent: "scylladb.query.cancel", Handle: cancelQuery},

		{ID: "scylladb.queries.list", Method: plugin.MethodGet, Path: "/saved-queries", Permission: "scylladb.queries.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.queries.list", Handle: listSavedQueries},
		{ID: "scylladb.query.save", Method: plugin.MethodPost, Path: "/saved-queries", Permission: "scylladb.queries.write", Risk: plugin.RiskWrite, AuditEvent: "scylladb.query.save", Input: savedQuerySchema(), Handle: saveQuery},
		{ID: "scylladb.query.delete", Method: plugin.MethodDelete, Path: "/saved-queries/{key}", Permission: "scylladb.queries.delete", Risk: plugin.RiskDestructive, AuditEvent: "scylladb.query.delete", Handle: deleteSavedQuery},
	}...)
}

func scyllaSession(rc *plugin.RequestContext) (*Session, error) {
	return unwrap(rc.Session)
}

// keyspaceCreateSchema defaults to NetworkTopologyStrategy because ScyllaDB
// rejects SimpleStrategy for tablet-replicated keyspaces.
func keyspaceCreateSchema() *plugin.Schema {
	simpleStrategy := plugin.Condition{AllOf: []plugin.Rule{{Field: "replication_class", Op: plugin.OpEq, Value: "SimpleStrategy"}}}
	networkTopology := plugin.Condition{AllOf: []plugin.Rule{{Field: "replication_class", Op: plugin.OpEq, Value: "NetworkTopologyStrategy"}}}
	tabletsEnabled := plugin.Condition{AllOf: []plugin.Rule{{Field: "tablets", Op: plugin.OpEq, Value: "enabled"}}}
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Keyspace", Fields: []plugin.Field{
		{Key: "name", Label: "Keyspace name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		{Key: "replication_class", Label: "Replication class", Type: plugin.FieldSelect, Required: true, Default: "NetworkTopologyStrategy", Options: []plugin.Option{
			{Label: "NetworkTopologyStrategy", Value: "NetworkTopologyStrategy"},
			{Label: "SimpleStrategy", Value: "SimpleStrategy"},
		}, Help: "ScyllaDB rejects SimpleStrategy for tablet-replicated keyspaces; prefer NetworkTopologyStrategy."},
		{Key: "replication_factor", Label: "Replication factor", Type: plugin.FieldNumber, Default: 1, VisibleWhen: &simpleStrategy, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 20}}},
		{Key: "datacenter_replication", Label: "Datacenter replication", Type: plugin.FieldMap, Required: true, VisibleWhen: &networkTopology, KeyPlaceholder: "datacenter", AddLabel: "Add datacenter", Item: &plugin.Field{Type: plugin.FieldNumber, Default: 1, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 20}}}},
		{Key: "durable_writes", Label: "Durable writes", Type: plugin.FieldToggle, Default: true},
		{Key: "tablets", Label: "Tablets", Type: plugin.FieldSelect, Default: "default", Options: []plugin.Option{
			{Label: "Cluster default", Value: "default"},
			{Label: "Enabled", Value: "enabled"},
			{Label: "Disabled (vnodes)", Value: "disabled"},
		}, Help: "Tablets let ScyllaDB rebalance data per table instead of per node."},
		{Key: "initial_tablets", Label: "Initial tablets", Type: plugin.FieldNumber, VisibleWhen: &tabletsEnabled, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 4096}}, Help: "Leave empty to let ScyllaDB pick the initial tablet count."},
		{Key: "if_not_exists", Label: "If not exists", Type: plugin.FieldToggle, Default: true},
	}}}}
}

func tableCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Table", Fields: []plugin.Field{
		{Key: "name", Label: "Table name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		{Key: "columns", Label: "Columns", Type: plugin.FieldArray, Required: true, MinItems: 1, ItemLabel: "Column", Item: &plugin.Field{Type: plugin.FieldObject, Fields: []plugin.Field{
			{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
			{Key: "type", Label: "Type", Type: plugin.FieldAutocomplete, Required: true, Placeholder: "text", Options: cqlTypeOptions()},
		}}},
		{Key: "primary_key", Label: "Primary key", Type: plugin.FieldText, Required: true, Help: "CQL primary key expression, for example id or ((tenant_id, bucket), created_at)."},
		{Key: "compaction_strategy", Label: "Compaction strategy", Type: plugin.FieldSelect, Default: "", Options: []plugin.Option{
			{Label: "Table default", Value: ""},
			{Label: "Incremental (ICS)", Value: "IncrementalCompactionStrategy"},
			{Label: "Size tiered (STCS)", Value: "SizeTieredCompactionStrategy"},
			{Label: "Leveled (LCS)", Value: "LeveledCompactionStrategy"},
			{Label: "Time window (TWCS)", Value: "TimeWindowCompactionStrategy"},
		}},
		{Key: "default_time_to_live", Label: "Default TTL", Type: plugin.FieldNumber, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 0}, {Type: plugin.ValidatorMax, Value: 630720000}}, Help: "Seconds. 0 keeps rows forever."},
		{Key: "if_not_exists", Label: "If not exists", Type: plugin.FieldToggle, Default: true},
	}}}}
}

func columnAddSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Column", Fields: []plugin.Field{
		{Key: "name", Label: "Column name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		{Key: "type", Label: "Type", Type: plugin.FieldAutocomplete, Required: true, Default: "text", Options: cqlTypeOptions()},
	}}}}
}

func indexCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Index", Fields: []plugin.Field{
		{Key: "name", Label: "Index name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		{Key: "column", Label: "Column", Type: plugin.FieldSelect, Required: true, OptionsSource: &plugin.DataSource{RouteID: "scylladb.table.columns", Params: tableParams()}, Help: "A ScyllaDB secondary index covers a single column."},
		{Key: "local", Label: "Local index", Type: plugin.FieldToggle, Help: "Restricts the index to each partition, avoiding a global index materialized view."},
	}}}}
}

func typeCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Type", Fields: []plugin.Field{
		{Key: "name", Label: "Type name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		{Key: "fields", Label: "Fields", Type: plugin.FieldArray, Required: true, MinItems: 1, ItemLabel: "Field", Item: &plugin.Field{Type: plugin.FieldObject, Fields: []plugin.Field{
			{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
			{Key: "type", Label: "Type", Type: plugin.FieldAutocomplete, Required: true, Placeholder: "text", Options: cqlTypeOptions()},
		}}},
		{Key: "if_not_exists", Label: "If not exists", Type: plugin.FieldToggle, Default: true},
	}}}}
}

func cqlTypeOptions() []plugin.Option {
	types := []string{
		"ascii", "bigint", "blob", "boolean", "counter", "date", "decimal", "double",
		"duration", "float", "inet", "int", "smallint", "text", "time", "timestamp",
		"timeuuid", "tinyint", "uuid", "varchar", "varint",
		"list<text>", "set<text>", "map<text, text>", "frozen<list<text>>",
	}
	out := make([]plugin.Option, 0, len(types))
	for _, t := range types {
		out = append(out, plugin.Option{Label: t, Value: t})
	}
	return out
}

// treeKeyspaces lists keyspaces as expandable branches that drill into their
// tables and materialized views.
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
			Ref:            &plugin.ResourceIdentity{Kind: "keyspace", Name: name, UID: name},
			ChildrenSource: &plugin.DataSource{RouteID: "scylladb.relations.tree", Params: map[string]string{"keyspace": name}},
			Data:           map[string]any{"tables": r["tables"], "views": r["views"]},
		})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

// treeRelations lists a keyspace's tables and materialized views as leaves,
// scoped by the p.keyspace param the parent node supplies.
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
			ref, ok := r["ref"].(plugin.ResourceIdentity)
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

func listKeyspaces(rc *plugin.RequestContext) (any, error) {
	rows, err := keyspaceItems(rc)
	if err != nil {
		return nil, err
	}
	return pageRows(rc, rows)
}

// keyspaceItems builds the unpaged keyspace list. Detail routes read it directly
// so a lookup still finds its record when the caller's page ends before it.
func keyspaceItems(rc *plugin.RequestContext) ([]row, error) {
	s, err := scyllaSession(rc)
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
	typeCounts, _ := countByKeyspace(rc.Ctx, s, "system_schema.types")
	out := make([]row, 0, len(rows))
	for _, r := range rows {
		name := fmt.Sprint(r["keyspace_name"])
		if isSystemKeyspace(name) {
			continue
		}
		totals := keyspaceEstimateTotals(rc.Ctx, s, name)
		out = append(out, row{
			"name":            name,
			"durable_writes":  r["durable_writes"],
			"replication":     compactJSON(r["replication"]),
			"tables":          tableCounts[name],
			"views":           viewCounts[name],
			"types":           typeCounts[name],
			"partitions":      totals.Partitions,
			"estimated_bytes": totals.Bytes,
			"ref":             plugin.ResourceIdentity{Kind: "keyspace", Name: name, UID: name},
		})
	}
	return out, nil
}

func keyspaceOverview(rc *plugin.RequestContext) (any, error) {
	keyspace, err := safeIdent(rc.Param("keyspace"))
	if err != nil {
		return nil, err
	}
	s, err := scyllaSession(rc)
	if err != nil {
		return nil, err
	}
	items, err := keyspaceItems(rc)
	if err != nil {
		return nil, err
	}
	for _, r := range items {
		if r["name"] != keyspace {
			continue
		}
		ranges, err := queryRows(rc.Ctx, s, `SELECT start_token FROM system.token_ring WHERE keyspace_name = ?`, []any{keyspace})
		if err == nil {
			r["token_ranges"] = len(ranges)
		}
		return r, nil
	}
	return nil, plugin.ErrNotFound
}

func listTables(rc *plugin.RequestContext) (any, error) {
	s, err := scyllaSession(rc)
	if err != nil {
		return nil, err
	}
	keyspace, err := scopedKeyspace(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT keyspace_name, table_name, comment, gc_grace_seconds, bloom_filter_fp_chance, caching, compaction, compression, default_time_to_live, speculative_retry
FROM system_schema.tables`, nil)
	if err != nil {
		return nil, err
	}
	estimates := map[string]estimateTotals{}
	out := make([]row, 0, len(rows))
	for _, r := range rows {
		ks, name := fmt.Sprint(r["keyspace_name"]), fmt.Sprint(r["table_name"])
		if isSystemKeyspace(ks) || (keyspace != "" && ks != keyspace) {
			continue
		}
		if _, ok := estimates[ks]; !ok {
			estimates[ks] = keyspaceEstimateTotals(rc.Ctx, s, ks)
		}
		totals := estimates[ks].Tables[name]
		out = append(out, row{
			"name":                   name,
			"keyspace":               ks,
			"comment":                r["comment"],
			"gc_grace_seconds":       r["gc_grace_seconds"],
			"bloom_filter_fp_chance": r["bloom_filter_fp_chance"],
			"default_time_to_live":   r["default_time_to_live"],
			"speculative_retry":      r["speculative_retry"],
			"caching":                compactJSON(r["caching"]),
			"compaction":             compactJSON(r["compaction"]),
			"compaction_strategy":    compactionStrategy(r["compaction"]),
			"compression":            compactJSON(r["compression"]),
			"partitions":             totals.Partitions,
			"estimated_bytes":        totals.Bytes,
			"ref":                    plugin.ResourceIdentity{Kind: "table", Namespace: ks, Name: name, UID: ks + "." + name},
		})
	}
	return pageRows(rc, out)
}

func listViews(rc *plugin.RequestContext) (any, error) {
	rows, err := viewItems(rc)
	if err != nil {
		return nil, err
	}
	return pageRows(rc, rows)
}

func viewItems(rc *plugin.RequestContext) ([]row, error) {
	s, err := scyllaSession(rc)
	if err != nil {
		return nil, err
	}
	keyspace, err := scopedKeyspace(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT keyspace_name, view_name, base_table_name, where_clause, include_all_columns
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
			"name":                name,
			"keyspace":            ks,
			"base_table":          r["base_table_name"],
			"where_clause":        r["where_clause"],
			"include_all_columns": r["include_all_columns"],
			"ref":                 plugin.ResourceIdentity{Kind: "view", Namespace: ks, Name: name, UID: ks + "." + name},
		})
	}
	return out, nil
}

func listTypes(rc *plugin.RequestContext) (any, error) {
	rows, err := typeItems(rc)
	if err != nil {
		return nil, err
	}
	return pageRows(rc, rows)
}

func typeItems(rc *plugin.RequestContext) ([]row, error) {
	s, err := scyllaSession(rc)
	if err != nil {
		return nil, err
	}
	keyspace, err := scopedKeyspace(rc)
	if err != nil {
		return nil, err
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
			"ref":      plugin.ResourceIdentity{Kind: "type", Namespace: ks, Name: name, UID: ks + "." + name},
		})
	}
	return out, nil
}

func typeOverview(rc *plugin.RequestContext) (any, error) {
	return scopedOverview(rc, "keyspace", "name", "name", typeItems)
}

func listFunctions(rc *plugin.RequestContext) (any, error) {
	rows, err := functionItems(rc)
	if err != nil {
		return nil, err
	}
	return pageRows(rc, rows)
}

func functionItems(rc *plugin.RequestContext) ([]row, error) {
	s, err := scyllaSession(rc)
	if err != nil {
		return nil, err
	}
	keyspace, err := scopedKeyspace(rc)
	if err != nil {
		return nil, err
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
		out = append(out, row{
			"name":        name,
			"keyspace":    ks,
			"arguments":   args,
			"return_type": r["return_type"],
			"language":    r["language"],
			"body":        r["body"],
			"ref":         plugin.ResourceIdentity{Kind: "function", Namespace: ks, Name: name, UID: ks + "." + name + "(" + args + ")"},
		})
	}
	return out, nil
}

func functionOverview(rc *plugin.RequestContext) (any, error) {
	id := rc.Param("id")
	items, err := functionItems(rc)
	if err != nil {
		return nil, err
	}
	for _, r := range items {
		ref, _ := r["ref"].(plugin.ResourceIdentity)
		if ref.UID == id {
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
	s, err := scyllaSession(rc)
	if err != nil {
		return nil, err
	}
	limit := min(req.Limit, s.opts.RowLimit)
	state, err := decodeCursor(req.Cursor)
	if err != nil {
		return nil, err
	}
	all, partitionKey, primaryKey, err := tableColumnNames(rc.Ctx, s, keyspace, table)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.QueryTimeout)
	defer cancel()
	// Paging is driver page state: a CQL LIMIT would exhaust the query on the
	// first page and the coordinator would stop handing back a paging state.
	stmt := fmt.Sprintf("SELECT %s FROM %s", rowProjection(all, partitionKey), qualified(keyspace, table))
	fetch := func(state []byte) ([]row, []byte, error) {
		iter := s.db.Query(stmt).
			WithContext(ctx).
			Consistency(s.opts.Consistency).
			PageSize(limit).
			PageState(state).
			Iter()
		page, err := iterRows(iter, limit, s.opts.RedactPatterns)
		if err != nil {
			return nil, nil, err
		}
		return page, iter.PageState(), nil
	}
	rows, next, err := scanRows(fetch, state, req.Search(), limit, s.opts.RowLimit)
	if err != nil {
		return nil, err
	}
	// A Murmur3 token needs all 64 bits, which a browser number cannot hold, so
	// the grid carries it as text.
	for _, r := range rows {
		if token, ok := r[tokenColumn]; ok {
			r[tokenColumn] = strconv.FormatInt(int64Value(token), 10)
		}
	}
	attachRowKeys(rows, primaryKey, s.opts.RedactPatterns)
	return plugin.Page[row]{Items: rows, NextCursor: encodeCursor(next)}, nil
}

// scanRows reads one driver page, or - when the grid carries a free-text term -
// walks pages forward and keeps the rows that match it. CQL cannot LIKE/scan
// arbitrary columns without per-column indexes, so the term is applied to the
// rows the scan pulls: it stops on the page boundary past limit matches, at the
// end of the table, or once budget rows have been read, and the cursor it
// returns resumes the scan on the next request.
func scanRows(fetch func(state []byte) ([]row, []byte, error), state []byte, term string, limit, budget int) ([]row, []byte, error) {
	term = strings.TrimSpace(term)
	out := []row{}
	for scanned := 0; ; {
		page, next, err := fetch(state)
		if err != nil {
			return nil, nil, err
		}
		out = append(out, matchRows(page, term)...)
		scanned += len(page)
		state = next
		if term == "" || len(state) == 0 || len(out) >= limit || scanned >= budget {
			return out, state, nil
		}
	}
}

// matchRows keeps the rows a free-text term matches. The projected token is a
// grid helper rather than table data, so it never matches.
func matchRows(rows []row, term string) []row {
	if term == "" {
		return rows
	}
	out := make([]row, 0, len(rows))
	for _, r := range rows {
		data := maps.Clone(r)
		delete(data, tokenColumn)
		if len(plugin.FilterRows([]row{data}, term)) == 1 {
			out = append(out, r)
		}
	}
	return out
}

// rowProjection asks for the partition token alongside the row so the grid can
// hand a real token to the partition locator without a second round trip. CQL
// rejects "*" next to a function selector, so columns are named explicitly.
func rowProjection(all, partitionKey []string) string {
	if len(all) == 0 {
		return "*"
	}
	columns := make([]string, 0, len(all)+1)
	if len(partitionKey) > 0 {
		quoted := make([]string, 0, len(partitionKey))
		for _, col := range partitionKey {
			quoted = append(quoted, quoteIdent(col))
		}
		columns = append(columns, "token("+strings.Join(quoted, ", ")+") AS "+quoteIdent(tokenColumn))
	}
	for _, col := range all {
		columns = append(columns, quoteIdent(col))
	}
	return strings.Join(columns, ", ")
}

// tableColumnNames returns every column of a table plus its partition key and
// full primary key, read from the schema catalog so a projection or a row key
// never trusts client input.
func tableColumnNames(ctx context.Context, s *Session, keyspace, table string) (all, partitionKey, primaryKey []string, err error) {
	rows, err := queryRows(ctx, s, `
SELECT column_name, kind, position
FROM system_schema.columns
WHERE keyspace_name = ? AND table_name = ?`, []any{keyspace, table})
	if err != nil {
		return nil, nil, nil, err
	}
	all = make([]string, 0, len(rows))
	for _, r := range rows {
		all = append(all, fmt.Sprint(r["column_name"]))
	}
	sort.Strings(all)
	return all, orderedKeyColumns(rows, "partition_key"), orderedKeyColumns(rows, "partition_key", "clustering"), nil
}

// primaryKeyColumns returns a table's CQL primary key - partition key columns
// followed by clustering columns, in key order - read from the schema catalog.
func primaryKeyColumns(ctx context.Context, s *Session, keyspace, table string) ([]string, error) {
	rows, err := queryRows(ctx, s, `
SELECT column_name, kind, position
FROM system_schema.columns
WHERE keyspace_name = ? AND table_name = ?`, []any{keyspace, table})
	if err != nil {
		return nil, err
	}
	return orderedKeyColumns(rows, "partition_key", "clustering"), nil
}

func partitionKeyColumns(ctx context.Context, s *Session, keyspace, table string) ([]string, error) {
	rows, err := queryRows(ctx, s, `
SELECT column_name, kind, position
FROM system_schema.columns
WHERE keyspace_name = ? AND table_name = ?`, []any{keyspace, table})
	if err != nil {
		return nil, err
	}
	return orderedKeyColumns(rows, "partition_key"), nil
}

func orderedKeyColumns(rows []row, kinds ...string) []string {
	wanted := map[string]bool{}
	for _, kind := range kinds {
		wanted[kind] = true
	}
	keyRows := make([]row, 0, len(rows))
	for _, r := range rows {
		if wanted[fmt.Sprint(r["kind"])] {
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
	return out
}

// attachRowKeys tags each row with the primary-key/value map the editable grid
// echoes back for UPDATE/DELETE. The grid stays read-only when the table has no
// usable primary key, or when a key column is itself sensitive.
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
	s, err := scyllaSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT column_name, kind, position, type, clustering_order
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
		columnName := fmt.Sprint(rows[i]["column_name"])
		databaseType := fmt.Sprint(rows[i]["type"])
		kind := fmt.Sprint(rows[i]["kind"])
		readOnly := sqldb.RedactColumn(columnName, s.opts.RedactPatterns) || kind == "partition_key" || kind == "clustering"
		sqldb.AnnotateTableColumn(rows[i], databaseType, readOnly)
	}
	return pageRows(rc, rows)
}

func tableIndexes(rc *plugin.RequestContext) (any, error) {
	keyspace, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	s, err := scyllaSession(rc)
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
		r["target"] = optionValue(r["options"], "target")
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
	s, err := scyllaSession(rc)
	if err != nil {
		return nil, err
	}
	meta, err := queryRows(rc.Ctx, s, `
SELECT * FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?`, []any{keyspace, table})
	if err != nil {
		return nil, err
	}
	definition := row{"keyspace": keyspace, "table": table, "columns": cols, "indexes": indexes}
	if len(meta) > 0 {
		definition["options"] = meta[0]
		definition["cql"] = createTableCQL(keyspace, table, cols.(plugin.Page[row]).Items, meta[0])
	}
	return definition, nil
}

// createTableCQL renders the CREATE TABLE statement for the schema catalog rows,
// so the Definition tab shows a statement an operator can copy and replay.
func createTableCQL(keyspace, table string, columns []row, options row) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE %s (\n", qualified(keyspace, table))
	partition := []string{}
	clustering := []string{}
	for _, col := range columns {
		name := fmt.Sprint(col["column_name"])
		fmt.Fprintf(&b, "    %s %s", quoteIdent(name), fmt.Sprint(col["type"]))
		if fmt.Sprint(col["kind"]) == "static" {
			b.WriteString(" static")
		}
		b.WriteString(",\n")
		switch fmt.Sprint(col["kind"]) {
		case "partition_key":
			partition = append(partition, quoteIdent(name))
		case "clustering":
			clustering = append(clustering, quoteIdent(name))
		}
	}
	key := strings.Join(partition, ", ")
	if len(partition) > 1 {
		key = "(" + key + ")"
	}
	if len(clustering) > 0 {
		key += ", " + strings.Join(clustering, ", ")
	}
	fmt.Fprintf(&b, "    PRIMARY KEY (%s)\n) WITH compaction = %s\n", key, cqlMapLiteral(options["compaction"]))
	fmt.Fprintf(&b, "    AND caching = %s\n", cqlMapLiteral(options["caching"]))
	fmt.Fprintf(&b, "    AND compression = %s\n", cqlMapLiteral(options["compression"]))
	fmt.Fprintf(&b, "    AND gc_grace_seconds = %d\n", intValue(options["gc_grace_seconds"]))
	fmt.Fprintf(&b, "    AND default_time_to_live = %d;", intValue(options["default_time_to_live"]))
	return b.String()
}

func viewDefinition(rc *plugin.RequestContext) (any, error) {
	keyspace, view, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	items, err := viewItems(rc)
	if err != nil {
		return nil, err
	}
	for _, r := range items {
		if r["keyspace"] == keyspace && r["name"] == view {
			return r, nil
		}
	}
	return nil, plugin.ErrNotFound
}

func completionRoute(rc *plugin.RequestContext) (any, error) {
	s, err := scyllaSession(rc)
	if err != nil {
		return nil, err
	}
	items := []sqldb.CompletionItem{}
	for _, keyword := range []string{
		"SELECT", "FROM", "WHERE", "LIMIT", "ALLOW FILTERING", "PER PARTITION LIMIT",
		"INSERT INTO", "UPDATE", "DELETE FROM", "BEGIN BATCH", "APPLY BATCH",
		"CREATE TABLE", "ALTER TABLE", "TRUNCATE", "USING TTL", "USING TIMEOUT",
		"IF NOT EXISTS", "BYPASS CACHE", "token(",
	} {
		items = append(items, sqldb.CompletionItem{Label: keyword, Type: "keyword"})
	}
	for _, table := range []string{
		"system.cluster_status", "system.runtime_info", "system.token_ring",
		"system.clients", "system.size_estimates", "system.large_partitions",
		"system.compaction_history", "system.config", "system_traces.sessions",
	} {
		items = append(items, sqldb.CompletionItem{Label: table, Type: "table", Detail: "ScyllaDB virtual table", Apply: table})
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
	s, err := scyllaSession(rc)
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
		Tablets               string `json:"tablets"`
		InitialTablets        int    `json:"initial_tablets"`
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
	tablets, err := tabletsClause(req.Tablets, req.InitialTablets)
	if err != nil {
		return nil, err
	}
	cql += tablets
	if err := execCQL(rc.Ctx, s, cql); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

// tabletsClause renders ScyllaDB's per-keyspace tablet option. An empty or
// "default" selection leaves the cluster default in place.
func tabletsClause(mode string, initial int) (string, error) {
	switch strings.TrimSpace(mode) {
	case "", "default":
		return "", nil
	case "disabled":
		return " AND tablets = {'enabled': false}", nil
	case "enabled":
		if initial <= 0 {
			return " AND tablets = {'enabled': true}", nil
		}
		if initial > 4096 {
			return "", fmt.Errorf("%w: initial tablets must be at most 4096", plugin.ErrInvalidInput)
		}
		return fmt.Sprintf(" AND tablets = {'enabled': true, 'initial': %d}", initial), nil
	default:
		return "", fmt.Errorf("%w: unsupported tablets mode", plugin.ErrInvalidInput)
	}
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
	s, err := scyllaSession(rc)
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
	if err := execCQL(rc.Ctx, s, prefix+qualified(keyspace, name)+" ("+strings.Join(fields, ", ")+")"); err != nil {
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
	s, err := scyllaSession(rc)
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
		Name               string `json:"name" validate:"required"`
		Columns            any    `json:"columns" validate:"required"`
		PrimaryKey         string `json:"primary_key" validate:"required"`
		CompactionStrategy string `json:"compaction_strategy"`
		DefaultTimeToLive  int    `json:"default_time_to_live"`
		IfNotExists        bool   `json:"if_not_exists"`
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
	options := []string{}
	if strategy := strings.TrimSpace(req.CompactionStrategy); strategy != "" {
		if !isKnownCompactionStrategy(strategy) {
			return nil, fmt.Errorf("%w: unsupported compaction strategy", plugin.ErrInvalidInput)
		}
		options = append(options, fmt.Sprintf("compaction = {'class': '%s'}", strategy))
	}
	if req.DefaultTimeToLive > 0 {
		options = append(options, fmt.Sprintf("default_time_to_live = %d", req.DefaultTimeToLive))
	}
	if len(options) > 0 {
		cql += " WITH " + strings.Join(options, " AND ")
	}
	if err := execCQL(rc.Ctx, s, cql); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func isKnownCompactionStrategy(name string) bool {
	switch name {
	case "IncrementalCompactionStrategy", "SizeTieredCompactionStrategy", "LeveledCompactionStrategy", "TimeWindowCompactionStrategy":
		return true
	default:
		return false
	}
}

func addColumn(rc *plugin.RequestContext) (any, error) {
	s, err := scyllaSession(rc)
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
	s, err := scyllaSession(rc)
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
	s, err := scyllaSession(rc)
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
		Local  bool   `json:"local"`
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
	target := quoteIdent(column)
	if req.Local {
		partition, err := partitionKeyColumns(rc.Ctx, s, keyspace, table)
		if err != nil {
			return nil, err
		}
		if len(partition) == 0 {
			return nil, fmt.Errorf("%w: table has no partition key for a local index", plugin.ErrInvalidInput)
		}
		quoted := make([]string, 0, len(partition))
		for _, col := range partition {
			quoted = append(quoted, quoteIdent(col))
		}
		target = "(" + strings.Join(quoted, ", ") + "), " + quoteIdent(column)
	}
	if err := execCQL(rc.Ctx, s, "CREATE INDEX "+quoteIdent(name)+" ON "+qualified(keyspace, table)+" ("+target+")"); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func dropIndex(rc *plugin.RequestContext) (any, error) {
	keyspace, err := safeIdent(rc.Param("keyspace"))
	if err != nil {
		return nil, err
	}
	name, err := safeIdent(rc.Param("name"))
	if err != nil {
		return nil, err
	}
	return execDDL(rc, "DROP INDEX "+qualified(keyspace, name))
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
	keyspace, err := safeIdent(rc.Param("keyspace"))
	if err != nil {
		return nil, err
	}
	view, err := safeIdent(rc.Param("view"))
	if err != nil {
		return nil, err
	}
	return execDDL(rc, "DROP MATERIALIZED VIEW "+qualified(keyspace, view))
}

func execDDL(rc *plugin.RequestContext, cql string) (any, error) {
	s, err := scyllaSession(rc)
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
	s, err := scyllaSession(rc)
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
	delete(m.Values, tokenColumn)
	delete(m.Key, tokenColumn)
	return s, keyspace, table, m, nil
}

// validateRowKey loads the table's primary key and rejects any client key that
// is not exactly that key, so a mutation can only ever target one row.
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
		return scyllaErr(err)
	}
	return nil
}

func cancelQuery(rc *plugin.RequestContext) (any, error) {
	s, err := scyllaSession(rc)
	if err != nil {
		return nil, err
	}
	return actionResult{OK: s.cancelAll()}, nil
}

func queryStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := scyllaSession(rc)
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
			// A malformed frame leaves the decoder unable to resynchronise, so
			// only a well-formed frame carrying the wrong shape is recoverable.
			var typeErr *json.UnmarshalTypeError
			if !errors.As(err, &typeErr) {
				return nil
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

// executeQueryRequest runs one console submission. "TRACING ON;" as the first
// statement turns on server-side tracing for the rest of the submission and the
// trace id is reported back with the result.
func executeQueryRequest(parent context.Context, s *Session, req sqldb.QueryRequest) (sqldb.QueryResult, error) {
	statements, tracing := splitTracingDirective(sqldb.SplitStatements(req.Query))
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
	tracer := &traceCollector{}
	for _, st := range statements {
		res, err := executeStatement(ctx, s, st, tracing, tracer)
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
		if ids := tracer.IDs(); len(ids) > 0 {
			out.CommandTag = last.CommandTag + " · trace " + ids[len(ids)-1]
		}
	}
	return out, nil
}

// splitTracingDirective consumes a leading cqlsh-style TRACING ON/OFF directive
// so the console can toggle server-side tracing without a separate control.
func splitTracingDirective(statements []string) ([]string, bool) {
	tracing := false
	out := make([]string, 0, len(statements))
	for _, st := range statements {
		switch strings.ToUpper(strings.Join(strings.Fields(st), " ")) {
		case "TRACING ON":
			tracing = true
		case "TRACING OFF":
			tracing = false
		default:
			out = append(out, st)
		}
	}
	return out, tracing
}

func executeStatement(ctx context.Context, s *Session, statement string, tracing bool, tracer *traceCollector) (sqldb.StatementResult, error) {
	start := time.Now()
	query := s.db.Query(statement).WithContext(ctx).Consistency(s.opts.Consistency)
	if tracing {
		query = query.Trace(tracer)
	}
	if !statementReturnsRows(statement) {
		if err := query.Exec(); err != nil {
			return sqldb.StatementResult{}, scyllaErr(err)
		}
		return sqldb.StatementResult{Statement: statement, ElapsedMS: time.Since(start).Milliseconds(), CommandTag: sqldb.FirstKeyword(statement)}, nil
	}
	iter := query.PageSize(s.opts.RowLimit).Iter()
	iterColumns := iter.Columns()
	rows, err := iterRows(iter, s.opts.RowLimit, s.opts.RedactPatterns)
	if err != nil {
		return sqldb.StatementResult{}, err
	}
	columns := make([]string, 0, len(iterColumns))
	for _, col := range iterColumns {
		columns = append(columns, col.Name)
	}
	if len(columns) == 0 && len(rows) > 0 {
		for name := range rows[0] {
			columns = append(columns, name)
		}
		sort.Strings(columns)
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
	return queryRowsOn(ctx, s, "", cql, args)
}

// queryRowsOn pins a query to one node. ScyllaDB's node-local virtual tables
// (system.clients, system.runtime_info) only ever describe the node that
// answered, so a per-node panel has to name its coordinator.
func queryRowsOn(ctx context.Context, s *Session, hostID, cql string, args []any) ([]row, error) {
	ctx, cancel := context.WithTimeout(ctx, s.opts.QueryTimeout)
	defer cancel()
	query := s.db.Query(cql, args...).WithContext(ctx).Consistency(s.opts.Consistency).PageSize(s.opts.PageSize)
	if hostID != "" {
		query = query.SetHostID(hostID)
	}
	return iterRows(query.Iter(), scanCap, nil)
}

func execCQL(ctx context.Context, s *Session, cql string) error {
	ctx, cancel := context.WithTimeout(ctx, s.opts.QueryTimeout)
	defer cancel()
	if err := s.db.Query(cql).WithContext(ctx).Consistency(s.opts.Consistency).Exec(); err != nil {
		return scyllaErr(err)
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
		return nil, scyllaErr(err)
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
	case bool, string, float32, float64, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64:
		// Scalars are passed through: round-tripping them via JSON would widen
		// 64-bit integers into float64 and silently lose precision.
		return x
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
	// A listing that filled the scan cap was cut short, so it carries no Total:
	// the grid must not show a count it already knows is short.
	truncated := len(rows) >= scanCap
	rows = plugin.FilterRows(rows, req.Search())
	sortRows(rows, req.Sort)
	start, err := offsetCursor(req.Cursor)
	if err != nil {
		return plugin.Page[row]{}, err
	}
	start = min(start, len(rows))
	end := min(start+req.Limit, len(rows))
	next := ""
	if end < len(rows) {
		next = strconv.Itoa(end)
	}
	page := plugin.Page[row]{Items: rows[start:end], NextCursor: next}
	if !truncated {
		total := len(rows)
		page.Total = &total
	}
	return page, nil
}

func treeFromPage(rc *plugin.RequestContext, kind, iconName, labelKey string, load func(*plugin.RequestContext) (any, error)) (any, error) {
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
		ref, _ := r["ref"].(plugin.ResourceIdentity)
		if ref.Kind == "" {
			continue
		}
		label := fmt.Sprint(r[labelKey])
		if keyspace := fmt.Sprint(r["keyspace"]); keyspace != "" && keyspace != "<nil>" {
			label = keyspace + "." + label
		}
		nodes = append(nodes, plugin.TreeNode{Key: kind + ":" + ref.UID, Label: label, Icon: icon(iconName), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

func scopedOverview(rc *plugin.RequestContext, namespaceParam, nameParam, rowNameKey string, load func(*plugin.RequestContext) ([]row, error)) (any, error) {
	namespace, err := safeIdent(rc.Param(namespaceParam))
	if err != nil {
		return nil, err
	}
	name, err := safeIdent(rc.Param(nameParam))
	if err != nil {
		return nil, err
	}
	items, err := load(rc)
	if err != nil {
		return nil, err
	}
	for _, r := range items {
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

func scopedKeyspace(rc *plugin.RequestContext) (string, error) {
	keyspace := strings.TrimSpace(rc.Query().Get("p.keyspace"))
	if keyspace == "" {
		keyspace = strings.TrimSpace(rc.Param("keyspace"))
	}
	if keyspace == "" {
		return "", nil
	}
	return safeIdent(keyspace)
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
	case "SELECT", "DESC", "DESCRIBE", "LIST", "USE":
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
	case "SELECT", "LIST", "DESC", "DESCRIBE":
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

func scyllaErr(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, gocql.ErrNotFound):
		return fmt.Errorf("%w: %v", plugin.ErrNotFound, err)
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	var reqErr gocql.RequestError
	if errors.As(err, &reqErr) {
		switch reqErr.Code() {
		case gocql.ErrCodeUnauthorized:
			return fmt.Errorf("%w: %v", plugin.ErrForbidden, err)
		case gocql.ErrCodeInvalid, gocql.ErrCodeSyntax, gocql.ErrCodeConfig:
			return fmt.Errorf("%w: %v", plugin.ErrInvalidInput, err)
		case gocql.ErrCodeAlreadyExists:
			return fmt.Errorf("%w: %v", plugin.ErrConflict, err)
		}
	}
	return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
}

func sortRows(rows []row, sorts []plugin.SortKey) {
	if len(sorts) == 0 {
		return
	}
	spec := sorts[0]
	sort.SliceStable(rows, func(i, j int) bool {
		less := compareValues(rows[i][spec.Field], rows[j][spec.Field])
		if spec.Desc {
			return less > 0
		}
		return less < 0
	})
}

// compareValues orders numbers numerically and everything else as text, so
// numeric columns such as sizes and token counts sort correctly.
func compareValues(a, b any) int {
	an, aok := numberOf(a)
	bn, bok := numberOf(b)
	if aok && bok {
		switch {
		case an < bn:
			return -1
		case an > bn:
			return 1
		default:
			return 0
		}
	}
	return strings.Compare(strings.ToLower(fmt.Sprint(a)), strings.ToLower(fmt.Sprint(b)))
}

func numberOf(v any) (float64, bool) {
	switch x := v.(type) {
	case int:
		return float64(x), true
	case int32:
		return float64(x), true
	case int64:
		return float64(x), true
	case float32:
		return float64(x), true
	case float64:
		return x, true
	default:
		return 0, false
	}
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

// redactRow masks sensitive columns. The projected partition token is plugin
// metadata, not user data, and its name would otherwise match the default
// "token" redaction pattern.
func redactRow(r row, patterns []string) {
	for key, value := range r {
		if key == tokenColumn {
			continue
		}
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

// cqlMapLiteral renders a schema option map back as the CQL map literal the
// definition tab replays.
func cqlMapLiteral(value any) string {
	values, ok := value.(map[string]any)
	if !ok {
		if raw, ok := value.(map[string]string); ok {
			values = map[string]any{}
			for k, v := range raw {
				values[k] = v
			}
		} else {
			return "{}"
		}
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("'%s': '%v'", key, values[key]))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func compactionStrategy(value any) string {
	if class := optionValue(value, "class"); class != "" {
		return class[strings.LastIndex(class, ".")+1:]
	}
	return ""
}

func optionValue(value any, key string) string {
	switch options := value.(type) {
	case map[string]string:
		return options[key]
	case map[string]any:
		if v, ok := options[key]; ok {
			return fmt.Sprint(v)
		}
	}
	return ""
}

func typeFields(names, types any) string {
	nameList := stringSlice(names)
	typeList := stringSlice(types)
	count := max(len(nameList), len(typeList))
	parts := make([]string, 0, count)
	for i := range count {
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

func isSystemKeyspace(name string) bool {
	return strings.HasPrefix(name, "system")
}

func columnKindRank(kind string) int {
	switch kind {
	case "partition_key":
		return 0
	case "clustering":
		return 1
	case "static":
		return 2
	case "regular":
		return 3
	default:
		return 4
	}
}

func intValue(v any) int {
	switch x := v.(type) {
	case int:
		return x
	case int32:
		return int(x)
	case int64:
		return int(x)
	case float64:
		return int(x)
	default:
		i, _ := strconv.Atoi(fmt.Sprint(x))
		return i
	}
}

func int64Value(v any) int64 {
	switch x := v.(type) {
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case int64:
		return x
	case float64:
		return int64(x)
	default:
		i, _ := strconv.ParseInt(strings.TrimSpace(fmt.Sprint(x)), 10, 64)
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
