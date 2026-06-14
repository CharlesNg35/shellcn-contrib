package clickhouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

type row map[string]any

type actionResult struct {
	OK      bool   `json:"ok"`
	Message string `json:"message,omitempty"`
}

type confirmationError struct {
	message string
}

func (e confirmationError) Error() string { return e.message }

var dialect = sqldb.Dialect{QuoteIdent: quoteIdent, Placeholder: sqldb.QuestionPlaceholder}

func routes() []plugin.Route {
	return []plugin.Route{
		{ID: "clickhouse.databases.tree", Method: plugin.MethodGet, Path: "/tree/databases", Permission: "clickhouse.databases.read", Risk: plugin.RiskSafe, AuditEvent: "clickhouse.databases.tree", Handle: treeDatabases},
		{ID: "clickhouse.databases.list", Method: plugin.MethodGet, Path: "/databases", Permission: "clickhouse.databases.read", Risk: plugin.RiskSafe, AuditEvent: "clickhouse.databases.list", Handle: listDatabases},
		{ID: "clickhouse.database.overview", Method: plugin.MethodGet, Path: "/databases/{database}/overview", Permission: "clickhouse.databases.read", Risk: plugin.RiskSafe, AuditEvent: "clickhouse.database.overview", Handle: databaseOverview},
		{ID: "clickhouse.relations.tree", Method: plugin.MethodGet, Path: "/tree/relations", Permission: "clickhouse.tables.read", Risk: plugin.RiskSafe, AuditEvent: "clickhouse.relations.tree", Handle: treeRelations},
		{ID: "clickhouse.tables.list", Method: plugin.MethodGet, Path: "/tables", Permission: "clickhouse.tables.read", Risk: plugin.RiskSafe, AuditEvent: "clickhouse.tables.list", Handle: listTables},
		{ID: "clickhouse.views.list", Method: plugin.MethodGet, Path: "/views", Permission: "clickhouse.views.read", Risk: plugin.RiskSafe, AuditEvent: "clickhouse.views.list", Handle: listViews},
		{ID: "clickhouse.view.drop", Method: plugin.MethodDelete, Path: "/views/{database}/{view}", Permission: "clickhouse.tables.write", Risk: plugin.RiskDestructive, AuditEvent: "clickhouse.view.drop", Handle: dropView},
		{ID: "clickhouse.dictionaries.tree", Method: plugin.MethodGet, Path: "/tree/dictionaries", Permission: "clickhouse.dictionaries.read", Risk: plugin.RiskSafe, AuditEvent: "clickhouse.dictionaries.tree", Handle: treeDictionaries},
		{ID: "clickhouse.dictionaries.list", Method: plugin.MethodGet, Path: "/dictionaries", Permission: "clickhouse.dictionaries.read", Risk: plugin.RiskSafe, AuditEvent: "clickhouse.dictionaries.list", Handle: listDictionaries},
		{ID: "clickhouse.dictionary.overview", Method: plugin.MethodGet, Path: "/dictionaries/{database}/{table}/overview", Permission: "clickhouse.dictionaries.read", Risk: plugin.RiskSafe, AuditEvent: "clickhouse.dictionary.overview", Handle: dictionaryOverview},
		{ID: "clickhouse.mutations.tree", Method: plugin.MethodGet, Path: "/tree/mutations", Permission: "clickhouse.mutations.read", Risk: plugin.RiskSafe, AuditEvent: "clickhouse.mutations.tree", Handle: treeMutations},
		{ID: "clickhouse.mutations.list", Method: plugin.MethodGet, Path: "/mutations", Permission: "clickhouse.mutations.read", Risk: plugin.RiskSafe, AuditEvent: "clickhouse.mutations.list", Handle: listMutations},
		{ID: "clickhouse.mutation.overview", Method: plugin.MethodGet, Path: "/mutations/{id}/overview", Permission: "clickhouse.mutations.read", Risk: plugin.RiskSafe, AuditEvent: "clickhouse.mutation.overview", Handle: mutationOverview},
		{ID: "clickhouse.merges.tree", Method: plugin.MethodGet, Path: "/tree/merges", Permission: "clickhouse.merges.read", Risk: plugin.RiskSafe, AuditEvent: "clickhouse.merges.tree", Handle: treeMerges},
		{ID: "clickhouse.merges.list", Method: plugin.MethodGet, Path: "/merges", Permission: "clickhouse.merges.read", Risk: plugin.RiskSafe, AuditEvent: "clickhouse.merges.list", Handle: listMerges},
		{ID: "clickhouse.merge.overview", Method: plugin.MethodGet, Path: "/merges/{id}/overview", Permission: "clickhouse.merges.read", Risk: plugin.RiskSafe, AuditEvent: "clickhouse.merge.overview", Handle: mergeOverview},
		{ID: "clickhouse.processes.tree", Method: plugin.MethodGet, Path: "/tree/processes", Permission: "clickhouse.processes.read", Risk: plugin.RiskSafe, AuditEvent: "clickhouse.processes.tree", Handle: treeProcesses},
		{ID: "clickhouse.processes.list", Method: plugin.MethodGet, Path: "/processes", Permission: "clickhouse.processes.read", Risk: plugin.RiskSafe, AuditEvent: "clickhouse.processes.list", Handle: listProcesses},
		{ID: "clickhouse.process.overview", Method: plugin.MethodGet, Path: "/processes/{id}/overview", Permission: "clickhouse.processes.read", Risk: plugin.RiskSafe, AuditEvent: "clickhouse.process.overview", Handle: processOverview},
		{ID: "clickhouse.users.tree", Method: plugin.MethodGet, Path: "/tree/users", Permission: "clickhouse.users.read", Risk: plugin.RiskSafe, AuditEvent: "clickhouse.users.tree", Handle: treeUsers},
		{ID: "clickhouse.users.list", Method: plugin.MethodGet, Path: "/users", Permission: "clickhouse.users.read", Risk: plugin.RiskSafe, AuditEvent: "clickhouse.users.list", Handle: listUsers},
		{ID: "clickhouse.user.overview", Method: plugin.MethodGet, Path: "/users/{user}/overview", Permission: "clickhouse.users.read", Risk: plugin.RiskSafe, AuditEvent: "clickhouse.user.overview", Handle: userOverview},
		{ID: "clickhouse.table.rows", Method: plugin.MethodGet, Path: "/tables/{database}/{table}/rows", Permission: "clickhouse.tables.data.read", Risk: plugin.RiskSafe, AuditEvent: "clickhouse.table.rows", Handle: tableRows},
		{ID: "clickhouse.table.row.insert", Method: plugin.MethodPost, Path: "/tables/{database}/{table}/rows", Permission: "clickhouse.tables.data.write", Risk: plugin.RiskWrite, AuditEvent: "clickhouse.table.row.insert", Handle: insertRow},
		{ID: "clickhouse.table.row.update", Method: plugin.MethodPatch, Path: "/tables/{database}/{table}/rows", Permission: "clickhouse.tables.data.write", Risk: plugin.RiskDestructive, AuditEvent: "clickhouse.table.row.update", Handle: updateRow},
		{ID: "clickhouse.table.row.delete", Method: plugin.MethodDelete, Path: "/tables/{database}/{table}/rows", Permission: "clickhouse.tables.data.delete", Risk: plugin.RiskDestructive, AuditEvent: "clickhouse.table.row.delete", Handle: deleteRow},
		{ID: "clickhouse.view.rows", Method: plugin.MethodGet, Path: "/views/{database}/{table}/rows", Permission: "clickhouse.views.data.read", Risk: plugin.RiskSafe, AuditEvent: "clickhouse.view.rows", Handle: tableRows},
		{ID: "clickhouse.table.columns", Method: plugin.MethodGet, Path: "/tables/{database}/{table}/columns", Permission: "clickhouse.tables.read", Risk: plugin.RiskSafe, AuditEvent: "clickhouse.table.columns", Handle: tableColumnsRoute},
		{ID: "clickhouse.table.indexes", Method: plugin.MethodGet, Path: "/tables/{database}/{table}/indexes", Permission: "clickhouse.tables.read", Risk: plugin.RiskSafe, AuditEvent: "clickhouse.table.indexes", Handle: tableIndexes},
		{ID: "clickhouse.table.constraints", Method: plugin.MethodGet, Path: "/tables/{database}/{table}/constraints", Permission: "clickhouse.tables.read", Risk: plugin.RiskSafe, AuditEvent: "clickhouse.table.constraints", Handle: tableConstraints},
		{ID: "clickhouse.table.definition", Method: plugin.MethodGet, Path: "/tables/{database}/{table}/definition", Permission: "clickhouse.tables.read", Risk: plugin.RiskSafe, AuditEvent: "clickhouse.table.definition", Handle: tableDefinition},
		{ID: "clickhouse.view.definition", Method: plugin.MethodGet, Path: "/views/{database}/{table}/definition", Permission: "clickhouse.views.read", Risk: plugin.RiskSafe, AuditEvent: "clickhouse.view.definition", Handle: tableDefinition},
		{ID: "clickhouse.completion", Method: plugin.MethodGet, Path: "/completion", Permission: "clickhouse.databases.read", Risk: plugin.RiskSafe, AuditEvent: "clickhouse.completion", Handle: completionRoute},
		{ID: "clickhouse.database.create", Method: plugin.MethodPost, Path: "/databases", Permission: "clickhouse.databases.write", Risk: plugin.RiskWrite, AuditEvent: "clickhouse.database.create", Input: databaseCreateSchema(), Handle: createDatabase},
		{ID: "clickhouse.database.drop", Method: plugin.MethodDelete, Path: "/databases/{database}", Permission: "clickhouse.databases.delete", Risk: plugin.RiskDestructive, AuditEvent: "clickhouse.database.drop", Handle: dropDatabase},
		{ID: "clickhouse.table.create", Method: plugin.MethodPost, Path: "/databases/{database}/tables", Permission: "clickhouse.tables.write", Risk: plugin.RiskWrite, AuditEvent: "clickhouse.table.create", Input: tableCreateSchema(), Handle: createTable},
		{ID: "clickhouse.table.rename", Method: plugin.MethodPost, Path: "/tables/{database}/{table}/rename", Permission: "clickhouse.tables.write", Risk: plugin.RiskWrite, AuditEvent: "clickhouse.table.rename", Input: tableRenameSchema(), Handle: renameTable},
		{ID: "clickhouse.column.add", Method: plugin.MethodPost, Path: "/tables/{database}/{table}/columns", Permission: "clickhouse.tables.write", Risk: plugin.RiskWrite, AuditEvent: "clickhouse.column.add", Input: columnAddSchema(), Handle: addColumn},
		{ID: "clickhouse.column.alter", Method: plugin.MethodPost, Path: "/tables/{database}/{table}/columns/alter", Permission: "clickhouse.tables.write", Risk: plugin.RiskDestructive, AuditEvent: "clickhouse.column.alter", Input: columnAlterSchema(), Handle: alterColumn},
		{ID: "clickhouse.column.drop", Method: plugin.MethodPost, Path: "/tables/{database}/{table}/columns/drop", Permission: "clickhouse.tables.write", Risk: plugin.RiskDestructive, AuditEvent: "clickhouse.column.drop", Handle: dropColumn},
		{ID: "clickhouse.index.create", Method: plugin.MethodPost, Path: "/tables/{database}/{table}/indexes", Permission: "clickhouse.tables.write", Risk: plugin.RiskWrite, AuditEvent: "clickhouse.index.create", Input: indexCreateSchema(), Handle: createIndex},
		{ID: "clickhouse.index.drop", Method: plugin.MethodPost, Path: "/tables/{database}/{table}/indexes/drop", Permission: "clickhouse.tables.write", Risk: plugin.RiskDestructive, AuditEvent: "clickhouse.index.drop", Handle: dropIndex},
		{ID: "clickhouse.constraint.add", Method: plugin.MethodPost, Path: "/tables/{database}/{table}/constraints", Permission: "clickhouse.tables.write", Risk: plugin.RiskWrite, AuditEvent: "clickhouse.constraint.add", Input: constraintAddSchema(), Handle: addConstraint},
		{ID: "clickhouse.constraint.drop", Method: plugin.MethodPost, Path: "/tables/{database}/{table}/constraints/drop", Permission: "clickhouse.tables.write", Risk: plugin.RiskDestructive, AuditEvent: "clickhouse.constraint.drop", Handle: dropConstraint},
		{ID: "clickhouse.table.truncate", Method: plugin.MethodPost, Path: "/tables/{database}/{table}/truncate", Permission: "clickhouse.tables.delete", Risk: plugin.RiskDestructive, AuditEvent: "clickhouse.table.truncate", Handle: truncateTable},
		{ID: "clickhouse.table.drop", Method: plugin.MethodDelete, Path: "/tables/{database}/{table}", Permission: "clickhouse.tables.delete", Risk: plugin.RiskDestructive, AuditEvent: "clickhouse.table.drop", Handle: dropTable},
		{ID: "clickhouse.process.kill", Method: plugin.MethodPost, Path: "/processes/{id}/kill", Permission: "clickhouse.processes.kill", Risk: plugin.RiskDestructive, AuditEvent: "clickhouse.process.kill", Handle: killProcess},
		{ID: "clickhouse.mutation.kill", Method: plugin.MethodPost, Path: "/mutations/{database}/{table}/{id}/kill", Permission: "clickhouse.mutations.kill", Risk: plugin.RiskDestructive, AuditEvent: "clickhouse.mutation.kill", Handle: killMutation},
		{ID: "clickhouse.merge.stop", Method: plugin.MethodPost, Path: "/merges/{database}/{table}/stop", Permission: "clickhouse.merges.control", Risk: plugin.RiskDestructive, AuditEvent: "clickhouse.merge.stop", Handle: stopMerges},
		{ID: "clickhouse.merge.start", Method: plugin.MethodPost, Path: "/merges/{database}/{table}/start", Permission: "clickhouse.merges.control", Risk: plugin.RiskWrite, AuditEvent: "clickhouse.merge.start", Handle: startMerges},
		{ID: "clickhouse.user.create", Method: plugin.MethodPost, Path: "/users", Permission: "clickhouse.users.write", Risk: plugin.RiskWrite, AuditEvent: "clickhouse.user.create", Input: userCreateSchema(), Handle: createUser},
		{ID: "clickhouse.user.grant", Method: plugin.MethodPost, Path: "/users/{user}/grant", Permission: "clickhouse.users.write", Risk: plugin.RiskPrivileged, AuditEvent: "clickhouse.user.grant", Input: userGrantSchema(), Handle: grantUser},
		{ID: "clickhouse.user.drop", Method: plugin.MethodDelete, Path: "/users/{user}", Permission: "clickhouse.users.delete", Risk: plugin.RiskDestructive, AuditEvent: "clickhouse.user.drop", Handle: dropUser},
		{ID: "clickhouse.query", Method: plugin.MethodWS, Path: "/query", Permission: "clickhouse.query.execute", Risk: plugin.RiskPrivileged, AuditEvent: "clickhouse.query", Stream: queryStream},
		{ID: "clickhouse.query.cancel", Method: plugin.MethodPost, Path: "/query/cancel", Permission: "clickhouse.query.cancel", Risk: plugin.RiskWrite, AuditEvent: "clickhouse.query.cancel", Handle: cancelQuery},
	}
}

func clickhouseSession(rc *plugin.RequestContext) (*Session, error) {
	return unwrap(rc.Session)
}

func databaseCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Database", Fields: []plugin.Field{
		{Key: "name", Label: "Database name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		{Key: "if_not_exists", Label: "If not exists", Type: plugin.FieldToggle, Default: true},
	}}}}
}

func tableCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Table", Fields: []plugin.Field{
		{Key: "name", Label: "Table name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		sqldb.ColumnsArrayField(sqldb.ColumnsField{TypePlaceholder: "DateTime", TypeSuggestions: []string{"UInt64", "UInt32", "UInt8", "Int64", "Int32", "Int8", "Float64", "Float32", "Decimal(10,2)", "String", "FixedString(16)", "Date", "DateTime", "DateTime64(3)", "Bool", "UUID", "Nullable(String)", "Array(String)", "LowCardinality(String)", "JSON"}, Default: true}),
		{Key: "engine", Label: "Engine", Type: plugin.FieldText, Required: true, Default: "MergeTree"},
		{Key: "order_by", Label: "ORDER BY", Type: plugin.FieldText, Required: true, Default: "tuple()"},
		{Key: "if_not_exists", Label: "If not exists", Type: plugin.FieldToggle, Default: true},
	}}}}
}

func columnAddSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Column", Fields: []plugin.Field{
		{Key: "name", Label: "Column name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		{Key: "type", Label: "Type", Type: plugin.FieldText, Required: true, Default: "String"},
		{Key: "nullable", Label: "Nullable", Type: plugin.FieldToggle, Default: false},
		{Key: "default", Label: "Default expression", Type: plugin.FieldText},
	}}}}
}

func columnAlterSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Column", Fields: []plugin.Field{
		{Key: "name", Label: "Column name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		{Key: "type", Label: "Type", Type: plugin.FieldText, Required: true, Default: "String"},
		{Key: "nullable", Label: "Nullable", Type: plugin.FieldToggle, Default: false},
		{Key: "default", Label: "Default expression", Type: plugin.FieldText},
	}}}}
}

func indexCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Data-skipping index", Fields: []plugin.Field{
		{Key: "name", Label: "Index name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		{Key: "expression", Label: "Expression", Type: plugin.FieldText, Required: true, Help: "Column or expression to index, e.g. `value` or `lower(name)`."},
		{Key: "type", Label: "Type", Type: plugin.FieldText, Required: true, Default: "minmax", Help: "Index type, e.g. minmax, set(0), bloom_filter(0.01), ngrambf_v1(...)."},
		{Key: "granularity", Label: "Granularity", Type: plugin.FieldNumber, Default: 1},
	}}}}
}

func tableRenameSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Rename table", Fields: []plugin.Field{
		{Key: "database", Label: "Target database", Type: plugin.FieldText, Help: "Leave blank to keep the current database.", Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		{Key: "name", Label: "New name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
	}}}}
}

func constraintAddSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Constraint", Fields: []plugin.Field{
		{Key: "name", Label: "Constraint name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		{Key: "expression", Label: "CHECK expression", Type: plugin.FieldText, Required: true, Help: "Boolean expression every row must satisfy, e.g. `age >= 0`."},
	}}}}
}

func userCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "User", Fields: []plugin.Field{
		{Key: "name", Label: "User name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		{Key: "auth_type", Label: "Authentication", Type: plugin.FieldSelect, Default: "sha256_password", Options: []plugin.Option{
			{Label: "Password (sha256)", Value: "sha256_password"},
			{Label: "Password (double sha1)", Value: "double_sha1_password"},
			{Label: "Plaintext password", Value: "plaintext_password"},
			{Label: "No password", Value: "no_password"},
		}},
		{Key: "password", Label: "Password", Type: plugin.FieldPassword, Secret: true, VisibleWhen: &plugin.Condition{AnyOf: []plugin.Rule{
			{Field: "auth_type", Op: plugin.OpIn, Value: []any{"sha256_password", "double_sha1_password", "plaintext_password"}},
		}}},
		{Key: "if_not_exists", Label: "If not exists", Type: plugin.FieldToggle, Default: true},
	}}}}
}

func userGrantSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Grant", Fields: []plugin.Field{
		{Key: "privilege", Label: "Privilege", Type: plugin.FieldText, Required: true, Default: "SELECT", Help: "Access type to grant, e.g. SELECT, INSERT, ALTER, or ALL."},
		{Key: "on", Label: "On", Type: plugin.FieldText, Required: true, Default: "*.*", Help: "Target scope: `*.*`, `db.*`, or `db.table`."},
		{Key: "with_grant_option", Label: "With grant option", Type: plugin.FieldToggle, Default: false},
	}}}}
}

// treeDatabases lists databases as expandable branches that drill into their
// tables/views (hierarchical, TablePlus-style).
func treeDatabases(rc *plugin.RequestContext) (any, error) {
	res, err := listDatabases(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, r := range page.Items {
		name := fmt.Sprint(r["name"])
		nodes = append(nodes, plugin.TreeNode{
			Key:            "db:" + name,
			Label:          name,
			Icon:           icon("database"),
			Ref:            &plugin.ResourceRef{Kind: "database", Name: name, UID: name},
			ChildrenSource: &plugin.DataSource{RouteID: "clickhouse.relations.tree", Params: map[string]string{"database": name}},
		})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

// treeRelations lists a database's tables and views as leaves (scoped by the
// p.database param the parent node supplies).
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

func treeDictionaries(rc *plugin.RequestContext) (any, error) {
	return treeFromPage(rc, "dictionary", "book-open", "name", listDictionaries)
}

func treeMutations(rc *plugin.RequestContext) (any, error) {
	return treeFromPage(rc, "mutation", "git-compare-arrows", "mutation_id", listMutations)
}

func treeMerges(rc *plugin.RequestContext) (any, error) {
	return treeFromPage(rc, "merge", "merge", "id", listMerges)
}

func treeProcesses(rc *plugin.RequestContext) (any, error) {
	return treeFromPage(rc, "process", "activity", "query_id", listProcesses)
}

func treeUsers(rc *plugin.RequestContext) (any, error) {
	return treeFromPage(rc, "user", "user", "user", listUsers)
}

func listDatabases(rc *plugin.RequestContext) (any, error) {
	s, err := clickhouseSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT d.name, d.engine, d.comment,
       countIf(t.engine NOT IN ('View', 'MaterializedView', 'LiveView', 'WindowView')) AS tables,
       countIf(t.engine IN ('View', 'MaterializedView', 'LiveView', 'WindowView')) AS views,
       sum(ifNull(t.total_bytes, 0)) AS size
FROM system.databases d
LEFT JOIN system.tables t ON t.database = d.name
WHERE d.name NOT IN ('INFORMATION_SCHEMA', 'information_schema', 'system')
GROUP BY d.name, d.engine, d.comment
ORDER BY d.name`, nil)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		name := fmt.Sprint(r["name"])
		r["ref"] = plugin.ResourceRef{Kind: "database", Name: name, UID: name}
	}
	return pageRows(rc, rows)
}

func databaseOverview(rc *plugin.RequestContext) (any, error) {
	database, err := sqldb.SafeIdentifier(rc.Param("database"))
	if err != nil {
		return nil, err
	}
	s, err := clickhouseSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT d.name, d.engine, d.comment, version() AS version,
       countIf(t.engine NOT IN ('View', 'MaterializedView', 'LiveView', 'WindowView')) AS tables,
       countIf(t.engine IN ('View', 'MaterializedView', 'LiveView', 'WindowView')) AS views,
       sum(ifNull(t.total_rows, 0)) AS rows,
       sum(ifNull(t.total_bytes, 0)) AS size
FROM system.databases d
LEFT JOIN system.tables t ON t.database = d.name
WHERE d.name = ?
GROUP BY d.name, d.engine, d.comment`, []any{database})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, plugin.ErrNotFound
	}
	return rows[0], nil
}

func listTables(rc *plugin.RequestContext) (any, error) {
	return relationList(rc, false, "table")
}

func listViews(rc *plugin.RequestContext) (any, error) {
	return relationList(rc, true, "view")
}

func relationList(rc *plugin.RequestContext, views bool, refKind string) (any, error) {
	s, err := clickhouseSession(rc)
	if err != nil {
		return nil, err
	}
	database, table, err := optionalTableFilter(rc)
	if err != nil {
		return nil, err
	}
	op := "NOT IN"
	if views {
		op = "IN"
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT name, database, engine, ifNull(total_rows, 0) AS rows, ifNull(total_bytes, 0) AS size,
       metadata_modification_time AS modified, comment
FROM system.tables
WHERE database NOT IN ('INFORMATION_SCHEMA', 'information_schema', 'system')
  AND engine `+op+` ('View', 'MaterializedView', 'LiveView', 'WindowView')
  AND (? = '' OR database = ?)
  AND (? = '' OR name = ?)
ORDER BY database, name`, []any{database, database, table, table})
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		name, db := fmt.Sprint(r["name"]), fmt.Sprint(r["database"])
		r["ref"] = plugin.ResourceRef{Kind: refKind, Namespace: db, Name: name, UID: db + "." + name}
	}
	return pageRows(rc, rows)
}

func listDictionaries(rc *plugin.RequestContext) (any, error) {
	s, err := clickhouseSession(rc)
	if err != nil {
		return nil, err
	}
	database, table, err := optionalTableFilter(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT name, database, status, type, origin, bytes_allocated, element_count
FROM system.dictionaries
WHERE database NOT IN ('INFORMATION_SCHEMA', 'information_schema', 'system')
  AND (? = '' OR database = ?)
  AND (? = '' OR name = ?)
ORDER BY database, name`, []any{database, database, table, table})
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		name, db := fmt.Sprint(r["name"]), fmt.Sprint(r["database"])
		r["ref"] = plugin.ResourceRef{Kind: "dictionary", Namespace: db, Name: name, UID: db + "." + name}
	}
	return pageRows(rc, rows)
}

func dictionaryOverview(rc *plugin.RequestContext) (any, error) {
	return tableScopedOverview(rc, "dictionary", "name", listDictionaries)
}

func listMutations(rc *plugin.RequestContext) (any, error) {
	s, err := clickhouseSession(rc)
	if err != nil {
		return nil, err
	}
	database, table, err := optionalTableFilter(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT mutation_id, database, table, command, create_time, is_done, latest_fail_reason
FROM system.mutations
WHERE database NOT IN ('INFORMATION_SCHEMA', 'information_schema', 'system')
  AND (? = '' OR database = ?)
  AND (? = '' OR table = ?)
ORDER BY create_time DESC, database, table`, []any{database, database, table, table})
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		id := mutationUID(r)
		r["ref"] = plugin.ResourceRef{Kind: "mutation", Namespace: fmt.Sprint(r["database"]), Scope: fmt.Sprint(r["table"]), Name: fmt.Sprint(r["mutation_id"]), UID: id}
	}
	return pageRows(rc, rows)
}

func mutationOverview(rc *plugin.RequestContext) (any, error) {
	return overviewByUID(rc, "id", listMutations)
}

func listMerges(rc *plugin.RequestContext) (any, error) {
	s, err := clickhouseSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT concat(database, '.', table, ':', result_part_name) AS id,
       database, table, elapsed, progress, num_parts
FROM system.merges
ORDER BY elapsed DESC, database, table`, nil)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		id := fmt.Sprint(r["id"])
		r["ref"] = plugin.ResourceRef{Kind: "merge", Namespace: fmt.Sprint(r["database"]), Scope: fmt.Sprint(r["table"]), Name: id, UID: id}
	}
	return pageRows(rc, rows)
}

func mergeOverview(rc *plugin.RequestContext) (any, error) {
	return overviewByUID(rc, "id", listMerges)
}

func listProcesses(rc *plugin.RequestContext) (any, error) {
	s, err := clickhouseSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT query_id, user, address, elapsed, read_rows, memory_usage, query
FROM system.processes
ORDER BY elapsed DESC`, nil)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		id := fmt.Sprint(r["query_id"])
		r["ref"] = plugin.ResourceRef{Kind: "process", Name: id, UID: id}
	}
	return pageRows(rc, rows)
}

func processOverview(rc *plugin.RequestContext) (any, error) {
	return overviewByUID(rc, "id", listProcesses)
}

func listUsers(rc *plugin.RequestContext) (any, error) {
	s, err := clickhouseSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT name AS user, auth_type, storage
FROM system.users
ORDER BY name`, nil)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		user := fmt.Sprint(r["user"])
		r["ref"] = plugin.ResourceRef{Kind: "user", Name: user, UID: user}
	}
	return pageRows(rc, rows)
}

func userOverview(rc *plugin.RequestContext) (any, error) {
	user, err := sqldb.SafeIdentifier(rc.Param("user"))
	if err != nil {
		return nil, err
	}
	s, err := clickhouseSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT name AS user, auth_type, storage
FROM system.users
WHERE name = ?`, []any{user})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, plugin.ErrNotFound
	}
	return rows[0], nil
}

// columnNames returns a table's column names in order, for the data grid's
// free-text search across every column.
func columnNames(ctx context.Context, s *Session, database, table string) ([]string, error) {
	rows, err := queryRows(ctx, s, "SELECT name FROM system.columns WHERE database = ? AND table = ? ORDER BY position", []any{database, table})
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, fmt.Sprint(r["name"]))
	}
	return out, nil
}

func tableRows(rc *plugin.RequestContext) (any, error) {
	database, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	req, err := rc.Page()
	if err != nil {
		return nil, err
	}
	s, err := clickhouseSession(rc)
	if err != nil {
		return nil, err
	}
	limit := req.Limit
	if limit > s.opts.RowLimit {
		limit = s.opts.RowLimit
	}
	offset, err := cursorOffset(req.Cursor)
	if err != nil {
		return nil, err
	}
	filter := req.Search()
	var cols []string
	if filter != "" {
		cols, err = columnNames(rc.Ctx, s, database, table)
		if err != nil {
			return nil, err
		}
	}
	searchDialect := sqldb.Dialect{QuoteIdent: quoteIdent, Placeholder: sqldb.QuestionPlaceholder}
	searchClause, searchArgs := searchDialect.SearchClause("String", cols, filter, 1)
	where := ""
	if searchClause != "" {
		where = " WHERE " + searchClause
	}
	var total uint64
	if err := s.db.QueryRowContext(rc.Ctx, "SELECT count() FROM "+qualified(database, table)+where, searchArgs...).Scan(&total); err != nil {
		return nil, clickhouseErr(err)
	}
	orderBy := ""
	if len(req.Sort) > 0 {
		col, err := sqldb.SafeIdentifier(req.Sort[0].Field)
		if err != nil {
			return nil, err
		}
		dir := "ASC"
		if req.Sort[0].Desc {
			dir = "DESC"
		}
		orderBy = " ORDER BY " + quoteIdent(col) + " " + dir
	}
	dataArgs := append(append([]any{}, searchArgs...), limit, offset)
	rows, err := queryRows(rc.Ctx, s, fmt.Sprintf("SELECT * FROM %s%s%s LIMIT ? OFFSET ?", qualified(database, table), where, orderBy), dataArgs)
	if err != nil {
		return nil, err
	}
	key, err := sortingKeyColumns(rc.Ctx, s, database, table)
	if err != nil {
		return nil, err
	}
	attachRowKeys(rows, key, s.opts.RedactPatterns)
	redactRows(rows, s.opts.RedactPatterns)
	next := ""
	if uint64(offset+len(rows)) < total {
		next = strconv.Itoa(offset + len(rows))
	}
	totalInt := int(total)
	return plugin.Page[row]{Items: rows, NextCursor: next, Total: &totalInt}, nil
}

func tableColumnsRoute(rc *plugin.RequestContext) (any, error) {
	database, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	s, err := clickhouseSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT name, type, default_kind, default_expression, position, comment
FROM system.columns
WHERE database = ? AND table = ?
ORDER BY position`, []any{database, table})
	if err != nil {
		return nil, err
	}
	for i := range rows {
		rows[i]["database"] = database
		rows[i]["table"] = table
	}
	return pageRows(rc, rows)
}

func tableIndexes(rc *plugin.RequestContext) (any, error) {
	database, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	s, err := clickhouseSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT name, expr AS expression, type, granularity
FROM system.data_skipping_indices
WHERE database = ? AND table = ?
ORDER BY name`, []any{database, table})
	if err != nil {
		return nil, err
	}
	for i := range rows {
		rows[i]["database"] = database
		rows[i]["table"] = table
	}
	return pageRows(rc, rows)
}

func tableConstraints(rc *plugin.RequestContext) (any, error) {
	return pageRows(rc, []row{})
}

func tableDefinition(rc *plugin.RequestContext) (any, error) {
	database, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	s, err := clickhouseSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT database, name, engine, create_table_query AS definition
FROM system.tables
WHERE database = ? AND name = ?`, []any{database, table})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, plugin.ErrNotFound
	}
	return rows[0], nil
}

func completionRoute(rc *plugin.RequestContext) (any, error) {
	s, err := clickhouseSession(rc)
	if err != nil {
		return nil, err
	}
	items := []sqldb.CompletionItem{
		{Label: "SELECT", Type: "keyword"},
		{Label: "FROM", Type: "keyword"},
		{Label: "WHERE", Type: "keyword"},
		{Label: "GROUP BY", Type: "keyword"},
		{Label: "ORDER BY", Type: "keyword"},
		{Label: "LIMIT", Type: "keyword"},
		{Label: "INSERT INTO", Type: "keyword"},
		{Label: "ALTER TABLE", Type: "keyword"},
		{Label: "OPTIMIZE TABLE", Type: "keyword"},
		{Label: "SYSTEM", Type: "keyword"},
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT c.database, c.table, t.engine, c.name AS column
FROM system.columns c
JOIN system.tables t ON t.database = c.database AND t.name = c.table
WHERE c.database NOT IN ('INFORMATION_SCHEMA', 'information_schema', 'system')
ORDER BY c.database, c.table, c.position
LIMIT 2500`, nil)
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
		database := fmt.Sprint(r["database"])
		relation := fmt.Sprint(r["table"])
		kind := "table"
		if strings.Contains(strings.ToLower(fmt.Sprint(r["engine"])), "view") {
			kind = "view"
		}
		add(sqldb.CompletionItem{Label: database, Type: "namespace", Detail: "database"})
		add(sqldb.CompletionItem{Label: relation, Type: kind, Detail: database, Apply: quoteIdent(database) + "." + quoteIdent(relation)})
		column := fmt.Sprint(r["column"])
		if column != "" && column != "<nil>" {
			add(sqldb.CompletionItem{Label: column, Type: "property", Detail: database + "." + relation})
		}
	}
	return items, nil
}

func createDatabase(rc *plugin.RequestContext) (any, error) {
	s, err := clickhouseSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Name        string `json:"name" validate:"required"`
		IfNotExists bool   `json:"if_not_exists"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name, err := sqldb.SafeIdentifier(req.Name)
	if err != nil {
		return nil, err
	}
	prefix := "CREATE DATABASE "
	if req.IfNotExists {
		prefix += "IF NOT EXISTS "
	}
	if _, err := s.db.ExecContext(rc.Ctx, prefix+quoteIdent(name)); err != nil {
		return nil, clickhouseErr(err)
	}
	return actionResult{OK: true}, nil
}

func dropDatabase(rc *plugin.RequestContext) (any, error) {
	database, err := sqldb.SafeIdentifier(rc.Param("database"))
	if err != nil {
		return nil, err
	}
	return execDDL(rc, "DROP DATABASE IF EXISTS "+quoteIdent(database))
}

func createTable(rc *plugin.RequestContext) (any, error) {
	s, err := clickhouseSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	database, err := sqldb.SafeIdentifier(rc.Param("database"))
	if err != nil {
		return nil, err
	}
	var req struct {
		Name        string `json:"name" validate:"required"`
		Columns     any    `json:"columns" validate:"required"`
		Engine      string `json:"engine" validate:"required"`
		OrderBy     string `json:"order_by" validate:"required"`
		IfNotExists bool   `json:"if_not_exists"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	table, err := sqldb.SafeIdentifier(req.Name)
	if err != nil {
		return nil, err
	}
	columns, err := parseDDLColumns(req.Columns)
	if err != nil {
		return nil, err
	}
	engine := strings.TrimSpace(req.Engine)
	if engine == "" {
		engine = "MergeTree"
	}
	if !sqldb.SafeType(engine) {
		return nil, fmt.Errorf("%w: unsafe table engine", plugin.ErrInvalidInput)
	}
	orderBy := strings.TrimSpace(req.OrderBy)
	if orderBy == "" {
		orderBy = "tuple()"
	}
	if !sqldb.SafeDefault(orderBy) {
		return nil, fmt.Errorf("%w: unsafe ORDER BY expression", plugin.ErrInvalidInput)
	}
	prefix := "CREATE TABLE "
	if req.IfNotExists {
		prefix += "IF NOT EXISTS "
	}
	sqlText := prefix + qualified(database, table) + " (" + strings.Join(columns, ", ") + ") ENGINE = " + engine + " ORDER BY " + orderBy
	if _, err := s.db.ExecContext(rc.Ctx, sqlText); err != nil {
		return nil, clickhouseErr(err)
	}
	return actionResult{OK: true}, nil
}

func renameTable(rc *plugin.RequestContext) (any, error) {
	s, err := clickhouseSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	database, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		Database string `json:"database"`
		Name     string `json:"name" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	targetDB := database
	if strings.TrimSpace(req.Database) != "" {
		if targetDB, err = sqldb.SafeIdentifier(req.Database); err != nil {
			return nil, err
		}
	}
	target, err := sqldb.SafeIdentifier(req.Name)
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(rc.Ctx, "RENAME TABLE "+qualified(database, table)+" TO "+qualified(targetDB, target)); err != nil {
		return nil, clickhouseErr(err)
	}
	return actionResult{OK: true}, nil
}

func addColumn(rc *plugin.RequestContext) (any, error) {
	s, err := clickhouseSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	database, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		Name     string `json:"name" validate:"required"`
		Type     string `json:"type" validate:"required"`
		Nullable bool   `json:"nullable"`
		Default  string `json:"default"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	column, err := ddlColumn(sqldb.ColumnSpec{Name: req.Name, Type: req.Type, Nullable: req.Nullable, Default: req.Default})
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(rc.Ctx, "ALTER TABLE "+qualified(database, table)+" ADD COLUMN "+column); err != nil {
		return nil, clickhouseErr(err)
	}
	return actionResult{OK: true}, nil
}

func alterColumn(rc *plugin.RequestContext) (any, error) {
	s, err := clickhouseSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	database, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		Name     string `json:"name" validate:"required"`
		Type     string `json:"type" validate:"required"`
		Nullable bool   `json:"nullable"`
		Default  string `json:"default"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	column, err := ddlColumn(sqldb.ColumnSpec{Name: req.Name, Type: req.Type, Nullable: req.Nullable, Default: req.Default})
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(rc.Ctx, "ALTER TABLE "+qualified(database, table)+" MODIFY COLUMN "+column); err != nil {
		return nil, clickhouseErr(err)
	}
	return actionResult{OK: true}, nil
}

func dropColumn(rc *plugin.RequestContext) (any, error) {
	s, err := clickhouseSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	database, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	column, err := sqldb.SafeIdentifier(rc.Param("name"))
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(rc.Ctx, "ALTER TABLE "+qualified(database, table)+" DROP COLUMN "+quoteIdent(column)); err != nil {
		return nil, clickhouseErr(err)
	}
	return actionResult{OK: true}, nil
}

func createIndex(rc *plugin.RequestContext) (any, error) {
	s, err := clickhouseSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	database, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		Name        string `json:"name" validate:"required"`
		Expression  string `json:"expression" validate:"required"`
		Type        string `json:"type" validate:"required"`
		Granularity int    `json:"granularity"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	clause, err := buildAddIndex(req.Name, req.Expression, req.Type, req.Granularity)
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(rc.Ctx, "ALTER TABLE "+qualified(database, table)+" "+clause); err != nil {
		return nil, clickhouseErr(err)
	}
	return actionResult{OK: true}, nil
}

func dropIndex(rc *plugin.RequestContext) (any, error) {
	s, err := clickhouseSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	database, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	name, err := sqldb.SafeIdentifier(rc.Param("name"))
	if err != nil {
		return nil, err
	}
	// ClickHouse data-skipping indexes are dropped via ALTER TABLE ... DROP INDEX.
	if _, err := s.db.ExecContext(rc.Ctx, "ALTER TABLE "+qualified(database, table)+" DROP INDEX "+quoteIdent(name)); err != nil {
		return nil, clickhouseErr(err)
	}
	return actionResult{OK: true}, nil
}

func addConstraint(rc *plugin.RequestContext) (any, error) {
	s, err := clickhouseSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	database, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		Name       string `json:"name" validate:"required"`
		Expression string `json:"expression" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	clause, err := buildAddConstraint(req.Name, req.Expression)
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(rc.Ctx, "ALTER TABLE "+qualified(database, table)+" "+clause); err != nil {
		return nil, clickhouseErr(err)
	}
	return actionResult{OK: true}, nil
}

func dropConstraint(rc *plugin.RequestContext) (any, error) {
	s, err := clickhouseSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	database, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	name, err := sqldb.SafeIdentifier(rc.Param("name"))
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(rc.Ctx, "ALTER TABLE "+qualified(database, table)+" DROP CONSTRAINT "+quoteIdent(name)); err != nil {
		return nil, clickhouseErr(err)
	}
	return actionResult{OK: true}, nil
}

func truncateTable(rc *plugin.RequestContext) (any, error) {
	database, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	return execDDL(rc, "TRUNCATE TABLE "+qualified(database, table))
}

func dropTable(rc *plugin.RequestContext) (any, error) {
	database, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	return execDDL(rc, "DROP TABLE "+qualified(database, table))
}

func dropView(rc *plugin.RequestContext) (any, error) {
	database, err := sqldb.SafeIdentifier(rc.Param("database"))
	if err != nil {
		return nil, err
	}
	view, err := sqldb.SafeIdentifier(rc.Param("view"))
	if err != nil {
		return nil, err
	}
	return execDDL(rc, "DROP VIEW "+qualified(database, view))
}

func execDDL(rc *plugin.RequestContext, sqlText string) (any, error) {
	s, err := clickhouseSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(rc.Ctx, sqlText); err != nil {
		return nil, clickhouseErr(err)
	}
	return actionResult{OK: true}, nil
}

// killProcess terminates a running query identified by its query_id. The id comes
// from the process row and is a server-assigned string, but it is still embedded
// as an escaped string literal (KILL takes no bound parameters).
func killProcess(rc *plugin.RequestContext) (any, error) {
	id := strings.TrimSpace(rc.Param("id"))
	if id == "" {
		return nil, fmt.Errorf("%w: query id is required", plugin.ErrInvalidInput)
	}
	return execDDL(rc, buildKillQuery(id))
}

// killMutation cancels an in-flight mutation. ClickHouse identifies a mutation by
// (database, table, mutation_id); the database/table are strict identifiers and the
// mutation_id is an escaped string literal. The action supplies all three from the
// mutation's resource ref (Namespace=database, Scope=table, Name=mutation_id).
func killMutation(rc *plugin.RequestContext) (any, error) {
	database, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	stmt, err := buildKillMutation(database, table, rc.Param("id"))
	if err != nil {
		return nil, err
	}
	return execDDL(rc, stmt)
}

func stopMerges(rc *plugin.RequestContext) (any, error) {
	database, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	return execDDL(rc, "SYSTEM STOP MERGES "+qualified(database, table))
}

func startMerges(rc *plugin.RequestContext) (any, error) {
	database, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	return execDDL(rc, "SYSTEM START MERGES "+qualified(database, table))
}

func createUser(rc *plugin.RequestContext) (any, error) {
	s, err := clickhouseSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Name        string `json:"name" validate:"required"`
		AuthType    string `json:"auth_type"`
		Password    string `json:"password"`
		IfNotExists bool   `json:"if_not_exists"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	stmt, err := buildCreateUser(req.Name, req.AuthType, req.Password, req.IfNotExists)
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(rc.Ctx, stmt); err != nil {
		return nil, clickhouseErr(err)
	}
	return actionResult{OK: true, Message: "User created."}, nil
}

func dropUser(rc *plugin.RequestContext) (any, error) {
	user, err := sqldb.SafeIdentifier(rc.Param("user"))
	if err != nil {
		return nil, err
	}
	return execDDL(rc, "DROP USER IF EXISTS "+quoteIdent(user))
}

func grantUser(rc *plugin.RequestContext) (any, error) {
	s, err := clickhouseSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Privilege       string `json:"privilege" validate:"required"`
		On              string `json:"on" validate:"required"`
		WithGrantOption bool   `json:"with_grant_option"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	stmt, err := buildGrant(rc.Param("user"), req.Privilege, req.On, req.WithGrantOption)
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(rc.Ctx, stmt); err != nil {
		return nil, clickhouseErr(err)
	}
	return actionResult{OK: true, Message: "Privilege granted."}, nil
}

// quoteLiteral renders a value as a single-quoted ClickHouse string literal,
// escaping backslashes and single quotes so it can be embedded safely in
// statements (KILL/SYSTEM) that do not accept bound parameters.
func quoteLiteral(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}

func buildKillQuery(queryID string) string {
	return "KILL QUERY WHERE query_id = " + quoteLiteral(queryID)
}

func buildKillMutation(database, table, mutationID string) (string, error) {
	id := strings.TrimSpace(mutationID)
	if id == "" {
		return "", fmt.Errorf("%w: mutation id is required", plugin.ErrInvalidInput)
	}
	return "KILL MUTATION WHERE database = " + quoteLiteral(database) +
		" AND table = " + quoteLiteral(table) +
		" AND mutation_id = " + quoteLiteral(id), nil
}

// buildCreateUser renders CREATE USER with an IDENTIFIED WITH clause. The user
// name is a strict identifier; the auth type is constrained to a known set and the
// password is embedded as an escaped string literal (CREATE USER takes no bound
// parameters).
func buildCreateUser(name, authType, password string, ifNotExists bool) (string, error) {
	user, err := sqldb.SafeIdentifier(name)
	if err != nil {
		return "", err
	}
	auth := strings.TrimSpace(authType)
	if auth == "" {
		auth = "sha256_password"
	}
	prefix := "CREATE USER "
	if ifNotExists {
		prefix += "IF NOT EXISTS "
	}
	stmt := prefix + quoteIdent(user)
	switch auth {
	case "no_password":
		stmt += " IDENTIFIED WITH no_password"
	case "sha256_password", "double_sha1_password", "plaintext_password":
		if password == "" {
			return "", fmt.Errorf("%w: password is required for %s", plugin.ErrInvalidInput, auth)
		}
		stmt += " IDENTIFIED WITH " + auth + " BY " + quoteLiteral(password)
	default:
		return "", fmt.Errorf("%w: unsupported authentication type %q", plugin.ErrInvalidInput, auth)
	}
	return stmt, nil
}

// buildGrant renders GRANT <privilege> ON <target> TO <user>. The privilege and
// target are free-form SQL (access types and db.table scopes), so they are screened
// by the safe-expression guard; the user is a strict identifier.
func buildGrant(user, privilege, on string, withGrantOption bool) (string, error) {
	ident, err := sqldb.SafeIdentifier(user)
	if err != nil {
		return "", err
	}
	priv := strings.TrimSpace(privilege)
	if priv == "" || !sqldb.SafeDefault(priv) {
		return "", fmt.Errorf("%w: unsafe privilege", plugin.ErrInvalidInput)
	}
	target := strings.TrimSpace(on)
	if target == "" || !sqldb.SafeDefault(target) {
		return "", fmt.Errorf("%w: unsafe grant target", plugin.ErrInvalidInput)
	}
	stmt := "GRANT " + priv + " ON " + target + " TO " + quoteIdent(ident)
	if withGrantOption {
		stmt += " WITH GRANT OPTION"
	}
	return stmt, nil
}

// insertRow appends one row. INSERT is a normal, cheap operation in ClickHouse,
// so it only needs the read-only/confirm gates the other writes use.
func insertRow(rc *plugin.RequestContext) (any, error) {
	s, database, table, m, err := rowMutationInput(rc)
	if err != nil {
		return nil, err
	}
	stmt, args, err := dialect.Insert(qualified(database, table), m.Values)
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(rc.Ctx, stmt, args...); err != nil {
		return nil, clickhouseErr(err)
	}
	return actionResult{OK: true, Message: "Row inserted."}, nil
}

// updateRow rewrites the matched row(s) with an ALTER TABLE ... UPDATE mutation.
// Mutations are asynchronous and rewrite affected parts, so the result message
// says the change was scheduled rather than implying an immediate row count.
func updateRow(rc *plugin.RequestContext) (any, error) {
	s, database, table, m, err := rowMutationInput(rc)
	if err != nil {
		return nil, err
	}
	if err := validateMutationKey(rc, s, database, table, m.Key); err != nil {
		return nil, err
	}
	stmt, args, err := buildAlterUpdate(qualified(database, table), m.Key, m.Values)
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(rc.Ctx, stmt, args...); err != nil {
		return nil, clickhouseErr(err)
	}
	return actionResult{OK: true, Message: "Update mutation scheduled. ClickHouse applies it asynchronously; the row may not reflect the change immediately."}, nil
}

// deleteRow removes the matched row(s) with an ALTER TABLE ... DELETE mutation
// (heavy and asynchronous in ClickHouse).
func deleteRow(rc *plugin.RequestContext) (any, error) {
	s, database, table, m, err := rowMutationInput(rc)
	if err != nil {
		return nil, err
	}
	if err := validateMutationKey(rc, s, database, table, m.Key); err != nil {
		return nil, err
	}
	stmt, args, err := buildAlterDelete(qualified(database, table), m.Key)
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(rc.Ctx, stmt, args...); err != nil {
		return nil, clickhouseErr(err)
	}
	return actionResult{OK: true, Message: "Delete mutation scheduled. ClickHouse applies it asynchronously; the row may not disappear immediately."}, nil
}

// rowMutationInput resolves the session and target table and decodes the uniform
// mutation body, applying the read-only and destructive-confirmation gates that
// govern every ClickHouse write.
func rowMutationInput(rc *plugin.RequestContext) (*Session, string, string, sqldb.RowMutation, error) {
	s, err := clickhouseSession(rc)
	if err != nil {
		return nil, "", "", sqldb.RowMutation{}, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, "", "", sqldb.RowMutation{}, err
	}
	database, table, err := tableIdent(rc)
	if err != nil {
		return nil, "", "", sqldb.RowMutation{}, err
	}
	var m sqldb.RowMutation
	if err := rc.Bind(&m); err != nil {
		return nil, "", "", sqldb.RowMutation{}, err
	}
	return s, database, table, m, nil
}

// validateMutationKey rejects any client key that is not exactly the table's
// sorting key, so a mutation can only ever target one identified row's columns.
func validateMutationKey(rc *plugin.RequestContext, s *Session, database, table string, key map[string]any) error {
	cols, err := sortingKeyColumns(rc.Ctx, s, database, table)
	if err != nil {
		return err
	}
	return sqldb.ValidateRowKey(cols, key)
}

// buildAlterUpdate builds "ALTER TABLE t UPDATE col = ?, … WHERE keycol = ? AND …"
// with bound parameters. Column order is sorted so the statement is deterministic.
func buildAlterUpdate(table string, key, values map[string]any) (string, []any, error) {
	if len(values) == 0 {
		return "", nil, fmt.Errorf("%w: no values to update", plugin.ErrInvalidInput)
	}
	if len(key) == 0 {
		return "", nil, fmt.Errorf("%w: row key is required to update a row", plugin.ErrInvalidInput)
	}
	setCols := sortedKeys(values)
	set := make([]string, len(setCols))
	args := make([]any, 0, len(setCols)+len(key))
	for i, c := range setCols {
		col, err := sqldb.SafeIdentifier(c)
		if err != nil {
			return "", nil, err
		}
		set[i] = quoteIdent(col) + " = ?"
		args = append(args, values[c])
	}
	where, whereArgs, err := matchClause(key)
	if err != nil {
		return "", nil, err
	}
	args = append(args, whereArgs...)
	return "ALTER TABLE " + table + " UPDATE " + strings.Join(set, ", ") + " WHERE " + where, args, nil
}

// buildAlterDelete builds "ALTER TABLE t DELETE WHERE keycol = ? AND …". The key
// must be non-empty so an editing mistake can never wipe a whole table.
func buildAlterDelete(table string, key map[string]any) (string, []any, error) {
	if len(key) == 0 {
		return "", nil, fmt.Errorf("%w: row key is required to delete a row", plugin.ErrInvalidInput)
	}
	where, args, err := matchClause(key)
	if err != nil {
		return "", nil, err
	}
	return "ALTER TABLE " + table + " DELETE WHERE " + where, args, nil
}

// matchClause builds a "col = ? AND …" (or "col IS NULL") predicate over the key
// columns in stable sorted order, returning the bound arguments for non-NULL keys.
func matchClause(key map[string]any) (string, []any, error) {
	cols := sortedKeys(key)
	parts := make([]string, len(cols))
	args := make([]any, 0, len(cols))
	for i, c := range cols {
		col, err := sqldb.SafeIdentifier(c)
		if err != nil {
			return "", nil, err
		}
		if key[c] == nil {
			parts[i] = quoteIdent(col) + " IS NULL"
			continue
		}
		parts[i] = quoteIdent(col) + " = ?"
		args = append(args, key[c])
	}
	return strings.Join(parts, " AND "), args, nil
}

func sortedKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// sortingKeyColumns returns the table's ORDER BY (sorting) key columns in order.
// ClickHouse has no enforced primary key, so the sorting key is the closest
// stable row identity; tables without one (e.g. ORDER BY tuple()) stay read-only.
func sortingKeyColumns(ctx context.Context, s *Session, database, table string) ([]string, error) {
	rows, err := queryRows(ctx, s, `
SELECT name
FROM system.columns
WHERE database = ? AND table = ? AND is_in_sorting_key = 1
ORDER BY position`, []any{database, table})
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, fmt.Sprint(r["name"]))
	}
	return out, nil
}

// attachRowKeys tags each row with the sorting-key/value map the editable grid
// echoes back for UPDATE/DELETE. The grid stays read-only when the table has no
// sorting key, or when a key column is itself sensitive (so a redacted value is
// never shipped raw inside _key).
func attachRowKeys(rows []row, key, patterns []string) {
	if len(key) == 0 || sqldb.AnyColumnRedacted(key, patterns) {
		return
	}
	for _, r := range rows {
		k := map[string]any{}
		for _, col := range key {
			k[col] = r[col]
		}
		r["_key"] = k
	}
}

func cancelQuery(rc *plugin.RequestContext) (any, error) {
	s, err := clickhouseSession(rc)
	if err != nil {
		return nil, err
	}
	return actionResult{OK: s.cancelAll()}, nil
}

func queryStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := clickhouseSession(rc)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(client)
	enc := json.NewEncoder(client)
	for {
		var req sqldb.QueryRequest
		if err := dec.Decode(&req); err != nil {
			if client.Context().Err() != nil {
				return nil
			}
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err := enc.Encode(map[string]any{"error": "Invalid query request."}); err != nil {
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
				payload["confirmMessage"] = "This ClickHouse statement can change data, schema, privileges, or server state. Review it before running."
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
		res, err := s.db.ExecContext(ctx, statement)
		if err != nil {
			return sqldb.StatementResult{}, clickhouseErr(err)
		}
		affected, _ := res.RowsAffected()
		out := sqldb.StatementResult{Statement: statement, RowCount: affected, ElapsedMS: time.Since(start).Milliseconds()}
		out.CommandTag = sqldb.FirstKeyword(statement)
		if affected >= 0 {
			out.CommandTag += " " + strconv.FormatInt(affected, 10)
		}
		return out, nil
	}
	rows, err := s.db.QueryContext(ctx, statement)
	if err != nil {
		return sqldb.StatementResult{}, clickhouseErr(err)
	}
	columns, err := rows.Columns()
	if err != nil {
		_ = rows.Close()
		return sqldb.StatementResult{}, clickhouseErr(err)
	}
	out := sqldb.StatementResult{Statement: statement, Columns: columns}
	for rows.Next() {
		values, err := scanValues(rows, columns)
		if err != nil {
			_ = rows.Close()
			return sqldb.StatementResult{}, clickhouseErr(err)
		}
		out.Rows = append(out.Rows, values)
		if len(out.Rows) >= s.opts.RowLimit {
			break
		}
	}
	if err := rows.Close(); err != nil {
		return sqldb.StatementResult{}, clickhouseErr(err)
	}
	if err := rows.Err(); err != nil {
		return sqldb.StatementResult{}, clickhouseErr(err)
	}
	out.RowCount = int64(len(out.Rows))
	out.CommandTag = sqldb.FirstKeyword(statement)
	out.Rows = sqldb.RedactRows(out.Columns, out.Rows, s.opts.RedactPatterns)
	out.ElapsedMS = time.Since(start).Milliseconds()
	return out, nil
}

func statementReturnsRows(statement string) bool {
	switch sqldb.FirstKeyword(statement) {
	case "SELECT", "SHOW", "EXPLAIN", "WITH", "DESCRIBE", "DESC", "CHECK":
		return true
	default:
		return false
	}
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

func statementsRequireReview(statements []string) bool {
	for _, st := range statements {
		if isDestructiveStatement(st) {
			return true
		}
	}
	return false
}

func isReadOnlyStatement(statement string) bool {
	switch sqldb.FirstKeyword(statement) {
	case "SELECT", "SHOW", "EXPLAIN", "WITH", "DESCRIBE", "DESC", "CHECK":
		return true
	default:
		return false
	}
}

func isDestructiveStatement(statement string) bool {
	switch sqldb.FirstKeyword(statement) {
	case "INSERT", "ALTER", "DELETE", "DROP", "TRUNCATE", "CREATE", "GRANT", "REVOKE", "OPTIMIZE", "SYSTEM", "KILL", "ATTACH", "DETACH", "RENAME", "EXCHANGE", "BACKUP", "RESTORE", "UNDROP":
		return true
	default:
		return false
	}
}

func queryRows(ctx context.Context, s *Session, sqlText string, args []any) ([]row, error) {
	ctx, cancel := context.WithTimeout(ctx, s.opts.QueryTimeout)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return nil, clickhouseErr(err)
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		return nil, clickhouseErr(err)
	}
	out := []row{}
	for rows.Next() {
		values, err := scanValues(rows, columns)
		if err != nil {
			return nil, clickhouseErr(err)
		}
		r := row{}
		for i, name := range columns {
			if i < len(values) {
				r[name] = values[i]
			}
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, clickhouseErr(err)
	}
	return out, nil
}

func scanValues(rows *sql.Rows, columns []string) ([]any, error) {
	values := make([]any, len(columns))
	ptrs := make([]any, len(columns))
	for i := range values {
		ptrs[i] = &values[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, err
	}
	values = sqldb.DisplayValues(columns, values)
	return values, nil
}

func redactRows(rows []row, patterns []string) {
	for _, r := range rows {
		for key, value := range r {
			if value != nil && sqldb.RedactColumn(key, patterns) {
				r[key] = sqldb.RedactedValue
			}
		}
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
	start, err := cursorOffset(req.Cursor)
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
		if database := fmt.Sprint(r["database"]); database != "" && database != "<nil>" && kind != "database" {
			label = database + "." + label
		}
		nodes = append(nodes, plugin.TreeNode{
			Key:   kind + ":" + ref.UID,
			Label: label,
			Icon:  icon(iconName),
			Ref:   &ref,
			Leaf:  true,
		})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

func tableScopedOverview(rc *plugin.RequestContext, kind string, key string, load func(*plugin.RequestContext) (any, error)) (any, error) {
	database, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	res, err := load(rc)
	if err != nil {
		return nil, err
	}
	page, ok := res.(plugin.Page[row])
	if !ok {
		return nil, fmt.Errorf("%w: overview source returned invalid page", plugin.ErrUnavailable)
	}
	for _, item := range page.Items {
		if fmt.Sprint(item["database"]) == database && fmt.Sprint(item[key]) == table {
			return item, nil
		}
		if ref, ok := item["ref"].(plugin.ResourceRef); ok && ref.Kind == kind && ref.UID == database+"."+table {
			return item, nil
		}
	}
	return nil, plugin.ErrNotFound
}

func overviewByUID(rc *plugin.RequestContext, param string, load func(*plugin.RequestContext) (any, error)) (any, error) {
	id := strings.TrimSpace(rc.Param(param))
	if id == "" {
		return nil, fmt.Errorf("%w: id is required", plugin.ErrInvalidInput)
	}
	res, err := load(rc)
	if err != nil {
		return nil, err
	}
	page, ok := res.(plugin.Page[row])
	if !ok {
		return nil, fmt.Errorf("%w: overview source returned invalid page", plugin.ErrUnavailable)
	}
	for _, item := range page.Items {
		if ref, ok := item["ref"].(plugin.ResourceRef); ok && ref.UID == id {
			return item, nil
		}
	}
	return nil, plugin.ErrNotFound
}

func filterRows(rows []row, q string) []row {
	return plugin.FilterRows(rows, q)
}

func sortRows(rows []row, keys []plugin.SortKey) {
	if len(keys) == 0 {
		return
	}
	key := keys[0]
	sort.SliceStable(rows, func(i, j int) bool {
		a, b := fmt.Sprint(rows[i][key.Field]), fmt.Sprint(rows[j][key.Field])
		if key.Desc {
			return a > b
		}
		return a < b
	})
}

func cursorOffset(raw string) (int, error) {
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%w: cursor must be an offset", plugin.ErrInvalidInput)
	}
	return n, nil
}

func ensureWritable(s *Session) error {
	if s.opts.ReadOnly {
		return fmt.Errorf("%w: read-only mode blocks write operations", plugin.ErrForbidden)
	}
	return nil
}

func clickhouseErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return plugin.ErrNotFound
	}
	return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
}

func tableIdent(rc *plugin.RequestContext) (string, string, error) {
	database, err := sqldb.SafeIdentifier(rc.Param("database"))
	if err != nil {
		return "", "", err
	}
	table, err := sqldb.SafeIdentifier(rc.Param("table"))
	if err != nil {
		return "", "", err
	}
	return database, table, nil
}

func optionalTableFilter(rc *plugin.RequestContext) (string, string, error) {
	database, err := sqldb.OptionalIdentifier(firstNonEmpty(rc.Query().Get("p.database"), rc.Param("database")))
	if err != nil {
		return "", "", err
	}
	table, err := sqldb.OptionalIdentifier(firstNonEmpty(rc.Query().Get("p.table"), rc.Param("table")))
	if err != nil {
		return "", "", err
	}
	return database, table, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func qualified(database, name string) string {
	return quoteIdent(database) + "." + quoteIdent(name)
}

func quoteIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

func mutationUID(r row) string {
	return fmt.Sprintf("%s.%s:%s", r["database"], r["table"], r["mutation_id"])
}

func parseDDLColumns(value any) ([]string, error) {
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
		col, err := ddlColumn(spec)
		if err != nil {
			return nil, err
		}
		out = append(out, col)
	}
	return out, nil
}

func ddlColumn(spec sqldb.ColumnSpec) (string, error) {
	name, err := sqldb.SafeIdentifier(spec.Name)
	if err != nil {
		return "", err
	}
	dataType := strings.TrimSpace(spec.Type)
	if !sqldb.SafeType(dataType) {
		return "", fmt.Errorf("%w: unsafe column type", plugin.ErrInvalidInput)
	}
	if spec.Nullable && !strings.HasPrefix(strings.ToLower(dataType), "nullable(") {
		dataType = "Nullable(" + dataType + ")"
	}
	parts := []string{quoteIdent(name), dataType}
	if strings.TrimSpace(spec.Default) != "" {
		if !sqldb.SafeDefault(spec.Default) {
			return "", fmt.Errorf("%w: unsafe default expression", plugin.ErrInvalidInput)
		}
		parts = append(parts, "DEFAULT "+strings.TrimSpace(spec.Default))
	}
	return strings.Join(parts, " "), nil
}

// buildAddIndex builds an "ADD INDEX name expr TYPE type GRANULARITY n" clause for
// a ClickHouse data-skipping index. The expression and type are free-form SQL, so
// they are screened by the same safe-expression/safe-type guards the other DDL
// helpers use; the name is a strict identifier and granularity defaults to 1.
func buildAddIndex(name, expression, indexType string, granularity int) (string, error) {
	ident, err := sqldb.SafeIdentifier(name)
	if err != nil {
		return "", err
	}
	expr := strings.TrimSpace(expression)
	if expr == "" || !sqldb.SafeDefault(expr) {
		return "", fmt.Errorf("%w: unsafe index expression", plugin.ErrInvalidInput)
	}
	idxType := strings.TrimSpace(indexType)
	if idxType == "" || !sqldb.SafeType(idxType) {
		return "", fmt.Errorf("%w: unsafe index type", plugin.ErrInvalidInput)
	}
	if granularity <= 0 {
		granularity = 1
	}
	return fmt.Sprintf("ADD INDEX %s %s TYPE %s GRANULARITY %d", quoteIdent(ident), expr, idxType, granularity), nil
}

// buildAddConstraint builds an "ADD CONSTRAINT name CHECK (expr)" clause. The CHECK
// expression is free-form SQL screened by the safe-expression guard; the name is a
// strict identifier.
func buildAddConstraint(name, expression string) (string, error) {
	ident, err := sqldb.SafeIdentifier(name)
	if err != nil {
		return "", err
	}
	expr := strings.TrimSpace(expression)
	if expr == "" || !sqldb.SafeDefault(expr) {
		return "", fmt.Errorf("%w: unsafe constraint expression", plugin.ErrInvalidInput)
	}
	return "ADD CONSTRAINT " + quoteIdent(ident) + " CHECK " + expr, nil
}
