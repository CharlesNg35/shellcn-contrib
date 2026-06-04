package oracle

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
	OK bool `json:"ok"`
}

type confirmationError struct {
	message string
}

func (e confirmationError) Error() string { return e.message }

var dialect = sqldb.Dialect{QuoteIdent: quoteIdent, Placeholder: sqldb.ColonPlaceholder}

func routes() []plugin.Route {
	return []plugin.Route{
		{ID: "oracle.schemas.tree", Method: plugin.MethodGet, Path: "/tree/schemas", Permission: "oracle.schemas.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.schemas.tree", Handle: treeSchemas},
		{ID: "oracle.relations.tree", Method: plugin.MethodGet, Path: "/tree/relations", Permission: "oracle.tables.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.relations.tree", Handle: treeRelations},
		{ID: "oracle.schemas.list", Method: plugin.MethodGet, Path: "/schemas", Permission: "oracle.schemas.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.schemas.list", Handle: listSchemas},
		{ID: "oracle.schema.overview", Method: plugin.MethodGet, Path: "/schemas/{schema}/overview", Permission: "oracle.schemas.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.schema.overview", Handle: schemaOverview},
		{ID: "oracle.tables.tree", Method: plugin.MethodGet, Path: "/tree/tables", Permission: "oracle.tables.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.tables.tree", Handle: treeTables},
		{ID: "oracle.tables.list", Method: plugin.MethodGet, Path: "/tables", Permission: "oracle.tables.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.tables.list", Handle: listTables},
		{ID: "oracle.relations.graph", Method: plugin.MethodGet, Path: "/relations/graph", Permission: "oracle.tables.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.relations.graph", Handle: relationGraph},
		{ID: "oracle.views.tree", Method: plugin.MethodGet, Path: "/tree/views", Permission: "oracle.views.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.views.tree", Handle: treeViews},
		{ID: "oracle.views.list", Method: plugin.MethodGet, Path: "/views", Permission: "oracle.views.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.views.list", Handle: listViews},
		{ID: "oracle.view.drop", Method: plugin.MethodDelete, Path: "/views/{id}", Permission: "oracle.views.delete", Risk: plugin.RiskDestructive, AuditEvent: "oracle.view.drop", Handle: dropView},
		{ID: "oracle.procedures.tree", Method: plugin.MethodGet, Path: "/tree/procedures", Permission: "oracle.procedures.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.procedures.tree", Handle: treeProcedures},
		{ID: "oracle.procedures.list", Method: plugin.MethodGet, Path: "/procedures", Permission: "oracle.procedures.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.procedures.list", Handle: listProcedures},
		{ID: "oracle.packages.tree", Method: plugin.MethodGet, Path: "/tree/packages", Permission: "oracle.packages.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.packages.tree", Handle: treePackages},
		{ID: "oracle.packages.list", Method: plugin.MethodGet, Path: "/packages", Permission: "oracle.packages.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.packages.list", Handle: listPackages},
		{ID: "oracle.sequences.tree", Method: plugin.MethodGet, Path: "/tree/sequences", Permission: "oracle.sequences.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.sequences.tree", Handle: treeSequences},
		{ID: "oracle.sequences.list", Method: plugin.MethodGet, Path: "/sequences", Permission: "oracle.sequences.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.sequences.list", Handle: listSequences},
		{ID: "oracle.sequence.overview", Method: plugin.MethodGet, Path: "/objects/{id}/sequence", Permission: "oracle.sequences.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.sequence.overview", Handle: sequenceOverview},
		{ID: "oracle.users.tree", Method: plugin.MethodGet, Path: "/tree/users", Permission: "oracle.users.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.users.tree", Handle: treeUsers},
		{ID: "oracle.users.list", Method: plugin.MethodGet, Path: "/users", Permission: "oracle.users.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.users.list", Handle: listUsers},
		{ID: "oracle.user.overview", Method: plugin.MethodGet, Path: "/users/{user}/overview", Permission: "oracle.users.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.user.overview", Handle: userOverview},
		{ID: "oracle.tablespaces.tree", Method: plugin.MethodGet, Path: "/tree/tablespaces", Permission: "oracle.tablespaces.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.tablespaces.tree", Handle: treeTablespaces},
		{ID: "oracle.tablespaces.list", Method: plugin.MethodGet, Path: "/tablespaces", Permission: "oracle.tablespaces.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.tablespaces.list", Handle: listTablespaces},
		{ID: "oracle.tablespace.overview", Method: plugin.MethodGet, Path: "/tablespaces/{tablespace}/overview", Permission: "oracle.tablespaces.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.tablespace.overview", Handle: tablespaceOverview},
		{ID: "oracle.sessions.tree", Method: plugin.MethodGet, Path: "/tree/sessions", Permission: "oracle.sessions.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.sessions.tree", Handle: treeSessions},
		{ID: "oracle.sessions.list", Method: plugin.MethodGet, Path: "/sessions", Permission: "oracle.sessions.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.sessions.list", Handle: listSessions},
		{ID: "oracle.session.overview", Method: plugin.MethodGet, Path: "/sessions/{id}/overview", Permission: "oracle.sessions.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.session.overview", Handle: sessionOverview},
		{ID: "oracle.session.kill", Method: plugin.MethodPost, Path: "/sessions/{id}/kill", Permission: "oracle.sessions.kill", Risk: plugin.RiskDestructive, AuditEvent: "oracle.session.kill", Handle: killSession},
		{ID: "oracle.user.drop", Method: plugin.MethodDelete, Path: "/users/{user}", Permission: "oracle.users.delete", Risk: plugin.RiskDestructive, AuditEvent: "oracle.user.drop", Handle: dropUser},
		{ID: "oracle.user.lock", Method: plugin.MethodPost, Path: "/users/{user}/lock", Permission: "oracle.users.write", Risk: plugin.RiskWrite, AuditEvent: "oracle.user.lock", Handle: lockUser},
		{ID: "oracle.user.unlock", Method: plugin.MethodPost, Path: "/users/{user}/unlock", Permission: "oracle.users.write", Risk: plugin.RiskWrite, AuditEvent: "oracle.user.unlock", Handle: unlockUser},
		{ID: "oracle.user.grant", Method: plugin.MethodPost, Path: "/users/{user}/grant", Permission: "oracle.users.write", Risk: plugin.RiskPrivileged, AuditEvent: "oracle.user.grant", Input: userGrantSchema(), Handle: grantUser},
		{ID: "oracle.table.rows", Method: plugin.MethodGet, Path: "/objects/{id}/rows", Permission: "oracle.tables.data.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.table.rows", Handle: tableRows},
		{ID: "oracle.view.rows", Method: plugin.MethodGet, Path: "/objects/{id}/view-rows", Permission: "oracle.views.data.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.view.rows", Handle: tableRows},
		{ID: "oracle.table.columns", Method: plugin.MethodGet, Path: "/objects/{id}/columns", Permission: "oracle.tables.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.table.columns", Handle: tableColumnsRoute},
		{ID: "oracle.table.indexes", Method: plugin.MethodGet, Path: "/objects/{id}/indexes", Permission: "oracle.tables.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.table.indexes", Handle: tableIndexes},
		{ID: "oracle.table.constraints", Method: plugin.MethodGet, Path: "/objects/{id}/constraints", Permission: "oracle.tables.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.table.constraints", Handle: tableConstraints},
		{ID: "oracle.view.definition", Method: plugin.MethodGet, Path: "/objects/{id}/view-definition", Permission: "oracle.views.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.view.definition", Handle: viewDefinition},
		{ID: "oracle.object.definition", Method: plugin.MethodGet, Path: "/objects/{id}/definition", Permission: "oracle.objects.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.object.definition", Handle: sourceDefinition},
		{ID: "oracle.package.spec", Method: plugin.MethodGet, Path: "/objects/{id}/package-spec", Permission: "oracle.packages.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.package.spec", Handle: packageSpec},
		{ID: "oracle.package.body", Method: plugin.MethodGet, Path: "/objects/{id}/package-body", Permission: "oracle.packages.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.package.body", Handle: packageBody},
		{ID: "oracle.completion", Method: plugin.MethodGet, Path: "/completion", Permission: "oracle.schemas.read", Risk: plugin.RiskSafe, AuditEvent: "oracle.completion", Handle: completionRoute},
		{ID: "oracle.table.row.insert", Method: plugin.MethodPost, Path: "/objects/{id}/rows", Permission: "oracle.tables.data.write", Risk: plugin.RiskWrite, AuditEvent: "oracle.table.row.insert", Handle: insertRow},
		{ID: "oracle.table.row.update", Method: plugin.MethodPatch, Path: "/objects/{id}/rows", Permission: "oracle.tables.data.write", Risk: plugin.RiskWrite, AuditEvent: "oracle.table.row.update", Handle: updateRow},
		{ID: "oracle.table.row.delete", Method: plugin.MethodDelete, Path: "/objects/{id}/rows", Permission: "oracle.tables.data.delete", Risk: plugin.RiskDestructive, AuditEvent: "oracle.table.row.delete", Handle: deleteRow},
		{ID: "oracle.schema.create", Method: plugin.MethodPost, Path: "/schemas", Permission: "oracle.schemas.write", Risk: plugin.RiskPrivileged, AuditEvent: "oracle.schema.create", Input: schemaCreateSchema(), Handle: createSchema},
		{ID: "oracle.schema.drop", Method: plugin.MethodDelete, Path: "/schemas/{schema}", Permission: "oracle.schemas.delete", Risk: plugin.RiskDestructive, AuditEvent: "oracle.schema.drop", Handle: dropSchema},
		{ID: "oracle.table.create", Method: plugin.MethodPost, Path: "/schemas/{schema}/tables", Permission: "oracle.tables.write", Risk: plugin.RiskWrite, AuditEvent: "oracle.table.create", Input: tableCreateSchema(), Handle: createTable},
		{ID: "oracle.table.rename", Method: plugin.MethodPost, Path: "/objects/{id}/rename", Permission: "oracle.tables.write", Risk: plugin.RiskWrite, AuditEvent: "oracle.table.rename", Input: tableRenameSchema(), Handle: renameTable},
		{ID: "oracle.column.add", Method: plugin.MethodPost, Path: "/objects/{id}/columns", Permission: "oracle.tables.write", Risk: plugin.RiskWrite, AuditEvent: "oracle.column.add", Input: columnAddSchema(), Handle: addColumn},
		{ID: "oracle.column.alter", Method: plugin.MethodPost, Path: "/objects/{id}/columns/alter", Permission: "oracle.tables.write", Risk: plugin.RiskWrite, AuditEvent: "oracle.column.alter", Input: columnAlterSchema(), Handle: alterColumn},
		{ID: "oracle.column.rename", Method: plugin.MethodPost, Path: "/objects/{id}/columns/rename", Permission: "oracle.tables.write", Risk: plugin.RiskWrite, AuditEvent: "oracle.column.rename", Input: columnRenameSchema(), Handle: renameColumn},
		{ID: "oracle.column.drop", Method: plugin.MethodPost, Path: "/objects/{id}/columns/drop", Permission: "oracle.tables.write", Risk: plugin.RiskDestructive, AuditEvent: "oracle.column.drop", Handle: dropColumn},
		{ID: "oracle.index.create", Method: plugin.MethodPost, Path: "/objects/{id}/indexes", Permission: "oracle.tables.write", Risk: plugin.RiskWrite, AuditEvent: "oracle.index.create", Input: indexCreateSchema(), Handle: createIndex},
		{ID: "oracle.index.drop", Method: plugin.MethodPost, Path: "/objects/{id}/indexes/drop", Permission: "oracle.tables.write", Risk: plugin.RiskDestructive, AuditEvent: "oracle.index.drop", Handle: dropIndex},
		{ID: "oracle.constraint.add", Method: plugin.MethodPost, Path: "/objects/{id}/constraints", Permission: "oracle.tables.write", Risk: plugin.RiskWrite, AuditEvent: "oracle.constraint.add", Input: constraintAddSchema(), Handle: addConstraint},
		{ID: "oracle.constraint.drop", Method: plugin.MethodPost, Path: "/objects/{id}/constraints/drop", Permission: "oracle.tables.write", Risk: plugin.RiskDestructive, AuditEvent: "oracle.constraint.drop", Handle: dropConstraint},
		{ID: "oracle.table.truncate", Method: plugin.MethodPost, Path: "/objects/{id}/truncate", Permission: "oracle.tables.delete", Risk: plugin.RiskDestructive, AuditEvent: "oracle.table.truncate", Handle: truncateTable},
		{ID: "oracle.table.drop", Method: plugin.MethodDelete, Path: "/objects/{id}", Permission: "oracle.tables.delete", Risk: plugin.RiskDestructive, AuditEvent: "oracle.table.drop", Handle: dropTable},
		{ID: "oracle.query", Method: plugin.MethodWS, Path: "/query", Permission: "oracle.query.execute", Risk: plugin.RiskPrivileged, AuditEvent: "oracle.query", Stream: queryStream},
		{ID: "oracle.query.cancel", Method: plugin.MethodPost, Path: "/query/cancel", Permission: "oracle.query.cancel", Risk: plugin.RiskWrite, AuditEvent: "oracle.query.cancel", Handle: cancelQuery},
	}
}

func oracleSession(rc *plugin.RequestContext) (*Session, error) {
	return unwrap(rc.Session)
}

func tableCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Table", Fields: []plugin.Field{
		{Key: "name", Label: "Table name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		sqldb.ColumnsArrayField(sqldb.ColumnsField{TypePlaceholder: "VARCHAR2(255)", TypeSuggestions: []string{"NUMBER", "NUMBER(10,2)", "VARCHAR2(255)", "CHAR(1)", "NVARCHAR2(255)", "CLOB", "BLOB", "DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE", "RAW(16)"}, Default: true, Primary: true, Unique: true}),
	}}}}
}

func columnAddSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Column", Fields: []plugin.Field{
		{Key: "name", Label: "Column name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		{Key: "type", Label: "Type", Type: plugin.FieldText, Required: true, Default: "VARCHAR2(255)"},
		{Key: "nullable", Label: "Nullable", Type: plugin.FieldToggle, Default: true},
		{Key: "default", Label: "Default expression", Type: plugin.FieldText},
	}}}}
}

func indexCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Index", Fields: []plugin.Field{
		{Key: "name", Label: "Index name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		{Key: "columns", Label: "Columns", Type: plugin.FieldMultiSelect, Required: true, OptionsSource: &plugin.DataSource{RouteID: "oracle.table.columns", Params: objectParams()}},
		{Key: "unique", Label: "Unique", Type: plugin.FieldToggle},
	}}}}
}

// In Oracle a schema is a user, so creating one is CREATE USER plus a minimal
// quota/grant so the new owner can actually create objects.
func schemaCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Schema", Fields: []plugin.Field{
		{Key: "name", Label: "Schema (user) name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		{Key: "password", Label: "Password", Type: plugin.FieldPassword, Required: true, Secret: true},
	}}}}
}

// userGrantSchema collects one or more roles/system privileges to GRANT to a
// user (e.g. CONNECT, RESOURCE, CREATE SESSION). Object privileges are out of
// scope here — those are managed from the owning object.
func userGrantSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Grant", Fields: []plugin.Field{
		{Key: "privileges", Label: "Roles / privileges", Type: plugin.FieldMultiSelect, Required: true, Options: []plugin.Option{
			{Label: "CONNECT", Value: "CONNECT"},
			{Label: "RESOURCE", Value: "RESOURCE"},
			{Label: "DBA", Value: "DBA"},
			{Label: "CREATE SESSION", Value: "CREATE SESSION"},
			{Label: "CREATE TABLE", Value: "CREATE TABLE"},
			{Label: "CREATE VIEW", Value: "CREATE VIEW"},
			{Label: "CREATE PROCEDURE", Value: "CREATE PROCEDURE"},
			{Label: "CREATE SEQUENCE", Value: "CREATE SEQUENCE"},
			{Label: "UNLIMITED TABLESPACE", Value: "UNLIMITED TABLESPACE"},
		}, Help: "Each entry is a role or system privilege granted to the user."},
	}}}}
}

func tableRenameSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Rename", Fields: []plugin.Field{
		{Key: "name", Label: "New table name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
	}}}}
}

func columnAlterSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Column", Fields: []plugin.Field{
		{Key: "type", Label: "Type", Type: plugin.FieldText, Required: true, Default: "VARCHAR2(255)"},
		{Key: "nullable", Label: "Nullable", Type: plugin.FieldToggle, Default: true},
		{Key: "default", Label: "Default expression", Type: plugin.FieldText},
	}}}}
}

func columnRenameSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Rename", Fields: []plugin.Field{
		{Key: "to", Label: "New column name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
	}}}}
}

func constraintAddSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Constraint", Fields: []plugin.Field{
		{Key: "name", Label: "Constraint name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		{Key: "type", Label: "Type", Type: plugin.FieldSelect, Required: true, Default: "PRIMARY KEY", Options: []plugin.Option{
			{Label: "Primary key", Value: "PRIMARY KEY"},
			{Label: "Unique", Value: "UNIQUE"},
			{Label: "Check", Value: "CHECK"},
			{Label: "Foreign key", Value: "FOREIGN KEY"},
		}},
		{Key: "columns", Label: "Columns", Type: plugin.FieldMultiSelect, OptionsSource: &plugin.DataSource{RouteID: "oracle.table.columns", Params: objectParams()}, Help: "Required for primary key, unique, and foreign key."},
		{Key: "check", Label: "Check expression", Type: plugin.FieldText, Help: `e.g. AGE >= 0. Required for a CHECK constraint.`},
		{Key: "ref_table", Label: "Referenced table", Type: plugin.FieldText, Help: "Foreign key only: target table (optionally OWNER.TABLE)."},
		{Key: "ref_columns", Label: "Referenced columns", Type: plugin.FieldText, Help: "Foreign key only: comma-separated columns in the referenced table."},
	}}}}
}

// treeSchemas lists schemas as expandable branches that drill into their
// tables/views (hierarchical, TablePlus-style).
func treeSchemas(rc *plugin.RequestContext) (any, error) {
	res, err := listSchemas(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, r := range page.Items {
		name := fmt.Sprint(r["name"])
		nodes = append(nodes, plugin.TreeNode{
			Key:            "schema:" + name,
			Label:          name,
			Icon:           icon("folder-tree"),
			Ref:            &plugin.ResourceRef{Kind: "schema", Name: name, UID: name},
			ChildrenSource: &plugin.DataSource{RouteID: "oracle.relations.tree", Params: map[string]string{"schema": name}},
		})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

// treeRelations lists a schema's tables and views as leaves (scoped by p.schema).
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

func treeTables(rc *plugin.RequestContext) (any, error) {
	return treeFromPage(rc, "table", "table-2", "name", listTables)
}

func treeViews(rc *plugin.RequestContext) (any, error) {
	return treeFromPage(rc, "view", "panel-top", "name", listViews)
}

func treeProcedures(rc *plugin.RequestContext) (any, error) {
	return treeFromPage(rc, "procedure", "function-square", "name", listProcedures)
}

func treePackages(rc *plugin.RequestContext) (any, error) {
	return treeFromPage(rc, "package", "package", "name", listPackages)
}

func treeSequences(rc *plugin.RequestContext) (any, error) {
	return treeFromPage(rc, "sequence", "list-ordered", "name", listSequences)
}

func treeUsers(rc *plugin.RequestContext) (any, error) {
	return treeFromPage(rc, "user", "user", "name", listUsers)
}

func treeTablespaces(rc *plugin.RequestContext) (any, error) {
	return treeFromPage(rc, "tablespace", "hard-drive", "name", listTablespaces)
}

func treeSessions(rc *plugin.RequestContext) (any, error) {
	return treeFromPage(rc, "session", "activity", "name", listSessions)
}

func listSchemas(rc *plugin.RequestContext) (any, error) {
	rows, err := userCatalogRows(rc)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		name := fmt.Sprint(firstValue(r, "NAME", "name"))
		normalizeRowKeys(r)
		r["ref"] = plugin.ResourceRef{Kind: "schema", Name: name, UID: name}
	}
	return pageRows(rc, rows)
}

func userCatalogRows(rc *plugin.RequestContext) ([]row, error) {
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT username AS name, account_status, default_tablespace, temporary_tablespace, created
FROM dba_users
ORDER BY username`, nil)
	if err != nil && isPrivilegeError(err) {
		rows, err = queryRows(rc.Ctx, s, `
SELECT username AS name, NULL AS account_status, NULL AS default_tablespace, NULL AS temporary_tablespace, created
FROM all_users
ORDER BY username`, nil)
	}
	return rows, err
}

func schemaOverview(rc *plugin.RequestContext) (any, error) {
	schema, err := safeIdent(rc.Param("schema"))
	if err != nil {
		return nil, err
	}
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT owner AS schema_name,
       SUM(CASE WHEN object_type = 'TABLE' THEN 1 ELSE 0 END) AS tables,
       SUM(CASE WHEN object_type = 'VIEW' THEN 1 ELSE 0 END) AS views,
       SUM(CASE WHEN object_type IN ('PROCEDURE','FUNCTION') THEN 1 ELSE 0 END) AS procedures,
       SUM(CASE WHEN object_type = 'PACKAGE' THEN 1 ELSE 0 END) AS packages
FROM all_objects
WHERE owner = :1
GROUP BY owner`, []any{schema})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return row{"schema_name": schema}, nil
	}
	normalizeRowKeys(rows[0])
	return rows[0], nil
}

func listTables(rc *plugin.RequestContext) (any, error) {
	return relationList(rc, "TABLE", "table")
}

func relationGraph(rc *plugin.RequestContext) (any, error) {
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	schema, err := optionalIdent(paramOrQuery(rc, "schema"))
	if err != nil {
		return nil, err
	}
	filter, args := "", []any{}
	if schema != "" {
		filter = " AND ac.owner = :1"
		args = append(args, schema)
	}
	colFilter, colArgs := "", []any{}
	if schema != "" {
		colFilter = " AND owner = :1"
		colArgs = append(colArgs, schema)
	}
	colRows, err := queryRows(rc.Ctx, s, `
SELECT owner AS "table_schema", table_name AS "table_name", column_name AS "column_name", data_type AS "data_type"
FROM all_tab_columns
WHERE 1 = 1`+colFilter+`
ORDER BY owner, table_name, column_id`, colArgs)
	if err != nil {
		return nil, err
	}
	fkRows, err := queryRows(rc.Ctx, s, `
SELECT ac.constraint_name AS "constraint_name",
       ac.owner AS "child_schema", ac.table_name AS "child_table", acc.column_name AS "child_column",
       rc.owner AS "parent_schema", rc.table_name AS "parent_table", rcc.column_name AS "parent_column"
FROM all_constraints ac
JOIN all_cons_columns acc ON acc.owner = ac.owner AND acc.constraint_name = ac.constraint_name
JOIN all_constraints rc ON rc.owner = ac.r_owner AND rc.constraint_name = ac.r_constraint_name
JOIN all_cons_columns rcc ON rcc.owner = rc.owner AND rcc.constraint_name = rc.constraint_name AND rcc.position = acc.position
WHERE ac.constraint_type = 'R'`+filter+`
ORDER BY ac.constraint_name, acc.position`, args)
	if err != nil {
		return nil, err
	}
	columns := make([]sqldb.TableColumn, 0, len(colRows))
	for _, r := range colRows {
		columns = append(columns, sqldb.TableColumnFromRow(r))
	}
	fks := make([]sqldb.ForeignKey, 0, len(fkRows))
	for _, r := range fkRows {
		fks = append(fks, sqldb.ForeignKeyFromRow(r))
	}
	return sqldb.RelationGraph(columns, fks), nil
}

func listViews(rc *plugin.RequestContext) (any, error) {
	return relationList(rc, "VIEW", "view")
}

func relationList(rc *plugin.RequestContext, objectType string, refKind string) (any, error) {
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	schema, err := optionalIdent(paramOrQuery(rc, "schema"))
	if err != nil {
		return nil, err
	}
	filter := ""
	args := []any{objectType}
	if schema != "" {
		filter = " AND o.owner = :2"
		args = append(args, schema)
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT o.object_name AS name, o.owner, t.tablespace_name AS tablespace, t.num_rows AS row_count, t.blocks,
       o.created, t.last_analyzed
FROM all_objects o
LEFT JOIN all_tables t ON t.owner = o.owner AND t.table_name = o.object_name
WHERE o.object_type = :1`+filter+`
ORDER BY o.owner, o.object_name`, args)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		normalizeRowKeys(r)
		r["rows"] = r["row_count"]
		delete(r, "row_count")
		owner, name := fmt.Sprint(r["owner"]), fmt.Sprint(r["name"])
		r["ref"] = plugin.ResourceRef{Kind: refKind, Namespace: owner, Name: qualified(owner, name), UID: objectID(owner, name)}
	}
	return pageRows(rc, rows)
}

func listProcedures(rc *plugin.RequestContext) (any, error) {
	return objectList(rc, []string{"PROCEDURE", "FUNCTION"}, "procedure")
}

func listPackages(rc *plugin.RequestContext) (any, error) {
	return objectList(rc, []string{"PACKAGE"}, "package")
}

func objectList(rc *plugin.RequestContext, types []string, refKind string) (any, error) {
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	schema, err := optionalIdent(paramOrQuery(rc, "schema"))
	if err != nil {
		return nil, err
	}
	placeholders := make([]string, 0, len(types))
	args := make([]any, 0, len(types)+1)
	for i, typ := range types {
		placeholders = append(placeholders, ":"+strconv.Itoa(i+1))
		args = append(args, typ)
	}
	filter := ""
	if schema != "" {
		filter = " AND owner = :" + strconv.Itoa(len(args)+1)
		args = append(args, schema)
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT object_name AS name, owner, object_type AS type, status, created, last_ddl_time AS modified
FROM all_objects
WHERE object_type IN (`+strings.Join(placeholders, ",")+`)`+filter+`
ORDER BY owner, object_name`, args)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		normalizeRowKeys(r)
		owner, name := fmt.Sprint(r["owner"]), fmt.Sprint(r["name"])
		r["ref"] = plugin.ResourceRef{Kind: refKind, Namespace: owner, Name: qualified(owner, name), UID: objectID(owner, name)}
	}
	return pageRows(rc, rows)
}

func listSequences(rc *plugin.RequestContext) (any, error) {
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	schema, err := optionalIdent(paramOrQuery(rc, "schema"))
	if err != nil {
		return nil, err
	}
	filter := ""
	args := []any{}
	if schema != "" {
		filter = " WHERE sequence_owner = :1"
		args = append(args, schema)
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT sequence_name AS name, sequence_owner AS owner, min_value, max_value, increment_by, last_number, cache_size
FROM all_sequences`+filter+`
ORDER BY sequence_owner, sequence_name`, args)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		normalizeRowKeys(r)
		owner, name := fmt.Sprint(r["owner"]), fmt.Sprint(r["name"])
		r["ref"] = plugin.ResourceRef{Kind: "sequence", Namespace: owner, Name: qualified(owner, name), UID: objectID(owner, name)}
	}
	return pageRows(rc, rows)
}

func sequenceOverview(rc *plugin.RequestContext) (any, error) {
	owner, name, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT sequence_name AS name, sequence_owner AS owner, min_value, max_value, increment_by, cycle_flag,
       order_flag, cache_size, last_number
FROM all_sequences
WHERE sequence_owner = :1 AND sequence_name = :2`, []any{owner, name})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, plugin.ErrNotFound
	}
	normalizeRowKeys(rows[0])
	return rows[0], nil
}

func listUsers(rc *plugin.RequestContext) (any, error) {
	rows, err := userCatalogRows(rc)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		name := fmt.Sprint(firstValue(r, "NAME", "name"))
		normalizeRowKeys(r)
		r["ref"] = plugin.ResourceRef{Kind: "user", Name: name, UID: name}
	}
	return pageRows(rc, rows)
}

func userOverview(rc *plugin.RequestContext) (any, error) {
	user, err := safeIdent(rc.Param("user"))
	if err != nil {
		return nil, err
	}
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT username AS name, account_status, default_tablespace, temporary_tablespace, created, expiry_date, lock_date
FROM dba_users
WHERE username = :1`, []any{user})
	if err != nil && isPrivilegeError(err) {
		rows, err = queryRows(rc.Ctx, s, `
SELECT username AS name, NULL AS account_status, NULL AS default_tablespace, NULL AS temporary_tablespace, created,
       NULL AS expiry_date, NULL AS lock_date
FROM all_users
WHERE username = :1`, []any{user})
	}
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, plugin.ErrNotFound
	}
	normalizeRowKeys(rows[0])
	return rows[0], nil
}

func listTablespaces(rc *plugin.RequestContext) (any, error) {
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT tablespace_name AS name, status, contents, extent_management,
       CASE WHEN bigfile = 'YES' THEN 1 ELSE 0 END AS bigfile
FROM dba_tablespaces
ORDER BY tablespace_name`, nil)
	if err != nil && isPrivilegeError(err) {
		rows, err = queryRows(rc.Ctx, s, `
SELECT tablespace_name AS name, status, contents, extent_management,
       CASE WHEN bigfile = 'YES' THEN 1 ELSE 0 END AS bigfile
FROM user_tablespaces
ORDER BY tablespace_name`, nil)
	}
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		normalizeRowKeys(r)
		name := fmt.Sprint(r["name"])
		r["bigfile"] = boolish(r["bigfile"])
		r["ref"] = plugin.ResourceRef{Kind: "tablespace", Name: name, UID: name}
	}
	return pageRows(rc, rows)
}

func tablespaceOverview(rc *plugin.RequestContext) (any, error) {
	tablespace, err := safeIdent(rc.Param("tablespace"))
	if err != nil {
		return nil, err
	}
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT tablespace_name AS name, SUM(bytes) AS bytes, SUM(maxbytes) AS max_bytes, COUNT(*) AS datafiles
FROM dba_data_files
WHERE tablespace_name = :1
GROUP BY tablespace_name`, []any{tablespace})
	if err != nil && isPrivilegeError(err) {
		return row{"name": tablespace, "note": "Datafile details require DBA catalog privileges."}, nil
	}
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return row{"name": tablespace}, nil
	}
	normalizeRowKeys(rows[0])
	return rows[0], nil
}

func listSessions(rc *plugin.RequestContext) (any, error) {
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT sid, serial# AS serial, username, status, machine, program, logon_time
FROM v$session
WHERE username IS NOT NULL
ORDER BY username, sid`, nil)
	if err != nil && isPrivilegeError(err) {
		rows, err = queryRows(rc.Ctx, s, `
SELECT SYS_CONTEXT('USERENV','SID') AS sid, NULL AS serial, USER AS username,
       'CURRENT' AS status, SYS_CONTEXT('USERENV','HOST') AS machine,
       SYS_CONTEXT('USERENV','MODULE') AS program, SYSDATE AS logon_time
FROM dual`, nil)
	}
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		normalizeRowKeys(r)
		id := fmt.Sprint(r["sid"]) + ":" + fmt.Sprint(r["serial"])
		r["name"] = id
		r["ref"] = plugin.ResourceRef{Kind: "session", Name: id, UID: id}
	}
	return pageRows(rc, rows)
}

func sessionOverview(rc *plugin.RequestContext) (any, error) {
	id := strings.TrimSpace(rc.Param("id"))
	sid, serial, _ := strings.Cut(id, ":")
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT sid, serial# AS serial, username, status, machine, program, module, action, client_identifier, logon_time
FROM v$session
WHERE sid = :1 AND (:2 IS NULL OR serial# = :2)`, []any{sid, nullIfEmpty(serial)})
	if err != nil && isPrivilegeError(err) {
		return row{"sid": sid, "serial": serial, "note": "Session details require V$SESSION privileges."}, nil
	}
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, plugin.ErrNotFound
	}
	normalizeRowKeys(rows[0])
	return rows[0], nil
}

// killSession terminates a session by SID,SERIAL#. The id carried by a session
// resource ref is "sid:serial"; both are validated as integers and rebuilt into
// the quoted-literal 'sid,serial' Oracle expects — never string-interpolated as
// identifiers.
func killSession(rc *plugin.RequestContext) (any, error) {
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	sid, serial, err := sessionSidSerial(rc.Param("id"))
	if err != nil {
		return nil, err
	}
	stmt := "ALTER SYSTEM KILL SESSION '" + sid + "," + serial + "' IMMEDIATE"
	if _, err := s.db.ExecContext(rc.Ctx, stmt); err != nil {
		return nil, oracleErr(err)
	}
	return actionResult{OK: true}, nil
}

// sessionSidSerial splits and validates a "sid:serial" session id into the two
// integer components used to build a KILL SESSION target.
func sessionSidSerial(id string) (string, string, error) {
	sidRaw, serialRaw, ok := strings.Cut(strings.TrimSpace(id), ":")
	if !ok {
		return "", "", fmt.Errorf("%w: session id must be sid:serial", plugin.ErrInvalidInput)
	}
	sid, err := numericToken(sidRaw)
	if err != nil {
		return "", "", err
	}
	serial, err := numericToken(serialRaw)
	if err != nil {
		return "", "", err
	}
	return sid, serial, nil
}

// numericToken accepts only an unsigned integer, guarding the KILL SESSION
// literal against any injection through the sid/serial values.
func numericToken(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("%w: a numeric value is required", plugin.ErrInvalidInput)
	}
	if _, err := strconv.ParseUint(raw, 10, 64); err != nil {
		return "", fmt.Errorf("%w: value must be a non-negative integer", plugin.ErrInvalidInput)
	}
	return raw, nil
}

// dropUser drops an Oracle user (schema) and everything it owns.
func dropUser(rc *plugin.RequestContext) (any, error) {
	name, err := safeIdent(rc.Param("user"))
	if err != nil {
		return nil, err
	}
	return execDDL(rc, "DROP USER "+quoteIdent(name)+" CASCADE")
}

func lockUser(rc *plugin.RequestContext) (any, error) {
	name, err := safeIdent(rc.Param("user"))
	if err != nil {
		return nil, err
	}
	return execDDL(rc, "ALTER USER "+quoteIdent(name)+" ACCOUNT LOCK")
}

func unlockUser(rc *plugin.RequestContext) (any, error) {
	name, err := safeIdent(rc.Param("user"))
	if err != nil {
		return nil, err
	}
	return execDDL(rc, "ALTER USER "+quoteIdent(name)+" ACCOUNT UNLOCK")
}

func grantUser(rc *plugin.RequestContext) (any, error) {
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	name, err := safeIdent(rc.Param("user"))
	if err != nil {
		return nil, err
	}
	var req struct {
		Privileges any `json:"privileges" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	privs, err := grantPrivileges(req.Privileges)
	if err != nil {
		return nil, err
	}
	stmt := "GRANT " + strings.Join(privs, ", ") + " TO " + quoteIdent(name)
	if _, err := s.db.ExecContext(rc.Ctx, stmt); err != nil {
		return nil, oracleErr(err)
	}
	return actionResult{OK: true}, nil
}

// grantPrivileges validates a list of roles/system privileges. Each is a SQL
// keyword (possibly multi-word, e.g. CREATE SESSION), not a quoted identifier,
// so it must contain only letters, digits, underscores, and single spaces.
func grantPrivileges(value any) ([]string, error) {
	var raw []string
	switch t := value.(type) {
	case string:
		raw = strings.Split(t, ",")
	case []string:
		raw = t
	case []any:
		for _, item := range t {
			raw = append(raw, fmt.Sprint(item))
		}
	default:
		return nil, fmt.Errorf("%w: privileges must be a list", plugin.ErrInvalidInput)
	}
	out := make([]string, 0, len(raw))
	for _, p := range raw {
		priv, err := safePrivilege(p)
		if err != nil {
			return nil, err
		}
		if priv != "" {
			out = append(out, priv)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: at least one privilege is required", plugin.ErrInvalidInput)
	}
	return out, nil
}

func safePrivilege(raw string) (string, error) {
	priv := strings.ToUpper(strings.Join(strings.Fields(raw), " "))
	if priv == "" {
		return "", nil
	}
	for _, r := range priv {
		if (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' && r != ' ' {
			return "", fmt.Errorf("%w: privilege contains unsupported characters", plugin.ErrInvalidInput)
		}
	}
	return priv, nil
}

func tableRows(rc *plugin.RequestContext) (any, error) {
	owner, name, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	req, err := rc.Page()
	if err != nil {
		return nil, err
	}
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	limit := req.Limit
	if limit > s.opts.RowLimit {
		limit = s.opts.RowLimit
	}
	offset, err := offsetCursor(req.Cursor)
	if err != nil {
		return nil, err
	}
	filter := req.Search()
	var cols []string
	if filter != "" {
		cols, err = columnNames(rc.Ctx, s, owner, name)
		if err != nil {
			return nil, err
		}
	}
	countClause, countArgs := oracleSearchClause(cols, filter, 1)
	countWhere := ""
	if countClause != "" {
		countWhere = " WHERE " + countClause
	}
	var total int
	if err := s.db.QueryRowContext(rc.Ctx, "SELECT COUNT(*) FROM "+qualified(owner, name)+countWhere, countArgs...).Scan(&total); err != nil {
		return nil, oracleErr(err)
	}
	orderBy := " ORDER BY 1"
	if len(req.Sort) > 0 {
		col, err := safeIdent(req.Sort[0].Field)
		if err != nil {
			return nil, err
		}
		dir := "ASC"
		if req.Sort[0].Desc {
			dir = "DESC"
		}
		orderBy = " ORDER BY " + quoteIdent(col) + " " + dir
	}
	// godror binds :N by order of appearance, so the WHERE clause (which precedes
	// OFFSET in the text) must take the first placeholders and OFFSET/FETCH follow.
	dataClause, dataSearch := oracleSearchClause(cols, filter, 1)
	dataWhere := ""
	if dataClause != "" {
		dataWhere = " WHERE " + dataClause
	}
	offsetPh := dialect.Placeholder(len(dataSearch) + 1)
	fetchPh := dialect.Placeholder(len(dataSearch) + 2)
	dataArgs := append(append([]any{}, dataSearch...), offset, limit)
	rows, err := queryRows(rc.Ctx, s, fmt.Sprintf("SELECT * FROM %s%s%s OFFSET %s ROWS FETCH NEXT %s ROWS ONLY", qualified(owner, name), dataWhere, orderBy, offsetPh, fetchPh), dataArgs)
	if err != nil {
		return nil, err
	}
	// Keep Oracle's real (uppercase) column names here so the editable grid's
	// quoted UPDATE/DELETE identifiers match; the primary key uses the same case.
	pk, err := primaryKeyColumns(rc.Ctx, s, owner, name)
	if err != nil {
		return nil, err
	}
	attachRowKeys(rows, pk, s.opts.RedactPatterns)
	fks, err := foreignKeys(rc.Ctx, s, owner, name)
	if err != nil {
		return nil, err
	}
	attachForeignKeys(rows, fks)
	redactRows(rows, s.opts.RedactPatterns)
	next := ""
	if offset+len(rows) < total {
		next = strconv.Itoa(offset + len(rows))
	}
	return plugin.Page[row]{Items: rows, NextCursor: next, Total: &total}, nil
}

// foreignKeys maps each FK column (real uppercase name, matching the grid's
// columns) to the referenced table's ref, attached under the generic "_links"
// field the grid renders as links.
func foreignKeys(ctx context.Context, s *Session, owner, table string) (map[string]plugin.ResourceRef, error) {
	rows, err := queryRows(ctx, s, `
SELECT cc.column_name AS col, rc.owner AS ref_owner, rc.table_name AS ref_table
FROM all_constraints c
JOIN all_cons_columns cc ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name
JOIN all_constraints rc ON rc.owner = c.r_owner AND rc.constraint_name = c.r_constraint_name
WHERE c.constraint_type = 'R' AND c.owner = :1 AND c.table_name = :2`, []any{owner, table})
	if err != nil {
		return nil, err
	}
	out := map[string]plugin.ResourceRef{}
	for _, r := range rows {
		normalizeRowKeys(r)
		col, refOwner, refTable := fmt.Sprint(r["col"]), fmt.Sprint(r["ref_owner"]), fmt.Sprint(r["ref_table"])
		out[col] = plugin.ResourceRef{Kind: "table", Namespace: refOwner, Name: qualified(refOwner, refTable), UID: objectID(refOwner, refTable)}
	}
	return out, nil
}

func attachForeignKeys(rows []row, fks map[string]plugin.ResourceRef) {
	if len(fks) == 0 {
		return
	}
	for _, r := range rows {
		r["_links"] = fks
	}
}

func tableColumnsRoute(rc *plugin.RequestContext) (any, error) {
	owner, name, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT c.column_name AS name,
       c.data_type || CASE
         WHEN c.data_type IN ('VARCHAR2','NVARCHAR2','CHAR','NCHAR') THEN '(' || c.char_length || ')'
         WHEN c.data_type = 'NUMBER' AND c.data_precision IS NOT NULL THEN '(' || c.data_precision || COALESCE(',' || c.data_scale, '') || ')'
         ELSE ''
       END AS type,
       CASE WHEN c.nullable = 'Y' THEN 1 ELSE 0 END AS nullable,
       c.data_default AS default_value,
       c.column_id AS position,
       cc.comments
FROM all_tab_columns c
LEFT JOIN all_col_comments cc ON cc.owner = c.owner AND cc.table_name = c.table_name AND cc.column_name = c.column_name
WHERE c.owner = :1 AND c.table_name = :2
ORDER BY c.column_id`, []any{owner, name})
	if err != nil {
		return nil, err
	}
	id := rc.Param("id")
	for _, r := range rows {
		normalizeRowKeys(r)
		r["default"] = r["default_value"]
		r["nullable"] = boolish(r["nullable"])
		delete(r, "default_value")
		cn := fmt.Sprint(r["name"])
		r["ref"] = plugin.ResourceRef{Kind: "column", Scope: id, Name: cn, UID: id + "." + cn}
	}
	return pageRows(rc, rows)
}

func tableIndexes(rc *plugin.RequestContext) (any, error) {
	owner, name, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT i.index_name AS name,
       LISTAGG(ic.column_name, ', ') WITHIN GROUP (ORDER BY ic.column_position) AS columns,
       CASE WHEN i.uniqueness = 'UNIQUE' THEN 1 ELSE 0 END AS unique_flag,
       i.index_type AS type,
       i.status
FROM all_indexes i
JOIN all_ind_columns ic ON ic.index_owner = i.owner AND ic.index_name = i.index_name
WHERE i.table_owner = :1 AND i.table_name = :2
GROUP BY i.index_name, i.uniqueness, i.index_type, i.status
ORDER BY i.index_name`, []any{owner, name})
	if err != nil {
		return nil, err
	}
	id := rc.Param("id")
	for _, r := range rows {
		normalizeRowKeys(r)
		r["unique"] = boolish(r["unique_flag"])
		delete(r, "unique_flag")
		in := fmt.Sprint(r["name"])
		r["ref"] = plugin.ResourceRef{Kind: "index", Scope: id, Name: in, UID: id + "." + in}
	}
	return pageRows(rc, rows)
}

func tableConstraints(rc *plugin.RequestContext) (any, error) {
	owner, name, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT c.constraint_name AS name,
       DECODE(c.constraint_type, 'P','PRIMARY KEY','R','FOREIGN KEY','U','UNIQUE','C','CHECK', c.constraint_type) AS type,
       LISTAGG(cc.column_name, ', ') WITHIN GROUP (ORDER BY cc.position) AS columns,
       CASE WHEN r.owner IS NULL THEN NULL ELSE r.owner || '.' || r.table_name || '.' || r.constraint_name END AS referenced,
       c.status
FROM all_constraints c
LEFT JOIN all_cons_columns cc ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name
LEFT JOIN all_constraints r ON r.owner = c.r_owner AND r.constraint_name = c.r_constraint_name
WHERE c.owner = :1 AND c.table_name = :2
GROUP BY c.constraint_name, c.constraint_type, r.owner, r.table_name, r.constraint_name, c.status
ORDER BY c.constraint_name`, []any{owner, name})
	if err != nil {
		return nil, err
	}
	id := rc.Param("id")
	for _, r := range rows {
		normalizeRowKeys(r)
		cn := fmt.Sprint(r["name"])
		r["ref"] = plugin.ResourceRef{Kind: "constraint", Scope: id, Name: cn, UID: id + "." + cn}
	}
	return pageRows(rc, rows)
}

func viewDefinition(rc *plugin.RequestContext) (any, error) {
	owner, name, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT text AS definition
FROM all_views
WHERE owner = :1 AND view_name = :2`, []any{owner, name})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, plugin.ErrNotFound
	}
	normalizeRowKeys(rows[0])
	return rows[0], nil
}

func sourceDefinition(rc *plugin.RequestContext) (any, error) {
	owner, name, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	return sourceByType(rc, owner, name, "")
}

func packageSpec(rc *plugin.RequestContext) (any, error) {
	owner, name, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	return sourceByType(rc, owner, name, "PACKAGE")
}

func packageBody(rc *plugin.RequestContext) (any, error) {
	owner, name, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	return sourceByType(rc, owner, name, "PACKAGE BODY")
}

func sourceByType(rc *plugin.RequestContext, owner, name, objectType string) (any, error) {
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	args := []any{owner, name}
	filter := ""
	if objectType != "" {
		filter = " AND type = :3"
		args = append(args, objectType)
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT line, text
FROM all_source
WHERE owner = :1 AND name = :2`+filter+`
ORDER BY line`, args)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, plugin.ErrNotFound
	}
	var b strings.Builder
	for _, r := range rows {
		text := fmt.Sprint(firstValue(r, "TEXT", "text"))
		b.WriteString(text)
	}
	return row{"definition": b.String()}, nil
}

func completionRoute(rc *plugin.RequestContext) (any, error) {
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	items := []sqldb.CompletionItem{
		{Label: "SELECT", Type: "keyword"},
		{Label: "FROM", Type: "keyword"},
		{Label: "WHERE", Type: "keyword"},
		{Label: "ORDER BY", Type: "keyword"},
		{Label: "FETCH FIRST", Type: "keyword"},
		{Label: "INSERT", Type: "keyword"},
		{Label: "UPDATE", Type: "keyword"},
		{Label: "DELETE", Type: "keyword"},
		{Label: "CREATE TABLE", Type: "keyword"},
		{Label: "ALTER TABLE", Type: "keyword"},
		{Label: "BEGIN", Type: "keyword"},
		{Label: "EXCEPTION", Type: "keyword"},
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT owner, table_name, column_name
FROM all_tab_columns
WHERE rownum <= 500
ORDER BY owner, table_name, column_id`, nil)
	if err == nil {
		seen := map[string]bool{}
		for _, r := range rows {
			for _, item := range []sqldb.CompletionItem{
				{Label: fmt.Sprint(r["OWNER"]), Type: "namespace", Detail: "schema"},
				{Label: fmt.Sprint(r["TABLE_NAME"]), Type: "table", Detail: fmt.Sprint(r["OWNER"])},
				{Label: fmt.Sprint(r["COLUMN_NAME"]), Type: "property", Detail: fmt.Sprint(r["OWNER"]) + "." + fmt.Sprint(r["TABLE_NAME"])},
			} {
				key := item.Type + ":" + item.Detail + ":" + item.Label
				if !seen[key] {
					seen[key] = true
					items = append(items, item)
				}
			}
		}
	}
	return items, nil
}

func createTable(rc *plugin.RequestContext) (any, error) {
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	owner, err := safeIdent(rc.Param("schema"))
	if err != nil {
		return nil, err
	}
	var req struct {
		Name    string `json:"name" validate:"required"`
		Columns any    `json:"columns" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	table, err := safeIdent(req.Name)
	if err != nil {
		return nil, err
	}
	columns, err := parseDDLColumns(req.Columns)
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(rc.Ctx, "CREATE TABLE "+qualified(owner, table)+" ("+strings.Join(columns, ", ")+")"); err != nil {
		return nil, oracleErr(err)
	}
	return actionResult{OK: true}, nil
}

func addColumn(rc *plugin.RequestContext) (any, error) {
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	owner, table, err := objectIdent(rc)
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
	if _, err := s.db.ExecContext(rc.Ctx, "ALTER TABLE "+qualified(owner, table)+" ADD "+column); err != nil {
		return nil, oracleErr(err)
	}
	return actionResult{OK: true}, nil
}

func dropColumn(rc *plugin.RequestContext) (any, error) {
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	owner, table, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	column, err := safeIdent(rc.Param("name"))
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(rc.Ctx, "ALTER TABLE "+qualified(owner, table)+" DROP COLUMN "+quoteIdent(column)); err != nil {
		return nil, oracleErr(err)
	}
	return actionResult{OK: true}, nil
}

// createSchema creates an Oracle schema, which is a user: CREATE USER plus a
// minimal grant and unlimited quota so the new owner can create objects.
func createSchema(rc *plugin.RequestContext) (any, error) {
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Name     string `json:"name" validate:"required"`
		Password string `json:"password" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name, err := safeIdent(req.Name)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.Password) == "" {
		return nil, fmt.Errorf("%w: password is required", plugin.ErrInvalidInput)
	}
	// The password is a quoted literal, not an identifier, so escape single quotes.
	password := "\"" + strings.ReplaceAll(req.Password, `"`, `""`) + "\""
	stmts := []string{
		"CREATE USER " + quoteIdent(name) + " IDENTIFIED BY " + password,
		"ALTER USER " + quoteIdent(name) + " QUOTA UNLIMITED ON USERS",
		"GRANT CONNECT, RESOURCE TO " + quoteIdent(name),
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(rc.Ctx, stmt); err != nil {
			return nil, oracleErr(err)
		}
	}
	return actionResult{OK: true}, nil
}

func dropSchema(rc *plugin.RequestContext) (any, error) {
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	name, err := safeIdent(rc.Param("schema"))
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(rc.Ctx, "DROP USER "+quoteIdent(name)+" CASCADE"); err != nil {
		return nil, oracleErr(err)
	}
	return actionResult{OK: true}, nil
}

func renameTable(rc *plugin.RequestContext) (any, error) {
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	owner, table, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		Name string `json:"name" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	to, err := safeIdent(req.Name)
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(rc.Ctx, "ALTER TABLE "+qualified(owner, table)+" RENAME TO "+quoteIdent(to)); err != nil {
		return nil, oracleErr(err)
	}
	return actionResult{OK: true}, nil
}

func alterColumn(rc *plugin.RequestContext) (any, error) {
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	owner, table, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	column, err := safeIdent(rc.Param("name"))
	if err != nil {
		return nil, err
	}
	var req struct {
		Type     string `json:"type" validate:"required"`
		Nullable bool   `json:"nullable"`
		Default  string `json:"default"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	clause, err := alterColumnClause(column, req.Type, req.Nullable, req.Default)
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(rc.Ctx, "ALTER TABLE "+qualified(owner, table)+" MODIFY ("+clause+")"); err != nil {
		return nil, oracleErr(err)
	}
	return actionResult{OK: true}, nil
}

func renameColumn(rc *plugin.RequestContext) (any, error) {
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	owner, table, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	column, err := safeIdent(rc.Param("name"))
	if err != nil {
		return nil, err
	}
	var req struct {
		To string `json:"to" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	to, err := safeIdent(req.To)
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(rc.Ctx, "ALTER TABLE "+qualified(owner, table)+" RENAME COLUMN "+quoteIdent(column)+" TO "+quoteIdent(to)); err != nil {
		return nil, oracleErr(err)
	}
	return actionResult{OK: true}, nil
}

func addConstraint(rc *plugin.RequestContext) (any, error) {
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	owner, table, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		Name       string `json:"name" validate:"required"`
		Type       string `json:"type" validate:"required"`
		Columns    any    `json:"columns"`
		Check      string `json:"check"`
		RefTable   string `json:"ref_table"`
		RefColumns string `json:"ref_columns"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	clause, err := constraintClause(req.Name, req.Type, req.Columns, req.Check, req.RefTable, req.RefColumns)
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(rc.Ctx, "ALTER TABLE "+qualified(owner, table)+" ADD "+clause); err != nil {
		return nil, oracleErr(err)
	}
	return actionResult{OK: true}, nil
}

func dropConstraint(rc *plugin.RequestContext) (any, error) {
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	owner, table, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	name, err := safeIdent(rc.Param("name"))
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(rc.Ctx, "ALTER TABLE "+qualified(owner, table)+" DROP CONSTRAINT "+quoteIdent(name)); err != nil {
		return nil, oracleErr(err)
	}
	return actionResult{OK: true}, nil
}

func createIndex(rc *plugin.RequestContext) (any, error) {
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	owner, table, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		Name    string `json:"name" validate:"required"`
		Columns any    `json:"columns" validate:"required"`
		Unique  bool   `json:"unique"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name, err := safeIdent(req.Name)
	if err != nil {
		return nil, err
	}
	cols, err := sqldb.IdentifierListValue(req.Columns, quoteIdent)
	if err != nil {
		return nil, err
	}
	unique := ""
	if req.Unique {
		unique = "UNIQUE "
	}
	stmt := "CREATE " + unique + "INDEX " + qualified(owner, name) + " ON " + qualified(owner, table) + " (" + strings.Join(cols, ", ") + ")"
	if _, err := s.db.ExecContext(rc.Ctx, stmt); err != nil {
		return nil, oracleErr(err)
	}
	return actionResult{OK: true}, nil
}

func dropIndex(rc *plugin.RequestContext) (any, error) {
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	owner, _, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	name, err := safeIdent(rc.Param("name"))
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(rc.Ctx, "DROP INDEX "+qualified(owner, name)); err != nil {
		return nil, oracleErr(err)
	}
	return actionResult{OK: true}, nil
}

func insertRow(rc *plugin.RequestContext) (any, error) {
	s, owner, table, m, err := rowMutationInput(rc)
	if err != nil {
		return nil, err
	}
	stmt, args, err := dialect.Insert(qualified(owner, table), m.Values)
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(rc.Ctx, stmt, args...); err != nil {
		return nil, oracleErr(err)
	}
	return actionResult{OK: true}, nil
}

func updateRow(rc *plugin.RequestContext) (any, error) {
	return keyedRowMutation(rc, false)
}

func deleteRow(rc *plugin.RequestContext) (any, error) {
	return keyedRowMutation(rc, true)
}

func rowMutationInput(rc *plugin.RequestContext) (*Session, string, string, sqldb.RowMutation, error) {
	s, err := oracleSession(rc)
	if err != nil {
		return nil, "", "", sqldb.RowMutation{}, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, "", "", sqldb.RowMutation{}, err
	}
	owner, table, err := objectIdent(rc)
	if err != nil {
		return nil, "", "", sqldb.RowMutation{}, err
	}
	var m sqldb.RowMutation
	if err := rc.Bind(&m); err != nil {
		return nil, "", "", sqldb.RowMutation{}, err
	}
	return s, owner, table, m, nil
}

// keyedRowMutation runs an UPDATE or DELETE only after confirming the client's
// key is exactly the table's primary key and that it affects a single row.
func keyedRowMutation(rc *plugin.RequestContext, del bool) (any, error) {
	s, owner, table, m, err := rowMutationInput(rc)
	if err != nil {
		return nil, err
	}
	pk, err := primaryKeyColumns(rc.Ctx, s, owner, table)
	if err != nil {
		return nil, err
	}
	if err := sqldb.ValidateRowKey(pk, m.Key); err != nil {
		return nil, err
	}
	qual := qualified(owner, table)
	var stmt string
	var args []any
	if del {
		stmt, args, err = dialect.Delete(qual, m.Key)
	} else {
		stmt, args, err = dialect.Update(qual, m.Key, m.Values)
	}
	if err != nil {
		return nil, err
	}
	res, err := s.db.ExecContext(rc.Ctx, stmt, args...)
	if err != nil {
		return nil, oracleErr(err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return nil, fmt.Errorf("%w: row no longer matches (it may have changed)", plugin.ErrNotFound)
	}
	return actionResult{OK: true}, nil
}

// primaryKeyColumns returns the real (unquoted, Oracle-uppercase) primary-key
// column names so the editable grid's UPDATE/DELETE match by quoted identifier.
// oracleSearchClause builds a case-insensitive "contains" filter across cols.
// It uses TO_CHAR rather than CAST(... AS VARCHAR2): casting a numeric column
// trips ORA-01722 when compared to the '%term%' pattern.
func oracleSearchClause(cols []string, term string, start int) (string, []any) {
	term = strings.TrimSpace(term)
	if term == "" || len(cols) == 0 {
		return "", nil
	}
	pattern := "%" + term + "%"
	parts := make([]string, 0, len(cols))
	args := make([]any, 0, len(cols))
	n := start
	for _, c := range cols {
		col, err := safeIdent(c)
		if err != nil {
			continue
		}
		parts = append(parts, "UPPER(TO_CHAR("+quoteIdent(col)+")) LIKE UPPER("+dialect.Placeholder(n)+")")
		args = append(args, pattern)
		n++
	}
	if len(parts) == 0 {
		return "", nil
	}
	return "(" + strings.Join(parts, " OR ") + ")", args
}

// columnNames returns a table's column names in order, for the data grid's
// free-text search across every column.
func columnNames(ctx context.Context, s *Session, owner, table string) ([]string, error) {
	rows, err := queryRows(ctx, s, `
SELECT column_name AS name FROM all_tab_columns
WHERE owner = :1 AND table_name = :2
ORDER BY column_id`, []any{owner, table})
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		normalizeRowKeys(r)
		out = append(out, fmt.Sprint(r["name"]))
	}
	return out, nil
}

func primaryKeyColumns(ctx context.Context, s *Session, owner, table string) ([]string, error) {
	rows, err := queryRows(ctx, s, `
SELECT cc.column_name AS name
FROM all_constraints c
JOIN all_cons_columns cc ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name
WHERE c.constraint_type = 'P' AND c.owner = :1 AND c.table_name = :2
ORDER BY cc.position`, []any{owner, table})
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		// Oracle reports the AS-aliased key uppercased; normalize so r["name"]
		// resolves. The value (the real column name) stays uppercase, matching
		// the un-normalized data rows the grid edits.
		normalizeRowKeys(r)
		out = append(out, fmt.Sprint(r["name"]))
	}
	return out, nil
}

// attachRowKeys tags each row with the primary-key map the editable grid echoes
// back for UPDATE/DELETE. The grid stays read-only when the table has no primary
// key or when a key column is itself sensitive (so a redacted value is never
// shipped raw inside _key).
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

func truncateTable(rc *plugin.RequestContext) (any, error) {
	owner, table, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	return execDDL(rc, "TRUNCATE TABLE "+qualified(owner, table))
}

func dropTable(rc *plugin.RequestContext) (any, error) {
	owner, table, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	return execDDL(rc, "DROP TABLE "+qualified(owner, table))
}

func dropView(rc *plugin.RequestContext) (any, error) {
	owner, view, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	return execDDL(rc, "DROP VIEW "+qualified(owner, view))
}

func execDDL(rc *plugin.RequestContext, sqlText string) (any, error) {
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(rc.Ctx, sqlText); err != nil {
		return nil, oracleErr(err)
	}
	return actionResult{OK: true}, nil
}

func cancelQuery(rc *plugin.RequestContext) (any, error) {
	s, err := oracleSession(rc)
	if err != nil {
		return nil, err
	}
	return actionResult{OK: s.cancelAll()}, nil
}

func queryStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := oracleSession(rc)
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
			if err := enc.Encode(map[string]any{"error": "Invalid query request."}); err != nil {
				return err
			}
			continue
		}
		schema := stringDefault(rc.Param("schema"), s.opts.Username)
		statements := sqldb.SplitStatements(req.Query)
		result, err := executeQueryRequest(client.Context(), s, schema, req)
		rc.Audit(queryAuditResult(err), sqldb.AuditParams(sqldb.QueryAudit{
			Query: req.Query, Statements: statements, Confirmed: req.Confirm, ReadOnlyMode: s.opts.ReadOnly,
			RequiresReview: statementsRequireReview(statements), RowCount: result.RowCount, ElapsedMS: result.ElapsedMS, CommandTag: result.CommandTag,
		}), err)
		if err != nil {
			payload := map[string]any{"error": err.Error()}
			var confirmErr confirmationError
			if errors.As(err, &confirmErr) {
				payload["requiresConfirmation"] = true
				payload["confirmMessage"] = "This Oracle statement can change data, schema, PL/SQL state, or privileges. Review it before running."
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

type sqlRunner interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func executeQueryRequest(parent context.Context, s *Session, schema string, req sqldb.QueryRequest) (sqldb.QueryResult, error) {
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
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return sqldb.QueryResult{}, oracleErr(err)
	}
	defer func() { _ = conn.Close() }()
	if schema != "" {
		if safeSchema, err := safeIdent(schema); err == nil {
			if _, err := conn.ExecContext(ctx, "ALTER SESSION SET CURRENT_SCHEMA = "+quoteIdent(safeSchema)); err != nil {
				return sqldb.QueryResult{}, oracleErr(err)
			}
		}
	}
	results := make([]sqldb.StatementResult, 0, len(statements))
	for _, st := range statements {
		res, err := executeStatement(ctx, conn, s, st)
		if err != nil {
			return sqldb.QueryResult{}, err
		}
		results = append(results, res)
	}
	out := sqldb.QueryResult{Statements: results}
	if len(results) > 0 {
		last := results[len(results)-1]
		out.Columns, out.Rows, out.RowCount = last.Columns, last.Rows, last.RowCount
		out.ElapsedMS, out.Statement, out.CommandTag = last.ElapsedMS, last.Statement, last.CommandTag
	}
	return out, nil
}

func executeStatement(ctx context.Context, runner sqlRunner, s *Session, statement string) (sqldb.StatementResult, error) {
	start := time.Now()
	if !statementReturnsRows(statement) {
		res, err := runner.ExecContext(ctx, statement)
		if err != nil {
			return sqldb.StatementResult{}, oracleErr(err)
		}
		affected, _ := res.RowsAffected()
		out := sqldb.StatementResult{Statement: statement, RowCount: affected, ElapsedMS: time.Since(start).Milliseconds(), CommandTag: sqldb.FirstKeyword(statement)}
		if affected >= 0 {
			out.CommandTag += " " + strconv.FormatInt(affected, 10)
		}
		return out, nil
	}
	rows, err := runner.QueryContext(ctx, statement)
	if err != nil {
		return sqldb.StatementResult{}, oracleErr(err)
	}
	columns, err := rows.Columns()
	if err != nil {
		_ = rows.Close()
		return sqldb.StatementResult{}, oracleErr(err)
	}
	for i := range columns {
		columns[i] = strings.ToLower(columns[i])
	}
	out := sqldb.StatementResult{Statement: statement, Columns: columns}
	for rows.Next() {
		values, err := scanValues(rows, columns)
		if err != nil {
			_ = rows.Close()
			return sqldb.StatementResult{}, oracleErr(err)
		}
		out.Rows = append(out.Rows, values)
		if len(out.Rows) >= s.opts.RowLimit {
			break
		}
	}
	if err := rows.Close(); err != nil {
		return sqldb.StatementResult{}, oracleErr(err)
	}
	if err := rows.Err(); err != nil {
		return sqldb.StatementResult{}, oracleErr(err)
	}
	out.RowCount = int64(len(out.Rows))
	out.Rows = sqldb.RedactRows(out.Columns, out.Rows, s.opts.RedactPatterns)
	out.CommandTag = sqldb.FirstKeyword(statement)
	out.ElapsedMS = time.Since(start).Milliseconds()
	return out, nil
}

func queryRows(ctx context.Context, s *Session, sqlText string, args []any) ([]row, error) {
	ctx, cancel := context.WithTimeout(ctx, s.opts.QueryTimeout)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return nil, oracleErr(err)
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		return nil, oracleErr(err)
	}
	out := []row{}
	for rows.Next() {
		values, err := scanValues(rows, columns)
		if err != nil {
			return nil, oracleErr(err)
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
		return nil, oracleErr(err)
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

func statementReturnsRows(statement string) bool {
	switch sqldb.FirstKeyword(statement) {
	case "SELECT", "WITH":
		return true
	default:
		return false
	}
}

func isReadOnlyStatement(statement string) bool {
	return statementReturnsRows(statement)
}

func isDestructiveStatement(statement string) bool {
	return !isReadOnlyStatement(statement)
}

func queryAuditResult(err error) plugin.AuditResult {
	if err == nil {
		return plugin.AuditAllowed
	}
	var confirmErr confirmationError
	if errors.As(err, &confirmErr) || errors.Is(err, plugin.ErrForbidden) {
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
		if owner := fmt.Sprint(firstValue(r, "owner", "OWNER")); owner != "" && owner != "<nil>" && kind != "schema" && kind != "user" && owner != label {
			label = owner + "." + label
		}
		nodes = append(nodes, plugin.TreeNode{Key: kind + ":" + ref.UID, Label: label, Icon: icon(iconName), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
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

func redactRows(rows []row, patterns []string) {
	for _, r := range rows {
		for key, value := range r {
			if key == "_key" {
				continue
			}
			if value != nil && sqldb.RedactColumn(key, patterns) {
				r[key] = sqldb.RedactedValue
			}
		}
	}
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
	name, err := safeIdent(spec.Name)
	if err != nil {
		return "", err
	}
	dataType := strings.TrimSpace(spec.Type)
	if !sqldb.SafeType(dataType) {
		return "", fmt.Errorf("%w: unsafe column type", plugin.ErrInvalidInput)
	}
	parts := []string{quoteIdent(name), dataType}
	if !spec.Nullable || spec.Primary {
		parts = append(parts, "NOT NULL")
	}
	if strings.TrimSpace(spec.Default) != "" {
		if !sqldb.SafeDefault(spec.Default) {
			return "", fmt.Errorf("%w: unsafe default expression", plugin.ErrInvalidInput)
		}
		parts = append(parts, "DEFAULT "+strings.TrimSpace(spec.Default))
	}
	if spec.Primary {
		parts = append(parts, "PRIMARY KEY")
	}
	if spec.Unique {
		parts = append(parts, "UNIQUE")
	}
	return strings.Join(parts, " "), nil
}

// alterColumnClause builds the body of an Oracle ALTER TABLE ... MODIFY (...)
// for one column: type, NULL/NOT NULL, and an optional default.
func alterColumnClause(column, dataType string, nullable bool, def string) (string, error) {
	col, err := safeIdent(column)
	if err != nil {
		return "", err
	}
	dataType = strings.TrimSpace(dataType)
	if !sqldb.SafeType(dataType) {
		return "", fmt.Errorf("%w: unsafe column type", plugin.ErrInvalidInput)
	}
	parts := []string{quoteIdent(col), dataType}
	if strings.TrimSpace(def) != "" {
		if !sqldb.SafeDefault(def) {
			return "", fmt.Errorf("%w: unsafe default expression", plugin.ErrInvalidInput)
		}
		parts = append(parts, "DEFAULT "+strings.TrimSpace(def))
	}
	if nullable {
		parts = append(parts, "NULL")
	} else {
		parts = append(parts, "NOT NULL")
	}
	return strings.Join(parts, " "), nil
}

// constraintClause builds an Oracle CONSTRAINT clause (the body of ALTER TABLE
// ADD ...) for a primary-key, unique, check, or foreign-key constraint.
func constraintClause(name, kind string, columns any, check, refTable, refColumns string) (string, error) {
	cn, err := safeIdent(name)
	if err != nil {
		return "", err
	}
	prefix := "CONSTRAINT " + quoteIdent(cn) + " "
	switch strings.ToUpper(strings.TrimSpace(kind)) {
	case "PRIMARY KEY", "UNIQUE":
		cols, err := sqldb.IdentifierListValue(columns, quoteIdent)
		if err != nil {
			return "", err
		}
		if len(cols) == 0 {
			return "", fmt.Errorf("%w: at least one column is required", plugin.ErrInvalidInput)
		}
		return prefix + strings.ToUpper(strings.TrimSpace(kind)) + " (" + strings.Join(cols, ", ") + ")", nil
	case "CHECK":
		expr := strings.TrimSpace(check)
		if expr == "" {
			return "", fmt.Errorf("%w: a check expression is required", plugin.ErrInvalidInput)
		}
		if !sqldb.SafeDefault(expr) {
			return "", fmt.Errorf("%w: unsafe check expression", plugin.ErrInvalidInput)
		}
		return prefix + "CHECK (" + expr + ")", nil
	case "FOREIGN KEY":
		cols, err := sqldb.IdentifierListValue(columns, quoteIdent)
		if err != nil {
			return "", err
		}
		if len(cols) == 0 {
			return "", fmt.Errorf("%w: at least one column is required", plugin.ErrInvalidInput)
		}
		ref, err := qualifiedRef(refTable)
		if err != nil {
			return "", err
		}
		refCols, err := sqldb.IdentifierListValue(refColumns, quoteIdent)
		if err != nil {
			return "", err
		}
		if len(refCols) == 0 {
			return "", fmt.Errorf("%w: referenced columns are required", plugin.ErrInvalidInput)
		}
		return prefix + "FOREIGN KEY (" + strings.Join(cols, ", ") + ") REFERENCES " + ref + " (" + strings.Join(refCols, ", ") + ")", nil
	default:
		return "", fmt.Errorf("%w: unsupported constraint type", plugin.ErrInvalidInput)
	}
}

// qualifiedRef quotes a foreign-key target that is either TABLE or OWNER.TABLE.
func qualifiedRef(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("%w: a referenced table is required", plugin.ErrInvalidInput)
	}
	owner, table, ok := strings.Cut(raw, ".")
	if !ok {
		name, err := safeIdent(raw)
		if err != nil {
			return "", err
		}
		return quoteIdent(name), nil
	}
	o, err := safeIdent(owner)
	if err != nil {
		return "", err
	}
	t, err := safeIdent(table)
	if err != nil {
		return "", err
	}
	return qualified(o, t), nil
}

func objectIdent(rc *plugin.RequestContext) (string, string, error) {
	return parseObjectID(rc.Param("id"))
}

func objectID(owner, name string) string {
	return owner + ":" + name
}

func parseObjectID(id string) (string, string, error) {
	owner, name, ok := strings.Cut(id, ":")
	if !ok {
		return "", "", fmt.Errorf("%w: object id is invalid", plugin.ErrInvalidInput)
	}
	owner, err := safeIdent(owner)
	if err != nil {
		return "", "", err
	}
	name, err = safeIdent(name)
	if err != nil {
		return "", "", err
	}
	return owner, name, nil
}

func safeIdent(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("%w: identifier is required", plugin.ErrInvalidInput)
	}
	if strings.ContainsAny(name, "\x00\":") {
		return "", fmt.Errorf("%w: identifier is invalid", plugin.ErrInvalidInput)
	}
	return name, nil
}

func optionalIdent(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", nil
	}
	return safeIdent(name)
}

func qualified(owner, name string) string {
	return quoteIdent(owner) + "." + quoteIdent(name)
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func offsetCursor(raw string) (int, error) {
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

func oracleErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return plugin.ErrNotFound
	}
	return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
}

func isPrivilegeError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "ORA-00942") || strings.Contains(msg, "ORA-01031")
}

func normalizeRowKeys(r row) {
	for key, value := range r {
		lower := strings.ToLower(key)
		if lower != key {
			r[lower] = value
			delete(r, key)
		}
	}
}

func firstValue(r row, keys ...string) any {
	for _, key := range keys {
		if value, ok := r[key]; ok {
			return value
		}
	}
	return nil
}

func boolish(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case int64:
		return x != 0
	case int:
		return x != 0
	case string:
		return x == "1" || strings.EqualFold(x, "YES") || strings.EqualFold(x, "TRUE")
	default:
		return false
	}
}

func nullIfEmpty(v string) any {
	if strings.TrimSpace(v) == "" || strings.TrimSpace(v) == "<nil>" {
		return nil
	}
	return v
}

func paramOrQuery(rc *plugin.RequestContext, key string) string {
	if v := rc.Param(key); v != "" {
		return v
	}
	return rc.Query().Get("p." + key)
}
