package mssql

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

type row = plugin.TableRow

type actionResult struct {
	OK bool `json:"ok"`
}

type confirmationError struct {
	message string
}

func (e confirmationError) Error() string { return e.message }

var dialect = sqldb.Dialect{QuoteIdent: quoteIdent, Placeholder: sqldb.AtPlaceholder}

func routes() []plugin.Route {
	return []plugin.Route{
		{ID: "mssql.databases.tree", Method: plugin.MethodGet, Path: "/tree/databases", Permission: "mssql.databases.read", Risk: plugin.RiskSafe, AuditEvent: "mssql.databases.tree", Handle: treeDatabases},
		{ID: "mssql.databases.list", Method: plugin.MethodGet, Path: "/databases", Permission: "mssql.databases.read", Risk: plugin.RiskSafe, AuditEvent: "mssql.databases.list", Handle: listDatabases},
		{ID: "mssql.database.overview", Method: plugin.MethodGet, Path: "/databases/{database}/overview", Permission: "mssql.databases.read", Risk: plugin.RiskSafe, AuditEvent: "mssql.database.overview", Handle: databaseOverview},
		{ID: "mssql.schemas.tree", Method: plugin.MethodGet, Path: "/tree/schemas", Permission: "mssql.schemas.read", Risk: plugin.RiskSafe, AuditEvent: "mssql.schemas.tree", Handle: treeSchemas},
		{ID: "mssql.schemas.list", Method: plugin.MethodGet, Path: "/schemas", Permission: "mssql.schemas.read", Risk: plugin.RiskSafe, AuditEvent: "mssql.schemas.list", Handle: listSchemas},
		{ID: "mssql.schema.overview", Method: plugin.MethodGet, Path: "/schemas/{database}/{schema}/overview", Permission: "mssql.schemas.read", Risk: plugin.RiskSafe, AuditEvent: "mssql.schema.overview", Handle: schemaOverview},
		{ID: "mssql.relations.tree", Method: plugin.MethodGet, Path: "/tree/relations", Permission: "mssql.tables.read", Risk: plugin.RiskSafe, AuditEvent: "mssql.relations.tree", Handle: treeRelations},
		{ID: "mssql.tables.list", Method: plugin.MethodGet, Path: "/tables", Permission: "mssql.tables.read", Risk: plugin.RiskSafe, AuditEvent: "mssql.tables.list", Handle: listTables},
		{ID: "mssql.relations.graph", Method: plugin.MethodGet, Path: "/relations/graph", Permission: "mssql.tables.read", Risk: plugin.RiskSafe, AuditEvent: "mssql.relations.graph", Handle: relationGraph},
		{ID: "mssql.views.list", Method: plugin.MethodGet, Path: "/views", Permission: "mssql.views.read", Risk: plugin.RiskSafe, AuditEvent: "mssql.views.list", Handle: listViews},
		{ID: "mssql.view.drop", Method: plugin.MethodDelete, Path: "/views/{id}", Permission: "mssql.views.delete", Risk: plugin.RiskDestructive, AuditEvent: "mssql.view.drop", Handle: dropView},
		{ID: "mssql.procedures.tree", Method: plugin.MethodGet, Path: "/tree/procedures", Permission: "mssql.procedures.read", Risk: plugin.RiskSafe, AuditEvent: "mssql.procedures.tree", Handle: treeProcedures},
		{ID: "mssql.procedures.list", Method: plugin.MethodGet, Path: "/procedures", Permission: "mssql.procedures.read", Risk: plugin.RiskSafe, AuditEvent: "mssql.procedures.list", Handle: listProcedures},
		{ID: "mssql.users.tree", Method: plugin.MethodGet, Path: "/tree/users", Permission: "mssql.users.read", Risk: plugin.RiskSafe, AuditEvent: "mssql.users.tree", Handle: treeUsers},
		{ID: "mssql.users.list", Method: plugin.MethodGet, Path: "/users", Permission: "mssql.users.read", Risk: plugin.RiskSafe, AuditEvent: "mssql.users.list", Handle: listUsers},
		{ID: "mssql.user.overview", Method: plugin.MethodGet, Path: "/users/{database}/{user}/overview", Permission: "mssql.users.read", Risk: plugin.RiskSafe, AuditEvent: "mssql.user.overview", Handle: userOverview},
		{ID: "mssql.jobs.tree", Method: plugin.MethodGet, Path: "/tree/jobs", Permission: "mssql.jobs.read", Risk: plugin.RiskSafe, AuditEvent: "mssql.jobs.tree", Handle: treeJobs},
		{ID: "mssql.jobs.list", Method: plugin.MethodGet, Path: "/jobs", Permission: "mssql.jobs.read", Risk: plugin.RiskSafe, AuditEvent: "mssql.jobs.list", Handle: listJobs},
		{ID: "mssql.job.overview", Method: plugin.MethodGet, Path: "/jobs/{id}/overview", Permission: "mssql.jobs.read", Risk: plugin.RiskSafe, AuditEvent: "mssql.job.overview", Handle: jobOverview},
		{ID: "mssql.job.start", Method: plugin.MethodPost, Path: "/jobs/{name}/start", Permission: "mssql.jobs.write", Risk: plugin.RiskWrite, AuditEvent: "mssql.job.start", Handle: startJob},
		{ID: "mssql.job.enable", Method: plugin.MethodPost, Path: "/jobs/{name}/enable", Permission: "mssql.jobs.write", Risk: plugin.RiskWrite, AuditEvent: "mssql.job.enable", Handle: enableJob},
		{ID: "mssql.job.disable", Method: plugin.MethodPost, Path: "/jobs/{name}/disable", Permission: "mssql.jobs.write", Risk: plugin.RiskWrite, AuditEvent: "mssql.job.disable", Handle: disableJob},
		{ID: "mssql.user.create", Method: plugin.MethodPost, Path: "/users", Permission: "mssql.users.write", Risk: plugin.RiskWrite, AuditEvent: "mssql.user.create", Input: userCreateSchema(), Handle: createUser},
		{ID: "mssql.user.grant", Method: plugin.MethodPost, Path: "/users/{database}/{user}/grant", Permission: "mssql.users.write", Risk: plugin.RiskPrivileged, AuditEvent: "mssql.user.grant", Input: userGrantSchema(), Handle: grantUser},
		{ID: "mssql.user.drop", Method: plugin.MethodDelete, Path: "/users/{database}/{user}", Permission: "mssql.users.delete", Risk: plugin.RiskDestructive, AuditEvent: "mssql.user.drop", Handle: dropUser},
		{ID: "mssql.table.rows", Method: plugin.MethodGet, Path: "/objects/{id}/rows", Permission: "mssql.tables.data.read", Risk: plugin.RiskSafe, AuditEvent: "mssql.table.rows", Handle: tableRows},
		{ID: "mssql.view.rows", Method: plugin.MethodGet, Path: "/objects/{id}/view-rows", Permission: "mssql.views.data.read", Risk: plugin.RiskSafe, AuditEvent: "mssql.view.rows", Handle: tableRows},
		{ID: "mssql.table.columns", Method: plugin.MethodGet, Path: "/objects/{id}/columns", Permission: "mssql.tables.read", Risk: plugin.RiskSafe, AuditEvent: "mssql.table.columns", Handle: tableColumnsRoute},
		{ID: "mssql.table.indexes", Method: plugin.MethodGet, Path: "/objects/{id}/indexes", Permission: "mssql.tables.read", Risk: plugin.RiskSafe, AuditEvent: "mssql.table.indexes", Handle: tableIndexes},
		{ID: "mssql.table.constraints", Method: plugin.MethodGet, Path: "/objects/{id}/constraints", Permission: "mssql.tables.read", Risk: plugin.RiskSafe, AuditEvent: "mssql.table.constraints", Handle: tableConstraints},
		{ID: "mssql.view.definition", Method: plugin.MethodGet, Path: "/objects/{id}/view-definition", Permission: "mssql.views.read", Risk: plugin.RiskSafe, AuditEvent: "mssql.view.definition", Handle: objectDefinition},
		{ID: "mssql.procedure.definition", Method: plugin.MethodGet, Path: "/objects/{id}/procedure-definition", Permission: "mssql.procedures.read", Risk: plugin.RiskSafe, AuditEvent: "mssql.procedure.definition", Handle: objectDefinition},
		{ID: "mssql.completion", Method: plugin.MethodGet, Path: "/completion", Permission: "mssql.databases.read", Risk: plugin.RiskSafe, AuditEvent: "mssql.completion", Handle: completionRoute},
		{ID: "mssql.table.row.insert", Method: plugin.MethodPost, Path: "/objects/{id}/rows", Permission: "mssql.tables.data.write", Risk: plugin.RiskWrite, AuditEvent: "mssql.table.row.insert", Handle: insertRow},
		{ID: "mssql.table.row.update", Method: plugin.MethodPatch, Path: "/objects/{id}/rows", Permission: "mssql.tables.data.write", Risk: plugin.RiskWrite, AuditEvent: "mssql.table.row.update", Handle: updateRow},
		{ID: "mssql.table.row.delete", Method: plugin.MethodDelete, Path: "/objects/{id}/rows", Permission: "mssql.tables.data.delete", Risk: plugin.RiskDestructive, AuditEvent: "mssql.table.row.delete", Handle: deleteRow},
		{ID: "mssql.database.create", Method: plugin.MethodPost, Path: "/databases", Permission: "mssql.databases.write", Risk: plugin.RiskWrite, AuditEvent: "mssql.database.create", Input: databaseCreateSchema(), Handle: createDatabase},
		{ID: "mssql.database.drop", Method: plugin.MethodDelete, Path: "/databases/{database}", Permission: "mssql.databases.delete", Risk: plugin.RiskDestructive, AuditEvent: "mssql.database.drop", Handle: dropDatabase},
		{ID: "mssql.schema.create", Method: plugin.MethodPost, Path: "/databases/{database}/schemas", Permission: "mssql.schemas.write", Risk: plugin.RiskWrite, AuditEvent: "mssql.schema.create", Input: schemaCreateSchema(), Handle: createSchema},
		{ID: "mssql.schema.drop", Method: plugin.MethodDelete, Path: "/schemas/{database}/{schema}", Permission: "mssql.schemas.delete", Risk: plugin.RiskDestructive, AuditEvent: "mssql.schema.drop", Handle: dropSchema},
		{ID: "mssql.table.create", Method: plugin.MethodPost, Path: "/schemas/{database}/{schema}/tables", Permission: "mssql.tables.write", Risk: plugin.RiskWrite, AuditEvent: "mssql.table.create", Input: tableCreateSchema(), Handle: createTable},
		{ID: "mssql.table.rename", Method: plugin.MethodPost, Path: "/objects/{id}/rename", Permission: "mssql.tables.write", Risk: plugin.RiskWrite, AuditEvent: "mssql.table.rename", Input: tableRenameSchema(), Handle: renameTable},
		{ID: "mssql.column.add", Method: plugin.MethodPost, Path: "/objects/{id}/columns", Permission: "mssql.tables.write", Risk: plugin.RiskWrite, AuditEvent: "mssql.column.add", Input: columnAddSchema(), Handle: addColumn},
		{ID: "mssql.column.alter", Method: plugin.MethodPost, Path: "/objects/{id}/columns/alter", Permission: "mssql.tables.write", Risk: plugin.RiskWrite, AuditEvent: "mssql.column.alter", Input: columnAlterSchema(), Handle: alterColumn},
		{ID: "mssql.column.drop", Method: plugin.MethodPost, Path: "/objects/{id}/columns/drop", Permission: "mssql.tables.write", Risk: plugin.RiskDestructive, AuditEvent: "mssql.column.drop", Handle: dropColumn},
		{ID: "mssql.constraint.add", Method: plugin.MethodPost, Path: "/objects/{id}/constraints", Permission: "mssql.tables.write", Risk: plugin.RiskWrite, AuditEvent: "mssql.constraint.add", Input: constraintAddSchema(), Handle: addConstraint},
		{ID: "mssql.constraint.drop", Method: plugin.MethodPost, Path: "/objects/{id}/constraints/drop", Permission: "mssql.tables.write", Risk: plugin.RiskDestructive, AuditEvent: "mssql.constraint.drop", Handle: dropConstraint},
		{ID: "mssql.index.create", Method: plugin.MethodPost, Path: "/objects/{id}/indexes", Permission: "mssql.tables.write", Risk: plugin.RiskWrite, AuditEvent: "mssql.index.create", Input: indexCreateSchema(), Handle: createIndex},
		{ID: "mssql.index.drop", Method: plugin.MethodPost, Path: "/objects/{id}/indexes/drop", Permission: "mssql.tables.write", Risk: plugin.RiskDestructive, AuditEvent: "mssql.index.drop", Handle: dropIndex},
		{ID: "mssql.table.truncate", Method: plugin.MethodPost, Path: "/objects/{id}/truncate", Permission: "mssql.tables.delete", Risk: plugin.RiskDestructive, AuditEvent: "mssql.table.truncate", Handle: truncateTable},
		{ID: "mssql.table.drop", Method: plugin.MethodDelete, Path: "/objects/{id}", Permission: "mssql.tables.delete", Risk: plugin.RiskDestructive, AuditEvent: "mssql.table.drop", Handle: dropTable},
		{ID: "mssql.query", Method: plugin.MethodWS, Path: "/query", Permission: "mssql.query.execute", Risk: plugin.RiskPrivileged, AuditEvent: "mssql.query", Stream: queryStream},
		{ID: "mssql.query.cancel", Method: plugin.MethodPost, Path: "/query/cancel", Permission: "mssql.query.cancel", Risk: plugin.RiskWrite, AuditEvent: "mssql.query.cancel", Handle: cancelQuery},
	}
}

func mssqlSession(rc *plugin.RequestContext) (*Session, error) {
	return unwrap(rc.Session)
}

func databaseCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Database", Fields: []plugin.Field{
		{Key: "name", Label: "Database name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
	}}}}
}

func tableCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Table", Fields: []plugin.Field{
		{Key: "name", Label: "Table name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		sqldb.ColumnsArrayField(sqldb.ColumnsField{TypePlaceholder: "bigint", TypeSuggestions: []string{"int", "bigint", "smallint", "tinyint", "bit", "decimal(18,2)", "money", "float", "real", "char(1)", "varchar(255)", "varchar(max)", "nvarchar(255)", "nvarchar(max)", "text", "date", "datetime2", "datetime", "time", "uniqueidentifier", "varbinary(max)"}, Default: true, Primary: true, Unique: true}),
	}}}}
}

func columnAddSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Column", Fields: []plugin.Field{
		{Key: "name", Label: "Column name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		{Key: "type", Label: "Type", Type: plugin.FieldText, Required: true, Default: "nvarchar(255)"},
		{Key: "nullable", Label: "Nullable", Type: plugin.FieldToggle, Default: true},
		{Key: "default", Label: "Default expression", Type: plugin.FieldText},
	}}}}
}

func indexCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Index", Fields: []plugin.Field{
		{Key: "name", Label: "Index name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		{Key: "columns", Label: "Columns", Type: plugin.FieldMultiSelect, Required: true, OptionsSource: &plugin.DataSource{RouteID: "mssql.table.columns", Params: objectParams()}},
		{Key: "unique", Label: "Unique", Type: plugin.FieldToggle},
	}}}}
}

func schemaCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Schema", Fields: []plugin.Field{
		{Key: "name", Label: "Schema name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
	}}}}
}

func tableRenameSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Rename", Fields: []plugin.Field{
		{Key: "name", Label: "New table name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
	}}}}
}

func columnAlterSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Column", Fields: []plugin.Field{
		{Key: "name", Label: "Column", Type: plugin.FieldMultiSelect, Required: true, OptionsSource: &plugin.DataSource{RouteID: "mssql.table.columns", Params: objectParams()}},
		{Key: "type", Label: "New type", Type: plugin.FieldText, Required: true, Default: "nvarchar(255)"},
		{Key: "nullable", Label: "Nullable", Type: plugin.FieldToggle, Default: true},
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
		{Key: "columns", Label: "Columns", Type: plugin.FieldMultiSelect, OptionsSource: &plugin.DataSource{RouteID: "mssql.table.columns", Params: objectParams()}, Help: "Columns for PRIMARY KEY, UNIQUE, or FOREIGN KEY."},
		{Key: "check", Label: "Check expression", Type: plugin.FieldText, Help: "Boolean expression for a CHECK constraint, e.g. [age] >= 0."},
		{Key: "ref_table", Label: "Referenced table", Type: plugin.FieldText, Help: "Schema-qualified target for a FOREIGN KEY, e.g. dbo.people."},
		{Key: "ref_columns", Label: "Referenced columns", Type: plugin.FieldText, Help: "Comma-separated referenced columns for a FOREIGN KEY."},
	}}}}
}

func userCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "User", Fields: []plugin.Field{
		{Key: "database", Label: "Database", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}, Help: "Database in which to create the user."},
		{Key: "name", Label: "User name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		{Key: "password", Label: "Password", Type: plugin.FieldPassword, Secret: true, Help: "Sets up a server login with this password; leave blank to create a user for an existing login."},
		{Key: "login", Label: "Existing login", Type: plugin.FieldText, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}, Help: "Map the user to this existing server login instead of creating one."},
	}}}}
}

func userGrantSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Grant", Fields: []plugin.Field{
		{Key: "permission", Label: "Permission", Type: plugin.FieldSelect, Required: true, Default: "SELECT", Options: []plugin.Option{
			{Label: "SELECT", Value: "SELECT"},
			{Label: "INSERT", Value: "INSERT"},
			{Label: "UPDATE", Value: "UPDATE"},
			{Label: "DELETE", Value: "DELETE"},
			{Label: "EXECUTE", Value: "EXECUTE"},
			{Label: "REFERENCES", Value: "REFERENCES"},
			{Label: "CONTROL", Value: "CONTROL"},
			{Label: "ALTER", Value: "ALTER"},
			{Label: "VIEW DEFINITION", Value: "VIEW DEFINITION"},
		}},
	}}}}
}

// treeDatabases → treeSchemas → treeRelations form the hierarchical drill-down
// (database → schema → table/view), TablePlus-style.
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
			Ref:            &plugin.ResourceIdentity{Kind: "database", Name: name, UID: name},
			ChildrenSource: &plugin.DataSource{RouteID: "mssql.schemas.tree", Params: map[string]string{"database": name}},
		})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

func treeSchemas(rc *plugin.RequestContext) (any, error) {
	res, err := listSchemas(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, r := range page.Items {
		ref, ok := r["ref"].(plugin.ResourceIdentity)
		if !ok {
			continue
		}
		database, name := fmt.Sprint(r["database"]), fmt.Sprint(r["name"])
		nodes = append(nodes, plugin.TreeNode{
			Key:            "schema:" + ref.UID,
			Label:          name,
			Icon:           icon("folder-tree"),
			Ref:            &ref,
			ChildrenSource: &plugin.DataSource{RouteID: "mssql.relations.tree", Params: map[string]string{"database": database, "schema": name}},
		})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

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

func treeProcedures(rc *plugin.RequestContext) (any, error) {
	return treeFromPage(rc, "procedure", "function-square", "name", listProcedures)
}

func treeUsers(rc *plugin.RequestContext) (any, error) {
	return treeFromPage(rc, "user", "user", "name", listUsers)
}

func treeJobs(rc *plugin.RequestContext) (any, error) {
	return treeFromPage(rc, "job", "calendar-clock", "name", listJobs)
}

func listDatabases(rc *plugin.RequestContext) (any, error) {
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT name, state_desc AS state, recovery_model_desc AS recovery, compatibility_level AS compatibility, create_date AS created
FROM sys.databases
WHERE database_id > 4 AND state_desc = 'ONLINE'
ORDER BY name`, nil)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		name := fmt.Sprint(r["name"])
		r["ref"] = plugin.ResourceIdentity{Kind: "database", Name: name, UID: name}
	}
	return pageRows(rc, rows)
}

func databaseOverview(rc *plugin.RequestContext) (any, error) {
	database, err := safeIdent(rc.Param("database"))
	if err != nil {
		return nil, err
	}
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT DB_NAME(database_id) AS name,
       SUM(size) * 8192 AS size,
       SUM(CASE WHEN type_desc = 'ROWS' THEN size ELSE 0 END) * 8192 AS data_size,
       SUM(CASE WHEN type_desc = 'LOG' THEN size ELSE 0 END) * 8192 AS log_size
FROM sys.master_files
WHERE database_id = DB_ID(@p1)
GROUP BY database_id`, []any{database})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, plugin.ErrNotFound
	}
	return rows[0], nil
}

func listSchemas(rc *plugin.RequestContext) (any, error) {
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	databases, err := targetDatabases(rc, s)
	if err != nil {
		return nil, err
	}
	out := []row{}
	for _, database := range databases {
		rows, err := queryRows(rc.Ctx, s, fmt.Sprintf(`
SELECT s.name, DB_NAME(DB_ID(%s)) AS [database], dp.name AS owner,
       SUM(CASE WHEN o.type = 'U' THEN 1 ELSE 0 END) AS tables,
       SUM(CASE WHEN o.type = 'V' THEN 1 ELSE 0 END) AS views
FROM %s.sys.schemas s
LEFT JOIN %s.sys.database_principals dp ON dp.principal_id = s.principal_id
LEFT JOIN %s.sys.objects o ON o.schema_id = s.schema_id
WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA')
GROUP BY s.name, dp.name
ORDER BY s.name`, quoteLiteral(database), quoteIdent(database), quoteIdent(database), quoteIdent(database)), nil)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			name := fmt.Sprint(r["name"])
			r["database"] = database
			r["ref"] = plugin.ResourceIdentity{Kind: "schema", Namespace: database, Name: name, UID: objectID(database, name, "")}
			out = append(out, r)
		}
	}
	return pageRows(rc, out)
}

func schemaOverview(rc *plugin.RequestContext) (any, error) {
	database, err := safeIdent(rc.Param("database"))
	if err != nil {
		return nil, err
	}
	schema, err := safeIdent(rc.Param("schema"))
	if err != nil {
		return nil, err
	}
	return row{"database": database, "schema": schema}, nil
}

func listTables(rc *plugin.RequestContext) (any, error) {
	return relationList(rc, "U", "table")
}

func relationGraph(rc *plugin.RequestContext) (any, error) {
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	databases, err := targetDatabases(rc, s)
	if err != nil {
		return nil, err
	}
	schema, err := optionalIdent(rc.Query().Get("p.schema"))
	if err != nil {
		return nil, err
	}
	columns := []sqldb.TableColumn{}
	fks := []sqldb.ForeignKey{}
	for _, database := range databases {
		colRows, err := queryRows(rc.Ctx, s, fmt.Sprintf(`
SELECT TABLE_SCHEMA AS table_schema, TABLE_NAME AS table_name, COLUMN_NAME AS column_name, DATA_TYPE AS data_type
FROM %[1]s.INFORMATION_SCHEMA.COLUMNS
WHERE (@p1 = '' OR TABLE_SCHEMA = @p1)
ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION`, quoteIdent(database)), []any{schema})
		if err != nil {
			return nil, err
		}
		for _, r := range colRows {
			columns = append(columns, sqldb.TableColumnFromRow(r))
		}
		fkRows, err := queryRows(rc.Ctx, s, fmt.Sprintf(`
SELECT fk.name AS constraint_name,
       cs.name AS child_schema, ct.name AS child_table, cc.name AS child_column,
       ps.name AS parent_schema, pt.name AS parent_table, pc.name AS parent_column
FROM %[1]s.sys.foreign_keys fk
JOIN %[1]s.sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
JOIN %[1]s.sys.tables ct ON ct.object_id = fkc.parent_object_id
JOIN %[1]s.sys.schemas cs ON cs.schema_id = ct.schema_id
JOIN %[1]s.sys.columns cc ON cc.object_id = fkc.parent_object_id AND cc.column_id = fkc.parent_column_id
JOIN %[1]s.sys.tables pt ON pt.object_id = fkc.referenced_object_id
JOIN %[1]s.sys.schemas ps ON ps.schema_id = pt.schema_id
JOIN %[1]s.sys.columns pc ON pc.object_id = fkc.referenced_object_id AND pc.column_id = fkc.referenced_column_id
WHERE (@p1 = '' OR cs.name = @p1)
ORDER BY fk.name, fkc.constraint_column_id`, quoteIdent(database)), []any{schema})
		if err != nil {
			return nil, err
		}
		for _, r := range fkRows {
			fks = append(fks, sqldb.ForeignKeyFromRow(r))
		}
	}
	return sqldb.RelationGraph(columns, fks), nil
}

func listViews(rc *plugin.RequestContext) (any, error) {
	return relationList(rc, "V", "view")
}

func relationList(rc *plugin.RequestContext, objectType string, refKind string) (any, error) {
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	databases, err := targetDatabases(rc, s)
	if err != nil {
		return nil, err
	}
	schema, err := optionalIdent(rc.Query().Get("p.schema"))
	if err != nil {
		return nil, err
	}
	out := []row{}
	for _, database := range databases {
		rows, err := queryRows(rc.Ctx, s, fmt.Sprintf(`
SELECT o.name, SCHEMA_NAME(o.schema_id) AS [schema], %s AS [database], o.create_date AS created, o.modify_date AS modified,
       COALESCE(SUM(ps.row_count), 0) AS [rows], COALESCE(SUM(ps.reserved_page_count), 0) * 8192 AS size
FROM %s.sys.objects o
LEFT JOIN %s.sys.dm_db_partition_stats ps ON ps.object_id = o.object_id AND ps.index_id IN (0,1)
WHERE o.type = @p1 AND (@p2 = '' OR SCHEMA_NAME(o.schema_id) = @p2)
GROUP BY o.name, o.schema_id, o.create_date, o.modify_date
ORDER BY SCHEMA_NAME(o.schema_id), o.name`, quoteLiteral(database), quoteIdent(database), quoteIdent(database)), []any{objectType, schema})
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			name, schemaName := fmt.Sprint(r["name"]), fmt.Sprint(r["schema"])
			r["ref"] = plugin.ResourceIdentity{Kind: refKind, Namespace: database, Name: quoteIdent(schemaName) + "." + quoteIdent(name), UID: objectID(database, schemaName, name)}
			out = append(out, r)
		}
	}
	return pageRows(rc, out)
}

func listProcedures(rc *plugin.RequestContext) (any, error) {
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	databases, err := targetDatabases(rc, s)
	if err != nil {
		return nil, err
	}
	schema, err := optionalIdent(rc.Query().Get("p.schema"))
	if err != nil {
		return nil, err
	}
	out := []row{}
	for _, database := range databases {
		rows, err := queryRows(rc.Ctx, s, fmt.Sprintf(`
SELECT o.name, SCHEMA_NAME(o.schema_id) AS [schema], %s AS [database], o.create_date AS created, o.modify_date AS modified
FROM %s.sys.objects o
WHERE o.type IN ('P', 'PC') AND (@p1 = '' OR SCHEMA_NAME(o.schema_id) = @p1)
ORDER BY SCHEMA_NAME(o.schema_id), o.name`, quoteLiteral(database), quoteIdent(database)), []any{schema})
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			name, schemaName := fmt.Sprint(r["name"]), fmt.Sprint(r["schema"])
			r["ref"] = plugin.ResourceIdentity{Kind: "procedure", Namespace: database, Name: quoteIdent(schemaName) + "." + quoteIdent(name), UID: objectID(database, schemaName, name)}
			out = append(out, r)
		}
	}
	return pageRows(rc, out)
}

func listUsers(rc *plugin.RequestContext) (any, error) {
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	databases, err := targetDatabases(rc, s)
	if err != nil {
		return nil, err
	}
	out := []row{}
	for _, database := range databases {
		rows, err := queryRows(rc.Ctx, s, fmt.Sprintf(`
SELECT dp.name, %s AS [database], dp.type_desc AS [type], sp.name AS [login], dp.create_date AS created
FROM %s.sys.database_principals dp
LEFT JOIN sys.server_principals sp ON sp.sid = dp.sid
WHERE dp.type IN ('S','U','G','E','X') AND dp.name NOT IN ('dbo','guest','INFORMATION_SCHEMA','sys')
ORDER BY dp.name`, quoteLiteral(database), quoteIdent(database)), nil)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			name := fmt.Sprint(r["name"])
			r["ref"] = plugin.ResourceIdentity{Kind: "user", Namespace: database, Name: name, UID: objectID(database, "", name)}
			out = append(out, r)
		}
	}
	return pageRows(rc, out)
}

func userOverview(rc *plugin.RequestContext) (any, error) {
	database, err := safeIdent(rc.Param("database"))
	if err != nil {
		return nil, err
	}
	user := strings.TrimSpace(rc.Param("user"))
	if user == "" {
		return nil, fmt.Errorf("%w: user is required", plugin.ErrInvalidInput)
	}
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, fmt.Sprintf(`
SELECT dp.name, %s AS [database], dp.type_desc AS [type], sp.name AS [login], dp.create_date AS created, dp.modify_date AS modified
FROM %s.sys.database_principals dp
LEFT JOIN sys.server_principals sp ON sp.sid = dp.sid
WHERE dp.name = @p1`, quoteLiteral(database), quoteIdent(database)), []any{user})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, plugin.ErrNotFound
	}
	return rows[0], nil
}

func listJobs(rc *plugin.RequestContext) (any, error) {
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT CONVERT(varchar(36), j.job_id) AS id, j.name, j.enabled, sp.name AS owner, j.date_created AS created, j.date_modified AS modified
FROM msdb.dbo.sysjobs j
LEFT JOIN sys.server_principals sp ON sp.sid = j.owner_sid
ORDER BY j.name`, nil)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		id, name := fmt.Sprint(r["id"]), fmt.Sprint(r["name"])
		r["ref"] = plugin.ResourceIdentity{Kind: "job", Name: name, UID: id}
	}
	return pageRows(rc, rows)
}

func jobOverview(rc *plugin.RequestContext) (any, error) {
	id := strings.TrimSpace(rc.Param("id"))
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT CONVERT(varchar(36), j.job_id) AS id, j.name, j.enabled, sp.name AS owner, j.description, j.date_created AS created, j.date_modified AS modified
FROM msdb.dbo.sysjobs j
LEFT JOIN sys.server_principals sp ON sp.sid = j.owner_sid
WHERE CONVERT(varchar(36), j.job_id) = @p1`, []any{id})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, plugin.ErrNotFound
	}
	return rows[0], nil
}

// jobName resolves the SQL Agent job name from the route param. SQL Agent job
// names allow arbitrary characters, so the value is never interpolated — it is
// passed as a parameter to the msdb stored procedures.
func jobName(rc *plugin.RequestContext) (string, error) {
	name := strings.TrimSpace(rc.Param("name"))
	if name == "" {
		return "", fmt.Errorf("%w: job name is required", plugin.ErrInvalidInput)
	}
	if strings.ContainsRune(name, '\x00') {
		return "", fmt.Errorf("%w: job name is invalid", plugin.ErrInvalidInput)
	}
	return name, nil
}

func startJob(rc *plugin.RequestContext) (any, error) {
	s, name, err := jobMutation(rc)
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(rc.Ctx, "EXEC msdb.dbo.sp_start_job @job_name = @job_name", sql.Named("job_name", name)); err != nil {
		return nil, mssqlErr(err)
	}
	return actionResult{OK: true}, nil
}

func enableJob(rc *plugin.RequestContext) (any, error) {
	return setJobEnabled(rc, true)
}

func disableJob(rc *plugin.RequestContext) (any, error) {
	return setJobEnabled(rc, false)
}

func setJobEnabled(rc *plugin.RequestContext, enabled bool) (any, error) {
	s, name, err := jobMutation(rc)
	if err != nil {
		return nil, err
	}
	flag := 0
	if enabled {
		flag = 1
	}
	if _, err := s.db.ExecContext(rc.Ctx, "EXEC msdb.dbo.sp_update_job @job_name = @job_name, @enabled = @enabled", sql.Named("job_name", name), sql.Named("enabled", flag)); err != nil {
		return nil, mssqlErr(err)
	}
	return actionResult{OK: true}, nil
}

func jobMutation(rc *plugin.RequestContext) (*Session, string, error) {
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, "", err
	}
	if err := ensureWritable(s); err != nil {
		return nil, "", err
	}
	name, err := jobName(rc)
	if err != nil {
		return nil, "", err
	}
	return s, name, nil
}

// createUser creates a database-scoped user. When a password is supplied it
// first provisions a matching server login (CREATE LOGIN) then maps a user to
// it; otherwise it creates a user for the named existing login, or a contained
// user without a login when neither is given.
func createUser(rc *plugin.RequestContext) (any, error) {
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Database string `json:"database" validate:"required"`
		Name     string `json:"name" validate:"required"`
		Password string `json:"password"`
		Login    string `json:"login"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	database, err := safeIdent(req.Database)
	if err != nil {
		return nil, err
	}
	name, err := safeIdent(req.Name)
	if err != nil {
		return nil, err
	}
	login := strings.TrimSpace(req.Login)
	password := strings.TrimSpace(req.Password)
	var userStmt string
	switch {
	case password != "":
		if _, err := s.db.ExecContext(rc.Ctx, "CREATE LOGIN "+quoteIdent(name)+" WITH PASSWORD = "+quoteLiteral(password)); err != nil {
			return nil, mssqlErr(err)
		}
		userStmt = "CREATE USER " + quoteIdent(name) + " FOR LOGIN " + quoteIdent(name)
	case login != "":
		loginName, err := safeIdent(login)
		if err != nil {
			return nil, err
		}
		userStmt = "CREATE USER " + quoteIdent(name) + " FOR LOGIN " + quoteIdent(loginName)
	default:
		userStmt = "CREATE USER " + quoteIdent(name) + " WITHOUT LOGIN"
	}
	// CREATE USER must run in the target database; wrap it in a USE-prefixed batch.
	stmt := "USE " + quoteIdent(database) + "; EXEC(" + quoteLiteral(userStmt) + ")"
	if _, err := s.db.ExecContext(rc.Ctx, stmt); err != nil {
		return nil, mssqlErr(err)
	}
	return actionResult{OK: true}, nil
}

func dropUser(rc *plugin.RequestContext) (any, error) {
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	database, err := safeIdent(rc.Param("database"))
	if err != nil {
		return nil, err
	}
	user, err := safeIdent(rc.Param("user"))
	if err != nil {
		return nil, err
	}
	stmt := "USE " + quoteIdent(database) + "; EXEC(" + quoteLiteral("DROP USER "+quoteIdent(user)) + ")"
	if _, err := s.db.ExecContext(rc.Ctx, stmt); err != nil {
		return nil, mssqlErr(err)
	}
	return actionResult{OK: true}, nil
}

func grantUser(rc *plugin.RequestContext) (any, error) {
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	database, err := safeIdent(rc.Param("database"))
	if err != nil {
		return nil, err
	}
	user, err := safeIdent(rc.Param("user"))
	if err != nil {
		return nil, err
	}
	var req struct {
		Permission string `json:"permission" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	permission, err := grantPermission(req.Permission)
	if err != nil {
		return nil, err
	}
	stmt := "USE " + quoteIdent(database) + "; EXEC(" + quoteLiteral("GRANT "+permission+" TO "+quoteIdent(user)) + ")"
	if _, err := s.db.ExecContext(rc.Ctx, stmt); err != nil {
		return nil, mssqlErr(err)
	}
	return actionResult{OK: true}, nil
}

// grantPermission validates a database-scoped permission against a fixed
// allow-list so the GRANT statement can never carry an injected clause.
func grantPermission(raw string) (string, error) {
	perm := strings.ToUpper(strings.TrimSpace(raw))
	switch perm {
	case "SELECT", "INSERT", "UPDATE", "DELETE", "EXECUTE", "REFERENCES", "CONTROL", "ALTER", "VIEW DEFINITION":
		return perm, nil
	default:
		return "", fmt.Errorf("%w: unsupported permission %q", plugin.ErrInvalidInput, raw)
	}
}

func tableRows(rc *plugin.RequestContext) (any, error) {
	database, schema, table, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	req, err := rc.Page()
	if err != nil {
		return nil, err
	}
	s, err := mssqlSession(rc)
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
		cols, err = columnNames(rc.Ctx, s, database, schema, table)
		if err != nil {
			return nil, err
		}
	}
	countClause, countArgs := dialect.SearchClause("NVARCHAR(MAX)", cols, filter, 1)
	countWhere := ""
	if countClause != "" {
		countWhere = " WHERE " + countClause
	}
	var total64 int64
	if err := s.db.QueryRowContext(rc.Ctx, "SELECT COUNT_BIG(*) FROM "+qualified(database, schema, table)+countWhere, countArgs...).Scan(&total64); err != nil {
		return nil, mssqlErr(err)
	}
	orderBy := " ORDER BY (SELECT NULL)"
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
	// Pagination keeps @p1/@p2; the search clause is numbered after it (@p3…).
	dataClause, dataSearch := dialect.SearchClause("NVARCHAR(MAX)", cols, filter, 3)
	dataWhere := ""
	if dataClause != "" {
		dataWhere = " WHERE " + dataClause
	}
	dataArgs := append([]any{offset, limit}, dataSearch...)
	rows, err := queryRows(rc.Ctx, s, fmt.Sprintf("SELECT * FROM %s%s%s OFFSET @p1 ROWS FETCH NEXT @p2 ROWS ONLY", qualified(database, schema, table), dataWhere, orderBy), dataArgs)
	if err != nil {
		return nil, err
	}
	pk, err := primaryKeyColumns(rc.Ctx, s, database, schema, table)
	if err != nil {
		return nil, err
	}
	attachRowKeys(rows, pk, s.opts.RedactPatterns)
	fks, err := foreignKeys(rc.Ctx, s, database, schema, table)
	if err != nil {
		return nil, err
	}
	attachForeignKeys(rows, fks)
	total := int(total64)
	redactRows(rows, s.opts.RedactPatterns)
	next := ""
	if offset+len(rows) < total {
		next = strconv.Itoa(offset + len(rows))
	}
	return plugin.Page[row]{Items: rows, NextCursor: next, Total: &total}, nil
}

// foreignKeys maps each FK column to the referenced table's ref, attached under
// the generic "_links" field the grid renders as links.
func foreignKeys(ctx context.Context, s *Session, database, schema, table string) (map[string]plugin.ResourceIdentity, error) {
	rows, err := queryRows(ctx, s, fmt.Sprintf(`
SELECT pc.name AS col, SCHEMA_NAME(rt.schema_id) AS ref_schema, rt.name AS ref_table
FROM %s.sys.foreign_keys fk
JOIN %s.sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
JOIN %s.sys.objects o ON o.object_id = fk.parent_object_id
JOIN %s.sys.schemas sc ON sc.schema_id = o.schema_id
JOIN %s.sys.columns pc ON pc.object_id = fk.parent_object_id AND pc.column_id = fkc.parent_column_id
JOIN %s.sys.objects rt ON rt.object_id = fk.referenced_object_id
WHERE sc.name = @p1 AND o.name = @p2`, quoteIdent(database), quoteIdent(database), quoteIdent(database), quoteIdent(database), quoteIdent(database), quoteIdent(database)), []any{schema, table})
	if err != nil {
		return nil, err
	}
	out := map[string]plugin.ResourceIdentity{}
	for _, r := range rows {
		col, refSchema, refTable := fmt.Sprint(r["col"]), fmt.Sprint(r["ref_schema"]), fmt.Sprint(r["ref_table"])
		out[col] = plugin.ResourceIdentity{Kind: "table", Namespace: database, Name: quoteIdent(refSchema) + "." + quoteIdent(refTable), UID: objectID(database, refSchema, refTable)}
	}
	return out, nil
}

func attachForeignKeys(rows []row, fks map[string]plugin.ResourceIdentity) {
	if len(fks) == 0 {
		return
	}
	for _, r := range rows {
		r["_links"] = fks
	}
}

func tableColumnsRoute(rc *plugin.RequestContext) (any, error) {
	database, schema, table, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, fmt.Sprintf(`
SELECT c.name, CONCAT(t.name, CASE WHEN t.name IN ('varchar','char','nvarchar','nchar','varbinary','binary') THEN CONCAT('(', CASE WHEN c.max_length = -1 THEN 'max' ELSE CONVERT(varchar(20), c.max_length) END, ')') ELSE '' END) AS [type],
       c.is_nullable AS nullable, c.is_identity AS [identity], dc.definition AS [default], c.column_id AS position
FROM %s.sys.columns c
JOIN %s.sys.objects o ON o.object_id = c.object_id
JOIN %s.sys.schemas s ON s.schema_id = o.schema_id
JOIN %s.sys.types t ON t.user_type_id = c.user_type_id
LEFT JOIN %s.sys.default_constraints dc ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
WHERE s.name = @p1 AND o.name = @p2
ORDER BY c.column_id`, quoteIdent(database), quoteIdent(database), quoteIdent(database), quoteIdent(database), quoteIdent(database)), []any{schema, table})
	if err != nil {
		return nil, err
	}
	id := rc.Param("id")
	for i := range rows {
		rows[i]["id"] = id
	}
	return pageRows(rc, rows)
}

func tableIndexes(rc *plugin.RequestContext) (any, error) {
	database, schema, table, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, fmt.Sprintf(`
SELECT i.name, STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns,
       i.is_unique AS [unique], i.is_primary_key AS [primary], i.type_desc AS [type]
FROM %s.sys.indexes i
JOIN %s.sys.objects o ON o.object_id = i.object_id
JOIN %s.sys.schemas s ON s.schema_id = o.schema_id
LEFT JOIN %s.sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
LEFT JOIN %s.sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE i.name IS NOT NULL AND s.name = @p1 AND o.name = @p2
GROUP BY i.name, i.is_unique, i.is_primary_key, i.type_desc
ORDER BY i.name`, quoteIdent(database), quoteIdent(database), quoteIdent(database), quoteIdent(database), quoteIdent(database)), []any{schema, table})
	if err != nil {
		return nil, err
	}
	id := rc.Param("id")
	for i := range rows {
		rows[i]["id"] = id
	}
	return pageRows(rc, rows)
}

func tableConstraints(rc *plugin.RequestContext) (any, error) {
	database, schema, table, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, fmt.Sprintf(`
SELECT kc.name, kc.type_desc AS [type], c.name AS [column], NULL AS referenced
FROM %s.sys.key_constraints kc
JOIN %s.sys.objects o ON o.object_id = kc.parent_object_id
JOIN %s.sys.schemas s ON s.schema_id = o.schema_id
LEFT JOIN %s.sys.index_columns ic ON ic.object_id = kc.parent_object_id AND ic.index_id = kc.unique_index_id
LEFT JOIN %s.sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE s.name = @p1 AND o.name = @p2
UNION ALL
SELECT fk.name, fk.type_desc, pc.name, CONCAT(rs.name, '.', ro.name, '.', rc.name)
FROM %s.sys.foreign_keys fk
JOIN %s.sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
JOIN %s.sys.objects po ON po.object_id = fk.parent_object_id
JOIN %s.sys.schemas ps ON ps.schema_id = po.schema_id
JOIN %s.sys.columns pc ON pc.object_id = po.object_id AND pc.column_id = fkc.parent_column_id
JOIN %s.sys.objects ro ON ro.object_id = fk.referenced_object_id
JOIN %s.sys.schemas rs ON rs.schema_id = ro.schema_id
JOIN %s.sys.columns rc ON rc.object_id = ro.object_id AND rc.column_id = fkc.referenced_column_id
WHERE ps.name = @p1 AND po.name = @p2`, quoteIdent(database), quoteIdent(database), quoteIdent(database), quoteIdent(database), quoteIdent(database), quoteIdent(database), quoteIdent(database), quoteIdent(database), quoteIdent(database), quoteIdent(database), quoteIdent(database), quoteIdent(database), quoteIdent(database)), []any{schema, table})
	if err != nil {
		return nil, err
	}
	id := rc.Param("id")
	for i := range rows {
		rows[i]["id"] = id
	}
	return pageRows(rc, rows)
}

func objectDefinition(rc *plugin.RequestContext) (any, error) {
	database, schema, name, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, fmt.Sprintf(`
SELECT m.definition
FROM %s.sys.sql_modules m
JOIN %s.sys.objects o ON o.object_id = m.object_id
JOIN %s.sys.schemas s ON s.schema_id = o.schema_id
WHERE s.name = @p1 AND o.name = @p2`, quoteIdent(database), quoteIdent(database), quoteIdent(database)), []any{schema, name})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, plugin.ErrNotFound
	}
	return rows[0], nil
}

func completionRoute(rc *plugin.RequestContext) (any, error) {
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	items := []sqldb.CompletionItem{
		{Label: "SELECT", Type: "keyword"},
		{Label: "FROM", Type: "keyword"},
		{Label: "WHERE", Type: "keyword"},
		{Label: "ORDER BY", Type: "keyword"},
		{Label: "TOP", Type: "keyword"},
		{Label: "INSERT", Type: "keyword"},
		{Label: "UPDATE", Type: "keyword"},
		{Label: "DELETE", Type: "keyword"},
		{Label: "CREATE TABLE", Type: "keyword"},
		{Label: "ALTER TABLE", Type: "keyword"},
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT TOP (500) TABLE_SCHEMA AS [schema], TABLE_NAME AS [table], COLUMN_NAME AS [column]
FROM INFORMATION_SCHEMA.COLUMNS
ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION`, nil)
	if err == nil {
		seen := map[string]bool{}
		for _, r := range rows {
			for _, item := range []sqldb.CompletionItem{
				{Label: fmt.Sprint(r["schema"]), Type: "namespace", Detail: "schema"},
				{Label: fmt.Sprint(r["table"]), Type: "table", Detail: fmt.Sprint(r["schema"])},
				{Label: fmt.Sprint(r["column"]), Type: "property", Detail: fmt.Sprint(r["schema"]) + "." + fmt.Sprint(r["table"])},
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

func createDatabase(rc *plugin.RequestContext) (any, error) {
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Name string `json:"name" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name, err := safeIdent(req.Name)
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(rc.Ctx, "CREATE DATABASE "+quoteIdent(name)); err != nil {
		return nil, mssqlErr(err)
	}
	return actionResult{OK: true}, nil
}

func createTable(rc *plugin.RequestContext) (any, error) {
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	database, err := safeIdent(rc.Param("database"))
	if err != nil {
		return nil, err
	}
	schema, err := safeIdent(rc.Param("schema"))
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
	if _, err := s.db.ExecContext(rc.Ctx, "CREATE TABLE "+qualified(database, schema, table)+" ("+strings.Join(columns, ", ")+")"); err != nil {
		return nil, mssqlErr(err)
	}
	return actionResult{OK: true}, nil
}

func addColumn(rc *plugin.RequestContext) (any, error) {
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	database, schema, table, err := objectIdent(rc)
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
	if _, err := s.db.ExecContext(rc.Ctx, "ALTER TABLE "+qualified(database, schema, table)+" ADD "+column); err != nil {
		return nil, mssqlErr(err)
	}
	return actionResult{OK: true}, nil
}

func dropColumn(rc *plugin.RequestContext) (any, error) {
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	database, schema, table, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	column, err := safeIdent(rc.Param("name"))
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(rc.Ctx, "ALTER TABLE "+qualified(database, schema, table)+" DROP COLUMN "+quoteIdent(column)); err != nil {
		return nil, mssqlErr(err)
	}
	return actionResult{OK: true}, nil
}

func dropDatabase(rc *plugin.RequestContext) (any, error) {
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	name, err := safeIdent(rc.Param("database"))
	if err != nil {
		return nil, err
	}
	if strings.EqualFold(name, s.opts.Database) {
		return nil, fmt.Errorf("%w: cannot drop the connected database", plugin.ErrForbidden)
	}
	// SQL Server refuses DROP DATABASE while sessions hold it; force it offline
	// (rolling back open transactions) first so the drop succeeds cleanly.
	if _, err := s.db.ExecContext(rc.Ctx, "ALTER DATABASE "+quoteIdent(name)+" SET SINGLE_USER WITH ROLLBACK IMMEDIATE"); err != nil {
		return nil, mssqlErr(err)
	}
	if _, err := s.db.ExecContext(rc.Ctx, "DROP DATABASE "+quoteIdent(name)); err != nil {
		return nil, mssqlErr(err)
	}
	return actionResult{OK: true}, nil
}

func createSchema(rc *plugin.RequestContext) (any, error) {
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	database, err := safeIdent(rc.Param("database"))
	if err != nil {
		return nil, err
	}
	var req struct {
		Name string `json:"name" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name, err := safeIdent(req.Name)
	if err != nil {
		return nil, err
	}
	// CREATE SCHEMA must be the first statement in its batch, so it is run in the
	// target database via a USE-prefixed batch rather than three-part naming.
	stmt := "USE " + quoteIdent(database) + "; EXEC(" + quoteLiteral("CREATE SCHEMA "+quoteIdent(name)) + ")"
	if _, err := s.db.ExecContext(rc.Ctx, stmt); err != nil {
		return nil, mssqlErr(err)
	}
	return actionResult{OK: true}, nil
}

func dropSchema(rc *plugin.RequestContext) (any, error) {
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	database, err := safeIdent(rc.Param("database"))
	if err != nil {
		return nil, err
	}
	schema, err := safeIdent(rc.Param("schema"))
	if err != nil {
		return nil, err
	}
	stmt := "USE " + quoteIdent(database) + "; EXEC(" + quoteLiteral("DROP SCHEMA "+quoteIdent(schema)) + ")"
	if _, err := s.db.ExecContext(rc.Ctx, stmt); err != nil {
		return nil, mssqlErr(err)
	}
	return actionResult{OK: true}, nil
}

func renameTable(rc *plugin.RequestContext) (any, error) {
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	database, schema, table, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		Name string `json:"name" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	newName, err := safeIdent(req.Name)
	if err != nil {
		return nil, err
	}
	// sp_rename takes the qualified current name and the bare new name; the new
	// name is passed literally so it can never carry separators.
	current := quoteLiteral(schema + "." + table)
	target := quoteLiteral(newName)
	stmt := "USE " + quoteIdent(database) + "; EXEC sp_rename " + current + ", " + target
	if _, err := s.db.ExecContext(rc.Ctx, stmt); err != nil {
		return nil, mssqlErr(err)
	}
	return actionResult{OK: true}, nil
}

func alterColumn(rc *plugin.RequestContext) (any, error) {
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	database, schema, table, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		Name     any    `json:"name" validate:"required"`
		Type     string `json:"type" validate:"required"`
		Nullable bool   `json:"nullable"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	column, err := singleIdentValue(req.Name)
	if err != nil {
		return nil, err
	}
	dataType := strings.TrimSpace(req.Type)
	if !sqldb.SafeType(dataType) {
		return nil, fmt.Errorf("%w: unsafe column type", plugin.ErrInvalidInput)
	}
	null := "NOT NULL"
	if req.Nullable {
		null = "NULL"
	}
	stmt := "ALTER TABLE " + qualified(database, schema, table) + " ALTER COLUMN " + quoteIdent(column) + " " + dataType + " " + null
	if _, err := s.db.ExecContext(rc.Ctx, stmt); err != nil {
		return nil, mssqlErr(err)
	}
	return actionResult{OK: true}, nil
}

func addConstraint(rc *plugin.RequestContext) (any, error) {
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	database, schema, table, err := objectIdent(rc)
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
	stmt := "ALTER TABLE " + qualified(database, schema, table) + " ADD " + clause
	if _, err := s.db.ExecContext(rc.Ctx, stmt); err != nil {
		return nil, mssqlErr(err)
	}
	return actionResult{OK: true}, nil
}

func dropConstraint(rc *plugin.RequestContext) (any, error) {
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	database, schema, table, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	name, err := safeIdent(rc.Param("name"))
	if err != nil {
		return nil, err
	}
	stmt := "ALTER TABLE " + qualified(database, schema, table) + " DROP CONSTRAINT " + quoteIdent(name)
	if _, err := s.db.ExecContext(rc.Ctx, stmt); err != nil {
		return nil, mssqlErr(err)
	}
	return actionResult{OK: true}, nil
}

func createIndex(rc *plugin.RequestContext) (any, error) {
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	database, schema, table, err := objectIdent(rc)
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
	stmt := "CREATE " + unique + "INDEX " + quoteIdent(name) + " ON " + qualified(database, schema, table) + " (" + strings.Join(cols, ", ") + ")"
	if _, err := s.db.ExecContext(rc.Ctx, stmt); err != nil {
		return nil, mssqlErr(err)
	}
	return actionResult{OK: true}, nil
}

func dropIndex(rc *plugin.RequestContext) (any, error) {
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	database, schema, table, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	name, err := safeIdent(rc.Param("name"))
	if err != nil {
		return nil, err
	}
	// SQL Server drops an index relative to its table: DROP INDEX name ON table.
	if _, err := s.db.ExecContext(rc.Ctx, "DROP INDEX "+quoteIdent(name)+" ON "+qualified(database, schema, table)); err != nil {
		return nil, mssqlErr(err)
	}
	return actionResult{OK: true}, nil
}

func insertRow(rc *plugin.RequestContext) (any, error) {
	s, database, schema, table, m, err := rowMutationInput(rc)
	if err != nil {
		return nil, err
	}
	stmt, args, err := dialect.Insert(qualified(database, schema, table), m.Values)
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(rc.Ctx, stmt, args...); err != nil {
		return nil, mssqlErr(err)
	}
	return actionResult{OK: true}, nil
}

func updateRow(rc *plugin.RequestContext) (any, error) {
	return keyedRowMutation(rc, false)
}

func deleteRow(rc *plugin.RequestContext) (any, error) {
	return keyedRowMutation(rc, true)
}

func rowMutationInput(rc *plugin.RequestContext) (*Session, string, string, string, sqldb.RowMutation, error) {
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, "", "", "", sqldb.RowMutation{}, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, "", "", "", sqldb.RowMutation{}, err
	}
	database, schema, table, err := objectIdent(rc)
	if err != nil {
		return nil, "", "", "", sqldb.RowMutation{}, err
	}
	var m sqldb.RowMutation
	if err := rc.Bind(&m); err != nil {
		return nil, "", "", "", sqldb.RowMutation{}, err
	}
	return s, database, schema, table, m, nil
}

// keyedRowMutation runs an UPDATE or DELETE only after confirming the client's
// key is exactly the table's primary key and that it affects a single row.
func keyedRowMutation(rc *plugin.RequestContext, del bool) (any, error) {
	s, database, schema, table, m, err := rowMutationInput(rc)
	if err != nil {
		return nil, err
	}
	pk, err := primaryKeyColumns(rc.Ctx, s, database, schema, table)
	if err != nil {
		return nil, err
	}
	if err := sqldb.ValidateRowKey(pk, m.Key); err != nil {
		return nil, err
	}
	qual := qualified(database, schema, table)
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
		return nil, mssqlErr(err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return nil, fmt.Errorf("%w: row no longer matches (it may have changed)", plugin.ErrNotFound)
	}
	return actionResult{OK: true}, nil
}

// columnNames returns a table's column names in order, for the data grid's
// free-text search across every column.
func columnNames(ctx context.Context, s *Session, database, schema, table string) ([]string, error) {
	rows, err := queryRows(ctx, s, fmt.Sprintf(`
SELECT c.name AS name
FROM %s.sys.columns c
JOIN %s.sys.objects o ON o.object_id = c.object_id
JOIN %s.sys.schemas sc ON sc.schema_id = o.schema_id
WHERE sc.name = @p1 AND o.name = @p2
ORDER BY c.column_id`, quoteIdent(database), quoteIdent(database), quoteIdent(database)), []any{schema, table})
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, fmt.Sprint(r["name"]))
	}
	return out, nil
}

func primaryKeyColumns(ctx context.Context, s *Session, database, schema, table string) ([]string, error) {
	rows, err := queryRows(ctx, s, fmt.Sprintf(`
SELECT c.name AS name
FROM %s.sys.indexes i
JOIN %s.sys.objects o ON o.object_id = i.object_id
JOIN %s.sys.schemas sc ON sc.schema_id = o.schema_id
JOIN %s.sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
JOIN %s.sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE i.is_primary_key = 1 AND sc.name = @p1 AND o.name = @p2
ORDER BY ic.key_ordinal`, quoteIdent(database), quoteIdent(database), quoteIdent(database), quoteIdent(database), quoteIdent(database)), []any{schema, table})
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
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
	database, schema, table, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	return execDDL(rc, "TRUNCATE TABLE "+qualified(database, schema, table))
}

func dropTable(rc *plugin.RequestContext) (any, error) {
	database, schema, table, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	return execDDL(rc, "DROP TABLE "+qualified(database, schema, table))
}

func dropView(rc *plugin.RequestContext) (any, error) {
	database, schema, view, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	return execDDL(rc, "DROP VIEW "+qualified(database, schema, view))
}

func execDDL(rc *plugin.RequestContext, sqlText string) (any, error) {
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(rc.Ctx, sqlText); err != nil {
		return nil, mssqlErr(err)
	}
	return actionResult{OK: true}, nil
}

func cancelQuery(rc *plugin.RequestContext) (any, error) {
	s, err := mssqlSession(rc)
	if err != nil {
		return nil, err
	}
	return actionResult{OK: s.cancelAll()}, nil
}

func queryStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := mssqlSession(rc)
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
		database := stringDefault(rc.Param("database"), s.opts.Database)
		statements := sqldb.SplitStatements(req.Query)
		result, err := executeQueryRequest(client.Context(), s, database, req)
		rc.Audit(queryAuditResult(err), sqldb.AuditParams(sqldb.QueryAudit{
			Query: req.Query, Statements: statements, Confirmed: req.Confirm, ReadOnlyMode: s.opts.ReadOnly,
			RequiresReview: statementsRequireReview(statements), RowCount: result.RowCount, ElapsedMS: result.ElapsedMS, CommandTag: result.CommandTag,
		}), err)
		if err != nil {
			payload := map[string]any{"error": err.Error()}
			var confirmErr confirmationError
			if errors.As(err, &confirmErr) {
				payload["requiresConfirmation"] = true
				payload["confirmMessage"] = "This T-SQL statement can change data, schema, or privileges. Review it before running."
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

func executeQueryRequest(parent context.Context, s *Session, database string, req sqldb.QueryRequest) (sqldb.QueryResult, error) {
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
		return sqldb.QueryResult{}, mssqlErr(err)
	}
	defer func() { _ = conn.Close() }()
	if database != "" {
		if _, err := conn.ExecContext(ctx, "USE "+quoteIdent(database)); err != nil {
			return sqldb.QueryResult{}, mssqlErr(err)
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
			return sqldb.StatementResult{}, mssqlErr(err)
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
		return sqldb.StatementResult{}, mssqlErr(err)
	}
	columns, err := rows.Columns()
	if err != nil {
		_ = rows.Close()
		return sqldb.StatementResult{}, mssqlErr(err)
	}
	out := sqldb.StatementResult{Statement: statement, Columns: columns}
	for rows.Next() {
		values, err := scanValues(rows, columns)
		if err != nil {
			_ = rows.Close()
			return sqldb.StatementResult{}, mssqlErr(err)
		}
		out.Rows = append(out.Rows, values)
		if len(out.Rows) >= s.opts.RowLimit {
			break
		}
	}
	if err := rows.Close(); err != nil {
		return sqldb.StatementResult{}, mssqlErr(err)
	}
	if err := rows.Err(); err != nil {
		return sqldb.StatementResult{}, mssqlErr(err)
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
		return nil, mssqlErr(err)
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		return nil, mssqlErr(err)
	}
	out := []row{}
	for rows.Next() {
		values, err := scanValues(rows, columns)
		if err != nil {
			return nil, mssqlErr(err)
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
		return nil, mssqlErr(err)
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
	case "SELECT", "WITH", "VALUES", "DECLARE":
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
		ref, _ := r["ref"].(plugin.ResourceIdentity)
		if ref.Kind == "" {
			continue
		}
		label := fmt.Sprint(r[labelKey])
		if database := fmt.Sprint(r["database"]); database != "" && database != "<nil>" && kind != "database" {
			if schema := fmt.Sprint(r["schema"]); schema != "" && schema != "<nil>" && schema != label {
				label = database + "." + schema + "." + label
			} else {
				label = database + "." + label
			}
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

func targetDatabases(rc *plugin.RequestContext, s *Session) ([]string, error) {
	if database, err := optionalIdent(rc.Query().Get("p.database")); err != nil {
		return nil, err
	} else if database != "" {
		return []string{database}, nil
	}
	res, err := listDatabases(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	out := make([]string, 0, len(page.Items))
	for _, r := range page.Items {
		out = append(out, fmt.Sprint(r["name"]))
	}
	if len(out) == 0 && s.opts.Database != "" {
		out = append(out, s.opts.Database)
	}
	return out, nil
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
	} else {
		parts = append(parts, "NULL")
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

func objectIdent(rc *plugin.RequestContext) (string, string, string, error) {
	return parseObjectID(rc.Param("id"))
}

func objectID(database, schema, name string) string {
	return database + ":" + schema + ":" + name
}

func parseObjectID(id string) (string, string, string, error) {
	parts := strings.Split(id, ":")
	if len(parts) != 3 {
		return "", "", "", fmt.Errorf("%w: object id is invalid", plugin.ErrInvalidInput)
	}
	database, err := safeIdent(parts[0])
	if err != nil {
		return "", "", "", err
	}
	schema, err := optionalIdent(parts[1])
	if err != nil {
		return "", "", "", err
	}
	name, err := safeIdent(parts[2])
	if err != nil {
		return "", "", "", err
	}
	return database, schema, name, nil
}

func safeIdent(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("%w: identifier is required", plugin.ErrInvalidInput)
	}
	if strings.ContainsAny(name, "\x00[]:") {
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

func qualified(database, schema, name string) string {
	return quoteIdent(database) + "." + quoteIdent(schema) + "." + quoteIdent(name)
}

func quoteIdent(s string) string {
	return "[" + strings.ReplaceAll(s, "]", "]]") + "]"
}

func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// singleIdentValue accepts a single identifier that the generic form may submit
// either as a bare string or as a one-element multiselect array.
func singleIdentValue(v any) (string, error) {
	switch t := v.(type) {
	case string:
		return safeIdent(t)
	case []any:
		if len(t) != 1 {
			return "", fmt.Errorf("%w: exactly one column is required", plugin.ErrInvalidInput)
		}
		return safeIdent(fmt.Sprint(t[0]))
	case []string:
		if len(t) != 1 {
			return "", fmt.Errorf("%w: exactly one column is required", plugin.ErrInvalidInput)
		}
		return safeIdent(t[0])
	default:
		return "", fmt.Errorf("%w: a column is required", plugin.ErrInvalidInput)
	}
}

// constraintClause builds the "CONSTRAINT name <body>" fragment for ALTER TABLE
// ADD across the four structural constraint kinds, validating every identifier
// and rejecting unsafe CHECK expressions.
func constraintClause(name, kind string, columns any, check, refTable, refColumns string) (string, error) {
	cname, err := safeIdent(name)
	if err != nil {
		return "", err
	}
	prefix := "CONSTRAINT " + quoteIdent(cname) + " "
	switch strings.ToUpper(strings.TrimSpace(kind)) {
	case "PRIMARY KEY", "UNIQUE":
		cols, err := sqldb.IdentifierListValue(columns, quoteIdent)
		if err != nil {
			return "", err
		}
		if len(cols) == 0 {
			return "", fmt.Errorf("%w: at least one column is required", plugin.ErrInvalidInput)
		}
		body := "PRIMARY KEY"
		if strings.EqualFold(kind, "UNIQUE") {
			body = "UNIQUE"
		}
		return prefix + body + " (" + strings.Join(cols, ", ") + ")", nil
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
		ref, err := refTableClause(refTable)
		if err != nil {
			return "", err
		}
		refCols, err := sqldb.IdentifierListValue(refColumns, quoteIdent)
		if err != nil {
			return "", err
		}
		if len(refCols) == 0 {
			return "", fmt.Errorf("%w: at least one referenced column is required", plugin.ErrInvalidInput)
		}
		return prefix + "FOREIGN KEY (" + strings.Join(cols, ", ") + ") REFERENCES " + ref + " (" + strings.Join(refCols, ", ") + ")", nil
	default:
		return "", fmt.Errorf("%w: unsupported constraint type %q", plugin.ErrInvalidInput, kind)
	}
}

// refTableClause quotes a (optionally schema-qualified) referenced table name.
func refTableClause(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("%w: a referenced table is required", plugin.ErrInvalidInput)
	}
	parts := strings.Split(raw, ".")
	if len(parts) > 2 {
		return "", fmt.Errorf("%w: referenced table is invalid", plugin.ErrInvalidInput)
	}
	quoted := make([]string, 0, len(parts))
	for _, p := range parts {
		ident, err := safeIdent(p)
		if err != nil {
			return "", err
		}
		quoted = append(quoted, quoteIdent(ident))
	}
	return strings.Join(quoted, "."), nil
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

func mssqlErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return plugin.ErrNotFound
	}
	return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
}
