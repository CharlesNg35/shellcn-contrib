package cockroachdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

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

var dialect = sqldb.Dialect{QuoteIdent: sqldb.QuoteIdent, Placeholder: sqldb.DollarPlaceholder}

func routes() []plugin.Route {
	return []plugin.Route{
		{ID: "cockroachdb.databases.tree", Method: plugin.MethodGet, Path: "/tree/databases", Permission: "cockroachdb.databases.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.databases.tree", Handle: treeDatabases},
		{ID: "cockroachdb.databases.list", Method: plugin.MethodGet, Path: "/databases", Permission: "cockroachdb.databases.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.databases.list", Handle: listDatabases},
		{ID: "cockroachdb.database.overview", Method: plugin.MethodGet, Path: "/databases/{database}/overview", Permission: "cockroachdb.databases.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.database.overview", Handle: databaseOverview},
		{ID: "cockroachdb.nodes.tree", Method: plugin.MethodGet, Path: "/tree/nodes", Permission: "cockroachdb.nodes.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.nodes.tree", Handle: treeNodes},
		{ID: "cockroachdb.nodes.list", Method: plugin.MethodGet, Path: "/nodes", Permission: "cockroachdb.nodes.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.nodes.list", Handle: listNodes},
		{ID: "cockroachdb.node.overview", Method: plugin.MethodGet, Path: "/nodes/{node}/overview", Permission: "cockroachdb.nodes.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.node.overview", Handle: nodeOverview},
		{ID: "cockroachdb.ranges.tree", Method: plugin.MethodGet, Path: "/tree/ranges", Permission: "cockroachdb.ranges.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.ranges.tree", Handle: treeRanges},
		{ID: "cockroachdb.ranges.list", Method: plugin.MethodGet, Path: "/ranges", Permission: "cockroachdb.ranges.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.ranges.list", Handle: listRanges},
		{ID: "cockroachdb.range.overview", Method: plugin.MethodGet, Path: "/ranges/{range}/overview", Permission: "cockroachdb.ranges.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.range.overview", Handle: rangeOverview},
		{ID: "cockroachdb.jobs.tree", Method: plugin.MethodGet, Path: "/tree/jobs", Permission: "cockroachdb.jobs.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.jobs.tree", Handle: treeJobs},
		{ID: "cockroachdb.jobs.list", Method: plugin.MethodGet, Path: "/jobs", Permission: "cockroachdb.jobs.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.jobs.list", Handle: listJobs},
		{ID: "cockroachdb.job.overview", Method: plugin.MethodGet, Path: "/jobs/{job}/overview", Permission: "cockroachdb.jobs.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.job.overview", Handle: jobOverview},
		{ID: "cockroachdb.sessions.tree", Method: plugin.MethodGet, Path: "/tree/sessions", Permission: "cockroachdb.sessions.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.sessions.tree", Handle: treeSessions},
		{ID: "cockroachdb.sessions.list", Method: plugin.MethodGet, Path: "/sessions", Permission: "cockroachdb.sessions.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.sessions.list", Handle: listSessions},
		{ID: "cockroachdb.session.overview", Method: plugin.MethodGet, Path: "/sessions/{session}/overview", Permission: "cockroachdb.sessions.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.session.overview", Handle: sessionOverview},
		{ID: "cockroachdb.queries.tree", Method: plugin.MethodGet, Path: "/tree/queries", Permission: "cockroachdb.queries.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.queries.tree", Handle: treeQueries},
		{ID: "cockroachdb.queries.list", Method: plugin.MethodGet, Path: "/queries", Permission: "cockroachdb.queries.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.queries.list", Handle: listQueries},
		{ID: "cockroachdb.query.overview", Method: plugin.MethodGet, Path: "/queries/{query}/overview", Permission: "cockroachdb.queries.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.query.overview", Handle: queryOverview},
		{ID: "cockroachdb.schemas.tree", Method: plugin.MethodGet, Path: "/tree/schemas", Permission: "cockroachdb.schemas.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.schemas.tree", Handle: schemaTree},
		{ID: "cockroachdb.schemas.list", Method: plugin.MethodGet, Path: "/schemas", Permission: "cockroachdb.schemas.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.schemas.list", Handle: listSchemas},
		{ID: "cockroachdb.schema.overview", Method: plugin.MethodGet, Path: "/schemas/{schema}/overview", Permission: "cockroachdb.schemas.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.schema.overview", Handle: schemaOverview},
		{ID: "cockroachdb.tables.tree", Method: plugin.MethodGet, Path: "/tree/tables", Permission: "cockroachdb.tables.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.tables.tree", Handle: treeTables},
		{ID: "cockroachdb.tables.list", Method: plugin.MethodGet, Path: "/tables", Permission: "cockroachdb.tables.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.tables.list", Handle: listTables},
		{ID: "cockroachdb.relations.graph", Method: plugin.MethodGet, Path: "/relations/graph", Permission: "cockroachdb.tables.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.relations.graph", Handle: relationGraph},
		{ID: "cockroachdb.views.tree", Method: plugin.MethodGet, Path: "/tree/views", Permission: "cockroachdb.views.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.views.tree", Handle: treeViews},
		{ID: "cockroachdb.views.list", Method: plugin.MethodGet, Path: "/views", Permission: "cockroachdb.views.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.views.list", Handle: listViews},
		{ID: "cockroachdb.view.drop", Method: plugin.MethodDelete, Path: "/views/{schema}/{view}", Permission: "cockroachdb.views.delete", Risk: plugin.RiskDestructive, AuditEvent: "cockroachdb.view.drop", Handle: dropView},
		{ID: "cockroachdb.functions.tree", Method: plugin.MethodGet, Path: "/tree/functions", Permission: "cockroachdb.functions.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.functions.tree", Handle: treeFunctions},
		{ID: "cockroachdb.functions.list", Method: plugin.MethodGet, Path: "/functions", Permission: "cockroachdb.functions.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.functions.list", Handle: listFunctions},
		{ID: "cockroachdb.sequences.list", Method: plugin.MethodGet, Path: "/sequences", Permission: "cockroachdb.sequences.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.sequences.list", Handle: listSequences},
		{ID: "cockroachdb.table.rows", Method: plugin.MethodGet, Path: "/tables/{schema}/{table}/rows", Permission: "cockroachdb.tables.data.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.table.rows", Handle: tableRows},
		{ID: "cockroachdb.view.rows", Method: plugin.MethodGet, Path: "/views/{schema}/{table}/rows", Permission: "cockroachdb.views.data.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.view.rows", Handle: tableRows},
		{ID: "cockroachdb.table.columns", Method: plugin.MethodGet, Path: "/tables/{schema}/{table}/columns", Permission: "cockroachdb.tables.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.table.columns", Handle: tableColumnsRoute},
		{ID: "cockroachdb.table.indexes", Method: plugin.MethodGet, Path: "/tables/{schema}/{table}/indexes", Permission: "cockroachdb.tables.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.table.indexes", Handle: tableIndexes},
		{ID: "cockroachdb.table.constraints", Method: plugin.MethodGet, Path: "/tables/{schema}/{table}/constraints", Permission: "cockroachdb.tables.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.table.constraints", Handle: tableConstraints},
		{ID: "cockroachdb.view.definition", Method: plugin.MethodGet, Path: "/views/{schema}/{table}/definition", Permission: "cockroachdb.views.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.view.definition", Handle: viewDefinition},
		{ID: "cockroachdb.function.definition", Method: plugin.MethodGet, Path: "/functions/{id}/definition", Permission: "cockroachdb.functions.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.function.definition", Handle: functionDefinition},
		{ID: "cockroachdb.sequence.overview", Method: plugin.MethodGet, Path: "/sequences/{schema}/{table}/overview", Permission: "cockroachdb.sequences.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.sequence.overview", Handle: sequenceOverview},
		{ID: "cockroachdb.completion", Method: plugin.MethodGet, Path: "/completion", Permission: "cockroachdb.schemas.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.completion", Handle: completionRoute},
		{ID: "cockroachdb.table.row.insert", Method: plugin.MethodPost, Path: "/tables/{schema}/{table}/rows", Permission: "cockroachdb.tables.data.write", Risk: plugin.RiskWrite, AuditEvent: "cockroachdb.table.row.insert", Handle: insertRow},
		{ID: "cockroachdb.table.row.update", Method: plugin.MethodPatch, Path: "/tables/{schema}/{table}/rows", Permission: "cockroachdb.tables.data.write", Risk: plugin.RiskWrite, AuditEvent: "cockroachdb.table.row.update", Handle: updateRow},
		{ID: "cockroachdb.table.row.delete", Method: plugin.MethodDelete, Path: "/tables/{schema}/{table}/rows", Permission: "cockroachdb.tables.data.delete", Risk: plugin.RiskDestructive, AuditEvent: "cockroachdb.table.row.delete", Handle: deleteRow},
		{ID: "cockroachdb.database.create", Method: plugin.MethodPost, Path: "/databases", Permission: "cockroachdb.databases.write", Risk: plugin.RiskWrite, AuditEvent: "cockroachdb.database.create", Input: databaseCreateSchema(), Handle: createDatabase},
		{ID: "cockroachdb.database.drop", Method: plugin.MethodDelete, Path: "/databases/{database}", Permission: "cockroachdb.databases.delete", Risk: plugin.RiskDestructive, AuditEvent: "cockroachdb.database.drop", Handle: dropDatabase},
		{ID: "cockroachdb.schema.create", Method: plugin.MethodPost, Path: "/schemas", Permission: "cockroachdb.schemas.write", Risk: plugin.RiskWrite, AuditEvent: "cockroachdb.schema.create", Input: schemaCreateSchema(), Handle: createSchema},
		{ID: "cockroachdb.schema.drop", Method: plugin.MethodDelete, Path: "/schemas/{schema}", Permission: "cockroachdb.schemas.delete", Risk: plugin.RiskDestructive, AuditEvent: "cockroachdb.schema.drop", Handle: dropSchema},
		{ID: "cockroachdb.table.create", Method: plugin.MethodPost, Path: "/schemas/{schema}/tables", Permission: "cockroachdb.tables.write", Risk: plugin.RiskWrite, AuditEvent: "cockroachdb.table.create", Input: tableCreateSchema(), Handle: createTable},
		{ID: "cockroachdb.column.add", Method: plugin.MethodPost, Path: "/tables/{schema}/{table}/columns", Permission: "cockroachdb.tables.write", Risk: plugin.RiskWrite, AuditEvent: "cockroachdb.column.add", Input: columnAddSchema(), Handle: addColumn},
		{ID: "cockroachdb.column.rename", Method: plugin.MethodPost, Path: "/tables/{schema}/{table}/columns/rename", Permission: "cockroachdb.tables.write", Risk: plugin.RiskWrite, AuditEvent: "cockroachdb.column.rename", Input: columnRenameSchema(), Handle: renameColumn},
		{ID: "cockroachdb.column.alter", Method: plugin.MethodPost, Path: "/tables/{schema}/{table}/columns/alter", Permission: "cockroachdb.tables.write", Risk: plugin.RiskWrite, AuditEvent: "cockroachdb.column.alter", Input: columnAlterSchema(), Handle: alterColumn},
		{ID: "cockroachdb.column.drop", Method: plugin.MethodPost, Path: "/tables/{schema}/{table}/columns/drop", Permission: "cockroachdb.tables.write", Risk: plugin.RiskDestructive, AuditEvent: "cockroachdb.column.drop", Handle: dropColumn},
		{ID: "cockroachdb.constraint.add", Method: plugin.MethodPost, Path: "/tables/{schema}/{table}/constraints", Permission: "cockroachdb.tables.write", Risk: plugin.RiskWrite, AuditEvent: "cockroachdb.constraint.add", Input: constraintAddSchema(), Handle: addConstraint},
		{ID: "cockroachdb.constraint.drop", Method: plugin.MethodPost, Path: "/tables/{schema}/{table}/constraints/drop", Permission: "cockroachdb.tables.write", Risk: plugin.RiskDestructive, AuditEvent: "cockroachdb.constraint.drop", Handle: dropConstraint},
		{ID: "cockroachdb.table.rename", Method: plugin.MethodPost, Path: "/tables/{schema}/{table}/rename", Permission: "cockroachdb.tables.write", Risk: plugin.RiskWrite, AuditEvent: "cockroachdb.table.rename", Input: tableRenameSchema(), Handle: renameTable},
		{ID: "cockroachdb.index.create", Method: plugin.MethodPost, Path: "/tables/{schema}/{table}/indexes", Permission: "cockroachdb.tables.write", Risk: plugin.RiskWrite, AuditEvent: "cockroachdb.index.create", Input: indexCreateSchema(), Handle: createIndex},
		{ID: "cockroachdb.index.drop", Method: plugin.MethodPost, Path: "/tables/{schema}/{table}/indexes/drop", Permission: "cockroachdb.tables.write", Risk: plugin.RiskDestructive, AuditEvent: "cockroachdb.index.drop", Handle: dropIndex},
		{ID: "cockroachdb.table.truncate", Method: plugin.MethodPost, Path: "/tables/{schema}/{table}/truncate", Permission: "cockroachdb.tables.delete", Risk: plugin.RiskDestructive, AuditEvent: "cockroachdb.table.truncate", Handle: truncateTable},
		{ID: "cockroachdb.table.drop", Method: plugin.MethodDelete, Path: "/tables/{schema}/{table}", Permission: "cockroachdb.tables.delete", Risk: plugin.RiskDestructive, AuditEvent: "cockroachdb.table.drop", Handle: dropTable},
		{ID: "cockroachdb.query", Method: plugin.MethodWS, Path: "/query", Permission: "cockroachdb.query.execute", Risk: plugin.RiskPrivileged, AuditEvent: "cockroachdb.query", Stream: queryStream},
		{ID: "cockroachdb.query.cancel", Method: plugin.MethodPost, Path: "/query/cancel", Permission: "cockroachdb.query.cancel", Risk: plugin.RiskWrite, AuditEvent: "cockroachdb.query.cancel", Handle: cancelQuery},
		{ID: "cockroachdb.session.cancel", Method: plugin.MethodPost, Path: "/sessions/{id}/cancel", Permission: "cockroachdb.sessions.cancel", Risk: plugin.RiskDestructive, AuditEvent: "cockroachdb.session.cancel", Handle: cancelSession},
		{ID: "cockroachdb.query.cancel.id", Method: plugin.MethodPost, Path: "/queries/{id}/cancel", Permission: "cockroachdb.queries.cancel", Risk: plugin.RiskDestructive, AuditEvent: "cockroachdb.query.cancel.id", Handle: cancelQueryByID},
		{ID: "cockroachdb.users.list", Method: plugin.MethodGet, Path: "/users", Permission: "cockroachdb.users.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.users.list", Handle: listUsers},
		{ID: "cockroachdb.users.tree", Method: plugin.MethodGet, Path: "/tree/users", Permission: "cockroachdb.users.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.users.tree", Handle: treeUsers},
		{ID: "cockroachdb.user.overview", Method: plugin.MethodGet, Path: "/users/{user}/overview", Permission: "cockroachdb.users.read", Risk: plugin.RiskSafe, AuditEvent: "cockroachdb.user.overview", Handle: userOverview},
		{ID: "cockroachdb.user.create", Method: plugin.MethodPost, Path: "/users", Permission: "cockroachdb.users.write", Risk: plugin.RiskWrite, AuditEvent: "cockroachdb.user.create", Input: userCreateSchema(), Handle: createUser},
		{ID: "cockroachdb.user.drop", Method: plugin.MethodDelete, Path: "/users/{user}", Permission: "cockroachdb.users.delete", Risk: plugin.RiskDestructive, AuditEvent: "cockroachdb.user.drop", Handle: dropUser},
		{ID: "cockroachdb.user.grant", Method: plugin.MethodPost, Path: "/users/{user}/grant", Permission: "cockroachdb.users.write", Risk: plugin.RiskPrivileged, AuditEvent: "cockroachdb.user.grant", Input: userGrantSchema(), Handle: grantUser},
	}
}

func treeDatabases(rc *plugin.RequestContext) (any, error) {
	return treeFromPage(rc, "database", "database", "name", listDatabases)
}

func treeNodes(rc *plugin.RequestContext) (any, error) {
	return treeFromPage(rc, "node", "server", "node_id", listNodes)
}

func treeRanges(rc *plugin.RequestContext) (any, error) {
	return treeFromPage(rc, "range", "blocks", "range_id", listRanges)
}

func treeJobs(rc *plugin.RequestContext) (any, error) {
	return treeFromPage(rc, "job", "briefcase-business", "job_id", listJobs)
}

func treeSessions(rc *plugin.RequestContext) (any, error) {
	return treeFromPage(rc, "session", "activity", "session_id", listSessions)
}

func treeQueries(rc *plugin.RequestContext) (any, error) {
	return treeFromPage(rc, "query", "search-code", "query_id", listQueries)
}

func databaseCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Database", Fields: []plugin.Field{
		{Key: "name", Label: "Database name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		{Key: "if_not_exists", Label: "If not exists", Type: plugin.FieldToggle, Default: true},
	}}}}
}

func schemaCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Schema", Fields: []plugin.Field{
		{Key: "name", Label: "Schema name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
	}}}}
}

func createSchema(rc *plugin.RequestContext) (any, error) {
	var req struct {
		Name string `json:"name" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name, err := sqldb.SafeIdentifier(req.Name)
	if err != nil {
		return nil, err
	}
	return execDDL(rc, "CREATE SCHEMA "+sqldb.QuoteIdent(name))
}

func tableCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Table", Fields: []plugin.Field{
		{Key: "name", Label: "Table name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		sqldb.ColumnsArrayField(sqldb.ColumnsField{TypePlaceholder: "INT8", TypeSuggestions: []string{"INT8", "INT8 DEFAULT unique_rowid()", "INT4", "INT2", "SERIAL", "DECIMAL(10,2)", "FLOAT8", "BOOL", "STRING", "VARCHAR(255)", "BYTES", "DATE", "TIMESTAMPTZ", "TIMESTAMP", "TIME", "UUID", "JSONB", "INTERVAL", "INET"}, Default: true, Primary: true, Unique: true}),
		{Key: "if_not_exists", Label: "If not exists", Type: plugin.FieldToggle, Default: true},
	}}}}
}

func columnAddSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Column", Fields: []plugin.Field{
		{Key: "name", Label: "Column name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		{Key: "type", Label: "Type", Type: plugin.FieldText, Required: true, Default: "STRING"},
		{Key: "nullable", Label: "Nullable", Type: plugin.FieldToggle, Default: true},
		{Key: "default", Label: "Default expression", Type: plugin.FieldText},
	}}}}
}

func indexCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Index", Fields: []plugin.Field{
		{Key: "name", Label: "Index name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		{Key: "columns", Label: "Columns", Type: plugin.FieldMultiSelect, Required: true, OptionsSource: &plugin.DataSource{RouteID: "cockroachdb.table.columns", Params: tableParams()}},
		{Key: "unique", Label: "Unique", Type: plugin.FieldToggle},
	}}}}
}

func columnRenameSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Rename column", Fields: []plugin.Field{
		{Key: "newName", Label: "New name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
	}}}}
}

func columnAlterSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Change type", Fields: []plugin.Field{
		{Key: "type", Label: "New type", Type: plugin.FieldText, Required: true, Default: "STRING"},
		{Key: "using", Label: "USING expression", Type: plugin.FieldText, Help: "Optional cast expression, e.g. column::INT8."},
	}}}}
}

func tableRenameSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Rename table", Fields: []plugin.Field{
		{Key: "newName", Label: "New name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
	}}}}
}

func constraintAddSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Constraint", Fields: []plugin.Field{
		{Key: "name", Label: "Constraint name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		{Key: "type", Label: "Type", Type: plugin.FieldSelect, Required: true, Default: constraintPrimaryKey, Options: []plugin.Option{
			{Label: "Primary key", Value: constraintPrimaryKey},
			{Label: "Unique", Value: constraintUnique},
			{Label: "Check", Value: constraintCheck},
			{Label: "Foreign key", Value: constraintForeignKey},
		}},
		{Key: "columns", Label: "Columns", Type: plugin.FieldMultiSelect, OptionsSource: &plugin.DataSource{RouteID: "cockroachdb.table.columns", Params: tableParams()}, Help: "Columns for primary key, unique, or the referencing side of a foreign key.", VisibleWhen: &plugin.Condition{AnyOf: []plugin.Rule{
			{Field: "type", Op: plugin.OpIn, Value: []any{constraintPrimaryKey, constraintUnique, constraintForeignKey}},
		}}},
		{Key: "check", Label: "Check expression", Type: plugin.FieldText, Help: "Boolean expression, e.g. price > 0.", VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "type", Op: plugin.OpEq, Value: constraintCheck}}}},
		{Key: "refTable", Label: "Referenced table", Type: plugin.FieldText, Help: "Target table for a foreign key (schema-qualified or bare).", VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "type", Op: plugin.OpEq, Value: constraintForeignKey}}}},
		{Key: "refColumns", Label: "Referenced columns", Type: plugin.FieldText, Help: "Comma-separated columns on the referenced table.", VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "type", Op: plugin.OpEq, Value: constraintForeignKey}}}},
		{Key: "onDelete", Label: "On delete", Type: plugin.FieldSelect, Options: []plugin.Option{
			{Label: "No action", Value: "NO ACTION"},
			{Label: "Restrict", Value: "RESTRICT"},
			{Label: "Cascade", Value: "CASCADE"},
			{Label: "Set null", Value: "SET NULL"},
			{Label: "Set default", Value: "SET DEFAULT"},
		}, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "type", Op: plugin.OpEq, Value: constraintForeignKey}}}},
	}}}}
}

func userCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "User", Fields: []plugin.Field{
		{Key: "name", Label: "Username", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}},
		{Key: "password", Label: "Password", Type: plugin.FieldPassword, Secret: true, Help: "Optional. Leave blank to create a user without a password."},
	}}}}
}

func userGrantSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Grant", Fields: []plugin.Field{
		{Key: "target", Label: "Grant", Type: plugin.FieldSelect, Required: true, Default: grantTargetRole, Options: []plugin.Option{
			{Label: "Role", Value: grantTargetRole},
			{Label: "Database privilege", Value: grantTargetDatabase},
			{Label: "Schema privilege", Value: grantTargetSchema},
			{Label: "Table privilege", Value: grantTargetTable},
		}},
		{Key: "role", Label: "Role", Type: plugin.FieldText, Help: "Role to grant to the user.", Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: sqldb.IdentifierPattern}}, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "target", Op: plugin.OpEq, Value: grantTargetRole}}}},
		{Key: "privilege", Label: "Privilege", Type: plugin.FieldSelect, Default: "ALL", Options: []plugin.Option{
			{Label: "ALL", Value: "ALL"},
			{Label: "SELECT", Value: "SELECT"},
			{Label: "INSERT", Value: "INSERT"},
			{Label: "UPDATE", Value: "UPDATE"},
			{Label: "DELETE", Value: "DELETE"},
			{Label: "CREATE", Value: "CREATE"},
			{Label: "DROP", Value: "DROP"},
			{Label: "CONNECT", Value: "CONNECT"},
			{Label: "USAGE", Value: "USAGE"},
		}, VisibleWhen: &plugin.Condition{AnyOf: []plugin.Rule{{Field: "target", Op: plugin.OpIn, Value: []any{grantTargetDatabase, grantTargetSchema, grantTargetTable}}}}},
		{Key: "object", Label: "Object", Type: plugin.FieldText, Help: "Database/schema name, or schema-qualified table (e.g. public.orders).", VisibleWhen: &plugin.Condition{AnyOf: []plugin.Rule{{Field: "target", Op: plugin.OpIn, Value: []any{grantTargetDatabase, grantTargetSchema, grantTargetTable}}}}},
	}}}}
}

func cockroachSession(rc *plugin.RequestContext) (*Session, error) {
	return unwrap(rc.Session)
}

func listDatabases(rc *plugin.RequestContext) (any, error) {
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT database_name AS name
FROM [SHOW DATABASES]
ORDER BY database_name`, nil)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		name := fmt.Sprint(r["name"])
		r["ref"] = plugin.ResourceRef{Kind: "database", Name: name, UID: name}
	}
	return pageRows(rc, rows)
}

func treeTables(rc *plugin.RequestContext) (any, error) {
	return treeFromPage(rc, "table", "table-2", "name", listTables)
}

func treeViews(rc *plugin.RequestContext) (any, error) {
	return treeFromPage(rc, "view", "panel-top", "name", listViews)
}

func treeFunctions(rc *plugin.RequestContext) (any, error) {
	return treeFromPage(rc, "function", "function-square", "name", listFunctions)
}

func databaseOverview(rc *plugin.RequestContext) (any, error) {
	database := strings.TrimSpace(rc.Param("database"))
	if database == "" {
		return nil, fmt.Errorf("%w: database is required", plugin.ErrInvalidInput)
	}
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT $1::STRING AS name, current_database() AS connected_database, current_user AS "user", version() AS version,
       (SELECT COUNT(*) FROM information_schema.schemata WHERE catalog_name = current_database() AND schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_extension', 'crdb_internal')) AS schemas,
       (SELECT COUNT(*) FROM information_schema.tables WHERE table_catalog = current_database() AND table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_extension', 'crdb_internal') AND table_type = 'BASE TABLE') AS tables,
       (SELECT COUNT(*) FROM information_schema.tables WHERE table_catalog = current_database() AND table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_extension', 'crdb_internal') AND table_type = 'VIEW') AS views`, []any{database})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, plugin.ErrNotFound
	}
	return rows[0], nil
}

func listNodes(rc *plugin.RequestContext) (any, error) {
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT 1::INT8 AS node_id, version() AS version, NULL::STRING AS cluster_id, NULL::STRING AS platform`, nil)
	if err != nil {
		return nil, err
	}
	addRefs(rows, "node", "node_id", "node_id")
	return pageRows(rc, rows)
}

func nodeOverview(rc *plugin.RequestContext) (any, error) {
	node := strings.TrimSpace(rc.Param("node"))
	if node == "" {
		return nil, fmt.Errorf("%w: node is required", plugin.ErrInvalidInput)
	}
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT $1::STRING AS node_id, version() AS version, current_database() AS database, current_user AS "user"`, []any{node})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, plugin.ErrNotFound
	}
	return rows[0], nil
}

func listRanges(rc *plugin.RequestContext) (any, error) {
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	var database string
	if err := s.pool.QueryRow(rc.Ctx, "SELECT current_database()").Scan(&database); err != nil {
		return nil, cockroachErr(err)
	}
	rows, err := queryRows(rc.Ctx, s, "SHOW RANGES FROM DATABASE "+sqldb.QuoteIdent(database), nil)
	if err != nil {
		return nil, err
	}
	addRefs(rows, "range", "range_id", "range_id")
	return pageRows(rc, rows)
}

func rangeOverview(rc *plugin.RequestContext) (any, error) {
	return overviewFromRows(rc, "range", "range_id", listRanges)
}

func listJobs(rc *plugin.RequestContext) (any, error) {
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, "SHOW JOBS", nil)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		copyKey(r, "id", "job_id")
		copyKey(r, "type", "job_type")
	}
	addRefs(rows, "job", "job_id", "job_id", "id")
	return pageRows(rc, rows)
}

func jobOverview(rc *plugin.RequestContext) (any, error) {
	return overviewFromRows(rc, "job", "job_id", listJobs)
}

func listSessions(rc *plugin.RequestContext) (any, error) {
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, "SHOW SESSIONS", nil)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		copyKey(r, "user_name", "username")
		copyKey(r, "session_start", "start")
	}
	addRefs(rows, "session", "session_id", "session_id")
	return pageRows(rc, rows)
}

func sessionOverview(rc *plugin.RequestContext) (any, error) {
	return overviewFromRows(rc, "session", "session_id", listSessions)
}

func listQueries(rc *plugin.RequestContext) (any, error) {
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, "SHOW QUERIES", nil)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		copyKey(r, "user_name", "username")
	}
	addRefs(rows, "query", "query_id", "query_id")
	return pageRows(rc, rows)
}

func queryOverview(rc *plugin.RequestContext) (any, error) {
	return overviewFromRows(rc, "query", "query_id", listQueries)
}

func schemaTree(rc *plugin.RequestContext) (any, error) {
	page, err := listSchemas(rc)
	if err != nil {
		return nil, err
	}
	schemas := page.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(schemas.Items))
	for _, r := range schemas.Items {
		name := fmt.Sprint(r["name"])
		nodes = append(nodes, plugin.TreeNode{
			Key:            "schema:" + name,
			Label:          name,
			Icon:           icon("folder"),
			Ref:            &plugin.ResourceRef{Kind: "schema", Name: name, UID: name},
			ChildrenSource: &plugin.DataSource{RouteID: "cockroachdb.tables.tree", Params: map[string]string{"schema": name}},
		})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: schemas.NextCursor, Total: schemas.Total}, nil
}

func listSchemas(rc *plugin.RequestContext) (any, error) {
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT s.schema_name AS name, NULL::STRING AS owner,
       COUNT(t.table_name) FILTER (WHERE t.table_type = 'BASE TABLE') AS tables,
       COUNT(t.table_name) FILTER (WHERE t.table_type = 'VIEW') AS views,
       (SELECT COUNT(*) FROM information_schema.routines r WHERE r.specific_schema = s.schema_name) AS functions
FROM information_schema.schemata s
LEFT JOIN information_schema.tables t ON t.table_schema = s.schema_name
WHERE s.schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_extension', 'crdb_internal')
GROUP BY s.schema_name
ORDER BY s.schema_name`, nil)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		name := fmt.Sprint(r["name"])
		r["ref"] = plugin.ResourceRef{Kind: "schema", Name: name, UID: name}
	}
	return pageRows(rc, rows)
}

func schemaOverview(rc *plugin.RequestContext) (any, error) {
	schema, err := sqldb.SafeIdentifier(rc.Param("schema"))
	if err != nil {
		return nil, err
	}
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT s.schema_name AS name, NULL::STRING AS owner,
       COUNT(t.table_name) FILTER (WHERE t.table_type = 'BASE TABLE') AS tables,
       COUNT(t.table_name) FILTER (WHERE t.table_type = 'VIEW') AS views,
       (SELECT COUNT(*) FROM information_schema.sequences seq WHERE seq.sequence_schema = s.schema_name) AS sequences,
       (SELECT COUNT(*) FROM information_schema.routines r WHERE r.specific_schema = s.schema_name) AS functions
FROM information_schema.schemata s
LEFT JOIN information_schema.tables t ON t.table_schema = s.schema_name
WHERE s.schema_name = $1
GROUP BY s.schema_name`, []any{schema})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, plugin.ErrNotFound
	}
	return rows[0], nil
}

func listTables(rc *plugin.RequestContext) (any, error) {
	return relationList(rc, []string{"BASE TABLE"}, "table")
}

const relationGraphSQL = `
SELECT rc.constraint_name AS constraint_name,
       kcu.table_schema AS child_schema, kcu.table_name AS child_table, kcu.column_name AS child_column,
       uk.table_schema AS parent_schema, uk.table_name AS parent_table, uk.column_name AS parent_column
FROM information_schema.referential_constraints rc
JOIN information_schema.key_column_usage kcu
  ON kcu.constraint_schema = rc.constraint_schema AND kcu.constraint_name = rc.constraint_name
JOIN information_schema.key_column_usage uk
  ON uk.constraint_schema = rc.unique_constraint_schema AND uk.constraint_name = rc.unique_constraint_name
 AND uk.ordinal_position = kcu.position_in_unique_constraint
WHERE kcu.table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_extension', 'crdb_internal')
  AND ($1::STRING = '' OR kcu.table_schema = $1)
ORDER BY rc.constraint_name, kcu.ordinal_position`

const relationColumnsSQL = `
SELECT table_schema, table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_extension', 'crdb_internal')
  AND ($1::STRING = '' OR table_schema = $1)
ORDER BY table_schema, table_name, ordinal_position`

func relationGraph(rc *plugin.RequestContext) (any, error) {
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	schema, err := sqldb.OptionalIdentifier(rc.Query().Get("p.schema"))
	if err != nil {
		return nil, err
	}
	colRows, err := queryRows(rc.Ctx, s, relationColumnsSQL, []any{schema})
	if err != nil {
		return nil, err
	}
	fkRows, err := queryRows(rc.Ctx, s, relationGraphSQL, []any{schema})
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
	return relationList(rc, []string{"VIEW"}, "view")
}

func relationList(rc *plugin.RequestContext, kinds []string, refKind string) (any, error) {
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	schema, err := sqldb.OptionalIdentifier(rc.Query().Get("p.schema"))
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT table_name AS name, table_schema AS schema, NULL::STRING AS owner,
       NULL::INT8 AS rows, NULL::INT8 AS size, is_insertable_into = 'YES' AS updatable
FROM information_schema.tables
WHERE table_type = ANY($1) AND table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_extension', 'crdb_internal')
  AND ($2::STRING = '' OR table_schema = $2)
ORDER BY table_schema, table_name`, []any{kinds, schema})
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		name, schema := fmt.Sprint(r["name"]), fmt.Sprint(r["schema"])
		r["ref"] = plugin.ResourceRef{Kind: refKind, Namespace: schema, Name: name, UID: schema + "." + name}
	}
	return pageRows(rc, rows)
}

func listFunctions(rc *plugin.RequestContext) (any, error) {
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	schema, err := sqldb.OptionalIdentifier(rc.Query().Get("p.schema"))
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT specific_schema || '.' || routine_name AS id, routine_name AS name, specific_schema AS schema,
       COALESCE(data_type, '') AS returns, COALESCE(routine_body, '') AS language,
       '' AS arguments
FROM information_schema.routines
WHERE specific_schema NOT IN ('information_schema', 'pg_catalog', 'pg_extension', 'crdb_internal')
  AND ($1::STRING = '' OR specific_schema = $1)
ORDER BY specific_schema, routine_name`, []any{schema})
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		name, schema, id := fmt.Sprint(r["name"]), fmt.Sprint(r["schema"]), fmt.Sprint(r["id"])
		r["ref"] = plugin.ResourceRef{Kind: "function", Namespace: schema, Name: name, UID: id}
	}
	return pageRows(rc, rows)
}

func listSequences(rc *plugin.RequestContext) (any, error) {
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	schema, err := sqldb.OptionalIdentifier(rc.Query().Get("p.schema"))
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT sequence_name AS name, sequence_schema AS schema, data_type AS "dataType",
       start_value AS start, increment AS increment
FROM information_schema.sequences
WHERE ($1::text = '' OR sequence_schema = $1)
ORDER BY sequence_schema, sequence_name`, []any{schema})
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		name, schema := fmt.Sprint(r["name"]), fmt.Sprint(r["schema"])
		r["ref"] = plugin.ResourceRef{Kind: "sequence", Namespace: schema, Name: name, UID: schema + "." + name}
	}
	return pageRows(rc, rows)
}

func tableRows(rc *plugin.RequestContext) (any, error) {
	schema, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	req, err := rc.Page()
	if err != nil {
		return nil, err
	}
	s, err := cockroachSession(rc)
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
	qualified := sqldb.Qualified(schema, table)
	filter := req.Search()
	countSQL := "SELECT COUNT(*) FROM " + qualified + " AS t"
	countArgs := []any{}
	if filter != "" {
		countSQL += " WHERE t::string ILIKE $1"
		countArgs = append(countArgs, "%"+filter+"%")
	}
	var total int
	if err := s.pool.QueryRow(rc.Ctx, countSQL, countArgs...).Scan(&total); err != nil {
		return nil, cockroachErr(err)
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
		orderBy = " ORDER BY " + sqldb.QuoteIdent(col) + " " + dir
	}
	dataArgs := []any{limit, offset}
	where := ""
	if filter != "" {
		where = " WHERE t::string ILIKE $3"
		dataArgs = append(dataArgs, "%"+filter+"%")
	}
	sqlText := "SELECT * FROM " + qualified + " AS t" + where + orderBy + " LIMIT $1 OFFSET $2"
	rows, err := queryRows(rc.Ctx, s, sqlText, dataArgs)
	if err != nil {
		return nil, err
	}
	pk, err := primaryKeyColumns(rc.Ctx, s, schema, table)
	if err != nil {
		return nil, err
	}
	attachRowKeys(rows, pk, s.opts.RedactPatterns)
	fks, err := foreignKeys(rc.Ctx, s, schema, table)
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

// foreignKeys maps each FK column to the referenced table's ref, attached under
// the generic "_links" field the grid renders as links.
func foreignKeys(ctx context.Context, s *Session, schema, table string) (map[string]plugin.ResourceRef, error) {
	rows, err := queryRows(ctx, s, `
SELECT kcu.column_name AS col, ccu.table_schema AS ref_schema, ccu.table_name AS ref_table
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON kcu.constraint_name = tc.constraint_name AND kcu.constraint_schema = tc.constraint_schema
JOIN information_schema.constraint_column_usage ccu
  ON ccu.constraint_name = tc.constraint_name AND ccu.constraint_schema = tc.constraint_schema
WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = $1 AND tc.table_name = $2`, []any{schema, table})
	if err != nil {
		return nil, err
	}
	out := map[string]plugin.ResourceRef{}
	for _, r := range rows {
		col, refSchema, refTable := fmt.Sprint(r["col"]), fmt.Sprint(r["ref_schema"]), fmt.Sprint(r["ref_table"])
		out[col] = plugin.ResourceRef{Kind: "table", Namespace: refSchema, Name: refTable, UID: refSchema + "." + refTable}
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
	schema, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT column_name AS name, data_type AS type, is_nullable = 'YES' AS nullable,
       column_default AS default, identity_generation AS identity, ordinal_position AS position
FROM information_schema.columns
WHERE table_schema = $1 AND table_name = $2
ORDER BY ordinal_position`, []any{schema, table})
	if err != nil {
		return nil, err
	}
	for i := range rows {
		rows[i]["schema"] = schema
		rows[i]["table"] = table
	}
	return pageRows(rc, rows)
}

func tableIndexes(rc *plugin.RequestContext) (any, error) {
	schema, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	raw, err := queryRows(rc.Ctx, s, "SHOW INDEXES FROM "+sqldb.Qualified(schema, table), nil)
	if err != nil {
		return nil, err
	}
	rows := normalizeIndexRows(raw)
	for i := range rows {
		rows[i]["schema"] = schema
		rows[i]["table"] = table
	}
	return pageRows(rc, rows)
}

func tableConstraints(rc *plugin.RequestContext) (any, error) {
	schema, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT tc.constraint_name AS name, lower(tc.constraint_type) AS type,
       COALESCE(string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position), '') AS definition
FROM information_schema.table_constraints tc
LEFT JOIN information_schema.key_column_usage kcu
  ON kcu.constraint_schema = tc.constraint_schema
 AND kcu.constraint_name = tc.constraint_name
 AND kcu.table_schema = tc.table_schema
 AND kcu.table_name = tc.table_name
WHERE tc.table_schema = $1 AND tc.table_name = $2
GROUP BY tc.constraint_name, tc.constraint_type
ORDER BY tc.constraint_name`, []any{schema, table})
	if err != nil {
		return nil, err
	}
	for i := range rows {
		rows[i]["schema"] = schema
		rows[i]["table"] = table
	}
	return pageRows(rc, rows)
}

func viewDefinition(rc *plugin.RequestContext) (any, error) {
	schema, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT table_schema AS schema, table_name AS name, COALESCE(view_definition, '') AS definition
FROM information_schema.views
WHERE table_schema = $1 AND table_name = $2`, []any{schema, table})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, plugin.ErrNotFound
	}
	return rows[0], nil
}

func functionDefinition(rc *plugin.RequestContext) (any, error) {
	id := strings.TrimSpace(rc.Param("id"))
	schema, name, ok := strings.Cut(id, ".")
	if !ok {
		return nil, fmt.Errorf("%w: function id is invalid", plugin.ErrInvalidInput)
	}
	if _, err := sqldb.SafeIdentifier(schema); err != nil {
		return nil, err
	}
	if _, err := sqldb.SafeIdentifier(name); err != nil {
		return nil, err
	}
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT specific_schema AS schema, routine_name AS name,
       COALESCE(routine_definition, '') AS definition,
       COALESCE(data_type, '') AS returns
FROM information_schema.routines
WHERE specific_schema = $1 AND routine_name = $2`, []any{schema, name})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, plugin.ErrNotFound
	}
	return rows[0], nil
}

func sequenceOverview(rc *plugin.RequestContext) (any, error) {
	schema, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT * FROM information_schema.sequences
WHERE sequence_schema = $1 AND sequence_name = $2`, []any{schema, table})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, plugin.ErrNotFound
	}
	return rows[0], nil
}

func completionRoute(rc *plugin.RequestContext) (any, error) {
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	items := []sqldb.CompletionItem{
		{Label: "SELECT", Type: "keyword"},
		{Label: "FROM", Type: "keyword"},
		{Label: "WHERE", Type: "keyword"},
		{Label: "ORDER BY", Type: "keyword"},
		{Label: "GROUP BY", Type: "keyword"},
		{Label: "LIMIT", Type: "keyword"},
		{Label: "INSERT", Type: "keyword"},
		{Label: "UPDATE", Type: "keyword"},
		{Label: "DELETE", Type: "keyword"},
		{Label: "CREATE TABLE", Type: "keyword"},
		{Label: "ALTER TABLE", Type: "keyword"},
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT t.table_schema AS schema, t.table_name AS relation, t.table_type AS kind, c.column_name AS column
FROM information_schema.tables t
LEFT JOIN information_schema.columns c ON c.table_schema = t.table_schema AND c.table_name = t.table_name
WHERE t.table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_extension', 'crdb_internal')
  AND t.table_type IN ('BASE TABLE', 'VIEW')
ORDER BY t.table_schema, t.table_name, c.ordinal_position
LIMIT 2000`, nil)
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
		schema := fmt.Sprint(r["schema"])
		relation := fmt.Sprint(r["relation"])
		kind := "table"
		if fmt.Sprint(r["kind"]) == "VIEW" {
			kind = "view"
		}
		add(sqldb.CompletionItem{Label: schema, Type: "namespace", Detail: "schema"})
		add(sqldb.CompletionItem{Label: relation, Type: kind, Detail: schema, Apply: sqldb.QuoteIdent(schema) + "." + sqldb.QuoteIdent(relation)})
		column := fmt.Sprint(r["column"])
		if column != "" && column != "<nil>" {
			add(sqldb.CompletionItem{Label: column, Type: "property", Detail: schema + "." + relation})
		}
	}
	functions, err := queryRows(rc.Ctx, s, `
SELECT specific_schema AS schema, routine_name AS name
FROM information_schema.routines
WHERE specific_schema NOT IN ('information_schema', 'pg_catalog', 'pg_extension', 'crdb_internal')
ORDER BY specific_schema, routine_name
LIMIT 500`, nil)
	if err != nil {
		return nil, err
	}
	for _, r := range functions {
		add(sqldb.CompletionItem{Label: fmt.Sprint(r["name"]), Type: "function", Detail: fmt.Sprint(r["schema"])})
	}
	return items, nil
}

func createDatabase(rc *plugin.RequestContext) (any, error) {
	s, err := cockroachSession(rc)
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
	if _, err := s.pool.Exec(rc.Ctx, prefix+sqldb.QuoteIdent(name)); err != nil {
		return nil, cockroachErr(err)
	}
	return actionResult{OK: true}, nil
}

func dropDatabase(rc *plugin.RequestContext) (any, error) {
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	name, err := sqldb.SafeIdentifier(rc.Param("database"))
	if err != nil {
		return nil, err
	}
	if name == s.opts.Database {
		return nil, fmt.Errorf("%w: cannot drop the connected database", plugin.ErrForbidden)
	}
	if _, err := s.pool.Exec(rc.Ctx, "DROP DATABASE "+sqldb.QuoteIdent(name)); err != nil {
		return nil, cockroachErr(err)
	}
	return actionResult{OK: true}, nil
}

func dropSchema(rc *plugin.RequestContext) (any, error) {
	name, err := sqldb.SafeIdentifier(rc.Param("schema"))
	if err != nil {
		return nil, err
	}
	return execDDL(rc, "DROP SCHEMA "+sqldb.QuoteIdent(name))
}

func createTable(rc *plugin.RequestContext) (any, error) {
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	schema, err := sqldb.SafeIdentifier(rc.Param("schema"))
	if err != nil {
		return nil, err
	}
	var req struct {
		Name        string `json:"name" validate:"required"`
		Columns     any    `json:"columns" validate:"required"`
		IfNotExists bool   `json:"if_not_exists"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	table, err := sqldb.SafeIdentifier(req.Name)
	if err != nil {
		return nil, err
	}
	columns, err := sqldb.ParseDDLColumns(req.Columns)
	if err != nil {
		return nil, err
	}
	prefix := "CREATE TABLE "
	if req.IfNotExists {
		prefix += "IF NOT EXISTS "
	}
	sqlText := prefix + sqldb.Qualified(schema, table) + " (" + strings.Join(columns, ", ") + ")"
	if _, err := s.pool.Exec(rc.Ctx, sqlText); err != nil {
		return nil, cockroachErr(err)
	}
	return actionResult{OK: true}, nil
}

func addColumn(rc *plugin.RequestContext) (any, error) {
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	schema, table, err := tableIdent(rc)
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
	column, err := sqldb.DDLColumn(sqldb.ColumnSpec{Name: req.Name, Type: req.Type, Nullable: req.Nullable, Default: req.Default})
	if err != nil {
		return nil, err
	}
	if _, err := s.pool.Exec(rc.Ctx, "ALTER TABLE "+sqldb.Qualified(schema, table)+" ADD COLUMN "+column); err != nil {
		return nil, cockroachErr(err)
	}
	return actionResult{OK: true}, nil
}

func dropColumn(rc *plugin.RequestContext) (any, error) {
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	schema, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	column, err := sqldb.SafeIdentifier(rc.Param("name"))
	if err != nil {
		return nil, err
	}
	if _, err := s.pool.Exec(rc.Ctx, "ALTER TABLE "+sqldb.Qualified(schema, table)+" DROP COLUMN "+sqldb.QuoteIdent(column)); err != nil {
		return nil, cockroachErr(err)
	}
	return actionResult{OK: true}, nil
}

func renameColumn(rc *plugin.RequestContext) (any, error) {
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	schema, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		NewName string `json:"newName" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	stmt, err := renameColumnSQL(schema, table, rc.Param("name"), req.NewName)
	if err != nil {
		return nil, err
	}
	if _, err := s.pool.Exec(rc.Ctx, stmt); err != nil {
		return nil, cockroachErr(err)
	}
	return actionResult{OK: true}, nil
}

func alterColumn(rc *plugin.RequestContext) (any, error) {
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	schema, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		Type  string `json:"type" validate:"required"`
		Using string `json:"using"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	stmt, err := alterColumnTypeSQL(schema, table, rc.Param("name"), req.Type, req.Using)
	if err != nil {
		return nil, err
	}
	if _, err := s.pool.Exec(rc.Ctx, stmt); err != nil {
		return nil, cockroachErr(err)
	}
	return actionResult{OK: true}, nil
}

func renameTable(rc *plugin.RequestContext) (any, error) {
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	schema, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		NewName string `json:"newName" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	stmt, err := renameTableSQL(schema, table, req.NewName)
	if err != nil {
		return nil, err
	}
	if _, err := s.pool.Exec(rc.Ctx, stmt); err != nil {
		return nil, cockroachErr(err)
	}
	return actionResult{OK: true}, nil
}

func addConstraint(rc *plugin.RequestContext) (any, error) {
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	schema, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	var req constraintRequest
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	stmt, err := addConstraintSQL(schema, table, req)
	if err != nil {
		return nil, err
	}
	if _, err := s.pool.Exec(rc.Ctx, stmt); err != nil {
		return nil, cockroachErr(err)
	}
	return actionResult{OK: true}, nil
}

func dropConstraint(rc *plugin.RequestContext) (any, error) {
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	schema, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	stmt, err := dropConstraintSQL(schema, table, rc.Param("name"))
	if err != nil {
		return nil, err
	}
	if _, err := s.pool.Exec(rc.Ctx, stmt); err != nil {
		return nil, cockroachErr(err)
	}
	return actionResult{OK: true}, nil
}

func createIndex(rc *plugin.RequestContext) (any, error) {
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	schema, table, err := tableIdent(rc)
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
	name, err := sqldb.SafeIdentifier(req.Name)
	if err != nil {
		return nil, err
	}
	cols, err := sqldb.IdentifierListValue(req.Columns, sqldb.QuoteIdent)
	if err != nil {
		return nil, err
	}
	unique := ""
	if req.Unique {
		unique = "UNIQUE "
	}
	stmt := "CREATE " + unique + "INDEX " + sqldb.QuoteIdent(name) + " ON " + sqldb.Qualified(schema, table) + " (" + strings.Join(cols, ", ") + ")"
	if _, err := s.pool.Exec(rc.Ctx, stmt); err != nil {
		return nil, cockroachErr(err)
	}
	return actionResult{OK: true}, nil
}

func dropIndex(rc *plugin.RequestContext) (any, error) {
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	schema, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	name, err := sqldb.SafeIdentifier(rc.Param("name"))
	if err != nil {
		return nil, err
	}
	// CockroachDB indexes are table-scoped: DROP INDEX <table>@<index>.
	if _, err := s.pool.Exec(rc.Ctx, "DROP INDEX "+sqldb.Qualified(schema, table)+"@"+sqldb.QuoteIdent(name)); err != nil {
		return nil, cockroachErr(err)
	}
	return actionResult{OK: true}, nil
}

func insertRow(rc *plugin.RequestContext) (any, error) {
	s, schema, table, m, err := rowMutationInput(rc)
	if err != nil {
		return nil, err
	}
	stmt, args, err := dialect.Insert(sqldb.Qualified(schema, table), m.Values)
	if err != nil {
		return nil, err
	}
	if _, err := s.pool.Exec(rc.Ctx, stmt, args...); err != nil {
		return nil, cockroachErr(err)
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
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, "", "", sqldb.RowMutation{}, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, "", "", sqldb.RowMutation{}, err
	}
	schema, table, err := tableIdent(rc)
	if err != nil {
		return nil, "", "", sqldb.RowMutation{}, err
	}
	var m sqldb.RowMutation
	if err := rc.Bind(&m); err != nil {
		return nil, "", "", sqldb.RowMutation{}, err
	}
	return s, schema, table, m, nil
}

// keyedRowMutation runs an UPDATE or DELETE only after confirming the client's
// key is exactly the table's primary key and that it affects a single row.
func keyedRowMutation(rc *plugin.RequestContext, del bool) (any, error) {
	s, schema, table, m, err := rowMutationInput(rc)
	if err != nil {
		return nil, err
	}
	pk, err := primaryKeyColumns(rc.Ctx, s, schema, table)
	if err != nil {
		return nil, err
	}
	if err := sqldb.ValidateRowKey(pk, m.Key); err != nil {
		return nil, err
	}
	qual := sqldb.Qualified(schema, table)
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
	tag, err := s.pool.Exec(rc.Ctx, stmt, args...)
	if err != nil {
		return nil, cockroachErr(err)
	}
	if tag.RowsAffected() == 0 {
		return nil, fmt.Errorf("%w: row no longer matches (it may have changed)", plugin.ErrNotFound)
	}
	return actionResult{OK: true}, nil
}

func primaryKeyColumns(ctx context.Context, s *Session, schema, table string) ([]string, error) {
	rows, err := queryRows(ctx, s, `
SELECT a.attname AS name
FROM pg_index i
JOIN pg_class c ON c.oid = i.indrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
WHERE i.indisprimary AND n.nspname = $1 AND c.relname = $2
ORDER BY array_position(i.indkey, a.attnum)`, []any{schema, table})
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
	schema, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	return execDDL(rc, "TRUNCATE TABLE "+sqldb.Qualified(schema, table))
}

func dropTable(rc *plugin.RequestContext) (any, error) {
	schema, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	return execDDL(rc, "DROP TABLE "+sqldb.Qualified(schema, table))
}

func dropView(rc *plugin.RequestContext) (any, error) {
	schema, err := sqldb.SafeIdentifier(rc.Param("schema"))
	if err != nil {
		return nil, err
	}
	view, err := sqldb.SafeIdentifier(rc.Param("view"))
	if err != nil {
		return nil, err
	}
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	// Regular and materialized views are listed together but dropped with
	// different statements, so resolve the relkind first.
	var relkind string
	if err := s.pool.QueryRow(rc.Ctx, `SELECT c.relkind::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2`, schema, view).Scan(&relkind); err != nil {
		return nil, cockroachErr(err)
	}
	stmt := "DROP VIEW " + sqldb.Qualified(schema, view)
	if relkind == "m" {
		stmt = "DROP MATERIALIZED VIEW " + sqldb.Qualified(schema, view)
	}
	return execDDL(rc, stmt)
}

func execDDL(rc *plugin.RequestContext, sqlText string) (any, error) {
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	if _, err := s.pool.Exec(rc.Ctx, sqlText); err != nil {
		return nil, cockroachErr(err)
	}
	return actionResult{OK: true}, nil
}

func cancelQuery(rc *plugin.RequestContext) (any, error) {
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	return actionResult{OK: s.cancelAll()}, nil
}

func cancelSession(rc *plugin.RequestContext) (any, error) {
	stmt, err := cancelSessionSQL(rc.Param("id"))
	if err != nil {
		return nil, err
	}
	return execDDL(rc, stmt)
}

func cancelQueryByID(rc *plugin.RequestContext) (any, error) {
	stmt, err := cancelQuerySQL(rc.Param("id"))
	if err != nil {
		return nil, err
	}
	return execDDL(rc, stmt)
}

func listUsers(rc *plugin.RequestContext) (any, error) {
	s, err := cockroachSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT username AS name,
       array_to_string(member_of, ', ') AS member_of,
       options
FROM [SHOW USERS]
ORDER BY username`, nil)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		name := fmt.Sprint(r["name"])
		r["ref"] = plugin.ResourceRef{Kind: "user", Name: name, UID: name}
	}
	return pageRows(rc, rows)
}

func treeUsers(rc *plugin.RequestContext) (any, error) {
	return treeFromPage(rc, "user", "user", "name", listUsers)
}

func userOverview(rc *plugin.RequestContext) (any, error) {
	if _, err := sqldb.SafeIdentifier(rc.Param("user")); err != nil {
		return nil, err
	}
	return overviewFromRows(rc, "user", "name", listUsers)
}

func createUser(rc *plugin.RequestContext) (any, error) {
	var req userCreateRequest
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	stmt, err := createUserSQL(req)
	if err != nil {
		return nil, err
	}
	return execDDL(rc, stmt)
}

func dropUser(rc *plugin.RequestContext) (any, error) {
	stmt, err := dropUserSQL(rc.Param("user"))
	if err != nil {
		return nil, err
	}
	return execDDL(rc, stmt)
}

func grantUser(rc *plugin.RequestContext) (any, error) {
	var req grantRequest
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	if req.User == "" {
		req.User = rc.Param("user")
	}
	stmt, err := grantSQL(req)
	if err != nil {
		return nil, err
	}
	return execDDL(rc, stmt)
}

func queryStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := cockroachSession(rc)
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
				payload["confirmMessage"] = "This CockroachDB statement can change data or schema. Review it before running."
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
			if !sqldb.IsReadOnlyStatement(st) {
				return sqldb.QueryResult{}, fmt.Errorf("%w: read-only mode blocks write statements", plugin.ErrForbidden)
			}
		}
	}
	if s.opts.RequireConfirm && !req.Confirm {
		for _, st := range statements {
			if sqldb.IsDestructiveStatement(st) {
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
		if sqldb.IsDestructiveStatement(st) {
			return true
		}
	}
	return false
}

func executeStatement(ctx context.Context, s *Session, statement string) (sqldb.StatementResult, error) {
	start := time.Now()
	rows, err := s.pool.Query(ctx, statement)
	if err != nil {
		return sqldb.StatementResult{}, cockroachErr(err)
	}
	defer rows.Close()
	fields := rows.FieldDescriptions()
	out := sqldb.StatementResult{Statement: statement, Columns: fieldNames(fields)}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return sqldb.StatementResult{}, cockroachErr(err)
		}
		out.Rows = append(out.Rows, sqldb.DisplayValues(out.Columns, values))
		if len(out.Rows) >= s.opts.RowLimit {
			break
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return sqldb.StatementResult{}, cockroachErr(err)
	}
	tag := rows.CommandTag()
	out.CommandTag = tag.String()
	out.RowCount = tag.RowsAffected()
	if out.RowCount == 0 && len(out.Rows) > 0 {
		out.RowCount = int64(len(out.Rows))
	}
	out.Rows = sqldb.RedactRows(out.Columns, out.Rows, s.opts.RedactPatterns)
	out.ElapsedMS = time.Since(start).Milliseconds()
	return out, nil
}

func queryRows(ctx context.Context, s *Session, sqlText string, args []any) ([]row, error) {
	ctx, cancel := context.WithTimeout(ctx, s.opts.QueryTimeout)
	defer cancel()
	rows, err := s.pool.Query(ctx, sqlText, args...)
	if err != nil {
		return nil, cockroachErr(err)
	}
	defer rows.Close()
	fields := rows.FieldDescriptions()
	names := fieldNames(fields)
	out := []row{}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, cockroachErr(err)
		}
		r := row{}
		for i, name := range names {
			if i < len(values) {
				r[name] = sqldb.DisplayValue(name, values[i])
			}
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, cockroachErr(err)
	}
	return out, nil
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

func fieldNames(fields []pgconn.FieldDescription) []string {
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		out = append(out, f.Name)
	}
	return out
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
		if schema := fmt.Sprint(r["schema"]); schema != "" && schema != "<nil>" && kind != "database" {
			label = schema + "." + label
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

func overviewFromRows(rc *plugin.RequestContext, param string, key string, load func(*plugin.RequestContext) (any, error)) (any, error) {
	want := strings.TrimSpace(rc.Param(param))
	if want == "" {
		return nil, fmt.Errorf("%w: %s is required", plugin.ErrInvalidInput, param)
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
		if fmt.Sprint(item[key]) == want {
			return item, nil
		}
		if ref, ok := item["ref"].(plugin.ResourceRef); ok && ref.UID == want {
			return item, nil
		}
	}
	return nil, plugin.ErrNotFound
}

func addRefs(rows []row, kind string, labelKey string, uidKeys ...string) {
	for _, r := range rows {
		uid := ""
		for _, key := range uidKeys {
			if value := fmt.Sprint(r[key]); value != "" && value != "<nil>" {
				uid = value
				break
			}
		}
		if uid == "" {
			continue
		}
		name := fmt.Sprint(r[labelKey])
		if name == "" || name == "<nil>" {
			name = uid
		}
		r["ref"] = plugin.ResourceRef{Kind: kind, Name: name, UID: uid}
	}
}

func copyKey(r row, from string, to string) {
	if _, exists := r[to]; exists {
		return
	}
	if value, ok := r[from]; ok {
		r[to] = value
	}
}

func normalizeIndexRows(raw []row) []row {
	type indexInfo struct {
		row     row
		columns []string
	}
	indexes := map[string]*indexInfo{}
	order := []string{}
	for _, r := range raw {
		name := firstString(r, "index_name", "index", "name")
		if name == "" {
			continue
		}
		info := indexes[name]
		if info == nil {
			unique := !boolFromAny(r["non_unique"])
			info = &indexInfo{row: row{
				"name":    name,
				"unique":  unique,
				"primary": strings.EqualFold(name, "primary"),
			}}
			indexes[name] = info
			order = append(order, name)
		}
		column := firstString(r, "column_name", "column")
		if column != "" {
			info.columns = append(info.columns, column)
		}
	}
	out := make([]row, 0, len(order))
	for _, name := range order {
		info := indexes[name]
		info.row["definition"] = strings.Join(info.columns, ", ")
		out = append(out, info.row)
	}
	return out
}

func firstString(r row, keys ...string) string {
	for _, key := range keys {
		value := strings.TrimSpace(fmt.Sprint(r[key]))
		if value != "" && value != "<nil>" {
			return value
		}
	}
	return ""
}

func boolFromAny(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(v, "true") || v == "1" || strings.EqualFold(v, "yes")
	default:
		return fmt.Sprint(value) == "true"
	}
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

func cockroachErr(err error) error {
	if err == nil {
		return nil
	}
	if err == pgx.ErrNoRows {
		return plugin.ErrNotFound
	}
	return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
}

func tableIdent(rc *plugin.RequestContext) (string, string, error) {
	schema, err := sqldb.SafeIdentifier(rc.Param("schema"))
	if err != nil {
		return "", "", err
	}
	table, err := sqldb.SafeIdentifier(rc.Param("table"))
	if err != nil {
		return "", "", err
	}
	return schema, table, nil
}
