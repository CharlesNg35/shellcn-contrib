package snowflake

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	sf "github.com/snowflakedb/gosnowflake/v2"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

type row = plugin.TableRow

type actionResult struct {
	OK      bool   `json:"ok"`
	Message string `json:"message,omitempty"`
}

type confirmationError struct {
	message string
}

func (e confirmationError) Error() string { return e.message }

// Snowflake binds positional parameters with "?" and quotes identifiers with
// double quotes, doubling any embedded quote.
var dialect = sqldb.Dialect{QuoteIdent: quoteIdent, Placeholder: sqldb.QuestionPlaceholder}

// identifierPattern is Snowflake's unquoted identifier grammar (letters, digits,
// underscore and dollar, up to 255 characters). Names are always emitted quoted,
// but restricting the accepted set keeps every interpolated name unambiguous.
const identifierPattern = `^[A-Za-z_][A-Za-z0-9_$]{0,254}$`

var identifierRE = regexp.MustCompile(identifierPattern)

// grantWordRE matches the privilege and object-type words SHOW GRANTS reports
// (SELECT, ALL PRIVILEGES, MATERIALIZED_VIEW, ...).
var grantWordRE = regexp.MustCompile(`^[A-Za-z][A-Za-z_ ]{0,63}$`)

// resultScanFrom reads back the preceding statement's output on the same
// session, which is the only way to filter, order and page SHOW/LIST results
// server-side instead of materializing them in the plugin.
const resultScanFrom = "TABLE(RESULT_SCAN(LAST_QUERY_ID()))"

// scanCap bounds any result set materialized in memory.
const scanCap = plugin.MaxPageLimit * 10

// completionCap bounds the catalog rows the SQL editor's completion route reads.
const completionCap = 2500

var warehouseSizes = []string{"XSMALL", "SMALL", "MEDIUM", "LARGE", "XLARGE", "XXLARGE", "XXXLARGE", "X4LARGE", "X5LARGE", "X6LARGE"}

var grantObjectTypes = []string{"ACCOUNT", "DATABASE", "SCHEMA", "TABLE", "VIEW", "MATERIALIZED VIEW", "STAGE", "FILE FORMAT", "PIPE", "TASK", "STREAM", "WAREHOUSE", "FUNCTION", "PROCEDURE", "SEQUENCE", "INTEGRATION", "ROLE"}

func routes() []plugin.Route {
	return []plugin.Route{
		{ID: "snowflake.databases.tree", Method: plugin.MethodGet, Path: "/tree/databases", Permission: "snowflake.databases.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.databases.tree", Handle: treeDatabases},
		{ID: "snowflake.schemas.tree", Method: plugin.MethodGet, Path: "/tree/schemas", Permission: "snowflake.schemas.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.schemas.tree", Handle: treeSchemas},
		{ID: "snowflake.relations.tree", Method: plugin.MethodGet, Path: "/tree/relations", Permission: "snowflake.tables.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.relations.tree", Handle: treeRelations},

		{ID: "snowflake.account.overview", Method: plugin.MethodGet, Path: "/account", Permission: "snowflake.account.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.account.overview", Handle: accountOverview},
		{ID: "snowflake.account.metrics", Method: plugin.MethodWS, Path: "/account/metrics", Permission: "snowflake.account.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.account.metrics", Stream: accountMetrics},

		{ID: "snowflake.databases.list", Method: plugin.MethodGet, Path: "/databases", Permission: "snowflake.databases.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.databases.list", Handle: listDatabases},
		{ID: "snowflake.database.overview", Method: plugin.MethodGet, Path: "/databases/{database}/overview", Permission: "snowflake.databases.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.database.overview", Handle: databaseOverview},
		{ID: "snowflake.database.create", Method: plugin.MethodPost, Path: "/databases", Permission: "snowflake.databases.write", Risk: plugin.RiskWrite, AuditEvent: "snowflake.database.create", Input: databaseCreateSchema(), Handle: createDatabase},
		{ID: "snowflake.database.drop", Method: plugin.MethodDelete, Path: "/databases/{database}", Permission: "snowflake.databases.delete", Risk: plugin.RiskDestructive, AuditEvent: "snowflake.database.drop", Handle: dropDatabase},

		{ID: "snowflake.schemas.list", Method: plugin.MethodGet, Path: "/schemas", Permission: "snowflake.schemas.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.schemas.list", Handle: listSchemas},
		{ID: "snowflake.schema.overview", Method: plugin.MethodGet, Path: "/schemas/{database}/{schema}/overview", Permission: "snowflake.schemas.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.schema.overview", Handle: schemaOverview},
		{ID: "snowflake.schema.create", Method: plugin.MethodPost, Path: "/databases/{database}/schemas", Permission: "snowflake.schemas.write", Risk: plugin.RiskWrite, AuditEvent: "snowflake.schema.create", Input: schemaCreateSchema(), Handle: createSchema},
		{ID: "snowflake.schema.drop", Method: plugin.MethodDelete, Path: "/schemas/{database}/{schema}", Permission: "snowflake.schemas.delete", Risk: plugin.RiskDestructive, AuditEvent: "snowflake.schema.drop", Handle: dropSchema},

		{ID: "snowflake.tables.list", Method: plugin.MethodGet, Path: "/tables", Permission: "snowflake.tables.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.tables.list", Handle: listTables},
		{ID: "snowflake.views.list", Method: plugin.MethodGet, Path: "/views", Permission: "snowflake.views.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.views.list", Handle: listViews},
		{ID: "snowflake.table.overview", Method: plugin.MethodGet, Path: "/tables/{database}/{schema}/{name}/overview", Permission: "snowflake.tables.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.table.overview", Handle: tableOverview},
		{ID: "snowflake.table.columns", Method: plugin.MethodGet, Path: "/tables/{database}/{schema}/{name}/columns", Permission: "snowflake.tables.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.table.columns", Handle: tableColumnsRoute},
		{ID: "snowflake.table.ddl", Method: plugin.MethodGet, Path: "/tables/{database}/{schema}/{name}/ddl", Permission: "snowflake.tables.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.table.ddl", Handle: tableDDL},
		{ID: "snowflake.view.ddl", Method: plugin.MethodGet, Path: "/views/{database}/{schema}/{name}/ddl", Permission: "snowflake.views.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.view.ddl", Handle: viewDDL},
		{ID: "snowflake.table.rows", Method: plugin.MethodGet, Path: "/tables/{database}/{schema}/{name}/rows", Permission: "snowflake.tables.data.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.table.rows", Handle: tableRows},
		{ID: "snowflake.view.rows", Method: plugin.MethodGet, Path: "/views/{database}/{schema}/{name}/rows", Permission: "snowflake.views.data.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.view.rows", Handle: viewRows},
		{ID: "snowflake.table.row.insert", Method: plugin.MethodPost, Path: "/tables/{database}/{schema}/{name}/rows", Permission: "snowflake.tables.data.write", Risk: plugin.RiskWrite, AuditEvent: "snowflake.table.row.insert", Handle: insertRow},
		{ID: "snowflake.table.row.update", Method: plugin.MethodPatch, Path: "/tables/{database}/{schema}/{name}/rows", Permission: "snowflake.tables.data.write", Risk: plugin.RiskWrite, AuditEvent: "snowflake.table.row.update", Handle: updateRow},
		{ID: "snowflake.table.row.delete", Method: plugin.MethodDelete, Path: "/tables/{database}/{schema}/{name}/rows", Permission: "snowflake.tables.data.delete", Risk: plugin.RiskDestructive, AuditEvent: "snowflake.table.row.delete", Handle: deleteRow},
		{ID: "snowflake.table.create", Method: plugin.MethodPost, Path: "/schemas/{database}/{schema}/tables", Permission: "snowflake.tables.write", Risk: plugin.RiskWrite, AuditEvent: "snowflake.table.create", Input: tableCreateSchema(), Handle: createTable},
		{ID: "snowflake.table.rename", Method: plugin.MethodPost, Path: "/tables/{database}/{schema}/{name}/rename", Permission: "snowflake.tables.write", Risk: plugin.RiskWrite, AuditEvent: "snowflake.table.rename", Input: tableRenameSchema(), Handle: renameTable},
		{ID: "snowflake.table.truncate", Method: plugin.MethodPost, Path: "/tables/{database}/{schema}/{name}/truncate", Permission: "snowflake.tables.delete", Risk: plugin.RiskDestructive, AuditEvent: "snowflake.table.truncate", Handle: truncateTable},
		{ID: "snowflake.table.drop", Method: plugin.MethodDelete, Path: "/tables/{database}/{schema}/{name}", Permission: "snowflake.tables.delete", Risk: plugin.RiskDestructive, AuditEvent: "snowflake.table.drop", Handle: dropTable},
		{ID: "snowflake.view.drop", Method: plugin.MethodDelete, Path: "/views/{database}/{schema}/{name}", Permission: "snowflake.views.delete", Risk: plugin.RiskDestructive, AuditEvent: "snowflake.view.drop", Handle: dropView},
		{ID: "snowflake.column.add", Method: plugin.MethodPost, Path: "/tables/{database}/{schema}/{name}/columns", Permission: "snowflake.tables.write", Risk: plugin.RiskWrite, AuditEvent: "snowflake.column.add", Input: columnAddSchema(), Handle: addColumn},
		{ID: "snowflake.column.drop", Method: plugin.MethodPost, Path: "/tables/{database}/{schema}/{name}/columns/drop", Permission: "snowflake.tables.write", Risk: plugin.RiskDestructive, AuditEvent: "snowflake.column.drop", Handle: dropColumn},

		{ID: "snowflake.warehouses.list", Method: plugin.MethodGet, Path: "/warehouses", Permission: "snowflake.warehouses.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.warehouses.list", Handle: listWarehouses},
		{ID: "snowflake.warehouse.overview", Method: plugin.MethodGet, Path: "/warehouses/{warehouse}/overview", Permission: "snowflake.warehouses.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.warehouse.overview", Handle: warehouseOverview},
		{ID: "snowflake.warehouse.metrics", Method: plugin.MethodWS, Path: "/warehouses/{warehouse}/metrics", Permission: "snowflake.warehouses.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.warehouse.metrics", Stream: warehouseMetrics},
		{ID: "snowflake.warehouse.resume", Method: plugin.MethodPost, Path: "/warehouses/{warehouse}/resume", Permission: "snowflake.warehouses.control", Risk: plugin.RiskWrite, AuditEvent: "snowflake.warehouse.resume", Handle: resumeWarehouse},
		{ID: "snowflake.warehouse.suspend", Method: plugin.MethodPost, Path: "/warehouses/{warehouse}/suspend", Permission: "snowflake.warehouses.control", Risk: plugin.RiskWrite, AuditEvent: "snowflake.warehouse.suspend", Handle: suspendWarehouse},
		{ID: "snowflake.warehouse.resize", Method: plugin.MethodPost, Path: "/warehouses/{warehouse}/resize", Permission: "snowflake.warehouses.control", Risk: plugin.RiskWrite, AuditEvent: "snowflake.warehouse.resize", Input: warehouseResizeSchema(), Handle: resizeWarehouse},

		{ID: "snowflake.roles.list", Method: plugin.MethodGet, Path: "/roles", Permission: "snowflake.roles.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.roles.list", Handle: listRoles},
		{ID: "snowflake.role.overview", Method: plugin.MethodGet, Path: "/roles/{role}/overview", Permission: "snowflake.roles.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.role.overview", Handle: roleOverview},
		{ID: "snowflake.role.grants", Method: plugin.MethodGet, Path: "/roles/{role}/grants", Permission: "snowflake.roles.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.role.grants", Handle: listRoleGrants},
		{ID: "snowflake.role.create", Method: plugin.MethodPost, Path: "/roles", Permission: "snowflake.roles.write", Risk: plugin.RiskWrite, AuditEvent: "snowflake.role.create", Input: roleCreateSchema(), Handle: createRole},
		{ID: "snowflake.role.drop", Method: plugin.MethodDelete, Path: "/roles/{role}", Permission: "snowflake.roles.delete", Risk: plugin.RiskDestructive, AuditEvent: "snowflake.role.drop", Handle: dropRole},
		{ID: "snowflake.role.grant", Method: plugin.MethodPost, Path: "/roles/{role}/grant", Permission: "snowflake.roles.grant", Risk: plugin.RiskPrivileged, AuditEvent: "snowflake.role.grant", Input: roleGrantSchema(), Handle: grantRolePrivilege},
		{ID: "snowflake.role.revoke", Method: plugin.MethodPost, Path: "/roles/{role}/revoke", Permission: "snowflake.roles.grant", Risk: plugin.RiskPrivileged, AuditEvent: "snowflake.role.revoke", Handle: revokeRolePrivilege},

		{ID: "snowflake.users.list", Method: plugin.MethodGet, Path: "/users", Permission: "snowflake.users.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.users.list", Handle: listUsers},
		{ID: "snowflake.user.overview", Method: plugin.MethodGet, Path: "/users/{user}/overview", Permission: "snowflake.users.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.user.overview", Handle: userOverview},
		{ID: "snowflake.user.grants", Method: plugin.MethodGet, Path: "/users/{user}/grants", Permission: "snowflake.users.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.user.grants", Handle: listUserGrants},
		{ID: "snowflake.user.role.grant", Method: plugin.MethodPost, Path: "/users/{user}/roles", Permission: "snowflake.users.grant", Risk: plugin.RiskPrivileged, AuditEvent: "snowflake.user.role.grant", Input: userRoleSchema(), Handle: grantUserRole},
		{ID: "snowflake.user.role.revoke", Method: plugin.MethodDelete, Path: "/users/{user}/roles/{role}", Permission: "snowflake.users.grant", Risk: plugin.RiskPrivileged, AuditEvent: "snowflake.user.role.revoke", Handle: revokeUserRole},

		{ID: "snowflake.stages.list", Method: plugin.MethodGet, Path: "/stages", Permission: "snowflake.stages.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.stages.list", Handle: listStages},
		{ID: "snowflake.stage.overview", Method: plugin.MethodGet, Path: "/stages/{database}/{schema}/{name}/overview", Permission: "snowflake.stages.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.stage.overview", Handle: stageOverview},
		{ID: "snowflake.stage.files", Method: plugin.MethodGet, Path: "/stages/{database}/{schema}/{name}/files", Permission: "snowflake.stages.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.stage.files", Handle: listStageFiles},

		{ID: "snowflake.formats.list", Method: plugin.MethodGet, Path: "/file-formats", Permission: "snowflake.formats.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.formats.list", Handle: listFileFormats},
		{ID: "snowflake.format.overview", Method: plugin.MethodGet, Path: "/file-formats/{database}/{schema}/{name}/overview", Permission: "snowflake.formats.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.format.overview", Handle: fileFormatOverview},

		{ID: "snowflake.pipes.list", Method: plugin.MethodGet, Path: "/pipes", Permission: "snowflake.pipes.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.pipes.list", Handle: listPipes},
		{ID: "snowflake.pipe.overview", Method: plugin.MethodGet, Path: "/pipes/{database}/{schema}/{name}/overview", Permission: "snowflake.pipes.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.pipe.overview", Handle: pipeOverview},

		{ID: "snowflake.tasks.list", Method: plugin.MethodGet, Path: "/tasks", Permission: "snowflake.tasks.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.tasks.list", Handle: listTasks},
		{ID: "snowflake.task.overview", Method: plugin.MethodGet, Path: "/tasks/{database}/{schema}/{name}/overview", Permission: "snowflake.tasks.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.task.overview", Handle: taskOverview},
		{ID: "snowflake.task.history", Method: plugin.MethodGet, Path: "/tasks/{database}/{schema}/{name}/history", Permission: "snowflake.tasks.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.task.history", Handle: listTaskHistory},
		{ID: "snowflake.task.resume", Method: plugin.MethodPost, Path: "/tasks/{database}/{schema}/{name}/resume", Permission: "snowflake.tasks.control", Risk: plugin.RiskWrite, AuditEvent: "snowflake.task.resume", Handle: resumeTask},
		{ID: "snowflake.task.suspend", Method: plugin.MethodPost, Path: "/tasks/{database}/{schema}/{name}/suspend", Permission: "snowflake.tasks.control", Risk: plugin.RiskWrite, AuditEvent: "snowflake.task.suspend", Handle: suspendTask},
		{ID: "snowflake.task.execute", Method: plugin.MethodPost, Path: "/tasks/{database}/{schema}/{name}/execute", Permission: "snowflake.tasks.control", Risk: plugin.RiskWrite, AuditEvent: "snowflake.task.execute", Handle: executeTask},

		{ID: "snowflake.streams.list", Method: plugin.MethodGet, Path: "/streams", Permission: "snowflake.streams.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.streams.list", Handle: listStreams},
		{ID: "snowflake.stream.overview", Method: plugin.MethodGet, Path: "/streams/{database}/{schema}/{name}/overview", Permission: "snowflake.streams.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.stream.overview", Handle: streamOverview},

		{ID: "snowflake.queries.list", Method: plugin.MethodGet, Path: "/queries", Permission: "snowflake.queries.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.queries.list", Handle: listQueries},
		{ID: "snowflake.query.overview", Method: plugin.MethodGet, Path: "/queries/{id}/overview", Permission: "snowflake.queries.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.query.overview", Handle: queryOverview},
		{ID: "snowflake.query.abort", Method: plugin.MethodPost, Path: "/queries/{id}/abort", Permission: "snowflake.queries.cancel", Risk: plugin.RiskDestructive, AuditEvent: "snowflake.query.abort", Handle: abortQuery},

		{ID: "snowflake.completion", Method: plugin.MethodGet, Path: "/completion", Permission: "snowflake.databases.read", Risk: plugin.RiskSafe, AuditEvent: "snowflake.completion", Handle: completionRoute},
		{ID: "snowflake.query", Method: plugin.MethodWS, Path: "/query", Permission: "snowflake.query.execute", Risk: plugin.RiskPrivileged, AuditEvent: "snowflake.query", Stream: queryStream},
		{ID: "snowflake.query.cancel", Method: plugin.MethodPost, Path: "/query/cancel", Permission: "snowflake.query.cancel", Risk: plugin.RiskWrite, AuditEvent: "snowflake.query.cancel", Handle: cancelQuery},
	}
}

func snowflakeSession(rc *plugin.RequestContext) (*Session, error) {
	return unwrap(rc.Session)
}

// ---------------------------------------------------------------- input schemas

func databaseCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Database", Fields: []plugin.Field{
		{Key: "name", Label: "Database name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: identifierPattern}}},
		{Key: "transient", Label: "Transient", Type: plugin.FieldToggle, Help: "Transient databases have no fail-safe period and reduced storage cost."},
		{Key: "comment", Label: "Comment", Type: plugin.FieldText},
		{Key: "if_not_exists", Label: "If not exists", Type: plugin.FieldToggle, Default: true},
	}}}}
}

func schemaCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Schema", Fields: []plugin.Field{
		{Key: "name", Label: "Schema name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: identifierPattern}}},
		{Key: "transient", Label: "Transient", Type: plugin.FieldToggle},
		{Key: "managed_access", Label: "Managed access", Type: plugin.FieldToggle, Help: "Only the schema owner may grant privileges on objects in a managed-access schema."},
		{Key: "comment", Label: "Comment", Type: plugin.FieldText},
		{Key: "if_not_exists", Label: "If not exists", Type: plugin.FieldToggle, Default: true},
	}}}}
}

func tableCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Table", Fields: []plugin.Field{
		{Key: "name", Label: "Table name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: identifierPattern}}},
		sqldb.ColumnsArrayField(sqldb.ColumnsField{TypePlaceholder: "VARCHAR", TypeSuggestions: []string{"NUMBER(38,0)", "NUMBER(38,2)", "INT", "BIGINT", "FLOAT", "DOUBLE", "VARCHAR", "VARCHAR(255)", "STRING", "TEXT", "BOOLEAN", "DATE", "TIME", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ", "VARIANT", "OBJECT", "ARRAY", "BINARY", "GEOGRAPHY"}, Default: true, Primary: true, Unique: true}),
		{Key: "transient", Label: "Transient", Type: plugin.FieldToggle},
		{Key: "cluster_by", Label: "Cluster by", Type: plugin.FieldText, Help: "Comma separated clustering key columns."},
		{Key: "comment", Label: "Comment", Type: plugin.FieldText},
		{Key: "if_not_exists", Label: "If not exists", Type: plugin.FieldToggle, Default: true},
	}}}}
}

func tableRenameSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Rename table", Fields: []plugin.Field{
		{Key: "database", Label: "Target database", Type: plugin.FieldText, Help: "Leave blank to keep the current database.", Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: identifierPattern}}},
		{Key: "schema", Label: "Target schema", Type: plugin.FieldText, Help: "Leave blank to keep the current schema.", Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: identifierPattern}}},
		{Key: "name", Label: "New name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: identifierPattern}}},
	}}}}
}

func columnAddSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Column", Fields: []plugin.Field{
		{Key: "name", Label: "Column name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: identifierPattern}}},
		{Key: "type", Label: "Type", Type: plugin.FieldAutocomplete, Required: true, Default: "VARCHAR", Options: typeOptions()},
		{Key: "nullable", Label: "Nullable", Type: plugin.FieldToggle, Default: true},
		{Key: "default", Label: "Default expression", Type: plugin.FieldText},
		{Key: "comment", Label: "Comment", Type: plugin.FieldText},
	}}}}
}

func warehouseResizeSchema() *plugin.Schema {
	options := make([]plugin.Option, 0, len(warehouseSizes))
	for _, size := range warehouseSizes {
		options = append(options, plugin.Option{Label: size, Value: size})
	}
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Warehouse", Fields: []plugin.Field{
		{Key: "size", Label: "Size", Type: plugin.FieldSelect, Required: true, Default: "XSMALL", Options: options},
		{Key: "wait_for_completion", Label: "Wait for completion", Type: plugin.FieldToggle, Help: "Blocks until the resize finishes provisioning."},
	}}}}
}

func roleCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Role", Fields: []plugin.Field{
		{Key: "name", Label: "Role name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: identifierPattern}}},
		{Key: "comment", Label: "Comment", Type: plugin.FieldText},
		{Key: "if_not_exists", Label: "If not exists", Type: plugin.FieldToggle, Default: true},
	}}}}
}

func roleGrantSchema() *plugin.Schema {
	types := make([]plugin.Option, 0, len(grantObjectTypes))
	for _, t := range grantObjectTypes {
		types = append(types, plugin.Option{Label: t, Value: t})
	}
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Grant", Fields: []plugin.Field{
		{Key: "privilege", Label: "Privilege", Type: plugin.FieldAutocomplete, Required: true, Default: "USAGE", Options: []plugin.Option{
			{Label: "USAGE", Value: "USAGE"},
			{Label: "SELECT", Value: "SELECT"},
			{Label: "INSERT", Value: "INSERT"},
			{Label: "UPDATE", Value: "UPDATE"},
			{Label: "DELETE", Value: "DELETE"},
			{Label: "TRUNCATE", Value: "TRUNCATE"},
			{Label: "REFERENCES", Value: "REFERENCES"},
			{Label: "OPERATE", Value: "OPERATE"},
			{Label: "MONITOR", Value: "MONITOR"},
			{Label: "MODIFY", Value: "MODIFY"},
			{Label: "CREATE SCHEMA", Value: "CREATE SCHEMA"},
			{Label: "CREATE TABLE", Value: "CREATE TABLE"},
			{Label: "ALL PRIVILEGES", Value: "ALL PRIVILEGES"},
		}},
		{Key: "granted_on", Label: "On", Type: plugin.FieldSelect, Required: true, Default: "DATABASE", Options: types},
		{Key: "object", Label: "Object", Type: plugin.FieldText, Help: "Qualified object name, such as SALES or SALES.PUBLIC.ORDERS. Leave blank when granting on ACCOUNT.", Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: `^$|^[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*){0,2}$`}}},
		{Key: "with_grant_option", Label: "With grant option", Type: plugin.FieldToggle},
	}}}}
}

func userRoleSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Role", Fields: []plugin.Field{
		{Key: "role", Label: "Role", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: identifierPattern}}},
	}}}}
}

func typeOptions() []plugin.Option {
	types := []string{"NUMBER(38,0)", "INT", "BIGINT", "FLOAT", "DOUBLE", "VARCHAR", "VARCHAR(255)", "STRING", "BOOLEAN", "DATE", "TIME", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ", "VARIANT", "OBJECT", "ARRAY", "BINARY", "GEOGRAPHY"}
	out := make([]plugin.Option, 0, len(types))
	for _, t := range types {
		out = append(out, plugin.Option{Label: t, Value: t})
	}
	return out
}

// ------------------------------------------------------------------ tree routes

func treeDatabases(rc *plugin.RequestContext) (any, error) {
	page, err := databasePage(rc)
	if err != nil {
		return nil, err
	}
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, r := range page.Items {
		name := text(r["name"])
		nodes = append(nodes, plugin.TreeNode{
			Key:            "database:" + name,
			Label:          name,
			Icon:           icon("database"),
			Ref:            &plugin.ResourceIdentity{Kind: "database", Name: name, UID: name},
			ChildrenSource: &plugin.DataSource{RouteID: "snowflake.schemas.tree", Params: map[string]string{"database": name}},
		})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

func treeSchemas(rc *plugin.RequestContext) (any, error) {
	page, err := schemaPage(rc)
	if err != nil {
		return nil, err
	}
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, r := range page.Items {
		database, name := text(r["database"]), text(r["name"])
		nodes = append(nodes, plugin.TreeNode{
			Key:            "schema:" + database + "." + name,
			Label:          name,
			Icon:           icon("folder-tree"),
			Ref:            &plugin.ResourceIdentity{Kind: "schema", Namespace: database, Name: name, UID: database + "." + name},
			ChildrenSource: &plugin.DataSource{RouteID: "snowflake.relations.tree", Params: map[string]string{"database": database, "schema": name}},
		})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

// treeRelations lists a schema's tables and views as one paged set, so a single
// cursor walks both kinds instead of two interleaved lists sharing one cursor.
func treeRelations(rc *plugin.RequestContext) (any, error) {
	page, err := relationPage(rc, relationAll)
	if err != nil {
		return nil, err
	}
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, r := range page.Items {
		ref, ok := r["ref"].(plugin.ResourceIdentity)
		if !ok {
			continue
		}
		name := icon("table-2")
		if ref.Kind == "view" {
			name = icon("panel-top")
		}
		nodes = append(nodes, plugin.TreeNode{Key: ref.Kind + ":" + ref.UID, Label: ref.Name, Icon: name, Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

// ------------------------------------------------------------------ catalog

func informationSchema(database string) string {
	return quoteIdent(database) + ".INFORMATION_SCHEMA"
}

func databaseSpec(database string) selectSpec {
	return selectSpec{
		From: informationSchema(database) + ".DATABASES",
		Columns: []outColumn{
			{Alias: "name", Expr: "DATABASE_NAME"},
			{Alias: "owner", Expr: "DATABASE_OWNER"},
			{Alias: "is_transient", Expr: "IS_TRANSIENT = 'YES'"},
			{Alias: "retention_days", Expr: "RETENTION_TIME"},
			{Alias: "created", Expr: "CREATED"},
			{Alias: "last_altered", Expr: "LAST_ALTERED"},
			{Alias: "comment", Expr: "COMMENT"},
		},
		Order: "DATABASE_NAME",
	}
}

func databasePage(rc *plugin.RequestContext) (plugin.Page[row], error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return plugin.Page[row]{}, err
	}
	page, err := selectPage(rc, s, s.db, databaseSpec(s.opts.Database))
	if err != nil {
		return plugin.Page[row]{}, err
	}
	for _, r := range page.Items {
		name := text(r["name"])
		r["ref"] = plugin.ResourceIdentity{Kind: "database", Name: name, UID: name}
	}
	return page, nil
}

func listDatabases(rc *plugin.RequestContext) (any, error) {
	return databasePage(rc)
}

func databaseOverview(rc *plugin.RequestContext) (any, error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return nil, err
	}
	database, err := safeIdentifier(rc.Param("database"))
	if err != nil {
		return nil, err
	}
	spec := databaseSpec(s.opts.Database)
	spec.Columns = append(spec.Columns, outColumn{Alias: "schemas", Expr: "(SELECT COUNT(*) FROM " + informationSchema(database) + ".SCHEMATA WHERE SCHEMA_NAME <> 'INFORMATION_SCHEMA')"})
	spec.Where = "DATABASE_NAME = ?"
	spec.Args = []any{database}
	return firstRow(rc.Ctx, s, s.db, spec)
}

func schemaSpec(database string) selectSpec {
	return selectSpec{
		From: informationSchema(database) + ".SCHEMATA",
		Columns: []outColumn{
			{Alias: "name", Expr: "SCHEMA_NAME"},
			{Alias: "database", Expr: "CATALOG_NAME"},
			{Alias: "owner", Expr: "SCHEMA_OWNER"},
			{Alias: "is_transient", Expr: "IS_TRANSIENT = 'YES'"},
			{Alias: "managed_access", Expr: "IS_MANAGED_ACCESS = 'YES'"},
			{Alias: "retention_days", Expr: "RETENTION_TIME"},
			{Alias: "created", Expr: "CREATED"},
			{Alias: "last_altered", Expr: "LAST_ALTERED"},
			{Alias: "comment", Expr: "COMMENT"},
		},
		Where: "SCHEMA_NAME <> 'INFORMATION_SCHEMA'",
		Order: "SCHEMA_NAME",
	}
}

func schemaPage(rc *plugin.RequestContext) (plugin.Page[row], error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return plugin.Page[row]{}, err
	}
	database, err := scopedDatabase(rc, s)
	if err != nil {
		return plugin.Page[row]{}, err
	}
	page, err := selectPage(rc, s, s.db, schemaSpec(database))
	if err != nil {
		return plugin.Page[row]{}, err
	}
	for _, r := range page.Items {
		name := text(r["name"])
		r["database"] = database
		r["ref"] = plugin.ResourceIdentity{Kind: "schema", Namespace: database, Name: name, UID: database + "." + name}
	}
	return page, nil
}

func listSchemas(rc *plugin.RequestContext) (any, error) {
	return schemaPage(rc)
}

func schemaOverview(rc *plugin.RequestContext) (any, error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return nil, err
	}
	database, schema, err := schemaIdent(rc)
	if err != nil {
		return nil, err
	}
	spec := schemaSpec(database)
	spec.Where = "SCHEMA_NAME = ?"
	spec.Args = []any{schema}
	return firstRow(rc.Ctx, s, s.db, spec)
}

type relationFilter int

const (
	relationAll relationFilter = iota
	relationTables
	relationViews
)

const viewTypes = "('VIEW', 'MATERIALIZED VIEW')"

func relationSpec(database string, filter relationFilter) selectSpec {
	spec := selectSpec{
		From: informationSchema(database) + ".TABLES",
		Columns: []outColumn{
			{Alias: "name", Expr: "TABLE_NAME"},
			{Alias: "schema", Expr: "TABLE_SCHEMA"},
			{Alias: "database", Expr: "TABLE_CATALOG"},
			{Alias: "kind", Expr: "LOWER(TABLE_TYPE)"},
			{Alias: "rows", Expr: "ROW_COUNT"},
			{Alias: "bytes", Expr: "BYTES"},
			{Alias: "clustering_key", Expr: "CLUSTERING_KEY"},
			{Alias: "is_transient", Expr: "IS_TRANSIENT = 'YES'"},
			{Alias: "retention_days", Expr: "RETENTION_TIME"},
			{Alias: "created", Expr: "CREATED"},
			{Alias: "last_altered", Expr: "LAST_ALTERED"},
			{Alias: "comment", Expr: "COMMENT"},
		},
		Where: "TABLE_SCHEMA <> 'INFORMATION_SCHEMA'",
		Order: "TABLE_SCHEMA, TABLE_NAME",
	}
	switch filter {
	case relationTables:
		spec.Where += " AND TABLE_TYPE NOT IN " + viewTypes
	case relationViews:
		spec.Where += " AND TABLE_TYPE IN " + viewTypes
	case relationAll:
	}
	return spec
}

func relationPage(rc *plugin.RequestContext, filter relationFilter) (plugin.Page[row], error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return plugin.Page[row]{}, err
	}
	database, err := scopedDatabase(rc, s)
	if err != nil {
		return plugin.Page[row]{}, err
	}
	spec := relationSpec(database, filter)
	schema, err := optionalIdentifier(rc.Param("schema"))
	if err != nil {
		return plugin.Page[row]{}, err
	}
	if schema != "" {
		spec.Where += " AND TABLE_SCHEMA = ?"
		spec.Args = append(spec.Args, schema)
	}
	page, err := selectPage(rc, s, s.db, spec)
	if err != nil {
		return plugin.Page[row]{}, err
	}
	for _, r := range page.Items {
		r["ref"] = relationRef(r)
	}
	return page, nil
}

func relationRef(r row) plugin.ResourceIdentity {
	database, schema, name := text(r["database"]), text(r["schema"]), text(r["name"])
	kind := "table"
	if strings.Contains(strings.ToLower(text(r["kind"])), "view") {
		kind = "view"
	}
	return plugin.ResourceIdentity{Kind: kind, Namespace: database, Scope: schema, Name: name, UID: database + "." + schema + "." + name}
}

func listTables(rc *plugin.RequestContext) (any, error) {
	return relationPage(rc, relationTables)
}

func listViews(rc *plugin.RequestContext) (any, error) {
	return relationPage(rc, relationViews)
}

func tableOverview(rc *plugin.RequestContext) (any, error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return nil, err
	}
	database, schema, name, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	spec := relationSpec(database, relationAll)
	spec.Columns = append(spec.Columns, outColumn{Alias: "columns", Expr: "(SELECT COUNT(*) FROM " + informationSchema(database) + ".COLUMNS c WHERE c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME)"})
	spec.From += " t"
	spec.Where = "t.TABLE_SCHEMA = ? AND t.TABLE_NAME = ?"
	spec.Args = []any{schema, name}
	spec.Order = ""
	detail, err := firstRow(rc.Ctx, s, s.db, spec)
	if err != nil {
		return nil, err
	}
	key, err := primaryKeyColumns(rc.Ctx, s, database, schema, name)
	if err != nil {
		return nil, err
	}
	detail["primary_key"] = strings.Join(key, ", ")
	return detail, nil
}

func tableColumnsRoute(rc *plugin.RequestContext) (any, error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return nil, err
	}
	database, schema, name, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	rows, err := columnRows(rc.Ctx, s, database, schema, name)
	if err != nil {
		return nil, err
	}
	key, err := primaryKeyColumns(rc.Ctx, s, database, schema, name)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		column := text(r["name"])
		r["database"], r["schema"], r["table"] = database, schema, name
		r["primary_key"] = slices.Contains(key, column)
		readOnly := slices.Contains(key, column) || sqldb.RedactColumn(column, s.opts.RedactPatterns)
		sqldb.AnnotateTableColumn(r, text(r["type"]), readOnly)
	}
	return broker.PageRows(rc, rows)
}

// columnRows renders the declared type the way Snowflake spells it, so the grid
// shows VARCHAR(255) rather than a bare VARCHAR with a separate length column.
func columnRows(ctx context.Context, s *Session, database, schema, name string) ([]row, error) {
	raw, err := queryRows(ctx, s, `
SELECT COLUMN_NAME AS "name", DATA_TYPE AS "data_type", IS_NULLABLE = 'YES' AS "nullable",
       COLUMN_DEFAULT AS "default", ORDINAL_POSITION AS "position", COMMENT AS "comment",
       CHARACTER_MAXIMUM_LENGTH AS "length", NUMERIC_PRECISION AS "precision", NUMERIC_SCALE AS "scale"
FROM `+informationSchema(database)+`.COLUMNS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
ORDER BY ORDINAL_POSITION`, []any{schema, name})
	if err != nil {
		return nil, err
	}
	for _, r := range raw {
		r["type"] = columnType(r)
		delete(r, "data_type")
		delete(r, "length")
		delete(r, "precision")
		delete(r, "scale")
	}
	return raw, nil
}

func columnType(r row) string {
	base := strings.ToUpper(text(r["data_type"]))
	switch base {
	case "TEXT", "VARCHAR", "CHAR", "BINARY", "VARBINARY":
		if length := text(r["length"]); length != "" {
			return base + "(" + length + ")"
		}
	case "NUMBER", "DECIMAL", "NUMERIC":
		precision, scale := text(r["precision"]), text(r["scale"])
		if precision != "" && scale != "" {
			return base + "(" + precision + "," + scale + ")"
		}
	}
	return base
}

func tableDDL(rc *plugin.RequestContext) (any, error) {
	return objectDDL(rc, "TABLE")
}

func viewDDL(rc *plugin.RequestContext) (any, error) {
	return objectDDL(rc, "VIEW")
}

// objectDDL renders an object's definition through GET_DDL. The object type is a
// literal from this package's own vocabulary, and the name is passed as a
// double-quoted identifier inside the literal so an object created with a
// case-sensitive name resolves instead of being uppercased away.
func objectDDL(rc *plugin.RequestContext, objectType string) (any, error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return nil, err
	}
	database, schema, name, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `SELECT GET_DDL('`+objectType+`', `+quoteLiteral(qualifiedQuoted(database, schema, name))+`, TRUE) AS "definition"`, nil)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, plugin.ErrNotFound
	}
	return row{
		"database":   database,
		"schema":     schema,
		"name":       name,
		"definition": rows[0]["definition"],
	}, nil
}

// ------------------------------------------------------------------ table data

func tableRows(rc *plugin.RequestContext) (any, error) { return relationRows(rc, true) }

// viewRows reads a view the same way, minus the two operations that only make
// sense for a base table: a view has no primary key to edit rows by, and its
// COUNT(*) scans rather than reading table metadata, so no total is claimed.
func viewRows(rc *plugin.RequestContext) (any, error) { return relationRows(rc, false) }

func relationRows(rc *plugin.RequestContext, table bool) (any, error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return nil, err
	}
	database, schema, name, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	req, err := rc.Page()
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
	columns, err := columnRows(rc.Ctx, s, database, schema, name)
	if err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, plugin.ErrNotFound
	}
	// Redacted columns are masked before the page is returned, so searching them
	// would match rows on a value the grid never showed.
	names := make([]string, 0, len(columns))
	searchable := make([]string, 0, len(columns))
	for _, c := range columns {
		column := text(c["name"])
		names = append(names, column)
		if !sqldb.RedactColumn(column, s.opts.RedactPatterns) {
			searchable = append(searchable, quoteIdent(column))
		}
	}

	var key []string
	if table {
		if key, err = primaryKeyColumns(rc.Ctx, s, database, schema, name); err != nil {
			return nil, err
		}
	}

	target := qualifiedQuoted(database, schema, name)
	where, args := searchClause(searchable, req.Search())
	clause := ""
	if where != "" {
		clause = " WHERE " + where
	}
	// Offset paging over an unordered Snowflake result can shuffle rows between
	// pages, so the primary key is both the default order and the tiebreaker
	// under a requested sort. A relation without one cannot be ordered
	// deterministically and pages on the engine's own order.
	tiebreak := ""
	if len(key) > 0 {
		quotedKey := make([]string, len(key))
		for i, column := range key {
			quotedKey[i] = quoteIdent(column)
		}
		tiebreak = strings.Join(quotedKey, ", ")
	}
	orderBy := ""
	if len(req.Sort) > 0 {
		field := req.Sort[0].Field
		if !slices.Contains(names, field) {
			return nil, fmt.Errorf("%w: cannot sort by %q", plugin.ErrInvalidInput, field)
		}
		orderBy = " ORDER BY " + quoteIdent(field) + direction(req.Sort[0].Desc) + " NULLS LAST"
		if tiebreak != "" {
			orderBy += ", " + tiebreak
		}
	} else if tiebreak != "" {
		orderBy = " ORDER BY " + tiebreak
	}

	// An unfiltered COUNT(*) is answered from Snowflake's table metadata, so it
	// stays authoritative and cheap. A filtered count would scan, so that path
	// pages on the sentinel row and leaves Total unset.
	var total *int
	if table && where == "" {
		var count int64
		if err := s.db.QueryRowContext(rc.Ctx, "SELECT COUNT(*) FROM "+target).Scan(&count); err != nil {
			return nil, snowflakeErr(err)
		}
		n := int(count)
		total = &n
	}

	rows, err := queryRows(rc.Ctx, s, "SELECT * FROM "+target+clause+orderBy+pageClause(limit+1, offset), args)
	if err != nil {
		return nil, err
	}
	next := ""
	if len(rows) > limit {
		rows = rows[:limit]
		next = encodeCursor(offset + limit)
	}
	attachRowKeys(rows, key, s.opts.RedactPatterns)
	redactRows(rows, s.opts.RedactPatterns)
	return plugin.Page[row]{Items: rows, NextCursor: next, Total: total}, nil
}

func insertRow(rc *plugin.RequestContext) (any, error) {
	s, target, _, m, err := rowMutationInput(rc)
	if err != nil {
		return nil, err
	}
	stmt, args, err := dialect.Insert(target, m.Values)
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(rc.Ctx, stmt, args...); err != nil {
		return nil, snowflakeErr(err)
	}
	return actionResult{OK: true, Message: "Row inserted."}, nil
}

func updateRow(rc *plugin.RequestContext) (any, error) {
	s, target, key, m, err := rowMutationInput(rc)
	if err != nil {
		return nil, err
	}
	if err := sqldb.ValidateRowKey(key, m.Key); err != nil {
		return nil, err
	}
	stmt, args, err := dialect.Update(target, m.Key, m.Values)
	if err != nil {
		return nil, err
	}
	res, err := s.db.ExecContext(rc.Ctx, stmt, args...)
	if err != nil {
		return nil, snowflakeErr(err)
	}
	return mutationResult(res, "Row updated."), nil
}

func deleteRow(rc *plugin.RequestContext) (any, error) {
	s, target, key, m, err := rowMutationInput(rc)
	if err != nil {
		return nil, err
	}
	if err := sqldb.ValidateRowKey(key, m.Key); err != nil {
		return nil, err
	}
	stmt, args, err := dialect.Delete(target, m.Key)
	if err != nil {
		return nil, err
	}
	res, err := s.db.ExecContext(rc.Ctx, stmt, args...)
	if err != nil {
		return nil, snowflakeErr(err)
	}
	return mutationResult(res, "Row deleted."), nil
}

func mutationResult(res sql.Result, message string) actionResult {
	if affected, err := res.RowsAffected(); err == nil {
		return actionResult{OK: true, Message: fmt.Sprintf("%s (%d row(s)).", strings.TrimSuffix(message, "."), affected)}
	}
	return actionResult{OK: true, Message: message}
}

// rowMutationInput resolves the target table, its declared primary key, and the
// uniform mutation body, applying the read-only gate that governs every write.
func rowMutationInput(rc *plugin.RequestContext) (*Session, string, []string, sqldb.RowMutation, error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return nil, "", nil, sqldb.RowMutation{}, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, "", nil, sqldb.RowMutation{}, err
	}
	database, schema, name, err := objectIdent(rc)
	if err != nil {
		return nil, "", nil, sqldb.RowMutation{}, err
	}
	var m sqldb.RowMutation
	if err := rc.Bind(&m); err != nil {
		return nil, "", nil, sqldb.RowMutation{}, err
	}
	if err := ensureWritableColumns(m.Values, s.opts.RedactPatterns); err != nil {
		return nil, "", nil, sqldb.RowMutation{}, err
	}
	key, err := primaryKeyColumns(rc.Ctx, s, database, schema, name)
	if err != nil {
		return nil, "", nil, sqldb.RowMutation{}, err
	}
	return s, qualifiedQuoted(database, schema, name), key, m, nil
}

// primaryKeyColumns returns the table's declared primary key in key order.
// Snowflake records but does not enforce primary keys, so a table without one
// keeps the data grid read-only rather than letting a mutation match by an
// arbitrary column.
func primaryKeyColumns(ctx context.Context, s *Session, database, schema, name string) ([]string, error) {
	rows, err := queryRows(ctx, s, "SHOW PRIMARY KEYS IN TABLE "+qualifiedQuoted(database, schema, name), nil)
	if err != nil {
		// This lookup only enriches the grid. Table kinds that carry no
		// constraint metadata (external, dynamic, Iceberg) reject SHOW PRIMARY
		// KEYS outright, and a caller that cannot reach Snowflake at all fails on
		// its own query moments later, so no key here simply means a read-only
		// grid.
		return nil, nil
	}
	sort.SliceStable(rows, func(i, j int) bool {
		return number(rows[i]["key_sequence"]) < number(rows[j]["key_sequence"])
	})
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		if column := text(r["column_name"]); column != "" {
			out = append(out, column)
		}
	}
	return out, nil
}

// attachRowKeys tags each row with the primary-key values the editable grid
// echoes back for UPDATE/DELETE. Tables without a primary key, or whose key is
// itself sensitive, stay read-only.
func attachRowKeys(rows []row, key, patterns []string) {
	if len(key) == 0 || sqldb.AnyColumnRedacted(key, patterns) {
		return
	}
	for _, r := range rows {
		k := map[string]any{}
		for _, column := range key {
			k[column] = r[column]
		}
		r["_key"] = k
	}
}

// ------------------------------------------------------------------ warehouses

func warehouseSpec() selectSpec {
	return selectSpec{
		From: resultScanFrom,
		Columns: []outColumn{
			{Alias: "name", Expr: `"name"`},
			{Alias: "state", Expr: `UPPER("state")`},
			{Alias: "size", Expr: `"size"`},
			{Alias: "kind", Expr: `"type"`},
			{Alias: "running", Expr: `"running"`},
			{Alias: "queued", Expr: `"queued"`},
			{Alias: "clusters", Expr: `"started_clusters"`},
			{Alias: "started_clusters", Expr: `"started_clusters"`},
			{Alias: "min_clusters", Expr: `"min_cluster_count"`},
			{Alias: "max_clusters", Expr: `"max_cluster_count"`},
			{Alias: "auto_suspend", Expr: `"auto_suspend"`},
			{Alias: "auto_resume", Expr: `TO_BOOLEAN("auto_resume")`},
			{Alias: "resource_monitor", Expr: `"resource_monitor"`},
			{Alias: "scaling_policy", Expr: `"scaling_policy"`},
			{Alias: "owner", Expr: `"owner"`},
			{Alias: "comment", Expr: `"comment"`},
			{Alias: "created", Expr: `"created_on"`},
			{Alias: "resumed", Expr: `"resumed_on"`},
		},
		Order: `"name"`,
	}
}

func listWarehouses(rc *plugin.RequestContext) (any, error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return nil, err
	}
	page, err := showPage(rc, s, "SHOW WAREHOUSES", warehouseSpec())
	if err != nil {
		return nil, err
	}
	for _, r := range page.Items {
		name := text(r["name"])
		r["ref"] = plugin.ResourceIdentity{Kind: "warehouse", Name: name, UID: name}
	}
	return page, nil
}

func warehouseOverview(rc *plugin.RequestContext) (any, error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return nil, err
	}
	name, err := safeIdentifier(rc.Param("warehouse"))
	if err != nil {
		return nil, err
	}
	detail, err := showOne(rc.Ctx, s, "SHOW WAREHOUSES LIKE "+quoteLiteral(name), warehouseSpec(), `"name"`, name)
	if err != nil {
		return nil, err
	}
	started, maximum := number(detail["started_clusters"]), number(detail["max_clusters"])
	detail["cluster_pct"] = percent(started, maximum)
	return detail, nil
}

func resumeWarehouse(rc *plugin.RequestContext) (any, error) {
	name, err := safeIdentifier(rc.Param("warehouse"))
	if err != nil {
		return nil, err
	}
	return execStatement(rc, "ALTER WAREHOUSE "+quoteIdent(name)+" RESUME IF SUSPENDED", "Warehouse resumed.")
}

func suspendWarehouse(rc *plugin.RequestContext) (any, error) {
	name, err := safeIdentifier(rc.Param("warehouse"))
	if err != nil {
		return nil, err
	}
	return execStatement(rc, "ALTER WAREHOUSE "+quoteIdent(name)+" SUSPEND", "Warehouse suspended.")
}

func resizeWarehouse(rc *plugin.RequestContext) (any, error) {
	name, err := safeIdentifier(rc.Param("warehouse"))
	if err != nil {
		return nil, err
	}
	var req struct {
		Size              string `json:"size" validate:"required"`
		WaitForCompletion bool   `json:"wait_for_completion"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	stmt, err := buildWarehouseResize(name, req.Size, req.WaitForCompletion)
	if err != nil {
		return nil, err
	}
	return execStatement(rc, stmt, "Warehouse resized.")
}

// buildWarehouseResize renders ALTER WAREHOUSE ... SET WAREHOUSE_SIZE. The size
// is matched against Snowflake's fixed size vocabulary, so it can be embedded as
// a literal that ALTER accepts (SET takes no bound parameters).
func buildWarehouseResize(name, size string, wait bool) (string, error) {
	normalized := strings.ToUpper(strings.TrimSpace(strings.ReplaceAll(size, "-", "")))
	normalized = strings.ReplaceAll(normalized, " ", "")
	if !slices.Contains(warehouseSizes, normalized) {
		return "", fmt.Errorf("%w: unsupported warehouse size %q", plugin.ErrInvalidInput, size)
	}
	stmt := "ALTER WAREHOUSE " + quoteIdent(name) + " SET WAREHOUSE_SIZE = " + quoteLiteral(normalized)
	if wait {
		stmt += " WAIT_FOR_COMPLETION = TRUE"
	}
	return stmt, nil
}

// ------------------------------------------------------------------ roles

func roleSpec() selectSpec {
	return selectSpec{
		From: resultScanFrom,
		Columns: []outColumn{
			{Alias: "name", Expr: `"name"`},
			{Alias: "owner", Expr: `"owner"`},
			{Alias: "assigned_to_users", Expr: `"assigned_to_users"`},
			{Alias: "granted_to_roles", Expr: `"granted_to_roles"`},
			{Alias: "granted_roles", Expr: `"granted_roles"`},
			{Alias: "created", Expr: `"created_on"`},
			{Alias: "comment", Expr: `"comment"`},
		},
		Order: `"name"`,
	}
}

func listRoles(rc *plugin.RequestContext) (any, error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return nil, err
	}
	page, err := showPage(rc, s, "SHOW ROLES", roleSpec())
	if err != nil {
		return nil, err
	}
	for _, r := range page.Items {
		name := text(r["name"])
		r["ref"] = plugin.ResourceIdentity{Kind: "role", Name: name, UID: name}
	}
	return page, nil
}

func roleOverview(rc *plugin.RequestContext) (any, error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return nil, err
	}
	name, err := safeIdentifier(rc.Param("role"))
	if err != nil {
		return nil, err
	}
	return showOne(rc.Ctx, s, "SHOW ROLES LIKE "+quoteLiteral(name), roleSpec(), `"name"`, name)
}

func listRoleGrants(rc *plugin.RequestContext) (any, error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return nil, err
	}
	name, err := safeIdentifier(rc.Param("role"))
	if err != nil {
		return nil, err
	}
	spec := selectSpec{
		From: resultScanFrom,
		Columns: []outColumn{
			{Alias: "privilege", Expr: `"privilege"`},
			{Alias: "granted_on", Expr: `"granted_on"`},
			{Alias: "name", Expr: `"name"`},
			{Alias: "grant_option", Expr: `TO_BOOLEAN("grant_option")`},
			{Alias: "granted_by", Expr: `"granted_by"`},
			{Alias: "created", Expr: `"created_on"`},
		},
		Order: `"granted_on", "name", "privilege"`,
	}
	return showPage(rc, s, "SHOW GRANTS TO ROLE "+quoteIdent(name), spec)
}

func createRole(rc *plugin.RequestContext) (any, error) {
	var req struct {
		Name        string `json:"name" validate:"required"`
		Comment     string `json:"comment"`
		IfNotExists bool   `json:"if_not_exists"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name, err := safeIdentifier(req.Name)
	if err != nil {
		return nil, err
	}
	stmt := "CREATE ROLE "
	if req.IfNotExists {
		stmt += "IF NOT EXISTS "
	}
	stmt += quoteIdent(name) + comment(req.Comment)
	return execStatement(rc, stmt, "Role created.")
}

func dropRole(rc *plugin.RequestContext) (any, error) {
	name, err := safeIdentifier(rc.Param("role"))
	if err != nil {
		return nil, err
	}
	return execStatement(rc, "DROP ROLE IF EXISTS "+quoteIdent(name), "Role dropped.")
}

func grantRolePrivilege(rc *plugin.RequestContext) (any, error) {
	name, err := safeIdentifier(rc.Param("role"))
	if err != nil {
		return nil, err
	}
	var req struct {
		Privilege       string `json:"privilege" validate:"required"`
		GrantedOn       string `json:"granted_on" validate:"required"`
		Object          string `json:"object"`
		WithGrantOption bool   `json:"with_grant_option"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	stmt, err := buildRoleGrant(false, name, req.Privilege, req.GrantedOn, req.Object, req.WithGrantOption)
	if err != nil {
		return nil, err
	}
	return execStatement(rc, stmt, "Privilege granted.")
}

func revokeRolePrivilege(rc *plugin.RequestContext) (any, error) {
	name, err := safeIdentifier(rc.Param("role"))
	if err != nil {
		return nil, err
	}
	stmt, err := buildRoleGrant(true, name, rc.Param("privilege"), rc.Param("granted_on"), rc.Param("object"), false)
	if err != nil {
		return nil, err
	}
	return execStatement(rc, stmt, "Privilege revoked.")
}

// buildRoleGrant renders GRANT/REVOKE <privilege> ON <object type> <object>
// TO/FROM ROLE <role>. Privilege and object type are screened against the word
// grammar SHOW GRANTS reports (and its underscored spellings are turned back
// into keywords); the object is re-quoted part by part so a catalog value can
// never carry SQL of its own.
func buildRoleGrant(revoke bool, role, privilege, grantedOn, object string, withGrantOption bool) (string, error) {
	ident, err := safeIdentifier(role)
	if err != nil {
		return "", err
	}
	priv, err := grantWord(privilege, "privilege")
	if err != nil {
		return "", err
	}
	target, err := grantWord(grantedOn, "object type")
	if err != nil {
		return "", err
	}
	on := " ON " + target
	if target != "ACCOUNT" {
		qualifiedName, err := qualifiedObject(object)
		if err != nil {
			return "", err
		}
		on += " " + qualifiedName
	} else if strings.TrimSpace(object) != "" {
		return "", fmt.Errorf("%w: an ACCOUNT grant takes no object name", plugin.ErrInvalidInput)
	}
	if revoke {
		return "REVOKE " + priv + on + " FROM ROLE " + quoteIdent(ident), nil
	}
	stmt := "GRANT " + priv + on + " TO ROLE " + quoteIdent(ident)
	if withGrantOption {
		stmt += " WITH GRANT OPTION"
	}
	return stmt, nil
}

func grantWord(raw, what string) (string, error) {
	word := strings.TrimSpace(raw)
	if !grantWordRE.MatchString(word) {
		return "", fmt.Errorf("%w: unsafe %s %q", plugin.ErrInvalidInput, what, raw)
	}
	return strings.ToUpper(strings.ReplaceAll(word, "_", " ")), nil
}

// qualifiedObject re-quotes a one to three part object name. Values that are not
// plain identifiers are refused rather than passed through, so no catalog value
// is ever interpolated verbatim.
func qualifiedObject(raw string) (string, error) {
	parts := strings.Split(strings.TrimSpace(raw), ".")
	if len(parts) == 0 || len(parts) > 3 {
		return "", fmt.Errorf("%w: object name must have at most three parts", plugin.ErrInvalidInput)
	}
	out := make([]string, len(parts))
	for i, part := range parts {
		ident, err := safeIdentifier(part)
		if err != nil {
			return "", err
		}
		out[i] = quoteIdent(ident)
	}
	return strings.Join(out, "."), nil
}

// ------------------------------------------------------------------ users

func userSpec() selectSpec {
	return selectSpec{
		From: resultScanFrom,
		Columns: []outColumn{
			{Alias: "name", Expr: `"name"`},
			{Alias: "login_name", Expr: `"login_name"`},
			{Alias: "display_name", Expr: `"display_name"`},
			{Alias: "email", Expr: `"email"`},
			{Alias: "disabled", Expr: `TO_BOOLEAN("disabled")`},
			{Alias: "must_change_password", Expr: `TO_BOOLEAN("must_change_password")`},
			{Alias: "has_rsa_public_key", Expr: `TO_BOOLEAN("has_rsa_public_key")`},
			{Alias: "default_role", Expr: `"default_role"`},
			{Alias: "default_warehouse", Expr: `"default_warehouse"`},
			{Alias: "default_namespace", Expr: `"default_namespace"`},
			{Alias: "last_success_login", Expr: `"last_success_login"`},
			{Alias: "created", Expr: `"created_on"`},
			{Alias: "owner", Expr: `"owner"`},
			{Alias: "comment", Expr: `"comment"`},
		},
		Order: `"name"`,
	}
}

func listUsers(rc *plugin.RequestContext) (any, error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return nil, err
	}
	page, err := showPage(rc, s, "SHOW USERS", userSpec())
	if err != nil {
		return nil, err
	}
	for _, r := range page.Items {
		name := text(r["name"])
		r["ref"] = plugin.ResourceIdentity{Kind: "user", Name: name, UID: name}
	}
	return page, nil
}

func userOverview(rc *plugin.RequestContext) (any, error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return nil, err
	}
	name, err := safeIdentifier(rc.Param("user"))
	if err != nil {
		return nil, err
	}
	return showOne(rc.Ctx, s, "SHOW USERS LIKE "+quoteLiteral(name), userSpec(), `"name"`, name)
}

func listUserGrants(rc *plugin.RequestContext) (any, error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return nil, err
	}
	name, err := safeIdentifier(rc.Param("user"))
	if err != nil {
		return nil, err
	}
	spec := selectSpec{
		From: resultScanFrom,
		Columns: []outColumn{
			{Alias: "role", Expr: `"role"`},
			{Alias: "granted_by", Expr: `"granted_by"`},
			{Alias: "created", Expr: `"created_on"`},
		},
		Order: `"role"`,
	}
	return showPage(rc, s, "SHOW GRANTS TO USER "+quoteIdent(name), spec)
}

func grantUserRole(rc *plugin.RequestContext) (any, error) {
	user, err := safeIdentifier(rc.Param("user"))
	if err != nil {
		return nil, err
	}
	var req struct {
		Role string `json:"role" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	role, err := safeIdentifier(req.Role)
	if err != nil {
		return nil, err
	}
	return execStatement(rc, "GRANT ROLE "+quoteIdent(role)+" TO USER "+quoteIdent(user), "Role granted.")
}

func revokeUserRole(rc *plugin.RequestContext) (any, error) {
	user, err := safeIdentifier(rc.Param("user"))
	if err != nil {
		return nil, err
	}
	role, err := safeIdentifier(rc.Param("role"))
	if err != nil {
		return nil, err
	}
	return execStatement(rc, "REVOKE ROLE "+quoteIdent(role)+" FROM USER "+quoteIdent(user), "Role revoked.")
}

// ------------------------------------------------------------------ stages

func stageSpec(database string) selectSpec {
	return selectSpec{
		From: informationSchema(database) + ".STAGES",
		Columns: []outColumn{
			{Alias: "name", Expr: "STAGE_NAME"},
			{Alias: "schema", Expr: "STAGE_SCHEMA"},
			{Alias: "database", Expr: "STAGE_CATALOG"},
			{Alias: "kind", Expr: "STAGE_TYPE"},
			{Alias: "url", Expr: "STAGE_URL"},
			{Alias: "region", Expr: "STAGE_REGION"},
			{Alias: "owner", Expr: "STAGE_OWNER"},
			{Alias: "created", Expr: "CREATED"},
			{Alias: "last_altered", Expr: "LAST_ALTERED"},
			{Alias: "comment", Expr: "COMMENT"},
		},
		Where: "STAGE_SCHEMA <> 'INFORMATION_SCHEMA'",
		Order: "STAGE_SCHEMA, STAGE_NAME",
	}
}

func listStages(rc *plugin.RequestContext) (any, error) {
	return schemaScopedList(rc, "stage", stageSpec, "STAGE_SCHEMA")
}

func stageOverview(rc *plugin.RequestContext) (any, error) {
	return schemaScopedOverview(rc, stageSpec, "STAGE_SCHEMA", "STAGE_NAME")
}

func listStageFiles(rc *plugin.RequestContext) (any, error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return nil, err
	}
	database, schema, name, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	spec := selectSpec{
		From: resultScanFrom,
		Columns: []outColumn{
			{Alias: "name", Expr: `"name"`},
			{Alias: "size", Expr: `"size"`},
			{Alias: "md5", Expr: `"md5"`},
			{Alias: "last_modified", Expr: `"last_modified"`},
		},
		Order: `"name"`,
	}
	return showPage(rc, s, "LIST @"+qualifiedQuoted(database, schema, name), spec)
}

// ------------------------------------------------------------------ file formats

func fileFormatSpec(database string) selectSpec {
	return selectSpec{
		From: informationSchema(database) + ".FILE_FORMATS",
		Columns: []outColumn{
			{Alias: "name", Expr: "FILE_FORMAT_NAME"},
			{Alias: "schema", Expr: "FILE_FORMAT_SCHEMA"},
			{Alias: "database", Expr: "FILE_FORMAT_CATALOG"},
			{Alias: "kind", Expr: "FILE_FORMAT_TYPE"},
			{Alias: "owner", Expr: "FILE_FORMAT_OWNER"},
			{Alias: "created", Expr: "CREATED"},
			{Alias: "last_altered", Expr: "LAST_ALTERED"},
			{Alias: "comment", Expr: "COMMENT"},
		},
		Where: "FILE_FORMAT_SCHEMA <> 'INFORMATION_SCHEMA'",
		Order: "FILE_FORMAT_SCHEMA, FILE_FORMAT_NAME",
	}
}

func listFileFormats(rc *plugin.RequestContext) (any, error) {
	return schemaScopedList(rc, "file_format", fileFormatSpec, "FILE_FORMAT_SCHEMA")
}

func fileFormatOverview(rc *plugin.RequestContext) (any, error) {
	return schemaScopedOverview(rc, fileFormatSpec, "FILE_FORMAT_SCHEMA", "FILE_FORMAT_NAME")
}

// ------------------------------------------------------------------ pipes

func pipeSpec(database string) selectSpec {
	return selectSpec{
		From: informationSchema(database) + ".PIPES",
		Columns: []outColumn{
			{Alias: "name", Expr: "PIPE_NAME"},
			{Alias: "schema", Expr: "PIPE_SCHEMA"},
			{Alias: "database", Expr: "PIPE_CATALOG"},
			{Alias: "definition", Expr: "DEFINITION"},
			{Alias: "auto_ingest", Expr: "IS_AUTOINGEST_ENABLED = 'YES'"},
			{Alias: "notification_channel", Expr: "NOTIFICATION_CHANNEL_NAME"},
			{Alias: "invalid_reason", Expr: "INVALID_REASON"},
			{Alias: "owner", Expr: "PIPE_OWNER"},
			{Alias: "created", Expr: "CREATED"},
			{Alias: "last_altered", Expr: "LAST_ALTERED"},
			{Alias: "comment", Expr: "COMMENT"},
		},
		Where: "PIPE_SCHEMA <> 'INFORMATION_SCHEMA'",
		Order: "PIPE_SCHEMA, PIPE_NAME",
	}
}

func listPipes(rc *plugin.RequestContext) (any, error) {
	return schemaScopedList(rc, "pipe", pipeSpec, "PIPE_SCHEMA")
}

func pipeOverview(rc *plugin.RequestContext) (any, error) {
	return schemaScopedOverview(rc, pipeSpec, "PIPE_SCHEMA", "PIPE_NAME")
}

// ------------------------------------------------------------------ tasks

func taskSpec() selectSpec {
	return selectSpec{
		From: resultScanFrom,
		Columns: []outColumn{
			{Alias: "name", Expr: `"name"`},
			{Alias: "schema", Expr: `"schema_name"`},
			{Alias: "database", Expr: `"database_name"`},
			{Alias: "state", Expr: `UPPER("state")`},
			{Alias: "warehouse", Expr: `"warehouse"`},
			{Alias: "schedule", Expr: `"schedule"`},
			{Alias: "predecessors", Expr: `TO_VARCHAR("predecessors")`},
			{Alias: "condition", Expr: `"condition"`},
			{Alias: "definition", Expr: `"definition"`},
			{Alias: "last_committed", Expr: `"last_committed_on"`},
			{Alias: "owner", Expr: `"owner"`},
			{Alias: "comment", Expr: `"comment"`},
		},
		Order: `"database_name", "schema_name", "name"`,
	}
}

func listTasks(rc *plugin.RequestContext) (any, error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return nil, err
	}
	scope, err := showScope(rc, s, "TASKS")
	if err != nil {
		return nil, err
	}
	page, err := showPage(rc, s, scope, taskSpec())
	if err != nil {
		return nil, err
	}
	for _, r := range page.Items {
		r["ref"] = objectRef("task", r)
	}
	return page, nil
}

func taskOverview(rc *plugin.RequestContext) (any, error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return nil, err
	}
	database, schema, name, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	return showOne(rc.Ctx, s, "SHOW TASKS LIKE "+quoteLiteral(name)+" IN SCHEMA "+quoteIdent(database)+"."+quoteIdent(schema), taskSpec(), `"name"`, name)
}

func listTaskHistory(rc *plugin.RequestContext) (any, error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return nil, err
	}
	database, schema, name, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	spec := selectSpec{
		From: "TABLE(" + informationSchema(database) + ".TASK_HISTORY(TASK_NAME => " + quoteLiteral(name) + ", RESULT_LIMIT => " + strconv.Itoa(s.opts.HistoryWindow) + "))",
		Columns: []outColumn{
			{Alias: "name", Expr: "NAME"},
			{Alias: "schema", Expr: "SCHEMA_NAME"},
			{Alias: "database", Expr: "DATABASE_NAME"},
			{Alias: "state", Expr: "STATE"},
			{Alias: "scheduled_time", Expr: "SCHEDULED_TIME"},
			{Alias: "completed_time", Expr: "COMPLETED_TIME"},
			{Alias: "query_id", Expr: "QUERY_ID"},
			{Alias: "error_code", Expr: "ERROR_CODE"},
			{Alias: "error_message", Expr: "ERROR_MESSAGE"},
		},
		Where: "SCHEMA_NAME = ?",
		Order: "SCHEDULED_TIME DESC",
	}
	spec.Args = []any{schema}
	page, err := selectPage(rc, s, s.db, spec)
	if err != nil {
		return nil, err
	}
	for _, r := range page.Items {
		state := strings.ToUpper(text(r["state"]))
		r["title"] = text(r["name"]) + " " + strings.ToLower(state)
		r["detail"] = text(r["error_message"])
		r["severity"] = taskRunSeverity(state)
		r["icon"] = "calendar-clock"
		r["target"] = text(r["database"]) + "." + text(r["schema"]) + "." + text(r["name"])
	}
	return page, nil
}

func taskRunSeverity(state string) string {
	switch state {
	case "SUCCEEDED":
		return "success"
	case "FAILED", "CANCELLED":
		return "danger"
	case "SKIPPED":
		return "warn"
	default:
		return "info"
	}
}

func resumeTask(rc *plugin.RequestContext) (any, error) {
	target, err := objectTarget(rc)
	if err != nil {
		return nil, err
	}
	return execStatement(rc, "ALTER TASK "+target+" RESUME", "Task resumed.")
}

func suspendTask(rc *plugin.RequestContext) (any, error) {
	target, err := objectTarget(rc)
	if err != nil {
		return nil, err
	}
	return execStatement(rc, "ALTER TASK "+target+" SUSPEND", "Task suspended.")
}

func executeTask(rc *plugin.RequestContext) (any, error) {
	target, err := objectTarget(rc)
	if err != nil {
		return nil, err
	}
	return execStatement(rc, "EXECUTE TASK "+target, "Task run submitted.")
}

// ------------------------------------------------------------------ streams

func streamSpec() selectSpec {
	return selectSpec{
		From: resultScanFrom,
		Columns: []outColumn{
			{Alias: "name", Expr: `"name"`},
			{Alias: "schema", Expr: `"schema_name"`},
			{Alias: "database", Expr: `"database_name"`},
			{Alias: "table_name", Expr: `"table_name"`},
			{Alias: "source_type", Expr: `"source_type"`},
			{Alias: "mode", Expr: `"mode"`},
			{Alias: "stale", Expr: `TO_BOOLEAN("stale")`},
			{Alias: "stale_after", Expr: `"stale_after"`},
			{Alias: "owner", Expr: `"owner"`},
			{Alias: "created", Expr: `"created_on"`},
			{Alias: "comment", Expr: `"comment"`},
		},
		Order: `"database_name", "schema_name", "name"`,
	}
}

func listStreams(rc *plugin.RequestContext) (any, error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return nil, err
	}
	scope, err := showScope(rc, s, "STREAMS")
	if err != nil {
		return nil, err
	}
	page, err := showPage(rc, s, scope, streamSpec())
	if err != nil {
		return nil, err
	}
	for _, r := range page.Items {
		r["ref"] = objectRef("stream", r)
	}
	return page, nil
}

func streamOverview(rc *plugin.RequestContext) (any, error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return nil, err
	}
	database, schema, name, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	return showOne(rc.Ctx, s, "SHOW STREAMS LIKE "+quoteLiteral(name)+" IN SCHEMA "+quoteIdent(database)+"."+quoteIdent(schema), streamSpec(), `"name"`, name)
}

// ------------------------------------------------------------------ query history

func querySpec(database string, window int) selectSpec {
	return selectSpec{
		From: "TABLE(" + informationSchema(database) + ".QUERY_HISTORY(RESULT_LIMIT => " + strconv.Itoa(window) + "))",
		Columns: []outColumn{
			{Alias: "query_id", Expr: "QUERY_ID"},
			{Alias: "query_text", Expr: "QUERY_TEXT"},
			{Alias: "query_type", Expr: "QUERY_TYPE"},
			{Alias: "status", Expr: "EXECUTION_STATUS"},
			{Alias: "user", Expr: "USER_NAME"},
			{Alias: "role", Expr: "ROLE_NAME"},
			{Alias: "warehouse", Expr: "WAREHOUSE_NAME"},
			{Alias: "warehouse_size", Expr: "WAREHOUSE_SIZE"},
			{Alias: "database", Expr: "DATABASE_NAME"},
			{Alias: "schema", Expr: "SCHEMA_NAME"},
			{Alias: "query_tag", Expr: "QUERY_TAG"},
			{Alias: "start_time", Expr: "START_TIME"},
			{Alias: "end_time", Expr: "END_TIME"},
			{Alias: "elapsed_ms", Expr: "TOTAL_ELAPSED_TIME"},
			{Alias: "compilation_ms", Expr: "COMPILATION_TIME"},
			{Alias: "execution_ms", Expr: "EXECUTION_TIME"},
			{Alias: "queued_ms", Expr: "COALESCE(QUEUED_OVERLOAD_TIME, 0) + COALESCE(QUEUED_PROVISIONING_TIME, 0)"},
			{Alias: "bytes_scanned", Expr: "BYTES_SCANNED"},
			{Alias: "bytes_written", Expr: "BYTES_WRITTEN"},
			{Alias: "rows_produced", Expr: "ROWS_PRODUCED"},
			{Alias: "cache_pct", Expr: "ROUND(COALESCE(PERCENTAGE_SCANNED_FROM_CACHE, 0) * 100, 2)"},
			{Alias: "credits", Expr: "CREDITS_USED_CLOUD_SERVICES"},
			{Alias: "error_code", Expr: "ERROR_CODE"},
			{Alias: "error_message", Expr: "ERROR_MESSAGE"},
		},
		Order: "START_TIME DESC",
	}
}

func listQueries(rc *plugin.RequestContext) (any, error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return nil, err
	}
	spec := querySpec(s.opts.Database, s.opts.HistoryWindow)
	if warehouse, err := optionalIdentifier(rc.Param("warehouse")); err != nil {
		return nil, err
	} else if warehouse != "" {
		spec.Where = appendPredicate(spec.Where, "WAREHOUSE_NAME = ?")
		spec.Args = append(spec.Args, warehouse)
	}
	if user, err := optionalIdentifier(rc.Param("user")); err != nil {
		return nil, err
	} else if user != "" {
		spec.Where = appendPredicate(spec.Where, "USER_NAME = ?")
		spec.Args = append(spec.Args, user)
	}
	page, err := selectPage(rc, s, s.db, spec)
	if err != nil {
		return nil, err
	}
	for _, r := range page.Items {
		id := text(r["query_id"])
		r["ref"] = plugin.ResourceIdentity{Kind: "query", Name: id, UID: id}
	}
	return page, nil
}

func queryOverview(rc *plugin.RequestContext) (any, error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return nil, err
	}
	id, err := queryID(rc.Param("id"))
	if err != nil {
		return nil, err
	}
	spec := querySpec(s.opts.Database, s.opts.HistoryWindow)
	spec.Where = "QUERY_ID = ?"
	spec.Args = []any{id}
	return firstRow(rc.Ctx, s, s.db, spec)
}

func abortQuery(rc *plugin.RequestContext) (any, error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	id, err := queryID(rc.Param("id"))
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `SELECT SYSTEM$CANCEL_QUERY(?) AS "message"`, []any{id})
	if err != nil {
		return nil, err
	}
	message := "Query abort requested."
	if len(rows) > 0 {
		if reported := text(rows[0]["message"]); reported != "" {
			message = reported
		}
	}
	return actionResult{OK: true, Message: message}, nil
}

// queryID validates a Snowflake query id, which is a UUID the server assigns.
func queryID(raw string) (string, error) {
	id := strings.TrimSpace(raw)
	if _, err := uuid.Parse(id); err != nil {
		return "", fmt.Errorf("%w: query id must be a Snowflake query UUID", plugin.ErrInvalidInput)
	}
	return id, nil
}

// ------------------------------------------------------------------ account

func accountOverview(rc *plugin.RequestContext) (any, error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT CURRENT_ACCOUNT() AS "account", CURRENT_REGION() AS "region", CURRENT_VERSION() AS "version",
       CURRENT_USER() AS "user", CURRENT_ROLE() AS "role", CURRENT_WAREHOUSE() AS "warehouse",
       CURRENT_DATABASE() AS "database", CURRENT_SCHEMA() AS "schema", CURRENT_SESSION() AS "session_id"`, nil)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, plugin.ErrNotFound
	}
	return rows[0], nil
}

func completionRoute(rc *plugin.RequestContext) (any, error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return nil, err
	}
	database, err := scopedDatabase(rc, s)
	if err != nil {
		return nil, err
	}
	items := []sqldb.CompletionItem{
		{Label: "SELECT", Type: "keyword"},
		{Label: "FROM", Type: "keyword"},
		{Label: "WHERE", Type: "keyword"},
		{Label: "GROUP BY", Type: "keyword"},
		{Label: "ORDER BY", Type: "keyword"},
		{Label: "QUALIFY", Type: "keyword"},
		{Label: "LIMIT", Type: "keyword"},
		{Label: "MERGE INTO", Type: "keyword"},
		{Label: "COPY INTO", Type: "keyword"},
		{Label: "CREATE OR REPLACE TABLE", Type: "keyword"},
		{Label: "SHOW WAREHOUSES", Type: "keyword"},
		{Label: "RESULT_SCAN", Type: "function"},
		{Label: "GET_DDL", Type: "function"},
		{Label: "SYSTEM$CANCEL_QUERY", Type: "function"},
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT c.TABLE_SCHEMA AS "schema", c.TABLE_NAME AS "table", t.TABLE_TYPE AS "kind", c.COLUMN_NAME AS "column"
FROM `+informationSchema(database)+`.COLUMNS c
JOIN `+informationSchema(database)+`.TABLES t
  ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
WHERE c.TABLE_SCHEMA <> 'INFORMATION_SCHEMA'
ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
LIMIT `+strconv.Itoa(completionCap), nil)
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
		schema, relation := text(r["schema"]), text(r["table"])
		kind := "table"
		if strings.Contains(strings.ToLower(text(r["kind"])), "view") {
			kind = "view"
		}
		add(sqldb.CompletionItem{Label: schema, Type: "namespace", Detail: database})
		add(sqldb.CompletionItem{Label: relation, Type: kind, Detail: database + "." + schema, Apply: qualifiedQuoted(database, schema, relation)})
		if column := text(r["column"]); column != "" {
			add(sqldb.CompletionItem{Label: column, Type: "property", Detail: schema + "." + relation})
		}
	}
	return items, nil
}

// ------------------------------------------------------------------ DDL writes

func createDatabase(rc *plugin.RequestContext) (any, error) {
	var req struct {
		Name        string `json:"name" validate:"required"`
		Transient   bool   `json:"transient"`
		Comment     string `json:"comment"`
		IfNotExists bool   `json:"if_not_exists"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name, err := safeIdentifier(req.Name)
	if err != nil {
		return nil, err
	}
	stmt := "CREATE "
	if req.Transient {
		stmt += "TRANSIENT "
	}
	stmt += "DATABASE "
	if req.IfNotExists {
		stmt += "IF NOT EXISTS "
	}
	stmt += quoteIdent(name) + comment(req.Comment)
	return execStatement(rc, stmt, "Database created.")
}

func dropDatabase(rc *plugin.RequestContext) (any, error) {
	name, err := safeIdentifier(rc.Param("database"))
	if err != nil {
		return nil, err
	}
	return execStatement(rc, "DROP DATABASE IF EXISTS "+quoteIdent(name), "Database dropped.")
}

func createSchema(rc *plugin.RequestContext) (any, error) {
	database, err := safeIdentifier(rc.Param("database"))
	if err != nil {
		return nil, err
	}
	var req struct {
		Name          string `json:"name" validate:"required"`
		Transient     bool   `json:"transient"`
		ManagedAccess bool   `json:"managed_access"`
		Comment       string `json:"comment"`
		IfNotExists   bool   `json:"if_not_exists"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name, err := safeIdentifier(req.Name)
	if err != nil {
		return nil, err
	}
	stmt := "CREATE "
	if req.Transient {
		stmt += "TRANSIENT "
	}
	stmt += "SCHEMA "
	if req.IfNotExists {
		stmt += "IF NOT EXISTS "
	}
	stmt += quoteIdent(database) + "." + quoteIdent(name)
	if req.ManagedAccess {
		stmt += " WITH MANAGED ACCESS"
	}
	stmt += comment(req.Comment)
	return execStatement(rc, stmt, "Schema created.")
}

func dropSchema(rc *plugin.RequestContext) (any, error) {
	database, schema, err := schemaIdent(rc)
	if err != nil {
		return nil, err
	}
	return execStatement(rc, "DROP SCHEMA IF EXISTS "+quoteIdent(database)+"."+quoteIdent(schema), "Schema dropped.")
}

func createTable(rc *plugin.RequestContext) (any, error) {
	database, schema, err := schemaIdent(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		Name        string `json:"name" validate:"required"`
		Columns     any    `json:"columns" validate:"required"`
		Transient   bool   `json:"transient"`
		ClusterBy   string `json:"cluster_by"`
		Comment     string `json:"comment"`
		IfNotExists bool   `json:"if_not_exists"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name, err := safeIdentifier(req.Name)
	if err != nil {
		return nil, err
	}
	columns, err := sqldb.ParseDDLColumns(req.Columns)
	if err != nil {
		return nil, err
	}
	stmt := "CREATE "
	if req.Transient {
		stmt += "TRANSIENT "
	}
	stmt += "TABLE "
	if req.IfNotExists {
		stmt += "IF NOT EXISTS "
	}
	stmt += qualifiedQuoted(database, schema, name) + " (" + strings.Join(columns, ", ") + ")"
	if strings.TrimSpace(req.ClusterBy) != "" {
		keys, err := sqldb.IdentifierList(req.ClusterBy, quoteIdent)
		if err != nil {
			return nil, err
		}
		stmt += " CLUSTER BY (" + strings.Join(keys, ", ") + ")"
	}
	stmt += comment(req.Comment)
	return execStatement(rc, stmt, "Table created.")
}

func renameTable(rc *plugin.RequestContext) (any, error) {
	database, schema, name, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		Database string `json:"database"`
		Schema   string `json:"schema"`
		Name     string `json:"name" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	targetDatabase, targetSchema := database, schema
	if strings.TrimSpace(req.Database) != "" {
		if targetDatabase, err = safeIdentifier(req.Database); err != nil {
			return nil, err
		}
	}
	if strings.TrimSpace(req.Schema) != "" {
		if targetSchema, err = safeIdentifier(req.Schema); err != nil {
			return nil, err
		}
	}
	target, err := safeIdentifier(req.Name)
	if err != nil {
		return nil, err
	}
	stmt := "ALTER TABLE " + qualifiedQuoted(database, schema, name) + " RENAME TO " + qualifiedQuoted(targetDatabase, targetSchema, target)
	return execStatement(rc, stmt, "Table renamed.")
}

func truncateTable(rc *plugin.RequestContext) (any, error) {
	target, err := objectTarget(rc)
	if err != nil {
		return nil, err
	}
	return execStatement(rc, "TRUNCATE TABLE IF EXISTS "+target, "Table truncated.")
}

func dropTable(rc *plugin.RequestContext) (any, error) {
	target, err := objectTarget(rc)
	if err != nil {
		return nil, err
	}
	return execStatement(rc, "DROP TABLE IF EXISTS "+target, "Table dropped.")
}

func dropView(rc *plugin.RequestContext) (any, error) {
	target, err := objectTarget(rc)
	if err != nil {
		return nil, err
	}
	return execStatement(rc, "DROP VIEW IF EXISTS "+target, "View dropped.")
}

func addColumn(rc *plugin.RequestContext) (any, error) {
	target, err := objectTarget(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		Name     string `json:"name" validate:"required"`
		Type     string `json:"type" validate:"required"`
		Nullable bool   `json:"nullable"`
		Default  string `json:"default"`
		Comment  string `json:"comment"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	column, err := sqldb.DDLColumn(sqldb.ColumnSpec{Name: req.Name, Type: req.Type, Nullable: req.Nullable, Default: req.Default})
	if err != nil {
		return nil, err
	}
	return execStatement(rc, "ALTER TABLE "+target+" ADD COLUMN "+column+columnComment(req.Comment), "Column added.")
}

func dropColumn(rc *plugin.RequestContext) (any, error) {
	target, err := objectTarget(rc)
	if err != nil {
		return nil, err
	}
	column, err := safeIdentifier(rc.Param("column"))
	if err != nil {
		return nil, err
	}
	return execStatement(rc, "ALTER TABLE "+target+" DROP COLUMN "+quoteIdent(column), "Column dropped.")
}

func execStatement(rc *plugin.RequestContext, statement, message string) (any, error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.QueryTimeout)
	defer cancel()
	if _, err := s.db.ExecContext(ctx, statement); err != nil {
		return nil, snowflakeErr(err)
	}
	return actionResult{OK: true, Message: message}, nil
}

// ------------------------------------------------------------------ SQL editor

func cancelQuery(rc *plugin.RequestContext) (any, error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return nil, err
	}
	return actionResult{OK: s.cancelAll()}, nil
}

type statementResult struct {
	sqldb.StatementResult
	QueryID string `json:"queryId,omitempty"`
}

type queryResult struct {
	Columns    []string          `json:"columns"`
	Rows       [][]any           `json:"rows"`
	RowCount   int64             `json:"rowCount,omitempty"`
	ElapsedMS  int64             `json:"elapsedMs"`
	Statement  string            `json:"statement,omitempty"`
	CommandTag string            `json:"commandTag,omitempty"`
	QueryID    string            `json:"queryId,omitempty"`
	Statements []statementResult `json:"statements,omitempty"`
}

func queryStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := snowflakeSession(rc)
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
				payload["confirmMessage"] = "This Snowflake statement can change data, schema, grants, or session state. Review it before running."
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

func executeQueryRequest(parent context.Context, s *Session, req sqldb.QueryRequest) (queryResult, error) {
	statements := sqldb.SplitStatements(req.Query)
	if len(statements) == 0 {
		return queryResult{}, fmt.Errorf("%w: query is empty", plugin.ErrInvalidInput)
	}
	if s.opts.ReadOnly {
		for _, st := range statements {
			if !isReadOnlyStatement(st) {
				return queryResult{}, fmt.Errorf("%w: read-only mode blocks write statements", plugin.ErrForbidden)
			}
		}
	}
	if s.opts.RequireConfirm && !req.Confirm {
		for _, st := range statements {
			if isDestructiveStatement(st) {
				return queryResult{}, confirmationError{message: "statement requires confirmation"}
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
	results := make([]statementResult, 0, len(statements))
	for _, st := range statements {
		res, err := executeStatement(ctx, s, st)
		if err != nil {
			return queryResult{}, err
		}
		results = append(results, res)
	}
	out := queryResult{Statements: results}
	if len(results) > 0 {
		last := results[len(results)-1]
		out.Columns = last.Columns
		out.Rows = last.Rows
		out.RowCount = last.RowCount
		out.ElapsedMS = last.ElapsedMS
		out.Statement = last.Statement
		out.CommandTag = last.CommandTag
		out.QueryID = last.QueryID
	}
	return out, nil
}

func executeStatement(ctx context.Context, s *Session, statement string) (statementResult, error) {
	start := time.Now()
	ids := make(chan string, 1)
	ctx = withQueryID(ctx, ids)
	if !statementReturnsRows(statement) {
		res, err := s.db.ExecContext(ctx, statement)
		if err != nil {
			return statementResult{}, snowflakeErr(err)
		}
		affected, _ := res.RowsAffected()
		out := statementResult{QueryID: takeQueryID(ids)}
		out.Statement = statement
		out.RowCount = affected
		out.ElapsedMS = time.Since(start).Milliseconds()
		out.CommandTag = sqldb.FirstKeyword(statement)
		if affected >= 0 {
			out.CommandTag += " " + strconv.FormatInt(affected, 10)
		}
		return out, nil
	}
	rows, err := s.db.QueryContext(ctx, statement)
	if err != nil {
		return statementResult{}, snowflakeErr(err)
	}
	columns, err := rows.Columns()
	if err != nil {
		_ = rows.Close()
		return statementResult{}, snowflakeErr(err)
	}
	out := statementResult{}
	out.Statement = statement
	out.Columns = columns
	for rows.Next() {
		values, err := scanValues(rows, columns)
		if err != nil {
			_ = rows.Close()
			return statementResult{}, snowflakeErr(err)
		}
		out.Rows = append(out.Rows, values)
		if len(out.Rows) >= s.opts.RowLimit {
			break
		}
	}
	if err := rows.Close(); err != nil {
		return statementResult{}, snowflakeErr(err)
	}
	if err := rows.Err(); err != nil {
		return statementResult{}, snowflakeErr(err)
	}
	out.RowCount = int64(len(out.Rows))
	out.CommandTag = sqldb.FirstKeyword(statement)
	out.Rows = sqldb.RedactRows(out.Columns, out.Rows, s.opts.RedactPatterns)
	out.ElapsedMS = time.Since(start).Milliseconds()
	out.QueryID = takeQueryID(ids)
	return out, nil
}

// takeQueryID reads the server-assigned query id the driver publishes for the
// statement just executed. The driver closes the channel after one send, so a
// non-blocking receive is enough and an unreported id simply stays empty.
func takeQueryID(ids <-chan string) string {
	select {
	case id := <-ids:
		return id
	default:
		return ""
	}
}

func statementReturnsRows(statement string) bool {
	switch sqldb.FirstKeyword(statement) {
	case "SELECT", "SHOW", "DESC", "DESCRIBE", "EXPLAIN", "WITH", "LIST", "CALL", "VALUES":
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
	case "SELECT", "SHOW", "DESC", "DESCRIBE", "EXPLAIN", "WITH", "LIST", "VALUES":
		return true
	default:
		return false
	}
}

func isDestructiveStatement(statement string) bool {
	switch sqldb.FirstKeyword(statement) {
	case "INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "ALTER", "DROP", "TRUNCATE", "UNDROP",
		"GRANT", "REVOKE", "COPY", "PUT", "GET", "REMOVE", "RM", "CALL", "EXECUTE", "USE",
		"BEGIN", "COMMIT", "ROLLBACK", "SET", "UNSET", "COMMENT":
		return true
	default:
		return false
	}
}

// ------------------------------------------------------------------ query plumbing

// outColumn maps one output alias onto the SQL expression that produces it. The
// expression is what search and sort push down to, so filtering and ordering
// always apply to the whole result set rather than the current page.
type outColumn struct {
	Alias    string
	Expr     string
	NoSearch bool
}

func (c outColumn) expr() string {
	if c.Expr != "" {
		return c.Expr
	}
	return quoteIdent(c.Alias)
}

type selectSpec struct {
	Columns []outColumn
	From    string
	Where   string
	Args    []any
	Order   string
}

func (spec selectSpec) sortExpr(field string) (string, bool) {
	for _, c := range spec.Columns {
		if c.Alias == field {
			return c.expr(), true
		}
	}
	return "", false
}

func (spec selectSpec) searchExprs() []string {
	out := make([]string, 0, len(spec.Columns))
	for _, c := range spec.Columns {
		if !c.NoSearch {
			out = append(out, c.expr())
		}
	}
	return out
}

func (spec selectSpec) build(req plugin.PageRequest, limit, offset int) (string, []any, error) {
	if len(spec.Columns) == 0 || spec.From == "" {
		return "", nil, fmt.Errorf("%w: incomplete catalog query", plugin.ErrUnavailable)
	}
	projection := make([]string, len(spec.Columns))
	for i, c := range spec.Columns {
		projection[i] = c.expr() + " AS " + quoteIdent(c.Alias)
	}
	args := append([]any{}, spec.Args...)
	where := spec.Where
	if clause, searchArgs := searchClause(spec.searchExprs(), req.Search()); clause != "" {
		where = appendPredicate(where, clause)
		args = append(args, searchArgs...)
	}
	order := spec.Order
	if len(req.Sort) > 0 {
		expr, ok := spec.sortExpr(req.Sort[0].Field)
		if !ok {
			return "", nil, fmt.Errorf("%w: cannot sort by %q", plugin.ErrInvalidInput, req.Sort[0].Field)
		}
		// The natural order stays as a tiebreaker: equal cells under an offset
		// page would otherwise be free to shuffle and drop or repeat a row.
		order = expr + direction(req.Sort[0].Desc) + " NULLS LAST"
		if spec.Order != "" {
			order += ", " + spec.Order
		}
	}
	stmt := "SELECT " + strings.Join(projection, ", ") + " FROM " + spec.From
	if where != "" {
		stmt += " WHERE " + where
	}
	if order != "" {
		stmt += " ORDER BY " + order
	}
	return stmt + pageClause(limit, offset), args, nil
}

// pageClause renders LIMIT/OFFSET. Snowflake requires integer constants there,
// so the already-bounded page size and offset are formatted into the statement
// rather than bound.
func pageClause(limit, offset int) string {
	return " LIMIT " + strconv.Itoa(limit) + " OFFSET " + strconv.Itoa(offset)
}

func appendPredicate(where, clause string) string {
	if clause == "" {
		return where
	}
	if where == "" {
		return clause
	}
	return "(" + where + ") AND " + clause
}

func direction(desc bool) string {
	if desc {
		return " DESC"
	}
	return " ASC"
}

// querier is the subset of database/sql both *sql.DB and a pinned *sql.Conn
// satisfy, so a paged read can run either on the pool or on the one session that
// produced a RESULT_SCAN source.
type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// selectPage renders spec into a bounded SELECT whose search, ordering, and
// offset are all applied by Snowflake, then reads one page plus a sentinel row
// so the next cursor is emitted only when more data really exists.
func selectPage(rc *plugin.RequestContext, s *Session, q querier, spec selectSpec) (plugin.Page[row], error) {
	req, err := rc.Page()
	if err != nil {
		return plugin.Page[row]{}, err
	}
	limit := req.Limit
	if limit > s.opts.RowLimit {
		limit = s.opts.RowLimit
	}
	offset, err := cursorOffset(req.Cursor)
	if err != nil {
		return plugin.Page[row]{}, err
	}
	stmt, args, err := spec.build(req, limit+1, offset)
	if err != nil {
		return plugin.Page[row]{}, err
	}
	rows, err := queryRowsOn(rc.Ctx, s, q, stmt, args)
	if err != nil {
		return plugin.Page[row]{}, err
	}
	next := ""
	if len(rows) > limit {
		rows = rows[:limit]
		next = encodeCursor(offset + limit)
	}
	return plugin.Page[row]{Items: rows, NextCursor: next}, nil
}

// showPage runs a SHOW or LIST command and pages its output through RESULT_SCAN
// on the same pinned session. Snowflake exposes no LIMIT/ORDER BY on those
// commands, so reading them back is what keeps large accounts pageable instead
// of materialized in the plugin.
func showPage(rc *plugin.RequestContext, s *Session, show string, spec selectSpec) (plugin.Page[row], error) {
	conn, err := s.db.Conn(rc.Ctx)
	if err != nil {
		return plugin.Page[row]{}, snowflakeErr(err)
	}
	defer func() { _ = conn.Close() }()
	if err := runCommand(rc.Ctx, s, conn, show); err != nil {
		return plugin.Page[row]{}, err
	}
	spec.From = resultScanFrom
	return selectPage(rc, s, conn, spec)
}

func showOne(ctx context.Context, s *Session, show string, spec selectSpec, keyExpr, want string) (row, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, snowflakeErr(err)
	}
	defer func() { _ = conn.Close() }()
	if err := runCommand(ctx, s, conn, show); err != nil {
		return nil, err
	}
	spec.From = resultScanFrom
	spec.Where = appendPredicate(spec.Where, keyExpr+" = ?")
	spec.Args = append(append([]any{}, spec.Args...), want)
	return firstRow(ctx, s, conn, spec)
}

func firstRow(ctx context.Context, s *Session, q querier, spec selectSpec) (row, error) {
	stmt, args, err := spec.build(plugin.PageRequest{}, 1, 0)
	if err != nil {
		return nil, err
	}
	rows, err := queryRowsOn(ctx, s, q, stmt, args)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, plugin.ErrNotFound
	}
	return rows[0], nil
}

// runCommand executes a SHOW or LIST statement whose output the caller reads
// back with RESULT_SCAN(LAST_QUERY_ID()); the rows are discarded here so only
// the projected page crosses the wire.
func runCommand(ctx context.Context, s *Session, conn *sql.Conn, statement string) error {
	ctx, cancel := context.WithTimeout(ctx, s.opts.QueryTimeout)
	defer cancel()
	rows, err := conn.QueryContext(ctx, statement)
	if err != nil {
		return snowflakeErr(err)
	}
	if err := rows.Close(); err != nil {
		return snowflakeErr(err)
	}
	return snowflakeErr(rows.Err())
}

func queryRows(ctx context.Context, s *Session, statement string, args []any) ([]row, error) {
	return queryRowsOn(ctx, s, s.db, statement, args)
}

func queryRowsOn(ctx context.Context, s *Session, q querier, statement string, args []any) ([]row, error) {
	ctx, cancel := context.WithTimeout(ctx, s.opts.QueryTimeout)
	defer cancel()
	rows, err := q.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, snowflakeErr(err)
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		return nil, snowflakeErr(err)
	}
	out := []row{}
	for rows.Next() {
		if len(out) >= scanCap {
			break
		}
		values, err := scanValues(rows, columns)
		if err != nil {
			return nil, snowflakeErr(err)
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
		return nil, snowflakeErr(err)
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
	return sqldb.DisplayValues(columns, values), nil
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

// searchClause renders the grid's free-text term as a case-insensitive contains
// filter across the given expressions, so the search reaches the whole result
// set instead of the rows already loaded.
func searchClause(exprs []string, term string) (string, []any) {
	term = strings.TrimSpace(term)
	if term == "" || len(exprs) == 0 {
		return "", nil
	}
	pattern := "%" + escapeLike(term) + "%"
	parts := make([]string, 0, len(exprs))
	args := make([]any, 0, len(exprs))
	for _, expr := range exprs {
		parts = append(parts, `UPPER(TO_VARCHAR(`+expr+`)) LIKE UPPER(?) ESCAPE '\\'`)
		args = append(args, pattern)
	}
	return "(" + strings.Join(parts, " OR ") + ")", args
}

func escapeLike(term string) string {
	replacer := strings.NewReplacer(`\`, `\\`, "%", `\%`, "_", `\_`)
	return replacer.Replace(term)
}

// ------------------------------------------------------------------ cursors

// cursorPrefix versions the opaque cursor so a stale cursor from an older
// encoding is rejected instead of silently decoding to the wrong offset.
const cursorPrefix = "sf1:"

func encodeCursor(offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(cursorPrefix + strconv.Itoa(offset)))
}

// cursorOffset decodes an opaque cursor. A bare integer is accepted too, because
// the grid falls back to numeric offsets when it resumes a list without one.
func cursorOffset(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	if n, err := strconv.Atoi(raw); err == nil {
		if n < 0 {
			return 0, invalidCursor()
		}
		return n, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return 0, invalidCursor()
	}
	text, ok := strings.CutPrefix(string(decoded), cursorPrefix)
	if !ok {
		return 0, invalidCursor()
	}
	n, err := strconv.Atoi(text)
	if err != nil || n < 0 {
		return 0, invalidCursor()
	}
	return n, nil
}

func invalidCursor() error {
	return fmt.Errorf("%w: cursor is not a valid page cursor", plugin.ErrInvalidInput)
}

// ------------------------------------------------------------------ identifiers

func safeIdentifier(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if !identifierRE.MatchString(raw) {
		return "", fmt.Errorf("%w: invalid Snowflake identifier %q", plugin.ErrInvalidInput, raw)
	}
	return raw, nil
}

func optionalIdentifier(raw string) (string, error) {
	if strings.TrimSpace(raw) == "" {
		return "", nil
	}
	return safeIdentifier(raw)
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// quoteLiteral renders a value as a single-quoted Snowflake string literal,
// escaping backslashes and single quotes so it stays safe in statements such as
// SHOW ... LIKE and ALTER ... SET that take no bound parameters.
func quoteLiteral(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}

func qualified(database, schema, name string) string {
	return database + "." + schema + "." + name
}

func qualifiedQuoted(database, schema, name string) string {
	return quoteIdent(database) + "." + quoteIdent(schema) + "." + quoteIdent(name)
}

// comment renders an object-level COMMENT property, which Snowflake spells with
// an "=" in CREATE and ALTER ... SET.
func comment(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	return " COMMENT = " + quoteLiteral(strings.TrimSpace(raw))
}

// columnComment renders a column-level COMMENT, which Snowflake spells without
// the "=" that object-level properties use.
func columnComment(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	return " COMMENT " + quoteLiteral(strings.TrimSpace(raw))
}

// scopedDatabase resolves the database a request targets, defaulting to the
// connection's own database so no listing escapes the connection's scope.
func scopedDatabase(rc *plugin.RequestContext, s *Session) (string, error) {
	raw := strings.TrimSpace(rc.Param("database"))
	if raw == "" {
		return s.opts.Database, nil
	}
	return safeIdentifier(raw)
}

func schemaIdent(rc *plugin.RequestContext) (string, string, error) {
	database, err := safeIdentifier(rc.Param("database"))
	if err != nil {
		return "", "", err
	}
	schema, err := safeIdentifier(rc.Param("schema"))
	if err != nil {
		return "", "", err
	}
	return database, schema, nil
}

func objectIdent(rc *plugin.RequestContext) (string, string, string, error) {
	database, schema, err := schemaIdent(rc)
	if err != nil {
		return "", "", "", err
	}
	name, err := safeIdentifier(rc.Param("name"))
	if err != nil {
		return "", "", "", err
	}
	return database, schema, name, nil
}

func objectTarget(rc *plugin.RequestContext) (string, error) {
	database, schema, name, err := objectIdent(rc)
	if err != nil {
		return "", err
	}
	return qualifiedQuoted(database, schema, name), nil
}

func objectRef(kind string, r row) plugin.ResourceIdentity {
	database, schema, name := text(r["database"]), text(r["schema"]), text(r["name"])
	return plugin.ResourceIdentity{Kind: kind, Namespace: database, Scope: schema, Name: name, UID: qualified(database, schema, name)}
}

// showScope builds "SHOW <objects> IN {SCHEMA|DATABASE}", so a SHOW that would
// otherwise sweep the whole account stays inside the requested database.
func showScope(rc *plugin.RequestContext, s *Session, objects string) (string, error) {
	database, err := scopedDatabase(rc, s)
	if err != nil {
		return "", err
	}
	schema, err := optionalIdentifier(rc.Param("schema"))
	if err != nil {
		return "", err
	}
	if schema != "" {
		return "SHOW " + objects + " IN SCHEMA " + quoteIdent(database) + "." + quoteIdent(schema), nil
	}
	return "SHOW " + objects + " IN DATABASE " + quoteIdent(database), nil
}

// schemaScopedList pages an INFORMATION_SCHEMA listing of schema-scoped objects,
// narrowing it to one schema when the panel supplies one.
func schemaScopedList(rc *plugin.RequestContext, kind string, spec func(string) selectSpec, schemaColumn string) (any, error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return nil, err
	}
	database, err := scopedDatabase(rc, s)
	if err != nil {
		return nil, err
	}
	built := spec(database)
	schema, err := optionalIdentifier(rc.Param("schema"))
	if err != nil {
		return nil, err
	}
	if schema != "" {
		built.Where = appendPredicate(built.Where, schemaColumn+" = ?")
		built.Args = append(built.Args, schema)
	}
	page, err := selectPage(rc, s, s.db, built)
	if err != nil {
		return nil, err
	}
	for _, r := range page.Items {
		r["ref"] = objectRef(kind, r)
	}
	return page, nil
}

func schemaScopedOverview(rc *plugin.RequestContext, spec func(string) selectSpec, schemaColumn, nameColumn string) (any, error) {
	s, err := snowflakeSession(rc)
	if err != nil {
		return nil, err
	}
	database, schema, name, err := objectIdent(rc)
	if err != nil {
		return nil, err
	}
	built := spec(database)
	built.Where = appendPredicate(built.Where, schemaColumn+" = ? AND "+nameColumn+" = ?")
	built.Args = append(built.Args, schema, name)
	return firstRow(rc.Ctx, s, s.db, built)
}

// ------------------------------------------------------------------ misc

func ensureWritable(s *Session) error {
	if s.opts.ReadOnly {
		return fmt.Errorf("%w: read-only mode blocks write operations", plugin.ErrForbidden)
	}
	return nil
}

// ensureWritableColumns refuses a row mutation that targets a redacted column.
// The grid only ever showed that column's mask, so writing it back would store
// the mask over the real value.
func ensureWritableColumns(values map[string]any, patterns []string) error {
	for column := range values {
		if sqldb.RedactColumn(column, patterns) {
			return fmt.Errorf("%w: column %q is redacted and cannot be written", plugin.ErrForbidden, column)
		}
	}
	return nil
}

// objectNotFoundCode is Snowflake's "object does not exist, or operation cannot
// be performed" server error.
const objectNotFoundCode = 390201

// snowflakeErr maps a driver error onto the SDK sentinel the gateway turns into
// an HTTP status, so an expired login or a missing object does not surface as a
// generic upstream outage.
func snowflakeErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return plugin.ErrNotFound
	}
	var sfErr *sf.SnowflakeError
	if errors.As(err, &sfErr) {
		switch {
		case strings.HasPrefix(sfErr.SQLState, "28"):
			return fmt.Errorf("%w: %v", plugin.ErrUnauthorized, err)
		case sfErr.SQLState == "42501":
			return fmt.Errorf("%w: %v", plugin.ErrForbidden, err)
		case sfErr.SQLState == "42S02" || sfErr.Number == objectNotFoundCode:
			return fmt.Errorf("%w: %v", plugin.ErrNotFound, err)
		}
	}
	return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
}

func text(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

func number(v any) float64 {
	switch n := v.(type) {
	case nil:
		return 0
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	}
	parsed, err := strconv.ParseFloat(strings.TrimSpace(fmt.Sprint(v)), 64)
	if err != nil {
		return 0
	}
	return parsed
}

func percent(used, total float64) float64 {
	if total <= 0 {
		return 0
	}
	return round2(used / total * 100)
}

func round2(v float64) float64 {
	return float64(int64(v*100+0.5)) / 100
}
