package trino

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

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

var dialect = sqldb.Dialect{QuoteIdent: quoteIdent, Placeholder: sqldb.QuestionPlaceholder}

// scanCap bounds the SHOW-style listings that Trino cannot page in SQL. Those
// results are structurally small (session properties, per-column statistics,
// one DDL row), so the cap is a guard rail, not a paging boundary: every list
// backed by a real table pages through OFFSET/LIMIT instead.
const scanCap = plugin.MaxPageLimit * 20

// The driver turns X-Trino-* named arguments into request headers, so a panel
// can scope one execution without mutating shared session state.
const (
	catalogHeaderParam          = "X-Trino-Catalog"
	schemaHeaderParam           = "X-Trino-Schema"
	progressCallbackParam       = "X-Trino-Progress-Callback"
	progressCallbackPeriodParam = "X-Trino-Progress-Callback-Period"
)

const progressPeriod = 250 * time.Millisecond

var requestSeq atomic.Uint64

func routes() []plugin.Route {
	return []plugin.Route{
		{ID: "trino.catalogs.tree", Method: plugin.MethodGet, Path: "/tree/catalogs", Permission: "trino.catalogs.read", Risk: plugin.RiskSafe, AuditEvent: "trino.catalogs.tree", Handle: treeCatalogs},
		{ID: "trino.schemas.tree", Method: plugin.MethodGet, Path: "/tree/schemas", Permission: "trino.schemas.read", Risk: plugin.RiskSafe, AuditEvent: "trino.schemas.tree", Handle: treeSchemas},
		{ID: "trino.relations.tree", Method: plugin.MethodGet, Path: "/tree/relations", Permission: "trino.tables.read", Risk: plugin.RiskSafe, AuditEvent: "trino.relations.tree", Handle: treeRelations},
		{ID: "trino.nodes.tree", Method: plugin.MethodGet, Path: "/tree/nodes", Permission: "trino.cluster.read", Risk: plugin.RiskSafe, AuditEvent: "trino.nodes.tree", Handle: treeNodes},
		{ID: "trino.queries.tree", Method: plugin.MethodGet, Path: "/tree/queries", Permission: "trino.queries.read", Risk: plugin.RiskSafe, AuditEvent: "trino.queries.tree", Handle: treeQueries},

		{ID: "trino.cluster.overview", Method: plugin.MethodGet, Path: "/cluster", Permission: "trino.cluster.read", Risk: plugin.RiskSafe, AuditEvent: "trino.cluster.overview", Handle: clusterOverview},
		{ID: "trino.cluster.metrics", Method: plugin.MethodWS, Path: "/cluster/metrics", Permission: "trino.cluster.read", Risk: plugin.RiskSafe, AuditEvent: "trino.cluster.metrics", Stream: clusterMetrics},

		{ID: "trino.catalogs.list", Method: plugin.MethodGet, Path: "/catalogs", Permission: "trino.catalogs.read", Risk: plugin.RiskSafe, AuditEvent: "trino.catalogs.list", Handle: listCatalogs},
		{ID: "trino.catalog.overview", Method: plugin.MethodGet, Path: "/catalogs/{catalog}", Permission: "trino.catalogs.read", Risk: plugin.RiskSafe, AuditEvent: "trino.catalog.overview", Handle: catalogOverview},
		{ID: "trino.schemas.list", Method: plugin.MethodGet, Path: "/schemas", Permission: "trino.schemas.read", Risk: plugin.RiskSafe, AuditEvent: "trino.schemas.list", Handle: listSchemas},
		{ID: "trino.tables.list", Method: plugin.MethodGet, Path: "/tables", Permission: "trino.tables.read", Risk: plugin.RiskSafe, AuditEvent: "trino.tables.list", Handle: listTables},
		{ID: "trino.views.list", Method: plugin.MethodGet, Path: "/views", Permission: "trino.views.read", Risk: plugin.RiskSafe, AuditEvent: "trino.views.list", Handle: listViews},

		{ID: "trino.table.rows", Method: plugin.MethodGet, Path: "/catalogs/{catalog}/schemas/{schema}/tables/{table}/rows", Permission: "trino.tables.data.read", Risk: plugin.RiskSafe, AuditEvent: "trino.table.rows", Handle: tableRows},
		{ID: "trino.table.row.insert", Method: plugin.MethodPost, Path: "/catalogs/{catalog}/schemas/{schema}/tables/{table}/rows", Permission: "trino.tables.data.write", Risk: plugin.RiskWrite, AuditEvent: "trino.table.row.insert", Handle: insertRow},
		{ID: "trino.table.columns", Method: plugin.MethodGet, Path: "/catalogs/{catalog}/schemas/{schema}/tables/{table}/columns", Permission: "trino.tables.read", Risk: plugin.RiskSafe, AuditEvent: "trino.table.columns", Handle: tableColumnsRoute},
		{ID: "trino.table.stats", Method: plugin.MethodGet, Path: "/catalogs/{catalog}/schemas/{schema}/tables/{table}/stats", Permission: "trino.tables.read", Risk: plugin.RiskSafe, AuditEvent: "trino.table.stats", Handle: tableStats},
		{ID: "trino.table.ddl", Method: plugin.MethodGet, Path: "/catalogs/{catalog}/schemas/{schema}/tables/{table}/ddl", Permission: "trino.tables.read", Risk: plugin.RiskSafe, AuditEvent: "trino.table.ddl", Handle: tableDDL},
		{ID: "trino.view.rows", Method: plugin.MethodGet, Path: "/catalogs/{catalog}/schemas/{schema}/views/{table}/rows", Permission: "trino.views.data.read", Risk: plugin.RiskSafe, AuditEvent: "trino.view.rows", Handle: tableRows},
		{ID: "trino.view.columns", Method: plugin.MethodGet, Path: "/catalogs/{catalog}/schemas/{schema}/views/{table}/columns", Permission: "trino.views.read", Risk: plugin.RiskSafe, AuditEvent: "trino.view.columns", Handle: tableColumnsRoute},
		{ID: "trino.view.definition", Method: plugin.MethodGet, Path: "/catalogs/{catalog}/schemas/{schema}/views/{table}/definition", Permission: "trino.views.read", Risk: plugin.RiskSafe, AuditEvent: "trino.view.definition", Handle: viewDefinition},

		{ID: "trino.nodes.list", Method: plugin.MethodGet, Path: "/nodes", Permission: "trino.cluster.read", Risk: plugin.RiskSafe, AuditEvent: "trino.nodes.list", Handle: listNodes},
		{ID: "trino.node.overview", Method: plugin.MethodGet, Path: "/nodes/{node}", Permission: "trino.cluster.read", Risk: plugin.RiskSafe, AuditEvent: "trino.node.overview", Handle: nodeOverview},
		{ID: "trino.queries.list", Method: plugin.MethodGet, Path: "/queries", Permission: "trino.queries.read", Risk: plugin.RiskSafe, AuditEvent: "trino.queries.list", Handle: listQueries},
		{ID: "trino.query.overview", Method: plugin.MethodGet, Path: "/queries/{query}", Permission: "trino.queries.read", Risk: plugin.RiskSafe, AuditEvent: "trino.query.overview", Handle: queryOverview},
		{ID: "trino.query.statement", Method: plugin.MethodGet, Path: "/queries/{query}/statement", Permission: "trino.queries.read", Risk: plugin.RiskSafe, AuditEvent: "trino.query.statement", Handle: queryStatement},
		{ID: "trino.query.kill", Method: plugin.MethodPost, Path: "/queries/{query}/kill", Permission: "trino.queries.kill", Risk: plugin.RiskDestructive, AuditEvent: "trino.query.kill", Handle: killQuery},
		{ID: "trino.session.list", Method: plugin.MethodGet, Path: "/session", Permission: "trino.cluster.read", Risk: plugin.RiskSafe, AuditEvent: "trino.session.list", Handle: listSessionProperties},

		{ID: "trino.schema.create", Method: plugin.MethodPost, Path: "/catalogs/{catalog}/schemas", Permission: "trino.schemas.write", Risk: plugin.RiskWrite, AuditEvent: "trino.schema.create", Input: schemaCreateSchema(), Handle: createSchema},
		{ID: "trino.schema.drop", Method: plugin.MethodDelete, Path: "/catalogs/{catalog}/schemas/{schema}", Permission: "trino.schemas.delete", Risk: plugin.RiskDestructive, AuditEvent: "trino.schema.drop", Handle: dropSchema},
		{ID: "trino.table.create", Method: plugin.MethodPost, Path: "/catalogs/{catalog}/schemas/{schema}/tables", Permission: "trino.tables.write", Risk: plugin.RiskWrite, AuditEvent: "trino.table.create", Input: tableCreateSchema(), Handle: createTable},
		{ID: "trino.table.rename", Method: plugin.MethodPost, Path: "/catalogs/{catalog}/schemas/{schema}/tables/{table}/rename", Permission: "trino.tables.write", Risk: plugin.RiskWrite, AuditEvent: "trino.table.rename", Input: tableRenameSchema(), Handle: renameTable},
		{ID: "trino.table.drop", Method: plugin.MethodDelete, Path: "/catalogs/{catalog}/schemas/{schema}/tables/{table}", Permission: "trino.tables.delete", Risk: plugin.RiskDestructive, AuditEvent: "trino.table.drop", Handle: dropTable},
		{ID: "trino.view.drop", Method: plugin.MethodDelete, Path: "/catalogs/{catalog}/schemas/{schema}/views/{table}", Permission: "trino.views.delete", Risk: plugin.RiskDestructive, AuditEvent: "trino.view.drop", Handle: dropView},

		{ID: "trino.completion", Method: plugin.MethodGet, Path: "/completion", Permission: "trino.catalogs.read", Risk: plugin.RiskSafe, AuditEvent: "trino.completion", Handle: completionRoute},
		{ID: "trino.query", Method: plugin.MethodWS, Path: "/query", Permission: "trino.query.execute", Risk: plugin.RiskPrivileged, AuditEvent: "trino.query", Stream: queryStream},
		{ID: "trino.query.cancel", Method: plugin.MethodPost, Path: "/query/cancel", Permission: "trino.query.cancel", Risk: plugin.RiskWrite, AuditEvent: "trino.query.cancel", Handle: cancelQuery},
	}
}

func trinoSession(rc *plugin.RequestContext) (*Session, error) {
	return unwrap(rc.Session)
}

func schemaCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Schema", Fields: []plugin.Field{
		{Key: "name", Label: "Schema name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: identifierPattern}}},
		{Key: "if_not_exists", Label: "If not exists", Type: plugin.FieldToggle, Default: true},
	}}}}
}

func tableCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Table", Fields: []plugin.Field{
		{Key: "name", Label: "Table name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: identifierPattern}}},
		sqldb.ColumnsArrayField(sqldb.ColumnsField{TypePlaceholder: "varchar", TypeSuggestions: []string{
			"boolean", "tinyint", "smallint", "integer", "bigint", "real", "double", "decimal(18,2)",
			"varchar", "varchar(255)", "char(10)", "varbinary", "json", "date", "time", "timestamp",
			"timestamp(3) with time zone", "uuid", "array(varchar)", "map(varchar, varchar)",
		}}),
		{Key: "comment", Label: "Comment", Type: plugin.FieldText},
		{Key: "if_not_exists", Label: "If not exists", Type: plugin.FieldToggle, Default: true},
	}}}}
}

func tableRenameSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Rename table", Fields: []plugin.Field{
		{Key: "schema", Label: "Target schema", Type: plugin.FieldText, Help: "Leave blank to keep the current schema.", Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: identifierPattern}}},
		{Key: "name", Label: "New name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: identifierPattern}}},
	}}}}
}

func treeCatalogs(rc *plugin.RequestContext) (any, error) {
	page, err := pageOf(listCatalogs(rc))
	if err != nil {
		return nil, err
	}
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, r := range page.Items {
		ref, ok := r["ref"].(plugin.ResourceIdentity)
		if !ok || ref.Kind == "" {
			continue
		}
		name := fmt.Sprint(r["catalog"])
		nodes = append(nodes, plugin.TreeNode{
			Key:            "catalog:" + name,
			Label:          name,
			Icon:           icon("library-big"),
			Ref:            &ref,
			ChildrenSource: &plugin.DataSource{RouteID: "trino.schemas.tree", Params: map[string]string{"catalog": name}},
		})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

func treeSchemas(rc *plugin.RequestContext) (any, error) {
	page, err := pageOf(listSchemas(rc))
	if err != nil {
		return nil, err
	}
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, r := range page.Items {
		ref, ok := r["ref"].(plugin.ResourceIdentity)
		if !ok || ref.Kind == "" {
			continue
		}
		catalog, schema := fmt.Sprint(r["catalog"]), fmt.Sprint(r["schema"])
		nodes = append(nodes, plugin.TreeNode{
			Key:            "schema:" + ref.UID,
			Label:          schema,
			Icon:           icon("folder-tree"),
			Ref:            &ref,
			ChildrenSource: &plugin.DataSource{RouteID: "trino.relations.tree", Params: map[string]string{"catalog": catalog, "schema": schema}},
		})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

// treeRelations lists a schema's tables and views from one paged query so the
// merged listing keeps a single, resumable cursor.
func treeRelations(rc *plugin.RequestContext) (any, error) {
	page, err := pageOf(relationList(rc, ""))
	if err != nil {
		return nil, err
	}
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, r := range page.Items {
		ref, ok := r["ref"].(plugin.ResourceIdentity)
		if !ok || ref.Kind == "" {
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

func treeNodes(rc *plugin.RequestContext) (any, error) {
	return treeFrom(rc, listNodes, "node_id", "server")
}

func treeQueries(rc *plugin.RequestContext) (any, error) {
	return treeFrom(rc, listQueries, "query_id", "activity")
}

func treeFrom(rc *plugin.RequestContext, load func(*plugin.RequestContext) (any, error), labelKey, iconName string) (any, error) {
	page, err := pageOf(load(rc))
	if err != nil {
		return nil, err
	}
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, r := range page.Items {
		ref, ok := r["ref"].(plugin.ResourceIdentity)
		if !ok || ref.Kind == "" {
			continue
		}
		nodes = append(nodes, plugin.TreeNode{Key: ref.Kind + ":" + ref.UID, Label: fmt.Sprint(r[labelKey]), Icon: icon(iconName), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

func listCatalogs(rc *plugin.RequestContext) (any, error) {
	s, err := trinoSession(rc)
	if err != nil {
		return nil, err
	}
	return pageSQL(rc, s, listSpec{
		Select:      `catalog_name AS "catalog", connector_name AS "connector"`,
		From:        "system.metadata.catalogs",
		SearchCols:  []string{"catalog_name", "connector_name"},
		SortCols:    []string{"catalog", "connector"},
		DefaultSort: `"catalog"`,
	}, func(r row) {
		name := fmt.Sprint(r["catalog"])
		r["ref"] = plugin.ResourceIdentity{Kind: "catalog", Name: name, UID: name}
	})
}

func catalogOverview(rc *plugin.RequestContext) (any, error) {
	s, err := trinoSession(rc)
	if err != nil {
		return nil, err
	}
	catalog, err := catalogScope(rc, s)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `SELECT catalog_name AS "catalog", connector_name AS "connector" FROM system.metadata.catalogs WHERE catalog_name = ?`, []any{catalog})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("%w: catalog %q", plugin.ErrNotFound, catalog)
	}
	out := rows[0]
	count, err := queryRows(rc.Ctx, s, `SELECT count(*) AS "schemas" FROM `+quoteIdent(catalog)+`.information_schema.schemata`, nil)
	if err != nil {
		return nil, err
	}
	if len(count) > 0 {
		out["schemas"] = count[0]["schemas"]
	}
	return out, nil
}

func listSchemas(rc *plugin.RequestContext) (any, error) {
	s, err := trinoSession(rc)
	if err != nil {
		return nil, err
	}
	catalog, err := catalogScope(rc, s)
	if err != nil {
		return nil, err
	}
	return pageSQL(rc, s, listSpec{
		Select:      `schema_name AS "schema"`,
		From:        quoteIdent(catalog) + ".information_schema.schemata",
		SearchCols:  []string{"schema_name"},
		SortCols:    []string{"schema"},
		DefaultSort: `"schema"`,
	}, func(r row) {
		name := fmt.Sprint(r["schema"])
		r["catalog"] = catalog
		r["ref"] = plugin.ResourceIdentity{Kind: "schema", Namespace: catalog, Name: name, UID: catalog + "." + name}
	})
}

func listTables(rc *plugin.RequestContext) (any, error) {
	return relationList(rc, "BASE TABLE")
}

func listViews(rc *plugin.RequestContext) (any, error) {
	return relationList(rc, "VIEW")
}

// relationList pages a catalog's information_schema.tables. An empty tableType
// returns tables and views together (the tree renders one merged, resumable
// listing); without a schema scope it spans the catalog's user schemas.
func relationList(rc *plugin.RequestContext, tableType string) (any, error) {
	s, err := trinoSession(rc)
	if err != nil {
		return nil, err
	}
	catalog, err := catalogScope(rc, s)
	if err != nil {
		return nil, err
	}
	schema, err := optionalIdentifier(scopeValue(rc, "schema", s.opts.Schema))
	if err != nil {
		return nil, err
	}
	where := "table_schema <> 'information_schema'"
	var args []any
	if schema != "" {
		where = "table_schema = ?"
		args = append(args, schema)
	}
	if tableType != "" {
		where += " AND table_type = ?"
		args = append(args, tableType)
	}
	return pageSQL(rc, s, listSpec{
		Select:      `table_name AS "name", table_schema AS "schema", table_type AS "type"`,
		From:        quoteIdent(catalog) + ".information_schema.tables",
		Where:       where,
		Args:        args,
		SearchCols:  []string{"table_name", "table_schema"},
		SortCols:    []string{"name", "schema", "type"},
		DefaultSort: `"schema", "name"`,
	}, func(r row) {
		name, relationSchema := fmt.Sprint(r["name"]), fmt.Sprint(r["schema"])
		kind := "table"
		if strings.EqualFold(fmt.Sprint(r["type"]), "VIEW") {
			kind = "view"
		}
		r["catalog"] = catalog
		r["ref"] = plugin.ResourceIdentity{Kind: kind, Scope: catalog, Namespace: relationSchema, Name: name, UID: catalog + "." + relationSchema + "." + name}
	})
}

func tableRows(rc *plugin.RequestContext) (any, error) {
	s, err := trinoSession(rc)
	if err != nil {
		return nil, err
	}
	catalog, schema, table, err := tableScope(rc, s)
	if err != nil {
		return nil, err
	}
	req, err := rc.Page()
	if err != nil {
		return nil, err
	}
	limit := min(req.Limit, s.opts.RowLimit)
	offset, err := cursorOffset(req.Cursor)
	if err != nil {
		return nil, err
	}
	term := req.Search()
	sorted := len(req.Sort) > 0
	var columns []columnMeta
	if term != "" || sorted {
		if columns, err = tableColumnMeta(rc.Ctx, s, catalog, schema, table); err != nil {
			return nil, err
		}
	}
	where := ""
	var args []any
	if term != "" {
		clause, searchArgs := rowSearchClause(searchableColumns(columns, s.opts.RedactPatterns), term)
		if clause == "" {
			return plugin.Page[row]{Items: []row{}}, nil
		}
		where = " WHERE " + clause
		args = searchArgs
	}
	orderBy := ""
	if sorted {
		col, err := sortColumn(columns, req.Sort[0].Field, s.opts.RedactPatterns)
		if err != nil {
			return nil, err
		}
		orderBy = " ORDER BY " + quoteIdent(col)
		if req.Sort[0].Desc {
			orderBy += " DESC"
		}
	}
	// A count(*) over a Trino table is a distributed scan, so Total stays unset
	// and "load more" is driven by the extra probe row instead. Without an
	// explicit sort the connector picks the row order, so an unsorted grid pages
	// a stable dataset consistently and a changing one approximately; Trino
	// exposes no row identity a keyset cursor could anchor on.
	stmt := fmt.Sprintf("SELECT * FROM %s%s%s OFFSET %d LIMIT %d", qualified(catalog, schema, table), where, orderBy, offset, limit+1)
	rows, err := queryRows(rc.Ctx, s, stmt, args)
	if err != nil {
		return nil, err
	}
	next := ""
	if len(rows) > limit {
		rows = rows[:limit]
		next = encodeCursor(offset + limit)
	}
	redactRows(rows, s.opts.RedactPatterns)
	return plugin.Page[row]{Items: rows, NextCursor: next}, nil
}

func insertRow(rc *plugin.RequestContext) (any, error) {
	s, err := trinoSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	catalog, schema, table, err := tableScope(rc, s)
	if err != nil {
		return nil, err
	}
	var m sqldb.RowMutation
	if err := rc.Bind(&m); err != nil {
		return nil, err
	}
	if len(m.Values) == 0 {
		return nil, fmt.Errorf("%w: no values to insert", plugin.ErrInvalidInput)
	}
	columns, err := tableColumnMeta(rc.Ctx, s, catalog, schema, table)
	if err != nil {
		return nil, err
	}
	if err := ensureWritableColumns(columns, m.Values, s.opts.RedactPatterns); err != nil {
		return nil, err
	}
	stmt, args, err := dialect.Insert(qualified(catalog, schema, table), m.Values)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.QueryTimeout)
	defer cancel()
	if _, err := s.db.ExecContext(ctx, stmt, args...); err != nil {
		return nil, trinoErr(err)
	}
	return actionResult{OK: true, Message: "Row inserted."}, nil
}

func tableColumnsRoute(rc *plugin.RequestContext) (any, error) {
	s, err := trinoSession(rc)
	if err != nil {
		return nil, err
	}
	catalog, schema, table, err := tableScope(rc, s)
	if err != nil {
		return nil, err
	}
	columns, err := tableColumnMeta(rc.Ctx, s, catalog, schema, table)
	if err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("%w: %s", plugin.ErrNotFound, qualified(catalog, schema, table))
	}
	rows := make([]row, 0, len(columns))
	for _, c := range columns {
		r := row{
			"name":     c.Name,
			"type":     c.DataType,
			"nullable": c.Nullable,
			"default":  c.Default,
			"position": c.Position,
			"comment":  c.Comment,
			"catalog":  catalog,
			"schema":   schema,
			"table":    table,
		}
		sqldb.AnnotateTableColumn(r, c.DataType, sqldb.RedactColumn(c.Name, s.opts.RedactPatterns))
		rows = append(rows, r)
	}
	return pageScanned(rc, rows, false)
}

func tableStats(rc *plugin.RequestContext) (any, error) {
	s, err := trinoSession(rc)
	if err != nil {
		return nil, err
	}
	catalog, schema, table, err := tableScope(rc, s)
	if err != nil {
		return nil, err
	}
	rows, truncated, err := queryRowsCapped(rc.Ctx, s, "SHOW STATS FOR "+qualified(catalog, schema, table), nil, scanCap)
	if err != nil {
		return nil, err
	}
	return pageScanned(rc, rows, truncated)
}

func tableDDL(rc *plugin.RequestContext) (any, error) {
	return showCreate(rc, "TABLE")
}

func viewDefinition(rc *plugin.RequestContext) (any, error) {
	return showCreate(rc, "VIEW")
}

func showCreate(rc *plugin.RequestContext, kind string) (any, error) {
	s, err := trinoSession(rc)
	if err != nil {
		return nil, err
	}
	catalog, schema, table, err := tableScope(rc, s)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, "SHOW CREATE "+kind+" "+qualified(catalog, schema, table), nil)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("%w: %s", plugin.ErrNotFound, qualified(catalog, schema, table))
	}
	return map[string]any{"catalog": catalog, "schema": schema, "name": table, "ddl": soleValue(rows[0])}, nil
}

func listNodes(rc *plugin.RequestContext) (any, error) {
	s, err := trinoSession(rc)
	if err != nil {
		return nil, err
	}
	return pageSQL(rc, s, listSpec{
		Select:      "node_id, http_uri, node_version, coordinator, state",
		From:        "system.runtime.nodes",
		SearchCols:  []string{"node_id", "http_uri", "node_version", "state"},
		SortCols:    []string{"node_id", "http_uri", "node_version", "state"},
		DefaultSort: "node_id",
	}, func(r row) {
		id := fmt.Sprint(r["node_id"])
		r["ref"] = plugin.ResourceIdentity{Kind: "node", Name: id, UID: id}
	})
}

func nodeOverview(rc *plugin.RequestContext) (any, error) {
	s, err := trinoSession(rc)
	if err != nil {
		return nil, err
	}
	id := strings.TrimSpace(rc.Param("node"))
	if id == "" {
		return nil, fmt.Errorf("%w: node id is required", plugin.ErrInvalidInput)
	}
	rows, err := queryRows(rc.Ctx, s, "SELECT node_id, http_uri, node_version, coordinator, state FROM system.runtime.nodes WHERE node_id = ?", []any{id})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("%w: node %q", plugin.ErrNotFound, id)
	}
	return rows[0], nil
}

// now() is fixed at the start of the observing query, so the query row for the
// listing itself would otherwise report a negative age; greatest() floors it.
const queriesSelect = `query_id, state, "user", source, query,
       array_join(resource_group_id, '.') AS "resource_group",
       created, started, "end",
       queued_time_ms, analysis_time_ms, planning_time_ms,
       greatest(0, date_diff('millisecond', created, coalesce("end", now()))) AS "elapsed_ms"`

func listQueries(rc *plugin.RequestContext) (any, error) {
	s, err := trinoSession(rc)
	if err != nil {
		return nil, err
	}
	return pageSQL(rc, s, listSpec{
		Select:      queriesSelect,
		From:        "system.runtime.queries",
		SearchCols:  []string{"query_id", "state", "user", "source", "query"},
		SortCols:    []string{"query_id", "state", "user", "source", "created", "elapsed_ms"},
		DefaultSort: "created DESC",
	}, func(r row) {
		id := fmt.Sprint(r["query_id"])
		r["ref"] = plugin.ResourceIdentity{Kind: "query", Name: id, UID: id}
	})
}

func queryOverview(rc *plugin.RequestContext) (any, error) {
	return queryRecord(rc)
}

func queryStatement(rc *plugin.RequestContext) (any, error) {
	r, err := queryRecord(rc)
	if err != nil {
		return nil, err
	}
	return map[string]any{"query_id": r["query_id"], "state": r["state"], "sql": r["query"]}, nil
}

func queryRecord(rc *plugin.RequestContext) (row, error) {
	s, err := trinoSession(rc)
	if err != nil {
		return nil, err
	}
	id := strings.TrimSpace(rc.Param("query"))
	if id == "" {
		return nil, fmt.Errorf("%w: query id is required", plugin.ErrInvalidInput)
	}
	rows, err := queryRows(rc.Ctx, s, "SELECT "+queriesSelect+" FROM system.runtime.queries WHERE query_id = ?", []any{id})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("%w: query %q", plugin.ErrNotFound, id)
	}
	return rows[0], nil
}

func killQuery(rc *plugin.RequestContext) (any, error) {
	id := strings.TrimSpace(rc.Param("query"))
	if id == "" {
		return nil, fmt.Errorf("%w: query id is required", plugin.ErrInvalidInput)
	}
	return execStatement(rc, buildKillQuery(id))
}

// buildKillQuery renders the kill procedure call. CALL takes no bound
// parameters, so the server-assigned query id is embedded as an escaped literal.
func buildKillQuery(id string) string {
	return "CALL system.runtime.kill_query(query_id => " + quoteLiteral(id) + ", message => " + quoteLiteral("Terminated from "+plugin.DefaultClientName) + ")"
}

func listSessionProperties(rc *plugin.RequestContext) (any, error) {
	s, err := trinoSession(rc)
	if err != nil {
		return nil, err
	}
	rows, truncated, err := queryRowsCapped(rc.Ctx, s, "SHOW SESSION", nil, scanCap)
	if err != nil {
		return nil, err
	}
	// SHOW SESSION labels its columns Name/Value/Default/Type/Description, which
	// the manifest columns address in lower case.
	for i, r := range rows {
		lowered := make(row, len(r))
		for key, value := range r {
			lowered[strings.ToLower(key)] = value
		}
		rows[i] = lowered
	}
	return pageScanned(rc, rows, truncated)
}

func clusterOverview(rc *plugin.RequestContext) (any, error) {
	s, err := trinoSession(rc)
	if err != nil {
		return nil, err
	}
	out := row{
		"default_catalog": s.opts.Catalog,
		"default_schema":  s.opts.Schema,
		"read_only":       s.opts.ReadOnly,
	}
	coordinator, err := queryRows(rc.Ctx, s, "SELECT node_id, http_uri, node_version FROM system.runtime.nodes WHERE coordinator LIMIT 1", nil)
	if err != nil {
		return nil, err
	}
	if len(coordinator) > 0 {
		out["coordinator"] = coordinator[0]["http_uri"]
		out["version"] = coordinator[0]["node_version"]
	}
	counts, err := clusterCounts(rc.Ctx, s)
	if err != nil {
		return nil, err
	}
	for key, value := range counts {
		out[key] = value
	}
	return out, nil
}

func clusterCounts(ctx context.Context, s *Session) (row, error) {
	out := row{}
	nodes, err := queryRows(ctx, s, "SELECT count(*) AS total_nodes, count_if(state = 'active') AS active_nodes FROM system.runtime.nodes", nil)
	if err != nil {
		return nil, err
	}
	queries, err := queryRows(ctx, s, `SELECT count_if(state = 'RUNNING') AS running_queries, count_if(state = 'QUEUED') AS queued_queries, count_if(state = 'FAILED') AS failed_queries FROM system.runtime.queries`, nil)
	if err != nil {
		return nil, err
	}
	catalogs, err := queryRows(ctx, s, "SELECT count(*) AS catalogs FROM system.metadata.catalogs", nil)
	if err != nil {
		return nil, err
	}
	for _, rows := range [][]row{nodes, queries, catalogs} {
		if len(rows) == 0 {
			continue
		}
		for key, value := range rows[0] {
			out[key] = value
		}
	}
	return out, nil
}

func clusterMetrics(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := trinoSession(rc)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(client)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		counts, err := clusterCounts(client.Context(), s)
		if err != nil {
			if client.Context().Err() != nil {
				return nil
			}
			counts = row{}
		}
		frame := map[string]any{
			"runningQueries": counts["running_queries"],
			"queuedQueries":  counts["queued_queries"],
			"failedQueries":  counts["failed_queries"],
			"activeNodes":    counts["active_nodes"],
			"totalNodes":     counts["total_nodes"],
			"catalogs":       counts["catalogs"],
		}
		if err := enc.Encode(frame); err != nil {
			return nil
		}
		select {
		case <-client.Context().Done():
			return nil
		case <-ticker.C:
		}
	}
}

func createSchema(rc *plugin.RequestContext) (any, error) {
	s, err := trinoSession(rc)
	if err != nil {
		return nil, err
	}
	catalog, err := catalogScope(rc, s)
	if err != nil {
		return nil, err
	}
	var in struct {
		Name        string `json:"name" validate:"required"`
		IfNotExists bool   `json:"if_not_exists"`
	}
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	name, err := safeIdentifier(in.Name)
	if err != nil {
		return nil, err
	}
	stmt := "CREATE SCHEMA "
	if in.IfNotExists {
		stmt += "IF NOT EXISTS "
	}
	return execStatement(rc, stmt+quoteIdent(catalog)+"."+quoteIdent(name))
}

func dropSchema(rc *plugin.RequestContext) (any, error) {
	s, err := trinoSession(rc)
	if err != nil {
		return nil, err
	}
	catalog, schema, err := schemaScope(rc, s)
	if err != nil {
		return nil, err
	}
	return execStatement(rc, "DROP SCHEMA IF EXISTS "+quoteIdent(catalog)+"."+quoteIdent(schema))
}

func createTable(rc *plugin.RequestContext) (any, error) {
	s, err := trinoSession(rc)
	if err != nil {
		return nil, err
	}
	catalog, schema, err := schemaScope(rc, s)
	if err != nil {
		return nil, err
	}
	var in struct {
		Name        string `json:"name" validate:"required"`
		Columns     any    `json:"columns" validate:"required"`
		Comment     string `json:"comment"`
		IfNotExists bool   `json:"if_not_exists"`
	}
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	name, err := safeIdentifier(in.Name)
	if err != nil {
		return nil, err
	}
	columns, err := parseDDLColumns(in.Columns)
	if err != nil {
		return nil, err
	}
	stmt := "CREATE TABLE "
	if in.IfNotExists {
		stmt += "IF NOT EXISTS "
	}
	stmt += qualified(catalog, schema, name) + " (" + strings.Join(columns, ", ") + ")"
	if comment := strings.TrimSpace(in.Comment); comment != "" {
		stmt += " COMMENT " + quoteLiteral(comment)
	}
	return execStatement(rc, stmt)
}

func renameTable(rc *plugin.RequestContext) (any, error) {
	s, err := trinoSession(rc)
	if err != nil {
		return nil, err
	}
	catalog, schema, table, err := tableScope(rc, s)
	if err != nil {
		return nil, err
	}
	var in struct {
		Schema string `json:"schema"`
		Name   string `json:"name" validate:"required"`
	}
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	target := schema
	if strings.TrimSpace(in.Schema) != "" {
		if target, err = safeIdentifier(in.Schema); err != nil {
			return nil, err
		}
	}
	name, err := safeIdentifier(in.Name)
	if err != nil {
		return nil, err
	}
	return execStatement(rc, "ALTER TABLE "+qualified(catalog, schema, table)+" RENAME TO "+qualified(catalog, target, name))
}

func dropTable(rc *plugin.RequestContext) (any, error) {
	return dropRelation(rc, "TABLE")
}

func dropView(rc *plugin.RequestContext) (any, error) {
	return dropRelation(rc, "VIEW")
}

func dropRelation(rc *plugin.RequestContext, kind string) (any, error) {
	s, err := trinoSession(rc)
	if err != nil {
		return nil, err
	}
	catalog, schema, table, err := tableScope(rc, s)
	if err != nil {
		return nil, err
	}
	return execStatement(rc, "DROP "+kind+" IF EXISTS "+qualified(catalog, schema, table))
}

func completionRoute(rc *plugin.RequestContext) (any, error) {
	s, err := trinoSession(rc)
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
		{Label: "OFFSET", Type: "keyword"},
		{Label: "SHOW CATALOGS", Type: "keyword"},
		{Label: "SHOW SCHEMAS", Type: "keyword"},
		{Label: "SHOW TABLES", Type: "keyword"},
		{Label: "SHOW STATS FOR", Type: "keyword"},
		{Label: "EXPLAIN", Type: "keyword"},
		{Label: "EXPLAIN ANALYZE", Type: "keyword"},
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
	catalogs, err := queryRows(rc.Ctx, s, "SELECT catalog_name FROM system.metadata.catalogs ORDER BY catalog_name LIMIT 500", nil)
	if err != nil {
		return nil, err
	}
	for _, r := range catalogs {
		add(sqldb.CompletionItem{Label: fmt.Sprint(r["catalog_name"]), Type: "namespace", Detail: "catalog"})
	}
	catalog, err := optionalIdentifier(scopeValue(rc, "catalog", s.opts.Catalog))
	if err != nil || catalog == "" {
		return items, nil //nolint:nilerr // completion stays best-effort without a catalog scope
	}
	schemas, err := queryRows(rc.Ctx, s, "SELECT schema_name FROM "+quoteIdent(catalog)+".information_schema.schemata ORDER BY schema_name LIMIT 500", nil)
	if err != nil {
		return nil, err
	}
	for _, r := range schemas {
		add(sqldb.CompletionItem{Label: fmt.Sprint(r["schema_name"]), Type: "namespace", Detail: catalog})
	}
	schema, err := optionalIdentifier(scopeValue(rc, "schema", s.opts.Schema))
	if err != nil || schema == "" {
		return items, nil //nolint:nilerr // completion stays best-effort without a schema scope
	}
	columns, err := queryRows(rc.Ctx, s, "SELECT table_name, column_name FROM "+quoteIdent(catalog)+".information_schema.columns WHERE table_schema = ? ORDER BY table_name, ordinal_position LIMIT 2000", []any{schema})
	if err != nil {
		return nil, err
	}
	for _, r := range columns {
		table := fmt.Sprint(r["table_name"])
		add(sqldb.CompletionItem{Label: table, Type: "table", Detail: catalog + "." + schema, Apply: qualified(catalog, schema, table)})
		add(sqldb.CompletionItem{Label: fmt.Sprint(r["column_name"]), Type: "property", Detail: schema + "." + table})
	}
	return items, nil
}

func cancelQuery(rc *plugin.RequestContext) (any, error) {
	s, err := trinoSession(rc)
	if err != nil {
		return nil, err
	}
	ids := s.cancelAll()
	if len(ids) == 0 {
		return actionResult{OK: false, Message: "No query is running."}, nil
	}
	return actionResult{OK: true, Message: "Cancelled " + strings.Join(ids, ", ")}, nil
}

type queryScope struct {
	Catalog string
	Schema  string
}

// args renders the scope as driver header arguments. The schema header is only
// meaningful alongside a catalog, so it is dropped without one.
func (q queryScope) args() []any {
	if q.Catalog == "" {
		return nil
	}
	out := []any{sql.Named(catalogHeaderParam, q.Catalog)}
	if q.Schema != "" {
		out = append(out, sql.Named(schemaHeaderParam, q.Schema))
	}
	return out
}

type queryResult struct {
	sqldb.QueryResult
	QueryID string      `json:"queryId,omitempty"`
	Stats   *queryStats `json:"stats,omitempty"`
	Catalog string      `json:"catalog,omitempty"`
	Schema  string      `json:"schema,omitempty"`
}

func queryStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := trinoSession(rc)
	if err != nil {
		return err
	}
	scope, err := panelScope(rc, s)
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
			// A JSON decoder cannot resynchronise after a malformed frame, so the
			// stream reports it once and closes instead of spinning on the same bytes.
			_ = enc.Encode(map[string]any{"error": "Invalid query request."})
			return nil
		}
		statements := sqldb.SplitStatements(req.Query)
		result, err := executeQueryRequest(client.Context(), s, scope, req)
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
				payload["confirmMessage"] = "This Trino statement can change data, schema, privileges, or cluster state. Review it before running."
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

func executeQueryRequest(parent context.Context, s *Session, scope queryScope, req sqldb.QueryRequest) (queryResult, error) {
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
		id = nextRequestID()
	}
	progress := &progressCollector{}
	s.track(id, runningQuery{cancel: cancel, progress: progress})
	defer func() {
		cancel()
		s.untrack(id)
	}()
	results := make([]sqldb.StatementResult, 0, len(statements))
	for _, st := range statements {
		res, err := executeStatement(ctx, s, scope, st, progress)
		if err != nil {
			return queryResult{}, err
		}
		results = append(results, res)
	}
	out := queryResult{Catalog: scope.Catalog, Schema: scope.Schema}
	out.Statements = results
	if len(results) > 0 {
		last := results[len(results)-1]
		out.Columns = last.Columns
		out.Rows = last.Rows
		out.RowCount = last.RowCount
		out.ElapsedMS = last.ElapsedMS
		out.Statement = last.Statement
		out.CommandTag = last.CommandTag
	}
	out.QueryID, out.Stats = progress.snapshot()
	return out, nil
}

func executeStatement(ctx context.Context, s *Session, scope queryScope, statement string, progress *progressCollector) (sqldb.StatementResult, error) {
	start := time.Now()
	args := scope.args()
	if progress != nil {
		args = append(args, sql.Named(progressCallbackParam, progress), sql.Named(progressCallbackPeriodParam, progressPeriod))
	}
	if !statementReturnsRows(statement) {
		res, err := s.db.ExecContext(ctx, statement, args...)
		if err != nil {
			return sqldb.StatementResult{}, trinoErr(err)
		}
		affected, _ := res.RowsAffected()
		out := sqldb.StatementResult{Statement: statement, RowCount: affected, ElapsedMS: time.Since(start).Milliseconds()}
		out.CommandTag = sqldb.FirstKeyword(statement)
		if affected > 0 {
			out.CommandTag += " " + strconv.FormatInt(affected, 10)
		}
		return out, nil
	}
	rows, err := s.db.QueryContext(ctx, statement, args...)
	if err != nil {
		return sqldb.StatementResult{}, trinoErr(err)
	}
	columns, err := rows.Columns()
	if err != nil {
		_ = rows.Close()
		return sqldb.StatementResult{}, trinoErr(err)
	}
	out := sqldb.StatementResult{Statement: statement, Columns: columns}
	for rows.Next() {
		values, err := scanValues(rows, columns)
		if err != nil {
			_ = rows.Close()
			return sqldb.StatementResult{}, trinoErr(err)
		}
		out.Rows = append(out.Rows, values)
		if len(out.Rows) >= s.opts.RowLimit {
			break
		}
	}
	if err := rows.Close(); err != nil {
		return sqldb.StatementResult{}, trinoErr(err)
	}
	if err := rows.Err(); err != nil {
		return sqldb.StatementResult{}, trinoErr(err)
	}
	out.RowCount = int64(len(out.Rows))
	out.CommandTag = sqldb.FirstKeyword(statement)
	out.Rows = sqldb.RedactRows(out.Columns, out.Rows, s.opts.RedactPatterns)
	out.ElapsedMS = time.Since(start).Milliseconds()
	return out, nil
}

func statementReturnsRows(statement string) bool {
	switch sqldb.FirstKeyword(statement) {
	case "SELECT", "SHOW", "EXPLAIN", "WITH", "DESCRIBE", "DESC", "VALUES", "TABLE":
		return true
	default:
		return false
	}
}

// isReadOnlyStatement rejects EXPLAIN ANALYZE: unlike a plain EXPLAIN it runs
// the analysed statement, so an EXPLAIN ANALYZE INSERT would write.
func isReadOnlyStatement(statement string) bool {
	switch sqldb.FirstKeyword(statement) {
	case "SELECT", "SHOW", "DESCRIBE", "DESC", "WITH", "VALUES", "TABLE":
		return true
	case "EXPLAIN":
		return !explainAnalyze(statement)
	default:
		return false
	}
}

func explainAnalyze(statement string) bool {
	fields := strings.Fields(strings.ToUpper(statement))
	return len(fields) > 1 && fields[1] == "ANALYZE"
}

func isDestructiveStatement(statement string) bool {
	switch sqldb.FirstKeyword(statement) {
	case "INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "DROP", "ALTER", "TRUNCATE",
		"CALL", "GRANT", "REVOKE", "DENY", "COMMENT", "ANALYZE", "REFRESH", "SET", "RESET", "USE":
		return true
	case "EXPLAIN":
		return explainAnalyze(statement)
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

func execStatement(rc *plugin.RequestContext, statement string) (any, error) {
	s, err := trinoSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.QueryTimeout)
	defer cancel()
	if _, err := s.db.ExecContext(ctx, statement); err != nil {
		return nil, trinoErr(err)
	}
	return actionResult{OK: true}, nil
}

type listSpec struct {
	Select string
	From   string
	Where  string
	Args   []any
	// SearchCols name source columns (the free-text filter runs in WHERE, which
	// cannot see output aliases); SortCols name output columns, which ORDER BY
	// resolves.
	SearchCols  []string
	SortCols    []string
	DefaultSort string
}

// pageSQL pushes the grid's search, sort, and cursor into one Trino statement so
// filtering and ordering span the whole dataset instead of the current page. It
// probes one extra row to decide hasNext and leaves Total unset, because a
// count over a connector is a second distributed query.
func pageSQL(rc *plugin.RequestContext, s *Session, spec listSpec, decorate func(row)) (plugin.Page[row], error) {
	req, err := rc.Page()
	if err != nil {
		return plugin.Page[row]{}, err
	}
	offset, err := cursorOffset(req.Cursor)
	if err != nil {
		return plugin.Page[row]{}, err
	}
	args := append([]any{}, spec.Args...)
	where := spec.Where
	if term := req.Search(); term != "" {
		clause, searchArgs := dialect.SearchClause("VARCHAR", spec.SearchCols, term, len(args)+1)
		if clause == "" {
			return plugin.Page[row]{Items: []row{}}, nil
		}
		where = andClause(where, clause)
		args = append(args, searchArgs...)
	}
	order := spec.DefaultSort
	if len(req.Sort) > 0 {
		field := strings.TrimSpace(req.Sort[0].Field)
		if !slices.Contains(spec.SortCols, field) {
			return plugin.Page[row]{}, fmt.Errorf("%w: cannot sort by %q", plugin.ErrInvalidInput, field)
		}
		order = quoteIdent(field)
		if req.Sort[0].Desc {
			order += " DESC"
		}
	}
	var b strings.Builder
	b.WriteString("SELECT ")
	b.WriteString(spec.Select)
	b.WriteString(" FROM ")
	b.WriteString(spec.From)
	if where != "" {
		b.WriteString(" WHERE ")
		b.WriteString(where)
	}
	if order != "" {
		b.WriteString(" ORDER BY ")
		b.WriteString(order)
	}
	// Trino rejects prepared-statement parameters in OFFSET/LIMIT, so the two
	// validated integers are rendered into the statement text.
	fmt.Fprintf(&b, " OFFSET %d LIMIT %d", offset, req.Limit+1)
	rows, err := queryRows(rc.Ctx, s, b.String(), args)
	if err != nil {
		return plugin.Page[row]{}, err
	}
	next := ""
	if len(rows) > req.Limit {
		rows = rows[:req.Limit]
		next = encodeCursor(offset + req.Limit)
	}
	if decorate != nil {
		for _, r := range rows {
			decorate(r)
		}
	}
	return plugin.Page[row]{Items: rows, NextCursor: next}, nil
}

// pageScanned pages a listing Trino can only return whole (SHOW output). The
// rows are materialized per request, so sorting them in place is safe; a
// truncated scan drops Total rather than reporting a count it knows is short.
func pageScanned(rc *plugin.RequestContext, rows []row, truncated bool) (plugin.Page[row], error) {
	req, err := rc.Page()
	if err != nil {
		return plugin.Page[row]{}, err
	}
	rows = plugin.SortRows(plugin.FilterRows(rows, req.Search()), req.Sort)
	start, err := cursorOffset(req.Cursor)
	if err != nil {
		return plugin.Page[row]{}, err
	}
	start = min(start, len(rows))
	end := min(start+req.Limit, len(rows))
	next := ""
	if end < len(rows) {
		next = encodeCursor(end)
	}
	page := plugin.Page[row]{Items: rows[start:end], NextCursor: next}
	if !truncated {
		total := len(rows)
		page.Total = &total
	}
	return page, nil
}

func pageOf(res any, err error) (plugin.Page[row], error) {
	if err != nil {
		return plugin.Page[row]{}, err
	}
	page, ok := res.(plugin.Page[row])
	if !ok {
		return plugin.Page[row]{}, fmt.Errorf("%w: list source returned an unexpected page", plugin.ErrUnavailable)
	}
	return page, nil
}

func queryRows(ctx context.Context, s *Session, statement string, args []any) ([]row, error) {
	rows, _, err := queryRowsCapped(ctx, s, statement, args, scanCap)
	return rows, err
}

// queryRowsCapped stops materializing at max and reports whether the result set
// still had rows.
func queryRowsCapped(ctx context.Context, s *Session, statement string, args []any, max int) ([]row, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, s.opts.QueryTimeout)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, false, trinoErr(err)
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		return nil, false, trinoErr(err)
	}
	out := []row{}
	truncated := false
	for rows.Next() {
		if len(out) >= max {
			truncated = true
			break
		}
		values, err := scanValues(rows, columns)
		if err != nil {
			return nil, false, trinoErr(err)
		}
		r := row{}
		for i, name := range columns {
			if i < len(values) {
				r[name] = values[i]
			}
		}
		out = append(out, r)
	}
	if !truncated {
		if err := rows.Err(); err != nil {
			return nil, false, trinoErr(err)
		}
	}
	return out, truncated, nil
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

type columnMeta struct {
	Name     string
	DataType string
	Nullable bool
	Default  string
	Position int64
	Comment  string
}

func tableColumnMeta(ctx context.Context, s *Session, catalog, schema, table string) ([]columnMeta, error) {
	rows, err := queryRows(ctx, s, `
SELECT column_name, data_type, is_nullable, column_default, ordinal_position, comment
FROM `+quoteIdent(catalog)+`.information_schema.columns
WHERE table_schema = ? AND table_name = ?
ORDER BY ordinal_position`, []any{schema, table})
	if err != nil {
		return nil, err
	}
	out := make([]columnMeta, 0, len(rows))
	for _, r := range rows {
		out = append(out, columnMeta{
			Name:     text(r["column_name"]),
			DataType: text(r["data_type"]),
			Nullable: truthy(r["is_nullable"]),
			Default:  text(r["column_default"]),
			Position: number(r["ordinal_position"]),
			Comment:  text(r["comment"]),
		})
	}
	return out, nil
}

// searchableColumns keeps the free-text filter to columns Trino can cast to
// varchar (array, map, row, and digest columns would fail the whole statement)
// and drops masked columns, so the filter cannot be used to probe a value the
// grid refuses to display.
func searchableColumns(columns []columnMeta, patterns []string) []string {
	out := make([]string, 0, len(columns))
	for _, c := range columns {
		if castableToVarchar(c.DataType) && !sqldb.RedactColumn(c.Name, patterns) {
			out = append(out, c.Name)
		}
	}
	return out
}

// rowSearchClause builds the data grid's filter itself: the shared builder
// validates column names against a stricter SQL identifier rule than Trino
// connectors use, and would silently drop "sales-2026" or "col$1" from the
// filter while still reporting the result as a full-table match.
func rowSearchClause(columns []string, term string) (string, []any) {
	term = strings.TrimSpace(term)
	if term == "" || len(columns) == 0 {
		return "", nil
	}
	parts := make([]string, 0, len(columns))
	args := make([]any, 0, len(columns))
	for _, name := range columns {
		col, err := safeIdentifier(name)
		if err != nil {
			continue
		}
		parts = append(parts, "UPPER(CAST("+quoteIdent(col)+" AS VARCHAR)) LIKE UPPER(?)")
		args = append(args, "%"+term+"%")
	}
	if len(parts) == 0 {
		return "", nil
	}
	return "(" + strings.Join(parts, " OR ") + ")", args
}

func castableToVarchar(dataType string) bool {
	t := strings.ToLower(strings.TrimSpace(dataType))
	for _, prefix := range []string{"array", "map", "row", "varbinary", "hyperloglog", "p4hyperloglog", "qdigest", "tdigest", "setdigest"} {
		if strings.HasPrefix(t, prefix) {
			return false
		}
	}
	return true
}

func sortColumn(columns []columnMeta, field string, patterns []string) (string, error) {
	name, err := safeIdentifier(field)
	if err != nil {
		return "", err
	}
	for _, c := range columns {
		if c.Name != name {
			continue
		}
		if sqldb.RedactColumn(c.Name, patterns) {
			return "", fmt.Errorf("%w: column %q is masked and cannot order the grid", plugin.ErrForbidden, field)
		}
		return name, nil
	}
	return "", fmt.Errorf("%w: cannot sort by %q", plugin.ErrInvalidInput, field)
}

func ensureWritableColumns(columns []columnMeta, values map[string]any, patterns []string) error {
	known := make(map[string]bool, len(columns))
	for _, c := range columns {
		known[c.Name] = true
	}
	for name := range values {
		if !known[name] {
			return fmt.Errorf("%w: unknown column %q", plugin.ErrInvalidInput, name)
		}
		if sqldb.RedactColumn(name, patterns) {
			return fmt.Errorf("%w: column %q is redacted and cannot be written", plugin.ErrForbidden, name)
		}
	}
	return nil
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

// ddlColumn renders one CREATE TABLE column. Trino has no column defaults, so
// only the name, type, and nullability are emitted.
func ddlColumn(spec sqldb.ColumnSpec) (string, error) {
	name, err := safeIdentifier(spec.Name)
	if err != nil {
		return "", err
	}
	dataType := strings.TrimSpace(spec.Type)
	if !sqldb.SafeType(dataType) {
		return "", fmt.Errorf("%w: unsafe column type", plugin.ErrInvalidInput)
	}
	out := quoteIdent(name) + " " + dataType
	if !spec.Nullable {
		out += " NOT NULL"
	}
	return out, nil
}

func ensureWritable(s *Session) error {
	if s.opts.ReadOnly {
		return fmt.Errorf("%w: read-only mode blocks write operations", plugin.ErrForbidden)
	}
	return nil
}

func trinoErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return plugin.ErrNotFound
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: query cancelled or timed out", plugin.ErrUnavailable)
	}
	return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
}

func catalogScope(rc *plugin.RequestContext, s *Session) (string, error) {
	raw := scopeValue(rc, "catalog", s.opts.Catalog)
	if strings.TrimSpace(raw) == "" {
		return "", fmt.Errorf("%w: catalog is required; set a default catalog on the connection", plugin.ErrInvalidInput)
	}
	return safeIdentifier(raw)
}

func schemaScope(rc *plugin.RequestContext, s *Session) (string, string, error) {
	catalog, err := catalogScope(rc, s)
	if err != nil {
		return "", "", err
	}
	raw := scopeValue(rc, "schema", s.opts.Schema)
	if strings.TrimSpace(raw) == "" {
		return "", "", fmt.Errorf("%w: schema is required", plugin.ErrInvalidInput)
	}
	schema, err := safeIdentifier(raw)
	if err != nil {
		return "", "", err
	}
	return catalog, schema, nil
}

func tableScope(rc *plugin.RequestContext, s *Session) (string, string, string, error) {
	catalog, schema, err := schemaScope(rc, s)
	if err != nil {
		return "", "", "", err
	}
	table, err := safeIdentifier(scopeValue(rc, "table", ""))
	if err != nil {
		return "", "", "", err
	}
	return catalog, schema, table, nil
}

// panelScope resolves the optional catalog/schema a query-editor panel was
// opened with; a console without either falls back to the connection defaults.
func panelScope(rc *plugin.RequestContext, s *Session) (queryScope, error) {
	catalog, err := optionalIdentifier(scopeValue(rc, "catalog", s.opts.Catalog))
	if err != nil {
		return queryScope{}, err
	}
	schema, err := optionalIdentifier(scopeValue(rc, "schema", s.opts.Schema))
	if err != nil {
		return queryScope{}, err
	}
	return queryScope{Catalog: catalog, Schema: schema}, nil
}

func scopeValue(rc *plugin.RequestContext, name, fallback string) string {
	for _, candidate := range []string{rc.Param(name), rc.Query().Get("p." + name), fallback} {
		if strings.TrimSpace(candidate) != "" {
			return strings.TrimSpace(candidate)
		}
	}
	return ""
}

func andClause(left, right string) string {
	switch {
	case left == "":
		return right
	case right == "":
		return left
	default:
		return "(" + left + ") AND " + right
	}
}

func qualified(catalog, schema, name string) string {
	return quoteIdent(catalog) + "." + quoteIdent(schema) + "." + quoteIdent(name)
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// encodeCursor hides the offset behind an opaque token so the client cannot
// treat it as a row count.
func encodeCursor(offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte("o" + strconv.Itoa(offset)))
}

// cursorOffset decodes an emitted cursor and still accepts the grid's plain
// numeric offset fallback.
func cursorOffset(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	if n, err := strconv.Atoi(raw); err == nil {
		if n < 0 {
			return 0, errInvalidCursor()
		}
		return n, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil || len(decoded) < 2 || decoded[0] != 'o' {
		return 0, errInvalidCursor()
	}
	n, err := strconv.Atoi(string(decoded[1:]))
	if err != nil || n < 0 {
		return 0, errInvalidCursor()
	}
	return n, nil
}

func errInvalidCursor() error {
	return fmt.Errorf("%w: cursor is not a valid page token", plugin.ErrInvalidInput)
}

func nextRequestID() string {
	return strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + strconv.FormatUint(requestSeq.Add(1), 36)
}

// soleValue reads the single cell of a one-column result such as SHOW CREATE,
// whose column is labelled "Create Table" or "Create View" by the server.
func soleValue(r row) string {
	for key, value := range r {
		if strings.HasPrefix(key, "Create") {
			return text(value)
		}
	}
	for _, value := range r {
		return text(value)
	}
	return ""
}

func text(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

func number(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int32:
		return int64(n)
	case int:
		return int64(n)
	case float64:
		return int64(n)
	case string:
		parsed, _ := strconv.ParseInt(n, 10, 64)
		return parsed
	}
	return 0
}

// truthy reads information_schema flags, which some connectors report as a
// boolean and others as the SQL-standard 'YES'/'NO' text.
func truthy(v any) bool {
	switch value := v.(type) {
	case bool:
		return value
	case string:
		return strings.EqualFold(value, "YES") || strings.EqualFold(value, "TRUE")
	}
	return false
}
