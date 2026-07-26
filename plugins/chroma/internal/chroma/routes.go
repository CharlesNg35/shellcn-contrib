package chroma

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

// maxCompletionKeys bounds the metadata keys offered to the query editor so a
// wide collection cannot flood the completion list.
const maxCompletionKeys = 200

func Routes() []plugin.Route {
	return []plugin.Route{
		{ID: rid("overview"), Method: plugin.MethodGet, Path: "/overview", Permission: "chroma.server.read", Risk: plugin.RiskSafe, AuditEvent: rid("overview"), Handle: overview},
		{ID: rid("server.metrics"), Method: plugin.MethodWS, Path: "/metrics", Permission: "chroma.server.read", Risk: plugin.RiskSafe, AuditEvent: rid("server.metrics"), Stream: serverMetrics},

		{ID: rid("tenants.tree"), Method: plugin.MethodGet, Path: "/tree/tenants", Permission: "chroma.tenants.read", Risk: plugin.RiskSafe, AuditEvent: rid("tenants.tree"), Handle: tenantsTree},
		{ID: rid("databases.tree"), Method: plugin.MethodGet, Path: "/tree/tenants/{tenant}/databases", Permission: "chroma.databases.read", Risk: plugin.RiskSafe, AuditEvent: rid("databases.tree"), Handle: databasesTree},
		{ID: rid("tenant.read"), Method: plugin.MethodGet, Path: "/tenants/{tenant}", Permission: "chroma.tenants.read", Risk: plugin.RiskSafe, AuditEvent: rid("tenant.read"), Handle: tenantRead},
		{ID: rid("tenant.create"), Method: plugin.MethodPost, Path: "/tenants", Permission: "chroma.tenants.write", Risk: plugin.RiskWrite, AuditEvent: rid("tenant.create"), Input: nameSchema("Tenant", "Tenant name"), Handle: tenantCreate},

		{ID: rid("databases.list"), Method: plugin.MethodGet, Path: "/tenants/{tenant}/databases", Permission: "chroma.databases.read", Risk: plugin.RiskSafe, AuditEvent: rid("databases.list"), Handle: databasesList},
		{ID: rid("database.create"), Method: plugin.MethodPost, Path: "/tenants/{tenant}/databases", Permission: "chroma.databases.write", Risk: plugin.RiskWrite, AuditEvent: rid("database.create"), Input: nameSchema("Database", "Database name"), Handle: databaseCreate},
		{ID: rid("database.delete"), Method: plugin.MethodDelete, Path: "/tenants/{tenant}/databases/{database}", Permission: "chroma.databases.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("database.delete"), Handle: databaseDelete},

		{ID: rid("collections.list"), Method: plugin.MethodGet, Path: "/collections", Permission: "chroma.collections.read", Risk: plugin.RiskSafe, AuditEvent: rid("collections.list"), Handle: collectionsList},
		{ID: rid("collection.read"), Method: plugin.MethodGet, Path: "/collections/{collection}", Permission: "chroma.collections.read", Risk: plugin.RiskSafe, AuditEvent: rid("collection.read"), Handle: collectionRead},
		{ID: rid("collection.schema"), Method: plugin.MethodGet, Path: "/collections/{collection}/schema", Permission: "chroma.collections.read", Risk: plugin.RiskSafe, AuditEvent: rid("collection.schema"), Handle: collectionSchema},
		{ID: rid("collection.metrics"), Method: plugin.MethodWS, Path: "/collections/{collection}/metrics", Permission: "chroma.collections.read", Risk: plugin.RiskSafe, AuditEvent: rid("collection.metrics"), Stream: collectionMetrics},
		{ID: rid("collection.create"), Method: plugin.MethodPost, Path: "/collections", Permission: "chroma.collections.write", Risk: plugin.RiskWrite, AuditEvent: rid("collection.create"), Input: collectionCreateSchema(), Handle: collectionCreate},
		{ID: rid("collection.update"), Method: plugin.MethodPut, Path: "/collections/{collection}", Permission: "chroma.collections.write", Risk: plugin.RiskWrite, AuditEvent: rid("collection.update"), Input: collectionUpdateSchema(), Handle: collectionUpdate},
		{ID: rid("collection.delete"), Method: plugin.MethodDelete, Path: "/collections/{collection}", Permission: "chroma.collections.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("collection.delete"), Handle: collectionDelete},

		{ID: rid("records.list"), Method: plugin.MethodGet, Path: "/collections/{collection}/records", Permission: "chroma.records.read", Risk: plugin.RiskSafe, AuditEvent: rid("records.list"), Handle: recordsList},
		{ID: rid("record.read"), Method: plugin.MethodGet, Path: "/collections/{collection}/records/{record}", Permission: "chroma.records.read", Risk: plugin.RiskSafe, AuditEvent: rid("record.read"), Handle: recordRead},
		{ID: rid("record.neighbors"), Method: plugin.MethodGet, Path: "/collections/{collection}/records/{record}/neighbors", Permission: "chroma.records.read", Risk: plugin.RiskSafe, AuditEvent: rid("record.neighbors"), Handle: recordNeighbors},
		{ID: rid("records.add"), Method: plugin.MethodPost, Path: "/collections/{collection}/records", Permission: "chroma.records.write", Risk: plugin.RiskWrite, AuditEvent: rid("records.add"), Handle: recordsAdd},
		{ID: rid("record.update"), Method: plugin.MethodPatch, Path: "/collections/{collection}/records", Permission: "chroma.records.write", Risk: plugin.RiskWrite, AuditEvent: rid("record.update"), Handle: recordUpdate},
		{ID: rid("records.delete"), Method: plugin.MethodDelete, Path: "/collections/{collection}/records", Permission: "chroma.records.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("records.delete"), Handle: recordsDelete},

		{ID: rid("query"), Method: plugin.MethodWS, Path: "/collections/{collection}/query", Permission: "chroma.query.execute", Risk: plugin.RiskSafe, AuditEvent: rid("query"), Stream: queryStream},
		{ID: rid("query.complete"), Method: plugin.MethodGet, Path: "/collections/{collection}/completions", Permission: "chroma.query.execute", Risk: plugin.RiskSafe, AuditEvent: rid("query.complete"), Handle: queryComplete},

		{ID: rid("projection"), Method: plugin.MethodWS, Path: "/collections/{collection}/projection", Permission: "chroma.visualize.read", Risk: plugin.RiskSafe, AuditEvent: rid("projection"), Stream: projectionStream},
		{ID: rid("spaces"), Method: plugin.MethodWS, Path: "/collections/{collection}/spaces", Permission: "chroma.visualize.read", Risk: plugin.RiskSafe, AuditEvent: rid("spaces"), Stream: spacesStream},

		{ID: rid("history.list"), Method: plugin.MethodGet, Path: "/collections/{collection}/history", Permission: "chroma.history.read", Risk: plugin.RiskSafe, AuditEvent: rid("history.list"), Handle: historyList},
		{ID: rid("history.clear"), Method: plugin.MethodDelete, Path: "/collections/{collection}/history", Permission: "chroma.history.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("history.clear"), Handle: historyClear},

		{ID: rid("queries.list"), Method: plugin.MethodGet, Path: "/queries", Permission: "chroma.query.read", Risk: plugin.RiskSafe, AuditEvent: rid("queries.list"), Handle: queriesList},
		{ID: rid("queries.save"), Method: plugin.MethodPost, Path: "/queries", Permission: "chroma.query.write", Risk: plugin.RiskWrite, AuditEvent: rid("queries.save"), Input: savedQuerySchema(), Handle: queriesSave},
		{ID: rid("queries.delete"), Method: plugin.MethodDelete, Path: "/queries/{id}", Permission: "chroma.query.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("queries.delete"), Handle: queriesDelete},
	}
}

func nameSchema(group, label string) *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: group, Fields: []plugin.Field{
		{Key: "name", Label: label, Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{
			{Type: plugin.ValidatorRegex, Value: `^[A-Za-z0-9][A-Za-z0-9._-]{1,127}$`, Message: "letters, digits, underscore, hyphen, or dot"},
		}},
	}}}}
}

func collectionCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{
		{Name: "Collection", Fields: []plugin.Field{
			{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{
				{Type: plugin.ValidatorRegex, Value: `^[A-Za-z0-9][A-Za-z0-9._-]{1,127}$`, Message: "letters, digits, underscore, hyphen, or dot"},
			}},
			{Key: "space", Label: "Distance space", Type: plugin.FieldSelect, Required: true, Default: spaceL2, Options: []plugin.Option{
				{Label: "Squared L2", Value: spaceL2},
				{Label: "Cosine", Value: spaceCosine},
				{Label: "Inner product", Value: spaceIP},
			}},
			{Key: "metadata", Label: "Metadata", Type: plugin.FieldJSON, Help: "Free-form collection metadata object."},
			{Key: "get_or_create", Label: "Reuse if it exists", Type: plugin.FieldToggle},
		}},
		{Name: "Index tuning", Fields: []plugin.Field{
			{Key: "ef_construction", Label: "efConstruction", Type: plugin.FieldNumber, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 10000}}},
			{Key: "ef_search", Label: "efSearch", Type: plugin.FieldNumber, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 10000}}},
			{Key: "max_neighbors", Label: "Max neighbors (M)", Type: plugin.FieldNumber, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 2}, {Type: plugin.ValidatorMax, Value: 512}}},
		}},
		{Name: "Location", Fields: []plugin.Field{
			{Key: "tenant", Label: "Tenant", Type: plugin.FieldText, Help: "Leave empty to use the connection tenant."},
			{Key: "database", Label: "Database", Type: plugin.FieldAutocomplete, Help: "Leave empty to use the connection database.",
				OptionsSource: &plugin.DataSource{RouteID: rid("databases.list")}},
		}},
	}}
}

func collectionUpdateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Collection", Fields: []plugin.Field{
		{Key: "new_name", Label: "New name", Type: plugin.FieldText, Default: "${resource.name}", Validators: []plugin.Validator{
			{Type: plugin.ValidatorRegex, Value: `^[A-Za-z0-9][A-Za-z0-9._-]{1,127}$`, Message: "letters, digits, underscore, hyphen, or dot"},
		}},
		{Key: "new_metadata", Label: "Metadata", Type: plugin.FieldJSON, Help: "Replaces the collection metadata object."},
	}}}}
}

func savedQuerySchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Saved query", Fields: []plugin.Field{
		{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true},
		{Key: "query", Label: "Query", Type: plugin.FieldJSON, Required: true},
		{Key: "collection", Label: "Collection", Type: plugin.FieldText, Default: "${resource.name}"},
	}}}}
}

func overview(rc *plugin.RequestContext) (any, error) {
	s, sc, err := sessionScope(rc)
	if err != nil {
		return nil, err
	}
	out := row{
		"ref":      serverRef(),
		"endpoint": s.opts.Endpoint,
		"tenant":   sc.Tenant,
		"database": sc.Database,
		"readOnly": s.opts.ReadOnly,
		"status":   "unreachable",
	}
	var version string
	if err := s.do(rc.Ctx, http.MethodGet, "/version", nil, nil, &version); err == nil {
		out["version"] = version
	}
	var beat map[string]any
	if err := s.do(rc.Ctx, http.MethodGet, "/heartbeat", nil, nil, &beat); err == nil {
		out["heartbeat"] = beat["nanosecond heartbeat"]
		out["status"] = "degraded"
	}
	var health map[string]bool
	if err := s.do(rc.Ctx, http.MethodGet, "/healthcheck", nil, nil, &health); err == nil {
		out["executorReady"] = health["is_executor_ready"]
		out["logClientReady"] = health["is_log_client_ready"]
		if health["is_executor_ready"] && health["is_log_client_ready"] {
			out["status"] = "ready"
		}
	}
	var checks struct {
		MaxBatchSize int  `json:"max_batch_size"`
		Base64       bool `json:"supports_base64_encoding"`
	}
	if err := s.do(rc.Ctx, http.MethodGet, "/pre-flight-checks", nil, nil, &checks); err == nil {
		out["maxBatchSize"] = checks.MaxBatchSize
		out["base64Embeddings"] = checks.Base64
	}
	if identity, err := s.identity(rc.Ctx); err == nil {
		out["identityTenant"] = identity.Tenant
		out["databases"] = identity.Databases
		if identity.UserID != "" {
			out["userId"] = identity.UserID
		}
	}
	var total int
	if err := s.do(rc.Ctx, http.MethodGet, sc.path()+"/collections_count", nil, nil, &total); err == nil {
		out["collections"] = total
	}
	return out, nil
}

type identity struct {
	UserID    string   `json:"user_id"`
	Tenant    string   `json:"tenant"`
	Databases []string `json:"databases"`
}

func (s *Session) identity(ctx context.Context) (identity, error) {
	var out identity
	err := s.do(ctx, http.MethodGet, "/auth/identity", nil, nil, &out)
	return out, err
}

func tenantsTree(rc *plugin.RequestContext) (any, error) {
	s, sc, err := sessionScope(rc)
	if err != nil {
		return nil, err
	}
	names := []string{sc.Tenant}
	if id, err := s.identity(rc.Ctx); err == nil && id.Tenant != "" && id.Tenant != sc.Tenant {
		names = append(names, id.Tenant)
	}
	nodes := make([]plugin.TreeNode, 0, len(names))
	for _, name := range names {
		nodes = append(nodes, plugin.TreeNode{
			Key:   "tenant:" + name,
			Label: name,
			Icon:  icon("building-2"),
			ChildrenSource: &plugin.DataSource{
				RouteID: rid("databases.tree"),
				Params:  map[string]string{"tenant": name},
			},
			Data: map[string]any{"tenant": name},
		})
	}
	total := len(nodes)
	return plugin.Page[plugin.TreeNode]{Items: nodes, Total: &total}, nil
}

func databasesTree(rc *plugin.RequestContext) (any, error) {
	s, sc, err := sessionScope(rc)
	if err != nil {
		return nil, err
	}
	databases, err := s.databases(rc.Ctx, sc.Tenant)
	if err != nil {
		return nil, err
	}
	nodes := make([]plugin.TreeNode, 0, len(databases))
	for _, db := range databases {
		nodes = append(nodes, plugin.TreeNode{
			Key:          "database:" + sc.Tenant + "/" + db.Name,
			Label:        db.Name,
			Icon:         icon("database"),
			Leaf:         true,
			ResourceKind: "collection",
			ListParams:   map[string]string{"tenant": sc.Tenant, "database": db.Name},
			Data:         map[string]any{"tenant": sc.Tenant, "database": db.Name},
		})
	}
	total := len(nodes)
	return plugin.Page[plugin.TreeNode]{Items: nodes, Total: &total}, nil
}

type database struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Tenant string `json:"tenant"`
}

func (s *Session) databases(ctx context.Context, tenant string) ([]database, error) {
	var out []database
	err := s.do(ctx, http.MethodGet, "/tenants/"+url.PathEscape(tenant)+"/databases", url.Values{"limit": []string{"1000"}}, nil, &out)
	return out, err
}

func tenantRead(rc *plugin.RequestContext) (any, error) {
	s, sc, err := sessionScope(rc)
	if err != nil {
		return nil, err
	}
	var out row
	if err := s.do(rc.Ctx, http.MethodGet, "/tenants/"+url.PathEscape(sc.Tenant), nil, nil, &out); err != nil {
		return nil, err
	}
	if out == nil {
		out = row{}
	}
	out["name"] = sc.Tenant
	if databases, err := s.databases(rc.Ctx, sc.Tenant); err == nil {
		names := make([]string, 0, len(databases))
		for _, db := range databases {
			names = append(names, db.Name)
		}
		out["databases"] = names
		out["database_count"] = len(names)
	}
	return out, nil
}

func tenantCreate(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := s.ensureWritable(); err != nil {
		return nil, err
	}
	name, err := bindName(rc)
	if err != nil {
		return nil, err
	}
	var out any
	if err := s.do(rc.Ctx, http.MethodPost, "/tenants", nil, row{"name": name}, &out); err != nil {
		return nil, err
	}
	return row{"ok": true, "name": name}, nil
}

func databasesList(rc *plugin.RequestContext) (any, error) {
	s, sc, err := sessionScope(rc)
	if err != nil {
		return nil, err
	}
	databases, err := s.databases(rc.Ctx, sc.Tenant)
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(databases))
	for _, db := range databases {
		item := row{"id": db.ID, "name": db.Name, "tenant": sc.Tenant, "value": db.Name, "label": db.Name}
		if total, err := s.collectionCount(rc.Ctx, scope{Tenant: sc.Tenant, Database: db.Name}); err == nil {
			item["collections"] = total
		}
		rows = append(rows, item)
	}
	return broker.PageRows(rc, rows)
}

func (s *Session) collectionCount(ctx context.Context, sc scope) (int, error) {
	var out int
	err := s.do(ctx, http.MethodGet, sc.path()+"/collections_count", nil, nil, &out)
	return out, err
}

func databaseCreate(rc *plugin.RequestContext) (any, error) {
	s, sc, err := sessionScope(rc)
	if err != nil {
		return nil, err
	}
	if err := s.ensureWritable(); err != nil {
		return nil, err
	}
	name, err := bindName(rc)
	if err != nil {
		return nil, err
	}
	var out any
	if err := s.do(rc.Ctx, http.MethodPost, sc.databasesPath(), nil, row{"name": name}, &out); err != nil {
		return nil, err
	}
	return row{"ok": true, "name": name, "tenant": sc.Tenant}, nil
}

func databaseDelete(rc *plugin.RequestContext) (any, error) {
	s, sc, err := sessionScope(rc)
	if err != nil {
		return nil, err
	}
	if err := s.ensureWritable(); err != nil {
		return nil, err
	}
	var out any
	if err := s.do(rc.Ctx, http.MethodDelete, sc.path(), nil, nil, &out); err != nil {
		return nil, err
	}
	return row{"ok": true, "name": sc.Database, "tenant": sc.Tenant}, nil
}

// collectionScanLimit caps the collections pulled in when a search or sort has
// to span every page.
const collectionScanLimit = plugin.MaxPageLimit

// countConcurrency bounds the in-flight per-collection record counts.
const countConcurrency = 8

// collectionsPage is the paged listing plus the scan cap signal. The embedded
// page keeps the wire contract (items, nextCursor, total) unchanged; truncated
// and scanLimit tell the browser the listing stopped at the cap.
type collectionsPage struct {
	plugin.Page[row]
	Truncated bool `json:"truncated,omitempty"`
	ScanLimit int  `json:"scanLimit,omitempty"`
}

func collectionsList(rc *plugin.RequestContext) (any, error) {
	s, sc, err := sessionScope(rc)
	if err != nil {
		return nil, err
	}
	req, err := rc.Page()
	if err != nil {
		return nil, err
	}
	// Search and sort span every page, so those fetch a bounded scan and page it
	// in memory; a plain listing is paged by the server instead.
	if req.Search() != "" || len(req.Sort) > 0 {
		out, err := s.collections(rc.Ctx, sc, collectionScanLimit+1, 0)
		if err != nil {
			return nil, err
		}
		truncated := len(out) > collectionScanLimit
		if truncated {
			out = out[:collectionScanLimit]
		}
		page, err := broker.PageRows(rc, collectionRows(sc, out))
		if err != nil {
			return nil, err
		}
		s.countInto(rc.Ctx, sc, page.Items)
		result := collectionsPage{Page: page, Truncated: truncated}
		if truncated {
			result.ScanLimit = collectionScanLimit
		}
		return result, nil
	}

	offset := 0
	if req.Cursor != "" {
		parsed, err := strconv.Atoi(req.Cursor)
		if err != nil || parsed < 0 {
			return nil, fmt.Errorf("%w: invalid cursor", plugin.ErrInvalidInput)
		}
		offset = parsed
	}
	out, err := s.collections(rc.Ctx, sc, req.Limit+1, offset)
	if err != nil {
		return nil, err
	}
	next := ""
	if len(out) > req.Limit {
		out = out[:req.Limit]
		next = strconv.Itoa(offset + req.Limit)
	}
	rows := collectionRows(sc, out)
	s.countInto(rc.Ctx, sc, rows)
	page := plugin.Page[row]{Items: rows, NextCursor: next}
	if total, err := s.collectionCount(rc.Ctx, sc); err == nil {
		page.Total = &total
	}
	return collectionsPage{Page: page}, nil
}

func (s *Session) collections(ctx context.Context, sc scope, limit, offset int) ([]collection, error) {
	query := url.Values{"limit": []string{strconv.Itoa(limit)}}
	if offset > 0 {
		query.Set("offset", strconv.Itoa(offset))
	}
	var out []collection
	if err := s.do(ctx, http.MethodGet, sc.collectionsPath(), query, nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func collectionRows(sc scope, cols []collection) []row {
	rows := make([]row, 0, len(cols))
	for _, col := range cols {
		if col.Tenant == "" {
			col.Tenant = sc.Tenant
		}
		if col.Database == "" {
			col.Database = sc.Database
		}
		rows = append(rows, col.row())
	}
	return rows
}

// countInto fills the record count for one page of rows; each goroutine owns
// one row, so the maps are never shared.
func (s *Session) countInto(ctx context.Context, sc scope, rows []row) {
	sem := make(chan struct{}, countConcurrency)
	var wg sync.WaitGroup
	for _, item := range rows {
		id, _ := item["id"].(string)
		if id == "" {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(item row, id string) {
			defer wg.Done()
			defer func() { <-sem }()
			if total, err := s.count(ctx, sc, id); err == nil {
				item["records"] = total
			}
		}(item, id)
	}
	wg.Wait()
}

func collectionRead(rc *plugin.RequestContext) (any, error) {
	s, sc, col, err := collectionOf(rc)
	if err != nil {
		return nil, err
	}
	out := col.row()
	out["configuration"] = col.Configuration
	if total, err := s.count(rc.Ctx, sc, col.ID); err == nil {
		out["records"] = total
		if col.Dimension != nil {
			out["estimated_vector_bytes"] = total * *col.Dimension * 4
		}
	}
	if params := col.params(); params != nil {
		out["resize_factor"] = params.ResizeFactor
		out["sync_threshold"] = intOrZero(params.SyncThreshold)
	}
	out["embedding_function"] = col.Configuration.EmbeddingFunction
	return out, nil
}

func collectionSchema(rc *plugin.RequestContext) (any, error) {
	_, _, col, err := collectionOf(rc)
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, 16)
	if col.Schema != nil {
		for valueType, indexes := range col.Schema.Defaults {
			rows = append(rows, schemaRows("(default)", valueType, indexes)...)
		}
		for key, byType := range col.Schema.Keys {
			for valueType, indexes := range byType {
				rows = append(rows, schemaRows(key, valueType, indexes)...)
			}
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i]["key"] != rows[j]["key"] {
			return fmt.Sprint(rows[i]["key"]) < fmt.Sprint(rows[j]["key"])
		}
		return fmt.Sprint(rows[i]["index"]) < fmt.Sprint(rows[j]["index"])
	})
	return broker.PageRows(rc, rows)
}

func schemaRows(key, valueType string, indexes map[string]indexEntry) []row {
	out := make([]row, 0, len(indexes))
	for name, entry := range indexes {
		item := row{
			"key":    key,
			"type":   valueType,
			"index":  name,
			"state":  "disabled",
			"config": entry.Config,
		}
		if entry.Enabled {
			item["state"] = "enabled"
		}
		if space, ok := entry.Config["space"].(string); ok {
			item["space"] = space
		}
		if source, ok := entry.Config["source_key"].(string); ok {
			item["source"] = source
		}
		out = append(out, item)
	}
	return out
}

func collectionCreate(rc *plugin.RequestContext) (any, error) {
	s, sc, err := sessionScope(rc)
	if err != nil {
		return nil, err
	}
	if err := s.ensureWritable(); err != nil {
		return nil, err
	}
	var req struct {
		Name           string         `json:"name" validate:"required"`
		Space          string         `json:"space"`
		Metadata       map[string]any `json:"metadata"`
		GetOrCreate    bool           `json:"get_or_create"`
		EFConstruction int            `json:"ef_construction"`
		EFSearch       int            `json:"ef_search"`
		MaxNeighbors   int            `json:"max_neighbors"`
		Tenant         string         `json:"tenant"`
		Database       string         `json:"database"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	if err := validateName("collection", req.Name); err != nil {
		return nil, err
	}
	if sc, err = overrideScope(sc, req.Tenant, req.Database); err != nil {
		return nil, err
	}
	hnsw := row{}
	if req.Space != "" {
		if !validSpace(req.Space) {
			return nil, fmt.Errorf("%w: distance space must be one of l2, cosine, ip", plugin.ErrInvalidInput)
		}
		hnsw["space"] = req.Space
	}
	if req.EFConstruction > 0 {
		hnsw["ef_construction"] = req.EFConstruction
	}
	if req.EFSearch > 0 {
		hnsw["ef_search"] = req.EFSearch
	}
	if req.MaxNeighbors > 0 {
		hnsw["max_neighbors"] = req.MaxNeighbors
	}
	body := row{"name": req.Name, "get_or_create": req.GetOrCreate}
	if len(req.Metadata) > 0 {
		body["metadata"] = req.Metadata
	}
	if len(hnsw) > 0 {
		body["configuration"] = row{"hnsw": hnsw}
	}
	var out collection
	if err := s.do(rc.Ctx, http.MethodPost, sc.collectionsPath(), nil, body, &out); err != nil {
		return nil, err
	}
	return row{"ok": true, "id": out.ID, "name": out.Name, "tenant": sc.Tenant, "database": sc.Database}, nil
}

// overrideScope lets a create form target a tenant/database other than the
// connection default without inventing a second set of routes.
func overrideScope(sc scope, tenant, database string) (scope, error) {
	if tenant = strings.TrimSpace(tenant); tenant != "" {
		if err := validateName("tenant", tenant); err != nil {
			return scope{}, err
		}
		sc.Tenant = tenant
	}
	if database = strings.TrimSpace(database); database != "" {
		if err := validateName("database", database); err != nil {
			return scope{}, err
		}
		sc.Database = database
	}
	return sc, nil
}

func collectionUpdate(rc *plugin.RequestContext) (any, error) {
	s, sc, col, err := collectionOf(rc)
	if err != nil {
		return nil, err
	}
	if err := s.ensureWritable(); err != nil {
		return nil, err
	}
	var req struct {
		NewName     string         `json:"new_name"`
		NewMetadata map[string]any `json:"new_metadata"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	body := row{}
	if name := strings.TrimSpace(req.NewName); name != "" && name != col.Name {
		if err := validateName("collection", name); err != nil {
			return nil, err
		}
		body["new_name"] = name
	}
	if req.NewMetadata != nil {
		body["new_metadata"] = req.NewMetadata
	}
	if len(body) == 0 {
		return nil, fmt.Errorf("%w: nothing to update", plugin.ErrInvalidInput)
	}
	var out any
	if err := s.do(rc.Ctx, http.MethodPut, sc.collectionPath(col.ID), nil, body, &out); err != nil {
		return nil, err
	}
	return row{"ok": true, "id": col.ID, "name": firstNonEmpty(fmt.Sprint(body["new_name"]), col.Name)}, nil
}

func collectionDelete(rc *plugin.RequestContext) (any, error) {
	s, sc, col, err := collectionOf(rc)
	if err != nil {
		return nil, err
	}
	if err := s.ensureWritable(); err != nil {
		return nil, err
	}
	// Chroma resolves the delete path segment as a collection name; the UUID that
	// every record route needs is rejected here.
	var out any
	if err := s.do(rc.Ctx, http.MethodDelete, sc.collectionPath(col.Name), nil, nil, &out); err != nil {
		return nil, err
	}
	return row{"ok": true, "id": col.ID, "name": col.Name}, nil
}

func recordsList(rc *plugin.RequestContext) (any, error) {
	s, sc, col, err := collectionOf(rc)
	if err != nil {
		return nil, err
	}
	req, err := rc.Page()
	if err != nil {
		return nil, err
	}
	limit := min(req.Limit, s.opts.PageLimit)
	offset := 0
	if req.Cursor != "" {
		offset, err = strconv.Atoi(req.Cursor)
		if err != nil || offset < 0 {
			return nil, fmt.Errorf("%w: invalid cursor", plugin.ErrInvalidInput)
		}
	}
	body := row{"limit": limit, "offset": offset, "include": []string{"documents", "metadatas", "uris"}}
	if term := req.Search(); term != "" {
		body["where_document"] = row{"$contains": term}
	}
	var out recordSet
	if err := s.do(rc.Ctx, http.MethodPost, sc.collectionPath(col.ID)+"/get", nil, body, &out); err != nil {
		return nil, err
	}
	// Chroma pages records by offset and exposes no ordering control, so rows keep
	// the server's order rather than a misleading page-local sort.
	next := ""
	if out.len() == limit {
		next = strconv.Itoa(offset + out.len())
	}
	return plugin.Page[row]{Items: out.rows(col), NextCursor: next}, nil
}

func recordRead(rc *plugin.RequestContext) (any, error) {
	s, sc, col, err := collectionOf(rc)
	if err != nil {
		return nil, err
	}
	id := strings.TrimSpace(rc.Param("record"))
	if id == "" {
		return nil, fmt.Errorf("%w: record id is required", plugin.ErrInvalidInput)
	}
	set, err := s.fetchByIDs(rc.Ctx, sc, col.ID, []string{id})
	if err != nil {
		return nil, err
	}
	if set.len() == 0 {
		return nil, fmt.Errorf("%w: record %q", plugin.ErrNotFound, id)
	}
	out := set.row(0, col)
	out["space"] = col.space()
	out["collection_id"] = col.ID
	out["tenant"] = col.Tenant
	out["database"] = col.Database
	if doc := set.document(0); doc != "" {
		out["document_length"] = len(doc)
	}
	out["metadata_keys"] = sortedKeys(set.metadata(0))
	return out, nil
}

func (s *Session) fetchByIDs(ctx context.Context, sc scope, collectionID string, ids []string) (recordSet, error) {
	var out recordSet
	body := row{"ids": ids, "include": []string{"documents", "metadatas", "embeddings", "uris"}}
	err := s.do(ctx, http.MethodPost, sc.collectionPath(collectionID)+"/get", nil, body, &out)
	return out, err
}

func recordNeighbors(rc *plugin.RequestContext) (any, error) {
	s, sc, col, err := collectionOf(rc)
	if err != nil {
		return nil, err
	}
	id := strings.TrimSpace(rc.Param("record"))
	if id == "" {
		return nil, fmt.Errorf("%w: record id is required", plugin.ErrInvalidInput)
	}
	anchor, err := s.fetchByIDs(rc.Ctx, sc, col.ID, []string{id})
	if err != nil {
		return nil, err
	}
	if anchor.len() == 0 || len(anchor.embedding(0)) == 0 {
		return nil, fmt.Errorf("%w: record %q has no stored embedding", plugin.ErrNotFound, id)
	}
	req, err := rc.Page()
	if err != nil {
		return nil, err
	}
	limit := min(req.Limit, s.opts.PageLimit)
	result, err := s.queryEmbedding(rc.Ctx, sc, col.ID, anchor.embedding(0), limit+1, nil, nil)
	if err != nil {
		return nil, err
	}
	set, distances := result.flatten()
	rows := make([]row, 0, set.len())
	for i := range set.IDs {
		if set.IDs[i] == id {
			continue
		}
		item := set.row(i, col)
		if i < len(distances) && distances[i] != nil {
			item["distance"] = round(*distances[i], 6)
			item["similarity"] = round(similarity(col.space(), *distances[i]), 6)
		}
		item["rank"] = len(rows) + 1
		rows = append(rows, item)
	}
	total := len(rows)
	return plugin.Page[row]{Items: rows, Total: &total}, nil
}

// similarity turns a Chroma distance into a 0..1 closeness score so tables can
// sort and shade neighbors consistently across spaces.
func similarity(space string, d float64) float64 {
	switch space {
	case spaceCosine:
		return 1 - d/2
	case spaceIP:
		return 1 - d
	default:
		return 1 / (1 + d)
	}
}

func (s *Session) queryEmbedding(ctx context.Context, sc scope, collectionID string, embedding []float64, n int, where, whereDocument map[string]any) (queryResult, error) {
	body := row{
		"query_embeddings": [][]float64{embedding},
		"n_results":        n,
		"include":          []string{"documents", "metadatas", "distances", "uris"},
	}
	if len(where) > 0 {
		body["where"] = where
	}
	if len(whereDocument) > 0 {
		body["where_document"] = whereDocument
	}
	var out queryResult
	err := s.do(ctx, http.MethodPost, sc.collectionPath(collectionID)+"/query", nil, body, &out)
	return out, err
}

func recordsAdd(rc *plugin.RequestContext) (any, error) {
	s, sc, col, err := collectionOf(rc)
	if err != nil {
		return nil, err
	}
	if err := s.ensureWritable(); err != nil {
		return nil, err
	}
	payload, err := recordPayload(rc.Body())
	if err != nil {
		return nil, err
	}
	if len(payload.IDs) == 0 {
		return nil, fmt.Errorf("%w: ids are required", plugin.ErrInvalidInput)
	}
	if len(payload.Embeddings) != len(payload.IDs) {
		return nil, fmt.Errorf("%w: one embedding is required per id", plugin.ErrInvalidInput)
	}
	body := row{"ids": payload.IDs, "embeddings": payload.Embeddings}
	if payload.Documents != nil {
		body["documents"] = payload.Documents
	}
	if payload.Metadatas != nil {
		body["metadatas"] = payload.Metadatas
	}
	if payload.URIs != nil {
		body["uris"] = payload.URIs
	}
	endpoint := "/add"
	if payload.Upsert {
		endpoint = "/upsert"
	}
	var out any
	if err := s.do(rc.Ctx, http.MethodPost, sc.collectionPath(col.ID)+endpoint, nil, body, &out); err != nil {
		return nil, err
	}
	return row{"ok": true, "count": len(payload.IDs), "upsert": payload.Upsert, "collection": col.Name}, nil
}

type recordsPayload struct {
	IDs        []string         `json:"ids"`
	Embeddings [][]float64      `json:"embeddings"`
	Documents  []*string        `json:"documents"`
	Metadatas  []map[string]any `json:"metadatas"`
	URIs       []*string        `json:"uris"`
	Upsert     bool             `json:"upsert"`
}

// recordPayload accepts the raw Chroma shape and the code-editor envelopes
// ({"records": ...} from SaveBodyKey, {"content": "..."} from a plain editor).
func recordPayload(body []byte) (recordsPayload, error) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil {
		return recordsPayload{}, fmt.Errorf("%w: body must be a JSON object", plugin.ErrInvalidInput)
	}
	raw := body
	if inner, ok := envelope["records"]; ok && len(envelope) == 1 {
		raw = inner
	} else if content, ok := envelope["content"]; ok && len(envelope) == 1 {
		var text string
		if err := json.Unmarshal(content, &text); err != nil {
			return recordsPayload{}, fmt.Errorf("%w: content must be a JSON string", plugin.ErrInvalidInput)
		}
		raw = []byte(text)
	}
	var payload recordsPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return recordsPayload{}, fmt.Errorf("%w: %v", plugin.ErrInvalidInput, err)
	}
	return payload, nil
}

func recordUpdate(rc *plugin.RequestContext) (any, error) {
	s, sc, col, err := collectionOf(rc)
	if err != nil {
		return nil, err
	}
	if err := s.ensureWritable(); err != nil {
		return nil, err
	}
	var req struct {
		ID        string         `json:"id" validate:"required"`
		Document  *string        `json:"document"`
		Metadata  map[string]any `json:"metadata"`
		Embedding []float64      `json:"embedding"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	body := row{"ids": []string{req.ID}}
	if req.Document != nil {
		body["documents"] = []*string{req.Document}
	}
	if req.Metadata != nil {
		body["metadatas"] = []map[string]any{req.Metadata}
	}
	if len(req.Embedding) > 0 {
		body["embeddings"] = [][]float64{req.Embedding}
	}
	if len(body) == 1 {
		return nil, fmt.Errorf("%w: nothing to update", plugin.ErrInvalidInput)
	}
	var out any
	if err := s.do(rc.Ctx, http.MethodPost, sc.collectionPath(col.ID)+"/update", nil, body, &out); err != nil {
		return nil, err
	}
	set, err := s.fetchByIDs(rc.Ctx, sc, col.ID, []string{req.ID})
	if err != nil || set.len() == 0 {
		return row{"ok": true, "id": req.ID}, nil
	}
	return set.row(0, col), nil
}

func recordsDelete(rc *plugin.RequestContext) (any, error) {
	s, sc, col, err := collectionOf(rc)
	if err != nil {
		return nil, err
	}
	if err := s.ensureWritable(); err != nil {
		return nil, err
	}
	var req struct {
		ID            string         `json:"id"`
		IDs           []string       `json:"ids"`
		Where         map[string]any `json:"where"`
		WhereDocument map[string]any `json:"where_document"`
	}
	if len(rc.Body()) > 0 {
		if err := rc.Bind(&req); err != nil {
			return nil, err
		}
	}
	ids := req.IDs
	if req.ID != "" {
		ids = append(ids, req.ID)
	}
	for _, id := range rc.Query()["id"] {
		if id != "" {
			ids = append(ids, id)
		}
	}
	body := row{}
	if len(ids) > 0 {
		body["ids"] = ids
	}
	if len(req.Where) > 0 {
		body["where"] = req.Where
	}
	if len(req.WhereDocument) > 0 {
		body["where_document"] = req.WhereDocument
	}
	if len(body) == 0 {
		return nil, fmt.Errorf("%w: ids, where, or where_document is required", plugin.ErrInvalidInput)
	}
	var out struct {
		Deleted int `json:"deleted"`
	}
	if err := s.do(rc.Ctx, http.MethodPost, sc.collectionPath(col.ID)+"/delete", nil, body, &out); err != nil {
		return nil, err
	}
	return row{"ok": true, "deleted": out.Deleted, "collection": col.Name}, nil
}

func queryComplete(rc *plugin.RequestContext) (any, error) {
	s, sc, col, err := collectionOf(rc)
	if err != nil {
		return nil, err
	}
	rows := []row{
		{"label": "query_embeddings", "value": `"query_embeddings": [[0, 0, 0]]`, "detail": "Vector similarity search", "type": "field"},
		{"label": "query_id", "value": `"query_id": ""`, "detail": "Search by an existing record's embedding", "type": "field"},
		{"label": "query_text", "value": `"query_text": ""`, "detail": "Literal document substring search", "type": "field"},
		{"label": "n_results", "value": `"n_results": 10`, "detail": "Neighbors to return", "type": "field"},
		{"label": "limit", "value": `"limit": 50`, "detail": "Rows returned by a metadata fetch", "type": "field"},
		{"label": "offset", "value": `"offset": 0`, "detail": "Rows skipped by a metadata fetch", "type": "field"},
		{"label": "where", "value": `"where": {}`, "detail": "Metadata filter", "type": "field"},
		{"label": "where_document", "value": `"where_document": {"$contains": ""}`, "detail": "Document filter", "type": "field"},
		{"label": "include", "value": `"include": ["documents", "metadatas", "distances"]`, "detail": "Fields to return", "type": "field"},
	}
	for _, op := range []struct{ name, detail string }{
		{"$eq", "equals"}, {"$ne", "not equals"}, {"$gt", "greater than"}, {"$gte", "greater or equal"},
		{"$lt", "less than"}, {"$lte", "less or equal"}, {"$in", "value in list"}, {"$nin", "value not in list"},
		{"$and", "all clauses match"}, {"$or", "any clause matches"},
		{"$contains", "document contains"}, {"$not_contains", "document does not contain"}, {"$regex", "document matches regex"},
	} {
		rows = append(rows, row{"label": op.name, "value": op.name, "detail": op.detail, "type": "operator"})
	}
	for _, key := range s.metadataKeys(rc.Ctx, sc, col) {
		rows = append(rows, row{"label": key, "value": `"` + key + `"`, "detail": "Metadata key", "type": "metadata"})
	}
	// Completion lists are consumed whole by the editor, so they are not paged.
	total := len(rows)
	return plugin.Page[row]{Items: rows, Total: &total}, nil
}

// metadataKeys samples records so completions offer the metadata this
// collection actually stores.
func (s *Session) metadataKeys(ctx context.Context, sc scope, col collection) []string {
	var out recordSet
	body := row{"limit": 100, "include": []string{"metadatas"}}
	if err := s.do(ctx, http.MethodPost, sc.collectionPath(col.ID)+"/get", nil, body, &out); err != nil {
		return nil
	}
	seen := map[string]struct{}{}
	for i := range out.IDs {
		for key := range out.metadata(i) {
			seen[key] = struct{}{}
		}
	}
	keys := sortedSet(seen)
	if len(keys) > maxCompletionKeys {
		keys = keys[:maxCompletionKeys]
	}
	return keys
}

func bindName(rc *plugin.RequestContext) (string, error) {
	var req struct {
		Name string `json:"name" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return "", err
	}
	name := strings.TrimSpace(req.Name)
	if err := validateName("name", name); err != nil {
		return "", err
	}
	return name, nil
}

func validSpace(space string) bool {
	for _, known := range spaces() {
		if space == known {
			return true
		}
	}
	return false
}

func sortedKeys(m map[string]any) []string {
	seen := make(map[string]struct{}, len(m))
	for key := range m {
		seen[key] = struct{}{}
	}
	return sortedSet(seen)
}

func sortedSet(seen map[string]struct{}) []string {
	out := make([]string, 0, len(seen))
	for key := range seen {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" && value != "<nil>" {
			return value
		}
	}
	return ""
}
