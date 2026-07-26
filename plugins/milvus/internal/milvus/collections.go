package milvus

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

func listDatabases(rc *plugin.RequestContext) (any, error) {
	s, c, err := clientOf(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	names, err := c.ListDatabase(ctx, milvusclient.NewListDatabaseOption())
	if err != nil {
		return nil, mapError(err)
	}
	sort.Strings(names)
	rows := make([]row, 0, len(names))
	for _, name := range names {
		item := row{
			"name":  name,
			"value": name,
			"label": name,
			"ref":   plugin.ResourceIdentity{Kind: "database", Name: name, UID: name},
		}
		if db, err := c.DescribeDatabase(ctx, milvusclient.NewDescribeDatabaseOption(name)); err == nil {
			item["properties"] = db.Properties
			item["property_count"] = len(db.Properties)
		}
		rows = append(rows, item)
	}
	return broker.PageRows(rc, rows)
}

func readDatabase(rc *plugin.RequestContext) (any, error) {
	name, err := identifier("database", rc.Param("name"))
	if err != nil {
		return nil, err
	}
	s, c, err := clientOf(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	db, err := c.DescribeDatabase(ctx, milvusclient.NewDescribeDatabaseOption(name))
	if err != nil {
		return nil, mapError(err)
	}
	dbClient, err := s.clientFor(rc.Ctx, name)
	if err != nil {
		return nil, err
	}
	collections, err := dbClient.ListCollections(ctx, milvusclient.NewListCollectionOption())
	if err != nil {
		return nil, mapError(err)
	}
	return row{
		"name":             db.Name,
		"properties":       db.Properties,
		"collection_count": len(collections),
		"collections":      collections,
	}, nil
}

func createDatabase(rc *plugin.RequestContext) (any, error) {
	var req struct {
		Name       string            `json:"name" validate:"required"`
		Properties map[string]string `json:"properties"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name, err := identifier("database", req.Name)
	if err != nil {
		return nil, err
	}
	s, c, err := writableClient(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	opt := milvusclient.NewCreateDatabaseOption(name)
	for key, value := range req.Properties {
		opt = opt.WithProperty(key, value)
	}
	err = c.CreateDatabase(ctx, opt)
	s.activity.record("database.create", name, "Created database", err)
	return ok(row{"ok": err == nil, "name": name}, err)
}

func dropDatabase(rc *plugin.RequestContext) (any, error) {
	name, err := identifier("database", rc.Param("name"))
	if err != nil {
		return nil, err
	}
	if name == defaultDatabase {
		return nil, fmt.Errorf("%w: the default database cannot be dropped", plugin.ErrInvalidInput)
	}
	s, c, err := writableClient(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	err = c.DropDatabase(ctx, milvusclient.NewDropDatabaseOption(name))
	s.activity.record("database.drop", name, "Dropped database", err)
	if err == nil {
		s.forget(name)
	}
	return ok(row{"ok": err == nil, "name": name}, err)
}

func collectionNames(rc *plugin.RequestContext) ([]string, *Session, *milvusclient.Client, error) {
	s, c, err := clientOf(rc)
	if err != nil {
		return nil, nil, nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	names, err := c.ListCollections(ctx, milvusclient.NewListCollectionOption())
	if err != nil {
		return nil, nil, nil, mapError(err)
	}
	sort.Strings(names)
	return names, s, c, nil
}

func listCollections(rc *plugin.RequestContext) (any, error) {
	names, s, c, err := collectionNames(rc)
	if err != nil {
		return nil, err
	}
	database := databaseName(rc)
	rows := make([]row, 0, len(names))
	for _, name := range names {
		rows = append(rows, row{
			"name":      name,
			"read_only": s.opts.ReadOnly,
			"ref":       plugin.ResourceIdentity{Kind: "collection", Namespace: database, Name: name, UID: database + "/" + name},
		})
	}
	req, err := rc.Page()
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	// Sorting and searching span every page, so the describe calls have to run
	// before paging whenever they feed the comparison.
	if req.Search() != "" || sortsOnCollectionDetail(req.Sort) {
		for _, item := range rows {
			describeCollectionInto(ctx, c, fmt.Sprint(item["name"]), item)
		}
		return broker.PageRows(rc, rows)
	}
	page, err := broker.PageRows(rc, rows)
	if err != nil {
		return nil, err
	}
	for _, item := range page.Items {
		describeCollectionInto(ctx, c, fmt.Sprint(item["name"]), item)
	}
	return page, nil
}

func sortsOnCollectionDetail(keys []plugin.SortKey) bool {
	for _, key := range keys {
		if key.Field != "" && key.Field != "name" {
			return true
		}
	}
	return false
}

// describeCollectionInto enriches a collection row with schema, load and size
// facts; a partial failure leaves the row usable instead of failing the list.
func describeCollectionInto(ctx context.Context, c *milvusclient.Client, name string, item row) {
	coll, err := c.DescribeCollection(ctx, milvusclient.NewDescribeCollectionOption(name))
	if err != nil {
		item["state"] = "unavailable"
		return
	}
	item["id"] = coll.ID
	item["description"] = coll.Schema.Description
	item["auto_id"] = coll.Schema.AutoID
	item["dynamic_field"] = coll.Schema.EnableDynamicField
	item["shards"] = coll.ShardNum
	item["consistency_level"] = consistencyName(coll.ConsistencyLevel)
	item["field_count"] = len(coll.Schema.Fields)
	item["vector_fields"] = vectorFieldNames(coll.Schema)
	item["properties"] = coll.Properties

	item["state"] = "not_loaded"
	if state, err := c.GetLoadState(ctx, milvusclient.NewGetLoadStateOption(name)); err == nil {
		item["state"] = loadStateName(state.State)
		item["load_progress"] = state.Progress
	}
	if stats, err := c.GetCollectionStats(ctx, milvusclient.NewGetCollectionStatsOption(name)); err == nil {
		if count, err := strconv.ParseInt(stats["row_count"], 10, 64); err == nil {
			item["entities"] = count
		}
	}
	if names, err := c.ListPartitions(ctx, milvusclient.NewListPartitionOption(name)); err == nil {
		item["partitions"] = len(names)
	}
	if names, err := c.ListIndexes(ctx, milvusclient.NewListIndexOption(name)); err == nil {
		item["indexes"] = len(names)
	}
}

func readCollection(rc *plugin.RequestContext) (any, error) {
	name, err := collectionParam(rc)
	if err != nil {
		return nil, err
	}
	s, c, err := clientOf(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	return collectionDetail(ctx, c, databaseName(rc), name)
}

func collectionDetail(ctx context.Context, c *milvusclient.Client, database, name string) (row, error) {
	coll, err := c.DescribeCollection(ctx, milvusclient.NewDescribeCollectionOption(name))
	if err != nil {
		return nil, mapError(err)
	}
	out := row{
		"name":              coll.Name,
		"database":          database,
		"id":                coll.ID,
		"description":       coll.Schema.Description,
		"auto_id":           coll.Schema.AutoID,
		"dynamic_field":     coll.Schema.EnableDynamicField,
		"shards":            coll.ShardNum,
		"consistency_level": consistencyName(coll.ConsistencyLevel),
		"field_count":       len(coll.Schema.Fields),
		"vector_fields":     vectorFieldNames(coll.Schema),
		"primary_key":       primaryKeyName(coll.Schema),
		"virtual_channels":  coll.VirtualChannels,
		"properties":        coll.Properties,
		"functions":         functionNames(coll.Schema),
		"ref":               plugin.ResourceIdentity{Kind: "collection", Namespace: database, Name: coll.Name, UID: database + "/" + coll.Name},
	}
	out["state"] = "not_loaded"
	if state, err := c.GetLoadState(ctx, milvusclient.NewGetLoadStateOption(name)); err == nil {
		out["state"] = loadStateName(state.State)
		out["load_progress"] = state.Progress
	}
	if stats, err := c.GetCollectionStats(ctx, milvusclient.NewGetCollectionStatsOption(name)); err == nil {
		if count, err := strconv.ParseInt(stats["row_count"], 10, 64); err == nil {
			out["entities"] = count
		}
	}
	if names, err := c.ListPartitions(ctx, milvusclient.NewListPartitionOption(name)); err == nil {
		out["partitions"] = len(names)
		out["partition_names"] = names
	}
	if names, err := c.ListIndexes(ctx, milvusclient.NewListIndexOption(name)); err == nil {
		out["indexes"] = len(names)
		out["index_names"] = names
	}
	if names, err := c.ListAliases(ctx, milvusclient.NewListAliasesOption(name)); err == nil {
		out["aliases"] = names
	}
	return out, nil
}

func listSchemaFields(rc *plugin.RequestContext) (any, error) {
	name, err := collectionParam(rc)
	if err != nil {
		return nil, err
	}
	s, c, err := clientOf(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	coll, err := c.DescribeCollection(ctx, milvusclient.NewDescribeCollectionOption(name))
	if err != nil {
		return nil, mapError(err)
	}
	byField := fieldIndexes(ctx, c, name, coll.Schema)
	rows := make([]row, 0, len(coll.Schema.Fields))
	for _, f := range coll.Schema.Fields {
		item := fieldRow(f)
		item["index"] = strings.Join(byField[f.Name], ", ")
		rows = append(rows, item)
	}
	return broker.PageRows(rc, rows)
}

// fieldIndexes maps each schema field to the indexes covering it. Milvus only
// reports the field name on the per-field describe call, so it is queried per
// field rather than derived from the flat index list.
func fieldIndexes(ctx context.Context, c *milvusclient.Client, collection string, schema *entity.Schema) map[string][]string {
	out := make(map[string][]string, len(schema.Fields))
	for _, f := range schema.Fields {
		names, err := c.ListIndexes(ctx, milvusclient.NewListIndexOption(collection).WithFieldName(f.Name))
		if err != nil || len(names) == 0 {
			continue
		}
		sort.Strings(names)
		out[f.Name] = names
	}
	return out
}

func indexOwners(ctx context.Context, c *milvusclient.Client, collection string, schema *entity.Schema) map[string]string {
	out := map[string]string{}
	for field, names := range fieldIndexes(ctx, c, collection, schema) {
		for _, name := range names {
			out[name] = field
		}
	}
	return out
}

func fieldRow(f *entity.Field) row {
	item := row{
		"name":           f.Name,
		"id":             f.ID,
		"value":          f.Name,
		"label":          f.Name,
		"type":           fieldTypeName(f.DataType),
		"primary":        f.PrimaryKey,
		"auto_id":        f.AutoID,
		"nullable":       f.Nullable,
		"partition_key":  f.IsPartitionKey,
		"clustering_key": f.IsClusteringKey,
		"dynamic":        f.IsDynamic,
		"description":    f.Description,
		"vector":         f.DataType.IsVectorType(),
	}
	if f.ElementType != entity.FieldTypeNone {
		item["element_type"] = fieldTypeName(f.ElementType)
	}
	if dim, ok := f.TypeParams[entity.TypeParamDim]; ok {
		if v, err := strconv.ParseInt(dim, 10, 64); err == nil {
			item["dim"] = v
		}
	}
	if maxLen, ok := f.TypeParams[entity.TypeParamMaxLength]; ok {
		if v, err := strconv.ParseInt(maxLen, 10, 64); err == nil {
			item["max_length"] = v
		}
	}
	return item
}

func createCollection(rc *plugin.RequestContext) (any, error) {
	var req struct {
		Name               string         `json:"name" validate:"required"`
		Description        string         `json:"description"`
		ConsistencyLevel   string         `json:"consistency_level"`
		ShardNum           int            `json:"shard_num"`
		EnableDynamicField bool           `json:"enable_dynamic_field"`
		Fields             []fieldRequest `json:"fields" validate:"required,min=1,dive"`
		AutoIndex          bool           `json:"auto_index"`
		MetricType         string         `json:"metric_type"`
		LoadAfterCreate    bool           `json:"load_after_create"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name, err := identifier("collection", req.Name)
	if err != nil {
		return nil, err
	}
	schema := entity.NewSchema().WithName(name).WithDescription(req.Description).WithDynamicFieldEnabled(req.EnableDynamicField)
	primaries := 0
	vectors := make([]string, 0, len(req.Fields))
	for _, spec := range req.Fields {
		f, err := spec.build()
		if err != nil {
			return nil, err
		}
		if f.PrimaryKey {
			primaries++
		}
		if f.DataType.IsVectorType() {
			vectors = append(vectors, f.Name)
		}
		schema = schema.WithField(f)
	}
	if primaries != 1 {
		return nil, fmt.Errorf("%w: exactly one primary key field is required", plugin.ErrInvalidInput)
	}
	if len(vectors) == 0 {
		return nil, fmt.Errorf("%w: at least one vector field is required", plugin.ErrInvalidInput)
	}

	s, c, err := writableClient(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	opt := milvusclient.NewCreateCollectionOption(name, schema).
		WithConsistencyLevel(consistencyLevel(req.ConsistencyLevel))
	if req.ShardNum > 0 {
		opt = opt.WithShardNum(int32(req.ShardNum))
	}
	if err := c.CreateCollection(ctx, opt); err != nil {
		s.activity.record("collection.create", name, "Create collection failed", err)
		return nil, mapError(err)
	}
	s.activity.record("collection.create", name, fmt.Sprintf("Created collection with %d field(s)", len(req.Fields)), nil)

	built := make([]string, 0, len(vectors))
	if req.AutoIndex {
		metric := entity.MetricType(strings.ToUpper(strings.TrimSpace(req.MetricType)))
		if metric == "" {
			metric = entity.COSINE
		}
		for _, field := range vectors {
			task, err := c.CreateIndex(ctx, milvusclient.NewCreateIndexOption(name, field, index.NewAutoIndex(metric)))
			if err != nil {
				s.activity.record("index.create", name+"."+field, "AUTOINDEX build failed", err)
				continue
			}
			if err := task.Await(ctx); err != nil {
				s.activity.record("index.create", name+"."+field, "AUTOINDEX build failed", err)
				continue
			}
			built = append(built, field)
			s.activity.record("index.create", name+"."+field, "Built AUTOINDEX", nil)
		}
	}
	loaded := false
	if req.LoadAfterCreate && len(built) > 0 {
		if task, err := c.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(name)); err == nil {
			loaded = task.Await(ctx) == nil
			s.activity.record("collection.load", name, "Loaded collection", nil)
		}
	}
	return row{"ok": true, "name": name, "indexed_fields": built, "loaded": loaded}, nil
}

func addCollectionField(rc *plugin.RequestContext) (any, error) {
	collection, err := collectionParam(rc)
	if err != nil {
		return nil, err
	}
	var spec fieldRequest
	if err := rc.Bind(&spec); err != nil {
		return nil, err
	}
	f, err := spec.build()
	if err != nil {
		return nil, err
	}
	if !f.Nullable {
		return nil, fmt.Errorf("%w: a field added to an existing collection must be nullable", plugin.ErrInvalidInput)
	}
	s, c, err := writableClient(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	err = c.AddCollectionField(ctx, milvusclient.NewAddCollectionFieldOption(collection, f))
	s.activity.record("collection.field.add", collection+"."+f.Name, "Added field", err)
	return ok(row{"ok": err == nil, "name": f.Name}, err)
}

func renameCollection(rc *plugin.RequestContext) (any, error) {
	collection, err := collectionParam(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		Name string `json:"name" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	target, err := identifier("collection", req.Name)
	if err != nil {
		return nil, err
	}
	s, c, err := writableClient(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	err = c.RenameCollection(ctx, milvusclient.NewRenameCollectionOption(collection, target))
	s.activity.record("collection.rename", collection, "Renamed to "+target, err)
	return ok(row{"ok": err == nil, "name": target}, err)
}

func loadCollection(rc *plugin.RequestContext) (any, error) {
	collection, err := collectionParam(rc)
	if err != nil {
		return nil, err
	}
	s, c, err := writableClient(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	task, err := c.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(collection))
	if err == nil {
		err = task.Await(ctx)
	}
	s.activity.record("collection.load", collection, "Loaded collection", err)
	return ok(row{"ok": err == nil, "name": collection}, err)
}

func releaseCollection(rc *plugin.RequestContext) (any, error) {
	collection, err := collectionParam(rc)
	if err != nil {
		return nil, err
	}
	s, c, err := writableClient(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	err = c.ReleaseCollection(ctx, milvusclient.NewReleaseCollectionOption(collection))
	s.activity.record("collection.release", collection, "Released collection from memory", err)
	return ok(row{"ok": err == nil, "name": collection}, err)
}

func flushCollection(rc *plugin.RequestContext) (any, error) {
	collection, err := collectionParam(rc)
	if err != nil {
		return nil, err
	}
	s, c, err := writableClient(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	task, err := c.Flush(ctx, milvusclient.NewFlushOption(collection))
	if err != nil {
		s.activity.record("collection.flush", collection, "Flush failed", err)
		return nil, mapError(err)
	}
	err = task.Await(ctx)
	segments, flushed, flushTs, _ := task.GetFlushStats()
	s.activity.record("collection.flush", collection, fmt.Sprintf("Flushed %d segment(s)", len(flushed)), err)
	return ok(row{"ok": err == nil, "segments": segments, "flushed_segments": flushed, "flush_timestamp": flushTs}, err)
}

func compactCollection(rc *plugin.RequestContext) (any, error) {
	collection, err := collectionParam(rc)
	if err != nil {
		return nil, err
	}
	s, c, err := writableClient(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	id, err := c.Compact(ctx, milvusclient.NewCompactOption(collection))
	if err != nil {
		s.activity.record("collection.compact", collection, "Compaction failed", err)
		return nil, mapError(err)
	}
	state := "unknown"
	if got, err := c.GetCompactionState(ctx, milvusclient.NewGetCompactionStateOption(id)); err == nil {
		state = compactionStateName(got)
	}
	s.activity.record("collection.compact", collection, fmt.Sprintf("Compaction %d %s", id, state), nil)
	return row{"ok": true, "compaction_id": id, "state": state}, nil
}

func dropCollection(rc *plugin.RequestContext) (any, error) {
	collection, err := collectionParam(rc)
	if err != nil {
		return nil, err
	}
	s, c, err := writableClient(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	err = c.DropCollection(ctx, milvusclient.NewDropCollectionOption(collection))
	s.activity.record("collection.drop", collection, "Dropped collection", err)
	return ok(row{"ok": err == nil, "name": collection}, err)
}

func listActivity(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	return broker.PageRows(rc, s.activity.list())
}

func consistencyLevel(name string) entity.ConsistencyLevel {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "strong":
		return entity.ClStrong
	case "session":
		return entity.ClSession
	case "eventually":
		return entity.ClEventually
	default:
		return entity.ClBounded
	}
}

func consistencyName(level entity.ConsistencyLevel) string {
	switch level {
	case entity.ClStrong:
		return "Strong"
	case entity.ClSession:
		return "Session"
	case entity.ClEventually:
		return "Eventually"
	case entity.ClCustomized:
		return "Customized"
	default:
		return "Bounded"
	}
}

func loadStateName(state entity.LoadStateCode) string {
	switch state {
	case entity.LoadStateLoaded:
		return "loaded"
	case entity.LoadStateLoading:
		return "loading"
	case entity.LoadStateNotLoad:
		return "not_loaded"
	default:
		return "unknown"
	}
}

func indexStateName(state index.IndexState) string {
	return strings.ToLower(strings.TrimPrefix(commonpb.IndexState(state).String(), "IndexState"))
}

func segmentStateName(state commonpb.SegmentState) string {
	return strings.ToLower(strings.TrimPrefix(state.String(), "SegmentState"))
}

func compactionStateName(state entity.CompactionState) string {
	switch state {
	case entity.CompactionStateCompleted:
		return "completed"
	case entity.CompactionStateRunning:
		return "running"
	default:
		return "unknown"
	}
}

func vectorFieldNames(schema *entity.Schema) []string {
	out := make([]string, 0, 2)
	for _, f := range schema.Fields {
		if f.DataType.IsVectorType() {
			out = append(out, f.Name)
		}
	}
	return out
}

func primaryKeyName(schema *entity.Schema) string {
	for _, f := range schema.Fields {
		if f.PrimaryKey {
			return f.Name
		}
	}
	return ""
}

func functionNames(schema *entity.Schema) []string {
	out := make([]string, 0, len(schema.Functions))
	for _, f := range schema.Functions {
		out = append(out, f.Name)
	}
	return out
}

func positive(value, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}
