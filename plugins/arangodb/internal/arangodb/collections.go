package arangodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	driver "github.com/arangodb/go-driver/v2/arangodb"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

func collectionsList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	database := databaseParam(rc, s)
	db, err := s.database(rc.Ctx, database)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	collections, err := db.Collections(ctx)
	if err != nil {
		return nil, arangoErr(err)
	}
	includeSystem := strings.EqualFold(paramOrQuery(rc, "system"), "true")
	rows := make([]row, 0, len(collections))
	for _, collection := range collections {
		name := collection.Name()
		system := strings.HasPrefix(name, "_")
		if system && !includeSystem {
			continue
		}
		item := row{
			"name":     name,
			"database": database,
			"type":     "document",
			"system":   system,
			"status":   "unknown",
			"ref":      plugin.ResourceIdentity{Kind: kindCollection, Namespace: database, Name: name, UID: encodeID(kindCollection, database, name)},
		}
		if props, err := collection.Properties(ctx); err == nil {
			item["type"] = collectionTypeName(props.Type)
			item["status"] = collectionStatus(props.StatusString, props.Status)
			item["shards"] = props.NumberOfShards
			item["replication_factor"] = replicationFactorText(props.ReplicationFactor)
			item["wait_for_sync"] = props.WaitForSync
			item["key_type"] = string(props.KeyOptions.Type)
		}
		if count, err := collection.Count(ctx); err == nil {
			item["documents"] = count
		}
		rows = append(rows, item)
	}
	return pageRows(rc, rows)
}

func collectionRead(rc *plugin.RequestContext) (any, error) {
	s, database, name, err := collectionScope(rc)
	if err != nil {
		return nil, err
	}
	col, err := s.collection(rc.Ctx, database, name)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	props, err := col.Properties(ctx)
	if err != nil {
		return nil, arangoErr(err)
	}
	out := row{
		"name":               name,
		"database":           database,
		"id":                 props.ID,
		"globally_unique_id": props.GloballyUniqueId,
		"type":               collectionTypeName(props.Type),
		"status":             collectionStatus(props.StatusString, props.Status),
		"system":             props.IsSystem,
		"wait_for_sync":      props.WaitForSync,
		"key_type":           string(props.KeyOptions.Type),
		"user_keys":          props.KeyOptions.AllowUserKeys,
		"shards":             props.NumberOfShards,
		"shard_keys":         joinStrings(props.ShardKeys),
		"replication_factor": replicationFactorText(props.ReplicationFactor),
		"write_concern":      props.WriteConcern,
		"cache_enabled":      props.CacheEnabled,
		"revision":           props.Revision,
		"smart":              props.IsSmart,
	}
	if count, err := col.Count(ctx); err == nil {
		out["documents"] = count
	}
	if figures, err := col.Statistics(ctx, true); err == nil {
		stats := asMap(figures)
		out["figures"] = stats["figures"]
		out["documents_size"] = documentsSize(stats)
		out["indexes_size"] = indexesSize(stats)
	}
	if indexes, err := col.Indexes(ctx); err == nil {
		out["indexes"] = len(indexes)
	}
	if props.Schema != nil {
		out["schema_level"] = string(props.Schema.Level)
	}
	return out, nil
}

func collectionCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Collection", Fields: []plugin.Field{
		{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true,
			Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: namePattern, Message: "most characters are allowed; not / : % ? # + or control characters"}}},
		{Key: "type", Label: "Type", Type: plugin.FieldSelect, Required: true, Default: "document", Options: []plugin.Option{
			{Label: "Document", Value: "document"},
			{Label: "Edge", Value: "edge"},
		}},
		{Key: "number_of_shards", Label: "Shards", Type: plugin.FieldStepper, Default: 1, Step: 1,
			Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 1024}},
			Help:       "Cluster deployments only; single servers ignore this."},
		{Key: "replication_factor", Label: "Replication factor", Type: plugin.FieldStepper, Default: 1, Step: 1,
			Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 10}}},
		{Key: "wait_for_sync", Label: "Wait for sync", Type: plugin.FieldToggle,
			Help: "Acknowledge writes only after they are on disk."},
	}}}}
}

func collectionCreate(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Name              string `json:"name" validate:"required"`
		Type              string `json:"type"`
		NumberOfShards    int    `json:"number_of_shards"`
		ReplicationFactor int    `json:"replication_factor"`
		WaitForSync       bool   `json:"wait_for_sync"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name, err := safeName(req.Name, "collection")
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(name, "_") {
		return nil, fmt.Errorf("%w: system collections cannot be created here", plugin.ErrForbidden)
	}
	database := databaseParam(rc, s)
	db, err := s.database(rc.Ctx, database)
	if err != nil {
		return nil, err
	}
	kind := driver.CollectionTypeDocument
	if strings.EqualFold(req.Type, "edge") {
		kind = driver.CollectionTypeEdge
	}
	props := &driver.CreateCollectionPropertiesV2{Type: &kind, WaitForSync: &req.WaitForSync}
	if req.NumberOfShards > 0 {
		props.NumberOfShards = &req.NumberOfShards
	}
	if req.ReplicationFactor > 0 {
		factor := driver.ReplicationFactor(req.ReplicationFactor)
		props.ReplicationFactor = &factor
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	if _, err := db.CreateCollectionV2(ctx, name, props); err != nil {
		return nil, arangoErr(err)
	}
	return row{"ok": true, "name": name, "database": database}, nil
}

func collectionDrop(rc *plugin.RequestContext) (any, error) {
	s, database, name, err := collectionScope(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	if strings.HasPrefix(name, "_") {
		return nil, fmt.Errorf("%w: system collections cannot be dropped", plugin.ErrForbidden)
	}
	col, err := s.collection(rc.Ctx, database, name)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	if err := col.Remove(ctx); err != nil {
		return nil, arangoErr(err)
	}
	return actionResult{OK: true}, nil
}

func collectionTruncate(rc *plugin.RequestContext) (any, error) {
	s, database, name, err := collectionScope(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	col, err := s.collection(rc.Ctx, database, name)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	if err := col.Truncate(ctx); err != nil {
		return nil, arangoErr(err)
	}
	return actionResult{OK: true}, nil
}

func collectionShards(rc *plugin.RequestContext) (any, error) {
	s, database, name, err := collectionScope(rc)
	if err != nil {
		return nil, err
	}
	col, err := s.collection(rc.Ctx, database, name)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	shards, err := col.Shards(ctx, true)
	if err != nil {
		// A single server answers the shards API with 501; present the collection
		// as one implicit shard instead of failing the tab. Any other failure is
		// a real error and must surface.
		if mapped := arangoErr(err); !errors.Is(mapped, plugin.ErrNotSupported) {
			return nil, mapped
		}
		return pageRows(rc, []row{{
			"shard": name, "leader": "single", "followers": "", "replicas": 1, "state": "single-server",
		}})
	}
	shardIDs := make([]string, 0, len(shards.Shards))
	for id := range shards.Shards {
		shardIDs = append(shardIDs, string(id))
	}
	sort.Strings(shardIDs)
	rows := make([]row, 0, len(shards.Shards))
	for _, shard := range shardIDs {
		servers := shards.Shards[driver.ShardID(shard)]
		leader, followers := "", make([]string, 0, len(servers))
		for i, server := range servers {
			if i == 0 {
				leader = string(server)
				continue
			}
			followers = append(followers, string(server))
		}
		state := "replicated"
		switch {
		case len(servers) == 0:
			state = "unassigned"
		case len(servers) == 1:
			state = "unreplicated"
		}
		rows = append(rows, row{
			"shard": shard, "leader": leader, "followers": joinStrings(followers),
			"replicas": len(servers), "state": state,
		})
	}
	return pageRows(rc, rows)
}

// collectionSchemaRead returns the collection's JSON-schema validator for the
// code editor; an empty object means validation is not configured.
func collectionSchemaRead(rc *plugin.RequestContext) (any, error) {
	s, database, name, err := collectionScope(rc)
	if err != nil {
		return nil, err
	}
	col, err := s.collection(rc.Ctx, database, name)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	props, err := col.Properties(ctx)
	if err != nil {
		return nil, arangoErr(err)
	}
	if props.Schema == nil {
		return editorContent(map[string]any{"rule": map[string]any{}, "level": "none", "message": ""})
	}
	return editorContent(props.Schema)
}

func collectionSchemaUpdate(rc *plugin.RequestContext) (any, error) {
	s, database, name, err := collectionScope(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	body, err := parseEditorBody(rc, "schema")
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", plugin.ErrInvalidInput, err)
	}
	var schema driver.CollectionSchemaOptions
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, fmt.Errorf("%w: schema must have rule, level and message fields", plugin.ErrInvalidInput)
	}
	col, err := s.collection(rc.Ctx, database, name)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	if err := col.SetPropertiesV2(ctx, driver.SetCollectionPropertiesOptionsV2{Schema: &schema}); err != nil {
		return nil, arangoErr(err)
	}
	props, err := col.Properties(ctx)
	if err != nil {
		return nil, arangoErr(err)
	}
	if props.Schema == nil {
		return editorContent(map[string]any{"rule": map[string]any{}, "level": "none", "message": ""})
	}
	return editorContent(props.Schema)
}

func indexesList(rc *plugin.RequestContext) (any, error) {
	s, database, name, err := collectionScope(rc)
	if err != nil {
		return nil, err
	}
	col, err := s.collection(rc.Ctx, database, name)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	indexes, err := col.Indexes(ctx)
	if err != nil {
		return nil, arangoErr(err)
	}
	rows := make([]row, 0, len(indexes))
	for _, index := range indexes {
		item := row{
			"name":        index.Name,
			"type":        string(index.Type),
			"id":          index.ID,
			"unique":      boolOrFalse(index.Unique),
			"sparse":      boolOrFalse(index.Sparse),
			"fields":      "",
			"selectivity": 0.0,
			"system":      index.Type == driver.PrimaryIndexType || index.Type == driver.EdgeIndexType,
		}
		if index.RegularIndex != nil {
			item["fields"] = joinStrings(index.RegularIndex.Fields)
			// The server reports selectivity as a 0..1 fraction; the grid's percent
			// column renders the value verbatim with a % sign.
			item["selectivity"] = index.RegularIndex.SelectivityEstimate * 100
			if index.RegularIndex.ExpireAfter != nil {
				item["expire_after"] = *index.RegularIndex.ExpireAfter
			}
		}
		if index.InvertedIndex != nil {
			names := make([]string, 0, len(index.InvertedIndex.Fields))
			for _, field := range index.InvertedIndex.Fields {
				names = append(names, field.Name)
			}
			item["fields"] = joinStrings(names)
		}
		rows = append(rows, item)
	}
	return pageRows(rc, rows)
}

func indexCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Index", Fields: []plugin.Field{
		{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true,
			Validators: []plugin.Validator{{Type: plugin.ValidatorRegex, Value: namePattern, Message: "most characters are allowed; not / : % ? # + or control characters"}}},
		{Key: "type", Label: "Type", Type: plugin.FieldSelect, Required: true, Default: "persistent", Options: []plugin.Option{
			{Label: "Persistent", Value: "persistent"},
			{Label: "Geo", Value: "geo"},
			{Label: "TTL", Value: "ttl"},
		}},
		{Key: "fields", Label: "Attributes", Type: plugin.FieldText, Required: true, Placeholder: "email, profile.city"},
		{Key: "unique", Label: "Unique", Type: plugin.FieldToggle,
			VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "type", Op: plugin.OpEq, Value: "persistent"}}}},
		{Key: "sparse", Label: "Sparse", Type: plugin.FieldToggle,
			VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "type", Op: plugin.OpEq, Value: "persistent"}}}},
		{Key: "expire_after", Label: "Expire after (seconds)", Type: plugin.FieldNumber, Default: 3600,
			Validators:  []plugin.Validator{{Type: plugin.ValidatorMin, Value: 0}},
			VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "type", Op: plugin.OpEq, Value: "ttl"}}}},
		{Key: "geo_json", Label: "GeoJSON ordering", Type: plugin.FieldToggle,
			VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "type", Op: plugin.OpEq, Value: "geo"}}}},
	}}}}
}

func indexCreate(rc *plugin.RequestContext) (any, error) {
	s, database, name, err := collectionScope(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Name        string `json:"name" validate:"required"`
		Type        string `json:"type"`
		Fields      string `json:"fields" validate:"required"`
		Unique      bool   `json:"unique"`
		Sparse      bool   `json:"sparse"`
		ExpireAfter int    `json:"expire_after"`
		GeoJSON     bool   `json:"geo_json"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	indexName, err := safeName(req.Name, "index")
	if err != nil {
		return nil, err
	}
	fields, err := attributePaths(req.Fields)
	if err != nil {
		return nil, err
	}
	col, err := s.collection(rc.Ctx, database, name)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	switch strings.ToLower(strings.TrimSpace(req.Type)) {
	case "", "persistent":
		_, _, err = col.EnsurePersistentIndex(ctx, fields, &driver.CreatePersistentIndexOptions{
			Name: indexName, Unique: &req.Unique, Sparse: &req.Sparse,
		})
	case "geo":
		_, _, err = col.EnsureGeoIndex(ctx, fields, &driver.CreateGeoIndexOptions{Name: indexName, GeoJSON: &req.GeoJSON})
	case "ttl":
		if len(fields) != 1 {
			return nil, fmt.Errorf("%w: a TTL index indexes exactly one attribute", plugin.ErrInvalidInput)
		}
		if req.ExpireAfter < 0 {
			return nil, fmt.Errorf("%w: expire after must not be negative", plugin.ErrInvalidInput)
		}
		_, _, err = col.EnsureTTLIndex(ctx, fields, req.ExpireAfter, &driver.CreateTTLIndexOptions{Name: indexName})
	default:
		return nil, fmt.Errorf("%w: unsupported index type %q", plugin.ErrInvalidInput, req.Type)
	}
	if err != nil {
		return nil, arangoErr(err)
	}
	return row{"ok": true, "name": indexName}, nil
}

func indexDelete(rc *plugin.RequestContext) (any, error) {
	s, database, name, err := collectionScope(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	index, err := safeName(paramOrQuery(rc, "index"), "index")
	if err != nil {
		return nil, err
	}
	col, err := s.collection(rc.Ctx, database, name)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	if err := col.DeleteIndex(ctx, index); err != nil {
		return nil, arangoErr(err)
	}
	return actionResult{OK: true}, nil
}

// attributePaths parses a comma separated attribute list, allowing the dotted
// paths ArangoDB indexes support (`profile.city`, `tags[*]`).
func attributePaths(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		path := strings.TrimSpace(part)
		if path == "" {
			continue
		}
		for _, segment := range strings.Split(strings.TrimSuffix(path, "[*]"), ".") {
			if !attributeName(strings.TrimSuffix(segment, "[*]")) {
				return nil, fmt.Errorf("%w: invalid attribute path %q", plugin.ErrInvalidInput, path)
			}
		}
		out = append(out, path)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: at least one attribute is required", plugin.ErrInvalidInput)
	}
	return out, nil
}

func collectionTypeName(kind driver.CollectionType) string {
	if kind == driver.CollectionTypeEdge {
		return "edge"
	}
	return "document"
}

func collectionStatus(text string, status driver.CollectionStatus) string {
	if text != "" {
		return strings.ToLower(text)
	}
	switch status {
	case driver.CollectionStatusLoaded:
		return "loaded"
	case driver.CollectionStatusUnloaded:
		return "unloaded"
	case driver.CollectionStatusLoading:
		return "loading"
	case driver.CollectionStatusUnloading:
		return "unloading"
	case driver.CollectionStatusDeleted:
		return "deleted"
	case driver.CollectionStatusNewBorn:
		return "new"
	default:
		return "unknown"
	}
}

func documentsSize(stats map[string]any) int64 {
	figures := asMap(stats["figures"])
	return numberOf(figures, "documentsSize")
}

func indexesSize(stats map[string]any) int64 {
	figures := asMap(stats["figures"])
	indexes := asMap(figures["indexes"])
	return numberOf(indexes, "size")
}

func numberOf(fields map[string]any, key string) int64 {
	switch v := fields[key].(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case json.Number:
		n, _ := v.Int64()
		return n
	default:
		return 0
	}
}

func ensureWritable(s *Session) error {
	if s.opts.ReadOnly {
		return fmt.Errorf("%w: connection is read-only", plugin.ErrForbidden)
	}
	return nil
}
