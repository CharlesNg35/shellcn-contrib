package weaviate

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/fault"
	"github.com/weaviate/weaviate/entities/models"
)

var (
	classNamePattern    = regexp.MustCompile(`^[A-Z][A-Za-z0-9_]{0,229}$`)
	propertyNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,229}$`)
	simpleNamePattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	uuidPattern         = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
)

func Routes() []plugin.Route {
	return []plugin.Route{
		{ID: rid("overview"), Method: plugin.MethodGet, Path: "/overview", Permission: perm("server.read"), Risk: plugin.RiskSafe, AuditEvent: rid("overview"), Handle: overview},
		{ID: rid("modules.list"), Method: plugin.MethodGet, Path: "/modules", Permission: perm("server.read"), Risk: plugin.RiskSafe, AuditEvent: rid("modules.list"), Handle: listModules},
		{ID: rid("metrics"), Method: plugin.MethodWS, Path: "/metrics", Permission: perm("metrics.read"), Risk: plugin.RiskSafe, AuditEvent: rid("metrics"), Stream: metricsStream},
		{ID: rid("activity.list"), Method: plugin.MethodGet, Path: "/activity", Permission: perm("activity.read"), Risk: plugin.RiskSafe, AuditEvent: rid("activity.list"), Handle: listActivity},
		{ID: rid("resource.watch"), Method: plugin.MethodWS, Path: "/watch/{kind}", Permission: perm("resources.watch"), Risk: plugin.RiskSafe, AuditEvent: rid("resource.watch"), Stream: watchResources},

		{ID: rid("collections.tree"), Method: plugin.MethodGet, Path: "/tree/collections", Permission: perm("collections.read"), Risk: plugin.RiskSafe, AuditEvent: rid("collections.tree"), Handle: treeCollections},
		{ID: rid("collections.list"), Method: plugin.MethodGet, Path: "/collections", Permission: perm("collections.read"), Risk: plugin.RiskSafe, AuditEvent: rid("collections.list"), Handle: listCollections},
		{ID: rid("collection.read"), Method: plugin.MethodGet, Path: "/collections/{collection}", Permission: perm("collections.read"), Risk: plugin.RiskSafe, AuditEvent: rid("collection.read"), Handle: readCollection},
		{ID: rid("collection.definition"), Method: plugin.MethodGet, Path: "/collections/{collection}/definition", Permission: perm("collections.read"), Risk: plugin.RiskSafe, AuditEvent: rid("collection.definition"), Handle: readCollectionDefinition},
		{ID: rid("collection.create"), Method: plugin.MethodPost, Path: "/collections", Permission: perm("collections.write"), Risk: plugin.RiskWrite, AuditEvent: rid("collection.create"), Input: collectionCreateSchema(), Handle: createCollection},
		{ID: rid("collection.update"), Method: plugin.MethodPut, Path: "/collections/{collection}", Permission: perm("collections.write"), Risk: plugin.RiskWrite, AuditEvent: rid("collection.update"), Handle: updateCollection},
		{ID: rid("collection.delete"), Method: plugin.MethodDelete, Path: "/collections/{collection}", Permission: perm("collections.delete"), Risk: plugin.RiskDestructive, AuditEvent: rid("collection.delete"), Handle: deleteCollection},
		{ID: rid("collection.columns"), Method: plugin.MethodGet, Path: "/collections/{collection}/columns", Permission: perm("collections.read"), Risk: plugin.RiskSafe, AuditEvent: rid("collection.columns"), Handle: listCollectionColumns},
		{ID: rid("properties.list"), Method: plugin.MethodGet, Path: "/collections/{collection}/properties", Permission: perm("collections.read"), Risk: plugin.RiskSafe, AuditEvent: rid("properties.list"), Handle: listProperties},
		{ID: rid("property.create"), Method: plugin.MethodPost, Path: "/collections/{collection}/properties", Permission: perm("collections.write"), Risk: plugin.RiskWrite, AuditEvent: rid("property.create"), Input: propertyCreateSchema(), Handle: createProperty},

		{ID: rid("objects.list"), Method: plugin.MethodGet, Path: "/collections/{collection}/objects", Permission: perm("objects.read"), Risk: plugin.RiskSafe, AuditEvent: rid("objects.list"), Handle: listObjects},
		{ID: rid("object.read"), Method: plugin.MethodGet, Path: "/collections/{collection}/objects/{id}", Permission: perm("objects.read"), Risk: plugin.RiskSafe, AuditEvent: rid("object.read"), Handle: readObject},
		{ID: rid("object.document"), Method: plugin.MethodGet, Path: "/collections/{collection}/objects/{id}/document", Permission: perm("objects.read"), Risk: plugin.RiskSafe, AuditEvent: rid("object.document"), Handle: readObjectDocument},
		{ID: rid("object.create"), Method: plugin.MethodPost, Path: "/collections/{collection}/objects", Permission: perm("objects.write"), Risk: plugin.RiskWrite, AuditEvent: rid("object.create"), Handle: createObject},
		{ID: rid("object.update"), Method: plugin.MethodPut, Path: "/collections/{collection}/objects/{id}", Permission: perm("objects.write"), Risk: plugin.RiskWrite, AuditEvent: rid("object.update"), Handle: updateObject},
		{ID: rid("object.delete"), Method: plugin.MethodDelete, Path: "/collections/{collection}/objects/{id}", Permission: perm("objects.delete"), Risk: plugin.RiskDestructive, AuditEvent: rid("object.delete"), Handle: deleteObject},
		{ID: rid("objects.delete"), Method: plugin.MethodDelete, Path: "/collections/{collection}/objects", Permission: perm("objects.delete"), Risk: plugin.RiskDestructive, AuditEvent: rid("objects.delete"), Input: batchDeleteSchema(), Handle: deleteObjectsByFilter},

		{ID: rid("object.references"), Method: plugin.MethodGet, Path: "/collections/{collection}/objects/{id}/references", Permission: perm("objects.read"), Risk: plugin.RiskSafe, AuditEvent: rid("object.references"), Handle: objectReferenceGraph},
		{ID: rid("object.references.expand"), Method: plugin.MethodGet, Path: "/collections/{collection}/objects/{id}/references/expand", Permission: perm("objects.read"), Risk: plugin.RiskSafe, AuditEvent: rid("object.references.expand"), Handle: expandObjectReferenceGraph},
		{ID: rid("reference.create"), Method: plugin.MethodPost, Path: "/collections/{collection}/objects/{id}/references", Permission: perm("objects.write"), Risk: plugin.RiskWrite, AuditEvent: rid("reference.create"), Input: referenceSchema(), Handle: createReference},
		{ID: rid("reference.delete"), Method: plugin.MethodDelete, Path: "/collections/{collection}/objects/{id}/references", Permission: perm("objects.delete"), Risk: plugin.RiskDestructive, AuditEvent: rid("reference.delete"), Input: referenceSchema(), Handle: deleteReference},

		{ID: rid("schema.graph"), Method: plugin.MethodGet, Path: "/schema/graph", Permission: perm("collections.read"), Risk: plugin.RiskSafe, AuditEvent: rid("schema.graph"), Handle: schemaGraph},
		{ID: rid("schema.graph.expand"), Method: plugin.MethodGet, Path: "/schema/graph/expand", Permission: perm("collections.read"), Risk: plugin.RiskSafe, AuditEvent: rid("schema.graph.expand"), Handle: expandSchemaGraph},

		{ID: rid("graphql"), Method: plugin.MethodWS, Path: "/graphql", Permission: perm("query.execute"), Risk: plugin.RiskSafe, AuditEvent: rid("graphql"), Stream: graphqlStream},
		{ID: rid("graphql.complete"), Method: plugin.MethodGet, Path: "/graphql/complete", Permission: perm("query.execute"), Risk: plugin.RiskSafe, AuditEvent: rid("graphql.complete"), Handle: completeQuery},
		{ID: rid("search"), Method: plugin.MethodWS, Path: "/collections/{collection}/search", Permission: perm("query.execute"), Risk: plugin.RiskSafe, AuditEvent: rid("search"), Stream: searchStream},
		{ID: rid("embedding"), Method: plugin.MethodWS, Path: "/collections/{collection}/embedding", Permission: perm("objects.read"), Risk: plugin.RiskSafe, AuditEvent: rid("embedding"), Stream: embeddingStream},

		{ID: rid("shards.list"), Method: plugin.MethodGet, Path: "/collections/{collection}/shards", Permission: perm("shards.read"), Risk: plugin.RiskSafe, AuditEvent: rid("shards.list"), Handle: listShards},
		{ID: rid("shard.update"), Method: plugin.MethodPut, Path: "/collections/{collection}/shards/{shard}", Permission: perm("shards.write"), Risk: plugin.RiskWrite, AuditEvent: rid("shard.update"), Input: shardUpdateSchema(), Handle: updateShard},
		{ID: rid("nodes.list"), Method: plugin.MethodGet, Path: "/nodes", Permission: perm("nodes.read"), Risk: plugin.RiskSafe, AuditEvent: rid("nodes.list"), Handle: listNodes},
		{ID: rid("node.read"), Method: plugin.MethodGet, Path: "/nodes/{node}", Permission: perm("nodes.read"), Risk: plugin.RiskSafe, AuditEvent: rid("node.read"), Handle: readNode},
		{ID: rid("node.shards"), Method: plugin.MethodGet, Path: "/nodes/{node}/shards", Permission: perm("nodes.read"), Risk: plugin.RiskSafe, AuditEvent: rid("node.shards"), Handle: listNodeShards},

		{ID: rid("tenants.list"), Method: plugin.MethodGet, Path: "/collections/{collection}/tenants", Permission: perm("tenants.read"), Risk: plugin.RiskSafe, AuditEvent: rid("tenants.list"), Handle: listTenants},
		{ID: rid("tenant.create"), Method: plugin.MethodPost, Path: "/collections/{collection}/tenants", Permission: perm("tenants.write"), Risk: plugin.RiskWrite, AuditEvent: rid("tenant.create"), Input: tenantCreateSchema(), Handle: createTenant},
		{ID: rid("tenant.update"), Method: plugin.MethodPut, Path: "/collections/{collection}/tenants/{tenant}", Permission: perm("tenants.write"), Risk: plugin.RiskWrite, AuditEvent: rid("tenant.update"), Input: tenantUpdateSchema(), Handle: updateTenant},
		{ID: rid("tenant.delete"), Method: plugin.MethodDelete, Path: "/collections/{collection}/tenants/{tenant}", Permission: perm("tenants.delete"), Risk: plugin.RiskDestructive, AuditEvent: rid("tenant.delete"), Handle: deleteTenant},

		{ID: rid("aliases.list"), Method: plugin.MethodGet, Path: "/aliases", Permission: perm("aliases.read"), Risk: plugin.RiskSafe, AuditEvent: rid("aliases.list"), Handle: listAliases},
		{ID: rid("alias.read"), Method: plugin.MethodGet, Path: "/aliases/{alias}", Permission: perm("aliases.read"), Risk: plugin.RiskSafe, AuditEvent: rid("alias.read"), Handle: readAlias},
		{ID: rid("alias.create"), Method: plugin.MethodPost, Path: "/aliases", Permission: perm("aliases.write"), Risk: plugin.RiskWrite, AuditEvent: rid("alias.create"), Input: aliasCreateSchema(), Handle: createAlias},
		{ID: rid("alias.update"), Method: plugin.MethodPut, Path: "/aliases/{alias}", Permission: perm("aliases.write"), Risk: plugin.RiskWrite, AuditEvent: rid("alias.update"), Input: aliasUpdateSchema(), Handle: updateAlias},
		{ID: rid("alias.delete"), Method: plugin.MethodDelete, Path: "/aliases/{alias}", Permission: perm("aliases.delete"), Risk: plugin.RiskDestructive, AuditEvent: rid("alias.delete"), Handle: deleteAlias},

		{ID: rid("backups.list"), Method: plugin.MethodGet, Path: "/backups", Permission: perm("backups.read"), Risk: plugin.RiskSafe, AuditEvent: rid("backups.list"), Handle: listBackups},
		{ID: rid("backup.read"), Method: plugin.MethodGet, Path: "/backups/{backend}/{backup}", Permission: perm("backups.read"), Risk: plugin.RiskSafe, AuditEvent: rid("backup.read"), Handle: readBackup},
		{ID: rid("backup.create"), Method: plugin.MethodPost, Path: "/backups", Permission: perm("backups.write"), Risk: plugin.RiskWrite, AuditEvent: rid("backup.create"), Input: backupCreateSchema(), Handle: createBackup},
		{ID: rid("backup.restore"), Method: plugin.MethodPost, Path: "/backups/{backend}/{backup}/restore", Permission: perm("backups.restore"), Risk: plugin.RiskDestructive, AuditEvent: rid("backup.restore"), Handle: restoreBackup},
		{ID: rid("backup.cancel"), Method: plugin.MethodDelete, Path: "/backups/{backend}/{backup}", Permission: perm("backups.write"), Risk: plugin.RiskDestructive, AuditEvent: rid("backup.cancel"), Handle: cancelBackup},

		{ID: rid("queries.list"), Method: plugin.MethodGet, Path: "/queries", Permission: perm("query.read"), Risk: plugin.RiskSafe, AuditEvent: rid("queries.list"), Handle: listSavedQueries},
		{ID: rid("query.read"), Method: plugin.MethodGet, Path: "/queries/{id}", Permission: perm("query.read"), Risk: plugin.RiskSafe, AuditEvent: rid("query.read"), Handle: readSavedQuery},
		{ID: rid("query.save"), Method: plugin.MethodPost, Path: "/queries", Permission: perm("query.write"), Risk: plugin.RiskWrite, AuditEvent: rid("query.save"), Input: savedQuerySchema(), Handle: saveQuery},
		{ID: rid("query.update"), Method: plugin.MethodPut, Path: "/queries/{id}", Permission: perm("query.write"), Risk: plugin.RiskWrite, AuditEvent: rid("query.update"), Handle: updateSavedQuery},
		{ID: rid("query.delete"), Method: plugin.MethodDelete, Path: "/queries/{id}", Permission: perm("query.delete"), Risk: plugin.RiskDestructive, AuditEvent: rid("query.delete"), Handle: deleteSavedQuery},
	}
}

func perm(suffix string) string { return protocolName + "." + suffix }

func collectionParam(rc *plugin.RequestContext) (string, error) {
	return validName(rc.Param("collection"), classNamePattern, "collection")
}

func objectIDParam(rc *plugin.RequestContext) (string, error) {
	id := strings.TrimSpace(rc.Param("id"))
	if !uuidPattern.MatchString(id) {
		return "", fmt.Errorf("%w: object id must be a UUID", plugin.ErrInvalidInput)
	}
	return id, nil
}

func validName(raw string, pattern *regexp.Regexp, label string) (string, error) {
	name := strings.TrimSpace(raw)
	if !pattern.MatchString(name) {
		return "", fmt.Errorf("%w: %s name %q is not valid", plugin.ErrInvalidInput, label, raw)
	}
	return name, nil
}

func overview(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	meta, err := s.client.misc.MetaGetter().Do(rc.Ctx)
	if err != nil {
		return nil, mapError(err)
	}
	out := row{
		"endpoint":    s.opts.Endpoint,
		"version":     meta.Version,
		"hostname":    meta.Hostname,
		"readOnly":    s.opts.ReadOnly,
		"consistency": s.opts.Consistency,
		"modules":     moduleNames(meta.Modules),
		"moduleCount": len(moduleNames(meta.Modules)),
	}
	if dump, err := s.schema(rc.Ctx); err == nil {
		out["collections"] = len(dump.Classes)
		out["vectorizers"] = distinctVectorizers(dump.Classes)
	}
	if stats, err := clusterStats(rc); err == nil {
		out["nodes"] = stats.Nodes
		out["healthyNodes"] = stats.Healthy
		out["nodesPct"] = stats.HealthyPct()
		out["shards"] = stats.Shards
		out["objects"] = stats.Objects
		out["batchQueue"] = stats.BatchQueue
		out["rate"] = stats.Rate
	}
	return out, nil
}

func listModules(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	meta, err := s.client.misc.MetaGetter().Do(rc.Ctx)
	if err != nil {
		return nil, mapError(err)
	}
	modules, _ := meta.Modules.(map[string]any)
	rows := make([]row, 0, len(modules))
	for name, raw := range modules {
		item := row{"name": name, "config": raw}
		if cfg, ok := raw.(map[string]any); ok {
			item["title"] = text(cfg["name"])
			item["documentation"] = text(cfg["documentationHref"])
		}
		item["kind"] = moduleKind(name)
		rows = append(rows, item)
	}
	sort.Slice(rows, func(i, j int) bool { return text(rows[i]["name"]) < text(rows[j]["name"]) })
	return broker.PageRows(rc, rows)
}

func moduleKind(name string) string {
	switch {
	case strings.HasPrefix(name, "text2vec"), strings.HasPrefix(name, "multi2vec"), strings.HasPrefix(name, "img2vec"), strings.HasPrefix(name, "ref2vec"):
		return "vectorizer"
	case strings.HasPrefix(name, "generative"):
		return "generative"
	case strings.HasPrefix(name, "reranker"):
		return "reranker"
	case strings.HasPrefix(name, "backup"):
		return "backup"
	case strings.HasPrefix(name, "qna"), strings.HasPrefix(name, "sum"), strings.HasPrefix(name, "ner"):
		return "reader"
	default:
		return "other"
	}
}

func moduleNames(raw any) []string {
	modules, _ := raw.(map[string]any)
	names := make([]string, 0, len(modules))
	for name := range modules {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func distinctVectorizers(classes []*models.Class) []string {
	seen := map[string]bool{}
	for _, class := range classes {
		for _, name := range classVectorizers(class) {
			seen[name] = true
		}
	}
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func classVectorizers(class *models.Class) []string {
	if class == nil {
		return nil
	}
	names := map[string]bool{}
	if class.Vectorizer != "" {
		names[class.Vectorizer] = true
	}
	for _, vector := range class.VectorConfig {
		if cfg, ok := vector.Vectorizer.(map[string]any); ok {
			for name := range cfg {
				names[name] = true
			}
		}
	}
	out := make([]string, 0, len(names))
	for name := range names {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func treeCollections(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	dump, err := s.schema(rc.Ctx)
	if err != nil {
		return nil, err
	}
	classes := sortedClasses(dump.Classes)
	nodes := make([]plugin.TreeNode, 0, len(classes))
	for _, class := range classes {
		ref := plugin.ResourceIdentity{Kind: "collection", Name: class.Class, UID: class.Class}
		nodes = append(nodes, plugin.TreeNode{
			Key:   "collection:" + class.Class,
			Label: class.Class,
			Icon:  icon("layers"),
			Ref:   &ref,
			Leaf:  true,
			Data:  map[string]any{"multiTenancy": multiTenancyEnabled(class), "vectorizer": primaryVectorizer(class)},
		})
	}
	total := len(nodes)
	return plugin.Page[plugin.TreeNode]{Items: nodes, Total: &total}, nil
}

func sortedClasses(classes []*models.Class) []*models.Class {
	out := make([]*models.Class, 0, len(classes))
	for _, class := range classes {
		if class != nil {
			out = append(out, class)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Class < out[j].Class })
	return out
}

func listCollections(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	dump, err := s.schema(rc.Ctx)
	if err != nil {
		return nil, err
	}
	stats, _ := clusterStats(rc)
	rows := make([]row, 0, len(dump.Classes))
	for _, class := range sortedClasses(dump.Classes) {
		item := row{
			"ref":             plugin.ResourceIdentity{Kind: "collection", Name: class.Class, UID: class.Class},
			"name":            class.Class,
			"description":     class.Description,
			"vectorizer":      primaryVectorizer(class),
			"vectorIndexType": class.VectorIndexType,
			"distance":        vectorIndexString(class, "distance"),
			"properties":      len(class.Properties),
			"references":      countReferences(class),
			"multiTenancy":    multiTenancyEnabled(class),
			"replication":     replicationFactor(class),
		}
		if stats != nil {
			perClass := stats.Classes[class.Class]
			item["objects"] = perClass.Objects
			item["shards"] = perClass.Shards
			item["status"] = perClass.Status
		}
		rows = append(rows, item)
	}
	return broker.PageRows(rc, rows)
}

func readCollection(rc *plugin.RequestContext) (any, error) {
	class, err := loadClass(rc)
	if err != nil {
		return nil, err
	}
	out := row{
		"name":                 class.Class,
		"description":          class.Description,
		"vectorizer":           primaryVectorizer(class),
		"vectorizers":          classVectorizers(class),
		"namedVectors":         namedVectorNames(class),
		"vectorIndexType":      class.VectorIndexType,
		"distance":             vectorIndexString(class, "distance"),
		"ef":                   vectorIndexNumber(class, "ef"),
		"efConstruction":       vectorIndexNumber(class, "efConstruction"),
		"maxConnections":       vectorIndexNumber(class, "maxConnections"),
		"quantization":         quantizationKind(class),
		"properties":           len(class.Properties),
		"references":           countReferences(class),
		"replication":          replicationFactor(class),
		"asyncReplication":     asyncReplication(class),
		"multiTenancy":         multiTenancyEnabled(class),
		"autoTenantCreation":   class.MultiTenancyConfig != nil && class.MultiTenancyConfig.AutoTenantCreation,
		"autoTenantActivation": class.MultiTenancyConfig != nil && class.MultiTenancyConfig.AutoTenantActivation,
		"shardingDesired":      shardingNumber(class, "desiredCount"),
		"moduleConfig":         class.ModuleConfig,
		"invertedIndex":        class.InvertedIndexConfig,
	}
	if stats, err := clusterStats(rc); err == nil {
		perClass := stats.Classes[class.Class]
		out["objects"] = perClass.Objects
		out["shards"] = perClass.Shards
		out["status"] = perClass.Status
	}
	return out, nil
}

func readCollectionDefinition(rc *plugin.RequestContext) (any, error) {
	class, err := loadClass(rc)
	if err != nil {
		return nil, err
	}
	return classDocument(class)
}

func classDocument(class *models.Class) (any, error) {
	data, err := json.MarshalIndent(class, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	return row{"content": string(data), "name": class.Class}, nil
}

func loadClass(rc *plugin.RequestContext) (*models.Class, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	name, err := collectionParam(rc)
	if err != nil {
		return nil, err
	}
	class, err := s.client.schema.ClassGetter().WithClassName(name).Do(rc.Ctx)
	if err != nil {
		return nil, mapError(err)
	}
	if class == nil {
		return nil, fmt.Errorf("%w: collection %q", plugin.ErrNotFound, name)
	}
	return class, nil
}

func createCollection(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Name              string  `json:"name" validate:"required"`
		Description       string  `json:"description"`
		Vectorizer        string  `json:"vectorizer"`
		VectorIndexType   string  `json:"vector_index_type"`
		Distance          string  `json:"distance"`
		ReplicationFactor int     `json:"replication_factor"`
		AsyncReplication  bool    `json:"async_replication"`
		MultiTenancy      bool    `json:"multi_tenancy"`
		AutoTenant        bool    `json:"auto_tenant_creation"`
		InvertedBM25K1    float64 `json:"bm25_k1"`
		InvertedBM25B     float64 `json:"bm25_b"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name, err := validName(req.Name, classNamePattern, "collection")
	if err != nil {
		return nil, fmt.Errorf("%w: collection names must start with an uppercase letter", plugin.ErrInvalidInput)
	}
	class := &models.Class{
		Class:       name,
		Description: strings.TrimSpace(req.Description),
		Vectorizer:  defaultString(req.Vectorizer, "none"),
	}
	if req.VectorIndexType != "" {
		class.VectorIndexType = req.VectorIndexType
	}
	if req.Distance != "" {
		class.VectorIndexConfig = map[string]any{"distance": req.Distance}
	}
	if req.ReplicationFactor > 0 || req.AsyncReplication {
		factor := int64(req.ReplicationFactor)
		if factor <= 0 {
			factor = 1
		}
		class.ReplicationConfig = &models.ReplicationConfig{Factor: factor, AsyncEnabled: req.AsyncReplication}
	}
	if req.MultiTenancy {
		class.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true, AutoTenantCreation: req.AutoTenant}
	}
	if req.InvertedBM25K1 > 0 || req.InvertedBM25B > 0 {
		class.InvertedIndexConfig = &models.InvertedIndexConfig{Bm25: &models.BM25Config{
			K1: float32(defaultFloat(req.InvertedBM25K1, 1.2)),
			B:  float32(defaultFloat(req.InvertedBM25B, 0.75)),
		}}
	}
	if err := s.client.schema.ClassCreator().WithClass(class).Do(rc.Ctx); err != nil {
		return nil, mapError(err)
	}
	s.forgetSchema()
	record(rc, "schema", plugin.SeveritySuccess, "Collection created", name)
	return row{"ok": true, "name": name}, nil
}

func updateCollection(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	name, err := collectionParam(rc)
	if err != nil {
		return nil, err
	}
	raw, err := documentBody(rc, "class")
	if err != nil {
		return nil, err
	}
	var class models.Class
	if err := json.Unmarshal(raw, &class); err != nil {
		return nil, fmt.Errorf("%w: collection definition must be a JSON object: %v", plugin.ErrInvalidInput, err)
	}
	if class.Class == "" {
		class.Class = name
	}
	if class.Class != name {
		return nil, fmt.Errorf("%w: collection definition names %q but the route targets %q", plugin.ErrInvalidInput, class.Class, name)
	}
	if err := s.client.schema.ClassUpdater().WithClass(&class).Do(rc.Ctx); err != nil {
		return nil, mapError(err)
	}
	s.forgetSchema()
	record(rc, "schema", plugin.SeverityInfo, "Collection definition updated", name)
	updated, err := s.client.schema.ClassGetter().WithClassName(name).Do(rc.Ctx)
	if err != nil {
		return nil, mapError(err)
	}
	return classDocument(updated)
}

func deleteCollection(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	name, err := collectionParam(rc)
	if err != nil {
		return nil, err
	}
	if err := s.client.schema.ClassDeleter().WithClassName(name).Do(rc.Ctx); err != nil {
		return nil, mapError(err)
	}
	s.forgetSchema()
	record(rc, "schema", plugin.SeverityDanger, "Collection deleted", name)
	return row{"ok": true, "name": name}, nil
}

func listProperties(rc *plugin.RequestContext) (any, error) {
	class, err := loadClass(rc)
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(class.Properties))
	for _, property := range class.Properties {
		if property == nil {
			continue
		}
		rows = append(rows, row{
			"name":              property.Name,
			"dataType":          strings.Join(property.DataType, ", "),
			"kind":              propertyKind(property),
			"description":       property.Description,
			"tokenization":      property.Tokenization,
			"indexFilterable":   boolValue(property.IndexFilterable, true),
			"indexSearchable":   boolValue(property.IndexSearchable, true),
			"indexRangeFilters": boolValue(property.IndexRangeFilters, false),
			"nestedProperties":  len(property.NestedProperties),
		})
	}
	return broker.PageRows(rc, rows)
}

func createProperty(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	name, err := collectionParam(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		Name            string `json:"name" validate:"required"`
		DataType        string `json:"data_type" validate:"required"`
		Description     string `json:"description"`
		Tokenization    string `json:"tokenization"`
		IndexFilterable *bool  `json:"index_filterable"`
		IndexSearchable *bool  `json:"index_searchable"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	propertyName, err := validName(req.Name, propertyNamePattern, "property")
	if err != nil {
		return nil, err
	}
	dataType := strings.TrimSpace(req.DataType)
	if dataType == "" {
		return nil, fmt.Errorf("%w: data type is required", plugin.ErrInvalidInput)
	}
	property := &models.Property{
		Name:            propertyName,
		DataType:        []string{dataType},
		Description:     strings.TrimSpace(req.Description),
		IndexFilterable: req.IndexFilterable,
		IndexSearchable: req.IndexSearchable,
	}
	if req.Tokenization != "" && !isReferenceType(dataType) {
		property.Tokenization = req.Tokenization
	}
	if err := s.client.schema.PropertyCreator().WithClassName(name).WithProperty(property).Do(rc.Ctx); err != nil {
		return nil, mapError(err)
	}
	s.forgetSchema()
	record(rc, "schema", plugin.SeverityInfo, "Property added", name+"."+propertyName)
	return row{"ok": true, "name": propertyName, "collection": name}, nil
}

func propertyKind(property *models.Property) string {
	if len(property.DataType) == 0 {
		return "unknown"
	}
	if isReferenceType(property.DataType[0]) {
		return "reference"
	}
	return "primitive"
}

// isReferenceType reports whether a Weaviate data type names another collection.
// Cross-reference targets are the only capitalised data types.
func isReferenceType(dataType string) bool {
	trimmed := strings.TrimSuffix(strings.TrimSpace(dataType), "[]")
	return classNamePattern.MatchString(trimmed)
}

func countReferences(class *models.Class) int {
	count := 0
	for _, property := range class.Properties {
		if property != nil && propertyKind(property) == "reference" {
			count++
		}
	}
	return count
}

func multiTenancyEnabled(class *models.Class) bool {
	return class != nil && class.MultiTenancyConfig != nil && class.MultiTenancyConfig.Enabled
}

func replicationFactor(class *models.Class) int64 {
	if class == nil || class.ReplicationConfig == nil || class.ReplicationConfig.Factor == 0 {
		return 1
	}
	return class.ReplicationConfig.Factor
}

func asyncReplication(class *models.Class) bool {
	return class != nil && class.ReplicationConfig != nil && class.ReplicationConfig.AsyncEnabled
}

func primaryVectorizer(class *models.Class) string {
	if class == nil {
		return ""
	}
	if class.Vectorizer != "" {
		return class.Vectorizer
	}
	if names := classVectorizers(class); len(names) > 0 {
		return names[0]
	}
	return "none"
}

func namedVectorNames(class *models.Class) []string {
	names := make([]string, 0, len(class.VectorConfig))
	for name := range class.VectorConfig {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func quantizationKind(class *models.Class) string {
	cfg, ok := class.VectorIndexConfig.(map[string]any)
	if !ok {
		return "none"
	}
	for _, key := range []string{"pq", "bq", "sq", "rq"} {
		if entry, ok := cfg[key].(map[string]any); ok {
			if enabled, ok := entry["enabled"].(bool); ok && enabled {
				return strings.ToUpper(key)
			}
		}
	}
	return "none"
}

func vectorIndexString(class *models.Class, key string) string {
	cfg, ok := class.VectorIndexConfig.(map[string]any)
	if !ok {
		return ""
	}
	return text(cfg[key])
}

func vectorIndexNumber(class *models.Class, key string) float64 {
	cfg, ok := class.VectorIndexConfig.(map[string]any)
	if !ok {
		return 0
	}
	value, _ := numberOf(cfg[key])
	return value
}

func shardingNumber(class *models.Class, key string) float64 {
	cfg, ok := class.ShardingConfig.(map[string]any)
	if !ok {
		return 0
	}
	value, _ := numberOf(cfg[key])
	return value
}

func boolValue(ptr *bool, fallback bool) bool {
	if ptr == nil {
		return fallback
	}
	return *ptr
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func defaultFloat(value, fallback float64) float64 {
	if value <= 0 {
		return fallback
	}
	return value
}

func text(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(t)
	default:
		return fmt.Sprint(v)
	}
}

func numberOf(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case float32:
		return float64(t), true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	case json.Number:
		value, err := t.Float64()
		return value, err == nil
	case string:
		value, err := strconv.ParseFloat(t, 64)
		return value, err == nil
	default:
		return 0, false
	}
}

func jsonText(v any) string {
	if v == nil {
		return ""
	}
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(data)
}

// documentBody accepts the shapes ShellCN panels send: a code editor save with
// SaveBodyKey ({"<key>": {...}}), a raw text save ({"content": "..."}), or the
// bare document itself.
func documentBody(rc *plugin.RequestContext, key string) ([]byte, error) {
	body := rc.Body()
	if len(strings.TrimSpace(string(body))) == 0 {
		return nil, fmt.Errorf("%w: request body is empty", plugin.ErrInvalidInput)
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("%w: body must be a JSON object", plugin.ErrInvalidInput)
	}
	if raw, ok := envelope[key]; ok {
		return raw, nil
	}
	if raw, ok := envelope["content"]; ok {
		var content string
		if err := json.Unmarshal(raw, &content); err != nil {
			return raw, nil
		}
		return []byte(content), nil
	}
	return body, nil
}

// weaviateFault digs out the innermost client error. The GraphQL helper re-wraps
// its status-code error as a derived error with StatusCode -1, so the outermost
// value alone would turn every 401/403/404 into an unavailable server.
func weaviateFault(err error) *fault.WeaviateClientError {
	var werr *fault.WeaviateClientError
	if !errors.As(err, &werr) {
		return nil
	}
	for werr.DerivedFromError != nil {
		var inner *fault.WeaviateClientError
		if !errors.As(werr.DerivedFromError, &inner) {
			break
		}
		werr = inner
	}
	return werr
}

func weaviateMessage(err *fault.WeaviateClientError) string {
	if err.DerivedFromError != nil {
		return err.DerivedFromError.Error()
	}
	message := strings.TrimSpace(err.Msg)
	var body struct {
		Error []struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal([]byte(message), &body) == nil && len(body.Error) > 0 {
		parts := make([]string, 0, len(body.Error))
		for _, item := range body.Error {
			if item.Message != "" {
				parts = append(parts, item.Message)
			}
		}
		if len(parts) > 0 {
			return strings.Join(parts, "; ")
		}
	}
	if message == "" {
		return "Weaviate request failed"
	}
	return message
}

func unixMillis(value any) string {
	millis, ok := numberOf(value)
	if !ok || millis <= 0 {
		return ""
	}
	return time.UnixMilli(int64(millis)).UTC().Format(time.RFC3339)
}
