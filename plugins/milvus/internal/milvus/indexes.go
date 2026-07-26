package milvus

import (
	"fmt"
	"sort"
	"strings"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

func listIndexes(rc *plugin.RequestContext) (any, error) {
	collection, err := collectionParam(rc)
	if err != nil {
		return nil, err
	}
	s, c, err := clientOf(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	coll, err := c.DescribeCollection(ctx, milvusclient.NewDescribeCollectionOption(collection))
	if err != nil {
		return nil, mapError(err)
	}
	owners := indexOwners(ctx, c, collection, coll.Schema)
	names, err := c.ListIndexes(ctx, milvusclient.NewListIndexOption(collection))
	if err != nil {
		return nil, mapError(err)
	}
	sort.Strings(names)
	database := databaseName(rc)
	rows := make([]row, 0, len(names))
	for _, name := range names {
		desc, err := c.DescribeIndex(ctx, milvusclient.NewDescribeIndexOption(collection, name))
		if err != nil {
			continue
		}
		rows = append(rows, indexRow(database, collection, name, owners[name], desc))
	}
	return broker.PageRows(rc, rows)
}

func indexRow(database, collection, name, fieldName string, desc milvusclient.IndexDescription) row {
	params := desc.Params()
	if fieldName == "" {
		fieldName = name
	}
	item := row{
		"name":         name,
		"collection":   collection,
		"index_type":   params[index.IndexTypeKey],
		"metric_type":  params[index.MetricTypeKey],
		"field_name":   fieldName,
		"state":        indexStateName(desc.State),
		"total_rows":   desc.TotalRows,
		"indexed_rows": desc.IndexedRows,
		"pending_rows": desc.PendingIndexRows,
		"params":       params,
		"ref": plugin.ResourceIdentity{
			Kind: "index", Scope: collection, Namespace: database, Name: name,
			UID: database + "/" + collection + "/" + name,
		},
	}
	if desc.TotalRows > 0 {
		item["progress"] = round1(float64(desc.IndexedRows) / float64(desc.TotalRows) * 100)
	} else {
		item["progress"] = float64(0)
	}
	return item
}

func readIndex(rc *plugin.RequestContext) (any, error) {
	collection, err := collectionParam(rc)
	if err != nil {
		return nil, err
	}
	name, err := identifier("index", rc.Param("index"))
	if err != nil {
		return nil, err
	}
	s, c, err := clientOf(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	desc, err := c.DescribeIndex(ctx, milvusclient.NewDescribeIndexOption(collection, name))
	if err != nil {
		return nil, mapError(err)
	}
	fieldName := name
	if coll, err := c.DescribeCollection(ctx, milvusclient.NewDescribeCollectionOption(collection)); err == nil {
		if owner := indexOwners(ctx, c, collection, coll.Schema)[name]; owner != "" {
			fieldName = owner
		}
	}
	return indexRow(databaseName(rc), collection, name, fieldName, desc), nil
}

func createIndex(rc *plugin.RequestContext) (any, error) {
	collection, err := collectionParam(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		FieldName      string `json:"field_name" validate:"required"`
		IndexName      string `json:"index_name"`
		IndexType      string `json:"index_type" validate:"required"`
		MetricType     string `json:"metric_type"`
		Nlist          int    `json:"nlist"`
		M              int    `json:"m"`
		EfConstruction int    `json:"ef_construction"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	field, err := identifier("field", req.FieldName)
	if err != nil {
		return nil, err
	}
	indexName := strings.TrimSpace(req.IndexName)
	if indexName != "" {
		if indexName, err = identifier("index", indexName); err != nil {
			return nil, err
		}
	}
	idx, err := buildIndex(req.IndexType, req.MetricType, req.Nlist, req.M, req.EfConstruction)
	if err != nil {
		return nil, err
	}
	s, c, err := writableClient(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	opt := milvusclient.NewCreateIndexOption(collection, field, idx)
	if indexName != "" {
		opt = opt.WithIndexName(indexName)
	}
	// The build runs server-side; the index build-progress stream reports it.
	if _, err := c.CreateIndex(ctx, opt); err != nil {
		s.activity.record("index.create", collection+"."+field, "Index creation failed", err)
		return nil, mapError(err)
	}
	s.activity.record("index.create", collection+"."+field, "Index build started ("+req.IndexType+")", nil)
	if indexName == "" {
		indexName = field
	}
	return row{"ok": true, "collection": collection, "index": indexName, "field": field}, nil
}

func buildIndex(indexType, metricType string, nlist, m, efConstruction int) (index.Index, error) {
	metric := entity.MetricType(strings.ToUpper(strings.TrimSpace(metricType)))
	if metric == "" {
		metric = entity.COSINE
	}
	switch index.IndexType(strings.TrimSpace(indexType)) {
	case index.AUTOINDEX:
		return index.NewAutoIndex(metric), nil
	case index.Flat:
		return index.NewFlatIndex(metric), nil
	case index.IvfFlat:
		return index.NewIvfFlatIndex(metric, positive(nlist, 128)), nil
	case index.IvfSQ8:
		return index.NewIvfSQ8Index(metric, positive(nlist, 128)), nil
	case index.HNSW:
		return index.NewHNSWIndex(metric, positive(m, 16), positive(efConstruction, 200)), nil
	case index.SCANN:
		return index.NewSCANNIndex(metric, positive(nlist, 128), true), nil
	case index.DISKANN:
		return index.NewDiskANNIndex(metric), nil
	case index.SparseInverted:
		return index.NewSparseInvertedIndex(metric, 0), nil
	case index.Inverted:
		return index.NewInvertedIndex(), nil
	case index.BITMAP:
		return index.NewBitmapIndex(), nil
	case index.Sorted:
		return index.NewSortedIndex(), nil
	case index.Trie:
		return index.NewTrieIndex(), nil
	default:
		return nil, fmt.Errorf("%w: unsupported index type %q", plugin.ErrInvalidInput, indexType)
	}
}

func dropIndex(rc *plugin.RequestContext) (any, error) {
	collection, err := collectionParam(rc)
	if err != nil {
		return nil, err
	}
	name, err := identifier("index", rc.Param("index"))
	if err != nil {
		return nil, err
	}
	s, c, err := writableClient(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	err = c.DropIndex(ctx, milvusclient.NewDropIndexOption(collection, name))
	s.activity.record("index.drop", collection+"."+name, "Dropped index", err)
	return ok(row{"ok": err == nil, "index": name}, err)
}

func listSegments(rc *plugin.RequestContext) (any, error) {
	collection, err := collectionParam(rc)
	if err != nil {
		return nil, err
	}
	s, c, err := clientOf(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	segments, err := c.GetPersistentSegmentInfo(ctx, milvusclient.NewGetPersistentSegmentInfoOption(collection))
	if err != nil {
		return nil, mapError(err)
	}
	rows := make([]row, 0, len(segments))
	for _, seg := range segments {
		rows = append(rows, row{
			"id":            seg.ID,
			"collection_id": seg.CollectionID,
			"partition_id":  seg.ParititionID,
			"rows":          seg.NumRows,
			"state":         segmentStateName(seg.State),
			"flushed":       seg.Flushed(),
		})
	}
	return broker.PageRows(rc, rows)
}

func listAliases(rc *plugin.RequestContext) (any, error) {
	collection, err := collectionParam(rc)
	if err != nil {
		return nil, err
	}
	s, c, err := clientOf(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	names, err := c.ListAliases(ctx, milvusclient.NewListAliasesOption(collection))
	if err != nil {
		return nil, mapError(err)
	}
	sort.Strings(names)
	rows := make([]row, 0, len(names))
	for _, name := range names {
		item := row{"alias": name, "collection": collection}
		if alias, err := c.DescribeAlias(ctx, milvusclient.NewDescribeAliasOption(name)); err == nil {
			item["collection"] = alias.CollectionName
			item["database"] = alias.DbName
		}
		rows = append(rows, item)
	}
	return broker.PageRows(rc, rows)
}

func createAlias(rc *plugin.RequestContext) (any, error) {
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
	alias, err := identifier("alias", req.Name)
	if err != nil {
		return nil, err
	}
	s, c, err := writableClient(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	err = c.CreateAlias(ctx, milvusclient.NewCreateAliasOption(collection, alias))
	s.activity.record("alias.create", alias, "Alias now points at "+collection, err)
	return ok(row{"ok": err == nil, "alias": alias}, err)
}

func dropAlias(rc *plugin.RequestContext) (any, error) {
	alias, err := identifier("alias", rc.Param("alias"))
	if err != nil {
		return nil, err
	}
	s, c, err := writableClient(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	err = c.DropAlias(ctx, milvusclient.NewDropAliasOption(alias))
	s.activity.record("alias.drop", alias, "Dropped alias", err)
	return ok(row{"ok": err == nil, "alias": alias}, err)
}

func listResourceGroups(rc *plugin.RequestContext) (any, error) {
	s, c, err := clientOf(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	names, err := c.ListResourceGroups(ctx, milvusclient.NewListResourceGroupsOption())
	if err != nil {
		return nil, mapError(err)
	}
	sort.Strings(names)
	rows := make([]row, 0, len(names))
	for _, name := range names {
		item := row{"name": name}
		if group, err := c.DescribeResourceGroup(ctx, milvusclient.NewDescribeResourceGroupOption(name)); err == nil {
			item["capacity"] = group.Capacity
			item["available_nodes"] = group.NumAvailableNode
			item["nodes"] = len(group.Nodes)
		}
		rows = append(rows, item)
	}
	return broker.PageRows(rc, rows)
}
