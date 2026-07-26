package milvus

import (
	"fmt"
	"sort"

	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

func treeCollections(rc *plugin.RequestContext) (any, error) {
	names, s, c, err := collectionNames(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	database := databaseName(rc)
	if len(names) > treeNodeLimit {
		names = names[:treeNodeLimit]
	}
	nodes := make([]plugin.TreeNode, 0, len(names))
	for _, name := range names {
		ref := plugin.ResourceIdentity{Kind: "collection", Namespace: database, Name: name, UID: database + "/" + name}
		state := "not_loaded"
		if got, err := c.GetLoadState(ctx, milvusclient.NewGetLoadStateOption(name)); err == nil {
			state = loadStateName(got.State)
		}
		nodes = append(nodes, plugin.TreeNode{
			Key:   "collection:" + database + "/" + name,
			Label: name,
			Icon:  icon("table-2"),
			Ref:   &ref,
			Data:  map[string]any{"state": state, "read_only": s.opts.ReadOnly},
			ChildrenSource: &plugin.DataSource{
				RouteID: rid("tree.partitions"),
				Params:  map[string]string{"database": database, "collection": name},
			},
		})
	}
	total := len(nodes)
	return plugin.Page[plugin.TreeNode]{Items: nodes, Total: &total}, nil
}

func treePartitions(rc *plugin.RequestContext) (any, error) {
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
	names, err := c.ListPartitions(ctx, milvusclient.NewListPartitionOption(collection))
	if err != nil {
		return nil, mapError(err)
	}
	sort.Strings(names)
	if len(names) > treeNodeLimit {
		names = names[:treeNodeLimit]
	}
	database := databaseName(rc)
	nodes := make([]plugin.TreeNode, 0, len(names))
	for _, name := range names {
		ref := plugin.ResourceIdentity{
			Kind: "partition", Scope: collection, Namespace: database, Name: name,
			UID: database + "/" + collection + "/" + name,
		}
		nodes = append(nodes, plugin.TreeNode{
			Key:   "partition:" + ref.UID,
			Label: name,
			Icon:  icon("split"),
			Ref:   &ref,
			Leaf:  true,
		})
	}
	total := len(nodes)
	return plugin.Page[plugin.TreeNode]{Items: nodes, Total: &total}, nil
}

func collectionGraph(rc *plugin.RequestContext) (any, error) {
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

	nodes := []graphNode{{
		ID: "collection:" + collection, Label: collection, Group: "collection",
		Summary:    fmt.Sprintf("%d field(s)", len(coll.Schema.Fields)),
		Properties: map[string]any{"id": coll.ID, "shards": coll.ShardNum, "consistency": consistencyName(coll.ConsistencyLevel)},
	}}
	edges := []graphEdge{}
	for _, f := range coll.Schema.Fields {
		id := "field:" + f.Name
		group := "field"
		if f.DataType.IsVectorType() {
			group = "vector"
		}
		if f.PrimaryKey {
			group = "primary"
		}
		nodes = append(nodes, graphNode{
			ID: id, Label: f.Name, Group: group, Summary: fieldTypeName(f.DataType),
			Fields:     []graphField{{Name: "type", Type: fieldTypeName(f.DataType), Key: f.Name}},
			Properties: fieldRow(f),
		})
		edges = append(edges, graphEdge{Source: "collection:" + collection, Target: id, Label: "field"})
	}
	owners := indexOwners(ctx, c, collection, coll.Schema)
	if names, err := c.ListIndexes(ctx, milvusclient.NewListIndexOption(collection)); err == nil {
		for _, name := range names {
			desc, err := c.DescribeIndex(ctx, milvusclient.NewDescribeIndexOption(collection, name))
			if err != nil {
				continue
			}
			id := "index:" + name
			field := owners[name]
			if field == "" {
				field = name
			}
			nodes = append(nodes, graphNode{
				ID: id, Label: name, Group: "index", Summary: desc.Params()[index.IndexTypeKey],
				Properties: map[string]any{"state": indexStateName(desc.State), "indexed_rows": desc.IndexedRows, "total_rows": desc.TotalRows},
			})
			edges = append(edges, graphEdge{Source: "field:" + field, Target: id, Label: "indexed by"})
		}
	}
	if names, err := c.ListPartitions(ctx, milvusclient.NewListPartitionOption(collection)); err == nil {
		for _, name := range names {
			id := "partition:" + name
			nodes = append(nodes, graphNode{ID: id, Label: name, Group: "partition"})
			edges = append(edges, graphEdge{Source: "collection:" + collection, Target: id, Label: "partition"})
		}
	}
	if names, err := c.ListAliases(ctx, milvusclient.NewListAliasesOption(collection)); err == nil {
		for _, name := range names {
			id := "alias:" + name
			nodes = append(nodes, graphNode{ID: id, Label: name, Group: "alias"})
			edges = append(edges, graphEdge{Source: id, Target: "collection:" + collection, Label: "alias of"})
		}
	}
	return graphPayload{Nodes: nodes, Edges: edges}, nil
}
