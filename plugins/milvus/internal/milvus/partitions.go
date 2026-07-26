package milvus

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/milvus-io/milvus/client/v2/milvusclient"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

func listPartitions(rc *plugin.RequestContext) (any, error) {
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
	database := databaseName(rc)
	rows := make([]row, 0, len(names))
	for _, name := range names {
		rows = append(rows, partitionRow(ctx, c, database, collection, name))
	}
	return broker.PageRows(rc, rows)
}

func partitionRow(ctx context.Context, c *milvusclient.Client, database, collection, name string) row {
	item := row{
		"name":       name,
		"collection": collection,
		"state":      "not_loaded",
		"ref": plugin.ResourceIdentity{
			Kind: "partition", Scope: collection, Namespace: database, Name: name,
			UID: database + "/" + collection + "/" + name,
		},
	}
	if stats, err := c.GetPartitionStats(ctx, milvusclient.NewGetPartitionStatsOption(collection, name)); err == nil {
		if count, err := strconv.ParseInt(stats["row_count"], 10, 64); err == nil {
			item["entities"] = count
		}
	}
	if state, err := c.GetLoadState(ctx, milvusclient.NewGetLoadStateOption(collection, name)); err == nil {
		item["state"] = loadStateName(state.State)
		item["load_progress"] = state.Progress
	}
	return item
}

func readPartition(rc *plugin.RequestContext) (any, error) {
	collection, err := collectionParam(rc)
	if err != nil {
		return nil, err
	}
	name, err := identifier("partition", rc.Param("partition"))
	if err != nil {
		return nil, err
	}
	s, c, err := clientOf(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	has, err := c.HasPartition(ctx, milvusclient.NewHasPartitionOption(collection, name))
	if err != nil {
		return nil, mapError(err)
	}
	if !has {
		return nil, fmt.Errorf("%w: partition %q", plugin.ErrNotFound, name)
	}
	return partitionRow(ctx, c, databaseName(rc), collection, name), nil
}

func createPartition(rc *plugin.RequestContext) (any, error) {
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
	name, err := identifier("partition", req.Name)
	if err != nil {
		return nil, err
	}
	s, c, err := writableClient(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	err = c.CreatePartition(ctx, milvusclient.NewCreatePartitionOption(collection, name))
	s.activity.record("partition.create", collection+"/"+name, "Created partition", err)
	return ok(row{"ok": err == nil, "name": name}, err)
}

func loadPartition(rc *plugin.RequestContext) (any, error) {
	collection, name, s, c, err := partitionTarget(rc, true)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	task, err := c.LoadPartitions(ctx, milvusclient.NewLoadPartitionsOption(collection, name))
	if err == nil {
		err = task.Await(ctx)
	}
	s.activity.record("partition.load", collection+"/"+name, "Loaded partition", err)
	return ok(row{"ok": err == nil, "name": name}, err)
}

func releasePartition(rc *plugin.RequestContext) (any, error) {
	collection, name, s, c, err := partitionTarget(rc, true)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	err = c.ReleasePartitions(ctx, milvusclient.NewReleasePartitionsOptions(collection, name))
	s.activity.record("partition.release", collection+"/"+name, "Released partition", err)
	return ok(row{"ok": err == nil, "name": name}, err)
}

func dropPartition(rc *plugin.RequestContext) (any, error) {
	collection, name, s, c, err := partitionTarget(rc, true)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	err = c.DropPartition(ctx, milvusclient.NewDropPartitionOption(collection, name))
	s.activity.record("partition.drop", collection+"/"+name, "Dropped partition", err)
	return ok(row{"ok": err == nil, "name": name}, err)
}

func partitionTarget(rc *plugin.RequestContext, write bool) (string, string, *Session, *milvusclient.Client, error) {
	collection, err := collectionParam(rc)
	if err != nil {
		return "", "", nil, nil, err
	}
	name, err := identifier("partition", rc.Param("partition"))
	if err != nil {
		return "", "", nil, nil, err
	}
	var s *Session
	var c *milvusclient.Client
	if write {
		s, c, err = writableClient(rc)
	} else {
		s, c, err = clientOf(rc)
	}
	if err != nil {
		return "", "", nil, nil, err
	}
	return collection, name, s, c, nil
}
