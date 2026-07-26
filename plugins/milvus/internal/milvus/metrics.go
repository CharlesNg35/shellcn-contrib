package milvus

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/milvus-io/milvus/client/v2/milvusclient"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	metricsInterval  = 5 * time.Second
	watchInterval    = 5 * time.Second
	progressInterval = 2 * time.Second
	overviewScanCap  = 200
	// clusterInterval is slower than the per-collection metrics because one
	// cluster frame walks every collection in the database.
	clusterInterval = 15 * time.Second
)

func overview(rc *plugin.RequestContext) (any, error) {
	s, c, err := clientOf(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()

	out := row{
		"name":      "Milvus",
		"address":   s.opts.Host + ":" + strconv.Itoa(s.opts.Port),
		"database":  databaseName(rc),
		"read_only": s.opts.ReadOnly,
		"ref":       plugin.ResourceIdentity{Kind: "server", Name: "Milvus", UID: "server"},
	}
	if version, err := c.GetServerVersion(ctx, milvusclient.NewGetServerVersionOption()); err == nil {
		out["version"] = version
	}
	stats := clusterStats(ctx, c)
	for key, value := range stats {
		out[key] = value
	}
	if names, err := c.ListResourceGroups(ctx, milvusclient.NewListResourceGroupsOption()); err == nil {
		out["resource_groups"] = names
	}
	return out, nil
}

// clusterStats collects the counters shared by the overview detail panel and
// the overview metrics stream.
func clusterStats(ctx context.Context, c *milvusclient.Client) row {
	out := row{}
	if names, err := c.ListDatabase(ctx, milvusclient.NewListDatabaseOption()); err == nil {
		out["databases"] = len(names)
	}
	if names, err := c.ListUsers(ctx, milvusclient.NewListUserOption()); err == nil {
		out["users"] = len(names)
	}
	if names, err := c.ListRoles(ctx, milvusclient.NewListRoleOption()); err == nil {
		out["roles"] = len(names)
	}
	names, err := c.ListCollections(ctx, milvusclient.NewListCollectionOption())
	if err != nil {
		return out
	}
	out["collections"] = len(names)
	if len(names) > overviewScanCap {
		names = names[:overviewScanCap]
	}
	loaded, entities, partitions, indexes := 0, int64(0), 0, 0
	for _, name := range names {
		if state, err := c.GetLoadState(ctx, milvusclient.NewGetLoadStateOption(name)); err == nil && loadStateName(state.State) == "loaded" {
			loaded++
		}
		if stats, err := c.GetCollectionStats(ctx, milvusclient.NewGetCollectionStatsOption(name)); err == nil {
			if count, err := strconv.ParseInt(stats["row_count"], 10, 64); err == nil {
				entities += count
			}
		}
		if parts, err := c.ListPartitions(ctx, milvusclient.NewListPartitionOption(name)); err == nil {
			partitions += len(parts)
		}
		if idx, err := c.ListIndexes(ctx, milvusclient.NewListIndexOption(name)); err == nil {
			indexes += len(idx)
		}
	}
	out["loaded_collections"] = loaded
	out["entities"] = entities
	out["partitions"] = partitions
	out["indexes"] = indexes
	if total, ok := out["collections"].(int); ok && total > 0 {
		out["loaded_pct"] = round1(float64(loaded) / float64(total) * 100)
	} else {
		out["loaded_pct"] = float64(0)
	}
	return out
}

func overviewMetrics(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, c, err := clientOf(rc)
	if err != nil {
		return err
	}
	return streamFrames(rc, client, clusterInterval, func(ctx context.Context) (any, bool) {
		return clusterStats(ctx, c), true
	}, s.opts.Timeout)
}

func collectionMetrics(rc *plugin.RequestContext, client plugin.ClientStream) error {
	collection, err := collectionParam(rc)
	if err != nil {
		return err
	}
	s, c, err := clientOf(rc)
	if err != nil {
		return err
	}
	return streamFrames(rc, client, metricsInterval, func(ctx context.Context) (any, bool) {
		return collectionFrame(ctx, c, collection), true
	}, s.opts.Timeout)
}

func collectionFrame(ctx context.Context, c *milvusclient.Client, collection string) row {
	frame := row{}
	if state, err := c.GetLoadState(ctx, milvusclient.NewGetLoadStateOption(collection)); err == nil {
		frame["loadProgress"] = state.Progress
		frame["loadState"] = loadStateName(state.State)
	}
	if stats, err := c.GetCollectionStats(ctx, milvusclient.NewGetCollectionStatsOption(collection)); err == nil {
		if count, err := strconv.ParseInt(stats["row_count"], 10, 64); err == nil {
			frame["entities"] = count
		}
	}
	if segments, err := c.GetPersistentSegmentInfo(ctx, milvusclient.NewGetPersistentSegmentInfoOption(collection)); err == nil {
		var rows, flushedRows, flushed int64
		for _, seg := range segments {
			rows += seg.NumRows
			if seg.Flushed() {
				flushed++
				flushedRows += seg.NumRows
			}
		}
		frame["segments"] = len(segments)
		frame["flushedSegments"] = flushed
		frame["segmentRows"] = rows
		frame["flushedRows"] = flushedRows
		if rows > 0 {
			frame["flushedPct"] = round1(float64(flushedRows) / float64(rows) * 100)
		} else {
			frame["flushedPct"] = float64(0)
		}
	}
	if names, err := c.ListIndexes(ctx, milvusclient.NewListIndexOption(collection)); err == nil {
		var indexed, total int64
		for _, name := range names {
			desc, err := c.DescribeIndex(ctx, milvusclient.NewDescribeIndexOption(collection, name))
			if err != nil {
				continue
			}
			indexed += desc.IndexedRows
			total += desc.TotalRows
		}
		frame["indexes"] = len(names)
		frame["indexedRows"] = indexed
		frame["indexTotalRows"] = total
		if total > 0 {
			frame["indexedPct"] = round1(float64(indexed) / float64(total) * 100)
		} else {
			frame["indexedPct"] = float64(0)
		}
	}
	if names, err := c.ListPartitions(ctx, milvusclient.NewListPartitionOption(collection)); err == nil {
		frame["partitions"] = len(names)
	}
	return frame
}

func watchCollection(rc *plugin.RequestContext, client plugin.ClientStream) error {
	collection, err := collectionParam(rc)
	if err != nil {
		return err
	}
	s, c, err := clientOf(rc)
	if err != nil {
		return err
	}
	database := databaseName(rc)
	return streamFrames(rc, client, watchInterval, func(ctx context.Context) (any, bool) {
		detail, err := collectionDetail(ctx, c, database, collection)
		if err != nil {
			return nil, true
		}
		return detail, true
	}, s.opts.Timeout)
}

func indexProgress(rc *plugin.RequestContext, client plugin.ClientStream) error {
	collection, err := collectionParam(rc)
	if err != nil {
		return err
	}
	name, err := identifier("index", rc.Param("index"))
	if err != nil {
		return err
	}
	s, c, err := clientOf(rc)
	if err != nil {
		return err
	}
	startedAt := time.Now().UTC()
	return streamFrames(rc, client, progressInterval, func(ctx context.Context) (any, bool) {
		desc, err := c.DescribeIndex(ctx, milvusclient.NewDescribeIndexOption(collection, name))
		if err != nil {
			return row{
				"status": "failed", "message": mapError(err).Error(),
				"startedAt": startedAt, "finishedAt": time.Now().UTC(),
			}, false
		}
		state := indexStateName(desc.State)
		frame := row{
			"status":    taskStatus(state),
			"message":   "Index " + name + " is " + state,
			"current":   desc.IndexedRows,
			"total":     desc.TotalRows,
			"pending":   desc.PendingIndexRows,
			"startedAt": startedAt,
		}
		if desc.TotalRows > 0 {
			frame["progress"] = round1(float64(desc.IndexedRows) / float64(desc.TotalRows) * 100)
		} else {
			frame["progress"] = float64(0)
		}
		if state == "finished" || state == "failed" {
			frame["finishedAt"] = time.Now().UTC()
			if state == "finished" {
				frame["progress"] = float64(100)
			}
			return frame, false
		}
		return frame, true
	}, s.opts.Timeout)
}

func taskStatus(state string) string {
	switch state {
	case "finished":
		return "completed"
	case "failed":
		return "failed"
	case "inprogress", "unissued", "retry":
		return "running"
	default:
		return "running"
	}
}

// streamFrames writes an immediate first frame and then ticks until the client
// disconnects. A nil frame is skipped so one failed poll does not drop the
// panel; build reports false to close the stream.
func streamFrames(rc *plugin.RequestContext, client plugin.ClientStream, interval time.Duration, build func(context.Context) (any, bool), timeout time.Duration) error {
	enc := json.NewEncoder(client)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		frameCtx, cancel := context.WithTimeout(rc.Ctx, timeout)
		frame, more := build(frameCtx)
		cancel()
		if frame != nil {
			if err := enc.Encode(frame); err != nil {
				return nil
			}
		}
		if !more {
			return nil
		}
		select {
		case <-client.Context().Done():
			return nil
		case <-rc.Ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
