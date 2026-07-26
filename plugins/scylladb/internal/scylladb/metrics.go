package scylladb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type tableEstimate struct {
	Partitions int64
	Bytes      int64
}

type estimateTotals struct {
	Partitions int64
	Bytes      int64
	Tables     map[string]tableEstimate
}

// keyspaceEstimateTotals aggregates system.size_estimates, ScyllaDB's per-range
// partition-count and mean-size sampling, into per-table and per-keyspace totals.
func keyspaceEstimateTotals(ctx context.Context, s *Session, keyspace string) estimateTotals {
	totals := estimateTotals{Tables: map[string]tableEstimate{}}
	rows, err := queryRows(ctx, s, `
SELECT table_name, mean_partition_size, partitions_count
FROM system.size_estimates
WHERE keyspace_name = ?`, []any{keyspace})
	if err != nil {
		return totals
	}
	for _, r := range rows {
		name := fmt.Sprint(r["table_name"])
		partitions := int64Value(r["partitions_count"])
		bytes := partitions * int64Value(r["mean_partition_size"])
		entry := totals.Tables[name]
		entry.Partitions += partitions
		entry.Bytes += bytes
		totals.Tables[name] = entry
		totals.Partitions += partitions
		totals.Bytes += bytes
	}
	return totals
}

func tableEstimates(rc *plugin.RequestContext) (any, error) {
	keyspace, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	s, err := scyllaSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT table_name, range_start, range_end, mean_partition_size, partitions_count
FROM system.size_estimates
WHERE keyspace_name = ?`, []any{keyspace})
	if err != nil {
		return nil, err
	}
	facts, err := clusterFacts(rc.Ctx, s)
	if err != nil {
		return nil, err
	}
	shards, msb := localShardLayout(facts)
	out := make([]row, 0, len(rows))
	for _, r := range rows {
		if fmt.Sprint(r["table_name"]) != table {
			continue
		}
		partitions := int64Value(r["partitions_count"])
		mean := int64Value(r["mean_partition_size"])
		end := int64Value(r["range_end"])
		out = append(out, row{
			"range_start":         fmt.Sprint(r["range_start"]),
			"range_end":           fmt.Sprint(r["range_end"]),
			"partitions_count":    partitions,
			"mean_partition_size": mean,
			"estimated_bytes":     partitions * mean,
			"shard":               shardOf(end, shards, msb),
		})
	}
	return pageRows(rc, out)
}

func tableLargePartitions(rc *plugin.RequestContext) (any, error) {
	keyspace, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	s, err := scyllaSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `
SELECT sstable_name, partition_size, partition_key, rows, dead_rows, range_tombstones, compaction_time
FROM system.large_partitions
WHERE keyspace_name = ? AND table_name = ?`, []any{keyspace, table})
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		r["keyspace"] = keyspace
		r["table"] = table
	}
	return pageRows(rc, rows)
}

func localShardLayout(facts []nodeFacts) (int, uint64) {
	for _, f := range facts {
		if f.Local && f.Shards > 0 {
			return f.Shards, f.MSBIgnore
		}
	}
	for _, f := range facts {
		if f.Shards > 0 {
			return f.Shards, f.MSBIgnore
		}
	}
	return 1, 0
}

// runtimeCounters flattens system.runtime_info into "group.item" keys.
func runtimeCounters(ctx context.Context, s *Session) map[string]float64 {
	out := map[string]float64{}
	rows, err := queryRows(ctx, s, `SELECT "group", item, value FROM system.runtime_info`, nil)
	if err != nil {
		return out
	}
	for _, r := range rows {
		out[fmt.Sprint(r["group"])+"."+fmt.Sprint(r["item"])] = floatValue(r["value"])
	}
	return out
}

// latencyProbe measures client round-trip latency for a statement and, less
// often, the coordinator's own duration from a server-side trace.
type latencyProbe struct {
	statement       string
	traceEvery      time.Duration
	lastTrace       time.Time
	coordinatorMS   float64
	haveCoordinator bool
}

func (p *latencyProbe) sample(ctx context.Context, s *Session) (clientMS float64, coordinatorMS float64, ok bool) {
	trace := time.Since(p.lastTrace) >= p.traceEvery
	collector := &traceCollector{}
	query := s.db.Query(p.statement).WithContext(ctx).Consistency(s.opts.Consistency).PageSize(1)
	if trace {
		query = query.Trace(collector)
	}
	start := time.Now()
	iter := query.Iter()
	scanned := map[string]any{}
	iter.MapScan(scanned)
	if err := iter.Close(); err != nil {
		return 0, p.coordinatorMS, false
	}
	clientMS = float64(time.Since(start).Microseconds()) / 1000
	if trace {
		p.lastTrace = time.Now()
		if ids := collector.IDs(); len(ids) > 0 {
			if micros, err := traceDuration(ctx, s, ids[len(ids)-1]); err == nil {
				p.coordinatorMS = float64(micros) / 1000
				p.haveCoordinator = true
			}
		}
	}
	return clientMS, p.coordinatorMS, true
}

func (p *latencyProbe) hasCoordinator() bool { return p.haveCoordinator }

// traceDuration reads the coordinator-reported duration of a trace session,
// retrying briefly because the coordinator flushes the row asynchronously.
func traceDuration(ctx context.Context, s *Session, sessionID string) (int64, error) {
	id, err := gocql.ParseUUID(sessionID)
	if err != nil {
		return 0, err
	}
	for attempt := range 5 {
		var duration int
		err := s.db.Query(`SELECT duration FROM system_traces.sessions WHERE session_id = ?`, id).WithContext(ctx).Consistency(gocql.One).Scan(&duration)
		if err == nil && duration > 0 {
			return int64(duration), nil
		}
		if err != nil && err != gocql.ErrNotFound {
			return 0, err
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(time.Duration(20*(attempt+1)) * time.Millisecond):
		}
	}
	return 0, gocql.ErrNotFound
}

type rateCounter struct {
	value float64
	at    time.Time
}

// perSecond returns the rate of change since the previous sample, or 0 on the
// first sample or after a counter reset (node restart).
func (c *rateCounter) perSecond(value float64) float64 {
	now := time.Now()
	defer func() { c.value, c.at = value, now }()
	if c.at.IsZero() {
		return 0
	}
	elapsed := now.Sub(c.at).Seconds()
	if elapsed <= 0 || value < c.value {
		return 0
	}
	return (value - c.value) / elapsed
}

func clusterMetrics(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := scyllaSession(rc)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(client)
	probe := &latencyProbe{statement: "SELECT release_version FROM system.local", traceEvery: 30 * time.Second}
	cacheRequests := &rateCounter{}
	compactionBytes := &rateCounter{}
	hits, misses := &rateCounter{}, &rateCounter{}
	return pollStream(rc, client, 5*time.Second, func(ctx context.Context) error {
		frame := map[string]any{}
		counters := runtimeCounters(ctx, s)
		memoryUsed, memoryTotal := counters["memory.used"], counters["memory.total"]
		frame["memoryUsed"] = memoryUsed
		frame["memoryTotal"] = memoryTotal
		frame["memoryPct"] = round2(percent(memoryUsed, memoryTotal))
		cacheUsed, cacheTotal := counters["cache.memory_used"], counters["cache.memory_total"]
		frame["cacheUsed"] = cacheUsed
		frame["cacheTotal"] = cacheTotal
		frame["cachePct"] = round2(percent(cacheUsed, cacheTotal))
		frame["cacheRequestsPerSec"] = round2(cacheRequests.perSecond(counters["cache.requests_total"]))
		hitRate := hits.perSecond(counters["cache.hits"])
		missRate := misses.perSecond(counters["cache.misses"])
		frame["cacheHitRate"] = round2(percent(hitRate, hitRate+missRate))

		if facts, err := clusterFacts(ctx, s); err == nil {
			up, shards, connections, storageUsed, storageTotal := 0, 0, 0, int64(0), int64(0)
			for _, f := range facts {
				if f.Up {
					up++
				}
				shards += f.Shards
				connections += f.DriverConnections
				storageUsed += f.StorageUsed
				storageTotal += f.StorageTotal
			}
			frame["nodesUp"] = up
			frame["shardsTotal"] = shards
			frame["driverConnections"] = connections
			frame["storageUsed"] = storageUsed
			frame["storageTotal"] = storageTotal
			frame["storagePct"] = round2(percent(float64(storageUsed), float64(storageTotal)))
		}
		if clients, err := clientRows(ctx, s); err == nil {
			frame["clientConnections"] = len(clients)
		}
		if events, err := compactionHistory(ctx, s, "", ""); err == nil {
			total := float64(0)
			for _, e := range events {
				total += float64(e.BytesOut)
			}
			frame["compactionBytesPerSec"] = round2(compactionBytes.perSecond(total))
		}
		if clientMS, coordinatorMS, ok := probe.sample(ctx, s); ok {
			frame["probeLatencyMs"] = round2(clientMS)
			if probe.hasCoordinator() {
				frame["coordinatorLatencyMs"] = round2(coordinatorMS)
			}
		}
		return enc.Encode(frame)
	})
}

func tableMetrics(rc *plugin.RequestContext, client plugin.ClientStream) error {
	keyspace, table, err := tableIdent(rc)
	if err != nil {
		return err
	}
	s, err := scyllaSession(rc)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(client)
	probe := &latencyProbe{
		statement:  fmt.Sprintf("SELECT * FROM %s LIMIT 1", qualified(keyspace, table)),
		traceEvery: 30 * time.Second,
	}
	compactionBytes := &rateCounter{}
	hits, misses := &rateCounter{}, &rateCounter{}
	return pollStream(rc, client, 5*time.Second, func(ctx context.Context) error {
		frame := map[string]any{}
		totals := keyspaceEstimateTotals(ctx, s, keyspace).Tables[table]
		frame["partitions"] = totals.Partitions
		frame["estimatedBytes"] = totals.Bytes
		if totals.Partitions > 0 {
			frame["meanPartitionSize"] = totals.Bytes / totals.Partitions
		} else {
			frame["meanPartitionSize"] = 0
		}
		if large, err := queryRows(ctx, s, `
SELECT partition_size FROM system.large_partitions WHERE keyspace_name = ? AND table_name = ?`, []any{keyspace, table}); err == nil {
			frame["largePartitions"] = len(large)
		}
		if events, err := compactionHistory(ctx, s, keyspace, table); err == nil {
			cutoff := time.Now().Add(-time.Hour)
			recent, bytesIn, bytesOut, total := 0, int64(0), int64(0), float64(0)
			for _, e := range events {
				total += float64(e.BytesOut)
				if e.At.After(cutoff) {
					recent++
					bytesIn += e.BytesIn
					bytesOut += e.BytesOut
				}
			}
			frame["compactionsLastHour"] = recent
			frame["compactionBytesPerSec"] = round2(compactionBytes.perSecond(total))
			frame["compactionRatio"] = round2(percent(float64(bytesOut), float64(bytesIn)))
		}
		counters := runtimeCounters(ctx, s)
		hitRate := hits.perSecond(counters["cache.hits"])
		missRate := misses.perSecond(counters["cache.misses"])
		frame["cacheHitRate"] = round2(percent(hitRate, hitRate+missRate))
		if clientMS, coordinatorMS, ok := probe.sample(ctx, s); ok {
			frame["readLatencyMs"] = round2(clientMS)
			if probe.hasCoordinator() {
				frame["coordinatorLatencyMs"] = round2(coordinatorMS)
			}
		}
		return enc.Encode(frame)
	})
}

func statementSummary(statement string) string {
	statement = strings.Join(strings.Fields(statement), " ")
	if len(statement) > 120 {
		return statement[:117] + "..."
	}
	return statement
}
