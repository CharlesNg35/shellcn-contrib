package scylladb

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

func clusterRoutes() []plugin.Route {
	return []plugin.Route{
		{ID: "scylladb.cluster.list", Method: plugin.MethodGet, Path: "/cluster", Permission: "scylladb.cluster.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.cluster.list", Handle: listCluster},
		{ID: "scylladb.cluster.overview", Method: plugin.MethodGet, Path: "/cluster/overview", Permission: "scylladb.cluster.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.cluster.overview", Handle: clusterOverview},
		{ID: "scylladb.cluster.watch", Method: plugin.MethodWS, Path: "/cluster/watch", Permission: "scylladb.cluster.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.cluster.watch", Stream: clusterWatch},
		{ID: "scylladb.cluster.metrics", Method: plugin.MethodWS, Path: "/cluster/metrics", Permission: "scylladb.cluster.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.cluster.metrics", Stream: clusterMetrics},
		{ID: "scylladb.nodes.tree", Method: plugin.MethodGet, Path: "/tree/nodes", Permission: "scylladb.nodes.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.nodes.tree", Handle: treeNodes},
		{ID: "scylladb.nodes.list", Method: plugin.MethodGet, Path: "/nodes", Permission: "scylladb.nodes.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.nodes.list", Handle: listNodes},
		{ID: "scylladb.nodes.watch", Method: plugin.MethodWS, Path: "/nodes/watch", Permission: "scylladb.nodes.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.nodes.watch", Stream: nodesWatch},
		{ID: "scylladb.node.overview", Method: plugin.MethodGet, Path: "/nodes/{address}/overview", Permission: "scylladb.nodes.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.node.overview", Handle: nodeOverview},
		{ID: "scylladb.node.runtime", Method: plugin.MethodGet, Path: "/nodes/{address}/runtime", Permission: "scylladb.nodes.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.node.runtime", Handle: nodeRuntime},
		{ID: "scylladb.clients.list", Method: plugin.MethodGet, Path: "/clients", Permission: "scylladb.nodes.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.clients.list", Handle: listClients},
		{ID: "scylladb.config.list", Method: plugin.MethodGet, Path: "/config", Permission: "scylladb.cluster.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.config.list", Handle: listConfig},
		{ID: "scylladb.compactions.list", Method: plugin.MethodGet, Path: "/compactions", Permission: "scylladb.cluster.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.compactions.list", Handle: listCompactions},
		{ID: "scylladb.compactions.watch", Method: plugin.MethodWS, Path: "/compactions/watch", Permission: "scylladb.cluster.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.compactions.watch", Stream: compactionWatch},
		{ID: "scylladb.tokenring.list", Method: plugin.MethodGet, Path: "/keyspaces/{keyspace}/token-ring", Permission: "scylladb.cluster.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.tokenring.list", Handle: listTokenRing},
		{ID: "scylladb.topology", Method: plugin.MethodWS, Path: "/topology", Permission: "scylladb.cluster.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.topology", Stream: topologyStream},
		{ID: "scylladb.table.metrics", Method: plugin.MethodWS, Path: "/tables/{keyspace}/{table}/metrics", Permission: "scylladb.tables.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.table.metrics", Stream: tableMetrics},
		{ID: "scylladb.table.estimates", Method: plugin.MethodGet, Path: "/tables/{keyspace}/{table}/estimates", Permission: "scylladb.tables.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.table.estimates", Handle: tableEstimates},
		{ID: "scylladb.table.large", Method: plugin.MethodGet, Path: "/tables/{keyspace}/{table}/large-partitions", Permission: "scylladb.tables.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.table.large", Handle: tableLargePartitions},
		{ID: "scylladb.partition.locate", Method: plugin.MethodPost, Path: "/tables/{keyspace}/{table}/locate", Permission: "scylladb.tables.data.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.partition.locate", Input: partitionLocateSchema(), Handle: locatePartition},
		{ID: "scylladb.partition.owner", Method: plugin.MethodGet, Path: "/keyspaces/{keyspace}/tokens/{token}", Permission: "scylladb.cluster.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.partition.owner", Handle: partitionOwner},
		{ID: "scylladb.traces.list", Method: plugin.MethodGet, Path: "/traces", Permission: "scylladb.tracing.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.traces.list", Handle: listTraces},
		{ID: "scylladb.trace.overview", Method: plugin.MethodGet, Path: "/traces/{id}", Permission: "scylladb.tracing.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.trace.overview", Handle: traceOverview},
		{ID: "scylladb.trace.events", Method: plugin.MethodGet, Path: "/traces/{id}/events", Permission: "scylladb.tracing.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.trace.events", Handle: traceEvents},
		{ID: "scylladb.trace.run", Method: plugin.MethodPost, Path: "/traces", Permission: "scylladb.tracing.execute", Risk: plugin.RiskPrivileged, AuditEvent: "scylladb.trace.run", Input: traceRunSchema(), Handle: runTrace},
		{ID: "scylladb.slowlog.list", Method: plugin.MethodGet, Path: "/slow-log", Permission: "scylladb.tracing.read", Risk: plugin.RiskSafe, AuditEvent: "scylladb.slowlog.list", Handle: listSlowLog},
	}
}

// shardOf maps a Murmur3 token to the ScyllaDB shard that owns it under the
// biased-token-round-robin sharding algorithm every Scylla node advertises.
func shardOf(token int64, shards int, msbIgnore uint64) int {
	if shards <= 1 {
		return 0
	}
	z := uint64(token+math.MinInt64) << msbIgnore
	lo := z & 0xffffffff
	hi := (z >> 32) & 0xffffffff
	return int(((lo*uint64(shards))>>32 + hi*uint64(shards)) >> 32)
}

type nodeFacts struct {
	Address           string
	HostID            string
	DataCenter        string
	Rack              string
	Status            string
	Up                bool
	ReleaseVersion    string
	SchemaVersion     string
	Tokens            []int64
	TokenCount        int
	OwnsPct           float64
	LoadBytes         int64
	Shards            int
	MSBIgnore         uint64
	ShardingAlgorithm string
	ShardAwarePort    int
	DriverConnections int
	DriverShards      int
	InFlight          int
	StorageUsed       int64
	StorageTotal      int64
	TabletsAllocated  int64
	Local             bool
}

// clusterFacts merges the driver's live ring/shard view with the node-side
// system tables, so shard counts and connection health stay honest even when a
// virtual table is unavailable on an older release.
func clusterFacts(ctx context.Context, s *Session) ([]nodeFacts, error) {
	byID := map[string]*nodeFacts{}
	order := []string{}
	get := func(hostID, address string) *nodeFacts {
		key := strings.ToLower(strings.TrimSpace(hostID))
		if key == "" {
			key = "addr:" + address
		}
		if f, ok := byID[key]; ok {
			if f.Address == "" {
				f.Address = address
			}
			return f
		}
		f := &nodeFacts{Address: address, HostID: hostID}
		byID[key] = f
		order = append(order, key)
		return f
	}

	local, err := queryRows(ctx, s, `
SELECT host_id, broadcast_address, rpc_address, listen_address, data_center, rack, release_version, schema_version, tokens
FROM system.local`, nil)
	if err != nil {
		return nil, err
	}
	for _, r := range local {
		address := fmt.Sprint(firstNonEmpty(r["broadcast_address"], r["rpc_address"], r["listen_address"]))
		f := get(text(r["host_id"]), address)
		f.Local = true
		applySystemRow(f, r)
	}
	peers, err := queryRows(ctx, s, `
SELECT host_id, peer, rpc_address, data_center, rack, release_version, schema_version, tokens
FROM system.peers`, nil)
	if err == nil {
		for _, r := range peers {
			address := fmt.Sprint(firstNonEmpty(r["peer"], r["rpc_address"]))
			applySystemRow(get(text(r["host_id"]), address), r)
		}
	}
	if status, err := queryRows(ctx, s, `SELECT peer, host_id, dc, rack, status, up, load, owns, tokens FROM system.cluster_status`, nil); err == nil {
		for _, r := range status {
			f := get(text(r["host_id"]), text(r["peer"]))
			f.Status = statusCode(text(r["status"]), boolValue(r["up"]))
			f.Up = boolValue(r["up"])
			f.DataCenter = stringDefault(text(r["dc"]), f.DataCenter)
			f.Rack = stringDefault(text(r["rack"]), f.Rack)
			f.LoadBytes = int64Value(r["load"])
			f.OwnsPct = floatValue(r["owns"]) * 100
			if count := intValue(r["tokens"]); count > 0 {
				f.TokenCount = count
			}
		}
	}
	if load, err := queryRows(ctx, s, `SELECT node, storage_capacity, storage_load, tablets_allocated FROM system.load_per_node`, nil); err == nil {
		for _, r := range load {
			f := get(text(r["node"]), "")
			f.StorageTotal = int64Value(r["storage_capacity"])
			f.StorageUsed = int64Value(r["storage_load"])
			f.TabletsAllocated = int64Value(r["tablets_allocated"])
		}
	}

	pools := map[string]gocql.HostPoolInfo{}
	s.db.IterateHostPools(func(info gocql.HostPoolInfo) bool {
		pools[strings.ToLower(info.Host().HostID())] = info
		return true
	})
	for _, host := range s.db.GetHosts() {
		f := get(host.HostID(), host.ConnectAddress().String())
		features := host.ScyllaFeatures()
		f.Shards = host.ScyllaShardCount()
		f.MSBIgnore = features.MSBIgnore()
		f.ShardingAlgorithm = features.ShardingAlgorithm()
		f.ShardAwarePort = int(host.ScyllaShardAwarePort())
		f.DataCenter = stringDefault(host.DataCenter(), f.DataCenter)
		f.Rack = stringDefault(host.Rack(), f.Rack)
		if f.Status == "" {
			f.Status = statusCode("NORMAL", host.IsUp())
			f.Up = host.IsUp()
		}
		if len(f.Tokens) == 0 {
			f.Tokens = parseTokens(host.Tokens())
			f.TokenCount = len(f.Tokens)
		}
		if pool, ok := pools[strings.ToLower(host.HostID())]; ok {
			f.DriverConnections = pool.GetConnectionCount()
			f.DriverShards = pool.GetShardCount()
			f.InFlight = pool.InFlight()
		}
	}

	out := make([]nodeFacts, 0, len(order))
	for _, key := range order {
		f := byID[key]
		if f.Address == "" || f.Address == "<nil>" {
			continue
		}
		if f.Shards <= 0 {
			f.Shards = 1
		}
		if f.TokenCount == 0 {
			f.TokenCount = len(f.Tokens)
		}
		out = append(out, *f)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Address < out[j].Address })
	return out, nil
}

// text renders a CQL value for display, mapping a NULL to an empty string
// instead of the "<nil>" fmt would otherwise print into the UI.
func text(v any) string {
	if v == nil {
		return ""
	}
	s := strings.TrimSpace(fmt.Sprint(v))
	if s == "<nil>" {
		return ""
	}
	return s
}

func applySystemRow(f *nodeFacts, r row) {
	f.DataCenter = stringDefault(text(r["data_center"]), f.DataCenter)
	f.Rack = stringDefault(text(r["rack"]), f.Rack)
	f.ReleaseVersion = stringDefault(text(r["release_version"]), f.ReleaseVersion)
	f.SchemaVersion = stringDefault(text(r["schema_version"]), f.SchemaVersion)
	if tokens := parseTokens(stringSlice(r["tokens"])); len(tokens) > 0 {
		f.Tokens = tokens
		f.TokenCount = len(tokens)
	}
}

func parseTokens(raw []string) []int64 {
	out := make([]int64, 0, len(raw))
	for _, token := range raw {
		value, err := strconv.ParseInt(strings.Trim(strings.TrimSpace(token), `"'`), 10, 64)
		if err != nil {
			continue
		}
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// statusCode renders the two-letter nodetool status code (UN, DN, ...).
func statusCode(state string, up bool) string {
	code := "?"
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case "NORMAL", "UN", "DN":
		code = "N"
	case "JOINING", "BOOTSTRAPPING":
		code = "J"
	case "LEAVING", "DECOMMISSIONING":
		code = "L"
	case "MOVING":
		code = "M"
	}
	if up {
		return "U" + code
	}
	return "D" + code
}

func nodeRow(f nodeFacts) row {
	return row{
		"address":            f.Address,
		"host_id":            f.HostID,
		"status":             f.Status,
		"state":              statusLabel(f.Status),
		"up":                 f.Up,
		"data_center":        f.DataCenter,
		"rack":               f.Rack,
		"release_version":    f.ReleaseVersion,
		"schema_version":     f.SchemaVersion,
		"shards":             f.Shards,
		"tokens":             f.TokenCount,
		"owns_pct":           round2(f.OwnsPct),
		"load_bytes":         f.LoadBytes,
		"shard_aware_port":   f.ShardAwarePort,
		"sharding_algorithm": f.ShardingAlgorithm,
		"driver_connections": f.DriverConnections,
		"storage_used":       f.StorageUsed,
		"storage_total":      f.StorageTotal,
		"storage_pct":        round2(percent(float64(f.StorageUsed), float64(f.StorageTotal))),
		"tablets_allocated":  f.TabletsAllocated,
		"ref":                plugin.ResourceIdentity{Kind: "node", Name: f.Address, UID: f.Address},
	}
}

func statusLabel(code string) string {
	if len(code) != 2 {
		return code
	}
	up := "Down"
	if code[0] == 'U' {
		up = "Up"
	}
	switch code[1] {
	case 'N':
		return up + " / Normal"
	case 'J':
		return up + " / Joining"
	case 'L':
		return up + " / Leaving"
	case 'M':
		return up + " / Moving"
	default:
		return up
	}
}

func listNodes(rc *plugin.RequestContext) (any, error) {
	rows, err := nodeItems(rc)
	if err != nil {
		return nil, err
	}
	return pageRows(rc, rows)
}

func nodeItems(rc *plugin.RequestContext) ([]row, error) {
	s, err := scyllaSession(rc)
	if err != nil {
		return nil, err
	}
	facts, err := clusterFacts(rc.Ctx, s)
	if err != nil {
		return nil, err
	}
	out := make([]row, 0, len(facts))
	for _, f := range facts {
		out = append(out, nodeRow(f))
	}
	return out, nil
}

// nodesWatch keeps the cluster-status list live. ScyllaDB exposes topology only
// through virtual tables, so this is a bounded server-side poll rather than a
// native change feed.
func nodesWatch(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := scyllaSession(rc)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(client)
	previous := map[string]string{}
	return pollStream(rc, client, 10*time.Second, func(ctx context.Context) error {
		facts, err := clusterFacts(ctx, s)
		if err != nil {
			return nil
		}
		seen := map[string]bool{}
		for _, f := range facts {
			item := nodeRow(f)
			ref, _ := item["ref"].(plugin.ResourceIdentity)
			seen[f.Address] = true
			fingerprint := fmt.Sprintf("%s|%d|%d|%.2f|%d", f.Status, f.Shards, f.TokenCount, f.OwnsPct, f.LoadBytes)
			eventType := "modified"
			if _, known := previous[f.Address]; !known {
				eventType = "added"
			} else if previous[f.Address] == fingerprint {
				continue
			}
			previous[f.Address] = fingerprint
			if err := enc.Encode(plugin.ResourceEvent{Type: eventType, Ref: ref, Resource: item}); err != nil {
				return err
			}
		}
		for address := range previous {
			if seen[address] {
				continue
			}
			delete(previous, address)
			if err := enc.Encode(plugin.ResourceEvent{
				Type: "deleted",
				Ref:  plugin.ResourceIdentity{Kind: "node", Name: address, UID: address},
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

func treeNodes(rc *plugin.RequestContext) (any, error) {
	res, err := listNodes(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, r := range page.Items {
		ref, _ := r["ref"].(plugin.ResourceIdentity)
		nodes = append(nodes, plugin.TreeNode{
			Key:   "node:" + ref.UID,
			Label: fmt.Sprintf("%s (%s)", ref.Name, r["data_center"]),
			Icon:  icon("server"),
			Ref:   &ref,
			Leaf:  true,
			Data:  map[string]any{"status": r["status"], "shards": r["shards"]},
		})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

func nodeOverview(rc *plugin.RequestContext) (any, error) {
	address := strings.TrimSpace(rc.Param("address"))
	items, err := nodeItems(rc)
	if err != nil {
		return nil, err
	}
	for _, r := range items {
		if fmt.Sprint(r["address"]) == address {
			return r, nil
		}
	}
	return nil, plugin.ErrNotFound
}

func listCluster(rc *plugin.RequestContext) (any, error) {
	overview, err := clusterOverview(rc)
	if err != nil {
		return nil, err
	}
	item := overview.(row)
	item["ref"] = plugin.ResourceIdentity{Kind: "cluster", Name: fmt.Sprint(item["cluster_name"]), UID: "cluster"}
	total := 1
	return plugin.Page[row]{Items: []row{item}, Total: &total}, nil
}

func clusterOverview(rc *plugin.RequestContext) (any, error) {
	s, err := scyllaSession(rc)
	if err != nil {
		return nil, err
	}
	return clusterSnapshot(rc.Ctx, s)
}

func clusterSnapshot(ctx context.Context, s *Session) (row, error) {
	facts, err := clusterFacts(ctx, s)
	if err != nil {
		return nil, err
	}
	local, err := queryRows(ctx, s, `SELECT cluster_name, cql_version, partitioner, release_version, schema_version FROM system.local`, nil)
	if err != nil {
		return nil, err
	}
	item := row{"cluster_name": "ScyllaDB cluster"}
	if len(local) > 0 {
		item["cluster_name"] = stringDefault(fmt.Sprint(local[0]["cluster_name"]), "ScyllaDB cluster")
		item["cql_version"] = local[0]["cql_version"]
		item["partitioner"] = local[0]["partitioner"]
		item["release_version"] = local[0]["release_version"]
	}
	if versions, err := queryRows(ctx, s, `SELECT version, build_mode, build_id FROM system.versions`, nil); err == nil && len(versions) > 0 {
		item["release_version"] = versions[0]["version"]
		item["build_mode"] = versions[0]["build_mode"]
		item["build_id"] = versions[0]["build_id"]
	}
	settings := configValues(ctx, s, "endpoint_snitch", "enable_tablets", "cluster_name")
	item["snitch"] = strings.Trim(settings["endpoint_snitch"], `"`)
	item["tablets_enabled"] = settings["enable_tablets"] == "true"

	datacenters, racks, schemas := map[string]bool{}, map[string]bool{}, map[string]bool{}
	up, shards, tokens, connections, coveredShards, inFlight := 0, 0, 0, 0, 0, 0
	for _, f := range facts {
		if f.Up {
			up++
		}
		shards += f.Shards
		tokens += f.TokenCount
		connections += f.DriverConnections
		coveredShards += f.DriverShards
		inFlight += f.InFlight
		datacenters[f.DataCenter] = true
		racks[f.DataCenter+"/"+f.Rack] = true
		if f.SchemaVersion != "" {
			schemas[f.SchemaVersion] = true
		}
		if item["sharding_algorithm"] == nil && f.ShardingAlgorithm != "" {
			item["sharding_algorithm"] = f.ShardingAlgorithm
			item["shard_aware_port"] = f.ShardAwarePort
		}
	}
	health := "healthy"
	switch {
	case up == 0:
		health = "down"
	case up < len(facts):
		health = "degraded"
	}
	item["health"] = health
	item["nodes_total"] = len(facts)
	item["nodes_up"] = up
	item["shards_total"] = shards
	item["tokens_total"] = tokens
	item["datacenters"] = len(datacenters)
	item["racks"] = len(racks)
	item["schema_agreement"] = len(schemas) <= 1
	item["driver_connections"] = connections
	item["driver_shard_connections"] = coveredShards
	item["shard_aware_enabled"] = s.opts.ShardAwarePort
	item["in_flight"] = inFlight
	if item["shard_aware_port"] == nil {
		item["shard_aware_port"] = 0
	}
	if item["sharding_algorithm"] == nil {
		item["sharding_algorithm"] = ""
	}
	return item, nil
}

// clusterWatch pushes a fresh cluster snapshot to the overview panel. ScyllaDB
// has no CQL change feed for topology, so this is a bounded server-side poll.
func clusterWatch(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := scyllaSession(rc)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(client)
	return pollStream(rc, client, 10*time.Second, func(ctx context.Context) error {
		snapshot, err := clusterSnapshot(ctx, s)
		if err != nil {
			return nil
		}
		return enc.Encode(plugin.ResourceEvent{
			Type:     "modified",
			Ref:      plugin.ResourceIdentity{Kind: "cluster", Name: fmt.Sprint(snapshot["cluster_name"]), UID: "cluster"},
			Resource: snapshot,
		})
	})
}

// pollStream runs fn immediately and then on every tick until the browser or the
// request context goes away. Server-push panels use it for backends without a
// native change feed.
func pollStream(rc *plugin.RequestContext, client plugin.ClientStream, every time.Duration, fn func(context.Context) error) error {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		if err := fn(rc.Ctx); err != nil {
			return err
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

// hostIDFor resolves a node address to the driver's host id, so a node-local
// virtual table is read on the node the panel is about.
func hostIDFor(s *Session, address string) (string, error) {
	address = strings.TrimSpace(address)
	if address == "" {
		return "", nil
	}
	for _, host := range s.db.GetHosts() {
		if host.ConnectAddress().String() == address || strings.EqualFold(host.HostID(), address) {
			return host.HostID(), nil
		}
	}
	return "", fmt.Errorf("%w: node %s is not in the driver's ring view", plugin.ErrNotFound, address)
}

func nodeRuntime(rc *plugin.RequestContext) (any, error) {
	s, err := scyllaSession(rc)
	if err != nil {
		return nil, err
	}
	address := strings.TrimSpace(rc.Param("address"))
	hostID, err := hostIDFor(s, address)
	if err != nil {
		return nil, err
	}
	rows, err := queryRowsOn(rc.Ctx, s, hostID, `SELECT "group", item, value FROM system.runtime_info`, nil)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		r["node"] = address
	}
	return pageRows(rc, rows)
}

const clientsCQL = `
SELECT address, port, client_type, connection_stage, driver_name, driver_version, hostname, protocol_version, shard_id, ssl_enabled, username, scheduling_group
FROM system.clients`

func listClients(rc *plugin.RequestContext) (any, error) {
	s, err := scyllaSession(rc)
	if err != nil {
		return nil, err
	}
	address := strings.TrimSpace(rc.Param("address"))
	if address == "" {
		rows, err := clientRows(rc.Ctx, s)
		if err != nil {
			return nil, err
		}
		return pageRows(rc, rows)
	}
	hostID, err := hostIDFor(s, address)
	if err != nil {
		return nil, err
	}
	rows, err := queryRowsOn(rc.Ctx, s, hostID, clientsCQL, nil)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		r["node"] = address
	}
	return pageRows(rc, rows)
}

// clientRows unions system.clients across the ring. The table is node-local, so
// one query would only ever describe the coordinator that answered it.
func clientRows(ctx context.Context, s *Session) ([]row, error) {
	out := []row{}
	var lastErr error
	for _, host := range s.db.GetHosts() {
		rows, err := queryRowsOn(ctx, s, host.HostID(), clientsCQL, nil)
		if err != nil {
			lastErr = err
			continue
		}
		address := host.ConnectAddress().String()
		for _, r := range rows {
			r["node"] = address
			out = append(out, r)
		}
	}
	if len(out) == 0 && lastErr != nil {
		return nil, lastErr
	}
	return out, nil
}

// clientsByShard counts one node's CQL clients per shard.
func clientsByShard(ctx context.Context, s *Session, hostID string) map[int]int {
	out := map[int]int{}
	rows, err := queryRowsOn(ctx, s, hostID, `SELECT shard_id FROM system.clients`, nil)
	if err != nil {
		return out
	}
	for _, r := range rows {
		out[intValue(r["shard_id"])]++
	}
	return out
}

func listConfig(rc *plugin.RequestContext) (any, error) {
	s, err := scyllaSession(rc)
	if err != nil {
		return nil, err
	}
	rows, err := queryRows(rc.Ctx, s, `SELECT name, value, type, source FROM system.config`, nil)
	if err != nil {
		return nil, err
	}
	return pageRows(rc, rows)
}

func configValues(ctx context.Context, s *Session, names ...string) map[string]string {
	out := map[string]string{}
	rows, err := queryRows(ctx, s, `SELECT name, value FROM system.config`, nil)
	if err != nil {
		return out
	}
	wanted := map[string]bool{}
	for _, name := range names {
		wanted[name] = true
	}
	for _, r := range rows {
		name := fmt.Sprint(r["name"])
		if wanted[name] {
			out[name] = fmt.Sprint(r["value"])
		}
	}
	return out
}

type compactionEvent struct {
	ID          string
	At          time.Time
	Keyspace    string
	Table       string
	Type        string
	BytesIn     int64
	BytesOut    int64
	Shard       int
	SSTablesIn  int
	SSTablesOut int
	Duration    time.Duration
}

func compactionHistory(ctx context.Context, s *Session, keyspace, table string) ([]compactionEvent, error) {
	rows, err := queryRows(ctx, s, `
SELECT id, keyspace_name, columnfamily_name, compaction_type, bytes_in, bytes_out, shard_id, sstables_in, sstables_out, compacted_at, started_at
FROM system.compaction_history`, nil)
	if err != nil {
		return nil, err
	}
	out := make([]compactionEvent, 0, len(rows))
	for _, r := range rows {
		ks, cf := text(r["keyspace_name"]), text(r["columnfamily_name"])
		if keyspace != "" && ks != keyspace {
			continue
		}
		if table != "" && cf != table {
			continue
		}
		at := timeValue(r["compacted_at"])
		out = append(out, compactionEvent{
			ID:          fmt.Sprint(r["id"]),
			At:          at,
			Keyspace:    ks,
			Table:       cf,
			Type:        text(r["compaction_type"]),
			BytesIn:     int64Value(r["bytes_in"]),
			BytesOut:    int64Value(r["bytes_out"]),
			Shard:       intValue(r["shard_id"]),
			SSTablesIn:  countList(r["sstables_in"]),
			SSTablesOut: countList(r["sstables_out"]),
			Duration:    at.Sub(timeValue(r["started_at"])),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].At.After(out[j].At) })
	return out, nil
}

func compactionRow(e compactionEvent) row {
	severity := plugin.SeverityInfo
	if e.BytesOut > e.BytesIn {
		severity = plugin.SeverityWarn
	}
	detail := fmt.Sprintf("shard %d · %s in → %s out · %d → %d sstables", e.Shard, humanBytes(e.BytesIn), humanBytes(e.BytesOut), e.SSTablesIn, e.SSTablesOut)
	if e.Duration > 0 {
		detail += " · " + e.Duration.Truncate(time.Millisecond).String()
	}
	return row{
		"id":           e.ID,
		"compacted_at": e.At.Format(time.RFC3339Nano),
		"title":        e.Type + " " + e.Keyspace + "." + e.Table,
		"detail":       detail,
		"severity":     string(severity),
		"icon":         "layers",
		"target":       e.Keyspace + "." + e.Table,
		"keyspace":     e.Keyspace,
		"table":        e.Table,
		"shard":        e.Shard,
		"bytes_in":     e.BytesIn,
		"bytes_out":    e.BytesOut,
		"ref":          plugin.ResourceIdentity{Kind: "compaction", Namespace: e.Keyspace, Name: e.Table, UID: e.ID},
	}
}

func listCompactions(rc *plugin.RequestContext) (any, error) {
	s, err := scyllaSession(rc)
	if err != nil {
		return nil, err
	}
	keyspace, table := strings.TrimSpace(rc.Param("keyspace")), strings.TrimSpace(rc.Param("table"))
	events, err := compactionHistory(rc.Ctx, s, keyspace, table)
	if err != nil {
		return nil, err
	}
	out := make([]row, 0, len(events))
	for _, e := range events {
		out = append(out, compactionRow(e))
	}
	return pageRows(rc, out)
}

// compactionWatch pushes compactions that finished after the stream opened, so
// the timeline grows without refetching the whole history.
func compactionWatch(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := scyllaSession(rc)
	if err != nil {
		return err
	}
	keyspace, table := strings.TrimSpace(rc.Param("keyspace")), strings.TrimSpace(rc.Param("table"))
	enc := json.NewEncoder(client)
	seen := map[string]bool{}
	first := true
	return pollStream(rc, client, 10*time.Second, func(ctx context.Context) error {
		events, err := compactionHistory(ctx, s, keyspace, table)
		if err != nil {
			return nil
		}
		for _, e := range events {
			if seen[e.ID] {
				continue
			}
			seen[e.ID] = true
			if first {
				continue
			}
			item := compactionRow(e)
			ref, _ := item["ref"].(plugin.ResourceIdentity)
			if err := enc.Encode(plugin.ResourceEvent{Type: "added", Ref: ref, Resource: item}); err != nil {
				return err
			}
		}
		first = false
		return nil
	})
}

type tokenRange struct {
	Start    int64
	End      int64
	Endpoint string
	DC       string
	Rack     string
}

func keyspaceTokenRanges(ctx context.Context, s *Session, keyspace string) ([]tokenRange, error) {
	rows, err := queryRows(ctx, s, `
SELECT start_token, end_token, endpoint, dc, rack
FROM system.token_ring
WHERE keyspace_name = ?`, []any{keyspace})
	if err != nil {
		return nil, err
	}
	out := make([]tokenRange, 0, len(rows))
	for _, r := range rows {
		out = append(out, tokenRange{
			Start:    int64Value(r["start_token"]),
			End:      int64Value(r["end_token"]),
			Endpoint: text(r["endpoint"]),
			DC:       text(r["dc"]),
			Rack:     text(r["rack"]),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Start < out[j].Start })
	return out, nil
}

func listTokenRing(rc *plugin.RequestContext) (any, error) {
	keyspace, err := safeIdent(rc.Param("keyspace"))
	if err != nil {
		return nil, err
	}
	s, err := scyllaSession(rc)
	if err != nil {
		return nil, err
	}
	ranges, err := keyspaceTokenRanges(rc.Ctx, s, keyspace)
	if err != nil {
		return nil, err
	}
	facts, err := clusterFacts(rc.Ctx, s)
	if err != nil {
		return nil, err
	}
	shardsByAddress := map[string]nodeFacts{}
	for _, f := range facts {
		shardsByAddress[f.Address] = f
	}
	out := make([]row, 0, len(ranges))
	for _, tr := range ranges {
		shard := 0
		if f, ok := shardsByAddress[tr.Endpoint]; ok {
			shard = shardOf(tr.End, f.Shards, f.MSBIgnore)
		}
		out = append(out, row{
			"start_token": strconv.FormatInt(tr.Start, 10),
			"end_token":   strconv.FormatInt(tr.End, 10),
			"endpoint":    tr.Endpoint,
			"shard":       shard,
			"data_center": tr.DC,
			"rack":        tr.Rack,
		})
	}
	return pageRows(rc, out)
}

func partitionLocateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Partition", Fields: []plugin.Field{
		{Key: "partition_key", Label: "Partition key", Type: plugin.FieldMap, Required: true, KeyPlaceholder: "column", AddLabel: "Add key column", Item: &plugin.Field{Type: plugin.FieldText}, Help: "One entry per partition-key column. Values are bound as CQL parameters and the coordinator computes the token."},
	}}}}
}

// locatePartition asks the coordinator for the real Murmur3 token of a partition
// key, so the placement answer never depends on a client-side hash.
func locatePartition(rc *plugin.RequestContext) (any, error) {
	keyspace, table, err := tableIdent(rc)
	if err != nil {
		return nil, err
	}
	s, err := scyllaSession(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		PartitionKey map[string]any `json:"partition_key" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	pk, err := partitionKeyColumns(rc.Ctx, s, keyspace, table)
	if err != nil {
		return nil, err
	}
	if len(pk) == 0 {
		return nil, fmt.Errorf("%w: table has no partition key", plugin.ErrInvalidInput)
	}
	if len(req.PartitionKey) != len(pk) {
		return nil, fmt.Errorf("%w: supply exactly the %d partition-key column(s): %s", plugin.ErrInvalidInput, len(pk), strings.Join(pk, ", "))
	}
	columns := make([]string, 0, len(pk))
	predicates := make([]string, 0, len(pk))
	args := make([]any, 0, len(pk))
	for _, col := range pk {
		value, ok := req.PartitionKey[col]
		if !ok {
			return nil, fmt.Errorf("%w: partition key column %q is missing", plugin.ErrInvalidInput, col)
		}
		columns = append(columns, quoteIdent(col))
		predicates = append(predicates, quoteIdent(col)+" = ?")
		args = append(args, value)
	}
	cql := fmt.Sprintf(`SELECT token(%s) AS "_token" FROM %s WHERE %s LIMIT 1`,
		strings.Join(columns, ", "), qualified(keyspace, table), strings.Join(predicates, " AND "))
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.QueryTimeout)
	defer cancel()
	var token int64
	if err := s.db.Query(cql, args...).WithContext(ctx).Consistency(s.opts.Consistency).Scan(&token); err != nil {
		if err == gocql.ErrNotFound {
			return nil, fmt.Errorf("%w: no row with that partition key, so the coordinator cannot compute its token", plugin.ErrNotFound)
		}
		return nil, scyllaErr(err)
	}
	return row{"keyspace": keyspace, "table": table, "token": strconv.FormatInt(token, 10)}, nil
}

func partitionOwner(rc *plugin.RequestContext) (any, error) {
	keyspace, err := safeIdent(rc.Param("keyspace"))
	if err != nil {
		return nil, err
	}
	token, err := strconv.ParseInt(strings.TrimSpace(rc.Param("token")), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: token must be a 64-bit integer", plugin.ErrInvalidInput)
	}
	s, err := scyllaSession(rc)
	if err != nil {
		return nil, err
	}
	ranges, err := keyspaceTokenRanges(rc.Ctx, s, keyspace)
	if err != nil {
		return nil, err
	}
	if len(ranges) == 0 {
		return nil, fmt.Errorf("%w: keyspace %q has no token ring", plugin.ErrNotFound, keyspace)
	}
	facts, err := clusterFacts(rc.Ctx, s)
	if err != nil {
		return nil, err
	}
	byAddress := map[string]nodeFacts{}
	for _, f := range facts {
		byAddress[f.Address] = f
	}
	owners := ownersOf(ranges, token)
	if len(owners) == 0 {
		return nil, fmt.Errorf("%w: no replica owns token %d", plugin.ErrNotFound, token)
	}
	primary := owners[0]
	replicas := make([]string, 0, len(owners))
	for _, tr := range owners {
		replicas = append(replicas, tr.Endpoint)
	}
	item := row{
		"keyspace":        keyspace,
		"table":           rc.Param("table"),
		"token":           strconv.FormatInt(token, 10),
		"range_start":     strconv.FormatInt(primary.Start, 10),
		"range_end":       strconv.FormatInt(primary.End, 10),
		"primary_replica": primary.Endpoint,
		"data_center":     primary.DC,
		"rack":            primary.Rack,
		"replicas":        replicas,
		"replica_count":   len(replicas),
		"shard":           0,
	}
	if f, ok := byAddress[primary.Endpoint]; ok {
		item["shard"] = shardOf(token, f.Shards, f.MSBIgnore)
		item["shards"] = f.Shards
	}
	return item, nil
}

// ownersOf returns every replica range covering the token. system.token_ring
// repeats a range once per replica, so all matching rows are the replica set.
func ownersOf(ranges []tokenRange, token int64) []tokenRange {
	var out []tokenRange
	for _, tr := range ranges {
		if tr.Start < tr.End {
			if token > tr.Start && token <= tr.End {
				out = append(out, tr)
			}
			continue
		}
		// The wrapping range covers both ends of the ring.
		if token > tr.Start || token <= tr.End {
			out = append(out, tr)
		}
	}
	return out
}

func firstNonEmpty(values ...any) any {
	for _, value := range values {
		text := strings.TrimSpace(fmt.Sprint(value))
		if text != "" && text != "<nil>" {
			return value
		}
	}
	return ""
}

func boolValue(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case string:
		return strings.EqualFold(strings.TrimSpace(x), "true")
	default:
		return false
	}
}

func floatValue(v any) float64 {
	switch x := v.(type) {
	case float32:
		return float64(x)
	case float64:
		return x
	case int:
		return float64(x)
	case int64:
		return float64(x)
	default:
		f, _ := strconv.ParseFloat(strings.TrimSpace(fmt.Sprint(x)), 64)
		return f
	}
}

func timeValue(v any) time.Time {
	switch x := v.(type) {
	case time.Time:
		return x
	case string:
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
			if t, err := time.Parse(layout, x); err == nil {
				return t
			}
		}
	}
	return time.Time{}
}

func countList(v any) int {
	switch x := v.(type) {
	case []any:
		return len(x)
	case []map[string]any:
		return len(x)
	case string:
		var decoded []any
		if json.Unmarshal([]byte(x), &decoded) == nil {
			return len(decoded)
		}
	}
	return 0
}

func percent(used, total float64) float64 {
	if total <= 0 {
		return 0
	}
	return used / total * 100
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}

func humanBytes(v int64) string {
	const unit = 1024
	if v < unit {
		return fmt.Sprintf("%d B", v)
	}
	div, exp := int64(unit), 0
	for n := v / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(v)/float64(div), "KMGTPE"[exp])
}
