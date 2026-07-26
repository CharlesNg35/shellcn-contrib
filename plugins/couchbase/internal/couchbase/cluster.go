package couchbase

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const clusterUID = "cluster"

type poolNode struct {
	Hostname          string   `json:"hostname"`
	Status            string   `json:"status"`
	ClusterMembership string   `json:"clusterMembership"`
	Version           string   `json:"version"`
	OS                string   `json:"os"`
	Uptime            string   `json:"uptime"`
	Services          []string `json:"services"`
	CPUCount          int      `json:"cpuCount"`
	MemoryTotal       float64  `json:"memoryTotal"`
	MemoryFree        float64  `json:"memoryFree"`
	SystemStats       struct {
		CPUUtilizationRate float64 `json:"cpu_utilization_rate"`
		MemTotal           float64 `json:"mem_total"`
		MemFree            float64 `json:"mem_free"`
		SwapTotal          float64 `json:"swap_total"`
		SwapUsed           float64 `json:"swap_used"`
	} `json:"systemStats"`
	InterestingStats struct {
		CurrItems     float64 `json:"curr_items"`
		CurrItemsTot  float64 `json:"curr_items_tot"`
		MemUsed       float64 `json:"mem_used"`
		Ops           float64 `json:"ops"`
		DocsDiskSize  float64 `json:"couch_docs_actual_disk_size"`
		IndexDiskSize float64 `json:"index_disk_size"`
	} `json:"interestingStats"`
}

type poolDefault struct {
	Name             string     `json:"name"`
	ClusterName      string     `json:"clusterName"`
	Nodes            []poolNode `json:"nodes"`
	BucketNames      []any      `json:"bucketNames"`
	Balanced         bool       `json:"balanced"`
	RebalanceStatus  string     `json:"rebalanceStatus"`
	MaxBucketCount   int        `json:"maxBucketCount"`
	MemoryQuota      float64    `json:"memoryQuota"`
	IndexMemoryQuota float64    `json:"indexMemoryQuota"`
	StorageTotals    struct {
		RAM struct {
			Total      float64 `json:"total"`
			QuotaTotal float64 `json:"quotaTotal"`
			QuotaUsed  float64 `json:"quotaUsed"`
			Used       float64 `json:"used"`
			UsedByData float64 `json:"usedByData"`
		} `json:"ram"`
		HDD struct {
			Total      float64 `json:"total"`
			Used       float64 `json:"used"`
			UsedByData float64 `json:"usedByData"`
			Free       float64 `json:"free"`
		} `json:"hdd"`
	} `json:"storageTotals"`
	Alerts []string `json:"alerts"`
}

type poolsRoot struct {
	IsEnterprise       bool   `json:"isEnterprise"`
	ImplementationVer  string `json:"implementationVersion"`
	IsDeveloperPreview bool   `json:"isDeveloperPreview"`
}

func clusterRef() plugin.ResourceIdentity {
	return plugin.ResourceIdentity{Kind: "cluster", Name: "Cluster", UID: clusterUID}
}

func (s *Session) clusterOverview(ctx context.Context) (row, error) {
	var pool poolDefault
	if err := s.mgmtGet(ctx, "/pools/default", &pool); err != nil {
		return nil, err
	}
	var root poolsRoot
	if err := s.mgmtGet(ctx, "/pools", &root); err != nil {
		return nil, err
	}

	services := map[string]bool{}
	healthy, items, ops, cpu := 0, 0.0, 0.0, 0.0
	for _, node := range pool.Nodes {
		for _, svc := range node.Services {
			services[svc] = true
		}
		if strings.EqualFold(node.Status, "healthy") {
			healthy++
		}
		items += node.InterestingStats.CurrItemsTot
		ops += node.InterestingStats.Ops
		cpu += node.SystemStats.CPUUtilizationRate
	}
	if len(pool.Nodes) > 0 {
		cpu /= float64(len(pool.Nodes))
	}
	edition := "Community"
	if root.IsEnterprise {
		edition = "Enterprise"
	}
	version := root.ImplementationVer
	if len(pool.Nodes) > 0 && pool.Nodes[0].Version != "" {
		version = pool.Nodes[0].Version
	}
	name := pool.ClusterName
	if name == "" {
		name = s.opts.Host
	}
	health := "healthy"
	switch {
	case healthy == 0:
		health = "down"
	case healthy < len(pool.Nodes):
		health = "degraded"
	case !pool.Balanced:
		health = "unbalanced"
	}

	ram := pool.StorageTotals.RAM
	hdd := pool.StorageTotals.HDD
	return row{
		"ref":                   clusterRef(),
		"name":                  name,
		"health":                health,
		"version":               version,
		"edition":               edition,
		"developer_preview":     root.IsDeveloperPreview,
		"nodes":                 len(pool.Nodes),
		"healthy_nodes":         healthy,
		"services":              sortedSet(services),
		"balanced":              pool.Balanced,
		"rebalance_status":      pool.RebalanceStatus,
		"buckets":               len(pool.BucketNames),
		"max_buckets":           pool.MaxBucketCount,
		"items":                 items,
		"opsPerSec":             round1(ops),
		"cpuPct":                round1(cpu),
		"alerts":                len(pool.Alerts),
		"quotaPct":              percent(ram.QuotaUsed, ram.QuotaTotal),
		"quotaUsed":             ram.QuotaUsed,
		"quotaTotal":            ram.QuotaTotal,
		"ramPct":                percent(ram.Used, ram.Total),
		"ramUsed":               ram.Used,
		"ramTotal":              ram.Total,
		"ramUsedByData":         ram.UsedByData,
		"diskPct":               percent(hdd.Used, hdd.Total),
		"diskUsed":              hdd.Used,
		"diskTotal":             hdd.Total,
		"diskFree":              hdd.Free,
		"diskUsedByData":        hdd.UsedByData,
		"data_memory_quota":     pool.MemoryQuota,
		"index_memory_quota":    pool.IndexMemoryQuota,
		"management_endpoint":   s.opts.managementURL(),
		"query_endpoint":        s.opts.queryURL(),
		"read_only":             s.opts.ReadOnly,
		"confirm_write_queries": s.opts.RequireConfirm,
	}, nil
}

func sortedSet(in map[string]bool) []string {
	out := make([]string, 0, len(in))
	for k := range in {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func clusterList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	item, err := s.clusterOverview(rc.Ctx)
	if err != nil {
		return nil, err
	}
	return broker.PageRows(rc, []row{item})
}

func clusterOverview(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	return s.clusterOverview(rc.Ctx)
}

// clusterWatch pushes the overview snapshot so the object-detail panel stays
// live without the operator refreshing it.
func clusterWatch(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := session(rc)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(client)
	return tick(rc, client, 5*time.Second, func() error {
		snapshot, err := s.clusterOverview(rc.Ctx)
		if err != nil {
			return nil
		}
		return enc.Encode(plugin.ResourceEvent{Type: "modified", Ref: clusterRef(), Resource: snapshot})
	})
}

func clusterMetrics(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := session(rc)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(client)
	return tick(rc, client, 5*time.Second, func() error {
		frame, err := s.clusterFrame(rc.Ctx)
		if err != nil {
			return nil
		}
		return enc.Encode(frame)
	})
}

func (s *Session) clusterFrame(ctx context.Context) (map[string]any, error) {
	overview, err := s.clusterOverview(ctx)
	if err != nil {
		return nil, err
	}
	buckets, err := s.bucketSummaries(ctx)
	if err != nil {
		return nil, err
	}
	ops, fetches := 0.0, 0.0
	for _, bucket := range buckets {
		ops += numberValue(bucket["opsPerSec"])
		fetches += numberValue(bucket["diskFetches"])
	}
	frame := map[string]any{
		"buckets":     len(buckets),
		"nodes":       overview["nodes"],
		"items":       overview["items"],
		"opsPerSec":   round1(ops),
		"diskFetches": round1(fetches),
		"cpuPct":      overview["cpuPct"],
	}
	for _, key := range []string{"quotaPct", "quotaUsed", "quotaTotal", "ramPct", "ramUsed", "ramTotal", "diskPct", "diskUsed", "diskTotal"} {
		frame[key] = overview[key]
	}
	return frame, nil
}

func nodesList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var pool poolDefault
	if err := s.mgmtGet(rc.Ctx, "/pools/default", &pool); err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(pool.Nodes))
	for _, node := range pool.Nodes {
		memTotal := node.SystemStats.MemTotal
		memUsed := memTotal - node.SystemStats.MemFree
		uptime := numberValue(node.Uptime)
		rows = append(rows, row{
			"hostname":           node.Hostname,
			"status":             strings.ToLower(node.Status),
			"cluster_membership": strings.ToLower(node.ClusterMembership),
			"services":           strings.Join(node.Services, ", "),
			"version":            node.Version,
			"os":                 node.OS,
			"cpu_cores":          node.CPUCount,
			"cpuPct":             round1(node.SystemStats.CPUUtilizationRate),
			"ramPct":             percent(memUsed, memTotal),
			"ramUsed":            memUsed,
			"ramTotal":           memTotal,
			"swapUsed":           node.SystemStats.SwapUsed,
			"items":              node.InterestingStats.CurrItemsTot,
			"opsPerSec":          round1(node.InterestingStats.Ops),
			"data_disk":          node.InterestingStats.DocsDiskSize,
			"index_disk":         node.InterestingStats.IndexDiskSize,
			"uptime":             uptime,
		})
	}
	return broker.PageRows(rc, rows)
}

// --- cluster event log ------------------------------------------------------

type logEntry struct {
	Node       string  `json:"node"`
	Type       string  `json:"type"`
	Code       int     `json:"code"`
	Module     string  `json:"module"`
	Timestamp  float64 `json:"tstamp"`
	ShortText  string  `json:"shortText"`
	Text       string  `json:"text"`
	ServerTime string  `json:"serverTime"`
}

var replicationKeywords = []string{"xdcr", "replicat", "rebalance", "failover", "goxdcr", "remote cluster"}

func (e logEntry) isReplication() bool {
	haystack := strings.ToLower(e.Module + " " + e.ShortText + " " + e.Text)
	for _, keyword := range replicationKeywords {
		if strings.Contains(haystack, keyword) {
			return true
		}
	}
	return false
}

func (e logEntry) row() row {
	severity := "info"
	switch strings.ToLower(e.Type) {
	case "warning", "warn":
		severity = "warn"
	case "critical", "error":
		severity = "danger"
	}
	iconName := "info"
	switch severity {
	case "warn":
		iconName = "triangle-alert"
	case "danger":
		iconName = "octagon-alert"
	}
	title := e.ShortText
	if title == "" {
		title = e.Module
	}
	return row{
		"ref":      plugin.ResourceIdentity{Kind: "event", Name: title, UID: e.uid()},
		"time":     e.time().UTC().Format(time.RFC3339Nano),
		"title":    title,
		"message":  strings.TrimSpace(e.Text),
		"severity": severity,
		"icon":     iconName,
		"module":   e.Module,
		"node":     e.Node,
		"code":     e.Code,
	}
}

func (e logEntry) time() time.Time {
	if e.ServerTime != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, e.ServerTime); err == nil {
			return parsed
		}
	}
	return time.UnixMilli(int64(e.Timestamp))
}

func (e logEntry) uid() string {
	return e.Node + "|" + e.Module + "|" + e.ServerTime + "|" + stringValue(e.Code)
}

func (s *Session) events(ctx context.Context, replicationOnly bool) ([]logEntry, error) {
	var out struct {
		List []logEntry `json:"list"`
	}
	if err := s.mgmtGet(ctx, "/logs", &out); err != nil {
		return nil, err
	}
	entries := make([]logEntry, 0, len(out.List))
	for _, entry := range out.List {
		if replicationOnly && !entry.isReplication() {
			continue
		}
		entries = append(entries, entry)
	}
	// Newest first: the timeline renders the head of the list at the top.
	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}
	return entries, nil
}

func eventsList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	entries, err := s.events(rc.Ctx, rc.Param("filter") == "replication")
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(entries))
	for _, entry := range entries {
		rows = append(rows, entry.row())
	}
	return broker.PageRows(rc, rows)
}

func eventsWatch(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := session(rc)
	if err != nil {
		return err
	}
	replicationOnly := rc.Param("filter") == "replication"
	enc := json.NewEncoder(client)
	seen := map[string]bool{}
	first := true
	return tick(rc, client, 5*time.Second, func() error {
		entries, err := s.events(rc.Ctx, replicationOnly)
		if err != nil {
			return nil
		}
		// Prime the seen set on the first poll: the panel already rendered the
		// list route's page, so replaying it would duplicate every entry.
		for i := len(entries) - 1; i >= 0; i-- {
			uid := entries[i].uid()
			if seen[uid] {
				continue
			}
			seen[uid] = true
			if first {
				continue
			}
			item := entries[i].row()
			if err := enc.Encode(plugin.ResourceEvent{
				Type:     "added",
				Ref:      plugin.ResourceIdentity{Kind: "event", Name: stringValue(item["title"]), UID: uid},
				Resource: item,
			}); err != nil {
				return err
			}
		}
		first = false
		return nil
	})
}

// tick writes an immediate frame and then repeats on an interval until the
// browser disconnects or the request context ends.
func tick(rc *plugin.RequestContext, client plugin.ClientStream, every time.Duration, emit func() error) error {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		if err := emit(); err != nil {
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
