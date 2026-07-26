package couchbase

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

type bucketInfo struct {
	Name            string  `json:"name"`
	BucketType      string  `json:"bucketType"`
	StorageBackend  string  `json:"storageBackend"`
	UUID            string  `json:"uuid"`
	NumVBuckets     int     `json:"numVBuckets"`
	ReplicaNumber   int     `json:"replicaNumber"`
	ThreadsNumber   int     `json:"threadsNumber"`
	EvictionPolicy  string  `json:"evictionPolicy"`
	DurabilityLevel string  `json:"durabilityMinLevel"`
	ConflictRes     string  `json:"conflictResolutionType"`
	CompressionMode string  `json:"compressionMode"`
	MaxTTL          float64 `json:"maxTTL"`
	Quota           struct {
		RAM    float64 `json:"ram"`
		RawRAM float64 `json:"rawRAM"`
	} `json:"quota"`
	BasicStats struct {
		QuotaPercentUsed float64 `json:"quotaPercentUsed"`
		OpsPerSec        float64 `json:"opsPerSec"`
		DiskFetches      float64 `json:"diskFetches"`
		ItemCount        float64 `json:"itemCount"`
		DiskUsed         float64 `json:"diskUsed"`
		DataUsed         float64 `json:"dataUsed"`
		MemUsed          float64 `json:"memUsed"`
		VBActiveNonRes   float64 `json:"vbActiveNumNonResident"`
	} `json:"basicStats"`
	Nodes []struct {
		Hostname string `json:"hostname"`
		Status   string `json:"status"`
	} `json:"nodes"`
	// controllers.flush is present only while flushing is enabled.
	Controllers struct {
		Flush string `json:"flush"`
	} `json:"controllers"`
}

func bucketRef(name string) plugin.ResourceIdentity {
	return plugin.ResourceIdentity{Kind: "bucket", Name: name, UID: name}
}

func (b bucketInfo) health() string {
	if len(b.Nodes) == 0 {
		return "unknown"
	}
	for _, node := range b.Nodes {
		if !strings.EqualFold(node.Status, "healthy") {
			return "degraded"
		}
	}
	return "healthy"
}

func (b bucketInfo) row() row {
	return row{
		"ref":              bucketRef(b.Name),
		"name":             b.Name,
		"health":           b.health(),
		"bucket_type":      b.BucketType,
		"storage_backend":  b.StorageBackend,
		"uuid":             b.UUID,
		"items":            b.BasicStats.ItemCount,
		"opsPerSec":        round1(b.BasicStats.OpsPerSec),
		"diskFetches":      round1(b.BasicStats.DiskFetches),
		"quotaPct":         round1(b.BasicStats.QuotaPercentUsed),
		"quotaTotal":       b.Quota.RAM,
		"memUsed":          b.BasicStats.MemUsed,
		"diskUsed":         b.BasicStats.DiskUsed,
		"dataUsed":         b.BasicStats.DataUsed,
		"replicas":         b.ReplicaNumber,
		"vbuckets":         b.NumVBuckets,
		"eviction_policy":  b.EvictionPolicy,
		"durability":       b.DurabilityLevel,
		"conflict_res":     b.ConflictRes,
		"compression":      b.CompressionMode,
		"max_ttl":          b.MaxTTL,
		"non_resident":     b.BasicStats.VBActiveNonRes,
		"nodes":            len(b.Nodes),
		"reader_writers":   b.ThreadsNumber,
		"quotaPerNodeRaw":  b.Quota.RawRAM,
		"ram_quota_mb":     b.ramQuotaMB(),
		"flush_enabled":    b.Controllers.Flush != "",
		"replica_topology": bucketNodeNames(b),
	}
}

// ramQuotaMB is the per-node quota the management API accepts back.
func (b bucketInfo) ramQuotaMB() int {
	return int(b.Quota.RawRAM / (1024 * 1024))
}

func bucketNodeNames(b bucketInfo) string {
	names := make([]string, 0, len(b.Nodes))
	for _, node := range b.Nodes {
		names = append(names, node.Hostname)
	}
	return strings.Join(names, ", ")
}

func (s *Session) bucketSummaries(ctx context.Context) ([]row, error) {
	var buckets []bucketInfo
	if err := s.mgmtGet(ctx, "/pools/default/buckets", &buckets); err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(buckets))
	for _, bucket := range buckets {
		rows = append(rows, bucket.row())
	}
	return rows, nil
}

func bucketsList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	rows, err := s.bucketSummaries(rc.Ctx)
	if err != nil {
		return nil, err
	}
	return broker.PageRows(rc, rows)
}

func (s *Session) bucket(ctx context.Context, name string) (bucketInfo, error) {
	bucket, err := safeBucket(name)
	if err != nil {
		return bucketInfo{}, err
	}
	var info bucketInfo
	if err := s.mgmtGet(ctx, "/pools/default/buckets/"+url.PathEscape(bucket), &info); err != nil {
		return bucketInfo{}, err
	}
	return info, nil
}

func bucketName(rc *plugin.RequestContext, s *Session) (string, error) {
	name := strings.TrimSpace(rc.Param("bucket"))
	if name == "" {
		name = s.opts.Bucket
	}
	if name == "" {
		return "", fmt.Errorf("%w: a bucket is required; pick one in the bucket selector", plugin.ErrInvalidInput)
	}
	return safeBucket(name)
}

func bucketRead(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	name, err := bucketName(rc, s)
	if err != nil {
		return nil, err
	}
	info, err := s.bucket(rc.Ctx, name)
	if err != nil {
		return nil, err
	}
	item := info.row()
	scopes, err := s.scopes(rc.Ctx, name)
	if err == nil {
		collections := 0
		for _, scope := range scopes.Scopes {
			collections += len(scope.Collections)
		}
		item["scopes"] = len(scopes.Scopes)
		item["collections"] = collections
	}
	return item, nil
}

func bucketCreate(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var in struct {
		Name           string `json:"name" validate:"required"`
		BucketType     string `json:"bucket_type"`
		RAMQuotaMB     int    `json:"ram_quota_mb"`
		ReplicaNumber  int    `json:"replica_number"`
		FlushEnabled   bool   `json:"flush_enabled"`
		EvictionPolicy string `json:"eviction_policy"`
	}
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	name, err := safeBucket(in.Name)
	if err != nil {
		return nil, err
	}
	if in.RAMQuotaMB < 100 {
		in.RAMQuotaMB = 100
	}
	form := url.Values{
		"name":          {name},
		"bucketType":    {stringDefault(in.BucketType, "couchbase")},
		"ramQuotaMB":    {strconv.Itoa(in.RAMQuotaMB)},
		"replicaNumber": {strconv.Itoa(in.ReplicaNumber)},
		"flushEnabled":  {boolForm(in.FlushEnabled)},
	}
	if in.EvictionPolicy != "" {
		form.Set("evictionPolicy", in.EvictionPolicy)
	}
	if err := s.mgmtForm(rc.Ctx, "POST", "/pools/default/buckets", form); err != nil {
		return nil, err
	}
	return row{"ok": true, "name": name, "ref": bucketRef(name)}, nil
}

// bucketUpdate re-posts the bucket definition with the supplied overrides. The
// management API replaces the whole definition, so the untouched settings are
// read back from the cluster first.
func bucketUpdate(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	name, err := bucketName(rc, s)
	if err != nil {
		return nil, err
	}
	current, err := s.bucket(rc.Ctx, name)
	if err != nil {
		return nil, err
	}
	var in struct {
		RAMQuotaMB    *int  `json:"ram_quota_mb"`
		ReplicaNumber *int  `json:"replica_number"`
		FlushEnabled  *bool `json:"flush_enabled"`
	}
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	quota := current.ramQuotaMB()
	if in.RAMQuotaMB != nil {
		quota = *in.RAMQuotaMB
	}
	if quota < 100 {
		return nil, fmt.Errorf("%w: the RAM quota must be at least 100 MB", plugin.ErrInvalidInput)
	}
	replicas := current.ReplicaNumber
	if in.ReplicaNumber != nil {
		replicas = *in.ReplicaNumber
	}
	if replicas < 0 || replicas > 3 {
		return nil, fmt.Errorf("%w: replicas must be between 0 and 3", plugin.ErrInvalidInput)
	}
	flush := current.Controllers.Flush != ""
	if in.FlushEnabled != nil {
		flush = *in.FlushEnabled
	}
	form := url.Values{
		"ramQuotaMB":    {strconv.Itoa(quota)},
		"replicaNumber": {strconv.Itoa(replicas)},
		"flushEnabled":  {boolForm(flush)},
	}
	if err := s.mgmtForm(rc.Ctx, "POST", "/pools/default/buckets/"+url.PathEscape(name), form); err != nil {
		return nil, err
	}
	return row{"ok": true, "name": name, "ref": bucketRef(name)}, nil
}

func bucketDelete(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	name, err := bucketName(rc, s)
	if err != nil {
		return nil, err
	}
	if err := s.mgmtForm(rc.Ctx, "DELETE", "/pools/default/buckets/"+url.PathEscape(name), nil); err != nil {
		return nil, err
	}
	return actionResult{OK: true, Message: "bucket " + name + " deleted"}, nil
}

func bucketFlush(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	name, err := bucketName(rc, s)
	if err != nil {
		return nil, err
	}
	if err := s.mgmtForm(rc.Ctx, "POST", "/pools/default/buckets/"+url.PathEscape(name)+"/controller/doFlush", nil); err != nil {
		return nil, err
	}
	return actionResult{OK: true, Message: "bucket " + name + " flushed"}, nil
}

func bucketCompact(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	name, err := bucketName(rc, s)
	if err != nil {
		return nil, err
	}
	if err := s.mgmtForm(rc.Ctx, "POST", "/pools/default/buckets/"+url.PathEscape(name)+"/controller/compactBucket", nil); err != nil {
		return nil, err
	}
	return actionResult{OK: true, Message: "compaction started for " + name}, nil
}

type bucketSamples map[string][]float64

// last reads the newest sample; the stats API returns a rolling window.
func (b bucketSamples) last(key string) float64 {
	values := b[key]
	if len(values) == 0 {
		return 0
	}
	return values[len(values)-1]
}

func (s *Session) bucketStats(ctx context.Context, bucket string) (bucketSamples, error) {
	var out struct {
		Op struct {
			Samples map[string][]any `json:"samples"`
		} `json:"op"`
	}
	query := url.Values{"zoom": {"minute"}}
	raw, err := s.mgmt.Raw(ctx, "GET", "/pools/default/buckets/"+url.PathEscape(bucket)+"/stats", query, "", nil)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("%w: decode bucket stats: %v", plugin.ErrUnavailable, err)
	}
	samples := bucketSamples{}
	for key, values := range out.Op.Samples {
		converted := make([]float64, 0, len(values))
		for _, value := range values {
			converted = append(converted, numberValue(value))
		}
		samples[key] = converted
	}
	return samples, nil
}

// dcpConsumers folds the flat ep_dcp_<consumer>_<metric> sample keys into one
// row per DCP consumer class (replication, indexing, XDCR, search, ...).
func dcpConsumers(samples bucketSamples) []row {
	consumers := map[string]row{}
	for key := range samples {
		if !strings.HasPrefix(key, "ep_dcp_") {
			continue
		}
		rest := strings.TrimPrefix(key, "ep_dcp_")
		for _, metric := range []string{"items_remaining", "items_sent", "total_bytes", "producer_count", "backoff", "count", "total_data_size"} {
			suffix := "_" + metric
			if !strings.HasSuffix(rest, suffix) {
				continue
			}
			name := strings.TrimSuffix(rest, suffix)
			if name == "" {
				continue
			}
			item, ok := consumers[name]
			if !ok {
				item = row{"consumer": name, "ref": plugin.ResourceIdentity{Kind: "dcp", Name: name, UID: name}}
				consumers[name] = item
			}
			item[metric] = round1(samples.last(key))
			break
		}
	}
	out := make([]row, 0, len(consumers))
	for _, name := range sortedStringKeys(consumers) {
		out = append(out, consumers[name])
	}
	return out
}

func sortedStringKeys(m map[string]row) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func bucketDCP(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	name, err := bucketName(rc, s)
	if err != nil {
		return nil, err
	}
	samples, err := s.bucketStats(rc.Ctx, name)
	if err != nil {
		return nil, err
	}
	return broker.PageRows(rc, dcpConsumers(samples))
}

func bucketMetrics(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := session(rc)
	if err != nil {
		return err
	}
	name, err := bucketName(rc, s)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(client)
	return tick(rc, client, 5*time.Second, func() error {
		frame, err := s.bucketFrame(rc.Ctx, name)
		if err != nil {
			return nil
		}
		return enc.Encode(frame)
	})
}

func (s *Session) bucketFrame(ctx context.Context, name string) (map[string]any, error) {
	info, err := s.bucket(ctx, name)
	if err != nil {
		return nil, err
	}
	frame := map[string]any{
		"items":       info.BasicStats.ItemCount,
		"opsPerSec":   round1(info.BasicStats.OpsPerSec),
		"diskFetches": round1(info.BasicStats.DiskFetches),
		"quotaPct":    round1(info.BasicStats.QuotaPercentUsed),
		"quotaUsed":   info.BasicStats.MemUsed,
		"quotaTotal":  info.Quota.RAM,
		"diskUsed":    info.BasicStats.DiskUsed,
		"dataUsed":    info.BasicStats.DataUsed,
	}
	samples, err := s.bucketStats(ctx, name)
	if err != nil {
		return frame, nil
	}
	remaining, sent := 0.0, 0.0
	for key := range samples {
		switch {
		case strings.HasPrefix(key, "ep_dcp_") && strings.HasSuffix(key, "_items_remaining"):
			remaining += samples.last(key)
		case strings.HasPrefix(key, "ep_dcp_") && strings.HasSuffix(key, "_items_sent"):
			sent += samples.last(key)
		}
	}
	frame["dcpRemaining"] = round1(remaining)
	frame["dcpSent"] = round1(sent)
	frame["cacheMissRate"] = round1(samples.last("ep_cache_miss_rate"))
	frame["residentRatio"] = round1(samples.last("vb_active_resident_items_ratio"))
	frame["queueSize"] = round1(samples.last("ep_queue_size"))
	frame["cmdGet"] = round1(samples.last("cmd_get"))
	frame["cmdSet"] = round1(samples.last("cmd_set"))
	return frame, nil
}

type collectionManifest struct {
	UID    string `json:"uid"`
	Scopes []struct {
		Name        string `json:"name"`
		UID         string `json:"uid"`
		Collections []struct {
			Name    string  `json:"name"`
			UID     string  `json:"uid"`
			MaxTTL  float64 `json:"maxTTL"`
			History bool    `json:"history"`
		} `json:"collections"`
	} `json:"scopes"`
}

func (s *Session) scopes(ctx context.Context, bucket string) (collectionManifest, error) {
	name, err := safeBucket(bucket)
	if err != nil {
		return collectionManifest{}, err
	}
	var manifest collectionManifest
	if err := s.mgmtGet(ctx, "/pools/default/buckets/"+url.PathEscape(name)+"/scopes", &manifest); err != nil {
		return collectionManifest{}, err
	}
	return manifest, nil
}

// collectionStats maps "<scope>/<collection>" to its live counters. Per-collection
// counters exist only in the statistics API, not in the manifest.
type collectionStat struct {
	Items    float64
	MemUsed  float64
	DataSize float64
	Ops      float64
}

func (s *Session) collectionStats(ctx context.Context, bucket string) map[string]collectionStat {
	out := map[string]collectionStat{}
	metrics := map[string]func(*collectionStat, float64){
		"kv_collection_item_count":      func(c *collectionStat, v float64) { c.Items = v },
		"kv_collection_mem_used_bytes":  func(c *collectionStat, v float64) { c.MemUsed = v },
		"kv_collection_data_size_bytes": func(c *collectionStat, v float64) { c.DataSize = v },
		"kv_collection_ops":             func(c *collectionStat, v float64) { c.Ops += v },
	}
	for metric, apply := range metrics {
		series, err := s.statsRange(ctx, metric, bucket, 60)
		if err != nil {
			continue
		}
		for i := range series {
			scope := series[i].Metric["scope"]
			collection := series[i].Metric["collection"]
			if scope == "" || collection == "" {
				continue
			}
			value, ok := series[i].last()
			if !ok {
				continue
			}
			key := scope + "/" + collection
			stat := out[key]
			apply(&stat, value)
			out[key] = stat
		}
	}
	return out
}

func scopeRef(bucket, scope string) plugin.ResourceIdentity {
	k := keyspace{Bucket: bucket, Scope: scope}
	return plugin.ResourceIdentity{Kind: "scope", Scope: bucket, Name: scope, UID: k.uid()}
}

func collectionRef(bucket, scope, collection string) plugin.ResourceIdentity {
	k := keyspace{Bucket: bucket, Scope: scope, Collection: collection}
	return plugin.ResourceIdentity{Kind: "collection", Scope: bucket, Namespace: scope, Name: collection, UID: k.uid()}
}

func (s *Session) scopeRows(ctx context.Context, bucket string) ([]row, error) {
	manifest, err := s.scopes(ctx, bucket)
	if err != nil {
		return nil, err
	}
	stats := s.collectionStats(ctx, bucket)
	rows := make([]row, 0, len(manifest.Scopes))
	for _, scope := range manifest.Scopes {
		items, memUsed := 0.0, 0.0
		for _, collection := range scope.Collections {
			stat := stats[scope.Name+"/"+collection.Name]
			items += stat.Items
			memUsed += stat.MemUsed
		}
		rows = append(rows, row{
			"ref":         scopeRef(bucket, scope.Name),
			"name":        scope.Name,
			"bucket":      bucket,
			"uid":         scope.UID,
			"collections": len(scope.Collections),
			"items":       items,
			"memUsed":     memUsed,
			"system":      strings.HasPrefix(scope.Name, "_"),
		})
	}
	return rows, nil
}

func scopesList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	bucket, err := bucketName(rc, s)
	if err != nil {
		return nil, err
	}
	rows, err := s.scopeRows(rc.Ctx, bucket)
	if err != nil {
		return nil, err
	}
	return broker.PageRows(rc, rows)
}

func scopeRead(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	k, err := keyspaceFrom(rc, s)
	if err != nil {
		return nil, err
	}
	if k.Scope == "" {
		return nil, fmt.Errorf("%w: a scope is required", plugin.ErrInvalidInput)
	}
	manifest, err := s.scopes(rc.Ctx, k.Bucket)
	if err != nil {
		return nil, err
	}
	stats := s.collectionStats(rc.Ctx, k.Bucket)
	for _, scope := range manifest.Scopes {
		if scope.Name != k.Scope {
			continue
		}
		items, memUsed, dataSize := 0.0, 0.0, 0.0
		names := make([]string, 0, len(scope.Collections))
		for _, collection := range scope.Collections {
			stat := stats[scope.Name+"/"+collection.Name]
			items += stat.Items
			memUsed += stat.MemUsed
			dataSize += stat.DataSize
			names = append(names, collection.Name)
		}
		return row{
			"ref":              scopeRef(k.Bucket, scope.Name),
			"name":             scope.Name,
			"bucket":           k.Bucket,
			"uid":              scope.UID,
			"collections":      len(scope.Collections),
			"collection_names": strings.Join(names, ", "),
			"items":            items,
			"memUsed":          memUsed,
			"dataSize":         dataSize,
			"system":           strings.HasPrefix(scope.Name, "_"),
			"keyspace":         k.label(),
		}, nil
	}
	return nil, fmt.Errorf("%w: scope %q not found in bucket %q", plugin.ErrNotFound, k.Scope, k.Bucket)
}

func scopeCreate(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	bucket, err := bucketName(rc, s)
	if err != nil {
		return nil, err
	}
	var in struct {
		Name string `json:"name" validate:"required"`
	}
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	name, err := safeScope(in.Name, "scope")
	if err != nil {
		return nil, err
	}
	if err := s.mgmtForm(rc.Ctx, "POST", "/pools/default/buckets/"+url.PathEscape(bucket)+"/scopes", url.Values{"name": {name}}); err != nil {
		return nil, err
	}
	return row{"ok": true, "name": name, "ref": scopeRef(bucket, name)}, nil
}

func scopeDelete(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	k, err := keyspaceFrom(rc, s)
	if err != nil {
		return nil, err
	}
	if k.Scope == "" {
		return nil, fmt.Errorf("%w: a scope is required", plugin.ErrInvalidInput)
	}
	path := "/pools/default/buckets/" + url.PathEscape(k.Bucket) + "/scopes/" + url.PathEscape(k.Scope)
	if err := s.mgmtForm(rc.Ctx, "DELETE", path, nil); err != nil {
		return nil, err
	}
	return actionResult{OK: true, Message: "scope " + k.label() + " dropped"}, nil
}

func (s *Session) collectionRows(ctx context.Context, k keyspace) ([]row, error) {
	manifest, err := s.scopes(ctx, k.Bucket)
	if err != nil {
		return nil, err
	}
	stats := s.collectionStats(ctx, k.Bucket)
	rows := make([]row, 0, 16)
	for _, scope := range manifest.Scopes {
		if k.Scope != "" && scope.Name != k.Scope {
			continue
		}
		for _, collection := range scope.Collections {
			stat := stats[scope.Name+"/"+collection.Name]
			rows = append(rows, row{
				"ref":       collectionRef(k.Bucket, scope.Name, collection.Name),
				"name":      collection.Name,
				"scope":     scope.Name,
				"bucket":    k.Bucket,
				"uid":       collection.UID,
				"items":     stat.Items,
				"memUsed":   stat.MemUsed,
				"dataSize":  stat.DataSize,
				"opsPerSec": round1(stat.Ops),
				"max_ttl":   collection.MaxTTL,
				"history":   collection.History,
				"system":    strings.HasPrefix(scope.Name, "_"),
			})
		}
	}
	return rows, nil
}

func collectionsList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	k, err := keyspaceFrom(rc, s)
	if err != nil {
		return nil, err
	}
	rows, err := s.collectionRows(rc.Ctx, k)
	if err != nil {
		return nil, err
	}
	return broker.PageRows(rc, rows)
}

func collectionRead(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	k, err := keyspaceFrom(rc, s)
	if err != nil {
		return nil, err
	}
	if k.Collection == "" {
		return nil, fmt.Errorf("%w: a collection is required", plugin.ErrInvalidInput)
	}
	manifest, err := s.scopes(rc.Ctx, k.Bucket)
	if err != nil {
		return nil, err
	}
	stats := s.collectionStats(rc.Ctx, k.Bucket)
	for _, scope := range manifest.Scopes {
		if scope.Name != k.Scope {
			continue
		}
		for _, collection := range scope.Collections {
			if collection.Name != k.Collection {
				continue
			}
			stat := stats[scope.Name+"/"+collection.Name]
			indexes, _ := s.collectionIndexes(rc.Ctx, k)
			return row{
				"ref":       collectionRef(k.Bucket, scope.Name, collection.Name),
				"name":      collection.Name,
				"scope":     scope.Name,
				"bucket":    k.Bucket,
				"keyspace":  k.label(),
				"path":      k.path(),
				"uid":       collection.UID,
				"items":     stat.Items,
				"memUsed":   stat.MemUsed,
				"dataSize":  stat.DataSize,
				"opsPerSec": round1(stat.Ops),
				"max_ttl":   collection.MaxTTL,
				"history":   collection.History,
				"indexes":   len(indexes),
			}, nil
		}
	}
	return nil, fmt.Errorf("%w: collection %q not found", plugin.ErrNotFound, k.label())
}

func collectionCreate(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	k, err := keyspaceFrom(rc, s)
	if err != nil {
		return nil, err
	}
	if k.Scope == "" {
		return nil, fmt.Errorf("%w: a scope is required", plugin.ErrInvalidInput)
	}
	var in struct {
		Name   string `json:"name" validate:"required"`
		MaxTTL int    `json:"max_ttl"`
	}
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	name, err := safeScope(in.Name, "collection")
	if err != nil {
		return nil, err
	}
	form := url.Values{"name": {name}}
	if in.MaxTTL != 0 {
		form.Set("maxTTL", strconv.Itoa(in.MaxTTL))
	}
	path := "/pools/default/buckets/" + url.PathEscape(k.Bucket) + "/scopes/" + url.PathEscape(k.Scope) + "/collections"
	if err := s.mgmtForm(rc.Ctx, "POST", path, form); err != nil {
		return nil, err
	}
	return row{"ok": true, "name": name, "ref": collectionRef(k.Bucket, k.Scope, name)}, nil
}

func collectionDelete(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	k, err := keyspaceFrom(rc, s)
	if err != nil {
		return nil, err
	}
	if k.Collection == "" {
		return nil, fmt.Errorf("%w: a collection is required", plugin.ErrInvalidInput)
	}
	path := "/pools/default/buckets/" + url.PathEscape(k.Bucket) +
		"/scopes/" + url.PathEscape(k.Scope) +
		"/collections/" + url.PathEscape(k.Collection)
	if err := s.mgmtForm(rc.Ctx, "DELETE", path, nil); err != nil {
		return nil, err
	}
	return actionResult{OK: true, Message: "collection " + k.label() + " dropped"}, nil
}

func stringDefault(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}

func boolForm(v bool) string {
	if v {
		return "1"
	}
	return "0"
}
