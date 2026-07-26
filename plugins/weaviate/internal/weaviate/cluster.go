package weaviate

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/go-openapi/strfmt"
	wvalias "github.com/weaviate/weaviate-go-client/v5/weaviate/alias"
	"github.com/weaviate/weaviate/entities/models"
)

const metricsInterval = 5 * time.Second

// backupBackends is probed in order; backends the server has not enabled simply
// return an error and are skipped, so one list shows every reachable backend.
var backupBackends = []string{"filesystem", "s3", "gcs", "azure"}

type classStats struct {
	Objects int64  `json:"objects"`
	Shards  int    `json:"shards"`
	Status  string `json:"status"`
}

type clusterSnapshot struct {
	Nodes      int
	Healthy    int
	Shards     int
	Objects    int64
	BatchQueue int64
	Rate       int64
	Classes    map[string]classStats
	Statuses   []*models.NodeStatus
}

func (c *clusterSnapshot) HealthyPct() float64 {
	if c == nil || c.Nodes == 0 {
		return 0
	}
	return round(float64(c.Healthy)/float64(c.Nodes)*100, 1)
}

func clusterStats(rc *plugin.RequestContext) (*clusterSnapshot, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.cluster.NodesStatusGetter().WithOutput("verbose").Do(rc.Ctx)
	if err != nil {
		return nil, mapError(err)
	}
	return snapshotOf(resp), nil
}

func snapshotOf(resp *models.NodesStatusResponse) *clusterSnapshot {
	out := &clusterSnapshot{Classes: map[string]classStats{}}
	if resp == nil {
		return out
	}
	out.Statuses = resp.Nodes
	for _, node := range resp.Nodes {
		if node == nil {
			continue
		}
		out.Nodes++
		if node.Status != nil && strings.EqualFold(*node.Status, "healthy") {
			out.Healthy++
		}
		if node.Stats != nil {
			out.Objects += node.Stats.ObjectCount
			out.Shards += int(node.Stats.ShardCount)
		}
		if node.BatchStats != nil {
			if node.BatchStats.QueueLength != nil {
				out.BatchQueue += *node.BatchStats.QueueLength
			}
			out.Rate += node.BatchStats.RatePerSecond
		}
		for _, shard := range node.Shards {
			if shard == nil {
				continue
			}
			entry := out.Classes[shard.Class]
			entry.Objects += shard.ObjectCount
			entry.Shards++
			entry.Status = worseIndexingStatus(entry.Status, shard.VectorIndexingStatus)
			out.Classes[shard.Class] = entry
		}
	}
	return out
}

func worseIndexingStatus(current, candidate string) string {
	if candidate == "" {
		return current
	}
	if current == "" || strings.EqualFold(current, "READY") {
		return strings.ToUpper(candidate)
	}
	return current
}

func metricsStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	enc := json.NewEncoder(client)
	node := strings.TrimSpace(rc.Param("node"))
	ticker := time.NewTicker(metricsInterval)
	defer ticker.Stop()
	for {
		if err := enc.Encode(metricsFrame(rc, node)); err != nil {
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

func metricsFrame(rc *plugin.RequestContext, node string) row {
	stats, err := clusterStats(rc)
	if err != nil {
		return row{"error": err.Error()}
	}
	if node != "" {
		return nodeMetricsFrame(stats, node)
	}
	return row{
		"nodes":        stats.Nodes,
		"healthyNodes": stats.Healthy,
		"nodesPct":     stats.HealthyPct(),
		"shards":       stats.Shards,
		"objects":      stats.Objects,
		"batchQueue":   stats.BatchQueue,
		"rate":         stats.Rate,
		"collections":  len(stats.Classes),
	}
}

func nodeMetricsFrame(stats *clusterSnapshot, name string) row {
	for _, status := range stats.Statuses {
		if status == nil || status.Name != name {
			continue
		}
		frame := row{
			"nodes":        1,
			"healthyNodes": map[bool]int{true: 1, false: 0}[status.Status != nil && strings.EqualFold(*status.Status, "healthy")],
			"collections":  len(nodeClasses(status)),
		}
		frame["nodesPct"] = round(float64(frame["healthyNodes"].(int))*100, 1)
		if status.Stats != nil {
			frame["objects"] = status.Stats.ObjectCount
			frame["shards"] = status.Stats.ShardCount
		}
		if status.BatchStats != nil {
			if status.BatchStats.QueueLength != nil {
				frame["batchQueue"] = *status.BatchStats.QueueLength
			}
			frame["rate"] = status.BatchStats.RatePerSecond
		}
		return frame
	}
	return row{"error": "node " + name + " is not reporting"}
}

func nodeClasses(status *models.NodeStatus) []string {
	seen := map[string]bool{}
	for _, shard := range status.Shards {
		if shard != nil {
			seen[shard.Class] = true
		}
	}
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func listNodes(rc *plugin.RequestContext) (any, error) {
	stats, err := clusterStats(rc)
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(stats.Statuses))
	for _, status := range stats.Statuses {
		if status == nil {
			continue
		}
		rows = append(rows, nodeRow(status))
	}
	sort.Slice(rows, func(i, j int) bool { return text(rows[i]["name"]) < text(rows[j]["name"]) })
	return broker.PageRows(rc, rows)
}

func nodeRow(status *models.NodeStatus) row {
	item := row{
		"ref":             plugin.ResourceIdentity{Kind: "node", Name: status.Name, UID: status.Name},
		"name":            status.Name,
		"status":          strings.ToUpper(stringOf(status.Status)),
		"version":         status.Version,
		"gitHash":         status.GitHash,
		"operationalMode": status.OperationalMode,
		"shards":          len(status.Shards),
		"collections":     len(nodeClasses(status)),
	}
	if status.Stats != nil {
		item["objects"] = status.Stats.ObjectCount
	}
	if status.BatchStats != nil {
		item["rate"] = status.BatchStats.RatePerSecond
		if status.BatchStats.QueueLength != nil {
			item["batchQueue"] = *status.BatchStats.QueueLength
		}
	}
	return item
}

func readNode(rc *plugin.RequestContext) (any, error) {
	name, err := validName(rc.Param("node"), simpleNamePattern, "node")
	if err != nil {
		return nil, err
	}
	stats, err := clusterStats(rc)
	if err != nil {
		return nil, err
	}
	for _, status := range stats.Statuses {
		if status != nil && status.Name == name {
			item := nodeRow(status)
			item["classes"] = nodeClasses(status)
			return item, nil
		}
	}
	return nil, fmt.Errorf("%w: node %q", plugin.ErrNotFound, name)
}

func listNodeShards(rc *plugin.RequestContext) (any, error) {
	name, err := validName(rc.Param("node"), simpleNamePattern, "node")
	if err != nil {
		return nil, err
	}
	stats, err := clusterStats(rc)
	if err != nil {
		return nil, err
	}
	for _, status := range stats.Statuses {
		if status == nil || status.Name != name {
			continue
		}
		rows := make([]row, 0, len(status.Shards))
		for _, shard := range status.Shards {
			if shard == nil {
				continue
			}
			rows = append(rows, row{
				"name":                 shard.Name,
				"collection":           shard.Class,
				"objects":              shard.ObjectCount,
				"vectorIndexingStatus": strings.ToUpper(shard.VectorIndexingStatus),
				"compressed":           shard.Compressed,
				"loaded":               shard.Loaded,
				"replicas":             shard.NumberOfReplicas,
				"replicationFactor":    shard.ReplicationFactor,
			})
		}
		sort.Slice(rows, func(i, j int) bool { return text(rows[i]["name"]) < text(rows[j]["name"]) })
		return broker.PageRows(rc, rows)
	}
	return nil, fmt.Errorf("%w: node %q", plugin.ErrNotFound, name)
}

func listShards(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	name, err := collectionParam(rc)
	if err != nil {
		return nil, err
	}
	shards, err := s.client.schema.ShardsGetter().WithClassName(name).Do(rc.Ctx)
	if err != nil {
		return nil, mapError(err)
	}
	stats, _ := clusterStats(rc)
	objectsByShard := map[string]int64{}
	nodeByShard := map[string]string{}
	if stats != nil {
		for _, status := range stats.Statuses {
			if status == nil {
				continue
			}
			for _, shard := range status.Shards {
				if shard != nil && shard.Class == name {
					objectsByShard[shard.Name] = shard.ObjectCount
					nodeByShard[shard.Name] = status.Name
				}
			}
		}
	}
	rows := make([]row, 0, len(shards))
	for _, shard := range shards {
		if shard == nil {
			continue
		}
		rows = append(rows, row{
			"name":            shard.Name,
			"collection":      name,
			"status":          strings.ToUpper(shard.Status),
			"vectorQueueSize": shard.VectorQueueSize,
			"objects":         objectsByShard[shard.Name],
			"node":            nodeByShard[shard.Name],
		})
	}
	sort.Slice(rows, func(i, j int) bool { return text(rows[i]["name"]) < text(rows[j]["name"]) })
	return broker.PageRows(rc, rows)
}

func updateShard(rc *plugin.RequestContext) (any, error) {
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
	shard, err := validName(rc.Param("shard"), simpleNamePattern, "shard")
	if err != nil {
		return nil, err
	}
	var req struct {
		Status string `json:"status" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	status := strings.ToUpper(strings.TrimSpace(req.Status))
	if status != "READY" && status != "READONLY" {
		return nil, fmt.Errorf("%w: shard status must be READY or READONLY", plugin.ErrInvalidInput)
	}
	updated, err := s.client.schema.ShardUpdater().WithClassName(name).WithShardName(shard).WithStatus(status).Do(rc.Ctx)
	if err != nil {
		return nil, mapError(err)
	}
	record(rc, "shards", plugin.SeverityWarn, "Shard status changed", name+"/"+shard+" -> "+status)
	result := row{"ok": true, "collection": name, "name": shard, "status": status}
	if updated != nil {
		result["status"] = strings.ToUpper(updated.Status)
	}
	return result, nil
}

func listTenants(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	name, err := collectionParam(rc)
	if err != nil {
		return nil, err
	}
	tenants, err := s.client.schema.TenantsGetter().WithClassName(name).Do(rc.Ctx)
	if err != nil {
		return nil, mapError(err)
	}
	rows := make([]row, 0, len(tenants))
	for _, tenant := range tenants {
		rows = append(rows, row{
			"name":           tenant.Name,
			"collection":     name,
			"activityStatus": strings.ToUpper(defaultString(tenant.ActivityStatus, "ACTIVE")),
		})
	}
	sort.Slice(rows, func(i, j int) bool { return text(rows[i]["name"]) < text(rows[j]["name"]) })
	return broker.PageRows(rc, rows)
}

func createTenant(rc *plugin.RequestContext) (any, error) {
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
		Name           string `json:"name" validate:"required"`
		ActivityStatus string `json:"activity_status"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	tenantName, err := validName(req.Name, simpleNamePattern, "tenant")
	if err != nil {
		return nil, err
	}
	tenant := models.Tenant{Name: tenantName, ActivityStatus: strings.ToUpper(defaultString(req.ActivityStatus, "ACTIVE"))}
	if err := s.client.schema.TenantsCreator().WithClassName(name).WithTenants(tenant).Do(rc.Ctx); err != nil {
		return nil, mapError(err)
	}
	record(rc, "tenants", plugin.SeveritySuccess, "Tenant created", name+"/"+tenantName)
	return row{"ok": true, "name": tenantName, "collection": name}, nil
}

func updateTenant(rc *plugin.RequestContext) (any, error) {
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
	tenantName, err := validName(rc.Param("tenant"), simpleNamePattern, "tenant")
	if err != nil {
		return nil, err
	}
	var req struct {
		ActivityStatus string `json:"activity_status" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	status := strings.ToUpper(strings.TrimSpace(req.ActivityStatus))
	switch status {
	case "ACTIVE", "INACTIVE", "OFFLOADED":
	default:
		return nil, fmt.Errorf("%w: activity status must be ACTIVE, INACTIVE, or OFFLOADED", plugin.ErrInvalidInput)
	}
	tenant := models.Tenant{Name: tenantName, ActivityStatus: status}
	if err := s.client.schema.TenantsUpdater().WithClassName(name).WithTenants(tenant).Do(rc.Ctx); err != nil {
		return nil, mapError(err)
	}
	record(rc, "tenants", plugin.SeverityInfo, "Tenant status changed", name+"/"+tenantName+" -> "+status)
	return row{"ok": true, "name": tenantName, "collection": name, "activityStatus": status}, nil
}

func deleteTenant(rc *plugin.RequestContext) (any, error) {
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
	tenantName, err := validName(rc.Param("tenant"), simpleNamePattern, "tenant")
	if err != nil {
		return nil, err
	}
	if err := s.client.schema.TenantsDeleter().WithClassName(name).WithTenants(tenantName).Do(rc.Ctx); err != nil {
		return nil, mapError(err)
	}
	record(rc, "tenants", plugin.SeverityDanger, "Tenant deleted", name+"/"+tenantName)
	return row{"ok": true, "name": tenantName, "collection": name}, nil
}

func listAliases(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	getter := s.client.alias.Getter()
	if scope := strings.TrimSpace(rc.Param("collection")); scope != "" {
		name, err := validName(scope, classNamePattern, "collection")
		if err != nil {
			return nil, err
		}
		getter = getter.WithClassName(name)
	}
	aliases, err := getter.Do(rc.Ctx)
	if err != nil {
		return nil, mapError(err)
	}
	rows := make([]row, 0, len(aliases))
	for _, item := range aliases {
		rows = append(rows, aliasRow(item))
	}
	sort.Slice(rows, func(i, j int) bool { return text(rows[i]["name"]) < text(rows[j]["name"]) })
	return broker.PageRows(rc, rows)
}

func aliasRow(item wvalias.Alias) row {
	return row{
		"ref":        plugin.ResourceIdentity{Kind: "alias", Name: item.Alias, UID: item.Alias},
		"name":       item.Alias,
		"collection": item.Class,
	}
}

func readAlias(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	name, err := validName(rc.Param("alias"), classNamePattern, "alias")
	if err != nil {
		return nil, err
	}
	item, err := s.client.alias.AliasGetter().WithAliasName(name).Do(rc.Ctx)
	if err != nil {
		return nil, mapError(err)
	}
	if item == nil {
		return nil, fmt.Errorf("%w: alias %q", plugin.ErrNotFound, name)
	}
	return aliasRow(*item), nil
}

func createAlias(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Name       string `json:"name" validate:"required"`
		Collection string `json:"collection" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name, err := validName(req.Name, classNamePattern, "alias")
	if err != nil {
		return nil, err
	}
	target, err := validName(req.Collection, classNamePattern, "collection")
	if err != nil {
		return nil, err
	}
	if err := s.client.alias.AliasCreator().WithAlias(&wvalias.Alias{Alias: name, Class: target}).Do(rc.Ctx); err != nil {
		return nil, mapError(err)
	}
	record(rc, "aliases", plugin.SeveritySuccess, "Alias created", name+" -> "+target)
	return row{"ok": true, "name": name, "collection": target}, nil
}

func updateAlias(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	name, err := validName(rc.Param("alias"), classNamePattern, "alias")
	if err != nil {
		return nil, err
	}
	var req struct {
		Collection string `json:"collection" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	target, err := validName(req.Collection, classNamePattern, "collection")
	if err != nil {
		return nil, err
	}
	if err := s.client.alias.AliasUpdater().WithAlias(&wvalias.Alias{Alias: name, Class: target}).Do(rc.Ctx); err != nil {
		return nil, mapError(err)
	}
	record(rc, "aliases", plugin.SeverityInfo, "Alias retargeted", name+" -> "+target)
	return row{"ok": true, "name": name, "collection": target}, nil
}

func deleteAlias(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	name, err := validName(rc.Param("alias"), classNamePattern, "alias")
	if err != nil {
		return nil, err
	}
	if err := s.client.alias.AliasDeleter().WithAliasName(name).Do(rc.Ctx); err != nil {
		return nil, mapError(err)
	}
	record(rc, "aliases", plugin.SeverityDanger, "Alias deleted", name)
	return row{"ok": true, "name": name}, nil
}

func listBackups(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	backends := backupBackends
	if scoped := strings.TrimSpace(rc.Param("backend")); scoped != "" {
		backend, err := backupBackendName(scoped)
		if err != nil {
			return nil, err
		}
		backends = []string{backend}
	}
	rows := make([]row, 0, 8)
	for _, backend := range backends {
		items, err := s.client.backup.Lister().WithBackend(backend).Do(rc.Ctx)
		if err != nil {
			continue
		}
		for _, item := range items {
			if item == nil {
				continue
			}
			rows = append(rows, row{
				"ref":         plugin.ResourceIdentity{Kind: "backup", Namespace: backend, Name: item.ID, UID: item.ID},
				"id":          item.ID,
				"backend":     backend,
				"status":      strings.ToUpper(item.Status),
				"collections": item.Classes,
				"size":        item.Size,
				"startedAt":   dateTime(item.StartedAt),
				"completedAt": dateTime(item.CompletedAt),
			})
		}
	}
	sort.Slice(rows, func(i, j int) bool { return text(rows[i]["startedAt"]) > text(rows[j]["startedAt"]) })
	return broker.PageRows(rc, rows)
}

func readBackup(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	backend, id, err := backupTarget(rc)
	if err != nil {
		return nil, err
	}
	status, err := s.client.backup.CreateStatusGetter().WithBackend(backend).WithBackupID(id).Do(rc.Ctx)
	if err != nil {
		return nil, mapError(err)
	}
	out := row{
		"id":          status.ID,
		"backend":     status.Backend,
		"status":      strings.ToUpper(stringOf(status.Status)),
		"path":        status.Path,
		"size":        status.Size,
		"startedAt":   dateTime(status.StartedAt),
		"completedAt": dateTime(status.CompletedAt),
		"error":       status.Error,
	}
	if restore, err := s.client.backup.RestoreStatusGetter().WithBackend(backend).WithBackupID(id).Do(rc.Ctx); err == nil && restore != nil {
		out["restoreStatus"] = strings.ToUpper(stringOf(restore.Status))
		out["restoreError"] = restore.Error
	}
	return out, nil
}

func createBackup(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		ID          string   `json:"id" validate:"required"`
		Backend     string   `json:"backend" validate:"required"`
		Collections []string `json:"collections"`
		Wait        bool     `json:"wait"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	id, err := validName(req.ID, simpleNamePattern, "backup id")
	if err != nil {
		return nil, err
	}
	backend, err := backupBackendName(req.Backend)
	if err != nil {
		return nil, err
	}
	creator := s.client.backup.Creator().WithBackend(backend).WithBackupID(strings.ToLower(id)).WithWaitForCompletion(req.Wait)
	if names, err := validClassNames(req.Collections); err != nil {
		return nil, err
	} else if len(names) > 0 {
		creator = creator.WithIncludeClassNames(names...)
	}
	resp, err := creator.Do(rc.Ctx)
	if err != nil {
		return nil, mapError(err)
	}
	record(rc, "backups", plugin.SeveritySuccess, "Backup started", backend+"/"+id)
	return row{"ok": true, "id": resp.ID, "backend": resp.Backend, "status": strings.ToUpper(stringOf(resp.Status)), "path": resp.Path}, nil
}

func restoreBackup(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	backend, id, err := backupTarget(rc)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.backup.Restorer().WithBackend(backend).WithBackupID(id).WithWaitForCompletion(true).Do(rc.Ctx)
	if err != nil {
		return nil, mapError(err)
	}
	if resp != nil && resp.Error != "" {
		return nil, fmt.Errorf("%w: %s", plugin.ErrUnavailable, resp.Error)
	}
	record(rc, "backups", plugin.SeverityWarn, "Backup restored", backend+"/"+id)
	return row{"ok": true, "id": resp.ID, "backend": resp.Backend, "status": strings.ToUpper(stringOf(resp.Status)), "collections": resp.Classes}, nil
}

func cancelBackup(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	backend, id, err := backupTarget(rc)
	if err != nil {
		return nil, err
	}
	if err := s.client.backup.Canceler().WithBackend(backend).WithBackupID(id).Do(rc.Ctx); err != nil {
		return nil, mapError(err)
	}
	record(rc, "backups", plugin.SeverityWarn, "Backup cancelled", backend+"/"+id)
	return row{"ok": true, "id": id, "backend": backend}, nil
}

func backupTarget(rc *plugin.RequestContext) (string, string, error) {
	backend, err := backupBackendName(rc.Param("backend"))
	if err != nil {
		return "", "", err
	}
	id, err := validName(rc.Param("backup"), simpleNamePattern, "backup id")
	if err != nil {
		return "", "", err
	}
	return backend, id, nil
}

func backupBackendName(raw string) (string, error) {
	backend := strings.ToLower(strings.TrimSpace(raw))
	for _, candidate := range backupBackends {
		if candidate == backend {
			return backend, nil
		}
	}
	return "", fmt.Errorf("%w: backup backend must be one of %s", plugin.ErrInvalidInput, strings.Join(backupBackends, ", "))
}

func validClassNames(names []string) ([]string, error) {
	out := make([]string, 0, len(names))
	for _, name := range names {
		if strings.TrimSpace(name) == "" {
			continue
		}
		valid, err := validName(name, classNamePattern, "collection")
		if err != nil {
			return nil, err
		}
		out = append(out, valid)
	}
	return out, nil
}

func stringOf(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func dateTime(value strfmt.DateTime) string {
	stamp := time.Time(value)
	if stamp.IsZero() {
		return ""
	}
	return stamp.UTC().Format(time.RFC3339)
}
