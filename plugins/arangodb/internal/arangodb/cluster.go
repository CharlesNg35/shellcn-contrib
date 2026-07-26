package arangodb

import (
	"context"
	"sort"
	"strings"
	"time"

	driver "github.com/arangodb/go-driver/v2/arangodb"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

// clusterList is the single-row resource behind the "Cluster & server" tree
// entry: one clickable overview instead of a list of nothing.
func clusterList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	overview, err := s.clusterOverview(rc.Ctx)
	if err != nil {
		return nil, err
	}
	overview["ref"] = plugin.ResourceIdentity{Kind: kindCluster, Name: "overview", UID: "overview"}
	return pageRows(rc, []row{overview})
}

func clusterRead(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	return s.clusterOverview(rc.Ctx)
}

func (s *Session) clusterOverview(ctx context.Context) (row, error) {
	tctx, cancel := context.WithTimeout(ctx, s.opts.Timeout)
	defer cancel()
	version, err := s.client.Version(tctx)
	if err != nil {
		return nil, arangoErr(err)
	}
	out := row{
		"name":       "ArangoDB deployment",
		"endpoint":   s.opts.Endpoint,
		"server":     version.Server,
		"version":    string(version.Version),
		"license":    version.License,
		"deployment": "single",
		"role":       "SINGLE",
		"status":     "unknown",
		"read_only":  s.opts.ReadOnly,
	}
	if role, err := s.client.ServerRole(tctx); err == nil {
		out["role"] = string(role)
		if role == driver.ServerRoleCoordinator || role == driver.ServerRoleDBServer || role == driver.ServerRoleAgent {
			out["deployment"] = "cluster"
		}
	}
	if id, err := s.client.ServerID(tctx); err == nil {
		out["server_id"] = id
	}
	if status, err := s.client.GetServerStatus(tctx, s.opts.Database); err == nil {
		out["status"] = strings.ToLower(stringOrEmpty(status.ServerInfo.State))
		out["host"] = stringOrEmpty(status.Hostname)
		out["pid"] = intOrZero(status.Pid)
		out["mode"] = stringOrEmpty(status.Mode)
		out["maintenance"] = boolOrFalse(status.ServerInfo.Maintenance)
		out["server_read_only"] = boolOrFalse(status.ServerInfo.ReadOnly)
		out["foxx_api"] = boolOrFalse(status.FoxxApi)
	}
	if mode, err := s.client.ServerMode(tctx); err == nil {
		out["server_mode"] = string(mode)
	}
	health, healthErr := s.client.Health(tctx)
	if healthErr == nil {
		out["deployment"] = "cluster"
		out["cluster_id"] = health.ID
		good, total := 0, 0
		for _, server := range health.Health {
			total++
			if server.Status == driver.ServerStatusGood {
				good++
			}
		}
		out["servers_total"] = total
		out["servers_good"] = good
		out["servers_percent"] = percent(good, total)
		out["status"] = clusterStatus(good, total)
	} else {
		out["servers_total"] = 1
		out["servers_good"] = 1
		out["servers_percent"] = 100.0
	}
	if counts, err := s.client.NumberOfServers(tctx); err == nil {
		out["coordinators"] = counts.NoCoordinators
		out["db_servers"] = counts.NoDBServers
		out["cleaned_servers"] = len(counts.CleanedServerIDs)
	}
	if endpoints, err := s.client.ClusterEndpoints(tctx); err == nil {
		list := make([]string, 0, len(endpoints.Endpoints))
		for _, endpoint := range endpoints.Endpoints {
			list = append(list, stringOrEmpty(endpoint.Endpoint))
		}
		out["endpoints"] = joinStrings(list)
	}
	if databases, err := s.client.Databases(tctx); err == nil {
		out["databases"] = len(databases)
	}
	return out, nil
}

// clusterServers lists every known server. A single server deployment has no
// health endpoint, so it degrades to one synthetic row built from server info.
func clusterServers(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	health, err := s.client.Health(ctx)
	if err != nil {
		version, versionErr := s.client.Version(ctx)
		if versionErr != nil {
			return nil, arangoErr(versionErr)
		}
		role := "SINGLE"
		if serverRole, err := s.client.ServerRole(ctx); err == nil {
			role = string(serverRole)
		}
		return pageRows(rc, []row{{
			"id": "single", "short_name": "single", "role": role, "status": "GOOD",
			"endpoint": s.opts.Endpoint, "version": string(version.Version), "sync_status": "SERVING",
			"can_be_deleted": false, "last_heartbeat": "",
		}})
	}
	rows := make([]row, 0, len(health.Health))
	for _, id := range sortedServerIDs(health.Health) {
		server := health.Health[id]
		rows = append(rows, row{
			"id":             string(id),
			"short_name":     server.ShortName,
			"role":           string(server.Role),
			"status":         string(server.Status),
			"endpoint":       server.Endpoint,
			"version":        string(server.Version),
			"engine":         string(server.Engine),
			"sync_status":    string(server.SyncStatus),
			"can_be_deleted": server.CanBeDeleted,
			"last_heartbeat": heartbeat(server.LastHeartbeatAcked),
			"leader":         stringOrEmpty(server.Leader),
		})
	}
	return pageRows(rc, rows)
}

func sortedServerIDs(health map[driver.ServerID]driver.ServerHealth) []driver.ServerID {
	ids := make([]driver.ServerID, 0, len(health))
	for id := range health {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func heartbeat(at time.Time) string {
	if at.IsZero() {
		return ""
	}
	return at.UTC().Format(time.RFC3339)
}

func clusterStatus(good, total int) string {
	switch {
	case total == 0:
		return "unknown"
	case good == total:
		return "good"
	case good == 0:
		return "failed"
	default:
		return "degraded"
	}
}

func percent(part, total int) float64 {
	if total == 0 {
		return 0
	}
	return float64(part) / float64(total) * 100
}
