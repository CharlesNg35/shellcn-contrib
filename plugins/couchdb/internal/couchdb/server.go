package couchdb

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

func serverRef() plugin.ResourceIdentity {
	return plugin.ResourceIdentity{Kind: "server", Name: "Server", UID: "server"}
}

// listServer is the one-row connection-level resource that the sidebar's Server
// node opens.
func listServer(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var welcome row
	if err := s.client.do(rc.Ctx, http.MethodGet, "/", nil, nil, &welcome); err != nil {
		return nil, err
	}
	names := s.databaseNames(rc)
	item := row{
		"name":      s.opts.addr(),
		"version":   stringOf(welcome["version"]),
		"vendor":    vendorName(welcome),
		"status":    s.upStatus(rc),
		"databases": len(names),
		"ref":       serverRef(),
	}
	return plugin.Page[row]{Items: []row{item}}, nil
}

func vendorName(welcome row) string {
	if vendor := mapOf(welcome["vendor"]); vendor != nil {
		return stringOf(vendor["name"])
	}
	return ""
}

func (s *Session) upStatus(rc *plugin.RequestContext) string {
	var up row
	if err := s.client.do(rc.Ctx, http.MethodGet, "/_up", nil, nil, &up); err != nil {
		return "unknown"
	}
	if status := stringOf(up["status"]); status != "" {
		return status
	}
	return "ok"
}

func (s *Session) databaseNames(rc *plugin.RequestContext) []string {
	var names []string
	if err := s.client.do(rc.Ctx, http.MethodGet, "/_all_dbs", nil, nil, &names); err != nil {
		return nil
	}
	return names
}

// databaseNamePage reads one bounded page of database names. CouchDB pages
// /_all_dbs by key, so the cursor is normally the name to resume from; the grid
// falls back to the row offset whenever it has no remembered cursor, and a
// database name can never be all digits, so that form is served with skip. One
// extra name is requested to derive the next cursor without counting the whole
// server.
func (s *Session) databaseNamePage(rc *plugin.RequestContext, cursor string, limit int, desc bool) ([]string, string, error) {
	if limit <= 0 {
		limit = plugin.DefaultPageLimit
	}
	if limit > plugin.MaxPageLimit {
		limit = plugin.MaxPageLimit
	}
	query := url.Values{"limit": []string{strconv.Itoa(limit + 1)}}
	if desc {
		query.Set("descending", "true")
	}
	if cursor != "" {
		if offset, err := strconv.Atoi(cursor); err == nil {
			if offset < 0 {
				return nil, "", fmt.Errorf("%w: invalid cursor", plugin.ErrInvalidInput)
			}
			query.Set("skip", strconv.Itoa(offset))
		} else {
			key, err := json.Marshal(cursor)
			if err != nil {
				return nil, "", fmt.Errorf("%w: invalid cursor", plugin.ErrInvalidInput)
			}
			query.Set("startkey", string(key))
		}
	}
	var names []string
	if err := s.client.do(rc.Ctx, http.MethodGet, "/_all_dbs", query, nil, &names); err != nil {
		return nil, "", err
	}
	if len(names) > limit {
		return names[:limit], names[limit], nil
	}
	return names, "", nil
}

// serverOverview aggregates the welcome document, cluster membership, the local
// node's system counters and the size of every database into one property sheet.
func serverOverview(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var welcome row
	if err := s.client.do(rc.Ctx, http.MethodGet, "/", nil, nil, &welcome); err != nil {
		return nil, err
	}
	out := row{
		"version":  stringOf(welcome["version"]),
		"vendor":   vendorName(welcome),
		"git_sha":  stringOf(welcome["git_sha"]),
		"features": welcome["features"],
		"status":   s.upStatus(rc),
	}

	var membership struct {
		AllNodes     []string `json:"all_nodes"`
		ClusterNodes []string `json:"cluster_nodes"`
	}
	if err := s.client.do(rc.Ctx, http.MethodGet, "/_membership", nil, nil, &membership); err == nil {
		out["cluster_nodes"] = len(membership.ClusterNodes)
		out["all_nodes"] = len(membership.AllNodes)
		if len(membership.ClusterNodes) > 0 {
			out["local_node"] = membership.ClusterNodes[0]
		}
	}

	var system row
	if err := s.client.do(rc.Ctx, http.MethodGet, "/_node/_local/_system", nil, nil, &system); err == nil {
		out["uptime"] = numberOf(system["uptime"])
	}

	// Document and disk totals are deliberately absent: they would cost an info
	// read per database, which is unbounded on a per-user-database deployment.
	out["databases"] = len(s.databaseNames(rc))

	var tasks []row
	if err := s.client.do(rc.Ctx, http.MethodGet, "/_active_tasks", nil, nil, &tasks); err == nil {
		out["active_tasks"] = len(tasks)
	}
	if docs, err := s.schedulerDocs(rc); err == nil {
		out["replications"] = len(docs)
	}
	return out, nil
}

// serverConfig renders the local node's configuration. CouchDB refuses this to
// non-admins, so a denial is reported as an explanatory document rather than a
// broken tab.
func serverConfig(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var config row
	if err := s.client.do(rc.Ctx, http.MethodGet, "/_node/_local/_config", nil, nil, &config); err != nil {
		return row{
			"content": "CouchDB did not return the node configuration: " + err.Error() +
				"\n\nReading /_node/_local/_config requires server admin rights.",
			"format": "markdown",
		}, nil
	}
	return redactConfig(config), nil
}

const redactedValue = "[redacted by ShellCN]"

// secretConfigSections hold credential material verbatim: [admins] carries the
// hashed server-admin passwords and the auth sections carry the cookie-signing
// secret, which is enough to mint a valid AuthSession.
var secretConfigSections = map[string]bool{"admins": true}

func secretConfigKey(key string) bool {
	lower := strings.ToLower(key)
	return strings.Contains(lower, "secret") || strings.Contains(lower, "password")
}

// redactConfig strips credential material out of the node configuration before
// it reaches a browser or an audit record.
func redactConfig(config row) row {
	out := make(row, len(config))
	for section, values := range config {
		entries := mapOf(values)
		if entries == nil {
			out[section] = values
			continue
		}
		cleaned := make(row, len(entries))
		for key, value := range entries {
			if secretConfigSections[section] || secretConfigKey(key) {
				cleaned[key] = redactedValue
				continue
			}
			cleaned[key] = value
		}
		out[section] = cleaned
	}
	return out
}

func fauxtonURL(rc *plugin.RequestContext) (any, error) {
	return row{"url": rc.ProxyURL("_utils")}, nil
}

func taskRows(tasks []row) []row {
	out := make([]row, 0, len(tasks))
	for _, task := range tasks {
		kind := stringOf(task["type"])
		target := stringOf(task["database"])
		if target == "" {
			target = stringOf(task["source"])
		}
		id := stringOf(task["pid"])
		if id == "" {
			id = kind + ":" + target
		}
		out = append(out, timelineRow(
			taskStarted(task),
			taskTitle(kind, target),
			taskMessage(task, kind),
			taskSeverity(kind),
			plugin.ResourceIdentity{Kind: "task", Name: id, UID: id},
		))
	}
	return out
}

func taskStarted(task row) time.Time {
	if started, ok := unixTime(task["started_on"]); ok {
		return started
	}
	if updated, ok := unixTime(task["updated_on"]); ok {
		return updated
	}
	return time.Now()
}

func taskTitle(kind, target string) string {
	if target == "" {
		return kind
	}
	return kind + " " + target
}

func taskMessage(task row, kind string) string {
	parts := make([]string, 0, 4)
	if progress := task["progress"]; progress != nil {
		parts = append(parts, fmt.Sprintf("progress %.0f%%", numberOf(progress)))
	}
	switch kind {
	case "indexer":
		parts = append(parts, fmt.Sprintf("design doc %s", stringOf(task["design_document"])))
		parts = append(parts, fmt.Sprintf("%.0f of %.0f changes", numberOf(task["changes_done"]), numberOf(task["total_changes"])))
	case "replication":
		parts = append(parts, fmt.Sprintf("%s -> %s", stringOf(task["source"]), stringOf(task["target"])))
		parts = append(parts, fmt.Sprintf("%.0f docs written", numberOf(task["docs_written"])))
	case "database_compaction", "view_compaction":
		parts = append(parts, fmt.Sprintf("%.0f of %.0f changes", numberOf(task["changes_done"]), numberOf(task["total_changes"])))
	}
	if node := stringOf(task["node"]); node != "" {
		parts = append(parts, "on "+node)
	}
	return strings.Join(parts, " · ")
}

func taskSeverity(kind string) plugin.Severity {
	switch kind {
	case "replication":
		return plugin.SeverityInfo
	case "indexer":
		return plugin.SeveritySuccess
	default:
		return plugin.SeveritySecondary
	}
}

func listTasks(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var tasks []row
	if err := s.client.do(rc.Ctx, http.MethodGet, "/_active_tasks", nil, nil, &tasks); err != nil {
		return nil, err
	}
	return broker.PageRows(rc, taskRows(tasks))
}

func watchTasks(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := session(rc)
	if err != nil {
		return err
	}
	return pollWatch(rc, client, 3*time.Second, func() ([]plugin.ResourceEvent, error) {
		var tasks []row
		if err := s.client.do(rc.Ctx, http.MethodGet, "/_active_tasks", nil, nil, &tasks); err != nil {
			return nil, err
		}
		return modifiedEvents(taskRows(tasks), "task", func(item row) plugin.ResourceIdentity {
			ref, _ := item["resource"].(plugin.ResourceIdentity)
			return ref
		}), nil
	})
}

// metricsStream turns CouchDB's cumulative counters into a live frame: absolute
// totals plus a request rate derived from the delta between ticks.
func metricsStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := session(rc)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(client)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var lastRequests float64
	var lastAt time.Time
	emit := func() error {
		frame, requests := s.metricsFrame(rc)
		if !lastAt.IsZero() {
			if elapsed := time.Since(lastAt).Seconds(); elapsed > 0 && requests >= lastRequests {
				frame["requestsPerSec"] = round1((requests - lastRequests) / elapsed)
			}
		}
		lastRequests, lastAt = requests, time.Now()
		return enc.Encode(frame)
	}
	if err := emit(); err != nil {
		return err
	}
	for {
		select {
		case <-client.Context().Done():
			return nil
		case <-rc.Ctx.Done():
			return nil
		case <-ticker.C:
			if err := emit(); err != nil {
				return err
			}
		}
	}
}

func (s *Session) metricsFrame(rc *plugin.RequestContext) (row, float64) {
	frame := row{"requestsPerSec": 0.0}

	var stats row
	requests := 0.0
	if err := s.client.do(rc.Ctx, http.MethodGet, "/_node/_local/_stats", nil, nil, &stats); err == nil {
		requests = statValue(stats, "couchdb", "httpd", "requests")
		frame["requests"] = requests
		frame["reads"] = statValue(stats, "couchdb", "database_reads")
		frame["writes"] = statValue(stats, "couchdb", "database_writes")
		frame["openDatabases"] = statValue(stats, "couchdb", "open_databases")
		frame["openFiles"] = statValue(stats, "couchdb", "open_os_files")
	}

	var system row
	if err := s.client.do(rc.Ctx, http.MethodGet, "/_node/_local/_system", nil, nil, &system); err == nil {
		frame["uptime"] = numberOf(system["uptime"])
		frame["runQueue"] = numberOf(system["run_queue"])
		memory := mapOf(system["memory"])
		used, total := numberOf(memory["processes"]), numberOf(memory["total"])
		frame["memUsed"] = used
		frame["memTotal"] = total
		if total > 0 {
			frame["memPct"] = round1(used / total * 100)
		}
	}

	var tasks []row
	if err := s.client.do(rc.Ctx, http.MethodGet, "/_active_tasks", nil, nil, &tasks); err == nil {
		frame["activeTasks"] = len(tasks)
	}
	return frame, requests
}

func round1(v float64) float64 { return float64(int64(v*10+0.5)) / 10 }

func (s *Session) nodeRows(rc *plugin.RequestContext) ([]row, error) {
	var membership struct {
		AllNodes     []string `json:"all_nodes"`
		ClusterNodes []string `json:"cluster_nodes"`
	}
	if err := s.client.do(rc.Ctx, http.MethodGet, "/_membership", nil, nil, &membership); err != nil {
		return nil, err
	}
	cluster := make(map[string]bool, len(membership.ClusterNodes))
	for _, node := range membership.ClusterNodes {
		cluster[node] = true
	}
	seen := map[string]bool{}
	names := append(append([]string{}, membership.ClusterNodes...), membership.AllNodes...)

	rows := make([]row, 0, len(names))
	for _, name := range names {
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		item := row{
			"name":       name,
			"membership": membershipLabel(cluster[name]),
			"ref":        plugin.ResourceIdentity{Kind: "node", Name: name, UID: name},
		}
		mergeNodeSystem(rc, s, name, item)
		rows = append(rows, item)
	}
	return rows, nil
}

func membershipLabel(inCluster bool) string {
	if inCluster {
		return "cluster"
	}
	return "known"
}

func mergeNodeSystem(rc *plugin.RequestContext, s *Session, node string, item row) {
	var system row
	if err := s.client.do(rc.Ctx, http.MethodGet, "/_node/"+url.PathEscape(node)+"/_system", nil, nil, &system); err != nil {
		return
	}
	item["uptime"] = numberOf(system["uptime"])
	item["process_count"] = numberOf(system["process_count"])
	item["run_queue"] = numberOf(system["run_queue"])
	memory := mapOf(system["memory"])
	used, total := numberOf(memory["processes"]), numberOf(memory["total"])
	item["memory_used"] = used
	item["memory_total"] = total
	if total > 0 {
		item["memory_pct"] = round1(used / total * 100)
	}
}

func listNodes(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	rows, err := s.nodeRows(rc)
	if err != nil {
		return nil, err
	}
	return broker.PageRows(rc, rows)
}

func treeNodes(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	rows, err := s.nodeRows(rc)
	if err != nil {
		return nil, err
	}
	nodes := make([]plugin.TreeNode, 0, len(rows))
	for _, item := range rows {
		name := stringOf(item["name"])
		ref := plugin.ResourceIdentity{Kind: "node", Name: name, UID: name}
		nodes = append(nodes, plugin.TreeNode{
			Key: "node:" + name, Label: name, Icon: icon("cpu"), Ref: &ref, Leaf: true,
			Data: map[string]any{"membership": item["membership"]},
		})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes}, nil
}

func readNode(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	name, err := nodeParam(rc)
	if err != nil {
		return nil, err
	}
	rows, err := s.nodeRows(rc)
	if err != nil {
		return nil, err
	}
	for _, item := range rows {
		if stringOf(item["name"]) == name {
			return item, nil
		}
	}
	return nil, fmt.Errorf("%w: node %q is not a member of this cluster", plugin.ErrNotFound, name)
}

func nodeStats(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	name, err := nodeParam(rc)
	if err != nil {
		return nil, err
	}
	var stats row
	if err := s.client.do(rc.Ctx, http.MethodGet, "/_node/"+url.PathEscape(name)+"/_stats", nil, nil, &stats); err != nil {
		return nil, err
	}
	return stats, nil
}
