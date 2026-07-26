package couchbase

import (
	"strings"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

var healthSeverities = map[string]plugin.Severity{
	"healthy":    plugin.SeveritySuccess,
	"degraded":   plugin.SeverityWarn,
	"unbalanced": plugin.SeverityWarn,
	"down":       plugin.SeverityDanger,
	"unknown":    plugin.SeveritySecondary,
}

var statusSeverities = map[string]plugin.Severity{
	"healthy": plugin.SeveritySuccess, "ready": plugin.SeveritySuccess, "online": plugin.SeveritySuccess,
	"running": plugin.SeveritySuccess, "active": plugin.SeveritySuccess, "created": plugin.SeveritySuccess,
	"warmup": plugin.SeverityWarn, "pausing": plugin.SeverityWarn, "paused": plugin.SeverityWarn,
	"scheduled for creation": plugin.SeverityWarn, "building": plugin.SeverityWarn, "replicating": plugin.SeverityWarn,
	"unhealthy": plugin.SeverityDanger, "failed": plugin.SeverityDanger, "error": plugin.SeverityDanger,
	"offline": plugin.SeverityDanger, "deleted": plugin.SeveritySecondary, "notrunning": plugin.SeveritySecondary,
	"inactivefailed": plugin.SeverityDanger, "inactiveadded": plugin.SeveritySecondary, "unknown": plugin.SeveritySecondary,
}

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:  plugin.CurrentAPIVersion,
		Name:        protocolName,
		Version:     "0.1.0",
		Title:       "Couchbase",
		Description: "Couchbase Server cockpit: buckets, scopes and collections, JSON documents, SQL++ with an index advisor, live cluster and DCP metrics, a data map, and XDCR activity.",
		Icon:        plugin.Icon{Type: plugin.IconSVG, Value: iconSVG},
		Category:    plugin.CategoryDatabases,
		Config:      configSchema(),
		Capabilities: []plugin.Capability{
			"buckets", "scopes", "collections", "documents", "kv", "sqlpp", "n1ql",
			"indexes", "index-advisor", "metrics", "dcp", "xdcr", "web-console",
		},
		SupportedTransports: []plugin.Transport{plugin.TransportDirect, plugin.TransportAgent},
		Agent: &plugin.AgentProfile{
			Proxy: plugin.ProxyTarget{Mode: plugin.AgentTCP, Address: "127.0.0.1:8091", Risk: plugin.RiskPrivileged},
			Install: []plugin.InstallArtifact{{
				Label:    "Docker",
				Kind:     "docker",
				Template: "docker run -d --network host shellcn/agent --connect {{shellquote .ConnectURL}} --token {{shellquote .Token}}",
			}},
		},
		Layout:        plugin.LayoutSidebarTree,
		Tree:          tree(),
		Scope:         []plugin.ScopeFilter{bucketScope()},
		Resources:     resources(),
		Actions:       actions(),
		HeaderActions: []string{rid("console.open")},
		Streams: []plugin.Stream{
			{ID: rid("query"), Kind: plugin.StreamQuery, RouteID: rid("query")},
			{ID: rid("advisor"), Kind: plugin.StreamQuery, RouteID: rid("advisor")},
			{ID: rid("cluster.metrics"), Kind: plugin.StreamMetrics, RouteID: rid("cluster.metrics")},
			{ID: rid("bucket.metrics"), Kind: plugin.StreamMetrics, RouteID: rid("bucket.metrics")},
			{ID: rid("cluster.watch"), Kind: plugin.StreamResource, RouteID: rid("cluster.watch")},
			{ID: rid("events.watch"), Kind: plugin.StreamResource, RouteID: rid("events.watch")},
			{ID: rid("resource.watch"), Kind: plugin.StreamResource, RouteID: rid("resource.watch")},
			{ID: rid("datamap"), Kind: plugin.StreamCanvas, RouteID: rid("datamap")},
		},
	}
}

func bucketScope() plugin.ScopeFilter {
	return plugin.ScopeFilter{
		Param:         "bucket",
		Label:         "Bucket",
		Icon:          icon("database"),
		Control:       plugin.ScopeSelect,
		OptionsSource: &plugin.DataSource{RouteID: rid("buckets.list")},
		ValueField:    "name",
		LabelField:    "name",
	}
}

func tree() []plugin.TreeGroup {
	return []plugin.TreeGroup{
		{Key: "cluster", Label: "Cluster", Icon: icon("server"), Ref: &plugin.ResourceIdentity{Kind: "cluster", Name: "Cluster", UID: clusterUID}},
		{Key: "buckets", Label: "Buckets", Icon: icon("database"), Source: plugin.DataSource{RouteID: rid("tree.buckets")}, ResourceKind: "bucket"},
		{Key: "indexes", Label: "Indexes", Icon: icon("list-tree"), ResourceKind: "index"},
		{Key: "xdcr", Label: "XDCR", Icon: icon("git-compare-arrows"), ResourceKind: "replication"},
	}
}

// treeBuckets carries the whole bucket summary as node data so a detail opened
// from the sidebar holds the same row as one opened from the bucket list; the
// bucket actions prefill their forms from it.
func treeBuckets(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	rows, err := s.bucketSummaries(rc.Ctx)
	if err != nil {
		return nil, err
	}
	nodes := make([]plugin.TreeNode, 0, len(rows))
	for _, item := range rows {
		name := stringValue(item["name"])
		ref := bucketRef(name)
		nodes = append(nodes, plugin.TreeNode{
			Key:            "bucket:" + name,
			Label:          name,
			Icon:           icon("database"),
			Ref:            &ref,
			ChildrenSource: &plugin.DataSource{RouteID: rid("tree.scopes"), Params: map[string]string{"bucket": name}},
			Data:           item,
		})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes}, nil
}

func treeScopes(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	bucket, err := bucketName(rc, s)
	if err != nil {
		return nil, err
	}
	manifest, err := s.scopes(rc.Ctx, bucket)
	if err != nil {
		return nil, err
	}
	nodes := make([]plugin.TreeNode, 0, len(manifest.Scopes))
	for _, scope := range manifest.Scopes {
		ref := scopeRef(bucket, scope.Name)
		nodes = append(nodes, plugin.TreeNode{
			Key:            "scope:" + bucket + "." + scope.Name,
			Label:          scope.Name,
			Icon:           icon("folder-tree"),
			Ref:            &ref,
			Leaf:           len(scope.Collections) == 0,
			ChildrenSource: &plugin.DataSource{RouteID: rid("tree.collections"), Params: map[string]string{"bucket": bucket, "scope": scope.Name}},
			Data:           map[string]any{"collections": len(scope.Collections), "system": strings.HasPrefix(scope.Name, "_")},
		})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes}, nil
}

func treeCollections(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	k, err := keyspaceFrom(rc, s)
	if err != nil {
		return nil, err
	}
	manifest, err := s.scopes(rc.Ctx, k.Bucket)
	if err != nil {
		return nil, err
	}
	nodes := make([]plugin.TreeNode, 0, 8)
	for _, scope := range manifest.Scopes {
		if scope.Name != k.Scope {
			continue
		}
		for _, collection := range scope.Collections {
			ref := collectionRef(k.Bucket, scope.Name, collection.Name)
			nodes = append(nodes, plugin.TreeNode{
				Key:   "collection:" + ref.UID,
				Label: collection.Name,
				Icon:  icon("table-2"),
				Ref:   &ref,
				Leaf:  true,
			})
		}
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes}, nil
}

// resourceWatchSource binds a resource list to the generic watch stream.
func resourceWatchSource(kind string, params map[string]string) *plugin.DataSource {
	merged := map[string]string{"kind": kind}
	for key, value := range params {
		merged[key] = value
	}
	return &plugin.DataSource{RouteID: rid("resource.watch"), Method: plugin.MethodWS, Params: merged}
}

func keyspaceParams() map[string]string {
	return map[string]string{"keyspace": "${resource.uid}"}
}

func objectDetail(sections []plugin.ObjectDetailSection, watch *plugin.DataSource) plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{Sections: sections, RawToggle: true, Watch: watch}
}

// keyspaceBucketExpr and keyspaceScopeExpr recover the bucket and the scope of a
// system:keyspaces row: the row describing a bucket's default scope and default
// collection carries neither field, and only the bucket name in `id`.
const (
	keyspaceBucketExpr = "IFMISSING(k.`bucket`, k.`id`)"
	keyspaceScopeExpr  = "IFMISSING(k.`scope`, \"_default\")"
)

// keyspaceCatalog lists the keyspaces an operator can query. It needs no index
// and projects the catalog row itself, so the grid shows one column per catalog
// field instead of a single nested document.
func keyspaceCatalog(where string) string {
	statement := "SELECT k.* FROM system:keyspaces k"
	if where != "" {
		statement += " WHERE " + where
	}
	return statement + " ORDER BY k.`path` LIMIT 25;"
}

// collectionSample reads documents straight out of a collection. It relies on no
// user-built index: the Query service serves it with a sequential scan.
func collectionSample(where string) string {
	statement := "SELECT META(d).id AS id, d.* FROM `${resource.scope}`.`${resource.namespace}`.`${resource.name}` d"
	if where != "" {
		statement += " WHERE " + where
	}
	return statement + " LIMIT 25;"
}

func queryConfig(label, initial string, params map[string]string) plugin.QueryEditorConfig {
	return plugin.QueryEditorConfig{
		Language:          "sql",
		Label:             label,
		ExecuteLabel:      "Run",
		RunningLabel:      "Running…",
		CancelLabel:       "Cancel",
		EmptyText:         "Run a SQL++ statement to see results.",
		InitialQuery:      initial,
		CancelRouteID:     rid("query.cancel"),
		CompletionRouteID: rid("query.complete"),
		CompletionParams:  params,
		Exportable:        true,
	}
}

func advisorConfig(initial string) plugin.QueryEditorConfig {
	return plugin.QueryEditorConfig{
		Language:     "sql",
		Label:        "Index advisor",
		ExecuteLabel: "Analyse",
		RunningLabel: "Analysing…",
		EmptyText:    "Paste a SELECT, UPDATE, DELETE, or MERGE statement to get index recommendations.",
		InitialQuery: initial,
		Exportable:   true,
	}
}

func nodeColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "hostname", Label: "Node", Sortable: true},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Sortable: true, Severities: statusSeverities},
		{Key: "services", Label: "Services"},
		{Key: "version", Label: "Version"},
		{Key: "cpuPct", Label: "CPU", Type: plugin.ColumnPercent, Sortable: true},
		{Key: "ramPct", Label: "RAM", Type: plugin.ColumnPercent, Sortable: true},
		{Key: "ramUsed", Label: "RAM used", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "items", Label: "Items", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "opsPerSec", Label: "Ops/s", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "uptime", Label: "Uptime", Type: plugin.ColumnDuration, Sortable: true},
	}
}

func bucketColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Bucket", Sortable: true},
		{Key: "health", Label: "Health", Type: plugin.ColumnBadge, Sortable: true, Severities: healthSeverities},
		{Key: "bucket_type", Label: "Type", Type: plugin.ColumnBadge, Sortable: true},
		{Key: "items", Label: "Documents", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "opsPerSec", Label: "Ops/s", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "quotaPct", Label: "RAM quota used", Type: plugin.ColumnPercent, Sortable: true},
		{Key: "memUsed", Label: "RAM used", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "diskUsed", Label: "Disk used", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "replicas", Label: "Replicas", Type: plugin.ColumnNumber, Sortable: true},
	}
}

func scopeColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Scope", Sortable: true},
		{Key: "collections", Label: "Collections", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "items", Label: "Documents", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "memUsed", Label: "Resident", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "system", Label: "System", Type: plugin.ColumnBool, Sortable: true},
	}
}

func collectionColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Collection", Sortable: true},
		{Key: "scope", Label: "Scope", Sortable: true},
		{Key: "items", Label: "Documents", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "memUsed", Label: "Resident", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "dataSize", Label: "Data size", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "opsPerSec", Label: "Ops/s", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "max_ttl", Label: "Expiry", Type: plugin.ColumnDuration, Sortable: true},
	}
}

func indexColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Index", Sortable: true},
		{Key: "keyspace", Label: "Keyspace", Sortable: true},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Sortable: true, Severities: statusSeverities},
		{Key: "progress", Label: "Build", Type: plugin.ColumnPercent, Sortable: true},
		{Key: "primary", Label: "Primary", Type: plugin.ColumnBool, Sortable: true},
		{Key: "replicas", Label: "Replicas", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "storage_mode", Label: "Storage", Sortable: true},
		{Key: "hosts", Label: "Hosts"},
	}
}

func replicationColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "id", Label: "Replication", Sortable: true},
		{Key: "source", Label: "Source", Sortable: true},
		{Key: "target", Label: "Target", Sortable: true},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Sortable: true, Severities: statusSeverities},
		{Key: "changes_left", Label: "Changes left", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "docs_written", Label: "Docs written", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "continuous", Label: "Continuous", Type: plugin.ColumnBool},
		{Key: "errors", Label: "Errors", Type: plugin.ColumnNumber, Sortable: true},
	}
}

func dcpColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "consumer", Label: "DCP consumer", Sortable: true},
		{Key: "count", Label: "Streams", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "producer_count", Label: "Producers", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "items_remaining", Label: "Items remaining", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "items_sent", Label: "Items sent", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "total_bytes", Label: "Bytes/s", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "backoff", Label: "Backoffs", Type: plugin.ColumnNumber, Sortable: true},
	}
}

func userColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "id", Label: "User", Sortable: true},
		{Key: "name", Label: "Name", Sortable: true},
		{Key: "domain", Label: "Domain", Type: plugin.ColumnBadge, Sortable: true},
		{Key: "role_count", Label: "Roles", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "roles", Label: "Granted roles"},
		{Key: "groups", Label: "Groups"},
	}
}

func remoteColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Remote cluster", Sortable: true},
		{Key: "hostname", Label: "Hostname", Sortable: true},
		{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Sortable: true, Severities: statusSeverities},
		{Key: "username", Label: "User"},
		{Key: "encryption", Label: "Encryption", Type: plugin.ColumnBadge},
	}
}

func schemaColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "field", Label: "Field", Sortable: true},
		{Key: "type", Label: "Type", Type: plugin.ColumnBadge, Sortable: true},
		{Key: "coverage", Label: "Documents", Type: plugin.ColumnPercent, Sortable: true},
		{Key: "flavours", Label: "Shapes", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "samples", Label: "Sample values"},
	}
}

func savedQueryColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Name", Sortable: true},
		{Key: "keyspace", Label: "Keyspace", Sortable: true},
		{Key: "description", Label: "Description"},
		{Key: "statement", Label: "SQL++"},
		{Key: "updated_at", Label: "Updated", Type: plugin.ColumnDateTime, Sortable: true},
	}
}

func timelineConfig(params map[string]string, empty string) plugin.TimelineConfig {
	return plugin.TimelineConfig{
		TimestampField: "time",
		TitleField:     "title",
		BodyField:      "message",
		SeverityField:  "severity",
		IconField:      "icon",
		ResourceField:  "module",
		EmptyText:      empty,
		Watch:          &plugin.DataSource{RouteID: rid("events.watch"), Method: plugin.MethodWS, Params: params},
	}
}

func dataMapConfig() plugin.CanvasConfig {
	return plugin.CanvasConfig{
		ScaleMode:      plugin.CanvasScaleResize,
		Interactive:    true,
		Pointer:        true,
		Keyboard:       true,
		ResizeEvents:   true,
		WheelMode:      plugin.CanvasWheelNone,
		HiDPI:          true,
		FocusOnPointer: true,
		AriaLabel:      "Bucket data map: scopes, collections, and document counts",
		Instructions:   "Use the arrow keys to move between collections, Home and End to jump to the first or last one. Each bar is one collection, sized by its share of the largest collection in the bucket.",
	}
}

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		clusterResource(),
		bucketResource(),
		scopeResource(),
		collectionResource(),
		documentResource(),
		indexResource(),
		replicationResource(),
	}
}

func clusterResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "cluster", Title: "Cluster",
		List:  plugin.DataSource{RouteID: rid("cluster.list")},
		Watch: resourceWatchSource("cluster", nil),
		Columns: []plugin.Column{
			{Key: "name", Label: "Cluster", Sortable: true},
			{Key: "health", Label: "Health", Type: plugin.ColumnBadge, Severities: healthSeverities},
			{Key: "version", Label: "Version"},
			{Key: "edition", Label: "Edition", Type: plugin.ColumnBadge},
			{Key: "nodes", Label: "Nodes", Type: plugin.ColumnNumber},
			{Key: "buckets", Label: "Buckets", Type: plugin.ColumnNumber},
		},
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.name}", StatusField: "health", Severities: healthSeverities},
			DefaultTab: "overview",
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("cluster.overview")},
					Config: objectDetail(clusterSections(), &plugin.DataSource{RouteID: rid("cluster.watch"), Method: plugin.MethodWS})},
				{Key: "metrics", Label: "Metrics", Icon: icon("activity"), Type: plugin.PanelMetrics,
					Source: &plugin.DataSource{RouteID: rid("cluster.metrics"), Method: plugin.MethodWS},
					Config: clusterMetricsConfig()},
				{Key: "nodes", Label: "Nodes", Icon: icon("server"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("nodes.list")},
					Config: plugin.TableConfig{Columns: nodeColumns(), DefaultSort: &plugin.SortKey{Field: "hostname"},
						RefreshIntervalMs: 10000, Exportable: true, EmptyText: "No nodes reported by this cluster."}},
				{Key: "buckets", Label: "Buckets", Icon: icon("database"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("buckets.list")},
					Config: plugin.TableConfig{Columns: bucketColumns(), DefaultSort: &plugin.SortKey{Field: "name"},
						ActionIDs: []string{rid("bucket.create")}, RefreshIntervalMs: 10000, Exportable: true,
						RowClick: plugin.RowClickNavigate, EmptyText: "No buckets yet. Create one to store documents."}},
				{Key: "datamap", Label: "Data map", Icon: icon("layout-dashboard"), Type: plugin.PanelCanvas,
					Source: &plugin.DataSource{RouteID: rid("datamap"), Method: plugin.MethodWS},
					Config: dataMapConfig()},
				{Key: "indexes", Label: "Indexes", Icon: icon("list-tree"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("indexes.list")},
					Config: plugin.TableConfig{Columns: indexColumns(), DefaultSort: &plugin.SortKey{Field: "name"},
						RefreshIntervalMs: 15000, Exportable: true, RowClick: plugin.RowClickNavigate,
						EmptyText: "No global secondary indexes."}},
				{Key: "users", Label: "Users", Icon: icon("users"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("users.list")},
					Config: plugin.TableConfig{Columns: userColumns(), DefaultSort: &plugin.SortKey{Field: "id"},
						RefreshIntervalMs: 60000, EmptyText: "No RBAC users are defined on this cluster."}},
				{Key: "events", Label: "Events", Icon: icon("history"), Type: plugin.PanelTimeline,
					Source: &plugin.DataSource{RouteID: rid("events.list")},
					Config: timelineConfig(nil, "No cluster events recorded yet.")},
				{Key: "query", Label: "SQL++", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor,
					Source: &plugin.DataSource{RouteID: rid("query"), Method: plugin.MethodWS},
					Config: queryConfig("SQL++", keyspaceCatalog(""), nil)},
				{Key: "advisor", Label: "Index advisor", Icon: icon("lightbulb"), Type: plugin.PanelQueryEditor,
					Source: &plugin.DataSource{RouteID: rid("advisor"), Method: plugin.MethodWS},
					// Unordered: sorting the catalog makes the plan recommend an
					// index on the system keyspace, which cannot be created.
					Config: advisorConfig("SELECT k.* FROM system:keyspaces k LIMIT 25;")},
				{Key: "saved", Label: "Saved queries", Icon: icon("bookmark"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("queries.list")},
					Config: plugin.TableConfig{Columns: savedQueryColumns(), DefaultSort: &plugin.SortKey{Field: "name"},
						ActionIDs: []string{rid("query.save")}, RowActionIDs: []string{rid("query.delete")},
						RefreshIntervalMs: 30000, EmptyText: "No saved SQL++ statements for this connection yet."}},
			},
		},
	}
}

func clusterSections() []plugin.ObjectDetailSection {
	return []plugin.ObjectDetailSection{
		{Title: "Cluster", Fields: []plugin.ObjectDetailField{
			{Key: "name", Label: "Name", Copy: true},
			{Key: "health", Label: "Health", Type: plugin.ColumnBadge, Severities: healthSeverities},
			{Key: "version", Label: "Version"},
			{Key: "edition", Label: "Edition", Type: plugin.ColumnBadge},
			{Key: "services", Label: "Services"},
			{Key: "nodes", Label: "Nodes", Type: plugin.ColumnNumber},
			{Key: "healthy_nodes", Label: "Healthy nodes", Type: plugin.ColumnNumber},
			{Key: "balanced", Label: "Balanced", Type: plugin.ColumnBool},
			{Key: "rebalance_status", Label: "Rebalance", Type: plugin.ColumnBadge, Severities: statusSeverities},
			{Key: "alerts", Label: "Active alerts", Type: plugin.ColumnNumber},
		}},
		{Title: "Capacity", Fields: []plugin.ObjectDetailField{
			{Key: "quotaPct", Label: "Bucket RAM quota", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
				PercentKey: "quotaPct", UsedKey: "quotaUsed", TotalKey: "quotaTotal",
				UsedType: plugin.ColumnBytes, TotalType: plugin.ColumnBytes, WarnAt: 80, CriticalAt: 95}},
			{Key: "ramPct", Label: "Node memory", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
				PercentKey: "ramPct", UsedKey: "ramUsed", TotalKey: "ramTotal",
				UsedType: plugin.ColumnBytes, TotalType: plugin.ColumnBytes, WarnAt: 85, CriticalAt: 95}},
			{Key: "diskPct", Label: "Disk", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
				PercentKey: "diskPct", UsedKey: "diskUsed", TotalKey: "diskTotal",
				UsedType: plugin.ColumnBytes, TotalType: plugin.ColumnBytes, WarnAt: 80, CriticalAt: 92}},
			{Key: "data_memory_quota", Label: "Data service quota", Type: plugin.ColumnNumber},
			{Key: "index_memory_quota", Label: "Index service quota", Type: plugin.ColumnNumber},
		}},
		{Title: "Workload", Fields: []plugin.ObjectDetailField{
			{Key: "buckets", Label: "Buckets", Type: plugin.ColumnNumber},
			{Key: "max_buckets", Label: "Bucket limit", Type: plugin.ColumnNumber},
			{Key: "items", Label: "Documents", Type: plugin.ColumnNumber},
			{Key: "opsPerSec", Label: "Ops/s", Type: plugin.ColumnNumber},
			{Key: "cpuPct", Label: "CPU", Type: plugin.ColumnPercent},
		}},
		{Title: "Connection", Fields: []plugin.ObjectDetailField{
			{Key: "management_endpoint", Label: "Management API", Copy: true},
			{Key: "query_endpoint", Label: "Query service", Copy: true},
			{Key: "read_only", Label: "Read-only mode", Type: plugin.ColumnBool},
			{Key: "confirm_write_queries", Label: "Confirm write statements", Type: plugin.ColumnBool},
		}},
	}
}

func clusterMetricsConfig() plugin.MetricsConfig {
	return plugin.MetricsConfig{
		Stats: []plugin.MetricStat{
			{Key: "buckets", Label: "Buckets"},
			{Key: "nodes", Label: "Nodes"},
			{Key: "items", Label: "Documents"},
			{Key: "opsPerSec", Label: "Ops/s"},
			{Key: "diskFetches", Label: "Disk fetches/s"},
		},
		Usage: []plugin.MetricUsage{
			{Key: "quotaPct", Label: "Bucket RAM quota", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
				PercentKey: "quotaPct", UsedKey: "quotaUsed", TotalKey: "quotaTotal",
				UsedType: plugin.ColumnBytes, TotalType: plugin.ColumnBytes, WarnAt: 80, CriticalAt: 95}},
			{Key: "diskPct", Label: "Disk", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
				PercentKey: "diskPct", UsedKey: "diskUsed", TotalKey: "diskTotal",
				UsedType: plugin.ColumnBytes, TotalType: plugin.ColumnBytes, WarnAt: 80, CriticalAt: 92}},
		},
		Series: []plugin.MetricSeries{
			{Key: "opsPerSec", Label: "Ops/s"},
			{Key: "cpuPct", Label: "CPU", Unit: "%"},
			{Key: "quotaPct", Label: "RAM quota", Unit: "%"},
		},
		History: 60,
	}
}

func bucketResource() plugin.ResourceType {
	bucketParams := map[string]string{"bucket": "${resource.name}"}
	return plugin.ResourceType{
		Kind: "bucket", Title: "Buckets",
		List:    plugin.DataSource{RouteID: rid("buckets.list")},
		Watch:   resourceWatchSource("bucket", nil),
		Columns: bucketColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{rid("bucket.create")},
			Row:     []string{rid("bucket.delete")},
			Detail:  []string{rid("scope.create"), rid("bucket.update"), rid("bucket.compact"), rid("bucket.flush"), rid("bucket.delete")},
		},
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.name}", StatusField: "health", Severities: healthSeverities},
			DefaultTab: "overview",
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("bucket.read"), Params: bucketParams},
					Config: objectDetail(bucketSections(), nil)},
				{Key: "metrics", Label: "Metrics", Icon: icon("activity"), Type: plugin.PanelMetrics,
					Source: &plugin.DataSource{RouteID: rid("bucket.metrics"), Method: plugin.MethodWS, Params: bucketParams},
					Config: bucketMetricsConfig()},
				{Key: "scopes", Label: "Scopes", Icon: icon("folder-tree"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("scopes.list"), Params: bucketParams},
					Config: plugin.TableConfig{Columns: scopeColumns(), DefaultSort: &plugin.SortKey{Field: "name"},
						ActionIDs: []string{rid("scope.create")}, RowActionIDs: []string{rid("scope.delete")},
						RefreshIntervalMs: 15000, RowClick: plugin.RowClickNavigate, Exportable: true,
						EmptyText: "This bucket has no scopes beyond the defaults."}},
				{Key: "collections", Label: "Collections", Icon: icon("table-2"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("collections.list"), Params: bucketParams},
					Config: plugin.TableConfig{Columns: collectionColumns(), DefaultSort: &plugin.SortKey{Field: "name"},
						RowActionIDs: []string{rid("collection.delete")}, RefreshIntervalMs: 15000,
						RowClick: plugin.RowClickNavigate, Exportable: true,
						EmptyText: "This bucket has no collections yet."}},
				{Key: "datamap", Label: "Data map", Icon: icon("layout-dashboard"), Type: plugin.PanelCanvas,
					Source: &plugin.DataSource{RouteID: rid("datamap"), Method: plugin.MethodWS, Params: bucketParams},
					Config: dataMapConfig()},
				{Key: "dcp", Label: "DCP streams", Icon: icon("radio"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("bucket.dcp"), Params: bucketParams},
					Config: plugin.TableConfig{Columns: dcpColumns(), DefaultSort: &plugin.SortKey{Field: "consumer"},
						RefreshIntervalMs: 5000, Exportable: true,
						EmptyText: "No DCP consumers are streaming from this bucket."}},
				{Key: "indexes", Label: "Indexes", Icon: icon("list-tree"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("indexes.list"), Params: bucketParams},
					Config: plugin.TableConfig{Columns: indexColumns(), DefaultSort: &plugin.SortKey{Field: "name"},
						RefreshIntervalMs: 15000, RowClick: plugin.RowClickNavigate, Exportable: true,
						EmptyText: "No indexes on this bucket."}},
				{Key: "query", Label: "SQL++", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor,
					Source: &plugin.DataSource{RouteID: rid("query"), Method: plugin.MethodWS, Params: bucketParams},
					Config: queryConfig("SQL++", keyspaceCatalog(keyspaceBucketExpr+" = \"${resource.name}\""), bucketParams)},
			},
		},
	}
}

func bucketSections() []plugin.ObjectDetailSection {
	return []plugin.ObjectDetailSection{
		{Title: "Bucket", Fields: []plugin.ObjectDetailField{
			{Key: "name", Label: "Name", Copy: true},
			{Key: "health", Label: "Health", Type: plugin.ColumnBadge, Severities: healthSeverities},
			{Key: "bucket_type", Label: "Type", Type: plugin.ColumnBadge},
			{Key: "storage_backend", Label: "Storage engine", Type: plugin.ColumnBadge},
			{Key: "uuid", Label: "UUID", Copy: true},
			{Key: "eviction_policy", Label: "Eviction policy"},
			{Key: "durability", Label: "Minimum durability"},
			{Key: "conflict_res", Label: "Conflict resolution"},
			{Key: "compression", Label: "Compression"},
			{Key: "flush_enabled", Label: "Flush allowed", Type: plugin.ColumnBool},
		}},
		{Title: "Capacity", Fields: []plugin.ObjectDetailField{
			{Key: "quotaPct", Label: "RAM quota", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
				PercentKey: "quotaPct", UsedKey: "memUsed", TotalKey: "quotaTotal",
				UsedType: plugin.ColumnBytes, TotalType: plugin.ColumnBytes, WarnAt: 80, CriticalAt: 95}},
			{Key: "diskUsed", Label: "Disk used", Type: plugin.ColumnBytes},
			{Key: "dataUsed", Label: "Data size", Type: plugin.ColumnBytes},
			{Key: "items", Label: "Documents", Type: plugin.ColumnNumber},
			{Key: "non_resident", Label: "Non-resident items", Type: plugin.ColumnNumber},
		}},
		{Title: "Topology", Fields: []plugin.ObjectDetailField{
			{Key: "nodes", Label: "Nodes", Type: plugin.ColumnNumber},
			{Key: "replica_topology", Label: "Serving nodes"},
			{Key: "replicas", Label: "Replicas", Type: plugin.ColumnNumber},
			{Key: "vbuckets", Label: "vBuckets", Type: plugin.ColumnNumber},
			{Key: "scopes", Label: "Scopes", Type: plugin.ColumnNumber},
			{Key: "collections", Label: "Collections", Type: plugin.ColumnNumber},
		}},
	}
}

func bucketMetricsConfig() plugin.MetricsConfig {
	return plugin.MetricsConfig{
		Stats: []plugin.MetricStat{
			{Key: "items", Label: "Documents"},
			{Key: "opsPerSec", Label: "Ops/s"},
			{Key: "diskFetches", Label: "Disk fetches/s"},
			{Key: "dcpRemaining", Label: "DCP items remaining"},
			{Key: "dcpSent", Label: "DCP items sent"},
			{Key: "queueSize", Label: "Disk queue"},
		},
		Gauges: []plugin.MetricGauge{
			{Key: "residentRatio", Label: "Resident ratio", Unit: "%"},
			{Key: "cacheMissRate", Label: "Cache miss", Unit: "%"},
		},
		Usage: []plugin.MetricUsage{
			{Key: "quotaPct", Label: "RAM quota", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
				PercentKey: "quotaPct", UsedKey: "quotaUsed", TotalKey: "quotaTotal",
				UsedType: plugin.ColumnBytes, TotalType: plugin.ColumnBytes, WarnAt: 80, CriticalAt: 95}},
		},
		Series: []plugin.MetricSeries{
			{Key: "opsPerSec", Label: "Ops/s"},
			{Key: "cmdGet", Label: "Gets/s"},
			{Key: "cmdSet", Label: "Sets/s"},
			{Key: "dcpRemaining", Label: "DCP backlog"},
		},
		History: 60,
	}
}

func scopeResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "scope", Title: "Scopes",
		List:    plugin.DataSource{RouteID: rid("scopes.list"), Params: map[string]string{"bucket": "${resource.scope}"}},
		Watch:   resourceWatchSource("scope", map[string]string{"bucket": "${resource.scope}"}),
		Columns: scopeColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{rid("collection.create")},
			Row:     []string{rid("scope.delete")},
			Detail:  []string{rid("collection.create"), rid("scope.delete")},
		},
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.scope}.${resource.name}"},
			DefaultTab: "collections",
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("scope.read"), Params: keyspaceParams()},
					Config: objectDetail([]plugin.ObjectDetailSection{{Title: "Scope", Fields: []plugin.ObjectDetailField{
						{Key: "keyspace", Label: "Keyspace", Copy: true},
						{Key: "name", Label: "Scope", Copy: true},
						{Key: "bucket", Label: "Bucket"},
						{Key: "uid", Label: "Manifest UID"},
						{Key: "collections", Label: "Collections", Type: plugin.ColumnNumber},
						{Key: "collection_names", Label: "Collection names"},
						{Key: "items", Label: "Documents", Type: plugin.ColumnNumber},
						{Key: "memUsed", Label: "Resident", Type: plugin.ColumnBytes},
						{Key: "dataSize", Label: "Data size", Type: plugin.ColumnBytes},
						{Key: "system", Label: "System scope", Type: plugin.ColumnBool},
					}}}, nil)},
				{Key: "collections", Label: "Collections", Icon: icon("table-2"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("collections.list"), Params: keyspaceParams()},
					Config: plugin.TableConfig{Columns: collectionColumns(), DefaultSort: &plugin.SortKey{Field: "name"},
						ActionIDs: []string{rid("collection.create")}, RowActionIDs: []string{rid("collection.delete")},
						RefreshIntervalMs: 15000, RowClick: plugin.RowClickNavigate, Exportable: true,
						EmptyText: "This scope has no collections yet."}},
				{Key: "query", Label: "SQL++", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor,
					Source: &plugin.DataSource{RouteID: rid("query"), Method: plugin.MethodWS, Params: keyspaceParams()},
					Config: queryConfig("SQL++", keyspaceCatalog(keyspaceBucketExpr+" = \"${resource.scope}\" AND "+
						keyspaceScopeExpr+" = \"${resource.name}\""), keyspaceParams())},
			},
		},
	}
}

func collectionResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "collection", Title: "Collections",
		List:    plugin.DataSource{RouteID: rid("collections.list"), Params: map[string]string{"bucket": "${resource.scope}"}},
		Watch:   resourceWatchSource("collection", map[string]string{"bucket": "${resource.scope}"}),
		Columns: collectionColumns(),
		Actions: plugin.ResourceActions{
			Toolbar: []string{rid("document.create")},
			Row:     []string{rid("collection.delete")},
			Detail:  []string{rid("document.create"), rid("index.create"), rid("collection.delete")},
		},
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.namespace}.${resource.name}"},
			DefaultTab: "documents",
			Tabs: []plugin.Panel{
				{Key: "documents", Label: "Documents", Icon: icon("file-json"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("documents.list"), Params: keyspaceParams()},
					Config: plugin.TableConfig{
						ColumnsSource: &plugin.DataSource{RouteID: rid("documents.columns"), Params: keyspaceParams()},
						DefaultSort:   &plugin.SortKey{Field: "id"},
						Editable:      true,
						StagedEdits:   true,
						RowKey:        []string{"id"},
						Insert:        &plugin.DataSource{RouteID: rid("document.insert"), Method: plugin.MethodPost, Params: keyspaceParams()},
						Update:        &plugin.DataSource{RouteID: rid("document.update"), Method: plugin.MethodPut, Params: keyspaceParams()},
						ActionIDs:     []string{rid("document.create")},
						RowActionIDs:  []string{rid("document.delete.row")},
						HiddenColumns: []string{"ref", "key", "flags"},
						RowClick:      plugin.RowClickNavigate,
						Exportable:    true,
						EmptyText:     "No documents in this collection yet.",
					}},
				{Key: "schema", Label: "Schema", Icon: icon("scan-text"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("collection.schema"), Params: keyspaceParams()},
					Config: plugin.TableConfig{Columns: schemaColumns(), DefaultSort: &plugin.SortKey{Field: "field"},
						RefreshIntervalMs: 60000, Exportable: true,
						EmptyText: "No fields inferred; the collection may be empty."}},
				{Key: "indexes", Label: "Indexes", Icon: icon("list-tree"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("indexes.list"), Params: keyspaceParams()},
					Config: plugin.TableConfig{Columns: indexColumns(), DefaultSort: &plugin.SortKey{Field: "name"},
						ActionIDs: []string{rid("index.create")}, RowActionIDs: []string{rid("index.drop")},
						RefreshIntervalMs: 15000, RowClick: plugin.RowClickNavigate, Exportable: true,
						EmptyText: "No indexes cover this collection; queries fall back to a primary scan."}},
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("collection.read"), Params: keyspaceParams()},
					Config: objectDetail([]plugin.ObjectDetailSection{{Title: "Collection", Fields: []plugin.ObjectDetailField{
						{Key: "keyspace", Label: "Keyspace", Copy: true},
						{Key: "path", Label: "SQL++ path", Copy: true},
						{Key: "bucket", Label: "Bucket"},
						{Key: "scope", Label: "Scope"},
						{Key: "uid", Label: "Manifest UID"},
						{Key: "items", Label: "Documents", Type: plugin.ColumnNumber},
						{Key: "memUsed", Label: "Resident", Type: plugin.ColumnBytes},
						{Key: "dataSize", Label: "Data size", Type: plugin.ColumnBytes},
						{Key: "opsPerSec", Label: "Ops/s", Type: plugin.ColumnNumber},
						{Key: "max_ttl", Label: "Expiry", Type: plugin.ColumnDuration},
						{Key: "history", Label: "Change history", Type: plugin.ColumnBool},
						{Key: "indexes", Label: "Indexes", Type: plugin.ColumnNumber},
					}}}, nil)},
				{Key: "query", Label: "SQL++", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor,
					Source: &plugin.DataSource{RouteID: rid("query"), Method: plugin.MethodWS, Params: keyspaceParams()},
					Config: queryConfig("SQL++", collectionSample(""), keyspaceParams())},
				{Key: "advisor", Label: "Index advisor", Icon: icon("lightbulb"), Type: plugin.PanelQueryEditor,
					Source: &plugin.DataSource{RouteID: rid("advisor"), Method: plugin.MethodWS, Params: keyspaceParams()},
					Config: advisorConfig(collectionSample("d.type = \"example\""))},
			},
		},
	}
}

func documentResource() plugin.ResourceType {
	documentParams := map[string]string{"id": "${resource.uid}"}
	return plugin.ResourceType{
		Kind: "document", Title: "Documents",
		List: plugin.DataSource{RouteID: rid("documents.list"), Params: map[string]string{"keyspace": "${resource.scope}"}},
		Columns: []plugin.Column{
			{Key: "id", Label: "Document ID", Sortable: true},
			{Key: "cas", Label: "CAS", Type: plugin.ColumnNumber},
			{Key: "expiration", Label: "Expiry", Type: plugin.ColumnDuration},
		},
		Actions: plugin.ResourceActions{
			Row:    []string{rid("document.delete")},
			Detail: []string{rid("document.delete")},
		},
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.name}"},
			DefaultTab: "content",
			Tabs: []plugin.Panel{
				{Key: "content", Label: "Document", Icon: icon("file-json"), Type: plugin.PanelCodeEditor,
					Source: &plugin.DataSource{RouteID: rid("document.content"), Params: documentParams},
					Config: plugin.CodeEditorConfig{
						Language:     "json",
						SaveRouteID:  rid("document.save"),
						SaveMethod:   plugin.MethodPut,
						SaveParams:   documentParams,
						SaveBodyKey:  "value",
						RefreshField: "content",
						SaveToast:    &plugin.SaveToast{Summary: "Document saved", Detail: "${response.id}", Severity: plugin.SeveritySuccess},
					}},
				{Key: "metadata", Label: "Metadata", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("document.read"), Params: documentParams},
					Config: objectDetail([]plugin.ObjectDetailSection{{Title: "Document", Fields: []plugin.ObjectDetailField{
						{Key: "key", Label: "Document ID", Copy: true},
						{Key: "keyspace", Label: "Keyspace", Copy: true},
						{Key: "cas", Label: "CAS", Type: plugin.ColumnNumber, Copy: true},
						{Key: "expiration", Label: "Expiry", Type: plugin.ColumnDuration},
						{Key: "flags", Label: "Flags", Type: plugin.ColumnNumber},
						{Key: "encoding", Label: "Encoding", Type: plugin.ColumnBadge},
						{Key: "size", Label: "Size", Type: plugin.ColumnBytes},
						{Key: "fields", Label: "Top-level fields", Type: plugin.ColumnNumber},
					}}}, nil)},
			},
		},
	}
}

func indexResource() plugin.ResourceType {
	indexParams := map[string]string{"id": "${resource.uid}"}
	return plugin.ResourceType{
		Kind: "index", Title: "Indexes",
		List:    plugin.DataSource{RouteID: rid("indexes.list")},
		Watch:   resourceWatchSource("index", nil),
		Columns: indexColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{rid("index.drop")},
			Detail: []string{rid("index.build"), rid("index.drop")},
		},
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.name}", StatusField: "status", Severities: statusSeverities},
			DefaultTab: "overview",
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("index.read"), Params: indexParams},
					Config: objectDetail([]plugin.ObjectDetailSection{
						{Title: "Index", Fields: []plugin.ObjectDetailField{
							{Key: "name", Label: "Name", Copy: true},
							{Key: "keyspace", Label: "Keyspace", Copy: true},
							{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: statusSeverities},
							{Key: "state", Label: "Catalog state", Type: plugin.ColumnBadge, Severities: statusSeverities},
							{Key: "progress", Label: "Build progress", Type: plugin.ColumnPercent},
							{Key: "primary", Label: "Primary", Type: plugin.ColumnBool},
							{Key: "index_keys", Label: "Index keys"},
							{Key: "condition", Label: "Partial index condition"},
							{Key: "partition", Label: "Partitioning"},
						}},
						{Title: "Placement", Fields: []plugin.ObjectDetailField{
							{Key: "storage_mode", Label: "Storage engine", Type: plugin.ColumnBadge},
							{Key: "using", Label: "Indexer", Type: plugin.ColumnBadge},
							{Key: "hosts", Label: "Hosts"},
							{Key: "replicas", Label: "Replicas", Type: plugin.ColumnNumber},
							{Key: "replica_id", Label: "Replica id", Type: plugin.ColumnNumber},
							{Key: "stale", Label: "Stale", Type: plugin.ColumnBool},
							{Key: "last_scan_time", Label: "Last scan"},
							{Key: "definition", Label: "Definition", Copy: true},
						}},
					}, nil)},
				{Key: "definition", Label: "Definition", Icon: icon("file-code"), Type: plugin.PanelDocument,
					Source: &plugin.DataSource{RouteID: rid("index.read"), Params: indexParams}},
			},
		},
	}
}

func replicationResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind: "replication", Title: "XDCR",
		List:    plugin.DataSource{RouteID: rid("replications.list")},
		Watch:   resourceWatchSource("replication", nil),
		Columns: replicationColumns(),
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.name}", StatusField: "status", Severities: statusSeverities},
			DefaultTab: "overview",
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: rid("replication.read"), Params: map[string]string{"id": "${resource.uid}"}},
					Config: objectDetail([]plugin.ObjectDetailSection{{Title: "Replication", Fields: []plugin.ObjectDetailField{
						{Key: "id", Label: "Replication id", Copy: true},
						{Key: "source", Label: "Source bucket"},
						{Key: "target", Label: "Target"},
						{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: statusSeverities},
						{Key: "replication_type", Label: "Protocol", Type: plugin.ColumnBadge},
						{Key: "continuous", Label: "Continuous", Type: plugin.ColumnBool},
						{Key: "filter", Label: "Filter expression"},
						{Key: "changes_left", Label: "Changes left", Type: plugin.ColumnNumber},
						{Key: "docs_checked", Label: "Docs checked", Type: plugin.ColumnNumber},
						{Key: "docs_written", Label: "Docs written", Type: plugin.ColumnNumber},
						{Key: "errors", Label: "Errors", Type: plugin.ColumnNumber},
					}}}, nil)},
				{Key: "remotes", Label: "Remote clusters", Icon: icon("globe"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: rid("remotes.list")},
					Config: plugin.TableConfig{Columns: remoteColumns(), DefaultSort: &plugin.SortKey{Field: "name"},
						RefreshIntervalMs: 30000, Exportable: true,
						EmptyText: "No remote cluster references are configured."}},
				{Key: "activity", Label: "Activity", Icon: icon("history"), Type: plugin.PanelTimeline,
					Source: &plugin.DataSource{RouteID: rid("events.list"), Params: map[string]string{"filter": "replication"}},
					Config: timelineConfig(map[string]string{"filter": "replication"},
						"No replication, rebalance, or failover events recorded yet.")},
			},
		},
	}
}

func actions() []plugin.Action {
	return []plugin.Action{
		{ID: rid("bucket.create"), Label: "Create bucket", Icon: icon("plus"), RouteID: rid("bucket.create"),
			OnSuccess: &plugin.ActionSuccess{SelectTab: "buckets"}},
		{ID: rid("bucket.update"), Label: "Edit bucket", Icon: icon("settings-2"), RouteID: rid("bucket.update"),
			Params:    map[string]string{"bucket": "${resource.name}"},
			OnSuccess: &plugin.ActionSuccess{SelectTab: "overview"}},
		{ID: rid("bucket.compact"), Label: "Compact", Icon: icon("archive-restore"), RouteID: rid("bucket.compact"),
			Params: map[string]string{"bucket": "${resource.name}"}, Group: "Maintenance",
			Confirm: true, ConfirmText: "Start compaction now? It runs in the background and adds disk and CPU load."},
		{ID: rid("bucket.flush"), Label: "Flush", Icon: icon("eraser"), RouteID: rid("bucket.flush"),
			Params: map[string]string{"bucket": "${resource.name}"}, Group: "Maintenance",
			Confirm: true, ConfirmText: "Delete every document in this bucket? The data cannot be recovered."},
		{ID: rid("bucket.delete"), Label: "Delete bucket", Icon: icon("trash-2"), RouteID: rid("bucket.delete"),
			Params: map[string]string{"bucket": "${resource.name}"}, Bulk: true,
			Confirm: true, ConfirmText: "Delete this bucket with all of its scopes, collections, documents, and indexes?",
			OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList}},

		{ID: rid("scope.create"), Label: "Create scope", Icon: icon("folder-plus"), RouteID: rid("scope.create"),
			Params: map[string]string{"bucket": "${resource.name}"}, OnSuccess: &plugin.ActionSuccess{SelectTab: "scopes"}},
		{ID: rid("scope.delete"), Label: "Drop scope", Icon: icon("trash-2"), RouteID: rid("scope.delete"),
			Params: keyspaceParams(), Bulk: true,
			Confirm: true, ConfirmText: "Drop this scope with every collection and document inside it?",
			OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList}},

		{ID: rid("collection.create"), Label: "Create collection", Icon: icon("plus"), RouteID: rid("collection.create"),
			Params: keyspaceParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "collections"}},
		{ID: rid("collection.delete"), Label: "Drop collection", Icon: icon("trash-2"), RouteID: rid("collection.delete"),
			Params: keyspaceParams(), Bulk: true,
			Confirm: true, ConfirmText: "Drop this collection and every document it holds?",
			OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList}},

		{ID: rid("document.create"), Label: "New document", Icon: icon("file-plus"), RouteID: rid("document.insert"),
			Params: keyspaceParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "documents"}},
		{ID: rid("document.delete.row"), Label: "Delete document", Icon: icon("trash-2"), RouteID: rid("document.remove"),
			Params: map[string]string{"id": "${record.ref.uid}"}, Bulk: true,
			Confirm: true, ConfirmText: "Delete the selected document(s)? Couchbase cannot undo this."},
		{ID: rid("document.delete"), Label: "Delete document", Icon: icon("trash-2"), RouteID: rid("document.remove"),
			Params: map[string]string{"id": "${resource.uid}"}, Bulk: true,
			Confirm: true, ConfirmText: "Delete this document? Couchbase cannot undo this.",
			OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList}},

		{ID: rid("index.create"), Label: "Create index", Icon: icon("plus"), RouteID: rid("index.create"),
			Params: keyspaceParams(), OnSuccess: &plugin.ActionSuccess{SelectTab: "indexes"}},
		{ID: rid("index.build"), Label: "Build index", Icon: icon("hammer"), RouteID: rid("index.build"),
			Params: map[string]string{"id": "${resource.uid}"}, Group: "Maintenance",
			EnabledWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "status", Op: plugin.OpNeq, Value: "ready"}}}},
		{ID: rid("index.drop"), Label: "Drop index", Icon: icon("trash-2"), RouteID: rid("index.drop"),
			Params: map[string]string{"id": "${resource.uid}"}, Bulk: true,
			Confirm: true, ConfirmText: "Drop this index? Queries that relied on it fall back to a primary scan or fail."},

		{ID: rid("query.save"), Label: "Save query", Icon: icon("bookmark"), RouteID: rid("query.save"),
			OnSuccess: &plugin.ActionSuccess{SelectTab: "saved"}},
		{ID: rid("query.delete"), Label: "Delete saved query", Icon: icon("trash-2"), RouteID: rid("query.delete"),
			Params: map[string]string{"id": "${record.id}"}, Bulk: true,
			Confirm: true, ConfirmText: "Delete the selected saved statement(s)?"},

		{ID: rid("console.open"), Label: "Web console", Icon: icon("external-link"),
			RouteID: rid("console.url"), Open: plugin.OpenURL},
	}
}
