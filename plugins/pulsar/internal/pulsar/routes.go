package pulsar

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

type row = plugin.TableRow

type actionResult struct {
	OK bool `json:"ok"`
}

func routes() []plugin.Route {
	return []plugin.Route{
		{ID: "pulsar.overview", Method: plugin.MethodGet, Path: "/overview", Permission: "pulsar.read", Risk: plugin.RiskSafe, AuditEvent: "pulsar.overview", Handle: overview},

		{ID: "pulsar.tenants.tree", Method: plugin.MethodGet, Path: "/tree/tenants", Permission: "pulsar.tenants.read", Risk: plugin.RiskSafe, AuditEvent: "pulsar.tenants.tree", Handle: tenantsTree},
		{ID: "pulsar.namespaces.tree", Method: plugin.MethodGet, Path: "/tree/tenants/{tenant}", Permission: "pulsar.namespaces.read", Risk: plugin.RiskSafe, AuditEvent: "pulsar.namespaces.tree", Handle: namespacesTree},
		{ID: "pulsar.topics.tree", Method: plugin.MethodGet, Path: "/tree/tenants/{tenant}/{namespace}", Permission: "pulsar.topics.read", Risk: plugin.RiskSafe, AuditEvent: "pulsar.topics.tree", Handle: topicsTree},

		{ID: "pulsar.tenants.list", Method: plugin.MethodGet, Path: "/tenants", Permission: "pulsar.tenants.read", Risk: plugin.RiskSafe, AuditEvent: "pulsar.tenants.list", Handle: listTenants},
		{ID: "pulsar.tenant.read", Method: plugin.MethodGet, Path: "/tenants/{tenant}", Permission: "pulsar.tenants.read", Risk: plugin.RiskSafe, AuditEvent: "pulsar.tenant.read", Handle: readTenant},
		{ID: "pulsar.tenant.create", Method: plugin.MethodPost, Path: "/tenants", Permission: "pulsar.tenants.write", Risk: plugin.RiskWrite, AuditEvent: "pulsar.tenant.create", Input: tenantCreateSchema(), Handle: createTenant},
		{ID: "pulsar.tenant.update", Method: plugin.MethodPut, Path: "/tenants/{tenant}", Permission: "pulsar.tenants.write", Risk: plugin.RiskWrite, AuditEvent: "pulsar.tenant.update", Input: tenantUpdateSchema(), Handle: updateTenant},
		{ID: "pulsar.tenant.delete", Method: plugin.MethodDelete, Path: "/tenants/{tenant}", Permission: "pulsar.tenants.delete", Risk: plugin.RiskDestructive, AuditEvent: "pulsar.tenant.delete", Handle: deleteTenant},

		{ID: "pulsar.namespaces.list", Method: plugin.MethodGet, Path: "/tenants/{tenant}/namespaces", Permission: "pulsar.namespaces.read", Risk: plugin.RiskSafe, AuditEvent: "pulsar.namespaces.list", Handle: listNamespaces},
		{ID: "pulsar.namespace.policies", Method: plugin.MethodGet, Path: "/tenants/{tenant}/namespaces/{namespace}", Permission: "pulsar.namespaces.read", Risk: plugin.RiskSafe, AuditEvent: "pulsar.namespace.policies", Handle: namespacePolicies},
		{ID: "pulsar.namespace.create", Method: plugin.MethodPost, Path: "/tenants/{tenant}/namespaces", Permission: "pulsar.namespaces.write", Risk: plugin.RiskWrite, AuditEvent: "pulsar.namespace.create", Input: namespaceCreateSchema(), Handle: createNamespace},
		{ID: "pulsar.namespace.retention", Method: plugin.MethodPost, Path: "/tenants/{tenant}/namespaces/{namespace}/retention", Permission: "pulsar.namespaces.write", Risk: plugin.RiskWrite, AuditEvent: "pulsar.namespace.retention", Input: retentionSchema(), Handle: setNamespaceRetention},
		{ID: "pulsar.namespace.unload", Method: plugin.MethodPost, Path: "/tenants/{tenant}/namespaces/{namespace}/unload", Permission: "pulsar.namespaces.write", Risk: plugin.RiskDestructive, AuditEvent: "pulsar.namespace.unload", Handle: unloadNamespace},
		{ID: "pulsar.namespace.delete", Method: plugin.MethodDelete, Path: "/tenants/{tenant}/namespaces/{namespace}", Permission: "pulsar.namespaces.delete", Risk: plugin.RiskDestructive, AuditEvent: "pulsar.namespace.delete", Handle: deleteNamespace},

		{ID: "pulsar.topics.list", Method: plugin.MethodGet, Path: "/tenants/{tenant}/namespaces/{namespace}/topics", Permission: "pulsar.topics.read", Risk: plugin.RiskSafe, AuditEvent: "pulsar.topics.list", Handle: listTopics},
		{ID: "pulsar.topic.stats", Method: plugin.MethodGet, Path: "/topics/{topic}/stats", Permission: "pulsar.topics.read", Risk: plugin.RiskSafe, AuditEvent: "pulsar.topic.stats", Handle: readTopicStats},
		{ID: "pulsar.topic.internal_stats", Method: plugin.MethodGet, Path: "/topics/{topic}/internal-stats", Permission: "pulsar.topics.read", Risk: plugin.RiskSafe, AuditEvent: "pulsar.topic.internal_stats", Handle: readTopicInternalStats},
		{ID: "pulsar.topic.partitions", Method: plugin.MethodGet, Path: "/topics/{topic}/partitions", Permission: "pulsar.topics.read", Risk: plugin.RiskSafe, AuditEvent: "pulsar.topic.partitions", Handle: listPartitions},
		{ID: "pulsar.topic.schema", Method: plugin.MethodGet, Path: "/topics/{topic}/schema", Permission: "pulsar.schemas.read", Risk: plugin.RiskSafe, AuditEvent: "pulsar.topic.schema", Handle: readTopicSchema},
		{ID: "pulsar.topic.create", Method: plugin.MethodPost, Path: "/tenants/{tenant}/namespaces/{namespace}/topics", Permission: "pulsar.topics.write", Risk: plugin.RiskWrite, AuditEvent: "pulsar.topic.create", Input: topicCreateSchema(), Handle: createTopic},
		{ID: "pulsar.topic.compact", Method: plugin.MethodPost, Path: "/topics/{topic}/compaction", Permission: "pulsar.topics.write", Risk: plugin.RiskWrite, AuditEvent: "pulsar.topic.compact", Handle: compactTopic},
		{ID: "pulsar.topic.unload", Method: plugin.MethodPost, Path: "/topics/{topic}/unload", Permission: "pulsar.topics.write", Risk: plugin.RiskDestructive, AuditEvent: "pulsar.topic.unload", Handle: unloadTopic},
		{ID: "pulsar.topic.terminate", Method: plugin.MethodPost, Path: "/topics/{topic}/terminate", Permission: "pulsar.topics.write", Risk: plugin.RiskDestructive, AuditEvent: "pulsar.topic.terminate", Handle: terminateTopic},
		{ID: "pulsar.topic.delete", Method: plugin.MethodDelete, Path: "/topics/{topic}", Permission: "pulsar.topics.delete", Risk: plugin.RiskDestructive, AuditEvent: "pulsar.topic.delete", Handle: deleteTopic},
		{ID: "pulsar.topic.schema.delete", Method: plugin.MethodDelete, Path: "/topics/{topic}/schema", Permission: "pulsar.schemas.delete", Risk: plugin.RiskDestructive, AuditEvent: "pulsar.topic.schema.delete", Handle: deleteTopicSchema},

		{ID: "pulsar.subscriptions.list", Method: plugin.MethodGet, Path: "/topics/{topic}/subscriptions", Permission: "pulsar.subscriptions.read", Risk: plugin.RiskSafe, AuditEvent: "pulsar.subscriptions.list", Handle: listSubscriptions},
		{ID: "pulsar.subscription.read", Method: plugin.MethodGet, Path: "/topics/{topic}/subscriptions/{subscription}", Permission: "pulsar.subscriptions.read", Risk: plugin.RiskSafe, AuditEvent: "pulsar.subscription.read", Handle: readSubscription},
		{ID: "pulsar.producers.list", Method: plugin.MethodGet, Path: "/topics/{topic}/producers", Permission: "pulsar.topics.read", Risk: plugin.RiskSafe, AuditEvent: "pulsar.producers.list", Handle: listProducers},
		{ID: "pulsar.consumers.list", Method: plugin.MethodGet, Path: "/topics/{topic}/consumers", Permission: "pulsar.subscriptions.read", Risk: plugin.RiskSafe, AuditEvent: "pulsar.consumers.list", Handle: listConsumers},
		{ID: "pulsar.subscription.create", Method: plugin.MethodPost, Path: "/topics/{topic}/subscriptions", Permission: "pulsar.subscriptions.write", Risk: plugin.RiskWrite, AuditEvent: "pulsar.subscription.create", Input: subscriptionCreateSchema(), Handle: createSubscription},
		{ID: "pulsar.subscription.skip", Method: plugin.MethodPost, Path: "/topics/{topic}/subscriptions/{subscription}/skip", Permission: "pulsar.subscriptions.write", Risk: plugin.RiskDestructive, AuditEvent: "pulsar.subscription.skip", Input: skipSchema(), Handle: skipMessages},
		{ID: "pulsar.subscription.clear", Method: plugin.MethodPost, Path: "/topics/{topic}/subscriptions/{subscription}/clear", Permission: "pulsar.subscriptions.write", Risk: plugin.RiskDestructive, AuditEvent: "pulsar.subscription.clear", Handle: clearBacklog},
		{ID: "pulsar.subscription.reset", Method: plugin.MethodPost, Path: "/topics/{topic}/subscriptions/{subscription}/reset", Permission: "pulsar.subscriptions.write", Risk: plugin.RiskDestructive, AuditEvent: "pulsar.subscription.reset", Input: resetCursorSchema(), Handle: resetCursor},
		{ID: "pulsar.subscription.expire", Method: plugin.MethodPost, Path: "/topics/{topic}/subscriptions/{subscription}/expire", Permission: "pulsar.subscriptions.write", Risk: plugin.RiskDestructive, AuditEvent: "pulsar.subscription.expire", Input: expireSchema(), Handle: expireMessages},
		{ID: "pulsar.subscription.delete", Method: plugin.MethodDelete, Path: "/topics/{topic}/subscriptions/{subscription}", Permission: "pulsar.subscriptions.delete", Risk: plugin.RiskDestructive, AuditEvent: "pulsar.subscription.delete", Handle: deleteSubscription},

		{ID: "pulsar.messages.list", Method: plugin.MethodGet, Path: "/topics/{topic}/messages", Permission: "pulsar.messages.read", Risk: plugin.RiskSafe, AuditEvent: "pulsar.messages.list", Handle: listMessages},
		{ID: "pulsar.messages.positions", Method: plugin.MethodGet, Path: "/messages/positions", Permission: "pulsar.messages.read", Risk: plugin.RiskSafe, AuditEvent: "pulsar.messages.positions", Handle: messagePositions},
		{ID: "pulsar.message.produce", Method: plugin.MethodPost, Path: "/topics/{topic}/messages", Permission: "pulsar.messages.write", Risk: plugin.RiskWrite, AuditEvent: "pulsar.message.produce", Input: produceSchema(), Handle: produceMessage},
		{ID: "pulsar.messages.consume", Method: plugin.MethodWS, Path: "/topics/{topic}/consume", Permission: "pulsar.messages.read", Risk: plugin.RiskSafe, AuditEvent: "pulsar.messages.consume", Stream: consumeStream},

		{ID: "pulsar.clusters.list", Method: plugin.MethodGet, Path: "/clusters", Permission: "pulsar.clusters.read", Risk: plugin.RiskSafe, AuditEvent: "pulsar.clusters.list", Handle: listClusters},
		{ID: "pulsar.cluster.read", Method: plugin.MethodGet, Path: "/clusters/{cluster}", Permission: "pulsar.clusters.read", Risk: plugin.RiskSafe, AuditEvent: "pulsar.cluster.read", Handle: readCluster},
		{ID: "pulsar.brokers.list", Method: plugin.MethodGet, Path: "/brokers", Permission: "pulsar.brokers.read", Risk: plugin.RiskSafe, AuditEvent: "pulsar.brokers.list", Handle: listBrokers},
		{ID: "pulsar.broker.read", Method: plugin.MethodGet, Path: "/brokers/{broker}", Permission: "pulsar.brokers.read", Risk: plugin.RiskSafe, AuditEvent: "pulsar.broker.read", Handle: readBroker},
	}
}

func tenantCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Tenant", Fields: []plugin.Field{
		{Key: "name", Label: "Tenant name", Type: plugin.FieldText, Required: true, Placeholder: "analytics"},
		{Key: "admin_roles", Label: "Admin roles", Type: plugin.FieldArray, AddLabel: "Add role", Item: &plugin.Field{Type: plugin.FieldText, Placeholder: "team-admin"}},
		{Key: "allowed_clusters", Label: "Allowed clusters", Type: plugin.FieldMultiSelect, Required: true, OptionsSource: &plugin.DataSource{RouteID: "pulsar.clusters.list"}},
	}}}}
}

func tenantUpdateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Tenant", Fields: []plugin.Field{
		{Key: "admin_roles", Label: "Admin roles", Type: plugin.FieldArray, AddLabel: "Add role", Item: &plugin.Field{Type: plugin.FieldText, Placeholder: "team-admin"}},
		{Key: "allowed_clusters", Label: "Allowed clusters", Type: plugin.FieldMultiSelect, Required: true, OptionsSource: &plugin.DataSource{RouteID: "pulsar.clusters.list"}},
	}}}}
}

func namespaceCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Namespace", Fields: []plugin.Field{
		{Key: "name", Label: "Namespace name", Type: plugin.FieldText, Required: true, Placeholder: "events"},
		{Key: "bundles", Label: "Bundles", Type: plugin.FieldNumber, Help: "Number of hash bundles the namespace is split into. Leave empty for the broker default.", Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}}},
		{Key: "replication_clusters", Label: "Replication clusters", Type: plugin.FieldMultiSelect, OptionsSource: &plugin.DataSource{RouteID: "pulsar.clusters.list"}},
	}}}}
}

func retentionSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Retention", Fields: []plugin.Field{
		{Key: "time_minutes", Label: "Retention time (minutes)", Type: plugin.FieldNumber, Required: true, Default: 0, Help: "0 disables time retention; -1 keeps messages forever."},
		{Key: "size_mb", Label: "Retention size (MB)", Type: plugin.FieldNumber, Required: true, Default: 0, Help: "0 disables size retention; -1 means unlimited."},
	}}}}
}

func topicCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Topic", Fields: []plugin.Field{
		{Key: "name", Label: "Topic name", Type: plugin.FieldText, Required: true, Placeholder: "orders"},
		{Key: "domain", Label: "Persistence", Type: plugin.FieldSelect, Required: true, Default: persistentDomain, Options: []plugin.Option{
			{Label: "Persistent", Value: persistentDomain},
			{Label: "Non-persistent", Value: nonPersistentDomain},
		}},
		{Key: "partitions", Label: "Partitions", Type: plugin.FieldNumber, Default: 0, Help: "0 creates a non-partitioned topic.", Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 0}}},
	}}}}
}

func subscriptionCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Subscription", Fields: []plugin.Field{
		{Key: "name", Label: "Subscription name", Type: plugin.FieldText, Required: true, Placeholder: "billing-workers"},
		{Key: "position", Label: "Start from", Type: plugin.FieldSelect, Required: true, Default: positionLatest, Options: positionOptions()},
	}}}}
}

func skipSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Skip messages", Fields: []plugin.Field{
		{Key: "count", Label: "Messages to skip", Type: plugin.FieldNumber, Required: true, Default: 1, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}}, Help: "Acknowledges the next N messages without delivering them."},
	}}}}
}

func resetCursorSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Reset cursor", Fields: []plugin.Field{
		{Key: "target", Label: "Reset to", Type: plugin.FieldSelect, Required: true, Default: positionEarliest, Options: []plugin.Option{
			{Label: "Earliest (replay everything)", Value: positionEarliest},
			{Label: "Latest (drop the backlog)", Value: positionLatest},
			{Label: "A point in time", Value: "time"},
		}},
		{Key: "age", Label: "How far back", Type: plugin.FieldDuration, Default: "1h", Help: "Used when resetting to a point in time; the cursor moves to now minus this duration.",
			VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "target", Op: plugin.OpEq, Value: "time"}}}},
	}}}}
}

func expireSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Expire messages", Fields: []plugin.Field{
		{Key: "age", Label: "Older than", Type: plugin.FieldDuration, Required: true, Default: "24h", Help: "Acknowledges every backlog message published before now minus this duration."},
	}}}}
}

func produceSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Message", Fields: []plugin.Field{
		{Key: "key", Label: "Key", Type: plugin.FieldText, Help: "Routing key used by partitioned topics and key-shared subscriptions."},
		{Key: "payload", Label: "Payload", Type: plugin.FieldTextarea, Required: true},
		{Key: "encoding", Label: "Payload encoding", Type: plugin.FieldSelect, Required: true, Default: "string", Options: []plugin.Option{
			{Label: "String", Value: "string"},
			{Label: "Base64", Value: "base64"},
		}},
		{Key: "properties", Label: "Properties", Type: plugin.FieldMap, KeyPlaceholder: "property", AddLabel: "Add property", Item: &plugin.Field{Type: plugin.FieldText}},
	}}}}
}

func pulsarSession(rc *plugin.RequestContext) (*Session, error) {
	return unwrap(rc.Session)
}

// param reads a renderer-supplied value from the resolved route params, falling
// back to the raw p.<name> query the gateway builds them from.
func param(rc *plugin.RequestContext, name string) string {
	if v := strings.TrimSpace(rc.Param(name)); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p." + name))
}

func scopeTenant(rc *plugin.RequestContext, s *Session) string {
	if v := param(rc, "tenant"); v != "" {
		return v
	}
	return s.opts.Tenant
}

func scopeNamespace(rc *plugin.RequestContext, s *Session) string {
	if v := param(rc, "namespace"); v != "" {
		return v
	}
	return s.opts.Namespace
}

// enrichConcurrency bounds the per-row detail lookups a listing route may run.
const enrichConcurrency = 8

// enrichRows fills in per-item detail for one already-sliced page. Enrichment
// costs a round trip per row, so it must never run over the whole dataset;
// columns it produces are therefore not sortable or searchable.
func enrichRows(ctx context.Context, rows []row, fill func(context.Context, row)) {
	if len(rows) == 0 {
		return
	}
	sem := make(chan struct{}, min(enrichConcurrency, len(rows)))
	var wg sync.WaitGroup
	for _, r := range rows {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			fill(ctx, r)
		}()
	}
	wg.Wait()
}

func overview(rc *plugin.RequestContext) (any, error) {
	s, err := pulsarSession(rc)
	if err != nil {
		return nil, err
	}
	out := row{
		"admin_url":   s.opts.AdminURL,
		"service_url": s.opts.ServiceURL,
		"tenant":      s.opts.Tenant,
		"namespace":   s.opts.Namespace,
		"readOnly":    s.opts.ReadOnly,
	}
	var clusters []string
	if err := s.get(rc.Ctx, "/admin/v2/clusters", nil, &clusters); err == nil {
		out["clusters"] = clusters
	}
	var brokers []string
	if err := s.get(rc.Ctx, "/admin/v2/brokers", nil, &brokers); err == nil {
		out["brokers"] = len(brokers)
	}
	// /brokers/version answers text/plain, so it is read raw rather than decoded.
	if _, data, err := s.raw(rc.Ctx, "/admin/v2/brokers/version", nil); err == nil {
		out["version"] = strings.TrimSpace(string(data))
	}
	return out, nil
}

func tenantsTree(rc *plugin.RequestContext) (any, error) {
	page, err := tenantsPage(rc, false)
	if err != nil {
		return nil, err
	}
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name := fmt.Sprint(item["name"])
		nodes = append(nodes, plugin.TreeNode{
			Key:            "tenant:" + name,
			Label:          name,
			Icon:           icon("building-2"),
			ChildrenSource: &plugin.DataSource{RouteID: "pulsar.namespaces.tree", Params: map[string]string{"tenant": name}},
			ResourceKind:   "namespace",
			ListParams:     map[string]string{"tenant": name},
			Data:           map[string]any{"tenant": name},
		})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

func namespacesTree(rc *plugin.RequestContext) (any, error) {
	page, err := namespacesPage(rc, false)
	if err != nil {
		return nil, err
	}
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		tenant, name := fmt.Sprint(item["tenant"]), fmt.Sprint(item["name"])
		ref := namespaceRef(tenant, name)
		nodes = append(nodes, plugin.TreeNode{
			Key:            "namespace:" + ref.UID,
			Label:          name,
			Icon:           icon("folder"),
			Ref:            &ref,
			ChildrenSource: &plugin.DataSource{RouteID: "pulsar.topics.tree", Params: map[string]string{"tenant": tenant, "namespace": name}},
			Data:           map[string]any{"tenant": tenant, "namespace": name},
		})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

func topicsTree(rc *plugin.RequestContext) (any, error) {
	page, err := topicsPage(rc, false)
	if err != nil {
		return nil, err
	}
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		ref, ok := item["ref"].(plugin.ResourceIdentity)
		if !ok {
			continue
		}
		nodes = append(nodes, plugin.TreeNode{
			Key:   "topic:" + ref.UID,
			Label: ref.Name,
			Icon:  icon(topicIconName(fmt.Sprint(item["domain"]), item["partitioned"] == true)),
			Ref:   &ref,
			Leaf:  true,
			Data:  map[string]any{"topic": ref.UID, "partitioned": item["partitioned"]},
		})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

func topicIconName(domain string, partitioned bool) string {
	if partitioned {
		return "columns-3"
	}
	if domain == nonPersistentDomain {
		return "zap"
	}
	return "radio-tower"
}

func tenantsPage(rc *plugin.RequestContext, withDetail bool) (plugin.Page[row], error) {
	s, err := pulsarSession(rc)
	if err != nil {
		return plugin.Page[row]{}, err
	}
	var names []string
	if err := s.get(rc.Ctx, "/admin/v2/tenants", nil, &names); err != nil {
		return plugin.Page[row]{}, err
	}
	sort.Strings(names)
	rows := make([]row, 0, len(names))
	for _, name := range names {
		rows = append(rows, row{"name": name, "ref": tenantRef(name)})
	}
	page, err := broker.PageRows(rc, rows)
	if err != nil {
		return plugin.Page[row]{}, err
	}
	if !withDetail {
		return page, nil
	}
	enrichRows(rc.Ctx, page.Items, func(ctx context.Context, r row) {
		info, err := s.tenantInfo(ctx, fmt.Sprint(r["name"]))
		if err != nil {
			return
		}
		r["admin_roles"], r["allowed_clusters"] = info.AdminRoles, info.AllowedClusters
	})
	return page, nil
}

func listTenants(rc *plugin.RequestContext) (any, error) {
	return tenantsPage(rc, true)
}

type tenantInfo struct {
	AdminRoles      []string `json:"adminRoles"`
	AllowedClusters []string `json:"allowedClusters"`
}

func (s *Session) tenantInfo(ctx context.Context, tenant string) (tenantInfo, error) {
	var out tenantInfo
	err := s.get(ctx, "/admin/v2/tenants/"+url.PathEscape(tenant), nil, &out)
	return out, err
}

func readTenant(rc *plugin.RequestContext) (any, error) {
	s, err := pulsarSession(rc)
	if err != nil {
		return nil, err
	}
	tenant := scopeTenant(rc, s)
	info, err := s.tenantInfo(rc.Ctx, tenant)
	if err != nil {
		return nil, err
	}
	out := row{"name": tenant, "admin_roles": info.AdminRoles, "allowed_clusters": info.AllowedClusters}
	var namespaces []string
	if err := s.get(rc.Ctx, "/admin/v2/namespaces/"+url.PathEscape(tenant), nil, &namespaces); err == nil {
		out["namespaces"] = len(namespaces)
	}
	return out, nil
}

type tenantRequest struct {
	Name            string   `json:"name"`
	AdminRoles      []string `json:"admin_roles"`
	AllowedClusters []string `json:"allowed_clusters"`
}

func (t tenantRequest) body() row {
	return row{"adminRoles": nonNilStrings(t.AdminRoles), "allowedClusters": nonNilStrings(t.AllowedClusters)}
}

func createTenant(rc *plugin.RequestContext) (any, error) {
	s, err := writableSession(rc)
	if err != nil {
		return nil, err
	}
	var req tenantRequest
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name, err := simpleName("tenant", req.Name)
	if err != nil {
		return nil, err
	}
	if len(req.AllowedClusters) == 0 {
		return nil, fmt.Errorf("%w: at least one allowed cluster is required", plugin.ErrInvalidInput)
	}
	if err := s.send(rc.Ctx, http.MethodPut, "/admin/v2/tenants/"+url.PathEscape(name), nil, req.body(), nil); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func updateTenant(rc *plugin.RequestContext) (any, error) {
	s, err := writableSession(rc)
	if err != nil {
		return nil, err
	}
	var req tenantRequest
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	tenant, err := simpleName("tenant", param(rc, "tenant"))
	if err != nil {
		return nil, err
	}
	if len(req.AllowedClusters) == 0 {
		return nil, fmt.Errorf("%w: at least one allowed cluster is required", plugin.ErrInvalidInput)
	}
	if err := s.send(rc.Ctx, http.MethodPost, "/admin/v2/tenants/"+url.PathEscape(tenant), nil, req.body(), nil); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func deleteTenant(rc *plugin.RequestContext) (any, error) {
	s, err := writableSession(rc)
	if err != nil {
		return nil, err
	}
	tenant, err := simpleName("tenant", param(rc, "tenant"))
	if err != nil {
		return nil, err
	}
	if err := s.send(rc.Ctx, http.MethodDelete, "/admin/v2/tenants/"+url.PathEscape(tenant), nil, nil, nil); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func namespacesPage(rc *plugin.RequestContext, withPolicies bool) (plugin.Page[row], error) {
	s, err := pulsarSession(rc)
	if err != nil {
		return plugin.Page[row]{}, err
	}
	tenant, err := simpleName("tenant", scopeTenant(rc, s))
	if err != nil {
		return plugin.Page[row]{}, err
	}
	var qualified []string
	if err := s.get(rc.Ctx, "/admin/v2/namespaces/"+url.PathEscape(tenant), nil, &qualified); err != nil {
		return plugin.Page[row]{}, err
	}
	sort.Strings(qualified)
	rows := make([]row, 0, len(qualified))
	for _, name := range qualified {
		// The broker answers with fully qualified "tenant/namespace" names.
		short := name
		if _, rest, ok := strings.Cut(name, "/"); ok {
			short = rest
		}
		rows = append(rows, row{"name": short, "tenant": tenant, "ref": namespaceRef(tenant, short)})
	}
	page, err := broker.PageRows(rc, rows)
	if err != nil {
		return plugin.Page[row]{}, err
	}
	if !withPolicies {
		return page, nil
	}
	enrichRows(rc.Ctx, page.Items, func(ctx context.Context, r row) {
		var policies namespacePolicyDoc
		if err := s.get(ctx, namespacePath(tenant, fmt.Sprint(r["name"])), nil, &policies); err != nil {
			return
		}
		r["message_ttl"] = policies.MessageTTLInSeconds
		r["replication_clusters"] = policies.ReplicationClusters
		r["bundles"] = policies.Bundles.NumBundles
		if policies.Retention != nil {
			r["retention_minutes"] = policies.Retention.TimeInMinutes
			r["retention_size"] = policies.Retention.SizeInMB
		}
	})
	return page, nil
}

type namespacePolicyDoc struct {
	MessageTTLInSeconds *int     `json:"message_ttl_in_seconds"`
	ReplicationClusters []string `json:"replication_clusters"`
	Bundles             struct {
		NumBundles int `json:"numBundles"`
	} `json:"bundles"`
	Retention *struct {
		TimeInMinutes int   `json:"retentionTimeInMinutes"`
		SizeInMB      int64 `json:"retentionSizeInMB"`
	} `json:"retention_policies"`
}

func listNamespaces(rc *plugin.RequestContext) (any, error) {
	return namespacesPage(rc, true)
}

func namespacePath(tenant, namespace string) string {
	return "/admin/v2/namespaces/" + url.PathEscape(tenant) + "/" + url.PathEscape(namespace)
}

func namespaceScope(rc *plugin.RequestContext) (*Session, string, string, error) {
	s, err := pulsarSession(rc)
	if err != nil {
		return nil, "", "", err
	}
	tenant, err := simpleName("tenant", scopeTenant(rc, s))
	if err != nil {
		return nil, "", "", err
	}
	namespace, err := simpleName("namespace", scopeNamespace(rc, s))
	if err != nil {
		return nil, "", "", err
	}
	return s, tenant, namespace, nil
}

func namespacePolicies(rc *plugin.RequestContext) (any, error) {
	s, tenant, namespace, err := namespaceScope(rc)
	if err != nil {
		return nil, err
	}
	var out row
	if err := s.get(rc.Ctx, namespacePath(tenant, namespace), nil, &out); err != nil {
		return nil, err
	}
	if out == nil {
		out = row{}
	}
	out["name"], out["tenant"], out["namespace"] = tenant+"/"+namespace, tenant, namespace
	return out, nil
}

func createNamespace(rc *plugin.RequestContext) (any, error) {
	s, err := writableSession(rc)
	if err != nil {
		return nil, err
	}
	tenant, err := simpleName("tenant", param(rc, "tenant"))
	if err != nil {
		return nil, err
	}
	var req struct {
		Name                string   `json:"name"`
		Bundles             int      `json:"bundles"`
		ReplicationClusters []string `json:"replication_clusters"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name, err := simpleName("namespace", req.Name)
	if err != nil {
		return nil, err
	}
	policies := row{}
	if req.Bundles > 0 {
		policies["bundles"] = row{"numBundles": req.Bundles}
	}
	if len(req.ReplicationClusters) > 0 {
		policies["replication_clusters"] = req.ReplicationClusters
	}
	if err := s.send(rc.Ctx, http.MethodPut, namespacePath(tenant, name), nil, policies, nil); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func setNamespaceRetention(rc *plugin.RequestContext) (any, error) {
	s, tenant, namespace, err := writableNamespace(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		TimeMinutes int   `json:"time_minutes"`
		SizeMB      int64 `json:"size_mb"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	body := row{"retentionTimeInMinutes": req.TimeMinutes, "retentionSizeInMB": req.SizeMB}
	if err := s.send(rc.Ctx, http.MethodPost, namespacePath(tenant, namespace)+"/retention", nil, body, nil); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func unloadNamespace(rc *plugin.RequestContext) (any, error) {
	s, tenant, namespace, err := writableNamespace(rc)
	if err != nil {
		return nil, err
	}
	if err := s.send(rc.Ctx, http.MethodPut, namespacePath(tenant, namespace)+"/unload", nil, nil, nil); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func deleteNamespace(rc *plugin.RequestContext) (any, error) {
	s, tenant, namespace, err := writableNamespace(rc)
	if err != nil {
		return nil, err
	}
	if err := s.send(rc.Ctx, http.MethodDelete, namespacePath(tenant, namespace), nil, nil, nil); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func listClusters(rc *plugin.RequestContext) (any, error) {
	s, err := pulsarSession(rc)
	if err != nil {
		return nil, err
	}
	var names []string
	if err := s.get(rc.Ctx, "/admin/v2/clusters", nil, &names); err != nil {
		return nil, err
	}
	sort.Strings(names)
	rows := make([]row, 0, len(names))
	for _, name := range names {
		rows = append(rows, row{"name": name, "value": name, "label": name, "ref": clusterRef(name)})
	}
	page, err := broker.PageRows(rc, rows)
	if err != nil {
		return nil, err
	}
	enrichRows(rc.Ctx, page.Items, func(ctx context.Context, r row) {
		var data row
		if err := s.get(ctx, "/admin/v2/clusters/"+url.PathEscape(fmt.Sprint(r["name"])), nil, &data); err != nil {
			return
		}
		r["service_url"], r["broker_service_url"] = data["serviceUrl"], data["brokerServiceUrl"]
	})
	return page, nil
}

func readCluster(rc *plugin.RequestContext) (any, error) {
	s, err := pulsarSession(rc)
	if err != nil {
		return nil, err
	}
	cluster, err := simpleName("cluster", param(rc, "cluster"))
	if err != nil {
		return nil, err
	}
	var out row
	if err := s.get(rc.Ctx, "/admin/v2/clusters/"+url.PathEscape(cluster), nil, &out); err != nil {
		return nil, err
	}
	if out == nil {
		out = row{}
	}
	out["name"] = cluster
	return out, nil
}

// localCluster resolves the cluster whose brokers a request targets: the
// explicit param when the caller has one, otherwise the only cluster the broker
// reports. Both broker endpoints are cluster-scoped, so the listing cannot be
// answered without it.
func (s *Session) localCluster(ctx context.Context, requested string) (string, error) {
	if requested != "" {
		return simpleName("cluster", requested)
	}
	var names []string
	if err := s.get(ctx, "/admin/v2/clusters", nil, &names); err != nil {
		return "", err
	}
	if len(names) == 0 {
		return "", fmt.Errorf("%w: the broker reports no clusters", plugin.ErrNotFound)
	}
	sort.Strings(names)
	return names[0], nil
}

func listBrokers(rc *plugin.RequestContext) (any, error) {
	s, err := pulsarSession(rc)
	if err != nil {
		return nil, err
	}
	cluster, err := s.localCluster(rc.Ctx, param(rc, "cluster"))
	if err != nil {
		return nil, err
	}
	var brokers []string
	if err := s.get(rc.Ctx, "/admin/v2/brokers/"+url.PathEscape(cluster), nil, &brokers); err != nil {
		return nil, err
	}
	leader := ""
	var info struct {
		BrokerID   string `json:"brokerId"`
		ServiceURL string `json:"serviceUrl"`
	}
	if err := s.get(rc.Ctx, "/admin/v2/brokers/leaderBroker", nil, &info); err == nil {
		leader = firstNonEmpty(info.BrokerID, info.ServiceURL)
	}
	sort.Strings(brokers)
	rows := make([]row, 0, len(brokers))
	for _, id := range brokers {
		rows = append(rows, row{
			"broker":  id,
			"cluster": cluster,
			"leader":  leader != "" && strings.Contains(leader, id),
			"ref":     brokerRef(cluster, id),
		})
	}
	return broker.PageRows(rc, rows)
}

func readBroker(rc *plugin.RequestContext) (any, error) {
	s, err := pulsarSession(rc)
	if err != nil {
		return nil, err
	}
	cluster, err := s.localCluster(rc.Ctx, param(rc, "cluster"))
	if err != nil {
		return nil, err
	}
	id := strings.TrimSpace(param(rc, "broker"))
	if id == "" {
		return nil, fmt.Errorf("%w: broker is required", plugin.ErrInvalidInput)
	}
	out := row{"broker": id, "cluster": cluster}
	var owned map[string]row
	if err := s.get(rc.Ctx, "/admin/v2/brokers/"+url.PathEscape(cluster)+"/"+url.PathEscape(id)+"/ownedNamespaces", nil, &owned); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(owned))
	for name := range owned {
		names = append(names, name)
	}
	sort.Strings(names)
	out["owned_namespaces"], out["namespaces"] = len(names), names
	return out, nil
}

func writableSession(rc *plugin.RequestContext) (*Session, error) {
	s, err := pulsarSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	return s, nil
}

// writableNamespace resolves a mutation target from its route params alone. A
// listing may fall back to the connection's default scope, but a write must
// never: a delete with a missing param would otherwise hit the default
// namespace instead of the one the operator clicked.
func writableNamespace(rc *plugin.RequestContext) (*Session, string, string, error) {
	s, err := writableSession(rc)
	if err != nil {
		return nil, "", "", err
	}
	tenant, err := simpleName("tenant", param(rc, "tenant"))
	if err != nil {
		return nil, "", "", err
	}
	namespace, err := simpleName("namespace", param(rc, "namespace"))
	if err != nil {
		return nil, "", "", err
	}
	return s, tenant, namespace, nil
}

// simpleName rejects the separators that would let a caller escape its path
// segment and address a different tenant, namespace, or topic.
func simpleName(kind, value string) (string, error) {
	name := strings.TrimSpace(value)
	if name == "" {
		return "", fmt.Errorf("%w: %s is required", plugin.ErrInvalidInput, kind)
	}
	if strings.ContainsAny(name, "/\\?#") {
		return "", fmt.Errorf("%w: %s %q contains an invalid character", plugin.ErrInvalidInput, kind, name)
	}
	return name, nil
}

func nonNilStrings(in []string) []string {
	if in == nil {
		return []string{}
	}
	return in
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func tenantRef(name string) plugin.ResourceIdentity {
	return plugin.ResourceIdentity{Kind: "tenant", Name: name, UID: name}
}

func namespaceRef(tenant, namespace string) plugin.ResourceIdentity {
	return plugin.ResourceIdentity{Kind: "namespace", Namespace: tenant, Name: namespace, UID: tenant + "/" + namespace}
}

func clusterRef(name string) plugin.ResourceIdentity {
	return plugin.ResourceIdentity{Kind: "cluster", Name: name, UID: name}
}

func brokerRef(cluster, id string) plugin.ResourceIdentity {
	return plugin.ResourceIdentity{Kind: "broker", Namespace: cluster, Name: id, UID: cluster + "/" + id}
}
