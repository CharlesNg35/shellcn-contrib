// Package pulsar implements the Apache Pulsar protocol plugin.
package pulsar

import (
	"context"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type Plugin struct{}

func New() *Plugin { return &Plugin{} }

const pulsarIconSVG = `<svg fill="#188fff" viewBox="0 0 24 24" role="img" xmlns="http://www.w3.org/2000/svg"><g id="SVGRepo_bgCarrier" stroke-width="0"></g><g id="SVGRepo_tracerCarrier" stroke-linecap="round" stroke-linejoin="round"></g><g id="SVGRepo_iconCarrier"><path d="M24 8.925h-5.866c-1.586-3.041-3.262-5.402-5.544-5.402-2.97 0-4.367 2.593-5.717 5.115l-.118.22H0v1.5h3.934c1.39 0 1.673.468 1.673.468-1.09 1.691-2.4 3.363-4.584 3.363H0v1.574h1.03c4.234 0 6.083-3.434 7.567-6.193 1.361-2.541 2.31-4.08 3.993-4.08 1.747 0 3.584 3.801 5.201 7.157.237.488.477.988.72 1.483-6.2.197-9.155 1.649-11.559 2.833-1.759.866-3.147 1.94-5.433 1.94H0v1.574h1.507c2.754 0 4.47-.85 6.295-1.751 2.53-1.243 5.398-2.652 12.157-2.652h3.907V14.5H21.66a1.18 1.18 0 0 1-.972-.393 70.83 70.83 0 0 1-1.133-2.321l-.511-1.047s.366-.393 1.38-.393H24z"></path></g></svg>`

var domainSeverities = map[string]plugin.Severity{
	persistentDomain:    plugin.SeveritySuccess,
	nonPersistentDomain: plugin.SeverityWarn,
}

var subscriptionSeverities = map[string]plugin.Severity{
	"exclusive":  plugin.SeverityInfo,
	"failover":   plugin.SeverityWarn,
	"shared":     plugin.SeveritySuccess,
	"key_shared": plugin.SeveritySecondary,
}

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:  plugin.CurrentAPIVersion,
		Name:        protocolName,
		Version:     "0.1.0",
		Title:       "Apache Pulsar",
		Description: "Pulsar browser for tenants, namespaces, topics, subscriptions, producers, consumers, schemas, message browsing, live tailing, and publishing.",
		Icon:        plugin.Icon{Type: plugin.IconSVG, Value: pulsarIconSVG},
		Category:    plugin.CategoryMessaging,
		Config:      configSchema(),
		Capabilities: []plugin.Capability{
			"tenants", "namespaces", "topics", "partitions", "subscriptions",
			"producers", "consumers", "schemas", "messages", "produce", "tail",
		},
		// pulsar-client-go dials the broker port itself, so the binary-protocol
		// half of this plugin (produce and tail) cannot ride an agent tunnel.
		SupportedTransports: []plugin.Transport{plugin.TransportDirect},
		Layout:              plugin.LayoutSidebarTree,
		Tree:                tree(),
		Resources:           resources(),
		Actions:             actions(),
		Streams: []plugin.Stream{
			{ID: "pulsar.messages.consume", Kind: plugin.StreamLogs, RouteID: "pulsar.messages.consume"},
		},
	}
}

func (p *Plugin) Routes() []plugin.Route { return routes() }

func (p *Plugin) Connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	return connect(ctx, cfg)
}

func icon(name string) plugin.Icon {
	return plugin.Icon{Type: plugin.IconLucide, Value: name}
}

var instanceRef = plugin.ResourceIdentity{Kind: "instance", Name: "Cluster", UID: "instance"}

func tree() []plugin.TreeGroup {
	return []plugin.TreeGroup{
		{Key: "overview", Label: "Overview", Icon: icon("layout-dashboard"), Ref: &instanceRef},
		{Key: "tenants", Label: "Tenants", Icon: icon("building-2"), Source: plugin.DataSource{RouteID: "pulsar.tenants.tree"}, ResourceKind: "tenant"},
		{Key: "clusters", Label: "Clusters", Icon: icon("globe"), ResourceKind: "cluster"},
		{Key: "brokers", Label: "Brokers", Icon: icon("server"), ResourceKind: "broker"},
	}
}

func topicParams() map[string]string {
	return map[string]string{"topic": "${resource.uid}"}
}

func namespaceParams() map[string]string {
	return map[string]string{"tenant": "${resource.namespace}", "namespace": "${resource.name}"}
}

func subscriptionParams() map[string]string {
	return map[string]string{"topic": "${resource.namespace}", "subscription": "${resource.name}"}
}

// browsableTopic gates the panels that read a stored message: the broker only
// serves peek and examine for a non-partitioned persistent topic.
func browsableTopic() *plugin.Condition {
	return &plugin.Condition{AllOf: []plugin.Rule{
		{Field: "partitioned", Op: plugin.OpNeq, Value: true},
		{Field: "domain", Op: plugin.OpEq, Value: persistentDomain},
	}}
}

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		{
			Kind: "instance", Title: "Cluster", List: plugin.DataSource{RouteID: "pulsar.overview"},
			Columns: []plugin.Column{{Key: "admin_url", Label: "Admin API URL"}},
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "Pulsar"}, Tabs: []plugin.Panel{
				{Key: "instance-overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: "pulsar.overview"},
					Config: plugin.ObjectDetailConfig{RawToggle: true, Sections: []plugin.ObjectDetailSection{
						{Title: "Connection", Fields: []plugin.ObjectDetailField{
							{Key: "admin_url", Label: "Admin API URL", Copy: true},
							{Key: "service_url", Label: "Broker service URL", Copy: true},
							{Key: "version", Label: "Broker version"},
							{Key: "readOnly", Label: "Read-only", Type: plugin.ColumnBool},
						}},
						{Title: "Topology", Fields: []plugin.ObjectDetailField{
							{Key: "clusters", Label: "Clusters", Type: plugin.ColumnJSON},
							{Key: "brokers", Label: "Active brokers", Type: plugin.ColumnNumber},
							{Key: "tenant", Label: "Default tenant"},
							{Key: "namespace", Label: "Default namespace"},
						}},
					}}},
			}},
		},
		{
			Kind: "tenant", Title: "Tenants", List: plugin.DataSource{RouteID: "pulsar.tenants.list"},
			Columns: tenantColumns(),
			Actions: plugin.ResourceActions{
				Toolbar: []string{"pulsar.tenant.create"},
				Row:     []string{"pulsar.tenant.delete"},
				Detail:  []string{"pulsar.tenant.update", "pulsar.tenant.delete"},
			},
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "tenant-overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: "pulsar.tenant.read", Params: map[string]string{"tenant": "${resource.name}"}},
					Config: plugin.ObjectDetailConfig{RawToggle: true, Sections: []plugin.ObjectDetailSection{{Title: "Tenant", Fields: []plugin.ObjectDetailField{
						{Key: "name", Label: "Name", Copy: true},
						{Key: "namespaces", Label: "Namespaces", Type: plugin.ColumnNumber},
						{Key: "admin_roles", Label: "Admin roles", Type: plugin.ColumnJSON},
						{Key: "allowed_clusters", Label: "Allowed clusters", Type: plugin.ColumnJSON},
					}}}}},
				{Key: "tenant-namespaces", Label: "Namespaces", Icon: icon("folder"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: "pulsar.namespaces.list", Params: map[string]string{"tenant": "${resource.name}"}},
					Config: plugin.TableConfig{Columns: namespaceColumns(), ActionIDs: []string{"pulsar.namespace.create"},
						RowActionIDs: []string{"pulsar.namespace.delete"}, DefaultSort: &plugin.SortKey{Field: "name"},
						EmptyText: "This tenant has no namespaces yet.", RefreshIntervalMs: 30000, Exportable: true}},
			}},
		},
		{
			Kind: "namespace", Title: "Namespaces", List: plugin.DataSource{RouteID: "pulsar.namespaces.list"},
			Columns: namespaceColumns(),
			Actions: plugin.ResourceActions{
				Row:    []string{"pulsar.namespace.delete"},
				Detail: []string{"pulsar.namespace.retention", "pulsar.namespace.unload", "pulsar.namespace.delete"},
			},
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.namespace}/${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "namespace-topics", Label: "Topics", Icon: icon("radio-tower"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: "pulsar.topics.list", Params: namespaceParams()},
					Config: plugin.TableConfig{Columns: topicColumns(), ActionIDs: []string{"pulsar.topic.create"},
						RowActionIDs: []string{"pulsar.topic.delete"}, DefaultSort: &plugin.SortKey{Field: "name"},
						EmptyText: "This namespace has no topics yet.", RefreshIntervalMs: 15000, Exportable: true}},
				{Key: "namespace-policies", Label: "Policies", Icon: icon("shield"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: "pulsar.namespace.policies", Params: namespaceParams()},
					Config: plugin.ObjectDetailConfig{RawToggle: true, Sections: namespaceSections()}},
			}},
		},
		{
			Kind: "topic", Title: "Topics", List: plugin.DataSource{RouteID: "pulsar.topics.list"},
			Columns: topicColumns(),
			Actions: plugin.ResourceActions{
				Row: []string{"pulsar.topic.delete"},
				Detail: []string{
					"pulsar.message.produce", "pulsar.subscription.create", "pulsar.topic.compact",
					"pulsar.topic.unload", "pulsar.topic.terminate", "pulsar.topic.schema.delete",
					"pulsar.topic.delete",
				},
			},
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "topic-stats", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: "pulsar.topic.stats", Params: topicParams()},
					Config: plugin.ObjectDetailConfig{RawToggle: true, Sections: topicSections()}},
				{Key: "topic-subscriptions", Label: "Subscriptions", Icon: icon("users"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: "pulsar.subscriptions.list", Params: topicParams()},
					Config: plugin.TableConfig{Columns: subscriptionColumns(), ActionIDs: []string{"pulsar.subscription.create"},
						RowActionIDs: []string{"pulsar.subscription.clear", "pulsar.subscription.delete"},
						DefaultSort:  &plugin.SortKey{Field: "name"}, EmptyText: "No subscriptions on this topic.",
						RefreshIntervalMs: 10000, Exportable: true}},
				{Key: "topic-producers", Label: "Producers", Icon: icon("send"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: "pulsar.producers.list", Params: topicParams()},
					Config: plugin.TableConfig{Columns: producerColumns(), ActionIDs: []string{"pulsar.message.produce"},
						EmptyText: "No producer is connected to this topic.", RefreshIntervalMs: 10000, Exportable: true}},
				{Key: "topic-consumers", Label: "Consumers", Icon: icon("download"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: "pulsar.consumers.list", Params: topicParams()},
					Config: plugin.TableConfig{Columns: consumerColumns(), EmptyText: "No consumer is connected to this topic.",
						RefreshIntervalMs: 10000, Exportable: true}},
				{Key: "topic-partitions", Label: "Partitions", Icon: icon("columns-3"), Type: plugin.PanelTable,
					Source:      &plugin.DataSource{RouteID: "pulsar.topic.partitions", Params: topicParams()},
					VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{{Field: "partitioned", Op: plugin.OpEq, Value: true}}},
					Config: plugin.TableConfig{Columns: partitionColumns(), DefaultSort: &plugin.SortKey{Field: "partition"},
						EmptyText: "This topic is not partitioned.", RefreshIntervalMs: 15000, Exportable: true}},
				{Key: "topic-messages", Label: "Messages", Icon: icon("mail"), Type: plugin.PanelTable,
					Source:      &plugin.DataSource{RouteID: "pulsar.messages.list", Params: topicParams()},
					VisibleWhen: browsableTopic(),
					Config: plugin.TableConfig{Columns: messageColumns(), ActionIDs: []string{"pulsar.message.produce"},
						EmptyText: "No messages are retained on this topic.", Exportable: true, RowClick: plugin.RowClickDetail}},
				{Key: "topic-tail", Label: "Live tail", Icon: icon("activity"), Type: plugin.PanelLogStream,
					Source: &plugin.DataSource{RouteID: "pulsar.messages.consume", Method: plugin.MethodWS, Params: topicParams()},
					Config: plugin.LogStreamConfig{Controls: []plugin.StreamControl{{
						Param: "position", Label: "Start from",
						OptionsSource: &plugin.DataSource{RouteID: "pulsar.messages.positions"},
					}}}},
				{Key: "topic-schema", Label: "Schema", Icon: icon("file-code"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: "pulsar.topic.schema", Params: topicParams()},
					Config: plugin.ObjectDetailConfig{RawToggle: true, Sections: []plugin.ObjectDetailSection{{Title: "Schema", Fields: []plugin.ObjectDetailField{
						{Key: "type", Label: "Type", Type: plugin.ColumnBadge},
						{Key: "version", Label: "Version", Type: plugin.ColumnNumber},
						{Key: "registered", Label: "Registered", Type: plugin.ColumnBool},
						{Key: "timestamp", Label: "Registered at", Type: plugin.ColumnDateTime},
						{Key: "properties", Label: "Properties", Type: plugin.ColumnJSON},
						{Key: "data", Label: "Definition"},
					}}}}},
				{Key: "topic-internal", Label: "Internal", Icon: icon("hard-drive"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: "pulsar.topic.internal_stats", Params: topicParams()},
					Config: plugin.ObjectDetailConfig{RawToggle: true, Sections: []plugin.ObjectDetailSection{{Title: "Managed ledger", Fields: []plugin.ObjectDetailField{
						{Key: "entriesAddedCounter", Label: "Entries added", Type: plugin.ColumnNumber},
						{Key: "numberOfEntries", Label: "Entries", Type: plugin.ColumnNumber},
						{Key: "totalSize", Label: "Total size", Type: plugin.ColumnBytes},
						{Key: "currentLedgerEntries", Label: "Current ledger entries", Type: plugin.ColumnNumber},
						{Key: "currentLedgerSize", Label: "Current ledger size", Type: plugin.ColumnBytes},
						{Key: "lastConfirmedEntry", Label: "Last confirmed entry", Copy: true},
						{Key: "state", Label: "State", Type: plugin.ColumnBadge},
						{Key: "ledgers", Label: "Ledgers", Type: plugin.ColumnJSON},
						{Key: "cursors", Label: "Cursors", Type: plugin.ColumnJSON},
					}}}}},
			}},
		},
		{
			Kind: "subscription", Title: "Subscriptions", List: plugin.DataSource{RouteID: "pulsar.subscriptions.list"},
			Columns: subscriptionColumns(),
			Actions: plugin.ResourceActions{
				Row: []string{"pulsar.subscription.clear", "pulsar.subscription.delete"},
				Detail: []string{
					"pulsar.subscription.skip", "pulsar.subscription.clear", "pulsar.subscription.reset",
					"pulsar.subscription.expire", "pulsar.subscription.delete",
				},
			},
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "subscription-overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: "pulsar.subscription.read", Params: subscriptionParams()},
					Config: plugin.ObjectDetailConfig{RawToggle: true, Sections: subscriptionSections()}},
				{Key: "subscription-consumers", Label: "Consumers", Icon: icon("users"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: "pulsar.consumers.list", Params: subscriptionParams()},
					Config: plugin.TableConfig{Columns: consumerColumns(), EmptyText: "No consumer is attached to this subscription.",
						RefreshIntervalMs: 10000, Exportable: true}},
				{Key: "subscription-messages", Label: "Backlog", Icon: icon("mail"), Type: plugin.PanelTable,
					Source:      &plugin.DataSource{RouteID: "pulsar.messages.list", Params: subscriptionParams()},
					VisibleWhen: browsableTopic(),
					Config: plugin.TableConfig{Columns: messageColumns(), EmptyText: "This subscription has no readable backlog.",
						Exportable: true, RowClick: plugin.RowClickDetail}},
			}},
		},
		{
			Kind: "cluster", Title: "Clusters", List: plugin.DataSource{RouteID: "pulsar.clusters.list"},
			Columns: clusterColumns(),
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "cluster-overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: "pulsar.cluster.read", Params: map[string]string{"cluster": "${resource.name}"}},
					Config: plugin.ObjectDetailConfig{RawToggle: true, Sections: []plugin.ObjectDetailSection{{Title: "Cluster", Fields: []plugin.ObjectDetailField{
						{Key: "name", Label: "Name", Copy: true},
						{Key: "serviceUrl", Label: "Web service URL", Copy: true},
						{Key: "serviceUrlTls", Label: "Web service URL (TLS)", Copy: true},
						{Key: "brokerServiceUrl", Label: "Broker service URL", Copy: true},
						{Key: "brokerServiceUrlTls", Label: "Broker service URL (TLS)", Copy: true},
						{Key: "peerClusterNames", Label: "Peer clusters", Type: plugin.ColumnJSON},
					}}}}},
			}},
		},
		{
			Kind: "broker", Title: "Brokers", List: plugin.DataSource{RouteID: "pulsar.brokers.list"},
			Columns: brokerColumns(),
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "broker-overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: "pulsar.broker.read", Params: map[string]string{"cluster": "${resource.namespace}", "broker": "${resource.name}"}},
					Config: plugin.ObjectDetailConfig{RawToggle: true, Sections: []plugin.ObjectDetailSection{{Title: "Broker", Fields: []plugin.ObjectDetailField{
						{Key: "broker", Label: "Broker", Copy: true},
						{Key: "cluster", Label: "Cluster", Copy: true},
						{Key: "owned_namespaces", Label: "Owned namespaces", Type: plugin.ColumnNumber},
						{Key: "namespaces", Label: "Namespace bundles", Type: plugin.ColumnJSON},
					}}}}},
			}},
		},
	}
}

func actions() []plugin.Action {
	return []plugin.Action{
		{ID: "pulsar.tenant.create", Label: "Create tenant", Icon: icon("plus"), RouteID: "pulsar.tenant.create"},
		{ID: "pulsar.tenant.update", Label: "Edit", Icon: icon("pencil"), RouteID: "pulsar.tenant.update", Params: map[string]string{"tenant": "${resource.name}"}},
		{ID: "pulsar.tenant.delete", Label: "Delete", Icon: icon("trash-2"), RouteID: "pulsar.tenant.delete", Params: map[string]string{"tenant": "${resource.name}"},
			Confirm: true, ConfirmText: "Delete this tenant? It must have no namespaces left.", Bulk: true, OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList}},

		{ID: "pulsar.namespace.create", Label: "Create namespace", Icon: icon("plus"), RouteID: "pulsar.namespace.create", Params: map[string]string{"tenant": "${resource.name}"}},
		{ID: "pulsar.namespace.retention", Label: "Set retention", Icon: icon("archive"), RouteID: "pulsar.namespace.retention", Params: namespaceParams()},
		{ID: "pulsar.namespace.unload", Label: "Unload", Icon: icon("power"), RouteID: "pulsar.namespace.unload", Params: namespaceParams(),
			Confirm: true, ConfirmText: "Unload this namespace? Every producer and consumer on it is disconnected while the bundles move."},
		{ID: "pulsar.namespace.delete", Label: "Delete", Icon: icon("trash-2"), RouteID: "pulsar.namespace.delete", Params: namespaceParams(),
			Confirm: true, ConfirmText: "Delete this namespace and every topic in it?", Bulk: true, OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList}},

		{ID: "pulsar.topic.create", Label: "Create topic", Icon: icon("plus"), RouteID: "pulsar.topic.create", Params: namespaceParams()},
		{ID: "pulsar.topic.compact", Label: "Compact", Icon: icon("layers"), RouteID: "pulsar.topic.compact", Params: topicParams()},
		{ID: "pulsar.topic.unload", Label: "Unload", Icon: icon("power"), RouteID: "pulsar.topic.unload", Params: topicParams(),
			Confirm: true, ConfirmText: "Unload this topic? Connected producers and consumers reconnect through another broker."},
		{ID: "pulsar.topic.terminate", Label: "Terminate", Icon: icon("octagon-x"), RouteID: "pulsar.topic.terminate", Params: topicParams(),
			Confirm: true, ConfirmText: "Terminate this topic? No message can ever be published to it again."},
		{ID: "pulsar.topic.delete", Label: "Delete", Icon: icon("trash-2"), RouteID: "pulsar.topic.delete", Params: topicParams(),
			Confirm: true, ConfirmText: "Delete this topic and every message stored in it?", Bulk: true, OnSuccess: &plugin.ActionSuccess{Navigate: plugin.NavigateList}},
		{ID: "pulsar.topic.schema.delete", Label: "Delete schema", Icon: icon("file-x"), RouteID: "pulsar.topic.schema.delete", Params: topicParams(),
			Confirm: true, ConfirmText: "Delete every registered schema version for this topic?"},

		{ID: "pulsar.message.produce", Label: "Produce", Icon: icon("send"), RouteID: "pulsar.message.produce", Params: topicParams(),
			Confirm: true, ConfirmText: "Publish this message to the topic?"},

		{ID: "pulsar.subscription.create", Label: "Create subscription", Icon: icon("user-plus"), RouteID: "pulsar.subscription.create", Params: topicParams()},
		{ID: "pulsar.subscription.skip", Label: "Skip messages", Icon: icon("skip-forward"), RouteID: "pulsar.subscription.skip", Params: subscriptionParams(),
			Confirm: true, ConfirmText: "Skip messages on this subscription? They are acknowledged without being delivered."},
		{ID: "pulsar.subscription.clear", Label: "Clear backlog", Icon: icon("eraser"), RouteID: "pulsar.subscription.clear", Params: subscriptionParams(),
			Confirm: true, ConfirmText: "Clear the whole backlog? Every pending message is acknowledged and never delivered.", Bulk: true},
		{ID: "pulsar.subscription.reset", Label: "Reset cursor", Icon: icon("history"), RouteID: "pulsar.subscription.reset", Params: subscriptionParams(),
			Confirm: true, ConfirmText: "Reset this cursor? Connected consumers are disconnected and the delivery position moves."},
		{ID: "pulsar.subscription.expire", Label: "Expire messages", Icon: icon("clock"), RouteID: "pulsar.subscription.expire", Params: subscriptionParams(),
			Confirm: true, ConfirmText: "Expire the older part of this backlog? Those messages are acknowledged and never delivered."},
		{ID: "pulsar.subscription.delete", Label: "Delete", Icon: icon("trash-2"), RouteID: "pulsar.subscription.delete", Params: subscriptionParams(),
			Confirm: true, ConfirmText: "Delete this subscription and its cursor?", Bulk: true},
	}
}

func tenantColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Tenant", Sortable: true},
		{Key: "admin_roles", Label: "Admin roles", Type: plugin.ColumnJSON},
		{Key: "allowed_clusters", Label: "Allowed clusters", Type: plugin.ColumnJSON},
	}
}

func namespaceColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Namespace", Sortable: true},
		{Key: "tenant", Label: "Tenant", Sortable: true},
		{Key: "bundles", Label: "Bundles", Type: plugin.ColumnNumber},
		{Key: "message_ttl", Label: "Message TTL", Type: plugin.ColumnDuration},
		{Key: "retention_minutes", Label: "Retention (min)", Type: plugin.ColumnNumber},
		{Key: "retention_size", Label: "Retention size", Type: plugin.ColumnNumber},
		{Key: "replication_clusters", Label: "Replication", Type: plugin.ColumnJSON},
	}
}

func topicColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Topic", Sortable: true},
		{Key: "domain", Label: "Domain", Type: plugin.ColumnBadge, Sortable: true, Severities: domainSeverities},
		{Key: "partitions", Label: "Partitions", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "producers", Label: "Producers", Type: plugin.ColumnNumber},
		{Key: "subscriptions", Label: "Subscriptions", Type: plugin.ColumnNumber},
		{Key: "msg_rate_in", Label: "Rate in", Type: plugin.ColumnNumber},
		{Key: "msg_rate_out", Label: "Rate out", Type: plugin.ColumnNumber},
		{Key: "storage_size", Label: "Storage", Type: plugin.ColumnBytes},
		{Key: "backlog_size", Label: "Backlog", Type: plugin.ColumnBytes},
	}
}

func partitionColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "partition", Label: "Partition", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "producers", Label: "Producers", Type: plugin.ColumnNumber},
		{Key: "subscriptions", Label: "Subscriptions", Type: plugin.ColumnNumber},
		{Key: "msg_rate_in", Label: "Rate in", Type: plugin.ColumnNumber},
		{Key: "msg_rate_out", Label: "Rate out", Type: plugin.ColumnNumber},
		{Key: "storage_size", Label: "Storage", Type: plugin.ColumnBytes},
		{Key: "backlog_size", Label: "Backlog", Type: plugin.ColumnBytes},
	}
}

func subscriptionColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Subscription", Sortable: true},
		{Key: "type", Label: "Type", Type: plugin.ColumnBadge, Sortable: true, Severities: subscriptionSeverities},
		{Key: "msg_backlog", Label: "Backlog", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "unacked_messages", Label: "Unacked", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "consumers", Label: "Consumers", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "msg_rate_out", Label: "Rate out", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "durable", Label: "Durable", Type: plugin.ColumnBool},
		{Key: "last_consumed", Label: "Last consumed", Type: plugin.ColumnRelativeTime, Sortable: true},
	}
}

func producerColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "producer_name", Label: "Producer", Sortable: true},
		{Key: "topic", Label: "Topic", Sortable: true},
		{Key: "address", Label: "Address"},
		{Key: "client_version", Label: "Client"},
		{Key: "access_mode", Label: "Access mode", Type: plugin.ColumnBadge},
		{Key: "msg_rate_in", Label: "Rate in", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "average_msg_size", Label: "Avg size", Type: plugin.ColumnBytes},
		{Key: "connected_since", Label: "Connected", Type: plugin.ColumnDateTime, Sortable: true},
	}
}

func consumerColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "consumer_name", Label: "Consumer", Sortable: true},
		{Key: "subscription", Label: "Subscription", Sortable: true},
		{Key: "address", Label: "Address"},
		{Key: "client_version", Label: "Client"},
		{Key: "msg_rate_out", Label: "Rate out", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "unacked_messages", Label: "Unacked", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "available_permits", Label: "Permits", Type: plugin.ColumnNumber},
		{Key: "connected_since", Label: "Connected", Type: plugin.ColumnDateTime, Sortable: true},
	}
}

func messageColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "position", Label: "#", Type: plugin.ColumnNumber},
		{Key: "message_id", Label: "Message ID"},
		{Key: "publish_time", Label: "Published", Type: plugin.ColumnRelativeTime},
		{Key: "producer", Label: "Producer"},
		{Key: "key", Label: "Key"},
		{Key: "size", Label: "Size", Type: plugin.ColumnBytes},
		{Key: "properties", Label: "Properties", Type: plugin.ColumnJSON},
		{Key: "payload", Label: "Payload"},
	}
}

func clusterColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Cluster", Sortable: true},
		{Key: "service_url", Label: "Web service URL"},
		{Key: "broker_service_url", Label: "Broker service URL"},
	}
}

func brokerColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "broker", Label: "Broker", Sortable: true},
		{Key: "cluster", Label: "Cluster", Sortable: true},
		{Key: "leader", Label: "Leader", Type: plugin.ColumnBool},
	}
}

func topicSections() []plugin.ObjectDetailSection {
	return []plugin.ObjectDetailSection{
		{Title: "Topic", Fields: []plugin.ObjectDetailField{
			{Key: "topic", Label: "Topic", Copy: true},
			{Key: "domain", Label: "Domain", Type: plugin.ColumnBadge, Severities: domainSeverities},
			{Key: "partitioned", Label: "Partitioned", Type: plugin.ColumnBool},
			{Key: "partitions", Label: "Partitions", Type: plugin.ColumnNumber},
			{Key: "ownerBroker", Label: "Owner broker", Copy: true},
		}},
		{Title: "Throughput", Fields: []plugin.ObjectDetailField{
			{Key: "msgRateIn", Label: "Publish rate", Type: plugin.ColumnNumber},
			{Key: "msgRateOut", Label: "Dispatch rate", Type: plugin.ColumnNumber},
			{Key: "msgThroughputIn", Label: "Publish throughput", Type: plugin.ColumnNumber},
			{Key: "msgThroughputOut", Label: "Dispatch throughput", Type: plugin.ColumnNumber},
			{Key: "averageMsgSize", Label: "Average message size", Type: plugin.ColumnBytes},
		}},
		{Title: "Storage", Fields: []plugin.ObjectDetailField{
			{Key: "storageSize", Label: "Storage size", Type: plugin.ColumnBytes},
			{Key: "backlogSize", Label: "Backlog size", Type: plugin.ColumnBytes},
			{Key: "msgInCounter", Label: "Messages in", Type: plugin.ColumnNumber},
			{Key: "msgOutCounter", Label: "Messages out", Type: plugin.ColumnNumber},
		}},
		{Title: "Clients", Fields: []plugin.ObjectDetailField{
			{Key: "producer_count", Label: "Producers", Type: plugin.ColumnNumber},
			{Key: "subscription_count", Label: "Subscriptions", Type: plugin.ColumnNumber},
		}},
	}
}

func namespaceSections() []plugin.ObjectDetailSection {
	return []plugin.ObjectDetailSection{
		{Title: "Namespace", Fields: []plugin.ObjectDetailField{
			{Key: "name", Label: "Name", Copy: true},
			{Key: "tenant", Label: "Tenant", Copy: true},
			{Key: "bundles", Label: "Bundles", Type: plugin.ColumnJSON},
			{Key: "replication_clusters", Label: "Replication clusters", Type: plugin.ColumnJSON},
		}},
		{Title: "Retention", Fields: []plugin.ObjectDetailField{
			{Key: "message_ttl_in_seconds", Label: "Message TTL", Type: plugin.ColumnDuration},
			{Key: "retention_policies", Label: "Retention", Type: plugin.ColumnJSON},
			{Key: "backlog_quota_map", Label: "Backlog quotas", Type: plugin.ColumnJSON},
			{Key: "compaction_threshold", Label: "Compaction threshold", Type: plugin.ColumnBytes},
		}},
		{Title: "Limits", Fields: []plugin.ObjectDetailField{
			{Key: "max_producers_per_topic", Label: "Max producers per topic", Type: plugin.ColumnNumber},
			{Key: "max_consumers_per_topic", Label: "Max consumers per topic", Type: plugin.ColumnNumber},
			{Key: "max_consumers_per_subscription", Label: "Max consumers per subscription", Type: plugin.ColumnNumber},
			{Key: "max_unacked_messages_per_consumer", Label: "Max unacked per consumer", Type: plugin.ColumnNumber},
		}},
		{Title: "Safety", Fields: []plugin.ObjectDetailField{
			{Key: "deduplicationEnabled", Label: "Deduplication", Type: plugin.ColumnBool},
			{Key: "encryption_required", Label: "Encryption required", Type: plugin.ColumnBool},
			{Key: "schema_validation_enforced", Label: "Schema validation enforced", Type: plugin.ColumnBool},
			{Key: "is_allow_auto_update_schema", Label: "Auto schema update", Type: plugin.ColumnBool},
		}},
	}
}

func subscriptionSections() []plugin.ObjectDetailSection {
	return []plugin.ObjectDetailSection{
		{Title: "Subscription", Fields: []plugin.ObjectDetailField{
			{Key: "name", Label: "Name", Copy: true},
			{Key: "topic", Label: "Topic", Copy: true},
			{Key: "type", Label: "Type", Type: plugin.ColumnBadge, Severities: subscriptionSeverities},
			{Key: "durable", Label: "Durable", Type: plugin.ColumnBool},
			{Key: "replicated", Label: "Replicated", Type: plugin.ColumnBool},
			{Key: "active_consumer", Label: "Active consumer"},
		}},
		{Title: "Backlog", Fields: []plugin.ObjectDetailField{
			{Key: "msg_backlog", Label: "Backlog", Type: plugin.ColumnNumber},
			{Key: "msg_backlog_no_delayed", Label: "Backlog without delayed", Type: plugin.ColumnNumber},
			{Key: "msg_delayed", Label: "Delayed", Type: plugin.ColumnNumber},
			{Key: "unacked_messages", Label: "Unacked", Type: plugin.ColumnNumber},
			{Key: "blocked", Label: "Blocked on unacked", Type: plugin.ColumnBool},
			{Key: "total_msg_expired", Label: "Expired", Type: plugin.ColumnNumber},
		}},
		{Title: "Activity", Fields: []plugin.ObjectDetailField{
			{Key: "consumers", Label: "Consumers", Type: plugin.ColumnNumber},
			{Key: "msg_rate_out", Label: "Dispatch rate", Type: plugin.ColumnNumber},
			{Key: "msg_rate_redeliver", Label: "Redelivery rate", Type: plugin.ColumnNumber},
			{Key: "last_consumed", Label: "Last consumed", Type: plugin.ColumnRelativeTime},
			{Key: "last_acked", Label: "Last acknowledged", Type: plugin.ColumnRelativeTime},
			{Key: "last_expire", Label: "Last expire", Type: plugin.ColumnRelativeTime},
		}},
	}
}
