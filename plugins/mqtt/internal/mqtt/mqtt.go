// Package mqtt implements the MQTT protocol plugin.
package mqtt

import (
	"context"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type Plugin struct{}

func New() *Plugin { return &Plugin{} }

const mqttIconSVG = `<svg xmlns="http://www.w3.org/2000/svg" xml:space="preserve" viewBox="0 0 512 512"><path d="M1.5 290.2H0V486c0 14.1 11.6 25.7 25.7 25.7h201.6C225.6 389.4 125.2 290.2 1.5 290.2m0-161.5H0V212c166.3.8 301.5 134.5 303.3 299.8h86.3C388.1 300.3 214.7 128.7 1.5 128.7M512 486.3V309.4C453.5 166 335.6 52.7 189 0H25.7C11.6 0 0 11.6 0 25.7v25c255.9.8 464.1 206.9 465.6 461.3h20.7c14.3-.3 25.7-11.6 25.7-25.7M444.6 71.4c23.7 23.7 47.9 53.7 67.4 80.2V25.5c0-14-11.3-25.4-25.3-25.5H356.5c30.3 20.9 61.6 44.9 88.1 71.4" style="fill:#606"/></svg>`

var qosSeverities = map[string]plugin.Severity{
	"0": plugin.SeveritySecondary,
	"1": plugin.SeverityInfo,
	"2": plugin.SeveritySuccess,
}

var connectedSeverities = map[string]plugin.Severity{
	"true":  plugin.SeveritySuccess,
	"false": plugin.SeverityDanger,
}

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:          plugin.CurrentAPIVersion,
		Name:                protocolName,
		Version:             "0.1.0",
		Title:               "MQTT",
		Description:         "MQTT broker client with live subscriptions, publishing, retained-message browsing, a topic tree, $SYS metrics, and EMQX management tables.",
		Icon:                plugin.Icon{Type: plugin.IconSVG, Value: mqttIconSVG},
		Category:            plugin.CategoryMessaging,
		Config:              configSchema(),
		Capabilities:        []plugin.Capability{"subscribe", "publish", "retained", "topics", "metrics", "emqx"},
		SupportedTransports: []plugin.Transport{plugin.TransportDirect},
		Layout:              plugin.LayoutSidebarTree,
		Tabs:                tabs(),
		Tree:                tree(),
		Resources:           resources(),
		Actions:             actions(),
		Streams:             streams(),
		HeaderActions:       []string{"mqtt.message.publish"},
	}
}

func (p *Plugin) Routes() []plugin.Route { return routes() }

func (p *Plugin) Connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	return connect(ctx, cfg)
}

func icon(name string) plugin.Icon {
	return plugin.Icon{Type: plugin.IconLucide, Value: name}
}

func streams() []plugin.Stream {
	return []plugin.Stream{
		{ID: "mqtt.subscribe", Kind: plugin.StreamLogs, RouteID: "mqtt.subscribe"},
	}
}

func livePanel() plugin.Panel {
	return plugin.Panel{
		Key: "live", Label: "Live", Icon: icon("radio"), Type: plugin.PanelLogStream,
		Source: &plugin.DataSource{RouteID: "mqtt.subscribe", Method: plugin.MethodWS},
		Config: plugin.LogStreamConfig{Controls: []plugin.StreamControl{
			{Param: "filter", Label: "Topic filter", OptionsSource: &plugin.DataSource{RouteID: "mqtt.filters.options"}},
			{Param: "qos", Label: "QoS", OptionsSource: &plugin.DataSource{RouteID: "mqtt.qos.options"}},
		}},
	}
}

// topicLivePanel deliberately declares no filter control: the selector's first
// option would replace the topic this tab is scoped to.
func topicLivePanel() plugin.Panel {
	return plugin.Panel{
		Key: "topic_live", Label: "Live", Icon: icon("radio"), Type: plugin.PanelLogStream,
		Source: &plugin.DataSource{RouteID: "mqtt.subscribe", Method: plugin.MethodWS, Params: topicParams()},
		Config: plugin.LogStreamConfig{Controls: []plugin.StreamControl{
			{Param: "qos", Label: "QoS", OptionsSource: &plugin.DataSource{RouteID: "mqtt.qos.options"}},
		}},
	}
}

func tabs() []plugin.Panel {
	return []plugin.Panel{
		livePanel(),
		{
			Key: "broker", Label: "Broker", Icon: icon("server"), Type: plugin.PanelObjectDetail,
			Source: &plugin.DataSource{RouteID: "mqtt.overview"},
			Config: plugin.ObjectDetailConfig{Sections: brokerSections(), RawToggle: true},
		},
		{
			Key: "retained", Label: "Retained", Icon: icon("pin"), Type: plugin.PanelTable,
			Source: &plugin.DataSource{RouteID: "mqtt.retained.list"},
			Config: plugin.TableConfig{
				Columns: topicColumns(), DefaultSort: &plugin.SortKey{Field: "topic"},
				ActionIDs: []string{"mqtt.message.publish", "mqtt.retained.rescan"}, RowActionIDs: []string{"mqtt.retained.clear"},
				RefreshIntervalMs: 10000, Exportable: true,
				EmptyText: "No retained messages observed. Retained messages arrive when the observation subscription is (re)established.",
			},
		},
		{
			Key: "messages", Label: "Messages", Icon: icon("mail"), Type: plugin.PanelTable,
			Source: &plugin.DataSource{RouteID: "mqtt.messages.list"},
			Config: plugin.TableConfig{
				Columns: messageColumns(), DefaultSort: &plugin.SortKey{Field: "seq", Desc: true},
				ActionIDs:         []string{"mqtt.message.publish", "mqtt.messages.clear"},
				RefreshIntervalMs: 5000, Exportable: true,
				EmptyText: "No messages buffered yet on the observed topic filter.",
			},
		},
		{
			Key: "sys", Label: "$SYS", Icon: icon("activity"), Type: plugin.PanelTable,
			Source: &plugin.DataSource{RouteID: "mqtt.sys.list"},
			Config: plugin.TableConfig{
				Columns: sysColumns(), DefaultSort: &plugin.SortKey{Field: "topic"},
				RefreshIntervalMs: 10000, Exportable: true,
				EmptyText: "No $SYS metrics published. The broker may not expose $SYS, or this client may not be allowed to read it.",
			},
		},
		{
			Key: "subscriptions", Label: "Subscriptions", Icon: icon("list-tree"), Type: plugin.PanelTable,
			Source: &plugin.DataSource{RouteID: "mqtt.emqx.subscriptions.list"},
			Config: plugin.TableConfig{
				Columns: subscriptionColumns(), RefreshIntervalMs: 15000, Exportable: true,
				EmptyText: "No subscriptions. This table needs the EMQX management API; set the dashboard URL on the connection.",
			},
		},
		{
			// No DefaultSort: EMQX cannot sort server-side, so a sorted default
			// would force every load of this tab through a bounded cluster walk.
			Key: "routes", Label: "Routed topics", Icon: icon("route"), Type: plugin.PanelTable,
			Source: &plugin.DataSource{RouteID: "mqtt.emqx.topics.list"},
			Config: plugin.TableConfig{
				Columns: emqxTopicColumns(), RefreshIntervalMs: 15000, Exportable: true,
				EmptyText: "No routed topics. This table needs the EMQX management API; set the dashboard URL on the connection.",
			},
		},
		{
			Key: "nodes", Label: "Nodes", Icon: icon("cpu"), Type: plugin.PanelTable,
			Source: &plugin.DataSource{RouteID: "mqtt.emqx.nodes.list"},
			Config: plugin.TableConfig{
				Columns: nodeColumns(), RefreshIntervalMs: 15000, Exportable: true,
				EmptyText: "No cluster nodes. This table needs the EMQX management API; set the dashboard URL on the connection.",
			},
		},
	}
}

func tree() []plugin.TreeGroup {
	return []plugin.TreeGroup{
		{Key: "topics", Label: "Topics", Icon: icon("list-tree"), Source: plugin.DataSource{RouteID: "mqtt.topics.tree"}, ResourceKind: "topic"},
		{Key: "clients", Label: "Clients", Icon: icon("users"), ResourceKind: "client"},
	}
}

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		{
			Kind: "topic", Title: "Topics", List: plugin.DataSource{RouteID: "mqtt.topics.list"},
			Columns: topicColumns(),
			Actions: plugin.ResourceActions{
				Toolbar: []string{"mqtt.message.publish", "mqtt.retained.rescan"},
				Row:     []string{"mqtt.retained.clear"},
				Detail:  []string{"mqtt.topic.publish", "mqtt.retained.clear"},
			},
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
				{
					Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: "mqtt.topic.overview", Params: topicParams()},
					Config: plugin.ObjectDetailConfig{Sections: topicSections(), RawToggle: true},
				},
				{
					Key: "topic_messages", Label: "Messages", Icon: icon("mail"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: "mqtt.messages.list", Params: topicParams()},
					Config: plugin.TableConfig{
						Columns: messageColumns(), DefaultSort: &plugin.SortKey{Field: "seq", Desc: true},
						ActionIDs: []string{"mqtt.topic.publish"}, RefreshIntervalMs: 5000, Exportable: true,
						EmptyText: "No buffered messages for this topic.",
					},
				},
				topicLivePanel(),
			}},
		},
		{
			Kind: "client", Title: "Clients", List: plugin.DataSource{RouteID: "mqtt.emqx.clients.list"},
			Columns: clientColumns(),
			Actions: plugin.ResourceActions{
				Row:    []string{"mqtt.client.kick"},
				Detail: []string{"mqtt.client.kick"},
			},
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}", StatusField: "connected", Severities: connectedSeverities}, Tabs: []plugin.Panel{
				{
					Key: "client_overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail,
					Source: &plugin.DataSource{RouteID: "mqtt.emqx.client.overview", Params: clientParams()},
					Config: plugin.ObjectDetailConfig{Sections: clientSections(), RawToggle: true},
				},
				{
					Key: "client_subscriptions", Label: "Subscriptions", Icon: icon("list-tree"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: "mqtt.emqx.subscriptions.list", Params: clientParams()},
					Config: plugin.TableConfig{
						Columns: subscriptionColumns(), RefreshIntervalMs: 15000, Exportable: true,
						EmptyText: "This client has no subscriptions.",
					},
				},
			}},
		},
	}
}

func actions() []plugin.Action {
	return []plugin.Action{
		{ID: "mqtt.message.publish", Label: "Publish", Icon: icon("send"), RouteID: "mqtt.publish"},
		{ID: "mqtt.topic.publish", Label: "Publish", Icon: icon("send"), RouteID: "mqtt.publish", Params: topicParams()},
		{ID: "mqtt.retained.rescan", Label: "Rescan retained", Icon: icon("refresh-cw"), RouteID: "mqtt.retained.rescan", Confirm: true, ConfirmText: "Drop the observation cache and re-subscribe so the broker replays its retained messages?"},
		{
			// Every row that carries this action is a topic resource, so the
			// resource token also resolves in the topic detail view.
			ID: "mqtt.retained.clear", Label: "Clear retained", Icon: icon("eraser"), RouteID: "mqtt.retained.clear",
			Params: map[string]string{"topic": "${resource.name}"}, Confirm: true, Bulk: true,
			ConfirmText: "Remove the retained message from the broker? Subscribers will no longer receive it on connect.",
		},
		{ID: "mqtt.messages.clear", Label: "Clear buffer", Icon: icon("trash-2"), RouteID: "mqtt.messages.clear", Confirm: true, ConfirmText: "Discard the buffered messages held for this connection?"},
		{
			ID: "mqtt.client.kick", Label: "Kick", Icon: icon("user-x"), RouteID: "mqtt.emqx.client.kick",
			Params: map[string]string{"clientid": "${resource.name}"}, Confirm: true, Bulk: true,
			ConfirmText: "Disconnect this client from the broker?",
		},
	}
}

func topicParams() map[string]string {
	return map[string]string{"topic": "${resource.name}"}
}

func clientParams() map[string]string {
	return map[string]string{"clientid": "${resource.name}"}
}

func topicColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "topic", Label: "Topic", Sortable: true},
		{Key: "payload", Label: "Payload"},
		{Key: "size", Label: "Size", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "qos", Label: "QoS", Type: plugin.ColumnBadge, Sortable: true, Severities: qosSeverities},
		{Key: "retained", Label: "Retained", Type: plugin.ColumnBool, Sortable: true},
		{Key: "messages", Label: "Messages", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "last_seen", Label: "Last seen", Type: plugin.ColumnRelativeTime, Sortable: true},
	}
}

func messageColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "seq", Label: "#", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "time", Label: "Time", Type: plugin.ColumnRelativeTime, Sortable: true},
		{Key: "topic", Label: "Topic", Sortable: true},
		{Key: "qos", Label: "QoS", Type: plugin.ColumnBadge, Sortable: true, Severities: qosSeverities},
		{Key: "retained", Label: "Retained", Type: plugin.ColumnBool, Sortable: true},
		{Key: "size", Label: "Size", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "payload", Label: "Payload"},
	}
}

func sysColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "topic", Label: "Metric", Sortable: true},
		{Key: "payload", Label: "Value"},
		{Key: "last_seen", Label: "Updated", Type: plugin.ColumnRelativeTime, Sortable: true},
	}
}

func clientColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "clientid", Label: "Client ID", Sortable: true},
		{Key: "username", Label: "Username", Sortable: true},
		{Key: "connected", Label: "Connected", Type: plugin.ColumnBool, Sortable: true},
		{Key: "ip_address", Label: "Address"},
		{Key: "proto_ver", Label: "Protocol", Type: plugin.ColumnNumber},
		{Key: "subscriptions_cnt", Label: "Subscriptions", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "connected_at", Label: "Connected at", Type: plugin.ColumnRelativeTime, Sortable: true},
	}
}

func subscriptionColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "clientid", Label: "Client ID", Sortable: true},
		{Key: "topic", Label: "Topic filter", Sortable: true},
		{Key: "qos", Label: "QoS", Type: plugin.ColumnBadge, Sortable: true, Severities: qosSeverities},
		{Key: "node", Label: "Node", Sortable: true},
	}
}

func emqxTopicColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "topic", Label: "Topic", Sortable: true},
		{Key: "node", Label: "Node", Sortable: true},
	}
}

func nodeColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "node", Label: "Node", Sortable: true},
		{Key: "node_status", Label: "Status", Type: plugin.ColumnBadge, Severities: map[string]plugin.Severity{"running": plugin.SeveritySuccess, "stopped": plugin.SeverityDanger}},
		{Key: "version", Label: "Version"},
		{Key: "connections", Label: "Connections", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "uptime", Label: "Uptime", Type: plugin.ColumnDuration, Sortable: true},
		{Key: "memory_used", Label: "Memory", Type: plugin.ColumnBytes, Sortable: true},
	}
}

func brokerSections() []plugin.ObjectDetailSection {
	return []plugin.ObjectDetailSection{
		{Title: "Connection", Fields: []plugin.ObjectDetailField{
			{Key: "address", Label: "Broker", Copy: true},
			{Key: "scheme", Label: "Scheme", Type: plugin.ColumnBadge},
			{Key: "client_id", Label: "Client ID", Copy: true},
			{Key: "connected", Label: "Connected", Type: plugin.ColumnBool},
			{Key: "protocol_version", Label: "Protocol"},
			{Key: "clean_session", Label: "Clean session", Type: plugin.ColumnBool},
			{Key: "keep_alive", Label: "Keep alive", Type: plugin.ColumnDuration},
			{Key: "read_only", Label: "Read-only", Type: plugin.ColumnBool},
		}},
		{Title: "Observation", Fields: []plugin.ObjectDetailField{
			{Key: "observe_filter", Label: "Observe filter", Copy: true},
			{Key: "topics", Label: "Topics observed", Type: plugin.ColumnNumber},
			{Key: "retained_topics", Label: "Retained topics", Type: plugin.ColumnNumber},
			{Key: "messages_observed", Label: "Messages observed", Type: plugin.ColumnNumber},
			{Key: "messages_buffered", Label: "Messages buffered", Type: plugin.ColumnNumber},
			{Key: "topics_dropped", Label: "Topics dropped at cap", Type: plugin.ColumnNumber},
			{Key: "topic_limit", Label: "Topic limit", Type: plugin.ColumnNumber},
		}},
		{Title: "Broker ($SYS)", Fields: []plugin.ObjectDetailField{
			{Key: "sys_version", Label: "Version"},
			{Key: "sys_uptime", Label: "Uptime"},
			{Key: "sys_clients_connected", Label: "Clients connected", Type: plugin.ColumnNumber},
			{Key: "sys_clients_total", Label: "Clients total", Type: plugin.ColumnNumber},
			{Key: "sys_subscriptions", Label: "Subscriptions", Type: plugin.ColumnNumber},
			{Key: "sys_messages_received", Label: "Messages received", Type: plugin.ColumnNumber},
			{Key: "sys_messages_sent", Label: "Messages sent", Type: plugin.ColumnNumber},
			{Key: "sys_bytes_received", Label: "Bytes received", Type: plugin.ColumnBytes},
			{Key: "sys_bytes_sent", Label: "Bytes sent", Type: plugin.ColumnBytes},
		}},
		{Title: "EMQX", Fields: []plugin.ObjectDetailField{
			{Key: "emqx_url", Label: "Dashboard URL", Copy: true},
			{Key: "emqx_enabled", Label: "Management API", Type: plugin.ColumnBool},
		}},
	}
}

func topicSections() []plugin.ObjectDetailSection {
	return []plugin.ObjectDetailSection{
		{Title: "Topic", Fields: []plugin.ObjectDetailField{
			{Key: "topic", Label: "Topic", Copy: true},
			{Key: "qos", Label: "QoS", Type: plugin.ColumnBadge, Severities: qosSeverities},
			{Key: "retained", Label: "Retained", Type: plugin.ColumnBool},
			{Key: "messages", Label: "Messages observed", Type: plugin.ColumnNumber},
			{Key: "first_seen", Label: "First seen", Type: plugin.ColumnRelativeTime},
			{Key: "last_seen", Label: "Last seen", Type: plugin.ColumnRelativeTime},
		}},
		{Title: "Last payload", Fields: []plugin.ObjectDetailField{
			{Key: "size", Label: "Size", Type: plugin.ColumnBytes},
			{Key: "encoding", Label: "Encoding", Type: plugin.ColumnBadge},
			{Key: "payload", Label: "Payload", Copy: true},
		}},
	}
}

func clientSections() []plugin.ObjectDetailSection {
	return []plugin.ObjectDetailSection{
		{Title: "Identity", Fields: []plugin.ObjectDetailField{
			{Key: "clientid", Label: "Client ID", Copy: true},
			{Key: "username", Label: "Username", Copy: true},
			{Key: "connected", Label: "Connected", Type: plugin.ColumnBool},
			{Key: "node", Label: "Node"},
			{Key: "ip_address", Label: "Address", Copy: true},
			{Key: "proto_name", Label: "Protocol"},
			{Key: "proto_ver", Label: "Protocol version", Type: plugin.ColumnNumber},
		}},
		{Title: "Session", Fields: []plugin.ObjectDetailField{
			{Key: "clean_start", Label: "Clean start", Type: plugin.ColumnBool},
			{Key: "keepalive", Label: "Keep alive", Type: plugin.ColumnDuration},
			{Key: "expiry_interval", Label: "Session expiry", Type: plugin.ColumnDuration},
			{Key: "subscriptions_cnt", Label: "Subscriptions", Type: plugin.ColumnNumber},
			{Key: "inflight_cnt", Label: "Inflight", Type: plugin.ColumnNumber},
			{Key: "mqueue_len", Label: "Queued", Type: plugin.ColumnNumber},
			{Key: "connected_at", Label: "Connected at", Type: plugin.ColumnRelativeTime},
		}},
		{Title: "Traffic", Fields: []plugin.ObjectDetailField{
			{Key: "recv_msg", Label: "Messages received", Type: plugin.ColumnNumber},
			{Key: "send_msg", Label: "Messages sent", Type: plugin.ColumnNumber},
			{Key: "recv_oct", Label: "Bytes received", Type: plugin.ColumnBytes},
			{Key: "send_oct", Label: "Bytes sent", Type: plugin.ColumnBytes},
		}},
	}
}
