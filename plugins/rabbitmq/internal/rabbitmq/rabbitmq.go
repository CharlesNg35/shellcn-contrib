// Package rabbitmq implements the RabbitMQ protocol plugin.
package rabbitmq

import (
	"context"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type Plugin struct{}

func New() *Plugin { return &Plugin{} }

// stateSeverities colors a queue's state badge by value.
var stateSeverities = map[string]plugin.Severity{
	"running": plugin.SeveritySuccess,
	"idle":    plugin.SeveritySecondary,
	"flow":    plugin.SeverityWarn,
	"down":    plugin.SeverityDanger,
}

const rabbitMQIconSVG = `<svg width="800px" height="800px" viewBox="-7.5 0 271 271" xmlns="http://www.w3.org/2000/svg" preserveAspectRatio="xMidYMid"><path d="M245.44 108.308h-85.09a7.738 7.738 0 0 1-7.735-7.734v-88.68C152.615 5.327 147.29 0 140.726 0h-30.375c-6.568 0-11.89 5.327-11.89 11.894v88.143c0 4.573-3.697 8.29-8.27 8.31l-27.885.133c-4.612.025-8.359-3.717-8.35-8.325l.173-88.241C54.144 5.337 48.817 0 42.24 0H11.89C5.321 0 0 5.327 0 11.894V260.21c0 5.834 4.726 10.56 10.555 10.56H245.44c5.834 0 10.56-4.726 10.56-10.56V118.868c0-5.834-4.726-10.56-10.56-10.56zm-39.902 93.233c0 7.645-6.198 13.844-13.843 13.844H167.69c-7.646 0-13.844-6.199-13.844-13.844v-24.005c0-7.646 6.198-13.844 13.844-13.844h24.005c7.645 0 13.843 6.198 13.843 13.844v24.005z" fill="#F60"/></svg>`

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:          plugin.CurrentAPIVersion,
		Name:                protocolName,
		Version:             "0.1.0",
		Title:               "RabbitMQ",
		Description:         "RabbitMQ management for queues, exchanges, bindings, consumers, message inspection, and publishing.",
		Icon:                plugin.Icon{Type: plugin.IconSVG, Value: rabbitMQIconSVG},
		Category:            plugin.CategoryMessaging,
		Config:              configSchema(),
		Capabilities:        []plugin.Capability{"queues", "exchanges", "bindings", "consumers", "messages"},
		SupportedTransports: []plugin.Transport{plugin.TransportDirect},
		Layout:              plugin.LayoutSidebarTree,
		Tree:                tree(),
		Resources:           resources(),
		Actions:             actions(),
	}
}

func (p *Plugin) Routes() []plugin.Route { return routes() }

func (p *Plugin) Connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	return connect(ctx, cfg)
}

func icon(name string) plugin.Icon {
	return plugin.Icon{Type: plugin.IconLucide, Value: name}
}

func objectDetailConfig() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{RawToggle: true}
}

func tree() []plugin.TreeGroup {
	return []plugin.TreeGroup{
		{Key: "queues", Label: "Queues", Icon: icon("list"), Source: plugin.DataSource{RouteID: "rabbitmq.queues.tree"}, ResourceKind: "queue"},
		{Key: "exchanges", Label: "Exchanges", Icon: icon("shuffle"), Source: plugin.DataSource{RouteID: "rabbitmq.exchanges.tree"}, ResourceKind: "exchange"},
	}
}

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		{
			Kind: "queue", Title: "Queues", List: plugin.DataSource{RouteID: "rabbitmq.queues.list"},
			Columns: queueColumns(),
			Actions: plugin.ResourceActions{
				Toolbar: []string{"rabbitmq.queue.create"},
				Row:     []string{"rabbitmq.queue.purge", "rabbitmq.queue.delete"},
				Detail:  []string{"rabbitmq.queue.publish", "rabbitmq.queue.purge", "rabbitmq.queue.delete"},
			},
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.namespace}/${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "rabbitmq.queue.overview", Params: queueParams()}, Config: objectDetailConfig()},
				{Key: "messages", Label: "Messages", Icon: icon("mail"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "rabbitmq.queue.messages", Params: queueParams()}, Config: plugin.TableConfig{Columns: messageColumns(), ActionIDs: []string{"rabbitmq.queue.publish"}, Exportable: true}},
				{Key: "bindings", Label: "Bindings", Icon: icon("link"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "rabbitmq.bindings.list", Params: queueParams()}, Config: plugin.TableConfig{Columns: bindingColumns(), ActionIDs: []string{"rabbitmq.binding.create"}, RowActionIDs: []string{"rabbitmq.binding.delete"}, Exportable: true}},
				{Key: "consumers", Label: "Consumers", Icon: icon("users"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "rabbitmq.consumers.list", Params: queueParams()}, Config: plugin.TableConfig{Columns: consumerColumns(), Exportable: true}},
			}},
		},
		{
			Kind: "exchange", Title: "Exchanges", List: plugin.DataSource{RouteID: "rabbitmq.exchanges.list"},
			Columns: exchangeColumns(),
			Actions: plugin.ResourceActions{
				Toolbar: []string{"rabbitmq.exchange.create"},
				Row:     []string{"rabbitmq.exchange.delete"},
				Detail:  []string{"rabbitmq.message.publish", "rabbitmq.exchange.delete"},
			},
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.namespace}/${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "rabbitmq.exchange.overview", Params: exchangeParams()}, Config: objectDetailConfig()},
				{Key: "bindings", Label: "Bindings", Icon: icon("link"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "rabbitmq.exchange.bindings", Params: exchangeParams()}, Config: plugin.TableConfig{Columns: bindingColumns(), RowActionIDs: []string{"rabbitmq.binding.delete"}, Exportable: true}},
			}},
		},
	}
}

func actions() []plugin.Action {
	return []plugin.Action{
		{ID: "rabbitmq.queue.create", Label: "Create queue", Icon: icon("plus"), RouteID: "rabbitmq.queue.create"},
		{ID: "rabbitmq.queue.purge", Label: "Purge", Icon: icon("eraser"), RouteID: "rabbitmq.queue.purge", Params: queueParams(), Confirm: true, ConfirmText: "Purge every ready message in this queue?"},
		{ID: "rabbitmq.queue.delete", Label: "Delete", Icon: icon("trash-2"), RouteID: "rabbitmq.queue.delete", Params: queueParams(), Confirm: true, ConfirmText: "Delete this queue?"},
		{ID: "rabbitmq.exchange.create", Label: "Create exchange", Icon: icon("plus"), RouteID: "rabbitmq.exchange.create"},
		{ID: "rabbitmq.exchange.delete", Label: "Delete", Icon: icon("trash-2"), RouteID: "rabbitmq.exchange.delete", Params: exchangeParams(), Confirm: true, ConfirmText: "Delete this exchange?"},
		{ID: "rabbitmq.message.publish", Label: "Publish", Icon: icon("send"), RouteID: "rabbitmq.message.publish", Params: map[string]string{"vhost": "${resource.namespace}", "exchange": "${resource.name}"}, Confirm: true, ConfirmText: "Publish this message?"},
		{ID: "rabbitmq.queue.publish", Label: "Publish", Icon: icon("send"), RouteID: "rabbitmq.queue.publish", Params: queueParams(), Confirm: true, ConfirmText: "Publish this message to the queue?"},
		{ID: "rabbitmq.binding.create", Label: "Add binding", Icon: icon("link"), RouteID: "rabbitmq.binding.create", Params: queueParams()},
		{ID: "rabbitmq.binding.delete", Label: "Unbind", Icon: icon("unlink"), RouteID: "rabbitmq.binding.delete", Params: map[string]string{"spec": "${record.spec}"}, Confirm: true, ConfirmText: "Remove this binding?"},
	}
}

func queueParams() map[string]string {
	return map[string]string{"vhost": "${resource.namespace}", "queue": "${resource.name}"}
}

func exchangeParams() map[string]string {
	return map[string]string{"vhost": "${resource.namespace}", "exchange": "${resource.name}"}
}

func queueColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Queue", Sortable: true},
		{Key: "vhost", Label: "Virtual host", Sortable: true},
		{Key: "messages", Label: "Messages", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "messages_ready", Label: "Ready", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "messages_unacknowledged", Label: "Unacked", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "consumers", Label: "Consumers", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "state", Label: "State", Type: plugin.ColumnBadge, Sortable: true, Severities: stateSeverities},
	}
}

func exchangeColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Exchange", Sortable: true},
		{Key: "vhost", Label: "Virtual host", Sortable: true},
		{Key: "type", Label: "Type", Type: plugin.ColumnBadge, Sortable: true},
		{Key: "durable", Label: "Durable", Type: plugin.ColumnBool},
		{Key: "auto_delete", Label: "Auto delete", Type: plugin.ColumnBool},
		{Key: "internal", Label: "Internal", Type: plugin.ColumnBool},
	}
}

func bindingColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "source", Label: "Source", Sortable: true},
		{Key: "destination", Label: "Destination", Sortable: true},
		{Key: "destination_type", Label: "Type", Type: plugin.ColumnBadge},
		{Key: "routing_key", Label: "Routing key"},
		{Key: "arguments", Label: "Arguments", Type: plugin.ColumnJSON},
	}
}

func consumerColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "consumer_tag", Label: "Consumer tag", Sortable: true},
		{Key: "queue", Label: "Queue", Sortable: true},
		{Key: "channel_details", Label: "Channel", Type: plugin.ColumnJSON},
		{Key: "ack_required", Label: "Ack required", Type: plugin.ColumnBool},
		{Key: "prefetch_count", Label: "Prefetch", Type: plugin.ColumnNumber},
	}
}

func messageColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "routing_key", Label: "Routing key", Sortable: true},
		{Key: "exchange", Label: "Exchange", Sortable: true},
		{Key: "redelivered", Label: "Redelivered", Type: plugin.ColumnBool},
		{Key: "message_count", Label: "Remaining", Type: plugin.ColumnNumber},
		{Key: "properties", Label: "Properties", Type: plugin.ColumnJSON},
		{Key: "payload", Label: "Payload"},
	}
}
