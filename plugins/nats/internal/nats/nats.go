// Package nats implements the NATS protocol plugin.
package nats

import (
	"context"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type Plugin struct{}

func New() *Plugin { return &Plugin{} }

const natsIconSVG = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 128 128"><rect width="128" height="128" rx="18" fill="#27aae1"/><path fill="#fff" d="M31 84V37h13l27 28V37h15v47H74L46 55v29z"/><path fill="#164b78" d="M88 87c9-10 13-21 12-33-2-19-17-34-36-34-15 0-29 9-34 23 8-8 18-12 30-10 17 2 29 16 29 33 0 8-2 15-7 21z"/></svg>`

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:          plugin.CurrentAPIVersion,
		Name:                protocolName,
		Version:             "0.1.0",
		Title:               "NATS",
		Description:         "NATS and JetStream browser with server info, streams, consumers, stored messages, and publishing.",
		Icon:                plugin.Icon{Type: plugin.IconSVG, Value: natsIconSVG},
		Category:            plugin.CategoryMessaging,
		Config:              configSchema(),
		Capabilities:        []plugin.Capability{"subjects", "streams", "consumers", "messages", "publish"},
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
		{Key: "streams", Label: "Streams", Icon: icon("waves"), Source: plugin.DataSource{RouteID: "nats.streams.tree"}, ResourceKind: "stream"},
	}
}

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		{
			Kind: "stream", Title: "Streams", List: plugin.DataSource{RouteID: "nats.streams.list"},
			Columns: streamColumns(),
			Actions: plugin.ResourceActions{
				Toolbar: []string{"nats.stream.create"},
				Row:     []string{"nats.stream.purge", "nats.stream.delete"},
				Detail:  []string{"nats.message.publish", "nats.stream.update", "nats.stream.purge", "nats.stream.delete"},
			},
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "nats.stream.overview", Params: map[string]string{"stream": "${resource.name}"}}, Config: objectDetailConfig()},
				{Key: "messages", Label: "Messages", Icon: icon("mail"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "nats.messages.list", Params: map[string]string{"stream": "${resource.name}"}}, Config: plugin.TableConfig{Columns: messageColumns(), ActionIDs: []string{"nats.message.publish"}, RowActionIDs: []string{"nats.message.delete"}, Exportable: true}},
				{Key: "consumers", Label: "Consumers", Icon: icon("users"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "nats.consumers.list", Params: map[string]string{"stream": "${resource.name}"}}, Config: plugin.TableConfig{Columns: consumerColumns(), ActionIDs: []string{"nats.consumer.create"}, RowActionIDs: []string{"nats.consumer.delete"}, Exportable: true}},
			}},
		},
		{
			Kind: "consumer", Title: "Consumers", List: plugin.DataSource{RouteID: "nats.consumers.list"},
			Columns: consumerColumns(),
			Actions: plugin.ResourceActions{
				Row:    []string{"nats.consumer.delete"},
				Detail: []string{"nats.consumer.delete"},
			},
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.namespace}/${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "nats.consumer.overview", Params: map[string]string{"stream": "${resource.namespace}", "consumer": "${resource.name}"}}, Config: objectDetailConfig()},
			}},
		},
	}
}

func actions() []plugin.Action {
	return []plugin.Action{
		{ID: "nats.stream.create", Label: "Create stream", Icon: icon("plus"), RouteID: "nats.stream.create"},
		{ID: "nats.stream.update", Label: "Edit", Icon: icon("pencil"), RouteID: "nats.stream.update", Params: map[string]string{"stream": "${resource.name}"}},
		{ID: "nats.stream.purge", Label: "Purge", Icon: icon("eraser"), RouteID: "nats.stream.purge", Params: map[string]string{"stream": "${resource.name}"}, Confirm: true, ConfirmText: "Purge every message in this stream?"},
		{ID: "nats.stream.delete", Label: "Delete", Icon: icon("trash-2"), RouteID: "nats.stream.delete", Params: map[string]string{"stream": "${resource.name}"}, Confirm: true, ConfirmText: "Delete this stream?"},
		{ID: "nats.consumer.create", Label: "Create consumer", Icon: icon("plus"), RouteID: "nats.consumer.create", Params: map[string]string{"stream": "${resource.name}"}},
		{ID: "nats.consumer.delete", Label: "Delete", Icon: icon("trash-2"), RouteID: "nats.consumer.delete", Params: map[string]string{"stream": "${resource.namespace}", "consumer": "${resource.name}"}, Confirm: true, ConfirmText: "Delete this consumer?"},
		{ID: "nats.message.publish", Label: "Publish", Icon: icon("send"), RouteID: "nats.message.publish", Confirm: true, ConfirmText: "Publish this message?"},
		{ID: "nats.message.delete", Label: "Delete", Icon: icon("trash"), RouteID: "nats.message.delete", Params: map[string]string{"stream": "${resource.namespace}", "sequence": "${resource.name}"}, Confirm: true, ConfirmText: "Delete this message?"},
	}
}

func streamColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Stream", Sortable: true},
		{Key: "subjects", Label: "Subjects", Type: plugin.ColumnJSON},
		{Key: "messages", Label: "Messages", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "bytes", Label: "Bytes", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "consumers", Label: "Consumers", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "storage", Label: "Storage", Type: plugin.ColumnBadge},
		{Key: "created", Label: "Created", Type: plugin.ColumnRelativeTime, Sortable: true},
	}
}

func consumerColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Consumer", Sortable: true},
		{Key: "stream", Label: "Stream", Sortable: true},
		{Key: "deliver_policy", Label: "Deliver policy", Type: plugin.ColumnNumber},
		{Key: "ack_policy", Label: "Ack policy", Type: plugin.ColumnNumber},
		{Key: "pending", Label: "Pending", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "ack_pending", Label: "Ack pending", Type: plugin.ColumnNumber},
		{Key: "created", Label: "Created", Type: plugin.ColumnRelativeTime, Sortable: true},
	}
}

func messageColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "sequence", Label: "Sequence", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "subject", Label: "Subject", Sortable: true},
		{Key: "time", Label: "Time", Type: plugin.ColumnRelativeTime, Sortable: true},
		{Key: "headers", Label: "Headers", Type: plugin.ColumnJSON},
		{Key: "data", Label: "Data"},
	}
}
