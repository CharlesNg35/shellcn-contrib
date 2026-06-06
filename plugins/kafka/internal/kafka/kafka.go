// Package kafka implements the Kafka protocol plugin.
package kafka

import (
	"context"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type Plugin struct{}

func New() *Plugin { return &Plugin{} }

// stateSeverities colors a consumer group's state badge by value.
var stateSeverities = map[string]plugin.Severity{
	"stable":             plugin.SeveritySuccess,
	"preparingrebalance": plugin.SeverityWarn, "completingrebalance": plugin.SeverityWarn,
	"empty": plugin.SeveritySecondary, "dead": plugin.SeverityDanger,
}

const kafkaIconSVG = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 128 128"><path d="M86.758 70.89c-4.992 0-9.465 2.208-12.528 5.68l-7.851-5.547a21.275 21.275 0 001.312-7.32c0-2.531-.46-4.95-1.27-7.203l7.837-5.488c3.062 3.457 7.523 5.652 12.5 5.652 9.207 0 16.703-7.48 16.703-16.672 0-9.195-7.496-16.672-16.703-16.672-9.211 0-16.707 7.477-16.707 16.672 0 1.645.25 3.23.699 4.735l-7.84 5.488a21.578 21.578 0 00-13.36-7.746v-9.43c7.567-1.586 13.27-8.293 13.27-16.312C62.82 7.53 55.324.055 46.117.055c-9.21 0-16.707 7.476-16.707 16.672 0 7.91 5.555 14.539 12.969 16.238v9.547c-10.117 1.773-17.84 10.59-17.84 21.191 0 10.652 7.797 19.5 17.992 21.211V95c-7.492 1.64-13.12 8.309-13.12 16.273 0 9.196 7.495 16.672 16.706 16.672 9.207 0 16.703-7.476 16.703-16.672 0-7.964-5.629-14.632-13.117-16.273V84.914a21.592 21.592 0 0013.133-7.625l7.902 5.586a16.45 16.45 0 00-.687 4.688c0 9.195 7.496 16.671 16.707 16.671 9.207 0 16.703-7.476 16.703-16.671 0-9.196-7.496-16.672-16.703-16.672zm0-38.984c4.465 0 8.097 3.63 8.097 8.086 0 4.453-3.632 8.082-8.097 8.082-4.469 0-8.102-3.629-8.102-8.082 0-4.457 3.633-8.086 8.102-8.086zm-48.742-15.18c0-4.456 3.632-8.081 8.101-8.081 4.465 0 8.098 3.625 8.098 8.082 0 4.457-3.633 8.082-8.098 8.082-4.469 0-8.101-3.625-8.101-8.082zm16.199 94.547c0 4.457-3.633 8.082-8.098 8.082-4.469 0-8.101-3.625-8.101-8.082 0-4.457 3.632-8.082 8.101-8.082 4.465 0 8.098 3.625 8.098 8.082zm-8.102-36.296c-6.226 0-11.293-5.059-11.293-11.274 0-6.219 5.067-11.277 11.293-11.277 6.23 0 11.297 5.058 11.297 11.277 0 6.215-5.066 11.274-11.297 11.274zm40.645 20.668c-4.469 0-8.102-3.625-8.102-8.082 0-4.458 3.633-8.083 8.102-8.083 4.465 0 8.097 3.625 8.097 8.082 0 4.458-3.632 8.083-8.097 8.083zm0 0" fill="#f97316"/></svg>`

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:          plugin.CurrentAPIVersion,
		Name:                protocolName,
		Version:             "0.1.0",
		Title:               "Kafka",
		Description:         "Kafka cluster browser with brokers, topics, partitions, recent messages, consumer groups, offsets, and producers.",
		Icon:                plugin.Icon{Type: plugin.IconSVG, Value: kafkaIconSVG},
		Category:            plugin.CategoryMessaging,
		Config:              configSchema(),
		Capabilities:        []plugin.Capability{"topics", "partitions", "consumer_groups", "messages", "produce"},
		SupportedTransports: []plugin.Transport{plugin.TransportDirect, plugin.TransportAgent},
		Agent: &plugin.AgentProfile{
			Proxy: plugin.ProxyTarget{
				Mode:    plugin.AgentTCP,
				Risk:    plugin.RiskPrivileged,
				Forward: true,
			},
			Install: []plugin.InstallArtifact{{
				Label:    "Docker",
				Kind:     "docker",
				Template: "docker run -d --network host shellcn/agent --connect {{.ConnectURL}} --token {{.Token}}",
			}},
		},
		Layout:    plugin.LayoutSidebarTree,
		Tree:      tree(),
		Resources: resources(),
		Actions:   actions(),
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
		{Key: "topics", Label: "Topics", Icon: icon("radio-tower"), Source: plugin.DataSource{RouteID: "kafka.topics.tree"}, ResourceKind: "topic"},
		{Key: "groups", Label: "Consumer groups", Icon: icon("users"), Source: plugin.DataSource{RouteID: "kafka.groups.tree"}, ResourceKind: "consumer_group"},
	}
}

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		{
			Kind: "topic", Title: "Topics", List: plugin.DataSource{RouteID: "kafka.topics.list"},
			Columns: topicColumns(),
			Actions: plugin.ResourceActions{
				Toolbar: []string{"kafka.topic.create"},
				Row:     []string{"kafka.topic.delete"},
				Detail:  []string{"kafka.message.produce", "kafka.topic.alter_config", "kafka.topic.add_partitions", "kafka.topic.delete"},
			},
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "kafka.topic.overview", Params: map[string]string{"topic": "${resource.name}"}}, Config: objectDetailConfig()},
				{Key: "partitions", Label: "Partitions", Icon: icon("columns-3"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "kafka.partitions.list", Params: map[string]string{"topic": "${resource.name}"}}, Config: plugin.TableConfig{Columns: partitionColumns(), ActionIDs: []string{"kafka.topic.add_partitions"}, Exportable: true}},
				{Key: "messages", Label: "Messages", Icon: icon("mail"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "kafka.messages.list", Params: map[string]string{"topic": "${resource.name}"}}, Config: plugin.TableConfig{Columns: messageColumns(), ActionIDs: []string{"kafka.message.produce"}, Exportable: true}},
				{Key: "config", Label: "Config", Icon: icon("settings"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "kafka.topic.config", Params: map[string]string{"topic": "${resource.name}"}}, Config: plugin.TableConfig{Columns: configColumns(), ActionIDs: []string{"kafka.topic.alter_config"}, Exportable: true}},
			}},
		},
		{
			Kind: "consumer_group", Title: "Consumer groups", List: plugin.DataSource{RouteID: "kafka.groups.list"},
			Columns: groupColumns(),
			Actions: plugin.ResourceActions{
				Row:    []string{"kafka.group.delete"},
				Detail: []string{"kafka.group.reset_offsets", "kafka.group.delete"},
			},
			Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: "kafka.group.overview", Params: map[string]string{"group": "${resource.name}"}}, Config: objectDetailConfig()},
				{Key: "offsets", Label: "Offsets", Icon: icon("gauge"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: "kafka.group.offsets", Params: map[string]string{"group": "${resource.name}"}}, Config: plugin.TableConfig{Columns: offsetColumns(), ActionIDs: []string{"kafka.group.reset_offsets"}, Exportable: true}},
			}},
		},
	}
}

func actions() []plugin.Action {
	return []plugin.Action{
		{ID: "kafka.topic.create", Label: "Create topic", Icon: icon("plus"), RouteID: "kafka.topic.create"},
		{ID: "kafka.topic.alter_config", Label: "Alter config", Icon: icon("settings-2"), RouteID: "kafka.topic.alter_config", Params: map[string]string{"topic": "${resource.name}"}},
		{ID: "kafka.topic.add_partitions", Label: "Add partitions", Icon: icon("columns-3"), RouteID: "kafka.topic.add_partitions", Params: map[string]string{"topic": "${resource.name}"}, Confirm: true, ConfirmText: "Add partitions? Increasing partitions is irreversible and can change key-to-partition routing."},
		{ID: "kafka.topic.delete", Label: "Delete", Icon: icon("trash-2"), RouteID: "kafka.topic.delete", Params: map[string]string{"topic": "${resource.name}"}, Confirm: true, ConfirmText: "Delete this topic?"},
		{ID: "kafka.message.produce", Label: "Produce", Icon: icon("send"), RouteID: "kafka.message.produce", Params: map[string]string{"topic": "${resource.name}"}, Confirm: true, ConfirmText: "Produce this record?"},
		{ID: "kafka.group.delete", Label: "Delete", Icon: icon("trash-2"), RouteID: "kafka.group.delete", Params: map[string]string{"group": "${resource.name}"}, Confirm: true, ConfirmText: "Delete this consumer group?"},
		{ID: "kafka.group.reset_offsets", Label: "Reset offsets", Icon: icon("history"), RouteID: "kafka.group.reset_offsets", Params: map[string]string{"group": "${resource.name}"}, Confirm: true, ConfirmText: "Reset this group's committed offsets?"},
	}
}

func topicColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Topic", Sortable: true},
		{Key: "partitions", Label: "Partitions", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "replication_factor", Label: "Replication", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "internal", Label: "Internal", Type: plugin.ColumnBool},
	}
}

func partitionColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "partition", Label: "Partition", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "leader", Label: "Leader", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "oldest_offset", Label: "Oldest offset", Type: plugin.ColumnNumber},
		{Key: "newest_offset", Label: "Newest offset", Type: plugin.ColumnNumber},
		{Key: "replicas", Label: "Replicas", Type: plugin.ColumnJSON},
		{Key: "isr", Label: "ISR", Type: plugin.ColumnJSON},
	}
}

func groupColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Group", Sortable: true},
		{Key: "protocol_type", Label: "Protocol", Sortable: true},
		{Key: "state", Label: "State", Type: plugin.ColumnBadge, Sortable: true, Severities: stateSeverities},
		{Key: "members", Label: "Members", Type: plugin.ColumnNumber, Sortable: true},
	}
}

func offsetColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "topic", Label: "Topic", Sortable: true},
		{Key: "partition", Label: "Partition", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "committed_offset", Label: "Committed", Type: plugin.ColumnNumber},
		{Key: "newest_offset", Label: "Newest", Type: plugin.ColumnNumber},
		{Key: "lag", Label: "Lag", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "metadata", Label: "Metadata"},
	}
}

func messageColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "partition", Label: "Partition", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "offset", Label: "Offset", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "timestamp", Label: "Timestamp", Type: plugin.ColumnRelativeTime, Sortable: true},
		{Key: "key", Label: "Key"},
		{Key: "headers", Label: "Headers", Type: plugin.ColumnJSON},
		{Key: "value", Label: "Value"},
	}
}

func configColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Name", Sortable: true},
		{Key: "value", Label: "Value"},
		{Key: "default", Label: "Default", Type: plugin.ColumnBool},
		{Key: "read_only", Label: "Read-only", Type: plugin.ColumnBool},
		{Key: "sensitive", Label: "Sensitive", Type: plugin.ColumnBool},
	}
}
