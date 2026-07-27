// Package nomad implements the HashiCorp Nomad protocol plugin.
package nomad

import (
	"context"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type Plugin struct{}

func New() *Plugin { return &Plugin{} }

const nomadIconSVG = `<svg xmlns="http://www.w3.org/2000/svg" xml:space="preserve" viewBox="0 0 512 512"><path d="M256 0 33.8 128v256L256 512l222.2-128V127.1zm98.7 280.9-59.6 33.8-71.1-39.1v81.8L156.4 400V229.3l53.3-32.9 73.8 39.1v-82.7l71.1-42.7v170.8z" style="fill:#00ca8e"/></svg>`

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:          plugin.CurrentAPIVersion,
		Name:                protocolName,
		Version:             "0.1.0",
		Title:               "Nomad",
		Description:         "HashiCorp Nomad workload orchestrator: jobs, allocations, clients, deployments, live logs, and exec.",
		Icon:                plugin.Icon{Type: plugin.IconSVG, Value: nomadIconSVG},
		Category:            plugin.CategoryOrchestration,
		Config:              configSchema(),
		Capabilities:        []plugin.Capability{"jobs", "allocations", "nodes", "deployments", "evaluations", "volumes", "logs", "exec", "metrics"},
		SupportedTransports: []plugin.Transport{plugin.TransportDirect},
		Layout:              plugin.LayoutSidebarTree,
		Tree:                tree(),
		Resources:           resources(),
		Actions:             actions(),
		Streams:             streams(),
		Scope:               scope(),
		Recording:           recording(),
	}
}

func (p *Plugin) Routes() []plugin.Route { return routes() }

func (p *Plugin) Connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	return connect(ctx, cfg)
}
