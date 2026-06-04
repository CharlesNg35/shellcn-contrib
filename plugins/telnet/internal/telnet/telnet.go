// Package telnet implements the Telnet terminal plugin.
package telnet

import (
	"context"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

// Plugin exposes Telnet as a terminal-only protocol.
type Plugin struct{}

func New() *Plugin { return &Plugin{} }

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:          plugin.CurrentAPIVersion,
		Name:                "telnet",
		Version:             "0.1.0",
		Title:               "Telnet",
		Description:         "Legacy Telnet terminal access.",
		Icon:                plugin.Icon{Type: plugin.IconLucide, Value: "square-terminal"},
		Category:            plugin.CategoryShell,
		Config:              configSchema(),
		Capabilities:        []plugin.Capability{"terminal"},
		SupportedTransports: []plugin.Transport{plugin.TransportDirect},
		Layout:              plugin.LayoutSingle,
		Tabs: []plugin.Panel{{
			Key: "terminal", Label: "Terminal", Icon: plugin.Icon{Type: plugin.IconLucide, Value: "terminal"},
			Type: plugin.PanelTerminal, Source: &plugin.DataSource{RouteID: "telnet.shell", Method: plugin.MethodWS, Params: map[string]string{"cols": "80", "rows": "24"}},
			Config: plugin.TerminalConfig{Zoom: true, Search: true},
		}},
		Streams: []plugin.Stream{{ID: "telnet.shell", Kind: plugin.StreamTerminal, RouteID: "telnet.shell"}},
		Recording: []plugin.RecordingCapability{{
			Class: plugin.RecordingTerminal, Formats: []plugin.RecordingFormat{plugin.FormatAsciicastV2},
			StreamIDs: []string{"telnet.shell"}, Authoritative: true,
		}},
	}
}

func (p *Plugin) Routes() []plugin.Route {
	return []plugin.Route{{
		ID: "telnet.shell", Method: plugin.MethodWS, Path: "/shell",
		Permission: "telnet.shell", Risk: plugin.RiskPrivileged,
		AuditEvent: "telnet.shell", Input: terminalSchema(), Stream: shell,
	}}
}

func (p *Plugin) Connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	return Connect(ctx, cfg)
}

func configSchema() plugin.Schema {
	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Basic", Fields: []plugin.Field{
			{Key: "host", Label: "Host", Type: plugin.FieldText, Required: true, Placeholder: "10.0.0.1"},
			{Key: "port", Label: "Port", Type: plugin.FieldNumber, Default: 23, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 65535}}},
		}},
	}}
}
