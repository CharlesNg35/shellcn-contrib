// Package vault implements the HashiCorp Vault protocol plugin.
package vault

import (
	"context"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type Plugin struct{}

func New() *Plugin { return &Plugin{} }

const vaultIconSVG = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 128 128"><path fill="#ffd814" d="m0 1.953 63.76 124.094L128 1.953Zm53.841 49.254H43.684V41.06H53.84zm0-15.227H43.684V25.822H53.84ZM69.08 66.444H58.97V56.286h10.108zm0-15.237H58.97V41.06h10.108zm0-15.227H58.97V25.822h10.108Zm15.147 15.227H74.027V41.06h10.159ZM74.027 35.98V25.822h10.159V35.98z"/></svg>`

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:  plugin.CurrentAPIVersion,
		Name:        protocolName,
		Version:     "0.1.0",
		Title:       "HashiCorp Vault",
		Description: "Vault browser for secret engines, KV v1 and v2 secrets with versions and metadata, auth methods, policies, tokens, leases, namespaces, and audit devices.",
		Icon:        plugin.Icon{Type: plugin.IconSVG, Value: vaultIconSVG},
		Category:    plugin.CategorySecurity,
		Config:      configSchema(),
		Capabilities: []plugin.Capability{
			"secrets", "kv", "kv-v2", "policies", "tokens", "leases", "namespaces", "audit",
		},
		CredentialKinds:     credentialKinds(),
		SupportedTransports: []plugin.Transport{plugin.TransportDirect, plugin.TransportAgent},
		Agent: &plugin.AgentProfile{
			Proxy: plugin.ProxyTarget{Mode: plugin.AgentTCP, Risk: plugin.RiskPrivileged, Forward: true},
			Install: []plugin.InstallArtifact{{
				Label:    "Docker",
				Kind:     "docker",
				Template: "docker run -d --network host shellcn/agent --connect {{shellquote .ConnectURL}} --token {{shellquote .Token}}",
			}},
		},
		Layout:    plugin.LayoutSidebarTree,
		Scope:     scope(),
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

func credentialKinds() []plugin.CredentialKindInfo {
	return []plugin.CredentialKindInfo{{
		Kind:  credentialKindAppRole,
		Label: "Vault AppRole",
		Fields: []plugin.Field{
			plugin.CredentialPublicField(plugin.Field{Key: "role_id", Label: "Role ID", Type: plugin.FieldText, Required: true}),
			plugin.CredentialSecretField(plugin.Field{Key: "secret_id", Label: "Secret ID", Type: plugin.FieldPassword, Required: true}),
		},
	}}
}
