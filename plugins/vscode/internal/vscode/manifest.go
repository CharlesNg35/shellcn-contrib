package vscode

import "github.com/charlesng35/shellcn/sdk/plugin"

const protocolName = "vscode"

const vscodeSvgIcon = `<svg fill=none viewBox="0 0 100 100"xmlns=http://www.w3.org/2000/svg><mask height=100 id=mask0 mask-type=alpha maskUnits=userSpaceOnUse width=100 x=0 y=0><path d="M70.9119 99.3171C72.4869 99.9307 74.2828 99.8914 75.8725 99.1264L96.4608 89.2197C98.6242 88.1787 100 85.9892 100 83.5872V16.4133C100 14.0113 98.6243 11.8218 96.4609 10.7808L75.8725 0.873756C73.7862 -0.130129 71.3446 0.11576 69.5135 1.44695C69.252 1.63711 69.0028 1.84943 68.769 2.08341L29.3551 38.0415L12.1872 25.0096C10.589 23.7965 8.35363 23.8959 6.86933 25.2461L1.36303 30.2549C-0.452552 31.9064 -0.454633 34.7627 1.35853 36.417L16.2471 50.0001L1.35853 63.5832C-0.454633 65.2374 -0.452552 68.0938 1.36303 69.7453L6.86933 74.7541C8.35363 76.1043 10.589 76.2037 12.1872 74.9905L29.3551 61.9587L68.769 97.9167C69.3925 98.5406 70.1246 99.0104 70.9119 99.3171ZM75.0152 27.2989L45.1091 50.0001L75.0152 72.7012V27.2989Z"fill=white clip-rule=evenodd fill-rule=evenodd /></mask><g mask=url(#mask0)><path d="M96.4614 10.7962L75.8569 0.875542C73.4719 -0.272773 70.6217 0.211611 68.75 2.08333L1.29858 63.5832C-0.515693 65.2373 -0.513607 68.0937 1.30308 69.7452L6.81272 74.754C8.29793 76.1042 10.5347 76.2036 12.1338 74.9905L93.3609 13.3699C96.086 11.3026 100 13.2462 100 16.6667V16.4275C100 14.0265 98.6246 11.8378 96.4614 10.7962Z"fill=#0065A9 /><g filter=url(#filter0_d)><path d="M96.4614 89.2038L75.8569 99.1245C73.4719 100.273 70.6217 99.7884 68.75 97.9167L1.29858 36.4169C-0.515693 34.7627 -0.513607 31.9063 1.30308 30.2548L6.81272 25.246C8.29793 23.8958 10.5347 23.7964 12.1338 25.0095L93.3609 86.6301C96.086 88.6974 100 86.7538 100 83.3334V83.5726C100 85.9735 98.6246 88.1622 96.4614 89.2038Z"fill=#007ACC /></g><g filter=url(#filter1_d)><path d="M75.8578 99.1263C73.4721 100.274 70.6219 99.7885 68.75 97.9166C71.0564 100.223 75 98.5895 75 95.3278V4.67213C75 1.41039 71.0564 -0.223106 68.75 2.08329C70.6219 0.211402 73.4721 -0.273666 75.8578 0.873633L96.4587 10.7807C98.6234 11.8217 100 14.0112 100 16.4132V83.5871C100 85.9891 98.6234 88.1786 96.4586 89.2196L75.8578 99.1263Z"fill=#1F9CF0 /></g><g opacity=0.25 style=mix-blend-mode:overlay><path d="M70.8511 99.3171C72.4261 99.9306 74.2221 99.8913 75.8117 99.1264L96.4 89.2197C98.5634 88.1787 99.9392 85.9892 99.9392 83.5871V16.4133C99.9392 14.0112 98.5635 11.8217 96.4001 10.7807L75.8117 0.873695C73.7255 -0.13019 71.2838 0.115699 69.4527 1.44688C69.1912 1.63705 68.942 1.84937 68.7082 2.08335L29.2943 38.0414L12.1264 25.0096C10.5283 23.7964 8.29285 23.8959 6.80855 25.246L1.30225 30.2548C-0.513334 31.9064 -0.515415 34.7627 1.29775 36.4169L16.1863 50L1.29775 63.5832C-0.515415 65.2374 -0.513334 68.0937 1.30225 69.7452L6.80855 74.754C8.29285 76.1042 10.5283 76.2036 12.1264 74.9905L29.2943 61.9586L68.7082 97.9167C69.3317 98.5405 70.0638 99.0104 70.8511 99.3171ZM74.9544 27.2989L45.0483 50L74.9544 72.7012V27.2989Z"fill=url(#paint0_linear) clip-rule=evenodd fill-rule=evenodd /></g></g><defs><filter color-interpolation-filters=sRGB filterUnits=userSpaceOnUse height=92.2456 id=filter0_d width=116.727 x=-8.39411 y=15.8291><feFlood flood-opacity=0 result=BackgroundImageFix /><feColorMatrix type=matrix values="0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 127 0"in=SourceAlpha /><feOffset/><feGaussianBlur stdDeviation=4.16667 /><feColorMatrix type=matrix values="0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0.25 0"/><feBlend in2=BackgroundImageFix mode=overlay result=effect1_dropShadow /><feBlend in2=effect1_dropShadow mode=normal result=shape in=SourceGraphic /></filter><filter color-interpolation-filters=sRGB filterUnits=userSpaceOnUse height=116.151 id=filter1_d width=47.9167 x=60.4167 y=-8.07558><feFlood flood-opacity=0 result=BackgroundImageFix /><feColorMatrix type=matrix values="0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 127 0"in=SourceAlpha /><feOffset/><feGaussianBlur stdDeviation=4.16667 /><feColorMatrix type=matrix values="0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0.25 0"/><feBlend in2=BackgroundImageFix mode=overlay result=effect1_dropShadow /><feBlend in2=effect1_dropShadow mode=normal result=shape in=SourceGraphic /></filter><linearGradient gradientUnits=userSpaceOnUse id=paint0_linear x1=49.9392 x2=49.9392 y1=0.257812 y2=99.7423><stop stop-color=white /><stop stop-color=white offset=1 stop-opacity=0 /></linearGradient></defs></svg>`

func New() VSCode { return VSCode{} }

func (VSCode) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:  plugin.CurrentAPIVersion,
		Name:        protocolName,
		Version:     "0.1.0",
		Title:       "VS Code",
		Description: "Agent-hosted Docker sandbox running code-server through a connection-scoped web proxy.",
		Icon:        plugin.Icon{Type: plugin.IconSVG, Value: vscodeSvgIcon},
		Category:    plugin.CategoryDevOps,
		Config:      configSchema(),
		Capabilities: []plugin.Capability{
			"editor",
			"repository",
			"web_proxy",
		},
		SupportedTransports: []plugin.Transport{plugin.TransportAgent},
		Agent: &plugin.AgentProfile{
			Proxy: plugin.ProxyTarget{Mode: plugin.AgentUnix, Address: agentDockerSocket, Risk: plugin.RiskPrivileged, Forward: true},
			Install: []plugin.InstallArtifact{
				{
					Label:      "Docker",
					Kind:       "docker-run",
					ConnectURL: plugin.ArtifactConnectURL{LocalhostHost: "host.docker.internal"},
					Template: "docker run --rm --name " + plugin.AgentBinary + "-vscode --network host " +
						"{{if .LocalhostHostRequired}}--add-host={{.LocalhostHost}}:host-gateway {{end}}" +
						`--group-add "$(stat -c '%g' /var/run/docker.sock)" ` +
						"-e SHELLCN_CONNECT_URL={{shellquote .ConnectURL}} " +
						"{{if .Insecure}}-e SHELLCN_INSECURE=1 {{end}}" +
						"-e SHELLCN_ENROLL_TOKEN={{shellquote .Token}} " +
						"-v {{shellquote \"/var/run/docker.sock:/var/docker/docker.sock\"}} " +
						"{{shellquote .Image}}",
				},
				{
					Label:    "Docker Compose",
					Kind:     "docker-compose",
					Filename: "shellcn-vscode-agent.compose.yml",
					Content:  composeContent,
				},
			},
		},
		Layout: plugin.LayoutSingle,
		Tabs: []plugin.Panel{{
			Key:   "editor",
			Label: "Editor",
			Icon:  icon("square-code"),
			Type:  plugin.PanelWebProxy,
			Config: plugin.WebProxyConfig{
				Path:          "/",
				InlineToolbar: boolPtr(false),
				Capabilities: []plugin.WebProxyCapability{
					plugin.WebProxyCapabilityClipboard,
					plugin.WebProxyCapabilityDownloads,
					plugin.WebProxyCapabilityFullscreen,
					plugin.WebProxyCapabilitySameOrigin,
				},
				AriaLabel:    "VS Code workspace",
				Instructions: "Edit the workspace through the connection-scoped editor proxy.",
			},
		}},
	}
}

func (VSCode) Routes() []plugin.Route { return nil }

func configSchema() plugin.Schema {
	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Sandbox", Fields: []plugin.Field{
			{Key: "sandbox", Label: "Sandbox", Type: plugin.FieldSelect, Default: sandboxDocker, Help: "Docker is the supported sandbox runtime.", Options: []plugin.Option{
				{Label: "Docker", Value: sandboxDocker},
			}},
		}},
		{Name: "Workspace", Fields: []plugin.Field{
			{Key: "workspace_path", Label: "Workspace path", Type: plugin.FieldText, Default: defaultWorkspacePath, Placeholder: defaultWorkspacePath, Help: "Folder path sent to the editor on the initial load."},
		}},
		{Name: "Repository", Fields: []plugin.Field{
			{Key: "repository_url", Label: "Repository URL", Type: plugin.FieldURL, Placeholder: "https://github.com/acme/app.git"},
			{Key: "repository_ref", Label: "Repository ref", Type: plugin.FieldText, Placeholder: "main"},
			{Key: "repository_auth", Label: "Repository auth", Type: plugin.FieldSelect, Default: "none", Options: []plugin.Option{
				{Label: "None", Value: "none"},
				{Label: "Stored token", Value: "stored_token"},
				{Label: "Inline token", Value: "inline_token"},
				{Label: "Stored username/password", Value: "stored_basic"},
				{Label: "Inline username/password", Value: "inline_basic"},
			}},
			{Key: "repository_token", Label: "Stored token", Type: plugin.FieldCredentialRef, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindAPIToken,
			}, VisibleWhen: visibleRule("repository_auth", "stored_token")},
			{Key: "repository_token_value", Label: "Inline token", Type: plugin.FieldPassword, Secret: true, VisibleWhen: visibleRule("repository_auth", "inline_token")},
			{Key: "repository_basic", Label: "Stored username/password", Type: plugin.FieldCredentialRef, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindBasicAuth,
			}, VisibleWhen: visibleRule("repository_auth", "stored_basic")},
			{Key: "repository_basic_username", Label: "Repository username", Type: plugin.FieldText, VisibleWhen: visibleRule("repository_auth", "inline_basic")},
			{Key: "repository_basic_password", Label: "Repository password", Type: plugin.FieldPassword, Secret: true, VisibleWhen: visibleRule("repository_auth", "inline_basic")},
		}},
	}}
}

func icon(name string) plugin.Icon {
	return plugin.Icon{Type: plugin.IconLucide, Value: name}
}

func visibleRule(field string, value any) *plugin.Condition {
	return &plugin.Condition{AllOf: []plugin.Rule{{Field: field, Op: plugin.OpEq, Value: value}}}
}

func boolPtr(v bool) *bool {
	return &v
}

const composeContent = `services:
  shellcn-agent:
    image: "{{.Image}}"
    container_name: shellcn-agent-vscode
    restart: unless-stopped
    network_mode: host
    security_opt:
      - no-new-privileges:true
    cap_drop:
      - ALL
    read_only: true
    tmpfs:
      - /tmp
    environment:
      SHELLCN_CONNECT_URL: "{{.GatewayConnectURL}}"
      SHELLCN_ENROLL_TOKEN: "{{.Token}}"
{{if .Insecure}}      SHELLCN_INSECURE: "1"
{{end}}    volumes:
      - "/var/run/docker.sock:/var/docker/docker.sock"
`
