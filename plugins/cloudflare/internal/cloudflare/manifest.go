package cloudflare

import "github.com/charlesng35/shellcn/sdk/plugin"

const CloudflareSvgIcon = `<svg width="800px" height="800px" viewBox="0 -70 256 256" version="1.1" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" preserveAspectRatio="xMidYMid"><g><g transform="translate(0.000000, -1.000000)"><path d="M202.3569,50.394 L197.0459,48.27 C172.0849,104.434 72.7859,70.289 66.8109,86.997 C65.8149,98.283 121.0379,89.143 160.5169,91.056 C172.5559,91.639 178.5929,100.727 173.4809,115.54 L183.5499,115.571 C195.1649,79.362 232.2329,97.841 233.7819,85.891 C231.2369,78.034 191.1809,85.891 202.3569,50.394 Z" fill="#FFFFFF"></path><path d="M176.332,109.3483 C177.925,104.0373 177.394,98.7263 174.739,95.5393 C172.083,92.3523 168.365,90.2283 163.585,89.6973 L71.17,88.6343 C70.639,88.6343 70.108,88.1033 69.577,88.1033 C69.046,87.5723 69.046,87.0413 69.577,86.5103 C70.108,85.4483 70.639,84.9163 71.701,84.9163 L164.647,83.8543 C175.801,83.3233 187.486,74.2943 191.734,63.6723 L197.046,49.8633 C197.046,49.3313 197.577,48.8003 197.046,48.2693 C191.203,21.1823 166.772,0.9993 138.091,0.9993 C111.535,0.9993 88.697,17.9953 80.73,41.8963 C75.419,38.1783 69.046,36.0533 61.61,36.5853 C48.863,37.6473 38.772,48.2693 37.178,61.0163 C36.647,64.2033 37.178,67.3903 37.71,70.5763 C16.996,71.1073 0,88.1033 0,109.3483 C0,111.4723 0,113.0663 0.531,115.1903 C0.531,116.2533 1.593,116.7843 2.125,116.7843 L172.614,116.7843 C173.676,116.7843 174.739,116.2533 174.739,115.1903 L176.332,109.3483 Z" fill="#F4811F"></path><path d="M205.5436,49.8628 L202.8876,49.8628 C202.3566,49.8628 201.8256,50.3938 201.2946,50.9248 L197.5766,63.6718 C195.9836,68.9828 196.5146,74.2948 199.1706,77.4808 C201.8256,80.6678 205.5436,82.7918 210.3236,83.3238 L229.9756,84.3858 C230.5066,84.3858 231.0376,84.9168 231.5686,84.9168 C232.0996,85.4478 232.0996,85.9788 231.5686,86.5098 C231.0376,87.5728 230.5066,88.1038 229.4436,88.1038 L209.2616,89.1658 C198.1076,89.6968 186.4236,98.7258 182.1746,109.3478 L181.1116,114.1288 C180.5806,114.6598 181.1116,115.7218 182.1746,115.7218 L252.2826,115.7218 C253.3446,115.7218 253.8756,115.1908 253.8756,114.1288 C254.9376,109.8798 255.9996,105.0998 255.9996,100.3188 C255.9996,72.7008 233.1616,49.8628 205.5436,49.8628" fill="#FAAD3F"></path></g></g></svg>`

func New() plugin.Plugin { return Cloudflare{} }

func (Cloudflare) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:          plugin.CurrentAPIVersion,
		Name:                protocolName,
		Version:             "0.1.0",
		Title:               "Cloudflare",
		Description:         "Cloudflare cockpit for accounts, zones, DNS, cache purge, tunnels, WAF/rulesets, page rules, certificates, Workers routes, settings, and API exploration.",
		Icon:                plugin.Icon{Type: plugin.IconSVG, Value: CloudflareSvgIcon},
		Category:            plugin.CategoryCloud,
		Config:              configSchema(),
		Capabilities:        []plugin.Capability{"accounts", "zones", "dns", "cache", "tunnels", "waf", "rulesets", "certificates", "workers", "api"},
		SupportedTransports: []plugin.Transport{plugin.TransportDirect},
		Layout:              plugin.LayoutSidebarTree,
		Tabs: []plugin.Panel{
			{
				Key:   "cockpit",
				Label: "Cockpit",
				Icon:  icon("layout-dashboard"),
				Type:  plugin.PanelWasm,
				Config: plugin.WasmConfig{
					Entry:     "app.js",
					Runtime:   plugin.WasmRuntimeGeneric,
					Boot:      plugin.WasmBoot{Scripts: []string{"app.js"}},
					ScaleMode: plugin.WasmScaleScroll,
					Assets: []plugin.WasmAsset{
						wasmAsset("app.js", "text/javascript"),
					},
					Bridge: plugin.WasmBridge{Routes: []plugin.WasmBridgeRoute{
						{RouteID: rid("summary"), Method: plugin.MethodGet},
						{RouteID: rid("zones.list"), Method: plugin.MethodGet},
						{RouteID: rid("accounts.list"), Method: plugin.MethodGet},
						{RouteID: rid("dns.list"), Method: plugin.MethodGet},
						{RouteID: rid("rulesets.list"), Method: plugin.MethodGet},
						{RouteID: rid("tunnels.list"), Method: plugin.MethodGet},
						{RouteID: rid("cache.purge"), Method: plugin.MethodPost},
					}},
					Capabilities: plugin.WasmCapabilities{Keyboard: true, Pointer: true},
					AriaLabel:    "Cloudflare operations cockpit",
					Instructions: "Review Cloudflare zones, accounts, DNS, rulesets, tunnels, and safe cache actions from the sandboxed cockpit.",
				},
			},
			{
				Key:    "zones",
				Label:  "Zones",
				Icon:   icon("globe"),
				Type:   plugin.PanelTable,
				Source: &plugin.DataSource{RouteID: rid("zones.list")},
				Config: plugin.TableConfig{Columns: zoneColumns(), Exportable: true, RowActionIDs: []string{rid("zone.pause"), rid("zone.unpause")}},
			},
			{
				Key:    "accounts",
				Label:  "Accounts",
				Icon:   icon("building-2"),
				Type:   plugin.PanelTable,
				Source: &plugin.DataSource{RouteID: rid("accounts.list")},
				Config: plugin.TableConfig{Columns: accountColumns(), Exportable: true},
			},
			{
				Key:   "api",
				Label: "API",
				Icon:  icon("send"),
				Type:  plugin.PanelHTTPClient,
				Config: plugin.HTTPClientConfig{
					ExecuteRouteID: rid("api.execute"),
					Methods:        []string{"GET", "POST", "PUT", "PATCH", "DELETE"},
					DefaultMethod:  "GET",
					DefaultURL:     "/zones",
					DefaultBody:    "{\n  \"comment\": \"Cloudflare API request body\"\n}",
				},
			},
		},
		Tree:      tree(),
		Resources: resources(),
		Actions:   actions(),
	}
}

func (Cloudflare) Routes() []plugin.Route { return routes() }

func icon(name string) plugin.Icon {
	return plugin.Icon{Type: plugin.IconLucide, Value: name}
}

func rid(name string) string { return protocolName + "." + name }

func configSchema() plugin.Schema {
	stored := &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "stored_token"}}}
	inline := &plugin.Condition{AllOf: []plugin.Rule{{Field: "auth", Op: plugin.OpEq, Value: "token"}}}
	return plugin.Schema{Groups: []plugin.Group{
		{Name: "Cloudflare API", Fields: []plugin.Field{
			{Key: "endpoint", Label: "API endpoint", Type: plugin.FieldURL, Required: true, Default: defaultEndpoint},
			{Key: "account_id", Label: "Default account ID", Type: plugin.FieldText, Placeholder: "023e105f4ecef8ad9ca31a8372d0c353", Help: "Used by account-scoped APIs such as tunnels. Empty means all visible accounts are listed."},
			{Key: "zone_filter", Label: "Zone filter", Type: plugin.FieldText, Placeholder: "example.com", Help: "Optional substring filter for the zone list."},
		}},
		{Name: "Authentication", Fields: []plugin.Field{
			{Key: "auth", Label: "Authentication", Type: plugin.FieldSelect, Required: true, Default: "stored_token", Options: []plugin.Option{
				{Label: "Stored API token", Value: "stored_token"},
				{Label: "Inline API token", Value: "token"},
			}},
			{Key: credentialField, Label: "API token credential", Type: plugin.FieldCredentialRef, Required: true, Credential: &plugin.CredentialSelector{
				Kind: plugin.CredentialKindAPIToken, Protocols: []string{protocolName},
			}, VisibleWhen: stored},
			{Key: "api_token", Label: "API token", Type: plugin.FieldPassword, Required: true, Secret: true, VisibleWhen: inline},
		}},
		{Name: "Behavior", Fields: []plugin.Field{
			{Key: "timeout", Label: "Request timeout", Type: plugin.FieldDuration, Default: "20s"},
			{Key: "page_limit", Label: "Page limit", Type: plugin.FieldNumber, Default: defaultPageLimit, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: plugin.MaxPageLimit}}},
			{Key: "read_only", Label: "Read-only mode", Type: plugin.FieldToggle, Default: true, Help: "Blocks write and destructive routes even when the token has permission."},
			{Key: "include_legacy_firewall", Label: "Include legacy firewall/page-rule APIs", Type: plugin.FieldToggle, Default: true},
		}},
	}}
}

func tree() []plugin.TreeGroup {
	return []plugin.TreeGroup{
		{Key: "zones", Label: "Zones", Icon: icon("globe"), Source: plugin.DataSource{RouteID: rid("zones.tree")}, ResourceKind: "zone"},
		{Key: "accounts", Label: "Accounts", Icon: icon("building-2"), Source: plugin.DataSource{RouteID: rid("accounts.tree")}, ResourceKind: "account"},
		{Key: "rulesets", Label: "Rulesets", Icon: icon("shield"), ResourceKind: "ruleset"},
		{Key: "tunnels", Label: "Tunnels", Icon: icon("route"), ResourceKind: "tunnel"},
	}
}

func resources() []plugin.ResourceType {
	return []plugin.ResourceType{
		zoneResource(),
		accountResource(),
		dnsRecordResource(),
		rulesetResource(),
		tunnelResource(),
		certResource(),
		workerRouteResource(),
		pageRuleResource(),
		settingResource(),
	}
}

func zoneResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind:    "zone",
		Title:   "Zones",
		List:    plugin.DataSource{RouteID: rid("zones.list")},
		Columns: zoneColumns(),
		Actions: plugin.ResourceActions{
			Row:    []string{rid("zone.pause"), rid("zone.unpause")},
			Detail: []string{rid("cache.purge"), rid("zone.pause"), rid("zone.unpause")},
		},
		Detail: plugin.DetailView{
			Header:     plugin.HeaderSpec{Title: "${resource.name}", StatusField: "status", Severities: statusSeverities()},
			DefaultTab: "overview",
			Tabs: []plugin.Panel{
				{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("zone.read"), Params: zoneParams()}, Config: plugin.ObjectDetailConfig{RawToggle: true}},
				{Key: "dns", Label: "DNS", Icon: icon("list-tree"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("dns.list"), Params: zoneParams()}, Config: plugin.TableConfig{Columns: dnsColumns(), Exportable: true, ActionIDs: []string{rid("dns.create")}, RowActionIDs: []string{rid("dns.update"), rid("dns.delete")}}},
				{Key: "rulesets", Label: "Rulesets", Icon: icon("shield"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("rulesets.list"), Params: zoneParams()}, Config: plugin.TableConfig{Columns: rulesetColumns(), Exportable: true}},
				{Key: "waf", Label: "WAF", Icon: icon("shield-alert"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("waf.list"), Params: zoneParams()}, Config: plugin.TableConfig{Columns: rulesetColumns(), Exportable: true}},
				{Key: "firewall", Label: "Firewall", Icon: icon("flame"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("firewall.rules.list"), Params: zoneParams()}, Config: plugin.TableConfig{Columns: firewallColumns(), Exportable: true}},
				{Key: "page_rules", Label: "Page rules", Icon: icon("scroll-text"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("page_rules.list"), Params: zoneParams()}, Config: plugin.TableConfig{Columns: pageRuleColumns(), Exportable: true, RowActionIDs: []string{rid("page_rule.delete")}}},
				{Key: "certificates", Label: "Certificates", Icon: icon("badge-check"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("certificates.list"), Params: zoneParams()}, Config: plugin.TableConfig{Columns: certificateColumns(), Exportable: true}},
				{Key: "workers", Label: "Workers", Icon: icon("workflow"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("workers.routes.list"), Params: zoneParams()}, Config: plugin.TableConfig{Columns: workerRouteColumns(), Exportable: true}},
				{Key: "settings", Label: "Settings", Icon: icon("sliders-horizontal"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("zone.settings.list"), Params: zoneParams()}, Config: plugin.TableConfig{Columns: settingColumns(), Exportable: true, RowActionIDs: []string{rid("zone.setting.update")}}},
				{Key: "cache", Label: "Cache", Icon: icon("refresh-cw"), Type: plugin.PanelForm, Config: plugin.FormPanelConfig{SubmitRouteID: rid("cache.purge"), SubmitMethod: plugin.MethodPost, SubmitLabel: "Purge cache", Params: zoneParams()}},
			},
		},
	}
}

func accountResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind:    "account",
		Title:   "Accounts",
		List:    plugin.DataSource{RouteID: rid("accounts.list")},
		Columns: accountColumns(),
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("account.read"), Params: accountParams()}, Config: plugin.ObjectDetailConfig{RawToggle: true}},
			{Key: "tunnels", Label: "Tunnels", Icon: icon("route"), Type: plugin.PanelTable, Source: &plugin.DataSource{RouteID: rid("tunnels.list"), Params: accountParams()}, Config: plugin.TableConfig{Columns: tunnelColumns(), Exportable: true}},
		}},
	}
}

func dnsRecordResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind:    "dns_record",
		Title:   "DNS records",
		List:    plugin.DataSource{RouteID: rid("dns.list")},
		Columns: dnsColumns(),
		Actions: plugin.ResourceActions{Toolbar: []string{rid("dns.create")}, Row: []string{rid("dns.update"), rid("dns.delete")}, Detail: []string{rid("dns.update"), rid("dns.delete")}},
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}", StatusField: "type"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("dns.read"), Params: dnsParams()}, Config: plugin.ObjectDetailConfig{RawToggle: true}},
		}},
	}
}

func rulesetResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind:    "ruleset",
		Title:   "Rulesets",
		List:    plugin.DataSource{RouteID: rid("rulesets.list")},
		Columns: rulesetColumns(),
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}", StatusField: "phase"}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("ruleset.read"), Params: rulesetParams()}, Config: plugin.ObjectDetailConfig{RawToggle: true}},
		}},
	}
}

func tunnelResource() plugin.ResourceType {
	return plugin.ResourceType{
		Kind:    "tunnel",
		Title:   "Tunnels",
		List:    plugin.DataSource{RouteID: rid("tunnels.list")},
		Columns: tunnelColumns(),
		Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}", StatusField: "status", Severities: statusSeverities()}, Tabs: []plugin.Panel{
			{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("tunnel.read"), Params: tunnelParams()}, Config: plugin.ObjectDetailConfig{RawToggle: true}},
			{Key: "config", Label: "Config", Icon: icon("braces"), Type: plugin.PanelDocument, Source: &plugin.DataSource{RouteID: rid("tunnel.config"), Params: tunnelParams()}},
		}},
	}
}

func certResource() plugin.ResourceType {
	return plugin.ResourceType{Kind: "certificate", Title: "Certificates", List: plugin.DataSource{RouteID: rid("certificates.list")}, Columns: certificateColumns(), Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("certificate.read"), Params: certParams()}, Config: plugin.ObjectDetailConfig{RawToggle: true}}}}}
}

func workerRouteResource() plugin.ResourceType {
	return plugin.ResourceType{Kind: "worker_route", Title: "Workers routes", List: plugin.DataSource{RouteID: rid("workers.routes.list")}, Columns: workerRouteColumns(), Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("worker.route.read"), Params: workerRouteParams()}, Config: plugin.ObjectDetailConfig{RawToggle: true}}}}}
}

func pageRuleResource() plugin.ResourceType {
	return plugin.ResourceType{Kind: "page_rule", Title: "Page rules", List: plugin.DataSource{RouteID: rid("page_rules.list")}, Columns: pageRuleColumns(), Actions: plugin.ResourceActions{Row: []string{rid("page_rule.delete")}, Detail: []string{rid("page_rule.delete")}}, Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}", StatusField: "status", Severities: statusSeverities()}, Tabs: []plugin.Panel{{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("page_rule.read"), Params: pageRuleParams()}, Config: plugin.ObjectDetailConfig{RawToggle: true}}}}}
}

func settingResource() plugin.ResourceType {
	return plugin.ResourceType{Kind: "setting", Title: "Zone settings", List: plugin.DataSource{RouteID: rid("zone.settings.list")}, Columns: settingColumns(), Actions: plugin.ResourceActions{Row: []string{rid("zone.setting.update")}, Detail: []string{rid("zone.setting.update")}}, Detail: plugin.DetailView{Header: plugin.HeaderSpec{Title: "${resource.name}"}, Tabs: []plugin.Panel{{Key: "overview", Label: "Overview", Icon: icon("info"), Type: plugin.PanelObjectDetail, Source: &plugin.DataSource{RouteID: rid("zone.setting.read"), Params: settingParams()}, Config: plugin.ObjectDetailConfig{RawToggle: true}}}}}
}

func actions() []plugin.Action {
	return []plugin.Action{
		{ID: rid("dns.create"), Label: "Create DNS record", Icon: icon("plus"), RouteID: rid("dns.create"), Params: zoneParams(), Confirm: true, OnSuccess: &plugin.ActionSuccess{SelectTab: "dns"}},
		{ID: rid("dns.update"), Label: "Edit DNS record", Icon: icon("pencil"), RouteID: rid("dns.update"), Params: dnsParams(), Confirm: true, OnSuccess: &plugin.ActionSuccess{SelectTab: "dns"}},
		{ID: rid("dns.delete"), Label: "Delete DNS record", Icon: icon("trash"), RouteID: rid("dns.delete"), Params: dnsParams(), Confirm: true, ConfirmText: "Delete this Cloudflare DNS record?"},
		{ID: rid("cache.purge"), Label: "Purge cache", Icon: icon("refresh-cw"), RouteID: rid("cache.purge"), Params: zoneParams(), Confirm: true, ConfirmText: "Purge selected Cloudflare cache objects?"},
		{ID: rid("zone.pause"), Label: "Pause zone", Icon: icon("pause"), RouteID: rid("zone.pause"), Params: zoneParams(), Confirm: true},
		{ID: rid("zone.unpause"), Label: "Unpause zone", Icon: icon("play"), RouteID: rid("zone.unpause"), Params: zoneParams(), Confirm: true},
		{ID: rid("page_rule.delete"), Label: "Delete page rule", Icon: icon("trash"), RouteID: rid("page_rule.delete"), Params: pageRuleParams(), Confirm: true},
		{ID: rid("zone.setting.update"), Label: "Update setting", Icon: icon("save"), RouteID: rid("zone.setting.update"), Params: settingParams(), Confirm: true},
	}
}

func wasmAsset(name, mime string) plugin.WasmAsset {
	return plugin.WasmAsset{Path: name, MIME: mime, Source: plugin.DataSource{RouteID: rid("asset"), Params: map[string]string{"path": name}}}
}

func zoneParams() map[string]string { return map[string]string{"zone": "${resource.uid}"} }
func accountParams() map[string]string {
	return map[string]string{"account": "${resource.uid}"}
}
func dnsParams() map[string]string {
	return map[string]string{"zone": "${resource.namespace}", "record": "${resource.uid}"}
}
func rulesetParams() map[string]string {
	return map[string]string{"zone": "${resource.namespace}", "ruleset": "${resource.uid}"}
}
func tunnelParams() map[string]string {
	return map[string]string{"account": "${resource.namespace}", "tunnel": "${resource.uid}"}
}
func certParams() map[string]string {
	return map[string]string{"zone": "${resource.namespace}", "cert": "${resource.uid}"}
}
func workerRouteParams() map[string]string {
	return map[string]string{"zone": "${resource.namespace}", "route": "${resource.uid}"}
}
func pageRuleParams() map[string]string {
	return map[string]string{"zone": "${resource.namespace}", "rule": "${resource.uid}"}
}
func settingParams() map[string]string {
	return map[string]string{"zone": "${resource.namespace}", "setting": "${resource.uid}"}
}

func statusSeverities() map[string]plugin.Severity {
	return map[string]plugin.Severity{
		"active": plugin.SeveritySuccess, "healthy": plugin.SeveritySuccess, "on": plugin.SeveritySuccess, "enabled": plugin.SeveritySuccess,
		"pending": plugin.SeverityWarn, "initializing": plugin.SeverityWarn, "moved": plugin.SeverityWarn,
		"paused": plugin.SeveritySecondary, "off": plugin.SeveritySecondary, "disabled": plugin.SeveritySecondary,
		"error": plugin.SeverityDanger, "degraded": plugin.SeverityDanger, "deleted": plugin.SeverityDanger,
	}
}

func accountColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Account", Sortable: true}, {Key: "id", Label: "ID"}, {Key: "type", Label: "Type", Type: plugin.ColumnBadge}, {Key: "created_on", Label: "Created", Type: plugin.ColumnRelativeTime, Sortable: true}}
}

func zoneColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Zone", Sortable: true}, {Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: statusSeverities(), Sortable: true}, {Key: "paused", Label: "Paused", Type: plugin.ColumnBool}, {Key: "plan", Label: "Plan"}, {Key: "type", Label: "Type", Type: plugin.ColumnBadge}, {Key: "created_on", Label: "Created", Type: plugin.ColumnRelativeTime, Sortable: true}}
}

func dnsColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Name", Sortable: true}, {Key: "type", Label: "Type", Type: plugin.ColumnBadge, Sortable: true}, {Key: "content", Label: "Content"}, {Key: "proxied", Label: "Proxied", Type: plugin.ColumnBool}, {Key: "ttl", Label: "TTL", Type: plugin.ColumnNumber}, {Key: "modified_on", Label: "Modified", Type: plugin.ColumnRelativeTime, Sortable: true}}
}

func rulesetColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Name", Sortable: true}, {Key: "phase", Label: "Phase", Type: plugin.ColumnBadge, Sortable: true}, {Key: "kind", Label: "Kind", Type: plugin.ColumnBadge}, {Key: "version", Label: "Version", Type: plugin.ColumnNumber}, {Key: "last_updated", Label: "Updated", Type: plugin.ColumnRelativeTime, Sortable: true}}
}

func firewallColumns() []plugin.Column {
	return []plugin.Column{{Key: "description", Label: "Description", Sortable: true}, {Key: "action", Label: "Action", Type: plugin.ColumnBadge}, {Key: "paused", Label: "Paused", Type: plugin.ColumnBool}, {Key: "filter_expression", Label: "Expression"}, {Key: "modified_on", Label: "Modified", Type: plugin.ColumnRelativeTime}}
}

func pageRuleColumns() []plugin.Column {
	return []plugin.Column{{Key: "targets", Label: "Targets"}, {Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: statusSeverities()}, {Key: "priority", Label: "Priority", Type: plugin.ColumnNumber}, {Key: "modified_on", Label: "Modified", Type: plugin.ColumnRelativeTime}}
}

func certificateColumns() []plugin.Column {
	return []plugin.Column{{Key: "hosts", Label: "Hosts"}, {Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: statusSeverities()}, {Key: "expires_on", Label: "Expires", Type: plugin.ColumnRelativeTime, Sortable: true}, {Key: "issuer", Label: "Issuer"}}
}

func workerRouteColumns() []plugin.Column {
	return []plugin.Column{{Key: "pattern", Label: "Pattern", Sortable: true}, {Key: "script", Label: "Script"}, {Key: "enabled", Label: "Enabled", Type: plugin.ColumnBool}}
}

func tunnelColumns() []plugin.Column {
	return []plugin.Column{{Key: "name", Label: "Tunnel", Sortable: true}, {Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: statusSeverities(), Sortable: true}, {Key: "created_at", Label: "Created", Type: plugin.ColumnRelativeTime}, {Key: "connections", Label: "Connections", Type: plugin.ColumnNumber}}
}

func settingColumns() []plugin.Column {
	return []plugin.Column{{Key: "id", Label: "Setting", Sortable: true}, {Key: "value", Label: "Value"}, {Key: "editable", Label: "Editable", Type: plugin.ColumnBool}, {Key: "modified_on", Label: "Modified", Type: plugin.ColumnRelativeTime}}
}
