package cloudflare

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

//go:embed assets/*
var assets embed.FS

type row = plugin.TableRow

type actionResult struct {
	OK bool `json:"ok"`
}

func routes() []plugin.Route {
	return []plugin.Route{
		{ID: rid("asset"), Method: plugin.MethodGet, Path: "/asset", Permission: "cloudflare.read", Risk: plugin.RiskSafe, AuditEvent: rid("asset"), Handle: assetRoute},
		{ID: rid("summary"), Method: plugin.MethodGet, Path: "/summary", Permission: "cloudflare.read", Risk: plugin.RiskSafe, AuditEvent: rid("summary"), Handle: summary},
		{ID: rid("accounts.tree"), Method: plugin.MethodGet, Path: "/tree/accounts", Permission: "cloudflare.accounts.read", Risk: plugin.RiskSafe, AuditEvent: rid("accounts.tree"), Handle: accountsTree},
		{ID: rid("zones.tree"), Method: plugin.MethodGet, Path: "/tree/zones", Permission: "cloudflare.zones.read", Risk: plugin.RiskSafe, AuditEvent: rid("zones.tree"), Handle: zonesTree},
		{ID: rid("accounts.list"), Method: plugin.MethodGet, Path: "/accounts", Permission: "cloudflare.accounts.read", Risk: plugin.RiskSafe, AuditEvent: rid("accounts.list"), Handle: accountsList},
		{ID: rid("account.read"), Method: plugin.MethodGet, Path: "/accounts/{account}", Permission: "cloudflare.accounts.read", Risk: plugin.RiskSafe, AuditEvent: rid("account.read"), Handle: accountRead},
		{ID: rid("zones.list"), Method: plugin.MethodGet, Path: "/zones", Permission: "cloudflare.zones.read", Risk: plugin.RiskSafe, AuditEvent: rid("zones.list"), Handle: zonesList},
		{ID: rid("zone.read"), Method: plugin.MethodGet, Path: "/zones/{zone}", Permission: "cloudflare.zones.read", Risk: plugin.RiskSafe, AuditEvent: rid("zone.read"), Handle: zoneRead},
		{ID: rid("zone.pause"), Method: plugin.MethodPost, Path: "/zones/{zone}/pause", Permission: "cloudflare.zones.write", Risk: plugin.RiskWrite, AuditEvent: rid("zone.pause"), Handle: zonePause},
		{ID: rid("zone.unpause"), Method: plugin.MethodPost, Path: "/zones/{zone}/unpause", Permission: "cloudflare.zones.write", Risk: plugin.RiskWrite, AuditEvent: rid("zone.unpause"), Handle: zoneUnpause},
		{ID: rid("dns.list"), Method: plugin.MethodGet, Path: "/dns_records", Permission: "cloudflare.dns.read", Risk: plugin.RiskSafe, AuditEvent: rid("dns.list"), Handle: dnsList},
		{ID: rid("dns.read"), Method: plugin.MethodGet, Path: "/zones/{zone}/dns_records/{record}", Permission: "cloudflare.dns.read", Risk: plugin.RiskSafe, AuditEvent: rid("dns.read"), Handle: dnsRead},
		{ID: rid("dns.create"), Method: plugin.MethodPost, Path: "/zones/{zone}/dns_records", Permission: "cloudflare.dns.write", Risk: plugin.RiskWrite, AuditEvent: rid("dns.create"), Input: dnsRecordSchema(false), Handle: dnsCreate},
		{ID: rid("dns.update"), Method: plugin.MethodPut, Path: "/zones/{zone}/dns_records/{record}", Permission: "cloudflare.dns.write", Risk: plugin.RiskWrite, AuditEvent: rid("dns.update"), Input: dnsRecordSchema(true), Handle: dnsUpdate},
		{ID: rid("dns.delete"), Method: plugin.MethodDelete, Path: "/zones/{zone}/dns_records/{record}", Permission: "cloudflare.dns.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("dns.delete"), Handle: dnsDelete},
		{ID: rid("cache.purge"), Method: plugin.MethodPost, Path: "/zones/{zone}/purge_cache", Permission: "cloudflare.cache.purge", Risk: plugin.RiskDestructive, AuditEvent: rid("cache.purge"), Input: cachePurgeSchema(), Handle: cachePurge},
		{ID: rid("rulesets.list"), Method: plugin.MethodGet, Path: "/rulesets", Permission: "cloudflare.rulesets.read", Risk: plugin.RiskSafe, AuditEvent: rid("rulesets.list"), Handle: rulesetsList},
		{ID: rid("ruleset.read"), Method: plugin.MethodGet, Path: "/zones/{zone}/rulesets/{ruleset}", Permission: "cloudflare.rulesets.read", Risk: plugin.RiskSafe, AuditEvent: rid("ruleset.read"), Handle: rulesetRead},
		{ID: rid("waf.list"), Method: plugin.MethodGet, Path: "/waf/rulesets", Permission: "cloudflare.rulesets.read", Risk: plugin.RiskSafe, AuditEvent: rid("waf.list"), Handle: wafList},
		{ID: rid("firewall.rules.list"), Method: plugin.MethodGet, Path: "/firewall/rules", Permission: "cloudflare.firewall.read", Risk: plugin.RiskSafe, AuditEvent: rid("firewall.rules.list"), Handle: firewallRulesList},
		{ID: rid("page_rules.list"), Method: plugin.MethodGet, Path: "/pagerules", Permission: "cloudflare.page_rules.read", Risk: plugin.RiskSafe, AuditEvent: rid("page_rules.list"), Handle: pageRulesList},
		{ID: rid("page_rule.read"), Method: plugin.MethodGet, Path: "/zones/{zone}/pagerules/{rule}", Permission: "cloudflare.page_rules.read", Risk: plugin.RiskSafe, AuditEvent: rid("page_rule.read"), Handle: pageRuleRead},
		{ID: rid("page_rule.delete"), Method: plugin.MethodDelete, Path: "/zones/{zone}/pagerules/{rule}", Permission: "cloudflare.page_rules.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("page_rule.delete"), Handle: pageRuleDelete},
		{ID: rid("certificates.list"), Method: plugin.MethodGet, Path: "/custom_certificates", Permission: "cloudflare.certificates.read", Risk: plugin.RiskSafe, AuditEvent: rid("certificates.list"), Handle: certificatesList},
		{ID: rid("certificate.read"), Method: plugin.MethodGet, Path: "/zones/{zone}/custom_certificates/{cert}", Permission: "cloudflare.certificates.read", Risk: plugin.RiskSafe, AuditEvent: rid("certificate.read"), Handle: certificateRead},
		{ID: rid("workers.routes.list"), Method: plugin.MethodGet, Path: "/workers/routes", Permission: "cloudflare.workers.read", Risk: plugin.RiskSafe, AuditEvent: rid("workers.routes.list"), Handle: workersRoutesList},
		{ID: rid("worker.route.read"), Method: plugin.MethodGet, Path: "/zones/{zone}/workers/routes/{route}", Permission: "cloudflare.workers.read", Risk: plugin.RiskSafe, AuditEvent: rid("worker.route.read"), Handle: workerRouteRead},
		{ID: rid("zone.settings.list"), Method: plugin.MethodGet, Path: "/settings", Permission: "cloudflare.settings.read", Risk: plugin.RiskSafe, AuditEvent: rid("zone.settings.list"), Handle: zoneSettingsList},
		{ID: rid("zone.setting.read"), Method: plugin.MethodGet, Path: "/zones/{zone}/settings/{setting}", Permission: "cloudflare.settings.read", Risk: plugin.RiskSafe, AuditEvent: rid("zone.setting.read"), Handle: zoneSettingRead},
		{ID: rid("zone.setting.update"), Method: plugin.MethodPatch, Path: "/zones/{zone}/settings/{setting}", Permission: "cloudflare.settings.write", Risk: plugin.RiskWrite, AuditEvent: rid("zone.setting.update"), Input: settingUpdateSchema(), Handle: zoneSettingUpdate},
		{ID: rid("tunnels.list"), Method: plugin.MethodGet, Path: "/cfd_tunnel", Permission: "cloudflare.tunnels.read", Risk: plugin.RiskSafe, AuditEvent: rid("tunnels.list"), Handle: tunnelsList},
		{ID: rid("tunnel.read"), Method: plugin.MethodGet, Path: "/accounts/{account}/cfd_tunnel/{tunnel}", Permission: "cloudflare.tunnels.read", Risk: plugin.RiskSafe, AuditEvent: rid("tunnel.read"), Handle: tunnelRead},
		{ID: rid("tunnel.config"), Method: plugin.MethodGet, Path: "/accounts/{account}/cfd_tunnel/{tunnel}/configurations", Permission: "cloudflare.tunnels.read", Risk: plugin.RiskSafe, AuditEvent: rid("tunnel.config"), Handle: tunnelConfig},
		{ID: rid("api.execute"), Method: plugin.MethodPost, Path: "/api", Permission: "cloudflare.api.execute", Risk: plugin.RiskPrivileged, AuditEvent: rid("api.execute"), Input: apiExecuteSchema(), Handle: apiExecute},
	}
}

func assetRoute(rc *plugin.RequestContext) (any, error) {
	name := path.Clean(strings.TrimPrefix(param(rc, "path"), "/"))
	if name == "." || strings.Contains(name, "..") {
		return nil, plugin.ErrInvalidInput
	}
	data, err := assets.ReadFile("assets/" + name)
	if err != nil {
		return nil, fmt.Errorf("%w: asset not found", plugin.ErrNotFound)
	}
	mime := "application/octet-stream"
	if strings.HasSuffix(name, ".js") {
		mime = "text/javascript"
	}
	return &plugin.Download{Name: name, MIME: mime, Size: int64(len(data)), Inline: true, Body: io.NopCloser(bytes.NewReader(data)), ModTime: time.Now()}, nil
}

func accountsTree(rc *plugin.RequestContext) (any, error) {
	page, err := accountsList(rc)
	if err != nil {
		return nil, err
	}
	return treeFromPage(page.(plugin.Page[row]), "account", "building-2"), nil
}

func zonesTree(rc *plugin.RequestContext) (any, error) {
	page, err := zonesList(rc)
	if err != nil {
		return nil, err
	}
	return treeFromPage(page.(plugin.Page[row]), "zone", "globe"), nil
}

func accountsList(rc *plugin.RequestContext) (any, error) {
	return listRoute(rc, "/accounts", nil, func(r row) row {
		r["ref"] = ref("account", "", text(r, "name"), text(r, "id"))
		return r
	})
}

func accountRead(rc *plugin.RequestContext) (any, error) {
	return getRoute(rc, "/accounts/"+url.PathEscape(requiredParam(rc, "account")))
}

func zonesList(rc *plugin.RequestContext) (any, error) {
	s, err := cfSession(rc)
	if err != nil {
		return nil, err
	}
	q := url.Values{}
	if s.opts.AccountID != "" {
		q.Set("account.id", s.opts.AccountID)
	}
	if s.opts.ZoneFilter != "" {
		q.Set("name", s.opts.ZoneFilter)
	}
	return listRoute(rc, "/zones", q, func(r row) row {
		plan := nestedText(r, "plan", "name")
		if plan != "" {
			r["plan"] = plan
		}
		r["ref"] = ref("zone", "", text(r, "name"), text(r, "id"))
		return r
	})
}

func zoneRead(rc *plugin.RequestContext) (any, error) {
	return getRoute(rc, "/zones/"+url.PathEscape(requiredParam(rc, "zone")))
}

func zonePause(rc *plugin.RequestContext) (any, error) {
	return zoneSetPaused(rc, true)
}

func zoneUnpause(rc *plugin.RequestContext) (any, error) {
	return zoneSetPaused(rc, false)
}

func zoneSetPaused(rc *plugin.RequestContext, paused bool) (any, error) {
	if err := ensureWritable(rc); err != nil {
		return nil, err
	}
	var env cfEnvelope[row]
	if err := cfDo(rc, http.MethodPatch, "/zones/"+url.PathEscape(requiredParam(rc, "zone")), row{"paused": paused}, &env); err != nil {
		return nil, err
	}
	return envelopeOK(env)
}

func dnsList(rc *plugin.RequestContext) (any, error) {
	q := url.Values{}
	if typ := rc.Query().Get("type"); typ != "" {
		q.Set("type", typ)
	}
	if name := rc.Query().Get("name"); name != "" {
		q.Set("name", name)
	}
	zone := param(rc, "zone")
	if zone == "" {
		return aggregateZoneList(rc, func(zoneID string) string {
			return "/zones/" + url.PathEscape(zoneID) + "/dns_records"
		}, q, func(r row, z row) row {
			return mapDNSRecord(r, text(z, "id"), text(z, "name"))
		})
	}
	return listRoute(rc, "/zones/"+url.PathEscape(zone)+"/dns_records", q, func(r row) row {
		return mapDNSRecord(r, zone, "")
	})
}

func mapDNSRecord(r row, zone, zoneName string) row {
	if zoneName != "" {
		r["zone_name"] = zoneName
	}
	r["ref"] = plugin.ResourceIdentity{Kind: "dns_record", Namespace: zone, Name: text(r, "name"), UID: text(r, "id")}
	return r
}

func dnsRead(rc *plugin.RequestContext) (any, error) {
	return getRoute(rc, "/zones/"+url.PathEscape(requiredParam(rc, "zone"))+"/dns_records/"+url.PathEscape(requiredParam(rc, "record")))
}

func dnsCreate(rc *plugin.RequestContext) (any, error) {
	if err := ensureWritable(rc); err != nil {
		return nil, err
	}
	var body row
	if err := rc.Bind(&body); err != nil {
		return nil, err
	}
	var env cfEnvelope[row]
	if err := cfDo(rc, http.MethodPost, "/zones/"+url.PathEscape(requiredParam(rc, "zone"))+"/dns_records", body, &env); err != nil {
		return nil, err
	}
	return envelopeOK(env)
}

func dnsUpdate(rc *plugin.RequestContext) (any, error) {
	if err := ensureWritable(rc); err != nil {
		return nil, err
	}
	var body row
	if err := rc.Bind(&body); err != nil {
		return nil, err
	}
	var env cfEnvelope[row]
	if err := cfDo(rc, http.MethodPut, "/zones/"+url.PathEscape(requiredParam(rc, "zone"))+"/dns_records/"+url.PathEscape(requiredParam(rc, "record")), body, &env); err != nil {
		return nil, err
	}
	return envelopeOK(env)
}

func dnsDelete(rc *plugin.RequestContext) (any, error) {
	return deleteRoute(rc, "/zones/"+url.PathEscape(requiredParam(rc, "zone"))+"/dns_records/"+url.PathEscape(requiredParam(rc, "record")))
}

func cachePurge(rc *plugin.RequestContext) (any, error) {
	if err := ensureWritable(rc); err != nil {
		return nil, err
	}
	var in struct {
		Everything bool     `json:"everything"`
		Files      []string `json:"files"`
		Tags       []string `json:"tags"`
		Hosts      []string `json:"hosts"`
		Prefixes   []string `json:"prefixes"`
	}
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	body := row{}
	switch {
	case in.Everything:
		body["purge_everything"] = true
	case len(in.Files) > 0:
		body["files"] = in.Files
	case len(in.Tags) > 0:
		body["tags"] = in.Tags
	case len(in.Hosts) > 0:
		body["hosts"] = in.Hosts
	case len(in.Prefixes) > 0:
		body["prefixes"] = in.Prefixes
	default:
		return nil, fmt.Errorf("%w: choose everything, files, tags, hosts, or prefixes", plugin.ErrInvalidInput)
	}
	var env cfEnvelope[row]
	if err := cfDo(rc, http.MethodPost, "/zones/"+url.PathEscape(requiredParam(rc, "zone"))+"/purge_cache", body, &env); err != nil {
		return nil, err
	}
	return envelopeOK(env)
}

func rulesetsList(rc *plugin.RequestContext) (any, error) {
	zone := param(rc, "zone")
	if zone == "" {
		return aggregateZoneList(rc, func(zoneID string) string {
			return "/zones/" + url.PathEscape(zoneID) + "/rulesets"
		}, nil, func(r row, z row) row {
			return mapRuleset(r, text(z, "id"), text(z, "name"))
		})
	}
	return listRoute(rc, "/zones/"+url.PathEscape(zone)+"/rulesets", nil, func(r row) row {
		return mapRuleset(r, zone, "")
	})
}

func mapRuleset(r row, zone, zoneName string) row {
	if zoneName != "" {
		r["zone_name"] = zoneName
	}
	r["ref"] = plugin.ResourceIdentity{Kind: "ruleset", Namespace: zone, Name: text(r, "name"), UID: text(r, "id")}
	return flattenRuleset(r)
}

func rulesetRead(rc *plugin.RequestContext) (any, error) {
	return getRoute(rc, "/zones/"+url.PathEscape(requiredParam(rc, "zone"))+"/rulesets/"+url.PathEscape(requiredParam(rc, "ruleset")))
}

func wafList(rc *plugin.RequestContext) (any, error) {
	page, err := rulesetsList(rc)
	if err != nil {
		return nil, err
	}
	p := page.(plugin.Page[row])
	filtered := make([]row, 0, len(p.Items))
	for _, item := range p.Items {
		phase := text(item, "phase")
		if strings.Contains(phase, "waf") || strings.Contains(phase, "firewall") || strings.Contains(phase, "http_request") {
			filtered = append(filtered, item)
		}
	}
	total := len(filtered)
	return plugin.Page[row]{Items: filtered, Total: &total}, nil
}

func firewallRulesList(rc *plugin.RequestContext) (any, error) {
	if err := ensureLegacyEnabled(rc); err != nil {
		return nil, err
	}
	zone := param(rc, "zone")
	if zone == "" {
		return aggregateZoneList(rc, func(zoneID string) string {
			return "/zones/" + url.PathEscape(zoneID) + "/firewall/rules"
		}, nil, func(r row, z row) row {
			return mapFirewallRule(r, text(z, "name"))
		})
	}
	return listRoute(rc, "/zones/"+url.PathEscape(zone)+"/firewall/rules", nil, func(r row) row {
		return mapFirewallRule(r, "")
	})
}

func mapFirewallRule(r row, zoneName string) row {
	if zoneName != "" {
		r["zone_name"] = zoneName
	}
	if filter, ok := r["filter"].(map[string]any); ok {
		r["filter_expression"] = filter["expression"]
	}
	return r
}

func pageRulesList(rc *plugin.RequestContext) (any, error) {
	if err := ensureLegacyEnabled(rc); err != nil {
		return nil, err
	}
	zone := param(rc, "zone")
	if zone == "" {
		return aggregateZoneList(rc, func(zoneID string) string {
			return "/zones/" + url.PathEscape(zoneID) + "/pagerules"
		}, nil, func(r row, z row) row {
			return mapPageRule(r, text(z, "id"), text(z, "name"))
		})
	}
	return listRoute(rc, "/zones/"+url.PathEscape(zone)+"/pagerules", nil, func(r row) row {
		return mapPageRule(r, zone, "")
	})
}

func mapPageRule(r row, zone, zoneName string) row {
	if zoneName != "" {
		r["zone_name"] = zoneName
	}
	r["targets"] = compactJSON(r["targets"])
	r["ref"] = plugin.ResourceIdentity{Kind: "page_rule", Namespace: zone, Name: text(r, "targets"), UID: text(r, "id")}
	return r
}

func pageRuleRead(rc *plugin.RequestContext) (any, error) {
	if err := ensureLegacyEnabled(rc); err != nil {
		return nil, err
	}
	return getRoute(rc, "/zones/"+url.PathEscape(requiredParam(rc, "zone"))+"/pagerules/"+url.PathEscape(requiredParam(rc, "rule")))
}

func pageRuleDelete(rc *plugin.RequestContext) (any, error) {
	if err := ensureLegacyEnabled(rc); err != nil {
		return nil, err
	}
	return deleteRoute(rc, "/zones/"+url.PathEscape(requiredParam(rc, "zone"))+"/pagerules/"+url.PathEscape(requiredParam(rc, "rule")))
}

func certificatesList(rc *plugin.RequestContext) (any, error) {
	zone := param(rc, "zone")
	if zone == "" {
		return aggregateZoneList(rc, func(zoneID string) string {
			return "/zones/" + url.PathEscape(zoneID) + "/custom_certificates"
		}, nil, func(r row, z row) row {
			return mapCertificate(r, text(z, "id"), text(z, "name"))
		})
	}
	return listRoute(rc, "/zones/"+url.PathEscape(zone)+"/custom_certificates", nil, func(r row) row {
		return mapCertificate(r, zone, "")
	})
}

func mapCertificate(r row, zone, zoneName string) row {
	if zoneName != "" {
		r["zone_name"] = zoneName
	}
	r["hosts"] = compactJSON(r["hosts"])
	r["ref"] = plugin.ResourceIdentity{Kind: "certificate", Namespace: zone, Name: text(r, "hosts"), UID: text(r, "id")}
	return r
}

func certificateRead(rc *plugin.RequestContext) (any, error) {
	return getRoute(rc, "/zones/"+url.PathEscape(requiredParam(rc, "zone"))+"/custom_certificates/"+url.PathEscape(requiredParam(rc, "cert")))
}

func workersRoutesList(rc *plugin.RequestContext) (any, error) {
	zone := param(rc, "zone")
	if zone == "" {
		return aggregateZoneList(rc, func(zoneID string) string {
			return "/zones/" + url.PathEscape(zoneID) + "/workers/routes"
		}, nil, func(r row, z row) row {
			return mapWorkerRoute(r, text(z, "id"), text(z, "name"))
		})
	}
	return listRoute(rc, "/zones/"+url.PathEscape(zone)+"/workers/routes", nil, func(r row) row {
		return mapWorkerRoute(r, zone, "")
	})
}

func mapWorkerRoute(r row, zone, zoneName string) row {
	if zoneName != "" {
		r["zone_name"] = zoneName
	}
	r["script"] = nestedText(r, "script", "name")
	r["ref"] = plugin.ResourceIdentity{Kind: "worker_route", Namespace: zone, Name: text(r, "pattern"), UID: text(r, "id")}
	return r
}

func workerRouteRead(rc *plugin.RequestContext) (any, error) {
	return getRoute(rc, "/zones/"+url.PathEscape(requiredParam(rc, "zone"))+"/workers/routes/"+url.PathEscape(requiredParam(rc, "route")))
}

func zoneSettingsList(rc *plugin.RequestContext) (any, error) {
	zone := param(rc, "zone")
	if zone == "" {
		return aggregateZoneList(rc, func(zoneID string) string {
			return "/zones/" + url.PathEscape(zoneID) + "/settings"
		}, nil, func(r row, z row) row {
			return mapZoneSetting(r, text(z, "id"), text(z, "name"))
		})
	}
	return listRoute(rc, "/zones/"+url.PathEscape(zone)+"/settings", nil, func(r row) row {
		return mapZoneSetting(r, zone, "")
	})
}

func mapZoneSetting(r row, zone, zoneName string) row {
	if zoneName != "" {
		r["zone_name"] = zoneName
	}
	r["ref"] = plugin.ResourceIdentity{Kind: "setting", Namespace: zone, Name: text(r, "id"), UID: text(r, "id")}
	return r
}

func zoneSettingRead(rc *plugin.RequestContext) (any, error) {
	return getRoute(rc, "/zones/"+url.PathEscape(requiredParam(rc, "zone"))+"/settings/"+url.PathEscape(requiredParam(rc, "setting")))
}

func zoneSettingUpdate(rc *plugin.RequestContext) (any, error) {
	if err := ensureWritable(rc); err != nil {
		return nil, err
	}
	var in struct {
		Value any `json:"value"`
	}
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	var env cfEnvelope[row]
	if err := cfDo(rc, http.MethodPatch, "/zones/"+url.PathEscape(requiredParam(rc, "zone"))+"/settings/"+url.PathEscape(requiredParam(rc, "setting")), row{"value": in.Value}, &env); err != nil {
		return nil, err
	}
	return envelopeOK(env)
}

func tunnelsList(rc *plugin.RequestContext) (any, error) {
	account := accountID(rc)
	if account == "" {
		return aggregateAccountList(rc, func(accountID string) string {
			return "/accounts/" + url.PathEscape(accountID) + "/cfd_tunnel"
		}, nil, func(r row, a row) row {
			return mapTunnel(r, text(a, "id"), text(a, "name"))
		})
	}
	return listRoute(rc, "/accounts/"+url.PathEscape(account)+"/cfd_tunnel", nil, func(r row) row {
		return mapTunnel(r, account, "")
	})
}

func mapTunnel(r row, account, accountName string) row {
	if accountName != "" {
		r["account_name"] = accountName
	}
	connections := 0
	if items, ok := r["connections"].([]any); ok {
		connections = len(items)
	}
	r["connections"] = connections
	r["ref"] = plugin.ResourceIdentity{Kind: "tunnel", Namespace: account, Name: text(r, "name"), UID: text(r, "id")}
	return r
}

func tunnelRead(rc *plugin.RequestContext) (any, error) {
	return getRoute(rc, "/accounts/"+url.PathEscape(accountID(rc))+"/cfd_tunnel/"+url.PathEscape(requiredParam(rc, "tunnel")))
}

func tunnelConfig(rc *plugin.RequestContext) (any, error) {
	return getRoute(rc, "/accounts/"+url.PathEscape(accountID(rc))+"/cfd_tunnel/"+url.PathEscape(requiredParam(rc, "tunnel"))+"/configurations")
}

func summary(rc *plugin.RequestContext) (any, error) {
	zones, _ := zonesList(rc)
	accounts, _ := accountsList(rc)
	out := row{"generated_at": time.Now().UTC().Format(time.RFC3339)}
	if p, ok := zones.(plugin.Page[row]); ok {
		out["zones"] = len(p.Items)
		out["paused_zones"] = countBool(p.Items, "paused")
		out["zone_rows"] = p.Items
	}
	if p, ok := accounts.(plugin.Page[row]); ok {
		out["accounts"] = len(p.Items)
		out["account_rows"] = p.Items
	}
	return out, nil
}

func apiExecute(rc *plugin.RequestContext) (any, error) {
	var in struct {
		Method  string            `json:"method"`
		Path    string            `json:"url"`
		Headers map[string]string `json:"headers"`
		Body    any               `json:"body"`
	}
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	method := strings.ToUpper(strings.TrimSpace(in.Method))
	if method == "" {
		method = http.MethodGet
	}
	if method != http.MethodGet && method != http.MethodHead {
		if err := ensureWritable(rc); err != nil {
			return nil, err
		}
	}
	rawPath := "/" + strings.TrimLeft(strings.TrimSpace(in.Path), "/")
	var out any
	if err := cfDoWithHeaders(rc, method, rawPath, in.Headers, in.Body, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func listRoute(rc *plugin.RequestContext, apiPath string, q url.Values, mapRow func(row) row) (any, error) {
	s, err := cfSession(rc)
	if err != nil {
		return nil, err
	}
	page, err := rc.Page()
	if err != nil {
		return nil, err
	}
	if q == nil {
		q = url.Values{}
	}
	if page.Search() != "" {
		q.Set("name", page.Search())
	}
	if page.Cursor != "" {
		if _, err := strconv.Atoi(page.Cursor); err != nil {
			return nil, fmt.Errorf("%w: cursor must be a Cloudflare page number", plugin.ErrInvalidInput)
		}
		q.Set("page", page.Cursor)
	}
	items, info, err := fetchList(rc.Ctx, s, apiPath, q, boundedLimit(page.Limit, s.opts.PageLimit), mapRow)
	if err != nil {
		return nil, err
	}
	total := info.TotalCount
	if total == 0 {
		total = len(items)
	}
	next := ""
	if info.TotalPages > 0 && info.Page < info.TotalPages {
		next = strconv.Itoa(info.Page + 1)
	}
	return plugin.Page[row]{Items: items, NextCursor: next, Total: &total}, nil
}

func fetchList(ctx context.Context, s *Session, apiPath string, q url.Values, limit int, mapRow func(row) row) ([]row, cfResultInfo, error) {
	var env cfListEnvelope[row]
	info, err := s.client.list(ctx, apiPath, q, limit, &env)
	if err != nil {
		return nil, cfResultInfo{}, err
	}
	items, err := listOK(env)
	if err != nil {
		return nil, cfResultInfo{}, err
	}
	for i := range items {
		if mapRow != nil {
			items[i] = mapRow(items[i])
		}
	}
	return items, info, nil
}

func aggregateZoneList(rc *plugin.RequestContext, apiPath func(string) string, q url.Values, mapRow func(row, row) row) (plugin.Page[row], error) {
	zones, err := zonesForAggregation(rc)
	if err != nil {
		return plugin.Page[row]{}, err
	}
	return aggregateScopedList(rc, zones, apiPath, q, mapRow)
}

func aggregateAccountList(rc *plugin.RequestContext, apiPath func(string) string, q url.Values, mapRow func(row, row) row) (plugin.Page[row], error) {
	accounts, err := accountsForAggregation(rc)
	if err != nil {
		return plugin.Page[row]{}, err
	}
	return aggregateScopedList(rc, accounts, apiPath, q, mapRow)
}

func aggregateScopedList(rc *plugin.RequestContext, scopes []row, apiPath func(string) string, q url.Values, mapRow func(row, row) row) (plugin.Page[row], error) {
	s, err := cfSession(rc)
	if err != nil {
		return plugin.Page[row]{}, err
	}
	page, err := rc.Page()
	if err != nil {
		return plugin.Page[row]{}, err
	}
	if q == nil {
		q = url.Values{}
	}
	if page.Search() != "" {
		q.Set("name", page.Search())
	}
	limit := boundedLimit(page.Limit, s.opts.PageLimit)
	cursor, err := parseAggregateCursor(page.Cursor)
	if err != nil {
		return plugin.Page[row]{}, err
	}
	items := make([]row, 0, limit)
	for cursor.Scope < len(scopes) && len(items) < limit {
		scope := scopes[cursor.Scope]
		scopeID := text(scope, "id")
		if scopeID == "" {
			cursor = aggregateCursor{Scope: cursor.Scope + 1, Page: 1}
			continue
		}
		scopeQ := cloneValues(q)
		scopeQ.Set("page", strconv.Itoa(cursor.Page))
		listed, info, err := fetchList(rc.Ctx, s, apiPath(scopeID), scopeQ, limit, func(r row) row {
			if mapRow != nil {
				return mapRow(r, scope)
			}
			return r
		})
		if err != nil {
			return plugin.Page[row]{}, err
		}
		if cursor.Offset > len(listed) {
			cursor.Offset = len(listed)
		}
		listed = listed[cursor.Offset:]
		take := min(limit-len(items), len(listed))
		items = append(items, listed[:take]...)
		if take < len(listed) {
			cursor.Offset += take
			break
		}
		cursor.Offset = 0
		if info.TotalPages > 0 && info.Page < info.TotalPages {
			cursor.Page = info.Page + 1
			continue
		}
		cursor = aggregateCursor{Scope: cursor.Scope + 1, Page: 1}
	}
	next := ""
	if cursor.Scope < len(scopes) {
		next = cursor.String()
	}
	sortRows(items, page.Sort)
	// No Total: the fan-out never sees the whole collection, and len() of a
	// partial materialization is exactly the number that was wrong before.
	return plugin.Page[row]{Items: items, NextCursor: next}, nil
}

// aggregateCursor resumes an account-wide fan-out at the scope, upstream page,
// and row it stopped on. Without it, page 3 of an aggregated list re-issues one
// Cloudflare call per zone before slicing 50 rows out of the concatenation.
type aggregateCursor struct {
	Scope  int
	Page   int
	Offset int
}

func (c aggregateCursor) String() string {
	return strconv.Itoa(c.Scope) + ":" + strconv.Itoa(c.Page) + ":" + strconv.Itoa(c.Offset)
}

func parseAggregateCursor(raw string) (aggregateCursor, error) {
	if raw == "" {
		return aggregateCursor{Page: 1}, nil
	}
	parts := strings.Split(raw, ":")
	if len(parts) != 3 {
		return aggregateCursor{}, fmt.Errorf("%w: invalid cursor", plugin.ErrInvalidInput)
	}
	scope, scopeErr := strconv.Atoi(parts[0])
	number, pageErr := strconv.Atoi(parts[1])
	offset, offsetErr := strconv.Atoi(parts[2])
	if scopeErr != nil || pageErr != nil || offsetErr != nil || scope < 0 || number < 1 || offset < 0 {
		return aggregateCursor{}, fmt.Errorf("%w: invalid cursor", plugin.ErrInvalidInput)
	}
	return aggregateCursor{Scope: scope, Page: number, Offset: offset}, nil
}

func zonesForAggregation(rc *plugin.RequestContext) ([]row, error) {
	s, err := cfSession(rc)
	if err != nil {
		return nil, err
	}
	q := url.Values{}
	if s.opts.AccountID != "" {
		q.Set("account.id", s.opts.AccountID)
	}
	if s.opts.ZoneFilter != "" {
		q.Set("name", s.opts.ZoneFilter)
	}
	items, _, err := fetchList(rc.Ctx, s, "/zones", q, s.opts.PageLimit, func(r row) row {
		plan := nestedText(r, "plan", "name")
		if plan != "" {
			r["plan"] = plan
		}
		r["ref"] = ref("zone", "", text(r, "name"), text(r, "id"))
		return r
	})
	return items, err
}

func accountsForAggregation(rc *plugin.RequestContext) ([]row, error) {
	s, err := cfSession(rc)
	if err != nil {
		return nil, err
	}
	items, _, err := fetchList(rc.Ctx, s, "/accounts", nil, s.opts.PageLimit, func(r row) row {
		r["ref"] = ref("account", "", text(r, "name"), text(r, "id"))
		return r
	})
	return items, err
}

func boundedLimit(requested, configured int) int {
	if requested <= 0 {
		requested = defaultPageLimit
	}
	if configured > 0 && requested > configured {
		return configured
	}
	if requested > plugin.MaxPageLimit {
		return plugin.MaxPageLimit
	}
	return requested
}

func cloneValues(in url.Values) url.Values {
	out := url.Values{}
	for key, values := range in {
		for _, value := range values {
			out.Add(key, value)
		}
	}
	return out
}

func getRoute(rc *plugin.RequestContext, apiPath string) (any, error) {
	var env cfEnvelope[row]
	if err := cfDo(rc, http.MethodGet, apiPath, nil, &env); err != nil {
		return nil, err
	}
	return envelopeOK(env)
}

func deleteRoute(rc *plugin.RequestContext, apiPath string) (any, error) {
	if err := ensureWritable(rc); err != nil {
		return nil, err
	}
	var env cfEnvelope[row]
	if err := cfDo(rc, http.MethodDelete, apiPath, nil, &env); err != nil {
		return nil, err
	}
	_, err := envelopeOK(env)
	if err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func cfDo(rc *plugin.RequestContext, method, apiPath string, body any, out any) error {
	return cfDoWithHeaders(rc, method, apiPath, nil, body, out)
}

func cfDoWithHeaders(rc *plugin.RequestContext, method, apiPath string, headers map[string]string, body any, out any) error {
	s, err := cfSession(rc)
	if err != nil {
		return err
	}
	return s.client.doWithHeaders(rc.Ctx, method, apiPath, headers, body, out)
}

func ensureWritable(rc *plugin.RequestContext) error {
	s, err := cfSession(rc)
	if err != nil {
		return err
	}
	if s.opts.ReadOnly {
		return fmt.Errorf("%w: Cloudflare connection is in read-only mode", plugin.ErrForbidden)
	}
	return nil
}

func ensureLegacyEnabled(rc *plugin.RequestContext) error {
	s, err := cfSession(rc)
	if err != nil {
		return err
	}
	if !s.opts.IncludeLegacy {
		return fmt.Errorf("%w: legacy Cloudflare firewall and page-rule APIs are disabled for this connection", plugin.ErrForbidden)
	}
	return nil
}

func requiredParam(rc *plugin.RequestContext, key string) string {
	v := param(rc, key)
	if v == "" {
		return "_"
	}
	return v
}

func param(rc *plugin.RequestContext, key string) string {
	if v := strings.TrimSpace(rc.Param(key)); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p." + key))
}

func accountID(rc *plugin.RequestContext) string {
	if v := param(rc, "account"); v != "" {
		return v
	}
	s, err := cfSession(rc)
	if err != nil {
		return ""
	}
	return s.opts.AccountID
}

func ref(kind, namespace, name, uid string) plugin.ResourceIdentity {
	return plugin.ResourceIdentity{Kind: kind, Namespace: namespace, Name: name, UID: uid}
}

func treeFromPage(page plugin.Page[row], kind, iconName string) plugin.Page[plugin.TreeNode] {
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		id := text(item, "id")
		name := text(item, "name")
		nodes = append(nodes, plugin.TreeNode{Key: kind + ":" + id, Label: name, Icon: icon(iconName), Ref: &plugin.ResourceIdentity{Kind: kind, Name: name, UID: id}, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}
}

func sortRows(rows []row, sortKeys []plugin.SortKey) {
	if len(sortKeys) == 0 {
		return
	}
	key := sortKeys[0].Field
	desc := sortKeys[0].Desc
	sort.SliceStable(rows, func(i, j int) bool {
		a, b := fmt.Sprint(rows[i][key]), fmt.Sprint(rows[j][key])
		if desc {
			return a > b
		}
		return a < b
	})
}

func text(r row, key string) string {
	return strings.TrimSpace(fmt.Sprint(r[key]))
}

func nestedText(r row, outer, inner string) string {
	if m, ok := r[outer].(map[string]any); ok {
		return strings.TrimSpace(fmt.Sprint(m[inner]))
	}
	return ""
}

func compactJSON(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(data)
}

func flattenRuleset(r row) row {
	if t := text(r, "last_updated"); t != "" {
		r["last_updated"] = t
	}
	return r
}

func countBool(rows []row, key string) int {
	count := 0
	for _, r := range rows {
		if v, _ := r[key].(bool); v {
			count++
		}
	}
	return count
}

func intPtr(v int) *int { return &v }

func dnsRecordSchema(update bool) *plugin.Schema {
	fields := []plugin.Field{
		{Key: "type", Label: "Type", Type: plugin.FieldSelect, Required: !update, Default: "A", Options: []plugin.Option{{Label: "A", Value: "A"}, {Label: "AAAA", Value: "AAAA"}, {Label: "CNAME", Value: "CNAME"}, {Label: "TXT", Value: "TXT"}, {Label: "MX", Value: "MX"}, {Label: "SRV", Value: "SRV"}, {Label: "CAA", Value: "CAA"}, {Label: "NS", Value: "NS"}}},
		{Key: "name", Label: "Name", Type: plugin.FieldText, Required: !update, Placeholder: "www"},
		{Key: "content", Label: "Content", Type: plugin.FieldText, Required: !update},
		{Key: "ttl", Label: "TTL", Type: plugin.FieldNumber, Default: 1, Help: "Use 1 for automatic TTL."},
		{Key: "proxied", Label: "Proxied", Type: plugin.FieldToggle, Default: false},
		{Key: "comment", Label: "Comment", Type: plugin.FieldTextarea},
	}
	if update {
		fields[0].Default = "${record.type}"
		fields[1].Default = "${record.name}"
		fields[2].Default = "${record.content}"
		fields[3].Default = "${record.ttl}"
		fields[4].Default = "${record.proxied}"
		fields[5].Default = "${record.comment}"
	}
	return &plugin.Schema{Groups: []plugin.Group{{Name: "DNS record", Fields: fields}}}
}

func cachePurgeSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Purge", Fields: []plugin.Field{
		{Key: "everything", Label: "Purge everything", Type: plugin.FieldToggle, Default: false},
		{Key: "files", Label: "Files", Type: plugin.FieldArray, ItemLabel: "URL", AddLabel: "Add URL", Item: &plugin.Field{Key: "url", Label: "URL", Type: plugin.FieldURL}},
		{Key: "tags", Label: "Cache tags", Type: plugin.FieldArray, ItemLabel: "Tag", AddLabel: "Add tag", Item: &plugin.Field{Key: "tag", Label: "Tag", Type: plugin.FieldText}},
		{Key: "hosts", Label: "Hosts", Type: plugin.FieldArray, ItemLabel: "Host", AddLabel: "Add host", Item: &plugin.Field{Key: "host", Label: "Host", Type: plugin.FieldText}},
		{Key: "prefixes", Label: "Prefixes", Type: plugin.FieldArray, ItemLabel: "Prefix", AddLabel: "Add prefix", Item: &plugin.Field{Key: "prefix", Label: "Prefix", Type: plugin.FieldURL}},
	}}}}
}

func settingUpdateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Setting", Fields: []plugin.Field{{Key: "value", Label: "Value", Type: plugin.FieldJSON, Required: true}}}}}
}

func apiExecuteSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Request", Fields: []plugin.Field{
		{Key: "method", Label: "Method", Type: plugin.FieldSelect, Required: true, Default: "GET", Options: []plugin.Option{{Label: "GET", Value: "GET"}, {Label: "POST", Value: "POST"}, {Label: "PUT", Value: "PUT"}, {Label: "PATCH", Value: "PATCH"}, {Label: "DELETE", Value: "DELETE"}}},
		{Key: "url", Label: "Path", Type: plugin.FieldText, Required: true, Default: "/zones"},
		{Key: "headers", Label: "Headers", Type: plugin.FieldMap, KeyLabel: "Header", KeyPlaceholder: "If-Match", Item: &plugin.Field{Key: "value", Label: "Value", Type: plugin.FieldText}},
		{Key: "body", Label: "Body", Type: plugin.FieldJSON},
	}}}}
}
