package consul

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/hashicorp/consul/api"

	"github.com/charlesng35/shellcn-contrib/shared/plugintest"
	"github.com/charlesng35/shellcn/sdk/plugin"
	sdktest "github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestManifestValidates(t *testing.T) {
	p := New()
	sdktest.ValidatePlugin(t, p)
	sdktest.ValidateProjectionPanelConfigs(t, sdktest.Projection(t, p))

	m := p.Manifest()
	if m.Layout != plugin.LayoutSidebarTree {
		t.Fatalf("consul should navigate through a sidebar tree, got %q", m.Layout)
	}
	if m.Agent == nil || len(m.SupportedTransports) != 2 {
		t.Fatalf("consul should offer direct and agent transports: %+v", m.SupportedTransports)
	}
	for _, kind := range []plugin.CredentialKind{plugin.CredentialKindAPIToken, plugin.CredentialKindTLSClientCert} {
		if !sdktest.CredentialKindSupported(m.Config, kind) {
			t.Fatalf("consul should accept stored %s credentials", kind)
		}
	}
	if len(m.Scope) != 1 || m.Scope[0].Param != "datacenter" {
		t.Fatalf("consul should scope every request by datacenter: %+v", m.Scope)
	}
}

func TestKVPanelDeclaresHierarchy(t *testing.T) {
	for _, resource := range New().Manifest().Resources {
		if resource.Kind != kindKV {
			continue
		}
		for _, tab := range resource.Detail.Tabs {
			if tab.Type != plugin.PanelKV {
				continue
			}
			cfg, ok := tab.Config.(plugin.KVConfig)
			if !ok {
				t.Fatalf("kv tab config is not a KVConfig: %T", tab.Config)
			}
			if cfg.Delimiter != "/" || cfg.KeyParam != "key" || !cfg.Writable {
				t.Fatalf("unexpected kv panel config: %+v", cfg)
			}
			return
		}
	}
	t.Fatal("kv resource has no PanelKV tab")
}

func TestRouteContract(t *testing.T) {
	routeMap := sdktest.RouteMap(routes())
	for id, route := range routeMap {
		if route.AuditEvent != id {
			t.Errorf("route %s: audit event %q must equal the route id", id, route.AuditEvent)
		}
		if !strings.HasPrefix(route.Permission, protocolName+".") {
			t.Errorf("route %s: permission %q must be namespaced", id, route.Permission)
		}
		if !strings.HasPrefix(id, protocolName+".") {
			t.Errorf("route %s: id must be namespaced", id)
		}
	}
	destructive := []string{
		rid("kv.delete"), rid("kv.tree.delete"), rid("instance.deregister"),
		rid("session.destroy"), rid("intention.delete"),
	}
	for _, id := range destructive {
		if routeMap[id].Risk != plugin.RiskDestructive {
			t.Errorf("route %s must be destructive, got %q", id, routeMap[id].Risk)
		}
	}
	for _, id := range []string{rid("kv.write"), rid("kv.put"), rid("intention.toggle")} {
		if routeMap[id].Risk != plugin.RiskWrite {
			t.Errorf("route %s must be a write, got %q", id, routeMap[id].Risk)
		}
	}
	if routeMap[rid("kv.watch")].Stream == nil {
		t.Error("kv.watch must declare a stream handler")
	}
}

func TestParseOptionsDefaults(t *testing.T) {
	opts, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{"host": "consul.internal"}})
	if err != nil {
		t.Fatalf("parse options: %v", err)
	}
	if opts.Port != defaultPort || opts.Token != "" || opts.TLSMode != tlsDisable {
		t.Fatalf("unexpected defaults: %+v", opts)
	}
	if !opts.ReadOnly {
		t.Fatal("read-only should default on")
	}
	if opts.PageLimit != defaultPageLimit || opts.Timeout != defaultTimeout {
		t.Fatalf("unexpected paging defaults: %+v", opts)
	}
	if opts.scheme() != "http" {
		t.Fatalf("plain connections should use http, got %q", opts.scheme())
	}
}

func TestParseOptionsUsesStoredToken(t *testing.T) {
	opts, err := parseOptions(plugin.ConnectConfig{
		Config: map[string]any{
			"host": "consul.internal", "auth": authCredential, "tls_mode": "verify-full",
			"path_prefix": "consul", "kv_prefix": "/apps/", "read_only": false,
		},
		Credentials: plugin.NewResolvedCredentials(
			plugin.CredentialBinding{
				Field:      credentialIDField,
				Credential: plugin.ResolvedCredential{Values: map[string]string{"token": "s3cr3t"}},
			},
			plugin.CredentialBinding{
				Field:      clientCertField,
				Credential: plugin.ResolvedCredential{Values: map[string]string{"certificate": "cert-pem", "private_key": "key-pem"}},
			},
		),
	})
	if err != nil {
		t.Fatalf("parse options: %v", err)
	}
	if opts.Token != "s3cr3t" {
		t.Fatalf("stored token was not applied: %+v", opts)
	}
	if opts.ClientCertificate != "cert-pem\nkey-pem" {
		t.Fatalf("client certificate was not assembled: %q", opts.ClientCertificate)
	}
	if opts.PathPrefix != "/consul" || opts.KVPrefix != "apps/" || opts.scheme() != "https" {
		t.Fatalf("unexpected normalisation: %+v", opts)
	}
}

func TestParseOptionsRejectsBadInput(t *testing.T) {
	cases := []map[string]any{
		{},
		{"host": "consul", "port": 70000},
		{"host": "consul", "auth": "kerberos"},
		{"host": "consul", "auth": authCredential},
		{"host": "consul", "tls_mode": "maybe"},
	}
	for _, config := range cases {
		if _, err := parseOptions(plugin.ConnectConfig{Config: config}); err == nil {
			t.Fatalf("expected %v to be rejected", config)
		}
	}
}

func TestConfigSchemaVisibility(t *testing.T) {
	schema := configSchema()
	values := map[string]any{}
	for _, group := range schema.Groups {
		for _, field := range group.Fields {
			values[field.Key] = "set"
		}
	}
	values["auth"] = authToken
	values[credentialIDField] = ""
	visible := schema.VisibleValues(values, nil)
	if _, ok := visible["token"]; !ok {
		t.Fatal("inline token should be visible for token auth")
	}
	if _, ok := visible[credentialIDField]; ok {
		t.Fatal("stored token should be hidden for inline token auth")
	}

	values["auth"] = authCredential
	values[credentialIDField] = "cred-1"
	visible = schema.VisibleValues(values, nil)
	if _, ok := visible[credentialIDField]; !ok {
		t.Fatal("stored token should be visible for credential auth")
	}
	if _, ok := visible["token"]; ok {
		t.Fatal("inline token should be hidden once a credential is selected")
	}

	values["tls_mode"] = "verify-ca"
	secrets := schema.VisibleSecretKeys(values, nil)
	if !slices.Contains(secrets, "ca_certificate") {
		t.Fatalf("CA certificate should be a visible secret: %v", secrets)
	}
	if slices.Contains(secrets, "token") {
		t.Fatalf("inline token must not be a visible secret under credential auth: %v", secrets)
	}
}

func TestClusterAndAgentRoutes(t *testing.T) {
	sess, _ := newTestSession(t, newFakeConsul())
	page := call(t, sess, rid("cluster.list"), nil, nil, nil).(plugin.Page[row])
	if len(page.Items) != 1 {
		t.Fatalf("expected one cluster row: %#v", page.Items)
	}
	item := page.Items[0]
	if item["status"] != "ready" || item["leader"] != "10.0.0.1:8300" {
		t.Fatalf("unexpected cluster row: %#v", item)
	}
	if item["nodes"] != 2 || item["services"] != 2 {
		t.Fatalf("cluster row should count catalog contents: %#v", item)
	}
	if _, ok := item["ref"].(plugin.ResourceIdentity); !ok {
		t.Fatalf("cluster row needs a resource ref: %#v", item["ref"])
	}

	agent := call(t, sess, rid("agent.self"), nil, nil, nil).(row)
	if agent["node_name"] != "consul-server-1" || agent["version"] != "1.20.1" || agent["server"] != true {
		t.Fatalf("unexpected agent detail: %#v", agent)
	}
	if agent["read_only"] != false {
		t.Fatalf("agent detail should report the connection safety flag: %#v", agent)
	}
	if addresses, ok := agent["tagged_addresses"].(map[string]any); !ok || addresses["lan"] != "10.0.0.1" {
		t.Fatalf("agent detail should carry the tagged addresses: %#v", agent["tagged_addresses"])
	}
	if tags, ok := agent["gossip_tags"].(map[string]any); !ok || tags["role"] != "consul" {
		t.Fatalf("agent detail should carry the gossip tags: %#v", agent["gossip_tags"])
	}

	members := call(t, sess, rid("members.list"), nil, nil, nil).(plugin.Page[row])
	if len(members.Items) != 2 || members.Items[0]["status"] != "alive" {
		t.Fatalf("unexpected members: %#v", members.Items)
	}
	raft := call(t, sess, rid("raft.list"), nil, nil, nil).(plugin.Page[row])
	if len(raft.Items) != 1 || raft.Items[0]["leader"] != true {
		t.Fatalf("unexpected raft peers: %#v", raft.Items)
	}
	datacenters := call(t, sess, rid("datacenters.list"), nil, nil, nil).(plugin.Page[row])
	if len(datacenters.Items) != 2 || datacenters.Items[0]["value"] != "dc1" {
		t.Fatalf("unexpected datacenters: %#v", datacenters.Items)
	}
}

func TestDatacenterScopeReachesEveryQuery(t *testing.T) {
	fake := newFakeConsul()
	sess, _ := newTestSession(t, fake)
	call(t, sess, rid("nodes.list"), map[string]string{"datacenter": "dc2"}, nil, nil)
	if got := fake.lastQuery("/v1/catalog/nodes").Get("dc"); got != "dc2" {
		t.Fatalf("the datacenter scope must reach the catalog query, got %q", got)
	}
	call(t, sess, rid("kv.list"), map[string]string{"datacenter": "dc2"},
		url.Values{"cursor": []string{"50"}}, nil)
	if got := fake.lastQuery("/v1/kv/").Get("dc"); got != "dc2" {
		t.Fatalf("the datacenter scope must survive pagination, got %q", got)
	}
}

func TestKVBrowsing(t *testing.T) {
	fake := newFakeConsul()
	fake.setKV("apps/web/config", []byte(`{"replicas":3}`))
	fake.setKV("apps/web/secret", []byte("plain"))
	fake.setKV("apps/db/dsn", []byte("postgres://"))
	fake.setKV("apps/version", []byte("3"))
	sess, _ := newTestSession(t, fake)

	tree := call(t, sess, rid("kv.tree"), nil, nil, nil).(plugin.Page[plugin.TreeNode])
	if len(tree.Items) != 2 {
		t.Fatalf("root should expose itself plus the apps/ folder: %#v", tree.Items)
	}
	if tree.Items[0].Ref == nil || tree.Items[0].Ref.UID != kvRootToken {
		t.Fatalf("the first tree node should address the KV root: %#v", tree.Items[0])
	}
	folder := tree.Items[1]
	if folder.Label != "apps" || folder.ChildrenSource == nil || folder.Ref.UID != "apps/" {
		t.Fatalf("unexpected folder node: %#v", folder)
	}

	nested := call(t, sess, rid("kv.tree"), map[string]string{"prefix": "apps/"}, nil, nil).(plugin.Page[plugin.TreeNode])
	labels := make([]string, 0, len(nested.Items))
	for _, node := range nested.Items {
		labels = append(labels, node.Label)
	}
	if !slices.Equal(labels, []string{"db", "web"}) {
		t.Fatalf("nested folders should be listed alphabetically: %v", labels)
	}

	entries := call(t, sess, rid("kv.entries"), map[string]string{"prefix": "apps/web/"}, nil, nil).(plugin.Page[row])
	if len(entries.Items) != 2 {
		t.Fatalf("expected two entries: %#v", entries.Items)
	}
	// The entry grid stays inside one folder level; the key browser sees the subtree.
	level := call(t, sess, rid("kv.entries"), map[string]string{"prefix": "apps/"}, nil, nil).(plugin.Page[row])
	if len(level.Items) != 1 || level.Items[0]["key"] != "apps/version" {
		t.Fatalf("the entry grid must list one folder level: %#v", level.Items)
	}
	subtree := call(t, sess, rid("kv.list"), map[string]string{"prefix": "apps/"}, nil, nil).(plugin.Page[row])
	if len(subtree.Items) != 4 {
		t.Fatalf("the key browser must see the whole subtree: %#v", subtree.Items)
	}
	if entries.Items[0]["key"] != "apps/web/config" || entries.Items[0]["size"] != len(`{"replicas":3}`) {
		t.Fatalf("entry metadata was not resolved: %#v", entries.Items[0])
	}

	detail := call(t, sess, rid("kv.read"), map[string]string{"key": "apps/web/config"}, nil, nil).(row)
	if detail["type"] != "json" || detail["value"] != `{"replicas":3}` {
		t.Fatalf("unexpected key detail: %#v", detail)
	}
	plain := call(t, sess, rid("kv.read"), map[string]string{"key": "apps/web/secret"}, nil, nil).(row)
	if plain["type"] != "string" {
		t.Fatalf("plain values should read back as strings: %#v", plain)
	}
}

func TestKVPagingSpansTheWholeKeyspace(t *testing.T) {
	fake := newFakeConsul()
	for i := range 250 {
		fake.setKV(fmt.Sprintf("apps/web/key-%03d", i), []byte(strconv.Itoa(i)))
	}
	sess, _ := newTestSession(t, fake)
	params := map[string]string{"prefix": "apps/web/"}

	first := call(t, sess, rid("kv.list"), params, url.Values{"limit": []string{"40"}}, nil).(plugin.Page[row])
	if len(first.Items) != 40 {
		t.Fatalf("expected a bounded first page, got %d", len(first.Items))
	}
	if first.Total == nil || *first.Total != 250 {
		t.Fatalf("the key count is authoritative and should be reported: %#v", first.Total)
	}
	if first.NextCursor == "" {
		t.Fatal("a truncated listing must expose a continue cursor")
	}
	if first.Items[0]["key"] != "apps/web/key-000" {
		t.Fatalf("unexpected first key: %#v", first.Items[0])
	}

	seen := map[string]bool{}
	cursor := ""
	pages := 0
	for {
		query := url.Values{"limit": []string{"40"}}
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		page := call(t, sess, rid("kv.list"), params, query, nil).(plugin.Page[row])
		pages++
		if len(page.Items) == 0 {
			t.Fatal("pagination produced a phantom empty page")
		}
		for _, item := range page.Items {
			key := item["key"].(string)
			if seen[key] {
				t.Fatalf("key %q was returned twice", key)
			}
			seen[key] = true
		}
		cursor = page.NextCursor
		if cursor == "" {
			break
		}
		if pages > 20 {
			t.Fatal("pagination never terminated")
		}
	}
	if len(seen) != 250 {
		t.Fatalf("paging must reach every key, saw %d", len(seen))
	}

	desc := call(t, sess, rid("kv.list"), params,
		url.Values{"limit": []string{"5"}, "sort": []string{"-key"}}, nil).(plugin.Page[row])
	if desc.Items[0]["key"] != "apps/web/key-249" {
		t.Fatalf("descending sort must span the dataset, got %#v", desc.Items[0])
	}

	filtered := call(t, sess, rid("kv.list"), params,
		url.Values{"limit": []string{"10"}, "filter": []string{"key-24"}}, nil).(plugin.Page[row])
	if filtered.Total == nil || *filtered.Total != 10 {
		t.Fatalf("search must span the dataset, got %#v", filtered.Total)
	}
	if filtered.NextCursor != "" {
		t.Fatalf("a complete filtered page must not advertise more rows: %q", filtered.NextCursor)
	}
}

func TestPageLimitBoundsEveryPage(t *testing.T) {
	fake := newFakeConsul()
	for i := range 25 {
		fake.setKV(fmt.Sprintf("apps/web/key-%02d", i), []byte(strconv.Itoa(i)))
	}
	sess := connectFake(t, fake, map[string]any{"page_limit": 10})
	params := map[string]string{"prefix": "apps/web/"}

	page := call(t, sess, rid("kv.entries"), params, url.Values{"limit": []string{"500"}}, nil).(plugin.Page[row])
	if len(page.Items) != 10 {
		t.Fatalf("the connection page limit must cap the request limit, got %d", len(page.Items))
	}
	if page.Total == nil || *page.Total != 25 {
		t.Fatalf("the total must still describe the whole key set: %#v", page.Total)
	}
	if page.NextCursor != "10" {
		t.Fatalf("the capped page must advertise a reachable cursor, got %q", page.NextCursor)
	}
	for _, item := range page.Items {
		if item["size"] == nil {
			t.Fatalf("every row on the page must carry its metadata: %#v", item)
		}
	}

	seen := map[string]bool{}
	cursor := ""
	for pages := 0; pages < 10; pages++ {
		query := url.Values{"limit": []string{"500"}}
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		next := call(t, sess, rid("kv.entries"), params, query, nil).(plugin.Page[row])
		if len(next.Items) == 0 {
			t.Fatal("the capped cursor produced a phantom empty page")
		}
		for _, item := range next.Items {
			seen[item["key"].(string)] = true
		}
		if cursor = next.NextCursor; cursor == "" {
			break
		}
	}
	if len(seen) != 25 {
		t.Fatalf("the capped cursor must still reach every key, saw %d", len(seen))
	}
}

func TestAgentConfigListsSettings(t *testing.T) {
	sess, _ := newTestSession(t, newFakeConsul())
	page := call(t, sess, rid("agent.config"), nil, nil, nil).(plugin.Page[row])
	settings := map[string]string{}
	for _, item := range page.Items {
		settings[item["setting"].(string)] = item["value"].(string)
	}
	if settings["BindAddr"] != "0.0.0.0" || settings["ACLsEnabled"] != "false" || settings["RaftProtocol"] != "3" {
		t.Fatalf("scalar settings should render as text: %#v", settings)
	}
	if settings["TaggedAddresses"] != `{"lan":"10.0.0.1"}` {
		t.Fatalf("structured settings should render as JSON: %#v", settings)
	}
	if page.Items[0]["setting"] != "ACLsEnabled" {
		t.Fatalf("settings should be listed alphabetically: %#v", page.Items[0])
	}
}

func TestKVBinaryValuesRoundTrip(t *testing.T) {
	fake := newFakeConsul()
	sess, _ := newTestSession(t, fake)

	call(t, sess, rid("kv.write"), map[string]string{"key": "apps/blob"}, nil,
		mustJSON(t, map[string]any{"type": "binary", "value": "//4="}))
	if got := fake.value("apps/blob"); len(got) != 2 || got[0] != 0xff {
		t.Fatalf("binary values must be stored decoded, got %#v", got)
	}
	detail := call(t, sess, rid("kv.read"), map[string]string{"key": "apps/blob"}, nil, nil).(row)
	if detail["type"] != "binary" || detail["value"] != "//4=" {
		t.Fatalf("binary values must read back base64-encoded: %#v", detail)
	}
	for _, resource := range New().Manifest().Resources {
		if resource.Kind != kindKV {
			continue
		}
		for _, tab := range resource.Detail.Tabs {
			cfg, ok := tab.Config.(plugin.KVConfig)
			if ok && !slices.Contains(cfg.ValueTypes, "binary") {
				t.Fatalf("the kv panel must offer the binary value type: %v", cfg.ValueTypes)
			}
		}
	}
}

func TestIntentionsDegradeWithoutConnect(t *testing.T) {
	fake := newFakeConsul()
	fake.connectDisabled = true
	sess, _ := newTestSession(t, fake)
	page := call(t, sess, rid("intentions.list"), nil, nil, nil).(plugin.Page[row])
	if len(page.Items) != 0 || page.Total == nil || *page.Total != 0 {
		t.Fatalf("a cluster without the service mesh should degrade to an empty page: %#v", page)
	}
}

func TestKVWriteReadDelete(t *testing.T) {
	fake := newFakeConsul()
	sess, _ := newTestSession(t, fake)

	call(t, sess, rid("kv.put"), nil, nil, mustJSON(t, map[string]any{"key": "apps/web/new", "value": "hello"}))
	if got := string(fake.value("apps/web/new")); got != "hello" {
		t.Fatalf("kv.put did not store the value, got %q", got)
	}

	call(t, sess, rid("kv.write"), map[string]string{"key": "apps/web/new"}, nil,
		mustJSON(t, map[string]any{"type": "json", "value": map[string]any{"a": 1}}))
	if got := string(fake.value("apps/web/new")); got != `{"a":1}` {
		t.Fatalf("structured values should be encoded as JSON, got %q", got)
	}

	index := fake.modifyIndex("apps/web/new")
	call(t, sess, rid("kv.write"), map[string]string{"key": "apps/web/new"}, nil,
		mustJSON(t, map[string]any{"value": "cas-ok", "cas": index}))
	if got := string(fake.value("apps/web/new")); got != "cas-ok" {
		t.Fatalf("check-and-set write did not apply, got %q", got)
	}

	route := sdktest.RouteMap(routes())[rid("kv.write")]
	_, err := route.Handle(plugin.NewRequestContext(context.Background(), plugin.User{}, sess,
		map[string]string{"key": "apps/web/new"}, nil, mustJSON(t, map[string]any{"value": "stale", "cas": index})))
	if err == nil || !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("a stale check-and-set write must conflict, got %v", err)
	}

	call(t, sess, rid("kv.delete"), map[string]string{"key": "apps/web/new"}, nil, nil)
	if fake.has("apps/web/new") {
		t.Fatal("kv.delete did not remove the key")
	}

	fake.setKV("apps/tmp/a", []byte("1"))
	fake.setKV("apps/tmp/b", []byte("2"))
	call(t, sess, rid("kv.tree.delete"), map[string]string{"prefix": "apps/tmp/"}, nil, nil)
	if fake.has("apps/tmp/a") || fake.has("apps/tmp/b") {
		t.Fatal("kv.tree.delete did not remove the subtree")
	}
}

func TestKVRespectsRootAndReadOnly(t *testing.T) {
	fake := newFakeConsul()
	fake.setKV("apps/web/config", []byte("x"))
	fake.setKV("other/secret", []byte("x"))
	sess := connectFake(t, fake, map[string]any{"kv_prefix": "apps/", "read_only": false})

	page := call(t, sess, rid("kv.list"), nil, nil, nil).(plugin.Page[row])
	if len(page.Items) != 1 || page.Items[0]["key"] != "apps/web/config" {
		t.Fatalf("the KV root must scope the listing: %#v", page.Items)
	}
	if _, err := callErr(sess, rid("kv.read"), map[string]string{"key": "other/secret"}, nil, nil); err == nil {
		t.Fatal("keys outside the configured root must be refused")
	}
	if _, err := callErr(sess, rid("kv.tree.delete"), map[string]string{"prefix": "other/"}, nil, nil); err == nil {
		t.Fatal("prefixes outside the configured root must be refused")
	}

	readOnly := connectFake(t, fake, map[string]any{"read_only": true})
	for _, id := range []string{
		rid("kv.put"), rid("kv.write"), rid("kv.delete"), rid("kv.tree.delete"),
		rid("instance.deregister"), rid("session.destroy"),
		rid("intention.toggle"), rid("intention.delete"),
	} {
		if _, err := callErr(readOnly, id, map[string]string{
			"key": "apps/web/config", "prefix": "apps/", "service": "web", "node": "node-1",
			"instance": "web-1", "session": "s1", "source": "api", "destination": "web",
		}, nil, nil); err == nil {
			t.Fatalf("%s must be blocked in read-only mode", id)
		}
	}
}

func TestCheckTableOpensOnCriticalChecks(t *testing.T) {
	cfg := checkTableConfig()
	if cfg.DefaultSort == nil || cfg.DefaultSort.Field != "status" || cfg.DefaultSort.Desc {
		t.Fatalf("health checks should default to an ascending status sort: %#v", cfg.DefaultSort)
	}
	rows := []row{
		{"status": api.HealthPassing}, {"status": api.HealthMaint},
		{"status": api.HealthWarning}, {"status": api.HealthCritical},
	}
	plugin.SortRows(rows, []plugin.SortKey{*cfg.DefaultSort})
	if rows[0]["status"] != api.HealthCritical {
		t.Fatalf("the default sort must surface critical checks first: %#v", rows)
	}
}

func TestKVDeleteTreeRefusesWholeKeyspace(t *testing.T) {
	sess, _ := newTestSession(t, newFakeConsul())
	if _, err := callErr(sess, rid("kv.tree.delete"), nil, nil, nil); err == nil {
		t.Fatal("deleting the whole key space without a prefix must be refused")
	}
}

func TestCatalogRoutes(t *testing.T) {
	sess, _ := newTestSession(t, newFakeConsul())

	services := call(t, sess, rid("services.list"), nil, nil, nil).(plugin.Page[row])
	if len(services.Items) != 2 {
		t.Fatalf("unexpected services: %#v", services.Items)
	}
	web := services.Items[1]
	if web["name"] != "web" || web["status"] != "warning" || web["passing"] != 1 || web["warning"] != 1 {
		t.Fatalf("service health must aggregate every check: %#v", web)
	}
	tagged := call(t, sess, rid("services.list"), map[string]string{"tag": "primary"}, nil, nil).(plugin.Page[row])
	if len(tagged.Items) != 1 || tagged.Items[0]["name"] != "db" {
		t.Fatalf("tag scoping failed: %#v", tagged.Items)
	}

	detail := call(t, sess, rid("service.read"), map[string]string{"service": "web"}, nil, nil).(row)
	if detail["instances"] != 2 || detail["nodes"] != 2 || detail["status"] != "warning" {
		t.Fatalf("unexpected service detail: %#v", detail)
	}
	if detail["ports"] != "8080" || !strings.Contains(detail["tags"].(string), "v1") {
		t.Fatalf("service detail should summarise registration: %#v", detail)
	}

	instances := call(t, sess, rid("service.instances"), map[string]string{"service": "web"}, nil, nil).(plugin.Page[row])
	if len(instances.Items) != 2 {
		t.Fatalf("unexpected instances: %#v", instances.Items)
	}
	ref := instances.Items[0]["ref"].(plugin.ResourceIdentity)
	if ref.Kind != kindInstance || ref.Scope != "web" || ref.Namespace != "node-1" || ref.Name != "web-1" {
		t.Fatalf("unexpected instance ref: %#v", ref)
	}

	instance := call(t, sess, rid("instance.read"), map[string]string{
		"service": "web", "node": "node-1", "instance": "web-1",
	}, nil, nil).(row)
	if instance["port"] != 8080 || instance["status"] != "passing" || instance["address"] != "10.0.0.11" {
		t.Fatalf("unexpected instance detail: %#v", instance)
	}

	checks := call(t, sess, rid("service.checks"), map[string]string{"service": "web"}, nil, nil).(plugin.Page[row])
	if len(checks.Items) != 2 {
		t.Fatalf("unexpected service checks: %#v", checks.Items)
	}

	nodes := call(t, sess, rid("nodes.list"), nil, nil, nil).(plugin.Page[row])
	if len(nodes.Items) != 2 || nodes.Items[0]["name"] != "node-1" || nodes.Items[0]["status"] != "passing" {
		t.Fatalf("unexpected nodes: %#v", nodes.Items)
	}
	node := call(t, sess, rid("node.read"), map[string]string{"node": "node-1"}, nil, nil).(row)
	if node["address"] != "10.0.0.11" || node["services"] != 1 || node["passing"] != 1 {
		t.Fatalf("unexpected node detail: %#v", node)
	}
	nodeServicesPage := call(t, sess, rid("node.services"), map[string]string{"node": "node-1"}, nil, nil).(plugin.Page[row])
	if len(nodeServicesPage.Items) != 1 || nodeServicesPage.Items[0]["service"] != "web" {
		t.Fatalf("unexpected node services: %#v", nodeServicesPage.Items)
	}
	nodeChecksPage := call(t, sess, rid("node.checks"), map[string]string{"node": "node-1"}, nil, nil).(plugin.Page[row])
	if len(nodeChecksPage.Items) != 1 {
		t.Fatalf("unexpected node checks: %#v", nodeChecksPage.Items)
	}
}

func TestHealthCheckScoping(t *testing.T) {
	fake := newFakeConsul()
	sess, _ := newTestSession(t, fake)

	all := call(t, sess, rid("checks.list"), nil, nil, nil).(plugin.Page[row])
	if len(all.Items) != 3 {
		t.Fatalf("unexpected checks: %#v", all.Items)
	}
	critical := call(t, sess, rid("checks.list"), map[string]string{"state": "critical"}, nil, nil).(plugin.Page[row])
	if len(critical.Items) != 0 {
		t.Fatalf("no critical checks were seeded: %#v", critical.Items)
	}
	if !strings.HasSuffix(fake.lastPath("/v1/health/state/"), "critical") {
		t.Fatal("state scoping must be pushed to the health endpoint")
	}
	warning := call(t, sess, rid("checks.list"),
		map[string]string{"node": "node-2", "state": "warning"}, nil, nil).(plugin.Page[row])
	if len(warning.Items) != 1 || warning.Items[0]["status"] != "warning" {
		t.Fatalf("node-scoped state filtering failed: %#v", warning.Items)
	}
	if _, err := callErr(sess, rid("checks.list"), map[string]string{"state": "sideways"}, nil, nil); err == nil {
		t.Fatal("an unknown check state must be rejected")
	}

	detail := call(t, sess, rid("check.read"), map[string]string{"node": "node-1", "check": "service:web-1"}, nil, nil).(row)
	if detail["status"] != "passing" || detail["http"] != "http://10.0.0.11:8080/health" {
		t.Fatalf("unexpected check detail: %#v", detail)
	}
}

func TestSessionsQueriesAndIntentions(t *testing.T) {
	fake := newFakeConsul()
	sess, _ := newTestSession(t, fake)

	sessions := call(t, sess, rid("sessions.list"), nil, nil, nil).(plugin.Page[row])
	if len(sessions.Items) != 1 || sessions.Items[0]["name"] != "leader-election" {
		t.Fatalf("unexpected sessions: %#v", sessions.Items)
	}
	detail := call(t, sess, rid("session.read"), map[string]string{"session": "sess-1"}, nil, nil).(row)
	if detail["behavior"] != "release" || detail["lock_delay"] != float64(15) {
		t.Fatalf("unexpected session detail: %#v", detail)
	}
	call(t, sess, rid("session.destroy"), map[string]string{"session": "sess-1"}, nil, nil)
	if !slices.Contains(fake.destroyedSessions(), "sess-1") {
		t.Fatal("session.destroy did not reach consul")
	}

	queries := call(t, sess, rid("queries.list"), nil, nil, nil).(plugin.Page[row])
	if len(queries.Items) != 1 || queries.Items[0]["service"] != "web" {
		t.Fatalf("unexpected prepared queries: %#v", queries.Items)
	}
	query := call(t, sess, rid("query.read"), map[string]string{"query": "query-1"}, nil, nil).(row)
	if query["only_passing"] != true || query["dns_ttl"] != "10s" {
		t.Fatalf("unexpected prepared query detail: %#v", query)
	}
	results := call(t, sess, rid("query.execute"), map[string]string{"query": "query-1"}, nil, nil).(plugin.Page[row])
	if len(results.Items) != 1 || results.Items[0]["node"] != "node-1" {
		t.Fatalf("unexpected query results: %#v", results.Items)
	}

	intentions := call(t, sess, rid("intentions.list"), nil, nil, nil).(plugin.Page[row])
	if len(intentions.Items) != 1 || intentions.Items[0]["action"] != "allow" {
		t.Fatalf("unexpected intentions: %#v", intentions.Items)
	}
	scoped := call(t, sess, rid("intentions.list"), map[string]string{"destination": "nope"}, nil, nil).(plugin.Page[row])
	if len(scoped.Items) != 0 {
		t.Fatalf("destination scoping failed: %#v", scoped.Items)
	}
	intention := call(t, sess, rid("intention.read"), map[string]string{"source": "api", "destination": "web"}, nil, nil).(row)
	if intention["precedence"] != 9 {
		t.Fatalf("unexpected intention detail: %#v", intention)
	}
	toggled := call(t, sess, rid("intention.toggle"), map[string]string{"source": "api", "destination": "web"}, nil, nil).(row)
	if toggled["action"] != "deny" {
		t.Fatalf("toggle should flip allow to deny: %#v", toggled)
	}
	if upserted := fake.upsertedIntention(); upserted == nil || upserted.Action != api.IntentionActionDeny || upserted.ID != "" {
		t.Fatalf("unexpected upsert payload: %#v", upserted)
	}
	call(t, sess, rid("intention.delete"), map[string]string{"source": "api", "destination": "web"}, nil, nil)
	if fake.lastQuery("/v1/connect/intentions/exact").Get("destination") != "web" {
		t.Fatal("intention.delete did not address the exact pair")
	}
}

func TestACLRoutes(t *testing.T) {
	sess, _ := newTestSession(t, newFakeConsul())

	tokens := call(t, sess, rid("acl.tokens.list"), nil, nil, nil).(plugin.Page[row])
	if len(tokens.Items) != 1 {
		t.Fatalf("unexpected tokens: %#v", tokens.Items)
	}
	for key := range tokens.Items[0] {
		if key == "secret_id" {
			t.Fatal("token listings must never carry the secret id")
		}
	}
	token := call(t, sess, rid("acl.token.read"), map[string]string{"token": "acc-1"}, nil, nil).(row)
	if token["secret_id"] != secretPlaceholder {
		t.Fatalf("token detail must redact the secret: %#v", token["secret_id"])
	}
	if token["policies"] != "web-read" {
		t.Fatalf("unexpected token policies: %#v", token)
	}

	policies := call(t, sess, rid("acl.policies.list"), nil, nil, nil).(plugin.Page[row])
	if len(policies.Items) != 1 || policies.Items[0]["name"] != "web-read" {
		t.Fatalf("unexpected policies: %#v", policies.Items)
	}
	policy := call(t, sess, rid("acl.policy.read"), map[string]string{"policy": "pol-1"}, nil, nil).(row)
	if !strings.Contains(policy["rules"].(string), "service_prefix") {
		t.Fatalf("policy detail should carry its rules: %#v", policy)
	}
	roles := call(t, sess, rid("acl.roles.list"), nil, nil, nil).(plugin.Page[row])
	if len(roles.Items) != 1 || roles.Items[0]["policies"] != "web-read" {
		t.Fatalf("unexpected roles: %#v", roles.Items)
	}
	role := call(t, sess, rid("acl.role.read"), map[string]string{"role": "role-1"}, nil, nil).(row)
	if role["service_identities"] != "web" {
		t.Fatalf("unexpected role detail: %#v", role)
	}
}

func TestACLRoutesDegradeWhenDisabled(t *testing.T) {
	fake := newFakeConsul()
	fake.aclDisabled = true
	sess, _ := newTestSession(t, fake)
	for _, id := range []string{rid("acl.tokens.list"), rid("acl.policies.list"), rid("acl.roles.list"), rid("queries.list")} {
		page := call(t, sess, id, nil, nil, nil).(plugin.Page[row])
		if len(page.Items) != 0 || page.Total == nil || *page.Total != 0 {
			t.Fatalf("%s should degrade to an empty page when ACLs are disabled: %#v", id, page)
		}
	}
	if _, err := callErr(sess, rid("acl.token.read"), map[string]string{"token": "acc-1"}, nil, nil); err == nil {
		t.Fatal("reading one token must still surface the ACL error")
	}
}

func TestTreeRoutes(t *testing.T) {
	sess, _ := newTestSession(t, newFakeConsul())

	catalog := call(t, sess, rid("catalog.tree"), nil, nil, nil).(plugin.Page[plugin.TreeNode])
	if len(catalog.Items) != 3 || catalog.Items[0].ResourceKind != kindService {
		t.Fatalf("unexpected catalog tree: %#v", catalog.Items)
	}
	services := call(t, sess, rid("services.tree"), nil, nil, nil).(plugin.Page[plugin.TreeNode])
	if len(services.Items) != 4 {
		t.Fatalf("expected an all-services node plus one per tag: %#v", services.Items)
	}
	if services.Items[1].ListParams["tag"] == "" {
		t.Fatalf("tag nodes must scope the service list: %#v", services.Items[1])
	}
	checks := call(t, sess, rid("checks.tree"), nil, nil, nil).(plugin.Page[plugin.TreeNode])
	if len(checks.Items) != 4 || checks.Items[3].ListParams["state"] != api.HealthCritical {
		t.Fatalf("unexpected checks tree: %#v", checks.Items)
	}
	mesh := call(t, sess, rid("mesh.tree"), nil, nil, nil).(plugin.Page[plugin.TreeNode])
	if len(mesh.Items) != 3 {
		t.Fatalf("unexpected mesh tree: %#v", mesh.Items)
	}
	acl := call(t, sess, rid("acl.tree"), nil, nil, nil).(plugin.Page[plugin.TreeNode])
	if len(acl.Items) != 3 || acl.Items[0].ResourceKind != kindACLToken {
		t.Fatalf("unexpected acl tree: %#v", acl.Items)
	}
	for _, node := range slices.Concat(catalog.Items, services.Items, checks.Items, mesh.Items, acl.Items) {
		if node.ResourceKind == "" {
			continue
		}
		if !slices.ContainsFunc(New().Manifest().Resources, func(r plugin.ResourceType) bool {
			return r.Kind == node.ResourceKind
		}) {
			t.Fatalf("tree node %q opens unknown resource kind %q", node.Key, node.ResourceKind)
		}
	}
}

func TestKVWatchStreamsChanges(t *testing.T) {
	fake := newFakeConsul()
	fake.setKV("apps/web/config", []byte("1"))
	sess, _ := newTestSession(t, fake)

	ctx, cancel := context.WithCancel(context.Background())
	fake.onKeysListed = func(count int) {
		if count >= 2 {
			cancel()
		}
	}
	go func() {
		fake.setKV("apps/web/extra", []byte("2"))
	}()

	h := plugintest.NewHarness(t, routes())
	out := h.Stream(ctx, rid("kv.watch"), sess, map[string]string{"prefix": "apps/"}, nil, nil)
	if !strings.Contains(string(out), "Watching consul KV prefix apps/") {
		t.Fatalf("the watch should announce its prefix: %s", out)
	}
	if !strings.Contains(string(out), `"line"`) {
		t.Fatalf("the watch must emit log frames: %s", out)
	}
}

func TestDescribeChangeReportsDiff(t *testing.T) {
	known := map[string]bool{"a": true, "b": true}
	current := map[string]bool{"b": true, "c": true}
	lines := describeChange(known, current, []string{"b", "c"}, 12, false)
	sort.Strings(lines)
	if !slices.Equal(lines, []string{"[+] c", "[-] a"}) {
		t.Fatalf("unexpected diff: %v", lines)
	}
	if got := describeChange(nil, current, []string{"b", "c"}, 12, true); !strings.HasPrefix(got[0], "[sync]") {
		t.Fatalf("the first pass should report a sync line: %v", got)
	}
	if got := describeChange(known, nil, []string{"b"}, 20, false); !strings.HasPrefix(got[0], "[index]") {
		t.Fatalf("an oversized prefix should report index movement: %v", got)
	}
	if got := describeChange(nil, current, []string{"b", "c"}, 21, false); !strings.HasPrefix(got[0], "[index]") {
		t.Fatalf("a prefix that was over the cap should keep reporting index movement: %v", got)
	}
	if got := describeChange(known, known, []string{"a", "b"}, 21, false); !strings.HasPrefix(got[0], "[update]") {
		t.Fatalf("a value-only change should report an update: %v", got)
	}
}

func TestValueCodec(t *testing.T) {
	if value, kind := decodeValue([]byte(`{"a":1}`)); kind != "json" || value != `{"a":1}` {
		t.Fatalf("unexpected json decode: %q %q", value, kind)
	}
	if _, kind := decodeValue([]byte{0xff, 0xfe}); kind != "binary" {
		t.Fatalf("invalid utf-8 should decode as binary, got %q", kind)
	}
	raw, err := encodeValue(map[string]any{"a": 1}, "json")
	if err != nil || string(raw) != `{"a":1}` {
		t.Fatalf("unexpected encode: %q %v", raw, err)
	}
	if raw, err := encodeValue("AQI=", "binary"); err != nil || len(raw) != 2 {
		t.Fatalf("binary values should base64-decode: %v %v", raw, err)
	}
	if _, err := encodeValue("not base64!!", "binary"); err == nil {
		t.Fatal("invalid base64 must be rejected")
	}
}

func TestHealthCountsStatus(t *testing.T) {
	var counts healthCounts
	if counts.status() != "unknown" {
		t.Fatalf("an empty set is unknown, got %q", counts.status())
	}
	counts = counts.add(api.HealthPassing)
	if counts.status() != api.HealthPassing {
		t.Fatalf("expected passing, got %q", counts.status())
	}
	counts = counts.add(api.HealthWarning)
	if counts.status() != api.HealthWarning {
		t.Fatalf("warning outranks passing, got %q", counts.status())
	}
	counts = counts.add(api.HealthCritical)
	if counts.status() != api.HealthCritical {
		t.Fatalf("critical outranks warning, got %q", counts.status())
	}
}

func TestEveryRouteIsCovered(t *testing.T) {
	fake := newFakeConsul()
	fake.setKV("apps/web/config", []byte("1"))
	sess, _ := newTestSession(t, fake)
	h := plugintest.NewHarness(t, routes())
	ctx := context.Background()

	params := map[string]string{
		"prefix": "apps/", "key": "apps/web/config", "service": "web", "node": "node-1",
		"instance": "web-1", "check": "service:web-1", "session": "sess-1", "query": "query-1",
		"source": "api", "destination": "web", "token": "acc-1", "policy": "pol-1", "role": "role-1",
	}
	body := map[string][]byte{
		rid("kv.put"):   mustJSON(t, map[string]any{"key": "apps/web/config", "value": "v"}),
		rid("kv.write"): mustJSON(t, map[string]any{"value": "v"}),
	}
	for _, route := range routes() {
		if route.Method == plugin.MethodWS {
			streamCtx, cancel := context.WithCancel(ctx)
			cancel()
			h.Stream(streamCtx, route.ID, sess, params, nil, nil)
			continue
		}
		h.Call(ctx, route.ID, sess, params, nil, body[route.ID])
	}
	h.AssertAllCovered()
}

func newTestSession(t *testing.T, fake *fakeConsul) (plugin.Session, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	fake.server = server
	sess := connectServer(t, server, map[string]any{"read_only": false})
	return sess, server
}

func connectFake(t *testing.T, fake *fakeConsul, config map[string]any) plugin.Session {
	t.Helper()
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	fake.server = server
	return connectServer(t, server, config)
}

func connectServer(t *testing.T, server *httptest.Server, config map[string]any) plugin.Session {
	t.Helper()
	host, port, err := splitURL(server.URL)
	if err != nil {
		t.Fatalf("parse test server URL: %v", err)
	}
	merged := map[string]any{"host": host, "port": port}
	for key, value := range config {
		merged[key] = value
	}
	sess, err := New().Connect(context.Background(), plugin.ConnectConfig{
		Config: merged,
		Net:    sdktest.DirectTransport(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = sess.Close() })
	return sess
}

func splitURL(raw string) (string, int, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(parsed.Port())
	if err != nil {
		return "", 0, err
	}
	return parsed.Hostname(), port, nil
}

func call(t *testing.T, sess plugin.Session, id string, params map[string]string, query url.Values, body []byte) any {
	t.Helper()
	out, err := callErr(sess, id, params, query, body)
	if err != nil {
		t.Fatalf("%s: %v", id, err)
	}
	return out
}

func callErr(sess plugin.Session, id string, params map[string]string, query url.Values, body []byte) (any, error) {
	route, ok := sdktest.RouteMap(routes())[id]
	if !ok || route.Handle == nil {
		return nil, fmt.Errorf("route %q has no handler", id)
	}
	return route.Handle(plugin.NewRequestContext(context.Background(), plugin.User{}, sess, params, query, body))
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

// fakeConsul answers the subset of the Consul HTTP API the handlers use.
type fakeConsul struct {
	mu              sync.Mutex
	server          *httptest.Server
	kv              map[string]*api.KVPair
	index           uint64
	requests        []recordedRequest
	destroyed       []string
	upserted        *api.Intention
	aclDisabled     bool
	connectDisabled bool
	onKeysListed    func(int)
}

type recordedRequest struct {
	method string
	path   string
	query  url.Values
}

func newFakeConsul() *fakeConsul {
	return &fakeConsul{kv: map[string]*api.KVPair{}, index: 100}
}

func (f *fakeConsul) setKV(key string, value []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.index++
	f.kv[key] = &api.KVPair{Key: key, Value: value, CreateIndex: f.index, ModifyIndex: f.index}
}

func (f *fakeConsul) value(key string) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	if pair, ok := f.kv[key]; ok {
		return pair.Value
	}
	return nil
}

func (f *fakeConsul) modifyIndex(key string) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if pair, ok := f.kv[key]; ok {
		return pair.ModifyIndex
	}
	return 0
}

func (f *fakeConsul) has(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.kv[key]
	return ok
}

func (f *fakeConsul) destroyedSessions() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.destroyed)
}

func (f *fakeConsul) upsertedIntention() *api.Intention {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.upserted
}

func (f *fakeConsul) lastQuery(path string) url.Values {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.requests) - 1; i >= 0; i-- {
		if strings.HasPrefix(f.requests[i].path, path) {
			return f.requests[i].query
		}
	}
	return url.Values{}
}

func (f *fakeConsul) lastPath(prefix string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.requests) - 1; i >= 0; i-- {
		if strings.HasPrefix(f.requests[i].path, prefix) {
			return f.requests[i].path
		}
	}
	return ""
}

func (f *fakeConsul) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests = append(f.requests, recordedRequest{method: r.Method, path: r.URL.Path, query: r.URL.Query()})
	index := f.index
	f.mu.Unlock()

	w.Header().Set("X-Consul-Index", strconv.FormatUint(index, 10))
	w.Header().Set("X-Consul-LastContact", "0")
	w.Header().Set("X-Consul-KnownLeader", "true")
	w.Header().Set("Content-Type", "application/json")

	path := r.URL.Path
	if f.aclDisabled && (strings.HasPrefix(path, "/v1/acl/") || strings.HasPrefix(path, "/v1/query")) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, "ACL support disabled")
		return
	}
	if f.connectDisabled && strings.HasPrefix(path, "/v1/connect/") {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, "Connect must be enabled in order to use this endpoint")
		return
	}
	if strings.HasPrefix(path, "/v1/kv") {
		f.serveKV(w, r)
		return
	}
	body, ok := f.staticBody(path, r)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, "not found")
		return
	}
	_, _ = io.WriteString(w, body)
}

func (f *fakeConsul) staticBody(path string, r *http.Request) (string, bool) {
	switch {
	case path == "/v1/status/leader":
		return `"10.0.0.1:8300"`, true
	case path == "/v1/agent/self":
		return `{"Config":{"NodeName":"consul-server-1","NodeID":"11111111-1111-1111-1111-111111111111",` +
			`"Datacenter":"dc1","PrimaryDatacenter":"dc1","Server":true,"Version":"1.20.1","Revision":"abc123"},` +
			`"DebugConfig":{"AdvertiseAddrLAN":"10.0.0.1","BindAddr":"0.0.0.0","ACLsEnabled":false,"RaftProtocol":3,` +
			`"TaggedAddresses":{"lan":"10.0.0.1"}},` +
			`"Member":{"Tags":{"role":"consul","dc":"dc1"}},"Meta":{"rack":"a1"}}`, true
	case path == "/v1/agent/members":
		return `[{"Name":"consul-server-1","Addr":"10.0.0.1","Port":8301,"Status":1,` +
			`"Tags":{"role":"consul","dc":"dc1","build":"1.20.1:abc"}},` +
			`{"Name":"node-2","Addr":"10.0.0.12","Port":8301,"Status":1,"Tags":{"role":"node","dc":"dc1"}}]`, true
	case path == "/v1/operator/raft/configuration":
		return `{"Servers":[{"ID":"11111111-1111-1111-1111-111111111111","Node":"consul-server-1",` +
			`"Address":"10.0.0.1:8300","Leader":true,"Voter":true,"ProtocolVersion":"3","LastIndex":42}],"Index":42}`, true
	case path == "/v1/catalog/datacenters":
		return `["dc1","dc2"]`, true
	case path == "/v1/catalog/services":
		return `{"web":["v1","canary"],"db":["primary"]}`, true
	case path == "/v1/catalog/nodes":
		return `[{"ID":"n1","Node":"node-1","Address":"10.0.0.11","Datacenter":"dc1","Meta":{"rack":"a1"}},` +
			`{"ID":"n2","Node":"node-2","Address":"10.0.0.12","Datacenter":"dc1","Meta":{}}]`, true
	case path == "/v1/catalog/node/node-1":
		return `{"Node":{"ID":"n1","Node":"node-1","Address":"10.0.0.11","Datacenter":"dc1","Meta":{"rack":"a1"}},` +
			`"Services":{"web-1":{"ID":"web-1","Service":"web","Port":8080}}}`, true
	case path == "/v1/catalog/node-services/node-1":
		return `{"Node":{"Node":"node-1","Address":"10.0.0.11"},` +
			`"Services":[{"ID":"web-1","Service":"web","Address":"10.0.0.11","Port":8080,"Tags":["v1"]}]}`, true
	case path == "/v1/catalog/deregister":
		return `true`, true
	case path == "/v1/health/service/web":
		return webServiceEntries, true
	case path == "/v1/health/service/db":
		return `[{"Node":{"Node":"node-2","Address":"10.0.0.12","Datacenter":"dc1"},` +
			`"Service":{"ID":"db-1","Service":"db","Address":"10.0.0.12","Port":5432,"Tags":["primary"]},` +
			`"Checks":[{"Node":"node-2","CheckID":"service:db-1","Name":"db alive","Status":"passing","ServiceName":"db"}]}]`, true
	case path == "/v1/health/checks/web":
		return webChecks, true
	case strings.HasPrefix(path, "/v1/health/state/"):
		state := strings.TrimPrefix(path, "/v1/health/state/")
		return filterChecksJSON(allChecks, state), true
	case path == "/v1/health/node/node-1":
		return `[{"Node":"node-1","CheckID":"service:web-1","Name":"web alive","Status":"passing","ServiceName":"web",` +
			`"Definition":{"HTTP":"http://10.0.0.11:8080/health","Interval":"10s","Timeout":"2s"}}]`, true
	case path == "/v1/health/node/node-2":
		return `[{"Node":"node-2","CheckID":"service:web-2","Name":"web degraded","Status":"warning","ServiceName":"web"}]`, true
	case path == "/v1/session/list":
		return sessionList, true
	case path == "/v1/session/info/sess-1":
		return sessionList, true
	case strings.HasPrefix(path, "/v1/session/destroy/"):
		f.mu.Lock()
		f.destroyed = append(f.destroyed, strings.TrimPrefix(path, "/v1/session/destroy/"))
		f.mu.Unlock()
		return `true`, true
	case path == "/v1/query":
		return preparedQueries, true
	case path == "/v1/query/query-1":
		return preparedQueries, true
	case path == "/v1/query/query-1/execute":
		return `{"Service":"web","Datacenter":"dc1","Nodes":[{"Node":{"Node":"node-1","Address":"10.0.0.11"},` +
			`"Service":{"ID":"web-1","Service":"web","Port":8080,"Tags":["v1"]},` +
			`"Checks":[{"Node":"node-1","CheckID":"service:web-1","Status":"passing"}]}]}`, true
	case path == "/v1/connect/intentions":
		return intentions, true
	case path == "/v1/connect/intentions/exact":
		switch r.Method {
		case http.MethodPut:
			var payload api.Intention
			_ = json.NewDecoder(r.Body).Decode(&payload)
			f.mu.Lock()
			f.upserted = &payload
			f.mu.Unlock()
			return `true`, true
		case http.MethodDelete:
			return `true`, true
		default:
			return exactIntentionJSON, true
		}
	case path == "/v1/acl/tokens":
		return aclTokens, true
	case path == "/v1/acl/token/acc-1":
		return aclToken, true
	case path == "/v1/acl/policies":
		return aclPolicies, true
	case path == "/v1/acl/policy/pol-1":
		return aclPolicy, true
	case path == "/v1/acl/roles":
		return aclRoles, true
	case path == "/v1/acl/role/role-1":
		return aclRole, true
	default:
		return "", false
	}
}

func (f *fakeConsul) serveKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/v1/kv"), "/")
	query := r.URL.Query()
	switch r.Method {
	case http.MethodGet:
		if _, ok := query["keys"]; ok {
			keys := f.keys(key, query.Get("separator"))
			if f.onKeysListed != nil {
				f.onKeysListed(len(keys))
			}
			_ = json.NewEncoder(w).Encode(keys)
			return
		}
		f.mu.Lock()
		pair, ok := f.kv[key]
		f.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode([]*api.KVPair{pair})
	case http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		defer f.mu.Unlock()
		if raw := query.Get("cas"); raw != "" {
			want, _ := strconv.ParseUint(raw, 10, 64)
			current, exists := f.kv[key]
			if (!exists && want != 0) || (exists && current.ModifyIndex != want) {
				_, _ = io.WriteString(w, "false")
				return
			}
		}
		f.index++
		flags, _ := strconv.ParseUint(query.Get("flags"), 10, 64)
		f.kv[key] = &api.KVPair{Key: key, Value: body, Flags: flags, CreateIndex: f.index, ModifyIndex: f.index}
		_, _ = io.WriteString(w, "true")
	case http.MethodDelete:
		f.mu.Lock()
		defer f.mu.Unlock()
		if _, recurse := query["recurse"]; recurse {
			for existing := range f.kv {
				if strings.HasPrefix(existing, key) {
					delete(f.kv, existing)
				}
			}
		} else {
			delete(f.kv, key)
		}
		_, _ = io.WriteString(w, "true")
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (f *fakeConsul) keys(prefix, separator string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	seen := map[string]bool{}
	out := make([]string, 0, len(f.kv))
	for key := range f.kv {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		entry := key
		if separator != "" {
			if idx := strings.Index(key[len(prefix):], separator); idx >= 0 {
				entry = key[:len(prefix)+idx+len(separator)]
			}
		}
		if seen[entry] {
			continue
		}
		seen[entry] = true
		out = append(out, entry)
	}
	sort.Strings(out)
	return out
}

func filterChecksJSON(raw, state string) string {
	var checks []map[string]any
	if err := json.Unmarshal([]byte(raw), &checks); err != nil {
		return raw
	}
	if state == api.HealthAny {
		return raw
	}
	kept := make([]map[string]any, 0, len(checks))
	for _, check := range checks {
		if check["Status"] == state {
			kept = append(kept, check)
		}
	}
	out, err := json.Marshal(kept)
	if err != nil {
		return raw
	}
	return string(out)
}

const webServiceEntries = `[
 {"Node":{"Node":"node-1","Address":"10.0.0.11","Datacenter":"dc1"},
  "Service":{"ID":"web-1","Service":"web","Address":"10.0.0.11","Port":8080,"Tags":["v1"],"Meta":{"team":"core"}},
  "Checks":[{"Node":"node-1","CheckID":"service:web-1","Name":"web alive","Status":"passing","ServiceName":"web"}]},
 {"Node":{"Node":"node-2","Address":"10.0.0.12","Datacenter":"dc1"},
  "Service":{"ID":"web-2","Service":"web","Port":8080,"Tags":["canary"]},
  "Checks":[{"Node":"node-2","CheckID":"service:web-2","Name":"web degraded","Status":"warning","ServiceName":"web"}]}]`

const webChecks = `[
 {"Node":"node-1","CheckID":"service:web-1","Name":"web alive","Status":"passing","ServiceName":"web"},
 {"Node":"node-2","CheckID":"service:web-2","Name":"web degraded","Status":"warning","ServiceName":"web"}]`

const allChecks = `[
 {"Node":"node-1","CheckID":"service:web-1","Name":"web alive","Status":"passing","ServiceName":"web"},
 {"Node":"node-2","CheckID":"service:web-2","Name":"web degraded","Status":"warning","ServiceName":"web"},
 {"Node":"node-2","CheckID":"service:db-1","Name":"db alive","Status":"passing","ServiceName":"db"}]`

const sessionList = `[{"ID":"sess-1","Name":"leader-election","Node":"node-1","Behavior":"release",
 "TTL":"30s","LockDelay":15000000000,"NodeChecks":["serfHealth"],"ServiceChecks":[],"CreateIndex":7}]`

const preparedQueries = `[{"ID":"query-1","Name":"web-nearby","Session":"",
 "Service":{"Service":"web","OnlyPassing":true,"Near":"_agent","Tags":["v1"]},"DNS":{"TTL":"10s"},
 "Template":{"Type":"","Regexp":""}}]`

const intentions = `[{"ID":"ixn-1","SourceNS":"default","SourceName":"api","DestinationNS":"default",
 "DestinationName":"web","SourceType":"consul","Action":"allow","Precedence":9,
 "Description":"api may call web","CreatedAt":"2026-01-01T00:00:00Z","UpdatedAt":"2026-01-02T00:00:00Z"}]`

const exactIntentionJSON = `{"ID":"ixn-1","SourceNS":"default","SourceName":"api","DestinationNS":"default",
 "DestinationName":"web","SourceType":"consul","Action":"allow","Precedence":9,
 "Description":"api may call web","CreatedAt":"2026-01-01T00:00:00Z","UpdatedAt":"2026-01-02T00:00:00Z"}`

const aclTokens = `[{"AccessorID":"acc-1","SecretID":"super-secret","Description":"web token",
 "Policies":[{"ID":"pol-1","Name":"web-read"}],"Roles":[],"Local":false,"CreateTime":"2026-01-01T00:00:00Z"}]`

const aclToken = `{"AccessorID":"acc-1","SecretID":"super-secret","Description":"web token",
 "Policies":[{"ID":"pol-1","Name":"web-read"}],"Roles":[],
 "ServiceIdentities":[{"ServiceName":"web"}],"NodeIdentities":[],"Local":false,
 "CreateTime":"2026-01-01T00:00:00Z","CreateIndex":5,"ModifyIndex":6}`

const aclPolicies = `[{"ID":"pol-1","Name":"web-read","Description":"read web","Datacenters":["dc1"]}]`

const aclPolicy = `{"ID":"pol-1","Name":"web-read","Description":"read web","Datacenters":["dc1"],
 "Rules":"service_prefix \"web\" { policy = \"read\" }"}`

const aclRoles = `[{"ID":"role-1","Name":"web-role","Description":"web role",
 "Policies":[{"ID":"pol-1","Name":"web-read"}]}]`

const aclRole = `{"ID":"role-1","Name":"web-role","Description":"web role",
 "Policies":[{"ID":"pol-1","Name":"web-read"}],"ServiceIdentities":[{"ServiceName":"web"}],"NodeIdentities":[]}`
