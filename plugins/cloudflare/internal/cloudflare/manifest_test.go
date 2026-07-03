package cloudflare

import (
	"strings"
	"testing"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestManifestValidates(t *testing.T) {
	p := Cloudflare{}
	plugintest.ValidatePlugin(t, p)
	plugintest.ValidateProjectionPanelConfigs(t, plugin.BuildProjection(p.Manifest(), routeMap(p.Routes())))
}

func TestManifestUsesDirectGatewayTransport(t *testing.T) {
	m := Cloudflare{}.Manifest()
	if !m.SupportsTransport(plugin.TransportDirect) || m.SupportsTransport(plugin.TransportAgent) {
		t.Fatalf("Cloudflare should support only direct transport: %+v", m.SupportedTransports)
	}
}

func TestConfigDoesNotExposeProcessEnvTokenMode(t *testing.T) {
	cfg := Cloudflare{}.Manifest().Config
	for _, group := range cfg.Groups {
		for _, field := range group.Fields {
			if field.Key != "auth" {
				continue
			}
			for _, option := range field.Options {
				if option.Value == "env" {
					t.Fatal("Cloudflare auth config should not expose process environment token mode")
				}
			}
			return
		}
	}
	t.Fatal("auth field not found")
}

func TestGlobalResourceListRoutesAreUnscoped(t *testing.T) {
	routes := routeMap(routes())
	for _, id := range []string{
		rid("dns.list"),
		rid("rulesets.list"),
		rid("waf.list"),
		rid("firewall.rules.list"),
		rid("page_rules.list"),
		rid("certificates.list"),
		rid("workers.routes.list"),
		rid("zone.settings.list"),
		rid("tunnels.list"),
	} {
		route, ok := routes[id]
		if !ok {
			t.Fatalf("missing route %s", id)
		}
		if strings.Contains(route.Path, "{") {
			t.Fatalf("route %s path %q requires a scoped path param", id, route.Path)
		}
	}
}

func TestConfigParsesStoredToken(t *testing.T) {
	cfg := plugin.ConnectConfig{
		Config: map[string]any{"auth": "stored_token"},
		Credentials: plugin.NewResolvedCredentials(plugin.CredentialBinding{
			Field: credentialField,
			Credential: plugin.ResolvedCredential{
				ID:     "cred_1",
				Kind:   plugin.CredentialKindAPIToken,
				Values: map[string]string{"token": "secret"},
			},
		}),
	}
	opts, err := parseOptions(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if opts.Token != "secret" {
		t.Fatalf("token = %q", opts.Token)
	}
}

func TestConfigRejectsMissingToken(t *testing.T) {
	_, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{"auth": "token"}})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestExplorerRejectsSensitiveHeaders(t *testing.T) {
	for _, header := range []string{"Authorization", "Cookie", "Host", "Connection"} {
		if _, err := safeExplorerHeader(header); err == nil {
			t.Fatalf("expected %s to be rejected", header)
		}
	}
	if got, err := safeExplorerHeader("if-match"); err != nil || got != "If-Match" {
		t.Fatalf("safe header = %q, err = %v", got, err)
	}
}

func TestDNSUpdateSchemaUsesRecordDefaults(t *testing.T) {
	schema := dnsRecordSchema(true)
	defaults := map[string]any{}
	for _, group := range schema.Groups {
		for _, field := range group.Fields {
			defaults[field.Key] = field.Default
		}
	}
	for _, key := range []string{"type", "name", "content", "ttl", "proxied", "comment"} {
		want := "${record." + key + "}"
		if defaults[key] != want {
			t.Fatalf("%s default = %#v, want %q", key, defaults[key], want)
		}
	}
}

func routeMap(routes []plugin.Route) map[string]plugin.Route {
	out := make(map[string]plugin.Route, len(routes))
	for _, route := range routes {
		out[route.ID] = route
	}
	return out
}
