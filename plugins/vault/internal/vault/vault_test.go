package vault

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	vaultapi "github.com/hashicorp/vault/api"

	contribtest "github.com/charlesng35/shellcn-contrib/shared/plugintest"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestManifestValidates(t *testing.T) {
	p := New()
	plugintest.ValidatePlugin(t, p)
	proj := plugintest.Projection(t, p)
	plugintest.ValidateProjectionPanelConfigs(t, proj)

	if proj.Layout != plugin.LayoutSidebarTree {
		t.Fatalf("layout: got %q want %q", proj.Layout, plugin.LayoutSidebarTree)
	}
	if proj.Category.Key != plugin.CategorySecurity {
		t.Fatalf("category: got %q want %q", proj.Category.Key, plugin.CategorySecurity)
	}
	if len(proj.Resources) != 9 {
		t.Fatalf("resources: got %d want 9", len(proj.Resources))
	}
	m := p.Manifest()
	if m.Agent == nil || len(m.SupportedTransports) != 2 {
		t.Fatalf("vault should offer direct and agent transports: %+v", m.SupportedTransports)
	}
	if !plugintest.CredentialKindSupported(m.Config, plugin.CredentialKindAPIToken) {
		t.Fatal("stored API token credential should be selectable")
	}
	if !plugintest.CredentialKindSupported(m.Config, credentialKindAppRole) {
		t.Fatal("AppRole credential kind should be selectable")
	}
}

func TestAppRoleCredentialKindKeepsSecretIDSecret(t *testing.T) {
	kinds := New().Manifest().CredentialKinds
	if len(kinds) != 1 {
		t.Fatalf("expected one declared credential kind, got %d", len(kinds))
	}
	for _, field := range kinds[0].Fields {
		switch field.Key {
		case "secret_id":
			if !field.Secret || field.Public {
				t.Fatalf("secret_id must be stored as secret material: %+v", field)
			}
		case "role_id":
			if field.Secret {
				t.Fatalf("role_id is public metadata, not secret material: %+v", field)
			}
		default:
			t.Fatalf("unexpected AppRole credential field %q", field.Key)
		}
	}
}

func TestSecretsPanelDeclaresPathDelimiter(t *testing.T) {
	for _, resource := range New().Manifest().Resources {
		if resource.Kind != "mount" {
			continue
		}
		for _, tab := range resource.Detail.Tabs {
			if tab.Key != "secrets" {
				continue
			}
			cfg, ok := tab.Config.(plugin.KVConfig)
			if !ok {
				t.Fatalf("secrets tab config is not a KVConfig: %T", tab.Config)
			}
			if cfg.Delimiter != "/" {
				t.Fatalf("secret keys are hierarchical: got delimiter %q", cfg.Delimiter)
			}
			if !cfg.Writable || cfg.KeyParam != "key" {
				t.Fatalf("unexpected KV config: %+v", cfg)
			}
			if tab.VisibleWhen == nil {
				t.Fatal("the secrets tab must be hidden for engines that are not key/value stores")
			}
			return
		}
	}
	t.Fatal("mount detail has no secrets tab")
}

func TestConnectionSchemaCoversEveryAuthMethod(t *testing.T) {
	fields := map[string]plugin.Field{}
	for _, group := range New().Manifest().Config.Groups {
		for _, field := range group.Fields {
			fields[field.Key] = field
		}
	}
	for _, key := range []string{"host", "port", "namespace", "auth", "token", tokenCredField, approleCredField, "approle_path", "tls_mode", "ca_certificate", "read_only", "timeout"} {
		if _, ok := fields[key]; !ok {
			t.Fatalf("missing connection field %q", key)
		}
	}
	if !fields["token"].Secret {
		t.Fatal("the inline token field must be stored as a secret")
	}
	if fields["auth"].Type != plugin.FieldSelect {
		t.Fatalf("auth should be a closed vocabulary, got %q", fields["auth"].Type)
	}
}

func TestParseOptionsResolvesStoredToken(t *testing.T) {
	opts, err := parseOptions(plugin.ConnectConfig{
		Config: map[string]any{"host": "vault.internal", "auth": authTokenCredential},
		Credentials: plugin.NewResolvedCredentials(plugin.CredentialBinding{
			Field:      tokenCredField,
			Credential: plugin.ResolvedCredential{Kind: plugin.CredentialKindAPIToken, Values: map[string]string{"token": "hvs.stored"}},
		}),
	})
	if err != nil {
		t.Fatalf("parse options: %v", err)
	}
	if opts.Token != "hvs.stored" {
		t.Fatalf("stored token not applied: %+v", opts)
	}
	if opts.Port != defaultPort || !opts.ReadOnly {
		t.Fatalf("unexpected defaults: %+v", opts)
	}
	if opts.address() != "https://vault.internal:8200" {
		t.Fatalf("unexpected address %q", opts.address())
	}
}

func TestParseOptionsResolvesAppRoleCredential(t *testing.T) {
	opts, err := parseOptions(plugin.ConnectConfig{
		Config: map[string]any{"host": "vault.internal", "auth": authAppRole, "approle_path": "/ci-approle/", "tls_mode": tlsDisable},
		Credentials: plugin.NewResolvedCredentials(plugin.CredentialBinding{
			Field:      approleCredField,
			Credential: plugin.ResolvedCredential{Kind: credentialKindAppRole, Values: map[string]string{"role_id": "role-1", "secret_id": "secret-1"}},
		}),
	})
	if err != nil {
		t.Fatalf("parse options: %v", err)
	}
	if opts.RoleID != "role-1" || opts.SecretID != "secret-1" {
		t.Fatalf("AppRole material not applied: %+v", opts)
	}
	if opts.AppRolePath != "ci-approle" {
		t.Fatalf("AppRole mount not normalized: %q", opts.AppRolePath)
	}
	if opts.address() != "http://vault.internal:8200" {
		t.Fatalf("tls disabled should select http: %q", opts.address())
	}
}

func TestParseOptionsRejectsIncompleteInput(t *testing.T) {
	cases := map[string]map[string]any{
		"missing host":     {},
		"missing token":    {"host": "vault.internal", "auth": authToken},
		"missing approle":  {"host": "vault.internal", "auth": authAppRole},
		"unknown auth":     {"host": "vault.internal", "auth": "kerberos"},
		"unknown tls mode": {"host": "vault.internal", "auth": authToken, "token": "t", "tls_mode": "maybe"},
		"bad port":         {"host": "vault.internal", "auth": authToken, "token": "t", "port": 70000},
	}
	for name, config := range cases {
		if _, err := parseOptions(plugin.ConnectConfig{Config: config}); err == nil {
			t.Fatalf("%s: expected an error", name)
		}
	}
}

func TestClientTLSConfigRejectsBadCA(t *testing.T) {
	if _, err := clientTLSConfig(options{TLSMode: tlsVerifyCA, CACertificate: "not-a-pem"}); err == nil {
		t.Fatal("expected an error for a malformed CA bundle")
	}
	cfg, err := clientTLSConfig(options{TLSMode: tlsRequire, Host: "vault.internal"})
	if err != nil {
		t.Fatalf("tls config: %v", err)
	}
	if !cfg.InsecureSkipVerify {
		t.Fatal("require mode encrypts without verifying the peer")
	}
	if cfg, err := clientTLSConfig(options{TLSMode: tlsDisable}); err != nil || cfg != nil {
		t.Fatalf("disable mode should not build a TLS config: %v %v", cfg, err)
	}
}

func TestEnsureWritableBlocksInReadOnly(t *testing.T) {
	if err := ensureWritable(&Session{opts: options{ReadOnly: true}}); err == nil {
		t.Fatal("read-only sessions must block writes")
	}
	if err := ensureWritable(&Session{opts: options{ReadOnly: false}}); err != nil {
		t.Fatalf("writable session should allow writes: %v", err)
	}
}

func TestKVVersionDetection(t *testing.T) {
	cases := []struct {
		mount vaultapi.MountOutput
		want  int
	}{
		{vaultapi.MountOutput{Type: "kv", Options: map[string]string{"version": "2"}}, 2},
		{vaultapi.MountOutput{Type: "kv", Options: map[string]string{"version": "1"}}, 1},
		{vaultapi.MountOutput{Type: "kv"}, 1},
		{vaultapi.MountOutput{Type: "generic"}, 1},
		{vaultapi.MountOutput{Type: "cubbyhole"}, 1},
		{vaultapi.MountOutput{Type: "database"}, 0},
		{vaultapi.MountOutput{Type: "pki"}, 0},
	}
	for _, tc := range cases {
		if got := kvVersion(&tc.mount); got != tc.want {
			t.Fatalf("%s %v: got kv version %d want %d", tc.mount.Type, tc.mount.Options, got, tc.want)
		}
	}
	if got := kvVersion(nil); got != 0 {
		t.Fatalf("nil mount should not be a KV engine, got %d", got)
	}
}

func TestListPathUsesMetadataIndexForKVv2(t *testing.T) {
	v2 := mountInfo{Path: "secret/", KVVersion: 2}
	v1 := mountInfo{Path: "kv1/", KVVersion: 1}
	cases := []struct {
		mount  mountInfo
		prefix string
		want   string
	}{
		{v2, "", "secret/metadata"},
		{v2, "app/", "secret/metadata/app"},
		{v1, "", "kv1"},
		{v1, "app/", "kv1/app"},
	}
	for _, tc := range cases {
		if got := listPath(tc.mount, tc.prefix); got != tc.want {
			t.Fatalf("listPath(%q, %q) = %q want %q", tc.mount.Path, tc.prefix, got, tc.want)
		}
	}
}

func TestPageWindowEmitsStableOffsetCursors(t *testing.T) {
	start, end, next, err := pageWindow(plugin.PageRequest{Limit: 2}, 5)
	if err != nil || start != 0 || end != 2 || next != "2" {
		t.Fatalf("first page: %d %d %q %v", start, end, next, err)
	}
	start, end, next, err = pageWindow(plugin.PageRequest{Limit: 2, Cursor: "4"}, 5)
	if err != nil || start != 4 || end != 5 || next != "" {
		t.Fatalf("last page must not advertise more rows: %d %d %q %v", start, end, next, err)
	}
	start, end, next, err = pageWindow(plugin.PageRequest{Limit: 2, Cursor: "9"}, 5)
	if err != nil || start != 5 || end != 5 || next != "" {
		t.Fatalf("cursor past the end should return an empty final page: %d %d %q %v", start, end, next, err)
	}
	if _, _, _, err := pageWindow(plugin.PageRequest{Limit: 2, Cursor: "abc"}, 5); err == nil {
		t.Fatal("a non-numeric cursor must be rejected, not silently treated as zero")
	}
}

func TestPageNamesFiltersAndSortsAcrossTheDataset(t *testing.T) {
	names := []string{"alpha", "beta", "gamma", "alphabet"}
	rc := requestContext(nil, url.Values{"limit": {"2"}, "filter": {"alpha"}})
	window, next, total, err := pageNames(rc, append([]string(nil), names...), "name")
	if err != nil {
		t.Fatalf("page names: %v", err)
	}
	if total != 2 || next != "" {
		t.Fatalf("filter must apply to the whole set: total=%d next=%q", total, next)
	}
	if strings.Join(window, ",") != "alpha,alphabet" {
		t.Fatalf("unexpected window: %v", window)
	}

	rc = requestContext(nil, url.Values{"limit": {"2"}, "sort": {"-name"}})
	window, next, total, err = pageNames(rc, append([]string(nil), names...), "name")
	if err != nil {
		t.Fatalf("page names: %v", err)
	}
	if total != 4 || next != "2" {
		t.Fatalf("sorted page should continue: total=%d next=%q", total, next)
	}
	if strings.Join(window, ",") != "gamma,beta" {
		t.Fatalf("descending sort must order the dataset, got %v", window)
	}
}

func TestOrderNamesAlwaysProducesADeterministicOrder(t *testing.T) {
	names := []string{"c", "a", "b"}
	orderNames(names, []string{"name"}, []plugin.SortKey{{Field: "ttl", Desc: true}})
	if strings.Join(names, ",") != "a,b,c" {
		// The cursor is an offset into this order, so an unsupported sort must
		// fall back to the default order rather than leave the source order.
		t.Fatalf("a sort on an enriched column must still order deterministically: %v", names)
	}
	orderNames(names, []string{"name"}, nil)
	if strings.Join(names, ",") != "a,b,c" {
		t.Fatalf("default order should be ascending: %v", names)
	}
	orderNames(names, []string{"name"}, []plugin.SortKey{{Field: "name", Desc: true}})
	if strings.Join(names, ",") != "c,b,a" {
		t.Fatalf("a supported descending sort must reverse the dataset: %v", names)
	}
}

func TestScanRowsPagesOneFixedSet(t *testing.T) {
	names := []string{"c", "a", "b"}
	build := func(name string) row { return row{"name": name} }

	page, err := scanRows(requestContext(nil, url.Values{"limit": {"2"}}), slices.Clone(names), true, []string{"name"}, build)
	if err != nil {
		t.Fatalf("scan rows: %v", err)
	}
	if !page.Truncated || page.ScanLimit != scanLimit {
		t.Fatalf("truncation must be reported with the cap that produced it: %+v", page)
	}
	if page.Page.Total == nil || *page.Page.Total != 3 {
		t.Fatalf("the total counts the rows that were loaded: %+v", page.Page.Total)
	}
	if page.Page.NextCursor != "2" || page.Page.Items[0]["name"] != "a" {
		t.Fatalf("a truncated walk still pages its own snapshot in order: %+v", page.Page)
	}

	last, err := scanRows(requestContext(nil, url.Values{"limit": {"2"}, "cursor": {"2"}}), slices.Clone(names), true, []string{"name"}, build)
	if err != nil {
		t.Fatalf("scan rows: %v", err)
	}
	if last.Page.NextCursor != "" {
		// A cursor past the snapshot would never resolve to new rows, so the
		// listing must stop instead of advertising an unreachable page.
		t.Fatalf("the last page of a snapshot must not advertise more rows: %q", last.Page.NextCursor)
	}

	full, err := scanRows(requestContext(nil, url.Values{"limit": {"10"}}), slices.Clone(names), false, []string{"name"}, build)
	if err != nil {
		t.Fatalf("scan rows: %v", err)
	}
	if full.Truncated || full.Page.NextCursor != "" || full.Page.Total == nil || *full.Page.Total != 3 {
		t.Fatalf("a complete walk reports an authoritative total and no next page: %+v", full)
	}

	filtered, err := scanRows(requestContext(nil, url.Values{"limit": {"10"}, "filter": {"a"}}), slices.Clone(names), true, []string{"name"}, build)
	if err != nil {
		t.Fatalf("scan rows: %v", err)
	}
	if len(filtered.Page.Items) != 1 || filtered.Page.NextCursor != "" {
		t.Fatalf("the search term must apply to the whole snapshot: %+v", filtered.Page)
	}
}

func TestSecretDataAcceptsBothRendererShapes(t *testing.T) {
	object, err := secretData(map[string]any{"user": "root"})
	if err != nil || object["user"] != "root" {
		t.Fatalf("object payload: %v %v", object, err)
	}
	text, err := secretData(`{"user":"root"}`)
	if err != nil || text["user"] != "root" {
		t.Fatalf("json text payload: %v %v", text, err)
	}
	for _, bad := range []any{nil, "", "not json", 42} {
		if _, err := secretData(bad); err == nil {
			t.Fatalf("expected an error for %#v", bad)
		}
	}
}

func TestParseVersions(t *testing.T) {
	got, err := parseVersions(" 3, 1 ,3")
	if err != nil {
		t.Fatalf("parse versions: %v", err)
	}
	if len(got) != 2 || got[0] != 3 || got[1] != 1 {
		t.Fatalf("duplicates should collapse while keeping order: %v", got)
	}
	for _, bad := range []string{"", "0", "-1", "latest"} {
		if _, err := parseVersions(bad); err == nil {
			t.Fatalf("expected an error for %q", bad)
		}
	}
}

func TestNamespaceHelpers(t *testing.T) {
	if got := normalizeNamespace("/admin/team/"); got != "admin/team" {
		t.Fatalf("normalizeNamespace = %q", got)
	}
	if got := joinNamespace("admin", "team/"); got != "admin/team" {
		t.Fatalf("joinNamespace = %q", got)
	}
	if got := joinNamespace("", "team/"); got != "team" {
		t.Fatalf("joinNamespace at root = %q", got)
	}
	if got := relativeNamespace("admin", "admin/team"); got != "team" {
		t.Fatalf("relativeNamespace = %q", got)
	}
	if got := relativeNamespace("admin", "other"); got != "other" {
		t.Fatalf("relativeNamespace outside the base = %q", got)
	}
	if got := normalizeFolder(" /app/ "); got != "app/" {
		t.Fatalf("normalizeFolder = %q", got)
	}
	if got := normalizeFolder("/"); got != "" {
		t.Fatalf("normalizeFolder at root = %q", got)
	}
}

func TestResolveMountPrefersTheLongestMount(t *testing.T) {
	srv, mock := newMockVault(t)
	defer srv.Close()
	mock.mounts["kv/"] = mountOutput{Type: "kv", Options: map[string]string{"version": "2"}}
	mock.mounts["kv/team/"] = mountOutput{Type: "kv", Options: map[string]string{"version": "2"}}

	s := newSession(t, srv)
	defer func() { _ = s.Close() }()
	client, _, err := s.clientFor(requestContext(nil, nil))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	m, rel, err := s.resolveMount(context.Background(), client, "", "kv/team/app/db")
	if err != nil {
		t.Fatalf("resolve mount: %v", err)
	}
	if m.Path != "kv/team/" || rel != "app/db" {
		t.Fatalf("nested mounts must resolve to the longest prefix: %q %q", m.Path, rel)
	}
	if _, _, err := s.resolveMount(context.Background(), client, "", "nothing/here"); err == nil {
		t.Fatal("an unmounted path must not resolve")
	}
}

func TestMountListReturnsACloneCallersMaySort(t *testing.T) {
	srv, _ := newMockVault(t)
	defer srv.Close()
	s := newSession(t, srv)
	defer func() { _ = s.Close() }()
	client, _, err := s.clientFor(requestContext(nil, nil))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	first, err := s.mountList(context.Background(), client, "", true)
	if err != nil {
		t.Fatalf("mount list: %v", err)
	}
	if len(first) < 2 {
		t.Fatalf("expected several mounts, got %d", len(first))
	}
	first[0], first[1] = first[1], first[0]
	second, err := s.mountList(context.Background(), client, "", false)
	if err != nil {
		t.Fatalf("cached mount list: %v", err)
	}
	if second[0].Path == first[0].Path {
		t.Fatal("mountList handed out the cached slice; reordering it corrupted the cache")
	}
}

func TestNamespaceScopeReachesEveryRequest(t *testing.T) {
	srv, mock := newMockVault(t)
	defer srv.Close()
	s := newSession(t, srv)
	defer func() { _ = s.Close() }()

	h := contribtest.NewHarness(t, routes())
	h.Call(context.Background(), "vault.mounts.list", s, map[string]string{"namespace": "admin/team"}, nil, nil)
	if got := mock.lastNamespace(); got != "admin/team" {
		t.Fatalf("scope filter not forwarded to vault: %q", got)
	}
	h.Call(context.Background(), "vault.mounts.list", s, nil, nil, nil)
	if got := mock.lastNamespace(); got != "" {
		t.Fatalf("connection namespace should apply when no scope is selected: %q", got)
	}
	if got := mock.lastToken(); got != "root" {
		t.Fatalf("vault token header not sent: %q", got)
	}
}

func TestRoutesAgainstMockVault(t *testing.T) {
	srv, mock := newMockVault(t)
	defer srv.Close()
	s := newSession(t, srv)
	defer func() { _ = s.Close() }()

	ctx := context.Background()
	h := contribtest.NewHarness(t, routes())
	key := map[string]string{"key": "secret/app/db"}

	systemRows := page(t, h.Call(ctx, "vault.system.list", s, nil, nil, nil))
	if len(systemRows.Items) != 1 || systemRows.Items[0]["state"] != "active" {
		t.Fatalf("unexpected system row: %#v", systemRows.Items)
	}
	health := object(t, h.Call(ctx, "vault.system.health", s, nil, nil, nil))
	if health["version"] != "1.20.1" || health["state"] != "active" {
		t.Fatalf("unexpected health: %#v", health)
	}
	seal := object(t, h.Call(ctx, "vault.system.seal", s, nil, nil, nil))
	if seal["state"] != "unsealed" || seal["type"] != "shamir" {
		t.Fatalf("unexpected seal status: %#v", seal)
	}

	mounts := page(t, h.Call(ctx, "vault.mounts.list", s, nil, nil, nil))
	if mounts.Total == nil || *mounts.Total != 3 {
		t.Fatalf("unexpected mount total: %#v", mounts.Total)
	}
	if !hasRow(mounts.Items, "path", "secret/") || !hasRow(mounts.Items, "path", "database/") {
		t.Fatalf("unexpected mounts: %#v", mounts.Items)
	}
	for _, item := range mounts.Items {
		if item["path"] == "secret/" && item["kv"] != true {
			t.Fatalf("kv v2 mount must be flagged for the secrets tab: %#v", item)
		}
		if item["path"] == "database/" && item["kv"] != false {
			t.Fatalf("a database engine is not a key/value store: %#v", item)
		}
	}
	mount := object(t, h.Call(ctx, "vault.mount.read", s, map[string]string{"mount": "secret"}, nil, nil))
	if mount["kv_version"] != 2 || mount["accessor"] != "kv_1" {
		t.Fatalf("unexpected mount detail: %#v", mount)
	}
	config := object(t, h.Call(ctx, "vault.mount.config", s, map[string]string{"mount": "secret/"}, nil, nil))
	if config["max_lease_ttl"] != 2764800 {
		t.Fatalf("unexpected mount config: %#v", config)
	}

	auth := page(t, h.Call(ctx, "vault.auth.list", s, nil, nil, nil))
	if len(auth.Items) != 2 {
		t.Fatalf("unexpected auth mounts: %#v", auth.Items)
	}
	authDetail := object(t, h.Call(ctx, "vault.auth.read", s, map[string]string{"mount": "approle"}, nil, nil))
	if authDetail["type"] != "approle" {
		t.Fatalf("unexpected auth detail: %#v", authDetail)
	}

	mountTree := treeNodes(t, h.Call(ctx, "vault.tree.mounts", s, nil, nil, nil))
	for _, node := range mountTree {
		switch node.Label {
		case "secret":
			if node.Leaf || node.ChildrenSource == nil {
				t.Fatalf("a KV mount must expand into its folders: %#v", node)
			}
		case "database":
			if !node.Leaf || node.ChildrenSource != nil {
				t.Fatalf("a non-KV mount has no browsable keyspace: %#v", node)
			}
		}
	}
	authTree := treeNodes(t, h.Call(ctx, "vault.tree.auth", s, nil, nil, nil))
	if len(authTree) != 2 || !authTree[0].Leaf {
		t.Fatalf("unexpected auth tree: %#v", authTree)
	}
	pathTree := treeNodes(t, h.Call(ctx, "vault.tree.paths", s, map[string]string{"mount": "secret/"}, nil, nil))
	if len(pathTree) != 1 || pathTree[0].Label != "app" || pathTree[0].ResourceKind != "secret" {
		t.Fatalf("the tree should stop at folders that open the secret list: %#v", pathTree)
	}

	secrets := scan(t, h.Call(ctx, "vault.secrets.list", s, map[string]string{"mount": "secret/"}, nil, nil))
	if secrets.Total == nil || *secrets.Total != 2 {
		t.Fatalf("the walk should reach both leaf secrets: %#v", secrets)
	}
	if !hasRow(secrets.Items, "key", "secret/app/db") || !hasRow(secrets.Items, "key", "secret/standalone") {
		t.Fatalf("unexpected secret keys: %#v", secrets.Items)
	}
	scoped := scan(t, h.Call(ctx, "vault.secrets.list", s, map[string]string{"mount": "secret/", "path": "app"}, nil, nil))
	if len(scoped.Items) != 1 || scoped.Items[0]["key"] != "secret/app/db" {
		t.Fatalf("folder-scoped listing should return one secret: %#v", scoped.Items)
	}

	secret := object(t, h.Call(ctx, "vault.secret.read", s, key, nil, nil))
	if secret["version"] != 3 || secret["kv_version"] != 2 {
		t.Fatalf("unexpected secret detail: %#v", secret)
	}
	if value, ok := secret["value"].(map[string]any); !ok || value["username"] != "root" {
		t.Fatalf("unexpected secret value: %#v", secret["value"])
	}

	written := object(t, h.Call(ctx, "vault.secret.write", s, key, nil, jsonBody(t, map[string]any{"value": `{"username":"admin"}`})))
	if written["version"] != 4 {
		t.Fatalf("the write response should carry the new version: %#v", written)
	}
	if got := mock.lastWrite("secret/data/app/db"); got["username"] != "admin" {
		t.Fatalf("KV panel payload not unwrapped into secret data: %#v", got)
	}
	h.Call(ctx, "vault.secret.put", s, nil, nil, jsonBody(t, map[string]any{"key": "secret/app/db", "data": map[string]any{"username": "form"}, "cas": 4}))
	if got := mock.lastWrite("secret/data/app/db"); got["username"] != "form" {
		t.Fatalf("form payload not written: %#v", got)
	}

	versions := page(t, h.Call(ctx, "vault.secret.versions", s, key, nil, nil))
	if len(versions.Items) != 3 {
		t.Fatalf("unexpected versions: %#v", versions.Items)
	}
	states := map[string]any{}
	for _, item := range versions.Items {
		states[fmt.Sprint(item["version"])] = item["state"]
	}
	if states["1"] != "active" || states["2"] != "deleted" || states["3"] != "destroyed" {
		t.Fatalf("version states not derived: %#v", states)
	}
	meta := object(t, h.Call(ctx, "vault.secret.metadata", s, key, nil, nil))
	if meta["current_version"] != 3 || meta["max_versions"] != 10 {
		t.Fatalf("unexpected metadata: %#v", meta)
	}

	h.Call(ctx, "vault.secret.delete", s, key, nil, nil)
	h.Call(ctx, "vault.secret.versions.delete", s, key, nil, jsonBody(t, map[string]any{"versions": "2"}))
	if got := mock.lastWrite("secret/delete/app/db"); fmt.Sprint(got["versions"]) != "[2]" {
		t.Fatalf("a version-scoped soft delete must post the version list: %#v", got)
	}
	h.Call(ctx, "vault.secret.undelete", s, key, nil, jsonBody(t, map[string]any{"versions": "2"}))
	h.Call(ctx, "vault.secret.destroy", s, key, nil, jsonBody(t, map[string]any{"versions": "1,2"}))
	h.Call(ctx, "vault.secret.purge", s, key, nil, nil)

	policies := page(t, h.Call(ctx, "vault.policies.list", s, nil, nil, nil))
	if policies.Total == nil || *policies.Total != 3 {
		t.Fatalf("unexpected policy total: %#v", policies.Total)
	}
	for _, item := range policies.Items {
		if item["name"] == "root" && item["builtin"] != true {
			t.Fatalf("root is a built-in policy: %#v", item)
		}
	}
	policy := h.Call(ctx, "vault.policy.read", s, map[string]string{"name": "app-readonly"}, nil, nil)
	hcl, ok := policy.(string)
	if !ok || !strings.Contains(hcl, "capabilities") {
		t.Fatalf("the code editor needs raw HCL text, got %#v", policy)
	}
	saved := object(t, h.Call(ctx, "vault.policy.write", s, map[string]string{"name": "app-readonly"}, nil, jsonBody(t, map[string]any{"content": "path \"secret/*\" { capabilities = [\"list\"] }"})))
	if _, ok := saved["content"].(string); !ok {
		t.Fatalf("the editor resets its baseline from the save response: %#v", saved)
	}
	h.Call(ctx, "vault.policy.create", s, nil, nil, jsonBody(t, map[string]any{"name": "new-policy", "policy": "path \"secret/*\" {}"}))
	h.Call(ctx, "vault.policy.delete", s, map[string]string{"name": "app-readonly"}, nil, nil)

	tokens := page(t, h.Call(ctx, "vault.tokens.list", s, nil, nil, nil))
	if tokens.Total == nil || *tokens.Total != 2 {
		t.Fatalf("unexpected accessor total: %#v", tokens.Total)
	}
	if tokens.Items[0]["display_name"] != "token" {
		t.Fatalf("accessor lookups should enrich the page: %#v", tokens.Items[0])
	}
	token := object(t, h.Call(ctx, "vault.token.read", s, map[string]string{"accessor": "acc-aaa"}, nil, nil))
	if token["accessor"] != "acc-aaa" || token["path"] != "auth/token/create" {
		t.Fatalf("unexpected token detail: %#v", token)
	}
	created := object(t, h.Call(ctx, "vault.token.create", s, nil, nil, jsonBody(t, map[string]any{"display_name": "ci", "policies": "default, app-readonly", "ttl": "1h", "num_uses": 3})))
	if created["accessor"] != "acc-new" {
		t.Fatalf("unexpected token creation result: %#v", created)
	}
	if got := mock.lastWrite("auth/token/create"); fmt.Sprint(got["policies"]) != "[default app-readonly]" {
		t.Fatalf("policies not split: %#v", got["policies"])
	}
	h.Call(ctx, "vault.token.revoke", s, map[string]string{"accessor": "acc-bbb"}, nil, nil)

	leases := scan(t, h.Call(ctx, "vault.leases.list", s, nil, nil, nil))
	if leases.Total == nil || *leases.Total != 3 {
		t.Fatalf("the lease walk should flatten the folder tree: %#v", leases)
	}
	if !hasRow(leases.Items, "id", "database/creds/readonly-1") {
		t.Fatalf("nested lease ids should keep their full prefix: %#v", leases.Items)
	}
	lease := object(t, h.Call(ctx, "vault.lease.read", s, map[string]string{"lease_id": "database/creds/readonly-1"}, nil, nil))
	if lease["renewable"] != true {
		t.Fatalf("unexpected lease detail: %#v", lease)
	}
	h.Call(ctx, "vault.lease.revoke", s, map[string]string{"lease_id": "database/creds/readonly-1"}, nil, nil)

	namespaces := page(t, h.Call(ctx, "vault.namespaces.list", s, nil, nil, nil))
	if len(namespaces.Items) != 1 || namespaces.Items[0]["path"] != "team" {
		t.Fatalf("unexpected namespaces: %#v", namespaces.Items)
	}
	namespace := object(t, h.Call(ctx, "vault.namespace.read", s, map[string]string{"path": "team"}, nil, nil))
	if namespace["id"] != "ns123" {
		t.Fatalf("unexpected namespace detail: %#v", namespace)
	}

	audit := page(t, h.Call(ctx, "vault.audit.list", s, nil, nil, nil))
	if len(audit.Items) != 1 || audit.Items[0]["type"] != "file" {
		t.Fatalf("unexpected audit devices: %#v", audit.Items)
	}
	device := object(t, h.Call(ctx, "vault.audit.read", s, map[string]string{"path": "file"}, nil, nil))
	if device["path"] != "file/" {
		t.Fatalf("unexpected audit device: %#v", device)
	}

	h.AssertAllCovered()
}

func TestKVv1EngineDegradesGracefully(t *testing.T) {
	srv, _ := newMockVault(t)
	defer srv.Close()
	s := newSession(t, srv)
	defer func() { _ = s.Close() }()

	ctx := context.Background()
	key := map[string]string{"key": "kv1/legacy"}
	secret, err := readSecret(plugin.NewRequestContext(ctx, plugin.User{}, s, key, nil, nil))
	if err != nil {
		t.Fatalf("read kv v1 secret: %v", err)
	}
	value := secret.(row)["value"].(map[string]any)
	if value["token"] != "legacy" {
		t.Fatalf("unexpected kv v1 value: %#v", value)
	}
	versions, err := listSecretVersions(plugin.NewRequestContext(ctx, plugin.User{}, s, key, nil, nil))
	if err != nil {
		t.Fatalf("kv v1 versions: %v", err)
	}
	if items := versions.(plugin.Page[row]).Items; len(items) != 0 {
		t.Fatalf("kv v1 keeps no versions: %#v", items)
	}
	if _, err := readSecretMetadata(plugin.NewRequestContext(ctx, plugin.User{}, s, key, nil, nil)); err == nil {
		t.Fatal("kv v1 metadata should report that it is unsupported")
	}
	for _, op := range []versionOp{opDeleteVersions, opUndeleteVersions, opDestroyVersions} {
		if _, err := versionOperation(plugin.NewRequestContext(ctx, plugin.User{}, s, key, nil, jsonBody(t, map[string]any{"versions": "1"})), op); err == nil {
			t.Fatalf("kv v1 has no %q version operation", op)
		}
	}
}

func TestSecretRoutesRejectNonKVEnginesAndBadInput(t *testing.T) {
	srv, _ := newMockVault(t)
	defer srv.Close()
	s := newSession(t, srv)
	defer func() { _ = s.Close() }()
	ctx := context.Background()

	if _, err := readSecret(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"key": "database/creds/x"}, nil, nil)); err == nil {
		t.Fatal("a database engine is not browsable as key/value")
	}
	if _, err := readSecret(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"key": "secret/"}, nil, nil)); err == nil {
		t.Fatal("a mount root is not a secret")
	}
	if _, err := readSecret(plugin.NewRequestContext(ctx, plugin.User{}, s, nil, nil, nil)); err == nil {
		t.Fatal("a missing key must be rejected")
	}
	empty, err := listSecrets(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"mount": "database"}, nil, nil))
	if err != nil {
		t.Fatalf("listing a non-KV engine should be empty, not an error: %v", err)
	}
	if items := empty.(plugin.Page[row]).Items; len(items) != 0 {
		t.Fatalf("expected no rows for a non-KV engine: %#v", items)
	}
}

func TestReadOnlySessionBlocksEveryMutation(t *testing.T) {
	srv, _ := newMockVault(t)
	defer srv.Close()
	s := newSession(t, srv)
	defer func() { _ = s.Close() }()
	s.opts.ReadOnly = true

	ctx := context.Background()
	mutations := map[string]struct {
		handler plugin.Handler
		params  map[string]string
		body    []byte
	}{
		"vault.secret.write":           {writeSecret, map[string]string{"key": "secret/app/db"}, jsonBody(t, map[string]any{"value": `{"a":"b"}`})},
		"vault.secret.put":             {putSecret, nil, jsonBody(t, map[string]any{"key": "secret/app/db", "data": map[string]any{"a": "b"}})},
		"vault.secret.delete":          {deleteSecret, map[string]string{"key": "secret/app/db"}, nil},
		"vault.secret.versions.delete": {deleteSecretVersions, map[string]string{"key": "secret/app/db"}, jsonBody(t, map[string]any{"versions": "1"})},
		"vault.secret.undelete":        {undeleteSecret, map[string]string{"key": "secret/app/db"}, jsonBody(t, map[string]any{"versions": "1"})},
		"vault.secret.destroy":         {destroySecret, map[string]string{"key": "secret/app/db"}, jsonBody(t, map[string]any{"versions": "1"})},
		"vault.secret.purge":           {purgeSecret, map[string]string{"key": "secret/app/db"}, nil},
		"vault.policy.write":           {writePolicy, map[string]string{"name": "app-readonly"}, jsonBody(t, map[string]any{"content": "x"})},
		"vault.policy.create":          {createPolicy, nil, jsonBody(t, map[string]any{"name": "n", "policy": "p"})},
		"vault.policy.delete":          {deletePolicy, map[string]string{"name": "app-readonly"}, nil},
		"vault.token.create":           {createToken, nil, jsonBody(t, map[string]any{})},
		"vault.token.revoke":           {revokeToken, map[string]string{"accessor": "acc-aaa"}, nil},
		"vault.lease.revoke":           {revokeLease, map[string]string{"lease_id": "x"}, nil},
	}
	for id, mutation := range mutations {
		if _, err := mutation.handler(plugin.NewRequestContext(ctx, plugin.User{}, s, mutation.params, nil, mutation.body)); err == nil {
			t.Fatalf("%s must be blocked in read-only mode", id)
		}
	}
}

func TestSecretListingPagesOneSnapshotUntilItIsWrittenTo(t *testing.T) {
	srv, mock := newMockVault(t)
	defer srv.Close()
	s := newSession(t, srv)
	defer func() { _ = s.Close() }()

	ctx := context.Background()
	list := func(query url.Values) scanPage {
		t.Helper()
		out, err := listSecrets(plugin.NewRequestContext(ctx, plugin.User{}, s, map[string]string{"mount": "secret/"}, query, nil))
		if err != nil {
			t.Fatalf("list secrets: %v", err)
		}
		return out.(scanPage)
	}

	first := list(url.Values{"limit": {"1"}})
	if len(first.Items) != 1 || first.Items[0]["key"] != "secret/app/db" || first.NextCursor != "1" {
		t.Fatalf("unexpected first page: %#v", first)
	}
	walks := mock.count("secret/metadata")
	second := list(url.Values{"limit": {"1"}, "cursor": {"1"}})
	if len(second.Items) != 1 || second.Items[0]["key"] != "secret/standalone" || second.NextCursor != "" {
		t.Fatalf("unexpected second page: %#v", second)
	}
	if got := mock.count("secret/metadata"); got != walks {
		// Vault has no LIST paging: re-walking per page would cost one request
		// per folder every time the cursor moves, and would page a set that can
		// change shape underneath the offset.
		t.Fatalf("paging re-walked the keyspace: %d listings, want %d", got, walks)
	}

	if _, err := storeSecret(plugin.NewRequestContext(ctx, plugin.User{}, s, nil, nil, nil), "secret/app/new", map[string]any{"a": "b"}, nil); err != nil {
		t.Fatalf("write secret: %v", err)
	}
	list(url.Values{"limit": {"1"}})
	if got := mock.count("secret/metadata"); got <= walks {
		t.Fatalf("a write must invalidate the walk snapshot: %d listings, want more than %d", got, walks)
	}
}

func TestAuditDevicesAreOrderedBeforeTheyArePaged(t *testing.T) {
	srv, mock := newMockVault(t)
	defer srv.Close()
	mock.audit = map[string]any{
		"syslog/": map[string]any{"type": "syslog", "path": "syslog/"},
		"file/":   map[string]any{"type": "file", "path": "file/"},
		"raw/":    map[string]any{"type": "file", "path": "raw/"},
	}
	s := newSession(t, srv)
	defer func() { _ = s.Close() }()

	// sys/audit answers a Go map, so an unordered listing would hand out a
	// different row for the same offset on every request.
	for range 8 {
		out, err := listAudit(plugin.NewRequestContext(context.Background(), plugin.User{}, s, nil, url.Values{"limit": {"1"}}, nil))
		if err != nil {
			t.Fatalf("list audit: %v", err)
		}
		items := out.(plugin.Page[row]).Items
		if len(items) != 1 || items[0]["path"] != "file/" {
			t.Fatalf("audit paging is not stable: %#v", items)
		}
	}
}

func TestMountRoutesRejectAMissingMountPath(t *testing.T) {
	srv, _ := newMockVault(t)
	defer srv.Close()
	s := newSession(t, srv)
	defer func() { _ = s.Close() }()
	for name, handler := range map[string]plugin.Handler{
		"vault.mount.read":   readMount,
		"vault.mount.config": readMountConfig,
		"vault.secrets.list": listSecrets,
		"vault.tree.paths":   treePaths,
	} {
		_, err := handler(plugin.NewRequestContext(context.Background(), plugin.User{}, s, nil, nil, nil))
		if !errors.Is(err, plugin.ErrInvalidInput) {
			t.Fatalf("%s without a mount should be rejected as bad input, got %v", name, err)
		}
	}
}

func TestBuiltInPoliciesCannotBeDeleted(t *testing.T) {
	srv, _ := newMockVault(t)
	defer srv.Close()
	s := newSession(t, srv)
	defer func() { _ = s.Close() }()
	for _, name := range []string{"root", "default"} {
		_, err := deletePolicy(plugin.NewRequestContext(context.Background(), plugin.User{}, s, map[string]string{"name": name}, nil, nil))
		if err == nil {
			t.Fatalf("%s must not be deletable", name)
		}
	}
}

func TestNamespacesDegradeOnCommunityEdition(t *testing.T) {
	srv, mock := newMockVault(t)
	defer srv.Close()
	mock.enterprise = false
	s := newSession(t, srv)
	defer func() { _ = s.Close() }()
	out, err := listNamespaces(plugin.NewRequestContext(context.Background(), plugin.User{}, s, nil, nil, nil))
	if err != nil {
		t.Fatalf("community edition should report no namespaces, not an error: %v", err)
	}
	if items := out.(plugin.Page[row]).Items; len(items) != 0 {
		t.Fatalf("expected no namespaces: %#v", items)
	}
}

func TestVaultErrorsMapToSDKSentinels(t *testing.T) {
	cases := map[int]error{
		http.StatusBadRequest:          plugin.ErrInvalidInput,
		http.StatusUnauthorized:        plugin.ErrUnauthorized,
		http.StatusForbidden:           plugin.ErrForbidden,
		http.StatusNotFound:            plugin.ErrNotFound,
		http.StatusMethodNotAllowed:    plugin.ErrNotSupported,
		http.StatusConflict:            plugin.ErrConflict,
		http.StatusInternalServerError: plugin.ErrUnavailable,
	}
	for status, want := range cases {
		err := vaultErr(&vaultapi.ResponseError{
			StatusCode: status,
			URL:        "https://vault.internal/v1/secret/data/app/db",
			Errors:     []string{"permission denied"},
		})
		if !errors.Is(err, want) {
			t.Fatalf("status %d mapped to %v, want %v", status, err, want)
		}
		if strings.Contains(err.Error(), "vault.internal") {
			// The URL carries the secret path, so it must not reach the client or audit log.
			t.Fatalf("status %d leaked the request URL: %v", status, err)
		}
		if !strings.Contains(err.Error(), "permission denied") {
			t.Fatalf("status %d dropped the upstream message: %v", status, err)
		}
	}
	// An empty error list must not fall through to ResponseError.Error(), which
	// prints the request URL and with it the secret path.
	silent := vaultErr(&vaultapi.ResponseError{StatusCode: http.StatusForbidden, URL: "https://vault.internal/v1/secret/data/app/db"})
	if !errors.Is(silent, plugin.ErrForbidden) || strings.Contains(silent.Error(), "vault.internal") {
		t.Fatalf("an empty vault error leaked its URL: %v", silent)
	}
	if err := vaultErr(fmt.Errorf("%w: at secret/data/app", vaultapi.ErrSecretNotFound)); !errors.Is(err, plugin.ErrNotFound) {
		t.Fatalf("a missing secret should map to not found, got %v", err)
	}
	if err := vaultErr(context.Canceled); !errors.Is(err, plugin.ErrUnavailable) {
		t.Fatalf("a cancelled request should map to unavailable, got %v", err)
	}
	if err := vaultErr(nil); err != nil {
		t.Fatalf("nil must stay nil, got %v", err)
	}
}

func TestSessionClosesAndRejectsFurtherWork(t *testing.T) {
	srv, _ := newMockVault(t)
	defer srv.Close()
	s := newSession(t, srv)
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, _, err := s.clientFor(requestContext(nil, nil)); err == nil {
		t.Fatal("a closed session must not hand out clients")
	}
	if err := s.HealthCheck(context.Background()); err == nil {
		t.Fatal("a closed session is not healthy")
	}
	if _, err := s.OpenChannel(context.Background(), plugin.ChannelRequest{}); err == nil {
		t.Fatal("vault has no streaming channels")
	}
}

func TestConnectRejectsMissingTransport(t *testing.T) {
	_, err := connect(context.Background(), plugin.ConnectConfig{Config: map[string]any{"host": "127.0.0.1", "auth": authToken, "token": "t"}})
	if err == nil {
		t.Fatal("connect must require the gateway transport")
	}
}

func TestConnectAuthenticatesWithAppRole(t *testing.T) {
	srv, mock := newMockVault(t)
	defer srv.Close()
	host, port := hostPort(t, srv)
	sess, err := connect(context.Background(), plugin.ConnectConfig{
		Config: map[string]any{
			"host": host, "port": port, "tls_mode": tlsDisable,
			"auth": authAppRole, "approle_path": "approle", "read_only": false,
		},
		Credentials: plugin.NewResolvedCredentials(plugin.CredentialBinding{
			Field:      approleCredField,
			Credential: plugin.ResolvedCredential{Kind: credentialKindAppRole, Values: map[string]string{"role_id": "role-1", "secret_id": "secret-1"}},
		}),
		Net: plugintest.DirectTransport(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	if got := mock.lastWrite("auth/approle/login"); got["role_id"] != "role-1" {
		t.Fatalf("AppRole login payload: %#v", got)
	}
	if got := mock.lastToken(); got != "hvs.approle" {
		t.Fatalf("the login token should authenticate later calls, got %q", got)
	}
}

// helpers

func newSession(t *testing.T, srv *httptest.Server) *Session {
	t.Helper()
	host, port := hostPort(t, srv)
	sess, err := connect(context.Background(), plugin.ConnectConfig{
		Config: map[string]any{
			"host": host, "port": port, "tls_mode": tlsDisable,
			"auth": authToken, "token": "root", "read_only": false, "timeout": "10s",
		},
		Net: plugintest.DirectTransport(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return sess.(*Session)
}

func hostPort(t *testing.T, srv *httptest.Server) (string, int) {
	t.Helper()
	parsed, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse test server URL: %v", err)
	}
	host, rawPort, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatalf("split test server host: %v", err)
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil {
		t.Fatalf("parse test server port: %v", err)
	}
	return host, port
}

func requestContext(params map[string]string, query url.Values) *plugin.RequestContext {
	return plugin.NewRequestContext(context.Background(), plugin.User{}, nil, params, query, nil)
}

func jsonBody(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	return raw
}

func page(t *testing.T, out any) plugin.Page[row] {
	t.Helper()
	p, ok := out.(plugin.Page[row])
	if !ok {
		t.Fatalf("expected a page of rows, got %T", out)
	}
	return p
}

func scan(t *testing.T, out any) scanPage {
	t.Helper()
	p, ok := out.(scanPage)
	if !ok {
		t.Fatalf("expected a scanned page, got %T", out)
	}
	return p
}

func object(t *testing.T, out any) row {
	t.Helper()
	o, ok := out.(row)
	if !ok {
		t.Fatalf("expected an object, got %T", out)
	}
	return o
}

func treeNodes(t *testing.T, out any) []plugin.TreeNode {
	t.Helper()
	p, ok := out.(plugin.Page[plugin.TreeNode])
	if !ok {
		t.Fatalf("expected a page of tree nodes, got %T", out)
	}
	return p.Items
}

func hasRow(rows []row, key string, want any) bool {
	for _, item := range rows {
		if fmt.Sprint(item[key]) == fmt.Sprint(want) {
			return true
		}
	}
	return false
}

// mock vault

type mountOutput struct {
	Type        string            `json:"type"`
	Description string            `json:"description,omitempty"`
	Accessor    string            `json:"accessor,omitempty"`
	UUID        string            `json:"uuid,omitempty"`
	Options     map[string]string `json:"options,omitempty"`
	Local       bool              `json:"local"`
	Config      map[string]any    `json:"config"`
}

type mockVault struct {
	mu         sync.Mutex
	namespace  string
	token      string
	writes     map[string]map[string]any
	requests   map[string]int
	mounts     map[string]mountOutput
	audit      map[string]any
	enterprise bool
}

func newMockVault(t *testing.T) (*httptest.Server, *mockVault) {
	t.Helper()
	mock := &mockVault{
		writes:     map[string]map[string]any{},
		requests:   map[string]int{},
		enterprise: true,
		audit: map[string]any{
			"file/": map[string]any{"type": "file", "description": "file audit", "local": false, "path": "file/", "options": map[string]string{"file_path": "/vault/logs/audit.log"}},
		},
		mounts: map[string]mountOutput{
			"secret/":   {Type: "kv", Description: "kv v2", Accessor: "kv_1", UUID: "u1", Options: map[string]string{"version": "2"}, Config: map[string]any{"default_lease_ttl": 0, "max_lease_ttl": 2764800}},
			"kv1/":      {Type: "kv", Description: "kv v1", Accessor: "kv_2", UUID: "u2", Options: map[string]string{"version": "1"}, Config: map[string]any{}},
			"database/": {Type: "database", Description: "dynamic creds", Accessor: "db_1", UUID: "u3", Config: map[string]any{}},
		},
	}
	srv := httptest.NewServer(mock)
	t.Cleanup(srv.Close)
	return srv, mock
}

func (m *mockVault) lastNamespace() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.namespace
}

func (m *mockVault) lastToken() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.token
}

func (m *mockVault) lastWrite(path string) map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.writes[path]
}

// count reports how many requests reached one path prefix, so a test can assert
// that paging a listing does not re-issue the walk behind it.
func (m *mockVault) count(prefix string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	total := 0
	for path, seen := range m.requests {
		if strings.HasPrefix(path, prefix) {
			total += seen
		}
	}
	return total
}

func (m *mockVault) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/")
	m.mu.Lock()
	m.namespace = r.Header.Get("X-Vault-Namespace")
	m.token = r.Header.Get("X-Vault-Token")
	m.requests[path]++
	m.mu.Unlock()

	var body map[string]any
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&body)
	}
	if body != nil {
		m.mu.Lock()
		m.writes[path] = body
		m.mu.Unlock()
	}
	list := r.URL.Query().Get("list") == "true"

	if status, ok := strings.CutPrefix(path, "sys/status/"); ok {
		code, _ := strconv.Atoi(status)
		writeJSON(w, code, map[string]any{"errors": []string{"synthetic " + status}})
		return
	}

	switch {
	case path == "sys/health":
		writeJSON(w, http.StatusOK, map[string]any{
			"initialized": true, "sealed": false, "standby": false, "performance_standby": false,
			"version": "1.20.1", "cluster_name": "vault-cluster-test", "cluster_id": "cid",
			"server_time_utc": 1767225600, "enterprise": m.enterprise,
			"replication_performance_mode": "disabled", "replication_dr_mode": "disabled",
		})
	case path == "sys/seal-status":
		writeJSON(w, http.StatusOK, map[string]any{
			"type": "shamir", "initialized": true, "sealed": false, "t": 3, "n": 5,
			"progress": 0, "version": "1.20.1", "storage_type": "raft", "cluster_name": "vault-cluster-test",
		})
	case path == "auth/token/lookup-self":
		writeData(w, map[string]any{"display_name": "root", "policies": []string{"root"}})
	case path == "auth/approle/login":
		writeJSON(w, http.StatusOK, map[string]any{"auth": map[string]any{"client_token": "hvs.approle", "accessor": "acc-approle", "policies": []string{"default"}, "lease_duration": 3600, "renewable": true}})
	case path == "sys/mounts":
		writeData(w, m.mountData())
	case path == "sys/auth":
		writeData(w, map[string]any{
			"token/":   map[string]any{"type": "token", "description": "token based credentials", "accessor": "auth_token_1", "uuid": "a1", "config": map[string]any{"default_lease_ttl": 0, "max_lease_ttl": 0, "token_type": "default-service"}},
			"approle/": map[string]any{"type": "approle", "description": "approle", "accessor": "auth_approle_1", "uuid": "a2", "config": map[string]any{}},
		})
	case path == "sys/audit":
		m.mu.Lock()
		devices := m.audit
		m.mu.Unlock()
		writeData(w, devices)
	case path == "sys/policies/acl" && list:
		writeData(w, map[string]any{"keys": []string{"default", "root", "app-readonly"}})
	case strings.HasPrefix(path, "sys/policies/acl/"):
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusNoContent, nil)
			return
		}
		writeData(w, map[string]any{"name": strings.TrimPrefix(path, "sys/policies/acl/"), "policy": "path \"secret/data/app/*\" {\n  capabilities = [\"read\"]\n}\n"})
	case path == "auth/token/accessors" && list:
		writeData(w, map[string]any{"keys": []string{"acc-aaa", "acc-bbb"}})
	case path == "auth/token/lookup-accessor":
		writeData(w, map[string]any{
			"accessor": fmt.Sprint(body["accessor"]), "display_name": "token", "policies": []string{"default"},
			"ttl": 3600, "num_uses": 0, "orphan": false, "renewable": true, "type": "service",
			"issue_time": "2026-07-01T00:00:00Z", "expire_time": "2026-07-02T00:00:00Z", "path": "auth/token/create",
		})
	case path == "auth/token/create":
		writeJSON(w, http.StatusOK, map[string]any{"auth": map[string]any{"client_token": "hvs.created", "accessor": "acc-new", "policies": []string{"default"}, "lease_duration": 3600, "renewable": true}})
	case path == "auth/token/revoke-accessor":
		writeJSON(w, http.StatusNoContent, nil)
	case path == "sys/leases/revoke":
		writeJSON(w, http.StatusNoContent, nil)
	case path == "sys/leases/lookup" && !list:
		writeData(w, map[string]any{
			"id": fmt.Sprint(body["lease_id"]), "issue_time": "2026-07-01T00:00:00Z",
			"expire_time": "2026-07-01T01:00:00Z", "last_renewal": nil, "renewable": true, "ttl": 3600,
		})
	// Vault re-appends the trailing slash a list operation needs, and the Go
	// client's path.Join strips it, so a folder walk arrives without one.
	case strings.HasPrefix(path, "sys/leases/lookup") && list:
		switch strings.Trim(strings.TrimPrefix(path, "sys/leases/lookup"), "/") {
		case "":
			writeData(w, map[string]any{"keys": []string{"database/", "token-1"}})
		case "database":
			writeData(w, map[string]any{"keys": []string{"creds/"}})
		case "database/creds":
			writeData(w, map[string]any{"keys": []string{"readonly-1", "readonly-2"}})
		default:
			writeJSON(w, http.StatusNotFound, nil)
		}
	case path == "sys/namespaces" && list:
		if !m.enterprise {
			writeJSON(w, http.StatusNotFound, map[string]any{"errors": []string{"unsupported path"}})
			return
		}
		writeData(w, map[string]any{"keys": []string{"team/"}})
	case strings.HasPrefix(path, "sys/namespaces/"):
		writeData(w, map[string]any{"id": "ns123", "path": strings.TrimPrefix(path, "sys/namespaces/") + "/", "custom_metadata": map[string]any{}})
	case path == "secret/metadata" && list:
		writeData(w, map[string]any{"keys": []string{"app/", "standalone"}})
	case path == "secret/metadata/app" && list:
		writeData(w, map[string]any{"keys": []string{"db"}})
	case strings.HasPrefix(path, "secret/metadata/"):
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusNoContent, nil)
			return
		}
		writeData(w, map[string]any{
			"cas_required": false, "created_time": "2026-07-01T10:00:00Z", "current_version": 3,
			"custom_metadata": map[string]any{"owner": "team"}, "delete_version_after": "0s",
			"max_versions": 10, "oldest_version": 1, "updated_time": "2026-07-02T10:00:00Z",
			"versions": map[string]any{
				"1": map[string]any{"created_time": "2026-07-01T10:00:00Z", "deletion_time": "", "destroyed": false},
				"2": map[string]any{"created_time": "2026-07-01T11:00:00Z", "deletion_time": "2026-07-01T12:00:00Z", "destroyed": false},
				"3": map[string]any{"created_time": "2026-07-02T10:00:00Z", "deletion_time": "", "destroyed": true},
			},
		})
	case strings.HasPrefix(path, "secret/data/"):
		switch r.Method {
		case http.MethodGet:
			writeData(w, map[string]any{
				"data": map[string]any{"username": "root", "password": "s3cr3t"},
				"metadata": map[string]any{
					"version": 3, "created_time": "2026-07-02T10:00:00Z", "deletion_time": "",
					"destroyed": false, "custom_metadata": map[string]any{"owner": "team"},
				},
			})
		case http.MethodPut, http.MethodPost:
			if data, ok := body["data"].(map[string]any); ok {
				m.mu.Lock()
				m.writes[path] = data
				m.mu.Unlock()
			}
			writeData(w, map[string]any{"version": 4, "created_time": "2026-07-03T10:00:00Z", "deletion_time": "", "destroyed": false})
		default:
			writeJSON(w, http.StatusNoContent, nil)
		}
	case strings.HasPrefix(path, "secret/delete/"), strings.HasPrefix(path, "secret/undelete/"), strings.HasPrefix(path, "secret/destroy/"):
		writeJSON(w, http.StatusNoContent, nil)
	case path == "kv1" && list:
		writeData(w, map[string]any{"keys": []string{"legacy"}})
	case strings.HasPrefix(path, "kv1/"):
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusNoContent, nil)
			return
		}
		writeData(w, map[string]any{"token": "legacy"})
	default:
		writeJSON(w, http.StatusNotFound, map[string]any{"errors": []string{"no handler for " + r.Method + " " + path}})
	}
}

func (m *mockVault) mountData() map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]any{}
	for path, mount := range m.mounts {
		out[path] = mount
	}
	return out
}

func writeData(w http.ResponseWriter, data map[string]any) {
	writeJSON(w, http.StatusOK, map[string]any{"data": data})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if body != nil {
		_ = json.NewEncoder(w).Encode(body)
	}
}
