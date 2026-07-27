package vault

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	vaultapi "github.com/hashicorp/vault/api"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

type row = plugin.TableRow

type actionResult struct {
	OK bool `json:"ok"`
}

func routes() []plugin.Route {
	return []plugin.Route{
		{ID: "vault.system.list", Method: plugin.MethodGet, Path: "/system", Permission: "vault.read", Risk: plugin.RiskSafe, AuditEvent: "vault.system.list", Handle: systemList},
		{ID: "vault.system.health", Method: plugin.MethodGet, Path: "/system/health", Permission: "vault.read", Risk: plugin.RiskSafe, AuditEvent: "vault.system.health", Handle: systemHealth},
		{ID: "vault.system.seal", Method: plugin.MethodGet, Path: "/system/seal", Permission: "vault.read", Risk: plugin.RiskSafe, AuditEvent: "vault.system.seal", Handle: systemSeal},

		{ID: "vault.tree.mounts", Method: plugin.MethodGet, Path: "/tree/mounts", Permission: "vault.read", Risk: plugin.RiskSafe, AuditEvent: "vault.tree.mounts", Handle: treeMounts},
		{ID: "vault.tree.auth", Method: plugin.MethodGet, Path: "/tree/auth", Permission: "vault.read", Risk: plugin.RiskSafe, AuditEvent: "vault.tree.auth", Handle: treeAuth},
		{ID: "vault.tree.paths", Method: plugin.MethodGet, Path: "/tree/paths", Permission: "vault.secrets.read", Risk: plugin.RiskSafe, AuditEvent: "vault.tree.paths", Handle: treePaths},

		{ID: "vault.mounts.list", Method: plugin.MethodGet, Path: "/mounts", Permission: "vault.read", Risk: plugin.RiskSafe, AuditEvent: "vault.mounts.list", Handle: listMounts},
		{ID: "vault.mount.read", Method: plugin.MethodGet, Path: "/mounts/detail", Permission: "vault.read", Risk: plugin.RiskSafe, AuditEvent: "vault.mount.read", Handle: readMount},
		{ID: "vault.mount.config", Method: plugin.MethodGet, Path: "/mounts/config", Permission: "vault.read", Risk: plugin.RiskSafe, AuditEvent: "vault.mount.config", Handle: readMountConfig},

		{ID: "vault.auth.list", Method: plugin.MethodGet, Path: "/auth", Permission: "vault.read", Risk: plugin.RiskSafe, AuditEvent: "vault.auth.list", Handle: listAuth},
		{ID: "vault.auth.read", Method: plugin.MethodGet, Path: "/auth/detail", Permission: "vault.read", Risk: plugin.RiskSafe, AuditEvent: "vault.auth.read", Handle: readAuth},

		{ID: "vault.secrets.list", Method: plugin.MethodGet, Path: "/secrets", Permission: "vault.secrets.read", Risk: plugin.RiskSafe, AuditEvent: "vault.secrets.list", Handle: listSecrets},
		{ID: "vault.secret.read", Method: plugin.MethodGet, Path: "/secrets/detail", Permission: "vault.secrets.read", Risk: plugin.RiskSafe, AuditEvent: "vault.secret.read", Handle: readSecret},
		{ID: "vault.secret.write", Method: plugin.MethodPut, Path: "/secrets", Permission: "vault.secrets.write", Risk: plugin.RiskWrite, AuditEvent: "vault.secret.write", Handle: writeSecret},
		{ID: "vault.secret.put", Method: plugin.MethodPost, Path: "/secrets", Permission: "vault.secrets.write", Risk: plugin.RiskWrite, AuditEvent: "vault.secret.put", Input: secretPutSchema(), Handle: putSecret},
		{ID: "vault.secret.delete", Method: plugin.MethodDelete, Path: "/secrets", Permission: "vault.secrets.delete", Risk: plugin.RiskDestructive, AuditEvent: "vault.secret.delete", Handle: deleteSecret},
		{ID: "vault.secret.versions", Method: plugin.MethodGet, Path: "/secrets/versions", Permission: "vault.secrets.read", Risk: plugin.RiskSafe, AuditEvent: "vault.secret.versions", Handle: listSecretVersions},
		{ID: "vault.secret.metadata", Method: plugin.MethodGet, Path: "/secrets/metadata", Permission: "vault.secrets.read", Risk: plugin.RiskSafe, AuditEvent: "vault.secret.metadata", Handle: readSecretMetadata},
		{ID: "vault.secret.versions.delete", Method: plugin.MethodPost, Path: "/secrets/delete-versions", Permission: "vault.secrets.delete", Risk: plugin.RiskDestructive, AuditEvent: "vault.secret.versions.delete", Input: versionsSchema("Versions to delete", "Comma-separated KV v2 version numbers. A deleted version keeps its data until it is destroyed."), Handle: deleteSecretVersions},
		{ID: "vault.secret.undelete", Method: plugin.MethodPost, Path: "/secrets/undelete", Permission: "vault.secrets.write", Risk: plugin.RiskWrite, AuditEvent: "vault.secret.undelete", Input: versionsSchema("Versions to undelete", "Comma-separated KV v2 version numbers."), Handle: undeleteSecret},
		{ID: "vault.secret.destroy", Method: plugin.MethodPost, Path: "/secrets/destroy", Permission: "vault.secrets.delete", Risk: plugin.RiskDestructive, AuditEvent: "vault.secret.destroy", Input: versionsSchema("Versions to destroy", "Comma-separated KV v2 version numbers. Destroyed data cannot be recovered."), Handle: destroySecret},
		{ID: "vault.secret.purge", Method: plugin.MethodDelete, Path: "/secrets/metadata", Permission: "vault.secrets.delete", Risk: plugin.RiskDestructive, AuditEvent: "vault.secret.purge", Handle: purgeSecret},

		{ID: "vault.policies.list", Method: plugin.MethodGet, Path: "/policies", Permission: "vault.policies.read", Risk: plugin.RiskSafe, AuditEvent: "vault.policies.list", Handle: listPolicies},
		{ID: "vault.policy.read", Method: plugin.MethodGet, Path: "/policies/detail", Permission: "vault.policies.read", Risk: plugin.RiskSafe, AuditEvent: "vault.policy.read", Handle: readPolicy},
		{ID: "vault.policy.write", Method: plugin.MethodPut, Path: "/policies", Permission: "vault.policies.write", Risk: plugin.RiskWrite, AuditEvent: "vault.policy.write", Handle: writePolicy},
		{ID: "vault.policy.create", Method: plugin.MethodPost, Path: "/policies", Permission: "vault.policies.write", Risk: plugin.RiskWrite, AuditEvent: "vault.policy.create", Input: policyCreateSchema(), Handle: createPolicy},
		{ID: "vault.policy.delete", Method: plugin.MethodDelete, Path: "/policies", Permission: "vault.policies.delete", Risk: plugin.RiskDestructive, AuditEvent: "vault.policy.delete", Handle: deletePolicy},

		{ID: "vault.tokens.list", Method: plugin.MethodGet, Path: "/tokens", Permission: "vault.tokens.read", Risk: plugin.RiskSafe, AuditEvent: "vault.tokens.list", Handle: listTokens},
		{ID: "vault.token.read", Method: plugin.MethodGet, Path: "/tokens/detail", Permission: "vault.tokens.read", Risk: plugin.RiskSafe, AuditEvent: "vault.token.read", Handle: readToken},
		{ID: "vault.token.create", Method: plugin.MethodPost, Path: "/tokens", Permission: "vault.tokens.write", Risk: plugin.RiskWrite, AuditEvent: "vault.token.create", Input: tokenCreateSchema(), Handle: createToken},
		{ID: "vault.token.revoke", Method: plugin.MethodDelete, Path: "/tokens", Permission: "vault.tokens.delete", Risk: plugin.RiskDestructive, AuditEvent: "vault.token.revoke", Handle: revokeToken},

		{ID: "vault.leases.list", Method: plugin.MethodGet, Path: "/leases", Permission: "vault.leases.read", Risk: plugin.RiskSafe, AuditEvent: "vault.leases.list", Handle: listLeases},
		{ID: "vault.lease.read", Method: plugin.MethodGet, Path: "/leases/detail", Permission: "vault.leases.read", Risk: plugin.RiskSafe, AuditEvent: "vault.lease.read", Handle: readLease},
		{ID: "vault.lease.revoke", Method: plugin.MethodPost, Path: "/leases/revoke", Permission: "vault.leases.write", Risk: plugin.RiskDestructive, AuditEvent: "vault.lease.revoke", Handle: revokeLease},

		{ID: "vault.namespaces.list", Method: plugin.MethodGet, Path: "/namespaces", Permission: "vault.read", Risk: plugin.RiskSafe, AuditEvent: "vault.namespaces.list", Handle: listNamespaces},
		{ID: "vault.namespace.read", Method: plugin.MethodGet, Path: "/namespaces/detail", Permission: "vault.read", Risk: plugin.RiskSafe, AuditEvent: "vault.namespace.read", Handle: readNamespace},

		{ID: "vault.audit.list", Method: plugin.MethodGet, Path: "/audit", Permission: "vault.read", Risk: plugin.RiskSafe, AuditEvent: "vault.audit.list", Handle: listAudit},
		{ID: "vault.audit.read", Method: plugin.MethodGet, Path: "/audit/detail", Permission: "vault.read", Risk: plugin.RiskSafe, AuditEvent: "vault.audit.read", Handle: readAudit},
	}
}

// scanLimit caps how many entries one walk of a Vault hierarchy collects, and
// maxWalkRequests caps the round trips it may spend getting them; Vault answers
// one folder per LIST and never paginates.
const (
	scanLimit       = 1000
	maxWalkRequests = 250
)

// scanPage extends the paged wire contract with the walk-cap signal: truncated
// tells the browser the walk stopped before the hierarchy ended, so the listing
// is a bounded prefix of it and the total counts only what was loaded.
type scanPage struct {
	plugin.Page[row]
	Truncated bool `json:"truncated,omitempty"`
	ScanLimit int  `json:"scanLimit,omitempty"`
}

type call struct {
	session *Session
	client  *vaultapi.Client
	ctx     context.Context
	cancel  context.CancelFunc
}

func begin(rc *plugin.RequestContext) (*call, error) {
	s, err := unwrap(rc.Session)
	if err != nil {
		return nil, err
	}
	client, _, err := s.clientFor(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	return &call{session: s, client: client, ctx: ctx, cancel: cancel}, nil
}

func (c *call) namespace() string { return c.client.Namespace() }

// param reads a route value from the resolved params, falling back to the "p."
// query form the grid uses for list requests.
func param(rc *plugin.RequestContext, name string) string {
	if v := strings.TrimSpace(rc.Param(name)); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p." + name))
}

// pageWindow slices one page out of an already filtered and ordered set. The
// cursor is a plain offset and a next cursor is emitted only when rows remain.
func pageWindow(req plugin.PageRequest, total int) (int, int, string, error) {
	start := 0
	if req.Cursor != "" {
		parsed, err := strconv.Atoi(req.Cursor)
		if err != nil || parsed < 0 {
			return 0, 0, "", fmt.Errorf("%w: invalid cursor", plugin.ErrInvalidInput)
		}
		start = parsed
	}
	if start > total {
		start = total
	}
	end := min(start+req.Limit, total)
	next := ""
	if end < total {
		next = strconv.Itoa(end)
	}
	return start, end, next, nil
}

// pageNames filters and orders the whole name set before the page window is
// taken, so the grid's search box and column sort cover the dataset instead of
// the visible page. names must be a slice the caller owns; it is sorted in place.
func pageNames(rc *plugin.RequestContext, names []string, sortFields ...string) ([]string, string, int, error) {
	req, err := rc.Page()
	if err != nil {
		return nil, "", 0, err
	}
	names = filterNames(names, req.Search())
	orderNames(names, sortFields, req.Sort)
	start, end, next, err := pageWindow(req, len(names))
	if err != nil {
		return nil, "", 0, err
	}
	return names[start:end], next, len(names), nil
}

func filterNames(names []string, term string) []string {
	term = strings.ToLower(strings.TrimSpace(term))
	if term == "" {
		return names
	}
	out := make([]string, 0, len(names))
	for _, name := range names {
		if strings.Contains(strings.ToLower(name), term) {
			out = append(out, name)
		}
	}
	return out
}

// orderNames puts the whole set in one deterministic order before it is paged,
// because the cursor is an offset into that order. Ascending by name is the
// baseline; a grid sort only reverses it, and only when it targets a column this
// listing can actually order by.
func orderNames(names []string, fields []string, keys []plugin.SortKey) {
	slices.Sort(names)
	if len(keys) > 0 && keys[0].Desc && slices.Contains(fields, keys[0].Field) {
		slices.Reverse(names)
	}
}

func namePage(items []row, next string, total int) plugin.Page[row] {
	return plugin.Page[row]{Items: items, NextCursor: next, Total: &total}
}

// scanRows pages one snapshotted walk. Filter, sort, and the offset cursor all
// cover the same fixed set, so every cursor this emits stays reachable and the
// total is exact for the rows that were loaded; truncated says the hierarchy
// continues past the cap.
func scanRows(rc *plugin.RequestContext, names []string, truncated bool, sortFields []string, build func(string) row) (scanPage, error) {
	window, next, total, err := pageNames(rc, names, sortFields...)
	if err != nil {
		return scanPage{}, err
	}
	rows := make([]row, 0, len(window))
	for _, name := range window {
		rows = append(rows, build(name))
	}
	page := scanPage{Page: namePage(rows, next, total)}
	if truncated {
		page.Truncated, page.ScanLimit = true, scanLimit
	}
	return page, nil
}

// walkKeyspace expands a Vault LIST hierarchy breadth-first, stopping at the
// entry or request cap and reporting whether more remains. Returned entries are
// leaf paths that already carry root as their prefix.
func walkKeyspace(ctx context.Context, client *vaultapi.Client, root string, listPath func(prefix string) string) ([]string, bool, error) {
	queue := []string{root}
	out := make([]string, 0, 256)
	for requests := 0; len(queue) > 0; requests++ {
		if len(out) >= scanLimit || requests >= maxWalkRequests {
			return out, true, nil
		}
		current := queue[0]
		queue = queue[1:]
		secret, err := client.Logical().ListWithContext(ctx, listPath(current))
		if err != nil {
			return nil, false, vaultErr(err)
		}
		for _, entry := range secretKeys(secret) {
			if strings.HasSuffix(entry, "/") {
				queue = append(queue, current+entry)
				continue
			}
			if len(out) >= scanLimit {
				return out, true, nil
			}
			out = append(out, current+entry)
		}
	}
	return out, false, nil
}

func systemList(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	health, err := c.client.Sys().HealthWithContext(c.ctx)
	if err != nil {
		return nil, vaultErr(err)
	}
	return plugin.Page[row]{Items: []row{{
		"ref":          plugin.ResourceIdentity{Kind: "system", Name: "Vault", UID: "server"},
		"name":         "Vault",
		"version":      health.Version,
		"cluster_name": health.ClusterName,
		"state":        healthState(health),
	}}, Total: ptr(1)}, nil
}

func systemHealth(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	health, err := c.client.Sys().HealthWithContext(c.ctx)
	if err != nil {
		return nil, vaultErr(err)
	}
	return row{
		"state":                        healthState(health),
		"version":                      health.Version,
		"enterprise":                   health.Enterprise,
		"initialized":                  health.Initialized,
		"sealed":                       health.Sealed,
		"standby":                      health.Standby,
		"performance_standby":          health.PerformanceStandby,
		"cluster_name":                 health.ClusterName,
		"cluster_id":                   health.ClusterID,
		"replication_performance_mode": health.ReplicationPerformanceMode,
		"replication_dr_mode":          health.ReplicationDRMode,
		"server_time_utc":              time.Unix(health.ServerTimeUTC, 0).UTC(),
		"namespace":                    c.namespace(),
	}, nil
}

func systemSeal(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	status, err := c.client.Sys().SealStatusWithContext(c.ctx)
	if err != nil {
		return nil, vaultErr(err)
	}
	return row{
		"state":         sealState(status),
		"type":          status.Type,
		"sealed":        status.Sealed,
		"initialized":   status.Initialized,
		"recovery_seal": status.RecoverySeal,
		"threshold":     status.T,
		"shares":        status.N,
		"progress":      status.Progress,
		"storage_type":  status.StorageType,
		"migration":     status.Migration,
		"version":       status.Version,
		"cluster_name":  status.ClusterName,
		"warnings":      status.Warnings,
	}, nil
}

func healthState(health *vaultapi.HealthResponse) string {
	switch {
	case health == nil:
		return "unknown"
	case !health.Initialized:
		return "uninitialized"
	case health.Sealed:
		return "sealed"
	case health.Standby:
		return "standby"
	default:
		return "active"
	}
}

func sealState(status *vaultapi.SealStatusResponse) string {
	switch {
	case status == nil:
		return "unknown"
	case !status.Initialized:
		return "uninitialized"
	case status.Sealed:
		return "sealed"
	default:
		return "unsealed"
	}
}

func mountRows(mounts []mountInfo, kind string) []row {
	rows := make([]row, 0, len(mounts))
	for _, m := range mounts {
		name := strings.TrimSuffix(m.Path, "/")
		rows = append(rows, row{
			"ref":             plugin.ResourceIdentity{Kind: kind, Name: name, UID: m.Path},
			"path":            m.Path,
			"type":            m.Type,
			"kv":              m.KVVersion > 0,
			"kv_version":      m.KVVersion,
			"description":     m.out.Description,
			"accessor":        m.out.Accessor,
			"running_version": m.out.RunningVersion,
			"local":           m.out.Local,
		})
	}
	return rows
}

func listMounts(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	mounts, err := c.session.mountList(c.ctx, c.client, c.namespace(), true)
	if err != nil {
		return nil, err
	}
	return broker.PageRows(rc, mountRows(mounts, "mount"))
}

func readMount(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	m, err := c.session.mountByPath(c.ctx, c.client, c.namespace(), param(rc, "mount"))
	if err != nil {
		return nil, err
	}
	return row{
		"path":                    m.Path,
		"type":                    m.Type,
		"kv":                      m.KVVersion > 0,
		"kv_version":              m.KVVersion,
		"description":             m.out.Description,
		"accessor":                m.out.Accessor,
		"uuid":                    m.out.UUID,
		"local":                   m.out.Local,
		"seal_wrap":               m.out.SealWrap,
		"external_entropy_access": m.out.ExternalEntropyAccess,
		"running_version":         m.out.RunningVersion,
		"plugin_version":          m.out.PluginVersion,
		"deprecation_status":      m.out.DeprecationStatus,
		"options":                 m.out.Options,
	}, nil
}

func readMountConfig(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	m, err := c.session.mountByPath(c.ctx, c.client, c.namespace(), param(rc, "mount"))
	if err != nil {
		return nil, err
	}
	return mountConfigRow(m.out.Config), nil
}

func mountConfigRow(cfg vaultapi.MountConfigOutput) row {
	return row{
		"default_lease_ttl":            cfg.DefaultLeaseTTL,
		"max_lease_ttl":                cfg.MaxLeaseTTL,
		"force_no_cache":               cfg.ForceNoCache,
		"listing_visibility":           cfg.ListingVisibility,
		"token_type":                   cfg.TokenType,
		"audit_non_hmac_request_keys":  cfg.AuditNonHMACRequestKeys,
		"audit_non_hmac_response_keys": cfg.AuditNonHMACResponseKeys,
		"passthrough_request_headers":  cfg.PassthroughRequestHeaders,
		"allowed_response_headers":     cfg.AllowedResponseHeaders,
		"allowed_managed_keys":         cfg.AllowedManagedKeys,
	}
}

func authMounts(ctx context.Context, client *vaultapi.Client) ([]mountInfo, error) {
	mounts, err := client.Sys().ListAuthWithContext(ctx)
	if err != nil {
		return nil, vaultErr(err)
	}
	items := make([]mountInfo, 0, len(mounts))
	for path, out := range mounts {
		if out == nil {
			continue
		}
		items = append(items, mountInfo{Path: path, Type: out.Type, out: out})
	}
	slices.SortFunc(items, func(a, b mountInfo) int { return strings.Compare(a.Path, b.Path) })
	return items, nil
}

func listAuth(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	mounts, err := authMounts(c.ctx, c.client)
	if err != nil {
		return nil, err
	}
	return broker.PageRows(rc, mountRows(mounts, "auth"))
}

func readAuth(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	want := strings.Trim(param(rc, "mount"), "/") + "/"
	mounts, err := authMounts(c.ctx, c.client)
	if err != nil {
		return nil, err
	}
	for _, m := range mounts {
		if m.Path != want {
			continue
		}
		out := row{
			"path":               m.Path,
			"type":               m.Type,
			"description":        m.out.Description,
			"accessor":           m.out.Accessor,
			"uuid":               m.out.UUID,
			"local":              m.out.Local,
			"running_version":    m.out.RunningVersion,
			"deprecation_status": m.out.DeprecationStatus,
		}
		for key, value := range mountConfigRow(m.out.Config) {
			out[key] = value
		}
		return out, nil
	}
	return nil, fmt.Errorf("%w: auth method %q is not enabled", plugin.ErrNotFound, param(rc, "mount"))
}

func treeMounts(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	mounts, err := c.session.mountList(c.ctx, c.client, c.namespace(), true)
	if err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(mounts))
	byPath := make(map[string]mountInfo, len(mounts))
	for _, m := range mounts {
		paths = append(paths, m.Path)
		byPath[m.Path] = m
	}
	window, next, total, err := pageNames(rc, paths, "path", "name")
	if err != nil {
		return nil, err
	}
	nodes := make([]plugin.TreeNode, 0, len(window))
	for _, path := range window {
		m := byPath[path]
		name := strings.TrimSuffix(path, "/")
		node := plugin.TreeNode{
			Key:   "mount:" + path,
			Label: name,
			Icon:  icon("database"),
			Ref:   &plugin.ResourceIdentity{Kind: "mount", Name: name, UID: path},
			Leaf:  m.KVVersion == 0,
			Data:  map[string]any{"type": m.Type, "kv": m.KVVersion > 0},
		}
		if m.KVVersion > 0 {
			node.ChildrenSource = &plugin.DataSource{RouteID: "vault.tree.paths", Params: map[string]string{"mount": path}}
		}
		nodes = append(nodes, node)
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: next, Total: &total}, nil
}

func treeAuth(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	mounts, err := authMounts(c.ctx, c.client)
	if err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(mounts))
	for _, m := range mounts {
		paths = append(paths, m.Path)
	}
	window, next, total, err := pageNames(rc, paths, "path", "name")
	if err != nil {
		return nil, err
	}
	nodes := make([]plugin.TreeNode, 0, len(window))
	for _, path := range window {
		name := strings.TrimSuffix(path, "/")
		nodes = append(nodes, plugin.TreeNode{
			Key:   "auth:" + path,
			Label: name,
			Icon:  icon("key-round"),
			Ref:   &plugin.ResourceIdentity{Kind: "auth", Name: name, UID: path},
			Leaf:  true,
		})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: next, Total: &total}, nil
}

func listPolicies(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	names, err := c.client.Sys().ListPoliciesWithContext(c.ctx)
	if err != nil {
		return nil, vaultErr(err)
	}
	window, next, total, err := pageNames(rc, names, "name")
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(window))
	for _, name := range window {
		rows = append(rows, row{
			"ref":     plugin.ResourceIdentity{Kind: "policy", Name: name, UID: name},
			"name":    name,
			"builtin": name == "root" || name == "default",
		})
	}
	return namePage(rows, next, total), nil
}

func readPolicy(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	name, err := policyName(rc)
	if err != nil {
		return nil, err
	}
	rules, err := c.client.Sys().GetPolicyWithContext(c.ctx, name)
	if err != nil {
		return nil, vaultErr(err)
	}
	if rules == "" {
		return nil, fmt.Errorf("%w: policy %q not found", plugin.ErrNotFound, name)
	}
	return rules, nil
}

func writePolicy(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	if err := ensureWritable(c.session); err != nil {
		return nil, err
	}
	name, err := policyName(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		Content string `json:"content"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.Content) == "" {
		return nil, fmt.Errorf("%w: policy document is required", plugin.ErrInvalidInput)
	}
	if err := c.client.Sys().PutPolicyWithContext(c.ctx, name, req.Content); err != nil {
		return nil, vaultErr(err)
	}
	// The editor resets its baseline from the save response, so the stored
	// document is read back rather than echoing the submitted text.
	stored, err := c.client.Sys().GetPolicyWithContext(c.ctx, name)
	if err != nil {
		return nil, vaultErr(err)
	}
	return row{"ok": true, "name": name, "content": stored}, nil
}

func createPolicy(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	if err := ensureWritable(c.session); err != nil {
		return nil, err
	}
	var req struct {
		Name   string `json:"name" validate:"required"`
		Policy string `json:"policy" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name := strings.TrimSpace(req.Name)
	if name == "" || strings.TrimSpace(req.Policy) == "" {
		return nil, fmt.Errorf("%w: policy name and document are required", plugin.ErrInvalidInput)
	}
	if err := c.client.Sys().PutPolicyWithContext(c.ctx, name, req.Policy); err != nil {
		return nil, vaultErr(err)
	}
	return row{"ok": true, "name": name}, nil
}

func deletePolicy(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	if err := ensureWritable(c.session); err != nil {
		return nil, err
	}
	name, err := policyName(rc)
	if err != nil {
		return nil, err
	}
	if name == "root" || name == "default" {
		return nil, fmt.Errorf("%w: the %q policy is built in and cannot be deleted", plugin.ErrForbidden, name)
	}
	if err := c.client.Sys().DeletePolicyWithContext(c.ctx, name); err != nil {
		return nil, vaultErr(err)
	}
	return actionResult{OK: true}, nil
}

func policyName(rc *plugin.RequestContext) (string, error) {
	name := strings.Trim(param(rc, "name"), "/")
	if name == "" {
		return "", fmt.Errorf("%w: policy name is required", plugin.ErrInvalidInput)
	}
	return name, nil
}

func listTokens(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	secret, err := c.client.Logical().ListWithContext(c.ctx, "auth/token/accessors")
	if err != nil {
		return nil, vaultErr(err)
	}
	// Only the accessor is cheap to list; lookups happen for the page window
	// alone, so the free-text search matches accessors rather than every column.
	window, next, total, err := pageNames(rc, secretKeys(secret), "accessor")
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(window))
	for _, accessor := range window {
		info, lookupErr := c.client.Auth().Token().LookupAccessorWithContext(c.ctx, accessor)
		if lookupErr != nil || info == nil {
			rows = append(rows, row{
				"ref":      plugin.ResourceIdentity{Kind: "token", Name: accessor, UID: accessor},
				"accessor": accessor,
			})
			continue
		}
		rows = append(rows, tokenRow(accessor, info.Data))
	}
	return namePage(rows, next, total), nil
}

func readToken(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	accessor := strings.TrimSpace(param(rc, "accessor"))
	if accessor == "" {
		return nil, fmt.Errorf("%w: token accessor is required", plugin.ErrInvalidInput)
	}
	info, err := c.client.Auth().Token().LookupAccessorWithContext(c.ctx, accessor)
	if err != nil {
		return nil, vaultErr(err)
	}
	if info == nil {
		return nil, fmt.Errorf("%w: token accessor %q not found", plugin.ErrNotFound, accessor)
	}
	return tokenRow(accessor, info.Data), nil
}

func tokenRow(accessor string, data map[string]any) row {
	out := row{
		"ref":      plugin.ResourceIdentity{Kind: "token", Name: accessor, UID: accessor},
		"accessor": accessor,
	}
	for _, key := range []string{
		"display_name", "type", "policies", "identity_policies", "ttl", "explicit_max_ttl",
		"creation_ttl", "num_uses", "renewable", "orphan", "issue_time", "expire_time",
		"entity_id", "path", "meta",
	} {
		if value, ok := data[key]; ok {
			out[key] = value
		}
	}
	return out
}

func createToken(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	if err := ensureWritable(c.session); err != nil {
		return nil, err
	}
	var req struct {
		DisplayName    string `json:"display_name"`
		Policies       string `json:"policies"`
		TTL            string `json:"ttl"`
		ExplicitMaxTTL string `json:"explicit_max_ttl"`
		NumUses        int    `json:"num_uses"`
		TokenType      string `json:"token_type"`
		Renewable      *bool  `json:"renewable"`
		NoParent       bool   `json:"no_parent"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	if req.NumUses < 0 {
		return nil, fmt.Errorf("%w: use limit cannot be negative", plugin.ErrInvalidInput)
	}
	create := &vaultapi.TokenCreateRequest{
		DisplayName:    strings.TrimSpace(req.DisplayName),
		Policies:       splitList(req.Policies),
		TTL:            strings.TrimSpace(req.TTL),
		ExplicitMaxTTL: strings.TrimSpace(req.ExplicitMaxTTL),
		NumUses:        req.NumUses,
		Type:           strings.TrimSpace(req.TokenType),
		NoParent:       req.NoParent,
		Renewable:      req.Renewable,
	}
	secret, err := c.client.Auth().Token().CreateWithContext(c.ctx, create)
	if err != nil {
		return nil, vaultErr(err)
	}
	if secret == nil || secret.Auth == nil {
		return nil, fmt.Errorf("%w: vault returned no token", plugin.ErrUnavailable)
	}
	// The client token is the product of this action, so it is returned once in
	// the response body and never echoed into route params or audit metadata.
	return row{
		"ok":        true,
		"token":     secret.Auth.ClientToken,
		"accessor":  secret.Auth.Accessor,
		"policies":  secret.Auth.Policies,
		"ttl":       secret.Auth.LeaseDuration,
		"renewable": secret.Auth.Renewable,
	}, nil
}

func revokeToken(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	if err := ensureWritable(c.session); err != nil {
		return nil, err
	}
	accessor := strings.TrimSpace(param(rc, "accessor"))
	if accessor == "" {
		return nil, fmt.Errorf("%w: token accessor is required", plugin.ErrInvalidInput)
	}
	if err := c.client.Auth().Token().RevokeAccessorWithContext(c.ctx, accessor); err != nil {
		return nil, vaultErr(err)
	}
	return actionResult{OK: true}, nil
}

func listLeases(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	prefix := normalizeFolder(param(rc, "prefix"))
	ids, truncated, err := c.session.scan(c.ctx, scanKey("leases", c.namespace(), prefix), func(ctx context.Context) ([]string, bool, error) {
		return walkKeyspace(ctx, c.client, prefix, func(folder string) string { return "sys/leases/lookup/" + folder })
	})
	if err != nil {
		return nil, err
	}
	return scanRows(rc, ids, truncated, []string{"id"}, func(id string) row {
		info, lookupErr := c.client.Sys().LookupWithContext(c.ctx, id)
		if lookupErr != nil || info == nil {
			return row{"ref": leaseRef(id), "id": id}
		}
		return leaseRow(id, info.Data)
	})
}

func readLease(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	id := strings.TrimSpace(param(rc, "lease_id"))
	if id == "" {
		return nil, fmt.Errorf("%w: lease id is required", plugin.ErrInvalidInput)
	}
	info, err := c.client.Sys().LookupWithContext(c.ctx, id)
	if err != nil {
		return nil, vaultErr(err)
	}
	if info == nil {
		return nil, fmt.Errorf("%w: lease %q not found", plugin.ErrNotFound, id)
	}
	return leaseRow(id, info.Data), nil
}

func revokeLease(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	if err := ensureWritable(c.session); err != nil {
		return nil, err
	}
	id := strings.TrimSpace(param(rc, "lease_id"))
	if id == "" {
		return nil, fmt.Errorf("%w: lease id is required", plugin.ErrInvalidInput)
	}
	if err := c.client.Sys().RevokeWithContext(c.ctx, id); err != nil {
		return nil, vaultErr(err)
	}
	c.session.dropScans()
	return actionResult{OK: true}, nil
}

func leaseRef(id string) plugin.ResourceIdentity {
	return plugin.ResourceIdentity{Kind: "lease", Name: id, UID: id}
}

func leaseRow(id string, data map[string]any) row {
	out := row{"ref": leaseRef(id), "id": id}
	for _, key := range []string{"ttl", "renewable", "issue_time", "expire_time", "last_renewal"} {
		if value, ok := data[key]; ok {
			out[key] = value
		}
	}
	return out
}

func listNamespaces(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	// sys/namespaces is Vault Enterprise only; community editions answer 404,
	// which the client reports as an empty list rather than an error.
	secret, err := c.client.Logical().ListWithContext(c.ctx, "sys/namespaces")
	if err != nil {
		if isUnsupportedPath(err) {
			return plugin.Page[row]{Items: []row{}, Total: ptr(0)}, nil
		}
		return nil, vaultErr(err)
	}
	base := c.namespace()
	children := secretKeys(secret)
	paths := make([]string, 0, len(children))
	for _, child := range children {
		paths = append(paths, joinNamespace(base, child))
	}
	window, next, total, err := pageNames(rc, paths, "path")
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(window))
	for _, path := range window {
		rows = append(rows, row{
			"ref":  plugin.ResourceIdentity{Kind: "namespace", Name: path, UID: path},
			"path": path,
		})
	}
	return namePage(rows, next, total), nil
}

func readNamespace(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	path := normalizeNamespace(param(rc, "path"))
	if path == "" {
		return nil, fmt.Errorf("%w: namespace path is required", plugin.ErrInvalidInput)
	}
	// A namespace is read from its parent, so the request stays in the current
	// scope with the child path appended.
	secret, err := c.client.Logical().ReadWithContext(c.ctx, "sys/namespaces/"+relativeNamespace(c.namespace(), path))
	if err != nil {
		return nil, vaultErr(err)
	}
	if secret == nil || secret.Data == nil {
		return nil, fmt.Errorf("%w: namespace %q not found", plugin.ErrNotFound, path)
	}
	out := row{"path": path}
	for _, key := range []string{"id", "custom_metadata"} {
		if value, ok := secret.Data[key]; ok {
			out[key] = value
		}
	}
	return out, nil
}

func joinNamespace(base, child string) string {
	child = normalizeNamespace(child)
	if base == "" {
		return child
	}
	return base + "/" + child
}

func relativeNamespace(base, path string) string {
	if base != "" && strings.HasPrefix(path, base+"/") {
		return strings.TrimPrefix(path, base+"/")
	}
	return path
}

func listAudit(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	devices, err := c.client.Sys().ListAuditWithContext(c.ctx)
	if err != nil {
		return nil, vaultErr(err)
	}
	// sys/audit answers a map, whose iteration order is random: the rows have to
	// be ordered before they are paged or an offset cursor addresses a different
	// device on every request.
	paths := make([]string, 0, len(devices))
	for path := range devices {
		paths = append(paths, path)
	}
	slices.Sort(paths)
	rows := make([]row, 0, len(paths))
	for _, path := range paths {
		rows = append(rows, auditRow(path, devices[path]))
	}
	return broker.PageRows(rc, rows)
}

func readAudit(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	want := strings.Trim(param(rc, "path"), "/") + "/"
	devices, err := c.client.Sys().ListAuditWithContext(c.ctx)
	if err != nil {
		return nil, vaultErr(err)
	}
	for path, device := range devices {
		if path == want {
			return auditRow(path, device), nil
		}
	}
	return nil, fmt.Errorf("%w: audit device %q is not enabled", plugin.ErrNotFound, param(rc, "path"))
}

func auditRow(path string, device *vaultapi.Audit) row {
	name := strings.TrimSuffix(path, "/")
	out := row{
		"ref":  plugin.ResourceIdentity{Kind: "audit", Name: name, UID: path},
		"path": path,
	}
	if device != nil {
		out["type"] = device.Type
		out["description"] = device.Description
		out["local"] = device.Local
		out["options"] = device.Options
	}
	return out
}

// secretKeys reads the "keys" list every Vault LIST response carries, tolerating
// the nil secret the client returns for an empty or missing path.
func secretKeys(secret *vaultapi.Secret) []string {
	if secret == nil || secret.Data == nil {
		return nil
	}
	raw, ok := secret.Data["keys"].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, entry := range raw {
		if key, ok := entry.(string); ok && key != "" {
			out = append(out, key)
		}
	}
	return out
}

// normalizeFolder renders a folder prefix the way Vault LIST responses do:
// empty for a root listing, otherwise exactly one trailing slash.
func normalizeFolder(raw string) string {
	trimmed := strings.Trim(strings.TrimSpace(raw), "/")
	if trimmed == "" {
		return ""
	}
	return trimmed + "/"
}

func isUnsupportedPath(err error) bool {
	message := strings.ToLower(vaultMessage(err))
	return strings.Contains(message, "unsupported path") || strings.Contains(message, "unsupported operation")
}

func splitList(raw string) []string {
	parts := strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || r == '\n' || r == '\r' || r == '\t' })
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func ptr[T any](v T) *T { return &v }
