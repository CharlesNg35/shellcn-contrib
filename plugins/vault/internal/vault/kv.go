package vault

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	vaultapi "github.com/hashicorp/vault/api"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

// target is one addressed secret: the engine that owns it, the path inside that
// engine, and the fully qualified key the UI round-trips.
type target struct {
	mount mountInfo
	path  string
	key   string
}

func (c *call) target(rc *plugin.RequestContext, key string) (target, error) {
	if strings.TrimSpace(key) == "" {
		key = param(rc, "key")
	}
	m, rel, err := c.session.resolveMount(c.ctx, c.client, c.namespace(), key)
	if err != nil {
		return target{}, err
	}
	if m.KVVersion == 0 {
		return target{}, fmt.Errorf("%w: %q is a %s engine, not a key/value store", plugin.ErrNotSupported, m.Path, m.Type)
	}
	rel = strings.Trim(rel, "/")
	if rel == "" {
		return target{}, fmt.Errorf("%w: a secret path inside %q is required", plugin.ErrInvalidInput, m.Path)
	}
	return target{mount: m, path: rel, key: m.Path + rel}, nil
}

func (t target) kv2(client *vaultapi.Client) *vaultapi.KVv2 {
	return client.KVv2(strings.TrimSuffix(t.mount.Path, "/"))
}

func (t target) kv1(client *vaultapi.Client) *vaultapi.KVv1 {
	return client.KVv1(strings.TrimSuffix(t.mount.Path, "/"))
}

// listPath maps a mount-relative folder onto the Vault path that lists it: KV v2
// keeps its directory index under metadata/, KV v1 lists the data path directly.
func listPath(m mountInfo, prefix string) string {
	base := m.Path
	if m.KVVersion == 2 {
		base += "metadata/"
	}
	return strings.TrimSuffix(base+prefix, "/")
}

func listSecrets(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	m, err := c.session.mountByPath(c.ctx, c.client, c.namespace(), param(rc, "mount"))
	if err != nil {
		return nil, err
	}
	if m.KVVersion == 0 {
		return plugin.Page[row]{Items: []row{}, Total: ptr(0)}, nil
	}
	prefix := normalizeFolder(param(rc, "path"))
	paths, truncated, err := c.session.scan(c.ctx, scanKey("secrets", c.namespace(), m.Path, prefix), func(ctx context.Context) ([]string, bool, error) {
		return walkKeyspace(ctx, c.client, prefix, func(folder string) string { return listPath(m, folder) })
	})
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(paths))
	for _, path := range paths {
		keys = append(keys, m.Path+path)
	}
	return scanRows(rc, keys, truncated, []string{"key", "name"}, func(key string) row {
		return row{
			"ref":   plugin.ResourceIdentity{Kind: "secret", Namespace: m.Path, Name: secretName(key), UID: key},
			"key":   key,
			"name":  secretName(key),
			"mount": m.Path,
		}
	})
}

func treePaths(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	m, err := c.session.mountByPath(c.ctx, c.client, c.namespace(), param(rc, "mount"))
	if err != nil {
		return nil, err
	}
	prefix := normalizeFolder(param(rc, "path"))
	folders := []string{}
	if m.KVVersion > 0 {
		secret, listErr := c.client.Logical().ListWithContext(c.ctx, listPath(m, prefix))
		if listErr != nil {
			return nil, vaultErr(listErr)
		}
		for _, entry := range secretKeys(secret) {
			if strings.HasSuffix(entry, "/") {
				folders = append(folders, prefix+entry)
			}
		}
	}
	window, next, total, err := pageNames(rc, folders, "path", "name")
	if err != nil {
		return nil, err
	}
	nodes := make([]plugin.TreeNode, 0, len(window))
	for _, folder := range window {
		nodes = append(nodes, plugin.TreeNode{
			Key:            "path:" + m.Path + folder,
			Label:          strings.TrimSuffix(strings.TrimPrefix(folder, prefix), "/"),
			Icon:           icon("folder"),
			ChildrenSource: &plugin.DataSource{RouteID: "vault.tree.paths", Params: map[string]string{"mount": m.Path, "path": folder}},
			ResourceKind:   "secret",
			ListParams:     map[string]string{"mount": m.Path, "path": folder},
		})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: next, Total: &total}, nil
}

func secretName(key string) string {
	if i := strings.LastIndex(key, "/"); i >= 0 {
		return key[i+1:]
	}
	return key
}

func readSecret(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	t, err := c.target(rc, "")
	if err != nil {
		return nil, err
	}
	out := row{"key": t.key, "mount": t.mount.Path, "kv_version": t.mount.KVVersion, "type": "json"}
	if t.mount.KVVersion == 1 {
		secret, readErr := t.kv1(c.client).Get(c.ctx, t.path)
		if readErr != nil {
			return nil, vaultErr(readErr)
		}
		out["value"] = secret.Data
		return out, nil
	}
	secret, err := t.kv2(c.client).Get(c.ctx, t.path)
	if err != nil {
		return nil, vaultErr(err)
	}
	out["value"] = secret.Data
	out["custom_metadata"] = secret.CustomMetadata
	if meta := secret.VersionMetadata; meta != nil {
		out["version"] = meta.Version
		out["created_time"] = meta.CreatedTime
		out["destroyed"] = meta.Destroyed
		if !meta.DeletionTime.IsZero() {
			out["deletion_time"] = meta.DeletionTime
		}
	}
	return out, nil
}

func writeSecret(rc *plugin.RequestContext) (any, error) {
	var req struct {
		Key   string `json:"key"`
		Value any    `json:"value"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	data, err := secretData(req.Value)
	if err != nil {
		return nil, err
	}
	return storeSecret(rc, req.Key, data, nil)
}

func putSecret(rc *plugin.RequestContext) (any, error) {
	var req struct {
		Key  string `json:"key" validate:"required"`
		Data any    `json:"data"`
		CAS  *int   `json:"cas"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	data, err := secretData(req.Data)
	if err != nil {
		return nil, err
	}
	return storeSecret(rc, req.Key, data, req.CAS)
}

func storeSecret(rc *plugin.RequestContext, key string, data map[string]any, cas *int) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	if err := ensureWritable(c.session); err != nil {
		return nil, err
	}
	t, err := c.target(rc, key)
	if err != nil {
		return nil, err
	}
	if t.mount.KVVersion == 1 {
		if cas != nil {
			return nil, fmt.Errorf("%w: check-and-set needs a KV v2 engine", plugin.ErrNotSupported)
		}
		if err := t.kv1(c.client).Put(c.ctx, t.path, data); err != nil {
			return nil, vaultErr(err)
		}
		c.session.dropScans()
		return row{"ok": true, "key": t.key}, nil
	}
	opts := []vaultapi.KVOption{}
	if cas != nil {
		if *cas < 0 {
			return nil, fmt.Errorf("%w: check-and-set version cannot be negative", plugin.ErrInvalidInput)
		}
		opts = append(opts, vaultapi.WithCheckAndSet(*cas))
	}
	written, err := t.kv2(c.client).Put(c.ctx, t.path, data, opts...)
	if err != nil {
		return nil, vaultErr(err)
	}
	c.session.dropScans()
	out := row{"ok": true, "key": t.key}
	if written != nil && written.VersionMetadata != nil {
		out["version"] = written.VersionMetadata.Version
	}
	return out, nil
}

// secretData accepts both shapes the renderer sends: the KV panel posts the
// value as JSON text, while a form field of type JSON posts a decoded object.
func secretData(value any) (map[string]any, error) {
	switch v := value.(type) {
	case map[string]any:
		return v, nil
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return nil, fmt.Errorf("%w: secret data is required", plugin.ErrInvalidInput)
		}
		var out map[string]any
		if err := json.Unmarshal([]byte(trimmed), &out); err != nil {
			return nil, fmt.Errorf("%w: secret data must be a JSON object of key/value pairs", plugin.ErrInvalidInput)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("%w: secret data must be a JSON object of key/value pairs", plugin.ErrInvalidInput)
	}
}

func deleteSecret(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	if err := ensureWritable(c.session); err != nil {
		return nil, err
	}
	t, err := c.target(rc, "")
	if err != nil {
		return nil, err
	}
	if t.mount.KVVersion == 1 {
		err = t.kv1(c.client).Delete(c.ctx, t.path)
	} else {
		err = t.kv2(c.client).Delete(c.ctx, t.path)
	}
	if err != nil {
		return nil, vaultErr(err)
	}
	c.session.dropScans()
	return actionResult{OK: true}, nil
}

// versionOp is the KV v2 operation a version list is submitted to. Soft delete
// and undelete are reversible; destroy is not.
type versionOp string

const (
	opDeleteVersions   versionOp = "delete"
	opUndeleteVersions versionOp = "undelete"
	opDestroyVersions  versionOp = "destroy"
)

func deleteSecretVersions(rc *plugin.RequestContext) (any, error) {
	return versionOperation(rc, opDeleteVersions)
}

func undeleteSecret(rc *plugin.RequestContext) (any, error) {
	return versionOperation(rc, opUndeleteVersions)
}

func destroySecret(rc *plugin.RequestContext) (any, error) {
	return versionOperation(rc, opDestroyVersions)
}

func versionOperation(rc *plugin.RequestContext, op versionOp) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	if err := ensureWritable(c.session); err != nil {
		return nil, err
	}
	t, err := c.target(rc, "")
	if err != nil {
		return nil, err
	}
	if t.mount.KVVersion != 2 {
		return nil, fmt.Errorf("%w: %q does not keep secret versions", plugin.ErrNotSupported, t.mount.Path)
	}
	var req struct {
		Versions string `json:"versions" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	versions, err := parseVersions(req.Versions)
	if err != nil {
		return nil, err
	}
	switch op {
	case opDeleteVersions:
		err = t.kv2(c.client).DeleteVersions(c.ctx, t.path, versions)
	case opUndeleteVersions:
		err = t.kv2(c.client).Undelete(c.ctx, t.path, versions)
	default:
		err = t.kv2(c.client).Destroy(c.ctx, t.path, versions)
	}
	if err != nil {
		return nil, vaultErr(err)
	}
	return row{"ok": true, "key": t.key, "versions": versions}, nil
}

func purgeSecret(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	if err := ensureWritable(c.session); err != nil {
		return nil, err
	}
	t, err := c.target(rc, "")
	if err != nil {
		return nil, err
	}
	if t.mount.KVVersion == 2 {
		err = t.kv2(c.client).DeleteMetadata(c.ctx, t.path)
	} else {
		err = t.kv1(c.client).Delete(c.ctx, t.path)
	}
	if err != nil {
		return nil, vaultErr(err)
	}
	c.session.dropScans()
	return actionResult{OK: true}, nil
}

func parseVersions(raw string) ([]int, error) {
	parts := splitList(raw)
	if len(parts) == 0 {
		return nil, fmt.Errorf("%w: at least one version is required", plugin.ErrInvalidInput)
	}
	out := make([]int, 0, len(parts))
	for _, part := range parts {
		version, err := strconv.Atoi(part)
		if err != nil || version <= 0 {
			return nil, fmt.Errorf("%w: %q is not a version number", plugin.ErrInvalidInput, part)
		}
		if !slices.Contains(out, version) {
			out = append(out, version)
		}
	}
	return out, nil
}

func listSecretVersions(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	t, err := c.target(rc, "")
	if err != nil {
		return nil, err
	}
	if t.mount.KVVersion != 2 {
		return plugin.Page[row]{Items: []row{}, Total: ptr(0)}, nil
	}
	versions, err := t.kv2(c.client).GetVersionsAsList(c.ctx, t.path)
	if err != nil {
		return nil, vaultErr(err)
	}
	rows := make([]row, 0, len(versions))
	for _, version := range versions {
		item := row{
			"version":      version.Version,
			"state":        versionState(version),
			"created_time": version.CreatedTime,
			"destroyed":    version.Destroyed,
		}
		if !version.DeletionTime.IsZero() {
			item["deletion_time"] = version.DeletionTime
		}
		rows = append(rows, item)
	}
	return broker.PageRows(rc, rows)
}

func versionState(version vaultapi.KVVersionMetadata) string {
	switch {
	case version.Destroyed:
		return "destroyed"
	case !version.DeletionTime.IsZero():
		return "deleted"
	default:
		return "active"
	}
}

func readSecretMetadata(rc *plugin.RequestContext) (any, error) {
	c, err := begin(rc)
	if err != nil {
		return nil, err
	}
	defer c.cancel()
	t, err := c.target(rc, "")
	if err != nil {
		return nil, err
	}
	if t.mount.KVVersion != 2 {
		return nil, fmt.Errorf("%w: %q is a KV v1 engine and stores no secret metadata", plugin.ErrNotSupported, t.mount.Path)
	}
	meta, err := t.kv2(c.client).GetMetadata(c.ctx, t.path)
	if err != nil {
		return nil, vaultErr(err)
	}
	return row{
		"key":                  t.key,
		"current_version":      meta.CurrentVersion,
		"oldest_version":       meta.OldestVersion,
		"max_versions":         meta.MaxVersions,
		"cas_required":         meta.CASRequired,
		"delete_version_after": meta.DeleteVersionAfter.Seconds(),
		"created_time":         meta.CreatedTime,
		"updated_time":         meta.UpdatedTime,
		"custom_metadata":      meta.CustomMetadata,
	}, nil
}
