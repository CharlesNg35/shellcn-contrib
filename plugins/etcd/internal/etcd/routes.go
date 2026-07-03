package etcd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type keyEntry struct {
	Key string `json:"key"`
}

type keyDetail struct {
	Key            string `json:"key"`
	Value          string `json:"value"`
	TTL            int64  `json:"ttl,omitempty"`
	Lease          string `json:"lease,omitempty"`
	CreateRevision int64  `json:"createRevision,omitempty"`
	ModRevision    int64  `json:"modRevision,omitempty"`
	Version        int64  `json:"version,omitempty"`
}

type leaseEntry struct {
	ID         string `json:"id"`
	TTL        int64  `json:"ttl"`
	GrantedTTL int64  `json:"granted_ttl"`
	Keys       int    `json:"keys"`
}

type memberEntry struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	ClientURLs string `json:"client_urls"`
	PeerURLs   string `json:"peer_urls"`
	IsLearner  bool   `json:"is_learner"`
}

type userEntry struct {
	Username string `json:"username"`
	Roles    string `json:"roles"`
}

type roleEntry struct {
	Name        string `json:"name"`
	Permissions int    `json:"permissions"`
}

type actionResult struct {
	OK bool `json:"ok"`
}

func routes() []plugin.Route {
	return []plugin.Route{
		{ID: "etcd.status", Method: plugin.MethodGet, Path: "/status", Permission: "etcd.read", Risk: plugin.RiskSafe, AuditEvent: "etcd.status", Handle: statusRoute},
		{ID: "etcd.keys.list", Method: plugin.MethodGet, Path: "/keys", Permission: "etcd.keys.read", Risk: plugin.RiskSafe, AuditEvent: "etcd.keys.list", Handle: listKeys},
		{ID: "etcd.key.read", Method: plugin.MethodGet, Path: "/keys/{key}", Permission: "etcd.keys.read", Risk: plugin.RiskSafe, AuditEvent: "etcd.key.read", Handle: readKey},
		{ID: "etcd.key.write", Method: plugin.MethodPut, Path: "/keys/{key}", Permission: "etcd.keys.write", Risk: plugin.RiskWrite, AuditEvent: "etcd.key.write", Handle: writeKey},
		{ID: "etcd.key.delete", Method: plugin.MethodDelete, Path: "/keys/{key}", Permission: "etcd.keys.delete", Risk: plugin.RiskDestructive, AuditEvent: "etcd.key.delete", Handle: deleteKey},
		{ID: "etcd.leases.list", Method: plugin.MethodGet, Path: "/leases", Permission: "etcd.leases.read", Risk: plugin.RiskSafe, AuditEvent: "etcd.leases.list", Handle: listLeases},
		{ID: "etcd.lease.grant", Method: plugin.MethodPost, Path: "/leases", Permission: "etcd.leases.write", Risk: plugin.RiskWrite, AuditEvent: "etcd.lease.grant", Input: grantLeaseSchema(), Handle: grantLease},
		{ID: "etcd.lease.revoke", Method: plugin.MethodDelete, Path: "/leases/{id}", Permission: "etcd.leases.write", Risk: plugin.RiskDestructive, AuditEvent: "etcd.lease.revoke", Handle: revokeLease},
		{ID: "etcd.members.list", Method: plugin.MethodGet, Path: "/members", Permission: "etcd.read", Risk: plugin.RiskSafe, AuditEvent: "etcd.members.list", Handle: listMembers},
		{ID: "etcd.users.list", Method: plugin.MethodGet, Path: "/users", Permission: "etcd.auth.read", Risk: plugin.RiskSafe, AuditEvent: "etcd.users.list", Handle: listUsers},
		{ID: "etcd.user.create", Method: plugin.MethodPost, Path: "/users", Permission: "etcd.auth.write", Risk: plugin.RiskWrite, AuditEvent: "etcd.user.create", Input: userCreateSchema(), Handle: createUser},
		{ID: "etcd.user.delete", Method: plugin.MethodDelete, Path: "/users/{username}", Permission: "etcd.auth.write", Risk: plugin.RiskDestructive, AuditEvent: "etcd.user.delete", Handle: deleteUser},
		{ID: "etcd.user.grant_role", Method: plugin.MethodPost, Path: "/users/{username}/grant_role", Permission: "etcd.auth.write", Risk: plugin.RiskWrite, AuditEvent: "etcd.user.grant_role", Input: roleRefSchema(), Handle: grantUserRole},
		{ID: "etcd.user.revoke_role", Method: plugin.MethodPost, Path: "/users/{username}/revoke_role", Permission: "etcd.auth.write", Risk: plugin.RiskWrite, AuditEvent: "etcd.user.revoke_role", Input: roleRefSchema(), Handle: revokeUserRole},
		{ID: "etcd.roles.list", Method: plugin.MethodGet, Path: "/roles", Permission: "etcd.auth.read", Risk: plugin.RiskSafe, AuditEvent: "etcd.roles.list", Handle: listRoles},
		{ID: "etcd.role.create", Method: plugin.MethodPost, Path: "/roles", Permission: "etcd.auth.write", Risk: plugin.RiskWrite, AuditEvent: "etcd.role.create", Input: roleCreateSchema(), Handle: createRole},
		{ID: "etcd.role.delete", Method: plugin.MethodDelete, Path: "/roles/{name}", Permission: "etcd.auth.write", Risk: plugin.RiskDestructive, AuditEvent: "etcd.role.delete", Handle: deleteRole},
		{ID: "etcd.maintenance.compact", Method: plugin.MethodPost, Path: "/maintenance/compact", Permission: "etcd.maintenance", Risk: plugin.RiskPrivileged, AuditEvent: "etcd.maintenance.compact", Input: compactSchema(), Handle: compactHistory},
		{ID: "etcd.maintenance.defragment", Method: plugin.MethodPost, Path: "/maintenance/defragment", Permission: "etcd.maintenance", Risk: plugin.RiskPrivileged, AuditEvent: "etcd.maintenance.defragment", Handle: defragment},
		{ID: "etcd.watch", Method: plugin.MethodWS, Path: "/watch", Permission: "etcd.read", Risk: plugin.RiskSafe, AuditEvent: "etcd.watch", Stream: watchStream},
	}
}

func etcdSession(rc *plugin.RequestContext) (*Session, error) {
	s, err := unwrap(rc.Session)
	if err != nil {
		return nil, err
	}
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}
	return s, nil
}

func commandContext(parent context.Context, s *Session) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, s.opts.Timeout)
}

func ensureWritable(s *Session) error {
	if s.opts.ReadOnly {
		return fmt.Errorf("%w: read-only mode blocks write operations", plugin.ErrForbidden)
	}
	return nil
}

func statusRoute(rc *plugin.RequestContext) (any, error) {
	s, err := etcdSession(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := commandContext(rc.Ctx, s)
	defer cancel()
	status, err := s.client.Status(ctx, s.endpoint())
	if err != nil {
		return nil, etcdErr(err)
	}
	out := map[string]any{
		"version":            status.Version,
		"db_size":            status.DbSize,
		"db_size_in_use":     status.DbSizeInUse,
		"leader":             strconv.FormatUint(status.Leader, 16),
		"raft_index":         status.RaftIndex,
		"raft_term":          status.RaftTerm,
		"raft_applied_index": status.RaftAppliedIndex,
		"is_learner":         status.IsLearner,
		"endpoint":           s.endpoint(),
	}
	if len(status.Errors) > 0 {
		out["errors"] = strings.Join(status.Errors, "; ")
	}
	return out, nil
}

func listKeys(rc *plugin.RequestContext) (any, error) {
	s, err := etcdSession(rc)
	if err != nil {
		return nil, err
	}
	req, err := rc.Page()
	if err != nil {
		return nil, err
	}
	prefix := s.opts.KeyPrefix
	if search := strings.TrimSpace(req.Search()); search != "" {
		prefix = search
	}
	limit := req.Limit
	if limit <= 0 || limit > s.opts.PageLimit {
		limit = s.opts.PageLimit
	}
	start := prefix
	if req.Cursor != "" {
		start = req.Cursor
	}
	ctx, cancel := commandContext(rc.Ctx, s)
	defer cancel()
	resp, err := s.client.Get(ctx, start,
		clientv3.WithRange(clientv3.GetPrefixRangeEnd(prefix)),
		clientv3.WithKeysOnly(),
		clientv3.WithLimit(int64(limit)),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	if err != nil {
		return nil, etcdErr(err)
	}
	items := make([]keyEntry, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		items = append(items, keyEntry{Key: string(kv.Key)})
	}
	next := ""
	if resp.More && len(resp.Kvs) > 0 {
		next = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\x00"
	}
	return plugin.Page[keyEntry]{Items: items, NextCursor: next}, nil
}

func readKey(rc *plugin.RequestContext) (any, error) {
	s, err := etcdSession(rc)
	if err != nil {
		return nil, err
	}
	key := rc.Param("key")
	if key == "" {
		return nil, fmt.Errorf("%w: key is required", plugin.ErrInvalidInput)
	}
	ctx, cancel := commandContext(rc.Ctx, s)
	defer cancel()
	resp, err := s.client.Get(ctx, key)
	if err != nil {
		return nil, etcdErr(err)
	}
	if len(resp.Kvs) == 0 {
		return nil, plugin.ErrNotFound
	}
	kv := resp.Kvs[0]
	detail := keyDetail{
		Key:            key,
		Value:          string(kv.Value),
		CreateRevision: kv.CreateRevision,
		ModRevision:    kv.ModRevision,
		Version:        kv.Version,
	}
	if kv.Lease != 0 {
		detail.Lease = strconv.FormatInt(kv.Lease, 16)
		if ttl, err := s.client.TimeToLive(ctx, clientv3.LeaseID(kv.Lease)); err == nil && ttl.TTL > 0 {
			detail.TTL = ttl.TTL
		}
	}
	return detail, nil
}

func writeKey(rc *plugin.RequestContext) (any, error) {
	s, err := etcdSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	key := rc.Param("key")
	if key == "" {
		return nil, fmt.Errorf("%w: key is required", plugin.ErrInvalidInput)
	}
	var req struct {
		Value any `json:"value"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	ctx, cancel := commandContext(rc.Ctx, s)
	defer cancel()
	if _, err := s.client.Put(ctx, key, stringValue(req.Value)); err != nil {
		return nil, etcdErr(err)
	}
	return actionResult{OK: true}, nil
}

func deleteKey(rc *plugin.RequestContext) (any, error) {
	s, err := etcdSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	key := rc.Param("key")
	if key == "" {
		return nil, fmt.Errorf("%w: key is required", plugin.ErrInvalidInput)
	}
	ctx, cancel := commandContext(rc.Ctx, s)
	defer cancel()
	if _, err := s.client.Delete(ctx, key); err != nil {
		return nil, etcdErr(err)
	}
	return actionResult{OK: true}, nil
}

func listLeases(rc *plugin.RequestContext) (any, error) {
	s, err := etcdSession(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := commandContext(rc.Ctx, s)
	defer cancel()
	resp, err := s.client.Leases(ctx)
	if err != nil {
		return nil, etcdErr(err)
	}
	items := make([]leaseEntry, 0, len(resp.Leases))
	for _, l := range resp.Leases {
		entry := leaseEntry{ID: strconv.FormatInt(int64(l.ID), 16)}
		if ttl, err := s.client.TimeToLive(ctx, l.ID, clientv3.WithAttachedKeys()); err == nil {
			entry.TTL = ttl.TTL
			entry.GrantedTTL = ttl.GrantedTTL
			entry.Keys = len(ttl.Keys)
		}
		items = append(items, entry)
	}
	return page(items), nil
}

func grantLease(rc *plugin.RequestContext) (any, error) {
	s, err := etcdSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		TTL int64 `json:"ttl"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	if req.TTL <= 0 {
		return nil, fmt.Errorf("%w: TTL must be positive", plugin.ErrInvalidInput)
	}
	ctx, cancel := commandContext(rc.Ctx, s)
	defer cancel()
	resp, err := s.client.Grant(ctx, req.TTL)
	if err != nil {
		return nil, etcdErr(err)
	}
	return map[string]any{"ok": true, "id": strconv.FormatInt(int64(resp.ID), 16)}, nil
}

func revokeLease(rc *plugin.RequestContext) (any, error) {
	s, err := etcdSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	id, err := strconv.ParseInt(strings.TrimSpace(rc.Param("id")), 16, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: lease id must be a hex identifier", plugin.ErrInvalidInput)
	}
	ctx, cancel := commandContext(rc.Ctx, s)
	defer cancel()
	if _, err := s.client.Revoke(ctx, clientv3.LeaseID(id)); err != nil {
		return nil, etcdErr(err)
	}
	return actionResult{OK: true}, nil
}

func listMembers(rc *plugin.RequestContext) (any, error) {
	s, err := etcdSession(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := commandContext(rc.Ctx, s)
	defer cancel()
	resp, err := s.client.MemberList(ctx)
	if err != nil {
		return nil, etcdErr(err)
	}
	items := make([]memberEntry, 0, len(resp.Members))
	for _, m := range resp.Members {
		items = append(items, memberEntry{
			ID:         strconv.FormatUint(m.ID, 16),
			Name:       m.Name,
			ClientURLs: strings.Join(m.ClientURLs, ", "),
			PeerURLs:   strings.Join(m.PeerURLs, ", "),
			IsLearner:  m.IsLearner,
		})
	}
	return page(items), nil
}

func listUsers(rc *plugin.RequestContext) (any, error) {
	s, err := etcdSession(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := commandContext(rc.Ctx, s)
	defer cancel()
	resp, err := s.client.UserList(ctx)
	if err != nil {
		if authDisabled(err) {
			return page([]userEntry{}), nil
		}
		return nil, etcdErr(err)
	}
	items := make([]userEntry, 0, len(resp.Users))
	for _, username := range resp.Users {
		entry := userEntry{Username: username}
		if info, err := s.client.UserGet(ctx, username); err == nil {
			entry.Roles = strings.Join(info.Roles, ", ")
		}
		items = append(items, entry)
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].Username < items[j].Username })
	return page(items), nil
}

func createUser(rc *plugin.RequestContext) (any, error) {
	s, err := etcdSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.Username) == "" {
		return nil, fmt.Errorf("%w: username is required", plugin.ErrInvalidInput)
	}
	ctx, cancel := commandContext(rc.Ctx, s)
	defer cancel()
	if _, err := s.client.UserAdd(ctx, req.Username, req.Password); err != nil {
		return nil, etcdErr(err)
	}
	return actionResult{OK: true}, nil
}

func deleteUser(rc *plugin.RequestContext) (any, error) {
	s, err := etcdSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	username := strings.TrimSpace(rc.Param("username"))
	if username == "" {
		return nil, fmt.Errorf("%w: username is required", plugin.ErrInvalidInput)
	}
	ctx, cancel := commandContext(rc.Ctx, s)
	defer cancel()
	if _, err := s.client.UserDelete(ctx, username); err != nil {
		return nil, etcdErr(err)
	}
	return actionResult{OK: true}, nil
}

func grantUserRole(rc *plugin.RequestContext) (any, error) {
	return userRole(rc, true)
}

func revokeUserRole(rc *plugin.RequestContext) (any, error) {
	return userRole(rc, false)
}

func userRole(rc *plugin.RequestContext, grant bool) (any, error) {
	s, err := etcdSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	username := strings.TrimSpace(rc.Param("username"))
	if username == "" {
		return nil, fmt.Errorf("%w: username is required", plugin.ErrInvalidInput)
	}
	var req struct {
		Role string `json:"role"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.Role) == "" {
		return nil, fmt.Errorf("%w: role is required", plugin.ErrInvalidInput)
	}
	ctx, cancel := commandContext(rc.Ctx, s)
	defer cancel()
	if grant {
		_, err = s.client.UserGrantRole(ctx, username, req.Role)
	} else {
		_, err = s.client.UserRevokeRole(ctx, username, req.Role)
	}
	if err != nil {
		return nil, etcdErr(err)
	}
	return actionResult{OK: true}, nil
}

func listRoles(rc *plugin.RequestContext) (any, error) {
	s, err := etcdSession(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := commandContext(rc.Ctx, s)
	defer cancel()
	resp, err := s.client.RoleList(ctx)
	if err != nil {
		if authDisabled(err) {
			return page([]roleEntry{}), nil
		}
		return nil, etcdErr(err)
	}
	items := make([]roleEntry, 0, len(resp.Roles))
	for _, name := range resp.Roles {
		entry := roleEntry{Name: name}
		if info, err := s.client.RoleGet(ctx, name); err == nil {
			entry.Permissions = len(info.Perm)
		}
		items = append(items, entry)
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	return page(items), nil
}

func createRole(rc *plugin.RequestContext) (any, error) {
	s, err := etcdSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Name string `json:"name"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.Name) == "" {
		return nil, fmt.Errorf("%w: role name is required", plugin.ErrInvalidInput)
	}
	ctx, cancel := commandContext(rc.Ctx, s)
	defer cancel()
	if _, err := s.client.RoleAdd(ctx, req.Name); err != nil {
		return nil, etcdErr(err)
	}
	return actionResult{OK: true}, nil
}

func deleteRole(rc *plugin.RequestContext) (any, error) {
	s, err := etcdSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	name := strings.TrimSpace(rc.Param("name"))
	if name == "" {
		return nil, fmt.Errorf("%w: role name is required", plugin.ErrInvalidInput)
	}
	ctx, cancel := commandContext(rc.Ctx, s)
	defer cancel()
	if _, err := s.client.RoleDelete(ctx, name); err != nil {
		return nil, etcdErr(err)
	}
	return actionResult{OK: true}, nil
}

func compactHistory(rc *plugin.RequestContext) (any, error) {
	s, err := etcdSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Revision int64 `json:"revision"`
		Physical bool  `json:"physical"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	if req.Revision <= 0 {
		return nil, fmt.Errorf("%w: revision must be positive", plugin.ErrInvalidInput)
	}
	ctx, cancel := commandContext(rc.Ctx, s)
	defer cancel()
	opts := []clientv3.CompactOption{}
	if req.Physical {
		opts = append(opts, clientv3.WithCompactPhysical())
	}
	if _, err := s.client.Compact(ctx, req.Revision, opts...); err != nil {
		return nil, etcdErr(err)
	}
	return actionResult{OK: true}, nil
}

func defragment(rc *plugin.RequestContext) (any, error) {
	s, err := etcdSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	ctx, cancel := commandContext(rc.Ctx, s)
	defer cancel()
	if _, err := s.client.Defragment(ctx, s.endpoint()); err != nil {
		return nil, etcdErr(err)
	}
	return actionResult{OK: true}, nil
}

func watchStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := etcdSession(rc)
	if err != nil {
		return err
	}
	ctx := client.Context()
	prefix := s.opts.KeyPrefix
	if err := writeLogFrame(client, "Watching prefix "+prefix); err != nil {
		return err
	}
	wch := s.client.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithPrevKV())
	for {
		select {
		case <-ctx.Done():
			return nil
		case resp, ok := <-wch:
			if !ok {
				return nil
			}
			if err := resp.Err(); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return etcdErr(err)
			}
			for _, ev := range resp.Events {
				if err := writeLogFrame(client, formatEvent(ev)); err != nil {
					if ctx.Err() != nil || errors.Is(err, io.ErrClosedPipe) {
						return nil
					}
					return err
				}
			}
		}
	}
}

func formatEvent(ev *clientv3.Event) string {
	key := string(ev.Kv.Key)
	if ev.Type == clientv3.EventTypeDelete {
		return "[DELETE] " + key
	}
	value := string(ev.Kv.Value)
	if len(value) > 512 {
		value = value[:512] + "…"
	}
	return "[PUT] " + key + " = " + value
}

func writeLogFrame(w io.Writer, line string) error {
	frame, err := json.Marshal(struct {
		TS   string `json:"ts,omitempty"`
		Line string `json:"line"`
	}{
		TS:   time.Now().UTC().Format(time.RFC3339),
		Line: line,
	})
	if err != nil {
		return err
	}
	_, err = w.Write(frame)
	return err
}

func page[T any](items []T) plugin.Page[T] {
	total := len(items)
	return plugin.Page[T]{Items: items, Total: &total}
}

func stringValue(value any) string {
	if s, ok := value.(string); ok {
		return s
	}
	if value == nil {
		return ""
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprint(value)
	}
	return string(raw)
}

func authDisabled(err error) bool {
	return err != nil && strings.Contains(err.Error(), "authentication is not enabled")
}

func etcdErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
}
