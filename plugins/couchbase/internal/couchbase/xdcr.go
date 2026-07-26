package couchbase

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugin/webproxy"
)

type clusterTask struct {
	Type            string  `json:"type"`
	Status          string  `json:"status"`
	ID              string  `json:"id"`
	Source          string  `json:"source"`
	Target          string  `json:"target"`
	ReplicationType string  `json:"replicationType"`
	Continuous      bool    `json:"continuous"`
	FilterExpr      string  `json:"filterExpression"`
	PauseRequested  bool    `json:"pauseRequested"`
	ChangesLeft     float64 `json:"changesLeft"`
	DocsChecked     float64 `json:"docsChecked"`
	DocsWritten     float64 `json:"docsWritten"`
	MaxVBReps       float64 `json:"maxVBReps"`
	Errors          []any   `json:"errors"`
	Progress        float64 `json:"progress"`
	SettingsURI     string  `json:"settingsURI"`
}

func (s *Session) tasks(ctx context.Context) ([]clusterTask, error) {
	var tasks []clusterTask
	if err := s.mgmtGet(ctx, "/pools/default/tasks", &tasks); err != nil {
		return nil, err
	}
	return tasks, nil
}

func replicationUID(id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(id))
}

func decodeReplicationUID(uid string) (string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(uid))
	if err != nil || len(raw) == 0 {
		return "", fmt.Errorf("%w: invalid replication reference", plugin.ErrInvalidInput)
	}
	return string(raw), nil
}

func (t clusterTask) row() row {
	status := strings.ToLower(t.Status)
	if t.PauseRequested && status == "running" {
		status = "pausing"
	}
	return row{
		"ref":              plugin.ResourceIdentity{Kind: "replication", Name: t.ID, UID: replicationUID(t.ID)},
		"id":               t.ID,
		"source":           t.Source,
		"target":           t.Target,
		"status":           status,
		"replication_type": t.ReplicationType,
		"continuous":       t.Continuous,
		"filter":           t.FilterExpr,
		"changes_left":     t.ChangesLeft,
		"docs_checked":     t.DocsChecked,
		"docs_written":     t.DocsWritten,
		"max_vb_reps":      t.MaxVBReps,
		"errors":           len(t.Errors),
		"progress":         round1(t.Progress),
	}
}

func replicationsList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	tasks, err := s.tasks(rc.Ctx)
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(tasks))
	for _, task := range tasks {
		if !strings.EqualFold(task.Type, "xdcr") || task.ID == "" {
			continue
		}
		rows = append(rows, task.row())
	}
	return broker.PageRows(rc, rows)
}

func replicationRead(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	id, err := decodeReplicationUID(rc.Param("id"))
	if err != nil {
		return nil, err
	}
	tasks, err := s.tasks(rc.Ctx)
	if err != nil {
		return nil, err
	}
	for _, task := range tasks {
		if task.ID != id {
			continue
		}
		item := task.row()
		item["settings_uri"] = task.SettingsURI
		return item, nil
	}
	return nil, fmt.Errorf("%w: replication %q not found", plugin.ErrNotFound, id)
}

func remotesList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var remotes []struct {
		Name               string `json:"name"`
		UUID               string `json:"uuid"`
		Hostname           string `json:"hostname"`
		Username           string `json:"username"`
		SecureType         string `json:"secureType"`
		Deleted            bool   `json:"deleted"`
		DemandEncr         bool   `json:"demandEncryption"`
		ConnectivityStatus string `json:"connectivityStatus"`
	}
	if err := s.mgmtGet(rc.Ctx, "/pools/default/remoteClusters", &remotes); err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(remotes))
	for _, remote := range remotes {
		status := strings.ToLower(remote.ConnectivityStatus)
		if status == "" {
			status = "unknown"
		}
		if remote.Deleted {
			status = "deleted"
		}
		rows = append(rows, row{
			"name":       remote.Name,
			"uuid":       remote.UUID,
			"hostname":   remote.Hostname,
			"username":   remote.Username,
			"status":     status,
			"secure":     remote.SecureType != "" || remote.DemandEncr,
			"encryption": stringDefault(remote.SecureType, "none"),
		})
	}
	return broker.PageRows(rc, rows)
}

func usersList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var users []struct {
		ID     string `json:"id"`
		Name   string `json:"name"`
		Domain string `json:"domain"`
		Roles  []struct {
			Role           string `json:"role"`
			BucketName     string `json:"bucket_name"`
			ScopeName      string `json:"scope_name"`
			CollectionName string `json:"collection_name"`
		} `json:"roles"`
		Groups             []string `json:"groups"`
		PasswordChangeDate string   `json:"password_change_date"`
	}
	if err := s.mgmtGet(rc.Ctx, "/settings/rbac/users", &users); err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(users))
	for _, user := range users {
		roles := make([]string, 0, len(user.Roles))
		for _, role := range user.Roles {
			scope := role.BucketName
			if role.ScopeName != "" {
				scope += "." + role.ScopeName
			}
			if role.CollectionName != "" {
				scope += "." + role.CollectionName
			}
			if scope == "" {
				roles = append(roles, role.Role)
				continue
			}
			roles = append(roles, role.Role+"["+scope+"]")
		}
		rows = append(rows, row{
			"id":            user.ID,
			"name":          user.Name,
			"domain":        user.Domain,
			"roles":         strings.Join(roles, ", "),
			"role_count":    len(user.Roles),
			"groups":        strings.Join(user.Groups, ", "),
			"password_date": user.PasswordChangeDate,
		})
	}
	return broker.PageRows(rc, rows)
}

func consoleURL(rc *plugin.RequestContext) (any, error) {
	return row{"url": rc.ProxyURL()}, nil
}

var _ plugin.HTTPProxy = (*Session)(nil)

// ServeHTTPProxy exposes the Couchbase Web Console through the gateway's
// per-connection proxy mount, so operators can reach the vendor UI without a
// separate network path. Egress still goes through cfg.Net.
func (s *Session) ServeHTTPProxy(w http.ResponseWriter, r *http.Request) {
	base, err := url.Parse(s.opts.managementURL())
	if err != nil {
		http.Error(w, "no upstream", http.StatusBadGateway)
		return
	}
	webproxy.Serve(w, r, webproxy.Options{
		Base:         base,
		Transport:    s.proxyTransport(),
		UpstreamPath: r.URL.Path,
		PublicPrefix: plugin.RequestProxyPrefix(r),
	})
}
