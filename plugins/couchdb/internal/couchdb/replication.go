package couchdb

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const replicatorDB = "_replicator"

// endpointLabel renders a replication endpoint for display. CouchDB accepts a
// bare name, a URL, or an object with headers; credentials embedded in any of
// those are stripped so they never reach the browser or an audit record.
func endpointLabel(value any) string {
	switch endpoint := value.(type) {
	case string:
		return redactURL(endpoint)
	case map[string]any:
		return redactURL(stringOf(endpoint["url"]))
	default:
		return ""
	}
}

func redactURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" || !strings.Contains(raw, "://") {
		return raw
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	if parsed.User != nil {
		parsed.User = url.User(parsed.User.Username())
	}
	parsed.RawQuery = ""
	return parsed.String()
}

type schedulerDoc struct {
	Database    string `json:"database"`
	DocID       string `json:"doc_id"`
	ID          string `json:"id"`
	Node        string `json:"node"`
	Source      any    `json:"source"`
	Target      any    `json:"target"`
	State       string `json:"state"`
	ErrorCount  int    `json:"error_count"`
	Info        any    `json:"info"`
	StartTime   any    `json:"start_time"`
	LastUpdated any    `json:"last_updated"`
}

func (s *Session) schedulerDocs(rc *plugin.RequestContext) ([]schedulerDoc, error) {
	var out struct {
		Docs []schedulerDoc `json:"docs"`
	}
	if err := s.client.do(rc.Ctx, http.MethodGet, "/_scheduler/docs", nil, nil, &out); err != nil {
		return nil, err
	}
	return out.Docs, nil
}

func replicationRef(id string) plugin.ResourceIdentity {
	return plugin.ResourceIdentity{Kind: "replication", Name: id, UID: id}
}

// replicationRows joins the durable _replicator documents with the scheduler's
// live view, so a replication that has not been picked up yet is still listed.
func (s *Session) replicationRows(rc *plugin.RequestContext) ([]row, error) {
	var docs allDocsResponse
	query := url.Values{"include_docs": []string{"true"}, "limit": []string{strconv.Itoa(s.boundedLimit(0))}}
	if err := s.client.do(rc.Ctx, http.MethodGet, dbPath(replicatorDB)+"/_all_docs", query, nil, &docs); err != nil {
		return nil, err
	}
	states := map[string]schedulerDoc{}
	if scheduled, err := s.schedulerDocs(rc); err == nil {
		for _, doc := range scheduled {
			states[doc.DocID] = doc
		}
	}

	rows := make([]row, 0, len(docs.Rows))
	for _, entry := range docs.Rows {
		if entry.ID == "" || strings.HasPrefix(entry.ID, "_design/") {
			continue
		}
		doc := entry.Doc
		item := row{
			"name":          entry.ID,
			"rev":           stringOf(entry.Value["rev"]),
			"source":        s.shortenEndpoint(rc, endpointLabel(doc["source"])),
			"target":        s.shortenEndpoint(rc, endpointLabel(doc["target"])),
			"continuous":    doc["continuous"] == true,
			"create_target": doc["create_target"] == true,
			"owner":         stringOf(doc["owner"]),
			"state":         defaultString(stringOf(doc["_replication_state"]), "pending"),
			"error_count":   0,
			"ref":           replicationRef(entry.ID),
		}
		if scheduled, ok := states[entry.ID]; ok {
			s.mergeSchedulerState(rc, item, scheduled)
		}
		rows = append(rows, item)
	}
	return rows, nil
}

func (s *Session) mergeSchedulerState(rc *plugin.RequestContext, item row, doc schedulerDoc) {
	item["state"] = defaultString(doc.State, stringOf(item["state"]))
	item["error_count"] = doc.ErrorCount
	item["node"] = doc.Node
	item["job_id"] = doc.ID
	item["info"] = doc.Info
	if source := endpointLabel(doc.Source); source != "" {
		item["source"] = s.shortenEndpoint(rc, source)
	}
	if target := endpointLabel(doc.Target); target != "" {
		item["target"] = s.shortenEndpoint(rc, target)
	}
	if started, ok := unixTime(doc.StartTime); ok {
		item["start_time"] = started.UTC().Format(time.RFC3339)
	}
	if updated, ok := unixTime(doc.LastUpdated); ok {
		item["last_updated"] = updated.UTC().Format(time.RFC3339)
	}
	if info := mapOf(doc.Info); info != nil {
		item["docs_written"] = numberOf(info["docs_written"])
		item["docs_read"] = numberOf(info["docs_read"])
		item["doc_write_failures"] = numberOf(info["doc_write_failures"])
		if changes := numberOf(info["changes_pending"]); changes > 0 {
			item["changes_pending"] = changes
		}
		item["progress"] = replicationProgress(info)
	}
}

// replicationProgress derives a percentage from the scheduler's read/pending
// counters, which CouchDB reports instead of a progress value for doc-based jobs.
func replicationProgress(info map[string]any) float64 {
	done := numberOf(info["docs_written"])
	pending := numberOf(info["changes_pending"])
	if total := done + pending; total > 0 {
		return round1(done / total * 100)
	}
	if done > 0 {
		return 100
	}
	return 0
}

func listReplications(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	rows, err := s.replicationRows(rc)
	if err != nil {
		return nil, err
	}
	return broker.PageRows(rc, rows)
}

func treeReplications(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	rows, err := s.replicationRows(rc)
	if err != nil {
		return nil, err
	}
	nodes := make([]plugin.TreeNode, 0, len(rows))
	for _, item := range rows {
		name := stringOf(item["name"])
		ref := replicationRef(name)
		nodes = append(nodes, plugin.TreeNode{
			Key: "replication:" + name, Label: name, Icon: icon("refresh-cw"), Ref: &ref, Leaf: true,
			Data: map[string]any{"state": item["state"]},
		})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes}, nil
}

func watchReplications(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := session(rc)
	if err != nil {
		return err
	}
	return pollWatch(rc, client, 5*time.Second, func() ([]plugin.ResourceEvent, error) {
		rows, err := s.replicationRows(rc)
		if err != nil {
			return nil, err
		}
		return modifiedEvents(rows, "replication", func(item row) plugin.ResourceIdentity {
			name := stringOf(item["name"])
			return plugin.ResourceIdentity{Name: name, UID: name}
		}), nil
	})
}

func replicationID(rc *plugin.RequestContext) (string, error) {
	id := strings.TrimSpace(rc.Param("id"))
	if id == "" || strings.HasPrefix(id, "_design/") {
		return "", fmt.Errorf("%w: a replication document id is required", plugin.ErrInvalidInput)
	}
	return id, nil
}

func readReplication(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	id, err := replicationID(rc)
	if err != nil {
		return nil, err
	}
	rows, err := s.replicationRows(rc)
	if err != nil {
		return nil, err
	}
	for _, item := range rows {
		if stringOf(item["name"]) == id {
			return item, nil
		}
	}
	return nil, fmt.Errorf("%w: replication %q", plugin.ErrNotFound, id)
}

// replicationEvents turns the scheduler's per-job history into a timeline. It is
// the only place CouchDB records why a replication crashed and restarted.
func replicationEvents(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	id, err := replicationID(rc)
	if err != nil {
		return nil, err
	}
	var out struct {
		Jobs []struct {
			ID      string `json:"id"`
			DocID   string `json:"doc_id"`
			Node    string `json:"node"`
			History []row  `json:"history"`
		} `json:"jobs"`
	}
	if err := s.client.do(rc.Ctx, http.MethodGet, "/_scheduler/jobs", nil, nil, &out); err != nil {
		return nil, err
	}
	items := make([]row, 0)
	for _, job := range out.Jobs {
		if job.DocID != id {
			continue
		}
		for _, event := range job.History {
			when, _ := unixTime(event["timestamp"])
			kind := stringOf(event["type"])
			items = append(items, timelineRow(when, kind, historyMessage(event, job.Node), historySeverity(kind), replicationRef(id)))
		}
	}
	if len(items) == 0 {
		if doc, err := s.replicationDoc(rc, id); err == nil {
			when, _ := unixTime(doc["_replication_state_time"])
			state := defaultString(stringOf(doc["_replication_state"]), "pending")
			items = append(items, timelineRow(when, state, "Recorded on the _replicator document; the scheduler has no live job for it.",
				historySeverity(state), replicationRef(id)))
		}
	}
	sort.SliceStable(items, func(i, j int) bool { return stringOf(items[i]["time"]) > stringOf(items[j]["time"]) })
	return broker.PageRows(rc, items)
}

func historyMessage(event row, node string) string {
	parts := make([]string, 0, 3)
	if reason := stringOf(event["reason"]); reason != "" {
		parts = append(parts, reason)
	}
	if detail := stringOf(event["info"]); detail != "" && detail != "<nil>" {
		parts = append(parts, detail)
	}
	if node != "" {
		parts = append(parts, "on "+node)
	}
	if len(parts) == 0 {
		return "Scheduler transition"
	}
	return strings.Join(parts, " · ")
}

func historySeverity(kind string) plugin.Severity {
	switch kind {
	case "started", "running":
		return plugin.SeveritySuccess
	case "completed":
		return plugin.SeverityInfo
	case "crashed", "error", "failed":
		return plugin.SeverityDanger
	case "stopped", "pending", "added":
		return plugin.SeverityWarn
	default:
		return plugin.SeveritySecondary
	}
}

func (s *Session) replicationDoc(rc *plugin.RequestContext, id string) (row, error) {
	return s.fetchDocument(rc, replicatorDB, id, nil)
}

// replicationEndpoint expands a bare database name into an absolute URL, because
// CouchDB 3 rejects local endpoint names inside _replicator documents. The
// connection's own credentials are attached only when the operator asks for it,
// since a _replicator document stores them in clear text.
func (s *Session) replicationEndpoint(rc *plugin.RequestContext, raw string, withCredentials bool) any {
	raw = strings.TrimSpace(raw)
	if strings.Contains(raw, "://") {
		return raw
	}
	endpoint := s.selfEndpoint(rc) + dbPath(raw)
	if !withCredentials || s.opts.username == "" {
		return endpoint
	}
	return row{"url": endpoint, "auth": row{"basic": row{
		"username": s.opts.username,
		"password": s.opts.password,
	}}}
}

func createReplication(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var in struct {
		ID                      string   `json:"id"`
		Source                  string   `json:"source" validate:"required"`
		Target                  string   `json:"target" validate:"required"`
		Continuous              bool     `json:"continuous"`
		CreateTarget            bool     `json:"create_target"`
		UseConnectionCredential bool     `json:"use_connection_credentials"`
		Selector                any      `json:"selector"`
		DocIDs                  []string `json:"doc_ids"`
	}
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	doc := row{
		"source":     s.replicationEndpoint(rc, in.Source, in.UseConnectionCredential),
		"target":     s.replicationEndpoint(rc, in.Target, in.UseConnectionCredential),
		"continuous": in.Continuous,
	}
	if in.CreateTarget {
		doc["create_target"] = true
	}
	if selector := normalizeSelector(in.Selector); selector != nil {
		doc["selector"] = selector
	}
	if ids := toAnySlice(in.DocIDs); len(ids) > 0 {
		doc["doc_ids"] = ids
	}
	id := strings.TrimSpace(in.ID)
	if id == "" {
		id = fmt.Sprintf("shellcn-%d", time.Now().UTC().UnixNano())
	}
	doc["_id"] = id

	// The _replicator database is created lazily on single-node installs.
	if err := s.ensureReplicator(rc); err != nil {
		return nil, err
	}
	out, err := s.putDocument(rc, replicatorDB, id, doc)
	if err != nil {
		return nil, err
	}
	return row{"ok": true, "id": id, "rev": stringOf(out["rev"])}, nil
}

func (s *Session) ensureReplicator(rc *plugin.RequestContext) error {
	var info row
	if err := s.client.do(rc.Ctx, http.MethodGet, dbPath(replicatorDB), nil, nil, &info); err == nil {
		return nil
	}
	var created row
	if err := s.client.do(rc.Ctx, http.MethodPut, dbPath(replicatorDB), nil, nil, &created); err != nil {
		return fmt.Errorf("%w: the _replicator database is missing and could not be created: %v", plugin.ErrUnavailable, err)
	}
	return nil
}

func deleteReplication(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	id, err := replicationID(rc)
	if err != nil {
		return nil, err
	}
	rev, err := s.documentRev(rc, replicatorDB, id, "")
	if err != nil {
		return nil, err
	}
	var out row
	if err := s.client.do(rc.Ctx, http.MethodDelete, docPath(replicatorDB, id), url.Values{"rev": []string{rev}}, nil, &out); err != nil {
		return nil, err
	}
	return row{"ok": true, "id": id}, nil
}

func schedulerJobs(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	var out struct {
		Jobs []struct {
			ID        string `json:"id"`
			Database  string `json:"database"`
			DocID     string `json:"doc_id"`
			Node      string `json:"node"`
			Source    any    `json:"source"`
			Target    any    `json:"target"`
			StartTime any    `json:"start_time"`
			History   []row  `json:"history"`
		} `json:"jobs"`
	}
	if err := s.client.do(rc.Ctx, http.MethodGet, "/_scheduler/jobs", nil, nil, &out); err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(out.Jobs))
	for _, job := range out.Jobs {
		item := row{
			"id":       defaultString(job.DocID, job.ID),
			"database": job.Database,
			"source":   s.shortenEndpoint(rc, endpointLabel(job.Source)),
			"target":   s.shortenEndpoint(rc, endpointLabel(job.Target)),
			"node":     job.Node,
			"state":    latestHistoryState(job.History),
		}
		if started, ok := unixTime(job.StartTime); ok {
			item["start_time"] = started.UTC().Format(time.RFC3339)
		}
		rows = append(rows, item)
	}
	return broker.PageRows(rc, rows)
}

func latestHistoryState(history []row) string {
	if len(history) == 0 {
		return "running"
	}
	return defaultString(stringOf(history[0]["type"]), "running")
}

// --- replication topology graph -------------------------------------------------

// shortenEndpoint collapses a URL that points back at this connection into the
// bare database name, so a local-to-local replication reads as one hop between
// two databases instead of two opaque URLs.
func (s *Session) shortenEndpoint(rc *plugin.RequestContext, label string) string {
	bare := label
	if parsed, err := url.Parse(label); err == nil && parsed.User != nil {
		parsed.User = nil
		bare = parsed.String()
	}
	for _, base := range []string{s.selfEndpoint(rc), s.opts.baseURL()} {
		if rest, ok := strings.CutPrefix(bare, base+"/"); ok {
			if decoded, err := url.PathUnescape(strings.TrimSuffix(rest, "/")); err == nil {
				return decoded
			}
		}
	}
	return label
}

// replicationGraph draws every configured replication as an edge between its
// endpoints, with local databases and remote clusters distinguished. It is the
// view CouchDB itself never offers: which databases feed which, and which links
// are currently broken.
func replicationGraph(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	rows, err := s.replicationRows(rc)
	if err != nil {
		return nil, err
	}
	local := map[string]bool{}
	for _, name := range s.databaseNames(rc) {
		local[name] = true
	}

	nodes := map[string]row{}
	order := make([]string, 0)
	addNode := func(label string) string {
		label = s.shortenEndpoint(rc, label)
		if label == "" {
			label = "(unset)"
		}
		id := "endpoint:" + label
		if _, ok := nodes[id]; ok {
			return id
		}
		group := "remote"
		summary := "Remote CouchDB endpoint"
		if local[label] {
			group = "local"
			summary = "Local database on " + s.opts.addr()
		}
		nodes[id] = row{
			"id": id, "label": label, "group": group, "summary": summary,
			"properties": row{"endpoint": label},
		}
		order = append(order, id)
		return id
	}

	edges := make([]row, 0, len(rows))
	for _, item := range rows {
		source := addNode(stringOf(item["source"]))
		target := addNode(stringOf(item["target"]))
		state := stringOf(item["state"])
		label := state
		if item["continuous"] == true {
			label += " · continuous"
		}
		edges = append(edges, row{
			"source": source, "target": target, "label": label,
			"id": "replication:" + stringOf(item["name"]),
		})
	}

	graphNodes := make([]row, 0, len(order))
	for _, id := range order {
		graphNodes = append(graphNodes, nodes[id])
	}
	return row{"nodes": graphNodes, "edges": edges}, nil
}
