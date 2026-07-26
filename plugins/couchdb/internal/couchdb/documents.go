package couchdb

import (
	"bufio"
	"encoding/json"
	"errors"
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

// gridReserved are the row fields the documents grid owns; they are never
// written back into a CouchDB document body.
var gridReserved = map[string]bool{"id": true, "rev": true, "deleted": true, "conflicts": true, "ref": true}

// couchReserved are document fields CouchDB manages; they are hidden from the
// grid so a user cannot corrupt them through a cell edit.
var couchReserved = map[string]bool{"_id": true, "_rev": true, "_conflicts": true, "_deleted": true, "_revisions": true, "_revs_info": true, "_attachments": true}

type allDocsResponse struct {
	TotalRows int `json:"total_rows"`
	Offset    int `json:"offset"`
	Rows      []struct {
		ID    string `json:"id"`
		Key   any    `json:"key"`
		Value row    `json:"value"`
		Doc   row    `json:"doc"`
		Error string `json:"error"`
	} `json:"rows"`
}

// allDocs reads one page of _all_docs. A search term becomes a startkey/endkey
// range so the scan stays index-backed instead of filtering a fetched page.
func (s *Session) allDocs(rc *plugin.RequestContext, db string, limit, skip int, term string, includeDocs bool) (allDocsResponse, error) {
	query := url.Values{
		"limit":     []string{strconv.Itoa(limit)},
		"skip":      []string{strconv.Itoa(skip)},
		"conflicts": []string{"true"},
	}
	if includeDocs {
		query.Set("include_docs", "true")
	}
	if term != "" {
		start, _ := json.Marshal(term)
		end, _ := json.Marshal(term + "￰")
		query.Set("startkey", string(start))
		query.Set("endkey", string(end))
	}
	var out allDocsResponse
	err := s.client.do(rc.Ctx, http.MethodGet, dbPath(db)+"/_all_docs", query, nil, &out)
	return out, err
}

func documentRef(db, id string) plugin.ResourceIdentity {
	return plugin.ResourceIdentity{Kind: "document", Namespace: db, Name: id, UID: db + "/" + id}
}

func documentRow(db, id string, rev string, doc row, conflicts []any) row {
	item := row{
		"id":        id,
		"rev":       rev,
		"deleted":   false,
		"conflicts": len(conflicts),
		"ref":       documentRef(db, id),
	}
	for key, value := range doc {
		if couchReserved[key] || gridReserved[key] {
			continue
		}
		item[key] = value
	}
	if doc["_deleted"] == true {
		item["deleted"] = true
	}
	return item
}

func listDocuments(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	page, err := rc.Page()
	if err != nil {
		return nil, err
	}
	limit := s.boundedLimit(page.Limit)
	skip := 0
	if page.Cursor != "" {
		if skip, err = strconv.Atoi(page.Cursor); err != nil || skip < 0 {
			return nil, fmt.Errorf("%w: invalid cursor", plugin.ErrInvalidInput)
		}
	}

	res, err := s.allDocs(rc, db, limit, skip, page.Search(), true)
	if err != nil {
		return nil, err
	}
	items := make([]row, 0, len(res.Rows))
	for _, entry := range res.Rows {
		if entry.Error != "" || entry.ID == "" {
			continue
		}
		rev := stringOf(entry.Value["rev"])
		items = append(items, documentRow(db, entry.ID, rev, entry.Doc, sliceOf(entry.Doc["_conflicts"])))
	}
	out := plugin.Page[row]{Items: items}
	if res.TotalRows > 0 {
		total := res.TotalRows
		out.Total = &total
	}
	if len(items) == limit {
		out.NextCursor = strconv.Itoa(skip + limit)
	}
	return out, nil
}

// documentColumns derives the editable grid's columns from a sample of the
// database. CouchDB is schemaless, so the shape is discovered, never assumed.
func documentColumns(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	sample := s.boundedLimit(0)
	res, err := s.allDocs(rc, db, sample, 0, "", true)
	if err != nil {
		return nil, err
	}
	kinds := map[string]string{}
	for _, entry := range res.Rows {
		for key, value := range entry.Doc {
			if couchReserved[key] || gridReserved[key] {
				continue
			}
			kinds[key] = mergeValueKind(kinds[key], valueKind(value))
		}
	}

	columns := []row{
		{"name": "id", "label": "_id", "type": "text", "readOnly": true},
		{"name": "rev", "label": "_rev", "type": "text", "readOnly": true},
		{"name": "conflicts", "label": "Conflicts", "type": "number", "readOnly": true},
		{"name": "deleted", "label": "Deleted", "type": "bool", "readOnly": true},
	}
	for _, key := range sortedKeys(kinds) {
		kind := kinds[key]
		columns = append(columns, row{
			"name": key, "label": key, "type": kind,
			"editable": true, "editor": columnEditor(kind), "nullable": true,
		})
	}
	return plugin.Page[row]{Items: columns}, nil
}

func valueKind(value any) string {
	switch value.(type) {
	case bool:
		return "bool"
	case float64, int, int64:
		return "number"
	case string:
		return "text"
	case nil:
		return ""
	default:
		return "json"
	}
}

func mergeValueKind(existing, next string) string {
	switch {
	case next == "":
		return existing
	case existing == "" || existing == next:
		return next
	default:
		return "json"
	}
}

func columnEditor(kind string) string {
	switch kind {
	case "bool":
		return string(plugin.ColumnEditorToggle)
	case "number":
		return string(plugin.ColumnEditorNumber)
	case "json":
		return string(plugin.ColumnEditorJSON)
	default:
		return string(plugin.ColumnEditorText)
	}
}

func (s *Session) fetchDocument(rc *plugin.RequestContext, db, id string, query url.Values) (row, error) {
	var doc row
	if err := s.client.do(rc.Ctx, http.MethodGet, docPath(db, id), query, nil, &doc); err != nil {
		return nil, err
	}
	return doc, nil
}

func readDocument(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	id, err := docParam(rc)
	if err != nil {
		return nil, err
	}
	return s.fetchDocument(rc, db, id, url.Values{"conflicts": []string{"true"}})
}

func watchDocument(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, db, err := dbSession(rc)
	if err != nil {
		return err
	}
	id, err := docParam(rc)
	if err != nil {
		return err
	}
	return pollObject(rc, client, 5*time.Second, func() (any, string, error) {
		doc, err := s.fetchDocument(rc, db, id, url.Values{"conflicts": []string{"true"}})
		if err != nil {
			return nil, "", err
		}
		return doc, stringOf(doc["_rev"]), nil
	})
}

// putDocument writes a document and turns CouchDB's bare 409 into an actionable
// message that names the revision the caller must rebase on.
func (s *Session) putDocument(rc *plugin.RequestContext, db, id string, doc row) (row, error) {
	var out row
	err := s.client.do(rc.Ctx, http.MethodPut, docPath(db, id), nil, doc, &out)
	if err == nil {
		return out, nil
	}
	if !errors.Is(err, plugin.ErrConflict) {
		return nil, err
	}
	current, readErr := s.fetchDocument(rc, db, id, nil)
	if readErr != nil {
		return nil, err
	}
	return nil, fmt.Errorf("%w: document update conflict on %q: your revision %q is stale, the server is at %q. Reload the document and reapply the change",
		plugin.ErrConflict, id, stringOf(doc["_rev"]), stringOf(current["_rev"]))
}

func createDocument(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	doc, err := contentBody(rc)
	if err != nil {
		return nil, err
	}
	delete(doc, "_rev")
	if id := strings.TrimSpace(stringOf(doc["_id"])); id != "" {
		out, err := s.putDocument(rc, db, id, doc)
		if err != nil {
			return nil, err
		}
		return row{"ok": true, "id": stringOf(out["id"]), "rev": stringOf(out["rev"])}, nil
	}
	delete(doc, "_id")
	var out row
	if err := s.client.do(rc.Ctx, http.MethodPost, dbPath(db), nil, doc, &out); err != nil {
		return nil, err
	}
	return row{"ok": true, "id": stringOf(out["id"]), "rev": stringOf(out["rev"])}, nil
}

func updateDocument(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	id, err := docParam(rc)
	if err != nil {
		return nil, err
	}
	doc, err := contentBody(rc)
	if err != nil {
		return nil, err
	}
	doc["_id"] = id
	if stringOf(doc["_rev"]) == "" {
		current, err := s.fetchDocument(rc, db, id, nil)
		if err != nil {
			return nil, err
		}
		doc["_rev"] = current["_rev"]
	}
	// _conflicts is a read-only projection CouchDB rejects on write.
	delete(doc, "_conflicts")
	out, err := s.putDocument(rc, db, id, doc)
	if err != nil {
		return nil, err
	}
	stored, err := s.fetchDocument(rc, db, id, url.Values{"conflicts": []string{"true"}})
	if err != nil {
		return nil, err
	}
	content, err := editorContent(stored)
	if err != nil {
		return nil, err
	}
	return row{"ok": true, "id": id, "rev": stringOf(out["rev"]), "content": content}, nil
}

// documentRev resolves the revision to write against: the caller's when supplied
// (so a stale edit is still rejected), otherwise the current one.
func (s *Session) documentRev(rc *plugin.RequestContext, db, id, provided string) (string, error) {
	if rev := strings.TrimSpace(provided); rev != "" {
		return rev, nil
	}
	current, err := s.fetchDocument(rc, db, id, nil)
	if err != nil {
		return "", err
	}
	rev := stringOf(current["_rev"])
	if rev == "" {
		return "", fmt.Errorf("%w: document %q has no revision", plugin.ErrNotFound, id)
	}
	return rev, nil
}

func deleteDocument(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	id, err := docParam(rc)
	if err != nil {
		return nil, err
	}
	var in struct {
		Rev string `json:"rev"`
	}
	if len(rc.Body()) > 0 {
		if err := rc.Bind(&in); err != nil {
			return nil, err
		}
	}
	rev, err := s.documentRev(rc, db, id, in.Rev)
	if err != nil {
		return nil, err
	}
	var out row
	if err := s.client.do(rc.Ctx, http.MethodDelete, docPath(db, id), url.Values{"rev": []string{rev}}, nil, &out); err != nil {
		return nil, err
	}
	return row{"ok": true, "id": id, "rev": stringOf(out["rev"])}, nil
}

// purgeDocument removes a document and every revision from this node. It is
// deliberately separate from delete: purge is not replicated and can be undone
// by any replica that still holds the document.
func purgeDocument(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	id, err := docParam(rc)
	if err != nil {
		return nil, err
	}
	// _purge takes leaf revisions; the winner, the conflicting leaves and the
	// deleted leaves are exactly the branches that still hold data on this node.
	doc, err := s.fetchDocument(rc, db, id, url.Values{
		"conflicts":         []string{"true"},
		"deleted_conflicts": []string{"true"},
	})
	if err != nil {
		return nil, err
	}
	revs := []any{stringOf(doc["_rev"])}
	revs = append(revs, sliceOf(doc["_conflicts"])...)
	revs = append(revs, sliceOf(doc["_deleted_conflicts"])...)
	var out row
	if err := s.client.do(rc.Ctx, http.MethodPost, dbPath(db)+"/_purge", nil, row{id: revs}, &out); err != nil {
		return nil, err
	}
	// CouchDB ignores revisions that are not leaves, so the count it reports back
	// is the one to show.
	purged := len(revs)
	if reported, ok := mapOf(out["purged"])[id]; ok {
		purged = len(sliceOf(reported))
	}
	return row{"ok": true, "id": id, "purged": purged}, nil
}

func bulkDocuments(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	body, err := contentBody(rc)
	if err != nil {
		return nil, err
	}
	docs := sliceOf(body["docs"])
	if len(docs) == 0 {
		return nil, fmt.Errorf("%w: the payload must contain a non-empty \"docs\" array", plugin.ErrInvalidInput)
	}
	var results []row
	if err := s.client.do(rc.Ctx, http.MethodPost, dbPath(db)+"/_bulk_docs", nil, row{"docs": docs}, &results); err != nil {
		return nil, err
	}
	ok, failed := 0, make([]row, 0)
	for _, result := range results {
		if stringOf(result["error"]) != "" {
			failed = append(failed, result)
			continue
		}
		ok++
	}
	return row{"ok": len(failed) == 0, "written": ok, "failed": failed, "submitted": len(docs)}, nil
}

func bindGridRow(rc *plugin.RequestContext) (row, error) {
	var raw map[string]any
	if err := json.Unmarshal(rc.Body(), &raw); err != nil {
		return nil, fmt.Errorf("%w: the grid row must be a JSON object", plugin.ErrInvalidInput)
	}
	if inner, ok := raw["row"].(map[string]any); ok {
		return inner, nil
	}
	return raw, nil
}

// gridFields extracts the user-editable document fields from a grid row.
func gridFields(item row) row {
	out := row{}
	for key, value := range item {
		if gridReserved[key] || couchReserved[key] {
			continue
		}
		out[key] = value
	}
	return out
}

func insertDocumentRow(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	item, err := bindGridRow(rc)
	if err != nil {
		return nil, err
	}
	doc := gridFields(item)
	if id := strings.TrimSpace(stringOf(item["id"])); id != "" {
		doc["_id"] = id
		out, err := s.putDocument(rc, db, id, doc)
		if err != nil {
			return nil, err
		}
		return row{"ok": true, "id": id, "rev": stringOf(out["rev"])}, nil
	}
	var out row
	if err := s.client.do(rc.Ctx, http.MethodPost, dbPath(db), nil, doc, &out); err != nil {
		return nil, err
	}
	return row{"ok": true, "id": stringOf(out["id"]), "rev": stringOf(out["rev"])}, nil
}

// updateDocumentRow merges the edited cells into the stored document instead of
// replacing it, so fields the grid never showed survive an inline edit.
func updateDocumentRow(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	item, err := bindGridRow(rc)
	if err != nil {
		return nil, err
	}
	id := strings.TrimSpace(stringOf(item["id"]))
	if id == "" {
		return nil, fmt.Errorf("%w: the edited row has no document id", plugin.ErrInvalidInput)
	}
	current, err := s.fetchDocument(rc, db, id, nil)
	if err != nil {
		return nil, err
	}
	rev := strings.TrimSpace(stringOf(item["rev"]))
	if rev == "" {
		rev = stringOf(current["_rev"])
	}
	if rev != stringOf(current["_rev"]) {
		return nil, fmt.Errorf("%w: document update conflict on %q: the grid holds revision %q but the server is at %q. Refresh the grid before editing",
			plugin.ErrConflict, id, rev, stringOf(current["_rev"]))
	}

	updated := row{}
	for key, value := range current {
		updated[key] = value
	}
	delete(updated, "_conflicts")
	for key, value := range gridFields(item) {
		updated[key] = value
	}
	updated["_id"] = id
	updated["_rev"] = rev

	out, err := s.putDocument(rc, db, id, updated)
	if err != nil {
		return nil, err
	}
	return row{"ok": true, "id": id, "rev": stringOf(out["rev"])}, nil
}

func deleteDocumentRow(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	item, err := bindGridRow(rc)
	if err != nil {
		return nil, err
	}
	id := strings.TrimSpace(stringOf(item["id"]))
	if id == "" {
		return nil, fmt.Errorf("%w: the selected row has no document id", plugin.ErrInvalidInput)
	}
	rev, err := s.documentRev(rc, db, id, stringOf(item["rev"]))
	if err != nil {
		return nil, err
	}
	var out row
	if err := s.client.do(rc.Ctx, http.MethodDelete, docPath(db, id), url.Values{"rev": []string{rev}}, nil, &out); err != nil {
		return nil, err
	}
	return row{"ok": true, "id": id, "rev": stringOf(out["rev"])}, nil
}

// timestampFields are the conventional document fields a revision can be dated
// by. CouchDB itself stores no per-revision timestamp.
var timestampFields = []string{"updated_at", "updatedAt", "modified_at", "timestamp", "created_at", "createdAt", "date", "@timestamp"}

func revisionTime(doc row) string {
	for _, field := range timestampFields {
		if value, ok := unixTime(doc[field]); ok {
			return value.UTC().Format(time.RFC3339)
		}
	}
	return ""
}

func documentRevisions(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	id, err := docParam(rc)
	if err != nil {
		return nil, err
	}
	doc, err := s.fetchDocument(rc, db, id, url.Values{"revs_info": []string{"true"}, "conflicts": []string{"true"}})
	if err != nil {
		return nil, err
	}
	winner := stringOf(doc["_rev"])
	conflicting := map[string]bool{}
	for _, rev := range sliceOf(doc["_conflicts"]) {
		conflicting[stringOf(rev)] = true
	}

	// CouchDB keeps up to _revs_limit (1000 by default) entries; the timeline is
	// bounded by the connection's page limit because every available revision
	// costs one extra read to date it.
	infos := sliceOf(doc["_revs_info"])
	if limit := s.boundedLimit(0); len(infos) > limit {
		infos = infos[:limit]
	}
	items := make([]row, 0, len(infos))
	for _, entry := range infos {
		info := mapOf(entry)
		rev := stringOf(info["rev"])
		status := stringOf(info["status"])
		when := ""
		if status == "available" {
			if body, err := s.fetchDocument(rc, db, id, url.Values{"rev": []string{rev}}); err == nil {
				when = revisionTime(body)
			}
		}
		items = append(items, row{
			"time":       when,
			"rev":        rev,
			"generation": revGeneration(rev),
			"status":     status,
			"message":    revisionMessage(rev, winner, status, conflicting[rev]),
			"severity":   string(revisionSeverity(rev, winner, status, conflicting[rev])),
			"resource":   plugin.ResourceIdentity{Kind: "revision", Scope: db, Namespace: id, Name: rev, UID: id + "@" + rev},
		})
	}
	return plugin.Page[row]{Items: items}, nil
}

func revisionMessage(rev, winner, status string, conflicting bool) string {
	parts := []string{"generation " + strconv.Itoa(revGeneration(rev)), status}
	switch {
	case rev == winner:
		parts = append(parts, "current winning revision")
	case conflicting:
		parts = append(parts, "conflicting leaf revision")
	case status == "missing":
		parts = append(parts, "body compacted away; CouchDB keeps only the revision id")
	}
	return strings.Join(parts, " · ")
}

func revisionSeverity(rev, winner, status string, conflicting bool) plugin.Severity {
	switch {
	case rev == winner:
		return plugin.SeveritySuccess
	case conflicting:
		return plugin.SeverityDanger
	case status == "available":
		return plugin.SeverityInfo
	default:
		return plugin.SeveritySecondary
	}
}

func documentConflicts(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	id, err := docParam(rc)
	if err != nil {
		return nil, err
	}
	doc, err := s.fetchDocument(rc, db, id, url.Values{"conflicts": []string{"true"}})
	if err != nil {
		return nil, err
	}
	items := make([]row, 0)
	for _, entry := range sliceOf(doc["_conflicts"]) {
		rev := stringOf(entry)
		items = append(items, row{
			"rev": rev, "generation": revGeneration(rev), "status": "available",
		})
	}
	if winner := stringOf(doc["_rev"]); winner != "" && len(items) > 0 {
		items = append(items, row{"rev": winner, "generation": revGeneration(winner), "status": "available"})
	}
	sort.SliceStable(items, func(i, j int) bool {
		return numberOf(items[i]["generation"]) > numberOf(items[j]["generation"])
	})
	return plugin.Page[row]{Items: items}, nil
}

// listConflicts scans the database for documents with conflicting revisions.
// CouchDB has no conflict index, so the scan walks _all_docs in pages and stops
// at maxScan rather than running unbounded over a large database.
func listConflicts(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	const maxScan = 5000
	batch := s.boundedLimit(0)
	items := make([]row, 0)
	scanned := 0

	for scanned < maxScan {
		res, err := s.allDocs(rc, db, batch, scanned, "", true)
		if err != nil {
			return nil, err
		}
		if len(res.Rows) == 0 {
			break
		}
		for _, entry := range res.Rows {
			conflicts := sliceOf(entry.Doc["_conflicts"])
			if entry.ID == "" || len(conflicts) == 0 {
				continue
			}
			items = append(items, row{
				"id":            entry.ID,
				"rev":           stringOf(entry.Value["rev"]),
				"conflicts":     len(conflicts),
				"conflict_revs": conflicts,
				"ref":           documentRef(db, entry.ID),
			})
		}
		scanned += len(res.Rows)
		if len(res.Rows) < batch {
			break
		}
	}
	return broker.PageRows(rc, items)
}

// resolveDocument keeps exactly one revision of a conflicted document and
// deletes the rest in a single _bulk_docs call.
func resolveDocument(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	id, err := docParam(rc)
	if err != nil {
		return nil, err
	}
	var in struct {
		Keep string `json:"keep"`
		Rev  string `json:"rev"`
	}
	if len(rc.Body()) > 0 {
		if err := rc.Bind(&in); err != nil {
			return nil, err
		}
	}
	doc, err := s.fetchDocument(rc, db, id, url.Values{"conflicts": []string{"true"}})
	if err != nil {
		return nil, err
	}
	winner := stringOf(doc["_rev"])
	revs := []string{winner}
	for _, entry := range sliceOf(doc["_conflicts"]) {
		revs = append(revs, stringOf(entry))
	}
	if len(revs) < 2 {
		return nil, fmt.Errorf("%w: document %q has no conflicting revisions", plugin.ErrInvalidInput, id)
	}

	keep, err := chooseRevision(in.Keep, in.Rev, winner, revs)
	if err != nil {
		return nil, err
	}
	docs := make([]any, 0, len(revs)-1)
	dropped := make([]string, 0, len(revs)-1)
	for _, rev := range revs {
		if rev == keep {
			continue
		}
		docs = append(docs, row{"_id": id, "_rev": rev, "_deleted": true})
		dropped = append(dropped, rev)
	}
	var results []row
	if err := s.client.do(rc.Ctx, http.MethodPost, dbPath(db)+"/_bulk_docs", nil, row{"docs": docs}, &results); err != nil {
		return nil, err
	}
	for _, result := range results {
		if reason := stringOf(result["error"]); reason != "" {
			return nil, fmt.Errorf("%w: could not delete revision %s: %s", plugin.ErrConflict, stringOf(result["rev"]), reason)
		}
	}
	return row{"ok": true, "id": id, "kept": keep, "deleted": dropped}, nil
}

func chooseRevision(strategy, explicit, winner string, revs []string) (string, error) {
	switch strategy {
	case "", "winner":
		return winner, nil
	case "highest":
		best := revs[0]
		for _, rev := range revs[1:] {
			if revGeneration(rev) > revGeneration(best) {
				best = rev
			}
		}
		return best, nil
	case "explicit":
		for _, rev := range revs {
			if rev == explicit {
				return rev, nil
			}
		}
		return "", fmt.Errorf("%w: revision %q is not one of this document's conflicting revisions", plugin.ErrInvalidInput, explicit)
	default:
		return "", fmt.Errorf("%w: unknown conflict resolution strategy %q", plugin.ErrInvalidInput, strategy)
	}
}

// watchDocuments patches the documents grid from CouchDB's real continuous
// _changes feed rather than polling.
func watchDocuments(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, db, err := dbSession(rc)
	if err != nil {
		return err
	}
	body, err := s.client.stream(rc.Ctx, http.MethodGet, dbPath(db)+"/_changes", url.Values{
		"feed":         []string{"continuous"},
		"since":        []string{"now"},
		"heartbeat":    []string{"10000"},
		"include_docs": []string{"true"},
		"conflicts":    []string{"true"},
	}, nil)
	if err != nil {
		return err
	}
	release := closeFeedOnDisconnect(client, body)
	defer release()

	enc := json.NewEncoder(client)
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64<<10), 8<<20)
	for scanner.Scan() {
		change := decodeChange(scanner.Bytes())
		if change == nil {
			continue
		}
		id := stringOf(change["id"])
		if id == "" {
			continue
		}
		doc := mapOf(change["doc"])
		event := plugin.ResourceEvent{
			Type: "modified",
			Ref:  documentRef(db, id),
			Resource: documentRow(db, id, changeRev(change, doc), doc,
				sliceOf(doc["_conflicts"])),
		}
		if change["deleted"] == true {
			event.Type = "deleted"
		}
		if err := enc.Encode(event); err != nil {
			return nil
		}
	}
	return nil
}

func decodeChange(line []byte) row {
	if len(strings.TrimSpace(string(line))) == 0 {
		return nil
	}
	var change row
	if err := json.Unmarshal(line, &change); err != nil {
		return nil
	}
	return change
}

func changeRev(change, doc row) string {
	if rev := stringOf(doc["_rev"]); rev != "" {
		return rev
	}
	for _, entry := range sliceOf(change["changes"]) {
		if rev := stringOf(mapOf(entry)["rev"]); rev != "" {
			return rev
		}
	}
	return ""
}
