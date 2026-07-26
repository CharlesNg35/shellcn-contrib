package couchdb

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

var builtinReducers = map[string]bool{"_count": true, "_sum": true, "_stats": true, "_approx_count_distinct": true}

func designRef(db, name string) plugin.ResourceIdentity {
	return plugin.ResourceIdentity{Kind: "designdoc", Namespace: db, Name: name, UID: db + "/_design/" + name}
}

func designDocRow(db string, id string, rev string, doc row) row {
	name := strings.TrimPrefix(id, "_design/")
	views := mapOf(doc["views"])
	reduce := false
	for _, view := range views {
		if stringOf(mapOf(view)["reduce"]) != "" {
			reduce = true
			break
		}
	}
	return row{
		"name":       name,
		"language":   defaultString(stringOf(doc["language"]), "javascript"),
		"views":      len(views),
		"has_reduce": reduce,
		"rev":        rev,
		"ref":        designRef(db, name),
	}
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func listDesignDocs(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	var out allDocsResponse
	query := url.Values{"include_docs": []string{"true"}, "limit": []string{strconv.Itoa(s.boundedLimit(0))}}
	if err := s.client.do(rc.Ctx, http.MethodGet, dbPath(db)+"/_design_docs", query, nil, &out); err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(out.Rows))
	for _, entry := range out.Rows {
		if entry.ID == "" || entry.Error != "" {
			continue
		}
		rows = append(rows, designDocRow(db, entry.ID, stringOf(entry.Value["rev"]), entry.Doc))
	}
	return broker.PageRows(rc, rows)
}

func readDesignDoc(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	ddoc, err := ddocParam(rc)
	if err != nil {
		return nil, err
	}
	return s.fetchDocument(rc, db, "_design/"+ddoc, nil)
}

func createDesignDoc(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	doc, err := contentBody(rc)
	if err != nil {
		return nil, err
	}
	id := strings.TrimSpace(stringOf(doc["_id"]))
	if !strings.HasPrefix(id, "_design/") || id == "_design/" {
		return nil, fmt.Errorf("%w: a design document's _id must look like \"_design/<name>\"", plugin.ErrInvalidInput)
	}
	if err := validateDesignDoc(doc); err != nil {
		return nil, err
	}
	delete(doc, "_rev")
	out, err := s.putDocument(rc, db, id, doc)
	if err != nil {
		return nil, err
	}
	return row{"ok": true, "id": id, "rev": stringOf(out["rev"])}, nil
}

func saveDesignDoc(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	ddoc, err := ddocParam(rc)
	if err != nil {
		return nil, err
	}
	doc, err := contentBody(rc)
	if err != nil {
		return nil, err
	}
	if err := validateDesignDoc(doc); err != nil {
		return nil, err
	}
	id := "_design/" + ddoc
	doc["_id"] = id
	if stringOf(doc["_rev"]) == "" {
		current, err := s.fetchDocument(rc, db, id, nil)
		if err != nil {
			return nil, err
		}
		doc["_rev"] = current["_rev"]
	}
	out, err := s.putDocument(rc, db, id, doc)
	if err != nil {
		return nil, err
	}
	stored, err := s.fetchDocument(rc, db, id, nil)
	if err != nil {
		return nil, err
	}
	content, err := editorContent(stored)
	if err != nil {
		return nil, err
	}
	return row{"ok": true, "id": id, "rev": stringOf(out["rev"]), "content": content}, nil
}

// validateDesignDoc rejects shapes CouchDB would accept but that break silently
// at view-build time, so the failure surfaces in the editor instead of in a
// background indexer.
func validateDesignDoc(doc row) error {
	views := mapOf(doc["views"])
	if doc["views"] != nil && views == nil {
		return fmt.Errorf("%w: \"views\" must be an object of view definitions", plugin.ErrInvalidInput)
	}
	for _, name := range sortedKeys(views) {
		view := mapOf(views[name])
		if view == nil {
			return fmt.Errorf("%w: view %q must be an object with a \"map\" function", plugin.ErrInvalidInput, name)
		}
		if strings.TrimSpace(stringOf(view["map"])) == "" {
			return fmt.Errorf("%w: view %q has no \"map\" function", plugin.ErrInvalidInput, name)
		}
		if reduce := strings.TrimSpace(stringOf(view["reduce"])); strings.HasPrefix(reduce, "_") && !builtinReducers[reduce] {
			return fmt.Errorf("%w: view %q uses unknown built-in reducer %q", plugin.ErrInvalidInput, name, reduce)
		}
	}
	return nil
}

func deleteDesignDoc(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	ddoc, err := ddocParam(rc)
	if err != nil {
		return nil, err
	}
	id := "_design/" + ddoc
	rev, err := s.documentRev(rc, db, id, "")
	if err != nil {
		return nil, err
	}
	var out row
	if err := s.client.do(rc.Ctx, http.MethodDelete, docPath(db, id), url.Values{"rev": []string{rev}}, nil, &out); err != nil {
		return nil, err
	}
	return row{"ok": true, "name": ddoc}, nil
}

// --- views ---------------------------------------------------------------------

func (s *Session) viewDefinition(rc *plugin.RequestContext, db, ddoc, view string) (row, error) {
	doc, err := s.fetchDocument(rc, db, "_design/"+ddoc, nil)
	if err != nil {
		return nil, err
	}
	definition := mapOf(mapOf(doc["views"])[view])
	if definition == nil {
		return nil, fmt.Errorf("%w: view %q is not declared in _design/%s", plugin.ErrNotFound, view, ddoc)
	}
	return definition, nil
}

func viewRow(db, ddoc, name string, definition row) row {
	reduce := strings.TrimSpace(stringOf(definition["reduce"]))
	kind := "none"
	switch {
	case builtinReducers[reduce]:
		kind = "builtin"
	case reduce != "":
		kind = "custom"
	}
	return row{
		"name": name, "reduce": reduce != "", "reduce_kind": kind,
		"ref": plugin.ResourceIdentity{Kind: "view", Scope: db, Namespace: ddoc, Name: name, UID: db + "/" + ddoc + "/" + name},
	}
}

func listViews(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	ddoc, err := ddocParam(rc)
	if err != nil {
		return nil, err
	}
	doc, err := s.fetchDocument(rc, db, "_design/"+ddoc, nil)
	if err != nil {
		return nil, err
	}
	views := mapOf(doc["views"])
	rows := make([]row, 0, len(views))
	for _, name := range sortedKeys(views) {
		rows = append(rows, viewRow(db, ddoc, name, mapOf(views[name])))
	}
	return broker.PageRows(rc, rows)
}

type viewResponse struct {
	TotalRows int `json:"total_rows"`
	Offset    int `json:"offset"`
	Rows      []struct {
		ID    string `json:"id"`
		Key   any    `json:"key"`
		Value any    `json:"value"`
	} `json:"rows"`
}

func (s *Session) queryView(rc *plugin.RequestContext, db, ddoc, view string, query url.Values) (viewResponse, error) {
	var out viewResponse
	path := dbPath(db) + "/_design/" + url.PathEscape(ddoc) + "/_view/" + url.PathEscape(view)
	err := s.client.do(rc.Ctx, http.MethodGet, path, query, nil, &out)
	return out, err
}

func viewTarget(rc *plugin.RequestContext) (*Session, string, string, string, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, "", "", "", err
	}
	ddoc, err := ddocParam(rc)
	if err != nil {
		return nil, "", "", "", err
	}
	view, err := viewParam(rc)
	if err != nil {
		return nil, "", "", "", err
	}
	return s, db, ddoc, view, nil
}

func viewRows(rc *plugin.RequestContext) (any, error) {
	s, db, ddoc, view, err := viewTarget(rc)
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
	query := url.Values{
		"limit":  []string{strconv.Itoa(limit)},
		"skip":   []string{strconv.Itoa(skip)},
		"reduce": []string{"false"},
	}
	if page.Sort != nil && page.Sort[0].Desc {
		query.Set("descending", "true")
	}
	res, err := s.queryView(rc, db, ddoc, view, query)
	if err != nil {
		return nil, err
	}
	items := make([]row, 0, len(res.Rows))
	for _, entry := range res.Rows {
		items = append(items, row{"key": entry.Key, "value": entry.Value, "id": entry.ID})
	}
	out := plugin.Page[row]{Items: plugin.FilterRows(items, page.Search())}
	if res.TotalRows > 0 {
		total := res.TotalRows
		out.Total = &total
	}
	if len(items) == limit {
		out.NextCursor = strconv.Itoa(skip + limit)
	}
	return out, nil
}

// viewReduce runs the grouped reduction. group_level arrives as a normal list
// param so the grid can drill from a single total into per-key groups.
func viewReduce(rc *plugin.RequestContext) (any, error) {
	s, db, ddoc, view, err := viewTarget(rc)
	if err != nil {
		return nil, err
	}
	definition, err := s.viewDefinition(rc, db, ddoc, view)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(stringOf(definition["reduce"])) == "" {
		return plugin.Page[row]{Items: []row{}}, nil
	}
	query := url.Values{"reduce": []string{"true"}, "group": []string{"true"}}
	if level := strings.TrimSpace(rc.Param("group_level")); level != "" {
		if n, err := strconv.Atoi(level); err != nil || n < 0 {
			return nil, fmt.Errorf("%w: group_level must be a non-negative integer", plugin.ErrInvalidInput)
		} else if n > 0 {
			query.Set("group_level", strconv.Itoa(n))
		}
	}
	res, err := s.queryView(rc, db, ddoc, view, query)
	if err != nil {
		return nil, err
	}
	items := make([]row, 0, len(res.Rows))
	for _, entry := range res.Rows {
		items = append(items, row{"key": entry.Key, "value": entry.Value})
	}
	return broker.PageRows(rc, items)
}

func viewDefinition(rc *plugin.RequestContext) (any, error) {
	s, db, ddoc, view, err := viewTarget(rc)
	if err != nil {
		return nil, err
	}
	definition, err := s.viewDefinition(rc, db, ddoc, view)
	if err != nil {
		return nil, err
	}
	doc, err := s.fetchDocument(rc, db, "_design/"+ddoc, nil)
	if err != nil {
		return nil, err
	}
	language := defaultString(stringOf(doc["language"]), "javascript")

	var b strings.Builder
	fmt.Fprintf(&b, "## %s/%s\n\nLanguage: `%s`\n\n### Map\n\n```%s\n%s\n```\n",
		ddoc, view, language, language, stringOf(definition["map"]))
	if reduce := stringOf(definition["reduce"]); reduce != "" {
		kind := "custom function"
		if builtinReducers[reduce] {
			kind = "built-in reducer"
		}
		fmt.Fprintf(&b, "\n### Reduce (%s)\n\n```%s\n%s\n```\n", kind, language, reduce)
	} else {
		b.WriteString("\n### Reduce\n\nThis view has no reduce function.\n")
	}
	return row{"content": b.String(), "format": "markdown"}, nil
}
