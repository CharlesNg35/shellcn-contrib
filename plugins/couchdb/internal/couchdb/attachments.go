package couchdb

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

func attachmentParam(rc *plugin.RequestContext) (string, error) {
	name := strings.TrimSpace(rc.Param("name"))
	if name == "" || strings.ContainsAny(name, "\x00\n\r") {
		return "", fmt.Errorf("%w: an attachment name is required", plugin.ErrInvalidInput)
	}
	return name, nil
}

// listAttachments reports the attachment stubs CouchDB keeps on a document.
// Bodies are never fetched: an attachment can be arbitrarily large and only its
// type, size and originating revision are useful in a grid.
func listAttachments(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	id, err := docParam(rc)
	if err != nil {
		return nil, err
	}
	doc, err := s.fetchDocument(rc, db, id, nil)
	if err != nil {
		return nil, err
	}
	attachments := mapOf(doc["_attachments"])
	rows := make([]row, 0, len(attachments))
	for _, name := range sortedKeys(attachments) {
		meta := mapOf(attachments[name])
		rows = append(rows, row{
			"name":         name,
			"content_type": stringOf(meta["content_type"]),
			"length":       numberOf(meta["length"]),
			"revpos":       numberOf(meta["revpos"]),
			"digest":       stringOf(meta["digest"]),
		})
	}
	return broker.PageRows(rc, rows)
}

// deleteAttachment removes one attachment and leaves the rest of the document
// untouched, which is what CouchDB's per-attachment endpoint does natively.
func deleteAttachment(rc *plugin.RequestContext) (any, error) {
	s, db, err := dbSession(rc)
	if err != nil {
		return nil, err
	}
	id, err := docParam(rc)
	if err != nil {
		return nil, err
	}
	name, err := attachmentParam(rc)
	if err != nil {
		return nil, err
	}
	rev, err := s.documentRev(rc, db, id, "")
	if err != nil {
		return nil, err
	}
	var out row
	path := docPath(db, id) + "/" + url.PathEscape(name)
	if err := s.client.do(rc.Ctx, http.MethodDelete, path, url.Values{"rev": []string{rev}}, nil, &out); err != nil {
		return nil, err
	}
	return row{"ok": true, "id": id, "name": name, "rev": stringOf(out["rev"])}, nil
}
