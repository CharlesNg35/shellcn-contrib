package pinecone

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

// scanLimit bounds every in-memory scan. A sort or a metadata-wide filter has to
// span the dataset before paging, so it reads at most this many rows.
const scanLimit = plugin.MaxPageLimit

// serverPageSize is the page a token-paginated Pinecone endpoint is asked for
// while a scan walks it; it is independent of the page the grid asked for.
const serverPageSize = 100

// maxScanPages backstops the scan against a server that keeps handing back a
// continuation token for empty pages.
const maxScanPages = 64

// pagedResult is a page plus the scan-cap signal. The embedded page keeps the
// wire contract (items, nextCursor, total) unchanged; truncated and scanLimit
// tell the browser the listing stopped at the cap.
type pagedResult struct {
	plugin.Page[row]
	Truncated bool `json:"truncated,omitempty"`
	ScanLimit int  `json:"scanLimit,omitempty"`
}

func capped(page plugin.Page[row], truncated bool) pagedResult {
	out := pagedResult{Page: page}
	if truncated {
		// The total only covers the prefix that was scanned, so the grid pages by
		// cursor instead of asserting a count it knows is short.
		out.Page.Total = nil
		out.Truncated = true
		out.ScanLimit = scanLimit
	}
	return out
}

// cursor addresses a position inside a token-paginated listing: the server token
// that produced a page plus how many of that page's rows are already consumed.
// It is opaque on the wire so the grid never depends on its shape.
type cursor struct {
	Token string `json:"t,omitempty"`
	Skip  int    `json:"s,omitempty"`
}

func encodeCursor(c cursor) string {
	if c.Token == "" && c.Skip <= 0 {
		return ""
	}
	data, err := json.Marshal(c)
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(data)
}

// decodeCursor accepts an opaque cursor and, because the grid falls back to a
// numeric offset whenever it has no cursor of ours, a bare row offset too.
func decodeCursor(raw string) (cursor, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return cursor{}, nil
	}
	if offset, err := strconv.Atoi(raw); err == nil {
		if offset < 0 {
			return cursor{}, fmt.Errorf("%w: invalid cursor", plugin.ErrInvalidInput)
		}
		return cursor{Skip: offset}, nil
	}
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return cursor{}, fmt.Errorf("%w: invalid cursor", plugin.ErrInvalidInput)
	}
	var out cursor
	if err := json.Unmarshal(data, &out); err != nil || out.Skip < 0 {
		return cursor{}, fmt.Errorf("%w: invalid cursor", plugin.ErrInvalidInput)
	}
	return out, nil
}

// fetchPage reads one server page starting at token and returns its rows plus
// the token for the following page ("" when the listing is exhausted).
type fetchPage func(ctx context.Context, token string, limit int) ([]row, string, error)

// keepRow decides whether a row belongs in the result. It runs before paging, so
// a filter always spans the whole listing rather than the current page.
type keepRow func(row) bool

// streamPage walks server pages until it has limit matching rows. The cursor it
// returns resumes mid-page, so filtering never hides rows behind the page
// boundary and the caller always gets a real continuation.
func streamPage(ctx context.Context, req plugin.PageRequest, fetch fetchPage, keep keepRow) (plugin.Page[row], bool, error) {
	start, err := decodeCursor(req.Cursor)
	if err != nil {
		return plugin.Page[row]{}, false, err
	}
	limit := req.Limit
	if limit <= 0 {
		limit = plugin.DefaultPageLimit
	}
	out := make([]row, 0, limit)
	token, skip, scanned := start.Token, start.Skip, 0
	for pages := 0; pages < maxScanPages; pages++ {
		rows, next, err := fetch(ctx, token, serverPageSize)
		if err != nil {
			return plugin.Page[row]{}, false, err
		}
		if skip >= len(rows) {
			// A numeric offset can address a row beyond this server page; keep
			// walking and charge the skipped rows to the scan budget.
			skip -= len(rows)
			scanned += len(rows)
		} else {
			for i := skip; i < len(rows); i++ {
				scanned++
				if keep != nil && !keep(rows[i]) {
					continue
				}
				if len(out) == limit {
					// One more match exists on this page: point the next cursor at it
					// rather than at the next server page, so nothing is skipped.
					return plugin.Page[row]{Items: out, NextCursor: encodeCursor(cursor{Token: token, Skip: i})}, false, nil
				}
				out = append(out, rows[i])
			}
			skip = 0
		}
		if next == "" {
			return plugin.Page[row]{Items: out, NextCursor: ""}, false, nil
		}
		if scanned >= scanLimit {
			// The scan spent its budget; hand back a real continuation so the rest
			// of the listing stays reachable instead of dropping off the end.
			return plugin.Page[row]{Items: out, NextCursor: encodeCursor(cursor{Token: next, Skip: skip})}, true, nil
		}
		token = next
	}
	return plugin.Page[row]{Items: out, NextCursor: encodeCursor(cursor{Token: token, Skip: skip})}, true, nil
}

// scanRows reads at most scanLimit rows so an in-memory sort can span the
// listing. It reports whether the cap cut the scan short.
func scanRows(ctx context.Context, fetch fetchPage, keep keepRow) ([]row, bool, error) {
	out := make([]row, 0, scanLimit)
	token, scanned := "", 0
	for pages := 0; pages < maxScanPages; pages++ {
		rows, next, err := fetch(ctx, token, serverPageSize)
		if err != nil {
			return nil, false, err
		}
		for _, item := range rows {
			if scanned >= scanLimit {
				return out, true, nil
			}
			scanned++
			if keep == nil || keep(item) {
				out = append(out, item)
			}
		}
		if next == "" {
			return out, false, nil
		}
		if scanned >= scanLimit {
			return out, true, nil
		}
		token = next
	}
	return out, true, nil
}

// pageRows sorts a fully materialized set and slices the requested page. Rows
// must already be filtered; the caller owns the slice, so sorting is safe.
func pageRows(req plugin.PageRequest, rows []row, truncated bool) (plugin.Page[row], error) {
	rows = plugin.SortRows(rows, req.Sort)
	limit := req.Limit
	if limit <= 0 {
		limit = plugin.DefaultPageLimit
	}
	start, err := decodeCursor(req.Cursor)
	if err != nil {
		return plugin.Page[row]{}, err
	}
	begin := min(start.Skip, len(rows))
	end := min(begin+limit, len(rows))
	next := ""
	if end < len(rows) {
		next = encodeCursor(cursor{Skip: end})
	}
	page := plugin.Page[row]{Items: rows[begin:end], NextCursor: next}
	if !truncated {
		total := len(rows)
		page.Total = &total
	}
	return page, nil
}

// searchFilter builds the row filter for a server-paged listing: the grid's
// search term is matched against one column, because that is the only value the
// listing endpoint returns before rows are hydrated.
func searchFilter(req plugin.PageRequest, field string) keepRow {
	needle := strings.ToLower(req.Search())
	return func(item row) bool {
		if needle == "" {
			return true
		}
		return strings.Contains(strings.ToLower(fmt.Sprint(item[field])), needle)
	}
}
