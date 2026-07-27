package nomad

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/hashicorp/nomad/api"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

type row = plugin.TableRow

// scanChunk is how much of a bounded walk one upstream round trip asks for.
const scanChunk = 200

// listPage embeds the unchanged paged wire contract and adds the walk-cap
// signal: truncated and scanLimit tell the browser a sorted or searched listing
// stopped at the cap rather than at the end of the cluster's data.
type listPage struct {
	plugin.Page[row]
	Truncated bool `json:"truncated,omitempty"`
	ScanLimit int  `json:"scanLimit,omitempty"`
}

// cursor is the opaque wire cursor. Token resumes Nomad's own next_token paging
// when the grid asks for raw cluster order; Offset addresses a row inside the
// bounded walk that a sort or a search term forces.
type cursor struct {
	Token  string `json:"t,omitempty"`
	Offset int    `json:"o,omitempty"`
}

func encodeCursor(c cursor) string {
	if c.Token == "" {
		return ""
	}
	data, err := json.Marshal(c)
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(data)
}

// decodeCursor accepts both cursor forms this plugin emits and the grid's bare
// numeric-offset fallback, so a restored or hand-built cursor still lands on the
// rows it names.
func decodeCursor(raw string) (cursor, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return cursor{}, nil
	}
	if offset, err := strconv.Atoi(raw); err == nil {
		if offset < 0 {
			return cursor{}, fmt.Errorf("%w: invalid cursor", plugin.ErrInvalidInput)
		}
		return cursor{Offset: offset}, nil
	}
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return cursor{}, fmt.Errorf("%w: invalid cursor", plugin.ErrInvalidInput)
	}
	var out cursor
	if err := json.Unmarshal(data, &out); err != nil || out.Offset < 0 {
		return cursor{}, fmt.Errorf("%w: invalid cursor", plugin.ErrInvalidInput)
	}
	return out, nil
}

// tokenFetch reads one server-side page of a Nomad list endpoint and returns the
// continuation token the cluster handed back.
type tokenFetch func(q *api.QueryOptions) ([]row, string, error)

// walkAll drains a Nomad list endpoint through its own continuation token and
// stops at the connection's scan limit, so a picker or a cluster-wide count
// reads the whole set on a normal cluster without ever becoming unbounded. The
// bool reports whether the walk reached the end of the data.
func walkAll[T any](s *Session, base *api.QueryOptions, fetch func(*api.QueryOptions) ([]T, *api.QueryMeta, error)) ([]T, bool, error) {
	var out []T
	token := ""
	for {
		q := *base
		q.PerPage = scanChunk
		q.NextToken = token
		page, meta, err := fetch(&q)
		if err != nil {
			return nil, false, nomadErr(err)
		}
		out = append(out, page...)
		token = nextToken(meta)
		if token == "" || len(page) == 0 {
			return out, true, nil
		}
		if len(out) >= s.opts.ScanLimit {
			return out, false, nil
		}
	}
}

// pageTokens serves a Nomad list endpoint that supports per_page/next_token.
// Raw cluster order pages straight through the upstream cursor. A sort or a
// search term cannot be pushed to Nomad, so those walk a bounded prefix of the
// dataset, order and filter the whole prefix, and only then cut the page.
func (s *Session) pageTokens(rc *plugin.RequestContext, base *api.QueryOptions, fetch tokenFetch) (listPage, error) {
	req, err := rc.Page()
	if err != nil {
		return listPage{}, err
	}
	cur, err := decodeCursor(req.Cursor)
	if err != nil {
		return listPage{}, err
	}
	if req.Search() == "" && len(req.Sort) == 0 && (cur.Token != "" || cur.Offset == 0) {
		q := *base
		q.PerPage = int32(req.Limit)
		q.NextToken = cur.Token
		rows, next, err := fetch(&q)
		if err != nil {
			return listPage{}, nomadErr(err)
		}
		return listPage{Page: plugin.Page[row]{Items: nonNilRows(rows), NextCursor: encodeCursor(cursor{Token: next})}}, nil
	}

	// The budget never stops short of the page being asked for, so a deep page
	// keeps walking instead of stopping at the cap with rows nobody can reach.
	budget := max(s.opts.ScanLimit, cur.Offset+req.Limit+1)
	rows := make([]row, 0, min(budget, scanChunk))
	truncated := false
	token := ""
	for {
		q := *base
		q.PerPage = int32(min(scanChunk, budget-len(rows)))
		q.NextToken = token
		page, next, err := fetch(&q)
		if err != nil {
			return listPage{}, nomadErr(err)
		}
		rows = append(rows, page...)
		token = next
		if token == "" || len(page) == 0 {
			break
		}
		if len(rows) >= budget {
			truncated = true
			break
		}
	}
	return sliceRows(rows, req, cur.Offset, truncated, budget), nil
}

// sliceRows orders and filters the whole walked set before cutting the page, so
// the grid's sort and search cover every row that was read rather than the one
// page being rendered.
func sliceRows(rows []row, req plugin.PageRequest, offset int, truncated bool, budget int) listPage {
	rows = plugin.SortRows(plugin.FilterRows(rows, req.Search()), req.Sort)
	start := min(offset, len(rows))
	end := min(start+req.Limit, len(rows))
	out := listPage{Page: plugin.Page[row]{Items: nonNilRows(rows[start:end])}}
	if end < len(rows) {
		out.NextCursor = strconv.Itoa(end)
	}
	if truncated {
		out.Truncated, out.ScanLimit = true, budget
		return out
	}
	total := len(rows)
	out.Total = &total
	return out
}

// staticPage serves an endpoint Nomad returns whole: the response is already the
// complete scoped set, so the shared grid helpers filter, sort, and slice it.
func staticPage(rc *plugin.RequestContext, rows []row) (listPage, error) {
	page, err := broker.PageRows(rc, rows)
	if err != nil {
		return listPage{}, err
	}
	page.Items = nonNilRows(page.Items)
	return listPage{Page: page}, nil
}

// markTruncated tells the grid a listing was assembled from a capped walk, so it
// never renders a total or a "that is everything" state it cannot vouch for.
func markTruncated(page listPage, whole bool, scanLimit int) listPage {
	if whole {
		return page
	}
	page.Truncated, page.ScanLimit, page.Total = true, scanLimit, nil
	return page
}

func treePage(page listPage, nodes []plugin.TreeNode) plugin.Page[plugin.TreeNode] {
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}
}

func nonNilRows(rows []row) []row {
	if rows == nil {
		return []row{}
	}
	return rows
}
