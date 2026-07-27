package mqtt

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

// emqxScanPageSize is the page used when a request needs the whole dataset
// (search or sort) rather than one server page.
const emqxScanPageSize = 200

type emqxClient struct {
	http   *http.Client
	base   string
	key    string
	secret string
}

// emqxList decodes both EMQX list shapes: the paginated {data, meta} envelope
// used by /clients, /subscriptions, and /topics, and the bare array returned by
// /nodes and /stats.
type emqxList struct {
	Data []row    `json:"data"`
	Meta emqxMeta `json:"meta"`

	Paginated bool `json:"-"`
}

type emqxMeta struct {
	Page    int  `json:"page"`
	Limit   int  `json:"limit"`
	Count   int  `json:"count"`
	HasNext bool `json:"hasnext"`
}

func (l *emqxList) UnmarshalJSON(data []byte) error {
	if trimmed := bytes.TrimLeft(data, " \t\r\n"); len(trimmed) > 0 && trimmed[0] == '[' {
		var items []row
		if err := json.Unmarshal(data, &items); err != nil {
			return err
		}
		*l = emqxList{Data: items}
		return nil
	}
	type envelope emqxList
	var out envelope
	if err := json.Unmarshal(data, &out); err != nil {
		return err
	}
	*l = emqxList(out)
	l.Paginated = true
	return nil
}

func (c *emqxClient) do(ctx context.Context, method, apiPath string, query url.Values, out any) error {
	target := c.base + "/api/v5" + apiPath
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, target, nil)
	if err != nil {
		return fmt.Errorf("%w: %v", plugin.ErrInvalidInput, err)
	}
	req.Header.Set("Accept", "application/json")
	if c.key != "" || c.secret != "" {
		req.SetBasicAuth(c.key, c.secret)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%w: EMQX API: %v", plugin.ErrUnavailable, err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return fmt.Errorf("%w: EMQX API: %v", plugin.ErrUnavailable, err)
	}
	if resp.StatusCode >= 400 {
		return emqxHTTPError(resp.StatusCode, data)
	}
	if out == nil || len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("%w: EMQX API returned an unexpected body: %v", plugin.ErrUnavailable, err)
	}
	return nil
}

func emqxHTTPError(status int, data []byte) error {
	var body struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	_ = json.Unmarshal(data, &body)
	message := strings.TrimSpace(body.Message)
	if message == "" {
		message = strings.TrimSpace(string(data))
	}
	if message == "" {
		message = http.StatusText(status)
	}
	switch status {
	case http.StatusNotFound:
		return fmt.Errorf("%w: %s", plugin.ErrNotFound, message)
	case http.StatusConflict:
		return fmt.Errorf("%w: %s", plugin.ErrConflict, message)
	case http.StatusUnauthorized:
		return fmt.Errorf("%w: %s", plugin.ErrUnauthorized, message)
	case http.StatusForbidden:
		return fmt.Errorf("%w: %s", plugin.ErrForbidden, message)
	case http.StatusBadRequest:
		return fmt.Errorf("%w: %s", plugin.ErrInvalidInput, message)
	default:
		return fmt.Errorf("%w: EMQX API returned %d: %s", plugin.ErrUnavailable, status, message)
	}
}

func emqxSession(rc *plugin.RequestContext) (*Session, *emqxClient, error) {
	s, err := mqttSession(rc)
	if err != nil {
		return nil, nil, err
	}
	if s.emqx == nil {
		return s, nil, nil
	}
	return s, s.emqx, nil
}

// emqxPage returns one page of an EMQX list. EMQX pages server-side but cannot
// sort, so a request carrying a sort or the grid's search box has to see the
// dataset before it is sliced; that path walks a bounded number of pages, the
// plain path pushes the window to EMQX. Both speak the same absolute-row-offset
// cursor broker.PageRows emits, so switching between them mid-browse still lands
// on the right rows.
func emqxPage(rc *plugin.RequestContext, client *emqxClient, apiPath string, query url.Values, decorate func(row) row) (scanPage, error) {
	req, err := rc.Page()
	if err != nil {
		return scanPage{}, err
	}
	if client == nil {
		return scanPage{Page: plugin.Page[row]{Items: []row{}}, Unavailable: true}, nil
	}
	offset, err := cursorOffset(req.Cursor)
	if err != nil {
		return scanPage{}, err
	}
	size := req.Limit
	number := offset/size + 1
	skip := offset - (number-1)*size
	if req.Search() != "" || len(req.Sort) > 0 || skip > 0 {
		return emqxScan(rc, client, apiPath, query, decorate)
	}

	page := url.Values{}
	maps.Copy(page, query)
	page.Set("page", strconv.Itoa(number))
	page.Set("limit", strconv.Itoa(size))
	var out emqxList
	if err := client.do(rc.Ctx, http.MethodGet, apiPath, page, &out); err != nil {
		return scanPage{}, err
	}
	rows := decorateRows(out.Data, decorate)
	if !out.Paginated {
		// The endpoint ignored the window (bare array): slice what it sent.
		local, err := broker.PageRows(rc, rows)
		return scanPage{Page: local}, err
	}
	result := scanPage{Page: plugin.Page[row]{Items: rows}}
	if out.Meta.HasNext {
		result.Page.NextCursor = strconv.Itoa(number * size)
	}
	if total, ok := emqxTotal(out.Meta, offset, len(rows)); ok {
		result.Page.Total = &total
	}
	return result, nil
}

// emqxTotal reports meta.count only when it is a real dataset size. EMQX sets
// count to -1 for multi-condition queries and leaves it at 0 when the count is
// unavailable, so a 0 alongside rows or a further page is not a total — only the
// last page of such a walk knows how far the dataset reaches.
func emqxTotal(meta emqxMeta, offset, rows int) (int, bool) {
	switch {
	case meta.Count > 0:
		return meta.Count, true
	case meta.Count < 0 || meta.HasNext:
		return 0, false
	case rows > 0 || offset == 0:
		return offset + rows, true
	}
	return 0, false
}

func emqxScan(rc *plugin.RequestContext, client *emqxClient, apiPath string, query url.Values, decorate func(row) row) (scanPage, error) {
	budget := scanBudget(rc)
	rows := []row{}
	truncated := false
	for number := 1; ; number++ {
		page := url.Values{}
		maps.Copy(page, query)
		page.Set("page", strconv.Itoa(number))
		page.Set("limit", strconv.Itoa(emqxScanPageSize))
		var out emqxList
		if err := client.do(rc.Ctx, http.MethodGet, apiPath, page, &out); err != nil {
			return scanPage{}, err
		}
		rows = append(rows, decorateRows(out.Data, decorate)...)
		if !out.Paginated || !out.Meta.HasNext || len(out.Data) == 0 {
			break
		}
		if len(rows) >= budget {
			truncated = true
			break
		}
	}
	return pageScan(rc, rows, truncated, budget)
}

func decorateRows(rows []row, decorate func(row) row) []row {
	out := make([]row, 0, len(rows))
	for _, item := range rows {
		if decorate != nil {
			item = decorate(item)
		}
		out = append(out, item)
	}
	return out
}
