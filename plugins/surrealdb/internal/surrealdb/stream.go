package surrealdb

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
	surrealdb "github.com/surrealdb/surrealdb.go"
)

// --- interactive REPL terminal ----------------------------------------------

// replStream is the WS StreamHandler for the SurrealQL REPL. It opens the
// session's terminal channel and pumps bytes both ways, exactly like a shell
// plugin — so the gateway records the stream and tears it down on disconnect.
func replStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	ch, err := rc.Session.OpenChannel(rc.Ctx, plugin.ChannelRequest{
		Kind:   plugin.StreamTerminal,
		Params: rc.Params(),
	})
	if err != nil {
		return err
	}
	defer ch.Close()

	errc := make(chan error, 2)
	go func() { _, e := io.Copy(client, ch); errc <- e }()       // upstream → browser
	go func() { errc <- plugin.CopyTerminalInput(ch, client) }() // browser → upstream, control frames handled
	select {
	case <-client.Context().Done():
		return nil
	case err := <-errc:
		if err == io.EOF {
			return nil
		}
		return err
	}
}

// repl is a pseudo-terminal Channel: bytes written by the browser are buffered
// into lines, each executed as SurrealQL, and the formatted result is read back.
// It satisfies plugin.Channel (io.ReadWriteCloser + Kind).
type repl struct {
	db   *surrealdb.DB
	opts options

	pr *io.PipeReader // results to browser
	pw *io.PipeWriter

	mu   sync.Mutex
	line []byte
	cols int
}

// Resize records the terminal width; the REPL has no PTY, so the size only
// informs output formatting.
func (r *repl) Resize(cols, _ int) error {
	r.mu.Lock()
	r.cols = cols
	r.mu.Unlock()
	return nil
}

// newREPL deliberately ignores the OpenChannel RPC context: that context ends
// when the RPC returns, while the channel lives until Close. Each statement gets
// its own bounded context in exec.
func newREPL(db *surrealdb.DB, opts options) *repl {
	pr, pw := io.Pipe()
	r := &repl{db: db, opts: opts, pr: pr, pw: pw}
	go func() {
		fmt.Fprintf(pw, "SurrealDB REPL — %s/%s. End a statement with ';' and Enter.\r\n", opts.namespace, opts.database)
		fmt.Fprint(pw, "surreal> ")
	}()
	return r
}

func (r *repl) Kind() plugin.StreamKind { return plugin.StreamTerminal }

func (r *repl) Read(p []byte) (int, error) { return r.pr.Read(p) }

func (r *repl) Close() error {
	_ = r.pw.Close()
	return r.pr.Close()
}

// Write receives keystrokes from the browser, echoes them, and on Enter executes
// the accumulated statement.
func (r *repl) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, b := range p {
		switch b {
		case '\r', '\n':
			fmt.Fprint(r.pw, "\r\n")
			stmt := strings.TrimSpace(string(r.line))
			r.line = r.line[:0]
			if stmt != "" {
				r.exec(stmt)
			}
			fmt.Fprint(r.pw, "surreal> ")
		case 0x7f, 0x08: // DEL / backspace
			if n := len(r.line); n > 0 {
				r.line = r.line[:n-1]
				fmt.Fprint(r.pw, "\b \b")
			}
		default:
			r.line = append(r.line, b)
			r.pw.Write([]byte{b}) // echo
		}
	}
	return len(p), nil
}

// exec runs one statement and writes a compact JSON result to the terminal.
func (r *repl) exec(stmt string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	res, err := surrealdb.Query[any](ctx, r.db, stmt, nil)
	if err != nil {
		fmt.Fprintf(r.pw, "error: %v\r\n", err)
		return
	}
	for _, qr := range deref(res) {
		out, _ := json.Marshal(normalize(qr.Result))
		fmt.Fprintf(r.pw, "[%s] %s\r\n", qr.Status, out)
	}
}

// --- query editor (WS) ------------------------------------------------------

// queryRequest is the JSON the query-editor panel sends per execution.
type queryRequest struct {
	Query     string `json:"query"`
	RequestID string `json:"requestId,omitempty"`
}

// queryResult is the grid the query-editor panel renders. SurrealDB returns
// objects, so columns are the union of keys across the last statement's rows.
type queryResult struct {
	Columns   []string `json:"columns"`
	Rows      [][]any  `json:"rows"`
	RowCount  int64    `json:"rowCount,omitempty"`
	ElapsedMS int64    `json:"elapsedMs"`
	Statement string   `json:"statement,omitempty"`
}

// queryStream is the WS handler behind the SurrealQL query editor: it reads a
// query, executes it, and writes back a column/row grid. Each execution is
// audited through the core hook, matching a built-in's stream-internal audit.
func queryStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s := rc.Session.(*session)
	db, err := s.client(rc.Ctx)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(client)
	enc := json.NewEncoder(client)
	for {
		var req queryRequest
		if err := dec.Decode(&req); err != nil {
			if client.Context().Err() != nil || err == io.EOF {
				return nil
			}
			_ = enc.Encode(map[string]any{"error": "invalid query request"})
			continue
		}
		if strings.TrimSpace(req.Query) == "" {
			_ = enc.Encode(map[string]any{"error": "query is empty"})
			continue
		}

		start := time.Now()
		res, qerr := surrealdb.Query[any](rc.Ctx, db, req.Query, nil)
		elapsed := time.Since(start).Milliseconds()
		rc.Audit(auditResult(qerr), map[string]string{"query": req.Query}, qerr)
		if qerr != nil {
			if err := enc.Encode(map[string]any{"error": qerr.Error()}); err != nil {
				return err
			}
			continue
		}
		out := resultsToGrid(res)
		out.ElapsedMS = elapsed
		if err := enc.Encode(out); err != nil {
			return err
		}
	}
}

func auditResult(err error) plugin.AuditResult {
	if err != nil {
		return plugin.AuditError
	}
	return plugin.AuditAllowed
}

// resultsToGrid converts the last query statement into a column/row grid. A
// statement error is reported as a single "error" column.
func resultsToGrid(res *[]surrealdb.QueryResult[any]) queryResult {
	stmts := deref(res)
	if len(stmts) == 0 {
		return queryResult{Columns: []string{}, Rows: [][]any{}}
	}
	last := stmts[len(stmts)-1]
	if strings.EqualFold(last.Status, "ERR") {
		return queryResult{Columns: []string{"error"}, Rows: [][]any{{normalize(last.Result)}}}
	}

	var objects []map[string]any
	switch v := normalize(last.Result).(type) {
	case []any:
		for _, e := range v {
			if m, ok := e.(map[string]any); ok {
				objects = append(objects, m)
			} else {
				objects = append(objects, map[string]any{"value": e})
			}
		}
	case map[string]any:
		objects = []map[string]any{v}
	case nil:
		return queryResult{Columns: []string{}, Rows: [][]any{}, RowCount: 0}
	default:
		return queryResult{Columns: []string{"value"}, Rows: [][]any{{v}}, RowCount: 1}
	}

	cols := unionKeys(objects)
	rows := make([][]any, 0, len(objects))
	for _, o := range objects {
		row := make([]any, len(cols))
		for i, c := range cols {
			row[i] = o[c]
		}
		rows = append(rows, row)
	}
	return queryResult{Columns: cols, Rows: rows, RowCount: int64(len(rows)), Statement: last.Status}
}

// unionKeys returns a stable column order: "id" first, then the remaining keys
// sorted, across all rows (SurrealDB records are schemaless).
func unionKeys(objects []map[string]any) []string {
	set := map[string]bool{}
	for _, o := range objects {
		for k := range o {
			set[k] = true
		}
	}
	rest := make([]string, 0, len(set))
	for k := range set {
		if k != "id" {
			rest = append(rest, k)
		}
	}
	sort.Strings(rest)
	cols := make([]string, 0, len(set))
	if set["id"] {
		cols = append(cols, "id")
	}
	return append(cols, rest...)
}

// --- server-streamed change tail --------------------------------------------

// changesStream is a server-stream (logs) panel. SurrealDB's native live queries
// require a WebSocket connection, which the driver cannot route through the
// gateway transport; instead this polls the selected table and emits newly seen
// records as JSON log events. It demonstrates the server-stream capability and
// the live-list pattern honestly, without a native change feed.
func tailStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	table := rc.Param("table")
	enc := json.NewEncoder(client)
	if table == "" {
		_ = enc.Encode(map[string]any{"message": "select a table to tail"})
		<-client.Context().Done()
		return nil
	}

	s := rc.Session.(*session)
	db, err := s.client(rc.Ctx)
	if err != nil {
		return err
	}

	seen := map[string]bool{}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	_ = enc.Encode(map[string]any{"message": fmt.Sprintf("tailing %s every 2s", table)})

	for {
		select {
		case <-client.Context().Done():
			return nil
		case <-rc.Ctx.Done():
			return nil
		case <-ticker.C:
			rows, err := queryOne[[]map[string]any](rc.Ctx, db,
				"SELECT * FROM type::table($tb) ORDER BY id DESC LIMIT 50",
				map[string]any{"tb": table})
			if err != nil {
				_ = enc.Encode(map[string]any{"error": err.Error()})
				continue
			}
			for _, row := range rows {
				norm, _ := normalize(row).(map[string]any)
				id, _ := norm["id"].(string)
				if id == "" || seen[id] {
					continue
				}
				seen[id] = true
				if err := enc.Encode(map[string]any{
					"time":   time.Now().Format(time.RFC3339),
					"id":     id,
					"record": norm,
				}); err != nil {
					return err
				}
			}
		}
	}
}

// --- open in browser (HTTP proxy) -------------------------------------------

// proxyURL returns the browser-facing "open in browser" URL. The proxy mount is
// core-owned and arrives on the request context — never hardcoded here.
func proxyURL(rc *plugin.RequestContext) (any, error) {
	return map[string]any{"url": rc.ProxyURL()}, nil
}

// ServeHTTPProxy reverse-proxies the SurrealDB HTTP endpoint into the browser
// through the gateway transport, satisfying the optional plugin.HTTPProxy
// capability. The gateway authenticates, authorizes, strips the route prefix, and
// hijacks the connection; redirects, assets, and WebSocket upgrades pass through.
// SurrealDB serves no web UI at its root, so "/" renders a small landing whose
// relative links stay under the proxy prefix.
func (s *session) ServeHTTPProxy(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "" || r.URL.Path == "/" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprintf(w, `<!doctype html><meta charset="utf-8"><title>SurrealDB — %[1]s</title>
<body style="font-family:system-ui;max-width:40rem;margin:3rem auto;line-height:1.6">
<h1>SurrealDB endpoint</h1>
<p>Proxied via the gateway to <code>%[1]s</code> (namespace <code>%[2]s</code>, database <code>%[3]s</code>).</p>
<ul><li><a href="health">health</a></li><li><a href="version">version</a></li></ul>
</body>`, s.opts.addr(), s.opts.namespace, s.opts.database)
		return
	}
	rp := httputil.NewSingleHostReverseProxy(s.opts.baseURL())
	rp.Transport = s.proxyTransport()
	rp.ServeHTTP(w, r)
}
