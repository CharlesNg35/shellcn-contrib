package snowflake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeTarget is a scripted Snowflake: every statement a handler issues is matched
// against an ordered rule list, so a handler that changes its SQL fails loudly
// instead of silently reading nothing. Statements that carry a LIMIT/OFFSET are
// sliced accordingly, which is what makes cursor and hasNext behaviour testable
// without a live account.
type fakeTarget struct {
	mu       sync.Mutex
	rules    []fakeRule
	executed []string
}

type fakeRule struct {
	match   string
	columns []string
	rows    [][]driver.Value
}

func newTarget() *fakeTarget { return &fakeTarget{} }

func (f *fakeTarget) on(match string, columns []string, rows ...[]driver.Value) *fakeTarget {
	f.rules = append(f.rules, fakeRule{match: match, columns: columns, rows: rows})
	return f
}

func (f *fakeTarget) record(statement string) {
	f.mu.Lock()
	f.executed = append(f.executed, statement)
	f.mu.Unlock()
}

func (f *fakeTarget) statements() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.executed...)
}

// sawStatement reports whether any executed statement contains every fragment.
func (f *fakeTarget) sawStatement(fragments ...string) bool {
	for _, statement := range f.statements() {
		matched := true
		for _, fragment := range fragments {
			if !strings.Contains(statement, fragment) {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func (f *fakeTarget) resolve(query string) (fakeRule, bool) {
	for _, rule := range f.rules {
		if strings.Contains(query, rule.match) {
			return rule, true
		}
	}
	return fakeRule{}, false
}

type fakeConnector struct{ target *fakeTarget }

func (c fakeConnector) Connect(context.Context) (driver.Conn, error) {
	return &fakeConn{target: c.target}, nil
}

func (c fakeConnector) Driver() driver.Driver { return fakeDriver{} }

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("fake: use the connector")
}

type fakeConn struct{ target *fakeTarget }

func (c *fakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("fake: prepared statements are not used")
}

func (c *fakeConn) Close() error { return nil }

func (c *fakeConn) Begin() (driver.Tx, error) {
	return nil, errors.New("fake: transactions are not used")
}

func (c *fakeConn) Ping(context.Context) error { return nil }

func (c *fakeConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.target.record(query)
	return fakeResult{}, nil
}

// pageBounds reads the LIMIT/OFFSET the handler formatted into the statement, so
// cursor behaviour is exercised against the SQL the plugin really sends.
func pageBounds(query string) (int, int, bool) {
	m := pageClauseRE.FindStringSubmatch(strings.TrimSpace(query))
	if m == nil {
		return 0, 0, false
	}
	limit, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, 0, false
	}
	offset, err := strconv.Atoi(m[2])
	if err != nil {
		return 0, 0, false
	}
	return limit, offset, true
}

var pageClauseRE = regexp.MustCompile(`LIMIT (\d+) OFFSET (\d+)$`)

func (c *fakeConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.target.record(query)
	rule, ok := c.target.resolve(query)
	if !ok {
		return nil, fmt.Errorf("fake: no canned response for %q", query)
	}
	rows := rule.rows
	if limit, offset, ok := pageBounds(query); ok {
		if offset > len(rows) {
			offset = len(rows)
		}
		end := min(offset+limit, len(rows))
		rows = rows[offset:end]
	}
	return &fakeRows{columns: rule.columns, rows: rows}, nil
}

type fakeResult struct{}

func (fakeResult) LastInsertId() (int64, error) { return 0, nil }
func (fakeResult) RowsAffected() (int64, error) { return 1, nil }

type fakeRows struct {
	columns []string
	rows    [][]driver.Value
	i       int
}

func (r *fakeRows) Columns() []string { return r.columns }
func (r *fakeRows) Close() error      { return nil }

func (r *fakeRows) Next(dest []driver.Value) error {
	if r.i >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.i])
	r.i++
	return nil
}

func newSession(t *testing.T, target *fakeTarget, mutate func(*options)) *Session {
	t.Helper()
	opts := options{
		Account:         "myorg-myaccount",
		Host:            "myorg-myaccount.snowflakecomputing.com",
		Port:            defaultPort,
		Database:        "TESTDB",
		Schema:          "PUBLIC",
		Username:        "svc",
		Password:        "secret",
		ReadOnly:        false,
		RequireConfirm:  false,
		QueryTimeout:    5 * time.Second,
		RowLimit:        defaultRowLimit,
		MaxConns:        2,
		HistoryWindow:   defaultHistoryWindow,
		MetricsInterval: defaultMetricsInterval,
		RedactPatterns:  []string{`(?i)token`, `(?i)password`},
	}
	if mutate != nil {
		mutate(&opts)
	}
	db := sql.OpenDB(fakeConnector{target: target})
	db.SetMaxOpenConns(opts.MaxConns)
	s := &Session{db: db, opts: opts, running: map[string]context.CancelFunc{}}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
