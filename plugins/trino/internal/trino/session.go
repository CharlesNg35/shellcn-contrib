package trino

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	trinodriver "github.com/trinodb/trino-go-client/trino"

	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

var clientSeq atomic.Uint64

type Session struct {
	db         *sql.DB
	opts       options
	clientName string

	mu      sync.Mutex
	running map[string]runningQuery
}

type runningQuery struct {
	cancel   context.CancelFunc
	progress *progressCollector
}

func connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	opts, err := parseOptions(cfg)
	if err != nil {
		return nil, err
	}
	httpClient, upstream, err := httpClientFor(opts, cfg.Net)
	if err != nil {
		return nil, err
	}
	clientName := protocolName + "-shellcn-" + strconv.FormatUint(clientSeq.Add(1), 36)
	if err := trinodriver.RegisterCustomClient(clientName, httpClient); err != nil {
		return nil, fmt.Errorf("%w: register Trino HTTP client: %v", plugin.ErrUnavailable, err)
	}
	dsn, err := buildDSN(opts, clientName, upstream)
	if err != nil {
		trinodriver.DeregisterCustomClient(clientName)
		return nil, err
	}
	db, err := sql.Open("trino", dsn)
	if err != nil {
		trinodriver.DeregisterCustomClient(clientName)
		return nil, fmt.Errorf("%w: open Trino connection: %v", plugin.ErrUnavailable, err)
	}
	db.SetMaxOpenConns(opts.MaxConns)
	db.SetMaxIdleConns(opts.MaxConns)
	db.SetConnMaxIdleTime(5 * time.Minute)
	db.SetConnMaxLifetime(30 * time.Minute)
	s := &Session{db: db, opts: opts, clientName: clientName, running: map[string]runningQuery{}}
	if err := s.HealthCheck(ctx); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

// httpClientFor prefers the gateway's L7 transport (agent HTTP proxy mode),
// returning the upstream it addresses, and otherwise wraps the gateway dialer,
// so the plugin never opens its own socket.
func httpClientFor(opts options, transport plugin.NetTransport) (*http.Client, string, error) {
	if upstream, rt, ok := transport.HTTP(); ok && rt != nil {
		return &http.Client{Transport: withAuth(rt, opts)}, strings.TrimSpace(upstream), nil
	}
	tlsConfig, err := sqldb.TLSConfig(sqldb.TLSOptions{
		Mode:              opts.TLSMode,
		Host:              opts.Host,
		CACertificate:     opts.CACertificate,
		ClientCertificate: opts.ClientCertificate,
	})
	if err != nil {
		return nil, "", err
	}
	return &http.Client{Transport: withAuth(&http.Transport{
		DialContext:           transport.DialContext,
		TLSClientConfig:       tlsConfig,
		MaxIdleConns:          16,
		MaxIdleConnsPerHost:   8,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: opts.QueryTimeout,
	}, opts)}, "", nil
}

// authTransport carries the coordinator credentials. They live here instead of
// in the DSN so the secret is never part of a string the driver can echo back
// in a parse error, and so it survives a gateway upstream whose scheme the
// driver's own basic-auth path would refuse.
type authTransport struct {
	base     http.RoundTripper
	username string
	password string
	token    string
}

func withAuth(base http.RoundTripper, opts options) http.RoundTripper {
	if opts.Password == "" && opts.AccessToken == "" {
		return base
	}
	return &authTransport{base: base, username: opts.Username, password: opts.Password, token: opts.AccessToken}
}

func (t *authTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	out := req.Clone(req.Context())
	switch {
	case t.token != "":
		out.Header.Set("Authorization", "Bearer "+t.token)
	case t.password != "":
		out.SetBasicAuth(t.username, t.password)
	}
	return t.base.RoundTrip(out)
}

func unwrap(sess plugin.Session) (*Session, error) {
	if s, ok := sess.(*Session); ok {
		return s, nil
	}
	type sessionGetter interface {
		Session() plugin.Session
	}
	if h, ok := sess.(sessionGetter); ok {
		if s, ok := h.Session().(*Session); ok {
			return s, nil
		}
	}
	return nil, fmt.Errorf("%w: Trino session unavailable", plugin.ErrUnavailable)
}

func (s *Session) HealthCheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.opts.QueryTimeout)
	defer cancel()
	var version string
	if err := s.db.QueryRowContext(ctx, "SELECT node_version FROM system.runtime.nodes WHERE coordinator LIMIT 1").Scan(&version); err != nil {
		return fmt.Errorf("%w: Trino coordinator probe: %v", plugin.ErrUnavailable, err)
	}
	return nil
}

func (s *Session) Close() error {
	s.mu.Lock()
	for id, q := range s.running {
		q.cancel()
		delete(s.running, id)
	}
	s.mu.Unlock()
	// The pool closes first: a connection still opening would fail to find its
	// client in the driver registry.
	err := s.db.Close()
	trinodriver.DeregisterCustomClient(s.clientName)
	return err
}

func (s *Session) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}

func (s *Session) track(id string, q runningQuery) {
	s.mu.Lock()
	s.running[id] = q
	s.mu.Unlock()
}

func (s *Session) untrack(id string) {
	s.mu.Lock()
	delete(s.running, id)
	s.mu.Unlock()
}

// cancelAll aborts every in-flight editor query. The driver issues the
// coordinator DELETE for each cancelled context, so the server-side query is
// torn down too; the collected query ids are returned for the audit trail.
func (s *Session) cancelAll() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.running))
	for id, q := range s.running {
		if queryID, _ := q.progress.snapshot(); queryID != "" {
			ids = append(ids, queryID)
		}
		q.cancel()
		delete(s.running, id)
	}
	return ids
}

// progressCollector receives the driver's query progress callbacks. The driver
// keeps the updater on the pooled connection after a query ends, so a later
// statement on that same connection may push one more frame here; the value is
// only ever read while its own query is in flight.
type progressCollector struct {
	mu      sync.Mutex
	queryID string
	stats   queryStats
	seen    bool
}

type queryStats struct {
	State              string  `json:"state,omitempty"`
	Scheduled          bool    `json:"scheduled,omitempty"`
	Nodes              int     `json:"nodes,omitempty"`
	TotalSplits        int     `json:"totalSplits,omitempty"`
	QueuedSplits       int     `json:"queuedSplits,omitempty"`
	RunningSplits      int     `json:"runningSplits,omitempty"`
	CompletedSplits    int     `json:"completedSplits,omitempty"`
	CPUTimeMS          int64   `json:"cpuTimeMs,omitempty"`
	WallTimeMS         int64   `json:"wallTimeMs,omitempty"`
	QueuedTimeMS       int64   `json:"queuedTimeMs,omitempty"`
	ElapsedTimeMS      int64   `json:"elapsedTimeMs,omitempty"`
	ProcessedRows      int64   `json:"processedRows,omitempty"`
	ProcessedBytes     int64   `json:"processedBytes,omitempty"`
	PhysicalInputBytes int64   `json:"physicalInputBytes,omitempty"`
	PeakMemoryBytes    int64   `json:"peakMemoryBytes,omitempty"`
	SpilledBytes       int64   `json:"spilledBytes,omitempty"`
	ProgressPercent    float64 `json:"progressPercent,omitempty"`
}

func (p *progressCollector) Update(info trinodriver.QueryProgressInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seen = true
	p.queryID = info.QueryId
	p.stats = queryStats{
		State:              info.QueryStats.State,
		Scheduled:          info.QueryStats.Scheduled,
		Nodes:              info.QueryStats.Nodes,
		TotalSplits:        info.QueryStats.TotalSplits,
		QueuedSplits:       info.QueryStats.QueuesSplits,
		RunningSplits:      info.QueryStats.RunningSplits,
		CompletedSplits:    info.QueryStats.CompletedSplits,
		CPUTimeMS:          info.QueryStats.CPUTimeMillis,
		WallTimeMS:         info.QueryStats.WallTimeMillis,
		QueuedTimeMS:       info.QueryStats.QueuedTimeMillis,
		ElapsedTimeMS:      info.QueryStats.ElapsedTimeMillis,
		ProcessedRows:      info.QueryStats.ProcessedRows,
		ProcessedBytes:     info.QueryStats.ProcessedBytes,
		PhysicalInputBytes: info.QueryStats.PhysicalInputBytes,
		PeakMemoryBytes:    info.QueryStats.PeakMemoryBytes,
		SpilledBytes:       info.QueryStats.SpilledBytes,
		ProgressPercent:    float64(info.QueryStats.ProgressPercentage),
	}
}

func (p *progressCollector) snapshot() (string, *queryStats) {
	if p == nil {
		return "", nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.seen {
		return "", nil
	}
	stats := p.stats
	return p.queryID, &stats
}

var _ trinodriver.ProgressUpdater = (*progressCollector)(nil)
