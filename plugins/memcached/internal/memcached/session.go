package memcached

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/bradfitz/gomemcache/memcache"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

// Session owns one memcached connection: the gomemcache pool for the data
// plane, a small pool of raw ASCII connections for the stats/slab/crawler
// surfaces gomemcache does not expose, and the dedicated watcher connection
// behind the activity timeline.
type Session struct {
	opts   options
	client *memcache.Client
	admin  *connPool
	events *eventHub
	closed atomic.Bool
}

func connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	opts, err := parseOptions(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.Net == nil {
		return nil, fmt.Errorf("%w: network transport is required", plugin.ErrUnavailable)
	}
	dial := dialer(opts, cfg.Net)

	client := memcache.NewFromSelector(pinnedServer(opts.address()))
	client.DialContext = dial
	client.Timeout = opts.Timeout
	client.MaxIdleConns = opts.IdleConns

	s := &Session{
		opts:   opts,
		client: client,
		admin:  newConnPool(opts.IdleConns, opts.address(), dial),
	}
	s.events = newEventHub(opts.EventBuffer, s.dialWatcher)
	if err := s.HealthCheck(ctx); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
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
	return nil, fmt.Errorf("%w: memcached session unavailable", plugin.ErrUnavailable)
}

func (s *Session) HealthCheck(ctx context.Context) error {
	if err := s.ensureOpen(); err != nil {
		return err
	}
	return s.withAdmin(ctx, func(c *textConn) error {
		_, err := c.version(ctx, s.opts.Timeout)
		return err
	})
}

func (s *Session) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}

func (s *Session) Close() error {
	s.closed.Store(true)
	if s.events != nil {
		s.events.stop()
	}
	if s.admin != nil {
		s.admin.close()
	}
	if s.client == nil {
		return nil
	}
	return s.client.Close()
}

func (s *Session) ensureOpen() error {
	if s == nil || s.closed.Load() {
		return fmt.Errorf("%w: memcached session closed", plugin.ErrUnavailable)
	}
	return nil
}

func (s *Session) ensureWritable() error {
	if s.opts.ReadOnly {
		return fmt.Errorf("%w: read-only mode blocks write operations", plugin.ErrForbidden)
	}
	return nil
}

// withAdmin borrows a raw connection, runs fn, and retires the connection when
// the protocol state can no longer be trusted.
func (s *Session) withAdmin(ctx context.Context, fn func(*textConn) error) error {
	return s.withAdminConn(ctx, func(c *textConn) (bool, error) {
		return false, fn(c)
	})
}

// withAdminConn is withAdmin for callers that must retire a healthy-looking
// connection themselves, such as an LRU dump abandoned before its END line.
func (s *Session) withAdminConn(ctx context.Context, fn func(*textConn) (bool, error)) error {
	if err := s.ensureOpen(); err != nil {
		return err
	}
	conn, err := s.admin.acquire(ctx)
	if err != nil {
		return err
	}
	retire, err := fn(conn)
	fatal := errors.Is(err, errProtocolFatal)
	s.admin.release(conn, retire || fatal)
	if fatal {
		return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	return err
}

func (s *Session) dialWatcher(ctx context.Context) (*textConn, error) {
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}
	conn, err := s.admin.dialRaw(ctx)
	if err != nil {
		return nil, err
	}
	if err := conn.watch(ctx, s.opts.WatchStreams, s.opts.Timeout); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

// serverAddr is the configured target, verbatim. memcache.New would resolve the
// host with the process resolver, which both sends a lookup outside cfg.Net and
// leaves the client with no servers whenever the target only resolves on the far
// side of the gateway, so the address is pinned and handed to the gateway dialer
// untouched.
type serverAddr string

func (a serverAddr) Network() string { return "tcp" }

func (a serverAddr) String() string { return string(a) }

type pinnedServer string

func (p pinnedServer) PickServer(string) (net.Addr, error) { return serverAddr(p), nil }

func (p pinnedServer) Each(fn func(net.Addr) error) error { return fn(serverAddr(p)) }

// connPool hands out raw ASCII connections. Capacity is bounded so a busy
// dashboard cannot open an unbounded number of sockets to the target.
type connPool struct {
	addr string
	dial func(ctx context.Context, network, addr string) (net.Conn, error)
	idle chan *textConn
	sem  chan struct{}

	mu     sync.Mutex
	closed bool
}

func newConnPool(size int, addr string, dial func(ctx context.Context, network, addr string) (net.Conn, error)) *connPool {
	if size < 1 {
		size = 1
	}
	return &connPool{
		addr: addr,
		dial: dial,
		idle: make(chan *textConn, size),
		sem:  make(chan struct{}, size),
	}
}

func (p *connPool) dialRaw(ctx context.Context) (*textConn, error) {
	conn, err := p.dial(ctx, "tcp", p.addr)
	if err != nil {
		return nil, err
	}
	return newTextConn(conn), nil
}

func (p *connPool) acquire(ctx context.Context) (*textConn, error) {
	select {
	case conn := <-p.idle:
		return conn, nil
	default:
	}
	select {
	case p.sem <- struct{}{}:
	case conn := <-p.idle:
		return conn, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: waiting for a memcached connection: %v", plugin.ErrUnavailable, ctx.Err())
	}
	conn, err := p.dialRaw(ctx)
	if err != nil {
		<-p.sem
		return nil, err
	}
	return conn, nil
}

func (p *connPool) release(conn *textConn, retire bool) {
	if conn == nil {
		return
	}
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if retire || closed {
		_ = conn.Close()
		<-p.sem
		return
	}
	select {
	case p.idle <- conn:
	default:
		_ = conn.Close()
		<-p.sem
	}
}

func (p *connPool) close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.mu.Unlock()
	for {
		select {
		case conn := <-p.idle:
			_ = conn.Close()
		default:
			return
		}
	}
}
