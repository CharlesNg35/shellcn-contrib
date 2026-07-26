package memcached

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

// activityEvent is one memcached watcher log line, normalised for the timeline.
type activityEvent struct {
	Ref      plugin.ResourceIdentity `json:"ref"`
	GID      uint64                  `json:"gid"`
	Time     string                  `json:"time"`
	Reason   string                  `json:"reason"`
	Message  string                  `json:"message"`
	Severity plugin.Severity         `json:"severity"`
	Icon     string                  `json:"icon"`
	Resource string                  `json:"resource,omitempty"`
}

// eventHub owns the dedicated watcher connection and fans its log lines out to
// the timeline list route and any live watch streams. The connection is opened
// on first use so a connection that never opens the Activity tab never spends a
// socket on it.
type eventHub struct {
	limit int
	dial  func(ctx context.Context) (*textConn, error)

	mu      sync.Mutex
	buf     []activityEvent
	subs    map[chan activityEvent]struct{}
	started bool
	stopped bool
	conn    *textConn
	cancel  context.CancelFunc
	lastErr error
}

func newEventHub(limit int, dial func(ctx context.Context) (*textConn, error)) *eventHub {
	return &eventHub{limit: limit, dial: dial, subs: map[chan activityEvent]struct{}{}}
}

// ensure starts the watcher once and re-reports the last failure to every later
// caller, so a memcached build that refuses "watch" surfaces the same clear
// error on the timeline route and on the watch stream.
func (h *eventHub) ensure(ctx context.Context) error {
	h.mu.Lock()
	if h.stopped {
		h.mu.Unlock()
		return nil
	}
	if h.started {
		err := h.lastErr
		h.mu.Unlock()
		return err
	}
	h.started = true
	h.mu.Unlock()

	conn, err := h.dial(ctx)
	if err != nil {
		h.mu.Lock()
		h.lastErr = err
		h.started = false
		h.mu.Unlock()
		return err
	}

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	h.mu.Lock()
	if h.stopped {
		h.mu.Unlock()
		cancel()
		_ = conn.Close()
		return nil
	}
	h.conn = conn
	h.cancel = cancel
	h.lastErr = nil
	h.mu.Unlock()

	go h.run(runCtx, conn)
	return nil
}

func (h *eventHub) run(ctx context.Context, conn *textConn) {
	defer func() {
		_ = conn.Close()
		h.mu.Lock()
		if h.conn == conn {
			h.conn = nil
			h.started = false
		}
		h.mu.Unlock()
	}()
	for {
		line, err := conn.readLine()
		if err != nil {
			if ctx.Err() == nil {
				h.mu.Lock()
				h.lastErr = err
				h.mu.Unlock()
			}
			return
		}
		if ctx.Err() != nil {
			return
		}
		if event, ok := parseActivityLine(line); ok {
			h.publish(event)
		}
	}
}

func (h *eventHub) publish(event activityEvent) {
	h.mu.Lock()
	h.buf = append(h.buf, event)
	if len(h.buf) > h.limit {
		h.buf = h.buf[len(h.buf)-h.limit:]
	}
	subs := make([]chan activityEvent, 0, len(h.subs))
	for ch := range h.subs {
		subs = append(subs, ch)
	}
	h.mu.Unlock()
	for _, ch := range subs {
		select {
		case ch <- event:
		default:
		}
	}
}

// snapshot returns buffered events newest first.
func (h *eventHub) snapshot() []activityEvent {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]activityEvent, len(h.buf))
	for i, event := range h.buf {
		out[len(h.buf)-1-i] = event
	}
	return out
}

func (h *eventHub) subscribe() (<-chan activityEvent, func()) {
	ch := make(chan activityEvent, 64)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	return ch, func() {
		h.mu.Lock()
		delete(h.subs, ch)
		h.mu.Unlock()
	}
}

func (h *eventHub) stop() {
	h.mu.Lock()
	h.stopped = true
	cancel := h.cancel
	conn := h.conn
	h.conn = nil
	h.cancel = nil
	h.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if conn != nil {
		_ = conn.Close()
	}
}

var activitySeverity = map[string]plugin.Severity{
	"eviction":        plugin.SeverityWarn,
	"eviction_failed": plugin.SeverityDanger,
	"item_store":      plugin.SeveritySuccess,
	"item_link":       plugin.SeveritySuccess,
	"deletion":        plugin.SeverityWarn,
	"conn_new":        plugin.SeverityInfo,
	"conn_close":      plugin.SeverityInfo,
	"item_get":        plugin.SeverityInfo,
	"error":           plugin.SeverityDanger,
}

var activityIcon = map[string]string{
	"eviction":   "trash-2",
	"deletion":   "eraser",
	"item_store": "save",
	"item_link":  "save",
	"item_get":   "search",
	"conn_new":   "plug",
	"conn_close": "unplug",
}

// parseActivityLine turns a "ts=... gid=... type=... k=v" watcher line into a
// timeline record.
func parseActivityLine(line string) (activityEvent, bool) {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return activityEvent{}, false
	}
	event := activityEvent{Severity: plugin.SeverityInfo, Icon: "activity"}
	parts := make([]string, 0, len(fields))
	seen := false
	for _, field := range fields {
		name, value, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		seen = true
		value = uriDecode(value)
		switch name {
		case "ts":
			event.Time = watcherTimestamp(value)
		case "gid":
			event.GID, _ = strconv.ParseUint(value, 10, 64)
		case "type":
			event.Reason = value
		case "key":
			event.Resource = value
			parts = append(parts, name+"="+value)
		default:
			parts = append(parts, name+"="+value)
		}
	}
	if !seen {
		return activityEvent{}, false
	}
	if event.Time == "" {
		event.Time = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if event.Reason == "" {
		event.Reason = "log"
	}
	if severity, ok := activitySeverity[event.Reason]; ok {
		event.Severity = severity
	}
	if icon, ok := activityIcon[event.Reason]; ok {
		event.Icon = icon
	}
	event.Message = strings.Join(parts, "  ")
	event.Ref = plugin.ResourceIdentity{
		Kind: "event",
		Name: event.Reason,
		UID:  strconv.FormatUint(event.GID, 10),
	}
	return event, true
}

// watcherTimestamp converts memcached's "seconds.microseconds" stamp to RFC3339.
func watcherTimestamp(raw string) string {
	secText, fracText, _ := strings.Cut(raw, ".")
	sec, err := strconv.ParseInt(secText, 10, 64)
	if err != nil {
		return ""
	}
	nsec := int64(0)
	if fracText != "" {
		for len(fracText) < 9 {
			fracText += "0"
		}
		nsec, _ = strconv.ParseInt(fracText[:9], 10, 64)
	}
	return time.Unix(sec, nsec).UTC().Format(time.RFC3339Nano)
}

// sortEventsByTime keeps the timeline newest-first even though memcached may
// emit log lines out of order across worker threads.
func sortEventsByTime(events []activityEvent) {
	sort.SliceStable(events, func(i, j int) bool { return events[i].GID > events[j].GID })
}
