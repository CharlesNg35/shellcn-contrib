package memcached

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

// errProtocolFatal marks a failure that leaves the connection in an unknown
// state, so callers must drop it instead of returning it to the pool.
var errProtocolFatal = errors.New("memcached: connection unusable")

// textConn speaks the memcached ASCII protocol directly. gomemcache covers the
// data plane but exposes none of the stats, slab, LRU-crawler, meta-debug, or
// watcher surfaces this plugin is built around.
type textConn struct {
	raw net.Conn
	br  *bufio.Reader
	bw  *bufio.Writer
}

func newTextConn(raw net.Conn) *textConn {
	return &textConn{
		raw: raw,
		br:  bufio.NewReaderSize(raw, 32*1024),
		bw:  bufio.NewWriterSize(raw, 4*1024),
	}
}

func (c *textConn) Close() error { return c.raw.Close() }

func (c *textConn) deadline(ctx context.Context, fallback time.Duration) func() {
	at := time.Now().Add(fallback)
	if dl, ok := ctx.Deadline(); ok && dl.Before(at) {
		at = dl
	}
	_ = c.raw.SetDeadline(at)
	return func() { _ = c.raw.SetDeadline(time.Time{}) }
}

func (c *textConn) send(payload string) error {
	if _, err := c.bw.WriteString(payload); err != nil {
		return fmt.Errorf("%w: write command: %v", errProtocolFatal, err)
	}
	if err := c.bw.Flush(); err != nil {
		return fmt.Errorf("%w: flush command: %v", errProtocolFatal, err)
	}
	return nil
}

func (c *textConn) readLine() (string, error) {
	line, err := c.br.ReadString('\n')
	if err != nil {
		if errors.Is(err, io.EOF) && line == "" {
			return "", fmt.Errorf("%w: server closed the connection", errProtocolFatal)
		}
		return "", fmt.Errorf("%w: read reply: %v", errProtocolFatal, err)
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// readBlock reads exactly n value bytes plus the trailing CRLF.
func (c *textConn) readBlock(n int) ([]byte, error) {
	buf := make([]byte, n+2)
	if _, err := io.ReadFull(c.br, buf); err != nil {
		return nil, fmt.Errorf("%w: read data block: %v", errProtocolFatal, err)
	}
	return buf[:n], nil
}

// serverError maps the protocol's error replies onto SDK sentinels. Only
// SERVER_ERROR leaves the connection questionable; the others are recoverable.
func serverError(line string) error {
	switch {
	case strings.HasPrefix(line, "ERROR"):
		return fmt.Errorf("%w: memcached rejected the command", plugin.ErrInvalidInput)
	case strings.HasPrefix(line, "CLIENT_ERROR"):
		return fmt.Errorf("%w: %s", plugin.ErrInvalidInput, strings.TrimSpace(strings.TrimPrefix(line, "CLIENT_ERROR")))
	case strings.HasPrefix(line, "SERVER_ERROR"):
		return fmt.Errorf("%w: %s", plugin.ErrUnavailable, strings.TrimSpace(strings.TrimPrefix(line, "SERVER_ERROR")))
	default:
		return nil
	}
}

// statPair is one "STAT <name> <value>" reply line.
type statPair struct {
	Name  string
	Value string
}

// stats runs "stats" or "stats <arg>" and collects every STAT line up to END.
func (c *textConn) stats(ctx context.Context, arg string, timeout time.Duration) ([]statPair, error) {
	restore := c.deadline(ctx, timeout)
	defer restore()

	cmd := "stats"
	if arg != "" {
		cmd += " " + arg
	}
	if err := c.send(cmd + "\r\n"); err != nil {
		return nil, err
	}
	out := make([]statPair, 0, 64)
	for {
		line, err := c.readLine()
		if err != nil {
			return nil, err
		}
		if line == "END" {
			return out, nil
		}
		if err := serverError(line); err != nil {
			return nil, err
		}
		if !strings.HasPrefix(line, "STAT ") {
			continue
		}
		rest := line[len("STAT "):]
		name, value, _ := strings.Cut(rest, " ")
		out = append(out, statPair{Name: name, Value: value})
	}
}

// statsMap flattens a stats reply into a lookup table. Duplicate names keep the
// last value, matching how memcached emits totals after per-class entries.
func statsMap(pairs []statPair) map[string]string {
	out := make(map[string]string, len(pairs))
	for _, p := range pairs {
		out[p.Name] = p.Value
	}
	return out
}

// classStats groups "STAT <class>:<name> <value>" lines by slab class id.
func classStats(pairs []statPair, trimPrefix string) map[int]map[string]string {
	out := map[int]map[string]string{}
	for _, p := range pairs {
		name := strings.TrimPrefix(p.Name, trimPrefix)
		classText, stat, ok := strings.Cut(name, ":")
		if !ok {
			continue
		}
		id, err := strconv.Atoi(classText)
		if err != nil {
			continue
		}
		if out[id] == nil {
			out[id] = map[string]string{}
		}
		out[id][stat] = p.Value
	}
	return out
}

// metaItem is one record from "lru_crawler metadump". Its "exp" and "la" fields
// are absolute Unix timestamps, unlike the relative values "me" reports.
type metaItem struct {
	Key      string
	Class    int
	Size     int64
	Expires  int64
	LastRead int64
	CAS      uint64
	Flags    uint32
	Fetched  bool
}

// metadumpResult reports whether the crawl was cut short by the configured cap.
// Retire is set when the dump was abandoned mid-stream, leaving unread bytes on
// the connection.
type metadumpResult struct {
	Items     []metaItem
	Truncated bool
	Retire    bool
}

// metadump walks the LRU with "lru_crawler metadump". The crawler emits one
// line per live item with no server-side paging, so the cap is enforced here and
// the connection is retired when it trips, rather than draining an unbounded
// dump just to reach END.
func (c *textConn) metadump(ctx context.Context, class string, limit int, timeout time.Duration) (metadumpResult, error) {
	restore := c.deadline(ctx, timeout)
	defer restore()

	if class == "" {
		class = "all"
	}
	if err := c.send("lru_crawler metadump " + class + "\r\n"); err != nil {
		return metadumpResult{}, err
	}
	res := metadumpResult{Items: make([]metaItem, 0, min(limit, 512))}
	for {
		line, err := c.readLine()
		if err != nil {
			return metadumpResult{}, err
		}
		switch {
		case line == "END":
			return res, nil
		case strings.HasPrefix(line, "BUSY"):
			return metadumpResult{}, fmt.Errorf("%w: the LRU crawler is already running", plugin.ErrConflict)
		case strings.HasPrefix(line, "BADCLASS"):
			return metadumpResult{}, fmt.Errorf("%w: unknown slab class %q", plugin.ErrInvalidInput, class)
		}
		if err := serverError(line); err != nil {
			return metadumpResult{}, err
		}
		item, ok := parseMetaItem(line)
		if !ok {
			continue
		}
		res.Items = append(res.Items, item)
		if len(res.Items) >= limit {
			res.Truncated = true
			res.Retire = true
			return res, nil
		}
	}
}

func parseMetaItem(line string) (metaItem, bool) {
	item := metaItem{Class: -1}
	found := false
	for _, field := range strings.Fields(line) {
		name, value, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		switch name {
		case "key":
			item.Key = uriDecode(value)
			found = true
		case "exp":
			item.Expires, _ = strconv.ParseInt(value, 10, 64)
		case "la":
			item.LastRead, _ = strconv.ParseInt(value, 10, 64)
		case "cas":
			item.CAS, _ = strconv.ParseUint(value, 10, 64)
		case "fetch":
			item.Fetched = value == "yes" || value == "1"
		case "cls":
			item.Class, _ = strconv.Atoi(value)
		case "size":
			item.Size, _ = strconv.ParseInt(value, 10, 64)
		case "flags":
			if flags, err := strconv.ParseUint(value, 10, 32); err == nil {
				item.Flags = uint32(flags)
			}
		}
	}
	return item, found
}

// metaDebug runs "me <key>", the only command that reports an item's slab class,
// last-access time, and internal size without touching its LRU position.
func (c *textConn) metaDebug(ctx context.Context, key string, timeout time.Duration) (map[string]string, error) {
	restore := c.deadline(ctx, timeout)
	defer restore()

	if err := c.send("me " + key + "\r\n"); err != nil {
		return nil, err
	}
	line, err := c.readLine()
	if err != nil {
		return nil, err
	}
	switch {
	case line == "EN", strings.HasPrefix(line, "EN "):
		return nil, plugin.ErrNotFound
	case strings.HasPrefix(line, "ME "):
	default:
		if err := serverError(line); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("%w: unexpected meta debug reply %q", plugin.ErrUnavailable, line)
	}
	out := map[string]string{}
	for _, field := range strings.Fields(strings.TrimPrefix(line, "ME ")) {
		name, value, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		out[name] = uriDecode(value)
	}
	return out, nil
}

// maxConsoleReplyRows bounds a console reply. "lru_crawler metadump" emits one
// line per live item with no server-side paging, so an unbounded read would
// buffer the whole keyspace into the result frame.
const maxConsoleReplyRows = 5000

// commandResult is the parsed outcome of one console execution. Truncated marks
// a reply abandoned at the row cap, which leaves unread bytes on the connection.
type commandResult struct {
	Lines     []string
	Values    []retrievedValue
	Stats     []statPair
	Truncated bool
}

type retrievedValue struct {
	Key   string
	Flags uint32
	Bytes int
	CAS   uint64
	Value []byte
}

// execute runs one already-validated console command and reads its reply
// according to the reply shape of the command family.
func (c *textConn) execute(ctx context.Context, spec commandSpec, payload string, timeout time.Duration) (commandResult, error) {
	restore := c.deadline(ctx, timeout)
	defer restore()

	if err := c.send(payload); err != nil {
		return commandResult{}, err
	}
	switch spec.Reply {
	case replyStats:
		out := commandResult{}
		for {
			line, err := c.readLine()
			if err != nil {
				return commandResult{}, err
			}
			if line == "END" {
				return out, nil
			}
			if err := serverError(line); err != nil {
				return commandResult{}, err
			}
			// The LRU crawler answers a refused request with a single line and
			// no END, so it terminates the read as well.
			if strings.HasPrefix(line, "BUSY") || strings.HasPrefix(line, "BADCLASS") {
				out.Lines = append(out.Lines, line)
				return out, nil
			}
			if rest, ok := strings.CutPrefix(line, "STAT "); ok {
				name, value, _ := strings.Cut(rest, " ")
				out.Stats = append(out.Stats, statPair{Name: name, Value: value})
			} else {
				out.Lines = append(out.Lines, line)
			}
			if len(out.Stats)+len(out.Lines) >= maxConsoleReplyRows {
				out.Truncated = true
				return out, nil
			}
		}
	case replyRetrieval:
		return c.readRetrieval()
	case replyMeta:
		return c.readMeta()
	default:
		line, err := c.readLine()
		if err != nil {
			return commandResult{}, err
		}
		if err := serverError(line); err != nil {
			return commandResult{}, err
		}
		return commandResult{Lines: []string{line}}, nil
	}
}

func (c *textConn) readRetrieval() (commandResult, error) {
	out := commandResult{}
	for {
		line, err := c.readLine()
		if err != nil {
			return commandResult{}, err
		}
		if line == "END" {
			return out, nil
		}
		if err := serverError(line); err != nil {
			return commandResult{}, err
		}
		if !strings.HasPrefix(line, "VALUE ") {
			out.Lines = append(out.Lines, line)
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 4 {
			return commandResult{}, fmt.Errorf("%w: malformed VALUE header %q", errProtocolFatal, line)
		}
		flags, _ := strconv.ParseUint(parts[2], 10, 32)
		size, err := strconv.Atoi(parts[3])
		if err != nil {
			return commandResult{}, fmt.Errorf("%w: malformed VALUE size %q", errProtocolFatal, line)
		}
		value := retrievedValue{Key: parts[1], Flags: uint32(flags), Bytes: size}
		if len(parts) >= 5 {
			value.CAS, _ = strconv.ParseUint(parts[4], 10, 64)
		}
		body, err := c.readBlock(size)
		if err != nil {
			return commandResult{}, err
		}
		value.Value = body
		out.Values = append(out.Values, value)
		if len(out.Values)+len(out.Lines) >= maxConsoleReplyRows {
			out.Truncated = true
			return out, nil
		}
	}
}

func (c *textConn) readMeta() (commandResult, error) {
	line, err := c.readLine()
	if err != nil {
		return commandResult{}, err
	}
	if err := serverError(line); err != nil {
		return commandResult{}, err
	}
	parts := strings.Fields(line)
	if len(parts) < 2 || parts[0] != "VA" {
		return commandResult{Lines: []string{line}}, nil
	}
	size, err := strconv.Atoi(parts[1])
	if err != nil {
		return commandResult{}, fmt.Errorf("%w: malformed VA size %q", errProtocolFatal, line)
	}
	body, err := c.readBlock(size)
	if err != nil {
		return commandResult{}, err
	}
	return commandResult{
		Lines:  []string{line},
		Values: []retrievedValue{{Key: metaKeyFlag(parts[2:]), Bytes: size, Value: body}},
	}, nil
}

// metaKeyFlag pulls the key back out of a meta reply's returned flag tokens.
func metaKeyFlag(flags []string) string {
	for _, flag := range flags {
		if strings.HasPrefix(flag, "k") {
			return flag[1:]
		}
	}
	return ""
}

// authenticate performs memcached's ASCII token authentication (the -Y authfile
// mode), which is a fake "set" carrying "<username> <password>".
func (c *textConn) authenticate(ctx context.Context, username, password string, timeout time.Duration) error {
	restore := c.deadline(ctx, timeout)
	defer restore()

	payload := username + " " + password
	req := "set auth 0 0 " + strconv.Itoa(len(payload)) + "\r\n" + payload + "\r\n"
	if err := c.send(req); err != nil {
		return err
	}
	line, err := c.readLine()
	if err != nil {
		return err
	}
	if line == "STORED" {
		return nil
	}
	if strings.HasPrefix(line, "CLIENT_ERROR") {
		return fmt.Errorf("%w: memcached rejected the credentials", plugin.ErrUnauthorized)
	}
	if err := serverError(line); err != nil {
		return err
	}
	return fmt.Errorf("%w: unexpected authentication reply %q", plugin.ErrUnauthorized, line)
}

func (c *textConn) version(ctx context.Context, timeout time.Duration) (string, error) {
	restore := c.deadline(ctx, timeout)
	defer restore()

	if err := c.send("version\r\n"); err != nil {
		return "", err
	}
	line, err := c.readLine()
	if err != nil {
		return "", err
	}
	if err := serverError(line); err != nil {
		return "", err
	}
	return strings.TrimSpace(strings.TrimPrefix(line, "VERSION")), nil
}

// watch turns the connection into a log watcher. The connection is dedicated to
// the watcher for its lifetime and can no longer run ordinary commands.
func (c *textConn) watch(ctx context.Context, streams []string, timeout time.Duration) error {
	restore := c.deadline(ctx, timeout)
	defer restore()

	if err := c.send("watch " + strings.Join(streams, " ") + "\r\n"); err != nil {
		return err
	}
	line, err := c.readLine()
	if err != nil {
		return err
	}
	if line == "OK" {
		return nil
	}
	if err := serverError(line); err != nil {
		return err
	}
	return fmt.Errorf("%w: memcached refused the watcher: %s", plugin.ErrNotSupported, line)
}

// uriDecode reverses the "%NN" escaping memcached applies to keys and log values.
func uriDecode(in string) string {
	if !strings.Contains(in, "%") {
		return in
	}
	var b strings.Builder
	b.Grow(len(in))
	for i := 0; i < len(in); i++ {
		if in[i] == '%' && i+2 < len(in) {
			if v, err := strconv.ParseUint(in[i+1:i+3], 16, 8); err == nil {
				b.WriteByte(byte(v))
				i += 2
				continue
			}
		}
		b.WriteByte(in[i])
	}
	return b.String()
}

func atoiDefault(v string, def int64) int64 {
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil {
		return def
	}
	return n
}

func atofDefault(v string, def float64) float64 {
	n, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if err != nil {
		return def
	}
	return n
}
