package nomad

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hashicorp/nomad/api"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type Session struct {
	client *api.Client
	opts   options

	// watch talks to the same cluster without the request client's deadline, for
	// the event stream a live list keeps open indefinitely.
	watch *api.Client

	// stream carries the unbounded reads (log follow, exec upgrade) that must not
	// inherit the request client's deadline.
	stream *http.Client
	dialer *websocket.Dialer
}

func connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	opts, err := parseOptions(cfg)
	if err != nil {
		return nil, err
	}
	transport := httpTransport(cfg, opts)
	streamClient := &http.Client{Transport: transport}
	newClient := func(httpClient *http.Client) (*api.Client, error) {
		return api.NewClient(&api.Config{
			Address:    opts.Address,
			Region:     opts.Region,
			Namespace:  opts.Namespace,
			SecretID:   opts.Token,
			HttpClient: httpClient,
		})
	}
	client, err := newClient(&http.Client{Transport: transport, Timeout: opts.Timeout})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", plugin.ErrInvalidInput, err)
	}
	watch, err := newClient(streamClient)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", plugin.ErrInvalidInput, err)
	}
	s := &Session{
		client: client,
		watch:  watch,
		opts:   opts,
		stream: streamClient,
		dialer: &websocket.Dialer{
			NetDialContext:   transport.DialContext,
			TLSClientConfig:  opts.TLSConfig,
			HandshakeTimeout: opts.Timeout,
			ReadBufferSize:   4096,
			WriteBufferSize:  4096,
		},
	}
	return s, s.HealthCheck(ctx)
}

func (s *Session) HealthCheck(ctx context.Context) error {
	if _, err := s.client.Status().Leader(); err != nil {
		return nomadErr(err)
	}
	q := s.baseQuery(ctx)
	q.PerPage = 1
	if _, _, err := s.client.Jobs().List(q); err != nil {
		return nomadErr(err)
	}
	return nil
}

func (s *Session) Close() error {
	s.client.Close()
	s.watch.Close()
	s.stream.CloseIdleConnections()
	return nil
}

func (s *Session) OpenChannel(ctx context.Context, req plugin.ChannelRequest) (plugin.Channel, error) {
	switch req.Kind {
	case plugin.StreamLogs:
		return s.openLogs(ctx, req.Params)
	case plugin.StreamTerminal:
		return s.openExec(ctx, req.Params)
	default:
		return nil, plugin.ErrNotSupported
	}
}

func (s *Session) baseQuery(ctx context.Context) *api.QueryOptions {
	q := &api.QueryOptions{Region: s.opts.Region, Namespace: s.opts.Namespace}
	return q.WithContext(ctx)
}

func (s *Session) writeOptions(ctx context.Context, namespace string) *api.WriteOptions {
	w := &api.WriteOptions{Region: s.opts.Region, Namespace: namespace}
	return w.WithContext(ctx)
}

// endpoint builds an absolute API URL carrying the connection's region plus the
// caller's namespace, which is how every scoped read stays scoped when it is not
// going through the typed client.
func (s *Session) endpoint(path, namespace string, params url.Values) string {
	if params == nil {
		params = url.Values{}
	}
	if s.opts.Region != "" {
		params.Set("region", s.opts.Region)
	}
	if namespace != "" {
		params.Set("namespace", namespace)
	}
	if encoded := params.Encode(); encoded != "" {
		return s.opts.Address + path + "?" + encoded
	}
	return s.opts.Address + path
}

func (s *Session) rawStream(ctx context.Context, rawURL string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", plugin.ErrInvalidInput, err)
	}
	req.Header.Set("Accept", "application/json")
	if s.opts.Token != "" {
		req.Header.Set("X-Nomad-Token", s.opts.Token)
	}
	resp, err := s.stream.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		_ = resp.Body.Close()
		return nil, statusErr(resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return resp.Body, nil
}

func (s *Session) openLogs(ctx context.Context, params map[string]string) (plugin.Channel, error) {
	alloc := strings.TrimSpace(params["alloc"])
	task := strings.TrimSpace(params["task"])
	if alloc == "" || task == "" {
		return nil, fmt.Errorf("%w: allocation and task are required", plugin.ErrInvalidInput)
	}
	logType := strings.TrimSpace(params["type"])
	if logType != api.FSLogNameStderr {
		logType = api.FSLogNameStdout
	}
	tail := s.opts.LogLines
	if n, err := strconv.Atoi(strings.TrimSpace(params["tail"])); err == nil && n > 0 {
		tail = min(n, 10000)
	}
	query := url.Values{
		"task":   {task},
		"type":   {logType},
		"follow": {"true"},
		"origin": {api.OriginEnd},
		// The endpoint offsets in bytes from the end of the file, so the tail size
		// is scaled into a byte window rather than a line count.
		"offset": {strconv.Itoa(tail * 256)},
	}
	body, err := s.rawStream(ctx, s.endpoint("/v1/client/fs/logs/"+url.PathEscape(alloc), namespaceOrDefault(params["ns"], s.opts.Namespace), query))
	if err != nil {
		return nil, err
	}
	return &logChannel{body: body, dec: json.NewDecoder(body)}, nil
}

func (s *Session) openExec(ctx context.Context, params map[string]string) (plugin.Channel, error) {
	if !s.opts.AllowExec {
		return nil, fmt.Errorf("%w: allocation exec is disabled for this connection", plugin.ErrForbidden)
	}
	alloc := strings.TrimSpace(params["alloc"])
	task := strings.TrimSpace(params["task"])
	if alloc == "" || task == "" {
		return nil, fmt.Errorf("%w: allocation and task are required", plugin.ErrInvalidInput)
	}
	command, err := json.Marshal(execCommand(params["command"]))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", plugin.ErrInvalidInput, err)
	}
	query := url.Values{"task": {task}, "tty": {"true"}, "command": {string(command)}}
	target := s.endpoint("/v1/client/allocation/"+url.PathEscape(alloc)+"/exec", namespaceOrDefault(params["ns"], s.opts.Namespace), query)
	target = strings.Replace(strings.Replace(target, "https://", "wss://", 1), "http://", "ws://", 1)

	header := http.Header{}
	if s.opts.Token != "" {
		header.Set("X-Nomad-Token", s.opts.Token)
	}
	conn, resp, err := s.dialer.DialContext(ctx, target, header)
	if err != nil {
		if resp != nil {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
			_ = resp.Body.Close()
			return nil, statusErr(resp.StatusCode, strings.TrimSpace(string(body)))
		}
		return nil, fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	ch := &execChannel{conn: conn, done: make(chan struct{})}
	if cols, rows := terminalSize(params); cols > 0 && rows > 0 {
		_ = ch.Resize(cols, rows)
	}
	go ch.heartbeat()
	return ch, nil
}

// execCommand parses the browser-supplied command, falling back to a shell that
// exists in nearly every task image.
func execCommand(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return []string{"/bin/sh"}
	}
	var parsed []string
	if err := json.Unmarshal([]byte(raw), &parsed); err == nil && len(parsed) > 0 {
		return parsed
	}
	return strings.Fields(raw)
}

func terminalSize(params map[string]string) (int, int) {
	cols, _ := strconv.Atoi(strings.TrimSpace(params["cols"]))
	rows, _ := strconv.Atoi(strings.TrimSpace(params["rows"]))
	if cols <= 0 || rows <= 0 {
		return 0, 0
	}
	return cols, rows
}

type logChannel struct {
	body io.ReadCloser
	dec  *json.Decoder
	rest []byte
	once sync.Once
}

func (c *logChannel) Kind() plugin.StreamKind { return plugin.StreamLogs }

func (c *logChannel) Read(p []byte) (int, error) {
	for len(c.rest) == 0 {
		var frame api.StreamFrame
		if err := c.dec.Decode(&frame); err != nil {
			return 0, err
		}
		if frame.IsHeartbeat() || len(frame.Data) == 0 {
			continue
		}
		c.rest = frame.Data
	}
	n := copy(p, c.rest)
	c.rest = c.rest[n:]
	return n, nil
}

func (c *logChannel) Write(p []byte) (int, error) { return len(p), nil }

func (c *logChannel) Close() error {
	var err error
	c.once.Do(func() { err = c.body.Close() })
	return err
}

type execChannel struct {
	conn *websocket.Conn

	mu     sync.Mutex
	closed bool

	rest   []byte
	done   chan struct{}
	finish sync.Once
}

// finished releases the heartbeat once the remote command is gone; Read and
// Close both reach it, so the close has to happen exactly once.
func (c *execChannel) finished() {
	c.finish.Do(func() { close(c.done) })
}

func (c *execChannel) Kind() plugin.StreamKind { return plugin.StreamTerminal }

func (c *execChannel) Read(p []byte) (int, error) {
	for len(c.rest) == 0 {
		var frame api.ExecStreamingOutput
		if err := c.conn.ReadJSON(&frame); err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return 0, io.EOF
			}
			return 0, err
		}
		switch {
		case frame.Stdout != nil && len(frame.Stdout.Data) > 0:
			c.rest = frame.Stdout.Data
		case frame.Stderr != nil && len(frame.Stderr.Data) > 0:
			c.rest = frame.Stderr.Data
		case frame.Exited:
			code := 0
			if frame.Result != nil {
				code = frame.Result.ExitCode
			}
			c.rest = []byte(fmt.Sprintf("\r\n[exit status %d]\r\n", code))
			c.finished()
		}
	}
	n := copy(p, c.rest)
	c.rest = c.rest[n:]
	return n, nil
}

func (c *execChannel) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	data := make([]byte, len(p))
	copy(data, p)
	if err := c.send(api.ExecStreamingInput{Stdin: &api.ExecStreamingIOOperation{Data: data}}); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *execChannel) Resize(cols, rows int) error {
	return c.send(api.ExecStreamingInput{TTYSize: &api.TerminalSize{Height: rows, Width: cols}})
}

// send serialises writes: a websocket connection has a single writer, and
// keystrokes, resizes, and heartbeats all arrive from different goroutines. A
// write that cannot land means the remote command is gone, which is a clean end
// of the terminal rather than a transport failure to report.
func (c *execChannel) send(in api.ExecStreamingInput) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return io.EOF
	}
	if err := c.conn.WriteJSON(in); err != nil {
		return io.EOF
	}
	return nil
}

// heartbeat keeps the exec socket alive across idle shells; the Nomad client
// endpoint drops a session that goes quiet.
func (c *execChannel) heartbeat() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			if err := c.send(api.ExecStreamingInput{}); err != nil {
				return
			}
		}
	}
}

func (c *execChannel) Close() error {
	c.finished()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	return c.conn.Close()
}

func unwrap(sess plugin.Session) (*Session, error) {
	if s, ok := sess.(*Session); ok {
		return s, nil
	}
	// rc.Session is the core's borrowed Handle, which exposes the live session.
	if h, ok := sess.(interface{ Session() plugin.Session }); ok {
		if s, ok := h.Session().(*Session); ok {
			return s, nil
		}
	}
	return nil, plugin.ErrInvalidInput
}

func ensureWritable(s *Session) error {
	if s.opts.ReadOnly {
		return fmt.Errorf("%w: connection is read-only", plugin.ErrForbidden)
	}
	return nil
}

func namespaceOrDefault(value, fallback string) string {
	if v := strings.TrimSpace(value); v != "" {
		return v
	}
	return fallback
}

func statusErr(status int, message string) error {
	if message == "" {
		message = http.StatusText(status)
	}
	switch status {
	case http.StatusBadRequest:
		return fmt.Errorf("%w: %s", plugin.ErrInvalidInput, message)
	case http.StatusUnauthorized:
		return fmt.Errorf("%w: %s", plugin.ErrUnauthorized, message)
	case http.StatusForbidden:
		return fmt.Errorf("%w: %s", plugin.ErrForbidden, message)
	case http.StatusNotFound:
		return fmt.Errorf("%w: %s", plugin.ErrNotFound, message)
	case http.StatusConflict:
		return fmt.Errorf("%w: %s", plugin.ErrConflict, message)
	default:
		return fmt.Errorf("%w: Nomad API returned %d: %s", plugin.ErrUnavailable, status, message)
	}
}

func nomadErr(err error) error {
	if err == nil {
		return nil
	}
	var unexpected api.UnexpectedResponseError
	if errors.As(err, &unexpected) && unexpected.HasStatusCode() {
		return statusErr(unexpected.StatusCode(), strings.TrimSpace(unexpected.Body()))
	}
	message := err.Error()
	switch {
	case strings.Contains(message, api.PermissionDeniedErrorContent):
		return fmt.Errorf("%w: %s", plugin.ErrForbidden, message)
	case strings.Contains(strings.ToLower(message), "not found"):
		return fmt.Errorf("%w: %s", plugin.ErrNotFound, message)
	default:
		return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
}
