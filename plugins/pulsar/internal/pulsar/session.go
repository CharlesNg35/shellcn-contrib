package pulsar

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	pulsarclient "github.com/apache/pulsar-client-go/pulsar"
	pulsarlog "github.com/apache/pulsar-client-go/pulsar/log"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

// maxResponseBody caps an admin API response so a pathological topic list or
// stats document cannot exhaust the plugin's memory.
const maxResponseBody = 16 << 20

type Session struct {
	http *http.Client
	opts options

	mu        sync.Mutex
	messaging pulsarclient.Client
	closed    bool
}

func connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	opts, err := parseOptions(cfg)
	if err != nil {
		return nil, err
	}
	s := &Session{http: httpClient(cfg, opts), opts: opts}
	if err := s.HealthCheck(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// HealthCheck proves the admin API is reachable and the credentials are
// accepted. A non-superuser token authenticates fine but cannot list clusters,
// so 403 still means a usable connection.
func (s *Session) HealthCheck(ctx context.Context) error {
	var clusters []string
	err := s.get(ctx, "/admin/v2/clusters", nil, &clusters)
	if errors.Is(err, plugin.ErrForbidden) {
		return nil
	}
	return err
}

func (s *Session) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}

func (s *Session) Close() error {
	s.mu.Lock()
	client := s.messaging
	s.messaging, s.closed = nil, true
	s.mu.Unlock()
	if client != nil {
		client.Close()
	}
	s.http.CloseIdleConnections()
	return nil
}

// broker lazily opens the binary-protocol client. Only produce and live tail
// need it, so a browse-only connection never dials the broker port.
//
// pulsar-client-go v0.21.0 dials with net.Dial internally and exposes no dialer
// hook, which is why the manifest only offers the direct transport: the admin
// REST calls ride cfg.Net, but this client cannot.
func (s *Session) broker() (pulsarclient.Client, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, fmt.Errorf("%w: session is closed", plugin.ErrUnavailable)
	}
	if s.messaging != nil {
		return s.messaging, nil
	}
	serviceURL := strings.TrimSpace(s.opts.ServiceURL)
	if serviceURL == "" {
		return nil, fmt.Errorf("%w: broker service URL is not configured on this connection", plugin.ErrInvalidInput)
	}
	opts := pulsarclient.ClientOptions{
		URL:               serviceURL,
		ConnectionTimeout: s.opts.Timeout,
		OperationTimeout:  s.opts.Timeout,
		TLSConfig:         s.opts.TLSConfig,
		Logger:            pulsarlog.DefaultNopLogger(),
	}
	switch {
	case s.opts.Token != "":
		opts.Authentication = pulsarclient.NewAuthenticationToken(s.opts.Token)
	case s.opts.Username != "" || s.opts.Password != "":
		auth, err := pulsarclient.NewAuthenticationBasic(s.opts.Username, s.opts.Password)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", plugin.ErrUnauthorized, err)
		}
		opts.Authentication = auth
	}
	client, err := pulsarclient.NewClient(opts)
	if err != nil {
		return nil, fmt.Errorf("%w: connect to broker: %v", plugin.ErrUnavailable, err)
	}
	s.messaging = client
	return client, nil
}

func (s *Session) get(ctx context.Context, path string, query url.Values, out any) error {
	return s.send(ctx, http.MethodGet, path, query, nil, out)
}

func (s *Session) send(ctx context.Context, method, path string, query url.Values, body, out any) error {
	req, err := s.newRequest(ctx, method, path, query, body)
	if err != nil {
		return err
	}
	_, data, err := s.do(req)
	if err != nil {
		return err
	}
	if out == nil || len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("%w: decode %s: %v", plugin.ErrUnavailable, path, err)
	}
	return nil
}

// raw returns the response headers alongside the body. Peek and examine answer
// with the message payload as the body and every metadata field as an
// X-Pulsar-* header, so both halves are needed to build a message row.
func (s *Session) raw(ctx context.Context, path string, query url.Values) (http.Header, []byte, error) {
	req, err := s.newRequest(ctx, http.MethodGet, path, query, nil)
	if err != nil {
		return nil, nil, err
	}
	return s.do(req)
}

func (s *Session) newRequest(ctx context.Context, method, path string, query url.Values, body any) (*http.Request, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(encoded)
	}
	target := s.opts.AdminURL + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", plugin.ErrInvalidInput, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json, */*")
	switch {
	case s.opts.Token != "":
		req.Header.Set("Authorization", "Bearer "+s.opts.Token)
	case s.opts.Username != "" || s.opts.Password != "":
		req.SetBasicAuth(s.opts.Username, s.opts.Password)
	}
	return req, nil
}

func (s *Session) do(req *http.Request) (http.Header, []byte, error) {
	resp, err := s.http.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
	if err != nil {
		return nil, nil, fmt.Errorf("%w: read response: %v", plugin.ErrUnavailable, err)
	}
	if resp.StatusCode >= 400 {
		return nil, nil, httpError(resp.StatusCode, data)
	}
	return resp.Header, data, nil
}

func httpError(status int, data []byte) error {
	message := reasonOf(data)
	if message == "" {
		message = http.StatusText(status)
	}
	switch status {
	case http.StatusBadRequest, http.StatusPreconditionFailed:
		return fmt.Errorf("%w: %s", plugin.ErrInvalidInput, message)
	case http.StatusUnauthorized:
		return fmt.Errorf("%w: %s", plugin.ErrUnauthorized, message)
	case http.StatusForbidden:
		return fmt.Errorf("%w: %s", plugin.ErrForbidden, message)
	case http.StatusNotFound:
		return fmt.Errorf("%w: %s", plugin.ErrNotFound, message)
	case http.StatusMethodNotAllowed, http.StatusNotImplemented:
		return fmt.Errorf("%w: %s", plugin.ErrNotSupported, message)
	case http.StatusConflict:
		return fmt.Errorf("%w: %s", plugin.ErrConflict, message)
	default:
		return fmt.Errorf("%w: Pulsar admin API returned %d: %s", plugin.ErrUnavailable, status, message)
	}
}

func reasonOf(data []byte) string {
	var body struct {
		Reason  string `json:"reason"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(data, &body); err == nil {
		if reason := strings.TrimSpace(body.Reason); reason != "" {
			return reason
		}
		if message := strings.TrimSpace(body.Message); message != "" {
			return message
		}
	}
	return strings.TrimSpace(string(data))
}

func ensureWritable(s *Session) error {
	if s.opts.ReadOnly {
		return fmt.Errorf("%w: connection is read-only", plugin.ErrForbidden)
	}
	return nil
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
