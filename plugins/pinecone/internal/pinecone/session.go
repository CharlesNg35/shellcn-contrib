package pinecone

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

// maxResponseBytes bounds a single REST response; a fetch of 1000 dense vectors
// is a few megabytes, and nothing legitimate approaches this.
const maxResponseBytes = 64 << 20

type dialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

type Session struct {
	opts    Options
	dial    dialFunc
	control *restClient

	mu    sync.Mutex
	data  map[string]*restClient
	hosts map[string]string
}

func newSession(opts Options, dial dialFunc) *Session {
	s := &Session{
		opts:  opts,
		dial:  dial,
		data:  map[string]*restClient{},
		hosts: map[string]string{},
	}
	s.control = s.newClient(opts.Endpoint)
	return s
}

func (s *Session) HealthCheck(ctx context.Context) error {
	var out indexList
	return s.control.do(ctx, http.MethodGet, "/indexes", nil, nil, &out)
}

func (s *Session) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}

func (s *Session) Close() error {
	s.mu.Lock()
	clients := make([]*restClient, 0, len(s.data)+1)
	for _, c := range s.data {
		clients = append(clients, c)
	}
	s.data = map[string]*restClient{}
	s.hosts = map[string]string{}
	s.mu.Unlock()
	for _, c := range append(clients, s.control) {
		c.close()
	}
	return nil
}

func (s *Session) ensureWritable() error {
	if s.opts.ReadOnly {
		return fmt.Errorf("%w: connection is read-only", plugin.ErrForbidden)
	}
	return nil
}

func (s *Session) newClient(base string) *restClient {
	return &restClient{
		http: &http.Client{
			Transport: &http.Transport{TLSClientConfig: s.opts.TLSConfig, DialContext: s.dial},
			Timeout:   s.opts.Timeout,
		},
		base: strings.TrimRight(base, "/"),
		headers: map[string]string{
			apiKeyHeader:     s.opts.APIKey,
			apiVersionHeader: s.opts.APIVersion,
		},
	}
}

// indexClient resolves an index's data-plane host and returns a client bound to
// it. Hosts never change for the lifetime of an index, so they are cached.
func (s *Session) indexClient(ctx context.Context, name string) (*restClient, error) {
	if err := validateName("index", name); err != nil {
		return nil, err
	}
	s.mu.Lock()
	host, cached := s.hosts[name]
	if cached {
		if client := s.data[host]; client != nil {
			s.mu.Unlock()
			return client, nil
		}
	}
	s.mu.Unlock()
	if !cached {
		idx, err := s.describeIndex(ctx, name)
		if err != nil {
			return nil, err
		}
		host = dataPlaneURL(s.opts, idx)
		if host == "" {
			return nil, fmt.Errorf("%w: index %q reported no data-plane host", plugin.ErrUnavailable, name)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hosts[name] = host
	if client := s.data[host]; client != nil {
		return client, nil
	}
	client := s.newClient(host)
	s.data[host] = client
	return client, nil
}

func (s *Session) forgetIndex(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.hosts, name)
}

// dataPlaneURL turns the scheme-less host Pinecone reports into a base URL. The
// control plane's scheme is reused so a plaintext deployment stays plaintext.
func dataPlaneURL(opts Options, idx indexInfo) string {
	host := strings.TrimSpace(idx.Host)
	if opts.PrivateNet && strings.TrimSpace(idx.PrivateHost) != "" {
		host = strings.TrimSpace(idx.PrivateHost)
	}
	if host == "" {
		return ""
	}
	if strings.Contains(host, "://") {
		return strings.TrimRight(host, "/")
	}
	return opts.Scheme + "://" + strings.TrimRight(host, "/")
}

type restClient struct {
	http    *http.Client
	base    string
	headers map[string]string
}

func (c *restClient) close() { c.http.CloseIdleConnections() }

func (c *restClient) do(ctx context.Context, method, path string, query url.Values, body, out any) error {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("%w: request body is not JSON-encodable: %v", plugin.ErrInvalidInput, err)
		}
		reader = bytes.NewReader(data)
	}
	endpoint := c.base + path
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return fmt.Errorf("%w: %v", plugin.ErrInvalidInput, err)
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for key, value := range c.headers {
		if value != "" {
			req.Header.Set(key, value)
		}
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	if resp.StatusCode >= 400 {
		return apiError(resp.StatusCode, data)
	}
	if out == nil || len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("%w: malformed Pinecone response: %v", plugin.ErrUnavailable, err)
	}
	return nil
}

// apiError maps both Pinecone error shapes: the control plane returns
// {"error":{"code","message"},"status"}, the data plane returns {"code","message"}.
func apiError(status int, data []byte) error {
	var body struct {
		Error *struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
		Message string `json:"message"`
	}
	message, code := "", ""
	if json.Unmarshal(data, &body) == nil {
		message = strings.TrimSpace(body.Message)
		if body.Error != nil {
			code = strings.ToUpper(strings.TrimSpace(body.Error.Code))
			if msg := strings.TrimSpace(body.Error.Message); msg != "" {
				message = msg
			}
		}
	}
	if message == "" {
		message = strings.TrimSpace(string(data))
	}
	if message == "" {
		message = http.StatusText(status)
	}
	switch {
	case code == "ALREADY_EXISTS", status == http.StatusConflict:
		return fmt.Errorf("%w: %s", plugin.ErrConflict, message)
	case code == "NOT_FOUND", status == http.StatusNotFound:
		return fmt.Errorf("%w: %s", plugin.ErrNotFound, message)
	case code == "UNAUTHORIZED", status == http.StatusUnauthorized:
		return fmt.Errorf("%w: %s", plugin.ErrUnauthorized, message)
	case code == "FORBIDDEN", code == "QUOTA_EXCEEDED", status == http.StatusForbidden:
		return fmt.Errorf("%w: %s", plugin.ErrForbidden, message)
	case status == http.StatusBadRequest, status == http.StatusUnprocessableEntity:
		return fmt.Errorf("%w: %s", plugin.ErrInvalidInput, message)
	default:
		return fmt.Errorf("%w: Pinecone returned %d: %s", plugin.ErrUnavailable, status, message)
	}
}

func unwrap(sess plugin.Session) (*Session, error) {
	if s, ok := sess.(*Session); ok {
		return s, nil
	}
	if h, ok := sess.(interface{ Session() plugin.Session }); ok {
		if s, ok := h.Session().(*Session); ok {
			return s, nil
		}
	}
	return nil, fmt.Errorf("%w: no Pinecone session on this request", plugin.ErrInvalidInput)
}

func session(rc *plugin.RequestContext) (*Session, error) { return unwrap(rc.Session) }

// namespaceOf resolves the namespace a request addresses. Params win over the
// connection default so one connection can browse every namespace, and the
// resolved value rides every paginated follow-up because it is a route param.
func namespaceOf(rc *plugin.RequestContext, s *Session) (string, error) {
	ns := strings.TrimSpace(rc.Param("namespace"))
	if ns == "" {
		ns = strings.TrimSpace(rc.Param("scope"))
	}
	if ns == "" {
		ns = s.opts.Namespace
	}
	if ns == "" {
		ns = defaultNamespace
	}
	if err := validateNamespace(ns); err != nil {
		return "", err
	}
	return ns, nil
}

func indexOf(rc *plugin.RequestContext) (string, error) {
	name := strings.TrimSpace(rc.Param("index"))
	if name == "" {
		name = strings.TrimSpace(rc.Param("scope"))
	}
	if err := validateName("index", name); err != nil {
		return "", err
	}
	return name, nil
}

// indexTarget resolves the session, the index name, the namespace, and a client
// bound to that index's data plane in one step.
func indexTarget(rc *plugin.RequestContext) (*Session, string, string, *restClient, error) {
	s, err := session(rc)
	if err != nil {
		return nil, "", "", nil, err
	}
	name, err := indexOf(rc)
	if err != nil {
		return nil, "", "", nil, err
	}
	ns, err := namespaceOf(rc, s)
	if err != nil {
		return nil, "", "", nil, err
	}
	client, err := s.indexClient(rc.Ctx, name)
	if err != nil {
		return nil, "", "", nil, err
	}
	return s, name, ns, client, nil
}
