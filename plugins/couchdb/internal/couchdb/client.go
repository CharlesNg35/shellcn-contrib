package couchdb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/searchrest"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const maxResponseBytes = 32 << 20

// client speaks the CouchDB HTTP API. Every byte leaves through the gateway
// transport: an injected RoundTripper when the connection is an L7 tunnel,
// otherwise a stdlib transport whose DialContext is the gateway's.
type client struct {
	http *http.Client
	// feed shares the gateway transport but carries no client timeout: a
	// continuous _changes feed is long-lived and http.Client.Timeout also bounds
	// reading the body, which would cut every tail at opts.timeout.
	feed *http.Client
	base string
	mode string // "none", "basic" or "session"
	user string
	pass string

	mu      sync.Mutex
	cookie  string
	cookies time.Time
}

func newClient(net plugin.NetTransport, opts options) (*client, error) {
	if net == nil {
		return nil, fmt.Errorf("%w: network transport is unavailable", plugin.ErrUnavailable)
	}
	base := opts.baseURL()
	rt, ok := gatewayRoundTripper(net, opts, &base)
	if !ok {
		return nil, fmt.Errorf("%w: gateway transport offers no usable egress", plugin.ErrUnavailable)
	}
	return &client{
		http: &http.Client{Transport: rt, Timeout: opts.timeout},
		feed: &http.Client{Transport: rt},
		base: strings.TrimRight(base, "/"),
		mode: opts.authMode,
		user: opts.username,
		pass: opts.password,
	}, nil
}

// gatewayRoundTripper prefers the gateway's L7 transport (agent http_proxy mode)
// and falls back to a dialer-backed transport for L4 tunnels and direct
// connections. The plugin never opens a socket itself in either case.
func gatewayRoundTripper(net plugin.NetTransport, opts options, base *string) (http.RoundTripper, bool) {
	if upstream, rt, ok := net.HTTP(); ok && rt != nil {
		if strings.TrimSpace(upstream) != "" {
			*base = upstream
		}
		return rt, true
	}
	return &http.Transport{
		DialContext:           net.DialContext,
		TLSClientConfig:       opts.tls,
		MaxIdleConns:          16,
		MaxIdleConnsPerHost:   8,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: opts.timeout,
	}, true
}

func (c *client) close() { c.http.CloseIdleConnections() }

// do runs a JSON request and decodes the response into out (which may be nil).
func (c *client) do(ctx context.Context, method, path string, query url.Values, body, out any) error {
	var payload []byte
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("%w: %v", plugin.ErrInvalidInput, err)
		}
		payload = encoded
	}
	data, _, err := c.raw(ctx, method, path, query, payload)
	if err != nil {
		return err
	}
	if out == nil || len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("%w: CouchDB returned a malformed response: %v", plugin.ErrUnavailable, err)
	}
	return nil
}

// raw performs one request and returns the body plus response headers. A 401 on
// a session-authenticated client triggers exactly one re-login and retry.
func (c *client) raw(ctx context.Context, method, path string, query url.Values, body []byte) ([]byte, http.Header, error) {
	resp, err := c.send(ctx, c.http, method, path, query, body)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode == http.StatusUnauthorized && c.mode == "session" {
		drain(resp)
		if err := c.login(ctx, true); err != nil {
			return nil, nil, err
		}
		if resp, err = c.send(ctx, c.http, method, path, query, body); err != nil {
			return nil, nil, err
		}
	}
	defer drain(resp)
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return nil, nil, fmt.Errorf("%w: reading CouchDB response: %v", plugin.ErrUnavailable, err)
	}
	if resp.StatusCode >= 400 {
		return nil, resp.Header, httpError(resp.StatusCode, data)
	}
	return data, resp.Header, nil
}

// stream issues a request whose body is consumed incrementally (the continuous
// _changes feed). The caller owns closing the returned reader.
func (c *client) stream(ctx context.Context, method, path string, query url.Values, body []byte) (io.ReadCloser, error) {
	resp, err := c.send(ctx, c.feed, method, path, query, body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusUnauthorized && c.mode == "session" {
		drain(resp)
		if err := c.login(ctx, true); err != nil {
			return nil, err
		}
		if resp, err = c.send(ctx, c.feed, method, path, query, body); err != nil {
			return nil, err
		}
	}
	if resp.StatusCode >= 400 {
		defer drain(resp)
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return nil, httpError(resp.StatusCode, data)
	}
	return resp.Body, nil
}

func (c *client) send(ctx context.Context, hc *http.Client, method, path string, query url.Values, body []byte) (*http.Response, error) {
	endpoint := c.base + path
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	var reader io.Reader
	if len(body) > 0 {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", plugin.ErrInvalidInput, err)
	}
	req.Header.Set("Accept", "application/json")
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	if err := c.authorize(ctx, req); err != nil {
		return nil, err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	return resp, nil
}

func (c *client) authorize(ctx context.Context, req *http.Request) error {
	switch c.mode {
	case "basic":
		req.SetBasicAuth(c.user, c.pass)
	case "session":
		if err := c.login(ctx, false); err != nil {
			return err
		}
		c.mu.Lock()
		cookie := c.cookie
		c.mu.Unlock()
		if cookie != "" {
			req.Header.Set("Cookie", cookie)
		}
	}
	return nil
}

// login exchanges the username/password for a CouchDB AuthSession cookie so the
// password is not replayed on every request. force re-authenticates after a 401.
func (c *client) login(ctx context.Context, force bool) error {
	c.mu.Lock()
	fresh := c.cookie != "" && time.Since(c.cookies) < 5*time.Minute
	c.mu.Unlock()
	if fresh && !force {
		return nil
	}

	payload, err := json.Marshal(map[string]string{"name": c.user, "password": c.pass})
	if err != nil {
		return fmt.Errorf("%w: %v", plugin.ErrInvalidInput, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/_session", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("%w: %v", plugin.ErrInvalidInput, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	defer drain(resp)
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return httpError(resp.StatusCode, data)
	}
	for _, cookie := range resp.Cookies() {
		if cookie.Name == "AuthSession" && cookie.Value != "" {
			c.mu.Lock()
			c.cookie = cookie.Name + "=" + cookie.Value
			c.cookies = time.Now()
			c.mu.Unlock()
			return nil
		}
	}
	return fmt.Errorf("%w: CouchDB accepted the session request without issuing a cookie", plugin.ErrUnauthorized)
}

func drain(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	_ = resp.Body.Close()
}

// httpError turns a CouchDB {"error","reason"} body into an SDK sentinel. Bodies
// that are not CouchDB-shaped fall back to the shared REST mapping.
func httpError(status int, data []byte) error {
	var body struct {
		Error  string `json:"error"`
		Reason string `json:"reason"`
	}
	if json.Unmarshal(data, &body) != nil || body.Error == "" {
		return searchrest.HTTPError(status, data)
	}
	message := body.Error
	if body.Reason != "" {
		message += ": " + body.Reason
	}
	switch status {
	case http.StatusNotFound:
		return fmt.Errorf("%w: %s", plugin.ErrNotFound, message)
	case http.StatusConflict, http.StatusPreconditionFailed:
		return fmt.Errorf("%w: %s", plugin.ErrConflict, message)
	case http.StatusUnauthorized:
		return fmt.Errorf("%w: %s", plugin.ErrUnauthorized, message)
	case http.StatusForbidden:
		return fmt.Errorf("%w: %s", plugin.ErrForbidden, message)
	case http.StatusBadRequest, http.StatusUnprocessableEntity, http.StatusNotAcceptable:
		return fmt.Errorf("%w: %s", plugin.ErrInvalidInput, message)
	case http.StatusNotImplemented:
		return fmt.Errorf("%w: %s", plugin.ErrNotSupported, message)
	default:
		return fmt.Errorf("%w: CouchDB returned %d: %s", plugin.ErrUnavailable, status, message)
	}
}

// dbPath escapes a database name for use as a URL path segment. CouchDB allows
// "/" inside database names, and it must travel encoded.
func dbPath(db string) string { return "/" + url.PathEscape(db) }

// docPath keeps the "_design/" and "_local/" prefixes literal while escaping the
// document name itself, which is what CouchDB expects.
func docPath(db, docID string) string {
	for _, prefix := range []string{"_design/", "_local/"} {
		if rest, ok := strings.CutPrefix(docID, prefix); ok {
			return dbPath(db) + "/" + prefix + url.PathEscape(rest)
		}
	}
	return dbPath(db) + "/" + url.PathEscape(docID)
}
