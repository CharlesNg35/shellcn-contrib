package cloudflare

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type cfClient struct {
	base  string
	token string
	http  *http.Client
}

type cfEnvelope[T any] struct {
	Success bool        `json:"success"`
	Result  T           `json:"result"`
	Errors  []cfMessage `json:"errors"`
}

type cfListEnvelope[T any] struct {
	Success    bool         `json:"success"`
	Result     []T          `json:"result"`
	Errors     []cfMessage  `json:"errors"`
	ResultInfo cfResultInfo `json:"result_info"`
}

type cfMessage struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type cfResultInfo struct {
	Page       int `json:"page"`
	PerPage    int `json:"per_page"`
	Count      int `json:"count"`
	TotalCount int `json:"total_count"`
	TotalPages int `json:"total_pages"`
}

func newCFClient(opts Options, dialer func(context.Context, string, string) (net.Conn, error)) *cfClient {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if dialer != nil {
		transport.DialContext = dialer
	}
	return &cfClient{
		base:  opts.Endpoint,
		token: opts.Token,
		http:  &http.Client{Timeout: opts.Timeout, Transport: transport},
	}
}

func (c *cfClient) do(ctx context.Context, method, path string, body any, out any) error {
	return c.doWithHeaders(ctx, method, path, nil, body, out)
}

func (c *cfClient) doWithHeaders(ctx context.Context, method, path string, headers map[string]string, body any, out any) error {
	req, err := c.newRequest(ctx, method, path, headers, body)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%w: Cloudflare request failed: %v", plugin.ErrUnavailable, err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return cfHTTPError(resp.StatusCode, data)
	}
	if out == nil || len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("%w: decode Cloudflare response: %v", plugin.ErrUnavailable, err)
	}
	return nil
}

func (c *cfClient) list(ctx context.Context, path string, q url.Values, limit int, out any) (cfResultInfo, error) {
	if q == nil {
		q = url.Values{}
	}
	if q.Get("per_page") == "" {
		q.Set("per_page", strconv.Itoa(limit))
	}
	if q.Get("page") == "" {
		q.Set("page", "1")
	}
	fullPath := path
	if encoded := q.Encode(); encoded != "" {
		fullPath += "?" + encoded
	}
	req, err := c.newRequest(ctx, http.MethodGet, fullPath, nil, nil)
	if err != nil {
		return cfResultInfo{}, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return cfResultInfo{}, fmt.Errorf("%w: Cloudflare request failed: %v", plugin.ErrUnavailable, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return cfResultInfo{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return cfResultInfo{}, cfHTTPError(resp.StatusCode, data)
	}
	if err := json.Unmarshal(data, out); err != nil {
		return cfResultInfo{}, fmt.Errorf("%w: decode Cloudflare response: %v", plugin.ErrUnavailable, err)
	}
	var probe struct {
		ResultInfo cfResultInfo `json:"result_info"`
	}
	_ = json.Unmarshal(data, &probe)
	return probe.ResultInfo, nil
}

func (c *cfClient) newRequest(ctx context.Context, method, path string, headers map[string]string, body any) (*http.Request, error) {
	rawURL := c.base + "/" + strings.TrimPrefix(path, "/")
	var rdr io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rdr = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, rawURL, rdr)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for key, value := range headers {
		name, err := safeExplorerHeader(key)
		if err != nil {
			return nil, err
		}
		req.Header.Set(name, value)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	return req, nil
}

func safeExplorerHeader(name string) (string, error) {
	name = http.CanonicalHeaderKey(strings.TrimSpace(name))
	switch strings.ToLower(name) {
	case "", "authorization", "cookie", "set-cookie", "host", "content-length", "connection", "proxy-authorization", "proxy-authenticate", "te", "trailer", "transfer-encoding", "upgrade":
		return "", fmt.Errorf("%w: header %q cannot be set from the API explorer", plugin.ErrInvalidInput, name)
	default:
		return name, nil
	}
}

func cfHTTPError(status int, data []byte) error {
	var env struct {
		Errors []cfMessage `json:"errors"`
	}
	_ = json.Unmarshal(data, &env)
	msg := http.StatusText(status)
	if len(env.Errors) > 0 && strings.TrimSpace(env.Errors[0].Message) != "" {
		msg = env.Errors[0].Message
	}
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return fmt.Errorf("%w: Cloudflare API denied request: %s", plugin.ErrForbidden, msg)
	case http.StatusNotFound:
		return fmt.Errorf("%w: Cloudflare resource not found: %s", plugin.ErrNotFound, msg)
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		return fmt.Errorf("%w: Cloudflare API rejected request: %s", plugin.ErrInvalidInput, msg)
	default:
		return fmt.Errorf("%w: Cloudflare API returned %d: %s", plugin.ErrUnavailable, status, msg)
	}
}

func envelopeOK[T any](env cfEnvelope[T]) (T, error) {
	if env.Success {
		return env.Result, nil
	}
	var zero T
	return zero, cfEnvelopeError(env.Errors)
}

func listOK[T any](env cfListEnvelope[T]) ([]T, error) {
	if env.Success {
		return env.Result, nil
	}
	return nil, cfEnvelopeError(env.Errors)
}

func cfEnvelopeError(messages []cfMessage) error {
	if len(messages) == 0 {
		return fmt.Errorf("%w: Cloudflare API request failed", plugin.ErrInvalidInput)
	}
	return fmt.Errorf("%w: Cloudflare API request failed: %s", plugin.ErrInvalidInput, messages[0].Message)
}
