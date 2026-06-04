// Package searchrest contains HTTP transport helpers for REST-backed search plugins.
package searchrest

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type Auth struct {
	Header string
	Value  string
}

type Client struct {
	http     *http.Client
	endpoint string
	auth     Auth
}

type Options struct {
	Endpoint  string
	Auth      Auth
	TLSConfig *tls.Config
	Timeout   time.Duration
	Dialer    func(context.Context, string, string) (net.Conn, error)
}

func New(options Options) *Client {
	transport := &http.Transport{TLSClientConfig: options.TLSConfig}
	if options.Dialer != nil {
		transport.DialContext = options.Dialer
	}
	return &Client{
		http:     &http.Client{Transport: transport, Timeout: options.Timeout},
		endpoint: strings.TrimRight(options.Endpoint, "/"),
		auth:     options.Auth,
	}
}

func (c *Client) Close() {
	c.http.CloseIdleConnections()
}

func (c *Client) Do(ctx context.Context, method, path string, query url.Values, body any, out any) error {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	resp, err := c.Raw(ctx, method, path, query, "application/json", data)
	if err != nil {
		return err
	}
	if out == nil || len(resp) == 0 {
		return nil
	}
	return json.Unmarshal(resp, out)
}

func (c *Client) Raw(ctx context.Context, method, path string, query url.Values, contentType string, body []byte) ([]byte, error) {
	endpoint := c.endpoint + path
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	var reader io.Reader
	if len(body) > 0 {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	if len(body) > 0 && contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if c.auth.Header != "" && c.auth.Value != "" {
		req.Header.Set(c.auth.Header, c.auth.Value)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, HTTPError(resp.StatusCode, data)
	}
	return data, nil
}

func HTTPError(status int, data []byte) error {
	message := strings.TrimSpace(string(data))
	var body map[string]any
	if json.Unmarshal(data, &body) == nil {
		for _, key := range []string{"message", "error", "error_message"} {
			if text := strings.TrimSpace(fmt.Sprint(body[key])); text != "" && text != "<nil>" {
				message = text
				break
			}
		}
		if errObj, ok := body["error"].(map[string]any); ok {
			if msg := strings.TrimSpace(fmt.Sprint(errObj["msg"])); msg != "" && msg != "<nil>" {
				message = msg
			}
			if reason := strings.TrimSpace(fmt.Sprint(errObj["reason"])); reason != "" {
				message = reason
			}
			if typ := strings.TrimSpace(fmt.Sprint(errObj["type"])); typ != "" {
				message = typ + ": " + message
			}
		}
	}
	if message == "" {
		message = http.StatusText(status)
	}
	switch status {
	case http.StatusNotFound:
		return fmt.Errorf("%w: %s", plugin.ErrNotFound, message)
	case http.StatusConflict:
		return fmt.Errorf("%w: %s", plugin.ErrConflict, message)
	case http.StatusUnauthorized:
		return fmt.Errorf("%w: %s", plugin.ErrUnauthorized, message)
	case http.StatusForbidden:
		return fmt.Errorf("%w: %s", plugin.ErrForbidden, message)
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		return fmt.Errorf("%w: %s", plugin.ErrInvalidInput, message)
	default:
		return fmt.Errorf("%w: search API returned %d: %s", plugin.ErrUnavailable, status, message)
	}
}
