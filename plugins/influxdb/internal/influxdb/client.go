package influxdb

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/csv"
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

type clientOptions struct {
	Endpoint  string
	Auth      authHeader
	TLSConfig *tls.Config
	Timeout   time.Duration
	Dialer    func(context.Context, string, string) (net.Conn, error)
}

type client struct {
	http     *http.Client
	endpoint string
	auth     authHeader
}

func newClient(opts clientOptions) *client {
	transport := &http.Transport{}
	transport.TLSClientConfig = opts.TLSConfig
	if opts.Dialer != nil {
		transport.DialContext = opts.Dialer
	}
	return &client{
		http:     &http.Client{Transport: transport, Timeout: opts.Timeout},
		endpoint: strings.TrimRight(opts.Endpoint, "/"),
		auth:     opts.Auth,
	}
}

func (c *client) close() {
	c.http.CloseIdleConnections()
}

func (c *client) health(ctx context.Context, mode string) error {
	switch mode {
	case modeV1:
		_, err := c.raw(ctx, http.MethodGet, "/ping", nil, "", nil, "application/json")
		return err
	default:
		_, err := c.raw(ctx, http.MethodGet, "/health", nil, "", nil, "application/json")
		return err
	}
}

func (c *client) json(ctx context.Context, method, path string, query url.Values, body any, out any) error {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	resp, err := c.raw(ctx, method, path, query, "application/json", data, "application/json")
	if err != nil {
		return err
	}
	if out == nil || len(resp) == 0 {
		return nil
	}
	return json.Unmarshal(resp, out)
}

func (c *client) text(ctx context.Context, method, path string, query url.Values, body []byte) ([]byte, error) {
	return c.raw(ctx, method, path, query, "text/plain; charset=utf-8", body, "text/plain")
}

func (c *client) csv(ctx context.Context, method, path string, query url.Values, body any) ([]row, error) {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return nil, err
		}
	}
	resp, err := c.raw(ctx, method, path, query, "application/json", data, "application/csv")
	if err != nil {
		return nil, err
	}
	return parseCSVRows(resp)
}

func (c *client) raw(ctx context.Context, method, path string, query url.Values, contentType string, body []byte, accept string) ([]byte, error) {
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
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
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
	data, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, httpError(resp.StatusCode, data)
	}
	return data, nil
}

func httpError(status int, data []byte) error {
	message := strings.TrimSpace(string(data))
	var body map[string]any
	if json.Unmarshal(data, &body) == nil {
		for _, key := range []string{"message", "error", "code"} {
			if text := strings.TrimSpace(fmt.Sprint(body[key])); text != "" && text != "<nil>" {
				message = text
				break
			}
		}
	}
	if message == "" {
		message = http.StatusText(status)
	}
	switch status {
	case http.StatusUnauthorized:
		return fmt.Errorf("%w: %s", plugin.ErrUnauthorized, message)
	case http.StatusForbidden:
		return fmt.Errorf("%w: %s", plugin.ErrForbidden, message)
	case http.StatusNotFound:
		return fmt.Errorf("%w: %s", plugin.ErrNotFound, message)
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		return fmt.Errorf("%w: %s", plugin.ErrInvalidInput, message)
	default:
		return fmt.Errorf("%w: InfluxDB returned %d: %s", plugin.ErrUnavailable, status, message)
	}
}

func parseCSVRows(data []byte) ([]row, error) {
	r := csv.NewReader(bytes.NewReader(data))
	r.FieldsPerRecord = -1
	var header []string
	out := []row{}
	for {
		rec, err := r.Read()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return nil, fmt.Errorf("%w: invalid CSV response: %v", plugin.ErrUnavailable, err)
		}
		if len(rec) == 0 || strings.HasPrefix(rec[0], "#") {
			continue
		}
		if header == nil {
			header = rec
			continue
		}
		item := row{}
		for i, key := range header {
			if key == "" || i >= len(rec) {
				continue
			}
			item[key] = rec[i]
		}
		out = append(out, item)
	}
}

func parseJSONLRows(data []byte) ([]row, error) {
	lines := bytes.Split(data, []byte{'\n'})
	out := []row{}
	for _, line := range lines {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var item row
		if err := json.Unmarshal(line, &item); err != nil {
			return nil, fmt.Errorf("%w: invalid JSONL response: %v", plugin.ErrUnavailable, err)
		}
		out = append(out, item)
	}
	return out, nil
}
