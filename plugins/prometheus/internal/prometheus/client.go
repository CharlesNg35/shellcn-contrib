package prometheus

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type client struct {
	http     *http.Client
	endpoint string
	auth     authHeader
}

type apiEnvelope struct {
	Status    string          `json:"status"`
	Data      json.RawMessage `json:"data"`
	ErrorType string          `json:"errorType"`
	Error     string          `json:"error"`
	Warnings  []string        `json:"warnings"`
	Infos     []string        `json:"infos"`
}

func (c *client) api(ctx context.Context, method, path string, query url.Values, body any, out any) error {
	data, err := c.raw(ctx, method, path, query, "application/json", body)
	if err != nil {
		return err
	}
	var env apiEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return fmt.Errorf("%w: invalid Prometheus response: %v", plugin.ErrUnavailable, err)
	}
	if env.Status == "error" {
		msg := strings.TrimSpace(env.Error)
		if msg == "" {
			msg = env.ErrorType
		}
		return fmt.Errorf("%w: %s", plugin.ErrInvalidInput, msg)
	}
	if out == nil {
		return nil
	}
	if len(env.Data) == 0 || string(env.Data) == "null" {
		return nil
	}
	return json.Unmarshal(env.Data, out)
}

func (c *client) raw(ctx context.Context, method, path string, query url.Values, contentType string, body any) ([]byte, error) {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(data)
	}
	endpoint := c.endpoint + path
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	if body != nil && contentType != "" {
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
		return nil, httpError(resp.StatusCode, data)
	}
	return data, nil
}

func httpError(status int, data []byte) error {
	message := strings.TrimSpace(string(data))
	var env apiEnvelope
	if json.Unmarshal(data, &env) == nil {
		if strings.TrimSpace(env.Error) != "" {
			message = env.Error
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
		return fmt.Errorf("%w: Prometheus returned %d: %s", plugin.ErrUnavailable, status, message)
	}
}
