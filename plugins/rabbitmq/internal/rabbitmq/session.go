package rabbitmq

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

type Session struct {
	client *http.Client
	opts   options
}

func connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	opts, err := parseOptions(cfg)
	if err != nil {
		return nil, err
	}
	s := &Session{client: httpClient(cfg, opts), opts: opts}
	return s, s.HealthCheck(ctx)
}

func (s *Session) HealthCheck(ctx context.Context) error {
	req, err := s.newRequest(ctx, http.MethodGet, "/api/overview", nil)
	if err != nil {
		return err
	}
	var out map[string]any
	return s.do(req, &out)
}

func (s *Session) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}

func (s *Session) Close() error {
	s.client.CloseIdleConnections()
	return nil
}

func (s *Session) newRequest(ctx context.Context, method, apiPath string, body any) (*http.Request, error) {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, s.opts.ManagementURL+apiPath, reader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json, */*")
	if s.opts.Username != "" || s.opts.Password != "" {
		req.SetBasicAuth(s.opts.Username, s.opts.Password)
	}
	return req, nil
}

func (s *Session) do(req *http.Request, out any) error {
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		return rabbitHTTPError(resp.StatusCode, data)
	}
	if out == nil || len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return err
	}
	return nil
}

func apiVHost(vhost string) string {
	return url.PathEscape(vhost)
}

func apiName(name string) string {
	return url.PathEscape(name)
}

func rabbitHTTPError(status int, data []byte) error {
	var body map[string]any
	_ = json.Unmarshal(data, &body)
	message := strings.TrimSpace(fmt.Sprint(body["reason"]))
	if message == "" {
		message = strings.TrimSpace(fmt.Sprint(body["error"]))
	}
	if message == "" {
		message = strings.TrimSpace(string(data))
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
	default:
		return fmt.Errorf("%w: RabbitMQ API returned %d: %s", plugin.ErrUnavailable, status, message)
	}
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
