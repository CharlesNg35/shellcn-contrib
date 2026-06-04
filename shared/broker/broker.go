// Package broker contains small helpers shared by message-broker plugins.
package broker

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

func SplitAddresses(raw string, defaultPort int) ([]string, error) {
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == '\n' || r == '\r' || r == '\t'
	})
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		addr := strings.TrimSpace(part)
		if addr == "" {
			continue
		}
		if defaultPort > 0 && !hasPort(addr) {
			addr = net.JoinHostPort(addr, strconv.Itoa(defaultPort))
		}
		out = append(out, addr)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: at least one server is required", plugin.ErrInvalidInput)
	}
	return out, nil
}

func StringValue(cfg map[string]any, key, fallback string) string {
	if v, ok := cfg[key].(string); ok {
		if s := strings.TrimSpace(v); s != "" {
			return s
		}
	}
	return fallback
}

func IntValue(cfg map[string]any, key string, fallback, minValue, maxValue int) int {
	value := fallback
	switch v := cfg[key].(type) {
	case int:
		value = v
	case int64:
		value = int(v)
	case float64:
		value = int(v)
	}
	if minValue > 0 && value < minValue {
		value = minValue
	}
	if maxValue > 0 && value > maxValue {
		value = maxValue
	}
	return value
}

func BoolValue(cfg map[string]any, key string, fallback bool) bool {
	if v, ok := cfg[key].(bool); ok {
		return v
	}
	return fallback
}

func DurationValue(cfg map[string]any, key string, fallback time.Duration) time.Duration {
	switch v := cfg[key].(type) {
	case string:
		if d, err := time.ParseDuration(strings.TrimSpace(v)); err == nil && d > 0 {
			return d
		}
	case int:
		if v > 0 {
			return time.Duration(v) * time.Second
		}
	case float64:
		if v > 0 {
			return time.Duration(v) * time.Second
		}
	}
	return fallback
}

func PageRows[T ~map[string]any](rc *plugin.RequestContext, rows []T) (plugin.Page[T], error) {
	req, err := rc.Page()
	if err != nil {
		return plugin.Page[T]{}, err
	}
	rows = filterRows(rows, req.Search())
	rows = plugin.SortRows(rows, req.Sort)
	start := 0
	if req.Cursor != "" {
		i, err := strconv.Atoi(req.Cursor)
		if err != nil || i < 0 {
			return plugin.Page[T]{}, fmt.Errorf("%w: invalid cursor", plugin.ErrInvalidInput)
		}
		start = i
	}
	if start > len(rows) {
		start = len(rows)
	}
	end := start + req.Limit
	if end > len(rows) {
		end = len(rows)
	}
	next := ""
	if end < len(rows) {
		next = strconv.Itoa(end)
	}
	total := len(rows)
	return plugin.Page[T]{Items: rows[start:end], NextCursor: next, Total: &total}, nil
}

// filterRows backs the table's filter box, delegating to the shared grid filter
// so every plugin searches rows identically (every visible cell, case-insensitive).
func filterRows[T ~map[string]any](rows []T, q string) []T {
	return plugin.FilterRows(rows, q)
}

func hasPort(addr string) bool {
	if strings.HasPrefix(addr, "[") {
		_, _, err := net.SplitHostPort(addr)
		return err == nil
	}
	if strings.Count(addr, ":") > 1 {
		return true
	}
	host, port, err := net.SplitHostPort(addr)
	return err == nil && host != "" && port != ""
}
