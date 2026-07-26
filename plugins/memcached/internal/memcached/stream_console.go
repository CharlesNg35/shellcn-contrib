package memcached

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type consoleRequest struct {
	Query   string `json:"query"`
	Confirm bool   `json:"confirm,omitempty"`
}

type consoleResult struct {
	Columns   []string `json:"columns,omitempty"`
	Rows      [][]any  `json:"rows,omitempty"`
	RowCount  int      `json:"rowCount"`
	ElapsedMs int64    `json:"elapsedMs"`
	Statement string   `json:"statement"`
	Message   string   `json:"message,omitempty"`
	Truncated bool     `json:"truncated,omitempty"`
}

// consoleStream is the memcached command console. Each submission is resolved
// against the command allow-list, re-checked against read-only mode, and audited
// individually; the socket stays open across executions.
func consoleStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := session(rc)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(client)
	enc := json.NewEncoder(client)

	for {
		var req consoleRequest
		if err := dec.Decode(&req); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) || client.Context().Err() != nil {
				return nil
			}
			return err
		}
		if err := runConsoleCommand(rc, s, enc, req); err != nil {
			return err
		}
		if client.Context().Err() != nil {
			return nil
		}
	}
}

func runConsoleCommand(rc *plugin.RequestContext, s *Session, enc *json.Encoder, req consoleRequest) error {
	text := strings.TrimSpace(req.Query)
	if text == "" {
		return enc.Encode(map[string]any{"error": "a command is required"})
	}
	spec, payload, err := resolveCommand(req.Query)
	if err != nil {
		return enc.Encode(map[string]any{"error": err.Error()})
	}
	params := map[string]string{
		"command":   spec.Name,
		"bytes":     strconv.Itoa(len(payload)),
		"confirmed": strconv.FormatBool(req.Confirm),
	}
	if spec.Class != classRead {
		if err := s.ensureWritable(); err != nil {
			rc.Audit(plugin.AuditDenied, params, err)
			return enc.Encode(map[string]any{"error": err.Error()})
		}
	}
	if spec.Class == classAdmin && !req.Confirm {
		return enc.Encode(map[string]any{
			"confirmRequired": true,
			"confirmMessage":  "\"" + spec.Name + "\" changes server-wide state on this memcached instance. Run it anyway?",
		})
	}

	started := time.Now()
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	var out commandResult
	err = s.withAdminConn(ctx, func(c *textConn) (bool, error) {
		result, err := c.execute(ctx, spec, payload, s.opts.Timeout)
		if err != nil {
			return false, err
		}
		out = result
		return out.Truncated, nil
	})
	cancel()
	if err != nil {
		rc.Audit(plugin.AuditError, params, err)
		return enc.Encode(map[string]any{"error": err.Error()})
	}
	rc.Audit(plugin.AuditAllowed, params, nil)
	// A console "set"/"delete"/"flush_all" changes the keyspace just like the key
	// routes do, so it has to drop the cached crawl the key browser pages from.
	if spec.Class != classRead {
		s.dropKeySnapshot()
	}
	return enc.Encode(renderConsoleResult(spec, out, time.Since(started)))
}

func renderConsoleResult(spec commandSpec, out commandResult, elapsed time.Duration) consoleResult {
	result := consoleResult{Statement: spec.Name, ElapsedMs: elapsed.Milliseconds()}
	switch {
	case len(out.Stats) > 0:
		result.Columns = []string{"stat", "value"}
		for _, pair := range out.Stats {
			result.Rows = append(result.Rows, []any{pair.Name, pair.Value})
		}
	case len(out.Values) > 0:
		result.Columns = []string{"key", "flags", "bytes", "cas", "value"}
		for _, value := range out.Values {
			result.Rows = append(result.Rows, []any{value.Key, value.Flags, value.Bytes, value.CAS, previewValue(value.Value)})
		}
	case len(out.Lines) > 1:
		result.Columns = []string{"line"}
		for _, line := range out.Lines {
			result.Rows = append(result.Rows, []any{line})
		}
	case len(out.Lines) == 1:
		result.Message = out.Lines[0]
	default:
		result.Message = "OK"
	}
	if len(out.Stats) > 0 && len(out.Lines) > 0 {
		result.Message = strings.Join(out.Lines, " ")
	}
	result.RowCount = len(result.Rows)
	if out.Truncated {
		result.Truncated = true
		result.Message = strings.TrimSpace(result.Message + " (stopped after " + strconv.Itoa(maxConsoleReplyRows) + " lines)")
	}
	return result
}

// previewValue keeps binary payloads out of the JSON result frame.
func previewValue(raw []byte) string {
	const limit = 4096
	if !utf8.Valid(raw) {
		return "<" + strconv.Itoa(len(raw)) + " binary bytes>"
	}
	if len(raw) > limit {
		return string(raw[:limit]) + "…"
	}
	return string(raw)
}
