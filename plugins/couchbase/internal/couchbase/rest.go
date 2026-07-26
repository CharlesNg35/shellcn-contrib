package couchbase

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/charlesng35/shellcn-contrib/shared/searchrest"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

var (
	bucketNameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._%\-]{0,99}$`)
	scopeNameRE  = regexp.MustCompile(`^[A-Za-z0-9_%\-][A-Za-z0-9_%\-]{0,250}$`)
	indexNameRE  = regexp.MustCompile(`^[A-Za-z#][A-Za-z0-9_\-#]{0,199}$`)
)

// safeBucket, safeScope, and safeIndexName whitelist the identifier character
// sets Couchbase itself accepts. Identifiers cannot be bound as query
// parameters, so every one is validated here before it is quoted into SQL++ or
// placed in a REST path.
func safeBucket(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if !bucketNameRE.MatchString(raw) {
		return "", fmt.Errorf("%w: invalid bucket name %q", plugin.ErrInvalidInput, raw)
	}
	return raw, nil
}

func safeScope(raw, label string) (string, error) {
	raw = strings.TrimSpace(raw)
	if !scopeNameRE.MatchString(raw) {
		return "", fmt.Errorf("%w: invalid %s name %q", plugin.ErrInvalidInput, label, raw)
	}
	return raw, nil
}

func safeIndexName(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if !indexNameRE.MatchString(raw) {
		return "", fmt.Errorf("%w: invalid index name %q", plugin.ErrInvalidInput, raw)
	}
	return raw, nil
}

func quoteName(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// keyspace addresses a bucket, a bucket.scope, or a bucket.scope.collection.
type keyspace struct {
	Bucket     string
	Scope      string
	Collection string
	Document   string
}

func (k keyspace) uid() string {
	parts := []string{k.Bucket}
	if k.Scope != "" {
		parts = append(parts, k.Scope)
	}
	if k.Collection != "" {
		parts = append(parts, k.Collection)
	}
	if k.Document != "" {
		parts = append(parts, k.Document)
	}
	return base64.RawURLEncoding.EncodeToString([]byte(strings.Join(parts, "\x00")))
}

// path is the SQL++ keyspace expression, fully quoted.
func (k keyspace) path() string {
	out := quoteName(k.Bucket)
	if k.Scope != "" {
		out += "." + quoteName(k.Scope)
	}
	if k.Collection != "" {
		out += "." + quoteName(k.Collection)
	}
	return out
}

func (k keyspace) label() string {
	parts := []string{k.Bucket}
	if k.Scope != "" {
		parts = append(parts, k.Scope)
	}
	if k.Collection != "" {
		parts = append(parts, k.Collection)
	}
	return strings.Join(parts, ".")
}

func parseKeyspaceUID(uid string) (keyspace, error) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(uid))
	if err != nil {
		return keyspace{}, fmt.Errorf("%w: invalid keyspace reference", plugin.ErrInvalidInput)
	}
	parts := strings.Split(string(raw), "\x00")
	if len(parts) == 0 || len(parts) > 4 {
		return keyspace{}, fmt.Errorf("%w: invalid keyspace reference", plugin.ErrInvalidInput)
	}
	k := keyspace{Bucket: parts[0]}
	if len(parts) > 1 {
		k.Scope = parts[1]
	}
	if len(parts) > 2 {
		k.Collection = parts[2]
	}
	if len(parts) > 3 {
		k.Document = parts[3]
	}
	return validateKeyspace(k)
}

func validateKeyspace(k keyspace) (keyspace, error) {
	bucket, err := safeBucket(k.Bucket)
	if err != nil {
		return keyspace{}, err
	}
	out := keyspace{Bucket: bucket, Document: k.Document}
	if k.Scope != "" {
		scope, err := safeScope(k.Scope, "scope")
		if err != nil {
			return keyspace{}, err
		}
		out.Scope = scope
	}
	if k.Collection != "" {
		if out.Scope == "" {
			return keyspace{}, fmt.Errorf("%w: a collection requires a scope", plugin.ErrInvalidInput)
		}
		collection, err := safeScope(k.Collection, "collection")
		if err != nil {
			return keyspace{}, err
		}
		out.Collection = collection
	}
	return out, nil
}

// keyspaceFrom resolves the addressed keyspace from explicit route params, an
// encoded resource uid, or the workspace bucket scope filter, in that order.
func keyspaceFrom(rc *plugin.RequestContext, s *Session) (keyspace, error) {
	if uid := strings.TrimSpace(rc.Param("keyspace")); uid != "" {
		return parseKeyspaceUID(uid)
	}
	k := keyspace{
		Bucket:     strings.TrimSpace(rc.Param("bucket")),
		Scope:      strings.TrimSpace(rc.Param("scope")),
		Collection: strings.TrimSpace(rc.Param("collection")),
	}
	if k.Bucket == "" {
		k.Bucket = s.opts.Bucket
	}
	if k.Bucket == "" {
		return keyspace{}, fmt.Errorf("%w: a bucket is required; pick one in the bucket selector", plugin.ErrInvalidInput)
	}
	return validateKeyspace(k)
}

func documentKeyspace(rc *plugin.RequestContext) (keyspace, error) {
	k, err := parseKeyspaceUID(rc.Param("id"))
	if err != nil {
		return keyspace{}, err
	}
	if k.Collection == "" || k.Document == "" {
		return keyspace{}, fmt.Errorf("%w: a document reference is required", plugin.ErrInvalidInput)
	}
	return k, nil
}

// --- management API ---------------------------------------------------------

func (s *Session) mgmtGet(ctx context.Context, path string, out any) error {
	return s.mgmt.Do(ctx, "GET", path, nil, nil, out)
}

// mgmtForm posts an application/x-www-form-urlencoded body, the encoding the
// Couchbase management API expects for every mutation.
func (s *Session) mgmtForm(ctx context.Context, method, path string, form url.Values) error {
	var body []byte
	if len(form) > 0 {
		body = []byte(form.Encode())
	}
	_, err := s.mgmt.Raw(ctx, method, path, nil, "application/x-www-form-urlencoded", body)
	return err
}

// statsRange reads the cluster statistics API. It powers per-collection counters
// that no other endpoint exposes.
type statsSeries struct {
	Metric map[string]string `json:"metric"`
	Values [][]any           `json:"values"`
}

func (s *statsSeries) last() (float64, bool) {
	for i := len(s.Values) - 1; i >= 0; i-- {
		if len(s.Values[i]) < 2 {
			continue
		}
		switch v := s.Values[i][1].(type) {
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return f, true
			}
		case float64:
			return v, true
		}
	}
	return 0, false
}

func (s *Session) statsRange(ctx context.Context, metric, bucket string, seconds int) ([]statsSeries, error) {
	labels := []map[string]string{{"label": "name", "value": metric}}
	if bucket != "" {
		labels = append(labels, map[string]string{"label": "bucket", "value": bucket})
	}
	body := []map[string]any{{
		"metric":           labels,
		"step":             10,
		"start":            -seconds,
		"nodesAggregation": "sum",
	}}
	var out []struct {
		Data   []statsSeries `json:"data"`
		Errors []any         `json:"errors"`
	}
	if err := s.mgmt.Do(ctx, "POST", "/pools/default/stats/range", nil, body, &out); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out[0].Data, nil
}

// --- query service ----------------------------------------------------------

type n1qlError struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

type n1qlMetrics struct {
	ElapsedTime   string `json:"elapsedTime"`
	ExecutionTime string `json:"executionTime"`
	ResultCount   int64  `json:"resultCount"`
	ResultSize    int64  `json:"resultSize"`
	MutationCount int64  `json:"mutationCount"`
	SortCount     int64  `json:"sortCount"`
	ErrorCount    int64  `json:"errorCount"`
	WarningCount  int64  `json:"warningCount"`
}

type n1qlResponse struct {
	RequestID       string            `json:"requestID"`
	ClientContextID string            `json:"clientContextID"`
	Signature       json.RawMessage   `json:"signature"`
	Results         []json.RawMessage `json:"results"`
	Status          string            `json:"status"`
	Errors          []n1qlError       `json:"errors"`
	Warnings        []n1qlError       `json:"warnings"`
	Metrics         n1qlMetrics       `json:"metrics"`
}

// Query service error codes that map onto SDK sentinels. Everything else is an
// upstream failure.
var (
	n1qlForbiddenCodes = map[int]bool{13014: true, 13015: true, 10000: true}
	n1qlNotFoundCodes  = map[int]bool{12003: true, 12004: true, 12016: true, 12021: true, 12022: true}
	n1qlNotSupported   = map[int]bool{3230: true}
)

func (r *n1qlResponse) err() error {
	if len(r.Errors) == 0 {
		return nil
	}
	first := r.Errors[0]
	msg := strings.TrimSpace(first.Msg)
	if msg == "" {
		msg = "query failed with code " + strconv.Itoa(first.Code)
	}
	switch {
	case n1qlForbiddenCodes[first.Code]:
		return fmt.Errorf("%w: %s", plugin.ErrForbidden, msg)
	case n1qlNotFoundCodes[first.Code]:
		return fmt.Errorf("%w: %s", plugin.ErrNotFound, msg)
	case n1qlNotSupported[first.Code]:
		return fmt.Errorf("%w: %s", plugin.ErrNotSupported, msg)
	case first.Code >= 3000 && first.Code < 5000:
		return fmt.Errorf("%w: %s", plugin.ErrInvalidInput, msg)
	default:
		return fmt.Errorf("%w: %s", plugin.ErrUnavailable, msg)
	}
}

// exec runs one SQL++ statement with bound positional arguments and decodes the
// service envelope.
func (s *Session) exec(ctx context.Context, statement string, args ...any) (*n1qlResponse, error) {
	return s.execWithContextID(ctx, "", statement, args...)
}

func (s *Session) execWithContextID(ctx context.Context, contextID, statement string, args ...any) (*n1qlResponse, error) {
	body := map[string]any{
		"statement":        statement,
		"timeout":          s.opts.Timeout.String(),
		"scan_consistency": s.opts.ScanConsistency,
	}
	if len(args) > 0 {
		body["args"] = args
	}
	if contextID != "" {
		body["client_context_id"] = contextID
	}
	status, raw, err := s.queryPost(ctx, "/query/service", "application/json", mustJSON(body))
	if err != nil {
		return nil, err
	}
	var out n1qlResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, searchrest.HTTPError(status, raw)
	}
	if err := out.err(); err != nil {
		return nil, err
	}
	if status >= 400 {
		return nil, searchrest.HTTPError(status, raw)
	}
	return &out, nil
}

// queryPost returns the Query service response body for any status code; the
// service reports errors as a JSON envelope with its own error codes.
func (s *Session) queryPost(ctx context.Context, path, contentType string, body []byte) (int, []byte, error) {
	return s.queryRequest(ctx, http.MethodPost, path, contentType, body)
}

// queryAdmin calls the Query service's admin endpoints, used to list and cancel
// in-flight requests.
func (s *Session) queryAdmin(ctx context.Context, method, path string) (int, []byte, error) {
	return s.queryRequest(ctx, method, path, "", nil)
}

func (s *Session) queryRequest(ctx context.Context, method, path, contentType string, body []byte) (int, []byte, error) {
	var reader io.Reader
	if len(body) > 0 {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, s.queryURL+path, reader)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Accept", "application/json")
	if len(body) > 0 {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("Authorization", s.auth)
	resp, err := s.query.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, data, nil
}

// queryRows runs a statement and decodes each result into a generic row.
func (s *Session) queryRows(ctx context.Context, statement string, args ...any) ([]row, error) {
	res, err := s.exec(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(res.Results))
	for _, raw := range res.Results {
		var item any
		if err := json.Unmarshal(raw, &item); err != nil {
			return nil, fmt.Errorf("%w: decode query row: %v", plugin.ErrUnavailable, err)
		}
		switch v := item.(type) {
		case map[string]any:
			rows = append(rows, v)
		default:
			rows = append(rows, row{"value": v})
		}
	}
	return rows, nil
}

func mustJSON(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		// The values marshalled here are plugin-built maps of JSON-safe types.
		return []byte("{}")
	}
	return data
}

// --- generic value helpers --------------------------------------------------

func asMap(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return map[string]any{}
}

func mapValue(m map[string]any, key string) any {
	if m == nil {
		return nil
	}
	return m[key]
}

func numberValue(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case json.Number:
		f, _ := n.Float64()
		return f
	case string:
		f, err := strconv.ParseFloat(n, 64)
		if err != nil {
			return 0
		}
		return f
	default:
		return 0
	}
}

func stringValue(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(t)
	default:
		return fmt.Sprint(t)
	}
}

func stringSlice(v any) []string {
	switch t := v.(type) {
	case []string:
		return t
	case []any:
		out := make([]string, 0, len(t))
		for _, item := range t {
			out = append(out, stringValue(item))
		}
		return out
	case nil:
		return nil
	default:
		return []string{stringValue(v)}
	}
}

func percent(used, total float64) float64 {
	if total <= 0 {
		return 0
	}
	return round1(used / total * 100)
}

func round1(v float64) float64 {
	return float64(int64(v*10+0.5)) / 10
}

func sortedKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
