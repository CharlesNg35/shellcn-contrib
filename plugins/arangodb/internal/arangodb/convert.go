package arangodb

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/arangodb/go-driver/v2/arangodb/shared"

	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	// ArangoDB's extended naming constraints accept most UTF-8 characters and
	// reject only control characters, "/" and ":". "%", "?", "#" and "+" are
	// rejected on top of that because the driver builds a request path by
	// unescaping and reparsing the URL, which turns them into a different path,
	// a query string or a fragment.
	nameCharClass = "[^\\x00-\\x1f\\x7f/:%?#+]"

	databasePattern = "^" + nameCharClass + "{1,128}$"
	namePattern     = "^" + nameCharClass + "{1,256}$"

	maxDatabaseNameBytes = 128
	maxNameBytes         = 256
	maxKeyLength         = 254
)

var (
	databaseRE = regexp.MustCompile(databasePattern)
	nameRE     = regexp.MustCompile(namePattern)
	// AQL document keys accept a documented punctuation set; everything else is
	// rejected before the key reaches a bound parameter or a URL path.
	keyRE = regexp.MustCompile(`^[A-Za-z0-9_\-:.@()+,=;$!*'%]{1,254}$`)
)

func safeDatabaseName(raw string) (string, error) {
	return safeIdentifier(raw, "database", databaseRE, maxDatabaseNameBytes)
}

func safeName(raw, label string) (string, error) {
	return safeIdentifier(raw, label, nameRE, maxNameBytes)
}

// safeIdentifier applies ArangoDB's extended naming constraints. The wide
// character set is safe because names only ever reach the server as escaped URL
// path segments or as AQL bind parameters, never interpolated into AQL text.
func safeIdentifier(raw, label string, re *regexp.Regexp, maxBytes int) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("%w: %s is required", plugin.ErrInvalidInput, label)
	}
	// Go's regexp reads invalid UTF-8 bytes as U+FFFD, which a negated class
	// matches, so the encoding is checked separately.
	if len(raw) > maxBytes || !utf8.ValidString(raw) || !re.MatchString(raw) {
		return "", fmt.Errorf("%w: invalid %s name %q", plugin.ErrInvalidInput, label, raw)
	}
	return raw, nil
}

// safeAnalyzerName also accepts the database-qualified `db::analyzer` spelling
// the server returns for analyzers defined outside the current database.
func safeAnalyzerName(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if prefix, rest, ok := strings.Cut(raw, "::"); ok {
		if _, err := safeDatabaseName(prefix); err != nil {
			return "", err
		}
		if _, err := safeName(rest, "analyzer"); err != nil {
			return "", err
		}
		return raw, nil
	}
	return safeName(raw, "analyzer")
}

func safeKey(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("%w: document key is required", plugin.ErrInvalidInput)
	}
	if len(raw) > maxKeyLength || !keyRE.MatchString(raw) {
		return "", fmt.Errorf("%w: invalid document key %q", plugin.ErrInvalidInput, raw)
	}
	return raw, nil
}

// safeMount validates a Foxx mount path (`/my-service`), which is the only
// identifier in this plugin that legitimately contains slashes.
func safeMount(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if !strings.HasPrefix(raw, "/") || strings.Contains(raw, "..") {
		return "", fmt.Errorf("%w: mount must be an absolute path such as /my-service", plugin.ErrInvalidInput)
	}
	for _, segment := range strings.Split(strings.TrimPrefix(raw, "/"), "/") {
		if segment == "" {
			return "", fmt.Errorf("%w: mount has an empty path segment", plugin.ErrInvalidInput)
		}
		for _, r := range segment {
			if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' && r != '-' && r != '.' {
				return "", fmt.Errorf("%w: mount contains unsupported characters", plugin.ErrInvalidInput)
			}
		}
	}
	return raw, nil
}

// quote wraps an already-validated identifier in AQL ticks. A backtick has no
// escape inside a backtick-quoted name, so such names use AQL's alternative
// forward-tick quoting. Values never take this path; they are always passed as
// bind parameters.
func quote(name string) string {
	if strings.ContainsRune(name, '`') {
		return "´" + name + "´"
	}
	return "`" + name + "`"
}

// aqlString renders a name as an AQL string literal, which follows JSON
// escaping rules.
func aqlString(value string) string {
	data, err := json.Marshal(value)
	if err != nil {
		return `""`
	}
	return string(data)
}

func encodeID(parts ...string) string {
	raw, err := json.Marshal(parts)
	if err != nil {
		return strings.Join(parts, "/")
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeID(encoded string, want int) ([]string, error) {
	data, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(encoded))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid resource id", plugin.ErrInvalidInput)
	}
	var parts []string
	if err := json.Unmarshal(data, &parts); err != nil || len(parts) != want {
		return nil, fmt.Errorf("%w: invalid resource id", plugin.ErrInvalidInput)
	}
	return parts, nil
}

func arangoErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, plugin.ErrInvalidInput) || errors.Is(err, plugin.ErrNotFound) ||
		errors.Is(err, plugin.ErrForbidden) || errors.Is(err, plugin.ErrUnauthorized) {
		return err
	}
	var arangoError shared.ArangoError
	if errors.As(err, &arangoError) {
		message := arangoError.ErrorMessage
		if message == "" {
			message = arangoError.Error()
		}
		switch {
		case arangoError.Code == 401:
			return fmt.Errorf("%w: %s", plugin.ErrUnauthorized, message)
		case arangoError.Code == 403:
			return fmt.Errorf("%w: %s", plugin.ErrForbidden, message)
		case arangoError.Code == 404:
			return fmt.Errorf("%w: %s", plugin.ErrNotFound, message)
		case arangoError.Code == 409 || arangoError.Code == 412:
			return fmt.Errorf("%w: %s", plugin.ErrConflict, message)
		case arangoError.Code == 400 || arangoError.Code == 422:
			return fmt.Errorf("%w: %s", plugin.ErrInvalidInput, message)
		case arangoError.Code == 501:
			return fmt.Errorf("%w: %s", plugin.ErrNotSupported, message)
		default:
			return fmt.Errorf("%w: %s", plugin.ErrUnavailable, message)
		}
	}
	if shared.IsNoMoreDocuments(err) {
		return fmt.Errorf("%w: no more documents", plugin.ErrNotFound)
	}
	return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
}

// normalize converts driver values into JSON-friendly plugin payloads and masks
// attributes matching the connection's redaction patterns.
func normalize(value any, patterns []string) any {
	switch v := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			if sqldb.RedactColumn(key, patterns) {
				out[key] = sqldb.RedactedValue
				continue
			}
			out[key] = normalize(item, patterns)
		}
		return out
	case []any:
		out := make([]any, 0, len(v))
		for _, item := range v {
			out = append(out, normalize(item, patterns))
		}
		return out
	case time.Time:
		return v.Format(time.RFC3339Nano)
	case json.RawMessage:
		var decoded any
		if err := json.Unmarshal(v, &decoded); err == nil {
			return normalize(decoded, patterns)
		}
		return string(v)
	default:
		return value
	}
}

func asMap(value any) map[string]any {
	switch v := value.(type) {
	case map[string]any:
		return v
	case nil:
		return map[string]any{}
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return map[string]any{}
		}
		out := map[string]any{}
		if err := json.Unmarshal(data, &out); err != nil {
			return map[string]any{}
		}
		return out
	}
}

// structRow round-trips a driver struct through JSON so table and object-detail
// payloads use the server's own field names.
func structRow(value any) row {
	data, err := json.Marshal(value)
	if err != nil {
		return row{}
	}
	out := row{}
	if err := json.Unmarshal(data, &out); err != nil {
		return row{}
	}
	return out
}

func compact(value any) string {
	if value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return v
	case []string:
		return strings.Join(v, ", ")
	}
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprint(value)
	}
	return string(data)
}

func joinStrings(values []string) string { return strings.Join(values, ", ") }

func editorContent(value any) (any, error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	return map[string]any{"content": string(data)}, nil
}

// parseEditorBody reads the uniform editor payload: the generic code editor
// posts {"content":"<json>"}; a typed object body is accepted too so routes can
// be driven directly.
func parseEditorBody(rc *plugin.RequestContext, key string) (map[string]any, error) {
	var body struct {
		Content string         `json:"content"`
		Value   map[string]any `json:"-"`
	}
	raw := rc.Body()
	if len(raw) > 0 {
		var envelope map[string]json.RawMessage
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return nil, fmt.Errorf("%w: body must be a JSON object", plugin.ErrInvalidInput)
		}
		if value, ok := envelope[key]; ok {
			out := map[string]any{}
			if err := json.Unmarshal(value, &out); err != nil {
				return nil, fmt.Errorf("%w: %s must be a JSON object", plugin.ErrInvalidInput, key)
			}
			return out, nil
		}
		if value, ok := envelope["content"]; ok {
			if err := json.Unmarshal(value, &body.Content); err != nil {
				return nil, fmt.Errorf("%w: content must be a JSON string", plugin.ErrInvalidInput)
			}
			return parseJSONObject(body.Content)
		}
	}
	return nil, fmt.Errorf("%w: %s or content is required", plugin.ErrInvalidInput, key)
}

func parseJSONObject(text string) (map[string]any, error) {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return map[string]any{}, nil
	}
	out := map[string]any{}
	if err := json.Unmarshal([]byte(trimmed), &out); err != nil {
		return nil, fmt.Errorf("%w: expected a JSON object: %v", plugin.ErrInvalidInput, err)
	}
	return out, nil
}

func cursorOffset(cursor string) (int, error) {
	if cursor == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(cursor)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%w: invalid cursor", plugin.ErrInvalidInput)
	}
	return n, nil
}

func nextCursor(offset, got, limit int) string {
	if got < limit {
		return ""
	}
	return strconv.Itoa(offset + got)
}

func sortedKeys[T any](in map[string]T) []string {
	keys := make([]string, 0, len(in))
	for key := range in {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func boolPtr(v bool) *bool { return &v }

func intOrZero(v *int) int {
	if v == nil {
		return 0
	}
	return *v
}

func stringOrEmpty(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func boolOrFalse(v *bool) bool {
	return v != nil && *v
}
