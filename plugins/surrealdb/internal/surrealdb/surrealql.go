package surrealdb

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"github.com/charlesng35/shellcn/sdk/plugin"
	surrealdb "github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

// validIdent gates any name the plugin interpolates into a SurrealQL statement;
// everything else flows through bound $vars. SurrealDB identifiers are letters,
// digits, and underscores, not starting with a digit.
func validIdent(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
		case r >= '0' && r <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}

func requireIdent(s string) (string, error) {
	if !validIdent(s) {
		return "", fmt.Errorf("%w: invalid identifier %q", plugin.ErrInvalidInput, s)
	}
	return s, nil
}

// normalize converts SurrealDB's CBOR value types into plain JSON-friendly values
// so the gateway can encode rows for the browser.
func normalize(v any) any {
	switch x := v.(type) {
	case models.RecordID:
		return x.String()
	case *models.RecordID:
		return x.String()
	case models.CustomDateTime:
		return x.Format("2006-01-02T15:04:05.999999999Z07:00")
	case map[string]any:
		for k, e := range x {
			x[k] = normalize(e)
		}
		return x
	case map[any]any:
		m := make(map[string]any, len(x))
		for k, e := range x {
			m[fmt.Sprint(k)] = normalize(e)
		}
		return m
	case []any:
		for i, e := range x {
			x[i] = normalize(e)
		}
		return x
	default:
		return v
	}
}

// sess pulls the typed session off the request context.
func sess(rc *plugin.RequestContext) *session { return rc.Session.(*session) }

// queryOne runs a single-statement SurrealQL query and returns its .Result,
// surfacing a transport error as ErrUnavailable and a statement error as
// ErrInvalidInput.
func queryOne[T any](ctx context.Context, db *surrealdb.DB, sql string, vars map[string]any) (T, error) {
	var zero T
	res, err := surrealdb.Query[T](ctx, db, sql, vars)
	if err != nil {
		return zero, fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	if res == nil || len(*res) == 0 {
		return zero, nil
	}
	r := (*res)[0]
	if strings.EqualFold(r.Status, "ERR") {
		return zero, fmt.Errorf("%w: %v", plugin.ErrInvalidInput, r.Result)
	}
	return r.Result, nil
}

// dbClient resolves the active table param (validated) and the live DB handle —
// the common preamble for every table-scoped handler.
func tableClient(rc *plugin.RequestContext) (*surrealdb.DB, string, error) {
	table, err := requireIdent(rc.Param("table"))
	if err != nil {
		return nil, "", err
	}
	db, err := sess(rc).client(rc.Ctx)
	if err != nil {
		return nil, "", err
	}
	return db, table, nil
}

func deref[T any](p *[]T) []T {
	if p == nil {
		return nil
	}
	return *p
}

func anySlice[T any](s []T) any {
	out := make([]any, len(s))
	for i := range s {
		out[i] = s[i]
	}
	return out
}

// splitRecordID splits a "table:key" record id into its parts. SurrealDB record
// ids may themselves contain colons in the key, so only the first is the
// separator.
func splitRecordID(s string) (table, key string, ok bool) {
	i := strings.IndexByte(s, ':')
	if i <= 0 || i == len(s)-1 {
		return "", "", false
	}
	return s[:i], s[i+1:], true
}

func makeCursor(offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

func parseCursor(c string) (int, error) {
	b, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(string(b))
}
