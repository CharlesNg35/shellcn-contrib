package neo4j

import (
	"errors"
	"fmt"
	"strings"
	"unicode"

	driver "github.com/neo4j/neo4j-go-driver/v6/neo4j"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

var cypherWriteKeywords = map[string]bool{
	"CREATE": true, "MERGE": true, "SET": true, "DELETE": true, "DETACH": true,
	"REMOVE": true, "DROP": true, "ALTER": true, "RENAME": true, "GRANT": true,
	"DENY": true, "REVOKE": true, "ENABLE": true, "START": true, "STOP": true,
	"LOAD": true, "CALL": true, "USE": true,
}

func cypherNeedsReview(query string) bool {
	clean := stripCypher(query)
	first := firstCypherKeyword(clean)
	if first == "EXPLAIN" {
		return false
	}
	if first == "PROFILE" {
		clean = strings.TrimSpace(clean[len(first):])
	}
	for _, token := range strings.FieldsFunc(clean, func(r rune) bool {
		return !unicode.IsLetter(r) && r != '_'
	}) {
		if cypherWriteKeywords[strings.ToUpper(token)] {
			return true
		}
	}
	return false
}

func firstCypherKeyword(query string) string {
	for _, token := range strings.FieldsFunc(query, func(r rune) bool {
		return !unicode.IsLetter(r) && r != '_'
	}) {
		if token != "" {
			return strings.ToUpper(token)
		}
	}
	return ""
}

func stripCypher(query string) string {
	var out strings.Builder
	var quote rune
	escaped := false
	for i := 0; i < len(query); i++ {
		r := rune(query[i])
		if quote != 0 {
			if r == quote && !escaped {
				quote = 0
			}
			escaped = r == '\\' && !escaped
			out.WriteByte(' ')
			continue
		}
		if r == '\'' || r == '"' || r == '`' {
			quote = r
			out.WriteByte(' ')
			continue
		}
		if r == '/' && i+1 < len(query) && query[i+1] == '/' {
			for i < len(query) && query[i] != '\n' {
				i++
			}
			out.WriteByte(' ')
			continue
		}
		out.WriteByte(query[i])
		escaped = false
	}
	return out.String()
}

func safeGraphName(raw, label string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("%w: %s is required", plugin.ErrInvalidInput, label)
	}
	for _, r := range raw {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			return "", fmt.Errorf("%w: %s contains unsupported characters", plugin.ErrInvalidInput, label)
		}
	}
	return raw, nil
}

func labelList(raw string) (string, error) {
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == '\n' || r == '\r' || r == '\t' || r == ' '
	})
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		name, err := safeGraphName(part, "label")
		if err != nil {
			return "", err
		}
		out = append(out, quoteCypherName(name))
	}
	if len(out) == 0 {
		return "", fmt.Errorf("%w: at least one label is required", plugin.ErrInvalidInput)
	}
	return ":" + strings.Join(out, ":"), nil
}

func quoteCypherName(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func neo4jErr(err error) error {
	if err == nil {
		return nil
	}
	var authErr *driver.InvalidAuthenticationError
	if errors.As(err, &authErr) {
		return fmt.Errorf("%w: %v", plugin.ErrUnauthorized, authErr)
	}
	var neoErr *driver.Neo4jError
	if errors.As(err, &neoErr) {
		msg := neoErr.Msg
		if msg == "" {
			msg = neoErr.Code
		}
		switch {
		case strings.Contains(neoErr.Code, "Security.Unauthorized"), strings.Contains(neoErr.Code, "Security.Forbidden"):
			return fmt.Errorf("%w: %s", plugin.ErrUnauthorized, msg)
		case strings.Contains(neoErr.Code, "Schema"), strings.Contains(neoErr.Code, "Statement"), strings.Contains(neoErr.Code, "Constraint"):
			return fmt.Errorf("%w: %s", plugin.ErrInvalidInput, msg)
		case strings.Contains(neoErr.Code, "DatabaseNotFound"), strings.Contains(neoErr.Code, "EntityNotFound"):
			return fmt.Errorf("%w: %s", plugin.ErrNotFound, msg)
		default:
			return fmt.Errorf("%w: %s", plugin.ErrUnavailable, msg)
		}
	}
	return err
}

func queryType(summary driver.ResultSummary) string {
	if summary == nil {
		return ""
	}
	return fmt.Sprint(summary.QueryType())
}
