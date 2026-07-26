package arangodb

import (
	"strings"
	"unicode"
)

// aqlWriteKeywords are the AQL data-modification operations. AQL has no DDL, so
// this set is the complete write surface of the query language.
var aqlWriteKeywords = map[string]bool{
	"INSERT": true, "UPDATE": true, "REPLACE": true, "REMOVE": true, "UPSERT": true,
}

type confirmationError struct{ message string }

func (e confirmationError) Error() string { return e.message }

// aqlWrites reports whether a query modifies data. Literals and comments are
// blanked first so a string such as "REMOVE" cannot flag a read-only query.
func aqlWrites(query string) bool {
	for _, token := range aqlTokens(stripAQL(query)) {
		if aqlWriteKeywords[strings.ToUpper(token)] {
			return true
		}
	}
	return false
}

// splitExplain recognises the editor-only `EXPLAIN <query>` prefix, which maps
// to the server's query explain endpoint instead of an execution.
func splitExplain(query string) (string, bool) {
	trimmed := strings.TrimSpace(query)
	if len(trimmed) < 8 || !strings.EqualFold(trimmed[:7], "EXPLAIN") {
		return trimmed, false
	}
	if !unicode.IsSpace(rune(trimmed[7])) {
		return trimmed, false
	}
	return strings.TrimSpace(trimmed[7:]), true
}

func aqlTokens(query string) []string {
	return strings.FieldsFunc(query, func(r rune) bool {
		return !unicode.IsLetter(r) && r != '_'
	})
}

func stripAQL(query string) string {
	var out strings.Builder
	out.Grow(len(query))
	runes := []rune(query)
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		switch {
		case r == '\'' || r == '"' || r == '`' || r == '´':
			quote := r
			out.WriteRune(' ')
			for i++; i < len(runes); i++ {
				if runes[i] == '\\' && i+1 < len(runes) {
					i++
					continue
				}
				if runes[i] == quote {
					break
				}
			}
		case r == '/' && i+1 < len(runes) && runes[i+1] == '/':
			for i < len(runes) && runes[i] != '\n' {
				i++
			}
			out.WriteRune(' ')
		case r == '/' && i+1 < len(runes) && runes[i+1] == '*':
			i += 2
			for i+1 < len(runes) && !(runes[i] == '*' && runes[i+1] == '/') {
				i++
			}
			i++
			out.WriteRune(' ')
		default:
			out.WriteRune(r)
		}
	}
	return out.String()
}

// aqlKeywords and aqlFunctions back the query editor's completion route. They
// are the documented AQL surface an operator reaches for most often.
var aqlKeywords = []string{
	"FOR", "RETURN", "FILTER", "SORT", "LIMIT", "LET", "COLLECT", "AGGREGATE",
	"INSERT", "UPDATE", "REPLACE", "REMOVE", "UPSERT", "WITH", "IN", "INTO",
	"GRAPH", "OUTBOUND", "INBOUND", "ANY", "SHORTEST_PATH", "K_SHORTEST_PATHS",
	"ALL_SHORTEST_PATHS", "K_PATHS", "PRUNE", "SEARCH", "OPTIONS", "DISTINCT",
	"WINDOW", "TRUE", "FALSE", "NULL", "AND", "OR", "NOT", "ASC", "DESC",
}

var aqlFunctions = []struct {
	Name   string
	Detail string
	Apply  string
}{
	{"LENGTH", "number of items", "LENGTH(doc)"},
	{"DOCUMENT", "look up by _id or _key", "DOCUMENT(\"collection/key\")"},
	{"KEEP", "keep listed attributes", "KEEP(doc, \"_key\")"},
	{"UNSET", "drop listed attributes", "UNSET(doc, \"_rev\")"},
	{"MERGE", "merge objects", "MERGE(doc, { })"},
	{"ATTRIBUTES", "attribute names of an object", "ATTRIBUTES(doc)"},
	{"VALUES", "attribute values of an object", "VALUES(doc)"},
	{"DATE_ISO8601", "format a timestamp", "DATE_ISO8601(DATE_NOW())"},
	{"DATE_NOW", "current epoch milliseconds", "DATE_NOW()"},
	{"RAND", "random number in [0,1)", "RAND()"},
	{"CONCAT", "concatenate values", "CONCAT(doc.first, doc.last)"},
	{"CONTAINS", "substring test", "CONTAINS(doc.name, \"value\")"},
	{"LOWER", "lowercase a string", "LOWER(doc.name)"},
	{"REGEX_TEST", "regular expression test", "REGEX_TEST(doc.name, \"^a\")"},
	{"BM25", "ArangoSearch relevance score", "BM25(doc)"},
	{"TFIDF", "ArangoSearch relevance score", "TFIDF(doc)"},
	{"ANALYZER", "scope a SEARCH expression", "ANALYZER(doc.text == \"value\", \"text_en\")"},
	{"PHRASE", "ArangoSearch phrase match", "PHRASE(doc.text, \"value\", \"text_en\")"},
	{"COLLECTION_COUNT", "document count of a collection", "COLLECTION_COUNT(\"collection\")"},
	{"COUNT_DISTINCT", "distinct aggregate", "COUNT_DISTINCT(doc.city)"},
	{"GEO_DISTANCE", "distance between two geo points", "GEO_DISTANCE(doc.location, [0, 0])"},
	{"NEAR", "geo nearest neighbours", "NEAR(collection, 0, 0, 10)"},
}
