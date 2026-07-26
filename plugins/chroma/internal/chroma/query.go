package chroma

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

// scanConfirmThreshold is the record count above which an unbounded, unfiltered
// fetch asks for confirmation instead of pulling the whole collection.
const scanConfirmThreshold = 1000

type queryFrame struct {
	Query   string `json:"query"`
	Confirm bool   `json:"confirm"`
}

// queryRequest is the Chroma-flavoured JSON the editor accepts.
type queryRequest struct {
	QueryEmbeddings [][]float64    `json:"query_embeddings"`
	QueryEmbedding  []float64      `json:"query_embedding"`
	QueryID         string         `json:"query_id"`
	QueryText       string         `json:"query_text"`
	IDs             []string       `json:"ids"`
	NResults        int            `json:"n_results"`
	Limit           int            `json:"limit"`
	Offset          int            `json:"offset"`
	Where           map[string]any `json:"where"`
	WhereDocument   map[string]any `json:"where_document"`
	Include         []string       `json:"include"`
}

func queryStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, sc, col, err := collectionOf(rc)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(client)
	enc := json.NewEncoder(client)
	for {
		var frame queryFrame
		if err := dec.Decode(&frame); err != nil {
			if errors.Is(err, io.EOF) || client.Context().Err() != nil || rc.Ctx.Err() != nil {
				return nil
			}
			// A syntax error leaves the decoder parked on the same bytes forever,
			// so the stream cannot resynchronize: report it once and close.
			var syntax *json.SyntaxError
			if errors.As(err, &syntax) {
				_ = enc.Encode(row{"error": "Malformed request frame; the search stream was closed."})
				return nil
			}
			if encErr := enc.Encode(row{"error": "Invalid request frame: send {\"query\": \"<json>\"}."}); encErr != nil {
				return encErr
			}
			continue
		}
		if client.Context().Err() != nil {
			return nil
		}
		result, err := runQuery(rc, s, sc, col, frame)
		if err != nil {
			var challenge *confirmChallenge
			if errors.As(err, &challenge) {
				if encErr := enc.Encode(row{"confirmRequired": true, "confirmMessage": challenge.message}); encErr != nil {
					return encErr
				}
				continue
			}
			rc.Audit(plugin.AuditError, auditParams(frame, col, "error"), err)
			recordHistory(rc, col, frame.Query, nil, err)
			if encErr := enc.Encode(row{"error": err.Error()}); encErr != nil {
				return encErr
			}
			continue
		}
		rc.Audit(plugin.AuditAllowed, auditParams(frame, col, fmt.Sprint(result["mode"])), nil)
		recordHistory(rc, col, frame.Query, result, nil)
		if err := enc.Encode(result); err != nil {
			return err
		}
	}
}

type confirmChallenge struct{ message string }

func (c *confirmChallenge) Error() string { return c.message }

func auditParams(frame queryFrame, col collection, mode string) map[string]string {
	return map[string]string{
		"collection": col.Name,
		"database":   col.Database,
		"tenant":     col.Tenant,
		"mode":       mode,
		"bytes":      strconv.Itoa(len(frame.Query)),
		"confirmed":  strconv.FormatBool(frame.Confirm),
	}
}

func runQuery(rc *plugin.RequestContext, s *Session, sc scope, col collection, frame queryFrame) (row, error) {
	text := strings.TrimSpace(frame.Query)
	if text == "" {
		return nil, fmt.Errorf("%w: a query body is required", plugin.ErrInvalidInput)
	}
	var req queryRequest
	if err := json.Unmarshal([]byte(text), &req); err != nil {
		return nil, fmt.Errorf("%w: query must be a JSON object", plugin.ErrInvalidInput)
	}
	embeddings := req.QueryEmbeddings
	if len(embeddings) == 0 && len(req.QueryEmbedding) > 0 {
		embeddings = [][]float64{req.QueryEmbedding}
	}
	if id := strings.TrimSpace(req.QueryID); id != "" && len(embeddings) == 0 {
		anchor, err := s.fetchByIDs(rc.Ctx, sc, col.ID, []string{id})
		if err != nil {
			return nil, err
		}
		if anchor.len() == 0 || len(anchor.embedding(0)) == 0 {
			return nil, fmt.Errorf("%w: record %q has no stored embedding", plugin.ErrNotFound, id)
		}
		embeddings = [][]float64{anchor.embedding(0)}
	}
	whereDocument := req.WhereDocument
	if text := strings.TrimSpace(req.QueryText); text != "" && len(whereDocument) == 0 {
		whereDocument = map[string]any{"$contains": text}
	}
	start := time.Now()
	if len(embeddings) > 0 {
		return searchResult(rc, s, sc, col, req, embeddings, whereDocument, start)
	}
	return fetchResult(rc, s, sc, col, req, whereDocument, frame.Confirm, start)
}

func searchResult(rc *plugin.RequestContext, s *Session, sc scope, col collection, req queryRequest, embeddings [][]float64, whereDocument map[string]any, start time.Time) (row, error) {
	n := req.NResults
	if n <= 0 {
		n = req.Limit
	}
	if n <= 0 {
		n = 10
	}
	if n > s.opts.PageLimit {
		n = s.opts.PageLimit
	}
	body := row{
		"query_embeddings": embeddings,
		"n_results":        n,
		"include":          includeList(req.Include, []string{"documents", "metadatas", "distances", "uris"}),
	}
	if len(req.Where) > 0 {
		body["where"] = req.Where
	}
	if len(whereDocument) > 0 {
		body["where_document"] = whereDocument
	}
	if len(req.IDs) > 0 {
		body["ids"] = req.IDs
	}
	var out queryResult
	if err := s.do(rc.Ctx, http.MethodPost, sc.collectionPath(col.ID)+"/query", nil, body, &out); err != nil {
		return nil, err
	}
	columns := []string{"query", "rank", "id", "distance", "similarity", "document", "metadata"}
	rows := make([][]any, 0, 16)
	for q := range out.IDs {
		for i, id := range out.IDs[q] {
			rows = append(rows, []any{
				q,
				i + 1,
				id,
				roundAt(out.Distances, q, i),
				similarityAt(col.space(), out.Distances, q, i),
				documentAt(out.Documents, q, i),
				jsonText(metadataAt(out.Metadatas, q, i)),
			})
		}
	}
	return row{
		"columns":   columns,
		"rows":      rows,
		"rowCount":  len(rows),
		"elapsedMs": elapsed(start),
		"statement": fmt.Sprintf("query %s (%s, n_results=%d)", col.Name, spaceLabel(col.space()), n),
		"mode":      "similarity",
	}, nil
}

func fetchResult(rc *plugin.RequestContext, s *Session, sc scope, col collection, req queryRequest, whereDocument map[string]any, confirmed bool, start time.Time) (row, error) {
	limit := req.Limit
	unbounded := limit <= 0 && len(req.IDs) == 0 && len(req.Where) == 0 && len(whereDocument) == 0
	if unbounded && !confirmed {
		total, err := s.count(rc.Ctx, sc, col.ID)
		if err == nil && total > scanConfirmThreshold {
			return nil, &confirmChallenge{message: fmt.Sprintf("This fetch has no ids, filter, or limit and would scan %d records in %q. Run it anyway?", total, col.Name)}
		}
	}
	if limit <= 0 || limit > s.opts.PageLimit {
		limit = s.opts.PageLimit
	}
	include := includeList(req.Include, []string{"documents", "metadatas", "uris"})
	body := row{
		"limit":   limit,
		"offset":  max(req.Offset, 0),
		"include": include,
	}
	if len(req.IDs) > 0 {
		body["ids"] = req.IDs
	}
	if len(req.Where) > 0 {
		body["where"] = req.Where
	}
	if len(whereDocument) > 0 {
		body["where_document"] = whereDocument
	}
	var out recordSet
	if err := s.do(rc.Ctx, http.MethodPost, sc.collectionPath(col.ID)+"/get", nil, body, &out); err != nil {
		return nil, err
	}
	// Vectors are only in the payload when the caller asked for them, so the
	// dimension column is offered only then instead of reporting a constant zero.
	vectors := slices.Contains(include, "embeddings")
	columns := []string{"id", "document", "metadata", "uri"}
	if vectors {
		columns = append(columns, "dimension")
	}
	rows := make([][]any, 0, out.len())
	for i := range out.IDs {
		values := []any{out.IDs[i], out.document(i), jsonText(out.metadata(i)), out.uri(i)}
		if vectors {
			values = append(values, len(out.embedding(i)))
		}
		rows = append(rows, values)
	}
	return row{
		"columns":   columns,
		"rows":      rows,
		"rowCount":  len(rows),
		"elapsedMs": elapsed(start),
		"statement": fmt.Sprintf("get %s (limit=%d)", col.Name, limit),
		"mode":      "fetch",
	}, nil
}

func includeList(requested, fallback []string) []string {
	allowed := map[string]struct{}{"documents": {}, "metadatas": {}, "embeddings": {}, "distances": {}, "uris": {}}
	out := make([]string, 0, len(requested))
	for _, field := range requested {
		if _, ok := allowed[field]; ok {
			out = append(out, field)
		}
	}
	if len(out) == 0 {
		return fallback
	}
	return out
}

func elapsed(start time.Time) float64 {
	return round(float64(time.Since(start).Microseconds())/1000, 3)
}

func roundAt(values [][]*float64, q, i int) any {
	if q < len(values) && i < len(values[q]) && values[q][i] != nil {
		return round(*values[q][i], 6)
	}
	return nil
}

func similarityAt(space string, values [][]*float64, q, i int) any {
	if q < len(values) && i < len(values[q]) && values[q][i] != nil {
		return round(similarity(space, *values[q][i]), 6)
	}
	return nil
}

func documentAt(values [][]*string, q, i int) string {
	if q < len(values) && i < len(values[q]) && values[q][i] != nil {
		return *values[q][i]
	}
	return ""
}

func metadataAt(values [][]map[string]any, q, i int) map[string]any {
	if q < len(values) && i < len(values[q]) && values[q][i] != nil {
		return values[q][i]
	}
	return nil
}

func jsonText(v any) string {
	if v == nil {
		return ""
	}
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(data)
}
