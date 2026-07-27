package pinecone

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	defaultTopK = 10
	// maxTopK matches Pinecone's own ceiling for a query.
	maxTopK = 10000
	// metadataSampleSize bounds the records read to learn a namespace's metadata
	// keys for query completions.
	metadataSampleSize = 50
	// maxCompletionKeys keeps a wide namespace from flooding the completion list.
	maxCompletionKeys = 200

	metricsInterval = 10 * time.Second
)

func vectorNeighbors(rc *plugin.RequestContext) (any, error) {
	s, name, ns, client, err := indexTarget(rc)
	if err != nil {
		return nil, err
	}
	id := rc.Param("vector")
	if id == "" {
		return nil, fmt.Errorf("%w: a vector id is required", plugin.ErrInvalidInput)
	}
	req, err := rc.Page()
	if err != nil {
		return nil, err
	}
	topK := min(max(req.Limit, 1), s.opts.PageLimit)
	// Pinecone returns the anchor itself as the best match, so ask for one extra
	// and drop it.
	result, err := s.queryVectors(rc.Ctx, client, row{
		"id":              id,
		"topK":            topK + 1,
		"namespace":       ns,
		"includeMetadata": true,
	})
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(result.Matches))
	for _, match := range result.Matches {
		if match.ID == id {
			continue
		}
		item := match.row(name, ns)
		item["score"] = round(match.Score, 6)
		item["rank"] = len(rows) + 1
		rows = append(rows, item)
		if len(rows) >= topK {
			break
		}
	}
	total := len(rows)
	return plugin.Page[row]{Items: rows, Total: &total}, nil
}

func queryStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, name, ns, index, err := indexTarget(rc)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(client)
	enc := json.NewEncoder(client)
	for {
		var frame any
		if err := dec.Decode(&frame); err != nil {
			if client.Context().Err() != nil || errors.Is(err, io.EOF) {
				return nil
			}
			if err := enc.Encode(row{"error": "Invalid JSON query."}); err != nil {
				return err
			}
			continue
		}
		body, err := queryBody(frame, ns)
		if err != nil {
			rc.Audit(plugin.AuditDenied, queryAudit(name, ns, body), err)
			if encErr := enc.Encode(row{"error": err.Error()}); encErr != nil {
				return encErr
			}
			continue
		}
		result, err := s.queryVectors(rc.Ctx, index, body)
		if err != nil {
			rc.Audit(plugin.AuditError, queryAudit(name, ns, body), err)
			if encErr := enc.Encode(row{"error": err.Error()}); encErr != nil {
				return encErr
			}
			continue
		}
		rc.Audit(plugin.AuditAllowed, queryAudit(name, ns, body), nil)
		if err := enc.Encode(queryTable(result, name, ns)); err != nil {
			return err
		}
	}
}

// queryAudit records what a submitted search targeted without copying the query
// vector or the metadata filter into the audit trail.
func queryAudit(index, namespace string, body row) map[string]string {
	params := map[string]string{"index": index, "namespace": namespace}
	if body == nil {
		return params
	}
	if ns, ok := body["namespace"].(string); ok && ns != "" {
		params["namespace"] = ns
	}
	if topK, ok := body["topK"].(int); ok {
		params["topK"] = strconv.Itoa(topK)
	}
	params["anchor"] = "vector"
	if _, ok := body["id"]; ok {
		params["anchor"] = "id"
	}
	params["filtered"] = strconv.FormatBool(body["filter"] != nil)
	return params
}

// queryBody turns a query-editor frame into a Pinecone /query request. The
// editor sends {"query": "<json text>", "confirm": false}; a raw object is
// accepted too so the same route serves programmatic callers.
func queryBody(frame any, namespace string) (row, error) {
	document, ok := frame.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: query must be a JSON object", plugin.ErrInvalidInput)
	}
	if text, isText := document["query"].(string); isText {
		var decoded map[string]any
		if err := json.Unmarshal([]byte(text), &decoded); err != nil {
			return nil, fmt.Errorf("%w: query must be valid JSON", plugin.ErrInvalidInput)
		}
		document = decoded
	}
	body := row{}
	for key, value := range document {
		switch key {
		case "confirm", "cancel":
		default:
			body[key] = value
		}
	}
	if !hasAnchor(body) {
		return nil, fmt.Errorf("%w: a query needs a non-empty vector, sparseVector, or id", plugin.ErrInvalidInput)
	}
	topK, err := topKOf(body["topK"])
	if err != nil {
		return nil, err
	}
	body["topK"] = topK
	if ns, ok := body["namespace"].(string); ok {
		if err := validateNamespace(ns); err != nil {
			return nil, err
		}
	} else {
		body["namespace"] = namespace
	}
	if _, ok := body["includeMetadata"]; !ok {
		body["includeMetadata"] = true
	}
	return body, nil
}

// hasAnchor reports whether the body names something to search by. The editor
// template ships every anchor key, so a blank one has to read as absent instead
// of travelling to Pinecone and coming back as a 400.
func hasAnchor(body row) bool {
	if id, ok := body["id"].(string); ok && strings.TrimSpace(id) != "" {
		return true
	}
	if values, ok := body["vector"].([]any); ok && len(values) > 0 {
		return true
	}
	if sparse, ok := body["sparseVector"].(map[string]any); ok {
		if values, ok := sparse["values"].([]any); ok && len(values) > 0 {
			return true
		}
	}
	return false
}

func topKOf(raw any) (int, error) {
	switch value := raw.(type) {
	case nil:
		return defaultTopK, nil
	case float64:
		if value < 1 || value > maxTopK {
			return 0, fmt.Errorf("%w: topK must be between 1 and %d", plugin.ErrInvalidInput, maxTopK)
		}
		return int(value), nil
	default:
		return 0, fmt.Errorf("%w: topK must be a number", plugin.ErrInvalidInput)
	}
}

func queryTable(result queryResult, index, namespace string) row {
	ns := result.Namespace
	if ns == "" {
		ns = namespace
	}
	rows := make([][]any, 0, len(result.Matches))
	for _, match := range result.Matches {
		rows = append(rows, []any{
			match.ID,
			round(match.Score, 6),
			ns,
			jsonText(match.Metadata),
			len(match.Values),
			jsonText(match.Values),
		})
	}
	out := row{
		"columns":  []string{"id", "score", "namespace", "metadata", "dimension", "values"},
		"rows":     rows,
		"rowCount": len(rows),
		"index":    index,
	}
	if result.Usage != nil {
		out["readUnits"] = result.Usage.ReadUnits
	}
	return out
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

func queryComplete(rc *plugin.RequestContext) (any, error) {
	s, _, ns, client, err := indexTarget(rc)
	if err != nil {
		return nil, err
	}
	rows := []row{
		{"label": "vector", "value": `"vector": []`, "detail": "Dense query vector", "type": "field"},
		{"label": "sparseVector", "value": `"sparseVector": {"indices": [], "values": []}`, "detail": "Sparse query vector", "type": "field"},
		{"label": "id", "value": `"id": ""`, "detail": "Search by an existing record's vector", "type": "field"},
		{"label": "topK", "value": `"topK": 10`, "detail": "Matches to return", "type": "field"},
		{"label": "namespace", "value": `"namespace": "` + ns + `"`, "detail": "Namespace to search", "type": "field"},
		{"label": "filter", "value": `"filter": {}`, "detail": "Metadata filter", "type": "field"},
		{"label": "includeMetadata", "value": `"includeMetadata": true`, "detail": "Return metadata with each match", "type": "field"},
		{"label": "includeValues", "value": `"includeValues": false`, "detail": "Return vector values with each match", "type": "field"},
	}
	for _, op := range []struct{ name, detail string }{
		{"$eq", "equals"}, {"$ne", "not equals"},
		{"$gt", "greater than"}, {"$gte", "greater or equal"},
		{"$lt", "less than"}, {"$lte", "less or equal"},
		{"$in", "value in list"}, {"$nin", "value not in list"},
		{"$and", "all clauses match"}, {"$or", "any clause matches"},
		{"$exists", "field is present"},
	} {
		rows = append(rows, row{"label": op.name, "value": op.name, "detail": op.detail, "type": "operator"})
	}
	for _, key := range s.metadataKeys(rc.Ctx, client, ns) {
		rows = append(rows, row{"label": key, "value": `"` + key + `"`, "detail": "Metadata key", "type": "metadata"})
	}
	// Completion lists are consumed whole by the editor, so they are not paged.
	total := len(rows)
	return plugin.Page[row]{Items: rows, Total: &total}, nil
}

// metadataKeys samples a namespace so completions offer the metadata it stores.
func (s *Session) metadataKeys(ctx context.Context, client *restClient, namespace string) []string {
	query := url.Values{
		"limit":     []string{strconv.Itoa(metadataSampleSize)},
		"namespace": []string{namespace},
	}
	var listed vectorIDList
	if err := client.do(ctx, http.MethodGet, "/vectors/list", query, nil, &listed); err != nil {
		return nil
	}
	ids := make([]string, 0, len(listed.Vectors))
	for _, v := range listed.Vectors {
		ids = append(ids, v.ID)
	}
	records, err := s.fetchVectors(ctx, client, namespace, ids)
	if err != nil {
		return nil
	}
	seen := map[string]struct{}{}
	for _, record := range records {
		for key := range record.Metadata {
			seen[key] = struct{}{}
		}
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	if len(keys) > maxCompletionKeys {
		keys = keys[:maxCompletionKeys]
	}
	return keys
}

func indexMetrics(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, name, _, _, err := indexTarget(rc)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(client)
	ticker := time.NewTicker(metricsInterval)
	defer ticker.Stop()
	for {
		if err := enc.Encode(s.metricsFrame(rc.Ctx, name)); err != nil {
			return nil
		}
		select {
		case <-client.Context().Done():
			return nil
		case <-rc.Ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (s *Session) metricsFrame(ctx context.Context, name string) row {
	frame := row{}
	start := time.Now()
	stats, err := s.stats(ctx, name)
	if err != nil {
		return frame
	}
	frame["latencyMs"] = round(float64(time.Since(start).Microseconds())/1000, 3)
	frame["vectors"] = stats.TotalVectorCount
	frame["namespaces"] = len(stats.Namespaces)
	frame["fullnessPct"] = percent(stats.IndexFullness)
	if stats.Dimension > 0 {
		frame["dimension"] = stats.Dimension
		frame["vectorBytes"] = stats.TotalVectorCount * int64(stats.Dimension) * 4
	}
	return frame
}
