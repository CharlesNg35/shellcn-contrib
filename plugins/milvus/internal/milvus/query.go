package milvus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/milvusclient"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

// fullScanConfirmLimit is the point at which an unfiltered query is treated as
// a full scan and needs an explicit confirmation from the console.
const fullScanConfirmLimit = 500

type queryFrame struct {
	Query   string `json:"query"`
	Confirm bool   `json:"confirm,omitempty"`
}

type queryRequest struct {
	Vector     []float64   `json:"vector"`
	Vectors    [][]float64 `json:"vectors"`
	Text       string      `json:"text"`
	Field      string      `json:"field"`
	Filter     string      `json:"filter"`
	IDs        []any       `json:"ids"`
	Limit      int         `json:"limit"`
	Offset     int         `json:"offset"`
	Output     []string    `json:"output"`
	Partitions []string    `json:"partitions"`
	Metric     string      `json:"metric"`
	GroupBy    string      `json:"group_by"`
	Nprobe     int         `json:"nprobe"`
	Ef         int         `json:"ef"`
}

func queryStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	collection, err := collectionParam(rc)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(client)
	enc := json.NewEncoder(client)
	for {
		var frame queryFrame
		if err := dec.Decode(&frame); err != nil {
			if errors.Is(err, io.EOF) || client.Context().Err() != nil {
				return nil
			}
			if encErr := enc.Encode(row{"error": "Invalid request frame; expected JSON."}); encErr != nil {
				return encErr
			}
			continue
		}
		result, err := runQuery(rc, collection, frame)
		if err != nil {
			rc.Audit(plugin.AuditError, queryAuditParams(collection, frame, nil), err)
			if encErr := enc.Encode(row{"error": err.Error()}); encErr != nil {
				return encErr
			}
			continue
		}
		rc.Audit(plugin.AuditAllowed, queryAuditParams(collection, frame, result), nil)
		if err := enc.Encode(result); err != nil {
			return err
		}
	}
}

func queryAuditParams(collection string, frame queryFrame, result row) map[string]string {
	params := map[string]string{
		"collection": collection,
		"bytes":      strconv.Itoa(len(frame.Query)),
		"confirmed":  strconv.FormatBool(frame.Confirm),
	}
	if result != nil {
		if mode, ok := result["mode"].(string); ok {
			params["mode"] = mode
		}
		if count, ok := result["rowCount"].(int); ok {
			params["rows"] = strconv.Itoa(count)
		}
	}
	return params
}

func runQuery(rc *plugin.RequestContext, collection string, frame queryFrame) (row, error) {
	req, err := parseQueryRequest(frame.Query)
	if err != nil {
		return nil, err
	}
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	limit := queryLimit(req, s.opts.PageLimit)
	if isFullScan(req, limit) && !frame.Confirm {
		return row{
			"confirmRequired": true,
			"confirmMessage":  fmt.Sprintf("This query has no filter and would scan up to %d entities. Run it anyway?", limit),
		}, nil
	}

	c, err := s.clientFor(rc.Ctx, rc.Param("database"))
	if err != nil {
		return nil, err
	}
	ctx, cancel := withTimeout(rc, s)
	defer cancel()
	coll, err := collectionSchemaOf(ctx, c, collection)
	if err != nil {
		return nil, err
	}
	output := req.Output
	if len(output) == 0 {
		output = []string{"*"}
	}

	started := time.Now()
	switch {
	case len(req.Vectors) > 0 || len(req.Vector) > 0 || req.Text != "":
		return searchResult(ctx, c, coll, collection, req, limit, output, started)
	case len(req.IDs) > 0:
		return lookupResult(ctx, c, coll, collection, req, limit, output, started)
	default:
		return filterResult(ctx, c, coll, collection, req, limit, output, started)
	}
}

func parseQueryRequest(text string) (queryRequest, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return queryRequest{}, fmt.Errorf("%w: a query is required", plugin.ErrInvalidInput)
	}
	var req queryRequest
	if err := json.Unmarshal([]byte(text), &req); err != nil {
		return queryRequest{}, fmt.Errorf("%w: query must be a JSON object", plugin.ErrInvalidInput)
	}
	for i, name := range req.Partitions {
		validated, err := identifier("partition", name)
		if err != nil {
			return queryRequest{}, err
		}
		req.Partitions[i] = validated
	}
	for _, field := range []string{req.Field, req.GroupBy} {
		if field == "" {
			continue
		}
		if _, err := identifier("field", field); err != nil {
			return queryRequest{}, err
		}
	}
	return req, nil
}

func queryLimit(req queryRequest, pageLimit int) int {
	limit := req.Limit
	if limit <= 0 {
		limit = 10
	}
	if pageLimit > 0 && limit > pageLimit {
		limit = pageLimit
	}
	return limit
}

// isFullScan reports a scalar query with no filter, ids, or vector, which walks
// the whole collection and therefore needs an explicit confirmation.
func isFullScan(req queryRequest, limit int) bool {
	if len(req.Vectors) > 0 || len(req.Vector) > 0 || req.Text != "" || len(req.IDs) > 0 {
		return false
	}
	return strings.TrimSpace(req.Filter) == "" && limit > fullScanConfirmLimit
}

func searchResult(ctx context.Context, c *milvusclient.Client, coll *entity.Collection, collection string, req queryRequest, limit int, output []string, started time.Time) (row, error) {
	vectors, err := searchVectors(req)
	if err != nil {
		return nil, err
	}
	opt := milvusclient.NewSearchOption(collection, limit, vectors).WithOutputFields(output...)
	field := req.Field
	if field == "" {
		field = annField(coll.Schema, req.Text != "")
	}
	if field != "" {
		opt = opt.WithANNSField(field)
	}
	if filter := strings.TrimSpace(req.Filter); filter != "" {
		opt = opt.WithFilter(filter)
	}
	if req.Offset > 0 {
		opt = opt.WithOffset(req.Offset)
	}
	if len(req.Partitions) > 0 {
		opt = opt.WithPartitions(req.Partitions...)
	}
	if req.GroupBy != "" {
		opt = opt.WithGroupByField(req.GroupBy)
	}
	if metric := strings.ToUpper(strings.TrimSpace(req.Metric)); metric != "" {
		opt = opt.WithSearchParam("metric_type", metric)
	}
	if req.Nprobe > 0 {
		opt = opt.WithSearchParam("nprobe", strconv.Itoa(req.Nprobe))
	}
	if req.Ef > 0 {
		opt = opt.WithSearchParam("ef", strconv.Itoa(req.Ef))
	}

	sets, err := c.Search(ctx, opt)
	if err != nil {
		return nil, mapError(err)
	}
	pk := primaryKeyName(coll.Schema)
	rows := make([]row, 0, limit)
	for i, set := range sets {
		if set.Err != nil {
			return nil, mapError(set.Err)
		}
		for _, item := range resultRows(set, pk) {
			if len(sets) > 1 {
				item["query"] = i
			}
			rows = append(rows, item)
		}
	}
	mode := "search"
	if req.Text != "" {
		mode = "text_search"
	}
	return tableResult(mode, rows, searchColumns(len(sets) > 1, pk, rows), started), nil
}

// annField picks the vector field a search runs against: a text search needs the
// sparse field a BM25 function writes, a vector search needs a dense field.
func annField(schema *entity.Schema, text bool) string {
	var dense, sparse string
	for _, f := range schema.Fields {
		if !f.DataType.IsVectorType() {
			continue
		}
		if f.DataType == entity.FieldTypeSparseVector {
			if sparse == "" {
				sparse = f.Name
			}
			continue
		}
		if dense == "" {
			dense = f.Name
		}
	}
	if text && sparse != "" {
		return sparse
	}
	if dense != "" {
		return dense
	}
	return sparse
}

func searchVectors(req queryRequest) ([]entity.Vector, error) {
	if req.Text != "" {
		return []entity.Vector{entity.Text(req.Text)}, nil
	}
	raw := req.Vectors
	if len(raw) == 0 {
		raw = [][]float64{req.Vector}
	}
	out := make([]entity.Vector, 0, len(raw))
	for _, values := range raw {
		if len(values) == 0 {
			return nil, fmt.Errorf("%w: search vectors must not be empty", plugin.ErrInvalidInput)
		}
		vector := make(entity.FloatVector, 0, len(values))
		for _, v := range values {
			vector = append(vector, float32(v))
		}
		out = append(out, vector)
	}
	return out, nil
}

func lookupResult(ctx context.Context, c *milvusclient.Client, coll *entity.Collection, collection string, req queryRequest, limit int, output []string, started time.Time) (row, error) {
	pkField := primaryKeyField(coll.Schema)
	if pkField == nil {
		return nil, fmt.Errorf("%w: collection has no primary key", plugin.ErrConflict)
	}
	var ids column.Column
	if pkField.DataType == entity.FieldTypeVarChar {
		keys := make([]string, 0, len(req.IDs))
		for _, id := range req.IDs {
			value, err := asString(id)
			if err != nil {
				return nil, fmt.Errorf("%w: %v", plugin.ErrInvalidInput, err)
			}
			keys = append(keys, value)
		}
		ids = column.NewColumnVarChar(pkField.Name, keys)
	} else {
		keys := make([]int64, 0, len(req.IDs))
		for _, id := range req.IDs {
			value, err := asInt64(id)
			if err != nil {
				return nil, fmt.Errorf("%w: %v", plugin.ErrInvalidInput, err)
			}
			keys = append(keys, value)
		}
		ids = column.NewColumnInt64(pkField.Name, keys)
	}
	opt := milvusclient.NewQueryOption(collection).WithIDs(ids).WithOutputFields(output...).WithLimit(limit)
	if len(req.Partitions) > 0 {
		opt = opt.WithPartitions(req.Partitions...)
	}
	rs, err := c.Get(ctx, opt)
	if err != nil {
		return nil, mapError(err)
	}
	rows := resultRows(rs, pkField.Name)
	return tableResult("get", rows, rowColumns(rows), started), nil
}

func filterResult(ctx context.Context, c *milvusclient.Client, coll *entity.Collection, collection string, req queryRequest, limit int, output []string, started time.Time) (row, error) {
	opt := milvusclient.NewQueryOption(collection).WithLimit(limit).WithOutputFields(output...)
	if filter := strings.TrimSpace(req.Filter); filter != "" {
		opt = opt.WithFilter(filter)
	}
	if req.Offset > 0 {
		opt = opt.WithOffset(req.Offset)
	}
	if len(req.Partitions) > 0 {
		opt = opt.WithPartitions(req.Partitions...)
	}
	rs, err := c.Query(ctx, opt)
	if err != nil {
		return nil, mapError(err)
	}
	rows := resultRows(rs, primaryKeyName(coll.Schema))
	return tableResult("query", rows, rowColumns(rows), started), nil
}

func tableResult(mode string, rows []row, columns []string, started time.Time) row {
	values := make([][]any, 0, len(rows))
	for _, item := range rows {
		record := make([]any, 0, len(columns))
		for _, name := range columns {
			record = append(record, item[name])
		}
		values = append(values, record)
	}
	return row{
		"columns":   columns,
		"rows":      values,
		"rowCount":  len(values),
		"elapsedMs": time.Since(started).Milliseconds(),
		"mode":      mode,
	}
}

func searchColumns(multi bool, pk string, rows []row) []string {
	columns := make([]string, 0, 8)
	if multi {
		columns = append(columns, "query")
	}
	if pk != "" {
		columns = append(columns, pk)
	}
	columns = append(columns, "score")
	seen := map[string]bool{"score": true, "query": true, entityKeyField: true}
	if pk != "" {
		seen[pk] = true
	}
	for _, name := range orderedKeys(rows) {
		if seen[name] {
			continue
		}
		seen[name] = true
		columns = append(columns, name)
	}
	return columns
}

func rowColumns(rows []row) []string {
	columns := make([]string, 0, 8)
	for _, name := range orderedKeys(rows) {
		if name == entityKeyField {
			continue
		}
		columns = append(columns, name)
	}
	return columns
}

// orderedKeys keeps result columns stable across frames by sorting the union of
// keys the backend returned.
func orderedKeys(rows []row) []string {
	seen := map[string]bool{}
	keys := make([]string, 0, 8)
	for _, item := range rows {
		for name := range item {
			if !seen[name] {
				seen[name] = true
				keys = append(keys, name)
			}
		}
	}
	sort.Strings(keys)
	return keys
}
