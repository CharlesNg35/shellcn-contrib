package weaviate

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
)

const defaultSearchLimit = 10

type queryFrame struct {
	Query   string `json:"query"`
	Confirm bool   `json:"confirm,omitempty"`
}

type sortSpec struct {
	Path  []string `json:"path"`
	Order string   `json:"order"`
}

type searchSpec struct {
	Mode         string          `json:"mode"`
	Query        string          `json:"query"`
	Concepts     []string        `json:"concepts"`
	Vector       []float32       `json:"vector"`
	Alpha        *float64        `json:"alpha"`
	Fusion       string          `json:"fusion"`
	Properties   []string        `json:"properties"`
	Limit        int             `json:"limit"`
	Offset       int             `json:"offset"`
	Autocut      int             `json:"autocut"`
	Certainty    *float64        `json:"certainty"`
	Distance     *float64        `json:"distance"`
	TargetVector string          `json:"target_vector"`
	Tenant       string          `json:"tenant"`
	Where        json.RawMessage `json:"where"`
	Sort         []sortSpec      `json:"sort"`
	Additional   []string        `json:"additional"`
}

func graphqlStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	return queryLoop(rc, client, func(query string) (any, error) {
		s, err := session(rc)
		if err != nil {
			return nil, err
		}
		started := time.Now()
		resp, err := s.client.graphql.Raw().WithQuery(query).Do(rc.Ctx)
		if err != nil {
			return nil, mapError(err)
		}
		if err := graphqlErrors(resp); err != nil {
			return nil, err
		}
		result := graphqlTable(resp)
		result["elapsedMs"] = time.Since(started).Milliseconds()
		record(rc, "query", plugin.SeverityInfo, "GraphQL query executed", truncate(singleLine(query), 160))
		return result, nil
	})
}

func searchStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	return queryLoop(rc, client, func(query string) (any, error) {
		s, err := session(rc)
		if err != nil {
			return nil, err
		}
		class, err := loadClass(rc)
		if err != nil {
			return nil, err
		}
		spec, err := parseSearchSpec(query)
		if err != nil {
			return nil, err
		}
		builder, err := buildSearch(s, class, spec)
		if err != nil {
			return nil, err
		}
		started := time.Now()
		resp, err := builder.Do(rc.Ctx)
		if err != nil {
			return nil, mapError(err)
		}
		if err := graphqlErrors(resp); err != nil {
			return nil, err
		}
		result := graphqlTable(resp)
		result["elapsedMs"] = time.Since(started).Milliseconds()
		result["statement"] = builder.Build()
		record(rc, "query", plugin.SeverityInfo, spec.Mode+" search on "+class.Class, truncate(singleLine(query), 160))
		return result, nil
	})
}

// queryLoop implements the PanelQueryEditor contract: read {"query","confirm"}
// frames, write one result or error frame per execution, and keep the socket open
// for normal execution failures.
func queryLoop(rc *plugin.RequestContext, client plugin.ClientStream, run func(string) (any, error)) error {
	dec := json.NewDecoder(client)
	enc := json.NewEncoder(client)
	for {
		var frame queryFrame
		if err := dec.Decode(&frame); err != nil {
			if errors.Is(err, io.EOF) || client.Context().Err() != nil || rc.Ctx.Err() != nil {
				return nil
			}
			if encErr := enc.Encode(row{"error": "Request frame must be JSON with a query field."}); encErr != nil {
				return encErr
			}
			continue
		}
		query := strings.TrimSpace(frame.Query)
		if query == "" {
			if err := enc.Encode(row{"error": "Query is required."}); err != nil {
				return err
			}
			continue
		}
		params := map[string]string{
			"bytes":     strconv.Itoa(len(query)),
			"confirmed": strconv.FormatBool(frame.Confirm),
		}
		result, err := run(query)
		if err != nil {
			rc.Audit(plugin.AuditError, params, err)
			if encErr := enc.Encode(row{"error": err.Error()}); encErr != nil {
				return encErr
			}
			continue
		}
		rc.Audit(plugin.AuditAllowed, params, nil)
		if err := enc.Encode(result); err != nil {
			return err
		}
	}
}

func parseSearchSpec(query string) (searchSpec, error) {
	var spec searchSpec
	if err := json.Unmarshal([]byte(query), &spec); err != nil {
		return searchSpec{}, fmt.Errorf("%w: search body must be a JSON object: %v", plugin.ErrInvalidInput, err)
	}
	spec.Mode = strings.TrimSpace(spec.Mode)
	if spec.Mode == "" {
		spec.Mode = inferMode(spec)
	}
	switch spec.Mode {
	case "fetch", "nearVector", "nearText", "hybrid", "bm25":
	default:
		return searchSpec{}, fmt.Errorf("%w: mode must be one of fetch, nearVector, nearText, hybrid, bm25", plugin.ErrInvalidInput)
	}
	if spec.Limit <= 0 {
		spec.Limit = defaultSearchLimit
	}
	if spec.Limit > plugin.MaxPageLimit {
		spec.Limit = plugin.MaxPageLimit
	}
	if spec.Offset < 0 {
		spec.Offset = 0
	}
	return spec, nil
}

func inferMode(spec searchSpec) string {
	switch {
	case len(spec.Vector) > 0 && strings.TrimSpace(spec.Query) != "":
		return "hybrid"
	case len(spec.Vector) > 0:
		return "nearVector"
	case len(spec.Concepts) > 0:
		return "nearText"
	case strings.TrimSpace(spec.Query) != "":
		return "hybrid"
	default:
		return "fetch"
	}
}

func buildSearch(s *Session, class *models.Class, spec searchSpec) (*graphql.GetBuilder, error) {
	available, _ := graphqlFields(class)
	byName := map[string]graphql.Field{}
	for _, field := range available {
		byName[field.Name] = field
	}
	selected := available
	if len(spec.Properties) > 0 {
		selected = make([]graphql.Field, 0, len(spec.Properties))
		names := make([]string, 0, len(spec.Properties))
		for _, name := range spec.Properties {
			field, ok := byName[strings.TrimSpace(name)]
			if !ok {
				return nil, fmt.Errorf("%w: %s has no selectable property %q", plugin.ErrInvalidInput, class.Class, name)
			}
			selected = append(selected, field)
			names = append(names, field.Name)
		}
		spec.Properties = names
	}
	additional, err := additionalFor(spec)
	if err != nil {
		return nil, err
	}
	builder := s.client.graphql.Get().
		WithClassName(class.Class).
		WithFields(append(selected, additionalField(additional...))...).
		WithLimit(spec.Limit)
	if spec.Offset > 0 {
		builder = builder.WithOffset(spec.Offset)
	}
	if spec.Autocut > 0 {
		builder = builder.WithAutocut(spec.Autocut)
	}
	if spec.Tenant != "" {
		builder = builder.WithTenant(spec.Tenant)
	}
	targetVector, err := targetVectorFor(class, spec.TargetVector)
	if err != nil {
		return nil, err
	}
	if len(spec.Where) > 0 {
		where, err := buildWhere(spec.Where)
		if err != nil {
			return nil, err
		}
		builder = builder.WithWhere(where)
	}
	for _, entry := range spec.Sort {
		if len(entry.Path) == 0 {
			continue
		}
		field, ok := byName[strings.TrimSpace(entry.Path[0])]
		if !ok {
			return nil, fmt.Errorf("%w: cannot sort by %q", plugin.ErrInvalidInput, entry.Path[0])
		}
		order := graphql.Asc
		if strings.EqualFold(entry.Order, "desc") {
			order = graphql.Desc
		}
		builder = builder.WithSort(graphql.Sort{Path: []string{field.Name}, Order: order})
	}

	switch spec.Mode {
	case "fetch":
		return builder, nil
	case "nearVector":
		if len(spec.Vector) == 0 {
			return nil, fmt.Errorf("%w: nearVector needs a vector", plugin.ErrInvalidInput)
		}
		near := s.client.graphql.NearVectorArgBuilder().WithVector(spec.Vector)
		if spec.Certainty != nil {
			near = near.WithCertainty(float32(*spec.Certainty))
		}
		if spec.Distance != nil {
			near = near.WithDistance(float32(*spec.Distance))
		}
		if targetVector != "" {
			near = near.WithTargetVectors(targetVector)
		}
		return builder.WithNearVector(near), nil
	case "nearText":
		concepts := spec.Concepts
		if len(concepts) == 0 && spec.Query != "" {
			concepts = []string{spec.Query}
		}
		if len(concepts) == 0 {
			return nil, fmt.Errorf("%w: nearText needs concepts or a query", plugin.ErrInvalidInput)
		}
		near := s.client.graphql.NearTextArgBuilder().WithConcepts(concepts)
		if spec.Certainty != nil {
			near = near.WithCertainty(float32(*spec.Certainty))
		}
		if spec.Distance != nil {
			near = near.WithDistance(float32(*spec.Distance))
		}
		if targetVector != "" {
			near = near.WithTargetVectors(targetVector)
		}
		return builder.WithNearText(near), nil
	case "bm25":
		if spec.Query == "" {
			return nil, fmt.Errorf("%w: bm25 needs a query", plugin.ErrInvalidInput)
		}
		bm25 := s.client.graphql.Bm25ArgBuilder().WithQuery(spec.Query)
		if len(spec.Properties) > 0 {
			bm25 = bm25.WithProperties(spec.Properties...)
		}
		return builder.WithBM25(bm25), nil
	default:
		if spec.Query == "" && len(spec.Vector) == 0 {
			return nil, fmt.Errorf("%w: hybrid needs a query or a vector", plugin.ErrInvalidInput)
		}
		hybrid := s.client.graphql.HybridArgumentBuilder().WithQuery(spec.Query)
		if len(spec.Vector) > 0 {
			hybrid = hybrid.WithVector(spec.Vector)
		}
		if spec.Alpha != nil {
			hybrid = hybrid.WithAlpha(float32(*spec.Alpha))
		}
		if len(spec.Properties) > 0 {
			hybrid = hybrid.WithProperties(spec.Properties)
		}
		switch strings.ToLower(strings.TrimSpace(spec.Fusion)) {
		case "":
		case "ranked", "rankedfusion":
			hybrid = hybrid.WithFusionType(graphql.Ranked)
		case "relativescore", "relative_score":
			hybrid = hybrid.WithFusionType(graphql.RelativeScore)
		default:
			return nil, fmt.Errorf("%w: fusion must be ranked or relativeScore", plugin.ErrInvalidInput)
		}
		if targetVector != "" {
			hybrid = hybrid.WithTargetVectors(targetVector)
		}
		return builder.WithHybrid(hybrid), nil
	}
}

func targetVectorFor(class *models.Class, name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", nil
	}
	for _, candidate := range namedVectorNames(class) {
		if candidate == name {
			return name, nil
		}
	}
	return "", fmt.Errorf("%w: %s has no named vector %q", plugin.ErrInvalidInput, class.Class, name)
}

var additionalByMode = map[string][]string{
	"fetch":      {"id", "vector", "creationTimeUnix", "lastUpdateTimeUnix"},
	"nearVector": {"id", "distance", "certainty", "creationTimeUnix"},
	"nearText":   {"id", "distance", "certainty", "creationTimeUnix"},
	"bm25":       {"id", "score", "explainScore", "creationTimeUnix"},
	"hybrid":     {"id", "score", "explainScore", "creationTimeUnix"},
}

var allowedAdditional = map[string]bool{
	"id": true, "vector": true, "distance": true, "certainty": true, "score": true,
	"explainScore": true, "creationTimeUnix": true, "lastUpdateTimeUnix": true, "isConsistent": true,
}

func additionalFor(spec searchSpec) ([]string, error) {
	if len(spec.Additional) == 0 {
		return additionalByMode[spec.Mode], nil
	}
	out := make([]string, 0, len(spec.Additional))
	for _, name := range spec.Additional {
		name = strings.TrimSpace(name)
		if !allowedAdditional[name] {
			return nil, fmt.Errorf("%w: unsupported _additional field %q", plugin.ErrInvalidInput, name)
		}
		out = append(out, name)
	}
	return out, nil
}

// buildWhere maps Weaviate's documented where-filter JSON onto the client's typed
// filter builder so the operator and values are never spliced into GraphQL text.
func buildWhere(raw json.RawMessage) (*filters.WhereBuilder, error) {
	var node struct {
		Operator     string            `json:"operator"`
		Path         []string          `json:"path"`
		Operands     []json.RawMessage `json:"operands"`
		ValueInt     []int64           `json:"valueInt"`
		ValueNumber  []float64         `json:"valueNumber"`
		ValueBoolean []bool            `json:"valueBoolean"`
		ValueString  []string          `json:"valueString"`
		ValueText    []string          `json:"valueText"`
		ValueDate    []string          `json:"valueDate"`
	}
	if err := json.Unmarshal(raw, &node); err != nil {
		if scalar, scalarErr := scalarWhere(raw); scalarErr == nil {
			return scalar, nil
		}
		return nil, fmt.Errorf("%w: where filter is not valid: %v", plugin.ErrInvalidInput, err)
	}
	operator, err := whereOperator(node.Operator)
	if err != nil {
		return nil, err
	}
	builder := filters.Where().WithOperator(operator)
	for _, segment := range node.Path {
		if _, err := validName(segment, propertyNamePattern, "filter path"); err != nil {
			if _, classErr := validName(segment, classNamePattern, "filter path"); classErr != nil {
				return nil, err
			}
		}
	}
	if len(node.Path) > 0 {
		builder = builder.WithPath(node.Path)
	}
	if len(node.Operands) > 0 {
		operands := make([]*filters.WhereBuilder, 0, len(node.Operands))
		for _, item := range node.Operands {
			operand, err := buildWhere(item)
			if err != nil {
				return nil, err
			}
			operands = append(operands, operand)
		}
		builder = builder.WithOperands(operands)
	}
	if len(node.ValueInt) > 0 {
		builder = builder.WithValueInt(node.ValueInt...)
	}
	if len(node.ValueNumber) > 0 {
		builder = builder.WithValueNumber(node.ValueNumber...)
	}
	if len(node.ValueBoolean) > 0 {
		builder = builder.WithValueBoolean(node.ValueBoolean...)
	}
	if len(node.ValueString) > 0 {
		builder = builder.WithValueString(node.ValueString...)
	}
	if len(node.ValueText) > 0 {
		builder = builder.WithValueText(node.ValueText...)
	}
	for _, value := range node.ValueDate {
		parsed, err := time.Parse(time.RFC3339, value)
		if err != nil {
			return nil, fmt.Errorf("%w: valueDate %q must be RFC3339", plugin.ErrInvalidInput, value)
		}
		builder = builder.WithValueDate(parsed)
	}
	return builder, nil
}

// scalarWhere accepts the single-value spelling (valueText: "x") alongside the
// list spelling the typed builder uses.
func scalarWhere(raw json.RawMessage) (*filters.WhereBuilder, error) {
	var node struct {
		Operator     string   `json:"operator"`
		Path         []string `json:"path"`
		ValueInt     *int64   `json:"valueInt"`
		ValueNumber  *float64 `json:"valueNumber"`
		ValueBoolean *bool    `json:"valueBoolean"`
		ValueString  *string  `json:"valueString"`
		ValueText    *string  `json:"valueText"`
		ValueDate    *string  `json:"valueDate"`
	}
	if err := json.Unmarshal(raw, &node); err != nil {
		return nil, err
	}
	operator, err := whereOperator(node.Operator)
	if err != nil {
		return nil, err
	}
	builder := filters.Where().WithOperator(operator)
	if len(node.Path) > 0 {
		builder = builder.WithPath(node.Path)
	}
	switch {
	case node.ValueInt != nil:
		builder = builder.WithValueInt(*node.ValueInt)
	case node.ValueNumber != nil:
		builder = builder.WithValueNumber(*node.ValueNumber)
	case node.ValueBoolean != nil:
		builder = builder.WithValueBoolean(*node.ValueBoolean)
	case node.ValueString != nil:
		builder = builder.WithValueString(*node.ValueString)
	case node.ValueText != nil:
		builder = builder.WithValueText(*node.ValueText)
	case node.ValueDate != nil:
		parsed, err := time.Parse(time.RFC3339, *node.ValueDate)
		if err != nil {
			return nil, fmt.Errorf("%w: valueDate must be RFC3339", plugin.ErrInvalidInput)
		}
		builder = builder.WithValueDate(parsed)
	}
	return builder, nil
}

var whereOperators = map[string]filters.WhereOperator{
	"and": filters.And, "or": filters.Or, "not": filters.Not,
	"equal": filters.Equal, "notequal": filters.NotEqual, "like": filters.Like,
	"greaterthan": filters.GreaterThan, "greaterthanequal": filters.GreaterThanEqual,
	"lessthan": filters.LessThan, "lessthanequal": filters.LessThanEqual,
	"withingeorange": filters.WithinGeoRange, "isnull": filters.IsNull,
	"containsany": filters.ContainsAny, "containsall": filters.ContainsAll, "containsnone": filters.ContainsNone,
}

func whereOperator(name string) (filters.WhereOperator, error) {
	operator, ok := whereOperators[strings.ToLower(strings.TrimSpace(name))]
	if !ok {
		return "", fmt.Errorf("%w: unsupported where operator %q", plugin.ErrInvalidInput, name)
	}
	return operator, nil
}

func graphqlErrors(resp *models.GraphQLResponse) error {
	if resp == nil || len(resp.Errors) == 0 {
		return nil
	}
	messages := make([]string, 0, len(resp.Errors))
	for _, item := range resp.Errors {
		if item != nil && item.Message != "" {
			messages = append(messages, item.Message)
		}
	}
	if len(messages) == 0 {
		return fmt.Errorf("%w: GraphQL request failed", plugin.ErrInvalidInput)
	}
	return fmt.Errorf("%w: %s", plugin.ErrInvalidInput, strings.Join(messages, "; "))
}

func getResultItems(resp *models.GraphQLResponse, className string) []map[string]any {
	if resp == nil {
		return nil
	}
	get, ok := resp.Data["Get"].(map[string]any)
	if !ok {
		return nil
	}
	items, ok := get[className].([]any)
	if !ok {
		return nil
	}
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if entry, ok := item.(map[string]any); ok {
			out = append(out, entry)
		}
	}
	return out
}

// graphqlTable flattens a GraphQL response into the columns/rows envelope the
// query editor renders, keeping the raw payload for non-tabular shapes.
func graphqlTable(resp *models.GraphQLResponse) row {
	records := make([]map[string]any, 0, 16)
	// Explore is a flat list of beacons; Get and Aggregate nest one list per collection.
	for _, item := range asList(resp.Data["Explore"]) {
		if entry, ok := item.(map[string]any); ok {
			records = append(records, flatten(entry, ""))
		}
	}
	for _, section := range []string{"Get", "Aggregate"} {
		block, ok := resp.Data[section].(map[string]any)
		if !ok {
			continue
		}
		names := make([]string, 0, len(block))
		for name := range block {
			names = append(names, name)
		}
		sort.Strings(names)
		multi := len(names) > 1
		for _, name := range names {
			items, ok := block[name].([]any)
			if !ok {
				continue
			}
			for _, item := range items {
				entry, ok := item.(map[string]any)
				if !ok {
					continue
				}
				flat := flatten(entry, "")
				if multi {
					flat["_collection"] = name
				}
				records = append(records, flat)
			}
		}
	}
	if len(records) == 0 {
		data := map[string]any{}
		for key, value := range resp.Data {
			data[key] = value
		}
		return row{"columns": []string{"result"}, "rows": [][]any{{jsonText(data)}}, "rowCount": 0}
	}
	columns := orderedColumns(records)
	rows := make([][]any, 0, len(records))
	for _, record := range records {
		cells := make([]any, len(columns))
		for i, column := range columns {
			cells[i] = record[column]
		}
		rows = append(rows, cells)
	}
	return row{"columns": columns, "rows": rows, "rowCount": len(rows)}
}

func asList(value any) []any {
	items, _ := value.([]any)
	return items
}

func flatten(entry map[string]any, prefix string) map[string]any {
	out := map[string]any{}
	for key, value := range entry {
		name := key
		if prefix != "" {
			name = prefix + "." + key
		}
		if key == "_additional" {
			if nested, ok := value.(map[string]any); ok {
				for child, childValue := range flatten(nested, "") {
					out[child] = childValue
				}
				continue
			}
		}
		switch typed := value.(type) {
		case map[string]any:
			for child, childValue := range flatten(typed, name) {
				out[child] = childValue
			}
		case []any:
			out[name] = jsonText(typed)
		default:
			out[name] = typed
		}
	}
	return out
}

var columnPriority = map[string]int{"id": 0, "_collection": 1, "score": 2, "distance": 3, "certainty": 4}

func orderedColumns(records []map[string]any) []string {
	seen := map[string]bool{}
	for _, record := range records {
		for key := range record {
			seen[key] = true
		}
	}
	columns := make([]string, 0, len(seen))
	for key := range seen {
		columns = append(columns, key)
	}
	sort.Slice(columns, func(i, j int) bool {
		pi, iOK := columnPriority[columns[i]]
		pj, jOK := columnPriority[columns[j]]
		switch {
		case iOK && jOK:
			return pi < pj
		case iOK:
			return true
		case jOK:
			return false
		default:
			return columns[i] < columns[j]
		}
	})
	return columns
}

func completeQuery(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	items := make([]sqldb.CompletionItem, 0, 64)
	for _, keyword := range []string{"Get", "Aggregate", "Explore", "limit", "offset", "where", "sort", "nearVector", "nearText", "hybrid", "bm25", "groupBy", "tenant", "autocut", "_additional"} {
		items = append(items, sqldb.CompletionItem{Label: keyword, Type: "keyword"})
	}
	for _, field := range []string{"id", "vector", "distance", "certainty", "score", "explainScore", "creationTimeUnix", "lastUpdateTimeUnix"} {
		items = append(items, sqldb.CompletionItem{Label: field, Type: "property", Detail: "_additional"})
	}
	dump, err := s.client.schema.Getter().Do(rc.Ctx)
	if err != nil {
		return nil, mapError(err)
	}
	scope := strings.TrimSpace(rc.Param("collection"))
	for _, class := range sortedClasses(dump.Classes) {
		items = append(items, sqldb.CompletionItem{Label: class.Class, Type: "class", Detail: primaryVectorizer(class)})
		if scope != "" && class.Class != scope {
			continue
		}
		for _, property := range sortedProperties(class) {
			items = append(items, sqldb.CompletionItem{
				Label:  property.Name,
				Type:   "property",
				Detail: class.Class + ": " + strings.Join(property.DataType, "|"),
			})
		}
	}
	return plugin.Page[sqldb.CompletionItem]{Items: items}, nil
}

func singleLine(value string) string {
	return strings.Join(strings.Fields(strings.ReplaceAll(value, "\n", " ")), " ")
}
