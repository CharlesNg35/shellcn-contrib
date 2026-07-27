package pinecone

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/url"
	"strings"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type indexList struct {
	Indexes []indexInfo `json:"indexes"`
}

type indexInfo struct {
	Name               string            `json:"name"`
	Dimension          *int              `json:"dimension"`
	Metric             string            `json:"metric"`
	Host               string            `json:"host"`
	PrivateHost        string            `json:"private_host"`
	VectorType         string            `json:"vector_type"`
	DeletionProtection string            `json:"deletion_protection"`
	Tags               map[string]string `json:"tags"`
	Status             indexStatus       `json:"status"`
	Spec               indexSpec         `json:"spec"`
	Embed              map[string]any    `json:"embed"`
}

type indexStatus struct {
	Ready bool   `json:"ready"`
	State string `json:"state"`
}

type indexSpec struct {
	Serverless *serverlessSpec `json:"serverless"`
	Pod        *podSpec        `json:"pod"`
	BYOC       *byocSpec       `json:"byoc"`
}

type serverlessSpec struct {
	Cloud            string         `json:"cloud"`
	Region           string         `json:"region"`
	SourceCollection string         `json:"source_collection"`
	ReadCapacity     map[string]any `json:"read_capacity"`
}

type podSpec struct {
	Environment      string         `json:"environment"`
	PodType          string         `json:"pod_type"`
	Pods             int            `json:"pods"`
	Replicas         int            `json:"replicas"`
	Shards           int            `json:"shards"`
	SourceCollection string         `json:"source_collection"`
	MetadataConfig   map[string]any `json:"metadata_config"`
}

type byocSpec struct {
	Environment string `json:"environment"`
}

func (i indexInfo) deployment() string {
	switch {
	case i.Spec.Serverless != nil:
		return "serverless"
	case i.Spec.Pod != nil:
		return "pod"
	case i.Spec.BYOC != nil:
		return "byoc"
	default:
		return "unknown"
	}
}

func (i indexInfo) location() string {
	switch {
	case i.Spec.Serverless != nil:
		return strings.TrimSpace(i.Spec.Serverless.Cloud + " " + i.Spec.Serverless.Region)
	case i.Spec.Pod != nil:
		return i.Spec.Pod.Environment
	case i.Spec.BYOC != nil:
		return i.Spec.BYOC.Environment
	default:
		return ""
	}
}

func (i indexInfo) ref() plugin.ResourceIdentity {
	return plugin.ResourceIdentity{Kind: "index", Name: i.Name, UID: i.Name}
}

func (i indexInfo) row() row {
	out := row{
		"ref":                 i.ref(),
		"name":                i.Name,
		"metric":              i.Metric,
		"vector_type":         orDefault(i.VectorType, "dense"),
		"host":                i.Host,
		"private_host":        i.PrivateHost,
		"deployment":          i.deployment(),
		"location":            i.location(),
		"state":               orDefault(i.Status.State, "Unknown"),
		"ready":               i.Status.Ready,
		"deletion_protection": orDefault(i.DeletionProtection, "disabled"),
		"tags":                i.Tags,
		"dimension":           0,
	}
	if i.Dimension != nil {
		out["dimension"] = *i.Dimension
	}
	if pod := i.Spec.Pod; pod != nil {
		out["pod_type"] = pod.PodType
		out["pods"] = pod.Pods
		out["replicas"] = pod.Replicas
		out["shards"] = pod.Shards
		out["source_collection"] = pod.SourceCollection
	}
	if sl := i.Spec.Serverless; sl != nil {
		out["cloud"] = sl.Cloud
		out["region"] = sl.Region
		out["source_collection"] = sl.SourceCollection
		out["read_capacity"] = sl.ReadCapacity
	}
	if i.Embed != nil {
		out["embed"] = i.Embed
	}
	return out
}

type collectionList struct {
	Collections []collectionInfo `json:"collections"`
}

type collectionInfo struct {
	Name        string `json:"name"`
	Size        int64  `json:"size"`
	Status      string `json:"status"`
	Dimension   int    `json:"dimension"`
	VectorCount int    `json:"vector_count"`
	Environment string `json:"environment"`
}

func (c collectionInfo) row() row {
	return row{
		"ref":          plugin.ResourceIdentity{Kind: "collection", Name: c.Name, UID: c.Name},
		"name":         c.Name,
		"size":         c.Size,
		"status":       orDefault(c.Status, "Unknown"),
		"dimension":    c.Dimension,
		"vector_count": c.VectorCount,
		"environment":  c.Environment,
	}
}

type indexStats struct {
	Namespaces       map[string]namespaceStats `json:"namespaces"`
	Dimension        int                       `json:"dimension"`
	IndexFullness    float64                   `json:"indexFullness"`
	TotalVectorCount int64                     `json:"totalVectorCount"`
	VectorType       string                    `json:"vectorType"`
	Metric           string                    `json:"metric"`
}

type namespaceStats struct {
	VectorCount int64 `json:"vectorCount"`
}

type namespaceList struct {
	Namespaces []namespaceInfo `json:"namespaces"`
	Pagination pagination      `json:"pagination"`
}

type namespaceInfo struct {
	Name        string `json:"name"`
	RecordCount int64  `json:"record_count"`
}

func (n namespaceInfo) row(index string) row {
	name := n.Name
	if name == "" {
		name = defaultNamespace
	}
	return row{
		"ref":          plugin.ResourceIdentity{Kind: "namespace", Scope: index, Name: name, UID: index + "/" + name},
		"name":         name,
		"record_count": n.RecordCount,
		"index":        index,
	}
}

type pagination struct {
	Next string `json:"next"`
}

type vectorIDList struct {
	Vectors    []vectorID `json:"vectors"`
	Pagination pagination `json:"pagination"`
	Namespace  string     `json:"namespace"`
	Usage      *usage     `json:"usage"`
}

type vectorID struct {
	ID string `json:"id"`
}

type usage struct {
	ReadUnits  int `json:"readUnits"`
	WriteUnits int `json:"writeUnits"`
}

type sparseValues struct {
	Indices []int64   `json:"indices"`
	Values  []float64 `json:"values"`
}

// vectorRecord is both the wire shape Pinecone returns and the one an upsert
// sends, so the optional members are omitted rather than encoded as null: a
// sparse-only record must not carry "values": null.
type vectorRecord struct {
	ID           string         `json:"id"`
	Values       []float64      `json:"values,omitempty"`
	SparseValues *sparseValues  `json:"sparseValues,omitempty"`
	Metadata     map[string]any `json:"metadata,omitempty"`
}

type fetchResult struct {
	Vectors   map[string]vectorRecord `json:"vectors"`
	Namespace string                  `json:"namespace"`
	Usage     *usage                  `json:"usage"`
}

type queryResult struct {
	Matches   []queryMatch `json:"matches"`
	Namespace string       `json:"namespace"`
	Usage     *usage       `json:"usage"`
}

type queryMatch struct {
	vectorRecord
	Score float64 `json:"score"`
}

func vectorRef(index, namespace, id string) plugin.ResourceIdentity {
	return plugin.ResourceIdentity{
		Kind:      "vector",
		Scope:     index,
		Namespace: namespace,
		Name:      id,
		UID:       index + "/" + namespace + "/" + id,
	}
}

func (v vectorRecord) row(index, namespace string) row {
	out := row{
		"ref":       vectorRef(index, namespace, v.ID),
		"id":        v.ID,
		"index":     index,
		"namespace": namespace,
		"metadata":  v.Metadata,
	}
	if v.Metadata == nil {
		out["metadata"] = map[string]any{}
	}
	if len(v.Values) > 0 {
		out["values"] = v.Values
		out["dimension"] = len(v.Values)
		out["norm"] = round(norm(v.Values), 6)
	}
	if v.SparseValues != nil && len(v.SparseValues.Indices) > 0 {
		out["sparse_values"] = v.SparseValues
		out["sparse_terms"] = len(v.SparseValues.Indices)
	}
	return out
}

func (s *Session) listIndexes(ctx context.Context) ([]indexInfo, error) {
	var out indexList
	if err := s.control.do(ctx, http.MethodGet, "/indexes", nil, nil, &out); err != nil {
		return nil, err
	}
	return out.Indexes, nil
}

func (s *Session) describeIndex(ctx context.Context, name string) (indexInfo, error) {
	if err := validateName("index", name); err != nil {
		return indexInfo{}, err
	}
	var out indexInfo
	if err := s.control.do(ctx, http.MethodGet, "/indexes/"+url.PathEscape(name), nil, nil, &out); err != nil {
		return indexInfo{}, err
	}
	if out.Name == "" {
		out.Name = name
	}
	return out, nil
}

func (s *Session) listCollections(ctx context.Context) ([]collectionInfo, error) {
	var out collectionList
	if err := s.control.do(ctx, http.MethodGet, "/collections", nil, nil, &out); err != nil {
		return nil, err
	}
	return out.Collections, nil
}

func (s *Session) stats(ctx context.Context, index string) (indexStats, error) {
	client, err := s.indexClient(ctx, index)
	if err != nil {
		return indexStats{}, err
	}
	var out indexStats
	err = client.do(ctx, http.MethodPost, "/describe_index_stats", nil, map[string]any{}, &out)
	return out, err
}

// namespaceRows reads every namespace from describe_index_stats. Pinecone's
// /namespaces listing is serverless-only, but the stats call answers for pod
// indexes too, so it backs the listing when the endpoint is unavailable.
func (s *Session) namespaceRows(ctx context.Context, index string) ([]row, error) {
	stats, err := s.stats(ctx, index)
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(stats.Namespaces))
	for name, value := range stats.Namespaces {
		rows = append(rows, namespaceInfo{Name: name, RecordCount: value.VectorCount}.row(index))
	}
	return rows, nil
}

// unsupportedEndpoint reports whether Pinecone refused a call because the index
// does not serve that endpoint rather than because the request was wrong.
func unsupportedEndpoint(err error) bool {
	return errors.Is(err, plugin.ErrNotFound) ||
		errors.Is(err, plugin.ErrInvalidInput) ||
		errors.Is(err, plugin.ErrNotSupported)
}

// fetchVectors reads records by id. The ids ride the query string, so requests
// are chunked well below Pinecone's 1000-id ceiling.
func (s *Session) fetchVectors(ctx context.Context, client *restClient, namespace string, ids []string) (map[string]vectorRecord, error) {
	out := make(map[string]vectorRecord, len(ids))
	for chunk := range chunks(ids, fetchBatchSize) {
		query := url.Values{}
		for _, id := range chunk {
			query.Add("ids", id)
		}
		if namespace != "" {
			query.Set("namespace", namespace)
		}
		var page fetchResult
		if err := client.do(ctx, http.MethodGet, "/vectors/fetch", query, nil, &page); err != nil {
			return nil, err
		}
		for id, record := range page.Vectors {
			if record.ID == "" {
				record.ID = id
			}
			out[record.ID] = record
		}
	}
	return out, nil
}

const (
	// fetchBatchSize keeps a fetch URL well under any proxy's query-string limit.
	fetchBatchSize = 100
	// writeBatchSize is Pinecone's per-request ceiling for upserted vectors and
	// for deleted ids.
	writeBatchSize = 1000
)

func chunks[T any](items []T, size int) func(func([]T) bool) {
	return func(yield func([]T) bool) {
		for start := 0; start < len(items); start += size {
			end := min(start+size, len(items))
			if !yield(items[start:end]) {
				return
			}
		}
	}
}

func (s *Session) queryVectors(ctx context.Context, client *restClient, body map[string]any) (queryResult, error) {
	var out queryResult
	err := client.do(ctx, http.MethodPost, "/query", nil, body, &out)
	return out, err
}

func orDefault(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func norm(values []float64) float64 {
	total := 0.0
	for _, v := range values {
		total += v * v
	}
	return math.Sqrt(total)
}

func round(v float64, digits int) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	scale := math.Pow(10, float64(digits))
	return math.Round(v*scale) / scale
}

func percent(v float64) float64 { return round(v*100, 4) }
