package chroma

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strings"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

// scope is the tenant/database pair a request addresses. Params win over the
// connection defaults so one connection can browse the whole server.
type scope struct {
	Tenant   string
	Database string
}

func (s scope) databasesPath() string { return "/tenants/" + url.PathEscape(s.Tenant) + "/databases" }

func (s scope) path() string { return s.databasesPath() + "/" + url.PathEscape(s.Database) }

func (s scope) collectionsPath() string { return s.path() + "/collections" }

func (s scope) collectionPath(id string) string {
	return s.collectionsPath() + "/" + url.PathEscape(id)
}

func scopeOf(rc *plugin.RequestContext, s *Session) (scope, error) {
	tenant, database := strings.TrimSpace(rc.Param("tenant")), strings.TrimSpace(rc.Param("database"))
	if raw := strings.TrimSpace(rc.Param("scope")); raw != "" {
		outer, inner, ok := strings.Cut(raw, "/")
		if tenant == "" {
			tenant = outer
		}
		if ok && database == "" {
			database = inner
		}
	}
	if tenant == "" {
		tenant = s.opts.Tenant
	}
	if database == "" {
		database = s.opts.Database
	}
	if err := validateName("tenant", tenant); err != nil {
		return scope{}, err
	}
	if err := validateName("database", database); err != nil {
		return scope{}, err
	}
	return scope{Tenant: tenant, Database: database}, nil
}

func session(rc *plugin.RequestContext) (*Session, error) { return unwrap(rc.Session) }

// sessionScope resolves the session and the addressed tenant/database together,
// which every collection and record route needs.
func sessionScope(rc *plugin.RequestContext) (*Session, scope, error) {
	s, err := session(rc)
	if err != nil {
		return nil, scope{}, err
	}
	sc, err := scopeOf(rc, s)
	if err != nil {
		return nil, scope{}, err
	}
	return s, sc, nil
}

func (s *Session) do(ctx context.Context, method, path string, query url.Values, body, out any) error {
	return s.client.Do(ctx, method, apiPrefix+path, query, body, out)
}

func (s *Session) ensureWritable() error {
	if s.opts.ReadOnly {
		return fmt.Errorf("%w: connection is read-only", plugin.ErrForbidden)
	}
	return nil
}

// collection is the subset of the Chroma v2 collection object the UI renders.
type collection struct {
	ID            string         `json:"id"`
	Name          string         `json:"name"`
	Tenant        string         `json:"tenant"`
	Database      string         `json:"database"`
	Dimension     *int           `json:"dimension"`
	Metadata      map[string]any `json:"metadata"`
	LogPosition   int64          `json:"log_position"`
	Version       int            `json:"version"`
	Configuration configuration  `json:"configuration_json"`
	Schema        *schemaDoc     `json:"schema"`
}

type configuration struct {
	HNSW              *indexParams   `json:"hnsw"`
	Spann             *indexParams   `json:"spann"`
	EmbeddingFunction map[string]any `json:"embedding_function"`
}

type indexParams struct {
	Space          string   `json:"space"`
	EFConstruction *int     `json:"ef_construction"`
	EFSearch       *int     `json:"ef_search"`
	MaxNeighbors   *int     `json:"max_neighbors"`
	ResizeFactor   *float64 `json:"resize_factor"`
	SyncThreshold  *int     `json:"sync_threshold"`
	SearchNprobe   *int     `json:"search_nprobe"`
	WriteNprobe    *int     `json:"write_nprobe"`
	SplitThreshold *int     `json:"split_threshold"`
	MergeThreshold *int     `json:"merge_threshold"`
}

type schemaDoc struct {
	Defaults map[string]map[string]indexEntry            `json:"defaults"`
	Keys     map[string]map[string]map[string]indexEntry `json:"keys"`
}

type indexEntry struct {
	Enabled bool           `json:"enabled"`
	Config  map[string]any `json:"config"`
}

// space reports the configured distance space, defaulting to Chroma's own l2.
func (c collection) space() string {
	if c.Configuration.HNSW != nil && c.Configuration.HNSW.Space != "" {
		return c.Configuration.HNSW.Space
	}
	if c.Configuration.Spann != nil && c.Configuration.Spann.Space != "" {
		return c.Configuration.Spann.Space
	}
	return spaceL2
}

func (c collection) indexKind() string {
	if c.Configuration.Spann != nil {
		return "spann"
	}
	if c.Configuration.HNSW != nil {
		return "hnsw"
	}
	return "unknown"
}

func (c collection) params() *indexParams {
	if c.Configuration.Spann != nil {
		return c.Configuration.Spann
	}
	return c.Configuration.HNSW
}

func (c collection) ref() plugin.ResourceIdentity {
	return plugin.ResourceIdentity{Kind: "collection", Scope: c.Tenant, Namespace: c.Database, Name: c.Name, UID: c.ID}
}

func (c collection) row() row {
	out := row{
		"ref":          c.ref(),
		"id":           c.ID,
		"name":         c.Name,
		"tenant":       c.Tenant,
		"database":     c.Database,
		"metadata":     c.Metadata,
		"space":        c.space(),
		"index":        c.indexKind(),
		"version":      c.Version,
		"log_position": c.LogPosition,
		"embedded":     c.Configuration.EmbeddingFunction != nil,
		"dimension":    0,
	}
	if c.Dimension != nil {
		out["dimension"] = *c.Dimension
	}
	if params := c.params(); params != nil {
		out["ef_construction"] = intOrZero(params.EFConstruction)
		out["ef_search"] = intOrZero(params.EFSearch)
		out["max_neighbors"] = intOrZero(params.MaxNeighbors)
	}
	return out
}

func intOrZero(v *int) int {
	if v == nil {
		return 0
	}
	return *v
}

// isUUID reports whether a path segment is a Chroma collection id. The v2 API
// splits lookups: `/collections/{name}` resolves names, `/collections/by-id/{id}`
// resolves ids, and record routes accept only the id.
func isUUID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for i, r := range value {
		switch i {
		case 8, 13, 18, 23:
			if r != '-' {
				return false
			}
		default:
			isHex := (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')
			if !isHex {
				return false
			}
		}
	}
	return true
}

// getCollection resolves a collection by name or id and returns both, so each
// handler can address the endpoint that Chroma actually accepts.
func (s *Session) getCollection(ctx context.Context, sc scope, ref string) (collection, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return collection{}, fmt.Errorf("%w: collection is required", plugin.ErrInvalidInput)
	}
	if err := validateName("collection", ref); err != nil {
		return collection{}, err
	}
	path := sc.collectionPath(ref)
	if isUUID(ref) {
		path = sc.collectionsPath() + "/by-id/" + url.PathEscape(ref)
	}
	var out collection
	if err := s.do(ctx, http.MethodGet, path, nil, nil, &out); err != nil {
		return collection{}, err
	}
	if out.ID == "" {
		return collection{}, fmt.Errorf("%w: collection %q", plugin.ErrNotFound, ref)
	}
	if out.Tenant == "" {
		out.Tenant = sc.Tenant
	}
	if out.Database == "" {
		out.Database = sc.Database
	}
	return out, nil
}

func collectionOf(rc *plugin.RequestContext) (*Session, scope, collection, error) {
	s, sc, err := sessionScope(rc)
	if err != nil {
		return nil, scope{}, collection{}, err
	}
	col, err := s.getCollection(rc.Ctx, sc, rc.Param("collection"))
	if err != nil {
		return nil, scope{}, collection{}, err
	}
	return s, sc, col, nil
}

func (s *Session) count(ctx context.Context, sc scope, id string) (int, error) {
	var out int
	err := s.do(ctx, http.MethodGet, sc.collectionPath(id)+"/count", nil, nil, &out)
	return out, err
}

// recordSet is Chroma's columnar record payload for /get.
type recordSet struct {
	IDs        []string         `json:"ids"`
	Documents  []*string        `json:"documents"`
	Metadatas  []map[string]any `json:"metadatas"`
	Embeddings [][]float64      `json:"embeddings"`
	URIs       []*string        `json:"uris"`
	Include    []string         `json:"include"`
}

// queryResult is Chroma's columnar /query payload: one outer entry per query
// embedding.
type queryResult struct {
	IDs        [][]string         `json:"ids"`
	Documents  [][]*string        `json:"documents"`
	Metadatas  [][]map[string]any `json:"metadatas"`
	Embeddings [][][]float64      `json:"embeddings"`
	Distances  [][]*float64       `json:"distances"`
	URIs       [][]*string        `json:"uris"`
}

func (q queryResult) flatten() (recordSet, []*float64) {
	out := recordSet{}
	var distances []*float64
	for i := range q.IDs {
		out.IDs = append(out.IDs, q.IDs[i]...)
		if i < len(q.Documents) {
			out.Documents = append(out.Documents, q.Documents[i]...)
		}
		if i < len(q.Metadatas) {
			out.Metadatas = append(out.Metadatas, q.Metadatas[i]...)
		}
		if i < len(q.Embeddings) {
			out.Embeddings = append(out.Embeddings, q.Embeddings[i]...)
		}
		if i < len(q.URIs) {
			out.URIs = append(out.URIs, q.URIs[i]...)
		}
		if i < len(q.Distances) {
			distances = append(distances, q.Distances[i]...)
		}
	}
	return out, distances
}

func (r recordSet) len() int { return len(r.IDs) }

func (r recordSet) document(i int) string {
	if i < len(r.Documents) && r.Documents[i] != nil {
		return *r.Documents[i]
	}
	return ""
}

func (r recordSet) uri(i int) string {
	if i < len(r.URIs) && r.URIs[i] != nil {
		return *r.URIs[i]
	}
	return ""
}

func (r recordSet) metadata(i int) map[string]any {
	if i < len(r.Metadatas) && r.Metadatas[i] != nil {
		return r.Metadatas[i]
	}
	return map[string]any{}
}

func (r recordSet) embedding(i int) []float64 {
	if i < len(r.Embeddings) {
		return r.Embeddings[i]
	}
	return nil
}

func (r recordSet) row(i int, col collection) row {
	vector := r.embedding(i)
	out := row{
		"ref": plugin.ResourceIdentity{
			Kind:      "record",
			Scope:     col.Tenant + "/" + col.Database,
			Namespace: col.ID,
			Name:      r.IDs[i],
			UID:       col.ID + "/" + r.IDs[i],
		},
		"id":         r.IDs[i],
		"document":   r.document(i),
		"metadata":   r.metadata(i),
		"uri":        r.uri(i),
		"collection": col.Name,
	}
	if len(vector) > 0 {
		out["embedding"] = vector
		out["dimension"] = len(vector)
		out["norm"] = round(norm(vector), 6)
	}
	return out
}

func (r recordSet) rows(col collection) []row {
	out := make([]row, 0, r.len())
	for i := range r.IDs {
		out = append(out, r.row(i, col))
	}
	return out
}

func round(v float64, digits int) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	scale := math.Pow(10, float64(digits))
	return math.Round(v*scale) / scale
}
