package pinecone

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

// statsConcurrency bounds the in-flight per-index describe_index_stats calls.
const statsConcurrency = 6

func Routes() []plugin.Route {
	return []plugin.Route{
		{ID: rid("overview"), Method: plugin.MethodGet, Path: "/overview", Permission: "pinecone.project.read", Risk: plugin.RiskSafe, AuditEvent: rid("overview"), Handle: overview},

		{ID: rid("indexes.tree"), Method: plugin.MethodGet, Path: "/tree/indexes", Permission: "pinecone.indexes.read", Risk: plugin.RiskSafe, AuditEvent: rid("indexes.tree"), Handle: indexesTree},
		{ID: rid("indexes.list"), Method: plugin.MethodGet, Path: "/indexes", Permission: "pinecone.indexes.read", Risk: plugin.RiskSafe, AuditEvent: rid("indexes.list"), Handle: indexesList},
		{ID: rid("index.read"), Method: plugin.MethodGet, Path: "/indexes/{index}", Permission: "pinecone.indexes.read", Risk: plugin.RiskSafe, AuditEvent: rid("index.read"), Handle: indexRead},
		{ID: rid("index.stats"), Method: plugin.MethodGet, Path: "/indexes/{index}/stats", Permission: "pinecone.indexes.read", Risk: plugin.RiskSafe, AuditEvent: rid("index.stats"), Handle: indexStatsRead},
		{ID: rid("index.metrics"), Method: plugin.MethodWS, Path: "/indexes/{index}/metrics", Permission: "pinecone.indexes.read", Risk: plugin.RiskSafe, AuditEvent: rid("index.metrics"), Stream: indexMetrics},
		{ID: rid("index.create"), Method: plugin.MethodPost, Path: "/indexes", Permission: "pinecone.indexes.write", Risk: plugin.RiskWrite, AuditEvent: rid("index.create"), Input: indexCreateSchema(), Handle: indexCreate},
		{ID: rid("index.configure"), Method: plugin.MethodPatch, Path: "/indexes/{index}", Permission: "pinecone.indexes.write", Risk: plugin.RiskWrite, AuditEvent: rid("index.configure"), Input: indexConfigureSchema(), Handle: indexConfigure},
		{ID: rid("index.delete"), Method: plugin.MethodDelete, Path: "/indexes/{index}", Permission: "pinecone.indexes.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("index.delete"), Handle: indexDelete},

		{ID: rid("collections.tree"), Method: plugin.MethodGet, Path: "/tree/collections", Permission: "pinecone.collections.read", Risk: plugin.RiskSafe, AuditEvent: rid("collections.tree"), Handle: collectionsTree},
		{ID: rid("collections.list"), Method: plugin.MethodGet, Path: "/collections", Permission: "pinecone.collections.read", Risk: plugin.RiskSafe, AuditEvent: rid("collections.list"), Handle: collectionsList},
		{ID: rid("collection.read"), Method: plugin.MethodGet, Path: "/collections/{collection}", Permission: "pinecone.collections.read", Risk: plugin.RiskSafe, AuditEvent: rid("collection.read"), Handle: collectionRead},
		{ID: rid("collection.create"), Method: plugin.MethodPost, Path: "/collections", Permission: "pinecone.collections.write", Risk: plugin.RiskWrite, AuditEvent: rid("collection.create"), Input: collectionCreateSchema(), Handle: collectionCreate},
		{ID: rid("collection.delete"), Method: plugin.MethodDelete, Path: "/collections/{collection}", Permission: "pinecone.collections.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("collection.delete"), Handle: collectionDelete},

		{ID: rid("namespaces.list"), Method: plugin.MethodGet, Path: "/indexes/{index}/namespaces", Permission: "pinecone.namespaces.read", Risk: plugin.RiskSafe, AuditEvent: rid("namespaces.list"), Handle: namespacesList},
		{ID: rid("namespace.read"), Method: plugin.MethodGet, Path: "/indexes/{index}/namespaces/{namespace}", Permission: "pinecone.namespaces.read", Risk: plugin.RiskSafe, AuditEvent: rid("namespace.read"), Handle: namespaceRead},
		{ID: rid("namespace.delete"), Method: plugin.MethodDelete, Path: "/indexes/{index}/namespaces/{namespace}", Permission: "pinecone.namespaces.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("namespace.delete"), Handle: namespaceDelete},

		{ID: rid("vectors.list"), Method: plugin.MethodGet, Path: "/indexes/{index}/vectors", Permission: "pinecone.vectors.read", Risk: plugin.RiskSafe, AuditEvent: rid("vectors.list"), Handle: vectorsList},
		{ID: rid("vector.read"), Method: plugin.MethodGet, Path: "/indexes/{index}/vectors/{vector}", Permission: "pinecone.vectors.read", Risk: plugin.RiskSafe, AuditEvent: rid("vector.read"), Handle: vectorRead},
		{ID: rid("vector.neighbors"), Method: plugin.MethodGet, Path: "/indexes/{index}/vectors/{vector}/neighbors", Permission: "pinecone.vectors.read", Risk: plugin.RiskSafe, AuditEvent: rid("vector.neighbors"), Handle: vectorNeighbors},
		{ID: rid("vectors.upsert"), Method: plugin.MethodPost, Path: "/indexes/{index}/vectors", Permission: "pinecone.vectors.write", Risk: plugin.RiskWrite, AuditEvent: rid("vectors.upsert"), Handle: vectorsUpsert},
		{ID: rid("vectors.delete"), Method: plugin.MethodDelete, Path: "/indexes/{index}/vectors", Permission: "pinecone.vectors.delete", Risk: plugin.RiskDestructive, AuditEvent: rid("vectors.delete"), Handle: vectorsDelete},

		{ID: rid("query"), Method: plugin.MethodWS, Path: "/indexes/{index}/query", Permission: "pinecone.query.execute", Risk: plugin.RiskSafe, AuditEvent: rid("query"), Stream: queryStream},
		{ID: rid("query.complete"), Method: plugin.MethodGet, Path: "/indexes/{index}/completions", Permission: "pinecone.query.execute", Risk: plugin.RiskSafe, AuditEvent: rid("query.complete"), Handle: queryComplete},
	}
}

func overview(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	out := row{
		"ref":        serverRef(),
		"endpoint":   s.opts.Endpoint,
		"apiVersion": s.opts.APIVersion,
		"namespace":  s.opts.Namespace,
		"readOnly":   s.opts.ReadOnly,
		"privateNet": s.opts.PrivateNet,
		"status":     "unreachable",
	}
	indexes, err := s.listIndexes(rc.Ctx)
	if err == nil {
		ready, serverless, pods := 0, 0, 0
		for _, idx := range indexes {
			if idx.Status.Ready {
				ready++
			}
			switch idx.deployment() {
			case "serverless":
				serverless++
			case "pod":
				pods++
			}
		}
		out["indexes"] = len(indexes)
		out["readyIndexes"] = ready
		out["serverlessIndexes"] = serverless
		out["podIndexes"] = pods
		out["status"] = "ready"
		if ready < len(indexes) {
			out["status"] = "degraded"
		}
	}
	if collections, err := s.listCollections(rc.Ctx); err == nil {
		out["collections"] = len(collections)
	}
	return out, nil
}

func indexesTree(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	indexes, err := s.listIndexes(rc.Ctx)
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(indexes))
	for _, idx := range indexes {
		rows = append(rows, idx.row())
	}
	req, err := rc.Page()
	if err != nil {
		return nil, err
	}
	page, err := pageRows(req, plugin.FilterRows(rows, req.Search()), false)
	if err != nil {
		return nil, err
	}
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name := fmt.Sprint(item["name"])
		ref := plugin.ResourceIdentity{Kind: "index", Name: name, UID: name}
		nodes = append(nodes, plugin.TreeNode{
			Key:   "index:" + name,
			Label: name,
			Icon:  icon("box"),
			Ref:   &ref,
			Leaf:  true,
			Data:  map[string]any{"state": item["state"], "deployment": item["deployment"]},
		})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

func indexesList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	req, err := rc.Page()
	if err != nil {
		return nil, err
	}
	indexes, err := s.listIndexes(rc.Ctx)
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(indexes))
	for _, idx := range indexes {
		rows = append(rows, idx.row())
	}
	// Vector counts cost one data-plane call per index, so they are filled in for
	// the page only — unless the grid searches or orders by them, which spans
	// every page and therefore has to happen before paging.
	if req.Search() != "" || sortsOnStats(req.Sort) {
		truncated := false
		if len(rows) > scanLimit {
			rows, truncated = rows[:scanLimit], true
		}
		s.statsInto(rc.Ctx, rows)
		page, err := pageRows(req, plugin.FilterRows(rows, req.Search()), truncated)
		if err != nil {
			return nil, err
		}
		return capped(page, truncated), nil
	}
	page, err := pageRows(req, rows, false)
	if err != nil {
		return nil, err
	}
	s.statsInto(rc.Ctx, page.Items)
	return capped(page, false), nil
}

// sortsOnStats reports whether the grid ordered by a column only
// describe_index_stats can fill in.
func sortsOnStats(keys []plugin.SortKey) bool {
	for _, key := range keys {
		switch key.Field {
		case "vectors", "namespaces", "fullness":
			return true
		}
	}
	return false
}

// statsInto fills vector counts for one page of rows; each goroutine owns one
// row, so the maps are never shared.
func (s *Session) statsInto(ctx context.Context, rows []row) {
	sem := make(chan struct{}, statsConcurrency)
	var wg sync.WaitGroup
	for _, item := range rows {
		name, _ := item["name"].(string)
		if name == "" {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(item row, name string) {
			defer wg.Done()
			defer func() { <-sem }()
			stats, err := s.stats(ctx, name)
			if err != nil {
				return
			}
			item["vectors"] = stats.TotalVectorCount
			item["namespaces"] = len(stats.Namespaces)
			item["fullness"] = percent(stats.IndexFullness)
		}(item, name)
	}
	wg.Wait()
}

func indexRead(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	name, err := indexOf(rc)
	if err != nil {
		return nil, err
	}
	idx, err := s.describeIndex(rc.Ctx, name)
	if err != nil {
		return nil, err
	}
	out := idx.row()
	out["spec"] = idx.Spec
	if stats, err := s.stats(rc.Ctx, name); err == nil {
		mergeStats(out, stats)
	}
	return out, nil
}

func mergeStats(out row, stats indexStats) {
	out["vectors"] = stats.TotalVectorCount
	out["namespaces"] = len(stats.Namespaces)
	out["fullness"] = percent(stats.IndexFullness)
	if stats.Dimension > 0 {
		out["dimension"] = stats.Dimension
		out["estimated_vector_bytes"] = stats.TotalVectorCount * int64(stats.Dimension) * 4
	}
}

func indexStatsRead(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	name, err := indexOf(rc)
	if err != nil {
		return nil, err
	}
	stats, err := s.stats(rc.Ctx, name)
	if err != nil {
		return nil, err
	}
	out := row{"name": name, "metric": stats.Metric, "vector_type": orDefault(stats.VectorType, "dense")}
	mergeStats(out, stats)
	counts := make([]row, 0, len(stats.Namespaces))
	for ns, value := range stats.Namespaces {
		if ns == "" {
			ns = defaultNamespace
		}
		counts = append(counts, row{"namespace": ns, "record_count": value.VectorCount})
	}
	out["namespace_counts"] = plugin.SortRows(counts, []plugin.SortKey{{Field: "namespace"}})
	return out, nil
}

func indexCreate(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := s.ensureWritable(); err != nil {
		return nil, err
	}
	var req struct {
		Name               string `json:"name" validate:"required"`
		Deployment         string `json:"deployment" validate:"required"`
		VectorType         string `json:"vector_type"`
		Dimension          int    `json:"dimension"`
		Metric             string `json:"metric"`
		Cloud              string `json:"cloud"`
		Region             string `json:"region"`
		Environment        string `json:"environment"`
		PodType            string `json:"pod_type"`
		Pods               int    `json:"pods"`
		Replicas           int    `json:"replicas"`
		Shards             int    `json:"shards"`
		SourceCollection   string `json:"source_collection"`
		DeletionProtection bool   `json:"deletion_protection"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	if err := validateName("index", req.Name); err != nil {
		return nil, err
	}
	vectorType := orDefault(req.VectorType, "dense")
	body := row{"name": req.Name, "vector_type": vectorType}
	switch vectorType {
	case "dense":
		if req.Dimension <= 0 {
			return nil, fmt.Errorf("%w: a dense index needs a dimension", plugin.ErrInvalidInput)
		}
		body["dimension"] = req.Dimension
		body["metric"] = orDefault(req.Metric, "cosine")
	case "sparse":
		// Pinecone builds sparse indexes on serverless only, always with the dot
		// product metric, and rejects a dimension on them.
		if req.Deployment != "serverless" {
			return nil, fmt.Errorf("%w: sparse indexes are serverless-only", plugin.ErrInvalidInput)
		}
		body["metric"] = "dotproduct"
	default:
		return nil, fmt.Errorf("%w: vector type must be dense or sparse", plugin.ErrInvalidInput)
	}
	if req.DeletionProtection {
		body["deletion_protection"] = "enabled"
	}
	switch req.Deployment {
	case "serverless":
		if req.Cloud == "" || req.Region == "" {
			return nil, fmt.Errorf("%w: a serverless index needs a cloud and a region", plugin.ErrInvalidInput)
		}
		spec := row{"cloud": req.Cloud, "region": req.Region}
		if req.SourceCollection != "" {
			spec["source_collection"] = req.SourceCollection
		}
		body["spec"] = row{"serverless": spec}
	case "pod":
		if req.Environment == "" || req.PodType == "" {
			return nil, fmt.Errorf("%w: a pod index needs an environment and a pod type", plugin.ErrInvalidInput)
		}
		spec := row{
			"environment": req.Environment,
			"pod_type":    req.PodType,
			"pods":        max(req.Pods, 1),
			"replicas":    max(req.Replicas, 1),
			"shards":      max(req.Shards, 1),
		}
		if req.SourceCollection != "" {
			spec["source_collection"] = req.SourceCollection
		}
		body["spec"] = row{"pod": spec}
	default:
		return nil, fmt.Errorf("%w: deployment must be serverless or pod", plugin.ErrInvalidInput)
	}
	var out indexInfo
	if err := s.control.do(rc.Ctx, http.MethodPost, "/indexes", nil, body, &out); err != nil {
		return nil, err
	}
	return row{"ok": true, "name": req.Name, "host": out.Host, "state": out.Status.State}, nil
}

func indexConfigure(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := s.ensureWritable(); err != nil {
		return nil, err
	}
	name, err := indexOf(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		DeletionProtection string            `json:"deletion_protection"`
		Tags               map[string]string `json:"tags"`
		PodType            string            `json:"pod_type"`
		Replicas           int               `json:"replicas"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	body := row{}
	switch strings.TrimSpace(req.DeletionProtection) {
	case "":
	case "enabled", "disabled":
		body["deletion_protection"] = req.DeletionProtection
	default:
		return nil, fmt.Errorf("%w: deletion protection must be enabled or disabled", plugin.ErrInvalidInput)
	}
	if req.Tags != nil {
		body["tags"] = req.Tags
	}
	pod := row{}
	if req.PodType != "" {
		pod["pod_type"] = req.PodType
	}
	if req.Replicas > 0 {
		pod["replicas"] = req.Replicas
	}
	if len(pod) > 0 {
		body["spec"] = row{"pod": pod}
	}
	if len(body) == 0 {
		return nil, fmt.Errorf("%w: nothing to update", plugin.ErrInvalidInput)
	}
	var out indexInfo
	if err := s.control.do(rc.Ctx, http.MethodPatch, "/indexes/"+url.PathEscape(name), nil, body, &out); err != nil {
		return nil, err
	}
	return row{"ok": true, "name": name, "state": out.Status.State}, nil
}

func indexDelete(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := s.ensureWritable(); err != nil {
		return nil, err
	}
	name, err := indexOf(rc)
	if err != nil {
		return nil, err
	}
	if err := s.control.do(rc.Ctx, http.MethodDelete, "/indexes/"+url.PathEscape(name), nil, nil, nil); err != nil {
		return nil, err
	}
	s.forgetIndex(name)
	return row{"ok": true, "name": name}, nil
}

func collectionsTree(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	collections, err := s.listCollections(rc.Ctx)
	if err != nil {
		return nil, err
	}
	req, err := rc.Page()
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(collections))
	for _, item := range collections {
		rows = append(rows, item.row())
	}
	page, err := pageRows(req, plugin.FilterRows(rows, req.Search()), false)
	if err != nil {
		return nil, err
	}
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name := fmt.Sprint(item["name"])
		ref := plugin.ResourceIdentity{Kind: "collection", Name: name, UID: name}
		nodes = append(nodes, plugin.TreeNode{
			Key:   "collection:" + name,
			Label: name,
			Icon:  icon("archive"),
			Ref:   &ref,
			Leaf:  true,
			Data:  map[string]any{"status": item["status"]},
		})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

func collectionsList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	req, err := rc.Page()
	if err != nil {
		return nil, err
	}
	collections, err := s.listCollections(rc.Ctx)
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(collections))
	for _, item := range collections {
		rows = append(rows, item.row())
	}
	page, err := pageRows(req, plugin.FilterRows(rows, req.Search()), false)
	if err != nil {
		return nil, err
	}
	return capped(page, false), nil
}

func collectionRead(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	name := strings.TrimSpace(rc.Param("collection"))
	if err := validateName("collection", name); err != nil {
		return nil, err
	}
	var out collectionInfo
	if err := s.control.do(rc.Ctx, http.MethodGet, "/collections/"+url.PathEscape(name), nil, nil, &out); err != nil {
		return nil, err
	}
	if out.Name == "" {
		out.Name = name
	}
	return out.row(), nil
}

func collectionCreate(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := s.ensureWritable(); err != nil {
		return nil, err
	}
	var req struct {
		Name   string `json:"name" validate:"required"`
		Source string `json:"source" validate:"required"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	if err := validateName("collection", req.Name); err != nil {
		return nil, err
	}
	if err := validateName("source index", req.Source); err != nil {
		return nil, err
	}
	var out collectionInfo
	if err := s.control.do(rc.Ctx, http.MethodPost, "/collections", nil, row{"name": req.Name, "source": req.Source}, &out); err != nil {
		return nil, err
	}
	return row{"ok": true, "name": req.Name, "source": req.Source, "status": out.Status}, nil
}

func collectionDelete(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := s.ensureWritable(); err != nil {
		return nil, err
	}
	name := strings.TrimSpace(rc.Param("collection"))
	if err := validateName("collection", name); err != nil {
		return nil, err
	}
	if err := s.control.do(rc.Ctx, http.MethodDelete, "/collections/"+url.PathEscape(name), nil, nil, nil); err != nil {
		return nil, err
	}
	return row{"ok": true, "name": name}, nil
}

func namespacesList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	name, err := indexOf(rc)
	if err != nil {
		return nil, err
	}
	client, err := s.indexClient(rc.Ctx, name)
	if err != nil {
		return nil, err
	}
	req, err := rc.Page()
	if err != nil {
		return nil, err
	}
	fetch := func(ctx context.Context, token string, limit int) ([]row, string, error) {
		query := url.Values{"limit": []string{strconv.Itoa(limit)}}
		if token != "" {
			query.Set("paginationToken", token)
		}
		var out namespaceList
		if err := client.do(ctx, http.MethodGet, "/namespaces", query, nil, &out); err != nil {
			return nil, "", err
		}
		rows := make([]row, 0, len(out.Namespaces))
		for _, ns := range out.Namespaces {
			rows = append(rows, ns.row(name))
		}
		return rows, out.Pagination.Next, nil
	}
	keep := searchFilter(req, "name")
	if len(req.Sort) > 0 {
		rows, truncated, err := scanRows(rc.Ctx, fetch, keep)
		if err != nil {
			return s.namespacesFromStats(rc.Ctx, req, name, keep, err)
		}
		page, err := pageRows(req, rows, truncated)
		if err != nil {
			return nil, err
		}
		return capped(page, truncated), nil
	}
	page, truncated, err := streamPage(rc.Ctx, req, fetch, keep)
	if err != nil {
		return s.namespacesFromStats(rc.Ctx, req, name, keep, err)
	}
	return capped(page, truncated), nil
}

// namespacesFromStats answers a listing from describe_index_stats when the
// data-plane /namespaces endpoint is not served, which is the case for
// pod-based indexes. The stats call reports every namespace at once, so the
// page it produces carries an authoritative total.
func (s *Session) namespacesFromStats(ctx context.Context, req plugin.PageRequest, index string, keep keepRow, cause error) (any, error) {
	if !unsupportedEndpoint(cause) {
		return nil, cause
	}
	all, err := s.namespaceRows(ctx, index)
	if err != nil {
		return nil, cause
	}
	rows := make([]row, 0, len(all))
	for _, item := range all {
		if keep(item) {
			rows = append(rows, item)
		}
	}
	if len(req.Sort) == 0 {
		// The endpoint this stands in for lists in name order; keep paging stable.
		req.Sort = []plugin.SortKey{{Field: "name"}}
	}
	page, err := pageRows(req, rows, false)
	if err != nil {
		return nil, err
	}
	return capped(page, false), nil
}

func namespaceRead(rc *plugin.RequestContext) (any, error) {
	s, name, ns, client, err := indexTarget(rc)
	if err != nil {
		return nil, err
	}
	var out namespaceInfo
	if err := client.do(rc.Ctx, http.MethodGet, "/namespaces/"+url.PathEscape(ns), nil, nil, &out); err != nil {
		if !unsupportedEndpoint(err) {
			return nil, err
		}
		found, statsErr := s.namespaceFromStats(rc.Ctx, name, ns)
		if statsErr != nil {
			return nil, err
		}
		out = found
	}
	if out.Name == "" {
		out.Name = ns
	}
	item := out.row(name)
	item["read_only"] = s.opts.ReadOnly
	return item, nil
}

// namespaceFromStats resolves one namespace through describe_index_stats so a
// pod-based index still has a namespace detail view.
func (s *Session) namespaceFromStats(ctx context.Context, index, ns string) (namespaceInfo, error) {
	stats, err := s.stats(ctx, index)
	if err != nil {
		return namespaceInfo{}, err
	}
	for name, value := range stats.Namespaces {
		if name == ns || (name == "" && ns == defaultNamespace) {
			return namespaceInfo{Name: ns, RecordCount: value.VectorCount}, nil
		}
	}
	return namespaceInfo{}, fmt.Errorf("%w: namespace %q in index %q", plugin.ErrNotFound, ns, index)
}

func namespaceDelete(rc *plugin.RequestContext) (any, error) {
	s, name, ns, client, err := indexTarget(rc)
	if err != nil {
		return nil, err
	}
	if err := s.ensureWritable(); err != nil {
		return nil, err
	}
	if err := client.do(rc.Ctx, http.MethodDelete, "/namespaces/"+url.PathEscape(ns), nil, nil, nil); err != nil {
		return nil, err
	}
	return row{"ok": true, "index": name, "namespace": ns}, nil
}

func vectorsList(rc *plugin.RequestContext) (any, error) {
	s, name, ns, client, err := indexTarget(rc)
	if err != nil {
		return nil, err
	}
	req, err := rc.Page()
	if err != nil {
		return nil, err
	}
	prefix := paramOrQuery(rc, "prefix")
	fetch := func(ctx context.Context, token string, limit int) ([]row, string, error) {
		query := url.Values{"limit": []string{strconv.Itoa(limit)}, "namespace": []string{ns}}
		if token != "" {
			query.Set("paginationToken", token)
		}
		if prefix != "" {
			query.Set("prefix", prefix)
		}
		var out vectorIDList
		if err := client.do(ctx, http.MethodGet, "/vectors/list", query, nil, &out); err != nil {
			return nil, "", err
		}
		rows := make([]row, 0, len(out.Vectors))
		for _, v := range out.Vectors {
			rows = append(rows, row{"ref": vectorRef(name, ns, v.ID), "id": v.ID, "index": name, "namespace": ns})
		}
		return rows, out.Pagination.Next, nil
	}
	// Pinecone lists ids only, so the search box matches ids; values and metadata
	// are hydrated for the rows that survive.
	keep := searchFilter(req, "id")
	var page plugin.Page[row]
	truncated := false
	if len(req.Sort) > 0 {
		rows, cut, err := scanRows(rc.Ctx, fetch, keep)
		if err != nil {
			return nil, err
		}
		if sortsOnFetchedField(req.Sort) {
			// The ordering column only exists after a fetch, so the whole scan has
			// to be hydrated before it can be ordered and sliced.
			if err := s.hydrate(rc.Ctx, client, ns, rows); err != nil {
				return nil, err
			}
			if page, err = pageRows(req, rows, cut); err != nil {
				return nil, err
			}
		} else {
			if page, err = pageRows(req, rows, cut); err != nil {
				return nil, err
			}
			if err := s.hydrate(rc.Ctx, client, ns, page.Items); err != nil {
				return nil, err
			}
		}
		truncated = cut
	} else {
		if page, truncated, err = streamPage(rc.Ctx, req, fetch, keep); err != nil {
			return nil, err
		}
		if err := s.hydrate(rc.Ctx, client, ns, page.Items); err != nil {
			return nil, err
		}
	}
	return capped(page, truncated), nil
}

// sortsOnFetchedField reports whether the grid ordered by a vector column that
// only a fetch can fill in; the listing itself carries id, index, and namespace.
func sortsOnFetchedField(keys []plugin.SortKey) bool {
	for _, key := range keys {
		switch key.Field {
		case "id", "index", "namespace", "":
		default:
			return true
		}
	}
	return false
}

// hydrate fills values and metadata for rows that carry only an id.
func (s *Session) hydrate(ctx context.Context, client *restClient, namespace string, rows []row) error {
	if len(rows) == 0 {
		return nil
	}
	ids := make([]string, 0, len(rows))
	for _, item := range rows {
		if id, ok := item["id"].(string); ok && id != "" {
			ids = append(ids, id)
		}
	}
	records, err := s.fetchVectors(ctx, client, namespace, ids)
	if err != nil {
		return err
	}
	for _, item := range rows {
		id, _ := item["id"].(string)
		record, ok := records[id]
		if !ok {
			continue
		}
		item["metadata"] = record.Metadata
		if record.Metadata == nil {
			item["metadata"] = map[string]any{}
		}
		if len(record.Values) > 0 {
			item["values"] = record.Values
			item["dimension"] = len(record.Values)
			item["norm"] = round(norm(record.Values), 6)
		}
		if record.SparseValues != nil {
			item["sparse_terms"] = len(record.SparseValues.Indices)
		}
	}
	return nil
}

func vectorRead(rc *plugin.RequestContext) (any, error) {
	s, name, ns, client, err := indexTarget(rc)
	if err != nil {
		return nil, err
	}
	id := strings.TrimSpace(rc.Param("vector"))
	if id == "" {
		return nil, fmt.Errorf("%w: a vector id is required", plugin.ErrInvalidInput)
	}
	records, err := s.fetchVectors(rc.Ctx, client, ns, []string{id})
	if err != nil {
		return nil, err
	}
	record, ok := records[id]
	if !ok {
		return nil, fmt.Errorf("%w: vector %q in namespace %q", plugin.ErrNotFound, id, ns)
	}
	out := record.row(name, ns)
	out["metadata_keys"] = sortedKeys(record.Metadata)
	return out, nil
}

func vectorsUpsert(rc *plugin.RequestContext) (any, error) {
	s, name, ns, client, err := indexTarget(rc)
	if err != nil {
		return nil, err
	}
	if err := s.ensureWritable(); err != nil {
		return nil, err
	}
	payload, err := upsertPayload(rc.Body())
	if err != nil {
		return nil, err
	}
	if len(payload.Vectors) == 0 {
		return nil, fmt.Errorf("%w: at least one vector is required", plugin.ErrInvalidInput)
	}
	for _, vector := range payload.Vectors {
		if strings.TrimSpace(vector.ID) == "" {
			return nil, fmt.Errorf("%w: every vector needs an id", plugin.ErrInvalidInput)
		}
		if len(vector.Values) == 0 && vector.SparseValues == nil {
			return nil, fmt.Errorf("%w: vector %q has no values", plugin.ErrInvalidInput, vector.ID)
		}
	}
	target := ns
	if payload.Namespace != "" {
		if err := validateNamespace(payload.Namespace); err != nil {
			return nil, err
		}
		target = payload.Namespace
	}
	upserted := 0
	for batch := range chunks(payload.Vectors, writeBatchSize) {
		var out struct {
			UpsertedCount int `json:"upsertedCount"`
		}
		body := row{"vectors": batch, "namespace": target}
		if err := client.do(rc.Ctx, http.MethodPost, "/vectors/upsert", nil, body, &out); err != nil {
			return nil, fmt.Errorf("%w (%d record(s) were already written)", err, upserted)
		}
		upserted += out.UpsertedCount
	}
	return row{"ok": true, "index": name, "namespace": target, "upserted": upserted}, nil
}

type upsertRequest struct {
	Vectors   []vectorRecord `json:"vectors"`
	Namespace string         `json:"namespace"`
}

// upsertPayload accepts the raw Pinecone shape and the code-editor envelopes
// ({"vectors": ...} from SaveBodyKey, {"content": "..."} from a plain editor).
func upsertPayload(body []byte) (upsertRequest, error) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil {
		return upsertRequest{}, fmt.Errorf("%w: body must be a JSON object", plugin.ErrInvalidInput)
	}
	raw := body
	switch {
	case len(envelope) == 1 && envelope["content"] != nil:
		var text string
		if err := json.Unmarshal(envelope["content"], &text); err != nil {
			return upsertRequest{}, fmt.Errorf("%w: content must be a JSON string", plugin.ErrInvalidInput)
		}
		raw = []byte(text)
	case len(envelope) == 1 && envelope["vectors"] != nil:
		var inner upsertRequest
		if err := json.Unmarshal(envelope["vectors"], &inner); err == nil && len(inner.Vectors) > 0 {
			// SaveBodyKey wrapped a whole request document.
			return inner, nil
		}
	}
	var out upsertRequest
	if err := json.Unmarshal(raw, &out); err != nil {
		return upsertRequest{}, fmt.Errorf("%w: %v", plugin.ErrInvalidInput, err)
	}
	return out, nil
}

func vectorsDelete(rc *plugin.RequestContext) (any, error) {
	s, name, ns, client, err := indexTarget(rc)
	if err != nil {
		return nil, err
	}
	if err := s.ensureWritable(); err != nil {
		return nil, err
	}
	var req struct {
		ID        string         `json:"id"`
		IDs       []string       `json:"ids"`
		Filter    map[string]any `json:"filter"`
		DeleteAll bool           `json:"deleteAll"`
	}
	if len(rc.Body()) > 0 {
		if err := rc.Bind(&req); err != nil {
			return nil, err
		}
	}
	ids := append([]string(nil), req.IDs...)
	if req.ID != "" {
		ids = append(ids, req.ID)
	}
	for _, id := range rc.Query()["id"] {
		if id != "" {
			ids = append(ids, id)
		}
	}
	out := row{"ok": true, "index": name, "namespace": ns}
	switch {
	case len(ids) > 0:
		deleted := 0
		for batch := range chunks(ids, writeBatchSize) {
			body := row{"namespace": ns, "ids": batch}
			if err := client.do(rc.Ctx, http.MethodPost, "/vectors/delete", nil, body, nil); err != nil {
				return nil, fmt.Errorf("%w (%d record(s) were already deleted)", err, deleted)
			}
			deleted += len(batch)
		}
		out["deleted"] = deleted
	case len(req.Filter) > 0:
		if err := client.do(rc.Ctx, http.MethodPost, "/vectors/delete", nil, row{"namespace": ns, "filter": req.Filter}, nil); err != nil {
			return nil, err
		}
		out["target"] = "filter"
	case req.DeleteAll:
		if err := client.do(rc.Ctx, http.MethodPost, "/vectors/delete", nil, row{"namespace": ns, "deleteAll": true}, nil); err != nil {
			return nil, err
		}
		out["target"] = "namespace"
	default:
		return nil, fmt.Errorf("%w: ids, filter, or deleteAll is required", plugin.ErrInvalidInput)
	}
	return out, nil
}

func paramOrQuery(rc *plugin.RequestContext, key string) string {
	if value := strings.TrimSpace(rc.Param(key)); value != "" {
		return value
	}
	return strings.TrimSpace(rc.Query().Get(key))
}

func sortedKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	slices.Sort(out)
	return out
}

func indexCreateSchema() *plugin.Schema {
	isPod := &plugin.Condition{AllOf: []plugin.Rule{{Field: "deployment", Op: plugin.OpEq, Value: "pod"}}}
	isServerless := &plugin.Condition{AllOf: []plugin.Rule{{Field: "deployment", Op: plugin.OpEq, Value: "serverless"}}}
	isDense := &plugin.Condition{AllOf: []plugin.Rule{{Field: "vector_type", Op: plugin.OpEq, Value: "dense"}}}
	return &plugin.Schema{Groups: []plugin.Group{
		{Name: "Index", Fields: []plugin.Field{
			{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{
				{Type: plugin.ValidatorRegex, Value: `^[a-z0-9][a-z0-9-]{0,44}$`, Message: "lower-case letters, digits, or hyphens"},
			}},
			{Key: "vector_type", Label: "Vector type", Type: plugin.FieldSelect, Required: true, Default: "dense", Options: []plugin.Option{
				{Label: "Dense", Value: "dense"},
				{Label: "Sparse", Value: "sparse"},
			}},
			{Key: "dimension", Label: "Dimension", Type: plugin.FieldNumber, Default: 1536, VisibleWhen: isDense, Validators: []plugin.Validator{
				{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 20000},
			}},
			{Key: "metric", Label: "Metric", Type: plugin.FieldSelect, Default: "cosine", VisibleWhen: isDense, Options: []plugin.Option{
				{Label: "Cosine", Value: "cosine"},
				{Label: "Euclidean", Value: "euclidean"},
				{Label: "Dot product", Value: "dotproduct"},
			}, Help: "Sparse indexes always use dot product."},
			{Key: "deletion_protection", Label: "Deletion protection", Type: plugin.FieldToggle},
		}},
		{Name: "Deployment", Fields: []plugin.Field{
			{Key: "deployment", Label: "Deployment", Type: plugin.FieldSelect, Required: true, Default: "serverless", Options: []plugin.Option{
				{Label: "Serverless", Value: "serverless"},
				{Label: "Pod-based", Value: "pod"},
			}},
			{Key: "cloud", Label: "Cloud", Type: plugin.FieldSelect, Default: "aws", VisibleWhen: isServerless, Options: []plugin.Option{
				{Label: "AWS", Value: "aws"},
				{Label: "GCP", Value: "gcp"},
				{Label: "Azure", Value: "azure"},
			}},
			{Key: "region", Label: "Region", Type: plugin.FieldAutocomplete, Default: "us-east-1", VisibleWhen: isServerless, Options: []plugin.Option{
				{Label: "us-east-1", Value: "us-east-1"},
				{Label: "us-west-2", Value: "us-west-2"},
				{Label: "eu-west-1", Value: "eu-west-1"},
				{Label: "us-central1", Value: "us-central1"},
				{Label: "eastus2", Value: "eastus2"},
			}},
			{Key: "environment", Label: "Environment", Type: plugin.FieldText, VisibleWhen: isPod, Placeholder: "us-east-1-aws"},
			{Key: "pod_type", Label: "Pod type", Type: plugin.FieldAutocomplete, Default: "p1.x1", VisibleWhen: isPod, Options: podTypeOptions()},
			{Key: "pods", Label: "Pods", Type: plugin.FieldNumber, Default: 1, VisibleWhen: isPod, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}}},
			{Key: "replicas", Label: "Replicas", Type: plugin.FieldNumber, Default: 1, VisibleWhen: isPod, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}}},
			{Key: "shards", Label: "Shards", Type: plugin.FieldNumber, Default: 1, VisibleWhen: isPod, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}}},
			{Key: "source_collection", Label: "Restore from collection", Type: plugin.FieldAutocomplete,
				OptionsSource: &plugin.DataSource{RouteID: rid("collections.list")},
				Help:          "Seed the new index from a backup collection with a matching dimension."},
		}},
	}}
}

func indexConfigureSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Index", Fields: []plugin.Field{
		{Key: "deletion_protection", Label: "Deletion protection", Type: plugin.FieldSelect, Options: []plugin.Option{
			{Label: "Leave unchanged", Value: ""},
			{Label: "Enabled", Value: "enabled"},
			{Label: "Disabled", Value: "disabled"},
		}},
		{Key: "tags", Label: "Tags", Type: plugin.FieldMap, Item: &plugin.Field{Key: "value", Label: "Value", Type: plugin.FieldText},
			Help: "Free-form key/value labels stored on the index."},
		{Key: "pod_type", Label: "Pod type", Type: plugin.FieldAutocomplete, Options: podTypeOptions(),
			Help: "Pod indexes only; vertical scaling is one-way."},
		{Key: "replicas", Label: "Replicas", Type: plugin.FieldNumber, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}},
			Help: "Pod indexes only."},
	}}}}
}

func collectionCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Collection", Fields: []plugin.Field{
		{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true, Validators: []plugin.Validator{
			{Type: plugin.ValidatorRegex, Value: `^[a-z0-9][a-z0-9-]{0,44}$`, Message: "lower-case letters, digits, or hyphens"},
		}},
		{Key: "source", Label: "Source index", Type: plugin.FieldAutocomplete, Required: true, Default: "${resource.name}",
			OptionsSource: &plugin.DataSource{RouteID: rid("indexes.list")}},
	}}}}
}

func podTypeOptions() []plugin.Option {
	sizes := []string{"x1", "x2", "x4", "x8"}
	families := []string{"s1", "p1", "p2"}
	out := make([]plugin.Option, 0, len(sizes)*len(families))
	for _, family := range families {
		for _, size := range sizes {
			value := family + "." + size
			out = append(out, plugin.Option{Label: value, Value: value})
		}
	}
	return out
}
