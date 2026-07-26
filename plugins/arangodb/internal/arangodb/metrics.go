package arangodb

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	driver "github.com/arangodb/go-driver/v2/arangodb"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

const metricsInterval = 5 * time.Second

// streamFrames writes an immediate first frame and then ticks until the browser
// disconnects. A failing sample degrades to a partial frame instead of killing
// the panel.
func streamFrames(rc *plugin.RequestContext, client plugin.ClientStream, sample func(context.Context) map[string]any) error {
	enc := json.NewEncoder(client)
	ticker := time.NewTicker(metricsInterval)
	defer ticker.Stop()
	for {
		if err := enc.Encode(sample(client.Context())); err != nil {
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

func databaseMetricsStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := session(rc)
	if err != nil {
		return err
	}
	database := databaseParam(rc, s)
	return streamFrames(rc, client, func(ctx context.Context) map[string]any {
		return s.databaseFrame(ctx, database)
	})
}

func (s *Session) databaseFrame(ctx context.Context, database string) map[string]any {
	frame := map[string]any{"collections": 0, "documents": 0, "edges": 0, "graphs": 0, "views": 0, "analyzers": 0, "runningQueries": 0, "slowQueries": 0}
	db, err := s.database(ctx, database)
	if err != nil {
		return frame
	}
	tctx, cancel := context.WithTimeout(ctx, s.opts.Timeout)
	defer cancel()
	if collections, err := db.Collections(tctx); err == nil {
		count, documents, edges := 0, int64(0), int64(0)
		for _, collection := range collections {
			if strings.HasPrefix(collection.Name(), "_") {
				continue
			}
			count++
			n, err := collection.Count(tctx)
			if err != nil {
				continue
			}
			documents += n
			if props, err := collection.Properties(tctx); err == nil && props.Type == driver.CollectionTypeEdge {
				edges += n
			}
		}
		frame["collections"] = count
		frame["documents"] = documents
		frame["edges"] = edges
	}
	if reader, err := db.Graphs(tctx); err == nil {
		graphs := 0
		for {
			if _, err := reader.Read(); err != nil {
				break
			}
			graphs++
		}
		frame["graphs"] = graphs
	}
	if views, err := db.ViewsAll(tctx); err == nil {
		frame["views"] = len(views)
	}
	if reader, err := db.Analyzers(tctx); err == nil {
		analyzers := 0
		for {
			if _, err := reader.Read(); err != nil {
				break
			}
			analyzers++
		}
		frame["analyzers"] = analyzers
	}
	if running, err := db.ListOfRunningAQLQueries(tctx, nil); err == nil {
		frame["runningQueries"] = len(running)
	}
	if slow, err := db.ListOfSlowAQLQueries(tctx, nil); err == nil {
		frame["slowQueries"] = len(slow)
	}
	return frame
}

func collectionMetricsStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, database, name, err := collectionScope(rc)
	if err != nil {
		return err
	}
	return streamFrames(rc, client, func(ctx context.Context) map[string]any {
		return s.collectionFrame(ctx, database, name)
	})
}

func (s *Session) collectionFrame(ctx context.Context, database, name string) map[string]any {
	frame := map[string]any{"documents": 0, "indexes": 0, "documentsSize": 0, "indexesSize": 0, "cacheUsed": 0, "cacheSize": 0, "cachePct": 0.0}
	col, err := s.collection(ctx, database, name)
	if err != nil {
		return frame
	}
	tctx, cancel := context.WithTimeout(ctx, s.opts.Timeout)
	defer cancel()
	if count, err := col.Count(tctx); err == nil {
		frame["documents"] = count
	}
	if indexes, err := col.Indexes(tctx); err == nil {
		frame["indexes"] = len(indexes)
	}
	stats, err := col.Statistics(tctx, true)
	if err != nil {
		return frame
	}
	figures := asMap(asMap(stats)["figures"])
	frame["documentsSize"] = numberOf(figures, "documentsSize")
	frame["indexesSize"] = numberOf(asMap(figures["indexes"]), "size")
	used := numberOf(figures, "cacheUsage")
	size := numberOf(figures, "cacheSize")
	frame["cacheUsed"] = used
	frame["cacheSize"] = size
	if size > 0 {
		frame["cachePct"] = float64(used) / float64(size) * 100
	}
	return frame
}

func clusterMetricsStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := session(rc)
	if err != nil {
		return err
	}
	return streamFrames(rc, client, s.clusterFrame)
}

func (s *Session) clusterFrame(ctx context.Context) map[string]any {
	frame := map[string]any{"databases": 0, "coordinators": 0, "dbServers": 0, "serversGood": 0, "serversTotal": 0, "serversPct": 0.0}
	tctx, cancel := context.WithTimeout(ctx, s.opts.Timeout)
	defer cancel()
	if databases, err := s.client.Databases(tctx); err == nil {
		frame["databases"] = len(databases)
	}
	if counts, err := s.client.NumberOfServers(tctx); err == nil {
		frame["coordinators"] = counts.NoCoordinators
		frame["dbServers"] = counts.NoDBServers
	}
	if health, err := s.client.Health(tctx); err == nil {
		good, total := 0, 0
		for _, server := range health.Health {
			total++
			if server.Status == driver.ServerStatusGood {
				good++
			}
		}
		frame["serversGood"] = good
		frame["serversTotal"] = total
		frame["serversPct"] = percent(good, total)
		return frame
	}
	// Single server: report itself as one healthy member so the panel stays useful.
	healthy := 0
	if err := s.HealthCheck(tctx); err == nil {
		healthy = 1
	}
	frame["serversGood"] = healthy
	frame["serversTotal"] = 1
	frame["serversPct"] = percent(healthy, 1)
	return frame
}
