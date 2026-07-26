package chroma

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

const metricsInterval = 5 * time.Second

func serverMetrics(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, sc, err := sessionScope(rc)
	if err != nil {
		return err
	}
	return pump(rc, client, func(ctx context.Context) row {
		return serverFrame(ctx, s, sc)
	})
}

func serverFrame(ctx context.Context, s *Session, sc scope) row {
	frame := row{}
	start := time.Now()
	var beat map[string]any
	if err := s.do(ctx, http.MethodGet, "/heartbeat", nil, nil, &beat); err == nil {
		frame["heartbeatMs"] = round(float64(time.Since(start).Microseconds())/1000, 3)
	}
	if databases, err := s.databases(ctx, sc.Tenant); err == nil {
		frame["databases"] = len(databases)
	}
	if total, err := s.collectionCount(ctx, sc); err == nil {
		frame["collections"] = total
	}
	var collections []collection
	if err := s.do(ctx, http.MethodGet, sc.collectionsPath(), nil, nil, &collections); err == nil {
		records, vectorBytes, widest := 0, 0, 0
		for _, col := range collections {
			count, err := s.count(ctx, sc, col.ID)
			if err != nil {
				continue
			}
			records += count
			if col.Dimension != nil {
				vectorBytes += count * *col.Dimension * 4
				widest = max(widest, *col.Dimension)
			}
		}
		frame["records"] = records
		frame["vectorBytes"] = vectorBytes
		frame["maxDimension"] = widest
	}
	var checks struct {
		MaxBatchSize int `json:"max_batch_size"`
	}
	if err := s.do(ctx, http.MethodGet, "/pre-flight-checks", nil, nil, &checks); err == nil {
		frame["maxBatchSize"] = checks.MaxBatchSize
	}
	return frame
}

func collectionMetrics(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, sc, col, err := collectionOf(rc)
	if err != nil {
		return err
	}
	return pump(rc, client, func(ctx context.Context) row {
		return collectionFrame(ctx, s, sc, col.ID)
	})
}

func collectionFrame(ctx context.Context, s *Session, sc scope, id string) row {
	frame := row{}
	if count, err := s.count(ctx, sc, id); err == nil {
		frame["records"] = count
	}
	if col, err := s.getCollection(ctx, sc, id); err == nil {
		frame["version"] = col.Version
		frame["logPosition"] = col.LogPosition
		if col.Dimension != nil {
			frame["dimension"] = *col.Dimension
			if count, ok := frame["records"].(int); ok {
				frame["vectorBytes"] = count * *col.Dimension * 4
			}
		}
		if params := col.params(); params != nil {
			frame["efSearch"] = intOrZero(params.EFSearch)
			frame["maxNeighbors"] = intOrZero(params.MaxNeighbors)
		}
	}
	// Indexing status only exists on distributed Chroma; a single-node server
	// answers "not implemented", so the usage row simply stays empty there.
	var status struct {
		Progress float64 `json:"op_indexing_progress"`
		Indexed  int64   `json:"num_indexed_ops"`
		Total    int64   `json:"total_ops"`
	}
	if err := s.do(ctx, http.MethodGet, sc.collectionPath(id)+"/indexing_status", nil, nil, &status); err == nil && status.Total > 0 {
		frame["indexPct"] = round(status.Progress*100, 2)
		frame["indexedOps"] = status.Indexed
		frame["totalOps"] = status.Total
	}
	return frame
}

// pump writes the first frame immediately, then keeps a fixed cadence until the
// browser or the request context goes away.
func pump(rc *plugin.RequestContext, client plugin.ClientStream, frame func(context.Context) row) error {
	enc := json.NewEncoder(client)
	ticker := time.NewTicker(metricsInterval)
	defer ticker.Stop()
	for {
		if err := enc.Encode(frame(rc.Ctx)); err != nil {
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
