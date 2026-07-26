package memcached

import (
	"context"
	"math"
	"sort"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

// A snapshot is only worth reading while the server still answers; an
// unreachable snapshot is the last good one, labelled so the panel says why the
// numbers stopped moving.
const (
	statusOK          = "ok"
	statusUnreachable = "unreachable"
)

// serverOverview is the connection-level property sheet and the payload the
// object-detail watch stream pushes.
type serverOverview struct {
	Address     string `json:"address"`
	Status      string `json:"status"`
	StatusError string `json:"statusError,omitempty"`
	Version     string `json:"version"`
	PID         int64  `json:"pid"`
	Uptime      int64  `json:"uptime"`
	ServerTime  string `json:"serverTime,omitempty"`
	PointerSize int64  `json:"pointerSize"`
	Threads     int64  `json:"threads"`
	MaxItemSize int64  `json:"maxItemSize"`
	ReadOnly    bool   `json:"readOnly"`

	CurrItems  int64 `json:"currItems"`
	TotalItems int64 `json:"totalItems"`

	Bytes         int64   `json:"bytes"`
	LimitMaxBytes int64   `json:"limitMaxBytes"`
	MemoryPct     float64 `json:"memoryPct"`
	TotalMalloced int64   `json:"totalMalloced"`
	ActiveSlabs   int64   `json:"activeSlabs"`
	WastedBytes   int64   `json:"wastedBytes"`
	WastedPct     float64 `json:"wastedPct"`

	CurrConnections  int64   `json:"currConnections"`
	MaxConnections   int64   `json:"maxConnections"`
	ConnectionsPct   float64 `json:"connectionsPct"`
	TotalConnections int64   `json:"totalConnections"`
	RejectedConns    int64   `json:"rejectedConnections"`

	CmdGet   int64 `json:"cmdGet"`
	CmdSet   int64 `json:"cmdSet"`
	CmdTouch int64 `json:"cmdTouch"`
	CmdFlush int64 `json:"cmdFlush"`

	GetHits    int64   `json:"getHits"`
	GetMisses  int64   `json:"getMisses"`
	GetExpired int64   `json:"getExpired"`
	HitRatio   float64 `json:"hitRatio"`

	DeleteHits int64 `json:"deleteHits"`
	DeleteMiss int64 `json:"deleteMisses"`
	IncrHits   int64 `json:"incrHits"`
	IncrMiss   int64 `json:"incrMisses"`
	DecrHits   int64 `json:"decrHits"`
	DecrMiss   int64 `json:"decrMisses"`
	CasHits    int64 `json:"casHits"`
	CasMiss    int64 `json:"casMisses"`
	CasBadval  int64 `json:"casBadval"`
	TouchHits  int64 `json:"touchHits"`
	TouchMiss  int64 `json:"touchMisses"`

	Evictions        int64 `json:"evictions"`
	Reclaimed        int64 `json:"reclaimed"`
	ExpiredUnfetched int64 `json:"expiredUnfetched"`
	EvictedUnfetched int64 `json:"evictedUnfetched"`

	BytesRead    int64 `json:"bytesRead"`
	BytesWritten int64 `json:"bytesWritten"`

	LRUCrawlerEnabled bool `json:"lruCrawlerEnabled"`
	CASEnabled        bool `json:"casEnabled"`
	AuthEnabled       bool `json:"authEnabled"`
}

// slabRow is one slab class in the allocation table and the canvas visualizer.
type slabRow struct {
	Class          int     `json:"class"`
	ChunkSize      int64   `json:"chunkSize"`
	ChunksPerPage  int64   `json:"chunksPerPage"`
	TotalPages     int64   `json:"totalPages"`
	TotalChunks    int64   `json:"totalChunks"`
	UsedChunks     int64   `json:"usedChunks"`
	FreeChunks     int64   `json:"freeChunks"`
	Items          int64   `json:"items"`
	AllocatedBytes int64   `json:"allocatedBytes"`
	UsedBytes      int64   `json:"usedBytes"`
	MemRequested   int64   `json:"memRequested"`
	WastedBytes    int64   `json:"wastedBytes"`
	WastedPct      float64 `json:"wastedPct"`
	FillPct        float64 `json:"fillPct"`
	Age            int64   `json:"age"`
	Evicted        int64   `json:"evicted"`
	OutOfMemory    int64   `json:"outOfMemory"`
	GetHits        int64   `json:"getHits"`
	CmdSet         int64   `json:"cmdSet"`
}

// lruRow is one slab class in the segmented-LRU table.
type lruRow struct {
	Class            int   `json:"class"`
	Number           int64 `json:"number"`
	NumberHot        int64 `json:"numberHot"`
	NumberWarm       int64 `json:"numberWarm"`
	NumberCold       int64 `json:"numberCold"`
	NumberTemp       int64 `json:"numberTemp"`
	Age              int64 `json:"age"`
	AgeHot           int64 `json:"ageHot"`
	AgeWarm          int64 `json:"ageWarm"`
	Evicted          int64 `json:"evicted"`
	EvictedNonzero   int64 `json:"evictedNonzero"`
	EvictedTime      int64 `json:"evictedTime"`
	OutOfMemory      int64 `json:"outOfMemory"`
	Reclaimed        int64 `json:"reclaimed"`
	ExpiredUnfetched int64 `json:"expiredUnfetched"`
	EvictedUnfetched int64 `json:"evictedUnfetched"`
	CrawlerReclaimed int64 `json:"crawlerReclaimed"`
	MovesToCold      int64 `json:"movesToCold"`
	MovesToWarm      int64 `json:"movesToWarm"`
	DirectReclaims   int64 `json:"directReclaims"`
}

type connRow struct {
	FD               int    `json:"fd"`
	Addr             string `json:"addr"`
	ListenAddr       string `json:"listenAddr"`
	State            string `json:"state"`
	SecsSinceLastCmd int64  `json:"secsSinceLastCmd"`
}

type settingRow struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// readOverview builds the connection overview from the general stats plus the
// settings and slab totals that give the numbers their capacity context.
func (s *Session) readOverview(ctx context.Context) (serverOverview, error) {
	var out serverOverview
	err := s.withAdmin(ctx, func(c *textConn) error {
		general, err := c.stats(ctx, "", s.opts.Timeout)
		if err != nil {
			return err
		}
		settings, err := c.stats(ctx, "settings", s.opts.Timeout)
		if err != nil {
			return err
		}
		slabs, err := c.stats(ctx, "slabs", s.opts.Timeout)
		if err != nil {
			return err
		}
		items, err := c.stats(ctx, "items", s.opts.Timeout)
		if err != nil {
			return err
		}
		out = buildOverview(s.opts, statsMap(general), statsMap(settings), statsMap(slabs), classStats(items, "items:"))
		return nil
	})
	return out, err
}

func buildOverview(opts options, general, settings, slabs map[string]string, items map[int]map[string]string) serverOverview {
	g := func(name string) int64 { return atoiDefault(general[name], 0) }
	out := serverOverview{
		Address:     opts.address(),
		Status:      statusOK,
		Version:     general["version"],
		PID:         g("pid"),
		Uptime:      g("uptime"),
		PointerSize: g("pointer_size"),
		Threads:     g("threads"),
		ReadOnly:    opts.ReadOnly,

		CurrItems:  g("curr_items"),
		TotalItems: g("total_items"),

		Bytes:         g("bytes"),
		LimitMaxBytes: g("limit_maxbytes"),
		TotalMalloced: atoiDefault(slabs["total_malloced"], 0),
		ActiveSlabs:   atoiDefault(slabs["active_slabs"], 0),

		CurrConnections:  g("curr_connections"),
		TotalConnections: g("total_connections"),
		RejectedConns:    g("rejected_connections"),

		CmdGet:   g("cmd_get"),
		CmdSet:   g("cmd_set"),
		CmdTouch: g("cmd_touch"),
		CmdFlush: g("cmd_flush"),

		GetHits:    g("get_hits"),
		GetMisses:  g("get_misses"),
		GetExpired: g("get_expired"),

		DeleteHits: g("delete_hits"),
		DeleteMiss: g("delete_misses"),
		IncrHits:   g("incr_hits"),
		IncrMiss:   g("incr_misses"),
		DecrHits:   g("decr_hits"),
		DecrMiss:   g("decr_misses"),
		CasHits:    g("cas_hits"),
		CasMiss:    g("cas_misses"),
		CasBadval:  g("cas_badval"),
		TouchHits:  g("touch_hits"),
		TouchMiss:  g("touch_misses"),

		Evictions:        g("evictions"),
		Reclaimed:        g("reclaimed"),
		ExpiredUnfetched: g("expired_unfetched"),
		EvictedUnfetched: g("evicted_unfetched"),

		BytesRead:    g("bytes_read"),
		BytesWritten: g("bytes_written"),

		MaxConnections:    atoiDefault(settings["maxconns"], 0),
		MaxItemSize:       atoiDefault(settings["item_size_max"], 0),
		LRUCrawlerEnabled: settings["lru_crawler"] == "yes",
		CASEnabled:        settings["cas_enabled"] != "no",
		AuthEnabled:       settings["auth_enabled_sasl"] == "yes" || settings["auth_enabled_ascii"] == "yes" || opts.Username != "",
	}
	if out.LimitMaxBytes == 0 {
		out.LimitMaxBytes = atoiDefault(settings["maxbytes"], 0)
	}
	if serverClock := g("time"); serverClock > 0 {
		out.ServerTime = timeUnix(serverClock)
	}
	out.MemoryPct = percent(out.Bytes, out.LimitMaxBytes)
	out.ConnectionsPct = percent(out.CurrConnections, out.MaxConnections)
	out.HitRatio = percent(out.GetHits, out.GetHits+out.GetMisses)

	requested := int64(0)
	for _, stats := range items {
		requested += atoiDefault(stats["mem_requested"], 0)
	}
	if out.Bytes > requested && requested > 0 {
		out.WastedBytes = out.Bytes - requested
	}
	out.WastedPct = percent(out.WastedBytes, out.Bytes)
	return out
}

// readSlabs joins "stats slabs" with "stats items" so each class carries both
// its allocation shape and the live item counts that explain the fragmentation.
func (s *Session) readSlabs(ctx context.Context) ([]slabRow, error) {
	var rows []slabRow
	err := s.withAdmin(ctx, func(c *textConn) error {
		slabs, err := c.stats(ctx, "slabs", s.opts.Timeout)
		if err != nil {
			return err
		}
		items, err := c.stats(ctx, "items", s.opts.Timeout)
		if err != nil {
			return err
		}
		rows = buildSlabRows(classStats(slabs, ""), classStats(items, "items:"))
		return nil
	})
	return rows, err
}

func buildSlabRows(slabs, items map[int]map[string]string) []slabRow {
	rows := make([]slabRow, 0, len(slabs))
	for class, stats := range slabs {
		item := items[class]
		row := slabRow{
			Class:         class,
			ChunkSize:     atoiDefault(stats["chunk_size"], 0),
			ChunksPerPage: atoiDefault(stats["chunks_per_page"], 0),
			TotalPages:    atoiDefault(stats["total_pages"], 0),
			TotalChunks:   atoiDefault(stats["total_chunks"], 0),
			UsedChunks:    atoiDefault(stats["used_chunks"], 0),
			FreeChunks:    atoiDefault(stats["free_chunks"], 0),
			GetHits:       atoiDefault(stats["get_hits"], 0),
			CmdSet:        atoiDefault(stats["cmd_set"], 0),
			Items:         atoiDefault(item["number"], 0),
			MemRequested:  atoiDefault(item["mem_requested"], 0),
			Age:           atoiDefault(item["age"], 0),
			Evicted:       atoiDefault(item["evicted"], 0),
			OutOfMemory:   atoiDefault(item["outofmemory"], 0),
		}
		row.AllocatedBytes = row.TotalChunks * row.ChunkSize
		row.UsedBytes = row.UsedChunks * row.ChunkSize
		if row.MemRequested > 0 && row.UsedBytes > row.MemRequested {
			row.WastedBytes = row.UsedBytes - row.MemRequested
		}
		row.WastedPct = percent(row.WastedBytes, row.UsedBytes)
		row.FillPct = percent(row.UsedChunks, row.TotalChunks)
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Class < rows[j].Class })
	return rows
}

func (s *Session) readLRU(ctx context.Context) ([]lruRow, error) {
	var rows []lruRow
	err := s.withAdmin(ctx, func(c *textConn) error {
		items, err := c.stats(ctx, "items", s.opts.Timeout)
		if err != nil {
			return err
		}
		rows = buildLRURows(classStats(items, "items:"))
		return nil
	})
	return rows, err
}

func buildLRURows(items map[int]map[string]string) []lruRow {
	rows := make([]lruRow, 0, len(items))
	for class, stats := range items {
		rows = append(rows, lruRow{
			Class:            class,
			Number:           atoiDefault(stats["number"], 0),
			NumberHot:        atoiDefault(stats["number_hot"], 0),
			NumberWarm:       atoiDefault(stats["number_warm"], 0),
			NumberCold:       atoiDefault(stats["number_cold"], 0),
			NumberTemp:       atoiDefault(stats["number_temp"], 0),
			Age:              atoiDefault(stats["age"], 0),
			AgeHot:           atoiDefault(stats["age_hot"], 0),
			AgeWarm:          atoiDefault(stats["age_warm"], 0),
			Evicted:          atoiDefault(stats["evicted"], 0),
			EvictedNonzero:   atoiDefault(stats["evicted_nonzero"], 0),
			EvictedTime:      atoiDefault(stats["evicted_time"], 0),
			OutOfMemory:      atoiDefault(stats["outofmemory"], 0),
			Reclaimed:        atoiDefault(stats["reclaimed"], 0),
			ExpiredUnfetched: atoiDefault(stats["expired_unfetched"], 0),
			EvictedUnfetched: atoiDefault(stats["evicted_unfetched"], 0),
			CrawlerReclaimed: atoiDefault(stats["crawler_reclaimed"], 0),
			MovesToCold:      atoiDefault(stats["moves_to_cold"], 0),
			MovesToWarm:      atoiDefault(stats["moves_to_warm"], 0),
			DirectReclaims:   atoiDefault(stats["direct_reclaims"], 0),
		})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Class < rows[j].Class })
	return rows
}

func (s *Session) readConns(ctx context.Context) ([]connRow, error) {
	var rows []connRow
	err := s.withAdmin(ctx, func(c *textConn) error {
		conns, err := c.stats(ctx, "conns", s.opts.Timeout)
		if err != nil {
			return err
		}
		rows = buildConnRows(classStats(conns, ""))
		return nil
	})
	return rows, err
}

func buildConnRows(conns map[int]map[string]string) []connRow {
	rows := make([]connRow, 0, len(conns))
	for fd, stats := range conns {
		rows = append(rows, connRow{
			FD:               fd,
			Addr:             stats["addr"],
			ListenAddr:       stats["listen_addr"],
			State:            stats["state"],
			SecsSinceLastCmd: atoiDefault(stats["secs_since_last_cmd"], 0),
		})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].FD < rows[j].FD })
	return rows
}

func (s *Session) readSettings(ctx context.Context) ([]settingRow, error) {
	var rows []settingRow
	err := s.withAdmin(ctx, func(c *textConn) error {
		settings, err := c.stats(ctx, "settings", s.opts.Timeout)
		if err != nil {
			return err
		}
		rows = make([]settingRow, 0, len(settings))
		for _, pair := range settings {
			rows = append(rows, settingRow{Name: pair.Name, Value: pair.Value})
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })
		return nil
	})
	return rows, err
}

// metricSample is one raw counter snapshot used to derive per-second rates.
type metricSample struct {
	At           time.Time
	CmdGet       int64
	CmdSet       int64
	GetHits      int64
	GetMisses    int64
	Evictions    int64
	BytesRead    int64
	BytesWritten int64
}

func sampleFrom(o serverOverview) metricSample {
	return metricSample{
		At:           time.Now(),
		CmdGet:       o.CmdGet,
		CmdSet:       o.CmdSet,
		GetHits:      o.GetHits,
		GetMisses:    o.GetMisses,
		Evictions:    o.Evictions,
		BytesRead:    o.BytesRead,
		BytesWritten: o.BytesWritten,
	}
}

// metricsWatcher turns each overview poll into a metrics frame. A failed poll
// is reported as an unavailable feed rather than skipped, so the panel says the
// server stopped answering instead of holding the last values forever.
type metricsWatcher struct {
	prev metricSample
}

func (w *metricsWatcher) frame(o serverOverview, err error) map[string]any {
	if err != nil {
		return map[string]any{
			"metricsAvailable": false,
			"message":          "Live statistics are unavailable: " + err.Error(),
		}
	}
	cur := sampleFrom(o)
	frame := metricFrame(o, w.prev, cur)
	w.prev = cur
	return frame
}

// overviewWatcher turns each overview poll into a watch event. A failed poll
// re-emits the last snapshot marked unreachable, so the property sheet keeps
// its context while stating that the values are no longer refreshing.
type overviewWatcher struct {
	last serverOverview
}

func newOverviewWatcher(address string) *overviewWatcher {
	return &overviewWatcher{last: serverOverview{Address: address}}
}

func (w *overviewWatcher) event(o serverOverview, err error) plugin.ResourceEvent {
	if err != nil {
		o = w.last
		o.Status = statusUnreachable
		o.StatusError = err.Error()
	} else {
		w.last = o
	}
	return plugin.ResourceEvent{
		Type:     "modified",
		Ref:      plugin.ResourceIdentity{Kind: "server", Name: o.Address, UID: o.Address},
		Resource: o,
	}
}

// metricFrame renders one metrics stream snapshot. Rates are windowed against
// the previous sample so the chart shows current load, not lifetime totals.
func metricFrame(o serverOverview, prev, cur metricSample) map[string]any {
	frame := map[string]any{
		"metricsAvailable": true,
		"currItems":        o.CurrItems,
		"totalItems":       o.TotalItems,
		"currConnections":  o.CurrConnections,
		"totalConnections": o.TotalConnections,
		"evictions":        o.Evictions,
		"reclaimed":        o.Reclaimed,
		"uptime":           o.Uptime,
		"hitRatio":         o.HitRatio,
		"memoryPct":        o.MemoryPct,
		"memoryUsed":       o.Bytes,
		"memoryTotal":      o.LimitMaxBytes,
		"connectionsPct":   o.ConnectionsPct,
		"connectionsUsed":  o.CurrConnections,
		"connectionsTotal": o.MaxConnections,
		"wastedBytes":      o.WastedBytes,
	}
	elapsed := cur.At.Sub(prev.At).Seconds()
	if prev.At.IsZero() || elapsed <= 0 {
		return frame
	}
	rate := func(now, before int64) float64 {
		if now < before {
			return 0
		}
		return round2(float64(now-before) / elapsed)
	}
	frame["getsPerSec"] = rate(cur.CmdGet, prev.CmdGet)
	frame["setsPerSec"] = rate(cur.CmdSet, prev.CmdSet)
	frame["evictionsPerSec"] = rate(cur.Evictions, prev.Evictions)
	frame["bytesReadPerSec"] = rate(cur.BytesRead, prev.BytesRead)
	frame["bytesWrittenPerSec"] = rate(cur.BytesWritten, prev.BytesWritten)
	hits := cur.GetHits - prev.GetHits
	misses := cur.GetMisses - prev.GetMisses
	if hits+misses > 0 {
		frame["liveHitRatio"] = percent(hits, hits+misses)
	}
	return frame
}

func percent(part, total int64) float64 {
	if total <= 0 {
		return 0
	}
	return round2(float64(part) / float64(total) * 100)
}

func round2(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return math.Round(v*100) / 100
}

func timeUnix(sec int64) string {
	return time.Unix(sec, 0).UTC().Format(time.RFC3339)
}
