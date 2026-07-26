// Package memcached implements the memcached protocol plugin.
package memcached

import (
	"context"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

type Plugin struct{}

const iconSvg = `<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 96 96" id="Memcached--Streamline-Svg-Logos" height="24" width="24"><desc>Memcached Streamline Icon: https://streamlinehq.com</desc><path fill="url(#a)" d="M1 64.5135v-33.027C1 4.81081 4.80685 1 31.455 1h33.0901C91.1932 1 95 4.81081 95 31.4865v33.027C95 91.1892 91.1932 95 64.5451 95H31.455C4.80685 95 1 91.1892 1 64.5135Z"></path><path fill="url(#b)" d="M21.2873 18.7168C16.6668 48 19.0842 75.4167 19.0842 75.4167h14.4427c-1.3743-7.3102-6.3042-40.7088-2.2031-40.819 2.1978.3488 12.2395 28.3346 12.2395 28.3346s2.2115-.2754 4.4369-.2754c2.2254 0 4.4368.2754 4.4368.2754s10.0418-27.9858 12.2396-28.3346c4.1011.1102-.8289 33.5088-2.2031 40.819h14.4427S79.3335 48 74.7131 18.7168H61.3413c-2.5451.0301-12.2284 17.013-13.3411 17.013-1.1127 0-10.796-16.9829-13.3412-17.013H21.2873Z"></path><path fill="url(#c)" d="M45.1513 71.9985c0 1.8878-1.5304 3.4182-3.4182 3.4182-1.8878 0-3.4182-1.5304-3.4182-3.4182 0-1.8878 1.5304-3.4182 3.4182-3.4182 1.8878 0 3.4182 1.5304 3.4182 3.4182Z"></path><path fill="url(#d)" d="M57.685 71.9985c0 1.8878-1.5304 3.4182-3.4182 3.4182-1.8878 0-3.4182-1.5304-3.4182-3.4182 0-1.8878 1.5304-3.4182 3.4182-3.4182 1.8878 0 3.4182 1.5304 3.4182 3.4182Z"></path><path fill="#000000" d="M74.0321 19.6849c2.0843 14.1081 2.5733 27.6481 2.535 37.7339-.0393 10.3163-.6303 17.0187-.6303 17.0187H63.7799l-1.3066.9792H76.916S79.3333 48 74.7129 18.7168l-.6808.9681Zm-38.2717-.3561c3.7941 4.2204 10.3564 15.4219 11.2604 15.4219-2.4057-3.0768-8.3149-12.8922-11.2604-15.4219Zm-5.416 14.2897c-4.1011.1103.8289 33.5088 2.2031 40.819H19.9397l-.8557.9792h14.4427c-1.3673-7.2732-6.2539-40.3752-2.2643-40.819-.3715-.558-.6924-.9434-.918-.9792Zm33.3529 0c-2.1979.3488-12.2396 28.3346-12.2396 28.3346s-2.2115-.2754-4.4369-.2754c-1.3167 0-2.4502.0812-3.2168.1526l-.2409 1.102s2.2115-.2754 4.4369-.2754c2.2254 0 4.4368.2754 4.4368.2754s9.9662-27.8022 12.209-28.3346c-.2417-.6123-.5403-.9682-.9485-.9792Z" opacity=".1"></path><path fill="#ffffff" d="M21.2873 18.7168C16.6668 48 19.0842 75.4167 19.0842 75.4167l.8504-.9559c-.4352-6.4684-1.5978-29.8595 2.3319-54.7648h13.3717c.2833.0033.6631.2354 1.1016.6119-.8756-.9739-1.6035-1.5855-2.0808-1.5911H21.2873Zm40.054 0c-2.5451.0301-12.2284 17.013-13.3411 17.013.4537.5803.8222.9792.9792.9792 1.1127 0 10.796-16.983 13.3411-17.013h11.8003l.5923-.9792H61.3413Zm-29.0996 16.86c3.2481 4.8782 11.3216 27.3555 11.3216 27.3555l.2363-1.0961c-1.9933-5.4657-9.5885-25.9565-11.4967-26.2594-.0208.0006-.0409-.0022-.0612 0Zm33.4141 0c2.1787 5.5568-1.9455 33.2607-3.1823 39.8399l1.3102-1.0227c1.6723-9.8119 5.6994-38.7142 1.8721-38.8172Z" opacity=".3"></path><defs><radialGradient id="c" cx="0" cy="0" r="1" gradientTransform="translate(321.384 360.55) scale(341.818)" gradientUnits="userSpaceOnUse"><stop stop-color="#db7c7c"></stop><stop offset="1" stop-color="#c83737"></stop></radialGradient><radialGradient id="d" cx="0" cy="0" r="1" gradientTransform="translate(353.5 360.55) scale(341.818)" gradientUnits="userSpaceOnUse"><stop stop-color="#db7c7c"></stop><stop offset="1" stop-color="#c83737"></stop></radialGradient><linearGradient id="a" x1="4701" x2="4701" y1="9401" y2=".999" gradientUnits="userSpaceOnUse"><stop stop-color="#574c4a"></stop><stop offset="1" stop-color="#80716d"></stop></linearGradient><linearGradient id="b" x1="5264.9" x2="2015.86" y1="5594.7" y2="-586.8" gradientUnits="userSpaceOnUse"><stop stop-color="#268d83"></stop><stop offset="1" stop-color="#2ea19e"></stop></linearGradient></defs></svg>`

func New() *Plugin { return &Plugin{} }

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:  plugin.CurrentAPIVersion,
		Name:        protocolName,
		Version:     "0.1.0",
		Title:       "Memcached",
		Description: "Memcached workspace with an LRU key browser, live cache metrics, a slab allocation map, segmented-LRU and connection statistics, a watcher activity feed, and an audited command console.",
		Icon:        plugin.Icon{Type: plugin.IconSVG, Value: iconSvg},
		Category:    plugin.CategoryDatabases,
		Config:      configSchema(),
		Capabilities: []plugin.Capability{
			"kv", "keys", "stats", "slabs", "lru", "metrics", "console", "watch",
		},
		SupportedTransports: []plugin.Transport{plugin.TransportDirect, plugin.TransportAgent},
		Agent: &plugin.AgentProfile{
			Proxy: plugin.ProxyTarget{Mode: plugin.AgentTCP, Risk: plugin.RiskPrivileged, Forward: true},
			Install: []plugin.InstallArtifact{{
				Label:    "Docker",
				Kind:     "docker",
				Template: "docker run -d --network host shellcn/agent --connect {{shellquote .ConnectURL}} --token {{shellquote .Token}}",
			}},
		},
		Layout:        plugin.LayoutTabs,
		Scope:         []plugin.ScopeFilter{slabClassScope()},
		Tabs:          tabs(),
		Actions:       actions(),
		HeaderActions: headerActions(),
		Streams:       streams(),
	}
}

func (p *Plugin) Routes() []plugin.Route { return routes() }

func (p *Plugin) Connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	return connect(ctx, cfg)
}

func icon(name string) plugin.Icon {
	return plugin.Icon{Type: plugin.IconLucide, Value: name}
}

func streams() []plugin.Stream {
	return []plugin.Stream{
		{ID: "memcached.metrics", Kind: plugin.StreamMetrics, RouteID: "memcached.metrics"},
		{ID: "memcached.overview.watch", Kind: plugin.StreamResource, RouteID: "memcached.overview.watch"},
		{ID: "memcached.slabs.canvas", Kind: plugin.StreamCanvas, RouteID: "memcached.slabs.canvas"},
		{ID: "memcached.events.watch", Kind: plugin.StreamResource, RouteID: "memcached.events.watch"},
		{ID: "memcached.console", Kind: plugin.StreamQuery, RouteID: "memcached.console"},
	}
}

// slabClassScope narrows the key browser, slab table, and LRU table to one slab
// class. Slab classes are memcached's only native partition of the keyspace.
func slabClassScope() plugin.ScopeFilter {
	return plugin.ScopeFilter{
		Param:         classScopeParam,
		Label:         "Slab class",
		Icon:          icon("layers"),
		Control:       plugin.ScopeSelect,
		OptionsSource: &plugin.DataSource{RouteID: "memcached.slabs.options"},
		ValueField:    "value",
		LabelField:    "label",
		DefaultValue:  classScopeAll,
	}
}

func tabs() []plugin.Panel {
	return []plugin.Panel{
		{Key: "overview", Label: "Overview", Icon: icon("gauge"), Type: plugin.PanelDashboard, Config: overviewDashboard()},
		{Key: "keys", Label: "Keys", Icon: icon("key-round"), Type: plugin.PanelKV,
			Source: &plugin.DataSource{RouteID: "memcached.keys.list"},
			Config: plugin.KVConfig{
				CreateRouteID: "memcached.key.write",
				ReadRouteID:   "memcached.key.read",
				WriteRouteID:  "memcached.key.write",
				DeleteRouteID: "memcached.key.delete",
				KeyParam:      "key",
				Writable:      true,
				ValueTypes:    []string{valueText, valueJSON, valueBinary},
				Delimiter:     keyDelimiter,
			}},
		{Key: "slabs", Label: "Slabs", Icon: icon("layers"), Type: plugin.PanelSplit, Config: slabSplit()},
		{Key: "lru", Label: "LRU", Icon: icon("history"), Type: plugin.PanelTable,
			Source: &plugin.DataSource{RouteID: "memcached.items.list"},
			Config: plugin.TableConfig{
				Columns:           lruColumns(),
				DefaultSort:       &plugin.SortKey{Field: "class"},
				EmptyText:         "No slab class holds items yet.",
				Exportable:        true,
				RefreshIntervalMs: 10000,
				RowClick:          plugin.RowClickDetail,
			}},
		{Key: "connections", Label: "Connections", Icon: icon("plug"), Type: plugin.PanelTable,
			Source: &plugin.DataSource{RouteID: "memcached.conns.list"},
			Config: plugin.TableConfig{
				Columns:           connColumns(),
				DefaultSort:       &plugin.SortKey{Field: "fd"},
				EmptyText:         "No client connections reported.",
				Exportable:        true,
				RefreshIntervalMs: 5000,
				RowClick:          plugin.RowClickDetail,
			}},
		{Key: "settings", Label: "Settings", Icon: icon("sliders-horizontal"), Type: plugin.PanelTable,
			Source: &plugin.DataSource{RouteID: "memcached.settings.list"},
			Config: plugin.TableConfig{
				Columns:     settingColumns(),
				DefaultSort: &plugin.SortKey{Field: "name"},
				EmptyText:   "No runtime settings reported.",
				Exportable:  true,
			}},
		{Key: "activity", Label: "Activity", Icon: icon("activity"), Type: plugin.PanelTimeline,
			Source: &plugin.DataSource{RouteID: "memcached.events.list"},
			Config: plugin.TimelineConfig{
				TimestampField: "time",
				TitleField:     "reason",
				BodyField:      "message",
				SeverityField:  "severity",
				IconField:      "icon",
				ResourceField:  "resource",
				EmptyText:      "No watcher events yet. memcached only reports events that happen while this tab's watcher is attached.",
				Watch:          &plugin.DataSource{RouteID: "memcached.events.watch", Method: plugin.MethodWS},
			}},
		{Key: "console", Label: "Console", Icon: icon("square-terminal"), Type: plugin.PanelQueryEditor,
			Source: &plugin.DataSource{RouteID: "memcached.console", Method: plugin.MethodWS},
			Config: plugin.QueryEditorConfig{
				Language:          "plaintext",
				Label:             "memcached command",
				ExecuteLabel:      "Run",
				RunningLabel:      "Running",
				CancelLabel:       "Cancel",
				EmptyText:         "Run a memcached command, for example \"stats slabs\" or \"mg mykey v t f\".",
				InitialQuery:      "stats",
				CompletionRouteID: "memcached.console.complete",
				Exportable:        true,
			}},
		{Key: "commands", Label: "Saved commands", Icon: icon("bookmark"), Type: plugin.PanelTable,
			Source: &plugin.DataSource{RouteID: "memcached.commands.list"},
			Config: plugin.TableConfig{
				Columns:      savedCommandColumns(),
				ActionIDs:    []string{"memcached.commands.save"},
				RowActionIDs: []string{"memcached.commands.delete"},
				DefaultSort:  &plugin.SortKey{Field: "name"},
				EmptyText:    "No saved commands yet.",
				RowClick:     plugin.RowClickDetail,
			}},
	}
}

// slabSplit pairs the allocation map with the numeric slab table so an operator
// can spot a fragmented class visually and then read its exact counters.
func slabSplit() plugin.SplitConfig {
	return plugin.SplitConfig{
		Orientation: plugin.SplitVertical,
		Panels: []plugin.SplitPanel{
			{
				Size:    58,
				MinSize: 30,
				Panel: plugin.Panel{
					Key: "map", Label: "Allocation map", Icon: icon("layout-grid"), Type: plugin.PanelCanvas,
					Source: &plugin.DataSource{RouteID: "memcached.slabs.canvas", Method: plugin.MethodWS},
					Config: plugin.CanvasConfig{
						ScaleMode:      plugin.CanvasScaleResize,
						ResizeEvents:   true,
						Interactive:    true,
						Pointer:        true,
						Keyboard:       true,
						WheelMode:      plugin.CanvasWheelModified,
						HiDPI:          true,
						FocusOnPointer: true,
						AriaLabel:      "Memcached slab class allocation map",
						Instructions:   "Each row is one slab class. The bar shows allocated capacity, the filled part shows used chunks, and the amber part is memory lost to chunk rounding. Use Up and Down to select a class.",
					},
				},
			},
			{
				Size:    42,
				MinSize: 20,
				Panel: plugin.Panel{
					Key: "table", Label: "Slab classes", Icon: icon("table"), Type: plugin.PanelTable,
					Source: &plugin.DataSource{RouteID: "memcached.slabs.list"},
					Config: plugin.TableConfig{
						Columns:           slabColumns(),
						DefaultSort:       &plugin.SortKey{Field: "class"},
						EmptyText:         "No slab pages allocated yet.",
						Exportable:        true,
						RefreshIntervalMs: 10000,
						RowClick:          plugin.RowClickDetail,
					},
				},
			},
		},
	}
}

func overviewDashboard() plugin.DashboardConfig {
	return plugin.DashboardConfig{Cells: []plugin.Panel{
		{Key: "live", Label: "Cache activity", Icon: icon("activity"), Type: plugin.PanelMetrics, Span: 2,
			Source: &plugin.DataSource{RouteID: "memcached.metrics", Method: plugin.MethodWS},
			Config: metricsConfig()},
		{Key: "server", Label: "Server", Icon: icon("info"), Type: plugin.PanelObjectDetail, Span: 2,
			Source: &plugin.DataSource{RouteID: "memcached.overview"},
			Config: overviewDetail()},
	}}
}

func metricsConfig() plugin.MetricsConfig {
	return plugin.MetricsConfig{
		Stats: []plugin.MetricStat{
			{Key: "currItems", Label: "Items"},
			{Key: "currConnections", Label: "Connections"},
			{Key: "evictions", Label: "Evictions"},
			{Key: "getsPerSec", Label: "Gets/sec"},
			{Key: "setsPerSec", Label: "Sets/sec"},
			{Key: "wastedBytes", Label: "Chunk slack", Unit: "bytes"},
			{Key: "uptime", Label: "Uptime", Unit: "s"},
		},
		Gauges: []plugin.MetricGauge{
			{Key: "hitRatio", Label: "Lifetime hit ratio", Unit: "%"},
		},
		Usage: []plugin.MetricUsage{
			{Key: "memoryPct", Label: "Cache memory", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
				PercentKey: "memoryPct", UsedKey: "memoryUsed", TotalKey: "memoryTotal",
				UsedType: plugin.ColumnBytes, TotalType: plugin.ColumnBytes, TotalLabel: "of",
				WarnAt: 80, CriticalAt: 95,
			}},
			{Key: "connectionsPct", Label: "Connection slots", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
				PercentKey: "connectionsPct", UsedKey: "connectionsUsed", TotalKey: "connectionsTotal",
				UsedType: plugin.ColumnNumber, TotalType: plugin.ColumnNumber, TotalLabel: "of", Unit: "connection(s)",
				WarnAt: 75, CriticalAt: 90,
			}},
		},
		Series: []plugin.MetricSeries{
			{Key: "getsPerSec", Label: "Gets", Unit: "/s"},
			{Key: "setsPerSec", Label: "Sets", Unit: "/s"},
			{Key: "evictionsPerSec", Label: "Evictions", Unit: "/s"},
			{Key: "liveHitRatio", Label: "Hit ratio", Unit: "%"},
		},
		History: 120,
	}
}

func overviewDetail() plugin.ObjectDetailConfig {
	return plugin.ObjectDetailConfig{
		RawToggle: true,
		Watch:     &plugin.DataSource{RouteID: "memcached.overview.watch", Method: plugin.MethodWS},
		Sections: []plugin.ObjectDetailSection{
			{Title: "Server", Fields: []plugin.ObjectDetailField{
				{Key: "status", Label: "Status", Type: plugin.ColumnBadge, Severities: map[string]plugin.Severity{
					statusOK: plugin.SeveritySuccess, statusUnreachable: plugin.SeverityDanger,
				}},
				{Key: "statusError", Label: "Last failure"},
				{Key: "address", Label: "Address", Copy: true},
				{Key: "version", Label: "Version", Copy: true},
				{Key: "pid", Label: "PID", Type: plugin.ColumnNumber},
				{Key: "uptime", Label: "Uptime", Type: plugin.ColumnDuration},
				{Key: "serverTime", Label: "Server time", Type: plugin.ColumnDateTime},
				{Key: "threads", Label: "Worker threads", Type: plugin.ColumnNumber},
				{Key: "pointerSize", Label: "Pointer size", Type: plugin.ColumnNumber},
				{Key: "maxItemSize", Label: "Max item size", Type: plugin.ColumnBytes},
				{Key: "readOnly", Label: "Read-only connection", Type: plugin.ColumnBool},
			}},
			{Title: "Memory", Fields: []plugin.ObjectDetailField{
				{Key: "memoryPct", Label: "Cache memory", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
					PercentKey: "memoryPct", UsedKey: "bytes", TotalKey: "limitMaxBytes",
					UsedType: plugin.ColumnBytes, TotalType: plugin.ColumnBytes, TotalLabel: "of",
					WarnAt: 80, CriticalAt: 95,
				}},
				{Key: "wastedPct", Label: "Chunk slack", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
					PercentKey: "wastedPct", UsedKey: "wastedBytes", TotalKey: "bytes",
					UsedType: plugin.ColumnBytes, TotalType: plugin.ColumnBytes, TotalLabel: "of",
					WarnAt: 20, CriticalAt: 40,
				}},
				{Key: "totalMalloced", Label: "Slab pages allocated", Type: plugin.ColumnBytes},
				{Key: "activeSlabs", Label: "Active slab classes", Type: plugin.ColumnNumber},
			}},
			{Title: "Items", Fields: []plugin.ObjectDetailField{
				{Key: "currItems", Label: "Current items", Type: plugin.ColumnNumber},
				{Key: "totalItems", Label: "Items stored since start", Type: plugin.ColumnNumber},
				{Key: "evictions", Label: "Evictions", Type: plugin.ColumnNumber},
				{Key: "reclaimed", Label: "Reclaimed expired slots", Type: plugin.ColumnNumber},
				{Key: "expiredUnfetched", Label: "Expired before first read", Type: plugin.ColumnNumber},
				{Key: "evictedUnfetched", Label: "Evicted before first read", Type: plugin.ColumnNumber},
			}},
			{Title: "Traffic", Fields: []plugin.ObjectDetailField{
				{Key: "hitRatio", Label: "Lifetime hit ratio", Type: plugin.ColumnPercent},
				{Key: "cmdGet", Label: "Gets", Type: plugin.ColumnNumber},
				{Key: "cmdSet", Label: "Sets", Type: plugin.ColumnNumber},
				{Key: "cmdTouch", Label: "Touches", Type: plugin.ColumnNumber},
				{Key: "cmdFlush", Label: "Flushes", Type: plugin.ColumnNumber},
				{Key: "getHits", Label: "Get hits", Type: plugin.ColumnNumber},
				{Key: "getMisses", Label: "Get misses", Type: plugin.ColumnNumber},
				{Key: "getExpired", Label: "Gets that found expired items", Type: plugin.ColumnNumber},
				{Key: "bytesRead", Label: "Bytes read", Type: plugin.ColumnBytes},
				{Key: "bytesWritten", Label: "Bytes written", Type: plugin.ColumnBytes},
			}},
			{Title: "Command outcomes", Fields: []plugin.ObjectDetailField{
				{Key: "deleteHits", Label: "Delete hits", Type: plugin.ColumnNumber},
				{Key: "deleteMisses", Label: "Delete misses", Type: plugin.ColumnNumber},
				{Key: "incrHits", Label: "Increment hits", Type: plugin.ColumnNumber},
				{Key: "incrMisses", Label: "Increment misses", Type: plugin.ColumnNumber},
				{Key: "decrHits", Label: "Decrement hits", Type: plugin.ColumnNumber},
				{Key: "decrMisses", Label: "Decrement misses", Type: plugin.ColumnNumber},
				{Key: "casHits", Label: "CAS hits", Type: plugin.ColumnNumber},
				{Key: "casMisses", Label: "CAS misses", Type: plugin.ColumnNumber},
				{Key: "casBadval", Label: "CAS conflicts", Type: plugin.ColumnNumber},
				{Key: "touchHits", Label: "Touch hits", Type: plugin.ColumnNumber},
				{Key: "touchMisses", Label: "Touch misses", Type: plugin.ColumnNumber},
			}},
			{Title: "Connections", Fields: []plugin.ObjectDetailField{
				{Key: "connectionsPct", Label: "Connection slots", Type: plugin.ColumnPercent, Usage: &plugin.UsageSpec{
					PercentKey: "connectionsPct", UsedKey: "currConnections", TotalKey: "maxConnections",
					UsedType: plugin.ColumnNumber, TotalType: plugin.ColumnNumber, TotalLabel: "of", Unit: "connection(s)",
					WarnAt: 75, CriticalAt: 90,
				}},
				{Key: "totalConnections", Label: "Connections since start", Type: plugin.ColumnNumber},
				{Key: "rejectedConnections", Label: "Rejected connections", Type: plugin.ColumnNumber},
				{Key: "lruCrawlerEnabled", Label: "LRU crawler enabled", Type: plugin.ColumnBool},
				{Key: "casEnabled", Label: "CAS enabled", Type: plugin.ColumnBool},
				{Key: "authEnabled", Label: "Authentication enabled", Type: plugin.ColumnBool},
			}},
		},
	}
}

func slabColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "class", Label: "Class", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "chunkSize", Label: "Chunk size", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "chunksPerPage", Label: "Chunks/page", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "totalPages", Label: "Pages", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "usedChunks", Label: "Used chunks", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "freeChunks", Label: "Free chunks", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "fillPct", Label: "Fill", Type: plugin.ColumnPercent, Sortable: true},
		{Key: "allocatedBytes", Label: "Allocated", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "memRequested", Label: "Item payload", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "wastedBytes", Label: "Chunk slack", Type: plugin.ColumnBytes, Sortable: true},
		{Key: "wastedPct", Label: "Slack", Type: plugin.ColumnPercent, Sortable: true},
		{Key: "items", Label: "Items", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "age", Label: "Oldest item", Type: plugin.ColumnDuration, Sortable: true},
		{Key: "evicted", Label: "Evicted", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "outOfMemory", Label: "Out of memory", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "getHits", Label: "Get hits", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "cmdSet", Label: "Sets", Type: plugin.ColumnNumber, Sortable: true},
	}
}

func lruColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "class", Label: "Class", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "number", Label: "Items", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "numberHot", Label: "Hot", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "numberWarm", Label: "Warm", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "numberCold", Label: "Cold", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "numberTemp", Label: "Temp", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "age", Label: "Oldest item", Type: plugin.ColumnDuration, Sortable: true},
		{Key: "ageHot", Label: "Oldest hot", Type: plugin.ColumnDuration, Sortable: true},
		{Key: "ageWarm", Label: "Oldest warm", Type: plugin.ColumnDuration, Sortable: true},
		{Key: "evicted", Label: "Evicted", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "evictedNonzero", Label: "Evicted with TTL", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "evictedTime", Label: "Since last eviction", Type: plugin.ColumnDuration, Sortable: true},
		{Key: "evictedUnfetched", Label: "Evicted unread", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "expiredUnfetched", Label: "Expired unread", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "outOfMemory", Label: "Out of memory", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "reclaimed", Label: "Reclaimed", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "crawlerReclaimed", Label: "Crawler reclaimed", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "movesToCold", Label: "Moves to cold", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "movesToWarm", Label: "Moves to warm", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "directReclaims", Label: "Direct reclaims", Type: plugin.ColumnNumber, Sortable: true},
	}
}

func connColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "fd", Label: "FD", Type: plugin.ColumnNumber, Sortable: true},
		{Key: "addr", Label: "Peer", Sortable: true},
		{Key: "listenAddr", Label: "Listener", Sortable: true},
		{Key: "state", Label: "State", Type: plugin.ColumnBadge, Sortable: true, Severities: map[string]plugin.Severity{
			"conn_listening": plugin.SeverityInfo,
			"conn_new_cmd":   plugin.SeveritySuccess,
			"conn_read":      plugin.SeveritySuccess,
			"conn_waiting":   plugin.SeverityInfo,
			"conn_closing":   plugin.SeverityWarn,
			"conn_closed":    plugin.SeverityDanger,
		}},
		{Key: "secsSinceLastCmd", Label: "Idle", Type: plugin.ColumnDuration, Sortable: true},
	}
}

func settingColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Setting", Sortable: true},
		{Key: "value", Label: "Value", Sortable: true},
	}
}

func savedCommandColumns() []plugin.Column {
	return []plugin.Column{
		{Key: "name", Label: "Name", Sortable: true},
		{Key: "command", Label: "Command"},
		{Key: "updatedAt", Label: "Updated", Type: plugin.ColumnRelativeTime, Sortable: true},
	}
}

func headerActions() []string {
	return []string{
		"memcached.key.store", "memcached.key.touch", "memcached.key.incr", "memcached.key.decr",
		"memcached.server.crawl", "memcached.server.stats_reset", "memcached.server.memlimit",
		"memcached.server.verbosity", "memcached.server.flush",
	}
}

func actions() []plugin.Action {
	const keyGroup = "Key operations"
	const serverGroup = "Server"
	return []plugin.Action{
		{ID: "memcached.key.store", Label: "Store item", Icon: icon("plus"), RouteID: "memcached.key.store", Group: keyGroup,
			OnSuccess: &plugin.ActionSuccess{SelectTab: "keys"}},
		{ID: "memcached.key.touch", Label: "Touch TTL", Icon: icon("timer"), RouteID: "memcached.key.touch", Group: keyGroup},
		{ID: "memcached.key.incr", Label: "Increment", Icon: icon("trending-up"), RouteID: "memcached.key.incr", Group: keyGroup},
		{ID: "memcached.key.decr", Label: "Decrement", Icon: icon("trending-down"), RouteID: "memcached.key.decr", Group: keyGroup},

		{ID: "memcached.server.crawl", Label: "Run LRU crawler", Icon: icon("broom"), RouteID: "memcached.server.crawl", Group: serverGroup},
		{ID: "memcached.server.stats_reset", Label: "Reset statistics", Icon: icon("rotate-ccw"), RouteID: "memcached.server.stats_reset", Group: serverGroup,
			Confirm: true, ConfirmText: "Reset every counter on this memcached server? Hit ratios, eviction counts, and traffic totals restart from zero for all clients."},
		{ID: "memcached.server.memlimit", Label: "Set memory limit", Icon: icon("hard-drive"), RouteID: "memcached.server.memlimit", Group: serverGroup,
			Confirm: true, ConfirmText: "Change the cache memory limit at runtime? Lowering it can force memcached to evict live items."},
		{ID: "memcached.server.verbosity", Label: "Set verbosity", Icon: icon("scroll-text"), RouteID: "memcached.server.verbosity", Group: serverGroup,
			Confirm: true, ConfirmText: "Change server log verbosity? High verbosity can flood the memcached host's logs."},
		{ID: "memcached.server.flush", Label: "Flush cache", Icon: icon("trash-2"), RouteID: "memcached.server.flush", Group: serverGroup,
			Confirm: true, ConfirmText: "Invalidate every item on this memcached server? All clients lose their cached data immediately and will fall through to the origin."},

		{ID: "memcached.commands.save", Label: "Save command", Icon: icon("bookmark-plus"), RouteID: "memcached.commands.save",
			OnSuccess: &plugin.ActionSuccess{SelectTab: "commands"}},
		{ID: "memcached.commands.delete", Label: "Delete", Icon: icon("trash-2"), RouteID: "memcached.commands.delete",
			Params: map[string]string{"id": "${record.id}"}, Confirm: true, ConfirmText: "Delete this saved command?", Bulk: true},
	}
}

func storeSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Item", Fields: []plugin.Field{
		{Key: "key", Label: "Key", Type: plugin.FieldText, Required: true, Default: "${record.key}", Validators: []plugin.Validator{
			{Type: plugin.ValidatorRegex, Value: `^[^\s\x00]{1,250}$`, Message: "up to 250 bytes, no spaces or control characters"},
		}},
		{Key: "mode", Label: "Mode", Type: plugin.FieldSelect, Required: true, Default: "set", Options: []plugin.Option{
			{Label: "Set (store unconditionally)", Value: "set"},
			{Label: "Add (only if the key is absent)", Value: "add"},
			{Label: "Replace (only if the key exists)", Value: "replace"},
			{Label: "Append (to an existing value)", Value: "append"},
			{Label: "Prepend (to an existing value)", Value: "prepend"},
			{Label: "Compare and swap", Value: "cas"},
		}},
		{Key: "type", Label: "Value type", Type: plugin.FieldSelect, Required: true, Default: valueText, Options: []plugin.Option{
			{Label: "Text", Value: valueText},
			{Label: "JSON", Value: valueJSON},
			{Label: "Binary (base64)", Value: valueBinary},
		}},
		{Key: "value", Label: "Value", Type: plugin.FieldTextarea},
		{Key: "ttl", Label: "TTL (seconds)", Type: plugin.FieldNumber, Default: 0, Validators: []plugin.Validator{
			{Type: plugin.ValidatorMin, Value: 0},
		}, Help: "0 never expires. Values above 2592000 (30 days) are read by memcached as an absolute Unix timestamp."},
		{Key: "flags", Label: "Client flags", Type: plugin.FieldNumber, Default: 0, Validators: []plugin.Validator{
			{Type: plugin.ValidatorMin, Value: 0}, {Type: plugin.ValidatorMax, Value: 4294967295},
		}, Help: "Opaque 32-bit value clients use to tag the serialization format."},
		{Key: "cas", Label: "CAS id", Type: plugin.FieldNumber, Default: 0, VisibleWhen: &plugin.Condition{AllOf: []plugin.Rule{
			{Field: "mode", Op: plugin.OpEq, Value: "cas"},
		}}, Help: "The CAS id read from the item; the store fails if the item changed since."},
	}}}}
}

func touchSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Fields: []plugin.Field{
		{Key: "key", Label: "Key", Type: plugin.FieldText, Required: true, Default: "${record.key}"},
		{Key: "ttl", Label: "New TTL (seconds)", Type: plugin.FieldNumber, Required: true, Default: 300, Validators: []plugin.Validator{
			{Type: plugin.ValidatorMin, Value: 0},
		}, Help: "0 removes the expiry."},
	}}}}
}

func counterSchema(deltaLabel string) *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Fields: []plugin.Field{
		{Key: "key", Label: "Key", Type: plugin.FieldText, Required: true, Default: "${record.key}"},
		{Key: "delta", Label: deltaLabel, Type: plugin.FieldNumber, Required: true, Default: 1, Validators: []plugin.Validator{
			{Type: plugin.ValidatorMin, Value: 1},
		}, Help: "The stored value must be an unsigned decimal number."},
	}}}}
}

func flushSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Fields: []plugin.Field{
		{Key: "delay", Label: "Delay (seconds)", Type: plugin.FieldNumber, Default: 0, Validators: []plugin.Validator{
			{Type: plugin.ValidatorMin, Value: 0},
		}, Help: "0 invalidates immediately. A delay lets a pool of servers be flushed in staggered waves."},
	}}}}
}

func memlimitSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Fields: []plugin.Field{
		{Key: "megabytes", Label: "Cache memory limit (MB)", Type: plugin.FieldNumber, Required: true, Default: 64, Validators: []plugin.Validator{
			{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 1048576},
		}},
	}}}}
}

func verbositySchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Fields: []plugin.Field{
		{Key: "level", Label: "Verbosity", Type: plugin.FieldSelect, Required: true, Default: 0, Options: []plugin.Option{
			{Label: "0 — errors only", Value: 0},
			{Label: "1 — connection events", Value: 1},
			{Label: "2 — command detail", Value: 2},
			{Label: "3 — full trace", Value: 3},
		}},
	}}}}
}

func crawlSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Fields: []plugin.Field{
		{Key: "classes", Label: "Slab classes", Type: plugin.FieldText, Default: classScopeAll,
			Help: "\"all\", or a comma separated list of slab class ids to reclaim expired items from."},
	}}}}
}

func savedCommandSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Fields: []plugin.Field{
		{Key: "name", Label: "Name", Type: plugin.FieldText, Required: true},
		{Key: "command", Label: "Command", Type: plugin.FieldTextarea, Required: true, Default: "stats slabs",
			Help: "Validated against the console allow-list before it is saved."},
	}}}}
}
