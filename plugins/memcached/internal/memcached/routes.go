package memcached

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	permRead     = "memcached.stats.read"
	permKeysRead = "memcached.keys.read"
	permKeysWr   = "memcached.keys.write"
	permKeysDel  = "memcached.keys.delete"
	permAdmin    = "memcached.server.admin"
	permConsole  = "memcached.console.exec"
	permQueryRd  = "memcached.commands.read"
	permQueryWr  = "memcached.commands.write"
)

type actionResult struct {
	OK      bool   `json:"ok"`
	Message string `json:"message,omitempty"`
	Value   string `json:"value,omitempty"`
}

func routes() []plugin.Route {
	return []plugin.Route{
		{ID: "memcached.overview", Method: plugin.MethodGet, Path: "/overview", Permission: permRead, Risk: plugin.RiskSafe, AuditEvent: "memcached.overview", Handle: overviewRoute},
		{ID: "memcached.overview.watch", Method: plugin.MethodWS, Path: "/overview/watch", Permission: permRead, Risk: plugin.RiskSafe, AuditEvent: "memcached.overview.watch", Stream: overviewWatchStream},
		{ID: "memcached.metrics", Method: plugin.MethodWS, Path: "/metrics", Permission: permRead, Risk: plugin.RiskSafe, AuditEvent: "memcached.metrics", Stream: metricsStream},

		{ID: "memcached.keys.list", Method: plugin.MethodGet, Path: "/keys", Permission: permKeysRead, Risk: plugin.RiskSafe, AuditEvent: "memcached.keys.list", Handle: listKeys},
		{ID: "memcached.key.read", Method: plugin.MethodGet, Path: "/keys/{key}", Permission: permKeysRead, Risk: plugin.RiskSafe, AuditEvent: "memcached.key.read", Handle: readKeyRoute},
		{ID: "memcached.key.write", Method: plugin.MethodPut, Path: "/keys/{key}", Permission: permKeysWr, Risk: plugin.RiskWrite, AuditEvent: "memcached.key.write", Handle: writeKeyRoute},
		{ID: "memcached.key.delete", Method: plugin.MethodDelete, Path: "/keys/{key}", Permission: permKeysDel, Risk: plugin.RiskDestructive, AuditEvent: "memcached.key.delete", Handle: deleteKeyRoute},
		{ID: "memcached.key.store", Method: plugin.MethodPost, Path: "/keys", Permission: permKeysWr, Risk: plugin.RiskWrite, AuditEvent: "memcached.key.store", Input: storeSchema(), Handle: storeKeyRoute},
		{ID: "memcached.key.touch", Method: plugin.MethodPost, Path: "/keys/touch", Permission: permKeysWr, Risk: plugin.RiskWrite, AuditEvent: "memcached.key.touch", Input: touchSchema(), Handle: touchKeyRoute},
		{ID: "memcached.key.incr", Method: plugin.MethodPost, Path: "/keys/incr", Permission: permKeysWr, Risk: plugin.RiskWrite, AuditEvent: "memcached.key.incr", Input: counterSchema("Increment by"), Handle: incrKeyRoute},
		{ID: "memcached.key.decr", Method: plugin.MethodPost, Path: "/keys/decr", Permission: permKeysWr, Risk: plugin.RiskWrite, AuditEvent: "memcached.key.decr", Input: counterSchema("Decrement by"), Handle: decrKeyRoute},

		{ID: "memcached.slabs.list", Method: plugin.MethodGet, Path: "/slabs", Permission: permRead, Risk: plugin.RiskSafe, AuditEvent: "memcached.slabs.list", Handle: listSlabs},
		{ID: "memcached.slabs.options", Method: plugin.MethodGet, Path: "/slabs/options", Permission: permRead, Risk: plugin.RiskSafe, AuditEvent: "memcached.slabs.options", Handle: listSlabOptions},
		{ID: "memcached.slabs.canvas", Method: plugin.MethodWS, Path: "/slabs/canvas", Permission: permRead, Risk: plugin.RiskSafe, AuditEvent: "memcached.slabs.canvas", Stream: slabCanvasStream},
		{ID: "memcached.items.list", Method: plugin.MethodGet, Path: "/items", Permission: permRead, Risk: plugin.RiskSafe, AuditEvent: "memcached.items.list", Handle: listItems},
		{ID: "memcached.conns.list", Method: plugin.MethodGet, Path: "/conns", Permission: permRead, Risk: plugin.RiskSafe, AuditEvent: "memcached.conns.list", Handle: listConns},
		{ID: "memcached.settings.list", Method: plugin.MethodGet, Path: "/settings", Permission: permRead, Risk: plugin.RiskSafe, AuditEvent: "memcached.settings.list", Handle: listSettings},

		{ID: "memcached.events.list", Method: plugin.MethodGet, Path: "/events", Permission: permRead, Risk: plugin.RiskSafe, AuditEvent: "memcached.events.list", Handle: listEvents},
		{ID: "memcached.events.watch", Method: plugin.MethodWS, Path: "/events/watch", Permission: permRead, Risk: plugin.RiskSafe, AuditEvent: "memcached.events.watch", Stream: eventsWatchStream},

		{ID: "memcached.console", Method: plugin.MethodWS, Path: "/console", Permission: permConsole, Risk: plugin.RiskPrivileged, AuditEvent: "memcached.console", Stream: consoleStream},
		{ID: "memcached.console.complete", Method: plugin.MethodGet, Path: "/console/complete", Permission: permConsole, Risk: plugin.RiskSafe, AuditEvent: "memcached.console.complete", Handle: completeConsole},

		{ID: "memcached.commands.list", Method: plugin.MethodGet, Path: "/commands", Permission: permQueryRd, Risk: plugin.RiskSafe, AuditEvent: "memcached.commands.list", Handle: listSavedCommands},
		{ID: "memcached.commands.save", Method: plugin.MethodPost, Path: "/commands", Permission: permQueryWr, Risk: plugin.RiskWrite, AuditEvent: "memcached.commands.save", Input: savedCommandSchema(), Handle: saveCommand},
		{ID: "memcached.commands.delete", Method: plugin.MethodDelete, Path: "/commands/{id}", Permission: permQueryWr, Risk: plugin.RiskDestructive, AuditEvent: "memcached.commands.delete", Handle: deleteSavedCommand},

		{ID: "memcached.server.flush", Method: plugin.MethodPost, Path: "/flush", Permission: permAdmin, Risk: plugin.RiskDestructive, AuditEvent: "memcached.server.flush", Input: flushSchema(), Handle: flushAll},
		{ID: "memcached.server.stats_reset", Method: plugin.MethodPost, Path: "/stats/reset", Permission: permAdmin, Risk: plugin.RiskDestructive, AuditEvent: "memcached.server.stats_reset", Handle: resetStats},
		{ID: "memcached.server.memlimit", Method: plugin.MethodPost, Path: "/memlimit", Permission: permAdmin, Risk: plugin.RiskPrivileged, AuditEvent: "memcached.server.memlimit", Input: memlimitSchema(), Handle: setMemlimit},
		{ID: "memcached.server.verbosity", Method: plugin.MethodPost, Path: "/verbosity", Permission: permAdmin, Risk: plugin.RiskPrivileged, AuditEvent: "memcached.server.verbosity", Input: verbositySchema(), Handle: setVerbosity},
		{ID: "memcached.server.crawl", Method: plugin.MethodPost, Path: "/crawl", Permission: permAdmin, Risk: plugin.RiskWrite, AuditEvent: "memcached.server.crawl", Input: crawlSchema(), Handle: startCrawl},
	}
}

func session(rc *plugin.RequestContext) (*Session, error) {
	s, err := unwrap(rc.Session)
	if err != nil {
		return nil, err
	}
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}
	return s, nil
}

func writableSession(rc *plugin.RequestContext) (*Session, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	if err := s.ensureWritable(); err != nil {
		return nil, err
	}
	return s, nil
}

func commandContext(rc *plugin.RequestContext, s *Session) (context.Context, context.CancelFunc) {
	return context.WithTimeout(rc.Ctx, s.opts.Timeout)
}

// scopeClass reads the slab-class scope filter. "all" (the default) means every
// class; anything else must be a slab class id.
func scopeClass(rc *plugin.RequestContext) (string, int, error) {
	raw := strings.TrimSpace(rc.Param(classScopeParam))
	if raw == "" || raw == classScopeAll {
		return classScopeAll, -1, nil
	}
	id, err := strconv.Atoi(raw)
	if err != nil || id < 0 {
		return "", 0, fmt.Errorf("%w: slab class must be a non-negative integer", plugin.ErrInvalidInput)
	}
	return strconv.Itoa(id), id, nil
}

func overviewRoute(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := commandContext(rc, s)
	defer cancel()
	return s.readOverview(ctx)
}

func overviewWatchStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := session(rc)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(client)
	watcher := newOverviewWatcher(s.opts.address())
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		ctx, cancel := context.WithTimeout(client.Context(), s.opts.Timeout)
		overview, pollErr := s.readOverview(ctx)
		cancel()
		if err := enc.Encode(watcher.event(overview, pollErr)); err != nil {
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

func metricsStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := session(rc)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(client)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	var watcher metricsWatcher
	for {
		ctx, cancel := context.WithTimeout(client.Context(), s.opts.Timeout)
		overview, pollErr := s.readOverview(ctx)
		cancel()
		if err := enc.Encode(watcher.frame(overview, pollErr)); err != nil {
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

func listKeys(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	req, err := rc.Page()
	if err != nil {
		return nil, err
	}
	class, _, err := scopeClass(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.scanTimeout())
	defer cancel()
	dump, err := s.scanKeys(ctx, class)
	if err != nil {
		return nil, err
	}

	now := time.Now().Unix()
	rows := make([]plugin.TableRow, 0, len(dump.Items))
	term := strings.ToLower(req.Search())
	for _, item := range dump.Items {
		if term != "" && !strings.Contains(strings.ToLower(item.Key), term) {
			continue
		}
		rows = append(rows, rowOf(metaItemToRow(item, now)))
	}
	plugin.SortRows(rows, req.Sort)
	if len(req.Sort) == 0 {
		sort.SliceStable(rows, func(i, j int) bool { return fmt.Sprint(rows[i]["key"]) < fmt.Sprint(rows[j]["key"]) })
	}

	limit := req.Limit
	if limit <= 0 || limit > s.opts.PageLimit {
		limit = s.opts.PageLimit
	}
	offset := 0
	if req.Cursor != "" {
		parsed, err := strconv.Atoi(req.Cursor)
		if err != nil || parsed < 0 {
			return nil, fmt.Errorf("%w: invalid cursor", plugin.ErrInvalidInput)
		}
		offset = parsed
	}
	total := len(rows)
	if offset > total {
		offset = total
	}
	end := min(offset+limit, total)
	next := ""
	if end < total {
		next = strconv.Itoa(end)
	}
	return plugin.Page[plugin.TableRow]{Items: rows[offset:end], NextCursor: next, Total: &total}, nil
}

func readKeyRoute(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := commandContext(rc, s)
	defer cancel()
	return s.readKey(ctx, rc.Param("key"))
}

func writeKeyRoute(rc *plugin.RequestContext) (any, error) {
	s, err := writableSession(rc)
	if err != nil {
		return nil, err
	}
	var in struct {
		Key   string  `json:"key"`
		Type  string  `json:"type"`
		Value string  `json:"value"`
		TTL   any     `json:"ttl"`
		Flags *uint32 `json:"flags"`
	}
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	key := stringDefault(rc.Param("key"), in.Key)
	value, err := decodeValue(in.Type, in.Value)
	if err != nil {
		return nil, err
	}
	ttl, hasTTL, err := parseTTL(in.TTL)
	if err != nil {
		return nil, err
	}
	req := storeRequest{Key: key, Mode: "set", Value: value, TTL: ttl, HasTTL: hasTTL}
	if in.Flags != nil {
		req.Flags = *in.Flags
		req.HasFlags = true
	}
	ctx, cancel := commandContext(rc, s)
	defer cancel()
	if err := s.store(ctx, req); err != nil {
		return nil, err
	}
	return s.readKey(ctx, key)
}

func storeKeyRoute(rc *plugin.RequestContext) (any, error) {
	s, err := writableSession(rc)
	if err != nil {
		return nil, err
	}
	var in struct {
		Key   string `json:"key" validate:"required"`
		Mode  string `json:"mode"`
		Type  string `json:"type"`
		Value string `json:"value"`
		TTL   any    `json:"ttl"`
		Flags uint32 `json:"flags"`
		CAS   uint64 `json:"cas"`
	}
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	value, err := decodeValue(in.Type, in.Value)
	if err != nil {
		return nil, err
	}
	ttl, _, err := parseTTL(in.TTL)
	if err != nil {
		return nil, err
	}
	ctx, cancel := commandContext(rc, s)
	defer cancel()
	req := storeRequest{
		Key: in.Key, Mode: stringDefault(in.Mode, "set"), Value: value,
		Flags: in.Flags, HasFlags: true, TTL: ttl, HasTTL: true, CAS: in.CAS,
	}
	if err := s.store(ctx, req); err != nil {
		return nil, err
	}
	return actionResult{OK: true, Message: req.Mode + " " + req.Key}, nil
}

func deleteKeyRoute(rc *plugin.RequestContext) (any, error) {
	s, err := writableSession(rc)
	if err != nil {
		return nil, err
	}
	key := rc.Param("key")
	if err := validateKey(key); err != nil {
		return nil, err
	}
	if err := mapClientError(s.client.Delete(key)); err != nil {
		return nil, err
	}
	return actionResult{OK: true, Message: "deleted " + key}, nil
}

func touchKeyRoute(rc *plugin.RequestContext) (any, error) {
	s, err := writableSession(rc)
	if err != nil {
		return nil, err
	}
	var in struct {
		Key string `json:"key" validate:"required"`
		TTL any    `json:"ttl"`
	}
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	if err := validateKey(in.Key); err != nil {
		return nil, err
	}
	ttl, _, err := parseTTL(in.TTL)
	if err != nil {
		return nil, err
	}
	if err := mapClientError(s.client.Touch(in.Key, ttl)); err != nil {
		return nil, err
	}
	return actionResult{OK: true, Message: fmt.Sprintf("touched %s to %d seconds", in.Key, ttl)}, nil
}

func incrKeyRoute(rc *plugin.RequestContext) (any, error) { return counterRoute(rc, true) }

func decrKeyRoute(rc *plugin.RequestContext) (any, error) { return counterRoute(rc, false) }

func counterRoute(rc *plugin.RequestContext, up bool) (any, error) {
	s, err := writableSession(rc)
	if err != nil {
		return nil, err
	}
	var in struct {
		Key   string `json:"key" validate:"required"`
		Delta uint64 `json:"delta"`
	}
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	if err := validateKey(in.Key); err != nil {
		return nil, err
	}
	if in.Delta == 0 {
		in.Delta = 1
	}
	var value uint64
	if up {
		value, err = s.client.Increment(in.Key, in.Delta)
	} else {
		value, err = s.client.Decrement(in.Key, in.Delta)
	}
	if err != nil {
		return nil, mapClientError(err)
	}
	return actionResult{OK: true, Value: strconv.FormatUint(value, 10)}, nil
}

func listSlabs(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	_, class, err := scopeClass(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := commandContext(rc, s)
	defer cancel()
	rows, err := s.readSlabs(ctx)
	if err != nil {
		return nil, err
	}
	return pageRows(rc, mapRows(rows, func(row slabRow) plugin.TableRow { return rowOf(row) }, func(row slabRow) bool {
		return class < 0 || row.Class == class
	}))
}

func listSlabOptions(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := commandContext(rc, s)
	defer cancel()
	rows, err := s.readSlabs(ctx)
	if err != nil {
		return nil, err
	}
	items := make([]plugin.FilterOption, 0, len(rows)+1)
	items = append(items, plugin.FilterOption{Value: classScopeAll, Label: "All classes"})
	for _, row := range rows {
		items = append(items, plugin.FilterOption{
			Value: strconv.Itoa(row.Class),
			Label: fmt.Sprintf("Class %d · %s chunks", row.Class, humanBytes(row.ChunkSize)),
		})
	}
	total := len(items)
	return plugin.Page[plugin.FilterOption]{Items: items, Total: &total}, nil
}

func listItems(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	_, class, err := scopeClass(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := commandContext(rc, s)
	defer cancel()
	rows, err := s.readLRU(ctx)
	if err != nil {
		return nil, err
	}
	return pageRows(rc, mapRows(rows, func(row lruRow) plugin.TableRow { return rowOf(row) }, func(row lruRow) bool {
		return class < 0 || row.Class == class
	}))
}

func listConns(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := commandContext(rc, s)
	defer cancel()
	rows, err := s.readConns(ctx)
	if err != nil {
		return nil, err
	}
	return pageRows(rc, mapRows(rows, func(row connRow) plugin.TableRow { return rowOf(row) }, nil))
}

func listSettings(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := commandContext(rc, s)
	defer cancel()
	rows, err := s.readSettings(ctx)
	if err != nil {
		return nil, err
	}
	return pageRows(rc, mapRows(rows, func(row settingRow) plugin.TableRow { return rowOf(row) }, nil))
}

func listEvents(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	req, err := rc.Page()
	if err != nil {
		return nil, err
	}
	ctx, cancel := commandContext(rc, s)
	defer cancel()
	if err := s.events.ensure(ctx); err != nil {
		return nil, err
	}
	events := s.events.snapshot()
	sortEventsByTime(events)
	if term := strings.ToLower(req.Search()); term != "" {
		filtered := events[:0]
		for _, event := range events {
			if strings.Contains(strings.ToLower(event.Reason+" "+event.Message), term) {
				filtered = append(filtered, event)
			}
		}
		events = filtered
	}
	limit := req.Limit
	if limit <= 0 || limit > s.opts.PageLimit {
		limit = s.opts.PageLimit
	}
	total := len(events)
	if len(events) > limit {
		events = events[:limit]
	}
	return plugin.Page[activityEvent]{Items: events, Total: &total}, nil
}

func eventsWatchStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := session(rc)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(client.Context(), s.opts.Timeout)
	err = s.events.ensure(ctx)
	cancel()
	if err != nil {
		return err
	}
	feed, unsubscribe := s.events.subscribe()
	defer unsubscribe()

	enc := json.NewEncoder(client)
	for {
		select {
		case <-client.Context().Done():
			return nil
		case <-rc.Ctx.Done():
			return nil
		case event := <-feed:
			if err := enc.Encode(plugin.ResourceEvent{Type: "added", Ref: event.Ref, Resource: event}); err != nil {
				return nil
			}
		}
	}
}

func completeConsole(*plugin.RequestContext) (any, error) {
	entries := completionEntries()
	sort.Slice(entries, func(i, j int) bool { return entries[i].Label < entries[j].Label })
	total := len(entries)
	return plugin.Page[completionEntry]{Items: entries, Total: &total}, nil
}

type savedCommand struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Command   string `json:"command"`
	UpdatedAt string `json:"updatedAt,omitempty"`
}

func listSavedCommands(rc *plugin.RequestContext) (any, error) {
	if rc.Storage == nil {
		return plugin.Page[savedCommand]{Items: []savedCommand{}}, nil
	}
	items, err := rc.Storage.List(rc.Ctx, plugin.ConnectionStorage(savedCommandsBucket), nil)
	if err != nil {
		return nil, fmt.Errorf("%w: list saved commands: %v", plugin.ErrUnavailable, err)
	}
	out := make([]savedCommand, 0, len(items))
	for _, item := range items {
		var record savedCommand
		if err := json.Unmarshal(item.Value, &record); err != nil {
			continue
		}
		record.ID = item.Key
		if !item.UpdatedAt.IsZero() {
			record.UpdatedAt = item.UpdatedAt.UTC().Format(time.RFC3339)
		}
		out = append(out, record)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	total := len(out)
	return plugin.Page[savedCommand]{Items: out, Total: &total}, nil
}

func saveCommand(rc *plugin.RequestContext) (any, error) {
	if rc.Storage == nil {
		return nil, fmt.Errorf("%w: plugin storage is unavailable", plugin.ErrUnavailable)
	}
	var in struct {
		Name    string `json:"name" validate:"required"`
		Command string `json:"command" validate:"required"`
	}
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	in.Name = strings.TrimSpace(in.Name)
	if in.Name == "" {
		return nil, fmt.Errorf("%w: name is required", plugin.ErrInvalidInput)
	}
	if len(in.Command) > maxSavedCommandBytes {
		return nil, fmt.Errorf("%w: saved commands are limited to %d bytes", plugin.ErrInvalidInput, maxSavedCommandBytes)
	}
	if _, _, err := resolveCommand(in.Command); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(savedCommand{Name: in.Name, Command: in.Command})
	if err != nil {
		return nil, fmt.Errorf("%w: encode saved command: %v", plugin.ErrInvalidInput, err)
	}
	item, err := rc.Storage.Put(rc.Ctx, savedCommandsBucket, plugin.StorageItem{
		Key:         storageKey(in.Name),
		Value:       payload,
		ContentType: "application/json",
		Metadata:    map[string]string{"name": in.Name},
	})
	if err != nil {
		return nil, fmt.Errorf("%w: save command: %v", plugin.ErrUnavailable, err)
	}
	return savedCommand{ID: item.Key, Name: in.Name, Command: in.Command}, nil
}

func deleteSavedCommand(rc *plugin.RequestContext) (any, error) {
	if rc.Storage == nil {
		return nil, fmt.Errorf("%w: plugin storage is unavailable", plugin.ErrUnavailable)
	}
	id := strings.TrimSpace(rc.Param("id"))
	if id == "" {
		return nil, fmt.Errorf("%w: saved command id is required", plugin.ErrInvalidInput)
	}
	if err := rc.Storage.Delete(rc.Ctx, plugin.ConnectionStorage(savedCommandsBucket), id); err != nil {
		return nil, fmt.Errorf("%w: delete saved command: %v", plugin.ErrUnavailable, err)
	}
	return actionResult{OK: true, Message: "deleted " + id}, nil
}

// storageKey derives a stable, filesystem-safe record key from a display name so
// re-saving the same name updates the record instead of duplicating it.
func storageKey(name string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(name) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	key := strings.Trim(b.String(), "-")
	if key == "" {
		key = "command"
	}
	if len(key) > 96 {
		key = key[:96]
	}
	return key
}

func flushAll(rc *plugin.RequestContext) (any, error) {
	s, err := writableSession(rc)
	if err != nil {
		return nil, err
	}
	var in struct {
		Delay int64 `json:"delay"`
	}
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	if in.Delay < 0 {
		return nil, fmt.Errorf("%w: delay cannot be negative", plugin.ErrInvalidInput)
	}
	return s.adminCommand(rc, fmt.Sprintf("flush_all %d", in.Delay), "OK")
}

func resetStats(rc *plugin.RequestContext) (any, error) {
	s, err := writableSession(rc)
	if err != nil {
		return nil, err
	}
	return s.adminCommand(rc, "stats reset", "RESET")
}

func setMemlimit(rc *plugin.RequestContext) (any, error) {
	s, err := writableSession(rc)
	if err != nil {
		return nil, err
	}
	var in struct {
		Megabytes int64 `json:"megabytes"`
	}
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	if in.Megabytes < 1 {
		return nil, fmt.Errorf("%w: the memory limit must be at least 1 megabyte", plugin.ErrInvalidInput)
	}
	return s.adminCommand(rc, fmt.Sprintf("cache_memlimit %d", in.Megabytes), "OK")
}

func setVerbosity(rc *plugin.RequestContext) (any, error) {
	s, err := writableSession(rc)
	if err != nil {
		return nil, err
	}
	var in struct {
		Level int64 `json:"level"`
	}
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	if in.Level < 0 || in.Level > 3 {
		return nil, fmt.Errorf("%w: verbosity must be between 0 and 3", plugin.ErrInvalidInput)
	}
	return s.adminCommand(rc, fmt.Sprintf("verbosity %d", in.Level), "OK")
}

func startCrawl(rc *plugin.RequestContext) (any, error) {
	s, err := writableSession(rc)
	if err != nil {
		return nil, err
	}
	var in struct {
		Classes string `json:"classes"`
	}
	if err := rc.Bind(&in); err != nil {
		return nil, err
	}
	classes := stringDefault(in.Classes, classScopeAll)
	if classes != classScopeAll && !validClassList(classes) {
		return nil, fmt.Errorf("%w: classes must be \"all\" or a comma separated list of slab class ids", plugin.ErrInvalidInput)
	}
	return s.adminCommand(rc, "lru_crawler crawl "+classes, "OK")
}

func validClassList(raw string) bool {
	for _, part := range strings.Split(raw, ",") {
		if _, err := strconv.Atoi(strings.TrimSpace(part)); err != nil {
			return false
		}
	}
	return true
}

// adminCommand runs a single-line administrative command and reports the exact
// server reply so an unexpected outcome is never silently reported as success.
func (s *Session) adminCommand(rc *plugin.RequestContext, command, expect string) (any, error) {
	ctx, cancel := commandContext(rc, s)
	defer cancel()
	var line string
	err := s.withAdmin(ctx, func(c *textConn) error {
		if err := c.send(command + "\r\n"); err != nil {
			return err
		}
		reply, err := c.readLine()
		if err != nil {
			return err
		}
		if err := serverError(reply); err != nil {
			return err
		}
		line = reply
		return nil
	})
	if err != nil {
		return nil, err
	}
	if expect != "" && line != expect {
		return nil, fmt.Errorf("%w: memcached replied %q", plugin.ErrUnavailable, line)
	}
	return actionResult{OK: true, Message: line}, nil
}

// mapRows converts typed rows to renderer rows, optionally filtering first.
func mapRows[T any](rows []T, to func(T) plugin.TableRow, keep func(T) bool) []plugin.TableRow {
	out := make([]plugin.TableRow, 0, len(rows))
	for _, row := range rows {
		if keep != nil && !keep(row) {
			continue
		}
		out = append(out, to(row))
	}
	return out
}

// pageRows applies the grid's search, sort, and cursor to an in-memory slice.
func pageRows(rc *plugin.RequestContext, rows []plugin.TableRow) (any, error) {
	req, err := rc.Page()
	if err != nil {
		return nil, err
	}
	rows = plugin.FilterRows(rows, req.Search())
	rows = plugin.SortRows(rows, req.Sort)
	limit := req.Limit
	if limit <= 0 || limit > plugin.MaxPageLimit {
		limit = plugin.MaxPageLimit
	}
	offset := 0
	if req.Cursor != "" {
		parsed, err := strconv.Atoi(req.Cursor)
		if err != nil || parsed < 0 {
			return nil, fmt.Errorf("%w: invalid cursor", plugin.ErrInvalidInput)
		}
		offset = parsed
	}
	total := len(rows)
	if offset > total {
		offset = total
	}
	end := min(offset+limit, total)
	next := ""
	if end < total {
		next = strconv.Itoa(end)
	}
	return plugin.Page[plugin.TableRow]{Items: rows[offset:end], NextCursor: next, Total: &total}, nil
}

// rowOf renders a typed row through its JSON tags so table column keys and the
// Go struct never drift apart.
func rowOf(value any) plugin.TableRow {
	raw, err := json.Marshal(value)
	if err != nil {
		return plugin.TableRow{}
	}
	out := plugin.TableRow{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return plugin.TableRow{}
	}
	return out
}

func humanBytes(v int64) string {
	const unit = 1024
	if v < unit {
		return strconv.FormatInt(v, 10) + " B"
	}
	div, exp := int64(unit), 0
	for n := v / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(v)/float64(div), "KMGT"[exp])
}
