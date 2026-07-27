package consul

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/hashicorp/consul/api"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	// kvRootToken is the resource UID of the connection's KV root, so the tree can
	// address it without an empty identifier.
	kvRootToken = "/"

	kvDescribeConcurrency = 8
	watchWaitTime         = 5 * time.Minute
	watchRetryDelay       = 5 * time.Second
	watchIdleDelay        = time.Second
	// watchKeyCap bounds the key set the watch remembers between blocking queries;
	// larger prefixes fall back to reporting index movement only.
	watchKeyCap = 5000
)

func kvPutSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Entry", Fields: []plugin.Field{
		{Key: "key", Label: "Key", Type: plugin.FieldText, Required: true, Default: "${resource.uid}",
			Help: "Full key path, for example apps/web/config."},
		{Key: "value", Label: "Value", Type: plugin.FieldTextarea},
		{Key: "flags", Label: "Flags", Type: plugin.FieldNumber, Validators: []plugin.Validator{
			{Type: plugin.ValidatorMin, Value: 0},
		}, Help: "Opaque 64-bit marker Consul stores alongside the value."},
		{Key: "cas", Label: "Check-and-set index", Type: plugin.FieldNumber, Validators: []plugin.Validator{
			{Type: plugin.ValidatorMin, Value: 0},
		}, Help: "Write only when the key is still at this modify index. Leave empty for an unconditional write."},
	}}}}
}

// kvPrefix resolves the prefix a request addresses and keeps it inside the
// connection's configured KV root.
func (s *Session) kvPrefix(rc *plugin.RequestContext) (string, error) {
	raw := paramOrQuery(rc, "prefix")
	if raw == "" || raw == kvRootToken {
		return s.opts.KVPrefix, nil
	}
	prefix := strings.TrimPrefix(strings.TrimSpace(raw), "/")
	if err := s.inRoot(prefix); err != nil {
		return "", err
	}
	return prefix, nil
}

func (s *Session) scopedKey(raw string) (string, error) {
	key := strings.TrimPrefix(strings.TrimSpace(raw), "/")
	if key == "" {
		return "", fmt.Errorf("%w: key is required", plugin.ErrInvalidInput)
	}
	if err := s.inRoot(key); err != nil {
		return "", err
	}
	return key, nil
}

func (s *Session) inRoot(path string) error {
	if s.opts.KVPrefix == "" || strings.HasPrefix(path, s.opts.KVPrefix) {
		return nil
	}
	return fmt.Errorf("%w: %q is outside the connection KV root %q", plugin.ErrForbidden, path, s.opts.KVPrefix)
}

func kvTree(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	prefix, err := s.kvPrefix(rc)
	if err != nil {
		return nil, err
	}
	keys, _, err := s.client.KV().Keys(prefix, "/", s.query(rc))
	if err != nil {
		return nil, consulErr(err)
	}
	nodes := make([]plugin.TreeNode, 0, len(keys)+1)
	if paramOrQuery(rc, "prefix") == "" {
		label := rootLabel(prefix)
		nodes = append(nodes, plugin.TreeNode{
			Key: "kv:root", Label: label, Icon: icon("folder-root"), Leaf: true,
			Ref:  &plugin.ResourceIdentity{Kind: kindKV, Name: label, UID: kvRootToken},
			Data: map[string]any{"prefix": prefix},
		})
	}
	folders := 0
	for _, key := range keys {
		if !strings.HasSuffix(key, "/") {
			continue
		}
		if folders >= s.opts.PageLimit {
			break
		}
		folders++
		label := strings.TrimSuffix(strings.TrimPrefix(key, prefix), "/")
		nodes = append(nodes, plugin.TreeNode{
			Key: "kv:" + key, Label: label, Icon: icon("folder"),
			Ref:            &plugin.ResourceIdentity{Kind: kindKV, Name: label, UID: key},
			ChildrenSource: &plugin.DataSource{RouteID: rid("kv.tree"), Params: map[string]string{"prefix": key}},
			Data:           map[string]any{"prefix": key},
		})
	}
	return treePage(nodes), nil
}

// kvList backs the key browser, which groups the returned keys into its own
// namespace tree and therefore needs the whole subtree under the selected
// prefix. Narrow the connection's KV root or pick a folder to bound it.
func kvList(rc *plugin.RequestContext) (any, error) {
	s, rows, err := kvKeyRows(rc, "")
	if err != nil {
		return nil, err
	}
	return s.page(rc, rows)
}

// kvEntries backs the entry grid. It lists one folder level, so the fetch is
// bounded by that folder's fan-out no matter how deep the key space is.
func kvEntries(rc *plugin.RequestContext) (any, error) {
	s, rows, err := kvKeyRows(rc, "/")
	if err != nil {
		return nil, err
	}
	page, err := s.page(rc, rows)
	if err != nil {
		return nil, err
	}
	s.describeKeys(rc, page.Items)
	return page, nil
}

func kvKeyRows(rc *plugin.RequestContext, separator string) (*Session, []row, error) {
	s, err := session(rc)
	if err != nil {
		return nil, nil, err
	}
	prefix, err := s.kvPrefix(rc)
	if err != nil {
		return nil, nil, err
	}
	keys, _, err := s.client.KV().Keys(prefix, separator, s.query(rc))
	if err != nil {
		return nil, nil, consulErr(err)
	}
	rows := make([]row, 0, len(keys))
	for _, key := range keys {
		if key == "" || strings.HasSuffix(key, "/") {
			continue
		}
		rows = append(rows, row{"key": key})
	}
	sortByKey(rows, "key")
	return s, rows, nil
}

// describeKeys fills per-key metadata for every row on the page the grid is
// about to render. Each goroutine owns one row map, so nothing is shared across
// writers.
func (s *Session) describeKeys(rc *plugin.RequestContext, items []row) {
	sem := make(chan struct{}, kvDescribeConcurrency)
	var wg sync.WaitGroup
	q := s.query(rc)
	for _, item := range items {
		key, _ := item["key"].(string)
		if key == "" {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(item row, key string) {
			defer wg.Done()
			defer func() { <-sem }()
			pair, _, err := s.client.KV().Get(key, q)
			if err != nil || pair == nil {
				return
			}
			item["size"] = len(pair.Value)
			item["flags"] = pair.Flags
			item["session"] = pair.Session
			item["create_index"] = pair.CreateIndex
			item["modify_index"] = pair.ModifyIndex
			item["lock_index"] = pair.LockIndex
		}(item, key)
	}
	wg.Wait()
}

func kvRead(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	key, err := s.scopedKey(paramOrQuery(rc, "key"))
	if err != nil {
		return nil, err
	}
	pair, _, err := s.client.KV().Get(key, s.query(rc))
	if err != nil {
		return nil, consulErr(err)
	}
	if pair == nil {
		return nil, fmt.Errorf("%w: key %q does not exist", plugin.ErrNotFound, key)
	}
	value, valueType := decodeValue(pair.Value)
	return row{
		"key":          pair.Key,
		"type":         valueType,
		"value":        value,
		"size":         len(pair.Value),
		"flags":        pair.Flags,
		"session":      pair.Session,
		"create_index": pair.CreateIndex,
		"modify_index": pair.ModifyIndex,
		"lock_index":   pair.LockIndex,
	}, nil
}

func kvWrite(rc *plugin.RequestContext) (any, error) {
	s, err := writableSession(rc)
	if err != nil {
		return nil, err
	}
	var body struct {
		Key   string  `json:"key"`
		Type  string  `json:"type"`
		Value any     `json:"value"`
		Flags *uint64 `json:"flags"`
		CAS   *uint64 `json:"cas"`
	}
	if err := rc.Bind(&body); err != nil {
		return nil, err
	}
	raw := paramOrQuery(rc, "key")
	if raw == "" {
		raw = body.Key
	}
	key, err := s.scopedKey(raw)
	if err != nil {
		return nil, err
	}
	value, err := encodeValue(body.Value, body.Type)
	if err != nil {
		return nil, err
	}
	return s.putKey(rc, key, value, body.Flags, body.CAS)
}

func kvPut(rc *plugin.RequestContext) (any, error) {
	s, err := writableSession(rc)
	if err != nil {
		return nil, err
	}
	var body struct {
		Key   string  `json:"key" validate:"required"`
		Value string  `json:"value"`
		Flags *uint64 `json:"flags"`
		CAS   *uint64 `json:"cas"`
	}
	if err := rc.Bind(&body); err != nil {
		return nil, err
	}
	key, err := s.scopedKey(body.Key)
	if err != nil {
		return nil, err
	}
	return s.putKey(rc, key, []byte(body.Value), body.Flags, body.CAS)
}

func (s *Session) putKey(rc *plugin.RequestContext, key string, value []byte, flags, cas *uint64) (any, error) {
	pair := &api.KVPair{Key: key, Value: value}
	if flags != nil {
		pair.Flags = *flags
	}
	options := s.writeOptions(rc)
	if cas != nil {
		pair.ModifyIndex = *cas
		ok, _, err := s.client.KV().CAS(pair, options)
		if err != nil {
			return nil, consulErr(err)
		}
		if !ok {
			return nil, fmt.Errorf("%w: %q changed since modify index %d", plugin.ErrConflict, key, *cas)
		}
		return row{"ok": true, "key": key, "cas": *cas}, nil
	}
	if _, err := s.client.KV().Put(pair, options); err != nil {
		return nil, consulErr(err)
	}
	return row{"ok": true, "key": key}, nil
}

func kvDelete(rc *plugin.RequestContext) (any, error) {
	s, err := writableSession(rc)
	if err != nil {
		return nil, err
	}
	key, err := s.scopedKey(paramOrQuery(rc, "key"))
	if err != nil {
		return nil, err
	}
	if _, err := s.client.KV().Delete(key, s.writeOptions(rc)); err != nil {
		return nil, consulErr(err)
	}
	return row{"ok": true, "key": key}, nil
}

func kvDeleteTree(rc *plugin.RequestContext) (any, error) {
	s, err := writableSession(rc)
	if err != nil {
		return nil, err
	}
	prefix, err := s.kvPrefix(rc)
	if err != nil {
		return nil, err
	}
	if prefix == "" {
		return nil, fmt.Errorf("%w: refusing to delete the entire key space; select a prefix first", plugin.ErrInvalidInput)
	}
	if _, err := s.client.KV().DeleteTree(prefix, s.writeOptions(rc)); err != nil {
		return nil, consulErr(err)
	}
	return row{"ok": true, "prefix": prefix}, nil
}

func kvWatch(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := session(rc)
	if err != nil {
		return err
	}
	prefix, err := s.kvPrefix(rc)
	if err != nil {
		return err
	}
	ctx := client.Context()
	if err := writeLine(client, "Watching consul KV prefix "+rootLabel(prefix)); err != nil {
		return err
	}
	var (
		index  uint64
		known  map[string]bool
		synced bool
	)
	for {
		if ctx.Err() != nil {
			return nil
		}
		options := (&api.QueryOptions{
			Datacenter: s.datacenter(rc),
			Namespace:  s.opts.Namespace,
			Partition:  s.opts.Partition,
			WaitIndex:  index,
			WaitTime:   watchWaitTime,
		}).WithContext(ctx)
		keys, meta, err := s.watcher.KV().Keys(prefix, "", options)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if err := writeLine(client, "watch error: "+err.Error()); err != nil {
				return streamErr(ctx, err)
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(watchRetryDelay):
			}
			continue
		}
		// Consul reserves index 0 and asks clients to restart from there when the
		// index regresses.
		next := max(meta.LastIndex, 1)
		if next < index {
			index = 0
			continue
		}
		// A blocking query that expired without a change repeats the index; the
		// panel stays quiet instead of reporting a change that did not happen.
		// The pause keeps the loop cold against a proxy that answers immediately
		// instead of honoring the blocking query.
		if next == index {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(watchIdleDelay):
			}
			continue
		}
		current := keySet(keys)
		for _, line := range describeChange(known, current, keys, next, !synced) {
			if err := writeLine(client, line); err != nil {
				return streamErr(ctx, err)
			}
		}
		known, synced, index = current, true, next
	}
}

func describeChange(known, current map[string]bool, keys []string, index uint64, first bool) []string {
	if first {
		return []string{fmt.Sprintf("[sync] %d key(s) at index %d", len(keys), index)}
	}
	if known == nil || current == nil {
		return []string{fmt.Sprintf("[index] moved to %d", index)}
	}
	lines := make([]string, 0, 8)
	for key := range current {
		if !known[key] {
			lines = append(lines, "[+] "+key)
		}
	}
	for key := range known {
		if !current[key] {
			lines = append(lines, "[-] "+key)
		}
	}
	if len(lines) == 0 {
		return []string{fmt.Sprintf("[update] %d key(s) changed value at index %d", len(keys), index)}
	}
	return lines
}

// keySet remembers the listed keys, or nil past the cap so a huge prefix reports
// index movement instead of retaining every key.
func keySet(keys []string) map[string]bool {
	if len(keys) > watchKeyCap {
		return nil
	}
	set := make(map[string]bool, len(keys))
	for _, key := range keys {
		set[key] = true
	}
	return set
}

func writeLine(w io.Writer, line string) error {
	frame, err := json.Marshal(struct {
		TS   string `json:"ts,omitempty"`
		Line string `json:"line"`
	}{TS: time.Now().UTC().Format(time.RFC3339), Line: line})
	if err != nil {
		return err
	}
	_, err = w.Write(frame)
	return err
}

func streamErr(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return nil
	}
	return err
}

func decodeValue(raw []byte) (string, string) {
	if !utf8.Valid(raw) {
		return base64.StdEncoding.EncodeToString(raw), "binary"
	}
	value := string(raw)
	trimmed := strings.TrimSpace(value)
	if trimmed != "" && (trimmed[0] == '{' || trimmed[0] == '[') && json.Valid(raw) {
		return value, "json"
	}
	return value, "string"
}

func encodeValue(value any, valueType string) ([]byte, error) {
	switch typed := value.(type) {
	case nil:
		return nil, nil
	case string:
		if valueType == "binary" {
			raw, err := base64.StdEncoding.DecodeString(typed)
			if err != nil {
				return nil, fmt.Errorf("%w: binary values must be base64", plugin.ErrInvalidInput)
			}
			return raw, nil
		}
		return []byte(typed), nil
	default:
		raw, err := json.Marshal(typed)
		if err != nil {
			return nil, fmt.Errorf("%w: value is not encodable: %v", plugin.ErrInvalidInput, err)
		}
		return raw, nil
	}
}

func rootLabel(prefix string) string {
	if prefix == "" {
		return "All keys"
	}
	return prefix
}
