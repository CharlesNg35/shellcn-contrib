package memcached

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/bradfitz/gomemcache/memcache"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	valueText   = "text"
	valueJSON   = "json"
	valueBinary = "binary"

	// relativeTTLMax is memcached's 30 day boundary: a larger expiration value is
	// read as an absolute Unix timestamp instead of a relative TTL.
	relativeTTLMax = 60 * 60 * 24 * 30
)

// keyRow is one entry in the key browser list.
type keyRow struct {
	Key        string `json:"key"`
	Type       string `json:"type"`
	Class      int    `json:"class"`
	Size       int64  `json:"size"`
	TTL        int64  `json:"ttl"`
	Flags      uint32 `json:"flags"`
	Expires    string `json:"expires,omitempty"`
	LastAccess string `json:"lastAccess,omitempty"`
	Fetched    bool   `json:"fetched"`
	CAS        uint64 `json:"cas,omitempty"`
}

// keyDetail is the editable payload the KV panel reads.
type keyDetail struct {
	Key        string `json:"key"`
	Type       string `json:"type"`
	Value      string `json:"value"`
	Flags      uint32 `json:"flags"`
	CAS        uint64 `json:"cas,omitempty"`
	Size       int64  `json:"size"`
	TTL        int64  `json:"ttl"`
	Class      int    `json:"class,omitempty"`
	LastAccess string `json:"lastAccess,omitempty"`
	Fetched    bool   `json:"fetched"`
	Truncated  bool   `json:"truncated,omitempty"`
}

// scanKeys walks the LRU with the crawler. memcached has no key-space index, so
// this is the only way to enumerate keys, and it is a point-in-time crawl: keys
// stored during the walk may be missed, and the walk stops at the configured cap.
func (s *Session) scanKeys(ctx context.Context, class string) (metadumpResult, error) {
	var res metadumpResult
	err := s.withAdminConn(ctx, func(c *textConn) (bool, error) {
		out, err := c.metadump(ctx, class, s.opts.ScanLimit, s.opts.scanTimeout())
		if err != nil {
			return false, err
		}
		res = out
		return out.Retire, nil
	})
	return res, err
}

func metaItemToRow(item metaItem, now int64) keyRow {
	row := keyRow{
		Key:     item.Key,
		Type:    valueText,
		Class:   item.Class,
		Size:    item.Size,
		CAS:     item.CAS,
		Flags:   item.Flags,
		Fetched: item.Fetched,
		TTL:     -1,
	}
	if item.Expires > 0 {
		row.Expires = timeUnix(item.Expires)
		if remaining := item.Expires - now; remaining > 0 {
			row.TTL = remaining
		} else {
			row.TTL = 0
		}
	}
	if item.LastRead > 0 {
		row.LastAccess = timeUnix(item.LastRead)
	}
	return row
}

func (s *Session) readKey(ctx context.Context, key string) (keyDetail, error) {
	if err := validateKey(key); err != nil {
		return keyDetail{}, err
	}
	item, err := s.client.Get(key)
	if err != nil {
		return keyDetail{}, mapClientError(err)
	}
	detail := keyDetail{
		Key:   key,
		Flags: item.Flags,
		CAS:   item.CasID,
		Size:  int64(len(item.Value)),
		TTL:   -1,
	}
	value := item.Value
	if len(value) > s.opts.ValueLimit {
		value = value[:s.opts.ValueLimit]
		detail.Truncated = true
	}
	detail.Type, detail.Value = encodeValue(value)

	meta, err := s.itemMetadata(ctx, key)
	if err != nil && !errors.Is(err, plugin.ErrNotFound) {
		return keyDetail{}, err
	}
	applyMetadata(&detail, meta)
	return detail, nil
}

// itemMetadata reads "me <key>", which reports slab class, last access, and the
// stored expiry without bumping the item in the LRU.
func (s *Session) itemMetadata(ctx context.Context, key string) (map[string]string, error) {
	var meta map[string]string
	err := s.withAdmin(ctx, func(c *textConn) error {
		out, err := c.metaDebug(ctx, key, s.opts.Timeout)
		if err != nil {
			return err
		}
		meta = out
		return nil
	})
	return meta, err
}

func applyMetadata(detail *keyDetail, meta map[string]string) {
	if meta == nil {
		return
	}
	if raw, ok := meta["cls"]; ok {
		detail.Class = int(atoiDefault(raw, 0))
	}
	if raw, ok := meta["la"]; ok {
		detail.LastAccess = time.Now().UTC().Add(-time.Duration(atoiDefault(raw, 0)) * time.Second).Format(time.RFC3339)
	}
	if raw, ok := meta["fetch"]; ok {
		detail.Fetched = raw == "yes" || raw == "1"
	}
	// "me" reports exp as the remaining TTL in seconds, with -1 for an item that
	// never expires; the LRU crawler dump reports it as an absolute timestamp.
	if raw, ok := meta["exp"]; ok {
		detail.TTL = atoiDefault(raw, -1)
		if detail.TTL < 0 {
			detail.TTL = -1
		}
	}
}

// storeRequest is the normalised payload for every write path: the KV panel's
// write route and the explicit store action share it.
type storeRequest struct {
	Key      string
	Mode     string
	Value    []byte
	Flags    uint32
	TTL      int32
	CAS      uint64
	HasTTL   bool
	HasFlags bool
}

var storeModes = map[string]bool{"set": true, "add": true, "replace": true, "append": true, "prepend": true, "cas": true}

func (s *Session) store(ctx context.Context, req storeRequest) error {
	if err := validateKey(req.Key); err != nil {
		return err
	}
	if !storeModes[req.Mode] {
		return fmt.Errorf("%w: unsupported store mode %q", plugin.ErrInvalidInput, req.Mode)
	}
	if int64(len(req.Value)) > int64(s.opts.ValueLimit) {
		return fmt.Errorf("%w: value exceeds the connection's %d byte limit", plugin.ErrInvalidInput, s.opts.ValueLimit)
	}

	// Preserve the item's current TTL and flags unless the caller set them, so a
	// value edit from the key browser never silently resets an expiry. "me"
	// reports the remaining TTL in seconds.
	if !req.HasTTL || !req.HasFlags {
		if existing, err := s.client.Get(req.Key); err == nil {
			if !req.HasFlags {
				req.Flags = existing.Flags
			}
			if !req.HasTTL {
				if meta, err := s.itemMetadata(ctx, req.Key); err == nil {
					if exp := atoiDefault(meta["exp"], -1); exp > 0 {
						req.TTL = expirationFor(exp)
					}
				}
			}
		}
	}

	item := &memcache.Item{Key: req.Key, Value: req.Value, Flags: req.Flags, Expiration: req.TTL, CasID: req.CAS}
	var err error
	switch req.Mode {
	case "set":
		err = s.client.Set(item)
	case "add":
		err = s.client.Add(item)
	case "replace":
		err = s.client.Replace(item)
	case "append":
		err = s.client.Append(item)
	case "prepend":
		err = s.client.Prepend(item)
	case "cas":
		if req.CAS == 0 {
			return fmt.Errorf("%w: cas mode requires a CAS id", plugin.ErrInvalidInput)
		}
		err = s.client.CompareAndSwap(item)
	}
	return mapClientError(err)
}

func validateKey(key string) error {
	switch {
	case key == "":
		return fmt.Errorf("%w: key is required", plugin.ErrInvalidInput)
	case len(key) > 250:
		return fmt.Errorf("%w: memcached keys are limited to 250 bytes", plugin.ErrInvalidInput)
	case strings.ContainsAny(key, " \r\n\x00"):
		return fmt.Errorf("%w: memcached keys cannot contain spaces, control characters, or newlines", plugin.ErrInvalidInput)
	default:
		return nil
	}
}

// decodeValue turns the browser's typed value payload into raw bytes.
func decodeValue(valueType, value string) ([]byte, error) {
	switch valueType {
	case valueBinary:
		raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(value))
		if err != nil {
			return nil, fmt.Errorf("%w: binary values must be base64 encoded", plugin.ErrInvalidInput)
		}
		return raw, nil
	case valueJSON:
		if !json.Valid([]byte(value)) {
			return nil, fmt.Errorf("%w: value is not valid JSON", plugin.ErrInvalidInput)
		}
		return []byte(value), nil
	default:
		return []byte(value), nil
	}
}

// encodeValue classifies a stored value so the editor picks the right mode and
// binary payloads survive the JSON round trip.
func encodeValue(raw []byte) (string, string) {
	if !utf8.Valid(raw) {
		return valueBinary, base64.StdEncoding.EncodeToString(raw)
	}
	text := string(raw)
	if json.Valid(raw) && strings.ContainsAny(strings.TrimSpace(text), "{[\"") {
		return valueJSON, text
	}
	return valueText, text
}

func mapClientError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, memcache.ErrCacheMiss):
		return fmt.Errorf("%w: key not found", plugin.ErrNotFound)
	case errors.Is(err, memcache.ErrNotStored):
		return fmt.Errorf("%w: the store condition was not met", plugin.ErrConflict)
	case errors.Is(err, memcache.ErrCASConflict):
		return fmt.Errorf("%w: the item changed since it was read", plugin.ErrConflict)
	case errors.Is(err, memcache.ErrMalformedKey):
		return fmt.Errorf("%w: malformed key", plugin.ErrInvalidInput)
	case errors.Is(err, memcache.ErrNoServers):
		return fmt.Errorf("%w: no memcached server available", plugin.ErrUnavailable)
	case errors.Is(err, context.Canceled):
		return err
	default:
		return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
}

func parseTTL(raw any) (int32, bool, error) {
	if raw == nil {
		return 0, false, nil
	}
	var seconds int64
	switch v := raw.(type) {
	case float64:
		seconds = int64(v)
	case int:
		seconds = int64(v)
	case int64:
		seconds = v
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return 0, false, nil
		}
		parsed, err := strconv.ParseInt(trimmed, 10, 64)
		if err != nil {
			return 0, false, fmt.Errorf("%w: TTL must be a number of seconds", plugin.ErrInvalidInput)
		}
		seconds = parsed
	default:
		return 0, false, fmt.Errorf("%w: TTL must be a number of seconds", plugin.ErrInvalidInput)
	}
	if seconds < 0 {
		return 0, false, fmt.Errorf("%w: TTL cannot be negative", plugin.ErrInvalidInput)
	}
	if seconds > math.MaxInt32 {
		return 0, false, fmt.Errorf("%w: TTL must be at most %d seconds", plugin.ErrInvalidInput, math.MaxInt32)
	}
	return int32(seconds), true, nil
}

// expirationFor turns a remaining TTL in seconds into the value a store command
// expects. memcached reads anything above 30 days as an absolute Unix timestamp,
// so a longer remaining TTL has to be sent as one or the item is stored already
// expired.
func expirationFor(seconds int64) int32 {
	if seconds <= relativeTTLMax {
		return int32(seconds)
	}
	absolute := time.Now().Unix() + seconds
	if absolute > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(absolute)
}
