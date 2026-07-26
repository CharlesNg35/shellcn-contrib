package milvus

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

// activityLog is the in-session operation feed rendered by the activity
// timeline. Milvus has no server-side event stream, so the plugin records the
// lifecycle operations it performs itself.
type activityLog struct {
	mu      sync.Mutex
	max     int
	seq     int64
	records []row
}

func newActivityLog(max int) *activityLog {
	if max <= 0 {
		max = 100
	}
	return &activityLog{max: max}
}

func (a *activityLog) record(operation, target, message string, err error) {
	if a == nil {
		return
	}
	severity := "success"
	if err != nil {
		severity = "danger"
		message = strings.TrimSpace(message + " — " + err.Error())
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.seq++
	id := fmt.Sprintf("activity-%d", a.seq)
	entry := row{
		"id":        id,
		"time":      time.Now().UTC().Format(time.RFC3339Nano),
		"operation": operation,
		"target":    target,
		"message":   message,
		"severity":  severity,
		"icon":      activityIcon(operation),
		"ref":       plugin.ResourceIdentity{Kind: "activity", Name: operation, UID: id},
	}
	a.records = append([]row{entry}, a.records...)
	if len(a.records) > a.max {
		a.records = a.records[:a.max]
	}
}

func (a *activityLog) list() []row {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]row, len(a.records))
	copy(out, a.records)
	return out
}

func activityIcon(operation string) string {
	switch {
	case strings.Contains(operation, "drop"), strings.Contains(operation, "delete"), strings.Contains(operation, "revoke"):
		return "trash-2"
	case strings.Contains(operation, "create"), strings.Contains(operation, "insert"), strings.Contains(operation, "grant"):
		return "plus"
	case strings.Contains(operation, "load"):
		return "download"
	case strings.Contains(operation, "release"):
		return "eject"
	case strings.Contains(operation, "compact"), strings.Contains(operation, "flush"):
		return "layers"
	default:
		return "activity"
	}
}

// mapError translates Milvus/gRPC failures into the SDK sentinels the gateway
// maps to HTTP status codes.
func mapError(err error) error {
	if err == nil {
		return nil
	}
	for _, sentinel := range []error{
		plugin.ErrInvalidInput, plugin.ErrNotFound, plugin.ErrUnauthorized,
		plugin.ErrForbidden, plugin.ErrConflict, plugin.ErrUnavailable, plugin.ErrNotSupported,
	} {
		if errors.Is(err, sentinel) {
			return err
		}
	}
	text := strings.ToLower(err.Error())
	switch {
	case strings.Contains(text, "not found"), strings.Contains(text, "not exist"), strings.Contains(text, "can't find"):
		return fmt.Errorf("%w: %v", plugin.ErrNotFound, err)
	case strings.Contains(text, "already exist"), strings.Contains(text, "duplicate"):
		return fmt.Errorf("%w: %v", plugin.ErrConflict, err)
	case strings.Contains(text, "unauthenticated"), strings.Contains(text, "auth check"), strings.Contains(text, "invalid credential"):
		return fmt.Errorf("%w: %v", plugin.ErrUnauthorized, err)
	case strings.Contains(text, "permission denied"), strings.Contains(text, "privilege"):
		return fmt.Errorf("%w: %v", plugin.ErrForbidden, err)
	case strings.Contains(text, "not loaded"), strings.Contains(text, "not been loaded"):
		return fmt.Errorf("%w: %v", plugin.ErrConflict, err)
	case strings.Contains(text, "illegal"), strings.Contains(text, "invalid"), strings.Contains(text, "should not be empty"):
		return fmt.Errorf("%w: %v", plugin.ErrInvalidInput, err)
	default:
		return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
}
