package weaviate

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

const watchInterval = 5 * time.Second

// watchKinds maps a resource kind onto its list handler. Weaviate has no change
// feed, so the watcher polls the same rows the table renders and emits the
// difference. The first snapshot is sent as adds, which is idempotent because
// the renderer keys rows by ref.
var watchKinds = map[string]func(*plugin.RequestContext) ([]row, error){
	"collection": listRows(listCollections),
	"object":     listRows(listObjects),
	"node":       listRows(listNodes),
	"backup":     listRows(listBackups),
}

type watchEntry struct {
	ref         plugin.ResourceIdentity
	item        row
	fingerprint string
}

func listRows(handler plugin.Handler) func(*plugin.RequestContext) ([]row, error) {
	return func(rc *plugin.RequestContext) ([]row, error) {
		out, err := handler(rc)
		if err != nil {
			return nil, err
		}
		page, ok := out.(plugin.Page[row])
		if !ok {
			return nil, fmt.Errorf("%w: list route returned %T", plugin.ErrUnavailable, out)
		}
		return page.Items, nil
	}
}

func watchResources(rc *plugin.RequestContext, client plugin.ClientStream) error {
	kind := rc.Param("kind")
	load, ok := watchKinds[kind]
	if !ok {
		return fmt.Errorf("%w: %q is not a watchable resource kind", plugin.ErrInvalidInput, kind)
	}
	enc := json.NewEncoder(client)
	ticker := time.NewTicker(watchInterval)
	defer ticker.Stop()

	previous := map[string]watchEntry{}
	for {
		if current, err := watchSnapshot(rc, load); err == nil {
			for _, event := range watchEvents(previous, current) {
				if encErr := enc.Encode(event); encErr != nil {
					return nil
				}
			}
			previous = current
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

func watchSnapshot(rc *plugin.RequestContext, load func(*plugin.RequestContext) ([]row, error)) (map[string]watchEntry, error) {
	items, err := load(rc)
	if err != nil {
		return nil, err
	}
	out := make(map[string]watchEntry, len(items))
	for _, item := range items {
		ref, ok := item["ref"].(plugin.ResourceIdentity)
		if !ok || ref.UID == "" {
			continue
		}
		fingerprint, err := json.Marshal(item)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
		}
		out[ref.Namespace+"/"+ref.UID] = watchEntry{ref: ref, item: item, fingerprint: string(fingerprint)}
	}
	return out, nil
}

func watchEvents(previous, current map[string]watchEntry) []plugin.ResourceEvent {
	events := make([]plugin.ResourceEvent, 0, len(current))
	for key, entry := range current {
		before, existed := previous[key]
		switch {
		case !existed:
			events = append(events, plugin.ResourceEvent{Type: "added", Ref: entry.ref, Resource: entry.item})
		case before.fingerprint != entry.fingerprint:
			events = append(events, plugin.ResourceEvent{Type: "modified", Ref: entry.ref, Resource: entry.item})
		}
	}
	for key, entry := range previous {
		if _, ok := current[key]; !ok {
			events = append(events, plugin.ResourceEvent{Type: "deleted", Ref: entry.ref})
		}
	}
	return events
}
