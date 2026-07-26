package couchdb

import (
	"encoding/json"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

// pollWatch is the shared StreamResource pump. CouchDB exposes a real change
// feed per database (see changesStream and watchDocuments); cluster-wide objects
// such as _active_tasks and _scheduler/docs have no feed, so those watches poll
// and emit the same ResourceEvent frames the renderer patches rows with. Server,
// design document, view and node objects declare no watch at all: CouchDB reports
// no change signal for them, and a poll dressed up as a feed would only pretend to.
func pollWatch(rc *plugin.RequestContext, client plugin.ClientStream, interval time.Duration, snapshot func() ([]plugin.ResourceEvent, error)) error {
	enc := json.NewEncoder(client)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	emit := func() error {
		events, err := snapshot()
		if err != nil {
			// A transient upstream failure must not kill a live panel.
			return nil
		}
		for _, event := range events {
			if err := enc.Encode(event); err != nil {
				return err
			}
		}
		return nil
	}
	if err := emit(); err != nil {
		return err
	}
	for {
		select {
		case <-client.Context().Done():
			return nil
		case <-rc.Ctx.Done():
			return nil
		case <-ticker.C:
			if err := emit(); err != nil {
				return err
			}
		}
	}
}

// pollObject pushes object snapshots for detail/editor panels, skipping frames
// that did not change so a clean editor is never disturbed needlessly.
func pollObject(rc *plugin.RequestContext, client plugin.ClientStream, interval time.Duration, snapshot func() (any, string, error)) error {
	enc := json.NewEncoder(client)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	last := ""
	emit := func() error {
		value, version, err := snapshot()
		if err != nil || version == last {
			return nil
		}
		last = version
		return enc.Encode(value)
	}
	if err := emit(); err != nil {
		return err
	}
	for {
		select {
		case <-client.Context().Done():
			return nil
		case <-rc.Ctx.Done():
			return nil
		case <-ticker.C:
			if err := emit(); err != nil {
				return err
			}
		}
	}
}

func modifiedEvents(rows []row, kind string, identity func(row) plugin.ResourceIdentity) []plugin.ResourceEvent {
	events := make([]plugin.ResourceEvent, 0, len(rows))
	for _, item := range rows {
		ref := identity(item)
		ref.Kind = kind
		events = append(events, plugin.ResourceEvent{Type: "modified", Ref: ref, Resource: item})
	}
	return events
}
