package couchbase

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

// resourceWatch keeps the sidebar lists live. Couchbase exposes no change feed
// for cluster metadata, so one generic poller diffs each list by resource
// identity and pushes only what actually changed.
func resourceWatch(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := session(rc)
	if err != nil {
		return err
	}
	kind := rc.Param("kind")
	fetch, err := watchFetcher(rc, s)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(client)
	known := map[string]string{}
	first := true
	return tick(rc, client, 5*time.Second, func() error {
		rows, err := fetch(rc.Ctx)
		if err != nil {
			return nil
		}
		events, next := diffResources(kind, known, rows)
		// The panel already rendered the list route's page, so the first poll only
		// primes the baseline instead of replaying every row.
		if !first {
			for _, event := range events {
				if err := enc.Encode(event); err != nil {
					return err
				}
			}
		}
		known = next
		first = false
		return nil
	})
}

// diffResources compares a freshly fetched list against the last snapshot and
// returns the resource events the browser needs, plus the new snapshot.
func diffResources(kind string, known map[string]string, rows []row) ([]plugin.ResourceEvent, map[string]string) {
	next := make(map[string]string, len(rows))
	events := make([]plugin.ResourceEvent, 0, 4)
	for _, item := range rows {
		ref, ok := item["ref"].(plugin.ResourceIdentity)
		if !ok || ref.UID == "" {
			continue
		}
		encoded, err := json.Marshal(item)
		if err != nil {
			continue
		}
		next[ref.UID] = string(encoded)
		previous, existed := known[ref.UID]
		switch {
		case !existed:
			events = append(events, plugin.ResourceEvent{Type: "added", Ref: ref, Resource: item})
		case previous != string(encoded):
			events = append(events, plugin.ResourceEvent{Type: "modified", Ref: ref, Resource: item})
		}
	}
	removed := make([]string, 0, len(known))
	for uid := range known {
		if _, ok := next[uid]; !ok {
			removed = append(removed, uid)
		}
	}
	sort.Strings(removed)
	for _, uid := range removed {
		var previous row
		if json.Unmarshal([]byte(known[uid]), &previous) != nil {
			continue
		}
		events = append(events, plugin.ResourceEvent{
			Type: "deleted",
			Ref:  plugin.ResourceIdentity{Kind: kind, Name: stringValue(previous["name"]), UID: uid},
		})
	}
	return events, next
}

// watchFetcher resolves the watched kind to the same reader its list route uses,
// so a live row and a fetched row are always the same shape.
func watchFetcher(rc *plugin.RequestContext, s *Session) (func(context.Context) ([]row, error), error) {
	switch rc.Param("kind") {
	case "cluster":
		return func(ctx context.Context) ([]row, error) {
			item, err := s.clusterOverview(ctx)
			if err != nil {
				return nil, err
			}
			return []row{item}, nil
		}, nil
	case "bucket":
		return s.bucketSummaries, nil
	case "scope":
		bucket, err := bucketName(rc, s)
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context) ([]row, error) { return s.scopeRows(ctx, bucket) }, nil
	case "collection":
		k, err := keyspaceFrom(rc, s)
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context) ([]row, error) { return s.collectionRows(ctx, k) }, nil
	case "index":
		return func(ctx context.Context) ([]row, error) {
			indexes, err := s.indexStatuses(ctx)
			if err != nil {
				return nil, err
			}
			rows := make([]row, 0, len(indexes))
			for _, index := range indexes {
				rows = append(rows, index.row())
			}
			return rows, nil
		}, nil
	case "replication":
		return func(ctx context.Context) ([]row, error) {
			tasks, err := s.tasks(ctx)
			if err != nil {
				return nil, err
			}
			rows := make([]row, 0, len(tasks))
			for _, task := range tasks {
				if task.Type == "xdcr" && task.ID != "" {
					rows = append(rows, task.row())
				}
			}
			return rows, nil
		}, nil
	default:
		return nil, fmt.Errorf("%w: %q is not a watchable resource kind", plugin.ErrInvalidInput, rc.Param("kind"))
	}
}
