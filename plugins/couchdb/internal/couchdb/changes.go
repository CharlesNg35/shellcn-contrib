package couchdb

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

// changeOrigins feeds the log panel's "Start from" selector. CouchDB accepts any
// update sequence, but the two useful starting points are now and the beginning
// of history.
func changeOrigins(rc *plugin.RequestContext) (any, error) {
	return []plugin.Option{
		{Label: "New changes only", Value: "now"},
		{Label: "Replay from the beginning", Value: "0"},
	}, nil
}

// changesStream tails CouchDB's continuous _changes feed and emits one JSON log
// frame per change. Heartbeats keep the upstream connection alive and are
// discarded; the browser sees only real changes.
func changesStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, db, err := dbSession(rc)
	if err != nil {
		return err
	}
	since := strings.TrimSpace(rc.Param("since"))
	if since == "" {
		since = "now"
	}
	enc := json.NewEncoder(client)
	body, err := s.client.stream(rc.Ctx, http.MethodGet, dbPath(db)+"/_changes", url.Values{
		"feed":         []string{"continuous"},
		"since":        []string{since},
		"heartbeat":    []string{"10000"},
		"include_docs": []string{"true"},
		"conflicts":    []string{"true"},
		"style":        []string{"all_docs"},
	}, nil)
	if err != nil {
		return err
	}
	release := closeFeedOnDisconnect(client, body)
	defer release()

	if err := enc.Encode(row{
		"time":    time.Now().UTC().Format(time.RFC3339),
		"level":   "info",
		"message": "Tailing " + db + "/_changes from " + since,
	}); err != nil {
		return err
	}

	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64<<10), 8<<20)
	for scanner.Scan() {
		change := decodeChange(scanner.Bytes())
		if change == nil {
			continue
		}
		if last := change["last_seq"]; last != nil {
			if err := enc.Encode(row{
				"time":    time.Now().UTC().Format(time.RFC3339),
				"level":   "info",
				"message": "Feed closed at sequence " + stringOf(last),
			}); err != nil {
				return err
			}
			return nil
		}
		if err := enc.Encode(changeFrame(db, change)); err != nil {
			return nil
		}
	}
	// A feed that ends on an error (an oversized change, a dropped upstream) must
	// say so; the panel would otherwise look like an idle database.
	if err := scanner.Err(); err != nil && client.Context().Err() == nil && rc.Ctx.Err() == nil {
		return enc.Encode(row{
			"time":    time.Now().UTC().Format(time.RFC3339),
			"level":   "error",
			"message": "The changes feed stopped: " + err.Error(),
		})
	}
	return nil
}

// closeFeedOnDisconnect unblocks a continuous feed read when the client goes
// away. The returned release closes the feed and waits for that watcher, so a
// feed that ends on its own leaves nothing behind either.
func closeFeedOnDisconnect(client plugin.ClientStream, body io.Closer) func() {
	ended := make(chan struct{})
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		select {
		case <-client.Context().Done():
		case <-ended:
		}
		_ = body.Close()
	}()
	return func() {
		close(ended)
		<-closed
	}
}

func changeFrame(db string, change row) row {
	doc := mapOf(change["doc"])
	id := stringOf(change["id"])
	revs := make([]string, 0, 2)
	for _, entry := range sliceOf(change["changes"]) {
		revs = append(revs, stringOf(mapOf(entry)["rev"]))
	}
	level := "info"
	action := "updated"
	switch {
	case change["deleted"] == true:
		level, action = "warn", "deleted"
	case len(revs) > 1:
		level, action = "error", "conflicted"
	case revGeneration(changeRev(change, doc)) == 1:
		action = "created"
	}
	frame := row{
		"time":    time.Now().UTC().Format(time.RFC3339),
		"level":   level,
		"seq":     stringOf(change["seq"]),
		"id":      id,
		"action":  action,
		"revs":    revs,
		"message": db + " " + action + " " + id + " (" + strings.Join(revs, ", ") + ")",
	}
	if doc != nil {
		frame["doc"] = doc
	}
	return frame
}
