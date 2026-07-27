package pulsar

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	pulsarclient "github.com/apache/pulsar-client-go/pulsar"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

// maxPayloadPreview truncates the payload a browse row carries so a grid page
// stays a reasonable size even on a topic of multi-megabyte messages.
const maxPayloadPreview = 64 << 10

func messagePositions(rc *plugin.RequestContext) (any, error) {
	items := positionOptions()
	total := len(items)
	return plugin.Page[plugin.Option]{Items: items, Total: &total}, nil
}

// listMessages walks a topic backwards from its newest message. Pulsar's admin
// API answers one message per call, so the walk is bounded by the page limit
// and the connection's message limit, and the cursor is the absolute position
// already read — a later page keeps walking instead of re-reading the head.
func listMessages(rc *plugin.RequestContext) (any, error) {
	s, ref, err := topicScope(rc)
	if err != nil {
		return nil, err
	}
	req, err := rc.Page()
	if err != nil {
		return nil, err
	}
	offset := 0
	if req.Cursor != "" {
		parsed, convErr := strconv.Atoi(req.Cursor)
		if convErr != nil || parsed < 0 {
			return nil, fmt.Errorf("%w: invalid cursor", plugin.ErrInvalidInput)
		}
		offset = parsed
	}
	limit := min(req.Limit, s.opts.MessageLimit)
	subscription := strings.TrimSpace(param(rc, "subscription"))
	if subscription != "" {
		if _, err := simpleName("subscription", subscription); err != nil {
			return nil, err
		}
	}
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()

	rows := make([]row, 0, limit)
	read := 0
	for read < limit {
		position := offset + read + 1
		item, err := s.messageAt(ctx, ref, subscription, position)
		if err != nil {
			// The walk ran past the end of the topic (or the broker's peek budget);
			// what was read so far is still a valid, complete last page.
			if errors.Is(err, plugin.ErrNotFound) || errors.Is(err, plugin.ErrInvalidInput) || ctx.Err() != nil {
				break
			}
			// The broker answers 405 for the two shapes it cannot read a stored
			// message from; say which one instead of forwarding "not supported".
			if errors.Is(err, plugin.ErrNotSupported) {
				return nil, fmt.Errorf("%w: %s cannot be browsed directly (a partitioned topic is browsed one partition at a time, and a non-persistent topic stores nothing) - use the live tail instead", plugin.ErrNotSupported, ref)
			}
			return nil, err
		}
		rows = append(rows, item)
		read++
	}
	next := ""
	if read == limit && limit > 0 {
		next = strconv.Itoa(offset + read)
	}
	// The log is a cursor walk, not a materialized set, so the grid's search and
	// sort apply to the window that was read; the cursor still counts positions.
	items := plugin.SortRows(plugin.FilterRows(rows, req.Search()), req.Sort)
	return plugin.Page[row]{Items: items, NextCursor: next}, nil
}

// messageAt reads the nth message from the end of the topic, or the nth message
// after a subscription's cursor when one is selected.
func (s *Session) messageAt(ctx context.Context, ref topicRef, subscription string, position int) (row, error) {
	path := ref.path() + "/examinemessage"
	query := url.Values{"initialPosition": {positionLatest}, "messagePosition": {strconv.Itoa(position)}}
	if subscription != "" {
		path = subscriptionPath(ref, subscription) + "/position/" + strconv.Itoa(position)
		query = nil
	}
	header, body, err := s.raw(ctx, path, query)
	if err != nil {
		if isEndOfTopic(err) {
			return nil, fmt.Errorf("%w: no message at position %d", plugin.ErrNotFound, position)
		}
		return nil, err
	}
	return messageRow(ref, position, header, body), nil
}

// isEndOfTopic reports the "examined past the last message" case: Pulsar answers
// it with a 500 that wraps BookKeeper's IncorrectParameter (code -14), so the
// walk must treat it as the end of the topic rather than a broker failure.
func isEndOfTopic(err error) bool {
	return strings.Contains(err.Error(), "error code: -14")
}

func messageRow(ref topicRef, position int, header http.Header, body []byte) row {
	payload, encoding := payloadText(body)
	item := row{
		"position":     position,
		"topic":        ref.String(),
		"message_id":   header.Get("X-Pulsar-Message-ID"),
		"publish_time": pulsarTime(header.Get("X-Pulsar-publish-time")),
		"producer":     header.Get("X-Pulsar-producer-name"),
		"key":          header.Get("X-Pulsar-partition-key"),
		"size":         len(body),
		"encoding":     encoding,
		"payload":      payload,
		"properties":   messageProperties(header),
	}
	if batch := header.Get("X-Pulsar-num-batch-message"); batch != "" {
		if count, err := strconv.Atoi(batch); err == nil {
			item["batch_size"] = count
		}
	}
	if event := pulsarTime(header.Get("X-Pulsar-event-time")); event != nil {
		item["event_time"] = event
	}
	return item
}

// chunkPropertyHeaders are chunking metadata, not application properties, even
// though they share the property header prefix.
var chunkPropertyHeaders = map[string]bool{"Total-Chunks": true, "Chunk-Id": true}

// messageProperties reads the application properties the broker attaches. They
// arrive as one JSON header; brokers that predate it emit a flat header per
// entry instead, and Go canonicalizes those names, so their case is lost.
func messageProperties(header http.Header) map[string]string {
	out := map[string]string{}
	if raw := header.Get("X-Pulsar-PROPERTY"); raw != "" {
		_ = json.Unmarshal([]byte(raw), &out)
	}
	if len(out) > 0 {
		return out
	}
	for name, values := range header {
		key, ok := strings.CutPrefix(name, "X-Pulsar-Property-")
		if !ok || key == "" || len(values) == 0 || chunkPropertyHeaders[key] {
			continue
		}
		out[strings.ToLower(key)] = values[0]
	}
	return out
}

// pulsarTime parses the broker's date header format, which is RFC3339 with
// milliseconds and a numeric zone.
func pulsarTime(raw string) any {
	value := strings.TrimSpace(raw)
	if value == "" {
		return nil
	}
	for _, layout := range []string{"2006-01-02T15:04:05.999Z0700", time.RFC3339Nano, time.RFC1123} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.UTC()
		}
	}
	return value
}

func payloadText(body []byte) (string, string) {
	if len(body) > maxPayloadPreview {
		body = body[:maxPayloadPreview]
	}
	if utf8.Valid(body) && !strings.ContainsRune(string(body), 0) {
		return string(body), "string"
	}
	return base64.StdEncoding.EncodeToString(body), "base64"
}

func produceMessage(rc *plugin.RequestContext) (any, error) {
	s, ref, err := writableTopic(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		Key        string            `json:"key"`
		Payload    string            `json:"payload"`
		Encoding   string            `json:"encoding"`
		Properties map[string]string `json:"properties"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	payload := []byte(req.Payload)
	if req.Encoding == "base64" {
		payload, err = base64.StdEncoding.DecodeString(req.Payload)
		if err != nil {
			return nil, fmt.Errorf("%w: payload is not valid base64", plugin.ErrInvalidInput)
		}
	}
	conn, err := s.broker()
	if err != nil {
		return nil, err
	}
	producer, err := conn.CreateProducer(pulsarclient.ProducerOptions{
		Topic:           ref.String(),
		SendTimeout:     s.opts.Timeout,
		DisableBatching: true,
	})
	if err != nil {
		return nil, brokerErr(err)
	}
	defer producer.Close()
	ctx, cancel := context.WithTimeout(rc.Ctx, s.opts.Timeout)
	defer cancel()
	id, err := producer.Send(ctx, &pulsarclient.ProducerMessage{
		Payload:    payload,
		Key:        req.Key,
		Properties: req.Properties,
	})
	if err != nil {
		return nil, brokerErr(err)
	}
	return row{"ok": true, "topic": ref.String(), "message_id": id.String()}, nil
}

// consumeStream tails a topic live. It always subscribes with its own
// non-durable subscription so the tail can acknowledge what it displays without
// ever consuming an operator's real backlog.
func consumeStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, ref, err := topicScope(rc)
	if err != nil {
		return err
	}
	position := param(rc, "position")
	initial := pulsarclient.SubscriptionPositionLatest
	switch position {
	case "", positionLatest:
		position = positionLatest
	case positionEarliest:
		initial = pulsarclient.SubscriptionPositionEarliest
	default:
		return fmt.Errorf("%w: unsupported position %q", plugin.ErrInvalidInput, position)
	}
	conn, err := s.broker()
	if err != nil {
		return err
	}
	name := "shellcn-tail-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	consumer, err := conn.Subscribe(pulsarclient.ConsumerOptions{
		Topic:                       ref.String(),
		SubscriptionName:            name,
		Type:                        pulsarclient.Shared,
		SubscriptionMode:            pulsarclient.NonDurable,
		SubscriptionInitialPosition: initial,
		ReceiverQueueSize:           100,
	})
	if err != nil {
		return brokerErr(err)
	}
	defer consumer.Close()

	ctx := client.Context()
	if err := writeLogFrame(client, time.Now().UTC(), "Tailing "+ref.String()+" from "+position); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case received, ok := <-consumer.Chan():
			if !ok {
				return nil
			}
			msg := received.Message
			_ = consumer.Ack(msg)
			if err := writeLogFrame(client, msg.PublishTime(), formatMessage(msg)); err != nil {
				if ctx.Err() != nil || errors.Is(err, io.ErrClosedPipe) {
					return nil
				}
				return err
			}
		}
	}
}

func formatMessage(msg pulsarclient.Message) string {
	var b strings.Builder
	b.WriteString(msg.ID().String())
	if key := msg.Key(); key != "" {
		b.WriteString(" key=" + key)
	}
	if props := msg.Properties(); len(props) > 0 {
		if encoded, err := json.Marshal(props); err == nil {
			b.WriteString(" props=" + string(encoded))
		}
	}
	payload, encoding := payloadText(msg.Payload())
	if encoding == "base64" {
		b.WriteString(" base64=")
	} else {
		b.WriteString(" ")
	}
	b.WriteString(payload)
	return b.String()
}

func writeLogFrame(w io.Writer, ts time.Time, line string) error {
	frame, err := json.Marshal(struct {
		TS   string `json:"ts,omitempty"`
		Line string `json:"line"`
	}{TS: ts.UTC().Format(time.RFC3339), Line: line})
	if err != nil {
		return err
	}
	_, err = w.Write(frame)
	return err
}

// brokerErr maps binary-protocol client failures onto the SDK sentinels; the
// client reports them as opaque *pulsar.Error values.
func brokerErr(err error) error {
	if err == nil {
		return nil
	}
	text := err.Error()
	switch {
	case strings.Contains(text, "TopicNotFound"), strings.Contains(text, "topic not found"):
		return fmt.Errorf("%w: %v", plugin.ErrNotFound, err)
	case strings.Contains(text, "AuthenticationError"):
		return fmt.Errorf("%w: %v", plugin.ErrUnauthorized, err)
	case strings.Contains(text, "AuthorizationError"), strings.Contains(text, "not authorized"):
		return fmt.Errorf("%w: %v", plugin.ErrForbidden, err)
	case strings.Contains(text, "ConsumerBusy"), strings.Contains(text, "ProducerBusy"):
		return fmt.Errorf("%w: %v", plugin.ErrConflict, err)
	default:
		return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
}
