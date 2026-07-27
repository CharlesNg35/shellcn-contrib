package mqtt

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	mqttclient "github.com/eclipse/paho.mqtt.golang"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

type row = plugin.TableRow

type actionResult struct {
	OK bool `json:"ok"`
}

func routes() []plugin.Route {
	return []plugin.Route{
		{ID: "mqtt.overview", Method: plugin.MethodGet, Path: "/overview", Permission: "mqtt.read", Risk: plugin.RiskSafe, AuditEvent: "mqtt.overview", Handle: overview},
		{ID: "mqtt.sys.list", Method: plugin.MethodGet, Path: "/sys", Permission: "mqtt.read", Risk: plugin.RiskSafe, AuditEvent: "mqtt.sys.list", Handle: listSys},
		{ID: "mqtt.topics.list", Method: plugin.MethodGet, Path: "/topics", Permission: "mqtt.topics.read", Risk: plugin.RiskSafe, AuditEvent: "mqtt.topics.list", Handle: listTopics},
		{ID: "mqtt.topics.tree", Method: plugin.MethodGet, Path: "/tree/topics", Permission: "mqtt.topics.read", Risk: plugin.RiskSafe, AuditEvent: "mqtt.topics.tree", Handle: treeTopics},
		{ID: "mqtt.topic.overview", Method: plugin.MethodGet, Path: "/topic", Permission: "mqtt.topics.read", Risk: plugin.RiskSafe, AuditEvent: "mqtt.topic.overview", Handle: topicOverview},
		{ID: "mqtt.retained.list", Method: plugin.MethodGet, Path: "/retained", Permission: "mqtt.topics.read", Risk: plugin.RiskSafe, AuditEvent: "mqtt.retained.list", Handle: listRetained},
		{ID: "mqtt.retained.rescan", Method: plugin.MethodPost, Path: "/retained/rescan", Permission: "mqtt.topics.read", Risk: plugin.RiskSafe, AuditEvent: "mqtt.retained.rescan", Handle: rescanRetained},
		{ID: "mqtt.retained.clear", Method: plugin.MethodDelete, Path: "/retained", Permission: "mqtt.messages.delete", Risk: plugin.RiskDestructive, AuditEvent: "mqtt.retained.clear", Handle: clearRetained},
		{ID: "mqtt.messages.list", Method: plugin.MethodGet, Path: "/messages", Permission: "mqtt.messages.read", Risk: plugin.RiskSafe, AuditEvent: "mqtt.messages.list", Handle: listMessages},
		{ID: "mqtt.messages.clear", Method: plugin.MethodPost, Path: "/messages/clear", Permission: "mqtt.messages.read", Risk: plugin.RiskSafe, AuditEvent: "mqtt.messages.clear", Handle: clearMessages},
		{ID: "mqtt.publish", Method: plugin.MethodPost, Path: "/publish", Permission: "mqtt.messages.write", Risk: plugin.RiskWrite, AuditEvent: "mqtt.publish", Input: publishSchema(), Handle: publishMessage},
		{ID: "mqtt.filters.options", Method: plugin.MethodGet, Path: "/filters", Permission: "mqtt.topics.read", Risk: plugin.RiskSafe, AuditEvent: "mqtt.filters.options", Handle: filterOptions},
		{ID: "mqtt.qos.options", Method: plugin.MethodGet, Path: "/qos", Permission: "mqtt.read", Risk: plugin.RiskSafe, AuditEvent: "mqtt.qos.options", Handle: qosOptions},
		{ID: "mqtt.subscribe", Method: plugin.MethodWS, Path: "/subscribe", Permission: "mqtt.messages.read", Risk: plugin.RiskSafe, AuditEvent: "mqtt.subscribe", Stream: subscribeStream},
		{ID: "mqtt.emqx.clients.list", Method: plugin.MethodGet, Path: "/emqx/clients", Permission: "mqtt.clients.read", Risk: plugin.RiskSafe, AuditEvent: "mqtt.emqx.clients.list", Handle: listEMQXClients},
		{ID: "mqtt.emqx.client.overview", Method: plugin.MethodGet, Path: "/emqx/clients/{clientid}", Permission: "mqtt.clients.read", Risk: plugin.RiskSafe, AuditEvent: "mqtt.emqx.client.overview", Handle: emqxClientOverview},
		{ID: "mqtt.emqx.client.kick", Method: plugin.MethodDelete, Path: "/emqx/clients/{clientid}", Permission: "mqtt.clients.delete", Risk: plugin.RiskDestructive, AuditEvent: "mqtt.emqx.client.kick", Handle: kickEMQXClient},
		{ID: "mqtt.emqx.subscriptions.list", Method: plugin.MethodGet, Path: "/emqx/subscriptions", Permission: "mqtt.clients.read", Risk: plugin.RiskSafe, AuditEvent: "mqtt.emqx.subscriptions.list", Handle: listEMQXSubscriptions},
		{ID: "mqtt.emqx.topics.list", Method: plugin.MethodGet, Path: "/emqx/topics", Permission: "mqtt.topics.read", Risk: plugin.RiskSafe, AuditEvent: "mqtt.emqx.topics.list", Handle: listEMQXTopics},
		{ID: "mqtt.emqx.nodes.list", Method: plugin.MethodGet, Path: "/emqx/nodes", Permission: "mqtt.read", Risk: plugin.RiskSafe, AuditEvent: "mqtt.emqx.nodes.list", Handle: listEMQXNodes},
	}
}

func publishSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Message", Fields: []plugin.Field{
		{Key: "topic", Label: "Topic", Type: plugin.FieldText, Placeholder: "sensors/kitchen/temperature", Help: "Required unless publishing from a topic view, which supplies the selected topic."},
		{Key: "payload", Label: "Payload", Type: plugin.FieldTextarea},
		{Key: "encoding", Label: "Payload encoding", Type: plugin.FieldSelect, Required: true, Default: "string", Options: []plugin.Option{
			{Label: "String", Value: "string"},
			{Label: "Base64", Value: "base64"},
		}},
		{Key: "qos", Label: "QoS", Type: plugin.FieldSelect, Required: true, Default: "0", Options: qosOptionList()},
		{Key: "retain", Label: "Retain", Type: plugin.FieldToggle, Help: "The broker keeps this payload and replays it to every new subscriber of the topic."},
	}}}}
}

func qosOptionList() []plugin.Option {
	return []plugin.Option{
		{Label: "0 - At most once", Value: "0"},
		{Label: "1 - At least once", Value: "1"},
		{Label: "2 - Exactly once", Value: "2"},
	}
}

func mqttSession(rc *plugin.RequestContext) (*Session, error) {
	return unwrap(rc.Session)
}

// paramValue reads a route param, falling back to the renderer's query form for
// params a route path does not carry as a placeholder.
func paramValue(rc *plugin.RequestContext, name string) string {
	if v := strings.TrimSpace(rc.Param(name)); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p." + name))
}

// scanLimit caps how far a listing walks a paginated backend, so a sorted or
// searched list cannot turn one 50-row page into thousands of round trips
// against a large cluster.
const scanLimit = 2000

// scanPage adds the two signals a bounded walk needs to the paged wire contract:
// truncated (the walk stopped at the cap) and unavailable (the backing API is
// not configured, so the empty page is not an empty list).
type scanPage struct {
	plugin.Page[row]
	Truncated   bool   `json:"truncated,omitempty"`
	ScanLimit   int    `json:"scanLimit,omitempty"`
	Unavailable bool   `json:"unavailable,omitempty"`
	Note        string `json:"note,omitempty"`
}

// pageScan slices one page out of a bounded walk. A capped walk reports
// truncated instead of a total, because the total it could offer would be the
// cap rather than the backend's real count.
func pageScan(rc *plugin.RequestContext, rows []row, truncated bool, budget int) (scanPage, error) {
	page, err := broker.PageRows(rc, rows)
	if err != nil {
		return scanPage{}, err
	}
	out := scanPage{Page: page}
	if truncated {
		out.Page.Total, out.Truncated, out.ScanLimit = nil, true, budget
	}
	return out, nil
}

// scanBudget is how far one request walks: scanLimit, but never short of the end
// of the page being asked for, so a deep page keeps walking instead of stopping
// at the cap with rows the client can no longer reach.
func scanBudget(rc *plugin.RequestContext) int {
	req, err := rc.Page()
	if err != nil {
		return scanLimit
	}
	offset, err := cursorOffset(req.Cursor)
	if err != nil {
		return scanLimit
	}
	return max(scanLimit, offset+req.Limit+1)
}

// cursorOffset decodes the absolute row offset broker.PageRows emits, which is
// also what the grid falls back to when it derives a cursor itself.
func cursorOffset(cursor string) (int, error) {
	if cursor == "" {
		return 0, nil
	}
	offset, err := strconv.Atoi(cursor)
	if err != nil || offset < 0 {
		return 0, fmt.Errorf("%w: invalid cursor", plugin.ErrInvalidInput)
	}
	return offset, nil
}

func overview(rc *plugin.RequestContext) (any, error) {
	s, err := mqttSession(rc)
	if err != nil {
		return nil, err
	}
	stats := s.stats()
	connected := s.client != nil && s.client.IsConnected()
	return row{
		"address":               s.opts.Address,
		"scheme":                s.opts.Scheme,
		"client_id":             s.opts.ClientID,
		"connected":             connected,
		"protocol_version":      "MQTT 3.1.1",
		"clean_session":         s.opts.CleanSession,
		"keep_alive":            s.opts.KeepAlive.Seconds(),
		"read_only":             s.opts.ReadOnly,
		"observe_filter":        observeFilterLabel(s),
		"topics":                stats.Topics,
		"retained_topics":       stats.Retained,
		"messages_observed":     stats.Messages,
		"messages_buffered":     stats.Buffered,
		"topics_dropped":        stats.Dropped,
		"topic_limit":           s.opts.TopicLimit,
		"sys_version":           s.sysValue("version"),
		"sys_uptime":            s.sysValue("uptime"),
		"sys_clients_connected": s.sysNumber("clients/connected"),
		"sys_clients_total":     s.sysNumber("clients/total"),
		"sys_subscriptions":     s.sysNumber("subscriptions/count"),
		"sys_messages_received": s.sysNumber("messages/received"),
		"sys_messages_sent":     s.sysNumber("messages/sent"),
		"sys_bytes_received":    s.sysNumber("bytes/received"),
		"sys_bytes_sent":        s.sysNumber("bytes/sent"),
		"emqx_url":              s.opts.EMQXURL,
		"emqx_enabled":          s.emqx != nil,
	}, nil
}

func observeFilterLabel(s *Session) string {
	if !s.opts.Observe {
		return ""
	}
	return s.opts.ObserveFilter
}

func listSys(rc *plugin.RequestContext) (any, error) {
	s, err := mqttSession(rc)
	if err != nil {
		return nil, err
	}
	// No resource ref: $SYS topics are cached separately, so a row that claimed to
	// be a topic resource would open a detail view the topic route cannot serve.
	return broker.PageRows(rc, topicRows(s.snapshotSys(), false))
}

func listTopics(rc *plugin.RequestContext) (any, error) {
	s, err := mqttSession(rc)
	if err != nil {
		return nil, err
	}
	return broker.PageRows(rc, topicRows(s.snapshotTopics(), true))
}

func listRetained(rc *plugin.RequestContext) (any, error) {
	s, err := mqttSession(rc)
	if err != nil {
		return nil, err
	}
	states := s.snapshotTopics()
	retained := make([]topicState, 0, len(states))
	for _, state := range states {
		if state.Retained {
			retained = append(retained, state)
		}
	}
	return broker.PageRows(rc, topicRows(retained, true))
}

func topicRows(states []topicState, resourceRef bool) []row {
	rows := make([]row, 0, len(states))
	for _, state := range states {
		item := row{
			"topic":      state.Topic,
			"payload":    state.Payload,
			"encoding":   state.Encoding,
			"size":       state.Size,
			"qos":        strconv.Itoa(int(state.QoS)),
			"retained":   state.Retained,
			"messages":   state.Count,
			"first_seen": state.FirstSeen,
			"last_seen":  state.LastSeen,
		}
		if resourceRef {
			item["ref"] = plugin.ResourceIdentity{Kind: "topic", Name: state.Topic, UID: state.Topic}
		}
		rows = append(rows, item)
	}
	return rows
}

// treeTopics expands one level of the observed topic space, so a broker with a
// large topic space still expands one bounded, pageable level at a time.
func treeTopics(rc *plugin.RequestContext) (any, error) {
	s, err := mqttSession(rc)
	if err != nil {
		return nil, err
	}
	prefix := paramValue(rc, "prefix")
	base := ""
	if prefix != "" {
		base = prefix + "/"
	}
	type child struct {
		full     string
		topic    bool
		branch   bool
		messages uint64
	}
	children := map[string]*child{}
	for _, state := range s.snapshotTopics() {
		if !strings.HasPrefix(state.Topic, base) {
			continue
		}
		rest := state.Topic[len(base):]
		if rest == "" {
			continue
		}
		segment, _, more := strings.Cut(rest, "/")
		entry, ok := children[segment]
		if !ok {
			entry = &child{full: base + segment}
			children[segment] = entry
		}
		if more {
			entry.branch = true
		} else {
			entry.topic = true
			entry.messages = state.Count
		}
	}
	rows := make([]row, 0, len(children))
	for segment, entry := range children {
		rows = append(rows, row{"segment": segment, "topic": entry.full, "leaf": !entry.branch, "isTopic": entry.topic, "messages": entry.messages})
	}
	slices.SortFunc(rows, func(a, b row) int {
		return strings.Compare(fmt.Sprint(a["segment"]), fmt.Sprint(b["segment"]))
	})
	page, err := broker.PageRows(rc, rows)
	if err != nil {
		return nil, err
	}
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		full := fmt.Sprint(item["topic"])
		node := plugin.TreeNode{
			Key:   "topic:" + full,
			Label: fmt.Sprint(item["segment"]),
			Icon:  icon("hash"),
			Leaf:  item["leaf"] == true,
			Data:  map[string]any{"topic": full, "messages": item["messages"]},
		}
		if item["isTopic"] == true {
			node.Ref = &plugin.ResourceIdentity{Kind: "topic", Name: full, UID: full}
		}
		if item["leaf"] != true {
			node.Icon = icon("folder")
			node.ChildrenSource = &plugin.DataSource{RouteID: "mqtt.topics.tree", Params: map[string]string{"prefix": full}}
		}
		nodes = append(nodes, node)
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

func topicOverview(rc *plugin.RequestContext) (any, error) {
	s, err := mqttSession(rc)
	if err != nil {
		return nil, err
	}
	topic := paramValue(rc, "topic")
	if topic == "" {
		return nil, fmt.Errorf("%w: topic is required", plugin.ErrInvalidInput)
	}
	state, ok := s.topic(topic)
	if !ok {
		return nil, fmt.Errorf("%w: topic %q has not been observed on this connection", plugin.ErrNotFound, topic)
	}
	return row{
		"topic":      state.Topic,
		"payload":    state.Payload,
		"encoding":   state.Encoding,
		"size":       state.Size,
		"qos":        strconv.Itoa(int(state.QoS)),
		"retained":   state.Retained,
		"messages":   state.Count,
		"first_seen": state.FirstSeen,
		"last_seen":  state.LastSeen,
	}, nil
}

func listMessages(rc *plugin.RequestContext) (any, error) {
	s, err := mqttSession(rc)
	if err != nil {
		return nil, err
	}
	topic := paramValue(rc, "topic")
	records := s.snapshotMessages()
	rows := make([]row, 0, len(records))
	for _, record := range records {
		if topic != "" && record.Topic != topic {
			continue
		}
		rows = append(rows, row{
			"seq":      record.Seq,
			"time":     record.Time,
			"topic":    record.Topic,
			"qos":      strconv.Itoa(int(record.QoS)),
			"retained": record.Retained,
			"size":     record.Size,
			"payload":  record.Payload,
			"encoding": record.Encoding,
		})
	}
	return broker.PageRows(rc, rows)
}

func clearMessages(rc *plugin.RequestContext) (any, error) {
	s, err := mqttSession(rc)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.messages = nil
	s.mu.Unlock()
	return actionResult{OK: true}, nil
}

func rescanRetained(rc *plugin.RequestContext) (any, error) {
	s, err := mqttSession(rc)
	if err != nil {
		return nil, err
	}
	if err := s.resetObservation(rc.Ctx); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func clearRetained(rc *plugin.RequestContext) (any, error) {
	s, err := mqttSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	topic := paramValue(rc, "topic")
	if err := validateTopic(topic); err != nil {
		return nil, err
	}
	// A zero-length retained publish is the protocol's only way to delete a
	// retained message.
	if err := s.publish(rc.Ctx, topic, 0, true, nil); err != nil {
		return nil, err
	}
	s.markRetained(topic, false)
	return actionResult{OK: true}, nil
}

func publishMessage(rc *plugin.RequestContext) (any, error) {
	s, err := mqttSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Topic    string `json:"topic"`
		Payload  string `json:"payload"`
		Encoding string `json:"encoding"`
		QoS      string `json:"qos"`
		Retain   bool   `json:"retain"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	topic := strings.TrimSpace(req.Topic)
	if topic == "" {
		topic = paramValue(rc, "topic")
	}
	if err := validateTopic(topic); err != nil {
		return nil, err
	}
	payload, err := decodePayload(req.Payload, req.Encoding)
	if err != nil {
		return nil, err
	}
	qos, err := parseQoS(req.QoS, 0)
	if err != nil {
		return nil, err
	}
	if err := s.publish(rc.Ctx, topic, qos, req.Retain, payload); err != nil {
		return nil, err
	}
	if req.Retain {
		s.markRetained(topic, true)
	}
	return row{"ok": true, "topic": topic, "qos": qos, "retain": req.Retain, "size": len(payload)}, nil
}

func decodePayload(payload, encoding string) ([]byte, error) {
	switch encoding {
	case "", "string":
		return []byte(payload), nil
	case "base64":
		data, err := base64.StdEncoding.DecodeString(strings.TrimSpace(payload))
		if err != nil {
			return nil, fmt.Errorf("%w: payload is not valid base64", plugin.ErrInvalidInput)
		}
		return data, nil
	default:
		return nil, fmt.Errorf("%w: unsupported payload encoding %q", plugin.ErrInvalidInput, encoding)
	}
}

// filterOptions offers the observed topic space as ready-made subscription
// filters, so the live panel's selector reflects what this broker publishes.
func filterOptions(rc *plugin.RequestContext) (any, error) {
	s, err := mqttSession(rc)
	if err != nil {
		return nil, err
	}
	options := []plugin.Option{{Label: "All topics (#)", Value: "#"}}
	seen := map[string]bool{"#": true}
	if s.opts.Observe && s.opts.ObserveFilter != "#" {
		options = append(options, plugin.Option{Label: "Observed (" + s.opts.ObserveFilter + ")", Value: s.opts.ObserveFilter})
		seen[s.opts.ObserveFilter] = true
	}
	roots := map[string]bool{}
	for _, state := range s.snapshotTopics() {
		root, _, more := strings.Cut(state.Topic, "/")
		if root == "" || !more {
			continue
		}
		roots[root+"/#"] = true
	}
	// Bounded so a broker with thousands of roots cannot turn a selector into an
	// unbounded payload.
	const maxFilterOptions = 50
	for _, filter := range slices.Sorted(maps.Keys(roots)) {
		if len(options) >= maxFilterOptions {
			break
		}
		if seen[filter] {
			continue
		}
		options = append(options, plugin.Option{Label: filter, Value: filter})
	}
	if s.opts.SysMetrics {
		options = append(options, plugin.Option{Label: "Broker metrics ($SYS/#)", Value: sysFilter})
	}
	return options, nil
}

func qosOptions(*plugin.RequestContext) (any, error) {
	return qosOptionList(), nil
}

// subscribeStream tails one subscription. It opens its own broker connection:
// paho keys message routes by filter, so subscribing on the session client would
// replace the observation callback that feeds the topic tree.
func subscribeStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := mqttSession(rc)
	if err != nil {
		return err
	}
	// A topic-scoped panel supplies "topic"; the connection-level panel's
	// selector supplies "filter".
	filter := paramValue(rc, "filter")
	if filter == "" {
		filter = paramValue(rc, "topic")
	}
	if filter == "" {
		filter = defaultObserveFilter
	}
	if err := validateFilter(filter); err != nil {
		return err
	}
	qos, err := parseQoS(paramValue(rc, "qos"), 0)
	if err != nil {
		return err
	}
	ctx := client.Context()
	// Buffered so a slow browser cannot block the paho router; the oldest frame
	// is dropped instead, which is the right trade-off for a live tail.
	frames := make(chan messageRecord, 512)
	// The session runs with OrderMatters disabled, so paho dispatches every
	// delivery in its own goroutine and the counter has to be atomic.
	var seq atomic.Uint64
	subscriber, err := s.subscriber(ctx, filter, qos, func(_ mqttclient.Client, msg mqttclient.Message) {
		payload, encoding := preview(msg.Payload())
		record := messageRecord{
			Seq: seq.Add(1), Topic: msg.Topic(), Payload: payload, Encoding: encoding,
			Size: len(msg.Payload()), QoS: msg.Qos(), Retained: msg.Retained(), Time: time.Now().UTC(),
		}
		offer(frames, record)
	})
	if err != nil {
		return err
	}
	defer subscriber.Disconnect(250)

	if err := writeLogFrame(client, "subscribed to "+filter+" at QoS "+strconv.Itoa(int(qos))); err != nil {
		return streamErr(ctx, err)
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case record := <-frames:
			if err := writeLogFrame(client, formatRecord(record)); err != nil {
				return streamErr(ctx, err)
			}
		}
	}
}

// offer never blocks the paho router: a browser that cannot keep up loses the
// oldest frame rather than stalling every subscriber on the connection.
func offer(frames chan messageRecord, record messageRecord) {
	select {
	case frames <- record:
		return
	default:
	}
	select {
	case <-frames:
	default:
	}
	select {
	case frames <- record:
	default:
	}
}

func formatRecord(record messageRecord) string {
	flags := "qos" + strconv.Itoa(int(record.QoS))
	if record.Retained {
		flags += " retained"
	}
	if record.Encoding == "base64" {
		flags += " base64"
	}
	return fmt.Sprintf("[%s] %s (%d bytes) %s", flags, record.Topic, record.Size, record.Payload)
}

func writeLogFrame(w io.Writer, line string) error {
	frame, err := json.Marshal(struct {
		TS   string `json:"ts"`
		Line string `json:"line"`
	}{TS: time.Now().UTC().Format(time.RFC3339), Line: line})
	if err != nil {
		return err
	}
	_, err = w.Write(frame)
	return err
}

func streamErr(ctx context.Context, err error) error {
	if ctx.Err() != nil || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func listEMQXClients(rc *plugin.RequestContext) (any, error) {
	_, client, err := emqxSession(rc)
	if err != nil {
		return nil, err
	}
	page, err := emqxPage(rc, client, "/clients", nil, func(item row) row {
		clientID := fmt.Sprint(item["clientid"])
		item["ref"] = plugin.ResourceIdentity{Kind: "client", Name: clientID, UID: clientID}
		return item
	})
	if err != nil {
		return nil, err
	}
	return withEMQXNote(page, client), nil
}

func listEMQXSubscriptions(rc *plugin.RequestContext) (any, error) {
	_, client, err := emqxSession(rc)
	if err != nil {
		return nil, err
	}
	query := url.Values{}
	// The client scope belongs on every page of the walk, not only the first.
	if clientID := paramValue(rc, "clientid"); clientID != "" {
		query.Set("clientid", clientID)
	}
	page, err := emqxPage(rc, client, "/subscriptions", query, decorateSubscription)
	if err != nil {
		return nil, err
	}
	return withEMQXNote(page, client), nil
}

func listEMQXTopics(rc *plugin.RequestContext) (any, error) {
	_, client, err := emqxSession(rc)
	if err != nil {
		return nil, err
	}
	page, err := emqxPage(rc, client, "/topics", nil, nil)
	if err != nil {
		return nil, err
	}
	return withEMQXNote(page, client), nil
}

func listEMQXNodes(rc *plugin.RequestContext) (any, error) {
	_, client, err := emqxSession(rc)
	if err != nil {
		return nil, err
	}
	page, err := emqxPage(rc, client, "/nodes", nil, decorateNode)
	if err != nil {
		return nil, err
	}
	return withEMQXNote(page, client), nil
}

// decorateSubscription renders QoS as the string the badge severities are keyed
// by; EMQX sends it as a number.
func decorateSubscription(item row) row {
	if qos, ok := numberValue(item["qos"]); ok {
		item["qos"] = strconv.Itoa(int(qos))
	}
	return item
}

// decorateNode converts the node uptime EMQX reports in milliseconds to the
// seconds a duration column renders.
func decorateNode(item row) row {
	if ms, ok := numberValue(item["uptime"]); ok {
		item["uptime"] = ms / 1000
	}
	return item
}

func numberValue(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case json.Number:
		number, err := v.Float64()
		return number, err == nil
	}
	return 0, false
}

func emqxClientOverview(rc *plugin.RequestContext) (any, error) {
	_, client, err := requireEMQX(rc)
	if err != nil {
		return nil, err
	}
	clientID := paramValue(rc, "clientid")
	if clientID == "" {
		return nil, fmt.Errorf("%w: client id is required", plugin.ErrInvalidInput)
	}
	var out row
	if err := client.do(rc.Ctx, http.MethodGet, "/clients/"+url.PathEscape(clientID), nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func kickEMQXClient(rc *plugin.RequestContext) (any, error) {
	s, client, err := requireEMQX(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	clientID := paramValue(rc, "clientid")
	if clientID == "" {
		return nil, fmt.Errorf("%w: client id is required", plugin.ErrInvalidInput)
	}
	if err := client.do(rc.Ctx, http.MethodDelete, "/clients/"+url.PathEscape(clientID), nil, nil); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func requireEMQX(rc *plugin.RequestContext) (*Session, *emqxClient, error) {
	s, client, err := emqxSession(rc)
	if err != nil {
		return nil, nil, err
	}
	if client == nil {
		return nil, nil, fmt.Errorf("%w: the EMQX management API is not configured for this connection", plugin.ErrNotSupported)
	}
	return s, client, nil
}

func withEMQXNote(page scanPage, client *emqxClient) scanPage {
	if client == nil {
		page.Note = "The EMQX management API is not configured for this connection."
	}
	return page
}
