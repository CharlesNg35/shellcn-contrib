package mqtt

import (
	"context"
	"encoding/base64"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	mqttclient "github.com/eclipse/paho.mqtt.golang"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

// payloadPreview caps the payload bytes held per topic and per buffered message;
// a broker can push megabyte payloads at line rate.
const payloadPreview = 2048

type topicState struct {
	Topic     string
	Payload   string
	Encoding  string
	Size      int
	QoS       byte
	Retained  bool
	Count     uint64
	FirstSeen time.Time
	LastSeen  time.Time
}

type messageRecord struct {
	Seq      uint64
	Topic    string
	Payload  string
	Encoding string
	Size     int
	QoS      byte
	Retained bool
	Time     time.Time
}

type Session struct {
	client    mqttclient.Client
	transport plugin.NetTransport
	opts      options
	emqx      *emqxClient

	// ready carries the result of the first observation subscribe so connect can
	// wait for it; paho runs the connect handler on its own goroutine.
	ready     chan error
	readyOnce sync.Once

	mu       sync.RWMutex
	topics   map[string]*topicState
	sys      map[string]*topicState
	messages []messageRecord
	seq      uint64
	// dropped counts distinct topics refused after TopicLimit so the UI can say
	// the cache is capped instead of silently under-reporting.
	dropped uint64
}

func connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	opts, err := parseOptions(cfg)
	if err != nil {
		return nil, err
	}
	s := &Session{
		transport: cfg.Net,
		opts:      opts,
		ready:     make(chan error, 1),
		topics:    map[string]*topicState{},
		sys:       map[string]*topicState{},
	}
	if opts.EMQXURL != "" {
		httpClient, err := emqxHTTPClient(cfg, opts)
		if err != nil {
			return nil, err
		}
		s.emqx = &emqxClient{http: httpClient, base: opts.EMQXURL, key: opts.EMQXKey, secret: opts.EMQXSecret}
	}
	client := mqttclient.NewClient(clientOptions(cfg.Net, opts, opts.ClientID, s.subscribeObservers))
	if err := waitToken(ctx, client.Connect(), opts.Timeout); err != nil {
		return nil, fmt.Errorf("%w: connect to %s: %v", plugin.ErrUnavailable, opts.Address, err)
	}
	s.client = client
	// A session is only usable once its observation subscription exists: paho
	// runs the connect handler asynchronously, so returning at CONNACK would let
	// everything published between CONNACK and SUBACK go unobserved.
	if err := awaitReady(ctx, s.ready, opts.Timeout); err != nil {
		client.Disconnect(0)
		return nil, err
	}
	return s, nil
}

// awaitReady waits for a connect handler to report its subscribe result.
func awaitReady(ctx context.Context, ready <-chan error, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-ready:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("%w: timed out establishing the subscription", plugin.ErrUnavailable)
	}
}

// subscribeObservers runs on every (re)connect because a broker that dropped the
// session also dropped its subscriptions.
func (s *Session) subscribeObservers(client mqttclient.Client) {
	err := s.subscribeAll(client)
	s.readyOnce.Do(func() { s.ready <- err })
}

func (s *Session) subscribeAll(client mqttclient.Client) error {
	if s.opts.Observe {
		err := subscribe(context.Background(), client, s.opts.ObserveFilter, 0, s.opts.Timeout, func(_ mqttclient.Client, msg mqttclient.Message) {
			s.record(msg)
		})
		if err != nil {
			return fmt.Errorf("%w; narrow the observe filter or turn observation off on the connection", err)
		}
	}
	if s.opts.SysMetrics {
		// $SYS is a secondary feed that broker ACLs commonly deny (EMQX denies it
		// by default); a refusal leaves the metrics empty rather than failing the
		// whole connection.
		_ = subscribe(context.Background(), client, sysFilter, 0, s.opts.Timeout, func(_ mqttclient.Client, msg mqttclient.Message) {
			s.recordSys(msg)
		})
	}
	return nil
}

// subackFailure is the MQTT 3.1.1 SUBACK return code for a refused filter.
const subackFailure = 0x80

// subscribe reports a refused subscription as an error. paho puts the per-filter
// SUBACK return code in the token result and leaves Error() nil, so a filter the
// broker's ACL denied otherwise looks like a subscription that succeeded and the
// panels behind it stay silently empty.
func subscribe(ctx context.Context, client mqttclient.Client, filter string, qos byte, timeout time.Duration, handler mqttclient.MessageHandler) error {
	token := client.Subscribe(filter, qos, handler)
	if err := waitToken(ctx, token, timeout); err != nil {
		return fmt.Errorf("%w: subscribe to %s: %v", plugin.ErrUnavailable, filter, err)
	}
	sub, ok := token.(*mqttclient.SubscribeToken)
	if !ok {
		return nil
	}
	return subackError(sub.Result())
}

func subackError(result map[string]byte) error {
	for filter, code := range result {
		if code == subackFailure {
			return fmt.Errorf("%w: the broker refused the subscription to %q", plugin.ErrForbidden, filter)
		}
	}
	return nil
}

func (s *Session) HealthCheck(context.Context) error {
	if s.client == nil || !s.client.IsConnected() {
		return fmt.Errorf("%w: not connected to %s", plugin.ErrUnavailable, s.opts.Address)
	}
	return nil
}

func (s *Session) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}

func (s *Session) Close() error {
	if s.client != nil {
		s.client.Disconnect(250)
	}
	return nil
}

func (s *Session) record(msg mqttclient.Message) {
	// $SYS has its own cache. The broker keeps it out of a root wildcard, but
	// paho's local router does not, so a session that also watches $SYS/# would
	// otherwise mirror every broker metric into the application topic space.
	if strings.HasPrefix(msg.Topic(), "$") {
		return
	}
	payload, encoding := preview(msg.Payload())
	now := time.Now().UTC()

	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.topics[msg.Topic()]
	if !ok {
		if len(s.topics) >= s.opts.TopicLimit {
			s.dropped++
		} else {
			state = &topicState{Topic: msg.Topic(), FirstSeen: now}
			s.topics[msg.Topic()] = state
		}
	}
	if state != nil {
		state.Payload, state.Encoding, state.Size = payload, encoding, len(msg.Payload())
		state.QoS, state.LastSeen = msg.Qos(), now
		// The retain flag is only set on the copy the broker replays to a new
		// subscription; a later live publish does not un-retain the topic, so the
		// flag is sticky until the retained message is actually removed.
		state.Retained = state.Retained || msg.Retained()
		state.Count++
	}
	s.seq++
	s.messages = append(s.messages, messageRecord{
		Seq: s.seq, Topic: msg.Topic(), Payload: payload, Encoding: encoding,
		Size: len(msg.Payload()), QoS: msg.Qos(), Retained: msg.Retained(), Time: now,
	})
	if extra := len(s.messages) - s.opts.MessageLimit; extra > 0 {
		s.messages = slices.Delete(s.messages, 0, extra)
	}
}

func (s *Session) recordSys(msg mqttclient.Message) {
	payload, encoding := preview(msg.Payload())
	now := time.Now().UTC()

	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.sys[msg.Topic()]
	if !ok {
		if len(s.sys) >= sysLimit {
			return
		}
		state = &topicState{Topic: msg.Topic(), FirstSeen: now}
		s.sys[msg.Topic()] = state
	}
	state.Payload, state.Encoding, state.Size = payload, encoding, len(msg.Payload())
	state.QoS, state.Retained, state.LastSeen = msg.Qos(), msg.Retained(), now
	state.Count++
}

// snapshotTopics copies the observation cache: callers sort and page the result,
// so they must never see the map's own values.
func (s *Session) snapshotTopics() []topicState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]topicState, 0, len(s.topics))
	for _, state := range s.topics {
		out = append(out, *state)
	}
	return out
}

func (s *Session) snapshotSys() []topicState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]topicState, 0, len(s.sys))
	for _, state := range s.sys {
		out = append(out, *state)
	}
	return out
}

func (s *Session) snapshotMessages() []messageRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Clone(s.messages)
}

// markRetained records what this session just told the broker to store: live
// deliveries never carry the retain flag, so a publish or a clear is the only
// moment the cache learns about the change without re-subscribing.
func (s *Session) markRetained(topic string, retained bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.topics[topic]
	if !ok {
		if len(s.topics) >= s.opts.TopicLimit {
			s.dropped++
			return
		}
		state = &topicState{Topic: topic, FirstSeen: time.Now().UTC()}
		s.topics[topic] = state
	}
	state.Retained = retained
}

func (s *Session) topic(name string) (topicState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	state, ok := s.topics[name]
	if !ok {
		return topicState{}, false
	}
	return *state, true
}

type observationStats struct {
	Topics   int
	Retained int
	Messages uint64
	Dropped  uint64
	Buffered int
}

func (s *Session) stats() observationStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := observationStats{Topics: len(s.topics), Messages: s.seq, Dropped: s.dropped, Buffered: len(s.messages)}
	for _, state := range s.topics {
		if state.Retained {
			out.Retained++
		}
	}
	return out
}

// sysValue reads a $SYS metric by topic suffix, because brokers disagree on the
// prefix: mosquitto publishes $SYS/broker/uptime and EMQX $SYS/brokers/<node>/uptime.
func (s *Session) sysValue(suffix string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, topic := range slices.Sorted(maps.Keys(s.sys)) {
		if strings.HasSuffix(topic, "/"+suffix) {
			return s.sys[topic].Payload
		}
	}
	return ""
}

func (s *Session) sysNumber(suffix string) any {
	raw := strings.TrimSpace(s.sysValue(suffix))
	if raw == "" {
		return nil
	}
	if value, err := strconv.ParseFloat(raw, 64); err == nil {
		return value
	}
	return raw
}

// resetObservation drops the cache and re-subscribes, which makes the broker
// replay its retained set — the only way MQTT offers to re-read retained
// messages on a live connection.
func (s *Session) resetObservation(ctx context.Context) error {
	s.mu.Lock()
	s.topics = map[string]*topicState{}
	s.messages = nil
	s.dropped = 0
	s.mu.Unlock()

	if s.client == nil || !s.client.IsConnected() {
		return fmt.Errorf("%w: not connected to %s", plugin.ErrUnavailable, s.opts.Address)
	}
	if !s.opts.Observe {
		return nil
	}
	if err := waitToken(ctx, s.client.Unsubscribe(s.opts.ObserveFilter), s.opts.Timeout); err != nil {
		return fmt.Errorf("%w: unsubscribe %s: %v", plugin.ErrUnavailable, s.opts.ObserveFilter, err)
	}
	return subscribe(ctx, s.client, s.opts.ObserveFilter, 0, s.opts.Timeout, func(_ mqttclient.Client, msg mqttclient.Message) {
		s.record(msg)
	})
}

func (s *Session) publish(ctx context.Context, topic string, qos byte, retained bool, payload []byte) error {
	if s.client == nil || !s.client.IsConnected() {
		return fmt.Errorf("%w: not connected to %s", plugin.ErrUnavailable, s.opts.Address)
	}
	if err := waitToken(ctx, s.client.Publish(topic, qos, retained, payload), s.opts.Timeout); err != nil {
		return fmt.Errorf("%w: publish to %s: %v", plugin.ErrUnavailable, topic, err)
	}
	return nil
}

// subscriber opens a second connection for one live stream. Sharing the session
// client would be wrong: paho keys routes by filter, so a browser subscription
// to the observer's filter would replace the observer's own callback.
func (s *Session) subscriber(ctx context.Context, filter string, qos byte, handler mqttclient.MessageHandler) (mqttclient.Client, error) {
	if s.transport == nil {
		return nil, fmt.Errorf("%w: session has no transport", plugin.ErrUnavailable)
	}
	clientID := streamClientID(s.opts.ClientID)
	ready := make(chan error, 1)
	var once sync.Once
	opts := clientOptions(s.transport, s.opts, clientID, func(client mqttclient.Client) {
		err := subscribe(context.Background(), client, filter, qos, s.opts.Timeout, handler)
		once.Do(func() { ready <- err })
	})
	// A stream is transient state; a persistent session would leave queued
	// messages on the broker for a client ID that never returns.
	opts.SetCleanSession(true)
	client := mqttclient.NewClient(opts)
	if err := waitToken(ctx, client.Connect(), s.opts.Timeout); err != nil {
		return nil, fmt.Errorf("%w: subscribe to %s: %v", plugin.ErrUnavailable, filter, err)
	}
	// The tail is only live once the broker has acknowledged the subscription.
	if err := awaitReady(ctx, ready, s.opts.Timeout); err != nil {
		client.Disconnect(0)
		return nil, err
	}
	return client, nil
}

func streamClientID(base string) string { return uniqueClientID(base, "-sub-") }

// sessionClientID keeps an explicit client ID exactly as configured but makes the
// shipped default unique per session. Two connections sharing one ID fight over
// the broker's takeover rule, and autoreconnect turns that into a loop.
func sessionClientID(configured string) string {
	trimmed := strings.TrimSpace(configured)
	if trimmed != "" && trimmed != plugin.DefaultClientName {
		return trimmed
	}
	return uniqueClientID(plugin.DefaultClientName, "-")
}

func uniqueClientID(base, marker string) string {
	suffix := marker + strconv.FormatInt(time.Now().UnixNano(), 36)
	// MQTT 3.1.1 brokers may reject client IDs longer than 23 bytes, and a cut
	// that splits a rune would leave an id that is not valid UTF-8.
	if len(base)+len(suffix) > 23 {
		limit := max(0, 23-len(suffix))
		for limit > 0 && !utf8.ValidString(base[:limit]) {
			limit--
		}
		base = base[:limit]
	}
	return base + suffix
}

func waitToken(ctx context.Context, token mqttclient.Token, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-token.Done():
		return token.Error()
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return context.DeadlineExceeded
	}
}

// preview renders a payload for the browser: printable UTF-8 stays text,
// anything else becomes base64 so binary payloads never corrupt a JSON frame.
func preview(payload []byte) (string, string) {
	if len(payload) == 0 {
		return "", "string"
	}
	clipped, truncated := clipPayload(payload)
	if utf8.Valid(clipped) && !strings.ContainsRune(string(clipped), 0) {
		text := string(clipped)
		if truncated {
			text += "…"
		}
		return text, "string"
	}
	encoded := base64.StdEncoding.EncodeToString(clipped)
	if truncated {
		encoded += "…"
	}
	return encoded, "base64"
}

// clipPayload cuts a payload to the preview budget without leaving a UTF-8 rune
// split across the cut, which would otherwise push a large text payload into the
// base64 branch. Bytes that stay invalid however far back the cut moves are
// binary and keep the full budget.
func clipPayload(payload []byte) ([]byte, bool) {
	if len(payload) <= payloadPreview {
		return payload, false
	}
	clipped := payload[:payloadPreview]
	if utf8.Valid(clipped) {
		return clipped, true
	}
	for back := 1; back < utf8.UTFMax && back < len(clipped); back++ {
		if utf8.Valid(clipped[:len(clipped)-back]) {
			return clipped[:len(clipped)-back], true
		}
	}
	return clipped, true
}

func unwrap(sess plugin.Session) (*Session, error) {
	if s, ok := sess.(*Session); ok {
		return s, nil
	}
	// rc.Session is the core's borrowed Handle, which exposes the live session.
	if h, ok := sess.(interface{ Session() plugin.Session }); ok {
		if s, ok := h.Session().(*Session); ok {
			return s, nil
		}
	}
	return nil, plugin.ErrInvalidInput
}

func ensureWritable(s *Session) error {
	if s.opts.ReadOnly {
		return fmt.Errorf("%w: connection is read-only", plugin.ErrForbidden)
	}
	return nil
}
