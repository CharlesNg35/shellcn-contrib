package mqtt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	mqttclient "github.com/eclipse/paho.mqtt.golang"

	"github.com/charlesng35/shellcn-contrib/shared/plugintest"
	"github.com/charlesng35/shellcn/sdk/plugin"
	sdktest "github.com/charlesng35/shellcn/sdk/plugintest"
)

type testMessage struct {
	topic    string
	payload  []byte
	qos      byte
	retained bool
}

func (m testMessage) Duplicate() bool   { return false }
func (m testMessage) Qos() byte         { return m.qos }
func (m testMessage) Retained() bool    { return m.retained }
func (m testMessage) Topic() string     { return m.topic }
func (m testMessage) MessageID() uint16 { return 0 }
func (m testMessage) Payload() []byte   { return m.payload }
func (m testMessage) Ack()              {}

var _ mqttclient.Message = testMessage{}

func testSession(opts options) *Session {
	if opts.TopicLimit == 0 {
		opts.TopicLimit = defaultTopicLimit
	}
	if opts.MessageLimit == 0 {
		opts.MessageLimit = defaultMessageLimit
	}
	if opts.Timeout == 0 {
		opts.Timeout = defaultTimeout
	}
	if opts.ObserveFilter == "" {
		opts.ObserveFilter = defaultObserveFilter
	}
	if opts.Address == "" {
		opts.Address = "localhost:1883"
	}
	return &Session{opts: opts, topics: map[string]*topicState{}, sys: map[string]*topicState{}}
}

func rc(t *testing.T, sess plugin.Session, params map[string]string, query url.Values, body []byte) *plugin.RequestContext {
	t.Helper()
	return plugin.NewRequestContext(context.Background(), plugin.User{ID: "u1"}, sess, params, query, body)
}

func TestMQTTManifestValidates(t *testing.T) {
	p := New()
	proj := sdktest.Projection(t, p)
	if proj.Category.Key != plugin.CategoryMessaging {
		t.Fatalf("category: got %q want %q", proj.Category.Key, plugin.CategoryMessaging)
	}
	if proj.Layout != plugin.LayoutSidebarTree {
		t.Fatalf("layout: got %q", proj.Layout)
	}
	if len(proj.Resources) != 2 {
		t.Fatalf("resources: got %d want 2", len(proj.Resources))
	}
}

func TestMQTTDeclaresLogStreamForSubscriptions(t *testing.T) {
	m := New().Manifest()
	if len(m.Streams) != 1 || m.Streams[0].Kind != plugin.StreamLogs || m.Streams[0].RouteID != "mqtt.subscribe" {
		t.Fatalf("streams: %#v", m.Streams)
	}
	var live *plugin.Panel
	for i, tab := range m.Tabs {
		if tab.Key == "live" {
			live = &m.Tabs[i]
		}
	}
	if live == nil || live.Type != plugin.PanelLogStream {
		t.Fatalf("connection tabs are missing the live log stream: %#v", m.Tabs)
	}
	if live.Source == nil || live.Source.Method != plugin.MethodWS {
		t.Fatalf("live panel must bind to the WS route: %#v", live.Source)
	}
	cfg, ok := live.Config.(plugin.LogStreamConfig)
	if !ok || len(cfg.Controls) != 2 {
		t.Fatalf("live panel should expose filter and qos controls: %#v", live.Config)
	}
}

// The topic detail's live tab is scoped by the topic param; a filter selector
// would silently replace that scope with its first option.
func TestTopicLivePanelHasNoFilterControl(t *testing.T) {
	panel := topicLivePanel()
	cfg := panel.Config.(plugin.LogStreamConfig)
	for _, control := range cfg.Controls {
		if control.Param == "filter" {
			t.Fatalf("topic live panel must not re-parameterize the filter")
		}
	}
	if panel.Source.Params["topic"] != "${resource.name}" {
		t.Fatalf("topic live panel must carry the topic scope: %#v", panel.Source.Params)
	}
}

func TestMQTTConfigSchemaIsSpecific(t *testing.T) {
	m := New().Manifest()
	fields := fieldMap(m.Config)
	for _, key := range []string{"host", "port", "client_id", "clean_session", "keep_alive", "auth", "username", "password", basicCredentialField, "tls_mode", "observe", "observe_filter", "emqx_url", emqxCredentialField} {
		if !fields[key] {
			t.Fatalf("missing field %q", key)
		}
	}
	for _, key := range []string{"urls", "management_url", "brokers"} {
		if fields[key] {
			t.Fatalf("mqtt should not expose %q", key)
		}
	}
}

// Secret material must never be a plain config field the projection can echo.
func TestMQTTSecretFieldsAreMarkedSecret(t *testing.T) {
	m := New().Manifest()
	secrets := map[string]bool{"password": true, "emqx_api_secret": true, "ca_certificate": true}
	for _, group := range m.Config.Groups {
		for _, field := range group.Fields {
			if secrets[field.Key] && !field.Secret {
				t.Fatalf("field %q must be marked secret", field.Key)
			}
		}
	}
	if !sdktest.CredentialKindSupported(m.Config, plugin.CredentialKindBasicAuth) {
		t.Fatal("broker auth should accept a stored basic-auth credential")
	}
	if !sdktest.CredentialKindSupported(m.Config, plugin.CredentialKindAPIToken) {
		t.Fatal("EMQX auth should accept a stored API token credential")
	}
}

func fieldMap(schema plugin.Schema) map[string]bool {
	out := map[string]bool{}
	for _, group := range schema.Groups {
		for _, field := range group.Fields {
			out[field.Key] = true
		}
	}
	return out
}

func TestParseOptionsBuildsBrokerURLAndCredentials(t *testing.T) {
	opts, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{
		"host": "broker.internal", "port": float64(8883), "auth": "password",
		"username": "sensor", "password": "s3cret", "tls_mode": "verify-full",
		"client_id": "shellcn-mqtt", "observe_filter": "sensors/#", "read_only": false,
	}})
	if err != nil {
		t.Fatalf("parseOptions: %v", err)
	}
	if opts.brokerURL() != "ssl://broker.internal:8883" {
		t.Fatalf("broker URL: got %q", opts.brokerURL())
	}
	if opts.TLSConfig == nil || opts.TLSConfig.ServerName != "broker.internal" {
		t.Fatalf("verify-full must pin the server name: %#v", opts.TLSConfig)
	}
	if opts.Username != "sensor" || opts.Password != "s3cret" {
		t.Fatalf("credentials not applied: %#v", opts)
	}
	if opts.ReadOnly {
		t.Fatal("read_only=false must disable read-only mode")
	}
}

// tls_mode=require disables verification for the broker socket. The EMQX API key
// rides the management requests as basic auth, so that relaxation must not
// silently follow the connection to a different HTTPS host.
func TestEMQXTLSDoesNotInheritTheBrokerRelaxation(t *testing.T) {
	cfg := plugin.ConnectConfig{Config: map[string]any{
		"host": "broker.internal", "port": float64(8883), "tls_mode": "require",
		"emqx_url": "https://dashboard.internal:18083",
	}}
	opts, err := parseOptions(cfg)
	if err != nil {
		t.Fatalf("parseOptions: %v", err)
	}
	if opts.TLSConfig == nil || !opts.TLSConfig.InsecureSkipVerify {
		t.Fatalf("the broker config should be the relaxed one: %#v", opts.TLSConfig)
	}
	remote, err := emqxTLSConfig(cfg, opts)
	if err != nil {
		t.Fatalf("emqxTLSConfig: %v", err)
	}
	if remote.InsecureSkipVerify {
		t.Fatal("a dashboard on another host must still be verified")
	}
	if remote.ServerName != "" {
		t.Fatalf("the broker's ServerName pin must not leak to the dashboard: %q", remote.ServerName)
	}

	sameCfg := plugin.ConnectConfig{Config: map[string]any{
		"host": "broker.internal", "port": float64(8883), "tls_mode": "require",
		"emqx_url": "https://broker.internal:18083",
	}}
	sameOpts, err := parseOptions(sameCfg)
	if err != nil {
		t.Fatalf("parseOptions: %v", err)
	}
	sameRemote, err := emqxTLSConfig(sameCfg, sameOpts)
	if err != nil {
		t.Fatalf("emqxTLSConfig: %v", err)
	}
	if !sameRemote.InsecureSkipVerify {
		t.Fatal("the dashboard on the broker host should keep the trust the user already granted it")
	}
}

func TestParseOptionsRejectsBadInput(t *testing.T) {
	cases := []map[string]any{
		{"host": "  "},
		{"host": "localhost", "auth": "kerberos"},
		{"host": "localhost", "observe_filter": "a/#/b"},
		{"host": "localhost", "emqx_url": "not-a-url"},
		{"host": "localhost", "emqx_url": "http://localhost:18083", "emqx_auth": "jwt"},
	}
	for i, cfg := range cases {
		if _, err := parseOptions(plugin.ConnectConfig{Config: cfg}); err == nil {
			t.Fatalf("case %d: expected an error for %#v", i, cfg)
		}
	}
}

// Two sessions on one client ID fight over the broker's takeover rule, and
// autoreconnect turns that into a loop, so the shipped default cannot be shared.
func TestSessionClientIDIsUniquePerSessionOnlyByDefault(t *testing.T) {
	first, second := sessionClientID(plugin.DefaultClientName), sessionClientID("")
	if first == second {
		t.Fatalf("two default sessions took the same client id: %q", first)
	}
	for _, id := range []string{first, second} {
		if !strings.HasPrefix(id, plugin.DefaultClientName+"-") {
			t.Fatalf("a defaulted client id should stay recognisable: %q", id)
		}
		if len(id) > 23 {
			t.Fatalf("client id %q exceeds the 23-byte MQTT 3.1.1 limit", id)
		}
	}
	if got := sessionClientID(" gateway-north "); got != "gateway-north" {
		t.Fatalf("an explicit client id must be used verbatim: %q", got)
	}
	stream := streamClientID("gateway-north")
	if len(stream) > 23 || stream == "gateway-north" {
		t.Fatalf("stream client id: %q", stream)
	}
}

func TestValidateFilterAndTopic(t *testing.T) {
	for _, filter := range []string{"#", "+", "a/#", "a/+/b", "$SYS/#", "a/b"} {
		if err := validateFilter(filter); err != nil {
			t.Fatalf("validateFilter(%q): %v", filter, err)
		}
	}
	for _, filter := range []string{"", "a/#/b", "a#", "a/b+"} {
		if err := validateFilter(filter); err == nil {
			t.Fatalf("validateFilter(%q): expected an error", filter)
		}
	}
	if err := validateTopic("a/b/c"); err != nil {
		t.Fatalf("validateTopic: %v", err)
	}
	for _, topic := range []string{"", "  ", "a/#", "a/+/b"} {
		if err := validateTopic(topic); err == nil {
			t.Fatalf("validateTopic(%q): expected an error", topic)
		}
	}
}

// paho records the per-filter SUBACK return code in the token result and leaves
// Error() nil, so a filter an ACL denied looks like a subscription that
// succeeded. EMQX denies "#" and "$SYS/#" out of the box.
func TestSubackFailureIsAnError(t *testing.T) {
	if err := subackError(map[string]byte{"a/#": 0, "b/+": 1}); err != nil {
		t.Fatalf("granted subscriptions must not error: %v", err)
	}
	err := subackError(map[string]byte{"#": subackFailure})
	if !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("a refused filter must map to forbidden: %v", err)
	}
	if !strings.Contains(err.Error(), `"#"`) {
		t.Fatalf("the error should name the refused filter: %v", err)
	}
	if err := subackError(nil); err != nil {
		t.Fatalf("no result means nothing to report: %v", err)
	}
}

func TestParseQoS(t *testing.T) {
	for raw, want := range map[string]byte{"": 1, "0": 0, "1": 1, "2": 2, " 2 ": 2} {
		got, err := parseQoS(raw, 1)
		if err != nil || got != want {
			t.Fatalf("parseQoS(%q) = %d, %v; want %d", raw, got, err, want)
		}
	}
	for _, raw := range []string{"3", "-1", "high"} {
		if _, err := parseQoS(raw, 0); err == nil {
			t.Fatalf("parseQoS(%q): expected an error", raw)
		}
	}
}

func TestPreviewKeepsTextAndEncodesBinary(t *testing.T) {
	text, encoding := preview([]byte("hello"))
	if text != "hello" || encoding != "string" {
		t.Fatalf("text payload: %q %q", text, encoding)
	}
	binary, encoding := preview([]byte{0xff, 0xfe, 0x00})
	if encoding != "base64" || binary == "" {
		t.Fatalf("binary payload: %q %q", binary, encoding)
	}
	long, _ := preview([]byte(strings.Repeat("a", payloadPreview*2)))
	if len([]rune(long)) != payloadPreview+1 {
		t.Fatalf("long payload was not clipped to the preview budget: %d runes", len([]rune(long)))
	}
}

// A byte-sized clip lands in the middle of the last rune here; treating the
// remainder as binary would base64 an ordinary UTF-8 payload.
func TestPreviewClipsOnARuneBoundary(t *testing.T) {
	payload := append([]byte(strings.Repeat("a", payloadPreview-1)), []byte("€tail")...)
	text, encoding := preview(payload)
	if encoding != "string" {
		t.Fatalf("a multibyte payload split by the clip fell back to %q: %q", encoding, text)
	}
	if !strings.HasSuffix(text, "a…") {
		t.Fatalf("the split rune should be dropped, not kept in part: %q", text[len(text)-8:])
	}
}

func TestObservationCacheIsBounded(t *testing.T) {
	s := testSession(options{TopicLimit: 2, MessageLimit: 3})
	for i := range 10 {
		s.record(testMessage{topic: fmt.Sprintf("t/%d", i), payload: []byte("v")})
	}
	stats := s.stats()
	if stats.Topics != 2 {
		t.Fatalf("topics: got %d want 2 (the configured cap)", stats.Topics)
	}
	if stats.Dropped != 8 {
		t.Fatalf("dropped: got %d want 8", stats.Dropped)
	}
	if stats.Buffered != 3 {
		t.Fatalf("buffered messages: got %d want 3 (the configured cap)", stats.Buffered)
	}
	if stats.Messages != 10 {
		t.Fatalf("observed messages: got %d want 10", stats.Messages)
	}
	buffered := s.snapshotMessages()
	if buffered[0].Seq != 8 || buffered[2].Seq != 10 {
		t.Fatalf("the buffer should keep the newest records: %#v", buffered)
	}
}

// Handlers sort and page the snapshot, so it must never alias the cache.
func TestSnapshotsCloneCacheState(t *testing.T) {
	s := testSession(options{})
	s.record(testMessage{topic: "a", payload: []byte("1"), retained: true})
	snapshot := s.snapshotTopics()
	snapshot[0].Topic = "mutated"
	snapshot[0].Retained = false
	if state, _ := s.topic("a"); state.Topic != "a" || !state.Retained {
		t.Fatalf("mutating a snapshot changed the cache: %#v", state)
	}
	messages := s.snapshotMessages()
	messages[0].Topic = "mutated"
	if s.snapshotMessages()[0].Topic != "a" {
		t.Fatal("mutating a message snapshot changed the buffer")
	}
}

// paho's local router does not apply the broker's "$ topics are excluded from a
// root wildcard" rule, so a session watching both # and $SYS/# would mirror every
// broker metric into the application topic space.
func TestObservationCacheExcludesDollarTopics(t *testing.T) {
	s := testSession(options{})
	s.record(testMessage{topic: "$SYS/broker/uptime", payload: []byte("10"), retained: true})
	s.record(testMessage{topic: "app/a", payload: []byte("1")})
	topics := s.snapshotTopics()
	if len(topics) != 1 || topics[0].Topic != "app/a" {
		t.Fatalf("topic cache: %#v", topics)
	}
	if stats := s.stats(); stats.Messages != 1 {
		t.Fatalf("a $SYS delivery must not count as an observed message: %#v", stats)
	}
}

// The retain flag only rides the copy replayed to a new subscription, so a later
// live publish must not make a retained topic look un-retained.
func TestRetainedFlagIsSticky(t *testing.T) {
	s := testSession(options{})
	s.record(testMessage{topic: "a", payload: []byte("kept"), retained: true})
	s.record(testMessage{topic: "a", payload: []byte("live")})
	state, _ := s.topic("a")
	if !state.Retained {
		t.Fatal("a live delivery cleared the retained flag")
	}
	if state.Payload != "live" {
		t.Fatalf("the newest payload should win: %q", state.Payload)
	}
}

func TestMarkRetainedTracksWhatThisSessionStored(t *testing.T) {
	s := testSession(options{})
	s.markRetained("a/b", true)
	if state, ok := s.topic("a/b"); !ok || !state.Retained {
		t.Fatalf("publishing with retain should mark the topic: %#v", state)
	}
	s.markRetained("a/b", false)
	if state, _ := s.topic("a/b"); state.Retained {
		t.Fatal("clearing a retained message should unmark the topic")
	}
	capped := testSession(options{TopicLimit: 1})
	capped.record(testMessage{topic: "first", payload: []byte("1")})
	capped.markRetained("second", true)
	if _, ok := capped.topic("second"); ok {
		t.Fatal("markRetained must respect the topic cap")
	}
}

func TestSysValueMatchesBrokerPrefixes(t *testing.T) {
	s := testSession(options{})
	s.recordSys(testMessage{topic: "$SYS/broker/uptime", payload: []byte("1200 seconds")})
	s.recordSys(testMessage{topic: "$SYS/brokers/emqx@127.0.0.1/clients/connected", payload: []byte("7")})
	if got := s.sysValue("uptime"); got != "1200 seconds" {
		t.Fatalf("mosquitto-style $SYS lookup: %q", got)
	}
	if got := s.sysNumber("clients/connected"); got != float64(7) {
		t.Fatalf("emqx-style $SYS lookup: %v", got)
	}
	if got := s.sysValue("nope"); got != "" {
		t.Fatalf("missing metric should be empty, got %q", got)
	}
}

func TestListsSortAndFilterAcrossTheWholeDataset(t *testing.T) {
	s := testSession(options{})
	for _, topic := range []string{"b/1", "a/1", "c/1", "a/2"} {
		s.record(testMessage{topic: topic, payload: []byte(topic)})
	}
	// Page 2 of a descending sort must come from the sorted dataset, not from a
	// sorted slice of page 2.
	out, err := listTopics(rc(t, s, nil, url.Values{"sort": {"-topic"}, "limit": {"2"}}, nil))
	if err != nil {
		t.Fatalf("listTopics: %v", err)
	}
	first := out.(plugin.Page[row])
	if fmt.Sprint(first.Items[0]["topic"]) != "c/1" || fmt.Sprint(first.Items[1]["topic"]) != "b/1" {
		t.Fatalf("page 1 of -topic: %v", rowTopics(first.Items))
	}
	if first.NextCursor != "2" {
		t.Fatalf("next cursor: got %q want %q", first.NextCursor, "2")
	}
	out, err = listTopics(rc(t, s, nil, url.Values{"sort": {"-topic"}, "limit": {"2"}, "cursor": {first.NextCursor}}, nil))
	if err != nil {
		t.Fatalf("listTopics page 2: %v", err)
	}
	second := out.(plugin.Page[row])
	if fmt.Sprint(second.Items[0]["topic"]) != "a/2" || fmt.Sprint(second.Items[1]["topic"]) != "a/1" {
		t.Fatalf("page 2 of -topic: %v", rowTopics(second.Items))
	}
	if second.NextCursor != "" {
		t.Fatalf("last page should not advertise more rows, got cursor %q", second.NextCursor)
	}

	// The grid's search box filters the dataset, not the loaded page.
	out, err = listTopics(rc(t, s, nil, url.Values{"filter": {"a/"}, "limit": {"1"}}, nil))
	if err != nil {
		t.Fatalf("listTopics filtered: %v", err)
	}
	filtered := out.(plugin.Page[row])
	if filtered.Total == nil || *filtered.Total != 2 {
		t.Fatalf("filtered total: %v", filtered.Total)
	}
}

func rowTopics(rows []row) []string {
	out := make([]string, 0, len(rows))
	for _, item := range rows {
		out = append(out, fmt.Sprint(item["topic"]))
	}
	return out
}

func TestListsRejectAnInvalidCursor(t *testing.T) {
	s := testSession(options{})
	if _, err := listTopics(rc(t, s, nil, url.Values{"cursor": {"nope"}}, nil)); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("invalid cursor: got %v", err)
	}
}

// A row that advertises a topic ref opens the topic detail. $SYS metrics live in
// their own cache, so a ref on those rows would open a detail that cannot load.
func TestOnlyObservedTopicRowsCarryAResourceRef(t *testing.T) {
	s := testSession(options{})
	s.record(testMessage{topic: "app/a", payload: []byte("1")})
	s.recordSys(testMessage{topic: "$SYS/broker/uptime", payload: []byte("10")})

	topics, err := listTopics(rc(t, s, nil, nil, nil))
	if err != nil {
		t.Fatalf("listTopics: %v", err)
	}
	ref, ok := topics.(plugin.Page[row]).Items[0]["ref"].(plugin.ResourceIdentity)
	if !ok || ref.Kind != "topic" || ref.Name != "app/a" {
		t.Fatalf("topic rows must open the topic detail: %#v", topics.(plugin.Page[row]).Items[0]["ref"])
	}

	sys, err := listSys(rc(t, s, nil, nil, nil))
	if err != nil {
		t.Fatalf("listSys: %v", err)
	}
	if got := sys.(plugin.Page[row]).Items[0]["ref"]; got != nil {
		t.Fatalf("$SYS rows must not claim to be topic resources: %#v", got)
	}
}

// Both actions ride rows that carry a resource ref, so they must address the
// resource rather than a plain record field that has no meaning in a detail view.
func TestRowActionsAddressTheResource(t *testing.T) {
	params := map[string]map[string]string{}
	for _, action := range New().Manifest().Actions {
		params[action.ID] = action.Params
	}
	for id, key := range map[string]string{"mqtt.retained.clear": "topic", "mqtt.client.kick": "clientid"} {
		if got := params[id][key]; got != "${resource.name}" {
			t.Fatalf("action %q param %q = %q, want ${resource.name}", id, key, got)
		}
	}
}

func TestRetainedListOnlyReturnsRetainedTopics(t *testing.T) {
	s := testSession(options{})
	s.record(testMessage{topic: "a", payload: []byte("1"), retained: true})
	s.record(testMessage{topic: "b", payload: []byte("2")})
	out, err := listRetained(rc(t, s, nil, nil, nil))
	if err != nil {
		t.Fatalf("listRetained: %v", err)
	}
	page := out.(plugin.Page[row])
	if len(page.Items) != 1 || page.Items[0]["topic"] != "a" {
		t.Fatalf("retained rows: %v", rowTopics(page.Items))
	}
}

func TestMessagesListScopesToTheRequestedTopic(t *testing.T) {
	s := testSession(options{})
	s.record(testMessage{topic: "a", payload: []byte("1")})
	s.record(testMessage{topic: "b", payload: []byte("2")})
	s.record(testMessage{topic: "a", payload: []byte("3")})
	out, err := listMessages(rc(t, s, map[string]string{"topic": "a"}, nil, nil))
	if err != nil {
		t.Fatalf("listMessages: %v", err)
	}
	page := out.(plugin.Page[row])
	if len(page.Items) != 2 {
		t.Fatalf("topic scope dropped rows: %v", page.Items)
	}
	// The renderer sends unmatched DataSource params as p.<name>.
	out, err = listMessages(rc(t, s, nil, url.Values{"p.topic": {"b"}}, nil))
	if err != nil {
		t.Fatalf("listMessages query param: %v", err)
	}
	if page := out.(plugin.Page[row]); len(page.Items) != 1 {
		t.Fatalf("query-form topic scope dropped rows: %v", page.Items)
	}
}

func TestTopicTreeExpandsOneLevel(t *testing.T) {
	s := testSession(options{})
	for _, topic := range []string{"sensors/kitchen/temp", "sensors/kitchen/humidity", "sensors/hall", "alerts/fire"} {
		s.record(testMessage{topic: topic, payload: []byte("v")})
	}
	out, err := treeTopics(rc(t, s, nil, nil, nil))
	if err != nil {
		t.Fatalf("treeTopics: %v", err)
	}
	roots := out.(plugin.Page[plugin.TreeNode]).Items
	if len(roots) != 2 || roots[0].Label != "alerts" || roots[1].Label != "sensors" {
		t.Fatalf("roots: %#v", roots)
	}
	if roots[1].Leaf || roots[1].ChildrenSource == nil || roots[1].ChildrenSource.Params["prefix"] != "sensors" {
		t.Fatalf("sensors must expand lazily: %#v", roots[1])
	}

	out, err = treeTopics(rc(t, s, map[string]string{"prefix": "sensors"}, nil, nil))
	if err != nil {
		t.Fatalf("treeTopics(sensors): %v", err)
	}
	children := out.(plugin.Page[plugin.TreeNode]).Items
	if len(children) != 2 {
		t.Fatalf("sensors children: %#v", children)
	}
	// "sensors/hall" is a real topic and a leaf; "sensors/kitchen" is a branch only.
	hall, kitchen := children[0], children[1]
	if hall.Label != "hall" || hall.Ref == nil || hall.Ref.UID != "sensors/hall" || !hall.Leaf {
		t.Fatalf("hall node: %#v", hall)
	}
	if kitchen.Ref != nil || kitchen.Leaf {
		t.Fatalf("kitchen is a branch with no message of its own: %#v", kitchen)
	}
}

func TestTopicTreePagesChildren(t *testing.T) {
	s := testSession(options{})
	for i := range 5 {
		s.record(testMessage{topic: fmt.Sprintf("root/%02d", i), payload: []byte("v")})
	}
	out, err := treeTopics(rc(t, s, map[string]string{"prefix": "root"}, url.Values{"limit": {"2"}}, nil))
	if err != nil {
		t.Fatalf("treeTopics: %v", err)
	}
	page := out.(plugin.Page[plugin.TreeNode])
	if len(page.Items) != 2 || page.NextCursor != "2" {
		t.Fatalf("tree paging: %d items, cursor %q", len(page.Items), page.NextCursor)
	}
}

func TestTopicOverviewRequiresAnObservedTopic(t *testing.T) {
	s := testSession(options{})
	if _, err := topicOverview(rc(t, s, nil, nil, nil)); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("missing topic: got %v", err)
	}
	if _, err := topicOverview(rc(t, s, map[string]string{"topic": "ghost"}, nil, nil)); !errors.Is(err, plugin.ErrNotFound) {
		t.Fatalf("unknown topic: got %v", err)
	}
	s.record(testMessage{topic: "a/b", payload: []byte("42"), qos: 1, retained: true})
	out, err := topicOverview(rc(t, s, map[string]string{"topic": "a/b"}, nil, nil))
	if err != nil {
		t.Fatalf("topicOverview: %v", err)
	}
	got := out.(row)
	if got["payload"] != "42" || got["qos"] != "1" || got["retained"] != true {
		t.Fatalf("topic overview: %#v", got)
	}
}

func TestPublishValidatesBeforeReachingTheBroker(t *testing.T) {
	readOnly := testSession(options{ReadOnly: true})
	body, _ := json.Marshal(map[string]any{"topic": "a/b", "payload": "hi", "encoding": "string", "qos": "0"})
	if _, err := publishMessage(rc(t, readOnly, nil, nil, body)); !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("read-only publish: got %v", err)
	}

	s := testSession(options{})
	wildcard, _ := json.Marshal(map[string]any{"topic": "a/#", "payload": "hi"})
	if _, err := publishMessage(rc(t, s, nil, nil, wildcard)); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("wildcard publish: got %v", err)
	}
	badBase64, _ := json.Marshal(map[string]any{"topic": "a/b", "payload": "!!!", "encoding": "base64"})
	if _, err := publishMessage(rc(t, s, nil, nil, badBase64)); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("bad base64: got %v", err)
	}
	badQoS, _ := json.Marshal(map[string]any{"topic": "a/b", "payload": "hi", "qos": "9"})
	if _, err := publishMessage(rc(t, s, nil, nil, badQoS)); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("bad qos: got %v", err)
	}
	// A disconnected session must not look like a successful publish.
	valid, _ := json.Marshal(map[string]any{"topic": "a/b", "payload": "hi"})
	if _, err := publishMessage(rc(t, s, nil, nil, valid)); !errors.Is(err, plugin.ErrUnavailable) {
		t.Fatalf("disconnected publish: got %v", err)
	}
}

// The topic detail's publish action supplies the topic as a route param.
func TestPublishFallsBackToTheTopicParam(t *testing.T) {
	s := testSession(options{})
	body, _ := json.Marshal(map[string]any{"payload": "hi"})
	_, err := publishMessage(rc(t, s, map[string]string{"topic": "a/b"}, nil, body))
	if !errors.Is(err, plugin.ErrUnavailable) {
		t.Fatalf("param topic should pass validation and fail at the broker: %v", err)
	}
	_, err = publishMessage(rc(t, s, nil, nil, body))
	if !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("no topic anywhere should be rejected: %v", err)
	}
}

func TestDecodePayload(t *testing.T) {
	if out, err := decodePayload("aGk=", "base64"); err != nil || string(out) != "hi" {
		t.Fatalf("base64: %q %v", out, err)
	}
	if out, err := decodePayload("hi", ""); err != nil || string(out) != "hi" {
		t.Fatalf("default encoding: %q %v", out, err)
	}
	if _, err := decodePayload("hi", "cbor"); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("unknown encoding: %v", err)
	}
}

func TestFilterOptionsAreDerivedAndBounded(t *testing.T) {
	s := testSession(options{Observe: true, ObserveFilter: "sensors/#", SysMetrics: true})
	for i := range 200 {
		s.record(testMessage{topic: fmt.Sprintf("root%03d/leaf", i), payload: []byte("v")})
	}
	out, err := filterOptions(rc(t, s, nil, nil, nil))
	if err != nil {
		t.Fatalf("filterOptions: %v", err)
	}
	options := out.([]plugin.Option)
	if options[0].Value != "#" {
		t.Fatalf("the default option must be the catch-all filter: %#v", options[0])
	}
	if options[1].Value != "sensors/#" {
		t.Fatalf("the configured observe filter should be offered: %#v", options[1])
	}
	if len(options) > 51 {
		t.Fatalf("filter options are not bounded: %d", len(options))
	}
	if options[len(options)-1].Value != sysFilter {
		t.Fatalf("$SYS filter should be offered when metrics are enabled: %#v", options[len(options)-1])
	}
}

func TestQoSOptions(t *testing.T) {
	out, err := qosOptions(nil)
	if err != nil {
		t.Fatalf("qosOptions: %v", err)
	}
	if len(out.([]plugin.Option)) != 3 {
		t.Fatalf("qos options: %#v", out)
	}
}

func TestOverviewReportsObservationAndSysState(t *testing.T) {
	s := testSession(options{Observe: true, ObserveFilter: "sensors/#", ClientID: "shellcn", CleanSession: true, KeepAlive: 30 * time.Second, TopicLimit: 1})
	s.record(testMessage{topic: "sensors/a", payload: []byte("1"), retained: true})
	s.record(testMessage{topic: "sensors/b", payload: []byte("2")})
	s.recordSys(testMessage{topic: "$SYS/broker/messages/received", payload: []byte("99")})
	out, err := overview(rc(t, s, nil, nil, nil))
	if err != nil {
		t.Fatalf("overview: %v", err)
	}
	got := out.(row)
	if got["connected"] != false || got["observe_filter"] != "sensors/#" {
		t.Fatalf("overview connection fields: %#v", got)
	}
	if got["retained_topics"] != 1 || got["topics_dropped"] != uint64(1) {
		t.Fatalf("overview observation fields: %#v", got)
	}
	if got["sys_messages_received"] != float64(99) {
		t.Fatalf("overview $SYS fields: %#v", got)
	}
}

func TestClearMessagesEmptiesTheBuffer(t *testing.T) {
	s := testSession(options{})
	s.record(testMessage{topic: "a", payload: []byte("1")})
	if _, err := clearMessages(rc(t, s, nil, nil, nil)); err != nil {
		t.Fatalf("clearMessages: %v", err)
	}
	if len(s.snapshotMessages()) != 0 {
		t.Fatal("buffer was not cleared")
	}
	if stats := s.stats(); stats.Topics != 1 {
		t.Fatalf("clearing the buffer must not drop observed topics: %#v", stats)
	}
}

func TestClearRetainedRequiresWriteAccess(t *testing.T) {
	readOnly := testSession(options{ReadOnly: true})
	if _, err := clearRetained(rc(t, readOnly, map[string]string{"topic": "a"}, nil, nil)); !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("read-only clear: got %v", err)
	}
	s := testSession(options{})
	if _, err := clearRetained(rc(t, s, map[string]string{"topic": "a/#"}, nil, nil)); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("wildcard clear: got %v", err)
	}
}

func TestRescanRequiresAConnection(t *testing.T) {
	s := testSession(options{Observe: true})
	s.record(testMessage{topic: "a", payload: []byte("1")})
	if _, err := rescanRetained(rc(t, s, nil, nil, nil)); !errors.Is(err, plugin.ErrUnavailable) {
		t.Fatalf("rescan without a connection: got %v", err)
	}
	if stats := s.stats(); stats.Topics != 0 {
		t.Fatalf("rescan should drop the cache before re-subscribing: %#v", stats)
	}
}

func TestScanBudgetCoversTheRequestedPage(t *testing.T) {
	budget := func(q url.Values) int {
		return scanBudget(plugin.NewRequestContext(context.Background(), plugin.User{}, nil, nil, q, nil))
	}
	if got := budget(nil); got != scanLimit {
		t.Fatalf("first page budget = %d, want %d", got, scanLimit)
	}
	if got := budget(url.Values{"cursor": {"4000"}, "limit": {"50"}}); got != 4051 {
		t.Fatalf("deep page budget = %d, want the walk to reach past row 4050", got)
	}
	if got := budget(url.Values{"cursor": {"bogus"}, "limit": {"50"}}); got != scanLimit {
		t.Fatalf("unparsable cursor budget = %d, want %d", got, scanLimit)
	}
}

func TestCursorOffset(t *testing.T) {
	if offset, err := cursorOffset(""); err != nil || offset != 0 {
		t.Fatalf("empty cursor: %d %v", offset, err)
	}
	if offset, err := cursorOffset("150"); err != nil || offset != 150 {
		t.Fatalf("numeric cursor: %d %v", offset, err)
	}
	for _, cursor := range []string{"-1", "abc"} {
		if _, err := cursorOffset(cursor); !errors.Is(err, plugin.ErrInvalidInput) {
			t.Fatalf("cursor %q: %v", cursor, err)
		}
	}
}

type emqxStub struct {
	server *httptest.Server
	total  int
	// suppressCount reproduces the EMQX behaviour of reporting count 0 when the
	// dataset size is not cheaply knowable (multi-condition or fuzzy queries).
	suppressCount bool
	queries       []url.Values
	auth          string
}

// newEMQXStub serves a synthetic client list with EMQX's {data, meta} envelope.
func newEMQXStub(t *testing.T, total int) *emqxStub {
	t.Helper()
	stub := &emqxStub{total: total}
	stub.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stub.queries = append(stub.queries, r.URL.Query())
		stub.auth = r.Header.Get("Authorization")
		switch {
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		case r.URL.Path == "/api/v5/nodes":
			_ = json.NewEncoder(w).Encode([]map[string]any{{
				"node": "emqx@127.0.0.1", "node_status": "running", "uptime": 5120000, "memory_used": 1024,
			}})
		case r.URL.Path == "/api/v5/clients/missing":
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"code": "CLIENTID_NOT_FOUND", "message": "client not found"})
		case strings.HasPrefix(r.URL.Path, "/api/v5/clients/"):
			_ = json.NewEncoder(w).Encode(map[string]any{"clientid": strings.TrimPrefix(r.URL.Path, "/api/v5/clients/"), "connected": true})
		default:
			page, _ := strconv.Atoi(r.URL.Query().Get("page"))
			limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
			if page < 1 {
				page = 1
			}
			if limit < 1 {
				limit = 50
			}
			start := (page - 1) * limit
			data := []map[string]any{}
			for i := start; i < min(start+limit, stub.total); i++ {
				data = append(data, map[string]any{
					"clientid": fmt.Sprintf("c%03d", stub.total-i), "topic": fmt.Sprintf("t%03d", i),
					"connected": true, "qos": 1, "node": "emqx@127.0.0.1",
				})
			}
			count := stub.total
			if stub.suppressCount {
				count = 0
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": data,
				"meta": map[string]any{"page": page, "limit": limit, "count": count, "hasnext": start+limit < stub.total},
			})
		}
	}))
	t.Cleanup(stub.server.Close)
	return stub
}

func (s *emqxStub) session(t *testing.T, opts options) *Session {
	t.Helper()
	sess := testSession(opts)
	sess.emqx = &emqxClient{http: s.server.Client(), base: s.server.URL, key: "key", secret: "secret"}
	return sess
}

func TestEMQXPagePushesTheWindowToTheServer(t *testing.T) {
	stub := newEMQXStub(t, 500)
	s := stub.session(t, options{})
	out, err := listEMQXClients(rc(t, s, nil, url.Values{"limit": {"50"}, "cursor": {"150"}}, nil))
	if err != nil {
		t.Fatalf("listEMQXClients: %v", err)
	}
	got := stub.queries[len(stub.queries)-1]
	if got.Get("page") != "4" || got.Get("limit") != "50" {
		t.Fatalf("row offset 150 asked for page=%q limit=%q, want 4/50", got.Get("page"), got.Get("limit"))
	}
	page := out.(scanPage)
	if len(page.Items) != 50 {
		t.Fatalf("items: got %d want 50", len(page.Items))
	}
	if page.NextCursor != "200" {
		t.Fatalf("next cursor: got %q want 200", page.NextCursor)
	}
	if page.Total == nil || *page.Total != 500 {
		t.Fatalf("total should come from the server's authoritative count: %v", page.Total)
	}
	if page.Items[0]["ref"] == nil {
		t.Fatal("client rows must carry a resource ref so the detail view can open")
	}
	if !strings.HasPrefix(stub.auth, "Basic ") {
		t.Fatalf("EMQX requests must carry the API key: %q", stub.auth)
	}
}

func TestEMQXLastPageDoesNotAdvertiseMoreRows(t *testing.T) {
	stub := newEMQXStub(t, 60)
	s := stub.session(t, options{})
	out, err := listEMQXClients(rc(t, s, nil, url.Values{"limit": {"50"}, "cursor": {"50"}}, nil))
	if err != nil {
		t.Fatalf("listEMQXClients: %v", err)
	}
	page := out.(scanPage)
	if len(page.Items) != 10 || page.NextCursor != "" {
		t.Fatalf("last page: %d items, cursor %q", len(page.Items), page.NextCursor)
	}
}

// EMQX cannot sort, so a sorted request must be served from a walk of the whole
// dataset rather than from a sorted single page.
func TestEMQXSortWalksTheDatasetBeforeSlicing(t *testing.T) {
	stub := newEMQXStub(t, 500)
	s := stub.session(t, options{})
	out, err := listEMQXClients(rc(t, s, nil, url.Values{"limit": {"5"}, "sort": {"clientid"}}, nil))
	if err != nil {
		t.Fatalf("listEMQXClients: %v", err)
	}
	page := out.(scanPage)
	// clientid counts down as the offset grows, so the lowest id lives on the
	// last server page; a page-local sort could never surface it first.
	if fmt.Sprint(page.Items[0]["clientid"]) != "c001" {
		t.Fatalf("global sort: first row is %v", page.Items[0]["clientid"])
	}
	if len(stub.queries) < 2 {
		t.Fatalf("expected a multi-page walk, got %d requests", len(stub.queries))
	}
}

func TestEMQXSearchFiltersTheWholeDataset(t *testing.T) {
	stub := newEMQXStub(t, 300)
	s := stub.session(t, options{})
	out, err := listEMQXClients(rc(t, s, nil, url.Values{"limit": {"10"}, "filter": {"c010"}}, nil))
	if err != nil {
		t.Fatalf("listEMQXClients: %v", err)
	}
	page := out.(scanPage)
	if len(page.Items) != 1 || fmt.Sprint(page.Items[0]["clientid"]) != "c010" {
		t.Fatalf("search across the dataset: %#v", page.Items)
	}
}

// A walk that stops at the cap must still hand back a cursor, or the rows past
// the cap become unreachable.
func TestEMQXScanStaysReachablePastTheCap(t *testing.T) {
	stub := newEMQXStub(t, scanLimit*2)
	s := stub.session(t, options{})
	out, err := listEMQXClients(rc(t, s, nil, url.Values{"limit": {"50"}, "sort": {"topic"}}, nil))
	if err != nil {
		t.Fatalf("listEMQXClients: %v", err)
	}
	page := out.(scanPage)
	if !page.Truncated || page.ScanLimit == 0 {
		t.Fatalf("a capped walk must say so: %#v", page)
	}
	if page.Total != nil {
		t.Fatal("a capped walk must not report the cap as the dataset total")
	}
	if page.NextCursor == "" {
		t.Fatal("a capped walk must still advertise a continue cursor")
	}
	deep, err := listEMQXClients(rc(t, s, nil, url.Values{"limit": {"50"}, "sort": {"topic"}, "cursor": {strconv.Itoa(scanLimit + 100)}}, nil))
	if err != nil {
		t.Fatalf("deep page: %v", err)
	}
	if len(deep.(scanPage).Items) != 50 {
		t.Fatalf("a page past the cap must still resolve: %d items", len(deep.(scanPage).Items))
	}
}

func TestEMQXSubscriptionScopeSurvivesEveryPageOfAWalk(t *testing.T) {
	stub := newEMQXStub(t, 400)
	s := stub.session(t, options{})
	if _, err := listEMQXSubscriptions(rc(t, s, map[string]string{"clientid": "c001"}, url.Values{"sort": {"topic"}}, nil)); err != nil {
		t.Fatalf("listEMQXSubscriptions: %v", err)
	}
	if len(stub.queries) < 2 {
		t.Fatalf("expected a multi-page walk, got %d requests", len(stub.queries))
	}
	for i, query := range stub.queries {
		if query.Get("clientid") != "c001" {
			t.Fatalf("request %d dropped the client scope: %v", i, query)
		}
	}
}

func TestEMQXBareArrayEndpointsPageLocally(t *testing.T) {
	stub := newEMQXStub(t, 1)
	s := stub.session(t, options{})
	out, err := listEMQXNodes(rc(t, s, nil, nil, nil))
	if err != nil {
		t.Fatalf("listEMQXNodes: %v", err)
	}
	page := out.(scanPage)
	if len(page.Items) != 1 || page.Items[0]["node"] != "emqx@127.0.0.1" {
		t.Fatalf("nodes: %#v", page.Items)
	}
}

// EMQX reports node uptime in milliseconds; a duration column renders seconds.
func TestEMQXNodeUptimeIsConvertedToSeconds(t *testing.T) {
	stub := newEMQXStub(t, 1)
	s := stub.session(t, options{})
	out, err := listEMQXNodes(rc(t, s, nil, nil, nil))
	if err != nil {
		t.Fatalf("listEMQXNodes: %v", err)
	}
	if got := out.(scanPage).Items[0]["uptime"]; got != float64(5120) {
		t.Fatalf("uptime: got %v want 5120 seconds", got)
	}
}

// The QoS badge severities are keyed by string; EMQX sends a number.
func TestEMQXSubscriptionQoSIsRenderedAsABadgeKey(t *testing.T) {
	stub := newEMQXStub(t, 1)
	s := stub.session(t, options{})
	out, err := listEMQXSubscriptions(rc(t, s, nil, nil, nil))
	if err != nil {
		t.Fatalf("listEMQXSubscriptions: %v", err)
	}
	if got := out.(scanPage).Items[0]["qos"]; got != "1" {
		t.Fatalf("qos: got %#v want the string \"1\"", got)
	}
	severities := map[string]plugin.Severity{}
	for _, column := range subscriptionColumns() {
		if column.Key == "qos" {
			severities = column.Severities
		}
	}
	if _, ok := severities[fmt.Sprint(out.(scanPage).Items[0]["qos"])]; !ok {
		t.Fatalf("the rendered qos value does not match any declared severity: %v", severities)
	}
}

// EMQX sets meta.count to 0 (or -1) when it cannot price the count. Echoing that
// as a total tells the grid the dataset is empty while it is still paging.
func TestEMQXDoesNotInventATotalWhenTheServerHasNone(t *testing.T) {
	stub := newEMQXStub(t, 500)
	stub.suppressCount = true
	s := stub.session(t, options{})
	out, err := listEMQXClients(rc(t, s, nil, url.Values{"limit": {"50"}}, nil))
	if err != nil {
		t.Fatalf("listEMQXClients: %v", err)
	}
	page := out.(scanPage)
	if page.Total != nil {
		t.Fatalf("an unavailable count must not become a total: %d", *page.Total)
	}
	if page.NextCursor != "50" || len(page.Items) != 50 {
		t.Fatalf("paging must keep working without a count: %d items, cursor %q", len(page.Items), page.NextCursor)
	}
}

func TestEMQXTotal(t *testing.T) {
	cases := []struct {
		name   string
		meta   emqxMeta
		offset int
		rows   int
		want   int
		ok     bool
	}{
		{name: "authoritative count", meta: emqxMeta{Count: 500}, rows: 50, want: 500, ok: true},
		{name: "fuzzy query", meta: emqxMeta{Count: -1, HasNext: true}, rows: 50},
		{name: "unavailable count mid-walk", meta: emqxMeta{Count: 0, HasNext: true}, rows: 50},
		{name: "last page of an uncounted walk", meta: emqxMeta{Count: 0}, offset: 100, rows: 20, want: 120, ok: true},
		{name: "genuinely empty", meta: emqxMeta{Count: 0}, want: 0, ok: true},
		{name: "empty page past the end", meta: emqxMeta{Count: 0}, offset: 100},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := emqxTotal(tc.meta, tc.offset, tc.rows)
			if ok != tc.ok || (ok && got != tc.want) {
				t.Fatalf("emqxTotal = %d, %v; want %d, %v", got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestEMQXErrorsMapToSentinels(t *testing.T) {
	stub := newEMQXStub(t, 1)
	s := stub.session(t, options{})
	if _, err := emqxClientOverview(rc(t, s, map[string]string{"clientid": "missing"}, nil, nil)); !errors.Is(err, plugin.ErrNotFound) {
		t.Fatalf("404: got %v", err)
	}
	for status, want := range map[int]error{
		http.StatusUnauthorized: plugin.ErrUnauthorized,
		http.StatusForbidden:    plugin.ErrForbidden,
		http.StatusConflict:     plugin.ErrConflict,
		http.StatusBadRequest:   plugin.ErrInvalidInput,
		http.StatusBadGateway:   plugin.ErrUnavailable,
	} {
		if err := emqxHTTPError(status, []byte(`{"message":"nope"}`)); !errors.Is(err, want) {
			t.Fatalf("status %d: got %v want %v", status, err, want)
		}
	}
}

func TestEMQXRoutesDegradeWithoutTheManagementAPI(t *testing.T) {
	s := testSession(options{})
	out, err := listEMQXClients(rc(t, s, nil, nil, nil))
	if err != nil {
		t.Fatalf("listEMQXClients without EMQX: %v", err)
	}
	page := out.(scanPage)
	if len(page.Items) != 0 || !page.Unavailable || page.Note == "" {
		t.Fatalf("an unconfigured management API should return an annotated empty page: %#v", page)
	}
	if _, err := emqxClientOverview(rc(t, s, map[string]string{"clientid": "c1"}, nil, nil)); !errors.Is(err, plugin.ErrNotSupported) {
		t.Fatalf("client detail without EMQX: got %v", err)
	}
	if _, err := kickEMQXClient(rc(t, s, map[string]string{"clientid": "c1"}, nil, nil)); !errors.Is(err, plugin.ErrNotSupported) {
		t.Fatalf("kick without EMQX: got %v", err)
	}
}

func TestKickRequiresWriteAccess(t *testing.T) {
	stub := newEMQXStub(t, 1)
	readOnly := stub.session(t, options{ReadOnly: true})
	if _, err := kickEMQXClient(rc(t, readOnly, map[string]string{"clientid": "c1"}, nil, nil)); !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("read-only kick: got %v", err)
	}
	s := stub.session(t, options{})
	if _, err := kickEMQXClient(rc(t, s, nil, nil, nil)); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("kick without a client id: got %v", err)
	}
	if _, err := kickEMQXClient(rc(t, s, map[string]string{"clientid": "c1"}, nil, nil)); err != nil {
		t.Fatalf("kick: %v", err)
	}
}

type memoryStream struct {
	ctx    context.Context
	writes []byte
}

func (m *memoryStream) Read([]byte) (int, error) { return 0, nil }
func (m *memoryStream) Write(p []byte) (int, error) {
	m.writes = append(m.writes, p...)
	return len(p), nil
}
func (m *memoryStream) Close() error             { return nil }
func (m *memoryStream) Context() context.Context { return m.ctx }
func (m *memoryStream) output() string           { return string(m.writes) }
func newMemoryStream() *memoryStream             { return &memoryStream{ctx: context.Background()} }
func streamRoute(t *testing.T) plugin.StreamHandler {
	t.Helper()
	return routeByID(t, "mqtt.subscribe").Stream
}
func routeByID(t *testing.T, id string) plugin.Route {
	t.Helper()
	return sdktest.RouteMap(New().Routes())[id]
}

func TestSubscribeStreamValidatesBeforeDialing(t *testing.T) {
	s := testSession(options{})
	handler := streamRoute(t)
	if err := handler(rc(t, s, map[string]string{"filter": "a/#/b"}, nil, nil), newMemoryStream()); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("bad filter: got %v", err)
	}
	if err := handler(rc(t, s, map[string]string{"qos": "7"}, nil, nil), newMemoryStream()); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("bad qos: got %v", err)
	}
	// No transport means the stream cannot dial its own connection.
	if err := handler(rc(t, s, nil, nil, nil), newMemoryStream()); !errors.Is(err, plugin.ErrUnavailable) {
		t.Fatalf("no transport: got %v", err)
	}
}

func TestLogFrameCarriesQoSAndRetainedFlags(t *testing.T) {
	stream := newMemoryStream()
	record := messageRecord{Topic: "a/b", Payload: "42", Size: 2, QoS: 2, Retained: true, Encoding: "string"}
	if err := writeLogFrame(stream, formatRecord(record)); err != nil {
		t.Fatalf("writeLogFrame: %v", err)
	}
	var frame struct {
		TS   string `json:"ts"`
		Line string `json:"line"`
	}
	if err := json.Unmarshal(stream.writes, &frame); err != nil {
		t.Fatalf("frame is not JSON: %v (%s)", err, stream.output())
	}
	if frame.TS == "" {
		t.Fatal("log frames must carry a timestamp")
	}
	for _, want := range []string{"qos2", "retained", "a/b", "42"} {
		if !strings.Contains(frame.Line, want) {
			t.Fatalf("line %q is missing %q", frame.Line, want)
		}
	}
}

// A route nothing in the manifest points at is dead weight the generic renderer
// can never reach; the projection carries every bound route id.
func TestEveryRouteIsReachableFromTheManifest(t *testing.T) {
	p := New()
	projection, err := json.Marshal(sdktest.Projection(t, p))
	if err != nil {
		t.Fatalf("marshal projection: %v", err)
	}
	for _, route := range p.Routes() {
		if !strings.Contains(string(projection), `"`+route.ID+`"`) {
			t.Fatalf("route %q is not bound to any panel, action, tree, or stream", route.ID)
		}
	}
}

// The observation cache is written from paho's dispatch goroutines while
// handlers read snapshots; run under -race this pins the locking down.
func TestObservationCacheIsSafeUnderConcurrentDeliveries(t *testing.T) {
	s := testSession(options{TopicLimit: 8, MessageLimit: 16})
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 50 {
				s.record(testMessage{topic: fmt.Sprintf("t/%d", (worker+i)%16), payload: []byte("v")})
				s.recordSys(testMessage{topic: "$SYS/broker/uptime", payload: []byte("1")})
				s.markRetained(fmt.Sprintf("t/%d", i%16), i%2 == 0)
			}
		}()
	}
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				_ = s.snapshotTopics()
				_ = s.snapshotMessages()
				_ = s.snapshotSys()
				_ = s.stats()
				_ = s.sysValue("uptime")
			}
		}()
	}
	wg.Wait()
	if stats := s.stats(); stats.Topics > 8 || stats.Buffered > 16 {
		t.Fatalf("caps were not held under concurrency: %#v", stats)
	}
}

func TestEveryRouteIsCovered(t *testing.T) {
	stub := newEMQXStub(t, 120)
	s := stub.session(t, options{Observe: true, SysMetrics: true})
	s.record(testMessage{topic: "sensors/a", payload: []byte("1"), retained: true})
	s.recordSys(testMessage{topic: "$SYS/broker/uptime", payload: []byte("10")})

	h := plugintest.NewHarness(t, New().Routes())
	ctx := context.Background()
	for _, id := range []string{
		"mqtt.overview", "mqtt.sys.list", "mqtt.topics.list", "mqtt.topics.tree",
		"mqtt.retained.list", "mqtt.messages.list", "mqtt.messages.clear",
		"mqtt.filters.options", "mqtt.qos.options",
		"mqtt.emqx.clients.list", "mqtt.emqx.subscriptions.list", "mqtt.emqx.topics.list", "mqtt.emqx.nodes.list",
	} {
		h.Call(ctx, id, s, nil, nil, nil)
	}
	h.Call(ctx, "mqtt.topic.overview", s, map[string]string{"topic": "sensors/a"}, nil, nil)
	h.Call(ctx, "mqtt.emqx.client.overview", s, map[string]string{"clientid": "c001"}, nil, nil)
	h.Call(ctx, "mqtt.emqx.client.kick", s, map[string]string{"clientid": "c001"}, nil, nil)
	// These need a live broker connection; the harness records coverage without
	// asserting the disconnected-session error.
	h.CallNoFail(ctx, "mqtt.publish", s, map[string]string{"topic": "sensors/a"})
	h.CallNoFail(ctx, "mqtt.retained.clear", s, map[string]string{"topic": "sensors/a"})
	h.CallNoFail(ctx, "mqtt.retained.rescan", s, nil)
	h.Route("mqtt.subscribe")
	h.AssertAllCovered()
}
