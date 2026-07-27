package pulsar

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
	"sync/atomic"
	"testing"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/plugintest"
	"github.com/charlesng35/shellcn/sdk/plugin"
	sdktest "github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestPulsarManifestValidates(t *testing.T) {
	p := New()
	proj := sdktest.Projection(t, p)
	if proj.Category.Key != plugin.CategoryMessaging {
		t.Fatalf("category: got %q want %q", proj.Category.Key, plugin.CategoryMessaging)
	}
	if proj.Layout != plugin.LayoutSidebarTree {
		t.Fatalf("layout: got %q", proj.Layout)
	}
	kinds := map[string]bool{}
	for _, res := range proj.Resources {
		kinds[res.Kind] = true
	}
	for _, kind := range []string{"tenant", "namespace", "topic", "subscription", "cluster", "broker"} {
		if !kinds[kind] {
			t.Fatalf("missing resource kind %q", kind)
		}
	}
}

func TestPulsarConfigSchemaIsSpecific(t *testing.T) {
	fields := map[string]plugin.Field{}
	for _, group := range New().Manifest().Config.Groups {
		for _, field := range group.Fields {
			fields[field.Key] = field
		}
	}
	for _, key := range []string{"admin_url", "service_url", "tenant", "namespace", "auth", "token", tokenCredentialField, plugin.CredentialRefField, clientCertField, "read_only", "message_limit"} {
		if _, ok := fields[key]; !ok {
			t.Fatalf("missing config field %q", key)
		}
	}
	for _, key := range []string{"brokers", "urls", "vhost"} {
		if _, ok := fields[key]; ok {
			t.Fatalf("pulsar should not expose %q", key)
		}
	}
	if !fields["token"].Secret {
		t.Fatal("the inline token must be stored as a secret")
	}
	if fields[tokenCredentialField].Credential == nil || fields[tokenCredentialField].Credential.Kind != plugin.CredentialKindBearerToken {
		t.Fatalf("stored token field must select bearer-token credentials, got %#v", fields[tokenCredentialField].Credential)
	}
}

func TestParseTopicAcceptsBothDomainsAndRejectsEscapes(t *testing.T) {
	ref, err := parseTopic("non-persistent://public/default/telemetry")
	if err != nil {
		t.Fatalf("parseTopic: %v", err)
	}
	if ref.Domain != nonPersistentDomain || ref.Tenant != "public" || ref.Namespace != "default" || ref.Name != "telemetry" {
		t.Fatalf("parsed %#v", ref)
	}
	if got := ref.path(); got != "/admin/v2/non-persistent/public/default/telemetry" {
		t.Fatalf("admin path: %q", got)
	}
	bare, err := parseTopic("public/default/orders")
	if err != nil || bare.Domain != persistentDomain {
		t.Fatalf("a bare name should default to the persistent domain, got %#v (%v)", bare, err)
	}
	for _, bad := range []string{"", "public/default", "kafka://public/default/x", "public/default/a/b", "public/../default/x"} {
		if _, err := parseTopic(bad); !errors.Is(err, plugin.ErrInvalidInput) {
			t.Fatalf("parseTopic(%q) = %v, want ErrInvalidInput", bad, err)
		}
	}
}

func TestSimpleNameRejectsPathTraversal(t *testing.T) {
	if _, err := simpleName("tenant", "public/../other"); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("got %v, want ErrInvalidInput", err)
	}
	if got, err := simpleName("tenant", " public "); err != nil || got != "public" {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestCursorPositionEncodesPulsarMessageIds(t *testing.T) {
	earliest, err := cursorPosition(positionEarliest)
	if err != nil || earliest["ledgerId"] != -1 || earliest["entryId"] != -1 {
		t.Fatalf("earliest: %#v (%v)", earliest, err)
	}
	latest, err := cursorPosition("")
	if err != nil || latest["ledgerId"].(int64) <= 0 {
		t.Fatalf("an empty target must default to latest, got %#v (%v)", latest, err)
	}
	if _, err := cursorPosition("middle"); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("got %v, want ErrInvalidInput", err)
	}
}

func TestMessageRowReadsPulsarHeaders(t *testing.T) {
	header := http.Header{}
	header.Set("X-Pulsar-Message-ID", "12:3:-1")
	header.Set("X-Pulsar-publish-time", "2026-07-27T10:11:12.000+0000")
	header.Set("X-Pulsar-producer-name", "orders-writer")
	header.Set("X-Pulsar-partition-key", "customer-9")
	header.Set("X-Pulsar-PROPERTY", `{"source":"checkout"}`)
	header.Set("X-Pulsar-num-batch-message", "4")

	ref, _ := parseTopic("persistent://public/default/orders")
	item := messageRow(ref, 3, header, []byte("hello"))
	if item["message_id"] != "12:3:-1" || item["producer"] != "orders-writer" || item["key"] != "customer-9" {
		t.Fatalf("identity fields: %#v", item)
	}
	if item["payload"] != "hello" || item["encoding"] != "string" || item["size"] != 5 {
		t.Fatalf("payload fields: %#v", item)
	}
	if item["batch_size"] != 4 {
		t.Fatalf("batch size: %#v", item["batch_size"])
	}
	props := item["properties"].(map[string]string)
	if len(props) != 1 || props["source"] != "checkout" {
		t.Fatalf("properties: %#v", props)
	}
	legacy := http.Header{}
	legacy.Set("X-Pulsar-PROPERTY-region", "eu")
	legacy.Set("X-Pulsar-PROPERTY-TOTAL-CHUNKS", "3")
	older := messageRow(ref, 1, legacy, nil)["properties"].(map[string]string)
	if len(older) != 1 || older["region"] != "eu" {
		t.Fatalf("legacy per-property headers: %#v", older)
	}
	published, ok := item["publish_time"].(time.Time)
	if !ok || published.UTC().Format(time.RFC3339) != "2026-07-27T10:11:12Z" {
		t.Fatalf("publish time: %#v", item["publish_time"])
	}
	binary := messageRow(ref, 1, http.Header{}, []byte{0x00, 0xff})
	if binary["encoding"] != "base64" || binary["payload"] != "AP8=" {
		t.Fatalf("binary payload should fall back to base64, got %#v", binary)
	}
}

func TestTopicsPageFoldsPartitionsAndSortsAcrossTheNamespace(t *testing.T) {
	mock := newAdminMock(t)
	mock.topics = []string{
		"persistent://public/default/orders",
		"persistent://public/default/events-partition-0",
		"persistent://public/default/events-partition-1",
		"persistent://public/default/events-partition-2",
		"non-persistent://public/default/telemetry",
	}
	mock.partitioned = []string{"persistent://public/default/events"}
	s := testSession(mock)

	page := callPage(t, listTopics, s, nil, url.Values{"limit": {"10"}})
	if page.Total == nil || *page.Total != 3 {
		t.Fatalf("total: %v, want the three logical topics", page.Total)
	}
	byName := map[string]row{}
	for _, item := range page.Items {
		byName[fmt.Sprint(item["name"])] = item
	}
	if _, folded := byName["events-partition-0"]; folded {
		t.Fatalf("partitions must be folded into their parent: %#v", page.Items)
	}
	events := byName["events"]
	if events["partitioned"] != true || events["partitions"] != 3 {
		t.Fatalf("partitioned parent: %#v", events)
	}
	if byName["telemetry"]["domain"] != nonPersistentDomain {
		t.Fatalf("non-persistent topic lost its domain: %#v", byName["telemetry"])
	}

	// The whole namespace is sorted before it is sliced, so the first row of a
	// one-row page must be the namespace-wide maximum, not the page maximum.
	first := callPage(t, listTopics, s, nil, url.Values{"limit": {"1"}, "sort": {"-name"}})
	if len(first.Items) != 1 || first.Items[0]["name"] != "telemetry" {
		t.Fatalf("descending sort across the dataset: %#v", first.Items)
	}
	if first.NextCursor != "1" {
		t.Fatalf("next cursor: got %q want %q", first.NextCursor, "1")
	}
	second := callPage(t, listTopics, s, nil, url.Values{"limit": {"1"}, "sort": {"-name"}, "cursor": {first.NextCursor}})
	if len(second.Items) != 1 || second.Items[0]["name"] != "orders" {
		t.Fatalf("second page: %#v", second.Items)
	}
	last := callPage(t, listTopics, s, nil, url.Values{"limit": {"1"}, "cursor": {"2"}})
	if last.NextCursor != "" {
		t.Fatalf("the final page must not advertise another page, got %q", last.NextCursor)
	}
}

func TestTopicsPagePrefersTheConfiguredPartitionCount(t *testing.T) {
	mock := newAdminMock(t)
	// Two partitions exist in the namespace listing, but the topic is configured
	// for five; the aggregate stats document is the authority.
	mock.configuredPartitions = 5
	s := testSession(mock)

	page := callPage(t, listTopics, s, nil, url.Values{"limit": {"10"}})
	for _, item := range page.Items {
		if item["name"] != "events" {
			continue
		}
		if item["partitions"] != 5 {
			t.Fatalf("partition count: %#v", item)
		}
		return
	}
	t.Fatalf("the partitioned parent is missing: %#v", page.Items)
}

func TestTopicsPageEnrichesOnlyTheCurrentPage(t *testing.T) {
	mock := newAdminMock(t)
	mock.topics, mock.partitioned = nil, nil
	for i := range 12 {
		mock.topics = append(mock.topics, fmt.Sprintf("persistent://public/default/topic-%02d", i))
	}
	s := testSession(mock)

	page := callPage(t, listTopics, s, nil, url.Values{"limit": {"4"}})
	if len(page.Items) != 4 || page.Total == nil || *page.Total != 12 {
		t.Fatalf("items: %d, total: %v", len(page.Items), page.Total)
	}
	if got := mock.statsCalls.Load(); got != 4 {
		t.Fatalf("stats round trips: got %d, want one per row on the page", got)
	}
	if page.Items[0]["subscriptions"] != 2 || page.Items[0]["storage_size"] != int64(4096) {
		t.Fatalf("page rows were not enriched: %#v", page.Items[0])
	}
}

func TestTopicsPageKeepsTheNamespaceScopeOnEveryPage(t *testing.T) {
	mock := newAdminMock(t)
	s := testSession(mock)
	params := map[string]string{"tenant": "acme", "namespace": "events"}

	if _, err := listTopics(newContext(s, params, url.Values{"limit": {"2"}, "cursor": {"2"}})); err != nil {
		t.Fatalf("listTopics: %v", err)
	}
	for _, req := range mock.seen() {
		scoping := strings.Contains(req.Path, "/namespaces/") || strings.HasSuffix(req.Path, "/partitioned")
		if scoping && !strings.Contains(req.Path, "/acme/events") {
			t.Fatalf("a paged request dropped the connection scope: %s", req.Path)
		}
	}
	if !mock.sawPath("/admin/v2/namespaces/acme/events/topics") {
		t.Fatalf("the scoped listing was never requested: %#v", mock.seen())
	}
}

func TestListMessagesWalksPositionsFromTheCursor(t *testing.T) {
	mock := newAdminMock(t)
	s := testSession(mock)
	params := map[string]string{"topic": "persistent://public/default/orders"}

	page := callPage(t, listMessages, s, params, url.Values{"limit": {"3"}})
	if len(page.Items) != 3 {
		t.Fatalf("items: %#v", page.Items)
	}
	if page.Items[0]["position"] != 1 || page.Items[2]["position"] != 3 {
		t.Fatalf("the walk must start at the newest message: %#v", page.Items)
	}
	if page.NextCursor != "3" {
		t.Fatalf("next cursor: got %q want %q", page.NextCursor, "3")
	}
	if page.Total != nil {
		t.Fatal("a log walk has no authoritative total")
	}

	next := callPage(t, listMessages, s, params, url.Values{"limit": {"3"}, "cursor": {page.NextCursor}})
	if len(next.Items) == 0 || next.Items[0]["position"] != 4 {
		t.Fatalf("the second page must continue past the cursor: %#v", next.Items)
	}
	positions := mock.examinePositions()
	if len(positions) != 6 || positions[0] != "1" || positions[5] != "6" {
		t.Fatalf("examined positions: %v", positions)
	}
}

func TestListMessagesStopsAtTheEndOfTheTopic(t *testing.T) {
	mock := newAdminMock(t)
	mock.messages = 2
	s := testSession(mock)
	params := map[string]string{"topic": "persistent://public/default/orders"}

	page := callPage(t, listMessages, s, params, url.Values{"limit": {"5"}})
	if len(page.Items) != 2 {
		t.Fatalf("items: %#v", page.Items)
	}
	if page.NextCursor != "" {
		t.Fatalf("a short page must end the walk, got cursor %q", page.NextCursor)
	}
}

func TestListMessagesRespectsTheConnectionMessageLimit(t *testing.T) {
	mock := newAdminMock(t)
	s := testSession(mock)
	s.opts.MessageLimit = 2
	params := map[string]string{"topic": "persistent://public/default/orders"}

	page := callPage(t, listMessages, s, params, url.Values{"limit": {"50"}})
	if len(page.Items) != 2 || page.NextCursor != "2" {
		t.Fatalf("the connection limit must cap the walk: %d items, cursor %q", len(page.Items), page.NextCursor)
	}
}

func TestListMessagesPeeksThroughASubscription(t *testing.T) {
	mock := newAdminMock(t)
	s := testSession(mock)
	params := map[string]string{"topic": "persistent://public/default/orders", "subscription": "billing"}

	page := callPage(t, listMessages, s, params, url.Values{"limit": {"2"}})
	if len(page.Items) != 2 {
		t.Fatalf("items: %#v", page.Items)
	}
	if !mock.sawPath("/admin/v2/persistent/public/default/orders/subscription/billing/position/1") {
		t.Fatalf("peek was not scoped to the subscription: %#v", mock.seen())
	}
	if mock.sawPathPrefix("/admin/v2/persistent/public/default/orders/examinemessage") {
		t.Fatal("a subscription-scoped browse must not fall back to examine")
	}
}

func TestListMessagesRejectsANonNumericCursor(t *testing.T) {
	s := testSession(newAdminMock(t))
	rc := newContext(s, map[string]string{"topic": "persistent://public/default/orders"}, url.Values{"cursor": {"abc"}})
	if _, err := listMessages(rc); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("got %v, want ErrInvalidInput", err)
	}
}

func TestSubscriptionsAndConsumersComeFromTopicStats(t *testing.T) {
	mock := newAdminMock(t)
	s := testSession(mock)
	params := map[string]string{"topic": "persistent://public/default/orders"}

	subs := callPage(t, listSubscriptions, s, params, nil)
	if len(subs.Items) != 2 {
		t.Fatalf("subscriptions: %#v", subs.Items)
	}
	if subs.Items[0]["name"] != "audit" || subs.Items[1]["name"] != "billing" {
		t.Fatalf("subscriptions must be name-ordered: %#v", subs.Items)
	}
	billing := subs.Items[1]
	if billing["consumers"] != 1 || billing["msg_backlog"] != int64(7) || billing["type"] != "Shared" {
		t.Fatalf("subscription row: %#v", billing)
	}
	ref := billing["ref"].(plugin.ResourceIdentity)
	if ref.Kind != "subscription" || ref.Namespace != "persistent://public/default/orders" || ref.Name != "billing" {
		t.Fatalf("subscription ref: %#v", ref)
	}

	all := callPage(t, listConsumers, s, params, nil)
	if len(all.Items) != 2 {
		t.Fatalf("topic consumers: %#v", all.Items)
	}
	scoped := callPage(t, listConsumers, s, map[string]string{"topic": params["topic"], "subscription": "billing"}, nil)
	if len(scoped.Items) != 1 || scoped.Items[0]["subscription"] != "billing" {
		t.Fatalf("subscription-scoped consumers: %#v", scoped.Items)
	}
}

func TestProducersFlattenPartitionsOfAPartitionedTopic(t *testing.T) {
	mock := newAdminMock(t)
	s := testSession(mock)
	params := map[string]string{"topic": "persistent://public/default/events"}

	producers := callPage(t, listProducers, s, params, nil)
	// The aggregate document repeats every partition's publishers, so a reader
	// that used both lists would report each producer twice.
	if len(producers.Items) != 2 {
		t.Fatalf("producers: %#v", producers.Items)
	}
	topics := map[string]bool{}
	names := map[string]int{}
	for _, item := range producers.Items {
		topics[fmt.Sprint(item["topic"])] = true
		names[fmt.Sprint(item["producer_name"])]++
	}
	if !topics["persistent://public/default/events-partition-0"] || !topics["persistent://public/default/events-partition-1"] {
		t.Fatalf("producers were not attributed to their partition: %#v", producers.Items)
	}
	for name, count := range names {
		if count != 1 {
			t.Fatalf("producer %q is listed %d times: %#v", name, count, producers.Items)
		}
	}

	plain := callPage(t, listProducers, s, map[string]string{"topic": "persistent://public/default/orders"}, nil)
	if len(plain.Items) != 1 || plain.Items[0]["topic"] != "persistent://public/default/orders" {
		t.Fatalf("a non-partitioned topic reports its own publishers: %#v", plain.Items)
	}
	// The broker's own date format is not a timestamp the grid can parse, so the
	// row carries a real time instead.
	if _, ok := plain.Items[0]["connected_since"].(time.Time); !ok {
		t.Fatalf("connected_since: %#v", plain.Items[0]["connected_since"])
	}
}

func TestTerminateUsesThePartitionedEndpoint(t *testing.T) {
	mock := newAdminMock(t)
	s := testSession(mock)

	if _, err := terminateTopic(newContext(s, map[string]string{"topic": "persistent://public/default/events"}, nil)); err != nil {
		t.Fatalf("terminate a partitioned topic: %v", err)
	}
	if !mock.sawPath("/admin/v2/persistent/public/default/events/terminate/partitions") {
		t.Fatalf("a partitioned topic must terminate through its partitions: %#v", mock.seen())
	}
	if _, err := terminateTopic(newContext(s, map[string]string{"topic": "persistent://public/default/orders"}, nil)); err != nil {
		t.Fatalf("terminate a plain topic: %v", err)
	}
	if !mock.sawPath("/admin/v2/persistent/public/default/orders/terminate") {
		t.Fatalf("a plain topic terminates directly: %#v", mock.seen())
	}
}

func TestSubscriptionRowsCarryTheTopicShape(t *testing.T) {
	mock := newAdminMock(t)
	s := testSession(mock)

	// The backlog panel is only shown for a topic the broker will peek, so the
	// row has to carry the fields that gate it.
	subs := callPage(t, listSubscriptions, s, map[string]string{"topic": "persistent://public/default/events"}, nil)
	if len(subs.Items) == 0 {
		t.Fatal("no subscriptions")
	}
	if subs.Items[0]["partitioned"] != true || subs.Items[0]["domain"] != persistentDomain {
		t.Fatalf("subscription row: %#v", subs.Items[0])
	}
	plain := callPage(t, listSubscriptions, s, map[string]string{"topic": "non-persistent://public/default/telemetry"}, nil)
	if len(plain.Items) == 0 || plain.Items[0]["partitioned"] != false || plain.Items[0]["domain"] != nonPersistentDomain {
		t.Fatalf("subscription row: %#v", plain.Items)
	}
}

func TestListMessagesExplainsAnUnbrowsableTopic(t *testing.T) {
	mock := newAdminMock(t)
	s := testSession(mock)

	_, err := listMessages(newContext(s, map[string]string{"topic": "persistent://public/default/events"}, url.Values{"limit": {"2"}}))
	if !errors.Is(err, plugin.ErrNotSupported) {
		t.Fatalf("got %v, want ErrNotSupported", err)
	}
	if !strings.Contains(err.Error(), "partition") {
		t.Fatalf("the error must point at the partitions: %v", err)
	}
}

func TestTopicStatsPicksThePartitionedEndpoint(t *testing.T) {
	mock := newAdminMock(t)
	s := testSession(mock)

	out, err := readTopicStats(newContext(s, map[string]string{"topic": "persistent://public/default/events"}, nil))
	if err != nil {
		t.Fatalf("readTopicStats: %v", err)
	}
	stats := out.(row)
	if stats["partitioned"] != true || stats["partitions"] != 2 {
		t.Fatalf("partition metadata: %#v", stats)
	}
	if !mock.sawPathPrefix("/admin/v2/persistent/public/default/events/partitioned-stats") {
		t.Fatalf("a partitioned topic must read partitioned-stats: %#v", mock.seen())
	}
	if mock.sawPath("/admin/v2/persistent/public/default/events/stats") {
		t.Fatal("a partitioned topic has no /stats document")
	}
}

func TestPartitionsAreNavigableTopics(t *testing.T) {
	mock := newAdminMock(t)
	s := testSession(mock)

	page := callPage(t, listPartitions, s, map[string]string{"topic": "persistent://public/default/events"}, nil)
	if len(page.Items) != 2 {
		t.Fatalf("partitions: %#v", page.Items)
	}
	if page.Items[0]["partition"] != 0 || page.Items[1]["partition"] != 1 {
		t.Fatalf("partitions must be index-ordered: %#v", page.Items)
	}
	ref, ok := page.Items[0]["ref"].(plugin.ResourceIdentity)
	if !ok || ref.Kind != "topic" || ref.UID != "persistent://public/default/events-partition-0" {
		t.Fatalf("partition row must open its own topic: %#v", page.Items[0]["ref"])
	}

	empty := callPage(t, listPartitions, s, map[string]string{"topic": "persistent://public/default/orders"}, nil)
	if len(empty.Items) != 0 {
		t.Fatalf("a non-partitioned topic has no partitions: %#v", empty.Items)
	}
}

func TestTopicSchemaReportsAnUnregisteredSchema(t *testing.T) {
	mock := newAdminMock(t)
	mock.schema = false
	s := testSession(mock)

	out, err := readTopicSchema(newContext(s, map[string]string{"topic": "persistent://public/default/orders"}, nil))
	if err != nil {
		t.Fatalf("readTopicSchema: %v", err)
	}
	schema := out.(row)
	if schema["registered"] != false || schema["type"] != "NONE" {
		t.Fatalf("schemaless topic: %#v", schema)
	}
}

func TestResetCursorChoosesTheRightEndpoint(t *testing.T) {
	mock := newAdminMock(t)
	s := testSession(mock)
	params := map[string]string{"topic": "persistent://public/default/orders", "subscription": "billing"}

	if _, err := resetCursor(newBodyContext(s, params, `{"target":"earliest"}`)); err != nil {
		t.Fatalf("reset to earliest: %v", err)
	}
	body := mock.lastBody("/admin/v2/persistent/public/default/orders/subscription/billing/resetcursor")
	if !strings.Contains(body, `"ledgerId":-1`) {
		t.Fatalf("earliest must post the earliest message id, got %q", body)
	}

	if _, err := resetCursor(newBodyContext(s, params, `{"target":"time","age":"90m"}`)); err != nil {
		t.Fatalf("reset to a timestamp: %v", err)
	}
	stamped := ""
	for _, req := range mock.seen() {
		if strings.HasPrefix(req.Path, "/admin/v2/persistent/public/default/orders/subscription/billing/resetcursor/") {
			stamped = strings.TrimPrefix(req.Path, "/admin/v2/persistent/public/default/orders/subscription/billing/resetcursor/")
		}
	}
	millis, err := strconv.ParseInt(stamped, 10, 64)
	if err != nil {
		t.Fatalf("timestamped reset path: %q", stamped)
	}
	if delta := time.Since(time.UnixMilli(millis)); delta < 89*time.Minute || delta > 91*time.Minute {
		t.Fatalf("reset timestamp is %s ago, want about 90m", delta)
	}

	if _, err := resetCursor(newBodyContext(s, params, `{"target":"time","age":"nonsense"}`)); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("got %v, want ErrInvalidInput", err)
	}
}

func TestReadOnlyConnectionsBlockEveryMutation(t *testing.T) {
	mock := newAdminMock(t)
	s := testSession(mock)
	s.opts.ReadOnly = true
	topic := map[string]string{"topic": "persistent://public/default/orders"}
	subscription := map[string]string{"topic": "persistent://public/default/orders", "subscription": "billing"}

	cases := []struct {
		name   string
		call   func() (any, error)
		params map[string]string
	}{
		{"topic.delete", func() (any, error) { return deleteTopic(newContext(s, topic, nil)) }, topic},
		{"topic.terminate", func() (any, error) { return terminateTopic(newContext(s, topic, nil)) }, topic},
		{"subscription.clear", func() (any, error) { return clearBacklog(newContext(s, subscription, nil)) }, subscription},
		{"namespace.delete", func() (any, error) { return deleteNamespace(newContext(s, nil, nil)) }, nil},
		{"tenant.delete", func() (any, error) { return deleteTenant(newContext(s, nil, nil)) }, nil},
		{"message.produce", func() (any, error) { return produceMessage(newBodyContext(s, topic, `{"payload":"x"}`)) }, topic},
	}
	for _, tc := range cases {
		if _, err := tc.call(); !errors.Is(err, plugin.ErrForbidden) {
			t.Fatalf("%s: got %v, want ErrForbidden", tc.name, err)
		}
	}
	for _, req := range mock.seen() {
		if req.Method != http.MethodGet {
			t.Fatalf("a read-only connection reached the broker with %s %s", req.Method, req.Path)
		}
	}
}

func TestMutationsRequireAnExplicitTarget(t *testing.T) {
	mock := newAdminMock(t)
	s := testSession(mock)

	// Without params a listing legitimately falls back to the connection scope;
	// a delete must not, or it would remove public/default instead.
	if _, err := deleteNamespace(newContext(s, nil, nil)); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("namespace delete without params: got %v, want ErrInvalidInput", err)
	}
	if _, err := deleteTenant(newContext(s, nil, nil)); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("tenant delete without params: got %v, want ErrInvalidInput", err)
	}
	if _, err := deleteTopic(newContext(s, nil, nil)); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("topic delete without params: got %v, want ErrInvalidInput", err)
	}
	for _, req := range mock.seen() {
		if req.Method != http.MethodGet {
			t.Fatalf("an unaddressed mutation reached the broker: %s %s", req.Method, req.Path)
		}
	}
	if _, err := listNamespaces(newContext(s, nil, nil)); err != nil {
		t.Fatalf("listing must still use the connection scope: %v", err)
	}
	if !mock.sawPath("/admin/v2/namespaces/public") {
		t.Fatalf("the default tenant was not used for the listing: %#v", mock.seen())
	}
}

func TestAdminErrorsMapToSentinels(t *testing.T) {
	cases := []struct {
		status int
		want   error
	}{
		{http.StatusBadRequest, plugin.ErrInvalidInput},
		{http.StatusPreconditionFailed, plugin.ErrInvalidInput},
		{http.StatusUnauthorized, plugin.ErrUnauthorized},
		{http.StatusForbidden, plugin.ErrForbidden},
		{http.StatusNotFound, plugin.ErrNotFound},
		{http.StatusMethodNotAllowed, plugin.ErrNotSupported},
		{http.StatusConflict, plugin.ErrConflict},
		{http.StatusInternalServerError, plugin.ErrUnavailable},
	}
	for _, tc := range cases {
		err := httpError(tc.status, []byte(`{"reason":"boom"}`))
		if !errors.Is(err, tc.want) {
			t.Fatalf("status %d: got %v, want %v", tc.status, err, tc.want)
		}
		if !strings.Contains(err.Error(), "boom") {
			t.Fatalf("status %d dropped the broker reason: %v", tc.status, err)
		}
	}
}

func TestHealthCheckToleratesANonSuperuserToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"reason":"Don't have admin permission"}`))
	}))
	defer srv.Close()
	s := &Session{http: srv.Client(), opts: options{AdminURL: srv.URL, Timeout: time.Second}}
	if err := s.HealthCheck(context.Background()); err != nil {
		t.Fatalf("a tenant-scoped token must still connect: %v", err)
	}
}

func TestTreeNavigatesTenantsToNamespacesToTopics(t *testing.T) {
	mock := newAdminMock(t)
	s := testSession(mock)

	tenants, err := tenantsTree(newContext(s, nil, nil))
	if err != nil {
		t.Fatalf("tenantsTree: %v", err)
	}
	tenantNodes := tenants.(plugin.Page[plugin.TreeNode])
	if len(tenantNodes.Items) == 0 || tenantNodes.Items[0].ChildrenSource == nil {
		t.Fatalf("tenant nodes must expand into namespaces: %#v", tenantNodes.Items)
	}
	if tenantNodes.Items[0].ChildrenSource.RouteID != "pulsar.namespaces.tree" {
		t.Fatalf("tenant children source: %#v", tenantNodes.Items[0].ChildrenSource)
	}

	namespaces, err := namespacesTree(newContext(s, map[string]string{"tenant": "public"}, nil))
	if err != nil {
		t.Fatalf("namespacesTree: %v", err)
	}
	nsNodes := namespaces.(plugin.Page[plugin.TreeNode])
	if len(nsNodes.Items) == 0 || nsNodes.Items[0].ChildrenSource == nil || nsNodes.Items[0].Ref == nil {
		t.Fatalf("namespace nodes must expand and open a detail: %#v", nsNodes.Items)
	}

	topics, err := topicsTree(newContext(s, map[string]string{"tenant": "public", "namespace": "default"}, nil))
	if err != nil {
		t.Fatalf("topicsTree: %v", err)
	}
	topicNodes := topics.(plugin.Page[plugin.TreeNode])
	if len(topicNodes.Items) == 0 {
		t.Fatal("no topic nodes")
	}
	for _, node := range topicNodes.Items {
		if !node.Leaf || node.Ref == nil || node.Ref.Kind != "topic" {
			t.Fatalf("topic node: %#v", node)
		}
	}
	// The tree must not pay for per-row detail it never renders.
	if got := mock.statsCalls.Load(); got != 0 {
		t.Fatalf("the topic tree made %d stats round trips", got)
	}
	for _, req := range mock.seen() {
		switch req.Path {
		case "/admin/v2/tenants/public", "/admin/v2/tenants/acme",
			"/admin/v2/namespaces/public/default", "/admin/v2/namespaces/public/events":
			t.Fatalf("the tree read per-row detail it does not render: %s", req.Path)
		}
	}
}

func TestListingsStillEnrichWhatTheTreeSkips(t *testing.T) {
	mock := newAdminMock(t)
	s := testSession(mock)

	tenants := callPage(t, listTenants, s, nil, nil)
	if len(tenants.Items) != 2 {
		t.Fatalf("tenants: %#v", tenants.Items)
	}
	for _, item := range tenants.Items {
		if item["allowed_clusters"] == nil {
			t.Fatalf("the tenant table needs its detail columns: %#v", item)
		}
	}
	namespaces := callPage(t, listNamespaces, s, map[string]string{"tenant": "public"}, nil)
	if len(namespaces.Items) == 0 || namespaces.Items[0]["bundles"] != 4 {
		t.Fatalf("the namespace table needs its policy columns: %#v", namespaces.Items)
	}
}

func TestMessagePanelsAreGatedOnABrowsableTopic(t *testing.T) {
	gated := map[string]bool{"topic-messages": false, "subscription-messages": false}
	for _, res := range New().Manifest().Resources {
		for _, panel := range res.Detail.Tabs {
			if _, wanted := gated[panel.Key]; !wanted {
				continue
			}
			if panel.VisibleWhen == nil || len(panel.VisibleWhen.AllOf) != 2 {
				t.Fatalf("panel %q must be hidden for topics the broker cannot peek: %#v", panel.Key, panel.VisibleWhen)
			}
			gated[panel.Key] = true
		}
	}
	for key, seen := range gated {
		if !seen {
			t.Fatalf("panel %q is missing", key)
		}
	}
}

func TestConsumeStreamValidatesItsStartPosition(t *testing.T) {
	s := testSession(newAdminMock(t))
	params := map[string]string{"topic": "persistent://public/default/orders", "position": "middle"}
	err := consumeStream(newContext(s, params, nil), &nullStream{ctx: context.Background()})
	if !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("got %v, want ErrInvalidInput", err)
	}
	// Every option the control offers must be accepted; without a broker the
	// tail can only get as far as the transport failure.
	for _, option := range positionOptions() {
		params["position"] = fmt.Sprint(option.Value)
		if err := consumeStream(newContext(s, params, nil), &nullStream{ctx: context.Background()}); !errors.Is(err, plugin.ErrUnavailable) {
			t.Fatalf("position %v: got %v, want ErrUnavailable", option.Value, err)
		}
	}
}

func TestEveryDeclaredActionIsReachable(t *testing.T) {
	m := New().Manifest()
	declared := map[string]bool{}
	for _, action := range m.Actions {
		declared[action.ID] = true
	}
	used := map[string]bool{}
	note := func(ids []string) {
		for _, id := range ids {
			used[id] = true
		}
	}
	for _, res := range m.Resources {
		note(res.Actions.Toolbar)
		note(res.Actions.Row)
		note(res.Actions.Detail)
		for _, panel := range res.Detail.Tabs {
			if table, ok := panel.Config.(plugin.TableConfig); ok {
				note(table.ActionIDs)
				note(table.RowActionIDs)
			}
		}
	}
	for id := range declared {
		if !used[id] {
			t.Errorf("action %q is declared but no surface references it", id)
		}
	}
	for id := range used {
		if !declared[id] {
			t.Errorf("surface references undeclared action %q", id)
		}
	}
}

func TestEveryRouteIsExercised(t *testing.T) {
	mock := newAdminMock(t)
	s := testSession(mock)
	h := plugintest.NewHarness(t, routes())
	ctx := context.Background()

	topic := map[string]string{"topic": "persistent://public/default/orders"}
	namespace := map[string]string{"tenant": "public", "namespace": "default"}
	subscription := map[string]string{"topic": "persistent://public/default/orders", "subscription": "billing"}

	h.Call(ctx, "pulsar.overview", s, nil, nil, nil)
	h.Call(ctx, "pulsar.tenants.tree", s, nil, nil, nil)
	h.Call(ctx, "pulsar.namespaces.tree", s, map[string]string{"tenant": "public"}, nil, nil)
	h.Call(ctx, "pulsar.topics.tree", s, namespace, nil, nil)

	h.Call(ctx, "pulsar.tenants.list", s, nil, nil, nil)
	h.Call(ctx, "pulsar.tenant.read", s, map[string]string{"tenant": "public"}, nil, nil)
	h.Call(ctx, "pulsar.tenant.create", s, nil, nil, []byte(`{"name":"acme","allowed_clusters":["standalone"]}`))
	h.Call(ctx, "pulsar.tenant.update", s, map[string]string{"tenant": "public"}, nil, []byte(`{"allowed_clusters":["standalone"]}`))
	h.Call(ctx, "pulsar.tenant.delete", s, map[string]string{"tenant": "acme"}, nil, nil)

	h.Call(ctx, "pulsar.namespaces.list", s, map[string]string{"tenant": "public"}, nil, nil)
	h.Call(ctx, "pulsar.namespace.policies", s, namespace, nil, nil)
	h.Call(ctx, "pulsar.namespace.create", s, map[string]string{"tenant": "public"}, nil, []byte(`{"name":"events","bundles":4}`))
	h.Call(ctx, "pulsar.namespace.retention", s, namespace, nil, []byte(`{"time_minutes":60,"size_mb":512}`))
	h.Call(ctx, "pulsar.namespace.unload", s, namespace, nil, nil)
	h.Call(ctx, "pulsar.namespace.delete", s, namespace, nil, nil)

	h.Call(ctx, "pulsar.topics.list", s, namespace, nil, nil)
	h.Call(ctx, "pulsar.topic.stats", s, topic, nil, nil)
	h.Call(ctx, "pulsar.topic.internal_stats", s, topic, nil, nil)
	h.Call(ctx, "pulsar.topic.partitions", s, map[string]string{"topic": "persistent://public/default/events"}, nil, nil)
	h.Call(ctx, "pulsar.topic.schema", s, topic, nil, nil)
	h.Call(ctx, "pulsar.topic.create", s, namespace, nil, []byte(`{"name":"created","domain":"persistent","partitions":3}`))
	h.Call(ctx, "pulsar.topic.compact", s, topic, nil, nil)
	h.Call(ctx, "pulsar.topic.unload", s, topic, nil, nil)
	h.Call(ctx, "pulsar.topic.terminate", s, topic, nil, nil)
	h.Call(ctx, "pulsar.topic.delete", s, topic, nil, nil)
	h.Call(ctx, "pulsar.topic.schema.delete", s, topic, nil, nil)

	h.Call(ctx, "pulsar.subscriptions.list", s, topic, nil, nil)
	h.Call(ctx, "pulsar.subscription.read", s, subscription, nil, nil)
	h.Call(ctx, "pulsar.producers.list", s, topic, nil, nil)
	h.Call(ctx, "pulsar.consumers.list", s, topic, nil, nil)
	h.Call(ctx, "pulsar.subscription.create", s, topic, nil, []byte(`{"name":"audit","position":"earliest"}`))
	h.Call(ctx, "pulsar.subscription.skip", s, subscription, nil, []byte(`{"count":5}`))
	h.Call(ctx, "pulsar.subscription.clear", s, subscription, nil, nil)
	h.Call(ctx, "pulsar.subscription.reset", s, subscription, nil, []byte(`{"target":"latest"}`))
	h.Call(ctx, "pulsar.subscription.expire", s, subscription, nil, []byte(`{"age":"24h"}`))
	h.Call(ctx, "pulsar.subscription.delete", s, subscription, nil, nil)

	h.Call(ctx, "pulsar.messages.list", s, topic, url.Values{"limit": {"2"}}, nil)
	h.Call(ctx, "pulsar.messages.positions", s, nil, nil, nil)

	h.Call(ctx, "pulsar.clusters.list", s, nil, nil, nil)
	h.Call(ctx, "pulsar.cluster.read", s, map[string]string{"cluster": "standalone"}, nil, nil)
	h.Call(ctx, "pulsar.brokers.list", s, nil, nil, nil)
	h.Call(ctx, "pulsar.broker.read", s, map[string]string{"cluster": "standalone", "broker": "localhost:8080"}, nil, nil)

	// Produce and live tail need the binary protocol; the session points at a
	// closed port, so both must surface the transport failure rather than hang.
	produce := h.Route("pulsar.message.produce")
	if _, err := produce.Handle(newBodyContext(s, topic, `{"payload":"hello","encoding":"string"}`)); !errors.Is(err, plugin.ErrUnavailable) {
		t.Fatalf("produce without a broker: got %v, want ErrUnavailable", err)
	}
	consume := h.Route("pulsar.messages.consume")
	if consume.Stream == nil {
		t.Fatal("the consume route must declare a stream handler")
	}
	if err := consume.Stream(newContext(s, topic, nil), &nullStream{ctx: ctx}); !errors.Is(err, plugin.ErrUnavailable) {
		t.Fatalf("tail without a broker: got %v, want ErrUnavailable", err)
	}

	h.AssertAllCovered()
}

// --- helpers -------------------------------------------------------------

func testSession(mock *adminMock) *Session {
	return &Session{
		http: mock.Client(),
		opts: options{
			AdminURL: mock.URL,
			// A closed port: nothing in the unit suite may reach a live broker.
			ServiceURL:   "pulsar://127.0.0.1:1",
			Tenant:       "public",
			Namespace:    "default",
			Timeout:      2 * time.Second,
			MessageLimit: 25,
		},
	}
}

func newContext(s *Session, params map[string]string, query url.Values) *plugin.RequestContext {
	return plugin.NewRequestContext(context.Background(), plugin.User{}, s, params, query, nil)
}

func newBodyContext(s *Session, params map[string]string, body string) *plugin.RequestContext {
	return plugin.NewRequestContext(context.Background(), plugin.User{}, s, params, nil, []byte(body))
}

func callPage(t *testing.T, handler func(*plugin.RequestContext) (any, error), s *Session, params map[string]string, query url.Values) plugin.Page[row] {
	t.Helper()
	out, err := handler(newContext(s, params, query))
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	page, ok := out.(plugin.Page[row])
	if !ok {
		t.Fatalf("handler returned %T, want plugin.Page[row]", out)
	}
	return page
}

type nullStream struct {
	ctx context.Context
}

func (s *nullStream) Read([]byte) (int, error)    { <-s.ctx.Done(); return 0, s.ctx.Err() }
func (s *nullStream) Write(p []byte) (int, error) { return len(p), nil }
func (s *nullStream) Close() error                { return nil }
func (s *nullStream) Context() context.Context    { return s.ctx }

type recordedRequest struct {
	Method string
	Path   string
	Query  url.Values
	Body   string
}

type adminMock struct {
	*httptest.Server

	topics      []string
	partitioned []string
	// configuredPartitions overrides the partition count the aggregate stats
	// document reports, so a listing that has not seen every partition yet can
	// be told apart from the configured value.
	configuredPartitions int
	messages             int
	schema               bool

	statsCalls atomic.Int64

	mu       sync.Mutex
	requests []recordedRequest
}

func newAdminMock(t *testing.T) *adminMock {
	t.Helper()
	m := &adminMock{
		topics: []string{
			"persistent://public/default/orders",
			"persistent://public/default/events-partition-0",
			"persistent://public/default/events-partition-1",
			"non-persistent://public/default/telemetry",
		},
		partitioned: []string{"persistent://public/default/events"},
		messages:    50,
		schema:      true,
	}
	m.Server = httptest.NewServer(m.record(m.mux()))
	t.Cleanup(m.Close)
	return m
}

func (m *adminMock) record(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := ""
		if r.Body != nil {
			raw := make([]byte, 4096)
			n, _ := r.Body.Read(raw)
			body = string(raw[:n])
		}
		m.mu.Lock()
		m.requests = append(m.requests, recordedRequest{Method: r.Method, Path: r.URL.Path, Query: r.URL.Query(), Body: body})
		m.mu.Unlock()
		next.ServeHTTP(w, r)
	})
}

func (m *adminMock) seen() []recordedRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]recordedRequest(nil), m.requests...)
}

func (m *adminMock) sawPath(path string) bool {
	for _, req := range m.seen() {
		if req.Path == path {
			return true
		}
	}
	return false
}

func (m *adminMock) sawPathPrefix(prefix string) bool {
	for _, req := range m.seen() {
		if strings.HasPrefix(req.Path, prefix) {
			return true
		}
	}
	return false
}

func (m *adminMock) lastBody(path string) string {
	out := ""
	for _, req := range m.seen() {
		if req.Path == path {
			out = req.Body
		}
	}
	return out
}

func (m *adminMock) examinePositions() []string {
	out := []string{}
	for _, req := range m.seen() {
		if strings.HasSuffix(req.Path, "/examinemessage") {
			out = append(out, req.Query.Get("messagePosition"))
		}
	}
	return out
}

func (m *adminMock) mux() http.Handler {
	mux := http.NewServeMux()
	writeJSON := func(w http.ResponseWriter, value any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(value)
	}
	noContent := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }

	mux.HandleFunc("GET /admin/v2/clusters", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, []string{"standalone"})
	})
	mux.HandleFunc("GET /admin/v2/clusters/{cluster}", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"serviceUrl": "http://localhost:8080", "brokerServiceUrl": "pulsar://localhost:6650"})
	})
	mux.HandleFunc("GET /admin/v2/brokers", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, []string{"localhost:8080"})
	})
	mux.HandleFunc("GET /admin/v2/brokers/version", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("4.0.0"))
	})
	mux.HandleFunc("GET /admin/v2/brokers/leaderBroker", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]any{"brokerId": "localhost:8080"})
	})
	mux.HandleFunc("GET /admin/v2/brokers/{cluster}", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, []string{"localhost:8080"})
	})
	mux.HandleFunc("GET /admin/v2/brokers/{cluster}/{broker}/ownedNamespaces", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]any{"public/default/0x00000000_0xffffffff": map[string]any{"broker_assignment": "shared"}})
	})

	mux.HandleFunc("GET /admin/v2/tenants", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, []string{"public", "acme"})
	})
	mux.HandleFunc("GET /admin/v2/tenants/{tenant}", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]any{"adminRoles": []string{"ops"}, "allowedClusters": []string{"standalone"}})
	})
	mux.HandleFunc("PUT /admin/v2/tenants/{tenant}", noContent)
	mux.HandleFunc("POST /admin/v2/tenants/{tenant}", noContent)
	mux.HandleFunc("DELETE /admin/v2/tenants/{tenant}", noContent)

	mux.HandleFunc("GET /admin/v2/namespaces/{tenant}", func(w http.ResponseWriter, r *http.Request) {
		tenant := r.PathValue("tenant")
		writeJSON(w, []string{tenant + "/default", tenant + "/events"})
	})
	mux.HandleFunc("GET /admin/v2/namespaces/{tenant}/{namespace}", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]any{
			"message_ttl_in_seconds": 300,
			"replication_clusters":   []string{"standalone"},
			"bundles":                map[string]any{"numBundles": 4},
			"retention_policies":     map[string]any{"retentionTimeInMinutes": 60, "retentionSizeInMB": 512},
		})
	})
	mux.HandleFunc("PUT /admin/v2/namespaces/{tenant}/{namespace}", noContent)
	mux.HandleFunc("DELETE /admin/v2/namespaces/{tenant}/{namespace}", noContent)
	mux.HandleFunc("PUT /admin/v2/namespaces/{tenant}/{namespace}/unload", noContent)
	mux.HandleFunc("POST /admin/v2/namespaces/{tenant}/{namespace}/retention", noContent)
	mux.HandleFunc("GET /admin/v2/namespaces/{tenant}/{namespace}/topics", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, m.topics)
	})

	mux.HandleFunc("GET /admin/v2/schemas/{tenant}/{namespace}/{topic}/schema", func(w http.ResponseWriter, _ *http.Request) {
		if !m.schema {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"reason":"Schema not found"}`))
			return
		}
		writeJSON(w, map[string]any{"version": 3, "type": "AVRO", "data": `{"type":"record"}`, "properties": map[string]string{}})
	})
	mux.HandleFunc("DELETE /admin/v2/schemas/{tenant}/{namespace}/{topic}/schema", noContent)

	mux.HandleFunc("GET /admin/v2/{domain}/{tenant}/{namespace}/partitioned", func(w http.ResponseWriter, r *http.Request) {
		names := []string{}
		for _, name := range m.partitioned {
			if strings.HasPrefix(name, r.PathValue("domain")+"://") {
				names = append(names, name)
			}
		}
		writeJSON(w, names)
	})
	mux.HandleFunc("GET /admin/v2/{domain}/{tenant}/{namespace}/{topic}/partitions", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"partitions": m.partitionsOf(r)})
	})
	mux.HandleFunc("GET /admin/v2/{domain}/{tenant}/{namespace}/{topic}/stats", func(w http.ResponseWriter, r *http.Request) {
		m.statsCalls.Add(1)
		writeJSON(w, m.statsDoc(r.PathValue("topic")))
	})
	mux.HandleFunc("GET /admin/v2/{domain}/{tenant}/{namespace}/{topic}/partitioned-stats", func(w http.ResponseWriter, r *http.Request) {
		m.statsCalls.Add(1)
		doc := m.statsDoc(r.PathValue("topic"))
		configured := m.partitionsOf(r)
		if m.configuredPartitions > 0 {
			configured = m.configuredPartitions
		}
		doc["metadata"] = map[string]any{"partitions": configured}
		base := r.PathValue("domain") + "://" + r.PathValue("tenant") + "/" + r.PathValue("namespace") + "/" + r.PathValue("topic")
		aggregated := []any{}
		partitions := map[string]any{}
		for i := range m.partitionsOf(r) {
			publisher := map[string]any{"producerName": fmt.Sprintf("writer-%d", i), "producerId": i}
			part := m.statsDoc(r.PathValue("topic"))
			part["publishers"] = []any{publisher}
			partitions[fmt.Sprintf("%s-partition-%d", base, i)] = part
			aggregated = append(aggregated, publisher)
		}
		// The broker merges every partition's publishers into the aggregate list,
		// so a reader that also walks the partitions would count each one twice.
		doc["publishers"] = aggregated
		if r.URL.Query().Get("perPartition") == "true" {
			doc["partitions"] = partitions
		}
		writeJSON(w, doc)
	})
	mux.HandleFunc("GET /admin/v2/{domain}/{tenant}/{namespace}/{topic}/internalStats", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]any{"entriesAddedCounter": 12, "numberOfEntries": 12, "totalSize": 4096, "state": "LedgerOpened"})
	})
	mux.HandleFunc("GET /admin/v2/{domain}/{tenant}/{namespace}/{topic}/examinemessage", func(w http.ResponseWriter, r *http.Request) {
		// The broker refuses to examine a partitioned topic; only a partition of
		// it holds messages.
		if m.partitionsOf(r) > 0 {
			w.WriteHeader(http.StatusMethodNotAllowed)
			_, _ = w.Write([]byte(`{"reason":"Examine messages on a partitioned topic is not allowed"}`))
			return
		}
		m.writeMessage(w, r.URL.Query().Get("messagePosition"))
	})
	mux.HandleFunc("GET /admin/v2/{domain}/{tenant}/{namespace}/{topic}/subscription/{sub}/position/{position}", func(w http.ResponseWriter, r *http.Request) {
		m.writeMessage(w, r.PathValue("position"))
	})

	mux.HandleFunc("PUT /admin/v2/{domain}/{tenant}/{namespace}/{topic}", noContent)
	mux.HandleFunc("DELETE /admin/v2/{domain}/{tenant}/{namespace}/{topic}", noContent)
	mux.HandleFunc("PUT /admin/v2/{domain}/{tenant}/{namespace}/{topic}/partitions", noContent)
	mux.HandleFunc("DELETE /admin/v2/{domain}/{tenant}/{namespace}/{topic}/partitions", noContent)
	mux.HandleFunc("PUT /admin/v2/{domain}/{tenant}/{namespace}/{topic}/compaction", noContent)
	mux.HandleFunc("PUT /admin/v2/{domain}/{tenant}/{namespace}/{topic}/unload", noContent)
	mux.HandleFunc("POST /admin/v2/{domain}/{tenant}/{namespace}/{topic}/terminate", func(w http.ResponseWriter, r *http.Request) {
		if m.partitionsOf(r) > 0 {
			w.WriteHeader(http.StatusMethodNotAllowed)
			_, _ = w.Write([]byte(`{"reason":"Termination of a partitioned topic is not allowed"}`))
			return
		}
		writeJSON(w, map[string]any{"ledgerId": 5, "entryId": 41})
	})
	mux.HandleFunc("POST /admin/v2/{domain}/{tenant}/{namespace}/{topic}/terminate/partitions", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]any{"persistent://public/default/events-partition-0": map[string]any{"ledgerId": 5, "entryId": 41}})
	})
	mux.HandleFunc("PUT /admin/v2/{domain}/{tenant}/{namespace}/{topic}/subscription/{sub}", noContent)
	mux.HandleFunc("DELETE /admin/v2/{domain}/{tenant}/{namespace}/{topic}/subscription/{sub}", noContent)
	mux.HandleFunc("POST /admin/v2/{domain}/{tenant}/{namespace}/{topic}/subscription/{sub}/skip/{count}", noContent)
	mux.HandleFunc("POST /admin/v2/{domain}/{tenant}/{namespace}/{topic}/subscription/{sub}/skip_all", noContent)
	mux.HandleFunc("POST /admin/v2/{domain}/{tenant}/{namespace}/{topic}/subscription/{sub}/resetcursor", noContent)
	mux.HandleFunc("POST /admin/v2/{domain}/{tenant}/{namespace}/{topic}/subscription/{sub}/resetcursor/{timestamp}", noContent)
	mux.HandleFunc("POST /admin/v2/{domain}/{tenant}/{namespace}/{topic}/subscription/{sub}/expireMessages/{seconds}", noContent)
	return mux
}

func (m *adminMock) partitionsOf(r *http.Request) int {
	name := r.PathValue("domain") + "://" + r.PathValue("tenant") + "/" + r.PathValue("namespace") + "/" + r.PathValue("topic")
	parent := false
	for _, partitioned := range m.partitioned {
		if partitioned == name {
			parent = true
		}
	}
	if !parent {
		return 0
	}
	count := 0
	for _, topic := range m.topics {
		if strings.HasPrefix(topic, name+"-partition-") {
			count++
		}
	}
	return count
}

func (m *adminMock) statsDoc(topic string) map[string]any {
	return map[string]any{
		"msgRateIn":    1.5,
		"msgRateOut":   2.5,
		"msgInCounter": 120,
		"storageSize":  4096,
		"backlogSize":  128,
		"ownerBroker":  "localhost:8080",
		"publishers": []any{
			map[string]any{
				"producerName": topic + "-writer", "producerId": 1, "address": "10.0.0.1:5000",
				"clientVersion": "Pulsar-Go", "msgRateIn": 1.5, "connectedSince": "2026-07-27T09:00:00.000+0000",
			},
		},
		"subscriptions": map[string]any{
			"billing": map[string]any{
				"type": "Shared", "msgBacklog": 7, "unackedMessages": 2, "msgRateOut": 2.5, "isDurable": true,
				"lastConsumedTimestamp": time.Now().UnixMilli(),
				"consumers": []any{
					map[string]any{"consumerName": "billing-1", "address": "10.0.0.2:5001", "availablePermits": 900},
				},
			},
			"audit": map[string]any{
				"type": "Exclusive", "msgBacklog": 0, "isDurable": true,
				"consumers": []any{
					map[string]any{"consumerName": "audit-1", "address": "10.0.0.3:5002"},
				},
			},
		},
	}
}

func (m *adminMock) writeMessage(w http.ResponseWriter, position string) {
	index, err := strconv.Atoi(position)
	if err != nil || index < 1 {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"reason":"messagePosition must be a positive integer"}`))
		return
	}
	if index > m.messages {
		// Pulsar answers a position past the last message with a 500 that wraps
		// BookKeeper's IncorrectParameter (-14), not a 404.
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"reason":"Incorrect parameter input error code: -14"}`))
		return
	}
	w.Header().Set("X-Pulsar-Message-ID", fmt.Sprintf("7:%d:-1", index))
	w.Header().Set("X-Pulsar-publish-time", "2026-07-27T09:00:00.000+0000")
	w.Header().Set("X-Pulsar-producer-name", "orders-writer")
	w.Header().Set("X-Pulsar-PROPERTY", `{"seq":"`+position+`"}`)
	_, _ = w.Write([]byte("payload-" + position))
}
