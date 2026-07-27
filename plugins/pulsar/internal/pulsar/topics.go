package pulsar

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	persistentDomain    = "persistent"
	nonPersistentDomain = "non-persistent"

	positionLatest   = "latest"
	positionEarliest = "earliest"
)

// partitionSuffix matches the per-partition topics a partitioned topic expands
// into. The namespace listing returns those instead of the parent name.
var partitionSuffix = regexp.MustCompile(`^(.+)-partition-(\d+)$`)

func positionOptions() []plugin.Option {
	return []plugin.Option{
		{Label: "Latest", Value: positionLatest},
		{Label: "Earliest", Value: positionEarliest},
	}
}

type topicRef struct {
	Domain    string
	Tenant    string
	Namespace string
	Name      string
}

func (t topicRef) String() string {
	return t.Domain + "://" + t.Tenant + "/" + t.Namespace + "/" + t.Name
}

func (t topicRef) path() string {
	return "/admin/v2/" + t.Domain + "/" + url.PathEscape(t.Tenant) + "/" + url.PathEscape(t.Namespace) + "/" + url.PathEscape(t.Name)
}

func (t topicRef) schemaPath() string {
	return "/admin/v2/schemas/" + url.PathEscape(t.Tenant) + "/" + url.PathEscape(t.Namespace) + "/" + url.PathEscape(t.Name) + "/schema"
}

func (t topicRef) identity() plugin.ResourceIdentity {
	return plugin.ResourceIdentity{Kind: "topic", Scope: t.Domain, Namespace: t.Tenant + "/" + t.Namespace, Name: t.Name, UID: t.String()}
}

func parseTopic(raw string) (topicRef, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return topicRef{}, fmt.Errorf("%w: topic is required", plugin.ErrInvalidInput)
	}
	domain := persistentDomain
	if scheme, rest, ok := strings.Cut(value, "://"); ok {
		switch scheme {
		case persistentDomain, nonPersistentDomain:
			domain, value = scheme, rest
		default:
			return topicRef{}, fmt.Errorf("%w: unsupported topic domain %q", plugin.ErrInvalidInput, scheme)
		}
	}
	parts := strings.Split(value, "/")
	if len(parts) != 3 {
		return topicRef{}, fmt.Errorf("%w: topic %q must be tenant/namespace/name", plugin.ErrInvalidInput, raw)
	}
	tenant, err := simpleName("tenant", parts[0])
	if err != nil {
		return topicRef{}, err
	}
	namespace, err := simpleName("namespace", parts[1])
	if err != nil {
		return topicRef{}, err
	}
	name, err := simpleName("topic", parts[2])
	if err != nil {
		return topicRef{}, err
	}
	return topicRef{Domain: domain, Tenant: tenant, Namespace: namespace, Name: name}, nil
}

func topicScope(rc *plugin.RequestContext) (*Session, topicRef, error) {
	s, err := pulsarSession(rc)
	if err != nil {
		return nil, topicRef{}, err
	}
	ref, err := parseTopic(param(rc, "topic"))
	if err != nil {
		return nil, topicRef{}, err
	}
	return s, ref, nil
}

func writableTopic(rc *plugin.RequestContext) (*Session, topicRef, error) {
	s, err := writableSession(rc)
	if err != nil {
		return nil, topicRef{}, err
	}
	ref, err := parseTopic(param(rc, "topic"))
	if err != nil {
		return nil, topicRef{}, err
	}
	return s, ref, nil
}

type topicStatsDoc struct {
	MsgRateIn        float64                      `json:"msgRateIn"`
	MsgRateOut       float64                      `json:"msgRateOut"`
	MsgThroughputIn  float64                      `json:"msgThroughputIn"`
	MsgThroughputOut float64                      `json:"msgThroughputOut"`
	MsgInCounter     int64                        `json:"msgInCounter"`
	MsgOutCounter    int64                        `json:"msgOutCounter"`
	AverageMsgSize   float64                      `json:"averageMsgSize"`
	StorageSize      int64                        `json:"storageSize"`
	BacklogSize      int64                        `json:"backlogSize"`
	OwnerBroker      string                       `json:"ownerBroker"`
	Publishers       []publisherStatsDoc          `json:"publishers"`
	Subscriptions    map[string]subscriptionStats `json:"subscriptions"`
	Partitions       map[string]topicStatsDoc     `json:"partitions"`
	Metadata         *partitionedTopicMetadata    `json:"metadata"`
}

type partitionedTopicMetadata struct {
	Partitions int `json:"partitions"`
}

type publisherStatsDoc struct {
	ProducerID      int64   `json:"producerId"`
	ProducerName    string  `json:"producerName"`
	Address         string  `json:"address"`
	ConnectedSince  string  `json:"connectedSince"`
	ClientVersion   string  `json:"clientVersion"`
	AccessMode      string  `json:"accessMode"`
	MsgRateIn       float64 `json:"msgRateIn"`
	MsgThroughputIn float64 `json:"msgThroughputIn"`
	AverageMsgSize  float64 `json:"averageMsgSize"`
}

type subscriptionStats struct {
	Type                  string             `json:"type"`
	MsgBacklog            int64              `json:"msgBacklog"`
	MsgBacklogNoDelayed   int64              `json:"msgBacklogNoDelayed"`
	MsgDelayed            int64              `json:"msgDelayed"`
	MsgRateOut            float64            `json:"msgRateOut"`
	MsgThroughputOut      float64            `json:"msgThroughputOut"`
	MsgRateRedeliver      float64            `json:"msgRateRedeliver"`
	UnackedMessages       int64              `json:"unackedMessages"`
	BlockedOnUnacked      bool               `json:"blockedSubscriptionOnUnackedMsgs"`
	Durable               bool               `json:"isDurable"`
	Replicated            bool               `json:"isReplicated"`
	ActiveConsumerName    string             `json:"activeConsumerName"`
	TotalMsgExpired       int64              `json:"totalMsgExpired"`
	LastConsumedTimestamp int64              `json:"lastConsumedTimestamp"`
	LastAckedTimestamp    int64              `json:"lastAckedTimestamp"`
	LastExpireTimestamp   int64              `json:"lastExpireTimestamp"`
	Consumers             []consumerStatsDoc `json:"consumers"`
}

type consumerStatsDoc struct {
	ConsumerName          string            `json:"consumerName"`
	Address               string            `json:"address"`
	ConnectedSince        string            `json:"connectedSince"`
	ClientVersion         string            `json:"clientVersion"`
	MsgRateOut            float64           `json:"msgRateOut"`
	MsgThroughputOut      float64           `json:"msgThroughputOut"`
	MsgRateRedeliver      float64           `json:"msgRateRedeliver"`
	AvailablePermits      int               `json:"availablePermits"`
	UnackedMessages       int64             `json:"unackedMessages"`
	BlockedOnUnacked      bool              `json:"blockedConsumerOnUnackedMsgs"`
	LastConsumedTimestamp int64             `json:"lastConsumedTimestamp"`
	LastAckedTimestamp    int64             `json:"lastAckedTimestamp"`
	Metadata              map[string]string `json:"metadata"`
}

// partitionCount reports how many partitions a topic has, 0 for a
// non-partitioned topic. It picks the stats endpoint: a partitioned topic has
// no /stats of its own, only aggregated /partitioned-stats.
func (s *Session) partitionCount(ctx context.Context, t topicRef) (int, error) {
	var meta partitionedTopicMetadata
	if err := s.get(ctx, t.path()+"/partitions", nil, &meta); err != nil {
		return 0, err
	}
	return meta.Partitions, nil
}

func statsEndpoint(t topicRef, partitioned, perPartition bool) (string, url.Values) {
	if partitioned {
		return t.path() + "/partitioned-stats", url.Values{"perPartition": {strconv.FormatBool(perPartition)}}
	}
	return t.path() + "/stats", nil
}

func (s *Session) topicStats(ctx context.Context, t topicRef, perPartition bool) (topicStatsDoc, int, error) {
	partitions, err := s.partitionCount(ctx, t)
	if err != nil {
		return topicStatsDoc{}, 0, err
	}
	path, query := statsEndpoint(t, partitions > 0, perPartition)
	var out topicStatsDoc
	if err := s.get(ctx, path, query, &out); err != nil {
		return topicStatsDoc{}, partitions, err
	}
	return out, partitions, nil
}

// topicsPage lists one namespace's topics. The namespace listing returns every
// partition of a partitioned topic and never the parent, so partitions are
// folded back into their parent before the page is sliced — otherwise the row
// count, the filter, and the cursor would all be computed over the wrong set.
func topicsPage(rc *plugin.RequestContext, withStats bool) (plugin.Page[row], error) {
	s, tenant, namespace, err := namespaceScope(rc)
	if err != nil {
		return plugin.Page[row]{}, err
	}
	query := url.Values{"mode": {"ALL"}}
	if s.opts.SystemTopics {
		query.Set("includeSystemTopic", "true")
	}
	var names []string
	if err := s.get(rc.Ctx, namespacePath(tenant, namespace)+"/topics", query, &names); err != nil {
		return plugin.Page[row]{}, err
	}
	parents, err := s.partitionedTopics(rc.Ctx, tenant, namespace, s.opts.SystemTopics)
	if err != nil {
		return plugin.Page[row]{}, err
	}
	counts := map[string]int{}
	plain := make([]topicRef, 0, len(names))
	for _, name := range names {
		ref, err := parseTopic(name)
		if err != nil {
			continue
		}
		if match := partitionSuffix.FindStringSubmatch(ref.Name); match != nil {
			parent := topicRef{Domain: ref.Domain, Tenant: ref.Tenant, Namespace: ref.Namespace, Name: match[1]}
			if _, ok := parents[parent.String()]; ok {
				counts[parent.String()]++
				continue
			}
		}
		plain = append(plain, ref)
	}
	rows := make([]row, 0, len(plain)+len(parents))
	for _, ref := range plain {
		rows = append(rows, topicRow(ref, false, 0))
	}
	for key, ref := range parents {
		rows = append(rows, topicRow(ref, true, counts[key]))
	}
	sort.Slice(rows, func(i, j int) bool { return fmt.Sprint(rows[i]["topic"]) < fmt.Sprint(rows[j]["topic"]) })
	page, err := broker.PageRows(rc, rows)
	if err != nil {
		return plugin.Page[row]{}, err
	}
	if !withStats {
		return page, nil
	}
	enrichRows(rc.Ctx, page.Items, func(ctx context.Context, r row) {
		ref, err := parseTopic(fmt.Sprint(r["topic"]))
		if err != nil {
			return
		}
		path, query := statsEndpoint(ref, r["partitioned"] == true, false)
		var stats topicStatsDoc
		if err := s.get(ctx, path, query, &stats); err != nil {
			return
		}
		// The namespace listing only reveals the partitions that already exist;
		// the aggregate document carries the configured count.
		if stats.Metadata != nil && stats.Metadata.Partitions > 0 {
			r["partitions"] = stats.Metadata.Partitions
		}
		r["producers"] = len(stats.Publishers)
		r["subscriptions"] = len(stats.Subscriptions)
		r["msg_rate_in"], r["msg_rate_out"] = stats.MsgRateIn, stats.MsgRateOut
		r["msg_in_counter"], r["msg_out_counter"] = stats.MsgInCounter, stats.MsgOutCounter
		r["storage_size"], r["backlog_size"] = stats.StorageSize, stats.BacklogSize
	})
	return page, nil
}

func topicRow(ref topicRef, partitioned bool, partitions int) row {
	return row{
		"name":        ref.Name,
		"topic":       ref.String(),
		"domain":      ref.Domain,
		"tenant":      ref.Tenant,
		"namespace":   ref.Namespace,
		"partitioned": partitioned,
		"partitions":  partitions,
		"ref":         ref.identity(),
	}
}

func (s *Session) partitionedTopics(ctx context.Context, tenant, namespace string, systemTopics bool) (map[string]topicRef, error) {
	out := map[string]topicRef{}
	query := url.Values{}
	if systemTopics {
		query.Set("includeSystemTopic", "true")
	}
	for _, domain := range []string{persistentDomain, nonPersistentDomain} {
		path := "/admin/v2/" + domain + "/" + url.PathEscape(tenant) + "/" + url.PathEscape(namespace) + "/partitioned"
		var names []string
		if err := s.get(ctx, path, query, &names); err != nil {
			// Non-persistent topics cannot be partitioned on every broker version,
			// so only the persistent sweep is required to succeed.
			if domain == persistentDomain {
				return nil, err
			}
			continue
		}
		for _, name := range names {
			ref, err := parseTopic(name)
			if err != nil {
				continue
			}
			out[ref.String()] = ref
		}
	}
	return out, nil
}

func listTopics(rc *plugin.RequestContext) (any, error) {
	return topicsPage(rc, true)
}

func readTopicStats(rc *plugin.RequestContext) (any, error) {
	s, ref, err := topicScope(rc)
	if err != nil {
		return nil, err
	}
	partitions, err := s.partitionCount(rc.Ctx, ref)
	if err != nil {
		return nil, err
	}
	path, query := statsEndpoint(ref, partitions > 0, false)
	var out row
	if err := s.get(rc.Ctx, path, query, &out); err != nil {
		return nil, err
	}
	if out == nil {
		out = row{}
	}
	out["name"], out["topic"], out["domain"] = ref.Name, ref.String(), ref.Domain
	out["tenant"], out["namespace"] = ref.Tenant, ref.Namespace
	out["partitioned"], out["partitions"] = partitions > 0, partitions
	if subs, ok := out["subscriptions"].(map[string]any); ok {
		out["subscription_count"] = len(subs)
	}
	if publishers, ok := out["publishers"].([]any); ok {
		out["producer_count"] = len(publishers)
	}
	return out, nil
}

func readTopicInternalStats(rc *plugin.RequestContext) (any, error) {
	s, ref, err := topicScope(rc)
	if err != nil {
		return nil, err
	}
	var out row
	if err := s.get(rc.Ctx, ref.path()+"/internalStats", nil, &out); err != nil {
		return nil, err
	}
	if out == nil {
		out = row{}
	}
	out["topic"] = ref.String()
	return out, nil
}

func listPartitions(rc *plugin.RequestContext) (any, error) {
	s, ref, err := topicScope(rc)
	if err != nil {
		return nil, err
	}
	stats, partitions, err := s.topicStats(rc.Ctx, ref, true)
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, partitions)
	for name, part := range stats.Partitions {
		index := -1
		if match := partitionSuffix.FindStringSubmatch(name); match != nil {
			if parsed, convErr := strconv.Atoi(match[2]); convErr == nil {
				index = parsed
			}
		}
		item := row{
			"partition":     index,
			"topic":         name,
			"producers":     len(part.Publishers),
			"subscriptions": len(part.Subscriptions),
			"msg_rate_in":   part.MsgRateIn,
			"msg_rate_out":  part.MsgRateOut,
			"storage_size":  part.StorageSize,
			"backlog_size":  part.BacklogSize,
		}
		// A partition is a topic of its own, and the only place a partitioned
		// topic's messages can be read.
		if partRef, parseErr := parseTopic(name); parseErr == nil {
			item["ref"] = partRef.identity()
		}
		rows = append(rows, item)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i]["partition"].(int) < rows[j]["partition"].(int) })
	return broker.PageRows(rc, rows)
}

func readTopicSchema(rc *plugin.RequestContext) (any, error) {
	s, ref, err := topicScope(rc)
	if err != nil {
		return nil, err
	}
	var out row
	if err := s.get(rc.Ctx, ref.schemaPath(), nil, &out); err != nil {
		// A topic without a registered schema is the normal case, not a failure of
		// the panel, so it renders as the schemaless NONE type.
		if errors.Is(err, plugin.ErrNotFound) {
			return row{"topic": ref.String(), "type": "NONE", "registered": false}, nil
		}
		return nil, err
	}
	if out == nil {
		out = row{}
	}
	out["topic"], out["registered"] = ref.String(), true
	return out, nil
}

func createTopic(rc *plugin.RequestContext) (any, error) {
	s, tenant, namespace, err := writableNamespace(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		Name       string `json:"name"`
		Domain     string `json:"domain"`
		Partitions int    `json:"partitions"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name, err := simpleName("topic", req.Name)
	if err != nil {
		return nil, err
	}
	domain := req.Domain
	if domain == "" {
		domain = persistentDomain
	}
	if domain != persistentDomain && domain != nonPersistentDomain {
		return nil, fmt.Errorf("%w: unsupported topic domain %q", plugin.ErrInvalidInput, req.Domain)
	}
	if req.Partitions < 0 {
		return nil, fmt.Errorf("%w: partitions cannot be negative", plugin.ErrInvalidInput)
	}
	ref := topicRef{Domain: domain, Tenant: tenant, Namespace: namespace, Name: name}
	if req.Partitions > 0 {
		if err := s.send(rc.Ctx, http.MethodPut, ref.path()+"/partitions", nil, req.Partitions, nil); err != nil {
			return nil, err
		}
		return row{"ok": true, "topic": ref.String(), "partitions": req.Partitions}, nil
	}
	if err := s.send(rc.Ctx, http.MethodPut, ref.path(), nil, nil, nil); err != nil {
		return nil, err
	}
	return row{"ok": true, "topic": ref.String(), "partitions": 0}, nil
}

func compactTopic(rc *plugin.RequestContext) (any, error) {
	s, ref, err := writableTopic(rc)
	if err != nil {
		return nil, err
	}
	if err := s.send(rc.Ctx, http.MethodPut, ref.path()+"/compaction", nil, nil, nil); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func unloadTopic(rc *plugin.RequestContext) (any, error) {
	s, ref, err := writableTopic(rc)
	if err != nil {
		return nil, err
	}
	if err := s.send(rc.Ctx, http.MethodPut, ref.path()+"/unload", nil, nil, nil); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func terminateTopic(rc *plugin.RequestContext) (any, error) {
	s, ref, err := writableTopic(rc)
	if err != nil {
		return nil, err
	}
	partitions, err := s.partitionCount(rc.Ctx, ref)
	if err != nil {
		return nil, err
	}
	// A partitioned topic rejects /terminate; each partition is terminated
	// through the fan-out endpoint instead.
	path := ref.path() + "/terminate"
	if partitions > 0 {
		path += "/partitions"
	}
	var out row
	if err := s.send(rc.Ctx, http.MethodPost, path, nil, nil, &out); err != nil {
		return nil, err
	}
	return row{"ok": true, "last_message_id": out}, nil
}

func deleteTopic(rc *plugin.RequestContext) (any, error) {
	s, ref, err := writableTopic(rc)
	if err != nil {
		return nil, err
	}
	partitions, err := s.partitionCount(rc.Ctx, ref)
	if err != nil {
		return nil, err
	}
	path := ref.path()
	if partitions > 0 {
		path += "/partitions"
	}
	if err := s.send(rc.Ctx, http.MethodDelete, path, nil, nil, nil); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func deleteTopicSchema(rc *plugin.RequestContext) (any, error) {
	s, ref, err := writableTopic(rc)
	if err != nil {
		return nil, err
	}
	if err := s.send(rc.Ctx, http.MethodDelete, ref.schemaPath(), nil, nil, nil); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func listSubscriptions(rc *plugin.RequestContext) (any, error) {
	s, ref, err := topicScope(rc)
	if err != nil {
		return nil, err
	}
	stats, partitions, err := s.topicStats(rc.Ctx, ref, false)
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(stats.Subscriptions))
	for name, sub := range stats.Subscriptions {
		rows = append(rows, subscriptionRow(ref, name, sub, partitions))
	}
	sort.Slice(rows, func(i, j int) bool { return fmt.Sprint(rows[i]["name"]) < fmt.Sprint(rows[j]["name"]) })
	return broker.PageRows(rc, rows)
}

func subscriptionRow(ref topicRef, name string, sub subscriptionStats, partitions int) row {
	return row{
		"name":                   name,
		"topic":                  ref.String(),
		"domain":                 ref.Domain,
		"partitioned":            partitions > 0,
		"type":                   sub.Type,
		"msg_backlog":            sub.MsgBacklog,
		"msg_backlog_no_delayed": sub.MsgBacklogNoDelayed,
		"msg_delayed":            sub.MsgDelayed,
		"unacked_messages":       sub.UnackedMessages,
		"consumers":              len(sub.Consumers),
		"msg_rate_out":           sub.MsgRateOut,
		"msg_throughput_out":     sub.MsgThroughputOut,
		"msg_rate_redeliver":     sub.MsgRateRedeliver,
		"durable":                sub.Durable,
		"replicated":             sub.Replicated,
		"blocked":                sub.BlockedOnUnacked,
		"active_consumer":        sub.ActiveConsumerName,
		"last_consumed":          epochMillis(sub.LastConsumedTimestamp),
		"last_acked":             epochMillis(sub.LastAckedTimestamp),
		"ref":                    subscriptionRef(ref, name),
	}
}

func subscriptionRef(ref topicRef, name string) plugin.ResourceIdentity {
	return plugin.ResourceIdentity{Kind: "subscription", Namespace: ref.String(), Name: name, UID: ref.String() + "::" + name}
}

func readSubscription(rc *plugin.RequestContext) (any, error) {
	s, ref, err := topicScope(rc)
	if err != nil {
		return nil, err
	}
	name, err := simpleName("subscription", param(rc, "subscription"))
	if err != nil {
		return nil, err
	}
	stats, partitions, err := s.topicStats(rc.Ctx, ref, false)
	if err != nil {
		return nil, err
	}
	sub, ok := stats.Subscriptions[name]
	if !ok {
		return nil, fmt.Errorf("%w: subscription %q does not exist on %s", plugin.ErrNotFound, name, ref.String())
	}
	out := subscriptionRow(ref, name, sub, partitions)
	out["total_msg_expired"], out["last_expire"] = sub.TotalMsgExpired, epochMillis(sub.LastExpireTimestamp)
	return out, nil
}

func listProducers(rc *plugin.RequestContext) (any, error) {
	s, ref, err := topicScope(rc)
	if err != nil {
		return nil, err
	}
	stats, partitions, err := s.topicStats(rc.Ctx, ref, true)
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(stats.Publishers))
	appendPublishers := func(topic string, publishers []publisherStatsDoc) {
		for _, p := range publishers {
			rows = append(rows, row{
				"producer_name":     p.ProducerName,
				"producer_id":       p.ProducerID,
				"topic":             topic,
				"address":           p.Address,
				"connected_since":   pulsarTime(p.ConnectedSince),
				"client_version":    p.ClientVersion,
				"access_mode":       p.AccessMode,
				"msg_rate_in":       p.MsgRateIn,
				"msg_throughput_in": p.MsgThroughputIn,
				"average_msg_size":  p.AverageMsgSize,
			})
		}
	}
	// partitioned-stats already merges every partition's publishers into its own
	// publishers list, so reading both would list each producer twice; the
	// per-partition documents are preferred because they attribute the producer
	// to the partition it is connected to.
	if partitions > 0 && len(stats.Partitions) > 0 {
		for _, name := range sortedKeys(stats.Partitions) {
			appendPublishers(name, stats.Partitions[name].Publishers)
		}
	} else {
		appendPublishers(ref.String(), stats.Publishers)
	}
	return broker.PageRows(rc, rows)
}

func listConsumers(rc *plugin.RequestContext) (any, error) {
	s, ref, err := topicScope(rc)
	if err != nil {
		return nil, err
	}
	stats, _, err := s.topicStats(rc.Ctx, ref, false)
	if err != nil {
		return nil, err
	}
	// A subscription tab scopes the listing to its own consumers instead of
	// enumerating the topic and filtering afterwards.
	wanted := strings.TrimSpace(param(rc, "subscription"))
	rows := []row{}
	for _, name := range sortedKeys(stats.Subscriptions) {
		if wanted != "" && name != wanted {
			continue
		}
		for _, c := range stats.Subscriptions[name].Consumers {
			rows = append(rows, row{
				"consumer_name":      c.ConsumerName,
				"subscription":       name,
				"topic":              ref.String(),
				"address":            c.Address,
				"connected_since":    pulsarTime(c.ConnectedSince),
				"client_version":     c.ClientVersion,
				"msg_rate_out":       c.MsgRateOut,
				"msg_throughput_out": c.MsgThroughputOut,
				"msg_rate_redeliver": c.MsgRateRedeliver,
				"available_permits":  c.AvailablePermits,
				"unacked_messages":   c.UnackedMessages,
				"blocked":            c.BlockedOnUnacked,
				"last_consumed":      epochMillis(c.LastConsumedTimestamp),
				"metadata":           c.Metadata,
			})
		}
	}
	return broker.PageRows(rc, rows)
}

func subscriptionTarget(rc *plugin.RequestContext) (*Session, topicRef, string, error) {
	s, ref, err := writableTopic(rc)
	if err != nil {
		return nil, topicRef{}, "", err
	}
	name, err := simpleName("subscription", param(rc, "subscription"))
	if err != nil {
		return nil, topicRef{}, "", err
	}
	return s, ref, name, nil
}

func subscriptionPath(ref topicRef, name string) string {
	return ref.path() + "/subscription/" + url.PathEscape(name)
}

// cursorPosition is the message id the admin API accepts for the earliest and
// latest positions: Pulsar encodes them as the extreme ledger/entry values.
func cursorPosition(position string) (row, error) {
	switch position {
	case positionEarliest:
		return row{"ledgerId": -1, "entryId": -1, "partitionIndex": -1}, nil
	case "", positionLatest:
		return row{"ledgerId": int64(math.MaxInt64), "entryId": int64(math.MaxInt64), "partitionIndex": -1}, nil
	default:
		return nil, fmt.Errorf("%w: unsupported position %q", plugin.ErrInvalidInput, position)
	}
}

func createSubscription(rc *plugin.RequestContext) (any, error) {
	s, ref, err := writableTopic(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		Name     string `json:"name"`
		Position string `json:"position"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	name, err := simpleName("subscription", req.Name)
	if err != nil {
		return nil, err
	}
	body, err := cursorPosition(req.Position)
	if err != nil {
		return nil, err
	}
	if err := s.send(rc.Ctx, http.MethodPut, subscriptionPath(ref, name), nil, body, nil); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func skipMessages(rc *plugin.RequestContext) (any, error) {
	s, ref, name, err := subscriptionTarget(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		Count int `json:"count"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	if req.Count < 1 {
		return nil, fmt.Errorf("%w: skip count must be at least 1", plugin.ErrInvalidInput)
	}
	path := subscriptionPath(ref, name) + "/skip/" + strconv.Itoa(req.Count)
	if err := s.send(rc.Ctx, http.MethodPost, path, nil, nil, nil); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func clearBacklog(rc *plugin.RequestContext) (any, error) {
	s, ref, name, err := subscriptionTarget(rc)
	if err != nil {
		return nil, err
	}
	if err := s.send(rc.Ctx, http.MethodPost, subscriptionPath(ref, name)+"/skip_all", nil, nil, nil); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func resetCursor(rc *plugin.RequestContext) (any, error) {
	s, ref, name, err := subscriptionTarget(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		Target string `json:"target"`
		Age    string `json:"age"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	if req.Target == "time" {
		age, err := parseAge(req.Age)
		if err != nil {
			return nil, err
		}
		stamp := time.Now().Add(-age).UnixMilli()
		path := subscriptionPath(ref, name) + "/resetcursor/" + strconv.FormatInt(stamp, 10)
		if err := s.send(rc.Ctx, http.MethodPost, path, nil, nil, nil); err != nil {
			return nil, err
		}
		return row{"ok": true, "timestamp": stamp}, nil
	}
	body, err := cursorPosition(req.Target)
	if err != nil {
		return nil, err
	}
	if err := s.send(rc.Ctx, http.MethodPost, subscriptionPath(ref, name)+"/resetcursor", nil, body, nil); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func expireMessages(rc *plugin.RequestContext) (any, error) {
	s, ref, name, err := subscriptionTarget(rc)
	if err != nil {
		return nil, err
	}
	var req struct {
		Age string `json:"age"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	age, err := parseAge(req.Age)
	if err != nil {
		return nil, err
	}
	path := subscriptionPath(ref, name) + "/expireMessages/" + strconv.Itoa(int(age.Seconds()))
	if err := s.send(rc.Ctx, http.MethodPost, path, nil, nil, nil); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func deleteSubscription(rc *plugin.RequestContext) (any, error) {
	s, ref, name, err := subscriptionTarget(rc)
	if err != nil {
		return nil, err
	}
	if err := s.send(rc.Ctx, http.MethodDelete, subscriptionPath(ref, name), nil, nil, nil); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, nil
}

func parseAge(raw string) (time.Duration, error) {
	age, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil || age <= 0 {
		return 0, fmt.Errorf("%w: duration must be positive (for example 30m or 24h)", plugin.ErrInvalidInput)
	}
	return age, nil
}

func epochMillis(ms int64) any {
	if ms <= 0 {
		return nil
	}
	return time.UnixMilli(ms).UTC()
}

func sortedKeys[V any](in map[string]V) []string {
	out := make([]string, 0, len(in))
	for key := range in {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}
