package kafka

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	// listSnapshotTTL is how long one cluster-wide name sweep backs the topic and
	// consumer-group listings before the next page takes a fresh one.
	listSnapshotTTL = 10 * time.Second

	// listScanLimit caps how many names one sweep materializes, so a cluster with
	// tens of thousands of topics or groups cannot be paged into the plugin heap.
	listScanLimit = 2000
)

type Session struct {
	client sarama.Client
	admin  sarama.ClusterAdmin
	opts   options
	net    plugin.NetTransport

	listMu    sync.Mutex
	topicSnap *listSnapshot
	groupSnap *listSnapshot
}

// listSnapshot is one cluster sweep rendered as rows. total is the name count
// before the cap, so an overview can report it without holding every row.
type listSnapshot struct {
	rows      []row
	total     int
	truncated bool
	takenAt   time.Time
}

func (snap *listSnapshot) fresh(now time.Time) bool {
	return snap != nil && now.Sub(snap.takenAt) < listSnapshotTTL
}

// page returns a copy of the sweep for one request: the caller filters and sorts
// it in place, and the cached rows must outlive that.
func (snap *listSnapshot) page() []row {
	return slices.Clone(snap.rows)
}

func snapshotOf(names []string, render func(string) row) *listSnapshot {
	sort.Strings(names)
	snap := &listSnapshot{total: len(names), takenAt: time.Now()}
	if len(names) > listScanLimit {
		names, snap.truncated = names[:listScanLimit], true
	}
	snap.rows = make([]row, 0, len(names))
	for _, name := range names {
		snap.rows = append(snap.rows, render(name))
	}
	return snap
}

// topics sweeps the topic catalogue at most once per TTL. ListTopics is a full
// cluster metadata request, so paging, re-sorting, and the tree all share it.
func (s *Session) topics() (*listSnapshot, error) {
	s.listMu.Lock()
	defer s.listMu.Unlock()
	if s.topicSnap.fresh(time.Now()) {
		return s.topicSnap, nil
	}
	topics, err := s.admin.ListTopics()
	if err != nil {
		return nil, kafkaErr(err)
	}
	names := make([]string, 0, len(topics))
	for name := range topics {
		names = append(names, name)
	}
	s.topicSnap = snapshotOf(names, func(name string) row {
		d := topics[name]
		return row{
			"name":               name,
			"partitions":         d.NumPartitions,
			"replication_factor": d.ReplicationFactor,
			"internal":           strings.HasPrefix(name, "__"),
			"ref":                plugin.ResourceIdentity{Kind: "topic", Name: name, UID: name},
		}
	})
	return s.topicSnap, nil
}

// groups sweeps the consumer-group names at most once per TTL. Only names and
// protocol types are cached; member state is described for the returned page.
func (s *Session) groups() (*listSnapshot, error) {
	s.listMu.Lock()
	defer s.listMu.Unlock()
	if s.groupSnap.fresh(time.Now()) {
		return s.groupSnap, nil
	}
	groups, err := s.admin.ListConsumerGroups()
	if err != nil {
		return nil, kafkaErr(err)
	}
	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}
	s.groupSnap = snapshotOf(names, func(name string) row {
		return row{"name": name, "protocol_type": groups[name], "ref": plugin.ResourceIdentity{Kind: "consumer_group", Name: name, UID: name}}
	})
	return s.groupSnap, nil
}

// dropSnapshots forces the next listing to take a fresh sweep, so a created or
// deleted topic or group never lingers in the browser.
func (s *Session) dropSnapshots() {
	s.listMu.Lock()
	defer s.listMu.Unlock()
	s.topicSnap, s.groupSnap = nil, nil
}

func connect(ctx context.Context, cfg plugin.ConnectConfig) (plugin.Session, error) {
	if cfg.Net == nil {
		return nil, fmt.Errorf("%w: network transport is unavailable", plugin.ErrUnavailable)
	}
	opts, err := parseOptions(cfg)
	if err != nil {
		return nil, err
	}
	saramaCfg := saramaConfig(opts, cfg.Net)
	client, err := sarama.NewClient(opts.Brokers, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	s := &Session{client: client, admin: admin, opts: opts, net: cfg.Net}
	return s, s.HealthCheck(ctx)
}

func (s *Session) HealthCheck(context.Context) error {
	err := s.client.RefreshMetadata()
	if err != nil {
		return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
	}
	return nil
}

func (s *Session) OpenChannel(context.Context, plugin.ChannelRequest) (plugin.Channel, error) {
	return nil, plugin.ErrNotSupported
}

func (s *Session) Close() error {
	if s.admin != nil {
		_ = s.admin.Close()
	}
	if s.client != nil {
		return s.client.Close()
	}
	return nil
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
