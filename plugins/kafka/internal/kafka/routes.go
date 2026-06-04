package kafka

import (
	"encoding/base64"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

type row map[string]any

type actionResult struct {
	OK bool `json:"ok"`
}

func routes() []plugin.Route {
	return []plugin.Route{
		{ID: "kafka.overview", Method: plugin.MethodGet, Path: "/overview", Permission: "kafka.read", Risk: plugin.RiskSafe, AuditEvent: "kafka.overview", Handle: overview},
		{ID: "kafka.topics.tree", Method: plugin.MethodGet, Path: "/tree/topics", Permission: "kafka.topics.read", Risk: plugin.RiskSafe, AuditEvent: "kafka.topics.tree", Handle: treeTopics},
		{ID: "kafka.groups.tree", Method: plugin.MethodGet, Path: "/tree/groups", Permission: "kafka.groups.read", Risk: plugin.RiskSafe, AuditEvent: "kafka.groups.tree", Handle: treeGroups},
		{ID: "kafka.topics.list", Method: plugin.MethodGet, Path: "/topics", Permission: "kafka.topics.read", Risk: plugin.RiskSafe, AuditEvent: "kafka.topics.list", Handle: listTopics},
		{ID: "kafka.topic.overview", Method: plugin.MethodGet, Path: "/topics/{topic}", Permission: "kafka.topics.read", Risk: plugin.RiskSafe, AuditEvent: "kafka.topic.overview", Handle: topicOverview},
		{ID: "kafka.partitions.list", Method: plugin.MethodGet, Path: "/topics/{topic}/partitions", Permission: "kafka.partitions.read", Risk: plugin.RiskSafe, AuditEvent: "kafka.partitions.list", Handle: listPartitions},
		{ID: "kafka.topic.config", Method: plugin.MethodGet, Path: "/topics/{topic}/config", Permission: "kafka.topics.read", Risk: plugin.RiskSafe, AuditEvent: "kafka.topic.config", Handle: topicConfig},
		{ID: "kafka.messages.list", Method: plugin.MethodGet, Path: "/topics/{topic}/messages", Permission: "kafka.messages.read", Risk: plugin.RiskSafe, AuditEvent: "kafka.messages.list", Handle: listMessages},
		{ID: "kafka.topic.create", Method: plugin.MethodPost, Path: "/topics", Permission: "kafka.topics.write", Risk: plugin.RiskWrite, AuditEvent: "kafka.topic.create", Input: topicCreateSchema(), Handle: createTopic},
		{ID: "kafka.topic.alter_config", Method: plugin.MethodPost, Path: "/topics/{topic}/config", Permission: "kafka.topics.write", Risk: plugin.RiskWrite, AuditEvent: "kafka.topic.alter_config", Input: alterConfigSchema(), Handle: alterTopicConfig},
		{ID: "kafka.topic.add_partitions", Method: plugin.MethodPost, Path: "/topics/{topic}/partitions", Permission: "kafka.partitions.write", Risk: plugin.RiskDestructive, AuditEvent: "kafka.topic.add_partitions", Input: addPartitionsSchema(), Handle: addPartitions},
		{ID: "kafka.topic.delete", Method: plugin.MethodDelete, Path: "/topics/{topic}", Permission: "kafka.topics.delete", Risk: plugin.RiskDestructive, AuditEvent: "kafka.topic.delete", Handle: deleteTopic},
		{ID: "kafka.message.produce", Method: plugin.MethodPost, Path: "/topics/{topic}/messages", Permission: "kafka.messages.write", Risk: plugin.RiskWrite, AuditEvent: "kafka.message.produce", Input: produceSchema(), Handle: produceMessage},
		{ID: "kafka.groups.list", Method: plugin.MethodGet, Path: "/groups", Permission: "kafka.groups.read", Risk: plugin.RiskSafe, AuditEvent: "kafka.groups.list", Handle: listGroups},
		{ID: "kafka.group.overview", Method: plugin.MethodGet, Path: "/groups/{group}", Permission: "kafka.groups.read", Risk: plugin.RiskSafe, AuditEvent: "kafka.group.overview", Handle: groupOverview},
		{ID: "kafka.group.offsets", Method: plugin.MethodGet, Path: "/groups/{group}/offsets", Permission: "kafka.groups.read", Risk: plugin.RiskSafe, AuditEvent: "kafka.group.offsets", Handle: groupOffsets},
		{ID: "kafka.group.delete", Method: plugin.MethodDelete, Path: "/groups/{group}", Permission: "kafka.groups.delete", Risk: plugin.RiskDestructive, AuditEvent: "kafka.group.delete", Handle: deleteGroup},
		{ID: "kafka.group.reset_offsets", Method: plugin.MethodPost, Path: "/groups/{group}/offsets/reset", Permission: "kafka.groups.write", Risk: plugin.RiskDestructive, AuditEvent: "kafka.group.reset_offsets", Input: offsetResetSchema(), Handle: resetGroupOffsets},
	}
}

func topicCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Topic", Fields: []plugin.Field{
		{Key: "name", Label: "Topic name", Type: plugin.FieldText, Required: true},
		{Key: "partitions", Label: "Partitions", Type: plugin.FieldNumber, Required: true, Default: 3, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}}},
		{Key: "replication_factor", Label: "Replication factor", Type: plugin.FieldNumber, Required: true, Default: 1, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}}},
		{Key: "config", Label: "Config entries", Type: plugin.FieldMap, KeyPlaceholder: "retention.ms", AddLabel: "Add config", Item: &plugin.Field{Type: plugin.FieldText, Placeholder: "604800000"}},
	}}}}
}

func alterConfigSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Alter config", Fields: []plugin.Field{
		{Key: "key", Label: "Config key", Type: plugin.FieldText, Required: true, Placeholder: "retention.ms", Help: "Topic-level config name, e.g. retention.ms or cleanup.policy."},
		{Key: "value", Label: "Value", Type: plugin.FieldText, Required: true},
	}}}}
}

func addPartitionsSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Add partitions", Fields: []plugin.Field{
		{Key: "count", Label: "New partition count", Type: plugin.FieldNumber, Required: true, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}}, Help: "Total partitions after the increase. Must be greater than the current count; partitions can only be added, never removed."},
	}}}}
}

func produceSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Record", Fields: []plugin.Field{
		{Key: "key", Label: "Key", Type: plugin.FieldText},
		{Key: "value", Label: "Value", Type: plugin.FieldTextarea, Required: true},
		{Key: "encoding", Label: "Encoding", Type: plugin.FieldSelect, Required: true, Default: "string", Options: []plugin.Option{{Label: "String", Value: "string"}, {Label: "Base64", Value: "base64"}}},
		{Key: "partition", Label: "Partition", Type: plugin.FieldNumber},
		{Key: "headers", Label: "Headers", Type: plugin.FieldMap, KeyPlaceholder: "header-name", Item: &plugin.Field{Type: plugin.FieldText}},
	}}}}
}

func kafkaSession(rc *plugin.RequestContext) (*Session, error) {
	return unwrap(rc.Session)
}

func overview(rc *plugin.RequestContext) (any, error) {
	s, err := kafkaSession(rc)
	if err != nil {
		return nil, err
	}
	brokers := s.client.Brokers()
	controllerID := int32(-1)
	if controller, err := s.client.Controller(); err == nil && controller != nil {
		controllerID = controller.ID()
	}
	topics, _ := s.admin.ListTopics()
	groups, _ := s.admin.ListConsumerGroups()
	return row{"brokers": len(brokers), "controller_id": controllerID, "topics": len(topics), "consumer_groups": len(groups), "readOnly": s.opts.ReadOnly}, nil
}

func treeTopics(rc *plugin.RequestContext) (any, error) {
	res, err := listTopics(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name := fmt.Sprint(item["name"])
		ref := plugin.ResourceRef{Kind: "topic", Name: name, UID: name}
		nodes = append(nodes, plugin.TreeNode{Key: "topic:" + name, Label: name, Icon: icon("radio-tower"), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

func treeGroups(rc *plugin.RequestContext) (any, error) {
	res, err := listGroups(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name := fmt.Sprint(item["name"])
		ref := plugin.ResourceRef{Kind: "consumer_group", Name: name, UID: name}
		nodes = append(nodes, plugin.TreeNode{Key: "group:" + name, Label: name, Icon: icon("users"), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

func listTopics(rc *plugin.RequestContext) (any, error) {
	s, err := kafkaSession(rc)
	if err != nil {
		return nil, err
	}
	topics, err := s.admin.ListTopics()
	if err != nil {
		return nil, kafkaErr(err)
	}
	names := make([]string, 0, len(topics))
	for name := range topics {
		names = append(names, name)
	}
	sort.Strings(names)
	rows := make([]row, 0, len(names))
	for _, name := range names {
		d := topics[name]
		rows = append(rows, row{
			"name":               name,
			"partitions":         d.NumPartitions,
			"replication_factor": d.ReplicationFactor,
			"internal":           strings.HasPrefix(name, "__"),
			"ref":                plugin.ResourceRef{Kind: "topic", Name: name, UID: name},
		})
	}
	return broker.PageRows(rc, rows)
}

func topicOverview(rc *plugin.RequestContext) (any, error) {
	topic := topicParam(rc)
	meta, err := describeTopic(rc, topic)
	if err != nil {
		return nil, err
	}
	return meta, nil
}

func listPartitions(rc *plugin.RequestContext) (any, error) {
	s, err := kafkaSession(rc)
	if err != nil {
		return nil, err
	}
	topic := topicParam(rc)
	partitions, err := s.client.Partitions(topic)
	if err != nil {
		return nil, kafkaErr(err)
	}
	rows := make([]row, 0, len(partitions))
	for _, p := range partitions {
		leader, _ := s.client.Leader(topic, p)
		replicas, _ := s.client.Replicas(topic, p)
		isr, _ := s.client.InSyncReplicas(topic, p)
		oldest, _ := s.client.GetOffset(topic, p, sarama.OffsetOldest)
		newest, _ := s.client.GetOffset(topic, p, sarama.OffsetNewest)
		leaderID := int32(-1)
		if leader != nil {
			leaderID = leader.ID()
		}
		rows = append(rows, row{"partition": p, "leader": leaderID, "oldest_offset": oldest, "newest_offset": newest, "replicas": replicas, "isr": isr})
	}
	return broker.PageRows(rc, rows)
}

func topicConfig(rc *plugin.RequestContext) (any, error) {
	s, err := kafkaSession(rc)
	if err != nil {
		return nil, err
	}
	entries, err := s.admin.DescribeConfig(sarama.ConfigResource{Type: sarama.TopicResource, Name: topicParam(rc)})
	if err != nil {
		return nil, kafkaErr(err)
	}
	rows := make([]row, 0, len(entries))
	for _, e := range entries {
		value := e.Value
		if e.Sensitive {
			value = ""
		}
		rows = append(rows, row{"name": e.Name, "value": value, "default": e.Default, "read_only": e.ReadOnly, "sensitive": e.Sensitive})
	}
	return broker.PageRows(rc, rows)
}

func listMessages(rc *plugin.RequestContext) (any, error) {
	s, err := kafkaSession(rc)
	if err != nil {
		return nil, err
	}
	req, err := rc.Page()
	if err != nil {
		return nil, err
	}
	limit := req.Limit
	if limit > s.opts.MessageLimit {
		limit = s.opts.MessageLimit
	}
	rows, err := readRecentMessages(s, topicParam(rc), limit)
	if err != nil {
		return nil, err
	}
	return broker.PageRows(rc, rows)
}

func createTopic(rc *plugin.RequestContext) (any, error) {
	s, err := kafkaSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Name              string            `json:"name"`
		Partitions        int32             `json:"partitions"`
		ReplicationFactor int16             `json:"replication_factor"`
		Config            map[string]string `json:"config"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	entries := make(map[string]*string, len(req.Config))
	for k, v := range req.Config {
		value := v
		entries[k] = &value
	}
	err = s.admin.CreateTopic(req.Name, &sarama.TopicDetail{NumPartitions: req.Partitions, ReplicationFactor: req.ReplicationFactor, ConfigEntries: entries}, false)
	return actionResult{OK: err == nil}, kafkaErr(err)
}

func validateConfigEntry(key, value string) error {
	if strings.TrimSpace(key) == "" {
		return fmt.Errorf("%w: config key is required", plugin.ErrInvalidInput)
	}
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%w: config value is required", plugin.ErrInvalidInput)
	}
	return nil
}

func validatePartitionCount(topic string, count, current int32) error {
	if strings.TrimSpace(topic) == "" {
		return fmt.Errorf("%w: topic is required", plugin.ErrInvalidInput)
	}
	if count <= current {
		return fmt.Errorf("%w: new partition count %d must be greater than current count %d", plugin.ErrInvalidInput, count, current)
	}
	return nil
}

func alterTopicConfig(rc *plugin.RequestContext) (any, error) {
	s, err := kafkaSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	if err := validateConfigEntry(req.Key, req.Value); err != nil {
		return nil, err
	}
	value := req.Value
	entries := map[string]sarama.IncrementalAlterConfigsEntry{
		req.Key: {Operation: sarama.IncrementalAlterConfigsOperationSet, Value: &value},
	}
	err = s.admin.IncrementalAlterConfig(sarama.TopicResource, topicParam(rc), entries, false)
	return actionResult{OK: err == nil}, kafkaErr(err)
}

func addPartitions(rc *plugin.RequestContext) (any, error) {
	s, err := kafkaSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Count int32 `json:"count"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	topic := topicParam(rc)
	current, err := s.client.Partitions(topic)
	if err != nil {
		return nil, kafkaErr(err)
	}
	if err := validatePartitionCount(topic, req.Count, int32(len(current))); err != nil {
		return nil, err
	}
	err = s.admin.CreatePartitions(topic, req.Count, nil, false)
	return actionResult{OK: err == nil}, kafkaErr(err)
}

func deleteTopic(rc *plugin.RequestContext) (any, error) {
	s, err := kafkaSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	err = s.admin.DeleteTopic(topicParam(rc))
	return actionResult{OK: err == nil}, kafkaErr(err)
}

func produceMessage(rc *plugin.RequestContext) (any, error) {
	s, err := kafkaSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Key       string            `json:"key"`
		Value     string            `json:"value"`
		Encoding  string            `json:"encoding"`
		Partition *int32            `json:"partition"`
		Headers   map[string]string `json:"headers"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	value := []byte(req.Value)
	if req.Encoding == "base64" {
		value, err = base64.StdEncoding.DecodeString(req.Value)
		if err != nil {
			return nil, fmt.Errorf("%w: value is not valid base64", plugin.ErrInvalidInput)
		}
	}
	cfg := saramaConfig(s.opts)
	producer, err := sarama.NewSyncProducerFromClient(s.client)
	if err != nil {
		producer, err = sarama.NewSyncProducer(s.opts.Brokers, cfg)
	}
	if err != nil {
		return nil, kafkaErr(err)
	}
	defer func() { _ = producer.Close() }()
	msg := &sarama.ProducerMessage{Topic: topicParam(rc), Key: sarama.ByteEncoder([]byte(req.Key)), Value: sarama.ByteEncoder(value)}
	if req.Partition != nil {
		msg.Partition = *req.Partition
	}
	for k, v := range req.Headers {
		msg.Headers = append(msg.Headers, sarama.RecordHeader{Key: []byte(k), Value: []byte(v)})
	}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return nil, kafkaErr(err)
	}
	return row{"ok": true, "partition": partition, "offset": offset}, nil
}

func listGroups(rc *plugin.RequestContext) (any, error) {
	s, err := kafkaSession(rc)
	if err != nil {
		return nil, err
	}
	groups, err := s.admin.ListConsumerGroups()
	if err != nil {
		return nil, kafkaErr(err)
	}
	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}
	sort.Strings(names)
	descs, _ := s.admin.DescribeConsumerGroups(names)
	byName := map[string]*sarama.GroupDescription{}
	for _, d := range descs {
		if d != nil {
			byName[d.GroupId] = d
		}
	}
	rows := make([]row, 0, len(names))
	for _, name := range names {
		r := row{"name": name, "protocol_type": groups[name], "ref": plugin.ResourceRef{Kind: "consumer_group", Name: name, UID: name}}
		if d := byName[name]; d != nil {
			r["state"] = d.State
			r["members"] = len(d.Members)
		}
		rows = append(rows, r)
	}
	return broker.PageRows(rc, rows)
}

func groupOverview(rc *plugin.RequestContext) (any, error) {
	s, err := kafkaSession(rc)
	if err != nil {
		return nil, err
	}
	descs, err := s.admin.DescribeConsumerGroups([]string{groupParam(rc)})
	if err != nil {
		return nil, kafkaErr(err)
	}
	if len(descs) == 0 || descs[0] == nil {
		return nil, plugin.ErrNotFound
	}
	d := descs[0]
	return row{"group": d.GroupId, "state": d.State, "protocol": d.Protocol, "protocol_type": d.ProtocolType, "members": len(d.Members)}, nil
}

func groupOffsets(rc *plugin.RequestContext) (any, error) {
	s, err := kafkaSession(rc)
	if err != nil {
		return nil, err
	}
	resp, err := s.admin.ListConsumerGroupOffsets(groupParam(rc), nil)
	if err != nil {
		return nil, kafkaErr(err)
	}
	rows := []row{}
	for topic, parts := range resp.Blocks {
		for partition, block := range parts {
			newest, _ := s.client.GetOffset(topic, partition, sarama.OffsetNewest)
			lag := int64(0)
			if block.Offset >= 0 && newest >= block.Offset {
				lag = newest - block.Offset
			}
			rows = append(rows, row{"topic": topic, "partition": partition, "committed_offset": block.Offset, "newest_offset": newest, "lag": lag, "metadata": block.Metadata})
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i]["topic"] == rows[j]["topic"] {
			return rows[i]["partition"].(int32) < rows[j]["partition"].(int32)
		}
		return fmt.Sprint(rows[i]["topic"]) < fmt.Sprint(rows[j]["topic"])
	})
	return broker.PageRows(rc, rows)
}

func deleteGroup(rc *plugin.RequestContext) (any, error) {
	s, err := kafkaSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	err = s.admin.DeleteConsumerGroup(groupParam(rc))
	return actionResult{OK: err == nil}, kafkaErr(err)
}

func offsetResetSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Reset offsets", Fields: []plugin.Field{
		{Key: "target", Label: "Reset to", Type: plugin.FieldSelect, Required: true, Default: "earliest", Options: []plugin.Option{
			{Label: "Earliest (oldest)", Value: "earliest"},
			{Label: "Latest (newest)", Value: "latest"},
		}, Help: "Move every committed partition of this group to the start or end of the log. The group must have no active members."},
	}}}}
}

func resetGroupOffsets(rc *plugin.RequestContext) (any, error) {
	s, err := kafkaSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Target string `json:"target"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	mark := sarama.OffsetOldest
	if req.Target == "latest" {
		mark = sarama.OffsetNewest
	}
	group := groupParam(rc)
	committed, err := s.admin.ListConsumerGroupOffsets(group, nil)
	if err != nil {
		return nil, kafkaErr(err)
	}
	offsets := map[string]map[int32]sarama.OffsetAndMetadata{}
	for topic, parts := range committed.Blocks {
		for partition := range parts {
			target, offErr := s.client.GetOffset(topic, partition, mark)
			if offErr != nil {
				return nil, kafkaErr(offErr)
			}
			if offsets[topic] == nil {
				offsets[topic] = map[int32]sarama.OffsetAndMetadata{}
			}
			offsets[topic][partition] = sarama.OffsetAndMetadata{Offset: target, LeaderEpoch: -1}
		}
	}
	if len(offsets) == 0 {
		return nil, fmt.Errorf("%w: group has no committed offsets to reset", plugin.ErrInvalidInput)
	}
	resp, err := s.admin.AlterConsumerGroupOffsets(group, offsets, nil)
	if err != nil {
		return nil, kafkaErr(err)
	}
	for topic, parts := range resp.Errors {
		for partition, kerr := range parts {
			if kerr != sarama.ErrNoError {
				return nil, fmt.Errorf("%w: %s[%d]: %s", plugin.ErrUnavailable, topic, partition, kerr.Error())
			}
		}
	}
	return actionResult{OK: true}, nil
}

func describeTopic(rc *plugin.RequestContext, topic string) (row, error) {
	s, err := kafkaSession(rc)
	if err != nil {
		return nil, err
	}
	meta, err := s.admin.DescribeTopics([]string{topic})
	if err != nil {
		return nil, kafkaErr(err)
	}
	if len(meta) == 0 || meta[0] == nil {
		return nil, plugin.ErrNotFound
	}
	t := meta[0]
	return row{"name": t.Name, "internal": t.IsInternal, "partitions": len(t.Partitions), "error": t.Err.Error()}, nil
}

func readRecentMessages(s *Session, topic string, limit int) ([]row, error) {
	consumer, err := sarama.NewConsumerFromClient(s.client)
	if err != nil {
		return nil, kafkaErr(err)
	}
	defer func() { _ = consumer.Close() }()
	partitions, err := s.client.Partitions(topic)
	if err != nil {
		return nil, kafkaErr(err)
	}
	rows := []row{}
	deadline := time.After(s.opts.Timeout)
	for _, partition := range partitions {
		if len(rows) >= limit {
			break
		}
		newest, err := s.client.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil || newest <= 0 {
			continue
		}
		oldest, _ := s.client.GetOffset(topic, partition, sarama.OffsetOldest)
		start := newest - int64(limit)
		if start < oldest {
			start = oldest
		}
		pc, err := consumer.ConsumePartition(topic, partition, start)
		if err != nil {
			continue
		}
		for len(rows) < limit {
			select {
			case msg := <-pc.Messages():
				if msg == nil {
					_ = pc.Close()
					goto nextPartition
				}
				rows = append(rows, kafkaMessageRow(msg))
				if msg.Offset >= newest-1 {
					_ = pc.Close()
					goto nextPartition
				}
			case <-pc.Errors():
				_ = pc.Close()
				goto nextPartition
			case <-deadline:
				_ = pc.Close()
				return rows, nil
			}
		}
		_ = pc.Close()
	nextPartition:
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i]["timestamp"].(time.Time).After(rows[j]["timestamp"].(time.Time))
	})
	return rows, nil
}

func kafkaMessageRow(msg *sarama.ConsumerMessage) row {
	headers := map[string]string{}
	for _, h := range msg.Headers {
		headers[string(h.Key)] = string(h.Value)
	}
	return row{"partition": msg.Partition, "offset": msg.Offset, "timestamp": msg.Timestamp, "key": string(msg.Key), "value": string(msg.Value), "headers": headers}
}

func topicParam(rc *plugin.RequestContext) string {
	if v := strings.TrimSpace(rc.Param("topic")); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p.topic"))
}

func groupParam(rc *plugin.RequestContext) string {
	if v := strings.TrimSpace(rc.Param("group")); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p.group"))
}

func kafkaErr(err error) error {
	if err == nil {
		return nil
	}
	if err == sarama.ErrTopicAlreadyExists {
		return fmt.Errorf("%w: %v", plugin.ErrAlreadyExists, err)
	}
	if strings.Contains(err.Error(), "does not exist") || strings.Contains(err.Error(), "Unknown Topic") {
		return fmt.Errorf("%w: %v", plugin.ErrNotFound, err)
	}
	if _, convErr := strconv.Atoi(err.Error()); convErr == nil {
		return err
	}
	return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
}
