package nats

import (
	"encoding/base64"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	natsclient "github.com/nats-io/nats.go"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

type row map[string]any

type actionResult struct {
	OK bool `json:"ok"`
}

func routes() []plugin.Route {
	return []plugin.Route{
		{ID: "nats.overview", Method: plugin.MethodGet, Path: "/overview", Permission: "nats.read", Risk: plugin.RiskSafe, AuditEvent: "nats.overview", Handle: overview},
		{ID: "nats.streams.tree", Method: plugin.MethodGet, Path: "/tree/streams", Permission: "nats.streams.read", Risk: plugin.RiskSafe, AuditEvent: "nats.streams.tree", Handle: treeStreams},
		{ID: "nats.streams.list", Method: plugin.MethodGet, Path: "/streams", Permission: "nats.streams.read", Risk: plugin.RiskSafe, AuditEvent: "nats.streams.list", Handle: listStreams},
		{ID: "nats.stream.overview", Method: plugin.MethodGet, Path: "/streams/{stream}", Permission: "nats.streams.read", Risk: plugin.RiskSafe, AuditEvent: "nats.stream.overview", Handle: streamOverview},
		{ID: "nats.stream.create", Method: plugin.MethodPost, Path: "/streams", Permission: "nats.streams.write", Risk: plugin.RiskWrite, AuditEvent: "nats.stream.create", Input: streamCreateSchema(), Handle: createStream},
		{ID: "nats.stream.update", Method: plugin.MethodPut, Path: "/streams/{stream}", Permission: "nats.streams.write", Risk: plugin.RiskWrite, AuditEvent: "nats.stream.update", Input: streamUpdateSchema(), Handle: updateStream},
		{ID: "nats.stream.purge", Method: plugin.MethodDelete, Path: "/streams/{stream}/messages", Permission: "nats.streams.delete", Risk: plugin.RiskDestructive, AuditEvent: "nats.stream.purge", Handle: purgeStream},
		{ID: "nats.stream.delete", Method: plugin.MethodDelete, Path: "/streams/{stream}", Permission: "nats.streams.delete", Risk: plugin.RiskDestructive, AuditEvent: "nats.stream.delete", Handle: deleteStream},
		{ID: "nats.consumers.list", Method: plugin.MethodGet, Path: "/consumers", Permission: "nats.consumers.read", Risk: plugin.RiskSafe, AuditEvent: "nats.consumers.list", Handle: listConsumers},
		{ID: "nats.consumer.overview", Method: plugin.MethodGet, Path: "/streams/{stream}/consumers/{consumer}", Permission: "nats.consumers.read", Risk: plugin.RiskSafe, AuditEvent: "nats.consumer.overview", Handle: consumerOverview},
		{ID: "nats.consumer.create", Method: plugin.MethodPost, Path: "/streams/{stream}/consumers", Permission: "nats.consumers.write", Risk: plugin.RiskWrite, AuditEvent: "nats.consumer.create", Input: consumerCreateSchema(), Handle: createConsumer},
		{ID: "nats.consumer.delete", Method: plugin.MethodDelete, Path: "/streams/{stream}/consumers/{consumer}", Permission: "nats.consumers.delete", Risk: plugin.RiskDestructive, AuditEvent: "nats.consumer.delete", Handle: deleteConsumer},
		{ID: "nats.messages.list", Method: plugin.MethodGet, Path: "/streams/{stream}/messages", Permission: "nats.messages.read", Risk: plugin.RiskSafe, AuditEvent: "nats.messages.list", Handle: listMessages},
		{ID: "nats.message.publish", Method: plugin.MethodPost, Path: "/messages", Permission: "nats.messages.write", Risk: plugin.RiskWrite, AuditEvent: "nats.message.publish", Input: publishSchema(), Handle: publishMessage},
		{ID: "nats.message.delete", Method: plugin.MethodDelete, Path: "/streams/{stream}/messages/{sequence}", Permission: "nats.messages.delete", Risk: plugin.RiskDestructive, AuditEvent: "nats.message.delete", Handle: deleteMessage},
	}
}

func streamCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Stream", Fields: []plugin.Field{
		{Key: "name", Label: "Stream name", Type: plugin.FieldText, Required: true},
		{Key: "subjects", Label: "Subjects", Type: plugin.FieldTextarea, Required: true, Placeholder: "orders.*\npayments.>"},
		{Key: "storage", Label: "Storage", Type: plugin.FieldSelect, Required: true, Default: "file", Options: []plugin.Option{{Label: "File", Value: "file"}, {Label: "Memory", Value: "memory"}}},
		{Key: "replicas", Label: "Replicas", Type: plugin.FieldNumber, Default: 1, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 5}}},
		{Key: "max_msgs", Label: "Max messages", Type: plugin.FieldNumber, Default: -1},
		{Key: "max_bytes", Label: "Max bytes", Type: plugin.FieldNumber, Default: -1},
	}}}}
}

func streamUpdateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Stream", Fields: []plugin.Field{
		{Key: "subjects", Label: "Subjects", Type: plugin.FieldTextarea, Placeholder: "orders.*\npayments.>", Help: "Leave blank to keep the current subjects."},
		{Key: "replicas", Label: "Replicas", Type: plugin.FieldNumber, Validators: []plugin.Validator{{Type: plugin.ValidatorMin, Value: 1}, {Type: plugin.ValidatorMax, Value: 5}}},
		{Key: "max_msgs", Label: "Max messages", Type: plugin.FieldNumber, Help: "-1 for unlimited."},
		{Key: "max_bytes", Label: "Max bytes", Type: plugin.FieldNumber, Help: "-1 for unlimited."},
		{Key: "max_age", Label: "Max age", Type: plugin.FieldDuration, Placeholder: "24h", Help: "Empty or 0 keeps messages forever."},
	}}}}
}

func consumerCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Consumer", Fields: []plugin.Field{
		{Key: "name", Label: "Consumer name", Type: plugin.FieldText, Required: true},
		{Key: "filter_subject", Label: "Filter subject", Type: plugin.FieldText},
		{Key: "ack_policy", Label: "Ack policy", Type: plugin.FieldSelect, Required: true, Default: "explicit", Options: []plugin.Option{{Label: "Explicit", Value: "explicit"}, {Label: "All", Value: "all"}, {Label: "None", Value: "none"}}},
	}}}}
}

func publishSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Message", Fields: []plugin.Field{
		{Key: "subject", Label: "Subject", Type: plugin.FieldText, Required: true},
		{Key: "data", Label: "Data", Type: plugin.FieldTextarea, Required: true},
		{Key: "encoding", Label: "Encoding", Type: plugin.FieldSelect, Required: true, Default: "string", Options: []plugin.Option{{Label: "String", Value: "string"}, {Label: "Base64", Value: "base64"}}},
		{Key: "jetstream", Label: "Require JetStream ack", Type: plugin.FieldToggle, Default: true},
		{Key: "headers", Label: "Headers", Type: plugin.FieldJSON},
	}}}}
}

func natsSession(rc *plugin.RequestContext) (*Session, error) {
	return unwrap(rc.Session)
}

func overview(rc *plugin.RequestContext) (any, error) {
	s, err := natsSession(rc)
	if err != nil {
		return nil, err
	}
	js, _ := s.conn.ConnectedServerJetStream()
	stats := s.conn.Stats()
	return row{
		"server_name":    s.conn.ConnectedServerName(),
		"server_id":      s.conn.ConnectedServerId(),
		"server_version": s.conn.ConnectedServerVersion(),
		"url":            s.conn.ConnectedUrlRedacted(),
		"status":         fmt.Sprint(s.conn.Status()),
		"jetstream":      js,
		"in_msgs":        stats.InMsgs,
		"out_msgs":       stats.OutMsgs,
		"in_bytes":       stats.InBytes,
		"out_bytes":      stats.OutBytes,
		"subscriptions":  s.conn.NumSubscriptions(),
		"readOnly":       s.opts.ReadOnly,
	}, nil
}

func treeStreams(rc *plugin.RequestContext) (any, error) {
	res, err := listStreams(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name := fmt.Sprint(item["name"])
		ref := plugin.ResourceRef{Kind: "stream", Name: name, UID: name}
		nodes = append(nodes, plugin.TreeNode{Key: "stream:" + name, Label: name, Icon: icon("waves"), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

func listStreams(rc *plugin.RequestContext) (any, error) {
	s, err := natsSession(rc)
	if err != nil {
		return nil, err
	}
	rows := []row{}
	for info := range s.js.Streams() {
		if info == nil {
			continue
		}
		rows = append(rows, streamRow(info))
	}
	sort.Slice(rows, func(i, j int) bool { return fmt.Sprint(rows[i]["name"]) < fmt.Sprint(rows[j]["name"]) })
	return broker.PageRows(rc, rows)
}

func streamOverview(rc *plugin.RequestContext) (any, error) {
	s, err := natsSession(rc)
	if err != nil {
		return nil, err
	}
	info, err := s.js.StreamInfo(streamParam(rc))
	if err != nil {
		return nil, natsErr(err)
	}
	return info, nil
}

func createStream(rc *plugin.RequestContext) (any, error) {
	s, err := natsSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Name     string `json:"name"`
		Subjects string `json:"subjects"`
		Storage  string `json:"storage"`
		Replicas int    `json:"replicas"`
		MaxMsgs  int64  `json:"max_msgs"`
		MaxBytes int64  `json:"max_bytes"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	storage := natsclient.FileStorage
	if req.Storage == "memory" {
		storage = natsclient.MemoryStorage
	}
	_, err = s.js.AddStream(&natsclient.StreamConfig{Name: req.Name, Subjects: splitSubjects(req.Subjects), Storage: storage, Replicas: req.Replicas, MaxMsgs: req.MaxMsgs, MaxBytes: req.MaxBytes})
	return actionResult{OK: err == nil}, natsErr(err)
}

type streamUpdateRequest struct {
	Subjects *string `json:"subjects"`
	Replicas *int    `json:"replicas"`
	MaxMsgs  *int64  `json:"max_msgs"`
	MaxBytes *int64  `json:"max_bytes"`
	MaxAge   *string `json:"max_age"`
}

func updateStream(rc *plugin.RequestContext) (any, error) {
	s, err := natsSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	name := streamParam(rc)
	if name == "" {
		return nil, fmt.Errorf("%w: stream name is required", plugin.ErrInvalidInput)
	}
	var req streamUpdateRequest
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	info, err := s.js.StreamInfo(name)
	if err != nil {
		return nil, natsErr(err)
	}
	cfg, err := applyStreamUpdate(info.Config, req)
	if err != nil {
		return nil, err
	}
	_, err = s.js.UpdateStream(&cfg)
	return actionResult{OK: err == nil}, natsErr(err)
}

// applyStreamUpdate overlays the provided changes onto the stream's current
// config so unspecified fields are preserved and immutable fields untouched.
func applyStreamUpdate(current natsclient.StreamConfig, req streamUpdateRequest) (natsclient.StreamConfig, error) {
	cfg := current
	if req.Subjects != nil {
		subjects := splitSubjects(*req.Subjects)
		if len(subjects) == 0 {
			return natsclient.StreamConfig{}, fmt.Errorf("%w: at least one subject is required", plugin.ErrInvalidInput)
		}
		cfg.Subjects = subjects
	}
	if req.Replicas != nil {
		if *req.Replicas < 1 || *req.Replicas > 5 {
			return natsclient.StreamConfig{}, fmt.Errorf("%w: replicas must be between 1 and 5", plugin.ErrInvalidInput)
		}
		cfg.Replicas = *req.Replicas
	}
	if req.MaxMsgs != nil {
		cfg.MaxMsgs = *req.MaxMsgs
	}
	if req.MaxBytes != nil {
		cfg.MaxBytes = *req.MaxBytes
	}
	if req.MaxAge != nil {
		age, err := parseMaxAge(*req.MaxAge)
		if err != nil {
			return natsclient.StreamConfig{}, err
		}
		cfg.MaxAge = age
	}
	return cfg, nil
}

func parseMaxAge(raw string) (time.Duration, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed == "0" {
		return 0, nil
	}
	age, err := time.ParseDuration(trimmed)
	if err != nil {
		return 0, fmt.Errorf("%w: max_age must be a duration (e.g. 24h)", plugin.ErrInvalidInput)
	}
	if age < 0 {
		return 0, fmt.Errorf("%w: max_age cannot be negative", plugin.ErrInvalidInput)
	}
	return age, nil
}

func purgeStream(rc *plugin.RequestContext) (any, error) {
	s, err := natsSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	err = s.js.PurgeStream(streamParam(rc))
	return actionResult{OK: err == nil}, natsErr(err)
}

func deleteStream(rc *plugin.RequestContext) (any, error) {
	s, err := natsSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	err = s.js.DeleteStream(streamParam(rc))
	return actionResult{OK: err == nil}, natsErr(err)
}

func listConsumers(rc *plugin.RequestContext) (any, error) {
	s, err := natsSession(rc)
	if err != nil {
		return nil, err
	}
	stream := streamParam(rc)
	streams := []string{stream}
	if stream == "" {
		streams = nil
		for name := range s.js.StreamNames() {
			streams = append(streams, name)
		}
		sort.Strings(streams)
	}
	rows := []row{}
	for _, streamName := range streams {
		if streamName == "" {
			continue
		}
		for info := range s.js.Consumers(streamName) {
			if info != nil {
				rows = append(rows, consumerRow(info))
			}
		}
	}
	return broker.PageRows(rc, rows)
}

func consumerOverview(rc *plugin.RequestContext) (any, error) {
	s, err := natsSession(rc)
	if err != nil {
		return nil, err
	}
	info, err := s.js.ConsumerInfo(streamParam(rc), consumerParam(rc))
	if err != nil {
		return nil, natsErr(err)
	}
	return info, nil
}

func createConsumer(rc *plugin.RequestContext) (any, error) {
	s, err := natsSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Name          string `json:"name"`
		FilterSubject string `json:"filter_subject"`
		AckPolicy     string `json:"ack_policy"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	ack := natsclient.AckExplicitPolicy
	switch req.AckPolicy {
	case "all":
		ack = natsclient.AckAllPolicy
	case "none":
		ack = natsclient.AckNonePolicy
	}
	_, err = s.js.AddConsumer(streamParam(rc), &natsclient.ConsumerConfig{Durable: req.Name, Name: req.Name, FilterSubject: req.FilterSubject, AckPolicy: ack})
	return actionResult{OK: err == nil}, natsErr(err)
}

func deleteConsumer(rc *plugin.RequestContext) (any, error) {
	s, err := natsSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	err = s.js.DeleteConsumer(streamParam(rc), consumerParam(rc))
	return actionResult{OK: err == nil}, natsErr(err)
}

func listMessages(rc *plugin.RequestContext) (any, error) {
	s, err := natsSession(rc)
	if err != nil {
		return nil, err
	}
	info, err := s.js.StreamInfo(streamParam(rc))
	if err != nil {
		return nil, natsErr(err)
	}
	req, err := rc.Page()
	if err != nil {
		return nil, err
	}
	limit := req.Limit
	if limit > s.opts.MessageLimit {
		limit = s.opts.MessageLimit
	}
	rows := []row{}
	start := info.State.FirstSeq
	if info.State.LastSeq >= uint64(limit) && info.State.LastSeq-uint64(limit)+1 > start {
		start = info.State.LastSeq - uint64(limit) + 1
	}
	for seq := start; seq <= info.State.LastSeq && len(rows) < limit; seq++ {
		msg, err := s.js.GetMsg(info.Config.Name, seq)
		if err != nil {
			continue
		}
		rows = append(rows, messageRow(info.Config.Name, msg))
	}
	return broker.PageRows(rc, rows)
}

func publishMessage(rc *plugin.RequestContext) (any, error) {
	s, err := natsSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Subject   string            `json:"subject"`
		Data      string            `json:"data"`
		Encoding  string            `json:"encoding"`
		JetStream bool              `json:"jetstream"`
		Headers   map[string]string `json:"headers"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	data := []byte(req.Data)
	if req.Encoding == "base64" {
		data, err = base64.StdEncoding.DecodeString(req.Data)
		if err != nil {
			return nil, fmt.Errorf("%w: data is not valid base64", plugin.ErrInvalidInput)
		}
	}
	msg := natsclient.NewMsg(req.Subject)
	msg.Data = data
	for k, v := range req.Headers {
		msg.Header.Set(k, v)
	}
	if req.JetStream {
		ack, err := s.js.PublishMsg(msg)
		if err != nil {
			return nil, natsErr(err)
		}
		return row{"ok": true, "stream": ack.Stream, "sequence": ack.Sequence}, nil
	}
	if err := s.conn.PublishMsg(msg); err != nil {
		return nil, natsErr(err)
	}
	return actionResult{OK: true}, nil
}

func deleteMessage(rc *plugin.RequestContext) (any, error) {
	s, err := natsSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	seq, err := strconv.ParseUint(rc.Param("sequence"), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid sequence", plugin.ErrInvalidInput)
	}
	err = s.js.DeleteMsg(streamParam(rc), seq)
	return actionResult{OK: err == nil}, natsErr(err)
}

func streamRow(info *natsclient.StreamInfo) row {
	return row{
		"name":      info.Config.Name,
		"subjects":  info.Config.Subjects,
		"messages":  info.State.Msgs,
		"bytes":     info.State.Bytes,
		"consumers": info.State.Consumers,
		"storage":   fmt.Sprint(info.Config.Storage),
		"created":   info.Created,
		"ref":       plugin.ResourceRef{Kind: "stream", Name: info.Config.Name, UID: info.Config.Name},
	}
}

func consumerRow(info *natsclient.ConsumerInfo) row {
	return row{
		"name":           info.Name,
		"stream":         info.Stream,
		"deliver_policy": info.Config.DeliverPolicy,
		"ack_policy":     info.Config.AckPolicy,
		"pending":        info.NumPending,
		"ack_pending":    info.NumAckPending,
		"created":        info.Created,
		"ref":            plugin.ResourceRef{Kind: "consumer", Namespace: info.Stream, Name: info.Name, UID: info.Stream + "/" + info.Name},
	}
}

func messageRow(stream string, msg *natsclient.RawStreamMsg) row {
	headers := map[string][]string(msg.Header)
	return row{
		"stream":   stream,
		"sequence": msg.Sequence,
		"subject":  msg.Subject,
		"time":     msg.Time,
		"headers":  headers,
		"data":     string(msg.Data),
	}
}

func streamParam(rc *plugin.RequestContext) string {
	if v := strings.TrimSpace(rc.Param("stream")); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p.stream"))
}

func consumerParam(rc *plugin.RequestContext) string {
	if v := strings.TrimSpace(rc.Param("consumer")); v != "" {
		return v
	}
	return strings.TrimSpace(rc.Query().Get("p.consumer"))
}

func splitSubjects(raw string) []string {
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == '\n' || r == '\r' || r == '\t'
	})
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if s := strings.TrimSpace(part); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func natsErr(err error) error {
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "not found") {
		return fmt.Errorf("%w: %v", plugin.ErrNotFound, err)
	}
	if strings.Contains(err.Error(), "already") {
		return fmt.Errorf("%w: %v", plugin.ErrAlreadyExists, err)
	}
	return fmt.Errorf("%w: %v", plugin.ErrUnavailable, err)
}
