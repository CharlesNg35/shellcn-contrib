package rabbitmq

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

type row = plugin.TableRow

type actionResult struct {
	OK bool `json:"ok"`
}

func routes() []plugin.Route {
	return []plugin.Route{
		{ID: "rabbitmq.overview", Method: plugin.MethodGet, Path: "/overview", Permission: "rabbitmq.read", Risk: plugin.RiskSafe, AuditEvent: "rabbitmq.overview", Handle: overview},
		{ID: "rabbitmq.queues.tree", Method: plugin.MethodGet, Path: "/tree/queues", Permission: "rabbitmq.queues.read", Risk: plugin.RiskSafe, AuditEvent: "rabbitmq.queues.tree", Handle: treeQueues},
		{ID: "rabbitmq.exchanges.tree", Method: plugin.MethodGet, Path: "/tree/exchanges", Permission: "rabbitmq.exchanges.read", Risk: plugin.RiskSafe, AuditEvent: "rabbitmq.exchanges.tree", Handle: treeExchanges},
		{ID: "rabbitmq.queues.list", Method: plugin.MethodGet, Path: "/queues", Permission: "rabbitmq.queues.read", Risk: plugin.RiskSafe, AuditEvent: "rabbitmq.queues.list", Handle: listQueues},
		{ID: "rabbitmq.queue.overview", Method: plugin.MethodGet, Path: "/queues/{vhost}/{queue}", Permission: "rabbitmq.queues.read", Risk: plugin.RiskSafe, AuditEvent: "rabbitmq.queue.overview", Handle: queueOverview},
		{ID: "rabbitmq.queue.messages", Method: plugin.MethodGet, Path: "/queues/{vhost}/{queue}/messages", Permission: "rabbitmq.messages.read", Risk: plugin.RiskSafe, AuditEvent: "rabbitmq.queue.messages", Handle: queueMessages},
		{ID: "rabbitmq.queue.create", Method: plugin.MethodPost, Path: "/queues", Permission: "rabbitmq.queues.write", Risk: plugin.RiskWrite, AuditEvent: "rabbitmq.queue.create", Input: queueCreateSchema(), Handle: createQueue},
		{ID: "rabbitmq.queue.purge", Method: plugin.MethodDelete, Path: "/queues/{vhost}/{queue}/contents", Permission: "rabbitmq.queues.delete", Risk: plugin.RiskDestructive, AuditEvent: "rabbitmq.queue.purge", Handle: purgeQueue},
		{ID: "rabbitmq.queue.delete", Method: plugin.MethodDelete, Path: "/queues/{vhost}/{queue}", Permission: "rabbitmq.queues.delete", Risk: plugin.RiskDestructive, AuditEvent: "rabbitmq.queue.delete", Handle: deleteQueue},
		{ID: "rabbitmq.exchanges.list", Method: plugin.MethodGet, Path: "/exchanges", Permission: "rabbitmq.exchanges.read", Risk: plugin.RiskSafe, AuditEvent: "rabbitmq.exchanges.list", Handle: listExchanges},
		{ID: "rabbitmq.exchange.overview", Method: plugin.MethodGet, Path: "/exchanges/{vhost}/{exchange}", Permission: "rabbitmq.exchanges.read", Risk: plugin.RiskSafe, AuditEvent: "rabbitmq.exchange.overview", Handle: exchangeOverview},
		{ID: "rabbitmq.exchange.bindings", Method: plugin.MethodGet, Path: "/exchanges/{vhost}/{exchange}/bindings", Permission: "rabbitmq.bindings.read", Risk: plugin.RiskSafe, AuditEvent: "rabbitmq.exchange.bindings", Handle: exchangeBindings},
		{ID: "rabbitmq.exchange.create", Method: plugin.MethodPost, Path: "/exchanges", Permission: "rabbitmq.exchanges.write", Risk: plugin.RiskWrite, AuditEvent: "rabbitmq.exchange.create", Input: exchangeCreateSchema(), Handle: createExchange},
		{ID: "rabbitmq.exchange.delete", Method: plugin.MethodDelete, Path: "/exchanges/{vhost}/{exchange}", Permission: "rabbitmq.exchanges.delete", Risk: plugin.RiskDestructive, AuditEvent: "rabbitmq.exchange.delete", Handle: deleteExchange},
		{ID: "rabbitmq.bindings.list", Method: plugin.MethodGet, Path: "/queues/{vhost}/{queue}/bindings", Permission: "rabbitmq.bindings.read", Risk: plugin.RiskSafe, AuditEvent: "rabbitmq.bindings.list", Handle: queueBindings},
		{ID: "rabbitmq.binding.create", Method: plugin.MethodPost, Path: "/queues/{vhost}/{queue}/bindings", Permission: "rabbitmq.bindings.write", Risk: plugin.RiskWrite, AuditEvent: "rabbitmq.binding.create", Input: bindingCreateSchema(), Handle: createBinding},
		{ID: "rabbitmq.binding.delete", Method: plugin.MethodDelete, Path: "/bindings/{spec}", Permission: "rabbitmq.bindings.delete", Risk: plugin.RiskDestructive, AuditEvent: "rabbitmq.binding.delete", Handle: deleteBinding},
		{ID: "rabbitmq.consumers.list", Method: plugin.MethodGet, Path: "/consumers", Permission: "rabbitmq.consumers.read", Risk: plugin.RiskSafe, AuditEvent: "rabbitmq.consumers.list", Handle: listConsumers},
		{ID: "rabbitmq.message.publish", Method: plugin.MethodPost, Path: "/exchanges/{vhost}/{exchange}/publish", Permission: "rabbitmq.messages.write", Risk: plugin.RiskWrite, AuditEvent: "rabbitmq.message.publish", Input: publishSchema(), Handle: publishMessage},
		{ID: "rabbitmq.queue.publish", Method: plugin.MethodPost, Path: "/queues/{vhost}/{queue}/publish", Permission: "rabbitmq.messages.write", Risk: plugin.RiskWrite, AuditEvent: "rabbitmq.queue.publish", Input: publishSchema(), Handle: publishQueueMessage},
	}
}

func queueCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Queue", Fields: []plugin.Field{
		{Key: "vhost", Label: "Virtual host", Type: plugin.FieldText, Default: "/", Required: true},
		{Key: "name", Label: "Queue name", Type: plugin.FieldText, Required: true},
		{Key: "durable", Label: "Durable", Type: plugin.FieldToggle, Default: true},
		{Key: "auto_delete", Label: "Auto delete", Type: plugin.FieldToggle, Default: false},
		{Key: "arguments", Label: "Arguments", Type: plugin.FieldJSON},
	}}}}
}

func exchangeCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Exchange", Fields: []plugin.Field{
		{Key: "vhost", Label: "Virtual host", Type: plugin.FieldText, Default: "/", Required: true},
		{Key: "name", Label: "Exchange name", Type: plugin.FieldText, Required: true},
		{Key: "type", Label: "Type", Type: plugin.FieldSelect, Required: true, Default: "direct", Options: []plugin.Option{{Label: "Direct", Value: "direct"}, {Label: "Fanout", Value: "fanout"}, {Label: "Topic", Value: "topic"}, {Label: "Headers", Value: "headers"}}},
		{Key: "durable", Label: "Durable", Type: plugin.FieldToggle, Default: true},
		{Key: "auto_delete", Label: "Auto delete", Type: plugin.FieldToggle, Default: false},
		{Key: "internal", Label: "Internal", Type: plugin.FieldToggle, Default: false},
		{Key: "arguments", Label: "Arguments", Type: plugin.FieldJSON},
	}}}}
}

func bindingCreateSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Binding", Fields: []plugin.Field{
		{Key: "source", Label: "Source exchange", Type: plugin.FieldSelect, Required: true, OptionsSource: &plugin.DataSource{RouteID: "rabbitmq.exchanges.list"}, Help: "Exchange to bind this queue to."},
		{Key: "routing_key", Label: "Routing key", Type: plugin.FieldText, Help: "Direct/topic routing key; ignored for fanout exchanges."},
		{Key: "arguments", Label: "Arguments", Type: plugin.FieldJSON, Help: "Optional binding arguments (used by headers exchanges)."},
	}}}}
}

func publishSchema() *plugin.Schema {
	return &plugin.Schema{Groups: []plugin.Group{{Name: "Message", Fields: []plugin.Field{
		{Key: "routing_key", Label: "Routing key", Type: plugin.FieldText},
		{Key: "payload", Label: "Payload", Type: plugin.FieldTextarea, Required: true},
		{Key: "payload_encoding", Label: "Payload encoding", Type: plugin.FieldSelect, Required: true, Default: "string", Options: []plugin.Option{{Label: "String", Value: "string"}, {Label: "Base64", Value: "base64"}}},
		{Key: "properties", Label: "Properties", Type: plugin.FieldJSON},
	}}}}
}

func rabbitSession(rc *plugin.RequestContext) (*Session, error) {
	return unwrap(rc.Session)
}

func overview(rc *plugin.RequestContext) (any, error) {
	s, err := rabbitSession(rc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := commandContext(rc.Ctx, s)
	defer cancel()
	var out row
	if err := s.get(ctx, "/api/overview", &out); err != nil {
		return nil, err
	}
	out["vhost"] = s.opts.VHost
	out["readOnly"] = s.opts.ReadOnly
	return out, nil
}

func treeQueues(rc *plugin.RequestContext) (any, error) {
	res, err := listQueues(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name, vhost := fmt.Sprint(item["name"]), fmt.Sprint(item["vhost"])
		ref := plugin.ResourceIdentity{Kind: "queue", Namespace: vhost, Name: name, UID: vhost + "/" + name}
		nodes = append(nodes, plugin.TreeNode{Key: "queue:" + ref.UID, Label: name, Icon: icon("list"), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

func treeExchanges(rc *plugin.RequestContext) (any, error) {
	res, err := listExchanges(rc)
	if err != nil {
		return nil, err
	}
	page := res.(plugin.Page[row])
	nodes := make([]plugin.TreeNode, 0, len(page.Items))
	for _, item := range page.Items {
		name, vhost := fmt.Sprint(item["name"]), fmt.Sprint(item["vhost"])
		ref := plugin.ResourceIdentity{Kind: "exchange", Namespace: vhost, Name: name, UID: vhost + "/" + name}
		nodes = append(nodes, plugin.TreeNode{Key: "exchange:" + ref.UID, Label: name, Icon: icon("shuffle"), Ref: &ref, Leaf: true})
	}
	return plugin.Page[plugin.TreeNode]{Items: nodes, NextCursor: page.NextCursor, Total: page.Total}, nil
}

func listQueues(rc *plugin.RequestContext) (any, error) {
	s, err := rabbitSession(rc)
	if err != nil {
		return nil, err
	}
	var rows []row
	if err := s.get(rc.Ctx, "/api/queues/"+apiVHost(s.opts.VHost), &rows); err != nil {
		return nil, err
	}
	for _, r := range rows {
		r["ref"] = plugin.ResourceIdentity{Kind: "queue", Namespace: fmt.Sprint(r["vhost"]), Name: fmt.Sprint(r["name"]), UID: fmt.Sprint(r["vhost"]) + "/" + fmt.Sprint(r["name"])}
	}
	return broker.PageRows(rc, rows)
}

func queueOverview(rc *plugin.RequestContext) (any, error) {
	s, err := rabbitSession(rc)
	if err != nil {
		return nil, err
	}
	var out row
	err = s.get(rc.Ctx, "/api/queues/"+apiVHost(vhostParam(rc))+"/"+apiName(rc.Param("queue")), &out)
	return out, err
}

func queueMessages(rc *plugin.RequestContext) (any, error) {
	s, err := rabbitSession(rc)
	if err != nil {
		return nil, err
	}
	req, err := rc.Page()
	if err != nil {
		return nil, err
	}
	count := req.Limit
	if count > s.opts.MessageLimit {
		count = s.opts.MessageLimit
	}
	body := map[string]any{"count": count, "ackmode": "ack_requeue_true", "encoding": "auto", "truncate": 65536}
	var rows []row
	if err := s.post(rc.Ctx, "/api/queues/"+apiVHost(vhostParam(rc))+"/"+apiName(rc.Param("queue"))+"/get", body, &rows); err != nil {
		return nil, err
	}
	return broker.PageRows(rc, rows)
}

func createQueue(rc *plugin.RequestContext) (any, error) {
	s, err := rabbitSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		VHost      string         `json:"vhost"`
		Name       string         `json:"name"`
		Durable    bool           `json:"durable"`
		AutoDelete bool           `json:"auto_delete"`
		Arguments  map[string]any `json:"arguments"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.VHost) == "" {
		req.VHost = "/"
	}
	body := map[string]any{"durable": req.Durable, "auto_delete": req.AutoDelete, "arguments": nonNilMap(req.Arguments)}
	return actionResult{OK: true}, s.put(rc.Ctx, "/api/queues/"+apiVHost(req.VHost)+"/"+apiName(req.Name), body, nil)
}

func purgeQueue(rc *plugin.RequestContext) (any, error) {
	s, err := rabbitSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, s.delete(rc.Ctx, "/api/queues/"+apiVHost(vhostParam(rc))+"/"+apiName(rc.Param("queue"))+"/contents")
}

func deleteQueue(rc *plugin.RequestContext) (any, error) {
	s, err := rabbitSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, s.delete(rc.Ctx, "/api/queues/"+apiVHost(vhostParam(rc))+"/"+apiName(rc.Param("queue")))
}

func listExchanges(rc *plugin.RequestContext) (any, error) {
	s, err := rabbitSession(rc)
	if err != nil {
		return nil, err
	}
	var rows []row
	if err := s.get(rc.Ctx, "/api/exchanges/"+apiVHost(s.opts.VHost), &rows); err != nil {
		return nil, err
	}
	filtered := rows[:0]
	for _, r := range rows {
		name := fmt.Sprint(r["name"])
		if name == "" {
			continue
		}
		r["ref"] = plugin.ResourceIdentity{Kind: "exchange", Namespace: fmt.Sprint(r["vhost"]), Name: name, UID: fmt.Sprint(r["vhost"]) + "/" + name}
		filtered = append(filtered, r)
	}
	return broker.PageRows(rc, filtered)
}

func exchangeOverview(rc *plugin.RequestContext) (any, error) {
	s, err := rabbitSession(rc)
	if err != nil {
		return nil, err
	}
	var out row
	err = s.get(rc.Ctx, "/api/exchanges/"+apiVHost(vhostParam(rc))+"/"+apiName(rc.Param("exchange")), &out)
	return out, err
}

func exchangeBindings(rc *plugin.RequestContext) (any, error) {
	return bindingRequest(rc, "/api/exchanges/"+apiVHost(vhostParam(rc))+"/"+apiName(rc.Param("exchange"))+"/bindings/source")
}

func createExchange(rc *plugin.RequestContext) (any, error) {
	s, err := rabbitSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		VHost      string         `json:"vhost"`
		Name       string         `json:"name"`
		Type       string         `json:"type"`
		Durable    bool           `json:"durable"`
		AutoDelete bool           `json:"auto_delete"`
		Internal   bool           `json:"internal"`
		Arguments  map[string]any `json:"arguments"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.VHost) == "" {
		req.VHost = "/"
	}
	body := map[string]any{"type": req.Type, "durable": req.Durable, "auto_delete": req.AutoDelete, "internal": req.Internal, "arguments": nonNilMap(req.Arguments)}
	return actionResult{OK: true}, s.put(rc.Ctx, "/api/exchanges/"+apiVHost(req.VHost)+"/"+apiName(req.Name), body, nil)
}

func deleteExchange(rc *plugin.RequestContext) (any, error) {
	s, err := rabbitSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	return actionResult{OK: true}, s.delete(rc.Ctx, "/api/exchanges/"+apiVHost(vhostParam(rc))+"/"+apiName(rc.Param("exchange")))
}

func queueBindings(rc *plugin.RequestContext) (any, error) {
	return bindingRequest(rc, "/api/queues/"+apiVHost(vhostParam(rc))+"/"+apiName(rc.Param("queue"))+"/bindings")
}

func bindingRequest(rc *plugin.RequestContext, path string) (any, error) {
	s, err := rabbitSession(rc)
	if err != nil {
		return nil, err
	}
	var rows []row
	if err := s.get(rc.Ctx, path, &rows); err != nil {
		return nil, err
	}
	vhost := vhostParam(rc)
	for _, r := range rows {
		spec := bindingSpec{
			VHost:       vhost,
			Source:      fmt.Sprint(r["source"]),
			Destination: fmt.Sprint(r["destination"]),
			DestType:    fmt.Sprint(r["destination_type"]),
			Props:       fmt.Sprint(r["properties_key"]),
		}
		r["spec"] = spec.encode()
	}
	return broker.PageRows(rc, rows)
}

type bindingSpec struct {
	VHost       string `json:"v"`
	Source      string `json:"s"`
	Destination string `json:"d"`
	DestType    string `json:"t"`
	Props       string `json:"p"`
}

func (b bindingSpec) encode() string {
	data, _ := json.Marshal(b)
	return base64.RawURLEncoding.EncodeToString(data)
}

func decodeBindingSpec(token string) (bindingSpec, error) {
	var b bindingSpec
	data, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(token))
	if err != nil {
		return b, fmt.Errorf("%w: invalid binding reference", plugin.ErrInvalidInput)
	}
	if err := json.Unmarshal(data, &b); err != nil {
		return b, fmt.Errorf("%w: invalid binding reference", plugin.ErrInvalidInput)
	}
	return b, nil
}

func createBinding(rc *plugin.RequestContext) (any, error) {
	s, err := rabbitSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		Source     string         `json:"source"`
		RoutingKey string         `json:"routing_key"`
		Arguments  map[string]any `json:"arguments"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	source := strings.TrimSpace(req.Source)
	if source == "" {
		return nil, fmt.Errorf("%w: source exchange is required", plugin.ErrInvalidInput)
	}
	vhost := vhostParam(rc)
	queue := rc.Param("queue")
	body := map[string]any{"routing_key": req.RoutingKey, "arguments": nonNilMap(req.Arguments)}
	path := "/api/bindings/" + apiVHost(vhost) + "/e/" + apiName(source) + "/q/" + apiName(queue)
	return actionResult{OK: true}, s.post(rc.Ctx, path, body, nil)
}

func deleteBinding(rc *plugin.RequestContext) (any, error) {
	s, err := rabbitSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	spec, err := decodeBindingSpec(rc.Param("spec"))
	if err != nil {
		return nil, err
	}
	destSeg := "q"
	if spec.DestType == "exchange" {
		destSeg = "e"
	}
	path := "/api/bindings/" + apiVHost(spec.VHost) + "/e/" + apiName(spec.Source) + "/" + destSeg + "/" + apiName(spec.Destination) + "/" + apiName(spec.Props)
	return actionResult{OK: true}, s.delete(rc.Ctx, path)
}

func listConsumers(rc *plugin.RequestContext) (any, error) {
	s, err := rabbitSession(rc)
	if err != nil {
		return nil, err
	}
	var rows []row
	if err := s.get(rc.Ctx, "/api/consumers/"+apiVHost(s.opts.VHost), &rows); err != nil {
		return nil, err
	}
	queue := strings.TrimSpace(rc.Param("queue"))
	if queue == "" {
		queue = strings.TrimSpace(rc.Query().Get("p.queue"))
	}
	if queue != "" {
		filtered := rows[:0]
		for _, r := range rows {
			if q, ok := r["queue"].(map[string]any); ok && fmt.Sprint(q["name"]) == queue {
				filtered = append(filtered, r)
			}
		}
		rows = filtered
	}
	return broker.PageRows(rc, rows)
}

func publishMessage(rc *plugin.RequestContext) (any, error) {
	return publishToExchange(rc, rc.Param("exchange"), "")
}

func publishQueueMessage(rc *plugin.RequestContext) (any, error) {
	return publishToExchange(rc, "", rc.Param("queue"))
}

func publishToExchange(rc *plugin.RequestContext, exchange, defaultRoutingKey string) (any, error) {
	s, err := rabbitSession(rc)
	if err != nil {
		return nil, err
	}
	if err := ensureWritable(s); err != nil {
		return nil, err
	}
	var req struct {
		RoutingKey      string         `json:"routing_key"`
		Payload         string         `json:"payload"`
		PayloadEncoding string         `json:"payload_encoding"`
		Properties      map[string]any `json:"properties"`
	}
	if err := rc.Bind(&req); err != nil {
		return nil, err
	}
	if req.PayloadEncoding == "" {
		req.PayloadEncoding = "string"
	}
	if req.RoutingKey == "" {
		req.RoutingKey = defaultRoutingKey
	}
	body := map[string]any{"routing_key": req.RoutingKey, "payload": req.Payload, "payload_encoding": req.PayloadEncoding, "properties": nonNilMap(req.Properties)}
	var out row
	if err := s.post(rc.Ctx, "/api/exchanges/"+apiVHost(vhostParam(rc))+"/"+apiName(exchange)+"/publish", body, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Session) get(ctx context.Context, path string, out any) error {
	req, err := s.newRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	return s.do(req, out)
}

func (s *Session) post(ctx context.Context, path string, body any, out any) error {
	req, err := s.newRequest(ctx, http.MethodPost, path, body)
	if err != nil {
		return err
	}
	return s.do(req, out)
}

func (s *Session) put(ctx context.Context, path string, body any, out any) error {
	req, err := s.newRequest(ctx, http.MethodPut, path, body)
	if err != nil {
		return err
	}
	return s.do(req, out)
}

func (s *Session) delete(ctx context.Context, path string) error {
	req, err := s.newRequest(ctx, http.MethodDelete, path, nil)
	if err != nil {
		return err
	}
	return s.do(req, nil)
}

func vhostParam(rc *plugin.RequestContext) string {
	if v := strings.TrimSpace(rc.Param("vhost")); v != "" {
		return v
	}
	return "/"
}

func nonNilMap(in map[string]any) map[string]any {
	if in == nil {
		return map[string]any{}
	}
	return in
}

func ensureWritable(s *Session) error {
	if s.opts.ReadOnly {
		return fmt.Errorf("%w: connection is read-only", plugin.ErrForbidden)
	}
	return nil
}
