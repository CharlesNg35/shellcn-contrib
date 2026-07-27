package consul

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/hashicorp/consul/api"

	"github.com/charlesng35/shellcn-contrib/shared/broker"
	"github.com/charlesng35/shellcn/sdk/plugin"
)

const (
	kindCluster       = "cluster"
	kindKV            = "kv"
	kindService       = "service"
	kindInstance      = "instance"
	kindNode          = "node"
	kindCheck         = "check"
	kindSession       = "session"
	kindPreparedQuery = "prepared_query"
	kindIntention     = "intention"
	kindACLToken      = "acl_token"
	kindACLPolicy     = "acl_policy"
	kindACLRole       = "acl_role"
)

// secretPlaceholder stands in for material the gateway must never hand to a
// browser; the value itself never leaves Consul.
const secretPlaceholder = "hidden by ShellCN"

// maxCheckOutput bounds the last-result text carried in list rows.
const maxCheckOutput = 2048

type row = plugin.TableRow

func routes() []plugin.Route {
	return []plugin.Route{
		readRoute("cluster.list", "/cluster", "cluster.read", clusterList),
		readRoute("agent.self", "/agent/self", "agent.read", agentSelf),
		readRoute("agent.config", "/agent/config", "agent.read", agentConfig),
		readRoute("members.list", "/agent/members", "agent.read", membersList),
		readRoute("raft.list", "/operator/raft", "operator.read", raftList),
		readRoute("datacenters.list", "/catalog/datacenters", "catalog.read", datacentersList),

		readRoute("kv.tree", "/tree/kv", "kv.read", kvTree),
		readRoute("kv.list", "/kv", "kv.read", kvList),
		readRoute("kv.entries", "/kv/entries", "kv.read", kvEntries),
		readRoute("kv.read", "/kv/entry/{key}", "kv.read", kvRead),
		writeRoute("kv.write", plugin.MethodPut, "/kv/entry/{key}", "kv.write", plugin.RiskWrite, nil, kvWrite),
		writeRoute("kv.put", plugin.MethodPost, "/kv/entry", "kv.write", plugin.RiskWrite, kvPutSchema(), kvPut),
		writeRoute("kv.delete", plugin.MethodDelete, "/kv/entry/{key}", "kv.delete", plugin.RiskDestructive, nil, kvDelete),
		writeRoute("kv.tree.delete", plugin.MethodDelete, "/kv/tree", "kv.delete", plugin.RiskDestructive, nil, kvDeleteTree),
		streamRoute("kv.watch", "/kv/watch", "kv.read", kvWatch),

		readRoute("catalog.tree", "/tree/catalog", "catalog.read", catalogTree),
		readRoute("services.tree", "/tree/services", "catalog.read", servicesTree),
		readRoute("checks.tree", "/tree/checks", "health.read", checksTree),
		readRoute("mesh.tree", "/tree/mesh", "catalog.read", meshTree),
		readRoute("acl.tree", "/tree/acl", "acl.read", aclTree),

		readRoute("services.list", "/services", "catalog.read", servicesList),
		readRoute("service.read", "/services/{service}", "catalog.read", serviceRead),
		readRoute("service.instances", "/services/{service}/instances", "catalog.read", serviceInstances),
		readRoute("service.checks", "/services/{service}/checks", "health.read", serviceChecks),
		readRoute("instance.read", "/services/{service}/instances/{instance}", "catalog.read", instanceRead),
		writeRoute("instance.deregister", plugin.MethodDelete, "/services/{service}/instances/{instance}", "catalog.write", plugin.RiskDestructive, nil, instanceDeregister),

		readRoute("nodes.list", "/nodes", "catalog.read", nodesList),
		readRoute("node.read", "/nodes/{node}", "catalog.read", nodeRead),
		readRoute("node.services", "/nodes/{node}/services", "catalog.read", nodeServices),
		readRoute("node.checks", "/nodes/{node}/checks", "health.read", nodeChecks),

		readRoute("checks.list", "/checks", "health.read", checksList),
		readRoute("check.read", "/checks/{check}", "health.read", checkRead),

		readRoute("sessions.list", "/sessions", "sessions.read", sessionsList),
		readRoute("session.read", "/sessions/{session}", "sessions.read", sessionRead),
		writeRoute("session.destroy", plugin.MethodDelete, "/sessions/{session}", "sessions.write", plugin.RiskDestructive, nil, sessionDestroy),

		readRoute("queries.list", "/prepared-queries", "queries.read", queriesList),
		readRoute("query.read", "/prepared-queries/{query}", "queries.read", queryRead),
		readRoute("query.execute", "/prepared-queries/{query}/results", "queries.read", queryExecute),

		readRoute("intentions.list", "/intentions", "intentions.read", intentionsList),
		readRoute("intention.read", "/intentions/entry", "intentions.read", intentionRead),
		writeRoute("intention.toggle", plugin.MethodPost, "/intentions/toggle", "intentions.write", plugin.RiskWrite, nil, intentionToggle),
		writeRoute("intention.delete", plugin.MethodDelete, "/intentions/entry", "intentions.delete", plugin.RiskDestructive, nil, intentionDelete),

		readRoute("acl.tokens.list", "/acl/tokens", "acl.read", aclTokensList),
		readRoute("acl.token.read", "/acl/tokens/{token}", "acl.read", aclTokenRead),
		readRoute("acl.policies.list", "/acl/policies", "acl.read", aclPoliciesList),
		readRoute("acl.policy.read", "/acl/policies/{policy}", "acl.read", aclPolicyRead),
		readRoute("acl.roles.list", "/acl/roles", "acl.read", aclRolesList),
		readRoute("acl.role.read", "/acl/roles/{role}", "acl.read", aclRoleRead),
	}
}

func readRoute(id, path, permission string, handle plugin.Handler) plugin.Route {
	return plugin.Route{
		ID: rid(id), Method: plugin.MethodGet, Path: path,
		Permission: rid(permission), Risk: plugin.RiskSafe, AuditEvent: rid(id), Handle: handle,
	}
}

func writeRoute(id string, method plugin.Method, path, permission string, risk plugin.RiskLevel, input *plugin.Schema, handle plugin.Handler) plugin.Route {
	return plugin.Route{
		ID: rid(id), Method: method, Path: path,
		Permission: rid(permission), Risk: risk, AuditEvent: rid(id), Input: input, Handle: handle,
	}
}

func streamRoute(id, path, permission string, stream plugin.StreamHandler) plugin.Route {
	return plugin.Route{
		ID: rid(id), Method: plugin.MethodWS, Path: path,
		Permission: rid(permission), Risk: plugin.RiskSafe, AuditEvent: rid(id), Stream: stream,
	}
}

func clusterList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	q := s.query(rc)
	leader, err := s.client.Status().LeaderWithQueryOptions(q)
	if err != nil {
		return nil, consulErr(err)
	}
	status := "ready"
	if strings.TrimSpace(leader) == "" {
		status = "no-leader"
	}
	item := row{
		"ref":        plugin.ResourceIdentity{Kind: kindCluster, Name: "Cluster", UID: kindCluster},
		"name":       "Cluster",
		"endpoint":   s.endpoint(),
		"leader":     strings.Trim(leader, `"`),
		"datacenter": s.datacenter(rc),
	}
	if self, err := s.client.Agent().Self(); err == nil {
		item["version"] = stringField(self["Config"], "Version")
		if item["datacenter"] == "" {
			item["datacenter"] = stringField(self["Config"], "Datacenter")
		}
	}
	if nodes, _, err := s.client.Catalog().Nodes(q); err == nil {
		item["nodes"] = len(nodes)
	} else {
		status = "degraded"
	}
	if services, _, err := s.client.Catalog().Services(q); err == nil {
		item["services"] = len(services)
	} else {
		status = "degraded"
	}
	item["status"] = status
	total := 1
	return plugin.Page[row]{Items: []row{item}, Total: &total}, nil
}

func agentSelf(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	self, err := s.client.Agent().Self()
	if err != nil {
		return nil, consulErr(err)
	}
	config, debug, member := self["Config"], self["DebugConfig"], self["Member"]
	out := row{
		"node_name":          stringField(config, "NodeName"),
		"node_id":            stringField(config, "NodeID"),
		"datacenter":         stringField(config, "Datacenter"),
		"primary_datacenter": stringField(config, "PrimaryDatacenter"),
		"server":             boolField(config, "Server"),
		"version":            stringField(config, "Version"),
		"revision":           stringField(config, "Revision"),
		"endpoint":           s.endpoint(),
		"advertise_addr":     stringField(debug, "AdvertiseAddrLAN"),
		"bind_addr":          stringField(debug, "BindAddr"),
		"tagged_addresses":   debug["TaggedAddresses"],
		"meta":               self["Meta"],
		"read_only":          s.opts.ReadOnly,
		"allow_stale":        s.opts.AllowStale,
		"namespace":          s.opts.Namespace,
		"partition":          s.opts.Partition,
		"kv_prefix":          s.opts.KVPrefix,
	}
	if member != nil {
		out["gossip_tags"] = member["Tags"]
	}
	return out, nil
}

// agentConfig lists the agent's effective runtime configuration. Consul already
// replaces tokens, keys, and other secret material with "hidden" in this
// payload, so no plaintext secret can reach the browser through it.
func agentConfig(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	self, err := s.client.Agent().Self()
	if err != nil {
		return nil, consulErr(err)
	}
	settings := self["DebugConfig"]
	rows := make([]row, 0, len(settings))
	for key, value := range settings {
		rows = append(rows, row{"setting": key, "value": settingText(value)})
	}
	sortByKey(rows, "setting")
	return s.page(rc, rows)
}

func settingText(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case bool:
		return strconv.FormatBool(typed)
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprint(value)
	}
	return string(raw)
}

func membersList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	members, err := s.client.Agent().Members(false)
	if err != nil {
		return nil, consulErr(err)
	}
	rows := make([]row, 0, len(members))
	for _, m := range members {
		rows = append(rows, row{
			"name":       m.Name,
			"status":     memberStatus(m.Status),
			"address":    net.JoinHostPort(m.Addr, strconv.Itoa(int(m.Port))),
			"role":       m.Tags["role"],
			"datacenter": m.Tags["dc"],
			"build":      m.Tags["build"],
		})
	}
	sortByKey(rows, "name")
	return s.page(rc, rows)
}

func raftList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	config, err := s.client.Operator().RaftGetConfiguration(s.query(rc))
	if err != nil {
		if degraded(err) {
			return emptyPage(), nil
		}
		return nil, consulErr(err)
	}
	rows := make([]row, 0, len(config.Servers))
	for _, server := range config.Servers {
		rows = append(rows, row{
			"node":       server.Node,
			"id":         server.ID,
			"address":    server.Address,
			"leader":     server.Leader,
			"voter":      server.Voter,
			"last_index": server.LastIndex,
			"protocol":   server.ProtocolVersion,
		})
	}
	sortByKey(rows, "node")
	return s.page(rc, rows)
}

func datacentersList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	datacenters, err := s.client.Catalog().Datacenters()
	if err != nil {
		if degraded(err) {
			return emptyPage(), nil
		}
		return nil, consulErr(err)
	}
	rows := make([]row, 0, len(datacenters))
	for _, dc := range datacenters {
		rows = append(rows, row{"name": dc, "value": dc, "label": dc})
	}
	sortByKey(rows, "label")
	return s.page(rc, rows)
}

func catalogTree(rc *plugin.RequestContext) (any, error) {
	if _, err := session(rc); err != nil {
		return nil, err
	}
	return treePage([]plugin.TreeNode{
		{Key: "catalog:services", Label: "Services", Icon: icon("boxes"), ResourceKind: kindService,
			ChildrenSource: &plugin.DataSource{RouteID: rid("services.tree")}},
		{Key: "catalog:nodes", Label: "Nodes", Icon: icon("server"), Leaf: true, ResourceKind: kindNode},
		{Key: "catalog:checks", Label: "Health checks", Icon: icon("heart-pulse"), ResourceKind: kindCheck,
			ListParams:     map[string]string{"state": api.HealthAny},
			ChildrenSource: &plugin.DataSource{RouteID: rid("checks.tree")}},
	}), nil
}

func servicesTree(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	services, _, err := s.client.Catalog().Services(s.query(rc))
	if err != nil {
		return nil, consulErr(err)
	}
	seen := map[string]bool{}
	tags := make([]string, 0, len(services))
	for _, serviceTags := range services {
		for _, tag := range serviceTags {
			if tag == "" || seen[tag] {
				continue
			}
			seen[tag] = true
			tags = append(tags, tag)
		}
	}
	sort.Strings(tags)
	if len(tags) > s.opts.PageLimit {
		tags = tags[:s.opts.PageLimit]
	}
	nodes := make([]plugin.TreeNode, 0, len(tags)+1)
	nodes = append(nodes, plugin.TreeNode{
		Key: "services:all", Label: "All services", Icon: icon("boxes"), Leaf: true, ResourceKind: kindService,
	})
	for _, tag := range tags {
		nodes = append(nodes, plugin.TreeNode{
			Key: "services:tag:" + tag, Label: tag, Icon: icon("tag"), Leaf: true,
			ResourceKind: kindService, ListParams: map[string]string{"tag": tag},
			Data: map[string]any{"tag": tag},
		})
	}
	return treePage(nodes), nil
}

func checksTree(rc *plugin.RequestContext) (any, error) {
	if _, err := session(rc); err != nil {
		return nil, err
	}
	states := []struct {
		state string
		label string
		icon  string
	}{
		{api.HealthAny, "All checks", "list"},
		{api.HealthPassing, "Passing", "circle-check"},
		{api.HealthWarning, "Warning", "triangle-alert"},
		{api.HealthCritical, "Critical", "circle-x"},
	}
	nodes := make([]plugin.TreeNode, 0, len(states))
	for _, entry := range states {
		nodes = append(nodes, plugin.TreeNode{
			Key: "checks:" + entry.state, Label: entry.label, Icon: icon(entry.icon), Leaf: true,
			ResourceKind: kindCheck, ListParams: map[string]string{"state": entry.state},
			Data: map[string]any{"state": entry.state},
		})
	}
	return treePage(nodes), nil
}

func meshTree(rc *plugin.RequestContext) (any, error) {
	if _, err := session(rc); err != nil {
		return nil, err
	}
	return treePage([]plugin.TreeNode{
		{Key: "mesh:intentions", Label: "Intentions", Icon: icon("shield-check"), Leaf: true, ResourceKind: kindIntention},
		{Key: "mesh:sessions", Label: "Sessions", Icon: icon("lock"), Leaf: true, ResourceKind: kindSession},
		{Key: "mesh:queries", Label: "Prepared queries", Icon: icon("search"), Leaf: true, ResourceKind: kindPreparedQuery},
	}), nil
}

func aclTree(rc *plugin.RequestContext) (any, error) {
	if _, err := session(rc); err != nil {
		return nil, err
	}
	return treePage([]plugin.TreeNode{
		{Key: "acl:tokens", Label: "Tokens", Icon: icon("key-square"), Leaf: true, ResourceKind: kindACLToken},
		{Key: "acl:policies", Label: "Policies", Icon: icon("scroll-text"), Leaf: true, ResourceKind: kindACLPolicy},
		{Key: "acl:roles", Label: "Roles", Icon: icon("user-cog"), Leaf: true, ResourceKind: kindACLRole},
	}), nil
}

func servicesList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	q := s.query(rc)
	services, _, err := s.client.Catalog().Services(q)
	if err != nil {
		return nil, consulErr(err)
	}
	byService, _ := s.healthIndex(rc)
	tag := paramOrQuery(rc, "tag")
	rows := make([]row, 0, len(services))
	for name, tags := range services {
		if tag != "" && !containsTag(tags, tag) {
			continue
		}
		counts := byService[name]
		rows = append(rows, row{
			"ref":      plugin.ResourceIdentity{Kind: kindService, Name: name, UID: name},
			"name":     name,
			"tags":     strings.Join(tags, ", "),
			"status":   counts.status(),
			"passing":  counts.Passing,
			"warning":  counts.Warning,
			"critical": counts.Critical,
		})
	}
	sortByKey(rows, "name")
	return s.page(rc, rows)
}

func serviceRead(rc *plugin.RequestContext) (any, error) {
	_, name, entries, err := serviceEntries(rc, "")
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("%w: service %q is not registered", plugin.ErrNotFound, name)
	}
	var counts healthCounts
	nodes, tags, kinds, ports := map[string]bool{}, map[string]bool{}, map[string]bool{}, map[int]bool{}
	meta := map[string]string{}
	for _, entry := range entries {
		nodes[entry.Node.Node] = true
		for _, tag := range entry.Service.Tags {
			tags[tag] = true
		}
		kind := string(entry.Service.Kind)
		if kind == "" {
			kind = "typical"
		}
		kinds[kind] = true
		if entry.Service.Port > 0 {
			ports[entry.Service.Port] = true
		}
		for key, value := range entry.Service.Meta {
			meta[key] = value
		}
		for _, check := range entry.Checks {
			counts = counts.add(check.Status)
		}
	}
	return row{
		"name":       name,
		"status":     counts.status(),
		"instances":  len(entries),
		"nodes":      len(nodes),
		"kinds":      strings.Join(sortedKeys(kinds), ", "),
		"tags":       strings.Join(sortedKeys(tags), ", "),
		"ports":      joinInts(sortedIntKeys(ports)),
		"datacenter": entries[0].Node.Datacenter,
		"meta":       meta,
		"passing":    counts.Passing,
		"warning":    counts.Warning,
		"critical":   counts.Critical,
	}, nil
}

func serviceInstances(rc *plugin.RequestContext) (any, error) {
	s, name, entries, err := serviceEntries(rc, paramOrQuery(rc, "tag"))
	if err != nil {
		return nil, err
	}
	rows := make([]row, 0, len(entries))
	for _, entry := range entries {
		var counts healthCounts
		for _, check := range entry.Checks {
			counts = counts.add(check.Status)
		}
		kind := string(entry.Service.Kind)
		if kind == "" {
			kind = "typical"
		}
		address := entry.Service.Address
		if address == "" {
			address = entry.Node.Address
		}
		rows = append(rows, row{
			"ref": plugin.ResourceIdentity{
				Kind: kindInstance, Scope: name, Namespace: entry.Node.Node,
				Name: entry.Service.ID, UID: entry.Node.Node + "/" + entry.Service.ID,
			},
			"name":    entry.Service.ID,
			"service": name,
			"node":    entry.Node.Node,
			"status":  counts.status(),
			"address": address,
			"port":    entry.Service.Port,
			"tags":    strings.Join(entry.Service.Tags, ", "),
			"kind":    kind,
		})
	}
	sortByKey(rows, "name")
	return s.page(rc, rows)
}

func serviceChecks(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	name, err := requiredParam(rc, "service")
	if err != nil {
		return nil, err
	}
	checks, _, err := s.client.Health().Checks(name, s.query(rc))
	if err != nil {
		return nil, consulErr(err)
	}
	return s.page(rc, checkRows(checks))
}

func instanceRead(rc *plugin.RequestContext) (any, error) {
	_, name, entries, err := serviceEntries(rc, "")
	if err != nil {
		return nil, err
	}
	node, err := requiredParam(rc, "node")
	if err != nil {
		return nil, err
	}
	instance, err := requiredParam(rc, "instance")
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.Node.Node != node || entry.Service.ID != instance {
			continue
		}
		var counts healthCounts
		for _, check := range entry.Checks {
			counts = counts.add(check.Status)
		}
		kind := string(entry.Service.Kind)
		if kind == "" {
			kind = "typical"
		}
		address := entry.Service.Address
		if address == "" {
			address = entry.Node.Address
		}
		out := row{
			"name":             instance,
			"service":          name,
			"node":             node,
			"status":           counts.status(),
			"kind":             kind,
			"address":          address,
			"node_address":     entry.Node.Address,
			"port":             entry.Service.Port,
			"socket_path":      entry.Service.SocketPath,
			"tagged_addresses": entry.Service.TaggedAddresses,
			"tags":             strings.Join(entry.Service.Tags, ", "),
			"meta":             entry.Service.Meta,
			"weights":          entry.Service.Weights,
			"passing":          counts.Passing,
			"critical":         counts.Critical,
		}
		if proxy := entry.Service.Proxy; proxy != nil {
			out["proxy_destination"] = proxy.DestinationServiceName
			out["upstreams"] = proxy.Upstreams
		}
		if connect := entry.Service.Connect; connect != nil {
			out["connect_native"] = connect.Native
		}
		return out, nil
	}
	return nil, fmt.Errorf("%w: instance %q is not registered on node %q", plugin.ErrNotFound, instance, node)
}

func instanceDeregister(rc *plugin.RequestContext) (any, error) {
	s, err := writableSession(rc)
	if err != nil {
		return nil, err
	}
	node, err := requiredParam(rc, "node")
	if err != nil {
		return nil, err
	}
	instance, err := requiredParam(rc, "instance")
	if err != nil {
		return nil, err
	}
	dereg := &api.CatalogDeregistration{
		Node:       node,
		ServiceID:  instance,
		Datacenter: s.datacenter(rc),
		Namespace:  s.opts.Namespace,
		Partition:  s.opts.Partition,
	}
	if _, err := s.client.Catalog().Deregister(dereg, s.writeOptions(rc)); err != nil {
		return nil, consulErr(err)
	}
	return row{"ok": true, "node": node, "instance": instance}, nil
}

func nodesList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	nodes, _, err := s.client.Catalog().Nodes(s.query(rc))
	if err != nil {
		return nil, consulErr(err)
	}
	_, byNode := s.healthIndex(rc)
	rows := make([]row, 0, len(nodes))
	for _, node := range nodes {
		counts := byNode[node.Node]
		rows = append(rows, row{
			"ref":        plugin.ResourceIdentity{Kind: kindNode, Name: node.Node, UID: node.Node},
			"name":       node.Node,
			"status":     counts.status(),
			"address":    node.Address,
			"datacenter": node.Datacenter,
			"id":         node.ID,
			"meta":       node.Meta,
		})
	}
	sortByKey(rows, "name")
	return s.page(rc, rows)
}

func nodeRead(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	name, err := requiredParam(rc, "node")
	if err != nil {
		return nil, err
	}
	q := s.query(rc)
	node, _, err := s.client.Catalog().Node(name, q)
	if err != nil {
		return nil, consulErr(err)
	}
	if node == nil || node.Node == nil {
		return nil, fmt.Errorf("%w: node %q is not in the catalog", plugin.ErrNotFound, name)
	}
	out := row{
		"name":             node.Node.Node,
		"id":               node.Node.ID,
		"address":          node.Node.Address,
		"datacenter":       node.Node.Datacenter,
		"partition":        node.Node.Partition,
		"tagged_addresses": node.Node.TaggedAddresses,
		"meta":             node.Node.Meta,
		"services":         len(node.Services),
	}
	var counts healthCounts
	if checks, _, err := s.client.Health().Node(name, q); err == nil {
		for _, check := range checks {
			counts = counts.add(check.Status)
		}
	}
	out["status"] = counts.status()
	out["passing"] = counts.Passing
	out["warning"] = counts.Warning
	out["critical"] = counts.Critical
	return out, nil
}

func nodeServices(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	name, err := requiredParam(rc, "node")
	if err != nil {
		return nil, err
	}
	list, _, err := s.client.Catalog().NodeServiceList(name, s.query(rc))
	if err != nil {
		return nil, consulErr(err)
	}
	services := []*api.AgentService(nil)
	if list != nil {
		services = list.Services
	}
	rows := make([]row, 0, len(services))
	for _, service := range services {
		kind := string(service.Kind)
		if kind == "" {
			kind = "typical"
		}
		rows = append(rows, row{
			"ref": plugin.ResourceIdentity{
				Kind: kindInstance, Scope: service.Service, Namespace: name,
				Name: service.ID, UID: name + "/" + service.ID,
			},
			"name":    service.ID,
			"service": service.Service,
			"address": service.Address,
			"port":    service.Port,
			"tags":    strings.Join(service.Tags, ", "),
			"kind":    kind,
		})
	}
	sortByKey(rows, "name")
	return s.page(rc, rows)
}

func nodeChecks(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	name, err := requiredParam(rc, "node")
	if err != nil {
		return nil, err
	}
	checks, _, err := s.client.Health().Node(name, s.query(rc))
	if err != nil {
		return nil, consulErr(err)
	}
	return s.page(rc, checkRows(checks))
}

func checksList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	state, err := checkState(paramOrQuery(rc, "state"))
	if err != nil {
		return nil, err
	}
	q := s.query(rc)
	node, service := paramOrQuery(rc, "node"), paramOrQuery(rc, "service")
	var (
		checks api.HealthChecks
		scoped bool
	)
	switch {
	case node != "":
		checks, _, err = s.client.Health().Node(node, q)
		scoped = true
	case service != "":
		checks, _, err = s.client.Health().Checks(service, q)
		scoped = true
	default:
		checks, _, err = s.client.Health().State(state, q)
	}
	if err != nil {
		return nil, consulErr(err)
	}
	rows := checkRows(checks)
	if scoped && state != api.HealthAny {
		filtered := rows[:0]
		for _, item := range rows {
			if item["status"] == state {
				filtered = append(filtered, item)
			}
		}
		rows = filtered
	}
	return s.page(rc, rows)
}

func checkRead(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	node, err := requiredParam(rc, "node")
	if err != nil {
		return nil, err
	}
	id, err := requiredParam(rc, "check")
	if err != nil {
		return nil, err
	}
	checks, _, err := s.client.Health().Node(node, s.query(rc))
	if err != nil {
		return nil, consulErr(err)
	}
	for _, check := range checks {
		if check.CheckID != id {
			continue
		}
		definition := check.Definition
		return row{
			"name":             check.CheckID,
			"title":            check.Name,
			"status":           check.Status,
			"type":             checkType(check),
			"node":             check.Node,
			"service":          check.ServiceName,
			"notes":            check.Notes,
			"output":           truncate(check.Output, maxCheckOutput),
			"http":             definition.HTTP,
			"tcp":              definition.TCP,
			"grpc":             definition.GRPC,
			"interval":         definition.Interval.String(),
			"timeout":          definition.Timeout.String(),
			"deregister_after": definition.DeregisterCriticalServiceAfter.String(),
		}, nil
	}
	return nil, fmt.Errorf("%w: check %q is not registered on node %q", plugin.ErrNotFound, id, node)
}

func sessionsList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	entries, _, err := s.client.Session().List(s.query(rc))
	if err != nil {
		return nil, consulErr(err)
	}
	rows := make([]row, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name
		if name == "" {
			name = entry.ID
		}
		rows = append(rows, row{
			"ref":        plugin.ResourceIdentity{Kind: kindSession, Namespace: entry.Node, Name: name, UID: entry.ID},
			"name":       name,
			"id":         entry.ID,
			"node":       entry.Node,
			"behavior":   entry.Behavior,
			"ttl":        entry.TTL,
			"lock_delay": entry.LockDelay.Seconds(),
			"checks":     len(entry.NodeChecks) + len(entry.ServiceChecks),
		})
	}
	sortByKey(rows, "name")
	return s.page(rc, rows)
}

func sessionRead(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	id, err := requiredParam(rc, "session")
	if err != nil {
		return nil, err
	}
	entry, _, err := s.client.Session().Info(id, s.query(rc))
	if err != nil {
		return nil, consulErr(err)
	}
	if entry == nil {
		return nil, fmt.Errorf("%w: session %q does not exist", plugin.ErrNotFound, id)
	}
	return row{
		"id":             entry.ID,
		"name":           entry.Name,
		"node":           entry.Node,
		"behavior":       entry.Behavior,
		"ttl":            entry.TTL,
		"lock_delay":     entry.LockDelay.Seconds(),
		"node_checks":    entry.NodeChecks,
		"service_checks": entry.ServiceChecks,
		"create_index":   entry.CreateIndex,
	}, nil
}

func sessionDestroy(rc *plugin.RequestContext) (any, error) {
	s, err := writableSession(rc)
	if err != nil {
		return nil, err
	}
	id, err := requiredParam(rc, "session")
	if err != nil {
		return nil, err
	}
	if _, err := s.client.Session().Destroy(id, s.writeOptions(rc)); err != nil {
		return nil, consulErr(err)
	}
	return row{"ok": true, "session": id}, nil
}

func queriesList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	queries, _, err := s.client.PreparedQuery().List(s.query(rc))
	if err != nil {
		if degraded(err) {
			return emptyPage(), nil
		}
		return nil, consulErr(err)
	}
	rows := make([]row, 0, len(queries))
	for _, query := range queries {
		rows = append(rows, preparedQueryRow(query))
	}
	sortByKey(rows, "name")
	return s.page(rc, rows)
}

func queryRead(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	id, err := requiredParam(rc, "query")
	if err != nil {
		return nil, err
	}
	queries, _, err := s.client.PreparedQuery().Get(id, s.query(rc))
	if err != nil {
		return nil, consulErr(err)
	}
	if len(queries) == 0 {
		return nil, fmt.Errorf("%w: prepared query %q does not exist", plugin.ErrNotFound, id)
	}
	query := queries[0]
	out := preparedQueryRow(query)
	out["node_meta"] = query.Service.NodeMeta
	out["failover"] = query.Service.Failover
	out["dns_ttl"] = query.DNS.TTL
	out["template_type"] = query.Template.Type
	out["template_regexp"] = query.Template.Regexp
	return out, nil
}

func queryExecute(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	id, err := requiredParam(rc, "query")
	if err != nil {
		return nil, err
	}
	result, _, err := s.client.PreparedQuery().Execute(id, s.query(rc))
	if err != nil {
		return nil, consulErr(err)
	}
	rows := make([]row, 0, len(result.Nodes))
	for _, entry := range result.Nodes {
		var counts healthCounts
		for _, check := range entry.Checks {
			counts = counts.add(check.Status)
		}
		address := entry.Service.Address
		if address == "" && entry.Node != nil {
			address = entry.Node.Address
		}
		node := ""
		if entry.Node != nil {
			node = entry.Node.Node
		}
		rows = append(rows, row{
			"node":    node,
			"address": address,
			"port":    entry.Service.Port,
			"status":  counts.status(),
			"tags":    strings.Join(entry.Service.Tags, ", "),
		})
	}
	sortByKey(rows, "node")
	return s.page(rc, rows)
}

func intentionsList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	intentions, _, err := s.client.Connect().Intentions(s.query(rc))
	if err != nil {
		if degraded(err) {
			return emptyPage(), nil
		}
		return nil, consulErr(err)
	}
	source, destination := paramOrQuery(rc, "source"), paramOrQuery(rc, "destination")
	rows := make([]row, 0, len(intentions))
	for _, intention := range intentions {
		src, dst := intention.SourceString(), intention.DestinationString()
		if source != "" && src != source {
			continue
		}
		if destination != "" && dst != destination {
			continue
		}
		rows = append(rows, row{
			"ref": plugin.ResourceIdentity{
				Kind: kindIntention, Scope: src, Name: dst, UID: src + "=>" + dst,
			},
			"source":      src,
			"destination": dst,
			"action":      string(intention.Action),
			"precedence":  intention.Precedence,
			"permissions": len(intention.Permissions),
			"updated_at":  intention.UpdatedAt,
		})
	}
	sortByKey(rows, "destination")
	return s.page(rc, rows)
}

func intentionRead(rc *plugin.RequestContext) (any, error) {
	_, intention, source, destination, err := exactIntention(rc)
	if err != nil {
		return nil, err
	}
	return row{
		"source":            source,
		"destination":       destination,
		"action":            string(intention.Action),
		"precedence":        intention.Precedence,
		"source_type":       string(intention.SourceType),
		"description":       intention.Description,
		"permissions":       len(intention.Permissions),
		"permission_detail": intention.Permissions,
		"created_at":        intention.CreatedAt,
		"updated_at":        intention.UpdatedAt,
		"meta":              intention.Meta,
	}, nil
}

func intentionToggle(rc *plugin.RequestContext) (any, error) {
	s, intention, source, destination, err := exactIntention(rc)
	if err != nil {
		return nil, err
	}
	if err := s.ensureWritable(); err != nil {
		return nil, err
	}
	if len(intention.Permissions) > 0 {
		return nil, fmt.Errorf("%w: this intention carries layer-7 permissions; edit its service-intentions config entry instead", plugin.ErrConflict)
	}
	next := api.IntentionActionDeny
	if intention.Action == api.IntentionActionDeny {
		next = api.IntentionActionAllow
	}
	intention.ID = ""
	intention.Action = next
	if _, err := s.client.Connect().IntentionUpsert(intention, s.writeOptions(rc)); err != nil {
		return nil, consulErr(err)
	}
	return row{"ok": true, "source": source, "destination": destination, "action": string(next)}, nil
}

func intentionDelete(rc *plugin.RequestContext) (any, error) {
	s, err := writableSession(rc)
	if err != nil {
		return nil, err
	}
	source, err := requiredParam(rc, "source")
	if err != nil {
		return nil, err
	}
	destination, err := requiredParam(rc, "destination")
	if err != nil {
		return nil, err
	}
	if _, err := s.client.Connect().IntentionDeleteExact(source, destination, s.writeOptions(rc)); err != nil {
		return nil, consulErr(err)
	}
	return row{"ok": true, "source": source, "destination": destination}, nil
}

func aclTokensList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	tokens, _, err := s.client.ACL().TokenList(s.query(rc))
	if err != nil {
		if degraded(err) {
			return emptyPage(), nil
		}
		return nil, consulErr(err)
	}
	rows := make([]row, 0, len(tokens))
	for _, token := range tokens {
		description := token.Description
		if description == "" {
			description = token.AccessorID
		}
		item := row{
			"ref":         plugin.ResourceIdentity{Kind: kindACLToken, Name: description, UID: token.AccessorID},
			"description": description,
			"accessor_id": token.AccessorID,
			"policies":    strings.Join(linkNames(token.Policies), ", "),
			"roles":       strings.Join(linkNames(token.Roles), ", "),
			"local":       token.Local,
			"created_at":  token.CreateTime,
		}
		if token.ExpirationTime != nil {
			item["expires_at"] = *token.ExpirationTime
		}
		rows = append(rows, item)
	}
	sortByKey(rows, "description")
	return s.page(rc, rows)
}

func aclTokenRead(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	accessor, err := requiredParam(rc, "token")
	if err != nil {
		return nil, err
	}
	token, _, err := s.client.ACL().TokenRead(accessor, s.query(rc))
	if err != nil {
		return nil, consulErr(err)
	}
	if token == nil {
		return nil, fmt.Errorf("%w: token %q does not exist", plugin.ErrNotFound, accessor)
	}
	out := row{
		"accessor_id":        token.AccessorID,
		"description":        token.Description,
		"secret_id":          secretPlaceholder,
		"local":              token.Local,
		"auth_method":        token.AuthMethod,
		"policies":           strings.Join(linkNames(token.Policies), ", "),
		"roles":              strings.Join(linkNames(token.Roles), ", "),
		"service_identities": strings.Join(serviceIdentityNames(token.ServiceIdentities), ", "),
		"node_identities":    strings.Join(nodeIdentityNames(token.NodeIdentities), ", "),
		"templated_policies": strings.Join(templatedPolicyNames(token.TemplatedPolicies), ", "),
		"created_at":         token.CreateTime,
		"create_index":       token.CreateIndex,
		"modify_index":       token.ModifyIndex,
	}
	if token.ExpirationTime != nil {
		out["expires_at"] = *token.ExpirationTime
	}
	return out, nil
}

func aclPoliciesList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	policies, _, err := s.client.ACL().PolicyList(s.query(rc))
	if err != nil {
		if degraded(err) {
			return emptyPage(), nil
		}
		return nil, consulErr(err)
	}
	rows := make([]row, 0, len(policies))
	for _, policy := range policies {
		rows = append(rows, row{
			"ref":         plugin.ResourceIdentity{Kind: kindACLPolicy, Name: policy.Name, UID: policy.ID},
			"id":          policy.ID,
			"name":        policy.Name,
			"description": policy.Description,
			"datacenters": strings.Join(policy.Datacenters, ", "),
		})
	}
	sortByKey(rows, "name")
	return s.page(rc, rows)
}

func aclPolicyRead(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	id, err := requiredParam(rc, "policy")
	if err != nil {
		return nil, err
	}
	policy, _, err := s.client.ACL().PolicyRead(id, s.query(rc))
	if err != nil {
		return nil, consulErr(err)
	}
	if policy == nil {
		return nil, fmt.Errorf("%w: policy %q does not exist", plugin.ErrNotFound, id)
	}
	return row{
		"id":          policy.ID,
		"name":        policy.Name,
		"description": policy.Description,
		"datacenters": strings.Join(policy.Datacenters, ", "),
		"rules":       policy.Rules,
	}, nil
}

func aclRolesList(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	roles, _, err := s.client.ACL().RoleList(s.query(rc))
	if err != nil {
		if degraded(err) {
			return emptyPage(), nil
		}
		return nil, consulErr(err)
	}
	rows := make([]row, 0, len(roles))
	for _, role := range roles {
		rows = append(rows, row{
			"ref":         plugin.ResourceIdentity{Kind: kindACLRole, Name: role.Name, UID: role.ID},
			"id":          role.ID,
			"name":        role.Name,
			"description": role.Description,
			"policies":    strings.Join(linkNames(role.Policies), ", "),
		})
	}
	sortByKey(rows, "name")
	return s.page(rc, rows)
}

func aclRoleRead(rc *plugin.RequestContext) (any, error) {
	s, err := session(rc)
	if err != nil {
		return nil, err
	}
	id, err := requiredParam(rc, "role")
	if err != nil {
		return nil, err
	}
	role, _, err := s.client.ACL().RoleRead(id, s.query(rc))
	if err != nil {
		return nil, consulErr(err)
	}
	if role == nil {
		return nil, fmt.Errorf("%w: role %q does not exist", plugin.ErrNotFound, id)
	}
	return row{
		"id":                 role.ID,
		"name":               role.Name,
		"description":        role.Description,
		"policies":           strings.Join(linkNames(role.Policies), ", "),
		"service_identities": strings.Join(serviceIdentityNames(role.ServiceIdentities), ", "),
		"node_identities":    strings.Join(nodeIdentityNames(role.NodeIdentities), ", "),
		"templated_policies": strings.Join(templatedPolicyNames(role.TemplatedPolicies), ", "),
	}, nil
}

func serviceEntries(rc *plugin.RequestContext, tag string) (*Session, string, []*api.ServiceEntry, error) {
	s, err := session(rc)
	if err != nil {
		return nil, "", nil, err
	}
	name, err := requiredParam(rc, "service")
	if err != nil {
		return nil, "", nil, err
	}
	entries, _, err := s.client.Health().Service(name, tag, false, s.query(rc))
	if err != nil {
		return nil, "", nil, consulErr(err)
	}
	return s, name, entries, nil
}

func exactIntention(rc *plugin.RequestContext) (*Session, *api.Intention, string, string, error) {
	s, err := session(rc)
	if err != nil {
		return nil, nil, "", "", err
	}
	source, err := requiredParam(rc, "source")
	if err != nil {
		return nil, nil, "", "", err
	}
	destination, err := requiredParam(rc, "destination")
	if err != nil {
		return nil, nil, "", "", err
	}
	intention, _, err := s.client.Connect().IntentionGetExact(source, destination, s.query(rc))
	if err != nil {
		return nil, nil, "", "", consulErr(err)
	}
	if intention == nil {
		return nil, nil, "", "", fmt.Errorf("%w: no intention from %q to %q", plugin.ErrNotFound, source, destination)
	}
	return s, intention, source, destination, nil
}

// page slices one list route's rows. Consul answers these endpoints in one shot,
// so search, sort, and paging all run over the whole result set and the offset
// cursor stays valid across requests. The connection's page limit caps the slice
// on top of the request limit, and the cursor it advertises stays reachable.
func (s *Session) page(rc *plugin.RequestContext, rows []row) (plugin.Page[row], error) {
	page, err := broker.PageRows(rc, rows)
	if err != nil {
		return plugin.Page[row]{}, err
	}
	if s.opts.PageLimit <= 0 || len(page.Items) <= s.opts.PageLimit {
		return page, nil
	}
	req, err := rc.Page()
	if err != nil {
		return plugin.Page[row]{}, err
	}
	start := 0
	if req.Cursor != "" {
		if start, err = strconv.Atoi(req.Cursor); err != nil {
			return plugin.Page[row]{}, fmt.Errorf("%w: invalid cursor", plugin.ErrInvalidInput)
		}
	}
	page.Items = page.Items[:s.opts.PageLimit]
	page.NextCursor = strconv.Itoa(start + s.opts.PageLimit)
	return page, nil
}

// healthIndex aggregates every check in the datacenter once, so service and node
// status are correct across the whole dataset rather than the current page.
func (s *Session) healthIndex(rc *plugin.RequestContext) (map[string]healthCounts, map[string]healthCounts) {
	checks, _, err := s.client.Health().State(api.HealthAny, s.query(rc))
	if err != nil {
		return nil, nil
	}
	services := make(map[string]healthCounts, len(checks))
	nodes := make(map[string]healthCounts, len(checks))
	for _, check := range checks {
		if check.ServiceName != "" {
			services[check.ServiceName] = services[check.ServiceName].add(check.Status)
		}
		if check.Node != "" {
			nodes[check.Node] = nodes[check.Node].add(check.Status)
		}
	}
	return services, nodes
}

func checkRows(checks api.HealthChecks) []row {
	rows := make([]row, 0, len(checks))
	for _, check := range checks {
		name := check.Name
		if name == "" {
			name = check.CheckID
		}
		rows = append(rows, row{
			"ref": plugin.ResourceIdentity{
				Kind: kindCheck, Scope: check.ServiceName, Namespace: check.Node,
				Name: check.CheckID, UID: check.Node + "/" + check.CheckID,
			},
			"name":    name,
			"id":      check.CheckID,
			"status":  check.Status,
			"node":    check.Node,
			"service": check.ServiceName,
			"type":    checkType(check),
			"output":  truncate(check.Output, maxCheckOutput),
		})
	}
	sortByKey(rows, "name")
	return rows
}

func preparedQueryRow(query *api.PreparedQueryDefinition) row {
	name := query.Name
	if name == "" {
		name = query.ID
	}
	return row{
		"ref":          plugin.ResourceIdentity{Kind: kindPreparedQuery, Name: name, UID: query.ID},
		"id":           query.ID,
		"name":         name,
		"service":      query.Service.Service,
		"only_passing": query.Service.OnlyPassing,
		"near":         query.Service.Near,
		"session":      query.Session,
		"tags":         strings.Join(query.Service.Tags, ", "),
	}
}

type healthCounts struct {
	Passing  int
	Warning  int
	Critical int
	Other    int
}

func (h healthCounts) add(status string) healthCounts {
	switch status {
	case api.HealthPassing:
		h.Passing++
	case api.HealthWarning:
		h.Warning++
	case api.HealthCritical:
		h.Critical++
	default:
		h.Other++
	}
	return h
}

func (h healthCounts) status() string {
	switch {
	case h.Critical > 0:
		return api.HealthCritical
	case h.Warning > 0:
		return api.HealthWarning
	case h.Passing > 0:
		return api.HealthPassing
	case h.Other > 0:
		return api.HealthMaint
	default:
		return "unknown"
	}
}

func checkState(value string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", api.HealthAny:
		return api.HealthAny, nil
	case api.HealthPassing:
		return api.HealthPassing, nil
	case api.HealthWarning:
		return api.HealthWarning, nil
	case api.HealthCritical:
		return api.HealthCritical, nil
	default:
		return "", fmt.Errorf("%w: state must be any, passing, warning, or critical", plugin.ErrInvalidInput)
	}
}

func checkType(check *api.HealthCheck) string {
	if check.Type != "" {
		return check.Type
	}
	switch {
	case check.Definition.HTTP != "":
		return "http"
	case check.Definition.TCP != "":
		return "tcp"
	case check.Definition.GRPC != "":
		return "grpc"
	case check.Definition.UDP != "":
		return "udp"
	default:
		return "unknown"
	}
}

func memberStatus(status int) string {
	switch status {
	case 1:
		return "alive"
	case 2:
		return "leaving"
	case 3:
		return "left"
	case 4:
		return "failed"
	default:
		return "none"
	}
}

func linkNames(links []*api.ACLLink) []string {
	out := make([]string, 0, len(links))
	for _, link := range links {
		if link == nil {
			continue
		}
		if link.Name != "" {
			out = append(out, link.Name)
			continue
		}
		out = append(out, link.ID)
	}
	return out
}

func serviceIdentityNames(identities []*api.ACLServiceIdentity) []string {
	out := make([]string, 0, len(identities))
	for _, identity := range identities {
		if identity != nil {
			out = append(out, identity.ServiceName)
		}
	}
	return out
}

func nodeIdentityNames(identities []*api.ACLNodeIdentity) []string {
	out := make([]string, 0, len(identities))
	for _, identity := range identities {
		if identity != nil {
			out = append(out, identity.NodeName)
		}
	}
	return out
}

func templatedPolicyNames(policies []*api.ACLTemplatedPolicy) []string {
	out := make([]string, 0, len(policies))
	for _, policy := range policies {
		if policy != nil {
			out = append(out, policy.TemplateName)
		}
	}
	return out
}

// degraded reports an error that should render an empty panel instead of a hard
// failure: clusters without ACLs or without the service mesh, and tokens without
// the privilege for one area.
func degraded(err error) bool {
	if aclDisabled(err) {
		return true
	}
	var status api.StatusError
	if !errors.As(err, &status) {
		return false
	}
	if status.Code == http.StatusForbidden {
		return true
	}
	return strings.Contains(strings.ToLower(status.Body), "connect must be enabled")
}

func treePage(nodes []plugin.TreeNode) plugin.Page[plugin.TreeNode] {
	total := len(nodes)
	return plugin.Page[plugin.TreeNode]{Items: nodes, Total: &total}
}

func emptyPage() plugin.Page[row] {
	total := 0
	return plugin.Page[row]{Items: []row{}, Total: &total}
}

func sortByKey(rows []row, key string) {
	sort.SliceStable(rows, func(i, j int) bool {
		return fmt.Sprint(rows[i][key]) < fmt.Sprint(rows[j][key])
	})
}

func sortedKeys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func sortedIntKeys(set map[int]bool) []int {
	out := make([]int, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	sort.Ints(out)
	return out
}

func joinInts(values []int) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, strconv.Itoa(value))
	}
	return strings.Join(parts, ", ")
}

func containsTag(tags []string, want string) bool {
	for _, tag := range tags {
		if tag == want {
			return true
		}
	}
	return false
}

func stringField(section map[string]any, key string) string {
	if section == nil {
		return ""
	}
	if value, ok := section[key].(string); ok {
		return value
	}
	return ""
}

func boolField(section map[string]any, key string) bool {
	if section == nil {
		return false
	}
	value, _ := section[key].(bool)
	return value
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(value[cut]) {
		cut--
	}
	return value[:cut] + "…"
}
