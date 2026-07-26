package couchbase

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugin/canvas"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestManifestValidates(t *testing.T) {
	p := New()
	plugintest.ValidatePlugin(t, p)
	plugintest.ValidateProjectionPanelConfigs(t, plugintest.Projection(t, p))
	if !plugintest.CredentialKindSupported(p.Manifest().Config, plugin.CredentialKindDBPassword) {
		t.Fatal("Couchbase should accept stored database password credentials")
	}
}

func TestRouteMetadataIsConsistent(t *testing.T) {
	p := New()
	name := p.Manifest().Name
	for _, route := range p.Routes() {
		if !strings.HasPrefix(route.ID, name+".") {
			t.Fatalf("route %q is outside the %q namespace", route.ID, name)
		}
		if route.AuditEvent != route.ID {
			t.Fatalf("route %q audit event is %q", route.ID, route.AuditEvent)
		}
		if !strings.HasPrefix(route.Permission, name+".") {
			t.Fatalf("route %q permission %q is outside the plugin namespace", route.ID, route.Permission)
		}
		if route.IsStream() == (route.Stream == nil) {
			t.Fatalf("route %q stream handler does not match its method", route.ID)
		}
	}
}

func TestParseOptionsAppliesDefaults(t *testing.T) {
	opts, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{
		"host":     "couchbase.internal",
		"username": "Administrator",
		"password": "secret",
	}})
	if err != nil {
		t.Fatalf("parseOptions: %v", err)
	}
	if opts.managementURL() != "http://couchbase.internal:8091" {
		t.Fatalf("unexpected management URL %q", opts.managementURL())
	}
	if opts.queryURL() != "http://couchbase.internal:8093" {
		t.Fatalf("unexpected query URL %q", opts.queryURL())
	}
	if !opts.ReadOnly || !opts.RequireConfirm {
		t.Fatal("safety defaults must be enabled")
	}
	if opts.Timeout != defaultTimeout || opts.PageLimit != defaultPageLimit {
		t.Fatalf("unexpected safety options: %v %d", opts.Timeout, opts.PageLimit)
	}
}

func TestParseOptionsTLSUsesSecureSchemeAndPorts(t *testing.T) {
	opts, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{
		"host":       "couchbase.internal",
		"port":       float64(18091),
		"query_port": float64(18093),
		"tls_mode":   "require",
		"username":   "app",
		"password":   "secret",
	}})
	if err != nil {
		t.Fatalf("parseOptions: %v", err)
	}
	if opts.Scheme != "https" || opts.TLSConfig == nil {
		t.Fatalf("expected TLS options, got scheme %q tls %v", opts.Scheme, opts.TLSConfig)
	}
	if opts.managementURL() != "https://couchbase.internal:18091" || opts.queryURL() != "https://couchbase.internal:18093" {
		t.Fatalf("unexpected endpoints: %s %s", opts.managementURL(), opts.queryURL())
	}
}

func TestParseOptionsStoredCredential(t *testing.T) {
	cfg := plugin.ConnectConfig{
		Config: map[string]any{"host": "cb", "auth": authCredential},
		Credentials: plugin.NewResolvedCredentials(plugin.CredentialBinding{
			Field: credentialField,
			Credential: plugin.ResolvedCredential{
				Kind:   plugin.CredentialKindDBPassword,
				Values: map[string]string{"username": "reporting", "password": "stored"},
			},
		}),
	}
	opts, err := parseOptions(cfg)
	if err != nil {
		t.Fatalf("parseOptions: %v", err)
	}
	if opts.Username != "reporting" || opts.Password != "stored" {
		t.Fatalf("stored credential was not applied: %+v", opts)
	}
}

func TestParseOptionsRejectsWrongCredentialKind(t *testing.T) {
	cfg := plugin.ConnectConfig{
		Config: map[string]any{"host": "cb", "auth": authCredential},
		Credentials: plugin.NewResolvedCredentials(plugin.CredentialBinding{
			Field:      credentialField,
			Credential: plugin.ResolvedCredential{Kind: plugin.CredentialKindAPIToken, Values: map[string]string{"token": "x"}},
		}),
	}
	if _, err := parseOptions(cfg); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("expected invalid input for the wrong credential kind, got %v", err)
	}
}

func TestParseOptionsRequiresHostAndCredentials(t *testing.T) {
	for name, cfg := range map[string]map[string]any{
		"no host":     {"username": "a", "password": "b"},
		"no username": {"host": "cb", "password": "b"},
		"no password": {"host": "cb", "username": "a"},
		"bad port":    {"host": "cb", "port": float64(99999), "username": "a", "password": "b"},
		"bad bucket":  {"host": "cb", "username": "a", "password": "b", "bucket": "bad bucket name!"},
		"bad auth":    {"host": "cb", "auth": "kerberos"},
	} {
		if _, err := parseOptions(plugin.ConnectConfig{Config: cfg}); err == nil {
			t.Fatalf("%s: expected an error", name)
		}
	}
}

func TestConnectRequiresNetworkTransport(t *testing.T) {
	_, err := New().Connect(context.Background(), plugin.ConnectConfig{Config: map[string]any{
		"host": "cb", "username": "a", "password": "b",
	}})
	if !errors.Is(err, plugin.ErrUnavailable) {
		t.Fatalf("expected an unavailable transport error, got %v", err)
	}
}

func TestConfigSchemaSecretVisibilityFollowsAuthMode(t *testing.T) {
	schema := New().Manifest().Config
	password := schema.VisibleSecretKeys(map[string]any{"auth": authPassword}, nil)
	if !slices.Contains(password, "password") {
		t.Fatalf("password auth must expose the inline password field: %v", password)
	}
	stored := schema.VisibleSecretKeys(map[string]any{"auth": authCredential}, nil)
	if slices.Contains(stored, "password") {
		t.Fatalf("stored credential auth must hide the inline password field: %v", stored)
	}
	if err := schema.ValidateValues(map[string]any{
		"host": "cb", "port": 8091, "query_port": 8093,
		"auth": authPassword, "username": "Administrator", "password": "secret",
		"tls_mode": "disable", "scan_consistency": "request_plus",
	}, nil); err != nil {
		t.Fatalf("valid password config rejected: %v", err)
	}
	if err := schema.ValidateValues(map[string]any{
		"host": "cb", "port": 8091, "query_port": 8093, "auth": authPassword,
		"tls_mode": "disable", "scan_consistency": "request_plus",
	}, nil); err == nil {
		t.Fatal("expected the missing password to fail validation")
	}
}

func TestKeyspaceEncodingRoundTrip(t *testing.T) {
	original := keyspace{Bucket: "travel-sample", Scope: "inventory", Collection: "hotel", Document: "hotel::10"}
	decoded, err := parseKeyspaceUID(original.uid())
	if err != nil {
		t.Fatalf("parseKeyspaceUID: %v", err)
	}
	if decoded != original {
		t.Fatalf("round trip changed the keyspace: %+v", decoded)
	}
	if decoded.path() != "`travel-sample`.`inventory`.`hotel`" {
		t.Fatalf("unexpected SQL++ path %q", decoded.path())
	}
	if decoded.label() != "travel-sample.inventory.hotel" {
		t.Fatalf("unexpected label %q", decoded.label())
	}
}

func TestKeyspaceValidationRejectsInjection(t *testing.T) {
	for _, name := range []string{"", "bad name", "a`b", "a;b", "../etc", "a\"b"} {
		if _, err := safeBucket(name); err == nil {
			t.Fatalf("bucket %q should be rejected", name)
		}
		if _, err := safeScope(name, "scope"); err == nil {
			t.Fatalf("scope %q should be rejected", name)
		}
	}
	if _, err := validateKeyspace(keyspace{Bucket: "b", Collection: "c"}); err == nil {
		t.Fatal("a collection without a scope should be rejected")
	}
	if _, err := parseKeyspaceUID("not-base64!!"); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("expected invalid input, got %v", err)
	}
}

func TestReadOnlyN1QLClassification(t *testing.T) {
	for _, statement := range []string{
		"SELECT * FROM `b`.`s`.`c`",
		"explain select 1",
		"INFER `b`.`s`.`c`",
		"ADVISE SELECT 1",
	} {
		if !isReadOnlyN1QL(statement) {
			t.Fatalf("%q should be read-only", statement)
		}
	}
	for _, statement := range []string{
		"UPSERT INTO `b`.`s`.`c` (KEY, VALUE) VALUES ('a', {})",
		"DELETE FROM `b`.`s`.`c`",
		"CREATE INDEX idx ON `b`.`s`.`c`(a)",
		"EXECUTE p1",
		"GRANT query_select ON `b` TO app",
	} {
		if isReadOnlyN1QL(statement) {
			t.Fatalf("%q must not be treated as read-only", statement)
		}
	}
}

func TestUnionKeysPutsIdentityFirst(t *testing.T) {
	columns := unionKeys([]map[string]any{
		{"total": 1, "name": "a", "id": "x"},
		{"zeta": 2, "type": "hotel"},
	})
	if !slices.Equal(columns, []string{"id", "name", "type", "total", "zeta"}) {
		t.Fatalf("unexpected column order: %v", columns)
	}
}

func TestDCPConsumersFoldFlatSamples(t *testing.T) {
	rows := dcpConsumers(bucketSamples{
		"ep_dcp_replica_items_remaining": {5, 7},
		"ep_dcp_replica_items_sent":      {100, 120},
		"ep_dcp_replica_count":           {2, 2},
		"ep_dcp_2i_items_remaining":      {0, 0},
		"ep_queue_size":                  {1},
	})
	if len(rows) != 2 {
		t.Fatalf("expected two DCP consumers, got %#v", rows)
	}
	if rows[0]["consumer"] != "2i" || rows[1]["consumer"] != "replica" {
		t.Fatalf("consumers are not sorted: %#v", rows)
	}
	if rows[1]["items_remaining"] != 7.0 || rows[1]["items_sent"] != 120.0 || rows[1]["count"] != 2.0 {
		t.Fatalf("unexpected replica consumer row: %#v", rows[1])
	}
}

func TestDocumentPayloadAcceptsEveryEditorShape(t *testing.T) {
	cases := map[string][]byte{
		"code editor": []byte(`{"content":"{\"total\": 3}"}`),
		"kv panel":    []byte(`{"key":"order::1","type":"json","value":"{\"total\": 3}"}`),
		"action form": []byte(`{"key":"order::1","value":{"total":3}}`),
		"bare object": []byte(`{"id":"order::1","total":3}`),
	}
	for name, body := range cases {
		rc := plugin.NewRequestContext(context.Background(), plugin.User{}, nil, map[string]string{"id": "order::1"}, nil, body)
		key, doc, err := documentPayload(rc)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if key != "order::1" {
			t.Fatalf("%s: unexpected key %q", name, key)
		}
		if doc["total"] != float64(3) {
			t.Fatalf("%s: unexpected document %#v", name, doc)
		}
		if _, ok := doc["id"]; ok {
			t.Fatalf("%s: metadata leaked into the document body: %#v", name, doc)
		}
	}
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, nil, nil, nil, []byte(`[1,2,3]`))
	if _, _, err := documentPayload(rc); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("expected invalid input for a non-object document, got %v", err)
	}
}

func TestInsertPayloadReadsGridRowMutation(t *testing.T) {
	body := []byte(`{"values":{"id":"order::9","total":12,"cas":1234}}`)
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, nil, nil, nil, body)
	key, doc, err := insertPayload(rc)
	if err != nil {
		t.Fatalf("insertPayload: %v", err)
	}
	if key != "order::9" || doc["total"] != float64(12) {
		t.Fatalf("unexpected insert payload: %q %#v", key, doc)
	}
	if _, ok := doc["cas"]; ok {
		t.Fatalf("metadata must not be written into the document: %#v", doc)
	}
}

func TestPlanFieldExtraction(t *testing.T) {
	fields := fieldNames("(((`orders`.`customer`) = \"ada\") and (3 < (`orders`.`total`)))")
	if !slices.Equal(uniqueStrings(fields), []string{"customer", "total"}) {
		t.Fatalf("unexpected predicate fields: %v", fields)
	}
}

func TestStorageKeyIsStableAndSafe(t *testing.T) {
	if storageKey("Top Sellers / 2026!") != "top-sellers-2026" {
		t.Fatalf("unexpected storage key %q", storageKey("Top Sellers / 2026!"))
	}
	if storageKey("!!!") != "query" {
		t.Fatalf("expected a fallback key, got %q", storageKey("!!!"))
	}
}

// --- handler tests against a fake Couchbase --------------------------------

type fakeCouchbase struct {
	t             *testing.T
	server        *httptest.Server
	documents     map[string]map[string]any
	statements    []string
	bucketUpdates []url.Values
}

func newFakeCouchbase(t *testing.T) *fakeCouchbase {
	t.Helper()
	f := &fakeCouchbase{t: t, documents: map[string]map[string]any{
		"order::1": {"customer": "ada", "total": 42.0, "password": "hunter2"},
		"order::2": {"customer": "grace", "total": 7.0},
	}}
	mux := http.NewServeMux()
	mux.HandleFunc("/pools", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]any{"isEnterprise": false, "implementationVersion": "7.6.2-community"})
	})
	mux.HandleFunc("/pools/default", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]any{
			"clusterName": "shellcn-it", "balanced": true, "rebalanceStatus": "none",
			"bucketNames": []any{map[string]any{"bucketName": "shop"}},
			"memoryQuota": 512, "indexMemoryQuota": 256, "maxBucketCount": 10,
			"storageTotals": map[string]any{
				"ram": map[string]any{"total": 1000, "quotaTotal": 500, "quotaUsed": 250, "used": 400, "usedByData": 100},
				"hdd": map[string]any{"total": 2000, "used": 500, "usedByData": 200, "free": 1500},
			},
			"nodes": []any{map[string]any{
				"hostname": "node1:8091", "status": "healthy", "clusterMembership": "active",
				"version": "7.6.2-community", "services": []any{"kv", "n1ql", "index"}, "uptime": "3600",
				"systemStats":      map[string]any{"cpu_utilization_rate": 12.5, "mem_total": 1000, "mem_free": 400},
				"interestingStats": map[string]any{"curr_items_tot": 2, "ops": 3},
			}},
		})
	})
	mux.HandleFunc("/pools/default/buckets", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		writeJSON(w, []any{bucketFixture()})
	})
	mux.HandleFunc("/pools/default/buckets/shop", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodDelete:
			w.WriteHeader(http.StatusOK)
		case http.MethodPost:
			_ = r.ParseForm()
			f.bucketUpdates = append(f.bucketUpdates, r.PostForm)
			w.WriteHeader(http.StatusOK)
		default:
			writeJSON(w, bucketFixture())
		}
	})
	mux.HandleFunc("/pools/default/buckets/shop/scopes", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]any{"uid": "3", "scopes": []any{
			map[string]any{"name": "app", "uid": "8", "collections": []any{
				map[string]any{"name": "orders", "uid": "9", "maxTTL": 0.0, "history": false},
			}},
			map[string]any{"name": "_default", "uid": "0", "collections": []any{
				map[string]any{"name": "_default", "uid": "0", "maxTTL": 0.0},
			}},
		}})
	})
	mux.HandleFunc("/pools/default/buckets/shop/stats", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]any{"op": map[string]any{"samples": map[string]any{
			"ep_dcp_replica_items_remaining": []any{3.0},
			"ep_dcp_replica_items_sent":      []any{9.0},
			"ep_cache_miss_rate":             []any{1.5},
			"vb_active_resident_items_ratio": []any{100.0},
			"ep_queue_size":                  []any{0.0},
			"cmd_get":                        []any{4.0},
			"cmd_set":                        []any{2.0},
		}}})
	})
	mux.HandleFunc("/pools/default/stats/range", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, []any{map[string]any{"data": []any{map[string]any{
			"metric": map[string]any{"bucket": "shop", "scope": "app", "collection": "orders"},
			"values": []any{[]any{1.0, "2"}},
		}}}})
	})
	mux.HandleFunc("/pools/default/tasks", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, []any{
			map[string]any{"type": "rebalance", "status": "notRunning"},
			map[string]any{"type": "xdcr", "id": "uuid/shop/shop", "source": "shop", "target": "/remoteClusters/dr/buckets/shop",
				"status": "running", "continuous": true, "replicationType": "xmem", "changesLeft": 12.0, "docsWritten": 34.0},
		})
	})
	mux.HandleFunc("/pools/default/remoteClusters", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, []any{map[string]any{"name": "dr", "uuid": "uuid", "hostname": "dr:8091", "username": "xdcr", "connectivityStatus": "RC_OK"}})
	})
	mux.HandleFunc("/settings/rbac/users", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, []any{map[string]any{"id": "app", "name": "App", "domain": "local",
			"roles": []any{map[string]any{"role": "data_reader", "bucket_name": "shop", "scope_name": "app"}}}})
	})
	mux.HandleFunc("/indexStatus", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]any{"indexes": []any{map[string]any{
			"indexName": "idx_customer", "bucket": "shop", "scope": "app", "collection": "orders",
			"status": "Ready", "progress": 100.0, "storageMode": "plasma", "hosts": []any{"node1:8091"},
			"definition": "CREATE INDEX `idx_customer` ON `shop`.`app`.`orders`(`customer`)",
		}}})
	})
	mux.HandleFunc("/logs", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]any{"list": []any{
			map[string]any{"node": "node1", "type": "info", "code": 1, "module": "ns_memcached",
				"shortText": "bucket online", "text": "Bucket shop is ready", "serverTime": "2026-07-26T10:00:00.000Z"},
			map[string]any{"node": "node1", "type": "warning", "code": 2, "module": "goxdcr",
				"shortText": "replication paused", "text": "XDCR replication paused", "serverTime": "2026-07-26T10:05:00.000Z"},
		}})
	})
	mux.HandleFunc("/query/service", f.query)
	mux.HandleFunc("/admin/active_requests", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, []any{})
	})
	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	return f
}

func bucketFixture() map[string]any {
	return map[string]any{
		"name": "shop", "bucketType": "membase", "storageBackend": "couchstore", "uuid": "bucket-uuid",
		"numVBuckets": 1024, "replicaNumber": 1, "evictionPolicy": "valueOnly", "durabilityMinLevel": "none",
		"quota":      map[string]any{"ram": 134217728, "rawRAM": 134217728},
		"basicStats": map[string]any{"quotaPercentUsed": 25.5, "opsPerSec": 3.0, "itemCount": 2.0, "diskUsed": 4096.0, "dataUsed": 2048.0, "memUsed": 1024.0},
		"nodes":      []any{map[string]any{"hostname": "node1:8091", "status": "healthy"}},
	}
}

// query answers the SQL++ statements the plugin issues, so handler tests drive
// the same code path as a real Query service.
func (f *fakeCouchbase) query(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Statement string `json:"statement"`
		Args      []any  `json:"args"`
	}
	body, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(body, &req)
	f.statements = append(f.statements, req.Statement)
	arg := func(i int) any {
		if i < len(req.Args) {
			return req.Args[i]
		}
		return nil
	}

	switch {
	case strings.HasPrefix(req.Statement, "SELECT META(d).id AS __id") && strings.Contains(req.Statement, "USE KEYS"):
		id, _ := arg(0).(string)
		doc, ok := f.documents[id]
		if !ok {
			writeJSON(w, map[string]any{"status": "success", "results": []any{}})
			return
		}
		writeJSON(w, map[string]any{"status": "success", "results": []any{map[string]any{
			"__id": id, "__cas": 1234, "__expiration": 0, "__flags": 0, "__type": "json", "__doc": doc,
		}}})
	case strings.HasPrefix(req.Statement, "SELECT META(d).id AS __id"):
		results := make([]any, 0, len(f.documents))
		for _, id := range sortedDocumentIDs(f.documents) {
			item := map[string]any{"__id": id, "__cas": 1234, "__expiration": 0, "__flags": 0}
			for key, value := range f.documents[id] {
				item[key] = value
			}
			results = append(results, item)
		}
		writeJSON(w, map[string]any{"status": "success", "results": results})
	case strings.HasPrefix(req.Statement, "UPSERT INTO"):
		id, _ := arg(0).(string)
		doc, _ := arg(1).(map[string]any)
		f.documents[id] = doc
		writeJSON(w, map[string]any{"status": "success", "metrics": map[string]any{"mutationCount": 1},
			"results": []any{map[string]any{"id": id, "cas": 4321}}})
	case strings.HasPrefix(req.Statement, "DELETE FROM"):
		id, _ := arg(0).(string)
		if _, ok := f.documents[id]; !ok {
			writeJSON(w, map[string]any{"status": "success", "results": []any{}})
			return
		}
		delete(f.documents, id)
		writeJSON(w, map[string]any{"status": "success", "results": []any{map[string]any{"id": id}}})
	case strings.HasPrefix(req.Statement, "INFER"):
		writeJSON(w, map[string]any{"status": "success", "results": []any{[]any{map[string]any{
			"#docs": 2.0, "properties": map[string]any{
				"customer": map[string]any{"#docs": 2.0, "%docs": 100.0, "type": "string", "samples": []any{"ada"}},
				"password": map[string]any{"#docs": 2.0, "%docs": 100.0, "type": "string", "samples": []any{"hunter2"}},
			},
		}}}})
	case strings.HasPrefix(req.Statement, "ADVISE"):
		writeJSON(w, map[string]any{"status": "fatal", "errors": []any{
			map[string]any{"code": 3230, "msg": "'Advise' is an enterprise level feature."}}})
	case strings.HasPrefix(req.Statement, "EXPLAIN"):
		writeJSON(w, map[string]any{"status": "success", "results": []any{map[string]any{"plan": map[string]any{
			"#operator": "Sequence",
			"~children": []any{
				map[string]any{"#operator": "PrimaryScan3", "bucket": "shop", "scope": "app", "keyspace": "orders", "index": "#primary"},
				map[string]any{"#operator": "Filter", "condition": "((`orders`.`customer`) = \"ada\")"},
			},
		}}}})
	case strings.HasPrefix(req.Statement, "SELECT RAW idx FROM system:indexes"):
		writeJSON(w, map[string]any{"status": "success", "results": []any{map[string]any{
			"index_key": []any{"`customer`"}, "state": "online", "using": "gsi", "is_primary": false,
		}}})
	case strings.Contains(req.Statement, "system:keyspaces"):
		writeJSON(w, map[string]any{"status": "success", "results": []any{
			map[string]any{"path": "default:shop.app.orders", "name": "orders"},
		}})
	case strings.HasPrefix(req.Statement, "CREATE INDEX"), strings.HasPrefix(req.Statement, "BUILD INDEX"), strings.HasPrefix(req.Statement, "DROP INDEX"):
		writeJSON(w, map[string]any{"status": "success", "results": []any{}})
	default:
		writeJSON(w, map[string]any{"status": "success", "results": []any{map[string]any{"value": 1}},
			"metrics": map[string]any{"resultCount": 1}})
	}
}

func sortedDocumentIDs(docs map[string]map[string]any) []string {
	out := make([]string, 0, len(docs))
	for id := range docs {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func (f *fakeCouchbase) connect(t *testing.T, extra map[string]any) *Session {
	t.Helper()
	host, port, err := net.SplitHostPort(strings.TrimPrefix(f.server.URL, "http://"))
	if err != nil {
		t.Fatalf("split test server address: %v", err)
	}
	config := map[string]any{
		"host": host, "username": "Administrator", "password": "password",
		"bucket": "shop", "read_only": false,
	}
	for key, value := range extra {
		config[key] = value
	}
	// Both Couchbase ports resolve to the one fake server; the dialer is the
	// plugin's only egress path, exactly as the gateway wires it.
	transport := plugintest.TransportFunc(func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, net.JoinHostPort(host, port))
	})
	sess, err := New().Connect(context.Background(), plugin.ConnectConfig{Net: transport, Config: config})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = sess.Close() })
	return sess.(*Session)
}

func routeFor(t *testing.T, id string) plugin.Route {
	t.Helper()
	route, ok := plugintest.RouteMap(New().Routes())[id]
	if !ok {
		t.Fatalf("route %q is not registered", id)
	}
	return route
}

func call(t *testing.T, sess plugin.Session, id string, params map[string]string, body []byte) any {
	t.Helper()
	route := routeFor(t, id)
	out, err := route.Handle(plugin.NewRequestContext(context.Background(), plugin.User{}, sess, params, url.Values{}, body))
	if err != nil {
		t.Fatalf("%s: %v", id, err)
	}
	return out
}

func pageItems(t *testing.T, value any) []map[string]any {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal page: %v", err)
	}
	var decoded struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode page: %v", err)
	}
	return decoded.Items
}

func object(t *testing.T, value any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal object: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode object: %v", err)
	}
	return decoded
}

func TestClusterAndBucketHandlers(t *testing.T) {
	fake := newFakeCouchbase(t)
	sess := fake.connect(t, nil)

	overview := object(t, call(t, sess, rid("cluster.overview"), nil, nil))
	if overview["name"] != "shellcn-it" || overview["edition"] != "Community" {
		t.Fatalf("unexpected cluster overview: %#v", overview)
	}
	if overview["quotaPct"] != 50.0 || overview["diskPct"] != 25.0 {
		t.Fatalf("capacity percentages are wrong: %#v", overview)
	}

	nodes := pageItems(t, call(t, sess, rid("nodes.list"), nil, nil))
	if len(nodes) != 1 || nodes[0]["hostname"] != "node1:8091" || nodes[0]["ramPct"] != 60.0 {
		t.Fatalf("unexpected nodes: %#v", nodes)
	}

	buckets := pageItems(t, call(t, sess, rid("buckets.list"), nil, nil))
	if len(buckets) != 1 || buckets[0]["name"] != "shop" || buckets[0]["health"] != "healthy" {
		t.Fatalf("unexpected buckets: %#v", buckets)
	}

	bucket := object(t, call(t, sess, rid("bucket.read"), map[string]string{"bucket": "shop"}, nil))
	if bucket["scopes"] != 2.0 || bucket["collections"] != 2.0 {
		t.Fatalf("bucket detail did not fold in the manifest: %#v", bucket)
	}

	dcp := pageItems(t, call(t, sess, rid("bucket.dcp"), map[string]string{"bucket": "shop"}, nil))
	if len(dcp) != 1 || dcp[0]["consumer"] != "replica" || dcp[0]["items_remaining"] != 3.0 {
		t.Fatalf("unexpected DCP rows: %#v", dcp)
	}

	scopes := pageItems(t, call(t, sess, rid("scopes.list"), map[string]string{"bucket": "shop"}, nil))
	if len(scopes) != 2 {
		t.Fatalf("unexpected scopes: %#v", scopes)
	}
	collections := pageItems(t, call(t, sess, rid("collections.list"), map[string]string{"bucket": "shop", "scope": "app"}, nil))
	if len(collections) != 1 || collections[0]["name"] != "orders" || collections[0]["items"] != 2.0 {
		t.Fatalf("unexpected collections: %#v", collections)
	}
}

func TestBucketUpdateCarriesOverUntouchedSettings(t *testing.T) {
	fake := newFakeCouchbase(t)
	sess := fake.connect(t, nil)
	call(t, sess, rid("bucket.update"), map[string]string{"bucket": "shop"}, []byte(`{"ram_quota_mb":256}`))
	if len(fake.bucketUpdates) != 1 {
		t.Fatalf("expected one bucket update, got %#v", fake.bucketUpdates)
	}
	form := fake.bucketUpdates[0]
	if form.Get("ramQuotaMB") != "256" || form.Get("replicaNumber") != "1" || form.Get("flushEnabled") != "0" {
		t.Fatalf("settings that were not edited must be carried over: %v", form)
	}
	if _, err := routeFor(t, rid("bucket.update")).Handle(plugin.NewRequestContext(context.Background(), plugin.User{}, sess,
		map[string]string{"bucket": "shop"}, nil, []byte(`{"ram_quota_mb":10}`))); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("an undersized quota must be rejected, got %v", err)
	}
}

// The edit form is prefilled from the bucket row the browser holds, so the
// values it submits untouched must reproduce the bucket exactly. Both entry
// points into the detail view - the bucket list and the sidebar tree - have to
// carry that row.
func TestBucketEditFormRoundTripsCurrentSettings(t *testing.T) {
	fake := newFakeCouchbase(t)
	sess := fake.connect(t, nil)

	node := pageItems(t, call(t, sess, rid("tree.buckets"), nil, nil))[0]
	data, ok := node["data"].(map[string]any)
	if !ok {
		t.Fatalf("bucket tree nodes must carry the bucket row: %#v", node)
	}
	rows := map[string]map[string]any{
		"buckets.list": pageItems(t, call(t, sess, rid("buckets.list"), nil, nil))[0],
		"tree.buckets": data,
	}
	for source, item := range rows {
		prefill := bucketEditPrefill(t, item)
		if prefill["ram_quota_mb"] != 128.0 || prefill["replica_number"] != 1.0 || prefill["flush_enabled"] != false {
			t.Fatalf("%s prefills the edit form with %#v", source, prefill)
		}
		body, err := json.Marshal(prefill)
		if err != nil {
			t.Fatalf("marshal prefill: %v", err)
		}
		call(t, sess, rid("bucket.update"), map[string]string{"bucket": "shop"}, body)
		form := fake.bucketUpdates[len(fake.bucketUpdates)-1]
		if form.Get("ramQuotaMB") != "128" || form.Get("replicaNumber") != "1" || form.Get("flushEnabled") != "0" {
			t.Fatalf("%s: submitting the untouched form rewrote the bucket: %v", source, form)
		}
	}
}

// bucketEditPrefill resolves the update schema's defaults the way the browser
// does: a lone ${record.x} token keeps the raw typed value, and anything that
// does not resolve stays a literal string and would be submitted as one.
func bucketEditPrefill(t *testing.T, record map[string]any) map[string]any {
	t.Helper()
	out := map[string]any{}
	for _, group := range updateBucketSchema().Groups {
		for _, field := range group.Fields {
			value := field.Default
			token, _ := field.Default.(string)
			if key, ok := strings.CutPrefix(token, "${record."); ok {
				if raw, found := record[strings.TrimSuffix(key, "}")]; found {
					value = raw
				}
			}
			out[field.Key] = value
		}
	}
	return out
}

func TestTreeNavigationIsBounded(t *testing.T) {
	fake := newFakeCouchbase(t)
	sess := fake.connect(t, nil)

	buckets := pageItems(t, call(t, sess, rid("tree.buckets"), nil, nil))
	if len(buckets) != 1 || buckets[0]["label"] != "shop" {
		t.Fatalf("unexpected bucket nodes: %#v", buckets)
	}
	scopes := pageItems(t, call(t, sess, rid("tree.scopes"), map[string]string{"bucket": "shop"}, nil))
	if len(scopes) != 2 {
		t.Fatalf("unexpected scope nodes: %#v", scopes)
	}
	collections := pageItems(t, call(t, sess, rid("tree.collections"), map[string]string{"bucket": "shop", "scope": "app"}, nil))
	if len(collections) != 1 || collections[0]["leaf"] != true {
		t.Fatalf("collection nodes must be leaves: %#v", collections)
	}
}

func TestDocumentLifecycleAndRedaction(t *testing.T) {
	fake := newFakeCouchbase(t)
	sess := fake.connect(t, nil)
	k := keyspace{Bucket: "shop", Scope: "app", Collection: "orders"}
	params := map[string]string{"keyspace": k.uid()}

	docs := pageItems(t, call(t, sess, rid("documents.list"), params, nil))
	if len(docs) != 2 || docs[0]["id"] != "order::1" {
		t.Fatalf("unexpected documents: %#v", docs)
	}
	if docs[0]["password"] != "***" {
		t.Fatalf("secret-looking fields must be redacted in the grid: %#v", docs[0])
	}

	columns := pageItems(t, call(t, sess, rid("documents.columns"), params, nil))
	byName := map[string]map[string]any{}
	for _, column := range columns {
		byName[column["name"].(string)] = column
	}
	if byName["id"]["readOnly"] != true || byName["total"]["editor"] != "number" {
		t.Fatalf("unexpected column metadata: %#v", byName)
	}
	if byName["password"]["editable"] != false {
		t.Fatalf("redacted columns must not be editable: %#v", byName["password"])
	}

	doc := k
	doc.Document = "order::1"
	read := object(t, call(t, sess, rid("document.read"), map[string]string{"id": doc.uid()}, nil))
	if read["key"] != "order::1" || read["keyspace"] != "shop.app.orders" {
		t.Fatalf("unexpected document read: %#v", read)
	}
	content := object(t, call(t, sess, rid("document.content"), map[string]string{"id": doc.uid()}, nil))
	if !strings.Contains(content["content"].(string), "\"customer\": \"ada\"") {
		t.Fatalf("unexpected document content: %#v", content)
	}

	inserted := object(t, call(t, sess, rid("document.insert"), params, []byte(`{"key":"order::3","value":{"customer":"linus","total":5}}`)))
	if inserted["id"] != "order::3" {
		t.Fatalf("unexpected insert result: %#v", inserted)
	}
	updated := object(t, call(t, sess, rid("document.update"), params, []byte(`{"key":{"id":"order::3"},"values":{"total":6}}`)))
	if updated["id"] != "order::3" || fake.documents["order::3"]["customer"] != "linus" {
		t.Fatalf("update must merge into the stored document: %#v", fake.documents["order::3"])
	}
	saved := object(t, call(t, sess, rid("document.save"), map[string]string{"id": doc.uid()}, []byte(`{"content":"{\"customer\":\"ada\",\"total\":50}"}`)))
	if !strings.Contains(saved["content"].(string), "50") {
		t.Fatalf("save must return the canonical stored content: %#v", saved)
	}
	if saved["id"] != "order::1" || fake.documents["order::1"]["total"] != 50.0 {
		t.Fatalf("save must write back to the referenced document: %#v", fake.documents)
	}
	removed := object(t, call(t, sess, rid("document.remove"), map[string]string{"id": doc.uid()}, nil))
	if removed["ok"] != true {
		t.Fatalf("unexpected remove result: %#v", removed)
	}
}

func TestDocumentUpdateRefusesRedactedFieldsAndForeignKeys(t *testing.T) {
	fake := newFakeCouchbase(t)
	sess := fake.connect(t, nil)
	k := keyspace{Bucket: "shop", Scope: "app", Collection: "orders"}
	params := map[string]string{"keyspace": k.uid()}
	route := routeFor(t, rid("document.update"))

	_, err := route.Handle(plugin.NewRequestContext(context.Background(), plugin.User{}, sess, params, nil,
		[]byte(`{"key":{"id":"order::1"},"values":{"password":"new"}}`)))
	if !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("expected a forbidden redacted-field edit, got %v", err)
	}
	_, err = route.Handle(plugin.NewRequestContext(context.Background(), plugin.User{}, sess, params, nil,
		[]byte(`{"key":{"customer":"ada"},"values":{"total":1}}`)))
	if !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("expected an invalid row key, got %v", err)
	}
}

func TestDocumentSaveKeepsMaskedFieldsIntact(t *testing.T) {
	fake := newFakeCouchbase(t)
	sess := fake.connect(t, nil)
	doc := keyspace{Bucket: "shop", Scope: "app", Collection: "orders", Document: "order::1"}
	call(t, sess, rid("document.save"), map[string]string{"id": doc.uid()},
		[]byte(`{"content":"{\"customer\":\"ada\",\"total\":50,\"password\":\"***\"}"}`))
	stored := fake.documents["order::1"]
	if stored["password"] != "hunter2" {
		t.Fatalf("saving the redacted view must not overwrite the real value: %#v", stored)
	}
	if stored["total"] != 50.0 {
		t.Fatalf("edited fields must still be written: %#v", stored)
	}
}

func TestIndexListHonoursTheRequestedKeyspace(t *testing.T) {
	fake := newFakeCouchbase(t)
	sess := fake.connect(t, map[string]any{"bucket": ""})
	other := keyspace{Bucket: "shop", Scope: "app", Collection: "shipments"}
	if rows := pageItems(t, call(t, sess, rid("indexes.list"), map[string]string{"keyspace": other.uid()}, nil)); len(rows) != 0 {
		t.Fatalf("a collection panel must only list its own indexes: %#v", rows)
	}
	orders := keyspace{Bucket: "shop", Scope: "app", Collection: "orders"}
	if rows := pageItems(t, call(t, sess, rid("indexes.list"), map[string]string{"keyspace": orders.uid()}, nil)); len(rows) != 1 {
		t.Fatalf("the collection's own index is missing: %#v", rows)
	}
	if rows := pageItems(t, call(t, sess, rid("indexes.list"), nil, nil)); len(rows) != 1 {
		t.Fatal("without a keyspace the list stays cluster-wide")
	}
}

func TestAdvisorRefusesSmuggledStatements(t *testing.T) {
	fake := newFakeCouchbase(t)
	sess := fake.connect(t, map[string]any{"read_only": true})
	out := runStream(t, sess, rid("advisor"), nil,
		`{"query":"SELECT 1; DELETE FROM `+"`shop`.`app`.`orders`"+`"}`+"\n", 5*time.Second)
	if !strings.Contains(out, "one statement at a time") {
		t.Fatalf("the advisor must refuse multi-statement input: %s", out)
	}
	for _, statement := range fake.statements {
		if strings.Contains(statement, "DELETE") {
			t.Fatalf("a smuggled statement reached the query service: %q", statement)
		}
	}
}

func TestCollectionSchemaInference(t *testing.T) {
	fake := newFakeCouchbase(t)
	sess := fake.connect(t, nil)
	k := keyspace{Bucket: "shop", Scope: "app", Collection: "orders"}
	rows := pageItems(t, call(t, sess, rid("collection.schema"), map[string]string{"keyspace": k.uid()}, nil))
	if len(rows) != 2 {
		t.Fatalf("unexpected inferred schema: %#v", rows)
	}
	for _, item := range rows {
		if item["field"] == "password" && item["samples"] != "***" {
			t.Fatalf("inferred samples must respect redaction: %#v", item)
		}
		if item["field"] == "customer" && item["type"] != "string" {
			t.Fatalf("unexpected inferred type: %#v", item)
		}
	}
}

func TestIndexesAndAdvisorFallBackToThePlan(t *testing.T) {
	fake := newFakeCouchbase(t)
	sess := fake.connect(t, nil)

	indexes := pageItems(t, call(t, sess, rid("indexes.list"), nil, nil))
	if len(indexes) != 1 || indexes[0]["name"] != "idx_customer" || indexes[0]["status"] != "ready" {
		t.Fatalf("unexpected indexes: %#v", indexes)
	}
	k := keyspace{Bucket: "shop", Scope: "app", Collection: "orders", Document: "idx_customer"}
	detail := object(t, call(t, sess, rid("index.read"), map[string]string{"id": k.uid()}, nil))
	if detail["index_keys"] != "`customer`" || detail["state"] != "online" {
		t.Fatalf("index detail did not merge the catalog entry: %#v", detail)
	}

	advice, err := sess.advise(context.Background(), "SELECT * FROM `shop`.`app`.`orders` WHERE customer = \"ada\"")
	if err != nil {
		t.Fatalf("advise: %v", err)
	}
	if len(advice) != 1 || advice[0].Source != "plan" {
		t.Fatalf("expected plan-derived advice, got %#v", advice)
	}
	if !strings.Contains(advice[0].Statement, "CREATE INDEX `idx_orders_customer` ON `shop`.`app`.`orders`(`customer`)") {
		t.Fatalf("unexpected recommendation: %#v", advice[0])
	}
}

func TestReplicationsUsersAndEvents(t *testing.T) {
	fake := newFakeCouchbase(t)
	sess := fake.connect(t, nil)

	replications := pageItems(t, call(t, sess, rid("replications.list"), nil, nil))
	if len(replications) != 1 || replications[0]["status"] != "running" || replications[0]["changes_left"] != 12.0 {
		t.Fatalf("unexpected replications: %#v", replications)
	}
	detail := object(t, call(t, sess, rid("replication.read"), map[string]string{"id": replicationUID("uuid/shop/shop")}, nil))
	if detail["source"] != "shop" {
		t.Fatalf("unexpected replication detail: %#v", detail)
	}
	remotes := pageItems(t, call(t, sess, rid("remotes.list"), nil, nil))
	if len(remotes) != 1 || remotes[0]["name"] != "dr" {
		t.Fatalf("unexpected remote clusters: %#v", remotes)
	}
	users := pageItems(t, call(t, sess, rid("users.list"), nil, nil))
	if len(users) != 1 || users[0]["roles"] != "data_reader[shop.app]" {
		t.Fatalf("unexpected users: %#v", users)
	}

	events := pageItems(t, call(t, sess, rid("events.list"), nil, nil))
	if len(events) != 2 || events[0]["title"] != "replication paused" {
		t.Fatalf("events must be newest first: %#v", events)
	}
	replicationEvents := pageItems(t, call(t, sess, rid("events.list"), map[string]string{"filter": "replication"}, nil))
	if len(replicationEvents) != 1 || replicationEvents[0]["severity"] != "warn" {
		t.Fatalf("unexpected replication events: %#v", replicationEvents)
	}
}

func TestReadOnlyModeBlocksMutations(t *testing.T) {
	fake := newFakeCouchbase(t)
	sess := fake.connect(t, map[string]any{"read_only": true})
	route := routeFor(t, rid("bucket.delete"))
	_, err := route.Handle(plugin.NewRequestContext(context.Background(), plugin.User{}, sess, map[string]string{"bucket": "shop"}, nil, nil))
	if !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("read-only mode must block bucket deletion, got %v", err)
	}
}

// --- stream tests -----------------------------------------------------------

type memoryStream struct {
	ctx    context.Context
	reader *bytes.Reader
	out    bytes.Buffer
}

func newMemoryStream(ctx context.Context, input string) *memoryStream {
	return &memoryStream{ctx: ctx, reader: bytes.NewReader([]byte(input))}
}

func (s *memoryStream) Read(p []byte) (int, error)  { return s.reader.Read(p) }
func (s *memoryStream) Write(p []byte) (int, error) { return s.out.Write(p) }
func (s *memoryStream) Close() error                { return nil }
func (s *memoryStream) Context() context.Context    { return s.ctx }

func runStream(t *testing.T, sess plugin.Session, id string, params map[string]string, input string, timeout time.Duration) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	route := routeFor(t, id)
	stream := newMemoryStream(ctx, input)
	if err := route.Stream(plugin.NewRequestContext(ctx, plugin.User{}, sess, params, url.Values{}, nil), stream); err != nil {
		t.Fatalf("%s: %v", id, err)
	}
	return stream.out.String()
}

func TestQueryStreamReturnsAGridAndHonoursConfirmation(t *testing.T) {
	fake := newFakeCouchbase(t)
	sess := fake.connect(t, nil)

	out := runStream(t, sess, rid("query"), nil, `{"query":"SELECT META(d).id AS __id, d.* FROM `+"`shop`.`app`.`orders`"+` d"}`+"\n", 5*time.Second)
	var result struct {
		Columns []string `json:"columns"`
		Rows    [][]any  `json:"rows"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &result); err != nil {
		t.Fatalf("decode query frame: %v (%s)", err, out)
	}
	if len(result.Rows) != 2 || !slices.Contains(result.Columns, "customer") {
		t.Fatalf("unexpected query result: %#v", result)
	}
	passwordIndex := slices.Index(result.Columns, "password")
	if passwordIndex < 0 || result.Rows[0][passwordIndex] != "***" {
		t.Fatalf("query results must redact secret-looking columns: %#v", result)
	}

	challenge := runStream(t, sess, rid("query"), nil, `{"query":"DELETE FROM `+"`shop`.`app`.`orders`"+`"}`+"\n", 5*time.Second)
	if !strings.Contains(challenge, `"confirmRequired":true`) {
		t.Fatalf("mutating statements must require confirmation: %s", challenge)
	}
	confirmed := runStream(t, sess, rid("query"), nil, `{"query":"DELETE FROM `+"`shop`.`app`.`orders`"+` USE KEYS [\"order::2\"]","confirm":true}`+"\n", 5*time.Second)
	if strings.Contains(confirmed, "confirmRequired") {
		t.Fatalf("a confirmed statement must run: %s", confirmed)
	}
}

func TestQueryStreamRejectsWritesInReadOnlyMode(t *testing.T) {
	fake := newFakeCouchbase(t)
	sess := fake.connect(t, map[string]any{"read_only": true})
	out := runStream(t, sess, rid("query"), nil, `{"query":"DELETE FROM `+"`shop`.`app`.`orders`"+`","confirm":true}`+"\n", 5*time.Second)
	if !strings.Contains(out, "read-only mode blocks DELETE") {
		t.Fatalf("expected a read-only rejection frame, got %s", out)
	}
}

func TestAdvisorStreamEmitsRecommendationRows(t *testing.T) {
	fake := newFakeCouchbase(t)
	sess := fake.connect(t, nil)
	out := runStream(t, sess, rid("advisor"), nil,
		`{"query":"SELECT * FROM `+"`shop`.`app`.`orders`"+` WHERE customer = \"ada\""}`+"\n", 5*time.Second)
	if !strings.Contains(out, "CREATE INDEX") || !strings.Contains(out, `"kind"`) {
		t.Fatalf("unexpected advisor frame: %s", out)
	}
	rejected := runStream(t, sess, rid("advisor"), nil, `{"query":"CREATE INDEX x ON `+"`shop`.`app`.`orders`"+`(a)"}`+"\n", 5*time.Second)
	if !strings.Contains(rejected, "only analyses") {
		t.Fatalf("the advisor must reject non-analysable statements: %s", rejected)
	}
}

func TestMetricsStreamEmitsAFrameImmediately(t *testing.T) {
	fake := newFakeCouchbase(t)
	sess := fake.connect(t, nil)
	out := runStream(t, sess, rid("cluster.metrics"), nil, "", 1500*time.Millisecond)
	frame := map[string]any{}
	if err := json.Unmarshal([]byte(strings.SplitN(strings.TrimSpace(out), "\n", 2)[0]), &frame); err != nil {
		t.Fatalf("decode metrics frame: %v (%s)", err, out)
	}
	if frame["buckets"] != 1.0 || frame["opsPerSec"] != 3.0 {
		t.Fatalf("unexpected cluster metrics frame: %#v", frame)
	}

	bucketOut := runStream(t, sess, rid("bucket.metrics"), map[string]string{"bucket": "shop"}, "", 1500*time.Millisecond)
	bucketFrame := map[string]any{}
	if err := json.Unmarshal([]byte(strings.SplitN(strings.TrimSpace(bucketOut), "\n", 2)[0]), &bucketFrame); err != nil {
		t.Fatalf("decode bucket metrics frame: %v (%s)", err, bucketOut)
	}
	if bucketFrame["dcpRemaining"] != 3.0 || bucketFrame["residentRatio"] != 100.0 {
		t.Fatalf("unexpected bucket metrics frame: %#v", bucketFrame)
	}
}

func TestDataMapStreamDrawsCollections(t *testing.T) {
	fake := newFakeCouchbase(t)
	sess := fake.connect(t, nil)
	out := runStream(t, sess, rid("datamap"), map[string]string{"bucket": "shop"}, "", 3*time.Second)
	var frame struct {
		Commands []map[string]any `json:"commands"`
		Regions  []map[string]any `json:"regions"`
	}
	if err := json.Unmarshal([]byte(strings.SplitN(strings.TrimSpace(out), "\n", 2)[0]), &frame); err != nil {
		t.Fatalf("decode canvas frame: %v (%s)", err, out)
	}
	if len(frame.Commands) == 0 || frame.Commands[0]["type"] != "clear" {
		t.Fatalf("a canvas frame must start with a clear: %#v", frame.Commands)
	}
	found := false
	for _, region := range frame.Regions {
		if region["id"] == "collection:app/orders" && region["label"] != "" {
			found = true
		}
	}
	if !found {
		t.Fatalf("the data map must expose a labelled region per collection: %#v", frame.Regions)
	}
}

func TestDataMapSceneIsKeyboardNavigableAndThemed(t *testing.T) {
	scene := &mapScene{width: 900, theme: plugin.PanelThemeDark}
	scene.setData(dataMap{Bucket: "shop", MaxItems: 10, Scopes: []mapScope{{
		Name: "app",
		Collections: []mapCollection{
			{Scope: "app", Name: "orders", Items: 10},
			{Scope: "app", Name: "customers", Items: 4},
		},
	}}})
	if !scene.handle(&canvas.KeyEvent{Event: "keydown", Key: "ArrowDown"}) || scene.selected != 1 {
		t.Fatalf("arrow keys must move the selection, got %d", scene.selected)
	}
	if !scene.handle(&canvas.KeyEvent{Event: "keydown", Key: "Home"}) || scene.selected != 0 {
		t.Fatalf("Home must select the first collection, got %d", scene.selected)
	}
	if scene.handle(&canvas.KeyEvent{Event: "keyup", Key: "ArrowDown"}) {
		t.Fatal("key-up events must be ignored")
	}
	if !scene.handle(&canvas.PointerEvent{Event: canvas.PointerDown, RegionID: "collection:app/customers"}) || scene.selected != 1 {
		t.Fatalf("clicking a region must select it, got %d", scene.selected)
	}

	dark := scene.frame()
	if !scene.handle(&canvas.ResizeEvent{Width: 500, Height: 400, Theme: plugin.PanelThemeLight}) {
		t.Fatal("a resize event must mark the scene dirty")
	}
	light := scene.frame()
	if len(dark.Regions) != len(light.Regions) || len(light.Regions) != 2 {
		t.Fatalf("both themes must draw the same regions: %d vs %d", len(dark.Regions), len(light.Regions))
	}
	darkClear, _ := dark.Commands[0].(canvas.Clear)
	lightClear, _ := light.Commands[0].(canvas.Clear)
	if darkClear.Color == lightClear.Color {
		t.Fatalf("the palette must follow the workspace theme: %q", lightClear.Color)
	}
}

func TestEventsWatchSkipsTheAlreadyRenderedPage(t *testing.T) {
	fake := newFakeCouchbase(t)
	sess := fake.connect(t, nil)
	out := runStream(t, sess, rid("events.watch"), nil, "", 1200*time.Millisecond)
	if strings.TrimSpace(out) != "" {
		t.Fatalf("the first watch poll must not replay existing events: %s", out)
	}
}

func TestClusterWatchPushesSnapshots(t *testing.T) {
	fake := newFakeCouchbase(t)
	sess := fake.connect(t, nil)
	out := runStream(t, sess, rid("cluster.watch"), nil, "", 1200*time.Millisecond)
	var event plugin.ResourceEvent
	if err := json.Unmarshal([]byte(strings.SplitN(strings.TrimSpace(out), "\n", 2)[0]), &event); err != nil {
		t.Fatalf("decode watch event: %v (%s)", err, out)
	}
	if event.Type != "modified" || event.Ref.Kind != "cluster" {
		t.Fatalf("unexpected watch event: %#v", event)
	}
}

// --- storage ----------------------------------------------------------------

type fakeStorage struct{ items map[string]plugin.StorageItem }

func (s *fakeStorage) Get(_ context.Context, scope plugin.StorageScope, key string) (plugin.StorageItem, error) {
	item, ok := s.items[scope.Collection+"/"+key]
	if !ok {
		return plugin.StorageItem{}, plugin.ErrNotFound
	}
	return item, nil
}

func (s *fakeStorage) Put(_ context.Context, collection string, item plugin.StorageItem) (plugin.StorageItem, error) {
	if s.items == nil {
		s.items = map[string]plugin.StorageItem{}
	}
	item.UpdatedAt = time.Unix(0, 0).UTC()
	s.items[collection+"/"+item.Key] = item
	return item, nil
}

func (s *fakeStorage) Delete(_ context.Context, scope plugin.StorageScope, key string) error {
	full := scope.Collection + "/" + key
	if _, ok := s.items[full]; !ok {
		return plugin.ErrNotFound
	}
	delete(s.items, full)
	return nil
}

func (s *fakeStorage) List(_ context.Context, scope plugin.StorageScope, filter *plugin.StorageListFilter) ([]plugin.StorageItem, error) {
	out := make([]plugin.StorageItem, 0, len(s.items))
	for key, item := range s.items {
		if !strings.HasPrefix(key, scope.Collection+"/") {
			continue
		}
		if filter != nil && filter.KeyPrefix != "" && !strings.HasPrefix(item.Key, filter.KeyPrefix) {
			continue
		}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func TestSavedQueriesRoundTrip(t *testing.T) {
	storage := &fakeStorage{}
	newRC := func(params map[string]string, body []byte) *plugin.RequestContext {
		return plugin.NewRequestContext(context.Background(), plugin.User{}, nil, params, nil, body).WithStorage(storage)
	}
	saved, err := routeFor(t, rid("query.save")).Handle(newRC(nil,
		[]byte(`{"name":"Top orders","statement":"SELECT * FROM `+"`shop`.`app`.`orders`"+`","keyspace":"shop.app.orders"}`)))
	if err != nil {
		t.Fatalf("save: %v", err)
	}
	item := object(t, saved)
	if item["id"] != "query/top-orders" {
		t.Fatalf("unexpected saved query id: %#v", item)
	}
	listed, err := routeFor(t, rid("queries.list")).Handle(newRC(nil, nil))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if rows := pageItems(t, listed); len(rows) != 1 || rows[0]["name"] != "Top orders" {
		t.Fatalf("unexpected saved queries: %#v", rows)
	}
	if _, err := routeFor(t, rid("query.delete")).Handle(newRC(map[string]string{"id": "query/top-orders"}, nil)); err != nil {
		t.Fatalf("delete: %v", err)
	}
	listed, _ = routeFor(t, rid("queries.list")).Handle(newRC(nil, nil))
	if rows := pageItems(t, listed); len(rows) != 0 {
		t.Fatalf("saved query was not deleted: %#v", rows)
	}
}

func TestSavedQueryValidation(t *testing.T) {
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, nil, nil, nil, []byte(`{"name":" ","statement":" "}`)).
		WithStorage(&fakeStorage{})
	if _, err := routeFor(t, rid("query.save")).Handle(rc); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("expected invalid input, got %v", err)
	}
}

func TestConsoleURLUsesTheCoreSuppliedMount(t *testing.T) {
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, nil, nil, nil, nil).
		WithProxyPrefix("/api/connections/abc/proxy")
	out, err := routeFor(t, rid("console.url")).Handle(rc)
	if err != nil {
		t.Fatalf("console.url: %v", err)
	}
	if object(t, out)["url"] != "/api/connections/abc/proxy/" {
		t.Fatalf("unexpected console URL: %#v", out)
	}
}

func TestResourceWatchDiffsByIdentity(t *testing.T) {
	first := []row{
		{"ref": bucketRef("a"), "name": "a", "items": 1.0},
		{"ref": bucketRef("b"), "name": "b", "items": 2.0},
	}
	events, snapshot := diffResources("bucket", map[string]string{}, first)
	if len(events) != 2 || events[0].Type != "added" {
		t.Fatalf("the first diff must add every row: %#v", events)
	}

	second := []row{
		{"ref": bucketRef("a"), "name": "a", "items": 1.0},
		{"ref": bucketRef("c"), "name": "c", "items": 3.0},
	}
	events, snapshot = diffResources("bucket", snapshot, second)
	if len(events) != 2 {
		t.Fatalf("expected one add and one delete, got %#v", events)
	}
	if events[0].Type != "added" || events[0].Ref.UID != "c" {
		t.Fatalf("unexpected add event: %#v", events[0])
	}
	if events[1].Type != "deleted" || events[1].Ref.UID != "b" {
		t.Fatalf("unexpected delete event: %#v", events[1])
	}

	third := []row{
		{"ref": bucketRef("a"), "name": "a", "items": 9.0},
		{"ref": bucketRef("c"), "name": "c", "items": 3.0},
	}
	events, _ = diffResources("bucket", snapshot, third)
	if len(events) != 1 || events[0].Type != "modified" || events[0].Ref.UID != "a" {
		t.Fatalf("only the changed row must be pushed: %#v", events)
	}
}

func TestResourceWatchRejectsUnknownKinds(t *testing.T) {
	fake := newFakeCouchbase(t)
	sess := fake.connect(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := routeFor(t, rid("resource.watch")).Stream(
		plugin.NewRequestContext(ctx, plugin.User{}, sess, map[string]string{"kind": "galaxy"}, url.Values{}, nil),
		newMemoryStream(ctx, ""))
	if !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("expected an invalid kind error, got %v", err)
	}
	if out := runStream(t, sess, rid("resource.watch"), map[string]string{"kind": "bucket"}, "", 1200*time.Millisecond); strings.TrimSpace(out) != "" {
		t.Fatalf("the first watch poll must not replay the rendered page: %s", out)
	}
}
