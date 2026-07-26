package scylladb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"

	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugin/canvas"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestManifestValidatesAndStaysDirectOnly(t *testing.T) {
	p := New()
	m := p.Manifest()
	plugintest.ValidatePlugin(t, p)
	if m.Agent != nil {
		t.Fatal("ScyllaDB must not declare agent transport")
	}
	if len(m.SupportedTransports) != 1 || m.SupportedTransports[0] != plugin.TransportDirect {
		t.Fatalf("unexpected transports: %+v", m.SupportedTransports)
	}
	if !plugintest.CredentialKindSupported(m.Config, plugin.CredentialKindDBPassword) {
		t.Fatal("database password credential should support ScyllaDB")
	}
	if !plugintest.CredentialKindSupported(m.Config, plugin.CredentialKindTLSClientCert) {
		t.Fatal("TLS client certificate credential should support ScyllaDB")
	}
}

func TestProjectionPanelConfigsAreRenderable(t *testing.T) {
	proj := plugintest.Projection(t, New())
	plugintest.ValidateProjectionPanelConfigs(t, proj)

	streamKinds := map[string]plugin.StreamKind{}
	for _, s := range proj.Streams {
		streamKinds[s.RouteID] = s.Kind
	}
	want := map[string]plugin.StreamKind{
		"scylladb.query":             plugin.StreamQuery,
		"scylladb.topology":          plugin.StreamCanvas,
		"scylladb.cluster.metrics":   plugin.StreamMetrics,
		"scylladb.table.metrics":     plugin.StreamMetrics,
		"scylladb.cluster.watch":     plugin.StreamResource,
		"scylladb.compactions.watch": plugin.StreamResource,
	}
	for routeID, kind := range want {
		if streamKinds[routeID] != kind {
			t.Fatalf("stream %s is %q, want %q", routeID, streamKinds[routeID], kind)
		}
	}
}

func TestRoutePermissionsAndAuditEventsAreNamespaced(t *testing.T) {
	for _, r := range New().Routes() {
		if !strings.HasPrefix(r.ID, protocolName+".") {
			t.Fatalf("route %q is not namespaced", r.ID)
		}
		if r.AuditEvent != r.ID {
			t.Fatalf("route %q audit event is %q, want the route id", r.ID, r.AuditEvent)
		}
		if !strings.HasPrefix(r.Permission, protocolName+".") {
			t.Fatalf("route %q permission %q is not namespaced", r.ID, r.Permission)
		}
	}
}

func TestReadmeStatesTheRuntimeRouteCount(t *testing.T) {
	routes := New().Routes()
	streams := 0
	for _, r := range routes {
		if r.IsStream() != (r.Stream != nil) || r.IsStream() == (r.Handle != nil) {
			t.Fatalf("route %q must carry exactly one of Handle/Stream", r.ID)
		}
		if r.IsStream() {
			streams++
		}
	}
	readme, err := os.ReadFile(filepath.Join("..", "..", "README.md"))
	if err != nil {
		t.Fatalf("read README: %v", err)
	}
	stated := fmt.Sprintf("It registers %d routes: %d request handlers and %d streams.", len(routes), len(routes)-streams, streams)
	if !strings.Contains(string(readme), stated) {
		t.Fatalf("README does not state the runtime route count: %q", stated)
	}
}

func TestParseOptionsDefaults(t *testing.T) {
	opts, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{"hosts": "127.0.0.1"}})
	if err != nil {
		t.Fatalf("parse options: %v", err)
	}
	if opts.Username != "" || opts.Password != "" || opts.Port != defaultPort || opts.Consistency != gocql.LocalQuorum {
		t.Fatalf("unexpected defaults: %+v", opts)
	}
	if !opts.ReadOnly || !opts.RequireConfirm {
		t.Fatal("safety toggles must default on")
	}
	if opts.ShardAwarePort {
		t.Fatal("the shard-aware port must stay opt-in so egress stays on the configured port")
	}
	if opts.NumConns != defaultNumConns {
		t.Fatalf("unexpected connection count: %d", opts.NumConns)
	}
}

func TestParseOptionsUsesStoredCredentials(t *testing.T) {
	opts, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{
		"hosts":                "db1, db2",
		"auth":                 authCredential,
		"tls_mode":             "require",
		"shard_aware_port":     true,
		"connections_per_host": float64(9),
	}, Credentials: plugin.NewResolvedCredentials(
		plugin.CredentialBinding{
			Field:      plugin.CredentialRefField,
			Credential: plugin.ResolvedCredential{Values: map[string]string{"username": "scylla", "password": "secret"}},
		},
		plugin.CredentialBinding{
			Field:      clientCertField,
			Credential: plugin.ResolvedCredential{Values: map[string]string{"certificate": "cert-pem", "private_key": "key-pem"}},
		},
	)})
	if err != nil {
		t.Fatalf("parse options: %v", err)
	}
	if opts.Username != "scylla" || opts.Password != "secret" {
		t.Fatalf("unexpected auth material: %+v", opts)
	}
	if opts.ClientCertificate != "cert-pem\nkey-pem" || len(opts.Hosts) != 2 {
		t.Fatalf("unexpected TLS material: %+v", opts)
	}
	if !opts.ShardAwarePort || opts.NumConns != 9 {
		t.Fatalf("unexpected shard settings: %+v", opts)
	}
}

func TestParseOptionsRejectsCertificateWithoutTLS(t *testing.T) {
	_, err := parseOptions(plugin.ConnectConfig{
		Config: map[string]any{"hosts": "db1", "tls_mode": "disable"},
		Credentials: plugin.NewResolvedCredentials(plugin.CredentialBinding{
			Field:      clientCertField,
			Credential: plugin.ResolvedCredential{Values: map[string]string{"certificate": "cert-pem"}},
		}),
	})
	if !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("expected invalid input, got %v", err)
	}
}

func TestClusterConfigWiresGatewayDialerAndShardAwarePort(t *testing.T) {
	transport := plugintest.DirectTransport()
	cluster, err := clusterConfig(options{
		Hosts: []string{"10.0.0.1"}, Port: 9042, Consistency: gocql.LocalOne,
		QueryTimeout: time.Second, NumConns: 3, PageSize: 10,
	}, transport)
	if err != nil {
		t.Fatalf("cluster config: %v", err)
	}
	if cluster.Dialer == nil {
		t.Fatal("the driver must dial through the gateway transport")
	}
	if !cluster.DisableShardAwarePort {
		t.Fatal("the shard-aware port must be disabled unless explicitly enabled")
	}
	if cluster.Hosts[0] != "10.0.0.1:9042" {
		t.Fatalf("unexpected host list: %v", cluster.Hosts)
	}

	enabled, err := clusterConfig(options{
		Hosts: []string{"10.0.0.1:9999"}, Port: 9042, Consistency: gocql.LocalOne,
		QueryTimeout: time.Second, NumConns: 1, PageSize: 10, ShardAwarePort: true, LocalDC: "dc1",
	}, transport)
	if err != nil {
		t.Fatalf("cluster config: %v", err)
	}
	if enabled.DisableShardAwarePort {
		t.Fatal("shard-aware dialing should be enabled when requested")
	}
	if enabled.Hosts[0] != "10.0.0.1:9999" {
		t.Fatalf("explicit host ports must be preserved: %v", enabled.Hosts)
	}
}

func TestConfigSchemaHidesSecretsBehindAuthMode(t *testing.T) {
	schema := configSchema()
	visible := schema.VisibleSecretKeys(map[string]any{"auth": authPassword, "tls_mode": "disable"}, nil)
	if !slices.Contains(visible, "password") {
		t.Fatalf("password should be a visible secret for password auth: %v", visible)
	}
	stored := schema.VisibleValues(map[string]any{
		"auth":             authCredential,
		"credential_id":    "cred-1",
		"password":         "should-be-hidden",
		"username":         "should-be-hidden",
		"tls_mode":         "verify-ca",
		"ca_certificate":   "pem",
		"hosts":            "db1",
		"port":             9042,
		"consistency":      "LOCAL_ONE",
		"read_only":        true,
		"shard_aware_port": false,
	}, nil)
	if _, ok := stored["password"]; ok {
		t.Fatal("inline password must be hidden when a stored credential is selected")
	}
	if _, ok := stored["credential_id"]; !ok {
		t.Fatal("the credential reference must stay visible")
	}
	if _, ok := stored["ca_certificate"]; !ok {
		t.Fatal("the CA certificate must be visible for verify-ca")
	}
}

func TestQuerySafetyStopsBeforeTheCluster(t *testing.T) {
	_, err := executeQueryRequest(context.Background(), &Session{opts: options{ReadOnly: true}}, sqldb.QueryRequest{Query: "insert into events (id) values (1)"})
	if !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("expected read-only forbidden error, got %v", err)
	}
	_, err = executeQueryRequest(context.Background(), &Session{opts: options{RequireConfirm: true}}, sqldb.QueryRequest{Query: "truncate events"})
	var confirmErr confirmationError
	if !errors.As(err, &confirmErr) {
		t.Fatalf("expected confirmation error, got %v", err)
	}
	if got := queryAuditResult(err); got != plugin.AuditDenied {
		t.Fatalf("confirmation should audit as denied, got %s", got)
	}
	if _, err := executeQueryRequest(context.Background(), &Session{opts: options{}}, sqldb.QueryRequest{Query: "  "}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("empty query should be rejected, got %v", err)
	}
}

func TestSplitTracingDirective(t *testing.T) {
	statements, tracing := splitTracingDirective([]string{"TRACING ON", "SELECT 1", "tracing off", "SELECT 2"})
	if tracing {
		t.Fatal("a trailing TRACING OFF must win")
	}
	if len(statements) != 2 || statements[0] != "SELECT 1" || statements[1] != "SELECT 2" {
		t.Fatalf("unexpected statements: %#v", statements)
	}
	if _, on := splitTracingDirective([]string{"tracing  on", "SELECT 1"}); !on {
		t.Fatal("TRACING ON should enable tracing regardless of spacing")
	}
}

func TestCQLDDLValidation(t *testing.T) {
	cols, err := parseColumns([]any{
		map[string]any{"name": "id", "type": "uuid"},
		map[string]any{"name": "payload", "type": "frozen<map<text, text>>"},
	})
	if err != nil {
		t.Fatalf("valid columns rejected: %v", err)
	}
	if len(cols) != 2 || cols[0] != `"id" uuid` {
		t.Fatalf("unexpected columns: %#v", cols)
	}
	if _, err := parseColumns([]any{map[string]any{"name": "bad-name", "type": "text"}}); err == nil {
		t.Fatal("invalid identifier accepted")
	}
	if _, err := parseColumns([]any{map[string]any{"name": "x", "type": "text; drop table users"}}); err == nil {
		t.Fatal("unsafe column type accepted")
	}
	if safePrimaryKey("id); DROP TABLE users") {
		t.Fatal("unsafe primary key accepted")
	}
	if quoteIdent(`a"b`) != `"a""b"` {
		t.Fatalf("identifier quoting must escape embedded quotes: %s", quoteIdent(`a"b`))
	}
}

func TestTabletsClause(t *testing.T) {
	for _, tc := range []struct {
		mode    string
		initial int
		want    string
	}{
		{"", 0, ""},
		{"default", 8, ""},
		{"disabled", 0, " AND tablets = {'enabled': false}"},
		{"enabled", 0, " AND tablets = {'enabled': true}"},
		{"enabled", 64, " AND tablets = {'enabled': true, 'initial': 64}"},
	} {
		got, err := tabletsClause(tc.mode, tc.initial)
		if err != nil {
			t.Fatalf("tabletsClause(%q,%d): %v", tc.mode, tc.initial, err)
		}
		if got != tc.want {
			t.Fatalf("tabletsClause(%q,%d) = %q, want %q", tc.mode, tc.initial, got, tc.want)
		}
	}
	if _, err := tabletsClause("enabled", 999999); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("out-of-range initial tablets should be rejected, got %v", err)
	}
	if _, err := tabletsClause("nonsense", 0); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("unknown tablets mode should be rejected, got %v", err)
	}
}

// ScyllaDB rejects SimpleStrategy for tablet keyspaces, so the form must lead
// with NetworkTopologyStrategy.
func TestKeyspaceCreateDefaultsToNetworkTopology(t *testing.T) {
	schema := routeInput(t, "scylladb.keyspace.create")
	field := schemaField(t, schema, "replication_class")
	if field.Default != "NetworkTopologyStrategy" {
		t.Fatalf("replication class default is %v, want NetworkTopologyStrategy", field.Default)
	}
	dc := schemaField(t, schema, "datacenter_replication")
	if dc.Type != plugin.FieldMap || dc.Item == nil || dc.Item.Type != plugin.FieldNumber {
		t.Fatalf("datacenter replication should be a number map, got %+v", dc)
	}
	got, err := replicationMap("NetworkTopologyStrategy", 1, map[string]any{"dc1": float64(3), "dc2": float64(2)})
	if err != nil {
		t.Fatalf("replication map: %v", err)
	}
	if want := "{'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 2}"; got != want {
		t.Fatalf("replicationMap = %q, want %q", got, want)
	}
	if _, err := replicationMap("NetworkTopologyStrategy", 1, map[string]any{"dc1": float64(21)}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("out-of-range factor: want ErrInvalidInput, got %v", err)
	}
	if _, err := replicationMap("NetworkTopologyStrategy", 1, map[string]any{"dc';drop": float64(1)}); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("unsafe datacenter name accepted: %v", err)
	}
}

func TestStructuredArrayFields(t *testing.T) {
	assertArrayItemKeys(t, "scylladb.table.create", "columns", []string{"name", "type"})
	assertArrayItemKeys(t, "scylladb.type.create", "fields", []string{"name", "type"})
}

func TestRowMutationCQLGeneration(t *testing.T) {
	table := qualified("shop", "orders")

	insertCQL, insertArgs, err := dialect.Insert(table, map[string]any{"id": "u1", "total": float64(42)})
	if err != nil {
		t.Fatalf("insert build: %v", err)
	}
	if insertCQL != `INSERT INTO "shop"."orders" ("id", "total") VALUES (?, ?)` {
		t.Fatalf("unexpected insert CQL: %s", insertCQL)
	}
	if len(insertArgs) != 2 || insertArgs[0] != "u1" || insertArgs[1] != int64(42) {
		t.Fatalf("unexpected insert args: %#v", insertArgs)
	}
	updateCQL, _, err := dialect.Update(table, map[string]any{"id": "u1"}, map[string]any{"total": float64(7)})
	if err != nil {
		t.Fatalf("update build: %v", err)
	}
	if updateCQL != `UPDATE "shop"."orders" SET "total" = ? WHERE "id" = ?` {
		t.Fatalf("unexpected update CQL: %s", updateCQL)
	}
	deleteCQL, _, err := dialect.Delete(table, map[string]any{"tenant": "t1", "id": "u1"})
	if err != nil {
		t.Fatalf("delete build: %v", err)
	}
	if deleteCQL != `DELETE FROM "shop"."orders" WHERE "id" = ? AND "tenant" = ?` {
		t.Fatalf("unexpected delete CQL: %s", deleteCQL)
	}
	if _, _, err := dialect.Delete(table, nil); err == nil {
		t.Fatal("delete with no key should be rejected (would sweep the table)")
	}
	if _, _, err := dialect.Insert(table, map[string]any{"bad-col": 1}); err == nil {
		t.Fatal("insert with unsafe column identifier should be rejected")
	}
}

// CQL rejects "*" beside a function selector, so every column is named.
func TestRowProjectionAsksForTheToken(t *testing.T) {
	got := rowProjection([]string{"id", "name", "tenant"}, []string{"tenant", "id"})
	if want := `token("tenant", "id") AS "_token", "id", "name", "tenant"`; got != want {
		t.Fatalf("rowProjection = %s, want %s", got, want)
	}
	if got := rowProjection([]string{"id"}, nil); got != `"id"` {
		t.Fatalf("keyless tables must not project a token: %s", got)
	}
	if got := rowProjection(nil, nil); got != "*" {
		t.Fatalf("unknown schemas fall back to a wildcard: %s", got)
	}
}

func TestAttachRowKeysGuards(t *testing.T) {
	rows := []row{{"id": "u1"}}
	attachRowKeys(rows, nil, nil)
	if _, ok := rows[0]["_key"]; ok {
		t.Fatal("keyless table must not receive a _key (stays read-only)")
	}
	rows = []row{{"api_key": "secret", "value": 1}}
	attachRowKeys(rows, []string{"api_key"}, sqldb.DefaultRedactColumnPatterns())
	if _, ok := rows[0]["_key"]; ok {
		t.Fatal("sensitive key column must keep the grid read-only")
	}
	rows = []row{{"tenant": "t1", "id": "u1", "total": 9}}
	attachRowKeys(rows, []string{"tenant", "id"}, nil)
	key, ok := rows[0]["_key"].(map[string]any)
	if !ok || len(key) != 2 || key["tenant"] != "t1" || key["id"] != "u1" {
		t.Fatalf("unexpected _key: %#v", rows[0]["_key"])
	}
}

func TestValidateRowKeyRejectsNonPrimaryKey(t *testing.T) {
	if err := sqldb.ValidateRowKey([]string{"id"}, map[string]any{"name": "x"}); err == nil {
		t.Fatal("non-primary-key column accepted as row key")
	}
	if err := sqldb.ValidateRowKey(nil, map[string]any{"id": "x"}); !errors.Is(err, plugin.ErrForbidden) {
		t.Fatalf("keyless table should forbid mutations, got %v", err)
	}
	if err := sqldb.ValidateRowKey([]string{"id"}, map[string]any{"id": "x"}); err != nil {
		t.Fatalf("exact primary key rejected: %v", err)
	}
}

func TestOrderedKeyColumnsFollowsKeyOrder(t *testing.T) {
	rows := []row{
		{"column_name": "name", "kind": "regular", "position": -1},
		{"column_name": "created", "kind": "clustering", "position": 0},
		{"column_name": "bucket", "kind": "partition_key", "position": 1},
		{"column_name": "tenant", "kind": "partition_key", "position": 0},
	}
	if got := orderedKeyColumns(rows, "partition_key", "clustering"); !slices.Equal(got, []string{"tenant", "bucket", "created"}) {
		t.Fatalf("unexpected primary key order: %v", got)
	}
	if got := orderedKeyColumns(rows, "partition_key"); !slices.Equal(got, []string{"tenant", "bucket"}) {
		t.Fatalf("unexpected partition key: %v", got)
	}
}

// shardOf reimplements the biased-token-round-robin sharding the driver reports;
// it must cover every shard and stay stable at the token-space edges.
func TestShardOfMatchesScyllaSharding(t *testing.T) {
	if got := shardOf(0, 1, 12); got != 0 {
		t.Fatalf("single-shard nodes always own shard 0, got %d", got)
	}
	if got := shardOf(math.MinInt64, 8, 12); got != 0 {
		t.Fatalf("the lowest token must land on shard 0, got %d", got)
	}
	if got := shardOf(math.MaxInt64, 8, 12); got != 7 {
		t.Fatalf("the highest token must land on the last shard, got %d", got)
	}
	// msb_ignore drops the top bits, so evenly spaced probes would all collapse
	// onto one shard; scramble the samples the way real partition tokens are.
	seen := map[int]int{}
	const shards = 16
	token := int64(0)
	for range 4096 {
		token = int64(uint64(token)*6364136223846793005 + 1442695040888963407)
		shard := shardOf(token, shards, 12)
		if shard < 0 || shard >= shards {
			t.Fatalf("shardOf produced out-of-range shard %d", shard)
		}
		seen[shard]++
	}
	if len(seen) != shards {
		t.Fatalf("expected every shard to be hit, got %d of %d", len(seen), shards)
	}
}

func TestOwnersOfHandlesTheWrappingRange(t *testing.T) {
	ranges := []tokenRange{
		{Start: math.MaxInt64 - 10, End: math.MinInt64 + 10, Endpoint: "10.0.0.3"},
		{Start: -100, End: 0, Endpoint: "10.0.0.1"},
		{Start: 0, End: 100, Endpoint: "10.0.0.2"},
		{Start: 0, End: 100, Endpoint: "10.0.0.3"},
	}
	owners := ownersOf(ranges, 50)
	if len(owners) != 2 || owners[0].Endpoint != "10.0.0.2" || owners[1].Endpoint != "10.0.0.3" {
		t.Fatalf("unexpected replica set: %#v", owners)
	}
	if got := ownersOf(ranges, math.MinInt64+5); len(got) != 1 || got[0].Endpoint != "10.0.0.3" {
		t.Fatalf("wrapping range must own the low end: %#v", got)
	}
	if got := ownersOf(ranges, math.MaxInt64-5); len(got) != 1 || got[0].Endpoint != "10.0.0.3" {
		t.Fatalf("wrapping range must own the high end: %#v", got)
	}
	if got := ownersOf(ranges, 100000); len(got) != 0 {
		t.Fatalf("unowned token must report no replica: %#v", got)
	}
}

func TestStatusCodeMatchesNodetool(t *testing.T) {
	for _, tc := range []struct {
		state string
		up    bool
		want  string
	}{
		{"NORMAL", true, "UN"},
		{"NORMAL", false, "DN"},
		{"JOINING", true, "UJ"},
		{"LEAVING", false, "DL"},
		{"MOVING", true, "UM"},
		{"", true, "U?"},
	} {
		if got := statusCode(tc.state, tc.up); got != tc.want {
			t.Fatalf("statusCode(%q,%v) = %q, want %q", tc.state, tc.up, got, tc.want)
		}
	}
	if got := statusLabel("UN"); got != "Up / Normal" {
		t.Fatalf("unexpected status label: %s", got)
	}
}

func TestRateCounterIgnoresFirstSampleAndResets(t *testing.T) {
	c := &rateCounter{}
	if got := c.perSecond(100); got != 0 {
		t.Fatalf("first sample must not report a rate, got %v", got)
	}
	c.at = time.Now().Add(-2 * time.Second)
	c.value = 100
	if got := c.perSecond(300); got < 90 || got > 110 {
		t.Fatalf("expected ~100/s, got %v", got)
	}
	c.at = time.Now().Add(-time.Second)
	c.value = 300
	if got := c.perSecond(5); got != 0 {
		t.Fatalf("counter reset must report 0, got %v", got)
	}
}

func TestKeyspaceEstimateAggregation(t *testing.T) {
	totals := estimateTotals{Tables: map[string]tableEstimate{}}
	for _, r := range []row{
		{"table_name": "orders", "partitions_count": int64(10), "mean_partition_size": int64(100)},
		{"table_name": "orders", "partitions_count": int64(5), "mean_partition_size": int64(200)},
		{"table_name": "users", "partitions_count": int64(2), "mean_partition_size": int64(50)},
	} {
		name := r["table_name"].(string)
		partitions := int64Value(r["partitions_count"])
		bytes := partitions * int64Value(r["mean_partition_size"])
		entry := totals.Tables[name]
		entry.Partitions += partitions
		entry.Bytes += bytes
		totals.Tables[name] = entry
		totals.Partitions += partitions
		totals.Bytes += bytes
	}
	if totals.Tables["orders"].Bytes != 2000 || totals.Tables["orders"].Partitions != 15 {
		t.Fatalf("unexpected orders estimate: %+v", totals.Tables["orders"])
	}
	if totals.Partitions != 17 || totals.Bytes != 2100 {
		t.Fatalf("unexpected keyspace totals: %+v", totals)
	}
}

func TestTraceSeverityHighlightsSlowAndFailedSteps(t *testing.T) {
	if got := traceSeverity("Read timeout", 5); got != plugin.SeverityDanger {
		t.Fatalf("timeouts must be dangerous, got %s", got)
	}
	if got := traceSeverity("Scanning cache", 4000); got != plugin.SeverityWarn {
		t.Fatalf("multi-millisecond steps must warn, got %s", got)
	}
	if got := traceSeverity("Request complete", 3); got != plugin.SeveritySuccess {
		t.Fatalf("completion must read as success, got %s", got)
	}
	if got := traceIcon("Merging data from memtable and 2 sstables"); got != "hard-drive" {
		t.Fatalf("unexpected icon: %s", got)
	}
}

func TestTraceCollectorRecordsSessionIDs(t *testing.T) {
	collector := &traceCollector{}
	id := gocql.TimeUUID()
	collector.Trace(id.Bytes())
	collector.Trace([]byte{1, 2, 3})
	if ids := collector.IDs(); len(ids) != 1 || ids[0] != id.String() {
		t.Fatalf("unexpected trace ids: %v", ids)
	}
}

func TestSavedQueryScopeRoundTrip(t *testing.T) {
	storage := newFakeStorage()
	rc := plugin.NewRequestContext(context.Background(), plugin.User{ID: "u1"}, nil, nil, nil, mustJSONBody(t, map[string]any{
		"name": "recent orders", "query": "SELECT * FROM shop.orders LIMIT 10;", "scope": storageScopeUser,
	})).WithStorage(storage)
	saved, err := saveQuery(rc)
	if err != nil {
		t.Fatalf("save query: %v", err)
	}
	item := saved.(row)
	if item["scope"] != storageScopeUser {
		t.Fatalf("unexpected scope: %#v", item)
	}
	key := item["key"].(string)
	if !strings.HasPrefix(key, storageScopeUser+"/") {
		t.Fatalf("user-scoped keys must be namespaced, got %q", key)
	}

	listRC := plugin.NewRequestContext(context.Background(), plugin.User{ID: "u1"}, nil, nil, url.Values{}, nil).WithStorage(storage)
	page, err := listSavedQueries(listRC)
	if err != nil {
		t.Fatalf("list saved queries: %v", err)
	}
	if items := page.(plugin.Page[row]).Items; len(items) != 1 || items[0]["name"] != "recent orders" {
		t.Fatalf("unexpected saved query list: %#v", items)
	}

	deleteRC := plugin.NewRequestContext(context.Background(), plugin.User{ID: "u1"}, nil, map[string]string{"key": key}, nil, nil).WithStorage(storage)
	if _, err := deleteSavedQuery(deleteRC); err != nil {
		t.Fatalf("delete saved query: %v", err)
	}
	page, err = listSavedQueries(listRC)
	if err != nil {
		t.Fatalf("list after delete: %v", err)
	}
	if items := page.(plugin.Page[row]).Items; len(items) != 0 {
		t.Fatalf("saved query survived delete: %#v", items)
	}
}

func TestSavedQueryRejectsUnknownScope(t *testing.T) {
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, nil, nil, nil, mustJSONBody(t, map[string]any{
		"name": "x", "query": "SELECT 1", "scope": "global",
	})).WithStorage(newFakeStorage())
	if _, err := saveQuery(rc); !errors.Is(err, plugin.ErrInvalidInput) {
		t.Fatalf("expected invalid input, got %v", err)
	}
}

func TestPageRowsSortsNumericallyAndPaginates(t *testing.T) {
	rows := []row{
		{"name": "b", "size": int64(20)},
		{"name": "a", "size": int64(100)},
		{"name": "c", "size": int64(3)},
	}
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, nil, nil, url.Values{"sort": {"-size"}, "limit": {"2"}}, nil)
	page, err := pageRows(rc, slices.Clone(rows))
	if err != nil {
		t.Fatalf("page rows: %v", err)
	}
	if len(page.Items) != 2 || page.Items[0]["size"] != int64(100) || page.Items[1]["size"] != int64(20) {
		t.Fatalf("numeric descending sort failed: %#v", page.Items)
	}
	if page.NextCursor != "2" || page.Total == nil || *page.Total != 3 {
		t.Fatalf("unexpected paging: cursor=%q total=%v", page.NextCursor, page.Total)
	}

	searchRC := plugin.NewRequestContext(context.Background(), plugin.User{}, nil, nil, url.Values{"filter": {"c"}}, nil)
	filtered, err := pageRows(searchRC, slices.Clone(rows))
	if err != nil {
		t.Fatalf("page rows: %v", err)
	}
	if len(filtered.Items) != 1 || filtered.Items[0]["name"] != "c" {
		t.Fatalf("free-text filter failed: %#v", filtered.Items)
	}
}

func TestScanRowsWalksPagesForTheGridSearchTerm(t *testing.T) {
	pages := [][]row{
		{{"name": "alpha", tokenColumn: int64(11)}, {"name": "beta", tokenColumn: int64(12)}},
		{{"name": "gamma", tokenColumn: int64(21)}, {"name": "delta", tokenColumn: int64(22)}},
		{{"name": "match-me", tokenColumn: int64(31)}, {"name": "epsilon", tokenColumn: int64(32)}},
	}
	fetch := func(state []byte) ([]row, []byte, error) {
		at, _ := strconv.Atoi(string(state))
		next := []byte(nil)
		if at+1 < len(pages) {
			next = []byte(strconv.Itoa(at + 1))
		}
		return pages[at], next, nil
	}

	rows, next, err := scanRows(fetch, nil, "", 2, 100)
	if err != nil {
		t.Fatalf("scan rows: %v", err)
	}
	if len(rows) != 2 || string(next) != "1" {
		t.Fatalf("an unfiltered scan must read one page: %#v cursor=%q", rows, next)
	}

	rows, next, err = scanRows(fetch, nil, "match-me", 2, 100)
	if err != nil {
		t.Fatalf("scan rows: %v", err)
	}
	if len(rows) != 1 || rows[0]["name"] != "match-me" {
		t.Fatalf("the search term must be honored across pages: %#v", rows)
	}
	if len(next) != 0 {
		t.Fatalf("a scan that reached the end must not hand back a cursor: %q", next)
	}

	rows, next, err = scanRows(fetch, nil, "match-me", 2, 3)
	if err != nil {
		t.Fatalf("scan rows: %v", err)
	}
	if len(rows) != 0 || string(next) != "2" {
		t.Fatalf("the scan budget must stop the walk and resume from a cursor: %#v cursor=%q", rows, next)
	}

	rows, _, err = scanRows(fetch, nil, "1", 2, 100)
	if err != nil {
		t.Fatalf("scan rows: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("the hidden token column must not match the search term: %#v", rows)
	}
}

func TestCreateTableCQLRendersPrimaryKey(t *testing.T) {
	columns := []row{
		{"column_name": "tenant", "kind": "partition_key", "type": "text"},
		{"column_name": "bucket", "kind": "partition_key", "type": "int"},
		{"column_name": "created", "kind": "clustering", "type": "timestamp"},
		{"column_name": "label", "kind": "static", "type": "text"},
		{"column_name": "payload", "kind": "regular", "type": "blob"},
	}
	options := row{
		"compaction":           map[string]any{"class": "IncrementalCompactionStrategy"},
		"caching":              map[string]any{"keys": "ALL"},
		"compression":          map[string]any{"sstable_compression": "LZ4Compressor"},
		"gc_grace_seconds":     864000,
		"default_time_to_live": 0,
	}
	cql := createTableCQL("shop", "orders", columns, options)
	for _, want := range []string{
		`CREATE TABLE "shop"."orders" (`,
		`"label" text static,`,
		`PRIMARY KEY (("tenant", "bucket"), "created")`,
		`compaction = {'class': 'IncrementalCompactionStrategy'}`,
		`gc_grace_seconds = 864000`,
	} {
		if !strings.Contains(cql, want) {
			t.Fatalf("generated CQL missing %q:\n%s", want, cql)
		}
	}
}

func TestScyllaErrMapsDriverErrors(t *testing.T) {
	if err := scyllaErr(gocql.ErrNotFound); !errors.Is(err, plugin.ErrNotFound) {
		t.Fatalf("not-found should map to ErrNotFound, got %v", err)
	}
	if err := scyllaErr(context.DeadlineExceeded); !errors.Is(err, plugin.ErrUnavailable) {
		t.Fatalf("timeouts should map to ErrUnavailable, got %v", err)
	}
	if err := scyllaErr(nil); err != nil {
		t.Fatalf("nil should stay nil, got %v", err)
	}
}

// The canvas scene must stay usable from the keyboard, redraw on theme change,
// and only report a redraw when something actually changed.
func TestTopologySceneInteraction(t *testing.T) {
	scene := &topologyScene{data: topologyData{
		ClusterName: "probe",
		Nodes: []topologyNode{
			{Address: "10.0.0.1", DataCenter: "dc1", Rack: "r1", Status: "UN", Up: true, Shards: 4, TokenCount: 3, ShardTokens: []int{3, 0, 1, 2}, Local: true},
			{Address: "10.0.0.2", DataCenter: "dc1", Rack: "r2", Status: "DN", Shards: 2, TokenCount: 2, ShardTokens: []int{1, 1}},
		},
		Boundaries: []ringBoundary{{Token: math.MinInt64 / 2, Node: 0}, {Token: 0, Node: 1}, {Token: math.MaxInt64 / 2, Node: 0}},
	}}

	if !scene.handle(&canvas.ReadyEvent{Width: 1200, Height: 720, Theme: plugin.PanelThemeLight}) {
		t.Fatal("ready must trigger a redraw")
	}
	if scene.theme != plugin.PanelThemeLight || scene.width != 1200 {
		t.Fatalf("ready did not update the scene: %+v", scene)
	}
	if !scene.handle(&canvas.KeyEvent{Event: "keydown", Key: "ArrowRight"}) || scene.node != 1 {
		t.Fatalf("arrow right must select the next node, got %d", scene.node)
	}
	if !scene.handle(&canvas.KeyEvent{Event: "keydown", Key: "ArrowDown"}) || scene.shard != 1 {
		t.Fatalf("arrow down must select the next shard, got %d", scene.shard)
	}
	if !scene.handle(&canvas.KeyEvent{Event: "keydown", Key: "ArrowDown"}) || scene.shard != 0 {
		t.Fatalf("shard selection must wrap within the node's shard count, got %d", scene.shard)
	}
	if !scene.handle(&canvas.KeyEvent{Event: "keydown", Key: "Home"}) || scene.node != 0 {
		t.Fatalf("Home must select the coordinator, got %d", scene.node)
	}
	if scene.handle(&canvas.KeyEvent{Event: "keydown", Key: "Escape"}) {
		t.Fatal("unhandled keys must not force a redraw")
	}
	if scene.handle(&canvas.PointerEvent{Event: canvas.PointerMove, X: 1, Y: 1}) {
		t.Fatal("pointer moves without a region must not force a redraw")
	}
	if !scene.handle(&canvas.PointerEvent{Event: canvas.PointerDown, RegionID: "node:10.0.0.2"}) || scene.node != 1 {
		t.Fatalf("clicking a node region must select it, got %d", scene.node)
	}
	if !scene.handle(&canvas.PointerEvent{Event: canvas.PointerDown, RegionID: "shard:1"}) || scene.shard != 1 {
		t.Fatalf("clicking a shard region must select it, got %d", scene.shard)
	}
	if scene.handle(&canvas.PointerEvent{Event: canvas.PointerDown, RegionID: "shard:9"}) {
		t.Fatal("out-of-range shard regions must be ignored")
	}
}

func TestTopologyFrameIsThemeAwareAndAccessible(t *testing.T) {
	scene := &topologyScene{width: 1100, height: 700, data: topologyData{
		ClusterName: "probe",
		Nodes: []topologyNode{
			{Address: "10.0.0.1", DataCenter: "dc1", Rack: "r1", Status: "UN", Up: true, Shards: 4, TokenCount: 2, ShardTokens: []int{2, 0, 0, 0}, ShardClients: []int{3, 0, 0, 0}, Local: true},
			{Address: "10.0.0.2", DataCenter: "dc1", Rack: "r2", Status: "UN", Up: true, Shards: 4, TokenCount: 2, ShardTokens: []int{0, 1, 1, 0}},
		},
		Boundaries: []ringBoundary{{Token: math.MinInt64 / 2, Node: 0}, {Token: 0, Node: 1}, {Token: math.MaxInt64 / 2, Node: 0}, {Token: math.MaxInt64, Node: 1}},
	}}
	scene.announce()

	for _, theme := range []plugin.PanelTheme{plugin.PanelThemeDark, plugin.PanelThemeLight} {
		scene.theme = theme
		frame := scene.frame()
		raw, err := json.Marshal(frame)
		if err != nil {
			t.Fatalf("frames must marshal to the canvas wire format: %v", err)
		}
		wantBackground := themePalette(theme).Background
		if !strings.Contains(string(raw), wantBackground) {
			t.Fatalf("%s frame does not use the themed background %s", theme, wantBackground)
		}
		if len(frame.Commands) == 0 {
			t.Fatal("frame has no draw commands")
		}
		nodeRegions, shardRegions := 0, 0
		for _, region := range frame.Regions {
			if region.Label == "" {
				t.Fatalf("every interactive region needs a label for assistive tech: %+v", region)
			}
			switch {
			case strings.HasPrefix(region.ID, "node:"):
				nodeRegions++
			case strings.HasPrefix(region.ID, "shard:"):
				shardRegions++
			}
		}
		if nodeRegions == 0 || shardRegions != 4 {
			t.Fatalf("expected node and per-shard hit targets, got %d node and %d shard regions", nodeRegions, shardRegions)
		}
	}
	if scene.announcement != "" {
		t.Fatal("an announcement must be emitted once, then cleared")
	}
}

// Very large clusters must not produce one arc per vnode.
func TestTopologyRingIsDownsampled(t *testing.T) {
	data := topologyData{ClusterName: "big"}
	for node := range 6 {
		data.Nodes = append(data.Nodes, topologyNode{Address: "10.0.0." + string(rune('1'+node)), Shards: 8, ShardTokens: make([]int, 8)})
	}
	for i := range 6 * 256 {
		data.Boundaries = append(data.Boundaries, ringBoundary{
			Token: math.MinInt64 + int64(i)*(math.MaxInt64/768),
			Node:  i % 6,
		})
	}
	sort.Slice(data.Boundaries, func(i, j int) bool { return data.Boundaries[i].Token < data.Boundaries[j].Token })
	scene := &topologyScene{width: 1400, height: 900, data: data}
	frame := scene.frame()
	if len(frame.Commands) > 1200 {
		t.Fatalf("ring drawing is not bounded: %d commands", len(frame.Commands))
	}
	if len(frame.Regions) > maxRingSlots+64 {
		t.Fatalf("ring hit targets are not bounded: %d regions", len(frame.Regions))
	}
}

func TestCanvasConfigDeclaresAccessibleInteraction(t *testing.T) {
	cfg := topologyCanvasConfig()
	if cfg.ScaleMode != plugin.CanvasScaleResize || !cfg.ResizeEvents {
		t.Fatal("a responsive canvas must use resize scaling with resize events")
	}
	if !cfg.Interactive || !cfg.Pointer || !cfg.Keyboard {
		t.Fatal("the topology canvas must accept pointer and keyboard input")
	}
	if cfg.WheelMode != plugin.CanvasWheelNone {
		t.Fatalf("wheel capture would break page scrolling: %q", cfg.WheelMode)
	}
	if cfg.AriaLabel == "" || cfg.Instructions == "" {
		t.Fatal("canvas panels need an aria label and instructions")
	}
}

func routeInput(t *testing.T, id string) *plugin.Schema {
	t.Helper()
	for _, r := range New().Routes() {
		if r.ID == id {
			if r.Input == nil {
				t.Fatalf("route %q has no input schema", id)
			}
			return r.Input
		}
	}
	t.Fatalf("route %q not found", id)
	return nil
}

func schemaField(t *testing.T, schema *plugin.Schema, key string) plugin.Field {
	t.Helper()
	for _, g := range schema.Groups {
		for _, f := range g.Fields {
			if f.Key == key {
				return f
			}
		}
	}
	t.Fatalf("field %q not found", key)
	return plugin.Field{}
}

func assertArrayItemKeys(t *testing.T, routeID, fieldKey string, wantKeys []string) {
	t.Helper()
	field := schemaField(t, routeInput(t, routeID), fieldKey)
	if field.Type != plugin.FieldArray || field.Item == nil || field.Item.Type != plugin.FieldObject {
		t.Fatalf("%s.%s is not an object array: %+v", routeID, fieldKey, field)
	}
	got := make([]string, 0, len(field.Item.Fields))
	for _, f := range field.Item.Fields {
		got = append(got, f.Key)
	}
	if !slices.Equal(got, wantKeys) {
		t.Fatalf("%s.%s item keys = %v, want %v", routeID, fieldKey, got, wantKeys)
	}
}

func mustJSONBody(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal JSON: %v", err)
	}
	return raw
}

type fakeStorage struct {
	items map[string]plugin.StorageItem
}

func newFakeStorage() *fakeStorage {
	return &fakeStorage{items: map[string]plugin.StorageItem{}}
}

func (s *fakeStorage) Get(_ context.Context, scope plugin.StorageScope, key string) (plugin.StorageItem, error) {
	item, ok := s.items[scope.Collection+"/"+key]
	if !ok {
		return plugin.StorageItem{}, plugin.ErrNotFound
	}
	return item, nil
}

func (s *fakeStorage) Put(_ context.Context, collection string, item plugin.StorageItem) (plugin.StorageItem, error) {
	item.UpdatedAt = time.Now()
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
	out := []plugin.StorageItem{}
	for full, item := range s.items {
		if !strings.HasPrefix(full, scope.Collection+"/") {
			continue
		}
		if filter != nil && filter.ContentType != "" && item.ContentType != filter.ContentType {
			continue
		}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}
