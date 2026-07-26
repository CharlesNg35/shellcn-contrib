package couchbase

import (
	"context"
	"encoding/json"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	contribtest "github.com/charlesng35/shellcn-contrib/shared/plugintest"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

var resourceToken = regexp.MustCompile(`\$\{resource\.([a-zA-Z]+)\}`)

// TestQueryEditorDefaultsIntegration runs every PanelQueryEditor initial query
// shipped in the manifest against a freshly seeded cluster that has no
// user-built index, interpolating ${resource.*} exactly as the web client does.
// A default that needs a prerequisite the operator has not performed yet fails
// here.
func TestQueryEditorDefaultsIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_COUCHBASE_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_COUCHBASE_INTEGRATION=1 to run against Couchbase Server")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	host, managementPort, queryPort := couchbaseEndpoint(ctx, t)
	p := New()
	sess, err := p.Connect(ctx, plugin.ConnectConfig{
		Net: plugintest.DirectTransport(),
		Config: map[string]any{
			"host": host, "port": managementPort, "query_port": queryPort,
			"username": integrationUser, "password": integrationPassword,
			"read_only": false, "require_write_confirmation": true, "timeout": "60s",
		},
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()

	h := contribtest.NewHarness(t, p.Routes())
	bucket := "shellcn_qd_" + time.Now().UTC().Format("20060102150405")
	h.Call(ctx, rid("bucket.create"), sess, nil, nil, mustMarshal(t, map[string]any{
		"name": bucket, "bucket_type": "couchbase", "ram_quota_mb": 128, "replica_number": 0,
	}))
	defer h.CallNoFail(context.Background(), rid("bucket.delete"), sess, map[string]string{"bucket": bucket})
	waitForBucket(ctx, t, h, sess, bucket)

	scopeKey := keyspace{Bucket: bucket, Scope: "app"}
	h.Call(ctx, rid("scope.create"), sess, map[string]string{"bucket": bucket}, nil,
		mustMarshal(t, map[string]any{"name": "app"}))
	h.Call(ctx, rid("collection.create"), sess, map[string]string{"keyspace": scopeKey.uid()}, nil,
		mustMarshal(t, map[string]any{"name": "orders", "max_ttl": 0}))
	collectionKey := keyspace{Bucket: bucket, Scope: "app", Collection: "orders"}
	waitForCollection(ctx, t, sess.(*Session), collectionKey)

	defaultKey := keyspace{Bucket: bucket, Scope: "_default", Collection: "_default"}
	waitForCollection(ctx, t, sess.(*Session), defaultKey)

	// Documents only: no index is created, so a default that needs one fails.
	for _, key := range []keyspace{collectionKey, defaultKey} {
		for _, customer := range []string{"ada", "grace", "linus"} {
			h.Call(ctx, rid("document.insert"), sess, map[string]string{"keyspace": key.uid()}, nil,
				mustMarshal(t, map[string]any{
					"key":   "order::" + customer,
					"value": map[string]any{"type": "order", "customer": customer, "total": 42, "status": "paid"},
				}))
		}
	}

	// A sequential scan reads the persisted snapshot, so the seeded documents
	// only become visible to SQL++ once they reach disk.
	for _, key := range []keyspace{collectionKey, defaultKey} {
		waitForScannableDocuments(ctx, t, sess.(*Session), key, 3)
	}

	// Every default is exercised against the named keyspaces and against the
	// bucket's default scope and collection, whose system:keyspaces rows omit the
	// bucket and scope fields.
	identities := map[string][]plugin.ResourceIdentity{
		"cluster": {{Kind: "cluster", Name: "Cluster", UID: clusterUID}},
		"bucket":  {bucketRef(bucket)},
		"scope":   {scopeRef(bucket, "app"), scopeRef(bucket, "_default")},
		"collection": {
			collectionRef(bucket, "app", "orders"),
			collectionRef(bucket, "_default", "_default"),
		},
	}

	panels := 0
	for _, resource := range p.Manifest().Resources {
		subjects, known := identities[resource.Kind]
		for _, tab := range resource.Detail.Tabs {
			if tab.Type != plugin.PanelQueryEditor {
				continue
			}
			config, ok := tab.Config.(plugin.QueryEditorConfig)
			if !ok {
				t.Fatalf("%s/%s: unexpected query editor config %T", resource.Kind, tab.Key, tab.Config)
			}
			if !known {
				t.Fatalf("%s/%s: no seeded resource for kind %q", resource.Kind, tab.Key, resource.Kind)
			}
			panels++
			for _, identity := range subjects {
				label := resource.Kind + "/" + tab.Key + " on " + identity.UID
				statement := interpolateResource(t, config.InitialQuery, identity)
				request, err := json.Marshal(map[string]any{"query": statement, "confirm": false})
				if err != nil {
					t.Fatalf("marshal request: %v", err)
				}
				params := map[string]string{}
				for key, value := range tab.Source.Params {
					params[key] = interpolateResource(t, value, identity)
				}
				frame := streamFrames(ctx, t, h, sess, tab.Source.RouteID, params, string(request))
				t.Logf("%s: %d row(s)\n%s", label, assertQueryFrame(t, label, statement, frame), statement)
			}
		}
	}
	if panels == 0 {
		t.Fatal("the manifest exposes no query editor panels")
	}
}

func waitForScannableDocuments(ctx context.Context, t *testing.T, s *Session, k keyspace, want int) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for {
		res, err := s.exec(ctx, "SELECT RAW COUNT(*) FROM "+k.path())
		if err == nil && len(res.Results) == 1 && strings.TrimSpace(string(res.Results[0])) == strconv.Itoa(want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never exposed %d documents to SQL++: %v", k.label(), want, err)
		}
		time.Sleep(2 * time.Second)
	}
}

// interpolateResource mirrors the web client's ${resource.*} substitution, which
// fails loudly when a token has no value on the opened resource.
func interpolateResource(t *testing.T, template string, identity plugin.ResourceIdentity) string {
	t.Helper()
	fields := map[string]string{
		"kind": identity.Kind, "name": identity.Name, "scope": identity.Scope,
		"namespace": identity.Namespace, "uid": identity.UID,
	}
	return resourceToken.ReplaceAllStringFunc(template, func(token string) string {
		key := resourceToken.FindStringSubmatch(token)[1]
		value, ok := fields[key]
		if !ok || value == "" {
			t.Fatalf("cannot resolve %s on %#v", token, identity)
		}
		return value
	})
}

// assertQueryFrame fails when a default errors, needs a write confirmation, or
// comes back empty, and returns how many rows it produced.
func assertQueryFrame(t *testing.T, label, statement, frame string) int {
	t.Helper()
	trimmed := strings.TrimSpace(frame)
	if trimmed == "" {
		t.Errorf("%s: the default query produced no frame\n%s", label, statement)
		return 0
	}
	var decoded struct {
		Error           string  `json:"error"`
		ConfirmRequired bool    `json:"confirmRequired"`
		Rows            [][]any `json:"rows"`
	}
	if err := json.Unmarshal([]byte(trimmed), &decoded); err != nil {
		t.Errorf("%s: undecodable frame %s", label, trimmed)
		return 0
	}
	switch {
	case decoded.Error != "":
		t.Errorf("%s: the default query failed: %s\n%s", label, decoded.Error, statement)
	case decoded.ConfirmRequired:
		t.Errorf("%s: the default query asks for write confirmation\n%s", label, statement)
	case len(decoded.Rows) == 0:
		t.Errorf("%s: the default query returned no rows\n%s\n%s", label, statement, trimmed)
	}
	return len(decoded.Rows)
}
