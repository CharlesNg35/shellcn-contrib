package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestDynamoDBPluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_DYNAMODB_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_DYNAMODB_INTEGRATION=1 to run against DynamoDB")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	cfg := dynamoDBIntegrationConfig(ctx, t)
	p := New()
	sess, err := p.Connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	s := sess.(*Session)

	routes := plugintest.RouteMap(p.Routes())
	table := "shellcn_it_" + time.Now().UTC().Format("20060102150405")
	call(ctx, t, routes[rid("table.create")], sess, nil, nil, testJSON(t, map[string]any{
		"name": table, "partition_key": "pk", "partition_key_type": "S", "sort_key": "sk", "sort_key_type": "S", "billing_mode": "PAY_PER_REQUEST",
	}))
	waitForTable(ctx, t, s, table)
	defer callNoFail(context.Background(), routes[rid("table.delete")], sess, map[string]string{"table": table})

	call(ctx, t, routes[rid("item.put")], sess, map[string]string{"table": table}, nil, testJSON(t, map[string]any{
		"item": map[string]any{"pk": "user#1", "sk": "profile", "name": "Ada Lovelace", "age": 37, "active": true},
	}))

	call(ctx, t, routes[rid("tables.tree")], sess, nil, nil, nil)
	tables := pageItems(call(ctx, t, routes[rid("tables.list")], sess, nil, nil, nil))
	if !hasRowName(tables, table) {
		t.Fatalf("expected table %q in %#v", table, tables)
	}
	call(ctx, t, routes[rid("table.read")], sess, map[string]string{"table": table}, nil, nil)
	call(ctx, t, routes[rid("table.capacity")], sess, map[string]string{"table": table}, nil, nil)
	call(ctx, t, routes[rid("indexes.list")], sess, map[string]string{"table": table}, nil, nil)
	call(ctx, t, routes[rid("ttl.read")], sess, map[string]string{"table": table}, nil, nil)
	call(ctx, t, routes[rid("tags.list")], sess, map[string]string{"table": table}, nil, nil)
	call(ctx, t, routes[rid("backups.tree")], sess, nil, nil, nil)
	call(ctx, t, routes[rid("backups.list")], sess, map[string]string{"table": table}, nil, nil)

	items := pageItems(call(ctx, t, routes[rid("items.list")], sess, map[string]string{"table": table}, url.Values{"limit": []string{"25"}}, nil))
	if len(items) != 1 || items[0]["name"] != "Ada Lovelace" {
		t.Fatalf("expected one item, got %#v", items)
	}
	ref := items[0]["ref"].(map[string]any)
	id := fmt.Sprint(ref["uid"])
	item := call(ctx, t, routes[rid("item.read")], sess, map[string]string{"id": id}, nil, nil)
	if row, ok := item.(row); !ok || row["name"] != "Ada Lovelace" {
		t.Fatalf("unexpected item: %#v", item)
	}

	call(ctx, t, routes[rid("completion")], sess, nil, nil, nil)
	result, err := executePartiQL(ctx, s, sqldb.QueryRequest{Query: fmt.Sprintf(`SELECT * FROM "%s" WHERE pk='user#1'`, table)})
	if err != nil || result.RowCount != 1 {
		t.Fatalf("partiql query: result=%#v err=%v", result, err)
	}

	call(ctx, t, routes[rid("item.delete")], sess, map[string]string{"id": id}, nil, nil)
}

func dynamoDBIntegrationConfig(ctx context.Context, t *testing.T) map[string]any {
	t.Helper()
	endpoint := os.Getenv("SHELLCN_DYNAMODB_ENDPOINT")
	if endpoint == "" {
		endpoint = startDynamoDBContainer(ctx, t)
	}
	accessKey := os.Getenv("SHELLCN_DYNAMODB_ACCESS_KEY_ID")
	secretKey := os.Getenv("SHELLCN_DYNAMODB_SECRET_ACCESS_KEY")
	if accessKey == "" {
		accessKey = "shellcn"
	}
	if secretKey == "" {
		secretKey = "shellcn"
	}
	region := os.Getenv("SHELLCN_DYNAMODB_REGION")
	if region == "" {
		region = defaultRegion
	}
	return map[string]any{
		"region":            region,
		"endpoint":          endpoint,
		"auth":              "access_key",
		"access_key_id":     accessKey,
		"secret_access_key": secretKey,
		"tls_mode":          "disable",
		"read_only":         false,
		"confirm_writes":    false,
		"timeout":           "15s",
		"page_limit":        100,
	}
}

func startDynamoDBContainer(ctx context.Context, t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI unavailable and SHELLCN_DYNAMODB_ENDPOINT is not set")
	}
	name := "shellcn-dynamodb-it-" + time.Now().UTC().Format("20060102150405")
	run(ctx, t, "docker", "run", "-d", "--rm", "--name", name,
		"-p", "127.0.0.1::8000",
		"amazon/dynamodb-local:latest",
		"-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb")
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "-f", name).Run()
	})
	out := run(ctx, t, "docker", "port", name, "8000/tcp")
	host, port, err := net.SplitHostPort(strings.TrimSpace(out))
	if err != nil {
		t.Fatalf("unexpected docker port output: %q", out)
	}
	endpoint := "http://" + net.JoinHostPort(host, port)
	cfg := map[string]any{"region": defaultRegion, "endpoint": endpoint, "auth": "access_key", "access_key_id": "shellcn", "secret_access_key": "shellcn", "tls_mode": "disable", "timeout": "15s", "page_limit": 100}
	deadline := time.Now().Add(60 * time.Second)
	for {
		p := New()
		sess, err := p.Connect(ctx, plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()})
		if err == nil {
			_ = sess.Close()
			return endpoint
		}
		if time.Now().After(deadline) {
			t.Fatalf("DynamoDB Local container did not become ready: %v", err)
		}
		time.Sleep(1 * time.Second)
	}
}

func waitForTable(ctx context.Context, t *testing.T, s *Session, table string) {
	t.Helper()
	waiter := awsdynamodb.NewTableExistsWaiter(s.client)
	if err := waiter.Wait(ctx, &awsdynamodb.DescribeTableInput{TableName: aws.String(table)}, 30*time.Second); err != nil {
		t.Fatalf("wait for table: %v", err)
	}
}

func call(ctx context.Context, t *testing.T, route plugin.Route, sess plugin.Session, params map[string]string, query url.Values, body []byte) any {
	t.Helper()
	out, err := route.Handle(plugin.NewRequestContext(ctx, plugin.User{}, sess, params, query, body))
	if err != nil {
		t.Fatalf("%s: %v", route.ID, err)
	}
	return out
}

func callNoFail(ctx context.Context, route plugin.Route, sess plugin.Session, params map[string]string) {
	_, _ = route.Handle(plugin.NewRequestContext(ctx, plugin.User{}, sess, params, nil, nil))
}

func pageItems(page any) []map[string]any {
	data, _ := json.Marshal(page)
	var decoded struct {
		Items []map[string]any `json:"items"`
	}
	_ = json.Unmarshal(data, &decoded)
	return decoded.Items
}

func testJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func run(ctx context.Context, t *testing.T, name string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
	return string(out)
}

func hasRowName(rows []map[string]any, name string) bool {
	for _, row := range rows {
		if fmt.Sprint(row["name"]) == name {
			return true
		}
	}
	return false
}
