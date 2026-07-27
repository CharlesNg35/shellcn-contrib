package snowflake

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	contribtest "github.com/charlesng35/shellcn-contrib/shared/plugintest"
	"github.com/charlesng35/shellcn-contrib/shared/sqldb"
	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

// TestSnowflakePluginIntegration drives the plugin against a real Snowflake
// account. Snowflake is SaaS, so there is nothing to start locally: the test is
// gated on SHELLCN_SNOWFLAKE_INTEGRATION=1 plus real credentials and skips
// otherwise. It creates one uniquely named database and drops it on the way out.
func TestSnowflakePluginIntegration(t *testing.T) {
	if os.Getenv("SHELLCN_SNOWFLAKE_INTEGRATION") != "1" {
		t.Skip("set SHELLCN_SNOWFLAKE_INTEGRATION=1 and SNOWFLAKE_* credentials to run against a Snowflake account")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	sess, err := connect(ctx, integrationConnectConfig(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = sess.Close() }()
	s := sess.(*Session)

	h := contribtest.NewHarness(t, routes())
	call := func(id string, params map[string]string, query url.Values, body []byte) any {
		t.Helper()
		return h.Call(ctx, id, plugin.Session(s), params, query, body)
	}

	account, ok := call("snowflake.account.overview", nil, nil, nil).(row)
	if !ok || text(account["account"]) == "" {
		t.Fatalf("account overview did not report a session: %#v", account)
	}
	t.Logf("connected to account %s (%s), version %s", account["account"], account["region"], account["version"])

	database := "SHELLCN_IT_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	call("snowflake.database.create", nil, nil, mustJSON(t, map[string]any{"name": database, "transient": true, "if_not_exists": true}))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cleanupCancel()
		if _, err := s.db.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS "+quoteIdent(database)); err != nil {
			t.Errorf("cleanup database %s: %v", database, err)
		}
	})

	scope := map[string]string{"database": database}
	if !pageHasName(t, call("snowflake.databases.list", nil, url.Values{"filter": {database}}, nil), database) {
		t.Fatalf("created database %s was not listed", database)
	}

	call("snowflake.schema.create", scope, nil, mustJSON(t, map[string]any{"name": "APP", "if_not_exists": true}))
	schemaScope := map[string]string{"database": database, "schema": "APP"}
	if !pageHasName(t, call("snowflake.schemas.list", scope, nil, nil), "APP") {
		t.Fatal("created schema was not listed")
	}

	call("snowflake.table.create", schemaScope, nil, mustJSON(t, map[string]any{
		"name": "PEOPLE",
		"columns": []map[string]any{
			{"name": "ID", "type": "NUMBER(38,0)", "primary": true},
			{"name": "NAME", "type": "VARCHAR(255)", "nullable": true},
			{"name": "ACCESS_TOKEN", "type": "VARCHAR(255)", "nullable": true},
		},
		"if_not_exists": true,
	}))
	object := map[string]string{"database": database, "schema": "APP", "name": "PEOPLE"}
	if !pageHasName(t, call("snowflake.tables.list", schemaScope, nil, nil), "PEOPLE") {
		t.Fatal("created table was not listed")
	}

	for i, name := range []string{"alice", "bob", "carol", "dave", "erin"} {
		call("snowflake.table.row.insert", object, nil, mustJSON(t, map[string]any{
			"values": map[string]any{"ID": i + 1, "NAME": name, "ACCESS_TOKEN": "tok-" + name},
		}))
	}

	columns := call("snowflake.table.columns", object, nil, nil).(plugin.Page[row])
	if len(columns.Items) != 3 {
		t.Fatalf("expected three columns, got %d", len(columns.Items))
	}
	for _, column := range columns.Items {
		if text(column["name"]) == "ID" && column["primary_key"] != true {
			t.Fatalf("declared primary key was not reported: %#v", column)
		}
	}

	// Paging must walk the whole table exactly once, with no phantom page.
	seen, cursor := 0, ""
	for page := 0; page < 6; page++ {
		query := url.Values{"limit": {"2"}}
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		p := call("snowflake.table.rows", object, query, nil).(plugin.Page[row])
		if len(p.Items) == 0 {
			t.Fatalf("page %d came back empty", page)
		}
		if p.Total == nil || *p.Total != 5 {
			t.Fatalf("unfiltered total should be 5, got %v", p.Total)
		}
		for _, r := range p.Items {
			if r["ACCESS_TOKEN"] != sqldb.RedactedValue {
				t.Fatalf("sensitive column was not redacted: %#v", r)
			}
			if _, ok := r["_key"].(map[string]any); !ok {
				t.Fatalf("row is missing its primary-key handle: %#v", r)
			}
		}
		seen += len(p.Items)
		if cursor = p.NextCursor; cursor == "" {
			break
		}
	}
	if seen != 5 || cursor != "" {
		t.Fatalf("paging saw %d rows with trailing cursor %q", seen, cursor)
	}

	filtered := call("snowflake.table.rows", object, url.Values{"filter": {"alice"}}, nil).(plugin.Page[row])
	if len(filtered.Items) != 1 {
		t.Fatalf("server-side filter should match one row, got %d", len(filtered.Items))
	}
	sorted := call("snowflake.table.rows", object, url.Values{"sort": {"-NAME"}, "limit": {"1"}}, nil).(plugin.Page[row])
	if len(sorted.Items) != 1 || text(sorted.Items[0]["NAME"]) != "erin" {
		t.Fatalf("descending sort should start at erin: %#v", sorted.Items)
	}

	call("snowflake.table.row.update", object, nil, mustJSON(t, map[string]any{
		"key": map[string]any{"ID": 1}, "values": map[string]any{"NAME": "alice-renamed"},
	}))
	call("snowflake.table.row.delete", object, nil, mustJSON(t, map[string]any{"key": map[string]any{"ID": 5}}))
	after := call("snowflake.table.rows", object, nil, nil).(plugin.Page[row])
	if after.Total == nil || *after.Total != 4 {
		t.Fatalf("row mutations were not applied: %v", after.Total)
	}

	ddl := call("snowflake.table.ddl", object, nil, nil).(row)
	if !strings.Contains(strings.ToUpper(text(ddl["definition"])), "PEOPLE") {
		t.Fatalf("unexpected DDL: %#v", ddl)
	}
	overview := call("snowflake.table.overview", object, nil, nil).(row)
	if text(overview["primary_key"]) != "ID" {
		t.Fatalf("table overview lost the primary key: %#v", overview)
	}

	warehouses := call("snowflake.warehouses.list", nil, nil, nil).(plugin.Page[row])
	if len(warehouses.Items) == 0 {
		t.Skip("the connected role sees no warehouses; skipping the warehouse, role and history assertions")
	}
	warehouse := text(warehouses.Items[0]["name"])
	detail := call("snowflake.warehouse.overview", map[string]string{"warehouse": warehouse}, nil, nil).(row)
	if text(detail["name"]) != warehouse {
		t.Fatalf("warehouse overview returned %#v", detail)
	}

	if roles := call("snowflake.roles.list", nil, nil, nil).(plugin.Page[row]); len(roles.Items) > 0 {
		name := text(roles.Items[0]["name"])
		call("snowflake.role.grants", map[string]string{"role": name}, nil, nil)
		call("snowflake.role.overview", map[string]string{"role": name}, nil, nil)
	}
	if users := call("snowflake.users.list", nil, nil, nil).(plugin.Page[row]); len(users.Items) > 0 {
		name := text(users.Items[0]["name"])
		call("snowflake.user.overview", map[string]string{"user": name}, nil, nil)
		call("snowflake.user.grants", map[string]string{"user": name}, nil, nil)
	}

	history := call("snowflake.queries.list", nil, url.Values{"limit": {"5"}}, nil).(plugin.Page[row])
	if len(history.Items) == 0 {
		t.Fatal("query history should contain the statements this test just ran")
	}
	queryUID := text(history.Items[0]["query_id"])
	if _, err := queryID(queryUID); err != nil {
		t.Fatalf("query history returned an unusable id %q: %v", queryUID, err)
	}
	call("snowflake.query.overview", map[string]string{"id": queryUID}, nil, nil)

	frames := h.Stream(ctx, "snowflake.query", plugin.Session(s), nil, nil, []byte(`{"query":"SELECT 1 AS ONE, 2 AS TWO;"}`))
	var result queryResult
	if err := json.Unmarshal(firstFrame(t, frames), &result); err != nil {
		t.Fatalf("decode query frame: %v", err)
	}
	if len(result.Columns) != 2 || result.RowCount != 1 || result.QueryID == "" {
		t.Fatalf("query editor result is incomplete: %+v", result)
	}

	call("snowflake.table.truncate", object, nil, nil)
	call("snowflake.table.drop", object, nil, nil)
	call("snowflake.schema.drop", schemaScope, nil, nil)
}

// integrationConnectConfig reads the account, session and authentication
// settings from the environment. Key-pair material arrives the way the gateway
// delivers it at runtime: as a resolved credential, never as plain config.
func integrationConnectConfig(t *testing.T) plugin.ConnectConfig {
	t.Helper()
	account := requireEnv(t, "SNOWFLAKE_ACCOUNT")
	host := os.Getenv("SNOWFLAKE_HOST")
	if strings.TrimSpace(host) == "" {
		host = account + ".snowflakecomputing.com"
	}
	cfg := map[string]any{
		"account":                          account,
		"host":                             host,
		"database":                         requireEnv(t, "SNOWFLAKE_DATABASE"),
		"schema":                           envDefault("SNOWFLAKE_SCHEMA", defaultSchema),
		"warehouse":                        os.Getenv("SNOWFLAKE_WAREHOUSE"),
		"role":                             os.Getenv("SNOWFLAKE_ROLE"),
		"username":                         requireEnv(t, "SNOWFLAKE_USER"),
		"read_only":                        false,
		"require_destructive_confirmation": false,
		"row_limit":                        50,
		"query_timeout":                    "60s",
	}
	if key := strings.TrimSpace(os.Getenv("SNOWFLAKE_PRIVATE_KEY")); key != "" {
		cfg["auth"] = authKeyPair
		return plugin.ConnectConfig{
			Config: cfg,
			Net:    plugintest.DirectTransport(),
			Credentials: plugin.NewResolvedCredentials(plugin.CredentialBinding{
				Field: keyPairField,
				Credential: plugin.ResolvedCredential{
					Kind:   credentialKindKeyPair,
					Values: map[string]string{"username": text(cfg["username"]), "private_key": key},
				},
			}),
		}
	}
	cfg["auth"] = authPassword
	cfg["password"] = requireEnv(t, "SNOWFLAKE_PASSWORD")
	return plugin.ConnectConfig{Config: cfg, Net: plugintest.DirectTransport()}
}

func requireEnv(t *testing.T, key string) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		t.Skipf("set %s to run the Snowflake integration test", key)
	}
	return value
}

func envDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func pageHasName(t *testing.T, out any, name string) bool {
	t.Helper()
	page, ok := out.(plugin.Page[row])
	if !ok {
		t.Fatalf("expected a page, got %T", out)
	}
	for _, item := range page.Items {
		if text(item["name"]) == name {
			return true
		}
	}
	return false
}
