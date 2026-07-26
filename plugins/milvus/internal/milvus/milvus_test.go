package milvus

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/client/v2/entity"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugin/canvas"
	"github.com/charlesng35/shellcn/sdk/plugintest"
)

func TestManifestValidates(t *testing.T) {
	p := New()
	plugintest.ValidatePlugin(t, p)

	proj := plugintest.Projection(t, p)
	plugintest.ValidateProjectionPanelConfigs(t, proj)

	cfg := p.Manifest().Config
	for _, kind := range []plugin.CredentialKind{plugin.CredentialKindDBPassword, plugin.CredentialKindAPIToken, plugin.CredentialKindTLSClientCert} {
		if !plugintest.CredentialKindSupported(cfg, kind) {
			t.Fatalf("Milvus should accept stored %s credentials", kind)
		}
	}
}

// Every declared action must be reachable: an action nobody references is a
// route the operator can never run.
func TestActionsAreReachable(t *testing.T) {
	manifest := New().Manifest()
	referenced := map[string]bool{}
	collect := func(ids ...string) {
		for _, id := range ids {
			referenced[id] = true
		}
	}
	for _, resource := range manifest.Resources {
		collect(resource.Actions.Toolbar...)
		collect(resource.Actions.Row...)
		collect(resource.Actions.Detail...)
		for _, panel := range resource.Detail.Tabs {
			for _, p := range append([]plugin.Panel{panel}, nestedPanels(panel)...) {
				if table, ok := p.Config.(plugin.TableConfig); ok {
					collect(table.ActionIDs...)
					collect(table.RowActionIDs...)
				}
			}
		}
	}
	for _, action := range manifest.Actions {
		if !referenced[action.ID] {
			t.Fatalf("action %q is declared but no resource or panel exposes it", action.ID)
		}
	}
}

func nestedPanels(panel plugin.Panel) []plugin.Panel {
	dashboard, ok := panel.Config.(plugin.DashboardConfig)
	if !ok {
		return nil
	}
	return dashboard.Cells
}

func TestRouteNamingContract(t *testing.T) {
	for _, route := range New().Routes() {
		if !strings.HasPrefix(route.ID, protocolName+".") {
			t.Fatalf("route %q is not namespaced", route.ID)
		}
		if route.AuditEvent != route.ID {
			t.Fatalf("route %q audit event %q must equal the route id", route.ID, route.AuditEvent)
		}
		if !strings.HasPrefix(route.Permission, protocolName+".") {
			t.Fatalf("route %q permission %q is not namespaced", route.ID, route.Permission)
		}
		switch route.Method {
		case plugin.MethodGet:
			if route.Risk != plugin.RiskSafe {
				t.Fatalf("read route %q must be safe", route.ID)
			}
		case plugin.MethodDelete:
			if route.Risk != plugin.RiskDestructive {
				t.Fatalf("delete route %q must be destructive", route.ID)
			}
		}
	}
}

func TestConfigSchemaVisibility(t *testing.T) {
	schema := configSchema()

	values := schema.ValuesWithDefaults(map[string]any{"host": "milvus.internal"})
	if values["port"] != defaultPort {
		t.Fatalf("expected default port, got %v", values["port"])
	}
	if err := schema.ValidateValues(values, nil); err != nil {
		t.Fatalf("defaults must validate: %v", err)
	}

	visible := schema.VisibleValues(map[string]any{"host": "h", "auth": authPassword, "username": "root", "password": "secret", "token": "leak"}, nil)
	if _, ok := visible["token"]; ok {
		t.Fatal("token must be hidden when password auth is selected")
	}
	secrets := schema.VisibleSecretKeys(map[string]any{"host": "h", "auth": authPassword}, nil)
	if !slices.Contains(secrets, "password") {
		t.Fatalf("password must be a visible secret for password auth, got %v", secrets)
	}
	if slices.Contains(secrets, "token") {
		t.Fatalf("token must not be a visible secret for password auth, got %v", secrets)
	}
	if schema.HasFileField() {
		t.Fatal("the Milvus connection form takes no file uploads")
	}

	if err := schema.ValidateValues(map[string]any{"host": "h", "auth": authToken}, nil); err == nil {
		t.Fatal("token auth must require a token")
	}
	if err := schema.ValidateValues(map[string]any{"host": "h", "database": "1bad"}, nil); err == nil {
		t.Fatal("invalid database identifiers must be rejected")
	}
}

func TestParseOptionsAuthModes(t *testing.T) {
	opts, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{
		"host": "milvus.internal", "port": 19531, "auth": authPassword,
		"username": "root", "password": "Milvus", "read_only": false,
		"timeout": "3s", "page_limit": 25, "sample_limit": 64,
	}})
	if err != nil {
		t.Fatalf("parseOptions: %v", err)
	}
	if opts.Username != "root" || opts.Password != "Milvus" || opts.Port != 19531 {
		t.Fatalf("unexpected options: %#v", opts)
	}
	if opts.Timeout != 3*time.Second || opts.PageLimit != 25 || opts.SampleLimit != 64 || opts.ReadOnly {
		t.Fatalf("unexpected safety options: %#v", opts)
	}

	if _, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{"host": "h", "auth": "kerberos"}}); err == nil {
		t.Fatal("unknown auth modes must be rejected")
	}
	if _, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{"auth": authNone}}); err == nil {
		t.Fatal("direct connections must require a host")
	}
	agent, err := parseOptions(plugin.ConnectConfig{Transport: plugin.TransportAgent, Config: map[string]any{"auth": authNone}})
	if err != nil {
		t.Fatalf("agent transport should default the host: %v", err)
	}
	if agent.Host != "127.0.0.1" || agent.Port != defaultPort {
		t.Fatalf("unexpected agent options: %#v", agent)
	}
}

func TestParseOptionsTLS(t *testing.T) {
	opts, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{
		"host": "milvus.internal", "auth": authNone, "tls_mode": "verify-full",
	}})
	if err != nil {
		t.Fatalf("parseOptions: %v", err)
	}
	if opts.TLSConfig == nil || opts.TLSConfig.ServerName != "milvus.internal" {
		t.Fatalf("verify-full must pin the server name: %#v", opts.TLSConfig)
	}
	if _, err := parseOptions(plugin.ConnectConfig{Config: map[string]any{
		"host": "h", "auth": authNone, "tls_mode": "verify-ca", "ca_certificate": "not-a-pem",
	}}); err == nil {
		t.Fatal("a malformed CA bundle must be rejected")
	}
}

func TestFieldRequestBuild(t *testing.T) {
	f, err := fieldRequest{Name: "embedding", Type: "FloatVector", Dim: 8}.build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if dim, err := f.GetDim(); err != nil || dim != 8 {
		t.Fatalf("unexpected dim %v %v", dim, err)
	}

	pk, err := fieldRequest{Name: "id", Type: "Int64", Primary: true, AutoID: true, Nullable: true}.build()
	if err != nil {
		t.Fatalf("build primary: %v", err)
	}
	if !pk.PrimaryKey || !pk.AutoID || pk.Nullable {
		t.Fatalf("a primary key must never be nullable: %#v", pk)
	}

	if _, err := (fieldRequest{Name: "id", Type: "Float", Primary: true}).build(); err == nil {
		t.Fatal("only Int64 and VarChar primary keys are valid")
	}
	if _, err := (fieldRequest{Name: "v", Type: "FloatVector"}).build(); err == nil {
		t.Fatal("vector fields must declare a dimension")
	}
	if _, err := (fieldRequest{Name: "bad name", Type: "Int64"}).build(); err == nil {
		t.Fatal("invalid identifiers must be rejected")
	}

	text, err := fieldRequest{Name: "title", Type: "VarChar"}.build()
	if err != nil {
		t.Fatalf("build varchar: %v", err)
	}
	if text.TypeParams[entity.TypeParamMaxLength] == "" {
		t.Fatal("VarChar fields must get a max length")
	}
}

func TestBuildColumnsCoercesJSONValues(t *testing.T) {
	schema := entity.NewSchema().
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("title").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64)).
		WithField(entity.NewField().WithName("score").WithDataType(entity.FieldTypeDouble)).
		WithField(entity.NewField().WithName("meta").WithDataType(entity.FieldTypeJSON)).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(3))

	rows := []map[string]any{{
		"id": float64(7), "title": "Ada", "score": float64(0.5),
		"meta": map[string]any{"tag": "x"}, "vector": []any{0.1, 0.2, 0.3},
	}}
	columns, err := buildColumns(schema, rows, false)
	if err != nil {
		t.Fatalf("buildColumns: %v", err)
	}
	if len(columns) != 5 {
		t.Fatalf("expected 5 columns, got %d", len(columns))
	}
	byName := map[string]int{}
	for i, col := range columns {
		byName[col.Name()] = i
	}
	id, err := columns[byName["id"]].GetAsInt64(0)
	if err != nil || id != 7 {
		t.Fatalf("unexpected id %v %v", id, err)
	}
	if _, ok := byName["vector"]; !ok {
		t.Fatalf("vector column missing: %v", byName)
	}

	if _, err := buildColumns(schema, []map[string]any{{"id": float64(1), "title": "a", "score": float64(1), "meta": map[string]any{}, "vector": []any{0.1}}}, false); err == nil {
		t.Fatal("a wrong-length vector must be rejected")
	}
	if _, err := buildColumns(schema, []map[string]any{{"title": "a"}}, false); err == nil {
		t.Fatal("missing non-nullable fields must be rejected")
	}
}

func TestMutationRowsAcceptsGridAndEditorBodies(t *testing.T) {
	bodies := [][]byte{
		[]byte(`{"id":1,"title":"a"}`),
		[]byte(`{"rows":[{"id":1},{"id":2}]}`),
		[]byte(`[{"id":1}]`),
		[]byte(`{"content":"{\"id\":1}"}`),
		[]byte(`{"key":{"_id":1}}`),
	}
	for _, body := range bodies {
		rows, err := mutationRows(plugin.NewRequestContext(context.Background(), plugin.User{}, nil, nil, nil, body))
		if err != nil {
			t.Fatalf("mutationRows(%s): %v", body, err)
		}
		if len(rows) == 0 {
			t.Fatalf("mutationRows(%s) returned nothing", body)
		}
	}
	if _, err := mutationRows(plugin.NewRequestContext(context.Background(), plugin.User{}, nil, nil, nil, []byte(`"nope"`))); err == nil {
		t.Fatal("scalar bodies must be rejected")
	}
}

func TestIdentifierRejectsInjection(t *testing.T) {
	for _, bad := range []string{"", "a b", "drop;", "1abc", strings.Repeat("a", 300), "коллекция"} {
		if _, err := identifier("collection", bad); err == nil {
			t.Fatalf("identifier(%q) should fail", bad)
		}
	}
	if got, err := identifier("collection", " docs "); err != nil || got != "docs" {
		t.Fatalf("identifier trimmed value: %q %v", got, err)
	}
}

func TestBuildIndexVariants(t *testing.T) {
	hnsw, err := buildIndex("HNSW", "L2", 0, 0, 0)
	if err != nil {
		t.Fatalf("buildIndex: %v", err)
	}
	if hnsw.Params()["M"] == "" && hnsw.Params()["m"] == "" {
		t.Fatalf("HNSW must carry its M parameter: %v", hnsw.Params())
	}
	if _, err := buildIndex("MAGIC", "L2", 0, 0, 0); err == nil {
		t.Fatal("unknown index types must be rejected")
	}
	auto, err := buildIndex("AUTOINDEX", "", 0, 0, 0)
	if err != nil {
		t.Fatalf("buildIndex auto: %v", err)
	}
	if auto.Params()["metric_type"] != "COSINE" {
		t.Fatalf("AUTOINDEX should default to COSINE: %v", auto.Params())
	}
}

func TestTableResultShape(t *testing.T) {
	rows := []row{{"id": int64(1), "score": 0.9, "title": "Ada"}}
	result := tableResult("search", rows, searchColumns(false, "id", rows), time.Now())
	columns, _ := result["columns"].([]string)
	if len(columns) < 3 || columns[0] != "id" || columns[1] != "score" {
		t.Fatalf("unexpected column order: %v", columns)
	}
	values, _ := result["rows"].([][]any)
	if len(values) != 1 || values[0][0] != int64(1) {
		t.Fatalf("unexpected rows: %v", values)
	}
	if result["rowCount"] != 1 {
		t.Fatalf("unexpected row count: %v", result["rowCount"])
	}
}

func TestParseQueryRequestValidatesIdentifiers(t *testing.T) {
	req, err := parseQueryRequest(`{"field":"vector","partitions":["p1"],"limit":5}`)
	if err != nil {
		t.Fatalf("parseQueryRequest: %v", err)
	}
	if req.Field != "vector" || len(req.Partitions) != 1 {
		t.Fatalf("unexpected request: %#v", req)
	}
	for _, bad := range []string{"", "not json", `{"field":"a;drop"}`, `{"partitions":["p 1"]}`, `{"group_by":"a b"}`} {
		if _, err := parseQueryRequest(bad); err == nil {
			t.Fatalf("parseQueryRequest(%q) should fail", bad)
		}
	}
}

func TestQueryLimitAndFullScanGuard(t *testing.T) {
	if got := queryLimit(queryRequest{}, 100); got != 10 {
		t.Fatalf("default limit = %d", got)
	}
	if got := queryLimit(queryRequest{Limit: 5000}, 100); got != 100 {
		t.Fatalf("limit must clamp to the page limit, got %d", got)
	}
	if !isFullScan(queryRequest{Limit: 5000}, 1000) {
		t.Fatal("an unfiltered query above the scan threshold is a full scan")
	}
	for _, req := range []queryRequest{
		{Filter: "id > 0"},
		{Vector: []float64{0.1}},
		{Text: "hello"},
		{IDs: []any{1}},
	} {
		if isFullScan(req, 1000) {
			t.Fatalf("%#v is not a full scan", req)
		}
	}
}

func TestQueryStreamReportsErrorsWithoutClosing(t *testing.T) {
	routes := plugintest.RouteMap(New().Routes())
	stream := &recordStream{ctx: context.Background(), input: strings.NewReader(
		"{\"query\":\"\"}\n{\"query\":\"not json\"}\n")}
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, nil, map[string]string{"collection": "docs"}, nil, nil)
	if err := routes[rid("query")].Stream(rc, stream); err != nil {
		t.Fatalf("query stream: %v", err)
	}
	frames := strings.Split(strings.TrimSpace(stream.out.String()), "\n")
	if len(frames) != 2 {
		t.Fatalf("expected an error frame per request, got %v", frames)
	}
	for _, frame := range frames {
		var decoded map[string]any
		if err := json.Unmarshal([]byte(frame), &decoded); err != nil {
			t.Fatalf("frame is not JSON: %v", err)
		}
		if decoded["error"] == nil {
			t.Fatalf("expected an error frame, got %s", frame)
		}
	}
}

func TestQueryStreamRejectsBadCollection(t *testing.T) {
	routes := plugintest.RouteMap(New().Routes())
	stream := &recordStream{ctx: context.Background(), input: strings.NewReader("{}\n")}
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, nil, map[string]string{"collection": "bad name"}, nil, nil)
	if err := routes[rid("query")].Stream(rc, stream); err == nil {
		t.Fatal("an invalid collection must fail the stream before any execution")
	}
}

func TestSearchVectors(t *testing.T) {
	vectors, err := searchVectors(queryRequest{Vector: []float64{0.1, 0.2}})
	if err != nil || len(vectors) != 1 {
		t.Fatalf("unexpected vectors: %v %v", vectors, err)
	}
	if _, ok := vectors[0].(entity.FloatVector); !ok {
		t.Fatalf("expected a float vector, got %T", vectors[0])
	}
	text, err := searchVectors(queryRequest{Text: "hello"})
	if err != nil {
		t.Fatalf("searchVectors text: %v", err)
	}
	if _, ok := text[0].(entity.Text); !ok {
		t.Fatalf("expected a text vector, got %T", text[0])
	}
	if _, err := searchVectors(queryRequest{Vectors: [][]float64{{}}}); err == nil {
		t.Fatal("empty vectors must be rejected")
	}
}

func TestMapError(t *testing.T) {
	cases := map[string]error{
		"collection not found":          plugin.ErrNotFound,
		"database already exist":        plugin.ErrConflict,
		"permission denied for user":    plugin.ErrForbidden,
		"collection not loaded":         plugin.ErrConflict,
		"unauthenticated: bad password": plugin.ErrUnauthorized,
		"connection refused":            plugin.ErrUnavailable,
	}
	for text, want := range cases {
		if got := mapError(errString(text)); !isSentinel(got, want) {
			t.Fatalf("mapError(%q) = %v, want %v", text, got, want)
		}
	}
	if mapError(nil) != nil {
		t.Fatal("mapError(nil) must stay nil")
	}
}

func TestActivityLogRing(t *testing.T) {
	log := newActivityLog(2)
	log.record("collection.create", "a", "created", nil)
	log.record("collection.drop", "b", "dropped", errString("boom"))
	log.record("collection.load", "c", "loaded", nil)
	entries := log.list()
	if len(entries) != 2 {
		t.Fatalf("expected the ring to keep 2 entries, got %d", len(entries))
	}
	if entries[0]["operation"] != "collection.load" {
		t.Fatalf("newest entry must come first: %v", entries[0]["operation"])
	}
	if entries[1]["severity"] != "danger" {
		t.Fatalf("failed operations must be flagged: %v", entries[1])
	}
}

func TestProjectionSceneProjectsAndSelects(t *testing.T) {
	scene := newTestScene(120, 16)
	scene.computeProjection()
	scene.project()

	if scene.variance[0] <= 0 {
		t.Fatalf("PCA must report explained variance: %v", scene.variance)
	}
	for _, p := range scene.points {
		if math.Abs(p.x) > 1.0001 || math.Abs(p.y) > 1.0001 {
			t.Fatalf("projected coordinates must be normalised: %v %v", p.x, p.y)
		}
	}

	// A lasso around the whole plot selects every visible point.
	left, top, right, bottom := scene.plotBounds()
	scene.lasso = []canvas.Point{{X: left, Y: top}, {X: right, Y: top}, {X: right, Y: bottom}, {X: left, Y: bottom}}
	scene.applyLasso()
	if len(scene.selected) == 0 {
		t.Fatal("lasso selection must select points")
	}
	if !strings.Contains(scene.summary(), "selected") {
		t.Fatalf("summary must report the selection: %q", scene.summary())
	}

	dirty, _ := scene.handle(&canvas.KeyEvent{Type: canvas.EventKey, Event: "keydown", Key: "c"})
	if !dirty || len(scene.selected) != 0 {
		t.Fatal("pressing c must clear the selection")
	}

	before := scene.method
	if _, _ = scene.handle(&canvas.KeyEvent{Type: canvas.EventKey, Event: "keydown", Key: "p"}); scene.method == before {
		t.Fatal("pressing p must toggle the projection method")
	}
	if _, resample := scene.handle(&canvas.KeyEvent{Type: canvas.EventKey, Event: "keydown", Key: "r"}); !resample {
		t.Fatal("pressing r must request a resample")
	}
}

// Query and search results carry entity.FloatVector, not []float32; a type
// switch that only knows the raw slice silently drops every sampled vector.
func TestAsFloat32SliceAcceptsResultVectors(t *testing.T) {
	cases := []any{
		entity.FloatVector{0.5, 1.5},
		[]float32{0.5, 1.5},
		[]any{0.5, 1.5},
		[]float64{0.5, 1.5},
		"[0.5, 1.5]",
	}
	for _, value := range cases {
		got, err := asFloat32Slice(value)
		if err != nil {
			t.Fatalf("asFloat32Slice(%T): %v", value, err)
		}
		if len(got) != 2 || got[0] != 0.5 || got[1] != 1.5 {
			t.Fatalf("asFloat32Slice(%T) = %v", value, got)
		}
	}
	if _, err := asFloat32Slice(42); err == nil {
		t.Fatal("a scalar is not a vector")
	}
}

func TestProjectionCyclesVectorFields(t *testing.T) {
	scene := newTestScene(10, 4)
	scene.fields = []string{"dense", "image"}
	scene.field = "dense"

	if _, resample := scene.handle(&canvas.KeyEvent{Type: canvas.EventKey, Event: "keydown", Key: "f"}); !resample {
		t.Fatal("f must resample the next vector field")
	}
	if scene.field != "image" {
		t.Fatalf("expected the next field, got %q", scene.field)
	}
	scene.handle(&canvas.PointerEvent{Type: canvas.EventPointer, Event: canvas.PointerDown, RegionID: "action:field"})
	if scene.field != "dense" {
		t.Fatalf("the field button must wrap around, got %q", scene.field)
	}
	if len(scene.buttons()) != 5 {
		t.Fatalf("a multi-vector collection gets the field button: %d", len(scene.buttons()))
	}

	single := newTestScene(4, 4)
	single.fields = []string{"vector"}
	if _, resample := single.handle(&canvas.KeyEvent{Type: canvas.EventKey, Event: "keydown", Key: "f"}); resample {
		t.Fatal("a single vector field has nothing to cycle")
	}
	if len(single.buttons()) != 4 {
		t.Fatalf("no field button for one vector field: %d", len(single.buttons()))
	}
}

func TestProjectionClearedLassoSurvivesDrag(t *testing.T) {
	scene := newTestScene(30, 6)
	scene.computeProjection()
	scene.project()

	scene.handle(&canvas.PointerEvent{Type: canvas.EventPointer, Event: canvas.PointerDown, X: 100, Y: 100})
	scene.handle(&canvas.KeyEvent{Type: canvas.EventKey, Event: "keydown", Key: "c"})
	// A pointer move after the selection was cleared mid-drag must not read a
	// lasso point that no longer exists.
	scene.handle(&canvas.PointerEvent{Type: canvas.EventPointer, Event: canvas.PointerMove, X: 140, Y: 160})
	if scene.dragging {
		t.Fatal("clearing the selection must end the drag")
	}
	scene.handle(&canvas.PointerEvent{Type: canvas.EventPointer, Event: canvas.PointerUp, X: 140, Y: 160})
}

func TestAnnFieldPrefersSparseForText(t *testing.T) {
	schema := entity.NewSchema().
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("sparse").WithDataType(entity.FieldTypeSparseVector)).
		WithField(entity.NewField().WithName("dense").WithDataType(entity.FieldTypeFloatVector).WithDim(4))

	if got := annField(schema, true); got != "sparse" {
		t.Fatalf("a text search must target the sparse field, got %q", got)
	}
	if got := annField(schema, false); got != "dense" {
		t.Fatalf("a vector search must target the dense field, got %q", got)
	}

	denseOnly := entity.NewSchema().
		WithField(entity.NewField().WithName("dense").WithDataType(entity.FieldTypeFloatVector).WithDim(4))
	if got := annField(denseOnly, true); got != "dense" {
		t.Fatalf("without a sparse field a text search falls back to the dense one, got %q", got)
	}
	if got := annField(entity.NewSchema(), false); got != "" {
		t.Fatalf("a schema without vectors has no ANN field, got %q", got)
	}
}

func TestSegmentStateName(t *testing.T) {
	cases := map[commonpb.SegmentState]string{
		commonpb.SegmentState_Flushed:          "flushed",
		commonpb.SegmentState_Growing:          "growing",
		commonpb.SegmentState_SegmentStateNone: "none",
		commonpb.SegmentState_NotExist:         "notexist",
	}
	for state, want := range cases {
		got := segmentStateName(state)
		if got != want {
			t.Fatalf("segmentStateName(%v) = %q, want %q", state, got, want)
		}
		if segmentSeverities[got] == "" {
			t.Fatalf("segment state %q has no badge severity", got)
		}
	}
}

func TestProjectionSceneFrameIsThemedAndAccessible(t *testing.T) {
	scene := newTestScene(40, 8)
	scene.computeProjection()

	for _, theme := range []plugin.PanelTheme{plugin.PanelThemeLight, plugin.PanelThemeDark} {
		dirty, _ := scene.handle(&canvas.ResizeEvent{Type: canvas.EventResize, Width: 1024, Height: 700, Theme: theme})
		if !dirty {
			t.Fatal("a resize must mark the scene dirty")
		}
		frame := scene.frame()
		data, err := json.Marshal(frame)
		if err != nil {
			t.Fatalf("frame must marshal: %v", err)
		}
		if !strings.Contains(string(data), scene.colors().background) {
			t.Fatalf("frame must clear with the %s background", theme)
		}
		if len(frame.Regions) != 4 {
			t.Fatalf("expected the four toolbar regions, got %d", len(frame.Regions))
		}
		for _, region := range frame.Regions {
			if region.Label == "" || region.ID == "" {
				t.Fatalf("canvas regions must be labelled: %#v", region)
			}
		}
	}
}

func TestProjectionButtonRegionsDriveActions(t *testing.T) {
	scene := newTestScene(20, 4)
	scene.computeProjection()
	scene.project()
	scene.selected = map[int]bool{0: true}

	if _, resample := scene.handle(&canvas.PointerEvent{Type: canvas.EventPointer, Event: canvas.PointerDown, RegionID: "action:resample"}); !resample {
		t.Fatal("the resample button must request a resample")
	}
	if _, _ = scene.handle(&canvas.PointerEvent{Type: canvas.EventPointer, Event: canvas.PointerDown, RegionID: "action:clear"}); len(scene.selected) != 0 {
		t.Fatal("the clear button must drop the selection")
	}
	scene.zoom = 4
	if _, _ = scene.handle(&canvas.PointerEvent{Type: canvas.EventPointer, Event: canvas.PointerDown, RegionID: "action:reset"}); scene.zoom != 1 {
		t.Fatal("the reset button must restore the default zoom")
	}
}

func TestQueryCompletionTemplatesAreRunnable(t *testing.T) {
	schema := entity.NewSchema().
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(4))
	byLabel := map[string]queryRequest{}
	for _, tpl := range queryTemplates(schema) {
		apply, _ := tpl["apply"].(string)
		var req queryRequest
		if err := json.Unmarshal([]byte(apply), &req); err != nil {
			t.Fatalf("template %v is not valid JSON: %v", tpl["label"], err)
		}
		byLabel[fmt.Sprint(tpl["label"])] = req
	}
	// A search vector of any other length is rejected by the segment core, so
	// the template must follow the field's own dimension.
	if got := len(byLabel["ANN search"].Vector); got != 4 {
		t.Fatalf("the ANN template must carry a 4-dimensional vector, got %d", got)
	}
	if got := byLabel["Scalar filter"].Filter; got != "id >= 0" {
		t.Fatalf("unexpected scalar filter template %q", got)
	}
	if _, ok := byLabel["Full-text search"]; ok {
		t.Fatal("a schema without a sparse field must not offer a full-text template")
	}

	varchar := entity.NewSchema().
		WithField(entity.NewField().WithName("key").WithDataType(entity.FieldTypeVarChar).WithIsPrimaryKey(true).WithMaxLength(64)).
		WithField(entity.NewField().WithName("sparse").WithDataType(entity.FieldTypeSparseVector))
	labels := map[string]string{}
	for _, tpl := range queryTemplates(varchar) {
		labels[fmt.Sprint(tpl["label"])] = fmt.Sprint(tpl["apply"])
	}
	if _, ok := labels["ANN search"]; ok {
		t.Fatal("a schema without a float vector field must not offer an ANN template")
	}
	if got := labels["Scalar filter"]; got != `{"filter":"key != \"\"","limit":25,"output":["*"]}` {
		t.Fatalf("a VarChar primary key needs a string filter, got %s", got)
	}
	if got := labels["Primary key lookup"]; !strings.Contains(got, `"ids":["id-1"`) {
		t.Fatalf("a VarChar primary key needs string ids, got %s", got)
	}
	if _, ok := labels["Full-text search"]; !ok {
		t.Fatal("a sparse field must offer a full-text template")
	}
}

func TestGridColumnMapping(t *testing.T) {
	cases := []struct {
		field  *entity.Field
		want   plugin.ColumnType
		editor plugin.ColumnEditor
	}{
		{entity.NewField().WithName("v").WithDataType(entity.FieldTypeFloatVector).WithDim(2), plugin.ColumnJSON, plugin.ColumnEditorJSON},
		{entity.NewField().WithName("b").WithDataType(entity.FieldTypeBool), plugin.ColumnBool, plugin.ColumnEditorToggle},
		{entity.NewField().WithName("n").WithDataType(entity.FieldTypeInt64), plugin.ColumnNumber, plugin.ColumnEditorNumber},
		{entity.NewField().WithName("s").WithDataType(entity.FieldTypeVarChar), plugin.ColumnText, plugin.ColumnEditorText},
	}
	for _, tc := range cases {
		if got := gridColumnType(tc.field); got != tc.want {
			t.Fatalf("gridColumnType(%s) = %v, want %v", tc.field.Name, got, tc.want)
		}
		if got := gridColumnEditor(tc.field); got != tc.editor {
			t.Fatalf("gridColumnEditor(%s) = %v, want %v", tc.field.Name, got, tc.editor)
		}
	}
}

func TestSavedQueryStorageRoundTrip(t *testing.T) {
	store := &fakeStorage{}
	routes := plugintest.RouteMap(New().Routes())

	body := []byte(`{"name":"Nearest docs","collection":"docs","query":"{\"vector\":[0.1],\"limit\":5}"}`)
	saveCtx := plugin.NewRequestContext(context.Background(), plugin.User{}, nil, nil, nil, body).WithStorage(store)
	saved, err := routes[rid("query.save")].Handle(saveCtx)
	if err != nil {
		t.Fatalf("query.save: %v", err)
	}
	id, _ := saved.(row)["id"].(string)
	if !strings.HasPrefix(id, savedQueryPrefix) {
		t.Fatalf("unexpected saved query id %q", id)
	}

	listCtx := plugin.NewRequestContext(context.Background(), plugin.User{}, nil, nil, nil, nil).WithStorage(store)
	page, err := routes[rid("queries.list")].Handle(listCtx)
	if err != nil {
		t.Fatalf("queries.list: %v", err)
	}
	items := page.(plugin.Page[row]).Items
	if len(items) != 1 || items[0]["name"] != "Nearest docs" {
		t.Fatalf("unexpected saved queries: %#v", items)
	}

	delCtx := plugin.NewRequestContext(context.Background(), plugin.User{}, nil, map[string]string{"id": id}, nil, nil).WithStorage(store)
	if _, err := routes[rid("query.delete")].Handle(delCtx); err != nil {
		t.Fatalf("query.delete: %v", err)
	}
	if len(store.items) != 0 {
		t.Fatalf("saved query was not removed: %#v", store.items)
	}

	badCtx := plugin.NewRequestContext(context.Background(), plugin.User{}, nil, map[string]string{"id": "../escape"}, nil, nil).WithStorage(store)
	if _, err := routes[rid("query.delete")].Handle(badCtx); err == nil {
		t.Fatal("keys outside the saved-query namespace must be rejected")
	}
}

func TestReadOnlyBlocksMutations(t *testing.T) {
	sess := &Session{opts: Options{ReadOnly: true, Database: defaultDatabase, Timeout: time.Second}, activity: newActivityLog(4)}
	rc := plugin.NewRequestContext(context.Background(), plugin.User{}, sess, map[string]string{"collection": "docs"}, nil, []byte(`{"name":"x"}`))
	if _, _, err := writableClient(rc); err == nil {
		t.Fatal("read-only sessions must refuse writes")
	}
}

// A read-only connection must not render mutation affordances the session will
// refuse, so the Data grid swaps to a variant without the write routes.
func TestEntityGridDropsMutationsWhenReadOnly(t *testing.T) {
	var data plugin.Panel
	for _, resource := range New().Manifest().Resources {
		if resource.Kind != "collection" {
			continue
		}
		for _, tab := range resource.Detail.Tabs {
			if tab.Key == "data" {
				data = tab
			}
		}
	}
	writable, ok := data.Config.(plugin.TableConfig)
	if !ok || !writable.Editable || writable.Insert == nil || writable.Update == nil || writable.Delete == nil {
		t.Fatalf("writable connections must keep the editable entity grid: %#v", data.Config)
	}
	if len(data.Variants) != 1 {
		t.Fatalf("the Data tab must declare one read-only variant, got %#v", data.Variants)
	}
	variant := data.Variants[0]
	want := plugin.Rule{Field: "read_only", Op: plugin.OpEq, Value: true}
	if variant.VisibleWhen == nil || len(variant.VisibleWhen.AllOf) != 1 || variant.VisibleWhen.AllOf[0] != want {
		t.Fatalf("the read-only variant must be gated on %#v, got %#v", want, variant.VisibleWhen)
	}
	readOnly, ok := variant.Config.(plugin.TableConfig)
	if !ok {
		t.Fatalf("the read-only variant must configure a table: %#v", variant.Config)
	}
	if readOnly.Editable || readOnly.Insert != nil || readOnly.Update != nil || readOnly.Delete != nil {
		t.Fatalf("the read-only variant must expose no mutation routes: %#v", readOnly)
	}
	if readOnly.ColumnsSource == nil || !readOnly.Exportable {
		t.Fatalf("the read-only variant must still browse and export entities: %#v", readOnly)
	}
}

func newTestScene(count, dims int) *projectionScene {
	points := make([]samplePoint, 0, count)
	for i := 0; i < count; i++ {
		vector := make([]float32, dims)
		for j := range vector {
			vector[j] = float32((i%7)*(j+1)) + float32(j)*0.25
		}
		points = append(points, samplePoint{
			id:      strings.Repeat("0", 3) + string(rune('a'+i%26)),
			payload: map[string]any{"bucket": i % 3},
			vector:  vector,
		})
	}
	return &projectionScene{
		collection: "docs", field: "vector", method: projectionPCA, seed: 42,
		width: 1024, height: 700, theme: plugin.PanelThemeDark,
		zoom: 1, hover: -1, selected: map[int]bool{}, points: points, dims: dims,
	}
}

type errString string

func (e errString) Error() string { return string(e) }

func isSentinel(err, sentinel error) bool {
	return err != nil && strings.Contains(err.Error(), strings.TrimSuffix(sentinel.Error(), ":"))
}

type fakeStorage struct {
	items map[string]plugin.StorageItem
}

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
	if filter == nil {
		filter = &plugin.StorageListFilter{}
	}
	out := make([]plugin.StorageItem, 0, len(s.items))
	for full, item := range s.items {
		if !strings.HasPrefix(full, scope.Collection+"/") {
			continue
		}
		if filter.KeyPrefix != "" && !strings.HasPrefix(item.Key, filter.KeyPrefix) {
			continue
		}
		if filter.ContentType != "" && item.ContentType != filter.ContentType {
			continue
		}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

type recordStream struct {
	ctx   context.Context
	input *strings.Reader
	out   strings.Builder
}

func (s *recordStream) Read(p []byte) (int, error)  { return s.input.Read(p) }
func (s *recordStream) Write(p []byte) (int, error) { return s.out.Write(p) }
func (s *recordStream) Close() error                { return nil }
func (s *recordStream) Context() context.Context    { return s.ctx }
