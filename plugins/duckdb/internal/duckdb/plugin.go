package duckdb

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

//go:embed assets/*
var assets embed.FS

const duckdbSvgIcon = `<svg id="Ebene_1" xmlns="http://www.w3.org/2000/svg" version="1.1" viewBox="0 0 500 500"><defs><style>.st0{fill:#1d1d1b}.st1{fill:#fff100}</style></defs><path class="st1" d="M249.9996737,500C111.9320267,500,0,388.068425,0,250.0000251,0,111.9318259,111.3637372,0,249.9996737,0,388.6367145,0,500,111.9318259,500,250.0000251c0,138.0683999-111.9317506,249.9999749-250.0003263,249.9999749Z"/><g><path class="st0" d="M190.0545045,146.5907724c-56.8184727,0-103.4089829,46.5908427-103.4089829,103.4092276,0,57.3868501,46.5905102,103.4092401,103.4089829,103.4092401,56.8183974,0,103.4091523-46.5908552,103.4091523-103.4092401s-46.5907549-103.4092401-103.4091523-103.4092276Z"/><path class="st0" d="M376.1380597,212.7835876h-49.1467777v74.432875h49.1467777c20.5540155,0,37.2164375-16.6623467,37.2164375-37.2164375v-.0000753c0-20.5540155-16.662422-37.2163622-37.2164375-37.2163622Z"/></g></svg>`

type Plugin struct{}

func New() Plugin { return Plugin{} }

func (Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:          plugin.CurrentAPIVersion,
		Name:                "duckdb",
		Version:             "0.1.0",
		Title:               "DuckDB",
		Description:         "Query local Parquet/CSV/JSON files with DuckDB, entirely in the browser. Files are never uploaded.",
		Icon:                plugin.Icon{Type: plugin.IconSVG, Value: duckdbSvgIcon},
		Category:            plugin.CategoryDatabases,
		Layout:              plugin.LayoutSingle,
		SupportedTransports: []plugin.Transport{plugin.TransportDirect},
		Tabs: []plugin.Panel{{
			Key:   "explorer",
			Label: "DuckDB Explorer",
			Icon:  plugin.Icon{Type: plugin.IconLucide, Value: "database-zap"},
			Type:  plugin.PanelWasm,
			Config: plugin.WasmConfig{
				Entry:     "app.js",
				Runtime:   plugin.WasmRuntimeGeneric,
				ScaleMode: plugin.WasmScaleResize,
				Boot:      plugin.WasmBoot{Scripts: []string{"app.js"}},
				Assets: []plugin.WasmAsset{
					asset("app.js", "text/javascript"),
					asset("duckdb.mjs", "text/javascript"),
					asset("duckdb-browser-eh.worker.js", "text/javascript"),
					asset("duckdb-eh.wasm", "application/wasm"),
				},
				Capabilities: plugin.WasmCapabilities{Keyboard: true, Pointer: true, Fullscreen: true},
				AriaLabel:    "DuckDB explorer",
				Instructions: "Open a local Parquet/CSV/JSON file and run SQL. Everything runs in your browser; files are never uploaded.",
			},
		}},
	}
}

func (Plugin) Routes() []plugin.Route {
	return []plugin.Route{{
		ID: "duckdb.asset", Method: plugin.MethodGet, Path: "/asset",
		Permission: "duckdb.read", Risk: plugin.RiskSafe, AuditEvent: "duckdb.asset",
		Handle: assetRoute,
	}}
}

func (Plugin) Connect(context.Context, plugin.ConnectConfig) (plugin.Session, error) {
	return session{}, nil
}

func asset(name, mime string) plugin.WasmAsset {
	return plugin.WasmAsset{
		Path:   name,
		MIME:   mime,
		Source: plugin.DataSource{RouteID: "duckdb.asset", Params: map[string]string{"path": name}},
	}
}

func assetRoute(rc *plugin.RequestContext) (any, error) {
	name := path.Clean(strings.TrimPrefix(rc.Param("path"), "/"))
	if name == "." || strings.Contains(name, "..") {
		return nil, plugin.ErrInvalidInput
	}
	data, err := assets.ReadFile("assets/" + name)
	if err != nil {
		return nil, fmt.Errorf("%w: asset not found", plugin.ErrNotFound)
	}
	return &plugin.Download{
		Name:    name,
		MIME:    assetMIME(name),
		Size:    int64(len(data)),
		Inline:  true,
		Body:    io.NopCloser(bytes.NewReader(data)),
		ModTime: time.Now(),
	}, nil
}

func assetMIME(name string) string {
	switch path.Ext(name) {
	case ".js", ".mjs":
		return "text/javascript"
	case ".wasm":
		return "application/wasm"
	case ".css":
		return "text/css"
	case ".json":
		return "application/json"
	default:
		return "application/octet-stream"
	}
}
