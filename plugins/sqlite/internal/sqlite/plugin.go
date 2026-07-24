package sqlite

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

const sqliteSvgIcon = `<svg xmlns="http://www.w3.org/2000/svg" width="64" height="64" viewBox="0 0 6.554 6.555" preserveAspectRatio="xMidYMid"><defs><linearGradient x1="2.983" y1=".53" x2="2.983" y2="4.744" id="A" gradientUnits="userSpaceOnUse"><stop stop-color="#97d9f6" offset="0%"/><stop stop-color="#0f80cc" offset="92.024%"/><stop stop-color="#0f80cc" offset="100%"/></linearGradient></defs><path d="M4.96.29H.847c-.276 0-.5.226-.5.5v4.536c0 .276.226.5.5.5h2.71c-.03-1.348.43-3.964 1.404-5.54z" fill="#0f80cc"/><path d="M4.81.437H.847c-.196 0-.355.16-.355.355v4.205c.898-.345 2.245-.642 3.177-.628A28.93 28.93 0 0 1 4.811.437z" fill="url(#A)"/><path d="M5.92.142c-.282-.25-.623-.15-.96.148l-.15.146c-.576.61-1.1 1.742-1.276 2.607a2.38 2.38 0 0 1 .148.426l.022.1.022.102s-.005-.02-.026-.08l-.014-.04a.461.461 0 0 0-.009-.022c-.038-.087-.14-.272-.187-.352a8.789 8.789 0 0 0-.103.321c.132.242.212.656.212.656s-.007-.027-.04-.12c-.03-.083-.176-.34-.21-.4-.06.22-.083.368-.062.404.04.07.08.2.115.324a7.52 7.52 0 0 1 .132.666l.005.062a6.11 6.11 0 0 0 .015.75c.026.313.075.582.137.726l.042-.023c-.09-.284-.128-.655-.112-1.084.025-.655.175-1.445.454-2.268C4.548 1.938 5.2.94 5.798.464c-.545.492-1.282 2.084-1.502 2.673-.247.66-.422 1.28-.528 1.873.182-.556.77-.796.77-.796s.29-.356.626-.865l-.645.172-.208.092s.53-.323.987-.47c.627-.987 1.31-2.39.622-3.002" fill="#003b57"/></svg>`

type Plugin struct{}

func New() Plugin { return Plugin{} }

func (Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion:          plugin.CurrentAPIVersion,
		Name:                "sqlite",
		Version:             "0.1.0",
		Title:               "SQLite",
		Description:         "Open and query local SQLite database files entirely in the browser — the file is never uploaded.",
		Icon:                plugin.Icon{Type: plugin.IconSVG, Value: sqliteSvgIcon},
		Category:            plugin.CategoryDatabases,
		Layout:              plugin.LayoutSingle,
		SupportedTransports: []plugin.Transport{plugin.TransportDirect},
		Tabs: []plugin.Panel{{
			Key:   "explorer",
			Label: "SQLite Explorer",
			Icon:  plugin.Icon{Type: plugin.IconLucide, Value: "database"},
			Type:  plugin.PanelWasm,
			Config: plugin.WasmConfig{
				Entry:     "app.js",
				Runtime:   plugin.WasmRuntimeGeneric,
				ScaleMode: plugin.WasmScaleResize,
				Boot:      plugin.WasmBoot{Scripts: []string{"sql-wasm.js", "app.js"}},
				Assets: []plugin.WasmAsset{
					asset("sql-wasm.js", "text/javascript"),
					asset("sql-wasm.wasm", "application/wasm"),
					asset("app.js", "text/javascript"),
				},
				Capabilities: plugin.WasmCapabilities{Keyboard: true, Pointer: true, Fullscreen: true},
				AriaLabel:    "SQLite database explorer",
				Instructions: "Open a local .sqlite/.db file (or create a new one) and run SQL. Everything runs in your browser; the file is never uploaded.",
			},
		}},
	}
}

func (Plugin) Routes() []plugin.Route {
	return []plugin.Route{{
		ID: "sqlite.asset", Method: plugin.MethodGet, Path: "/asset",
		Permission: "sqlite.read", Risk: plugin.RiskSafe, AuditEvent: "sqlite.asset",
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
		Source: plugin.DataSource{RouteID: "sqlite.asset", Params: map[string]string{"path": name}},
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
	case ".js":
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
