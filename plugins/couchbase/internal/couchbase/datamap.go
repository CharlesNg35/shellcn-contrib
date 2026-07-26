package couchbase

import (
	"context"
	"errors"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugin/canvas"
)

// The data map is a bucket-scoped picture of scopes, collections, and their
// document counts. A table cannot show relative size at a glance and a graph has
// no meaningful edges here, so this is a drawn surface with keyboard navigation,
// hit regions, and screen-reader announcements.

type mapCollection struct {
	Scope    string
	Name     string
	Items    float64
	MemUsed  float64
	DataSize float64
}

type mapScope struct {
	Name        string
	System      bool
	Collections []mapCollection
	Items       float64
}

type dataMap struct {
	Bucket    string
	Scopes    []mapScope
	Items     float64
	MaxItems  float64
	Collected time.Time
}

func (s *Session) dataMap(ctx context.Context, bucket string) (dataMap, error) {
	manifest, err := s.scopes(ctx, bucket)
	if err != nil {
		return dataMap{}, err
	}
	stats := s.collectionStats(ctx, bucket)
	out := dataMap{Bucket: bucket, Collected: time.Now()}
	for _, scope := range manifest.Scopes {
		item := mapScope{Name: scope.Name, System: strings.HasPrefix(scope.Name, "_")}
		for _, collection := range scope.Collections {
			stat := stats[scope.Name+"/"+collection.Name]
			item.Collections = append(item.Collections, mapCollection{
				Scope:    scope.Name,
				Name:     collection.Name,
				Items:    stat.Items,
				MemUsed:  stat.MemUsed,
				DataSize: stat.DataSize,
			})
			item.Items += stat.Items
			if stat.Items > out.MaxItems {
				out.MaxItems = stat.Items
			}
		}
		sort.Slice(item.Collections, func(i, j int) bool {
			if item.Collections[i].Items != item.Collections[j].Items {
				return item.Collections[i].Items > item.Collections[j].Items
			}
			return item.Collections[i].Name < item.Collections[j].Name
		})
		out.Items += item.Items
		out.Scopes = append(out.Scopes, item)
	}
	sort.Slice(out.Scopes, func(i, j int) bool {
		if out.Scopes[i].System != out.Scopes[j].System {
			return !out.Scopes[i].System
		}
		return out.Scopes[i].Name < out.Scopes[j].Name
	})
	return out, nil
}

type mapPalette struct {
	Background string
	Surface    string
	SurfaceAlt string
	Border     string
	Text       string
	Muted      string
	Accent     string
	AccentSoft string
	Selected   string
}

func palette(theme plugin.PanelTheme) mapPalette {
	if theme == plugin.PanelThemeLight {
		return mapPalette{
			Background: "#f8fafc", Surface: "#ffffff", SurfaceAlt: "#eef2f7", Border: "#cbd5e1",
			Text: "#0f172a", Muted: "#475569", Accent: "#b3151b", AccentSoft: "#f3c7c9", Selected: "#0f766e",
		}
	}
	return mapPalette{
		Background: "#020617", Surface: "#0f172a", SurfaceAlt: "#1e293b", Border: "#334155",
		Text: "#e2e8f0", Muted: "#94a3b8", Accent: "#ef4444", AccentSoft: "#7f1d1d", Selected: "#2dd4bf",
	}
}

type mapScene struct {
	width    float64
	theme    plugin.PanelTheme
	data     dataMap
	err      string
	selected int
	targets  []mapCollection
	regions  []canvas.Region
	announce string
}

const (
	mapPadding    = 20.0
	mapCardWidth  = 320.0
	mapCardGap    = 16.0
	mapRowHeight  = 26.0
	mapCardHeader = 46.0
)

func (s *mapScene) colors() mapPalette { return palette(s.theme) }

func (s *mapScene) setData(data dataMap) {
	s.data = data
	s.err = ""
	s.targets = s.targets[:0]
	for _, scope := range data.Scopes {
		s.targets = append(s.targets, scope.Collections...)
	}
	if s.selected >= len(s.targets) {
		s.selected = len(s.targets) - 1
	}
	if s.selected < 0 {
		s.selected = 0
	}
}

func (s *mapScene) handle(ev canvas.Event) bool {
	switch e := ev.(type) {
	case *canvas.ReadyEvent:
		s.width, s.theme = e.Width, e.Theme
		return true
	case *canvas.ResizeEvent:
		s.width, s.theme = e.Width, e.Theme
		return true
	case *canvas.PointerEvent:
		if e.Event != canvas.PointerDown || e.RegionID == "" {
			return false
		}
		for i, target := range s.targets {
			if regionID(target) == e.RegionID {
				s.selected = i
				s.announce = describe(target)
				return true
			}
		}
		return false
	case *canvas.KeyEvent:
		if e.Event != "keydown" || len(s.targets) == 0 {
			return false
		}
		switch e.Key {
		case "ArrowDown", "ArrowRight":
			s.selected = (s.selected + 1) % len(s.targets)
		case "ArrowUp", "ArrowLeft":
			s.selected = (s.selected - 1 + len(s.targets)) % len(s.targets)
		case "Home":
			s.selected = 0
		case "End":
			s.selected = len(s.targets) - 1
		default:
			return false
		}
		s.announce = describe(s.targets[s.selected])
		return true
	}
	return false
}

func regionID(c mapCollection) string { return "collection:" + c.Scope + "/" + c.Name }

func describe(c mapCollection) string {
	return c.Scope + "." + c.Name + ": " + formatCount(c.Items) + " documents, " + formatBytes(c.MemUsed) + " resident"
}

func formatCount(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

func formatBytes(v float64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	i := 0
	for v >= 1024 && i < len(units)-1 {
		v /= 1024
		i++
	}
	return strconv.FormatFloat(round1(v), 'f', -1, 64) + " " + units[i]
}

func (s *mapScene) frame() canvas.Frame {
	c := s.colors()
	width := s.width
	if width < 360 {
		width = 360
	}
	commands := []canvas.Command{canvas.Clear{Color: c.Background}}
	s.regions = s.regions[:0]

	title := "Data map"
	if s.data.Bucket != "" {
		title = s.data.Bucket + " · data map"
	}
	commands = append(commands,
		canvas.FillText{
			Paint: canvas.Paint{Fill: c.Text, Font: "600 18px Inter, system-ui, sans-serif", TextBaseline: canvas.TextBaselineMiddle},
			X:     mapPadding, Y: mapPadding + 10, Text: title,
		},
		canvas.FillText{
			Paint: canvas.Paint{Fill: c.Muted, Font: "13px Inter, system-ui, sans-serif", TextBaseline: canvas.TextBaselineMiddle},
			X:     mapPadding, Y: mapPadding + 32,
			Text: s.summary(),
		},
	)

	if s.err != "" {
		commands = append(commands, canvas.FillText{
			Paint: canvas.Paint{Fill: c.Accent, Font: "13px Inter, system-ui, sans-serif"},
			X:     mapPadding, Y: mapPadding + 70, Text: s.err,
		})
		return canvas.Frame{Commands: commands}
	}
	if len(s.data.Scopes) == 0 {
		commands = append(commands, canvas.FillText{
			Paint: canvas.Paint{Fill: c.Muted, Font: "13px Inter, system-ui, sans-serif"},
			X:     mapPadding, Y: mapPadding + 70, Text: "No scopes in this bucket yet.",
		})
		return canvas.Frame{Commands: commands}
	}

	columns := int((width - 2*mapPadding + mapCardGap) / (mapCardWidth + mapCardGap))
	if columns < 1 {
		columns = 1
	}
	cardWidth := (width - 2*mapPadding - float64(columns-1)*mapCardGap) / float64(columns)
	x, y := mapPadding, mapPadding+56
	rowHeight := 0.0
	index := 0
	for i, scope := range s.data.Scopes {
		height := mapCardHeader + float64(len(scope.Collections))*mapRowHeight + 12
		if i%columns == 0 && i > 0 {
			x = mapPadding
			y += rowHeight + mapCardGap
			rowHeight = 0
		}
		if height > rowHeight {
			rowHeight = height
		}
		cmds, regions, used := s.scopeCard(scope, x, y, cardWidth, index)
		commands = append(commands, cmds...)
		s.regions = append(s.regions, regions...)
		index += used
		x += cardWidth + mapCardGap
	}
	if s.announce != "" {
		commands = append(commands, canvas.Announce{Text: s.announce, Mode: canvas.AnnouncePolite})
		s.announce = ""
	}
	if s.selected >= 0 && s.selected < len(s.targets) {
		commands = append(commands, canvas.FocusRegion{ID: regionID(s.targets[s.selected])})
	}
	return canvas.Frame{Commands: commands, Regions: s.regions}
}

func (s *mapScene) summary() string {
	collections := 0
	for _, scope := range s.data.Scopes {
		collections += len(scope.Collections)
	}
	parts := []string{
		strconv.Itoa(len(s.data.Scopes)) + " scopes",
		strconv.Itoa(collections) + " collections",
		formatCount(s.data.Items) + " documents",
	}
	if !s.data.Collected.IsZero() {
		parts = append(parts, "updated "+s.data.Collected.UTC().Format("15:04:05")+" UTC")
	}
	return strings.Join(parts, " · ")
}

func (s *mapScene) scopeCard(scope mapScope, x, y, width float64, firstIndex int) ([]canvas.Command, []canvas.Region, int) {
	c := s.colors()
	height := mapCardHeader + float64(len(scope.Collections))*mapRowHeight + 12
	commands := []canvas.Command{
		canvas.Rect{
			Paint: canvas.Paint{Fill: c.Surface, Stroke: c.Border, LineWidth: 1},
			X:     x, Y: y, Width: width, Height: height,
			Radii: &canvas.Radii{TopLeft: 10, TopRight: 10, BottomRight: 10, BottomLeft: 10},
		},
		canvas.FillText{
			Paint: canvas.Paint{Fill: c.Text, Font: "600 14px Inter, system-ui, sans-serif", TextBaseline: canvas.TextBaselineMiddle},
			X:     x + 14, Y: y + 20, Text: scope.Name,
		},
		canvas.FillText{
			Paint: canvas.Paint{Fill: c.Muted, Font: "12px Inter, system-ui, sans-serif", TextBaseline: canvas.TextBaselineMiddle},
			X:     x + 14, Y: y + 38,
			Text: strconv.Itoa(len(scope.Collections)) + " collections · " + formatCount(scope.Items) + " docs",
		},
	}
	regions := make([]canvas.Region, 0, len(scope.Collections))
	barLeft := x + 14
	barWidth := width - 28
	for i, collection := range scope.Collections {
		top := y + mapCardHeader + float64(i)*mapRowHeight
		share := 0.0
		if s.data.MaxItems > 0 {
			share = collection.Items / s.data.MaxItems
		}
		fill := c.AccentSoft
		label := c.Text
		if firstIndex+i == s.selected {
			fill = c.Selected
			label = c.Text
		}
		commands = append(commands,
			canvas.Rect{
				Paint: canvas.Paint{Fill: c.SurfaceAlt},
				X:     barLeft, Y: top, Width: barWidth, Height: mapRowHeight - 6,
				Radii: &canvas.Radii{TopLeft: 5, TopRight: 5, BottomRight: 5, BottomLeft: 5},
			},
			canvas.Rect{
				Paint: canvas.Paint{Fill: fill},
				X:     barLeft, Y: top, Width: maxFloat(barWidth*share, 3), Height: mapRowHeight - 6,
				Radii: &canvas.Radii{TopLeft: 5, TopRight: 5, BottomRight: 5, BottomLeft: 5},
			},
			canvas.TextBox{
				Paint: canvas.Paint{Fill: label, Font: "12px Inter, system-ui, sans-serif"},
				X:     barLeft + 8, Y: top, Width: barWidth - 90, Height: mapRowHeight - 6,
				VerticalAlign: canvas.TextVerticalAlignMiddle, MaxLines: 1, Ellipsis: "…",
				Text: collection.Name,
			},
			canvas.FillText{
				Paint: canvas.Paint{Fill: c.Muted, Font: "12px Inter, system-ui, sans-serif", TextAlign: canvas.TextAlignRight, TextBaseline: canvas.TextBaselineMiddle},
				X:     barLeft + barWidth - 8, Y: top + (mapRowHeight-6)/2,
				Text: formatCount(collection.Items),
			},
		)
		regions = append(regions, canvas.RectRegion(
			regionID(collection), barLeft, top, barWidth, mapRowHeight-6,
			canvas.WithCursor("pointer"),
			canvas.WithLabel(describe(collection)),
		))
	}
	return commands, regions, len(scope.Collections)
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func dataMapStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := session(rc)
	if err != nil {
		return err
	}
	bucket, err := bucketName(rc, s)
	if err != nil {
		return err
	}
	scene := &mapScene{width: 1200, theme: plugin.PanelThemeDark}
	refresh := func() {
		data, err := s.dataMap(rc.Ctx, bucket)
		if err != nil {
			scene.err = err.Error()
			return
		}
		scene.setData(data)
	}
	refresh()

	events := make(chan canvas.Event, 32)
	errc := make(chan error, 1)
	go func() {
		for {
			ev, err := canvas.DecodeEvent(client)
			if err != nil {
				errc <- err
				return
			}
			select {
			case events <- ev:
			case <-client.Context().Done():
				return
			}
		}
	}()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	dirty := true
	for {
		if dirty {
			if err := canvas.WriteFrame(client, scene.frame()); err != nil {
				return err
			}
			dirty = false
		}
		select {
		case <-client.Context().Done():
			return nil
		case <-rc.Ctx.Done():
			return nil
		case err := <-errc:
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		case ev := <-events:
			dirty = scene.handle(ev)
		case <-ticker.C:
			refresh()
			dirty = true
		}
	}
}
