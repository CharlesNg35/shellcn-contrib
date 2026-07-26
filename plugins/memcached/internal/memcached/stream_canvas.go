package memcached

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugin/canvas"
)

const (
	slabCanvasWidth  = 960
	slabCanvasHeight = 620
	slabHeaderHeight = 96
	slabFooterHeight = 78
	slabRowHeight    = 34
	slabRowGap       = 6
	slabMargin       = 18
	slabLabelWidth   = 150
	slabStatsWidth   = 210
)

type slabPalette struct {
	Background string
	Surface    string
	SurfaceAlt string
	Text       string
	Muted      string
	Track      string
	Used       string
	Waste      string
	Accent     string
	Danger     string
}

func paletteFor(theme plugin.PanelTheme) slabPalette {
	if theme == plugin.PanelThemeLight {
		return slabPalette{
			Background: "#f8fafc", Surface: "#ffffff", SurfaceAlt: "#f1f5f9",
			Text: "#0f172a", Muted: "#64748b", Track: "#e2e8f0",
			Used: "#0284c7", Waste: "#d97706", Accent: "#7c3aed", Danger: "#dc2626",
		}
	}
	return slabPalette{
		Background: "#0b1120", Surface: "#111c33", SurfaceAlt: "#16233d",
		Text: "#e2e8f0", Muted: "#94a3b8", Track: "#1e293b",
		Used: "#38bdf8", Waste: "#fbbf24", Accent: "#c084fc", Danger: "#f87171",
	}
}

// slabScene renders the slab-class allocation map: one row per class showing
// allocated capacity, the used portion, and the slack between what items asked
// for and the chunk they were rounded up into.
type slabScene struct {
	width, height float64
	theme         plugin.PanelTheme
	rows          []slabRow
	err           string
	selected      int
	scroll        int
	announce      string
}

func newSlabScene() *slabScene {
	return &slabScene{width: slabCanvasWidth, height: slabCanvasHeight, theme: plugin.PanelThemeDark, selected: -1}
}

func (s *slabScene) setRows(rows []slabRow, err error) bool {
	message := ""
	if err != nil {
		message = err.Error()
	}
	if message == s.err && sameSlabRows(s.rows, rows) {
		return false
	}
	s.rows = rows
	s.err = message
	if s.selected >= len(rows) {
		s.selected = len(rows) - 1
	}
	if s.selected < 0 && len(rows) > 0 {
		s.selected = 0
	}
	s.clampScroll()
	return true
}

func sameSlabRows(a, b []slabRow) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (s *slabScene) visibleRows() int {
	usable := s.height - slabHeaderHeight - slabFooterHeight
	n := int(usable / (slabRowHeight + slabRowGap))
	if n < 1 {
		return 1
	}
	return n
}

func (s *slabScene) clampScroll() {
	maxScroll := len(s.rows) - s.visibleRows()
	if maxScroll < 0 {
		maxScroll = 0
	}
	if s.scroll > maxScroll {
		s.scroll = maxScroll
	}
	if s.scroll < 0 {
		s.scroll = 0
	}
	if s.selected >= 0 {
		if s.selected < s.scroll {
			s.scroll = s.selected
		}
		if s.selected >= s.scroll+s.visibleRows() {
			s.scroll = s.selected - s.visibleRows() + 1
		}
	}
}

func (s *slabScene) selectIndex(index int) bool {
	if len(s.rows) == 0 {
		return false
	}
	if index < 0 {
		index = 0
	}
	if index >= len(s.rows) {
		index = len(s.rows) - 1
	}
	if index == s.selected {
		return false
	}
	s.selected = index
	s.clampScroll()
	row := s.rows[index]
	s.announce = fmt.Sprintf("Slab class %d selected. Chunk size %s, %d of %d chunks used, %.0f percent slack.",
		row.Class, humanBytes(row.ChunkSize), row.UsedChunks, row.TotalChunks, row.WastedPct)
	return true
}

func (s *slabScene) handle(ev canvas.Event) bool {
	switch e := ev.(type) {
	case *canvas.ReadyEvent:
		s.theme, s.width, s.height = e.Theme, e.Width, e.Height
		s.clampScroll()
		return true
	case *canvas.ResizeEvent:
		s.theme, s.width, s.height = e.Theme, e.Width, e.Height
		s.clampScroll()
		return true
	case *canvas.PointerEvent:
		if e.Event != canvas.PointerDown || !strings.HasPrefix(e.RegionID, "class:") {
			return false
		}
		index, err := strconv.Atoi(strings.TrimPrefix(e.RegionID, "class:"))
		if err != nil {
			return false
		}
		return s.selectIndex(index)
	case *canvas.WheelEvent:
		before := s.scroll
		if e.DeltaY > 0 {
			s.scroll++
		} else if e.DeltaY < 0 {
			s.scroll--
		}
		s.clampScroll()
		return s.scroll != before
	case *canvas.KeyEvent:
		if e.Event != "keydown" {
			return false
		}
		switch e.Key {
		case "ArrowDown", "j":
			return s.selectIndex(s.selected + 1)
		case "ArrowUp", "k":
			return s.selectIndex(s.selected - 1)
		case "PageDown":
			return s.selectIndex(s.selected + s.visibleRows())
		case "PageUp":
			return s.selectIndex(s.selected - s.visibleRows())
		case "Home":
			return s.selectIndex(0)
		case "End":
			return s.selectIndex(len(s.rows) - 1)
		}
	}
	return false
}

func (s *slabScene) frame() canvas.Frame {
	p := paletteFor(s.theme)
	commands := []canvas.Command{canvas.Clear{Color: p.Background}}
	regions := make([]canvas.Region, 0, len(s.rows)+1)

	commands = append(commands, s.headerCommands(p)...)
	if s.err != "" {
		commands = append(commands, canvas.TextBox{
			Paint:      canvas.Paint{Fill: p.Danger, Font: "500 13px ui-sans-serif, system-ui, sans-serif"},
			X:          slabMargin,
			Y:          slabHeaderHeight,
			Width:      s.width - 2*slabMargin,
			Height:     48,
			Padding:    10,
			Background: p.Surface,
			Radius:     8,
			MaxLines:   2,
			Ellipsis:   "…",
			Text:       "Slab statistics unavailable: " + s.err,
		})
		return canvas.Frame{Commands: commands, Regions: regions}
	}
	if len(s.rows) == 0 {
		commands = append(commands, canvas.FillText{
			Paint: canvas.Paint{Fill: p.Muted, Font: "400 14px ui-sans-serif, system-ui, sans-serif"},
			X:     slabMargin, Y: slabHeaderHeight + 24,
			Text: "No slab classes are allocated yet. Store an item to make memcached carve its first page.",
		})
		return canvas.Frame{Commands: commands, Regions: regions}
	}

	maxAllocated := int64(1)
	for _, row := range s.rows {
		if row.AllocatedBytes > maxAllocated {
			maxAllocated = row.AllocatedBytes
		}
	}
	barX := float64(slabMargin + slabLabelWidth)
	barW := s.width - barX - slabStatsWidth - slabMargin
	if barW < 80 {
		barW = 80
	}

	end := min(s.scroll+s.visibleRows(), len(s.rows))
	y := float64(slabHeaderHeight)
	for i := s.scroll; i < end; i++ {
		row := s.rows[i]
		rowCmds, region := s.rowCommands(p, row, i, y, barX, barW, maxAllocated)
		commands = append(commands, rowCmds...)
		regions = append(regions, region)
		y += slabRowHeight + slabRowGap
	}
	commands = append(commands, s.footerCommands(p)...)
	if s.announce != "" {
		commands = append(commands, canvas.Announce{Text: s.announce, Mode: canvas.AnnouncePolite})
		s.announce = ""
	}
	if s.selected >= s.scroll && s.selected < end {
		commands = append(commands, canvas.FocusRegion{ID: "class:" + strconv.Itoa(s.selected)})
	}
	return canvas.Frame{Commands: commands, Regions: regions}
}

func (s *slabScene) headerCommands(p slabPalette) []canvas.Command {
	var allocated, used, requested, items int64
	for _, row := range s.rows {
		allocated += row.AllocatedBytes
		used += row.UsedBytes
		requested += row.MemRequested
		items += row.Items
	}
	waste := used - requested
	if waste < 0 || requested == 0 {
		waste = 0
	}
	summary := fmt.Sprintf("%d classes   %s allocated   %s in used chunks   %s item payload   %s slack   %d items",
		len(s.rows), humanBytes(allocated), humanBytes(used), humanBytes(requested), humanBytes(waste), items)

	return []canvas.Command{
		canvas.Rect{Paint: canvas.Paint{Fill: p.Surface}, X: 0, Y: 0, Width: s.width, Height: slabHeaderHeight - 12},
		canvas.FillText{
			Paint: canvas.Paint{Fill: p.Text, Font: "600 16px ui-sans-serif, system-ui, sans-serif"},
			X:     slabMargin, Y: 30, Text: "Slab class allocation",
		},
		canvas.FillText{
			Paint: canvas.Paint{Fill: p.Muted, Font: "400 12px ui-sans-serif, system-ui, sans-serif"},
			X:     slabMargin, Y: 52, Text: summary,
		},
		canvas.Rect{Paint: canvas.Paint{Fill: p.Used}, X: slabMargin, Y: 64, Width: 10, Height: 10, Radius: 2},
		canvas.FillText{Paint: canvas.Paint{Fill: p.Muted, Font: "400 11px ui-sans-serif, system-ui, sans-serif"}, X: slabMargin + 16, Y: 73, Text: "item payload"},
		canvas.Rect{Paint: canvas.Paint{Fill: p.Waste}, X: slabMargin + 110, Y: 64, Width: 10, Height: 10, Radius: 2},
		canvas.FillText{Paint: canvas.Paint{Fill: p.Muted, Font: "400 11px ui-sans-serif, system-ui, sans-serif"}, X: slabMargin + 126, Y: 73, Text: "chunk slack"},
		canvas.Rect{Paint: canvas.Paint{Fill: p.Track}, X: slabMargin + 220, Y: 64, Width: 10, Height: 10, Radius: 2},
		canvas.FillText{Paint: canvas.Paint{Fill: p.Muted, Font: "400 11px ui-sans-serif, system-ui, sans-serif"}, X: slabMargin + 236, Y: 73, Text: "free chunks"},
	}
}

func (s *slabScene) rowCommands(p slabPalette, row slabRow, index int, y, barX, barW float64, maxAllocated int64) ([]canvas.Command, canvas.Region) {
	selected := index == s.selected
	surface := p.Background
	if index%2 == 1 {
		surface = p.SurfaceAlt
	}
	if selected {
		surface = p.Surface
	}

	trackW := barW * float64(row.AllocatedBytes) / float64(maxAllocated)
	if trackW < 2 {
		trackW = 2
	}
	usedW, payloadW := 0.0, 0.0
	if row.AllocatedBytes > 0 {
		usedW = trackW * float64(row.UsedBytes) / float64(row.AllocatedBytes)
	}
	if row.UsedBytes > 0 && row.MemRequested > 0 {
		payloadW = usedW * float64(min(row.MemRequested, row.UsedBytes)) / float64(row.UsedBytes)
	} else {
		payloadW = usedW
	}

	commands := []canvas.Command{
		canvas.Rect{Paint: canvas.Paint{Fill: surface, Stroke: selectedStroke(selected, p), LineWidth: 1.5}, X: slabMargin - 6, Y: y, Width: s.width - 2*slabMargin + 12, Height: slabRowHeight, Radius: 6},
		canvas.FillText{
			Paint: canvas.Paint{Fill: p.Text, Font: "600 12px ui-sans-serif, system-ui, sans-serif"},
			X:     slabMargin, Y: y + 15, Text: "class " + strconv.Itoa(row.Class),
		},
		canvas.FillText{
			Paint: canvas.Paint{Fill: p.Muted, Font: "400 11px ui-sans-serif, system-ui, sans-serif"},
			X:     slabMargin, Y: y + 28, Text: humanBytes(row.ChunkSize) + " chunks · " + strconv.FormatInt(row.TotalPages, 10) + " pages",
		},
		canvas.Rect{Paint: canvas.Paint{Fill: p.Track}, X: barX, Y: y + 8, Width: trackW, Height: 18, Radius: 4},
	}
	if usedW > 0 {
		commands = append(commands,
			canvas.Rect{Paint: canvas.Paint{Fill: p.Waste}, X: barX, Y: y + 8, Width: usedW, Height: 18, Radii: &canvas.Radii{TopLeft: 4, BottomLeft: 4}},
			canvas.Rect{Paint: canvas.Paint{Fill: p.Used}, X: barX, Y: y + 8, Width: payloadW, Height: 18, Radii: &canvas.Radii{TopLeft: 4, BottomLeft: 4}},
		)
	}
	commands = append(commands, canvas.TextBox{
		Paint:         canvas.Paint{Fill: p.Muted, Font: "400 11px ui-sans-serif, system-ui, sans-serif", TextAlign: canvas.TextAlignRight},
		X:             s.width - slabMargin - slabStatsWidth,
		Y:             y + 4,
		Width:         slabStatsWidth,
		Height:        slabRowHeight - 8,
		LineHeight:    13,
		MaxLines:      2,
		VerticalAlign: canvas.TextVerticalAlignMiddle,
		Ellipsis:      "…",
		Text: fmt.Sprintf("%d/%d chunks · %.0f%% full\n%s slack · %d items · %d evicted",
			row.UsedChunks, row.TotalChunks, row.FillPct, humanBytes(row.WastedBytes), row.Items, row.Evicted),
	})

	region := canvas.RectRegion("class:"+strconv.Itoa(index), slabMargin-6, y, s.width-2*slabMargin+12, slabRowHeight,
		canvas.WithCursor("pointer"),
		canvas.WithLabel(fmt.Sprintf("Slab class %d, chunk size %s, %d of %d chunks used, %.0f percent slack",
			row.Class, humanBytes(row.ChunkSize), row.UsedChunks, row.TotalChunks, row.WastedPct)),
	)
	return commands, region
}

func (s *slabScene) footerCommands(p slabPalette) []canvas.Command {
	y := s.height - slabFooterHeight + 6
	commands := []canvas.Command{
		canvas.Rect{Paint: canvas.Paint{Fill: p.Surface}, X: 0, Y: y, Width: s.width, Height: slabFooterHeight - 6},
	}
	if s.selected < 0 || s.selected >= len(s.rows) {
		return commands
	}
	row := s.rows[s.selected]
	detail := fmt.Sprintf("class %d · chunk %s · %d chunks/page · %d pages · %s allocated · %s in used chunks · %s item payload",
		row.Class, humanBytes(row.ChunkSize), row.ChunksPerPage, row.TotalPages,
		humanBytes(row.AllocatedBytes), humanBytes(row.UsedBytes), humanBytes(row.MemRequested))
	activity := fmt.Sprintf("%d items · oldest %ds · %d get hits · %d sets · %d evicted · %d out-of-memory · %.1f%% slack",
		row.Items, row.Age, row.GetHits, row.CmdSet, row.Evicted, row.OutOfMemory, row.WastedPct)
	return append(commands,
		canvas.FillText{Paint: canvas.Paint{Fill: p.Accent, Font: "600 12px ui-sans-serif, system-ui, sans-serif"}, X: slabMargin, Y: y + 22, Text: detail},
		canvas.FillText{Paint: canvas.Paint{Fill: p.Muted, Font: "400 11px ui-sans-serif, system-ui, sans-serif"}, X: slabMargin, Y: y + 42, Text: activity},
		canvas.FillText{Paint: canvas.Paint{Fill: p.Muted, Font: "400 11px ui-sans-serif, system-ui, sans-serif"}, X: slabMargin, Y: y + 60,
			Text: "Up/Down or J/K select a class, PageUp/PageDown jump, Home/End go to the ends, Alt+wheel scrolls."},
	)
}

func selectedStroke(selected bool, p slabPalette) string {
	if selected {
		return p.Accent
	}
	return ""
}

// slabCanvasStream drives the allocation map: it polls slab statistics, redraws
// only when the scene changes, and consumes pointer/keyboard/wheel input on the
// same socket.
func slabCanvasStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := session(rc)
	if err != nil {
		return err
	}
	scene := newSlabScene()
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

	refresh := func() bool {
		ctx, cancel := context.WithTimeout(client.Context(), s.opts.Timeout)
		rows, err := s.readSlabs(ctx)
		cancel()
		return scene.setRows(rows, err)
	}
	refresh()

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	dirty := true
	for {
		if dirty {
			if err := canvas.WriteFrame(client, scene.frame()); err != nil {
				return nil
			}
			dirty = false
		}
		select {
		case <-client.Context().Done():
			return nil
		case <-rc.Ctx.Done():
			return nil
		case err := <-errc:
			if errors.Is(err, io.EOF) || client.Context().Err() != nil {
				return nil
			}
			return err
		case ev := <-events:
			dirty = scene.handle(ev)
		case <-ticker.C:
			dirty = refresh()
		}
	}
}
