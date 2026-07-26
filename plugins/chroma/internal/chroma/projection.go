package chroma

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugin/canvas"
)

const (
	projectionClusters  = 6
	projectionNeighbors = 6
	plotPadLeft         = 56.0
	plotPadRight        = 24.0
	plotPadTop          = 68.0
	plotPadBottom       = 132.0
)

func projectionStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, sc, col, err := collectionOf(rc)
	if err != nil {
		return err
	}
	data, err := loadSample(rc.Ctx, s, sc, col)
	if err != nil {
		return err
	}
	return runCanvas(rc, client, newProjectionScene(data))
}

type projectionScene struct {
	data      sample
	proj      projection
	clusters  []int
	order     []int
	width     float64
	height    float64
	theme     plugin.PanelTheme
	selected  int
	hovered   int
	zoom      float64
	panX      float64
	panY      float64
	dragging  bool
	dragX     float64
	dragY     float64
	announce  string
	minX      float64
	maxX      float64
	minY      float64
	maxY      float64
	neighbors []scored
}

func newProjectionScene(data sample) *projectionScene {
	s := &projectionScene{
		data:     data,
		width:    960,
		height:   540,
		selected: -1,
		hovered:  -1,
		zoom:     1,
	}
	if data.len() > 0 {
		s.proj = project2D(data.Vectors)
		s.clusters = cluster(data.Vectors, data.Collection.space(), min(projectionClusters, data.len()))
		s.bounds()
		s.order = make([]int, data.len())
		for i := range s.order {
			s.order[i] = i
		}
		sort.SliceStable(s.order, func(a, b int) bool {
			return s.proj.Points[s.order[a]].X < s.proj.Points[s.order[b]].X
		})
		s.selected = 0
		s.refreshNeighbors()
	}
	return s
}

func (s *projectionScene) bounds() {
	s.minX, s.maxX = math.MaxFloat64, -math.MaxFloat64
	s.minY, s.maxY = math.MaxFloat64, -math.MaxFloat64
	for _, p := range s.proj.Points {
		s.minX, s.maxX = math.Min(s.minX, p.X), math.Max(s.maxX, p.X)
		s.minY, s.maxY = math.Min(s.minY, p.Y), math.Max(s.maxY, p.Y)
	}
	if s.maxX-s.minX < 1e-9 {
		s.minX, s.maxX = s.minX-1, s.maxX+1
	}
	if s.maxY-s.minY < 1e-9 {
		s.minY, s.maxY = s.minY-1, s.maxY+1
	}
}

func (s *projectionScene) refreshNeighbors() {
	s.neighbors = nil
	if s.selected < 0 || s.selected >= s.data.len() {
		return
	}
	s.neighbors = rank(s.data.Collection.space(), s.data.Vectors, s.data.Vectors[s.selected], s.selected, projectionNeighbors)
}

func (s *projectionScene) Handle(ev canvas.Event) bool {
	switch e := ev.(type) {
	case *canvas.ReadyEvent:
		s.theme, s.width, s.height = e.Theme, e.Width, e.Height
		return true
	case *canvas.ResizeEvent:
		s.theme, s.width, s.height = e.Theme, e.Width, e.Height
		return true
	case *canvas.PointerEvent:
		return s.pointer(e)
	case *canvas.WheelEvent:
		if e.DeltaY == 0 {
			return false
		}
		s.zoom = clampFloat(s.zoom*math.Exp(-e.DeltaY/600), 0.5, 12)
		return true
	case *canvas.KeyEvent:
		return s.key(e)
	}
	return false
}

func (s *projectionScene) pointer(e *canvas.PointerEvent) bool {
	index := regionIndex(e.RegionID, "pt:")
	switch e.Event {
	case "down":
		s.dragging = index < 0
		s.dragX, s.dragY = e.X, e.Y
		if index >= 0 {
			return s.selectPoint(index)
		}
		return false
	case "up", "cancel", "leave":
		s.dragging = false
		if e.Event == "leave" && s.hovered != -1 {
			s.hovered = -1
			return true
		}
		return false
	case "move":
		if s.dragging {
			s.panX += e.X - s.dragX
			s.panY += e.Y - s.dragY
			s.dragX, s.dragY = e.X, e.Y
			return true
		}
		if index != s.hovered {
			s.hovered = index
			return true
		}
	}
	return false
}

func (s *projectionScene) key(e *canvas.KeyEvent) bool {
	if e.Event != "down" {
		return false
	}
	switch e.Key {
	case "ArrowRight", "ArrowDown":
		return s.step(1)
	case "ArrowLeft", "ArrowUp":
		return s.step(-1)
	case "Home":
		return s.selectPoint(s.orderAt(0))
	case "End":
		return s.selectPoint(s.orderAt(len(s.order) - 1))
	case "+", "=":
		s.zoom = clampFloat(s.zoom*1.25, 0.5, 12)
		return true
	case "-", "_":
		s.zoom = clampFloat(s.zoom/1.25, 0.5, 12)
		return true
	case "0":
		s.zoom, s.panX, s.panY = 1, 0, 0
		return true
	case "Escape":
		if s.selected < 0 {
			return false
		}
		s.selected = -1
		s.neighbors = nil
		s.announce = "Selection cleared"
		return true
	}
	return false
}

func (s *projectionScene) orderAt(i int) int {
	if len(s.order) == 0 {
		return -1
	}
	return s.order[clampInt(i, 0, len(s.order)-1)]
}

func (s *projectionScene) step(delta int) bool {
	if len(s.order) == 0 {
		return false
	}
	position := 0
	for i, index := range s.order {
		if index == s.selected {
			position = i
			break
		}
	}
	return s.selectPoint(s.orderAt(position + delta))
}

func (s *projectionScene) selectPoint(index int) bool {
	if index < 0 || index >= s.data.len() || index == s.selected {
		return false
	}
	s.selected = index
	s.refreshNeighbors()
	s.announce = fmt.Sprintf("Selected %s. %s", s.data.id(index), truncate(s.data.document(index), 90))
	return true
}

func regionIndex(id, prefix string) int {
	if !strings.HasPrefix(id, prefix) {
		return -1
	}
	value, err := strconv.Atoi(strings.TrimPrefix(id, prefix))
	if err != nil {
		return -1
	}
	return value
}

func (s *projectionScene) plot() (x0, y0, w, h float64) {
	w = math.Max(s.width-plotPadLeft-plotPadRight, 80)
	h = math.Max(s.height-plotPadTop-plotPadBottom, 80)
	return plotPadLeft, plotPadTop, w, h
}

func (s *projectionScene) screen(p point2D) (float64, float64) {
	x0, y0, w, h := s.plot()
	nx := (p.X - s.minX) / (s.maxX - s.minX)
	ny := (p.Y - s.minY) / (s.maxY - s.minY)
	cx := x0 + w/2 + (nx-0.5)*w*s.zoom + s.panX
	cy := y0 + h/2 - (ny-0.5)*h*s.zoom + s.panY
	return cx, cy
}

func (s *projectionScene) Frame() canvas.Frame {
	c := paletteFor(s.theme)
	commands := []canvas.Command{canvas.Clear{Color: c.Background}}
	commands = append(commands, s.header(c)...)
	if s.data.len() == 0 {
		commands = append(commands, canvas.TextBox{
			Paint: canvas.Paint{Fill: c.Muted, Font: fontBody},
			X:     plotPadLeft, Y: s.height/2 - 24, Width: math.Max(s.width-2*plotPadLeft, 120), Height: 48,
			Text: "This collection has no stored embeddings yet. Add records with vectors to see the map.",
		})
		return canvas.Frame{Commands: commands}
	}

	x0, y0, w, h := s.plot()
	commands = append(commands,
		canvas.Rect{Paint: canvas.Paint{Fill: c.Surface, Stroke: c.Border, LineWidth: 1}, X: x0 - 8, Y: y0 - 8, Width: w + 16, Height: h + 16, Radius: 10},
	)
	for i := 1; i < 4; i++ {
		gx := x0 + w*float64(i)/4
		gy := y0 + h*float64(i)/4
		commands = append(commands,
			canvas.Line{Paint: canvas.Paint{Stroke: c.Grid, LineWidth: 1}, X1: gx, Y1: y0, X2: gx, Y2: y0 + h},
			canvas.Line{Paint: canvas.Paint{Stroke: c.Grid, LineWidth: 1}, X1: x0, Y1: gy, X2: x0 + w, Y2: gy},
		)
	}

	if s.selected >= 0 {
		sx, sy := s.screen(s.proj.Points[s.selected])
		for _, neighbor := range s.neighbors {
			nx, ny := s.screen(s.proj.Points[neighbor.Index])
			commands = append(commands, canvas.Line{
				Paint: canvas.Paint{Stroke: c.Accent, LineWidth: 1.5, Alpha: alpha(0.45)},
				X1:    sx, Y1: sy, X2: nx, Y2: ny,
			})
		}
	}

	// Every point the sample carries stays selectable, and the selection is drawn
	// even when panned out of view, so FocusRegion always resolves.
	regions := make([]canvas.Region, 0, s.data.len())
	radius := clampFloat(6-float64(s.data.len())/400, 2.2, 6)
	for i := range s.proj.Points {
		px, py := s.screen(s.proj.Points[i])
		if i != s.selected && (px < x0-20 || px > x0+w+20 || py < y0-20 || py > y0+h+20) {
			continue
		}
		fill := c.series(s.clusterOf(i))
		size := radius
		if i == s.hovered {
			size = radius + 2
		}
		commands = append(commands, canvas.Circle{
			Paint: canvas.Paint{Fill: fill, Alpha: alpha(0.85)}, X: px, Y: py, Radius: size,
		})
		regions = append(regions, canvas.CircleRegion("pt:"+strconv.Itoa(i), px, py, math.Max(size+3, 7),
			canvas.WithCursor("pointer"), canvas.WithLabel(s.data.id(i))))
	}
	if s.selected >= 0 {
		sx, sy := s.screen(s.proj.Points[s.selected])
		commands = append(commands,
			canvas.Circle{Paint: canvas.Paint{Stroke: c.Text, LineWidth: 2, NoFill: true}, X: sx, Y: sy, Radius: radius + 6},
			canvas.Circle{Paint: canvas.Paint{Fill: c.Text}, X: sx, Y: sy, Radius: 2.5},
		)
	}

	commands = append(commands, s.legend(c, x0, y0+h+18, w)...)
	commands = append(commands, s.details(c)...)
	// Focus follows the selection only when it changes, so hovering never steals
	// the screen reader's place.
	if s.announce != "" {
		if s.selected >= 0 {
			commands = append(commands, canvas.FocusRegion{ID: "pt:" + strconv.Itoa(s.selected)})
		}
		commands = append(commands, canvas.Announce{Text: s.announce, Mode: canvas.AnnouncePolite})
		s.announce = ""
	}
	return canvas.Frame{Commands: commands, Regions: regions}
}

func (s *projectionScene) clusterOf(i int) int {
	if i < len(s.clusters) {
		return s.clusters[i]
	}
	return 0
}

func (s *projectionScene) header(c palette) []canvas.Command {
	col := s.data.Collection
	subtitle := fmt.Sprintf("%d vectors sampled · %s space", s.data.len(), spaceLabel(col.space()))
	if s.proj.Fallback {
		subtitle += " · raw axes (variance too low for PCA)"
	} else if s.proj.Total > 0 {
		subtitle += fmt.Sprintf(" · PC1 %.0f%% / PC2 %.0f%% variance", s.proj.Explained(0)*100, s.proj.Explained(1)*100)
	}
	return []canvas.Command{
		canvas.Text{Paint: canvas.Paint{Fill: c.Text, Font: fontTitle}, X: 24, Y: 30, Text: col.Name + " — embedding map"},
		canvas.Text{Paint: canvas.Paint{Fill: c.Muted, Font: fontBody}, X: 24, Y: 50, Text: subtitle},
		canvas.Text{Paint: canvas.Paint{Fill: c.Muted, Font: fontBody, TextAlign: canvas.TextAlignRight}, X: math.Max(s.width-24, 200), Y: 50,
			Text: fmt.Sprintf("zoom %.1fx · arrows select · +/- zoom · 0 reset", s.zoom)},
	}
}

func (s *projectionScene) legend(c palette, x, y, w float64) []canvas.Command {
	out := []canvas.Command{canvas.Text{Paint: canvas.Paint{Fill: c.Muted, Font: fontLabel}, X: x, Y: y, Text: "clusters"}}
	step := math.Min(84, math.Max(w-70, 40)/float64(projectionClusters))
	for i := 0; i < min(projectionClusters, s.data.len()); i++ {
		cx := x + 62 + float64(i)*step
		out = append(out,
			canvas.Circle{Paint: canvas.Paint{Fill: c.series(i)}, X: cx, Y: y - 4, Radius: 4},
			canvas.Text{Paint: canvas.Paint{Fill: c.Muted, Font: fontBody}, X: cx + 9, Y: y, Text: "c" + strconv.Itoa(i+1)},
		)
	}
	return out
}

func (s *projectionScene) details(c palette) []canvas.Command {
	index := s.hovered
	if index < 0 {
		index = s.selected
	}
	if index < 0 || index >= s.data.len() {
		return nil
	}
	lines := []string{"id  " + s.data.id(index)}
	if doc := s.data.document(index); doc != "" {
		lines = append(lines, "doc "+truncate(doc, 70))
	}
	if meta := s.data.metadata(index); len(meta) > 0 {
		lines = append(lines, "meta "+truncate(jsonText(meta), 70))
	}
	if index == s.selected && len(s.neighbors) > 0 {
		parts := make([]string, 0, len(s.neighbors))
		for _, neighbor := range s.neighbors {
			parts = append(parts, fmt.Sprintf("%s (%.4f)", truncate(s.data.id(neighbor.Index), 18), neighbor.Distance))
		}
		lines = append(lines, "near "+truncate(strings.Join(parts, ", "), 78))
	}
	height := 22 + float64(len(lines))*16
	y := math.Max(s.height-height-8, 8)
	return []canvas.Command{canvas.TextBox{
		Paint:      canvas.Paint{Fill: c.Text, Font: fontMono},
		Background: c.Panel,
		X:          24, Y: y, Width: math.Max(s.width-48, 160), Height: height,
		Padding: 10, LineHeight: 16, Radius: 8, MaxLines: len(lines),
		Text: strings.Join(lines, "\n"),
	}}
}

func alpha(v float64) *float64 { return &v }
