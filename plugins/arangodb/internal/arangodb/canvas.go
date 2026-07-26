package arangodb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugin/canvas"
)

const (
	topologyRefresh = 15 * time.Second
	nodeWidth       = 176.0
	nodeHeight      = 60.0
)

type topoNode struct {
	Name  string
	Count int64
	X     float64
	Y     float64
}

type topoEdge struct {
	Collection string
	From       string
	To         string
	Count      int64
}

// topologyScene draws a named graph's schema: vertex collections as boxes and
// edge definitions as directed connections, with live document counts. It is a
// map an operator navigates, so it declares regions, keyboard cycling and
// screen-reader announcements rather than being a decorative picture.
type topologyScene struct {
	title    string
	nodes    []topoNode
	edges    []topoEdge
	index    map[string]int
	width    float64
	height   float64
	theme    plugin.PanelTheme
	hover    string
	selected int
	announce string
	status   string
}

type topoPalette struct {
	background string
	surface    string
	surfaceAlt string
	border     string
	text       string
	muted      string
	accent     string
	edge       string
	edgeText   string
	highlight  string
}

func (s *topologyScene) palette() topoPalette {
	if s.theme == plugin.PanelThemeLight {
		return topoPalette{
			background: "#f8fafc", surface: "#ffffff", surfaceAlt: "#eef2ff", border: "#cbd5e1",
			text: "#0f172a", muted: "#475569", accent: "#0284c7", edge: "#94a3b8",
			edgeText: "#475569", highlight: "#0ea5e9",
		}
	}
	return topoPalette{
		background: "#020617", surface: "#0f172a", surfaceAlt: "#1e293b", border: "#334155",
		text: "#e2e8f0", muted: "#94a3b8", accent: "#38bdf8", edge: "#475569",
		edgeText: "#94a3b8", highlight: "#7dd3fc",
	}
}

func (s *topologyScene) load(nodes []topoNode, edges []topoEdge, status string) {
	s.nodes = nodes
	s.edges = edges
	s.status = status
	s.index = make(map[string]int, len(nodes))
	for i, node := range nodes {
		s.index[node.Name] = i
	}
	if s.selected >= len(nodes) {
		s.selected = 0
	}
	s.layout()
}

// layout places vertex collections on an ellipse so every edge definition stays
// visible without an external layout engine.
func (s *topologyScene) layout() {
	count := len(s.nodes)
	if count == 0 || s.width <= 0 || s.height <= 0 {
		return
	}
	cx, cy := s.width/2, s.height/2+16
	rx := math.Max(s.width/2-nodeWidth/2-24, nodeWidth/2)
	ry := math.Max(s.height/2-nodeHeight/2-56, nodeHeight)
	if count == 1 {
		s.nodes[0].X, s.nodes[0].Y = cx, cy
		return
	}
	for i := range s.nodes {
		angle := -math.Pi/2 + 2*math.Pi*float64(i)/float64(count)
		s.nodes[i].X = cx + rx*math.Cos(angle)
		s.nodes[i].Y = cy + ry*math.Sin(angle)
	}
}

func (s *topologyScene) Handle(ev canvas.Event) bool {
	switch e := ev.(type) {
	case *canvas.ReadyEvent:
		s.width, s.height, s.theme = e.Width, e.Height, e.Theme
		s.layout()
		s.announce = s.summary()
		return true
	case *canvas.ResizeEvent:
		s.width, s.height, s.theme = e.Width, e.Height, e.Theme
		s.layout()
		return true
	case *canvas.PointerEvent:
		return s.pointer(e)
	case *canvas.KeyEvent:
		return s.key(e)
	}
	return false
}

func (s *topologyScene) pointer(e *canvas.PointerEvent) bool {
	name := regionNode(e.RegionID)
	switch e.Event {
	case canvas.PointerMove:
		if name == s.hover {
			return false
		}
		s.hover = name
		return true
	case canvas.PointerDown:
		if name == "" {
			return false
		}
		if i, ok := s.index[name]; ok {
			s.selected = i
			s.announce = s.describe(i)
			return true
		}
	}
	return false
}

func (s *topologyScene) key(e *canvas.KeyEvent) bool {
	if e.Event != "keydown" || len(s.nodes) == 0 {
		return false
	}
	switch e.Key {
	case "ArrowRight", "ArrowDown", "Tab":
		s.selected = (s.selected + 1) % len(s.nodes)
	case "ArrowLeft", "ArrowUp":
		s.selected = (s.selected - 1 + len(s.nodes)) % len(s.nodes)
	case "Home":
		s.selected = 0
	case "End":
		s.selected = len(s.nodes) - 1
	case "Enter", " ":
		s.announce = s.describe(s.selected)
		return true
	default:
		return false
	}
	s.announce = s.describe(s.selected)
	return true
}

func (s *topologyScene) summary() string {
	return fmt.Sprintf("%s topology: %d vertex collections, %d edge definitions. Use arrow keys to move between collections.",
		s.title, len(s.nodes), len(s.edges))
}

func (s *topologyScene) describe(i int) string {
	if i < 0 || i >= len(s.nodes) {
		return s.summary()
	}
	node := s.nodes[i]
	in, out := 0, 0
	for _, edge := range s.edges {
		if edge.To == node.Name {
			in++
		}
		if edge.From == node.Name {
			out++
		}
	}
	return fmt.Sprintf("%s, %d documents, %d incoming and %d outgoing edge definitions.", node.Name, node.Count, in, out)
}

func (s *topologyScene) Frame() canvas.Frame {
	colors := s.palette()
	commands := []canvas.Command{
		canvas.Clear{Color: colors.background},
		canvas.Rect{Paint: canvas.Paint{Fill: colors.surfaceAlt, NoStroke: true}, X: 0, Y: 0, Width: s.width, Height: 58},
	}
	commands = append(commands, canvas.FillText{
		Paint: canvas.Paint{Fill: colors.text, Font: "600 15px Inter, system-ui, sans-serif"},
		X:     20, Y: 28, Text: s.title,
	})
	commands = append(commands, canvas.FillText{
		Paint: canvas.Paint{Fill: colors.muted, Font: "12px Inter, system-ui, sans-serif"},
		X:     20, Y: 46, Text: s.status,
	})

	if len(s.nodes) == 0 {
		commands = append(commands, canvas.FillText{
			Paint: canvas.Paint{Fill: colors.muted, Font: "13px Inter, system-ui, sans-serif"},
			X:     20, Y: 80, Text: "This graph has no vertex collections yet.",
		})
		return canvas.Frame{Commands: commands}
	}

	for _, edge := range s.edges {
		commands = append(commands, s.edgeCommands(edge, colors)...)
	}

	regions := make([]canvas.Region, 0, len(s.nodes))
	for i, node := range s.nodes {
		x, y := node.X-nodeWidth/2, node.Y-nodeHeight/2
		fill := colors.surface
		border := colors.border
		switch {
		case i == s.selected:
			border = colors.accent
		case node.Name == s.hover:
			border = colors.highlight
		}
		commands = append(commands,
			canvas.Rect{
				Paint: canvas.Paint{Fill: fill, Stroke: border, LineWidth: boolFloat(i == s.selected, 2.5, 1.25)},
				X:     x, Y: y, Width: nodeWidth, Height: nodeHeight,
				Radii: &canvas.Radii{TopLeft: 10, TopRight: 10, BottomRight: 10, BottomLeft: 10},
			},
			canvas.TextBox{
				Paint: canvas.Paint{Fill: colors.text, Font: "600 13px Inter, system-ui, sans-serif"},
				X:     x + 12, Y: y + 10, Width: nodeWidth - 24, Height: 18,
				MaxLines: 1, Ellipsis: "…", Text: node.Name,
			},
			canvas.FillText{
				Paint: canvas.Paint{Fill: colors.muted, Font: "11px Inter, system-ui, sans-serif"},
				X:     x + 12, Y: y + 44, Text: strconv.FormatInt(node.Count, 10) + " documents",
			},
		)
		regions = append(regions, canvas.RectRegion("node:"+node.Name, x, y, nodeWidth, nodeHeight,
			canvas.WithCursor("pointer"), canvas.WithLabel(s.describe(i))))
	}

	if s.announce != "" {
		commands = append(commands, canvas.Announce{Text: s.announce, Mode: canvas.AnnouncePolite})
	}
	if s.selected < len(s.nodes) {
		commands = append(commands, canvas.FocusRegion{ID: "node:" + s.nodes[s.selected].Name})
	}
	return canvas.Frame{Commands: commands, Regions: regions}
}

func (s *topologyScene) edgeCommands(edge topoEdge, colors topoPalette) []canvas.Command {
	from, okFrom := s.index[edge.From]
	to, okTo := s.index[edge.To]
	if !okFrom || !okTo {
		return nil
	}
	a, b := s.nodes[from], s.nodes[to]
	highlight := s.hover == edge.From || s.hover == edge.To ||
		(s.selected < len(s.nodes) && (s.nodes[s.selected].Name == edge.From || s.nodes[s.selected].Name == edge.To))
	stroke := colors.edge
	width := 1.25
	if highlight {
		stroke = colors.accent
		width = 2
	}
	label := edge.Collection + " (" + strconv.FormatInt(edge.Count, 10) + ")"

	if from == to {
		radius := nodeHeight
		cx, cy := a.X+nodeWidth/2, a.Y-nodeHeight/2
		return []canvas.Command{
			canvas.Arc{
				Paint: canvas.Paint{Stroke: stroke, LineWidth: width, NoFill: true},
				X:     cx, Y: cy, Radius: radius, StartAngle: 0, EndAngle: 2 * math.Pi,
			},
			canvas.FillText{
				Paint: canvas.Paint{Fill: colors.edgeText, Font: "11px Inter, system-ui, sans-serif"},
				X:     cx + radius, Y: cy - radius, Text: label,
			},
		}
	}

	sx, sy := anchor(a, b)
	tx, ty := anchor(b, a)
	mx, my := (sx+tx)/2, (sy+ty)/2
	commands := []canvas.Command{
		canvas.QuadraticCurve{
			Paint: canvas.Paint{Stroke: stroke, LineWidth: width, NoFill: true},
			X0:    sx, Y0: sy, CPX: mx, CPY: my, X: tx, Y: ty,
		},
	}
	commands = append(commands, arrowHead(sx, sy, tx, ty, stroke))
	commands = append(commands, canvas.FillText{
		Paint: canvas.Paint{Fill: colors.edgeText, Font: "11px Inter, system-ui, sans-serif", TextAlign: canvas.TextAlignCenter},
		X:     mx, Y: my - 6, Text: label,
	})
	return commands
}

// anchor projects the line between two node centres onto the source node's
// border so connectors start and end at the box edge, not inside it.
func anchor(from, to topoNode) (float64, float64) {
	dx, dy := to.X-from.X, to.Y-from.Y
	if dx == 0 && dy == 0 {
		return from.X, from.Y
	}
	halfW, halfH := nodeWidth/2, nodeHeight/2
	scale := math.Min(halfW/math.Max(math.Abs(dx), 1e-6), halfH/math.Max(math.Abs(dy), 1e-6))
	return from.X + dx*scale, from.Y + dy*scale
}

func arrowHead(sx, sy, tx, ty float64, stroke string) canvas.Command {
	angle := math.Atan2(ty-sy, tx-sx)
	const size = 9
	return canvas.Polygon{
		Paint: canvas.Paint{Fill: stroke, NoStroke: true},
		Points: []canvas.Point{
			{X: tx, Y: ty},
			{X: tx - size*math.Cos(angle-math.Pi/7), Y: ty - size*math.Sin(angle-math.Pi/7)},
			{X: tx - size*math.Cos(angle+math.Pi/7), Y: ty - size*math.Sin(angle+math.Pi/7)},
		},
	}
}

func boolFloat(cond bool, yes, no float64) float64 {
	if cond {
		return yes
	}
	return no
}

func regionNode(id string) string {
	name, ok := strings.CutPrefix(id, "node:")
	if !ok {
		return ""
	}
	return name
}

func graphCanvasStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, database, name, err := graphScope(rc)
	if err != nil {
		return err
	}
	scene := &topologyScene{title: name + " · " + database, width: 960, height: 560}
	if err := s.loadTopology(client.Context(), scene, database, name); err != nil {
		return err
	}

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

	refresh := time.NewTicker(topologyRefresh)
	defer refresh.Stop()
	dirty := true
	for {
		if dirty {
			if err := canvas.WriteFrame(client, scene.Frame()); err != nil {
				return err
			}
			scene.announce = ""
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
			dirty = scene.Handle(ev)
		case <-refresh.C:
			if err := s.loadTopology(client.Context(), scene, database, name); err == nil {
				dirty = true
			}
		}
	}
}

func (s *Session) loadTopology(ctx context.Context, scene *topologyScene, database, name string) error {
	graph, err := s.graphDefinition(ctx, database, name)
	if err != nil {
		return err
	}
	edgeCollections := map[string]bool{}
	for _, definition := range graph.EdgeDefinitions() {
		edgeCollections[definition.Collection] = true
	}
	nodes := make([]topoNode, 0, 8)
	for _, collection := range vertexCollectionNames(graph) {
		nodes = append(nodes, topoNode{
			Name:  collection,
			Count: s.queryInt(ctx, database, "RETURN LENGTH(@@col)", map[string]any{"@col": collection}),
		})
	}
	edges := make([]topoEdge, 0, 8)
	for _, definition := range graph.EdgeDefinitions() {
		count := s.queryInt(ctx, database, "RETURN LENGTH(@@col)", map[string]any{"@col": definition.Collection})
		for _, from := range definition.From {
			for _, to := range definition.To {
				edges = append(edges, topoEdge{Collection: definition.Collection, From: from, To: to, Count: count})
			}
		}
	}
	status := fmt.Sprintf("%d vertex collections · %d edge definitions · refreshed %s",
		len(nodes), len(graph.EdgeDefinitions()), time.Now().UTC().Format("15:04:05")+" UTC")
	scene.load(nodes, edges, status)
	return nil
}
