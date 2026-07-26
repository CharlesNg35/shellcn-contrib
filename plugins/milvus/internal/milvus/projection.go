package milvus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/milvusclient"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugin/canvas"
)

const (
	projectionPCA    = "pca"
	projectionRandom = "random"

	plotTop      = 56.0
	plotBottom   = 34.0
	plotMargin   = 24.0
	pointRadius  = 3.5
	hoverRadius  = 10.0
	powerIters   = 30
	sidePanelWid = 230.0

	frameInterval = 200 * time.Millisecond
)

type samplePoint struct {
	id      string
	label   string
	payload map[string]any
	vector  []float32
	x, y    float64 // projected, normalized to [-1,1]
	sx, sy  float64 // screen coordinates
}

type projectionScene struct {
	collection string
	field      string
	fields     []string
	dims       int
	method     string
	seed       int64

	width, height float64
	theme         plugin.PanelTheme

	points   []samplePoint
	variance [2]float64
	status   string

	zoom     float64
	panX     float64
	panY     float64
	hover    int
	lasso    []canvas.Point
	dragging bool
	selected map[int]bool
}

func projectionStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	collection, err := collectionParam(rc)
	if err != nil {
		return err
	}
	s, c, err := clientOf(rc)
	if err != nil {
		return err
	}
	scene := &projectionScene{
		collection: collection,
		method:     projectionPCA,
		seed:       time.Now().UnixNano(),
		width:      960,
		height:     600,
		zoom:       1,
		hover:      -1,
		selected:   map[int]bool{},
		status:     "Sampling vectors…",
		theme:      plugin.PanelThemeDark,
	}
	if field := strings.TrimSpace(rc.Param("field")); field != "" {
		name, err := identifier("field", field)
		if err != nil {
			return err
		}
		scene.field = name
	}

	events := make(chan canvas.Event, 64)
	readErr := make(chan error, 1)
	go func() {
		for {
			ev, err := canvas.DecodeEvent(client)
			if err != nil {
				readErr <- err
				return
			}
			select {
			case events <- ev:
			case <-client.Context().Done():
				return
			}
		}
	}()

	scene.resample(rc.Ctx, s, c)
	if err := canvas.WriteFrame(client, scene.frame()); err != nil {
		return err
	}

	// Frames carry every sampled point, so pointer moves are coalesced onto a
	// redraw tick instead of writing one frame per event.
	redraw := time.NewTicker(frameInterval)
	defer redraw.Stop()
	dirty := false
	for {
		select {
		case <-client.Context().Done():
			return nil
		case <-rc.Ctx.Done():
			return nil
		case err := <-readErr:
			if errors.Is(err, io.EOF) || client.Context().Err() != nil {
				return nil
			}
			return err
		case ev := <-events:
			changed, resample := scene.handle(ev)
			if resample {
				scene.resample(rc.Ctx, s, c)
				changed = true
			}
			dirty = dirty || changed
		case <-redraw.C:
			if !dirty {
				continue
			}
			dirty = false
			if err := canvas.WriteFrame(client, scene.frame()); err != nil {
				return err
			}
		}
	}
}

func (s *projectionScene) handle(ev canvas.Event) (dirty bool, resample bool) {
	switch e := ev.(type) {
	case *canvas.ReadyEvent:
		s.width, s.height, s.theme = e.Width, e.Height, e.Theme
		s.project()
		return true, false
	case *canvas.ResizeEvent:
		s.width, s.height, s.theme = e.Width, e.Height, e.Theme
		s.project()
		return true, false
	case *canvas.PointerEvent:
		return s.handlePointer(e)
	case *canvas.WheelEvent:
		factor := math.Pow(0.999, e.DeltaY)
		s.zoom = clampFloat(s.zoom*factor, 0.4, 12)
		s.project()
		return true, false
	case *canvas.KeyEvent:
		return s.handleKey(e)
	}
	return false, false
}

func (s *projectionScene) handlePointer(e *canvas.PointerEvent) (bool, bool) {
	switch e.RegionID {
	case "action:resample":
		if e.Event == canvas.PointerDown {
			s.seed = time.Now().UnixNano()
			return false, true
		}
		return false, false
	case "action:method":
		if e.Event == canvas.PointerDown {
			s.toggleMethod()
			return true, false
		}
		return false, false
	case "action:field":
		if e.Event == canvas.PointerDown {
			return false, s.nextField()
		}
		return false, false
	case "action:clear":
		if e.Event == canvas.PointerDown {
			s.clearSelection()
			return true, false
		}
		return false, false
	case "action:reset":
		if e.Event == canvas.PointerDown {
			s.zoom, s.panX, s.panY = 1, 0, 0
			s.project()
			return true, false
		}
		return false, false
	}

	switch e.Event {
	case canvas.PointerDown:
		s.dragging = true
		s.lasso = []canvas.Point{{X: e.X, Y: e.Y}}
		return true, false
	case canvas.PointerMove:
		changed := s.updateHover(e.X, e.Y)
		if s.dragging && len(s.lasso) > 0 {
			last := s.lasso[len(s.lasso)-1]
			if math.Hypot(e.X-last.X, e.Y-last.Y) > 4 {
				s.lasso = append(s.lasso, canvas.Point{X: e.X, Y: e.Y})
				changed = true
			}
		}
		return changed, false
	case canvas.PointerUp, canvas.PointerCancel:
		if !s.dragging {
			return false, false
		}
		s.dragging = false
		s.applyLasso()
		return true, false
	}
	return false, false
}

func (s *projectionScene) handleKey(e *canvas.KeyEvent) (bool, bool) {
	if e.Event != "keydown" {
		return false, false
	}
	switch strings.ToLower(e.Key) {
	case "r":
		s.seed = time.Now().UnixNano()
		return false, true
	case "p":
		s.toggleMethod()
		return true, false
	case "f":
		return false, s.nextField()
	case "c":
		s.clearSelection()
		return true, false
	case "0":
		s.zoom, s.panX, s.panY = 1, 0, 0
		s.project()
		return true, false
	case "+", "=":
		s.zoom = clampFloat(s.zoom*1.2, 0.4, 12)
		s.project()
		return true, false
	case "-", "_":
		s.zoom = clampFloat(s.zoom/1.2, 0.4, 12)
		s.project()
		return true, false
	case "arrowleft":
		s.panX += 0.1 / s.zoom
		s.project()
		return true, false
	case "arrowright":
		s.panX -= 0.1 / s.zoom
		s.project()
		return true, false
	case "arrowup":
		s.panY += 0.1 / s.zoom
		s.project()
		return true, false
	case "arrowdown":
		s.panY -= 0.1 / s.zoom
		s.project()
		return true, false
	}
	return false, false
}

// nextField moves to the next float vector field of the collection and reports
// whether the sample has to be redrawn from it.
func (s *projectionScene) nextField() bool {
	if len(s.fields) < 2 {
		return false
	}
	for i, name := range s.fields {
		if name == s.field {
			s.field = s.fields[(i+1)%len(s.fields)]
			return true
		}
	}
	s.field = s.fields[0]
	return true
}

// clearSelection also ends any in-flight drag so the lasso never keeps
// collecting points into a selection the user just dropped.
func (s *projectionScene) clearSelection() {
	s.selected = map[int]bool{}
	s.lasso = nil
	s.dragging = false
	s.status = s.summary()
}

func (s *projectionScene) toggleMethod() {
	if s.method == projectionPCA {
		s.method = projectionRandom
	} else {
		s.method = projectionPCA
	}
	s.computeProjection()
	s.project()
	s.status = s.summary()
}

func (s *projectionScene) updateHover(x, y float64) bool {
	best, bestDist := -1, hoverRadius
	for i := range s.points {
		d := math.Hypot(s.points[i].sx-x, s.points[i].sy-y)
		if d < bestDist {
			best, bestDist = i, d
		}
	}
	if best == s.hover {
		return false
	}
	s.hover = best
	return true
}

func (s *projectionScene) applyLasso() {
	if len(s.lasso) < 3 {
		s.lasso = nil
		if s.hover >= 0 {
			s.selected = map[int]bool{s.hover: true}
		}
		s.status = s.summary()
		return
	}
	selected := map[int]bool{}
	for i := range s.points {
		if pointInPolygon(s.points[i].sx, s.points[i].sy, s.lasso) {
			selected[i] = true
		}
	}
	s.selected = selected
	s.status = s.summary()
}

func (s *projectionScene) summary() string {
	if len(s.points) == 0 {
		return "No vectors sampled."
	}
	base := fmt.Sprintf("%d vectors · %d dims · %s", len(s.points), s.dims, strings.ToUpper(s.method))
	if s.method == projectionPCA {
		base += fmt.Sprintf(" · PC1 %.1f%% / PC2 %.1f%%", s.variance[0]*100, s.variance[1]*100)
	}
	if len(s.selected) > 0 {
		base += fmt.Sprintf(" · %d selected", len(s.selected))
	}
	return base
}

func (s *projectionScene) resample(ctx context.Context, sess *Session, c *milvusclient.Client) {
	sampleCtx, cancel := context.WithTimeout(ctx, sess.opts.Timeout)
	defer cancel()

	coll, err := collectionSchemaOf(sampleCtx, c, s.collection)
	if err != nil {
		s.points = nil
		s.status = "Cannot read collection schema: " + err.Error()
		return
	}
	s.fields = make([]string, 0, 2)
	for _, f := range coll.Schema.Fields {
		if f.DataType == entity.FieldTypeFloatVector {
			s.fields = append(s.fields, f.Name)
		}
	}
	if len(s.fields) == 0 {
		s.points = nil
		s.status = "This collection has no float vector field to project."
		return
	}
	if !slices.Contains(s.fields, s.field) {
		s.field = s.fields[0]
	}
	field := s.field
	pk := primaryKeyName(coll.Schema)

	rs, err := c.Query(sampleCtx, milvusclient.NewQueryOption(s.collection).
		WithLimit(sess.opts.SampleLimit).
		WithOutputFields("*"))
	if err != nil {
		s.points = nil
		s.status = "Sampling failed: " + mapError(err).Error()
		return
	}
	vectorFields := map[string]bool{}
	for _, f := range coll.Schema.Fields {
		if f.DataType.IsVectorType() {
			vectorFields[f.Name] = true
		}
	}
	rows := resultRows(rs, pk)
	points := make([]samplePoint, 0, len(rows))
	for _, item := range rows {
		vector, err := asFloat32Slice(item[field])
		if err != nil || len(vector) == 0 {
			continue
		}
		payload := map[string]any{}
		for key, value := range item {
			if key == entityKeyField || vectorFields[key] {
				continue
			}
			payload[key] = value
		}
		points = append(points, samplePoint{
			id:      fmt.Sprint(item[pk]),
			label:   labelFor(payload, pk, item),
			payload: payload,
			vector:  vector,
		})
	}
	s.points = points
	s.clearSelection()
	s.hover = -1
	if len(points) == 0 {
		s.dims = 0
		s.status = "No vectors returned. Load the collection and make sure it holds entities."
		return
	}
	s.dims = len(points[0].vector)
	s.computeProjection()
	s.project()
	s.status = s.summary()
}

func labelFor(payload map[string]any, pk string, item row) string {
	keys := make([]string, 0, len(payload))
	for key := range payload {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if key == pk || key == "score" {
			continue
		}
		if text, ok := payload[key].(string); ok && strings.TrimSpace(text) != "" {
			return text
		}
	}
	return fmt.Sprint(item[pk])
}

// computeProjection reduces the sampled vectors to two dimensions using either
// power-iteration PCA or a seeded Gaussian random projection.
func (s *projectionScene) computeProjection() {
	n := len(s.points)
	if n == 0 {
		return
	}
	d := s.dims
	data := make([][]float64, n)
	mean := make([]float64, d)
	for i, p := range s.points {
		row := make([]float64, d)
		for j := 0; j < d && j < len(p.vector); j++ {
			row[j] = float64(p.vector[j])
			mean[j] += row[j]
		}
		data[i] = row
	}
	for j := range mean {
		mean[j] /= float64(n)
	}
	for i := range data {
		for j := range data[i] {
			data[i][j] -= mean[j]
		}
	}

	var axes [2][]float64
	if s.method == projectionRandom {
		rng := rand.New(rand.NewSource(s.seed))
		for k := 0; k < 2; k++ {
			axis := make([]float64, d)
			for j := range axis {
				axis[j] = rng.NormFloat64()
			}
			normalize(axis)
			axes[k] = axis
		}
		s.variance = [2]float64{0, 0}
	} else {
		total := 0.0
		for i := range data {
			for j := range data[i] {
				total += data[i][j] * data[i][j]
			}
		}
		for k := 0; k < 2; k++ {
			axis := principalAxis(data, d, s.seed+int64(k))
			axes[k] = axis
			energy := 0.0
			for i := range data {
				score := dot(data[i], axis)
				energy += score * score
				for j := range data[i] {
					data[i][j] -= score * axis[j]
				}
			}
			if total > 0 {
				s.variance[k] = energy / total
			}
		}
		// Recompute centred data because deflation consumed it.
		for i, p := range s.points {
			for j := 0; j < d && j < len(p.vector); j++ {
				data[i][j] = float64(p.vector[j]) - mean[j]
			}
		}
	}

	minX, maxX := math.Inf(1), math.Inf(-1)
	minY, maxY := math.Inf(1), math.Inf(-1)
	for i := range s.points {
		x := dot(data[i], axes[0])
		y := dot(data[i], axes[1])
		s.points[i].x, s.points[i].y = x, y
		minX, maxX = math.Min(minX, x), math.Max(maxX, x)
		minY, maxY = math.Min(minY, y), math.Max(maxY, y)
	}
	spanX := math.Max(maxX-minX, 1e-9)
	spanY := math.Max(maxY-minY, 1e-9)
	for i := range s.points {
		s.points[i].x = (s.points[i].x-minX)/spanX*2 - 1
		s.points[i].y = (s.points[i].y-minY)/spanY*2 - 1
	}
}

func principalAxis(data [][]float64, d int, seed int64) []float64 {
	rng := rand.New(rand.NewSource(seed))
	axis := make([]float64, d)
	for j := range axis {
		axis[j] = rng.NormFloat64()
	}
	normalize(axis)
	next := make([]float64, d)
	for iter := 0; iter < powerIters; iter++ {
		for j := range next {
			next[j] = 0
		}
		for i := range data {
			score := dot(data[i], axis)
			for j := range data[i] {
				next[j] += score * data[i][j]
			}
		}
		if norm(next) == 0 {
			break
		}
		normalize(next)
		copy(axis, next)
	}
	return axis
}

func (s *projectionScene) project() {
	left, top, right, bottom := s.plotBounds()
	cx, cy := (left+right)/2, (top+bottom)/2
	halfW := (right - left) / 2
	halfH := (bottom - top) / 2
	for i := range s.points {
		s.points[i].sx = cx + (s.points[i].x+s.panX)*halfW*s.zoom
		s.points[i].sy = cy - (s.points[i].y+s.panY)*halfH*s.zoom
	}
}

func (s *projectionScene) plotBounds() (left, top, right, bottom float64) {
	left = plotMargin
	top = plotTop
	right = math.Max(s.width-plotMargin-sidePanelWid, left+80)
	bottom = math.Max(s.height-plotBottom, top+80)
	return
}

type paletteColors struct {
	background, surface, text, muted, border, accent, accentSoft, selection string
}

func (s *projectionScene) colors() paletteColors {
	if s.theme == plugin.PanelThemeLight {
		return paletteColors{
			background: "#f8fafc", surface: "#ffffff", text: "#0f172a", muted: "#475569",
			border: "#cbd5e1", accent: "#0284c7", accentSoft: "rgba(2,132,199,0.45)",
			selection: "#c2410c",
		}
	}
	return paletteColors{
		background: "#020617", surface: "#0f172a", text: "#e2e8f0", muted: "#94a3b8",
		border: "#334155", accent: "#38bdf8", accentSoft: "rgba(56,189,248,0.45)",
		selection: "#fb923c",
	}
}

func (s *projectionScene) frame() canvas.Frame {
	c := s.colors()
	left, top, right, bottom := s.plotBounds()
	commands := []canvas.Command{
		canvas.Clear{Color: c.background},
		canvas.Rect{Paint: canvas.Paint{Fill: c.surface, Stroke: c.border, LineWidth: 1}, X: left, Y: top, Width: right - left, Height: bottom - top, Radius: 10},
	}
	commands = append(commands, s.gridCommands(c, left, top, right, bottom)...)
	commands = append(commands,
		canvas.FillText{Paint: canvas.Paint{Fill: c.text, Font: "600 15px Inter, system-ui, sans-serif"}, X: plotMargin, Y: 26, Text: s.collection + " · " + s.field},
		canvas.FillText{Paint: canvas.Paint{Fill: c.muted, Font: "12px Inter, system-ui, sans-serif"}, X: plotMargin, Y: 44, Text: s.status},
	)
	commands = append(commands, s.buttonCommands(c)...)
	commands = append(commands, s.pointCommands(c)...)
	commands = append(commands, s.lassoCommands(c)...)
	commands = append(commands, s.panelCommands(c)...)
	commands = append(commands,
		canvas.FillText{Paint: canvas.Paint{Fill: c.muted, Font: "11px Inter, system-ui, sans-serif"}, X: plotMargin, Y: s.height - 12,
			Text: "Drag to lasso · R resample · P projection · F vector field · C clear · 0 reset view · +/- zoom · arrows pan"},
	)
	return canvas.Frame{Commands: commands, Regions: s.regions()}
}

func (s *projectionScene) gridCommands(c paletteColors, left, top, right, bottom float64) []canvas.Command {
	out := make([]canvas.Command, 0, 12)
	steps := 4
	for i := 1; i < steps; i++ {
		x := left + (right-left)*float64(i)/float64(steps)
		y := top + (bottom-top)*float64(i)/float64(steps)
		out = append(out,
			canvas.Line{Paint: canvas.Paint{Stroke: c.border, LineWidth: 0.5, Alpha: alpha(0.5)}, X1: x, Y1: top, X2: x, Y2: bottom},
			canvas.Line{Paint: canvas.Paint{Stroke: c.border, LineWidth: 0.5, Alpha: alpha(0.5)}, X1: left, Y1: y, X2: right, Y2: y},
		)
	}
	return out
}

func (s *projectionScene) buttonCommands(c paletteColors) []canvas.Command {
	out := make([]canvas.Command, 0, 8)
	for _, b := range s.buttons() {
		out = append(out,
			canvas.Rect{Paint: canvas.Paint{Fill: c.surface, Stroke: c.border, LineWidth: 1}, X: b.x, Y: b.y, Width: b.w, Height: b.h, Radius: 6},
			canvas.TextBox{
				Paint: canvas.Paint{Fill: c.accent, Font: "600 12px Inter, system-ui, sans-serif", TextAlign: canvas.TextAlignCenter},
				X:     b.x, Y: b.y, Width: b.w, Height: b.h,
				VerticalAlign: canvas.TextVerticalAlignMiddle, MaxLines: 1, Text: b.label,
			},
		)
	}
	return out
}

type sceneButton struct {
	id, label     string
	x, y, w, h    float64
	accessibility string
}

func (s *projectionScene) buttons() []sceneButton {
	method := "Projection: PCA"
	if s.method == projectionRandom {
		method = "Projection: Random"
	}
	labels := []sceneButton{
		{id: "action:resample", label: "Resample", accessibility: "Draw a new sample of vectors"},
		{id: "action:method", label: method, accessibility: "Switch between PCA and random projection"},
		{id: "action:clear", label: "Clear selection", accessibility: "Clear the lasso selection"},
		{id: "action:reset", label: "Reset view", accessibility: "Reset zoom and pan"},
	}
	if len(s.fields) > 1 {
		labels = append(labels, sceneButton{
			id: "action:field", label: "Vector: " + s.field, accessibility: "Project the next vector field",
		})
	}
	out := make([]sceneButton, 0, len(labels))
	x := math.Max(s.width-plotMargin-sidePanelWid, plotMargin)
	for i, item := range labels {
		item.x, item.y = x, plotTop+float64(i)*34
		item.w, item.h = sidePanelWid-8, 28
		out = append(out, item)
	}
	return out
}

func (s *projectionScene) pointCommands(c paletteColors) []canvas.Command {
	out := make([]canvas.Command, 0, len(s.points)+4)
	left, top, right, bottom := s.plotBounds()
	for i := range s.points {
		p := s.points[i]
		if p.sx < left || p.sx > right || p.sy < top || p.sy > bottom {
			continue
		}
		fill := c.accentSoft
		radius := pointRadius
		if s.selected[i] {
			fill = c.selection
			radius = pointRadius + 1.5
		}
		if i == s.hover {
			fill = c.accent
			radius = pointRadius + 2.5
		}
		out = append(out, canvas.Circle{Paint: canvas.Paint{Fill: fill}, X: p.sx, Y: p.sy, Radius: radius})
	}
	if s.hover >= 0 && s.hover < len(s.points) {
		out = append(out, s.tooltipCommands(c, s.points[s.hover])...)
	}
	return out
}

func (s *projectionScene) tooltipCommands(c paletteColors, p samplePoint) []canvas.Command {
	lines := []string{"id: " + p.id}
	if p.label != "" && p.label != p.id {
		lines = append(lines, "label: "+truncate(p.label, 48))
	}
	keys := make([]string, 0, len(p.payload))
	for key := range p.payload {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if len(lines) >= 6 {
			break
		}
		lines = append(lines, key+": "+truncate(fmt.Sprint(p.payload[key]), 48))
	}
	width := 260.0
	height := float64(len(lines))*16 + 16
	left, top, right, bottom := s.plotBounds()
	x := math.Min(p.sx+14, right-width)
	x = math.Max(x, left)
	y := math.Min(p.sy+12, bottom-height)
	y = math.Max(y, top)
	return []canvas.Command{
		canvas.Rect{Paint: canvas.Paint{Fill: c.surface, Stroke: c.accent, LineWidth: 1}, X: x, Y: y, Width: width, Height: height, Radius: 8},
		canvas.TextBox{
			Paint: canvas.Paint{Fill: c.text, Font: "12px ui-monospace, SFMono-Regular, monospace"},
			X:     x, Y: y, Width: width, Height: height, Padding: 8, LineHeight: 16,
			MaxLines: len(lines), Ellipsis: "…", Text: strings.Join(lines, "\n"),
		},
	}
}

func (s *projectionScene) lassoCommands(c paletteColors) []canvas.Command {
	if len(s.lasso) < 2 {
		return nil
	}
	return []canvas.Command{canvas.Polygon{
		Paint:  canvas.Paint{Stroke: c.selection, LineWidth: 1.5, Fill: "rgba(251,146,60,0.12)"},
		Points: s.lasso,
	}}
}

func (s *projectionScene) panelCommands(c paletteColors) []canvas.Command {
	x := math.Max(s.width-plotMargin-sidePanelWid, plotMargin)
	y := plotTop + float64(len(s.buttons()))*34 + 8
	height := math.Max(s.height-plotBottom-y, 60)
	lines := make([]string, 0, 14)
	if len(s.selected) == 0 {
		lines = append(lines, "Lasso points to sample them.", "", "Hover a point for its id and payload.")
	} else {
		ids := make([]string, 0, len(s.selected))
		for i := range s.selected {
			if i >= len(s.points) {
				continue
			}
			entry := s.points[i].id
			if label := s.points[i].label; label != "" && label != entry {
				entry += " · " + truncate(label, 20)
			}
			ids = append(ids, entry)
		}
		sort.Strings(ids)
		lines = append(lines, fmt.Sprintf("Selected %d of %d", len(ids), len(s.points)), "")
		for i, id := range ids {
			if i >= 12 {
				lines = append(lines, fmt.Sprintf("… %d more", len(ids)-i))
				break
			}
			lines = append(lines, id)
		}
	}
	return []canvas.Command{
		canvas.Rect{Paint: canvas.Paint{Fill: c.surface, Stroke: c.border, LineWidth: 1}, X: x, Y: y, Width: sidePanelWid - 8, Height: height, Radius: 8},
		canvas.TextBox{
			Paint: canvas.Paint{Fill: c.muted, Font: "12px ui-monospace, SFMono-Regular, monospace"},
			X:     x, Y: y, Width: sidePanelWid - 8, Height: height, Padding: 10, LineHeight: 16,
			MaxLines: int(height / 16), Ellipsis: "…", Text: strings.Join(lines, "\n"),
		},
	}
}

func (s *projectionScene) regions() []canvas.Region {
	out := make([]canvas.Region, 0, 5)
	for _, b := range s.buttons() {
		out = append(out, canvas.RectRegion(b.id, b.x, b.y, b.w, b.h,
			canvas.WithCursor("pointer"), canvas.WithLabel(b.accessibility)))
	}
	return out
}

func pointInPolygon(x, y float64, polygon []canvas.Point) bool {
	inside := false
	for i, j := 0, len(polygon)-1; i < len(polygon); j, i = i, i+1 {
		xi, yi := polygon[i].X, polygon[i].Y
		xj, yj := polygon[j].X, polygon[j].Y
		if (yi > y) != (yj > y) && x < (xj-xi)*(y-yi)/(yj-yi)+xi {
			inside = !inside
		}
	}
	return inside
}

func dot(a, b []float64) float64 {
	sum := 0.0
	for i := range a {
		if i >= len(b) {
			break
		}
		sum += a[i] * b[i]
	}
	return sum
}

func norm(v []float64) float64 { return math.Sqrt(dot(v, v)) }

func normalize(v []float64) {
	n := norm(v)
	if n == 0 {
		return
	}
	for i := range v {
		v[i] /= n
	}
}

func clampFloat(v, lo, hi float64) float64 { return math.Max(lo, math.Min(hi, v)) }

func alpha(v float64) *float64 { return &v }

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-1] + "…"
}
