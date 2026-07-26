package weaviate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugin/canvas"
	"github.com/weaviate/weaviate/entities/models"
)

const (
	embeddingClusters = 5
	pcaIterations     = 64
	kmeansIterations  = 40
)

type embeddingPoint struct {
	ID      string
	Label   string
	X       float64
	Y       float64
	Cluster int
	screenX float64
	screenY float64
}

type palette struct {
	Background string
	Surface    string
	Grid       string
	Text       string
	Muted      string
	Border     string
	Clusters   []string
}

var darkPalette = palette{
	Background: "#0b1120", Surface: "#111c33", Grid: "#1e293b", Text: "#e2e8f0",
	Muted: "#94a3b8", Border: "#334155",
	Clusters: []string{"#38bdf8", "#34d399", "#f472b6", "#fbbf24", "#a78bfa", "#fb7185"},
}

var lightPalette = palette{
	Background: "#f8fafc", Surface: "#ffffff", Grid: "#e2e8f0", Text: "#0f172a",
	Muted: "#475569", Border: "#cbd5e1",
	Clusters: []string{"#0284c7", "#059669", "#db2777", "#b45309", "#7c3aed", "#e11d48"},
}

type embeddingScene struct {
	className    string
	targetVector string
	theme        plugin.PanelTheme
	width        float64
	height       float64
	points       []embeddingPoint
	variance     [2]float64
	clusters     int
	selected     int
	hovered      int
	zoom         float64
	status       string
	failure      string
	announcement string
}

func embeddingStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := session(rc)
	if err != nil {
		return err
	}
	class, err := loadClass(rc)
	if err != nil {
		return err
	}
	scene := &embeddingScene{
		className:    class.Class,
		targetVector: strings.TrimSpace(rc.Param("target_vector")),
		theme:        plugin.PanelThemeDark,
		width:        960,
		height:       600,
		selected:     -1,
		hovered:      -1,
		zoom:         1,
		status:       "Loading embeddings…",
	}
	events := make(chan canvas.Event, 64)
	errc := make(chan error, 1)
	go func() {
		for {
			event, err := canvas.DecodeEvent(client)
			if err != nil {
				errc <- err
				return
			}
			select {
			case events <- event:
			case <-client.Context().Done():
				return
			}
		}
	}()

	// Draw the placeholder before sampling: fetching thousands of vectors can
	// take a while and an empty canvas reads as a broken panel.
	if err := canvas.WriteFrame(client, scene.frame()); err != nil {
		return err
	}
	scene.load(rc.Ctx, s, class)
	if err := canvas.WriteFrame(client, scene.frame()); err != nil {
		return err
	}
	for {
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
		case event := <-events:
			if !scene.handle(rc.Ctx, s, class, event) {
				continue
			}
			if err := canvas.WriteFrame(client, scene.frame()); err != nil {
				return err
			}
		}
	}
}

func (s *embeddingScene) handle(ctx context.Context, sess *Session, class *models.Class, event canvas.Event) bool {
	switch typed := event.(type) {
	case *canvas.ReadyEvent:
		s.theme, s.width, s.height = typed.Theme, typed.Width, typed.Height
		return true
	case *canvas.ResizeEvent:
		s.theme, s.width, s.height = typed.Theme, typed.Width, typed.Height
		return true
	case *canvas.PointerEvent:
		return s.handlePointer(typed)
	case *canvas.WheelEvent:
		s.setZoom(s.zoom * math.Pow(0.999, typed.DeltaY))
		return true
	case *canvas.KeyEvent:
		return s.handleKey(ctx, sess, class, typed)
	default:
		return false
	}
}

func (s *embeddingScene) handlePointer(event *canvas.PointerEvent) bool {
	index := pointIndex(event.RegionID)
	switch event.Event {
	case canvas.PointerMove:
		if index == s.hovered {
			return false
		}
		s.hovered = index
		return true
	case canvas.PointerDown:
		if index < 0 {
			return false
		}
		s.selected = index
		s.announceSelection()
		return true
	default:
		return false
	}
}

func (s *embeddingScene) handleKey(ctx context.Context, sess *Session, class *models.Class, event *canvas.KeyEvent) bool {
	if event.Event != "keydown" {
		return false
	}
	reload := event.Key == "r" || event.Key == "R"
	if len(s.points) == 0 && !reload {
		return false
	}
	switch event.Key {
	case "ArrowRight", "ArrowDown":
		s.selected = (s.selected + 1 + len(s.points)) % len(s.points)
		s.announceSelection()
		return true
	case "ArrowLeft", "ArrowUp":
		s.selected = (s.selected - 1 + len(s.points)) % len(s.points)
		s.announceSelection()
		return true
	case "+", "=":
		s.setZoom(s.zoom * 1.2)
		return true
	case "-", "_":
		s.setZoom(s.zoom / 1.2)
		return true
	case "0":
		s.setZoom(1)
		return true
	case "r", "R":
		s.status = "Reloading embeddings…"
		s.load(ctx, sess, class)
		s.announcement = s.status
		return true
	default:
		return false
	}
}

func (s *embeddingScene) setZoom(value float64) {
	s.zoom = math.Min(6, math.Max(0.4, value))
}

func (s *embeddingScene) announceSelection() {
	if s.selected < 0 || s.selected >= len(s.points) {
		return
	}
	point := s.points[s.selected]
	s.announcement = fmt.Sprintf("Selected %s, cluster %d, %s", shortID(point.ID), point.Cluster+1, point.Label)
}

func pointIndex(regionID string) int {
	if !strings.HasPrefix(regionID, "pt:") || len(regionID) == len("pt:") {
		return -1
	}
	index := 0
	for _, r := range regionID[3:] {
		if r < '0' || r > '9' {
			return -1
		}
		index = index*10 + int(r-'0')
	}
	return index
}

func (s *embeddingScene) load(ctx context.Context, sess *Session, class *models.Class) {
	s.points = nil
	s.failure = ""
	s.selected = -1
	s.hovered = -1

	getter := sess.client.data.ObjectsGetter().
		WithClassName(class.Class).
		WithVector().
		WithLimit(sess.opts.VectorSample)
	objects, err := getter.Do(ctx)
	if err != nil {
		s.failure = mapError(err).Error()
		s.status = "Could not load embeddings"
		return
	}
	labelKey := labelProperty(class)
	vectors := make([][]float32, 0, len(objects))
	labels := make([]string, 0, len(objects))
	ids := make([]string, 0, len(objects))
	for _, object := range objects {
		if object == nil {
			continue
		}
		vector := s.vectorFor(object)
		if len(vector) < 2 {
			continue
		}
		vectors = append(vectors, vector)
		ids = append(ids, string(object.ID))
		labels = append(labels, objectLabel(object, labelKey))
	}
	if len(vectors) < 3 {
		s.status = fmt.Sprintf("%s has fewer than 3 objects with vectors", class.Class)
		return
	}
	dims := len(vectors[0])
	for _, vector := range vectors {
		if len(vector) != dims {
			s.status = "Objects have mixed vector dimensions; pick a named vector"
			return
		}
	}
	coords, variance := projectPCA(vectors)
	s.variance = variance
	assignments, clusters := kmeans(coords, min(embeddingClusters, len(coords)))
	s.clusters = clusters
	s.points = make([]embeddingPoint, 0, len(coords))
	for i, coord := range coords {
		s.points = append(s.points, embeddingPoint{
			ID: ids[i], Label: labels[i], X: coord[0], Y: coord[1], Cluster: assignments[i],
		})
	}
	s.status = fmt.Sprintf("%d objects · %d dims · %d clusters", len(s.points), dims, clusters)
}

func (s *embeddingScene) vectorFor(object *models.Object) []float32 {
	if s.targetVector != "" {
		vector, _ := toFloat32Slice(object.Vectors[s.targetVector])
		return vector
	}
	return objectVector(object)
}

func objectLabel(object *models.Object, key string) string {
	properties, _ := object.Properties.(map[string]any)
	if key != "" {
		if value := strings.TrimSpace(text(properties[key])); value != "" {
			return truncate(value, 48)
		}
	}
	return summaryLabel(properties, object.Class, string(object.ID))
}

func (s *embeddingScene) colors() palette {
	if s.theme == plugin.PanelThemeLight {
		return lightPalette
	}
	return darkPalette
}

func (s *embeddingScene) frame() canvas.Frame {
	colors := s.colors()
	commands := []canvas.Command{canvas.Clear{Color: colors.Background}}
	commands = append(commands, s.header(colors)...)

	plotTop, plotBottom := 84.0, s.height-72
	plotLeft, plotRight := 64.0, s.width-24
	if plotBottom-plotTop < 60 || plotRight-plotLeft < 60 {
		return canvas.Frame{Commands: commands}
	}
	commands = append(commands, s.axes(colors, plotLeft, plotTop, plotRight, plotBottom)...)

	if len(s.points) == 0 {
		commands = append(commands, canvas.FillText{
			Paint: canvas.Paint{Fill: colors.Muted, Font: "14px sans-serif", TextBaseline: canvas.TextBaselineMiddle, TextAlign: canvas.TextAlignCenter},
			X:     (plotLeft + plotRight) / 2, Y: (plotTop + plotBottom) / 2,
			Text: emptyMessage(s),
		})
		return canvas.Frame{Commands: append(commands, s.announce()...)}
	}

	centerX, centerY := (plotLeft+plotRight)/2, (plotTop+plotBottom)/2
	scale := math.Min(plotRight-plotLeft, plotBottom-plotTop) / 2 * 0.9 * s.zoom
	regions := make([]canvas.Region, 0, len(s.points))
	for i := range s.points {
		point := &s.points[i]
		point.screenX = centerX + point.X*scale
		point.screenY = centerY - point.Y*scale
	}

	for i, point := range s.points {
		radius := 4.0
		if i == s.hovered {
			radius = 6
		}
		if i == s.selected {
			radius = 7
		}
		alpha := 0.85
		commands = append(commands, canvas.Circle{
			Paint: canvas.Paint{Fill: clusterColor(colors, point.Cluster), Alpha: &alpha},
			X:     point.screenX, Y: point.screenY, Radius: radius,
		})
		if i == s.selected {
			commands = append(commands, canvas.Circle{
				Paint: canvas.Paint{Stroke: colors.Text, LineWidth: 2},
				X:     point.screenX, Y: point.screenY, Radius: radius + 5,
			})
		}
		regions = append(regions, canvas.CircleRegion(
			fmt.Sprintf("pt:%d", i), point.screenX, point.screenY, math.Max(radius+4, 9),
			canvas.WithCursor("pointer"),
			canvas.WithLabel(fmt.Sprintf("%s · cluster %d · %s", shortID(point.ID), point.Cluster+1, point.Label)),
		))
	}

	if tip := s.tooltip(colors); tip != nil {
		commands = append(commands, tip...)
	}
	commands = append(commands, s.legend(colors, plotLeft, s.height-44)...)
	commands = append(commands, s.announce()...)
	return canvas.Frame{Commands: commands, Regions: regions}
}

func emptyMessage(s *embeddingScene) string {
	if s.failure != "" {
		return s.failure
	}
	return s.status
}

func (s *embeddingScene) header(colors palette) []canvas.Command {
	title := s.className + " embedding projection"
	if s.targetVector != "" {
		title += " · " + s.targetVector
	}
	return []canvas.Command{
		canvas.Rect{Paint: canvas.Paint{Fill: colors.Surface}, X: 0, Y: 0, Width: s.width, Height: 64},
		canvas.FillText{
			Paint: canvas.Paint{Fill: colors.Text, Font: "600 17px sans-serif", TextBaseline: canvas.TextBaselineMiddle},
			X:     24, Y: 26, Text: title,
		},
		canvas.FillText{
			Paint: canvas.Paint{Fill: colors.Muted, Font: "12px sans-serif", TextBaseline: canvas.TextBaselineMiddle},
			X:     24, Y: 47, Text: emptyMessage(s) + "  ·  arrows select, R reloads, +/- zooms",
		},
	}
}

func (s *embeddingScene) axes(colors palette, left, top, right, bottom float64) []canvas.Command {
	centerX, centerY := (left+right)/2, (top+bottom)/2
	commands := []canvas.Command{
		canvas.Rect{Paint: canvas.Paint{Stroke: colors.Border, LineWidth: 1}, X: left, Y: top, Width: right - left, Height: bottom - top, Radius: 8},
		canvas.Line{Paint: canvas.Paint{Stroke: colors.Grid, LineWidth: 1}, X1: left, Y1: centerY, X2: right, Y2: centerY},
		canvas.Line{Paint: canvas.Paint{Stroke: colors.Grid, LineWidth: 1}, X1: centerX, Y1: top, X2: centerX, Y2: bottom},
		canvas.FillText{
			Paint: canvas.Paint{Fill: colors.Muted, Font: "11px sans-serif", TextAlign: canvas.TextAlignRight},
			X:     right - 8, Y: centerY - 8, Text: fmt.Sprintf("PC1 · %.1f%% variance", s.variance[0]*100),
		},
		canvas.FillText{
			Paint: canvas.Paint{Fill: colors.Muted, Font: "11px sans-serif"},
			X:     centerX + 8, Y: top + 16, Text: fmt.Sprintf("PC2 · %.1f%% variance", s.variance[1]*100),
		},
	}
	return commands
}

func (s *embeddingScene) tooltip(colors palette) []canvas.Command {
	index := s.selected
	if index < 0 {
		index = s.hovered
	}
	if index < 0 || index >= len(s.points) {
		return nil
	}
	point := s.points[index]
	width, height := 300.0, 62.0
	x := math.Min(math.Max(point.screenX+14, 8), math.Max(8, s.width-width-8))
	y := math.Min(math.Max(point.screenY-height-10, 72), math.Max(72, s.height-height-8))
	return []canvas.Command{canvas.TextBox{
		Paint: canvas.Paint{Fill: colors.Text, Font: "12px sans-serif"},
		X:     x, Y: y, Width: width, Height: height,
		Padding:    10,
		LineHeight: 16,
		MaxLines:   3,
		Ellipsis:   "…",
		Background: colors.Surface,
		Radius:     8,
		Text:       fmt.Sprintf("%s\n%s\ncluster %d · PC1 %.3f · PC2 %.3f", point.ID, point.Label, point.Cluster+1, point.X, point.Y),
	}}
}

func (s *embeddingScene) legend(colors palette, x, y float64) []canvas.Command {
	commands := make([]canvas.Command, 0, s.clusters*2+1)
	commands = append(commands, canvas.FillText{
		Paint: canvas.Paint{Fill: colors.Muted, Font: "11px sans-serif", TextBaseline: canvas.TextBaselineMiddle},
		X:     x, Y: y, Text: "clusters",
	})
	offset := x + 60
	for cluster := 0; cluster < s.clusters; cluster++ {
		commands = append(commands,
			canvas.Circle{Paint: canvas.Paint{Fill: clusterColor(colors, cluster)}, X: offset, Y: y, Radius: 5},
			canvas.FillText{
				Paint: canvas.Paint{Fill: colors.Muted, Font: "11px sans-serif", TextBaseline: canvas.TextBaselineMiddle},
				X:     offset + 10, Y: y, Text: fmt.Sprintf("%d", cluster+1),
			},
		)
		offset += 34
	}
	return commands
}

func (s *embeddingScene) announce() []canvas.Command {
	if s.announcement == "" {
		return nil
	}
	text := s.announcement
	s.announcement = ""
	return []canvas.Command{canvas.Announce{Text: text, Mode: canvas.AnnouncePolite}}
}

func clusterColor(colors palette, cluster int) string {
	if cluster < 0 {
		cluster = 0
	}
	return colors.Clusters[cluster%len(colors.Clusters)]
}

// projectPCA reduces vectors to two dimensions with power iteration on the
// covariance matrix, returning coordinates normalised to [-1,1] and the fraction
// of total variance each component explains.
func projectPCA(vectors [][]float32) ([][2]float64, [2]float64) {
	rows := len(vectors)
	dims := len(vectors[0])
	centred := make([][]float64, rows)
	mean := make([]float64, dims)
	for i, vector := range vectors {
		centred[i] = make([]float64, dims)
		for j, value := range vector {
			centred[i][j] = float64(value)
			mean[j] += float64(value)
		}
	}
	for j := range mean {
		mean[j] /= float64(rows)
	}
	total := 0.0
	for i := range centred {
		for j := range centred[i] {
			centred[i][j] -= mean[j]
			total += centred[i][j] * centred[i][j]
		}
	}
	total /= float64(rows)

	first, firstVar := principalComponent(centred)
	residual := make([][]float64, rows)
	for i := range centred {
		residual[i] = append([]float64(nil), centred[i]...)
	}
	deflate(residual, first)
	second, secondVar := principalComponent(residual)

	coords := make([][2]float64, rows)
	maxAbs := 0.0
	for i, sample := range centred {
		x, y := dot(sample, first), dot(sample, second)
		coords[i] = [2]float64{x, y}
		maxAbs = math.Max(maxAbs, math.Max(math.Abs(x), math.Abs(y)))
	}
	if maxAbs > 0 {
		for i := range coords {
			coords[i][0] /= maxAbs
			coords[i][1] /= maxAbs
		}
	}
	variance := [2]float64{}
	if total > 0 {
		variance[0] = firstVar / total
		variance[1] = secondVar / total
	}
	return coords, variance
}

func principalComponent(data [][]float64) ([]float64, float64) {
	dims := len(data[0])
	vector := make([]float64, dims)
	for i := range vector {
		vector[i] = 1 / math.Sqrt(float64(dims))
	}
	scratch := make([]float64, dims)
	for iteration := 0; iteration < pcaIterations; iteration++ {
		for i := range scratch {
			scratch[i] = 0
		}
		for _, sample := range data {
			projection := dot(sample, vector)
			for j, value := range sample {
				scratch[j] += projection * value
			}
		}
		length := math.Sqrt(dot(scratch, scratch))
		if length == 0 {
			break
		}
		for i := range vector {
			vector[i] = scratch[i] / length
		}
	}
	eigenvalue := 0.0
	for _, sample := range data {
		projection := dot(sample, vector)
		eigenvalue += projection * projection
	}
	return vector, eigenvalue / float64(len(data))
}

func deflate(data [][]float64, component []float64) {
	for _, sample := range data {
		projection := dot(sample, component)
		for j := range sample {
			sample[j] -= projection * component[j]
		}
	}
}

func dot(a, b []float64) float64 {
	sum := 0.0
	for i := range a {
		sum += a[i] * b[i]
	}
	return sum
}

// kmeans groups the projected points so visually distinct regions get distinct
// colours. Seeding is deterministic so repeated loads render the same palette.
func kmeans(points [][2]float64, k int) ([]int, int) {
	if k < 1 {
		k = 1
	}
	if k > len(points) {
		k = len(points)
	}
	centroids := make([][2]float64, 0, k)
	ordered := make([]int, len(points))
	for i := range ordered {
		ordered[i] = i
	}
	sort.Slice(ordered, func(i, j int) bool {
		a, b := points[ordered[i]], points[ordered[j]]
		if a[0] != b[0] {
			return a[0] < b[0]
		}
		return a[1] < b[1]
	})
	for i := 0; i < k; i++ {
		centroids = append(centroids, points[ordered[i*len(points)/k]])
	}

	assignments := make([]int, len(points))
	for iteration := 0; iteration < kmeansIterations; iteration++ {
		changed := false
		for i, point := range points {
			best, bestDistance := 0, math.Inf(1)
			for c, centroid := range centroids {
				distance := squared(point, centroid)
				if distance < bestDistance {
					best, bestDistance = c, distance
				}
			}
			if assignments[i] != best {
				assignments[i] = best
				changed = true
			}
		}
		sums := make([][2]float64, k)
		counts := make([]int, k)
		for i, point := range points {
			sums[assignments[i]][0] += point[0]
			sums[assignments[i]][1] += point[1]
			counts[assignments[i]]++
		}
		for c := range centroids {
			if counts[c] == 0 {
				continue
			}
			centroids[c] = [2]float64{sums[c][0] / float64(counts[c]), sums[c][1] / float64(counts[c])}
		}
		if !changed {
			break
		}
	}
	return assignments, k
}

func squared(a, b [2]float64) float64 {
	dx, dy := a[0]-b[0], a[1]-b[1]
	return dx*dx + dy*dy
}

func norm(vector []float32) float64 {
	sum := 0.0
	for _, value := range vector {
		sum += float64(value) * float64(value)
	}
	return math.Sqrt(sum)
}
