package scylladb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugin/canvas"
)

const (
	ringTop      = -math.Pi / 2
	tokenSpan    = 18446744073709551616.0 // 2^64, the Murmur3 token space
	minRingSlots = 180
	maxRingSlots = 720
)

type topologyNode struct {
	Address      string
	DataCenter   string
	Rack         string
	Status       string
	Up           bool
	Local        bool
	Shards       int
	MSBIgnore    uint64
	TokenCount   int
	OwnsPct      float64
	LoadBytes    int64
	Connections  int
	ShardTokens  []int
	ShardClients []int
}

type ringBoundary struct {
	Token int64
	Node  int
}

type topologyData struct {
	ClusterName string
	Nodes       []topologyNode
	Boundaries  []ringBoundary
	Sampled     time.Time
}

// topologySnapshot builds the ring and shard picture from the driver's live view
// of tokens and shards plus the coordinator's client table.
func topologySnapshot(ctx context.Context, s *Session) (topologyData, error) {
	facts, err := clusterFacts(ctx, s)
	if err != nil {
		return topologyData{}, err
	}
	snapshot, err := clusterSnapshot(ctx, s)
	if err != nil {
		return topologyData{}, err
	}
	// The shard wheel only claims client counts for the node it read them from.
	local, shardClients := "", map[int]int{}
	for _, f := range facts {
		if f.Local {
			local = f.Address
			shardClients = clientsByShard(ctx, s, f.HostID)
			break
		}
	}
	data := topologyData{ClusterName: fmt.Sprint(snapshot["cluster_name"]), Sampled: time.Now()}
	for i, f := range facts {
		node := topologyNode{
			Address:     f.Address,
			DataCenter:  f.DataCenter,
			Rack:        f.Rack,
			Status:      f.Status,
			Up:          f.Up,
			Local:       f.Local,
			Shards:      max(f.Shards, 1),
			MSBIgnore:   f.MSBIgnore,
			TokenCount:  f.TokenCount,
			OwnsPct:     f.OwnsPct,
			LoadBytes:   f.LoadBytes,
			Connections: f.DriverConnections,
		}
		node.ShardTokens = make([]int, node.Shards)
		for _, token := range f.Tokens {
			node.ShardTokens[shardOf(token, node.Shards, node.MSBIgnore)]++
			data.Boundaries = append(data.Boundaries, ringBoundary{Token: token, Node: i})
		}
		if f.Address == local {
			node.ShardClients = make([]int, node.Shards)
			for shard, count := range shardClients {
				if shard >= 0 && shard < node.Shards {
					node.ShardClients[shard] = count
				}
			}
		}
		data.Nodes = append(data.Nodes, node)
	}
	sort.Slice(data.Boundaries, func(i, j int) bool { return data.Boundaries[i].Token < data.Boundaries[j].Token })
	return data, nil
}

// ownerAt returns the node owning the range that ends at the first token at or
// after the probe, wrapping around the ring like the partitioner does.
func (d topologyData) ownerAt(token int64) int {
	if len(d.Boundaries) == 0 {
		return -1
	}
	i := sort.Search(len(d.Boundaries), func(i int) bool { return d.Boundaries[i].Token >= token })
	if i == len(d.Boundaries) {
		i = 0
	}
	return d.Boundaries[i].Node
}

func tokenAtFraction(frac float64) int64 {
	value := frac*tokenSpan + float64(math.MinInt64)
	switch {
	case value >= float64(math.MaxInt64):
		return math.MaxInt64
	case value <= float64(math.MinInt64):
		return math.MinInt64
	default:
		return int64(value)
	}
}

type palette struct {
	Background string
	Surface    string
	SurfaceAlt string
	Text       string
	Muted      string
	Border     string
	Accent     string
	Down       string
	Nodes      []string
}

func themePalette(theme plugin.PanelTheme) palette {
	if theme == plugin.PanelThemeLight {
		return palette{
			Background: "#f8fafc", Surface: "#ffffff", SurfaceAlt: "#eef2f7",
			Text: "#0f172a", Muted: "#64748b", Border: "#cbd5e1", Accent: "#0f766e", Down: "#b91c1c",
			Nodes: []string{"#0d9488", "#2563eb", "#b45309", "#7c3aed", "#be123c", "#0891b2", "#4d7c0f", "#c2410c"},
		}
	}
	return palette{
		Background: "#020617", Surface: "#0f172a", SurfaceAlt: "#1e293b",
		Text: "#e2e8f0", Muted: "#94a3b8", Border: "#334155", Accent: "#2dd4bf", Down: "#f87171",
		Nodes: []string{"#2dd4bf", "#60a5fa", "#fbbf24", "#c084fc", "#fb7185", "#22d3ee", "#a3e635", "#fb923c"},
	}
}

func (p palette) node(i int) string {
	if len(p.Nodes) == 0 {
		return p.Accent
	}
	return p.Nodes[i%len(p.Nodes)]
}

type topologyScene struct {
	theme         plugin.PanelTheme
	width, height float64
	data          topologyData
	node          int
	shard         int
	announcement  string
}

func (sc *topologyScene) clampSelection() {
	if len(sc.data.Nodes) == 0 {
		sc.node, sc.shard = 0, 0
		return
	}
	sc.node = ((sc.node % len(sc.data.Nodes)) + len(sc.data.Nodes)) % len(sc.data.Nodes)
	shards := sc.data.Nodes[sc.node].Shards
	sc.shard = ((sc.shard % shards) + shards) % shards
}

func (sc *topologyScene) selectNodeByAddress(address string) {
	for i, node := range sc.data.Nodes {
		if node.Address == address {
			sc.node, sc.shard = i, 0
			return
		}
	}
}

func (sc *topologyScene) handle(ev canvas.Event) bool {
	switch e := ev.(type) {
	case *canvas.ReadyEvent:
		sc.theme, sc.width, sc.height = e.Theme, e.Width, e.Height
		return true
	case *canvas.ResizeEvent:
		sc.theme, sc.width, sc.height = e.Theme, e.Width, e.Height
		return true
	case *canvas.PointerEvent:
		if e.Event != canvas.PointerDown || e.RegionID == "" {
			return false
		}
		return sc.selectRegion(e.RegionID)
	case *canvas.KeyEvent:
		if e.Event != "keydown" {
			return false
		}
		return sc.handleKey(e.Key)
	}
	return false
}

func (sc *topologyScene) selectRegion(id string) bool {
	kind, value, _ := strings.Cut(id, ":")
	switch kind {
	case "node":
		for i, node := range sc.data.Nodes {
			if node.Address == value {
				sc.node, sc.shard = i, 0
				sc.announce()
				return true
			}
		}
	case "shard":
		index := intValue(value)
		if index >= 0 && index < sc.currentShards() {
			sc.shard = index
			sc.announce()
			return true
		}
	}
	return false
}

func (sc *topologyScene) handleKey(key string) bool {
	switch key {
	case "ArrowRight":
		sc.node, sc.shard = sc.node+1, 0
	case "ArrowLeft":
		sc.node, sc.shard = sc.node-1, 0
	case "ArrowDown":
		sc.shard++
	case "ArrowUp":
		sc.shard--
	case "Home":
		for i, node := range sc.data.Nodes {
			if node.Local {
				sc.node, sc.shard = i, 0
				break
			}
		}
	default:
		return false
	}
	sc.clampSelection()
	sc.announce()
	return true
}

func (sc *topologyScene) currentShards() int {
	if len(sc.data.Nodes) == 0 {
		return 1
	}
	return sc.data.Nodes[sc.node].Shards
}

func (sc *topologyScene) announce() {
	if len(sc.data.Nodes) == 0 {
		return
	}
	node := sc.data.Nodes[sc.node]
	sc.announcement = fmt.Sprintf("Node %s in %s/%s, status %s, %d shards, %d tokens. Shard %d owns %d token ranges.",
		node.Address, node.DataCenter, node.Rack, node.Status, node.Shards, node.TokenCount, sc.shard, shardTokenCount(node, sc.shard))
}

func shardTokenCount(node topologyNode, shard int) int {
	if shard < 0 || shard >= len(node.ShardTokens) {
		return 0
	}
	return node.ShardTokens[shard]
}

func (sc *topologyScene) frame() canvas.Frame {
	colors := themePalette(sc.theme)
	width, height := sc.width, sc.height
	if width <= 0 || height <= 0 {
		width, height = 960, 600
	}
	commands := []canvas.Command{canvas.Clear{Color: colors.Background}}
	regions := []canvas.Region{}

	legendWidth := 0.0
	if width >= 780 {
		legendWidth = math.Min(320, width*0.34)
	}
	headerHeight := 56.0
	footerHeight := 26.0
	ringWidth := width - legendWidth
	ringCX := ringWidth / 2
	ringCY := headerHeight + (height-headerHeight-footerHeight)/2
	radius := math.Min(ringWidth, height-headerHeight-footerHeight)/2 - 18
	if radius < 40 {
		radius = 40
	}

	commands = append(commands, sc.header(colors, width, headerHeight)...)
	if len(sc.data.Nodes) == 0 {
		commands = append(commands, canvas.FillText{
			Paint: canvas.Paint{Fill: colors.Muted, Font: "500 15px ui-sans-serif, system-ui, sans-serif", TextAlign: canvas.TextAlignCenter, TextBaseline: canvas.TextBaselineMiddle},
			X:     width / 2, Y: height / 2, Text: "No ring information available from this cluster.",
		})
		return canvas.Frame{Commands: commands}
	}

	ringCommands, ringRegions := sc.ring(colors, ringCX, ringCY, radius)
	commands = append(commands, ringCommands...)
	regions = append(regions, ringRegions...)

	shardCommands, shardRegions := sc.shardWheel(colors, ringCX, ringCY, radius)
	commands = append(commands, shardCommands...)
	regions = append(regions, shardRegions...)

	commands = append(commands, sc.center(colors, ringCX, ringCY, radius)...)

	if legendWidth > 0 {
		legendCommands, legendRegions := sc.legend(colors, width-legendWidth, headerHeight, legendWidth, height-headerHeight-footerHeight)
		commands = append(commands, legendCommands...)
		regions = append(regions, legendRegions...)
	}

	commands = append(commands, canvas.FillText{
		Paint: canvas.Paint{Fill: colors.Muted, Font: "400 11px ui-sans-serif, system-ui, sans-serif", TextBaseline: canvas.TextBaselineBottom},
		X:     16, Y: height - 8,
		Text: "Left/right arrows select a node · up/down arrows select a shard · Home selects the coordinator",
	})
	if sc.announcement != "" {
		commands = append(commands, canvas.Announce{Text: sc.announcement, Mode: canvas.AnnouncePolite})
		sc.announcement = ""
	}
	return canvas.Frame{Commands: commands, Regions: regions}
}

func (sc *topologyScene) header(colors palette, width, height float64) []canvas.Command {
	nodes, shards, tokens := len(sc.data.Nodes), 0, 0
	up := 0
	for _, node := range sc.data.Nodes {
		shards += node.Shards
		tokens += node.TokenCount
		if node.Up {
			up++
		}
	}
	return []canvas.Command{
		canvas.Rect{Paint: canvas.Paint{Fill: colors.Surface}, X: 0, Y: 0, Width: width, Height: height},
		canvas.Line{Paint: canvas.Paint{Stroke: colors.Border, LineWidth: 1}, X1: 0, Y1: height, X2: width, Y2: height},
		canvas.FillText{
			Paint: canvas.Paint{Fill: colors.Text, Font: "600 16px ui-sans-serif, system-ui, sans-serif", TextBaseline: canvas.TextBaselineMiddle},
			X:     16, Y: height/2 - 8, Text: sc.data.ClusterName,
		},
		canvas.FillText{
			Paint: canvas.Paint{Fill: colors.Muted, Font: "400 12px ui-sans-serif, system-ui, sans-serif", TextBaseline: canvas.TextBaselineMiddle},
			X:     16, Y: height/2 + 12,
			Text: fmt.Sprintf("%d/%d nodes up · %d shards · %d tokens · sampled %s", up, nodes, shards, tokens, sc.data.Sampled.UTC().Format("15:04:05")+" UTC"),
		},
	}
}

// ring buckets the token space into fixed-width slots so the drawing stays a
// bounded number of arcs regardless of how many vnodes the cluster has.
func (sc *topologyScene) ring(colors palette, cx, cy, radius float64) ([]canvas.Command, []canvas.Region) {
	band := math.Max(10, radius*0.16)
	mid := radius - band/2
	slots := int(math.Round(2 * math.Pi * mid / 3))
	slots = min(max(slots, minRingSlots), maxRingSlots)

	commands := []canvas.Command{
		canvas.Arc{Paint: canvas.Paint{Stroke: colors.SurfaceAlt, LineWidth: band, NoFill: true}, X: cx, Y: cy, Radius: mid, StartAngle: 0, EndAngle: 2 * math.Pi},
	}
	regions := []canvas.Region{}
	start, owner := 0, sc.data.ownerAt(tokenAtFraction(0))
	flush := func(end int) {
		if owner < 0 {
			return
		}
		startAngle := ringTop + 2*math.Pi*float64(start)/float64(slots)
		endAngle := ringTop + 2*math.Pi*float64(end)/float64(slots)
		alpha := 1.0
		if owner != sc.node {
			alpha = 0.55
		}
		commands = append(commands, canvas.Arc{
			Paint:      canvas.Paint{Stroke: colors.node(owner), LineWidth: band, NoFill: true, Alpha: &alpha, LineCap: canvas.LineCapButt},
			X:          cx,
			Y:          cy,
			Radius:     mid,
			StartAngle: startAngle,
			EndAngle:   endAngle,
		})
		node := sc.data.Nodes[owner]
		// One hit target per contiguous arc keeps the ring clickable without
		// emitting a region per vnode.
		regions = append(regions, canvas.PolygonRegion(
			"node:"+node.Address,
			arcPolygon(cx, cy, mid-band/2, mid+band/2, startAngle, endAngle),
			canvas.WithCursor("pointer"),
			canvas.WithLabel(fmt.Sprintf("Token range owned by %s", node.Address)),
		))
	}
	for slot := 1; slot < slots; slot++ {
		next := sc.data.ownerAt(tokenAtFraction((float64(slot) + 0.5) / float64(slots)))
		if next == owner {
			continue
		}
		flush(slot)
		start, owner = slot, next
	}
	flush(slots)
	return commands, regions
}

// arcPolygon approximates an annulus sector so pointer hit testing follows the
// drawn band.
func arcPolygon(cx, cy, inner, outer, startAngle, endAngle float64) []canvas.Point {
	steps := max(2, int(math.Ceil((endAngle-startAngle)/(math.Pi/36))))
	points := make([]canvas.Point, 0, (steps+1)*2)
	for i := 0; i <= steps; i++ {
		angle := startAngle + (endAngle-startAngle)*float64(i)/float64(steps)
		points = append(points, canvas.Point{X: cx + outer*math.Cos(angle), Y: cy + outer*math.Sin(angle)})
	}
	for i := steps; i >= 0; i-- {
		angle := startAngle + (endAngle-startAngle)*float64(i)/float64(steps)
		points = append(points, canvas.Point{X: cx + inner*math.Cos(angle), Y: cy + inner*math.Sin(angle)})
	}
	return points
}

func (sc *topologyScene) shardWheel(colors palette, cx, cy, radius float64) ([]canvas.Command, []canvas.Region) {
	node := sc.data.Nodes[sc.node]
	outer := radius * 0.74
	inner := radius * 0.5
	if outer-inner < 8 {
		inner = math.Max(6, outer-8)
	}
	band := outer - inner
	mid := (outer + inner) / 2
	peak := 1
	for _, count := range node.ShardTokens {
		peak = max(peak, count)
	}
	commands := []canvas.Command{
		canvas.Arc{Paint: canvas.Paint{Stroke: colors.SurfaceAlt, LineWidth: band, NoFill: true}, X: cx, Y: cy, Radius: mid, StartAngle: 0, EndAngle: 2 * math.Pi},
	}
	regions := make([]canvas.Region, 0, node.Shards)
	step := 2 * math.Pi / float64(node.Shards)
	gap := math.Min(step*0.06, 0.05)
	for shard := range node.Shards {
		startAngle := ringTop + step*float64(shard) + gap/2
		endAngle := ringTop + step*float64(shard+1) - gap/2
		alpha := 0.35 + 0.65*float64(shardTokenCount(node, shard))/float64(peak)
		commands = append(commands, canvas.Arc{
			Paint: canvas.Paint{Stroke: colors.node(sc.node), LineWidth: band, NoFill: true, Alpha: &alpha, LineCap: canvas.LineCapButt},
			X:     cx, Y: cy, Radius: mid, StartAngle: startAngle, EndAngle: endAngle,
		})
		if shard == sc.shard {
			commands = append(commands,
				canvas.Arc{Paint: canvas.Paint{Stroke: colors.Text, LineWidth: 2, NoFill: true}, X: cx, Y: cy, Radius: outer + 2, StartAngle: startAngle, EndAngle: endAngle},
			)
		}
		if node.Shards <= 32 && band >= 14 {
			labelAngle := (startAngle + endAngle) / 2
			commands = append(commands, canvas.FillText{
				Paint: canvas.Paint{Fill: colors.Background, Font: "600 10px ui-monospace, monospace", TextAlign: canvas.TextAlignCenter, TextBaseline: canvas.TextBaselineMiddle},
				X:     cx + mid*math.Cos(labelAngle), Y: cy + mid*math.Sin(labelAngle),
				Text: fmt.Sprintf("%d", shard),
			})
		}
		label := fmt.Sprintf("Shard %d of %s: %d token ranges", shard, node.Address, shardTokenCount(node, shard))
		if shard < len(node.ShardClients) {
			label += fmt.Sprintf(", %d client connections", node.ShardClients[shard])
		}
		regions = append(regions, canvas.PolygonRegion(
			fmt.Sprintf("shard:%d", shard),
			arcPolygon(cx, cy, inner, outer, startAngle, endAngle),
			canvas.WithCursor("pointer"),
			canvas.WithLabel(label),
		))
	}
	return commands, regions
}

func (sc *topologyScene) center(colors palette, cx, cy, radius float64) []canvas.Command {
	node := sc.data.Nodes[sc.node]
	lines := []string{
		node.Address,
		fmt.Sprintf("%s / %s · %s", node.DataCenter, node.Rack, node.Status),
		fmt.Sprintf("shard %d of %d · %d token ranges", sc.shard, node.Shards, shardTokenCount(node, sc.shard)),
		fmt.Sprintf("%d tokens · %.1f%% of ring", node.TokenCount, node.OwnsPct),
		fmt.Sprintf("%s load · %d driver connections", humanBytes(node.LoadBytes), node.Connections),
	}
	if sc.shard < len(node.ShardClients) {
		lines = append(lines, fmt.Sprintf("%d client connections on this shard", node.ShardClients[sc.shard]))
	}
	statusColor := colors.Accent
	if !node.Up {
		statusColor = colors.Down
	}
	commands := []canvas.Command{
		canvas.Circle{Paint: canvas.Paint{Fill: colors.Surface, Stroke: colors.Border, LineWidth: 1}, X: cx, Y: cy, Radius: radius * 0.46},
		canvas.Circle{Paint: canvas.Paint{Fill: statusColor}, X: cx, Y: cy - radius*0.32, Radius: 4},
	}
	if radius*0.46 < 54 {
		return commands
	}
	fonts := []string{
		"600 14px ui-sans-serif, system-ui, sans-serif",
		"400 11px ui-sans-serif, system-ui, sans-serif",
		"400 11px ui-sans-serif, system-ui, sans-serif",
		"400 11px ui-sans-serif, system-ui, sans-serif",
		"400 11px ui-sans-serif, system-ui, sans-serif",
		"400 11px ui-sans-serif, system-ui, sans-serif",
	}
	top := cy - float64(len(lines)-1)*8
	for i, line := range lines {
		fill := colors.Muted
		if i == 0 {
			fill = colors.Text
		}
		commands = append(commands, canvas.FillText{
			Paint: canvas.Paint{Fill: fill, Font: fonts[min(i, len(fonts)-1)], TextAlign: canvas.TextAlignCenter, TextBaseline: canvas.TextBaselineMiddle},
			X:     cx, Y: top + float64(i)*16, Text: line,
		})
	}
	return commands
}

func (sc *topologyScene) legend(colors palette, x, y, width, height float64) ([]canvas.Command, []canvas.Region) {
	commands := []canvas.Command{
		canvas.Rect{Paint: canvas.Paint{Fill: colors.Surface}, X: x, Y: y, Width: width, Height: height},
		canvas.Line{Paint: canvas.Paint{Stroke: colors.Border, LineWidth: 1}, X1: x, Y1: y, X2: x, Y2: y + height},
		canvas.FillText{
			Paint: canvas.Paint{Fill: colors.Muted, Font: "600 11px ui-sans-serif, system-ui, sans-serif", TextBaseline: canvas.TextBaselineTop},
			X:     x + 16, Y: y + 14, Text: "NODES",
		},
	}
	regions := make([]canvas.Region, 0, len(sc.data.Nodes))
	rowHeight := 54.0
	top := y + 36
	for i, node := range sc.data.Nodes {
		rowY := top + float64(i)*rowHeight
		if rowY+rowHeight > y+height {
			commands = append(commands, canvas.FillText{
				Paint: canvas.Paint{Fill: colors.Muted, Font: "400 11px ui-sans-serif, system-ui, sans-serif", TextBaseline: canvas.TextBaselineTop},
				X:     x + 16, Y: rowY, Text: fmt.Sprintf("+%d more nodes", len(sc.data.Nodes)-i),
			})
			break
		}
		if i == sc.node {
			commands = append(commands, canvas.Rect{Paint: canvas.Paint{Fill: colors.SurfaceAlt}, X: x + 8, Y: rowY - 6, Width: width - 16, Height: rowHeight - 4, Radius: 8})
		}
		commands = append(commands,
			canvas.Rect{Paint: canvas.Paint{Fill: colors.node(i)}, X: x + 16, Y: rowY + 2, Width: 10, Height: 10, Radius: 3},
			canvas.FillText{
				Paint: canvas.Paint{Fill: colors.Text, Font: "600 12px ui-sans-serif, system-ui, sans-serif", TextBaseline: canvas.TextBaselineTop},
				X:     x + 34, Y: rowY, Text: node.Address,
			},
			canvas.FillText{
				Paint: canvas.Paint{Fill: colors.Muted, Font: "400 11px ui-sans-serif, system-ui, sans-serif", TextBaseline: canvas.TextBaselineTop},
				X:     x + 34, Y: rowY + 17,
				Text: fmt.Sprintf("%s/%s · %s · %d shards", node.DataCenter, node.Rack, node.Status, node.Shards),
			},
			canvas.FillText{
				Paint: canvas.Paint{Fill: colors.Muted, Font: "400 11px ui-sans-serif, system-ui, sans-serif", TextBaseline: canvas.TextBaselineTop},
				X:     x + 34, Y: rowY + 32,
				Text: fmt.Sprintf("%d tokens · %.1f%% ring · %s", node.TokenCount, node.OwnsPct, humanBytes(node.LoadBytes)),
			},
		)
		regions = append(regions, canvas.RectRegion(
			"node:"+node.Address,
			x+8, rowY-6, width-16, rowHeight-4,
			canvas.WithCursor("pointer"),
			canvas.WithLabel("Select node "+node.Address),
		))
	}
	return commands, regions
}

// topologyStream drives the token-ring canvas: it reads browser input events on
// one goroutine, refreshes the cluster picture on a timer, and only redraws when
// something actually changed.
func topologyStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, err := scyllaSession(rc)
	if err != nil {
		return err
	}
	scene := &topologyScene{theme: plugin.PanelThemeDark}
	if data, err := topologySnapshot(rc.Ctx, s); err == nil {
		scene.data = data
		scene.selectLocal()
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

	refresh := time.NewTicker(5 * time.Second)
	defer refresh.Stop()
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
			if errors.Is(err, io.EOF) || client.Context().Err() != nil {
				return nil
			}
			return err
		case ev := <-events:
			dirty = scene.handle(ev)
		case <-refresh.C:
			data, err := topologySnapshot(rc.Ctx, s)
			if err != nil {
				continue
			}
			selected := ""
			if len(scene.data.Nodes) > 0 && scene.node < len(scene.data.Nodes) {
				selected = scene.data.Nodes[scene.node].Address
			}
			scene.data = data
			if selected != "" {
				scene.selectNodeByAddress(selected)
			}
			scene.clampSelection()
			dirty = true
		}
	}
}

func (sc *topologyScene) selectLocal() {
	for i, node := range sc.data.Nodes {
		if node.Local {
			sc.node = i
			return
		}
	}
}
