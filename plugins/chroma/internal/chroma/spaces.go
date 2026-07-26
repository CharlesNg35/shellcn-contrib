package chroma

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugin/canvas"
)

const (
	spaceRankDepth = 8
	cardHeight     = 44.0
	cardGap        = 8.0
)

func spacesStream(rc *plugin.RequestContext, client plugin.ClientStream) error {
	s, sc, col, err := collectionOf(rc)
	if err != nil {
		return err
	}
	data, err := loadSample(rc.Ctx, s, sc, col)
	if err != nil {
		return err
	}
	return runCanvas(rc, client, newSpacesScene(data))
}

// spacesScene ranks the same anchor's neighbors under every Chroma distance
// space so an operator can see whether the configured space actually changes
// retrieval for this data.
type spacesScene struct {
	data     sample
	width    float64
	height   float64
	theme    plugin.PanelTheme
	anchor   int
	focused  int
	hovered  string
	columns  [][]scored
	agree    []float64
	announce string
}

func newSpacesScene(data sample) *spacesScene {
	s := &spacesScene{data: data, width: 960, height: 540, anchor: -1}
	if data.len() > 1 {
		s.setAnchor(0)
	}
	return s
}

func (s *spacesScene) setAnchor(index int) bool {
	if index < 0 || index >= s.data.len() || index == s.anchor {
		return false
	}
	s.anchor = index
	s.columns = make([][]scored, len(spaces()))
	s.agree = make([]float64, len(spaces()))
	configured := s.data.Collection.space()
	baseline := []string{}
	for i, space := range spaces() {
		s.columns[i] = rank(space, s.data.Vectors, s.data.Vectors[index], index, spaceRankDepth)
		if space == configured {
			baseline = s.ids(s.columns[i])
		}
	}
	for i := range s.columns {
		s.agree[i] = spearman(s.ids(s.columns[i]), baseline)
	}
	s.announce = fmt.Sprintf("Anchor %s. Comparing %d neighbors across %d distance spaces.",
		s.data.id(index), spaceRankDepth, len(spaces()))
	return true
}

func (s *spacesScene) ids(entries []scored) []string {
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		out = append(out, s.data.id(entry.Index))
	}
	return out
}

func (s *spacesScene) Handle(ev canvas.Event) bool {
	switch e := ev.(type) {
	case *canvas.ReadyEvent:
		s.theme, s.width, s.height = e.Theme, e.Width, e.Height
		return true
	case *canvas.ResizeEvent:
		s.theme, s.width, s.height = e.Theme, e.Width, e.Height
		return true
	case *canvas.PointerEvent:
		switch e.Event {
		case "down":
			if index, ok := s.cardTarget(e.RegionID); ok {
				return s.setAnchor(index)
			}
		case "move":
			if e.RegionID != s.hovered {
				s.hovered = e.RegionID
				return true
			}
		case "leave":
			if s.hovered != "" {
				s.hovered = ""
				return true
			}
		}
	case *canvas.KeyEvent:
		return s.key(e)
	}
	return false
}

func (s *spacesScene) key(e *canvas.KeyEvent) bool {
	if e.Event != "down" {
		return false
	}
	switch e.Key {
	case "ArrowLeft":
		s.focused = clampInt(s.focused-1, 0, len(spaces())-1)
		return true
	case "ArrowRight":
		s.focused = clampInt(s.focused+1, 0, len(spaces())-1)
		return true
	case "ArrowDown":
		return s.stepAnchor(1)
	case "ArrowUp":
		return s.stepAnchor(-1)
	case "Enter", " ":
		if s.focused < len(s.columns) && len(s.columns[s.focused]) > 0 {
			return s.setAnchor(s.columns[s.focused][0].Index)
		}
	case "r", "R":
		return s.setAnchor(0)
	}
	return false
}

func (s *spacesScene) stepAnchor(delta int) bool {
	if s.data.len() == 0 {
		return false
	}
	return s.setAnchor(clampInt(s.anchor+delta, 0, s.data.len()-1))
}

func (s *spacesScene) cardTarget(id string) (int, bool) {
	if !strings.HasPrefix(id, "card:") {
		return 0, false
	}
	parts := strings.Split(id, ":")
	if len(parts) != 3 {
		return 0, false
	}
	column, err := strconv.Atoi(parts[1])
	if err != nil || column < 0 || column >= len(s.columns) {
		return 0, false
	}
	rankIndex, err := strconv.Atoi(parts[2])
	if err != nil || rankIndex < 0 || rankIndex >= len(s.columns[column]) {
		return 0, false
	}
	return s.columns[column][rankIndex].Index, true
}

func (s *spacesScene) Frame() canvas.Frame {
	c := paletteFor(s.theme)
	commands := []canvas.Command{canvas.Clear{Color: c.Background}}
	commands = append(commands,
		canvas.Text{Paint: canvas.Paint{Fill: c.Text, Font: fontTitle}, X: 24, Y: 30,
			Text: s.data.Collection.Name + " — distance space comparison"},
	)
	if s.anchor < 0 {
		commands = append(commands, canvas.TextBox{
			Paint: canvas.Paint{Fill: c.Muted, Font: fontBody},
			X:     24, Y: 56, Width: math.Max(s.width-48, 160), Height: 44,
			Text: "At least two records with stored embeddings are needed to compare distance spaces.",
		})
		return canvas.Frame{Commands: commands}
	}

	commands = append(commands,
		canvas.Text{Paint: canvas.Paint{Fill: c.Muted, Font: fontBody}, X: 24, Y: 50,
			Text: fmt.Sprintf("anchor %s · %s · %d vectors sampled · configured space: %s",
				truncate(s.data.id(s.anchor), 32), truncate(s.data.document(s.anchor), 46), s.data.len(), spaceLabel(s.data.Collection.space()))},
		canvas.Text{Paint: canvas.Paint{Fill: c.Muted, Font: fontBody, TextAlign: canvas.TextAlignRight}, X: math.Max(s.width-24, 200), Y: 50,
			Text: "click a card to re-anchor · ←/→ column · ↑/↓ anchor · r reset"},
	)

	columnWidth := math.Max((s.width-48-2*cardGap*2)/float64(len(spaces())), 150)
	top := 96.0
	regions := make([]canvas.Region, 0, len(spaces())*spaceRankDepth)
	positions := make([]map[string]float64, len(spaces()))

	for col, space := range spaces() {
		x := 24 + float64(col)*(columnWidth+cardGap*2)
		header := c.Panel
		if space == s.data.Collection.space() {
			header = c.Accent
		}
		commands = append(commands,
			canvas.Rect{Paint: canvas.Paint{Fill: header, Alpha: alpha(0.22)}, X: x, Y: top - 34, Width: columnWidth, Height: 28, Radius: 6},
			canvas.Text{Paint: canvas.Paint{Fill: c.Text, Font: fontLabel}, X: x + 10, Y: top - 15, Text: spaceLabel(space)},
			canvas.Text{Paint: canvas.Paint{Fill: c.Muted, Font: fontBody, TextAlign: canvas.TextAlignRight}, X: x + columnWidth - 10, Y: top - 15,
				Text: fmt.Sprintf("ρ %.2f", s.agree[col])},
		)
		if s.focused == col {
			commands = append(commands, canvas.Rect{
				Paint: canvas.Paint{Stroke: c.Accent, LineWidth: 1.5, NoFill: true},
				X:     x - 3, Y: top - 37, Width: columnWidth + 6, Height: 34 + float64(len(s.columns[col]))*(cardHeight+cardGap), Radius: 8,
			})
		}
		positions[col] = map[string]float64{}
		for i, entry := range s.columns[col] {
			y := top + float64(i)*(cardHeight+cardGap)
			id := s.data.id(entry.Index)
			positions[col][id] = y + cardHeight/2
			regionID := fmt.Sprintf("card:%d:%d", col, i)
			fill := c.Surface
			if s.hovered == regionID {
				fill = c.Panel
			}
			commands = append(commands,
				canvas.Rect{Paint: canvas.Paint{Fill: fill, Stroke: c.Border, LineWidth: 1}, X: x, Y: y, Width: columnWidth, Height: cardHeight, Radius: 8},
				canvas.Rect{Paint: canvas.Paint{Fill: c.series(entry.Index)}, X: x, Y: y, Width: 3, Height: cardHeight, Radii: &canvas.Radii{TopLeft: 8, BottomLeft: 8}},
				canvas.Text{Paint: canvas.Paint{Fill: c.Muted, Font: fontBody}, X: x + 12, Y: y + 17, Text: "#" + strconv.Itoa(i+1)},
				canvas.Text{Paint: canvas.Paint{Fill: c.Text, Font: fontLabel}, X: x + 38, Y: y + 17, Text: truncate(id, int(columnWidth/8))},
				canvas.Text{Paint: canvas.Paint{Fill: c.Muted, Font: fontMono}, X: x + 12, Y: y + 34,
					Text: fmt.Sprintf("d %.5f", entry.Distance)},
				canvas.Text{Paint: canvas.Paint{Fill: c.Muted, Font: fontBody, TextAlign: canvas.TextAlignRight}, X: x + columnWidth - 10, Y: y + 34,
					Text: truncate(s.data.document(entry.Index), int(columnWidth/11))},
			)
			regions = append(regions, canvas.RectRegion(regionID, x, y, columnWidth, cardHeight,
				canvas.WithCursor("pointer"),
				canvas.WithLabel(fmt.Sprintf("%s rank %d: %s, distance %.5f", spaceLabel(space), i+1, id, entry.Distance)),
			))
		}
	}

	commands = append(commands, s.ribbons(c, positions, columnWidth)...)
	commands = append(commands, s.summary(c, top)...)
	if s.announce != "" {
		commands = append(commands, canvas.Announce{Text: s.announce, Mode: canvas.AnnouncePolite})
		s.announce = ""
	}
	return canvas.Frame{Commands: commands, Regions: regions}
}

// ribbons connect the same record across adjacent columns; a steep line means
// the two spaces disagree about that neighbor's rank.
func (s *spacesScene) ribbons(c palette, positions []map[string]float64, columnWidth float64) []canvas.Command {
	out := make([]canvas.Command, 0, spaceRankDepth*2)
	for col := 0; col+1 < len(positions); col++ {
		x1 := 24 + float64(col)*(columnWidth+cardGap*2) + columnWidth
		x2 := 24 + float64(col+1)*(columnWidth+cardGap*2)
		for id, y1 := range positions[col] {
			y2, ok := positions[col+1][id]
			if !ok {
				continue
			}
			stroke := c.Accent
			if math.Abs(y1-y2) > (cardHeight+cardGap)*1.5 {
				stroke = c.Warn
			}
			out = append(out, canvas.BezierCurve{
				Paint: canvas.Paint{Stroke: stroke, LineWidth: 1.4, Alpha: alpha(0.5), NoFill: true},
				X0:    x1, Y0: y1,
				CP1X: x1 + (x2-x1)/2, CP1Y: y1,
				CP2X: x1 + (x2-x1)/2, CP2Y: y2,
				X: x2, Y: y2,
			})
		}
	}
	return out
}

func (s *spacesScene) summary(c palette, top float64) []canvas.Command {
	shared := 0
	if len(s.columns) > 1 {
		first := map[string]struct{}{}
		for _, entry := range s.columns[0] {
			first[s.data.id(entry.Index)] = struct{}{}
		}
		for _, entry := range s.columns[len(s.columns)-1] {
			if _, ok := first[s.data.id(entry.Index)]; ok {
				shared++
			}
		}
	}
	verdict := "spaces largely agree for this anchor"
	for _, value := range s.agree {
		if value < 0.6 {
			verdict = "spaces disagree — the configured space materially changes retrieval"
			break
		}
	}
	y := top + float64(spaceRankDepth)*(cardHeight+cardGap) + 16
	if y > s.height-34 {
		y = math.Max(s.height-34, top)
	}
	return []canvas.Command{canvas.TextBox{
		Paint:      canvas.Paint{Fill: c.Text, Font: fontBody},
		Background: c.Panel,
		X:          24, Y: y, Width: math.Max(s.width-48, 160), Height: 30,
		Padding: 8, LineHeight: 14, Radius: 6, MaxLines: 1,
		Text: fmt.Sprintf("ρ is Spearman rank agreement against %s · %d of %d neighbors shared between l2 and ip · %s",
			spaceLabel(s.data.Collection.space()), shared, spaceRankDepth, verdict),
	}}
}
