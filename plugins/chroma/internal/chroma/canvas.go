package chroma

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/charlesng35/shellcn/sdk/plugin"
	"github.com/charlesng35/shellcn/sdk/plugin/canvas"
)

type palette struct {
	Background string
	Surface    string
	Panel      string
	Grid       string
	Text       string
	Muted      string
	Border     string
	Accent     string
	Warn       string
	Series     []string
}

func paletteFor(theme plugin.PanelTheme) palette {
	series := []string{"#2563eb", "#ea580c", "#059669", "#db2777", "#7c3aed", "#0891b2"}
	if theme == plugin.PanelThemeLight {
		return palette{
			Background: "#f8fafc", Surface: "#ffffff", Panel: "#eef2f7", Grid: "#e2e8f0",
			Text: "#0f172a", Muted: "#475569", Border: "#cbd5e1", Accent: "#0284c7", Warn: "#b45309",
			Series: series,
		}
	}
	return palette{
		Background: "#0b1120", Surface: "#111827", Panel: "#0f172a", Grid: "#1e293b",
		Text: "#e2e8f0", Muted: "#94a3b8", Border: "#334155", Accent: "#38bdf8", Warn: "#fbbf24",
		Series: []string{"#60a5fa", "#fb923c", "#34d399", "#f472b6", "#a78bfa", "#22d3ee"},
	}
}

func (p palette) series(i int) string {
	return p.Series[((i%len(p.Series))+len(p.Series))%len(p.Series)]
}

const (
	fontTitle = "600 15px Inter, system-ui, sans-serif"
	fontLabel = "500 12px Inter, system-ui, sans-serif"
	fontBody  = "12px Inter, system-ui, sans-serif"
	fontMono  = "12px ui-monospace, SFMono-Regular, Menlo, monospace"
)

// sample is the bounded set of vectors both visual panels work from.
type sample struct {
	Collection collection
	Records    recordSet
	Vectors    [][]float64
	Indexes    []int
}

func loadSample(ctx context.Context, s *Session, sc scope, col collection) (sample, error) {
	body := row{
		"limit":   s.opts.Sample,
		"offset":  0,
		"include": []string{"documents", "metadatas", "embeddings"},
	}
	var set recordSet
	if err := s.do(ctx, http.MethodPost, sc.collectionPath(col.ID)+"/get", nil, body, &set); err != nil {
		return sample{}, err
	}
	out := sample{Collection: col, Records: set}
	for i := range set.IDs {
		if vector := set.embedding(i); len(vector) > 0 {
			out.Vectors = append(out.Vectors, vector)
			out.Indexes = append(out.Indexes, i)
		}
	}
	return out, nil
}

func (s sample) len() int { return len(s.Vectors) }

func (s sample) id(i int) string { return s.Records.IDs[s.Indexes[i]] }

func (s sample) document(i int) string { return s.Records.document(s.Indexes[i]) }

func (s sample) metadata(i int) map[string]any { return s.Records.metadata(s.Indexes[i]) }

// scene is the per-panel drawing state shared by the canvas stream loop.
type scene interface {
	Handle(ev canvas.Event) bool
	Frame() canvas.Frame
}

// runCanvas pumps a scene: it reads browser events on one goroutine, redraws
// only when the scene reports a change, and exits on disconnect.
func runCanvas(rc *plugin.RequestContext, client plugin.ClientStream, sc scene) error {
	events := make(chan canvas.Event, 64)
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

	if err := canvas.WriteFrame(client, sc.Frame()); err != nil {
		return err
	}
	idle := time.NewTicker(200 * time.Millisecond)
	defer idle.Stop()
	dirty := false
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
		case ev := <-events:
			if sc.Handle(ev) {
				dirty = true
			}
		case <-idle.C:
			if dirty {
				if err := canvas.WriteFrame(client, sc.Frame()); err != nil {
					return err
				}
				dirty = false
			}
		}
	}
}

func truncate(text string, limit int) string {
	text = strings.ReplaceAll(strings.TrimSpace(text), "\n", " ")
	if len([]rune(text)) <= limit {
		return text
	}
	return string([]rune(text)[:limit-1]) + "…"
}

func clampFloat(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
