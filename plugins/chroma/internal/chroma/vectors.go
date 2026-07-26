package chroma

import (
	"math"
	"sort"
)

const (
	spaceL2     = "l2"
	spaceCosine = "cosine"
	spaceIP     = "ip"
)

func spaces() []string { return []string{spaceL2, spaceCosine, spaceIP} }

func spaceLabel(space string) string {
	switch space {
	case spaceCosine:
		return "Cosine"
	case spaceIP:
		return "Inner product"
	default:
		return "Squared L2"
	}
}

func norm(v []float64) float64 {
	total := 0.0
	for _, x := range v {
		total += x * x
	}
	return math.Sqrt(total)
}

func dot(a, b []float64) float64 {
	n := min(len(a), len(b))
	total := 0.0
	for i := 0; i < n; i++ {
		total += a[i] * b[i]
	}
	return total
}

// distance mirrors Chroma's space semantics: lower is closer for every space,
// including inner product, which Chroma reports as 1 - dot.
func distance(space string, a, b []float64) float64 {
	switch space {
	case spaceCosine:
		na, nb := norm(a), norm(b)
		if na == 0 || nb == 0 {
			return 1
		}
		return 1 - dot(a, b)/(na*nb)
	case spaceIP:
		return 1 - dot(a, b)
	default:
		n := min(len(a), len(b))
		total := 0.0
		for i := 0; i < n; i++ {
			d := a[i] - b[i]
			total += d * d
		}
		return total
	}
}

type scored struct {
	Index    int
	Distance float64
}

// rank orders every vector except skip by distance from query, closest first.
func rank(space string, vectors [][]float64, query []float64, skip int, limit int) []scored {
	out := make([]scored, 0, len(vectors))
	for i, v := range vectors {
		if i == skip || len(v) == 0 {
			continue
		}
		out = append(out, scored{Index: i, Distance: distance(space, query, v)})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Distance < out[j].Distance })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

type point2D struct {
	X float64
	Y float64
}

type projection struct {
	Points   []point2D
	Variance [2]float64
	Total    float64
	Fallback bool
}

// Explained returns the fraction of total variance captured by each axis.
func (p projection) Explained(axis int) float64 {
	if p.Total <= 0 {
		return 0
	}
	return p.Variance[axis] / p.Total
}

// project2D reduces embeddings to two dimensions with a deterministic power
// iteration PCA so repeated opens of the same collection render the same map.
func project2D(vectors [][]float64) projection {
	rows := len(vectors)
	if rows == 0 {
		return projection{}
	}
	dim := 0
	for _, v := range vectors {
		if len(v) > dim {
			dim = len(v)
		}
	}
	if dim == 0 {
		return projection{Points: make([]point2D, rows)}
	}
	centered := make([][]float64, rows)
	mean := make([]float64, dim)
	for i, v := range vectors {
		row := make([]float64, dim)
		copy(row, v)
		centered[i] = row
		for j, x := range row {
			mean[j] += x
		}
	}
	for j := range mean {
		mean[j] /= float64(rows)
	}
	total := 0.0
	for _, v := range centered {
		for j := range v {
			v[j] -= mean[j]
			total += v[j] * v[j]
		}
	}
	total /= float64(rows)
	if dim == 1 || total == 0 {
		points := make([]point2D, rows)
		for i, v := range centered {
			points[i] = point2D{X: v[0], Y: secondOrZero(v)}
		}
		return projection{Points: points, Total: total, Fallback: true}
	}

	first, lambda1 := principalAxis(centered, nil)
	second, lambda2 := principalAxis(centered, first)
	points := make([]point2D, rows)
	for i, v := range centered {
		points[i] = point2D{X: dot(v, first), Y: dot(v, second)}
	}
	return projection{Points: points, Variance: [2]float64{lambda1, lambda2}, Total: total}
}

func secondOrZero(v []float64) float64 {
	if len(v) > 1 {
		return v[1]
	}
	return 0
}

// principalAxis runs power iteration on the covariance matrix implicitly, so a
// 2000x1536 sample never materializes a dense d*d matrix.
func principalAxis(centered [][]float64, deflate []float64) ([]float64, float64) {
	dim := len(centered[0])
	vec := seedVector(dim)
	if deflate != nil {
		orthogonalize(vec, deflate)
	}
	scratch := make([]float64, dim)
	lambda := 0.0
	for iter := 0; iter < 96; iter++ {
		for j := range scratch {
			scratch[j] = 0
		}
		for _, row := range centered {
			p := dot(row, vec)
			if p == 0 {
				continue
			}
			for j, x := range row {
				scratch[j] += p * x
			}
		}
		if deflate != nil {
			orthogonalize(scratch, deflate)
		}
		length := norm(scratch)
		if length == 0 {
			break
		}
		lambda = length / float64(len(centered))
		for j := range scratch {
			vec[j] = scratch[j] / length
		}
	}
	return vec, lambda
}

func orthogonalize(vec, basis []float64) {
	p := dot(vec, basis)
	for j := range vec {
		vec[j] -= p * basis[j]
	}
}

// seedVector is a fixed pseudo-random unit vector; a constant vector can be
// orthogonal to the leading eigenvector of symmetric data.
func seedVector(dim int) []float64 {
	out := make([]float64, dim)
	state := uint64(0x9e3779b97f4a7c15)
	for i := range out {
		state = state*6364136223846793005 + 1442695040888963407
		out[i] = float64(int64(state>>11))/float64(int64(1)<<52) - 0.5
	}
	length := norm(out)
	if length == 0 {
		out[0] = 1
		return out
	}
	for i := range out {
		out[i] /= length
	}
	return out
}

// cluster assigns deterministic k-means labels used to color the embedding map.
func cluster(vectors [][]float64, space string, k int) []int {
	labels := make([]int, len(vectors))
	if k <= 1 || len(vectors) <= k {
		for i := range labels {
			labels[i] = i % max(k, 1)
		}
		return labels
	}
	centroids := make([][]float64, 0, k)
	centroids = append(centroids, clone(vectors[0]))
	for len(centroids) < k {
		best, bestDist := -1, -1.0
		for i, v := range vectors {
			nearest := math.MaxFloat64
			for _, c := range centroids {
				if d := distance(space, v, c); d < nearest {
					nearest = d
				}
			}
			if nearest > bestDist {
				best, bestDist = i, nearest
			}
		}
		if best < 0 {
			break
		}
		centroids = append(centroids, clone(vectors[best]))
	}
	dim := len(centroids[0])
	for iter := 0; iter < 24; iter++ {
		changed := false
		for i, v := range vectors {
			best, bestDist := 0, math.MaxFloat64
			for c, centroid := range centroids {
				if d := distance(space, v, centroid); d < bestDist {
					best, bestDist = c, d
				}
			}
			if labels[i] != best {
				labels[i] = best
				changed = true
			}
		}
		sums := make([][]float64, len(centroids))
		counts := make([]int, len(centroids))
		for c := range sums {
			sums[c] = make([]float64, dim)
		}
		for i, v := range vectors {
			counts[labels[i]]++
			for j := 0; j < min(dim, len(v)); j++ {
				sums[labels[i]][j] += v[j]
			}
		}
		for c := range centroids {
			if counts[c] == 0 {
				continue
			}
			for j := range centroids[c] {
				centroids[c][j] = sums[c][j] / float64(counts[c])
			}
		}
		if !changed {
			break
		}
	}
	return labels
}

func clone(v []float64) []float64 {
	out := make([]float64, len(v))
	copy(out, v)
	return out
}

// spearman measures rank agreement between two neighbor orderings; it drives the
// distance-space comparison summary.
func spearman(a, b []string) float64 {
	if len(a) < 2 {
		return 0
	}
	positions := make(map[string]int, len(b))
	for i, id := range b {
		positions[id] = i
	}
	n := 0
	sum := 0.0
	for i, id := range a {
		j, ok := positions[id]
		if !ok {
			continue
		}
		d := float64(i - j)
		sum += d * d
		n++
	}
	if n < 2 {
		return 0
	}
	size := float64(n)
	return 1 - (6*sum)/(size*(size*size-1))
}
