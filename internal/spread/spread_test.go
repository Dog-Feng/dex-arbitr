package spread

import (
	"math"
	"testing"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
)

func TestComputeRejectsInvalidTopLevels(t *testing.T) {
	for _, tc := range []struct {
		name           string
		bid, ask, size float64
	}{
		{"negative price", -2, -1, 1}, {"zero price", 0, 1, 1}, {"nan price", math.NaN(), 2, 1}, {"infinite price", 1, math.Inf(1), 1}, {"negative depth", 1, 2, -1}, {"nan depth", 1, 2, math.NaN()}, {"infinite depth", 1, 2, math.Inf(1)}, {"crossed", 2, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := domain.Book{Bids: []domain.Level{{Price: tc.bid, Size: tc.size}}, Asks: []domain.Level{{Price: tc.ask, Size: tc.size}}}
			if _, ok := Compute(b, b); ok {
				t.Fatal("accepted invalid top levels")
			}
		})
	}
}
