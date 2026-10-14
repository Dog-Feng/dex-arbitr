package engine

import (
	"math"
	"testing"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/symbols"
)

func bk(bid, ask, bidSize, askSize float64) domain.Book {
	return domain.Book{
		Bids: []domain.Level{{Price: bid, Size: bidSize}},
		Asks: []domain.Level{{Price: ask, Size: askSize}},
	}
}

func TestCandidateSizedQty(t *testing.T) {
	cases := []struct {
		name       string
		notional   float64
		depthRatio float64
		sizes      [4]float64
		prices     [4]float64
		want       float64
	}{
		{
			name: "deep book takes full notional", notional: 1000, depthRatio: 0.25,
			sizes: [4]float64{50, 50, 50, 50}, prices: [4]float64{778.0, 778.1, 778.05, 778.15},
			want: domain.Floor(1000/778.075, 4),
		},
		{
			name: "thin book caps by depth", notional: 1000, depthRatio: 0.25,
			sizes: [4]float64{2, 2, 0.5, 2}, prices: [4]float64{1700.0, 1700.1, 1700.05, 1700.15},
			want: domain.Floor(0.25*0.5*1700.075/1700.075, 4),
		},
		{
			name: "below min qty is rejected", notional: 1, depthRatio: 0.25,
			sizes: [4]float64{50, 50, 50, 50}, prices: [4]float64{778.0, 778.1, 778.05, 778.15},
			want: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &candidate{
				pair:       symbols.Pair{SizeDecimals: 4, MinQty: 0.0050},
				bookL:      bk(tc.prices[0], tc.prices[1], tc.sizes[0], tc.sizes[1]),
				bookR:      bk(tc.prices[2], tc.prices[3], tc.sizes[2], tc.sizes[3]),
				notional:   tc.notional,
				depthRatio: tc.depthRatio,
			}
			got, ok := c.sizedQty()
			if tc.want == 0 {
				if ok {
					t.Fatalf("expected rejection, got qty=%v", got)
				}
				return
			}
			if !ok {
				t.Fatalf("expected ok, got rejected")
			}
			if math.Abs(got-tc.want) > 1e-9 {
				t.Fatalf("qty=%v, want %v", got, tc.want)
			}
		})
	}
}
