// Package spread computes executable and mid spreads between two books.
package spread

import (
	"math"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
)

// Quote is the spread view of a (left, right) book pair. All values in bps.
//
//	LS  = (R.bid1 - L.ask1) / mid   executable: buy Left, sell Right
//	SL  = (L.bid1 - R.ask1) / mid   executable: buy Right, sell Left
//	Mid = (R.mid  - L.mid ) / mid   sampled into the mean estimator
type Quote struct {
	LS, SL, Mid float64
	RefMid      float64
	L, R        domain.Book
}

// Compute returns the spread quote; ok=false if either book is invalid.
func Compute(l, r domain.Book) (Quote, bool) {
	if !validTop(l) || !validTop(r) {
		return Quote{}, false
	}
	lm, rm := l.Mid(), r.Mid()
	ref := (lm + rm) / 2
	if math.IsInf(ref, 0) || ref <= 0 {
		return Quote{}, false
	}
	const bps = 1e4
	return Quote{
		LS:     (r.Bids[0].Price - l.Asks[0].Price) / ref * bps,
		SL:     (l.Bids[0].Price - r.Asks[0].Price) / ref * bps,
		Mid:    (rm - lm) / ref * bps,
		RefMid: ref,
		L:      l, R: r,
	}, true
}

func validTop(b domain.Book) bool {
	if !b.Valid() {
		return false
	}
	for _, lv := range []domain.Level{b.Bids[0], b.Asks[0]} {
		if lv.Price <= 0 || math.IsNaN(lv.Price) || math.IsInf(lv.Price, 0) || lv.Size < 0 || math.IsNaN(lv.Size) || math.IsInf(lv.Size, 0) {
			return false
		}
	}
	return true
}
