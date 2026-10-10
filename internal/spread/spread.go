// Package spread computes executable and mid spreads between two books.
package spread

import "github.com/Dog-Feng/dex-arbitr/internal/domain"

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
	if !l.Valid() || !r.Valid() {
		return Quote{}, false
	}
	lm, rm := l.Mid(), r.Mid()
	ref := (lm + rm) / 2
	const bps = 1e4
	return Quote{
		LS:     (r.Bids[0].Price - l.Asks[0].Price) / ref * bps,
		SL:     (l.Bids[0].Price - r.Asks[0].Price) / ref * bps,
		Mid:    (rm - lm) / ref * bps,
		RefMid: ref,
		L:      l, R: r,
	}, true
}
