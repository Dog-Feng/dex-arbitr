package engine

import (
	"math"

	"github.com/Dog-Feng/dex-arbitr/internal/store"
)

// recoveryReservation returns current exposure and a fixed reservation that
// also permits replacing any historical step with a newly sized step. Counting
// only the currently empty slots would miss replacing a small historical step
// while larger historical steps remain open.
func recoveryReservation(st store.PairState, perStep float64, maxSteps int) (used, reserved float64, priced bool) {
	priced = true
	left, right := 0.0, 0.0
	for _, step := range st.Steps {
		l, r := step.Qty*step.OpenL, step.Qty*step.OpenR
		left += l
		right += r
		reserved += math.Max(perStep, math.Max(l, r))
		if !finitePositive(step.OpenL) || !finitePositive(step.OpenR) {
			priced = false
		}
	}
	used = math.Max(left, right)
	if spare := maxSteps - len(st.Steps); spare > 0 {
		reserved += float64(spare) * perStep
	}
	return used, reserved, priced
}
