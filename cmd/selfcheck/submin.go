package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
	"github.com/Dog-Feng/dex-arbitr/internal/execution"
	"github.com/Dog-Feng/dex-arbitr/internal/symbols"
)

// subminFlow verifies the live venue behavior the flatten round-up relies on:
// when a residual is below the venue's minimum order value, a minimum-sized
// reduce-only order must be accepted and its fill capped at the remaining
// position. It only trades when the market starts flat and ends with a
// best-effort flatten back to zero.
func subminFlow(ctx context.Context, v exchange.Venue, m symbols.Matcher, base string, qty float64, out io.Writer) error {
	if !v.CanTrade() {
		return fmt.Errorf("%s has no trading credentials", v.Name())
	}
	insts, err := v.Instruments(ctx)
	if err != nil {
		return fmt.Errorf("instruments: %w", err)
	}
	norm := m.Normalize(base)
	inst, ok := domain.Instrument{}, false
	for _, i := range insts {
		if m.Normalize(i.Symbol) == norm || strings.EqualFold(i.Symbol, base) {
			inst, ok = i, true
			break
		}
	}
	if !ok {
		return fmt.Errorf("instrument %q not found on %s", base, v.Name())
	}
	fmt.Fprintf(out, "venue=%s symbol=%s market=%d size_decimals=%d price_decimals=%d min_quote=%g\n",
		v.Name(), inst.Symbol, inst.MarketID, inst.SizeDecimals, inst.PriceDecimals, inst.MinQuote)

	eps := math.Pow10(-inst.SizeDecimals) / 2
	pos, err := execution.MarketPosition(ctx, v, inst.MarketID)
	if err != nil {
		return fmt.Errorf("position read: %w", err)
	}
	if math.Abs(pos) >= eps {
		return fmt.Errorf("market not flat (position %.8f): flatten manually before the sub-min test", pos)
	}
	if qty <= 0 || math.IsNaN(qty) || math.IsInf(qty, 0) {
		return fmt.Errorf("qty must be positive")
	}
	minUSD := inst.MinQuote
	if minUSD <= 0 {
		minUSD = 10
	}

	bctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	books, err := v.SubscribeBook(bctx, inst)
	if err != nil {
		return fmt.Errorf("book stream: %w", err)
	}
	var book domain.Book
	for !book.Valid() {
		select {
		case <-bctx.Done():
			return fmt.Errorf("no book snapshot within 20s")
		case snap, ok := <-books:
			if !ok {
				return fmt.Errorf("book stream closed")
			}
			if snap.Valid() {
				book = snap
			}
		}
	}
	buyPx := domain.Ceil(book.Asks[0].Price*1.005, inst.PriceDecimals)
	sellPx := domain.Floor(book.Bids[0].Price*0.995, inst.PriceDecimals)
	fmt.Fprintf(out, "book bid=%.6f ask=%.6f buy_px=%.6f sell_px=%.6f\n", book.Bids[0].Price, book.Asks[0].Price, buyPx, sellPx)

	step := math.Pow10(-inst.SizeDecimals)
	minQty := domain.Ceil(minUSD/sellPx, inst.SizeDecimals)
	leave := domain.Round(qty-minQty, inst.SizeDecimals)
	if leave >= minQty-eps {
		leave -= step
	}
	reduce := domain.Round(qty-leave, inst.SizeDecimals)
	if leave <= eps || leave >= minQty-eps || reduce < minQty-eps {
		return fmt.Errorf("qty %v cannot form a tradable sub-min residual (min %.8f): pick a larger qty", qty, minQty)
	}
	fmt.Fprintf(out, "plan: min_qty=%.5f leave=%.5f reduce=%.5f\n", minQty, leave, reduce)

	ids := execution.NewIDGen()
	submit := func(side domain.Side, q float64, reduceOnly bool) error {
		px := buyPx
		if side == domain.Sell {
			px = sellPx
		}
		_, err := v.Submit(ctx, domain.OrderReq{
			ClientID: ids.Next(), Inst: inst, Side: side, Qty: q,
			Price: px, Type: domain.Limit, ReduceOnly: reduceOnly,
		})
		return err
	}
	check := func(name string, err error, detail string, args ...any) bool {
		mark := "PASS"
		if err != nil {
			mark = "FAIL"
		}
		fmt.Fprintf(out, "[%s] %-34s %s\n", mark, name, fmt.Sprintf(detail, args...))
		return err == nil
	}

	allOK := true
	for _, dir := range []struct {
		label       string
		open, close domain.Side
	}{{"long", domain.Buy, domain.Sell}, {"short", domain.Sell, domain.Buy}} {
		px := buyPx
		if dir.open == domain.Sell {
			px = sellPx
		}
		dirMinQty := domain.Ceil(minUSD/px, inst.SizeDecimals)
		fmt.Fprintf(out, "== %s residual: open %.5f, reduce %.5f, min_qty %.5f ==\n", dir.label, qty, reduce, dirMinQty)
		if err := submit(dir.open, qty, false); err != nil {
			allOK = check(dir.label+" open", err, "%v", err) && allOK
		} else {
			allOK = check(dir.label+" open", nil, "qty %.5f", qty) && allOK
		}
		want := qty * float64(dir.open)
		pos, err = awaitPosition(ctx, v, inst, want, 25*time.Second)
		allOK = check(dir.label+" open reflected", err, "position %.8f", pos) && allOK

		if err := submit(dir.close, reduce, true); err != nil {
			allOK = check(dir.label+" reduce to residual", err, "%v", err) && allOK
		} else {
			allOK = check(dir.label+" reduce to residual", nil, "reduced %.5f -> leave %.5f", reduce, leave) && allOK
		}
		want = leave * float64(dir.open)
		pos, err = awaitPosition(ctx, v, inst, want, 25*time.Second)
		allOK = check(dir.label+" residual position", err, "position %.8f want %.8f", pos, want) && allOK

		// Key check: a minimum-sized reduce-only order must be accepted and
		// its fill capped at the remaining sub-minimum position (no overshoot).
		if err := submit(dir.close, dirMinQty, true); err != nil {
			allOK = check(dir.label+" min-sized reduce-only accepted", err, "%v", err) && allOK
		} else {
			allOK = check(dir.label+" min-sized reduce-only accepted", nil, "qty %.5f (~$%.2f)", dirMinQty, dirMinQty*px) && allOK
		}
		pos, err = awaitPosition(ctx, v, inst, 0, 25*time.Second)
		allOK = check(dir.label+" capped fill closed residual", err, "position %.8f", pos) && allOK

		// Rebuild the same residual to record the exact-close behavior.
		if err := submit(dir.open, qty, false); err != nil {
			allOK = check(dir.label+" reopen", err, "%v", err) && allOK
		}
		pos, err = awaitPosition(ctx, v, inst, qty*float64(dir.open), 25*time.Second)
		allOK = check(dir.label+" reopen reflected", err, "position %.8f", pos) && allOK
		if err := submit(dir.close, reduce, true); err != nil {
			allOK = check(dir.label+" re-reduce", err, "%v", err) && allOK
		}
		pos, err = awaitPosition(ctx, v, inst, leave*float64(dir.open), 25*time.Second)
		allOK = check(dir.label+" re-reduce reflected", err, "position %.8f", pos) && allOK
		rejectErr := submit(dir.close, leave, true)
		if errors.Is(rejectErr, domain.ErrRejected) {
			allOK = check(dir.label+" exact residual rejected", nil, "%v", rejectErr) && allOK
		} else if rejectErr != nil {
			allOK = check(dir.label+" exact residual rejected", rejectErr, "%v", rejectErr) && allOK
		} else {
			fmt.Fprintf(out, "[INFO] %s: exact residual close was accepted by the venue\n", dir.label)
		}

		pos, err = awaitPosition(ctx, v, inst, 0, 25*time.Second)
		if err != nil {
			if err := submit(dir.close, dirMinQty, true); err != nil {
				allOK = check(dir.label+" residual cleanup", err, "%v", err) && allOK
			} else {
				pos, err = awaitPosition(ctx, v, inst, 0, 25*time.Second)
				allOK = check(dir.label+" residual cleanup", err, "position %.8f", pos) && allOK
			}
		} else {
			allOK = check(dir.label+" exact residual closed", nil, "position %.8f", pos) && allOK
		}
	}
	if err := flattenToZero(ctx, v, inst, minQty, buyPx, sellPx, out); err != nil {
		allOK = check("cleanup", err, "%v", err) && allOK
	}
	if !allOK {
		return fmt.Errorf("sub-min verification failed")
	}
	return nil
}

func awaitPosition(ctx context.Context, v exchange.Venue, inst domain.Instrument, want float64, wait time.Duration) (float64, error) {
	deadline := time.Now().Add(wait)
	var pos float64
	var err error
	interval := time.NewTimer(1500 * time.Millisecond)
	defer interval.Stop()
	for {
		pos, err = execution.MarketPosition(ctx, v, inst.MarketID)
		if err != nil {
			return pos, err
		}
		eps := math.Pow10(-inst.SizeDecimals) / 2
		if math.Abs(pos-want) < eps {
			return pos, nil
		}
		if time.Now().After(deadline) {
			return pos, fmt.Errorf("position %.8f did not reach %.8f within %s", pos, want, wait)
		}
		select {
		case <-ctx.Done():
			return pos, ctx.Err()
		case <-interval.C:
			interval.Reset(1500 * time.Millisecond)
		}
	}
}

func flattenToZero(ctx context.Context, v exchange.Venue, inst domain.Instrument, minQty, buyPx, sellPx float64, out io.Writer) error {
	ids := execution.NewIDGen()
	interval := time.NewTimer(2 * time.Second)
	defer interval.Stop()
	for attempt := 0; attempt < 8; attempt++ {
		pos, err := execution.MarketPosition(ctx, v, inst.MarketID)
		if err != nil {
			return err
		}
		eps := math.Pow10(-inst.SizeDecimals) / 2
		if math.Abs(pos) < eps {
			return nil
		}
		q := domain.Ceil(math.Abs(pos), inst.SizeDecimals)
		if q < minQty {
			q = minQty
		}
		side := domain.Sell
		px := sellPx
		if pos < 0 {
			side = domain.Buy
			px = buyPx
		}
		if _, err := v.Submit(ctx, domain.OrderReq{
			ClientID: ids.Next(), Inst: inst, Side: side, Qty: q,
			Price: px, Type: domain.Limit, ReduceOnly: true,
		}); err != nil {
			return fmt.Errorf("cleanup order: %w", err)
		}
		fmt.Fprintf(out, "[INFO] cleanup reduce %s %.5f at %.6f\n", side, q, px)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-interval.C:
			interval.Reset(2 * time.Second)
		}
	}
	return fmt.Errorf("cleanup did not flatten the account after 8 attempts")
}
