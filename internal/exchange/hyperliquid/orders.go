package hyperliquid

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// cancelNothingLeft is the exchange's reply when canceling an order that has
// already terminated. Treating it as success makes cancels idempotent.
const cancelNothingLeft = "Order was never placed, already canceled, or filled."

// submitExpiry returns the expiresAfter timestamp (ms) attached to signed
// actions. An action that reaches the exchange after this instant is rejected
// instead of executed, so a later timeout can be resolved definitively.
func submitExpiry(now time.Time) uint64 { return uint64(now.Add(30 * time.Second).UnixMilli()) }

// nonce returns a strictly increasing millisecond timestamp for signing.
func (v *Venue) nonce() uint64 {
	for {
		n := uint64(time.Now().UnixMilli())
		last := uint64(v.lastNonce.Load())
		if n <= last {
			n = last + 1
		}
		if v.lastNonce.CompareAndSwap(int64(last), int64(n)) {
			return n
		}
	}
}

func (v *Venue) lockSend(mu chan struct{}, ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-v.ws.done:
		return fmt.Errorf("venue closed")
	case mu <- struct{}{}:
	}
	if v.closed.Load() || ctx.Err() != nil {
		<-mu
		if err := ctx.Err(); err != nil {
			return err
		}
		return fmt.Errorf("venue closed")
	}
	return nil
}

// orderWire validates and renders the wire form of one order.
func orderWire(r domain.OrderReq) (asset int, c string, price, size string, qty float64, err error) {
	if r.Inst.MarketID < 0 {
		return 0, "", "", "", 0, fmt.Errorf("%w: invalid market id", domain.ErrRejected)
	}
	if r.Side != domain.Buy && r.Side != domain.Sell {
		return 0, "", "", "", 0, fmt.Errorf("%w: invalid order side", domain.ErrRejected)
	}
	if r.Qty <= 0 || math.IsNaN(r.Qty) || math.IsInf(r.Qty, 0) {
		return 0, "", "", "", 0, fmt.Errorf("%w: invalid order quantity", domain.ErrRejected)
	}
	if r.Price <= 0 || math.IsNaN(r.Price) || math.IsInf(r.Price, 0) {
		return 0, "", "", "", 0, fmt.Errorf("%w: invalid order price", domain.ErrRejected)
	}
	// Round to native precision in the tightening direction so the submitted
	// bound can never be wider than the requested worst-acceptable price.
	px := r.Price
	if r.Side == domain.Buy {
		px = domain.Floor(px, r.Inst.PriceDecimals)
	} else {
		px = domain.Ceil(px, r.Inst.PriceDecimals)
	}
	if px <= 0 {
		return 0, "", "", "", 0, fmt.Errorf("%w: order price below native precision", domain.ErrRejected)
	}
	// Perp prices carry at most 5 significant figures; round in the
	// tightening direction so the submitted bound is never widened.
	px = sigFigTighten(px, r.Inst.PriceDecimals, r.Side == domain.Buy)
	qt := domain.Floor(r.Qty, r.Inst.SizeDecimals)
	if qt <= 0 {
		return 0, "", "", "", 0, fmt.Errorf("%w: quantity below native precision", domain.ErrRejected)
	}
	c, err = cloid(r.ClientID)
	if err != nil {
		return 0, "", "", "", 0, fmt.Errorf("%w: %v", domain.ErrRejected, err)
	}
	return r.Inst.MarketID, c, sigFmt(px), sigFmt(qt), qt, nil
}

// sigFigTighten rounds px to at most 5 significant figures without ever
// widening the bound: buys floor, sells ceil. The rounding grid never exceeds
// the instrument's price precision.
func sigFigTighten(px float64, priceDecimals int, buy bool) float64 {
	if px <= 0 || math.IsNaN(px) || math.IsInf(px, 0) {
		return px
	}
	digits := int(math.Floor(math.Log10(px))) + 1
	d := 5 - digits
	if d < 0 {
		d = 0 // integer prices are always allowed; never truncate the units
	}
	if d > priceDecimals {
		d = priceDecimals
	}
	if buy {
		return domain.Floor(px, d)
	}
	return domain.Ceil(px, d)
}

// Submit signs and sends an IOC limit order.
func (v *Venue) Submit(ctx context.Context, r domain.OrderReq) (domain.SubmitAck, error) {
	if v.signer == nil || v.cfg.Account == "" {
		return domain.SubmitAck{}, domain.ErrNoCredentials
	}
	asset, c, price, size, qty, err := orderWire(r)
	if err != nil {
		return domain.SubmitAck{}, err
	}
	if err := v.lockSend(v.orderSendMu, ctx); err != nil {
		return domain.SubmitAck{}, fmt.Errorf("%w: %v", domain.ErrRejected, err)
	}
	defer func() { <-v.orderSendMu }()
	nonce := v.nonce()
	expiresAfter := submitExpiry(time.Now())
	sig, err := v.signer.signOrder(asset, r.Side == domain.Buy, r.ReduceOnly, price, size, c, nonce, expiresAfter)
	if err != nil {
		return domain.SubmitAck{}, fmt.Errorf("%w: sign: %v", domain.ErrRejected, err)
	}
	// Record the identity and expiry before sending: a transport error or
	// timeout returns before any oid is known, and only the recorded expiry
	// lets a later unknownOid resolve as a definitive rejection.
	v.ordersMu.Lock()
	v.orderExpires[r.ClientID] = int64(expiresAfter)
	v.knownCIDs[r.ClientID] = time.Now()
	v.ordersMu.Unlock()
	resp, err := v.rest.send(ctx, exchangeRequest{
		Action: map[string]any{
			"type": "order",
			"orders": []map[string]any{{
				"a": asset, "b": r.Side == domain.Buy, "p": price, "s": size,
				"r": r.ReduceOnly, "t": map[string]any{"limit": map[string]any{"tif": "Ioc"}},
				"c": c,
			}},
			"grouping": "na",
		},
		Nonce:        nonce,
		Signature:    sig,
		ExpiresAfter: expiresAfter,
	})
	if err != nil {
		var hs *httpStatusError
		if errors.As(err, &hs) && hs.code == http.StatusTooManyRequests {
			slog.Warn("order rate limited; nothing executed", "venue", v.cfg.Name, "cid", r.ClientID)
			return domain.SubmitAck{ClientID: r.ClientID}, fmt.Errorf("%w: rate limited", domain.ErrRejected)
		}
		slog.Warn("order send outcome unknown", "venue", v.cfg.Name, "cid", r.ClientID, "err", err)
		return domain.SubmitAck{ClientID: r.ClientID}, err
	}
	statuses := resp.orderStatuses()
	if resp.Status != "ok" {
		// An action-level rejection means the order was definitively not
		// executed; report it as rejected so the executor can roll back.
		return domain.SubmitAck{ClientID: r.ClientID}, fmt.Errorf("%w: %s", domain.ErrRejected, rawResponse(resp))
	}
	if statuses == nil {
		return domain.SubmitAck{ClientID: r.ClientID}, fmt.Errorf("unexpected response shape: outcome unknown")
	}
	if len(statuses) != 1 {
		return domain.SubmitAck{ClientID: r.ClientID}, fmt.Errorf("unexpected status count %d: outcome unknown", len(statuses))
	}
	st := statuses[0]
	if st.Error != nil {
		return domain.SubmitAck{ClientID: r.ClientID}, fmt.Errorf("%w: %s", domain.ErrRejected, *st.Error)
	}
	var oid int64
	switch {
	case st.Resting != nil:
		oid, err = strconv.ParseInt(st.Resting.Oid.String(), 10, 64)
	case st.Filled != nil:
		oid, err = strconv.ParseInt(st.Filled.Oid.String(), 10, 64)
	default:
		return domain.SubmitAck{ClientID: r.ClientID}, fmt.Errorf("empty status: outcome unknown")
	}
	if err != nil {
		return domain.SubmitAck{ClientID: r.ClientID}, fmt.Errorf("invalid oid: outcome unknown")
	}
	v.ordersMu.Lock()
	v.orderLookup[oid] = r.ClientID
	v.orderOrig[oid] = qty
	v.drainPending(oid)
	if st.Filled != nil {
		// An IOC that filled synchronously carries its exact fill. Publish the
		// terminal update immediately so the executor does not wait for the
		// user stream. The fill state keeps the synchronous total separate
		// from per-trade fills so later deliveries of the same fills can only
		// be deduplicated, never double counted.
		filledQty, qtyErr := number(st.Filled.TotalSz)
		avgPx, pxErr := number(st.Filled.AvgPx)
		if qtyErr == nil && pxErr == nil && filledQty > 0 && avgPx > 0 {
			fs := v.fillState(oid)
			fs.syncQty, fs.syncQuote = filledQty, filledQty*avgPx
			effQty, effQuote, effFee := fs.effective()
			v.markFinal(oid, partialIOC(domain.StatusFilled, effQty, qty))
			v.publishOrder(domain.OrderUpdate{
				Venue: v.cfg.Name, ClientID: r.ClientID, OrderIndex: oid,
				Status: v.orderFinal[oid], FilledQty: effQty, FilledQuote: effQuote, Fee: effFee,
				Ts: time.Now(),
			})
		}
	}
	v.ordersMu.Unlock()
	return domain.SubmitAck{ClientID: r.ClientID, TxHash: strconv.FormatInt(oid, 10)}, nil
}

// CancelOrder cancels the outstanding quantity of an order by cloid.
func (v *Venue) CancelOrder(ctx context.Context, inst domain.Instrument, clientID int64) error {
	if v.signer == nil || v.cfg.Account == "" {
		return domain.ErrNoCredentials
	}
	if inst.MarketID < 0 {
		return fmt.Errorf("%w: invalid market id", domain.ErrRejected)
	}
	c, err := cloid(clientID)
	if err != nil {
		return fmt.Errorf("%w: %v", domain.ErrRejected, err)
	}
	if err := v.lockSend(v.auxSendMu, ctx); err != nil {
		return err
	}
	defer func() { <-v.auxSendMu }()
	nonce := v.nonce()
	expiresAfter := submitExpiry(time.Now())
	sig, err := v.signer.signCancelByCloid(inst.MarketID, c, nonce, expiresAfter)
	if err != nil {
		return fmt.Errorf("%w: sign: %v", domain.ErrRejected, err)
	}
	resp, err := v.rest.send(ctx, exchangeRequest{
		Action: map[string]any{
			"type":    "cancelByCloid",
			"cancels": []map[string]any{{"asset": inst.MarketID, "cloid": c}},
		},
		Nonce:        nonce,
		Signature:    sig,
		ExpiresAfter: expiresAfter,
	})
	if err != nil {
		var hs *httpStatusError
		if errors.As(err, &hs) && hs.code == http.StatusTooManyRequests {
			return fmt.Errorf("%w: rate limited", domain.ErrRejected)
		}
		slog.Warn("cancel send outcome unknown", "venue", v.cfg.Name, "cid", clientID, "err", err)
		return err
	}
	if resp.Status != "ok" {
		// An order that already terminated has nothing left to cancel; that
		// is a successful idempotent cancel, not an error.
		if strings.Contains(rawResponse(resp), cancelNothingLeft) {
			slog.Debug("cancel of already-terminal order", "venue", v.cfg.Name, "cid", clientID)
			return nil
		}
		return fmt.Errorf("cancelByCloid rejected: %s", rawResponse(resp))
	}
	// A cancel response lists per-order statuses. An explicit per-order error
	// is surfaced except for the idempotent "already terminal" reply.
	for _, st := range resp.orderStatuses() {
		if st.Error != nil {
			if strings.Contains(*st.Error, cancelNothingLeft) {
				slog.Debug("cancel of already-terminal order", "venue", v.cfg.Name, "cid", clientID)
				continue
			}
			return fmt.Errorf("cancelByCloid rejected: %s", *st.Error)
		}
	}
	return nil
}

// QueryOrder resolves the terminal state of one order by cloid. A fill size
// is only reported when the fills endpoint confirms it; a terminal report
// without fills is returned as unknown so callers fall back to position
// inference instead of trusting a zero.
func (v *Venue) QueryOrder(ctx context.Context, inst domain.Instrument, clientID int64) (domain.OrderUpdate, error) {
	if v.cfg.Account == "" {
		return domain.OrderUpdate{}, domain.ErrNoCredentials
	}
	c, err := cloid(clientID)
	if err != nil {
		return domain.OrderUpdate{}, fmt.Errorf("invalid client id: %w", err)
	}
	st, err := v.rest.orderStatus(ctx, v.cfg.Account, v.cfg.Dex, c)
	if err != nil {
		return domain.OrderUpdate{}, err
	}
	if st.Status != "order" || st.Order == nil {
		// unknownOid / error: an action that is now past its expiresAfter
		// window was definitively rejected before execution.
		if v.expiredOrder(clientID) {
			return domain.OrderUpdate{
				Venue: v.cfg.Name, ClientID: clientID, MarketID: inst.MarketID,
				Status: domain.StatusRejected, Reason: "never placed, expired", Ts: time.Now(),
			}, nil
		}
		return domain.OrderUpdate{Venue: v.cfg.Name, ClientID: clientID, MarketID: inst.MarketID, Status: domain.StatusUnknown}, nil
	}
	oid, err := strconv.ParseInt(st.Order.Order.Oid.String(), 10, 64)
	if err != nil {
		return domain.OrderUpdate{}, fmt.Errorf("invalid order id: %w", err)
	}
	status := mapOrderStatus(st.Order.Status)
	if !status.Terminal() {
		return domain.OrderUpdate{
			Venue: v.cfg.Name, ClientID: clientID, OrderIndex: oid,
			MarketID: inst.MarketID, Status: status, Ts: time.Now(),
		}, nil
	}
	orig, origErr := number(st.Order.Order.OrigSz)
	if origErr != nil || orig <= 0 {
		orig = 0
	}
	// The exchange's own view of the executed quantity: requested minus
	// remaining. A canceled order's fill feed may not be indexed yet, in
	// which case the trade facts must stay unresolved instead of reporting a
	// zero-fill terminal.
	exFilled := 0.0
	if orig > 0 {
		rem, remErr := number(st.Order.Order.Sz)
		if remErr == nil && rem >= 0 && rem <= orig+1e-9 {
			exFilled = orig - rem
		}
	}
	var qty, quote, fee float64
	ok := true
	if status == domain.StatusFilled || status == domain.StatusCanceled {
		qty, quote, fee, ok = v.mergeFills(ctx, oid, st.Order.StatusTimestamp)
	}
	status = partialIOC(status, qty, orig)
	eps := 1e-9
	if !ok && !(status == domain.StatusCanceled && exFilled <= eps) {
		// The fill feed could not be read; only a zero-execution cancel is
		// certain from the order status itself.
		return domain.OrderUpdate{
			Venue: v.cfg.Name, ClientID: clientID, OrderIndex: oid,
			MarketID: inst.MarketID, Status: domain.StatusUnknown, Ts: time.Now(),
		}, nil
	}
	if status == domain.StatusFilled && (qty <= 0 || qty < exFilled-eps) {
		// The order is filled but the fill feed has not caught up. Report a
		// non-terminal state; the executor infers fills from the position
		// delta instead of trusting a zero.
		return domain.OrderUpdate{
			Venue: v.cfg.Name, ClientID: clientID, OrderIndex: oid,
			MarketID: inst.MarketID, Status: domain.StatusUnknown, Ts: time.Now(),
		}, nil
	}
	if status == domain.StatusCanceled && qty < exFilled-eps {
		// A canceled order carries partial fills that are not indexed yet;
		// without them its fill facts are unknown.
		return domain.OrderUpdate{
			Venue: v.cfg.Name, ClientID: clientID, OrderIndex: oid,
			MarketID: inst.MarketID, Status: domain.StatusUnknown, Ts: time.Now(),
		}, nil
	}
	return domain.OrderUpdate{
		Venue: v.cfg.Name, ClientID: clientID, OrderIndex: oid,
		MarketID: inst.MarketID, Status: status, FilledQty: qty, FilledQuote: quote, Fee: fee, Ts: time.Now(),
	}, nil
}

// mergeFills fetches the REST fill feed for an order id around its status
// time and merges every trade into the venue's per-oid fill state. The
// effective cumulative fill is returned. ok=false means the feed could not be
// read, which is distinct from "no fills exist".
func (v *Venue) mergeFills(ctx context.Context, oid, statusTs int64) (qty, quote, fee float64, ok bool) {
	end := time.Now().UnixMilli() + 2000
	fills, err := v.rest.fillsByTime(ctx, v.cfg.Account, statusTs-1000, end)
	if err != nil {
		return 0, 0, 0, false
	}
	v.ordersMu.Lock()
	defer v.ordersMu.Unlock()
	fs := v.fillState(oid)
	for _, f := range fills {
		if f.Oid.String() != strconv.FormatInt(oid, 10) {
			continue
		}
		sz, err1 := number(f.Sz)
		px, err2 := number(f.Px)
		if err1 != nil || err2 != nil || sz <= 0 || px <= 0 || math.IsNaN(sz) || math.IsNaN(px) {
			continue
		}
		feeN, feeErr := number(f.Fee)
		if feeErr != nil {
			feeN = 0
		}
		fs.add(f.Tid, sz, px, feeN)
	}
	q, qu, fe := fs.effective()
	return q, qu, fe, true
}

// CheckKey verifies that the configured agent key and main account are
// locally consistent. Actual agent authorization is enforced by the exchange
// on the first signed action; a rejected action reports it explicitly.
func (v *Venue) CheckKey(ctx context.Context) error {
	if v.signer == nil || v.cfg.Account == "" {
		return domain.ErrNoCredentials
	}
	if v.signer.address == common.HexToAddress(v.cfg.Account) {
		return fmt.Errorf("agent key derives the main account; an API wallet must be a separate approved agent")
	}
	if _, err := v.rest.clearinghouse(ctx, v.cfg.Account, v.cfg.Dex); err != nil {
		return fmt.Errorf("check account access: %w", err)
	}
	return nil
}

// SignOnly signs an order without sending (self-check helper).
func (v *Venue) SignOnly(r domain.OrderReq, nonce int64) (string, error) {
	if v.signer == nil || v.cfg.Account == "" {
		return "", domain.ErrNoCredentials
	}
	asset, c, price, size, _, err := orderWire(r)
	if err != nil {
		return "", err
	}
	sig, err := v.signer.signOrder(asset, r.Side == domain.Buy, r.ReduceOnly, price, size, c, uint64(nonce), 0)
	if err != nil {
		return "", err
	}
	return sig.R + sig.S + ":" + strconv.Itoa(sig.V), nil
}

// SignEphemeral validates signing against the configured chain without any
// network operation, using a throwaway key.
func (v *Venue) SignEphemeral(r domain.OrderReq, nonce int64) (string, error) {
	key, err := randomSigner()
	if err != nil {
		return "", fmt.Errorf("temporary signer: %w", err)
	}
	asset, c, price, size, _, err := orderWire(r)
	if err != nil {
		return "", err
	}
	sig, err := key.signOrder(asset, r.Side == domain.Buy, r.ReduceOnly, price, size, c, uint64(nonce), 0)
	if err != nil {
		return "", err
	}
	return sig.R + sig.S + ":" + strconv.Itoa(sig.V), nil
}

// SetLeverage applies account leverage to a market via an updateLeverage
// action. HIP-3 builder perps only support isolated margin, so isCross is
// always false. A requested leverage above the market maximum is reported as
// ErrLeverageRejected so the caller's downward search can proceed.
func (v *Venue) SetLeverage(ctx context.Context, inst domain.Instrument, leverage int) error {
	if v.signer == nil || v.cfg.Account == "" {
		return domain.ErrNoCredentials
	}
	if inst.MarketID < 0 {
		return fmt.Errorf("%w: invalid market id", domain.ErrRejected)
	}
	if leverage < 1 {
		return fmt.Errorf("leverage %d out of range", leverage)
	}
	if cap, ok := v.maxLeverage(inst.MarketID); ok && leverage > cap {
		return fmt.Errorf("%w: market %d maximum leverage is %d", domain.ErrLeverageRejected, inst.MarketID, cap)
	}
	if err := v.lockSend(v.auxSendMu, ctx); err != nil {
		return err
	}
	defer func() { <-v.auxSendMu }()
	nonce := v.nonce()
	expiresAfter := submitExpiry(time.Now())
	sig, err := v.signer.signUpdateLeverage(inst.MarketID, false, leverage, nonce, expiresAfter)
	if err != nil {
		return fmt.Errorf("%w: sign: %v", domain.ErrRejected, err)
	}
	resp, err := v.rest.send(ctx, exchangeRequest{
		Action: map[string]any{
			"type":     "updateLeverage",
			"asset":    inst.MarketID,
			"isCross":  false,
			"leverage": leverage,
		},
		Nonce:        nonce,
		Signature:    sig,
		ExpiresAfter: expiresAfter,
	})
	if err != nil {
		var hs *httpStatusError
		if errors.As(err, &hs) && hs.code == http.StatusTooManyRequests {
			return fmt.Errorf("%w: rate limited", domain.ErrRejected)
		}
		return fmt.Errorf("updateLeverage: %w", err)
	}
	if resp.Status != "ok" {
		return fmt.Errorf("updateLeverage rejected: %s", rawResponse(resp))
	}
	return nil
}

// expiredOrder reports whether the order's submission window has elapsed, so
// an unknown oid means the action never executed.
func (v *Venue) expiredOrder(cid int64) bool {
	v.ordersMu.Lock()
	defer v.ordersMu.Unlock()
	exp, ok := v.orderExpires[cid]
	return ok && time.Now().UnixMilli() > exp+5000
}

// randomSigner builds a throwaway API-wallet identity for offline checks.
func randomSigner() (*signer, error) {
	key, err := crypto.GenerateKey()
	if err != nil {
		return nil, err
	}
	return &signer{key: key, address: crypto.PubkeyToAddress(key.PublicKey)}, nil
}
