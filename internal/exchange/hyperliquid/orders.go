package hyperliquid

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/ethereum/go-ethereum/crypto"
)

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

func (v *Venue) lockSend(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-v.ws.done:
		return fmt.Errorf("venue closed")
	case v.sendMu <- struct{}{}:
	}
	if v.closed.Load() || ctx.Err() != nil {
		<-v.sendMu
		if err := ctx.Err(); err != nil {
			return err
		}
		return fmt.Errorf("venue closed")
	}
	return nil
}

// orderWire validates and renders the wire form of one order.
func orderWire(r domain.OrderReq) (asset int, c string, price, size string, err error) {
	if r.Inst.MarketID <= 0 {
		return 0, "", "", "", fmt.Errorf("%w: invalid market id", domain.ErrRejected)
	}
	if r.Side != domain.Buy && r.Side != domain.Sell {
		return 0, "", "", "", fmt.Errorf("%w: invalid order side", domain.ErrRejected)
	}
	if r.Qty <= 0 || math.IsNaN(r.Qty) || math.IsInf(r.Qty, 0) {
		return 0, "", "", "", fmt.Errorf("%w: invalid order quantity", domain.ErrRejected)
	}
	if r.Price <= 0 || math.IsNaN(r.Price) || math.IsInf(r.Price, 0) {
		return 0, "", "", "", fmt.Errorf("%w: invalid order price", domain.ErrRejected)
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
		return 0, "", "", "", fmt.Errorf("%w: order price below native precision", domain.ErrRejected)
	}
	// Perp prices carry at most 5 significant figures; round in the
	// tightening direction so the submitted bound is never widened.
	px = sigFigTighten(px, r.Inst.PriceDecimals, r.Side == domain.Buy)
	qty := domain.Floor(r.Qty, r.Inst.SizeDecimals)
	if qty <= 0 {
		return 0, "", "", "", fmt.Errorf("%w: quantity below native precision", domain.ErrRejected)
	}
	c, err = cloid(r.ClientID)
	if err != nil {
		return 0, "", "", "", fmt.Errorf("%w: %v", domain.ErrRejected, err)
	}
	return r.Inst.MarketID, c, sigFmt(px), sigFmt(qty), nil
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
	asset, c, price, size, err := orderWire(r)
	if err != nil {
		return domain.SubmitAck{}, err
	}
	if err := v.lockSend(ctx); err != nil {
		return domain.SubmitAck{}, fmt.Errorf("%w: %v", domain.ErrRejected, err)
	}
	defer func() { <-v.sendMu }()
	nonce := v.nonce()
	sig, err := v.signer.signOrder(asset, r.Side == domain.Buy, r.ReduceOnly, price, size, c, nonce)
	if err != nil {
		return domain.SubmitAck{}, fmt.Errorf("%w: sign: %v", domain.ErrRejected, err)
	}
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
		Nonce:     nonce,
		Signature: sig,
	})
	if err != nil {
		slog.Warn("order send outcome unknown", "venue", v.cfg.Name, "cid", r.ClientID, "err", err)
		return domain.SubmitAck{ClientID: r.ClientID}, err
	}
	statuses := resp.orderStatuses()
	if resp.Status != "ok" || statuses == nil {
		return domain.SubmitAck{ClientID: r.ClientID}, fmt.Errorf("exchange status %q: outcome unknown", resp.Status)
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
	if st.Filled != nil {
		// An IOC that filled synchronously carries its exact fill. Publish the
		// terminal update immediately so the executor does not wait for the
		// user stream.
		filledQty, qtyErr := number(st.Filled.TotalSz)
		avgPx, pxErr := number(st.Filled.AvgPx)
		if qtyErr == nil && pxErr == nil && filledQty > 0 && avgPx > 0 {
			v.fillAgg[oid] = fillAgg{qty: filledQty, quote: filledQty * avgPx}
			v.orderFinal[oid] = domain.StatusFilled
			v.publishOrder(domain.OrderUpdate{
				Venue: v.cfg.Name, ClientID: r.ClientID, OrderIndex: oid,
				Status: domain.StatusFilled, FilledQty: filledQty, FilledQuote: filledQty * avgPx,
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
	if inst.MarketID <= 0 {
		return fmt.Errorf("%w: invalid market id", domain.ErrRejected)
	}
	c, err := cloid(clientID)
	if err != nil {
		return fmt.Errorf("%w: %v", domain.ErrRejected, err)
	}
	if err := v.lockSend(ctx); err != nil {
		return err
	}
	defer func() { <-v.sendMu }()
	nonce := v.nonce()
	sig, err := v.signer.signCancelByCloid(inst.MarketID, c, nonce)
	if err != nil {
		return fmt.Errorf("%w: sign: %v", domain.ErrRejected, err)
	}
	resp, err := v.rest.send(ctx, exchangeRequest{
		Action: map[string]any{
			"type":    "cancelByCloid",
			"cancels": []map[string]any{{"asset": inst.MarketID, "cloid": c}},
		},
		Nonce:     nonce,
		Signature: sig,
	})
	if err != nil {
		slog.Warn("cancel send outcome unknown", "venue", v.cfg.Name, "cid", clientID, "err", err)
		return err
	}
	if resp.Status != "ok" {
		return fmt.Errorf("cancelByCloid rejected: %s", rawResponse(resp))
	}
	// A cancel response lists per-order statuses. An explicit per-order error
	// is surfaced; a missing order has nothing left to cancel and is fine.
	for _, st := range resp.orderStatuses() {
		if st.Error != nil {
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
	var qty, quote float64
	ok := true
	if status == domain.StatusFilled || status == domain.StatusCanceled {
		qty, quote, ok = v.terminalFills(ctx, oid, st.Order.StatusTimestamp)
	}
	if status == domain.StatusFilled && (!ok || qty <= 0) {
		// The order is filled but the fill feed has not caught up. Report a
		// non-terminal state; the executor infers fills from the position
		// delta instead of trusting a zero.
		return domain.OrderUpdate{
			Venue: v.cfg.Name, ClientID: clientID, OrderIndex: oid,
			MarketID: inst.MarketID, Status: domain.StatusUnknown, Ts: time.Now(),
		}, nil
	}
	if status == domain.StatusCanceled && !ok {
		// A canceled order may carry partial fills; without a confirmed fill
		// read its fill facts are unknown.
		return domain.OrderUpdate{
			Venue: v.cfg.Name, ClientID: clientID, OrderIndex: oid,
			MarketID: inst.MarketID, Status: domain.StatusUnknown, Ts: time.Now(),
		}, nil
	}
	return domain.OrderUpdate{
		Venue: v.cfg.Name, ClientID: clientID, OrderIndex: oid,
		MarketID: inst.MarketID, Status: status, FilledQty: qty, FilledQuote: quote, Ts: time.Now(),
	}, nil
}

// terminalFills sums the fill sizes for an order id around its status time.
// ok=false means the fill feed could not be read, which is distinct from
// "no fills exist".
func (v *Venue) terminalFills(ctx context.Context, oid, statusTs int64) (qty, quote float64, ok bool) {
	end := time.Now().UnixMilli() + 2000
	fills, err := v.rest.fillsByTime(ctx, v.cfg.Account, statusTs-1000, end)
	if err != nil {
		return 0, 0, false
	}
	for _, f := range fills {
		if f.Oid.String() != strconv.FormatInt(oid, 10) {
			continue
		}
		sz, err1 := number(f.Sz)
		px, err2 := number(f.Px)
		if err1 != nil || err2 != nil {
			continue
		}
		qty += sz
		quote += sz * px
	}
	return qty, quote, true
}

// CheckKey verifies that the configured agent key and main account are
// locally consistent. Actual agent authorization is enforced by the exchange
// on the first signed action; a rejected action reports it explicitly.
func (v *Venue) CheckKey(ctx context.Context) error {
	if v.signer == nil || v.cfg.Account == "" {
		return domain.ErrNoCredentials
	}
	if v.signer.address.String() == v.cfg.Account {
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
	asset, c, price, size, err := orderWire(r)
	if err != nil {
		return "", err
	}
	sig, err := v.signer.signOrder(asset, r.Side == domain.Buy, r.ReduceOnly, price, size, c, uint64(nonce))
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
	asset, c, price, size, err := orderWire(r)
	if err != nil {
		return "", err
	}
	sig, err := key.signOrder(asset, r.Side == domain.Buy, r.ReduceOnly, price, size, c, uint64(nonce))
	if err != nil {
		return "", err
	}
	return sig.R + sig.S + ":" + strconv.Itoa(sig.V), nil
}

// randomSigner builds a throwaway API-wallet identity for offline checks.
func randomSigner() (*signer, error) {
	key, err := crypto.GenerateKey()
	if err != nil {
		return nil, err
	}
	return &signer{key: key, address: crypto.PubkeyToAddress(key.PublicKey)}, nil
}
