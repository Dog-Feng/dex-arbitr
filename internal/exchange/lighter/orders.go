package lighter

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync"

	lclient "github.com/elliottech/lighter-go/client"
	"github.com/elliottech/lighter-go/types"
	"github.com/elliottech/lighter-go/types/txtypes"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
)

// nonceMgr hands out local nonces; it resyncs from the server whenever the
// outcome of a send is unknown or the API rejected the tx.
type nonceMgr struct {
	mu    sync.Mutex
	next  int64
	valid bool
	fetch func(context.Context) (int64, error)
}

func (n *nonceMgr) take(ctx context.Context) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if !n.valid {
		v, err := n.fetch(ctx)
		if err != nil {
			return 0, err
		}
		if v < 0 || v == math.MaxInt64 {
			return 0, fmt.Errorf("nonce out of range")
		}
		n.next, n.valid = v, true
	}
	if n.next < 0 || n.next == math.MaxInt64 {
		n.valid = false
		return 0, fmt.Errorf("nonce out of range")
	}
	v := n.next
	n.next++
	return v, nil
}

func (n *nonceMgr) invalidate() {
	n.mu.Lock()
	n.valid = false
	n.mu.Unlock()
}

// Scaled integer conversion helpers.
func scaled(v float64, dec int, max float64) (float64, error) {
	if math.IsNaN(v) || math.IsInf(v, 0) || v <= 0 || dec < 0 || dec > 18 {
		return 0, fmt.Errorf("invalid positive amount or decimals")
	}
	x := math.Round(v * math.Pow10(dec))
	if math.IsNaN(x) || math.IsInf(x, 0) || x < 1 || x > max {
		return 0, fmt.Errorf("scaled amount out of range")
	}
	return x, nil
}
func toBase(qty float64, dec int) (int64, error) {
	x, err := scaled(qty, dec, float64(txtypes.MaxOrderBaseAmount))
	if err != nil {
		return 0, err
	}
	raw := qty * math.Pow10(dec)
	ulp := math.Nextafter(raw, math.Inf(1)) - raw
	if math.Abs(raw-x) > 2*ulp {
		return 0, fmt.Errorf("quantity exceeds native precision")
	}
	return int64(x), nil
}
func toPrice(price float64, dec int) (uint32, error) {
	x, err := scaled(price, dec, float64(math.MaxUint32))
	if err != nil {
		return 0, err
	}
	return uint32(x), nil
}

func marketIndex(inst domain.Instrument) (int16, error) {
	if inst.MarketID < 0 || inst.MarketID > int(txtypes.MaxMarketIndex) || inst.MarketID == int(txtypes.NilMarketIndex) {
		return 0, fmt.Errorf("invalid market index %d", inst.MarketID)
	}
	return int16(inst.MarketID), nil
}

func orderRequest(r domain.OrderReq) (*types.CreateOrderTxReq, error) {
	base, err := toBase(r.Qty, r.Inst.SizeDecimals)
	if err != nil {
		return nil, fmt.Errorf("%w: qty: %v", domain.ErrRejected, err)
	}
	price, err := toPrice(r.Price, r.Inst.PriceDecimals)
	if err != nil {
		return nil, fmt.Errorf("%w: price: %v", domain.ErrRejected, err)
	}
	market, err := marketIndex(r.Inst)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", domain.ErrRejected, err)
	}
	if r.ClientID < txtypes.MinClientOrderIndex || r.ClientID > txtypes.MaxClientOrderIndex {
		return nil, fmt.Errorf("%w: invalid client id", domain.ErrRejected)
	}
	if r.Side != domain.Buy && r.Side != domain.Sell {
		return nil, fmt.Errorf("%w: invalid order side", domain.ErrRejected)
	}
	if r.Type != domain.Limit && r.Type != domain.Market {
		return nil, fmt.Errorf("%w: invalid order type", domain.ErrRejected)
	}
	req := &types.CreateOrderTxReq{MarketIndex: market, ClientOrderIndex: r.ClientID, BaseAmount: base, Price: price, Type: uint8(txtypes.LimitOrder), TimeInForce: uint8(txtypes.ImmediateOrCancel), TriggerPrice: txtypes.NilOrderTriggerPrice, OrderExpiry: txtypes.NilOrderExpiry}
	if r.Side == domain.Sell {
		req.IsAsk = 1
	}
	if r.Type == domain.Market {
		req.Type = uint8(txtypes.MarketOrder)
	}
	if r.ReduceOnly {
		req.ReduceOnly = 1
	}
	return req, nil
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

// Submit signs and sends an IOC order (phase 1).
func (v *Venue) Submit(ctx context.Context, r domain.OrderReq) (domain.SubmitAck, error) {
	if v.tx == nil {
		return domain.SubmitAck{}, domain.ErrNoCredentials
	}
	req, err := orderRequest(r)
	if err != nil {
		return domain.SubmitAck{}, err
	}
	if err := v.lockSend(ctx); err != nil {
		return domain.SubmitAck{}, fmt.Errorf("%w: %v", domain.ErrRejected, err)
	}
	defer func() { <-v.sendMu }()
	nonce, err := v.nonce.take(ctx)
	if err != nil {
		return domain.SubmitAck{}, fmt.Errorf("%w: nonce: %v", domain.ErrRejected, err) // nothing sent
	}
	txInfo, err := v.tx.GetCreateOrderTransaction(req, &types.TransactOpts{Nonce: &nonce})
	if err != nil {
		v.nonce.invalidate()
		return domain.SubmitAck{}, fmt.Errorf("%w: sign: %v", domain.ErrRejected, err)
	}
	body, err := txInfo.GetTxInfo()
	if err != nil {
		v.nonce.invalidate()
		return domain.SubmitAck{}, fmt.Errorf("%w: encode: %v", domain.ErrRejected, err)
	}
	hash, err := v.rest.sendTx(ctx, txInfo.GetTxType(), body)
	if err != nil {
		v.nonce.invalidate()
		slog.Warn("sendTx outcome unknown", "venue", v.cfg.Name, "cid", r.ClientID, "err", err)
		return domain.SubmitAck{ClientID: r.ClientID}, err // unknown
	}
	return domain.SubmitAck{ClientID: r.ClientID, TxHash: hash}, nil
}

// SetLeverage applies account leverage to a market by submitting an
// updateLeverage transaction (leverage N -> initial margin fraction 10000/N bps,
// cross margin). Venues without credentials return ErrNoCredentials.
func (v *Venue) SetLeverage(ctx context.Context, inst domain.Instrument, leverage int) error {
	if v.tx == nil {
		return domain.ErrNoCredentials
	}
	if leverage < 1 || leverage > 10000 {
		return fmt.Errorf("%w: leverage %d out of range [1, 10000]", domain.ErrRejected, leverage)
	}
	market, err := marketIndex(inst)
	if err != nil {
		return fmt.Errorf("%w: %v", domain.ErrRejected, err)
	}
	if err := v.lockSend(ctx); err != nil {
		return err
	}
	defer func() { <-v.sendMu }()
	nonce, err := v.nonce.take(ctx)
	if err != nil {
		return err
	}
	txInfo, err := v.tx.GetUpdateLeverageTransaction(&types.UpdateLeverageTxReq{
		MarketIndex:           market,
		InitialMarginFraction: uint16((10000 + leverage - 1) / leverage),
		MarginMode:            0, // cross margin
	}, &types.TransactOpts{Nonce: &nonce})
	if err != nil {
		v.nonce.invalidate()
		return err
	}
	body, err := txInfo.GetTxInfo()
	if err != nil {
		v.nonce.invalidate()
		return err
	}
	if _, err := v.rest.sendTx(ctx, txInfo.GetTxType(), body); err != nil {
		v.nonce.invalidate()
		return err
	}
	return nil
}

// CancelOrder cancels by order index (Lighter accepts client order index here as well).
func (v *Venue) CancelOrder(ctx context.Context, inst domain.Instrument, clientID int64) error {
	if v.tx == nil {
		return domain.ErrNoCredentials
	}
	market, err := marketIndex(inst)
	if err != nil {
		return fmt.Errorf("%w: %v", domain.ErrRejected, err)
	}
	if clientID < txtypes.MinClientOrderIndex || clientID > txtypes.MaxClientOrderIndex {
		return fmt.Errorf("%w: invalid client id", domain.ErrRejected)
	}
	if err := v.lockSend(ctx); err != nil {
		return err
	}
	defer func() { <-v.sendMu }()
	nonce, err := v.nonce.take(ctx)
	if err != nil {
		return err
	}
	txInfo, err := v.tx.GetCancelOrderTransaction(&types.CancelOrderTxReq{MarketIndex: market, Index: clientID},
		&types.TransactOpts{Nonce: &nonce})
	if err != nil {
		v.nonce.invalidate()
		return err
	}
	body, err := txInfo.GetTxInfo()
	if err != nil {
		v.nonce.invalidate()
		return err
	}
	if _, err := v.rest.sendTx(ctx, txInfo.GetTxType(), body); err != nil {
		v.nonce.invalidate()
		return err
	}
	return nil
}

// SignOnly builds and signs an order without sending (self-check helper).
func (v *Venue) SignOnly(r domain.OrderReq, nonce int64) (string, error) {
	if v.tx == nil {
		return "", domain.ErrNoCredentials
	}
	if nonce < 0 {
		return "", fmt.Errorf("offline signing requires an explicit nonnegative nonce")
	}
	req, err := orderRequest(r)
	if err != nil {
		return "", err
	}
	tx, err := v.tx.GetCreateOrderTransaction(req, &types.TransactOpts{Nonce: &nonce})
	if err != nil {
		return "", err
	}
	return tx.GetTxInfo()
}

// SignEphemeral validates the actual venue chain configuration without using
// account credentials or performing any network operation.
func (v *Venue) SignEphemeral(r domain.OrderReq, nonce int64) (string, error) {
	if nonce < 0 {
		return "", fmt.Errorf("offline signing requires an explicit nonnegative nonce")
	}
	req, err := orderRequest(r)
	if err != nil {
		return "", err
	}
	key, _, err := lclient.GenerateAPIKey()
	if err != nil {
		return "", fmt.Errorf("generate temporary signer: %w", err)
	}
	tx, err := lclient.NewTxClient(nil, key, 1, 4, v.cfg.ChainID)
	if err != nil {
		return "", fmt.Errorf("temporary signer: %w", err)
	}
	info, err := tx.GetCreateOrderTransaction(req, &types.TransactOpts{Nonce: &nonce})
	if err != nil {
		return "", err
	}
	return info.GetTxInfo()
}
