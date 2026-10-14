package lighter

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync"

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
	fetch func() (int64, error)
}

func (n *nonceMgr) take() (int64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.valid {
		v, err := n.fetch()
		if err != nil {
			return 0, err
		}
		n.next, n.valid = v, true
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
func toBase(qty float64, dec int) int64     { return int64(math.Round(qty * math.Pow10(dec))) }
func toPrice(price float64, dec int) uint32 { return uint32(math.Round(price * math.Pow10(dec))) }

// Submit signs and sends an IOC order (phase 1).
func (v *Venue) Submit(ctx context.Context, r domain.OrderReq) (domain.SubmitAck, error) {
	if v.tx == nil {
		return domain.SubmitAck{}, domain.ErrNoCredentials
	}
	base := toBase(r.Qty, r.Inst.SizeDecimals)
	price := toPrice(r.Price, r.Inst.PriceDecimals)
	if base <= 0 || price == 0 {
		return domain.SubmitAck{}, fmt.Errorf("%w: invalid qty/price %v/%v", domain.ErrRejected, r.Qty, r.Price)
	}
	req := &types.CreateOrderTxReq{
		MarketIndex:      int16(r.Inst.MarketID),
		ClientOrderIndex: r.ClientID,
		BaseAmount:       base,
		Price:            price,
		IsAsk:            0,
		Type:             uint8(txtypes.LimitOrder),
		TimeInForce:      uint8(txtypes.ImmediateOrCancel),
		ReduceOnly:       0,
		TriggerPrice:     txtypes.NilOrderTriggerPrice,
		OrderExpiry:      txtypes.NilOrderExpiry, // IOC requires nil expiry
	}
	if r.Side == domain.Sell {
		req.IsAsk = 1
	}
	if r.Type == domain.Market {
		req.Type = uint8(txtypes.MarketOrder)
	}
	if r.ReduceOnly {
		req.ReduceOnly = 1
	}

	v.sendMu.Lock()
	defer v.sendMu.Unlock()
	nonce, err := v.nonce.take()
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
		if isAPIError(err) {
			return domain.SubmitAck{ClientID: r.ClientID}, fmt.Errorf("%w: %v", domain.ErrRejected, err)
		}
		slog.Warn("sendTx outcome unknown", "venue", v.cfg.Name, "cid", r.ClientID, "err", err)
		return domain.SubmitAck{ClientID: r.ClientID}, err // unknown
	}
	return domain.SubmitAck{ClientID: r.ClientID, TxHash: hash}, nil
}

// SetLeverage applies account leverage to a market by submitting an
// updateLeverage transaction (leverage N -> initial margin fraction 10000/N bps,
// cross margin). It is a no-op on venues without trading credentials.
func (v *Venue) SetLeverage(ctx context.Context, inst domain.Instrument, leverage int) error {
	if v.tx == nil {
		return domain.ErrNoCredentials
	}
	if leverage < 1 || leverage > 10000 {
		return fmt.Errorf("%w: leverage %d out of range [1, 10000]", domain.ErrRejected, leverage)
	}
	v.sendMu.Lock()
	defer v.sendMu.Unlock()
	nonce, err := v.nonce.take()
	if err != nil {
		return err
	}
	txInfo, err := v.tx.GetUpdateLeverageTransaction(&types.UpdateLeverageTxReq{
		MarketIndex:           int16(inst.MarketID),
		InitialMarginFraction: uint16(10000 / leverage),
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
		if ae, ok := apiErrorOf(err); ok && ae.Code == 21113 {
			return fmt.Errorf("%w: %v", domain.ErrLeverageRejected, err)
		}
		return err
	}
	return nil
}

// CancelOrder cancels by order index (Lighter accepts client order index here as well).
func (v *Venue) CancelOrder(ctx context.Context, inst domain.Instrument, clientID int64) error {
	if v.tx == nil {
		return domain.ErrNoCredentials
	}
	v.sendMu.Lock()
	defer v.sendMu.Unlock()
	nonce, err := v.nonce.take()
	if err != nil {
		return err
	}
	txInfo, err := v.tx.GetCancelOrderTransaction(&types.CancelOrderTxReq{MarketIndex: int16(inst.MarketID), Index: clientID},
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
	req := &types.CreateOrderTxReq{
		MarketIndex: int16(r.Inst.MarketID), ClientOrderIndex: r.ClientID,
		BaseAmount: toBase(r.Qty, r.Inst.SizeDecimals), Price: toPrice(r.Price, r.Inst.PriceDecimals),
		Type: uint8(txtypes.LimitOrder), TimeInForce: uint8(txtypes.ImmediateOrCancel),
		TriggerPrice: txtypes.NilOrderTriggerPrice, OrderExpiry: txtypes.NilOrderExpiry,
	}
	if r.Side == domain.Sell {
		req.IsAsk = 1
	}
	tx, err := v.tx.GetCreateOrderTransaction(req, &types.TransactOpts{Nonce: &nonce})
	if err != nil {
		return "", err
	}
	return tx.GetTxInfo()
}
