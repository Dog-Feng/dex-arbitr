// Package domain holds venue-agnostic value types shared by every layer.
package domain

import (
	"errors"
	"math"
	"strconv"
	"time"
)

// Side of an order.
type Side int8

const (
	Buy  Side = 1
	Sell Side = -1
)

func (s Side) String() string {
	if s == Buy {
		return "BUY"
	}
	return "SELL"
}

// Opposite returns the other side.
func (s Side) Opposite() Side { return -s }

// Instrument is a tradable perpetual market on one venue.
type Instrument struct {
	Venue         string
	Symbol        string // venue native symbol, e.g. "SNDK"
	Base          string // normalized base after suffix stripping
	MarketID      int
	SizeDecimals  int
	PriceDecimals int
	MinBase       float64
	MinQuote      float64
	TakerFee      float64
	MakerFee      float64
}

// Tick is the minimal price increment.
func (i Instrument) Tick() float64 { return math.Pow10(-i.PriceDecimals) }

// Level is one price level.
type Level struct {
	Price float64
	Size  float64
}

// Book is a top-N order book snapshot (bids desc, asks asc).
type Book struct {
	Venue    string
	MarketID int
	Bids     []Level
	Asks     []Level
	Seq      int64
	Ts       time.Time // local receive time
}

// Valid reports whether both sides have at least one level and are not crossed.
func (b Book) Valid() bool {
	return len(b.Bids) > 0 && len(b.Asks) > 0 && b.Bids[0].Price < b.Asks[0].Price
}

// Mid price.
func (b Book) Mid() float64 { return (b.Bids[0].Price + b.Asks[0].Price) / 2 }

// OrderType for submission.
type OrderType int8

const (
	Limit  OrderType = 0
	Market OrderType = 1
)

// OrderReq is a venue-agnostic order request. Always IOC in this system.
type OrderReq struct {
	ClientID   int64
	Inst       Instrument
	Side       Side
	Qty        float64
	Price      float64 // worst acceptable price (also for Market, used as slippage bound)
	Type       OrderType
	ReduceOnly bool
}

// OrderStatus is the normalized lifecycle state.
type OrderStatus int8

const (
	StatusUnknown  OrderStatus = iota // never treat as failure
	StatusPending                     // accepted, not yet matched
	StatusOpen                        // resting (should not happen for IOC)
	StatusFilled                      // fully filled
	StatusCanceled                    // terminal; may be partially filled
	StatusRejected                    // terminal, nothing filled
)

func (s OrderStatus) Terminal() bool {
	return s == StatusFilled || s == StatusCanceled || s == StatusRejected
}

func (s OrderStatus) String() string {
	return [...]string{"UNKNOWN", "PENDING", "OPEN", "FILLED", "CANCELED", "REJECTED"}[s]
}

// OrderUpdate is a normalized order state report (from WS or query).
type OrderUpdate struct {
	Venue       string
	ClientID    int64
	OrderIndex  int64
	MarketID    int
	Status      OrderStatus
	FilledQty   float64
	FilledQuote float64
	Reason      string
	Ts          time.Time
}

// AvgPrice of the filled portion.
func (u OrderUpdate) AvgPrice() float64 {
	if u.FilledQty == 0 {
		return 0
	}
	return u.FilledQuote / u.FilledQty
}

// SubmitAck is the synchronous (phase 1) result of a submission.
type SubmitAck struct {
	ClientID int64
	TxHash   string
}

// Position on a venue market. Qty is signed (+long / -short).
type Position struct {
	MarketID         int
	Symbol           string
	Qty              float64
	AvgEntry         float64
	LiquidationPrice float64
}

var (
	// ErrRejected: venue definitively rejected the order (nothing will fill).
	ErrRejected = errors.New("order rejected")
	// ErrNoCredentials: venue is read-only.
	ErrNoCredentials = errors.New("venue has no trading credentials")
	// ErrLeverageRejected: the venue rejected the requested leverage because the
	// market's maximum leverage is lower than requested.
	ErrLeverageRejected = errors.New("venue rejected leverage")
)

// Round rounds v to d decimals.
func Round(v float64, d int) float64 {
	p := math.Pow10(d)
	return math.Round(v*p) / p
}

// Floor floors v to d decimals (with epsilon tolerance).
func Floor(v float64, d int) float64 {
	p := math.Pow10(d)
	return math.Floor(v*p+1e-9) / p
}

// ParseF parses a decimal string, returning 0 on error.
func ParseF(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}
