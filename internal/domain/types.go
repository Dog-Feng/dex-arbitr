// Package domain holds venue-agnostic value types shared by every layer.
package domain

import (
	"errors"
	"math"
	"strconv"
	"strings"
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
	SigFigs       int // max significant figures for prices; 0 = no constraint
	MinBase       float64
	MinQuote      float64
	TakerFee      float64
	MakerFee      float64
}

// Tick is the minimal price increment.
func (i Instrument) Tick() float64 { return math.Pow10(-i.PriceDecimals) }

// TickAt returns the smallest price increment actually available at price px.
// Venues that restrict prices to SigFigs significant figures (Hyperliquid: 5)
// trade on a coarser grid for large prices; the integer grid always remains
// available, so the grid never exceeds 1.
func (i Instrument) TickAt(px float64) float64 {
	t := math.Pow10(-i.PriceDecimals)
	if i.SigFigs <= 0 || px <= 0 || math.IsNaN(px) || math.IsInf(px, 0) {
		return t
	}
	exp := int(math.Floor(math.Log10(px))) - (i.SigFigs - 1)
	if exp > 0 {
		exp = 0
	}
	if s := math.Pow10(exp); s > t {
		t = s
	}
	return t
}

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
	Fee         float64 // exchange-reported fee of the filled portion; 0 = unknown
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

// MarginMode identifies which pool of collateral backs a position.
type MarginMode uint8

const (
	MarginUnknown MarginMode = iota
	MarginCross
	MarginIsolated
)

// Position on a venue market. Qty is signed (+long / -short).
type Position struct {
	MarketID         int
	Symbol           string
	Qty              float64
	AvgEntry         float64
	LiquidationPrice float64
	MarginMode       MarginMode
	Notional         float64 // absolute USD position value; 0 means unavailable
}

// CrossMarginRisk is account-wide cross equity and its maintenance requirement.
// Equity can be negative; Maintenance is nonnegative. Both must be finite.
type CrossMarginRisk struct {
	Equity      float64
	Maintenance float64
}

// AccountSnapshot keeps positions and account risk from one venue response.
type AccountSnapshot struct {
	Positions   []Position
	CrossMargin *CrossMarginRisk // nil means unavailable or invalid
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

// Floor rounds v down to d decimal places, using its shortest round-trip
// decimal representation. It preserves native decimal values without adding
// an epsilon that could increase a quantity just below a size step.
func Floor(v float64, d int) float64 {
	return roundDecimal(v, d, false)
}

// Ceil rounds v up to d decimal places using the same decimal representation
// as Floor. Negative d rounds to powers of ten before the decimal point.
func Ceil(v float64, d int) float64 {
	return roundDecimal(v, d, true)
}

func roundDecimal(v float64, d int, up bool) float64 {
	if v == 0 || math.IsNaN(v) || math.IsInf(v, 0) || d >= 324 {
		return v
	}
	// Rounding a negative value down increases its magnitude.
	increase := up != math.Signbit(v)
	if d < -308 {
		if increase {
			return math.Copysign(math.Inf(1), v)
		}
		return math.Copysign(0, v)
	}

	// Working with decimal digits avoids v*10^d crossing an integer because of
	// binary multiplication error. The coefficient has at most 17 digits, so
	// its truncated prefix and a carry both fit in uint64.
	mantissa, exponent, ok := strings.Cut(strconv.FormatFloat(math.Abs(v), 'e', -1, 64), "e")
	if !ok {
		return math.NaN()
	}
	e, err := strconv.Atoi(exponent)
	if err != nil {
		return math.NaN()
	}
	digits := strings.ReplaceAll(mantissa, ".", "")
	keep := e + 1 + d
	if keep >= len(digits) {
		return v
	}
	var units uint64
	if keep > 0 {
		units, err = strconv.ParseUint(digits[:keep], 10, 64)
		if err != nil {
			return math.NaN()
		}
	}
	// A shortest coefficient has no trailing zero, so a truncated suffix
	// always contains a nonzero digit.
	if increase {
		units++
	}
	if units == 0 {
		return math.Copysign(0, v)
	}
	rounded, err := strconv.ParseFloat(strconv.FormatUint(units, 10)+"e"+strconv.Itoa(-d), 64)
	if err != nil && !errors.Is(err, strconv.ErrRange) {
		return math.NaN()
	}
	return math.Copysign(rounded, v)
}

// ParseF parses a decimal string, returning 0 on error.
func ParseF(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}
