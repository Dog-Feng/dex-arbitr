package martingale

import (
	"dex-grid/internal/domain/market"
	"dex-grid/internal/domain/position"

	"github.com/shopspring/decimal"
)

var (
	hundred = decimal.NewFromInt(100)
	one     = decimal.NewFromInt(1)
)

// Level 是加仓计划里的一行（含首单 k=0）。
type Level struct {
	Index        int             `json:"index"`
	TriggerPrice decimal.Decimal `json:"trigger_price"`
	Margin       decimal.Decimal `json:"margin"`
	Qty          decimal.Decimal `json:"qty"`
	Notional     decimal.Decimal `json:"notional"`
	CumNotional  decimal.Decimal `json:"cum_notional"`
	AvgPrice     decimal.Decimal `json:"avg_price"`
	TakeProfit   decimal.Decimal `json:"take_profit"`
	CumDropPct   decimal.Decimal `json:"cum_drop_pct"`
	Position     decimal.Decimal `json:"position"`
}

// Plan 是从首单价格推出来的完整加仓表。
type Plan struct {
	Levels           []Level         `json:"levels"`
	TotalMargin      decimal.Decimal `json:"total_margin"`
	TotalNotional    decimal.Decimal `json:"total_notional"`
	LiquidationPrice decimal.Decimal `json:"liquidation_price"`
	MaxDrawdownPct   decimal.Decimal `json:"max_drawdown_pct"`
}

// BuildPlan 按首单价格计算加仓计划。p0 必须已规整。
func BuildPlan(p Params, mkt market.Market, p0 decimal.Decimal) (Plan, error) {
	var out Plan
	if !p0.IsPositive() {
		return out, strategyErr(CodeNoMarkPrice, "", "没有有效的首单价格")
	}
	if err := validateStatic(p); err != nil {
		return out, err
	}

	drop := p.Martingale.AddDropPct.Div(hundred)
	tp := p.Martingale.TakeProfitPct.Div(hundred)
	mult := p.Martingale.AddMultiplier
	long := p.Direction == Long

	n := p.Martingale.MaxAddTimes + 1
	out.Levels = make([]Level, 0, n)

	var pos, cost decimal.Decimal
	prevPx := p0
	avg := p0

	for k := 0; k < n; k++ {
		var px, margin decimal.Decimal
		if k == 0 {
			px = p0
			margin = p.Martingale.InitialMargin
		} else {
			margin = p.Martingale.AddMargin.Mul(pow(mult, k-1))
			base := prevPx
			if p.Martingale.AddDropMode == FromAvg {
				base = avg
			}
			if long {
				px = base.Mul(one.Sub(drop))
			} else {
				px = base.Mul(one.Add(drop))
			}
		}
		px = mkt.RoundPrice(px, market.RoundNearest)
		if !px.IsPositive() {
			return out, strategyErr(CodeInvalidRange, "martingale.add_drop_pct", "第 %d 次加仓价格无效", k)
		}
		qty := mkt.RoundQty(margin.Mul(decimal.NewFromInt(int64(p.Leverage))).Div(px))
		if err := mkt.CheckOrder(px, qty); err != nil {
			return out, strategyErr(CodeNotionalTooSmall, "martingale.add_margin", "第 %d 档数量不满足交易所限制：%v", k, err)
		}
		notional := px.Mul(qty)
		signed := qty
		if !long {
			signed = qty.Neg()
		}
		pos = pos.Add(signed)
		cost = cost.Add(px.Mul(signed))
		if !pos.IsZero() {
			avg = cost.Div(pos)
		}
		var takeProfit decimal.Decimal
		if long {
			takeProfit = mkt.RoundPrice(avg.Mul(one.Add(tp)), market.RoundNearest)
		} else {
			takeProfit = mkt.RoundPrice(avg.Mul(one.Sub(tp)), market.RoundNearest)
		}
		cumDrop := decimal.Zero
		if p0.IsPositive() {
			cumDrop = p0.Sub(px).Div(p0).Mul(hundred)
			if !long {
				cumDrop = px.Sub(p0).Div(p0).Mul(hundred)
			}
		}
		out.Levels = append(out.Levels, Level{
			Index:        k,
			TriggerPrice: px,
			Margin:       margin,
			Qty:          qty,
			Notional:     notional,
			CumNotional:  pos.Abs().Mul(avg),
			AvgPrice:     avg,
			TakeProfit:   takeProfit,
			CumDropPct:   cumDrop,
			Position:     pos,
		})
		out.TotalMargin = out.TotalMargin.Add(margin)
		out.TotalNotional = out.TotalNotional.Add(notional)
		prevPx = px
	}

	last := out.Levels[len(out.Levels)-1]
	dir := position.Long
	if !long {
		dir = position.Short
	}
	out.LiquidationPrice = position.EstimateLiquidationPrice(last.AvgPrice, p.Leverage, mkt.MaintMarginRate, dir)
	if p0.IsPositive() && out.LiquidationPrice.IsPositive() {
		if long {
			out.MaxDrawdownPct = p0.Sub(out.LiquidationPrice).Div(p0).Mul(hundred)
		} else {
			out.MaxDrawdownPct = out.LiquidationPrice.Sub(p0).Div(p0).Mul(hundred)
		}
	}
	return out, nil
}

func pow(base decimal.Decimal, exp int) decimal.Decimal {
	if exp <= 0 {
		return one
	}
	out := one
	for i := 0; i < exp; i++ {
		out = out.Mul(base)
	}
	return out
}
