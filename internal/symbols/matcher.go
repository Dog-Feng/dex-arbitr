// Package symbols matches perpetual instruments across venues and aligns precision.
package symbols

import (
	"math"
	"sort"
	"strings"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
)

// Matcher normalizes venue symbols to a base asset.
type Matcher struct {
	// Suffixes stripped from the end (case-insensitive), repeatedly.
	Suffixes []string
	// Separators trimmed between tokens.
	Separators string
}

// DefaultMatcher strips common perp / quote suffixes. USDC/USDG/USDT/USD are treated 1:1.
func DefaultMatcher() Matcher {
	return Matcher{Suffixes: []string{"PERP", "USDC", "USDG", "USDT", "USD"}, Separators: "-_/: ."}
}

// Normalize returns the base asset, e.g. "BTC-USDC-PERP" -> "BTC", "SNDK" -> "SNDK".
func (m Matcher) Normalize(sym string) string {
	s := strings.ToUpper(strings.TrimSpace(sym))
	for {
		s = strings.TrimRight(s, m.Separators)
		changed := false
		for _, suf := range m.Suffixes {
			suf = strings.ToUpper(suf)
			if len(s) > len(suf) && strings.HasSuffix(s, suf) {
				s = s[:len(s)-len(suf)]
				changed = true
				break
			}
		}
		if !changed {
			break
		}
	}
	return strings.Trim(s, m.Separators)
}

// Pair is a matched instrument pair with aligned trading precision.
type Pair struct {
	Base         string
	Left, Right  domain.Instrument
	SizeDecimals int     // min of both sides
	MinQty       float64 // max of both sides' min base, rounded up to SizeDecimals
	MinQuote     float64 // max of both sides
}

// AlignQty floors qty to the aligned size precision.
func (p Pair) AlignQty(q float64) float64 { return domain.Floor(q, p.SizeDecimals) }

// ValidQty reports whether q is tradable on both venues at the given price.
func (p Pair) ValidQty(q, price float64) (bool, string) {
	if p.AlignQty(q) != domain.Round(q, p.SizeDecimals) {
		return false, "qty has more decimals than aligned precision"
	}
	if q+1e-12 < p.MinQty {
		return false, "qty below min base"
	}
	if price > 0 && q*price+1e-9 < p.MinQuote {
		return false, "notional below min quote"
	}
	return true, ""
}

// Match pairs instruments whose normalized base is identical.
func Match(m Matcher, left, right []domain.Instrument) []Pair {
	idx := map[string]domain.Instrument{}
	for _, r := range right {
		b := m.Normalize(r.Symbol)
		if b == "" {
			continue
		}
		if _, dup := idx[b]; !dup {
			idx[b] = r
		}
	}
	var out []Pair
	seen := map[string]bool{}
	for _, l := range left {
		b := m.Normalize(l.Symbol)
		r, ok := idx[b]
		if b == "" || !ok || seen[b] {
			continue
		}
		seen[b] = true
		l.Base, r.Base = b, b
		out = append(out, NewPair(b, l, r))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Base < out[j].Base })
	return out
}

// NewPair aligns precision of two instruments.
func NewPair(base string, l, r domain.Instrument) Pair {
	dec := l.SizeDecimals
	if r.SizeDecimals < dec {
		dec = r.SizeDecimals
	}
	minQ := math.Max(l.MinBase, r.MinBase)
	p := math.Pow10(dec)
	minQ = math.Ceil(minQ*p-1e-9) / p
	return Pair{Base: base, Left: l, Right: r, SizeDecimals: dec, MinQty: minQ, MinQuote: math.Max(l.MinQuote, r.MinQuote)}
}
