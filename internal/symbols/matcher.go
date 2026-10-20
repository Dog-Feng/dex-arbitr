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
	// Aliases maps a venue symbol to its canonical base before suffix
	// stripping (case-insensitive keys, e.g. "ANTH" -> "ANTHROPIC").
	Aliases map[string]string
}

// DefaultAliases maps common cross-venue symbol spellings to one canonical
// base. Entries are deterministic and curated; config `matching.aliases`
// overrides or extends them. Never auto-derive aliases at runtime: a wrong
// mapping hedges unrelated assets.
func DefaultAliases() map[string]string {
	return map[string]string{
		"ANTH": "ANTHROPIC",
		"OAI":  "OPENAI",
	}
}

// DefaultMatcher strips common perp / quote suffixes. USDC/USDG/USDT/USD are treated 1:1.
func DefaultMatcher() Matcher {
	return Matcher{Suffixes: []string{"PERP", "USDC", "USDG", "USDT", "USD"}, Separators: "-_/: .", Aliases: DefaultAliases()}
}

// Config holds user-supplied matching overrides.
type Config struct {
	StripSuffixes []string
	Separators    string
	Aliases       map[string]string
	// AliasesBuiltin drops the built-in alias table when false (nil keeps it).
	AliasesBuiltin *bool
}

// MatcherFor builds a matcher from optional configuration. All three call
// sites (config validation, the engine, and the scanner) use this single
// merge path so alias handling can never drift.
func MatcherFor(c Config) Matcher {
	m := DefaultMatcher()
	if len(c.StripSuffixes) > 0 {
		m.Suffixes = c.StripSuffixes
	}
	if c.Separators != "" {
		m.Separators = c.Separators
	}
	builtin := true
	if c.AliasesBuiltin != nil {
		builtin = *c.AliasesBuiltin
	}
	m.Aliases = MergeAliases(c.Aliases, builtin)
	return m
}

// MergeAliases combines the built-in alias table with configured aliases.
// Configured entries override built-ins; an empty canonical value deletes a
// built-in alias; builtin=false drops all built-in aliases.
func MergeAliases(configured map[string]string, builtin bool) map[string]string {
	aliases := map[string]string{}
	if builtin {
		for raw, canon := range DefaultAliases() {
			aliases[strings.ToUpper(strings.TrimSpace(raw))] = strings.ToUpper(strings.TrimSpace(canon))
		}
	}
	for raw, canon := range configured {
		key := strings.ToUpper(strings.TrimSpace(raw))
		if key == "" {
			continue
		}
		val := strings.ToUpper(strings.TrimSpace(canon))
		if val == "" {
			delete(aliases, key)
			continue
		}
		aliases[key] = val
	}
	return aliases
}

// Normalize returns the base asset, e.g. "BTC-USDC-PERP" -> "BTC", "SNDK" ->
// "SNDK". Suffixes are stripped first, then the remaining token resolves
// through aliases, and the result is stripped and trimmed again.
func (m Matcher) Normalize(sym string) string {
	s := strings.ToUpper(strings.TrimSpace(sym))
	s = m.stripSuffixes(s)
	s = m.resolveAlias(s)
	s = m.stripSuffixes(s)
	return strings.Trim(s, m.Separators)
}

// stripSuffixes repeatedly removes known suffixes and separator tails.
func (m Matcher) stripSuffixes(s string) string {
	for {
		s = strings.TrimRight(s, m.Separators)
		changed := false
		for _, suf := range m.Suffixes {
			suf = strings.ToUpper(strings.TrimSpace(suf))
			if suf != "" && len(s) > len(suf) && strings.HasSuffix(s, suf) {
				s = s[:len(s)-len(suf)]
				changed = true
				break
			}
		}
		if !changed {
			break
		}
	}
	return s
}

// resolveAlias follows chained aliases up to a small hop limit with a cycle
// guard so a self-referential mapping cannot loop.
func (m Matcher) resolveAlias(s string) string {
	seen := map[string]bool{}
	for hops := 0; hops < 10; hops++ {
		canon, ok := m.Aliases[s]
		if !ok || seen[s] {
			return s
		}
		canon = strings.ToUpper(strings.TrimSpace(canon))
		if canon == "" || canon == s {
			return s
		}
		seen[s] = true
		s = canon
	}
	return s
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
	if math.IsNaN(q) || math.IsInf(q, 0) || q <= 0 {
		return false, "qty must be finite and positive"
	}
	if math.IsNaN(price) || math.IsInf(price, 0) || price < 0 {
		return false, "price must be finite and nonnegative"
	}
	scale := math.Pow10(p.SizeDecimals)
	if p.SizeDecimals < 0 || math.IsInf(scale, 0) || scale == 0 ||
		math.IsNaN(p.MinQty) || math.IsInf(p.MinQty, 0) || p.MinQty < 0 ||
		math.IsNaN(p.MinQuote) || math.IsInf(p.MinQuote, 0) || p.MinQuote < 0 {
		return false, "invalid aligned trading limits"
	}
	scaled := q * scale
	units := math.Round(scaled)
	// Allow only floating-point representation error, not part of a size step.
	ulp := math.Nextafter(scaled, math.Inf(1)) - scaled
	if math.IsInf(scaled, 0) || units <= 0 || math.Abs(scaled-units) > 2*ulp {
		return false, "qty has more decimals than aligned precision"
	}
	qtyULP := math.Nextafter(q, math.Inf(1)) - q
	if q < p.MinQty && p.MinQty-q > 2*qtyULP {
		return false, "qty below min base"
	}
	if price > 0 && math.IsInf(q*price, 0) {
		return false, "notional must be finite"
	}
	if price > 0 {
		notional := q * price
		notionalULP := math.Nextafter(notional, math.Inf(1)) - notional
		if notional < p.MinQuote && p.MinQuote-notional > 2*notionalULP {
			return false, "notional below min quote"
		}
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
	units := minQ * p
	nearest := math.Round(units)
	ulp := math.Nextafter(units, math.Inf(1)) - units
	if math.Abs(units-nearest) <= 2*ulp {
		units = nearest
	}
	minQ = math.Ceil(units) / p
	return Pair{Base: base, Left: l, Right: r, SizeDecimals: dec, MinQty: minQ, MinQuote: math.Max(l.MinQuote, r.MinQuote)}
}
