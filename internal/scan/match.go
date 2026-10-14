package scan

import (
	"context"
	"fmt"
	"sort"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
	"github.com/Dog-Feng/dex-arbitr/internal/symbols"
)

// MatchedPair is a scan candidate with the exact instruments and venue order
// that downstream execution must use.
type MatchedPair struct {
	Pair symbols.Pair
	L, R exchange.Venue
}

// MatchAll discovers all common perpetual markets in a deterministic venue
// order. Both automatic trading and the read-only scanner use this source;
// manual trading resolves only its explicitly configured pairs.
func MatchAll(ctx context.Context, m symbols.Matcher, venues map[string]exchange.Venue) ([]MatchedPair, error) {
	names := make([]string, 0, len(venues))
	for name := range venues {
		names = append(names, name)
	}
	sort.Strings(names)
	markets := make(map[string][]domain.Instrument, len(names))
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		v := venues[name]
		if v == nil || v.Name() != name {
			return nil, fmt.Errorf("scan venue %s has an inconsistent identity", name)
		}
		inst, err := v.Instruments(ctx)
		if err != nil {
			return nil, fmt.Errorf("%s instruments: %w", name, err)
		}
		markets[name] = inst
	}
	var out []MatchedPair
	for i, left := range names {
		for _, right := range names[i+1:] {
			for _, p := range symbols.Match(m, markets[left], markets[right]) {
				out = append(out, MatchedPair{Pair: p, L: venues[left], R: venues[right]})
			}
		}
	}
	return out, nil
}
