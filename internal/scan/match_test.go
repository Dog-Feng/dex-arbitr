package scan

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
	"github.com/Dog-Feng/dex-arbitr/internal/symbols"
)

type matchingVenue struct {
	exchange.Venue
	name        string
	instruments []domain.Instrument
	err         error
}

func (v matchingVenue) Name() string { return v.name }
func (v matchingVenue) Instruments(context.Context) ([]domain.Instrument, error) {
	return v.instruments, v.err
}

func TestMatchAllPreservesEveryVenueCombinationAndInstrument(t *testing.T) {
	a := matchingVenue{name: "A", instruments: []domain.Instrument{{MarketID: 11, Symbol: "BTC-PERP", SizeDecimals: 3}, {MarketID: 12, Symbol: "ETH", SizeDecimals: 3}}}
	b := matchingVenue{name: "B", instruments: []domain.Instrument{{MarketID: 21, Symbol: "BTCUSD", SizeDecimals: 2}, {MarketID: 22, Symbol: "ETH-PERP", SizeDecimals: 2}}}
	c := matchingVenue{name: "C", instruments: []domain.Instrument{{MarketID: 31, Symbol: "BTC", SizeDecimals: 4}}}
	got, err := MatchAll(context.Background(), symbols.DefaultMatcher(), map[string]exchange.Venue{"C": c, "B": b, "A": a})
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, m := range got {
		keys = append(keys, fmt.Sprintf("%s:%s#%d/%s#%d:%d", m.Pair.Base, m.L.Name(), m.Pair.Left.MarketID, m.R.Name(), m.Pair.Right.MarketID, m.Pair.SizeDecimals))
	}
	want := []string{"BTC:A#11/B#21:2", "ETH:A#12/B#22:2", "BTC:A#11/C#31:3", "BTC:B#21/C#31:2"}
	if !reflect.DeepEqual(keys, want) {
		t.Fatalf("scan candidates=%v want %v", keys, want)
	}
}

func TestMatchAllRejectsIncompleteOrCanceledUniverse(t *testing.T) {
	want := errors.New("market metadata unavailable")
	a := matchingVenue{name: "A", instruments: []domain.Instrument{{Symbol: "BTC"}}}
	b := matchingVenue{name: "B", err: want}
	got, err := MatchAll(context.Background(), symbols.DefaultMatcher(), map[string]exchange.Venue{"A": a, "B": b})
	if !errors.Is(err, want) || got != nil {
		t.Fatalf("partial universe accepted: %v %v", got, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got, err = MatchAll(ctx, symbols.DefaultMatcher(), map[string]exchange.Venue{"A": a}); !errors.Is(err, context.Canceled) || got != nil {
		t.Fatalf("canceled scan=%v %v", got, err)
	}
	if _, err = MatchAll(context.Background(), symbols.DefaultMatcher(), map[string]exchange.Venue{"Wrong": a}); err == nil {
		t.Fatal("inconsistent venue identity accepted")
	}
}
