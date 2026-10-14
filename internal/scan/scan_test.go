package scan

import (
	"testing"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/spread"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
)

func TestEval(t *testing.T) {
	// R richer than L: LS ~ +10bps, mu 0, sigma 0, fee 1, volK 0, open 1 => net ~ 8
	q, _ := spread.Compute(
		domain.Book{Bids: []domain.Level{{Price: 999.9, Size: 1}}, Asks: []domain.Level{{Price: 1000, Size: 1}}},
		domain.Book{Bids: []domain.Level{{Price: 1001, Size: 1}}, Asks: []domain.Level{{Price: 1001.1, Size: 1}}},
	)
	ls, sl := Eval(q, 0, 0, 1, 1, 0)
	if ls <= 0 || sl >= 0 {
		t.Fatalf("ls=%v sl=%v", ls, sl)
	}
}

func TestScoreFields(t *testing.T) {
	o := Score{Base: "SPY", Dir: strategy.DirLS, NetBps: 2.5, MuReady: true}
	if o.Base != "SPY" || o.NetBps != 2.5 {
		t.Fatalf("%+v", o)
	}
}
