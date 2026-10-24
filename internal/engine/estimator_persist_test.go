package engine

import (
	"math"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/spread/estimator"
	"github.com/Dog-Feng/dex-arbitr/internal/store"
	"github.com/Dog-Feng/dex-arbitr/internal/symbols"
)

func zscoreParams() estimator.Params {
	return estimator.Params{Interval: time.Second, MinWarmup: 10 * time.Second,
		MuHalfLife: time.Minute, SigmaHalfLife: time.Hour, SigmaFloor: 0.5}
}

func testInst() domain.Instrument {
	return domain.Instrument{MarketID: 1, SizeDecimals: 4, PriceDecimals: 2, TakerFee: .001}
}

// TestEstimatorRestoredOnActorRestart locks the warmup persistence: a restart
// resumes the mean estimator's history instead of paying the full warmup
// window again.
func TestEstimatorRestoredOnActorRestart(t *testing.T) {
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Error(err)
		}
	})
	est1 := estimator.NewZScore(zscoreParams())
	start := time.Now().Add(-40 * time.Second)
	for i := 1; i <= 30; i++ {
		est1.Update(start.Add(time.Duration(i)*time.Second), float64(i))
	}
	cfg := func(e estimator.Estimator) PairConfig {
		return PairConfig{Pair: symbols.NewPair("TEST", testInst(), testInst()),
			L: newActorTestVenue("L"), R: newActorTestVenue("R"), DryRun: true,
			Store: st, Est: e, MaxBookAge: time.Second}
	}
	a1, err := NewPairActor(cfg(est1))
	if err != nil {
		t.Fatal(err)
	}
	if err := a1.persist(); err != nil {
		t.Fatal(err)
	}
	est2 := estimator.NewZScore(zscoreParams())
	if _, err := NewPairActor(cfg(est2)); err != nil {
		t.Fatal(err)
	}
	if est2.Mean() != est1.Mean() || est2.Save().N != 30 {
		t.Fatalf("estimator state not restored: mean %v want %v, n %d", est2.Mean(), est1.Mean(), est2.Save().N)
	}
}

// TestPersistBeforeFirstSampleSkipsEstimator locks the zero-history boundary:
// a shutdown persist before the estimator's first sample must succeed and
// simply not write an estimator snapshot.
func TestPersistBeforeFirstSampleSkipsEstimator(t *testing.T) {
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Error(err)
		}
	})
	est := estimator.NewZScore(zscoreParams())
	a, err := NewPairActor(PairConfig{Pair: symbols.NewPair("TEST", testInst(), testInst()),
		L: newActorTestVenue("L"), R: newActorTestVenue("R"), DryRun: true,
		Store: st, Est: est, MaxBookAge: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.persist(); err != nil {
		t.Fatalf("persist before first sample failed: %v", err)
	}
	loaded, err := st.Load(a.key)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Estimator != nil {
		t.Fatalf("zero-history estimator state persisted: %+v", loaded.Estimator)
	}
}

// TestSampleMeanFeedsEstimatorFromBooks locks the event-driven mean sampling:
// a book wake samples the spread mid into the estimator in the actor's own
// goroutine.
func TestSampleMeanFeedsEstimatorFromBooks(t *testing.T) {
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Error(err)
		}
	})
	est := estimator.NewZScore(zscoreParams())
	a, err := NewPairActor(PairConfig{Pair: symbols.NewPair("TEST", testInst(), testInst()),
		L: newActorTestVenue("L"), R: newActorTestVenue("R"), DryRun: true,
		Store: st, Est: est, MaxBookAge: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	a.bookL, a.bookR = actorTestBook("L", 100), actorTestBook("R", 102)
	a.sampleMean(now)
	want := (102.05 - 100.05) / ((100.05 + 102.05) / 2) * 1e4
	if got := estimator.MeanAt(est, now); math.Abs(got-want) > 1e-9 {
		t.Fatalf("mean %v want %v", got, want)
	}
	if est.Save().N != 1 {
		t.Fatalf("sample count %d, want 1", est.Save().N)
	}
}
