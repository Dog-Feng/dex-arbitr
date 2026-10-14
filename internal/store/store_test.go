package store

import (
	"bytes"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func TestSavePreservesPreviousStateOnInvalidInput(t *testing.T) {
	s := newTestStore(t)
	good := PairState{NextID: 2, Steps: []strategy.Step{{ID: 1, Dir: strategy.DirLS, Qty: 1}}, Day: "2026-10-14", VolumeUSD: 100, PnLUSD: -10}
	if err := s.Save("BTC_L_R", good); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(s.path("BTC_L_R"))
	if err != nil {
		t.Fatal(err)
	}
	bad := good
	bad.PnLUSD = math.NaN()
	if err := s.Save("BTC_L_R", bad); err == nil {
		t.Fatal("expected invalid number error")
	}
	after, err := os.ReadFile(s.path("BTC_L_R"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("state overwritten: %s, %v", after, err)
	}
	got, err := s.Load("BTC_L_R")
	if err != nil || got.VolumeUSD != 100 || got.PnLUSD != -10 {
		t.Fatalf("reload budget: %+v, %v", got, err)
	}
}

func TestLoadLegacyAndRejectInvalidLedger(t *testing.T) {
	s := newTestStore(t)
	for _, tc := range []struct {
		name, state string
		invalid     bool
	}{
		{"legacy", `{"next_id":2,"steps":[{"id":1,"dir":-1,"qty":1,"entry_bps":2}]}`, false},
		{"bad direction", `{"next_id":2,"steps":[{"id":1,"dir":0,"qty":1}]}`, true},
		{"negative size", `{"next_id":2,"steps":[{"id":1,"dir":-1,"qty":-1}]}`, true},
		{"mixed directions", `{"next_id":3,"steps":[{"id":1,"dir":-1,"qty":1},{"id":2,"dir":1,"qty":1}]}`, true},
		{"duplicate id", `{"next_id":2,"steps":[{"id":1,"dir":-1,"qty":1},{"id":1,"dir":-1,"qty":1}]}`, true},
		{"bad next id", `{"next_id":1,"steps":[{"id":1,"dir":-1,"qty":1}]}`, true},
		{"truncated JSON", `{"next_id":`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(s.path("test"), []byte(tc.state), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := s.Load("test")
			if (err != nil) != tc.invalid {
				t.Fatalf("Load error=%v, want invalid=%v", err, tc.invalid)
			}
		})
	}
}

func TestListIncludesPaperAndBudgetOnlyKeys(t *testing.T) {
	s := newTestStore(t)
	for _, key := range []string{"BTC_L_R", "ETH_L_R_paper"} {
		if err := s.Save(key, PairState{NextID: 1, Day: "2026-10-14", VolumeUSD: 100}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.List()
	if err != nil || len(got) != 2 || got["ETH_L_R_paper"].VolumeUSD != 100 {
		t.Fatalf("List=%+v, %v", got, err)
	}
	for _, key := range []string{"", "../escape", "a/b"} {
		if _, err := s.Load(key); err == nil {
			t.Fatalf("accepted invalid key %q", key)
		}
	}
}

func TestConcurrentSaveSameKey(t *testing.T) {
	s := newTestStore(t)
	var wg sync.WaitGroup
	for i := 1; i <= 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Save("BTC_L_R", PairState{NextID: int64(i), VolumeUSD: float64(i)}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if _, err := s.Load("BTC_L_R"); err != nil {
		t.Fatal(err)
	}
	temps, err := filepath.Glob(filepath.Join(s.dir, "*.tmp"))
	if err != nil || len(temps) != 0 {
		t.Fatalf("temporary files remain: %v, %v", temps, err)
	}
}

func TestStoreCloseAndReopen(t *testing.T) {
	s := newTestStore(t)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Save("test", PairState{NextID: 1}); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed store Save: %v", err)
	}
	reopened, err := New(s.dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Save("test", PairState{NextID: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Load("test"); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSettlementAndUncertaintySurviveReload(t *testing.T) {
	s := newTestStore(t)
	st := PairState{
		NextID: 2, Unresolved: true, PnLUncertain: true, Day: "2026-10-14", VolumeUSD: 99, PnLUSD: -1,
		Steps: []strategy.Step{{ID: 1, Dir: strategy.DirLS, Qty: 0.5, OriginalQty: 1,
			OpenL: 100, OpenR: 102, RealizedUSD: -0.25, Realized: -25,
			EntryFeesUSD: 0.1, PriceEstimated: true, OpenedAt: time.Unix(1_760_000_000, 0).UTC()}},
	}
	if err := s.Save("BTC_L_R", st); err != nil {
		t.Fatal(err)
	}
	got, err := s.Load("BTC_L_R")
	if err != nil || !got.Unresolved || !got.PnLUncertain || len(got.Steps) != 1 {
		t.Fatalf("reload uncertainty: %+v, %v", got, err)
	}
	step := got.Steps[0]
	if step.RealizedUSD != -0.25 || step.OriginalQty != 1 || step.EntryFeesUSD != 0.1 || !step.PriceEstimated || !step.OpenedAt.Equal(st.Steps[0].OpenedAt) {
		t.Fatalf("settlement basis lost: %+v", step)
	}
	if err := s.Save("empty_unresolved", PairState{NextID: 1, Unresolved: true, PnLUncertain: true}); err != nil {
		t.Fatal(err)
	}
	all, err := s.List()
	if err != nil || !all["empty_unresolved"].Unresolved || !all["empty_unresolved"].PnLUncertain {
		t.Fatalf("empty checkpoint lost: %+v, %v", all, err)
	}
}

func TestRejectInvalidSettlementBeforeReplacingState(t *testing.T) {
	s := newTestStore(t)
	good := PairState{NextID: 2, Steps: []strategy.Step{{ID: 1, Dir: strategy.DirSL, Qty: 0.5, OriginalQty: 1, OpenL: 100, OpenR: 102}}}
	if err := s.Save("BTC_L_R", good); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*strategy.Step)
	}{
		{"NaN realized", func(st *strategy.Step) { st.RealizedUSD = math.NaN() }},
		{"infinite fee", func(st *strategy.Step) { st.EntryFeesUSD = math.Inf(1) }},
		{"negative fee", func(st *strategy.Step) { st.EntryFeesUSD = -1 }},
		{"infinite original size", func(st *strategy.Step) { st.OriginalQty = math.Inf(1) }},
		{"negative original size", func(st *strategy.Step) { st.OriginalQty = -1 }},
		{"original smaller than remaining", func(st *strategy.Step) { st.OriginalQty = 0.25 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := good
			bad.Steps = append([]strategy.Step(nil), good.Steps...)
			tc.mutate(&bad.Steps[0])
			if err := s.Save("BTC_L_R", bad); err == nil {
				t.Fatal("invalid settlement accepted")
			}
			got, err := s.Load("BTC_L_R")
			if err != nil || got.Steps[0] != good.Steps[0] {
				t.Fatalf("invalid write replaced known ledger: %+v, %v", got, err)
			}
		})
	}
}

func TestSaveWriteFailureCleansTemporaryFile(t *testing.T) {
	s := newTestStore(t)
	if err := os.Mkdir(s.path("blocked"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := s.Save("blocked", PairState{NextID: 1}); err == nil {
		t.Fatal("save reported success after rename failed")
	}
	temps, err := filepath.Glob(filepath.Join(s.dir, "*.tmp"))
	if err != nil || len(temps) != 0 {
		t.Fatalf("failed write left temporary files: %v, %v", temps, err)
	}
}
