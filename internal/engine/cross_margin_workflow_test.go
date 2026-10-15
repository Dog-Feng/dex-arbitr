package engine

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/execution"
	"github.com/Dog-Feng/dex-arbitr/internal/spread"
	"github.com/Dog-Feng/dex-arbitr/internal/store"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
	"github.com/Dog-Feng/dex-arbitr/internal/symbols"
)

type crossWorkflowVenue struct {
	*actorTestVenue
	riskMu    sync.RWMutex
	position  domain.Position
	haveRisk  bool
	submitted atomic.Int64
}

func (v *crossWorkflowVenue) AccountSnapshot(context.Context) (domain.AccountSnapshot, error) {
	v.riskMu.RLock()
	defer v.riskMu.RUnlock()
	snapshot := domain.AccountSnapshot{Positions: []domain.Position{v.position}}
	if v.haveRisk {
		snapshot.CrossMargin = &domain.CrossMarginRisk{Equity: 50, Maintenance: 1}
	}
	return snapshot, nil
}

func (v *crossWorkflowVenue) Positions(ctx context.Context) ([]domain.Position, error) {
	snapshot, err := v.AccountSnapshot(ctx)
	return snapshot.Positions, err
}

func (v *crossWorkflowVenue) Submit(context.Context, domain.OrderReq) (domain.SubmitAck, error) {
	v.submitted.Add(1)
	return domain.SubmitAck{}, domain.ErrRejected
}

func (v *crossWorkflowVenue) setRisk(available bool) {
	v.riskMu.Lock()
	defer v.riskMu.Unlock()
	v.haveRisk = available
}

type crossWorkflowDecision struct {
	mode   strategy.Mode
	action strategy.Action
}

type crossWorkflowStrategy struct{ decisions chan crossWorkflowDecision }

func (s crossWorkflowStrategy) Decide(p strategy.Params, state strategy.State, q spread.Quote, mu float64, ready bool, vol float64) strategy.Action {
	action := (strategy.Volume{}).Decide(p, state, q, mu, ready, vol)
	select {
	case s.decisions <- crossWorkflowDecision{mode: state.Mode, action: action}:
	default:
	}
	// Observe the real strategy and mode transitions without placing orders.
	return strategy.Action{}
}

func TestCrossMarginZeroLiquidationWorkflow(t *testing.T) {
	for _, dir := range []strategy.Dir{strategy.DirSL, strategy.DirLS} {
		t.Run(dir.String(), func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			st, err := store.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := st.Close(); err != nil {
					t.Error(err)
				}
			}()
			count := 1
			if dir == strategy.DirSL {
				count = 2
			}
			qty := .02 * float64(count)
			leftQty := -float64(dir) * qty
			left := &crossWorkflowVenue{actorTestVenue: newActorTestVenue("left")}
			right := &crossWorkflowVenue{actorTestVenue: newActorTestVenue("right")}
			for i, v := range []*crossWorkflowVenue{left, right} {
				positionQty := leftQty
				if i == 1 {
					positionQty = -leftQty
				}
				v.position = domain.Position{MarketID: 1, Qty: positionQty, AvgEntry: 100,
					LiquidationPrice: 150, MarginMode: domain.MarginCross, Notional: math.Abs(positionQty) * 100}
				if positionQty > 0 {
					v.position.LiquidationPrice = 0
				}
			}
			steps := make([]strategy.Step, count)
			for i := range steps {
				steps[i] = strategy.Step{ID: int64(i + 1), Dir: dir, Qty: .02, OpenL: 100, OpenR: 100}
			}
			if err := st.Save("TEST_left_right", store.PairState{NextID: int64(count + 1), Steps: steps}); err != nil {
				t.Fatal(err)
			}
			decisions := make(chan crossWorkflowDecision, 64)
			inst := domain.Instrument{MarketID: 1, SizeDecimals: 4, PriceDecimals: 2}
			actor, err := NewPairActor(PairConfig{
				Pair: symbols.NewPair("TEST", inst, inst), L: left, R: right, Store: st,
				Strategy: crossWorkflowStrategy{decisions: decisions}, Est: actorTestEstimator{},
				Params:  strategy.Params{StepQty: .02, MaxSteps: 15, DepthMult: 2, VolK: 1},
				ExecCfg: execution.DefaultConfig(), IDs: execution.NewIDGen(),
				SampleEvery: 100 * time.Millisecond, MaxBookAge: time.Second,
				IntervalMin: time.Millisecond, IntervalMax: time.Millisecond,
				Risk: marginConfig{floor: .2, trigger: .1, hysteresis: .05, refresh: 100 * time.Millisecond},
			})
			if err != nil {
				t.Fatal(err)
			}
			feedDone := make(chan struct{})
			go func() {
				defer close(feedDone)
				ticker := time.NewTicker(20 * time.Millisecond)
				defer ticker.Stop()
				for {
					for i, v := range []*crossWorkflowVenue{left, right} {
						bid := 100.0
						if dir == strategy.DirSL && i == 0 || dir == strategy.DirLS && i == 1 {
							bid = 100.05
						}
						book := actorTestBook(v.name, bid)
						book.Asks[0].Price = bid + .01
						select {
						case v.books <- book:
						default:
						}
					}
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
					}
				}
			}()
			done := make(chan error, 1)
			go func() { done <- actor.Run(ctx) }()
			defer func() {
				cancel()
				if err := <-done; err != nil {
					t.Error(err)
				}
				<-feedDone
				if left.submitted.Load()+right.submitted.Load() != 0 {
					t.Error("workflow submitted an order")
				}
			}()
			await := func(mode strategy.Mode, kind strategy.Kind) {
				t.Helper()
				for {
					select {
					case d := <-decisions:
						if d.mode == mode && d.action.Kind == kind {
							if kind == strategy.Open && (d.action.Dir != dir || d.action.Qty != .02) {
								t.Fatalf("wrong opening: %+v", d.action)
							}
							return
						}
					case <-ctx.Done():
						t.Fatalf("did not reach mode=%s action=%s", mode, kind)
					}
				}
			}
			await(strategy.ModeReduceOnly, strategy.None)
			left.setRisk(true)
			right.setRisk(true)
			await(strategy.ModeRunning, strategy.Open)
			left.setRisk(false)
			right.setRisk(false)
			await(strategy.ModeReduceOnly, strategy.None)
			left.setRisk(true)
			right.setRisk(true)
			await(strategy.ModeRunning, strategy.Open)
		})
	}
}
