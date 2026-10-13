package backtest

import (
	"testing"
	"time"
)

func TestReplayCapturesOscillation(t *testing.T) {
	t0 := time.Now().UnixNano()
	var samples []Sample
	for i := 0; i < 40000; i++ {
		ls := 3.0
		if i%2 == 1 {
			ls = -3.0
		}
		samples = append(samples, Sample{Ts: t0 + int64(i)*100e6, Mid: 0, LS: ls, SL: -ls})
	}
	r := Replay(samples, Params{StepQty: 1, MaxSteps: 10, OpenBps: 1, CloseBps: 1, VolK: 0, FeeBps: 0})
	if r.RoundTrips == 0 {
		t.Fatal("expected round trips")
	}
	if r.NetBps <= 0 {
		t.Fatalf("net should be positive on a mean-reverting series: %+v", r)
	}
}
