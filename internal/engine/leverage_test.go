package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
)

type fakeLeverageSetter struct {
	max    int
	calls  []int
	reject bool // always return a non-leverage error
}

func (f *fakeLeverageSetter) SetLeverage(_ context.Context, _ domain.Instrument, leverage int) error {
	f.calls = append(f.calls, leverage)
	if f.reject {
		return errors.New("boom")
	}
	if leverage <= f.max {
		return nil
	}
	return domain.ErrLeverageRejected
}

func TestResolveLeverage(t *testing.T) {
	cases := []struct {
		name       string
		max        int
		configured int
		reject     bool
		want       int
		wantErr    bool
	}{
		{name: "market allows configured", max: 10, configured: 5, want: 5},
		{name: "market caps below configured", max: 2, configured: 5, want: 2},
		{name: "market only allows 1x", max: 1, configured: 5, want: 1},
		{name: "transient error aborts", configured: 5, reject: true, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeLeverageSetter{max: tc.max, reject: tc.reject}
			got, err := resolveLeverage(context.Background(), f, domain.Instrument{}, tc.configured)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got leverage %d", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("leverage=%d, want %d", got, tc.want)
			}
			if len(f.calls) == 0 {
				t.Fatalf("expected at least one SetLeverage call")
			}
		})
	}
}
