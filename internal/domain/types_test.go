package domain

import (
	"math"
	"testing"
)

func TestDecimalRoundingDirections(t *testing.T) {
	for _, tc := range []struct {
		name        string
		value       float64
		decimals    int
		floor, ceil float64
	}{
		{name: "native buy regression", value: 33554.401, decimals: 3, floor: 33554.401, ceil: 33554.401},
		{name: "native sell regression", value: 33554.404, decimals: 3, floor: 33554.404, ceil: 33554.404},
		{name: "extra precision", value: 100.005, decimals: 2, floor: 100, ceil: 100.01},
		{name: "negative extra precision", value: -100.005, decimals: 2, floor: -100.01, ceil: -100},
		{name: "below quantity step", value: .01 - 5e-12, decimals: 2, floor: 0, ceil: .01},
		{name: "above price step", value: .01 + 5e-12, decimals: 2, floor: .01, ceil: .02},
		{name: "one ulp below integer", value: math.Nextafter(1, 0), floor: 0, ceil: 1},
		{name: "one ulp above integer", value: math.Nextafter(1, 2), floor: 1, ceil: 2},
		{name: "one ulp below quantity step", value: math.Nextafter(.01, 0), decimals: 2, floor: 0, ceil: .01},
		{name: "negative near zero", value: -1e-14, decimals: 2, floor: -.01, ceil: math.Copysign(0, -1)},
		{name: "tens", value: 125, decimals: -1, floor: 120, ceil: 130},
		{name: "negative tens", value: -125, decimals: -1, floor: -130, ceil: -120},
		{name: "carry", value: 999.99, decimals: -1, floor: 990, ceil: 1000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, result := range []struct {
				name      string
				got, want float64
			}{{"floor", Floor(tc.value, tc.decimals), tc.floor}, {"ceil", Ceil(tc.value, tc.decimals), tc.ceil}} {
				if result.got != result.want || math.Signbit(result.got) != math.Signbit(result.want) {
					t.Errorf("%s(%0.17g, %d)=%0.17g, want %0.17g", result.name, tc.value, tc.decimals, result.got, result.want)
				}
			}
		})
	}
}

func TestDecimalRoundingFloatLimits(t *testing.T) {
	for _, tc := range []struct {
		name        string
		value       float64
		decimals    int
		floor, ceil float64
	}{
		{name: "largest integer", value: math.MaxFloat64, floor: math.MaxFloat64, ceil: math.MaxFloat64},
		{name: "largest coarse decimal", value: math.MaxFloat64, decimals: -308, floor: 1e308, ceil: math.Inf(1)},
		{name: "smallest unchanged", value: math.SmallestNonzeroFloat64, decimals: 324, floor: math.SmallestNonzeroFloat64, ceil: math.SmallestNonzeroFloat64},
		{name: "smallest rounded", value: math.SmallestNonzeroFloat64, decimals: 323, floor: 0, ceil: 1e-323},
		{name: "large positive precision", value: 1.234, decimals: math.MaxInt, floor: 1.234, ceil: 1.234},
		{name: "large negative precision", value: 1.234, decimals: math.MinInt, floor: 0, ceil: math.Inf(1)},
		{name: "negative large precision", value: -1.234, decimals: math.MinInt, floor: math.Inf(-1), ceil: math.Copysign(0, -1)},
		{name: "positive infinity", value: math.Inf(1), decimals: -309, floor: math.Inf(1), ceil: math.Inf(1)},
		{name: "negative infinity", value: math.Inf(-1), decimals: 309, floor: math.Inf(-1), ceil: math.Inf(-1)},
		{name: "negative zero", value: math.Copysign(0, -1), decimals: 2, floor: math.Copysign(0, -1), ceil: math.Copysign(0, -1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if floor, ceil := Floor(tc.value, tc.decimals), Ceil(tc.value, tc.decimals); floor != tc.floor || ceil != tc.ceil || math.Signbit(floor) != math.Signbit(tc.floor) || math.Signbit(ceil) != math.Signbit(tc.ceil) {
				t.Fatalf("Floor/Ceil(%g,%d)=(%g,%g), want (%g,%g)", tc.value, tc.decimals, floor, ceil, tc.floor, tc.ceil)
			}
		})
	}
	if !math.IsNaN(Floor(math.NaN(), 2)) || !math.IsNaN(Ceil(math.NaN(), 2)) {
		t.Fatal("NaN was not preserved")
	}
}

func TestDecimalRoundingStaysConservativeAndIdempotent(t *testing.T) {
	// Include both sides of integer powers and their immediate float neighbors.
	for exponent := -323; exponent <= 308; exponent++ {
		power := math.Pow10(exponent)
		for _, value := range []float64{power, math.Nextafter(power, 0), math.Nextafter(power, math.Inf(1))} {
			if value == 0 || math.IsInf(value, 0) {
				continue
			}
			for _, sign := range []float64{-1, 1} {
				v := value * sign
				for _, d := range []int{-309, -308, -exponent - 1, -exponent, -exponent + 1, 323, 324} {
					floor, ceil := Floor(v, d), Ceil(v, d)
					if math.IsNaN(floor) || math.IsNaN(ceil) || floor > v || ceil < v {
						t.Fatalf("nonconservative rounding v=%.17g d=%d floor=%.17g ceil=%.17g", v, d, floor, ceil)
					}
					if Floor(floor, d) != floor || Ceil(ceil, d) != ceil {
						t.Fatalf("nonidempotent rounding v=%.17g d=%d floor=%.17g ceil=%.17g", v, d, floor, ceil)
					}
				}
			}
		}
	}
}
