package main

import (
	"strings"
	"testing"
	"time"
)

func TestValidateFlags(t *testing.T) {
	if err := validateFlags("", "lenses.json", "out.jsonl", time.Second); err == nil {
		t.Fatal("empty config accepted")
	}
	if err := validateFlags("c.yaml", "", "out.jsonl", time.Second); err == nil {
		t.Fatal("empty lenses accepted")
	}
	if err := validateFlags("c.yaml", "lenses.json", "out.jsonl", 0); err == nil {
		t.Fatal("non-positive interval accepted")
	}
	if err := validateFlags("c.yaml", "lenses.json", "out.jsonl", time.Second); err != nil {
		t.Fatalf("valid flags rejected: %v", err)
	}
}

func TestLoadLenses(t *testing.T) {
	good := `{"lenses":[
		{"name":"arb","strategy":"arbitrage","estimator_kind":"sma","window":"2h","min_warmup":"1h","min_coverage":0.8,"interval":"1s","open_bps":3,"vol_k":1,"depth_mult":3,"step_qty":0.01,"max_steps":50},
		{"name":"z","strategy":"zscore","estimator_kind":"zscore","min_warmup":"15m","interval":"1s","mu_half_life":"15m","sigma_half_life":"3h","sigma_floor":0.5,"z_open":2,"step_z":0.5,"depth_mult":3,"step_qty":0.01,"max_steps":50},
		{"name":"vol","strategy":"volume","estimator_kind":"sma","window":"2h","min_warmup":"1h","min_coverage":0.8,"interval":"1s","open_bps":5,"vol_k":1,"min_inventory":2,"depth_mult":3,"step_qty":0.01,"max_steps":50}
	]}`
	lenses, err := loadLenses(strings.NewReader(good))
	if err != nil {
		t.Fatal(err)
	}
	if len(lenses) != 3 || lenses[0].Name != "arb" || lenses[1].Strategy != "zscore" || lenses[2].Strategy != "volume" {
		t.Fatalf("lenses parsed wrong: %+v", lenses)
	}
	if lenses[1].Estimator.MuHalfLife != 15*time.Minute || lenses[1].Estimator.SigmaHalfLife != 3*time.Hour {
		t.Fatalf("zscore estimator params wrong: %+v", lenses[1].Estimator)
	}

	dup := `{"lenses":[{"name":"a","strategy":"arbitrage"},{"name":"a","strategy":"volume"}]}`
	if _, err := loadLenses(strings.NewReader(dup)); err == nil {
		t.Fatal("duplicate lens names accepted")
	}
	bad := `{"lenses":[{"name":"a","strategy":"momentum"}]}`
	if _, err := loadLenses(strings.NewReader(bad)); err == nil {
		t.Fatal("unknown strategy accepted")
	}
	badDur := `{"lenses":[{"name":"a","strategy":"arbitrage","window":"soon"}]}`
	if _, err := loadLenses(strings.NewReader(badDur)); err == nil {
		t.Fatal("bad duration accepted")
	}
	empty := `{"lenses":[]}`
	if _, err := loadLenses(strings.NewReader(empty)); err == nil {
		t.Fatal("empty lens list accepted")
	}
}
