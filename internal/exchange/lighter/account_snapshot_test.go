package lighter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
)

var _ exchange.AccountSnapshotter = (*Venue)(nil)

func TestAccountSnapshotOptionalRiskData(t *testing.T) {
	const position = `"market_id":7,"symbol":"TEST","sign":1,"position":"0.04","avg_entry_price":"700","liquidation_price":"0"`
	for _, tc := range []struct {
		name, positionFields, accountFields string
		wantMode                            domain.MarginMode
		wantNotional                        float64
		wantRisk                            *domain.CrossMarginRisk
	}{
		{
			name:           "complete cross",
			positionFields: `,"margin_mode":0,"position_value":"28"`,
			accountFields:  `,"cross_asset_value":"50","cross_maintenance_margin_requirement":"0.5"`,
			wantMode:       domain.MarginCross, wantNotional: 28,
			wantRisk: &domain.CrossMarginRisk{Equity: 50, Maintenance: 0.5},
		},
		{
			name:           "isolated with valid cross account data",
			positionFields: `,"margin_mode":1,"position_value":"28"`,
			accountFields:  `,"cross_asset_value":"50","cross_maintenance_margin_requirement":"0"`,
			wantMode:       domain.MarginIsolated, wantNotional: 28,
			wantRisk: &domain.CrossMarginRisk{Equity: 50},
		},
		{
			name:           "negative equity is risk data",
			positionFields: `,"margin_mode":0,"position_value":"28"`,
			accountFields:  `,"cross_asset_value":"-1","cross_maintenance_margin_requirement":"0.5"`,
			wantMode:       domain.MarginCross, wantNotional: 28,
			wantRisk: &domain.CrossMarginRisk{Equity: -1, Maintenance: 0.5},
		},
		{
			name:          "zero equity is risk data",
			accountFields: `,"cross_asset_value":"0","cross_maintenance_margin_requirement":"0"`,
			wantRisk:      &domain.CrossMarginRisk{},
		},
		{name: "legacy fields only"},
		{name: "missing equity", accountFields: `,"cross_maintenance_margin_requirement":"0.5"`},
		{name: "missing maintenance", accountFields: `,"cross_asset_value":"50"`},
		{name: "null equity", accountFields: `,"cross_asset_value":null,"cross_maintenance_margin_requirement":"0.5"`},
		{name: "numeric equity", accountFields: `,"cross_asset_value":50,"cross_maintenance_margin_requirement":"0.5"`},
		{name: "object maintenance", accountFields: `,"cross_asset_value":"50","cross_maintenance_margin_requirement":{}`},
		{name: "nan equity", accountFields: `,"cross_asset_value":"NaN","cross_maintenance_margin_requirement":"0.5"`},
		{name: "infinite equity", accountFields: `,"cross_asset_value":"+Inf","cross_maintenance_margin_requirement":"0.5"`},
		{name: "nan maintenance", accountFields: `,"cross_asset_value":"50","cross_maintenance_margin_requirement":"NaN"`},
		{name: "negative maintenance", accountFields: `,"cross_asset_value":"50","cross_maintenance_margin_requirement":"-0.5"`},
		{name: "unknown margin enum", positionFields: `,"margin_mode":2`},
		{name: "null margin mode", positionFields: `,"margin_mode":null`},
		{name: "string margin mode", positionFields: `,"margin_mode":"0"`},
		{name: "fractional margin mode", positionFields: `,"margin_mode":0.5`},
		{name: "invalid notional type", positionFields: `,"margin_mode":0,"position_value":28`, wantMode: domain.MarginCross},
		{name: "negative notional", positionFields: `,"position_value":"-28"`},
		{name: "zero notional", positionFields: `,"position_value":"0"`},
		{name: "nan notional", positionFields: `,"position_value":"NaN"`},
		{name: "infinite notional", positionFields: `,"position_value":"+Inf"`},
		{name: "null notional", positionFields: `,"position_value":null`},
		{
			name:           "bad optional types preserve healthy liquidation data",
			positionFields: `,"margin_mode":{},"position_value":false`,
			accountFields:  `,"cross_asset_value":[],"cross_maintenance_margin_requirement":true`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fields := position
			if tc.name == "bad optional types preserve healthy liquidation data" {
				fields = strings.Replace(fields, `"liquidation_price":"0"`, `"liquidation_price":"350"`, 1)
			}
			body := fmt.Sprintf(`{"code":200,"accounts":[{"positions":[{%s%s}]%s}]}`, fields, tc.positionFields, tc.accountFields)
			r := &restClient{host: "https://offline.invalid", client: responseClient(200, body)}
			snapshot, err := r.accountSnapshot(context.Background(), 9)
			if err != nil {
				t.Fatalf("optional risk data discarded positions: %v", err)
			}
			if len(snapshot.Positions) != 1 {
				t.Fatalf("positions=%+v", snapshot.Positions)
			}
			p := snapshot.Positions[0]
			if p.MarketID != 7 || p.Symbol != "TEST" || p.Qty != 0.04 || p.AvgEntry != 700 || p.MarginMode != tc.wantMode || p.Notional != tc.wantNotional {
				t.Fatalf("position=%+v", p)
			}
			wantLiq := 0.0
			if tc.name == "bad optional types preserve healthy liquidation data" {
				wantLiq = 350
			}
			if p.LiquidationPrice != wantLiq || !reflect.DeepEqual(snapshot.CrossMargin, tc.wantRisk) {
				t.Fatalf("liquidation=%v risk=%+v, want liquidation=%v risk=%+v", p.LiquidationPrice, snapshot.CrossMargin, wantLiq, tc.wantRisk)
			}
		})
	}
}

func TestAccountSnapshotStillRequiresValidPositions(t *testing.T) {
	for _, tc := range []struct {
		name, fields string
	}{
		{name: "negative quantity", fields: `"position":"-0.04","avg_entry_price":"700","liquidation_price":"0"`},
		{name: "invalid entry", fields: `"position":"0.04","avg_entry_price":"NaN","liquidation_price":"0"`},
		{name: "missing entry", fields: `"position":"0.04","liquidation_price":"0"`},
		{name: "negative liquidation", fields: `"position":"0.04","avg_entry_price":"700","liquidation_price":"-1"`},
		{name: "infinite liquidation", fields: `"position":"0.04","avg_entry_price":"700","liquidation_price":"+Inf"`},
		{name: "missing liquidation", fields: `"position":"0.04","avg_entry_price":"700"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := fmt.Sprintf(`{"code":200,"accounts":[{"positions":[{"market_id":7,"sign":1,"margin_mode":0,"position_value":"28",%s}],"cross_asset_value":"50","cross_maintenance_margin_requirement":"0.5"}]}`, tc.fields)
			r := &restClient{host: "https://offline.invalid", client: responseClient(200, body)}
			if snapshot, err := r.accountSnapshot(context.Background(), 9); err == nil {
				t.Fatalf("accepted invalid positions: %+v", snapshot)
			}
		})
	}
}

func TestVenueAccountSnapshotUsesOneResponseAndPositionsRemainCompatible(t *testing.T) {
	v := testVenue(t, true)
	requests := 0
	v.rest.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requests++
		if req.Method != http.MethodGet || req.URL.Path != "/api/v1/account" || req.URL.Query().Get("by") != "index" || req.URL.Query().Get("value") != fmt.Sprint(v.cfg.AccountIndex) {
			t.Errorf("unexpected account request: %s %s", req.Method, req.URL.Path)
		}
		// Make each response different, exposing accidental separate reads.
		body := fmt.Sprintf(`{"code":200,"accounts":[{"positions":[{"market_id":7,"symbol":"TEST","sign":-1,"position":"0.04","avg_entry_price":"700","liquidation_price":"900","margin_mode":0,"position_value":"%d"}],"cross_asset_value":"%d","cross_maintenance_margin_requirement":"0.5"}]}`, requests*28, requests*50)
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}
	snapshot, err := v.AccountSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if requests != 1 || len(snapshot.Positions) != 1 || snapshot.Positions[0].Notional != 28 || snapshot.Positions[0].Qty != -0.04 || snapshot.CrossMargin == nil || snapshot.CrossMargin.Equity != 50 {
		t.Fatalf("requests=%d snapshot=%+v", requests, snapshot)
	}
	ps, err := v.Positions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if requests != 2 || len(ps) != 1 || ps[0].Notional != 56 || ps[0].MarginMode != domain.MarginCross || ps[0].Qty != -0.04 {
		t.Fatalf("requests=%d positions=%+v", requests, ps)
	}
}

func TestAccountSnapshotRequiresCredentials(t *testing.T) {
	v := testVenue(t, false)
	v.rest.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("read-only venue must preserve the existing credential guard")
		return nil, errors.New("unexpected request")
	})}
	if _, err := v.AccountSnapshot(context.Background()); !errors.Is(err, domain.ErrNoCredentials) {
		t.Fatalf("snapshot error=%v", err)
	}
	if _, err := v.Positions(context.Background()); !errors.Is(err, domain.ErrNoCredentials) {
		t.Fatalf("positions error=%v", err)
	}
}
