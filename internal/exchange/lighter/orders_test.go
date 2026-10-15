package lighter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"mime"
	"mime/multipart"
	"net/http"
	"testing"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	lclient "github.com/elliottech/lighter-go/client"
	"github.com/elliottech/lighter-go/types/txtypes"
)

func testVenue(t *testing.T, signing bool) *Venue {
	t.Helper()
	opts := map[string]string{"host": "https://offline.invalid", "ws_url": "wss://offline.invalid/stream"}
	if signing {
		key, _, err := lclient.GenerateAPIKey()
		if err != nil {
			t.Fatal(err)
		}
		opts["private_key"], opts["account_index"], opts["api_key_index"] = key, "1", "4"
	}
	v, err := New("test", "lighter", opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := v.Close(); err != nil {
			t.Error(err)
		}
	})
	return v
}

func TestOrderRequestRoundsBoundTightly(t *testing.T) {
	inst := domain.Instrument{MarketID: 1, SizeDecimals: 2, PriceDecimals: 2}
	req, err := orderRequest(domain.OrderReq{ClientID: 1, Inst: inst, Side: domain.Buy, Qty: 1, Price: 100.005, Type: domain.Limit})
	if err != nil {
		t.Fatal(err)
	}
	if req.Price != 10000 {
		t.Fatalf("buy worst-case bound widened by rounding: price=%d", req.Price)
	}
	req, err = orderRequest(domain.OrderReq{ClientID: 2, Inst: inst, Side: domain.Sell, Qty: 1, Price: 99.995, Type: domain.Limit})
	if err != nil {
		t.Fatal(err)
	}
	if req.Price != 10000 {
		t.Fatalf("sell worst-case bound widened by rounding: price=%d", req.Price)
	}
}

func TestOrderRequestPreservesNativePriceUnits(t *testing.T) {
	units := []uint32{1, 2, 9, 10, 99, 100, 101, 1<<24 - 1, 1 << 24, 1<<25 - 1, 1<<25 + 1, 33554401, 33554404, 1 << 31, math.MaxUint32 - 1, math.MaxUint32}
	// Exercise the whole supported uint32 price range at every precision.
	seed := uint32(1)
	for i := 0; i < 128; i++ {
		seed = 1664525*seed + 1013904223
		units = append(units, max(1, seed))
	}
	for decimals := 0; decimals <= 18; decimals++ {
		t.Run(fmt.Sprintf("decimals_%d", decimals), func(t *testing.T) {
			for _, want := range units {
				price := float64(want) / math.Pow10(decimals)
				for _, side := range []domain.Side{domain.Buy, domain.Sell} {
					r := validRequest()
					r.Inst.PriceDecimals, r.Side = decimals, side
					// The executor aligns before the adapter aligns again.
					r.Price = domain.Floor(price, decimals)
					if side == domain.Sell {
						r.Price = domain.Ceil(price, decimals)
					}
					req, err := orderRequest(r)
					if err != nil || req.Price != want {
						t.Fatalf("side=%s native=%d price=%.17g request=%+v error=%v", side, want, price, req, err)
					}
				}
			}
		})
	}
}

func TestOrderRequestTightBoundNearNativeTick(t *testing.T) {
	for _, tc := range []struct {
		name    string
		side    domain.Side
		price   float64
		units   uint32
		rejects bool
	}{
		{name: "buy below minimum", side: domain.Buy, price: .01 - 5e-12, rejects: true},
		{name: "buy one ulp below minimum", side: domain.Buy, price: math.Nextafter(.01, 0), rejects: true},
		{name: "buy above tick", side: domain.Buy, price: .01 + 5e-12, units: 1},
		{name: "sell above tick", side: domain.Sell, price: .01 + 5e-12, units: 2},
		{name: "sell one ulp above tick", side: domain.Sell, price: math.Nextafter(.01, 1), units: 2},
		{name: "sell below tick", side: domain.Sell, price: .01 - 5e-12, units: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := validRequest()
			r.Side, r.Price = tc.side, tc.price
			req, err := orderRequest(r)
			if tc.rejects {
				if !errors.Is(err, domain.ErrRejected) {
					t.Fatalf("unrepresentable bound accepted: request=%+v error=%v", req, err)
				}
				return
			}
			if err != nil || req.Price != tc.units {
				t.Fatalf("bound=%.17g request=%+v error=%v, want native price %d", tc.price, req, err, tc.units)
			}
		})
	}
}

func TestSignedNativePriceBounds(t *testing.T) {
	v := testVenue(t, true)
	for _, tc := range []struct {
		name     string
		side     domain.Side
		decimals int
		units    uint32
	}{
		{name: "native buy regression", side: domain.Buy, decimals: 3, units: 33554401},
		{name: "native sell regression", side: domain.Sell, decimals: 3, units: 33554404},
		{name: "maximum integer price", side: domain.Buy, units: math.MaxUint32},
		{name: "maximum fractional price", side: domain.Sell, decimals: 18, units: math.MaxUint32},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := validRequest()
			r.Side, r.Inst.PriceDecimals = tc.side, tc.decimals
			r.Price = float64(tc.units) / math.Pow10(tc.decimals)
			body, err := v.SignOnly(r, 0)
			if err != nil {
				t.Fatal(err)
			}
			var signed txtypes.L2CreateOrderTxInfo
			if err := json.Unmarshal([]byte(body), &signed); err != nil {
				t.Fatal(err)
			}
			if signed.Price != tc.units {
				t.Fatalf("signed price=%d, want %d", signed.Price, tc.units)
			}
		})
	}
}

func validRequest() domain.OrderReq {
	return domain.OrderReq{ClientID: 1, Inst: domain.Instrument{MarketID: 1, SizeDecimals: 3, PriceDecimals: 2}, Side: domain.Buy, Qty: 0.01, Price: 100}
}

func TestScaledPriceAndQuantityBounds(t *testing.T) {
	for _, tc := range []struct {
		name   string
		value  float64
		dec    int
		wantOK bool
	}{
		{name: "minimum", value: 0.01, dec: 2, wantOK: true},
		{name: "maximum", value: float64(math.MaxUint32), wantOK: true},
		{name: "negative", value: -0.01, dec: 2},
		{name: "zero"},
		{name: "nan", value: math.NaN()},
		{name: "infinity", value: math.Inf(1)},
		{name: "too big", value: float64(math.MaxUint32) + 1},
		{name: "rounds below step", value: 0.001, dec: 2},
		{name: "negative decimals", value: 1, dec: -1},
		{name: "excessive decimals", value: 1, dec: 19},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := toPrice(tc.value, tc.dec)
			if (err == nil) != tc.wantOK {
				t.Fatalf("price err=%v", err)
			}
		})
	}
	if _, err := toBase(float64(txtypes.MaxOrderBaseAmount)+1, 0); err == nil {
		t.Fatal("quantity overflow accepted")
	}
	if _, err := toBase(0.0101, 3); err == nil {
		t.Fatal("native quantity precision silently rounded")
	}
}

func TestOrderRequestRejectsInvalidValuesBeforeSigning(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*domain.OrderReq)
	}{
		{name: "negative market", change: func(r *domain.OrderReq) { r.Inst.MarketID = -1 }},
		{name: "nil market", change: func(r *domain.OrderReq) { r.Inst.MarketID = int(txtypes.NilMarketIndex) }},
		{name: "market overflow", change: func(r *domain.OrderReq) { r.Inst.MarketID = int(txtypes.MaxMarketIndex) + 1 }},
		{name: "zero client id", change: func(r *domain.OrderReq) { r.ClientID = 0 }},
		{name: "client overflow", change: func(r *domain.OrderReq) { r.ClientID = txtypes.MaxClientOrderIndex + 1 }},
		{name: "side", change: func(r *domain.OrderReq) { r.Side = 0 }},
		{name: "type", change: func(r *domain.OrderReq) { r.Type = 2 }},
		{name: "negative price", change: func(r *domain.OrderReq) { r.Price = -1 }},
		{name: "nonfinite qty", change: func(r *domain.OrderReq) { r.Qty = math.Inf(1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := validRequest()
			tc.change(&r)
			if _, err := orderRequest(r); !errors.Is(err, domain.ErrRejected) {
				t.Fatalf("request error=%v", err)
			}
		})
	}
}

func TestSignOnlyAndSubmitUseIdenticalOrderFields(t *testing.T) {
	v := testVenue(t, true)
	v.nonce.fetch = func(context.Context) (int64, error) { return 7, nil }
	for _, tc := range []struct {
		name   string
		typ    domain.OrderType
		reduce bool
		side   domain.Side
	}{
		{name: "limit buy", typ: domain.Limit, side: domain.Buy},
		{name: "reduce market sell", typ: domain.Market, reduce: true, side: domain.Sell},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := validRequest()
			r.Type, r.ReduceOnly, r.Side = tc.typ, tc.reduce, tc.side
			js, err := v.SignOnly(r, 7)
			if err != nil {
				t.Fatal(err)
			}
			var signed txtypes.L2CreateOrderTxInfo
			if err := json.Unmarshal([]byte(js), &signed); err != nil {
				t.Fatal(err)
			}
			var sent txtypes.L2CreateOrderTxInfo
			v.rest.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				_, params, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
				if err != nil {
					return nil, err
				}
				form, err := multipart.NewReader(req.Body, params["boundary"]).ReadForm(1 << 20)
				if err != nil {
					return nil, err
				}
				defer func() {
					if err := form.RemoveAll(); err != nil {
						t.Error(err)
					}
				}()
				if err := json.Unmarshal([]byte(form.Value["tx_info"][0]), &sent); err != nil {
					return nil, err
				}
				return responseClient(200, `{"code":200,"tx_hash":"offline"}`).Transport.RoundTrip(req)
			})}
			if _, err := v.Submit(context.Background(), r); err != nil {
				t.Fatal(err)
			}
			if *signed.OrderInfo != *sent.OrderInfo {
				t.Fatalf("signed fields=%+v sent=%+v", signed.OrderInfo, sent.OrderInfo)
			}
			if signed.Type != uint8(tc.typ) || signed.ReduceOnly != boolByte(tc.reduce) || signed.IsAsk != boolByte(tc.side == domain.Sell) {
				t.Fatalf("wrong order fields: %+v", signed.OrderInfo)
			}
		})
	}
}

func boolByte(v bool) uint8 {
	if v {
		return 1
	}
	return 0
}

func TestSubmitRemoteErrorStaysUnknownAndResyncsNonce(t *testing.T) {
	v := testVenue(t, true)
	fetches := 0
	v.nonce.fetch = func(context.Context) (int64, error) { fetches++; return int64(fetches), nil }
	v.rest.client = responseClient(200, `{"code":21111}`)
	for i := 0; i < 2; i++ {
		ack, err := v.Submit(context.Background(), validRequest())
		if err == nil || errors.Is(err, domain.ErrRejected) || ack.ClientID != 1 {
			t.Fatalf("ack=%+v error=%v", ack, err)
		}
	}
	if fetches != 2 {
		t.Fatalf("nonce fetches=%d", fetches)
	}
}

func TestNonceCancellationAndOfflineSigning(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	n := nonceMgr{fetch: func(context.Context) (int64, error) { t.Fatal("cancelled nonce was fetched"); return 0, nil }}
	if _, err := n.take(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("nonce=%v", err)
	}
	v := testVenue(t, true)
	if _, err := v.SignOnly(validRequest(), -1); err == nil {
		t.Fatal("implicit SDK nonce fetch accepted")
	}
	if _, err := v.SignEphemeral(validRequest(), -1); err == nil {
		t.Fatal("negative offline nonce accepted")
	}
	if _, err := v.SignEphemeral(validRequest(), 0); err != nil {
		t.Fatal(err)
	}
	v.sendMu <- struct{}{}
	if err := v.lockSend(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled send wait=%v", err)
	}
	<-v.sendMu
}
