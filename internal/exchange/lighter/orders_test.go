package lighter

import (
	"context"
	"encoding/json"
	"errors"
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
