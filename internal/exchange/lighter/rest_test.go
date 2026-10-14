package lighter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func responseClient(status int, body string) *http.Client {
	return &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}
}

func TestSendTxRequiresCompleteSuccess(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		wantHash   string
		wantAPI    bool
	}{
		{name: "success", status: 200, body: `{"code":200,"tx_hash":"abc"}`, wantHash: "abc"},
		{name: "missing code", status: 200, body: `{"tx_hash":"abc"}`},
		{name: "null code", status: 200, body: `{"code":null,"tx_hash":"abc"}`},
		{name: "null body", status: 200, body: `null`},
		{name: "invalid JSON", status: 200, body: `{`},
		{name: "missing hash", status: 200, body: `{"code":200}`},
		{name: "blank hash", status: 200, body: `{"code":200,"tx_hash":" "}`},
		{name: "gateway missing code", status: 502, body: `{"message":"gateway"}`},
		{name: "gateway with code", status: 503, body: `{"code":200,"tx_hash":"abc"}`},
		{name: "unverified business rejection", status: 200, body: `{"code":21111,"message":"error"}`, wantAPI: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &restClient{host: "https://offline.invalid", client: responseClient(tc.status, tc.body)}
			hash, err := r.sendTx(context.Background(), 1, "{}")
			if hash != tc.wantHash || (err == nil) != (tc.wantHash != "") {
				t.Fatalf("sendTx=(%q,%v), want hash %q", hash, err, tc.wantHash)
			}
			if errors.Is(err, domain.ErrRejected) {
				t.Fatal("a remote response cannot prove no transaction was sent")
			}
			var ae *apiError
			if ok := errors.As(err, &ae); ok != tc.wantAPI {
				t.Fatalf("API classification=%v, want %v", ok, tc.wantAPI)
			}
		})
	}
}

type brokenBody struct{}

func (brokenBody) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (brokenBody) Close() error             { return nil }

func TestSendTxReadFailureKeepsUnknown(t *testing.T) {
	r := &restClient{host: "https://offline.invalid", client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: brokenBody{}, Header: make(http.Header)}, nil
	})}}
	_, err := r.sendTx(context.Background(), 1, "{}")
	if !errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, domain.ErrRejected) {
		t.Fatalf("read error=%v", err)
	}
}

func TestRESTInvalidHostReturnsError(t *testing.T) {
	r := &restClient{host: "http://%"}
	var out resultCode
	if err := r.get(context.Background(), "account", nil, &out); err == nil {
		t.Fatal("GET accepted invalid host")
	}
	if _, err := r.sendTx(context.Background(), 1, "{}"); !errors.Is(err, domain.ErrRejected) {
		t.Fatalf("invalid request must fail before send: %v", err)
	}
}

func TestPositionsRequiresExplicitCompleteData(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		wantQty    float64
		wantOK     bool
	}{
		{name: "flat", body: `{"code":200,"accounts":[{"positions":[]}]}`, wantOK: true},
		{name: "missing accounts", body: `{"code":200}`},
		{name: "missing positions", body: `{"code":200,"accounts":[{}]}`},
		{name: "null positions", body: `{"code":200,"accounts":[{"positions":null}]}`},
		{name: "ambiguous accounts", body: `{"code":200,"accounts":[{"positions":[]},{"positions":[]}]}`},
		{name: "long", body: `{"code":200,"accounts":[{"positions":[{"market_id":1,"sign":1,"position":"0.1","avg_entry_price":"10","liquidation_price":"5"}]}]}`, wantQty: 0.1, wantOK: true},
		{name: "short", body: `{"code":200,"accounts":[{"positions":[{"market_id":1,"sign":-1,"position":"0.1","avg_entry_price":"10","liquidation_price":"15"}]}]}`, wantQty: -0.1, wantOK: true},
		{name: "nonfinite", body: `{"code":200,"accounts":[{"positions":[{"market_id":1,"sign":1,"position":"NaN"}]}]}`},
		{name: "missing sign", body: `{"code":200,"accounts":[{"positions":[{"market_id":1,"position":"0.1","avg_entry_price":"10","liquidation_price":"5"}]}]}`},
		{name: "missing market", body: `{"code":200,"accounts":[{"positions":[{"position":"0","sign":1}]}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &restClient{host: "https://offline.invalid", client: responseClient(200, tc.body)}
			ps, err := r.positions(context.Background(), 1)
			if (err == nil) != tc.wantOK {
				t.Fatalf("positions=%v, err=%v", ps, err)
			}
			if tc.wantOK && tc.wantQty != 0 && (len(ps) != 1 || ps[0].Qty != tc.wantQty) {
				t.Fatalf("positions=%v", ps)
			}
		})
	}
}

func TestNextNonceRequiresField(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		wantOK     bool
	}{
		{name: "zero", body: `{"code":200,"nonce":0}`, wantOK: true},
		{name: "missing", body: `{"code":200}`},
		{name: "negative", body: `{"code":200,"nonce":-1}`},
		{name: "missing code", body: `{"nonce":0}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &restClient{host: "https://offline.invalid", client: responseClient(200, tc.body)}
			_, err := r.nextNonce(context.Background(), 1, 4)
			if (err == nil) != tc.wantOK {
				t.Fatalf("nonce err=%v", err)
			}
		})
	}
}

func TestInstrumentsRequirePrecisionFields(t *testing.T) {
	for _, tc := range []struct {
		name, fields string
		wantOK       bool
	}{
		{name: "complete", fields: `"market_id":1,"supported_size_decimals":3,"supported_price_decimals":2`, wantOK: true},
		{name: "missing market", fields: `"supported_size_decimals":3,"supported_price_decimals":2`},
		{name: "missing size precision", fields: `"market_id":1,"supported_price_decimals":2`},
		{name: "missing price precision", fields: `"market_id":1,"supported_size_decimals":3`},
		{name: "negative precision", fields: `"market_id":1,"supported_size_decimals":-1,"supported_price_decimals":2`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := testVenue(t, false)
			v.rest.client = responseClient(200, `{"code":200,"order_books":[{"symbol":"TEST","market_type":"perp","status":"active","min_base_amount":"0.01","min_quote_amount":"1","maker_fee":"0","taker_fee":"0.001",`+tc.fields+`}]}`)
			_, err := v.Instruments(context.Background())
			if (err == nil) != tc.wantOK {
				t.Fatalf("instruments error=%v", err)
			}
		})
	}
}

func TestOrderQueryDoesNotResolveAcrossMarkets(t *testing.T) {
	v := testVenue(t, true)
	v.rest.client = responseClient(200, `{"code":200,"orders":[{"client_order_index":1,"market_index":2,"filled_base_amount":"0","filled_quote_amount":"0","status":"canceled"}]}`)
	if _, err := v.QueryOrder(context.Background(), validRequest().Inst, 1); err == nil {
		t.Fatal("terminal outcome from another market accepted")
	}
}

func TestOrderHistoryBoundIsExplicitlyUnresolved(t *testing.T) {
	v := testVenue(t, true)
	orders := make([]string, 100)
	for i := range orders {
		orders[i] = fmt.Sprintf(`{"client_order_index":%d,"market_index":1}`, i+2)
	}
	v.rest.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		body := `{"code":200,"orders":[]}`
		if strings.Contains(req.URL.Path, "Inactive") {
			body = `{"code":200,"orders":[` + strings.Join(orders, ",") + `]}`
		}
		return responseClient(200, body).Transport.RoundTrip(req)
	})}
	u, err := v.QueryOrder(context.Background(), validRequest().Inst, 1)
	if err == nil || u.Status.Terminal() {
		t.Fatalf("bounded history looked resolved: %+v %v", u, err)
	}
}
