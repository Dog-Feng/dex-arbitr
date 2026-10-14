package lighter

import (
	"context"
	"encoding/json"
	"errors"
	"mime"
	"mime/multipart"
	"net/http"
	"testing"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/elliottech/lighter-go/types/txtypes"
)

func TestLeverageDoesNotClassifyUnverifiedRemoteCodes(t *testing.T) {
	v := testVenue(t, true)
	v.nonce.fetch = func(context.Context) (int64, error) { return 0, nil }
	v.rest.client = responseClient(200, `{"code":21113}`)
	err := v.SetLeverage(context.Background(), validRequest().Inst, 3)
	if err == nil || errors.Is(err, domain.ErrLeverageRejected) || errors.Is(err, domain.ErrRejected) {
		t.Fatalf("unverified code incorrectly classified: %v", err)
	}
}

func TestLeverageRoundsMarginUp(t *testing.T) {
	v := testVenue(t, true)
	v.nonce.fetch = func(context.Context) (int64, error) { return 0, nil }
	var sent txtypes.L2UpdateLeverageTxInfo
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
	if err := v.SetLeverage(context.Background(), validRequest().Inst, 3); err != nil {
		t.Fatal(err)
	}
	if sent.InitialMarginFraction != 3334 {
		t.Fatalf("margin fraction=%d, want3334", sent.InitialMarginFraction)
	}
}
