package lighter

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"testing"
)

func keyResponse(key string) string {
	return fmt.Sprintf(`{"code":200,"api_keys":[{"account_index":1,"api_key_index":4,"public_key":%q}]}`, key)
}

func TestKeyCheckDoesNotShareSDKCacheAcrossVenues(t *testing.T) {
	left := testVenue(t, true)
	pub := left.tx.GetKeyManager().PubKeyBytes()
	left.rest.client = responseClient(200, keyResponse(hex.EncodeToString(pub[:])))
	if err := left.CheckKey(context.Background()); err != nil {
		t.Fatal(err)
	}
	right, err := New("secondary", "rhlighter", map[string]string{"private_key": left.cfg.PrivateKey, "account_index": "1", "api_key_index": "4"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := right.Close(); err != nil {
			t.Error(err)
		}
	})
	right.rest.client = responseClient(200, keyResponse("00"))
	if err := right.CheckKey(context.Background()); err == nil {
		t.Fatal("another venue's key registration was accepted")
	}
	if left.tx.HTTP() != nil || right.tx.HTTP() != nil {
		t.Fatal("signer can still silently use SDK HTTP client")
	}
}

func TestKeyCheckRejectsMissingFieldsAndFollowsContext(t *testing.T) {
	for _, body := range []string{`{"code":200}`, `{"code":200,"api_keys":[]}`, `{"code":200,"api_keys":[{"api_key_index":4,"public_key":"00"}]}`, `{"code":200,"api_keys":[{"account_index":1,"public_key":"00"}]}`} {
		t.Run(body, func(t *testing.T) {
			v := testVenue(t, true)
			v.rest.client = responseClient(200, body)
			if err := v.CheckKey(context.Background()); err == nil {
				t.Fatal("incomplete key data accepted")
			}
		})
	}
	v := testVenue(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	v.rest.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) { return nil, req.Context().Err() })}
	if err := v.CheckKey(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("key check context=%v", err)
	}
}
