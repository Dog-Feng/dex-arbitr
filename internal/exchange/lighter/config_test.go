package lighter

import (
	"net/url"
	"testing"
)

func TestVenueConfigRejectsMalformedEndpointsAndDepth(t *testing.T) {
	for _, opts := range []map[string]string{
		{"host": "http://%"},
		{"host": "ftp://offline.invalid"},
		{"host": "https://offline.invalid?auth=hidden"},
		{"ws_url": "http://offline.invalid/stream"},
		{"ws_url": "wss://%"},
		{"book_depth": "0"},
		{"book_depth": "-1"},
		{"book_depth": "bad"},
	} {
		t.Run("config", func(t *testing.T) {
			if _, err := New("offline", "lighter", opts); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}

func TestReadOnlyWebsocketPreservesExistingQuery(t *testing.T) {
	v, err := New("offline", "lighter", map[string]string{"ws_url": "wss://offline.invalid/stream?session=test"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := v.Close(); err != nil {
			t.Error(err)
		}
	}()
	u, err := url.Parse(v.ws.url)
	if err != nil {
		t.Fatal(err)
	}
	if u.Query().Get("session") != "test" || u.Query().Get("readonly") != "true" {
		t.Fatalf("websocket query=%v", u.Query())
	}
}
