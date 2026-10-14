package lighter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
)

var httpc = &http.Client{Timeout: 10 * time.Second}

type restClient struct{ host string }

// apiError is a definitive, API-level error response (code != 200).
type apiError struct {
	Code int
	Msg  string
}

func (e *apiError) Error() string { return fmt.Sprintf("lighter api error %d: %s", e.Code, e.Msg) }

type resultCode struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (r *restClient) get(ctx context.Context, path string, q url.Values, out any) error {
	u := r.host + "/api/v1/" + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	resp, err := httpc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var rc resultCode
	if err := json.Unmarshal(body, &rc); err != nil {
		return fmt.Errorf("GET %s: http %d: %.200s", path, resp.StatusCode, body)
	}
	if rc.Code != 200 {
		return &apiError{Code: rc.Code, Msg: rc.Message}
	}
	return json.Unmarshal(body, out)
}

type orderBookMeta struct {
	Symbol        string `json:"symbol"`
	MarketID      int    `json:"market_id"`
	MarketType    string `json:"market_type"`
	Status        string `json:"status"`
	TakerFee      string `json:"taker_fee"`
	MakerFee      string `json:"maker_fee"`
	MinBase       string `json:"min_base_amount"`
	MinQuote      string `json:"min_quote_amount"`
	SizeDecimals  int    `json:"supported_size_decimals"`
	PriceDecimals int    `json:"supported_price_decimals"`
}

func (r *restClient) orderBooks(ctx context.Context) ([]orderBookMeta, error) {
	var out struct {
		OrderBooks []orderBookMeta `json:"order_books"`
	}
	if err := r.get(ctx, "orderBooks", nil, &out); err != nil {
		return nil, err
	}
	return out.OrderBooks, nil
}

func (r *restClient) nextNonce(ctx context.Context, account int64, key uint8) (int64, error) {
	var out struct {
		Nonce int64 `json:"nonce"`
	}
	q := url.Values{"account_index": {strconv.FormatInt(account, 10)}, "api_key_index": {strconv.Itoa(int(key))}}
	if err := r.get(ctx, "nextNonce", q, &out); err != nil {
		return 0, err
	}
	return out.Nonce, nil
}

func (r *restClient) positions(ctx context.Context, account int64) ([]domain.Position, error) {
	var out struct {
		Accounts []struct {
			Positions []struct {
				MarketID         int    `json:"market_id"`
				Symbol           string `json:"symbol"`
				Sign             int    `json:"sign"`
				Position         string `json:"position"`
				AvgEntry         string `json:"avg_entry_price"`
				LiquidationPrice string `json:"liquidation_price"`
			} `json:"positions"`
		} `json:"accounts"`
	}
	q := url.Values{"by": {"index"}, "value": {strconv.FormatInt(account, 10)}}
	if err := r.get(ctx, "account", q, &out); err != nil {
		return nil, err
	}
	if len(out.Accounts) == 0 {
		return nil, fmt.Errorf("account %d not found", account)
	}
	var ps []domain.Position
	for _, p := range out.Accounts[0].Positions {
		qty := domain.ParseF(p.Position)
		if p.Sign < 0 {
			qty = -qty
		}
		if qty == 0 {
			continue
		}
		ps = append(ps, domain.Position{MarketID: p.MarketID, Symbol: p.Symbol, Qty: qty,
			AvgEntry: domain.ParseF(p.AvgEntry), LiquidationPrice: domain.ParseF(p.LiquidationPrice)})
	}
	return ps, nil
}

// wsOrder mirrors the Lighter Order object (subset).
type wsOrder struct {
	OrderIndex       int64  `json:"order_index"`
	ClientOrderIndex int64  `json:"client_order_index"`
	MarketIndex      int    `json:"market_index"`
	InitialBase      string `json:"initial_base_amount"`
	FilledBase       string `json:"filled_base_amount"`
	FilledQuote      string `json:"filled_quote_amount"`
	Status           string `json:"status"`
	IsAsk            bool   `json:"is_ask"`
	UpdatedAt        int64  `json:"updated_at"`
}

func (r *restClient) accountOrders(ctx context.Context, path string, account int64, market int, auth string) ([]wsOrder, error) {
	var out struct {
		Orders []wsOrder `json:"orders"`
	}
	q := url.Values{
		"account_index": {strconv.FormatInt(account, 10)},
		"market_id":     {strconv.Itoa(market)},
		"auth":          {auth},
	}
	if path == "accountInactiveOrders" {
		q.Set("limit", "100")
	}
	if err := r.get(ctx, path, q, &out); err != nil {
		return nil, err
	}
	return out.Orders, nil
}

// sendTx posts a signed tx. Returns tx hash. *apiError => definitive rejection.
func (r *restClient) sendTx(ctx context.Context, txType uint8, txInfo string) (string, error) {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	_ = w.WriteField("tx_type", strconv.Itoa(int(txType)))
	_ = w.WriteField("tx_info", txInfo)
	_ = w.Close()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, r.host+"/api/v1/sendTx", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	resp, err := httpc.Do(req)
	if err != nil {
		return "", err // unknown outcome
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var out struct {
		resultCode
		TxHash string `json:"tx_hash"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", fmt.Errorf("sendTx: http %d: %.200s", resp.StatusCode, body) // unknown
	}
	if out.Code != 200 {
		return "", &apiError{Code: out.Code, Msg: out.Message}
	}
	return out.TxHash, nil
}

func isAPIError(err error) bool {
	var ae *apiError
	return errors.As(err, &ae)
}

// apiErrorOf unwraps the venue's definitive API rejection, if any.
func apiErrorOf(err error) (*apiError, bool) {
	var ae *apiError
	if errors.As(err, &ae) {
		return ae, true
	}
	return nil, false
}
