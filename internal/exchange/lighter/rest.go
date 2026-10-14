package lighter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
)

type restClient struct {
	host   string
	client *http.Client
}

func (r *restClient) httpClient() *http.Client {
	if r.client != nil {
		return r.client
	}
	return &http.Client{Timeout: 10 * time.Second}
}

// apiError preserves an explicit API error code. It is not, by itself, proof
// that a submitted transaction cannot execute.
type apiError struct {
	Code int
	Msg  string
}

func (e *apiError) Error() string { return fmt.Sprintf("lighter api error %d: %s", e.Code, e.Msg) }

type resultCode struct {
	Code    *int   `json:"code"`
	Message string `json:"message"`
}

func (r *restClient) get(ctx context.Context, path string, q url.Values, out any) error {
	u := r.host + "/api/v1/" + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return fmt.Errorf("GET %s request: %w", path, err)
	}
	resp, err := r.httpClient().Do(req)
	if err != nil {
		// Do's url.Error contains query auth; retain its cause without the URL.
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return fmt.Errorf("GET %s: %w", path, err)
	}
	defer closeBody(resp.Body)
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var rc resultCode
	if err := json.Unmarshal(body, &rc); err != nil {
		return fmt.Errorf("GET %s: http %d: invalid JSON response: %w", path, resp.StatusCode, err)
	}
	if rc.Code == nil || *rc.Code <= 0 {
		return fmt.Errorf("GET %s: missing or invalid response code", path)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("GET %s: http %d", path, resp.StatusCode)
	}
	if *rc.Code != 200 {
		return &apiError{Code: *rc.Code, Msg: rc.Message}
	}
	return json.Unmarshal(body, out)
}

type orderBookMeta struct {
	Symbol        string `json:"symbol"`
	MarketID      *int   `json:"market_id"`
	MarketType    string `json:"market_type"`
	Status        string `json:"status"`
	TakerFee      string `json:"taker_fee"`
	MakerFee      string `json:"maker_fee"`
	MinBase       string `json:"min_base_amount"`
	MinQuote      string `json:"min_quote_amount"`
	SizeDecimals  *int   `json:"supported_size_decimals"`
	PriceDecimals *int   `json:"supported_price_decimals"`
}

func (r *restClient) orderBooks(ctx context.Context) ([]orderBookMeta, error) {
	var out struct {
		OrderBooks *[]orderBookMeta `json:"order_books"`
	}
	if err := r.get(ctx, "orderBooks", nil, &out); err != nil {
		return nil, err
	}
	if out.OrderBooks == nil {
		return nil, fmt.Errorf("missing order books")
	}
	return *out.OrderBooks, nil
}

func (r *restClient) nextNonce(ctx context.Context, account int64, key uint8) (int64, error) {
	var out struct {
		Nonce *int64 `json:"nonce"`
	}
	q := url.Values{"account_index": {strconv.FormatInt(account, 10)}, "api_key_index": {strconv.Itoa(int(key))}}
	if err := r.get(ctx, "nextNonce", q, &out); err != nil {
		return 0, err
	}
	if out.Nonce == nil || *out.Nonce < 0 {
		return 0, fmt.Errorf("missing or invalid nonce")
	}
	return *out.Nonce, nil
}

func finiteDecimal(s string) (float64, error) {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, fmt.Errorf("invalid decimal value")
	}
	return v, nil
}

// This endpoint and these fields are also used by the pinned SDK. We do not
// share its process-wide account/key cache between different venue hosts.
func (r *restClient) apiKey(ctx context.Context, account int64, key uint8) (string, error) {
	var out struct {
		Keys *[]struct {
			Account   *int64 `json:"account_index"`
			Index     *uint8 `json:"api_key_index"`
			PublicKey string `json:"public_key"`
		} `json:"api_keys"`
	}
	if err := r.get(ctx, "apikeys", url.Values{"account_index": {strconv.FormatInt(account, 10)}}, &out); err != nil {
		return "", err
	}
	if out.Keys == nil {
		return "", fmt.Errorf("missing API keys")
	}
	found := ""
	for _, k := range *out.Keys {
		if k.Account == nil || k.Index == nil || *k.Account != account {
			return "", fmt.Errorf("invalid API key account or index")
		}
		if *k.Index == key {
			if found != "" || k.PublicKey == "" {
				return "", fmt.Errorf("missing or repeated API key")
			}
			found = k.PublicKey
		}
	}
	if found == "" {
		return "", fmt.Errorf("configured API key index not found")
	}
	return found, nil
}

func (r *restClient) positions(ctx context.Context, account int64) ([]domain.Position, error) {
	var out struct {
		Accounts []struct {
			Positions *[]struct {
				MarketID         *int   `json:"market_id"`
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
	if len(out.Accounts) != 1 || out.Accounts[0].Positions == nil {
		return nil, fmt.Errorf("account %d: missing or ambiguous positions", account)
	}
	var ps []domain.Position
	seen := make(map[int]bool)
	for _, p := range *out.Accounts[0].Positions {
		if p.MarketID == nil {
			return nil, fmt.Errorf("missing position market")
		}
		marketID := *p.MarketID
		if _, err := marketIndex(domain.Instrument{MarketID: marketID}); err != nil || seen[marketID] {
			return nil, fmt.Errorf("invalid or repeated position market %d", marketID)
		}
		seen[marketID] = true
		qty, err := finiteDecimal(p.Position)
		if err != nil || qty < 0 {
			return nil, fmt.Errorf("market %d: invalid position", marketID)
		}
		if qty != 0 && p.Sign != -1 && p.Sign != 1 {
			return nil, fmt.Errorf("market %d: invalid position sign", marketID)
		}
		if p.Sign < 0 {
			qty = -qty
		}
		if qty == 0 {
			continue
		}
		avg, err := finiteDecimal(p.AvgEntry)
		if err != nil || avg <= 0 {
			return nil, fmt.Errorf("market %d: invalid entry price", marketID)
		}
		liq, err := finiteDecimal(p.LiquidationPrice)
		if err != nil || liq < 0 {
			return nil, fmt.Errorf("market %d: invalid liquidation price", marketID)
		}
		ps = append(ps, domain.Position{MarketID: marketID, Symbol: p.Symbol, Qty: qty, AvgEntry: avg, LiquidationPrice: liq})
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
		Orders *[]wsOrder `json:"orders"`
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
	if out.Orders == nil {
		return nil, fmt.Errorf("missing account orders")
	}
	return *out.Orders, nil
}

// sendTx posts a signed tx. Remote errors retain an unknown outcome unless a
// caller has an independently verified interpretation of that API error code.
func (r *restClient) sendTx(ctx context.Context, txType uint8, txInfo string) (string, error) {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	if err := w.WriteField("tx_type", strconv.Itoa(int(txType))); err != nil {
		return "", fmt.Errorf("%w: encode tx type: %v", domain.ErrRejected, err)
	}
	if err := w.WriteField("tx_info", txInfo); err != nil {
		return "", fmt.Errorf("%w: encode tx info: %v", domain.ErrRejected, err)
	}
	if err := w.Close(); err != nil {
		return "", fmt.Errorf("%w: encode tx body: %v", domain.ErrRejected, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.host+"/api/v1/sendTx", &buf)
	if err != nil {
		return "", fmt.Errorf("%w: sendTx request: %v", domain.ErrRejected, err)
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	resp, err := r.httpClient().Do(req)
	if err != nil {
		return "", err // unknown outcome
	}
	defer closeBody(resp.Body)
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("sendTx response read: %w", err)
	}
	var out struct {
		resultCode
		TxHash string `json:"tx_hash"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", fmt.Errorf("sendTx: http %d: invalid JSON response: %w", resp.StatusCode, err) // unknown
	}
	if out.Code == nil || *out.Code <= 0 {
		return "", fmt.Errorf("sendTx: missing or invalid response code")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("sendTx: http %d: outcome unknown", resp.StatusCode)
	}
	if *out.Code != 200 {
		return "", &apiError{Code: *out.Code, Msg: out.Message}
	}
	if strings.TrimSpace(out.TxHash) == "" {
		return "", fmt.Errorf("sendTx: success response missing tx hash")
	}
	return out.TxHash, nil
}

func closeBody(body io.Closer) {
	if err := body.Close(); err != nil {
		// A close error cannot undo an already parsed response. Keep it observable.
		slog.Warn("response body close failed", "err", err)
	}
}
