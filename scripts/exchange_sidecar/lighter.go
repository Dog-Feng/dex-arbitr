package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	lighterclient "github.com/elliottech/lighter-go/client"
	lighterhttp "github.com/elliottech/lighter-go/client/http"
	"github.com/elliottech/lighter-go/types"
	"github.com/elliottech/lighter-go/types/txtypes"
	"github.com/shopspring/decimal"
	"gopkg.in/yaml.v3"
)

type lighterVenue struct {
	ID               string `yaml:"id"`
	Rest             string `yaml:"rest"`
	ChainID          uint32 `yaml:"chain_id"`
	AccountIndex     int64  `yaml:"account_index"`
	APIKeyIndex      uint8  `yaml:"api_key_index"`
	APIKeyPrivateKey string `yaml:"api_key_private_key"`
	PrivateKey       string `yaml:"private_key"`
	Quote            string `yaml:"quote"`
}

type lighterSession struct {
	venue      lighterVenue
	baseURL    string
	txClient   *lighterclient.TxClient
	httpClient *http.Client
	nonceMu    sync.Mutex
	nextNonce  *int64
}

func loadLighterVenue(path string) (lighterVenue, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return lighterVenue{}, err
	}
	var v lighterVenue
	if err := yaml.Unmarshal(b, &v); err != nil {
		return lighterVenue{}, err
	}
	pk := strings.TrimSpace(v.APIKeyPrivateKey)
	if pk == "" {
		pk = strings.TrimSpace(v.PrivateKey)
	}
	if pk == "" {
		return lighterVenue{}, fmt.Errorf("lighter api_key_private_key missing in %s", path)
	}
	if v.AccountIndex <= 0 {
		return lighterVenue{}, fmt.Errorf("lighter account_index must be positive in %s", path)
	}
	v.APIKeyPrivateKey = pk
	return v, nil
}

func lighterBaseURL(rest string) string {
	rest = strings.TrimRight(strings.TrimSpace(rest), "/")
	rest = strings.TrimSuffix(rest, "/api/v1")
	return rest
}

func (s *lighterSession) connect(ctx context.Context) error {
	if s.txClient != nil {
		return nil
	}
	httpAPI := lighterhttp.NewClient(s.baseURL)
	txClient, err := lighterclient.NewTxClient(httpAPI, s.venue.APIKeyPrivateKey, s.venue.AccountIndex, s.venue.APIKeyIndex, s.venue.ChainID)
	if err != nil {
		return fmt.Errorf("create lighter signer: %w", err)
	}
	if err := txClient.Check(); err != nil {
		return fmt.Errorf("validate lighter api key: %w", err)
	}
	s.txClient = txClient
	return nil
}

func dispatchLighter(ctx context.Context, req request) (any, error) {
	venue, err := loadLighterVenue(req.VenueYAML)
	if err != nil {
		return nil, err
	}
	s := &lighterSession{
		venue:      venue,
		baseURL:    lighterBaseURL(venue.Rest),
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
	if err := s.connect(ctx); err != nil {
		return nil, err
	}
	params := req.Params
	if params == nil {
		params = map[string]any{}
	}
	switch req.Cmd {
	case "account":
		return s.account(ctx)
	case "place":
		return s.place(ctx, params)
	case "cancel":
		return s.cancel(ctx, params)
	case "order_status":
		return s.orderStatus(ctx, params)
	default:
		return nil, fmt.Errorf("unknown cmd %q", req.Cmd)
	}
}

func (s *lighterSession) account(ctx context.Context) (map[string]any, error) {
	result, err := s.get(ctx, "/api/v1/account", url.Values{
		"by":    {"index"},
		"value": {strconv.FormatInt(s.venue.AccountIndex, 10)},
	}, nil)
	if err != nil {
		return nil, err
	}
	accounts := rawList(result["accounts"])
	if len(accounts) == 0 {
		return nil, errors.New("lighter account snapshot unavailable")
	}
	acc := accounts[0]
	avail := decimalValue(acc["available_balance"])
	collateral := decimalValue(acc["collateral"])
	balances := make([]map[string]string, 0)
	if collateral.GreaterThan(decimal.Zero) {
		asset := "USDC"
		if strings.EqualFold(s.venue.Quote, "USDG") {
			asset = "USDG"
		}
		balances = append(balances, map[string]string{
			"asset":     asset,
			"available": avail.String(),
			"total":     collateral.String(),
		})
	}
	positions := make([]map[string]any, 0)
	for _, raw := range rawList(acc["positions"]) {
		size := decimalValue(firstValue(raw, "position", "size"))
		if size.IsZero() {
			continue
		}
		sign := intOr(raw["sign"], 0)
		if sign < 0 {
			size = size.Neg()
		}
		entry := map[string]any{
			"symbol": stringValue(raw["symbol"]),
			"qty":    size.String(),
		}
		if avg := stringValue(raw["avg_entry_price"]); avg != "" {
			entry["entry_price"] = avg
		}
		positions = append(positions, entry)
	}
	return map[string]any{"balances": balances, "positions": positions}, nil
}

type marketDecimals struct {
	sizeDec  int
	priceDec int
}

func (s *lighterSession) marketDecimals(ctx context.Context, marketIndex int) (marketDecimals, error) {
	result, err := s.get(ctx, "/api/v1/orderBookDetails", url.Values{"filter": {"perp"}}, nil)
	if err != nil {
		return marketDecimals{4, 2}, err
	}
	for _, item := range rawList(result["order_book_details"]) {
		if intOr(firstValue(item, "market_id", "market_index"), 0) == marketIndex {
			return marketDecimals{
				sizeDec:  intOr(firstValue(item, "supported_size_decimals", "size_decimals"), 4),
				priceDec: intOr(firstValue(item, "supported_price_decimals", "price_decimals"), 2),
			}, nil
		}
	}
	return marketDecimals{4, 2}, nil
}

func (s *lighterSession) place(ctx context.Context, params map[string]any) (map[string]string, error) {
	marketIndex, err := paramInt(params, "market_index")
	if err != nil {
		return nil, err
	}
	qty, err := paramDecimal(params, "qty")
	if err != nil {
		return nil, err
	}
	isBuy := paramBool(params, "is_buy")
	reduceOnly := paramBool(params, "reduce_only")
	style := paramString(params, "style", "market")
	limitPriceStr := paramString(params, "limit_price", "")

	dec, err := s.marketDecimals(ctx, marketIndex)
	if err != nil {
		return nil, err
	}
	scale := decimal.New(1, int32(dec.sizeDec))
	baseAmount := qty.Mul(scale).IntPart()
	if baseAmount <= 0 {
		return nil, fmt.Errorf("qty too small after scale: %s", qty)
	}

	coidStr := paramString(params, "client_order_id", fmt.Sprintf("%d", time.Now().UnixMilli()))
	coid, err := strconv.ParseInt(coidStr, 10, 64)
	if err != nil || coid <= 0 {
		coid = time.Now().UnixMilli()
	}

	orderType := uint8(txtypes.LimitOrder)
	tif := uint8(txtypes.PostOnly)
	expiry := time.Now().Add(28 * 24 * time.Hour).UnixMilli()
	priceUnits := int64(0)

	if style == "limit" {
		if limitPriceStr == "" {
			return nil, errors.New("limit_price required for limit order")
		}
		lp, err := decimal.NewFromString(limitPriceStr)
		if err != nil {
			return nil, err
		}
		pScale := decimal.New(1, int32(dec.priceDec))
		priceUnits = lp.Mul(pScale).IntPart()
		if priceUnits <= 0 {
			return nil, errors.New("invalid limit price")
		}
	} else {
		last, err := s.lastTradePrice(ctx, marketIndex)
		if err != nil {
			return nil, err
		}
		ratio := decimal.NewFromFloat(1.05)
		if !isBuy {
			ratio = decimal.NewFromFloat(0.95)
		}
		prot := last.Mul(ratio)
		pScale := decimal.New(1, int32(dec.priceDec))
		priceUnits = prot.Mul(pScale).IntPart()
		if priceUnits <= 0 {
			return nil, errors.New("invalid market protection price")
		}
		orderType = txtypes.MarketOrder
		tif = txtypes.ImmediateOrCancel
		expiry = txtypes.NilOrderExpiry
	}

	nonce, err := s.reserveNonce(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := s.txClient.GetCreateOrderTransaction(&types.CreateOrderTxReq{
		MarketIndex:      int16(marketIndex),
		ClientOrderIndex: coid,
		BaseAmount:       baseAmount,
		Price:            uint32(priceUnits),
		IsAsk:            boolByte(!isBuy),
		Type:             orderType,
		TimeInForce:      tif,
		ReduceOnly:       boolByte(reduceOnly),
		OrderExpiry:      expiry,
	}, s.ops(nonce))
	if err != nil {
		return nil, fmt.Errorf("sign create order: %w", err)
	}
	if err := s.sendTx(ctx, int(tx.GetTxType()), txInfo(tx)); err != nil {
		return nil, err
	}

	coidStr = strconv.FormatInt(coid, 10)
	if style == "limit" {
		return map[string]string{
			"order_id":          coidStr,
			"client_order_id":   coidStr,
			"filled_qty":        "0",
			"status":            "accepted",
			"avg_price":         limitPriceStr,
		}, nil
	}
	return map[string]string{
		"order_id":        coidStr,
		"client_order_id": coidStr,
		"filled_qty":      qty.String(),
		"status":          "filled",
		"avg_price":       limitPriceStr,
	}, nil
}

func (s *lighterSession) lastTradePrice(ctx context.Context, marketIndex int) (decimal.Decimal, error) {
	result, err := s.get(ctx, "/api/v1/orderBookDetails", url.Values{
		"market_id": {strconv.Itoa(marketIndex)},
	}, nil)
	if err != nil {
		return decimal.Zero, err
	}
	list := rawList(result["order_book_details"])
	if len(list) == 0 {
		return decimal.Zero, errors.New("no order book detail")
	}
	last := decimalValue(list[0]["last_trade_price"])
	if !last.GreaterThan(decimal.Zero) {
		return decimal.Zero, errors.New("invalid last trade price")
	}
	return last, nil
}

func (s *lighterSession) cancel(ctx context.Context, params map[string]any) (map[string]string, error) {
	marketIndex, err := paramInt(params, "market_index")
	if err != nil {
		return nil, err
	}
	orderID := paramString(params, "order_id", "")
	orderIndex, err := strconv.ParseInt(orderID, 10, 64)
	if err != nil || orderIndex <= 0 {
		return nil, fmt.Errorf("lighter cancel requires numeric client_order_index, got %q", orderID)
	}
	nonce, err := s.reserveNonce(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := s.txClient.GetCancelOrderTransaction(&types.CancelOrderTxReq{
		MarketIndex: int16(marketIndex),
		Index:       orderIndex,
	}, s.ops(nonce))
	if err != nil {
		return nil, err
	}
	if err := s.sendTx(ctx, int(tx.GetTxType()), txInfo(tx)); err != nil {
		return nil, err
	}
	return map[string]string{"order_id": orderID, "status": "canceled"}, nil
}

func (s *lighterSession) orderStatus(ctx context.Context, params map[string]any) (map[string]string, error) {
	marketIndex, err := paramInt(params, "market_index")
	if err != nil {
		return nil, err
	}
	orderID := paramString(params, "order_id", "")
	qty, _ := paramDecimal(params, "qty")

	token, err := s.txClient.GetAuthToken(time.Now().Add(10 * time.Minute))
	if err != nil {
		return nil, fmt.Errorf("lighter auth token: %w", err)
	}
	result, err := s.get(ctx, "/api/v1/accountActiveOrders", url.Values{
		"account_index": {strconv.FormatInt(s.venue.AccountIndex, 10)},
		"market_id":     {strconv.Itoa(marketIndex)},
	}, http.Header{"authorization": {token}})
	if err != nil {
		return nil, err
	}
	for _, raw := range rawList(result["orders"]) {
		coid := stringValue(firstValue(raw, "client_order_index", "client_order_id"))
		oid := stringValue(firstValue(raw, "order_index", "order_id"))
		if coid != orderID && oid != orderID {
			continue
		}
		filled := decimalValue(firstValue(raw, "filled_base_amount", "filled_amount"))
		status := "accepted"
		if filled.GreaterThan(decimal.Zero) {
			if qty.GreaterThan(decimal.Zero) && filled.GreaterThanOrEqual(qty) {
				status = "filled"
			} else {
				status = "partial"
			}
		}
		price := stringValue(raw["price"])
		return map[string]string{
			"order_id":   orderID,
			"filled_qty": filled.String(),
			"status":     status,
			"avg_price":  price,
		}, nil
	}
	fq := "0"
	status := "filled"
	if qty.GreaterThan(decimal.Zero) {
		fq = qty.String()
	}
	return map[string]string{
		"order_id":   orderID,
		"filled_qty": fq,
		"status":     status,
		"avg_price":  "",
	}, nil
}

func (s *lighterSession) reserveNonce(ctx context.Context) (int64, error) {
	s.nonceMu.Lock()
	defer s.nonceMu.Unlock()
	if s.nextNonce == nil {
		api := lighterhttp.NewClient(s.baseURL)
		nonce, err := api.GetNextNonce(s.venue.AccountIndex, s.venue.APIKeyIndex)
		if err != nil {
			return 0, err
		}
		s.nextNonce = &nonce
	}
	n := *s.nextNonce
	next := n + 1
	s.nextNonce = &next
	return n, nil
}

func (s *lighterSession) ops(nonce int64) *types.TransactOpts {
	account := s.venue.AccountIndex
	apiKey := s.venue.APIKeyIndex
	exp := time.Now().Add(10 * time.Minute).UnixMilli()
	return &types.TransactOpts{FromAccountIndex: &account, ApiKeyIndex: &apiKey, Nonce: &nonce, ExpiredAt: exp}
}

func (s *lighterSession) sendTx(ctx context.Context, txType int, info string) error {
	_, err := s.postForm(ctx, "/api/v1/sendTx", url.Values{
		"tx_type": {strconv.Itoa(txType)},
		"tx_info": {info},
	})
	return err
}

func (s *lighterSession) get(ctx context.Context, path string, query url.Values, headers http.Header) (map[string]any, error) {
	reqURL := s.baseURL + path
	if query != nil {
		reqURL += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	for k, vals := range headers {
		for _, v := range vals {
			req.Header.Add(k, v)
		}
	}
	return s.doJSON(req)
}

func (s *lighterSession) postForm(ctx context.Context, path string, form url.Values) (map[string]any, error) {
	reqURL := s.baseURL + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return s.doJSON(req)
}

func (s *lighterSession) doJSON(req *http.Request) (map[string]any, error) {
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("lighter API invalid json: %s", string(body[:min(len(body), 200)]))
	}
	if code, ok := intValue(result["code"]); ok && code != 200 && code != 0 {
		return nil, fmt.Errorf("lighter API code=%d: %s", code, stringValue(result["message"]))
	}
	return result, nil
}

func txInfo(info interface{ GetTxInfo() (string, error) }) string {
	v, _ := info.GetTxInfo()
	return v
}

func boolByte(v bool) uint8 {
	if v {
		return 1
	}
	return 0
}

func paramInt(params map[string]any, key string) (int, error) {
	n, err := paramUint64(params, key)
	return int(n), err
}

func rawList(value any) []map[string]any {
	if value == nil {
		return nil
	}
	switch v := value.(type) {
	case []any:
		out := make([]map[string]any, 0, len(v))
		for _, item := range v {
			if m, ok := item.(map[string]any); ok {
				out = append(out, m)
			}
		}
		return out
	default:
		return nil
	}
}

func stringValue(v any) string {
	if v == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(v))
}

func firstValue(m map[string]any, keys ...string) any {
	for _, k := range keys {
		if v, ok := m[k]; ok && v != nil {
			return v
		}
	}
	return nil
}

func decimalValue(v any) decimal.Decimal {
	if v == nil {
		return decimal.Zero
	}
	d, err := decimal.NewFromString(stringValue(v))
	if err != nil {
		return decimal.Zero
	}
	return d
}

func intOr(v any, def int) int {
	if n, ok := intValue(v); ok {
		return n
	}
	return def
}

func intValue(v any) (int, bool) {
	switch t := v.(type) {
	case float64:
		return int(t), true
	case int:
		return t, true
	case int64:
		return int(t), true
	case json.Number:
		n, err := t.Int64()
		if err != nil {
			return 0, false
		}
		return int(n), true
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(t))
		if err != nil {
			return 0, false
		}
		return n, true
	default:
		return 0, false
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
