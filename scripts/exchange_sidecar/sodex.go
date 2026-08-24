// SoDEX handlers（对齐 scripts/sodex_bridge + internal/exchange/sodex.go）
package main

import (
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/shopspring/decimal"
	sodexclient "github.com/sodex-tech/sodex-go-sdk-public/client"
	"github.com/sodex-tech/sodex-go-sdk-public/common/enums"
	ptypes "github.com/sodex-tech/sodex-go-sdk-public/perps/types"
	"gopkg.in/yaml.v3"
)

type sodexVenueFile struct {
	ID             string `yaml:"id"`
	Rest           string `yaml:"rest"`
	ChainID        uint64 `yaml:"chain_id"`
	EIP712Chain    uint64 `yaml:"eip712_chain_id"`
	AccountID      int64  `yaml:"account_id"`
	AccountIndex   int64  `yaml:"account_index"`
	AccountAddress string `yaml:"account_address"`
	APIKeyName     string `yaml:"api_key_name"`
	PrivateKey     string `yaml:"private_key"`
}

func dispatchSodex(ctx context.Context, req request) (any, error) {
	venue, err := loadSodexVenue(req.VenueYAML)
	if err != nil {
		return nil, err
	}
	client, accountID, addr, err := connect(ctx, venue)
	if err != nil {
		return nil, err
	}
	params := req.Params
	if params == nil {
		params = map[string]any{}
	}
	switch req.Cmd {
	case "account":
		return accountSnapshot(ctx, gatewayBase(venue.Rest), addr, accountID)
	case "place":
		return placeOrder(ctx, client, accountID, venue, params)
	case "cancel":
		return cancelOrder(ctx, client, accountID, params)
	case "order_status":
		return orderStatus(ctx, client, accountID, params)
	default:
		return nil, fmt.Errorf("unknown cmd %q", req.Cmd)
	}
}

func loadSodexVenue(path string) (sodexVenueFile, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return sodexVenueFile{}, err
	}
	var v sodexVenueFile
	if err := yaml.Unmarshal(b, &v); err != nil {
		return sodexVenueFile{}, err
	}
	if v.PrivateKey == "" {
		return sodexVenueFile{}, fmt.Errorf("sodex private_key missing in %s", path)
	}
	if v.APIKeyName == "" {
		return sodexVenueFile{}, fmt.Errorf("sodex api_key_name missing in %s", path)
	}
	if strings.TrimSpace(v.AccountAddress) == "" {
		if env := strings.TrimSpace(os.Getenv("SODEX_ACCOUNT_ADDRESS")); env != "" {
			v.AccountAddress = env
		}
	}
	return v, nil
}

func readAccountAddress(v sodexVenueFile, c *sodexclient.Client) string {
	if addr := strings.TrimSpace(v.AccountAddress); addr != "" {
		return addr
	}
	return c.Address()
}

func accountQuery(accountID uint64) string {
	if accountID == 0 {
		return ""
	}
	return "?accountID=" + strconv.FormatUint(accountID, 10)
}

func gatewayBase(rest string) string {
	rest = strings.TrimRight(strings.TrimSpace(rest), "/")
	if strings.HasSuffix(rest, "/api/v1/perps") {
		return strings.TrimSuffix(rest, "/api/v1/perps")
	}
	return rest
}

func eip712Chain(v sodexVenueFile) uint64 {
	if v.EIP712Chain > 0 {
		return v.EIP712Chain
	}
	if v.ChainID >= 100000 {
		return v.ChainID
	}
	return sodexclient.DefaultChainID
}

func configuredAccountID(v sodexVenueFile) uint64 {
	if v.AccountID > 0 {
		return uint64(v.AccountID)
	}
	if v.AccountIndex > 0 {
		return uint64(v.AccountIndex)
	}
	return 0
}

func connect(ctx context.Context, v sodexVenueFile) (*sodexclient.Client, uint64, string, error) {
	pk, err := parsePrivateKey(v.PrivateKey)
	if err != nil {
		return nil, 0, "", err
	}
	client := sodexclient.New(sodexclient.Config{
		BaseURL:    gatewayBase(v.Rest),
		ChainID:    eip712Chain(v),
		PrivateKey: pk,
		APIKeyName: strings.TrimSpace(v.APIKeyName),
		HTTPClient: &http.Client{Timeout: 30 * time.Second},
	})
	addr := readAccountAddress(v, client)
	if addr == "" {
		return nil, 0, "", fmt.Errorf("empty sodex account address; set account_address or SODEX_ACCOUNT_ADDRESS")
	}
	accountID := configuredAccountID(v)
	if accountID == 0 {
		accountID, err = discoverAccountID(ctx, gatewayBase(v.Rest), addr)
		if err != nil {
			return nil, 0, "", err
		}
	}
	return client, accountID, addr, nil
}

func discoverAccountID(ctx context.Context, base, addr string) (uint64, error) {
	u := strings.TrimRight(base, "/") + "/api/v1/perps/accounts/" + url.PathEscape(addr) + "/state"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("discover account id: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var envelope struct {
		Code int `json:"code"`
		Data struct {
			AID        uint64 `json:"aid"`
			AccountID  uint64 `json:"accountID"`
		} `json:"data"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return 0, fmt.Errorf("decode account state: %w", err)
	}
	if envelope.Code != 0 {
		return 0, fmt.Errorf("account state code=%d msg=%s", envelope.Code, envelope.Message)
	}
	if envelope.Data.AID > 0 {
		return envelope.Data.AID, nil
	}
	if envelope.Data.AccountID > 0 {
		return envelope.Data.AccountID, nil
	}
	return 0, fmt.Errorf("account state missing aid; set account_id in venue yaml")
}

func accountSnapshot(ctx context.Context, baseURL, addr string, accountID uint64) (map[string]any, error) {
	query := accountQuery(accountID)
	balances, err := fetchPerpsBalances(ctx, baseURL, addr, query)
	if err != nil {
		return nil, err
	}
	positions, err := fetchPerpsPositions(ctx, baseURL, addr, query)
	if err != nil {
		return nil, err
	}
	if len(balances) == 0 {
		if stateBal, statePos, err := fetchPerpsState(ctx, baseURL, addr, query); err == nil {
			if len(stateBal) > 0 {
				balances = stateBal
			}
			if len(positions) == 0 && len(statePos) > 0 {
				positions = statePos
			}
		}
	}
	outBal := make([]map[string]string, 0, len(balances))
	for _, b := range balances {
		total := decimalFromString(b.Total)
		locked := decimalFromString(b.Locked)
		free := total.Sub(locked)
		if free.IsNegative() {
			free = decimal.Zero
		}
		outBal = append(outBal, map[string]string{
			"asset":     b.Coin,
			"available": free.String(),
			"total":     total.String(),
		})
	}
	outPos := make([]map[string]any, 0, len(positions))
	for _, p := range positions {
		size := decimalFromString(p.Size)
		if size.IsZero() {
			continue
		}
		entry := map[string]any{
			"symbol": p.Symbol,
			"qty":    size.String(),
		}
		if p.AvgEntryPrice != "" {
			entry["entry_price"] = p.AvgEntryPrice
		}
		outPos = append(outPos, entry)
	}
	return map[string]any{"balances": outBal, "positions": outPos}, nil
}

func fetchPerpsBalances(ctx context.Context, baseURL, addr, query string) ([]sodexclient.Balance, error) {
	var wrapper struct {
		Code int `json:"code"`
		Data struct {
			Balances []sodexclient.Balance `json:"balances"`
		} `json:"data"`
		Message string `json:"message"`
	}
	path := strings.TrimRight(baseURL, "/") + "/api/v1/perps/accounts/" + url.PathEscape(addr) + "/balances" + query
	if err := httpGetJSON(ctx, path, &wrapper); err != nil {
		return nil, err
	}
	if wrapper.Code != 0 {
		return nil, fmt.Errorf("sodex balances code=%d msg=%s", wrapper.Code, wrapper.Message)
	}
	return wrapper.Data.Balances, nil
}

func fetchPerpsPositions(ctx context.Context, baseURL, addr, query string) ([]sodexclient.Position, error) {
	var wrapper struct {
		Code int `json:"code"`
		Data struct {
			Positions []sodexclient.Position `json:"positions"`
		} `json:"data"`
		Message string `json:"message"`
	}
	path := strings.TrimRight(baseURL, "/") + "/api/v1/perps/accounts/" + url.PathEscape(addr) + "/positions" + query
	if err := httpGetJSON(ctx, path, &wrapper); err != nil {
		return nil, err
	}
	if wrapper.Code != 0 {
		return nil, fmt.Errorf("sodex positions code=%d msg=%s", wrapper.Code, wrapper.Message)
	}
	return wrapper.Data.Positions, nil
}

func fetchPerpsState(ctx context.Context, baseURL, addr, query string) ([]sodexclient.Balance, []sodexclient.Position, error) {
	var wrapper struct {
		Code int `json:"code"`
		Data struct {
			Av        string          `json:"av"`
			Am        string          `json:"am"`
			Balances  json.RawMessage `json:"B"`
			Positions json.RawMessage `json:"P"`
		} `json:"data"`
		Message string `json:"message"`
	}
	path := strings.TrimRight(baseURL, "/") + "/api/v1/perps/accounts/" + url.PathEscape(addr) + "/state" + query
	if err := httpGetJSON(ctx, path, &wrapper); err != nil {
		return nil, nil, err
	}
	if wrapper.Code != 0 {
		return nil, nil, fmt.Errorf("sodex state code=%d msg=%s", wrapper.Code, wrapper.Message)
	}
	var balances []sodexclient.Balance
	if len(wrapper.Data.Balances) > 0 {
		_ = json.Unmarshal(wrapper.Data.Balances, &balances)
	}
	if len(balances) == 0 {
		total := decimalFromString(firstNonEmpty(wrapper.Data.Am, wrapper.Data.Av))
		if total.GreaterThan(decimal.Zero) {
			balances = []sodexclient.Balance{{
				Coin:  "vUSDC",
				Total: total.String(),
			}}
		}
	}
	var positions []sodexclient.Position
	if len(wrapper.Data.Positions) > 0 {
		_ = json.Unmarshal(wrapper.Data.Positions, &positions)
	}
	return balances, positions, nil
}

func httpGetJSON(ctx context.Context, rawURL string, dest any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("GET %s HTTP %s: %s", rawURL, resp.Status, strings.TrimSpace(string(body)))
	}
	return json.Unmarshal(body, dest)
}

type symbolSpec struct {
	SymbolID uint64
	Symbol   string
	TickSize decimal.Decimal
	StepSize decimal.Decimal
}

func lookupSymbol(ctx context.Context, c *sodexclient.Client, symbolID uint64, symbolName string) (symbolSpec, error) {
	symbols, err := c.PerpsSymbols(ctx)
	if err != nil {
		return symbolSpec{}, err
	}
	for _, s := range symbols {
		if s.SymbolID == symbolID || (symbolName != "" && strings.EqualFold(s.Symbol, symbolName)) {
			tick := decimalFromString(s.TickSize)
			step := decimalFromString(s.StepSize)
			if !tick.GreaterThan(decimal.Zero) {
				tick = decimal.NewFromInt(1)
			}
			if !step.GreaterThan(decimal.Zero) {
				step = decimal.NewFromFloat(0.00001)
			}
			return symbolSpec{
				SymbolID: s.SymbolID,
				Symbol:   s.Symbol,
				TickSize: tick,
				StepSize: step,
			}, nil
		}
	}
	return symbolSpec{}, fmt.Errorf("symbol not found id=%d name=%q", symbolID, symbolName)
}

func placeOrder(ctx context.Context, c *sodexclient.Client, accountID uint64, v sodexVenueFile, params map[string]any) (map[string]string, error) {
	symbolID, err := paramUint64(params, "market_index")
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
	clOrdID := paramString(params, "client_order_id", fmt.Sprintf("arb-%d", time.Now().UnixMilli()))
	spec, err := lookupSymbol(ctx, c, symbolID, paramString(params, "symbol", ""))
	if err != nil {
		return nil, err
	}
	qty = roundToStep(qty, spec.StepSize, false)
	if !qty.GreaterThan(decimal.Zero) {
		return nil, fmt.Errorf("quantity below step size")
	}

	side := enums.OrderSideBuy
	if !isBuy {
		side = enums.OrderSideSell
	}
	raw := &ptypes.RawOrder{
		ClOrdID:      clOrdID,
		Modifier:     enums.OrderModifierNormal,
		Side:         side,
		PositionSide: enums.PositionSideBoth,
		Quantity:     decimalPtr(qty),
		ReduceOnly:   reduceOnly,
	}
	if style == "limit" {
		price, err := paramDecimal(params, "limit_price")
		if err != nil {
			return nil, err
		}
		price = roundToStep(price, spec.TickSize, isBuy)
		raw.Type = enums.OrderTypeLimit
		raw.TimeInForce = enums.TimeInForceGTX
		raw.Price = decimalPtr(price)
	} else {
		protected, err := marketProtectPrice(ctx, c, spec.Symbol, side, spec.TickSize)
		if err != nil {
			return nil, err
		}
		raw.Type = enums.OrderTypeMarket
		raw.TimeInForce = enums.TimeInForceIOC
		if protected.GreaterThan(decimal.Zero) {
			raw.Price = decimalPtr(protected)
		}
	}
	results, err := c.PlacePerpsOrder(ctx, &ptypes.NewOrderRequest{
		AccountID: accountID,
		SymbolID:  spec.SymbolID,
		Orders:    []*ptypes.RawOrder{raw},
	})
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("sodex place returned no results")
	}
	r := results[0]
	status := strings.ToLower(strings.TrimSpace(r.Status))
	filled := "0"
	avgPrice := paramString(params, "limit_price", "")
	if r.OrderID > 0 || r.ClOrdID != "" {
		if o, ok := findOrder(ctx, c, c.Address(), r.OrderID, firstNonEmpty(r.ClOrdID, clOrdID)); ok {
			filled = o.ExecutedQty
			if ap := avgPriceFromOrder(o); ap != "" {
				avgPrice = ap
			}
		}
	}
	outStatus := "accepted"
	if strings.Contains(status, "fill") {
		outStatus = "filled"
	} else if style == "market" {
		outStatus = "filled"
		if filled == "" || filled == "0" {
			filled = qty.String()
		}
	}
	return map[string]string{
		"order_id":        strconv.FormatUint(r.OrderID, 10),
		"client_order_id": firstNonEmpty(r.ClOrdID, clOrdID),
		"filled_qty":      filled,
		"status":          outStatus,
		"avg_price":       avgPrice,
	}, nil
}

func marketProtectPrice(ctx context.Context, c *sodexclient.Client, symbol string, side enums.OrderSide, tickSize decimal.Decimal) (decimal.Decimal, error) {
	if symbol == "" {
		return decimal.Zero, fmt.Errorf("symbol required for market order")
	}
	book, err := c.PerpsOrderBook(ctx, symbol, 5)
	if err != nil {
		return decimal.Zero, err
	}
	ratio := decimal.RequireFromString("0.005")
	var best decimal.Decimal
	if side == enums.OrderSideBuy {
		if len(book.Asks) == 0 {
			return decimal.Zero, fmt.Errorf("no ask liquidity for %s", symbol)
		}
		best = decimalFromString(book.Asks[0].Price)
		return roundToStep(best.Mul(decimal.NewFromInt(1).Add(ratio)), tickSize, true), nil
	}
	if len(book.Bids) == 0 {
		return decimal.Zero, fmt.Errorf("no bid liquidity for %s", symbol)
	}
	best = decimalFromString(book.Bids[0].Price)
	return roundToStep(best.Mul(decimal.NewFromInt(1).Sub(ratio)), tickSize, false), nil
}

func roundToStep(value, step decimal.Decimal, ceil bool) decimal.Decimal {
	if !step.GreaterThan(decimal.Zero) {
		return value
	}
	units := value.Div(step)
	if ceil {
		return units.Ceil().Mul(step)
	}
	return units.Truncate(0).Mul(step)
}

func cancelOrder(ctx context.Context, c *sodexclient.Client, accountID uint64, params map[string]any) (map[string]string, error) {
	symbolID, err := paramUint64(params, "market_index")
	if err != nil {
		return nil, err
	}
	orderID := paramString(params, "order_id", "")
	if orderID == "" {
		return nil, fmt.Errorf("order_id required")
	}
	cancel := &ptypes.CancelOrder{SymbolID: symbolID}
	if parsed, err := strconv.ParseUint(orderID, 10, 64); err == nil && parsed > 0 {
		cancel.OrderID = &parsed
	} else {
		cl := orderID
		cancel.ClOrdID = &cl
	}
	_, err = c.CancelPerpsOrders(ctx, &ptypes.CancelOrderRequest{
		AccountID: accountID,
		Cancels:   []*ptypes.CancelOrder{cancel},
	})
	if err != nil {
		return nil, err
	}
	return map[string]string{"order_id": orderID, "status": "canceled"}, nil
}

func orderStatus(ctx context.Context, c *sodexclient.Client, accountID uint64, params map[string]any) (map[string]string, error) {
	_ = accountID
	addr := c.Address()
	orderID := paramString(params, "order_id", "")
	qty, _ := paramDecimal(params, "qty")
	orders, err := c.PerpsOrders(ctx, addr)
	if err != nil {
		return nil, err
	}
	for _, o := range orders {
		oid := strconv.FormatUint(o.OrderID, 10)
		if oid != orderID && o.ClOrdID != orderID {
			continue
		}
		filled := decimalFromString(o.ExecutedQty)
		st := "accepted"
		if filled.GreaterThan(decimal.Zero) {
			if qty.GreaterThan(decimal.Zero) && filled.GreaterThanOrEqual(qty) {
				st = "filled"
			} else {
				st = "partial"
			}
		}
		return map[string]string{
			"order_id":   orderID,
			"filled_qty": filled.String(),
			"status":     st,
			"avg_price":  avgPriceFromOrder(o),
		}, nil
	}
	fq := "0"
	st := "filled"
	if qty.GreaterThan(decimal.Zero) {
		fq = qty.String()
	}
	return map[string]string{
		"order_id":   orderID,
		"filled_qty": fq,
		"status":     st,
		"avg_price":  "",
	}, nil
}

func parsePrivateKey(raw string) (*ecdsa.PrivateKey, error) {
	keyText := strings.TrimPrefix(strings.TrimSpace(raw), "0x")
	pk, err := crypto.HexToECDSA(keyText)
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}
	return pk, nil
}

func findOrder(ctx context.Context, c *sodexclient.Client, addr string, orderID uint64, clOrdID string) (sodexclient.Order, bool) {
	orders, err := c.PerpsOrders(ctx, addr)
	if err != nil {
		return sodexclient.Order{}, false
	}
	for _, o := range orders {
		if orderID > 0 && o.OrderID == orderID {
			return o, true
		}
		if clOrdID != "" && o.ClOrdID == clOrdID {
			return o, true
		}
	}
	return sodexclient.Order{}, false
}

func avgPriceFromOrder(o sodexclient.Order) string {
	executed := decimalFromString(o.ExecutedQty)
	value := decimalFromString(o.ExecutedValue)
	if executed.GreaterThan(decimal.Zero) && value.GreaterThan(decimal.Zero) {
		return value.Div(executed).String()
	}
	if o.Price != "" {
		return o.Price
	}
	return ""
}

func decimalPtr(v decimal.Decimal) *decimal.Decimal { return &v }

func decimalFromString(s string) decimal.Decimal {
	v, err := decimal.NewFromString(strings.TrimSpace(s))
	if err != nil {
		return decimal.Zero
	}
	return v
}

func paramString(params map[string]any, key, def string) string {
	v, ok := params[key]
	if !ok || v == nil {
		return def
	}
	return strings.TrimSpace(fmt.Sprint(v))
}

func paramStringMap(params map[string]any, key string) string {
	return paramString(params, key, "0")
}

func paramBool(params map[string]any, key string) bool {
	v, ok := params[key]
	if !ok {
		return false
	}
	switch t := v.(type) {
	case bool:
		return t
	case string:
		return t == "true" || t == "1"
	default:
		return false
	}
}

func paramUint64(params map[string]any, key string) (uint64, error) {
	s := paramString(params, key, "")
	if s == "" {
		return 0, fmt.Errorf("%s required", key)
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	return n, nil
}

func paramDecimal(params map[string]any, key string) (decimal.Decimal, error) {
	s := paramString(params, key, "")
	if s == "" {
		return decimal.Zero, fmt.Errorf("%s required", key)
	}
	return decimal.NewFromString(s)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
