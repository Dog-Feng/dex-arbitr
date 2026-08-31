// EntropyIO（Hyperliquid HIP-3，dex=io）handlers。
//
// 公开行情在 Rust 侧直接打 HL REST/WS；这里只做签名交易六条 cmd。
// 第一期只暴露 io 命名空间，不是通用 Hyperliquid 适配器。
package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
	"gopkg.in/yaml.v3"
)

const entropyHip3Dex = "io"

type entropyVenueFile struct {
	ID             string `yaml:"id"`
	Rest           string `yaml:"rest"`
	WS             string `yaml:"ws"`
	AccountAddress string `yaml:"account_address"`
	PrivateKey     string `yaml:"private_key"`
	Hip3Dex        string `yaml:"hip3_dex"`
}

type entropyAsset struct {
	Coin       string
	AssetID    int
	SzDecimals int
}

type entropySession struct {
	venue entropyVenueFile
	pk    *ecdsa.PrivateKey
	addr  string
	info  string
	exch  string
	wsURL string
	http  *http.Client

	mu        sync.Mutex
	lastNonce int64
	assets    map[string]entropyAsset
	dexIndex  int

	wsOnce sync.Once
	wsStop context.CancelFunc

	fillMu sync.Mutex
	fills  map[string]entropyFillNote
}

type entropyFillNote struct {
	// orderUpdates 是累计成交；userFills 是增量。两边都会推同一笔，
	// 不能加在一起，否则市价腿确认会变成 2 倍。认两者的较大值。
	fromOrder decimal.Decimal
	fromFills decimal.Decimal
	px        string
	closedPnl decimal.Decimal
	// userFills 见过这条单。closedPnl 可以是 0（打平），不能用「非零」当有没有。
	sawFill bool
	at      time.Time
}

func (n entropyFillNote) qty() decimal.Decimal {
	if n.fromOrder.GreaterThan(n.fromFills) {
		return n.fromOrder
	}
	return n.fromFills
}

func (s *entropySession) close() {
	if s.wsStop != nil {
		s.wsStop()
	}
}

func (s *entropySession) dex() string {
	if d := strings.TrimSpace(s.venue.Hip3Dex); d != "" {
		return d
	}
	return entropyHip3Dex
}

func (r *registry) entropySession(ctx context.Context, path string) (*entropySession, error) {
	r.mu.Lock()
	if r.entropy != nil {
		s := r.entropy
		r.mu.Unlock()
		return s, nil
	}
	r.mu.Unlock()

	s, err := newEntropySession(ctx, path)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.entropy != nil {
		s.close()
		return r.entropy, nil
	}
	r.entropy = s
	return s, nil
}

func dispatchEntropy(ctx context.Context, reg *registry, req request) (any, error) {
	s, err := reg.entropySession(ctx, req.VenueYAML)
	if err != nil {
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
	case "funding":
		return s.funding(ctx)
	case "watch":
		s.startOrderStream()
		return map[string]string{"status": "watching"}, nil
	case "fill_pnl":
		return s.fillPnl(ctx, params)
	default:
		return nil, fmt.Errorf("unknown cmd %q", req.Cmd)
	}
}

func loadEntropyVenue(path string) (entropyVenueFile, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return entropyVenueFile{}, err
	}
	var v entropyVenueFile
	if err := yaml.Unmarshal(b, &v); err != nil {
		return entropyVenueFile{}, err
	}
	if strings.TrimSpace(v.PrivateKey) == "" {
		if env := strings.TrimSpace(os.Getenv("ENTROPY_PRIVATE_KEY")); env != "" {
			v.PrivateKey = env
		}
	}
	if strings.TrimSpace(v.AccountAddress) == "" {
		if env := strings.TrimSpace(os.Getenv("ENTROPY_ACCOUNT_ADDRESS")); env != "" {
			v.AccountAddress = env
		}
	}
	if strings.TrimSpace(v.PrivateKey) == "" {
		return entropyVenueFile{}, fmt.Errorf("entropy private_key missing in %s", path)
	}
	return v, nil
}

func newEntropySession(ctx context.Context, path string) (*entropySession, error) {
	venue, err := loadEntropyVenue(path)
	if err != nil {
		return nil, err
	}
	pk, err := parsePrivateKey(venue.PrivateKey)
	if err != nil {
		return nil, err
	}
	addr := strings.TrimSpace(venue.AccountAddress)
	if addr == "" {
		addr = crypto.PubkeyToAddress(pk.PublicKey).Hex()
	}
	rest := strings.TrimRight(strings.TrimSpace(venue.Rest), "/")
	info := rest
	if !strings.HasSuffix(info, "/info") {
		info += "/info"
	}
	exch := strings.TrimSuffix(info, "/info") + "/exchange"
	wsURL := strings.TrimSpace(venue.WS)
	if wsURL == "" {
		wsURL = "wss://api.hyperliquid.xyz/ws"
	}
	s := &entropySession{
		venue:  venue,
		pk:     pk,
		addr:   addr,
		info:   info,
		exch:   exch,
		wsURL:  wsURL,
		http:   &http.Client{Timeout: 20 * time.Second},
		assets: map[string]entropyAsset{},
	}
	if err := s.refreshMeta(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *entropySession) refreshMeta(ctx context.Context) error {
	var dexes []json.RawMessage
	if err := s.infoJSON(ctx, map[string]any{"type": "perpDexs"}, &dexes); err != nil {
		return err
	}
	dexName := s.dex()
	idx, err := perpDexIndex(dexes, dexName)
	if err != nil {
		return err
	}
	var body []json.RawMessage
	if err := s.infoJSON(ctx, map[string]any{"type": "metaAndAssetCtxs", "dex": dexName}, &body); err != nil {
		return err
	}
	if len(body) < 1 {
		return fmt.Errorf("entropy metaAndAssetCtxs empty")
	}
	assets, err := parseIoUniverse(body[0], idx)
	if err != nil {
		return err
	}
	s.dexIndex = idx
	s.assets = assets
	return nil
}

func perpDexIndex(dexes []json.RawMessage, name string) (int, error) {
	for i, raw := range dexes {
		if len(raw) == 0 || string(raw) == "null" {
			continue
		}
		var obj struct {
			Name string `json:"name"`
		}
		if json.Unmarshal(raw, &obj) != nil {
			continue
		}
		if strings.EqualFold(obj.Name, name) {
			return i, nil
		}
	}
	return 0, fmt.Errorf("HIP-3 dex %q not in perpDexs", name)
}

func hip3AssetID(dexIndex, indexInMeta int) int {
	return 100000 + dexIndex*10000 + indexInMeta
}

func parseIoUniverse(metaRaw json.RawMessage, dexIndex int) (map[string]entropyAsset, error) {
	var meta struct {
		Universe []struct {
			Name       string `json:"name"`
			SzDecimals int    `json:"szDecimals"`
			IsDelisted bool   `json:"isDelisted"`
		} `json:"universe"`
	}
	if err := json.Unmarshal(metaRaw, &meta); err != nil {
		return nil, err
	}
	out := make(map[string]entropyAsset, len(meta.Universe)*3)
	for i, u := range meta.Universe {
		if u.IsDelisted {
			continue
		}
		coin := strings.TrimSpace(u.Name)
		if coin == "" {
			continue
		}
		if !strings.Contains(coin, ":") {
			coin = entropyHip3Dex + ":" + coin
		}
		a := entropyAsset{Coin: coin, AssetID: hip3AssetID(dexIndex, i), SzDecimals: u.SzDecimals}
		out[strings.ToUpper(coin)] = a
		if _, rest, ok := strings.Cut(coin, ":"); ok {
			out[strings.ToUpper(rest)] = a
		}
		out[strconv.Itoa(a.AssetID)] = a
	}
	return out, nil
}

func (s *entropySession) lookupAsset(symbol string, marketIndex int) (entropyAsset, error) {
	if marketIndex > 0 {
		if a, ok := s.assets[strconv.Itoa(marketIndex)]; ok {
			return a, nil
		}
	}
	key := strings.ToUpper(strings.TrimSpace(symbol))
	if a, ok := s.assets[key]; ok {
		return a, nil
	}
	if _, rest, ok := strings.Cut(key, ":"); ok {
		if a, ok := s.assets[rest]; ok {
			return a, nil
		}
	}
	return entropyAsset{}, fmt.Errorf("entropy asset not found symbol=%q market_index=%d", symbol, marketIndex)
}

func (s *entropySession) account(ctx context.Context) (map[string]any, error) {
	// 持仓仍按 dex=io 过滤，避免把 HL 主所 BTC 等混进 Entropy。
	var ioState hlClearinghouse
	if err := s.infoJSON(ctx, map[string]any{
		"type": "clearinghouseState",
		"user": s.addr,
		"dex":  s.dex(),
	}, &ioState); err != nil {
		return nil, err
	}
	var mainState hlClearinghouse
	_ = s.infoJSON(ctx, map[string]any{
		"type": "clearinghouseState",
		"user": s.addr,
	}, &mainState)
	var spot struct {
		Balances []struct {
			Coin  string `json:"coin"`
			Total string `json:"total"`
			Hold  string `json:"hold"`
		} `json:"balances"`
	}
	_ = s.infoJSON(ctx, map[string]any{
		"type": "spotClearinghouseState",
		"user": s.addr,
	}, &spot)

	spotTotal, spotHold := decimal.Zero, decimal.Zero
	for _, b := range spot.Balances {
		if strings.EqualFold(b.Coin, "USDC") {
			spotTotal = decimalFromString(b.Total)
			spotHold = decimalFromString(b.Hold)
			break
		}
	}
	avail := entropyAvailableUSDC(
		decimalFromString(ioState.Withdrawable),
		decimalFromString(mainState.Withdrawable),
		spotTotal,
		spotHold,
	)
	total := spotTotal
	if ioVal := decimalFromString(ioState.MarginSummary.AccountValue); ioVal.GreaterThan(total) {
		total = ioVal
	}
	if mainVal := decimalFromString(mainState.MarginSummary.AccountValue); mainVal.GreaterThan(total) {
		total = mainVal
	}
	if avail.GreaterThan(total) {
		total = avail
	}

	balances := []map[string]string{{
		"asset":     "USDC",
		"available": avail.String(),
		"total":     total.String(),
	}}
	prefix := strings.ToLower(s.dex()) + ":"
	positions := make([]map[string]string, 0)
	for _, row := range ioState.AssetPositions {
		coin := strings.TrimSpace(row.Position.Coin)
		if !strings.HasPrefix(strings.ToLower(coin), prefix) {
			continue
		}
		qty := decimalFromString(row.Position.Szi)
		if qty.IsZero() {
			continue
		}
		positions = append(positions, map[string]string{
			"symbol":      coin,
			"qty":         qty.String(),
			"entry_price": row.Position.EntryPx,
		})
	}
	return map[string]any{"balances": balances, "positions": positions}, nil
}

type hlClearinghouse struct {
	Withdrawable  string `json:"withdrawable"`
	MarginSummary struct {
		AccountValue string `json:"accountValue"`
	} `json:"marginSummary"`
	AssetPositions []struct {
		Position struct {
			Coin    string `json:"coin"`
			Szi     string `json:"szi"`
			EntryPx string `json:"entryPx"`
		} `json:"position"`
	} `json:"assetPositions"`
}

// 统一账户 / 组合保证金：可用资金只出现在 spotClearinghouseState，
// 各 perp dex 的 withdrawable 没有意义。手动分账模式才读 dex 自己的保证金。
func entropyAvailableUSDC(ioWithdrawable, mainWithdrawable, spotTotal, spotHold decimal.Decimal) decimal.Decimal {
	spotAvail := spotTotal.Sub(spotHold)
	if spotAvail.GreaterThan(decimal.Zero) {
		return spotAvail
	}
	if ioWithdrawable.GreaterThan(decimal.Zero) {
		return ioWithdrawable
	}
	if mainWithdrawable.GreaterThan(decimal.Zero) {
		return mainWithdrawable
	}
	return decimal.Zero
}

func (s *entropySession) funding(ctx context.Context) (map[string]any, error) {
	var body []json.RawMessage
	if err := s.infoJSON(ctx, map[string]any{"type": "metaAndAssetCtxs", "dex": s.dex()}, &body); err != nil {
		return nil, err
	}
	if len(body) < 2 {
		return map[string]any{"rates": []any{}}, nil
	}
	var meta struct {
		Universe []struct {
			Name       string `json:"name"`
			IsDelisted bool   `json:"isDelisted"`
		} `json:"universe"`
	}
	if err := json.Unmarshal(body[0], &meta); err != nil {
		return nil, err
	}
	var ctxs []struct {
		Funding string `json:"funding"`
	}
	if err := json.Unmarshal(body[1], &ctxs); err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(meta.Universe))
	for i, u := range meta.Universe {
		if u.IsDelisted || i >= len(ctxs) {
			continue
		}
		rate := strings.TrimSpace(ctxs[i].Funding)
		if rate == "" {
			continue
		}
		coin := u.Name
		if !strings.Contains(coin, ":") {
			coin = s.dex() + ":" + coin
		}
		out = append(out, map[string]any{
			"symbol":        coin,
			"rate":          rate,
			"interval_secs": entropyFundingInterval,
		})
	}
	return map[string]any{"rates": out}, nil
}

func (s *entropySession) place(ctx context.Context, params map[string]any) (map[string]string, error) {
	marketIndex := 0
	if n, err := paramUint64(params, "market_index"); err == nil {
		marketIndex = int(n)
	}
	asset, err := s.lookupAsset(paramString(params, "symbol", ""), marketIndex)
	if err != nil {
		return nil, err
	}
	qty, err := paramDecimal(params, "qty")
	if err != nil {
		return nil, err
	}
	qty = roundHlSz(qty, asset.SzDecimals)
	if !qty.GreaterThan(decimal.Zero) {
		return nil, fmt.Errorf("quantity below lot size")
	}
	isBuy := paramBool(params, "is_buy")
	reduceOnly := paramBool(params, "reduce_only")
	style := paramString(params, "style", "market")
	ioc := style != "limit"
	tif := "Gtc"
	if ioc {
		tif = "Ioc"
	} else {
		tif = "Alo"
	}
	var px decimal.Decimal
	if style == "limit" {
		px, err = paramDecimal(params, "limit_price")
		if err != nil {
			return nil, err
		}
		// post-only：取整远离对手价（买向下、卖向上）
		px = roundHlPx(px, asset.SzDecimals, !isBuy)
	} else if style == "aggressive_limit" {
		px, err = paramDecimal(params, "limit_price")
		if err != nil {
			return nil, err
		}
		// IOC：取整朝对手价靠（买向上、卖向下）
		px = roundHlPx(px, asset.SzDecimals, isBuy)
	} else {
		targetPx, _ := paramDecimal(params, "target_price")
		slipPct, _ := paramDecimal(params, "slippage_pct")
		px, err = s.marketProtectPx(ctx, asset.Coin, isBuy, targetPx, slipPct, asset.SzDecimals)
		if err != nil {
			return nil, err
		}
	}
	if !px.GreaterThan(decimal.Zero) {
		return nil, fmt.Errorf("invalid limit price")
	}
	rawCloid := paramString(params, "client_order_id", nextArbClientOrderID())
	cloid := rawCloid
	if !isHLCloid(cloid) {
		cloid = hlCloid(rawCloid)
	}
	action := hlOrderAction{
		Type:     "order",
		Grouping: "na",
		Orders: []hlOrderWire{{
			A: asset.AssetID,
			B: isBuy,
			P: hlPxWire(px),
			S: hlPxWire(qty),
			R: reduceOnly,
			T: hlOrderTypeWire{Limit: hlLimitTif{Tif: tif}},
			C: cloid,
		}},
	}
	resp, rtt, postErr := s.postAction(ctx, action)
	rtt.log(venueIDOr(s.venue.ID, "entropy"), rawCloid, postErr == nil)
	if postErr != nil {
		if view, ok := s.lookupByCloid(ctx, cloid); ok {
			filled, avg, st := view.filledAndStatus(qty, ioc)
			fmt.Fprintf(os.Stderr, "entropy place: request failed (%v) but cloid %s is live; reporting %s\n", postErr, cloid, st)
			return rtt.merge(map[string]string{
				"order_id":        view.oidString(),
				"client_order_id": rawCloid,
				"filled_qty":      filled.String(),
				"status":          st,
				"avg_price":       avg,
			}), nil
		}
		return nil, postErr
	}
	st, err := parseHlPlaceStatus(resp)
	if err != nil {
		return nil, err
	}
	if st.errMsg != "" {
		return nil, fmt.Errorf("entropy rejected order: %s", st.errMsg)
	}
	oid := st.oid
	filled := st.filled
	avg := st.avgPx
	if ioc {
		f, ap, _ := s.waitOrderFill(ctx, oid, cloid, qty, fillWaitOf(params))
		if f.GreaterThan(filled) {
			filled = f
		}
		// waitOrderFill 从订单视图拿到的是 limitPx（滑点保护限价），不是成交均价。
		// 已有 Filled.AvgPx 或 userFills 的 px 时不要覆盖。
		if avg == "" && ap != "" {
			avg = ap
		}
	} else if oid != 0 || cloid != "" {
		if view, ok := s.lookupOrder(ctx, oid, cloid); ok {
			if f := view.filledQty(); f.GreaterThan(filled) {
				filled = f
			}
			if oid == 0 {
				oid = view.oid
			}
		}
	}
	outStatus := "accepted"
	switch {
	case st.errMsg != "":
		return nil, fmt.Errorf("entropy rejected order: %s", st.errMsg)
	case filled.GreaterThan(decimal.Zero) && filled.GreaterThanOrEqual(qty):
		outStatus = "filled"
	case filled.GreaterThan(decimal.Zero):
		outStatus = "partial"
	case ioc:
		outStatus = "unknown"
	}
	// 缺成交均价就留空：上层用决策 BBO。绝不能回填保护限价，否则执行带
	// 会把每腿 ~max_slippage 记成真实滑点（开+平约 20 bp 假亏）。
	return rtt.merge(map[string]string{
		"order_id":        strconv.FormatInt(oid, 10),
		"client_order_id": rawCloid,
		"filled_qty":      filled.String(),
		"status":          outStatus,
		"avg_price":       avg,
	}), nil
}

type hlPlaceParsed struct {
	oid     int64
	filled  decimal.Decimal
	avgPx   string
	errMsg  string
	resting bool
}

func parseHlPlaceStatus(raw json.RawMessage) (hlPlaceParsed, error) {
	var wrap struct {
		Status   string `json:"status"`
		Response any    `json:"response"`
	}
	if err := json.Unmarshal(raw, &wrap); err != nil {
		return hlPlaceParsed{}, err
	}
	if strings.EqualFold(wrap.Status, "err") {
		return hlPlaceParsed{}, fmt.Errorf("%v", wrap.Response)
	}
	var inner struct {
		Type string `json:"type"`
		Data struct {
			Statuses []json.RawMessage `json:"statuses"`
		} `json:"data"`
	}
	b, _ := json.Marshal(wrap.Response)
	if err := json.Unmarshal(b, &inner); err != nil {
		return hlPlaceParsed{}, fmt.Errorf("entropy place: decode response: %w", err)
	}
	if len(inner.Data.Statuses) == 0 {
		return hlPlaceParsed{}, fmt.Errorf("entropy place returned no statuses")
	}
	var one struct {
		Resting *struct {
			Oid int64 `json:"oid"`
		} `json:"resting"`
		Filled *struct {
			Oid     int64  `json:"oid"`
			TotalSz string `json:"totalSz"`
			AvgPx   string `json:"avgPx"`
		} `json:"filled"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(inner.Data.Statuses[0], &one); err != nil {
		// 有时 status 直接是 error 字符串
		var s string
		if json.Unmarshal(inner.Data.Statuses[0], &s) == nil && s != "" {
			return hlPlaceParsed{errMsg: s}, nil
		}
		return hlPlaceParsed{}, err
	}
	out := hlPlaceParsed{errMsg: strings.TrimSpace(one.Error)}
	if one.Filled != nil {
		out.oid = one.Filled.Oid
		out.filled = decimalFromString(one.Filled.TotalSz)
		out.avgPx = one.Filled.AvgPx
	}
	if one.Resting != nil {
		out.oid = one.Resting.Oid
		out.resting = true
	}
	return out, nil
}

func (s *entropySession) cancel(ctx context.Context, params map[string]any) (map[string]string, error) {
	marketIndex := 0
	if n, err := paramUint64(params, "market_index"); err == nil {
		marketIndex = int(n)
	}
	asset, err := s.lookupAsset(paramString(params, "symbol", ""), marketIndex)
	if err != nil {
		return nil, err
	}
	orderID := paramString(params, "order_id", "")
	if orderID == "" {
		return nil, fmt.Errorf("order_id required")
	}
	var action any
	if oid, err := strconv.ParseInt(orderID, 10, 64); err == nil && oid > 0 {
		action = hlCancelAction{
			Type:    "cancel",
			Cancels: []hlCancelItem{{A: asset.AssetID, O: oid}},
		}
	} else {
		cloid := orderID
		if !isHLCloid(cloid) {
			cloid = hlCloid(orderID)
		}
		action = hlCancelByCloidAction{
			Type:    "cancelByCloid",
			Cancels: []hlCancelCloidItem{{Asset: asset.AssetID, Cloid: cloid}},
		}
	}
	if _, _, err := s.postAction(ctx, action); err != nil {
		return nil, err
	}
	return map[string]string{"order_id": orderID, "status": "canceled"}, nil
}

func (s *entropySession) orderStatus(ctx context.Context, params map[string]any) (map[string]string, error) {
	orderID := paramString(params, "order_id", "")
	qty, _ := paramDecimal(params, "qty")
	var oid int64
	cloid := ""
	if n, err := strconv.ParseInt(orderID, 10, 64); err == nil && n > 0 {
		oid = n
	} else if orderID != "" {
		cloid = orderID
		if !isHLCloid(cloid) {
			cloid = hlCloid(orderID)
		}
	}
	view, ok := s.lookupOrder(ctx, oid, cloid)
	if !ok {
		return map[string]string{
			"order_id":        orderID,
			"client_order_id": paramString(params, "client_order_id", ""),
			"filled_qty":      "0",
			"status":          "unknown",
			"avg_price":       "",
		}, nil
	}
	filled, avg, st := view.filledAndStatus(qty, false)
	return map[string]string{
		"order_id":        view.oidString(),
		"client_order_id": firstNonEmpty(view.cloid, orderID),
		"filled_qty":      filled.String(),
		"status":          st,
		"avg_price":       avg,
	}, nil
}

type hlOrderView struct {
	oid    int64
	cloid  string
	origSz decimal.Decimal
	remSz  decimal.Decimal
	px     string
	status string
}

func (v hlOrderView) oidString() string {
	if v.oid > 0 {
		return strconv.FormatInt(v.oid, 10)
	}
	return v.cloid
}

func (v hlOrderView) filledQty() decimal.Decimal {
	if v.origSz.GreaterThan(v.remSz) {
		return v.origSz.Sub(v.remSz)
	}
	return decimal.Zero
}

func (v hlOrderView) avgPx() string {
	return v.px
}

func (v hlOrderView) filledAndStatus(reqQty decimal.Decimal, ioc bool) (decimal.Decimal, string, string) {
	filled := v.filledQty()
	st := strings.ToLower(v.status)
	out := "accepted"
	switch {
	case strings.Contains(st, "reject"):
		out = "unknown"
	case strings.Contains(st, "fill"):
		if filled.GreaterThan(decimal.Zero) {
			out = "filled"
		} else {
			out = "unknown"
		}
	case strings.Contains(st, "cancel") || strings.Contains(st, "expire"):
		if filled.GreaterThan(decimal.Zero) {
			out = "partial"
		} else {
			out = "canceled"
		}
	case ioc:
		if filled.GreaterThan(decimal.Zero) {
			if reqQty.GreaterThan(decimal.Zero) && filled.GreaterThanOrEqual(reqQty) {
				out = "filled"
			} else {
				out = "partial"
			}
		} else {
			out = "unknown"
		}
	default:
		if filled.GreaterThan(decimal.Zero) {
			if reqQty.GreaterThan(decimal.Zero) && filled.GreaterThanOrEqual(reqQty) {
				out = "filled"
			} else {
				out = "partial"
			}
		}
	}
	return filled, "", out
}

func (s *entropySession) lookupByCloid(ctx context.Context, cloid string) (hlOrderView, bool) {
	return s.lookupOrder(ctx, 0, cloid)
}

func (s *entropySession) lookupOrder(ctx context.Context, oid int64, cloid string) (hlOrderView, bool) {
	var oidArg any
	if oid > 0 {
		oidArg = oid
	} else if cloid != "" {
		oidArg = cloid
	} else {
		return hlOrderView{}, false
	}
	var resp struct {
		Status string `json:"status"`
		Order  struct {
			Order struct {
				Oid     int64  `json:"oid"`
				Cloid   string `json:"cloid"`
				OrigSz  string `json:"origSz"`
				Sz      string `json:"sz"`
				LimitPx string `json:"limitPx"`
			} `json:"order"`
			Status string `json:"status"`
		} `json:"order"`
	}
	if err := s.infoJSON(ctx, map[string]any{
		"type": "orderStatus",
		"user": s.addr,
		"oid":  oidArg,
	}, &resp); err != nil {
		return hlOrderView{}, false
	}
	if !strings.EqualFold(resp.Status, "order") {
		return hlOrderView{}, false
	}
	o := resp.Order.Order
	return hlOrderView{
		oid:    o.Oid,
		cloid:  firstNonEmpty(o.Cloid, cloid),
		origSz: decimalFromString(o.OrigSz),
		remSz:  decimalFromString(o.Sz),
		px:     o.LimitPx,
		status: resp.Order.Status,
	}, true
}

func (s *entropySession) waitOrderFill(ctx context.Context, oid int64, cloid string, qty decimal.Decimal, fillWait time.Duration) (decimal.Decimal, string, string) {
	if fillWait <= 0 {
		fillWait = iocFillWait
	}
	deadline := time.Now().Add(fillWait)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			break
		}
		if f, px, hit := s.wsFill(oid, cloid); hit && f.GreaterThan(decimal.Zero) {
			st := "partial"
			if qty.GreaterThan(decimal.Zero) && f.GreaterThanOrEqual(qty) {
				st = "filled"
			}
			return f, px, st
		}
		tightFillPoll(deadline)
	}
	if f, px, hit := s.wsFill(oid, cloid); hit && f.GreaterThan(decimal.Zero) {
		st := "partial"
		if qty.GreaterThan(decimal.Zero) && f.GreaterThanOrEqual(qty) {
			st = "filled"
		}
		return f, px, st
	}
	if v, found := s.lookupOrder(ctx, oid, cloid); found {
		filled, avg, status := v.filledAndStatus(qty, true)
		return filled, avg, status
	}
	return decimal.Zero, "", "unknown"
}

func (s *entropySession) marketProtectPx(
	ctx context.Context,
	coin string,
	isBuy bool,
	target decimal.Decimal,
	slipPct decimal.Decimal,
	szDecimals int,
) (decimal.Decimal, error) {
	ratio := slipPct.Div(decimal.NewFromInt(100))
	if !ratio.GreaterThan(decimal.Zero) {
		ratio = decimal.RequireFromString("0.005")
	}
	base := target
	if !base.GreaterThan(decimal.Zero) {
		var book struct {
			Levels [][]struct {
				Px string `json:"px"`
			} `json:"levels"`
		}
		if err := s.infoJSON(ctx, map[string]any{"type": "l2Book", "coin": coin}, &book); err != nil {
			return decimal.Zero, err
		}
		if isBuy {
			if len(book.Levels) < 2 || len(book.Levels[1]) == 0 {
				return decimal.Zero, fmt.Errorf("no ask liquidity for %s", coin)
			}
			base = decimalFromString(book.Levels[1][0].Px)
		} else {
			if len(book.Levels) < 1 || len(book.Levels[0]) == 0 {
				return decimal.Zero, fmt.Errorf("no bid liquidity for %s", coin)
			}
			base = decimalFromString(book.Levels[0][0].Px)
		}
	}
	if !base.GreaterThan(decimal.Zero) {
		return decimal.Zero, fmt.Errorf("no protect price base for %s", coin)
	}
	if isBuy {
		return roundHlPx(base.Mul(decimal.NewFromInt(1).Add(ratio)), szDecimals, true), nil
	}
	return roundHlPx(base.Mul(decimal.NewFromInt(1).Sub(ratio)), szDecimals, false), nil
}

func (s *entropySession) nextNonce() int64 {
	n := time.Now().UnixMilli()
	if n <= s.lastNonce {
		n = s.lastNonce + 1
	}
	s.lastNonce = n
	return n
}

func (s *entropySession) postAction(ctx context.Context, action any) (json.RawMessage, placeChainRTT, error) {
	var rtt placeChainRTT
	s.mu.Lock()
	nonce := s.nextNonce()
	s.mu.Unlock()
	t0 := time.Now()
	sig, err := hlSignL1(s.pk, action, nonce, true)
	rtt.signMs = time.Since(t0).Milliseconds()
	if err != nil {
		rtt.totalMs = rtt.signMs
		return nil, rtt, err
	}
	payload := map[string]any{
		"action":    action,
		"nonce":     nonce,
		"signature": sig,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		rtt.totalMs = time.Since(t0).Milliseconds()
		return nil, rtt, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.exch, bytes.NewReader(body))
	if err != nil {
		rtt.totalMs = time.Since(t0).Milliseconds()
		return nil, rtt, err
	}
	req.Header.Set("Content-Type", "application/json")
	tSend := time.Now()
	resp, err := s.http.Do(req)
	if err != nil {
		rtt.sendMs = time.Since(tSend).Milliseconds()
		rtt.totalMs = time.Since(t0).Milliseconds()
		return nil, rtt, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	rtt.sendMs = time.Since(tSend).Milliseconds()
	rtt.totalMs = time.Since(t0).Milliseconds()
	if err != nil {
		return nil, rtt, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, rtt, fmt.Errorf("POST /exchange HTTP %s: %s", resp.Status, strings.TrimSpace(string(raw)))
	}
	return json.RawMessage(raw), rtt, nil
}

func (s *entropySession) infoJSON(ctx context.Context, payload any, dest any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.info, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("POST /info HTTP %s: %s", resp.Status, strings.TrimSpace(string(raw)))
	}
	return json.Unmarshal(raw, dest)
}

func (s *entropySession) noteWsMsg(msg []byte) {
	var env struct {
		Channel string          `json:"channel"`
		Data    json.RawMessage `json:"data"`
	}
	if json.Unmarshal(msg, &env) != nil {
		return
	}
	switch env.Channel {
	case "orderUpdates":
		var items []struct {
			Status string `json:"status"`
			Order  struct {
				Oid     int64  `json:"oid"`
				OrigSz  string `json:"origSz"`
				Sz      string `json:"sz"`
				LimitPx string `json:"limitPx"`
				Cloid   string `json:"cloid"`
			} `json:"order"`
		}
		if json.Unmarshal(env.Data, &items) != nil {
			var one struct {
				Status string `json:"status"`
				Order  struct {
					Oid     int64  `json:"oid"`
					OrigSz  string `json:"origSz"`
					Sz      string `json:"sz"`
					LimitPx string `json:"limitPx"`
					Cloid   string `json:"cloid"`
				} `json:"order"`
			}
			if json.Unmarshal(env.Data, &one) != nil {
				return
			}
			items = []struct {
				Status string `json:"status"`
				Order  struct {
					Oid     int64  `json:"oid"`
					OrigSz  string `json:"origSz"`
					Sz      string `json:"sz"`
					LimitPx string `json:"limitPx"`
					Cloid   string `json:"cloid"`
				} `json:"order"`
			}{one}
		}
		for _, it := range items {
			st := strings.ToLower(it.Status)
			if st == "open" || st == "new" || st == "resting" {
				continue
			}
			orig := decimalFromString(it.Order.OrigSz)
			rem := decimalFromString(it.Order.Sz)
			filled := orig.Sub(rem)
			if filled.LessThan(decimal.Zero) {
				filled = decimal.Zero
			}
			if st == "filled" && orig.GreaterThan(decimal.Zero) {
				filled = orig
			}
			s.setFill(strconv.FormatInt(it.Order.Oid, 10), it.Order.Cloid, filled, "")
		}
	case "userFills":
		var inner struct {
			IsSnapshot bool `json:"isSnapshot"`
			Fills      []struct {
				Oid       int64  `json:"oid"`
				Sz        string `json:"sz"`
				Px        string `json:"px"`
				Cloid     string `json:"cloid"`
				ClosedPnl string `json:"closedPnl"`
			} `json:"fills"`
		}
		if json.Unmarshal(env.Data, &inner) != nil || inner.IsSnapshot {
			return
		}
		for _, f := range inner.Fills {
			s.addFill(strconv.FormatInt(f.Oid, 10), f.Cloid, decimalFromString(f.Sz), f.Px, decimalFromString(f.ClosedPnl))
		}
	}
}

func (s *entropySession) setFill(oid, cloid string, qty decimal.Decimal, px string) {
	if !qty.GreaterThan(decimal.Zero) {
		return
	}
	s.fillMu.Lock()
	defer s.fillMu.Unlock()
	if s.fills == nil {
		s.fills = make(map[string]entropyFillNote)
	}
	for _, k := range []string{oid, cloid} {
		if k == "" || k == "0" {
			continue
		}
		n := s.fills[k]
		if qty.GreaterThan(n.fromOrder) {
			n.fromOrder = qty
		}
		if px != "" {
			n.px = px
		}
		n.at = time.Now()
		s.fills[k] = n
	}
	pruneFillCache(s.fills, func(v entropyFillNote) time.Time { return v.at })
}

func (s *entropySession) addFill(oid, cloid string, qty decimal.Decimal, px string, closedPnl decimal.Decimal) {
	if !qty.GreaterThan(decimal.Zero) && closedPnl.IsZero() {
		return
	}
	s.fillMu.Lock()
	defer s.fillMu.Unlock()
	if s.fills == nil {
		s.fills = make(map[string]entropyFillNote)
	}
	for _, k := range []string{oid, cloid} {
		if k == "" || k == "0" {
			continue
		}
		n := s.fills[k]
		n.sawFill = true
		if qty.GreaterThan(decimal.Zero) {
			n.fromFills = n.fromFills.Add(qty)
		}
		if px != "" {
			n.px = px
		}
		n.closedPnl = n.closedPnl.Add(closedPnl)
		n.at = time.Now()
		s.fills[k] = n
	}
	pruneFillCache(s.fills, func(v entropyFillNote) time.Time { return v.at })
}

func (s *entropySession) wsClosedPnl(oid, cloid string) (decimal.Decimal, bool) {
	s.fillMu.Lock()
	defer s.fillMu.Unlock()
	for _, k := range []string{oid, cloid} {
		if k == "" || k == "0" {
			continue
		}
		if n, ok := s.fills[k]; ok && n.sawFill {
			return n.closedPnl, true
		}
	}
	return decimal.Zero, false
}

func (s *entropySession) wsFill(oid int64, cloid string) (decimal.Decimal, string, bool) {
	s.fillMu.Lock()
	defer s.fillMu.Unlock()
	for _, k := range []string{strconv.FormatInt(oid, 10), cloid} {
		if k == "" || k == "0" {
			continue
		}
		if n, ok := s.fills[k]; ok {
			if q := n.qty(); q.GreaterThan(decimal.Zero) {
				return q, n.px, true
			}
		}
	}
	return decimal.Zero, "", false
}

func (s *entropySession) startOrderStream() {
	s.wsOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		s.wsStop = cancel
		go s.runOrderStream(ctx)
	})
}

func (s *entropySession) runOrderStream(ctx context.Context) {
	for ctx.Err() == nil {
		if err := s.orderStreamOnce(ctx); err != nil && ctx.Err() == nil {
			emitPush("entropy", map[string]any{"stream_error": err.Error()})
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
		}
	}
}

func (s *entropySession) orderStreamOnce(ctx context.Context) error {
	d := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	c, _, err := d.DialContext(ctx, s.wsURL, nil)
	if err != nil {
		return err
	}
	defer c.Close()
	subs := []map[string]any{
		{"method": "subscribe", "subscription": map[string]any{"type": "orderUpdates", "user": s.addr}},
		{"method": "subscribe", "subscription": map[string]any{"type": "userFills", "user": s.addr}},
	}
	for _, sub := range subs {
		if err := c.WriteJSON(sub); err != nil {
			return err
		}
	}
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(40 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-t.C:
				_ = c.WriteJSON(map[string]string{"method": "ping"})
			}
		}
	}()
	defer close(done)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		_ = c.SetReadDeadline(time.Now().Add(90 * time.Second))
		_, msg, err := c.ReadMessage()
		if err != nil {
			return err
		}
		var env struct {
			Channel string          `json:"channel"`
			Data    json.RawMessage `json:"data"`
		}
		if json.Unmarshal(msg, &env) != nil {
			continue
		}
		if env.Channel == "orderUpdates" || env.Channel == "userFills" {
			s.noteWsMsg(msg)
			emitPush("entropy", json.RawMessage(msg))
		}
	}
}

func roundHlSz(sz decimal.Decimal, szDecimals int) decimal.Decimal {
	if szDecimals < 0 {
		szDecimals = 0
	}
	step := decimal.New(1, int32(-szDecimals))
	return roundToStep(sz, step, false)
}

func roundHlPx(px decimal.Decimal, szDecimals int, roundUp bool) decimal.Decimal {
	if !px.GreaterThan(decimal.Zero) {
		return decimal.Zero
	}
	pxDec := 6 - szDecimals
	if pxDec < 0 {
		pxDec = 0
	}
	tick := decimal.New(1, int32(-pxDec))
	sig := hlSigFigStep(px, 5)
	step := tick
	if sig.GreaterThan(step) {
		step = sig
	}
	return roundToStep(px, step, roundUp)
}

func hlSigFigStep(px decimal.Decimal, figs int) decimal.Decimal {
	if !px.GreaterThan(decimal.Zero) || figs < 1 {
		return decimal.New(1, -8)
	}
	n := px.Abs()
	exp := int32(0)
	ten := decimal.NewFromInt(10)
	one := decimal.NewFromInt(1)
	for n.GreaterThanOrEqual(ten) {
		n = n.Div(ten)
		exp++
		if exp > 24 {
			break
		}
	}
	for n.LessThan(one) {
		n = n.Mul(ten)
		exp--
		if exp < -24 {
			break
		}
	}
	return decimal.New(1, exp-int32(figs-1))
}

func hlPxWire(d decimal.Decimal) string {
	s := d.String()
	if s == "-0" {
		return "0"
	}
	return s
}

func stripHip3Prefix(symbol string) string {
	if i := strings.IndexByte(symbol, ':'); i > 0 && i < 9 {
		dex := symbol[:i]
		rest := symbol[i+1:]
		if rest != "" && isAlpha(dex) {
			return rest
		}
	}
	return symbol
}

func isAlpha(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') {
			return false
		}
	}
	return true
}

func (s *entropySession) fillPnl(ctx context.Context, params map[string]any) (map[string]any, error) {
	oid := paramString(params, "order_id", "")
	want := paramString(params, "symbol", "")
	if oid != "" {
		if pnl, ok := s.wsClosedPnl(oid, oid); ok {
			return fillPnlResult(pnl, true, true), nil
		}
	}
	var raw json.RawMessage
	if err := s.infoJSON(ctx, map[string]any{"type": "userFills", "user": s.addr}, &raw); err != nil {
		return nil, err
	}
	type fillRow struct {
		Coin      string `json:"coin"`
		Oid       int64  `json:"oid"`
		Cloid     string `json:"cloid"`
		ClosedPnl string `json:"closedPnl"`
		Time      int64  `json:"time"`
	}
	var fills []fillRow
	if json.Unmarshal(raw, &fills) != nil {
		var wrap struct {
			Fills []fillRow `json:"fills"`
		}
		if err := json.Unmarshal(raw, &wrap); err != nil {
			return nil, fmt.Errorf("userFills decode: %w", err)
		}
		fills = wrap.Fills
	}
	sum := decimal.Zero
	found := false
	fillCount := 0
	nonzero := 0
	since := time.Now().Add(-30 * time.Second).UnixMilli()
	if secs := paramString(params, "lookback_secs", ""); secs != "" {
		if n, err := strconv.ParseInt(secs, 10, 64); err == nil && n > 0 {
			since = time.Now().Add(-time.Duration(n) * time.Second).UnixMilli()
		}
	}
	for _, f := range fills {
		match := false
		if oid != "" {
			match = strconv.FormatInt(f.Oid, 10) == oid || strings.EqualFold(f.Cloid, oid)
		} else if want == "*" {
			match = f.Time >= since
		} else {
			match = want != "" && symbolMatch(f.Coin, want) && f.Time >= since
		}
		if !match {
			continue
		}
		found = true
		fillCount++
		pnl := decimalFromString(f.ClosedPnl)
		sum = sum.Add(pnl)
		if !pnl.IsZero() {
			nonzero++
		}
	}
	out := fillPnlResult(sum, true, found)
	if want == "*" {
		out["fill_count"] = fillCount
		out["nonzero_closed_pnl"] = nonzero
	}
	return out, nil
}
