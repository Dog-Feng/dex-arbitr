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
	"github.com/gorilla/websocket"
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
	// submitMu 串行化「取 nonce → 签名 → sendTx」这一整段。对齐参考
	// `_lighter_order_lock`（"确保所有下单操作串行"）。
	//
	// 只锁 nonceMu 不够：它保护的是计数器，不是**提交顺序**。两个 goroutine
	// 各自拿到 5 和 6 后并发发送，6 先到链上就会被判 invalid nonce，而 5 随后
	// 也废掉。A1 常驻化之后进程复用，这个窗口才真实存在——裸仓自动对冲
	// (`try_hedge_naked_exposures` 只按 slot 加锁) 可以和主执行任务并发命中
	// 同一个 Lighter 账户。
	//
	// 刻意只覆盖到 sendTx 返回：之后的成交回查是 REST 轮询，把它圈进来会让
	// 并发的撤单排在后面等，而撤单在超时路径上是要抢时间的。
	submitMu sync.Mutex

	// 市场精度缓存。A3：marketDecimals 原来每次下单都打一遍
	// /api/v1/orderBookDetails，和 lastTradePrice 重复请求同一接口。
	// 常驻后首次拉取即缓存——精度不会在运行期变。
	decMu sync.RWMutex
	decs  map[int]marketDecimals

	// 私有 WS 订单流。ws 非 nil 表示已启动监听。
	wsOnce sync.Once
	wsStop context.CancelFunc

	// WS 推送的成交量，按 client_order_index / order_index 索引。
	// waitMarketFill 命中后立刻返回，不必再等 REST 持仓快照（原先 2s 一轮）。
	fillMu sync.Mutex
	fills  map[string]decimal.Decimal
}

func (s *lighterSession) close() {
	if s.wsStop != nil {
		s.wsStop()
	}
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
	// 传 nil 作为 HTTP 客户端：nonce 由我们自己管理（reserveNonce），
	// 绝不让 SDK 在签名路径上偷偷发请求，也不调 Check()。
	// Check() 内部走 /api/v1/apikeys，在某些网络环境下会阻塞 10+ 秒，
	// 而且对下单没有任何保护意义——参数错了 sendTx 会直接报错。
	txClient, err := lighterclient.NewTxClient(nil, s.venue.APIKeyPrivateKey, s.venue.AccountIndex, s.venue.APIKeyIndex, s.venue.ChainID)
	if err != nil {
		return fmt.Errorf("create lighter signer: %w", err)
	}
	s.txClient = txClient
	return nil
}

// dispatchLighter 复用 registry 里的长存会话：连接、API key 校验、
// 市场精度都只做一次。
func dispatchLighter(ctx context.Context, reg *registry, venueID string, req request) (any, error) {
	s, err := reg.lighterSession(ctx, venueID, req.VenueYAML)
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
		// 启动私有订单流。幂等：重复调用只会启动一次。
		s.startOrderStream(venueID)
		return map[string]string{"status": "watching"}, nil
	case "fill_pnl":
		return s.fillPnl(ctx, params)
	default:
		return nil, fmt.Errorf("unknown cmd %q", req.Cmd)
	}
}

func (r *registry) lighterSession(ctx context.Context, venueID, path string) (*lighterSession, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.lighter[venueID]; ok {
		return s, nil
	}
	venue, err := loadLighterVenue(path)
	if err != nil {
		return nil, err
	}
	s := &lighterSession{
		venue:      venue,
		baseURL:    lighterBaseURL(venue.Rest),
		httpClient: &http.Client{Timeout: 30 * time.Second},
		decs:       make(map[int]marketDecimals),
	}
	if err := s.connect(ctx); err != nil {
		return nil, err
	}
	r.lighter[venueID] = s
	return s, nil
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
		if rpnl := stringValue(firstValue(raw, "realized_pnl", "realizedPnl")); rpnl != "" {
			entry["realized_pnl"] = rpnl
		}
		positions = append(positions, entry)
	}
	return map[string]any{"balances": balances, "positions": positions}, nil
}

type marketDecimals struct {
	sizeDec  int
	priceDec int
}

// marketPositionSize 返回该市场的**带符号**持仓量。
// 市价单成交确认要用它做 delta：sendTx 成功只代表序列器收下交易，
// 不代表撮合成交（IOC 吃不到量会整单撤销）。
func (s *lighterSession) marketPositionSize(ctx context.Context, marketIndex int, symbol string) (decimal.Decimal, error) {
	result, err := s.get(ctx, "/api/v1/account", url.Values{
		"by":    {"index"},
		"value": {strconv.FormatInt(s.venue.AccountIndex, 10)},
	}, nil)
	if err != nil {
		return decimal.Zero, err
	}
	accounts := rawList(result["accounts"])
	if len(accounts) == 0 {
		return decimal.Zero, errors.New("lighter account snapshot unavailable")
	}
	want := strings.TrimSpace(symbol)
	total := decimal.Zero
	for _, raw := range rawList(accounts[0]["positions"]) {
		// market_id 优先；缺失时退回 symbol 匹配。
		if mi, ok := intValue(firstValue(raw, "market_id", "market_index")); ok {
			if mi != marketIndex {
				continue
			}
		} else if want == "" || !strings.EqualFold(stringValue(raw["symbol"]), want) {
			continue
		}
		size := decimalValue(firstValue(raw, "position", "size"))
		if intOr(raw["sign"], 0) < 0 {
			size = size.Neg()
		}
		total = total.Add(size)
	}
	return total, nil
}

// waitMarketFill 确认市价腿真实成交量。
//
// 优先看私有 WS（account_all_orders 推送到就认），REST 持仓 delta 兜底。
// 轮询间隔 100ms：旧实现复刻参考的 2s sleep，市价其实已经成交，本地却要
// 再等一轮才返回——日志上就是「第一腿成交后 3–4 秒第二腿才成功」。
func (s *lighterSession) waitMarketFill(
	ctx context.Context,
	marketIndex int,
	symbol string,
	baseline decimal.Decimal,
	isBuy bool,
	want decimal.Decimal,
	orderID string,
) decimal.Decimal {
	deadline := time.Now().Add(lighterFillWait)
	best := decimal.Zero
	sizeDec := 4
	if d, err := s.marketDecimals(ctx, marketIndex); err == nil {
		sizeDec = d.sizeDec
	}
	for {
		if orderID != "" {
			if f, ok := s.wsFilled(orderID, want, sizeDec); ok && f.GreaterThan(best) {
				best = f
				if best.GreaterThanOrEqual(want) {
					return want
				}
			}
		}
		if cur, err := s.marketPositionSize(ctx, marketIndex, symbol); err == nil {
			delta := cur.Sub(baseline)
			if !isBuy {
				delta = delta.Neg()
			}
			if delta.GreaterThan(best) {
				best = delta
			}
			if best.GreaterThanOrEqual(want) {
				return want
			}
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return best
		}
		remain := time.Until(deadline)
		nap := 100 * time.Millisecond
		if remain < nap {
			nap = remain
		}
		if nap <= 0 {
			return best
		}
		time.Sleep(nap)
	}
}

func (s *lighterSession) noteWsFills(msg map[string]any) {
	orders := rawList(firstValue(msg, "orders"))
	if len(orders) == 0 {
		if firstValue(msg, "client_order_index", "order_index", "filled_base_amount") != nil {
			orders = []map[string]any{msg}
		}
	}
	if len(orders) == 0 {
		return
	}
	s.fillMu.Lock()
	defer s.fillMu.Unlock()
	if s.fills == nil {
		s.fills = make(map[string]decimal.Decimal)
	}
	for _, raw := range orders {
		filled := decimalValue(firstValue(raw, "filled_base_amount", "filled_amount"))
		if !filled.GreaterThan(decimal.Zero) {
			continue
		}
		for _, key := range []string{
			stringValue(firstValue(raw, "client_order_index", "client_order_id")),
			stringValue(firstValue(raw, "order_index", "order_id")),
		} {
			if key != "" && key != "0" {
				if prev, ok := s.fills[key]; !ok || filled.GreaterThan(prev) {
					s.fills[key] = filled
				}
			}
		}
	}
}

func (s *lighterSession) wsFilled(orderID string, want decimal.Decimal, sizeDec int) (decimal.Decimal, bool) {
	s.fillMu.Lock()
	defer s.fillMu.Unlock()
	raw, ok := s.fills[orderID]
	if !ok || !raw.GreaterThan(decimal.Zero) {
		return decimal.Zero, false
	}
	filled := normalizeWsFilled(raw, want, sizeDec)
	if !filled.GreaterThan(decimal.Zero) {
		return decimal.Zero, false
	}
	return filled, true
}

// WS 有的所回报人类可读数量，有的回报 size_decimals 缩放整数。
// 只在「整数且明显比 want 大一个数量级」时才除回去，避免把 1.5 ETH 误当成整数缩放。
func normalizeWsFilled(raw, want decimal.Decimal, sizeDec int) decimal.Decimal {
	if sizeDec <= 0 || !raw.GreaterThan(decimal.Zero) {
		return raw
	}
	if raw.Equal(raw.Truncate(0)) && want.GreaterThan(decimal.Zero) && raw.GreaterThan(want.Mul(decimal.NewFromInt(50))) {
		return raw.Div(decimal.New(1, int32(sizeDec)))
	}
	return raw
}

func (s *lighterSession) marketDecimals(ctx context.Context, marketIndex int) (marketDecimals, error) {
	// A3：精度是静态的，命中缓存就不再打 REST。
	s.decMu.RLock()
	if d, ok := s.decs[marketIndex]; ok {
		s.decMu.RUnlock()
		return d, nil
	}
	s.decMu.RUnlock()

	result, err := s.get(ctx, "/api/v1/orderBookDetails", url.Values{"filter": {"perp"}}, nil)
	if err != nil {
		return marketDecimals{4, 2}, err
	}
	// 一次拿到全部市场，顺手全缓存——之后任何币种都不用再请求。
	s.decMu.Lock()
	for _, item := range rawList(result["order_book_details"]) {
		mi := intOr(firstValue(item, "market_id", "market_index"), -1)
		if mi < 0 {
			continue
		}
		s.decs[mi] = marketDecimals{
			sizeDec:  intOr(firstValue(item, "supported_size_decimals", "size_decimals"), 4),
			priceDec: intOr(firstValue(item, "supported_price_decimals", "price_decimals"), 2),
		}
	}
	d, ok := s.decs[marketIndex]
	s.decMu.Unlock()
	if ok {
		return d, nil
	}
	return marketDecimals{4, 2}, nil
}

// startOrderStream 连接 Lighter 私有 WS，订阅本账户订单推送。
// 对齐参考 `_send_account_subscriptions`：channel = account_all_orders/{account_index}，
// 带 auth token。收到推送后立刻 emitPush 给 Rust，取代 REST 轮询。
func (s *lighterSession) startOrderStream(venueID string) {
	s.wsOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		s.wsStop = cancel
		go s.runOrderStream(ctx, venueID)
	})
}

func (s *lighterSession) runOrderStream(ctx context.Context, venueID string) {
	for ctx.Err() == nil {
		if err := s.orderStreamOnce(ctx, venueID); err != nil && ctx.Err() == nil {
			// 断线重连，退避 2s。成交检测仍有 REST 轮询兜底，这里不致命。
			emitPush(venueID, map[string]any{"stream_error": err.Error()})
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
		}
	}
}

func (s *lighterSession) orderStreamOnce(ctx context.Context, venueID string) error {
	wsURL := lighterWSURL(s.venue.Rest)
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial %s: %w", wsURL, err)
	}
	defer conn.Close()

	token, err := s.txClient.GetAuthToken(time.Now().Add(30 * time.Minute))
	if err != nil {
		return fmt.Errorf("auth token: %w", err)
	}
	sub := map[string]any{
		"type":    "subscribe",
		"channel": fmt.Sprintf("account_all_orders/%d", s.venue.AccountIndex),
		"auth":    token,
	}
	if err := conn.WriteJSON(sub); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	for {
		var msg map[string]any
		if err := conn.ReadJSON(&msg); err != nil {
			return err
		}
		kind := stringValue(msg["type"])
		if !strings.Contains(kind, "order") {
			continue
		}
		s.noteWsFills(msg)
		// 原样透传给 Rust，由它按 client_order_index 匹配挂单。
		emitPush(venueID, msg)
	}
}

func lighterWSURL(rest string) string {
	base := lighterBaseURL(rest)
	base = strings.TrimPrefix(base, "https://")
	base = strings.TrimPrefix(base, "http://")
	return "wss://" + strings.TrimRight(base, "/") + "/stream"
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
	// 不驻留的单：market 和 aggressive_limit 都是 IOC，下完要么成交要么消失。
	// 成交量只能靠持仓 delta 确认，不能假设它挂在盘口上等着被查。
	ioc := style != "limit"
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
		// client_order_id 无法解析为正 int64（如 UUID 字符串）时明确报错，
		// 而非静默替换为时间戳——替换后 OrderAck 返回的 id 与请求不符，
		// 导致 order_status/cancel 找不到订单。
		return nil, fmt.Errorf("lighter requires a positive int64 client_order_id, got %q (parse error: %v)", coidStr, err)
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
	} else if style == "aggressive_limit" {
		// 激进限价：上层已经把 `信号价 × (1 ± 2×滑点)` 算好传下来，这里直接
		// 用作限价上限。类型仍是 LimitOrder（价格必须被尊重），但 TIF 换成
		// IOC——吃不到就整单撤销，绝不能用 PostOnly，那会让这张越过盘口的单
		// 直接被拒，兜底就废了。
		if limitPriceStr == "" {
			return nil, errors.New("limit_price required for aggressive limit order")
		}
		lp, err := decimal.NewFromString(limitPriceStr)
		if err != nil {
			return nil, err
		}
		pScale := decimal.New(1, int32(dec.priceDec))
		priceUnits = lp.Mul(pScale).IntPart()
		if priceUnits <= 0 {
			return nil, errors.New("invalid aggressive limit price")
		}
		tif = txtypes.ImmediateOrCancel
		expiry = txtypes.NilOrderExpiry
	} else {
		// 保护价基准优先用决策信号价（target_price）：保护要约束「相对
		// 决策价滑了多少」，用下单时的盘口做基准等于自我实现。
		// 对齐参考 `payload["target_price"] + slippage_percent`。
		base, _ := paramDecimal(params, "target_price")
		if !base.GreaterThan(decimal.Zero) {
			last, err := s.lastTradePrice(ctx, marketIndex)
			if err != nil {
				return nil, err
			}
			base = last
		}
		// slippage_pct 以百分比传入（0.1 = 0.1%）。缺省退回旧的 ±5%。
		slipPct, _ := paramDecimal(params, "slippage_pct")
		ratioDelta := slipPct.Div(decimal.NewFromInt(100))
		if !ratioDelta.GreaterThan(decimal.Zero) {
			ratioDelta = decimal.NewFromFloat(0.05)
		}
		one := decimal.NewFromInt(1)
		ratio := one.Add(ratioDelta)
		if !isBuy {
			ratio = one.Sub(ratioDelta)
		}
		prot := base.Mul(ratio)
		pScale := decimal.New(1, int32(dec.priceDec))
		priceUnits = prot.Mul(pScale).IntPart()
		if priceUnits <= 0 {
			return nil, errors.New("invalid market protection price")
		}
		orderType = txtypes.MarketOrder
		tif = txtypes.ImmediateOrCancel
		expiry = txtypes.NilOrderExpiry
	}

	// IOC 腿：下单**前**取持仓基线，成交量只能靠 delta 确认。
	// 基线查询要一次 REST，可能失败，所以必须排在 reserveNonce **之前**：
	// 对齐参考 `_place_market_order`「只有在所有前置条件都满足后，才获取nonce，
	// 这样可以避免nonce被消耗但订单没有发送的情况」。
	baseline := decimal.Zero
	haveBaseline := false
	if ioc {
		if b, berr := s.marketPositionSize(ctx, marketIndex, paramString(params, "symbol", "")); berr == nil {
			baseline = b
			haveBaseline = true
		} else {
			// 拿不到基线就无法确认成交；上报 unknown 让上层走自己的对账，
			// 绝不能假定全额成交。
			return nil, fmt.Errorf("market baseline unavailable: %w", berr)
		}
	}

	// 取 nonce / 签名 / 发送必须作为一个整体串行，见 submitMu 注释。
	var reserveErr, signErr, sendErr error
	func() {
		s.submitMu.Lock()
		defer s.submitMu.Unlock()
		nonce, nerr := s.reserveNonce(ctx)
		if nerr != nil {
			reserveErr = nerr
			return
		}
		tx, terr := s.txClient.GetCreateOrderTransaction(&types.CreateOrderTxReq{
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
		if terr != nil {
			s.invalidateNonce()
			signErr = terr
			return
		}
		// nonce 无条件重同步：交易可能已上链也可能没有，本地回退会撞已用号。
		if sendErr = s.sendTx(ctx, int(tx.GetTxType()), txInfo(tx)); sendErr != nil {
			s.invalidateNonce()
		}
	}()
	if reserveErr != nil {
		return nil, reserveErr
	}
	if signErr != nil {
		return nil, fmt.Errorf("sign create order: %w", signErr)
	}
	coidStr = strconv.FormatInt(coid, 10)
	if sendErr != nil {
		// 对齐参考「不确认就不下结论」：先问交易所这张单在不在。
		if !ioc {
			if found, ok := s.confirmOrderLanded(ctx, marketIndex, coidStr); ok {
				status := "accepted"
				if found.filled.GreaterThan(decimal.Zero) {
					if found.filled.GreaterThanOrEqual(qty) {
						status = "filled"
					} else {
						status = "partial"
					}
				}
				fmt.Fprintf(os.Stderr, "lighter place: sendTx failed (%v) but order %s is live; reporting %s\n", sendErr, coidStr, status)
				return map[string]string{
					"order_id":        coidStr,
					"client_order_id": coidStr,
					"filled_qty":      found.filled.String(),
					"status":          status,
					"avg_price":       found.price,
				}, nil
			}
			return nil, sendErr
		}
		// IOC 腿：撤不了也查不到活跃单（不驻留），只能看持仓 delta。
		// 有成交就必须上报，否则上层按「未下单」记账 → 裸仓。
		if haveBaseline {
			filled := s.waitMarketFill(ctx, marketIndex, paramString(params, "symbol", ""), baseline, isBuy, qty, coidStr)
			if filled.GreaterThan(decimal.Zero) {
				status := "filled"
				if filled.LessThan(qty) {
					status = "partial"
				}
				fmt.Fprintf(os.Stderr, "lighter place: sendTx failed (%v) but position moved %s; reporting %s\n", sendErr, filled, status)
				return map[string]string{
					"order_id":        coidStr,
					"client_order_id": coidStr,
					"filled_qty":      filled.String(),
					"status":          status,
					"avg_price":       limitPriceStr,
				}, nil
			}
		}
		return nil, sendErr
	}

	if !ioc {
		return map[string]string{
			"order_id":        coidStr,
			"client_order_id": coidStr,
			"filled_qty":      "0",
			"status":          "accepted",
			"avg_price":       limitPriceStr,
		}, nil
	}
	// IOC 腿：用持仓 delta 确认真实成交量。sendTx 成功 ≠ 撮合成交——
	// IOC 在限价（市价是保护价 ±5%，激进限价是上层算的价）内吃不到量会整单
	// 撤销，此时若上报 filled=请求量，上层会按幻影成交记账，平仓时会把实际
	// 还在的仓位清零成裸仓。
	if !haveBaseline {
		return nil, errors.New("market fill unverifiable: no baseline")
	}
	filled := s.waitMarketFill(ctx, marketIndex, paramString(params, "symbol", ""), baseline, isBuy, qty, coidStr)
	status := "filled"
	switch {
	case filled.LessThanOrEqual(decimal.Zero):
		// 没观察到任何成交：可能确实整单撤销，也可能是快照延迟。
		// 报 unknown 而不是 filled/rejected，让上层用自己的仓位对账兜底。
		status = "unknown"
	case filled.LessThan(qty):
		status = "partial"
	}
	return map[string]string{
		"order_id":        coidStr,
		"client_order_id": coidStr,
		"filled_qty":      filled.String(),
		"status":          status,
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
	// 撤单同样要串行：它和下单共用同一条 nonce 序列。
	s.submitMu.Lock()
	defer s.submitMu.Unlock()
	nonce, err := s.reserveNonce(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := s.txClient.GetCancelOrderTransaction(&types.CancelOrderTxReq{
		MarketIndex: int16(marketIndex),
		Index:       orderIndex,
	}, s.ops(nonce))
	if err != nil {
		s.invalidateNonce()
		return nil, err
	}
	if err := s.sendTx(ctx, int(tx.GetTxType()), txInfo(tx)); err != nil {
		s.invalidateNonce()
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
	if found, ok := matchActiveOrder(result, orderID); ok {
		status := "accepted"
		if found.filled.GreaterThan(decimal.Zero) {
			if qty.GreaterThan(decimal.Zero) && found.filled.GreaterThanOrEqual(qty) {
				status = "filled"
			} else {
				status = "partial"
			}
		}
		return map[string]string{
			"order_id":   orderID,
			"filled_qty": found.filled.String(),
			"status":     status,
			"avg_price":  found.price,
		}, nil
	}
	return map[string]string{
		"order_id":   orderID,
		"filled_qty": "0",
		"status":     "unknown",
		"avg_price":  "",
	}, nil
}

type activeOrder struct {
	filled decimal.Decimal
	price  string
}

func matchActiveOrder(result map[string]any, orderID string) (activeOrder, bool) {
	for _, raw := range rawList(result["orders"]) {
		coid := stringValue(firstValue(raw, "client_order_index", "client_order_id"))
		oid := stringValue(firstValue(raw, "order_index", "order_id"))
		if coid != orderID && oid != orderID {
			continue
		}
		return activeOrder{
			filled: decimalValue(firstValue(raw, "filled_base_amount", "filled_amount")),
			price:  stringValue(raw["price"]),
		}, true
	}
	return activeOrder{}, false
}

// confirmOrderLanded 回答「这张单到底有没有挂上」。
//
// sendTx 报错不代表交易没被序列器收下：超时/连接中断时它可能已经上链。
// 此时若直接把错误抛给上层，上层会认为「没下单」而丢掉 coid，
// 于是这张单再也没人撤——参考项目靠 `_cancel_after_ws_timeout` 撤单来确认
// 成交与否，前提是**上层始终握有 order_id**。所以这里宁可多打一次 REST，
// 也要把「已挂上」的事实连同 coid 交回去。
func (s *lighterSession) confirmOrderLanded(ctx context.Context, marketIndex int, orderID string) (activeOrder, bool) {
	token, err := s.txClient.GetAuthToken(time.Now().Add(10 * time.Minute))
	if err != nil {
		return activeOrder{}, false
	}
	result, err := s.get(ctx, "/api/v1/accountActiveOrders", url.Values{
		"account_index": {strconv.FormatInt(s.venue.AccountIndex, 10)},
		"market_id":     {strconv.Itoa(marketIndex)},
	}, http.Header{"authorization": {token}})
	if err != nil {
		return activeOrder{}, false
	}
	return matchActiveOrder(result, orderID)
}

// reserveNonce 取一个 nonce 并本地自增。
//
// A1 常驻化前每次调用都是新进程，`nextNonce` 必然重新拉取，自带纠偏。
// 常驻后本地自增会一直跑下去，所以**任何 sendTx 失败都必须调
// invalidateNonce**：预留的号没有上链，不清掉的话本地序号比链上永久
// 高出 1，该 venue 之后每笔下单和撤单都会因 nonce 不匹配被拒——
// 持仓期间撞上就是平仓指令再也发不出去。
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

// invalidateNonce 丢弃本地序号，下次 reserveNonce 重新向链上同步。
//
// 不做 `nextNonce--`：失败原因可能是超时或网络中断，交易实际已被序列器
// 收下，此时回退会把下一笔的 nonce 撞到已用号上。重新拉取是唯一安全的选择。
func (s *lighterSession) invalidateNonce() {
	s.nonceMu.Lock()
	defer s.nonceMu.Unlock()
	s.nextNonce = nil
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

// funding 返回本所各市场的当期资金费率。
//
// ⚠️ `/api/v1/funding-rates` 是**聚合**端点：一次返回 700+ 条，混了
// binance / bybit / hyperliquid / lighter 四个所（实测 725 条里 lighter 只占
// 212）。必须按 `exchange == "lighter"` 过滤，否则会拿别人的费率去算我们的
// 持仓成本——binance 是 8 小时结算，数量级差 8 倍，年化直接错一个数量级。
//
// `rate` 已经是**每小时实收值**：Lighter 文档的 `fundingRate = clamp(...) / 8`
// 里那个 /8 是交易所侧算好的，结算时按小时直接扣，不再除。所以年化倍数是
// 24 × 365，不是参考项目那个按 8 小时结算写的 3 × 365。
func (s *lighterSession) funding(ctx context.Context) (map[string]any, error) {
	result, err := s.get(ctx, "/api/v1/funding-rates", nil, nil)
	if err != nil {
		return nil, err
	}
	rows := rawList(firstValue(result, "funding_rates", "fundingRates"))
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		if !strings.EqualFold(stringValue(firstValue(row, "exchange")), "lighter") {
			continue
		}
		symbol := stringValue(firstValue(row, "symbol"))
		if symbol == "" {
			continue
		}
		out = append(out, map[string]any{
			"symbol":        symbol,
			"market_index":  intOr(firstValue(row, "market_id", "marketId"), -1),
			"rate":          decimalValue(firstValue(row, "rate", "funding_rate")).String(),
			"interval_secs": lighterFundingInterval,
		})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no lighter funding rates in %d rows", len(rows))
	}
	return map[string]any{"rates": out}, nil
}

// fillPnl：全平后持仓 realized_pnl 会回到 0，不能当本笔盈亏。
// 改走 /api/v1/trades（该 order 的成交），用所方带回的 entry_quote / size_before，
// 或成交上若有 pnl 字段就直接用。
func (s *lighterSession) fillPnl(ctx context.Context, params map[string]any) (map[string]any, error) {
	oid := paramString(params, "order_id", "")
	want := paramString(params, "symbol", "")
	if oid != "" {
		all, err := s.fetchAccountTrades(ctx)
		if err != nil {
			return nil, err
		}
		var trades []map[string]any
		for _, t := range all {
			if tradeMatchesOrder(t, oid) {
				trades = append(trades, t)
			}
		}
		sum := decimal.Zero
		found := false
		for _, t := range trades {
			if v, ok := tradeExplicitPnl(t, s.venue.AccountIndex); ok {
				found = true
				sum = sum.Add(v)
				continue
			}
			if v, ok := tradeRealizedFromEntry(t, s.venue.AccountIndex); ok {
				found = true
				sum = sum.Add(v)
			}
		}
		out := fillPnlResult(sum, true, found)
		if paramString(params, "debug", "") == "1" {
			out["trade_count"] = len(trades)
			out["all_count"] = len(all)
			if len(trades) > 0 {
				out["sample"] = trades[0]
			} else if len(all) > 0 {
				out["sample"] = all[0]
			}
		}
		return out, nil
	}
	result, err := s.get(ctx, "/api/v1/account", url.Values{
		"by":    {"index"},
		"value": {strconv.FormatInt(s.venue.AccountIndex, 10)},
	}, nil)
	if err != nil {
		return nil, err
	}
	accounts := rawList(result["accounts"])
	if len(accounts) == 0 {
		return fillPnlResult(decimal.Zero, false, false), nil
	}
	for _, raw := range rawList(accounts[0]["positions"]) {
		sym := stringValue(raw["symbol"])
		if !symbolMatch(sym, want) {
			continue
		}
		pnl := decimalValue(firstValue(raw, "realized_pnl", "realizedPnl"))
		return fillPnlResult(pnl, false, true), nil
	}
	return fillPnlResult(decimal.Zero, false, false), nil
}

func (s *lighterSession) fetchAccountTrades(ctx context.Context) ([]map[string]any, error) {
	token, err := s.txClient.GetAuthToken(time.Now().Add(10 * time.Minute))
	if err != nil {
		return nil, fmt.Errorf("lighter auth token: %w", err)
	}
	q := url.Values{
		"account_index": {strconv.FormatInt(s.venue.AccountIndex, 10)},
		"sort_by":       {"timestamp"},
		"sort_dir":      {"desc"},
		"limit":         {"50"},
		"auth":          {token},
	}
	result, err := s.get(ctx, "/api/v1/trades", q, http.Header{"authorization": {token}})
	if err != nil {
		return nil, err
	}
	return rawList(result["trades"]), nil
}

func tradeMatchesOrder(t map[string]any, orderID string) bool {
	for _, k := range []string{
		"ask_id", "bid_id", "ask_id_str", "bid_id_str",
		"ask_client_id", "bid_client_id", "ask_client_id_str", "bid_client_id_str",
	} {
		v := firstValue(t, k)
		if v == nil {
			continue
		}
		if strings.TrimSpace(fmt.Sprint(v)) == orderID {
			return true
		}
	}
	return false
}

func tradeExplicitPnl(t map[string]any, accountIndex int64) (decimal.Decimal, bool) {
	for _, k := range []string{
		"account_pnl", "realized_pnl", "closed_pnl", "pnl",
		"ask_account_pnl", "bid_account_pnl",
	} {
		if _, ok := t[k]; !ok {
			continue
		}
		// ask/bid 字段只认自己那一侧
		if k == "ask_account_pnl" && int64(intOr(t["ask_account_id"], 0)) != accountIndex {
			continue
		}
		if k == "bid_account_pnl" && int64(intOr(t["bid_account_id"], 0)) != accountIndex {
			continue
		}
		return decimalValue(t[k]), true
	}
	return decimal.Zero, false
}

func tradeRealizedFromEntry(t map[string]any, accountIndex int64) (decimal.Decimal, bool) {
	bidID := int64(intOr(firstValue(t, "bid_account_id", "bid_account_index"), 0))
	askID := int64(intOr(firstValue(t, "ask_account_id", "ask_account_index"), 0))
	weBid := bidID == accountIndex
	weAsk := askID == accountIndex
	if !weBid && !weAsk {
		return decimal.Zero, false
	}
	isMakerAsk := false
	switch v := t["is_maker_ask"].(type) {
	case bool:
		isMakerAsk = v
	}
	weMaker := (weAsk && isMakerAsk) || (weBid && !isMakerAsk)
	role := "taker"
	if weMaker {
		role = "maker"
	}
	sizeBefore := decimalValue(firstValue(t, role+"_position_size_before"))
	entryQuote := decimalValue(firstValue(t, role+"_entry_quote_before"))
	px := decimalValue(t["price"])
	sz := decimalValue(t["size"])
	if !sz.GreaterThan(decimal.Zero) || !px.GreaterThan(decimal.Zero) {
		return decimal.Zero, false
	}
	if !sizeBefore.GreaterThan(decimal.Zero) || !entryQuote.GreaterThan(decimal.Zero) {
		// 开仓或加仓：没有已实现。
		return decimal.Zero, true
	}
	entryPx := entryQuote.Div(sizeBefore)
	closed := sz
	if sizeBefore.LessThan(sz) {
		closed = sizeBefore
	}
	if weAsk {
		return px.Sub(entryPx).Mul(closed), true
	}
	return entryPx.Sub(px).Mul(closed), true
}
