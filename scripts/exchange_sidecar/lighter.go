package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	lighterclient "github.com/elliottech/lighter-go/client"
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
	// IOC：sendTx → 等该单 WS 1 秒 → 查一次订单。同市场串行，避免并发
	// 市价单的确认窗口叠在一起。
	marketLocks sync.Map // marketIndex -> *sync.Mutex

	// 市场精度缓存。A3：marketDecimals 原来每次下单都打一遍
	// /api/v1/orderBookDetails，和 lastTradePrice 重复请求同一接口。
	// 常驻后首次拉取即缓存——精度不会在运行期变。
	decMu sync.RWMutex
	decs  map[int]marketDecimals

	// 私有 WS 订单流。ws 非 nil 表示已启动监听。
	wsOnce sync.Once
	wsStop context.CancelFunc

	// WS 推送的成交量，按 client_order_index / order_index 索引。
	// waitMarketFill 命中 WS 后立刻返回；1 秒没有再查一次订单。
	fillMu sync.Mutex
	fills  map[string]lighterFill
}

type lighterFill struct {
	qty decimal.Decimal
	at  time.Time
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
	// 传 nil 作为 HTTP 客户端：nonce 由我们自己管理（syncNonce），
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
	if s, ok := r.lighter[venueID]; ok {
		r.mu.Unlock()
		return s, nil
	}
	r.mu.Unlock()

	venue, err := loadLighterVenue(path)
	if err != nil {
		return nil, err
	}
	s := &lighterSession{
		venue:      venue,
		baseURL:    lighterBaseURL(venue.Rest),
		httpClient: &http.Client{Timeout: 30 * time.Second},
		decs:       make(map[int]marketDecimals),
		fills:      make(map[string]lighterFill),
	}
	if err := s.connect(ctx); err != nil {
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.lighter[venueID]; ok {
		s.close()
		return existing, nil
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
// 只认该 order_id：先等私有 WS（最多 1 秒），没有再 REST 查一次
// 活跃单 / 非活跃单。不用持仓 delta。
func (s *lighterSession) waitMarketFill(
	ctx context.Context,
	marketIndex int,
	want decimal.Decimal,
	orderID string,
) decimal.Decimal {
	deadline := time.Now().Add(iocFillWait)
	sizeDec := 0
	if d, err := s.marketDecimals(ctx, marketIndex); err == nil {
		sizeDec = d.sizeDec
	}
	for {
		if orderID != "" {
			if f, ok := s.wsFilled(orderID, want, sizeDec); ok {
				if want.GreaterThan(decimal.Zero) && f.GreaterThan(want) {
					return want
				}
				return f
			}
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			break
		}
		tightFillPoll(deadline)
	}
	if orderID != "" {
		if f, ok := s.wsFilled(orderID, want, sizeDec); ok {
			if want.GreaterThan(decimal.Zero) && f.GreaterThan(want) {
				return want
			}
			return f
		}
		if found, ok := s.lookupOrder(ctx, marketIndex, orderID); ok && found.filled.GreaterThan(decimal.Zero) {
			if want.GreaterThan(decimal.Zero) && found.filled.GreaterThan(want) {
				return want
			}
			return found.filled
		}
	}
	return decimal.Zero
}

func (s *lighterSession) noteWsFills(msg map[string]any) {
	orders := rawList(firstValue(msg, "orders", "trades"))
	if len(orders) == 0 && looksLikeOrderOrTrade(msg) {
		orders = []map[string]any{msg}
	}
	if len(orders) == 0 {
		return
	}
	s.fillMu.Lock()
	defer s.fillMu.Unlock()
	if s.fills == nil {
		s.fills = make(map[string]lighterFill)
	}
	now := time.Now()
	for _, raw := range orders {
		filled := orderFilledQty(raw)
		if !filled.GreaterThan(decimal.Zero) {
			continue
		}
		tradeInc := firstValue(raw, "trade_id", "trade_id_str") != nil
		for _, key := range orderIdKeys(raw) {
			if key == "" || key == "0" {
				continue
			}
			qty := filled
			if tradeInc {
				if prev, ok := s.fills[key]; ok {
					qty = prev.qty.Add(filled)
				}
			} else if prev, ok := s.fills[key]; ok && prev.qty.GreaterThan(qty) {
				continue
			}
			s.fills[key] = lighterFill{qty: qty, at: now}
		}
	}
	pruneFillCache(s.fills, func(v lighterFill) time.Time { return v.at })
}

func orderFilledQty(raw map[string]any) decimal.Decimal {
	filled := decimalValue(firstValue(raw, "filled_base_amount", "filled_amount", "size"))
	if filled.GreaterThan(decimal.Zero) {
		return filled
	}
	init := decimalValue(raw["initial_base_amount"])
	rem := decimalValue(raw["remaining_base_amount"])
	if init.GreaterThan(rem) && rem.GreaterThanOrEqual(decimal.Zero) {
		return init.Sub(rem)
	}
	return decimal.Zero
}

func orderIdKeys(raw map[string]any) []string {
	// 官方 Order/Trade：整数 id 另有 *_str。优先字符串，避免 JSON 整数走 float64。
	return []string{
		stringValue(firstValue(raw, "client_order_id", "client_order_index")),
		stringValue(firstValue(raw, "order_id", "order_index")),
		stringValue(firstValue(raw, "ask_client_id_str", "ask_client_id")),
		stringValue(firstValue(raw, "bid_client_id_str", "bid_client_id")),
		stringValue(firstValue(raw, "ask_id_str", "ask_id")),
		stringValue(firstValue(raw, "bid_id_str", "bid_id")),
	}
}

func looksLikeOrderOrTrade(m map[string]any) bool {
	return firstValue(m, "client_order_index", "order_index", "filled_base_amount", "trade_id", "ask_id", "bid_id", "size") != nil
}

func (s *lighterSession) wsFilled(orderID string, want decimal.Decimal, sizeDec int) (decimal.Decimal, bool) {
	s.fillMu.Lock()
	defer s.fillMu.Unlock()
	n, ok := s.fills[orderID]
	if !ok || !n.qty.GreaterThan(decimal.Zero) {
		return decimal.Zero, false
	}
	filled := normalizeWsFilled(n.qty, want, sizeDec)
	if !filled.GreaterThan(decimal.Zero) {
		return decimal.Zero, false
	}
	return filled, true
}

// WS 有的所回报人类可读数量，有的回报 size_decimals 缩放整数。
// 整数时两种解读都算一遍，取更接近请求量的那个——比「>50× 就除」稳。
func normalizeWsFilled(raw, want decimal.Decimal, sizeDec int) decimal.Decimal {
	if sizeDec <= 0 || !raw.GreaterThan(decimal.Zero) {
		return raw
	}
	if !want.GreaterThan(decimal.Zero) {
		return raw
	}
	scaled := raw.Div(decimal.New(1, int32(sizeDec)))
	lo := want.Div(decimal.NewFromInt(2))
	hi := want.Mul(decimal.NewFromInt(2))
	if scaled.GreaterThanOrEqual(lo) && scaled.LessThanOrEqual(hi) && raw.GreaterThan(hi) {
		return scaled
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
		return marketDecimals{}, err
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
	return marketDecimals{}, fmt.Errorf("lighter market %d decimals unknown", marketIndex)
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
	acct := s.venue.AccountIndex
	for _, ch := range []string{
		fmt.Sprintf("account_all_orders/%d", acct),
		fmt.Sprintf("account_all_trades/%d", acct),
	} {
		sub := map[string]any{"type": "subscribe", "channel": ch, "auth": token}
		if err := conn.WriteJSON(sub); err != nil {
			return fmt.Errorf("subscribe %s: %w", ch, err)
		}
	}

	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(40 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				_ = conn.Close()
				return
			case <-done:
				_ = conn.Close()
				return
			case <-t.C:
				_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					_ = conn.Close()
					return
				}
			}
		}
	}()
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		return nil
	})
	_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))

	for {
		var msg map[string]any
		if err := conn.ReadJSON(&msg); err != nil {
			return err
		}
		_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		kind := stringValue(msg["type"])
		ch := stringValue(msg["channel"])
		if !strings.Contains(kind, "order") && !strings.Contains(kind, "trade") &&
			!strings.Contains(ch, "order") && !strings.Contains(ch, "trade") {
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

	coidStr := paramString(params, "client_order_id", fmt.Sprintf("%d", nextLighterClientOrderID()))
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

	price, err := uint32PriceUnits(priceUnits, dec.priceDec)
	if err != nil {
		return nil, err
	}
	mi, err := int16MarketIndex(marketIndex)
	if err != nil {
		return nil, err
	}

	// IOC：per-market 锁罩住 sendTx → waitMarketFill，避免同市场并发
	// 市价确认窗口叠在一起。
	if ioc {
		lk := s.marketLock(marketIndex)
		lk.Lock()
		defer lk.Unlock()
	}

	if err := s.syncNonce(ctx); err != nil {
		return nil, err
	}
	// 取 nonce / 签名 / 发送必须作为一个整体串行，见 submitMu 注释。
	// 链上拉 nonce 已在 syncNonce（锁外）完成，这里只做本地自增。
	var signErr, sendErr error
	var signMs, sendMs, totalMs int64
	func() {
		s.submitMu.Lock()
		defer s.submitMu.Unlock()
		nonce, nerr := s.takeNonce()
		if nerr != nil {
			signErr = nerr
			return
		}
		// 耗时：Go 签名开始 → sendTx 收到 Lighter 确认回包。不含等锁、拉 nonce、成交回查。
		tSign := time.Now()
		tx, terr := s.txClient.GetCreateOrderTransaction(&types.CreateOrderTxReq{
			MarketIndex:      mi,
			ClientOrderIndex: coid,
			BaseAmount:       baseAmount,
			Price:            price,
			IsAsk:            boolByte(!isBuy),
			Type:             orderType,
			TimeInForce:      tif,
			ReduceOnly:       boolByte(reduceOnly),
			OrderExpiry:      expiry,
		}, s.ops(nonce))
		if terr != nil {
			s.invalidateNonce()
			signErr = terr
			signMs = time.Since(tSign).Milliseconds()
			totalMs = signMs
			return
		}
		info := txInfo(tx)
		signMs = time.Since(tSign).Milliseconds()
		tSend := time.Now()
		// nonce 无条件重同步：交易可能已上链也可能没有，本地回退会撞已用号。
		sendErr = s.sendTx(ctx, int(tx.GetTxType()), info)
		sendMs = time.Since(tSend).Milliseconds()
		totalMs = time.Since(tSign).Milliseconds()
		if sendErr != nil {
			s.invalidateNonce()
		}
	}()
	coidStr = strconv.FormatInt(coid, 10)
	if signMs > 0 || sendMs > 0 || totalMs > 0 {
		logLighterPlaceRTT(s.venue.ID, coidStr, signMs, sendMs, totalMs, sendErr == nil && signErr == nil)
	}
	if signErr != nil {
		return nil, fmt.Errorf("sign create order: %w", signErr)
	}
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
				return mergePlaceRTT(map[string]string{
					"order_id":        coidStr,
					"client_order_id": coidStr,
					"filled_qty":      found.filled.String(),
					"status":          status,
					"avg_price":       found.price,
				}, signMs, sendMs, totalMs), nil
			}
			return nil, sendErr
		}
		// IOC 腿：sendTx 报错仍可能已上链。等该单 WS / 查一次订单。
		filled := s.waitMarketFill(ctx, marketIndex, qty, coidStr)
		if filled.GreaterThan(decimal.Zero) {
			status := "filled"
			if filled.LessThan(qty) {
				status = "partial"
			}
			fmt.Fprintf(os.Stderr, "lighter place: sendTx failed (%v) but order %s filled %s; reporting %s\n", sendErr, coidStr, filled, status)
			return mergePlaceRTT(map[string]string{
				"order_id":        coidStr,
				"client_order_id": coidStr,
				"filled_qty":      filled.String(),
				"status":          status,
				"avg_price":       "",
			}, signMs, sendMs, totalMs), nil
		}
		return nil, sendErr
	}

	if !ioc {
		return mergePlaceRTT(map[string]string{
			"order_id":        coidStr,
			"client_order_id": coidStr,
			"filled_qty":      "0",
			"status":          "accepted",
			"avg_price":       limitPriceStr,
		}, signMs, sendMs, totalMs), nil
	}
	// IOC：sendTx 成功 ≠ 撮合成交。等该单 WS 1 秒，没有再查一次。
	filled := s.waitMarketFill(ctx, marketIndex, qty, coidStr)
	status := "filled"
	switch {
	case filled.LessThanOrEqual(decimal.Zero):
		status = "unknown"
	case filled.LessThan(qty):
		status = "partial"
	}
	return mergePlaceRTT(map[string]string{
		"order_id":        coidStr,
		"client_order_id": coidStr,
		"filled_qty":      filled.String(),
		"status":          status,
		"avg_price":       "",
	}, signMs, sendMs, totalMs), nil
}

func logLighterPlaceRTT(venue, orderID string, signMs, sendMs, totalMs int64, ok bool) {
	fmt.Fprintf(os.Stderr, "%s\n", formatLighterPlaceRTT(venue, orderID, signMs, sendMs, totalMs, ok))
}

func formatLighterPlaceRTT(venue, orderID string, signMs, sendMs, totalMs int64, ok bool) string {
	result := "ok"
	if !ok {
		result = "err"
	}
	return fmt.Sprintf(
		"lighter place rtt venue=%s order=%s sign_ms=%d send_ms=%d sign_to_ack_ms=%d result=%s",
		venue, orderID, signMs, sendMs, totalMs, result,
	)
}

func mergePlaceRTT(out map[string]string, signMs, sendMs, totalMs int64) map[string]string {
	if out == nil {
		out = make(map[string]string)
	}
	out["sign_ms"] = strconv.FormatInt(signMs, 10)
	out["send_ms"] = strconv.FormatInt(sendMs, 10)
	out["sign_to_ack_ms"] = strconv.FormatInt(totalMs, 10)
	return out
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
	mi, err := int16MarketIndex(marketIndex)
	if err != nil {
		return nil, err
	}
	if err := s.syncNonce(ctx); err != nil {
		return nil, err
	}
	// 撤单同样要串行：它和下单共用同一条 nonce 序列。
	s.submitMu.Lock()
	defer s.submitMu.Unlock()
	nonce, err := s.takeNonce()
	if err != nil {
		return nil, err
	}
	tx, err := s.txClient.GetCancelOrderTransaction(&types.CancelOrderTxReq{
		MarketIndex: mi,
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

	found, ok := s.lookupOrder(ctx, marketIndex, orderID)
	if !ok {
		if qty.GreaterThan(decimal.Zero) {
			if dec, err := s.marketDecimals(ctx, marketIndex); err == nil {
				if f, hit := s.wsFilled(orderID, qty, dec.sizeDec); hit {
					status := "partial"
					if f.GreaterThanOrEqual(qty) {
						status = "filled"
					}
					return map[string]string{
						"order_id":   orderID,
						"filled_qty": f.String(),
						"status":     status,
						"avg_price":  "",
					}, nil
				}
			}
		}
		return map[string]string{
			"order_id":   orderID,
			"filled_qty": "0",
			"status":     "unknown",
			"avg_price":  "",
		}, nil
	}
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
			filled: orderFilledQty(raw),
			price:  stringValue(firstValue(raw, "price", "avg_price")),
		}, true
	}
	return activeOrder{}, false
}

// lookupOrder 查一次该单：先活跃列表，没有再查非活跃（IOC 成交后不在活跃列表）。
func (s *lighterSession) lookupOrder(ctx context.Context, marketIndex int, orderID string) (activeOrder, bool) {
	if orderID == "" {
		return activeOrder{}, false
	}
	token, err := s.txClient.GetAuthToken(time.Now().Add(10 * time.Minute))
	if err != nil {
		return activeOrder{}, false
	}
	hdr := http.Header{"authorization": {token}}
	acct := strconv.FormatInt(s.venue.AccountIndex, 10)
	mid := strconv.Itoa(marketIndex)
	if result, err := s.get(ctx, "/api/v1/accountActiveOrders", url.Values{
		"account_index": {acct},
		"market_id":     {mid},
	}, hdr); err == nil {
		if found, ok := matchActiveOrder(result, orderID); ok {
			return found, true
		}
	}
	if result, err := s.get(ctx, "/api/v1/accountInactiveOrders", url.Values{
		"account_index": {acct},
		"market_id":     {mid},
		"limit":         {"50"},
	}, hdr); err == nil {
		if found, ok := matchActiveOrder(result, orderID); ok {
			return found, true
		}
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

// 任何 sendTx 失败都必须调 invalidateNonce：预留的号没有上链，不清掉的话
// 本地序号比链上永久高出 1，该 venue 之后每笔下单和撤单都会因 nonce 不匹配被拒。
//
// syncNonce 保证本地有可自增的序号。网络拉取在 nonceMu / submitMu 之外，
// 这样 REST 挂起时不会堵住已经有缓存 nonce 的撤单。
func (s *lighterSession) syncNonce(ctx context.Context) error {
	s.nonceMu.Lock()
	if s.nextNonce != nil {
		s.nonceMu.Unlock()
		return nil
	}
	s.nonceMu.Unlock()

	fetched, err := s.fetchNextNonce(ctx)
	if err != nil {
		return err
	}
	s.nonceMu.Lock()
	defer s.nonceMu.Unlock()
	if s.nextNonce == nil {
		s.nextNonce = &fetched
	}
	return nil
}

func (s *lighterSession) takeNonce() (int64, error) {
	s.nonceMu.Lock()
	defer s.nonceMu.Unlock()
	if s.nextNonce == nil {
		return 0, errors.New("nonce not synced")
	}
	n := *s.nextNonce
	next := n + 1
	s.nextNonce = &next
	return n, nil
}

func (s *lighterSession) fetchNextNonce(ctx context.Context) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, nonceFetchTimeout)
	defer cancel()
	result, err := s.get(ctx, "/api/v1/nextNonce", url.Values{
		"account_index": {strconv.FormatInt(s.venue.AccountIndex, 10)},
		"api_key_index": {strconv.Itoa(int(s.venue.APIKeyIndex))},
	}, nil)
	if err != nil {
		return 0, fmt.Errorf("fetch nonce: %w", err)
	}
	n, ok := int64Value(result["nonce"])
	if !ok {
		return 0, errors.New("fetch nonce: missing nonce in response")
	}
	return n, nil
}

func (s *lighterSession) marketLock(marketIndex int) *sync.Mutex {
	v, _ := s.marketLocks.LoadOrStore(marketIndex, &sync.Mutex{})
	return v.(*sync.Mutex)
}

func uint32PriceUnits(units int64, priceDec int) (uint32, error) {
	if units <= 0 {
		return 0, errors.New("non-positive price units")
	}
	if units > int64(math.MaxUint32) {
		return 0, fmt.Errorf("price overflow uint32 after scale 10^%d: %d", priceDec, units)
	}
	return uint32(units), nil
}

func int16MarketIndex(idx int) (int16, error) {
	if idx < math.MinInt16 || idx > math.MaxInt16 {
		return 0, fmt.Errorf("market index %d out of int16 range", idx)
	}
	return int16(idx), nil
}

func int64Value(v any) (int64, bool) {
	switch t := v.(type) {
	case float64:
		return int64(t), true
	case int64:
		return t, true
	case int:
		return int64(t), true
	case json.Number:
		n, err := t.Int64()
		return n, err == nil
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(t), 10, 64)
		return n, err == nil
	default:
		return 0, false
	}
}

// invalidateNonce 丢弃本地序号，下次 syncNonce 重新向链上同步。
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
	case map[string]any:
		// Lighter WS：{"1": [order, ...]} 按市场 id 分组。账户 / orderBookDetails
		// 是对象数组，走上面 []any，不能把对象内部字段拆开，否则丢 market_id。
		var out []map[string]any
		for _, item := range v {
			if arr, ok := item.([]any); ok {
				out = append(out, rawList(arr)...)
			}
		}
		if len(out) > 0 {
			return out
		}
		if looksLikeOrderOrTrade(v) {
			return []map[string]any{v}
		}
		return nil
	default:
		return nil
	}
}

func stringValue(v any) string {
	if v == nil {
		return ""
	}
	switch n := v.(type) {
	case string:
		return strings.TrimSpace(n)
	case json.Number:
		return n.String()
	case float64:
		// encoding/json 把整数解成 float64；%v/%g 对 >1e6 的 client_order_id
		// 会打成科学计数法，WS 对不上下单时的十进制字符串，市价腿就永远 unknown。
		if n == math.Trunc(n) && n >= -1e15 && n <= 1e15 {
			return strconv.FormatInt(int64(n), 10)
		}
		return strconv.FormatFloat(n, 'f', -1, 64)
	case float32:
		return stringValue(float64(n))
	case int:
		return strconv.Itoa(n)
	case int64:
		return strconv.FormatInt(n, 10)
	case uint64:
		return strconv.FormatUint(n, 10)
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
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
		"ask_client_id_str", "bid_client_id_str", "ask_id_str", "bid_id_str",
		"ask_client_id", "bid_client_id", "ask_id", "bid_id",
	} {
		if stringValue(firstValue(t, k)) == orderID {
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
