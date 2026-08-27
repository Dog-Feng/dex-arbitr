// SoDEX handlers（基于官方 sodex-go-sdk-public）
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
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/shopspring/decimal"
	sodexclient "github.com/sodex-tech/sodex-go-sdk-public/client"
	"github.com/sodex-tech/sodex-go-sdk-public/common/enums"
	ptypes "github.com/sodex-tech/sodex-go-sdk-public/perps/types"
	sodexws "github.com/sodex-tech/sodex-go-sdk-public/ws"
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

// sodexSession 长存 SoDEX 连接：client、accountID、地址都只解析一次。
type sodexSession struct {
	venue     sodexVenueFile
	client    *sodexclient.Client
	accountID uint64
	addr      string

	wsOnce sync.Once
	wsStop context.CancelFunc

	fillMu sync.Mutex
	fills  map[string]sodexFillNote
}

type sodexFillNote struct {
	qty decimal.Decimal
	px  string
}

func (s *sodexSession) close() {
	if s.wsStop != nil {
		s.wsStop()
	}
}

func (r *registry) sodexSession(ctx context.Context, path string) (*sodexSession, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sodex != nil {
		return r.sodex, nil
	}
	venue, err := loadSodexVenue(path)
	if err != nil {
		return nil, err
	}
	client, accountID, addr, err := connect(ctx, venue)
	if err != nil {
		return nil, err
	}
	s := &sodexSession{venue: venue, client: client, accountID: accountID, addr: addr}
	r.sodex = s
	return s, nil
}

func dispatchSodex(ctx context.Context, reg *registry, req request) (any, error) {
	s, err := reg.sodexSession(ctx, req.VenueYAML)
	if err != nil {
		return nil, err
	}
	params := req.Params
	if params == nil {
		params = map[string]any{}
	}
	switch req.Cmd {
	case "account":
		return accountSnapshot(ctx, gatewayBase(s.venue.Rest), s.addr, s.accountID)
	case "place":
		return placeOrder(ctx, s, params)
	case "cancel":
		return cancelOrder(ctx, s.client, s.accountID, params)
	case "order_status":
		return orderStatus(ctx, s.client, s.accountID, s.addr, params)
	case "funding":
		return s.funding(ctx)
	case "watch":
		s.startOrderStream()
		return map[string]string{"status": "watching"}, nil
	default:
		return nil, fmt.Errorf("unknown cmd %q", req.Cmd)
	}
}

// startOrderStream 订阅 SoDEX 私有订单流。
//
// 注意：官方文档明确 "User-specific streams do not require subscription
// authorization"——账户频道无需鉴权，任何知道地址的人都能订阅。这让实现
// 简单，但也意味着本账户的下单活动对第三方可见。
func (s *sodexSession) startOrderStream() {
	s.wsOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		s.wsStop = cancel
		go s.runOrderStream(ctx)
	})
}

func (s *sodexSession) runOrderStream(ctx context.Context) {
	for ctx.Err() == nil {
		if err := s.orderStreamOnce(ctx); err != nil && ctx.Err() == nil {
			emitPush("sodex", map[string]any{"stream_error": err.Error()})
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
		}
	}
}

func (s *sodexSession) orderStreamOnce(ctx context.Context) error {
	c, err := sodexws.NewClient(sodexWSBase(s.venue.Rest), "perps")
	if err != nil {
		return err
	}
	defer c.Close()
	if err := c.Connect(ctx); err != nil {
		return err
	}
	// SDK 自带断线重订与 ping/pong 保活。
	if _, err := c.Subscribe(
		sodexws.SubscribeParams{Channel: sodexws.ChannelAccountOrderUpd, User: s.addr},
		func(p sodexws.Push) {
			var upd sodexws.AccountOrderUpdate
			if json.Unmarshal(p.Data, &upd) != nil {
				return
			}
			s.noteOrderUpdate(upd)
			emitPush("sodex", upd)
		},
	); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}

func sodexWSBase(rest string) string {
	base := gatewayBase(rest)
	base = strings.TrimPrefix(base, "https://")
	base = strings.TrimPrefix(base, "http://")
	return "wss://" + strings.TrimRight(base, "/")
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
			AID       uint64 `json:"aid"`
			AccountID uint64 `json:"accountID"`
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

// funding 返回本所各市场的当期资金费率。
//
// 费率在 `/markets/tickers` 的 `fundingRate`（SDK 的 Ticker 有这个字段），
// 结算周期在 `/markets/symbols` 的 `fundingInterval`——后者 SDK 的 Symbol
// 结构体**没有**映射，只能自己拉原始 JSON。
//
// 周期要单独取而不是写死 3600：实测 88 个市场当前全是 3600，但这是交易所可以
// 改的参数，而改了之后费率**数值本身看不出区别**。上层年化用周期算，静默改
// 周期就会静默算错年化，所以以交易所返回的为准，拉不到才回落常量。
func (s *sodexSession) funding(ctx context.Context) (map[string]any, error) {
	tickers, err := s.client.PerpsTickers(ctx)
	if err != nil {
		return nil, err
	}
	intervals := fundingIntervals(ctx, s.venue)
	out := make([]map[string]any, 0, len(tickers))
	for _, t := range tickers {
		if t.FundingRate == nil || strings.TrimSpace(*t.FundingRate) == "" {
			continue
		}
		secs := sodexFundingIntervalFallback
		if v, ok := intervals[strings.ToUpper(t.Symbol)]; ok && v > 0 {
			secs = v
		}
		out = append(out, map[string]any{
			"symbol":        t.Symbol,
			"rate":          decimalFromString(*t.FundingRate).String(),
			"interval_secs": secs,
		})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no sodex funding rates in %d tickers", len(tickers))
	}
	return map[string]any{"rates": out}, nil
}

// fundingIntervals 按 symbol 取结算周期（秒）。拉不到就返回空表，让调用方
// 回落常量——费率本身已经拿到了，不该因为周期查询失败就整个 funding 失败。
func fundingIntervals(ctx context.Context, v sodexVenueFile) map[string]int {
	var rows []struct {
		Name            string `json:"name"`
		FundingInterval any    `json:"fundingInterval"`
	}
	u := gatewayBase(v.Rest) + "/api/v1/perps/markets/symbols"
	if err := httpGetJSON(ctx, u, &rows); err != nil {
		return nil
	}
	out := make(map[string]int, len(rows))
	for _, r := range rows {
		if n := int(decimalFromString(strings.TrimSpace(fmt.Sprint(r.FundingInterval))).IntPart()); n > 0 {
			out[strings.ToUpper(r.Name)] = n
		}
	}
	return out
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

func placeOrder(ctx context.Context, s *sodexSession, params map[string]any) (map[string]string, error) {
	c := s.client
	accountID := s.accountID
	addr := s.addr
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
	// 不驻留的单（market / aggressive_limit 都是 IOC）：下完要么成交要么
	// 消失，成交量必须轮询回查。驻留的限价单才可以单次查询。
	ioc := style != "limit"
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
		// 取整方向必须**远离**对手价：买向下、卖向上。
		// 反过来会把价格推过盘口，GTX(post-only) 直接拒单——上层给的挂价
		// 已经贴在点差内侧 1 tick，且那个 tick 是从报价小数位推断的，
		// 偏小时向上取整就会穿价。
		price = roundToStep(price, spec.TickSize, !isBuy)
		raw.Type = enums.OrderTypeLimit
		raw.TimeInForce = enums.TimeInForceGTX
		raw.Price = decimalPtr(price)
	} else if style == "aggressive_limit" {
		// 激进限价：市价腿失败后的兜底。限价 + IOC，吃不到就整单撤销。
		//
		// 取整方向和 post-only 相反——这里是**要吃单**，取整必须朝对手价
		// 靠（买向上、卖向下），朝反方向取整会让本来能吃到的量吃不到。
		// TimeInForce 用 IOC 而非 GTX：GTX 会把这张单变成 post-only，
		// 越过盘口直接被拒，兜底就废了。
		price, err := paramDecimal(params, "limit_price")
		if err != nil {
			return nil, err
		}
		price = roundToStep(price, spec.TickSize, isBuy)
		raw.Type = enums.OrderTypeLimit
		raw.TimeInForce = enums.TimeInForceIOC
		raw.Price = decimalPtr(price)
	} else {
		targetPx, _ := paramDecimal(params, "target_price")
		slipPct, _ := paramDecimal(params, "slippage_pct")
		protected, err := marketProtectPrice(ctx, c, spec.Symbol, side, spec.TickSize, targetPx, slipPct)
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
		// 请求报错不代表订单没落地：超时/连接中断时交易所可能已经收单。
		// 直接抛错会让上层丢掉 clOrdID，这张单就再也没人撤了。
		// clOrdID 是本地生成的，据此回查一次即可确认。对齐参考
		// 「WS 超时 → 撤单/REST 确认」的前提：上层必须始终握有 order_id。
		if o, ok := findOrder(ctx, c, addr, 0, clOrdID); ok {
			filled := o.ExecutedQty
			// 不驻留的单再轮询确认一次：请求虽然报错，单子可能已经落地并成交。
			if ioc {
				if f, ap, _ := waitOrderFill(ctx, s, c, addr, o.OrderID, clOrdID, qty); f.GreaterThan(decimalFromString(filled)) {
					filled = f.String()
					if ap != "" {
						o.Price = ap
					}
				}
			}
			outStatus := "accepted"
			if decimalFromString(filled).GreaterThan(decimal.Zero) {
				if decimalFromString(filled).GreaterThanOrEqual(qty) {
					outStatus = "filled"
				} else {
					outStatus = "partial"
				}
			} else if ioc {
				// IOC 查到了单但成交量为 0：不驻留，"accepted" 会让上层
				// 空等一张不存在的挂单。报 unknown 交对账兜底。
				outStatus = "unknown"
			}
			fmt.Fprintf(os.Stderr, "sodex place: request failed (%v) but order %s is live; reporting %s\n", err, clOrdID, outStatus)
			avgPrice := paramString(params, "limit_price", "")
			if ap := avgPriceFromOrder(o); ap != "" {
				avgPrice = ap
			}
			return map[string]string{
				"order_id":        strconv.FormatUint(o.OrderID, 10),
				"client_order_id": clOrdID,
				"filled_qty":      filled,
				"status":          outStatus,
				"avg_price":       avgPrice,
			}, nil
		}
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
		id := firstNonEmpty(r.ClOrdID, clOrdID)
		if ioc {
			// IOC 腿要轮询：不驻留，单次查询看到的 0 分不清
			// 「整单撤销」还是「索引还没跟上」。
			f, ap, _ := waitOrderFill(ctx, s, c, addr, r.OrderID, id, qty)
			filled = f.String()
			if ap != "" {
				avgPrice = ap
			}
		} else if o, ok := findOrder(ctx, c, addr, r.OrderID, id); ok {
			// 限价腿会驻留，成交检测由上层轮询负责，这里单次查询就够。
			filled = o.ExecutedQty
			if ap := avgPriceFromOrder(o); ap != "" {
				avgPrice = ap
			}
		}
	}
	// 交易所明确报拒单/撤单：必须如实上报，不能吞成 accepted——
	// 否则上层会把一张不存在的单当成活跃挂单，空等超时再去撤。
	if strings.Contains(status, "reject") {
		return nil, fmt.Errorf("sodex rejected order: %s", firstNonEmpty(r.Status, "rejected"))
	}
	outStatus := "accepted"
	switch {
	case strings.Contains(status, "fill"):
		// 交易所说成交了，但成交量必须真查到才算。查不到就报 unknown，
		// 让上层走仓位对账/人工介入——**绝不**用请求量顶替。对齐参考
		// 对市价单关闭 `allow_fallback`（`_infer_fill_from_status`
		// 遇市价单直接返回 None）。
		if decimalFromString(filled).GreaterThan(decimal.Zero) {
			outStatus = "filled"
		} else {
			outStatus = "unknown"
		}
	case strings.Contains(status, "cancel") || strings.Contains(status, "expire"):
		// IOC 市价单吃不到量会整单撤销。已成交部分照实报，没有就是 canceled。
		if decimalFromString(filled).GreaterThan(decimal.Zero) {
			outStatus = "partial"
		} else {
			outStatus = "canceled"
		}
	case ioc:
		// sendOrder 成功 ≠ 撮合成交。查得到就用真实成交量；查不到
		// 报 unknown 让上层用仓位对账兜底，**绝不**用请求量顶替。
		// 这里绝不能报 accepted：IOC 不驻留，上层会空等一张不存在的挂单。
		if decimalFromString(filled).GreaterThan(decimal.Zero) {
			if decimalFromString(filled).GreaterThanOrEqual(qty) {
				outStatus = "filled"
			} else {
				outStatus = "partial"
			}
		} else {
			outStatus = "unknown"
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

// marketProtectPrice 算市价单的限价保护。
//
// 基准优先用**决策信号价**（target_price）——保护要约束「相对决策价滑了多少」；
// 用下单时的盘口做基准等于自我实现，价格跑了照样成交。拿不到信号价才退回盘口。
// 对齐参考 `payload["target_price"] + slippage_percent`。
func marketProtectPrice(
	ctx context.Context,
	c *sodexclient.Client,
	symbol string,
	side enums.OrderSide,
	tickSize decimal.Decimal,
	targetPrice decimal.Decimal,
	slipPct decimal.Decimal,
) (decimal.Decimal, error) {
	// slipPct 以百分比传入（0.1 = 0.1%），转成比例。
	ratio := slipPct.Div(decimal.NewFromInt(100))
	if !ratio.GreaterThan(decimal.Zero) {
		ratio = decimal.RequireFromString("0.005")
	}
	base := targetPrice
	if !base.GreaterThan(decimal.Zero) {
		if symbol == "" {
			return decimal.Zero, fmt.Errorf("symbol required for market order")
		}
		book, err := c.PerpsOrderBook(ctx, symbol, 5)
		if err != nil {
			return decimal.Zero, err
		}
		if side == enums.OrderSideBuy {
			if len(book.Asks) == 0 {
				return decimal.Zero, fmt.Errorf("no ask liquidity for %s", symbol)
			}
			base = decimalFromString(book.Asks[0].Price)
		} else {
			if len(book.Bids) == 0 {
				return decimal.Zero, fmt.Errorf("no bid liquidity for %s", symbol)
			}
			base = decimalFromString(book.Bids[0].Price)
		}
	}
	if !base.GreaterThan(decimal.Zero) {
		return decimal.Zero, fmt.Errorf("no protect price base for %s", symbol)
	}
	if side == enums.OrderSideBuy {
		return roundToStep(base.Mul(decimal.NewFromInt(1).Add(ratio)), tickSize, true), nil
	}
	return roundToStep(base.Mul(decimal.NewFromInt(1).Sub(ratio)), tickSize, false), nil
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

func orderStatus(ctx context.Context, c *sodexclient.Client, accountID uint64, addr string, params map[string]any) (map[string]string, error) {
	_ = accountID
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
	// 不在活跃列表：可能是已成交/已撤，也可能是刚下单 API 尚未可见；禁止假定 filled。
	return map[string]string{
		"order_id":   orderID,
		"filled_qty": "0",
		"status":     "unknown",
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

// waitOrderFill 轮询订单直到查到成交量或窗口用尽。
//
// 对齐参考 `_wait_for_order_fill_rest`：下单响应里的数量不可信，成交量必须
// 回查，且**单次查询不够**——撮合与订单索引之间有延迟，刚下的市价单常在
// 第一次查询时还显示 ExecutedQty=0。单次查询会把「已成交」误判成
// 「查不到」，上层随即当成未对冲，正是幻影成交的反向版本。
//
// 返回 (已确认成交量, 均价, 是否至少查到过这张单)。
func (s *sodexSession) noteOrderUpdate(upd sodexws.AccountOrderUpdate) {
	qty := decimalFromString(upd.FilledQty)
	if !qty.GreaterThan(decimal.Zero) {
		return
	}
	px := upd.LastPrice
	if px == "" {
		px = upd.Price
	}
	s.fillMu.Lock()
	defer s.fillMu.Unlock()
	if s.fills == nil {
		s.fills = make(map[string]sodexFillNote)
	}
	note := sodexFillNote{qty: qty, px: px}
	for _, k := range []string{upd.ClOrdID, strconv.FormatInt(upd.OrderID, 10)} {
		if k == "" || k == "0" {
			continue
		}
		if prev, ok := s.fills[k]; ok && prev.qty.GreaterThan(qty) {
			continue
		}
		s.fills[k] = note
	}
}

func (s *sodexSession) wsFill(orderID uint64, clOrdID string) (decimal.Decimal, string, bool) {
	s.fillMu.Lock()
	defer s.fillMu.Unlock()
	for _, k := range []string{clOrdID, strconv.FormatUint(orderID, 10)} {
		if k == "" || k == "0" {
			continue
		}
		if n, ok := s.fills[k]; ok && n.qty.GreaterThan(decimal.Zero) {
			return n.qty, n.px, true
		}
	}
	return decimal.Zero, "", false
}

func waitOrderFill(
	ctx context.Context,
	s *sodexSession,
	c *sodexclient.Client,
	addr string,
	orderID uint64,
	clOrdID string,
	want decimal.Decimal,
) (decimal.Decimal, string, bool) {
	deadline := time.Now().Add(sodexFillWait)
	best := decimal.Zero
	avg := ""
	seen := false
	for {
		if s != nil {
			if f, px, ok := s.wsFill(orderID, clOrdID); ok {
				seen = true
				if f.GreaterThan(best) {
					best = f
					if px != "" {
						avg = px
					}
				}
				if want.GreaterThan(decimal.Zero) && best.GreaterThanOrEqual(want) {
					return best, avg, true
				}
			}
		}
		if o, ok := findOrder(ctx, c, addr, orderID, clOrdID); ok {
			seen = true
			if f := decimalFromString(o.ExecutedQty); f.GreaterThan(best) {
				best = f
				if ap := avgPriceFromOrder(o); ap != "" {
					avg = ap
				}
			}
			if want.GreaterThan(decimal.Zero) && best.GreaterThanOrEqual(want) {
				return best, avg, true
			}
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return best, avg, seen
		}
		tightFillPoll(deadline)
	}
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
