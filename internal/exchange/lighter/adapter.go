// Package lighter æ¯ Lighterï¼zkLighterï¼æ°¸ç»­åçº¦äº¤ææçééå¨ã
//
// äº¤æèµ°ãæ¬å°ç­¾å L2 äº¤æ â POST /api/v1/sendTxãï¼è¡æä¸è´¦æ·èµ°æ®é RESTï¼
// è®¢ååæ¥ä¸ä»ä½èµ° WebSocketãç­¾åä¾èµå®æ¹ç github.com/elliottech/lighter-goã
package lighter

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"dex-grid/internal/config"
	"dex-grid/internal/domain/account"
	"dex-grid/internal/domain/market"
	"dex-grid/internal/domain/order"
	"dex-grid/internal/domain/position"
	"dex-grid/internal/exchange"

	lclient "github.com/elliottech/lighter-go/client"
	ltypes "github.com/elliottech/lighter-go/types"
	"github.com/elliottech/lighter-go/types/txtypes"
	"github.com/shopspring/decimal"
)

// Name æ¯äº¤ææå¨æ³¨åè¡¨ä¸­çåç§°ã
const Name = "lighter"

// marketCacheTTL æ¯å¸åºåæ°æ®çç¼å­æ¶é¿ã
// åæ°æ®ååæå°ï¼ä½ä¿è¯éçåä¸æ¶ç¶æä¼è°æ´ï¼æä»¥ä¸åæ°¸ä¹ç¼å­ã
const marketCacheTTL = 10 * time.Minute

// Lighter çå¸åºç±»åä¸ç¶æåå¼ã
//
// ä¸»ç½ä¸é¤äº 227 ä¸ªæ°¸ç»­åçº¦ï¼è¿æ 8 ä¸ªç°è´§å¸åºï¼market_id ä» 2048 èµ·ï¼
// ç¬¦å·å½¢å¦ "ETH/USDC"ï¼ãç°è´§æ²¡ææ æãä¸æ¯æ reduce-onlyï¼åçº¦ç½æ ¼å¨
// ä¸é¢æ ¹æ¬è·ä¸èµ·æ¥ï¼å æ­¤ééå¨ä¸è¿é¨å°±æå®ä»¬æ»¤æã
const (
	perpMarketType = "perp"
	activeStatus   = "active"
)

// Options æ¯ config.yaml ä¸­ exchanges[].options çåå®¹ã
type Options struct {
	ChainID         uint32          `yaml:"chain_id"`
	TxSendChannel   string          `yaml:"tx_send_channel"`
	BatchEnabled    bool            `yaml:"batch_enabled"`
	BatchSize       int             `yaml:"batch_size"`
	OrderExpiry     config.Duration `yaml:"order_expiry"`
	PriceProtection *bool           `yaml:"price_protection"`
}

type cachedMarket struct {
	detail orderBookDetail
	model  market.Market
}

// Adapter å®ç° exchange.Exchange ä¸ exchange.Streamerã
type Adapter struct {
	log          *slog.Logger
	rest         *restClient
	tx           *txSender
	accountIndex int64
	orderExpiry  time.Duration

	wsURL            string
	http             *http.Client
	reconnectInitial time.Duration
	reconnectMax     time.Duration

	mu       sync.RWMutex
	bySymbol map[string]cachedMarket
	byIndex  map[int]cachedMarket
	loadedAt time.Time
}

var _ exchange.Exchange = (*Adapter)(nil)

func init() {
	exchange.Register(Name, New)
}

// New ä»éç½®æé ééå¨ã
func New(cfg config.Exchange, deps exchange.Deps) (exchange.Exchange, error) {
	var opts Options
	if err := cfg.DecodeOptions(&opts); err != nil {
		return nil, err
	}

	baseURL, wsURL := cfg.BaseURL, cfg.WSURL
	chainID := opts.ChainID
	if baseURL == "" {
		if cfg.Network == "testnet" {
			baseURL = testnetREST
		} else {
			baseURL = mainnetREST
		}
	}
	if wsURL == "" {
		if cfg.Network == "testnet" {
			wsURL = testnetWS
		} else {
			wsURL = mainnetWS
		}
	}
	if chainID == 0 {
		if cfg.Network == "testnet" {
			chainID = testnetChainID
		} else {
			chainID = mainnetChainID
		}
	}
	expiry := opts.OrderExpiry.Std()
	if expiry <= 0 {
		expiry = 28 * 24 * time.Hour
	}
	if max := 30 * 24 * time.Hour; expiry > max {
		return nil, fmt.Errorf("lighter: options.order_expiry %s è¶è¿äº¤ææä¸é 30d", expiry)
	}
	priceProtection := opts.PriceProtection == nil || *opts.PriceProtection

	rest := newRESTClient(baseURL, deps.HTTP, cfg.RateLimit.RPS, cfg.RateLimit.Burst, cfg.MaxRetries)

	tx, err := newTxSender(rest, cfg.Credentials.APIKeyPrivateKey,
		cfg.Credentials.AccountIndex, cfg.Credentials.APIKeyIndex, chainID, priceProtection)
	if err != nil {
		return nil, err
	}

	log := deps.Log
	if log == nil {
		log = slog.Default()
	}
	return &Adapter{
		log:              log.With("exchange", Name),
		rest:             rest,
		tx:               tx,
		accountIndex:     cfg.Credentials.AccountIndex,
		orderExpiry:      expiry,
		wsURL:            wsURL,
		http:             deps.HTTP,
		reconnectInitial: cfg.Reconnect.Initial.Std(),
		reconnectMax:     cfg.Reconnect.Max.Std(),
		bySymbol:         map[string]cachedMarket{},
		byIndex:          map[int]cachedMarket{},
	}, nil
}

// httpClient è¿åç» WebSocket æ¨å·ç¨çå®¢æ·ç«¯ã
//
// å¤ç¨ REST çå®¢æ·ç«¯æ¯ä¸ºäºå±äº«ä»£çéç½®ï¼ä½å¿é¡»å»æè¶æ¶ï¼
// http.Client.Timeout ä¼ä½ç¨äºæ´ä¸ªè¿æ¥ççå½å¨æï¼å¥å¨é¿è¿æ¥ä¸
// ä¼è®© WebSocket å¨è¶æ¶æ¶é´å°ç¹åè¢«ç¡¬ççææ­ã
func (a *Adapter) httpClient() *http.Client {
	if a.http == nil {
		return nil
	}
	c := *a.http
	c.Timeout = 0
	return &c
}

func (a *Adapter) Name() string { return Name }

func (a *Adapter) Capabilities() exchange.Capabilities {
	return exchange.Capabilities{
		// æ¹éæäº¤ï¼sendTxBatchï¼å°æªå®ç°ï¼å¦å®æ¥ 0ï¼ç±ä¸å±ä¸²è¡åéã
		BatchPlace:  0,
		BatchCancel: 0,
		ModifyOrder: true,
		PostOnly:    true,
		ReduceOnly:  true,
		NativeTPSL:  true,
	}
}

func (a *Adapter) Close() error { return nil }

// --- å¸åºåæ°æ® ---

// refreshMarkets æåå¨é¨å¸åºè¯¦æå¹¶å·æ°ç¼å­ã
//
// åªç¼å­æ°¸ç»­åçº¦ãå·²ä¸æ¶çåçº¦ä»ç¶ç¼å­ââè¿è¡ä¸­çå®ä¾å¯è½ææå®çä»ä½ï¼
// å¯¹è´¦æ¶éè¦åæ°æ®ââä½ä¸ä¼åºç°å¨ä¸æåè¡¨éï¼ä¹ä¸åè®¸æ°å¼ä»ã
func (a *Adapter) refreshMarkets(ctx context.Context) error {
	details, err := a.rest.orderBookDetails(ctx, -1)
	if err != nil {
		return err
	}
	bySymbol := make(map[string]cachedMarket, len(details))
	byIndex := make(map[int]cachedMarket, len(details))
	skipped := 0
	for _, d := range details {
		if d.MarketType != perpMarketType {
			skipped++
			continue
		}
		cm := cachedMarket{detail: d, model: toMarket(d)}
		bySymbol[strings.ToUpper(d.Symbol)] = cm
		byIndex[d.MarketID] = cm
	}

	a.mu.Lock()
	a.bySymbol, a.byIndex, a.loadedAt = bySymbol, byIndex, time.Now()
	a.mu.Unlock()

	a.log.Debug("å¸åºåæ°æ®å·²å·æ°", "perp", len(byIndex), "skipped_non_perp", skipped)
	return nil
}

func (a *Adapter) ensureMarkets(ctx context.Context) error {
	a.mu.RLock()
	fresh := time.Since(a.loadedAt) < marketCacheTTL && len(a.bySymbol) > 0
	a.mu.RUnlock()
	if fresh {
		return nil
	}
	return a.refreshMarkets(ctx)
}

// lookup æ symbol æ¥æ¾æ°¸ç»­åçº¦å¸åºãç°è´§å·²ç»å¨ç¼å­é¶æ®µè¢«æ»¤æï¼
// ä¼ å¥ç°è´§ç¬¦å·ï¼å¦ "ETH/USDC"ï¼ä¼å½ä¸­è¿éçãæªæ¾å°ãåæ¯ã
func (a *Adapter) lookup(ctx context.Context, symbol string) (cachedMarket, error) {
	if err := a.ensureMarkets(ctx); err != nil {
		return cachedMarket{}, err
	}
	a.mu.RLock()
	cm, ok := a.bySymbol[strings.ToUpper(symbol)]
	a.mu.RUnlock()
	if !ok {
		return cachedMarket{}, exchange.Classify(exchange.ClassInvalidParam, "lookup_market",
			fmt.Errorf("lighter: æªæ¾å°æ°¸ç»­åçº¦äº¤æå¯¹ %qï¼æ¬ç³»ç»åªååçº¦ç½æ ¼ï¼ä¸æ¯æç°è´§ï¼", symbol))
	}
	return cm, nil
}

// tradable å¨ lookup ä¹ä¸è¿½å ãå½åå¯å¼ä»ãçæ£æ¥ã
// ä¸æ¶çåçº¦ä»å¯æ¥è¯¢ä¸å¹³ä»ï¼ä½ä¸åè®¸å¼æ°ä»ã
func (a *Adapter) tradable(ctx context.Context, symbol string) (cachedMarket, error) {
	cm, err := a.lookup(ctx, symbol)
	if err != nil {
		return cm, err
	}
	if cm.detail.Status != activeStatus {
		return cm, exchange.Classify(exchange.ClassInvalidParam, "lookup_market",
			fmt.Errorf("lighter: åçº¦ %s å½åç¶æä¸º %qï¼ä¸å¯äº¤æ", cm.detail.Symbol, cm.detail.Status))
	}
	return cm, nil
}

// LookupByIndex æ market_index æ¥æ¾æ°¸ç»­åçº¦å¸åºã
//
// é¡µé¢æ¥å¥åæ²¡æäº¤æå¯¹ä¸æï¼éè¦ç´æ¥ç¨ market_index æå®æ çã
func (a *Adapter) LookupByIndex(ctx context.Context, index int) (market.Market, error) {
	if err := a.ensureMarkets(ctx); err != nil {
		return market.Market{}, err
	}
	a.mu.RLock()
	cm, ok := a.byIndex[index]
	a.mu.RUnlock()
	if !ok {
		return market.Market{}, fmt.Errorf(
			"lighter: æªæ¾å° market_index %d å¯¹åºçæ°¸ç»­åçº¦ï¼ç°è´§å¸åºä¸å¨æ¯æèå´åï¼", index)
	}
	return cm.model, nil
}

// Markets è¿åå¯äº¤æçæ°¸ç»­åçº¦ï¼æ 24 å°æ¶æäº¤é¢éåºââæ´»è·çæå¨ä¸æåé¢ã
//
// ç°è´§å¨ç¼å­é¶æ®µå°±è¢«æ»¤æäºï¼è¿éåæ»¤æå·²ä¸æ¶çåçº¦ã
func (a *Adapter) Markets(ctx context.Context) ([]exchange.MarketInfo, error) {
	if err := a.ensureMarkets(ctx); err != nil {
		return nil, err
	}
	a.mu.RLock()
	out := make([]exchange.MarketInfo, 0, len(a.byIndex))
	for _, cm := range a.byIndex {
		if cm.detail.Status != activeStatus {
			continue
		}
		out = append(out, toMarketInfo(cm))
	}
	a.mu.RUnlock()

	sortByVolume(out)
	return out, nil
}

func toMarketInfo(cm cachedMarket) exchange.MarketInfo {
	return exchange.MarketInfo{
		Symbol:           cm.detail.Symbol,
		MarketIndex:      cm.detail.MarketID,
		Type:             cm.detail.MarketType,
		Status:           cm.detail.Status,
		MarkPrice:        cm.detail.MarkPrice.Decimal,
		DailyQuoteVolume: cm.detail.DailyQuoteTokenVolume.Decimal,
		MaxLeverage:      cm.model.MaxLeverage,
	}
}

// sortByVolume æ 24 å°æ¶æäº¤é¢éåºæåï¼æäº¤é¢ç¸åæ¶æç¬¦å·å­å¸åºã
//
// äº¤ææè¿åçé¡ºåºæ¯ä¹±çï¼è 200 å¤ä¸ªå¸åºä¸æåºå¨ä¸æéæ²¡æ³æ¾ã
func sortByVolume(ms []exchange.MarketInfo) {
	sort.Slice(ms, func(i, j int) bool {
		if !ms[i].DailyQuoteVolume.Equal(ms[j].DailyQuoteVolume) {
			return ms[i].DailyQuoteVolume.GreaterThan(ms[j].DailyQuoteVolume)
		}
		return ms[i].Symbol < ms[j].Symbol
	})
}

func (a *Adapter) Market(ctx context.Context, symbol string) (market.Market, error) {
	cm, err := a.lookup(ctx, symbol)
	if err != nil {
		return market.Market{}, err
	}
	return cm.model, nil
}

func (a *Adapter) Ticker(ctx context.Context, symbol string) (exchange.Ticker, error) {
	cm, err := a.lookup(ctx, symbol)
	if err != nil {
		return exchange.Ticker{}, err
	}
	// å¸åºè¯¦æéçè¡æå­æ®µä¼éç¼å­è¿æï¼çå£å¿é¡»å®æ¶æã
	detail, err := a.rest.orderBookDetails(ctx, cm.detail.MarketID)
	if err != nil {
		return exchange.Ticker{}, err
	}
	if len(detail) == 0 {
		return exchange.Ticker{}, fmt.Errorf("lighter: %s æ²¡æè¿åè¡æ", symbol)
	}
	d := detail[0]

	book, err := a.rest.bookTop(ctx, cm.detail.MarketID, 1)
	if err != nil {
		return exchange.Ticker{}, err
	}
	t := exchange.Ticker{
		Symbol: d.Symbol,
		Mark:   d.MarkPrice.Decimal,
		Index:  d.IndexPrice.Decimal,
		Last:   d.LastTradePrice.Decimal,
		Time:   time.Now(),
	}
	if len(book.Bids) > 0 {
		t.Book.Bid = book.Bids[0].Price.Decimal
		t.Book.BidSize = book.Bids[0].RemainingBaseAmount.Decimal
	}
	if len(book.Asks) > 0 {
		t.Book.Ask = book.Asks[0].Price.Decimal
		t.Book.AskSize = book.Asks[0].RemainingBaseAmount.Decimal
	}
	t.Book.Time = t.Time
	return t, nil
}

// Klines 拉取公开 K 线。主网用 /api/v1/candles（旧的 /candlesticks 会 403）。
func (a *Adapter) Klines(ctx context.Context, symbol, interval string, limit int) ([]market.Kline, error) {
	cm, err := a.lookup(ctx, symbol)
	if err != nil {
		return nil, err
	}
	res, dur, ok := normalizeResolution(interval)
	if !ok {
		return nil, fmt.Errorf("lighter: 不支持的 K 线周期 %q", interval)
	}
	if limit <= 0 {
		limit = 72
	}
	if limit > 500 {
		limit = 500
	}
	end := time.Now().UTC()
	start := end.Add(-time.Duration(limit) * dur)
	raw, err := a.rest.candles(ctx, cm.detail.MarketID, res, start, end, limit)
	if err != nil {
		return nil, err
	}
	out := make([]market.Kline, 0, len(raw))
	for _, c := range raw {
		if c.T <= 0 || !c.C.Decimal.IsPositive() {
			continue
		}
		out = append(out, market.Kline{
			OpenTime: candleTime(c.T),
			Open:     c.O.Decimal,
			High:     c.H.Decimal,
			Low:      c.L.Decimal,
			Close:    c.C.Decimal,
			Volume:   c.V.Decimal,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].OpenTime.Before(out[j].OpenTime) })
	return out, nil
}

// --- è´¦æ·ä¸æä» ---

func (a *Adapter) Account(ctx context.Context) (account.Snapshot, error) {
	info, err := a.rest.account(ctx, a.accountIndex)
	if err != nil {
		return account.Snapshot{}, err
	}
	upnl := decimal.Zero
	for _, p := range info.Positions {
		upnl = upnl.Add(p.UnrealizedPnL.Decimal)
	}
	return account.Snapshot{
		Balance:       info.Collateral.Decimal,
		Equity:        info.TotalAssetValue.Decimal,
		Available:     info.AvailableBalance.Decimal,
		MarginUsed:    info.CrossInitialMarginRequirement.Decimal,
		UnrealizedPnL: upnl,
		UpdatedAt:     time.Now(),
	}, nil
}

func (a *Adapter) Position(ctx context.Context, symbol string) (position.Position, error) {
	cm, err := a.lookup(ctx, symbol)
	if err != nil {
		return position.Position{}, err
	}
	info, err := a.rest.account(ctx, a.accountIndex)
	if err != nil {
		return position.Position{}, err
	}
	for _, p := range info.Positions {
		if p.MarketID == cm.detail.MarketID {
			return toPosition(p, cm.detail.MarkPrice.Decimal), nil
		}
	}
	return position.Position{Symbol: cm.detail.Symbol, MarkPrice: cm.detail.MarkPrice.Decimal}, nil
}

// Positions è¿åå¨é¨éç©ºä»ä½ï¼ä¾ CLI ä¸é¡µé¢æ»è§ä½¿ç¨ã
func (a *Adapter) Positions(ctx context.Context) ([]position.Position, error) {
	if err := a.ensureMarkets(ctx); err != nil {
		return nil, err
	}
	info, err := a.rest.account(ctx, a.accountIndex)
	if err != nil {
		return nil, err
	}
	var out []position.Position
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, p := range info.Positions {
		if p.Position.Decimal.IsZero() {
			continue
		}
		mark := decimal.Zero
		if cm, ok := a.byIndex[p.MarketID]; ok {
			mark = cm.detail.MarkPrice.Decimal
		}
		out = append(out, toPosition(p, mark))
	}
	return out, nil
}

func (a *Adapter) OpenOrders(ctx context.Context, symbol string) ([]order.Order, error) {
	cm, err := a.lookup(ctx, symbol)
	if err != nil {
		return nil, err
	}
	auth, err := a.tx.authToken()
	if err != nil {
		return nil, err
	}
	raw, err := a.rest.activeOrders(ctx, a.accountIndex, cm.detail.MarketID, auth)
	if err != nil {
		return nil, err
	}
	out := make([]order.Order, 0, len(raw))
	for _, o := range raw {
		out = append(out, toOrder(o, cm.detail.Symbol))
	}
	return out, nil
}

// --- äº¤æ ---

func (a *Adapter) SetLeverage(ctx context.Context, symbol string, leverage int, mode market.MarginMode) error {
	cm, err := a.tradable(ctx, symbol)
	if err != nil {
		return err
	}
	if leverage < 1 || leverage > cm.model.MaxLeverage {
		return exchange.Classify(exchange.ClassInvalidParam, "set_leverage",
			fmt.Errorf("lighter: %s çæ æå¿é¡»å¨ 1 - %d ä¹é´ï¼æ¶å° %d",
				symbol, cm.model.MaxLeverage, leverage))
	}
	imf := marginFractionTick / leverage
	if min := cm.detail.MinInitialMarginFraction; min > 0 && imf < min {
		imf = min
	}

	_, err = a.tx.send(ctx, "set_leverage", func(ops *ltypes.TransactOpts) (txtypes.TxInfo, error) {
		return a.tx.tx.GetUpdateLeverageTransaction(&ltypes.UpdateLeverageTxReq{
			MarketIndex:           int16(cm.detail.MarketID),
			InitialMarginFraction: uint16(imf),
			MarginMode:            marginModeToUint8(mode),
		}, ops)
	})
	return err
}

// PlaceOrders éç¬ç­¾åæäº¤ã
//
// åç¬å¤±è´¥ä¸ä¼ä¸­æ­åç»­è®¢åï¼ç»ææå¥åé¡ºåºä¸ä¸å¯¹åºï¼è°ç¨æ¹éæ¡æ£æ¥ Errã
// è¿æ ·é¨åæåæ¶ä¸å±è½åç¡®ç¥éåªå ç¬éè¦éè¯ï¼èä¸æ¯æ´æ¹éæ¥ã
func (a *Adapter) PlaceOrders(ctx context.Context, reqs []exchange.PlaceRequest) ([]exchange.PlaceResult, error) {
	results := make([]exchange.PlaceResult, len(reqs))
	for i, req := range reqs {
		results[i] = exchange.PlaceResult{ClientOrderID: req.ClientOrderID}
		hash, err := a.placeOne(ctx, req)
		if err != nil {
			results[i].Err = err
			continue
		}
		results[i].TxHash = hash
	}
	return results, nil
}

func (a *Adapter) placeOne(ctx context.Context, req exchange.PlaceRequest) (string, error) {
	// åªåä»çåå¨ä¸æ¶åçº¦ä¸ä»éæ¾è¡ï¼å¦åä¼æä»ä½éæ­»å¨éé¢ã
	lookup := a.tradable
	if req.ReduceOnly {
		lookup = a.lookup
	}
	cm, err := lookup(ctx, req.Symbol)
	if err != nil {
		return "", err
	}
	m := cm.model

	// ä»¥ä¸é½æ¯æ¬å°å°±è½å¤å®çåæ°é®é¢ï¼ç»ä¸å½ä¸º invalid_paramï¼
	// éè¯ä¸ä¼è®©å®ä»¬åå¥½ï¼ä¸å±åºè¯¥è·³è¿è¿ä¸ç¬å¹¶è®¡å¥å¤±è´¥ã
	invalid := func(err error) (string, error) {
		return "", exchange.Classify(exchange.ClassInvalidParam, "place_order", err)
	}
	if !req.ClientOrderID.Valid() {
		return invalid(fmt.Errorf("lighter: å®¢æ·ç«¯è®¢åå· %d éæ³", req.ClientOrderID))
	}
	price := m.RoundPrice(req.Price, market.RoundNearest)
	qty := m.RoundQty(req.Quantity)
	if err := m.CheckOrder(price, qty); err != nil {
		return invalid(fmt.Errorf("lighter: %s è®¢åä¸æ»¡è¶³å¸åºéå¶: %w", req.Symbol, err))
	}

	priceInt, err := priceToInt(price, m.PriceDecimals)
	if err != nil {
		return invalid(err)
	}
	sizeInt, err := sizeToInt(qty, m.SizeDecimals)
	if err != nil {
		return invalid(err)
	}

	tr := &ltypes.CreateOrderTxReq{
		MarketIndex:      int16(cm.detail.MarketID),
		ClientOrderIndex: int64(req.ClientOrderID),
		BaseAmount:       sizeInt,
		Price:            priceInt,
		IsAsk:            isAsk(req.Side),
		ReduceOnly:       boolToUint8(req.ReduceOnly),
	}

	switch req.Type {
	case order.Market:
		// å¸ä»·åå¨ Lighter ä¸æ¯ãIOC éä»·åãï¼Price æ¯å¯æ¥åçæå·®ä»·æ ¼ã
		// æææå¿é¡»çç©ºï¼å¦åäº¤ææä¼æç»ã
		tr.Type = txtypes.MarketOrder
		tr.TimeInForce = txtypes.ImmediateOrCancel
		tr.OrderExpiry = txtypes.NilOrderExpiry
	case order.Limit:
		tif, err := toTIF(req.TIF)
		if err != nil {
			return invalid(err)
		}
		tr.Type = txtypes.LimitOrder
		tr.TimeInForce = tif
		if tif == txtypes.ImmediateOrCancel {
			tr.OrderExpiry = txtypes.NilOrderExpiry
		} else {
			expire := req.ExpireAt
			if expire.IsZero() {
				expire = time.Now().Add(a.orderExpiry)
			}
			tr.OrderExpiry = expire.UnixMilli()
		}
	default:
		return invalid(fmt.Errorf("lighter: ä¸æ¯æçè®¢åç±»å %s", req.Type))
	}

	return a.tx.send(ctx, "place_order", func(ops *ltypes.TransactOpts) (txtypes.TxInfo, error) {
		return a.tx.tx.GetCreateOrderTransaction(tr, ops)
	})
}

func (a *Adapter) ModifyOrders(ctx context.Context, reqs []exchange.ModifyRequest) ([]exchange.ModifyResult, error) {
	results := make([]exchange.ModifyResult, len(reqs))
	for i, req := range reqs {
		results[i] = exchange.ModifyResult{ClientOrderID: req.ClientOrderID}
		hash, err := a.modifyOne(ctx, req)
		if err != nil {
			results[i].Err = err
			continue
		}
		results[i].TxHash = hash
	}
	return results, nil
}

func (a *Adapter) modifyOne(ctx context.Context, req exchange.ModifyRequest) (string, error) {
	cm, err := a.lookup(ctx, req.Symbol)
	if err != nil {
		return "", err
	}
	m := cm.model

	invalid := func(err error) (string, error) {
		return "", exchange.Classify(exchange.ClassInvalidParam, "modify_order", err)
	}
	if !req.ClientOrderID.Valid() {
		return invalid(fmt.Errorf("lighter: 客户端订单号 %d 非法", req.ClientOrderID))
	}
	price := m.RoundPrice(req.Price, market.RoundNearest)
	qty := m.RoundQty(req.Quantity)
	if err := m.CheckOrder(price, qty); err != nil {
		return invalid(fmt.Errorf("lighter: %s 改单不满足市场限制: %w", req.Symbol, err))
	}
	priceInt, err := priceToInt(price, m.PriceDecimals)
	if err != nil {
		return invalid(err)
	}
	sizeInt, err := sizeToInt(qty, m.SizeDecimals)
	if err != nil {
		return invalid(err)
	}

	return a.tx.send(ctx, "modify_order", func(ops *ltypes.TransactOpts) (txtypes.TxInfo, error) {
		return a.tx.tx.GetModifyOrderTransaction(&ltypes.ModifyOrderTxReq{
			MarketIndex:  int16(cm.detail.MarketID),
			Index:        int64(req.ClientOrderID),
			BaseAmount:   sizeInt,
			Price:        priceInt,
			TriggerPrice: txtypes.NilOrderTriggerPrice,
		}, ops)
	})
}

func (a *Adapter) CancelOrders(ctx context.Context, reqs []exchange.CancelRequest) ([]exchange.CancelResult, error) {
	results := make([]exchange.CancelResult, len(reqs))
	for i, req := range reqs {
		results[i] = exchange.CancelResult{ClientOrderID: req.ClientOrderID}
		cm, err := a.lookup(ctx, req.Symbol)
		if err != nil {
			results[i].Err = err
			continue
		}
		hash, err := a.tx.send(ctx, "cancel_order", func(ops *ltypes.TransactOpts) (txtypes.TxInfo, error) {
			return a.tx.tx.GetCancelOrderTransaction(&ltypes.CancelOrderTxReq{
				MarketIndex: int16(cm.detail.MarketID),
				Index:       int64(req.ClientOrderID),
			}, ops)
		})
		if err != nil {
			results[i].Err = err
			continue
		}
		results[i].TxHash = hash
	}
	return results, nil
}

// CancelAll 只撤销指定交易对上的挂单，不碰其他市场，也不平仓。
//
// Lighter 原生全撤是账户级的，这里改为拉取该市场的活动单再逐笔撤销，
// 避免把同一账户上手动挂的其他币种单一起撤掉。symbol 为空时拒绝执行。
func (a *Adapter) CancelAll(ctx context.Context, symbol string) error {
	if symbol == "" {
		return exchange.Classify(exchange.ClassInvalidParam, "cancel_all",
			fmt.Errorf("lighter: 撤单必须指定交易对，禁止账户级全撤"))
	}
	orders, err := a.OpenOrders(ctx, symbol)
	if err != nil {
		return err
	}
	if len(orders) == 0 {
		return nil
	}
	reqs := make([]exchange.CancelRequest, 0, len(orders))
	for _, o := range orders {
		if !o.State.IsActive() {
			continue
		}
		reqs = append(reqs, exchange.CancelRequest{
			Symbol:        symbol,
			ClientOrderID: o.ClientOrderID,
			ExchangeID:    o.ExchangeID,
		})
	}
	results, err := a.CancelOrders(ctx, reqs)
	if err != nil {
		return err
	}
	for _, r := range results {
		if r.Err != nil {
			return r.Err
		}
	}
	return nil
}

// Tradable æ£æ¥è¯¥äº¤æå¯¹å½åæ¯å¦å¯ä»¥å¼ä»ã
//
// ç°è´§ä¼æ¥ãæªæ¾å°æ°¸ç»­åçº¦ãï¼å·²ä¸æ¶çåçº¦ä¼æ¥ç¶æä¸å¯äº¤æã
// è°ç¨æ¹å¨åå¤ä¸åååé®ä¸æ¬¡ï¼è½ç»åºæ¯ãçå£ä¸ºç©ºãæ´åç¡®çæç¤ºã
func (a *Adapter) Tradable(ctx context.Context, symbol string) error {
	_, err := a.tradable(ctx, symbol)
	return err
}

// AllPerpMarkets è¿åå¨é¨æ°¸ç»­åçº¦ï¼åå«å·²ä¸æ¶çã
//
// åªç»è¯æ­å·¥å·ç¨ï¼æ­£å¸¸éæ©äº¤æå¯¹èµ° Markets()ï¼é£éä¼æ»¤æä¸å¯äº¤æçã
// ä½è´¦æ·ä¸å¦æå¨æä¸ªå·²ä¸æ¶åçº¦éè¿æä»ä½ï¼ææ¥æ¶éè¦è½çå°å®ã
func (a *Adapter) AllPerpMarkets(ctx context.Context) ([]exchange.MarketInfo, error) {
	if err := a.ensureMarkets(ctx); err != nil {
		return nil, err
	}
	a.mu.RLock()
	out := make([]exchange.MarketInfo, 0, len(a.byIndex))
	for _, cm := range a.byIndex {
		out = append(out, toMarketInfo(cm))
	}
	a.mu.RUnlock()

	sortByVolume(out)
	return out, nil
}

// CheckCredentials éªè¯ API key ä¸è´¦æ·éç½®æ¯å¦å¹éï¼ç¨äºé¨ç½²éªæ¶ã
func (a *Adapter) CheckCredentials(ctx context.Context) error {
	if _, err := a.tx.authToken(); err != nil {
		return err
	}
	if _, err := a.rest.nextNonce(ctx, a.accountIndex, a.tx.apiKeyIndex); err != nil {
		return fmt.Errorf("lighter: æ æ³è·å nonceï¼account_index æ api_key_index å¯è½ä¸æ­£ç¡®ï¼: %w", err)
	}
	return nil
}

// TxClient æ´é²åºå±ç­¾åå®¢æ·ç«¯ï¼ä»ä¾è¯æ­å·¥å·ä½¿ç¨ã
func (a *Adapter) TxClient() *lclient.TxClient { return a.tx.tx }
