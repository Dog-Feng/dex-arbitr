package lighter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"dex-grid/internal/domain/market"
	"dex-grid/internal/exchange"
	"dex-grid/internal/exchange/lighttrade"

	"github.com/coder/websocket"
	"github.com/shopspring/decimal"
)

// 事件流参数。
const (
	// keepaliveInterval 必须小于服务端的 2 分钟静默超时。
	keepaliveInterval = 45 * time.Second
	// readTimeout 是两条消息之间的最长间隔。ticker 频道很活跃，
	// 超过这个时间没消息说明连接已经僵死，主动断开重连比干等好。
	readTimeout = 90 * time.Second
	// eventBuffer 是输出 channel 的缓冲。上层是单 goroutine 顺序消费，
	// 留一点缓冲避免行情突发时阻塞读循环。
	eventBuffer = 256
	// maxMessageSize 是单条消息的上限，防止异常响应撑爆内存。
	maxMessageSize = 4 << 20
)

var _ exchange.Streamer = (*Adapter)(nil)

// Subscribe 建立事件流。
//
// 订阅三个频道：
//   - ticker/{market}         最优买卖价。交易所直接推 BBO，不必自己维护订单簿
//   - market_stats/{market}   标记价与指数价。ticker 频道不带标记价，而风控默认按标记价触发
//   - account_market/{m}/{a}  该市场下本账户的订单、仓位与成交（需要 auth token）
//
// 三者合流到一个 channel，保证上层只需要一个 select 分支，也不会出现
// 「先收到成交、后收到挂单确认」这种跨 channel 的乱序。
func (a *Adapter) Subscribe(ctx context.Context, symbol string) (<-chan exchange.StreamEvent, error) {
	return a.subscribe(ctx, symbol, nil)
}

// SubscribeRaw 与 Subscribe 相同，但额外把每一帧原始 JSON 交给 onFrame。
// 只用于诊断工具，正常路径不要用。
func (a *Adapter) SubscribeRaw(ctx context.Context, symbol string, onFrame func([]byte)) (<-chan exchange.StreamEvent, error) {
	return a.subscribe(ctx, symbol, onFrame)
}

func (a *Adapter) subscribe(ctx context.Context, symbol string, onFrame func([]byte)) (<-chan exchange.StreamEvent, error) {
	cm, err := a.lookup(ctx, symbol)
	if err != nil {
		return nil, err
	}
	s := &stream{
		adapter:  a,
		log:      a.log.With("symbol", cm.detail.Symbol),
		marketID: cm.detail.MarketID,
		symbol:   cm.detail.Symbol,
		out:      make(chan exchange.StreamEvent, eventBuffer),
		onFrame:  onFrame,
		// 每次重连都重新取一次令牌：令牌只有 8 小时有效期，
		// 长时间断线后用旧令牌订阅会被服务端拒绝。
		authToken: a.tx.authToken,
	}
	go s.run(ctx)
	return s.out, nil
}

type stream struct {
	adapter  *Adapter
	log      *slog.Logger
	marketID int
	symbol   string
	out      chan exchange.StreamEvent
	onFrame  func([]byte)
	// authToken 每次重连时调用，用于订阅需要鉴权的账户频道。
	authToken func() (string, error)

	// latest 是合并后的行情快照：盘口来自 ticker 频道，标记价来自
	// market_stats 频道，两边各更新自己的字段后再一起发出去。
	// 只被读循环这一个 goroutine 访问，不需要加锁。
	latest exchange.Ticker
}

// run 是重连循环：断开后指数退避重试，直到上下文结束。
func (s *stream) run(ctx context.Context) {
	defer close(s.out)

	backoff := s.adapter.reconnectInitial
	if backoff <= 0 {
		backoff = time.Second // 配置缺省时兜底，避免退避为 0 造成重连风暴
	}
	maxBackoff := s.adapter.reconnectMax
	if maxBackoff < backoff {
		maxBackoff = 30 * time.Second
	}

	for {
		err := s.session(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			s.log.Warn("事件流断开，准备重连", "err", err, "退避", backoff)
			s.emit(ctx, exchange.StreamEvent{Err: err, Time: time.Now()})
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// session 完成一次连接的全生命周期，返回时连接已关闭。
func (s *stream) session(ctx context.Context) error {
	conn, _, err := websocket.Dial(ctx, s.adapter.wsURL, &websocket.DialOptions{
		HTTPClient: s.adapter.httpClient(),
	})
	if err != nil {
		return fmt.Errorf("连接 %s 失败: %w", s.adapter.wsURL, err)
	}
	conn.SetReadLimit(maxMessageSize)
	defer conn.CloseNow()

	// 这个 defer 要注册在 CloseNow 之后，好让它先执行：
	// 先停掉心跳 goroutine，再关连接，避免往已关闭的连接上写。
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := s.subscribe(ctx, conn); err != nil {
		return err
	}

	// 重连后本地状态可能已经过期（断线期间可能有成交），让上层重新对账。
	s.emit(ctx, exchange.StreamEvent{Resync: true, Time: time.Now()})
	s.log.Info("事件流已连接", "market_id", s.marketID)

	go s.keepalive(ctx, conn)

	for {
		readCtx, cancelRead := context.WithTimeout(ctx, readTimeout)
		_, data, err := conn.Read(readCtx)
		cancelRead()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("读取消息失败: %w", err)
		}
		if s.onFrame != nil {
			s.onFrame(data)
		}
		if err := s.handle(ctx, data); err != nil {
			s.log.Warn("处理消息失败", "err", err)
		}
	}
}

func (s *stream) subscribe(ctx context.Context, conn *websocket.Conn) error {
	auth, err := s.authToken()
	if err != nil {
		return err
	}
	subs := []map[string]any{
		{"type": "subscribe", "channel": fmt.Sprintf("ticker/%d", s.marketID)},
		{"type": "subscribe", "channel": fmt.Sprintf("market_stats/%d", s.marketID)},
		{"type": "subscribe", "auth": auth,
			"channel": fmt.Sprintf("account_market/%d/%d", s.marketID, s.adapter.accountIndex)},
	}
	for _, sub := range subs {
		payload, err := json.Marshal(sub)
		if err != nil {
			return err
		}
		if err := conn.Write(ctx, websocket.MessageText, payload); err != nil {
			return fmt.Errorf("发送订阅失败: %w", err)
		}
	}
	return nil
}

// keepalive 定期发心跳。服务端要求客户端至少每 2 分钟发一帧，
// 否则会主动断开连接。
func (s *stream) keepalive(ctx context.Context, conn *websocket.Conn) {
	t := time.NewTicker(keepaliveInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"ping"}`)); err != nil {
				return // 读循环会感知到同一个错误并触发重连
			}
		}
	}
}

func (s *stream) emit(ctx context.Context, ev exchange.StreamEvent) {
	if ev.Time.IsZero() {
		ev.Time = time.Now()
	}
	select {
	case s.out <- ev:
	case <-ctx.Done():
	}
}

// --- 消息解析 ---

// wsEnvelope 是所有推送消息的公共外层。
type wsEnvelope struct {
	Type    string `json:"type"`
	Channel string `json:"channel"`
	// 服务端拒绝订阅时会带上错误信息。
	Message string `json:"message"`
	Code    int    `json:"code"`
}

type wsTickerMsg struct {
	Ticker struct {
		Symbol string `json:"s"`
		Ask    struct {
			Price num `json:"price"`
			Size  num `json:"size"`
		} `json:"a"`
		Bid struct {
			Price num `json:"price"`
			Size  num `json:"size"`
		} `json:"b"`
		LastUpdatedAt int64 `json:"last_updated_at"`
	} `json:"ticker"`
	Timestamp int64 `json:"timestamp"`
}

type wsMarketStatsMsg struct {
	Stats struct {
		Symbol         string `json:"symbol"`
		MarketID       int    `json:"market_id"`
		MarkPrice      num    `json:"mark_price"`
		IndexPrice     num    `json:"index_price"`
		LastTradePrice num    `json:"last_trade_price"`
		FundingRate    num    `json:"current_funding_rate"`
	} `json:"market_stats"`
	Timestamp int64 `json:"timestamp"`
}

type wsAccountMarketMsg struct {
	Account int64                 `json:"account"`
	Orders  []apiOrder            `json:"orders"`
	Trades  []lighttrade.APITrade `json:"trades"`
	// 文档把 position 写成数组，实际推送的是单个对象。
	// 用 RawMessage 兜住两种形态，免得哪天服务端改回数组就全线解析失败。
	Position json.RawMessage `json:"position"`
}

func (m wsAccountMarketMsg) positions() ([]accountPosition, error) {
	raw := m.Position
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var one accountPosition
	if err := json.Unmarshal(raw, &one); err == nil {
		return []accountPosition{one}, nil
	}
	var many []accountPosition
	if err := json.Unmarshal(raw, &many); err != nil {
		return nil, fmt.Errorf("position 字段既不是对象也不是数组: %w", err)
	}
	return many, nil
}

func (s *stream) handle(ctx context.Context, data []byte) error {
	var env wsEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return fmt.Errorf("无法解析消息外层: %w", err)
	}

	switch {
	case env.Type == "pong" || env.Type == "connected":
		return nil

	case env.Type == "error":
		// 订阅被拒（比如 auth 过期）不该被当成普通日志吞掉，
		// 上抛让重连逻辑处理——重连会重新生成 auth token。
		s.emit(ctx, exchange.StreamEvent{
			Err: fmt.Errorf("lighter 事件流返回错误: %s (code=%d)", env.Message, env.Code),
		})
		return nil

	case env.Type == "update/ticker" || env.Type == "subscribed/ticker":
		return s.handleTicker(ctx, data)

	case env.Type == "update/market_stats" || env.Type == "subscribed/market_stats":
		return s.handleMarketStats(ctx, data)

	case env.Type == "update/account_market" || env.Type == "subscribed/account_market":
		return s.handleAccountMarket(ctx, data)

	default:
		return nil // 未订阅或未知的频道，忽略
	}
}

func (s *stream) handleTicker(ctx context.Context, data []byte) error {
	var msg wsTickerMsg
	if err := json.Unmarshal(data, &msg); err != nil {
		return fmt.Errorf("无法解析 ticker: %w", err)
	}
	t := msg.Ticker
	if !t.Bid.Price.IsPositive() && !t.Ask.Price.IsPositive() {
		return nil
	}

	ts := time.Now()
	if msg.Timestamp > 0 {
		ts = time.UnixMilli(msg.Timestamp)
	}
	s.latest.Symbol = s.symbol
	s.latest.Book = market.BookTicker{
		Bid:     t.Bid.Price.Decimal,
		Ask:     t.Ask.Price.Decimal,
		BidSize: t.Bid.Size.Decimal,
		AskSize: t.Ask.Size.Decimal,
		Time:    ts,
	}
	s.emitTicker(ctx, ts)
	return nil
}

func (s *stream) handleMarketStats(ctx context.Context, data []byte) error {
	var msg wsMarketStatsMsg
	if err := json.Unmarshal(data, &msg); err != nil {
		return fmt.Errorf("无法解析 market_stats: %w", err)
	}
	st := msg.Stats
	if st.MarketID != s.marketID {
		return nil
	}

	ts := time.Now()
	if msg.Timestamp > 0 {
		ts = time.UnixMilli(msg.Timestamp)
	}
	s.latest.Symbol = s.symbol
	s.latest.Mark = st.MarkPrice.Decimal
	s.latest.Index = st.IndexPrice.Decimal
	s.latest.Last = st.LastTradePrice.Decimal

	// 盘口还没到之前不发：只有标记价的行情事件对策略没用，
	// 反而会让还没拿到盘口的 post-only 检查误判。
	if !s.latest.Book.Valid() {
		return nil
	}
	s.emitTicker(ctx, ts)
	return nil
}

// emitTicker 发出合并后的行情快照。
//
// 标记价来自 market_stats，盘口来自 ticker，两个频道的到达顺序不确定，
// 因此这里把最新已知值合到一起再发，保证每个事件都是完整的。
func (s *stream) emitTicker(ctx context.Context, ts time.Time) {
	snapshot := s.latest
	snapshot.Time = ts
	if !snapshot.Mark.IsPositive() {
		// market_stats 还没到，先用中间价顶上。风控默认按标记价触发，
		// 这段时间的触发判定会略有偏差，但总比没有价格好。
		snapshot.Mark = snapshot.Book.Mid()
	}
	s.emit(ctx, exchange.StreamEvent{Ticker: &snapshot, Time: ts})
}

func (s *stream) handleAccountMarket(ctx context.Context, data []byte) error {
	var msg wsAccountMarketMsg
	if err := json.Unmarshal(data, &msg); err != nil {
		return fmt.Errorf("无法解析 account_market: %w", err)
	}

	for i := range msg.Orders {
		o := toOrder(msg.Orders[i], s.symbol)
		s.emit(ctx, exchange.StreamEvent{Order: &o})
	}

	for i := range msg.Trades {
		fill, ok := lighttrade.FillForAccount(msg.Trades[i], s.adapter.accountIndex)
		if !ok {
			continue
		}
		fill.Symbol = s.symbol
		cp := fill
		s.emit(ctx, exchange.StreamEvent{Trade: &cp, Time: fill.Time})
	}

	positions, err := msg.positions()
	if err != nil {
		return fmt.Errorf("无法解析 account_market 的仓位: %w", err)
	}
	for _, p := range positions {
		if p.MarketID != s.marketID {
			continue
		}
		pos := toPosition(p, s.markPrice())
		s.emit(ctx, exchange.StreamEvent{Position: &pos})
	}
	return nil
}

// markPrice 从市场缓存里取标记价。
// 缓存最长 10 分钟，用于仓位展示够用；风控触发另有实时来源。
func (s *stream) markPrice() decimal.Decimal {
	s.adapter.mu.RLock()
	defer s.adapter.mu.RUnlock()
	if cm, ok := s.adapter.byIndex[s.marketID]; ok {
		return cm.detail.MarkPrice.Decimal
	}
	return decimal.Zero
}
