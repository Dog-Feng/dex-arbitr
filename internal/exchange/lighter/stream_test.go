package lighter

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dex-grid/internal/domain/order"
	"dex-grid/internal/exchange"

	"github.com/coder/websocket"
	"github.com/shopspring/decimal"
)

// 以下报文都是从 Lighter 主网抓下来的真实帧，只改了数值。
const (
	frameConnected = `{"session_id":"s-1","type":"connected"}`

	frameTicker = `{"channel":"ticker:2","last_updated_at":1786770759112163,"nonce":19250278871,
		"ticker":{"s":"SOL","a":{"price":"75.515","size":"52.981"},"b":{"price":"75.511","size":"4.777"},
		"last_updated_at":1786770759112163},"timestamp":1786770759145,"type":"subscribed/ticker"}`

	frameMarketStats = `{"channel":"market_stats:2","market_stats":{"symbol":"SOL","market_id":2,
		"index_price":"75.533","mark_price":"75.499","mid_price":"75.513","last_trade_price":"75.504",
		"current_funding_rate":"0.0008"},"timestamp":1786770759200,"type":"update/market_stats"}`

	// position 在实际推送里是单个对象，而文档写的是数组。
	frameAccountMarket = `{"account":411813,"assets":null,"channel":"account_market:2:411813",
		"funding_history":null,
		"orders":[{"order_index":1125898835217581,"client_order_index":439059755024,"market_index":2,
			"initial_base_amount":"0.150","remaining_base_amount":"0.150","filled_base_amount":"0.000",
			"filled_quote_amount":"0.000","price":"73.991","is_ask":false,"type":"limit",
			"time_in_force":"post_only","reduce_only":false,"status":"open"}],
		"position":{"market_id":2,"symbol":"SOL","initial_margin_fraction":"10.00","sign":1,
			"position":"0.140","avg_entry_price":"75.533","unrealized_pnl":"-0.003",
			"liquidation_price":"0","margin_mode":0},
		"trades":[],"type":"subscribed/account_market"}`

	// position 写成数组的形态也要能解析，免得服务端改回文档描述时全线失败。
	frameAccountMarketArray = `{"account":411813,"channel":"account_market:2:411813","orders":[],
		"position":[{"market_id":2,"symbol":"SOL","sign":-1,"position":"2.000",
			"avg_entry_price":"70.000","unrealized_pnl":"1.5"}],
		"type":"update/account_market"}`
)

func testAdapter() *Adapter {
	return &Adapter{
		log:              slog.New(slog.NewTextHandler(io.Discard, nil)),
		accountIndex:     411813,
		reconnectInitial: 5 * time.Millisecond,
		reconnectMax:     20 * time.Millisecond,
		bySymbol:         map[string]cachedMarket{},
		byIndex: map[int]cachedMarket{
			2: {detail: orderBookDetail{
				orderBook: orderBook{Symbol: "SOL", MarketID: 2, MarketType: perpMarketType, Status: activeStatus},
				MarkPrice: num{decimal.RequireFromString("75.499")},
			}},
		},
		loadedAt: time.Now(),
	}
}

func newTestStream(a *Adapter) *stream {
	return &stream{
		adapter:   a,
		log:       a.log,
		marketID:  2,
		symbol:    "SOL",
		out:       make(chan exchange.StreamEvent, 64),
		authToken: func() (string, error) { return "test-token", nil },
	}
}

// 收集事件直到 channel 关闭或超时。
func drain(t *testing.T, ch <-chan exchange.StreamEvent, want int, timeout time.Duration) []exchange.StreamEvent {
	t.Helper()
	var out []exchange.StreamEvent
	deadline := time.After(timeout)
	for len(out) < want {
		select {
		case ev, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, ev)
		case <-deadline:
			return out
		}
	}
	return out
}

// 盘口来自 ticker、标记价来自 market_stats，两个频道要合成一个完整的行情事件。
func TestStreamMergesTickerAndMarketStats(t *testing.T) {
	s := newTestStream(testAdapter())
	ctx := context.Background()

	if err := s.handle(ctx, []byte(frameTicker)); err != nil {
		t.Fatal(err)
	}
	if err := s.handle(ctx, []byte(frameMarketStats)); err != nil {
		t.Fatal(err)
	}
	close(s.out)

	evs := drain(t, s.out, 2, time.Second)
	if len(evs) != 2 {
		t.Fatalf("收到 %d 个事件，期望 2", len(evs))
	}

	// market_stats 还没到时，标记价用中间价兜底。
	first := evs[0].Ticker
	if first == nil {
		t.Fatal("第一个事件应当是行情")
	}
	if !first.Book.Bid.Equal(decimal.RequireFromString("75.511")) {
		t.Errorf("买一 = %s，期望 75.511", first.Book.Bid)
	}
	if !first.Mark.Equal(first.Book.Mid()) {
		t.Errorf("market_stats 到达前标记价应当回退为中间价，实际 %s", first.Mark)
	}

	// market_stats 到达后，标记价用真实值，盘口保持不变。
	second := evs[1].Ticker
	if !second.Mark.Equal(decimal.RequireFromString("75.499")) {
		t.Errorf("标记价 = %s，期望 75.499", second.Mark)
	}
	if !second.Book.Ask.Equal(decimal.RequireFromString("75.515")) {
		t.Errorf("卖一 = %s，期望合并后仍为 75.515", second.Book.Ask)
	}
	if !second.Index.Equal(decimal.RequireFromString("75.533")) {
		t.Errorf("指数价 = %s，期望 75.533", second.Index)
	}
}

// 盘口还没到之前，只有标记价的 market_stats 不应当产生行情事件。
func TestStreamSuppressesStatsBeforeBook(t *testing.T) {
	s := newTestStream(testAdapter())
	if err := s.handle(context.Background(), []byte(frameMarketStats)); err != nil {
		t.Fatal(err)
	}
	if len(s.out) != 0 {
		t.Fatalf("盘口未就绪时不应发出行情事件，实际发出 %d 个", len(s.out))
	}
}

func TestStreamParsesOrdersAndPosition(t *testing.T) {
	s := newTestStream(testAdapter())
	if err := s.handle(context.Background(), []byte(frameAccountMarket)); err != nil {
		t.Fatal(err)
	}
	close(s.out)

	evs := drain(t, s.out, 2, time.Second)
	if len(evs) != 2 {
		t.Fatalf("收到 %d 个事件，期望订单与仓位各一个", len(evs))
	}

	o := evs[0].Order
	if o == nil {
		t.Fatal("第一个事件应当是订单")
	}
	if o.ClientOrderID != order.ClientOrderID(439059755024) {
		t.Errorf("客户端订单号 = %d", o.ClientOrderID)
	}
	if o.Side != order.Buy {
		t.Errorf("方向 = %s，期望 buy（is_ask 是 false）", o.Side)
	}
	if o.TIF != order.PostOnly {
		t.Errorf("有效期 = %s，期望 post_only", o.TIF)
	}
	if o.State != order.StateOpen {
		t.Errorf("状态 = %s，期望 open", o.State)
	}

	p := evs[1].Position
	if p == nil {
		t.Fatal("第二个事件应当是仓位")
	}
	if !p.Size.Equal(decimal.RequireFromString("0.14")) {
		t.Errorf("仓位 = %s，期望 +0.14（sign=1 表示多头）", p.Size)
	}
	if p.Leverage != 10 {
		t.Errorf("杠杆 = %d，期望 10（initial_margin_fraction 10.00%%）", p.Leverage)
	}
}

// sign 为 -1 时是空头，仓位要带负号。
func TestStreamParsesPositionArrayAndShortSign(t *testing.T) {
	s := newTestStream(testAdapter())
	if err := s.handle(context.Background(), []byte(frameAccountMarketArray)); err != nil {
		t.Fatal(err)
	}
	close(s.out)

	evs := drain(t, s.out, 1, time.Second)
	if len(evs) != 1 || evs[0].Position == nil {
		t.Fatalf("期望解析出一个仓位事件，实际 %+v", evs)
	}
	if got := evs[0].Position.Size; !got.Equal(decimal.RequireFromString("-2")) {
		t.Fatalf("仓位 = %s，期望 -2", got)
	}
}

func TestStreamEmitsAccountTradeWithVenuePnL(t *testing.T) {
	s := newTestStream(testAdapter())
	frame := `{"account":411813,"channel":"account_market:2:411813","orders":[],
		"position":null,
		"trades":[{"trade_id":88,"size":"0.2","price":"75.5","ask_account_id":411813,"bid_account_id":1,
			"ask_client_id":1001,"bid_client_id":0,"ask_account_pnl":"0.04","bid_account_pnl":"0",
			"is_maker_ask":true,"maker_fee":"0.001","taker_fee":"0","timestamp":1700000000000}],
		"type":"update/account_market"}`
	if err := s.handle(context.Background(), []byte(frame)); err != nil {
		t.Fatal(err)
	}
	close(s.out)
	evs := drain(t, s.out, 1, time.Second)
	if len(evs) != 1 || evs[0].Trade == nil {
		t.Fatalf("期望一笔成交事件，实际 %+v", evs)
	}
	tr := evs[0].Trade
	if tr.ID != 88 || tr.Side != order.Sell {
		t.Fatalf("trade = %+v", tr)
	}
	if !tr.RealizedPnL.Equal(decimal.RequireFromString("0.04")) {
		t.Fatalf("realized = %s, want venue ask_account_pnl", tr.RealizedPnL)
	}
}

func TestStreamIgnoresPongAndUnknownChannels(t *testing.T) {
	s := newTestStream(testAdapter())
	ctx := context.Background()
	for _, frame := range []string{
		`{"type":"pong"}`,
		frameConnected,
		`{"type":"update/trade","channel":"trade:2"}`,
	} {
		if err := s.handle(ctx, []byte(frame)); err != nil {
			t.Fatalf("处理 %s 出错: %v", frame, err)
		}
	}
	if len(s.out) != 0 {
		t.Fatalf("这些消息不应产生事件，实际产生 %d 个", len(s.out))
	}
}

// 连接建立后要先发订阅、再发一个 Resync 让上层重新对账；
// 服务端断开后要自动重连，并且每次重连都重新发一次 Resync。
func TestStreamReconnectsAndResyncs(t *testing.T) {
	var (
		mu       sync.Mutex
		sessions [][]string // 每次连接收到的订阅频道
	)
	var connects atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()

		n := connects.Add(1)
		ctx := r.Context()

		// 读取客户端发来的订阅消息
		var channels []string
		for i := 0; i < 3; i++ {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			var msg struct {
				Type    string `json:"type"`
				Channel string `json:"channel"`
				Auth    string `json:"auth"`
			}
			if err := json.Unmarshal(data, &msg); err != nil {
				return
			}
			channels = append(channels, msg.Channel)
			if strings.HasPrefix(msg.Channel, "account_market") && msg.Auth != "test-token" {
				t.Errorf("account_market 订阅缺少 auth，实际 %q", msg.Auth)
			}
		}
		mu.Lock()
		sessions = append(sessions, channels)
		mu.Unlock()

		_ = conn.Write(ctx, websocket.MessageText, []byte(frameTicker))

		// 第一次连接推完就断开，模拟服务端主动关闭；第二次保持住。
		if n == 1 {
			return
		}
		<-ctx.Done()
	}))
	defer srv.Close()

	a := testAdapter()
	a.wsURL = "ws" + strings.TrimPrefix(srv.URL, "http")

	s := newTestStream(a)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go s.run(ctx)

	// 两轮连接：每轮一个 Resync + 一个行情，中间可能夹一个断开错误。
	var resyncs, tickers int
	deadline := time.After(2500 * time.Millisecond)
	for resyncs < 2 {
		select {
		case ev, ok := <-s.out:
			if !ok {
				t.Fatal("事件流意外关闭")
			}
			switch {
			case ev.Resync:
				resyncs++
			case ev.Ticker != nil:
				tickers++
			}
		case <-deadline:
			t.Fatalf("超时：只收到 %d 个 Resync、%d 个行情", resyncs, tickers)
		}
	}

	if tickers == 0 {
		t.Error("重连过程中应当至少收到一个行情事件")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(sessions) < 2 {
		t.Fatalf("服务端只收到 %d 次连接，期望至少 2 次", len(sessions))
	}
	want := []string{"ticker/2", "market_stats/2", "account_market/2/411813"}
	for i, got := range sessions[:2] {
		if len(got) != len(want) {
			t.Errorf("第 %d 次连接订阅了 %v，期望 %v", i+1, got, want)
			continue
		}
		for j := range want {
			if got[j] != want[j] {
				t.Errorf("第 %d 次连接的第 %d 个订阅是 %q，期望 %q", i+1, j, got[j], want[j])
			}
		}
	}
}

// 取不到 auth token 时不能静默降级成只订阅公开频道，
// 否则策略会收不到成交回报却以为一切正常。
func TestStreamFailsWhenAuthTokenUnavailable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		<-r.Context().Done()
	}))
	defer srv.Close()

	a := testAdapter()
	a.wsURL = "ws" + strings.TrimPrefix(srv.URL, "http")
	s := newTestStream(a)
	s.authToken = func() (string, error) { return "", errAuth }

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go s.run(ctx)

	for {
		select {
		case ev, ok := <-s.out:
			if !ok {
				t.Fatal("事件流关闭前应当先报错")
			}
			if ev.Err != nil {
				return // 符合预期
			}
			if ev.Resync {
				t.Fatal("auth 失败时不应当报告连接就绪")
			}
		case <-ctx.Done():
			t.Fatal("超时：没有收到预期的错误事件")
		}
	}
}

var errAuth = errAuthType("生成 auth token 失败")

type errAuthType string

func (e errAuthType) Error() string { return string(e) }
