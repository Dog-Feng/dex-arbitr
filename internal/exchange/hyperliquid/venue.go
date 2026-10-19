package hyperliquid

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
)

// preset is a named Hyperliquid deployment. HIP-3 builder dexs share the
// standard API and differ only by the dex name carried in info requests.
type preset struct {
	dex string
}

var presets = map[string]preset{
	"hyperliquid": {dex: ""}, // validator-operated perps (the first perp dex)
	"entropy":     {dex: "io"},
}

func init() {
	for kind := range presets {
		k := kind
		exchange.Register(k, func(name string, opts map[string]string) (exchange.Venue, error) {
			return New(name, k, opts)
		})
	}
}

// Config of one Hyperliquid venue instance.
type Config struct {
	Name        string
	Dex         string // empty = first (validator) perp dex
	InfoURL     string
	ExchangeURL string
	WSURL       string
	Account     string // main account address; required to trade or read positions
	AgentKey    string // API-wallet (agent) private key; empty = read-only
	BookDepth   int
}

// Venue is the Hyperliquid adapter, including HIP-3 dexs such as Entropy.
type Venue struct {
	cfg    Config
	signer *signer
	rest   *restClient
	ws     *wsClient

	// assets maps coin name (as returned by meta, e.g. "io:SNDK") to the
	// numeric asset id used in signed orders.
	assetsMu sync.RWMutex
	assets   map[string]int

	booksMu sync.Mutex
	books   map[int]*bookState

	ordersMu    sync.Mutex
	orderSubs   []chan domain.OrderUpdate
	ordersOnce  sync.Once
	orderLookup map[int64]int64 // oid -> client id, filled from submit acks and updates
	orderFinal  map[int64]domain.OrderStatus
	fillAgg     map[int64]fillAgg

	lastNonce atomic.Int64
	sendMu    chan struct{}
	closed    atomic.Bool
	closeDone chan struct{}
}

// fillAgg accumulates per-order fill quantity and quote.
type fillAgg struct {
	qty   float64
	quote float64
}

// New builds the venue from a preset kind plus option overrides.
// Recognized opts: dex, info_url, exchange_url, ws_url, account, private_key,
// book_depth.
func New(name, kind string, opts map[string]string) (*Venue, error) {
	p, ok := presets[kind]
	if !ok {
		return nil, fmt.Errorf("hyperliquid: unknown preset %q", kind)
	}
	cfg := Config{
		Name: name, Dex: p.dex,
		InfoURL: defaultInfoURL, ExchangeURL: defaultExchangeURL, WSURL: defaultWSURL,
		BookDepth: 20,
	}
	if v := strings.TrimSpace(opts["dex"]); v != "" {
		cfg.Dex = v
	}
	if v := strings.TrimSpace(opts["info_url"]); v != "" {
		cfg.InfoURL = strings.TrimRight(v, "/")
	}
	if v := strings.TrimSpace(opts["exchange_url"]); v != "" {
		cfg.ExchangeURL = strings.TrimRight(v, "/")
	}
	if v := strings.TrimSpace(opts["ws_url"]); v != "" {
		cfg.WSURL = v
	}
	cfg.Account = strings.ToLower(strings.TrimSpace(opts["account"]))
	cfg.AgentKey = strings.TrimSpace(opts["private_key"])
	if v := strings.TrimSpace(opts["book_depth"]); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("book_depth must be positive")
		}
		cfg.BookDepth = n
	}
	return NewWithConfig(cfg)
}

// NewWithConfig builds a venue from an explicit config.
func NewWithConfig(cfg Config) (*Venue, error) {
	if strings.TrimSpace(cfg.Name) == "" {
		return nil, fmt.Errorf("venue name must not be empty")
	}
	if cfg.BookDepth <= 0 {
		return nil, fmt.Errorf("book_depth must be positive")
	}
	if !validURL(cfg.InfoURL, false) {
		return nil, fmt.Errorf("invalid info URL")
	}
	if !validURL(cfg.ExchangeURL, false) {
		return nil, fmt.Errorf("invalid exchange URL")
	}
	if !validURL(cfg.WSURL, true) {
		return nil, fmt.Errorf("invalid websocket URL")
	}
	cfg.Account = strings.ToLower(strings.TrimSpace(cfg.Account))
	cfg.AgentKey = strings.TrimSpace(cfg.AgentKey)
	if cfg.Account != "" {
		if len(cfg.Account) == 40 {
			cfg.Account = "0x" + cfg.Account
		}
		if len(cfg.Account) != 42 || !strings.HasPrefix(cfg.Account, "0x") {
			return nil, fmt.Errorf("account must be a 42-character address")
		}
		for _, ch := range cfg.Account[2:] {
			if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
				return nil, fmt.Errorf("account must be a hexadecimal address")
			}
		}
	}
	v := &Venue{
		cfg: cfg, assets: map[string]int{}, books: map[int]*bookState{},
		orderLookup: map[int64]int64{}, orderFinal: map[int64]domain.OrderStatus{}, fillAgg: map[int64]fillAgg{},
		sendMu: make(chan struct{}, 1), closeDone: make(chan struct{}),
	}
	if cfg.AgentKey != "" {
		s, err := newSigner(cfg.AgentKey)
		if err != nil {
			return nil, err
		}
		v.signer = s
	}
	v.rest = newRestClient(cfg.InfoURL, cfg.ExchangeURL)
	v.ws = newWSClient(cfg.Name, cfg.WSURL, v.onWSMessage)
	v.ws.onReset = v.resetBooks
	return v, nil
}

func (v *Venue) Name() string   { return v.cfg.Name }
func (v *Venue) CanTrade() bool { return v.signer != nil && v.cfg.Account != "" }

// Close stops websocket loops and consumer channels.
func (v *Venue) Close() error {
	if !v.closed.CompareAndSwap(false, true) {
		<-v.closeDone
		return nil
	}
	defer close(v.closeDone)
	v.ws.close()
	v.booksMu.Lock()
	for _, bs := range v.books {
		for _, ch := range bs.subs {
			close(ch)
		}
		bs.subs = nil
	}
	v.booksMu.Unlock()
	v.ordersMu.Lock()
	for _, ch := range v.orderSubs {
		close(ch)
	}
	v.orderSubs = nil
	v.ordersMu.Unlock()
	return nil
}

// dexIndex resolves the configured dex name to its numeric perp dex index.
func (r *restClient) dexIndex(ctx context.Context, dex string) (int, error) {
	if dex == "" {
		return 0, nil
	}
	raw, err := r.postInfo(ctx, map[string]any{"type": "perpDexs"})
	if err != nil {
		return 0, err
	}
	var out []struct {
		Name *string `json:"name"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return 0, fmt.Errorf("perpDexs: invalid JSON: %w", err)
	}
	for i, d := range out {
		if d.Name != nil && *d.Name == dex {
			return i, nil
		}
	}
	return 0, fmt.Errorf("perp dex %q not found", dex)
}

// Instruments returns active perpetual markets of the configured dex.
func (v *Venue) Instruments(ctx context.Context) ([]domain.Instrument, error) {
	dexIndex, err := v.rest.dexIndex(ctx, v.cfg.Dex)
	if err != nil {
		return nil, err
	}
	meta, err := v.rest.meta(ctx, v.cfg.Dex)
	if err != nil {
		return nil, err
	}
	out := make([]domain.Instrument, 0, len(meta.Universe))
	for i, m := range meta.Universe {
		if m.IsDelisted {
			continue
		}
		if m.SzDecimals < 0 || m.SzDecimals > 18 {
			return nil, fmt.Errorf("asset %s: invalid size decimals", m.Name)
		}
		sym := strings.TrimPrefix(m.Name, v.cfg.Dex+":")
		if sym == "" {
			return nil, fmt.Errorf("asset %s: empty symbol", m.Name)
		}
		// Perp prices carry at most 5 significant figures and 6 - szDecimals
		// decimal places (clamped at 0, which still leaves the 5-sig-fig rule
		// to the wire formatter).
		priceDecimals := 6 - m.SzDecimals
		if priceDecimals < 0 {
			priceDecimals = 0
		}
		// Validator perps use the plain meta index as the asset id. Builder
		// perps use 100000 + perp_dex_index*10000 + index_in_meta; perpDexs
		// reports "io" at index 10, so io:SNDK (index 2) is asset 200002.
		id := i
		if v.cfg.Dex != "" {
			id = 100000 + dexIndex*10000 + i
		}
		v.assetsMu.Lock()
		v.assets[m.Name] = id
		v.assetsMu.Unlock()
		taker, maker := baseFees(m, v.cfg.Dex != "")
		out = append(out, domain.Instrument{
			Venue: v.cfg.Name, Symbol: sym, MarketID: id,
			SizeDecimals: m.SzDecimals, PriceDecimals: priceDecimals,
			MinBase: 0, MinQuote: 0, TakerFee: taker, MakerFee: maker,
		})
	}
	return out, nil
}

// baseFees returns per-fill fee rates at the base volume tier. Growth-mode
// HIP-3 assets with deployerFeeScale 1.0 trade at 9 bps taker / 3 bps maker;
// a better account tier only lowers the realized fee, so this estimate is
// conservative for decision thresholds.
func baseFees(m metaItem, isHIP3 bool) (taker, maker float64) {
	if !isHIP3 {
		return 0.00045, 0.00015
	}
	growth := m.GrowthMode == "enabled"
	scale := 1.0
	if f, err := strconv.ParseFloat(m.DeployerFeeScale, 64); err == nil && f > 0 {
		scale = f
	}
	mult := scale
	if scale < 1 {
		mult = scale + 1
	} else {
		mult = scale * 2
	}
	g := 1.0
	if growth {
		g = 0.1
	}
	return 0.00045 * mult * g, 0.00015 * mult * g
}

// coinFor maps a numeric asset id back to its meta coin name.
func (v *Venue) coinFor(id int) (string, bool) {
	v.assetsMu.RLock()
	defer v.assetsMu.RUnlock()
	for coin, asset := range v.assets {
		if asset == id {
			return coin, true
		}
	}
	return "", false
}

// SubscribeBook streams full top-N snapshots (Hyperliquid pushes whole books,
// not deltas; latest wins and slow readers drop).
func (v *Venue) SubscribeBook(ctx context.Context, inst domain.Instrument) (<-chan domain.Book, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if inst.MarketID <= 0 {
		return nil, fmt.Errorf("invalid market id %d", inst.MarketID)
	}
	coin, ok := v.coinFor(inst.MarketID)
	if !ok {
		return nil, fmt.Errorf("market %d not in %s universe", inst.MarketID, v.cfg.Name)
	}
	ch := make(chan domain.Book, 1)
	v.booksMu.Lock()
	if v.closed.Load() {
		v.booksMu.Unlock()
		return nil, fmt.Errorf("venue closed")
	}
	bs, ok := v.books[inst.MarketID]
	if !ok {
		bs = &bookState{marketID: inst.MarketID, depth: v.cfg.BookDepth}
		v.books[inst.MarketID] = bs
	}
	bs.subs = append(bs.subs, ch)
	if bs.synced {
		ch <- bs.snapshot(v.cfg.Name)
	}
	v.booksMu.Unlock()
	if !ok {
		// fast:true delivers 5 levels per side per block (~0.5s) instead of
		// the throttled 20-level feed (~5s). The bbo feed refreshes the top
		// level even faster (~0.4s) and is merged into the same book state.
		v.ws.subscribe("l2Book/"+coin, map[string]any{"type": "l2Book", "coin": coin, "fast": true})
		v.ws.subscribe("bbo/"+coin, map[string]any{"type": "bbo", "coin": coin})
	}
	go func() {
		select {
		case <-ctx.Done():
		case <-v.ws.done:
		}
		v.booksMu.Lock()
		defer v.booksMu.Unlock()
		for i, c := range bs.subs {
			if c == ch {
				bs.subs = append(bs.subs[:i], bs.subs[i+1:]...)
				close(ch)
				break
			}
		}
	}()
	return ch, nil
}

// bookState holds the latest full book for one asset.
type bookState struct {
	marketID int
	depth    int
	bids     []domain.Level
	asks     []domain.Level
	seq      int64
	synced   bool
	subs     []chan domain.Book
	updated  time.Time
}

func (bs *bookState) snapshot(venue string) domain.Book {
	bids := bs.bids
	if bs.depth > 0 && len(bids) > bs.depth {
		bids = bids[:bs.depth]
	}
	asks := bs.asks
	if bs.depth > 0 && len(asks) > bs.depth {
		asks = asks[:bs.depth]
	}
	return domain.Book{Venue: venue, MarketID: bs.marketID, Bids: bids, Asks: asks, Seq: bs.seq, Ts: bs.updated}
}

func (v *Venue) publishBook(bs *bookState, b domain.Book) {
	for _, ch := range bs.subs {
		select {
		case <-ch:
		default:
		}
		select {
		case ch <- b:
		default:
		}
	}
}

func (v *Venue) invalidateBook(bs *bookState) {
	bs.synced, bs.bids, bs.asks = false, nil, nil
	bs.seq = 0
	bs.updated = time.Time{}
	v.publishBook(bs, domain.Book{Venue: v.cfg.Name, MarketID: bs.marketID, Ts: time.Now()})
}

type wsBookMsg struct {
	Channel string `json:"channel"`
	Data    struct {
		Coin   string      `json:"coin"`
		Time   int64       `json:"time"`
		Levels [][]wsLevel `json:"levels"`
	} `json:"data"`
}

type wsBboMsg struct {
	Channel string `json:"channel"`
	Data    struct {
		Coin string `json:"coin"`
		Time int64  `json:"time"`
		Bbo  []struct {
			Px string `json:"px"`
			Sz string `json:"sz"`
		} `json:"bbo"`
	} `json:"data"`
}

type wsLevel struct {
	Px string `json:"px"`
	Sz string `json:"sz"`
}

func (v *Venue) onBook(raw []byte) {
	var m wsBookMsg
	if err := json.Unmarshal(raw, &m); err != nil || m.Data.Coin == "" || len(m.Data.Levels) != 2 {
		return
	}
	bids, err := parseLevels(m.Data.Levels[0])
	if err != nil {
		return
	}
	asks, err := parseLevels(m.Data.Levels[1])
	if err != nil {
		return
	}
	v.assetsMu.RLock()
	id, ok := v.assets[m.Data.Coin]
	v.assetsMu.RUnlock()
	if !ok {
		return
	}
	v.booksMu.Lock()
	defer v.booksMu.Unlock()
	bs := v.books[id]
	if bs == nil {
		return
	}
	bs.bids, bs.asks = bids, asks
	bs.seq = m.Data.Time
	bs.updated = time.Now()
	bs.synced = true
	v.publishBook(bs, bs.snapshot(v.cfg.Name))
}

// onBbo merges the top-of-book feed into the book state. The bbo channel
// refreshes the best levels between full-book snapshots; deeper levels keep
// their age until the next l2Book push.
func (v *Venue) onBbo(raw []byte) {
	var m wsBboMsg
	if err := json.Unmarshal(raw, &m); err != nil || m.Data.Coin == "" || len(m.Data.Bbo) != 2 {
		return
	}
	v.assetsMu.RLock()
	id, ok := v.assets[m.Data.Coin]
	v.assetsMu.RUnlock()
	if !ok {
		return
	}
	bid, bidErr := parseBboLevel(m.Data.Bbo[0])
	ask, askErr := parseBboLevel(m.Data.Bbo[1])
	if bidErr != nil && askErr != nil {
		return
	}
	v.booksMu.Lock()
	defer v.booksMu.Unlock()
	bs := v.books[id]
	if bs == nil || !bs.synced {
		return // wait for the first full snapshot
	}
	topBid, haveBid := domain.Level{}, false
	if len(bs.bids) > 0 {
		topBid, haveBid = bs.bids[0], true
	}
	if bidErr == nil {
		topBid, haveBid = bid, true
	}
	topAsk, haveAsk := domain.Level{}, false
	if len(bs.asks) > 0 {
		topAsk, haveAsk = bs.asks[0], true
	}
	if askErr == nil {
		topAsk, haveAsk = ask, true
	}
	if haveBid && haveAsk && topBid.Price >= topAsk.Price {
		return // never publish a crossed book
	}
	if bidErr == nil {
		if len(bs.bids) == 0 {
			bs.bids = []domain.Level{topBid}
		} else {
			bs.bids[0] = topBid
		}
	}
	if askErr == nil {
		if len(bs.asks) == 0 {
			bs.asks = []domain.Level{topAsk}
		} else {
			bs.asks[0] = topAsk
		}
	}
	bs.seq = m.Data.Time
	bs.updated = time.Now()
	v.publishBook(bs, bs.snapshot(v.cfg.Name))
}

func parseBboLevel(l struct {
	Px string `json:"px"`
	Sz string `json:"sz"`
}) (domain.Level, error) {
	p, err1 := strconv.ParseFloat(l.Px, 64)
	s, err2 := strconv.ParseFloat(l.Sz, 64)
	if err1 != nil || err2 != nil || math.IsNaN(p) || math.IsInf(p, 0) || p <= 0 || math.IsNaN(s) || math.IsInf(s, 0) || s < 0 {
		return domain.Level{}, fmt.Errorf("invalid bbo level")
	}
	return domain.Level{Price: p, Size: s}, nil
}

func parseLevels(levels []wsLevel) ([]domain.Level, error) {
	out := make([]domain.Level, 0, len(levels))
	for _, l := range levels {
		p, err := strconv.ParseFloat(l.Px, 64)
		if err != nil || math.IsNaN(p) || math.IsInf(p, 0) || p <= 0 {
			return nil, fmt.Errorf("invalid book price")
		}
		s, err := strconv.ParseFloat(l.Sz, 64)
		if err != nil || math.IsNaN(s) || math.IsInf(s, 0) || s < 0 {
			return nil, fmt.Errorf("invalid book size")
		}
		out = append(out, domain.Level{Price: p, Size: s})
	}
	return out, nil
}

func (v *Venue) onWSMessage(channel string, raw []byte) {
	switch channel {
	case "l2Book":
		v.onBook(raw)
	case "bbo":
		v.onBbo(raw)
	case "orderUpdates":
		v.onOrderUpdate(raw)
	case "userFills":
		v.onUserFills(raw)
	default:
		slog.Debug("ws message ignored", "venue", v.cfg.Name, "channel", channel)
	}
}

// resetBooks drops book snapshots after a reconnect so callers see an
// explicit invalid book instead of a stale one while the first snapshot
// arrives.
func (v *Venue) resetBooks() {
	v.booksMu.Lock()
	defer v.booksMu.Unlock()
	for _, bs := range v.books {
		v.invalidateBook(bs)
	}
}

// wsOrderUpdate is one element of the orderUpdates channel data.
type wsOrderUpdate struct {
	Order struct {
		Coin  string `json:"coin"`
		Sz    string `json:"sz"`
		Oid   int64  `json:"oid"`
		Cloid string `json:"cloid"`
	} `json:"order"`
	Status          string `json:"status"`
	StatusTimestamp int64  `json:"statusTimestamp"`
}

type wsFillsMsg struct {
	Data struct {
		User  string `json:"user"`
		Fills []struct {
			Coin string `json:"coin"`
			Px   string `json:"px"`
			Sz   string `json:"sz"`
			Oid  int64  `json:"oid"`
			Time int64  `json:"time"`
		} `json:"fills"`
	} `json:"data"`
}

func (v *Venue) onOrderUpdate(raw []byte) {
	var m struct {
		Data []wsOrderUpdate `json:"data"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return
	}
	for _, u := range m.Data {
		cid, ok := cloidToID(u.Order.Cloid)
		if !ok {
			continue // not one of our orders
		}
		v.ordersMu.Lock()
		v.orderLookup[u.Order.Oid] = cid
		agg := v.fillAgg[u.Order.Oid]
		status := mapOrderStatus(u.Status)
		if status.Terminal() {
			v.orderFinal[u.Order.Oid] = status
		}
		// Only publish a terminal report that already carries its fill facts.
		// A filled or partially-filled canceled order whose fills have not
		// been observed yet must resolve through the fill backfill; a
		// zero-size terminal report would desync the executor's position
		// accounting.
		publishNow := status == domain.StatusRejected ||
			(status == domain.StatusCanceled && agg.qty > 0)
		if publishNow {
			v.publishOrder(domain.OrderUpdate{
				Venue: v.cfg.Name, ClientID: cid, OrderIndex: u.Order.Oid,
				Status: status, FilledQty: agg.qty, FilledQuote: agg.quote,
				Ts: time.Now(),
			})
		}
		v.ordersMu.Unlock()
		if (status == domain.StatusFilled || status == domain.StatusCanceled) && agg.qty == 0 {
			// Late fill aggregation: fetch fills once for the terminal oid.
			go v.backfillFills(u.Order.Oid, cid, u.StatusTimestamp, status)
		}
	}
}

func mapOrderStatus(s string) domain.OrderStatus {
	switch s {
	case "filled":
		return domain.StatusFilled
	case "canceled", "marginCanceled", "openInterestCapCanceled", "selfTradeCanceled",
		"reduceOnlyCanceled", "siblingFilledCanceled", "delistedCanceled", "liquidatedCanceled",
		"scheduledCancel", "vaultWithdrawalCanceled":
		return domain.StatusCanceled
	case "rejected", "tickRejected", "minTradeNtlRejected", "perpMarginRejected",
		"reduceOnlyRejected", "badAloPxRejected", "iocCancelRejected", "badTriggerPxRejected",
		"marketOrderNoLiquidityRejected", "positionIncreaseAtOpenInterestCapRejected",
		"positionFlipAtOpenInterestCapRejected", "tooAggressiveAtOpenInterestCapRejected",
		"openInterestIncreaseRejected", "insufficientSpotBalanceRejected", "oracleRejected",
		"perpMaxPositionRejected":
		return domain.StatusRejected
	case "open", "triggered":
		return domain.StatusOpen
	}
	return domain.StatusUnknown
}

func (v *Venue) onUserFills(raw []byte) {
	var m wsFillsMsg
	if err := json.Unmarshal(raw, &m); err != nil {
		return
	}
	v.ordersMu.Lock()
	defer v.ordersMu.Unlock()
	for _, f := range m.Data.Fills {
		cid, known := v.orderLookup[f.Oid]
		if !known {
			continue
		}
		sz, err1 := strconv.ParseFloat(f.Sz, 64)
		px, err2 := strconv.ParseFloat(f.Px, 64)
		if err1 != nil || err2 != nil || sz <= 0 || px <= 0 || math.IsNaN(sz) || math.IsNaN(px) {
			continue
		}
		agg := v.fillAgg[f.Oid]
		agg.qty += sz
		agg.quote += sz * px
		v.fillAgg[f.Oid] = agg
		status := v.orderFinal[f.Oid]
		if !status.Terminal() {
			status = domain.StatusUnknown
		}
		v.publishOrder(domain.OrderUpdate{
			Venue: v.cfg.Name, ClientID: cid, OrderIndex: f.Oid,
			Status: status, FilledQty: agg.qty, FilledQuote: agg.quote,
			Ts: time.Now(),
		})
	}
}

func (v *Venue) publishOrder(u domain.OrderUpdate) {
	for _, c := range v.orderSubs {
		select {
		case c <- u:
		default:
			slog.Warn("order update dropped (slow consumer)", "venue", v.cfg.Name, "cid", u.ClientID)
		}
	}
}

// backfillFills fetches fills for a terminal order whose fills have not been
// observed yet, then publishes the terminal update with complete fill facts.
func (v *Venue) backfillFills(oid, cid int64, statusTs int64, status domain.OrderStatus) {
	if v.cfg.Account == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	end := time.Now().UnixMilli() + 2000
	fills, err := v.rest.fillsByTime(ctx, v.cfg.Account, statusTs-1000, end)
	if err != nil {
		slog.Warn("fill backfill failed", "venue", v.cfg.Name, "oid", oid, "err", err)
		return
	}
	v.ordersMu.Lock()
	defer v.ordersMu.Unlock()
	var qty, quote float64
	for _, f := range fills {
		if f.Oid.String() != strconv.FormatInt(oid, 10) {
			continue
		}
		sz, err1 := number(f.Sz)
		px, err2 := number(f.Px)
		if err1 != nil || err2 != nil {
			continue
		}
		qty += sz
		quote += sz * px
	}
	if qty <= 0 {
		// A canceled order may genuinely have no fills.
		if status == domain.StatusCanceled {
			v.publishOrder(domain.OrderUpdate{
				Venue: v.cfg.Name, ClientID: cid, OrderIndex: oid,
				Status: status, Ts: time.Now(),
			})
		}
		return
	}
	v.fillAgg[oid] = fillAgg{qty: qty, quote: quote}
	v.publishOrder(domain.OrderUpdate{
		Venue: v.cfg.Name, ClientID: cid, OrderIndex: oid,
		Status: status, FilledQty: qty, FilledQuote: quote,
		Ts: time.Now(),
	})
}

// SubscribeOrders streams order updates for the configured main account.
func (v *Venue) SubscribeOrders(ctx context.Context) (<-chan domain.OrderUpdate, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if v.cfg.Account == "" {
		return nil, domain.ErrNoCredentials
	}
	ch := make(chan domain.OrderUpdate, 1024)
	v.ordersMu.Lock()
	if v.closed.Load() {
		v.ordersMu.Unlock()
		return nil, fmt.Errorf("venue closed")
	}
	v.orderSubs = append(v.orderSubs, ch)
	v.ordersMu.Unlock()
	v.ordersOnce.Do(func() {
		v.ws.subscribe("orderUpdates", map[string]any{"type": "orderUpdates", "user": v.cfg.Account})
		v.ws.subscribe("userFills", map[string]any{"type": "userFills", "user": v.cfg.Account})
	})
	go func() {
		select {
		case <-ctx.Done():
		case <-v.ws.done:
		}
		v.ordersMu.Lock()
		defer v.ordersMu.Unlock()
		for i, c := range v.orderSubs {
			if c == ch {
				v.orderSubs = append(v.orderSubs[:i], v.orderSubs[i+1:]...)
				close(ch)
				break
			}
		}
	}()
	return ch, nil
}

// Position returns the pair's position on marketID and the venue's total
// notional aggregated across all markets.
func (v *Venue) Positions(ctx context.Context) ([]domain.Position, error) {
	snapshot, err := v.AccountSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	return snapshot.Positions, nil
}

// AccountSnapshot returns positions and account risk from one read.
func (v *Venue) AccountSnapshot(ctx context.Context) (domain.AccountSnapshot, error) {
	if v.cfg.Account == "" {
		return domain.AccountSnapshot{}, domain.ErrNoCredentials
	}
	ch, err := v.rest.clearinghouse(ctx, v.cfg.Account, v.cfg.Dex)
	if err != nil {
		return domain.AccountSnapshot{}, err
	}
	var snapshot domain.AccountSnapshot
	seen := map[string]bool{}
	for _, ap := range ch.AssetPositions {
		coin := ap.Position.Coin
		if seen[coin] {
			return domain.AccountSnapshot{}, fmt.Errorf("repeated position coin %q", coin)
		}
		seen[coin] = true
		id, ok := v.assetForCoin(coin)
		if !ok {
			return domain.AccountSnapshot{}, fmt.Errorf("position coin %q not in universe", coin)
		}
		qty, err := number(ap.Position.Szi)
		if err != nil || math.IsNaN(qty) {
			return domain.AccountSnapshot{}, fmt.Errorf("coin %q: invalid position size", coin)
		}
		if qty == 0 {
			continue
		}
		entry, err := number(ap.Position.EntryPx)
		if err != nil || entry <= 0 {
			return domain.AccountSnapshot{}, fmt.Errorf("coin %q: invalid entry price", coin)
		}
		liq, err := number(ap.Position.LiquidationPx)
		if err != nil || liq < 0 {
			return domain.AccountSnapshot{}, fmt.Errorf("coin %q: invalid liquidation price", coin)
		}
		notional, err := number(ap.Position.PositionValue)
		if err != nil || notional < 0 {
			notional = 0
		}
		mode := domain.MarginIsolated
		if ap.Position.Leverage.Type == "cross" {
			mode = domain.MarginCross
		}
		snapshot.Positions = append(snapshot.Positions, domain.Position{
			MarketID: id, Symbol: strings.TrimPrefix(coin, v.cfg.Dex+":"),
			Qty: qty, AvgEntry: entry, LiquidationPrice: liq,
			MarginMode: mode, Notional: notional,
		})
	}
	sort.Slice(snapshot.Positions, func(i, j int) bool {
		return snapshot.Positions[i].MarketID < snapshot.Positions[j].MarketID
	})
	// Cross-margin evidence only applies when the account actually runs cross
	// positions; isolated-only dex accounts (io) leave it nil.
	for _, ap := range ch.AssetPositions {
		if ap.Position.Leverage.Type == "cross" {
			equity, err1 := number(ch.CrossMarginSummary.AccountValue)
			maint, err2 := number(ch.CrossMaintenanceMarginUsed)
			if err1 == nil && err2 == nil && maint >= 0 {
				snapshot.CrossMargin = &domain.CrossMarginRisk{Equity: equity, Maintenance: maint}
			}
			break
		}
	}
	return snapshot, nil
}

func (v *Venue) assetForCoin(coin string) (int, bool) {
	v.assetsMu.RLock()
	defer v.assetsMu.RUnlock()
	id, ok := v.assets[coin]
	return id, ok
}
