package hyperliquid

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/url"
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
	// Network selects the L1 signing source: "mainnet" or "testnet". Empty
	// infers it from the exchange URL host (testnet only when the host names
	// testnet), so proxies and trailing slashes keep mainnet signatures.
	Network string
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
	// assetsLoaded marks that the asset universe (including delisted coins)
	// has been resolved, so AccountSnapshot no longer depends on a prior
	// Instruments call.
	assetsLoaded bool

	// maxLev caches the per-market maximum leverage from meta.
	levMu  sync.RWMutex
	maxLev map[int]int

	warnMu      sync.Mutex
	warnedCoins map[string]time.Time

	booksMu sync.Mutex
	books   map[int]*bookState

	ordersMu     sync.Mutex
	orderSubs    []chan domain.OrderUpdate
	ordersOnce   sync.Once
	orderLookup  map[int64]int64 // oid -> client id, filled from submit acks and updates
	orderFinal   map[int64]domain.OrderStatus
	orderOrig    map[int64]float64 // oid -> requested quantity (wire size)
	fillStates   map[int64]*fillState
	orderExpires map[int64]int64 // cid -> expiresAfter (ms)
	knownCIDs    map[int64]time.Time
	pendingFills map[int64][]pendingFill
	finalTouched map[int64]time.Time
	// staleSeen records when cleanup first saw an oid without a terminal
	// state (a never-reported order, or a fill state rebuilt by a REST query
	// after the oid was cleaned) so such entries age out instead of leaking.
	staleSeen map[int64]time.Time

	lastNonce atomic.Int64
	sendMu    chan struct{}
	closed    atomic.Bool
	closeDone chan struct{}
}

// fillState accumulates the fill facts of one order. Every trade is deduped
// by tid; the synchronous /exchange response contributes an authoritative
// total that per-trade deliveries can only approach, never double count.
type fillState struct {
	tids      map[int64]struct{}
	uniq      int64
	qty       float64
	quote     float64
	fee       float64
	syncQty   float64
	syncQuote float64
}

func newFillState() *fillState { return &fillState{tids: map[int64]struct{}{}} }

// add merges one trade identified by tid. It reports whether the trade was
// new; a replay (WS snapshot, re-delivery, REST backfill) returns false.
func (s *fillState) add(tid int64, sz, px, fee float64) bool {
	key := tid
	if key <= 0 {
		// A fill without a tid cannot be deduplicated; count it once.
		s.uniq++
		key = -s.uniq
	}
	if _, dup := s.tids[key]; dup {
		return false
	}
	s.tids[key] = struct{}{}
	s.qty += sz
	s.quote += sz * px
	s.fee += fee
	return true
}

// effective returns the published cumulative fill: the per-trade sum once it
// covers the synchronous total, otherwise the synchronous total itself. The
// fee is only reported with the per-trade sum: fees of a partial subset of
// trades would understate the order's fee, so it is reported as 0 (unknown).
func (s *fillState) effective() (qty, quote, fee float64) {
	if s.qty >= s.syncQty-1e-12 {
		return s.qty, s.quote, s.fee
	}
	return s.syncQty, s.syncQuote, 0
}

// fillState returns the accumulator for an oid, creating it when absent.
// Callers must hold ordersMu.
func (v *Venue) fillState(oid int64) *fillState {
	fs := v.fillStates[oid]
	if fs == nil {
		fs = newFillState()
		v.fillStates[oid] = fs
	}
	return fs
}

// pendingFill parks a fill that arrived before its oid was registered.
type pendingFill struct {
	fill wsFill
	at   time.Time
}

// New builds the venue from a preset kind plus option overrides.
// Recognized opts: dex, info_url, exchange_url, ws_url, account, private_key,
// book_depth, network.
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
	cfg.Network = strings.TrimSpace(opts["network"])
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
	mainnet, err := signingMainnet(cfg.Network, cfg.ExchangeURL)
	if err != nil {
		return nil, err
	}
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
		orderLookup: map[int64]int64{}, orderFinal: map[int64]domain.OrderStatus{},
		orderOrig: map[int64]float64{}, fillStates: map[int64]*fillState{},
		orderExpires: map[int64]int64{}, knownCIDs: map[int64]time.Time{},
		pendingFills: map[int64][]pendingFill{}, finalTouched: map[int64]time.Time{},
		staleSeen: map[int64]time.Time{},
		maxLev:    map[int]int{}, warnedCoins: map[string]time.Time{},
		sendMu: make(chan struct{}, 1), closeDone: make(chan struct{}),
	}
	if cfg.AgentKey != "" {
		s, err := newSigner(cfg.AgentKey)
		if err != nil {
			return nil, err
		}
		s.mainnet = mainnet
		v.signer = s
	}
	v.rest = newRestClient(cfg.InfoURL, cfg.ExchangeURL)
	v.ws = newWSClient(cfg.Name, cfg.WSURL, v.onWSMessage)
	v.ws.onReset = v.resetBooks
	go v.cleanupLoop()
	return v, nil
}

// signingMainnet resolves the L1 signing network. An explicit network wins;
// otherwise only an exchange host naming testnet signs as testnet. Exact URL
// equality is not used: a trailing slash or a mainnet proxy must not silently
// switch every signature to the testnet source.
func signingMainnet(network, exchangeURL string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(network)) {
	case "mainnet":
		return true, nil
	case "testnet":
		return false, nil
	case "":
	default:
		return false, fmt.Errorf("network must be mainnet or testnet, got %q", network)
	}
	u, err := url.Parse(strings.TrimSpace(exchangeURL))
	if err != nil {
		return false, fmt.Errorf("invalid exchange URL: %w", err)
	}
	return !strings.Contains(strings.ToLower(u.Hostname()), "testnet"), nil
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
	// Register the whole universe (including delisted coins) before
	// validating tradable markets, so a concurrent snapshot never observes a
	// half-built asset table marked as loaded.
	v.registerAssets(meta.Universe, dexIndex)
	out := make([]domain.Instrument, 0, len(meta.Universe))
	for i, m := range meta.Universe {
		id := v.assetID(dexIndex, i)
		// Delisted assets are registered so account snapshots can attribute
		// leftover positions, but they are not tradable.
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
		taker, maker := baseFees(m, v.cfg.Dex != "")
		out = append(out, domain.Instrument{
			Venue: v.cfg.Name, Symbol: sym, MarketID: id,
			SizeDecimals: m.SzDecimals, PriceDecimals: priceDecimals,
			MinBase: 0, MinQuote: 10, TakerFee: taker, MakerFee: maker,
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
	if inst.MarketID < 0 {
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
		Coin   string `json:"coin"`
		Sz     string `json:"sz"`
		OrigSz string `json:"origSz"`
		Oid    int64  `json:"oid"`
		Cloid  string `json:"cloid"`
	} `json:"order"`
	Status          string `json:"status"`
	StatusTimestamp int64  `json:"statusTimestamp"`
}

// partialIOC normalizes a terminal FILLED report that covered only part of
// the requested quantity. Hyperliquid auto-cancels the unfilled remainder of
// an IOC but still reports the order as "filled"; the executor treats a
// FILLED terminal as exactly-requested and would otherwise fail its fill
// verification. Reporting Canceled with the partial fill routes the leg
// through the executor's existing partial-fill + rescue handling instead.
func partialIOC(status domain.OrderStatus, filled, requested float64) domain.OrderStatus {
	// Only normalize when actual fill facts exist: a FILLED report with no
	// known fills must stay unresolved instead of becoming a zero-fill
	// Canceled terminal.
	if status != domain.StatusFilled || requested <= 0 || filled <= 0 || filled >= requested-1e-9 {
		return status
	}
	return domain.StatusCanceled
}

type wsFillsMsg struct {
	Data struct {
		User       string   `json:"user"`
		IsSnapshot bool     `json:"isSnapshot"`
		Fills      []wsFill `json:"fills"`
	} `json:"data"`
}

type wsFill struct {
	Coin  string `json:"coin"`
	Px    string `json:"px"`
	Sz    string `json:"sz"`
	Oid   int64  `json:"oid"`
	Time  int64  `json:"time"`
	Tid   int64  `json:"tid"`
	Hash  string `json:"hash"`
	Fee   string `json:"fee"`
	Cloid string `json:"cloid"`
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
		if orig, oerr := strconv.ParseFloat(u.Order.OrigSz, 64); oerr == nil && orig > 0 && !math.IsNaN(orig) {
			v.orderOrig[u.Order.Oid] = orig
		}
		v.drainPending(u.Order.Oid)
		aggQty, aggQuote, aggFee := v.fillState(u.Order.Oid).effective()
		status := mapOrderStatus(u.Status)
		orig := v.orderOrig[u.Order.Oid]
		if status.Terminal() {
			v.markFinal(u.Order.Oid, status)
		}
		// Only publish a terminal report that already carries its fill facts.
		// A filled or partially-filled canceled order whose fills have not
		// been observed yet must resolve through the fill backfill; a
		// zero-size terminal report would desync the executor's position
		// accounting.
		publishNow := status == domain.StatusRejected ||
			(status.Terminal() && aggQty > 0)
		if publishNow {
			status = partialIOC(status, aggQty, v.orderOrig[u.Order.Oid])
			v.publishOrder(domain.OrderUpdate{
				Venue: v.cfg.Name, ClientID: cid, OrderIndex: u.Order.Oid,
				Status: status, FilledQty: aggQty, FilledQuote: aggQuote, Fee: aggFee,
				Ts: time.Now(),
			})
		}
		v.ordersMu.Unlock()
		if (status == domain.StatusFilled || status == domain.StatusCanceled) && aggQty == 0 {
			// Late fill aggregation: fetch fills once for the terminal oid.
			go v.backfillFills(u.Order.Oid, cid, u.StatusTimestamp, status, orig)
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
			// A fill may arrive before the submit ack or order update
			// registered its oid. A cloid known to this process identifies it
			// as ours; otherwise park the fill until registration.
			if c, ok := cloidToID(f.Cloid); ok {
				if _, ours := v.knownCIDs[c]; ours {
					v.orderLookup[f.Oid] = c
					cid, known = c, true
				}
			}
			if !known {
				v.pendingFills[f.Oid] = append(v.pendingFills[f.Oid], pendingFill{fill: f, at: time.Now()})
				continue
			}
		}
		if !v.mergeFillLocked(f) {
			continue // duplicate trade (replay, snapshot, or backfill overlap)
		}
		if m.Data.IsSnapshot {
			continue // snapshot replays only fill gaps; they never re-publish
		}
		aggQty, aggQuote, aggFee := v.fillState(f.Oid).effective()
		status := v.orderFinal[f.Oid]
		if !status.Terminal() {
			status = domain.StatusUnknown
		} else {
			status = partialIOC(status, aggQty, v.orderOrig[f.Oid])
		}
		v.publishOrder(domain.OrderUpdate{
			Venue: v.cfg.Name, ClientID: cid, OrderIndex: f.Oid,
			Status: status, FilledQty: aggQty, FilledQuote: aggQuote, Fee: aggFee,
			Ts: time.Now(),
		})
	}
}

// mergeFillLocked merges one WS fill into the per-oid state and reports
// whether it was a new trade. Callers must hold ordersMu.
func (v *Venue) mergeFillLocked(f wsFill) bool {
	sz, err1 := strconv.ParseFloat(f.Sz, 64)
	px, err2 := strconv.ParseFloat(f.Px, 64)
	if err1 != nil || err2 != nil || sz <= 0 || px <= 0 || math.IsNaN(sz) || math.IsNaN(px) {
		return false
	}
	fee, feeErr := strconv.ParseFloat(f.Fee, 64)
	if feeErr != nil || fee < 0 || math.IsNaN(fee) {
		fee = 0
	}
	return v.fillState(f.Oid).add(f.Tid, sz, px, fee)
}

// drainPending merges fills that arrived before the oid was registered.
// Callers must hold ordersMu.
func (v *Venue) drainPending(oid int64) {
	pending := v.pendingFills[oid]
	if len(pending) == 0 {
		return
	}
	delete(v.pendingFills, oid)
	now := time.Now()
	for _, pf := range pending {
		if now.Sub(pf.at) > time.Minute {
			continue
		}
		v.mergeFillLocked(pf.fill)
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
	if u.Status.Terminal() && u.OrderIndex > 0 {
		v.finalTouched[u.OrderIndex] = time.Now()
	}
}

// markFinal records a terminal status and starts its retention window. A
// terminal order still awaiting its fill backfill has not been published
// yet, so the window must start here, not at publication, or cleanup would
// drop the oid before its fills arrive. Callers must hold ordersMu.
func (v *Venue) markFinal(oid int64, status domain.OrderStatus) {
	v.orderFinal[oid] = status
	v.finalTouched[oid] = time.Now()
}

// backfillFills fetches fills for a terminal order whose fills have not been
// observed yet, then publishes the terminal update with complete fill facts.
// origQty is the requested quantity; a FILLED report below it is normalized
// to Canceled (partial IOC).
func (v *Venue) backfillFills(oid, cid int64, statusTs int64, status domain.OrderStatus, origQty float64) {
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
	added := false
	for _, f := range fills {
		if f.Oid.String() != strconv.FormatInt(oid, 10) {
			continue
		}
		sz, err1 := number(f.Sz)
		px, err2 := number(f.Px)
		if err1 != nil || err2 != nil {
			continue
		}
		fee, feeErr := number(f.Fee)
		if feeErr != nil || fee < 0 {
			fee = 0
		}
		if v.fillState(oid).add(f.Tid, sz, px, fee) {
			added = true
		}
	}
	qty, quote, fee := v.fillState(oid).effective()
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
	if !added {
		// Every trade was already merged through another source; the
		// published terminal update is still current.
		return
	}
	status = partialIOC(status, qty, origQty)
	v.publishOrder(domain.OrderUpdate{
		Venue: v.cfg.Name, ClientID: cid, OrderIndex: oid,
		Status: status, FilledQty: qty, FilledQuote: quote, Fee: fee,
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
	if err := v.ensureAssets(ctx); err != nil {
		return domain.AccountSnapshot{}, err
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
			// A delisted or otherwise unknown coin still contributes to the
			// account exposure. Attribute it with a reserved negative id so
			// monitoring and reconciliation can see it without failing.
			id = -1
			v.warnCoinOnce(coin)
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
			if id >= 0 {
				return domain.AccountSnapshot{}, fmt.Errorf("coin %q: invalid entry price", coin)
			}
			entry = 0
		}
		liq, err := number(ap.Position.LiquidationPx)
		if err != nil || liq < 0 {
			if id >= 0 {
				return domain.AccountSnapshot{}, fmt.Errorf("coin %q: invalid liquidation price", coin)
			}
			liq = 0
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
	// The per-dex account value feeds the minute reconciliation log. Use the
	// margin summary first and the cross summary as a fallback for dexes that
	// report account value only there.
	if v, err := number(ch.MarginSummary.AccountValue); err == nil && v > 0 {
		snapshot.AccountValue = v
	} else if v, err := number(ch.CrossMarginSummary.AccountValue); err == nil && v > 0 {
		snapshot.AccountValue = v
	}
	return snapshot, nil
}

func (v *Venue) assetForCoin(coin string) (int, bool) {
	v.assetsMu.RLock()
	defer v.assetsMu.RUnlock()
	id, ok := v.assets[coin]
	return id, ok
}

// ensureAssets resolves the asset universe once, so AccountSnapshot no longer
// depends on Instruments having been called first.
func (v *Venue) ensureAssets(ctx context.Context) error {
	v.assetsMu.RLock()
	loaded := v.assetsLoaded
	v.assetsMu.RUnlock()
	if loaded {
		return nil
	}
	// Fetch without holding assetsMu: the book path reads the asset table
	// and must not block behind network I/O. A concurrent duplicate fetch
	// registers identical ids and is harmless.
	meta, err := v.rest.meta(ctx, v.cfg.Dex)
	if err != nil {
		return fmt.Errorf("asset universe: %w", err)
	}
	dexIndex, err := v.rest.dexIndex(ctx, v.cfg.Dex)
	if err != nil {
		return fmt.Errorf("perp dex index: %w", err)
	}
	v.registerAssets(meta.Universe, dexIndex)
	return nil
}

// assetID maps a meta index to the signed asset id. Validator perps use the
// plain meta index. Builder perps use 100000 + perp_dex_index*10000 +
// index_in_meta; perpDexs reports "io" at index 10, so io:SNDK (index 2) is
// asset 200002.
func (v *Venue) assetID(dexIndex, i int) int {
	if v.cfg.Dex != "" {
		return 100000 + dexIndex*10000 + i
	}
	return i
}

// registerAssets records every coin's asset id and max leverage, then marks
// the universe loaded. Each map is written under its own lock.
func (v *Venue) registerAssets(universe []metaItem, dexIndex int) {
	v.levMu.Lock()
	for i, m := range universe {
		v.maxLev[v.assetID(dexIndex, i)] = m.MaxLeverage
	}
	v.levMu.Unlock()
	v.assetsMu.Lock()
	for i, m := range universe {
		v.assets[m.Name] = v.assetID(dexIndex, i)
	}
	v.assetsLoaded = true
	v.assetsMu.Unlock()
}

// maxLeverage returns the market's maximum leverage from meta when known.
func (v *Venue) maxLeverage(asset int) (int, bool) {
	v.levMu.RLock()
	defer v.levMu.RUnlock()
	cap, ok := v.maxLev[asset]
	return cap, ok
}

// warnCoinOnce logs an unknown-position-coin warning at most once a minute
// per coin so a delisted leftover cannot spam the log.
func (v *Venue) warnCoinOnce(coin string) {
	now := time.Now()
	v.warnMu.Lock()
	defer v.warnMu.Unlock()
	if last, ok := v.warnedCoins[coin]; ok && now.Sub(last) < time.Minute {
		return
	}
	v.warnedCoins[coin] = now
	slog.Warn("position coin not in universe", "venue", v.cfg.Name, "coin", coin)
}

// cleanupLoop periodically drops per-order state once a terminal outcome has
// been published long enough to cover every query and recovery window.
func (v *Venue) cleanupLoop() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-v.closeDone:
			return
		case <-t.C:
			v.cleanupOrders()
		}
	}
}

func (v *Venue) cleanupOrders() {
	now := time.Now()
	finalCutoff := now.Add(-10 * time.Minute)
	cidCutoff := now.Add(-time.Hour)
	v.ordersMu.Lock()
	drop := func(oid int64) {
		delete(v.orderLookup, oid)
		delete(v.orderFinal, oid)
		delete(v.orderOrig, oid)
		delete(v.fillStates, oid)
		delete(v.finalTouched, oid)
		delete(v.staleSeen, oid)
	}
	for oid, st := range v.orderFinal {
		if !st.Terminal() {
			continue
		}
		// An entry without a touch time is treated as just touched: its
		// retention window starts now rather than expiring immediately.
		touched, ok := v.finalTouched[oid]
		if !ok {
			v.finalTouched[oid] = now
			continue
		}
		if touched.After(finalCutoff) {
			continue
		}
		drop(oid)
	}
	// Entries never reaching a terminal state age out an hour after cleanup
	// first notices them.
	stale := func(oid int64) {
		if _, final := v.orderFinal[oid]; final {
			delete(v.staleSeen, oid)
			return
		}
		first, ok := v.staleSeen[oid]
		if !ok {
			v.staleSeen[oid] = now
			return
		}
		if first.Before(cidCutoff) {
			drop(oid)
		}
	}
	for oid := range v.orderLookup {
		stale(oid)
	}
	for oid := range v.fillStates {
		stale(oid)
	}
	for oid := range v.staleSeen {
		if _, lookup := v.orderLookup[oid]; !lookup {
			if _, fs := v.fillStates[oid]; !fs {
				delete(v.staleSeen, oid)
			}
		}
	}
	for oid, pending := range v.pendingFills {
		kept := pending[:0]
		for _, pf := range pending {
			if now.Sub(pf.at) <= time.Minute {
				kept = append(kept, pf)
			}
		}
		if len(kept) == 0 {
			delete(v.pendingFills, oid)
		} else {
			v.pendingFills[oid] = kept
		}
	}
	for cid, at := range v.knownCIDs {
		if at.Before(cidCutoff) {
			delete(v.knownCIDs, cid)
		}
	}
	for cid, exp := range v.orderExpires {
		// Keep the expiry long after it elapsed: the elapsed marker is what
		// turns a late unknownOid into a definitive rejection.
		if exp < now.UnixMilli()-time.Hour.Milliseconds() {
			delete(v.orderExpires, cid)
		}
	}
	v.ordersMu.Unlock()
}
