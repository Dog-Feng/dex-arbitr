// Package lighter implements exchange.Venue for Lighter-family venues
// (Lighter mainnet and RH Lighter share the same API; they differ only by
// endpoints and chain id).
package lighter

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	lclient "github.com/elliottech/lighter-go/client"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
)

// Preset endpoints.
type preset struct {
	host    string
	ws      string
	chainID uint32
}

var presets = map[string]preset{
	"lighter":   {host: "https://mainnet.zklighter.elliot.ai", ws: "wss://mainnet.zklighter.elliot.ai/stream", chainID: 304},
	"rhlighter": {host: "https://api.rh.lighter.xyz", ws: "wss://api.rh.lighter.xyz/stream", chainID: 466324},
}

func init() {
	for kind := range presets {
		k := kind
		exchange.Register(k, func(name string, opts map[string]string) (exchange.Venue, error) {
			return New(name, k, opts)
		})
	}
}

// Config of one Lighter-family venue instance.
type Config struct {
	Name         string
	Host         string // e.g. https://mainnet.zklighter.elliot.ai (no /api/v1)
	WSURL        string
	ChainID      uint32
	AccountIndex int64
	APIKeyIndex  uint8
	PrivateKey   string
	BookDepth    int
}

// Venue is a Lighter-family adapter.
type Venue struct {
	cfg    Config
	tx     *lclient.TxClient // nil => read-only
	rest   *restClient
	ws     *wsClient
	nonce  nonceMgr
	sendMu chan struct{} // serialize sign+send while allowing cancelled waiters to leave

	authMu     sync.Mutex
	authToken  string
	authExpiry time.Time

	booksMu sync.Mutex
	books   map[int]*bookState

	ordersOnce sync.Once
	orderSubs  []chan domain.OrderUpdate
	orderMu    sync.Mutex

	closed    atomic.Bool
	closeDone chan struct{}
}

// New builds the venue from a preset kind plus option overrides.
// Recognized opts: host, ws_url, chain_id, account_index, api_key_index, private_key, book_depth.
func New(name, kind string, opts map[string]string) (*Venue, error) {
	p, ok := presets[kind]
	if !ok {
		return nil, fmt.Errorf("lighter: unknown preset %q", kind)
	}
	cfg := Config{Name: name, Host: p.host, WSURL: p.ws, ChainID: p.chainID, BookDepth: 20}
	if v := opts["host"]; v != "" {
		cfg.Host = strings.TrimRight(v, "/")
	}
	if v := opts["ws_url"]; v != "" {
		cfg.WSURL = v
	}
	if v := opts["chain_id"]; v != "" {
		n, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("chain_id: %w", err)
		}
		cfg.ChainID = uint32(n)
	}
	if v := opts["book_depth"]; v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("book_depth: %w", err)
		}
		cfg.BookDepth = n
	}
	cfg.PrivateKey = strings.TrimSpace(opts["private_key"])
	if cfg.PrivateKey != "" {
		ai, err := strconv.ParseInt(opts["account_index"], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%s: account_index: %w", name, err)
		}
		ki, err := strconv.ParseUint(opts["api_key_index"], 10, 8)
		if err != nil {
			return nil, fmt.Errorf("%s: api_key_index: %w", name, err)
		}
		cfg.AccountIndex, cfg.APIKeyIndex = ai, uint8(ki)
	}
	return NewWithConfig(cfg)
}

// NewWithConfig builds a venue from an explicit config.
func NewWithConfig(cfg Config) (*Venue, error) {
	if cfg.BookDepth <= 0 {
		return nil, fmt.Errorf("book_depth must be positive")
	}
	host, err := url.Parse(cfg.Host)
	if err != nil || host.Host == "" || (host.Scheme != "http" && host.Scheme != "https") || host.RawQuery != "" || host.Fragment != "" || host.User != nil {
		return nil, fmt.Errorf("invalid REST host")
	}
	wsURL, err := url.Parse(cfg.WSURL)
	if err != nil || wsURL.Host == "" || (wsURL.Scheme != "ws" && wsURL.Scheme != "wss") || wsURL.User != nil || wsURL.Fragment != "" {
		return nil, fmt.Errorf("invalid websocket URL")
	}
	cfg.Host = strings.TrimRight(cfg.Host, "/")
	v := &Venue{cfg: cfg, books: map[int]*bookState{}, sendMu: make(chan struct{}, 1), closeDone: make(chan struct{})}
	v.rest = &restClient{host: cfg.Host}
	if cfg.PrivateKey != "" {
		// Nonces and key checks use our contextual REST client. Supplying no SDK
		// HTTP client also prevents an accidental implicit nonce fetch.
		tx, err := lclient.NewTxClient(nil, cfg.PrivateKey, cfg.AccountIndex, cfg.APIKeyIndex, cfg.ChainID)
		if err != nil {
			return nil, fmt.Errorf("%s: signer: %w", cfg.Name, err)
		}
		v.tx = tx
	}
	if v.tx == nil {
		// Read-only sessions use the readonly endpoint (also works from restricted regions).
		q := wsURL.Query()
		q.Set("readonly", "true")
		wsURL.RawQuery = q.Encode()
	}
	v.ws = newWSClient(cfg.Name, wsURL.String(), v.onWSMessage)
	v.ws.onReset = v.resetBooks
	v.nonce.fetch = func(ctx context.Context) (int64, error) {
		return v.rest.nextNonce(ctx, cfg.AccountIndex, cfg.APIKeyIndex)
	}
	return v, nil
}

func (v *Venue) Name() string   { return v.cfg.Name }
func (v *Venue) CanTrade() bool { return v.tx != nil }

// Close stops websocket loops.
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
	v.orderMu.Lock()
	for _, ch := range v.orderSubs {
		close(ch)
	}
	v.orderSubs = nil
	v.orderMu.Unlock()
	return nil
}

// CheckKey verifies the configured API key matches the one registered on chain.
func (v *Venue) CheckKey(ctx context.Context) error {
	if v.tx == nil {
		return domain.ErrNoCredentials
	}
	key, err := v.rest.apiKey(ctx, v.cfg.AccountIndex, v.cfg.APIKeyIndex)
	if err != nil {
		return fmt.Errorf("check configured API key: %w", err)
	}
	registered, err := hex.DecodeString(strings.TrimPrefix(key, "0x"))
	public := v.tx.GetKeyManager().PubKeyBytes()
	if err != nil || !bytes.Equal(registered, public[:]) {
		return fmt.Errorf("configured API key does not match this venue")
	}
	return nil
}

// auth returns a cached auth token (valid up to 8h; we use 2h and refresh at <10min).
func (v *Venue) auth() (string, error) {
	if v.tx == nil {
		return "", domain.ErrNoCredentials
	}
	v.authMu.Lock()
	defer v.authMu.Unlock()
	if v.authToken != "" && time.Until(v.authExpiry) > 10*time.Minute {
		return v.authToken, nil
	}
	exp := time.Now().Add(2 * time.Hour)
	tok, err := v.tx.GetAuthToken(exp)
	if err != nil {
		return "", err
	}
	v.authToken, v.authExpiry = tok, exp
	return tok, nil
}

// Instruments returns active perpetual markets.
func (v *Venue) Instruments(ctx context.Context) ([]domain.Instrument, error) {
	obs, err := v.rest.orderBooks(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]domain.Instrument, 0, len(obs))
	for _, o := range obs {
		if o.MarketType != "perp" || o.Status != "active" {
			continue
		}
		if o.MarketID == nil || o.SizeDecimals == nil || o.PriceDecimals == nil {
			return nil, fmt.Errorf("market metadata missing id or precision")
		}
		marketID, sizeDecimals, priceDecimals := *o.MarketID, *o.SizeDecimals, *o.PriceDecimals
		if _, err := marketIndex(domain.Instrument{MarketID: marketID}); err != nil {
			return nil, err
		}
		if sizeDecimals < 0 || sizeDecimals > 18 || priceDecimals < 0 || priceDecimals > 18 {
			return nil, fmt.Errorf("market %d: invalid precision", marketID)
		}
		minBase, err := finiteDecimal(o.MinBase)
		if err != nil || minBase < 0 {
			return nil, fmt.Errorf("market %d: invalid min base", marketID)
		}
		minQuote, err := finiteDecimal(o.MinQuote)
		if err != nil || minQuote < 0 {
			return nil, fmt.Errorf("market %d: invalid min quote", marketID)
		}
		taker, err := finiteDecimal(o.TakerFee)
		if err != nil {
			return nil, fmt.Errorf("market %d: invalid taker fee", marketID)
		}
		maker, err := finiteDecimal(o.MakerFee)
		if err != nil {
			return nil, fmt.Errorf("market %d: invalid maker fee", marketID)
		}
		out = append(out, domain.Instrument{
			Venue:         v.cfg.Name,
			Symbol:        o.Symbol,
			MarketID:      marketID,
			SizeDecimals:  sizeDecimals,
			PriceDecimals: priceDecimals,
			MinBase:       minBase,
			MinQuote:      minQuote,
			TakerFee:      taker / 100,
			MakerFee:      maker / 100,
		})
	}
	return out, nil
}

// Positions returns non-zero positions of the configured account.
func (v *Venue) Positions(ctx context.Context) ([]domain.Position, error) {
	if v.tx == nil {
		return nil, domain.ErrNoCredentials
	}
	return v.rest.positions(ctx, v.cfg.AccountIndex)
}
