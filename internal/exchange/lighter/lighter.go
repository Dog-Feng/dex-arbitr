// Package lighter implements exchange.Venue for Lighter-family venues
// (Lighter mainnet and RH Lighter share the same API; they differ only by
// endpoints and chain id).
package lighter

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	lclient "github.com/elliottech/lighter-go/client"
	lhttp "github.com/elliottech/lighter-go/client/http"

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
	sendMu sync.Mutex // serialize sign+send to keep nonces ordered

	authMu     sync.Mutex
	authToken  string
	authExpiry time.Time

	booksMu sync.Mutex
	books   map[int]*bookState

	ordersOnce sync.Once
	orderSubs  []chan domain.OrderUpdate
	orderMu    sync.Mutex

	closed atomic.Bool
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
		cfg.BookDepth, _ = strconv.Atoi(v)
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
	v := &Venue{cfg: cfg, books: map[int]*bookState{}}
	v.rest = &restClient{host: cfg.Host}
	if cfg.PrivateKey != "" {
		tx, err := lclient.NewTxClient(lhttp.NewClient(cfg.Host), cfg.PrivateKey, cfg.AccountIndex, cfg.APIKeyIndex, cfg.ChainID)
		if err != nil {
			return nil, fmt.Errorf("%s: signer: %w", cfg.Name, err)
		}
		v.tx = tx
	}
	wsURL := cfg.WSURL
	if v.tx == nil {
		// Read-only sessions use the readonly endpoint (also works from restricted regions).
		wsURL += "?readonly=true"
	}
	v.ws = newWSClient(cfg.Name, wsURL, v.onWSMessage)
	v.ws.onReset = v.resetBooks
	v.nonce.fetch = func() (int64, error) {
		return v.rest.nextNonce(context.Background(), cfg.AccountIndex, cfg.APIKeyIndex)
	}
	return v, nil
}

func (v *Venue) Name() string   { return v.cfg.Name }
func (v *Venue) CanTrade() bool { return v.tx != nil }

// Close stops websocket loops.
func (v *Venue) Close() error {
	v.closed.Store(true)
	v.ws.close()
	return nil
}

// CheckKey verifies the configured API key matches the one registered on chain.
func (v *Venue) CheckKey() error {
	if v.tx == nil {
		return domain.ErrNoCredentials
	}
	return v.tx.Check()
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
		out = append(out, domain.Instrument{
			Venue:         v.cfg.Name,
			Symbol:        o.Symbol,
			MarketID:      o.MarketID,
			SizeDecimals:  o.SizeDecimals,
			PriceDecimals: o.PriceDecimals,
			MinBase:       domain.ParseF(o.MinBase),
			MinQuote:      domain.ParseF(o.MinQuote),
			TakerFee:      domain.ParseF(o.TakerFee) / 100,
			MakerFee:      domain.ParseF(o.MakerFee) / 100,
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
