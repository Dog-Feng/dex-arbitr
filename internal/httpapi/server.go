package httpapi

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"dex-grid-arbitr/internal/config"
	"dex-grid-arbitr/internal/domain"
	"dex-grid-arbitr/internal/runtime"
	"github.com/shopspring/decimal"
)

//go:embed web/index.html web/favicon.svg
var webFiles embed.FS

type Server struct {
	Runtime  *runtime.Runtime
	Settings config.Settings
}

func NewServer(rt *runtime.Runtime, settings config.Settings) *Server {
	return &Server{Runtime: rt, Settings: settings}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.frontend)
	mux.HandleFunc("/favicon.ico", s.favicon)
	mux.HandleFunc("/api/health", s.health)
	mux.HandleFunc("/api/account", s.account)
	mux.HandleFunc("/api/position/close", s.closePosition)
	mux.HandleFunc("/api/markets", s.markets)
	mux.HandleFunc("/api/strategy", s.strategyView)
	mux.HandleFunc("/api/monitor", s.monitor)
	mux.HandleFunc("/api/strategy/start", s.startStrategy)
	mux.HandleFunc("/api/strategy/tick", s.tick)
	mux.HandleFunc("/api/strategy/stop", s.stopStrategy)
	mux.HandleFunc("/api/strategy/emergency-stop", s.emergencyStop)
	mux.HandleFunc("/api/strategy/events", s.events)
	mux.HandleFunc("/api/strategy/history", s.history)
	return withJSONHeaders(mux)
}

type startRequest struct {
	DEX                  string           `json:"dex"`
	StrategyType         string           `json:"strategy_type"`
	EntryMode            domain.EntryMode `json:"entry_mode"`
	MarketIndex          int              `json:"market_index"`
	Symbol               string           `json:"symbol"`
	Direction            domain.Direction `json:"direction"`
	InitialPrice         *decimal.Decimal `json:"initial_price"`
	MakerRequoteSeconds  int              `json:"maker_requote_seconds"`
	Leverage             decimal.Decimal  `json:"leverage"`
	InitialMargin        decimal.Decimal  `json:"initial_margin"`
	SafetyMargin         decimal.Decimal  `json:"safety_margin"`
	PriceStepPct         decimal.Decimal  `json:"price_step_pct"`
	PriceStepMultiplier  decimal.Decimal  `json:"price_step_multiplier"`
	AmountMultiplier     decimal.Decimal  `json:"amount_multiplier"`
	MaxSafetyOrders      int              `json:"max_safety_orders"`
	TakeProfitPct        decimal.Decimal  `json:"take_profit_pct"`
	MarketMaxSlippagePct decimal.Decimal  `json:"market_max_slippage_pct"`
	MarketMaxRetries     int              `json:"market_max_retries"`
	ReinvestProfit       bool             `json:"reinvest_profit"`
	ReinvestRatio        decimal.Decimal  `json:"reinvest_ratio"`
	MaxCycleMargin       decimal.Decimal  `json:"max_cycle_margin"`
	MaxPositionQuantity  decimal.Decimal  `json:"max_position_quantity"`
	Continuous           bool             `json:"continuous"`
	GridLowerPrice       decimal.Decimal  `json:"grid_lower_price"`
	GridUpperPrice       decimal.Decimal  `json:"grid_upper_price"`
	GridCount            int              `json:"grid_count"`
	GridMargin           decimal.Decimal  `json:"grid_margin"`
	GridFollowUp         bool             `json:"grid_follow_up"`
	GridFollowDown       bool             `json:"grid_follow_down"`
	GridMode             string           `json:"grid_mode"`
}

func defaultStartRequest() startRequest {
	return startRequest{
		DEX: "", StrategyType: "martingale", EntryMode: domain.EntryModeMarket, MarketIndex: 0, Symbol: "BTCUSDT-PERP", Direction: domain.DirectionLong,
		Leverage: decimal.NewFromInt(5), InitialMargin: decimal.RequireFromString("1.92"), SafetyMargin: decimal.RequireFromString("1.91"),
		PriceStepPct: decimal.RequireFromString("0.01"), PriceStepMultiplier: decimal.NewFromInt(1), AmountMultiplier: decimal.NewFromInt(1),
		MaxSafetyOrders: 8, TakeProfitPct: decimal.RequireFromString("0.01"), MarketMaxSlippagePct: decimal.RequireFromString("0.5"),
		MarketMaxRetries: 5, MakerRequoteSeconds: 10, ReinvestRatio: decimal.Zero, Continuous: true,
		GridLowerPrice: decimal.RequireFromString("90"), GridUpperPrice: decimal.RequireFromString("110"),
		GridCount: 10, GridMargin: decimal.NewFromInt(10), GridMode: "arithmetic",
	}
}

type priceTickRequest struct {
	Price decimal.Decimal `json:"price"`
}
type stopRequest struct {
	MarketIndex *int `json:"market_index"`
}
type closePositionRequest struct {
	MarketIndex int `json:"market_index"`
}
type addMarketRequest struct {
	DEX            string          `json:"dex"`
	Symbol         string          `json:"symbol"`
	ExchangeSymbol string          `json:"exchange_symbol"`
	MarketIndex    int             `json:"market_index"`
	BaseAsset      string          `json:"base_asset"`
	QuoteAsset     string          `json:"quote_asset"`
	MarketType     string          `json:"market_type"`
	PriceStep      decimal.Decimal `json:"price_step"`
	SizeStep       decimal.Decimal `json:"size_step"`
	MinBaseAmount  decimal.Decimal `json:"min_base_amount"`
	MinQuoteAmount decimal.Decimal `json:"min_quote_amount"`
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "mode": s.Runtime.BackendMode(), "configured_mode": s.Settings.Mode, "active_dex": s.Runtime.ActiveDEX(), "available_dexes": s.Runtime.AvailableDEXes(), "connected": s.Runtime.Connected(), "configured_symbols": s.Runtime.ConfiguredSymbols(), "trading_enabled": s.Runtime.TradingEnabled()})
}

func (s *Server) account(w http.ResponseWriter, r *http.Request) {
	account, err := s.Runtime.AccountSnapshot(r.Context())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, account)
}

func (s *Server) closePosition(w http.ResponseWriter, r *http.Request) {
	var request closePositionRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	order, err := s.Runtime.ClosePosition(r.Context(), request.MarketIndex)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, order)
}

func (s *Server) markets(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, s.Runtime.Markets())
		return
	}
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	var request addMarketRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if request.QuoteAsset == "" {
		request.QuoteAsset = "USD"
	}
	if request.MarketType == "" {
		request.MarketType = "perp"
	}
	if err := s.Runtime.AddMarket(domain.MarketDescriptor{DEX: request.DEX, Symbol: request.Symbol, ExchangeSymbol: request.ExchangeSymbol, MarketIndex: request.MarketIndex, BaseAsset: request.BaseAsset, QuoteAsset: request.QuoteAsset, MarketType: request.MarketType, PriceStep: request.PriceStep, SizeStep: request.SizeStep, MinBaseAmount: request.MinBaseAmount, MinQuoteAmount: request.MinQuoteAmount}); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	wantedDEX := request.DEX
	if wantedDEX == "" {
		wantedDEX = s.Runtime.ActiveDEX()
	}
	for _, market := range s.Runtime.Markets() {
		if strings.EqualFold(market.DEX, wantedDEX) && market.MarketIndex == request.MarketIndex {
			writeJSON(w, http.StatusOK, market)
			return
		}
	}
}

func (s *Server) strategyView(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	if account, err := s.Runtime.AccountSnapshot(r.Context()); err == nil {
		writeJSON(w, http.StatusOK, s.Runtime.ViewWithAccount(account))
		return
	}
	writeJSON(w, http.StatusOK, s.Runtime.View())
}

func (s *Server) monitor(w http.ResponseWriter, r *http.Request) {
	// Refresh market metadata/tickers before building the view so the page can
	// display a current price on its one-second monitor interval.
	_ = s.Runtime.RefreshMarkets(r.Context())
	accountData := map[string]any{"assets": []domain.AssetSnapshot{}, "positions": []domain.PositionSnapshot{}}
	var accountError string
	var accountSnapshot *domain.AccountSnapshot
	if account, err := s.Runtime.AccountSnapshot(r.Context()); err == nil {
		accountCopy := account
		accountSnapshot = &accountCopy
		accountData = map[string]any{"account_index": account.AccountIndex, "status": account.Status, "collateral": account.Collateral, "available_balance": account.AvailableBalance, "total_asset_value": account.TotalAssetValue, "cross_asset_value": account.CrossAssetValue, "cross_initial_margin_requirement": account.CrossInitialMarginRequirement, "cross_maintenance_margin_requirement": account.CrossMaintenanceMarginRequirement, "positions": account.Positions, "assets": account.Assets}
	} else {
		accountError = err.Error()
	}
	strategyView := s.Runtime.View()
	if accountSnapshot != nil {
		strategyView = s.Runtime.ViewWithAccount(*accountSnapshot)
	}
	writeJSON(w, http.StatusOK, map[string]any{"health": map[string]any{"status": "ok", "mode": s.Runtime.BackendMode(), "configured_mode": s.Settings.Mode, "active_dex": s.Runtime.ActiveDEX(), "available_dexes": s.Runtime.AvailableDEXes(), "connected": s.Runtime.Connected(), "configured_symbols": s.Runtime.ConfiguredSymbols(), "trading_enabled": s.Runtime.TradingEnabled()}, "markets": s.Runtime.Markets(), "strategy": strategyView, "account": accountData, "account_error": accountError})
}

func (s *Server) startStrategy(w http.ResponseWriter, r *http.Request) {
	request := defaultStartRequest()
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	cfg := domain.StrategyConfig{DEX: request.DEX, StrategyType: request.StrategyType, EntryMode: request.EntryMode, InitialPrice: request.InitialPrice, MakerRequoteSeconds: request.MakerRequoteSeconds, MarketIndex: request.MarketIndex, Symbol: request.Symbol, Direction: request.Direction, Leverage: request.Leverage, InitialMargin: request.InitialMargin, SafetyMargin: request.SafetyMargin, PriceStepPct: request.PriceStepPct.Div(decimal.NewFromInt(100)), PriceStepMultiplier: request.PriceStepMultiplier, AmountMultiplier: request.AmountMultiplier, MaxSafetyOrders: request.MaxSafetyOrders, TakeProfitPct: request.TakeProfitPct.Div(decimal.NewFromInt(100)), MarketMaxSlippage: request.MarketMaxSlippagePct.Div(decimal.NewFromInt(100)), MarketMaxRetries: request.MarketMaxRetries, ReinvestProfit: request.ReinvestProfit, ReinvestRatio: request.ReinvestRatio, MaxCycleMargin: request.MaxCycleMargin, MaxPositionQuantity: request.MaxPositionQuantity, Continuous: request.Continuous, GridLowerPrice: request.GridLowerPrice, GridUpperPrice: request.GridUpperPrice, GridCount: request.GridCount, GridMargin: request.GridMargin, GridFollowUp: request.GridFollowUp, GridFollowDown: request.GridFollowDown, GridMode: request.GridMode}
	view, err := s.Runtime.Start(r.Context(), cfg, request.InitialPrice)
	if err != nil {
		if errors.Is(err, runtime.ErrExistingPosition) {
			writeError(w, http.StatusConflict, err)
		} else {
			writeError(w, http.StatusBadRequest, err)
		}
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (s *Server) tick(w http.ResponseWriter, r *http.Request) {
	var request priceTickRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	view, err := s.Runtime.Tick(r.Context(), request.Price)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (s *Server) stopStrategy(w http.ResponseWriter, r *http.Request)  { s.stop(w, r, false) }
func (s *Server) emergencyStop(w http.ResponseWriter, r *http.Request) { s.stop(w, r, true) }
func (s *Server) stop(w http.ResponseWriter, r *http.Request, emergency bool) {
	request := stopRequest{}
	_ = decodeJSONAllowEmpty(r, &request)
	view, err := s.Runtime.Stop(r.Context(), emergency, request.MarketIndex)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (s *Server) events(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.Runtime.View().Events)
}
func (s *Server) history(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit == 0 {
		limit = 100
	}
	result, err := s.Runtime.History(r.Context(), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) frontend(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	data, err := webFiles.ReadFile("web/index.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
}
func (s *Server) favicon(w http.ResponseWriter, _ *http.Request) {
	data, err := webFiles.ReadFile("web/favicon.svg")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "image/svg+xml")
	_, _ = w.Write(data)
}

func withJSONHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}
func decodeJSON(r *http.Request, target any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, 2<<20))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}
func decodeJSONAllowEmpty(r *http.Request, target any) error {
	if r.Body == nil {
		return nil
	}
	decoder := json.NewDecoder(io.LimitReader(r.Body, 2<<20))
	err := decoder.Decode(target)
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]any{"detail": err.Error()})
}
func methodNotAllowed(w http.ResponseWriter) {
	w.Header().Set("Allow", strings.Join([]string{http.MethodGet, http.MethodPost}, ", "))
	w.WriteHeader(http.StatusMethodNotAllowed)
}
func contextCanceled(ctx context.Context) bool { return ctx.Err() != nil }
