// Package supervisor 管理每个已启用交易所的 Runner：一个 DEX 一个实例。
package supervisor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"dex-grid/internal/app/engine"
	"dex-grid/internal/config"
	"dex-grid/internal/domain/market"
	"dex-grid/internal/domain/order"
	"dex-grid/internal/domain/strategy"
	"dex-grid/internal/domain/strategy/grid"
	_ "dex-grid/internal/domain/strategy/martingale"
	"dex-grid/internal/exchange"
	"dex-grid/internal/infra/logx"
	"dex-grid/internal/infra/store"
)

// Supervisor 是控制台与 Runner 之间的编排层。
type Supervisor struct {
	cfg     *config.Config
	store   *store.Store
	logs    *logx.Buffer
	log     *slog.Logger
	started time.Time

	mu   sync.RWMutex
	inst map[string]*instance
}

type instance struct {
	name   string
	slot   uint8
	ex     exchange.StreamingExchange
	exCfg  config.Exchange
	runner *engine.Runner
	cancel context.CancelFunc
}

// New 构造 Supervisor，不创建交易所连接。调用 Attach 注入适配器。
func New(cfg *config.Config, st *store.Store, logs *logx.Buffer, log *slog.Logger) *Supervisor {
	if log == nil {
		log = slog.Default()
	}
	return &Supervisor{
		cfg:     cfg,
		store:   st,
		logs:    logs,
		log:     log,
		started: time.Now().UTC(),
		inst:    map[string]*instance{},
	}
}

// Attach 注册一个已构造好的交易所适配器。
func (s *Supervisor) Attach(exCfg config.Exchange, ex exchange.StreamingExchange, slot uint8) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inst[exCfg.Name] = &instance{name: exCfg.Name, slot: slot, ex: ex, exCfg: exCfg}
}

// Close 停止所有 Runner 并关闭交易所连接。
func (s *Supervisor) Close(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, inst := range s.inst {
		if inst.runner != nil && inst.runner.Status() != engine.StatusStopped {
			_ = inst.runner.Call(ctx, engine.CmdStop, nil)
		}
		if inst.cancel != nil {
			inst.cancel()
		}
		_ = inst.ex.Close()
	}
}

func (s *Supervisor) get(name string) (*instance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	inst, ok := s.inst[name]
	if !ok {
		return nil, fmt.Errorf("未知或不存在的交易所 %s", name)
	}
	return inst, nil
}

// SystemStatus 对应 GET /api/system/status。
func (s *Supervisor) SystemStatus() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	exs := make([]map[string]any, 0, len(s.inst))
	for _, inst := range s.inst {
		st := "idle"
		if inst.runner != nil {
			st = inst.runner.Status().String()
		}
		exs = append(exs, map[string]any{
			"name":   inst.name,
			"status": st,
		})
	}
	return map[string]any{
		"version":   "0.2.0",
		"uptime_s":  int(time.Since(s.started).Seconds()),
		"proxy":     s.cfg.Proxy.Enabled,
		"exchanges": exs,
	}
}

// Exchanges 返回已启用交易所列表。
func (s *Supervisor) Exchanges() []map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]map[string]any, 0, len(s.inst))
	for _, inst := range s.inst {
		caps := inst.ex.Capabilities()
		out = append(out, map[string]any{
			"name":         inst.name,
			"network":      inst.exCfg.Network,
			"capabilities": caps,
		})
	}
	return out
}

// Symbols 返回可交易永续列表。
func (s *Supervisor) Symbols(ctx context.Context, name string) ([]exchange.MarketInfo, error) {
	inst, err := s.get(name)
	if err != nil {
		return nil, err
	}
	return inst.ex.Markets(ctx)
}

// Klines 返回当前策略交易对（或指定 symbol）的 K 线。
func (s *Supervisor) Klines(ctx context.Context, name, symbol, interval string, limit int) ([]market.Kline, error) {
	inst, err := s.get(name)
	if err != nil {
		return nil, err
	}
	if symbol == "" {
		symbol = s.strategySymbol(name, "")
	}
	if symbol == "" {
		return nil, fmt.Errorf("必须指定交易对")
	}
	if interval == "" {
		interval = "1h"
	}
	return inst.ex.Klines(ctx, symbol, interval, limit)
}

// Preview 校验并计算派生量，不保存。
func (s *Supervisor) Preview(ctx context.Context, name string, raw []byte) (any, error) {
	inst, err := s.get(name)
	if err != nil {
		return nil, err
	}
	common, err := strategy.ParseCommon(raw)
	if err != nil {
		return nil, err
	}
	mkt, err := inst.ex.Market(ctx, common.Symbol)
	if err != nil {
		return nil, err
	}
	tick, err := inst.ex.Ticker(ctx, common.Symbol)
	if err != nil {
		return nil, err
	}
	acct, err := inst.ex.Account(ctx)
	if err != nil {
		return nil, err
	}
	mark := tick.Mark
	if !mark.IsPositive() {
		mark = tick.Book.Mid()
	}
	in := strategy.PreviewContext{
		Market:        mkt,
		Mark:          mark,
		Available:     acct.Available,
		MaxOpenOrders: inst.ex.Capabilities().MaxOpenOrders,
	}
	if pos, err := inst.ex.Position(ctx, common.Symbol); err == nil {
		in.Position = pos.Size
	}
	return strategy.RunPreview(strategy.NameOf(raw), raw, in)
}

// GetConfig 返回已保存的策略参数 JSON。
func (s *Supervisor) GetConfig(name string) (json.RawMessage, error) {
	if _, err := s.get(name); err != nil {
		return nil, err
	}
	c, ok, err := s.store.LoadConfig(name)
	if err != nil {
		return nil, err
	}
	if !ok {
		return json.RawMessage(`{}`), nil
	}
	return c.Params, nil
}

// PutConfig 校验并保存策略配置。运行中拒绝。
func (s *Supervisor) PutConfig(ctx context.Context, name string, raw []byte) (any, error) {
	inst, err := s.get(name)
	if err != nil {
		return nil, err
	}
	if inst.runner != nil {
		st := inst.runner.Status()
		if st == engine.StatusRunning || st == engine.StatusStarting {
			return nil, fmt.Errorf("实例运行中，不能修改配置")
		}
	}
	derived, err := s.Preview(ctx, name, raw)
	if err != nil {
		return nil, err
	}
	common, err := strategy.ParseCommon(raw)
	if err != nil {
		return nil, err
	}
	if err := s.store.SaveConfig(store.Config{
		Exchange:  name,
		Symbol:    common.Symbol,
		Strategy:  common.Strategy,
		Direction: common.Direction,
		Params:    json.RawMessage(append([]byte(nil), raw...)),
	}); err != nil {
		return nil, err
	}
	return derived, nil
}

// View 返回实例聚合视图。
func (s *Supervisor) View(name string) (engine.InstanceView, error) {
	inst, err := s.get(name)
	if err != nil {
		return engine.InstanceView{}, err
	}
	if inst.runner == nil {
		return engine.InstanceView{Exchange: name, Status: engine.StatusStopped.String()}, nil
	}
	return inst.runner.View(), nil
}

// Status 返回页面「账户状态」所需的视图，未运行时也补上账户与行情。
func (s *Supervisor) Status(ctx context.Context, name string) (engine.InstanceView, error) {
	view, err := s.View(name)
	if err != nil {
		return view, err
	}
	inst, err := s.get(name)
	if err != nil {
		return view, err
	}
	if acct, err := inst.ex.Account(ctx); err == nil {
		view.Account = acct
	}
	symbol := s.strategySymbol(name, view.Symbol)
	if symbol != "" {
		view.Symbol = symbol
	}
	if symbol == "" {
		return view, nil
	}
	if tick, err := inst.ex.Ticker(ctx, symbol); err == nil {
		if tick.Mark.IsPositive() {
			view.Mark = tick.Mark
		} else if tick.Book.Valid() {
			view.Mark = tick.Book.Mid()
		}
	}
	needPos := view.Status == engine.StatusStopped.String() || view.Status == engine.StatusError.String() || view.Position.Symbol != symbol
	if needPos {
		if pos, err := inst.ex.Position(ctx, symbol); err == nil {
			view.Position = pos
			view.Residual = !pos.IsFlat()
		}
	}
	return view, nil
}

// Proxy 返回代理配置的只读视图（不含密码）。
func (s *Supervisor) Proxy() map[string]any {
	p := s.cfg.Proxy
	return map[string]any{
		"enabled":  p.Enabled,
		"url":      p.URL,
		"no_proxy": p.NoProxy,
	}
}

// Trades 返回当前策略交易对的成交记录。
func (s *Supervisor) Trades(name string, limit int) ([]store.Fill, error) {
	if _, err := s.get(name); err != nil {
		return nil, err
	}
	symbol := ""
	if view, err := s.View(name); err == nil {
		symbol = view.Symbol
	}
	symbol = s.strategySymbol(name, symbol)
	return s.store.ListFills(name, symbol, limit)
}

func (s *Supervisor) strategySymbol(name, fallback string) string {
	if fallback != "" {
		return fallback
	}
	if c, ok, err := s.store.LoadConfig(name); err == nil && ok {
		return c.Symbol
	}
	return ""
}

// Logs 返回环形缓冲里的日志。
func (s *Supervisor) Logs(name, level string, limit int) []logx.Record {
	if s.logs == nil {
		return nil
	}
	return s.logs.List(name, level, limit)
}

// Start 启动网格。
func (s *Supervisor) Start(ctx context.Context, name string) (engine.InstanceView, error) {
	inst, err := s.get(name)
	if err != nil {
		return engine.InstanceView{}, err
	}
	c, ok, err := s.store.LoadConfig(name)
	if err != nil {
		return engine.InstanceView{}, err
	}
	if !ok {
		return engine.InstanceView{}, fmt.Errorf("尚未保存策略配置")
	}
	common, err := strategy.ParseCommon(c.Params)
	if err != nil {
		return engine.InstanceView{}, err
	}
	if err := s.ensureRunner(inst, common.Strategy, c.Params, false); err != nil {
		return engine.InstanceView{}, err
	}
	res := inst.runner.Call(ctx, engine.CmdStart, engine.StartPayload{
		Symbol: common.Symbol,
		Entry:  common.Entry,
		Risk:   common.Risk,
	})
	if !res.OK {
		return res.View, fmt.Errorf("%s", res.Message)
	}
	return res.View, nil
}

// Stop 停止策略：撤销本交易对挂单，保留仓位。
func (s *Supervisor) Stop(ctx context.Context, name string) (engine.InstanceView, error) {
	return s.command(ctx, name, engine.CmdStop, nil)
}

// CancelOrders 撤单保留持仓。
func (s *Supervisor) CancelOrders(ctx context.Context, name string) (engine.InstanceView, error) {
	return s.command(ctx, name, engine.CmdCancelOrders, nil)
}

// Refill 补齐挂单。
func (s *Supervisor) Refill(ctx context.Context, name string) (engine.InstanceView, error) {
	return s.command(ctx, name, engine.CmdRefill, nil)
}

// ResetStats 重置显示统计。
func (s *Supervisor) ResetStats(ctx context.Context, name string) (engine.InstanceView, error) {
	if err := s.store.ResetStats(name, time.Now().UTC()); err != nil {
		return engine.InstanceView{}, err
	}
	inst, err := s.get(name)
	if err != nil {
		return engine.InstanceView{}, err
	}
	if inst.runner == nil {
		return engine.InstanceView{Exchange: name, Status: engine.StatusStopped.String()}, nil
	}
	return s.command(ctx, name, engine.CmdResetStats, nil)
}

// Reconnect 重连交易所事件流。
func (s *Supervisor) Reconnect(ctx context.Context, name string) (engine.InstanceView, error) {
	return s.command(ctx, name, engine.CmdReconnect, nil)
}

// AdjustRange 调整区间。
func (s *Supervisor) AdjustRange(ctx context.Context, name string, req grid.AdjustRange) (engine.InstanceView, error) {
	return s.command(ctx, name, engine.CmdAdjustRange, req)
}

func (s *Supervisor) command(ctx context.Context, name string, kind engine.CommandKind, payload any) (engine.InstanceView, error) {
	inst, err := s.get(name)
	if err != nil {
		return engine.InstanceView{}, err
	}
	if inst.runner == nil {
		return engine.InstanceView{}, fmt.Errorf("实例尚未启动")
	}
	res := inst.runner.Call(ctx, kind, payload)
	if !res.OK {
		return res.View, fmt.Errorf("%s", res.Message)
	}
	return res.View, nil
}

func (s *Supervisor) ensureRunner(inst *instance, name string, params []byte, restore bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if inst.runner != nil {
		st := inst.runner.Status()
		if st == engine.StatusRunning || st == engine.StatusStarting {
			return nil
		}
	}
	if inst.cancel != nil {
		inst.cancel()
		inst.cancel = nil
	}
	if name == "" {
		name = strategy.NameOf(params)
	}
	strat, err := strategy.New(name, params)
	if err != nil {
		return err
	}
	if restore {
		if rt, ok, err := s.store.LoadRuntime(inst.name); err == nil && ok && len(rt.Snapshot) > 0 {
			if err := strat.Restore(rt.Snapshot); err != nil {
				s.log.Warn("restore snapshot failed", "exchange", inst.name, "err", err)
			}
		}
	}
	inst.runner = engine.New(inst.ex, strat, engine.Config{
		Name:             inst.name,
		Slot:             inst.slot,
		TickInterval:     s.cfg.App.TickInterval.Std(),
		WatchdogInterval: s.cfg.App.ReconcileInterval.Std(),
		MaxRetries:       inst.exCfg.MaxRetries,
		Log:              s.log,
		Persist:          persistAdapter{store: s.store},
	})
	ctx, cancel := context.WithCancel(context.Background())
	inst.cancel = cancel
	go func() { _ = inst.runner.Run(ctx) }()
	return nil
}

// LoadStrategyFiles 把各交易所 strategy_file 写入 SQLite。必须在实例未运行时调用。
//
// 仅在该交易所还没有已保存策略时作为初始模板；页面/API 已写入的配置不会被覆盖。
func (s *Supervisor) LoadStrategyFiles(ctx context.Context) error {
	for _, e := range s.cfg.EnabledExchanges() {
		if e.StrategyFile == "" {
			continue
		}
		if _, ok, err := s.store.LoadConfig(e.Name); err != nil {
			return fmt.Errorf("交易所 %s: 读取已保存配置失败: %w", e.Name, err)
		} else if ok {
			s.log.Info("strategy file skipped, saved config exists", "exchange", e.Name, "file", e.StrategyFile)
			continue
		}
		path := config.ResolvePath(e.StrategyFile)
		raw, err := config.LoadStrategyFile(path)
		if err != nil {
			return fmt.Errorf("交易所 %s: %w", e.Name, err)
		}
		if _, err := s.PutConfig(ctx, e.Name, raw); err != nil {
			return fmt.Errorf("交易所 %s: 应用策略文件 %s 失败: %w", e.Name, path, err)
		}
		s.log.Info("strategy file loaded", "exchange", e.Name, "file", path)
	}
	return nil
}

// Autostart 按配置启动尚未运行的实例。
func (s *Supervisor) Autostart(ctx context.Context) {
	for _, e := range s.cfg.EnabledExchanges() {
		if !e.Autostart {
			continue
		}
		inst, err := s.get(e.Name)
		if err != nil {
			s.log.Error("autostart skipped", "exchange", e.Name, "err", err)
			continue
		}
		if inst.runner != nil {
			st := inst.runner.Status()
			if st == engine.StatusRunning || st == engine.StatusStarting {
				continue
			}
		}
		s.log.Info("autostart", "exchange", e.Name)
		if _, err := s.Start(ctx, e.Name); err != nil {
			s.log.Error("autostart failed", "exchange", e.Name, "err", err)
		}
	}
}

// RestoreRunning 进程启动时把上次 running 的实例拉起来。
func (s *Supervisor) RestoreRunning(ctx context.Context) {
	s.mu.RLock()
	names := make([]string, 0, len(s.inst))
	for name := range s.inst {
		names = append(names, name)
	}
	s.mu.RUnlock()
	for _, name := range names {
		rt, ok, err := s.store.LoadRuntime(name)
		if err != nil || !ok || (rt.Status != "running" && rt.Status != "starting" && rt.Status != "paused") {
			continue
		}
		s.log.Info("restoring instance", "exchange", name, "status", rt.Status)
		if _, err := s.Start(ctx, name); err != nil {
			s.log.Error("restore failed", "exchange", name, "err", err)
		}
	}
}

type persistAdapter struct{ store *store.Store }

func (p persistAdapter) SaveRuntime(exchange, status, reason string, epoch uint16, snapshot []byte) error {
	return p.store.SaveRuntime(store.Runtime{
		Exchange:   exchange,
		Status:     status,
		StopReason: reason,
		Epoch:      epoch,
		Snapshot:   snapshot,
	})
}

func (p persistAdapter) RecordFill(exchange string, o order.Order) error {
	if !o.FilledQty.IsPositive() {
		return nil
	}
	return p.store.RecordOrderFill(exchange, o)
}
