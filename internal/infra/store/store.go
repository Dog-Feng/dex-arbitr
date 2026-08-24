// Package store 用 SQLite 保存策略配置、运行快照与成交记录。
//
// 密钥不进库。策略参数以 JSON 原文落盘，由各策略自己解析。
package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dex-grid/internal/domain/order"

	"github.com/shopspring/decimal"
	_ "modernc.org/sqlite"
)

// Store 是进程内唯一的数据库句柄。
type Store struct {
	db *sql.DB
}

// Config 是页面下发的策略配置，每个交易所一行。
type Config struct {
	Exchange  string
	Symbol    string
	Strategy  string
	Direction string
	Params    json.RawMessage
	UpdatedAt time.Time
}

// Runtime 是实例运行快照。
type Runtime struct {
	Exchange   string
	Status     string
	StopReason string
	Epoch      uint16
	Snapshot   []byte
	UpdatedAt  time.Time
}

// Fill 是一笔成交，供页面成交记录表使用。
type Fill struct {
	ID       int64               `json:"id"`
	Exchange string              `json:"exchange"`
	Symbol   string              `json:"symbol,omitempty"`
	COID     order.ClientOrderID `json:"coid"`
	Side     string              `json:"side"`
	Price    string              `json:"price"`
	Qty      string              `json:"qty"`
	Fee      string              `json:"fee"`
	IsMaker  bool                `json:"is_maker"`
	Time     time.Time           `json:"ts"`
}

// Open 打开（或创建）数据库文件，并执行 schema 迁移。
func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("store: 创建数据目录失败: %w", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("store: 打开 %s 失败: %w", path, err)
	}
	db.SetMaxOpenConns(1) // SQLite 单写者，省掉锁争用
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA busy_timeout=5000;",
		"PRAGMA foreign_keys=ON;",
		"PRAGMA synchronous=NORMAL;",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("store: %s: %w", pragma, err)
		}
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	if err := s.migrateFillsSymbol(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS strategy_configs (
    exchange    TEXT PRIMARY KEY,
    symbol      TEXT NOT NULL,
    strategy    TEXT NOT NULL,
    direction   TEXT NOT NULL,
    params      TEXT NOT NULL,
    updated_at  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS runtime_state (
    exchange    TEXT PRIMARY KEY,
    status      TEXT NOT NULL,
    stop_reason TEXT,
    epoch       INTEGER NOT NULL,
    snapshot    BLOB,
    updated_at  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS orders (
    exchange     TEXT NOT NULL,
    coid         INTEGER NOT NULL,
    exchange_oid TEXT,
    epoch        INTEGER NOT NULL,
    level        INTEGER NOT NULL,
    side         TEXT NOT NULL,
    price        TEXT NOT NULL,
    qty          TEXT NOT NULL,
    filled_qty   TEXT NOT NULL,
    state        TEXT NOT NULL,
    updated_at   INTEGER NOT NULL,
    PRIMARY KEY (exchange, coid)
);
CREATE TABLE IF NOT EXISTS fills (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    exchange   TEXT NOT NULL,
    coid       INTEGER NOT NULL,
    side       TEXT NOT NULL,
    price      TEXT NOT NULL,
    qty        TEXT NOT NULL,
    fee        TEXT NOT NULL,
    is_maker   INTEGER NOT NULL,
    ts         INTEGER NOT NULL,
    symbol     TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_fills_ex_ts ON fills(exchange, ts DESC);
CREATE TABLE IF NOT EXISTS stats (
    exchange        TEXT PRIMARY KEY,
    reset_at        INTEGER NOT NULL,
    realized_pnl    TEXT NOT NULL,
    completed_grids INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);
`)
	return err
}

// migrateFillsSymbol 给已有库补上成交表的交易对列。
func (s *Store) migrateFillsSymbol() error {
	_, err := s.db.Exec(`ALTER TABLE fills ADD COLUMN symbol TEXT NOT NULL DEFAULT ''`)
	if err != nil {
		msg := strings.ToLower(err.Error())
		if !strings.Contains(msg, "duplicate column") {
			return err
		}
	}
	_, err = s.db.Exec(`CREATE INDEX IF NOT EXISTS idx_fills_ex_symbol_ts ON fills(exchange, symbol, ts DESC)`)
	return err
}

// SaveConfig 覆盖写入策略配置。
func (s *Store) SaveConfig(c Config) error {
	if c.UpdatedAt.IsZero() {
		c.UpdatedAt = time.Now().UTC()
	}
	_, err := s.db.Exec(`
INSERT INTO strategy_configs(exchange, symbol, strategy, direction, params, updated_at)
VALUES(?, ?, ?, ?, ?, ?)
ON CONFLICT(exchange) DO UPDATE SET
    symbol=excluded.symbol,
    strategy=excluded.strategy,
    direction=excluded.direction,
    params=excluded.params,
    updated_at=excluded.updated_at`,
		c.Exchange, c.Symbol, c.Strategy, c.Direction, string(c.Params), c.UpdatedAt.Unix())
	return err
}

// LoadConfig 读取某个交易所的策略配置。
func (s *Store) LoadConfig(exchange string) (Config, bool, error) {
	var c Config
	var params string
	var ts int64
	err := s.db.QueryRow(`
SELECT exchange, symbol, strategy, direction, params, updated_at
FROM strategy_configs WHERE exchange=?`, exchange).
		Scan(&c.Exchange, &c.Symbol, &c.Strategy, &c.Direction, &params, &ts)
	if err == sql.ErrNoRows {
		return Config{}, false, nil
	}
	if err != nil {
		return Config{}, false, err
	}
	c.Params = json.RawMessage(params)
	c.UpdatedAt = time.Unix(ts, 0).UTC()
	return c, true, nil
}

// SaveRuntime 覆盖写入运行快照。
func (s *Store) SaveRuntime(r Runtime) error {
	if r.UpdatedAt.IsZero() {
		r.UpdatedAt = time.Now().UTC()
	}
	_, err := s.db.Exec(`
INSERT INTO runtime_state(exchange, status, stop_reason, epoch, snapshot, updated_at)
VALUES(?, ?, ?, ?, ?, ?)
ON CONFLICT(exchange) DO UPDATE SET
    status=excluded.status,
    stop_reason=excluded.stop_reason,
    epoch=excluded.epoch,
    snapshot=excluded.snapshot,
    updated_at=excluded.updated_at`,
		r.Exchange, r.Status, r.StopReason, r.Epoch, r.Snapshot, r.UpdatedAt.Unix())
	return err
}

// LoadRuntime 读取运行快照。
func (s *Store) LoadRuntime(exchange string) (Runtime, bool, error) {
	var r Runtime
	var ts int64
	var reason sql.NullString
	err := s.db.QueryRow(`
SELECT exchange, status, stop_reason, epoch, snapshot, updated_at
FROM runtime_state WHERE exchange=?`, exchange).
		Scan(&r.Exchange, &r.Status, &reason, &r.Epoch, &r.Snapshot, &ts)
	if err == sql.ErrNoRows {
		return Runtime{}, false, nil
	}
	if err != nil {
		return Runtime{}, false, err
	}
	r.StopReason = reason.String
	r.UpdatedAt = time.Unix(ts, 0).UTC()
	return r, true, nil
}

// InsertFill 追加一笔成交。
func (s *Store) InsertFill(f Fill) error {
	if f.Time.IsZero() {
		f.Time = time.Now().UTC()
	}
	maker := 0
	if f.IsMaker {
		maker = 1
	}
	_, err := s.db.Exec(`
INSERT INTO fills(exchange, symbol, coid, side, price, qty, fee, is_maker, ts)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		f.Exchange, f.Symbol, int64(f.COID), f.Side, f.Price, f.Qty, f.Fee, maker, f.Time.Unix())
	return err
}

// RecordOrderFill 把订单回报里的累计成交拆成增量再落库。
// 一张单吃多档会多次推送 filled_base_amount，直接存累计值会把同一笔量记多遍。
func (s *Store) RecordOrderFill(exchange string, o order.Order) error {
	if !o.FilledQty.IsPositive() || !o.ClientOrderID.Valid() {
		return nil
	}
	prevQty, prevNotional, prevFee, err := s.fillProgress(exchange, o.ClientOrderID)
	if err != nil {
		return err
	}
	deltaQty := o.FilledQty.Sub(prevQty)
	if deltaQty.LessThanOrEqual(decimal.Zero) {
		return nil
	}
	cumNotional := o.FillPrice().Mul(o.FilledQty)
	deltaNotional := cumNotional.Sub(prevNotional)
	if !deltaNotional.IsPositive() {
		deltaNotional = o.FillPrice().Mul(deltaQty)
	}
	deltaPx := deltaNotional.Div(deltaQty)
	deltaFee := o.Fee.Sub(prevFee)
	if deltaFee.IsNegative() {
		deltaFee = decimal.Zero
	}
	return s.InsertFill(Fill{
		Exchange: exchange,
		Symbol:   o.Symbol,
		COID:     o.ClientOrderID,
		Side:     o.Side.String(),
		Price:    deltaPx.String(),
		Qty:      deltaQty.String(),
		Fee:      deltaFee.String(),
		IsMaker:  o.IsMaker,
		Time:     o.UpdatedAt,
	})
}

func (s *Store) fillProgress(exchange string, coid order.ClientOrderID) (qty, notional, fee decimal.Decimal, err error) {
	rows, err := s.db.Query(`SELECT qty, price, fee FROM fills WHERE exchange=? AND coid=?`, exchange, int64(coid))
	if err != nil {
		return decimal.Zero, decimal.Zero, decimal.Zero, err
	}
	defer rows.Close()
	for rows.Next() {
		var qStr, pStr, fStr string
		if err := rows.Scan(&qStr, &pStr, &fStr); err != nil {
			return decimal.Zero, decimal.Zero, decimal.Zero, err
		}
		q, err := decimal.NewFromString(qStr)
		if err != nil {
			continue
		}
		p, _ := decimal.NewFromString(pStr)
		f, _ := decimal.NewFromString(fStr)
		qty = qty.Add(q)
		notional = notional.Add(q.Mul(p))
		fee = fee.Add(f)
	}
	return qty, notional, fee, rows.Err()
}

// ListFills 返回重置统计时间点之后的成交，最新在前。
// symbol 非空时只返回该交易对的成交，对应页面「当前策略配置的交易对」。
func (s *Store) ListFills(exchange, symbol string, limit int) ([]Fill, error) {
	if limit <= 0 {
		limit = 50
	}
	resetAt := int64(0)
	_ = s.db.QueryRow(`SELECT reset_at FROM stats WHERE exchange=?`, exchange).Scan(&resetAt)

	q := `
SELECT id, exchange, symbol, coid, side, price, qty, fee, is_maker, ts
FROM fills WHERE exchange=? AND ts>=?`
	args := []any{exchange, resetAt}
	if symbol != "" {
		q += ` AND (symbol=? OR symbol='')`
		args = append(args, symbol)
	}
	q += ` ORDER BY ts DESC, id DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Fill
	for rows.Next() {
		var f Fill
		var coid int64
		var maker int
		var ts int64
		if err := rows.Scan(&f.ID, &f.Exchange, &f.Symbol, &coid, &f.Side, &f.Price, &f.Qty, &f.Fee, &maker, &ts); err != nil {
			return nil, err
		}
		f.COID = order.ClientOrderID(coid)
		f.IsMaker = maker != 0
		f.Time = time.Unix(ts, 0).UTC()
		out = append(out, f)
	}
	return out, rows.Err()
}

// ResetStats 把显示用统计清零，成交明细保留。
func (s *Store) ResetStats(exchange string, now time.Time) error {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	_, err := s.db.Exec(`
INSERT INTO stats(exchange, reset_at, realized_pnl, completed_grids, updated_at)
VALUES(?, ?, '0', 0, ?)
ON CONFLICT(exchange) DO UPDATE SET
    reset_at=excluded.reset_at,
    realized_pnl='0',
    completed_grids=0,
    updated_at=excluded.updated_at`,
		exchange, now.Unix(), now.Unix())
	return err
}
