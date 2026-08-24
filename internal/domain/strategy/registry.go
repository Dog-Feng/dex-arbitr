package strategy

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"dex-grid/internal/domain/market"

	"github.com/shopspring/decimal"
)

// Factory 从策略参数构造一个策略实例。
//
// params 是从数据库读出的策略专有参数（JSON 反序列化后的原始字节），
// 由各策略自行解析，这样新增策略不需要改动应用层。
type Factory func(params []byte) (Strategy, error)

// PreviewContext 是各策略 Preview 的共用入参。
type PreviewContext struct {
	Market        market.Market
	Mark          decimal.Decimal
	Available     decimal.Decimal
	Position      decimal.Decimal
	MaxOpenOrders int
}

// PreviewFunc 把原始 JSON 参数变成页面派生量。
type PreviewFunc func(params []byte, in PreviewContext) (any, error)

var (
	mu        sync.RWMutex
	factories = map[string]Factory{}
	previews  = map[string]PreviewFunc{}
)

// Register 注册一个策略。重复注册会 panic，因为这只会是编译期的编码错误。
func Register(name string, f Factory) {
	mu.Lock()
	defer mu.Unlock()
	if _, dup := factories[name]; dup {
		panic(fmt.Sprintf("strategy: duplicate registration of %q", name))
	}
	factories[name] = f
}

// RegisterPreview 注册策略的派生量计算。重复注册会 panic。
func RegisterPreview(name string, f PreviewFunc) {
	mu.Lock()
	defer mu.Unlock()
	if _, dup := previews[name]; dup {
		panic(fmt.Sprintf("strategy: duplicate preview of %q", name))
	}
	previews[name] = f
}

// RunPreview 按名称计算派生量。
func RunPreview(name string, params []byte, in PreviewContext) (any, error) {
	mu.RLock()
	f, ok := previews[name]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("strategy: unknown strategy %q", name)
	}
	return f(params, in)
}

// NameOf 从参数 JSON 读出策略名；缺省为普通网格。
func NameOf(params []byte) string {
	var e struct {
		Strategy string `json:"strategy"`
	}
	if len(params) > 0 {
		_ = json.Unmarshal(params, &e)
	}
	if e.Strategy == "" {
		return "grid"
	}
	return e.Strategy
}

// Common 是各策略 JSON 里共用的字段，供 Supervisor 启停时读取。
type Common struct {
	Strategy  string      `json:"strategy"`
	Symbol    string      `json:"symbol"`
	Direction string      `json:"direction"`
	Entry     EntryParams `json:"entry"`
	Risk      RiskParams  `json:"risk"`
}

// ParseCommon 抽出共用字段并补默认值。
func ParseCommon(raw []byte) (Common, error) {
	var c Common
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &c); err != nil {
			return c, fmt.Errorf("策略参数无法解析: %w", err)
		}
	}
	if c.Strategy == "" {
		c.Strategy = "grid"
	}
	if c.Symbol == "" {
		return c, fmt.Errorf("必须指定交易对")
	}
	c.Entry = mergeEntry(c.Entry)
	c.Risk = mergeRisk(c.Risk)
	return c, nil
}

func mergeEntry(p EntryParams) EntryParams {
	d := DefaultEntryParams()
	if p.Mode == 0 && p.Timeout == 0 && p.SliceCount == 0 {
		return d
	}
	if p.Timeout == 0 {
		p.Timeout = d.Timeout
	}
	if p.FillTolerance.IsZero() {
		p.FillTolerance = d.FillTolerance
	}
	if p.SliceCount <= 0 {
		p.SliceCount = d.SliceCount
	}
	if p.SliceInterval == 0 {
		p.SliceInterval = d.SliceInterval
	}
	if p.MaxSlippage.IsZero() {
		p.MaxSlippage = d.MaxSlippage
	}
	if p.DepthLevel <= 0 {
		p.DepthLevel = d.DepthLevel
	}
	if p.RepriceTicks <= 0 {
		p.RepriceTicks = d.RepriceTicks
	}
	if p.RepriceInterval == 0 {
		p.RepriceInterval = d.RepriceInterval
	}
	return p
}

func mergeRisk(p RiskParams) RiskParams {
	d := DefaultRiskParams()
	if p.MaxConsecutiveErrors <= 0 {
		p.MaxConsecutiveErrors = d.MaxConsecutiveErrors
	}
	if p.StaleTimeout == 0 {
		p.StaleTimeout = d.StaleTimeout
	}
	if p.ResumeConfirm == 0 {
		p.ResumeConfirm = d.ResumeConfirm
	}
	if p.CloseOnStop == nil {
		p.CloseOnStop = d.CloseOnStop
	}
	if p.PriceSource == 0 {
		p.PriceSource = d.PriceSource
	}
	return p
}

// New 按名称构造策略。
func New(name string, params []byte) (Strategy, error) {
	mu.RLock()
	f, ok := factories[name]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("strategy: unknown strategy %q", name)
	}
	return f(params)
}

// Names 返回已注册的策略名，按字典序排列。
func Names() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(factories))
	for name := range factories {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
