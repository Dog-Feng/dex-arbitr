package exchange

import (
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"sync"

	"dex-grid/internal/config"
)

// Deps 是应用层注入给适配器的公共依赖。
//
// HTTP 客户端由应用层按全局代理配置构造后传入，适配器不必关心代理从哪来。
type Deps struct {
	Log  *slog.Logger
	HTTP *http.Client
}

// Factory 从配置构造一个交易所适配器。
type Factory func(cfg config.Exchange, deps Deps) (Exchange, error)

type entry struct {
	factory Factory
	// slot 是交易所在注册表中的固定索引，编码进 ClientOrderID。
	// 注册顺序一旦确定就不能调整，否则历史订单号会解码到错误的交易所。
	slot uint8
}

var (
	mu       sync.RWMutex
	registry = map[string]entry{}
	order_   []string
)

// Register 注册一个交易所。只能追加，不能插到中间。
func Register(name string, f Factory) {
	mu.Lock()
	defer mu.Unlock()
	if _, dup := registry[name]; dup {
		panic(fmt.Sprintf("exchange: duplicate registration of %q", name))
	}
	if len(order_) > 254 {
		panic("exchange: too many exchanges registered (slot is 8 bits, 255 reserved)")
	}
	registry[name] = entry{factory: f, slot: uint8(len(order_))}
	order_ = append(order_, name)
}

// Slot 返回交易所的槽位索引。
func Slot(name string) (uint8, error) {
	mu.RLock()
	defer mu.RUnlock()
	e, ok := registry[name]
	if !ok {
		return 0, fmt.Errorf("exchange: unknown exchange %q", name)
	}
	return e.slot, nil
}

// New 按配置构造交易所适配器。
func New(cfg config.Exchange, deps Deps) (Exchange, error) {
	mu.RLock()
	e, ok := registry[cfg.Name]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("exchange: unknown exchange %q", cfg.Name)
	}
	if deps.Log == nil {
		deps.Log = slog.Default()
	}
	if deps.HTTP == nil {
		deps.HTTP = http.DefaultClient
	}
	return e.factory(cfg, deps)
}

// Names 返回已注册的交易所名，按字典序排列。
func Names() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(registry))
	for name := range registry {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
