// exchange_sidecar：统一 Lighter + SoDEX + Entropy 实盘层（对齐 internal/exchange/）。
// Rust dex-arbitr 经 stdin/stdout 长连接调用，无需 Python。
//
// ── 协议（JSON-lines，一行一条消息）──
//
// 请求（Rust → sidecar）：
//
//	{"id":1,"cmd":"account|place|cancel|order_status|watch","venue_yaml":"...","params":{...}}
//
// 响应（sidecar → Rust）：
//
//	{"id":1,"ok":true,"data":{...}}
//
// 主动推送（sidecar → Rust，无 id，私有 WS 订单流）：
//
//	{"push":"order","venue":"lighter","data":{...}}
//
// ── 为什么是常驻进程 ──
// 旧实现每次调用起一个进程：光进程启动 33ms，加上全新 TLS 握手、重新签名
// 认证，单次调用 200~800ms。第一腿成交检测靠轮询，每轮都要付这个开销，
// 导致「第一腿成交 → 第二腿下单」窗口远超 1 秒。常驻后连接、认证、
// 市场元数据全部复用，单次调用降到几十 ms。
//
// 构建：cd scripts/exchange_sidecar && go build -o exchange_sidecar .
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shopspring/decimal"
	"gopkg.in/yaml.v3"
)

// 市价 / IOC 成交确认：等该单 WS；没有就 REST 查一次该单；仍没有量当失败。
// 窗口默认 1 秒，可由 place params.fill_wait_ms 覆盖（Rust order.ioc_fill_wait_ms）。
const (
	iocFillWait    = time.Second
	marketFillPoll = 2 * time.Second
	// 参考在最后一轮把休眠夹到 deadline：`min(safe_interval,
	// max(0.5, deadline - now))`。0.5s 是它的下限。
	minFillPoll = 500 * time.Millisecond
	// 成交缓存超过这么久就丢掉，避免长驻进程无限涨。
	fillCacheTTL = time.Hour
	// stdin 请求并发上限。满了直接报 busy，不堵 scanner（撤单还要读进来）。
	maxInflightRequests = 32
	nonceFetchTimeout   = 5 * time.Second
)

// 同毫秒多笔下单（邻档 plus/minus）不能共用一个 client_order_id。
// Lighter 的 ClientOrderIndex 上限是 2^48-1；毫秒×1000 会超（约 1.76e15）。
const lighterMaxClientOrderIndex int64 = 281474976710655

var clientOrderSeq atomic.Int64

func nextLighterClientOrderID() int64 {
	n := clientOrderSeq.Add(1)
	ms := time.Now().UnixMilli()
	if ms < 0 {
		ms = 0
	}
	// 毫秒约 1.76e12，×100 后约 1.76e14，仍低于 2^48-1；末两位区分同毫秒多单。
	id := ms*100 + n%100
	if id <= 0 || id > lighterMaxClientOrderIndex {
		id = n % lighterMaxClientOrderIndex
		if id <= 0 {
			id = 1
		}
	}
	return id
}

func nextArbClientOrderID() string {
	n := clientOrderSeq.Add(1)
	return fmt.Sprintf("arb-%d-%d", time.Now().UnixMilli(), n)
}

// fillPollSleep 复刻参考的 `await asyncio.sleep(min(safe_interval,
// max(0.5, deadline - loop.time())))`：最后一轮不睡过 deadline，
// 保证窗口末尾还能再查一次，而不是白等一个完整间隔后直接超时。
func fillPollSleep(deadline time.Time) {
	remain := time.Until(deadline)
	if remain <= 0 {
		return
	}
	nap := marketFillPoll
	if remain < nap {
		nap = remain
	}
	if nap < minFillPoll {
		nap = minFillPoll
	}
	time.Sleep(nap)
}

// 事件驱动路径上的密轮询：WS 没带到成交量时，REST 兜底也不要再睡 2s。
func tightFillPoll(deadline time.Time) {
	remain := time.Until(deadline)
	if remain <= 0 {
		return
	}
	nap := 80 * time.Millisecond
	if remain < nap {
		nap = remain
	}
	time.Sleep(nap)
}

// 资金费率的结算周期（秒）。两个所都是**小时结算**，返回的费率也都已经是
// 「每小时实收值」，所以上层年化统一乘 24 × 365。
//
// 这里明确写出周期而不是让 Rust 侧假定，是因为参考项目把 8 小时结算硬编成了
// `× 3 × 365`。一旦哪个所改成 4h/8h，费率数值本身看不出变化，只有这个字段会
// 变——上层拿它算，改周期就不会静默算错年化。
//
// SoDEX 由 `/markets/symbols` 的 `fundingInterval` 给出（实测 88 个市场全是
// 3600）；Lighter 没有对应字段，文档写明按小时结算，取常量。
const (
	lighterFundingInterval       = 3600
	entropyFundingInterval       = 3600
	sodexFundingIntervalFallback = 3600
)

type request struct {
	ID        int64          `json:"id"`
	Cmd       string         `json:"cmd"`
	VenueYAML string         `json:"venue_yaml"`
	Params    map[string]any `json:"params"`
}

type response struct {
	ID    int64  `json:"id"`
	OK    bool   `json:"ok"`
	Data  any    `json:"data,omitempty"`
	Error string `json:"error,omitempty"`
}

// push 是 sidecar 主动下发的消息（私有 WS 订单流）。没有 id。
type push struct {
	Push  string `json:"push"`
	Venue string `json:"venue"`
	Data  any    `json:"data"`
}

// 单次请求处理超时 = 成交确认窗口 + 15s（下单往返和签名）。
// place 带 fill_wait_ms 时按该窗口加长，避免 ctx 先到期打断回查。
func handlerTimeout(req request) time.Duration {
	const extra = 15 * time.Second
	if req.Cmd == "place" {
		return fillWaitOf(req.Params) + extra
	}
	return iocFillWait + extra
}

// fillWaitOf 读 place 的 fill_wait_ms；缺省或 0 用 1 秒。夹在 100ms–30s。
func fillWaitOf(params map[string]any) time.Duration {
	if params == nil {
		return iocFillWait
	}
	n, err := paramUint64(params, "fill_wait_ms")
	if err != nil || n == 0 {
		return iocFillWait
	}
	if n < 100 {
		n = 100
	}
	if n > 30_000 {
		n = 30_000
	}
	return time.Duration(n) * time.Millisecond
}

var (
	// stdout 必须串行写：响应和 WS 推送来自不同 goroutine。
	outMu  sync.Mutex
	outEnc = json.NewEncoder(os.Stdout)
)

func writeJSON(v any) {
	outMu.Lock()
	defer outMu.Unlock()
	_ = outEnc.Encode(v)
}

func respond(id int64, ok bool, data any, errMsg string) {
	writeJSON(response{ID: id, OK: ok, Data: data, Error: errMsg})
}

// emitPush 供 WS 回调使用，把订单更新推给 Rust。
func emitPush(venue string, data any) {
	writeJSON(push{Push: "order", Venue: venue, Data: data})
}

func main() {
	reg := newRegistry()
	defer reg.closeAll()

	scanner := bufio.NewScanner(os.Stdin)
	// 订单簿快照可能很大，放宽单行上限（默认 64KB）。
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)

	// stdin 关闭时（父进程退出或管道用完）必须等 in-flight 请求写完响应，
	// 否则最后几条请求会静默丢失——尤其是管道式调用，EOF 来得比处理快。
	var inflight sync.WaitGroup
	slots := make(chan struct{}, maxInflightRequests)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var req request
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			respond(0, false, nil, "invalid json: "+err.Error())
			continue
		}
		select {
		case slots <- struct{}{}:
		default:
			respond(req.ID, false, nil, "sidecar busy: too many in-flight requests")
			continue
		}
		// 每条请求独立 goroutine：一次慢的 place 不能挡住后续的 order_status。
		// 会话内部各自加锁（nonce、WS 写）保证并发安全。
		inflight.Add(1)
		go func(req request) {
			defer inflight.Done()
			defer func() { <-slots }()
			defer func() {
				if rec := recover(); rec != nil {
					fmt.Fprintf(os.Stderr, "sidecar panic id=%d cmd=%s: %v\n%s\n", req.ID, req.Cmd, rec, debug.Stack())
					respond(req.ID, false, nil, fmt.Sprintf("internal panic: %v", rec))
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), handlerTimeout(req))
			defer cancel()
			data, err := dispatch(ctx, reg, req)
			if err != nil {
				respond(req.ID, false, nil, err.Error())
				return
			}
			respond(req.ID, true, data, "")
		}(req)
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "stdin scanner: %v\n", err)
		inflight.Wait()
		os.Exit(1)
	}
	inflight.Wait()
}

func pruneFillCache[K comparable, V any](m map[K]V, at func(V) time.Time) {
	if len(m) == 0 {
		return
	}
	cutoff := time.Now().Add(-fillCacheTTL)
	for k, v := range m {
		if at(v).Before(cutoff) {
			delete(m, k)
		}
	}
}

func dispatch(ctx context.Context, reg *registry, req request) (any, error) {
	id, err := peekVenueID(req.VenueYAML)
	if err != nil {
		return nil, err
	}
	switch id {
	case "lighter", "lighter_rh":
		return dispatchLighter(ctx, reg, id, req)
	case "sodex":
		return dispatchSodex(ctx, reg, req)
	case "entropy":
		return dispatchEntropy(ctx, reg, req)
	default:
		return nil, fmt.Errorf("unsupported venue %q", id)
	}
}

// registry 持有各 venue 的长存会话。常驻进程的核心：连接、认证、
// 市场元数据都只建一次。
type registry struct {
	mu      sync.Mutex
	lighter map[string]*lighterSession
	sodex   *sodexSession
	entropy *entropySession
}

func newRegistry() *registry {
	return &registry{lighter: make(map[string]*lighterSession)}
}

func (r *registry) closeAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range r.lighter {
		s.close()
	}
	if r.sodex != nil {
		r.sodex.close()
	}
	if r.entropy != nil {
		r.entropy.close()
	}
}

func peekVenueID(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	var meta struct {
		ID string `yaml:"id"`
	}
	if err := yaml.Unmarshal(b, &meta); err != nil {
		return "", err
	}
	id := strings.TrimSpace(meta.ID)
	if id == "" {
		return "", fmt.Errorf("venue id missing in %s", path)
	}
	return id, nil
}

func fillPnlResult(pnl decimal.Decimal, perFill, found bool) map[string]any {
	return map[string]any{
		"realized_pnl": pnl.String(),
		"per_fill":     perFill,
		"found":        found,
	}
}

func symbolMatch(have, want string) bool {
	h := strings.ToUpper(strings.TrimSpace(have))
	w := strings.ToUpper(strings.TrimSpace(want))
	if h == "" || w == "" {
		return false
	}
	if h == w {
		return true
	}
	return strings.HasSuffix(h, ":"+w) || strings.HasSuffix(h, "-"+w) || strings.HasPrefix(h, w+"-")
}
