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
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// 市价单（IOC）成交确认的等待窗口与轮询间隔，逐项对齐参考项目。
//
// 下单响应不带可信成交量，必须回查，且单次查询不够——撮合与查询接口之间
// 有延迟。参考 `_wait_for_order_fill_rest`（order_monitor.go 对应的 py
// 文件 :356）就是这么轮询的，参数取自它的签名和 order_execution 配置：
//
//	poll_interval = 2.0，safe_interval = max(1.0, poll_interval)  → 2s
//	limit_order_timeout: 3            → 非 Lighter 市价腿的窗口
//	lighter_market_order_timeout: 60  → Lighter 市价腿专用窗口
//
// 窗口按所区分不是随意加的：参考里 `lighter_retry_mode` 只在
// `exchange_name == "lighter"` 时置位（order_strategy_executor.py:459），
// 命中才用 60s，否则一律回落 `limit_order_timeout`。Lighter 需要长窗口是
// 因为它的成交确认最慢——我们这边同样得靠持仓 delta 回查，序列器收下
// 交易到持仓快照更新之间的延迟不可控。
const (
	sodexFillWait = 3 * time.Second
	// Entropy/HL 的 orderStatus 能看到已终结单，窗口对齐 SoDEX。
	entropyFillWait = 3 * time.Second
	lighterFillWait = 60 * time.Second
	marketFillPoll  = 2 * time.Second
	// 参考在最后一轮把休眠夹到 deadline：`min(safe_interval,
	// max(0.5, deadline - now))`。0.5s 是它的下限。
	minFillPoll = 500 * time.Millisecond
)

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

// 单次请求的处理超时。必须留在 lighterFillWait 之上：place 会在进程内
// 阻塞轮询整个成交确认窗口，ctx 先到期的话回查会被打断，成交量查不到就
// 白白退化成 unknown。留 15s 余量给下单本身的往返和签名。
const requestTimeout = lighterFillWait + 15*time.Second

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
		// 每条请求独立 goroutine：一次慢的 place 不能挡住后续的 order_status。
		// 会话内部各自加锁（nonce、WS 写）保证并发安全。
		inflight.Add(1)
		go func(req request) {
			defer inflight.Done()
			ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
			defer cancel()
			data, err := dispatch(ctx, reg, req)
			if err != nil {
				respond(req.ID, false, nil, err.Error())
				return
			}
			respond(req.ID, true, data, "")
		}(req)
	}
	inflight.Wait()
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
