package rhlighter

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"dex-grid/internal/exchange"
)

// classify 把 Lighter 的错误归入统一的分类。
//
// 交易所没有稳定的错误码体系（同一类问题在不同端点上的 code 不一致），
// 所以这里按 HTTP 状态码 + 错误文案关键字来判断。文案匹配是脆弱的，
// 因此兜底策略偏保守：认不出来的一律算 ClassUnknown，由上层计入连续失败，
// 而不是乐观地当成可重试导致无限重试。
//
// 注意 post-only 被拒【不会】走到这里。主网实测：会立即成交的 post-only 单
// 提交时 sendTx 照样返回 200 + tx_hash，是排序器随后拒绝的，最终以
// "canceled-post-only" 这个订单状态从 WebSocket 推回来。所以下面的
// post-only 关键字只是兜底，真正的判定在 stateFromStatus 里。
func classify(op string, err error) error {
	if err == nil {
		return nil
	}
	return exchange.Classify(classOf(err), op, err)
}

func classOf(err error) exchange.ErrorClass {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return exchange.ClassRetryable
	}

	var te *transportError
	if errors.As(err, &te) {
		return exchange.ClassRetryable
	}

	var ae *apiError
	if !errors.As(err, &ae) {
		return matchMessage(err.Error())
	}

	// 5xx 与 429 无条件可重试，跟文案无关。
	if ae.Status >= 500 || ae.Status == http.StatusTooManyRequests {
		return exchange.ClassRetryable
	}
	if c := matchMessage(ae.Message); c != exchange.ClassUnknown {
		return c
	}
	if ae.Status == http.StatusUnauthorized || ae.Status == http.StatusForbidden {
		return exchange.ClassFatal
	}
	if ae.Status >= 400 && ae.Status < 500 {
		return exchange.ClassInvalidParam
	}
	return exchange.ClassUnknown
}

// 错误文案关键字。顺序有意义：先匹配更具体的模式。
var messagePatterns = []struct {
	class    exchange.ErrorClass
	keywords []string
}{
	{exchange.ClassPostOnlyRejected, []string{
		"post only", "post-only", "postonly",
		"would match", "immediately match", "would cross", "crosses the book",
		"maker order would", "taker order not allowed",
	}},
	{exchange.ClassInsufficientMargin, []string{
		"insufficient", "not enough", "exceeds available", "margin requirement",
		"collateral", "below maintenance",
	}},
	{exchange.ClassRetryable, []string{
		"nonce", "rate limit", "too many requests", "timeout", "timed out",
		"temporarily", "try again", "sequencer is", "busy",
	}},
	{exchange.ClassFatal, []string{
		"signature", "unauthorized", "invalid api key", "auth token", "forbidden",
	}},
	{exchange.ClassInvalidParam, []string{
		"invalid", "must be", "out of range", "not found", "unknown market",
		"reduce only", "reduce-only", "min ", "max ",
	}},
}

func matchMessage(msg string) exchange.ErrorClass {
	lower := strings.ToLower(msg)
	for _, p := range messagePatterns {
		for _, kw := range p.keywords {
			if strings.Contains(lower, kw) {
				return p.class
			}
		}
	}
	return exchange.ClassUnknown
}
