package exchange

import (
	"errors"
	"fmt"
)

// ErrorClass 描述一个交易所错误该怎么处理。
//
// 分类由适配器负责——只有它知道自家的错误码和错误文案长什么样。
// 上层按分类决策，不去猜字符串。
type ErrorClass uint8

const (
	// ClassUnknown 无法归类。按不可重试处理，计入连续失败。
	ClassUnknown ErrorClass = iota
	// ClassRetryable 网络超时、5xx、限流、nonce 冲突，退避后重试。
	ClassRetryable
	// ClassPostOnlyRejected post-only 单会立即成交被拒。
	// 这不算失败：说明价格已经穿过该层级，等下一个 tick 用新价重挂即可。
	ClassPostOnlyRejected
	// ClassInsufficientMargin 保证金不足。暂停开仓腿，保留平仓腿。
	ClassInsufficientMargin
	// ClassInvalidParam 价格/数量非法、市场不存在。重试没有意义。
	ClassInvalidParam
	// ClassFatal 签名失败、认证失败。立刻熔断。
	ClassFatal
)

func (c ErrorClass) String() string {
	switch c {
	case ClassRetryable:
		return "retryable"
	case ClassPostOnlyRejected:
		return "post_only_rejected"
	case ClassInsufficientMargin:
		return "insufficient_margin"
	case ClassInvalidParam:
		return "invalid_param"
	case ClassFatal:
		return "fatal"
	default:
		return "unknown"
	}
}

// Retryable 表示该分类值得退避后重试。
func (c ErrorClass) Retryable() bool { return c == ClassRetryable }

// CountsAsFailure 表示该分类应当计入连续失败计数（熔断依据）。
//
// post-only 被拒不算失败：行情快速穿过挂单价位时它会频繁出现，
// 把它计入熔断会在正常波动中误杀实例。
func (c ErrorClass) CountsAsFailure() bool {
	return c != ClassPostOnlyRejected
}

// Error 是带分类的交易所错误。
type Error struct {
	Class ErrorClass
	// Op 是出错的操作，例如 "place_order"、"cancel_order"。
	Op  string
	Err error
}

func (e *Error) Error() string {
	if e.Op == "" {
		return fmt.Sprintf("[%s] %v", e.Class, e.Err)
	}
	return fmt.Sprintf("%s [%s]: %v", e.Op, e.Class, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// Classify 把错误包装成带分类的形式。err 为 nil 时返回 nil。
func Classify(class ErrorClass, op string, err error) error {
	if err == nil {
		return nil
	}
	return &Error{Class: class, Op: op, Err: err}
}

// ClassOf 取出错误的分类。未分类的错误返回 ClassUnknown。
func ClassOf(err error) ErrorClass {
	var e *Error
	if errors.As(err, &e) {
		return e.Class
	}
	return ClassUnknown
}
