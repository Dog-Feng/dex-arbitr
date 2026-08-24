package strategy

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Duration 是支持 "28d" 写法的时长，用于配置与 API。
//
// 标准库的 time.ParseDuration 最大单位是小时，而订单有效期这类配置
// 用天来表达更自然，所以在这里扩展一个 d 单位。
type Duration time.Duration

// Std 返回标准库时长。
func (d Duration) Std() time.Duration { return time.Duration(d) }

func (d Duration) String() string {
	std := time.Duration(d)
	if std != 0 && std%(24*time.Hour) == 0 {
		return strconv.FormatInt(int64(std/(24*time.Hour)), 10) + "d"
	}
	return std.String()
}

func (d Duration) MarshalText() ([]byte, error) { return []byte(d.String()), nil }

func (d *Duration) UnmarshalText(b []byte) error {
	v, err := ParseDuration(string(b))
	if err != nil {
		return err
	}
	*d = v
	return nil
}

// ParseDuration 解析时长，额外支持纯 "Nd" 形式。
func ParseDuration(s string) (Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	if days, ok := strings.CutSuffix(s, "d"); ok {
		n, err := strconv.ParseFloat(days, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q: %w", s, err)
		}
		return Duration(time.Duration(n * float64(24*time.Hour))), nil
	}
	std, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q: %w", s, err)
	}
	return Duration(std), nil
}

// MustParseDuration 在解析失败时 panic，仅用于常量默认值。
func MustParseDuration(s string) Duration {
	d, err := ParseDuration(s)
	if err != nil {
		panic(err)
	}
	return d
}
