package lighter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"golang.org/x/time/rate"
)

// num 同时接受 JSON 字符串和数字形式的十进制数。
//
// Lighter 的响应里两种都有：mark_price 是 "75.499"，last_trade_price 是 75.504。
type num struct{ decimal.Decimal }

func (n *num) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "" || s == "null" {
		n.Decimal = decimal.Zero
		return nil
	}
	v, err := decimal.NewFromString(s)
	if err != nil {
		return fmt.Errorf("lighter: 无法解析数值 %s: %w", string(b), err)
	}
	n.Decimal = v
	return nil
}

// flexBool 同时接受 JSON 布尔值和 0/1 数字。
//
// Lighter 不同端点对同一语义的字段用了不同表示：下单请求里 is_ask 是 0/1，
// 查询挂单返回的却是 true/false。
type flexBool bool

func (b *flexBool) UnmarshalJSON(raw []byte) error {
	switch s := strings.Trim(string(raw), `"`); s {
	case "true", "1":
		*b = true
	case "false", "0", "", "null":
		*b = false
	default:
		return fmt.Errorf("lighter: 无法解析布尔值 %s", string(raw))
	}
	return nil
}

func (b flexBool) Bool() bool { return bool(b) }

type resultCode struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// apiError 是交易所返回的业务错误。
type apiError struct {
	Endpoint string
	Status   int
	Code     int
	Message  string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("lighter %s: http=%d code=%d %s", e.Endpoint, e.Status, e.Code, e.Message)
}

// restClient 是带限流与代理的 HTTP 客户端。
type restClient struct {
	baseURL string
	http    *http.Client
	limiter *rate.Limiter
	// retries 是可重试错误的最大重试次数。
	retries int
}

func newRESTClient(baseURL string, httpc *http.Client, rps, burst, retries int) *restClient {
	return &restClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    httpc,
		limiter: rate.NewLimiter(rate.Limit(rps), burst),
		retries: retries,
	}
}

func (c *restClient) get(ctx context.Context, path string, params url.Values, out any) error {
	u := c.baseURL + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}
	return c.do(ctx, http.MethodGet, u, nil, out)
}

func (c *restClient) postForm(ctx context.Context, path string, form url.Values, out any) error {
	return c.do(ctx, http.MethodPost, c.baseURL+path, form, out)
}

func (c *restClient) do(ctx context.Context, method, u string, form url.Values, out any) error {
	var lastErr error
	for attempt := 0; attempt <= c.retries; attempt++ {
		if attempt > 0 {
			// 指数退避：200ms、400ms、800ms…
			delay := time.Duration(200<<(attempt-1)) * time.Millisecond
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
		if err := c.limiter.Wait(ctx); err != nil {
			return err
		}

		err := c.attempt(ctx, method, u, form, out)
		if err == nil {
			return nil
		}
		lastErr = err
		if !isRetryable(err) {
			return err
		}
	}
	return lastErr
}

func (c *restClient) attempt(ctx context.Context, method, u string, form url.Values, out any) error {
	var body io.Reader
	if form != nil {
		body = strings.NewReader(form.Encode())
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", userAgent)
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return &transportError{err: err}
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return &transportError{err: err}
	}

	endpoint := trimEndpoint(u)
	if resp.StatusCode != http.StatusOK {
		var rc resultCode
		_ = json.Unmarshal(raw, &rc)
		msg := rc.Message
		if msg == "" {
			msg = truncate(string(raw), 300)
		}
		return &apiError{Endpoint: endpoint, Status: resp.StatusCode, Code: rc.Code, Message: msg}
	}

	// 即使 HTTP 200，业务层仍可能返回非 200 的 code。
	var rc resultCode
	if err := json.Unmarshal(raw, &rc); err == nil && rc.Code != 0 && rc.Code != 200 {
		return &apiError{Endpoint: endpoint, Status: resp.StatusCode, Code: rc.Code, Message: rc.Message}
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("lighter %s: 解析响应失败: %w (body=%s)", endpoint, err, truncate(string(raw), 300))
	}
	return nil
}

type transportError struct{ err error }

func (e *transportError) Error() string { return "lighter transport: " + e.err.Error() }
func (e *transportError) Unwrap() error { return e.err }

// isRetryable 判断错误是否值得重试。
//
// 网络层错误、5xx、429 可以重试；4xx 是参数问题，重试只会重复失败。
func isRetryable(err error) bool {
	var te *transportError
	if errorsAs(err, &te) {
		return true
	}
	var ae *apiError
	if errorsAs(err, &ae) {
		return ae.Status >= 500 || ae.Status == http.StatusTooManyRequests
	}
	return false
}

func trimEndpoint(u string) string {
	if i := strings.Index(u, "/api/"); i >= 0 {
		if q := strings.Index(u[i:], "?"); q >= 0 {
			return u[i : i+q]
		}
		return u[i:]
	}
	return u
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func itoa(v int64) string { return strconv.FormatInt(v, 10) }
