// Package httpx 按全局代理配置构造 HTTP 客户端。
//
// 代理配置在 config.yaml 的全局段而不是各交易所段，所以由应用层统一构造
// 客户端再注入给适配器，适配器自己不关心代理怎么来的。
package httpx

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"dex-grid/internal/config"

	"golang.org/x/net/proxy"
)

// New 构造带代理与超时的 HTTP 客户端。
func New(p config.Proxy, timeout time.Duration) (*http.Client, error) {
	tr := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 60 * time.Second,
		}).DialContext,
		MaxIdleConnsPerHost:   32,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}

	if p.Enabled {
		if err := applyProxy(tr, p); err != nil {
			return nil, err
		}
	}
	return &http.Client{Transport: tr, Timeout: timeout}, nil
}

func applyProxy(tr *http.Transport, p config.Proxy) error {
	if strings.TrimSpace(p.URL) == "" {
		return fmt.Errorf("httpx: proxy.enabled 为 true 但 proxy.url 为空")
	}
	u, err := url.Parse(p.URL)
	if err != nil {
		return fmt.Errorf("httpx: 代理地址无法解析: %w", err)
	}
	if p.Username != "" {
		u.User = url.UserPassword(p.Username, p.Password)
	}

	switch u.Scheme {
	case "http", "https":
		bypass := bypassSet(p.NoProxy)
		tr.Proxy = func(req *http.Request) (*url.URL, error) {
			if bypass[req.URL.Hostname()] {
				return nil, nil
			}
			return u, nil
		}
	case "socks5", "socks5h":
		d, err := proxy.FromURL(u, proxy.Direct)
		if err != nil {
			return fmt.Errorf("httpx: 构造 SOCKS5 代理失败: %w", err)
		}
		cd, ok := d.(proxy.ContextDialer)
		if !ok {
			return fmt.Errorf("httpx: SOCKS5 代理不支持带上下文的拨号")
		}
		tr.DialContext = cd.DialContext
	default:
		return fmt.Errorf("httpx: 不支持的代理协议 %q", u.Scheme)
	}
	return nil
}

func bypassSet(hosts []string) map[string]bool {
	out := make(map[string]bool, len(hosts))
	for _, h := range hosts {
		out[strings.TrimSpace(h)] = true
	}
	return out
}
