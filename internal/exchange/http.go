package exchange

import (
	"net"
	"net/http"
	"time"
)

// NewHTTPTransport returns a connection-pooled transport for venue REST
// clients. Larger idle pools than http.DefaultTransport absorb bursts of
// concurrent queries and signed actions without per-request TLS handshakes,
// and the longer idle timeout keeps connections warm across quiet stretches
// that exceed the default 90s.
func NewHTTPTransport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          16,
		MaxIdleConnsPerHost:   8,
		IdleConnTimeout:       5 * time.Minute,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
}
