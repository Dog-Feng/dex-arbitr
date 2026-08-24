// Package api 提供 REST 接口，并同源托管 web 控制台静态文件。
//
// 所有写操作都翻译成 Supervisor 命令，由 Runner 单线程执行。
package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"dex-grid/internal/app/supervisor"
	"dex-grid/internal/config"
	"dex-grid/internal/domain/strategy"
	"dex-grid/internal/domain/strategy/grid"
	"dex-grid/web"
)

// Server 是 HTTP 入口。
type Server struct {
	sup  *supervisor.Supervisor
	cfg  config.Server
	mux  *http.ServeMux
	http *http.Server
}

// New 构造 API 服务。
func New(sup *supervisor.Supervisor, cfg config.Server) *Server {
	s := &Server{sup: sup, cfg: cfg, mux: http.NewServeMux()}
	s.routes()
	s.http = &http.Server{Addr: cfg.Addr, Handler: s.withAccess(s.mux)}
	return s
}

// Handler 返回已包装鉴权的 handler，供 httptest 使用。
func (s *Server) Handler() http.Handler { return s.withAccess(s.mux) }

// ListenAndServe 阻塞服务。
func (s *Server) ListenAndServe() error { return s.http.ListenAndServe() }

// Shutdown 优雅关闭。
func (s *Server) Shutdown(ctx context.Context) error { return s.http.Shutdown(ctx) }

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /api/system/status", s.handleSystem)
	s.mux.HandleFunc("GET /api/exchanges", s.handleExchanges)
	s.mux.HandleFunc("GET /api/exchanges/{ex}/symbols", s.handleSymbols)
	s.mux.HandleFunc("GET /api/exchanges/{ex}/klines", s.handleKlines)
	s.mux.HandleFunc("POST /api/exchanges/{ex}/preview", s.handlePreview)
	s.mux.HandleFunc("GET /api/exchanges/{ex}/config", s.handleGetConfig)
	s.mux.HandleFunc("PUT /api/exchanges/{ex}/config", s.handlePutConfig)
	s.mux.HandleFunc("GET /api/exchanges/{ex}/status", s.handleStatus)
	s.mux.HandleFunc("GET /api/exchanges/{ex}/levels", s.handleLevels)
	s.mux.HandleFunc("GET /api/exchanges/{ex}/trades", s.handleTrades)
	s.mux.HandleFunc("GET /api/exchanges/{ex}/logs", s.handleLogs)
	s.mux.HandleFunc("POST /api/exchanges/{ex}/start", s.handleStart)
	s.mux.HandleFunc("POST /api/exchanges/{ex}/stop", s.handleStop)
	s.mux.HandleFunc("POST /api/exchanges/{ex}/adjust-range", s.handleAdjust)
	s.mux.HandleFunc("POST /api/exchanges/{ex}/cancel-orders", s.handleCancel)
	s.mux.HandleFunc("POST /api/exchanges/{ex}/refill", s.handleRefill)
	s.mux.HandleFunc("POST /api/exchanges/{ex}/reset-stats", s.handleReset)
	s.mux.HandleFunc("POST /api/exchanges/{ex}/reconnect", s.handleReconnect)
	s.mux.HandleFunc("GET /api/proxy", s.handleProxy)
	s.mountStatic()
}

func (s *Server) mountStatic() {
	s.mux.Handle("GET /css/", noCache(http.FileServer(http.FS(web.FS))))
	s.mux.Handle("GET /js/", noCache(http.FileServer(http.FS(web.FS))))
	s.mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		http.ServeFileFS(w, r, web.FS, "index.html")
	})
}

func noCache(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

func (s *Server) withAccess(next http.Handler) http.Handler {
	return s.withIPWhitelist(s.withAuth(next))
}

func (s *Server) withIPWhitelist(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.IPWhitelist.Enabled {
			ip := clientIP(r)
			if !ipAllowed(ip, s.cfg.IPWhitelist.Allow) {
				writeError(w, http.StatusForbidden, "FORBIDDEN", "来源 IP 不在白名单", "")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
func (s *Server) withAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.Auth.Enabled && strings.HasPrefix(r.URL.Path, "/api/") {
			got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if got == "" || got != s.cfg.Auth.Token {
				writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "无效的访问令牌", "")
				return
			}
		}
		if len(s.cfg.CORSOrigins) > 0 {
			origin := r.Header.Get("Origin")
			for _, o := range s.cfg.CORSOrigins {
				if o == origin || o == "*" {
					w.Header().Set("Access-Control-Allow-Origin", o)
					w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
					w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS")
					break
				}
			}
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeOK(w, map[string]string{"status": "ok"})
}

func (s *Server) handleSystem(w http.ResponseWriter, _ *http.Request) {
	writeOK(w, s.sup.SystemStatus())
}

func (s *Server) handleExchanges(w http.ResponseWriter, _ *http.Request) {
	writeOK(w, s.sup.Exchanges())
}

func (s *Server) handleSymbols(w http.ResponseWriter, r *http.Request) {
	list, err := s.sup.Symbols(r.Context(), r.PathValue("ex"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, list)
}

func (s *Server) handleKlines(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	list, err := s.sup.Klines(r.Context(), r.PathValue("ex"), q.Get("symbol"), q.Get("interval"), limit)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, list)
}

func (s *Server) handlePreview(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, 400, "BAD_REQUEST", err.Error(), "")
		return
	}
	d, err := s.sup.Preview(r.Context(), r.PathValue("ex"), raw)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, d)
}

func (s *Server) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	raw, err := s.sup.GetConfig(r.PathValue("ex"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, json.RawMessage(raw))
}

func (s *Server) handlePutConfig(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, 400, "BAD_REQUEST", err.Error(), "")
		return
	}
	d, err := s.sup.PutConfig(r.Context(), r.PathValue("ex"), raw)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, d)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	v, err := s.sup.Status(r.Context(), r.PathValue("ex"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, v)
}

func (s *Server) handleLevels(w http.ResponseWriter, r *http.Request) {
	v, err := s.sup.View(r.PathValue("ex"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, v.Strategy.Cells)
}

func (s *Server) handleTrades(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	fills, err := s.sup.Trades(r.PathValue("ex"), limit)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, fills)
}

func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	writeOK(w, s.sup.Logs(r.PathValue("ex"), q.Get("level"), limit))
}

func (s *Server) handleStart(w http.ResponseWriter, r *http.Request) {
	s.cmd(w, r, 15*time.Second, func(ctx context.Context) (any, error) {
		return s.sup.Start(ctx, r.PathValue("ex"))
	})
}

func (s *Server) handleStop(w http.ResponseWriter, r *http.Request) {
	s.cmd(w, r, 30*time.Second, func(ctx context.Context) (any, error) {
		return s.sup.Stop(ctx, r.PathValue("ex"))
	})
}

func (s *Server) handleAdjust(w http.ResponseWriter, r *http.Request) {
	var req grid.AdjustRange
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, 400, "BAD_REQUEST", err.Error(), "")
		return
	}
	s.cmd(w, r, 15*time.Second, func(ctx context.Context) (any, error) {
		return s.sup.AdjustRange(ctx, r.PathValue("ex"), req)
	})
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	s.cmd(w, r, 10*time.Second, func(ctx context.Context) (any, error) {
		return s.sup.CancelOrders(ctx, r.PathValue("ex"))
	})
}

func (s *Server) handleRefill(w http.ResponseWriter, r *http.Request) {
	s.cmd(w, r, 10*time.Second, func(ctx context.Context) (any, error) {
		return s.sup.Refill(ctx, r.PathValue("ex"))
	})
}

func (s *Server) handleReset(w http.ResponseWriter, r *http.Request) {
	s.cmd(w, r, 10*time.Second, func(ctx context.Context) (any, error) {
		return s.sup.ResetStats(ctx, r.PathValue("ex"))
	})
}

func (s *Server) handleReconnect(w http.ResponseWriter, r *http.Request) {
	s.cmd(w, r, 15*time.Second, func(ctx context.Context) (any, error) {
		return s.sup.Reconnect(ctx, r.PathValue("ex"))
	})
}

func (s *Server) handleProxy(w http.ResponseWriter, _ *http.Request) {
	writeOK(w, s.sup.Proxy())
}

func (s *Server) cmd(w http.ResponseWriter, r *http.Request, timeout time.Duration, fn func(context.Context) (any, error)) {
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	data, err := fn(ctx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			writeError(w, http.StatusGatewayTimeout, "TIMEOUT", "命令仍在执行中", "")
			return
		}
		writeErr(w, err)
		return
	}
	writeOK(w, data)
}

type envelope struct {
	OK    bool      `json:"ok"`
	Data  any       `json:"data,omitempty"`
	Error *apiError `json:"error,omitempty"`
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Field   string `json:"field,omitempty"`
}

func writeOK(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(envelope{OK: true, Data: data})
}

func writeError(w http.ResponseWriter, status int, code, msg, field string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(envelope{OK: false, Error: &apiError{Code: code, Message: msg, Field: field}})
}

func writeErr(w http.ResponseWriter, err error) {
	var issue *grid.Issue
	if errors.As(err, &issue) {
		writeError(w, http.StatusBadRequest, issue.Code, issue.Message, issue.Field)
		return
	}
	var sissue *strategy.Issue
	if errors.As(err, &sissue) {
		writeError(w, http.StatusBadRequest, sissue.Code, sissue.Message, sissue.Field)
		return
	}
	msg := err.Error()
	status := http.StatusBadRequest
	code := "ERROR"
	switch {
	case strings.Contains(msg, "未知或不存在"):
		status, code = http.StatusNotFound, "NOT_FOUND"
	case strings.Contains(msg, "运行中"):
		status, code = http.StatusConflict, "RUNNING"
	case strings.Contains(msg, "尚未"):
		status, code = http.StatusConflict, "NOT_READY"
	}
	writeError(w, status, code, msg, "")
}

func clientIP(r *http.Request) net.IP {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return net.ParseIP(host)
}

func ipAllowed(ip net.IP, allow []string) bool {
	if ip == nil {
		return false
	}
	if ip.IsLoopback() {
		return true
	}
	for _, raw := range allow {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		if strings.Contains(raw, "/") {
			_, n, err := net.ParseCIDR(raw)
			if err == nil && n.Contains(ip) {
				return true
			}
			continue
		}
		if parsed := net.ParseIP(raw); parsed != nil && parsed.Equal(ip) {
			return true
		}
	}
	return false
}
