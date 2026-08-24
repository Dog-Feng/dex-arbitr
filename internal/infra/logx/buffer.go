// Package logx 提供 slog 输出与内存环形缓冲，供页面日志面板读取。
package logx

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"time"
)

// Record 是一条可供页面展示的日志。
type Record struct {
	Time     time.Time         `json:"ts"`
	Level    string            `json:"level"`
	Message  string            `json:"msg"`
	Exchange string            `json:"exchange,omitempty"`
	Attrs    map[string]string `json:"attrs,omitempty"`
}

// Buffer 是固定容量的环形缓冲，并发安全。
type Buffer struct {
	mu   sync.Mutex
	recs []Record
	head int
	size int
	cap  int
}

// NewBuffer 构造容量为 n 的缓冲。n <= 0 时用 2000。
func NewBuffer(n int) *Buffer {
	if n <= 0 {
		n = 2000
	}
	return &Buffer{recs: make([]Record, n), cap: n}
}

// Append 写入一条记录，满了覆盖最旧的。
func (b *Buffer) Append(r Record) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.recs[b.head] = r
	b.head = (b.head + 1) % b.cap
	if b.size < b.cap {
		b.size++
	}
}

// List 返回最近的记录，最新在前。可按交易所与级别过滤。
func (b *Buffer) List(exchange, level string, limit int) []Record {
	if limit <= 0 {
		limit = 100
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]Record, 0, min(limit, b.size))
	for i := 0; i < b.size && len(out) < limit; i++ {
		idx := (b.head - 1 - i + b.cap) % b.cap
		r := b.recs[idx]
		if exchange != "" && r.Exchange != exchange {
			continue
		}
		if level != "" && r.Level != level {
			continue
		}
		out = append(out, r)
	}
	return out
}

// Handler 是同时写到 stderr/文件 与环形缓冲的 slog Handler。
type Handler struct {
	inner slog.Handler
	buf   *Buffer
	attrs []slog.Attr
}

// NewHandler 包装 inner，每条日志额外写入 buf。
func NewHandler(inner slog.Handler, buf *Buffer) *Handler {
	return &Handler{inner: inner, buf: buf}
}

func (h *Handler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *Handler) Handle(ctx context.Context, rec slog.Record) error {
	if h.buf != nil {
		r := Record{
			Time:    rec.Time.UTC(),
			Level:   rec.Level.String(),
			Message: rec.Message,
			Attrs:   map[string]string{},
		}
		for _, a := range h.attrs {
			collectAttr(&r, a)
		}
		rec.Attrs(func(a slog.Attr) bool {
			collectAttr(&r, a)
			return true
		})
		if len(r.Attrs) == 0 {
			r.Attrs = nil
		}
		h.buf.Append(r)
	}
	return h.inner.Handle(ctx, rec)
}

func collectAttr(r *Record, a slog.Attr) {
	if a.Equal(slog.Attr{}) {
		return
	}
	if a.Key == "exchange" {
		r.Exchange = a.Value.String()
	}
	if r.Attrs == nil {
		r.Attrs = map[string]string{}
	}
	r.Attrs[a.Key] = a.Value.String()
}

func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	combined := append(append([]slog.Attr{}, h.attrs...), attrs...)
	return &Handler{inner: h.inner.WithAttrs(attrs), buf: h.buf, attrs: combined}
}

func (h *Handler) WithGroup(name string) slog.Handler {
	return &Handler{inner: h.inner.WithGroup(name), buf: h.buf, attrs: h.attrs}
}

// Setup 按配置构造 slog 默认 logger，并返回环形缓冲。
func Setup(level, format, file string, bufferSize int) (*Buffer, error) {
	var lv slog.Level
	switch level {
	case "debug":
		lv = slog.LevelDebug
	case "warn":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	default:
		lv = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lv}
	out := os.Stderr
	if file != "" {
		f, err := os.OpenFile(file, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, err
		}
		out = f
	}
	var inner slog.Handler
	if format == "text" {
		inner = slog.NewTextHandler(out, opts)
	} else {
		inner = slog.NewJSONHandler(out, opts)
	}
	buf := NewBuffer(bufferSize)
	slog.SetDefault(slog.New(NewHandler(inner, buf)))
	return buf, nil
}
