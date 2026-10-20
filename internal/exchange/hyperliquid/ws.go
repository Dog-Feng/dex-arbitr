package hyperliquid

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const defaultWSURL = "wss://api.hyperliquid.xyz/ws"

type wsConn interface {
	ReadMessage() (int, []byte, error)
	WriteMessage(int, []byte) error
	WriteControl(int, []byte, time.Time) error
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
	SetPongHandler(func(string) error)
	Close() error
}

// wsClient maintains one connection for all subscriptions and resubscribes
// everything after a reconnect. Subscriptions are static, so rebuilding them
// is equivalent to the venue's own snapshot-latest-wins book design.
type wsClient struct {
	name    string
	url     string
	onMsg   func(channel string, raw []byte)
	onReset func()

	ctx    context.Context
	cancel context.CancelFunc
	done   <-chan struct{}
	dial   func(context.Context, string) (wsConn, error)

	mu       sync.Mutex
	conn     wsConn
	subs     map[string]map[string]any
	started  bool
	wg       sync.WaitGroup
	writeMu  sync.Mutex
	lastPong atomic.Int64
}

func newWSClient(name, url string, onMsg func(channel string, raw []byte)) *wsClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &wsClient{
		name: name, url: url, onMsg: onMsg,
		subs: map[string]map[string]any{},
		ctx:  ctx, cancel: cancel, done: ctx.Done(),
		dial: func(ctx context.Context, url string) (wsConn, error) {
			conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
			if err != nil {
				return nil, err
			}
			return conn, nil
		},
	}
}

// subscribe registers a subscription message. The key only has to be unique;
// it is never sent.
func (c *wsClient) subscribe(key string, sub map[string]any) {
	c.mu.Lock()
	if c.ctx.Err() != nil {
		c.mu.Unlock()
		return
	}
	c.subs[key] = sub
	conn := c.conn
	if !c.started {
		c.started = true
		c.wg.Add(1)
		go func() { defer c.wg.Done(); c.loop() }()
	}
	c.mu.Unlock()
	if conn != nil {
		c.sendSub(conn, sub)
	}
}

func (c *wsClient) sendSub(conn wsConn, sub map[string]any) {
	msg := map[string]any{"method": "subscribe", "subscription": sub}
	if err := c.write(conn, msg); err != nil {
		slog.Warn("ws subscribe write failed", "venue", c.name, "err", err)
		closeConn(conn)
	}
}

func (c *wsClient) write(conn wsConn, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, b)
}

func closeConn(conn wsConn) {
	if err := conn.Close(); err != nil {
		slog.Debug("ws close failed", "err", err)
	}
}

func (c *wsClient) close() {
	c.mu.Lock()
	c.cancel()
	conn := c.conn
	c.mu.Unlock()
	if conn != nil {
		closeConn(conn)
	}
	c.wg.Wait()
}

func (c *wsClient) loop() {
	backoff := time.Second
	for c.ctx.Err() == nil {
		conn, err := c.dial(c.ctx, c.url)
		if err != nil {
			if c.ctx.Err() != nil {
				return
			}
			slog.Warn("ws dial failed", "venue", c.name, "err", err, "retry_in", backoff)
			if !waitTimer(c.done, backoff) {
				return
			}
			if backoff < 30*time.Second {
				backoff *= 2
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
			}
			continue
		}
		c.mu.Lock()
		if c.ctx.Err() != nil {
			c.mu.Unlock()
			closeConn(conn)
			return
		}
		c.conn = conn
		subs := make([]map[string]any, 0, len(c.subs))
		for _, s := range c.subs {
			subs = append(subs, s)
		}
		c.mu.Unlock()
		backoff = time.Second
		if c.onReset != nil {
			c.onReset()
		}
		conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(90 * time.Second)) })
		c.lastPong.Store(time.Now().UnixMilli())
		for _, s := range subs {
			c.sendSub(conn, s)
		}
		stopPing := make(chan struct{})
		pingDone := make(chan struct{})
		go func() {
			defer close(pingDone)
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					// Hyperliquid closes connections it has not heard from in
					// 60 seconds; an application-level ping is what the server
					// counts, and it answers with {"channel":"pong"}.
					if err := c.write(conn, map[string]any{"method": "ping"}); err != nil {
						closeConn(conn)
						return
					}
					c.writeMu.Lock()
					err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second))
					c.writeMu.Unlock()
					if err != nil {
						closeConn(conn)
						return
					}
					if time.Since(time.UnixMilli(c.lastPong.Load())) > 75*time.Second {
						// Two consecutive pongs missed: the server stopped
						// talking; reconnect instead of trading blind.
						slog.Warn("ws heartbeat stale; reconnecting", "venue", c.name)
						closeConn(conn)
						return
					}
				case <-stopPing:
					return
				case <-c.done:
					return
				}
			}
		}()
		c.readLoop(conn)
		if c.onReset != nil {
			c.onReset()
		}
		close(stopPing)
		<-pingDone
		c.mu.Lock()
		c.conn = nil
		c.mu.Unlock()
		closeConn(conn)
	}
}

func waitTimer(done <-chan struct{}, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-done:
		return false
	}
}

func (c *wsClient) readLoop(conn wsConn) {
	for c.ctx.Err() == nil {
		if err := conn.SetReadDeadline(time.Now().Add(90 * time.Second)); err != nil {
			return
		}
		_, raw, err := conn.ReadMessage()
		if err != nil {
			if c.ctx.Err() == nil {
				slog.Warn("ws read error, reconnecting", "venue", c.name, "err", err)
			}
			return
		}
		if c.ctx.Err() != nil {
			return
		}
		var hdr struct {
			Channel string `json:"channel"`
		}
		if err := json.Unmarshal(raw, &hdr); err != nil {
			continue
		}
		switch hdr.Channel {
		case "ping":
			// Defensive: reply to server-initiated pings; the venue's own
			// heartbeat uses the application-level ping/pong pair.
			if err := c.write(conn, map[string]any{"method": "pong"}); err != nil {
				return
			}
		case "pong":
			c.lastPong.Store(time.Now().UnixMilli())
		case "subscriptionResponse", "error", "":
			// acks and errors carry no book or order data
		default:
			if c.onMsg != nil {
				c.onMsg(hdr.Channel, raw)
			}
		}
	}
}
