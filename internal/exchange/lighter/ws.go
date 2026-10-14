package lighter

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// subscription builds a fresh authenticated message for each connection.
type subscription func() (map[string]any, error)

type wsConn interface {
	ReadMessage() (int, []byte, error)
	WriteMessage(int, []byte) error
	WriteControl(int, []byte, time.Time) error
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
	SetPongHandler(func(string) error)
	Close() error
}

type wsClient struct {
	name    string
	url     string
	onMsg   func(typ, channel string, raw []byte)
	onReset func()
	ctx     context.Context
	cancel  context.CancelFunc
	done    <-chan struct{}
	dial    func(context.Context, string) (wsConn, error)

	mu      sync.Mutex
	conn    wsConn
	subs    map[string]subscription
	started bool
	wg      sync.WaitGroup
	writeMu sync.Mutex
}

func newWSClient(name, url string, onMsg func(typ, channel string, raw []byte)) *wsClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &wsClient{name: name, url: url, onMsg: onMsg, subs: map[string]subscription{}, ctx: ctx, cancel: cancel, done: ctx.Done(), dial: func(ctx context.Context, url string) (wsConn, error) {
		conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
		if err != nil {
			return nil, err
		}
		return conn, nil
	}}
}

func (c *wsClient) subscribe(channel string, s subscription) {
	c.mu.Lock()
	if c.ctx.Err() != nil {
		c.mu.Unlock()
		return
	}
	c.subs[channel] = s
	conn := c.conn
	if !c.started {
		c.started = true
		c.wg.Add(1)
		go func() { defer c.wg.Done(); c.loop() }()
	}
	c.mu.Unlock()
	if conn != nil {
		c.sendSub(conn, s)
	}
}

func (c *wsClient) resubscribe(channel string) {
	c.mu.Lock()
	s, conn := c.subs[channel], c.conn
	c.mu.Unlock()
	if conn == nil || s == nil || c.ctx.Err() != nil {
		return
	}
	if err := c.write(conn, map[string]any{"type": "unsubscribe", "channel": channel}); err != nil {
		slog.Warn("ws unsubscribe failed", "venue", c.name, "err", err)
		closeConn(conn)
		return
	}
	c.sendSub(conn, s)
}

func (c *wsClient) sendSub(conn wsConn, s subscription) {
	m, err := s()
	if err != nil {
		slog.Error("ws subscribe build failed", "venue", c.name, "err", err)
		closeConn(conn)
		return
	}
	if err := c.write(conn, m); err != nil {
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
			timer := time.NewTimer(backoff)
			select {
			case <-timer.C:
			case <-c.done:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
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
		subs := make([]subscription, 0, len(c.subs))
		for _, s := range c.subs {
			subs = append(subs, s)
		}
		c.mu.Unlock()
		backoff = time.Second
		if c.onReset != nil {
			c.onReset()
		}
		conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(90 * time.Second)) })
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
					c.writeMu.Lock()
					err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second))
					c.writeMu.Unlock()
					if err != nil {
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
			Type    string `json:"type"`
			Channel string `json:"channel"`
		}
		if err := json.Unmarshal(raw, &hdr); err != nil {
			continue
		}
		if hdr.Type == "ping" {
			if err := c.write(conn, map[string]any{"type": "pong"}); err != nil {
				return
			}
			continue
		}
		if c.onMsg != nil {
			c.onMsg(hdr.Type, hdr.Channel, raw)
		}
	}
}
