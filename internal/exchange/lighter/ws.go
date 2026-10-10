package lighter

import (
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// subscription builds its subscribe message on every (re)connect, so auth
// tokens are refreshed automatically.
type subscription func() (map[string]any, error)

type wsClient struct {
	name    string
	url     string
	onMsg   func(typ, channel string, raw []byte)
	onReset func() // called after reconnect (state must be rebuilt)

	mu      sync.Mutex
	conn    *websocket.Conn
	subs    map[string]subscription // channel -> builder
	started bool
	done    chan struct{}
	writeMu sync.Mutex
}

func newWSClient(name, url string, onMsg func(typ, channel string, raw []byte)) *wsClient {
	return &wsClient{name: name, url: url, onMsg: onMsg, subs: map[string]subscription{}, done: make(chan struct{})}
}

// subscribe registers a channel and sends it immediately if connected.
func (c *wsClient) subscribe(channel string, s subscription) {
	c.mu.Lock()
	c.subs[channel] = s
	conn := c.conn
	if !c.started {
		c.started = true
		go c.loop()
	}
	c.mu.Unlock()
	if conn != nil {
		c.sendSub(conn, s)
	}
}

// resubscribe forces unsubscribe+subscribe for a channel (e.g. book gap).
func (c *wsClient) resubscribe(channel string) {
	c.mu.Lock()
	s, conn := c.subs[channel], c.conn
	c.mu.Unlock()
	if conn == nil || s == nil {
		return
	}
	_ = c.write(conn, map[string]any{"type": "unsubscribe", "channel": channel})
	c.sendSub(conn, s)
}

func (c *wsClient) sendSub(conn *websocket.Conn, s subscription) {
	m, err := s()
	if err != nil {
		slog.Error("ws subscribe build failed", "venue", c.name, "err", err)
		return
	}
	if err := c.write(conn, m); err != nil {
		slog.Warn("ws subscribe write failed", "venue", c.name, "err", err)
	}
}

func (c *wsClient) write(conn *websocket.Conn, v any) error {
	b, _ := json.Marshal(v)
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return conn.WriteMessage(websocket.TextMessage, b)
}

func (c *wsClient) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.done:
	default:
		close(c.done)
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
}

func (c *wsClient) loop() {
	backoff := time.Second
	for {
		select {
		case <-c.done:
			return
		default:
		}
		conn, _, err := websocket.DefaultDialer.Dial(c.url, nil)
		if err != nil {
			slog.Warn("ws dial failed", "venue", c.name, "err", err, "retry_in", backoff)
			select {
			case <-time.After(backoff):
			case <-c.done:
				return
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
		c.mu.Lock()
		c.conn = conn
		subs := make([]subscription, 0, len(c.subs))
		for _, s := range c.subs {
			subs = append(subs, s)
		}
		c.mu.Unlock()
		if c.onReset != nil {
			c.onReset()
		}
		for _, s := range subs {
			c.sendSub(conn, s)
		}
		stopPing := make(chan struct{})
		go func() { // keepalive: Lighter needs a frame at least every 2 minutes
			t := time.NewTicker(30 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					c.writeMu.Lock()
					_ = conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second))
					c.writeMu.Unlock()
				case <-stopPing:
					return
				}
			}
		}()
		c.readLoop(conn)
		close(stopPing)
		c.mu.Lock()
		c.conn = nil
		c.mu.Unlock()
		_ = conn.Close()
	}
}

func (c *wsClient) readLoop(conn *websocket.Conn) {
	for {
		_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		_, raw, err := conn.ReadMessage()
		if err != nil {
			select {
			case <-c.done:
			default:
				slog.Warn("ws read error, reconnecting", "venue", c.name, "err", err)
			}
			return
		}
		var hdr struct {
			Type    string `json:"type"`
			Channel string `json:"channel"`
		}
		if json.Unmarshal(raw, &hdr) != nil {
			continue
		}
		if hdr.Type == "ping" {
			_ = c.write(conn, map[string]any{"type": "pong"})
			continue
		}
		c.onMsg(hdr.Type, hdr.Channel, raw)
	}
}
