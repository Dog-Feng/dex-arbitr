package lighter

import (
	"bufio"
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// pipeWS implements the websocket boundary over an in-memory pipe. No listener
// or remote service is involved in these connection lifecycle tests.
type pipeWS struct {
	conn   net.Conn
	reader *bufio.Reader
	writes chan []byte
	closed atomic.Bool
	reads  atomic.Int32
}

func newPipeWS(t *testing.T) (*pipeWS, net.Conn) {
	t.Helper()
	conn, peer := net.Pipe()
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
		if err := peer.Close(); err != nil {
			t.Error(err)
		}
	})
	return &pipeWS{conn: conn, reader: bufio.NewReader(conn), writes: make(chan []byte, 16)}, peer
}

func (p *pipeWS) ReadMessage() (int, []byte, error) {
	p.reads.Add(1)
	b, err := p.reader.ReadBytes('\n')
	return websocket.TextMessage, b, err
}
func (p *pipeWS) WriteMessage(_ int, b []byte) error {
	if p.closed.Load() {
		return net.ErrClosed
	}
	select {
	case p.writes <- append([]byte(nil), b...):
		return nil
	default:
		return errors.New("write buffer full")
	}
}
func (p *pipeWS) WriteControl(int, []byte, time.Time) error { return nil }
func (p *pipeWS) SetReadDeadline(t time.Time) error         { return p.conn.SetReadDeadline(t) }
func (p *pipeWS) SetWriteDeadline(t time.Time) error        { return p.conn.SetWriteDeadline(t) }
func (p *pipeWS) SetPongHandler(func(string) error)         {}
func (p *pipeWS) Close() error                              { p.closed.Store(true); return p.conn.Close() }

func TestWSCloseDuringSuccessfulDialClosesLateConnection(t *testing.T) {
	conn, _ := newPipeWS(t)
	c := newWSClient("offline", "wss://offline.invalid", nil)
	t.Cleanup(c.close)
	entered := make(chan struct{})
	c.dial = func(ctx context.Context, _ string) (wsConn, error) { close(entered); <-ctx.Done(); return conn, nil }
	c.subscribe("test", func() (map[string]any, error) { return map[string]any{"type": "subscribe"}, nil })
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("dial not called")
	}
	closed := make(chan struct{})
	go func() { c.close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not wait for cancelled dial")
	}
	if !conn.closed.Load() || conn.reads.Load() != 0 {
		t.Fatalf("late connection closed=%v reads=%d", conn.closed.Load(), conn.reads.Load())
	}
	select {
	case b := <-conn.writes:
		t.Fatalf("subscription after close: %s", b)
	default:
	}
}

func TestWSDisconnectInvalidatesBookBeforeReconnect(t *testing.T) {
	v := testVenue(t, false)
	conn, peer := newPipeWS(t)
	first := true
	v.ws.dial = func(ctx context.Context, _ string) (wsConn, error) {
		if first {
			first = false
			return conn, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	ch, err := v.SubscribeBook(context.Background(), validRequest().Inst)
	if err != nil {
		t.Fatal(err)
	}
	// The initial reset is published before subscribing to a connection.
	if b := receiveBook(t, ch); b.Valid() {
		t.Fatal("new connection retained valid old book")
	}
	select {
	case <-conn.writes:
	case <-time.After(time.Second):
		t.Fatal("subscription missing")
	}
	if _, err := peer.Write([]byte(`{"type":"subscribed/order_book","channel":"order_book:1","order_book":{"nonce":1,"bids":[{"price":"99","size":"1"}],"asks":[{"price":"100","size":"1"}]}}` + "\n")); err != nil {
		t.Fatal(err)
	}
	if b := receiveBook(t, ch); !b.Valid() {
		t.Fatalf("snapshot=%+v", b)
	}
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
	if b := receiveBook(t, ch); b.Valid() {
		t.Fatal("disconnected book stayed valid")
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if !conn.closed.Load() {
		t.Fatal("connection survived Close")
	}
}

func TestWSReadLoopStopsOnCancellationBeforeCallback(t *testing.T) {
	conn, peer := newPipeWS(t)
	called := atomic.Bool{}
	c := newWSClient("offline", "wss://offline.invalid", func(string, string, []byte) { called.Store(true) })
	c.cancel()
	c.readLoop(conn)
	if conn.reads.Load() != 0 || called.Load() {
		t.Fatal("cancelled reader consumed a message")
	}
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentVenueCloseWaitsForReader(t *testing.T) {
	v := testVenue(t, false)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	finishDial := func() { releaseOnce.Do(func() { close(release) }) }
	defer finishDial()
	v.ws.dial = func(ctx context.Context, _ string) (wsConn, error) {
		close(entered)
		<-ctx.Done()
		<-release
		return nil, ctx.Err()
	}
	if _, err := v.SubscribeBook(context.Background(), validRequest().Inst); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("dial not entered")
	}
	firstDone, secondDone := make(chan struct{}), make(chan struct{})
	go func() {
		if err := v.Close(); err != nil {
			t.Error(err)
		}
		close(firstDone)
	}()
	select {
	case <-v.ws.done:
	case <-time.After(time.Second):
		t.Fatal("close not cancelled")
	}
	go func() {
		if err := v.Close(); err != nil {
			t.Error(err)
		}
		close(secondDone)
	}()
	select {
	case <-secondDone:
		t.Fatal("concurrent Close returned before loop stopped")
	case <-time.After(20 * time.Millisecond):
	}
	finishDial()
	for _, ch := range []<-chan struct{}{firstDone, secondDone} {
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatal("Close hung")
		}
	}
}
