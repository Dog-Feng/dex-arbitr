package lighter

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
)

// bookState is a locally maintained L2 book built from snapshot + deltas.
type bookState struct {
	marketID  int
	bids      map[float64]float64
	asks      map[float64]float64
	nonce     int64
	synced    bool
	subs      []chan domain.Book
	lastPrint time.Time
}

type wsLevel struct {
	Price string `json:"price"`
	Size  string `json:"size"`
}

type wsBookMsg struct {
	Type      string `json:"type"`
	Channel   string `json:"channel"`
	OrderBook struct {
		Asks       []wsLevel `json:"asks"`
		Bids       []wsLevel `json:"bids"`
		Nonce      int64     `json:"nonce"`
		BeginNonce int64     `json:"begin_nonce"`
	} `json:"order_book"`
}

func bookChannel(m int) string { return "order_book/" + strconv.Itoa(m) }

// SubscribeBook streams top-N snapshots for an instrument.
func (v *Venue) SubscribeBook(ctx context.Context, inst domain.Instrument) (<-chan domain.Book, error) {
	ch := make(chan domain.Book, 1)
	v.booksMu.Lock()
	bs, ok := v.books[inst.MarketID]
	if !ok {
		bs = &bookState{marketID: inst.MarketID, bids: map[float64]float64{}, asks: map[float64]float64{}}
		v.books[inst.MarketID] = bs
	}
	bs.subs = append(bs.subs, ch)
	v.booksMu.Unlock()
	if !ok {
		chName := bookChannel(inst.MarketID)
		v.ws.subscribe(chName, func() (map[string]any, error) {
			return map[string]any{"type": "subscribe", "channel": chName}, nil
		})
	}
	go func() {
		<-ctx.Done()
		v.booksMu.Lock()
		defer v.booksMu.Unlock()
		for i, c := range bs.subs {
			if c == ch {
				bs.subs = append(bs.subs[:i], bs.subs[i+1:]...)
				break
			}
		}
	}()
	return ch, nil
}

func (v *Venue) resetBooks() {
	v.booksMu.Lock()
	defer v.booksMu.Unlock()
	for _, b := range v.books {
		b.synced = false
		b.bids, b.asks = map[float64]float64{}, map[float64]float64{}
	}
}

func (v *Venue) onWSMessage(typ, channel string, raw []byte) {
	switch {
	case strings.HasPrefix(channel, "order_book:"):
		v.onBook(typ, channel, raw)
	case strings.HasPrefix(channel, "account_all_orders:"):
		v.onOrders(raw)
	case typ == "error" || strings.Contains(string(raw), `"error"`):
		slog.Warn("ws error message", "venue", v.cfg.Name, "msg", string(raw))
	}
}

func (v *Venue) onBook(typ, channel string, raw []byte) {
	var m wsBookMsg
	if err := json.Unmarshal(raw, &m); err != nil {
		return
	}
	id, err := strconv.Atoi(strings.TrimPrefix(channel, "order_book:"))
	if err != nil {
		return
	}
	v.booksMu.Lock()
	defer v.booksMu.Unlock()
	bs := v.books[id]
	if bs == nil {
		return
	}
	snapshot := strings.HasPrefix(typ, "subscribed/")
	if snapshot {
		bs.bids, bs.asks = map[float64]float64{}, map[float64]float64{}
		bs.synced = true
	} else {
		if !bs.synced {
			return // wait for snapshot
		}
		if m.OrderBook.BeginNonce != 0 && bs.nonce != 0 && m.OrderBook.BeginNonce != bs.nonce {
			slog.Warn("book nonce gap, resubscribing", "venue", v.cfg.Name, "market", id,
				"have", bs.nonce, "begin", m.OrderBook.BeginNonce)
			bs.synced = false
			go v.ws.resubscribe(bookChannel(id))
			return
		}
	}
	apply := func(side map[float64]float64, lv []wsLevel) {
		for _, l := range lv {
			p, s := domain.ParseF(l.Price), domain.ParseF(l.Size)
			if s <= 0 {
				delete(side, p)
			} else {
				side[p] = s
			}
		}
	}
	apply(bs.bids, m.OrderBook.Bids)
	apply(bs.asks, m.OrderBook.Asks)
	bs.nonce = m.OrderBook.Nonce

	b := domain.Book{Venue: v.cfg.Name, MarketID: id, Seq: bs.nonce, Ts: time.Now(),
		Bids: topN(bs.bids, v.cfg.BookDepth, true), Asks: topN(bs.asks, v.cfg.BookDepth, false)}
	for _, c := range bs.subs {
		select { // latest wins
		case <-c:
		default:
		}
		select {
		case c <- b:
		default:
		}
	}
}

func topN(m map[float64]float64, n int, desc bool) []domain.Level {
	ps := make([]float64, 0, len(m))
	for p := range m {
		ps = append(ps, p)
	}
	if desc {
		sort.Sort(sort.Reverse(sort.Float64Slice(ps)))
	} else {
		sort.Float64s(ps)
	}
	if n > 0 && len(ps) > n {
		ps = ps[:n]
	}
	out := make([]domain.Level, len(ps))
	for i, p := range ps {
		out[i] = domain.Level{Price: p, Size: m[p]}
	}
	return out
}

// ---------------- orders ----------------

func mapStatus(s string) domain.OrderStatus {
	switch {
	case s == "filled":
		return domain.StatusFilled
	case strings.HasPrefix(s, "canceled"):
		return domain.StatusCanceled
	case s == "open":
		return domain.StatusOpen
	case s == "pending" || s == "in-progress":
		return domain.StatusPending
	}
	return domain.StatusUnknown
}

func (v *Venue) toUpdate(o wsOrder) domain.OrderUpdate {
	u := domain.OrderUpdate{
		Venue: v.cfg.Name, ClientID: o.ClientOrderIndex, OrderIndex: o.OrderIndex, MarketID: o.MarketIndex,
		Status: mapStatus(o.Status), FilledQty: domain.ParseF(o.FilledBase), FilledQuote: domain.ParseF(o.FilledQuote),
		Reason: o.Status, Ts: time.Now(),
	}
	return u
}

func (v *Venue) onOrders(raw []byte) {
	var m struct {
		Orders map[string][]wsOrder `json:"orders"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return
	}
	v.orderMu.Lock()
	subs := append([]chan domain.OrderUpdate(nil), v.orderSubs...)
	v.orderMu.Unlock()
	for _, list := range m.Orders {
		for _, o := range list {
			u := v.toUpdate(o)
			for _, c := range subs {
				select {
				case c <- u:
				default:
					slog.Warn("order update dropped (slow consumer)", "venue", v.cfg.Name, "cid", u.ClientID)
				}
			}
		}
	}
}

// SubscribeOrders streams account order updates (requires credentials).
func (v *Venue) SubscribeOrders(ctx context.Context) (<-chan domain.OrderUpdate, error) {
	if v.tx == nil {
		return nil, domain.ErrNoCredentials
	}
	ch := make(chan domain.OrderUpdate, 1024)
	v.orderMu.Lock()
	v.orderSubs = append(v.orderSubs, ch)
	v.orderMu.Unlock()
	v.ordersOnce.Do(func() {
		chName := fmt.Sprintf("account_all_orders/%d", v.cfg.AccountIndex)
		v.ws.subscribe(chName, func() (map[string]any, error) {
			tok, err := v.auth()
			if err != nil {
				return nil, err
			}
			return map[string]any{"type": "subscribe", "channel": chName, "auth": tok}, nil
		})
	})
	return ch, nil
}

// QueryOrder looks the order up via REST (active, then inactive).
// Returns StatusUnknown if not found (never treated as failure).
func (v *Venue) QueryOrder(ctx context.Context, inst domain.Instrument, clientID int64) (domain.OrderUpdate, error) {
	tok, err := v.auth()
	if err != nil {
		return domain.OrderUpdate{}, err
	}
	for _, path := range []string{"accountActiveOrders", "accountInactiveOrders"} {
		orders, err := v.rest.accountOrders(ctx, path, v.cfg.AccountIndex, inst.MarketID, tok)
		if err != nil {
			return domain.OrderUpdate{}, err
		}
		for _, o := range orders {
			if o.ClientOrderIndex == clientID {
				return v.toUpdate(o), nil
			}
		}
	}
	return domain.OrderUpdate{Venue: v.cfg.Name, ClientID: clientID, MarketID: inst.MarketID, Status: domain.StatusUnknown}, nil
}
