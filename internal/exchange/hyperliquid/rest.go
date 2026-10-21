package hyperliquid

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const defaultInfoURL = "https://api.hyperliquid.xyz/info"
const defaultExchangeURL = "https://api.hyperliquid.xyz/exchange"

type restClient struct {
	infoURL     string
	exchangeURL string
	client      *http.Client
}

func newRestClient(infoURL, exchangeURL string) *restClient {
	return &restClient{infoURL: infoURL, exchangeURL: exchangeURL, client: &http.Client{Timeout: 10 * time.Second}}
}

func validURL(s string, allowWS bool) bool {
	u, err := url.Parse(s)
	if err != nil || u.Host == "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return false
	}
	if allowWS {
		return u.Scheme == "http" || u.Scheme == "https" || u.Scheme == "ws" || u.Scheme == "wss"
	}
	return u.Scheme == "http" || u.Scheme == "https"
}

// postInfo sends an info request and returns the raw JSON response body.
func (r *restClient) postInfo(ctx context.Context, req map[string]any) (json.RawMessage, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("encode info request: %w", err)
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, r.infoURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("info request: %w", err)
	}
	hreq.Header.Set("Content-Type", "application/json")
	resp, err := r.client.Do(hreq)
	if err != nil {
		return nil, fmt.Errorf("info: %w", err)
	}
	defer closeRespBody(resp.Body)
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("info response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("info: http %d", resp.StatusCode)
	}
	return raw, nil
}

// metaItem is one perp universe entry from the meta info response.
type metaItem struct {
	Name             string `json:"name"`
	SzDecimals       int    `json:"szDecimals"`
	MaxLeverage      int    `json:"maxLeverage"`
	MarginTableID    int    `json:"marginTableId"`
	OnlyIsolated     bool   `json:"onlyIsolated"`
	IsDelisted       bool   `json:"isDelisted"`
	MarginMode       string `json:"marginMode"`
	GrowthMode       string `json:"growthMode"`
	DeployerFeeScale string `json:"deployerFeeScale"`
}

type metaResponse struct {
	Universe []metaItem `json:"universe"`
}

func (r *restClient) meta(ctx context.Context, dex string) (metaResponse, error) {
	req := map[string]any{"type": "meta"}
	if dex != "" {
		req["dex"] = dex
	}
	raw, err := r.postInfo(ctx, req)
	if err != nil {
		return metaResponse{}, err
	}
	var out metaResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return metaResponse{}, fmt.Errorf("meta: invalid JSON: %w", err)
	}
	if len(out.Universe) == 0 {
		return metaResponse{}, fmt.Errorf("meta: empty universe")
	}
	return out, nil
}

// clearinghouseState is the subset of a perp-dex account summary this
// adapter consumes.
type clearinghouseState struct {
	AssetPositions []struct {
		Position struct {
			Coin           string      `json:"coin"`
			EntryPx        json.Number `json:"entryPx"`
			LiquidationPx  json.Number `json:"liquidationPx"`
			Szi            json.Number `json:"szi"`
			MarginUsed     json.Number `json:"marginUsed"`
			PositionValue  json.Number `json:"positionValue"`
			UnrealizedPnl  json.Number `json:"unrealizedPnl"`
			ReturnOnEquity json.Number `json:"returnOnEquity"`
			CumFunding     struct {
				SinceOpen json.Number `json:"sinceOpen"`
			} `json:"cumFunding"`
			Leverage struct {
				Type string `json:"type"`
			} `json:"leverage"`
		} `json:"position"`
	} `json:"assetPositions"`
	MarginSummary struct {
		AccountValue    json.Number `json:"accountValue"`
		TotalNtlPos     json.Number `json:"totalNtlPos"`
		TotalRawUsd     json.Number `json:"totalRawUsd"`
		TotalMarginUsed json.Number `json:"totalMarginUsed"`
	} `json:"marginSummary"`
	CrossMarginSummary struct {
		AccountValue    json.Number `json:"accountValue"`
		TotalNtlPos     json.Number `json:"totalNtlPos"`
		TotalRawUsd     json.Number `json:"totalRawUsd"`
		TotalMarginUsed json.Number `json:"totalMarginUsed"`
	} `json:"crossMarginSummary"`
	CrossMaintenanceMarginUsed json.Number `json:"crossMaintenanceMarginUsed"`
}

func (r *restClient) clearinghouse(ctx context.Context, user, dex string) (clearinghouseState, error) {
	req := map[string]any{"type": "clearinghouseState", "user": strings.ToLower(user)}
	if dex != "" {
		req["dex"] = dex
	}
	raw, err := r.postInfo(ctx, req)
	if err != nil {
		return clearinghouseState{}, err
	}
	var out clearinghouseState
	if err := json.Unmarshal(raw, &out); err != nil {
		return clearinghouseState{}, fmt.Errorf("clearinghouseState: invalid JSON: %w", err)
	}
	return out, nil
}

// orderStatusResponse carries the order status for an oid or cloid.
type orderStatusResponse struct {
	Status string `json:"status"` // "order" | "error" | "unknownOid"
	Order  *struct {
		Order struct {
			Coin    string      `json:"coin"`
			Oid     json.Number `json:"oid"`
			Sz      json.Number `json:"sz"`
			OrigSz  json.Number `json:"origSz"`
			Side    string      `json:"side"`
			LimitPx json.Number `json:"limitPx"`
		} `json:"order"`
		Status          string `json:"status"`
		StatusTimestamp int64  `json:"statusTimestamp"`
	} `json:"order"`
}

func (r *restClient) orderStatus(ctx context.Context, user, dex, id string) (orderStatusResponse, error) {
	req := map[string]any{"type": "orderStatus", "user": strings.ToLower(user), "oid": id}
	if dex != "" {
		req["dex"] = dex
	}
	raw, err := r.postInfo(ctx, req)
	if err != nil {
		return orderStatusResponse{}, err
	}
	var out orderStatusResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return orderStatusResponse{}, fmt.Errorf("orderStatus: invalid JSON: %w", err)
	}
	return out, nil
}

// userFill is one fill entry from userFillsByTime.
type userFill struct {
	Coin  string      `json:"coin"`
	Px    json.Number `json:"px"`
	Sz    json.Number `json:"sz"`
	Side  string      `json:"side"`
	Time  int64       `json:"time"`
	Oid   json.Number `json:"oid"`
	Cloid string      `json:"cloid"`
	Tid   int64       `json:"tid"`
	Hash  string      `json:"hash"`
	Fee   json.Number `json:"fee"`
}

// fillsByTime returns fills in the half-open window [start, end].
func (r *restClient) fillsByTime(ctx context.Context, user string, start, end int64) ([]userFill, error) {
	req := map[string]any{"type": "userFillsByTime", "user": strings.ToLower(user), "startTime": start, "endTime": end}
	raw, err := r.postInfo(ctx, req)
	if err != nil {
		return nil, err
	}
	var out []userFill
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("userFillsByTime: invalid JSON: %w", err)
	}
	return out, nil
}

// exchangeRequest is the signed-action envelope posted to /exchange.
type exchangeRequest struct {
	Action    map[string]any `json:"action"`
	Nonce     uint64         `json:"nonce"`
	Signature signature      `json:"signature"`
}

// orderStatusEntry is one per-order outcome in the exchange response.
type orderStatusEntry struct {
	Resting *struct {
		Oid json.Number `json:"oid"`
	} `json:"resting"`
	Filled *struct {
		Oid     json.Number `json:"oid"`
		TotalSz json.Number `json:"totalSz"`
		AvgPx   json.Number `json:"avgPx"`
	} `json:"filled"`
	Error *string `json:"error"`
}

type exchangeResponse struct {
	Status   string          `json:"status"`
	Response json.RawMessage `json:"response"`
}

// orderStatuses parses the order-action response envelope into per-order
// outcomes. Returns nil when the response has a different shape (e.g. a
// string error payload).
func (r exchangeResponse) orderStatuses() []orderStatusEntry {
	var env struct {
		Type string `json:"type"`
		Data *struct {
			Statuses []orderStatusEntry `json:"statuses"`
		} `json:"data"`
	}
	if len(r.Response) == 0 || json.Unmarshal(r.Response, &env) != nil || env.Data == nil {
		return nil
	}
	return env.Data.Statuses
}

// send posts a signed action and returns the parsed response.
func (r *restClient) send(ctx context.Context, req exchangeRequest) (exchangeResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return exchangeResponse{}, fmt.Errorf("encode action: %w", err)
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, r.exchangeURL, bytes.NewReader(body))
	if err != nil {
		return exchangeResponse{}, fmt.Errorf("exchange request: %w", err)
	}
	hreq.Header.Set("Content-Type", "application/json")
	resp, err := r.client.Do(hreq)
	if err != nil {
		return exchangeResponse{}, fmt.Errorf("exchange: %w", err)
	}
	defer closeRespBody(resp.Body)
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return exchangeResponse{}, fmt.Errorf("exchange response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return exchangeResponse{}, fmt.Errorf("exchange: http %d", resp.StatusCode)
	}
	var out exchangeResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return exchangeResponse{}, fmt.Errorf("exchange: invalid JSON: %w", err)
	}
	return out, nil
}

func number(v json.Number) (float64, error) {
	if v == "" {
		return 0, nil
	}
	f, err := strconv.ParseFloat(string(v), 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, fmt.Errorf("invalid decimal value")
	}
	return f, nil
}

// sigFmt formats a price/quantity the way the exchange accepts it, mirroring
// the official SDK's float_to_wire: round to 8 decimals, then strip trailing
// zeros. Callers round to native precision first, so the 8-decimal rounding
// is a no-op and the signed bytes match the reference implementation.
func sigFmt(f float64) string {
	s := strconv.FormatFloat(f, 'f', 8, 64)
	if !strings.Contains(s, ".") {
		return s
	}
	s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	if s == "" || s == "-" {
		return "0"
	}
	return s
}

func closeRespBody(body io.Closer) {
	if err := body.Close(); err != nil {
		slog.Warn("hyperliquid response body close failed", "err", err)
	}
}
