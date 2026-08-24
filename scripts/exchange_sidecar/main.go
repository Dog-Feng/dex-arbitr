// exchange_sidecar：统一 Lighter + SoDEX 实盘层（对齐 internal/exchange/）。
// Rust dex-arbitr 经 stdin JSON 调用，无需 Python。
//
//	{"cmd":"account|place|cancel|order_status","venue_yaml":"...","params":{...}}
//
// 构建：cd scripts/exchange_sidecar && go build -o exchange_sidecar .
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type request struct {
	Cmd       string         `json:"cmd"`
	VenueYAML string         `json:"venue_yaml"`
	Params    map[string]any `json:"params"`
}

type response struct {
	OK    bool   `json:"ok"`
	Data  any    `json:"data,omitempty"`
	Error string `json:"error,omitempty"`
}

func main() {
	raw, err := io.ReadAll(os.Stdin)
	if err != nil {
		respond(false, nil, err.Error())
		return
	}
	var req request
	if err := json.Unmarshal(raw, &req); err != nil {
		respond(false, nil, "invalid json: "+err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	data, err := dispatch(ctx, req)
	if err != nil {
		respond(false, nil, err.Error())
		return
	}
	respond(true, data, "")
}

func respond(ok bool, data any, errMsg string) {
	out, _ := json.Marshal(response{OK: ok, Data: data, Error: errMsg})
	fmt.Println(string(out))
}

func dispatch(ctx context.Context, req request) (any, error) {
	id, err := peekVenueID(req.VenueYAML)
	if err != nil {
		return nil, err
	}
	switch id {
	case "lighter", "lighter_rh":
		return dispatchLighter(ctx, req)
	case "sodex":
		return dispatchSodex(ctx, req)
	default:
		return nil, fmt.Errorf("unsupported venue %q", id)
	}
}

func peekVenueID(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	var meta struct {
		ID string `yaml:"id"`
	}
	if err := yaml.Unmarshal(b, &meta); err != nil {
		return "", err
	}
	id := strings.TrimSpace(meta.ID)
	if id == "" {
		return "", fmt.Errorf("venue id missing in %s", path)
	}
	return id, nil
}
