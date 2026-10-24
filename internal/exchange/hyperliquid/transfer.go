package hyperliquid

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
)

// usdcToken is the collateral token id of USDC on mainnet (token index 0).
const usdcToken = "USDC:0x6d1e7cde53ba9467b783cb7c530ce054"

// SendAsset moves the agent's own account collateral between perp dexs
// (source "" = default perps dex) or spot. Only the dex collateral token
// (USDC) may cross a perp-dex boundary. This is an agent-signed L1 action and
// is NOT part of the exchange.Venue contract; callers use it explicitly.
func (v *Venue) SendAsset(ctx context.Context, sourceDex, destinationDex string, amount string) error {
	if v.signer == nil || v.cfg.Account == "" {
		return domain.ErrNoCredentials
	}
	if err := v.lockSend(v.auxSendMu, ctx); err != nil {
		return err
	}
	defer func() { <-v.auxSendMu }()
	nonce := v.nonce()
	sig, err := v.signer.signSendAsset(v.cfg.Account, sourceDex, destinationDex, usdcToken, amount, nonce)
	if err != nil {
		return fmt.Errorf("sign transfer: %w", err)
	}
	resp, err := v.rest.send(ctx, exchangeRequest{
		Action: map[string]any{
			"type":           "agentSendAsset",
			"destination":    v.cfg.Account,
			"sourceDex":      sourceDex,
			"destinationDex": destinationDex,
			"token":          usdcToken,
			"amount":         amount,
			"fromSubAccount": "",
			"nonce":          nonce,
		},
		Nonce:     nonce,
		Signature: sig,
	})
	if err != nil {
		slog.Warn("transfer outcome unknown", "venue", v.cfg.Name, "src", sourceDex, "dst", destinationDex, "err", err)
		return err
	}
	if resp.Status != "ok" {
		return fmt.Errorf("transfer rejected: %s", rawResponse(resp))
	}
	return nil
}

// rawResponse renders the exchange error payload for logging.
func rawResponse(resp exchangeResponse) string {
	if statuses := resp.orderStatuses(); len(statuses) > 0 {
		if e := statuses[0].Error; e != nil {
			return *e
		}
	}
	if len(resp.Response) > 0 {
		return string(resp.Response)
	}
	return resp.Status
}
