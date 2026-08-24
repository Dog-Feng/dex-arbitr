package engine

import (
	"dex-grid/internal/domain/order"
)

// Persist 是 Runner 可选的落盘端口。实现放在 infra/store，避免 app 依赖 SQLite。
type Persist interface {
	SaveRuntime(exchange, status, reason string, epoch uint16, snapshot []byte) error
	RecordFill(exchange string, o order.Order) error
}

func (r *Runner) persist() {
	if r.cfg.Persist == nil || r.strat == nil {
		return
	}
	snap, err := r.strat.Snapshot()
	if err != nil {
		r.log.Warn("snapshot failed", "err", err)
		snap = nil
	}
	reason := ""
	if r.status == StatusStopped || r.status == StatusError {
		reason = r.stopReason.String()
	}
	if err := r.cfg.Persist.SaveRuntime(r.cfg.Name, r.status.String(), reason, r.epoch, snap); err != nil {
		r.log.Warn("persist runtime failed", "err", err)
	}
}

func (r *Runner) recordFill(o order.Order) {
	r.logOrderFill(o)
	if r.cfg.Persist == nil || !o.FilledQty.IsPositive() {
		return
	}
	if !o.ClientOrderID.Valid() {
		return
	}
	if o.ClientOrderID.Decode().Slot != r.cfg.Slot {
		return
	}
	if err := r.cfg.Persist.RecordFill(r.cfg.Name, o); err != nil {
		r.log.Warn("record fill failed", "err", err)
	}
}
