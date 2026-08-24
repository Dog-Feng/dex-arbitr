package engine

import (
	"dex-grid/internal/domain/account"
	"dex-grid/internal/domain/position"
	"dex-grid/internal/domain/strategy"

	"github.com/shopspring/decimal"
)

// CommandKind 是 Runner 接受的页面命令。比策略命令多了启动/停止/重连/存配置。
type CommandKind uint8

const (
	CmdStart CommandKind = iota
	CmdStop
	CmdAdjustRange
	CmdCancelOrders
	CmdRefill
	CmdResetStats
	CmdReconnect
	CmdSaveConfig
)

func (k CommandKind) String() string {
	switch k {
	case CmdStart:
		return "start"
	case CmdStop:
		return "stop"
	case CmdAdjustRange:
		return "adjust_range"
	case CmdCancelOrders:
		return "cancel_orders"
	case CmdRefill:
		return "refill"
	case CmdResetStats:
		return "reset_stats"
	case CmdReconnect:
		return "reconnect"
	case CmdSaveConfig:
		return "save_config"
	default:
		return "unknown"
	}
}

// Command 是一条投递给 Runner 的命令。
type Command struct {
	Kind    CommandKind
	Payload any
	Reply   chan CommandResult
}

// CommandResult 是命令的回执，直接回给 API。
type CommandResult struct {
	OK      bool         `json:"ok"`
	Message string       `json:"message,omitempty"`
	View    InstanceView `json:"view"`
}

// StartPayload 是 CmdStart 的载荷。
type StartPayload struct {
	Symbol string
	Entry  strategy.EntryParams
	Risk   strategy.RiskParams
}

// Status 是实例对外状态。
type Status uint8

const (
	StatusStopped Status = iota
	StatusStarting
	StatusRunning
	StatusPaused
	StatusReconnecting
	StatusError
)

func (s Status) String() string {
	switch s {
	case StatusStarting:
		return "starting"
	case StatusRunning:
		return "running"
	case StatusPaused:
		return "paused"
	case StatusReconnecting:
		return "reconnecting"
	case StatusError:
		return "error"
	default:
		return "stopped"
	}
}

// InstanceView 是控制台「账户状态」卡片与层级表的聚合视图。
type InstanceView struct {
	Exchange   string            `json:"exchange"`
	Status     string            `json:"status"`
	StopReason string            `json:"stop_reason,omitempty"`
	Residual   bool              `json:"residual"`
	Entering   bool              `json:"entering"`
	Symbol     string            `json:"symbol"`
	Mark       decimal.Decimal   `json:"mark"`
	Position   position.Position `json:"position"`
	Account    account.Snapshot  `json:"account"`
	Strategy   strategy.View     `json:"strategy"`
}
