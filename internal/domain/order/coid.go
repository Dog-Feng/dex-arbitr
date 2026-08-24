package order

import "fmt"

// ClientOrderID 是确定性编码的客户端订单号。
//
// 位布局（共 48 位，兼容 Lighter 的 ClientOrderIndex 上限 2^48-1）：
//
//	47..40  (8)   slot     交易所在注册表中的固定索引
//	39..24  (16)  epoch    网格轮次，每次启动/调整区间/trailing 移动递增
//	23..12  (12)  cell     网格格子索引
//	11..4   (8)   purpose  订单用途
//	 3..0   (4)   seq      同一格子的重挂序号，模 16 循环
//
// 编码是确定性的：给定同样的 (slot, epoch, cell, purpose, seq) 一定得到同一个号。
// 这带来两个能力：对账时解码任意挂单即知归属，重复下单会被交易所以
// "重复 client order index" 拒绝而不是真的下出两单。
//
// purpose 取值从 1 开始，保证编码结果永远非零（0 是 Lighter 的 Nil 值）。
type ClientOrderID uint64

const (
	slotBits    = 8
	epochBits   = 16
	cellBits    = 12
	purposeBits = 8
	seqBits     = 4

	seqShift     = 0
	purposeShift = seqShift + seqBits
	cellShift    = purposeShift + purposeBits
	epochShift   = cellShift + cellBits
	slotShift    = epochShift + epochBits

	seqMask     = 1<<seqBits - 1
	purposeMask = 1<<purposeBits - 1
	cellMask    = 1<<cellBits - 1
	epochMask   = 1<<epochBits - 1
	slotMask    = 1<<slotBits - 1
)

const (
	// MaxSlot 是交易所槽位上限。255 保留，因此有效范围 0..254。
	MaxSlot = slotMask - 1
	// MaxEpoch 是轮次上限。
	MaxEpoch = epochMask
	// MaxCell 是网格格子索引上限，决定单个网格最多能有多少格。
	MaxCell = cellMask
	// MaxSeq 是重挂序号上限，超过后回绕。
	MaxSeq = seqMask
	// MaxClientOrderID 是编码结果的上限。
	MaxClientOrderID = 1<<48 - 1
)

// Purpose 描述订单在策略中扮演的角色。
type Purpose uint8

const (
	PurposeUnknown    Purpose = 0
	PurposeOpen       Purpose = 1 // 开仓腿
	PurposeClose      Purpose = 2 // 平仓腿
	PurposeEntry      Purpose = 3 // 初始建仓
	PurposeTakeProfit Purpose = 4
	PurposeStopLoss   Purpose = 5
	PurposeExit       Purpose = 6 // 停止时的平仓
)

func (p Purpose) String() string {
	switch p {
	case PurposeOpen:
		return "open"
	case PurposeClose:
		return "close"
	case PurposeEntry:
		return "entry"
	case PurposeTakeProfit:
		return "take_profit"
	case PurposeStopLoss:
		return "stop_loss"
	case PurposeExit:
		return "exit"
	default:
		return "unknown"
	}
}

// Ref 是 ClientOrderID 解码后的结构。
type Ref struct {
	Slot    uint8
	Epoch   uint16
	Cell    uint16
	Purpose Purpose
	Seq     uint8
}

func (r Ref) String() string {
	return fmt.Sprintf("slot=%d epoch=%d cell=%d purpose=%s seq=%d",
		r.Slot, r.Epoch, r.Cell, r.Purpose, r.Seq)
}

// Encode 把 Ref 编码成 ClientOrderID。
func Encode(r Ref) (ClientOrderID, error) {
	switch {
	case r.Slot > MaxSlot:
		return 0, fmt.Errorf("coid: slot %d exceeds max %d", r.Slot, MaxSlot)
	case r.Cell > MaxCell:
		return 0, fmt.Errorf("coid: cell %d exceeds max %d", r.Cell, MaxCell)
	case r.Purpose == PurposeUnknown:
		return 0, fmt.Errorf("coid: purpose must be set")
	case uint8(r.Purpose) > purposeMask:
		return 0, fmt.Errorf("coid: purpose %d exceeds max %d", r.Purpose, purposeMask)
	case r.Seq > MaxSeq:
		return 0, fmt.Errorf("coid: seq %d exceeds max %d", r.Seq, MaxSeq)
	}
	v := uint64(r.Slot)<<slotShift |
		uint64(r.Epoch)<<epochShift |
		uint64(r.Cell)<<cellShift |
		uint64(r.Purpose)<<purposeShift |
		uint64(r.Seq)<<seqShift
	return ClientOrderID(v), nil
}

// MustEncode 在参数非法时 panic，仅用于测试与已知安全的常量参数。
func MustEncode(r Ref) ClientOrderID {
	id, err := Encode(r)
	if err != nil {
		panic(err)
	}
	return id
}

// Decode 拆解出各字段。对不属于本系统的订单号，调用方应先用 Valid 过滤，
// 再用 Ref.Slot 判断是否属于当前交易所。
func (c ClientOrderID) Decode() Ref {
	v := uint64(c)
	return Ref{
		Slot:    uint8(v >> slotShift & slotMask),
		Epoch:   uint16(v >> epochShift & epochMask),
		Cell:    uint16(v >> cellShift & cellMask),
		Purpose: Purpose(v >> purposeShift & purposeMask),
		Seq:     uint8(v >> seqShift & seqMask),
	}
}

// Valid 判断编码是否落在合法区间内。
func (c ClientOrderID) Valid() bool {
	if c == 0 || uint64(c) > MaxClientOrderID {
		return false
	}
	return c.Decode().Purpose != PurposeUnknown
}

// NextSeq 返回同一格子重挂时的下一个序号，到达上限后回绕。
//
// 回绕是安全的：同一格子在同一 epoch 内重挂超过 16 次的情况下，
// 最早那批订单早已终结，不会与新单撞号。
func NextSeq(seq uint8) uint8 {
	return (seq + 1) & seqMask
}
