package order

import "testing"

func TestEncodeDecodeRoundTrip(t *testing.T) {
	cases := []Ref{
		{Slot: 0, Epoch: 1, Cell: 0, Purpose: PurposeOpen, Seq: 0},
		{Slot: 254, Epoch: MaxEpoch, Cell: MaxCell, Purpose: PurposeExit, Seq: MaxSeq},
		{Slot: 3, Epoch: 1234, Cell: 80, Purpose: PurposeClose, Seq: 7},
		{Slot: 1, Epoch: 0, Cell: 4095, Purpose: PurposeEntry, Seq: 15},
	}
	for _, want := range cases {
		id, err := Encode(want)
		if err != nil {
			t.Fatalf("Encode(%+v) failed: %v", want, err)
		}
		if !id.Valid() {
			t.Fatalf("Encode(%+v) produced invalid id %d", want, id)
		}
		if uint64(id) > MaxClientOrderID {
			t.Fatalf("Encode(%+v) = %d exceeds 48-bit limit", want, id)
		}
		if got := id.Decode(); got != want {
			t.Fatalf("round trip mismatch: got %+v want %+v", got, want)
		}
	}
}

func TestEncodeRejectsOutOfRange(t *testing.T) {
	cases := map[string]Ref{
		"slot 255 reserved": {Slot: 255, Epoch: 1, Cell: 0, Purpose: PurposeOpen},
		"cell overflow":     {Slot: 0, Epoch: 1, Cell: MaxCell + 1, Purpose: PurposeOpen},
		"seq overflow":      {Slot: 0, Epoch: 1, Cell: 0, Purpose: PurposeOpen, Seq: MaxSeq + 1},
		"purpose unset":     {Slot: 0, Epoch: 1, Cell: 0},
	}
	for name, ref := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Encode(ref); err == nil {
				t.Fatalf("Encode(%+v) should have failed", ref)
			}
		})
	}
}

// 编码结果必须永远非零：Lighter 用 0 表示「无客户端订单号」。
func TestEncodeNeverZero(t *testing.T) {
	id, err := Encode(Ref{Slot: 0, Epoch: 0, Cell: 0, Purpose: PurposeOpen, Seq: 0})
	if err != nil {
		t.Fatal(err)
	}
	if id == 0 {
		t.Fatal("encoded id must not be zero")
	}
}

func TestDifferentFieldsProduceDifferentIDs(t *testing.T) {
	base := Ref{Slot: 2, Epoch: 10, Cell: 20, Purpose: PurposeOpen, Seq: 3}
	seen := map[ClientOrderID]Ref{}
	variants := []Ref{
		base,
		{Slot: 3, Epoch: 10, Cell: 20, Purpose: PurposeOpen, Seq: 3},
		{Slot: 2, Epoch: 11, Cell: 20, Purpose: PurposeOpen, Seq: 3},
		{Slot: 2, Epoch: 10, Cell: 21, Purpose: PurposeOpen, Seq: 3},
		{Slot: 2, Epoch: 10, Cell: 20, Purpose: PurposeClose, Seq: 3},
		{Slot: 2, Epoch: 10, Cell: 20, Purpose: PurposeOpen, Seq: 4},
	}
	for _, v := range variants {
		id := MustEncode(v)
		if prev, dup := seen[id]; dup {
			t.Fatalf("collision: %+v and %+v both encode to %d", prev, v, id)
		}
		seen[id] = v
	}
}

func TestNextSeqWraps(t *testing.T) {
	if got := NextSeq(MaxSeq); got != 0 {
		t.Fatalf("NextSeq(%d) = %d, want 0", MaxSeq, got)
	}
	if got := NextSeq(0); got != 1 {
		t.Fatalf("NextSeq(0) = %d, want 1", got)
	}
}

func TestInvalidIDs(t *testing.T) {
	if ClientOrderID(0).Valid() {
		t.Fatal("zero must be invalid")
	}
	if ClientOrderID(MaxClientOrderID + 1).Valid() {
		t.Fatal("above 48-bit limit must be invalid")
	}
	// purpose 位为 0 的号码不是本系统生成的
	if ClientOrderID(1).Valid() {
		t.Fatal("id with zero purpose must be invalid")
	}
}
