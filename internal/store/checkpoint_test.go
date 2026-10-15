package store

import "testing"

func TestCheckpointExitRequiresDurableUnresolvedMarker(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	st := PairState{NextID: 1, Unresolved: true, CheckpointExit: true, PnLUncertain: true}
	if err := s.Save("pair", st); err != nil {
		t.Fatal(err)
	}
	// Clearing just unresolved must never create a durable state whose exit
	// guard could be ignored on restart. A rejected save retains the old file.
	st.Unresolved = false
	if err := s.Save("pair", st); err == nil {
		t.Fatal("accepted checkpoint_exit without unresolved")
	}
	got, err := s.Load("pair")
	if err != nil || !got.Unresolved || !got.CheckpointExit || !got.PnLUncertain {
		t.Fatalf("invalid save changed the recovery state: %+v, %v", got, err)
	}
}
