package replay

import (
	"testing"

	"github.com/holiman/uint256"

	"github.com/containerman17/flatstate/capture"
	"github.com/containerman17/flatstate/schema"
	"github.com/containerman17/flatstate/store"
)

var (
	addrA = schema.Address{0xaa}
	s1    = schema.Hash{1}
)

func h(b byte) schema.Hash { return schema.Hash{31: b} }

// history: S=10 baseline (A balance 5, s1=0x10), block 11 @t1000 (s1=0x21),
// block 12 @t2000 (s1=0x22, balance 7), mempool txs at t=500, 1500, 2500.
func newStore(t *testing.T) *store.DB {
	t.Helper()
	d, err := store.Open(t.TempDir(), 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	bl, err := d.NewBaseline(10)
	if err != nil {
		t.Fatal(err)
	}
	a := schema.Account{Balance: *uint256.NewInt(5)}
	if err := bl.Account(addrA, &a); err != nil {
		t.Fatal(err)
	}
	if err := bl.Slot(addrA, s1, h(0x10)); err != nil {
		t.Fatal(err)
	}
	if err := bl.Finish(); err != nil {
		t.Fatal(err)
	}
	blocks := []*capture.Batch{
		{Block: 11, Hash: h(11), Time: 1000, Ops: []capture.Op{
			{Kind: capture.OpSlot, Addr: addrA, Slot: s1, Value: h(0x21)},
		}},
		{Block: 12, Hash: h(12), Time: 2000, Ops: []capture.Op{
			{Kind: capture.OpSlot, Addr: addrA, Slot: s1, Value: h(0x22)},
			{Kind: capture.OpAccount, Addr: addrA, Account: schema.Account{Balance: *uint256.NewInt(7)}},
		}},
	}
	for _, b := range blocks {
		if err := d.WriteBlock(b); err != nil {
			t.Fatal(err)
		}
		if err := d.SetFinalized(b.Block); err != nil {
			t.Fatal(err)
		}
	}
	for i, tm := range []uint64{500, 1500, 2500} {
		if _, err := d.AppendMempool(tm, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	return d
}

func TestSessionInterleave(t *testing.T) {
	d := newStore(t)
	s, err := Open(d, 10)
	if err != nil {
		t.Fatal(err)
	}

	// Lazy seed at 10.
	if v, err := s.Slot(addrA, s1); err != nil || v != h(0x10) {
		t.Fatalf("seed slot = %x %v", v, err)
	}
	if a, exists, err := s.Account(addrA); err != nil || !exists || a.Balance.Uint64() != 5 {
		t.Fatalf("seed account = %+v %v %v", a, exists, err)
	}

	// Expected event order by timestamp: tx@500, block11@1000, tx@1500,
	// block12@2000, tx@2500, then caught up.
	type want struct {
		block uint64 // 0 = tx event
		time  uint64
	}
	seq := []want{{0, 500}, {11, 0}, {0, 1500}, {12, 0}, {0, 2500}}
	for i, w := range seq {
		ev, err := s.Next()
		if err != nil {
			t.Fatal(err)
		}
		if ev == nil {
			t.Fatalf("event %d: unexpected end", i)
		}
		if w.block == 0 {
			if ev.Tx == nil || ev.Time != w.time {
				t.Fatalf("event %d: want tx@%d, got %+v", i, w.time, ev)
			}
		} else {
			if ev.Block == nil || ev.Block.Block != w.block {
				t.Fatalf("event %d: want block %d, got %+v", i, w.block, ev)
			}
		}
		// State advances with the blocks: the cached slot is updated by the
		// same apply code as live.
		switch {
		case w.block == 11:
			if v, _ := s.Slot(addrA, s1); v != h(0x21) {
				t.Fatal("state after block 11 wrong")
			}
		case w.block == 12:
			if v, _ := s.Slot(addrA, s1); v != h(0x22) {
				t.Fatal("state after block 12 wrong")
			}
			if a, _, _ := s.Account(addrA); a.Balance.Uint64() != 7 {
				t.Fatal("account after block 12 wrong")
			}
		}
	}
	if ev, err := s.Next(); err != nil || ev != nil {
		t.Fatalf("want caught up, got %+v %v", ev, err)
	}
	if s.Block() != 12 {
		t.Fatalf("session height = %d", s.Block())
	}

	// A session tailing a live writer picks up new commits.
	b13 := &capture.Batch{Block: 13, Hash: h(13), Time: 3000, Ops: []capture.Op{
		{Kind: capture.OpSlot, Addr: addrA, Slot: s1, Value: h(0x23)},
	}}
	if err := d.WriteBlock(b13); err != nil {
		t.Fatal(err)
	}
	ev, err := s.Next()
	if err != nil || ev == nil || ev.Block == nil || ev.Block.Block != 13 {
		t.Fatalf("tail: %+v %v", ev, err)
	}
	if v, _ := s.Slot(addrA, s1); v != h(0x23) {
		t.Fatal("state after tailed block wrong")
	}
}

func TestOpenMidHistory(t *testing.T) {
	d := newStore(t)
	// Open at 11: mempool positioned at the first arrival >= t(11)=1000.
	s, err := Open(d, 11)
	if err != nil {
		t.Fatal(err)
	}
	if v, err := s.Slot(addrA, s1); err != nil || v != h(0x21) {
		t.Fatalf("seed at 11 = %x %v", v, err)
	}
	ev, err := s.Next()
	if err != nil || ev.Tx == nil || ev.Time != 1500 {
		t.Fatalf("first event at 11: %+v %v", ev, err)
	}
}

func TestOpenBounds(t *testing.T) {
	d := newStore(t)
	if _, err := Open(d, 9); err == nil {
		t.Fatal("open below genesis must fail")
	}
	empty, err := store.Open(t.TempDir(), 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer empty.Close()
	if _, err := Open(empty, 0); err == nil {
		t.Fatal("open with no genesis must fail")
	}
}
