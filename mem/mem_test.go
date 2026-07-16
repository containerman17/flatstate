package mem

import (
	"sync"
	"testing"

	"github.com/holiman/uint256"

	"github.com/containerman17/flatstate/capture"
	"github.com/containerman17/flatstate/schema"
	"github.com/containerman17/flatstate/store"
)

var (
	addrA = schema.Address{0xaa}
	addrB = schema.Address{0xbb}
	s1    = schema.Hash{1}
	s2    = schema.Hash{2}
	ch    = schema.Hash{0xc0}
)

func h(b byte) schema.Hash { return schema.Hash{31: b} }

func acct(balance uint64, nonce uint64) schema.Account {
	return schema.Account{Balance: *uint256.NewInt(balance), Nonce: nonce, CodeHash: ch}
}

// newStore builds a store with baseline at 100: A(balance 1, slot s1=0x11),
// code ch.
func newStore(t testing.TB) *store.DB {
	t.Helper()
	d, err := store.Open(t.TempDir(), 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	bl, err := d.NewBaseline(100)
	if err != nil {
		t.Fatal(err)
	}
	a := acct(1, 1)
	if err := bl.Account(addrA, &a); err != nil {
		t.Fatal(err)
	}
	if err := bl.Slot(addrA, s1, h(0x11)); err != nil {
		t.Fatal(err)
	}
	if err := bl.Code(ch, []byte{0xde, 0xad}); err != nil {
		t.Fatal(err)
	}
	if err := bl.Finish(); err != nil {
		t.Fatal(err)
	}
	if err := d.SetFinalized(100); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestPinOnMissAndMerge(t *testing.T) {
	d := newStore(t)
	st, err := New(d)
	if err != nil {
		t.Fatal(err)
	}
	sb := NewSideBuffer()
	st.Register(sb)

	stamp := st.BeginBatch()
	if stamp != (schema.Hash{}) {
		t.Fatal("tip should start zero")
	}
	// Cold miss: reads LMDB, records into sb, base untouched.
	v, err := st.Slot(addrA, s1, sb)
	if err != nil || v != h(0x11) {
		t.Fatalf("slot = %x %v", v, err)
	}
	if _, ok := st.base.Account(addrA); ok {
		t.Fatal("read phase must not write the base map")
	}
	// The slot pin dragged the account pin along.
	if _, ok := sb.accounts[addrA]; !ok {
		t.Fatal("slot pin must record the account too")
	}
	// Second read hits the side buffer (within-batch cache).
	if v, _ := st.Slot(addrA, s1, sb); v != h(0x11) {
		t.Fatal("side buffer miss")
	}
	// Known-nonexistent account pins too.
	if _, exists, err := st.Account(addrB, sb); err != nil || exists {
		t.Fatalf("B should not exist: %v", err)
	}
	if c, err := st.Code(ch, sb); err != nil || string(c) != "\xde\xad" {
		t.Fatalf("code: %x %v", c, err)
	}
	st.EndBatch()

	// Write phase merges the side buffers into the base first.
	b101 := &capture.Batch{Block: 101, Hash: h(101), Time: 1000, Ops: []capture.Op{
		{Kind: capture.OpSlot, Addr: addrA, Slot: s1, Value: h(0x12)},
		{Kind: capture.OpSlot, Addr: addrA, Slot: s2, Value: h(0x99)}, // s2 never cached
	}}
	st.ApplyBlock(b101)
	if a, ok := st.base.Account(addrA); !ok || a.Storage[s1] != h(0x11) {
		t.Fatal("merge did not land the pin in the base")
	}
	if a, ok := st.base.Account(addrB); !ok || a.Exists {
		t.Fatal("nonexistence pin lost")
	}
	if len(sb.slots) != 0 || len(sb.accounts) != 0 {
		t.Fatal("side buffer must be cleared after merge")
	}

	// Unfinalized layer shadows the base.
	stamp = st.BeginBatch()
	if stamp != h(101) {
		t.Fatal("tip stamp should be the unfinalized block hash")
	}
	if v, _ := st.Slot(addrA, s1, sb); v != h(0x12) {
		t.Fatal("layer must shadow base")
	}
	st.EndBatch()

	// Finalize: LMDB first (D7), then apply to cached keys only.
	if err := d.WriteBlock(b101); err != nil {
		t.Fatal(err)
	}
	if err := st.Finalize(101, h(101)); err != nil {
		t.Fatal(err)
	}
	if err := d.SetFinalized(101); err != nil {
		t.Fatal(err)
	}
	a, _ := st.base.Account(addrA)
	if a.Storage[s1] != h(0x12) {
		t.Fatal("finalize must apply cached slot")
	}
	if _, known := a.Storage[s2]; known {
		t.Fatal("finalize must skip uncached slots")
	}
	// The skipped slot still reads correctly via LMDB.
	st.BeginBatch()
	if v, _ := st.Slot(addrA, s2, sb); v != h(0x99) {
		t.Fatal("uncached slot must read current value from LMDB")
	}
	st.EndBatch()
	if st.FinalizedHeight() != 101 {
		t.Fatal("finalized height not bumped")
	}
}

func TestFinalizeMismatch(t *testing.T) {
	st, err := New(newStore(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Finalize(101, h(101)); err == nil {
		t.Fatal("finalize with no layers must fail")
	}
	st.ApplyBlock(&capture.Batch{Block: 101, Hash: h(101)})
	if err := st.Finalize(102, h(102)); err == nil {
		t.Fatal("finalize of wrong block must fail")
	}
	if err := st.Finalize(101, h(99)); err == nil {
		t.Fatal("finalize with wrong hash must fail")
	}
}

func TestDestructApply(t *testing.T) {
	d := newStore(t)
	st, err := New(d)
	if err != nil {
		t.Fatal(err)
	}
	sb := NewSideBuffer()
	st.Register(sb)

	st.BeginBatch()
	if _, _, err := st.Account(addrA, sb); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Slot(addrA, s1, sb); err != nil {
		t.Fatal(err)
	}
	st.EndBatch()

	b := &capture.Batch{Block: 101, Hash: h(101), Ops: []capture.Op{
		{Kind: capture.OpDestruct, Addr: addrA},
	}}
	st.ApplyBlock(b)

	// Layer view: destructed account reads nonexistent, slots zero.
	st.BeginBatch()
	if _, exists, _ := st.Account(addrA, sb); exists {
		t.Fatal("destructed account must not exist in layer view")
	}
	if v, _ := st.Slot(addrA, s1, sb); v != (schema.Hash{}) {
		t.Fatal("destructed slot must read zero in layer view")
	}
	st.EndBatch()

	if err := d.WriteBlock(b); err != nil {
		t.Fatal(err)
	}
	if err := st.Finalize(101, h(101)); err != nil {
		t.Fatal(err)
	}
	a, ok := st.base.Account(addrA)
	if !ok || a.Exists {
		t.Fatal("destruct apply must keep the entry as known-nonexistent")
	}
	if v, known := a.Storage[s1]; !known || v != (schema.Hash{}) {
		t.Fatal("destruct apply must zero known slots")
	}
}

func TestPreferenceReset(t *testing.T) {
	st, err := New(newStore(t))
	if err != nil {
		t.Fatal(err)
	}
	sb := NewSideBuffer()
	st.Register(sb)
	st.ApplyBlock(&capture.Batch{Block: 101, Hash: h(1), Ops: []capture.Op{
		{Kind: capture.OpSlot, Addr: addrA, Slot: s1, Value: h(0x77)},
	}})
	st.ApplyBlock(&capture.Batch{Block: 102, Hash: h(2)})
	// Reset to a different 101' without the slot write.
	st.PreferenceReset([]*capture.Batch{{Block: 101, Hash: h(3)}})
	stamp := st.BeginBatch()
	if stamp != h(3) {
		t.Fatalf("tip after reset = %x", stamp)
	}
	if v, _ := st.Slot(addrA, s1, sb); v != h(0x11) {
		t.Fatal("dropped layer must not shadow the base")
	}
	st.EndBatch()
}

func TestMapPinSlotWithoutAccountPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("PinSlot without PinAccount must panic")
		}
	}()
	NewMap().PinSlot(addrA, s1, h(1))
}

// TestConcurrentBatches exercises the D9 discipline under -race: readers in
// batches with per-executor side buffers, a writer applying and finalizing
// blocks.
func TestConcurrentBatches(t *testing.T) {
	d := newStore(t)
	st, err := New(d)
	if err != nil {
		t.Fatal(err)
	}
	const readers = 4
	sbs := make([]*SideBuffer, readers)
	for i := range sbs {
		sbs[i] = NewSideBuffer()
		st.Register(sbs[i])
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(sb *SideBuffer) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				st.BeginBatch()
				if _, err := st.Slot(addrA, s1, sb); err != nil {
					t.Error(err)
				}
				if _, _, err := st.Account(addrA, sb); err != nil {
					t.Error(err)
				}
				st.EndBatch()
			}
		}(sbs[i])
	}
	for n := uint64(101); n <= 150; n++ {
		b := &capture.Batch{Block: n, Hash: h(byte(n)), Time: n * 10, Ops: []capture.Op{
			{Kind: capture.OpSlot, Addr: addrA, Slot: s1, Value: h(byte(n))},
			{Kind: capture.OpAccount, Addr: addrA, Account: acct(n, n)},
		}}
		st.ApplyBlock(b)
		if err := d.WriteBlock(b); err != nil {
			t.Fatal(err)
		}
		if err := st.Finalize(n, h(byte(n))); err != nil {
			t.Fatal(err)
		}
		if err := d.SetFinalized(n); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
	a, ok := st.base.Account(addrA)
	if !ok || a.Storage[s1] != h(150) || a.Nonce != 150 {
		t.Fatalf("final base state wrong: %+v", a)
	}
}

func BenchmarkBaseMapRead(b *testing.B) {
	d := newStore(b)
	st, err := New(d)
	if err != nil {
		b.Fatal(err)
	}
	sb := NewSideBuffer()
	st.Register(sb)
	// Warm the base: pin then merge via a no-op write phase.
	st.BeginBatch()
	if _, err := st.Slot(addrA, s1, sb); err != nil {
		b.Fatal(err)
	}
	st.EndBatch()
	st.PreferenceReset(nil)
	b.ResetTimer()
	st.BeginBatch()
	defer st.EndBatch()
	for i := 0; i < b.N; i++ {
		if _, err := st.Slot(addrA, s1, sb); err != nil {
			b.Fatal(err)
		}
	}
}
