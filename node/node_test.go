package node

import (
	"errors"
	"strings"
	"testing"

	"github.com/holiman/uint256"

	"github.com/containerman17/flatstate/capture"
	"github.com/containerman17/flatstate/mem"
	"github.com/containerman17/flatstate/schema"
	"github.com/containerman17/flatstate/store"
	"github.com/containerman17/flatstate/tipbus"
)

var (
	addrA = schema.Address{1}
	addrB = schema.Address{2}
	s1    = schema.Hash{31: 1}
	ch    = schema.Hash{30: 0xcc}
)

func h(b byte) schema.Hash { return schema.Hash{31: b} }

func acct(balance, nonce uint64) schema.Account {
	return schema.Account{Balance: *uint256.NewInt(balance), Nonce: nonce, CodeHash: ch}
}

func batch(block uint64, hash, parent schema.Hash, ops ...capture.Op) *capture.Batch {
	return &capture.Batch{Block: block, Hash: hash, Parent: parent, Time: block * 1000, Ops: ops}
}

func slotOp(addr schema.Address, slot, val schema.Hash) capture.Op {
	return capture.Op{Kind: capture.OpSlot, Addr: addr, Slot: slot, Value: val}
}

// rig builds a store with baseline at 100, a mem state, a tipbus, and the
// composite sink.
type rig struct {
	db   *store.DB
	st   *mem.State
	bus  *tipbus.Bus
	sink *Sink
}

func newRig(t *testing.T) *rig {
	t.Helper()
	db, err := store.Open(t.TempDir(), 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	bl, err := db.NewBaseline(100)
	if err != nil {
		t.Fatal(err)
	}
	a := acct(1, 1)
	if err := bl.Account(addrA, &a); err != nil {
		t.Fatal(err)
	}
	if err := bl.Finish(); err != nil {
		t.Fatal(err)
	}
	if err := db.SetFinalized(100); err != nil {
		t.Fatal(err)
	}
	st, err := mem.New(db)
	if err != nil {
		t.Fatal(err)
	}
	bus, err := tipbus.OpenWriter(t.TempDir(), 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { bus.Close() })
	return &rig{db: db, st: st, bus: bus, sink: NewSink(db, st, bus)}
}

func TestSinkD7Order(t *testing.T) {
	r := newRig(t)
	b101 := batch(101, h(0xA1), h(0), slotOp(addrA, s1, h(0x11)))

	if err := r.sink.Block(b101); err != nil {
		t.Fatal(err)
	}
	// Unfinalized: the store must not have the row yet (reads as zero).
	if v, err := r.db.GetSlot(addrA, s1, 101); err != nil || v != (schema.Hash{}) {
		t.Fatalf("unfinalized slot visible in store: %x, %v", v, err)
	}
	if err := r.sink.Finalize(101, h(0xA1)); err != nil {
		t.Fatal(err)
	}
	v, err := r.db.GetSlot(addrA, s1, 101)
	if err != nil || v != h(0x11) {
		t.Fatalf("finalized slot = %x, %v", v, err)
	}
	if fh, ok, _ := r.db.Finalized(); !ok || fh != 101 {
		t.Fatalf("finalized watermark = %d, %v", fh, ok)
	}
	if r.st.FinalizedHeight() != 101 {
		t.Fatalf("mem finalized = %d", r.st.FinalizedHeight())
	}
	// Bus carries block + finalize.
	seq, err := r.bus.Seq()
	if err != nil || seq != 2 {
		t.Fatalf("bus seq = %d, %v", seq, err)
	}
	evs, _, err := r.bus.Poll(0)
	if err != nil || len(evs) != 2 || evs[0].Kind != tipbus.EvBlock || evs[1].Kind != tipbus.EvFinalize {
		t.Fatalf("bus events = %+v, %v", evs, err)
	}
}

func TestSinkFinalizeUnknownBlock(t *testing.T) {
	r := newRig(t)
	if err := r.sink.Finalize(101, h(0xA1)); err == nil {
		t.Fatal("finalize of undelivered block must fail")
	}
}

func TestWriteFinal(t *testing.T) {
	r := newRig(t)
	b := batch(101, h(0xA1), h(0), slotOp(addrA, s1, h(0x11)))
	if err := WriteFinal(r.db, b); err != nil {
		t.Fatal(err)
	}
	if v, err := r.db.GetSlot(addrA, s1, 101); err != nil || v != h(0x11) {
		t.Fatalf("slot = %x, %v", v, err)
	}
	if fh, _, _ := r.db.Finalized(); fh != 101 {
		t.Fatalf("watermark = %d", fh)
	}
}

func TestTrackerExtendAndFinalize(t *testing.T) {
	r := newRig(t)
	tr := NewTracker(r.sink, 100, h(0))

	b101 := batch(101, h(0xA1), h(0), slotOp(addrA, s1, h(0x11)))
	b102 := batch(102, h(0xA2), h(0xA1), slotOp(addrA, s1, h(0x12)))

	tr.Verified(b101)
	if err := tr.Head(h(0xA1)); err != nil {
		t.Fatal(err)
	}
	tr.Verified(b102)
	if err := tr.Head(h(0xA2)); err != nil {
		t.Fatal(err)
	}
	// Two single-block extensions, no reset.
	evs, _, err := r.bus.Poll(0)
	if err != nil || len(evs) != 2 || evs[0].Kind != tipbus.EvBlock || evs[1].Kind != tipbus.EvBlock {
		t.Fatalf("bus events = %+v, %v", evs, err)
	}
	// Duplicate head is a no-op.
	if err := tr.Head(h(0xA2)); err != nil {
		t.Fatal(err)
	}
	if seq, _ := r.bus.Seq(); seq != 2 {
		t.Fatalf("duplicate head published, seq = %d", seq)
	}
	if err := tr.Accepted(101, h(0xA1)); err != nil {
		t.Fatal(err)
	}
	if fh, _, _ := r.db.Finalized(); fh != 101 {
		t.Fatalf("watermark = %d", fh)
	}
	// 102 stays published and finalizes next.
	if err := tr.Accepted(102, h(0xA2)); err != nil {
		t.Fatal(err)
	}
	if v, err := r.db.GetSlot(addrA, s1, 102); err != nil || v != h(0x12) {
		t.Fatalf("slot at 102 = %x, %v", v, err)
	}
}

func TestTrackerForkSwitchResets(t *testing.T) {
	r := newRig(t)
	tr := NewTracker(r.sink, 100, h(0))

	b101a := batch(101, h(0xA1), h(0), slotOp(addrA, s1, h(0x11)))
	b101b := batch(101, h(0xB1), h(0), slotOp(addrA, s1, h(0x21)))
	b102b := batch(102, h(0xB2), h(0xB1), slotOp(addrB, s1, h(0x22)))

	tr.Verified(b101a)
	tr.Verified(b101b)
	tr.Verified(b102b)
	if err := tr.Head(h(0xA1)); err != nil {
		t.Fatal(err)
	}
	// Preference jumps to the B fork: must reset, not extend.
	if err := tr.Head(h(0xB2)); err != nil {
		t.Fatal(err)
	}
	evs, _, err := r.bus.Poll(0)
	if err != nil || len(evs) != 2 || evs[1].Kind != tipbus.EvReset {
		t.Fatalf("bus events = %+v, %v", evs, err)
	}
	if len(evs[1].Batches) != 2 || evs[1].Batches[0].Hash != h(0xB1) {
		t.Fatalf("reset stack = %+v", evs[1].Batches)
	}
	if err := tr.Accepted(101, h(0xB1)); err != nil {
		t.Fatal(err)
	}
	if v, err := r.db.GetSlot(addrA, s1, 101); err != nil || v != h(0x21) {
		t.Fatalf("slot from B fork = %x, %v", v, err)
	}
}

func TestTrackerAcceptedWithoutHead(t *testing.T) {
	r := newRig(t)
	tr := NewTracker(r.sink, 100, h(0))
	b101 := batch(101, h(0xA1), h(0), slotOp(addrA, s1, h(0x11)))
	tr.Verified(b101)
	// Accept before any head event: tracker must publish then finalize.
	if err := tr.Accepted(101, h(0xA1)); err != nil {
		t.Fatal(err)
	}
	if fh, _, _ := r.db.Finalized(); fh != 101 {
		t.Fatalf("watermark = %d", fh)
	}
}

func TestTrackerUnknownBlocksFailLoud(t *testing.T) {
	r := newRig(t)
	tr := NewTracker(r.sink, 100, h(0))
	if err := tr.Head(h(0xA1)); err == nil {
		t.Fatal("head over unknown block must fail")
	}
	if err := tr.Accepted(101, h(0xA1)); err == nil {
		t.Fatal("accept of unknown block must fail")
	}
}

// --- baseline ---

type sliceIter struct {
	entries []Entry
	i       int
	err     error
}

func (s *sliceIter) Next() (Entry, bool, error) {
	if s.err != nil && s.i == len(s.entries) {
		return Entry{}, false, s.err
	}
	if s.i == len(s.entries) {
		return Entry{}, false, nil
	}
	e := s.entries[s.i]
	s.i++
	return e, true, nil
}

func TestRunBaseline(t *testing.T) {
	db, err := store.Open(t.TempDir(), 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	a := acct(5, 1)
	it := &sliceIter{entries: []Entry{
		{Kind: EntryAccount, Addr: addrA, Account: a},
		{Kind: EntryAccount, Addr: addrB, Account: a},
		{Kind: EntrySlot, Addr: addrA, Slot: s1, Value: h(0x11)},
		{Kind: EntrySlot, Addr: addrB, Slot: s1, Value: h(0x22)},
		{Kind: EntryCode, Hash: ch, Code: []byte{0xde, 0xad}},
	}}
	if err := RunBaseline(db, 100, it); err != nil {
		t.Fatal(err)
	}
	if done, _ := db.BaselineComplete(); !done {
		t.Fatal("baseline watermark not set")
	}
	if v, err := db.GetSlot(addrB, s1, 100); err != nil || v != h(0x22) {
		t.Fatalf("slot = %x, %v", v, err)
	}
	if code, err := db.GetCode(ch); err != nil || len(code) != 2 {
		t.Fatalf("code = %x, %v", code, err)
	}
}

func TestRunBaselineOrderViolations(t *testing.T) {
	cases := map[string][]Entry{
		"kind regression": {
			{Kind: EntrySlot, Addr: addrA, Slot: s1},
			{Kind: EntryAccount, Addr: addrA},
		},
		"duplicate key": {
			{Kind: EntryAccount, Addr: addrA},
			{Kind: EntryAccount, Addr: addrA},
		},
		"descending key": {
			{Kind: EntryAccount, Addr: addrB},
			{Kind: EntryAccount, Addr: addrA},
		},
	}
	for name, entries := range cases {
		db, err := store.Open(t.TempDir(), 1<<30)
		if err != nil {
			t.Fatal(err)
		}
		err = RunBaseline(db, 100, &sliceIter{entries: entries})
		if err == nil || !strings.Contains(err.Error(), "baseline entry") {
			t.Fatalf("%s: err = %v", name, err)
		}
		db.Close()
	}
}

func TestRunBaselineIteratorError(t *testing.T) {
	db, err := store.Open(t.TempDir(), 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	boom := errors.New("boom")
	if err := RunBaseline(db, 100, &sliceIter{err: boom}); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if done, _ := db.BaselineComplete(); done {
		t.Fatal("watermark set despite iterator error")
	}
}
