package engine

import (
	"sync"
	"testing"

	"github.com/holiman/uint256"

	"github.com/containerman17/flatstate/capture"
	"github.com/containerman17/flatstate/mem"
	"github.com/containerman17/flatstate/schema"
	"github.com/containerman17/flatstate/store"
)

var (
	addrA = schema.Address{0xaa}
	s1    = schema.Hash{1}
	ch    = schema.Hash{0xc0}
)

func h(b byte) schema.Hash { return schema.Hash{31: b} }

func newState(t testing.TB) (*store.DB, *mem.State) {
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
	a := schema.Account{Balance: *uint256.NewInt(5), Nonce: 1, CodeHash: ch}
	if err := bl.Account(addrA, &a); err != nil {
		t.Fatal(err)
	}
	if err := bl.Slot(addrA, s1, h(0x11)); err != nil {
		t.Fatal(err)
	}
	if err := bl.Finish(); err != nil {
		t.Fatal(err)
	}
	st, err := mem.New(d)
	if err != nil {
		t.Fatal(err)
	}
	return d, st
}

// fake executor: optionally sets an overlay slot, then reads it back.
type call struct {
	set  *schema.Hash
	addr schema.Address
	slot schema.Hash
}

type fakeExec struct{}

func (fakeExec) Execute(c any, v *View) any {
	cc := c.(call)
	if cc.set != nil {
		v.SetSlot(cc.addr, cc.slot, *cc.set)
	}
	val, err := v.Slot(cc.addr, cc.slot)
	if err != nil {
		return err
	}
	return val
}

func newEngine(st *mem.State, n int) *Engine {
	execs := make([]Executor, n)
	for i := range execs {
		execs[i] = fakeExec{}
	}
	return New(st, execs)
}

func TestExecuteAndOverlayIsolation(t *testing.T) {
	_, st := newState(t)
	e := newEngine(st, 2)
	override := h(0x55)
	results := e.Execute([]any{
		call{addr: addrA, slot: s1},                 // plain read
		call{addr: addrA, slot: s1, set: &override}, // overlay write then read
		call{addr: addrA, slot: s1},                 // must not see the overlay
	})
	if results[0] != h(0x11) || results[2] != h(0x11) {
		t.Fatalf("plain reads = %x %x, want %x", results[0], results[2], h(0x11))
	}
	if results[1] != override {
		t.Fatalf("overlay read = %x, want %x", results[1], override)
	}
	// Overlays are per call and reset: a fresh batch sees base state.
	if r := e.Execute([]any{call{addr: addrA, slot: s1}}); r[0] != h(0x11) {
		t.Fatal("overlay leaked across calls")
	}
}

func TestStaleStampRequeue(t *testing.T) {
	_, st := newState(t)
	e := newEngine(st, 1)
	calls := []any{call{addr: addrA, slot: s1}}

	// run() stamps with the tip at RLock time.
	results, stamp := e.run(calls)
	if results[0] != h(0x11) || stamp != (schema.Hash{}) {
		t.Fatalf("run = %v stamp %x", results, stamp)
	}
	// Tip moves: the old stamp is now stale, Execute must rerun against the
	// new tip and return the layered value.
	st.ApplyBlock(&capture.Batch{Block: 101, Hash: h(101), Ops: []capture.Op{
		{Kind: capture.OpSlot, Addr: addrA, Slot: s1, Value: h(0x22)},
	}})
	if st.TipHash() == stamp {
		t.Fatal("stamp should be stale after ApplyBlock")
	}
	if r := e.Execute(calls); r[0] != h(0x22) {
		t.Fatalf("Execute after tip move = %x, want %x", r[0], h(0x22))
	}
}

// TestConcurrentExecuteAndBlocks runs batches against a moving tip under -race.
func TestConcurrentExecuteAndBlocks(t *testing.T) {
	d, st := newState(t)
	e := newEngine(st, 4)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			calls := []any{
				call{addr: addrA, slot: s1},
				call{addr: addrA, slot: s1},
				call{addr: addrA, slot: s1},
				call{addr: addrA, slot: s1},
			}
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, r := range e.Execute(calls) {
					if err, ok := r.(error); ok {
						t.Error(err)
						return
					}
				}
			}
		}()
	}
	for n := uint64(101); n <= 130; n++ {
		b := &capture.Batch{Block: n, Hash: h(byte(n)), Ops: []capture.Op{
			{Kind: capture.OpSlot, Addr: addrA, Slot: s1, Value: h(byte(n))},
		}}
		st.ApplyBlock(b)
		if err := d.WriteBlock(b); err != nil {
			t.Fatal(err)
		}
		if err := st.Finalize(n, h(byte(n))); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
}

// BenchmarkBatchPhaseTransition measures one full cycle: a batch of reads
// (read phase) plus an ApplyBlock+Finalize pair (two write phases).
func BenchmarkBatchPhaseTransition(b *testing.B) {
	d, st := newState(b)
	e := newEngine(st, PoolSize())
	calls := make([]any, 10)
	for i := range calls {
		calls[i] = call{addr: addrA, slot: s1}
	}
	e.Execute(calls) // warm the pins
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := uint64(101 + i)
		batch := &capture.Batch{Block: n, Hash: h(byte(n)), Ops: []capture.Op{
			{Kind: capture.OpSlot, Addr: addrA, Slot: s1, Value: h(byte(n))},
		}}
		st.ApplyBlock(batch)
		e.Execute(calls)
		if err := st.Finalize(n, h(byte(n))); err != nil {
			b.Fatal(err)
		}
	}
	_ = d
}
