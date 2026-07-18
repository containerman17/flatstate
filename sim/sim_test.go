package sim

import (
	"sync"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/crypto"
	"github.com/holiman/uint256"

	"github.com/containerman17/flatstate/capture"
	"github.com/containerman17/flatstate/engine"
	"github.com/containerman17/flatstate/mem"
	"github.com/containerman17/flatstate/schema"
	"github.com/containerman17/flatstate/store"
)

// Deterministic synthetic baseline (same approach as follower/exec tests) at
// S=100, with handwritten contracts covering the real read/write paths:
// mapping loads (ERC20 balanceOf shape with SHA3), plain SLOAD, SSTORE,
// revert, and value transfer.
var (
	// balanceOf(address): SLOAD(keccak(pad32(addr) || pad32(0))), return it.
	tokenCode = []byte{
		0x60, 0x04, 0x35, // PUSH1 4 CALLDATALOAD
		0x60, 0x00, 0x52, // PUSH1 0 MSTORE
		0x60, 0x00, 0x60, 0x20, 0x52, // PUSH1 0 PUSH1 32 MSTORE
		0x60, 0x40, 0x60, 0x00, 0x20, // PUSH1 64 PUSH1 0 SHA3
		0x54,             // SLOAD
		0x60, 0x00, 0x52, // PUSH1 0 MSTORE
		0x60, 0x20, 0x60, 0x00, 0xf3, // RETURN mem[0:32]
	}
	// getter: return SLOAD(0)
	getterCode = []byte{0x60, 0x00, 0x54, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3}
	// counter: SSTORE(0, SLOAD(0)+1), then return SLOAD(0)
	counterCode = []byte{
		0x60, 0x00, 0x54, 0x60, 0x01, 0x01, 0x60, 0x00, 0x55,
		0x60, 0x00, 0x54, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3,
	}
	// reverter: SSTORE(0, 1) then REVERT(0, 0)
	reverterCode = []byte{0x60, 0x01, 0x60, 0x00, 0x55, 0x60, 0x00, 0x60, 0x00, 0xfd}
	// selfbalance: return SELFBALANCE
	selfBalanceCode = []byte{0x47, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3}

	senderAddr      = common.HexToAddress("0x00000000000000000000000000000000000000aa")
	tokenAddr       = common.HexToAddress("0x00000000000000000000000000000000000000d0")
	getterAddr      = common.HexToAddress("0x00000000000000000000000000000000000000e0")
	counterAddr     = common.HexToAddress("0x00000000000000000000000000000000000000c0")
	reverterAddr    = common.HexToAddress("0x00000000000000000000000000000000000000f0")
	selfBalanceAddr = common.HexToAddress("0x00000000000000000000000000000000000000b0")
	// holderAddr is chosen so its mapping slot keccak has bit 0 of byte 0
	// SET (0xfb...), locking the state-key normalization regression: the
	// contract SLOADs the raw slot, the store row lives at the normalized
	// key (found live on UniV3 factory.getPool).
	holderAddr = common.HexToAddress("0x1000000000000000000000000000000000000002")
)

const (
	baseHeight = 100
	baseTime   = uint64(1_760_000_000) // recent mainnet-like: current upgrades active
)

func h(b byte) schema.Hash { return schema.Hash{31: b} }

// holderSlot is the STORE key of the mapping slot the token contract
// computes in-EVM: normalized, exactly as the follower's capture and the
// baseline loader key it (coreth multicoin normalization).
func holderSlot(addr common.Address) schema.Hash {
	var buf [64]byte
	copy(buf[12:32], addr[:])
	return schema.Hash(normSlot(crypto.Keccak256Hash(buf[:])))
}

func deploy(t testing.TB, bl *store.Baseline, addr common.Address, code []byte, slots map[schema.Hash]schema.Hash) {
	t.Helper()
	ch := schema.Hash(crypto.Keccak256Hash(code))
	if err := bl.Account(schema.Address(addr), &schema.Account{Nonce: 1, CodeHash: ch}); err != nil {
		t.Fatal(err)
	}
	if err := bl.Code(ch, code); err != nil {
		t.Fatal(err)
	}
	for k, v := range slots {
		if err := bl.Slot(schema.Address(addr), k, v); err != nil {
			t.Fatal(err)
		}
	}
}

// newState builds the baseline store, the mem state, and applies one tip
// block at 101 (TipInfo requires a tip, D13).
func newState(t testing.TB) (*store.DB, *mem.State) {
	db, err := store.Open(t.TempDir(), 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	bl, err := db.NewBaseline(baseHeight)
	if err != nil {
		t.Fatal(err)
	}
	if err := bl.Account(schema.Address(senderAddr), &schema.Account{Balance: *uint256.NewInt(1_000_000)}); err != nil {
		t.Fatal(err)
	}
	deploy(t, bl, tokenAddr, tokenCode, map[schema.Hash]schema.Hash{holderSlot(holderAddr): h(0x77)})
	deploy(t, bl, getterAddr, getterCode, map[schema.Hash]schema.Hash{h(0): h(0x11)})
	deploy(t, bl, counterAddr, counterCode, map[schema.Hash]schema.Hash{h(0): h(0x41)})
	deploy(t, bl, reverterAddr, reverterCode, nil)
	deploy(t, bl, selfBalanceAddr, selfBalanceCode, nil)
	if err := bl.Finish(); err != nil {
		t.Fatal(err)
	}
	st, err := mem.New(db)
	if err != nil {
		t.Fatal(err)
	}
	st.ApplyBlock(tipBatch(101, nil))
	return db, st
}

func tipBatch(block uint64, ops []capture.Op) *capture.Batch {
	return &capture.Batch{Block: block, Hash: h(byte(block)), Time: (baseTime + block) * 1000, Ops: ops}
}

func newEngine(t testing.TB, st *mem.State, n int) *engine.Engine {
	execs, err := NewPool(n)
	if err != nil {
		t.Fatal(err)
	}
	return engine.New(st, execs)
}

func one(t *testing.T, e *engine.Engine, c *Call) *Result {
	t.Helper()
	r := e.Execute([]any{c})[0].(*Result)
	return r
}

func wantWord(t *testing.T, r *Result, want schema.Hash) {
	t.Helper()
	if r.Err != nil {
		t.Fatalf("call err: %v", r.Err)
	}
	if len(r.ReturnData) != 32 || schema.Hash(common.BytesToHash(r.ReturnData)) != want {
		t.Fatalf("return = %x, want %x", r.ReturnData, want)
	}
}

func balanceOfCall(holder common.Address) *Call {
	input := make([]byte, 36)
	copy(input[:4], []byte{0x70, 0xa0, 0x82, 0x31})
	copy(input[16:36], holder[:])
	return &Call{From: senderAddr, To: tokenAddr, Input: input}
}

// TestBalanceOfMapping exercises the real ERC20 read shape: in-EVM keccak of
// the mapping slot, then SLOAD through the full read path.
func TestBalanceOfMapping(t *testing.T) {
	_, st := newState(t)
	e := newEngine(t, st, 2)
	r := one(t, e, balanceOfCall(holderAddr))
	wantWord(t, r, h(0x77))
	if r.GasUsed == 0 {
		t.Fatal("gasUsed = 0")
	}
	// Unknown holder reads a known zero.
	wantWord(t, one(t, e, balanceOfCall(common.HexToAddress("0xdead"))), schema.Hash{})
}

// TestPrecedence checks overlay -> unfinalized layers -> baseline order.
func TestPrecedence(t *testing.T) {
	_, st := newState(t)
	e := newEngine(t, st, 2)
	getter := &Call{From: senderAddr, To: getterAddr}

	wantWord(t, one(t, e, getter), h(0x11)) // baseline

	st.ApplyBlock(tipBatch(102, []capture.Op{
		{Kind: capture.OpSlot, Addr: schema.Address(getterAddr), Slot: h(0), Value: h(0x22)},
	}))
	wantWord(t, one(t, e, getter), h(0x22)) // unfinalized layer wins

	over := &Call{From: senderAddr, To: getterAddr, StorageOverrides: map[common.Address]map[common.Hash]common.Hash{
		getterAddr: {common.Hash(h(0)): common.Hash(h(0x33))},
	}}
	wantWord(t, one(t, e, over), h(0x33)) // per-call override wins
	wantWord(t, one(t, e, getter), h(0x22))
}

// TestWriteIsolation: SSTOREs live in the per-call overlay only.
func TestWriteIsolation(t *testing.T) {
	_, st := newState(t)
	e := newEngine(t, st, 2)
	counter := &Call{From: senderAddr, To: counterAddr}
	wantWord(t, one(t, e, counter), h(0x42)) // 0x41 + 1
	wantWord(t, one(t, e, counter), h(0x42)) // unchanged: previous write discarded
	getter := &Call{From: senderAddr, To: getterAddr}
	wantWord(t, one(t, e, getter), h(0x11))
}

// TestRevert: journaled writes unwind, error surfaces, executor stays usable.
func TestRevert(t *testing.T) {
	_, st := newState(t)
	e := newEngine(t, st, 1)
	r := one(t, e, &Call{From: senderAddr, To: reverterAddr})
	if !r.Reverted() {
		t.Fatalf("err = %v, want revert", r.Err)
	}
	if r.GasUsed == 0 {
		t.Fatal("revert consumed no gas")
	}
	wantWord(t, one(t, e, &Call{From: senderAddr, To: getterAddr}), h(0x11))
}

// TestValueTransferAndBalanceOverride: value moves through the journaled
// overlay; balance overrides fund a poor sender.
func TestValueTransferAndBalanceOverride(t *testing.T) {
	_, st := newState(t)
	e := newEngine(t, st, 1)

	r := one(t, e, &Call{From: senderAddr, To: selfBalanceAddr, Value: uint256.NewInt(5)})
	wantWord(t, r, h(5)) // contract had 0, received 5

	// A sender the baseline never funded cannot transfer...
	poor := common.HexToAddress("0x9999")
	r = one(t, e, &Call{From: poor, To: selfBalanceAddr, Value: uint256.NewInt(5)})
	if r.Err == nil {
		t.Fatal("expected insufficient balance")
	}
	// ...unless a balance override funds it (D14: balance overrides).
	r = one(t, e, &Call{From: poor, To: selfBalanceAddr, Value: uint256.NewInt(5),
		BalanceOverrides: map[common.Address]*uint256.Int{poor: uint256.NewInt(10)}})
	wantWord(t, r, h(5))
}

// TestConcurrentWithBlocks hammers simulations while the tip moves; run
// under -race. Results must always be a value some applied tip held.
func TestConcurrentWithBlocks(t *testing.T) {
	d, st := newState(t)
	e := newEngine(t, st, 4)
	valid := map[schema.Hash]bool{h(0x11): true}
	var mu sync.Mutex
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			calls := []any{
				&Call{From: senderAddr, To: getterAddr},
				&Call{From: senderAddr, To: getterAddr},
				balanceOfCall(holderAddr),
				&Call{From: senderAddr, To: counterAddr},
			}
			for {
				select {
				case <-stop:
					return
				default:
				}
				res := e.Execute(calls)
				for _, ri := range res[:2] {
					r := ri.(*Result)
					if r.Err != nil {
						t.Error(r.Err)
						return
					}
					got := schema.Hash(common.BytesToHash(r.ReturnData))
					mu.Lock()
					ok := valid[got]
					mu.Unlock()
					if !ok {
						t.Errorf("getter returned %x, never a tip value", got)
						return
					}
				}
			}
		}()
	}
	for n := uint64(102); n <= 130; n++ {
		b := tipBatch(n, []capture.Op{
			{Kind: capture.OpSlot, Addr: schema.Address(getterAddr), Slot: h(0), Value: h(byte(n))},
		})
		mu.Lock()
		valid[h(byte(n))] = true
		mu.Unlock()
		st.ApplyBlock(b)
		if err := d.WriteBlock(b); err != nil {
			t.Fatal(err)
		}
		// Finalize the previous block: one layer always stays unfinalized,
		// like the live 1s-block/2-3s-finality cadence.
		if err := st.Finalize(n-1, h(byte(n-1))); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
}

// BenchmarkBalanceOf is the hot-path regression guard: a full engine batch
// of ERC20-shaped reads (keccak + SLOAD) against a warm base map. Reported
// per CALL, not per batch.
func BenchmarkBalanceOf(b *testing.B) {
	_, st := newState(b)
	e := newEngine(b, st, engine.PoolSize())
	const batch = 10
	calls := make([]any, batch)
	for i := range calls {
		calls[i] = balanceOfCall(holderAddr)
	}
	e.Execute(calls) // warm pins + jumpdest analysis
	b.ResetTimer()
	for i := 0; i < b.N; i += batch {
		for _, r := range e.Execute(calls) {
			if err := r.(*Result).Err; err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkBalanceOfSingle is the per-call latency floor through the engine
// (batch of one).
func BenchmarkBalanceOfSingle(b *testing.B) {
	_, st := newState(b)
	e := newEngine(b, st, 1)
	calls := []any{balanceOfCall(holderAddr)}
	e.Execute(calls)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := e.Execute(calls)[0].(*Result).Err; err != nil {
			b.Fatal(err)
		}
	}
}
