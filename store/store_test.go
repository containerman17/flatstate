package store

import (
	"errors"
	"testing"

	"github.com/holiman/uint256"

	"github.com/containerman17/flatstate/capture"
	"github.com/containerman17/flatstate/schema"
)

const testMapSize = 1 << 30

var (
	addrA = schema.Address{0xaa}
	addrB = schema.Address{0xbb}
	addrC = schema.Address{0xcc}
	addrD = schema.Address{0xdd}
	addrE = schema.Address{0xee}
	s1    = schema.Hash{1}
	s2    = schema.Hash{2}
	cs    = schema.Hash{3}
	ds    = schema.Hash{4}
	ds2   = schema.Hash{5}
	ch    = schema.Hash{0xc0}
)

func acct(balance uint64, nonce uint64, codeHash schema.Hash) schema.Account {
	return schema.Account{Balance: *uint256.NewInt(balance), Nonce: nonce, CodeHash: codeHash}
}

func h(b byte) schema.Hash { return schema.Hash{31: b} }

// buildHistory loads a baseline at S=100 and writes blocks 101-103.
func buildHistory(t *testing.T, d *DB) {
	t.Helper()
	bl, err := d.NewBaseline(100)
	if err != nil {
		t.Fatal(err)
	}
	aA := acct(1, 1, ch)
	if err := bl.Account(addrA, &aA); err != nil {
		t.Fatal(err)
	}
	aB := acct(2, 0, schema.Hash{})
	if err := bl.Account(addrB, &aB); err != nil {
		t.Fatal(err)
	}
	if err := bl.Slot(addrA, s1, h(0x11)); err != nil {
		t.Fatal(err)
	}
	if err := bl.Code(ch, []byte{0xde, 0xad}); err != nil {
		t.Fatal(err)
	}

	// Baseline incomplete: an uncovered key must fail loud.
	if err := bl.flush(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := d.GetAccount(addrC, 100); !errors.Is(err, ErrBaselineIncomplete) {
		t.Fatalf("want ErrBaselineIncomplete, got %v", err)
	}
	if _, err := d.GetSlot(addrA, s2, 100); !errors.Is(err, ErrBaselineIncomplete) {
		t.Fatalf("want ErrBaselineIncomplete, got %v", err)
	}
	// A covered key answers even before the watermark.
	if v, err := d.GetSlot(addrA, s1, 100); err != nil || v != h(0x11) {
		t.Fatalf("covered key during baseline: %v %v", v, err)
	}
	if err := bl.Finish(); err != nil {
		t.Fatal(err)
	}

	// 101: A changes, C and D created.
	b101 := &capture.Batch{Block: 101, Hash: h(101), Parent: h(100), Time: 1000, Ops: []capture.Op{
		{Kind: capture.OpAccount, Addr: addrA, Account: acct(11, 2, ch)},
		{Kind: capture.OpSlot, Addr: addrA, Slot: s1, Value: h(0x12)},
		{Kind: capture.OpSlot, Addr: addrA, Slot: s2, Value: h(0x99)},
		{Kind: capture.OpAccount, Addr: addrC, Account: acct(3, 0, schema.Hash{})},
		{Kind: capture.OpSlot, Addr: addrC, Slot: cs, Value: h(0x31)},
		{Kind: capture.OpAccount, Addr: addrD, Account: acct(4, 0, schema.Hash{})},
		{Kind: capture.OpSlot, Addr: addrD, Slot: ds, Value: h(0x41)},
		{Kind: capture.OpSlot, Addr: addrD, Slot: ds2, Value: h(0x42)},
	}}
	// 102: C destructed, A slot s1 cleared.
	b102 := &capture.Batch{Block: 102, Hash: h(102), Parent: h(101), Time: 2000, Ops: []capture.Op{
		{Kind: capture.OpDestruct, Addr: addrC},
		{Kind: capture.OpDeleteSlot, Addr: addrA, Slot: s1},
	}}
	// 103: C recreated fresh; D destructed and recreated in the same block.
	b103 := &capture.Batch{Block: 103, Hash: h(103), Parent: h(102), Time: 3000, Ops: []capture.Op{
		{Kind: capture.OpAccount, Addr: addrC, Account: acct(30, 0, schema.Hash{})},
		{Kind: capture.OpDestruct, Addr: addrD},
		{Kind: capture.OpAccount, Addr: addrD, Account: acct(40, 0, schema.Hash{})},
		{Kind: capture.OpSlot, Addr: addrD, Slot: ds, Value: h(0x43)},
	}}
	for _, b := range []*capture.Batch{b101, b102, b103} {
		if err := d.WriteBlock(b); err != nil {
			t.Fatal(err)
		}
		if err := d.SetFinalized(b.Block); err != nil {
			t.Fatal(err)
		}
	}
}

func TestIntegration(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, testMapSize)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	buildHistory(t, d)

	checkReads := func(t *testing.T, d *DB) {
		t.Helper()
		// Below genesis fails loud.
		if _, _, err := d.GetAccount(addrA, 99); !errors.Is(err, ErrBelowGenesis) {
			t.Fatalf("want ErrBelowGenesis, got %v", err)
		}
		// Account history at multiple heights.
		for _, tc := range []struct {
			addr    schema.Address
			at      uint64
			balance uint64
			exists  bool
		}{
			{addrA, 100, 1, true},
			{addrA, 101, 11, true},
			{addrA, Latest, 11, true},
			{addrB, 103, 2, true},
			{addrC, 100, 0, false}, // never existed yet
			{addrC, 101, 3, true},
			{addrC, 102, 0, false}, // destructed
			{addrC, 103, 30, true}, // recreated
			{addrD, 102, 4, true},
			{addrD, 103, 40, true}, // destruct+recreate same block
			{addrE, 103, 0, false}, // never existed
		} {
			a, exists, err := d.GetAccount(tc.addr, tc.at)
			if err != nil {
				t.Fatalf("GetAccount(%x, %d): %v", tc.addr[0], tc.at, err)
			}
			if exists != tc.exists || (exists && a.Balance.Uint64() != tc.balance) {
				t.Fatalf("GetAccount(%x, %d) = %v/%v, want %d/%v", tc.addr[0], tc.at, a.Balance.Uint64(), exists, tc.balance, tc.exists)
			}
		}
		// Slot history, tombstones, destruct semantics.
		for _, tc := range []struct {
			addr schema.Address
			slot schema.Hash
			at   uint64
			want schema.Hash
		}{
			{addrA, s1, 100, h(0x11)},
			{addrA, s1, 101, h(0x12)},
			{addrA, s1, 102, schema.Hash{}}, // deleted (zero tombstone)
			{addrA, s2, 100, schema.Hash{}}, // baseline complete: absent = zero
			{addrA, s2, 101, h(0x99)},
			{addrC, cs, 101, h(0x31)},
			{addrC, cs, 102, schema.Hash{}}, // destructed after last write
			{addrC, cs, 103, schema.Hash{}}, // recreated fresh: still zero
			{addrD, ds, 102, h(0x41)},
			{addrD, ds, 103, h(0x43)},        // rewritten in the destruct+recreate block
			{addrD, ds2, 103, schema.Hash{}}, // wiped by same-block destruct+recreate
			{addrE, s1, 103, schema.Hash{}},  // never existed
		} {
			v, err := d.GetSlot(tc.addr, tc.slot, tc.at)
			if err != nil {
				t.Fatalf("GetSlot(%x, %x, %d): %v", tc.addr[0], tc.slot, tc.at, err)
			}
			if v != tc.want {
				t.Fatalf("GetSlot(%x, %x, %d) = %x, want %x", tc.addr[0], tc.slot, tc.at, v, tc.want)
			}
		}
		// Code.
		code, err := d.GetCode(ch)
		if err != nil || string(code) != "\xde\xad" {
			t.Fatalf("GetCode: %x %v", code, err)
		}
		if _, err := d.GetCode(schema.Hash{0x77}); !errors.Is(err, ErrNotFound) {
			t.Fatalf("missing code: want ErrNotFound, got %v", err)
		}
		// Per-block diff.
		diff, err := d.GetDiff(101)
		if err != nil || diff.Block != 101 || len(diff.Ops) != 8 || diff.Time != 1000 {
			t.Fatalf("GetDiff(101): %+v %v", diff, err)
		}
		if _, err := d.GetDiff(999); !errors.Is(err, ErrNotFound) {
			t.Fatalf("missing diff: want ErrNotFound, got %v", err)
		}
		// Watermarks.
		if fh, ok, _ := d.Finalized(); !ok || fh != 103 {
			t.Fatalf("finalized = %d/%v", fh, ok)
		}
		if s, ok, _ := d.Genesis(); !ok || s != 100 {
			t.Fatalf("genesis = %d/%v", s, ok)
		}
	}

	checkReads(t, d)

	// Second read-only "process" over the same files.
	ro, err := OpenReadOnly(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	checkReads(t, ro)

	// The reader sees new commits without reopening (MVCC per-txn snapshots).
	b104 := &capture.Batch{Block: 104, Hash: h(104), Parent: h(103), Time: 4000, Ops: []capture.Op{
		{Kind: capture.OpAccount, Addr: addrA, Account: acct(12, 3, ch)},
	}}
	if err := d.WriteBlock(b104); err != nil {
		t.Fatal(err)
	}
	if a, _, err := ro.GetAccount(addrA, 104); err != nil || a.Balance.Uint64() != 12 {
		t.Fatalf("read-only handle missed new block: %v %v", a.Balance.Uint64(), err)
	}
}

func TestDestructEdge(t *testing.T) {
	d, err := Open(t.TempDir(), testMapSize)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	buildHistory(t, d)
	// Contract-violating batch: slot write and destruct in one block with no
	// recreating account row. The store must refuse to guess (D13).
	bad := &capture.Batch{Block: 104, Hash: h(104), Parent: h(103), Time: 4000, Ops: []capture.Op{
		{Kind: capture.OpSlot, Addr: addrE, Slot: s1, Value: h(0x51)},
		{Kind: capture.OpDestruct, Addr: addrE},
	}}
	if err := d.WriteBlock(bad); err != nil {
		t.Fatal(err)
	}
	if _, err := d.GetSlot(addrE, s1, 104); !errors.Is(err, ErrDestructEdge) {
		t.Fatalf("want ErrDestructEdge, got %v", err)
	}
}

func TestNoGenesis(t *testing.T) {
	d, err := Open(t.TempDir(), testMapSize)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if _, _, err := d.GetAccount(addrA, 1); !errors.Is(err, ErrNoGenesis) {
		t.Fatalf("want ErrNoGenesis, got %v", err)
	}
}

func TestMempoolLog(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, testMapSize)
	if err != nil {
		t.Fatal(err)
	}
	for i, tx := range []string{"tx0", "tx1", "tx2"} {
		seq, err := d.AppendMempool(uint64(1000*(i+1)), []byte(tx))
		if err != nil || seq != uint64(i) {
			t.Fatalf("append %d: seq=%d err=%v", i, seq, err)
		}
	}
	tm, tx, ok, err := d.GetMempool(1)
	if err != nil || !ok || tm != 2000 || string(tx) != "tx1" {
		t.Fatalf("GetMempool(1) = %d %q %v %v", tm, tx, ok, err)
	}
	if _, _, ok, _ := d.GetMempool(3); ok {
		t.Fatal("seq 3 should not exist")
	}
	if seq, ok, _ := d.FirstMempoolAt(1500); !ok || seq != 1 {
		t.Fatalf("FirstMempoolAt(1500) = %d %v", seq, ok)
	}
	if _, ok, _ := d.FirstMempoolAt(9999); ok {
		t.Fatal("FirstMempoolAt past end should miss")
	}
	// Seq counter survives reopen (dense log).
	d.Close()
	d, err = Open(dir, testMapSize)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if seq, err := d.AppendMempool(4000, []byte("tx3")); err != nil || seq != 3 {
		t.Fatalf("after reopen: seq=%d err=%v", seq, err)
	}
}

func BenchmarkGetSlot(b *testing.B) {
	d, err := Open(b.TempDir(), testMapSize)
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()
	bl, _ := d.NewBaseline(100)
	a := acct(1, 1, ch)
	_ = bl.Account(addrA, &a)
	_ = bl.Slot(addrA, s1, h(0x11))
	if err := bl.Finish(); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := d.GetSlot(addrA, s1, 100); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetAccount(b *testing.B) {
	d, err := Open(b.TempDir(), testMapSize)
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()
	bl, _ := d.NewBaseline(100)
	a := acct(1, 1, ch)
	_ = bl.Account(addrA, &a)
	if err := bl.Finish(); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := d.GetAccount(addrA, 100); err != nil {
			b.Fatal(err)
		}
	}
}
