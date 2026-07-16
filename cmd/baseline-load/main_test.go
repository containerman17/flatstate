package main

import (
	"log/slog"
	"math/big"
	"testing"

	"github.com/ava-labs/avalanchego/vms/evm/sync/customrawdb"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/ethdb"
	"github.com/ava-labs/libevm/rlp"
	"github.com/holiman/uint256"

	"github.com/containerman17/flatstate/follower/net"
	"github.com/containerman17/flatstate/schema"
	"github.com/containerman17/flatstate/store"
)

const testS = uint64(1000)

type fixture struct {
	chaindb ethdb.Database
	sHash   common.Hash

	eoa      schema.Address // balance 5, nonce 7, no code
	contract schema.Address // code + two slots
	codeHash schema.Hash
	code     []byte
	slotA    schema.Hash // slot whose physical 0x08 key sorts first
	slotB    schema.Hash
	valA     schema.Hash
	valB     schema.Hash
}

// makeFixture builds a fake node chaindb: a 3-header chain whose block S
// root matches the snapshot root marker (acceptor tip two blocks above S),
// snapshot rows for two accounts, two slots, and one code blob.
func makeFixture(t *testing.T) *fixture {
	t.Helper()
	net.RegisterExtras()
	f := &fixture{chaindb: rawdb.NewMemoryDatabase()}

	f.eoa = schema.Address{0x11}
	f.contract = schema.Address{0x22}
	f.code = []byte{0x60, 0x00, 0x60, 0x00, 0xfd}
	f.codeHash = store.Keccak(f.code)

	// Headers: S (root = snapshot root), S+1, S+2 (acceptor tip).
	snapRoot := common.Hash{0xaa}
	parent := common.Hash{}
	var tip common.Hash
	for i := uint64(0); i <= 2; i++ {
		h := &types.Header{
			Number:     new(big.Int).SetUint64(testS + i),
			ParentHash: parent,
			Root:       snapRoot,
		}
		if i > 0 {
			h.Root = common.Hash{0xbb, byte(i)} // post-S roots differ
		}
		rawdb.WriteHeader(f.chaindb, h)
		parent = h.Hash()
		tip = h.Hash()
		if i == 0 {
			f.sHash = h.Hash()
		}
	}
	if err := customrawdb.WriteAcceptorTip(f.chaindb, tip); err != nil {
		t.Fatal(err)
	}
	rawdb.WriteSnapshotRoot(f.chaindb, snapRoot)

	// Snapshot accounts (slim RLP under keccak(addr)).
	eoaSlim := types.SlimAccountRLP(types.StateAccount{
		Nonce:    7,
		Balance:  uint256.NewInt(5),
		Root:     types.EmptyRootHash,
		CodeHash: types.EmptyCodeHash[:],
	})
	rawdb.WriteAccountSnapshot(f.chaindb, common.Hash(store.Keccak(f.eoa[:])), eoaSlim)
	conSlim := types.SlimAccountRLP(types.StateAccount{
		Nonce:    1,
		Balance:  uint256.NewInt(0),
		Root:     common.Hash{0xcc}, // non-empty storage root round-trips
		CodeHash: f.codeHash[:],
	})
	conHash := common.Hash(store.Keccak(f.contract[:]))
	rawdb.WriteAccountSnapshot(f.chaindb, conHash, conSlim)

	// Two storage slots, named so slotA's hashed key sorts before slotB's.
	s1, s2 := schema.Hash{0x01}, schema.Hash{0x02}
	h1, h2 := store.Keccak(s1[:]), store.Keccak(s2[:])
	f.slotA, f.slotB = s1, s2
	if string(h2[:]) < string(h1[:]) {
		f.slotA, f.slotB = s2, s1
		h1, h2 = h2, h1
	}
	f.valA = schema.Hash{30: 0x0a, 31: 0xbc} // short value: RLP trims zeros
	f.valB = schema.Hash{0xff}               // full 32-byte value
	for i, sv := range []struct {
		h common.Hash
		v schema.Hash
	}{{common.Hash(h1), f.valA}, {common.Hash(h2), f.valB}} {
		trimmed := sv.v[:]
		for len(trimmed) > 0 && trimmed[0] == 0 {
			trimmed = trimmed[1:]
		}
		enc, err := rlp.EncodeToBytes(trimmed)
		if err != nil {
			t.Fatalf("slot %d: %v", i, err)
		}
		rawdb.WriteStorageSnapshot(f.chaindb, conHash, sv.h, enc)
	}

	rawdb.WriteCode(f.chaindb, common.Hash(f.codeHash), f.code)
	return f
}

func openStore(t *testing.T) *store.DB {
	t.Helper()
	db, err := store.Open(t.TempDir(), 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestFindPivot(t *testing.T) {
	f := makeFixture(t)
	s, hash, err := findPivot(f.chaindb)
	if err != nil {
		t.Fatal(err)
	}
	if s != testS || hash != f.sHash {
		t.Fatalf("pivot = %d %x, want %d %x", s, hash, testS, f.sHash)
	}
}

func TestLoad(t *testing.T) {
	f := makeFixture(t)
	db := openStore(t)
	if err := load(db, f.chaindb, testS, slog.Default()); err != nil {
		t.Fatal(err)
	}
	done, err := db.BaselineComplete()
	if err != nil || !done {
		t.Fatalf("baseline complete = %v, %v", done, err)
	}

	a, exists, err := db.GetAccount(f.eoa, testS)
	if err != nil || !exists {
		t.Fatalf("eoa: exists=%v err=%v", exists, err)
	}
	if a.Nonce != 7 || a.Balance.Uint64() != 5 || a.CodeHash != schema.Hash(types.EmptyCodeHash) {
		t.Fatalf("eoa account = %+v", a)
	}
	c, exists, err := db.GetAccount(f.contract, testS)
	if err != nil || !exists || c.CodeHash != f.codeHash {
		t.Fatalf("contract: exists=%v err=%v acct=%+v", exists, err, c)
	}
	for _, sv := range []struct {
		slot, want schema.Hash
	}{{f.slotA, f.valA}, {f.slotB, f.valB}} {
		got, err := db.GetSlot(f.contract, sv.slot, testS)
		if err != nil || got != sv.want {
			t.Fatalf("slot %x = %x, %v; want %x", sv.slot, got, err, sv.want)
		}
	}
	code, err := db.GetCode(f.codeHash)
	if err != nil || string(code) != string(f.code) {
		t.Fatalf("code = %x, %v", code, err)
	}

	// Absent keys are known zeros once the baseline is complete.
	if _, exists, err := db.GetAccount(schema.Address{0x99}, testS); err != nil || exists {
		t.Fatalf("absent account: exists=%v err=%v", exists, err)
	}
	if v, err := db.GetSlot(f.contract, schema.Hash{0x77}, testS); err != nil || v != (schema.Hash{}) {
		t.Fatalf("absent slot = %x, %v", v, err)
	}

	// Idempotent rerun over already-written rows.
	if err := load(db, f.chaindb, testS, slog.Default()); err != nil {
		t.Fatalf("rerun: %v", err)
	}
}

// TestResume: a progress cursor mid-phase-2 skips phase 1 entirely and all
// phase-2 rows before the cursor.
func TestResume(t *testing.T) {
	f := makeFixture(t)
	db := openStore(t)
	bl, err := db.NewBaseline(testS)
	if err != nil {
		t.Fatal(err)
	}
	conHash := store.Keccak(f.contract[:])
	slotBHash := store.Keccak(f.slotB[:])
	prog := append([]byte{2}, rawdb.SnapshotStoragePrefix...)
	prog = append(prog, conHash[:]...)
	prog = append(prog, slotBHash[:]...)
	if err := loadFrom(bl, f.chaindb, prog, slog.Default()); err != nil {
		t.Fatal(err)
	}

	// Phase 1 skipped: accounts absent.
	if _, exists, err := db.GetAccount(f.eoa, testS); err != nil || exists {
		t.Fatalf("eoa after resume: exists=%v err=%v", exists, err)
	}
	// Slot A (before the cursor) skipped, slot B (at the cursor) loaded.
	if v, err := db.GetSlot(f.contract, f.slotA, testS); err != nil || v != (schema.Hash{}) {
		t.Fatalf("slotA after resume = %x, %v", v, err)
	}
	if v, err := db.GetSlot(f.contract, f.slotB, testS); err != nil || v != f.valB {
		t.Fatalf("slotB after resume = %x, %v; want %x", v, err, f.valB)
	}
	// Phase 3 ran.
	if code, err := db.GetCode(f.codeHash); err != nil || string(code) != string(f.code) {
		t.Fatalf("code after resume = %x, %v", code, err)
	}
}
