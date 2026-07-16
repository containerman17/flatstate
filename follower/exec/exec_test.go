package exec

import (
	"math/big"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/trie"
	"github.com/holiman/uint256"

	"github.com/containerman17/flatstate/capture"
	"github.com/containerman17/flatstate/schema"
	"github.com/containerman17/flatstate/store"
)

// Contract runtime code: SSTORE(0, SLOAD(0) + 1 + SLOAD(7)).
// Slot 0 comes from the hash-keyed baseline, slot 7 is a known zero.
var counterCode = []byte{
	0x60, 0x00, 0x54, // PUSH1 0 SLOAD
	0x60, 0x01, 0x01, // PUSH1 1 ADD
	0x60, 0x07, 0x54, // PUSH1 7 SLOAD
	0x01,             // ADD
	0x60, 0x00, 0x55, // PUSH1 0 SSTORE
	0x00, // STOP
}

var (
	senderKey, _ = crypto.ToECDSA(common.LeftPadBytes([]byte{1}, 32))
	senderAddr   = crypto.PubkeyToAddress(senderKey.PublicKey)
	dstAddr      = common.HexToAddress("0x00000000000000000000000000000000000000dd")
	contractAddr = common.HexToAddress("0x00000000000000000000000000000000000000cc")
)

const (
	baseHeight = 100
	gasLimit   = 15_000_000
	// A recent mainnet-like timestamp so current upgrades are active.
	baseTime = uint64(1_760_000_000)
)

func slotHash(b byte) schema.Hash { return schema.Hash{31: b} }

// buildStore seeds a hash-keyed baseline at S=100: funded sender, the
// counter contract with slot0=41, and the contract code.
func buildStore(t *testing.T) *store.DB {
	t.Helper()
	db, err := store.Open(t.TempDir(), 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	bl, err := db.NewBaseline(baseHeight)
	if err != nil {
		t.Fatal(err)
	}

	codeHash := schema.Hash(crypto.Keccak256Hash(counterCode))
	sender := schema.Account{Balance: *uint256.MustFromDecimal("10000000000000000000"), Nonce: 0, CodeHash: schema.Hash(types.EmptyCodeHash)}
	if err := db.PutBaseAccount(store.Keccak(senderAddr[:]), &sender); err != nil {
		t.Fatal(err)
	}
	contract := schema.Account{Nonce: 1, CodeHash: codeHash}
	if err := db.PutBaseAccount(store.Keccak(contractAddr[:]), &contract); err != nil {
		t.Fatal(err)
	}
	s0 := slotHash(0)
	if err := db.PutBaseSlot(store.Keccak(contractAddr[:]), store.Keccak(s0[:]), slotHash(41)); err != nil {
		t.Fatal(err)
	}
	if err := bl.Code(codeHash, counterCode); err != nil {
		t.Fatal(err)
	}
	if err := bl.Finish(); err != nil {
		t.Fatal(err)
	}
	return db
}

func parentHeader() *types.Header {
	return &types.Header{
		Number:     big.NewInt(baseHeight),
		GasLimit:   gasLimit,
		BaseFee:    big.NewInt(25_000_000_000),
		Difficulty: big.NewInt(1),
		Time:       baseTime,
	}
}

// makeBlock builds a child block; run once with a draft header to learn the
// computed receipts values, then again with the final header so Execute's
// validation passes (the point under test is the capture, not header
// forgery).
func makeBlock(t *testing.T, e *Exec, parent *types.Header, txs []*types.Transaction) *types.Block {
	t.Helper()
	header := &types.Header{
		ParentHash: parent.Hash(),
		Number:     new(big.Int).Add(parent.Number, big.NewInt(1)),
		GasLimit:   gasLimit,
		BaseFee:    big.NewInt(25_000_000_000),
		Difficulty: big.NewInt(1),
		Time:       parent.Time + 2,
	}
	draft := types.NewBlock(header, txs, nil, nil, trie.NewStackTrie(nil))
	e.mu.Lock()
	_, usedGas, receipts, err := e.run(schema.Hash(parent.Hash()), draft)
	e.mu.Unlock()
	if err != nil {
		t.Fatalf("draft run: %v", err)
	}
	header.GasUsed = usedGas
	header.ReceiptHash = types.DeriveSha(receipts, trie.NewStackTrie(nil))
	header.Bloom = types.CreateBloom(receipts)
	return types.NewBlock(header, txs, nil, receipts, trie.NewStackTrie(nil))
}

func signedTx(t *testing.T, e *Exec, nonce uint64, to common.Address, value *big.Int, gas uint64) *types.Transaction {
	t.Helper()
	tx := types.NewTx(&types.LegacyTx{
		Nonce:    nonce,
		To:       &to,
		Value:    value,
		Gas:      gas,
		GasPrice: big.NewInt(25_000_000_000),
	})
	signed, err := types.SignTx(tx, types.LatestSigner(e.chainCfg), senderKey)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func findSlotOp(ops []capture.Op, addr common.Address, slot schema.Hash) (schema.Hash, bool) {
	for _, op := range ops {
		if op.Kind == capture.OpSlot && op.Addr == schema.Address(addr) && op.Slot == slot {
			return op.Value, true
		}
	}
	return schema.Hash{}, false
}

func findAccountOp(ops []capture.Op, addr common.Address) (schema.Account, bool) {
	for _, op := range ops {
		if op.Kind == capture.OpAccount && op.Addr == schema.Address(addr) {
			return op.Account, true
		}
	}
	return schema.Account{}, false
}

// TestExecuteCaptureAndReadOrder executes two blocks: block 101 reads the
// hash-keyed baseline (slot0=41, sender account) and a known zero (slot7),
// writes post-images; block 102 reads block 101's value through the pending
// layer. It then verifies the D7-ordered store flow and pruning.
func TestExecuteCaptureAndReadOrder(t *testing.T) {
	db := buildStore(t)
	e, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	parent := parentHeader()
	e.SeedHeaders([]*types.Header{parent})

	oneEther := big.NewInt(1_000_000_000_000_000_000)
	blk1 := makeBlock(t, e, parent, []*types.Transaction{
		signedTx(t, e, 0, dstAddr, oneEther, 21_000),
		signedTx(t, e, 1, contractAddr, nil, 100_000),
	})
	batch1, err := e.Execute(schema.Hash(parent.Hash()), blk1)
	if err != nil {
		t.Fatal(err)
	}
	if batch1.Block != baseHeight+1 || batch1.Hash != schema.Hash(blk1.Hash()) || batch1.Parent != schema.Hash(parent.Hash()) {
		t.Fatalf("batch identity: %+v", batch1)
	}
	// Baseline slot 41 + 1 + zero-slot 0 = 42.
	if v, ok := findSlotOp(batch1.Ops, contractAddr, slotHash(0)); !ok || v != slotHash(42) {
		t.Fatalf("counter slot op: ok=%v v=%x", ok, v)
	}
	if a, ok := findAccountOp(batch1.Ops, dstAddr); !ok || a.Balance.CmpBig(oneEther) != 0 {
		t.Fatalf("dst account op: ok=%v %+v", ok, a)
	}
	if a, ok := findAccountOp(batch1.Ops, senderAddr); !ok || a.Nonce != 2 {
		t.Fatalf("sender account op: ok=%v %+v", ok, a)
	}

	// Block 102 on top of the unfinalized 101: reads 42 from the pending
	// layer (not the store), stores 43.
	blk2 := makeBlock(t, e, blk1.Header(), []*types.Transaction{
		signedTx(t, e, 2, contractAddr, nil, 100_000),
	})
	batch2, err := e.Execute(schema.Hash(blk1.Hash()), blk2)
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := findSlotOp(batch2.Ops, contractAddr, slotHash(0)); !ok || v != slotHash(43) {
		t.Fatalf("second counter op: ok=%v v=%x", ok, v)
	}

	// D7 flow: commit 101 to the store, prune the layer, and confirm 102
	// still executes identically against store+remaining layer.
	if err := db.WriteBlock(batch1); err != nil {
		t.Fatal(err)
	}
	if err := db.SetFinalized(batch1.Block); err != nil {
		t.Fatal(err)
	}
	e.OnFinalized(batch1.Block)
	if len(e.pending) != 1 {
		t.Fatalf("pending after finalize: %d", len(e.pending))
	}
	if v, err := db.GetSlot(schema.Address(contractAddr), slotHash(0), store.Latest); err != nil || v != slotHash(42) {
		t.Fatalf("store after finalize: %v %x", err, v)
	}

	// Re-execution of 102 (e.g. after a preference reset) reads 42 from the
	// STORE now and the pending 102 layer is unchanged.
	e.mu.Lock()
	ops, _, _, err := e.run(schema.Hash(blk1.Hash()), blk2)
	e.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := findSlotOp(ops, contractAddr, slotHash(0)); !ok || v != slotHash(43) {
		t.Fatalf("re-exec counter op: ok=%v v=%x", ok, v)
	}
}

// TestValidationFailsLoud corrupts the header's gasUsed and receiptsRoot and
// expects Execute to refuse (D2 rev 2 validation).
func TestValidationFailsLoud(t *testing.T) {
	db := buildStore(t)
	e, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	parent := parentHeader()
	e.SeedHeaders([]*types.Header{parent})
	blk := makeBlock(t, e, parent, []*types.Transaction{
		signedTx(t, e, 0, dstAddr, big.NewInt(1), 21_000),
	})

	bad := blk.Header()
	bad.GasUsed++
	badBlk := types.NewBlockWithHeader(bad).WithBody(types.Body{Transactions: blk.Transactions()})
	if _, err := e.Execute(schema.Hash(parent.Hash()), badBlk); err == nil {
		t.Fatal("gasUsed mismatch must fail")
	}

	bad = blk.Header()
	bad.ReceiptHash = common.Hash{0xde, 0xad}
	badBlk = types.NewBlockWithHeader(bad).WithBody(types.Body{Transactions: blk.Transactions()})
	if _, err := e.Execute(schema.Hash(parent.Hash()), badBlk); err == nil {
		t.Fatal("receiptsRoot mismatch must fail")
	}
}

// TestUncoveredKeyFailsLoud: executing a tx from an account the baseline
// does not cover must error, not guess (D13).
func TestUncoveredKeyFailsLoud(t *testing.T) {
	db, err := store.Open(t.TempDir(), 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Genesis set but baseline NOT complete.
	if _, err := db.NewBaseline(baseHeight); err != nil {
		t.Fatal(err)
	}
	e, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	parent := parentHeader()
	e.SeedHeaders([]*types.Header{parent})
	tx := signedTx(t, e, 0, dstAddr, big.NewInt(1), 21_000)
	header := &types.Header{
		ParentHash: parent.Hash(),
		Number:     big.NewInt(baseHeight + 1),
		GasLimit:   gasLimit,
		BaseFee:    big.NewInt(25_000_000_000),
		Difficulty: big.NewInt(1),
		Time:       baseTime + 2,
	}
	blk := types.NewBlock(header, []*types.Transaction{tx}, nil, nil, trie.NewStackTrie(nil))
	if _, err := e.Execute(schema.Hash(parent.Hash()), blk); err == nil {
		t.Fatal("uncovered read must fail loud")
	}
}
