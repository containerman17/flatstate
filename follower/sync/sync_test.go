package sync

import (
	"bytes"
	"context"
	"encoding/binary"
	"sort"
	gosync "sync"
	"testing"

	"github.com/ava-labs/avalanchego/graft/evm/message"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
	"github.com/holiman/uint256"

	"github.com/containerman17/flatstate/follower/net"
	"github.com/containerman17/flatstate/schema"
	"github.com/containerman17/flatstate/store"
)

const testS = uint64(32768)

var testRoot = common.Hash{0xaa}

type kv struct{ k, v []byte }

// fakeClient serves in-memory tries with the same range semantics as the
// real client (Start/End inclusive, More = leaves right of the last returned
// key in the whole trie). maxLeafs forces pagination.
type fakeClient struct {
	mu       gosync.Mutex
	accounts []kv
	storage  map[common.Hash][]kv
	codes    map[common.Hash][]byte
	maxLeafs int

	mainStarts map[byte][][]byte // segment -> start keys of main-trie requests
	codeReqs   int
}

func (f *fakeClient) GetLeafs(_ context.Context, req message.LeafsRequest) (message.LeafsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	list := f.accounts
	if req.AccountHash() != (common.Hash{}) {
		list = f.storage[req.AccountHash()]
	} else {
		if f.mainStarts == nil {
			f.mainStarts = make(map[byte][][]byte)
		}
		seg := req.StartKey()[0]
		f.mainStarts[seg] = append(f.mainStarts[seg], bytes.Clone(req.StartKey()))
	}
	limit := int(req.KeyLimit())
	if f.maxLeafs > 0 && f.maxLeafs < limit {
		limit = f.maxLeafs
	}
	var resp message.LeafsResponse
	lastIdx := -1
	for i, e := range list {
		if len(req.StartKey()) > 0 && bytes.Compare(e.k, req.StartKey()) < 0 {
			continue
		}
		if len(req.EndKey()) > 0 && bytes.Compare(e.k, req.EndKey()) > 0 {
			break
		}
		if len(resp.Keys) >= limit {
			break
		}
		resp.Keys = append(resp.Keys, e.k)
		resp.Vals = append(resp.Vals, e.v)
		lastIdx = i
	}
	resp.More = lastIdx >= 0 && lastIdx+1 < len(list) // trie-wide, like the proof check
	return resp, nil
}

func (f *fakeClient) GetCode(_ context.Context, hashes []common.Hash) ([][]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.codeReqs++
	out := make([][]byte, len(hashes))
	for i, h := range hashes {
		c, ok := f.codes[h]
		if !ok {
			return nil, context.DeadlineExceeded
		}
		out[i] = c
	}
	return out, nil
}

type fixture struct {
	client *fakeClient

	eoa      schema.Address
	contract schema.Address
	code     []byte
	codeHash schema.Hash
	slots    map[schema.Hash]schema.Hash // preimage slot -> value
}

func encodeAccount(t *testing.T, acc types.StateAccount) []byte {
	t.Helper()
	b, err := rlp.EncodeToBytes(&acc)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func trimmedRLP(t *testing.T, v schema.Hash) []byte {
	t.Helper()
	trimmed := v[:]
	for len(trimmed) > 0 && trimmed[0] == 0 {
		trimmed = trimmed[1:]
	}
	b, err := rlp.EncodeToBytes(trimmed)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func makeFixture(t *testing.T) *fixture {
	t.Helper()
	net.RegisterExtras()
	f := &fixture{
		eoa:      schema.Address{0x11},
		contract: schema.Address{0x22},
		code:     []byte{0x60, 0x00, 0x60, 0x00, 0xfd},
		slots:    make(map[schema.Hash]schema.Hash),
	}
	f.codeHash = store.Keccak(f.code)
	conHash := common.Hash(store.Keccak(f.contract[:]))

	// Five slots so maxLeafs=2 forces storage pagination.
	var storage []kv
	for i := byte(1); i <= 5; i++ {
		slot := schema.Hash{31: i}
		val := schema.Hash{30: i, 31: 0xcd}
		f.slots[slot] = val
		h := store.Keccak(slot[:])
		storage = append(storage, kv{k: bytes.Clone(h[:]), v: trimmedRLP(t, val)})
	}
	sort.Slice(storage, func(i, j int) bool { return bytes.Compare(storage[i].k, storage[j].k) < 0 })

	accounts := []kv{
		{
			k: hashOf(f.eoa[:]),
			v: encodeAccount(t, types.StateAccount{
				Nonce: 7, Balance: uint256.NewInt(5),
				Root: types.EmptyRootHash, CodeHash: types.EmptyCodeHash[:],
			}),
		},
		{
			k: hashOf(f.contract[:]),
			v: encodeAccount(t, types.StateAccount{
				Nonce: 1, Balance: uint256.NewInt(0),
				Root: common.Hash{0xbb}, CodeHash: f.codeHash[:],
			}),
		},
	}
	sort.Slice(accounts, func(i, j int) bool { return bytes.Compare(accounts[i].k, accounts[j].k) < 0 })

	f.client = &fakeClient{
		accounts: accounts,
		storage:  map[common.Hash][]kv{conHash: storage},
		codes:    map[common.Hash][]byte{common.Hash(f.codeHash): f.code},
		maxLeafs: 2,
	}
	return f
}

func hashOf(b []byte) []byte {
	h := store.Keccak(b)
	return bytes.Clone(h[:])
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

func verifyLoaded(t *testing.T, db *store.DB, f *fixture) {
	t.Helper()
	done, err := db.BaselineComplete()
	if err != nil || !done {
		t.Fatalf("baseline complete = %v, %v", done, err)
	}
	a, exists, err := db.GetAccount(f.eoa, testS)
	if err != nil || !exists || a.Nonce != 7 || a.Balance.Uint64() != 5 {
		t.Fatalf("eoa = %+v exists=%v err=%v", a, exists, err)
	}
	if a.CodeHash != schema.Hash(types.EmptyCodeHash) {
		t.Fatalf("eoa codehash = %x", a.CodeHash)
	}
	c, exists, err := db.GetAccount(f.contract, testS)
	if err != nil || !exists || c.CodeHash != f.codeHash {
		t.Fatalf("contract = %+v exists=%v err=%v", c, exists, err)
	}
	for slot, want := range f.slots {
		got, err := db.GetSlot(f.contract, slot, testS)
		if err != nil || got != want {
			t.Fatalf("slot %x = %x, %v; want %x", slot, got, err, want)
		}
	}
	code, err := db.GetCode(f.codeHash)
	if err != nil || !bytes.Equal(code, f.code) {
		t.Fatalf("code = %x, %v", code, err)
	}
	// Absent keys are known zeros once complete.
	if _, exists, err := db.GetAccount(schema.Address{0x99}, testS); err != nil || exists {
		t.Fatalf("absent account exists=%v err=%v", exists, err)
	}
	if v, err := db.GetSlot(f.contract, schema.Hash{0x77}, testS); err != nil || v != (schema.Hash{}) {
		t.Fatalf("absent slot = %x, %v", v, err)
	}
}

func TestRun(t *testing.T) {
	f := makeFixture(t)
	db := openStore(t)
	if err := Run(context.Background(), Config{Client: f.client, DB: db, Height: testS, Root: testRoot, Workers: 4}); err != nil {
		t.Fatal(err)
	}
	verifyLoaded(t, db, f)
	// The fake serves 2 leaves per response: the contract's 5 slots need 3
	// storage requests, proving the pagination loop advances.
	if len(f.client.storage) != 1 {
		t.Fatal("fixture broken")
	}
}

// TestResume: done segments are skipped entirely, an undone segment resumes
// past its committed watermark, and the code sweep fills a code row missing
// for an already-committed account.
func TestResume(t *testing.T) {
	f := makeFixture(t)
	db := openStore(t)

	// A second account in the contract's segment, hash-below the contract,
	// plays the committed watermark.
	conHash := store.Keccak(f.contract[:])
	seg := conHash[0]
	var low schema.Hash
	copy(low[:], conHash[:])
	low[31] = 0 // strictly below unless conHash ends in 0; then use 1
	if low == conHash {
		low[31] = 1
	}
	if bytes.Compare(low[:], conHash[:]) > 0 {
		t.Fatal("fixture: low watermark not below contract hash")
	}

	bl, err := db.NewBaseline(testS)
	if err != nil {
		t.Fatal(err)
	}
	// Committed account with the shared code hash but NO code row: the sweep
	// must fetch it even though the live claim map is empty on this run.
	if err := bl.BaseAccount(low, &schema.Account{Nonce: 9, CodeHash: f.codeHash}); err != nil {
		t.Fatal(err)
	}
	// All segments done except the contract's.
	var bitmap [32]byte
	for i := range bitmap {
		bitmap[i] = 0xff
	}
	bitmap[seg/8] &^= 1 << (seg % 8)
	bl.SetProgress(bitmap[:])
	if err := bl.Flush(); err != nil {
		t.Fatal(err)
	}

	if err := Run(context.Background(), Config{Client: f.client, DB: db, Height: testS, Root: testRoot, Workers: 4}); err != nil {
		t.Fatal(err)
	}

	// Only the contract's segment was fetched, starting past the watermark.
	if len(f.client.mainStarts) != 1 {
		t.Fatalf("segments fetched: %v", f.client.mainStarts)
	}
	starts := f.client.mainStarts[seg]
	if len(starts) == 0 || bytes.Compare(starts[0], low[:]) <= 0 {
		t.Fatalf("segment %02x start %x not past watermark %x", seg, starts[0], low)
	}
	// The contract (above the watermark) was loaded with storage and code.
	c, exists, err := db.GetAccount(f.contract, testS)
	if err != nil || !exists || c.CodeHash != f.codeHash {
		t.Fatalf("contract after resume = %+v exists=%v err=%v", c, exists, err)
	}
	code, err := db.GetCode(f.codeHash)
	if err != nil || !bytes.Equal(code, f.code) {
		t.Fatalf("code after sweep = %x, %v", code, err)
	}
	if f.client.codeReqs == 0 {
		t.Fatal("code sweep sent no requests")
	}
	// EOA's segment was marked done, so it reads as absent; that is the
	// bitmap's contract, not a loader bug.
	if _, exists, _ := db.GetAccount(f.eoa, testS); exists {
		t.Fatal("eoa should be absent (its segment was pre-marked done)")
	}
}

// TestIncKey covers the boundary arithmetic the range walk depends on.
func TestIncKey(t *testing.T) {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, 41)
	if got := binary.BigEndian.Uint64(incKey(k)); got != 42 {
		t.Fatalf("incKey = %d", got)
	}
	if incKey([]byte{0xff, 0xff}) != nil {
		t.Fatal("overflow must return nil")
	}
	carry := incKey([]byte{0x01, 0xff})
	if !bytes.Equal(carry, []byte{0x02, 0x00}) {
		t.Fatalf("carry = %x", carry)
	}
}
