package exec

import (
	"errors"
	"fmt"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	"github.com/ava-labs/libevm/core/state"
	ethtypes "github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/ethdb"
	"github.com/ava-labs/libevm/trie"
	"github.com/ava-labs/libevm/trie/trienode"
	"github.com/ava-labs/libevm/triedb"
	"github.com/holiman/uint256"

	"github.com/containerman17/flatstate/capture"
	"github.com/containerman17/flatstate/schema"
	"github.com/containerman17/flatstate/store"
)

// view resolves reads through the pending unfinalized layers of the block's
// parent chain (newest first), then LMDB at Latest (finalized only, D7).
// D6 rev 2 read order (history rows, hash-keyed baseline probe, known-zero)
// lives inside store.GetAccount/GetSlot.
type view struct {
	db     *store.DB
	layers []*layer // newest first; a dry-run fold base, if any, is last
}

// layer is one executed-but-unfinalized block's diff, compiled for lookup
// (same semantics as mem's unfinalized layers).
type layer struct {
	block      uint64
	parent     schema.Hash
	accounts   map[schema.Address]schema.Account
	slots      map[schema.SKey]schema.Hash
	destructed map[schema.Address]bool
	codes      map[schema.Hash][]byte
}

func newLayer(b *capture.Batch) *layer {
	l := &layer{
		block:      b.Block,
		parent:     b.Parent,
		accounts:   make(map[schema.Address]schema.Account),
		slots:      make(map[schema.SKey]schema.Hash),
		destructed: make(map[schema.Address]bool),
		codes:      make(map[schema.Hash][]byte),
	}
	l.apply(b.Ops)
	return l
}

// apply folds ops in batch order. Reads check accounts/slots before
// destructed, so a destruct-then-recreate (destruct clears, later ops
// repopulate) keeps working when layers are folded cumulatively.
func (l *layer) apply(ops []capture.Op) {
	for i := range ops {
		op := &ops[i]
		switch op.Kind {
		case capture.OpAccount:
			l.accounts[op.Addr] = op.Account
		case capture.OpSlot:
			l.slots[schema.SKey{Addr: op.Addr, Slot: op.Slot}] = op.Value
		case capture.OpDeleteSlot:
			l.slots[schema.SKey{Addr: op.Addr, Slot: op.Slot}] = schema.Hash{}
		case capture.OpDestruct:
			l.destructed[op.Addr] = true
			delete(l.accounts, op.Addr)
			for k := range l.slots {
				if k.Addr == op.Addr {
					delete(l.slots, k)
				}
			}
		case capture.OpCode:
			l.codes[op.CodeHash] = op.Code
		}
	}
}

func (v *view) account(addr schema.Address) (schema.Account, bool, error) {
	for _, l := range v.layers {
		if a, ok := l.accounts[addr]; ok {
			return a, true, nil
		}
		if l.destructed[addr] {
			return schema.Account{}, false, nil
		}
	}
	return v.db.GetAccount(addr, store.Latest)
}

func (v *view) slot(addr schema.Address, slot schema.Hash) (schema.Hash, error) {
	sk := schema.SKey{Addr: addr, Slot: slot}
	for _, l := range v.layers {
		if val, ok := l.slots[sk]; ok {
			return val, nil
		}
		if l.destructed[addr] {
			return schema.Hash{}, nil
		}
	}
	return v.db.GetSlot(addr, slot, store.Latest)
}

func (v *view) code(hash schema.Hash) ([]byte, error) {
	for _, l := range v.layers {
		if c, ok := l.codes[hash]; ok {
			return c, nil
		}
	}
	return v.db.GetCode(hash)
}

// captureDB implements libevm's state.Database against a view, capturing
// every commit-time mutation as capture ops. One instance per executed
// block. The "tries" it opens are not tries: reads resolve through the
// view, writes append post-image ops, and all roots are EmptyRootHash so
// libevm's Commit never touches a real trie database (root == origin skips
// TrieDB().Update, and returning Root=EmptyRootHash from GetAccount makes
// handleDestruction skip storage enumeration; see package comment for why
// that is sound under EIP-6780).
type captureDB struct {
	v    *view
	ops  []capture.Op
	disk ethdb.Database
	tdb  *triedb.Database
}

func newCaptureDB(v *view) *captureDB {
	disk := rawdb.NewMemoryDatabase() // sink for libevm's code writes; ours come from UpdateContractCode
	return &captureDB{
		v:    v,
		disk: disk,
		tdb:  triedb.NewDatabase(disk, nil),
	}
}

var _ state.Database = (*captureDB)(nil)

func (d *captureDB) OpenTrie(common.Hash) (state.Trie, error) {
	return &accountTrie{d: d}, nil
}

func (d *captureDB) OpenStorageTrie(_ common.Hash, address common.Address, _ common.Hash, _ state.Trie) (state.Trie, error) {
	return &storageTrie{d: d, addr: schema.Address(address)}, nil
}

func (d *captureDB) CopyTrie(t state.Trie) state.Trie { return t }

func (d *captureDB) ContractCode(_ common.Address, codeHash common.Hash) ([]byte, error) {
	return d.v.code(schema.Hash(codeHash))
}

func (d *captureDB) ContractCodeSize(addr common.Address, codeHash common.Hash) (int, error) {
	c, err := d.ContractCode(addr, codeHash)
	return len(c), err
}

func (d *captureDB) DiskDB() ethdb.KeyValueStore { return d.disk }
func (d *captureDB) TrieDB() *triedb.Database    { return d.tdb }

// baseTrie provides the shared no-op trie plumbing.
type baseTrie struct{}

func (baseTrie) GetKey([]byte) []byte { return nil }
func (baseTrie) Hash() common.Hash    { return ethtypes.EmptyRootHash }
func (baseTrie) Commit(bool) (common.Hash, *trienode.NodeSet, error) {
	return ethtypes.EmptyRootHash, nil, nil
}
func (baseTrie) NodeIterator([]byte) (trie.NodeIterator, error) {
	return nil, errors.New("exec: trie iteration unsupported (D13)")
}
func (baseTrie) Prove([]byte, ethdb.KeyValueWriter) error {
	return errors.New("exec: merkle proofs unsupported (D13)")
}

// accountTrie is the account "trie": preimage-keyed reads through the view,
// commit-time writes captured as ops.
type accountTrie struct {
	baseTrie
	d *captureDB
}

var _ state.Trie = (*accountTrie)(nil)

func (t *accountTrie) GetAccount(address common.Address) (*ethtypes.StateAccount, error) {
	a, exists, err := t.d.v.account(schema.Address(address))
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	balance := new(uint256.Int).Set(&a.Balance)
	codeHash := a.CodeHash
	if codeHash == (schema.Hash{}) {
		codeHash = schema.Hash(ethtypes.EmptyCodeHash)
	}
	return &ethtypes.StateAccount{
		Nonce:   a.Nonce,
		Balance: balance,
		// Root is always empty: we keep no storage tries. This also makes
		// libevm skip destructed-storage enumeration, which is exactly the
		// EIP-6780 assumption documented in the package comment.
		Root:     ethtypes.EmptyRootHash,
		CodeHash: codeHash[:],
	}, nil
}

func (t *accountTrie) UpdateAccount(address common.Address, account *ethtypes.StateAccount) error {
	op := capture.Op{Kind: capture.OpAccount, Addr: schema.Address(address)}
	if account.Balance != nil {
		op.Account.Balance = *account.Balance
	}
	op.Account.Nonce = account.Nonce
	if len(account.CodeHash) == 32 {
		op.Account.CodeHash = schema.Hash(common.BytesToHash(account.CodeHash))
	} else {
		op.Account.CodeHash = schema.Hash(ethtypes.EmptyCodeHash)
	}
	t.d.ops = append(t.d.ops, op)
	return nil
}

func (t *accountTrie) DeleteAccount(address common.Address) error {
	t.d.ops = append(t.d.ops, capture.Op{Kind: capture.OpDestruct, Addr: schema.Address(address)})
	return nil
}

func (t *accountTrie) UpdateContractCode(_ common.Address, codeHash common.Hash, code []byte) error {
	t.d.ops = append(t.d.ops, capture.Op{Kind: capture.OpCode, CodeHash: schema.Hash(codeHash), Code: code})
	return nil
}

func (t *accountTrie) GetStorage(common.Address, []byte) ([]byte, error) {
	return nil, errors.New("exec: storage read on account trie")
}
func (t *accountTrie) UpdateStorage(common.Address, []byte, []byte) error {
	return errors.New("exec: storage write on account trie")
}
func (t *accountTrie) DeleteStorage(common.Address, []byte) error {
	return errors.New("exec: storage delete on account trie")
}

// storageTrie is one account's storage "trie".
type storageTrie struct {
	baseTrie
	d    *captureDB
	addr schema.Address
}

var _ state.Trie = (*storageTrie)(nil)

func (t *storageTrie) GetStorage(_ common.Address, key []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("exec: storage key has %d bytes", len(key))
	}
	v, err := t.d.v.slot(t.addr, schema.Hash(common.BytesToHash(key)))
	if err != nil {
		return nil, err
	}
	if v == (schema.Hash{}) {
		return nil, nil
	}
	return v[:], nil
}

func (t *storageTrie) UpdateStorage(_ common.Address, key, value []byte) error {
	if len(key) != 32 {
		return fmt.Errorf("exec: storage key has %d bytes", len(key))
	}
	op := capture.Op{Kind: capture.OpSlot, Addr: t.addr, Slot: schema.Hash(common.BytesToHash(key))}
	op.Value = schema.Hash(common.BytesToHash(value))
	t.d.ops = append(t.d.ops, op)
	return nil
}

func (t *storageTrie) DeleteStorage(_ common.Address, key []byte) error {
	if len(key) != 32 {
		return fmt.Errorf("exec: storage key has %d bytes", len(key))
	}
	t.d.ops = append(t.d.ops, capture.Op{Kind: capture.OpDeleteSlot, Addr: t.addr, Slot: schema.Hash(common.BytesToHash(key))})
	return nil
}

func (t *storageTrie) GetAccount(common.Address) (*ethtypes.StateAccount, error) {
	return nil, errors.New("exec: account read on storage trie")
}
func (t *storageTrie) UpdateAccount(common.Address, *ethtypes.StateAccount) error {
	return errors.New("exec: account write on storage trie")
}
func (t *storageTrie) DeleteAccount(common.Address) error {
	return errors.New("exec: account delete on storage trie")
}
func (t *storageTrie) UpdateContractCode(common.Address, common.Hash, []byte) error {
	return errors.New("exec: code write on storage trie")
}
