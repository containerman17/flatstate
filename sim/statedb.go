package sim

import (
	"github.com/ava-labs/libevm/common"
	ethtypes "github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/core/vm"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/libevm/stateconf"
	"github.com/ava-labs/libevm/params"
	"github.com/holiman/uint256"

	"github.com/containerman17/flatstate/engine"
	"github.com/containerman17/flatstate/schema"
)

type skey struct {
	addr common.Address
	slot common.Hash
}

// normSlot clears bit 0 of the first key byte, exactly like coreth's
// extstate state-key normalization (multicoin support). The whole flatstate
// store is keyed by NORMALIZED slot preimages: the follower captures through
// the extstate-wrapped statedb and the state-sync baseline is hashed from
// normalized keys, so every persistent storage read or write in sim must
// normalize first. Found live: UniV3 factory.getPool read zero because its
// mapping slot has bit 0 set.
func normSlot(slot common.Hash) common.Hash {
	slot[0] &^= 0x01
	return slot
}

// stateDB is the per-executor vm.StateDB (DESIGN.md D14): a thin journaled
// overlay over the engine View. Storage and balance are the hot overrides;
// nonce/code/selfdestruct maps exist only for CREATE-performing simulations
// and normally stay empty. Reset with clear() between calls, never
// reallocated (fresh-per-call was a measured 2.4x regression).
//
// View read errors (LMDB fail-loud, D13) cannot surface through the
// vm.StateDB interface; the first one is recorded in readErr, the EVM is
// cancelled, and the executor turns it into Result.Err.
type stateDB struct {
	view *engine.View
	call *Call
	evm  *vm.EVM

	balances map[common.Address]*uint256.Int
	slots    map[skey]common.Hash // normalized keys
	ovSlots  map[skey]common.Hash // caller storage overrides, normalized keys

	// cold paths, normally empty
	nonces     map[common.Address]uint64
	codes      map[common.Address][]byte
	codeHashes map[common.Address]common.Hash
	created    map[common.Address]struct{}
	destructed map[common.Address]struct{}
	transient  map[skey]common.Hash

	// access list: nil slot map = address only
	al map[common.Address]map[common.Hash]struct{}

	refund  uint64
	logs    []*ethtypes.Log
	journal []jentry
	readErr error
}

func newStateDB() *stateDB {
	return &stateDB{
		balances:   make(map[common.Address]*uint256.Int),
		slots:      make(map[skey]common.Hash),
		ovSlots:    make(map[skey]common.Hash),
		nonces:     make(map[common.Address]uint64),
		codes:      make(map[common.Address][]byte),
		codeHashes: make(map[common.Address]common.Hash),
		created:    make(map[common.Address]struct{}),
		destructed: make(map[common.Address]struct{}),
		transient:  make(map[skey]common.Hash),
		al:         make(map[common.Address]map[common.Hash]struct{}),
	}
}

// reset rebinds the statedb to a call and clears all per-call state.
func (s *stateDB) reset(v *engine.View, c *Call, evm *vm.EVM) {
	s.view, s.call, s.evm = v, c, evm
	clear(s.balances)
	clear(s.slots)
	clear(s.ovSlots)
	clear(s.nonces)
	clear(s.codes)
	clear(s.codeHashes)
	clear(s.created)
	clear(s.destructed)
	clear(s.transient)
	clear(s.al)
	s.refund = 0
	s.logs = s.logs[:0]
	s.journal = s.journal[:0]
	s.readErr = nil
	// Seed the caller's overrides into the overlay (pre-snapshot, so a
	// revert never unwinds them).
	for addr, b := range c.BalanceOverrides {
		s.balances[addr] = new(uint256.Int).Set(b)
	}
	for addr, slots := range c.StorageOverrides {
		for slot, val := range slots {
			k := skey{addr, normSlot(slot)}
			s.slots[k] = val
			s.ovSlots[k] = val // committed-state view of the override
		}
	}
}

// fail records the first View read error and aborts the EVM run; results of
// this call are garbage and the executor reports the error instead (D13).
func (s *stateDB) fail(err error) {
	if s.readErr == nil {
		s.readErr = err
		if s.evm != nil {
			s.evm.InvalidateExecution(err)
			s.evm.Cancel() // note: Cancel is sticky; the executor rebuilds the EVM
		}
	}
}

// --- underlying reads (overlay miss -> View: layers -> base -> LMDB) ---

func (s *stateDB) account(addr common.Address) (schema.Account, bool) {
	a, exists, err := s.view.Account(schema.Address(addr))
	if err != nil {
		s.fail(err)
		return schema.Account{}, false
	}
	return a, exists
}

func (s *stateDB) balance(addr common.Address) *uint256.Int {
	if b, ok := s.balances[addr]; ok {
		return b
	}
	b, err := s.view.Balance(schema.Address(addr))
	if err != nil {
		s.fail(err)
		return new(uint256.Int)
	}
	return &b
}

// --- journal ---

type jkind uint8

const (
	jBalance jkind = iota
	jStorage
	jNonce
	jCode
	jRefund
	jLog
	jDestruct
	jCreated
	jTransient
	jALAddr
	jALSlot
)

type jentry struct {
	kind    jkind
	addr    common.Address
	slot    common.Hash
	prevVal common.Hash
	prevBal uint256.Int
	prevU64 uint64
	had     bool // overlay had an entry before this write
}

func (s *stateDB) Snapshot() int { return len(s.journal) }

func (s *stateDB) RevertToSnapshot(rev int) {
	for i := len(s.journal) - 1; i >= rev; i-- {
		e := &s.journal[i]
		switch e.kind {
		case jBalance:
			if e.had {
				s.balances[e.addr] = new(uint256.Int).Set(&e.prevBal)
			} else {
				delete(s.balances, e.addr)
			}
		case jStorage:
			k := skey{e.addr, e.slot}
			if e.had {
				s.slots[k] = e.prevVal
			} else {
				delete(s.slots, k)
			}
		case jNonce:
			if e.had {
				s.nonces[e.addr] = e.prevU64
			} else {
				delete(s.nonces, e.addr)
			}
		case jCode:
			delete(s.codes, e.addr)
			delete(s.codeHashes, e.addr)
		case jRefund:
			s.refund = e.prevU64
		case jLog:
			s.logs = s.logs[:len(s.logs)-1]
		case jDestruct:
			delete(s.destructed, e.addr)
		case jCreated:
			delete(s.created, e.addr)
		case jTransient:
			k := skey{e.addr, e.slot}
			if e.had {
				s.transient[k] = e.prevVal
			} else {
				delete(s.transient, k)
			}
		case jALAddr:
			delete(s.al, e.addr)
		case jALSlot:
			delete(s.al[e.addr], e.slot)
		}
	}
	s.journal = s.journal[:rev]
}

// --- balances ---

func (s *stateDB) GetBalance(addr common.Address) *uint256.Int {
	return s.balance(addr)
}

func (s *stateDB) setBalance(addr common.Address, b *uint256.Int) {
	prev, had := s.balances[addr]
	e := jentry{kind: jBalance, addr: addr, had: had}
	if had {
		e.prevBal = *prev
	}
	s.journal = append(s.journal, e)
	s.balances[addr] = b
}

func (s *stateDB) AddBalance(addr common.Address, amount *uint256.Int) {
	s.setBalance(addr, new(uint256.Int).Add(s.balance(addr), amount))
}

func (s *stateDB) SubBalance(addr common.Address, amount *uint256.Int) {
	s.setBalance(addr, new(uint256.Int).Sub(s.balance(addr), amount))
}

// --- nonces, code ---

func (s *stateDB) GetNonce(addr common.Address) uint64 {
	if n, ok := s.nonces[addr]; ok {
		return n
	}
	a, _ := s.account(addr)
	return a.Nonce
}

func (s *stateDB) SetNonce(addr common.Address, n uint64) {
	prev, had := s.nonces[addr]
	s.journal = append(s.journal, jentry{kind: jNonce, addr: addr, prevU64: prev, had: had})
	s.nonces[addr] = n
}

func (s *stateDB) GetCodeHash(addr common.Address) common.Hash {
	if h, ok := s.codeHashes[addr]; ok {
		return h
	}
	a, exists := s.account(addr)
	if !exists {
		return common.Hash{}
	}
	if a.CodeHash == (schema.Hash{}) {
		return ethtypes.EmptyCodeHash
	}
	return common.Hash(a.CodeHash)
}

func (s *stateDB) GetCode(addr common.Address) []byte {
	if c, ok := s.codes[addr]; ok {
		return c
	}
	h := s.GetCodeHash(addr)
	if h == (common.Hash{}) || h == ethtypes.EmptyCodeHash {
		return nil
	}
	c, err := s.view.Code(schema.Hash(h))
	if err != nil {
		s.fail(err)
		return nil
	}
	return c
}

func (s *stateDB) GetCodeSize(addr common.Address) int { return len(s.GetCode(addr)) }

// SetCode is the CREATE deposit path; keccak(code) is computed exactly once
// here (D14: EXTCODEHASH re-hashing was a measured 14% of CPU).
func (s *stateDB) SetCode(addr common.Address, code []byte) {
	s.journal = append(s.journal, jentry{kind: jCode, addr: addr})
	s.codes[addr] = code
	s.codeHashes[addr] = crypto.Keccak256Hash(code)
}

// --- storage ---

func (s *stateDB) GetState(addr common.Address, slot common.Hash, _ ...stateconf.StateDBStateOption) common.Hash {
	slot = normSlot(slot)
	if v, ok := s.slots[skey{addr, slot}]; ok {
		return v
	}
	v, err := s.view.Slot(schema.Address(addr), schema.Hash(slot))
	if err != nil {
		s.fail(err)
		return common.Hash{}
	}
	return common.Hash(v)
}

// GetCommittedState returns the pre-call value: a caller storage override if
// one was given, else the shared state (overlay writes made during the call
// are not visible, which is what SSTORE gas accounting needs).
func (s *stateDB) GetCommittedState(addr common.Address, slot common.Hash, _ ...stateconf.StateDBStateOption) common.Hash {
	slot = normSlot(slot)
	if v, ok := s.ovSlots[skey{addr, slot}]; ok {
		return v
	}
	v, err := s.view.Slot(schema.Address(addr), schema.Hash(slot))
	if err != nil {
		s.fail(err)
		return common.Hash{}
	}
	return common.Hash(v)
}

func (s *stateDB) SetState(addr common.Address, slot, val common.Hash, _ ...stateconf.StateDBStateOption) {
	k := skey{addr, normSlot(slot)}
	prev, had := s.slots[k]
	s.journal = append(s.journal, jentry{kind: jStorage, addr: addr, slot: k.slot, prevVal: prev, had: had})
	s.slots[k] = val
}

// --- transient storage (EIP-1153) ---

func (s *stateDB) GetTransientState(addr common.Address, slot common.Hash) common.Hash {
	return s.transient[skey{addr, slot}]
}

func (s *stateDB) SetTransientState(addr common.Address, slot, val common.Hash) {
	k := skey{addr, slot}
	prev, had := s.transient[k]
	s.journal = append(s.journal, jentry{kind: jTransient, addr: addr, slot: slot, prevVal: prev, had: had})
	s.transient[k] = val
}

// --- account lifecycle ---

// CreateAccount marks the address created this call. Balance carry-over is
// implicit (reads fall through to the overlay/underlying balance); prior
// storage is not wiped because at the tip a CREATE target can never have
// pre-existing storage (EIP-6780 world, same argument as follower/exec).
func (s *stateDB) CreateAccount(addr common.Address) {
	if _, ok := s.created[addr]; ok {
		return
	}
	s.journal = append(s.journal, jentry{kind: jCreated, addr: addr})
	s.created[addr] = struct{}{}
}

func (s *stateDB) SelfDestruct(addr common.Address) {
	if _, ok := s.destructed[addr]; !ok {
		s.journal = append(s.journal, jentry{kind: jDestruct, addr: addr})
		s.destructed[addr] = struct{}{}
	}
	s.setBalance(addr, new(uint256.Int))
}

func (s *stateDB) HasSelfDestructed(addr common.Address) bool {
	_, ok := s.destructed[addr]
	return ok
}

func (s *stateDB) Selfdestruct6780(addr common.Address) {
	if _, ok := s.created[addr]; ok {
		s.SelfDestruct(addr)
	}
}

func (s *stateDB) Exist(addr common.Address) bool {
	if _, ok := s.created[addr]; ok {
		return true
	}
	if _, ok := s.destructed[addr]; ok {
		return true
	}
	if _, ok := s.balances[addr]; ok {
		return true
	}
	if _, ok := s.nonces[addr]; ok {
		return true
	}
	_, exists := s.account(addr)
	return exists
}

func (s *stateDB) Empty(addr common.Address) bool {
	if s.GetNonce(addr) != 0 {
		return false
	}
	if !s.balance(addr).IsZero() {
		return false
	}
	h := s.GetCodeHash(addr)
	return h == (common.Hash{}) || h == ethtypes.EmptyCodeHash
}

// --- refunds, logs, preimages ---

func (s *stateDB) AddRefund(gas uint64) {
	s.journal = append(s.journal, jentry{kind: jRefund, prevU64: s.refund})
	s.refund += gas
}

func (s *stateDB) SubRefund(gas uint64) {
	s.journal = append(s.journal, jentry{kind: jRefund, prevU64: s.refund})
	if gas > s.refund {
		s.fail(errRefundUnderflow)
		return
	}
	s.refund -= gas
}

func (s *stateDB) GetRefund() uint64 { return s.refund }

func (s *stateDB) AddLog(l *ethtypes.Log) {
	s.journal = append(s.journal, jentry{kind: jLog})
	s.logs = append(s.logs, l)
}

func (s *stateDB) AddPreimage(common.Hash, []byte) {}

// --- access list (EIP-2929/2930) ---

func (s *stateDB) AddressInAccessList(addr common.Address) bool {
	_, ok := s.al[addr]
	return ok
}

func (s *stateDB) SlotInAccessList(addr common.Address, slot common.Hash) (bool, bool) {
	m, ok := s.al[addr]
	if !ok {
		return false, false
	}
	if m == nil {
		return true, false
	}
	_, sok := m[slot]
	return true, sok
}

func (s *stateDB) AddAddressToAccessList(addr common.Address) {
	if _, ok := s.al[addr]; !ok {
		s.journal = append(s.journal, jentry{kind: jALAddr, addr: addr})
		s.al[addr] = nil
	}
}

func (s *stateDB) AddSlotToAccessList(addr common.Address, slot common.Hash) {
	m, ok := s.al[addr]
	if !ok {
		s.journal = append(s.journal, jentry{kind: jALAddr, addr: addr})
		m = make(map[common.Hash]struct{})
		s.al[addr] = m
	} else if m == nil {
		m = make(map[common.Hash]struct{})
		s.al[addr] = m
	}
	if _, ok := m[slot]; !ok {
		s.journal = append(s.journal, jentry{kind: jALSlot, addr: addr, slot: slot})
		m[slot] = struct{}{}
	}
}

// Prepare mirrors geth's statedb.Prepare (Berlin warm-up set; Shanghai warm
// coinbase). Transient storage is already clear: reset ran before this.
func (s *stateDB) Prepare(rules params.Rules, sender, coinbase common.Address, dst *common.Address, precompiles []common.Address, list ethtypes.AccessList) {
	if !rules.IsBerlin {
		return
	}
	s.AddAddressToAccessList(sender)
	if dst != nil {
		s.AddAddressToAccessList(*dst)
	}
	for _, addr := range precompiles {
		s.AddAddressToAccessList(addr)
	}
	for _, el := range list {
		s.AddAddressToAccessList(el.Address)
		for _, key := range el.StorageKeys {
			s.AddSlotToAccessList(el.Address, key)
		}
	}
	if rules.IsShanghai {
		s.AddAddressToAccessList(coinbase)
	}
}

// --- StateDBRemainder ---

func (s *stateDB) TxHash() common.Hash { return common.Hash{} }
func (s *stateDB) TxIndex() int        { return 0 }

var _ vm.StateDB = (*stateDB)(nil)
