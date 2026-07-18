// Package mem is the in-memory model (DESIGN.md D8, D9): a lazily pinned
// base map of finalized values, a small stack of unfinalized per-block
// layers, and per-executor side buffers for pin-on-miss during read phases.
//
// Concurrency discipline (D9), enforced structurally:
//   - Read methods (Account, Slot, Code) never write the base map; a cold
//     miss reads LMDB and records the pin into the caller's SideBuffer.
//     They are valid only between BeginBatch and EndBatch (read lock held).
//   - All base-map mutation lives in the write-phase methods (ApplyBlock,
//     Finalize, PreferenceReset), which take the write lock and first merge
//     every registered side buffer, so a pinned value can never be applied
//     over by a later diff out of order.
package mem

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/holiman/uint256"

	"github.com/containerman17/flatstate/capture"
	"github.com/containerman17/flatstate/schema"
	"github.com/containerman17/flatstate/store"
)

// Account is a base-map entry. Presence in the map encodes knowledge:
// a fetched zero is stored as zero, Exists=false is a known-nonexistent
// account. Leaves are pointer-free; the GC never scans them.
type Account struct {
	Balance  uint256.Int // inline, pointer-free ([4]uint64)
	Nonce    uint64
	CodeHash schema.Hash
	Exists   bool
	Storage  map[schema.Hash]schema.Hash
}

func fromSchema(a *schema.Account) (out Account) {
	out.Balance = a.Balance
	out.Nonce = a.Nonce
	out.CodeHash = a.CodeHash
	out.Exists = true
	return
}

func (a *Account) ToSchema() (out schema.Account) {
	out.Balance = a.Balance
	out.Nonce = a.Nonce
	out.CodeHash = a.CodeHash
	return
}

// Map is the shared lazily pinned state map. It is also reused by replay
// sessions (single-threaded there), so it carries no locking itself.
type Map struct {
	accounts map[schema.Address]*Account
	code     map[schema.Hash][]byte
}

func NewMap() *Map {
	return &Map{
		accounts: make(map[schema.Address]*Account),
		code:     make(map[schema.Hash][]byte),
	}
}

// Account returns the entry and whether the address is known.
func (m *Map) Account(addr schema.Address) (*Account, bool) {
	a, ok := m.accounts[addr]
	return a, ok
}

// Code returns pinned code and whether the hash is known.
func (m *Map) Code(hash schema.Hash) ([]byte, bool) {
	c, ok := m.code[hash]
	return c, ok
}

// PinAccount records knowledge of an account (exists=false pins nonexistence).
func (m *Map) PinAccount(addr schema.Address, acct *schema.Account, exists bool) *Account {
	if a, ok := m.accounts[addr]; ok {
		return a
	}
	a := &Account{Exists: exists, Storage: make(map[schema.Hash]schema.Hash)}
	if exists {
		*a = fromSchema(acct)
		a.Storage = make(map[schema.Hash]schema.Hash)
	}
	m.accounts[addr] = a
	return a
}

// PinSlot records a fetched slot value. The account must already be pinned;
// pinning a slot without account knowledge would fake account presence.
func (m *Map) PinSlot(addr schema.Address, slot schema.Hash, val schema.Hash) {
	a, ok := m.accounts[addr]
	if !ok {
		panic("mem: PinSlot before PinAccount")
	}
	if _, known := a.Storage[slot]; !known {
		a.Storage[slot] = val
	}
}

// PinCode records fetched code.
func (m *Map) PinCode(hash schema.Hash, code []byte) {
	if _, ok := m.code[hash]; !ok {
		m.code[hash] = code
	}
}

// Apply applies a finalized block diff in place, restricted to keys already
// present (D8): an absent key needs no update because its next miss reads
// LMDB, which is already current (D7 ordering). Ops apply in batch order.
func (m *Map) Apply(b *capture.Batch) {
	for i := range b.Ops {
		op := &b.Ops[i]
		switch op.Kind {
		case capture.OpAccount:
			if a, ok := m.accounts[op.Addr]; ok {
				st := a.Storage
				*a = fromSchema(&op.Account)
				a.Storage = st
			}
		case capture.OpSlot:
			if a, ok := m.accounts[op.Addr]; ok {
				if _, known := a.Storage[op.Slot]; known {
					a.Storage[op.Slot] = op.Value
				}
			}
		case capture.OpDeleteSlot:
			if a, ok := m.accounts[op.Addr]; ok {
				if _, known := a.Storage[op.Slot]; known {
					a.Storage[op.Slot] = schema.Hash{}
				}
			}
		case capture.OpDestruct:
			if a, ok := m.accounts[op.Addr]; ok {
				st := a.Storage
				*a = Account{Storage: st}
				for k := range st {
					st[k] = schema.Hash{} // keep slot knowledge: post-destruct slots are zero
				}
			}
		case capture.OpCode:
			// Codes are content-addressed and immutable; stay lazy, pin on miss.
		}
	}
}

// SideBuffer is the per-executor pin buffer (D9). Written only during read
// phases by its owning executor, merged and cleared by the next write phase.
type SideBuffer struct {
	accounts map[schema.Address]sbAccount
	slots    map[schema.SKey]schema.Hash
	code     map[schema.Hash][]byte
}

type sbAccount struct {
	acct   schema.Account
	exists bool
}

func NewSideBuffer() *SideBuffer {
	return &SideBuffer{
		accounts: make(map[schema.Address]sbAccount),
		slots:    make(map[schema.SKey]schema.Hash),
		code:     make(map[schema.Hash][]byte),
	}
}

// State is the live tip view: base map + unfinalized layer stack.
type State struct {
	db *store.DB

	mu        sync.RWMutex
	base      *Map
	layers    []layer // oldest first
	buffers   []*SideBuffer
	finalized uint64
	tip       schema.Hash // preferred tip hash (batch staleness stamp)
	tipBlock  uint64      // height of the newest applied block (0 = none yet)
	tipTime   uint64      // its timestamp, unix seconds

	pinsMerged atomic.Uint64 // side-buffer entries merged into the base (D9)
}

type layer struct {
	batch      *capture.Batch
	accounts   map[schema.Address]schema.Account
	slots      map[schema.SKey]schema.Hash
	destructed map[schema.Address]bool
}

func newLayer(b *capture.Batch) layer {
	l := layer{
		batch:      b,
		accounts:   make(map[schema.Address]schema.Account),
		slots:      make(map[schema.SKey]schema.Hash),
		destructed: make(map[schema.Address]bool),
	}
	for i := range b.Ops {
		op := &b.Ops[i]
		switch op.Kind {
		case capture.OpAccount:
			l.accounts[op.Addr] = op.Account
		case capture.OpSlot:
			l.slots[schema.SKey{Addr: op.Addr, Slot: op.Slot}] = op.Value
		case capture.OpDeleteSlot:
			l.slots[schema.SKey{Addr: op.Addr, Slot: op.Slot}] = schema.Hash{}
		case capture.OpDestruct:
			l.destructed[op.Addr] = true
			// Post-image contract: recreation ops follow the destruct, so
			// nothing earlier in this batch needs removal; be defensive anyway.
			delete(l.accounts, op.Addr)
			for k := range l.slots {
				if k.Addr == op.Addr {
					delete(l.slots, k)
				}
			}
		}
	}
	return l
}

// New builds a State over db. The finalized watermark, if present, seeds the
// height; the tip hash starts zero until the first block event.
func New(db *store.DB) (*State, error) {
	s := &State{db: db, base: NewMap()}
	h, ok, err := db.Finalized()
	if err != nil {
		return nil, err
	}
	if ok {
		s.finalized = h
	}
	return s, nil
}

// Register adds an executor's side buffer to the merge set.
func (s *State) Register(sb *SideBuffer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buffers = append(s.buffers, sb)
}

// BeginBatch enters a read phase and returns the staleness stamp (preferred
// tip hash). Every read must happen between BeginBatch and EndBatch.
func (s *State) BeginBatch() schema.Hash {
	s.mu.RLock()
	return s.tip
}

// EndBatch leaves the read phase.
func (s *State) EndBatch() { s.mu.RUnlock() }

// TipHash returns the current preferred tip hash. Never call it while
// holding a batch (recursive RLock with a pending writer deadlocks).
func (s *State) TipHash() schema.Hash {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tip
}

// Finalized returns the applied finalized height.
func (s *State) FinalizedHeight() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.finalized
}

// PinsMerged returns the cumulative count of side-buffer entries merged into
// the base map by write phases (accounts + slots + codes).
func (s *State) PinsMerged() uint64 { return s.pinsMerged.Load() }

// --- read phase (call only between BeginBatch/EndBatch) ---

// TipInfo returns the tip hash, block height, and timestamp (unix seconds) of
// the newest applied block. Read-phase only: the caller must hold a batch
// (BeginBatch/EndBatch); it takes no lock itself. block == 0 means no block
// event has been applied yet (the executor must refuse to simulate, D13).
func (s *State) TipInfo() (schema.Hash, uint64, uint64) {
	return s.tip, s.tipBlock, s.tipTime
}

// Account resolves an account through layers -> base -> side buffer -> LMDB.
func (s *State) Account(addr schema.Address, sb *SideBuffer) (schema.Account, bool, error) {
	for i := len(s.layers) - 1; i >= 0; i-- {
		l := &s.layers[i]
		if a, ok := l.accounts[addr]; ok {
			return a, true, nil
		}
		if l.destructed[addr] {
			return schema.Account{}, false, nil
		}
	}
	if a, ok := s.base.accounts[addr]; ok {
		return a.ToSchema(), a.Exists, nil
	}
	if e, ok := sb.accounts[addr]; ok {
		return e.acct, e.exists, nil
	}
	acct, exists, err := s.db.GetAccount(addr, store.Latest)
	if err != nil {
		return schema.Account{}, false, err
	}
	sb.accounts[addr] = sbAccount{acct: acct, exists: exists}
	return acct, exists, nil
}

// Slot resolves a storage slot through layers -> base -> side buffer -> LMDB.
func (s *State) Slot(addr schema.Address, slot schema.Hash, sb *SideBuffer) (schema.Hash, error) {
	sk := schema.SKey{Addr: addr, Slot: slot}
	for i := len(s.layers) - 1; i >= 0; i-- {
		l := &s.layers[i]
		if v, ok := l.slots[sk]; ok {
			return v, nil
		}
		if l.destructed[addr] {
			return schema.Hash{}, nil
		}
	}
	if a, ok := s.base.accounts[addr]; ok {
		if !a.Exists {
			return schema.Hash{}, nil
		}
		if v, ok := a.Storage[slot]; ok {
			return v, nil
		}
	}
	if v, ok := sb.slots[sk]; ok {
		return v, nil
	}
	v, err := s.db.GetSlot(addr, slot, store.Latest)
	if err != nil {
		return schema.Hash{}, err
	}
	sb.slots[sk] = v
	// A slot pin needs its account pinned too (the merge would otherwise
	// have to fake account knowledge); fetch it once if fully unknown.
	if _, ok := s.base.accounts[addr]; !ok {
		if _, ok := sb.accounts[addr]; !ok {
			acct, exists, err := s.db.GetAccount(addr, store.Latest)
			if err != nil {
				return schema.Hash{}, err
			}
			sb.accounts[addr] = sbAccount{acct: acct, exists: exists}
		}
	}
	return v, nil
}

// Code resolves contract code through base -> side buffer -> LMDB.
func (s *State) Code(hash schema.Hash, sb *SideBuffer) ([]byte, error) {
	if c, ok := s.base.code[hash]; ok {
		return c, nil
	}
	if c, ok := sb.code[hash]; ok {
		return c, nil
	}
	c, err := s.db.GetCode(hash)
	if err != nil {
		return nil, err
	}
	sb.code[hash] = c
	return c, nil
}

// --- write phase ---

// lockAndDrain enters the write phase: takes the write lock and merges every
// registered side buffer into the base BEFORE the caller mutates, so pins
// can never overwrite a newer applied diff. Caller must Unlock.
func (s *State) lockAndDrain() {
	s.mu.Lock()
	var merged uint64
	for _, sb := range s.buffers {
		merged += uint64(len(sb.accounts) + len(sb.slots) + len(sb.code))
		for addr, e := range sb.accounts {
			if e.exists {
				s.base.PinAccount(addr, &e.acct, true)
			} else {
				s.base.PinAccount(addr, nil, false)
			}
		}
		for sk, v := range sb.slots {
			if _, ok := s.base.accounts[sk.Addr]; ok {
				s.base.PinSlot(sk.Addr, sk.Slot, v)
			}
		}
		for h, c := range sb.code {
			s.base.PinCode(h, c)
		}
		clear(sb.accounts)
		clear(sb.slots)
		clear(sb.code)
	}
	s.pinsMerged.Add(merged)
}

// ApplyBlock pushes a new unfinalized block layer and moves the tip stamp.
func (s *State) ApplyBlock(b *capture.Batch) {
	s.lockAndDrain()
	defer s.mu.Unlock()
	s.layers = append(s.layers, newLayer(b))
	s.tip = b.Hash
	s.tipBlock = b.Block
	s.tipTime = b.Time / 1000
}

// Finalize applies the oldest unfinalized layer to the base (cached keys
// only). Per D7 the caller must have committed the block to the store FIRST;
// after Finalize returns it bumps the store watermark (step 3).
func (s *State) Finalize(block uint64, hash schema.Hash) error {
	s.lockAndDrain()
	defer s.mu.Unlock()
	if len(s.layers) == 0 || s.layers[0].batch.Block != block {
		return fmt.Errorf("mem: finalize %d does not match oldest unfinalized layer", block)
	}
	if s.layers[0].batch.Hash != hash {
		return fmt.Errorf("mem: finalize %d hash mismatch", block)
	}
	s.base.Apply(s.layers[0].batch)
	s.layers = s.layers[1:]
	s.finalized = block
	if len(s.layers) == 0 {
		s.tip = hash
	}
	return nil
}

// PreferenceReset drops all unfinalized layers and rebuilds from the new
// preferred chain's captures (oldest first). The base is untouched (D8).
func (s *State) PreferenceReset(preferred []*capture.Batch) {
	s.lockAndDrain()
	defer s.mu.Unlock()
	s.layers = s.layers[:0]
	for _, b := range preferred {
		s.layers = append(s.layers, newLayer(b))
		s.tip = b.Hash
		s.tipBlock = b.Block
		s.tipTime = b.Time / 1000
	}
}
