// Package replay re-runs history block by block (DESIGN.md D12): a session
// seeds a mutable map lazily from store reads at height B, then advances by
// applying 0x04 diff rows. Same apply code and miss path as live (mem.Map).
// A session tailing a live writer sees each block as it commits (LMDB MVCC).
package replay

import (
	"errors"

	"github.com/containerman17/flatstate/capture"
	"github.com/containerman17/flatstate/mem"
	"github.com/containerman17/flatstate/schema"
	"github.com/containerman17/flatstate/store"
)

// Session is single-threaded.
type Session struct {
	db    *store.DB
	m     *mem.Map
	block uint64 // state reflects end of this block
}

// Open starts a session at startBlock (>= S).
func Open(db *store.DB, startBlock uint64) (*Session, error) {
	s, ok, err := db.Genesis()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, store.ErrNoGenesis
	}
	if startBlock < s {
		return nil, store.ErrBelowGenesis
	}
	return &Session{db: db, m: mem.NewMap(), block: startBlock}, nil
}

// Block returns the session's current height.
func (s *Session) Block() uint64 { return s.block }

// Next applies the next block diff and returns it; the session then sits at
// that height. Returns (nil, nil) when caught up with the store.
func (s *Session) Next() (*capture.Batch, error) {
	diff, err := s.db.GetDiff(s.block + 1)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil // caught up
	}
	if err != nil {
		return nil, err
	}
	s.m.Apply(diff)
	s.block = diff.Block
	return diff, nil
}

// Account reads through the session map, pinning from the store at the
// session's current height on a miss.
func (s *Session) Account(addr schema.Address) (schema.Account, bool, error) {
	if a, ok := s.m.Account(addr); ok {
		return a.ToSchema(), a.Exists, nil
	}
	acct, exists, err := s.db.GetAccount(addr, s.block)
	if err != nil {
		return schema.Account{}, false, err
	}
	s.m.PinAccount(addr, &acct, exists)
	return acct, exists, nil
}

// Slot reads through the session map, pinning on miss.
func (s *Session) Slot(addr schema.Address, slot schema.Hash) (schema.Hash, error) {
	if a, ok := s.m.Account(addr); ok {
		if !a.Exists {
			return schema.Hash{}, nil
		}
		if v, ok := a.Storage[slot]; ok {
			return v, nil
		}
	} else {
		// Pin the account first; a slot pin must not fake account knowledge.
		if _, _, err := s.Account(addr); err != nil {
			return schema.Hash{}, err
		}
	}
	v, err := s.db.GetSlot(addr, slot, s.block)
	if err != nil {
		return schema.Hash{}, err
	}
	s.m.PinSlot(addr, slot, v)
	return v, nil
}

// Code reads contract code, pinning on miss.
func (s *Session) Code(hash schema.Hash) ([]byte, error) {
	if c, ok := s.m.Code(hash); ok {
		return c, nil
	}
	c, err := s.db.GetCode(hash)
	if err != nil {
		return nil, err
	}
	s.m.PinCode(hash, c)
	return c, nil
}
