// Package replay re-runs history block by block (DESIGN.md D12): a session
// seeds a mutable map lazily from store reads at height B, then advances by
// applying 0x04 diff rows interleaved with the 0x05 mempool log by
// timestamp. Same apply code and miss path as live (mem.Map). A session
// tailing a live writer sees each block as it commits (LMDB MVCC).
package replay

import (
	"errors"

	"github.com/containerman17/flatstate/capture"
	"github.com/containerman17/flatstate/mem"
	"github.com/containerman17/flatstate/schema"
	"github.com/containerman17/flatstate/store"
)

// Event is the next replay item: exactly one field is set. A Block event
// means the diff was already applied and the session now sits at that
// height; a Tx event is a mempool arrival to simulate against the current
// state.
type Event struct {
	Block *capture.Batch
	Tx    []byte
	Time  uint64 // arrival time for Tx, unix ms
}

// Session is single-threaded.
type Session struct {
	db      *store.DB
	m       *mem.Map
	block   uint64 // state reflects end of this block
	nextSeq uint64
}

// Open starts a session at startBlock (>= S). Mempool events are delivered
// starting from the first arrival at or after startBlock's timestamp (or
// from the beginning when startBlock has no diff row, e.g. startBlock == S).
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
	sess := &Session{db: db, m: mem.NewMap(), block: startBlock}
	var startTime uint64
	if d, err := db.GetDiff(startBlock); err == nil {
		startTime = d.Time
	} else if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	seq, ok, err := db.FirstMempoolAt(startTime)
	if err != nil {
		return nil, err
	}
	if ok {
		sess.nextSeq = seq
	}
	// When no entry exists yet, start from seq 0 and let Next discover new
	// appends while tailing a live writer.
	return sess, nil
}

// Block returns the session's current height.
func (s *Session) Block() uint64 { return s.block }

// Next advances the session: it returns the earlier of the next mempool
// arrival and the next block diff (block wins timestamp ties). Returns
// (nil, nil) when caught up with the store.
func (s *Session) Next() (*Event, error) {
	diff, err := s.db.GetDiff(s.block + 1)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	t, tx, haveTx, err := s.db.GetMempool(s.nextSeq)
	if err != nil {
		return nil, err
	}
	if haveTx && (diff == nil || t < diff.Time) {
		s.nextSeq++
		return &Event{Tx: tx, Time: t}, nil
	}
	if diff == nil {
		return nil, nil // caught up
	}
	s.m.Apply(diff)
	s.block = diff.Block
	return &Event{Block: diff}, nil
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
