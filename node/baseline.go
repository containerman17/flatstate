package node

import (
	"bytes"
	"fmt"

	"github.com/containerman17/flatstate/schema"
	"github.com/containerman17/flatstate/store"
)

// EntryKind orders the three baseline phases; entries must arrive in this
// order (matching the store's key layout: 0x01 accounts, 0x02 slots, 0x06
// codes).
type EntryKind byte

const (
	EntryAccount EntryKind = 1
	EntrySlot    EntryKind = 2
	EntryCode    EntryKind = 3
)

// Entry is one full-state item at the pivot S. Only the fields for its Kind
// are meaningful.
type Entry struct {
	Kind    EntryKind
	Addr    schema.Address
	Account schema.Account
	Slot    schema.Hash
	Value   schema.Hash
	Hash    schema.Hash // code hash (EntryCode)
	Code    []byte
}

// StateIterator enumerates the full state at the pivot S: all accounts by
// ascending address, then all slots by ascending (address, slot), then all
// codes by ascending hash. See docs/node-integration.md for why no local
// preimage-keyed enumeration exists today and what can implement this.
type StateIterator interface {
	// Next returns the next entry; ok=false at a clean end of the state.
	Next() (e Entry, ok bool, err error)
}

// RunBaseline drains the iterator into a store baseline at S and sets the
// baseline_complete watermark. Ordering is validated and any violation
// aborts loudly (D13): the store's read correctness depends on it.
func RunBaseline(db *store.DB, s uint64, it StateIterator) error {
	bl, err := db.NewBaseline(s)
	if err != nil {
		return err
	}
	var (
		prevKind EntryKind
		prevKey  []byte
	)
	for n := 0; ; n++ {
		e, ok, err := it.Next()
		if err != nil {
			return fmt.Errorf("node: baseline iterator at entry %d: %w", n, err)
		}
		if !ok {
			return bl.Finish()
		}
		var key []byte
		switch e.Kind {
		case EntryAccount:
			key = e.Addr[:]
		case EntrySlot:
			key = append(append([]byte{}, e.Addr[:]...), e.Slot[:]...)
		case EntryCode:
			key = e.Hash[:]
		default:
			return fmt.Errorf("node: baseline entry %d has unknown kind %d", n, e.Kind)
		}
		if e.Kind < prevKind {
			return fmt.Errorf("node: baseline entry %d kind %d after kind %d", n, e.Kind, prevKind)
		}
		if e.Kind == prevKind && bytes.Compare(key, prevKey) <= 0 {
			return fmt.Errorf("node: baseline entry %d key %x not ascending", n, key)
		}
		prevKind, prevKey = e.Kind, key
		switch e.Kind {
		case EntryAccount:
			err = bl.Account(e.Addr, &e.Account)
		case EntrySlot:
			err = bl.Slot(e.Addr, e.Slot, e.Value)
		case EntryCode:
			err = bl.Code(e.Hash, e.Code)
		}
		if err != nil {
			return err
		}
	}
}
