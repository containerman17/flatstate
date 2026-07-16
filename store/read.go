package store

import (
	"bytes"
	"fmt"
	"math"

	"github.com/PowerDNS/lmdb-go/lmdb"

	"github.com/containerman17/flatstate/capture"
	"github.com/containerman17/flatstate/schema"
)

// Latest reads the newest committed value (tip pin-on-miss path, D8).
const Latest = uint64(math.MaxUint64)

// seekLE positions cur at the greatest write at or before the block encoded
// in seekKey (which ends in ^block). Returns the row's block and raw value.
func seekLE(cur *lmdb.Cursor, seekKey []byte) (block uint64, val []byte, found bool, err error) {
	prefixLen := len(seekKey) - 8
	k, v, err := cur.Get(seekKey, nil, lmdb.SetRange)
	if lmdb.IsNotFound(err) {
		return 0, nil, false, nil
	}
	if err != nil {
		return 0, nil, false, err
	}
	if len(k) != len(seekKey) || !bytes.Equal(k[:prefixLen], seekKey[:prefixLen]) {
		return 0, nil, false, nil
	}
	return schema.DecodeInvBlock(k[prefixLen:]), v, true, nil
}

// checkAt validates a historical read height per D13.
func (d *DB) checkAt(at uint64) error {
	s, ok, err := d.Genesis()
	if err != nil {
		return err
	}
	if !ok {
		return ErrNoGenesis
	}
	if at < s {
		return fmt.Errorf("%w: block %d < S %d", ErrBelowGenesis, at, s)
	}
	return nil
}

// missAllowed reports nil when an absent row means "key does not exist"
// (baseline complete), else ErrBaselineIncomplete (D6/D13).
func (d *DB) missAllowed() error {
	done, err := d.BaselineComplete()
	if err != nil {
		return err
	}
	if !done {
		return ErrBaselineIncomplete
	}
	return nil
}

// GetAccount returns the account post-image at the greatest write at or
// before block at. exists=false means the account does not exist at that
// height (destructed or never created).
func (d *DB) GetAccount(addr schema.Address, at uint64) (acct schema.Account, exists bool, err error) {
	if err = d.checkAt(at); err != nil {
		return
	}
	err = d.env.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true
		cur, err := txn.OpenCursor(d.dbi)
		if err != nil {
			return err
		}
		defer cur.Close()
		var kbuf [schema.SlotKeyLen]byte
		h, raw, found, err := seekLE(cur, schema.AppendAccountKey(kbuf[:0], addr, at))
		if err != nil {
			return err
		}
		dh, _, dfound, err := seekLE(cur, schema.AppendDestructKey(kbuf[:0], addr, at))
		if err != nil {
			return err
		}
		switch {
		case found && (!dfound || dh <= h):
			// dh == h is destruct-then-recreate in one block: the account row
			// is the end-of-block post-image (capture batch contract).
			acct, err = schema.DecodeAccount(raw)
			exists = err == nil
			return err
		case found || dfound:
			// Destructed after its last write, or created-and-destructed in
			// one block (destruct marker only): does not exist.
			return nil
		default:
			return d.missAllowed()
		}
	})
	return
}

// GetSlot returns the slot value at the greatest write at or before block at.
// A missing slot of a covered baseline is zero; destructs after the last
// write read zero (D5).
func (d *DB) GetSlot(addr schema.Address, slot schema.Hash, at uint64) (val schema.Hash, err error) {
	if err = d.checkAt(at); err != nil {
		return
	}
	err = d.env.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true
		cur, err := txn.OpenCursor(d.dbi)
		if err != nil {
			return err
		}
		defer cur.Close()
		var kbuf [schema.SlotKeyLen]byte
		h, raw, found, err := seekLE(cur, schema.AppendSlotKey(kbuf[:0], addr, slot, at))
		if err != nil {
			return err
		}
		dh, _, dfound, err := seekLE(cur, schema.AppendDestructKey(kbuf[:0], addr, at))
		if err != nil {
			return err
		}
		if !found {
			// No write at or before at. Zero if the account was destructed
			// at or before at, or if the baseline is complete (key covered,
			// so absence means the slot was always zero).
			if dfound {
				return nil
			}
			return d.missAllowed()
		}
		if dfound && dh > h {
			return nil // destructed after the slot's last write: zero
		}
		if dfound && dh == h {
			// Same-block destruct and slot write: only valid when the account
			// was recreated in that block (account row at exactly dh). A slot
			// row for a dead account violates the capture contract; refuse to
			// guess (D13).
			ah, _, afound, err := seekLE(cur, schema.AppendAccountKey(kbuf[:0], addr, dh))
			if err != nil {
				return err
			}
			if !afound || ah != dh {
				return fmt.Errorf("%w: slot write and destruct at block %d without recreation", ErrDestructEdge, dh)
			}
		}
		if len(raw) != 32 {
			return fmt.Errorf("store: slot row has %d bytes, want 32", len(raw))
		}
		copy(val[:], raw)
		return nil
	})
	return
}

// GetCode returns contract code by hash. Missing code is a loud error.
func (d *DB) GetCode(hash schema.Hash) (code []byte, err error) {
	err = d.env.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true
		v, err := txn.Get(d.dbi, schema.AppendCodeKey(nil, hash))
		if lmdb.IsNotFound(err) {
			return fmt.Errorf("%w: code %x", ErrNotFound, hash[:8])
		}
		if err != nil {
			return err
		}
		code = bytes.Clone(v)
		return nil
	})
	return
}

// GetDiff returns the 0x04 per-block diff (the capture batch verbatim).
// Returns ErrNotFound when the block has no diff row.
func (d *DB) GetDiff(block uint64) (b *capture.Batch, err error) {
	err = d.env.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true
		v, err := txn.Get(d.dbi, schema.AppendDiffKey(nil, block))
		if lmdb.IsNotFound(err) {
			return fmt.Errorf("%w: diff for block %d", ErrNotFound, block)
		}
		if err != nil {
			return err
		}
		b, err = capture.Decode(v)
		return err
	})
	return
}
