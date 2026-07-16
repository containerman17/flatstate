package store

import (
	"bytes"
	"fmt"
	"math"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"golang.org/x/crypto/sha3"

	"github.com/containerman17/flatstate/capture"
	"github.com/containerman17/flatstate/schema"
)

// Keccak hashes a preimage for a 0x07/0x08 baseline probe (the one keccak a
// cold key ever costs, D6 rev 2).
func Keccak(b []byte) (h schema.Hash) {
	k := sha3.NewLegacyKeccak256()
	k.Write(b)
	k.Sum(h[:0])
	return h
}

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
			// D6 rev 2 read order step 2: no preimage history row, probe the
			// hash-keyed baseline at S. Valid for any at >= S: a later change
			// would have left a history row.
			raw, bok, err := getBaseRow(txn, d.dbi, schema.AppendBaseAccountKey(kbuf[:0], Keccak(addr[:])))
			if err != nil {
				return err
			}
			if bok {
				acct, err = schema.DecodeAccount(raw)
				exists = err == nil
				return err
			}
			// Step 3: known zero, if the baseline is complete.
			return d.missAllowed()
		}
	})
	return
}

// getBaseRow reads one 0x07/0x08 baseline row. ok=false on absence.
func getBaseRow(txn *lmdb.Txn, dbi lmdb.DBI, key []byte) ([]byte, bool, error) {
	v, err := txn.Get(dbi, key)
	if lmdb.IsNotFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return v, true, nil
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
			// at or before at (destruct wiped pre-S storage too).
			if dfound {
				return nil
			}
			// D6 rev 2 step 2: hash-keyed baseline probe.
			raw, bok, err := getBaseRow(txn, d.dbi,
				schema.AppendBaseSlotKey(kbuf[:0], Keccak(addr[:]), Keccak(slot[:])))
			if err != nil {
				return err
			}
			if bok {
				if len(raw) != 32 {
					return fmt.Errorf("store: baseline slot row has %d bytes, want 32", len(raw))
				}
				copy(val[:], raw)
				return nil
			}
			// Step 3: known zero, if the baseline is complete.
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

// MaxBaseAccountWithPrefix returns the greatest 0x07 baseline account hash
// whose first byte is p. Loader resume: baseline rows commit in ascending
// order per segment, so this is the segment's durable watermark.
func (d *DB) MaxBaseAccountWithPrefix(p byte) (h schema.Hash, ok bool, err error) {
	err = d.env.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true
		cur, err := txn.OpenCursor(d.dbi)
		if err != nil {
			return err
		}
		defer cur.Close()
		seek := make([]byte, 33)
		seek[0] = schema.PrefBaseAccount
		seek[1] = p
		for i := 2; i < len(seek); i++ {
			seek[i] = 0xff
		}
		k, _, err := cur.Get(seek, nil, lmdb.SetRange)
		switch {
		case lmdb.IsNotFound(err):
			k, _, err = cur.Get(nil, nil, lmdb.Last)
		case err == nil && !bytes.Equal(k, seek):
			k, _, err = cur.Get(nil, nil, lmdb.Prev)
		}
		if lmdb.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if len(k) == 33 && k[0] == schema.PrefBaseAccount && k[1] == p {
			copy(h[:], k[1:])
			ok = true
		}
		return nil
	})
	return
}

// MaxBaseSlot returns the greatest committed 0x08 slot hash of addrHash
// within [start, end] (inclusive; nil end = to the top of the keyspace).
// Loader resume for storage sub-ranges: each sub-walker commits its slots
// contiguously from its start, so this is a sound per-sub-range watermark.
func (d *DB) MaxBaseSlot(addrHash schema.Hash, start, end []byte) (h schema.Hash, ok bool, err error) {
	err = d.env.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true
		cur, err := txn.OpenCursor(d.dbi)
		if err != nil {
			return err
		}
		defer cur.Close()
		seek := make([]byte, 65)
		seek[0] = schema.PrefBaseSlot
		copy(seek[1:33], addrHash[:])
		if end == nil {
			for i := 33; i < 65; i++ {
				seek[i] = 0xff
			}
		} else {
			copy(seek[33:65], end)
		}
		k, _, err := cur.Get(seek, nil, lmdb.SetRange)
		switch {
		case lmdb.IsNotFound(err):
			k, _, err = cur.Get(nil, nil, lmdb.Last)
		case err == nil && !bytes.Equal(k, seek):
			k, _, err = cur.Get(nil, nil, lmdb.Prev)
		}
		if lmdb.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if len(k) != 65 || k[0] != schema.PrefBaseSlot || !bytes.Equal(k[1:33], addrHash[:]) {
			return nil
		}
		if start != nil && bytes.Compare(k[33:65], start) < 0 {
			return nil
		}
		copy(h[:], k[33:65])
		ok = true
		return nil
	})
	return
}

// MissingCodeHashes scans the 0x07 baseline accounts and returns the unique
// referenced code hashes that have no 0x06 row yet (loader code sweep).
// One long read txn; run it only while the writer is quiet.
func (d *DB) MissingCodeHashes() ([]schema.Hash, error) {
	emptyCode := Keccak(nil)
	seen := make(map[schema.Hash]struct{})
	var missing []schema.Hash
	err := d.env.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true
		cur, err := txn.OpenCursor(d.dbi)
		if err != nil {
			return err
		}
		defer cur.Close()
		k, v, err := cur.Get([]byte{schema.PrefBaseAccount}, nil, lmdb.SetRange)
		for ; err == nil; k, v, err = cur.Get(nil, nil, lmdb.Next) {
			if len(k) != 33 || k[0] != schema.PrefBaseAccount {
				break
			}
			a, derr := schema.DecodeAccount(v)
			if derr != nil {
				return derr
			}
			ch := a.CodeHash
			if ch == (schema.Hash{}) || ch == emptyCode {
				continue
			}
			if _, dup := seen[ch]; dup {
				continue
			}
			seen[ch] = struct{}{}
			if _, gerr := txn.Get(d.dbi, schema.AppendCodeKey(nil, ch)); gerr == nil {
				continue
			} else if !lmdb.IsNotFound(gerr) {
				return gerr
			}
			missing = append(missing, ch)
		}
		if err != nil && !lmdb.IsNotFound(err) {
			return err
		}
		return nil
	})
	return missing, err
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
