// Package store is the main LMDB environment (DESIGN.md D4, D6, D7): the
// readers' single source of truth. One writer process, N read-only processes.
package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/PowerDNS/lmdb-go/lmdb"

	"github.com/containerman17/flatstate/capture"
	"github.com/containerman17/flatstate/schema"
)

// DefaultMapSize is a fat sparse map (D4); pages are only allocated as written.
const DefaultMapSize = 200 << 30

// Fail-loud errors per D13.
var (
	ErrNoGenesis          = errors.New("store: history genesis not set (no baseline started)")
	ErrBelowGenesis       = errors.New("store: read below history genesis S")
	ErrBaselineIncomplete = errors.New("store: baseline incomplete, key not yet covered")
	ErrDestructEdge       = errors.New("store: destruct edge the store cannot answer")
	ErrNotFound           = errors.New("store: not found")
)

// DB is one LMDB environment handle. Open returns the writer handle,
// OpenReadOnly a reader handle for a second process.
type DB struct {
	env *lmdb.Env
	dbi lmdb.DBI
	ro  bool

	mempoolMu sync.Mutex // guards nextSeq; keeps 0x05 seq dense
	nextSeq   uint64

	genesisPlus1 atomic.Uint64 // cached MetaGenesis+1; 0 = not yet observed
	baselineDone atomic.Bool   // cached MetaBaselineComplete; monotonic
}

// Open opens (creating if needed) the environment for writing.
// mapSize <= 0 uses DefaultMapSize.
func Open(path string, mapSize int64) (*DB, error) {
	if mapSize <= 0 {
		mapSize = DefaultMapSize
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, err
	}
	d, err := open(path, mapSize, 0)
	if err != nil {
		return nil, err
	}
	// Initialize the mempool seq counter from the last 0x05 row.
	err = d.env.View(func(txn *lmdb.Txn) error {
		last, ok, err := lastMempoolSeq(txn, d.dbi)
		if err != nil {
			return err
		}
		if ok {
			d.nextSeq = last + 1
		}
		return nil
	})
	if err != nil {
		d.env.Close()
		return nil, err
	}
	return d, nil
}

// OpenReadOnly opens an existing environment for reading (second processes).
func OpenReadOnly(path string) (*DB, error) {
	return open(path, 0, lmdb.Readonly)
}

func open(path string, mapSize int64, flags uint) (*DB, error) {
	env, err := lmdb.NewEnv()
	if err != nil {
		return nil, err
	}
	if mapSize > 0 {
		if err := env.SetMapSize(mapSize); err != nil {
			env.Close()
			return nil, err
		}
	}
	if err := env.Open(path, flags, 0o644); err != nil {
		env.Close()
		return nil, err
	}
	d := &DB{env: env, ro: flags&lmdb.Readonly != 0}
	err = d.env.RunTxn(flags&lmdb.Readonly, func(txn *lmdb.Txn) error {
		dbi, err := txn.OpenRoot(0)
		d.dbi = dbi
		return err
	})
	if err != nil {
		env.Close()
		return nil, err
	}
	return d, nil
}

func (d *DB) Close() error { return d.env.Close() }

// --- meta ---

func (d *DB) putMeta(key []byte, val []byte) error {
	return d.env.Update(func(txn *lmdb.Txn) error {
		return txn.Put(d.dbi, key, val, 0)
	})
}

func getMetaU64(txn *lmdb.Txn, dbi lmdb.DBI, key []byte) (uint64, bool, error) {
	v, err := txn.Get(dbi, key)
	if lmdb.IsNotFound(err) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if len(v) != 8 {
		return 0, false, fmt.Errorf("store: meta %x has %d bytes, want 8", key, len(v))
	}
	return beU64(v), true, nil
}

// Genesis returns the history genesis S.
func (d *DB) Genesis() (uint64, bool, error) {
	if g := d.genesisPlus1.Load(); g != 0 {
		return g - 1, true, nil
	}
	var s uint64
	var ok bool
	err := d.env.View(func(txn *lmdb.Txn) error {
		var err error
		s, ok, err = getMetaU64(txn, d.dbi, schema.MetaGenesis)
		return err
	})
	if err == nil && ok {
		d.genesisPlus1.Store(s + 1)
	}
	return s, ok, err
}

// BaselineComplete reports whether the snapshot baseline watermark is set.
func (d *DB) BaselineComplete() (bool, error) {
	if d.baselineDone.Load() {
		return true, nil
	}
	var done bool
	err := d.env.View(func(txn *lmdb.Txn) error {
		_, err := txn.Get(d.dbi, schema.MetaBaselineComplete)
		if lmdb.IsNotFound(err) {
			return nil
		}
		done = err == nil
		return err
	})
	if done {
		d.baselineDone.Store(true)
	}
	return done, err
}

// Finalized returns the finalized-height watermark.
func (d *DB) Finalized() (uint64, bool, error) {
	var h uint64
	var ok bool
	err := d.env.View(func(txn *lmdb.Txn) error {
		var err error
		h, ok, err = getMetaU64(txn, d.dbi, schema.MetaFinalized)
		return err
	})
	return h, ok, err
}

// SetFinalized bumps the finalized-height watermark. Per D7 this is step 3:
// call it only after WriteBlock committed and the in-memory base was updated.
func (d *DB) SetFinalized(h uint64) error {
	return d.putMeta(schema.MetaFinalized, beBytes(h))
}

// --- per-block write (D7 step 1) ---

// WriteBlock writes one accepted block in a single write txn: history rows
// for every op plus the 0x04 diff row (the batch verbatim). Idempotent:
// bootstrap replay after a crash rewrites identical rows.
func (d *DB) WriteBlock(b *capture.Batch) error {
	return d.env.Update(func(txn *lmdb.Txn) error {
		var kbuf [schema.SlotKeyLen]byte
		var vbuf [schema.AccountSize]byte
		zero := make([]byte, 32)
		for i := range b.Ops {
			op := &b.Ops[i]
			var k, v []byte
			switch op.Kind {
			case capture.OpAccount:
				k = schema.AppendAccountKey(kbuf[:0], op.Addr, b.Block)
				v = schema.EncodeAccount(vbuf[:0], &op.Account)
			case capture.OpSlot:
				k = schema.AppendSlotKey(kbuf[:0], op.Addr, op.Slot, b.Block)
				v = op.Value[:]
			case capture.OpDeleteSlot:
				k = schema.AppendSlotKey(kbuf[:0], op.Addr, op.Slot, b.Block)
				v = zero // tombstone: post-image of a cleared slot is zero
			case capture.OpDestruct:
				k = schema.AppendDestructKey(kbuf[:0], op.Addr, b.Block)
				v = nil
			case capture.OpCode:
				k = schema.AppendCodeKey(kbuf[:0], op.CodeHash)
				v = op.Code
			default:
				return fmt.Errorf("store: unknown op kind %d", op.Kind)
			}
			if err := txn.Put(d.dbi, k, v, 0); err != nil {
				return err
			}
		}
		return txn.Put(d.dbi, schema.AppendDiffKey(kbuf[:0], b.Block), b.Encode(nil), 0)
	})
}

// --- baseline bulk load (D6) ---

const baselineChunk = 1 << 16 // rows per write txn; chunked so block capture txns interleave

// Baseline is the bulk-load path for the full-state snapshot at height S.
// Feed keys in ascending order (all accounts by address, then all slots by
// (address, slot), then codes by hash) for sequential B+tree inserts; the
// loader chunks commits so capture write txns for S+1... interleave.
type Baseline struct {
	d        *DB
	s        uint64
	keys     [][]byte
	vals     [][]byte
	progress []byte // written with the next flush txn, then cleared
}

// NewBaseline starts the baseline at S and records the history genesis meta
// immediately so capture of S+1... can begin in parallel.
func (d *DB) NewBaseline(s uint64) (*Baseline, error) {
	cur, ok, err := d.Genesis()
	if err != nil {
		return nil, err
	}
	if ok && cur != s {
		return nil, fmt.Errorf("store: genesis already set to %d, cannot baseline at %d", cur, s)
	}
	if !ok {
		if err := d.putMeta(schema.MetaGenesis, beBytes(s)); err != nil {
			return nil, err
		}
		d.genesisPlus1.Store(s + 1)
	}
	return &Baseline{d: d, s: s}, nil
}

func (bl *Baseline) add(k, v []byte) error {
	bl.keys = append(bl.keys, k)
	bl.vals = append(bl.vals, v)
	if len(bl.keys) >= baselineChunk {
		return bl.Flush()
	}
	return nil
}

// SetProgress records an opaque loader resume cursor; it is committed in the
// same txn as the next Flush, so it can never run ahead of the flushed rows.
func (bl *Baseline) SetProgress(p []byte) {
	bl.progress = bytes.Clone(p)
}

// BaselineProgress returns the loader resume cursor, if one was recorded.
func (d *DB) BaselineProgress() ([]byte, bool, error) {
	var p []byte
	var ok bool
	err := d.env.View(func(txn *lmdb.Txn) error {
		v, err := txn.Get(d.dbi, schema.MetaBaselineProgress)
		if lmdb.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		p = bytes.Clone(v)
		ok = true
		return nil
	})
	return p, ok, err
}

// Flush commits the buffered rows (and any pending progress cursor) in one
// write txn.
func (bl *Baseline) Flush() error {
	if len(bl.keys) == 0 && bl.progress == nil {
		return nil
	}
	err := bl.d.env.Update(func(txn *lmdb.Txn) error {
		cur, err := txn.OpenCursor(bl.d.dbi)
		if err != nil {
			return err
		}
		defer cur.Close()
		for i := range bl.keys {
			if err := cur.Put(bl.keys[i], bl.vals[i], 0); err != nil {
				return err
			}
		}
		if bl.progress != nil {
			return txn.Put(bl.d.dbi, schema.MetaBaselineProgress, bl.progress, 0)
		}
		return nil
	})
	bl.keys = bl.keys[:0]
	bl.vals = bl.vals[:0]
	if err == nil {
		bl.progress = nil
	}
	return err
}

func (bl *Baseline) Account(addr schema.Address, a *schema.Account) error {
	return bl.add(schema.AppendAccountKey(nil, addr, bl.s), schema.EncodeAccount(nil, a))
}

func (bl *Baseline) Slot(addr schema.Address, slot schema.Hash, val schema.Hash) error {
	return bl.add(schema.AppendSlotKey(nil, addr, slot, bl.s), val[:])
}

func (bl *Baseline) Code(hash schema.Hash, code []byte) error {
	return bl.add(schema.AppendCodeKey(nil, hash), bytes.Clone(code))
}

// BaseAccount buffers one 0x07 hash-keyed baseline account row (D6 rev 2);
// the batched sibling of DB.PutBaseAccount for the bulk loader.
func (bl *Baseline) BaseAccount(addrHash schema.Hash, a *schema.Account) error {
	return bl.add(schema.AppendBaseAccountKey(nil, addrHash), schema.EncodeAccount(nil, a))
}

// BaseSlot buffers one 0x08 hash-keyed baseline slot row (D6 rev 2).
func (bl *Baseline) BaseSlot(addrHash, slotHash, val schema.Hash) error {
	return bl.add(schema.AppendBaseSlotKey(nil, addrHash, slotHash), val[:])
}

// --- hash-keyed baseline rows (D6 rev 2, 0x07/0x08) ---
//
// The snapshot baseline at S is keyed by keccak(addr)/keccak(slot) because no
// preimage-keyed full-state enumeration exists anywhere. These puts take the
// ALREADY-HASHED keys: the loader's source (state-sync artifact / node
// snapshot) only has hashed keys. The loader itself is a later phase; it must
// end by calling Finish on a Baseline (or setting MetaBaselineComplete) so
// reads stop failing loud.

// PutBaseAccount writes one 0x07 baseline account row under keccak(addr).
func (d *DB) PutBaseAccount(addrHash schema.Hash, a *schema.Account) error {
	return d.env.Update(func(txn *lmdb.Txn) error {
		return txn.Put(d.dbi, schema.AppendBaseAccountKey(nil, addrHash), schema.EncodeAccount(nil, a), 0)
	})
}

// PutBaseSlot writes one 0x08 baseline slot row under keccak(addr)||keccak(slot).
func (d *DB) PutBaseSlot(addrHash, slotHash schema.Hash, val schema.Hash) error {
	return d.env.Update(func(txn *lmdb.Txn) error {
		return txn.Put(d.dbi, schema.AppendBaseSlotKey(nil, addrHash, slotHash), val[:], 0)
	})
}

// Finish flushes, clears the loader progress cursor, and sets the
// baseline_complete watermark.
func (bl *Baseline) Finish() error {
	bl.progress = nil
	if err := bl.Flush(); err != nil {
		return err
	}
	err := bl.d.env.Update(func(txn *lmdb.Txn) error {
		if err := txn.Del(bl.d.dbi, schema.MetaBaselineProgress, nil); err != nil && !lmdb.IsNotFound(err) {
			return err
		}
		return txn.Put(bl.d.dbi, schema.MetaBaselineComplete, []byte{1}, 0)
	})
	if err != nil {
		return err
	}
	bl.d.baselineDone.Store(true)
	return nil
}

// --- mempool log (0x05) ---

// AppendMempool durably appends one arrival (irrecoverable data, D4).
// t is unix milliseconds. Seq numbers are dense.
func (d *DB) AppendMempool(t uint64, tx []byte) (seq uint64, err error) {
	d.mempoolMu.Lock()
	defer d.mempoolMu.Unlock()
	seq = d.nextSeq
	val := make([]byte, 8+len(tx))
	copy(val, beBytes(t))
	copy(val[8:], tx)
	err = d.env.Update(func(txn *lmdb.Txn) error {
		return txn.Put(d.dbi, schema.AppendMempoolKey(nil, seq), val, 0)
	})
	if err == nil {
		d.nextSeq++
	}
	return seq, err
}

// GetMempool reads one arrival by seq. ok=false past the end of the log.
func (d *DB) GetMempool(seq uint64) (t uint64, tx []byte, ok bool, err error) {
	err = d.env.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true
		v, err := txn.Get(d.dbi, schema.AppendMempoolKey(nil, seq))
		if lmdb.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if len(v) < 8 {
			return fmt.Errorf("store: mempool entry %d truncated", seq)
		}
		t = beU64(v[:8])
		tx = bytes.Clone(v[8:])
		ok = true
		return nil
	})
	return
}

// FirstMempoolAt returns the seq of the first arrival with time >= t
// (linear cursor scan; used once to position a replay session).
// ok=false when no such entry exists.
func (d *DB) FirstMempoolAt(t uint64) (seq uint64, ok bool, err error) {
	err = d.env.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true
		cur, err := txn.OpenCursor(d.dbi)
		if err != nil {
			return err
		}
		defer cur.Close()
		k, v, err := cur.Get([]byte{schema.PrefMempool}, nil, lmdb.SetRange)
		for ; err == nil; k, v, err = cur.Get(nil, nil, lmdb.Next) {
			if len(k) != schema.MempoolKeyLen || k[0] != schema.PrefMempool {
				break
			}
			if len(v) >= 8 && beU64(v[:8]) >= t {
				seq = beU64(k[1:])
				ok = true
				return nil
			}
		}
		if err != nil && !lmdb.IsNotFound(err) {
			return err
		}
		return nil
	})
	return
}

func lastMempoolSeq(txn *lmdb.Txn, dbi lmdb.DBI) (uint64, bool, error) {
	cur, err := txn.OpenCursor(dbi)
	if err != nil {
		return 0, false, err
	}
	defer cur.Close()
	// Position at the first key after the 0x05 range, then step back.
	k, _, err := cur.Get([]byte{schema.PrefMempool + 1}, nil, lmdb.SetRange)
	if lmdb.IsNotFound(err) {
		k, _, err = cur.Get(nil, nil, lmdb.Last)
	} else if err == nil {
		k, _, err = cur.Get(nil, nil, lmdb.Prev)
	}
	if lmdb.IsNotFound(err) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if len(k) != schema.MempoolKeyLen || k[0] != schema.PrefMempool {
		return 0, false, nil
	}
	return beU64(k[1:]), true, nil
}

func beBytes(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }

func beU64(b []byte) uint64 { return binary.BigEndian.Uint64(b) }
