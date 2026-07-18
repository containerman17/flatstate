// Package tipbus is the ephemeral LMDB environment (DESIGN.md D7, D10):
// unfinalized block diffs and preference resets published by the node,
// poll-consumed by any number of reader processes. NOSYNC, and
// truncated whenever the writer opens it, so a restart can never resume on a
// fork by construction.
package tipbus

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"github.com/PowerDNS/lmdb-go/lmdb"

	"github.com/containerman17/flatstate/capture"
	"github.com/containerman17/flatstate/schema"
)

// Keys: 0x00 = seq counter, 0x01 = handshake state, 0x02|seq(8) = event.
var (
	keySeq   = []byte{0x00}
	keyState = []byte{0x01}
)

const prefEvent byte = 0x02

type EventKind byte

const (
	EvBlock    EventKind = 1 // new unfinalized block on the preferred chain
	EvFinalize EventKind = 2 // block accepted
	EvReset    EventKind = 3 // preference reset: full new unfinalized stack
)

// Event is one published entry. Only the fields for its Kind are set.
type Event struct {
	Kind    EventKind
	Batch   *capture.Batch   // EvBlock
	Batches []*capture.Batch // EvReset, oldest first
	Height  uint64           // EvFinalize
	Hash    schema.Hash      // EvFinalize
}

const defaultMapSize = 8 << 30

// Bus is a handle on the ephemeral env; writer or reader depending on Open.
type Bus struct {
	env *lmdb.Env
	dbi lmdb.DBI

	// writer-only mirror of the handshake state
	seq       uint64
	finalized uint64
	layers    []*capture.Batch
}

// OpenWriter opens (creating and TRUNCATING) the bus for the single
// publisher. mapSize <= 0 uses an 8 GB sparse default.
func OpenWriter(path string, mapSize int64) (*Bus, error) {
	if mapSize <= 0 {
		mapSize = defaultMapSize
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, err
	}
	b, err := open(path, mapSize, lmdb.NoSync|lmdb.NoMetaSync)
	if err != nil {
		return nil, err
	}
	// Truncate: unfinalized data must never survive a restart (D7).
	err = b.env.Update(func(txn *lmdb.Txn) error {
		if err := txn.Drop(b.dbi, false); err != nil {
			return err
		}
		if err := txn.Put(b.dbi, keySeq, binary.BigEndian.AppendUint64(nil, 0), 0); err != nil {
			return err
		}
		return b.writeState(txn)
	})
	if err != nil {
		b.env.Close()
		return nil, err
	}
	return b, nil
}

// OpenReader opens the bus read-only. The writer must have opened it first.
func OpenReader(path string) (*Bus, error) {
	return open(path, 0, lmdb.NoSync|lmdb.NoMetaSync|lmdb.Readonly)
}

func open(path string, mapSize int64, flags uint) (*Bus, error) {
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
	b := &Bus{env: env}
	err = env.RunTxn(flags&lmdb.Readonly, func(txn *lmdb.Txn) error {
		dbi, err := txn.OpenRoot(0)
		b.dbi = dbi
		return err
	})
	if err != nil {
		env.Close()
		return nil, err
	}
	return b, nil
}

func (b *Bus) Close() error { return b.env.Close() }

// --- publisher ---

// writeState rewrites the handshake state key from the writer's mirror.
func (b *Bus) writeState(txn *lmdb.Txn) error {
	v := binary.BigEndian.AppendUint64(nil, b.finalized)
	v = binary.BigEndian.AppendUint32(v, uint32(len(b.layers)))
	for _, l := range b.layers {
		enc := l.Encode(nil)
		v = binary.BigEndian.AppendUint32(v, uint32(len(enc)))
		v = append(v, enc...)
	}
	return txn.Put(b.dbi, keyState, v, 0)
}

func (b *Bus) publish(payload []byte) error {
	return b.env.Update(func(txn *lmdb.Txn) error {
		key := append([]byte{prefEvent}, binary.BigEndian.AppendUint64(nil, b.seq)...)
		if err := txn.Put(b.dbi, key, payload, 0); err != nil {
			return err
		}
		if err := b.writeState(txn); err != nil {
			return err
		}
		b.seq++
		return txn.Put(b.dbi, keySeq, binary.BigEndian.AppendUint64(nil, b.seq), 0)
	})
}

// PublishBlock publishes a new unfinalized block diff.
func (b *Bus) PublishBlock(batch *capture.Batch) error {
	b.layers = append(b.layers, batch)
	return b.publish(batch.Encode([]byte{byte(EvBlock)}))
}

// PublishFinalize marks the oldest unfinalized block accepted.
func (b *Bus) PublishFinalize(height uint64, hash schema.Hash) error {
	if len(b.layers) == 0 || b.layers[0].Block != height {
		return fmt.Errorf("tipbus: finalize %d does not match oldest layer", height)
	}
	b.layers = b.layers[1:]
	b.finalized = height
	p := binary.BigEndian.AppendUint64([]byte{byte(EvFinalize)}, height)
	return b.publish(append(p, hash[:]...))
}

// PublishReset replaces the unfinalized stack (preference reset), oldest first.
func (b *Bus) PublishReset(preferred []*capture.Batch) error {
	b.layers = append(b.layers[:0], preferred...)
	p := binary.BigEndian.AppendUint32([]byte{byte(EvReset)}, uint32(len(preferred)))
	for _, l := range preferred {
		enc := l.Encode(nil)
		p = binary.BigEndian.AppendUint32(p, uint32(len(enc)))
		p = append(p, enc...)
	}
	return b.publish(p)
}

// --- subscriber ---

// Seq returns the current event count; poll it cheaply and call Poll when it
// grows.
func (b *Bus) Seq() (uint64, error) {
	var seq uint64
	err := b.env.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true
		v, err := txn.Get(b.dbi, keySeq)
		if lmdb.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		seq = binary.BigEndian.Uint64(v)
		return nil
	})
	return seq, err
}

// Handshake returns the current unfinalized layers above the finalized
// height plus the seq to start polling from, all from one snapshot.
func (b *Bus) Handshake() (finalized uint64, layers []*capture.Batch, seq uint64, err error) {
	err = b.env.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true
		sv, err := txn.Get(b.dbi, keySeq)
		if lmdb.IsNotFound(err) {
			return errors.New("tipbus: no writer has initialized this bus")
		}
		if err != nil {
			return err
		}
		seq = binary.BigEndian.Uint64(sv)
		v, err := txn.Get(b.dbi, keyState)
		if err != nil {
			return err
		}
		if len(v) < 12 {
			return errors.New("tipbus: truncated state")
		}
		finalized = binary.BigEndian.Uint64(v[:8])
		n := binary.BigEndian.Uint32(v[8:12])
		v = v[12:]
		for i := uint32(0); i < n; i++ {
			if len(v) < 4 {
				return errors.New("tipbus: truncated state")
			}
			l := binary.BigEndian.Uint32(v[:4])
			if len(v) < 4+int(l) {
				return errors.New("tipbus: truncated state")
			}
			batch, err := capture.Decode(v[4 : 4+l])
			if err != nil {
				return err
			}
			layers = append(layers, batch)
			v = v[4+l:]
		}
		return nil
	})
	return
}

// Poll returns events with seq in [from, current) and the new cursor.
func (b *Bus) Poll(from uint64) (events []Event, next uint64, err error) {
	next = from
	err = b.env.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true
		sv, err := txn.Get(b.dbi, keySeq)
		if lmdb.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		cur := binary.BigEndian.Uint64(sv)
		for ; next < cur; next++ {
			key := append([]byte{prefEvent}, binary.BigEndian.AppendUint64(nil, next)...)
			v, err := txn.Get(b.dbi, key)
			if err != nil {
				return err
			}
			ev, err := decodeEvent(v)
			if err != nil {
				return err
			}
			events = append(events, ev)
		}
		return nil
	})
	return
}

func decodeEvent(v []byte) (Event, error) {
	if len(v) == 0 {
		return Event{}, errors.New("tipbus: empty event")
	}
	ev := Event{Kind: EventKind(v[0])}
	body := v[1:]
	switch ev.Kind {
	case EvBlock:
		b, err := capture.Decode(body)
		if err != nil {
			return Event{}, err
		}
		ev.Batch = b
	case EvFinalize:
		if len(body) != 40 {
			return Event{}, errors.New("tipbus: bad finalize event")
		}
		ev.Height = binary.BigEndian.Uint64(body[:8])
		copy(ev.Hash[:], body[8:])
	case EvReset:
		if len(body) < 4 {
			return Event{}, errors.New("tipbus: bad reset event")
		}
		n := binary.BigEndian.Uint32(body[:4])
		body = body[4:]
		for i := uint32(0); i < n; i++ {
			if len(body) < 4 {
				return Event{}, errors.New("tipbus: bad reset event")
			}
			l := binary.BigEndian.Uint32(body[:4])
			if len(body) < 4+int(l) {
				return Event{}, errors.New("tipbus: bad reset event")
			}
			b, err := capture.Decode(body[4 : 4+l])
			if err != nil {
				return Event{}, err
			}
			ev.Batches = append(ev.Batches, b)
			body = body[4+l:]
		}
	default:
		return Event{}, fmt.Errorf("tipbus: unknown event kind %d", ev.Kind)
	}
	return ev, nil
}
