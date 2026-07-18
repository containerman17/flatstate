// Package capture defines the per-block capture batch and the interface the
// embedded node implements in a later phase (DESIGN.md D3). Everything is
// post-image only: an op records the value at end of block, never the
// previous value (D5).
//
// Batch contract (the store's read logic depends on it):
//   - Ops describe the end-of-block state. An account created and destroyed
//     within the same block emits only OpDestruct.
//   - Destruct-then-recreate in one block emits OpDestruct first, then the
//     recreating OpAccount/OpSlot ops. Ops apply in order.
//   - OpDeleteSlot is a slot cleared to zero; the store writes a zero-value
//     tombstone row for it.
package capture

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/containerman17/flatstate/schema"
)

type OpKind byte

const (
	OpAccount    OpKind = 1 // set account post-image
	OpSlot       OpKind = 2 // set slot post-image
	OpDeleteSlot OpKind = 3 // slot cleared (zero tombstone)
	OpDestruct   OpKind = 4 // selfdestruct marker
	OpCode       OpKind = 5 // contract code by hash (deduped)
)

// Op is one state mutation. Only the fields for its Kind are meaningful.
type Op struct {
	Kind     OpKind
	Addr     schema.Address
	Slot     schema.Hash
	Value    schema.Hash // slot value (OpSlot)
	Account  schema.Account
	CodeHash schema.Hash
	Code     []byte
}

// Batch is one block's capture: written verbatim as the 0x04 diff row.
type Batch struct {
	Block  uint64
	Hash   schema.Hash
	Parent schema.Hash
	Time   uint64 // block timestamp, unix milliseconds
	Ops    []Op
}

// Encode appends the batch encoding to dst.
func (b *Batch) Encode(dst []byte) []byte {
	dst = binary.BigEndian.AppendUint64(dst, b.Block)
	dst = append(dst, b.Hash[:]...)
	dst = append(dst, b.Parent[:]...)
	dst = binary.BigEndian.AppendUint64(dst, b.Time)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(b.Ops)))
	for i := range b.Ops {
		op := &b.Ops[i]
		dst = append(dst, byte(op.Kind))
		switch op.Kind {
		case OpAccount:
			dst = append(dst, op.Addr[:]...)
			dst = schema.EncodeAccount(dst, &op.Account)
		case OpSlot:
			dst = append(dst, op.Addr[:]...)
			dst = append(dst, op.Slot[:]...)
			dst = append(dst, op.Value[:]...)
		case OpDeleteSlot:
			dst = append(dst, op.Addr[:]...)
			dst = append(dst, op.Slot[:]...)
		case OpDestruct:
			dst = append(dst, op.Addr[:]...)
		case OpCode:
			dst = append(dst, op.CodeHash[:]...)
			dst = binary.BigEndian.AppendUint32(dst, uint32(len(op.Code)))
			dst = append(dst, op.Code...)
		default:
			panic(fmt.Sprintf("capture: unknown op kind %d", op.Kind))
		}
	}
	return dst
}

// Decode parses a batch. The result does not alias data.
func Decode(data []byte) (*Batch, error) {
	r := reader{b: data}
	var b Batch
	b.Block = r.u64()
	r.bytes(b.Hash[:])
	r.bytes(b.Parent[:])
	b.Time = r.u64()
	n := r.u32()
	if r.err != nil {
		return nil, r.err
	}
	b.Ops = make([]Op, 0, n)
	for i := uint32(0); i < n; i++ {
		var op Op
		op.Kind = OpKind(r.u8())
		switch op.Kind {
		case OpAccount:
			r.bytes(op.Addr[:])
			var enc [schema.AccountSize]byte
			r.bytes(enc[:])
			if r.err == nil {
				op.Account, r.err = schema.DecodeAccount(enc[:])
			}
		case OpSlot:
			r.bytes(op.Addr[:])
			r.bytes(op.Slot[:])
			r.bytes(op.Value[:])
		case OpDeleteSlot:
			r.bytes(op.Addr[:])
			r.bytes(op.Slot[:])
		case OpDestruct:
			r.bytes(op.Addr[:])
		case OpCode:
			r.bytes(op.CodeHash[:])
			cl := r.u32()
			op.Code = make([]byte, cl)
			r.bytes(op.Code)
		default:
			if r.err == nil {
				r.err = fmt.Errorf("capture: unknown op kind %d", op.Kind)
			}
		}
		if r.err != nil {
			return nil, r.err
		}
		b.Ops = append(b.Ops, op)
	}
	if len(r.b) != 0 {
		return nil, fmt.Errorf("capture: %d trailing bytes", len(r.b))
	}
	return &b, nil
}

type reader struct {
	b   []byte
	err error
}

func (r *reader) take(n int) []byte {
	if r.err != nil {
		return nil
	}
	if len(r.b) < n {
		r.err = fmt.Errorf("capture: truncated batch")
		return nil
	}
	v := r.b[:n]
	r.b = r.b[n:]
	return v
}

func (r *reader) u8() byte {
	v := r.take(1)
	if v == nil {
		return 0
	}
	return v[0]
}

func (r *reader) u32() uint32 {
	v := r.take(4)
	if v == nil {
		return 0
	}
	return binary.BigEndian.Uint32(v)
}

func (r *reader) u64() uint64 {
	v := r.take(8)
	if v == nil {
		return 0
	}
	return binary.BigEndian.Uint64(v)
}

func (r *reader) bytes(dst []byte) {
	v := r.take(len(dst))
	if v != nil {
		copy(dst, v)
	}
}

// Sink consumes capture events. store.DB and mem.State cover the pieces; the
// node phase drives them in D7 order (LMDB txn, then base map, then watermark).
type Sink interface {
	// Block delivers a new unfinalized block on the preferred chain.
	Block(b *Batch) error
	// Finalize marks a previously delivered block accepted.
	Finalize(block uint64, hash schema.Hash) error
	// PreferenceReset replaces all unfinalized blocks with the new preferred
	// chain above the finalized height, oldest first.
	PreferenceReset(preferred []*Batch) error
}

// Source is implemented by the embedded-node phase; it exists only for that
// phase split. It feeds capture events into a Sink until ctx ends.
type Source interface {
	Run(ctx context.Context, sink Sink) error
}
