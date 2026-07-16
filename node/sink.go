// Package node is the writer-process glue (DESIGN.md D2, D7, D10): it turns
// the embedded avalanchego node's capture events into store/mem/tipbus calls
// in the D7 order. The avalanchego-facing side (the capture.Source) requires
// a small fork patch and lives in the fork; see docs/node-integration.md.
// Everything here is pure flatstate code, driven by the fork glue through
// Tracker (raw node events) or directly through Sink (capture.Sink).
package node

import (
	"fmt"

	"github.com/containerman17/flatstate/capture"
	"github.com/containerman17/flatstate/mem"
	"github.com/containerman17/flatstate/schema"
	"github.com/containerman17/flatstate/store"
	"github.com/containerman17/flatstate/tipbus"
)

// Sink is the composite capture.Sink over the writer process's three
// destinations. Per D7: unfinalized blocks touch only mem and the ephemeral
// bus; on finalize the main LMDB txn commits first, then the mem base folds
// the layer, then the finalized watermark bumps, so a concurrent reader miss
// can never pin a stale value.
//
// Not concurrent-safe; the caller (Tracker, or the fork glue) serializes.
type Sink struct {
	db  *store.DB
	st  *mem.State
	bus *tipbus.Bus

	// pending holds the published unfinalized batches by hash; Finalize
	// needs the batch to write its store rows.
	pending map[schema.Hash]*capture.Batch
}

var _ capture.Sink = (*Sink)(nil)

func NewSink(db *store.DB, st *mem.State, bus *tipbus.Bus) *Sink {
	return &Sink{db: db, st: st, bus: bus, pending: make(map[schema.Hash]*capture.Batch)}
}

// Block delivers a new unfinalized block on the preferred chain.
func (s *Sink) Block(b *capture.Batch) error {
	s.pending[b.Hash] = b
	s.st.ApplyBlock(b)
	return s.bus.PublishBlock(b)
}

// Finalize marks a previously delivered block accepted, in D7 order.
func (s *Sink) Finalize(block uint64, hash schema.Hash) error {
	b, ok := s.pending[hash]
	if !ok {
		return fmt.Errorf("node: finalize %d %x: block was never delivered", block, hash[:4])
	}
	if b.Block != block {
		return fmt.Errorf("node: finalize height %d does not match delivered height %d for %x", block, b.Block, hash[:4])
	}
	if err := s.db.WriteBlock(b); err != nil { // D7 step 1: main env txn
		return err
	}
	if err := s.st.Finalize(block, hash); err != nil { // step 2: base override
		return err
	}
	if err := s.db.SetFinalized(block); err != nil { // step 3: watermark
		return err
	}
	for h, p := range s.pending {
		if p.Block <= block {
			delete(s.pending, h)
		}
	}
	return s.bus.PublishFinalize(block, hash)
}

// PreferenceReset replaces the unfinalized stack, oldest first.
func (s *Sink) PreferenceReset(preferred []*capture.Batch) error {
	clear(s.pending)
	for _, b := range preferred {
		s.pending[b.Hash] = b
	}
	s.st.PreferenceReset(preferred)
	return s.bus.PublishReset(preferred)
}

// Mempool records one arrival durably (D4) and publishes it to the bus.
func (s *Sink) Mempool(tx []byte, time uint64) error {
	if _, err := s.db.AppendMempool(time, tx); err != nil {
		return err
	}
	return s.bus.PublishMempool(time, tx)
}

// WriteFinal is the bootstrap/backfill path: blocks executed during
// bootstrap are final on arrival and no reader is attached yet, so they skip
// mem and the bus entirely. Idempotent, same as WriteBlock (D7).
func WriteFinal(db *store.DB, b *capture.Batch) error {
	if err := db.WriteBlock(b); err != nil {
		return err
	}
	return db.SetFinalized(b.Block)
}
