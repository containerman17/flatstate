package node

import (
	"fmt"
	"sync"

	"github.com/containerman17/flatstate/capture"
	"github.com/containerman17/flatstate/schema"
)

// Tracker translates the embedded node's raw events (block verified, head
// moved, block accepted, mempool arrival) into capture.Sink calls. Coreth
// executes blocks at Verify time for every processing block, preferred or
// not; the preferred chain is only known from head events. Tracker keeps the
// verified-but-unfinalized batches in a side table and derives the published
// stack by walking parent hashes from the head down to the finalized block.
//
// Concurrent-safe: coreth feeds arrive on separate goroutines.
type Tracker struct {
	mu   sync.Mutex
	sink capture.Sink

	table map[schema.Hash]*capture.Batch // verified, above finalized
	stack []*capture.Batch               // published preferred chain, oldest first

	finalizedHeight uint64
	finalizedHash   schema.Hash
}

// NewTracker seeds the tracker with the last accepted block (coreth:
// BlockChain().LastConsensusAcceptedBlock()).
func NewTracker(sink capture.Sink, finalizedHeight uint64, finalizedHash schema.Hash) *Tracker {
	return &Tracker{
		sink:            sink,
		table:           make(map[schema.Hash]*capture.Batch),
		finalizedHeight: finalizedHeight,
		finalizedHash:   finalizedHash,
	}
}

// Verified records a block's capture batch when it is executed (coreth
// insertBlock / firewood proposal creation). It publishes nothing: the block
// may sit on a non-preferred fork.
func (t *Tracker) Verified(b *capture.Batch) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if b.Block <= t.finalizedHeight {
		return // stale re-verification below the watermark
	}
	t.table[b.Hash] = b
}

// chainTo walks parent hashes from hash down to the finalized block and
// returns the batches oldest first. Fail loud on any gap (D13).
func (t *Tracker) chainTo(hash schema.Hash) ([]*capture.Batch, error) {
	var rev []*capture.Batch
	for hash != t.finalizedHash {
		b, ok := t.table[hash]
		if !ok {
			return nil, fmt.Errorf("node: head chain references unknown block %x", hash[:4])
		}
		rev = append(rev, b)
		hash = b.Parent
	}
	for i, j := 0, len(rev)-1; i < j; i, j = i+1, j-1 {
		rev[i], rev[j] = rev[j], rev[i]
	}
	return rev, nil
}

// Head reacts to a preferred-tip change (coreth ChainHeadEvent). A one-block
// extension of the current stack publishes a single Block; anything else is
// a preference reset.
func (t *Tracker) Head(hash schema.Hash) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	chain, err := t.chainTo(hash)
	if err != nil {
		return err
	}
	if sameChain(chain, t.stack) {
		return nil
	}
	if len(chain) == len(t.stack)+1 && sameChain(chain[:len(t.stack)], t.stack) {
		t.stack = chain
		return t.sink.Block(chain[len(chain)-1])
	}
	t.stack = chain
	return t.sink.PreferenceReset(chain)
}

func sameChain(a, b []*capture.Batch) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Hash != b[i].Hash {
			return false
		}
	}
	return true
}

// Accepted finalizes a block. Accept order is chain order, so the block is
// normally the oldest published layer; if preference lagged (the accepted
// block was never published), the stack is reset to it first. The Sink's
// LMDB write completes before this returns, so calling it synchronously
// from the fork's TrieDB.Commit hook (before the Firewood commit) gives the
// "history durable before state commit" crash invariant of D7.
func (t *Tracker) Accepted(block uint64, hash schema.Hash) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.stack) == 0 || t.stack[0].Hash != hash {
		b, ok := t.table[hash]
		if !ok {
			return fmt.Errorf("node: accepted unknown block %d %x", block, hash[:4])
		}
		if b.Parent != t.finalizedHash {
			return fmt.Errorf("node: accepted block %d does not extend finalized %d", block, t.finalizedHeight)
		}
		// Stale or diverged preference: reset to just the accepted block;
		// the next head event rebuilds the rest of the stack.
		t.stack = []*capture.Batch{b}
		if err := t.sink.PreferenceReset(t.stack); err != nil {
			return err
		}
	}
	if err := t.sink.Finalize(block, hash); err != nil {
		return err
	}
	t.stack = t.stack[1:]
	t.finalizedHeight = block
	t.finalizedHash = hash
	for h, b := range t.table {
		if b.Block <= block {
			delete(t.table, h)
		}
	}
	return nil
}

// Mempool records one arrival (unix ms, stamped by the glue at receipt).
func (t *Tracker) Mempool(tx []byte, time uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.sink.Mempool(tx, time)
}
