// Package engine is the batch-phase execution engine (DESIGN.md D9, D14).
// One read lock per batch, plain-map reads inside, per-executor side buffers
// for cold pins, tip-hash stamping with stale-batch requeue. The EVM itself
// plugs in later through the Executor interface.
package engine

import (
	"runtime"
	"sync"

	"github.com/holiman/uint256"

	"github.com/containerman17/flatstate/mem"
	"github.com/containerman17/flatstate/schema"
)

// Executor runs one call against a View. Implementations are long-lived and
// reused (D14); libevm plugs in here in a later phase. The View is valid
// only for the duration of the call.
type Executor interface {
	Execute(call any, v *View) any
}

// View is the per-call state view: overlay -> layers -> base -> LMDB. The
// overlay stores only storage and balance overrides; code, nonce and
// codehash delegate to the shared state (D14). Reset between calls with
// clear(), never reallocated.
type View struct {
	st       *mem.State
	sb       *mem.SideBuffer
	balances map[schema.Address]uint256.Int
	slots    map[schema.SKey]schema.Hash
}

func newView(st *mem.State, sb *mem.SideBuffer) *View {
	return &View{
		st:       st,
		sb:       sb,
		balances: make(map[schema.Address]uint256.Int),
		slots:    make(map[schema.SKey]schema.Hash),
	}
}

func (v *View) reset() {
	clear(v.balances)
	clear(v.slots)
}

// Balance returns the overlay balance if overridden, else the shared state's.
func (v *View) Balance(addr schema.Address) (uint256.Int, error) {
	if b, ok := v.balances[addr]; ok {
		return b, nil
	}
	a, _, err := v.st.Account(addr, v.sb)
	return a.Balance, err
}

func (v *View) SetBalance(addr schema.Address, b uint256.Int) {
	v.balances[addr] = b
}

// Slot returns the overlay slot if overridden, else the shared state's.
func (v *View) Slot(addr schema.Address, slot schema.Hash) (schema.Hash, error) {
	sk := schema.SKey{Addr: addr, Slot: slot}
	if val, ok := v.slots[sk]; ok {
		return val, nil
	}
	return v.st.Slot(addr, slot, v.sb)
}

func (v *View) SetSlot(addr schema.Address, slot, val schema.Hash) {
	v.slots[schema.SKey{Addr: addr, Slot: slot}] = val
}

// Account returns the shared account (nonce/codehash reads delegate here).
func (v *View) Account(addr schema.Address) (schema.Account, bool, error) {
	return v.st.Account(addr, v.sb)
}

func (v *View) Code(hash schema.Hash) ([]byte, error) {
	return v.st.Code(hash, v.sb)
}

type worker struct {
	ex   Executor
	view *View
}

// Engine fans batches of calls over a fixed executor pool.
type Engine struct {
	st   *mem.State
	pool chan *worker
}

// New builds an engine over st with the given executors (one pool slot
// each). Pass runtime.NumCPU() executors for the designed sizing; size to
// cores, never to workload (D14).
func New(st *mem.State, execs []Executor) *Engine {
	e := &Engine{st: st, pool: make(chan *worker, len(execs))}
	for _, ex := range execs {
		sb := mem.NewSideBuffer()
		st.Register(sb)
		e.pool <- &worker{ex: ex, view: newView(st, sb)}
	}
	return e
}

// PoolSize is the designed executor count.
func PoolSize() int { return runtime.NumCPU() }

// Execute runs one batch (roughly cores x 10 calls). The batch is stamped
// with the preferred-tip hash at read-lock time; if the tip moved by the
// time it finishes, the results are stale and the whole batch is requeued
// against the new tip.
func (e *Engine) Execute(calls []any) []any {
	for {
		results, stamp := e.run(calls)
		if e.st.TipHash() == stamp {
			return results
		}
	}
}

// run executes one attempt under a single read phase.
func (e *Engine) run(calls []any) ([]any, schema.Hash) {
	stamp := e.st.BeginBatch()
	defer e.st.EndBatch()
	results := make([]any, len(calls))
	var wg sync.WaitGroup
	for i, c := range calls {
		w := <-e.pool
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.view.reset()
			results[i] = w.ex.Execute(c, w.view)
			e.pool <- w
		}()
	}
	wg.Wait()
	return results, stamp
}
