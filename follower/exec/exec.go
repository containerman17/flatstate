// Package exec executes C-chain blocks with coreth's state transition as a
// library (DESIGN.md D2 rev 2, deforestationdb executor precedent) against
// our own statedb: reads resolve through pending unfinalized layers and then
// LMDB per the D6 rev 2 read order; commit-time writes are journaled by
// libevm and captured as a capture.Batch (post-images only, D5).
//
// Per-block validation: computed receiptsRoot, gasUsed, and logsBloom are
// compared against the header; any mismatch is a loud error and the caller
// must halt capture (D13). State roots are NOT computed (no trie); snowman
// acceptance plus the receipts check is the D2 rev 2 validation trade.
//
// EIP-6780 assumption (Cancun is active on mainnet C-chain since Etna): a
// SELFDESTRUCT only destroys accounts created in the same transaction, so a
// destructed account can never have pre-existing storage. libevm's
// destructed-storage enumeration is therefore never needed, which is what
// lets the capture "tries" report Root=EmptyRootHash everywhere. Following
// pre-Cancun history through this path would silently skip storage wipes of
// destructed accounts; do not use it for that.
package exec

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/ava-labs/avalanchego/genesis"
	"github.com/ava-labs/avalanchego/graft/coreth/consensus"
	"github.com/ava-labs/avalanchego/graft/coreth/consensus/dummy"
	corethcore "github.com/ava-labs/avalanchego/graft/coreth/core"
	"github.com/ava-labs/avalanchego/graft/coreth/core/extstate"
	cparams "github.com/ava-labs/avalanchego/graft/coreth/params"
	cextras "github.com/ava-labs/avalanchego/graft/coreth/params/extras"
	"github.com/ava-labs/avalanchego/graft/coreth/plugin/evm/atomic"
	ccustomtypes "github.com/ava-labs/avalanchego/graft/coreth/plugin/evm/customtypes"
	warpcontract "github.com/ava-labs/avalanchego/graft/coreth/precompile/contracts/warp"
	_ "github.com/ava-labs/avalanchego/graft/coreth/precompile/registry"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/avalanchego/upgrade"
	avaconstants "github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/state"
	ethtypes "github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/core/vm"
	"github.com/ava-labs/libevm/params"
	"github.com/ava-labs/libevm/trie"

	"github.com/containerman17/flatstate/capture"
	"github.com/containerman17/flatstate/follower/net"
	"github.com/containerman17/flatstate/schema"
	"github.com/containerman17/flatstate/store"
)

// MainnetAVAXAssetID is required by the atomic-tx state transfer to credit
// imported AVAX correctly.
const MainnetAVAXAssetID = "FvwEAhmxKfeiG8SnEvq42hc6whRyY3EFYAvebMqDNDGCgxN5Z"

// MainnetCChainID feeds the warp precompile's source chain ID; a wrong value
// would emit warp logs with a wrong payload and break the receipts check.
const MainnetCChainID = "2q9e4r6Mu3U68nU1fYjgbR6JvwrRx36CohpAX5UQxse55x1Q5"

// headerHistory is how many finalized headers stay resident for the
// BLOCKHASH 256-block window (with slack).
const headerHistory = 300

// Exec executes blocks and produces capture batches. Methods are
// concurrency-safe but the intended caller is the single consensus engine
// goroutine: Execute for every verified block, OnFinalized after every
// accepted block (after the store committed it, D7 order).
type Exec struct {
	db       *store.DB
	chainCfg *params.ChainConfig
	snowCtx  *snow.Context

	mu        sync.Mutex
	pending   map[schema.Hash]*layer // executed, above finalized, by eth hash
	dryBase   *layer                 // dry-run only: folded accepted diffs (store is read-only)
	finalized uint64

	// headers has its own lock: chainCtx.GetHeader is called back from
	// INSIDE the EVM (BLOCKHASH) while Execute holds mu; sharing mu was a
	// self-deadlock on the first BLOCKHASH-using transaction.
	hmu     sync.RWMutex
	headers map[common.Hash]*ethtypes.Header // parent chain for BLOCKHASH + upgrade timestamps
}

// New builds an executor over the store (the store must have its baseline /
// history; reads of uncovered keys fail loud per D13).
func New(db *store.DB) (*Exec, error) {
	net.RegisterExtras()
	cfg := genesis.GetConfig(avaconstants.MainnetID)
	var g corethcore.Genesis
	if err := json.Unmarshal([]byte(cfg.CChainGenesis), &g); err != nil {
		return nil, fmt.Errorf("exec: unmarshal C-chain genesis: %w", err)
	}
	// The genesis JSON carries no post-genesis upgrade schedule; the VM
	// injects it from the avalanchego runtime config before aligning the
	// eth upgrades (coreth parseGenesis). Without this, Durango/Etna never
	// activate, so Shanghai/Cancun stay off and PUSH0 is an invalid opcode:
	// every modern contract call burns its full gas limit.
	configExtra := cparams.GetExtra(g.Config)
	configExtra.NetworkUpgrades = cextras.GetNetworkUpgrades(upgrade.GetConfig(avaconstants.MainnetID))
	// Mirror parseGenesis: the Warp precompile activates at Durango; a tx
	// calling it would otherwise no-op into an empty address and diverge.
	if configExtra.DurangoBlockTimestamp != nil {
		configExtra.PrecompileUpgrades = append(configExtra.PrecompileUpgrades, cextras.PrecompileUpgrade{
			Config: warpcontract.NewDefaultConfig(configExtra.DurangoBlockTimestamp),
		})
	}
	if err := configExtra.Verify(); err != nil {
		return nil, fmt.Errorf("exec: invalid chain config: %w", err)
	}
	if err := cparams.SetEthUpgrades(g.Config); err != nil {
		return nil, fmt.Errorf("exec: set eth upgrades: %w", err)
	}
	avaxAssetID, err := ids.FromString(MainnetAVAXAssetID)
	if err != nil {
		return nil, err
	}
	cChainID, err := ids.FromString(MainnetCChainID)
	if err != nil {
		return nil, err
	}
	snowCtx := &snow.Context{
		NetworkID:   avaconstants.MainnetID,
		ChainID:     cChainID,
		AVAXAssetID: avaxAssetID,
	}
	// The warp precompile reads the snow context out of the chain config
	// extras (sendWarpMessage panics on nil, and the emitted message embeds
	// NetworkID and ChainID).
	configExtra.AvalancheContext = cextras.AvalancheContext{SnowCtx: snowCtx}
	return &Exec{
		db:       db,
		chainCfg: g.Config,
		snowCtx:  snowCtx,
		pending:  make(map[schema.Hash]*layer),
		headers:  make(map[common.Hash]*ethtypes.Header),
	}, nil
}

// SeedHeaders installs ancestor headers (the resume block and >=256 below
// it) so BLOCKHASH and upgrade-timestamp lookups work from the first block.
func (e *Exec) SeedHeaders(headers []*ethtypes.Header) {
	e.hmu.Lock()
	defer e.hmu.Unlock()
	for _, h := range headers {
		e.headers[h.Hash()] = h
	}
}

// OnFinalized prunes pending layers at or below the accepted height. Call
// AFTER the store committed the block (D7: LMDB runs ahead of views).
func (e *Exec) OnFinalized(height uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.prune(height)
}

// FoldFinalized is the dry-run replacement for OnFinalized: the store is
// read-only and never advances, so accepted diffs fold into an in-memory
// base consulted between the pending layers and the store. Never mix with
// OnFinalized in one process.
func (e *Exec) FoldFinalized(b *capture.Batch) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.dryBase == nil {
		e.dryBase = newLayer(&capture.Batch{})
	}
	e.dryBase.apply(b.Ops)
	e.prune(b.Block)
}

func (e *Exec) prune(height uint64) {
	e.finalized = height
	for h, l := range e.pending {
		if l.block <= height {
			delete(e.pending, h)
		}
	}
	if height > headerHistory {
		cut := height - headerHistory
		e.hmu.Lock()
		for h, hdr := range e.headers {
			if hdr.Number.Uint64() < cut {
				delete(e.headers, h)
			}
		}
		e.hmu.Unlock()
	}
}

// viewAt builds the read view for a block whose parent eth hash is parent:
// the chain of pending layers from parent down to the finalized boundary
// (a hash with no pending layer), then LMDB.
func (e *Exec) viewAt(parent schema.Hash) *view {
	var layers []*layer
	for h := parent; ; {
		l, ok := e.pending[h]
		if !ok {
			break
		}
		layers = append(layers, l)
		h = l.parent
	}
	if e.dryBase != nil {
		layers = append(layers, e.dryBase)
	}
	return &view{db: e.db, layers: layers}
}

// chainCtx serves coreth's ChainContext: Author via the dummy engine,
// GetHeader from the resident header map (BLOCKHASH window).
type chainCtx struct{ e *Exec }

func (c chainCtx) Engine() consensus.Engine { return dummy.NewFullFaker() }

func (c chainCtx) GetHeader(hash common.Hash, number uint64) *ethtypes.Header {
	c.e.hmu.RLock()
	defer c.e.hmu.RUnlock()
	h, ok := c.e.headers[hash]
	if !ok || h.Number.Uint64() != number {
		return nil
	}
	return h
}

// Execute runs one block against the state at its parent and returns the
// capture batch. It implements consensus.Executor. Errors are fatal to
// capture (D13): receipts mismatch, unresolvable state read, gas mismatch.
func (e *Exec) Execute(parent schema.Hash, blk *ethtypes.Block) (*capture.Batch, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	header := blk.Header()
	num := blk.NumberU64()
	ops, usedGas, receipts, err := e.run(parent, blk)
	if err != nil {
		return nil, err
	}

	// Per-block validation (D2 rev 2): fail loud, halt capture on mismatch.
	if usedGas != header.GasUsed {
		return nil, fmt.Errorf("exec: block %d gasUsed mismatch: computed %d, header %d", num, usedGas, header.GasUsed)
	}
	receiptsRoot := ethtypes.DeriveSha(receipts, trie.NewStackTrie(nil))
	if receiptsRoot != header.ReceiptHash {
		return nil, fmt.Errorf("exec: block %d receiptsRoot mismatch: computed %x, header %x", num, receiptsRoot, header.ReceiptHash)
	}
	if bloom := ethtypes.CreateBloom(receipts); bloom != header.Bloom {
		return nil, fmt.Errorf("exec: block %d logsBloom mismatch", num)
	}

	batch := &capture.Batch{
		Block:  num,
		Hash:   schema.Hash(blk.Hash()),
		Parent: parent,
		Time:   header.Time * 1000,
		Ops:    ops,
	}
	e.pending[batch.Hash] = newLayer(batch)
	e.hmu.Lock()
	e.headers[blk.Hash()] = header
	e.hmu.Unlock()
	return batch, nil
}

// run executes without validating or recording; caller holds e.mu.
func (e *Exec) run(parent schema.Hash, blk *ethtypes.Block) ([]capture.Op, uint64, ethtypes.Receipts, error) {
	header := blk.Header()
	num := blk.NumberU64()
	e.hmu.RLock()
	parentHeader, ok := e.headers[common.Hash(parent)]
	e.hmu.RUnlock()
	if !ok {
		return nil, 0, nil, fmt.Errorf("exec: parent header %x of block %d not seeded", parent[:4], num)
	}
	if parentHeader.Number.Uint64() != num-1 {
		return nil, 0, nil, fmt.Errorf("exec: parent height %d does not precede block %d", parentHeader.Number.Uint64(), num)
	}

	capDB := newCaptureDB(e.viewAt(parent))
	sdb, err := state.New(ethtypes.EmptyRootHash, capDB, nil)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("exec: open statedb: %w", err)
	}

	upgradeCtx := corethcore.NewBlockContext(header.Number, header.Time)
	if err := corethcore.ApplyUpgrades(e.chainCfg, &parentHeader.Time, upgradeCtx, sdb); err != nil {
		return nil, 0, nil, fmt.Errorf("exec: apply upgrades: %w", err)
	}

	blockCtx := corethcore.NewEVMBlockContext(header, chainCtx{e}, nil)
	gp := new(corethcore.GasPool).AddGas(header.GasLimit)
	var (
		usedGas  uint64
		receipts ethtypes.Receipts
	)
	for txIndex, tx := range blk.Transactions() {
		sdb.SetTxContext(tx.Hash(), txIndex)
		receipt, err := corethcore.ApplyTransaction(
			e.chainCfg, chainCtx{e}, blockCtx, gp, sdb,
			header, tx, &usedGas, vm.Config{},
		)
		if err != nil {
			return nil, 0, nil, fmt.Errorf("exec: block %d tx %d: %w", num, txIndex, err)
		}
		receipts = append(receipts, receipt)
	}

	// Atomic transactions (AVAX import/export) live in the block ExtData and
	// mutate EVM state without receipts.
	if extData := ccustomtypes.BlockExtData(blk); len(extData) > 0 {
		rules := e.chainCfg.Rules(header.Number, cparams.IsMergeTODO, header.Time)
		isAP5 := false
		if rulesExtra := cparams.GetRulesExtra(rules); rulesExtra != nil {
			isAP5 = rulesExtra.AvalancheRules.IsApricotPhase5
		}
		atomicTxs, err := atomic.ExtractAtomicTxs(extData, isAP5, atomic.Codec)
		if err != nil {
			return nil, 0, nil, fmt.Errorf("exec: block %d extract atomic txs: %w", num, err)
		}
		wrapped := extstate.New(sdb)
		for i, atx := range atomicTxs {
			if err := atx.UnsignedAtomicTx.EVMStateTransfer(e.snowCtx, wrapped); err != nil {
				return nil, 0, nil, fmt.Errorf("exec: block %d atomic tx %d: %w", num, i, err)
			}
		}
	}

	// Commit routes every post-image through the capture tries. The returned
	// root is meaningless (no trie); validation is receipts-based (D2 rev 2).
	if _, err := sdb.Commit(num, e.chainCfg.IsEIP158(header.Number)); err != nil {
		return nil, 0, nil, fmt.Errorf("exec: block %d commit: %w", num, err)
	}
	return capDB.ops, usedGas, receipts, nil
}
