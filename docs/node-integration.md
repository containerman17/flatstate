# Node integration: capture hook, embedding, and the required fork patch

Investigation against avalanchego monorepo tag `v1.14.2` (modules
`github.com/ava-labs/avalanchego`, `.../graft/coreth`, `.../graft/evm`; the
graft tags `graft/coreth/v1.14.2` and `graft/evm/v1.14.2` exist, so upstream
is consumable as a plain Go dependency, no replace directives). PR 5624 is
branch `state-history-firewood` (commit `1f6d585955`).

## 1. Hook point: firewood `baseTrie`, preimage side (fork patch required)

Preimage keys (D3) last exist at the `state.Trie` implementation layer.
`StateDB.Commit` calls `UpdateAccount(addr, account)`,
`UpdateStorage(addr, slotPreimage, value)`, `DeleteAccount(addr)`,
`DeleteStorage(addr, slotPreimage)` with preimages as arguments; the firewood
adapter `graft/evm/firewood/base_trie.go` receives them and keccaks them
itself. PR 5624 mirrors the ops AFTER hashing (`ffi.BatchOp` side); we mirror
one line earlier, BEFORE hashing, in the same functions.

Every no-fork route was checked and eliminated:

- `libevm` `state.RegisterExtras` (`core/state/statedb.libevm.go`): only
  `TransformStateKey`, no commit mirror, and coreth already occupies the
  at-most-once slot (`graft/coreth/core/extstate/statedb.go:29`).
- `triedb.Config.DBOverride` (libevm injectable backend): set internally by
  `graft/coreth/core/blockchain.go:233` when `state-scheme=firewood`; the
  chain of construction (`plugin/evm/vm.go` -> `eth.New` ->
  `core.NewBlockChain`) is driven entirely by JSON config, no func fields to
  inject a wrapping backend.
- `extension.Config` (`plugin/evm/extension/config.go`): reaches consensus
  callbacks and block lifecycle but not the state layer.
  `OnExtraStateChange` hands over the `*state.StateDB`, but libevm's StateDB
  exposes no dirty-set enumeration.
- `core.BlockChain` subscriptions: `ChainEvent`/`ChainHeadEvent`/
  `ChainAcceptedEvent` carry blocks and logs, never state diffs; the
  snapshot layer (`SnapshotTree.Update`) is hashed-key and disabled under
  firewood (`SnapshotLimit = 0`, blockchain.go:268).
- Re-executing accepted blocks against our own capturing `state.Database`
  was rejected: 2x interpreter cost and firewood proposal side effects from
  `accountTrie.Hash()`.

## 2. Exact fork patch

All files are in one repo (monorepo), one branch. Two touch points.

### 2a. `graft/evm/firewood`: preimage capture (~70 lines)

New file `graft/evm/firewood/capture.go`:

```go
package firewood

import "github.com/ava-labs/libevm/common"

type CaptureOpKind byte

const (
    CaptureAccount CaptureOpKind = iota + 1 // Value = RLP(types.StateAccount)
    CaptureSlot                             // Value = raw slot bytes, trimmed; empty means delete
    CaptureDeleteSlot
    CaptureDestruct
)

type CaptureOp struct {
    Kind  CaptureOpKind
    Addr  common.Address
    Slot  common.Hash // preimage, only CaptureSlot/CaptureDeleteSlot
    Value []byte
}

// CaptureSink receives per-block preimage state diffs. Both methods are
// called synchronously under the TrieDB locks; implementations must be fast
// and must not call back into the TrieDB.
type CaptureSink interface {
    // BlockVerified fires from TrieDB.Update, i.e. at insertBlock time for
    // every processing block (preferred or not), with the block identity
    // from the stateconf payload.
    BlockVerified(height uint64, blockHash, parentBlockHash, root common.Hash, ops []CaptureOp)
    // BlockCommitted fires from TrieDB.Commit BEFORE the Firewood proposal
    // commit, once per accepted block in accept order. Returning an error
    // fails the commit (history durable before state commit, D7).
    BlockCommitted(height uint64, root common.Hash, ops []CaptureOp) error
}

var captureSink CaptureSink

// SetCaptureSink must be called before the TrieDB is constructed.
func SetCaptureSink(s CaptureSink) { captureSink = s }
```

`base_trie.go` (mirror PR 5624's diff shape, preimage side; `baseTrie` gains
`captureOps []CaptureOp` and the `copy()` method clones it exactly like the
PR clones `historyOps`):

- `UpdateAccount(addr, account)`: append `{CaptureAccount, addr, nil, data}`
  (data is the already-computed RLP).
- `UpdateStorage(addr, key, value)`: append
  `{CaptureSlot, addr, common.BytesToHash(key), value}` (note: `key` is the
  32-byte slot preimage, `value` the trimmed big-endian bytes as passed by
  statedb, pre-RLP).
- `DeleteAccount(addr)`: append `{CaptureDestruct, addr}`.
- `DeleteStorage(addr, key)`: append `{CaptureDeleteSlot, addr, key}`.
- `accountTrie.hash()` passes `captureOps` into `createProposals` alongside
  `updateOps` (same plumbing as PR 5624's `historyOps`).

`triedb.go`:

- `proposal` gains `captureOps []CaptureOp` (parked, like PR 5624).
- `TrieDB.Update(root, parent, height, ...)` already extracts
  `(parentBlockHash, blockHash)` via `stateconf.ExtractTrieDBUpdatePayload`
  (triedb.go:308); after the proposal is resolved, call
  `captureSink.BlockVerified(height, blockHash, parentBlockHash, root, p.captureOps)`.
- `TrieDB.Commit(root, ...)`: immediately before `p.handle.Commit()` (the
  exact spot PR 5624 flushes its history store), call
  `captureSink.BlockCommitted(p.height, root, p.captureOps)` and abort the
  commit on error.

Ordering guarantee inherited from PR 5624's analysis: Commit is per accepted
block, in accept order, and the LMDB write completing before the Firewood
commit means a crash can never leave state ahead of history.

### 2b. `graft/coreth/plugin/evm/vm.go`: VM handle callback (~5 lines)

The embedded node needs the live `*eth.Ethereum` for the mempool feed and
block feeds. The C-chain factory chain is hardcoded
(`node/node.go:1230`, `transitionvm.Factory{PreFactory: &coreth.Factory{}}`),
so the smallest exposure is a package-level callback at the end of
`(*VM).Initialize` (after `vm.eth`/`vm.txPool`/`vm.blockChain` are set,
vm.go:547-568):

```go
// vm.go, package evm
var OnInitialized func(vm *VM)
...
if OnInitialized != nil { OnInitialized(vm) }
```

`evm.VM` already exports everything needed from there: `Ethereum()`,
`Config()`, `ChainConfig()` (vm_extensible.go implements
`extension.ExtensibleVM`).

Nothing else is patched. `node/node.go`, libevm, and coreth's blockchain are
untouched.

## 3. Event glue (fork side, thin; all logic already in `flatstate/node`)

The fork's `cmd/flatstate-node/main.go` embeds the node exactly like
avalanchego's own `main/main.go`:
`evm.RegisterAllLibEVMExtras()`, `config.BuildFlagSet()` ->
`config.BuildViper(fs, args)` -> `config.GetNodeConfig(v)` ->
`app.New(nodeConfig)` -> `app.Run(app)`. C-chain config must set
`state-scheme: firewood` (capture hooks live in the firewood adapter;
snapshot is unavailable there anyway). This satisfies D2: one process, real
consensus, no RPC ingestion.

Glue mapping onto `flatstate/node.Tracker` (which drives `node.Sink` in D7
order):

| node event | source | Tracker call |
|---|---|---|
| block executed | `CaptureSink.BlockVerified` joined with the block header (height+blockHash identify it; `capture.Batch{Block, Hash, Parent, Time}` from the header, ops translated) | `Verified(batch)` |
| preferred tip moved | `BlockChain().SubscribeChainHeadEvent` (fires from `setPreference`, blockchain.go:1067, and on insert-as-head) | `Head(hash)` |
| block accepted | `CaptureSink.BlockCommitted` (synchronous, pre-Firewood-commit; do NOT use the async `SubscribeChainAcceptedEvent` acceptor queue for this, it breaks the crash invariant) | `Accepted(height, hash)`; the error propagates and fails the Firewood commit |
| mempool arrival | `Ethereum().TxPool().SubscribeTransactions(ch, false)`, stamp unix ms at receipt | `Mempool(txBytes, t)` |

Translation notes:

- `CaptureAccount` RLP decodes to `types.StateAccount`; map to
  `schema.Account{Balance, Nonce, CodeHash}`.
- Contract code never passes through the trie (statedb writes it to the
  chain database via `rawdb.WriteCode`). Emit `capture.OpCode` by reading
  `rawdb.ReadCode(chainDb, codeHash)` for any account op whose code hash is
  new; `Ethereum().ChainDb()` is exported.
- `CaptureSlot` with empty value and `CaptureDeleteSlot` both map to
  `capture.OpDeleteSlot`; non-empty values left-pad to 32 bytes.
- Destruct-then-recreate order is preserved by construction: ops are
  appended in statedb commit order, destruct first (capture batch contract).
- During bootstrap (before consensus reaches normal op), skip
  Tracker/tipbus entirely and call `node.WriteFinal(db, batch)` from
  `BlockCommitted`; the tipbus would otherwise accumulate one event per
  replayed block. Construct the Tracker at normal-op with
  `BlockChain().LastConsensusAcceptedBlock()` as the seed.

## 4. Baseline at the pivot S: open decision

No local artifact can enumerate the full state with PREIMAGE keys:

- Firewood iterates hashed keys only (`ffi.Database.Revision(root)` +
  `ffi.Iterator`, batched; keys are `keccak(addr)` /
  `keccak(addr)||keccak(slot)`).
- The geth snapshot is hashed-key too, and disabled under firewood.
- State-sync leaf requests transfer hashed keys.
- The preimage table only ever contains keys touched by locally executed
  transactions.

Address/slot preimages are only observable from executed transactions plus
the genesis allocation. Options, for the user to pick:

1. **Sync from genesis with capture enabled** (PR 5624's stance). S = 0; the
   genesis alloc (preimage-keyed JSON) seeds the baseline via
   `node.RunBaseline`, and capture covers every later key. Cost: one full
   C-chain re-execution bootstrap (days to weeks); PR 5624 measured ~1.2x
   over plain bootstrap for the capture overhead itself. No design change.
2. **Hash-keyed baseline fallback** (design deviation from D3, baseline rows
   only). Keep state sync; iterate the pinned Firewood revision at S into a
   separate hashed-key keyspace; a read that misses preimage history and has
   `baseline_complete` set does one keccak (two for slots) and consults the
   hashed baseline; the result is pinned, so the cost is one keccak per cold
   key ever. Post-S writes are preimage-keyed as normal. Violates the letter
   of "no keccak on any read path" but not its hot-path intent.
3. External preimage dataset: none exists for the C-chain; building one
   requires option 1's replay anyway.

`node.RunBaseline` and `node.StateIterator` are written against preimage
keys (option 1's genesis-alloc iterator implements it directly; option 2
would bypass it with a store-level addition).

## 5. Fork mechanics (do not execute without approval)

Fork `ava-labs/avalanchego` to `github.com/containerman17/avalanchego`,
branch `containerman17/flatstate-capture`. Two ways to consume it without a
`replace` directive:

- **A. Reverse dependency (recommended).** Keep upstream module paths
  untouched in the fork; add `cmd/flatstate-node/` there, importing
  `github.com/containerman17/flatstate` as a normal tagged/pseudo-versioned
  dependency. flatstate stays avalanchego-free (as it is now), the fork
  builds the binary in-tree where its own `go.work`/replaces are native. No
  module renames, trivial upstream rebases.
- **B. Module rename fork.** Rewrite module paths (`go.mod` files plus every
  internal import) to `github.com/containerman17/avalanchego/...` so
  flatstate can `require` the fork path directly. Mechanical but huge diff,
  painful rebases. Only worth it if flatstate itself must import node types.

The Helicon upgrade replaces coreth with `vms/saevm` on the C-chain
(`transitionvm.Factory`, node.go:1230; unscheduled as of v1.14.2). saevm has
a first-class user-injected hook interface (`vms/saevm/hook.Points`) and its
own firewood layer; the capture patch must be redone for saevm before
Helicon activates on mainnet. Revisit then.

## 6. Status

Implemented in this repo (no avalanchego dependency, unit-tested with a real
LMDB store/bus): `node.Sink` (composite capture.Sink, D7 ordering),
`node.Tracker` (verified/head/accepted/mempool to Sink translation,
fork-switch resets, accept-before-head handling), `node.WriteFinal`
(bootstrap path), `node.RunBaseline` + `node.StateIterator` (ordering
validated, fail loud). Remaining: the fork patch above, the glue cmd in the
fork, and the baseline decision.
