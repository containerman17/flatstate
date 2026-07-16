# Baseline loader: source decision

Goal: enumerate full C-chain state at a known height S as (keccak(addr) -> account,
keccak(addr)||keccak(slot) -> value, codeHash -> code) to fill the 0x07/0x08 hash-keyed
baseline and 0x06 code rows (DESIGN.md D6 rev 2).

## Decision: read the geth snapshot layer of a state-synced avalanchego v1.14.2 node, offline

Run a stock avalanchego v1.14.2 node with the DEFAULT C-chain state scheme, let C-chain
state sync finish and the chain reach normal operation, stop the node cleanly, then open
its PebbleDB directly (node stopped, no lock conflict) and iterate the snapshot key ranges.

Facts this rests on (verified in the v1.14.2 source tree):

- The default C-chain `state-scheme` is `""`, which coreth treats as `rawdb.HashScheme`
  (`plugin/evm/vm.go`, `case rawdb.HashScheme, "":`). Firewood is opt-in in v1.14.2, not
  the default, so the geth snapshot layer is ACTIVE on a default node.
- The state-sync client itself writes the snapshot as it syncs: `writeAccountSnapshot` /
  `writeAccountStorageSnapshotFromTrie` in `graft/evm/sync/evmstate/` write every synced
  account and slot into the snapshot keyspace, and contract code is fetched per account
  (`codeQueue.AddCode`) and stored under the rawdb code prefix. No post-sync snapshot
  generation pass is needed.
- coreth flattens the accepted block's snapshot diff layer into the DISK layer on every
  Accept (`core/blockchain.go flattenSnapshot` -> `snapshot.Tree.Flatten`), and the
  flatten path maintains `rawdb.WriteSnapshotRoot`. So after a CLEAN shutdown the disk
  snapshot is a consistent full state at exactly one height: the last accepted block.
  S is recovered offline by matching `rawdb.ReadSnapshotRoot` against the header roots
  walking back from the acceptor tip; no match = fail loud, do not load.
- Snapshot key layout (libevm rawdb, iterated through the same wrappers the node uses):
  - `'a' || keccak(addr)` -> slim account RLP (`types.FullAccount` decodes; empty root /
    codehash fields mean empty-trie root / empty-code hash),
  - `'o' || keccak(addr) || keccak(slot)` -> RLP-encoded slot value,
  - `'c' || codeHash` -> code bytes.
- Physical keys in the node's PebbleDB are nested prefixes:
  `sha256(sha256(cChainID) || "vm") || sha256("ethdb") || <ethdb key>`
  (chains/manager.go: `prefixdb.New(chainID)` then `prefixdb.New("vm")`; coreth
  `vm_database.go`: `prefixdb.NewNested("ethdb")`). The loader does not hand-compute
  this: it opens the DB with avalanchego's own `pebbledb` + `prefixdb` packages and
  wraps with coreth's `database.New` + `rawdb.NewDatabase`, reproducing the node's
  exact stack.

## Options rejected

- **Iterate Firewood**: v1.14.2 defaults to hash scheme, so we would have to opt INTO
  firewood only to then need FFI revision iteration; strictly more code and a less
  proven path than reading the snapshot the default node already builds for us.
- **Legacy hashdb + snapshot via config**: this IS the default in v1.14.2; nothing to
  configure. (If a future version flips the default to firewood, `"state-scheme":
  "hash"` in the C-chain config restores this path.)
- **Capture state-sync leaves as they arrive**: requires forking/hooking the sync
  client; the exact same bytes land in the snapshot keyspace anyway, so capturing them
  in flight buys nothing except losing resumability.

## Loader shape

`cmd/baseline-load` (one process, node must be stopped):

1. Open node PebbleDB read-write is unnecessary; open via avalanchego pebbledb
   (it has no read-only mode issue for a stopped node), derive chaindb view.
2. Determine S: acceptor tip -> walk headers back until `header.Root ==
   ReadSnapshotRoot`, max 1024 steps, else abort.
3. `store.NewBaseline(S)`, then three ordered phases into batched LMDB txns
   (Baseline.flush-style chunks): accounts (0x07), slots (0x08), code (0x06).
4. Resumable: each flush also records a `meta:baseline_progress` cursor (phase +
   last source key); on restart the pebble iterator seeks past it and rewrites of
   the boundary chunk are idempotent.
5. `Finish()` sets `baseline_complete` last, after all three phases.
