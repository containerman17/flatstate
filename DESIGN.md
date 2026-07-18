# flatstate: design decisions

A fast EVM state engine for the Avalanche C-Chain: historical and tip state access for high-rate simulation, replay, and testing. Target: ~200,000 EVM executions per second on one large box (300 GB RAM), latency-optimized at the tip.

This document records the decisions AND the reasoning. Implementers: when in doubt, the reasoning wins over the letter.

## Problem

Stock avalanchego/geth state access is built for validation, not simulation:

- Every cold key goes to disk through PebbleDB; finding and decompressing an sstable block is heavy even when fully page-cached.
- String keys and per-access byte-slice allocations cost real time at high rates; GC scans pointer-heavy structures.
- Keccak hashing of storage slots (to place MPT leaves) measured at 30-40% of execution time. We never compute state roots ourselves, so this cost is structurally removable, not tunable.
- Go-to-Rust FFI (Firewood) per state access is a non-starter at 200k reads/sec.

Three workloads:

1. **Tip simulation** (latency-critical): simulate candidate and mempool transactions against the preferred tip with overlay/overwrite semantics.
2. **Replay**: re-run history block by block (plus recorded mempool arrivals) as fast as the loop turns.
3. **Correctness tests**: a fixed list of 10-20 blocks tortured repeatedly, ~70k calls per block, heavy key overlap.

## Decisions

### D1. Go, not Rust

Prior measured data (internal quoting engine, March-June 2026): the interpreter loop is ~98% of CPU; the state layer is <5%; Go holiman/uint256 benchmarked 1.2x FASTER than Rust ruint on hot V3 math. The endgame is bypassing the interpreter with closed-form pool formulas anyway, so interpreter speed is a temporary concern and state-layer language choice is not the battleground. Go also keeps us close to avalanchego for embedding.

### D2. Custom C-chain follower, not an embedded node (rev 2)

Rev 1 embedded the full avalanchego node as a library. Superseded: the capture hook point inside coreth/firewood is not exposed and would have required a fork. Instead we assemble a minimal C-chain follower from avalanchego packages, taking chunks as libraries and owning the rest:

- **P2P**: avalanchego's `network` stack with an ephemeral staking cert (proven recipe in deforestationdb `blockfetcher/`, ~200 LOC): `NewTestNetworkConfig`/`NewTestNetwork`, `ManuallyTrack` peers, custom `HandleInbound`, `message.Creator`, `PeerTracker`.
- **Validator set**: fetched and periodically refreshed from public P-chain RPC (`platform.getCurrentValidators` on api.avax.network); peer IPs via `info.peers`. The P-chain VM is never run.
- **Finality**: real snowman sampling against that weighted validator set (PullQuery/Chits polling). Gossip (`Put`) provides the preferred tip; our own polls decide acceptance. Decided over a depth/quorum heuristic: finality is verified locally, not trusted.
- **Execution**: coreth's state transition as a library (`NewEVMBlockContext` + `ApplyTransaction`, deforestationdb `executor/` precedent) against OUR statedb. No Firewood, no trie, no full node, no fork, no replace directives.
- **Mempool**: NOT from our p2p. A plain WebSocket client subscribed to external nodes (`newPendingTransactions`); arrivals are timestamped at receipt.

Validation trade (recorded honestly): rev 1 got state-root checking free from Firewood. Without a trie we cannot compute state roots. Per-block validation is now: snowman acceptance (network-verified finality) + computed receiptsRoot, gasUsed, logsBloom compared against the header, fail loud on mismatch. Silent state divergence with matching receipts is theoretically possible and accepted; a periodic cross-check of sampled accounts against a public archival RPC is the cheap watchdog if wanted.

### D3. Preimage keys everywhere

Capture is native: we own the statedb the follower executes against, so every mutation (put account / put slot / delete / destruct) is recorded with address and slot preimages as a side effect of our own commit code. Our entire store is keyed by `(address, slot)` preimages. No trie, no hashed keys, no keccak on any read path (one exception: baseline probes, see D6).

### D4. LMDB is the readers' single source of truth

One LMDB environment (via `github.com/PowerDNS/lmdb-go`) holds:

- full state snapshot baseline at height S (the state-sync pivot; see D6),
- post-image history rows for every key changed since S,
- per-block diff lists,
- mempool arrival log (irrecoverable data; capture it durably).

Why LMDB: single writer + N concurrent cross-process readers over a shared mmap, MVCC, readers lock-free and always see the latest committed txn, no compaction, no background threads, B+tree cursor `SetRange` implements our seek pattern natively. With tens of GB against 300 GB RAM the whole tree lives in page cache.

Rejected alternatives (verified 2026-07): Pebble takes an exclusive lock even in ReadOnly mode (single-process, cockroachdb/pebble#1583); bbolt's read-only shared flock still excludes a live writer; RocksDB secondary instances are pull-based (`TryCatchUpWithPrimary`) with catch-up corruption reports and a C++ build dependency; SQLite/WAL works but adds SQL machinery per lookup; hand-rolled mmap segments end up reimplementing the B+tree index.

LMDB knobs: preallocate a fat sparse map size (200 GB+), keep reader transactions short, use zero-copy reads (`RawRead`), writer uses one write txn per block.

### D5. Post-image encoding, everywhere; no pre-images

History rows store the value WRITTEN at block N. Reading key K at height B is one cursor seek for the greatest write at or before B (see D6 key layout). Pre-images have no remaining consumer:

- The in-memory base holds finalized values only, so nothing is ever unwound (a preference reset drops unfinalized overlay layers; it never undoes the base).
- The snapshot baseline (D6) kills the "no write at or before B" hole that mid-chain history would otherwise have.
- Accepted blocks are final on Avalanche; no disk unwind exists.

Capture therefore never needs the previous value: it emits `(key, newValue)` only.

Selfdestruct: a destruct marker row per (address, block). Slot reads at height B check "destructed after this slot's last write at or before B" and read zero. Post-EIP-6780 real destructs of aged accounts are near-impossible; if a historical read ever hits a destruct edge the store cannot answer, it returns a loud error, never a guess (see D13).

### D6. LMDB key layout

```
0x01 | addr(20)            | ^block(8)   -> account post-image (balance, nonce, codehash; fixed encoding)
0x02 | addr(20) | slot(32) | ^block(8)   -> slot value post-image (32B; tombstone for delete)
0x03 | addr(20)            | ^block(8)   -> destruct marker
0x04 | block(8)                          -> per-block diff (encoded key/value list, the capture batch verbatim)
0x05 | reserved (mempool moved out of scope, external JSONL capture)
0x06 | code hash(32)                     -> contract code (deduped)
0x07 | keccak(addr)(32)                  -> baseline account at S (hash-keyed, see below)
0x08 | keccak(addr)(32) | keccak(slot)(32) -> baseline slot at S (hash-keyed, see below)
meta: baseline_complete watermark, finalized height, history genesis S
```

`^block` = bitwise-inverted block number, so a forward `SetRange` seek on `key || ^B` lands on the greatest write at or before B in one hop (idea from PR 5624).

**Snapshot baseline (rev 2: hash-keyed)**: no full-state enumeration with preimage keys exists anywhere (firewood revisions, geth snapshots, and state-sync leaves are all keccak-keyed; preimages only exist in genesis JSON plus executed transactions). So the baseline at height S is stored under HASHED keys (a dedicated keyspace, `keccak(addr)` / `keccak(addr)||keccak(slot)`), sourced from a state-sync artifact or a synced node's snapshot at S. Read order: (1) preimage history rows, (2) on miss, one keccak to probe the hash-keyed baseline, (3) still nothing = the value is zero, pinned in memory as a known zero. Cost: one keccak per cold key EVER, off the steady-state path; the pin (and any later write) is preimage-keyed. The zero-pin matters: V3/V4 tick probes hammer nonexistent slots; each costs one keccak + one failed seek once, then it is a ~30ns map hit forever. Until the `baseline_complete` watermark is set, reads of not-yet-covered keys fail loud.

Reasoning for 0x04: replay must advance a session view block by block in O(diff); without per-block diff rows it would degenerate into per-key seeks (~200k/block). The diff row is the capture batch we already have in hand, so writing it is free.

### D7. Finalized-only persistence; unfinalized is ephemeral

The main LMDB env only ever contains data from ACCEPTED blocks. Unfinalized (processing/preferred) block diffs and mempool arrivals are published to a SEPARATE ephemeral LMDB env that is truncated at follower boot. Consequence: a restart can never resume on a fork, by construction; there is no poisoned-row detection problem because poison never persists.

Write ordering per accepted block: (1) main-env LMDB write txn commits, (2) in-memory base override, (3) finalized-height watermark bump. A concurrent reader miss during apply therefore cannot pin a stale value: LMDB runs ahead of the base map, never behind. Bootstrap replay after a crash rewrites identical rows (idempotent), same invariant as PR 5624 ("history durable before state commit").

### D8. In-memory model: two layers, no version history

- **Base map** = lazily pinned live values: `map[common.Address]*Account`, per-account `map[[32]byte][32]byte` storage, `uint256.Int` balances inline, pointer-free leaves the GC never scans. Presence encodes knowledge (a fetched zero is stored as zero), so absence = "never asked" = fetch from LMDB and pin. No eviction ever; restart to shrink.
- **Unfinalized stack** = 2-3 tiny immutable per-block diff layers above the base (1s blocks, 2-3s finality). Preference reset (roughly hourly, rolls back 2-3 blocks) = drop layers, rebuild from the new preferred blocks' captures; the base is untouched.

On finalization, the block diff is applied in place to the base, but ONLY for keys already present: an absent key needs no update because its next miss reads LMDB, which is already current (D7 ordering). Apply cost is O(diff ∩ cached).

There is deliberately NO in-memory version trail. "Value at height B" exists only as an LMDB seek (~1-2 microseconds page-cache-warm). We tried the design with an MVCC trail and deleted it: its only consumer was a workload LMDB already serves.

Read path for a simulation: per-call overlay -> unfinalized layers -> base map -> LMDB pin-on-miss.

### D9. Batch-phase concurrency: mutex-less reads

Execution is batched: batches of roughly cores x 10 calls. One `sync.RWMutex` taken ONCE PER BATCH (RLock), not per read and not per sim. Within a batch, all shared structures are read as plain unsynchronized Go maps, which is safe under the zero-writers guarantee. Go's RWMutex writer-pending semantics naturally alternate phases: in-flight batches drain (single-digit ms, bounded by batch size, no timers), the block diff lands under Lock, batches resume. ~300 lock operations/sec total.

The landmine this design must respect: a cold miss during a read phase MUST NOT insert into the shared base map. Misses read LMDB directly and record the pin into a per-executor side buffer; the next write phase merges the buffers. The side buffer doubles as a within-batch cache (check it before seeking). First batch after a cold start mostly hits LMDB; that is accepted (a miss is ~1-2 microseconds against ~400 microseconds of interpreter per call; batch 2 is warm). No warmup machinery. If first-touch ever dominates a replay profile, the upgrade path is prefetch-from-next-block's-0x04-diff, not warmup batches.

Staleness: every batch is stamped with the preferred-tip hash at RLock time; results whose stamp no longer matches the current tip are discarded and requeued. The write phase never coordinates with readers.

### D10. Process topology: one follower, N equal readers

- **Follower process** (the only writer): the D2 rev 2 follower (p2p + snowman sampling + coreth-as-library execution) + capture + main LMDB env writer + ephemeral tip env publisher.
- **Bot processes** (any number, all equal): open main env read-only for state, poll the ephemeral env for tip diffs and preference resets. Polling = reading a `seq` counter key (~100ns mmap read); sleep-polling gives ~0.5-1ms tip latency, busy-poll on a pinned goroutine gives 10-100 microseconds. Publish cost: one NOSYNC txn per event (~50-100 microseconds for a block diff), paid once regardless of reader count.
- **Test/replay processes**: main env read-only; they do not need the tip env.

Reasoning: bots redeploy dozens of times a day during development; the follower must not restart with them. LMDB has no watch/subscribe; polling a counter at this cost is simpler than a socket protocol and was chosen over one deliberately. There is NO HTTP/RPC state serving anywhere: readers treat LMDB as a shared read-only memory segment.

### D11. Test-suite snapshot cache

Per (suite, block): one flat file (fixed-size records, optionally zstd). Load into a plain `map`, misses fall through to the main LMDB env and are added, `Close()` rewrites the file if anything was added. Run 2 hits ~97% from the file. A block's state at height B never changes, so there is no invalidation problem. This also makes test processes nearly independent of the live store.

### D12. Replay is live minus the network

A replay session = seed a mutable session map lazily (LMDB greatest-at-or-before-B seeks), then advance block by block applying 0x04 diff rows. Same apply code as live, same miss path, every block "finalized". A replay process tailing a live writer sees each block as it commits (LMDB MVCC), so replay-on-live-data needs nothing special.

### D13. Fail loud, never guess

Below history genesis S: error. Baseline not yet complete for a key: error. Destruct edge the store cannot answer: error. Merkle proofs / trie iteration: unsupported, error. A wrong-height answer served silently is the worst failure mode this system can have; every ambiguous read errors instead. (Philosophy inherited from PR 5624.)

### D14. Executor pool mechanics (from measured prior work)

- Long-lived executors in a buffered channel (natural backpressure); size to cores, never to workload. Reset with `clear()`, never reallocate (fresh overlay per call was a measured 2.4x regression).
- Per-call overlay stores ONLY storage and balance overrides; code/nonce/codehash delegate to the base (measured 1.58x).
- Journal-based snapshot/revert: undo entries, O(mutations), never O(state).
- keccak(code) computed once at ingest (EXTCODEHASH re-hashing was a measured 14% of CPU); JUMPDEST analysis shared across calls via a persistent contract object (~15% CPU); block context + chain rules cached per block, NOT in an unsynchronized global.
- All writes, including balance changes, go through the journaled overlay (a prior implementation wrote balances into a side layer, which breaks stacked tx-sequence simulation).

### D15. Known non-goals

- No eviction anywhere (300 GB RAM vs ~100 GB ceiling; restart to shrink).
- No bloom filters over the unfinalized stack (depth 2-3).
- No flat 52-byte-key unified map (measured: 4.3x faster in microbenchmarks, zero end-to-end, and it breaks per-call account-pointer amortization).
- No sync.Map (write churn promotes dirty maps; plain maps under the batch-phase discipline win).
- No custom replay protocol, no HTTP, no gRPC.
- No merkle anything in this layer.
- Mempool capture: out of scope, external JSONL files.

## Performance calibration (for sanity checks)

- One EVM call: ~400 microseconds interpreter floor (libevm); this dominates everything.
- LMDB point read, page-cache-warm: ~1-2 microseconds.
- Base-map hit: ~50-100ns (two map probes, one after per-call account-pointer caching).
- 70k calls x 10 SLOADs = 700k reads is ~70ms of state access total; interpreter cost is ~28 CPU-seconds. The state layer must merely stay out of the way; the throughput win comes from parallel executors now and pool formulas later.

## Roadmap context

Phase 1 (this repo): kill disk/state access as a cost. Phase 2: replace hot pool contracts with closed-form formulas that read state directly (prior measured 41x on a V3 pool). Phase 3: profile-guided elimination of remaining SHA3-opcode hashing (memoize hot mapping-slot derivations). This repo should not pre-build for phases 2-3, but must not preclude them: formulas will read the same base map through the same batch discipline.
