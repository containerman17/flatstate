# flatstate

Fast EVM state engine for the Avalanche C-Chain: tip simulation, replay, and historical test execution at ~200k EVM calls/sec on one box.

One writer process embeds avalanchego (real consensus), captures preimage-keyed post-image state history into LMDB, and publishes unfinalized tip diffs to an ephemeral env. Any number of reader processes (bots, replay, tests) open LMDB read-only and treat it as shared memory.

See [DESIGN.md](DESIGN.md) for the full set of design decisions and the reasoning behind them.

Packages (bottom up):

- `schema`: LMDB key layout (0x01-0x06 keyspaces, inverted-block suffix) and fixed binary encodings.
- `capture`: per-block capture batch (post-image ops) and the Source/Sink interfaces the embedded node wires into later.
- `store`: the main LMDB env. Per-block write txns, baseline bulk load at the sync pivot S, greatest-write-at-or-before-B reads, fail-loud error edges.
- `tipbus`: ephemeral NOSYNC LMDB env for unfinalized diffs and preference resets; poll a seq counter, no sockets.
- `mem`: lazily pinned base map, unfinalized layer stack, per-executor side buffers, batch-phase locking.
- `engine`: batch execution over an executor pool with tip-hash stamping and stale-batch requeue; the EVM plugs in via the Executor interface.
- `replay`: block-by-block history sessions.
- `node`: writer-process glue. Composite Sink (store -> mem -> watermark in D7 order, tipbus publish), Tracker (verified/head/accepted node events to Sink calls), baseline job over a StateIterator. The avalanchego-facing capture source needs a small fork patch; see [docs/node-integration.md](docs/node-integration.md).
- `suitecache`: per-(suite, block) flat-file cache for correctness test loops.

Status: core storage/memory/engine layers built and tested; node-side glue (Sink/Tracker/baseline) built and tested; the avalanchego capture hook itself requires the fork patch documented in docs/node-integration.md.
