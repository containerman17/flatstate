# flatstate

Fast EVM state engine for the Avalanche C-Chain: tip simulation, replay, and historical test execution at ~200k EVM calls/sec on one box.

One writer process embeds avalanchego (real consensus), captures preimage-keyed post-image state history into LMDB, and publishes unfinalized tip diffs to an ephemeral env. Any number of reader processes (bots, replay, tests) open LMDB read-only and treat it as shared memory.

See [DESIGN.md](DESIGN.md) for the full set of design decisions and the reasoning behind them.

Packages (bottom up):

- `schema`: LMDB key layout (0x01-0x06 keyspaces, inverted-block suffix) and fixed binary encodings.
- `capture`: per-block capture batch (post-image ops) and the Source/Sink interfaces the embedded node wires into later.
- `store`: the main LMDB env. Per-block write txns, baseline bulk load at the sync pivot S, mempool log, greatest-write-at-or-before-B reads, fail-loud error edges.
- `tipbus`: ephemeral NOSYNC LMDB env for unfinalized diffs, preference resets, and mempool arrivals; poll a seq counter, no sockets.
- `mem`: lazily pinned base map, unfinalized layer stack, per-executor side buffers, batch-phase locking.
- `engine`: batch execution over an executor pool with tip-hash stamping and stale-batch requeue; the EVM plugs in via the Executor interface.
- `replay`: block-by-block history sessions interleaved with the mempool log by timestamp.
- `suitecache`: per-(suite, block) flat-file cache for correctness test loops.

Status: core storage/memory/engine layers built and tested; node integration (avalanchego capture) not wired yet.
