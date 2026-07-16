# flatstate

Fast EVM state engine for the Avalanche C-Chain: tip simulation, replay, and historical test execution at ~200k EVM calls/sec on one box.

One writer process embeds avalanchego (real consensus), captures preimage-keyed post-image state history into LMDB, and publishes unfinalized tip diffs to an ephemeral env. Any number of reader processes (bots, replay, tests) open LMDB read-only and treat it as shared memory.

See [DESIGN.md](DESIGN.md) for the full set of design decisions and the reasoning behind them.

Status: early construction.
