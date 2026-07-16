# Baseline loader: source decision

Goal: enumerate full C-chain state at a known height S as (keccak(addr) -> account,
keccak(addr)||keccak(slot) -> value, codeHash -> code) to fill the 0x07/0x08 hash-keyed
baseline and 0x06 code rows (DESIGN.md D6 rev 2).

## Decision (rev 2): be a state-sync client; no node anywhere

`cmd/baseline-load` joins mainnet p2p directly (the existing `follower/net` stack) and
speaks the C-chain state-sync leaf protocol as a client, writing leaves straight into
LMDB. The coreth sync client (`graft/evm/sync/client`) is reused as a library; it
verifies every response as a merkle RANGE PROOF against the pivot block's state root,
which is the baseline's integrity guarantee. The only trusted input is the pivot
header (height + state root) fetched from the public RPC, the same source the follower
already trusts for its validator set.

Wire facts (verified in v1.14.2 source):

- Leaf exchange is chain-scoped AppRequest/AppResponse: `message.CorethLeafsRequest`
  marshaled with `message.CorethCodec` (`message.RequestToBytes` inside the client),
  sent to the C-chain ID. Responses arrive as AppResponse; failures as AppError.
- Legacy coreth sync handlers only accept EVEN request IDs; odd IDs are routed to the
  peer's SDK p2p network (`coreth/network.IsNetworkRequest`). The NetClient adapter
  doubles its counter.
- `Start`/`End` in a LeafsRequest are both inclusive; responses are capped at 1024
  leaves server-side; the client's verified `More` flag says whether the trie has more
  leaves right of the last returned key.
- Peers retain and serve the state at summary heights: multiples of coreth's
  `StateSyncCommitInterval` (16384). S defaults to the newest boundary at least 256
  below head.
- Account leaves are full `types.StateAccount` RLP (coreth multicoin extras must be
  registered before decoding); storage leaves are RLP of the trimmed big-endian value.

## Loader shape (follower/sync)

- 256 segments by first account-hash byte, N concurrent workers (default 32); each
  worker walks its segment's account leaves, streams each account's storage trie, and
  first-claim fetches code. A single writer goroutine owns the store.Baseline, so row
  order is global: an account's slots and code are always committed before or with the
  account row itself.
- Resume: a 256-bit segment-done bitmap rides the baseline progress row
  (`meta 0x00 0x04`); per-segment watermarks are recovered from the greatest committed
  0x07 key (`MaxBaseAccountWithPrefix`). A final code sweep
  (`MissingCodeHashes` + GetCode) closes cross-worker code claim races and crash gaps,
  then `Finish()` sets `baseline_complete`.
- ponytail: one worker per storage trie; split giant storage tries into parallel
  sub-ranges if a profile shows a single-account tail dominating.

## Options rejected

- **Run a helper avalanchego node and read its snapshot layer offline** (the previous
  rev of this doc, fully implemented then removed): correct and least-code, but a fresh
  mainnet node must bootstrap the P-chain first, and EXECUTING all ~25M P-chain blocks
  measured ~8-9h on the target box (fetch alone was ~75 min). Rejected by the user:
  running any full node is exactly what flatstate exists to avoid. The ~20 min sync
  figure is the C-chain leaf exchange itself, which this loader now performs directly.
- **Iterate Firewood**: v1.14.2 defaults to the hash scheme; opting into firewood to
  then need FFI revision iteration is strictly more code.
- **eth_getProof / RPC scraping**: no range enumeration, per-key only; unusable for
  ~10^8 keys and adds a second trust model.
