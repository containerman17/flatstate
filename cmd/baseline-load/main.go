// Command baseline-load fills the flatstate hash-keyed snapshot baseline
// (0x07 accounts, 0x08 slots) and 0x06 code rows from a STOPPED avalanchego
// node's PebbleDB (see docs/baseline-loader.md). The node must have synced
// the C-chain with the default hash state scheme, so its geth snapshot disk
// layer is a consistent full state at the last accepted height S (coreth
// flattens the snapshot on every accept). S is recovered by matching header
// roots below the acceptor tip against the snapshot root marker; no match
// aborts loudly.
//
// Resumable: a progress cursor (phase + last source key) is committed with
// the row chunks; on restart the source iterator seeks back to it and the
// boundary chunk is rewritten idempotently.
package main

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/ava-labs/avalanchego/database/pebbledb"
	"github.com/ava-labs/avalanchego/database/prefixdb"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/logging"
	evmdb "github.com/ava-labs/avalanchego/vms/evm/database"
	"github.com/ava-labs/avalanchego/vms/evm/sync/customrawdb"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/ethdb"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/flatstate/follower/net"
	"github.com/containerman17/flatstate/schema"
	"github.com/containerman17/flatstate/store"
)

// cChainID is the mainnet C-chain blockchain ID.
const cChainID = "2q9e4r6Mu3U68nU1fYjgbR6JvwrRx36CohpAX5UQxse55x1Q5"

func main() {
	if err := run(); err != nil {
		slog.Error("baseline-load failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		nodeDB     = flag.String("node-db", "", "pebble directory of the STOPPED node (…/db/mainnet/…)")
		dbPath     = flag.String("db", "", "flatstate LMDB env path (created if needed)")
		mapGB      = flag.Int64("map-size-gb", 200, "LMDB map size in GiB")
		chainIDStr = flag.String("chain-id", cChainID, "C-chain blockchain ID")
	)
	flag.Parse()
	if *nodeDB == "" || *dbPath == "" {
		return errors.New("-node-db and -db are required")
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Account RLP carries the coreth multicoin extra payload; decoding
	// without the graft extras registered misparses it.
	net.RegisterExtras()

	raw, err := pebbledb.New(*nodeDB, nil, logging.NoLog{}, nil)
	if err != nil {
		return fmt.Errorf("open node db (is the node stopped?): %w", err)
	}
	defer raw.Close()
	chainID, err := ids.FromString(*chainIDStr)
	if err != nil {
		return fmt.Errorf("chain-id: %w", err)
	}
	// Replicate the node's exact prefix stack: chains/manager.go
	// (chainID, then "vm") + coreth vm_database.go ("ethdb", nested).
	vmDB := prefixdb.New([]byte("vm"), prefixdb.New(chainID[:], raw))
	chaindb := rawdb.NewDatabase(evmdb.New(prefixdb.NewNested([]byte("ethdb"), vmDB)))

	s, hash, err := findPivot(chaindb)
	if err != nil {
		return err
	}
	log.Info("baseline pivot", "height", s, "hash", hash)

	db, err := store.Open(*dbPath, *mapGB<<30)
	if err != nil {
		return err
	}
	defer db.Close()
	return load(db, chaindb, s, log)
}

// findPivot returns the height S whose state the snapshot disk layer holds:
// the block at or below the acceptor tip whose header root equals the
// snapshot root marker.
func findPivot(chaindb ethdb.Database) (uint64, common.Hash, error) {
	snapRoot := rawdb.ReadSnapshotRoot(chaindb)
	if snapRoot == (common.Hash{}) {
		return 0, common.Hash{}, errors.New("no snapshot root marker: snapshot missing or sync unfinished")
	}
	tip, err := customrawdb.ReadAcceptorTip(chaindb)
	if err != nil {
		return 0, common.Hash{}, err
	}
	if tip == (common.Hash{}) {
		return 0, common.Hash{}, errors.New("no acceptor tip marker")
	}
	hash := tip
	for range 1024 {
		num := rawdb.ReadHeaderNumber(chaindb, hash)
		if num == nil {
			return 0, common.Hash{}, fmt.Errorf("no header number for %x", hash)
		}
		h := rawdb.ReadHeader(chaindb, hash, *num)
		if h == nil {
			return 0, common.Hash{}, fmt.Errorf("no header %d %x", *num, hash)
		}
		if h.Root == snapRoot {
			return *num, hash, nil
		}
		hash = h.ParentHash
	}
	return 0, common.Hash{}, fmt.Errorf("no header within 1024 below the acceptor tip matches snapshot root %x", snapRoot)
}

// load drains the snapshot key ranges into the baseline and finishes it.
func load(db *store.DB, chaindb ethdb.Database, s uint64, log *slog.Logger) error {
	bl, err := db.NewBaseline(s)
	if err != nil {
		return err
	}
	prog, _, err := db.BaselineProgress()
	if err != nil {
		return err
	}
	return loadFrom(bl, chaindb, prog, log)
}

type phaseDef struct {
	id     byte
	name   string
	prefix []byte
	keyLen int // exact physical key length; other lengths are foreign rows
	put    func(bl *store.Baseline, key, val []byte) error
}

var phases = []phaseDef{
	{1, "accounts", rawdb.SnapshotAccountPrefix, 33, putAccount},
	{2, "slots", rawdb.SnapshotStoragePrefix, 65, putSlot},
	{3, "code", rawdb.CodePrefix, 33, putCode},
}

const progressEvery = 1 << 18

func loadFrom(bl *store.Baseline, chaindb ethdb.Database, prog []byte, log *slog.Logger) error {
	resumePhase := byte(1)
	var resumeKey []byte
	if len(prog) >= 1 {
		resumePhase = prog[0]
		resumeKey = prog[1:]
		log.Info("resuming", "phase", resumePhase, "key", fmt.Sprintf("%x", resumeKey))
	}
	for _, ph := range phases {
		if ph.id < resumePhase {
			continue
		}
		var start []byte
		if ph.id == resumePhase && len(resumeKey) > len(ph.prefix) {
			start = resumeKey[len(ph.prefix):]
		}
		if err := runPhase(bl, chaindb, ph, start, log); err != nil {
			return err
		}
	}
	return bl.Finish()
}

func runPhase(bl *store.Baseline, chaindb ethdb.Database, ph phaseDef, start []byte, log *slog.Logger) error {
	it := chaindb.NewIterator(ph.prefix, start)
	defer it.Release()
	t0 := time.Now()
	var n uint64
	for it.Next() {
		key := it.Key()
		if len(key) != ph.keyLen {
			continue
		}
		if err := ph.put(bl, key, it.Value()); err != nil {
			return fmt.Errorf("%s row %x: %w", ph.name, key, err)
		}
		n++
		if n%progressEvery == 0 {
			bl.SetProgress(append([]byte{ph.id}, key...))
		}
		if n%(1<<22) == 0 {
			log.Info("phase progress", "phase", ph.name, "rows", n,
				"rows_per_sec", uint64(float64(n)/time.Since(t0).Seconds()))
		}
	}
	if err := it.Error(); err != nil {
		return fmt.Errorf("%s iterator: %w", ph.name, err)
	}
	// Phase complete: park the cursor at the next phase and make it durable.
	bl.SetProgress([]byte{ph.id + 1})
	if err := bl.Flush(); err != nil {
		return err
	}
	log.Info("phase done", "phase", ph.name, "rows", n, "elapsed", time.Since(t0).Round(time.Second).String())
	return nil
}

func putAccount(bl *store.Baseline, key, val []byte) error {
	acc, err := types.FullAccount(val)
	if err != nil {
		return err
	}
	a := schema.Account{Nonce: acc.Nonce, CodeHash: schema.Hash(common.BytesToHash(acc.CodeHash))}
	if acc.Balance != nil {
		a.Balance = *acc.Balance
	}
	return bl.BaseAccount(schema.Hash(key[1:33]), &a)
}

func putSlot(bl *store.Baseline, key, val []byte) error {
	// Snapshot storage values are the RLP-encoded trimmed big-endian value.
	_, content, _, err := rlp.Split(val)
	if err != nil {
		return err
	}
	if len(content) > 32 {
		return fmt.Errorf("slot value %d bytes after RLP, want <= 32", len(content))
	}
	var v schema.Hash
	copy(v[32-len(content):], content)
	return bl.BaseSlot(schema.Hash(key[1:33]), schema.Hash(key[33:65]), v)
}

func putCode(bl *store.Baseline, key, val []byte) error {
	return bl.Code(schema.Hash(key[1:33]), val)
}
