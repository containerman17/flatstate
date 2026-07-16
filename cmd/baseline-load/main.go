// Command baseline-load fills the flatstate hash-keyed snapshot baseline
// (0x07 accounts, 0x08 slots) and 0x06 code rows by acting as a C-chain
// state-sync CLIENT over mainnet p2p (docs/baseline-loader.md). No node runs
// anywhere: leaf ranges come from peers as AppRequest/AppResponse and are
// verified as merkle range proofs against the state root of the pivot block
// S by the reused coreth sync client.
//
// S defaults to the most recent state-sync summary height (a multiple of the
// coreth StateSyncCommitInterval, which peers retain and serve); its state
// root is fetched from the public RPC. Resumable: rerunning after a kill
// continues from the per-segment watermarks.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ava-labs/avalanchego/graft/evm/message"
	syncclient "github.com/ava-labs/avalanchego/graft/evm/sync/client"
	"github.com/ava-labs/avalanchego/graft/evm/sync/client/stats"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/libevm/common"

	fnet "github.com/containerman17/flatstate/follower/net"
	fsync "github.com/containerman17/flatstate/follower/sync"
	"github.com/containerman17/flatstate/store"
)

// summaryInterval is coreth's StateSyncCommitInterval: every peer retains and
// serves the state at these heights.
const summaryInterval = 16384

// summaryMargin keeps S at least this far below head so the newest boundary
// has been committed and its summary registered on effectively every peer.
const summaryMargin = 256

func main() {
	if err := run(); err != nil {
		slog.Error("baseline-load failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		dbPath   = flag.String("db", "", "flatstate LMDB env path (created if needed)")
		mapGB    = flag.Int64("map-size-gb", 200, "LMDB map size in GiB")
		nodeURI  = flag.String("node-uri", fnet.DefaultNodeURI, "public node for bootstrap RPC")
		workers  = flag.Int("workers", 32, "concurrent leaf-range fetchers")
		inflight = flag.Int("inflight", 320, "global cap on outstanding leaf/code requests")
		height   = flag.Uint64("height", 0, "pivot height S (0 = latest summary boundary)")
	)
	flag.Parse()
	if *dbPath == "" {
		return errors.New("-db is required")
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := store.Open(*dbPath, *mapGB<<30)
	if err != nil {
		return err
	}
	defer db.Close()

	// Pivot: a resumed store dictates S; otherwise the flag or the latest
	// summary boundary.
	s := *height
	if g, ok, err := db.Genesis(); err != nil {
		return err
	} else if ok {
		if s != 0 && s != g {
			return fmt.Errorf("store already has genesis %d, cannot load at %d", g, s)
		}
		s = g
		log.Info("resuming existing baseline", "height", s)
	} else if s == 0 {
		head, err := rpcBlockNumber(ctx, *nodeURI)
		if err != nil {
			return err
		}
		s = (head - summaryMargin) / summaryInterval * summaryInterval
		log.Info("pivot selected", "head", head, "height", s)
	}
	root, hash, err := rpcStateRoot(ctx, *nodeURI, s)
	if err != nil {
		return err
	}
	log.Info("pivot block", "height", s, "hash", hash, "root", root)

	fnet.RegisterExtras() // account RLP carries the coreth multicoin extra

	var nc *fsync.NetClient
	network, err := fnet.Dial(ctx, fnet.Config{
		NodeURI: *nodeURI,
		Callbacks: fnet.Callbacks{
			AppResponse: func(nodeID ids.NodeID, requestID uint32, response []byte, failed bool) {
				if nc != nil {
					nc.OnAppResponse(nodeID, requestID, response, failed)
				}
			},
		},
		Log: log,
	})
	if err != nil {
		return fmt.Errorf("net: %w", err)
	}
	defer network.Close()
	nc = fsync.NewNetClient(network, *inflight)

	client := syncclient.New(&syncclient.Config{
		Network: nc,
		Codec:   message.CorethCodec,
		Stats:   stats.NewNoOpStats(),
	})
	return fsync.Run(ctx, fsync.Config{
		Client:   client,
		DB:       db,
		Height:   s,
		Root:     root,
		Workers:  *workers,
		Log:      log,
		Timeouts: nc.Timeouts,
	})
}

// --- minimal C-chain JSON-RPC (stdlib only) ---

func rpcCall(ctx context.Context, nodeURI, method string, params ...any) (json.RawMessage, error) {
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		return nil, err
	}
	url := strings.TrimSuffix(nodeURI, "/") + "/ext/bc/C/rpc"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("%s: %w", method, err)
	}
	if out.Error != nil {
		return nil, fmt.Errorf("%s: %s", method, out.Error.Message)
	}
	return out.Result, nil
}

func rpcBlockNumber(ctx context.Context, nodeURI string) (uint64, error) {
	raw, err := rpcCall(ctx, nodeURI, "eth_blockNumber")
	if err != nil {
		return 0, err
	}
	var hexNum string
	if err := json.Unmarshal(raw, &hexNum); err != nil {
		return 0, err
	}
	return strconv.ParseUint(strings.TrimPrefix(hexNum, "0x"), 16, 64)
}

func rpcStateRoot(ctx context.Context, nodeURI string, height uint64) (root, hash common.Hash, err error) {
	raw, err := rpcCall(ctx, nodeURI, "eth_getBlockByNumber", fmt.Sprintf("0x%x", height), false)
	if err != nil {
		return common.Hash{}, common.Hash{}, err
	}
	var blk struct {
		StateRoot string `json:"stateRoot"`
		Hash      string `json:"hash"`
	}
	if err := json.Unmarshal(raw, &blk); err != nil {
		return common.Hash{}, common.Hash{}, err
	}
	if blk.StateRoot == "" {
		return common.Hash{}, common.Hash{}, fmt.Errorf("block %d has no stateRoot (missing block?)", height)
	}
	return common.HexToHash(blk.StateRoot), common.HexToHash(blk.Hash), nil
}
