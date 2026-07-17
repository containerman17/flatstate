// Command follower is the flatstate writer process (DESIGN.md D10): the
// D2 rev 2 custom C-chain follower (p2p + snowman sampling + coreth-as-
// library execution) + capture + external mempool WS client + main LMDB
// writer + ephemeral tip env publisher.
//
// --dry-run connects, follows, executes, and validates but writes nothing:
// with a baseline store it opens it read-only and folds accepted diffs in
// memory; without one it follows consensus only (header-only batches).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ava-labs/avalanchego/ids"

	"github.com/containerman17/flatstate/capture"
	"github.com/containerman17/flatstate/follower/consensus"
	"github.com/containerman17/flatstate/follower/exec"
	"github.com/containerman17/flatstate/follower/mempoolws"
	"github.com/containerman17/flatstate/follower/net"
	"github.com/containerman17/flatstate/mem"
	"github.com/containerman17/flatstate/node"
	"github.com/containerman17/flatstate/schema"
	"github.com/containerman17/flatstate/store"
	"github.com/containerman17/flatstate/tipbus"
)

type duration time.Duration

func (d *duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	*d = duration(v)
	return err
}

type fileConfig struct {
	NodeURI          string   `json:"node_uri"`          // bootstrap RPC, default https://api.avax.network
	BootstrapPeers   []string `json:"bootstrap_peers"`   // optional extra "NodeID-...@ip:port"
	WSURLs           []string `json:"ws_urls"`           // mempool WebSocket endpoints
	DBPath           string   `json:"db_path"`           // main LMDB env
	TipbusPath       string   `json:"tipbus_path"`       // ephemeral tip env
	MapSizeGB        int64    `json:"map_size_gb"`       // main env map size, default 200
	ValidatorRefresh duration `json:"validator_refresh"` // default 5m
	PollInterval     duration `json:"poll_interval"`     // default 100ms
}

func main() {
	if err := run(); err != nil {
		slog.Error("follower exiting", "err", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		configPath = flag.String("config", "", "path to JSON config file")
		dryRun     = flag.Bool("dry-run", false, "connect, follow, execute, validate; write nothing")
		debug      = flag.Bool("debug", false, "debug logging")
	)
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log)

	var cfg fileConfig
	if *configPath != "" {
		raw, err := os.ReadFile(*configPath)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return fmt.Errorf("config: %w", err)
		}
	} else if !*dryRun {
		return errors.New("-config required (or use -dry-run)")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// --- storage side ---
	var (
		db       *store.DB
		resume   *consensus.Anchor
		executor consensus.Executor
		ex       *exec.Exec
	)

	openStore := func() error {
		var err error
		if *dryRun {
			db, err = store.OpenReadOnly(cfg.DBPath)
		} else {
			db, err = store.Open(cfg.DBPath, cfg.MapSizeGB<<30)
		}
		return err
	}

	followOnly := false
	switch {
	case *dryRun && cfg.DBPath == "":
		followOnly = true
	case *dryRun:
		if err := openStore(); err != nil {
			log.Warn("dry-run: cannot open store, following without execution", "err", err)
			followOnly = true
		}
	default:
		if cfg.DBPath == "" {
			return errors.New("db_path required")
		}
		if err := openStore(); err != nil {
			return err
		}
	}
	if db != nil {
		defer db.Close()
		done, err := db.BaselineComplete()
		if err != nil {
			return err
		}
		if !done {
			if *dryRun {
				log.Warn("dry-run: baseline incomplete, following without execution")
				followOnly = true
				db.Close()
				db = nil
			} else {
				return errors.New("baseline incomplete: run the baseline loader first (D6 rev 2)")
			}
		}
	}

	if followOnly {
		executor = consensus.HeaderOnly{}
		log.Info("mode: follow-only (no baseline; consensus + header validation, no state execution)")
	} else {
		var err error
		ex, err = exec.New(db)
		if err != nil {
			return err
		}
		executor = ex
		resume, err = resumeFromStore(db)
		if err != nil {
			return err
		}
		log.Info("resume point", "height", resume.Height, "hash_known", resume.HashSet, "dry_run", *dryRun)
	}

	// --- sinks ---
	type mempoolSink interface {
		Mempool(tx []byte, time uint64) error
	}
	sinkReady := make(chan mempoolSink, 1) // real mode: the node sink's tracker feeds mempool too
	var makeSink func(uint64, schema.Hash) (consensus.Sink, error)
	if *dryRun {
		makeSink = func(h uint64, hash schema.Hash) (consensus.Sink, error) {
			log.Info("dry-run sink attached", "start", h)
			return &drySink{log: log, ex: ex, batches: make(map[schema.Hash]*capture.Batch)}, nil
		}
	} else {
		bus, err := tipbus.OpenWriter(cfg.TipbusPath, 0)
		if err != nil {
			return fmt.Errorf("tipbus: %w", err)
		}
		defer bus.Close()
		st, err := mem.New(db)
		if err != nil {
			return err
		}
		nodeSink := node.NewSink(db, st, bus)
		makeSink = func(h uint64, hash schema.Hash) (consensus.Sink, error) {
			tracker := node.NewTracker(nodeSink, h, hash)
			select {
			case sinkReady <- tracker:
			default:
			}
			return &liveSink{tracker: tracker, ex: ex}, nil
		}
	}

	// --- consensus engine over the network ---
	var engine atomic.Pointer[consensus.Engine]
	callbacks := net.Callbacks{
		Container: func(nodeID ids.NodeID, c []byte) {
			if e := engine.Load(); e != nil {
				e.OnContainer(nodeID, c)
			}
		},
		Chits: func(nodeID ids.NodeID, reqID uint32, preferred, preferredAtHeight, accepted ids.ID, acceptedHeight uint64) {
			if e := engine.Load(); e != nil {
				e.OnChits(nodeID, reqID, preferred, preferredAtHeight, accepted, acceptedHeight)
			}
		},
		Ancestors: func(nodeID ids.NodeID, reqID uint32, containers [][]byte) {
			if e := engine.Load(); e != nil {
				e.OnAncestors(nodeID, reqID, containers)
			}
		},
	}
	network, err := net.Dial(ctx, net.Config{
		NodeURI:         cfg.NodeURI,
		RefreshInterval: time.Duration(cfg.ValidatorRefresh),
		BootstrapPeers:  cfg.BootstrapPeers,
		Callbacks:       callbacks,
		Log:             log,
	})
	if err != nil {
		return fmt.Errorf("net: %w", err)
	}
	defer network.Close()

	engCfg := consensus.Config{
		Net:          network,
		Exec:         executor,
		MakeSink:     makeSink,
		Resume:       resume,
		PollInterval: time.Duration(cfg.PollInterval),
		Log:          log,
	}
	if ex != nil {
		engCfg.SeedHeaders = ex.SeedHeaders
	}
	eng, err := consensus.New(engCfg)
	if err != nil {
		return err
	}
	engine.Store(eng)

	// --- mempool (real mode: after the tracker exists; dry-run: counter) ---
	mempoolErr := make(chan error, 1)
	if len(cfg.WSURLs) > 0 {
		go func() {
			var sink func(tx []byte, t uint64) error
			if *dryRun {
				var n atomic.Uint64
				sink = func(tx []byte, t uint64) error {
					if c := n.Add(1); c%1000 == 0 {
						log.Info("dry-run mempool arrivals", "count", c)
					}
					return nil
				}
			} else {
				var tracker mempoolSink
				select {
				case tracker = <-sinkReady:
				case <-ctx.Done():
					return
				}
				sink = func(tx []byte, t uint64) error { return tracker.Mempool(tx, t) }
			}
			mempoolErr <- mempoolws.Run(ctx, mempoolws.Config{URLs: cfg.WSURLs, Sink: sink, Log: log})
		}()
	} else {
		log.Warn("no ws_urls configured: mempool arrivals are NOT being captured")
	}

	// --- status + watchdog (own goroutine: the main loop can sit inside
	// Tick for minutes during backfill execution, and LastAccepted takes
	// the engine lock; the store watermark is lock-free truth) ---
	if db != nil && !*dryRun {
		go func() {
			t := time.NewTicker(30 * time.Second)
			defer t.Stop()
			var last uint64
			stall := time.Now()
			dumped := false
			for range t.C {
				f, ok, err := db.Finalized()
				if err != nil || !ok {
					continue
				}
				log.Info("watermark", "finalized", f, "peers", network.NumConnected())
				if f != last {
					last, stall, dumped = f, time.Now(), false
					continue
				}
				since := time.Since(stall)
				if since > 10*time.Minute {
					log.Error("watchdog: finalized stalled, aborting", "for", since.Round(time.Second).String())
					buf := make([]byte, 1<<22)
					os.Stderr.Write(buf[:runtime.Stack(buf, true)])
					os.Exit(2)
				}
				if since > 2*time.Minute && !dumped {
					dumped = true
					log.Warn("watchdog: finalized not advancing, dumping stacks", "for", since.Round(time.Second).String())
					buf := make([]byte, 1<<22)
					os.Stderr.Write(buf[:runtime.Stack(buf, true)])
				}
			}
		}()
	}

	// --- main loop ---
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	status := time.NewTicker(30 * time.Second)
	defer status.Stop()
	log.Info("follower running", "dry_run", *dryRun)
	for {
		select {
		case <-ctx.Done():
			log.Info("shutting down")
			return nil
		case err := <-eng.Fatal():
			return fmt.Errorf("capture halted: %w", err)
		case err := <-network.DispatchDone():
			return fmt.Errorf("network dispatch stopped: %w", err)
		case err := <-mempoolErr:
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("mempool: %w", err)
		case <-tick.C:
			eng.Tick()
		case <-status.C:
			h, hash := eng.LastAccepted()
			log.Info("status", "finalized", h, "hash", fmt.Sprintf("%x", hash[:6]), "peers", network.NumConnected())
		}
	}
}

// resumeFromStore derives the consensus resume point: the finalized
// watermark (hash from its 0x04 diff row) or, on a fresh baseline, the
// history genesis S with the hash learned from the fetched chain.
func resumeFromStore(db *store.DB) (*consensus.Anchor, error) {
	if f, ok, err := db.Finalized(); err != nil {
		return nil, err
	} else if ok {
		b, err := db.GetDiff(f)
		if err != nil {
			return nil, fmt.Errorf("finalized watermark %d has no diff row: %w", f, err)
		}
		return &consensus.Anchor{Height: f, EthHash: b.Hash, HashSet: true}, nil
	}
	if s, ok, err := db.Genesis(); err != nil {
		return nil, err
	} else if ok {
		return &consensus.Anchor{Height: s}, nil
	}
	return nil, errors.New("store has neither finalized watermark nor baseline genesis")
}

// liveSink is the real-mode consensus sink: node.Tracker drives the D7
// order (store txn, mem base, watermark, tipbus), then the executor prunes.
type liveSink struct {
	tracker *node.Tracker
	ex      *exec.Exec
}

func (s *liveSink) Verified(b *capture.Batch) { s.tracker.Verified(b) }
func (s *liveSink) Head(h schema.Hash) error  { return s.tracker.Head(h) }
func (s *liveSink) Accepted(block uint64, hash schema.Hash) error {
	if err := s.tracker.Accepted(block, hash); err != nil {
		return err
	}
	s.ex.OnFinalized(block) // after the store committed (D7)
	return nil
}

// drySink validates without writing: accepted diffs fold into the
// executor's in-memory base (read-only store never advances).
type drySink struct {
	log     *slog.Logger
	ex      *exec.Exec // nil in follow-only mode
	batches map[schema.Hash]*capture.Batch
}

func (s *drySink) Verified(b *capture.Batch) { s.batches[b.Hash] = b }
func (s *drySink) Head(schema.Hash) error    { return nil }
func (s *drySink) Accepted(block uint64, hash schema.Hash) error {
	b, ok := s.batches[hash]
	if !ok {
		return fmt.Errorf("dry-run: accepted %d %x was never verified", block, hash[:4])
	}
	if s.ex != nil {
		s.ex.FoldFinalized(b)
	}
	for h, p := range s.batches {
		if p.Block <= block {
			delete(s.batches, h)
		}
	}
	return nil
}
