// Package sync bulk-loads the flatstate hash-keyed baseline (0x07 accounts,
// 0x08 slots, 0x06 code) at a chosen state root S by acting as a C-chain
// state-sync client (docs/baseline-loader.md): leaf ranges are fetched from
// mainnet peers over AppRequest and verified as merkle range proofs against
// S's root by the reused coreth sync client. No node runs anywhere.
//
// Resumable: the account keyspace is split into 256 segments by first hash
// byte; a done-bitmap rides the baseline progress row and per-segment
// watermarks are recovered from the greatest committed 0x07 key. Per account,
// storage and (first-claim) code rows are enqueued before the account row,
// so a committed account row implies its state is complete; code gaps from
// cross-worker claim races are closed by a final code sweep.
package sync

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math/big"
	gosync "sync"
	"sync/atomic"
	"time"

	"github.com/ava-labs/avalanchego/graft/evm/message"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
	"golang.org/x/sync/errgroup"

	"github.com/containerman17/flatstate/schema"
	"github.com/containerman17/flatstate/store"
)

// Client is the slice of the coreth sync client this loader uses (the full
// syncclient.Client interface also carries GetBlocks/AddClient, unused here).
type Client interface {
	GetLeafs(ctx context.Context, req message.LeafsRequest) (message.LeafsResponse, error)
	GetCode(ctx context.Context, hashes []common.Hash) ([][]byte, error)
}

type Config struct {
	Client  Client
	DB      *store.DB
	Height  uint64      // S
	Root    common.Hash // state root of block S
	Workers int         // concurrent segment fetchers; <=0 = 32
	Log     *slog.Logger
	// Timeouts optionally reports cumulative request timeouts for the
	// progress log (congestion visibility).
	Timeouts func() uint64
}

const leafLimit = 1024 // server-side response cap

// storageConcurrency is how many storage tries one segment worker resolves
// in parallel within an account response (storage requests dominate).
const storageConcurrency = 6

// Giant storage tries dominate the load's tail: after splitAfter sequential
// responses a trie is declared giant and the rest of its keyspace is fetched
// as splitWays parallel sub-ranges. Vars so tests can shrink them.
// DO NOT change the values between runs of the same store: sub-range resume
// watermarks (MaxBaseSlot) are only sound while the split boundaries are
// reproducible.
var (
	splitAfter = 32
	splitWays  = 16
)

type bundle struct {
	seg   int
	final bool
	apply func(bl *store.Baseline) error
}

type syncer struct {
	cfg    Config
	writes chan bundle

	codeClaims gosync.Map // codeHash -> struct{}

	accounts atomic.Uint64
	slots    atomic.Uint64
	codes    atomic.Uint64
}

// Run drives the load to completion. On error or ctx cancellation the
// already-committed rows and progress bitmap allow a later Run to resume.
func Run(ctx context.Context, cfg Config) error {
	if cfg.Workers <= 0 {
		cfg.Workers = 32
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	bl, err := cfg.DB.NewBaseline(cfg.Height)
	if err != nil {
		return err
	}

	var bitmap [32]byte
	if prog, ok, err := cfg.DB.BaselineProgress(); err != nil {
		return err
	} else if ok {
		if len(prog) != len(bitmap) {
			return fmt.Errorf("sync: progress row has %d bytes, want %d", len(prog), len(bitmap))
		}
		copy(bitmap[:], prog)
	}

	type segTask struct {
		seg   int
		start []byte // 32B resume key, nil = segment beginning
	}
	var tasks []segTask
	for seg := range 256 {
		if bitmap[seg/8]&(1<<(seg%8)) != 0 {
			continue
		}
		h, ok, err := cfg.DB.MaxBaseAccountWithPrefix(byte(seg))
		if err != nil {
			return err
		}
		var start []byte
		if ok {
			start = incKey(h[:]) // watermark account is fully committed
		}
		tasks = append(tasks, segTask{seg, start})
	}
	cfg.Log.Info("baseline sync starting", "height", cfg.Height, "root", cfg.Root,
		"segments_left", len(tasks), "workers", cfg.Workers)

	s := &syncer{cfg: cfg, writes: make(chan bundle, cfg.Workers*2)}

	// Single writer goroutine: the only Baseline user, so per-account row
	// order (slots, code, then account) is a global commit-order invariant.
	writerErr := make(chan error, 1)
	go func() {
		for b := range s.writes {
			if b.apply != nil {
				if err := b.apply(bl); err != nil {
					writerErr <- err
					for range s.writes {
					} // drain so workers never block
					return
				}
			}
			if b.final {
				bitmap[b.seg/8] |= 1 << (b.seg % 8)
				bl.SetProgress(bitmap[:])
			}
		}
		writerErr <- nil
	}()

	t0 := time.Now()
	statusDone := make(chan struct{})
	go func() {
		tick := time.NewTicker(15 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-statusDone:
				return
			case <-tick.C:
				el := time.Since(t0).Seconds()
				args := []any{
					"accounts", s.accounts.Load(), "slots", s.slots.Load(), "codes", s.codes.Load(),
					"rows_per_sec", uint64(float64(s.accounts.Load()+s.slots.Load()) / el),
				}
				if cfg.Timeouts != nil {
					args = append(args, "timeouts", cfg.Timeouts())
				}
				cfg.Log.Info("baseline sync progress", args...)
			}
		}
	}()

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(cfg.Workers)
	for _, t := range tasks {
		g.Go(func() error { return s.runSegment(gctx, t.seg, t.start) })
	}
	segErr := g.Wait()
	close(s.writes)
	werr := <-writerErr
	close(statusDone)
	if segErr != nil {
		return segErr
	}
	if werr != nil {
		return werr
	}

	if err := bl.Flush(); err != nil { // make all segment-final bits durable
		return err
	}
	if err := s.codeSweep(ctx, bl); err != nil {
		return err
	}
	if err := bl.Finish(); err != nil {
		return err
	}
	cfg.Log.Info("baseline sync complete", "accounts", s.accounts.Load(), "slots", s.slots.Load(),
		"codes", s.codes.Load(), "elapsed", time.Since(t0).Round(time.Second).String())
	return nil
}

// runSegment walks one 1/256th of the account trie.
func (s *syncer) runSegment(ctx context.Context, seg int, start []byte) error {
	segEnd := make([]byte, 32)
	segEnd[0] = byte(seg)
	for i := 1; i < len(segEnd); i++ {
		segEnd[i] = 0xff
	}
	if start == nil {
		start = make([]byte, 32)
		start[0] = byte(seg)
	}
	for {
		req, err := message.NewLeafsRequest(message.CorethLeafsRequestType,
			s.cfg.Root, common.Hash{}, start, segEnd, leafLimit, message.StateTrieNode)
		if err != nil {
			return err
		}
		resp, err := s.cfg.Client.GetLeafs(ctx, req)
		if err != nil {
			return fmt.Errorf("sync: segment %02x leafs at %x: %w", seg, start, err)
		}
		// Storage tries are fetched concurrently (they dominate the request
		// count); slot bundles stream as they arrive, which is safe in any
		// order. Only the account rows must commit ascending per segment
		// (the resume watermark), so they are collected and enqueued in key
		// order after the whole response resolved.
		accountRows := make([]func(*store.Baseline) error, len(resp.Keys))
		ag, agctx := errgroup.WithContext(ctx)
		ag.SetLimit(storageConcurrency)
		for i := range resp.Keys {
			ag.Go(func() error {
				apply, err := s.handleAccount(agctx, seg, resp.Keys[i], resp.Vals[i])
				accountRows[i] = apply
				return err
			})
		}
		if err := ag.Wait(); err != nil {
			return err
		}
		for _, apply := range accountRows {
			if err := s.enqueue(ctx, bundle{seg: seg, apply: apply}); err != nil {
				return err
			}
		}
		if len(resp.Keys) == 0 || !resp.More {
			break
		}
		last := resp.Keys[len(resp.Keys)-1]
		if bytes.Compare(last, segEnd) >= 0 {
			break
		}
		start = incKey(last)
		if start == nil {
			break
		}
	}
	select {
	case s.writes <- bundle{seg: seg, final: true}:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// handleAccount fetches the account's storage and code. Slot bundles are
// enqueued as they arrive (order-independent); the returned apply func is the
// account row (plus a first-claim code row), which the caller enqueues in
// ascending key order to keep the segment watermark sound.
func (s *syncer) handleAccount(ctx context.Context, seg int, key, val []byte) (func(*store.Baseline) error, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("sync: account leaf key has %d bytes", len(key))
	}
	var acc types.StateAccount
	if err := rlp.DecodeBytes(val, &acc); err != nil {
		return nil, fmt.Errorf("sync: account %x: %w", key, err)
	}
	addrHash := schema.Hash(common.BytesToHash(key))
	row := schema.Account{Nonce: acc.Nonce, CodeHash: schema.Hash(common.BytesToHash(acc.CodeHash))}
	if acc.Balance != nil {
		row.Balance = *acc.Balance
	}

	// Storage trie: sequential first; a trie still going after splitAfter
	// responses is giant, so the rest of its keyspace fans out into
	// splitWays parallel sub-ranges (slot bundles are order-independent;
	// the account row below is returned only after all of them finish).
	if acc.Root != (common.Hash{}) && acc.Root != types.EmptyRootHash {
		next, err := s.walkStorage(ctx, seg, addrHash, acc.Root, nil, nil, splitAfter)
		if err != nil {
			return nil, err
		}
		if next != nil {
			sg, sgctx := errgroup.WithContext(ctx)
			for _, r := range splitRange(next, splitWays) {
				sg.Go(func() error {
					_, err := s.walkStorage(sgctx, seg, addrHash, acc.Root, r[0], r[1], 0)
					return err
				})
			}
			if err := sg.Wait(); err != nil {
				return nil, err
			}
		}
	}

	// Code: first claimer fetches; races are closed by the final sweep.
	var code []byte
	codeHash := common.BytesToHash(acc.CodeHash)
	if codeHash != (common.Hash{}) && codeHash != types.EmptyCodeHash {
		if _, claimed := s.codeClaims.LoadOrStore(codeHash, struct{}{}); !claimed {
			blobs, err := s.cfg.Client.GetCode(ctx, []common.Hash{codeHash})
			if err != nil {
				return nil, fmt.Errorf("sync: code %x: %w", codeHash, err)
			}
			code = blobs[0]
			s.codes.Add(1)
		}
	}

	s.accounts.Add(1)
	return func(bl *store.Baseline) error {
		if code != nil {
			if err := bl.Code(schema.Hash(codeHash), code); err != nil {
				return err
			}
		}
		return bl.BaseAccount(addrHash, &row)
	}, nil
}

func (s *syncer) enqueue(ctx context.Context, b bundle) error {
	select {
	case s.writes <- b:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// walkStorage fetches storage leaves of one trie in [start, end] (nil = open)
// and enqueues slot bundles. maxReqs > 0 caps the number of requests: when
// the cap is hit with leaves remaining, the next start key is returned so the
// caller can fan out; a nil return means the range is exhausted.
//
// Sub-range mode (maxReqs == 0) resumes past the greatest already-committed
// slot in its range: sub-walkers commit contiguously from their start, so
// rows below the watermark are complete. The sequential warmup cannot use
// this (an earlier run's fan-out leaves holes across, never within, ranges).
func (s *syncer) walkStorage(ctx context.Context, seg int, addrHash schema.Hash, root common.Hash, start, end []byte, maxReqs int) ([]byte, error) {
	if maxReqs == 0 {
		if wm, ok, err := s.cfg.DB.MaxBaseSlot(addrHash, start, end); err != nil {
			return nil, err
		} else if ok {
			if next := incKey(wm[:]); next == nil || (len(end) > 0 && bytes.Compare(next, end) > 0) {
				return nil, nil // range already fully committed
			} else if bytes.Compare(next, start) > 0 {
				start = next
			}
		}
	}
	for n := 0; ; n++ {
		if maxReqs > 0 && n >= maxReqs {
			return start, nil
		}
		req, err := message.NewLeafsRequest(message.CorethLeafsRequestType,
			root, common.Hash(addrHash), start, end, leafLimit, message.StateTrieNode)
		if err != nil {
			return nil, err
		}
		resp, err := s.cfg.Client.GetLeafs(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("sync: storage of %x: %w", addrHash, err)
		}
		slots := make([][2]schema.Hash, 0, len(resp.Keys))
		for i := range resp.Keys {
			if len(resp.Keys[i]) != 32 {
				return nil, fmt.Errorf("sync: storage leaf key has %d bytes", len(resp.Keys[i]))
			}
			_, content, _, err := rlp.Split(resp.Vals[i])
			if err != nil || len(content) > 32 {
				return nil, fmt.Errorf("sync: storage value of %x: %v", addrHash, err)
			}
			var v schema.Hash
			copy(v[32-len(content):], content)
			slots = append(slots, [2]schema.Hash{schema.Hash(common.BytesToHash(resp.Keys[i])), v})
		}
		s.slots.Add(uint64(len(slots)))
		if err := s.enqueue(ctx, bundle{seg: seg, apply: func(bl *store.Baseline) error {
			for _, kv := range slots {
				if err := bl.BaseSlot(addrHash, kv[0], kv[1]); err != nil {
					return err
				}
			}
			return nil
		}}); err != nil {
			return nil, err
		}
		if len(resp.Keys) == 0 || !resp.More {
			return nil, nil
		}
		last := resp.Keys[len(resp.Keys)-1]
		if len(end) > 0 && bytes.Compare(last, end) >= 0 {
			return nil, nil
		}
		start = incKey(last)
		if start == nil {
			return nil, nil
		}
	}
}

// splitRange divides [from, ff..ff] into n contiguous inclusive sub-ranges.
func splitRange(from []byte, n int) [][2][]byte {
	lo := new(big.Int).SetBytes(from)
	hi := new(big.Int).Lsh(big.NewInt(1), uint(len(from)*8))
	hi.Sub(hi, big.NewInt(1))
	span := new(big.Int).Sub(hi, lo)
	if span.Sign() <= 0 || n <= 1 {
		return [][2][]byte{{from, nil}}
	}
	step := new(big.Int).Div(span, big.NewInt(int64(n)))
	if step.Sign() == 0 {
		return [][2][]byte{{from, nil}}
	}
	pad := func(v *big.Int) []byte {
		b := v.Bytes()
		out := make([]byte, len(from))
		copy(out[len(out)-len(b):], b)
		return out
	}
	var out [][2][]byte
	cur := new(big.Int).Set(lo)
	for i := range n {
		var endB []byte
		if i == n-1 {
			endB = nil // open end: the trie stops on its own
		} else {
			e := new(big.Int).Add(cur, step)
			endB = pad(e)
		}
		out = append(out, [2][]byte{pad(cur), endB})
		if i < n-1 {
			cur.Add(cur, step)
			cur.Add(cur, big.NewInt(1))
		}
	}
	return out
}

// codeSweep fetches any code hash referenced by a committed account row but
// missing its 0x06 row (crash between a claim and its commit, or an account
// row that relied on another worker's still-buffered claim).
func (s *syncer) codeSweep(ctx context.Context, bl *store.Baseline) error {
	missing, err := s.cfg.DB.MissingCodeHashes()
	if err != nil {
		return err
	}
	if len(missing) == 0 {
		return nil
	}
	s.cfg.Log.Info("code sweep", "missing", len(missing))
	for i := 0; i < len(missing); i += message.MaxCodeHashesPerRequest {
		batch := missing[i:min(i+message.MaxCodeHashesPerRequest, len(missing))]
		hashes := make([]common.Hash, len(batch))
		for j, h := range batch {
			hashes[j] = common.Hash(h)
		}
		blobs, err := s.cfg.Client.GetCode(ctx, hashes)
		if err != nil {
			return fmt.Errorf("sync: code sweep: %w", err)
		}
		for j, blob := range blobs {
			if err := bl.Code(batch[j], blob); err != nil {
				return err
			}
		}
		s.codes.Add(uint64(len(blobs)))
	}
	return nil
}

// incKey returns key+1 (fresh slice), or nil on overflow past 0xff...ff.
func incKey(key []byte) []byte {
	next := bytes.Clone(key)
	for i := len(next) - 1; i >= 0; i-- {
		next[i]++
		if next[i] != 0 {
			return next
		}
	}
	return nil
}
