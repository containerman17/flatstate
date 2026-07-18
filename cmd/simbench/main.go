// Command simbench is the bot-view benchmark (DESIGN.md D10, D16): a second
// process that opens the live LMDB read-only, follows the tip through the
// tipbus, and hammers real mainnet contracts with libevm simulations at the
// live tip. It never writes anything; the follower is not touched.
//
// Workloads (verified deployed C-chain contracts):
//   - USDC balanceOf (proxy + delegatecall, the classic ERC20 read)
//   - Trader Joe V2-style pair getReserves + router getAmountOut
//   - Uniswap V3 WAVAX/USDC 0.05% pool quote via QuoterV2 (tick walking);
//     the pool address is discovered through the simulator itself
//     (factory.getPool), i.e. from the store, not hardcoded.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ava-labs/libevm/common"
	"github.com/holiman/uint256"

	"github.com/containerman17/flatstate/engine"
	"github.com/containerman17/flatstate/mem"
	"github.com/containerman17/flatstate/sim"
	"github.com/containerman17/flatstate/store"
	"github.com/containerman17/flatstate/tipbus"
)

var (
	wavax        = common.HexToAddress("0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7")
	usdc         = common.HexToAddress("0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E")
	joePair      = common.HexToAddress("0xf4003F4efBE8691B60249E6afbD307aBE7758adb") // WAVAX/USDC
	joeRouter    = common.HexToAddress("0x60aE616a2155Ee3d9A68541Ba4544862310933d4") // JoeRouter02
	univ3Factory = common.HexToAddress("0x740b1c1de25031C31FF4fC9A62f554A55cdC1baD")
	quoterV2     = common.HexToAddress("0xbe0F5544EC67e9B3b2D979aaA43f18Fd87E6257F")
	botAddr      = common.HexToAddress("0x1111111111111111111111111111111111111111")
)

func sel(b4 uint32) []byte {
	return []byte{byte(b4 >> 24), byte(b4 >> 16), byte(b4 >> 8), byte(b4)}
}

func word(dst []byte, v *uint256.Int) []byte {
	b := v.Bytes32()
	return append(dst, b[:]...)
}

func addrWord(dst []byte, a common.Address) []byte {
	var b [32]byte
	copy(b[12:], a[:])
	return append(dst, b[:]...)
}

func balanceOfCall(token, holder common.Address) *sim.Call {
	return &sim.Call{From: botAddr, To: token, Input: addrWord(sel(0x70a08231), holder)}
}

func getReservesCall() *sim.Call {
	return &sim.Call{From: botAddr, To: joePair, Input: sel(0x0902f1ac)}
}

func getAmountOutCall(amountIn, rIn, rOut *uint256.Int) *sim.Call {
	in := sel(0x054d50d4)
	in = word(in, amountIn)
	in = word(in, rIn)
	in = word(in, rOut)
	return &sim.Call{From: botAddr, To: joeRouter, Input: in}
}

func getPoolCall(a, b common.Address, fee uint64) *sim.Call {
	in := sel(0x1698ee82)
	in = addrWord(in, a)
	in = addrWord(in, b)
	in = word(in, uint256.NewInt(fee))
	return &sim.Call{From: botAddr, To: univ3Factory, Input: in}
}

func quoteCall(amountIn *uint256.Int) *sim.Call {
	// quoteExactInputSingle((tokenIn,tokenOut,amountIn,fee,sqrtPriceLimitX96))
	in := sel(0xc6a5026a)
	in = addrWord(in, wavax)
	in = addrWord(in, usdc)
	in = word(in, amountIn)
	in = word(in, uint256.NewInt(500))
	in = word(in, uint256.NewInt(0))
	return &sim.Call{From: botAddr, To: quoterV2, Input: in}
}

func retWord(r *sim.Result, i int) *uint256.Int {
	if len(r.ReturnData) < (i+1)*32 {
		return uint256.NewInt(0)
	}
	return new(uint256.Int).SetBytes(r.ReturnData[i*32 : (i+1)*32])
}

// applyStats tracks write-phase overhead as seen by this reader process.
type applyStats struct {
	mu      sync.Mutex
	applies int
	total   time.Duration
	max     time.Duration
}

func (a *applyStats) add(d time.Duration) {
	a.mu.Lock()
	a.applies++
	a.total += d
	a.max = max(a.max, d)
	a.mu.Unlock()
}

func (a *applyStats) String() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.applies == 0 {
		return "no block events"
	}
	return fmt.Sprintf("%d events, mean %v, max %v", a.applies, a.total/time.Duration(a.applies), a.max)
}

// follow applies tipbus events to the mem state forever.
func follow(bus *tipbus.Bus, st *mem.State, from uint64, stats *applyStats, blocks *atomic.Uint64) {
	cur := from
	for {
		seq, err := bus.Seq()
		if err != nil {
			log.Fatalf("tipbus seq: %v", err)
		}
		if seq == cur {
			time.Sleep(200 * time.Microsecond)
			continue
		}
		events, next, err := bus.Poll(cur)
		if err != nil {
			log.Fatalf("tipbus poll: %v", err)
		}
		for _, ev := range events {
			t0 := time.Now()
			switch ev.Kind {
			case tipbus.EvBlock:
				st.ApplyBlock(ev.Batch)
				blocks.Add(1)
			case tipbus.EvFinalize:
				if err := st.Finalize(ev.Height, ev.Hash); err != nil {
					// Raced the handshake; resync the whole unfinalized stack.
					log.Printf("finalize %d: %v; resyncing", ev.Height, err)
					_, layers, seq2, err2 := bus.Handshake()
					if err2 != nil {
						log.Fatalf("resync: %v", err2)
					}
					st.PreferenceReset(layers)
					next = seq2
				}
			case tipbus.EvReset:
				st.PreferenceReset(ev.Batches)
			}
			stats.add(time.Since(t0))
		}
		cur = next
	}
}

// runSingle drives batch-of-1 calls for dur and reports latency percentiles.
func runSingle(eng *engine.Engine, call *sim.Call, dur time.Duration) (p50, p99, mean time.Duration, rate float64) {
	calls := []any{call}
	lats := make([]time.Duration, 0, 1<<20)
	deadline := time.Now().Add(dur)
	for time.Now().Before(deadline) {
		t0 := time.Now()
		r := eng.Execute(calls)[0].(*sim.Result)
		lats = append(lats, time.Since(t0))
		if r.Err != nil {
			log.Fatalf("single-thread call failed: %v", r.Err)
		}
	}
	slices.Sort(lats)
	var tot time.Duration
	for _, l := range lats {
		tot += l
	}
	n := len(lats)
	return lats[n/2], lats[n*99/100], tot / time.Duration(n), float64(n) / dur.Seconds()
}

// runParallel drives nWorkers goroutines, each submitting batches of
// batchSize, for dur; returns total calls/sec.
func runParallel(eng *engine.Engine, call *sim.Call, nWorkers, batchSize int, dur time.Duration) float64 {
	var total atomic.Uint64
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < nWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			calls := make([]any, batchSize)
			for j := range calls {
				calls[j] = call
			}
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, r := range eng.Execute(calls) {
					if err := r.(*sim.Result).Err; err != nil {
						log.Fatalf("parallel call failed: %v", err)
					}
				}
				total.Add(uint64(batchSize))
			}
		}()
	}
	time.Sleep(dur)
	close(stop)
	wg.Wait()
	return float64(total.Load()) / dur.Seconds()
}

func mustOK(name string, r *sim.Result) *sim.Result {
	if r.Err != nil {
		log.Fatalf("%s: %v (return %x)", name, r.Err, r.ReturnData)
	}
	return r
}

func main() {
	var (
		dbPath  = flag.String("db", "", "main LMDB env path (read-only)")
		busPath = flag.String("tipbus", "", "tipbus env path")
		dur     = flag.Duration("dur", 8*time.Second, "duration of each measured phase")
		workers = flag.Int("workers", runtime.NumCPU(), "parallel submitter goroutines")
		batch   = flag.Int("batch", 10, "calls per batch in the parallel phase")
	)
	flag.Parse()
	if *dbPath == "" || *busPath == "" {
		fmt.Fprintln(os.Stderr, "usage: simbench -db <lmdb path> -tipbus <tipbus path>")
		os.Exit(2)
	}

	db, err := store.OpenReadOnly(*dbPath)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	defer db.Close()
	bus, err := tipbus.OpenReader(*busPath)
	if err != nil {
		log.Fatalf("tipbus: %v", err)
	}
	defer bus.Close()
	st, err := mem.New(db)
	if err != nil {
		log.Fatal(err)
	}

	finalized, layers, seq, err := bus.Handshake()
	if err != nil {
		log.Fatalf("handshake: %v", err)
	}
	for _, l := range layers {
		st.ApplyBlock(l)
	}
	log.Printf("handshake: finalized %d, %d unfinalized layers, seq %d", finalized, len(layers), seq)
	if len(layers) == 0 {
		log.Printf("no unfinalized layers yet; waiting for the first live block")
	}

	stats := &applyStats{}
	var liveBlocks atomic.Uint64
	go follow(bus, st, seq, stats, &liveBlocks)

	// Wait until a tip exists (TipInfo needs at least one applied block).
	for st.TipHash() == [32]byte{} {
		time.Sleep(10 * time.Millisecond)
	}

	execs, err := sim.NewPool(runtime.NumCPU())
	if err != nil {
		log.Fatal(err)
	}
	eng := engine.New(st, execs)

	// --- correctness pass: one of each, decoded ---
	poolWord := retWord(mustOK("getPool",
		eng.Execute([]any{getPoolCall(wavax, usdc, 500)})[0].(*sim.Result)), 0).Bytes32()
	pool := common.BytesToAddress(poolWord[12:])
	log.Printf("discovered UniV3 WAVAX/USDC 0.05%% pool via sim: %s", pool)

	rBal := mustOK("usdc balanceOf", eng.Execute([]any{balanceOfCall(usdc, joePair)})[0].(*sim.Result))
	bal := retWord(rBal, 0)
	log.Printf("USDC.balanceOf(joePair) = %s (%d gas)", bal, rBal.GasUsed)

	rRes := mustOK("getReserves", eng.Execute([]any{getReservesCall()})[0].(*sim.Result))
	r0, r1 := retWord(rRes, 0), retWord(rRes, 1)
	oneAvax := uint256.MustFromDecimal("1000000000000000000")
	rOut := mustOK("getAmountOut", eng.Execute([]any{getAmountOutCall(oneAvax, r0, r1)})[0].(*sim.Result))
	v2Out := retWord(rOut, 0)
	log.Printf("JoePair reserves = %s WAVAX / %s USDC; V2 out for 1 AVAX = %s (%d gas)",
		r0, r1, v2Out, rRes.GasUsed+rOut.GasUsed)

	rQ := mustOK("v3 quote", eng.Execute([]any{quoteCall(oneAvax)})[0].(*sim.Result))
	v3Out := retWord(rQ, 0)
	log.Printf("UniV3 QuoterV2 out for 1 AVAX = %s (%d gas used, quoter estimate %s)",
		v3Out, rQ.GasUsed, retWord(rQ, 3))
	if v2Out.IsZero() || v3Out.IsZero() {
		log.Fatal("zero quote from a live pool: state is wrong")
	}

	// --- benchmarks ---
	type workload struct {
		name string
		call *sim.Call
	}
	workloads := []workload{
		{"usdc_balanceOf", balanceOfCall(usdc, joePair)},
		{"v2_getReserves", getReservesCall()},
		{"v2_getAmountOut", getAmountOutCall(oneAvax, r0, r1)},
		{"v3_quote", quoteCall(oneAvax)},
	}
	basePins := st.PinsMerged()
	baseReq := eng.Requeues()
	baseBlocks := liveBlocks.Load()
	t0 := time.Now()

	fmt.Printf("\n%-16s %10s %10s %10s %12s %14s\n", "workload", "p50", "p99", "mean", "1-thread/s", "all-cores/s")
	for _, w := range workloads {
		// warm pins + jumpdest analysis on every pool executor
		warm := make([]any, runtime.NumCPU()*2)
		for i := range warm {
			warm[i] = w.call
		}
		eng.Execute(warm)
		p50, p99, mean, single := runSingle(eng, w.call, *dur)
		multi := runParallel(eng, w.call, *workers, *batch, *dur)
		fmt.Printf("%-16s %10v %10v %10v %12.0f %14.0f\n", w.name, p50, p99, mean, single, multi)
	}

	elapsed := time.Since(t0)
	fmt.Printf("\nlive tip during run: %d block events in %v (%.2f/s)\n",
		liveBlocks.Load()-baseBlocks, elapsed.Round(time.Second), float64(liveBlocks.Load()-baseBlocks)/elapsed.Seconds())
	fmt.Printf("write-phase (ApplyBlock/Finalize/Reset in this process): %s\n", stats.String())
	fmt.Printf("stale-batch requeues: %d\n", eng.Requeues()-baseReq)
	fmt.Printf("side-buffer pins merged: %d during benchmarks (%d total incl. warmup)\n",
		st.PinsMerged()-basePins, st.PinsMerged())
}
