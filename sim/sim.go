// Package sim is the libevm-backed simulation executor (DESIGN.md D14, D16):
// eth_call-style contract simulation against the tip through the engine's
// batch discipline. Reads resolve overlay -> unfinalized layers -> base map
// -> LMDB pin (D8/D9); all writes go through the journaled per-call overlay
// and are discarded after the call.
//
// PROCESS ISOLATION (hard rule): sim must never share a process with
// follower/exec or anything else that calls corethcore.RegisterExtras. That
// registration installs vm hooks whose OverrideNewEVMArgs type-asserts the
// concrete *state.StateDB, which panics on sim's custom vm.StateDB. sim
// registers only the params extras it needs (ChainConfig, once). The D10
// topology already keeps the follower and bot processes separate; this is
// one more reason it must stay that way.
//
// Known deviations from full-node semantics, all irrelevant at the tip for
// normal contracts and accepted deliberately:
//   - BLOCKHASH returns the zero hash (no header history in bot processes).
//   - Pre-AP1 GetCommittedState quirks are not reproduced. Multicoin
//     state-key normalization IS reproduced (the store is keyed by
//     normalized slots, see statedb.go normSlot), but multicoin balance
//     opcodes themselves are not.
//   - GasUsed excludes intrinsic gas (evm.Call is below ApplyMessage).
//   - GASLIMIT reports a fixed constant; the batch carries no gas limit.
package sim

import (
	"errors"
	"fmt"
	"math/big"
	"sync"

	cparams "github.com/ava-labs/avalanchego/graft/coreth/params"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core"
	ethtypes "github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/core/vm"
	"github.com/ava-labs/libevm/params"
	"github.com/holiman/uint256"

	"github.com/containerman17/flatstate/chaincfg"
	"github.com/containerman17/flatstate/engine"
	"github.com/containerman17/flatstate/schema"
)

// DefaultGasCap bounds a call that does not set its own gas (same order as
// coreth's RPC gas cap).
const DefaultGasCap = 50_000_000

// blockGasLimit feeds the GASLIMIT opcode only. ponytail: fixed constant,
// wire the real limit through capture.Batch if a bot ever cares.
const blockGasLimit = 30_000_000

// blackholeAddr is the C-chain coinbase.
var blackholeAddr = common.Address{0x01}

var (
	errNoTip           = errors.New("sim: no tip applied yet, refusing to simulate (D13)")
	errRefundUnderflow = errors.New("sim: refund counter underflow")
)

// Call is one eth_call-style simulation request. Overrides are applied
// before execution and are visible as committed state (D14: storage and
// balance overrides only).
type Call struct {
	From  common.Address
	To    common.Address
	Input []byte
	Gas   uint64       // 0 = DefaultGasCap
	Value *uint256.Int // nil = 0

	BalanceOverrides map[common.Address]*uint256.Int
	StorageOverrides map[common.Address]map[common.Hash]common.Hash
	AccessList       ethtypes.AccessList // optional pre-warmed access list
}

// Result is the outcome of one simulated call.
type Result struct {
	ReturnData []byte
	GasUsed    uint64
	Logs       []*ethtypes.Log
	Err        error // vm error (revert, OOG, ...) or a fail-loud state read error
}

// Reverted reports an execution revert; ReturnData then carries the reason.
func (r *Result) Reverted() bool { return errors.Is(r.Err, vm.ErrExecutionReverted) }

var registerOnce = sync.OnceFunc(func() {
	cparams.RegisterExtras()
})

// ChainConfig returns the mainnet C-chain config for simulation, registering
// the params extras on first use. See the package comment for why this must
// not run in a process that also registered coreth's vm hooks.
func ChainConfig() (*params.ChainConfig, error) {
	registerOnce()
	cfg, _, err := chaincfg.Mainnet()
	return cfg, err
}

// Executor is a long-lived engine.Executor over libevm (D14): pooled by the
// engine, reset per call, never reallocated. Not concurrency-safe; the
// engine's pool guarantees single-threaded use.
type Executor struct {
	chainCfg *params.ChainConfig
	sdb      *stateDB

	// per-tip environment, rebuilt only when the tip hash moves (never a
	// racy global: each executor owns its own copy, D14)
	envTip      schema.Hash
	evm         *vm.EVM
	rules       params.Rules
	precompiles []common.Address

	// JUMPDEST analysis is shared across calls through a persistent contract
	// object: vm.NewContract inherits the jumpdests map from a *vm.Contract
	// caller, so every callee analysis lands in root's map and persists.
	root    *vm.Contract
	callers map[common.Address]*vm.Contract
}

// New builds one executor. Executors sharing a pool may (and should) share
// cfg; it is read-only after construction.
func New(cfg *params.ChainConfig) *Executor {
	root := vm.NewContract(vm.AccountRef{}, vm.AccountRef{}, new(uint256.Int), 0)
	return &Executor{
		chainCfg: cfg,
		sdb:      newStateDB(),
		root:     root,
		callers:  make(map[common.Address]*vm.Contract),
	}
}

// NewPool builds n executors sharing the mainnet chain config, ready for
// engine.New.
func NewPool(n int) ([]engine.Executor, error) {
	cfg, err := ChainConfig()
	if err != nil {
		return nil, err
	}
	execs := make([]engine.Executor, n)
	for i := range execs {
		execs[i] = New(cfg)
	}
	return execs, nil
}

// callerFor returns the persistent caller contract for a from-address; its
// Address() feeds CALLER and jumpdests feed the shared analysis cache.
func (e *Executor) callerFor(from common.Address) *vm.Contract {
	if c, ok := e.callers[from]; ok {
		return c
	}
	c := vm.NewContract(e.root, vm.AccountRef(from), new(uint256.Int), 0)
	e.callers[from] = c
	return c
}

// rebuildEnv rebuilds the per-tip block context, rules, and EVM. Mirrors the
// two things coreth's vm hook would have done for a tip block: Random takes
// the pre-merge difficulty (1) and Difficulty becomes 0 (Shanghai+ jump
// table via Random != nil).
func (e *Executor) rebuildEnv(tip schema.Hash, num, time uint64) {
	n := new(big.Int).SetUint64(num)
	random := common.BigToHash(big.NewInt(1))
	blockCtx := vm.BlockContext{
		CanTransfer: core.CanTransfer,
		Transfer:    core.Transfer,
		GetHash:     func(uint64) common.Hash { return common.Hash{} }, // BLOCKHASH unsupported in sim
		Coinbase:    blackholeAddr,
		GasLimit:    blockGasLimit,
		BlockNumber: n,
		Time:        time,
		Difficulty:  new(big.Int),
		BaseFee:     new(big.Int),
		BlobBaseFee: new(big.Int),
		Random:      &random,
	}
	e.evm = vm.NewEVM(blockCtx, vm.TxContext{GasPrice: new(big.Int)}, e.sdb, e.chainCfg, vm.Config{NoBaseFee: true})
	e.rules = e.chainCfg.Rules(n, true, time)
	e.precompiles = vm.ActivePrecompiles(e.rules)
	e.envTip = tip
}

// Execute implements engine.Executor. c must be a *Call; the result is a
// *Result.
func (e *Executor) Execute(c any, v *engine.View) any {
	call, ok := c.(*Call)
	if !ok {
		return &Result{Err: fmt.Errorf("sim: call is %T, want *Call", c)}
	}
	tip, num, time := v.TipInfo()
	if num == 0 {
		return &Result{Err: errNoTip}
	}
	if e.evm == nil || e.envTip != tip {
		e.rebuildEnv(tip, num, time)
	}
	e.sdb.reset(v, call, e.evm)
	e.evm.Reset(vm.TxContext{Origin: call.From, GasPrice: new(big.Int)}, e.sdb)
	e.sdb.Prepare(e.rules, call.From, blackholeAddr, &call.To, e.precompiles, call.AccessList)

	gas := call.Gas
	if gas == 0 {
		gas = DefaultGasCap
	}
	value := call.Value
	if value == nil {
		value = new(uint256.Int)
	}
	ret, left, err := e.evm.Call(e.callerFor(call.From), call.To, call.Input, gas, value)
	if rerr := e.sdb.readErr; rerr != nil {
		// evm.Cancel is sticky; force a fresh EVM for the next call.
		e.envTip = schema.Hash{}
		e.evm = nil
		return &Result{Err: rerr}
	}
	res := &Result{GasUsed: gas - left, Err: err}
	// The return buffer and log slice alias per-executor memory reused by
	// the next call; copy before the worker goes back to the pool.
	if len(ret) > 0 {
		res.ReturnData = append([]byte(nil), ret...)
	}
	if len(e.sdb.logs) > 0 {
		res.Logs = append([]*ethtypes.Log(nil), e.sdb.logs...)
	}
	return res
}

var _ engine.Executor = (*Executor)(nil)
