package net

import (
	"fmt"
	"sync"

	corethcore "github.com/ava-labs/avalanchego/graft/coreth/core"
	"github.com/ava-labs/avalanchego/graft/coreth/core/extstate"
	cparams "github.com/ava-labs/avalanchego/graft/coreth/params"
	ccustomtypes "github.com/ava-labs/avalanchego/graft/coreth/plugin/evm/customtypes"
	"github.com/ava-labs/avalanchego/ids"
	proposerblock "github.com/ava-labs/avalanchego/vms/proposervm/block"
	ethtypes "github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
)

// RegisterExtras installs the avalanchego graft extras every C-chain
// state-touching code path depends on. It MUST run before any coreth/libevm
// type is decoded or executed (proven recipe from deforestationdb):
//   - corethcore registers the EVM hooks (pre-AP1 StateDB wrap, Shanghai
//     random handling),
//   - ccustomtypes installs the Avalanche header/body/account extras
//     (ExtDataHash, BlockGasCost, isMultiCoin), needed for correct block
//     hashes,
//   - cparams installs the Avalanche rules extras on *params.ChainConfig,
//   - extstate installs the state-key normalization hook (multi-coin slots).
//
// Idempotent after the first call.
var RegisterExtras = sync.OnceFunc(func() {
	corethcore.RegisterExtras()
	ccustomtypes.Register()
	cparams.RegisterExtras()
	extstate.RegisterExtras()
})

// Container is a parsed C-chain consensus container: the ProposerVM wrapper
// identity plus the inner eth block. Pre-ProposerVM containers are the raw
// eth block and the container ID equals the eth block hash.
type Container struct {
	ID       ids.ID
	ParentID ids.ID
	Eth      *ethtypes.Block
	Bytes    []byte
}

// ParseContainer decodes a raw container (ProposerVM-wrapped or pre-fork eth).
func ParseContainer(raw []byte) (*Container, error) {
	RegisterExtras()
	if len(raw) == 0 {
		return nil, fmt.Errorf("net: empty container")
	}
	if proposerBlk, err := proposerblock.ParseWithoutVerification(raw); err == nil {
		inner := new(ethtypes.Block)
		if err := rlp.DecodeBytes(proposerBlk.Block(), inner); err != nil {
			return nil, fmt.Errorf("net: decode inner eth block: %w", err)
		}
		return &Container{
			ID:       proposerBlk.ID(),
			ParentID: proposerBlk.ParentID(),
			Eth:      inner,
			Bytes:    raw,
		}, nil
	}
	// Pre-ProposerVM: raw bytes are the RLP eth block, container ID is the
	// eth block hash. Only reachable when following ancient history.
	inner := new(ethtypes.Block)
	if err := rlp.DecodeBytes(raw, inner); err != nil {
		return nil, fmt.Errorf("net: decode pre-fork eth block: %w", err)
	}
	return &Container{
		ID:       ids.ID(inner.Hash()),
		ParentID: ids.ID(inner.ParentHash()),
		Eth:      inner,
		Bytes:    raw,
	}, nil
}
