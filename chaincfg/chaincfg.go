// Package chaincfg assembles the mainnet C-chain chain config exactly the
// way coreth's parseGenesis does: genesis JSON + injected network upgrade
// schedule + Durango warp precompile + snow context for the warp payload.
// Shared by the follower's block executor (follower/exec) and the simulation
// executor (sim).
//
// Registration contract: callers MUST register the required params extras
// BEFORE calling Mainnet (cparams.RegisterExtras at minimum; the follower
// registers the full set via follower/net.RegisterExtras). This package has
// no registration side effects of its own.
package chaincfg

import (
	"encoding/json"
	"fmt"

	"github.com/ava-labs/avalanchego/genesis"
	corethcore "github.com/ava-labs/avalanchego/graft/coreth/core"
	cparams "github.com/ava-labs/avalanchego/graft/coreth/params"
	cextras "github.com/ava-labs/avalanchego/graft/coreth/params/extras"
	warpcontract "github.com/ava-labs/avalanchego/graft/coreth/precompile/contracts/warp"
	_ "github.com/ava-labs/avalanchego/graft/coreth/precompile/registry"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/avalanchego/upgrade"
	avaconstants "github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/libevm/params"
)

// MainnetAVAXAssetID is required by the atomic-tx state transfer to credit
// imported AVAX correctly.
const MainnetAVAXAssetID = "FvwEAhmxKfeiG8SnEvq42hc6whRyY3EFYAvebMqDNDGCgxN5Z"

// MainnetCChainID feeds the warp precompile's source chain ID; a wrong value
// would emit warp logs with a wrong payload and break the receipts check.
const MainnetCChainID = "2q9e4r6Mu3U68nU1fYjgbR6JvwrRx36CohpAX5UQxse55x1Q5"

// Mainnet builds the mainnet C-chain config and the snow context wired into
// its Avalanche extras (the warp precompile reads it from there).
func Mainnet() (*params.ChainConfig, *snow.Context, error) {
	cfg := genesis.GetConfig(avaconstants.MainnetID)
	var g corethcore.Genesis
	if err := json.Unmarshal([]byte(cfg.CChainGenesis), &g); err != nil {
		return nil, nil, fmt.Errorf("chaincfg: unmarshal C-chain genesis: %w", err)
	}
	// The genesis JSON carries no post-genesis upgrade schedule; the VM
	// injects it from the avalanchego runtime config before aligning the
	// eth upgrades (coreth parseGenesis). Without this, Durango/Etna never
	// activate, so Shanghai/Cancun stay off and PUSH0 is an invalid opcode:
	// every modern contract call burns its full gas limit.
	configExtra := cparams.GetExtra(g.Config)
	configExtra.NetworkUpgrades = cextras.GetNetworkUpgrades(upgrade.GetConfig(avaconstants.MainnetID))
	// Mirror parseGenesis: the Warp precompile activates at Durango; a tx
	// calling it would otherwise no-op into an empty address and diverge.
	if configExtra.DurangoBlockTimestamp != nil {
		configExtra.PrecompileUpgrades = append(configExtra.PrecompileUpgrades, cextras.PrecompileUpgrade{
			Config: warpcontract.NewDefaultConfig(configExtra.DurangoBlockTimestamp),
		})
	}
	if err := configExtra.Verify(); err != nil {
		return nil, nil, fmt.Errorf("chaincfg: invalid chain config: %w", err)
	}
	if err := cparams.SetEthUpgrades(g.Config); err != nil {
		return nil, nil, fmt.Errorf("chaincfg: set eth upgrades: %w", err)
	}
	avaxAssetID, err := ids.FromString(MainnetAVAXAssetID)
	if err != nil {
		return nil, nil, err
	}
	cChainID, err := ids.FromString(MainnetCChainID)
	if err != nil {
		return nil, nil, err
	}
	snowCtx := &snow.Context{
		NetworkID:   avaconstants.MainnetID,
		ChainID:     cChainID,
		AVAXAssetID: avaxAssetID,
	}
	// The warp precompile reads the snow context out of the chain config
	// extras (sendWarpMessage panics on nil, and the emitted message embeds
	// NetworkID and ChainID).
	configExtra.AvalancheContext = cextras.AvalancheContext{SnowCtx: snowCtx}
	return g.Config, snowCtx, nil
}
