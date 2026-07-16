package consensus

import (
	ethtypes "github.com/ava-labs/libevm/core/types"

	"github.com/containerman17/flatstate/capture"
	"github.com/containerman17/flatstate/schema"
)

// HeaderOnly is the follow-only Executor for dry runs without a baseline:
// it emits empty batches (hash/parent/height/time only) so consensus and
// the tracker run end to end while no state is touched.
type HeaderOnly struct{}

func (HeaderOnly) Execute(parent schema.Hash, blk *ethtypes.Block) (*capture.Batch, error) {
	return &capture.Batch{
		Block:  blk.NumberU64(),
		Hash:   schema.Hash(blk.Hash()),
		Parent: parent,
		Time:   blk.Time() * 1000,
	}, nil
}
