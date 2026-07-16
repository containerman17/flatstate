package net

import (
	"math/big"
	"testing"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/message"
	avap2p "github.com/ava-labs/avalanchego/network/p2p"
	"github.com/ava-labs/avalanchego/proto/pb/p2p"
	"github.com/ava-labs/avalanchego/snow/validators"
	avaconstants "github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/avalanchego/utils/set"
	ethtypes "github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
	"github.com/prometheus/client_golang/prometheus"
)

func node(b byte) ids.NodeID { return ids.BuildTestNodeID([]byte{b}) }

func TestReconcileValidators(t *testing.T) {
	m := validators.NewManager()
	sub := avaconstants.PrimaryNetworkID
	if err := reconcileValidators(m, map[ids.NodeID]uint64{node(1): 10, node(2): 20, node(4): 0}); err != nil {
		t.Fatal(err)
	}
	if w := m.GetWeight(sub, node(1)); w != 10 {
		t.Fatalf("node1 weight %d", w)
	}
	if w := m.GetWeight(sub, node(4)); w != 0 {
		t.Fatalf("zero-weight validator added: %d", w)
	}
	// Update: node1 grows, node2 leaves, node3 joins.
	if err := reconcileValidators(m, map[ids.NodeID]uint64{node(1): 15, node(3): 5}); err != nil {
		t.Fatal(err)
	}
	if w := m.GetWeight(sub, node(1)); w != 15 {
		t.Fatalf("node1 weight after grow %d", w)
	}
	if w := m.GetWeight(sub, node(2)); w != 0 {
		t.Fatalf("node2 not removed: %d", w)
	}
	if w := m.GetWeight(sub, node(3)); w != 5 {
		t.Fatalf("node3 weight %d", w)
	}
	if total, err := m.TotalWeight(sub); err != nil || total != 20 {
		t.Fatalf("total %d err %v", total, err)
	}
	// Shrink weight.
	if err := reconcileValidators(m, map[ids.NodeID]uint64{node(1): 1, node(3): 5}); err != nil {
		t.Fatal(err)
	}
	if w := m.GetWeight(sub, node(1)); w != 1 {
		t.Fatalf("node1 weight after shrink %d", w)
	}
}

func TestParseBootstrapPeer(t *testing.T) {
	id := node(7)
	nodeID, addr, err := parseBootstrapPeer(id.String() + "@1.2.3.4:9651")
	if err != nil || nodeID != id || addr.String() != "1.2.3.4:9651" {
		t.Fatalf("got %v %v %v", nodeID, addr, err)
	}
	if _, _, err := parseBootstrapPeer("garbage"); err == nil {
		t.Fatal("want error")
	}
}

// TestParseContainerPreFork round-trips an RLP eth block as a pre-ProposerVM
// container. (ProposerVM containers can only be parsed, not minted, without
// a staking key; that path is exercised live.)
func TestParseContainerPreFork(t *testing.T) {
	RegisterExtras()
	header := &ethtypes.Header{
		Number:     big.NewInt(42),
		ParentHash: ethtypes.EmptyRootHash,
		GasLimit:   8_000_000,
		BaseFee:    big.NewInt(1),
		Difficulty: big.NewInt(1),
	}
	blk := ethtypes.NewBlockWithHeader(header)
	raw, err := rlp.EncodeToBytes(blk)
	if err != nil {
		t.Fatal(err)
	}
	c, err := ParseContainer(raw)
	if err != nil {
		t.Fatal(err)
	}
	if c.Eth.NumberU64() != 42 {
		t.Fatalf("number %d", c.Eth.NumberU64())
	}
	if c.ID != ids.ID(blk.Hash()) || c.ParentID != ids.ID(blk.ParentHash()) {
		t.Fatalf("ids mismatch")
	}
	if _, err := ParseContainer(nil); err == nil {
		t.Fatal("empty container should error")
	}
}

func TestHandlerRouting(t *testing.T) {
	var (
		gotContainer []byte
		gotChits     *p2p.Chits
		gotAnc       [][]byte
	)
	cb := Callbacks{
		Container: func(_ ids.NodeID, c []byte) { gotContainer = c },
		Chits: func(_ ids.NodeID, reqID uint32, preferred, atHeight, accepted ids.ID, height uint64) {
			gotChits = &p2p.Chits{
				RequestId:           reqID,
				PreferredId:         preferred[:],
				PreferredIdAtHeight: atHeight[:],
				AcceptedId:          accepted[:],
				AcceptedHeight:      height,
			}
		},
		Ancestors: func(_ ids.NodeID, _ uint32, cs [][]byte) { gotAnc = cs },
	}
	tracker, err := avap2p.NewPeerTracker(logging.NoLog{}, "t", prometheus.NewRegistry(), set.Set[ids.NodeID]{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := newHandler(cb, tracker)

	h.HandleInbound(t.Context(), &message.InboundMessage{
		NodeID: node(1), Op: message.PutOp,
		Message: &p2p.Put{Container: []byte{1, 2}},
	})
	if string(gotContainer) != "\x01\x02" {
		t.Fatal("put not routed")
	}
	h.HandleInbound(t.Context(), &message.InboundMessage{
		NodeID: node(1), Op: message.PushQueryOp,
		Message: &p2p.PushQuery{Container: []byte{3}},
	})
	if string(gotContainer) != "\x03" {
		t.Fatal("push query container not routed")
	}
	pref, acc := ids.ID{1}, ids.ID{2}
	h.HandleInbound(t.Context(), &message.InboundMessage{
		NodeID: node(2), Op: message.ChitsOp,
		Message: &p2p.Chits{RequestId: 9, PreferredId: pref[:], PreferredIdAtHeight: pref[:], AcceptedId: acc[:], AcceptedHeight: 5},
	})
	if gotChits == nil || gotChits.RequestId != 9 || gotChits.AcceptedHeight != 5 {
		t.Fatalf("chits not routed: %+v", gotChits)
	}
	h.HandleInbound(t.Context(), &message.InboundMessage{
		NodeID: node(2), Op: message.AncestorsOp,
		Message: &p2p.Ancestors{RequestId: 3, Containers: [][]byte{{9}}},
	})
	if len(gotAnc) != 1 {
		t.Fatal("ancestors not routed")
	}

	// Connected/Disconnected bookkeeping.
	h.Connected(node(3), nil, ids.Empty)
	if !h.isConnected(node(3)) || h.numConnected() != 1 {
		t.Fatal("connected bookkeeping")
	}
	h.Disconnected(node(3))
	if h.isConnected(node(3)) {
		t.Fatal("disconnected bookkeeping")
	}
}
