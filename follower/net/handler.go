package net

import (
	"context"
	"sync"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/message"
	avap2p "github.com/ava-labs/avalanchego/network/p2p"
	"github.com/ava-labs/avalanchego/proto/pb/p2p"
	"github.com/ava-labs/avalanchego/utils/set"
	"github.com/ava-labs/avalanchego/version"
)

// Callbacks receive inbound consensus traffic. They are invoked from network
// goroutines; implementations must be concurrency-safe. Nil callbacks drop.
type Callbacks struct {
	// Container delivers a raw container from a Put (Get response, gossip)
	// or a PushQuery push.
	Container func(nodeID ids.NodeID, container []byte)
	// Chits delivers a poll response.
	Chits func(nodeID ids.NodeID, requestID uint32, preferred, preferredAtHeight, accepted ids.ID, acceptedHeight uint64)
	// Ancestors delivers a GetAncestors response (containers newest first).
	Ancestors func(nodeID ids.NodeID, requestID uint32, containers [][]byte)
}

type handler struct {
	cb      Callbacks
	tracker *avap2p.PeerTracker

	mu          sync.Mutex
	connected   set.Set[ids.NodeID]
	connectedCh chan ids.NodeID
}

func newHandler(cb Callbacks, tracker *avap2p.PeerTracker) *handler {
	return &handler{
		cb:          cb,
		tracker:     tracker,
		connectedCh: make(chan ids.NodeID, 64),
	}
}

func (h *handler) Connected(nodeID ids.NodeID, _ *version.Application, _ ids.ID) {
	h.tracker.Connected(nodeID, nil)
	h.mu.Lock()
	h.connected.Add(nodeID)
	h.mu.Unlock()
	select {
	case h.connectedCh <- nodeID:
	default:
	}
}

func (h *handler) Disconnected(nodeID ids.NodeID) {
	h.tracker.Disconnected(nodeID)
	h.mu.Lock()
	h.connected.Remove(nodeID)
	h.mu.Unlock()
}

func (h *handler) isConnected(nodeID ids.NodeID) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.connected.Contains(nodeID)
}

func (h *handler) numConnected() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.connected.Len()
}

func (h *handler) HandleInbound(_ context.Context, msg *message.InboundMessage) {
	defer msg.OnFinishedHandling()
	switch msg.Op {
	case message.PutOp:
		p, ok := msg.Message.(*p2p.Put)
		if ok && h.cb.Container != nil {
			h.cb.Container(msg.NodeID, p.Container)
		}
	case message.PushQueryOp:
		p, ok := msg.Message.(*p2p.PushQuery)
		if ok && h.cb.Container != nil {
			h.cb.Container(msg.NodeID, p.Container)
		}
	case message.ChitsOp:
		p, ok := msg.Message.(*p2p.Chits)
		if !ok || h.cb.Chits == nil {
			return
		}
		preferred, err := ids.ToID(p.PreferredId)
		if err != nil {
			return
		}
		preferredAtHeight, err := ids.ToID(p.PreferredIdAtHeight)
		if err != nil {
			preferredAtHeight = preferred
		}
		accepted, err := ids.ToID(p.AcceptedId)
		if err != nil {
			return
		}
		h.cb.Chits(msg.NodeID, p.RequestId, preferred, preferredAtHeight, accepted, p.AcceptedHeight)
	case message.AncestorsOp:
		p, ok := msg.Message.(*p2p.Ancestors)
		if ok && h.cb.Ancestors != nil {
			h.cb.Ancestors(msg.NodeID, p.RequestId, p.Containers)
		}
	}
}
