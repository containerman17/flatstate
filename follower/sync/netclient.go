package sync

import (
	"context"
	"errors"
	"fmt"
	gosync "sync"
	"sync/atomic"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	avap2p "github.com/ava-labs/avalanchego/network/p2p"
	"github.com/ava-labs/avalanchego/version"

	"github.com/containerman17/flatstate/follower/net"
)

var (
	errNoPeer           = errors.New("sync: no responsive peer available")
	errAppRequestFailed = errors.New("sync: peer answered with AppError")
)

// NetClient adapts follower/net to the coreth sync client's Network
// interface: synchronous AppRequest exchange. Legacy coreth sync handlers
// only accept EVEN request IDs (odd IDs route to the peer's SDK network).
type NetClient struct {
	net   *net.Network
	reqID atomic.Uint32 // doubled for even IDs

	mu      gosync.Mutex
	pending map[uint32]chan appReply
}

type appReply struct {
	bytes  []byte
	failed bool
}

// NewNetClient wraps the network. Wire Callbacks.AppResponse to
// OnAppResponse when dialing.
func NewNetClient(n *net.Network) *NetClient {
	return &NetClient{net: n, pending: make(map[uint32]chan appReply)}
}

// OnAppResponse routes an AppResponse / AppError to its waiting request.
func (c *NetClient) OnAppResponse(_ ids.NodeID, requestID uint32, response []byte, failed bool) {
	c.mu.Lock()
	ch := c.pending[requestID]
	delete(c.pending, requestID)
	c.mu.Unlock()
	if ch != nil {
		ch <- appReply{bytes: response, failed: failed}
	}
}

// SendSyncedAppRequestAny sends to a tracker-selected responsive peer.
// The minVersion filter is ignored: every current mainnet peer far exceeds
// the sync client's minimum.
func (c *NetClient) SendSyncedAppRequestAny(ctx context.Context, _ *version.Application, request []byte) ([]byte, ids.NodeID, error) {
	nodeID, ok := c.net.SelectPeer()
	if !ok {
		return nil, ids.EmptyNodeID, errNoPeer
	}
	resp, err := c.SendSyncedAppRequest(ctx, nodeID, request)
	return resp, nodeID, err
}

// SendSyncedAppRequest sends one AppRequest and blocks for its response.
// Every call issues exactly one RegisterRequest; the sync client's
// TrackBandwidth call afterwards records the paired response/failure.
func (c *NetClient) SendSyncedAppRequest(ctx context.Context, nodeID ids.NodeID, request []byte) ([]byte, error) {
	id := c.reqID.Add(1) * 2 // even IDs only
	ch := make(chan appReply, 1)
	c.mu.Lock()
	c.pending[id] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}()

	c.net.RegisterRequest(nodeID)
	if err := c.net.SendAppRequest(nodeID, id, request); err != nil {
		return nil, err
	}
	t := time.NewTimer(net.RequestTimeout)
	defer t.Stop()
	select {
	case r := <-ch:
		if r.failed {
			return nil, errAppRequestFailed
		}
		return r.bytes, nil
	case <-t.C:
		return nil, fmt.Errorf("sync: request %d to %s timed out", id, nodeID)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// TrackBandwidth completes the tracker pairing for the preceding request.
func (c *NetClient) TrackBandwidth(nodeID ids.NodeID, bandwidth float64) {
	if nodeID == ids.EmptyNodeID {
		return
	}
	if bandwidth > 0 {
		c.net.RegisterResponseBW(nodeID, bandwidth)
	} else {
		c.net.RegisterFailure(nodeID)
	}
}

// Sample implements p2p.NodeSampler.
func (c *NetClient) Sample(_ context.Context, limit int) []ids.NodeID {
	nodes, err := c.net.SampleValidators(limit)
	if err != nil {
		return nil
	}
	return nodes
}

// P2PNetwork is required by the interface but only used by Client.AddClient,
// which this loader never calls.
func (c *NetClient) P2PNetwork() *avap2p.Network { return nil }
