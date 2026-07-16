package sync

import (
	"context"
	"errors"
	"fmt"
	mrand "math/rand/v2"
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
	errTimeout          = errors.New("sync: request timed out")
)

// badPeerCooldown keeps a peer out of rotation after a timeout or AppError:
// non-serving peers (firewood scheme, throttlers) otherwise burn a 10s
// in-flight slot on every visit.
const badPeerCooldown = 3 * time.Minute

// NetClient adapts follower/net to the coreth sync client's Network
// interface: synchronous AppRequest exchange. Legacy coreth sync handlers
// only accept EVEN request IDs (odd IDs route to the peer's SDK network).
//
// A global in-flight cap converts over-subscription into local queueing:
// peers throttle a zero-weight non-validator hard, and thousands of
// outstanding requests degenerate into timeout churn (measured live: the
// giant-trie fan-out at ~4000 outstanding collapsed to <20 req/s).
type NetClient struct {
	net     *net.Network
	reqID   atomic.Uint32 // doubled for even IDs
	sem     chan struct{}
	perPeer int

	timeouts atomic.Uint64

	mu      gosync.Mutex
	pending map[uint32]chan appReply

	peerMu      gosync.Mutex
	outstanding map[ids.NodeID]int
	badUntil    map[ids.NodeID]time.Time
}

// perPeer bounds outstanding requests per peer: hundreds of concurrent
// callers through the tracker's SelectPeer all pile onto the few "best"
// peers, which then throttle us (measured: 8x slowdown). Random spread over
// every connected peer with a small per-peer cap keeps all of them busy.

type appReply struct {
	bytes  []byte
	failed bool
}

// NewNetClient wraps the network. Wire Callbacks.AppResponse to
// OnAppResponse when dialing. inflight <= 0 defaults to 320; perPeer <= 0
// defaults to 6.
func NewNetClient(n *net.Network, inflight, perPeer int) *NetClient {
	if inflight <= 0 {
		inflight = 320
	}
	if perPeer <= 0 {
		perPeer = 6
	}
	return &NetClient{
		net:         n,
		sem:         make(chan struct{}, inflight),
		perPeer:     perPeer,
		pending:     make(map[uint32]chan appReply),
		outstanding: make(map[ids.NodeID]int),
		badUntil:    make(map[ids.NodeID]time.Time),
	}
}

// Timeouts reports the total number of timed-out requests.
func (c *NetClient) Timeouts() uint64 { return c.timeouts.Load() }

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

// SendSyncedAppRequestAny sends to a random connected peer with capacity.
// The minVersion filter is ignored: every current mainnet peer far exceeds
// the sync client's minimum.
func (c *NetClient) SendSyncedAppRequestAny(ctx context.Context, _ *version.Application, request []byte) ([]byte, ids.NodeID, error) {
	nodeID, ok := c.pickPeer()
	if !ok {
		return nil, ids.EmptyNodeID, errNoPeer
	}
	defer c.releasePeer(nodeID)
	resp, err := c.SendSyncedAppRequest(ctx, nodeID, request)
	if errors.Is(err, errTimeout) || errors.Is(err, errAppRequestFailed) {
		c.peerMu.Lock()
		c.badUntil[nodeID] = time.Now().Add(badPeerCooldown)
		c.peerMu.Unlock()
	}
	return resp, nodeID, err
}

func (c *NetClient) pickPeer() (ids.NodeID, bool) {
	peers := c.net.ConnectedPeers()
	if len(peers) == 0 {
		return ids.EmptyNodeID, false
	}
	c.peerMu.Lock()
	defer c.peerMu.Unlock()
	now := time.Now()
	off := mrand.IntN(len(peers))
	var fallback ids.NodeID
	var haveFallback bool
	for i := range peers {
		id := peers[(off+i)%len(peers)]
		if c.outstanding[id] >= c.perPeer {
			continue
		}
		if now.Before(c.badUntil[id]) {
			if !haveFallback {
				fallback, haveFallback = id, true
			}
			continue
		}
		c.outstanding[id]++
		return id, true
	}
	if haveFallback { // everyone healthy is saturated; revisit a bad peer
		c.outstanding[fallback]++
		return fallback, true
	}
	return ids.EmptyNodeID, false
}

func (c *NetClient) releasePeer(id ids.NodeID) {
	c.peerMu.Lock()
	if c.outstanding[id] <= 1 {
		delete(c.outstanding, id)
	} else {
		c.outstanding[id]--
	}
	c.peerMu.Unlock()
}

// SendSyncedAppRequest sends one AppRequest and blocks for its response.
// Every call issues exactly one RegisterRequest; the sync client's
// TrackBandwidth call afterwards records the paired response/failure.
func (c *NetClient) SendSyncedAppRequest(ctx context.Context, nodeID ids.NodeID, request []byte) ([]byte, error) {
	select {
	case c.sem <- struct{}{}:
		defer func() { <-c.sem }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
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
		c.timeouts.Add(1)
		return nil, fmt.Errorf("%w: request %d to %s", errTimeout, id, nodeID)
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
