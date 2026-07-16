// Package net joins the Avalanche mainnet p2p network as a passive C-chain
// follower (DESIGN.md D2 rev 2): avalanchego's network stack with an
// ephemeral staking cert, validator set and weights from the public P-chain
// RPC (platform.getCurrentValidators), peer IPs from info.peers. It sends
// Get / GetAncestors / PullQuery and receives Put / PushQuery containers,
// Chits, and Ancestors. Recipe proven in deforestationdb blockfetcher.
package net

import (
	"context"
	"fmt"
	"log/slog"
	"net/netip"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ava-labs/avalanchego/api/info"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/message"
	"github.com/ava-labs/avalanchego/network"
	avap2p "github.com/ava-labs/avalanchego/network/p2p"
	avacommon "github.com/ava-labs/avalanchego/snow/engine/common"
	"github.com/ava-labs/avalanchego/snow/validators"
	"github.com/ava-labs/avalanchego/staking"
	"github.com/ava-labs/avalanchego/subnets"
	"github.com/ava-labs/avalanchego/utils/compression"
	avaconstants "github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/avalanchego/utils/set"
	"github.com/ava-labs/avalanchego/vms/platformvm"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	DefaultNodeURI         = "https://api.avax.network"
	DefaultRefreshInterval = 5 * time.Minute
	defaultConnectTimeout  = 30 * time.Second
	defaultPeerWarmup      = 5 * time.Second
	// RequestTimeout is the deadline advertised on outbound requests; the
	// caller owns actual timeout handling (there is no router here).
	RequestTimeout = 10 * time.Second
)

type Config struct {
	// NodeURI is a public node used only for bootstrap RPC (network ID,
	// C-chain ID, validator list, peer IPs). Empty = DefaultNodeURI.
	NodeURI string
	// RefreshInterval is how often the validator set, weights, and peer IPs
	// are re-fetched. <=0 = DefaultRefreshInterval.
	RefreshInterval time.Duration
	// BootstrapPeers are extra peers to track, "NodeID-...@ip:port".
	BootstrapPeers []string
	Callbacks      Callbacks
	Log            *slog.Logger
}

// Network is a live p2p connection to mainnet.
type Network struct {
	log     *slog.Logger
	chainID ids.ID
	net     network.Network
	creator message.Creator
	tracker *avap2p.PeerTracker
	handler *handler
	vdrs    validators.Manager

	reqID       atomic.Uint32
	dispatchErr chan error
	done        chan struct{}
}

// permissiveValidators answers yes to every membership check so our
// transient node accepts messages from any peer.
type permissiveValidators struct{ validators.Manager }

func (permissiveValidators) Contains(ids.ID, ids.NodeID) bool { return true }

// Dial connects: fetches the validator set, discovers peer IPs, joins the
// network with an ephemeral staking cert, and waits for the first peer.
func Dial(ctx context.Context, cfg Config) (*Network, error) {
	if cfg.NodeURI == "" {
		cfg.NodeURI = DefaultNodeURI
	}
	if cfg.RefreshInterval <= 0 {
		cfg.RefreshInterval = DefaultRefreshInterval
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}

	infoClient := info.NewClient(cfg.NodeURI)
	pClient := platformvm.NewClient(cfg.NodeURI)

	networkID, err := infoClient.GetNetworkID(ctx)
	if err != nil {
		return nil, fmt.Errorf("info.getNetworkID: %w", err)
	}
	chainID, err := infoClient.GetBlockchainID(ctx, "C")
	if err != nil {
		return nil, fmt.Errorf("info.getBlockchainID(C): %w", err)
	}

	vdrs := validators.NewManager()
	weights, err := fetchWeights(ctx, pClient)
	if err != nil {
		return nil, fmt.Errorf("platform.getCurrentValidators: %w", err)
	}
	if err := reconcileValidators(vdrs, weights); err != nil {
		return nil, err
	}
	cfg.Log.Info("validator set loaded", "validators", len(vdrs.GetValidatorIDs(avaconstants.PrimaryNetworkID)))

	tracker, err := avap2p.NewPeerTracker(logging.NoLog{}, "flatstate_follower", prometheus.NewRegistry(), set.Set[ids.NodeID]{}, nil)
	if err != nil {
		return nil, fmt.Errorf("NewPeerTracker: %w", err)
	}
	handler := newHandler(cfg.Callbacks, tracker)

	netCfg, err := network.NewTestNetworkConfig(
		prometheus.NewRegistry(),
		networkID,
		permissiveValidators{vdrs},
		set.Set[ids.ID]{},
	)
	if err != nil {
		return nil, fmt.Errorf("NewTestNetworkConfig: %w", err)
	}
	stakingCert, err := staking.ParseCertificate(netCfg.TLSConfig.Certificates[0].Leaf.Raw)
	if err != nil {
		return nil, fmt.Errorf("ParseCertificate: %w", err)
	}
	netCfg.MyNodeID = ids.NodeIDFromCert(stakingCert)

	avaNet, err := network.NewTestNetwork(logging.NoLog{}, prometheus.NewRegistry(), netCfg, handler)
	if err != nil {
		return nil, fmt.Errorf("NewTestNetwork: %w", err)
	}
	creator, err := message.NewCreator(prometheus.NewRegistry(), compression.TypeZstd, avaconstants.DefaultNetworkMaximumInboundTimeout)
	if err != nil {
		avaNet.StartClose()
		return nil, fmt.Errorf("message.NewCreator: %w", err)
	}

	n := &Network{
		log:         cfg.Log,
		chainID:     chainID,
		net:         avaNet,
		creator:     creator,
		tracker:     tracker,
		handler:     handler,
		vdrs:        vdrs,
		dispatchErr: make(chan error, 1),
		done:        make(chan struct{}),
	}

	go func() { n.dispatchErr <- avaNet.Dispatch() }()

	tracked, err := n.trackPeers(ctx, infoClient, vdrs.GetValidatorIDs(avaconstants.PrimaryNetworkID))
	if err != nil {
		avaNet.StartClose()
		return nil, err
	}
	for _, s := range cfg.BootstrapPeers {
		id, addr, err := parseBootstrapPeer(s)
		if err != nil {
			avaNet.StartClose()
			return nil, err
		}
		avaNet.ManuallyTrack(id, addr)
		tracked++
	}
	if tracked == 0 {
		avaNet.StartClose()
		return nil, fmt.Errorf("net: no peers to track (info.peers empty and no bootstrap peers)")
	}
	cfg.Log.Info("tracking peers", "peers", tracked, "node_id", netCfg.MyNodeID, "network_id", networkID, "c_chain", chainID)

	if err := n.waitForPeer(ctx); err != nil {
		avaNet.StartClose()
		return nil, err
	}
	warmup := time.NewTimer(defaultPeerWarmup)
	defer warmup.Stop()
	select {
	case <-warmup.C:
	case <-ctx.Done():
	}
	cfg.Log.Info("connected", "peers", handler.numConnected())

	go n.refreshLoop(cfg.RefreshInterval, infoClient, pClient)
	return n, nil
}

func (n *Network) Close() {
	close(n.done)
	n.net.StartClose()
}

// DispatchDone reports a fatal network-dispatch error.
func (n *Network) DispatchDone() <-chan error { return n.dispatchErr }

// NextRequestID returns a process-unique request ID.
func (n *Network) NextRequestID() uint32 { return n.reqID.Add(1) }

// SampleValidators samples k validators by stake weight (duplicates possible
// for heavy validators, matching the snowman engine's sampling).
func (n *Network) SampleValidators(k int) ([]ids.NodeID, error) {
	return n.vdrs.Sample(avaconstants.PrimaryNetworkID, k)
}

// IsConnected reports whether the peer is currently connected.
func (n *Network) IsConnected(nodeID ids.NodeID) bool { return n.handler.isConnected(nodeID) }

// NumConnected returns the number of connected peers.
func (n *Network) NumConnected() int { return n.handler.numConnected() }

// SelectPeer picks a responsive peer for a Get-style request.
func (n *Network) SelectPeer() (ids.NodeID, bool) { return n.tracker.SelectPeer() }

// RegisterRequest / RegisterResponse / RegisterFailure feed the peer tracker
// so SelectPeer prefers responsive peers.
func (n *Network) RegisterRequest(id ids.NodeID)  { n.tracker.RegisterRequest(id) }
func (n *Network) RegisterFailure(id ids.NodeID)  { n.tracker.RegisterFailure(id) }
func (n *Network) RegisterResponse(id ids.NodeID) { n.tracker.RegisterResponse(id, 1) }

// RegisterResponseBW records a response with a measured bandwidth so the
// tracker can rank peers (state-sync leaf downloads).
func (n *Network) RegisterResponseBW(id ids.NodeID, bandwidth float64) {
	n.tracker.RegisterResponse(id, bandwidth)
}

func (n *Network) send(msg *message.OutboundMessage, nodeIDs set.Set[ids.NodeID]) {
	n.net.Send(msg, avacommon.SendConfig{NodeIDs: nodeIDs}, avaconstants.PrimaryNetworkID, subnets.NoOpAllower)
}

// SendGet requests one container by ID.
func (n *Network) SendGet(nodeID ids.NodeID, requestID uint32, containerID ids.ID) error {
	msg, err := n.creator.Get(n.chainID, requestID, RequestTimeout, containerID)
	if err != nil {
		return err
	}
	n.send(msg, set.Of(nodeID))
	return nil
}

// SendGetAncestors requests a container and its ancestors (newest first).
func (n *Network) SendGetAncestors(nodeID ids.NodeID, requestID uint32, containerID ids.ID) error {
	msg, err := n.creator.GetAncestors(n.chainID, requestID, RequestTimeout, containerID, 0)
	if err != nil {
		return err
	}
	n.send(msg, set.Of(nodeID))
	return nil
}

// SendAppRequest sends a chain-scoped application request (C-chain state
// sync leaf/code exchange). Legacy coreth sync handlers only accept EVEN
// request IDs (odd IDs route to the peer's SDK network); callers must comply.
func (n *Network) SendAppRequest(nodeID ids.NodeID, requestID uint32, appBytes []byte) error {
	msg, err := n.creator.AppRequest(n.chainID, requestID, RequestTimeout, appBytes)
	if err != nil {
		return err
	}
	n.send(msg, set.Of(nodeID))
	return nil
}

// SendPullQuery polls the given validators for their preference at height.
func (n *Network) SendPullQuery(nodeIDs set.Set[ids.NodeID], requestID uint32, containerID ids.ID, requestedHeight uint64) error {
	msg, err := n.creator.PullQuery(n.chainID, requestID, RequestTimeout, containerID, requestedHeight)
	if err != nil {
		return err
	}
	n.send(msg, nodeIDs)
	return nil
}

// --- validator/peer refresh ---

func fetchWeights(ctx context.Context, c *platformvm.Client) (map[ids.NodeID]uint64, error) {
	list, err := c.GetCurrentValidators(ctx, avaconstants.PrimaryNetworkID, nil)
	if err != nil {
		return nil, err
	}
	weights := make(map[ids.NodeID]uint64, len(list))
	for _, v := range list {
		weights[v.NodeID] += v.Weight
	}
	return weights, nil
}

// reconcileValidators diffs the manager's primary-network set against the
// fetched weights: new validators are added, changed weights adjusted,
// missing validators removed. Weights of zero are dropped.
func reconcileValidators(m validators.Manager, weights map[ids.NodeID]uint64) error {
	subnetID := avaconstants.PrimaryNetworkID
	for _, id := range m.GetValidatorIDs(subnetID) {
		cur := m.GetWeight(subnetID, id)
		want := weights[id]
		delete(weights, id)
		switch {
		case want == cur:
		case want > cur:
			if err := m.AddWeight(subnetID, id, want-cur); err != nil {
				return err
			}
		default:
			if err := m.RemoveWeight(subnetID, id, cur-want); err != nil {
				return err
			}
		}
	}
	for id, w := range weights {
		if w == 0 {
			continue
		}
		if err := m.AddStaker(subnetID, id, nil, ids.Empty, w); err != nil {
			return err
		}
	}
	return nil
}

// trackPeers fetches peer IPs for the validator set and ManuallyTracks them.
func (n *Network) trackPeers(ctx context.Context, c *info.Client, validatorIDs []ids.NodeID) (int, error) {
	peers, err := c.Peers(ctx, validatorIDs)
	if err != nil || len(peers) == 0 {
		peers, err = c.Peers(ctx, nil)
		if err != nil {
			return 0, fmt.Errorf("info.peers: %w", err)
		}
	}
	for _, p := range peers {
		addr := p.PublicIP
		if !addr.IsValid() {
			addr = p.IP
		}
		if addr.IsValid() {
			n.net.ManuallyTrack(p.ID, addr)
		}
	}
	return len(peers), nil
}

func (n *Network) refreshLoop(interval time.Duration, infoClient *info.Client, pClient *platformvm.Client) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-n.done:
			return
		case <-t.C:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		weights, err := fetchWeights(ctx, pClient)
		if err == nil {
			err = reconcileValidators(n.vdrs, weights)
		}
		if err != nil {
			n.log.Warn("validator refresh failed", "err", err)
			cancel()
			continue
		}
		if _, err := n.trackPeers(ctx, infoClient, n.vdrs.GetValidatorIDs(avaconstants.PrimaryNetworkID)); err != nil {
			n.log.Warn("peer refresh failed", "err", err)
		}
		cancel()
		n.log.Debug("validator set refreshed", "validators", len(weights), "connected", n.NumConnected())
	}
}

func (n *Network) waitForPeer(ctx context.Context) error {
	if n.handler.numConnected() > 0 {
		return nil
	}
	t := time.NewTimer(defaultConnectTimeout)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-n.dispatchErr:
			return fmt.Errorf("net: dispatch stopped: %w", err)
		case <-t.C:
			return fmt.Errorf("net: no peer connected after %s", defaultConnectTimeout)
		case <-n.handler.connectedCh:
			return nil
		}
	}
}

func parseBootstrapPeer(s string) (ids.NodeID, netip.AddrPort, error) {
	id, rest, ok := strings.Cut(s, "@")
	if !ok {
		return ids.EmptyNodeID, netip.AddrPort{}, fmt.Errorf("net: bootstrap peer %q: want NodeID@ip:port", s)
	}
	nodeID, err := ids.NodeIDFromString(id)
	if err != nil {
		return ids.EmptyNodeID, netip.AddrPort{}, fmt.Errorf("net: bootstrap peer %q: %w", s, err)
	}
	addr, err := netip.ParseAddrPort(rest)
	if err != nil {
		return ids.EmptyNodeID, netip.AddrPort{}, fmt.Errorf("net: bootstrap peer %q: %w", s, err)
	}
	return nodeID, addr, nil
}
