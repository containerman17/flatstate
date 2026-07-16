// Package consensus is the follower's finality engine (DESIGN.md D2 rev 2):
// real snowman sampling against the weighted validator set. Gossip and Get
// responses provide containers; our own PullQuery polls decide preference
// and acceptance locally.
//
// Design choice, recorded per the task brief: we reuse avalanchego's
// snow/consensus/snowman.Topological consensus object and its poll.Set /
// early-term poll accounting verbatim, and reimplement only the thin engine
// shell around them (K-sampling from the validator manager, PullQuery out,
// Chits in, vote bubbling to the nearest processing ancestor, poll expiry).
// The alternative, embedding snow/engine/snowman.Engine, was investigated
// and rejected: it drags in the whole block.ChainVM plumbing (a VM facade
// over our exec pipeline), the common.Sender/router timeout machinery, the
// bootstrap/state-sync engines, and job schedulers, all to replace ~300
// lines of shell. Topological is the part with actual consensus semantics
// (vote transitivity, preference switching, commit rule); that is what must
// not be reimplemented, and it is not.
//
// Deviations from the real engine, all fail-safe (they can only delay
// acceptance, never accept something the network did not):
//   - Chits votes whose blocks we cannot fetch are dropped after bubbling
//     through known-but-unissued ancestors; the real engine additionally
//     blocks vote application on in-flight fetches.
//   - No push gossip and no query serving: we never answer PullQuery (we
//     are not a validator; nobody samples us).
//   - Poll expiry is a coarse per-tick sweep instead of a per-request
//     timeout manager.
package consensus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/avalanchego/snow/consensus/snowball"
	"github.com/ava-labs/avalanchego/snow/consensus/snowman"
	"github.com/ava-labs/avalanchego/snow/consensus/snowman/poll"
	"github.com/ava-labs/avalanchego/utils/bag"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/avalanchego/utils/set"
	ethtypes "github.com/ava-labs/libevm/core/types"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/containerman17/flatstate/capture"
	"github.com/containerman17/flatstate/follower/net"
	"github.com/containerman17/flatstate/schema"
)

// Executor executes one block against the state at its parent and returns
// the capture batch (stage C), or a header-only pseudo batch (dry-run
// without a baseline). A non-nil error is fatal: capture halts (D13).
type Executor interface {
	Execute(parent schema.Hash, blk *ethtypes.Block) (*capture.Batch, error)
}

// Sink receives the engine's block events; node.Tracker implements it.
type Sink interface {
	// Verified records an executed block's batch (may sit on a fork).
	Verified(b *capture.Batch)
	// Head reacts to a preferred-tip change (extension or reset).
	Head(hash schema.Hash) error
	// Accepted finalizes a block.
	Accepted(block uint64, hash schema.Hash) error
}

// Net is the outbound side the engine needs; *net.Network implements it.
type Net interface {
	NextRequestID() uint32
	SampleValidators(k int) ([]ids.NodeID, error)
	IsConnected(nodeID ids.NodeID) bool
	SelectPeer() (ids.NodeID, bool)
	SendGet(nodeID ids.NodeID, requestID uint32, containerID ids.ID) error
	SendGetAncestors(nodeID ids.NodeID, requestID uint32, containerID ids.ID) error
	SendPullQuery(nodeIDs set.Set[ids.NodeID], requestID uint32, containerID ids.ID, requestedHeight uint64) error
}

// Anchor pins the height and eth hash the exec/store side resumes from.
type Anchor struct {
	Height  uint64
	EthHash schema.Hash // zero = unknown, trust the fetched chain
	HashSet bool
}

type Config struct {
	Net  Net
	Exec Executor
	// MakeSink is called exactly once, when the bootstrap start point is
	// known: at Resume (after hash verification) or at the network anchor
	// when Resume is nil. The returned Sink receives all further events.
	MakeSink func(startHeight uint64, startHash schema.Hash) (Sink, error)
	// Resume, if set, is where the local store stands (finalized height /
	// baseline S). The gap up to the network's accepted anchor is backfilled
	// via GetAncestors and executed before going live. Nil = start at the
	// anchor (dry-run without a baseline).
	Resume *Anchor

	Params snowball.Parameters // zero = snowball.DefaultParameters
	// PollInterval is the live poll cadence while blocks are processing;
	// IdlePollInterval is the discovery cadence while quiesced.
	PollInterval     time.Duration // default 100ms
	IdlePollInterval time.Duration // default 1s
	Log              *slog.Logger
}

type phase int

const (
	phasePollAnchor phase = iota // polling validators for the accepted anchor
	phaseBackfill                // fetching anchor/gap containers via Get/GetAncestors
	phaseLive
)

type rec struct {
	c       *net.Container
	ethHash schema.Hash
	issued  bool
}

type pollState struct {
	remaining set.Set[ids.NodeID]
	deadline  time.Time
}

type getReq struct {
	deadline time.Time
	tries    int
}

// Engine drives consensus. All exported methods are concurrency-safe; wire
// OnContainer/OnChits/OnAncestors to net.Callbacks and call Tick on a timer.
type Engine struct {
	cfg  Config
	log  *slog.Logger
	sink Sink

	// guarded by the run loop: every entry point takes mu.
	mu sync.Mutex

	phase phase
	fatal chan error

	// bootstrap
	bsReqID  uint32
	bsVotes  map[ids.ID]int
	bsVoters set.Set[ids.NodeID]
	bsHeight map[ids.ID]uint64
	lastBSAt time.Time

	// backfill (phaseBackfill)
	anchorID ids.ID
	bfChain  map[uint64]*net.Container // contiguous [lowest..anchor] by eth number
	bfAnchor *net.Container
	bfLow    *net.Container
	bfReqID  uint32
	bfDue    time.Time

	// live
	cons        *snowman.Topological
	polls       poll.Set
	pollVdrs    map[uint32]*pollState
	recs        map[ids.ID]*rec
	pendingByPa map[ids.ID][]ids.ID // children container IDs waiting for parent
	gets        map[ids.ID]*getReq  // outstanding container fetches
	lastPollAt  time.Time
	lastPref    schema.Hash

	lastAcceptedID      ids.ID
	lastAcceptedHeight  uint64
	lastAcceptedEthHash schema.Hash
	lastAcceptAt        time.Time
}

func New(cfg Config) (*Engine, error) {
	if cfg.Net == nil || cfg.Exec == nil || cfg.MakeSink == nil {
		return nil, errors.New("consensus: Net, Exec, MakeSink required")
	}
	if cfg.Params == (snowball.Parameters{}) {
		cfg.Params = snowball.DefaultParameters
	}
	if err := cfg.Params.Verify(); err != nil {
		return nil, err
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	if cfg.IdlePollInterval <= 0 {
		cfg.IdlePollInterval = time.Second
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	return &Engine{
		cfg:         cfg,
		log:         cfg.Log,
		phase:       phasePollAnchor,
		fatal:       make(chan error, 1),
		bsVotes:     make(map[ids.ID]int),
		bsHeight:    make(map[ids.ID]uint64),
		pollVdrs:    make(map[uint32]*pollState),
		recs:        make(map[ids.ID]*rec),
		pendingByPa: make(map[ids.ID][]ids.ID),
		gets:        make(map[ids.ID]*getReq),
	}, nil
}

// Fatal reports the first unrecoverable error (execution/validation failure,
// sink failure). The follower must halt capture on it (D13).
func (e *Engine) Fatal() <-chan error { return e.fatal }

func (e *Engine) fail(err error) {
	e.log.Error("consensus: fatal", "err", err)
	select {
	case e.fatal <- err:
	default:
	}
}

// LastAccepted returns the engine's current accepted frontier.
func (e *Engine) LastAccepted() (uint64, schema.Hash) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.lastAcceptedHeight, e.lastAcceptedEthHash
}

// --- inbound (wired to net.Callbacks) ---

func (e *Engine) OnContainer(nodeID ids.NodeID, raw []byte) {
	c, err := net.ParseContainer(raw)
	if err != nil {
		e.log.Debug("bad container", "from", nodeID, "err", err)
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.gets, c.ID)
	switch e.phase {
	case phaseBackfill:
		e.backfillContainer(c)
		if e.phase == phaseBackfill {
			e.sendBackfillRequest()
		}
	case phaseLive:
		e.addContainer(c)
	}
}

func (e *Engine) OnChits(nodeID ids.NodeID, requestID uint32, preferred, preferredAtHeight, accepted ids.ID, acceptedHeight uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.phase == phasePollAnchor {
		e.bootstrapChit(nodeID, requestID, accepted, acceptedHeight)
		return
	}
	if e.phase != phaseLive {
		return
	}
	ps, ok := e.pollVdrs[requestID]
	if !ok || !ps.remaining.Contains(nodeID) {
		return
	}
	ps.remaining.Remove(nodeID)
	if ps.remaining.Len() == 0 {
		delete(e.pollVdrs, requestID)
	}

	// Fetch unknown blocks named by the chits so future polls can use them.
	e.fetchIfUnknown(preferred)
	if preferredAtHeight != preferred {
		e.fetchIfUnknown(preferredAtHeight)
	}

	// Bubble the vote to the nearest block consensus knows about; response
	// options in decreasing-height order, first applicable wins (mirrors
	// snow/engine/snowman voter.go).
	var results []bag.Bag[ids.ID]
	if vote, ok := e.bubble(preferred); ok {
		results = e.polls.Vote(requestID, nodeID, vote)
	} else if vote, ok := e.bubble(preferredAtHeight); ok {
		results = e.polls.Vote(requestID, nodeID, vote)
	} else {
		results = e.polls.Drop(requestID, nodeID)
	}
	e.recordPolls(results)
}

func (e *Engine) OnAncestors(nodeID ids.NodeID, requestID uint32, containers [][]byte) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.phase != phaseBackfill || requestID != e.bfReqID {
		return
	}
	e.bfReqID = 0
	for _, raw := range containers {
		c, err := net.ParseContainer(raw)
		if err != nil {
			e.log.Warn("backfill: bad container in ancestors", "from", nodeID, "err", err)
			return // retry with another peer on next tick
		}
		e.backfillContainer(c)
		if e.phase != phaseBackfill {
			return // backfill finished mid-batch
		}
	}
	e.sendBackfillRequest()
}

// Tick drives timeouts, polls, and bootstrap retries. Call every ~50-100ms.
func (e *Engine) Tick() {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now()
	switch e.phase {
	case phasePollAnchor:
		if now.Sub(e.lastBSAt) >= 2*time.Second {
			e.sendBootstrapPoll()
		}
	case phaseBackfill:
		if e.bfReqID == 0 || now.After(e.bfDue) {
			e.sendBackfillRequest()
		}
	case phaseLive:
		e.expirePolls(now)
		e.retryGets(now)
		interval := e.cfg.IdlePollInterval
		if e.cons.NumProcessing() > 0 {
			interval = e.cfg.PollInterval
		}
		if now.Sub(e.lastPollAt) >= interval {
			e.sendPoll()
		}
		if !e.lastAcceptAt.IsZero() && now.Sub(e.lastAcceptAt) > 30*time.Second {
			e.log.Warn("no block accepted recently", "since", now.Sub(e.lastAcceptAt), "processing", e.cons.NumProcessing())
			e.lastAcceptAt = now // rate-limit the warning
		}
	}
}

// --- bootstrap: find the network's accepted anchor via polls ---

func (e *Engine) sendBootstrapPoll() {
	vdrs, err := e.cfg.Net.SampleValidators(e.cfg.Params.K)
	if err != nil || len(vdrs) == 0 {
		e.log.Warn("bootstrap: cannot sample validators", "err", err)
		return
	}
	e.bsReqID = e.cfg.Net.NextRequestID()
	clear(e.bsVotes)
	clear(e.bsHeight)
	e.bsVoters.Clear()
	targets := set.NewSet[ids.NodeID](len(vdrs))
	for _, v := range vdrs {
		if e.cfg.Net.IsConnected(v) {
			targets.Add(v)
		}
	}
	if targets.Len() == 0 {
		e.log.Warn("bootstrap: no sampled validator connected")
		return
	}
	e.lastBSAt = time.Now()
	if err := e.cfg.Net.SendPullQuery(targets, e.bsReqID, ids.Empty, 0); err != nil {
		e.log.Warn("bootstrap: pull query send failed", "err", err)
	}
}

func (e *Engine) bootstrapChit(nodeID ids.NodeID, requestID uint32, accepted ids.ID, acceptedHeight uint64) {
	if requestID != e.bsReqID || e.bsVoters.Contains(nodeID) {
		return
	}
	e.bsVoters.Add(nodeID)
	e.bsVotes[accepted]++
	e.bsHeight[accepted] = acceptedHeight
	if e.bsVotes[accepted] < e.cfg.Params.AlphaPreference {
		return
	}
	// Alpha of a K-sample agrees on the accepted frontier: that is our anchor.
	e.anchorID = accepted
	e.phase = phaseBackfill
	e.bfChain = make(map[uint64]*net.Container)
	e.log.Info("anchor found", "container", accepted, "height", e.bsHeight[accepted], "votes", e.bsVotes[accepted])
	e.sendBackfillRequest()
}

// --- backfill: fetch anchor (and the gap down to Resume), execute forward ---

func (e *Engine) sendBackfillRequest() {
	peer, ok := e.cfg.Net.SelectPeer()
	if !ok {
		e.log.Warn("backfill: no peer available")
		return
	}
	e.bfReqID = e.cfg.Net.NextRequestID()
	e.bfDue = time.Now().Add(net.RequestTimeout)
	var err error
	if e.bfLow == nil {
		err = e.cfg.Net.SendGet(peer, e.bfReqID, e.anchorID)
	} else {
		// Ancestors of the current frontier's parent; the response includes
		// the requested container itself, newest first.
		err = e.cfg.Net.SendGetAncestors(peer, e.bfReqID, e.bfLow.ParentID)
	}
	if err != nil {
		e.log.Warn("backfill: send failed", "err", err)
	}
}

// backfillContainer extends the anchor chain downward and finishes the
// bootstrap when it reaches the resume point.
func (e *Engine) backfillContainer(c *net.Container) {
	h := c.Eth.NumberU64()
	if e.bfLow == nil {
		if c.ID != e.anchorID {
			return
		}
	} else {
		// Must be the next parent down, verified by eth hash linkage.
		if h != e.bfLow.Eth.NumberU64()-1 || c.Eth.Hash() != e.bfLow.Eth.ParentHash() || c.ID != e.bfLow.ParentID {
			return
		}
	}
	e.bfChain[h] = c
	if e.bfAnchor == nil {
		e.bfAnchor = c
	}
	e.bfLow = c

	resume := e.cfg.Resume
	if resume == nil || h == resume.Height || h == 0 {
		e.finishBootstrap()
		return
	}
	if h < resume.Height {
		e.fail(fmt.Errorf("consensus: network anchor chain passed below resume height %d without matching", resume.Height))
	}
}

func (e *Engine) finishBootstrap() {
	anchor := e.bfAnchor
	start := e.bfLow
	startHeight := start.Eth.NumberU64()
	startHash := schema.Hash(start.Eth.Hash())

	if r := e.cfg.Resume; r != nil {
		if startHeight != r.Height {
			e.fail(fmt.Errorf("consensus: backfill stopped at %d, resume is %d", startHeight, r.Height))
			return
		}
		if r.HashSet && startHash != r.EthHash {
			e.fail(fmt.Errorf("consensus: chain hash %x at %d does not match store %x", startHash[:4], r.Height, r.EthHash[:4]))
			return
		}
	}

	sink, err := e.cfg.MakeSink(startHeight, startHash)
	if err != nil {
		e.fail(err)
		return
	}
	e.sink = sink

	// Execute the gap forward; every block in it is network-accepted.
	parentHash := startHash
	for h := startHeight + 1; ; h++ {
		c, ok := e.bfChain[h]
		if !ok {
			break
		}
		batch, err := e.cfg.Exec.Execute(parentHash, c.Eth)
		if err != nil {
			e.fail(fmt.Errorf("consensus: backfill execute %d: %w", h, err))
			return
		}
		e.sink.Verified(batch)
		if err := e.sink.Accepted(h, batch.Hash); err != nil {
			e.fail(fmt.Errorf("consensus: backfill accept %d: %w", h, err))
			return
		}
		parentHash = batch.Hash
	}

	anchorHeight := anchor.Eth.NumberU64()
	e.lastAcceptedID = anchor.ID
	e.lastAcceptedHeight = anchorHeight
	e.lastAcceptedEthHash = schema.Hash(anchor.Eth.Hash())
	e.lastPref = e.lastAcceptedEthHash
	e.lastAcceptAt = time.Now()

	// Real snowman consensus over containers above the anchor.
	consCtx := &snow.ConsensusContext{
		Context:       &snow.Context{Log: logging.NoLog{}},
		Registerer:    prometheus.NewRegistry(),
		BlockAcceptor: noOpAcceptor{},
	}
	// SnowballFactory is what production avalanchego uses (snowflake+ballot).
	cons := &snowman.Topological{Factory: snowball.SnowballFactory}
	if err := cons.Initialize(consCtx, e.cfg.Params, anchor.ID, anchorHeight, time.Unix(int64(anchor.Eth.Time()), 0)); err != nil {
		e.fail(err)
		return
	}
	factory, err := poll.NewEarlyTermFactory(e.cfg.Params.AlphaPreference, e.cfg.Params.AlphaConfidence, prometheus.NewRegistry(), cons)
	if err != nil {
		e.fail(err)
		return
	}
	polls, err := poll.NewSet(factory, logging.NoLog{}, prometheus.NewRegistry())
	if err != nil {
		e.fail(err)
		return
	}
	e.cons = cons
	e.polls = polls
	e.bfChain = nil
	e.bfAnchor = nil
	e.bfLow = nil
	e.phase = phaseLive
	e.log.Info("live", "anchor_height", anchorHeight, "anchor", anchor.ID)
	e.sendPoll()
}

// --- live: containers ---

func (e *Engine) addContainer(c *net.Container) {
	if _, ok := e.recs[c.ID]; ok {
		return
	}
	if c.ID == e.lastAcceptedID || c.Eth.NumberU64() <= e.lastAcceptedHeight {
		return
	}
	r := &rec{c: c, ethHash: schema.Hash(c.Eth.Hash())}
	e.recs[c.ID] = r
	e.tryIssue(r)
}

// tryIssue executes the block once its parent is known and issued, then
// feeds consensus and drains children that were waiting on it.
func (e *Engine) tryIssue(r *rec) {
	if r.issued {
		return
	}
	parentID := r.c.ParentID
	var parentEthHash schema.Hash
	var parentHeight uint64
	switch {
	case parentID == e.lastAcceptedID:
		parentEthHash = e.lastAcceptedEthHash
		parentHeight = e.lastAcceptedHeight
	default:
		pr, ok := e.recs[parentID]
		if !ok || !pr.issued {
			if !ok {
				e.fetchIfUnknown(parentID)
			}
			e.pendingByPa[parentID] = append(e.pendingByPa[parentID], r.c.ID)
			return
		}
		parentEthHash = pr.ethHash
		parentHeight = pr.c.Eth.NumberU64()
	}

	// Sanity per D13: the inner chain must mirror the container chain.
	if schema.Hash(r.c.Eth.ParentHash()) != parentEthHash || r.c.Eth.NumberU64() != parentHeight+1 {
		e.log.Error("container inner chain does not match container parent; dropping",
			"container", r.c.ID, "height", r.c.Eth.NumberU64())
		delete(e.recs, r.c.ID)
		return
	}

	batch, err := e.cfg.Exec.Execute(parentEthHash, r.c.Eth)
	if err != nil {
		e.fail(fmt.Errorf("consensus: execute %d %x: %w", r.c.Eth.NumberU64(), r.ethHash[:4], err))
		return
	}
	e.sink.Verified(batch)
	if err := e.cons.Add(&blockAdapter{e: e, r: r}); err != nil {
		e.fail(fmt.Errorf("consensus: add %x: %w", r.c.ID, err))
		return
	}
	r.issued = true
	e.emitHead()

	children := e.pendingByPa[r.c.ID]
	delete(e.pendingByPa, r.c.ID)
	for _, childID := range children {
		if cr, ok := e.recs[childID]; ok {
			e.tryIssue(cr)
		}
	}
}

func (e *Engine) fetchIfUnknown(id ids.ID) {
	if id == ids.Empty || id == e.lastAcceptedID {
		return
	}
	if _, ok := e.recs[id]; ok {
		return
	}
	if g, ok := e.gets[id]; ok && time.Now().Before(g.deadline) {
		return
	}
	peer, ok := e.cfg.Net.SelectPeer()
	if !ok {
		return
	}
	g := e.gets[id]
	if g == nil {
		g = &getReq{}
		e.gets[id] = g
	}
	g.deadline = time.Now().Add(net.RequestTimeout)
	g.tries++
	if err := e.cfg.Net.SendGet(peer, e.cfg.Net.NextRequestID(), id); err != nil {
		e.log.Warn("get send failed", "err", err)
	}
}

func (e *Engine) retryGets(now time.Time) {
	for id, g := range e.gets {
		if now.Before(g.deadline) {
			continue
		}
		if g.tries >= 10 {
			e.log.Warn("giving up on container fetch", "container", id, "tries", g.tries)
			delete(e.gets, id)
			continue
		}
		e.fetchIfUnknown(id)
	}
}

// --- live: polls ---

func (e *Engine) sendPoll() {
	vdrs, err := e.cfg.Net.SampleValidators(e.cfg.Params.K)
	if err != nil || len(vdrs) == 0 {
		e.log.Warn("poll: cannot sample validators", "err", err)
		return
	}
	var vdrBag bag.Bag[ids.NodeID]
	vdrBag.Add(vdrs...)
	reqID := e.cfg.Net.NextRequestID()
	if !e.polls.Add(reqID, vdrBag) {
		return
	}
	e.lastPollAt = time.Now()

	targets := set.NewSet[ids.NodeID](len(vdrs))
	var disconnected []ids.NodeID
	for _, v := range vdrBag.List() {
		if e.cfg.Net.IsConnected(v) {
			targets.Add(v)
		} else {
			disconnected = append(disconnected, v)
		}
	}
	ps := &pollState{remaining: targets, deadline: time.Now().Add(net.RequestTimeout)}
	if targets.Len() > 0 {
		e.pollVdrs[reqID] = ps
		if err := e.cfg.Net.SendPullQuery(targets, reqID, e.cons.Preference(), e.lastAcceptedHeight+1); err != nil {
			e.log.Warn("poll: send failed", "err", err)
		}
	}
	// Unconnected samples can never answer: drop immediately.
	var results []bag.Bag[ids.ID]
	for _, v := range disconnected {
		results = append(results, e.polls.Drop(reqID, v)...)
	}
	e.recordPolls(results)
}

func (e *Engine) expirePolls(now time.Time) {
	for reqID, ps := range e.pollVdrs {
		if now.Before(ps.deadline) {
			continue
		}
		delete(e.pollVdrs, reqID)
		var results []bag.Bag[ids.ID]
		for _, v := range ps.remaining.List() {
			results = append(results, e.polls.Drop(reqID, v)...)
		}
		e.recordPolls(results)
	}
}

// bubble walks id up through known containers to the nearest one consensus
// is processing (or the accepted frontier). ok=false when the walk dead-ends
// at an unknown block.
func (e *Engine) bubble(id ids.ID) (ids.ID, bool) {
	for {
		if id == e.lastAcceptedID || e.cons.Processing(id) {
			return id, true
		}
		r, ok := e.recs[id]
		if !ok {
			return ids.Empty, false
		}
		id = r.c.ParentID
	}
}

func (e *Engine) recordPolls(results []bag.Bag[ids.ID]) {
	for _, votes := range results {
		if err := e.cons.RecordPoll(context.Background(), votes); err != nil {
			e.fail(fmt.Errorf("consensus: record poll: %w", err))
			return
		}
	}
	if len(results) > 0 {
		e.emitHead()
	}
}

func (e *Engine) emitHead() {
	pref := e.cons.Preference()
	var h schema.Hash
	if pref == e.lastAcceptedID {
		h = e.lastAcceptedEthHash
	} else if r, ok := e.recs[pref]; ok {
		h = r.ethHash
	} else {
		return
	}
	if h == e.lastPref {
		return
	}
	e.lastPref = h
	if err := e.sink.Head(h); err != nil {
		e.fail(fmt.Errorf("consensus: head: %w", err))
	}
}

// --- decision callbacks (invoked by Topological.RecordPoll, mu held) ---

func (e *Engine) onAccept(r *rec) error {
	h := r.c.Eth.NumberU64()
	e.lastAcceptedID = r.c.ID
	e.lastAcceptedHeight = h
	e.lastAcceptedEthHash = r.ethHash
	e.lastAcceptAt = time.Now()
	delete(e.recs, r.c.ID)
	// Anything at or below the accepted height is decided; drop leftovers.
	for id, o := range e.recs {
		if !o.issued && o.c.Eth.NumberU64() <= h {
			delete(e.recs, id)
		}
	}
	if err := e.sink.Accepted(h, r.ethHash); err != nil {
		return err
	}
	e.log.Info("accepted", "height", h, "hash", fmt.Sprintf("%x", r.ethHash[:8]))
	return nil
}

func (e *Engine) onReject(r *rec) {
	e.log.Info("rejected", "height", r.c.Eth.NumberU64(), "container", r.c.ID)
	delete(e.recs, r.c.ID)
	delete(e.pendingByPa, r.c.ID)
}

type noOpAcceptor struct{}

func (noOpAcceptor) Accept(*snow.ConsensusContext, ids.ID, []byte) error { return nil }

// blockAdapter exposes an executed container to snowman.Topological.
type blockAdapter struct {
	e *Engine
	r *rec
}

func (b *blockAdapter) ID() ids.ID     { return b.r.c.ID }
func (b *blockAdapter) Parent() ids.ID { return b.r.c.ParentID }
func (b *blockAdapter) Height() uint64 { return b.r.c.Eth.NumberU64() }
func (b *blockAdapter) Timestamp() time.Time {
	return time.Unix(int64(b.r.c.Eth.Time()), 0)
}
func (b *blockAdapter) Bytes() []byte { return b.r.c.Bytes }
func (b *blockAdapter) Verify(context.Context) error {
	return nil // executed and receipt-validated before Add
}
func (b *blockAdapter) Accept(context.Context) error { return b.e.onAccept(b.r) }
func (b *blockAdapter) Reject(context.Context) error { b.e.onReject(b.r); return nil }
