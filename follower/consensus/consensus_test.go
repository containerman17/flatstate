package consensus

import (
	"math/big"
	"testing"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow/consensus/snowball"
	"github.com/ava-labs/avalanchego/utils/set"
	ethtypes "github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/flatstate/capture"
	"github.com/containerman17/flatstate/follower/net"
	"github.com/containerman17/flatstate/schema"
)

var vdr = ids.BuildTestNodeID([]byte{1})

// fakeNet records outbound traffic.
type fakeNet struct {
	reqID       uint32
	pullQueries []pullQuery
	gets        []ids.ID
	ancestors   []ids.ID
}

type pullQuery struct {
	reqID     uint32
	container ids.ID
	height    uint64
}

func (f *fakeNet) NextRequestID() uint32 { f.reqID++; return f.reqID }
func (f *fakeNet) SampleValidators(k int) ([]ids.NodeID, error) {
	out := make([]ids.NodeID, k)
	for i := range out {
		out[i] = vdr
	}
	return out, nil
}
func (f *fakeNet) IsConnected(ids.NodeID) bool    { return true }
func (f *fakeNet) SelectPeer() (ids.NodeID, bool) { return vdr, true }
func (f *fakeNet) SendGet(_ ids.NodeID, _ uint32, containerID ids.ID) error {
	f.gets = append(f.gets, containerID)
	return nil
}
func (f *fakeNet) SendGetAncestors(_ ids.NodeID, _ uint32, containerID ids.ID) error {
	f.ancestors = append(f.ancestors, containerID)
	return nil
}
func (f *fakeNet) SendPullQuery(_ set.Set[ids.NodeID], reqID uint32, containerID ids.ID, height uint64) error {
	f.pullQueries = append(f.pullQueries, pullQuery{reqID, containerID, height})
	return nil
}

// fakeSink records events.
type fakeSink struct {
	startHeight uint64
	startHash   schema.Hash
	verified    []uint64
	heads       []schema.Hash
	accepted    []uint64
}

func (s *fakeSink) Verified(b *capture.Batch)     { s.verified = append(s.verified, b.Block) }
func (s *fakeSink) Head(h schema.Hash) error      { s.heads = append(s.heads, h); return nil }
func (s *fakeSink) Accepted(b uint64, _ schema.Hash) error {
	s.accepted = append(s.accepted, b)
	return nil
}

func testParams() snowball.Parameters {
	return snowball.Parameters{
		K: 1, AlphaPreference: 1, AlphaConfidence: 1, Beta: 1,
		ConcurrentRepolls: 1, OptimalProcessing: 1,
		MaxOutstandingItems: 16, MaxItemProcessingTime: time.Minute,
	}
}

// chain builds n+1 linked eth blocks as pre-fork containers, heights 100..100+n.
func chain(t *testing.T, n int) []*net.Container {
	t.Helper()
	net.RegisterExtras()
	out := make([]*net.Container, 0, n+1)
	parent := ethtypes.EmptyRootHash
	for i := 0; i <= n; i++ {
		header := &ethtypes.Header{
			Number:     big.NewInt(int64(100 + i)),
			ParentHash: parent,
			GasLimit:   8_000_000,
			BaseFee:    big.NewInt(1),
			Difficulty: big.NewInt(1),
			Time:       uint64(1000 + i),
		}
		blk := ethtypes.NewBlockWithHeader(header)
		raw, err := rlp.EncodeToBytes(blk)
		if err != nil {
			t.Fatal(err)
		}
		c, err := net.ParseContainer(raw)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, c)
		parent = blk.Hash()
	}
	return out
}

func newEngine(t *testing.T, fn *fakeNet, sink *fakeSink, resume *Anchor) *Engine {
	t.Helper()
	e, err := New(Config{
		Net:  fn,
		Exec: HeaderOnly{},
		MakeSink: func(h uint64, hash schema.Hash) (Sink, error) {
			sink.startHeight, sink.startHash = h, hash
			return sink, nil
		},
		Resume: resume,
		Params: testParams(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func checkNoFatal(t *testing.T, e *Engine) {
	t.Helper()
	select {
	case err := <-e.Fatal():
		t.Fatal(err)
	default:
	}
}

func TestBootstrapAndAccept(t *testing.T) {
	cs := chain(t, 2) // heights 100,101,102
	anchor, child, grand := cs[0], cs[1], cs[2]
	fn := &fakeNet{}
	sink := &fakeSink{}
	e := newEngine(t, fn, sink, nil)

	// Bootstrap poll goes out on the first tick.
	e.Tick()
	if len(fn.pullQueries) != 1 || fn.pullQueries[0].container != ids.Empty {
		t.Fatalf("want bootstrap pull query, got %+v", fn.pullQueries)
	}
	// One chit reaches alpha (K=1): anchor found, Get sent.
	e.OnChits(vdr, fn.pullQueries[0].reqID, anchor.ID, anchor.ID, anchor.ID, 100)
	if len(fn.gets) != 1 || fn.gets[0] != anchor.ID {
		t.Fatalf("want anchor get, got %v", fn.gets)
	}
	// Anchor container arrives: no resume, go live at the anchor.
	e.OnContainer(vdr, anchor.Bytes)
	checkNoFatal(t, e)
	if sink.startHeight != 100 || sink.startHash != schema.Hash(anchor.Eth.Hash()) {
		t.Fatalf("sink start %d %x", sink.startHeight, sink.startHash[:4])
	}
	if h, hash := e.LastAccepted(); h != 100 || hash != schema.Hash(anchor.Eth.Hash()) {
		t.Fatalf("last accepted %d", h)
	}
	// Going live issues a poll for the anchor preference.
	if len(fn.pullQueries) != 2 || fn.pullQueries[1].container != anchor.ID || fn.pullQueries[1].height != 101 {
		t.Fatalf("live poll: %+v", fn.pullQueries)
	}

	// Child arrives: executed, verified, preference extends to it.
	e.OnContainer(vdr, child.Bytes)
	checkNoFatal(t, e)
	if len(sink.verified) != 1 || sink.verified[0] != 101 {
		t.Fatalf("verified %v", sink.verified)
	}
	if len(sink.heads) == 0 || sink.heads[len(sink.heads)-1] != schema.Hash(child.Eth.Hash()) {
		t.Fatalf("head not emitted for child: %v", sink.heads)
	}

	// Chits for the live poll vote for the child: K=1 alpha=1 beta=1 accepts it.
	e.OnChits(vdr, fn.pullQueries[1].reqID, child.ID, child.ID, anchor.ID, 100)
	checkNoFatal(t, e)
	if len(sink.accepted) != 1 || sink.accepted[0] != 101 {
		t.Fatalf("accepted %v", sink.accepted)
	}
	if h, _ := e.LastAccepted(); h != 101 {
		t.Fatalf("last accepted height %d", h)
	}

	// A chit naming an unknown block triggers a fetch and a dropped vote.
	e.Tick() // may or may not poll depending on timing; use last poll if issued
	prevGets := len(fn.gets)
	e.sendPollForTest()
	pq := fn.pullQueries[len(fn.pullQueries)-1]
	e.OnChits(vdr, pq.reqID, grand.ID, grand.ID, child.ID, 101)
	if len(fn.gets) <= prevGets {
		t.Fatal("unknown chit block should be fetched")
	}
	// The fetched container arrives and gets issued on top of the accepted child.
	e.OnContainer(vdr, grand.Bytes)
	checkNoFatal(t, e)
	if len(sink.verified) != 2 || sink.verified[1] != 102 {
		t.Fatalf("verified %v", sink.verified)
	}
}

func TestBackfillToResume(t *testing.T) {
	cs := chain(t, 3) // 100 resume, 101, 102 anchor, 103 live
	resume, mid, anchor, next := cs[0], cs[1], cs[2], cs[3]
	fn := &fakeNet{}
	sink := &fakeSink{}
	e := newEngine(t, fn, sink, &Anchor{
		Height:  100,
		EthHash: schema.Hash(resume.Eth.Hash()),
		HashSet: true,
	})

	e.Tick()
	e.OnChits(vdr, fn.pullQueries[0].reqID, anchor.ID, anchor.ID, anchor.ID, 102)
	e.OnContainer(vdr, anchor.Bytes) // anchor at 102 > resume 100: backfill
	checkNoFatal(t, e)
	if len(fn.ancestors) != 1 || fn.ancestors[0] != anchor.ParentID {
		t.Fatalf("want GetAncestors(parent of anchor), got %v", fn.ancestors)
	}
	// Ancestors response, newest first: 101 then 100.
	bfReq := fn.reqID
	e.OnAncestors(vdr, bfReq, [][]byte{mid.Bytes, resume.Bytes})
	checkNoFatal(t, e)
	if sink.startHeight != 100 || sink.startHash != schema.Hash(resume.Eth.Hash()) {
		t.Fatalf("start %d", sink.startHeight)
	}
	// Gap executed forward and finalized: 101, 102.
	if len(sink.verified) != 2 || sink.verified[0] != 101 || sink.verified[1] != 102 {
		t.Fatalf("verified %v", sink.verified)
	}
	if len(sink.accepted) != 2 || sink.accepted[1] != 102 {
		t.Fatalf("accepted %v", sink.accepted)
	}
	if h, _ := e.LastAccepted(); h != 102 {
		t.Fatalf("anchor height %d", h)
	}
	// Live from the anchor.
	e.OnContainer(vdr, next.Bytes)
	checkNoFatal(t, e)
	if len(sink.verified) != 3 || sink.verified[2] != 103 {
		t.Fatalf("live verified %v", sink.verified)
	}
}

func TestBackfillHashMismatchFailsLoud(t *testing.T) {
	cs := chain(t, 1)
	anchor := cs[1]
	fn := &fakeNet{}
	sink := &fakeSink{}
	e := newEngine(t, fn, sink, &Anchor{Height: 100, EthHash: schema.Hash{0xff}, HashSet: true})

	e.Tick()
	e.OnChits(vdr, fn.pullQueries[0].reqID, anchor.ID, anchor.ID, anchor.ID, 101)
	e.OnContainer(vdr, anchor.Bytes)
	e.OnAncestors(vdr, fn.reqID, [][]byte{cs[0].Bytes})
	select {
	case err := <-e.Fatal():
		if err == nil {
			t.Fatal("nil fatal")
		}
	default:
		t.Fatal("hash mismatch must be fatal")
	}
}

// sendPollForTest issues a poll regardless of tick timing.
func (e *Engine) sendPollForTest() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sendPoll()
}
