package tipbus

import (
	"testing"

	"github.com/containerman17/flatstate/capture"
	"github.com/containerman17/flatstate/schema"
)

func h(b byte) schema.Hash { return schema.Hash{31: b} }

func batch(n uint64) *capture.Batch {
	return &capture.Batch{Block: n, Hash: h(byte(n)), Time: n * 10, Ops: []capture.Op{
		{Kind: capture.OpSlot, Addr: schema.Address{0xaa}, Slot: h(1), Value: h(byte(n))},
	}}
}

func TestPublishPollHandshake(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(dir, 1<<28)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	r, err := OpenReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if seq, err := r.Seq(); err != nil || seq != 0 {
		t.Fatalf("initial seq = %d %v", seq, err)
	}

	if err := w.PublishBlock(batch(101)); err != nil {
		t.Fatal(err)
	}
	if err := w.PublishMempool(555, []byte("tx")); err != nil {
		t.Fatal(err)
	}
	if err := w.PublishBlock(batch(102)); err != nil {
		t.Fatal(err)
	}
	if err := w.PublishFinalize(101, h(101)); err != nil {
		t.Fatal(err)
	}

	// Late subscriber handshake: finalized height + current unfinalized layers.
	fin, layers, seq, err := r.Handshake()
	if err != nil {
		t.Fatal(err)
	}
	if fin != 101 || len(layers) != 1 || layers[0].Block != 102 || seq != 4 {
		t.Fatalf("handshake = fin %d, %d layers, seq %d", fin, len(layers), seq)
	}

	// Full poll from the start sees every event in order.
	events, next, err := r.Poll(0)
	if err != nil || next != 4 || len(events) != 4 {
		t.Fatalf("poll: %d events, next %d, %v", len(events), next, err)
	}
	if events[0].Kind != EvBlock || events[0].Batch.Block != 101 {
		t.Fatalf("event 0: %+v", events[0])
	}
	if events[1].Kind != EvMempool || events[1].Time != 555 || string(events[1].Tx) != "tx" {
		t.Fatalf("event 1: %+v", events[1])
	}
	if events[2].Kind != EvBlock || events[2].Batch.Block != 102 {
		t.Fatalf("event 2: %+v", events[2])
	}
	if events[3].Kind != EvFinalize || events[3].Height != 101 || events[3].Hash != h(101) {
		t.Fatalf("event 3: %+v", events[3])
	}

	// Incremental poll from the handshake point.
	if err := w.PublishReset([]*capture.Batch{batch(102), batch(103)}); err != nil {
		t.Fatal(err)
	}
	events, next, err = r.Poll(next)
	if err != nil || len(events) != 1 || next != 5 {
		t.Fatalf("incremental poll: %d events, next %d, %v", len(events), next, err)
	}
	if events[0].Kind != EvReset || len(events[0].Batches) != 2 || events[0].Batches[1].Block != 103 {
		t.Fatalf("reset event: %+v", events[0])
	}
	// Reset replaced the handshake layers too.
	_, layers, _, err = r.Handshake()
	if err != nil || len(layers) != 2 {
		t.Fatalf("handshake after reset: %d layers %v", len(layers), err)
	}
}

func TestFinalizeMismatch(t *testing.T) {
	w, err := OpenWriter(t.TempDir(), 1<<28)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.PublishFinalize(101, h(101)); err == nil {
		t.Fatal("finalize with no layers must fail")
	}
}

func TestWriterTruncatesOnOpen(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(dir, 1<<28)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.PublishBlock(batch(101)); err != nil {
		t.Fatal(err)
	}
	w.Close()

	// Reopen: unfinalized data must be gone (never resume on a fork, D7).
	w, err = OpenWriter(dir, 1<<28)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	r, err := OpenReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	fin, layers, seq, err := r.Handshake()
	if err != nil {
		t.Fatal(err)
	}
	if fin != 0 || len(layers) != 0 || seq != 0 {
		t.Fatalf("bus not truncated: fin %d, %d layers, seq %d", fin, len(layers), seq)
	}
}

func BenchmarkPublishMempool(b *testing.B) {
	w, err := OpenWriter(b.TempDir(), 1<<30)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	tx := make([]byte, 200)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.PublishMempool(uint64(i), tx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSeqPoll(b *testing.B) {
	dir := b.TempDir()
	w, err := OpenWriter(dir, 1<<28)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	r, err := OpenReader(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer r.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.Seq(); err != nil {
			b.Fatal(err)
		}
	}
}
