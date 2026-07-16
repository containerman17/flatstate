package capture

import (
	"reflect"
	"testing"

	"github.com/containerman17/flatstate/schema"
)

func TestBatchRoundtrip(t *testing.T) {
	var acct schema.Account
	acct.Balance.SetUint64(77)
	acct.Nonce = 3
	acct.CodeHash = schema.Hash{9}
	b := &Batch{
		Block:  123,
		Hash:   schema.Hash{1},
		Parent: schema.Hash{2},
		Time:   999,
		Ops: []Op{
			{Kind: OpAccount, Addr: schema.Address{0xaa}, Account: acct},
			{Kind: OpSlot, Addr: schema.Address{0xaa}, Slot: schema.Hash{3}, Value: schema.Hash{4}},
			{Kind: OpDeleteSlot, Addr: schema.Address{0xaa}, Slot: schema.Hash{5}},
			{Kind: OpDestruct, Addr: schema.Address{0xbb}},
			{Kind: OpCode, CodeHash: schema.Hash{6}, Code: []byte{0xde, 0xad, 0xbe, 0xef}},
		},
	}
	enc := b.Encode(nil)
	got, err := Decode(enc)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(b, got) {
		t.Fatalf("roundtrip mismatch:\n%+v\n%+v", b, got)
	}
	// The decoded batch must not alias the input (raw LMDB reads).
	enc[len(enc)-1] ^= 0xff
	if got.Ops[4].Code[3] != 0xef {
		t.Fatal("decoded batch aliases input buffer")
	}
}

func TestBatchDecodeErrors(t *testing.T) {
	b := &Batch{Block: 1, Ops: []Op{{Kind: OpDestruct, Addr: schema.Address{1}}}}
	enc := b.Encode(nil)
	if _, err := Decode(enc[:len(enc)-1]); err == nil {
		t.Fatal("truncated batch should fail")
	}
	if _, err := Decode(append(enc, 0)); err == nil {
		t.Fatal("trailing bytes should fail")
	}
	bad := b.Encode(nil)
	bad[len(bad)-21] = 0x7f // corrupt the op kind
	if _, err := Decode(bad); err == nil {
		t.Fatal("unknown op kind should fail")
	}
}

func TestEmptyBatch(t *testing.T) {
	b := &Batch{Block: 5, Time: 1}
	got, err := Decode(b.Encode(nil))
	if err != nil {
		t.Fatal(err)
	}
	if got.Block != 5 || len(got.Ops) != 0 {
		t.Fatalf("bad empty batch: %+v", got)
	}
}
