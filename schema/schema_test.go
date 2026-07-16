package schema

import (
	"bytes"
	"testing"

	"github.com/holiman/uint256"
)

func TestAccountRoundtrip(t *testing.T) {
	a := Account{Nonce: 42, CodeHash: Hash{1, 2, 3}}
	a.Balance.SetUint64(1 << 40)
	enc := EncodeAccount(nil, &a)
	if len(enc) != AccountSize {
		t.Fatalf("encoded %d bytes, want %d", len(enc), AccountSize)
	}
	got, err := DecodeAccount(enc)
	if err != nil {
		t.Fatal(err)
	}
	if got.Nonce != a.Nonce || got.CodeHash != a.CodeHash || !got.Balance.Eq(&a.Balance) {
		t.Fatalf("roundtrip mismatch: %+v != %+v", got, a)
	}
	if _, err := DecodeAccount(enc[:71]); err == nil {
		t.Fatal("short decode should fail")
	}
}

func TestInvBlockOrdering(t *testing.T) {
	addr := Address{0xaa}
	// Higher blocks must sort BEFORE lower blocks so a forward SetRange on
	// key||^B lands on the greatest write at or before B.
	k10 := AppendAccountKey(nil, addr, 10)
	k5 := AppendAccountKey(nil, addr, 5)
	seek7 := AppendAccountKey(nil, addr, 7)
	if bytes.Compare(k10, k5) >= 0 {
		t.Fatal("key for block 10 must sort before key for block 5")
	}
	// SetRange(seek7) = first key >= seek7; that must be k5 (greatest <= 7),
	// and k10 must sort strictly before seek7.
	if bytes.Compare(k10, seek7) >= 0 || bytes.Compare(seek7, k5) > 0 {
		t.Fatal("seek key for block 7 must fall between block 10 and block 5 rows")
	}
	if DecodeInvBlock(k10[len(k10)-8:]) != 10 {
		t.Fatal("DecodeInvBlock roundtrip failed")
	}
}

func TestKeyLens(t *testing.T) {
	addr := Address{1}
	slot := Hash{2}
	for _, tc := range []struct {
		key  []byte
		want int
	}{
		{AppendAccountKey(nil, addr, 1), AccountKeyLen},
		{AppendSlotKey(nil, addr, slot, 1), SlotKeyLen},
		{AppendDestructKey(nil, addr, 1), DestructKeyLen},
		{AppendDiffKey(nil, 1), DiffKeyLen},
		{AppendMempoolKey(nil, 1), MempoolKeyLen},
		{AppendCodeKey(nil, slot), CodeKeyLen},
	} {
		if len(tc.key) != tc.want {
			t.Fatalf("key %x: len %d, want %d", tc.key[0], len(tc.key), tc.want)
		}
	}
}

func TestZeroAllocEncode(t *testing.T) {
	addr := Address{1}
	slot := Hash{2}
	a := Account{Nonce: 7, Balance: *uint256.NewInt(9)}
	buf := make([]byte, 0, 128)
	allocs := testing.AllocsPerRun(100, func() {
		buf = AppendSlotKey(buf[:0], addr, slot, 123)
		buf = AppendAccountKey(buf[:0], addr, 123)
		buf = EncodeAccount(buf[:0], &a)
	})
	if allocs != 0 {
		t.Fatalf("encode into caller buffer allocated %v times", allocs)
	}
}
