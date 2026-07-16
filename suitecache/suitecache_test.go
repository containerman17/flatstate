package suitecache

import (
	"os"
	"testing"

	"github.com/holiman/uint256"

	"github.com/containerman17/flatstate/capture"
	"github.com/containerman17/flatstate/schema"
	"github.com/containerman17/flatstate/store"
)

var (
	addrA = schema.Address{0xaa}
	addrB = schema.Address{0xbb}
	s1    = schema.Hash{1}
	ch    = schema.Hash{0xc0}
)

func h(b byte) schema.Hash { return schema.Hash{31: b} }

func newStore(t *testing.T) *store.DB {
	t.Helper()
	d, err := store.Open(t.TempDir(), 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	bl, err := d.NewBaseline(10)
	if err != nil {
		t.Fatal(err)
	}
	a := schema.Account{Balance: *uint256.NewInt(5), Nonce: 1, CodeHash: ch}
	if err := bl.Account(addrA, &a); err != nil {
		t.Fatal(err)
	}
	if err := bl.Slot(addrA, s1, h(0x10)); err != nil {
		t.Fatal(err)
	}
	if err := bl.Code(ch, []byte{0xde, 0xad}); err != nil {
		t.Fatal(err)
	}
	if err := bl.Finish(); err != nil {
		t.Fatal(err)
	}
	b11 := &capture.Batch{Block: 11, Hash: h(11), Time: 1000, Ops: []capture.Op{
		{Kind: capture.OpSlot, Addr: addrA, Slot: s1, Value: h(0x21)},
	}}
	if err := d.WriteBlock(b11); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestCacheLifecycle(t *testing.T) {
	d := newStore(t)
	dir := t.TempDir()

	// Run 1: everything misses through to the store, Close writes the file.
	c, err := Open(dir, "suite", 10, d)
	if err != nil {
		t.Fatal(err)
	}
	if v, err := c.Slot(addrA, s1); err != nil || v != h(0x10) {
		t.Fatalf("slot = %x %v", v, err)
	}
	if a, exists, err := c.Account(addrA); err != nil || !exists || a.Balance.Uint64() != 5 {
		t.Fatalf("account = %+v %v %v", a, exists, err)
	}
	if _, exists, err := c.Account(addrB); err != nil || exists {
		t.Fatalf("nonexistent account: %v %v", exists, err)
	}
	if code, err := c.Code(ch); err != nil || string(code) != "\xde\xad" {
		t.Fatalf("code = %x %v", code, err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	// Run 2: no store attached; everything must come from the file,
	// including cached nonexistence.
	c2, err := Open(dir, "suite", 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if v, err := c2.Slot(addrA, s1); err != nil || v != h(0x10) {
		t.Fatalf("cached slot = %x %v", v, err)
	}
	if a, exists, err := c2.Account(addrA); err != nil || !exists || a.Balance.Uint64() != 5 || a.CodeHash != ch {
		t.Fatalf("cached account = %+v %v %v", a, exists, err)
	}
	if _, exists, err := c2.Account(addrB); err != nil || exists {
		t.Fatalf("cached nonexistence: %v %v", exists, err)
	}
	if code, err := c2.Code(ch); err != nil || string(code) != "\xde\xad" {
		t.Fatalf("cached code = %x %v", code, err)
	}
	// A genuine miss with no store fails loud.
	if _, err := c2.Slot(addrB, s1); err == nil {
		t.Fatal("miss without store must fail")
	}
	// Nothing added: Close must not rewrite.
	before, err := os.Stat(c2.path)
	if err != nil {
		t.Fatal(err)
	}
	if err := c2.Close(); err != nil {
		t.Fatal(err)
	}
	after, _ := os.Stat(c2.path)
	if !after.ModTime().Equal(before.ModTime()) || after.Size() != before.Size() {
		t.Fatal("clean close must not rewrite the file")
	}

	// Run 3: new key falls through, gets added, Close rewrites.
	c3, err := Open(dir, "suite", 10, d)
	if err != nil {
		t.Fatal(err)
	}
	if v, err := c3.Slot(addrB, s1); err != nil || v != (schema.Hash{}) {
		t.Fatalf("new slot = %x %v", v, err)
	}
	if err := c3.Close(); err != nil {
		t.Fatal(err)
	}
	c4, err := Open(dir, "suite", 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if v, err := c4.Slot(addrB, s1); err != nil || v != (schema.Hash{}) {
		t.Fatal("rewritten file lost the added key")
	}
}

// Different blocks and suites get independent files; state is read at the
// cache's own height.
func TestPerSuitePerBlock(t *testing.T) {
	d := newStore(t)
	dir := t.TempDir()
	c10, err := Open(dir, "s", 10, d)
	if err != nil {
		t.Fatal(err)
	}
	c11, err := Open(dir, "s", 11, d)
	if err != nil {
		t.Fatal(err)
	}
	v10, _ := c10.Slot(addrA, s1)
	v11, _ := c11.Slot(addrA, s1)
	if v10 != h(0x10) || v11 != h(0x21) {
		t.Fatalf("heights mixed up: %x %x", v10, v11)
	}
	if err := c10.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c11.Close(); err != nil {
		t.Fatal(err)
	}
	// A cache file refuses to load for the wrong block.
	if _, err := Open(dir, "s-10", 99, nil); err != nil {
		t.Fatal(err) // different name: fine, empty cache
	}
	if err := os.Rename(dir+"/s-10.fsc", dir+"/s-99.fsc"); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, "s", 99, nil); err == nil {
		t.Fatal("wrong-block file must be rejected")
	}
}
