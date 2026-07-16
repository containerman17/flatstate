// Package schema defines the LMDB key layout (DESIGN.md D6) and the fixed
// binary encodings shared by every other package. All encoders append into
// caller-provided buffers and allocate nothing.
package schema

import (
	"encoding/binary"
	"fmt"

	"github.com/holiman/uint256"
)

// Keyspace prefixes per D6.
const (
	PrefMeta     byte = 0x00
	PrefAccount  byte = 0x01 // 0x01 | addr(20) | ^block(8) -> account post-image
	PrefSlot     byte = 0x02 // 0x02 | addr(20) | slot(32) | ^block(8) -> slot value post-image
	PrefDestruct byte = 0x03 // 0x03 | addr(20) | ^block(8) -> destruct marker (empty value)
	PrefDiff     byte = 0x04 // 0x04 | block(8) -> per-block diff (capture batch verbatim)
	PrefMempool  byte = 0x05 // 0x05 | seq(8) -> arrival time(8) + tx bytes
	PrefCode     byte = 0x06 // 0x06 | code hash(32) -> contract code
	// Hash-keyed snapshot baseline at S (D6 rev 2). Probed once per cold key
	// after a preimage-history miss; one keccak, off the steady-state path.
	PrefBaseAccount byte = 0x07 // 0x07 | keccak(addr)(32) -> account post-image at S
	PrefBaseSlot    byte = 0x08 // 0x08 | keccak(addr)(32) | keccak(slot)(32) -> slot value at S
)

// Meta keys (prefix 0x00).
var (
	MetaGenesis          = []byte{PrefMeta, 0x01} // history genesis S (8 bytes BE)
	MetaBaselineComplete = []byte{PrefMeta, 0x02} // presence = baseline complete
	MetaFinalized        = []byte{PrefMeta, 0x03} // finalized height (8 bytes BE)
	MetaBaselineProgress = []byte{PrefMeta, 0x04} // opaque loader resume cursor; deleted by Finish
)

// Key lengths.
const (
	AccountKeyLen     = 1 + 20 + 8
	SlotKeyLen        = 1 + 20 + 32 + 8
	DestructKeyLen    = 1 + 20 + 8
	DiffKeyLen        = 1 + 8
	MempoolKeyLen     = 1 + 8
	CodeKeyLen        = 1 + 32
	BaseAccountKeyLen = 1 + 32
	BaseSlotKeyLen    = 1 + 32 + 32
)

type (
	Address [20]byte
	Hash    [32]byte
)

// SKey identifies a storage slot.
type SKey struct {
	Addr Address
	Slot Hash
}

// Account is the fixed-size account post-image: balance(32) nonce(8) codehash(32).
type Account struct {
	Balance  uint256.Int
	Nonce    uint64
	CodeHash Hash
}

const AccountSize = 32 + 8 + 32

// EncodeAccount appends the 72-byte fixed encoding.
func EncodeAccount(dst []byte, a *Account) []byte {
	b32 := a.Balance.Bytes32()
	dst = append(dst, b32[:]...)
	dst = binary.BigEndian.AppendUint64(dst, a.Nonce)
	return append(dst, a.CodeHash[:]...)
}

// DecodeAccount parses the 72-byte fixed encoding.
func DecodeAccount(b []byte) (Account, error) {
	if len(b) != AccountSize {
		return Account{}, fmt.Errorf("schema: account encoding is %d bytes, want %d", len(b), AccountSize)
	}
	var a Account
	a.Balance.SetBytes32(b[:32])
	a.Nonce = binary.BigEndian.Uint64(b[32:40])
	copy(a.CodeHash[:], b[40:72])
	return a, nil
}

// InvBlock is the bitwise-inverted block number. For a fixed key prefix,
// rows sort by ^block ascending, so a forward SetRange on key||^B lands on
// the greatest write at or before B in one hop.
func InvBlock(block uint64) uint64 { return ^block }

// AppendInvBlock appends ^block big-endian.
func AppendInvBlock(dst []byte, block uint64) []byte {
	return binary.BigEndian.AppendUint64(dst, ^block)
}

// DecodeInvBlock recovers the block number from the trailing 8 bytes of a key.
func DecodeInvBlock(keyTail []byte) uint64 {
	return ^binary.BigEndian.Uint64(keyTail)
}

func AppendAccountKey(dst []byte, addr Address, block uint64) []byte {
	dst = append(dst, PrefAccount)
	dst = append(dst, addr[:]...)
	return AppendInvBlock(dst, block)
}

func AppendSlotKey(dst []byte, addr Address, slot Hash, block uint64) []byte {
	dst = append(dst, PrefSlot)
	dst = append(dst, addr[:]...)
	dst = append(dst, slot[:]...)
	return AppendInvBlock(dst, block)
}

func AppendDestructKey(dst []byte, addr Address, block uint64) []byte {
	dst = append(dst, PrefDestruct)
	dst = append(dst, addr[:]...)
	return AppendInvBlock(dst, block)
}

func AppendDiffKey(dst []byte, block uint64) []byte {
	dst = append(dst, PrefDiff)
	return binary.BigEndian.AppendUint64(dst, block)
}

func AppendMempoolKey(dst []byte, seq uint64) []byte {
	dst = append(dst, PrefMempool)
	return binary.BigEndian.AppendUint64(dst, seq)
}

func AppendCodeKey(dst []byte, hash Hash) []byte {
	dst = append(dst, PrefCode)
	return append(dst, hash[:]...)
}

// Baseline keys take the ALREADY-HASHED keccak(addr) / keccak(slot); the
// store computes the hashes so callers stay preimage-only.
func AppendBaseAccountKey(dst []byte, addrHash Hash) []byte {
	dst = append(dst, PrefBaseAccount)
	return append(dst, addrHash[:]...)
}

func AppendBaseSlotKey(dst []byte, addrHash Hash, slotHash Hash) []byte {
	dst = append(dst, PrefBaseSlot)
	dst = append(dst, addrHash[:]...)
	return append(dst, slotHash[:]...)
}
