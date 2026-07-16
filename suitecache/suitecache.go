// Package suitecache is the per-(suite, block) flat-file snapshot cache for
// correctness tests (DESIGN.md D11). One file per suite and block, loaded
// into plain maps; misses fall through to the main store at height B and are
// added; Close rewrites the file if anything was added. State at a fixed
// height never changes, so there is no invalidation. Single-threaded by
// design, like the test loops it serves.
package suitecache

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/containerman17/flatstate/schema"
	"github.com/containerman17/flatstate/store"
)

var magic = [8]byte{'F', 'S', 'C', '1', 0, 0, 0, 0}

// Record tags.
const (
	recAccount byte = 1 // addr(20) exists(1) account(72)
	recSlot    byte = 2 // addr(20) slot(32) value(32)
	recCode    byte = 3 // hash(32) len(4) code
)

type acct struct {
	a      schema.Account
	exists bool
}

// Cache is one suite's state cache at a fixed block.
type Cache struct {
	db    *store.DB
	block uint64
	path  string
	dirty bool

	accounts map[schema.Address]acct
	slots    map[schema.SKey]schema.Hash
	code     map[schema.Hash][]byte
}

// Open loads (or starts) the cache file for (suite, block). db may be nil
// for a file-only run; misses then fail loud.
func Open(dir, suite string, block uint64, db *store.DB) (*Cache, error) {
	c := &Cache{
		db:       db,
		block:    block,
		path:     filepath.Join(dir, fmt.Sprintf("%s-%d.fsc", suite, block)),
		accounts: make(map[schema.Address]acct),
		slots:    make(map[schema.SKey]schema.Hash),
		code:     make(map[schema.Hash][]byte),
	}
	data, err := os.ReadFile(c.path)
	if errors.Is(err, os.ErrNotExist) {
		return c, nil
	}
	if err != nil {
		return nil, err
	}
	if err := c.load(data); err != nil {
		return nil, fmt.Errorf("suitecache %s: %w", c.path, err)
	}
	return c, nil
}

func (c *Cache) load(data []byte) error {
	if len(data) < 16 || [8]byte(data[:8]) != magic {
		return errors.New("bad magic")
	}
	if b := binary.BigEndian.Uint64(data[8:16]); b != c.block {
		return fmt.Errorf("file is for block %d, want %d", b, c.block)
	}
	data = data[16:]
	for len(data) > 0 {
		tag := data[0]
		data = data[1:]
		switch tag {
		case recAccount:
			if len(data) < 20+1+schema.AccountSize {
				return io.ErrUnexpectedEOF
			}
			var addr schema.Address
			copy(addr[:], data[:20])
			a, err := schema.DecodeAccount(data[21 : 21+schema.AccountSize])
			if err != nil {
				return err
			}
			c.accounts[addr] = acct{a: a, exists: data[20] == 1}
			data = data[21+schema.AccountSize:]
		case recSlot:
			if len(data) < 20+32+32 {
				return io.ErrUnexpectedEOF
			}
			var sk schema.SKey
			copy(sk.Addr[:], data[:20])
			copy(sk.Slot[:], data[20:52])
			var v schema.Hash
			copy(v[:], data[52:84])
			c.slots[sk] = v
			data = data[84:]
		case recCode:
			if len(data) < 36 {
				return io.ErrUnexpectedEOF
			}
			var h schema.Hash
			copy(h[:], data[:32])
			l := binary.BigEndian.Uint32(data[32:36])
			if len(data) < 36+int(l) {
				return io.ErrUnexpectedEOF
			}
			c.code[h] = append([]byte(nil), data[36:36+l]...)
			data = data[36+int(l):]
		default:
			return fmt.Errorf("unknown record tag %d", tag)
		}
	}
	return nil
}

func (c *Cache) miss() error {
	if c.db == nil {
		return errors.New("suitecache: miss with no store attached")
	}
	return nil
}

// Account reads at the cache's block, falling through to the store on miss.
func (c *Cache) Account(addr schema.Address) (schema.Account, bool, error) {
	if e, ok := c.accounts[addr]; ok {
		return e.a, e.exists, nil
	}
	if err := c.miss(); err != nil {
		return schema.Account{}, false, err
	}
	a, exists, err := c.db.GetAccount(addr, c.block)
	if err != nil {
		return schema.Account{}, false, err
	}
	c.accounts[addr] = acct{a: a, exists: exists}
	c.dirty = true
	return a, exists, nil
}

// Slot reads at the cache's block, falling through to the store on miss.
func (c *Cache) Slot(addr schema.Address, slot schema.Hash) (schema.Hash, error) {
	sk := schema.SKey{Addr: addr, Slot: slot}
	if v, ok := c.slots[sk]; ok {
		return v, nil
	}
	if err := c.miss(); err != nil {
		return schema.Hash{}, err
	}
	v, err := c.db.GetSlot(addr, slot, c.block)
	if err != nil {
		return schema.Hash{}, err
	}
	c.slots[sk] = v
	c.dirty = true
	return v, nil
}

// Code reads contract code, falling through to the store on miss.
func (c *Cache) Code(hash schema.Hash) ([]byte, error) {
	if v, ok := c.code[hash]; ok {
		return v, nil
	}
	if err := c.miss(); err != nil {
		return nil, err
	}
	v, err := c.db.GetCode(hash)
	if err != nil {
		return nil, err
	}
	c.code[hash] = v
	c.dirty = true
	return v, nil
}

// Close rewrites the file (atomically, via rename) if anything was added.
func (c *Cache) Close() error {
	if !c.dirty {
		return nil
	}
	buf := append([]byte(nil), magic[:]...)
	buf = binary.BigEndian.AppendUint64(buf, c.block)
	for addr, e := range c.accounts {
		buf = append(buf, recAccount)
		buf = append(buf, addr[:]...)
		if e.exists {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
		buf = schema.EncodeAccount(buf, &e.a)
	}
	for sk, v := range c.slots {
		buf = append(buf, recSlot)
		buf = append(buf, sk.Addr[:]...)
		buf = append(buf, sk.Slot[:]...)
		buf = append(buf, v[:]...)
	}
	for h, code := range c.code {
		buf = append(buf, recCode)
		buf = append(buf, h[:]...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(code)))
		buf = append(buf, code...)
	}
	tmp := c.path + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, c.path); err != nil {
		return err
	}
	c.dirty = false
	return nil
}
