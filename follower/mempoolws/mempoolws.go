// Package mempoolws captures mempool arrivals (DESIGN.md D2 rev 2): NOT from
// our p2p, but a plain WebSocket client subscribed to newPendingTransactions
// (full transactions) on N external nodes. Arrivals are deduped by tx hash
// across all connections and timestamped at first receipt (unix ms).
package mempoolws

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	ethtypes "github.com/ava-labs/libevm/core/types"
	"github.com/gorilla/websocket"
)

// dedupWindow is how long a seen tx hash suppresses duplicates. Re-gossiped
// transactions older than this are recorded again; the store's arrival log
// is chronology, not identity.
const dedupWindow = 10 * time.Minute

type Config struct {
	// URLs are WebSocket endpoints (ws://host/ext/bc/C/ws). Every URL gets
	// its own connection with independent reconnect backoff.
	URLs []string
	// Sink receives each first-seen raw tx (eth binary encoding) with its
	// arrival time in unix ms. A Sink error is fatal (mempool arrivals are
	// irrecoverable data, D4): Run returns it.
	Sink func(tx []byte, timeMS uint64) error
	Log  *slog.Logger
}

// Run connects to every URL and pumps arrivals into the sink until ctx ends
// or the sink fails. Connection errors reconnect with backoff, forever.
func Run(ctx context.Context, cfg Config) error {
	if len(cfg.URLs) == 0 {
		return fmt.Errorf("mempoolws: no URLs")
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	d := &dedup{seen: make(map[[32]byte]time.Time)}
	fatal := make(chan error, 1)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	for _, url := range cfg.URLs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			connLoop(ctx, url, cfg, d, fatal)
		}()
	}
	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case err = <-fatal:
		cancel()
	}
	wg.Wait()
	return err
}

type dedup struct {
	mu   sync.Mutex
	seen map[[32]byte]time.Time
	n    int
}

// firstSeen reports whether the hash is new and records it.
func (d *dedup) firstSeen(h [32]byte, now time.Time) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if t, ok := d.seen[h]; ok && now.Sub(t) < dedupWindow {
		return false
	}
	d.seen[h] = now
	// Opportunistic prune, amortized: every 4096 inserts sweep expired.
	if d.n++; d.n >= 4096 {
		d.n = 0
		for k, t := range d.seen {
			if now.Sub(t) >= dedupWindow {
				delete(d.seen, k)
			}
		}
	}
	return true
}

func connLoop(ctx context.Context, url string, cfg Config, d *dedup, fatal chan<- error) {
	backoff := time.Second
	for ctx.Err() == nil {
		err := runConn(ctx, url, cfg, d, fatal)
		if ctx.Err() != nil {
			return
		}
		cfg.Log.Warn("mempool ws disconnected", "url", url, "err", err, "retry_in", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
	}
}

type wsMessage struct {
	ID     json.RawMessage `json:"id"`
	Error  *struct {
		Message string `json:"message"`
	} `json:"error"`
	Method string `json:"method"`
	Params struct {
		Result json.RawMessage `json:"result"`
	} `json:"params"`
}

func runConn(ctx context.Context, url string, cfg Config, d *dedup, fatal chan<- error) error {
	dialCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	conn, _, err := websocket.DefaultDialer.DialContext(dialCtx, url, nil)
	cancel()
	if err != nil {
		return err
	}
	defer conn.Close()
	// Unblock the read loop on ctx cancellation.
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()

	// Subscribe to full pending transactions.
	sub := `{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newPendingTransactions",true]}`
	if err := conn.WriteMessage(websocket.TextMessage, []byte(sub)); err != nil {
		return err
	}
	cfg.Log.Info("mempool ws connected", "url", url)

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		var msg wsMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			cfg.Log.Warn("mempool ws bad frame", "url", url, "err", err)
			continue
		}
		if msg.Error != nil {
			return fmt.Errorf("mempoolws: subscribe rejected: %s", msg.Error.Message)
		}
		if msg.Method != "eth_subscription" || len(msg.Params.Result) == 0 {
			continue // subscribe ack or unrelated frame
		}
		var tx ethtypes.Transaction
		if err := tx.UnmarshalJSON(msg.Params.Result); err != nil {
			// Node sent hashes (no fullTx support) or an unknown tx type.
			cfg.Log.Warn("mempool ws unparseable tx", "url", url, "err", err)
			continue
		}
		now := time.Now()
		if !d.firstSeen(tx.Hash(), now) {
			continue
		}
		raw, err := tx.MarshalBinary()
		if err != nil {
			cfg.Log.Warn("mempool ws tx encode", "err", err)
			continue
		}
		if err := cfg.Sink(raw, uint64(now.UnixMilli())); err != nil {
			select {
			case fatal <- fmt.Errorf("mempoolws: sink: %w", err):
			default:
			}
			return err
		}
	}
}
