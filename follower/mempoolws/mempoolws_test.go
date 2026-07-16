package mempoolws

import (
	"context"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ava-labs/libevm/common"
	ethtypes "github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/gorilla/websocket"
)

func makeTx(t *testing.T, nonce uint64) *ethtypes.Transaction {
	t.Helper()
	key, _ := crypto.ToECDSA(common.LeftPadBytes([]byte{9}, 32))
	to := common.HexToAddress("0xdd")
	tx := ethtypes.NewTx(&ethtypes.LegacyTx{
		Nonce: nonce, To: &to, Value: big.NewInt(1), Gas: 21000, GasPrice: big.NewInt(1),
	})
	signed, err := ethtypes.SignTx(tx, ethtypes.LatestSignerForChainID(big.NewInt(43114)), key)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

// wsServer answers the subscribe request and pushes the given txs.
func wsServer(t *testing.T, txs []*ethtypes.Transaction) *httptest.Server {
	t.Helper()
	up := websocket.Upgrader{}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		if _, _, err := conn.ReadMessage(); err != nil { // subscribe request
			return
		}
		if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","id":1,"result":"0xabc"}`)); err != nil {
			return
		}
		for _, tx := range txs {
			j, err := tx.MarshalJSON()
			if err != nil {
				t.Error(err)
				return
			}
			frame := `{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xabc","result":` + string(j) + `}}`
			if err := conn.WriteMessage(websocket.TextMessage, []byte(frame)); err != nil {
				return
			}
		}
		// Hold the connection open until the client goes away.
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
}

func TestDedupAcrossConnections(t *testing.T) {
	tx1, tx2 := makeTx(t, 0), makeTx(t, 1)
	// Both servers push tx1; only one pushes tx2.
	s1 := wsServer(t, []*ethtypes.Transaction{tx1, tx2})
	s2 := wsServer(t, []*ethtypes.Transaction{tx1})
	defer s1.Close()
	defer s2.Close()

	var (
		mu   sync.Mutex
		got  [][]byte
		seen = make(chan struct{}, 8)
	)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Config{
			URLs: []string{
				"ws" + strings.TrimPrefix(s1.URL, "http"),
				"ws" + strings.TrimPrefix(s2.URL, "http"),
			},
			Sink: func(tx []byte, timeMS uint64) error {
				if timeMS == 0 {
					t.Error("zero timestamp")
				}
				mu.Lock()
				got = append(got, tx)
				mu.Unlock()
				seen <- struct{}{}
				return nil
			},
		})
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-seen:
		case <-ctx.Done():
			t.Fatal("timed out waiting for txs")
		}
	}
	// Give the duplicate a moment to (not) arrive, then stop.
	time.Sleep(200 * time.Millisecond)
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("want 2 unique txs, got %d", len(got))
	}
	want1, _ := tx1.MarshalBinary()
	want2, _ := tx2.MarshalBinary()
	seenBin := map[string]bool{string(got[0]): true, string(got[1]): true}
	if !seenBin[string(want1)] || !seenBin[string(want2)] {
		t.Fatal("tx bytes mismatch")
	}
}

func TestSinkErrorIsFatal(t *testing.T) {
	tx := makeTx(t, 0)
	s := wsServer(t, []*ethtypes.Transaction{tx})
	defer s.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	err := Run(ctx, Config{
		URLs: []string{"ws" + strings.TrimPrefix(s.URL, "http")},
		Sink: func([]byte, uint64) error { return context.DeadlineExceeded },
	})
	if err == nil || ctx.Err() != nil {
		t.Fatalf("want fatal sink error, got %v", err)
	}
}
