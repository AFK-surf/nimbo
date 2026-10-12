package mesh

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/netip"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/curve25519"

	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/internal/node"
)

type tsNode struct {
	ts   *TS
	id   ids.BrokerID
	pub  [32]byte
	mu   sync.Mutex
	info node.MemberInfo
	got  chan struct{}
}

func newTSNode(t *testing.T, psk []byte) *tsNode {
	t.Helper()
	var priv [32]byte
	rand.Read(priv[:])
	priv[0] &= 248
	priv[31] = (priv[31] & 127) | 64
	pub, _ := curve25519.X25519(priv[:], curve25519.Basepoint)
	n := &tsNode{got: make(chan struct{}, 1)}
	copy(n.pub[:], pub)
	n.id = ids.BrokerIDFromPub(pub)
	ts, err := NewTS(TSConfig{
		NodePrivate: priv,
		PSK:         psk,
		Logf:        func(f string, a ...any) { t.Logf("[%s] "+f, append([]any{n.id.Abbrev()}, a...)...) },
		OnLocalInfo: func(disco string, eps []string, home int) {
			n.mu.Lock()
			n.info.DiscoPub = disco
			n.mu.Unlock()
			select {
			case n.got <- struct{}{}:
			default:
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ts.Close() })
	n.ts = ts
	return n
}

func (n *tsNode) member() node.Member {
	n.mu.Lock()
	defer n.mu.Unlock()
	info := n.info
	info.NodePub = fmt.Sprintf("%x", n.pub[:])
	info.Endpoints = []string{netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), n.ts.ms.LocalPort()).String()}
	return node.Member{ID: n.id, Info: info}
}

func TestTSMeshLoopback(t *testing.T) {
	if testing.Short() {
		t.Skip("starts two userspace WireGuard engines")
	}
	psk := make([]byte, 32)
	rand.Read(psk)
	a, b := newTSNode(t, psk), newTSNode(t, psk)
	<-a.got
	<-b.got
	ms := []node.Member{a.member(), b.member()}
	a.ts.SetPeers(ms)
	b.ts.SetPeers(ms)

	done := make(chan string, 1)
	go func() {
		c, from, err := b.ts.Accept()
		if err != nil {
			done <- err.Error()
			return
		}
		if from != a.id {
			done <- "wrong peer"
			return
		}
		buf := make([]byte, 5)
		io.ReadFull(c, buf)
		c.Write([]byte("pong!"))
		done <- string(buf)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var err error
	for ctx.Err() == nil {
		dctx, dcancel := context.WithTimeout(ctx, 3*time.Second)
		c, derr := a.ts.Dial(dctx, b.id)
		dcancel()
		if derr != nil {
			err = derr
			continue
		}
		c.Write([]byte("ping!"))
		buf := make([]byte, 5)
		if _, err = io.ReadFull(c, buf); err == nil && string(buf) == "pong!" {
			if got := <-done; got != "ping!" {
				t.Fatalf("server got %q", got)
			}
			return
		}
	}
	t.Fatalf("no connection over the mesh: %v", err)
}
