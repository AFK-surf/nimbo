package mesh

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/netip"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/curve25519"
	"tailscale.com/tailcfg"

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
	return newTSNodeDERP(t, psk, nil, nil)
}

// newTSNodeDERP starts a node choosing its home relay from dm, calling
// unknown when WireGuard hears from a node it does not know.
func newTSNodeDERP(t *testing.T, psk []byte, dm *tailcfg.DERPMap, unknown func(ids.BrokerID)) *tsNode {
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
		NodePrivate:   priv,
		PSK:           psk,
		Logf:          func(f string, a ...any) { t.Logf("[%s] "+f, append([]any{n.id.Abbrev()}, a...)...) },
		OnUnknownPeer: unknown,
		DERPMap:       dm,
		OnLocalInfo: func(disco string, eps []string, derp json.RawMessage) {
			n.mu.Lock()
			n.info.DiscoPub = disco
			n.info.Endpoints = eps
			n.info.DERP = derp
			n.mu.Unlock()
			if len(eps) == 0 || (dm != nil && derp == nil) {
				return
			}
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
	// Advertise what magicsock reports (as nimbod does), plus loopback on
	// its current port: magicsock may rebind to a new port after start.
	info.Endpoints = append([]string{netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), n.ts.LocalPort()).String()}, info.Endpoints...)
	return node.Member{ID: n.id, Info: info}
}

func TestTSMeshLoopback(t *testing.T) {
	if testing.Short() {
		t.Skip("starts two userspace WireGuard engines")
	}
	psk := make([]byte, 32)
	rand.Read(psk)
	a, b := newTSNode(t, psk), newTSNode(t, psk)
	// Wait briefly for magicsock to report its endpoints; loopback on the
	// current port is advertised in any case.
	for _, n := range []*tsNode{a, b} {
		select {
		case <-n.got:
		case <-time.After(5 * time.Second):
		}
	}
	// Keep the peer configuration current, as membership updates do in
	// production.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			ms := []node.Member{a.member(), b.member()}
			a.ts.SetPeers(ms)
			b.ts.SetPeers(ms)
			select {
			case <-stop:
				return
			case <-time.After(time.Second):
			}
		}
	}()

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

// Each Broker chooses its own relay and publishes it. With no direct
// endpoints advertised, two Brokers on different relays must reach each
// other through the relay the other one published.
func TestTSMeshThroughPublishedRelays(t *testing.T) {
	if testing.Short() {
		t.Skip("uses Tailscale's public DERP relays")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	dm, err := fetchDERPMap(ctx, DefaultDERPMapURL)
	cancel()
	if err != nil {
		t.Skipf("no access to Tailscale's DERP map: %v", err)
	}
	only := func(id tailcfg.DERPRegionID) *tailcfg.DERPMap {
		r := dm.Regions[id]
		if r == nil {
			t.Skipf("Tailscale's DERP map has no region %d", id)
		}
		return &tailcfg.DERPMap{Regions: map[tailcfg.DERPRegionID]*tailcfg.DERPRegion{id: r}}
	}
	psk := make([]byte, 32)
	rand.Read(psk)
	a := newTSNodeDERP(t, psk, only(1), nil)  // nyc
	b := newTSNodeDERP(t, psk, only(12), nil) // ord
	for _, n := range []*tsNode{a, b} {
		select {
		case <-n.got:
		case <-time.After(20 * time.Second):
			t.Skip("no DERP home region chosen (relays unreachable?)")
		}
	}
	relayOnly := func(n *tsNode) node.Member {
		m := n.member()
		m.Info.Endpoints = nil
		return m
	}
	ms := []node.Member{relayOnly(a), relayOnly(b)}
	a.ts.SetPeers(ms)
	b.ts.SetPeers(ms)
	go func() {
		c, _, err := a.ts.Accept()
		if err == nil {
			io.Copy(c, c)
		}
	}()
	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var lastErr error
	for ctx.Err() == nil {
		dctx, dcancel := context.WithTimeout(ctx, 5*time.Second)
		c, err := b.ts.Dial(dctx, a.id)
		dcancel()
		if err != nil {
			lastErr = err
			continue
		}
		c.Write([]byte("ping!"))
		buf := make([]byte, 5)
		c.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, err = io.ReadFull(c, buf)
		c.Close()
		if err == nil && string(buf) == "ping!" {
			return
		}
		lastErr = err
	}
	t.Fatalf("no connection through the published relays: %v", lastErr)
}
