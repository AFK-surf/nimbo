// Package mesh provides Broker-to-Broker transports for internal/host.
//
// TS (tailscale.go) is the production transport of design §5: Tailscale's
// data plane (magicsock, wireguard-go, gVisor netstack) with no control
// plane. TCP is a plain-TCP transport for development and tests on a
// trusted network: it authenticates Brokers with the cluster PSK but does
// not encrypt.
package mesh

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/internal/node"
)

// TCP is the development transport. Each Broker advertises its listen
// address as the first entry of MemberInfo.Endpoints.
type TCP struct {
	self ids.BrokerID
	psk  []byte
	ln   net.Listener

	mu    sync.Mutex
	addrs map[ids.BrokerID]string
}

// NewTCP listens on addr.
func NewTCP(self ids.BrokerID, psk []byte, addr string) (*TCP, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &TCP{self: self, psk: psk, ln: ln, addrs: map[ids.BrokerID]string{}}, nil
}

// Addr is the listen address.
func (t *TCP) Addr() net.Addr { return t.ln.Addr() }

func (t *TCP) mac(label string, nonce []byte, id ids.BrokerID) []byte {
	m := hmac.New(sha256.New, t.psk)
	m.Write([]byte("nimbo/v1/tcpmesh/" + label))
	m.Write(nonce)
	m.Write(id[:])
	return m.Sum(nil)
}

// Accept implements host.Mesh.
func (t *TCP) Accept() (net.Conn, ids.BrokerID, error) {
	for {
		c, err := t.ln.Accept()
		if err != nil {
			return nil, ids.BrokerID{}, err
		}
		c.SetDeadline(time.Now().Add(5 * time.Second))
		var hello [64]byte
		if _, err := io.ReadFull(c, hello[:]); err != nil {
			c.Close()
			continue
		}
		var peer ids.BrokerID
		copy(peer[:], hello[:32])
		var ns [32]byte
		rand.Read(ns[:])
		resp := append(append(append([]byte{}, t.self[:]...), ns[:]...), t.mac("s", hello[32:], t.self)...)
		if _, err := c.Write(resp); err != nil {
			c.Close()
			continue
		}
		proof := make([]byte, 32)
		if _, err := io.ReadFull(c, proof); err != nil || !hmac.Equal(proof, t.mac("c", ns[:], peer)) {
			c.Close()
			continue
		}
		c.SetDeadline(time.Time{})
		return c, peer, nil
	}
}

// Dial implements host.Mesh.
func (t *TCP) Dial(ctx context.Context, to ids.BrokerID) (net.Conn, error) {
	t.mu.Lock()
	addr, ok := t.addrs[to]
	t.mu.Unlock()
	if !ok {
		return nil, errors.New("mesh: unknown peer")
	}
	var d net.Dialer
	c, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	c.SetDeadline(time.Now().Add(5 * time.Second))
	var nc [32]byte
	rand.Read(nc[:])
	if _, err := c.Write(append(append([]byte{}, t.self[:]...), nc[:]...)); err != nil {
		c.Close()
		return nil, err
	}
	var resp [96]byte
	if _, err := io.ReadFull(c, resp[:]); err != nil {
		c.Close()
		return nil, err
	}
	var peer ids.BrokerID
	copy(peer[:], resp[:32])
	if peer != to || !hmac.Equal(resp[64:], t.mac("s", nc[:], peer)) {
		c.Close()
		return nil, errors.New("mesh: peer authentication failed")
	}
	if _, err := c.Write(t.mac("c", resp[32:64], t.self)); err != nil {
		c.Close()
		return nil, err
	}
	c.SetDeadline(time.Time{})
	return c, nil
}

// SetPeers implements host.Mesh.
func (t *TCP) SetPeers(ms []node.Member) {
	addrs := map[ids.BrokerID]string{}
	for _, m := range ms {
		if len(m.Info.Endpoints) > 0 {
			addrs[m.ID] = m.Info.Endpoints[0]
		}
	}
	t.mu.Lock()
	t.addrs = addrs
	t.mu.Unlock()
}

// Close implements host.Mesh.
func (t *TCP) Close() error { return t.ln.Close() }
