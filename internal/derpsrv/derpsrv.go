// Package derpsrv embeds a DERP relay and a STUN server in nimbod (design
// §5). Relay Brokers with the same region ID mesh with each other using the
// cluster's DERP mesh key; the DERP map itself is built by the mesh from
// membership.
package derpsrv

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"

	"tailscale.com/derp"
	"tailscale.com/derp/derphttp"
	"tailscale.com/derp/derpserver"
	"tailscale.com/net/netmon"
	"tailscale.com/net/stunserver"
	"tailscale.com/types/key"

	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/internal/node"
)

// Server is an embedded DERP relay.
type Server struct {
	self   ids.BrokerID
	region int
	logf   func(string, ...any)
	s      *derpserver.Server

	mu    sync.Mutex
	peers map[ids.BrokerID]context.CancelFunc
}

// New creates a relay for region with the cluster's DERP mesh key (hex).
func New(self ids.BrokerID, region int, meshKey string, logf func(string, ...any)) (*Server, error) {
	s := derpserver.New(key.NewNode(), logf)
	if err := s.SetMeshKey(meshKey); err != nil {
		return nil, fmt.Errorf("derp mesh key: %w", err)
	}
	return &Server{self: self, region: region, logf: logf, s: s, peers: map[ids.BrokerID]context.CancelFunc{}}, nil
}

// Register adds the DERP endpoints to mux.
func (d *Server) Register(mux *http.ServeMux) {
	mux.Handle("/derp", derpserver.Handler(d.s))
	mux.HandleFunc("/derp/probe", derpserver.ProbeHandler)
	mux.HandleFunc("/derp/latency-check", derpserver.ProbeHandler)
	mux.HandleFunc("/generate_204", derpserver.ServeNoContent)
}

// ServeSTUN serves STUN on addr (for example ":3478") until ctx ends.
func ServeSTUN(ctx context.Context, addr string) error {
	ss := stunserver.New(ctx)
	if err := ss.Listen(addr); err != nil {
		return err
	}
	go ss.Serve()
	return nil
}

// SetPeers meshes with the other relays of this region.
func (d *Server) SetPeers(members []node.Member) {
	want := map[ids.BrokerID]string{}
	for _, m := range members {
		if m.ID == d.self || m.Info.DERP == nil || m.Info.DERP.RegionID != d.region {
			continue
		}
		port := m.Info.DERP.DERPPort
		if port == 0 {
			port = 443
		}
		want[m.ID] = net.JoinHostPort(m.Info.DERP.HostName, strconv.Itoa(port))
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for id, cancel := range d.peers {
		if _, ok := want[id]; !ok {
			cancel()
			delete(d.peers, id)
		}
	}
	for id, hostport := range want {
		if _, ok := d.peers[id]; ok {
			continue
		}
		c, err := derphttp.NewClient(d.s.PrivateKey(), "https://"+hostport+"/derp", d.logf, netmon.NewStatic())
		if err != nil {
			d.logf("derp mesh with %s: %v", hostport, err)
			continue
		}
		c.MeshKey = d.s.MeshKey()
		c.WatchConnectionChanges = true
		ctx, cancel := context.WithCancel(context.Background())
		d.peers[id] = func() { cancel(); c.Close() }
		add := func(m derp.PeerPresentMessage) { d.s.AddPacketForwarder(m.Key, c) }
		remove := func(m derp.PeerGoneMessage) { d.s.RemovePacketForwarder(m.Peer, c) }
		go c.RunWatchConnectionLoop(ctx, d.s.PublicKey(), d.logf, add, remove, func(error) {})
	}
}
