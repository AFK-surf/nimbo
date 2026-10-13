package mesh

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sort"
	"strconv"
	"sync"

	"go4.org/mem"
	"go4.org/netipx"
	"tailscale.com/net/dns"
	"tailscale.com/net/netmon"
	"tailscale.com/net/tsdial"
	"tailscale.com/tailcfg"
	"tailscale.com/tsd"
	"tailscale.com/types/ipproto"
	"tailscale.com/types/key"
	"tailscale.com/types/netmap"
	"tailscale.com/types/views"
	"tailscale.com/wgengine"
	"tailscale.com/wgengine/filter"
	"tailscale.com/wgengine/magicsock"
	"tailscale.com/wgengine/netstack"
	"tailscale.com/wgengine/router"
	"tailscale.com/wgengine/wgcfg"

	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/internal/node"

	// DERP connections honor HTTPS_PROXY, for Brokers behind an egress proxy.
	_ "tailscale.com/feature/useproxy"
)

// PeerPort is the peer RPC port inside the netstack (design §5).
const PeerPort = 7946

// ULAPrefix is the mesh's IPv6 prefix, fd4e:6d62:6f00::/40.
var ULAPrefix = netip.MustParsePrefix("fd4e:6d62:6f00::/40")

// ULA derives a Broker's mesh address: the /40 prefix plus 88 bits of its ID.
func ULA(id ids.BrokerID) netip.Addr {
	var a [16]byte
	copy(a[:5], []byte{0xfd, 0x4e, 0x6d, 0x62, 0x6f})
	copy(a[5:], id[:11])
	return netip.AddrFrom16(a)
}

// TSConfig configures the Tailscale mesh.
type TSConfig struct {
	NodePrivate [32]byte
	PSK         []byte
	// Port is magicsock's UDP port (0 picks one).
	Port uint16
	// StaticEndpoints are advertised in addition to discovered ones.
	StaticEndpoints []netip.AddrPort
	Logf            func(format string, args ...any)
	// OnUnknownPeer is called when WireGuard hears from a node key that is
	// not a member (fast join, design §4).
	OnUnknownPeer func(ids.BrokerID)
	// OnLocalInfo reports this Broker's disco key, current UDP endpoints and
	// home DERP region (as published in its member object, nil if none).
	OnLocalInfo func(disco string, endpoints []string, derp json.RawMessage)
	// DERPMap holds the DERP relays this Broker may use as its home region
	// (see LoadDERPMap and RelayMap). Nil means this Broker has no relay of
	// its own; it can still reach peers through theirs.
	DERPMap *tailcfg.DERPMap
}

// TS is the production mesh: magicsock + userspace wireguard-go + gVisor
// netstack, configured from S3 membership with no control plane.
type TS struct {
	cfg    TSConfig
	self   ids.BrokerID
	selfIP netip.Addr
	priv   key.NodePrivate
	sys    *tsd.System
	eng    wgengine.Engine
	ns     *netstack.Impl
	ms     *magicsock.Conn

	accepts chan accepted
	closed  chan struct{}
	once    sync.Once

	mu       sync.Mutex
	byKey    map[key.NodePublic]ids.BrokerID
	byIP     map[netip.Addr]key.NodePublic
	disco    map[key.NodePublic]key.DiscoPublic
	endpts   []string
	homeDERP int
	derp     derpIndex
	members  []node.Member // last SetPeers
}

type accepted struct {
	c    net.Conn
	from ids.BrokerID
}

func nodeID(id ids.BrokerID) tailcfg.NodeID {
	return tailcfg.NodeID(binary.BigEndian.Uint64(id[:8]) >> 2)
}

// NewTS starts the data plane.
func NewTS(cfg TSConfig) (*TS, error) {
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	priv := key.NodePrivateFromRaw32(mem.B(cfg.NodePrivate[:]))
	pub := priv.Public().Raw32()
	self := ids.BrokerIDFromPub(pub[:])
	t := &TS{
		cfg:     cfg,
		self:    self,
		selfIP:  ULA(self),
		priv:    priv,
		accepts: make(chan accepted, 64),
		closed:  make(chan struct{}),
		byKey:   map[key.NodePublic]ids.BrokerID{},
		byIP:    map[netip.Addr]key.NodePublic{},
		disco:   map[key.NodePublic]key.DiscoPublic{},
	}
	logf := func(format string, args ...any) { cfg.Logf(format, args...) }
	sys := tsd.NewSystem()
	t.sys = sys
	netMon, err := netmon.New(sys.Bus.Get(), logf)
	if err != nil {
		return nil, err
	}
	dialer := &tsdial.Dialer{Logf: logf}
	dialer.SetBus(sys.Bus.Get())
	eng, err := wgengine.NewUserspaceEngine(logf, wgengine.Config{
		EventBus:      sys.Bus.Get(),
		ListenPort:    cfg.Port,
		NetMon:        netMon,
		Dialer:        dialer,
		SetSubsystem:  sys.Set,
		ControlKnobs:  sys.ControlKnobs(),
		HealthTracker: sys.HealthTracker.Get(),
		Metrics:       sys.UserMetricsRegistry(),
	})
	if err != nil {
		return nil, err
	}
	t.eng = eng
	sys.Set(eng)
	ns, err := netstack.Create(logf, sys.Tun.Get(), eng, sys.MagicSock.Get(), dialer, sys.DNSManager.Get(), sys.ProxyMapper())
	if err != nil {
		eng.Close()
		return nil, fmt.Errorf("netstack: %w", err)
	}
	t.ns = ns
	t.ms = sys.MagicSock.Get()
	sys.Tun.Get().Start()
	ns.ProcessLocalIPs = true
	ns.GetTCPHandlerForFlow = t.tcpHandler
	if err := ns.Start(nil); err != nil {
		eng.Close()
		return nil, err
	}
	eng.SetPeerConfigFunc(t.peerConfig)
	eng.SetPeerByIPPacketFunc(t.peerByIP)
	eng.SetFilter(t.filter())
	t.ms.SetNetInfoCallback(func(ni *tailcfg.NetInfo) {
		t.mu.Lock()
		t.homeDERP = int(ni.PreferredDERP)
		t.mu.Unlock()
		t.reportLocal()
	})
	eng.SetStatusCallback(func(st *wgengine.Status, err error) {
		if err != nil || st == nil {
			return
		}
		eps := make([]string, 0, len(st.LocalAddrs)+len(cfg.StaticEndpoints))
		for _, ap := range cfg.StaticEndpoints {
			eps = append(eps, ap.String())
		}
		for _, ep := range st.LocalAddrs {
			eps = append(eps, ep.Addr.String())
		}
		sort.Strings(eps)
		eps = compactStrings(eps)
		t.mu.Lock()
		changed := fmt.Sprint(eps) != fmt.Sprint(t.endpts)
		t.endpts = eps
		t.mu.Unlock()
		if changed {
			t.reportLocal()
		}
	})
	if len(cfg.StaticEndpoints) > 0 {
		t.ms.SetStaticEndpoints(views.SliceOf(cfg.StaticEndpoints))
	}
	pfx := netip.PrefixFrom(t.selfIP, 128)
	if err := eng.Reconfig(&wgcfg.Config{PrivateKey: priv, Addresses: []netip.Prefix{pfx}}, &router.Config{LocalAddrs: []netip.Prefix{pfx}}, &dns.Config{}); err != nil && !errors.Is(err, wgengine.ErrNoChanges) {
		eng.Close()
		return nil, err
	}
	selfNode := t.selfNode().View()
	eng.SetSelfNode(selfNode)
	ns.UpdateNetstackIPs(&netmap.NetworkMap{SelfNode: selfNode})
	t.ms.SetNetworkMap(selfNode, nil)
	if cfg.DERPMap != nil {
		if err := checkOwnMap(cfg.DERPMap); err != nil {
			eng.Close()
			return nil, err
		}
		t.derp.setOwn(cfg.DERPMap)
		dm, _ := t.derp.build(nil)
		t.ms.SetDERPMap(dm)
	}
	t.reportLocal()
	return t, nil
}

func compactStrings(s []string) []string {
	out := s[:0]
	for i, v := range s {
		if i == 0 || v != s[i-1] {
			out = append(out, v)
		}
	}
	return out
}

func (t *TS) selfNode() *tailcfg.Node {
	pfx := netip.PrefixFrom(t.selfIP, 128)
	return &tailcfg.Node{
		ID:                nodeID(t.self),
		StableID:          tailcfg.StableNodeID(t.self.String()),
		Name:              t.self.Abbrev() + ".nimbo.",
		Key:               t.priv.Public(),
		DiscoKey:          t.ms.DiscoPublicKey(),
		Addresses:         []netip.Prefix{pfx},
		AllowedIPs:        []netip.Prefix{pfx},
		MachineAuthorized: true,
	}
}

func (t *TS) reportLocal() {
	if t.cfg.OnLocalInfo == nil {
		return
	}
	dk, _ := t.ms.DiscoPublicKey().MarshalText()
	t.mu.Lock()
	eps := append([]string(nil), t.endpts...)
	var derp json.RawMessage
	if t.derp.own != nil {
		if r := t.derp.own.Regions[tailcfg.DERPRegionID(t.homeDERP)]; r != nil {
			derp = publishedRegion(r)
		}
	}
	t.mu.Unlock()
	t.cfg.OnLocalInfo(string(dk), eps, derp)
}

// filter admits peer RPC traffic from mesh addresses to this Broker only.
func (t *TS) filter() *filter.Filter {
	self := netip.PrefixFrom(t.selfIP, 128)
	ms := []filter.Match{{
		IPProto: views.SliceOf([]ipproto.Proto{ipproto.TCP}),
		Srcs:    []netip.Prefix{ULAPrefix},
		Dsts:    []filter.NetPortRange{{Net: self, Ports: filter.PortRange{First: PeerPort, Last: PeerPort}}},
	}, {
		IPProto: views.SliceOf([]ipproto.Proto{ipproto.ICMPv6}),
		Srcs:    []netip.Prefix{ULAPrefix},
		Dsts:    []filter.NetPortRange{{Net: self, Ports: filter.PortRange{First: 0, Last: 65535}}},
	}}
	var b netipx.IPSetBuilder
	b.AddPrefix(self)
	local, _ := b.IPSet()
	var lb netipx.IPSetBuilder
	logIPs, _ := lb.IPSet()
	return filter.New(ms, nil, local, logIPs, nil, t.cfg.Logf)
}

func (t *TS) peerConfig(k key.NodePublic) (wgcfg.PeerConfig, bool) {
	t.mu.Lock()
	id, ok := t.byKey[k]
	t.mu.Unlock()
	if !ok {
		if t.cfg.OnUnknownPeer != nil {
			raw := k.Raw32()
			go t.cfg.OnUnknownPeer(ids.BrokerIDFromPub(raw[:]))
		}
		return wgcfg.PeerConfig{}, false
	}
	pc := wgcfg.PeerConfig{AllowedIPs: []netip.Prefix{netip.PrefixFrom(ULA(id), 128)}}
	copy(pc.PresharedKey[:], t.cfg.PSK)
	return pc, true
}

func (t *TS) peerByIP(ip netip.Addr) (key.NodePublic, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	k, ok := t.byIP[ip]
	return k, ok
}

func (t *TS) tcpHandler(src, dst netip.AddrPort) (func(net.Conn), bool) {
	if dst.Addr() != t.selfIP || dst.Port() != PeerPort {
		return nil, false
	}
	t.mu.Lock()
	k, ok := t.byIP[src.Addr()]
	id := t.byKey[k]
	t.mu.Unlock()
	if !ok {
		return nil, false
	}
	return func(c net.Conn) {
		select {
		case t.accepts <- accepted{c, id}:
		case <-t.closed:
			c.Close()
		}
	}, true
}

// Accept implements host.Mesh.
func (t *TS) Accept() (net.Conn, ids.BrokerID, error) {
	select {
	case a := <-t.accepts:
		return a.c, a.from, nil
	case <-t.closed:
		return nil, ids.BrokerID{}, net.ErrClosed
	}
}

// Dial implements host.Mesh.
func (t *TS) Dial(ctx context.Context, to ids.BrokerID) (net.Conn, error) {
	c, err := t.ns.DialContextTCP(ctx, netip.AddrPortFrom(ULA(to), PeerPort))
	if err != nil {
		return nil, err
	}
	return c, nil
}

// SetPeers implements host.Mesh: it rebuilds the WireGuard peer set and the
// magicsock peer list from membership.
func (t *TS) SetPeers(members []node.Member) {
	published := make([]json.RawMessage, len(members))
	for i, m := range members {
		if m.ID != t.self {
			published[i] = m.Info.DERP
		}
	}
	t.mu.Lock()
	t.members = members
	dm, regions := t.derp.build(published)
	t.mu.Unlock()
	if dm != nil {
		// Before the peers: their home regions must be in the map.
		t.ms.SetDERPMap(dm)
	}
	var peers []tailcfg.NodeView
	byKey := map[key.NodePublic]ids.BrokerID{}
	byIP := map[netip.Addr]key.NodePublic{}
	disco := map[key.NodePublic]key.DiscoPublic{}
	for i, m := range members {
		if m.ID == t.self {
			continue
		}
		var pubRaw []byte
		if b, err := hexDecode(m.Info.NodePub); err == nil && len(b) == 32 {
			pubRaw = b
		} else {
			continue
		}
		k := key.NodePublicFromRaw32(mem.B(pubRaw))
		var dk key.DiscoPublic
		if err := dk.UnmarshalText([]byte(m.Info.DiscoPub)); err != nil {
			continue
		}
		ip := ULA(m.ID)
		pfx := netip.PrefixFrom(ip, 128)
		var eps []netip.AddrPort
		for _, e := range m.Info.Endpoints {
			if ap, err := netip.ParseAddrPort(e); err == nil {
				eps = append(eps, ap)
			}
		}
		n := &tailcfg.Node{
			ID:                nodeID(m.ID),
			StableID:          tailcfg.StableNodeID(m.ID.String()),
			Name:              m.ID.Abbrev() + ".nimbo.",
			Key:               k,
			DiscoKey:          dk,
			Addresses:         []netip.Prefix{pfx},
			AllowedIPs:        []netip.Prefix{pfx},
			Endpoints:         eps,
			HomeDERP:          regions[i],
			MachineAuthorized: true,
		}
		peers = append(peers, n.View())
		byKey[k] = m.ID
		byIP[ip] = k
		disco[k] = dk
	}
	t.mu.Lock()
	old, oldDisco := t.byKey, t.disco
	t.byKey, t.byIP, t.disco = byKey, byIP, disco
	t.mu.Unlock()
	t.ms.SetNetworkMap(t.selfNode().View(), peers)
	for k := range old {
		if _, ok := byKey[k]; !ok {
			t.eng.SyncDevicePeer(k)
		}
	}
	for k := range byKey {
		t.eng.SyncDevicePeer(k)
		if od, ok := oldDisco[k]; ok && od != disco[k] {
			t.eng.MarkDevicePeerForHandshake(k)
		}
	}
}

// SetDERPMap replaces this Broker's own DERP relays, e.g. after a periodic
// refresh.
func (t *TS) SetDERPMap(dm *tailcfg.DERPMap) error {
	if err := checkOwnMap(dm); err != nil {
		return err
	}
	t.mu.Lock()
	t.derp.setOwn(dm)
	members := t.members
	t.mu.Unlock()
	t.SetPeers(members)
	t.reportLocal()
	return nil
}

// LocalPort is magicsock's UDP port.
func (t *TS) LocalPort() uint16 { return t.ms.LocalPort() }

// Close implements host.Mesh.
func (t *TS) Close() error {
	t.once.Do(func() {
		close(t.closed)
		t.eng.Close()
		t.sys.Bus.Get().Close()
	})
	return nil
}

func hexDecode(s string) ([]byte, error) {
	if len(s)%2 != 0 {
		return nil, errors.New("odd length")
	}
	out := make([]byte, len(s)/2)
	for i := range out {
		v, err := strconv.ParseUint(s[2*i:2*i+2], 16, 8)
		if err != nil {
			return nil, err
		}
		out[i] = byte(v)
	}
	return out, nil
}
