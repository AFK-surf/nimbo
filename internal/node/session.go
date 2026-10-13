package node

import (
	"crypto/hmac"
	"encoding/json"
	"net/netip"
	"slices"
	"sort"
	"time"

	"golang.org/x/crypto/curve25519"

	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/pkg/wire"
)

// Endpoint sessions: handshake, Action dispatch and limits (design §6).

type session struct {
	id          SessionID
	ip          netip.Addr
	established bool
	closed      bool
	opened      time.Time

	ephPriv [32]byte
	ephPub  [32]byte
	nonce   [32]byte

	key    ids.Key
	wireID string
	maxMsg int

	actions *bucket
	mints   *bucket

	window      time.Time // start of the current 10s quota window
	usedCaps    map[ids.CapID]bool
	unknown     int
	blobBytes   int
	created     map[ids.CapID]bool
	pendingSend int
	// holder halves this session opened that have not paired or failed
	pendingStreams int
}

func (n *Node) allowlisted(ip netip.Addr) bool {
	for _, p := range n.lim.IPAllowlist {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

// OnSessionOpen starts the handshake on a new endpoint connection. ip is the
// client address (from PROXY protocol behind a load balancer).
func (n *Node) OnSessionOpen(id SessionID, ip netip.Addr) {
	if n.stopped {
		return
	}
	now := n.now()
	if !n.Ready() {
		reason := "unavailable"
		if n.fenced {
			reason = "fenced"
		} else if n.draining {
			reason = "draining"
		}
		n.sendFrame(id, wire.Closing{Op: "closing", Reason: reason, RetryAfterMs: n.retryAfter()})
		n.env.CloseSession(id)
		return
	}
	if !n.allowlisted(ip) {
		b := n.ipBuckets[ip]
		if b == nil {
			b = newBucket(n.lim.HandshakesPerSec, n.lim.HandshakeBurst, now)
			n.ipBuckets[ip] = b
		}
		if n.ipSessions[ip] >= n.lim.SessionsPerIP || !b.take(now, 1) {
			n.sendFrame(id, wire.Closing{Op: "closing", Reason: wire.ErrRateLimited, RetryAfterMs: n.retryAfter()})
			n.env.CloseSession(id)
			return
		}
	}
	s := &session{id: id, ip: ip, opened: now}
	ids.RandBytes(n.env.Rand(), s.ephPriv[:])
	pub, err := curve25519.X25519(s.ephPriv[:], curve25519.Basepoint)
	if err != nil {
		panic(err)
	}
	copy(s.ephPub[:], pub)
	ids.RandBytes(n.env.Rand(), s.nonce[:])
	n.sessions[id] = s
	n.ipSessions[ip]++
	n.sendFrame(id, wire.Challenge{Op: "challenge", V: 1, Broker: n.id.String(), Eph: ids.B32.EncodeToString(s.ephPub[:]), Nonce: ids.B32.EncodeToString(s.nonce[:])})
}

func (n *Node) retryAfter() int64 {
	return 500 + int64(ids.RandUint64(n.env.Rand())%10000)
}

func (n *Node) sendFrame(id SessionID, v any) bool {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return n.env.SendSession(id, b)
}

// OnSessionFrame handles one text frame from an endpoint.
func (n *Node) OnSessionFrame(id SessionID, data []byte) {
	s := n.sessions[id]
	if s == nil || s.closed || n.stopped {
		return
	}
	var in wire.In
	if err := json.Unmarshal(data, &in); err != nil {
		if !s.established {
			n.closeSession(s, wire.ErrBadRequest)
			return
		}
		n.fail(s, 0, wire.ErrBadRequest, "malformed frame")
		return
	}
	if !s.established {
		if in.Op != "hello" || !n.verifyHello(s, &in) {
			n.closeSession(s, "auth_failed")
			return
		}
		n.establish(s, &in)
		return
	}
	now := n.now()
	if !s.actions.take(now, 1) {
		n.fail(s, in.Rid, wire.ErrRateLimited, "actions")
		return
	}
	n.rollWindow(s)
	switch in.Op {
	case "send", "stream":
		n.actSend(s, &in)
	case "mint":
		n.actMint(s, &in)
	case "transfer":
		n.actTransfer(s, &in)
	case "renew":
		n.actRenew(s, &in)
	case "inspect":
		n.actInspect(s, &in)
	default:
		n.fail(s, in.Rid, wire.ErrBadRequest, "unknown op")
	}
}

func (n *Node) verifyHello(s *session, in *wire.In) bool {
	key, err := ids.ParseKey(in.Key)
	if err != nil {
		return false
	}
	if !slices.Contains(n.cfg.PublicNames, in.Authority) {
		return false
	}
	proof, err := ids.B32.DecodeString(in.Proof)
	if err != nil || len(proof) != 32 {
		return false
	}
	ss, err := curve25519.X25519(s.ephPriv[:], key[:])
	if err != nil { // all-zero shared secret (low-order point)
		return false
	}
	want := wire.Proof(ss, s.nonce[:], in.Authority, key[:], s.ephPub[:])
	if !hmac.Equal(proof, want) {
		return false
	}
	s.key = key
	return true
}

func (n *Node) establish(s *session, in *wire.In) {
	now := n.now()
	s.ephPriv = [32]byte{}
	s.established = true
	var sid [16]byte
	ids.RandBytes(n.env.Rand(), sid[:])
	s.wireID = ids.B32.EncodeToString(sid[:])
	s.maxMsg = n.lim.MaxPayloadBytes
	if in.MaxMessageBytes > 0 && in.MaxMessageBytes < s.maxMsg {
		s.maxMsg = in.MaxMessageBytes
	}
	s.actions = newBucket(n.lim.ActionsPerSec, n.lim.ActionsBurst, now)
	s.mints = newBucket(n.lim.MintsPerSec, n.lim.MintsBurst, now)
	s.window = now
	s.usedCaps = map[ids.CapID]bool{}
	s.created = map[ids.CapID]bool{}
	n.byKey[s.key] = append(n.byKey[s.key], s)
	n.sessionsChanged(s.key, +1)
	n.sendFrame(s.id, wire.Welcome{Op: "welcome", Session: s.wireID, Limits: n.wireLimits(), Brokers: n.suggestBrokers(s.key)})
	n.flushHeldLocal(s.key)
}

func (n *Node) wireLimits() wire.Limits {
	return wire.Limits{
		MaxPayloadBytes: n.lim.MaxPayloadBytes,
		MaxCarriedCaps:  n.lim.MaxCarriedCaps,
		MaxSchemaBytes:  n.lim.MaxSchemaBytes,
		MaxBaseBytes:    n.lim.MaxBaseBytes,
		ActionsPerSec:   int(n.lim.ActionsPerSec),
		MintsPerSec:     int(n.lim.MintsPerSec),
		MaxLiveCaps:     n.lim.MaxLiveCaps,
		DefaultTTLms:    ms(n.tun.DefaultMsgTTL),
		MaxTTLms:        ms(n.tun.MaxMsgTTL),

		MaxPendingStreams:  n.lim.MaxPendingStreams,
		StreamAttachTTLms:  ms(n.tun.StreamAttachTTL),
		StreamSocketsPerIP: n.lim.StreamSocketsPerIP,
	}
}

// suggestBrokers picks 2 other non-draining Brokers: same region first,
// then XOR-nearest to the endpoint key (design §6.2).
func (n *Node) suggestBrokers(k ids.Key) []string {
	near := n.view.idx.Nearest(k.PlacementKey(), n.view.idx.Len(), func(b ids.BrokerID) bool {
		m := n.view.members[b]
		return b != n.id && !m.draining && len(m.info.WSURLs) > 0
	})
	region := n.cfg.Member.Region
	sort.SliceStable(near, func(i, j int) bool {
		ri := n.view.members[near[i]].info.Region == region
		rj := n.view.members[near[j]].info.Region == region
		return ri && !rj
	})
	var out []string
	for _, b := range near {
		if len(out) == 2 {
			break
		}
		out = append(out, n.view.members[b].info.WSURLs[0])
	}
	return out
}

func (n *Node) rollWindow(s *session) {
	now := n.now()
	if now.Sub(s.window) >= 10*time.Second {
		s.window = now
		s.usedCaps = map[ids.CapID]bool{}
		s.unknown = 0
		s.blobBytes = 0
	}
}

// noteUse enforces the distinct-caps-used quota.
func (n *Node) noteUse(s *session, id ids.CapID) bool {
	if s.usedCaps[id] {
		return true
	}
	if len(s.usedCaps) >= n.lim.CapsUsedPer10s {
		return false
	}
	s.usedCaps[id] = true
	return true
}

// OnSessionClosed handles a closed endpoint connection.
func (n *Node) OnSessionClosed(id SessionID) {
	if s := n.sessions[id]; s != nil {
		n.dropSession(s)
	}
}

func (n *Node) dropSession(s *session) {
	if s.closed {
		return
	}
	s.closed = true
	delete(n.sessions, s.id)
	if c := n.ipSessions[s.ip] - 1; c > 0 {
		n.ipSessions[s.ip] = c
	} else {
		delete(n.ipSessions, s.ip)
	}
	if s.established {
		list := n.byKey[s.key]
		for i, x := range list {
			if x == s {
				list = append(list[:i:i], list[i+1:]...)
				break
			}
		}
		if len(list) == 0 {
			delete(n.byKey, s.key)
		} else {
			n.byKey[s.key] = list
		}
		n.sessionsChanged(s.key, -1)
	}
}

func (n *Node) closeSession(s *session, reason string) {
	if s.closed {
		return
	}
	n.sendFrame(s.id, wire.Closing{Op: "closing", Reason: reason, RetryAfterMs: n.retryAfter()})
	n.env.CloseSession(s.id)
	n.dropSession(s)
}

func (n *Node) closeAllSessions(reason string) {
	ids := make([]SessionID, 0, len(n.sessions))
	for id := range n.sessions {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		if s := n.sessions[id]; s != nil {
			n.closeSession(s, reason)
		}
	}
}

func (n *Node) sweepSessions() {
	now := n.now()
	var stale []*session
	for _, s := range n.sessions {
		if !s.established && now.Sub(s.opened) > n.tun.HandshakeTTL {
			stale = append(stale, s)
		}
	}
	slices.SortFunc(stale, func(a, b *session) int { return int(a.id) - int(b.id) })
	for _, s := range stale {
		n.closeSession(s, "handshake_timeout")
	}
	for ip, b := range n.ipBuckets {
		if n.ipSessions[ip] == 0 && now.Sub(b.at) > time.Minute {
			delete(n.ipBuckets, ip)
		}
	}
	for ip, b := range n.attachFails {
		if now.Sub(b.at) > time.Minute {
			delete(n.attachFails, ip)
		}
	}
}

func (n *Node) fail(s *session, rid uint64, code, detail string) {
	if s.closed {
		return
	}
	n.sendFrame(s.id, wire.Result{Op: "result", Rid: rid, Error: &wire.Error{Code: code, Detail: detail}})
}

func (n *Node) ok(s *session, r wire.Result) {
	if s.closed {
		return
	}
	r.Op, r.OK = "result", true
	n.sendFrame(s.id, r)
}

// SessionCount returns the number of established sessions.
func (n *Node) SessionCount() int {
	c := 0
	for _, s := range n.sessions {
		if s.established {
			c++
		}
	}
	return c
}
