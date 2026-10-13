package node

import (
	"crypto/sha256"
	"net/netip"
	"time"

	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/pkg/wire"
)

// Streams (design §9.7). A stream is two half-streams with the same ID: the
// holder half on the holder's Broker H, created by the stream Action, and the
// target half on the target's Broker T, created when T delivers the offer.
// Each half issues and checks only its own endpoint's token. The target's
// Broker dials the holder's Broker with a data connection that pairs the
// halves; the two Brokers exchange nothing else for a stream beyond the
// offer's Forward and its Bounce.
//
// The node decides; the runtime moves the bytes. A leg is one data-plane
// connection: an endpoint's data socket or a mesh data connection.

// StreamLeg identifies one data-plane connection, assigned by the runtime.
type StreamLeg uint64

// AttachCode answers a data-socket attach.
type AttachCode int

const (
	AttachOK           AttachCode = iota // 101: upgrade the socket
	AttachUnauthorized                   // 401: unknown, used or expired token
	AttachForbidden                      // 403: the holder's Broker has no live, unpaired half
	AttachUnavailable                    // 503: unreachable peer, draining, overload
	AttachRateLimited                    // 429: too many sockets or failed attaches
)

// PeerStatus is the answer to a data connection (the status byte), or the
// outcome of dialing one.
type PeerStatus uint8

const (
	PeerUnreachable PeerStatus = 0 // dial or status read failed
	PeerPaired      PeerStatus = 1
	PeerNoHalf      PeerStatus = 2
	PeerBusy        PeerStatus = 3
)

type halfSide uint8

const (
	sideHolder halfSide = iota + 1
	sideTarget
)

type halfStream struct {
	id       ids.MsgID
	side     halfSide
	token    [32]byte // SHA-256 of the token
	used     bool     // the token attached
	sess     SessionID
	key      ids.Key      // the endpoint this half answers for
	peer     ids.BrokerID // target half: the holder's Broker
	local    StreamLeg    // the endpoint's data socket
	remote   StreamLeg    // the other half's leg
	paired   bool
	dialing  bool
	spliced  bool
	reported bool // holder: stream_status sent (or due no more)
	counted  bool // target: counts against UnattachedPerTarget
	deadline time.Time
	timer    Timer
}

// newStreamToken draws a token and returns it with its hash.
func (n *Node) newStreamToken() (string, [32]byte) {
	var tok [32]byte
	ids.RandBytes(n.env.Rand(), tok[:])
	return ids.B32.EncodeToString(tok[:]), sha256.Sum256(tok[:])
}

// StreamTokenHash is how the runtime turns a token from a URL into the key
// a Broker stores. Malformed tokens hash to a value no half has.
func StreamTokenHash(token string) [32]byte {
	b, err := ids.B32.DecodeString(token)
	if err != nil || len(b) != 32 {
		return [32]byte{}
	}
	return sha256.Sum256(b)
}

// openHolderHalf creates the holder half of a stream the session s opens.
func (n *Node) openHolderHalf(s *session, id ids.MsgID, ttl time.Duration) string {
	tok, h := n.newStreamToken()
	hs := &halfStream{id: id, side: sideHolder, token: h, sess: s.id, key: s.key, deadline: n.now().Add(ttl + n.tun.StreamAttachTTL)}
	n.holders[id] = hs
	n.tokens[h] = hs
	s.pendingStreams++
	hs.timer = n.after(hs.deadline.Sub(n.now()), func() { n.halfExpired(hs) })
	return tok
}

// openTargetHalf creates the target half when an offer is delivered to the
// target's session. from is the holder's Broker (the offer's ingress).
func (n *Node) openTargetHalf(id ids.MsgID, key ids.Key, from ids.BrokerID) (*halfStream, string) {
	tok, h := n.newStreamToken()
	hs := &halfStream{id: id, side: sideTarget, token: h, key: key, peer: from, counted: true, deadline: n.now().Add(n.tun.StreamAttachTTL)}
	n.targets[id] = hs
	n.tokens[h] = hs
	n.unattached[key]++
	hs.timer = n.after(n.tun.StreamAttachTTL, func() { n.halfExpired(hs) })
	return hs, tok
}

func (n *Node) uncount(hs *halfStream) {
	if !hs.counted {
		return
	}
	hs.counted = false
	if c := n.unattached[hs.key] - 1; c > 0 {
		n.unattached[hs.key] = c
	} else {
		delete(n.unattached, hs.key)
	}
}

// dropHalf forgets a half. Its legs stay known until the runtime reports
// them closed.
func (n *Node) dropHalf(hs *halfStream) {
	if hs.timer != nil {
		hs.timer.Stop()
	}
	if n.tokens[hs.token] == hs {
		delete(n.tokens, hs.token)
	}
	switch hs.side {
	case sideHolder:
		if n.holders[hs.id] == hs {
			delete(n.holders, hs.id)
		}
		n.settleHolder(hs)
	case sideTarget:
		if n.targets[hs.id] == hs {
			delete(n.targets, hs.id)
		}
		n.uncount(hs)
	}
	for _, l := range []StreamLeg{hs.local, hs.remote} {
		if l != 0 && n.legs[l] == hs && !hs.spliced {
			delete(n.legs, l)
		}
	}
}

// settleHolder ends the holder half's claim on its session's pending quota.
func (n *Node) settleHolder(hs *halfStream) {
	if hs.reported {
		return
	}
	hs.reported = true
	if s := n.sessions[hs.sess]; s != nil {
		s.pendingStreams--
	}
}

// reportHolder sends the holder's one stream_status, if its session is still
// connected.
func (n *Node) reportHolder(hs *halfStream, st wire.StreamStatus) {
	if hs.reported {
		return
	}
	n.settleHolder(hs)
	if s := n.sessions[hs.sess]; s != nil && s.established && !s.closed {
		st.Op, st.Stream = "stream_status", hs.id.String()
		n.sendFrame(s.id, st)
	}
}

// failHolder ends a holder half that has not paired: the holder learns the
// reason, and its data socket, if attached, closes with the matching code.
func (n *Node) failHolder(hs *halfStream, reason, detail string) {
	n.reportHolder(hs, wire.StreamStatus{State: wire.StreamFailed, Reason: reason, Detail: detail})
	if hs.local != 0 {
		n.env.CloseStreamLeg(hs.local, wire.CloseCodeFor(reason), reason)
	}
	n.dropHalf(hs)
}

func (n *Node) halfExpired(hs *halfStream) {
	if hs.spliced {
		return
	}
	switch hs.side {
	case sideHolder:
		if n.holders[hs.id] != hs {
			return
		}
		if !hs.paired {
			n.failHolder(hs, wire.ReasonPeerAttachTimeout, "")
			return
		}
		// Paired, but the holder never attached: the target's leg waits.
		if hs.remote != 0 {
			n.env.CloseStreamLeg(hs.remote, wire.ClosePeerAttachTimeout, wire.ReasonPeerAttachTimeout)
		}
		n.dropHalf(hs)
	case sideTarget:
		if n.targets[hs.id] != hs || hs.used {
			return // attached: the attach resolves it
		}
		n.dropHalf(hs)
	}
}

// onStreamBounce handles a bounce of a stream offer at the holder's Broker.
// It reports whether msg was a stream this Broker knows.
func (n *Node) onStreamBounce(msg ids.MsgID, reason string) bool {
	hs := n.holders[msg]
	if hs == nil {
		return false
	}
	if !hs.paired {
		n.failHolder(hs, reason, "")
	}
	return true
}

// failPendingStreams ends every half that is not established (fencing,
// draining). Established streams are kept.
func (n *Node) failPendingStreams() {
	var hs []*halfStream
	for _, h := range n.holders {
		if !h.spliced {
			hs = append(hs, h)
		}
	}
	for _, h := range n.targets {
		if !h.spliced {
			hs = append(hs, h)
		}
	}
	sortHalves(hs)
	for _, h := range hs {
		switch {
		case h.side == sideHolder && !h.paired:
			n.failHolder(h, wire.BounceUnavailable, "")
		case h.side == sideHolder:
			if h.remote != 0 {
				n.env.CloseStreamLeg(h.remote, wire.CloseUnavailable, wire.BounceUnavailable)
			}
			n.dropHalf(h)
		case h.dialing:
			// The dial resolves it.
		default:
			n.dropHalf(h)
		}
	}
}

// closeEstablishedStreams closes every spliced stream (end of a drain).
func (n *Node) closeEstablishedStreams(code int, reason string) {
	var hs []*halfStream
	seen := map[*halfStream]bool{}
	for _, h := range n.legs {
		if h.spliced && !seen[h] {
			seen[h] = true
			hs = append(hs, h)
		}
	}
	sortHalves(hs)
	for _, h := range hs {
		n.env.CloseStreamLeg(h.local, code, reason)
	}
}

func sortHalves(hs []*halfStream) {
	for i := 1; i < len(hs); i++ {
		for j := i; j > 0 && halfLess(hs[j], hs[j-1]); j-- {
			hs[j], hs[j-1] = hs[j-1], hs[j]
		}
	}
}

func halfLess(a, b *halfStream) bool {
	if a.id != b.id {
		return string(a.id[:]) < string(b.id[:])
	}
	return a.side < b.side
}

// splice joins a half's two legs once both exist and the halves are paired.
func (n *Node) splice(hs *halfStream) {
	if hs.spliced || !hs.paired || hs.local == 0 || hs.remote == 0 {
		return
	}
	hs.spliced = true
	if hs.timer != nil {
		hs.timer.Stop()
	}
	switch hs.side {
	case sideHolder:
		if n.holders[hs.id] == hs {
			delete(n.holders, hs.id)
		}
	case sideTarget:
		if n.targets[hs.id] == hs {
			delete(n.targets, hs.id)
		}
	}
	n.stats.StreamsSpliced++
	n.env.SpliceStream(hs.local, hs.remote)
}

// pairHolder pairs the holder half with a target half whose leg is l.
func (n *Node) pairHolder(hs *halfStream, l StreamLeg) {
	hs.paired = true
	hs.remote = l
	n.legs[l] = hs
	n.stats.StreamsPaired++
	n.reportHolder(hs, wire.StreamStatus{State: wire.StreamAccepted})
	n.splice(hs)
}

// --- entry points from the runtime ---

func (n *Node) attachFailed(ip netip.Addr) AttachCode {
	if n.allowlisted(ip) {
		return AttachUnauthorized
	}
	now := n.now()
	b := n.attachFails[ip]
	if b == nil {
		b = newBucket(n.lim.StreamAttachFailPerSec, n.lim.StreamAttachFailPerSec, now)
		n.attachFails[ip] = b
	}
	if !b.take(now, 1) {
		return AttachRateLimited
	}
	return AttachUnauthorized
}

// OnStreamAttach handles an endpoint's data-socket request (GET /v1/stream)
// before the upgrade. The node answers with Env.AnswerAttach. The runtime
// reports the leg closed (OnStreamLegClosed) whatever the answer.
func (n *Node) OnStreamAttach(l StreamLeg, tokenHash [32]byte, ip netip.Addr) {
	if n.stopped {
		return
	}
	n.legIP[l] = ip
	n.ipLegs[ip]++
	if !n.Ready() || len(n.legs) >= n.lim.MaxStreamLegs {
		n.env.AnswerAttach(l, AttachUnavailable)
		return
	}
	if !n.allowlisted(ip) && n.ipLegs[ip] > n.lim.StreamSocketsPerIP {
		n.env.AnswerAttach(l, AttachRateLimited)
		return
	}
	hs := n.tokens[tokenHash]
	if hs == nil || hs.used || n.now().After(hs.deadline) {
		n.env.AnswerAttach(l, n.attachFailed(ip))
		return
	}
	delete(n.tokens, tokenHash)
	hs.used = true
	hs.local = l
	n.legs[l] = hs
	n.stats.StreamAttaches++
	switch hs.side {
	case sideHolder:
		n.env.AnswerAttach(l, AttachOK)
		n.splice(hs)
	case sideTarget:
		n.uncount(hs)
		if hs.peer == n.id {
			n.attachInProcess(hs)
			return
		}
		hs.dialing = true
		hs.remote = n.env.DialStreamPeer(hs.peer, hs.id)
		n.legs[hs.remote] = hs
	}
}

// attachInProcess pairs a target half with the holder half on this Broker.
// The target's socket becomes the holder half's remote leg.
func (n *Node) attachInProcess(ths *halfStream) {
	l := ths.local
	n.dropHalf(ths)
	hh := n.holders[ths.id]
	if hh == nil || hh.paired || n.now().After(hh.deadline) {
		delete(n.legs, l)
		n.env.AnswerAttach(l, AttachForbidden)
		return
	}
	n.env.AnswerAttach(l, AttachOK)
	n.pairHolder(hh, l)
}

// OnStreamDialed reports the outcome of Env.DialStreamPeer.
func (n *Node) OnStreamDialed(l StreamLeg, st PeerStatus) {
	if n.stopped {
		return
	}
	hs := n.legs[l]
	if hs == nil || hs.side != sideTarget || hs.remote != l || !hs.dialing {
		n.env.CloseStreamLeg(l, wire.ClosePeerLost, wire.ReasonPeerLost)
		return
	}
	hs.dialing = false
	if hs.local == 0 {
		// The endpoint's request gave up meanwhile.
		n.env.CloseStreamLeg(l, wire.ClosePeerLost, wire.ReasonPeerLost)
		n.dropHalf(hs)
		return
	}
	switch st {
	case PeerPaired:
		hs.paired = true
		n.env.AnswerAttach(hs.local, AttachOK)
		n.splice(hs)
		return
	case PeerNoHalf:
		n.env.AnswerAttach(hs.local, AttachForbidden)
	default:
		n.env.AnswerAttach(hs.local, AttachUnavailable)
	}
	n.dropHalf(hs)
}

// OnStreamPeer handles an inbound data connection for stream id from Broker
// from. The node answers with Env.AnswerStreamPeer.
func (n *Node) OnStreamPeer(l StreamLeg, from ids.BrokerID, id ids.MsgID) {
	if n.stopped {
		return
	}
	if !n.Ready() || len(n.legs) >= n.lim.MaxStreamLegs {
		n.env.AnswerStreamPeer(l, PeerBusy)
		return
	}
	hs := n.holders[id]
	if hs == nil || hs.paired || n.now().After(hs.deadline) {
		n.env.AnswerStreamPeer(l, PeerNoHalf)
		return
	}
	n.env.AnswerStreamPeer(l, PeerPaired)
	n.pairHolder(hs, l)
}

// OnStreamLegClosed reports that a leg is gone: an endpoint or peer closed
// it, it failed, or the node closed it. The runtime reports each leg once.
func (n *Node) OnStreamLegClosed(l StreamLeg) {
	if ip, ok := n.legIP[l]; ok {
		delete(n.legIP, l)
		if c := n.ipLegs[ip] - 1; c > 0 {
			n.ipLegs[ip] = c
		} else {
			delete(n.ipLegs, ip)
		}
	}
	hs := n.legs[l]
	if hs == nil {
		return
	}
	delete(n.legs, l)
	if n.stopped {
		return
	}
	if hs.spliced {
		// The runtime closes the other leg of the splice too.
		return
	}
	switch {
	case hs.side == sideHolder && l == hs.local:
		// The holder gave up before pairing completed.
		hs.local = 0
		if !hs.paired {
			n.reportHolder(hs, wire.StreamStatus{State: wire.StreamFailed, Reason: wire.ReasonCanceled})
		} else if hs.remote != 0 {
			n.env.CloseStreamLeg(hs.remote, wire.ClosePeerLost, wire.ReasonPeerLost)
		}
		n.dropHalf(hs)
	case hs.side == sideHolder && l == hs.remote:
		// The target's leg went away before the holder attached.
		hs.remote = 0
		n.dropHalf(hs)
	case hs.side == sideTarget && l == hs.local:
		hs.local = 0
		if hs.remote != 0 && !hs.dialing {
			n.env.CloseStreamLeg(hs.remote, wire.ClosePeerLost, wire.ReasonPeerLost)
		}
		if !hs.dialing {
			n.dropHalf(hs)
		}
	}
}

// StreamInfo describes one half-stream, for tests and the simulator.
type StreamInfo struct {
	ID       ids.MsgID
	Holder   bool
	Paired   bool
	Spliced  bool
	Deadline time.Time
	// Late is how far past its deadline the half is (negative: before).
	Late time.Duration
}

// PendingStreams lists the half-streams that are not yet spliced.
func (n *Node) PendingStreams() []StreamInfo {
	var out []StreamInfo
	for _, m := range []map[ids.MsgID]*halfStream{n.holders, n.targets} {
		for _, h := range m {
			out = append(out, StreamInfo{ID: h.id, Holder: h.side == sideHolder, Paired: h.paired, Spliced: h.spliced, Deadline: h.deadline, Late: n.now().Sub(h.deadline)})
		}
	}
	return out
}

// StreamLegs returns the number of data-plane legs this Broker tracks.
func (n *Node) StreamLegs() int { return len(n.legs) }
