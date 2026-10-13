package sim

import (
	"fmt"
	"time"

	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/internal/node"
	"github.com/afk-surf/nimbo/pkg/wire"
)

// Streams (design §9.7). Data sockets and mesh data connections are
// simulated pipes of items (messages and closes) with per-hop latency and
// order. A Broker crash kills its legs, and partitions and link resets break
// the mesh data connections across them; the Brokers on the other side see
// the failure and close their legs as peer_lost.

type sitem struct {
	msg    *smsg
	close  bool
	code   int
	reason string
}

// smsg is one endpoint message on a stream.
type smsg struct {
	stream     string
	from       *Endpoint
	fromHolder bool
	seq        int
}

type legKind int

const (
	legSocket legKind = iota
	legMesh
)

// simLeg is one data-plane connection as seen by one Broker.
type simLeg struct {
	w        *World
	id       node.StreamLeg
	b        *Broker
	gen      int
	kind     legKind
	att      *attachReq // socket legs
	remote   *simLeg    // mesh legs: the other Broker's end
	partner  *simLeg    // splice partner
	buf      []sitem    // items that arrived before the splice
	closed   bool
	reported bool
	dialing  bool
	lastOut  time.Duration
}

type attachReq struct {
	es       *epStream
	token    string
	sentAt   time.Duration
	onIssuer bool
	probe    bool // must be refused
	late     bool // sent after the token's half expired
}

func (b *Broker) newLeg(kind legKind) *simLeg {
	b.w.nextLeg++
	l := &simLeg{w: b.w, id: b.w.nextLeg, b: b, gen: b.gen, kind: kind}
	b.legs[l.id] = l
	return l
}

func (l *simLeg) live() bool { return !l.closed && l.b.alive && l.b.gen == l.gen }

// shut closes the leg and reports it to its node, once.
func (l *simLeg) shut() {
	l.closed = true
	if l.reported {
		return
	}
	l.reported = true
	b, gen, id := l.b, l.gen, l.id
	if b.legs[id] == l {
		delete(b.legs, id)
	}
	l.w.After(0, func() {
		if b.alive && b.gen == gen {
			b.node.OnStreamLegClosed(id)
		}
	})
}

func (w *World) hop() time.Duration { return w.dur(500*time.Microsecond, 10*time.Millisecond) }

func (l *simLeg) departure() time.Duration {
	at := l.w.now + l.w.hop()
	if at < l.lastOut {
		at = l.lastOut
	}
	l.lastOut = at
	return at
}

// emit sends an item out of the Broker through l.
func (l *simLeg) emit(it sitem) {
	switch l.kind {
	case legSocket:
		att := l.att
		l.w.At(l.departure(), func() { att.es.onItem(l, it) })
	case legMesh:
		r := l.remote
		if r == nil {
			return
		}
		l.w.At(l.departure(), func() {
			if r.live() {
				r.arrive(it)
			}
		})
	}
}

// arrive handles an item coming into the Broker on l. Until the splice, the
// runtime does not read the connection: items wait.
func (l *simLeg) arrive(it sitem) {
	if !l.live() {
		return
	}
	if l.partner == nil {
		l.buf = append(l.buf, it)
		return
	}
	l.forward(it)
}

func (l *simLeg) forward(it sitem) {
	p := l.partner
	if it.close && l.kind == legSocket {
		it.code = wire.EndpointCloseCode(it.code)
	}
	p.emit(it)
	if it.close {
		l.shut()
		p.shut()
	}
}

// fail: the connection under l broke.
func (l *simLeg) fail() {
	if !l.live() {
		return
	}
	if l.dialing {
		l.dialing = false
		b, gen, id := l.b, l.gen, l.id
		l.closed = true
		l.w.After(0, func() {
			if b.alive && b.gen == gen {
				b.node.OnStreamDialed(id, node.PeerUnreachable)
			}
		})
		l.shut()
		return
	}
	if p := l.partner; p != nil && p.live() {
		p.emit(sitem{close: true, code: wire.ClosePeerLost, reason: wire.ReasonPeerLost})
		p.shut()
	}
	l.shut()
}

// killLegs: the Broker process died with its legs. Endpoints notice their
// sockets drop; the other Brokers notice their data connections drop.
func (b *Broker) killLegs() {
	w := b.w
	for _, id := range sortedLegIDs(b.legs) {
		l := b.legs[id]
		l.closed, l.reported = true, true
		switch l.kind {
		case legSocket:
			att := l.att
			w.After(w.dur(100*time.Millisecond, 3*time.Second), func() { att.es.onLost(l) })
		case legMesh:
			if r := l.remote; r != nil {
				w.After(w.dur(time.Millisecond, 50*time.Millisecond), r.fail)
			}
		}
	}
	b.legs = map[node.StreamLeg]*simLeg{}
}

func sortedLegIDs(m map[node.StreamLeg]*simLeg) []node.StreamLeg {
	out := make([]node.StreamLeg, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// breakMesh breaks data connections: between a and b if given (a link
// reset), otherwise every one whose Brokers can no longer reach each other.
func (w *World) breakMesh(a, b *Broker) {
	keep := w.meshLegs[:0]
	var broken []*simLeg
	for _, l := range w.meshLegs {
		r := l.remote
		if !l.live() && (r == nil || !r.live()) {
			continue
		}
		keep = append(keep, l)
		if r == nil {
			continue
		}
		if a != nil && !((l.b == a && r.b == b) || (l.b == b && r.b == a)) {
			continue
		}
		if a == nil && w.connected(l.b, r.b) {
			continue
		}
		broken = append(broken, l)
	}
	w.meshLegs = keep
	for _, l := range broken {
		r := l.remote
		w.After(w.dur(time.Millisecond, 50*time.Millisecond), l.fail)
		w.After(w.dur(time.Millisecond, 50*time.Millisecond), r.fail)
	}
}

// --- node.Env ---

func (e *brokerEnv) AnswerAttach(id node.StreamLeg, code node.AttachCode) {
	if !e.live() {
		return
	}
	l := e.b.legs[id]
	if l == nil || l.closed || l.kind != legSocket {
		return
	}
	att := l.att
	e.b.w.At(l.departure(), func() { att.es.onAttach(l, code) })
	if code != node.AttachOK {
		l.shut()
	}
}

func (e *brokerEnv) DialStreamPeer(to ids.BrokerID, id ids.MsgID) node.StreamLeg {
	if !e.live() {
		return 0
	}
	b := e.b
	l := b.newLeg(legMesh)
	l.dialing = true
	b.w.After(b.w.hop(), func() { b.w.dialStream(l, to, id) })
	return l.id
}

func (w *World) dialStream(l *simLeg, to ids.BrokerID, id ids.MsgID) {
	if !l.live() {
		return
	}
	h := w.byID[to]
	if h == nil || !w.connected(l.b, h) {
		w.After(w.dur(5*time.Millisecond, 300*time.Millisecond), l.fail)
		return
	}
	m := h.newLeg(legMesh)
	l.remote, m.remote = m, l
	w.meshLegs = append(w.meshLegs, l)
	w.check.stats.StreamDials++
	h.node.OnStreamPeer(m.id, l.b.id, id)
}

func (e *brokerEnv) AnswerStreamPeer(id node.StreamLeg, st node.PeerStatus) {
	if !e.live() {
		return
	}
	m := e.b.legs[id]
	if m == nil || m.closed || m.remote == nil {
		return
	}
	l := m.remote
	if st != node.PeerPaired {
		m.shut()
	}
	e.b.w.At(m.departure(), func() {
		if !l.live() || !l.dialing {
			m.fail()
			return
		}
		l.dialing = false
		l.b.node.OnStreamDialed(l.id, st)
		if st != node.PeerPaired {
			l.shut()
		}
	})
}

func (e *brokerEnv) SpliceStream(a, c node.StreamLeg) {
	if !e.live() {
		return
	}
	la, lc := e.b.legs[a], e.b.legs[c]
	if la == nil || lc == nil || !la.live() || !lc.live() {
		for _, l := range []*simLeg{la, lc} {
			if l != nil && l.live() {
				l.emit(sitem{close: true, code: wire.ClosePeerLost, reason: wire.ReasonPeerLost})
				l.shut()
			}
		}
		return
	}
	la.partner, lc.partner = lc, la
	for _, x := range []*simLeg{la, lc} {
		buf := x.buf
		x.buf = nil
		for _, it := range buf {
			if !x.live() {
				break
			}
			x.forward(it)
		}
	}
}

func (e *brokerEnv) CloseStreamLeg(id node.StreamLeg, code int, reason string) {
	if !e.live() {
		return
	}
	l := e.b.legs[id]
	if l == nil || !l.live() {
		return
	}
	it := sitem{close: true, code: code, reason: reason}
	if p := l.partner; p != nil && p.live() {
		p.emit(it)
		p.shut()
	}
	l.emit(it)
	l.shut()
}

// --- endpoints ---

type streamPlan int

const (
	planNow     streamPlan = iota // attach (holder) or accept (target) at once
	planDelayed                   // holder: attach a few seconds late
	planNever                     // holder: never attach; target: ignore
	planRefuse                    // target: attach and close with 4901
	planLate                      // target: attach after the token expired
)

// epStream is one endpoint's side of a stream.
type epStream struct {
	sr      *streamRec
	holder  bool
	ep      *Endpoint
	plan    streamPlan
	leg     *simLeg
	want    int
	sent    int
	got     int
	gapLo   time.Duration
	gapHi   time.Duration
	early   bool // target: closes right after its last message
	closed  bool // this side sent a close or the socket ended
	ended   bool // a close or a failure arrived
	lastIn  time.Duration
	pending map[*simLeg]bool // attaches in flight
}

func (w *World) newEpStream(sr *streamRec, e *Endpoint, holder bool) *epStream {
	es := &epStream{sr: sr, holder: holder, ep: e, want: 1 + w.rng.IntN(6), gapLo: time.Millisecond, gapHi: 300 * time.Millisecond, pending: map[*simLeg]bool{}}
	if w.chance(0.1) {
		// A long-lived stream: it spans faults (crashes, fencing, drains).
		es.want, es.gapLo, es.gapHi = 3+w.rng.IntN(4), 5*time.Second, 15*time.Second
	}
	e.streams = append(e.streams, es)
	return es
}

func (es *epStream) peer() *Endpoint {
	if es.holder {
		return es.sr.target
	}
	return es.sr.holder
}

// attach requests a data socket on b.
func (w *World) attach(es *epStream, b *Broker, token string, onIssuer, probe, late bool) {
	att := &attachReq{es: es, token: token, sentAt: w.now, onIssuer: onIssuer, probe: probe, late: late}
	w.check.stats.StreamAttaches++
	if !b.alive {
		w.After(w.dur(10*time.Millisecond, 500*time.Millisecond), func() { es.attachDone(att, nil, node.AttachUnavailable) })
		return
	}
	l := b.newLeg(legSocket)
	l.att = att
	es.pending[l] = true
	gen := b.gen
	w.After(w.hop(), func() {
		if b.alive && b.gen == gen && !l.closed {
			b.node.OnStreamAttach(l.id, node.StreamTokenHash(token), es.ep.ip)
		}
	})
}

func (es *epStream) onAttach(l *simLeg, code node.AttachCode) {
	if !es.pending[l] {
		return
	}
	delete(es.pending, l)
	es.attachDone(l.att, l, code)
}

func (es *epStream) attachDone(att *attachReq, l *simLeg, code node.AttachCode) {
	w := es.ep.w
	w.check.streamAttached(es, att, l, code)
	if code != node.AttachOK || att.probe || es.leg != nil {
		if code == node.AttachOK && l != nil {
			es.sendOn(l, sitem{close: true, code: 1000})
		}
		return
	}
	es.leg = l
	if es.ep.retired {
		es.abort()
		return
	}
	if es.plan == planRefuse {
		es.close(wire.CloseStreamRejected, "no")
		return
	}
	es.next()
}

// next sends the next message after a gap, or closes when done.
func (es *epStream) next() {
	w := es.ep.w
	if es.closed {
		return
	}
	if es.sent >= es.want {
		es.maybeClose()
		return
	}
	w.After(w.dur(es.gapLo, es.gapHi), func() {
		if es.closed || es.ep.retired {
			return
		}
		es.sent++
		es.sendOn(es.leg, sitem{msg: &smsg{stream: es.sr.id, from: es.ep, fromHolder: es.holder, seq: es.sent}})
		es.next()
	})
}

// maybeClose: the holder closes once it sent everything and got all the
// target's messages; an early target closes after its last message.
func (es *epStream) maybeClose() {
	if es.closed || es.sent < es.want {
		return
	}
	w := es.ep.w
	switch {
	case !es.holder && es.early:
		es.close(3000+w.rng.IntN(1000), "early")
	case es.holder && es.sr.tes != nil && es.got >= es.sr.tes.want:
		es.close(3000+w.rng.IntN(1000), fmt.Sprintf("done-%d", es.sent))
	}
}

func (es *epStream) close(code int, reason string) {
	es.closed = true
	es.ep.w.check.streamCloseSent(es, code, reason)
	es.sendOn(es.leg, sitem{close: true, code: code, reason: reason})
}

func (es *epStream) sendOn(l *simLeg, it sitem) {
	w := es.ep.w
	at := w.now + w.hop()
	if at < es.lastIn {
		at = es.lastIn
	}
	es.lastIn = at
	w.At(at, func() {
		if l.live() {
			l.arrive(it)
		}
	})
}

func (es *epStream) onItem(l *simLeg, it sitem) {
	if l != es.leg || es.ended {
		return
	}
	c := es.ep.w.check
	if it.close {
		es.ended, es.closed = true, true
		c.streamCloseRecv(es, it.code, it.reason)
		return
	}
	c.streamData(es, it.msg)
	es.got++
	if es.holder {
		es.maybeClose()
	}
}

func (es *epStream) onLost(l *simLeg) {
	if es.pending[l] {
		delete(es.pending, l)
		es.attachDone(l.att, nil, node.AttachUnavailable)
		return
	}
	if l != es.leg || es.ended {
		return
	}
	es.ended, es.closed = true, true
	es.ep.w.check.stats.StreamLost++
}

// abort models the endpoint process going away: its sockets drop.
func (es *epStream) abort() {
	if es.leg != nil && !es.closed {
		es.closed = true
		l := es.leg
		es.ep.w.After(es.ep.w.hop(), l.fail)
	}
}

// openedStream runs on the holder when its stream Action succeeds.
func (e *Endpoint) openedStream(c *Conn, rec *sendRec, out *wire.Out) {
	w := e.w
	sr := rec.sr
	w.check.streamOpened(sr, c.b, out)
	es := w.newEpStream(sr, e, true)
	sr.hes = es
	token := out.Token
	b := c.b
	switch r := w.rng.Float64(); {
	case r < 0.85:
		es.plan = planNow
		w.attach(es, b, token, true, false, false)
	case r < 0.90:
		es.plan = planDelayed
		w.After(w.dur(time.Second, 8*time.Second), func() {
			if !e.retired {
				w.attach(es, b, token, true, false, w.now-sr.resultAt > sr.rec.ttl+w.cfg.Tunables.StreamAttachTTL+2*time.Second)
			}
		})
	case r < 0.95:
		es.plan = planNever
	default:
		// The token is single use: a second attach must fail.
		es.plan = planNow
		w.attach(es, b, token, true, false, false)
		w.After(w.dur(time.Second, 3*time.Second), func() { w.attach(es, b, token, true, true, false) })
	}
	if w.chance(0.03) {
		// A token is valid only on its issuer.
		if o := w.otherBroker(b); o != nil {
			w.attach(es, o, token, false, true, false)
		}
	}
}

// onOffer runs on the target when a stream offer is delivered.
func (e *Endpoint) onOffer(c *Conn, out *wire.Out, rec *sendRec) {
	w := e.w
	sr := rec.sr
	if sr.tes != nil {
		return // a second delivery: the checker reports it
	}
	sr.targetIssuer, sr.targetGen, sr.deliveredAt = c.b, c.b.gen, w.now
	es := w.newEpStream(sr, e, false)
	sr.tes = es
	es.early = w.chance(0.1)
	token := out.StreamToken
	b := c.b
	switch r := w.rng.Float64(); {
	case r < 0.75:
		es.plan = planNow
		w.attach(es, b, token, true, false, false)
	case r < 0.85:
		es.plan = planRefuse
		w.attach(es, b, token, true, false, false)
	case r < 0.95:
		es.plan = planNever
	default:
		es.plan = planLate
		w.After(w.cfg.Tunables.StreamAttachTTL+w.dur(2*time.Second, 10*time.Second), func() {
			if !e.retired {
				w.attach(es, b, token, true, true, true)
			}
		})
	}
	if w.chance(0.03) {
		if o := w.otherBroker(b); o != nil {
			w.attach(es, o, token, false, true, false)
		}
	}
}

func (w *World) otherBroker(not *Broker) *Broker {
	var cs []*Broker
	for _, b := range w.brokers {
		if b != not && b.alive {
			cs = append(cs, b)
		}
	}
	if len(cs) == 0 {
		return nil
	}
	return cs[w.rng.IntN(len(cs))]
}

// --- checker ---

// streamRec is the checker's record of one stream.
type streamRec struct {
	id           string
	rec          *sendRec
	holder       *Endpoint
	target       *Endpoint
	hes, tes     *epStream
	holderIssuer *Broker
	holderGen    int
	targetIssuer *Broker
	targetGen    int
	resultAt     time.Duration
	deliveredAt  time.Duration
	statuses     int
	state        string
	reason       string
	data         bool
	closeSent    [2]*sitem // by side: [0] target, [1] holder
}

func side(holder bool) int {
	if holder {
		return 1
	}
	return 0
}

func (c *Checker) streamOpened(sr *streamRec, b *Broker, out *wire.Out) {
	c.stats.StreamsOpened++
	if out.Stream == "" || out.Token == "" || out.Stream != out.Msg {
		c.violate("stream %s: result without stream ID or token (%s/%s)", sr.rec.nonce, out.Stream, out.Msg)
	}
	if sr.id != "" && sr.id != out.Stream {
		c.violate("stream %s: result names %s, offer named %s", sr.rec.nonce, out.Stream, sr.id)
	}
	sr.id = out.Stream
	sr.holderIssuer, sr.holderGen, sr.resultAt = b, b.gen, c.w.now
	c.streams[sr.id] = sr
}

func (c *Checker) streamOffered(e *Endpoint, out *wire.Out, rec *sendRec) {
	if !rec.stream {
		if out.Stream != "" {
			c.violate("message %s delivered as a stream offer", rec.nonce)
		}
		return
	}
	if out.Stream == "" || out.StreamToken == "" || out.Stream != out.Msg {
		c.violate("stream offer %s delivered without stream ID or token", rec.nonce)
		return
	}
	sr := rec.sr
	if sr.id != "" && sr.id != out.Stream {
		c.violate("stream %s: offer names %s, result named %s", rec.nonce, out.Stream, sr.id)
	}
	sr.id = out.Stream
	c.streams[sr.id] = sr
	c.stats.StreamOffers++
}

func (c *Checker) streamStatus(e *Endpoint, out *wire.Out) {
	sr := c.streams[out.Stream]
	if sr == nil {
		c.violate("%s: stream_status for unknown stream %s", e.name, out.Stream)
		return
	}
	if e != sr.holder {
		c.violate("%s: stream_status for a stream %s opened", e.name, sr.holder.name)
	}
	sr.statuses++
	if sr.statuses > 1 {
		c.violate("stream %s: %d stream_status frames", sr.rec.nonce, sr.statuses)
	}
	sr.state, sr.reason = out.State, out.Reason
	switch out.State {
	case wire.StreamAccepted:
		c.stats.StreamsAccepted++
	case wire.StreamFailed:
		c.stats.StreamsFailed++
		c.stats.Errors["stream:"+out.Reason]++
		switch out.Reason {
		case wire.BounceTTLExpired, wire.BounceTargetGone, wire.BounceReceiverBusy, wire.BounceTooLarge:
			c.bounces[sr.id] = out.Reason
		}
		if sr.data {
			c.violate("stream %s failed (%s) after data flowed", sr.rec.nonce, out.Reason)
		}
	default:
		c.violate("stream %s: bad state %q", sr.rec.nonce, out.State)
	}
}

func (c *Checker) streamAttached(es *epStream, att *attachReq, l *simLeg, code node.AttachCode) {
	if code != node.AttachOK {
		c.stats.Errors[fmt.Sprintf("attach:%d", code)]++
		return
	}
	c.stats.StreamAttachOK++
	c.tokenOK[att.token]++
	sr := es.sr
	if c.tokenOK[att.token] > 1 {
		c.violate("stream %s: token attached %d times", sr.rec.nonce, c.tokenOK[att.token])
	}
	if !att.onIssuer {
		c.violate("stream %s: token attached on a Broker that did not issue it", sr.rec.nonce)
	}
	issuer, gen := sr.holderIssuer, sr.holderGen
	if !es.holder {
		issuer, gen = sr.targetIssuer, sr.targetGen
	}
	if l == nil || l.b != issuer || l.gen != gen {
		c.violate("stream %s: token attached on another Broker or incarnation than its issuer", sr.rec.nonce)
	}
	if att.late {
		c.violate("stream %s: token attached after its half's deadline", sr.rec.nonce)
	}
	if att.probe {
		c.violate("stream %s: a refused attach (reuse, wrong Broker or late) succeeded", sr.rec.nonce)
	}
	if !es.holder && !sr.rec.cap.allowStream {
		c.violate("stream %s: paired on a cap without allow_stream", sr.rec.nonce)
	}
}

func (c *Checker) streamData(es *epStream, m *smsg) {
	sr := es.sr
	c.stats.StreamMsgs++
	if m.stream != sr.id {
		c.violate("stream %s: %s got a message of stream %s", sr.id, es.ep.name, m.stream)
		return
	}
	if m.from != es.peer() || m.fromHolder == es.holder {
		c.violate("stream %s: %s got a message from %s (holder=%v), not its peer %s", sr.id, es.ep.name, m.from.name, m.fromHolder, es.peer().name)
	}
	if m.seq != es.got+1 {
		c.violate("stream %s: %s got seq %d after %d (loss, duplicate or reorder)", sr.id, es.ep.name, m.seq, es.got)
	}
	sr.data = true
	if sr.state == wire.StreamFailed {
		c.violate("stream %s: data flowed after the stream failed (%s)", sr.id, sr.reason)
	}
}

func (c *Checker) streamCloseSent(es *epStream, code int, reason string) {
	if i := side(es.holder); es.sr.closeSent[i] == nil {
		es.sr.closeSent[i] = &sitem{close: true, code: code, reason: reason}
	}
}

func (c *Checker) streamCloseRecv(es *epStream, code int, reason string) {
	sr := es.sr
	c.stats.StreamCloses++
	if code >= 3000 && code < 4000 {
		c.stats.Errors["close:3xxx"]++
	} else {
		c.stats.Errors[fmt.Sprintf("close:%d", code)]++
	}
	if code == wire.CloseNormal || code == wire.CloseStreamRejected || (code >= 3000 && code < 4900) {
		sent := sr.closeSent[side(!es.holder)]
		if sent == nil {
			// A side that attached for a probe closes it with 1000 itself;
			// that never reaches the peer as a stream close.
			c.violate("stream %s: %s got close %d %q its peer never sent", sr.id, es.ep.name, code, reason)
		} else if sent.code != code || sent.reason != reason {
			c.violate("stream %s: %s got close %d %q, peer sent %d %q", sr.id, es.ep.name, code, reason, sent.code, sent.reason)
		}
	}
	if code == wire.CloseUnavailable && es.got > 0 {
		c.violate("stream %s: established stream closed as unavailable (fencing must keep it)", sr.id)
	}
}

// checkStreamDeadlines: no half-stream outlives its deadline.
func (w *World) checkStreamDeadlines() {
	for _, b := range w.brokers {
		if !b.alive {
			continue
		}
		for _, s := range b.node.PendingStreams() {
			if s.Late > 2*time.Second {
				w.check.violate("%s: half-stream %s (holder=%v paired=%v) %v past its deadline", b, s.ID, s.Holder, s.Paired, s.Late)
			}
		}
	}
	w.After(5*time.Second, w.checkStreamDeadlines)
}

func (c *Checker) finalStreamChecks() {
	w := c.w
	end := w.cfg.FaultPhase + w.cfg.Quiet
	if n := c.stats.PeerFrames["other"]; n > 0 {
		c.violate("%d peer frames of an unknown kind", n)
	}
	for _, rec := range c.order {
		sr := rec.sr
		if sr == nil || !rec.expected || rec.status != stOK {
			continue
		}
		hes, tes := sr.hes, sr.tes
		if hes == nil || tes == nil || hes.plan != planNow || tes.plan != planNow || tes.early ||
			hes.gapHi > time.Second || tes.gapHi > time.Second || sr.resultAt > end-40*time.Second {
			continue
		}
		c.stats.StreamsExpected++
		switch {
		case sr.state != wire.StreamAccepted:
			c.violate("quiet-phase stream %s: state %q %s", sr.id, sr.state, sr.reason)
		case hes.got != tes.want || tes.got != hes.want:
			c.violate("quiet-phase stream %s: holder got %d/%d, target got %d/%d", sr.id, hes.got, tes.want, tes.got, hes.want)
		case !tes.ended:
			c.violate("quiet-phase stream %s: the target never saw the close", sr.id)
		default:
			c.stats.StreamsExpectedOK++
		}
	}
}
