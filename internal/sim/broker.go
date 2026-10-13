package sim

import (
	"fmt"
	"io"
	"math/rand/v2"
	"net/netip"
	"time"

	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/internal/node"
	"github.com/afk-surf/nimbo/internal/peerproto"
)

// Broker is a simulated nimbod process slot. A crash discards the node;
// a restart creates a new node with the same key and a new incarnation.
type Broker struct {
	w        *World
	idx      int
	pub      []byte
	id       ids.BrokerID
	name     string
	node     *node.Node
	gen      int
	alive    bool
	left     bool
	baseSkew time.Duration
	skew     time.Duration
	s3Broken bool
	group    int
	nextSess node.SessionID
	conns    map[node.SessionID]*Conn
	started  time.Duration
}

func newBroker(w *World, i int) *Broker {
	r := rand.New(rand.NewChaCha8(seedBytes(w.cfg.Seed, fmt.Sprintf("broker-key-%d", i))))
	pub := make([]byte, 32)
	for j := range pub {
		pub[j] = byte(r.Uint32())
	}
	b := &Broker{w: w, idx: i, pub: pub, id: ids.BrokerIDFromPub(pub), name: fmt.Sprintf("b%d.sim", i), conns: map[node.SessionID]*Conn{}}
	b.baseSkew = w.dur(-1500*time.Millisecond, 1500*time.Millisecond)
	b.skew = b.baseSkew
	return b
}

func (b *Broker) String() string { return fmt.Sprintf("b%d", b.idx) }

func (b *Broker) start() {
	b.gen++
	b.alive = true
	b.left = false
	b.started = b.w.now
	env := &brokerEnv{b: b, gen: b.gen, rng: rand.NewChaCha8(seedBytes(b.w.cfg.Seed, fmt.Sprintf("broker-%d-gen-%d", b.idx, b.gen))), mono: b.w.dur(-time.Hour, time.Hour)}
	cfg := node.Config{
		NodePub:     b.pub,
		Member:      node.MemberInfo{WSURLs: []string{"wss://" + b.name + "/v1/ws"}, Region: fmt.Sprintf("r%d", b.idx%2)},
		PublicNames: []string{b.name},
		Tunables:    b.w.cfg.Tunables,
		Limits:      b.w.cfg.Limits,
		Debug:       b.w.cfg.Log != nil,
	}
	b.node = node.New(cfg, env)
	b.w.logf("%s start gen=%d", b, b.gen)
	b.node.Start()
}

// crash kills the process: all state, timers, links and sessions are lost.
func (b *Broker) crash() {
	if !b.alive {
		return
	}
	b.w.logf("%s CRASH", b)
	b.node.Stop()
	b.alive = false
	b.gen++
	b.w.resetLinksOf(b)
	for _, sid := range sortedSessions(b.conns) {
		c := b.conns[sid]
		delete(b.conns, sid)
		c.brokerGone(b.w.dur(100*time.Millisecond, 3*time.Second))
	}
}

func (b *Broker) restart() {
	if b.alive {
		return
	}
	b.start()
}

// brokerEnv implements node.Env for one broker incarnation.
type brokerEnv struct {
	b    *Broker
	gen  int
	rng  *rand.ChaCha8
	mono time.Duration
}

func (e *brokerEnv) live() bool { return e.b.alive && e.b.gen == e.gen }

// Now is monotonic: true time plus a constant per-process offset.
func (e *brokerEnv) Now() time.Time { return e.b.w.Now().Add(e.mono) }

// Wall is true time plus the broker's current (steppable) skew.
func (e *brokerEnv) Wall() time.Time { return e.b.w.Now().Add(e.b.skew) }
func (e *brokerEnv) Rand() io.Reader { return e.rng }

type simTimer struct{ ev *event }

func (t simTimer) Stop() bool {
	was := !t.ev.canceled
	t.ev.canceled = true
	return was
}

func (e *brokerEnv) AfterFunc(d time.Duration, f func()) node.Timer {
	return simTimer{e.b.w.After(d, func() {
		if e.live() {
			f()
		}
	})}
}

func (e *brokerEnv) SendPeer(to ids.BrokerID, env *peerproto.Envelope) {
	if e.live() {
		e.b.w.sendPeer(e.b, to, env)
	}
}

func (e *brokerEnv) SendSession(s node.SessionID, frame []byte) bool {
	if !e.live() {
		return false
	}
	c := e.b.conns[s]
	if c == nil {
		return true
	}
	c.toEndpoint(frame)
	return true
}

func (e *brokerEnv) CloseSession(s node.SessionID) {
	if !e.live() {
		return
	}
	if c := e.b.conns[s]; c != nil {
		delete(e.b.conns, s)
		c.closedByBroker()
	}
}

func (e *brokerEnv) S3(req node.S3Request, done func(node.S3Response)) {
	if e.live() {
		e.b.w.s3.do(e, req, done)
	}
}

func (e *brokerEnv) MembersChanged([]node.Member) {}

func (e *brokerEnv) Left() {
	if !e.live() {
		return
	}
	b := e.b
	b.w.logf("%s left", b)
	if !b.w.faultsOn {
		b.w.After(5*time.Second, b.restart)
	}
	b.left = true
	b.node.Stop()
	b.alive = false
	b.gen++
	b.w.resetLinksOf(b)
	for _, sid := range sortedSessions(b.conns) {
		c := b.conns[sid]
		delete(b.conns, sid)
		c.brokerGone(time.Millisecond)
	}
}

func (e *brokerEnv) Logf(format string, args ...any) {
	if e.live() {
		e.b.w.logf("%s: %s", e.b, fmt.Sprintf(format, args...))
	}
}

// --- network ---

type link struct {
	gen  uint64
	last time.Duration
}

func (w *World) link(a, b int) *link {
	k := [2]int{a, b}
	l := w.links[k]
	if l == nil {
		l = &link{}
		w.links[k] = l
	}
	return l
}

func (w *World) connected(a, b *Broker) bool {
	return a.alive && b.alive && a.group == b.group
}

func (w *World) resetLink(a, b int) {
	w.link(a, b).gen++
	w.link(b, a).gen++
}

func (w *World) resetLinksOf(b *Broker) {
	for _, o := range w.brokers {
		if o != b {
			w.resetLink(b.idx, o.idx)
		}
	}
}

func (w *World) resetAllLinks() {
	for _, a := range w.brokers {
		for _, b := range w.brokers {
			if a.idx < b.idx {
				w.resetLink(a.idx, b.idx)
			}
		}
	}
}

func (w *World) latency() time.Duration {
	d := w.dur(200*time.Microsecond, 3*time.Millisecond)
	if w.faultsOn && w.chance(w.cfg.Faults.LatencySpikes) {
		d += w.dur(50*time.Millisecond, 500*time.Millisecond)
	}
	return d
}

// sendPeer models the mesh: frames on a link arrive in order; a link reset,
// partition or crash drops frames in flight; sending to an unreachable peer
// fails before anything is written (a dial failure).
func (w *World) sendPeer(src *Broker, to ids.BrokerID, env *peerproto.Envelope) {
	dst := w.byID[to]
	data := peerproto.Marshal(env)
	w.account(env, len(data))
	srcGen := src.gen
	if dst == nil || !w.connected(src, dst) {
		w.After(w.dur(5*time.Millisecond, 300*time.Millisecond), func() {
			if src.alive && src.gen == srcGen {
				src.node.OnPeerSendFailed(to, env)
			}
		})
		return
	}
	l := w.link(src.idx, dst.idx)
	at := w.now + w.latency()
	if at < l.last {
		at = l.last
	}
	l.last = at
	lgen, dstGen := l.gen, dst.gen
	w.At(at, func() {
		if l.gen != lgen || dst.gen != dstGen || !dst.alive {
			return
		}
		e, err := peerproto.Unmarshal(data)
		if err != nil {
			panic(err)
		}
		w.traceEvent("peer", src.id[:4], dst.id[:4], data)
		dst.node.OnPeerMessage(src.id, e)
	})
}

// --- endpoint connections ---

// Conn is one WebSocket session between a simulated endpoint and a broker.
type Conn struct {
	w       *World
	ep      *Endpoint
	b       *Broker
	bgen    int
	sid     node.SessionID
	open    bool // both ends consider it open
	ready   bool // welcome received
	lastToE time.Duration
	lastToB time.Duration
	opened  time.Duration
	readyAt time.Duration
	epDone  bool // the endpoint has processed the close
	// forge: this connection presents a handshake proof bound to another
	// authority (a relayed challenge); the broker must reject it.
	forge bool
}

func sortedSessions(m map[node.SessionID]*Conn) []node.SessionID {
	out := make([]node.SessionID, 0, len(m))
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

func (w *World) dial(ep *Endpoint, b *Broker, ip netip.Addr) *Conn {
	c := &Conn{w: w, ep: ep, b: b, opened: w.now}
	if !b.alive {
		w.After(w.dur(10*time.Millisecond, 500*time.Millisecond), func() { ep.onClosed(c, 0) })
		return c
	}
	b.nextSess++
	c.sid = b.nextSess
	c.bgen = b.gen
	c.open = true
	b.conns[c.sid] = c
	w.After(w.dur(time.Millisecond, 20*time.Millisecond), func() {
		if c.open && b.alive && b.gen == c.bgen {
			b.node.OnSessionOpen(c.sid, ip)
		}
	})
	return c
}

func (c *Conn) arrival(last *time.Duration) time.Duration {
	at := c.w.now + c.w.dur(500*time.Microsecond, 10*time.Millisecond)
	if at < *last {
		at = *last
	}
	*last = at
	return at
}

func (c *Conn) toEndpoint(frame []byte) {
	if !c.open {
		return
	}
	data := append([]byte(nil), frame...)
	c.w.At(c.arrival(&c.lastToE), func() {
		if c.epDone {
			return
		}
		c.w.traceEvent("e<-b", []byte(c.ep.name), data)
		c.w.logf("%s <- %s: %s", c.ep.name, c.b, data)
		c.ep.onFrame(c, data)
	})
}

func (c *Conn) toBroker(frame []byte) {
	if !c.open {
		return
	}
	b, gen, sid := c.b, c.bgen, c.sid
	c.w.At(c.arrival(&c.lastToB), func() {
		if !c.open || !b.alive || b.gen != gen {
			return
		}
		c.w.traceEvent("e->b", []byte(c.ep.name), frame)
		c.w.logf("%s -> %s: %s", c.ep.name, c.b, frame)
		b.node.OnSessionFrame(sid, frame)
	})
}

// closedByBroker: the broker closed the session; frames already queued to
// the endpoint still arrive first.
func (c *Conn) closedByBroker() {
	if !c.open {
		return
	}
	c.open = false
	at := c.arrival(&c.lastToE)
	c.w.At(at, func() { c.ep.onClosed(c, 0) })
}

// brokerGone: the broker process died; the endpoint notices after d.
func (c *Conn) brokerGone(d time.Duration) {
	if !c.open {
		return
	}
	c.open = false
	c.w.After(d, func() { c.ep.onClosed(c, 0) })
}

// close: the endpoint closes the session.
func (c *Conn) close() {
	if !c.open {
		return
	}
	c.open = false
	b, gen, sid := c.b, c.bgen, c.sid
	delete(b.conns, sid)
	c.w.At(c.arrival(&c.lastToB), func() {
		if b.alive && b.gen == gen {
			b.node.OnSessionClosed(sid)
		}
	})
}

func (e *brokerEnv) AnswerAttach(node.StreamLeg, node.AttachCode)             {}
func (e *brokerEnv) DialStreamPeer(ids.BrokerID, ids.MsgID) node.StreamLeg    { return 0 }
func (e *brokerEnv) AnswerStreamPeer(node.StreamLeg, node.PeerStatus)         {}
func (e *brokerEnv) SpliceStream(a, b node.StreamLeg)                         {}
func (e *brokerEnv) CloseStreamLeg(l node.StreamLeg, code int, reason string) {}
