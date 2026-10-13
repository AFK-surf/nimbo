// Package node is the Nimbo Broker as a deterministic, single-threaded state
// machine. It performs no I/O and reads no clock or randomness of its own:
// everything goes through Env. The production runtime (internal/host) and the
// deterministic simulator (internal/sim) both drive this same code.
//
// All methods must be called from one goroutine (the node loop). Callbacks
// passed to Env (timers, S3 completions) must also run on that loop.
package node

import (
	"bytes"
	"container/list"
	"errors"
	"io"
	"net/netip"
	"sort"
	"time"

	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/internal/peerproto"
	"github.com/afk-surf/nimbo/internal/presence"
	"github.com/afk-surf/nimbo/internal/schema"
)

// SessionID identifies an endpoint connection, assigned by the runtime.
type SessionID uint64

// Timer is a cancellable timer.
type Timer interface{ Stop() bool }

// S3Op is an S3 operation.
type S3Op int

const (
	S3Get S3Op = iota + 1
	S3Put
	S3List
	S3Delete
)

// S3Request is relative to the cluster prefix.
type S3Request struct {
	Op   S3Op
	Key  string
	Body []byte
}

// S3Object is one LIST entry.
type S3Object struct {
	Key          string
	ETag         string
	LastModified time.Time
}

// S3Response carries the result and the response Date header (zero if
// missing).
type S3Response struct {
	Err          error
	NotFound     bool
	Body         []byte
	ETag         string
	LastModified time.Time
	Date         time.Time
	Objects      []S3Object
}

// Member is a live cluster member, as reported to the runtime for mesh
// configuration.
type Member struct {
	ID   ids.BrokerID
	Info MemberInfo
}

// Env is everything a Node needs from the outside world.
type Env interface {
	// Now is the local clock used for all deadlines and durations. It must
	// be monotonic: in production it is time.Now(), whose monotonic reading
	// makes Sub and Before immune to wall-clock steps.
	Now() time.Time
	// Wall is the local wall clock, used only against S3 time (§4).
	Wall() time.Time
	// Rand is a CSPRNG in production and a seeded stream in simulation.
	Rand() io.Reader
	// AfterFunc runs f on the node loop after d.
	AfterFunc(d time.Duration, f func()) Timer
	// SendPeer sends asynchronously. If the frame cannot be written to the
	// link, the runtime calls Node.OnPeerSendFailed with the same envelope.
	SendPeer(to ids.BrokerID, e *peerproto.Envelope)
	// SendSession queues a frame; false means the session queue is full.
	SendSession(s SessionID, frame []byte) bool
	// CloseSession closes a session. The runtime must not call
	// OnSessionClosed for it afterwards (doing so is harmless).
	CloseSession(s SessionID)
	// S3 runs a request and calls done on the node loop.
	S3(req S3Request, done func(S3Response))
	// MembersChanged reports the live member list (for mesh configuration).
	MembersChanged(members []Member)
	// Left is called once a graceful leave has completed.
	Left()
	Logf(format string, args ...any)

	// Stream data plane (design §9.7). The runtime calls
	// Node.OnStreamLegClosed once for every leg it reported or created,
	// whatever happens to it.
	//
	// AnswerAttach answers a data-socket request: AttachOK upgrades it.
	AnswerAttach(l StreamLeg, code AttachCode)
	// DialStreamPeer opens a data connection for stream id to Broker to,
	// and reports the status byte with Node.OnStreamDialed.
	DialStreamPeer(to ids.BrokerID, id ids.MsgID) StreamLeg
	// AnswerStreamPeer writes the status byte on an inbound data
	// connection.
	AnswerStreamPeer(l StreamLeg, st PeerStatus)
	// SpliceStream joins two legs: messages and the close flow between
	// them until one side closes or fails, then both legs close.
	SpliceStream(a, b StreamLeg)
	// CloseStreamLeg closes a leg with a WebSocket close code and reason;
	// if the leg is spliced, its partner closes with the same code.
	CloseStreamLeg(l StreamLeg, code int, reason string)
}

var (
	errTimeout = errors.New("rpc timeout")
	errNotSent = errors.New("not sent")
)

type rpcCall struct {
	to    ids.BrokerID
	timer Timer
	cb    func(*peerproto.Envelope, error)
}

// Node is one Broker.
type Node struct {
	cfg   Config
	tun   Tunables
	lim   Limits
	env   Env
	id    ids.BrokerID
	short ids.Short
	inc   uint64

	started bool
	stopped bool

	// RPC
	nextReq uint64
	pending map[uint64]*rpcCall

	// membership (membership.go)
	view          *view
	prevView      *view
	viewChangedAt time.Time
	departed      map[presence.RegKey]time.Time
	s3Offset      time.Duration
	learnSeq      uint64
	offSamples    []time.Duration
	s3OffsetOK    bool
	lastHB        time.Time // S3 time of the last successful heartbeat
	hbInFlight    bool
	pollInFlight  bool
	pollAgain     bool // a refresh was requested while a poll was in flight
	hbAgain       bool // the member object changed while a heartbeat was in flight
	lastPut       []byte
	signalGetting bool
	signalETag    string // last ETag of the membership signal seen
	signalWant    bool   // a signal write is due
	signalPutting bool
	ready         bool
	fenced        bool
	draining      bool
	leaving       bool
	left          bool
	drainStarted  time.Time
	knownETags    map[ids.BrokerID]string
	fastJoinNeg   map[ids.BrokerID]time.Time
	fastJoinBusy  map[ids.BrokerID]bool
	fastJoinTok   bucket
	rejoinPending map[ids.BrokerID]bool

	// peers and replication (peers.go)
	peers map[ids.BrokerID]*peer

	// records (store.go)
	recs     map[recKey]*record
	lru      *list.List
	lookups  map[recKey][]func(*record, lookupOutcome)
	schemas  *schema.Cache
	mintTok  bucket
	durables int

	// presence (presence.go)
	pres     map[ids.Key]*presRec
	own      map[ids.Key]*ownReg
	presSeq  uint64
	locCache map[ids.Key]locEntry

	// sessions (session.go)
	sessions   map[SessionID]*session
	byKey      map[ids.Key][]*session
	ipSessions map[netip.Addr]int
	ipBuckets  map[netip.Addr]*bucket

	// routing (router.go)
	seqs map[seqKey]*sequencer
	held map[ids.Key]*holdQueue

	// streams (stream.go)
	holders     map[ids.MsgID]*halfStream
	targets     map[ids.MsgID]*halfStream
	tokens      map[[32]byte]*halfStream
	legs        map[StreamLeg]*halfStream
	unattached  map[ids.Key]int
	legIP       map[StreamLeg]netip.Addr
	ipLegs      map[netip.Addr]int
	attachFails map[netip.Addr]*bucket
	streamDrain bool // established streams were closed at the end of the drain grace

	stats Stats
}

// Stats are counters for metrics and tests.
type Stats struct {
	Delivered, Forwarded, Bounced, Dropped, Held uint64
	Minted, Transferred, Replies                 uint64
	LookupsRemote, LookupsUnresolved             uint64
	Fenced                                       uint64
	ViewChanges                                  uint64
	HintedWrites                                 uint64
	StreamsOpened, StreamOffers, StreamAttaches  uint64
	StreamsPaired, StreamsSpliced                uint64
}

// New creates a Node. Call Start to begin.
func New(cfg Config, env Env) *Node {
	n := &Node{
		cfg:           cfg,
		tun:           cfg.Tunables,
		lim:           cfg.Limits,
		env:           env,
		id:            ids.BrokerIDFromPub(cfg.NodePub),
		pending:       map[uint64]*rpcCall{},
		departed:      map[presence.RegKey]time.Time{},
		knownETags:    map[ids.BrokerID]string{},
		fastJoinNeg:   map[ids.BrokerID]time.Time{},
		fastJoinBusy:  map[ids.BrokerID]bool{},
		rejoinPending: map[ids.BrokerID]bool{},
		peers:         map[ids.BrokerID]*peer{},
		recs:          map[recKey]*record{},
		lru:           list.New(),
		lookups:       map[recKey][]func(*record, lookupOutcome){},
		pres:          map[ids.Key]*presRec{},
		own:           map[ids.Key]*ownReg{},
		locCache:      map[ids.Key]locEntry{},
		sessions:      map[SessionID]*session{},
		byKey:         map[ids.Key][]*session{},
		ipSessions:    map[netip.Addr]int{},
		ipBuckets:     map[netip.Addr]*bucket{},
		seqs:          map[seqKey]*sequencer{},
		held:          map[ids.Key]*holdQueue{},
		holders:       map[ids.MsgID]*halfStream{},
		targets:       map[ids.MsgID]*halfStream{},
		tokens:        map[[32]byte]*halfStream{},
		legs:          map[StreamLeg]*halfStream{},
		unattached:    map[ids.Key]int{},
		legIP:         map[StreamLeg]netip.Addr{},
		ipLegs:        map[netip.Addr]int{},
		attachFails:   map[netip.Addr]*bucket{},
	}
	if n.tun.Replicas == 0 {
		n.tun = DefaultTunables()
	}
	if n.lim.MaxPayloadBytes == 0 {
		n.lim = DefaultLimits()
	}
	n.short = n.id.Short()
	n.schemas = schema.NewCache(n.tun.SchemaCache)
	return n
}

// ID returns the Broker ID.
func (n *Node) ID() ids.BrokerID { return n.id }

// Incarnation returns this process's incarnation nonce.
func (n *Node) Incarnation() uint64 { return n.inc }

// Stats returns a copy of the counters.
func (n *Node) Stats() Stats { return n.stats }

// Ready reports whether the Broker has joined and is not fenced.
func (n *Node) Ready() bool { return n.ready && !n.fenced && !n.draining && !n.left }

// Fenced reports whether the Broker has fenced itself.
func (n *Node) Fenced() bool { return n.fenced }

func (n *Node) now() time.Time { return n.env.Now() }

func (n *Node) logf(format string, args ...any) { n.env.Logf(format, args...) }

func (n *Node) debugf(format string, args ...any) {
	if n.cfg.Debug {
		n.env.Logf(format, args...)
	}
}

// Start begins membership and periodic work.
func (n *Node) Start() {
	if n.started {
		return
	}
	n.started = true
	n.newIncarnation()
	now := n.now()
	n.fastJoinTok = bucket{rate: n.tun.FastJoinPerSec, burst: n.tun.FastJoinPerSec, tokens: n.tun.FastJoinPerSec, at: now}
	n.mintTok = bucket{rate: n.lim.MintAdmission, burst: n.lim.MintAdmission, tokens: n.lim.MintAdmission, at: now}
	n.view = newView(n.tun, map[ids.BrokerID]*member{n.id: n.selfMember()})
	n.viewChangedAt = now
	n.every(n.tun.SweepInterval, n.sweep)
	n.every(n.tun.MemberHeartbeat, n.heartbeat)
	n.every(n.tun.MemberPoll, n.poll)
	n.every(n.tun.MemberSignalPoll, n.pollSignal)
	n.every(n.tun.PresenceReconcile, n.reconcilePresence)
	n.heartbeat()
}

func (n *Node) newIncarnation() {
	old := n.inc
	for n.inc == 0 || n.inc == old {
		n.inc = ids.RandUint64(n.env.Rand())
	}
}

// Stop halts the node without leaving gracefully (as a crash would).
func (n *Node) Stop() {
	n.stopped = true
	for _, c := range n.pending {
		c.timer.Stop()
	}
}

// every runs f periodically with up to 10% jitter.
func (n *Node) every(d time.Duration, f func()) {
	var tick func()
	tick = func() {
		if n.stopped {
			return
		}
		f()
		n.env.AfterFunc(n.jitter(d, 10), tick)
	}
	n.env.AfterFunc(n.jitter(d, 10), tick)
}

// jitter returns d plus up to pct% random extra.
func (n *Node) jitter(d time.Duration, pct int) time.Duration {
	if d <= 0 || pct <= 0 {
		return d
	}
	span := uint64(d) * uint64(pct) / 100
	if span == 0 {
		return d
	}
	return d + time.Duration(ids.RandUint64(n.env.Rand())%span)
}

func (n *Node) after(d time.Duration, f func()) Timer {
	return n.env.AfterFunc(d, func() {
		if n.stopped {
			return
		}
		f()
	})
}

// --- RPC layer ---

// call sends a request and runs cb with the reply or an error. Calls to self
// are dispatched locally on a zero-delay timer.
func (n *Node) call(to ids.BrokerID, e *peerproto.Envelope, cb func(*peerproto.Envelope, error)) {
	n.nextReq++
	req := n.nextReq
	e.From, e.Inc, e.Req = n.id, n.inc, req
	if to == n.id {
		n.after(0, func() { cb(n.handleRequest(n.id, e), nil) })
		return
	}
	c := &rpcCall{to: to, cb: cb}
	c.timer = n.after(n.tun.RPCTimeout, func() {
		if n.pending[req] != c {
			return
		}
		delete(n.pending, req)
		n.peerFailed(to)
		cb(nil, errTimeout)
	})
	n.pending[req] = c
	n.env.SendPeer(to, e)
}

// send sends a one-way message.
func (n *Node) send(to ids.BrokerID, e *peerproto.Envelope) {
	e.From, e.Inc = n.id, n.inc
	if to == n.id {
		n.after(0, func() { n.handleRequest(n.id, e) })
		return
	}
	n.env.SendPeer(to, e)
}

// OnPeerMessage handles a frame from another Broker.
func (n *Node) OnPeerMessage(from ids.BrokerID, e *peerproto.Envelope) {
	if n.stopped || !n.started || e.From != from || from == n.id {
		return
	}
	m := n.view.members[from]
	if m == nil {
		// Unknown sender: the mesh would not admit it. Check S3 (fast join)
		// and drop the frame; the sender retries.
		n.fastJoin(from)
		return
	}
	if m.inc != e.Inc {
		n.observeIncarnation(from, e.Inc)
	}
	if e.Resp != 0 {
		c := n.pending[e.Resp]
		if c == nil || c.to != from {
			return
		}
		delete(n.pending, e.Resp)
		c.timer.Stop()
		n.peerOK(from)
		c.cb(e, nil)
		return
	}
	resp := n.handleRequest(from, e)
	if resp != nil && e.Req != 0 {
		resp.From, resp.Inc, resp.Resp = n.id, n.inc, e.Req
		n.env.SendPeer(from, resp)
	}
}

// OnPeerSendFailed reports a frame that was never written to the link.
func (n *Node) OnPeerSendFailed(to ids.BrokerID, e *peerproto.Envelope) {
	if n.stopped {
		return
	}
	n.peerFailed(to)
	if e.Req == 0 {
		return
	}
	c := n.pending[e.Req]
	if c == nil || c.to != to {
		return
	}
	delete(n.pending, e.Req)
	c.timer.Stop()
	c.cb(nil, errNotSent)
}

func (n *Node) handleRequest(from ids.BrokerID, e *peerproto.Envelope) *peerproto.Envelope {
	switch {
	case e.Forward != nil:
		return &peerproto.Envelope{ForwardResult: n.handleForward(from, e.Forward)}
	case e.RCR != nil:
		return &peerproto.Envelope{RCRAck: n.handleRCR(from, e.RCR)}
	case e.Get != nil:
		return &peerproto.Envelope{GetResult: n.handleGet(e.Get)}
	case e.Consume != nil:
		return &peerproto.Envelope{ConsumeResult: n.handleConsume(e.Consume)}
	case e.Watch != nil:
		return &peerproto.Envelope{WatchResult: n.handleWatch(from, e.Watch)}
	case e.Notify != nil:
		n.handleNotify(e.Notify)
	case e.Rejoin != nil:
		n.handleRejoin(from, e.Rejoin)
		return &peerproto.Envelope{RejoinAck: &peerproto.RejoinAck{}}
	case e.Bounce != nil:
		n.handleBounce(e.Bounce)
	}
	return nil
}

// sweep runs once per SweepInterval.
func (n *Node) sweep() {
	n.checkFence()
	n.sweepRecords()
	n.sweepPresence()
	n.sweepPeers()
	n.sweepHeld()
	n.sweepSessions()
	n.sweepDrain()
}

// --- helpers ---

type bucket struct {
	rate, burst, tokens float64
	at                  time.Time
}

func newBucket(rate, burst float64, now time.Time) *bucket {
	return &bucket{rate: rate, burst: burst, tokens: burst, at: now}
}

func (b *bucket) take(now time.Time, n float64) bool {
	if el := now.Sub(b.at).Seconds(); el > 0 {
		b.tokens = min(b.burst, b.tokens+el*b.rate)
		b.at = now
	}
	if b.tokens < n {
		return false
	}
	b.tokens -= n
	return true
}

func sortBrokers(bs []ids.BrokerID) {
	sort.Slice(bs, func(i, j int) bool { return bytes.Compare(bs[i][:], bs[j][:]) < 0 })
}

func sortedKeys[V any](m map[ids.Key]V) []ids.Key {
	ks := make([]ids.Key, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Slice(ks, func(i, j int) bool { return bytes.Compare(ks[i][:], ks[j][:]) < 0 })
	return ks
}

func sortedBrokerKeys[V any](m map[ids.BrokerID]V) []ids.BrokerID {
	ks := make([]ids.BrokerID, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sortBrokers(ks)
	return ks
}

func ms(d time.Duration) int64 { return d.Milliseconds() }

func fromMs(v int64) time.Duration { return time.Duration(v) * time.Millisecond }

// Snapshot is a point-in-time view for metrics and /debug.
type Snapshot struct {
	Stats
	Members      int
	Sessions     int
	Records      int
	Presence     int
	QueuedOps    int
	HeldMessages int
	Suspects     int
	FencedNow    bool
	Draining     bool
	Ready        bool
	S3Offset     time.Duration
	Incarnation  uint64
	// Half-streams not yet spliced, and data-plane legs.
	PendingStreams int
	StreamLegs     int
}

// Snapshot returns current counters and gauges.
func (n *Node) Snapshot() Snapshot {
	sus := 0
	for id := range n.peers {
		if n.isSuspect(id) {
			sus++
		}
	}
	return Snapshot{
		Stats:        n.stats,
		Members:      len(n.view.members),
		Sessions:     n.SessionCount(),
		Records:      len(n.recs),
		Presence:     len(n.pres),
		QueuedOps:    n.queuedOps(),
		HeldMessages: n.HeldMessages(),
		Suspects:     sus,
		FencedNow:    n.fenced,
		Draining:     n.draining,
		Ready:        n.Ready(),
		S3Offset:     n.s3Offset,
		Incarnation:  n.inc,

		PendingStreams: len(n.holders) + len(n.targets),
		StreamLegs:     len(n.legs),
	}
}
