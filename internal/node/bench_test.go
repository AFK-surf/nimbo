package node

import (
	"bytes"
	"container/heap"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/netip"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/curve25519"

	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/internal/peerproto"
	"github.com/afk-surf/nimbo/pkg/wire"
)

// A synchronous, in-process cluster for benchmarking Broker CPU cost per
// operation. Unlike internal/sim it adds no latency or faults: every queued
// event runs as soon as the previous one finishes, so wall time is pure
// Broker work (plus frame encoding and a minimal client).

type bench struct {
	tb     testing.TB
	now    time.Time
	q      []func()
	timers timerHeap
	seq    uint64
	nodes  []*Node
	byID   map[ids.BrokerID]*Node
	s3     map[string]benchObj
	eps    map[*Node]map[SessionID]*benchEP
	frames uint64 // peer frames
	bytes  uint64 // peer bytes

	holdList map[*Node]bool // hold back LIST responses to these nodes
	holdS3   map[*Node]bool // hold back every S3 response to these nodes
	started  int
	heldList []func()
	mute     map[ids.BrokerID]bool // frames from these Brokers are not sent
	tune     func(*Tunables)       // applied to Brokers added from now on
	lists    map[*Node]int         // S3 LISTs per Broker
	// s3Fail, if set, makes the S3 requests it returns true for fail
	// without taking effect.
	s3Fail func(*Node, S3Request) bool
}

type benchObj struct {
	body []byte
	lm   time.Time
}

type btimer struct {
	at   time.Time
	seq  uint64
	f    func()
	dead bool
}

func (t *btimer) Stop() bool { was := !t.dead; t.dead = true; return was }

type timerHeap []*btimer

func (h timerHeap) Len() int { return len(h) }
func (h timerHeap) Less(i, j int) bool {
	if !h[i].at.Equal(h[j].at) {
		return h[i].at.Before(h[j].at)
	}
	return h[i].seq < h[j].seq
}
func (h timerHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *timerHeap) Push(x any)   { *h = append(*h, x.(*btimer)) }
func (h *timerHeap) Pop() any {
	o := *h
	t := o[len(o)-1]
	*h = o[:len(o)-1]
	return t
}

type benchEnv struct {
	b   *bench
	n   *Node
	rng *rand.ChaCha8
}

func (e *benchEnv) Now() time.Time  { return e.b.now }
func (e *benchEnv) Wall() time.Time { return e.b.now }
func (e *benchEnv) Rand() io.Reader { return e.rng }
func (e *benchEnv) AfterFunc(d time.Duration, f func()) Timer {
	if d <= 0 {
		t := &btimer{}
		e.b.q = append(e.b.q, func() {
			if !t.dead {
				f()
			}
		})
		return t
	}
	e.b.seq++
	t := &btimer{at: e.b.now.Add(d), seq: e.b.seq, f: f}
	heap.Push(&e.b.timers, t)
	return t
}
func (e *benchEnv) SendPeer(to ids.BrokerID, m *peerproto.Envelope) {
	data := peerproto.Marshal(m)
	e.b.frames++
	e.b.bytes += uint64(len(data))
	from := e.n.id
	if e.b.mute[from] {
		e.b.q = append(e.b.q, func() { e.n.OnPeerSendFailed(to, m) })
		return
	}
	e.b.q = append(e.b.q, func() {
		d, err := peerproto.Unmarshal(data)
		if err != nil {
			panic(err)
		}
		e.b.byID[to].OnPeerMessage(from, d)
	})
}
func (e *benchEnv) SendSession(s SessionID, frame []byte) bool {
	if ep := e.b.eps[e.n][s]; ep != nil {
		ep.onFrame(frame)
	}
	return true
}
func (e *benchEnv) CloseSession(SessionID) {}
func (e *benchEnv) S3(req S3Request, done func(S3Response)) {
	b := e.b
	// The request takes effect now; the response is delivered later (and
	// can be held back by a test to reorder it against other events).
	resp := S3Response{Date: b.now.Truncate(time.Second)}
	op := req.Op
	if b.s3Fail != nil && b.s3Fail(e.n, req) {
		resp.Err, op = fmt.Errorf("injected S3 failure"), -1
	}
	switch op {
	case S3Put:
		b.s3[req.Key] = benchObj{body: append([]byte(nil), req.Body...), lm: b.now}
		resp.ETag = fmt.Sprint(len(req.Body), string(req.Body))
	case S3Get:
		o, ok := b.s3[req.Key]
		resp.NotFound = !ok
		resp.Body, resp.LastModified, resp.ETag = o.body, o.lm, fmt.Sprint(len(o.body), string(o.body))
	case S3List:
		if b.lists != nil {
			b.lists[e.n]++
		}
		var keys []string
		for k := range b.s3 {
			if strings.HasPrefix(k, req.Key) {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		for _, k := range keys {
			o := b.s3[k]
			resp.Objects = append(resp.Objects, S3Object{Key: k, ETag: fmt.Sprint(len(o.body), string(o.body)), LastModified: o.lm})
		}
	case S3Delete:
		delete(b.s3, req.Key)
	}
	deliver := func() { done(resp) }
	if b.holdS3[e.n] || (req.Op == S3List && b.holdList[e.n]) {
		b.heldList = append(b.heldList, deliver)
		return
	}
	b.q = append(b.q, deliver)
}

func (e *benchEnv) MembersChanged([]Member)                             {}
func (e *benchEnv) Left()                                               {}
func (e *benchEnv) Logf(format string, args ...any)                     {}
func (e *benchEnv) AnswerAttach(StreamLeg, AttachCode)                  {}
func (e *benchEnv) DialStreamPeer(ids.BrokerID, ids.MsgID) StreamLeg    { return 0 }
func (e *benchEnv) AnswerStreamPeer(StreamLeg, PeerStatus)              {}
func (e *benchEnv) SpliceStream(a, b StreamLeg)                         {}
func (e *benchEnv) CloseStreamLeg(l StreamLeg, code int, reason string) {}

// drain runs queued events until none are left.
func (b *bench) drain() {
	for len(b.q) > 0 {
		f := b.q[0]
		b.q[0] = nil
		b.q = b.q[1:]
		f()
	}
}

// advance moves time forward, firing due timers.
func (b *bench) advance(d time.Duration) {
	end := b.now.Add(d)
	for {
		b.drain()
		if b.timers.Len() == 0 || b.timers[0].at.After(end) {
			break
		}
		t := heap.Pop(&b.timers).(*btimer)
		if t.dead {
			continue
		}
		b.now = t.at
		t.f()
	}
	b.now = end
	b.drain()
}

func newBench(tb testing.TB, brokers int) *bench {
	b := &bench{tb: tb, now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), byID: map[ids.BrokerID]*Node{}, s3: map[string]benchObj{}, eps: map[*Node]map[SessionID]*benchEP{}, holdList: map[*Node]bool{}, holdS3: map[*Node]bool{}}
	for i := 0; i < brokers; i++ {
		b.addNode(i)
	}
	for _, n := range b.nodes {
		n.Start()
		b.advance(10 * time.Millisecond)
	}
	b.advance(35 * time.Second) // everyone polls and sees everyone
	for _, n := range b.nodes {
		if len(n.view.members) != brokers || !n.Ready() {
			tb.Fatalf("cluster not formed: %d/%d", len(n.view.members), brokers)
		}
	}
	return b
}

// addNode creates (but does not start) Broker number i.
func (b *bench) addNode(i int) *Node {
	{
		var seed [32]byte
		seed[0], seed[1] = byte(i), byte(i>>8)
		pub := make([]byte, 32)
		rand.NewChaCha8(seed).Read(pub)
		b.started++ // a restarted Broker gets a new randomness stream
		env := &benchEnv{b: b, rng: rand.NewChaCha8([32]byte{byte(i), byte(i >> 8), 1, byte(b.started), byte(b.started >> 8)})}
		lim := DefaultLimits()
		// The harness drives one session as fast as the CPU allows.
		lim.ActionsPerSec, lim.ActionsBurst, lim.MintsPerSec, lim.MintsBurst = 1e12, 1e12, 1e12, 1e12
		lim.MintAdmission, lim.MaxLiveCaps = 1e12, 1<<30
		lim.CapsUsedPer10s, lim.UnknownPer10s, lim.BlobBytesPer10s = 1<<30, 1<<30, 1<<30
		lim.SessionsPerIP, lim.HandshakesPerSec, lim.HandshakeBurst = 1<<30, 1e12, 1e12
		n := New(Config{NodePub: pub, PublicNames: []string{"b.bench"}, Tunables: b.tunables(), Limits: lim}, env)
		env.n = n
		b.nodes = append(b.nodes, n)
		b.byID[n.id] = n
		b.eps[n] = map[SessionID]*benchEP{}
		return n
	}
}

func (b *bench) tunables() Tunables {
	t := DefaultTunables()
	if b.tune != nil {
		b.tune(&t)
	}
	return t
}

// benchEP is a minimal endpoint client.
type benchEP struct {
	b        *bench
	n        *Node
	sid      SessionID
	priv     [32]byte
	key      ids.Key
	rid      uint64
	last     wire.Out // last result
	results  int
	delivers int
	lastDel  wire.Out
	ready    bool
}

var nextSID SessionID

func (b *bench) connect(n *Node) *benchEP {
	nextSID++
	ep := &benchEP{b: b, n: n, sid: nextSID}
	b.rngBytes(ep.priv[:])
	pub, _ := curve25519.X25519(ep.priv[:], curve25519.Basepoint)
	copy(ep.key[:], pub)
	b.eps[n][ep.sid] = ep
	n.OnSessionOpen(ep.sid, netip.MustParseAddr("10.0.0.1"))
	b.drain()
	if !ep.ready {
		b.tb.Fatal("handshake failed")
	}
	return ep
}

var benchRng = rand.New(rand.NewChaCha8([32]byte{9}))

func (b *bench) rngBytes(p []byte) {
	for i := range p {
		p[i] = byte(benchRng.Uint32())
	}
}

func (ep *benchEP) onFrame(f []byte) {
	switch {
	case bytes.HasPrefix(f, []byte(`{"op":"deliver"`)):
		ep.delivers++
		if ep.b.tb != nil && ep.wantDeliver() {
			json.Unmarshal(f, &ep.lastDel)
		}
	case bytes.HasPrefix(f, []byte(`{"op":"result"`)):
		ep.results++
		ep.last = wire.Out{}
		json.Unmarshal(f, &ep.last)
	case bytes.HasPrefix(f, []byte(`{"op":"challenge"`)):
		var ch wire.Out
		json.Unmarshal(f, &ch)
		eph, _ := ids.B32.DecodeString(ch.Eph)
		nonce, _ := ids.B32.DecodeString(ch.Nonce)
		ss, _ := curve25519.X25519(ep.priv[:], eph)
		proof := wire.Proof(ss, nonce, "b.bench", ep.key[:], eph)
		in, _ := json.Marshal(wire.In{Op: "hello", Key: ep.key.String(), Authority: "b.bench", Proof: ids.B32.EncodeToString(proof)})
		ep.b.q = append(ep.b.q, func() { ep.n.OnSessionFrame(ep.sid, in) })
	case bytes.HasPrefix(f, []byte(`{"op":"welcome"`)):
		ep.ready = true
	}
}

// wantDeliver: only decode deliveries when a test needs them (reply caps).
func (ep *benchEP) wantDeliver() bool { return true }

// do sends a request frame and runs the cluster until it is quiet.
func (ep *benchEP) do(in []byte) {
	ep.n.OnSessionFrame(ep.sid, in)
	ep.b.drain()
}

func (ep *benchEP) req(v *wire.In) wire.Out {
	ep.rid++
	v.Rid = ep.rid
	b, _ := json.Marshal(v)
	ep.do(b)
	return ep.last
}

func (ep *benchEP) mustReq(v *wire.In) wire.Out {
	out := ep.req(v)
	if !out.OK {
		ep.b.tb.Fatalf("%s failed: %+v", v.Op, out.Error)
	}
	return out
}

// grant: owner mints a cap (with options) and transfers it to holder.
func grant(owner, holder *benchEP, autoReply bool, schema string) string {
	in := &wire.In{Op: "mint", AutoReply: autoReply}
	if schema != "" {
		in.Schema = json.RawMessage(schema)
	}
	c := owner.mustReq(in).Cap
	return owner.mustReq(&wire.In{Op: "transfer", Cap: c, To: holder.key.String()}).Cap
}

func sendFrame(capID string, payload string) []byte {
	return []byte(`{"op":"send","rid":1,"cap":"` + capID + `","payload":` + payload + `}`)
}

const benchPayload = `{"q":"what is the weather in Lisbon","n":3,"tags":["a","b","c"]}`
const benchSchema = `{"type":"object","required":["q","n"],"properties":{"q":{"type":"string","maxLength":200},"n":{"type":"integer","minimum":0},"tags":{"type":"array","items":{"type":"string"}}}}`

// runSends measures one send per op, advancing simulated time by 100µs per
// op so that sliding-expiry touches and periodic work happen at a
// realistic rate (one cap used 10,000 times a second).
func runSends(b *testing.B, h *bench, from, to *benchEP, capID string) {
	frame := sendFrame(capID, benchPayload)
	from.do(frame) // warm caches and the location cache
	if to.delivers == 0 {
		b.Fatalf("not delivered: %+v", from.last)
	}
	start, frames, bytes := to.delivers, h.frames, h.bytes
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		from.do(frame)
		if i%100 == 99 {
			h.advance(10 * time.Millisecond)
		}
	}
	b.StopTimer()
	if got := to.delivers - start; got != b.N {
		b.Fatalf("delivered %d of %d", got, b.N)
	}
	b.ReportMetric(float64(h.frames-frames)/float64(b.N), "peerframes/op")
	b.ReportMetric(float64(h.bytes-bytes)/float64(b.N), "peerbytes/op")
}

func benchSend(b *testing.B, brokers int, remote, autoReply bool, schema string) {
	h := newBench(b, brokers)
	target := h.connect(h.nodes[0])
	senderNode := h.nodes[0]
	if remote {
		senderNode = h.nodes[1]
	}
	sender := h.connect(senderNode)
	capID := grant(target, sender, autoReply, schema)
	runSends(b, h, sender, target, capID)
}

func BenchmarkSendLocal3(b *testing.B)        { benchSend(b, 3, false, false, "") }
func BenchmarkSendRemote3(b *testing.B)       { benchSend(b, 3, true, false, "") }
func BenchmarkSendRemoteSchema3(b *testing.B) { benchSend(b, 3, true, false, benchSchema) }
func BenchmarkSendRemoteReply3(b *testing.B)  { benchSend(b, 3, true, true, "") }
func BenchmarkSendRemote100(b *testing.B)     { benchSend(b, 100, true, false, "") }
func BenchmarkSendRemoteReply100(b *testing.B) {
	benchSend(b, 100, true, true, "")
}

func benchCapOp(b *testing.B, brokers int, op string) {
	h := newBench(b, brokers)
	a := h.connect(h.nodes[0])
	other := h.connect(h.nodes[1])
	capID := a.mustReq(&wire.In{Op: "mint"}).Cap
	var frame []byte
	switch op {
	case "mint":
		frame = []byte(`{"op":"mint","rid":1}`)
	case "mint-durable":
		frame = []byte(`{"op":"mint","rid":1,"durable":true}`)
	case "transfer":
		frame = []byte(`{"op":"transfer","rid":1,"cap":"` + capID + `","to":"` + other.key.String() + `"}`)
	case "renew":
		frame = []byte(`{"op":"renew","rid":1,"caps":["` + capID + `"]}`)
	}
	frames := h.frames
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.do(frame)
		if !a.last.OK {
			b.Fatalf("%s: %+v", op, a.last.Error)
		}
		if i%100 == 99 {
			h.advance(10 * time.Millisecond)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(h.frames-frames)/float64(b.N), "peerframes/op")
}

func BenchmarkMint3(b *testing.B)          { benchCapOp(b, 3, "mint") }
func BenchmarkMint100(b *testing.B)        { benchCapOp(b, 100, "mint") }
func BenchmarkMintDurable100(b *testing.B) { benchCapOp(b, 100, "mint-durable") }
func BenchmarkTransfer100(b *testing.B)    { benchCapOp(b, 100, "transfer") }
func BenchmarkRenew100(b *testing.B)       { benchCapOp(b, 100, "renew") }

// BenchmarkSessionLifecycle: handshake (2 X25519 on the Broker + HMAC),
// welcome, presence write to 3 Brokers, and close.
func BenchmarkSessionLifecycle100(b *testing.B) {
	h := newBench(b, 100)
	n := h.nodes[0]
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ep := h.connect(n)
		n.OnSessionClosed(ep.sid)
		delete(h.eps[n], ep.sid)
		h.drain()
		if i%100 == 99 {
			h.advance(10 * time.Millisecond)
		}
	}
}

// TestMemoryFootprint reports heap bytes per session, per cap record and
// per presence register held, for capacity planning (run with -v).
func TestMemoryFootprint(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates a lot")
	}
	measure := func(name string, n int, f func(i int)) {
		runtime.GC()
		var m0, m1 runtime.MemStats
		runtime.ReadMemStats(&m0)
		for i := 0; i < n; i++ {
			f(i)
		}
		runtime.GC()
		runtime.ReadMemStats(&m1)
		t.Logf("%-34s %6.0f bytes each", name, float64(int64(m1.HeapAlloc)-int64(m0.HeapAlloc))/float64(n))
	}
	h := newBench(t, 3)
	n0 := h.nodes[0]
	var keep []*benchEP
	measure("session (all 3 brokers' state)", 2000, func(int) { keep = append(keep, h.connect(n0)) })
	a := keep[0]
	measure("minted cap (all copies)", 20000, func(int) {
		a.do([]byte(`{"op":"mint","rid":1}`))
	})
	runtime.KeepAlive(keep)
}
