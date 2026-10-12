package sim

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"time"

	"golang.org/x/crypto/curve25519"

	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/pkg/wire"
)

const testSchema = `{"type":"object","properties":{"seq":{"type":"integer"}},"required":["seq"]}`

// Endpoint is a simulated endpoint running a randomized workload.
type Endpoint struct {
	w       *World
	idx     int
	name    string
	priv    [32]byte
	key     ids.Key
	ip      netip.Addr
	retired bool
	want    int
	conns   []*Conn
	backoff time.Duration
	rid     uint64
	pending map[uint64]*pendingReq
	wallet  []*heldCap
	byID    map[ids.CapID]*heldCap
	counter int
	maxMsg  int
	fifo    map[fifoKey]int
}

type pendingReq struct {
	c  *Conn
	cb func(*wire.Out)
}

// heldCap is the endpoint's knowledge of a cap it holds.
type heldCap struct {
	id        ids.CapID
	target    *Endpoint
	lineage   ids.Lineage
	replyOf   string // nonce of the send this is the reply cap of
	singleUse bool
	schema    bool
	base      string
	autoReply bool
	obtained  time.Duration
	lastUse   time.Duration
	spent     bool // a single-use cap that was used successfully
	fresh     bool // obtained in the settled quiet phase
	minted    bool
	contested bool // deliberately used twice at once
}

type fifoKey struct {
	c   *Conn
	cap ids.CapID
}

func (w *World) addEndpoint(delay time.Duration) *Endpoint {
	i := w.nextEpIdx
	w.nextEpIdx++
	e := &Endpoint{w: w, idx: i, name: fmt.Sprintf("e%d", i), pending: map[uint64]*pendingReq{}, byID: map[ids.CapID]*heldCap{}, fifo: map[fifoKey]int{}}
	for j := range e.priv {
		e.priv[j] = byte(w.rng.Uint32())
	}
	pub, _ := curve25519.X25519(e.priv[:], curve25519.Basepoint)
	copy(e.key[:], pub)
	e.ip = netip.AddrFrom4([4]byte{10, 0, byte(i / 8), byte(i % 8)})
	e.want = 1
	if w.chance(0.3) {
		e.want = 2
	}
	e.maxMsg = 16384
	e.backoff = 250 * time.Millisecond
	w.eps = append(w.eps, e)
	w.After(delay, func() {
		e.ensureConns()
		e.scheduleAct()
	})
	return e
}

func (e *Endpoint) liveConns() []*Conn {
	var out []*Conn
	for _, c := range e.conns {
		if c.open && c.ready {
			out = append(out, c)
		}
	}
	return out
}

func (e *Endpoint) connected() bool { return len(e.liveConns()) > 0 }

func (e *Endpoint) ensureConns() {
	if e.retired {
		return
	}
	for len(e.conns) < e.want {
		used := map[*Broker]bool{}
		for _, c := range e.conns {
			used[c.b] = true
		}
		var cands []*Broker
		for _, b := range e.w.brokers {
			if !used[b] {
				cands = append(cands, b)
			}
		}
		if len(cands) == 0 {
			return
		}
		b := cands[e.w.rng.IntN(len(cands))]
		e.conns = append(e.conns, e.w.dial(e, b, e.ip))
	}
}

func (e *Endpoint) send(c *Conn, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	c.toBroker(b)
}

func (e *Endpoint) request(c *Conn, in *wire.In, cb func(*wire.Out)) {
	e.rid++
	in.Rid = e.rid
	e.pending[in.Rid] = &pendingReq{c: c, cb: cb}
	e.send(c, in)
}

func (e *Endpoint) onFrame(c *Conn, data []byte) {
	var out wire.Out
	if err := json.Unmarshal(data, &out); err != nil {
		e.w.check.violate("endpoint %s: bad frame %q", e.name, data)
		return
	}
	switch out.Op {
	case "challenge":
		eph, err1 := ids.B32.DecodeString(out.Eph)
		nonce, err2 := ids.B32.DecodeString(out.Nonce)
		if err1 != nil || err2 != nil {
			e.w.check.violate("bad challenge")
			return
		}
		ss, err := curve25519.X25519(e.priv[:], eph)
		if err != nil {
			return
		}
		proof := wire.Proof(ss, nonce, c.b.name, e.key[:], eph)
		e.send(c, wire.In{Op: "hello", Key: e.key.String(), Authority: c.b.name, Proof: ids.B32.EncodeToString(proof), Client: "sim/1", MaxMessageBytes: e.maxMsg})
	case "welcome":
		c.ready = true
		c.readyAt = e.w.now
		e.backoff = 250 * time.Millisecond
	case "result":
		p := e.pending[out.Rid]
		if p == nil {
			e.w.check.violate("%s: result for unknown rid %d", e.name, out.Rid)
			return
		}
		delete(e.pending, out.Rid)
		p.cb(&out)
	case "deliver":
		e.w.check.delivered(e, c, &out)
	case "bounce":
		e.w.check.stats.Bounced++
	case "closing":
		if out.RetryAfterMs > 0 {
			e.backoff = time.Duration(out.RetryAfterMs) * time.Millisecond
		}
	}
}

func (e *Endpoint) onClosed(c *Conn, _ time.Duration) {
	if c.epDone {
		return
	}
	c.epDone = true
	for i, x := range e.conns {
		if x == c {
			e.conns = append(e.conns[:i:i], e.conns[i+1:]...)
			break
		}
	}
	// Fail requests in flight on this connection: outcome unknown.
	var rids []uint64
	for rid, p := range e.pending {
		if p.c == c {
			rids = append(rids, rid)
		}
	}
	sortU64(rids)
	for _, rid := range rids {
		p := e.pending[rid]
		delete(e.pending, rid)
		p.cb(nil)
	}
	if e.retired {
		return
	}
	d := e.w.dur(e.backoff/2, e.backoff*3/2+time.Millisecond)
	e.backoff = min(2*e.backoff, 30*time.Second)
	e.w.After(d, e.ensureConns)
}

func sortU64(a []uint64) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j] < a[j-1]; j-- {
			a[j], a[j-1] = a[j-1], a[j]
		}
	}
}

// retire models key rotation: the endpoint disconnects for good.
func (e *Endpoint) retire() {
	e.retired = true
	for _, c := range append([]*Conn(nil), e.conns...) {
		c.close()
		e.onClosed(c, 0)
	}
}

// churn drops one connection; the endpoint reconnects with backoff.
func (e *Endpoint) churn() {
	if len(e.conns) == 0 {
		return
	}
	c := e.conns[e.w.rng.IntN(len(e.conns))]
	c.close()
	e.onClosed(c, 0)
}

// --- wallet ---

func (e *Endpoint) addCap(h *heldCap) {
	if old := e.byID[h.id]; old != nil {
		e.w.check.violate("%s: cap %s issued twice", e.name, h.id)
		return
	}
	h.obtained, h.lastUse = e.w.now, e.w.now
	h.fresh = !e.w.faultsOn && e.w.now >= e.w.settledAt
	e.wallet = append(e.wallet, h)
	e.byID[h.id] = h
	for len(e.wallet) > 48 {
		e.dropCap(e.wallet[0])
	}
}

func (e *Endpoint) dropCap(h *heldCap) {
	if e.byID[h.id] != h {
		return
	}
	delete(e.byID, h.id)
	for i, x := range e.wallet {
		if x == h {
			e.wallet = append(e.wallet[:i:i], e.wallet[i+1:]...)
			return
		}
	}
}

// pick chooses a random wallet cap. Single-use caps already being used are
// skipped (deliberate double use is tested separately).
func (e *Endpoint) pick(filter func(*heldCap) bool) *heldCap {
	var cs []*heldCap
	for _, h := range e.wallet {
		if h.singleUse && e.w.check.attempts[h] > 0 {
			continue
		}
		if filter == nil || filter(h) {
			cs = append(cs, h)
		}
	}
	if len(cs) == 0 {
		return nil
	}
	return cs[e.w.rng.IntN(len(cs))]
}

// terminal reports errors after which a cap is useless to its holder.
func terminal(code string) bool {
	switch code {
	case wire.ErrInvalidCap, wire.ErrExpired, wire.ErrConsumed, wire.ErrTargetGone, wire.ErrNotHolder:
		return true
	}
	return false
}

// --- workload ---

func (e *Endpoint) scheduleAct() {
	e.w.After(e.w.exp(700*time.Millisecond)+time.Millisecond, func() {
		if e.retired {
			return
		}
		e.act()
		e.scheduleAct()
	})
}

func (e *Endpoint) act() {
	cs := e.liveConns()
	if len(cs) == 0 {
		return
	}
	c := cs[e.w.rng.IntN(len(cs))]
	// Forget caps idle long past expiry, keeping a few to probe expiry.
	for _, h := range append([]*heldCap(nil), e.wallet...) {
		if e.w.now-h.lastUse > 3*time.Minute {
			e.dropCap(h)
		}
	}
	r := e.w.rng.Float64()
	switch {
	case r < 0.08 || len(e.wallet) == 0:
		e.doMint(c)
	case r < 0.22:
		e.doTransfer(c)
	case r < 0.75:
		e.doSend(c, nil)
	case r < 0.85:
		e.doRenew(c)
	case r < 0.88:
		e.doInspect(c)
	case r < 0.92:
		e.doAttack(c, false)
	case r < 0.93:
		e.doAttack(c, true)
	}
}

func (e *Endpoint) doMint(c *Conn) {
	in := &wire.In{Op: "mint", AutoReply: e.w.chance(0.4), Durable: e.w.chance(0.2)}
	h := &heldCap{target: e, autoReply: in.AutoReply, minted: true}
	if e.w.chance(0.3) {
		in.Schema = json.RawMessage(testSchema)
		h.schema = true
	}
	if e.w.chance(0.3) {
		e.counter++
		h.base = fmt.Sprintf("%s-b%d", e.name, e.counter)
		in.Base = json.RawMessage(fmt.Sprintf(`{"tag":%q}`, h.base))
	}
	e.w.check.stats.Mints++
	e.request(c, in, func(out *wire.Out) {
		if out == nil || !out.OK {
			e.w.check.noteErr(out)
			return
		}
		id, err := ids.ParseCapID(out.Cap)
		if err != nil {
			e.w.check.violate("mint returned bad id %q", out.Cap)
			return
		}
		h.id = id
		h.lineage = ids.Lineage(id)
		e.addCap(h)
		// Grant access: transfer the new cap to someone else.
		if to := e.w.randomPeer(e); to != nil && e.w.chance(0.8) {
			e.transfer(c, h, to)
		}
	})
}

func (w *World) randomPeer(not *Endpoint) *Endpoint {
	var cs []*Endpoint
	for _, e := range w.eps {
		if e != not && !e.retired {
			cs = append(cs, e)
		}
	}
	if len(cs) == 0 {
		return nil
	}
	return cs[w.rng.IntN(len(cs))]
}

func (e *Endpoint) doTransfer(c *Conn) {
	h := e.pick(func(h *heldCap) bool { return !h.spent })
	to := e.w.randomPeer(e)
	if h == nil || to == nil {
		return
	}
	e.transfer(c, h, to)
}

// transfer gives to a copy of h; the new ID is handed over out of band.
func (e *Endpoint) transfer(c *Conn, h *heldCap, to *Endpoint) {
	in := &wire.In{Op: "transfer", Cap: h.id.String(), To: to.key.String(), Durable: !h.singleUse}
	e.w.check.stats.Transfers++
	e.w.check.useAttempt(e, h)
	e.request(c, in, func(out *wire.Out) {
		e.w.check.useResult(e, h, out, useTransfer)
		if out == nil || !out.OK {
			if out != nil && terminal(out.Error.Code) {
				e.dropCap(h)
			}
			return
		}
		id, err := ids.ParseCapID(out.Cap)
		if err != nil {
			e.w.check.violate("transfer returned bad id")
			return
		}
		if h.singleUse {
			h.spent = true
			e.dropCap(h)
		}
		nh := *h
		nh.id, nh.spent, nh.minted = id, false, false
		e.w.After(e.w.dur(time.Millisecond, 50*time.Millisecond), func() {
			if !to.retired {
				to.addCap(&nh)
			}
		})
	})
}

func (e *Endpoint) doSend(c *Conn, h *heldCap) {
	if h == nil {
		h = e.pick(func(h *heldCap) bool { return !h.spent })
	}
	if h == nil {
		return
	}
	e.counter++
	nonce := fmt.Sprintf("%s-%d", e.name, e.counter)
	payload := map[string]any{"nonce": nonce, "seq": e.counter, "from": e.name}
	violate := ""
	if h.schema && e.w.chance(0.05) {
		payload["seq"] = "bad"
		violate = wire.ErrSchemaViolation
	} else if h.base != "" && e.w.chance(0.05) {
		payload["tag"] = "clash"
		violate = wire.ErrBaseConflict
	}
	pb, _ := json.Marshal(payload)
	in := &wire.In{Op: "send", Cap: h.id.String(), Payload: pb, TTLms: int64(e.w.dur(2*time.Second, 20*time.Second) / time.Millisecond)}
	var carried []*heldCap
	if e.w.chance(0.2) {
		if ch := e.pick(func(x *heldCap) bool { return x != h && !x.spent }); ch != nil {
			carried = append(carried, ch)
			in.Caps = []string{ch.id.String()}
		}
	}
	rec := &sendRec{nonce: nonce, from: e, conn: c, cap: *h, ttl: time.Duration(in.TTLms) * time.Millisecond, violate: violate}
	if h.autoReply {
		rec.reply.auto = e.w.chance(0.3)
		if e.w.chance(0.2) {
			rec.reply.schema = true
		}
		if e.w.chance(0.2) {
			e.counter++
			rec.reply.base = fmt.Sprintf("%s-r%d", e.name, e.counter)
		}
		in.Reply = &wire.ReplyOpts{AutoReply: rec.reply.auto}
		if rec.reply.schema {
			in.Reply.Schema = json.RawMessage(testSchema)
		}
		if rec.reply.base != "" {
			in.Reply.Base = json.RawMessage(fmt.Sprintf(`{"tag":%q}`, rec.reply.base))
		}
	}
	for _, ch := range carried {
		rec.carried = append(rec.carried, *ch)
	}
	k := fifoKey{c, h.id}
	e.fifo[k]++
	rec.fifoSeq = e.fifo[k]
	e.w.check.sent(rec)
	e.w.check.useAttempt(e, h)
	for _, ch := range carried {
		e.w.check.useAttempt(e, ch)
	}
	e.request(c, in, func(out *wire.Out) {
		e.w.check.sendResult(rec, out)
		e.w.check.useResult(e, h, out, useSend)
		for _, ch := range carried {
			e.w.check.useResult(e, ch, out, useSend)
		}
		if out != nil && out.OK {
			if h.singleUse {
				h.spent = true
				e.dropCap(h)
			}
			for _, ch := range carried {
				if ch.singleUse {
					ch.spent = true
					e.dropCap(ch)
				}
			}
			return
		}
		if out != nil && terminal(out.Error.Code) && out.Error.Code != wire.ErrNotHolder {
			e.dropCap(h)
		}
	})
}

func (e *Endpoint) doRenew(c *Conn) {
	var hs []*heldCap
	for _, h := range e.wallet {
		if len(hs) < 4 && e.w.chance(0.3) {
			hs = append(hs, h)
		}
	}
	if len(hs) == 0 {
		return
	}
	in := &wire.In{Op: "renew"}
	for _, h := range hs {
		in.Caps = append(in.Caps, h.id.String())
		e.w.check.useAttempt(e, h)
	}
	e.w.check.stats.Renews++
	e.request(c, in, func(out *wire.Out) {
		if out == nil || !out.OK {
			for _, h := range hs {
				e.w.check.useResult(e, h, out, useRenew)
			}
			return
		}
		for i, h := range hs {
			item := out.Renewed[i]
			o := &wire.Out{OK: item.OK, Error: item.Error}
			e.w.check.useResult(e, h, o, useRenew)
			if item.Error != nil && terminal(item.Error.Code) {
				e.dropCap(h)
			}
		}
	})
}

func (e *Endpoint) doInspect(c *Conn) {
	h := e.pick(nil)
	if h == nil {
		return
	}
	e.request(c, &wire.In{Op: "inspect", Cap: h.id.String()}, func(out *wire.Out) {
		e.w.check.inspected(e, h, out)
	})
}

// doAttack uses a cap ID this endpoint does not hold: another endpoint's,
// or a random one. It must never succeed.
func (e *Endpoint) doAttack(c *Conn, random bool) {
	var id ids.CapID
	if random {
		for i := range id {
			id[i] = byte(e.w.rng.Uint32())
		}
	} else {
		victim := e.w.randomPeer(e)
		if victim == nil {
			return
		}
		h := victim.pick(nil)
		if h == nil || e.byID[h.id] != nil {
			return
		}
		id = h.id
	}
	e.w.check.stats.Attacks++
	ops := []*wire.In{
		{Op: "send", Cap: id.String(), Payload: json.RawMessage(`{"attack":true,"seq":1}`)},
		{Op: "transfer", Cap: id.String(), To: e.key.String()},
		{Op: "renew", Caps: []string{id.String()}},
		{Op: "inspect", Cap: id.String()},
	}
	in := ops[e.w.rng.IntN(len(ops))]
	e.request(c, in, func(out *wire.Out) {
		if out == nil {
			return
		}
		ok := out.OK
		if in.Op == "renew" && ok {
			ok = len(out.Renewed) == 1 && out.Renewed[0].OK
		}
		if ok {
			e.w.check.stats.AttacksOK++
			e.w.check.violate("%s: %s with a cap it does not hold succeeded (random=%v)", e.name, in.Op, random)
		}
	})
}

// onDeliver handles the workload side of a delivery: new caps and replies.
func (e *Endpoint) onDeliver(c *Conn, out *wire.Out, rec *sendRec) {
	for i, cs := range out.Caps {
		id, err := ids.ParseCapID(cs)
		if err != nil || rec == nil || i >= len(rec.carried) {
			continue
		}
		h := rec.carried[i]
		h.id, h.spent, h.minted = id, false, false
		e.addCap(&h)
	}
	if out.Reply == "" || rec == nil {
		return
	}
	id, err := ids.ParseCapID(out.Reply)
	if err != nil {
		e.w.check.violate("bad reply id")
		return
	}
	h := &heldCap{id: id, target: rec.from, replyOf: rec.nonce, singleUse: true, schema: rec.reply.schema, base: rec.reply.base, autoReply: rec.reply.auto}
	e.addCap(h)
	switch r := e.w.rng.Float64(); {
	case r < 0.55:
		e.w.After(e.w.dur(time.Millisecond, 200*time.Millisecond), func() {
			if cs := e.liveConns(); len(cs) > 0 && e.byID[id] == h && e.w.check.attempts[h] == 0 {
				e.doSend(cs[0], h)
			}
		})
	case r < 0.65:
		// Double use through two sessions at once: at most one may succeed.
		e.w.After(e.w.dur(time.Millisecond, 200*time.Millisecond), func() {
			cs := e.liveConns()
			if len(cs) == 0 || e.byID[id] != h || e.w.check.attempts[h] > 0 {
				return
			}
			h.contested = true
			e.doSend(cs[0], h)
			e.doSend(cs[len(cs)-1], h)
		})
	case r < 0.72:
		if to := e.w.randomPeer(e); to != nil {
			if cs := e.liveConns(); len(cs) > 0 {
				e.transfer(cs[0], h, to)
			}
		}
	}
}
