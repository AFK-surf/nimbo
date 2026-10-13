package node

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"sort"
	"time"

	"github.com/afk-surf/nimbo/internal/caprec"
	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/internal/peerproto"
	"github.com/afk-surf/nimbo/internal/presence"
	"github.com/afk-surf/nimbo/internal/schema"
	"github.com/afk-surf/nimbo/pkg/wire"
)

// Routing: ingress sequencing and validation, location, forwarding, egress
// delivery and held messages (design §9).

type seqKey struct {
	s SessionID
	c ids.CapID
}

type sendReq struct {
	s     *session
	in    *wire.In
	capID ids.CapID
}

type sequencer struct {
	q    []*sendReq
	busy bool
}

type message struct {
	id             ids.MsgID
	deadline       time.Time
	hop            int
	reroutes       int
	target         ids.CapID
	targetRec      []byte
	tcap           *caprec.Record
	payload        []byte
	carried        []peerproto.CapRef
	replyID        *ids.Lineage
	replySchema    *ids.Hash
	replyBase      *ids.Hash
	replyAuto      bool
	ingress        ids.BrokerID
	ingressSession uint64
	sender         ids.Key
	senderHints    []ids.Short
}

type holdQueue struct {
	key          ids.Key
	msgs         []*message
	nextBackstop time.Time
	watchUntil   time.Time
}

func (n *Node) actSend(s *session, in *wire.In) {
	id, err := ids.ParseCapID(in.Cap)
	if err != nil {
		n.fail(s, in.Rid, wire.ErrBadRequest, "cap")
		return
	}
	if s.pendingSend >= n.lim.MaxPendingPerSess {
		n.fail(s, in.Rid, wire.ErrRateLimited, "pending sends")
		return
	}
	s.pendingSend++
	k := seqKey{s.id, id}
	sq := n.seqs[k]
	if sq == nil {
		sq = &sequencer{}
		n.seqs[k] = sq
	}
	sq.q = append(sq.q, &sendReq{s: s, in: in, capID: id})
	n.runSeq(k)
}

// runSeq processes one send at a time per (session, cap): message n+1 starts
// only after message n is handed off, held or dropped.
func (n *Node) runSeq(k seqKey) {
	sq := n.seqs[k]
	if sq == nil || sq.busy {
		return
	}
	if len(sq.q) == 0 {
		delete(n.seqs, k)
		return
	}
	req := sq.q[0]
	sq.q = sq.q[1:]
	sq.busy = true
	n.processSend(req, func() {
		req.s.pendingSend--
		sq.busy = false
		n.runSeq(k)
	})
}

func isObject(b []byte) bool {
	b = bytes.TrimLeft(b, " \t\r\n")
	return len(b) > 0 && b[0] == '{'
}

func (n *Node) processSend(req *sendReq, done func()) {
	s, in := req.s, req.in
	if s.closed {
		done()
		return
	}
	failDone := func(e *capErr) {
		n.fail(s, in.Rid, e.code, e.detail)
		done()
	}
	var payload map[string]json.RawMessage
	if len(in.Payload) > n.lim.MaxPayloadBytes {
		failDone(cerr(wire.ErrTooLarge, "payload"))
		return
	}
	if !isObject(in.Payload) || json.Unmarshal(in.Payload, &payload) != nil || payload == nil {
		failDone(cerr(wire.ErrBadRequest, "payload must be a JSON object"))
		return
	}
	if len(in.Caps) > n.lim.MaxCarriedCaps {
		failDone(cerr(wire.ErrBadRequest, "too many carried caps"))
		return
	}
	carriedIDs := make([]ids.CapID, len(in.Caps))
	for i, c := range in.Caps {
		id, err := ids.ParseCapID(c)
		if err != nil || id == req.capID {
			failDone(cerr(wire.ErrBadRequest, "carried cap"))
			return
		}
		for _, prev := range carriedIDs[:i] {
			if prev == id {
				failDone(cerr(wire.ErrBadRequest, "duplicate carried cap"))
				return
			}
		}
		carriedIDs[i] = id
	}
	ttl := n.tun.DefaultMsgTTL
	if in.TTLms > 0 {
		ttl = min(fromMs(in.TTLms), n.tun.MaxMsgTTL)
	}
	n.validateCap(s, req.capID, true, func(v *validated, e *capErr) {
		if e != nil {
			failDone(e)
			return
		}
		n.checkTarget(v.r.cap, func(st presence.State, owners []ids.BrokerID, e *capErr) {
			if e != nil {
				failDone(e)
				return
			}
			n.validateCarried(s, carriedIDs, func(carried []*validated, e *capErr) {
				if e != nil {
					failDone(e)
					return
				}
				n.acceptSend(s, in, v, carried, payload, ttl, done, failDone)
			})
		})
	})
}

func (n *Node) validateCarried(s *session, idsIn []ids.CapID, cb func([]*validated, *capErr)) {
	out := make([]*validated, len(idsIn))
	var firstErr *capErr
	left := len(idsIn)
	if left == 0 {
		cb(out, nil)
		return
	}
	for i, id := range idsIn {
		i := i
		n.validateCap(s, id, false, func(v *validated, e *capErr) {
			out[i] = v
			if e != nil && firstErr == nil {
				firstErr = e
			}
			left--
			if left == 0 {
				cb(out, firstErr)
			}
		})
	}
}

// acceptSend runs steps 3-5 of ingress and hands the message to routing.
func (n *Node) acceptSend(s *session, in *wire.In, v *validated, carried []*validated, payload map[string]json.RawMessage, ttl time.Duration, done func(), failDone func(*capErr)) {
	c := v.r.cap
	if v.schema != nil {
		sch, err := n.schemas.Get(ids.Hash(v.schema.key.id), v.schema.data)
		if err != nil {
			failDone(cerr(wire.ErrUnavailable, "schema blob"))
			return
		}
		if err := schema.Validate(sch, in.Payload); err != nil {
			failDone(cerr(wire.ErrSchemaViolation, err.Error()))
			return
		}
	}
	if v.base != nil {
		var base map[string]json.RawMessage
		if json.Unmarshal(v.base.data, &base) != nil {
			failDone(cerr(wire.ErrUnavailable, "base blob"))
			return
		}
		for k, val := range base {
			if _, dup := payload[k]; dup {
				failDone(cerr(wire.ErrBaseConflict, k))
				return
			}
			payload[k] = val
		}
	}
	merged, err := json.Marshal(payload)
	if err != nil {
		failDone(cerr(wire.ErrBadRequest, "payload"))
		return
	}
	m := &message{
		deadline:       n.now().Add(ttl),
		target:         v.r.key.capID(),
		targetRec:      v.r.data,
		tcap:           c,
		payload:        merged,
		ingress:        n.id,
		ingressSession: uint64(s.id),
		sender:         s.key,
		senderHints:    []ids.Short{n.short},
	}
	if c.AutoReply {
		var e *capErr
		var reply wire.ReplyOpts
		if in.Reply != nil {
			reply = *in.Reply
		}
		m.replySchema, m.replyBase, e = n.prepBlobs(s, reply.Schema, reply.Base)
		if e != nil {
			failDone(e)
			return
		}
		m.replyAuto = reply.AutoReply
		var rid ids.Lineage
		ids.RandBytes(n.env.Rand(), rid[:])
		m.replyID = &rid
	}
	for _, cv := range carried {
		m.carried = append(m.carried, peerproto.CapRef{ID: cv.r.key.capID(), Record: cv.r.data})
	}
	// Step 6 of validation: consume single-use caps (target first) once
	// everything else has passed.
	var single []*record
	if v.r.singleUse() {
		single = append(single, v.r)
	}
	for _, cv := range carried {
		if cv.r.singleUse() {
			single = append(single, cv.r)
		}
	}
	var consumeNext func(i int)
	consumeNext = func(i int) {
		if i < len(single) {
			n.consume(single[i], func(e *capErr) {
				if e != nil {
					failDone(e)
					return
				}
				consumeNext(i + 1)
			})
			return
		}
		if s.closed {
			done()
			return
		}
		// Accept: every validated cap counts as a use.
		for _, x := range append([]*validated{v}, carried...) {
			if !x.r.singleUse() {
				n.useRec(x.r)
			}
			n.useBlobs(x.r.cap)
		}
		ids.RandBytes(n.env.Rand(), m.id[:])
		res := wire.Result{Rid: in.Rid, Msg: m.id.String()}
		if m.replyID != nil {
			res.ReplyID = m.replyID.String()
		}
		n.ok(s, res)
		n.route(m, done)
	}
	consumeNext(0)
}

// --- routing ---

func (n *Node) bounce(m *message, reason string) {
	n.stats.Bounced++
	if m.ingress == n.id {
		n.bounceLocal(SessionID(m.ingressSession), m.id, reason)
		return
	}
	n.send(m.ingress, &peerproto.Envelope{Bounce: &peerproto.Bounce{Msg: m.id, Session: m.ingressSession, Reason: reason}})
}

func (n *Node) bounceLocal(sid SessionID, msg ids.MsgID, reason string) {
	if s := n.sessions[sid]; s != nil && s.established {
		n.sendFrame(sid, wire.Bounce{Op: "bounce", Msg: msg.String(), Reason: reason})
	}
}

func (n *Node) handleBounce(b *peerproto.Bounce) {
	n.bounceLocal(SessionID(b.Session), b.Msg, b.Reason)
}

// route hands a message toward its target. done runs once the message is
// handed off, held or dropped.
func (n *Node) route(m *message, done func()) {
	if n.now().After(m.deadline) {
		n.bounce(m, wire.BounceTTLExpired)
		done()
		return
	}
	k := m.tcap.Target
	if hq := n.held[k]; hq != nil && len(hq.msgs) > 0 {
		n.hold(m)
		done()
		return
	}
	if len(n.byKey[k]) > 0 {
		n.deliverLocal(m)
		done()
		return
	}
	n.locate(k, func(st presence.State, owners []ids.BrokerID) {
		if len(n.byKey[k]) > 0 {
			n.deliverLocal(m)
			done()
			return
		}
		switch st {
		case presence.Present:
			n.tryForward(m, n.candidates(m, owners), func() { n.hold(m); done() }, func(retry bool) {
				if retry {
					n.route(m, done)
					return
				}
				done()
			})
		case presence.Gone:
			n.bounce(m, wire.BounceTargetGone)
			done()
		default:
			var hints []ids.BrokerID
			if st == presence.Unknown {
				for _, h := range m.tcap.Hints {
					if b, ok := n.view.byShort[h]; ok && b != n.id {
						hints = append(hints, b)
					}
				}
			}
			n.tryForward(m, n.candidates(m, hints), func() { n.hold(m); done() }, func(retry bool) {
				if retry {
					n.hold(m)
				}
				done()
			})
		}
	})
}

// candidates orders Brokers: non-suspect first, then by rendezvous hash of
// (cap ID, Broker ID) so that one cap's messages follow one path.
func (n *Node) candidates(m *message, bs []ids.BrokerID) []ids.BrokerID {
	type c struct {
		b   ids.BrokerID
		sus bool
		h   uint64
	}
	var cs []c
	for _, b := range bs {
		if b == n.id || n.view.members[b] == nil {
			continue
		}
		var buf [48]byte
		copy(buf[:16], m.target[:])
		copy(buf[16:], b[:])
		sum := sha256.Sum256(buf[:])
		cs = append(cs, c{b, n.isSuspect(b), binary.BigEndian.Uint64(sum[:8])})
	}
	sort.SliceStable(cs, func(i, j int) bool {
		if cs[i].sus != cs[j].sus {
			return !cs[i].sus
		}
		return cs[i].h > cs[j].h
	})
	out := make([]ids.BrokerID, len(cs))
	for i, x := range cs {
		out[i] = x.b
	}
	return out
}

func (n *Node) forwardEnv(m *message) *peerproto.Envelope {
	return &peerproto.Envelope{Forward: &peerproto.Forward{
		Msg:            m.id,
		TTLms:          ms(m.deadline.Sub(n.now())),
		Hop:            uint8(m.hop + 1),
		Reroutes:       uint8(m.reroutes),
		Target:         peerproto.CapRef{ID: m.target, Record: m.targetRec},
		Payload:        m.payload,
		Carried:        m.carried,
		ReplyID:        m.replyID,
		ReplySchema:    m.replySchema,
		ReplyBase:      m.replyBase,
		ReplyAuto:      m.replyAuto,
		Ingress:        m.ingress,
		IngressSession: m.ingressSession,
		Sender:         m.sender,
		SenderHints:    m.senderHints,
	}}
}

// tryForward offers the message to each candidate in turn (design §9.2
// step 8). noCand runs if no candidate took it and none answered not_here.
// end(retry) runs otherwise: retry means a candidate answered not_here and
// the caller should re-locate (bounded by max re-routes).
func (n *Node) tryForward(m *message, cands []ids.BrokerID, noCand func(), end func(retry bool)) {
	notHere := false
	var try func(i int)
	try = func(i int) {
		if i >= len(cands) {
			if !notHere {
				noCand()
				return
			}
			m.reroutes++
			if m.reroutes > n.tun.MaxReroutes {
				n.bounce(m, wire.BounceUnavailable)
				end(false)
				return
			}
			end(true)
			return
		}
		b := cands[i]
		if n.isSuspect(b) {
			try(i + 1)
			return
		}
		n.call(b, n.forwardEnv(m), func(resp *peerproto.Envelope, err error) {
			switch {
			case err == errNotSent:
				try(i + 1)
			case err != nil || resp.ForwardResult == nil:
				// Written but unanswered: the outcome is unknown, so the
				// message is dropped, never re-sent (at most once).
				n.stats.Dropped++
				end(false)
			case resp.ForwardResult.Code == peerproto.FwdAccepted:
				n.stats.Forwarded++
				end(false)
			case resp.ForwardResult.Code == peerproto.FwdNotHere:
				delete(n.locCache, m.tcap.Target)
				notHere = true
				try(i + 1)
			default:
				n.bounce(m, resp.ForwardResult.Reason)
				end(false)
			}
		})
	}
	try(0)
}

// --- egress ---

func (n *Node) handleForward(from ids.BrokerID, f *peerproto.Forward) *peerproto.ForwardResult {
	c, err := caprec.Decode(f.Target.Record)
	if err != nil {
		return &peerproto.ForwardResult{Code: peerproto.FwdRejected, Reason: wire.BounceUnavailable}
	}
	m := &message{
		id:             f.Msg,
		deadline:       n.now().Add(min(fromMs(f.TTLms), n.tun.MaxMsgTTL)),
		hop:            int(f.Hop),
		reroutes:       int(f.Reroutes),
		target:         f.Target.ID,
		targetRec:      f.Target.Record,
		tcap:           c,
		payload:        f.Payload,
		carried:        f.Carried,
		replyID:        f.ReplyID,
		replySchema:    f.ReplySchema,
		replyBase:      f.ReplyBase,
		replyAuto:      f.ReplyAuto,
		ingress:        f.Ingress,
		ingressSession: f.IngressSession,
		sender:         f.Sender,
		senderHints:    f.SenderHints,
	}
	k := c.Target
	if len(n.byKey[k]) > 0 {
		n.deliverLocal(m)
		return &peerproto.ForwardResult{Code: peerproto.FwdAccepted}
	}
	if m.hop > 4 {
		return &peerproto.ForwardResult{Code: peerproto.FwdNotHere}
	}
	if hq := n.held[k]; hq != nil && len(hq.msgs) > 0 {
		n.hold(m)
		return &peerproto.ForwardResult{Code: peerproto.FwdAccepted}
	}
	if o := n.own[k]; o != nil && o.sessions == 0 && n.now().Sub(o.absentSince) <= n.tun.CapTTL {
		// The target disconnected from here recently: hold and watch.
		n.hold(m)
		return &peerproto.ForwardResult{Code: peerproto.FwdAccepted}
	}
	// Read repair: re-push our own register so stale views correct.
	if n.own[k] != nil {
		n.pushPresence(k)
	}
	return &peerproto.ForwardResult{Code: peerproto.FwdNotHere}
}

// deliverLocal creates the receiver's caps and writes deliver to the
// target's newest session (design §9.3).
func (n *Node) deliverLocal(m *message) {
	list := n.byKey[m.tcap.Target]
	s := list[len(list)-1]
	if len(m.payload) > s.maxMsg {
		n.bounce(m, wire.BounceTooLarge)
		return
	}
	if n.now().After(m.deadline) {
		n.bounce(m, wire.BounceTTLExpired)
		return
	}
	now := n.env.Wall().UnixMilli()
	d := wire.Deliver{Op: "deliver", Msg: m.id.String(), On: m.tcap.Lineage.String(), Payload: m.payload}
	for _, ref := range m.carried {
		rc, err := caprec.Decode(ref.Record)
		if err != nil {
			continue
		}
		var nid ids.CapID
		var ce *capErr
		n.createCap(rc.Derive(s.key, n.short, now), false, func(id ids.CapID, e *capErr) { nid, ce = id, e })
		if ce != nil {
			n.bounce(m, wire.BounceUnavailable)
			return
		}
		d.Caps = append(d.Caps, nid.String())
	}
	if m.tcap.AutoReply && m.replyID != nil {
		rc := &caprec.Record{
			Lineage:   *m.replyID,
			Kind:      caprec.KindReply,
			SingleUse: true,
			Target:    m.sender,
			Holder:    s.key,
			Issuer:    n.short,
			MintedAt:  now,
			Schema:    m.replySchema,
			Base:      m.replyBase,
			AutoReply: m.replyAuto,
			Hints:     m.senderHints,
		}
		var rid ids.CapID
		var ce *capErr
		n.createCap(rc, false, func(id ids.CapID, e *capErr) { rid, ce = id, e })
		if ce != nil {
			n.bounce(m, wire.BounceUnavailable)
			return
		}
		n.stats.Replies++
		d.Reply = rid.String()
	}
	if !n.sendFrame(s.id, d) {
		n.bounce(m, wire.BounceReceiverBusy)
		return
	}
	n.stats.Delivered++
}

// --- held messages (design §9.4) ---

func (n *Node) hold(m *message) {
	k := m.tcap.Target
	hq := n.held[k]
	if hq == nil {
		hq = &holdQueue{key: k, nextBackstop: n.now().Add(n.tun.HoldBackstop)}
		n.held[k] = hq
	}
	if len(hq.msgs) >= n.lim.HeldPerTarget {
		n.bounce(m, wire.BounceReceiverBusy)
		return
	}
	hq.msgs = append(hq.msgs, m)
	n.stats.Held++
	if n.now().After(hq.watchUntil) {
		n.watch(hq)
	}
}

func (n *Node) watch(hq *holdQueue) {
	hq.watchUntil = n.now().Add(n.tun.MaxMsgTTL)
	k := hq.key
	for _, t := range n.view.placementSet(k.PlacementKey(), n.tun.Replicas) {
		if n.isSuspect(t) {
			continue
		}
		n.call(t, &peerproto.Envelope{Watch: &peerproto.Watch{Key: k, TTLms: ms(n.tun.MaxMsgTTL)}}, func(resp *peerproto.Envelope, err error) {
			if err != nil || resp.WatchResult == nil {
				return
			}
			n.onPresenceHint(k, presence.FromWire(resp.WatchResult.Regs, n.now(), n.tun.PresenceTombstoneTTL))
		})
	}
}

// onPresenceHint re-routes held messages when a watch reports the target
// present. Partial answers never declare a target gone; the backstop lookup
// does that.
func (n *Node) onPresenceHint(k ids.Key, s presence.Set) {
	hq := n.held[k]
	if hq == nil || len(hq.msgs) == 0 {
		return
	}
	if len(n.byKey[k]) > 0 {
		n.flushHeldLocal(k)
		return
	}
	if st, owners := n.classify(s); st == presence.Present {
		n.flushHeld(hq, owners)
	}
}

func (n *Node) flushHeldLocal(k ids.Key) {
	hq := n.held[k]
	if hq == nil {
		return
	}
	msgs := hq.msgs
	hq.msgs = nil
	delete(n.held, k)
	for _, m := range msgs {
		if len(n.byKey[k]) == 0 {
			n.hold(m)
			continue
		}
		n.deliverLocal(m)
	}
}

// flushHeld forwards all held messages at once, in order, each to the first
// non-suspect candidate. Forwards to one Broker share an ordered link, and
// the queue is released before any reply, so later messages queue behind
// these on the link instead of overtaking them. A message that is not taken
// is held again.
func (n *Node) flushHeld(hq *holdQueue, owners []ids.BrokerID) {
	k := hq.key
	if n.held[k] != hq {
		return
	}
	if len(n.byKey[k]) > 0 {
		n.flushHeldLocal(k)
		return
	}
	// Messages with no reachable candidate stay held, in order, in the same
	// queue (keeping its watch); the rest go out now.
	type out struct {
		m  *message
		to ids.BrokerID
	}
	var send []out
	keep := hq.msgs[:0]
	for _, m := range hq.msgs {
		var to ids.BrokerID
		for _, c := range n.candidates(m, owners) {
			if !n.isSuspect(c) {
				to = c
				break
			}
		}
		if to.IsZero() {
			keep = append(keep, m)
			continue
		}
		send = append(send, out{m, to})
	}
	hq.msgs = keep
	if len(hq.msgs) == 0 {
		delete(n.held, k)
	}
	for _, o := range send {
		m, to := o.m, o.to
		m.hop++
		n.call(to, n.forwardEnv(m), func(resp *peerproto.Envelope, err error) {
			switch {
			case err == errNotSent:
				n.hold(m)
			case err != nil || resp.ForwardResult == nil:
				n.stats.Dropped++ // written but unanswered: at most once
			case resp.ForwardResult.Code == peerproto.FwdAccepted:
				n.stats.Forwarded++
			case resp.ForwardResult.Code == peerproto.FwdNotHere:
				delete(n.locCache, k)
				m.reroutes++
				if m.reroutes > n.tun.MaxReroutes {
					n.bounce(m, wire.BounceUnavailable)
					return
				}
				n.hold(m)
			default:
				n.bounce(m, resp.ForwardResult.Reason)
			}
		})
	}
}

// kickHeld re-evaluates every held message at once, for example when a
// suspected Broker answers again.
func (n *Node) kickHeld() {
	now := n.now()
	for _, hq := range n.held {
		hq.nextBackstop = now
	}
	n.after(0, n.sweepHeld)
}

func (n *Node) sweepHeld() {
	now := n.now()
	for _, k := range sortedKeys(n.held) {
		hq := n.held[k]
		keep := hq.msgs[:0]
		for _, m := range hq.msgs {
			if now.After(m.deadline) {
				n.bounce(m, wire.BounceTTLExpired)
				continue
			}
			keep = append(keep, m)
		}
		hq.msgs = keep
		if len(hq.msgs) == 0 {
			delete(n.held, k)
			continue
		}
		if len(n.byKey[k]) > 0 {
			n.flushHeldLocal(k)
			continue
		}
		if now.After(hq.watchUntil) {
			n.watch(hq)
		}
		if now.Before(hq.nextBackstop) {
			continue
		}
		hq.nextBackstop = now.Add(n.tun.HoldBackstop)
		n.lookupPresence(k, func(s presence.Set) {
			if n.held[k] != hq {
				return
			}
			st, owners := n.classify(s)
			switch st {
			case presence.Present:
				n.flushHeld(hq, owners)
			case presence.Gone:
				msgs := hq.msgs
				delete(n.held, k)
				for _, m := range msgs {
					n.bounce(m, wire.BounceTargetGone)
				}
			}
		})
	}
}

// HeldMessages returns the number of messages held on this Broker.
func (n *Node) HeldMessages() int {
	c := 0
	for _, hq := range n.held {
		c += len(hq.msgs)
	}
	return c
}
