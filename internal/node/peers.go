package node

import (
	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/internal/peerproto"
)

// Peer suspicion and acknowledged replication queues (design §5, §7.2).

type opMode uint8

const (
	modeTouch    opMode = 1
	modeStore    opMode = 2
	modePresence opMode = 3
)

type opKey struct {
	rk   recKey
	hint ids.BrokerID
}

type repOp struct {
	key     opKey
	mode    opMode
	waiters []func(bool)
}

type peer struct {
	id           ids.BrokerID
	suspectUntil timeT
	suspectFor   durationT
	q            map[opKey]*repOp
	order        []opKey
	inflight     int
	flushSched   bool
	probing      bool
}

func (n *Node) peer(id ids.BrokerID) *peer {
	p := n.peers[id]
	if p == nil {
		p = &peer{id: id, q: map[opKey]*repOp{}}
		n.peers[id] = p
	}
	return p
}

// isSuspect reports local suspicion of a peer. Self is never suspect.
func (n *Node) isSuspect(id ids.BrokerID) bool {
	p := n.peers[id]
	return p != nil && n.now().Before(p.suspectUntil)
}

func (n *Node) peerFailed(id ids.BrokerID) {
	if id == n.id || n.view.members[id] == nil {
		return
	}
	p := n.peer(id)
	now := n.now()
	if now.Before(p.suspectUntil) {
		return
	}
	if p.suspectFor == 0 {
		p.suspectFor = n.tun.SuspectBase
	} else {
		p.suspectFor = min(2*p.suspectFor, n.tun.SuspectMax)
	}
	p.suspectUntil = now.Add(p.suspectFor)
}

func (n *Node) peerOK(id ids.BrokerID) {
	p := n.peers[id]
	if p == nil {
		return
	}
	was := n.now().Before(p.suspectUntil)
	p.suspectFor = 0
	p.suspectUntil = timeT{}
	if len(p.q) > 0 {
		n.scheduleFlush(p)
	}
	if was {
		n.repushPresenceTo(id)
		n.kickHeld()
	}
}

func (n *Node) dropPeer(id ids.BrokerID) {
	p := n.peers[id]
	if p == nil {
		return
	}
	delete(n.peers, id)
	for _, k := range p.order {
		if op := p.q[k]; op != nil {
			delete(p.q, k)
			for _, w := range op.waiters {
				w(false)
			}
		}
	}
}

// queuedOps is the total number of queued replication ops.
func (n *Node) queuedOps() int {
	t := 0
	for _, p := range n.peers {
		t += len(p.q)
	}
	return t
}

// overloaded reports replication back-pressure (design §6.4).
func (n *Node) overloaded() bool {
	for _, p := range n.peers {
		if len(p.q) >= n.lim.MaxPeerQueue*9/10 {
			return true
		}
	}
	return false
}

func (n *Node) enqueue(to ids.BrokerID, rk recKey, mode opMode, hint ids.BrokerID, waiter func(bool)) {
	if to == n.id {
		if waiter != nil {
			waiter(true)
		}
		return
	}
	if n.view.members[to] == nil {
		if waiter != nil {
			waiter(false)
		}
		return
	}
	p := n.peer(to)
	k := opKey{rk: rk, hint: hint}
	if op := p.q[k]; op != nil {
		if mode > op.mode {
			op.mode = mode
		}
		if waiter != nil {
			op.waiters = append(op.waiters, waiter)
		}
		return
	}
	if len(p.q) >= n.lim.MaxPeerQueue {
		if waiter != nil {
			waiter(false)
		}
		return
	}
	op := &repOp{key: k, mode: mode}
	if waiter != nil {
		op.waiters = []func(bool){waiter}
	}
	p.q[k] = op
	p.order = append(p.order, k)
	n.scheduleFlush(p)
}

func (n *Node) requeue(p *peer, op *repOp) {
	if cur := p.q[op.key]; cur != nil {
		if op.mode > cur.mode {
			cur.mode = op.mode
		}
		cur.waiters = append(cur.waiters, op.waiters...)
		return
	}
	p.q[op.key] = op
	p.order = append(p.order, op.key)
}

// requeueFailed re-queues an op whose send failed and writes a hinted copy
// to the next-nearest non-suspect Broker.
func (n *Node) requeueFailed(p *peer, op *repOp) {
	n.requeue(p, op)
	if op.key.hint.IsZero() && op.key.rk.kind != peerproto.KindPresence {
		n.writeHinted(op.key.rk, p.id)
	}
}

func (n *Node) scheduleFlush(p *peer) {
	if p.flushSched {
		return
	}
	p.flushSched = true
	n.after(0, func() {
		p.flushSched = false
		if n.peers[p.id] == p {
			n.flush(p)
		}
	})
}

func (n *Node) flush(p *peer) {
	if len(p.q) == 0 || p.inflight >= n.tun.MaxInflight || n.isSuspect(p.id) {
		return
	}
	var ops []peerproto.Op
	var sent []*repOp
	i := 0
	for ; i < len(p.order) && len(ops) < n.tun.BatchOps; i++ {
		k := p.order[i]
		op := p.q[k]
		if op == nil {
			continue
		}
		delete(p.q, k)
		w, ok := n.buildOp(op)
		if !ok {
			for _, f := range op.waiters {
				f(false)
			}
			continue
		}
		ops = append(ops, w)
		sent = append(sent, op)
	}
	rest := p.order[i:]
	p.order = append(make([]opKey, 0, len(rest)), rest...)
	if len(ops) == 0 {
		return
	}
	p.inflight++
	n.call(p.id, &peerproto.Envelope{RCR: &peerproto.RCRBatch{Ops: ops}}, func(resp *peerproto.Envelope, err error) {
		if n.peers[p.id] != p {
			for _, op := range sent {
				for _, f := range op.waiters {
					f(false)
				}
			}
			return
		}
		p.inflight--
		if err != nil || resp.RCRAck == nil || len(resp.RCRAck.Results) != len(sent) {
			for _, op := range sent {
				n.requeueFailed(p, op)
			}
			return
		}
		rejected := false
		for i, op := range sent {
			switch resp.RCRAck.Results[i] {
			case peerproto.OpOK:
				for _, f := range op.waiters {
					f(true)
				}
			case peerproto.OpMissing:
				op.mode = modeStore
				n.requeue(p, op)
			default:
				rejected = true
				n.requeueFailed(p, op)
			}
		}
		if rejected {
			n.peerFailed(p.id)
		}
		if len(p.q) > 0 {
			n.scheduleFlush(p)
		}
	})
}

// buildOp renders a queued op from current local state, so retries always
// carry the latest TTL and registers.
func (n *Node) buildOp(op *repOp) (peerproto.Op, bool) {
	var w peerproto.Op
	if !op.key.hint.IsZero() {
		h := op.key.hint
		w.Hint = &h
	}
	if op.key.rk.kind == peerproto.KindPresence {
		var k ids.Key
		copy(k[:], op.key.rk.id[:])
		regs := n.presenceForPush(k)
		if len(regs) == 0 {
			return w, false
		}
		w.Presence = &peerproto.PresenceStore{Key: k, Regs: regs}
		return w, true
	}
	r := n.recs[op.key.rk]
	if r == nil || n.pastRetention(r) {
		return w, false
	}
	if op.mode == modeStore {
		w.Store = n.wireRec(r)
	} else {
		w.Touch = &peerproto.Touch{Kind: r.key.kind, ID: r.idBytes(), TTLms: ms(r.deadline.Sub(n.now())), Origin: r.origin}
	}
	return w, true
}

// probeGet is a lookup nobody stores; any answer proves the peer is back.
var probeGet = peerproto.Get{Kind: peerproto.KindCap, ID: make([]byte, 16)}

func (n *Node) sweepPeers() {
	for _, id := range sortedBrokerKeys(n.peers) {
		p := n.peers[id]
		// Probe suspected peers so that suspicion ends as soon as they
		// answer, not only when its timer runs out: messages to targets
		// connected only there are held meanwhile.
		if n.isSuspect(id) && !p.probing && n.view.members[id] != nil {
			p.probing = true
			g := probeGet
			n.call(id, &peerproto.Envelope{Get: &g}, func(*peerproto.Envelope, error) { p.probing = false })
		}
		if len(p.q) > 0 && p.inflight == 0 && !n.isSuspect(id) {
			n.flush(p)
		}
	}
}

// PeerSuspect reports whether this node currently suspects a peer.
func (n *Node) PeerSuspect(id ids.BrokerID) bool { return n.isSuspect(id) }
