package node

import (
	"time"

	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/internal/peerproto"
	"github.com/afk-surf/nimbo/internal/presence"
)

// Presence registers, owner re-push, watches and location (design §9.1, §9.4).

type presRec struct {
	regs     presence.Set
	watchers map[ids.BrokerID]time.Time
	// leaveAt is set once this Broker is no longer in the key's placement
	// set; the copy is kept briefly so the handoff push can read it.
	leaveAt time.Time
}

type ownReg struct {
	seq         uint64
	sessions    int
	absentSince time.Time
}

type locEntry struct {
	brokers []ids.BrokerID
	at      time.Time
}

func regKey(id ids.BrokerID, inc uint64) presence.RegKey { return presence.RegKey{Owner: id, Inc: inc} }

func (n *Node) validity(k presence.RegKey) presence.Validity {
	if k.Owner == n.id {
		if k.Inc == n.inc {
			return presence.Valid
		}
		return presence.Invalid
	}
	if _, ok := n.departed[k]; ok {
		return presence.Invalid
	}
	if m := n.view.members[k.Owner]; m != nil && m.inc == k.Inc {
		return presence.Valid
	}
	return presence.UnknownOwner
}

func tombstone(r presence.Reg, now time.Time) presence.Reg {
	t := presence.Reg{Seq: r.Seq, Tomb: true, AbsentSince: r.AbsentSince}
	if !r.Absent() {
		t.AbsentSince = now
	}
	return t
}

func (n *Node) ownSet(k ids.Key) presence.Set {
	o := n.own[k]
	if o == nil {
		return nil
	}
	return presence.Set{regKey(n.id, n.inc): presence.Reg{Seq: o.seq, Sessions: uint32(o.sessions), AbsentSince: o.absentSince}}
}

// sessionsChanged updates this Broker's own register for k and pushes it.
func (n *Node) sessionsChanged(k ids.Key, delta int) {
	o := n.own[k]
	if o == nil {
		o = &ownReg{}
		n.own[k] = o
	}
	o.sessions += delta
	n.presSeq++
	o.seq = n.presSeq
	if o.sessions == 0 {
		o.absentSince = n.now()
	} else {
		o.absentSince = time.Time{}
	}
	delete(n.locCache, k)
	n.pushPresence(k)
}

// pushPresence sends the full registers for k to its placement set.
func (n *Node) pushPresence(k ids.Key) {
	for _, m := range n.view.placementSet(k.PlacementKey(), n.tun.Replicas) {
		if m == n.id {
			if s := n.ownSet(k); s != nil {
				n.mergePresence(k, s)
			}
			continue
		}
		n.enqueue(m, presKey(k), modePresence, ids.BrokerID{}, nil)
	}
}

// presenceForPush is what a presence op carries: held registers plus this
// Broker's own.
func (n *Node) presenceForPush(k ids.Key) []presence.Wire {
	s := presence.Set{}
	if pr := n.pres[k]; pr != nil {
		s.MergeInto(pr.regs)
	}
	if o := n.ownSet(k); o != nil {
		s.MergeInto(o)
	}
	return presence.ToWire(s, n.now())
}

func (n *Node) heldPresenceWire(k ids.Key) []presence.Wire { return n.presenceForPush(k) }

func (n *Node) mergePresenceWire(k ids.Key, ws []presence.Wire) {
	n.mergePresence(k, presence.FromWire(ws, n.now(), n.tun.PresenceTombstoneTTL))
}

func (n *Node) mergePresence(k ids.Key, in presence.Set) {
	pr := n.pres[k]
	if pr == nil {
		pr = &presRec{regs: presence.Set{}, watchers: map[ids.BrokerID]time.Time{}}
		n.pres[k] = pr
	}
	now := n.now()
	for rk, r := range in {
		if !r.Tomb && n.validity(rk) == presence.Invalid {
			in[rk] = tombstone(r, now)
		}
	}
	if pr.regs.MergeInto(in) {
		n.notifyWatchers(k, pr)
	}
}

func (n *Node) notifyWatchers(k ids.Key, pr *presRec) {
	if len(pr.watchers) == 0 {
		return
	}
	now := n.now()
	regs := presence.ToWire(pr.regs, now)
	for _, w := range sortedBrokerKeys(pr.watchers) {
		if now.After(pr.watchers[w]) {
			delete(pr.watchers, w)
			continue
		}
		n.send(w, &peerproto.Envelope{Notify: &peerproto.Notify{Key: k, Regs: regs}})
	}
}

func (n *Node) onViewChangePresence(old, nv *view) {
	now := n.now()
	for _, k := range sortedKeys(n.pres) {
		pr := n.pres[k]
		changed := false
		for _, rk := range pr.regs.Keys() {
			r := pr.regs[rk]
			if !r.Tomb && n.validity(rk) == presence.Invalid {
				pr.regs[rk] = tombstone(r, now)
				changed = true
			}
		}
		if changed {
			n.notifyWatchers(k, pr)
		}
		pk := k.PlacementKey()
		newSet := nv.placementSet(pk, n.tun.Replicas)
		oldSet := old.placementSet(pk, n.tun.Replicas)
		for _, m := range newSet {
			if m != n.id && newlyIn(m, oldSet, old, nv) {
				n.enqueue(m, presKey(k), modePresence, ids.BrokerID{}, nil)
			}
		}
		if contains(newSet, n.id) {
			pr.leaveAt = time.Time{}
		} else if pr.leaveAt.IsZero() {
			pr.leaveAt = now.Add(3 * n.tun.HandoffWindow)
		}
	}
	for _, k := range sortedKeys(n.own) {
		pk := k.PlacementKey()
		newSet := nv.placementSet(pk, n.tun.Replicas)
		oldSet := old.placementSet(pk, n.tun.Replicas)
		for _, m := range newSet {
			if m == n.id {
				if newlyIn(m, oldSet, old, nv) {
					n.mergePresence(k, n.ownSet(k))
				}
				continue
			}
			if newlyIn(m, oldSet, old, nv) {
				n.enqueue(m, presKey(k), modePresence, ids.BrokerID{}, nil)
			}
		}
	}
	n.locCache = map[ids.Key]locEntry{}
}

// repushAllPresence re-sends every own register with a new sequence
// number, so it also overrides tombstones written while this Broker was
// wrongly considered dead.
func (n *Node) repushAllPresence() {
	for _, k := range sortedKeys(n.own) {
		n.presSeq++
		n.own[k].seq = n.presSeq
		n.pushPresence(k)
	}
}

func (n *Node) reconcilePresence() { n.repushAllPresence() }

// repushPresenceTo re-pushes own registers held by a peer whose link just
// recovered.
func (n *Node) repushPresenceTo(id ids.BrokerID) {
	for _, k := range sortedKeys(n.own) {
		if contains(n.view.placementSet(k.PlacementKey(), n.tun.Replicas), id) {
			n.enqueue(id, presKey(k), modePresence, ids.BrokerID{}, nil)
		}
	}
}

func (n *Node) sweepPresence() {
	now := n.now()
	ttl := n.tun.PresenceTombstoneTTL
	for _, k := range sortedKeys(n.pres) {
		pr := n.pres[k]
		for rk, r := range pr.regs {
			if r.Absent() && now.Sub(r.AbsentSince) > ttl {
				delete(pr.regs, rk)
			}
		}
		for w, t := range pr.watchers {
			if now.After(t) {
				delete(pr.watchers, w)
			}
		}
		if (!pr.leaveAt.IsZero() && now.After(pr.leaveAt)) || (len(pr.regs) == 0 && len(pr.watchers) == 0) {
			delete(n.pres, k)
		}
	}
	for k, o := range n.own {
		if o.sessions == 0 && now.Sub(o.absentSince) > ttl {
			delete(n.own, k)
		}
	}
	for k, e := range n.locCache {
		if now.Sub(e.at) > n.tun.LocationCacheTTL {
			delete(n.locCache, k)
		}
	}
}

func (n *Node) handleWatch(from ids.BrokerID, w *peerproto.Watch) *peerproto.WatchResult {
	pr := n.pres[w.Key]
	if pr == nil {
		pr = &presRec{regs: presence.Set{}, watchers: map[ids.BrokerID]time.Time{}}
		if !contains(n.view.placementSet(w.Key.PlacementKey(), n.tun.Replicas), n.id) {
			pr.leaveAt = n.now().Add(n.tun.MaxMsgTTL)
		}
		n.pres[w.Key] = pr
	}
	ttl := min(fromMs(w.TTLms), n.tun.MaxMsgTTL)
	if until := n.now().Add(ttl); until.After(pr.watchers[from]) {
		pr.watchers[from] = until
	}
	return &peerproto.WatchResult{Regs: n.presenceForPush(w.Key)}
}

func (n *Node) handleNotify(nt *peerproto.Notify) {
	n.onPresenceHint(nt.Key, presence.FromWire(nt.Regs, n.now(), n.tun.PresenceTombstoneTTL))
}

// lookupPresence queries the presence placement set and merges all answers,
// including this Broker's own register.
func (n *Node) lookupPresence(k ids.Key, cb func(presence.Set)) {
	targets := n.lookupTargets(k.PlacementKey())
	merged := presence.Set{}
	finish := func() {
		if o := n.ownSet(k); o != nil {
			merged.MergeInto(o)
		}
		cb(merged)
	}
	left := len(targets)
	if left == 0 {
		finish()
		return
	}
	for _, t := range targets {
		n.call(t, &peerproto.Envelope{Get: &peerproto.Get{Kind: peerproto.KindPresence, ID: append([]byte(nil), k[:]...)}}, func(resp *peerproto.Envelope, err error) {
			left--
			if err == nil && resp.GetResult != nil {
				merged.MergeInto(presence.FromWire(resp.GetResult.Regs, n.now(), n.tun.PresenceTombstoneTTL))
			}
			if left == 0 {
				finish()
			}
		})
	}
}

func (n *Node) classify(s presence.Set) (presence.State, []ids.BrokerID) {
	return presence.Classify(s, n.now(), n.tun.CapTTL, n.validity)
}

// locate returns the target's state and the Brokers it is connected to,
// using the location cache first (design §9.2 step 6).
func (n *Node) locate(k ids.Key, cb func(presence.State, []ids.BrokerID)) {
	if e, ok := n.locCache[k]; ok && n.now().Sub(e.at) < n.tun.LocationCacheTTL {
		cb(presence.Present, e.brokers)
		return
	}
	n.lookupPresence(k, func(s presence.Set) {
		st, owners := n.classify(s)
		if n.cfg.Debug && st != presence.Present {
			for _, rk := range s.Keys() {
				r := s[rk]
				n.debugf("locate %s: %s owner=%s inc=%x seq=%d sessions=%d tomb=%v absent=%v validity=%d",
					k.String()[:8], st, rk.Owner.Abbrev(), rk.Inc, r.Seq, r.Sessions, r.Tomb, n.now().Sub(r.AbsentSince).Round(time.Millisecond), n.validity(rk))
			}
		}
		if st == presence.Present {
			n.locCache[k] = locEntry{brokers: owners, at: n.now()}
		}
		cb(st, owners)
	})
}

// OwnSessions reports how many sessions this Broker's own register for k
// counts.
func (n *Node) OwnSessions(k ids.Key) int {
	if o := n.own[k]; o != nil {
		return o.sessions
	}
	return 0
}

// HeldPresence reports whether this Broker holds a live register for k from
// owner with the given incarnation.
func (n *Node) HeldPresence(k ids.Key, owner ids.BrokerID, inc uint64) (sessions uint32, ok bool) {
	pr := n.pres[k]
	if pr == nil {
		return 0, false
	}
	r, ok := pr.regs[regKey(owner, inc)]
	if !ok || r.Tomb {
		return 0, false
	}
	return r.Sessions, true
}
