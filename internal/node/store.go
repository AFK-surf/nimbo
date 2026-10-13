package node

import (
	"bytes"
	"container/list"
	"crypto/sha256"
	"sort"
	"time"

	"github.com/afk-surf/nimbo/internal/caprec"
	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/internal/peerproto"
)

// RCR store for cap and blob records (design §7).

type recKey struct {
	kind peerproto.RecKind
	id   [32]byte
}

func capKey(id ids.CapID) recKey {
	k := recKey{kind: peerproto.KindCap}
	copy(k.id[:], id[:])
	return k
}

func blobKey(h ids.Hash) recKey { return recKey{kind: peerproto.KindBlob, id: h} }

func presKey(k ids.Key) recKey { return recKey{kind: peerproto.KindPresence, id: k} }

func (k recKey) capID() ids.CapID {
	var c ids.CapID
	copy(c[:], k.id[:16])
	return c
}

func (k recKey) pkey() ids.Hash {
	switch k.kind {
	case peerproto.KindCap:
		return ids.Hash(sha256.Sum256(k.id[:16]))
	case peerproto.KindPresence:
		return ids.Hash(sha256.Sum256(k.id[:]))
	}
	return ids.Hash(k.id)
}

func keyFromWire(kind peerproto.RecKind, id []byte) (recKey, bool) {
	k := recKey{kind: kind}
	switch kind {
	case peerproto.KindCap:
		if len(id) != 16 {
			return k, false
		}
	case peerproto.KindBlob, peerproto.KindPresence:
		if len(id) != 32 {
			return k, false
		}
	default:
		return k, false
	}
	copy(k.id[:], id)
	return k, true
}

const (
	fHolder uint8 = 1 << iota // member of the placement set (or single-use issuer)
	fHinted                   // hinted copy for an unreachable member
	fIssuer                   // pinned copy on the creating Broker
)

type record struct {
	key      recKey
	pkey     ids.Hash
	data     []byte
	blobKind peerproto.BlobKind
	cap      *caprec.Record
	deadline time.Time // local
	touched  time.Time // deadline last propagated to the placement set
	// origin stamps the use that set deadline. Relays carry the deadline as
	// a relative TTL, so each hop adds its in-flight time; a relay of the
	// same use must not extend it again, or records ping-ponging between
	// Brokers outlive their last use.
	origin   uint64
	consumed bool
	flags    uint8
	hintFor  ids.BrokerID
	elem     *list.Element // LRU element while flags == 0
}

func (r *record) idBytes() []byte {
	if r.key.kind == peerproto.KindCap {
		return append([]byte(nil), r.key.id[:16]...)
	}
	return append([]byte(nil), r.key.id[:]...)
}

func (r *record) singleUse() bool { return r.cap != nil && r.cap.SingleUse }

// expired applies the soft-expiry acceptance rule: a record is accepted until
// grace past its deadline (design §7.5).
func (n *Node) expired(r *record) bool { return n.now().After(r.deadline.Add(n.tun.Grace)) }

// pastRetention reports whether holders may drop the record.
func (n *Node) pastRetention(r *record) bool {
	return n.now().After(r.deadline.Add(n.tun.Grace + n.tun.TouchThreshold))
}

func (n *Node) remaining(r *record) time.Duration { return r.deadline.Sub(n.now()) }

func (n *Node) setFlags(r *record, f uint8) {
	if r.flags == 0 && f != 0 && r.elem != nil {
		n.lru.Remove(r.elem)
		r.elem = nil
	}
	r.flags = f
	if f == 0 && r.elem == nil {
		r.elem = n.lru.PushFront(r)
		n.evictLRU()
	}
}

func (n *Node) lruTouch(r *record) {
	if r.elem != nil {
		n.lru.MoveToFront(r.elem)
	}
}

func (n *Node) evictLRU() {
	for n.lru.Len() > n.tun.CacheEntries {
		e := n.lru.Back()
		r := e.Value.(*record)
		n.lru.Remove(e)
		r.elem = nil
		delete(n.recs, r.key)
	}
}

func (n *Node) dropRec(r *record) {
	if r.elem != nil {
		n.lru.Remove(r.elem)
		r.elem = nil
	}
	delete(n.recs, r.key)
}

// putNew installs a record created on this Broker.
func (n *Node) putNew(r *record, flags uint8) {
	r.pkey = r.key.pkey()
	n.recs[r.key] = r
	r.elem = n.lru.PushFront(r)
	n.setFlags(r, flags)
}

func (n *Node) wireRec(r *record) *peerproto.Rec {
	return &peerproto.Rec{
		Kind:     r.key.kind,
		ID:       r.idBytes(),
		Data:     r.data,
		BlobKind: r.blobKind,
		TTLms:    ms(n.remaining(r)),
		Consumed: r.consumed,
		Origin:   r.origin,
	}
}

// mergeWire merges a received record into the local store with the merge
// rules of design §7.2 and returns it, or nil if it is invalid.
func (n *Node) mergeWire(w *peerproto.Rec) *record {
	rk, ok := keyFromWire(w.Kind, w.ID)
	if !ok || rk.kind == peerproto.KindPresence {
		return nil
	}
	ttl := min(fromMs(w.TTLms), n.tun.CapTTL)
	dl := n.now().Add(ttl)
	r := n.recs[rk]
	if r == nil {
		r = &record{key: rk, data: w.Data, blobKind: w.BlobKind, deadline: dl, touched: dl, origin: w.Origin, consumed: w.Consumed}
		switch rk.kind {
		case peerproto.KindCap:
			c, err := caprec.Decode(w.Data)
			if err != nil {
				return nil
			}
			r.cap = c
		case peerproto.KindBlob:
			if ids.Sum(w.Data) != ids.Hash(rk.id) {
				return nil
			}
		}
		n.putNew(r, 0)
		return r
	}
	n.extend(r, dl, w.Origin)
	r.consumed = r.consumed || w.Consumed
	n.lruTouch(r)
	return r
}

// extend applies a relayed deadline: it slides the record's deadline unless
// the relay is of the use that already set it.
func (n *Node) extend(r *record, dl time.Time, origin uint64) {
	if origin != 0 && origin == r.origin {
		return
	}
	if dl.After(r.deadline) {
		r.deadline, r.origin = dl, origin
	}
	if dl.After(r.touched) {
		r.touched = dl
	}
}

func (n *Node) newOrigin() uint64 { return ids.RandUint64(n.env.Rand()) | 1 }

func (n *Node) sweepRecords() {
	var drop []*record
	for _, r := range n.recs {
		if n.pastRetention(r) {
			drop = append(drop, r)
		}
	}
	for _, r := range drop {
		n.dropRec(r)
	}
	for k, t := range n.departed {
		if n.now().Sub(t) > n.tun.PresenceTombstoneTTL {
			delete(n.departed, k)
		}
	}
	for id, t := range n.fastJoinNeg {
		if n.now().After(t) {
			delete(n.fastJoinNeg, id)
		}
	}
}

func (n *Node) sortedRecKeys() []recKey {
	ks := make([]recKey, 0, len(n.recs))
	for k := range n.recs {
		ks = append(ks, k)
	}
	sort.Slice(ks, func(i, j int) bool {
		if ks[i].kind != ks[j].kind {
			return ks[i].kind < ks[j].kind
		}
		return bytes.Compare(ks[i].id[:], ks[j].id[:]) < 0
	})
	return ks
}

func contains(bs []ids.BrokerID, b ids.BrokerID) bool {
	for _, x := range bs {
		if x == b {
			return true
		}
	}
	return false
}

// newlyIn reports whether m belongs to newSet but did not hold the record
// under the old view: it was not in oldSet, or it restarted since.
func newlyIn(m ids.BrokerID, oldSet []ids.BrokerID, old, nv *view) bool {
	if !contains(oldSet, m) {
		return true
	}
	om, nm := old.members[m], nv.members[m]
	return om == nil || nm == nil || om.inc != nm.inc
}

// onViewChangeStore hands records off to Brokers that newly belong in their
// placement sets (design §7.4).
func (n *Node) onViewChangeStore(old, nv *view) {
	for _, rk := range n.sortedRecKeys() {
		r := n.recs[rk]
		if r.singleUse() || n.pastRetention(r) {
			continue
		}
		newSet := nv.placementSet(r.pkey, n.tun.Replicas)
		inNew := contains(newSet, n.id)
		owned := r.flags&(fHolder|fHinted|fIssuer) != 0
		if owned {
			oldSet := old.placementSet(r.pkey, n.tun.Replicas)
			for _, m := range newSet {
				if m != n.id && newlyIn(m, oldSet, old, nv) {
					n.enqueue(m, rk, modeStore, ids.BrokerID{}, nil)
				}
			}
		}
		f := r.flags &^ fHolder
		if inNew {
			f |= fHolder
		}
		if r.flags&fHinted != 0 && nv.members[r.hintFor] == nil {
			f &^= fHinted
		}
		if f != r.flags {
			n.setFlags(r, f)
		}
	}
}

// replicate sends a Store or Touch to the record's placement set, with
// hinted copies for suspect members. waiter, if set, is called once per
// remote member with the ack outcome.
func (n *Node) replicate(r *record, mode opMode, waiter func(bool)) int {
	if r.singleUse() {
		if mode == modeTouch {
			if is, ok := n.view.byShort[r.cap.Issuer]; ok && is != n.id {
				n.enqueue(is, r.key, modeTouch, ids.BrokerID{}, nil)
			}
		}
		return 0
	}
	remote := 0
	for _, m := range n.view.placementSet(r.pkey, n.tun.Replicas) {
		if m == n.id {
			if r.flags&fHolder == 0 {
				n.setFlags(r, r.flags|fHolder)
			}
			continue
		}
		remote++
		n.enqueue(m, r.key, mode, ids.BrokerID{}, waiter)
		if n.isSuspect(m) {
			n.writeHinted(r.key, m)
		}
	}
	return remote
}

func (n *Node) fallbackFor(pkey ids.Hash) ids.BrokerID {
	cands := n.view.placementSet(pkey, n.tun.Replicas+4)
	for i := n.tun.Replicas; i < len(cands); i++ {
		if !n.isSuspect(cands[i]) {
			return cands[i]
		}
	}
	return ids.BrokerID{}
}

// writeHinted stores a hinted copy for intended on the next-nearest
// non-suspect Broker and returns that Broker (zero if none).
func (n *Node) writeHinted(rk recKey, intended ids.BrokerID) ids.BrokerID {
	r := n.recs[rk]
	if r == nil {
		return ids.BrokerID{}
	}
	fb := n.fallbackFor(r.pkey)
	if fb.IsZero() {
		return fb
	}
	n.stats.HintedWrites++
	if fb == n.id {
		r.hintFor = intended
		n.setFlags(r, r.flags|fHinted)
		return fb
	}
	n.enqueue(fb, rk, modeStore, intended, nil)
	return fb
}

// useRec records a use: slide the local deadline and touch the placement
// set once the extension reaches touch_threshold (design §7.5).
func (n *Node) useRec(r *record) {
	dl := n.now().Add(n.tun.CapTTL)
	if dl.After(r.deadline) {
		r.deadline, r.origin = dl, n.newOrigin()
	}
	n.lruTouch(r)
	if r.deadline.Sub(r.touched) >= n.tun.TouchThreshold {
		r.touched = r.deadline
		n.replicate(r, modeTouch, nil)
	}
}

func (n *Node) handleRCR(from ids.BrokerID, b *peerproto.RCRBatch) *peerproto.RCRAck {
	res := make([]peerproto.OpResult, len(b.Ops))
	for i, op := range b.Ops {
		if n.fenced || n.left || n.leaving {
			res[i] = peerproto.OpRejected
			continue
		}
		switch {
		case op.Store != nil:
			r := n.mergeWire(op.Store)
			if r == nil {
				continue // invalid: ack so the sender stops retrying
			}
			if op.Hint != nil && *op.Hint != n.id {
				r.hintFor = *op.Hint
				n.setFlags(r, r.flags|fHinted)
				n.enqueue(*op.Hint, r.key, modeTouch, ids.BrokerID{}, nil)
			} else {
				n.setFlags(r, r.flags|fHolder)
			}
		case op.Touch != nil:
			rk, ok := keyFromWire(op.Touch.Kind, op.Touch.ID)
			if !ok {
				continue
			}
			r := n.recs[rk]
			if r == nil {
				res[i] = peerproto.OpMissing
				continue
			}
			n.extend(r, n.now().Add(min(fromMs(op.Touch.TTLms), n.tun.CapTTL)), op.Touch.Origin)
			if op.Hint != nil && *op.Hint != n.id {
				r.hintFor = *op.Hint
				n.setFlags(r, r.flags|fHinted)
				n.enqueue(*op.Hint, r.key, modeTouch, ids.BrokerID{}, nil)
			} else if r.flags&fHolder == 0 {
				n.setFlags(r, r.flags|fHolder)
			}
		case op.Presence != nil:
			n.mergePresenceWire(op.Presence.Key, op.Presence.Regs)
		}
	}
	return &peerproto.RCRAck{Results: res}
}

func (n *Node) handleGet(g *peerproto.Get) *peerproto.GetResult {
	rk, ok := keyFromWire(g.Kind, g.ID)
	if !ok {
		return &peerproto.GetResult{}
	}
	if rk.kind == peerproto.KindPresence {
		var k ids.Key
		copy(k[:], g.ID)
		regs := n.heldPresenceWire(k)
		return &peerproto.GetResult{Found: len(regs) > 0, Regs: regs}
	}
	r := n.recs[rk]
	if r == nil || n.pastRetention(r) {
		return &peerproto.GetResult{}
	}
	return &peerproto.GetResult{Found: true, Rec: n.wireRec(r)}
}

func (n *Node) handleConsume(c *peerproto.Consume) *peerproto.ConsumeResult {
	r := n.recs[capKey(c.ID)]
	if r == nil || n.pastRetention(r) {
		return &peerproto.ConsumeResult{Code: peerproto.ConsumeMissing}
	}
	if r.consumed {
		return &peerproto.ConsumeResult{Code: peerproto.ConsumeConsumed}
	}
	r.consumed = true
	return &peerproto.ConsumeResult{Code: peerproto.ConsumeOK}
}

// --- lookups (design §7.3) ---

type lookupOutcome int

const (
	lkFound lookupOutcome = iota
	lkNotFound
	lkUnresolved
)

// lookupTargets are the lookup_fanout nearest live Brokers (draining ones
// included) plus, within handoff_window of a view change, the previous
// placement set. Suspect Brokers are skipped.
func (n *Node) lookupTargets(pk ids.Hash) []ids.BrokerID {
	ts := n.view.nearestAll(pk, n.tun.LookupFanout)
	if n.prevView != nil && n.now().Sub(n.viewChangedAt) < n.tun.HandoffWindow {
		for _, b := range n.prevView.placementSet(pk, n.tun.Replicas) {
			if n.view.members[b] != nil && !contains(ts, b) {
				ts = append(ts, b)
			}
		}
	}
	out := ts[:0:0]
	for _, b := range ts {
		if !n.isSuspect(b) {
			out = append(out, b)
		}
	}
	return out
}

// positiveNotFound applies the not-found rule: at least 2 (or all, if fewer)
// members of the current placement set answered "not found" and none of
// them is suspect.
func (n *Node) positiveNotFound(pk ids.Hash, nf map[ids.BrokerID]bool) bool {
	pset := n.view.placementSet(pk, n.tun.Replicas)
	need := min(2, len(pset))
	c := 0
	for _, m := range pset {
		if n.isSuspect(m) {
			return false
		}
		if nf[m] {
			c++
		}
	}
	return c >= need && need > 0
}

// lookupRec finds a cap or blob record. With fresh, the local copy is
// bypassed and the placement set re-read (its answers are merged in).
func (n *Node) lookupRec(rk recKey, fresh bool, cb func(*record, lookupOutcome)) {
	if !fresh {
		if r := n.recs[rk]; r != nil && !n.pastRetention(r) {
			n.lruTouch(r)
			cb(r, lkFound)
			return
		}
	}
	if w, ok := n.lookups[rk]; ok {
		n.lookups[rk] = append(w, cb)
		return
	}
	n.lookups[rk] = []func(*record, lookupOutcome){cb}
	n.stats.LookupsRemote++
	start := n.now()
	pk := rk.pkey()
	complete := func(r *record, o lookupOutcome) {
		cbs := n.lookups[rk]
		delete(n.lookups, rk)
		if o == lkUnresolved {
			n.stats.LookupsUnresolved++
		}
		for _, f := range cbs {
			f(r, o)
		}
	}
	var attempt func()
	attempt = func() {
		targets := n.lookupTargets(pk)
		left := len(targets)
		done := false
		var best *peerproto.Rec
		nf := map[ids.BrokerID]bool{}
		decide := func() {
			done = true
			if best != nil {
				if r := n.mergeWire(best); r != nil {
					complete(r, lkFound)
					return
				}
			}
			pos := n.positiveNotFound(pk, nf)
			if n.now().Sub(start) < n.tun.LookupWait {
				n.after(n.tun.LookupRetry, attempt)
				return
			}
			if pos {
				complete(nil, lkNotFound)
			} else {
				complete(nil, lkUnresolved)
			}
		}
		if left == 0 {
			decide()
			return
		}
		for _, t := range targets {
			t := t
			n.call(t, &peerproto.Envelope{Get: &peerproto.Get{Kind: rk.kind, ID: rk.idBytes()}}, func(resp *peerproto.Envelope, err error) {
				left--
				if done {
					return
				}
				if err == nil && resp.GetResult != nil {
					g := resp.GetResult
					if g.Found && g.Rec != nil {
						if best == nil || g.Rec.TTLms > best.TTLms {
							c := *g.Rec
							c.Consumed = c.Consumed || (best != nil && best.Consumed)
							best = &c
						} else if g.Rec.Consumed {
							best.Consumed = true
						}
						if best.TTLms > -ms(n.tun.Grace) {
							decide()
							return
						}
					} else {
						nf[t] = true
					}
				}
				if left == 0 {
					decide()
				}
			})
		}
	}
	attempt()
}

func (k recKey) idBytes() []byte {
	if k.kind == peerproto.KindCap {
		return append([]byte(nil), k.id[:16]...)
	}
	return append([]byte(nil), k.id[:]...)
}

// --- introspection for tests and the simulator ---

// RecInfo describes a held record.
type RecInfo struct {
	Kind      peerproto.RecKind
	ID        [32]byte
	Holder    bool
	Hinted    bool
	Issuer    bool
	Remaining time.Duration
	Consumed  bool
}

// HeldCap reports this node's copy of a cap record, if any.
func (n *Node) HeldCap(id ids.CapID) (RecInfo, bool) {
	r := n.recs[capKey(id)]
	if r == nil {
		return RecInfo{}, false
	}
	return RecInfo{Kind: r.key.kind, ID: r.key.id, Holder: r.flags&fHolder != 0, Hinted: r.flags&fHinted != 0,
		Issuer: r.flags&fIssuer != 0, Remaining: n.remaining(r), Consumed: r.consumed}, true
}

// PlacementSet returns the current placement set for a cap ID.
func (n *Node) PlacementSet(id ids.CapID) []ids.BrokerID {
	return n.view.placementSet(id.PlacementKey(), n.tun.Replicas)
}

// PresencePlacement returns the current presence placement set for a key.
func (n *Node) PresencePlacement(k ids.Key) []ids.BrokerID {
	return n.view.placementSet(k.PlacementKey(), n.tun.Replicas)
}

// QueuedOps returns the number of queued replication ops.
func (n *Node) QueuedOps() int { return n.queuedOps() }
