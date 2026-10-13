package node

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/internal/peerproto"
	"github.com/afk-surf/nimbo/internal/placement"
	"github.com/afk-surf/nimbo/pkg/wire"
)

// Membership through S3 (design §4).

type member struct {
	id       ids.BrokerID
	info     MemberInfo
	inc      uint64
	draining bool
	lastSeen time.Time // S3 time
	// learned orders membership knowledge on this Broker: it is taken from
	// a counter when the information is obtained (a Rejoin or frame is
	// received, or the S3 request that returned it is issued).
	learned uint64
}

type view struct {
	members map[ids.BrokerID]*member
	idx     *placement.Index
	byShort map[ids.Short]ids.BrokerID
	n       int
}

func newView(tun Tunables, members map[ids.BrokerID]*member) *view {
	v := &view{members: members, byShort: map[ids.Short]ids.BrokerID{}, n: tun.VirtualIDs}
	bs := make([]ids.BrokerID, 0, len(members))
	for id := range members {
		bs = append(bs, id)
		v.byShort[id.Short()] = id
	}
	v.idx = placement.New(bs, tun.VirtualIDs)
	return v
}

// placementSet returns the k nearest live, non-draining members.
func (v *view) placementSet(key ids.Hash, k int) []ids.BrokerID {
	return v.idx.Nearest(key, k, func(b ids.BrokerID) bool { return !v.members[b].draining })
}

// nearestAll returns the k nearest live members, draining ones included.
func (v *view) nearestAll(key ids.Hash, k int) []ids.BrokerID {
	return v.idx.Nearest(key, k, nil)
}

func memberKey(id ids.BrokerID) string { return "members/" + id.String() + ".json" }

func parseMemberKey(k string) (ids.BrokerID, bool) {
	s, ok := strings.CutPrefix(k, "members/")
	if !ok {
		return ids.BrokerID{}, false
	}
	s, ok = strings.CutSuffix(s, ".json")
	if !ok {
		return ids.BrokerID{}, false
	}
	id, err := ids.ParseBrokerID(s)
	return id, err == nil
}

func (n *Node) memberInfo() MemberInfo {
	mi := n.cfg.Member
	mi.NodePub = hex.EncodeToString(n.cfg.NodePub)
	mi.Incarnation = strconv.FormatUint(n.inc, 16)
	mi.Draining = n.draining
	if mi.Version == "" {
		mi.Version = "nimbo/1"
	}
	return mi
}

func (n *Node) memberJSON() []byte {
	b, err := json.Marshal(n.memberInfo())
	if err != nil {
		panic(err)
	}
	return b
}

func (n *Node) selfMember() *member {
	return &member{id: n.id, info: n.memberInfo(), inc: n.inc, draining: n.draining, lastSeen: n.s3Now(), learned: n.learn()}
}

// parseMember validates a member object against the Broker ID it is filed
// under.
func parseMember(id ids.BrokerID, b []byte) (*member, bool) {
	var mi MemberInfo
	if err := json.Unmarshal(b, &mi); err != nil {
		return nil, false
	}
	pub, err := hex.DecodeString(mi.NodePub)
	if err != nil || ids.BrokerIDFromPub(pub) != id {
		return nil, false
	}
	inc, err := strconv.ParseUint(mi.Incarnation, 16, 64)
	if err != nil || inc == 0 {
		return nil, false
	}
	return &member{id: id, info: mi, inc: inc, draining: mi.Draining}, true
}

// s3Now is the local estimate of S3 time.
func (n *Node) s3Now() time.Time { return n.env.Wall().Add(n.s3Offset) }

func (n *Node) observeDate(start time.Time, date time.Time) {
	if date.IsZero() {
		return
	}
	end := n.env.Wall()
	mid := start.Add(end.Sub(start) / 2)
	// Date has one-second resolution; the true time is in [Date, Date+1s).
	sample := date.Add(500 * time.Millisecond).Sub(mid)
	// The offset is the median of the last 5 samples: robust to one slow
	// response, yet it follows a clock step within 3 samples.
	n.offSamples = append(n.offSamples, sample)
	if len(n.offSamples) > 5 {
		n.offSamples = n.offSamples[1:]
	}
	sorted := append([]time.Duration(nil), n.offSamples...)
	slices.Sort(sorted)
	n.s3Offset, n.s3OffsetOK = sorted[len(sorted)/2], true
}

// S3Offset returns the estimated offset of S3 time from the local clock.
func (n *Node) S3Offset() time.Duration { return n.s3Offset }

func (n *Node) heartbeat() {
	if n.left || n.leaving || n.stopped {
		return
	}
	if n.hbInFlight {
		// Its body may predate a change: write again when it completes.
		n.hbAgain = true
		return
	}
	n.hbInFlight, n.hbAgain = true, false
	start := n.env.Wall()
	body := n.memberJSON()
	n.env.S3(S3Request{Op: S3Put, Key: memberKey(n.id), Body: body}, func(r S3Response) {
		n.hbInFlight = false
		if n.stopped || n.left || n.leaving {
			return
		}
		n.observeDate(start, r.Date)
		if r.Err != nil {
			n.after(2*time.Second, n.heartbeat)
			return
		}
		if !bytes.Equal(body, n.lastPut) {
			// Joined, restarted or changed its info: tell the others.
			n.lastPut = body
			n.signal()
		}
		if n.hbAgain {
			n.after(0, n.heartbeat)
		}
		t := r.Date
		if t.IsZero() {
			t = start.Add(n.s3Offset)
		}
		if t.After(n.lastHB) {
			n.lastHB = t
		}
		n.checkFence()
		if !n.ready {
			n.poll()
		}
	})
}

// signalKey is the membership signal: a Broker rewrites it (with a random
// token, so that its ETag changes) after its member object changed, and
// every Broker re-reads membership when the ETag changes. Heartbeats that
// only refresh LastModified do not signal; a crashed Broker cannot, so the
// periodic poll still runs as a backstop.
const signalKey = "members.signal"

// signal rewrites the membership signal; writes requested while one is in
// flight coalesce into one more.
func (n *Node) signal() {
	n.signalWant = true
	if n.signalPutting || n.stopped {
		return
	}
	n.signalWant, n.signalPutting = false, true
	var tok [16]byte
	ids.RandBytes(n.env.Rand(), tok[:])
	n.env.S3(S3Request{Op: S3Put, Key: signalKey, Body: []byte(hex.EncodeToString(tok[:]))}, func(r S3Response) {
		n.signalPutting = false
		if n.stopped {
			return
		}
		if r.Err != nil {
			n.signalWant = true
			n.after(2*time.Second, n.signal)
			return
		}
		if n.signalWant {
			n.signal()
		}
	})
}

// pollSignal checks the membership signal and refreshes membership when it
// changed. The ETag of this Broker's own writes is not recorded as seen: a
// change another Broker signalled just before would otherwise be missed.
func (n *Node) pollSignal() {
	if n.signalGetting || !n.ready || n.left || n.stopped {
		return
	}
	n.signalGetting = true
	start := n.env.Wall()
	n.env.S3(S3Request{Op: S3Get, Key: signalKey}, func(r S3Response) {
		n.signalGetting = false
		if n.stopped || r.Err != nil {
			return
		}
		n.observeDate(start, r.Date)
		if r.ETag != n.signalETag {
			n.signalETag = r.ETag
			n.refresh()
		}
	})
}

// refresh runs a full membership poll now, or right after the one in
// flight (whose LIST may predate the change).
func (n *Node) refresh() {
	if n.pollInFlight {
		n.pollAgain = true
		return
	}
	n.poll()
}

func (n *Node) poll() {
	if n.pollInFlight || n.left || n.stopped || n.lastHB.IsZero() {
		return
	}
	n.pollInFlight, n.pollAgain = true, false
	start := n.env.Wall()
	listStart := n.learn()
	n.env.S3(S3Request{Op: S3List, Key: "members/"}, func(r S3Response) {
		if n.stopped {
			return
		}
		n.observeDate(start, r.Date)
		if r.Err != nil {
			// Freeze: keep the last view and expire nobody.
			n.pollInFlight = false
			if !n.ready || n.pollAgain {
				n.after(2*time.Second, n.poll)
			}
			return
		}
		next := map[ids.BrokerID]*member{}
		type need struct {
			id   ids.BrokerID
			etag string
			lm   time.Time
		}
		var needs []need
		s3now := n.s3Now()
		for _, o := range r.Objects {
			id, ok := parseMemberKey(o.Key)
			if !ok || id == n.id {
				continue
			}
			if s3now.Sub(o.LastModified) > n.tun.MemberTTL {
				continue
			}
			if cur := n.view.members[id]; cur != nil && n.knownETags[id] == o.ETag {
				m := *cur
				if o.LastModified.After(m.lastSeen) {
					m.lastSeen = o.LastModified
				}
				m.learned = max(m.learned, listStart)
				next[id] = &m
				continue
			}
			needs = append(needs, need{id, o.ETag, o.LastModified})
		}
		finish := func() {
			n.pollInFlight = false
			if n.pollAgain {
				n.after(0, n.poll)
			}
			// The LIST is a snapshot from listStart. What this Broker
			// learned since (a Rejoin, a fast join, a frame carrying a new
			// incarnation) is newer and wins; otherwise applying the
			// snapshot would drop a member that just joined, or roll back
			// a restart or a draining flag.
			for _, id := range sortedBrokerKeys(n.view.members) {
				cur := n.view.members[id]
				if id == n.id || cur.learned <= listStart {
					continue
				}
				if nx := next[id]; nx == nil || cur.learned > nx.learned {
					c := *cur
					next[id] = &c
				}
			}
			n.applyMembers(next)
			if !n.ready {
				n.ready = true
				n.logf("joined: %d members", len(n.view.members))
				n.announce()
				n.repushAllPresence()
			}
		}
		if len(needs) == 0 {
			finish()
			return
		}
		left := len(needs)
		for _, nd := range needs {
			nd := nd
			n.env.S3(S3Request{Op: S3Get, Key: memberKey(nd.id)}, func(g S3Response) {
				if n.stopped {
					return
				}
				if g.Err == nil && !g.NotFound {
					if m, ok := parseMember(nd.id, g.Body); ok {
						m.lastSeen, m.learned = nd.lm, listStart
						next[nd.id] = m
						n.knownETags[nd.id] = nd.etag
					}
				} else if cur := n.view.members[nd.id]; cur != nil && g.Err != nil {
					m := *cur
					next[nd.id] = &m
				}
				left--
				if left == 0 {
					finish()
				}
			})
		}
	})
}

func (n *Node) copyMembers() map[ids.BrokerID]*member {
	out := make(map[ids.BrokerID]*member, len(n.view.members))
	for id, m := range n.view.members {
		c := *m
		out[id] = &c
	}
	return out
}

// applyMembers installs a new member map, changing the view only if the set
// of members, an incarnation or a draining flag changed.
func (n *Node) applyMembers(next map[ids.BrokerID]*member) {
	next[n.id] = n.selfMember()
	changed := len(next) != len(n.view.members)
	if !changed {
		for id, m := range next {
			cur := n.view.members[id]
			if cur == nil || cur.inc != m.inc || cur.draining != m.draining {
				changed = true
				break
			}
		}
	}
	if !changed {
		// Refresh liveness and info in place.
		for id, m := range next {
			*n.view.members[id] = *m
		}
		return
	}
	n.setView(newView(n.tun, next))
}

func (n *Node) setView(nv *view) {
	old := n.view
	now := n.now()
	n.prevView, n.view, n.viewChangedAt = old, nv, now
	n.stats.ViewChanges++
	for _, id := range sortedBrokerKeys(old.members) {
		m := old.members[id]
		if nm := nv.members[id]; nm == nil || nm.inc != m.inc {
			n.departed[regKey(id, m.inc)] = now
		}
	}
	for _, id := range sortedBrokerKeys(nv.members) {
		m := nv.members[id]
		delete(n.departed, regKey(id, m.inc))
		delete(n.fastJoinNeg, id)
		if om := old.members[id]; om == nil || om.inc != m.inc {
			// A new process: suspicion earned by its previous incarnation
			// no longer applies, and handoff to it must not wait.
			if p := n.peers[id]; p != nil {
				p.suspectUntil, p.suspectFor = time.Time{}, 0
			}
		}
	}
	for _, id := range sortedBrokerKeys(n.peers) {
		if nv.members[id] == nil {
			n.dropPeer(id)
		}
	}
	n.onViewChangeStore(old, nv)
	n.onViewChangePresence(old, nv)
	ms := make([]Member, 0, len(nv.members))
	for _, id := range sortedBrokerKeys(nv.members) {
		ms = append(ms, Member{ID: id, Info: nv.members[id].info})
	}
	n.env.MembersChanged(ms)
}

// announce sends Rejoin to every live member (design §4).
func (n *Node) announce() {
	for _, id := range sortedBrokerKeys(n.view.members) {
		if id != n.id {
			n.rejoin(id)
		}
	}
}

func (n *Node) rejoin(id ids.BrokerID) {
	if n.rejoinPending[id] {
		return
	}
	n.rejoinPending[id] = true
	var try func()
	try = func() {
		if n.view.members[id] == nil || n.left {
			delete(n.rejoinPending, id)
			return
		}
		n.call(id, &peerproto.Envelope{Rejoin: &peerproto.Rejoin{Member: n.memberJSON()}}, func(_ *peerproto.Envelope, err error) {
			if err != nil {
				n.after(n.tun.RejoinRetry, try)
				return
			}
			delete(n.rejoinPending, id)
		})
	}
	try()
}

func (n *Node) handleRejoin(from ids.BrokerID, rj *peerproto.Rejoin) {
	m, ok := parseMember(from, rj.Member)
	if !ok {
		return
	}
	m.lastSeen, m.learned = n.s3Now(), n.learn()
	next := n.copyMembers()
	next[from] = m
	n.applyMembers(next)
}

// observeIncarnation handles a frame from a known Broker carrying a new
// incarnation: the Broker restarted (equivalent to a Rejoin).
func (n *Node) observeIncarnation(from ids.BrokerID, inc uint64) {
	next := n.copyMembers()
	m := next[from]
	m.inc = inc
	m.lastSeen, m.learned = n.s3Now(), n.learn()
	n.applyMembers(next)
}

// fastJoin admits an unknown Broker if its member object exists and is fresh.
func (n *Node) fastJoin(id ids.BrokerID) {
	now := n.now()
	if n.fastJoinBusy[id] || n.left {
		return
	}
	if t, ok := n.fastJoinNeg[id]; ok && now.Before(t) {
		return
	}
	if !n.fastJoinTok.take(now, 1) {
		return
	}
	n.fastJoinBusy[id] = true
	start := n.env.Wall()
	issued := n.learn()
	n.env.S3(S3Request{Op: S3Get, Key: memberKey(id)}, func(r S3Response) {
		if n.stopped {
			return
		}
		delete(n.fastJoinBusy, id)
		n.observeDate(start, r.Date)
		var m *member
		ok := r.Err == nil && !r.NotFound && n.s3Now().Sub(r.LastModified) <= n.tun.MemberTTL
		if ok {
			m, ok = parseMember(id, r.Body)
		}
		if !ok {
			n.fastJoinNeg[id] = n.now().Add(n.tun.FastJoinNegTTL)
			return
		}
		m.lastSeen, m.learned = r.LastModified, issued
		n.knownETags[id] = r.ETag
		next := n.copyMembers()
		next[id] = m
		n.applyMembers(next)
	})
}

func (n *Node) checkFence() {
	if !n.ready || n.left || n.stopped {
		return
	}
	stale := n.s3Now().Sub(n.lastHB) > n.tun.MemberTTL-n.tun.FenceMargin
	skew := n.s3OffsetOK && (n.s3Offset > n.tun.MaxSkew || n.s3Offset < -n.tun.MaxSkew)
	f := stale || skew
	switch {
	case f && !n.fenced:
		n.fenced = true
		n.stats.Fenced++
		n.logf("fenced (stale heartbeat=%v, skew=%v)", stale, skew)
		n.closeAllSessions("fenced")
		// Established streams survive fencing; pending ones end.
		n.failPendingStreams()
	case !f && n.fenced:
		// Peers may have declared this Broker dead meanwhile and turned its
		// registers into tombstones. Come back as a new incarnation, exactly
		// as after a restart, so nothing written under the old one can
		// shadow new state.
		n.fenced = false
		n.newIncarnation()
		n.logf("unfenced as incarnation %x", n.inc)
		n.applyMembers(n.copyMembers())
		n.after(0, n.heartbeat)
		n.announce()
		n.repushAllPresence()
	}
}

// Drain starts a graceful leave (design §4): mark draining, close sessions,
// hand off records, then delete the member object.
func (n *Node) Drain() {
	if n.draining || !n.started || n.stopped {
		return
	}
	n.draining = true
	n.drainStarted = n.now()
	n.logf("draining")
	n.after(0, n.heartbeat)
	n.closeAllSessions("draining")
	n.failPendingStreams()
	n.applyMembers(n.copyMembers())
	n.announce()
}

// Draining reports whether Drain was called.
func (n *Node) Draining() bool { return n.draining }

func (n *Node) sweepDrain() {
	if !n.draining || n.leaving || n.left {
		return
	}
	// Established streams get up to StreamDrainGrace, then close with
	// 1001; their Brokers on the other side relay the close.
	since := n.now().Sub(n.drainStarted)
	if since >= n.tun.StreamDrainGrace && !n.streamDrain {
		n.streamDrain = true
		n.closeEstablishedStreams(wire.CloseDraining, wire.ReasonDraining)
	}
	// Leave only once the closes went out (the runtime reports every leg
	// closed), so the process does not cut them off; at most 5s more.
	if len(n.legs) > 0 && since < n.tun.StreamDrainGrace+5*time.Second {
		return
	}
	idle := len(n.rejoinPending) == 0
	for _, id := range sortedBrokerKeys(n.peers) {
		p := n.peers[id]
		if len(p.q) > 0 || p.inflight > 0 {
			idle = false
		}
	}
	if !idle && n.now().Sub(n.drainStarted) < n.tun.DrainTimeout {
		return
	}
	n.leaving = true
	n.env.S3(S3Request{Op: S3Delete, Key: memberKey(n.id)}, func(S3Response) {
		if n.stopped {
			return
		}
		var tok [16]byte
		ids.RandBytes(n.env.Rand(), tok[:])
		n.env.S3(S3Request{Op: S3Put, Key: signalKey, Body: []byte(hex.EncodeToString(tok[:]))}, func(S3Response) {
			if n.stopped {
				return
			}
			n.left = true
			n.logf("left the cluster")
			n.env.Left()
		})
	})
}

// Members returns the current view's member IDs in sorted order.
func (n *Node) Members() []ids.BrokerID { return sortedBrokerKeys(n.view.members) }

// OnUnknownPeer is called by the runtime when the mesh sees a handshake from
// a node key that is not a member: the Broker checks S3 (fast join).
func (n *Node) OnUnknownPeer(id ids.BrokerID) {
	if n.started && !n.stopped && n.view.members[id] == nil {
		n.fastJoin(id)
	}
}

// UpdateMemberInfo changes this Broker's member object (for example the
// mesh's disco key, endpoints and home DERP region) and publishes it at once:
// a heartbeat PUT, and a Rejoin to every member.
func (n *Node) UpdateMemberInfo(f func(*MemberInfo)) {
	before := n.memberJSON()
	f(&n.cfg.Member)
	if !n.started || n.stopped || string(before) == string(n.memberJSON()) {
		return
	}
	n.after(0, n.heartbeat)
	n.applyMembers(n.copyMembers())
	if !n.fenced {
		// A fenced Broker must not re-announce itself: peers may have
		// declared it dead, and it rejoins when it unfences.
		n.announce()
	}
}

func (n *Node) learn() uint64 {
	n.learnSeq++
	return n.learnSeq
}
