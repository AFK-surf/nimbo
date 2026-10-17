package node

import (
	"fmt"
	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/internal/peerproto"
	"testing"
	"time"
)

// release delivers held S3 responses.
func (b *bench) release() {
	h := b.heldList
	b.heldList = nil
	b.q = append(b.q, h...)
	b.drain()
}

// A member LIST is a snapshot. A Broker that joins after the snapshot was
// taken, and is admitted (Rejoin + fast join) before the LIST response is
// applied, must not be dropped by it.
func TestPollDoesNotDropMemberLearnedLater(t *testing.T) {
	b := newBench(t, 3)
	a := b.nodes[0]
	b.holdList[a] = true
	a.pollInFlight = false
	a.poll() // snapshot taken now, response held
	d := b.addNode(3)
	d.Start()
	b.advance(2 * time.Second) // d joins and Rejoins; a fast-joins it
	if a.view.members[d.id] == nil {
		t.Fatal("setup: a did not admit the joiner")
	}
	b.holdList[a] = false
	b.release()
	if a.view.members[d.id] == nil {
		t.Fatal("a stale LIST snapshot removed a member learned after it")
	}
	if _, gone := a.departed[regKey(d.id, d.inc)]; gone {
		t.Fatal("joiner marked departed")
	}
}

// A Broker restarts (new incarnation) while another Broker's poll is between
// its LIST and its GETs. Applying the poll must not roll the incarnation
// back to the one in the snapshot.
func TestPollDoesNotRollBackIncarnation(t *testing.T) {
	b := newBench(t, 3)
	a, other, c := b.nodes[0], b.nodes[1], b.nodes[2]
	// Change another member's object so a's poll needs a GET.
	other.UpdateMemberInfo(func(mi *MemberInfo) { mi.Region = "elsewhere" })
	b.advance(10 * time.Millisecond)
	o := b.s3[memberKey(c.id)]
	if a.knownETags[c.id] != fmt.Sprint(len(o.body), string(o.body)) {
		t.Fatal("setup: the poll must take the unchanged-object path for c")
	}
	b.holdS3[a] = true
	a.pollInFlight = false
	a.poll()
	b.release() // LIST response: a issues a GET for the changed member (held)
	// c restarts: same key, new incarnation, Rejoin reaches a.
	c.Stop()
	c2 := b.addNode(2)
	b.byID[c2.id] = c2
	c2.Start()
	b.advance(2 * time.Second)
	if c2.inc == c.inc {
		t.Fatal("setup: same incarnation")
	}
	if a.view.members[c2.id].inc != c2.inc {
		t.Fatal("setup: a did not learn the new incarnation")
	}
	b.holdS3[a] = false
	// Deliver only the held responses, without letting c2's later frames
	// (which carry its incarnation) repair a's view first.
	h := b.heldList
	b.heldList = nil
	for _, f := range h {
		f()
	}
	if got := a.view.members[c2.id].inc; got != c2.inc {
		t.Fatalf("a rolled the incarnation back from %x to %x", c2.inc, got)
	}
	if _, gone := a.departed[regKey(c2.id, c2.inc)]; gone {
		t.Fatal("the new incarnation was marked departed")
	}
}

// A fast join's GET can return a member object older than what the Broker
// learned while it was in flight: the member restarted (or unfenced) and its
// Rejoin or frames arrived first, possibly while its PUTs were failing. The
// stale object must not roll the incarnation back: that would declare the
// live incarnation departed and tombstone its presence registers, which
// nothing repairs until the owner's periodic reconcile (nightly DST seed
// 3950053).
func TestFastJoinDoesNotRollBackIncarnation(t *testing.T) {
	b := newBench(t, 3)
	a, c := b.nodes[0], b.nodes[2]
	// a does not know c (as right after a restart), and fast-joins it; the
	// GET reads c's object now, but its response is held.
	next := a.copyMembers()
	delete(next, c.id)
	a.applyMembers(next)
	b.holdS3[a] = true
	a.fastJoin(c.id)
	if len(b.heldList) != 1 {
		t.Fatalf("setup: %d held S3 responses, want the fast join's GET", len(b.heldList))
	}
	// c comes back as a new incarnation and a learns it from c itself.
	oldInc := c.inc
	c.newIncarnation()
	c.applyMembers(c.copyMembers())
	a.handleRejoin(c.id, &peerproto.Rejoin{Member: c.memberJSON()})
	if a.view.members[c.id] == nil || a.view.members[c.id].inc != c.inc {
		t.Fatal("setup: a did not learn the new incarnation")
	}
	// An endpoint on c: with 3 Brokers and 3 replicas, a holds its register.
	ep := b.connect(c)
	b.drain()
	rk := regKey(c.id, c.inc)
	pr := a.pres[ep.key]
	if pr == nil || pr.regs[rk].Tomb || pr.regs[rk].Sessions != 1 {
		t.Fatal("setup: a does not hold the live register")
	}
	// Now the GET response (c's object under the old incarnation) arrives.
	b.holdS3[a] = false
	h := b.heldList
	b.heldList = nil
	for _, f := range h {
		f()
	}
	b.drain()
	if got := a.view.members[c.id].inc; got != c.inc {
		t.Fatalf("a rolled the incarnation back from %x to %x (old %x)", c.inc, got, oldInc)
	}
	if _, gone := a.departed[rk]; gone {
		t.Fatal("the live incarnation was marked departed")
	}
	if pr.regs[rk].Tomb {
		t.Fatal("the live presence register was tombstoned")
	}
}

// A poll's GET of a fresh member object fails, and a fast join issued
// before the poll's LIST admits that member while the poll still waits for
// its other GETs. The failed read says nothing about the member: the poll
// must not drop it (which would declare its live incarnation departed and
// tombstone its presence registers until it is re-admitted; nightly DST
// seed 7050039).
func TestPollDoesNotDropMemberOnFailedGet(t *testing.T) {
	b := newBench(t, 3)
	a, other, c := b.nodes[0], b.nodes[1], b.nodes[2]
	// a knows neither (as right after a restart), so its poll GETs both.
	next := a.copyMembers()
	delete(next, other.id)
	delete(next, c.id)
	a.applyMembers(next)
	b.holdS3[a] = true
	a.fastJoin(c.id) // issued before the LIST
	a.pollInFlight = false
	a.poll()
	h := b.heldList
	b.heldList = nil
	if len(h) != 2 {
		t.Fatalf("setup: %d held S3 responses, want the fast join's GET and the LIST", len(h))
	}
	fastJoinGet, list := h[0], h[1]
	b.s3Fail = func(n *Node, req S3Request) bool { return n == a && req.Key == memberKey(c.id) }
	list() // the poll issues its GETs: c's fails, other's succeeds
	b.s3Fail = nil
	gets := b.heldList
	b.heldList = nil
	if len(gets) != 2 {
		t.Fatalf("setup: %d GETs, want 2", len(gets))
	}
	var cGet, otherGet func()
	if sortedBrokerKeys(map[ids.BrokerID]bool{other.id: true, c.id: true})[0] == c.id {
		cGet, otherGet = gets[0], gets[1]
	} else {
		cGet, otherGet = gets[1], gets[0]
	}
	cGet()        // fails
	fastJoinGet() // admits c
	if a.view.members[c.id] == nil {
		t.Fatal("setup: the fast join did not admit c")
	}
	otherGet() // the poll finishes
	b.holdS3[a] = false
	if a.view.members[c.id] == nil {
		t.Fatal("a failed GET dropped a member")
	}
	if _, gone := a.departed[regKey(c.id, c.inc)]; gone {
		t.Fatal("the live incarnation was marked departed")
	}
	if a.view.members[other.id] == nil {
		t.Fatal("the poll did not admit the other member")
	}
}

// A joiner whose frames cannot reach the existing Brokers (over the
// Tailscale mesh, magicsock drops packets from unknown keys) must still be
// admitted within about one signal poll, not one full poll: it rewrites
// the membership signal after writing its member object.
func TestJoinerSeenThroughSignal(t *testing.T) {
	b := newBench(t, 3)
	for _, n := range b.nodes {
		n.tun.MemberPoll = time.Hour // only the signal can trigger a refresh
	}
	b.mute = map[ids.BrokerID]bool{}
	b.tune = func(tu *Tunables) { tu.MemberPoll = time.Hour }
	d := b.addNode(3)
	b.mute[d.id] = true // no Rejoin reaches the others
	d.Start()
	b.advance(d.tun.MemberSignalPoll + time.Second)
	for _, n := range b.nodes[:3] {
		if n.view.members[d.id] == nil {
			t.Fatalf("%s did not admit the joiner within a signal poll", n.id.Abbrev())
		}
	}
	// And a Broker that leaves is gone as fast.
	b.mute[d.id] = false
	d.Drain()
	b.advance(d.tun.DrainTimeout + d.tun.MemberSignalPoll + 2*time.Second)
	for _, n := range b.nodes[:3] {
		if n.view.members[d.id] != nil {
			t.Fatalf("%s still lists the Broker that left", n.id.Abbrev())
		}
	}
}

// Without changes, Brokers only read the signal: no LIST until the
// backstop poll.
func TestNoRefreshWithoutSignal(t *testing.T) {
	b := newBench(t, 3)
	b.lists = map[*Node]int{}
	b.advance(25 * time.Second) // < MemberPoll since the last poll
	for _, n := range b.nodes {
		if got := b.lists[n]; got > 1 {
			t.Errorf("%s listed members %d times without a change", n.id.Abbrev(), got)
		}
	}
}
