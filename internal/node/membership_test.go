package node

import (
	"fmt"
	"github.com/afk-surf/nimbo/internal/ids"
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
