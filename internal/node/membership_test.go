package node

import (
	"fmt"
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
