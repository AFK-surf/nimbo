package presence

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/afk-surf/nimbo/internal/ids"
)

func randReg(r *rand.Rand, t0 time.Time) Reg {
	reg := Reg{Seq: uint64(r.IntN(4)), Sessions: uint32(r.IntN(2)), Tomb: r.IntN(3) == 0}
	if reg.Absent() {
		reg.AbsentSince = t0.Add(time.Duration(r.IntN(3)) * time.Second)
	}
	return reg
}

func TestMergeLattice(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	t0 := time.Unix(1000, 0)
	for i := 0; i < 20000; i++ {
		a, b, c := randReg(r, t0), randReg(r, t0), randReg(r, t0)
		if Merge(a, b) != Merge(b, a) {
			t.Fatalf("not commutative: %+v %+v", a, b)
		}
		if Merge(Merge(a, b), c) != Merge(a, Merge(b, c)) {
			t.Fatalf("not associative")
		}
		if Merge(a, a) != a {
			t.Fatalf("not idempotent")
		}
	}
}

func TestTombstoneWins(t *testing.T) {
	t0 := time.Unix(1000, 0)
	live := Reg{Seq: 9, Sessions: 1}
	tomb := Reg{Seq: 9, Tomb: true, AbsentSince: t0}
	if Merge(live, tomb) != tomb {
		t.Fatal("tombstone must win at equal seq")
	}
	newer := Reg{Seq: 10, Sessions: 1}
	if Merge(newer, tomb) != newer {
		t.Fatal("a newer owner write must win over a tombstone")
	}
}

func TestClassify(t *testing.T) {
	now := time.Unix(10000, 0)
	k1 := RegKey{Owner: ids.BrokerID{1}, Inc: 1}
	k2 := RegKey{Owner: ids.BrokerID{2}, Inc: 1}
	valid := func(RegKey) Validity { return Valid }
	if st, _ := Classify(Set{}, now, time.Minute, valid); st != Unknown {
		t.Fatal(st)
	}
	s := Set{k1: {Seq: 1, Sessions: 1}, k2: {Seq: 1, Tomb: true, AbsentSince: now.Add(-2 * time.Minute)}}
	if st, o := Classify(s, now, time.Minute, valid); st != Present || len(o) != 1 {
		t.Fatal(st)
	}
	// Owner 1 is known dead: absent from now, so recently absent, never gone.
	inv := func(k RegKey) Validity {
		if k == k1 {
			return Invalid
		}
		return Valid
	}
	if st, _ := Classify(s, now, time.Minute, inv); st != RecentlyAbsent {
		t.Fatal(st)
	}
	delete(s, k1)
	if st, _ := Classify(s, now, time.Minute, valid); st != Gone {
		t.Fatal(st)
	}
}
