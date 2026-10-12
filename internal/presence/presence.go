// Package presence implements the per-owner presence registers of design §9.1:
// the merge rule (a last-writer-wins lattice) and state classification.
package presence

import (
	"bytes"
	"sort"
	"time"

	"github.com/afk-surf/nimbo/internal/ids"
)

// RegKey names one register: the owner Broker and its incarnation.
type RegKey struct {
	Owner ids.BrokerID
	Inc   uint64
}

func (a RegKey) Less(b RegKey) bool {
	if c := bytes.Compare(a.Owner[:], b.Owner[:]); c != 0 {
		return c < 0
	}
	return a.Inc < b.Inc
}

// Reg is one owner's view of an endpoint's sessions on it.
type Reg struct {
	Seq      uint64
	Sessions uint32
	// Tomb marks a register whose owner died or restarted. Tombstones win
	// over live registers.
	Tomb bool
	// AbsentSince is set (local time) when Sessions == 0 or Tomb.
	AbsentSince time.Time
}

// Absent reports whether the register holds no live session.
func (r Reg) Absent() bool { return r.Tomb || r.Sessions == 0 }

// cmp orders registers; Merge takes the maximum, so merging is commutative,
// associative and idempotent. The owner's sequence number dominates, so an
// owner that was wrongly declared dead can override a holder's tombstone
// with a newer write; at equal sequence numbers a tombstone wins, and among
// tombstones the later absence wins (the conservative choice for "gone").
// Writes from owners a holder knows are dead are re-tombstoned on receipt,
// so a delayed write cannot resurrect a dead owner's session.
func cmp(a, b Reg) int {
	if c := cmpU(a.Seq, b.Seq); c != 0 {
		return c
	}
	if a.Tomb != b.Tomb {
		if a.Tomb {
			return 1
		}
		return -1
	}
	if c := a.AbsentSince.Compare(b.AbsentSince); c != 0 {
		return c
	}
	return cmpU(uint64(a.Sessions), uint64(b.Sessions))
}

func cmpU(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// Merge returns the winner of two writes to the same register.
func Merge(a, b Reg) Reg {
	if cmp(a, b) >= 0 {
		return a
	}
	return b
}

// Set is the presence record of one endpoint: one register per owner.
type Set map[RegKey]Reg

// MergeInto merges src into dst and reports whether dst changed.
func (dst Set) MergeInto(src Set) bool {
	changed := false
	for k, r := range src {
		old, ok := dst[k]
		if !ok {
			dst[k] = r
			changed = true
			continue
		}
		m := Merge(old, r)
		if m != old {
			dst[k] = m
			changed = true
		}
	}
	return changed
}

// Keys returns the register keys in a deterministic order.
func (s Set) Keys() []RegKey {
	ks := make([]RegKey, 0, len(s))
	for k := range s {
		ks = append(ks, k)
	}
	sort.Slice(ks, func(i, j int) bool { return ks[i].Less(ks[j]) })
	return ks
}

// Clone copies the set.
func (s Set) Clone() Set {
	c := make(Set, len(s))
	for k, v := range s {
		c[k] = v
	}
	return c
}

// State of an endpoint as seen by a lookup.
type State int

const (
	Unknown State = iota
	Present
	RecentlyAbsent
	Gone
)

func (s State) String() string {
	return [...]string{"unknown", "present", "recently_absent", "gone"}[s]
}

// Validity reports how the reader sees a register's owner.
type Validity int

const (
	// Valid: the owner is a live member with this incarnation.
	Valid Validity = iota
	// UnknownOwner: the reader does not know this owner yet (a new Broker).
	UnknownOwner
	// Invalid: the owner is dead or has restarted.
	Invalid
)

// Classify applies the §9.1 state table. Owners with live sessions are
// returned in deterministic order.
func Classify(s Set, now time.Time, capTTL time.Duration, validity func(RegKey) Validity) (State, []ids.BrokerID) {
	if len(s) == 0 {
		return Unknown, nil
	}
	var owners []ids.BrokerID
	var latest time.Time
	for _, k := range s.Keys() {
		r := s[k]
		absentSince := r.AbsentSince
		if !r.Absent() {
			switch validity(k) {
			case Valid, UnknownOwner:
				owners = append(owners, k.Owner)
				continue
			case Invalid:
				// The reader knows the owner is gone but this holder has
				// not pruned yet: treat as absent from now on.
				absentSince = now
			}
		}
		if absentSince.After(latest) {
			latest = absentSince
		}
	}
	if len(owners) > 0 {
		return Present, owners
	}
	if now.Sub(latest) <= capTTL {
		return RecentlyAbsent, nil
	}
	return Gone, nil
}

// Wire is the relative-time wire form of a register.
type Wire struct {
	Owner    ids.BrokerID `cbor:"1,keyasint"`
	Inc      uint64       `cbor:"2,keyasint"`
	Seq      uint64       `cbor:"3,keyasint"`
	Sessions uint32       `cbor:"4,keyasint,omitempty"`
	Tomb     bool         `cbor:"5,keyasint,omitempty"`
	AbsentMs int64        `cbor:"6,keyasint,omitempty"` // elapsed since absence began
}

// ToWire converts a set to wire form relative to now.
func ToWire(s Set, now time.Time) []Wire {
	out := make([]Wire, 0, len(s))
	for _, k := range s.Keys() {
		r := s[k]
		w := Wire{Owner: k.Owner, Inc: k.Inc, Seq: r.Seq, Sessions: r.Sessions, Tomb: r.Tomb}
		if r.Absent() {
			w.AbsentMs = max(0, now.Sub(r.AbsentSince).Milliseconds())
		}
		out = append(out, w)
	}
	return out
}

// FromWire converts wire registers to local time. Elapsed times are clamped
// to maxAbsent so that a bad clock cannot age a register arbitrarily.
func FromWire(ws []Wire, now time.Time, maxAbsent time.Duration) Set {
	s := make(Set, len(ws))
	for _, w := range ws {
		r := Reg{Seq: w.Seq, Sessions: w.Sessions, Tomb: w.Tomb}
		if r.Absent() {
			el := time.Duration(max(0, w.AbsentMs)) * time.Millisecond
			el = min(el, maxAbsent)
			r.AbsentSince = now.Add(-el)
		}
		k := RegKey{Owner: w.Owner, Inc: w.Inc}
		if old, ok := s[k]; ok {
			r = Merge(old, r)
		}
		s[k] = r
	}
	return s
}
