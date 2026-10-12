// Package sim is a deterministic simulator for Nimbo clusters. One seeded,
// single-threaded event loop drives real node.Node instances through
// simulated time, a simulated network, a simulated S3 and simulated
// endpoints, injects faults, and checks invariants. The same seed always
// produces the same execution, recorded as a trace hash.
package sim

import (
	"container/heap"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/internal/node"
)

// Config describes one simulation run.
type Config struct {
	Seed      uint64
	Brokers   int
	Endpoints int
	// FaultPhase is how long faults are injected; then Quiet runs with no
	// faults so liveness and convergence can be checked.
	FaultPhase time.Duration
	Quiet      time.Duration
	Faults     Faults
	Tunables   node.Tunables
	Limits     node.Limits
	// Log receives a human-readable event log if set.
	Log io.Writer
}

// Faults sets the mean interval between injected faults of each kind; zero
// disables a kind.
type Faults struct {
	Crash       time.Duration
	Partition   time.Duration
	LinkReset   time.Duration
	S3Outage    time.Duration
	ClockSkew   time.Duration
	Drain       time.Duration
	Churn       time.Duration // endpoint disconnects
	KeyRotation time.Duration
	// S3ErrorRate is the probability that an S3 request fails during the
	// fault phase.
	S3ErrorRate float64
	// LatencySpikes is the probability that a network message is delayed by
	// 50-500ms during the fault phase.
	LatencySpikes float64
}

// DefaultFaults injects every kind of fault.
func DefaultFaults() Faults {
	return Faults{
		Crash:         40 * time.Second,
		Partition:     60 * time.Second,
		LinkReset:     5 * time.Second,
		S3Outage:      90 * time.Second,
		ClockSkew:     120 * time.Second,
		Drain:         150 * time.Second,
		Churn:         2 * time.Second,
		KeyRotation:   30 * time.Second,
		S3ErrorRate:   0.02,
		LatencySpikes: 0.01,
	}
}

// World is one simulation.
type World struct {
	cfg   Config
	rng   *rand.Rand
	epoch time.Time
	now   time.Duration
	q     eventQueue
	seq   uint64
	trace hash.Hash
	steps uint64

	brokers []*Broker
	byID    map[ids.BrokerID]*Broker
	eps     []*Endpoint
	s3      *S3
	links   map[[2]int]*link

	faultsOn  bool
	quietAt   time.Duration
	settledAt time.Duration
	check     *Checker
	nextEpIdx int
}

type event struct {
	at       time.Duration
	seq      uint64
	fn       func()
	canceled bool
	idx      int
}

type eventQueue []*event

func (q eventQueue) Len() int { return len(q) }
func (q eventQueue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}
func (q eventQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i]; q[i].idx = i; q[j].idx = j }
func (q *eventQueue) Push(x any)   { e := x.(*event); e.idx = len(*q); *q = append(*q, e) }
func (q *eventQueue) Pop() any {
	old := *q
	e := old[len(old)-1]
	*q = old[:len(old)-1]
	return e
}

// At schedules fn at absolute simulated time t.
func (w *World) At(t time.Duration, fn func()) *event {
	if t < w.now {
		t = w.now
	}
	w.seq++
	e := &event{at: t, seq: w.seq, fn: fn}
	heap.Push(&w.q, e)
	return e
}

// After schedules fn after d.
func (w *World) After(d time.Duration, fn func()) *event { return w.At(w.now+d, fn) }

// Now is the true simulated time.
func (w *World) Now() time.Time { return w.epoch.Add(w.now) }

// Elapsed is the simulated time since the start.
func (w *World) Elapsed() time.Duration { return w.now }

func (w *World) logf(format string, args ...any) {
	if w.cfg.Log != nil {
		fmt.Fprintf(w.cfg.Log, "%10.3fs %s\n", w.now.Seconds(), fmt.Sprintf(format, args...))
	}
}

// traceEvent mixes an event into the trace hash.
func (w *World) traceEvent(kind string, parts ...[]byte) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(w.now))
	w.trace.Write(b[:])
	w.trace.Write([]byte(kind))
	for _, p := range parts {
		binary.LittleEndian.PutUint64(b[:], uint64(len(p)))
		w.trace.Write(b[:])
		w.trace.Write(p)
	}
}

// dur draws a uniform duration in [lo, hi).
func (w *World) dur(lo, hi time.Duration) time.Duration {
	if hi <= lo {
		return lo
	}
	return lo + time.Duration(w.rng.Int64N(int64(hi-lo)))
}

// exp draws an exponential interval with the given mean.
func (w *World) exp(mean time.Duration) time.Duration {
	return time.Duration(w.rng.ExpFloat64() * float64(mean))
}

func (w *World) chance(p float64) bool { return w.rng.Float64() < p }

// New builds a world. Brokers and endpoints start at once.
func New(cfg Config) *World {
	if cfg.Tunables.Replicas == 0 {
		cfg.Tunables = node.DefaultTunables()
	}
	if cfg.Limits.MaxPayloadBytes == 0 {
		cfg.Limits = node.DefaultLimits()
	}
	w := &World{
		cfg:   cfg,
		rng:   rand.New(rand.NewChaCha8(seedBytes(cfg.Seed, "world"))),
		epoch: time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC),
		trace: sha256.New(),
		byID:  map[ids.BrokerID]*Broker{},
		links: map[[2]int]*link{},
	}
	w.s3 = newS3(w)
	w.check = newChecker(w)
	for i := 0; i < cfg.Brokers; i++ {
		b := newBroker(w, i)
		w.brokers = append(w.brokers, b)
		w.byID[b.id] = b
		// Stagger starts so brokers join a cluster that is forming.
		w.After(w.dur(0, 3*time.Second), b.start)
	}
	for i := 0; i < cfg.Endpoints; i++ {
		w.addEndpoint(w.dur(5*time.Second, 10*time.Second))
	}
	return w
}

func seedBytes(seed uint64, label string) [32]byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], seed)
	return sha256.Sum256(append(b[:], label...))
}

// Result summarises a run.
type Result struct {
	Seed       uint64
	Trace      string
	Steps      uint64
	Violations []string
	Stats      Stats
}

// Stats are workload counters.
type Stats struct {
	Sends, SendOK, Delivered, Bounced, Replies     int
	Mints, Transfers, Renews, Attacks, AttacksOK   int
	Expected, ExpectedOK, ExpectedDelivered        int
	Crashes, Partitions, S3Outages, Skews, Drains  int
	Rotations, Churns, LinkResets                  int
	Errors                                         map[string]int
	BrokerDelivered, BrokerForwarded, BrokerHinted uint64
}

func (r *Result) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "seed=%d trace=%s steps=%d\n", r.Seed, r.Trace[:16], r.Steps)
	s := r.Stats
	fmt.Fprintf(&b, "  sends=%d ok=%d delivered=%d bounced=%d replies=%d mints=%d transfers=%d renews=%d attacks=%d\n",
		s.Sends, s.SendOK, s.Delivered, s.Bounced, s.Replies, s.Mints, s.Transfers, s.Renews, s.Attacks)
	fmt.Fprintf(&b, "  quiet-phase expected=%d ok=%d delivered=%d\n", s.Expected, s.ExpectedOK, s.ExpectedDelivered)
	fmt.Fprintf(&b, "  faults: crashes=%d partitions=%d s3outages=%d skews=%d drains=%d rotations=%d churns=%d linkresets=%d\n",
		s.Crashes, s.Partitions, s.S3Outages, s.Skews, s.Drains, s.Rotations, s.Churns, s.LinkResets)
	fmt.Fprintf(&b, "  brokers: delivered=%d forwarded=%d hinted=%d\n", s.BrokerDelivered, s.BrokerForwarded, s.BrokerHinted)
	fmt.Fprintf(&b, "  errors: %v\n", s.Errors)
	for _, v := range r.Violations {
		fmt.Fprintf(&b, "  VIOLATION: %s\n", v)
	}
	return b.String()
}

// Run executes the simulation to completion.
func (w *World) Run() *Result {
	w.faultsOn = true
	w.scheduleFaults()
	w.quietAt = w.cfg.FaultPhase
	w.At(w.quietAt, w.beginQuiet)
	end := w.cfg.FaultPhase + w.cfg.Quiet
	w.settledAt = w.quietAt + (end-w.quietAt)/2
	w.At(end-time.Second, w.check.finalChecks)
	for w.q.Len() > 0 {
		e := heap.Pop(&w.q).(*event)
		if e.at > end {
			break
		}
		if e.canceled {
			continue
		}
		w.now = e.at
		w.steps++
		e.fn()
	}
	res := &Result{Seed: w.cfg.Seed, Trace: fmt.Sprintf("%x", w.trace.Sum(nil)), Steps: w.steps}
	res.Violations = w.check.violations
	res.Stats = w.check.stats
	for _, b := range w.brokers {
		if b.node != nil {
			st := b.node.Stats()
			res.Stats.BrokerDelivered += st.Delivered
			res.Stats.BrokerForwarded += st.Forwarded
			res.Stats.BrokerHinted += st.HintedWrites
		}
	}
	return res
}

// beginQuiet stops faults and heals everything.
func (w *World) beginQuiet() {
	w.faultsOn = false
	w.logf("quiet phase: healing all faults")
	for _, b := range w.brokers {
		b.group = 0
		b.s3Broken = false
		b.skew = b.baseSkew
		if !b.alive {
			b.restart()
		}
	}
	w.resetAllLinks()
}
