package sim

import "time"

func (w *World) periodic(mean time.Duration, fn func()) {
	if mean <= 0 {
		return
	}
	var tick func()
	tick = func() {
		if !w.faultsOn {
			return
		}
		fn()
		w.After(w.exp(mean)+time.Millisecond, tick)
	}
	w.After(w.exp(mean)+10*time.Second, tick)
}

func (w *World) scheduleFaults() {
	f := w.cfg.Faults
	w.periodic(f.Crash, w.faultCrash)
	w.periodic(f.Partition, w.faultPartition)
	w.periodic(f.LinkReset, w.faultLinkReset)
	w.periodic(f.S3Outage, w.faultS3Outage)
	w.periodic(f.ClockSkew, w.faultSkew)
	w.periodic(f.Drain, w.faultDrain)
	w.periodic(f.Churn, w.faultChurn)
	w.periodic(f.KeyRotation, w.faultRotate)
}

func (w *World) aliveBrokers() []*Broker {
	var out []*Broker
	for _, b := range w.brokers {
		if b.alive {
			out = append(out, b)
		}
	}
	return out
}

func (w *World) faultCrash() {
	al := w.aliveBrokers()
	if len(al) <= 2 {
		return
	}
	b := al[w.rng.IntN(len(al))]
	w.check.stats.Crashes++
	b.crash()
	w.After(w.dur(2*time.Second, 60*time.Second), func() {
		if w.faultsOn {
			b.restart()
		}
	})
}

func (w *World) faultPartition() {
	for _, b := range w.brokers {
		if b.group != 0 {
			return // one partition at a time
		}
	}
	n := 1 + w.rng.IntN(max(1, len(w.brokers)/2))
	perm := w.rng.Perm(len(w.brokers))
	for _, i := range perm[:n] {
		w.brokers[i].group = 1
	}
	w.check.stats.Partitions++
	w.logf("PARTITION %d brokers", n)
	w.resetAllLinks()
	w.After(w.dur(3*time.Second, 40*time.Second), func() {
		for _, b := range w.brokers {
			b.group = 0
		}
		w.logf("partition healed")
		w.resetAllLinks()
	})
}

func (w *World) faultLinkReset() {
	a, b := w.rng.IntN(len(w.brokers)), w.rng.IntN(len(w.brokers))
	if a != b {
		w.check.stats.LinkResets++
		w.resetLink(a, b)
	}
}

func (w *World) faultS3Outage() {
	al := w.aliveBrokers()
	if len(al) == 0 {
		return
	}
	b := al[w.rng.IntN(len(al))]
	if b.s3Broken {
		return
	}
	d := w.dur(10*time.Second, 120*time.Second)
	w.check.stats.S3Outages++
	w.logf("%s S3 OUTAGE for %v", b, d)
	b.s3Broken = true
	gen := b.gen
	if d > 55*time.Second {
		// A Broker that cannot heartbeat must fence itself before its peers
		// declare it dead (design §4).
		w.After(55*time.Second, func() {
			if b.alive && b.gen == gen && b.s3Broken && b.node.Ready() {
				w.check.violate("%s not fenced 55s into an S3 outage", b)
			}
		})
	}
	w.After(d, func() { b.s3Broken = false })
}

func (w *World) faultSkew() {
	b := w.brokers[w.rng.IntN(len(w.brokers))]
	if b.skew != b.baseSkew {
		return
	}
	s := w.dur(5*time.Second, 9*time.Second)
	if w.chance(0.5) {
		s = -s
	}
	d := w.dur(30*time.Second, 120*time.Second)
	w.check.stats.Skews++
	w.logf("%s CLOCK SKEW %v for %v", b, s, d)
	b.skew = s
	gen := b.gen
	if d > 50*time.Second {
		w.After(45*time.Second, func() {
			if b.alive && b.gen == gen && b.skew == s && !b.s3Broken && b.node.Ready() {
				w.check.violate("%s not fenced with clock skew %v", b, s)
			}
		})
	}
	w.After(d, func() { b.skew = b.baseSkew })
}

func (w *World) faultDrain() {
	al := w.aliveBrokers()
	if len(al) <= 3 {
		return
	}
	b := al[w.rng.IntN(len(al))]
	if b.node.Draining() {
		return
	}
	w.check.stats.Drains++
	w.logf("%s DRAIN", b)
	b.node.Drain()
	w.After(w.dur(40*time.Second, 90*time.Second), func() {
		if w.faultsOn && !b.alive {
			b.restart()
		}
	})
}

func (w *World) faultChurn() {
	if len(w.eps) == 0 {
		return
	}
	e := w.eps[w.rng.IntN(len(w.eps))]
	if !e.retired {
		w.check.stats.Churns++
		e.churn()
	}
}

func (w *World) faultRotate() {
	e := w.randomPeer(nil)
	if e == nil {
		return
	}
	w.check.stats.Rotations++
	w.logf("%s rotates its key", e.name)
	e.retire()
	w.addEndpoint(w.dur(0, time.Second))
}
