// Command nimbo-sim runs deterministic simulations of a Nimbo cluster over a
// range of seeds and reports invariant violations. A failing seed replays
// exactly: rerun it with -seed N -n 1 -v to get the full event log.
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/afk-surf/nimbo/internal/sim"
)

func main() {
	seed := flag.Uint64("seed", 1, "first seed")
	n := flag.Int("n", 100, "number of seeds")
	brokers := flag.Int("brokers", 6, "brokers")
	endpoints := flag.Int("endpoints", 24, "endpoints")
	faultPhase := flag.Duration("faults", 4*time.Minute, "fault phase (simulated)")
	quiet := flag.Duration("quiet", 5*time.Minute, "quiet phase (simulated)")
	noFaults := flag.Bool("nofaults", false, "disable fault injection")
	scale := flag.Float64("fault-scale", 1, "divide mean fault intervals by this (higher = more faults)")
	verbose := flag.Bool("v", false, "print the event log (use with -n 1)")
	par := flag.Int("p", runtime.NumCPU(), "parallel runs")
	flag.Parse()

	faults := sim.DefaultFaults()
	if *noFaults {
		faults = sim.Faults{}
	}
	if *scale != 1 {
		f := &faults
		for _, d := range []*time.Duration{&f.Crash, &f.Partition, &f.LinkReset, &f.S3Outage, &f.ClockSkew, &f.Drain, &f.Churn, &f.KeyRotation} {
			*d = time.Duration(float64(*d) / *scale)
		}
	}
	type out struct {
		seed uint64
		res  *sim.Result
		dur  time.Duration
	}
	results := make(chan out)
	seeds := make(chan uint64)
	var wg sync.WaitGroup
	for i := 0; i < *par; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := range seeds {
				cfg := sim.Config{Seed: s, Brokers: *brokers, Endpoints: *endpoints, FaultPhase: *faultPhase, Quiet: *quiet, Faults: faults}
				if *verbose {
					cfg.Log = os.Stdout
				}
				t0 := time.Now()
				r := sim.New(cfg).Run()
				results <- out{s, r, time.Since(t0)}
			}
		}()
	}
	go func() {
		for i := 0; i < *n; i++ {
			seeds <- *seed + uint64(i)
		}
		close(seeds)
		wg.Wait()
		close(results)
	}()
	failed := 0
	for o := range results {
		if len(o.res.Violations) > 0 || *verbose {
			fmt.Print(o.res.String())
		}
		if len(o.res.Violations) > 0 {
			failed++
			fmt.Printf("FAIL seed %d (%v)\n", o.seed, o.dur.Round(time.Millisecond))
		}
	}
	fmt.Printf("%d/%d seeds passed\n", *n-failed, *n)
	if failed > 0 {
		os.Exit(1)
	}
}
