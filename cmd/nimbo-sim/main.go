// Command nimbo-sim runs deterministic simulations of a Nimbo cluster over a
// range of seeds and reports invariant violations. A failing seed replays
// exactly: rerun it with -seed N -n 1 -v to get the full event log.
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"
	"text/tabwriter"
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
	account := flag.Bool("account", false, "protocol cost report: one fault-free run, counters reset after -warm")
	warm := flag.Duration("warm", 2*time.Minute, "warm-up before accounting starts (with -account)")
	flag.Parse()
	if *account {
		cfg := sim.Config{Seed: *seed, Brokers: *brokers, Endpoints: *endpoints, FaultPhase: *warm, Quiet: *quiet}
		cfg.Script = func(w *sim.World) { w.At(*warm, w.ResetAccounting) }
		t0 := time.Now()
		r := sim.New(cfg).Run()
		report(r, *brokers, *endpoints, time.Since(t0))
		if len(r.Violations) > 0 {
			fmt.Print(r.String())
			os.Exit(1)
		}
		return
	}

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

func report(r *sim.Result, brokers, endpoints int, wall time.Duration) {
	st := r.Stats
	secs := r.Window.Seconds()
	fmt.Printf("protocol cost: %d Brokers, %d endpoints, fault-free, %.0fs simulated (after warm-up), %v wall\n\n", brokers, endpoints, secs, wall.Round(time.Second))
	fmt.Printf("workload (cluster-wide, per second):\n")
	fmt.Printf("  sends ok %.0f  delivered %.0f  mints %.0f  transfers %.0f  renews %.0f  reply deliveries %.0f  attacks %.0f\n\n",
		float64(st.SendOK)/secs, float64(st.Delivered)/secs, float64(st.Mints)/secs, float64(st.Transfers)/secs, float64(st.Renews)/secs, float64(st.Replies)/secs, float64(st.Attacks)/secs)
	total, bytes := 0, 0
	for k, v := range st.PeerFrames {
		total += v
		bytes += st.PeerBytes[k]
	}
	fmt.Printf("peer traffic: %.0f frames/s, %.1f KB/s cluster-wide; per Broker %.0f frames/s, %.1f KB/s; %.2f frames per delivered message\n",
		float64(total)/secs, float64(bytes)/secs/1e3, float64(total)/secs/float64(brokers), float64(bytes)/secs/1e3/float64(brokers), float64(total)/float64(max(1, st.Delivered)))
	keys := func(m map[string]int) []string {
		var ks []string
		for k := range m {
			ks = append(ks, k)
		}
		sort.Slice(ks, func(i, j int) bool { return m[ks[i]] > m[ks[j]] })
		return ks
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "  kind\tframes/s\tshare\tavg bytes\tper delivered msg\t")
	for _, k := range keys(st.PeerFrames) {
		v := st.PeerFrames[k]
		fmt.Fprintf(tw, "  %s\t%.1f\t%.1f%%\t%.0f\t%.3f\t\n", k, float64(v)/secs, 100*float64(v)/float64(total), float64(st.PeerBytes[k])/float64(v), float64(v)/float64(max(1, st.Delivered)))
	}
	tw.Flush()
	fmt.Println()
	ops := 0
	for _, v := range st.RCROps {
		ops += v
	}
	fmt.Printf("replication ops: %.0f/s (%.1f per RCR frame)\n", float64(ops)/secs, float64(ops)/float64(max(1, st.PeerFrames["RCR"])))
	for _, k := range keys(st.RCROps) {
		fmt.Printf("  %-14s %8.1f/s\n", k, float64(st.RCROps[k])/secs)
	}
	fmt.Println()
	fmt.Printf("S3 requests per Broker per minute:")
	for _, k := range keys(st.S3Ops) {
		fmt.Printf("  %s %.1f", k, float64(st.S3Ops[k])/secs*60/float64(brokers))
	}
	fmt.Println()
}
