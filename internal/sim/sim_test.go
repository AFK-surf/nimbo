package sim

import (
	"os"
	"strconv"
	"testing"
	"time"
)

func smallConfig(seed uint64) Config {
	return Config{
		Seed:       seed,
		Brokers:    6,
		Endpoints:  24,
		FaultPhase: 4 * time.Minute,
		Quiet:      5 * time.Minute,
		Faults:     DefaultFaults(),
	}
}

func runSeed(t *testing.T, cfg Config) *Result {
	t.Helper()
	if os.Getenv("NIMBO_SIM_LOG") != "" {
		cfg.Log = os.Stderr
	}
	res := New(cfg).Run()
	t.Log(res.String())
	if len(res.Violations) > 0 {
		t.Fatalf("seed %d: %d violations", cfg.Seed, len(res.Violations))
	}
	return res
}

func TestNoFaults(t *testing.T) {
	cfg := smallConfig(1)
	cfg.Faults = Faults{}
	res := runSeed(t, cfg)
	if res.Stats.Expected == 0 || res.Stats.Delivered == 0 {
		t.Fatalf("workload did nothing: %+v", res.Stats)
	}
}

func TestDeterminism(t *testing.T) {
	cfg := smallConfig(7)
	cfg.FaultPhase, cfg.Quiet = 2*time.Minute, 3*time.Minute
	a := New(cfg).Run()
	b := New(cfg).Run()
	if a.Trace != b.Trace || a.Steps != b.Steps {
		t.Fatalf("same seed, different executions: %s/%d vs %s/%d", a.Trace, a.Steps, b.Trace, b.Steps)
	}
}

// TestDST runs randomized fault schedules. NIMBO_DST_SEEDS sets how many
// (default 6) and NIMBO_DST_SEED_BASE the first seed.
func TestDST(t *testing.T) {
	n, base := 6, uint64(100)
	if testing.Short() {
		n = 2
	}
	if v, err := strconv.Atoi(os.Getenv("NIMBO_DST_SEEDS")); err == nil {
		n = v
	}
	if v, err := strconv.ParseUint(os.Getenv("NIMBO_DST_SEED_BASE"), 10, 64); err == nil {
		base = v
	}
	for i := 0; i < n; i++ {
		seed := base + uint64(i)
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			t.Parallel()
			runSeed(t, smallConfig(seed))
		})
	}
}

// scripted runs a fault-free workload with one scripted fault.
func scripted(seed uint64, script func(w *World)) Config {
	cfg := smallConfig(seed)
	cfg.Faults = Faults{}
	cfg.FaultPhase, cfg.Quiet = 4*time.Minute, 3*time.Minute
	cfg.Script = script
	return cfg
}

// A crashed Broker's endpoints, once reconnected elsewhere, keep all their
// caps (design §11 property).
func TestCrashKeepsCaps(t *testing.T) {
	for seed := uint64(1); seed <= seeds(); seed++ {
		runSeed(t, scripted(seed, func(w *World) {
			w.Crash(90*time.Second, int(seed)%6, 0)
			// Endpoints reconnect within ~30s (backoff); from then on, all
			// caps obtained 5s before the crash must keep working.
			w.ExpectStable(140*time.Second, 0, 85*time.Second)
		}))
	}
}

// After a partition heals, old caps and presence work again.
func TestPartitionHeals(t *testing.T) {
	for seed := uint64(1); seed <= seeds(); seed++ {
		runSeed(t, scripted(seed, func(w *World) {
			w.Partition(80*time.Second, 30*time.Second, 0, 1)
			w.ExpectStable(150*time.Second, 0, 75*time.Second)
		}))
	}
}

// A Broker that loses S3 fences itself; its endpoints move and keep their
// caps.
func TestS3OutageFences(t *testing.T) {
	for seed := uint64(1); seed <= seeds(); seed++ {
		runSeed(t, scripted(seed, func(w *World) {
			w.BreakS3(80*time.Second, 120*time.Second, int(seed)%6)
			w.ExpectStable(170*time.Second, 0, 75*time.Second)
		}))
	}
}

// Rolling restarts: each Broker restarts in turn, faster than S3 failure
// detection. Handoff on Rejoin must keep every record fully replicated, so
// caps used within their TTL survive.
func TestRollingRestarts(t *testing.T) {
	for seed := uint64(1); seed <= seeds(); seed++ {
		runSeed(t, scripted(seed, func(w *World) {
			for i := 0; i < 6; i++ {
				w.RestartBroker(80*time.Second+time.Duration(i)*8*time.Second, i, 3*time.Second)
			}
			w.CheckReplication(80*time.Second + 6*8*time.Second + 10*time.Second)
			w.ExpectStable(170*time.Second, 0, 75*time.Second)
		}))
	}
}

// Endpoints that vanish together with their Broker become "gone" once the
// Broker is declared dead (60-90s) plus cap_ttl: their caps fail with
// target_gone instead of living forever (review C1).
func TestGoneAfterBrokerCrash(t *testing.T) {
	for seed := uint64(1); seed <= seeds(); seed++ {
		res := runSeed(t, scripted(seed, func(w *World) {
			w.CrashWithEndpoints(60*time.Second, int(seed)%6)
			w.ExpectGone(60*time.Second + 95*time.Second + 65*time.Second)
		}))
		if res.Stats.ExpectedGone == 0 {
			t.Errorf("seed %d: no send exercised a gone target", seed)
		}
	}
}

func seeds() uint64 {
	if testing.Short() {
		return 1
	}
	return 4
}

// Scale-up: Brokers join a running cluster while S3 is slow, so member
// LISTs are often in flight when a joiner's Rejoin arrives. Membership,
// presence and caps must stay correct throughout.
func TestScaleUp(t *testing.T) {
	for seed := uint64(1); seed <= seeds(); seed++ {
		cfg := scripted(seed, nil)
		cfg.S3Latency = [2]time.Duration{500 * time.Millisecond, 3 * time.Second}
		cfg.Script = func(w *World) {
			for i := 0; i < 4; i++ {
				w.AddBroker(90*time.Second + time.Duration(i)*700*time.Millisecond)
			}
			w.CheckReplication(150 * time.Second)
			w.ExpectStable(150*time.Second, 0, 85*time.Second)
		}
		runSeed(t, cfg)
	}
}
