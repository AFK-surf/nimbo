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
