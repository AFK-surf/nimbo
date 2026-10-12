// Command nimbo-bench load-tests a Nimbo cluster end to end: real
// WebSocket endpoints (pkg/client), real Brokers (internal/host) and a real
// mesh. By default it starts an in-process cluster on loopback with an
// in-memory S3; with -urls it drives an existing cluster instead.
//
//	nimbo-bench -brokers 3 -mesh tcp -pairs 32 -window 16 -duration 20s
//	nimbo-bench -mesh tailscale -rate 2000 -duration 30s
//	nimbo-bench -urls wss://a/v1/ws,wss://b/v1/ws -pairs 64
//
// Scenarios: connect (session setup rate), caps (mint, durable mint,
// durable transfer latency), throughput (closed loop: pairs × window
// messages in flight), latency (open loop at -rate msgs/s), and reply
// (request/reply round trips on single-use reply caps).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/afk-surf/nimbo/internal/config"
	"github.com/afk-surf/nimbo/internal/host"
	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/internal/mesh"
	"github.com/afk-surf/nimbo/internal/node"
	"github.com/afk-surf/nimbo/internal/s3store"
	"github.com/afk-surf/nimbo/pkg/client"
	"github.com/afk-surf/nimbo/pkg/wire"
)

var (
	flagBrokers  = flag.Int("brokers", 3, "in-process Brokers")
	flagMesh     = flag.String("mesh", "tcp", "in-process mesh: tcp or tailscale")
	flagURLs     = flag.String("urls", "", "comma-separated wss:// URLs of an existing cluster (skips the in-process cluster)")
	flagPairs    = flag.Int("pairs", 32, "sender/receiver endpoint pairs")
	flagWindow   = flag.Int("window", 16, "messages in flight per pair (throughput)")
	flagDuration = flag.Duration("duration", 15*time.Second, "duration of each timed scenario")
	flagRate     = flag.Int("rate", 2000, "messages per second for the latency scenario")
	flagPayload  = flag.Int("payload", 256, "payload size in bytes")
	flagRemote   = flag.Bool("remote", true, "receivers on a different Broker than senders")
	flagConnect  = flag.Int("connect", 500, "sessions to open in the connect scenario")
	flagScen     = flag.String("scenarios", "connect,caps,throughput,latency,reply", "scenarios to run")
	flagSchema   = flag.Bool("schema", false, "mint caps with a JSON Schema (validated on every send)")
	flagProfile  = flag.String("cpuprofile", "", "write a CPU profile of the whole process")
)

func main() {
	flag.Parse()
	ctx := context.Background()
	urls := splitList(*flagURLs)
	var stop func()
	if len(urls) == 0 {
		urls, stop = startCluster(*flagBrokers, *flagMesh)
		defer stop()
	}
	fmt.Printf("nimbo-bench: %d Brokers (%s), %d CPUs, GOMAXPROCS=%d, payload %dB, remote=%v\n",
		len(urls), meshName(), runtime.NumCPU(), runtime.GOMAXPROCS(0), *flagPayload, *flagRemote)
	warmUp(ctx, urls)
	if *flagProfile != "" {
		f, err := os.Create(*flagProfile)
		must(err)
		must(pprof.StartCPUProfile(f))
		defer pprof.StopCPUProfile()
	}
	for _, s := range splitList(*flagScen) {
		switch s {
		case "connect":
			benchConnect(ctx, urls)
		case "caps":
			benchCaps(ctx, urls)
		case "throughput":
			benchThroughput(ctx, urls)
		case "latency":
			benchLatency(ctx, urls)
		case "reply":
			benchReply(ctx, urls)
		default:
			fmt.Println("unknown scenario", s)
		}
		brokerStats(s)
		bounces.Range(func(k, v any) bool {
			fmt.Printf("  bounces %s: %d\n", k, v.(*atomic.Int64).Load())
			return true
		})
	}
}

func meshName() string {
	if *flagURLs != "" {
		return "external"
	}
	return *flagMesh + " mesh, in-process"
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// --- in-process cluster ---

var hosts []*host.Host

func brokerStats(label string) {
	if len(hosts) == 0 {
		return
	}
	var t node.Snapshot
	for _, h := range hosts {
		s := h.Snapshot()
		t.Delivered += s.Delivered
		t.Forwarded += s.Forwarded
		t.Dropped += s.Dropped
		t.Held += s.Held
		t.Bounced += s.Bounced
		t.LookupsRemote += s.LookupsRemote
		t.LookupsUnresolved += s.LookupsUnresolved
		t.HintedWrites += s.HintedWrites
		t.QueuedOps += s.QueuedOps
	}
	fmt.Printf("  [brokers after %s] delivered %d forwarded %d dropped %d held %d bounced %d lookups %d unresolved %d hinted %d queued %d\n",
		label, t.Delivered, t.Forwarded, t.Dropped, t.Held, t.Bounced, t.LookupsRemote, t.LookupsUnresolved, t.HintedWrites, t.QueuedOps)
}

var bounces sync.Map // reason -> *atomic.Int64

func countBounce(b client.Bounce) {
	v, _ := bounces.LoadOrStore(b.Reason, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

func startCluster(n int, meshKind string) ([]string, func()) {
	store := s3store.NewMem()
	psk := make([]byte, 32)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	var urls []string
	var closers []func()
	for i := 0; i < n; i++ {
		k, err := config.GenerateNodeKey()
		must(err)
		id := ids.BrokerIDFromPub(k.Public[:])
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		must(err)
		authority := ln.Addr().String()
		url := "ws://" + authority + "/v1/ws"
		var hm host.Mesh
		var endpoints []string
		var ref struct {
			sync.Mutex
			h *host.Host
		}
		withHost := func(f func(h *host.Host)) {
			ref.Lock()
			h := ref.h
			ref.Unlock()
			if h != nil {
				f(h)
			}
		}
		var disco string
		switch meshKind {
		case "tcp":
			tm, err := mesh.NewTCP(id, psk, "127.0.0.1:0")
			must(err)
			hm, endpoints = tm, []string{tm.Addr().String()}
		case "tailscale":
			ts, err := mesh.NewTS(mesh.TSConfig{
				NodePrivate:   k.Private,
				PSK:           psk,
				OnUnknownPeer: func(id ids.BrokerID) { withHost(func(h *host.Host) { h.UnknownPeer(id) }) },
				OnLocalInfo: func(d string, eps []string, _ int) {
					disco = d
					withHost(func(h *host.Host) {
						h.Do(func() {
							h.Node().UpdateMemberInfo(func(mi *node.MemberInfo) {
								mi.DiscoPub = d
								if len(eps) > 0 {
									mi.Endpoints = eps
								}
							})
						})
					})
				},
			})
			must(err)
			hm, endpoints = ts, []string{fmt.Sprintf("127.0.0.1:%d", ts.LocalPort())}
		default:
			must(fmt.Errorf("unknown mesh %q", meshKind))
		}
		lim := node.DefaultLimits()
		// One benchmark host runs every endpoint from one IP.
		lim.SessionsPerIP, lim.HandshakesPerSec, lim.HandshakeBurst = 1<<30, 1e9, 1e9
		lim.ActionsPerSec, lim.ActionsBurst, lim.MintsPerSec, lim.MintsBurst = 1e9, 1e9, 1e9, 1e9
		h := host.New(host.Options{
			Node: node.Config{
				NodePub:     k.Public[:],
				Member:      node.MemberInfo{WSURLs: []string{url}, Endpoints: endpoints, DiscoPub: disco},
				PublicNames: []string{authority},
				Tunables:    node.DefaultTunables(),
				Limits:      lim,
			},
			Store:  store,
			Mesh:   hm,
			Logger: logger,
		})
		ref.Lock()
		ref.h = h
		ref.Unlock()
		srv := &http.Server{Handler: h}
		go srv.Serve(ln)
		h.Start()
		hosts = append(hosts, h)
		urls = append(urls, url)
		closers = append(closers, func() { srv.Close(); h.Close() })
	}
	return urls, func() {
		for _, c := range closers {
			c()
		}
	}
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "nimbo-bench:", err)
		os.Exit(1)
	}
}

// --- endpoints ---

type ep struct {
	c   *client.Client
	key client.Key
	in  chan client.Delivery
}

func dial(ctx context.Context, url string, buf int) *ep {
	k, err := client.GenerateKey()
	must(err)
	e := &ep{key: k, in: make(chan client.Delivery, buf)}
	e.c, err = client.Dial(ctx, client.Options{URL: url, Key: k, OnDeliver: func(d client.Delivery) { e.in <- d }, OnBounce: countBounce})
	must(err)
	return e
}

// retry runs f until it does not fail with unavailable (a cold mesh).
func retry[T any](ctx context.Context, f func() (T, error)) T {
	deadline := time.Now().Add(90 * time.Second)
	for {
		v, err := f()
		if err == nil {
			return v
		}
		if !strings.Contains(err.Error(), "unavailable") || time.Now().After(deadline) {
			must(err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// warmUp waits until every pair of Brokers can serve a durable grant.
func warmUp(ctx context.Context, urls []string) {
	t0 := time.Now()
	for i := range urls {
		a := dial(ctx, urls[i], 1)
		b := dial(ctx, urls[(i+1)%len(urls)], 1)
		retry(ctx, func() (string, error) { return grant(ctx, a, b, false) })
		a.c.Close()
		b.c.Close()
	}
	fmt.Printf("warm-up: %v\n\n", time.Since(t0).Round(time.Millisecond))
}

func grant(ctx context.Context, owner, holder *ep, autoReply bool) (string, error) {
	o := client.MintOptions{AutoReply: autoReply, Durable: true}
	if *flagSchema {
		o.Schema = json.RawMessage(`{"type":"object","required":["t","p"],"properties":{"t":{"type":"integer"},"p":{"type":"string"}}}`)
	}
	c, err := owner.c.Mint(ctx, o)
	if err != nil {
		return "", err
	}
	return owner.c.Transfer(ctx, c, holder.key.PublicString(), true)
}

// --- statistics ---

type hist struct {
	mu sync.Mutex
	v  []time.Duration
}

func (h *hist) add(d time.Duration) {
	h.mu.Lock()
	h.v = append(h.v, d)
	h.mu.Unlock()
}

func (h *hist) summary() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.v) == 0 {
		return "no samples"
	}
	slices.Sort(h.v)
	p := func(q float64) time.Duration {
		i := int(math.Ceil(q*float64(len(h.v)))) - 1
		return h.v[max(0, min(i, len(h.v)-1))]
	}
	r := func(d time.Duration) string {
		switch {
		case d < time.Millisecond:
			return fmt.Sprintf("%dµs", d.Microseconds())
		default:
			return fmt.Sprintf("%.2fms", float64(d.Microseconds())/1000)
		}
	}
	return fmt.Sprintf("p50 %s  p90 %s  p99 %s  p99.9 %s  max %s  (n=%d)", r(p(.5)), r(p(.9)), r(p(.99)), r(p(.999)), r(h.v[len(h.v)-1]), len(h.v))
}

// --- scenarios ---

func benchConnect(ctx context.Context, urls []string) {
	n := *flagConnect
	conc := 64
	var h hist
	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var cs []*client.Client
	t0 := time.Now()
	for i := 0; i < n; i++ {
		sem <- struct{}{}
		wg.Add(1)
		go func(i int) {
			defer func() { <-sem; wg.Done() }()
			k, _ := client.GenerateKey()
			s := time.Now()
			c, err := client.Dial(ctx, client.Options{URL: urls[i%len(urls)], Key: k})
			must(err)
			h.add(time.Since(s))
			mu.Lock()
			cs = append(cs, c)
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	el := time.Since(t0)
	fmt.Printf("connect: %d sessions (64 concurrent) in %v = %.0f sessions/s\n  handshake latency: %s\n\n", n, el.Round(time.Millisecond), float64(n)/el.Seconds(), h.summary())
	for _, c := range cs {
		c.Close()
	}
}

func benchCaps(ctx context.Context, urls []string) {
	a := dial(ctx, urls[0], 1)
	b := dial(ctx, urls[len(urls)-1], 1)
	defer a.c.Close()
	defer b.c.Close()
	const n = 300
	var mint, mintD, xfer hist
	for i := 0; i < n; i++ {
		s := time.Now()
		_, err := a.c.Mint(ctx, client.MintOptions{})
		must(err)
		mint.add(time.Since(s))
		s = time.Now()
		c, err := a.c.Mint(ctx, client.MintOptions{Durable: true})
		must(err)
		mintD.add(time.Since(s))
		s = time.Now()
		_, err = a.c.Transfer(ctx, c, b.key.PublicString(), true)
		must(err)
		xfer.add(time.Since(s))
	}
	fmt.Printf("caps (sequential, %d each):\n  mint            %s\n  mint durable    %s\n  transfer durable %s\n\n", n, mint.summary(), mintD.summary(), xfer.summary())
}

// pair: a sender holding a cap that targets its receiver.
type pair struct {
	snd, rcv *ep
	capID    string
}

func makePairs(ctx context.Context, urls []string, n int, autoReply bool, buf int) []*pair {
	ps := make([]*pair, n)
	var wg sync.WaitGroup
	for i := range ps {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ri := i % len(urls)
			si := ri
			if *flagRemote && len(urls) > 1 {
				si = (ri + 1) % len(urls)
			}
			p := &pair{rcv: dial(ctx, urls[ri], buf), snd: dial(ctx, urls[si], 1)}
			p.capID = retry(ctx, func() (string, error) {
				o := client.MintOptions{AutoReply: autoReply, Durable: true}
				if *flagSchema {
					o.Schema = json.RawMessage(`{"type":"object","required":["t","p"],"properties":{"t":{"type":"integer"},"p":{"type":"string"}}}`)
				}
				c, err := p.rcv.c.Mint(ctx, o)
				if err != nil {
					return "", err
				}
				return p.rcv.c.Transfer(ctx, c, p.snd.key.PublicString(), true)
			})
			ps[i] = p
		}(i)
	}
	wg.Wait()
	return ps
}

func closePairs(ps []*pair) {
	for _, p := range ps {
		p.snd.c.Close()
		p.rcv.c.Close()
	}
}

var pad string

func payload(t time.Time) map[string]any {
	if pad == "" {
		pad = strings.Repeat("x", max(0, *flagPayload-40))
	}
	return map[string]any{"t": t.UnixNano(), "p": pad}
}

func sentAt(d client.Delivery) time.Time {
	var p struct {
		T int64 `json:"t"`
	}
	json.Unmarshal(d.Payload, &p)
	return time.Unix(0, p.T)
}

func benchThroughput(ctx context.Context, urls []string) {
	ps := makePairs(ctx, urls, *flagPairs, false, *flagWindow*2)
	defer closePairs(ps)
	var delivered, sent, failed atomic.Int64
	var lat hist
	end := time.Now().Add(*flagDuration)
	var wg sync.WaitGroup
	for _, p := range ps {
		// Receiver: count and timestamp deliveries; release the window.
		window := make(chan struct{}, *flagWindow)
		wg.Add(1)
		go func(p *pair) {
			defer wg.Done()
			for {
				select {
				case d := <-p.rcv.in:
					lat.add(time.Since(sentAt(d)))
					delivered.Add(1)
					<-window
				case <-time.After(time.Until(end) + 2*time.Second):
					return
				}
			}
		}(p)
		for w := 0; w < *flagWindow; w++ {
			wg.Add(1)
			go func(p *pair) {
				defer wg.Done()
				for time.Now().Before(end) {
					window <- struct{}{}
					if _, err := p.snd.c.Send(ctx, p.capID, payload(time.Now()), client.SendOptions{}); err != nil {
						failed.Add(1)
						<-window
						continue
					}
					sent.Add(1)
				}
			}(p)
		}
	}
	t0 := time.Now()
	wg.Wait()
	el := min(time.Since(t0), *flagDuration)
	fmt.Printf("throughput: %d pairs × window %d, %v: sent %d, delivered %d, failed %d\n  %.0f msgs/s delivered (%.1f MB/s payload)\n  send→deliver latency under load: %s\n\n",
		len(ps), *flagWindow, flagDuration.Round(time.Second), sent.Load(), delivered.Load(), failed.Load(),
		float64(delivered.Load())/el.Seconds(), float64(delivered.Load())*float64(*flagPayload)/el.Seconds()/1e6, lat.summary())
}

func benchLatency(ctx context.Context, urls []string) {
	ps := makePairs(ctx, urls, *flagPairs, false, 1024)
	defer closePairs(ps)
	var lat, ack hist
	var delivered atomic.Int64
	end := time.Now().Add(*flagDuration)
	var wg sync.WaitGroup
	for _, p := range ps {
		wg.Add(1)
		go func(p *pair) {
			defer wg.Done()
			for {
				select {
				case d := <-p.rcv.in:
					lat.add(time.Since(sentAt(d)))
					delivered.Add(1)
				case <-time.After(time.Until(end) + 2*time.Second):
					return
				}
			}
		}(p)
	}
	// Open loop: messages go out on a fixed schedule, independent of
	// responses, and latency is measured from the scheduled time (no
	// coordinated omission).
	interval := time.Second / time.Duration(*flagRate)
	var sent atomic.Int64
	next := time.Now()
	i := 0
	for {
		now := time.Now()
		if now.After(end) {
			break
		}
		for !next.After(now) {
			p := ps[i%len(ps)]
			i++
			go func(s time.Time) {
				if _, err := p.snd.c.Send(ctx, p.capID, payload(s), client.SendOptions{}); err == nil {
					ack.add(time.Since(s))
					sent.Add(1)
				}
			}(next)
			next = next.Add(interval)
		}
		time.Sleep(time.Until(next))
	}
	wg.Wait()
	fmt.Printf("latency: open loop at %d msgs/s over %d pairs for %v: sent %d, delivered %d\n  send result (ack):  %s\n  send→deliver:       %s\n\n",
		*flagRate, len(ps), flagDuration.Round(time.Second), sent.Load(), delivered.Load(), ack.summary(), lat.summary())
}

func benchReply(ctx context.Context, urls []string) {
	ps := makePairs(ctx, urls, *flagPairs, true, 64)
	defer closePairs(ps)
	// Receivers answer every request on its single-use reply cap.
	for _, p := range ps {
		go func(p *pair) {
			for d := range p.rcv.in {
				if d.Reply != "" {
					p.rcv.c.Send(ctx, d.Reply, map[string]any{"ok": true, "t": sentAt(d).UnixNano()}, client.SendOptions{})
				}
			}
		}(p)
	}
	var rtt hist
	var done, lost atomic.Int64
	end := time.Now().Add(*flagDuration)
	var wg sync.WaitGroup
	for _, p := range ps {
		wg.Add(1)
		go func(p *pair) {
			defer wg.Done()
			for time.Now().Before(end) {
				s := time.Now()
				if _, err := p.snd.c.Send(ctx, p.capID, payload(s), client.SendOptions{Reply: &wire.ReplyOpts{}}); err != nil {
					continue
				}
				select {
				case <-p.snd.in:
					rtt.add(time.Since(s))
					done.Add(1)
				case <-time.After(5 * time.Second):
					lost.Add(1)
				}
			}
		}(p)
	}
	wg.Wait()
	fmt.Printf("reply: %d pairs, one request in flight each, %v: %d round trips = %.0f req/s, %d lost\n  request→reply RTT (2 messages, 1 single-use cap): %s\n\n",
		len(ps), flagDuration.Round(time.Second), done.Load(), float64(done.Load())/flagDuration.Seconds(), lost.Load(), rtt.summary())
}
