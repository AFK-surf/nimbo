# Benchmarks

Measured on a 4-vCPU Intel Xeon @ 2.80GHz cloud VM (Go 1.27.1, `GOMAXPROCS=4`). In the end-to-end runs, clients, Brokers and the mesh all share those 4 cores. Absolute numbers are therefore a floor, not a capacity statement; the per-operation costs are the useful part.

How to reproduce:

```sh
go test -run x -bench . ./internal/placement ./internal/caprec ./internal/peerproto ./internal/presence ./internal/schema ./pkg/wire
go test -run x -bench . -benchtime 3000x ./internal/node     # Broker CPU per operation
go test -run TestMemoryFootprint -v ./internal/node            # memory per session and per cap
go run ./cmd/nimbo-bench -mesh tcp                             # end to end, in-process cluster
go run ./cmd/nimbo-bench -mesh tailscale                       # same over userspace WireGuard
go run ./cmd/nimbo-bench -urls wss://a/v1/ws,wss://b/v1/ws     # against a real cluster
go run ./cmd/nimbo-sim -account -brokers 100 -endpoints 1000   # protocol cost at 100 Brokers
```

## 1. Hot paths (micro-benchmarks)

| Operation | Cost | Notes |
|---|---|---|
| XOR placement, 3 of 100 Brokers | **0.94 µs** (was 32 µs) | 1,600 virtual IDs; see §5 |
| XOR placement, 3 of 1,000 Brokers | **1.1 µs** (was 384 µs) | |
| Nearest owner (single-use ID sampling), 100 Brokers | 0.28 µs incl. SHA-256 | prefix table; SHA-256 alone is 0.26 µs |
| Cap record encode / decode (CBOR) | 1.5 µs / 2.6 µs | ~160 B |
| Forward frame encode / decode, 1 KiB payload | 2.4 µs / 3.7 µs | |
| RCR batch of 64 touches encode / decode | 14 µs / 32 µs | 0.5 µs per touch to decode |
| Presence merge + classify | 0.7 µs | |
| JSON Schema validation, cached schema, small payload | 7.4 µs | compile: 95 µs, cached by hash |
| Endpoint handshake (Broker side: 2 X25519 + HMAC) | ~160 µs | 237 µs incl. generating the client key in the benchmark |

## 2. Broker CPU per operation

`internal/node/bench_test.go` runs real Brokers in one process with a synchronous environment: no latency, no I/O, every queued event runs at once. Peer frames are CBOR-encoded and decoded on every hop, and a minimal client sends and parses JSON frames. Wall time per operation is therefore the CPU the cluster spends on it. For remote sends, that time is split between two Brokers' loops.

| Operation | 3 Brokers | 100 Brokers | Peer frames |
|---|---|---|---|
| send, sender and target on one Broker | **11 µs** | | 0 |
| send to another Broker | **30 µs** (two loops) | **24 µs** | 2 (Forward + result) |
| send to another Broker, with JSON Schema | 43 µs | | 2 |
| send with auto-reply (creates a single-use reply cap) | 36 µs | **64 µs** (was 3.3 ms) | 2 |
| mint | 31 µs | 42 µs | 6 (3 Stores + acks) |
| mint, `durable` | | 45 µs | 6 |
| transfer (to another endpoint) | | 50 µs | 6 |
| renew | | 4.4 µs | 0 (touches only every 10s of extension) |
| session open + handshake + presence + close | | ~0.7 ms (incl. client X25519) | 6 |

In capacity terms, one Broker loop spends about 12–15 µs per message it handles (ingress or egress). That is roughly **70k messages/s per core**, before WebSocket and TCP costs.

Memory, from `TestMemoryFootprint` (heap, everything included):

| | Bytes |
|---|---|
| one endpoint session, summed over all Brokers (session, register on 3 holders, own register) | ~5.4 KB |
| one minted cap, summed over all copies (issuer + 3 holders) | ~2.6 KB |

At the v1 target, 10k sessions and 30k cap records per Broker come to well under 100 MB.

## 3. End to end (real WebSockets and mesh)

`cmd/nimbo-bench` starts 3 Brokers in-process: real `host` runtime, real WebSocket listeners on loopback, an in-memory S3, and the chosen mesh. It drives them with `pkg/client` endpoints. Pairs are a sender on one Broker and a receiver on another (`-remote`, the default). 256-byte payloads.

TCP mesh, 3 Brokers. These numbers were taken before the held-message fix of §5, but that fix does not change the steady-state path these scenarios measure:

| Scenario | Result |
|---|---|
| connect: 500 sessions, 64 concurrent | 2,266 sessions/s; handshake p50 21 ms, p99 76 ms (CPU-bound) |
| mint / durable mint / durable transfer, sequential | p50 170 µs / 318 µs / 318 µs; p99 360 / 540 / 507 µs |
| throughput: 32 pairs × 16 in flight | **~26–30k messages/s** delivered, 0 lost; latency under saturation p50 17 ms (Little's law: 512 in flight) |
| open loop, 2,000 msgs/s | send→deliver **p50 0.75 ms, p99 1.7 ms, p99.9 3.9 ms** |
| request/reply on single-use reply caps, 32 in flight | ~13k round trips/s; RTT p50 2.1 ms, p99 9 ms |

Before the held-message fix, the same open-loop scenario lost 10–30% of messages (§5).

The Tailscale mesh runs the same code over userspace WireGuard and gVisor netstack. It adds per-packet encryption and a userspace TCP stack, so expect lower throughput on the same cores. Run `nimbo-bench -mesh tailscale` to measure it. Its warm-up without a DERP relay takes 5–30 s while WireGuard paths form (see the design's DERP requirement).

## 4. Protocol cost at 100 Brokers (simulation)

`nimbo-sim -account` runs the deterministic simulator fault-free, resets its counters after a 2-minute warm-up, and counts every peer frame and replication op. The workload is the DST mix: each endpoint acts about every 0.7 s, holds up to 48 caps, and uses most of them rarely. That is a heavy, cache-unfriendly mix, so treat the results as an upper bound on control traffic per message.

100 Brokers, 1,000 endpoints, 180 s measured:

| | |
|---|---|
| workload | 1,130 sends/s, 113 mints/s, 318 transfers/s, 142 renews/s, 355 reply deliveries/s |
| peer traffic | 25.7k frames/s cluster-wide = **257 frames/s and 27 KB/s per Broker**; **22.7 frames per delivered message** |
| S3 per Broker | 3.8 PUT + 2.0 LIST per minute (heartbeat 15 s, poll 30 s) |

| Frame kind | Per delivered message | Share |
|---|---|---|
| GetResult | 5.3 | 23% |
| RCR (replication batches) + acks | 5.2 + 5.2 | 45% |
| Get (presence) | 2.3 | 10% |
| Get (cap) | 2.2 | 10% |
| Forward + result | 0.8 + 0.8 | 7% |
| Get (blob) | 0.75 | 3% |
| Consume + result | 0.09 + 0.09 | <1% |

| Replication op | Per second |
|---|---|
| Touch | 3,879 |
| Store (cap) | 1,689 |
| Store (blob) | 349 |

Where the frames go:

- **Lookups** are about 45% of frames. Every cap, blob or presence lookup fans out to 4 Brokers in parallel (`lookup_fanout`). Cap lookups happen whenever a cap is first used on a Broker that has never seen it; presence lookups happen whenever the 30 s location cache misses.
- **Touches** dominate replication. A cap used less often than every 10 s sends a touch to its 3 holders on every use; this workload uses most caps rarely.
- **Coalescing** reaches only 1.0 op per RCR frame at this rate (59 ops/s per Broker spread over 99 peers). It pays off only at much higher per-peer rates.

With 6 Brokers and 24 endpoints the same workload costs 11 frames per delivered message.

## 5. What benchmarking found and fixed

1. **XOR placement was a linear scan** over every virtual ID (32 µs at 100 Brokers, 384 µs at 1,000), and it runs for every replication, lookup and presence push.
   - It now walks the sorted virtual IDs as a binary trie in XOR order: 0.9–1.1 µs, verified against brute force.
   - Virtual IDs are memoized, so a membership change no longer rehashes 1,600 IDs on every Broker.
2. **Reply caps cost 3.3 ms at 100 Brokers.** Single-use ID rejection sampling takes about one try per Broker, and each try used the slow placement.
   - It now uses a 64K-entry prefix table that decides the nearest owner for most keys, and draws randomness in batches.
   - Result: 64 µs per auto-reply send, mostly 100 SHA-256s.
3. **Held messages drained one per round trip.** Once a target's messages were held, everything sent to it afterwards queued behind them (FIFO), and the queue drained one Forward per RTT. Under 2,000 msgs/s the queue never emptied: 10–30% of messages bounced with `receiver_busy` or `ttl_expired`. Fixed in three parts:
   - held messages are forwarded all at once, in order on the link;
   - suspected peers are probed every second, so suspicion ends as soon as they answer, and held messages are re-evaluated then;
   - the host reconfigures the mesh synchronously on membership changes. The asynchronous update raced the first dial to a new Broker and made it suspect, which is what triggered the holds.

   After the fix the scenario delivers 100% with no bounces.

## 6. Where the next gains are

- **Lookup fan-out:** querying the nearest holder first and fanning out only on miss or timeout (hedged requests) would cut most of the 45% of frames spent on lookups.
- **Records travelling with transfers:** the transferring Broker could push a new cap record to the recipient's current Broker, avoiding the first-use lookup.
- **JSON:** about 40% of the local send path is `encoding/json` (frame parse, payload object check, result and deliver encoding). Passing the payload through without a map round trip when the cap has no base would save several µs per message.
- **Handshakes:** X25519 runs on the Broker loop (~160 µs per session). A reconnect storm of 10k sessions costs about 1.6 s of loop time; moving the handshake to the session goroutine would take it off the loop.
