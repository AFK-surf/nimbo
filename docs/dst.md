# Deterministic simulation testing

Nimbo's Broker is built so that the code that runs in production is the code that is simulation-tested. This document describes how, what the simulator injects and checks, and what it found.

## 1. Design for determinism

All Broker logic is in `internal/node` as a single-threaded state machine. It never starts a goroutine, reads a clock, draws randomness or does I/O. Everything comes in as an event and goes out through `node.Env`:

| `Env` | Production (`internal/host`) | Simulation (`internal/sim`) |
|---|---|---|
| `Now()` | `time.Now()` (monotonic) | true simulated time + a constant per-process offset |
| `Wall()` | wall clock | true time + the Broker's current skew, which faults can step |
| `Rand()` | `crypto/rand` | a ChaCha8 stream seeded from (seed, broker, incarnation) |
| `AfterFunc` | `time.AfterFunc`, posted to the loop; zero delay runs after the current event | an event in the simulator's queue |
| `SendPeer` / `OnPeerSendFailed` | lazily dialed peer links; frames never written are reported "not sent" | simulated links (below) |
| `SendSession` / `CloseSession` | WebSocket writer goroutines | simulated connections |
| `S3` | aws-sdk-go-v2 | in-memory store with latency, a 1s-resolution `Date` header and failures |

Inputs are `OnPeerMessage`, `OnPeerSendFailed`, `OnSessionOpen/Frame/Closed`, `OnUnknownPeer`, timers and S3 completions. The node iterates maps only in sorted order wherever the order can reach the outside, so the same inputs always produce the same outputs.

The simulator runs one event queue ordered by (time, sequence number) and delivers every event on one goroutine. Peer frames are CBOR-encoded and decoded on every hop, so the wire codec is exercised too. All randomness comes from the seed. **Every run is reproducible from its seed**: `TestDeterminism` runs a seed twice and compares the SHA-256 of every frame delivered (the trace hash), and any failing seed replays exactly with `nimbo-sim -seed N -n 1 -v`, which prints the full event log including every endpoint frame.

## 2. What is simulated

- **Brokers**: real `node.Node`s. A crash discards the node with all its timers, links and sessions; a restart creates a new node with the same key and a new incarnation. Graceful drains run the real leave protocol.
- **Network**: per-pair links with 0.2–3ms latency and FIFO order (like a TCP stream). A link reset, partition or crash drops frames in flight silently; sending to an unreachable peer fails before anything is written (a dial failure), which the node sees as "not sent". During the fault phase 1% of frames are delayed by 50–500ms.
- **S3**: a strongly consistent object store with 5–60ms latency, `LastModified` and a `Date` header truncated to the second. Faults: a 2% error rate and per-Broker outages of up to 2 minutes.
- **Clocks**: every Broker has a constant skew of up to ±1.5s; faults step one Broker's wall clock by ±5–9s for up to 2 minutes.
- **Endpoints**: simulated clients that speak the real JSON protocol, including the X25519 handshake. They hold 1 or 2 sessions, reconnect with jittered backoff and honour `retry_after_ms`, and run a randomized workload:
  - mint caps (with and without schema, base, auto-reply, `durable`) and grant them to other endpoints by durable transfer, handing the new ID over out of band;
  - send messages (with carried caps, reply options, bursts of three on one cap, deliberate schema violations and base conflicts);
  - reply on single-use reply caps, sometimes through two sessions at once, sometimes moving the reply cap to another endpoint;
  - renew and inspect caps;
  - open streams (§9.7 of the design) on caps with `allow_stream`, and some without. Holders attach at once, a few seconds late, never, or twice with the same token. Targets accept, refuse (4901), ignore, or attach after their token expired. Both sides exchange numbered messages and the holder closes with a random code; some targets close early, and some streams are long-lived (a minute) so that they span faults. Some attaches deliberately use another Broker;
  - attack: use other endpoints' cap IDs and random IDs, and forge handshakes (a proof bound to another authority, or for a different nonce).
- **Stream data plane**: data sockets and mesh data connections are pipes with 0.5–10ms per hop and FIFO order. A crash kills the Broker's legs (endpoints notice within 0.1–3s, the Brokers at the other end of its data connections within 50ms); partitions and link resets break the data connections that cross them.
- **Faults** during the fault phase (defaults; `-fault-scale` multiplies their rate): crashes with restarts, partitions, link resets, S3 outages, clock steps, drains, endpoint disconnects and key rotations. Then a quiet phase heals everything (restarting crashed and departed Brokers) so that liveness can be checked.

## 3. Invariants

Checked continuously (safety):

1. **No forgery.** Any operation on a cap ID the endpoint does not hold fails; forged handshakes are never welcomed.
2. **At most once.** No message is delivered twice; nothing is delivered that was not sent, or whose send failed; a delivered message is never also bounced.
3. **Integrity.** A delivery reaches the cap's target, reports the right lineage (`on`, including reply lineages through moves), carries the sent payload with the base merged in, delivers as many new caps as were carried, and has a reply cap exactly when the target cap has `auto_reply`. Schema violations and base conflicts are always rejected, and only they.
4. **Single use.** A single-use cap succeeds at most once (send or transfer); each reply lineage is delivered at most once.
5. **Expiry.** No cap is accepted (send, transfer, renew or inspect) more than `cap_ttl + grace` (75s, plus 5s of observation slack) after its last use.
6. **TTL.** No message is delivered after its TTL.
7. **Fencing.** A Broker that cannot reach S3, or whose clock is more than `max_skew` off, fences itself within 45–55s.
8. **Streams.**
   - A stream opens only on a cap with `allow_stream`, and `stream_not_allowed` is returned only without it; `inspect` reports the flag.
   - Each token attaches at most once, only on the Broker incarnation that issued it, and never after its half's deadline. Reused, misplaced and late attaches are always refused.
   - Every message on a stream comes from its peer (the holder from the cap's target, and back) on the same stream, in sequence: no loss, duplicate or reorder before the close.
   - A close code an endpoint sends reaches the other side unchanged, with its reason; any other close is a Broker's (49xx, 1001). A stream that carried data is never closed as `unavailable` (fencing keeps established streams).
   - At most one `stream_status` per stream, only to its holder; a failed stream never carries data; an offer is delivered as such only for a `stream`.
   - No half-stream outlives its deadline by more than 2s (checked on every Broker every 5s), and no peer frame of an unknown kind is ever sent.

Checked once the system is stable (liveness and convergence):

9. **Service.** In the second half of the quiet phase (or in a scripted stable window), every send on a live cap to a connected target succeeds and is delivered exactly once, in order per (session, cap).
10. **Replication.** Every cap used in the last 15s is held by every member of its placement set; every settled session's register is on every member of its presence placement set.
11. **Membership.** All live Brokers agree on the member list and none is fenced.
12. **Streams complete.** In the quiet phase, a stream opened on a live cap to a connected target whose two sides attach at once is accepted, every message arrives on both sides, and the target sees the holder's close.

Scripted scenarios (`sim_test.go`) add targeted expectations: a crashed Broker's endpoints keep all their caps once reconnected elsewhere (`TestCrashKeepsCaps`); old caps work again after a partition heals (`TestPartitionHeals`) and after a Broker's S3 outage (`TestS3OutageFences`); rolling restarts faster than S3 failure detection keep every live record fully replicated (`TestRollingRestarts`); endpoints that vanish with their Broker become `target_gone` once the Broker is declared dead plus `cap_ttl` (`TestGoneAfterBrokerCrash`).

## 4. Running it

```sh
go test ./internal/sim/                         # all scenarios + 6 random seeds (~1 min)
NIMBO_DST_SEEDS=200 go test ./internal/sim/ -run TestDST
go run ./cmd/nimbo-sim -seed 1 -n 500           # 500 seeds in parallel, reports failures
go run ./cmd/nimbo-sim -seed 1 -n 200 -fault-scale 3 -brokers 8 -endpoints 30
go run ./cmd/nimbo-sim -seed 1234 -n 1 -v       # replay one seed with the full event log
```

A default run simulates 6 Brokers and 24 endpoints for 9 minutes (4 with faults, 5 quiet), about 12,000 messages, in about 2-3 seconds of CPU.

## 5. Results

With the fixes of §6 in place:

| Batch | Brokers / endpoints | Faults | Seeds | Result |
|---|---|---|---|---|
| default | 6 / 24 | default rates | 700 | all pass |
| harsh | 8 / 30 | 3× default rates | 200 | all pass |
| larger | 12 / 60 | default rates | 60 | all pass |
| scripted scenarios | 6 / 24 | one targeted fault each | 5 × 4 | all pass |

A typical default seed (seed 100) performs about 15,000 sends, 1,500 mints, 4,000 transfers, 1,700 renews and 900 attacks; it injects 3 crashes, 2 partitions, 2 S3 outages, a clock step, 2 drains, 6 key rotations, about 100 endpoint disconnects and 36 link resets; and in the quiet phase it expects, and gets, about 1,350 sends delivered exactly once. The error mix during faults is dominated by the expected cases (`unavailable`, `consumed` from deliberate double use, `schema_violation` and `base_conflict` from deliberate bad payloads, `expired` and `invalid_cap` from long-idle caps).

## 6. What the simulator found

The bugs below were all found by failing seeds while building the Broker; each fix is recorded in design §16.

- **Presence shadowed by tombstones (R1, R2, R4).** Under S3 outages and restarts, a holder could tombstone a live owner's register: the owner came back from fencing with the same incarnation, or a stale S3 member object rolled a peer's view back to an old incarnation. Tombstones then won every merge, so the endpoint looked absent and, a minute later, `gone`: caps to a connected endpoint failed with `target_gone`. Fixed three ways: unfencing takes a new incarnation; Rejoin-learned incarnations beat older S3 objects; the owner's sequence number dominates the merge.
- **Clock steps stretched TTLs (R5).** Deadlines were computed from the wall clock, so a backwards step extended every local deadline and caps were accepted 80s after their last use. Deadlines now use the monotonic clock.
- **Slow skew detection (R6).** The averaged S3 clock offset needed too many samples to notice a clock step. It is now a median of 5.
- **Suspicion outlived restarts (R3).** A restarted Broker stayed suspect for up to 2 minutes, so handoff to it waited and records were under-replicated. Suspicion is now reset on a new incarnation. Found by the replication check after rolling restarts.
- **Blobs outlived by caps (R7).** Moving a single-use reply cap did not count as a use of its base blob, so the new cap failed with `unavailable` once the blob expired.
- Several harness bugs were found the same way (for example, a session's frames arriving after the endpoint had closed it).

## 7. Does it catch bugs? (mutation testing)

Each mutation below was injected into the Broker and the named tests rerun:

| Mutation | Caught by |
|---|---|
| `Consume` always succeeds (double use of single-use caps) | random seeds: hundreds of single-use violations |
| Acceptance ignores `grace` (accept up to 105s) | random seeds: expiry violations |
| Forward timeouts retried (breaks at most once) | random seeds: duplicate deliveries |
| No handoff on membership change | `TestRollingRestarts` replication check |
| Holders never prune a dead owner's registers | `TestGoneAfterBrokerCrash` |
| No self-fencing | `TestS3OutageFences` and random seeds |
| No suspicion reset on restart | `TestRollingRestarts` |
| Stream tokens reusable | random seeds: token attached twice |
| No `allow_stream` check | random seeds: stream opened and paired on a cap without it |
| No holder-half deadline | random seeds: half-stream past its deadline |
| Fencing closes established streams | random seeds: established stream closed as unavailable |
| The holder's Broker pairs any pending half | random seeds: messages of another stream |
| A relay drops a message (simulator) | random seeds: sequence gaps |

Not caught, because other mechanisms cover them (defence in depth): removing `missing` → re-store on touches, removing hinted copies, removing the 2s backstop presence lookup for held messages, and removing the ingress sequencer (the workload's bursts did not produce a cold lookup ahead of a warm one), and letting a holder half pair twice (a stream has exactly one target token, used once).

## 8. Limits

- The simulator replaces the mesh, WebSocket and S3 client layers; those are covered by end-to-end tests over real sockets (`internal/host`, including the Tailscale data plane) rather than by simulation.
- Links are modelled as FIFO streams that either deliver or drop everything after a reset, like TCP. Corruption and duplication at the transport layer are out of scope (WireGuard and TCP exclude them).
- Scale: simulations use 6–12 Brokers and tens of endpoints. Load balance at 100 Brokers is checked separately (`placement` tests).
