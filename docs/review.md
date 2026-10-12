# Nimbo design review: correctness, liveness, robustness

Reviewed: `docs/design.md` revision 4 (XOR placement, no gossip). Two independent passes were merged here: one by the design author and one by a separate reviewer reading the document cold. Load figures come from a simulation of XOR placement.

## Verdict

- **Safety holds** for the core message and capability guarantees, apart from clock skew:
  - at-most-once delivery;
  - unforgeability;
  - schema and base enforcement;
  - the 75s idle bound.
- **Liveness and robustness do not hold as written.** The presence design has one critical flaw and one high one.
  - The critical flaw: one broker missing S3 heartbeats or crashing can permanently kill every cap that targets its endpoints.
  - The high one: presence merges are not conflict-free, so ghost sessions can persist.
- **Lost writes are never repaired.** Records lost or missed by fire-and-forget writes are never re-sent. So the claim that "a record is lost only if its whole placement set fails" is false under partitions, and under a timing window in expiry.
- **The claimed clock independence is false.** Membership and deadline handling depend on broker clocks in ways the doc says they don't.

All findings have contained fixes (§4). Four of them change owner decisions or add notable machinery and need a decision (§5).

---

## 1. Properties checked and found to hold

| Property | Why it holds |
|---|---|
| **At-most-once delivery** | Each message has exactly one owner at a time. Ownership moves only on an explicit `accepted`, and an unknown outcome drops the message. Egress never re-sends after writing to the socket. Watch re-routes move ownership, never copy it. |
| **Unforgeability** (brokers trusted) | IDs are 128-bit random, the record binds the holder, and the handshake proves possession of the private key. An all-zero shared secret is rejected. The authority binding defeats relayed challenges. Content addressing stops blob substitution. |
| **No side channel on transfer** | TransferCap never reads presence or recipient state. |
| **Cap and blob merges are CRDTs** | Immutable content (first write wins) plus a max-merged deadline: commutative, associative and idempotent. Presence is the exception (H1). |
| **75s idle bound** | The deadline is at most last use + 60, acceptance is at most deadline + 15, and a stale cached deadline is re-read before rejecting. This holds with synchronised clocks (see H3). |
| **Schema and base enforcement** | Validation happens at ingress, base keys cannot be overridden (`base_conflict`), and egress trusts ingress. |
| **No consensus or leader** | All writes are local plus asynchronous replication with deterministic merges. Membership comes from S3 with a single writer per object. |

---

## 2. Findings

Severity: **C** critical, **H** high, **M** medium, **L** low. § numbers refer to `design.md`.

### C1. One broker's S3 heartbeat lapse or crash permanently kills caps to its endpoints (§4, §7.4, §8.4.5, §9.1)

1. Broker B serves endpoint T. B's heartbeat PUTs fail for more than 60s, for example from S3 throttling, or B crashes.
2. Presence holders see B as dead and prune its sessions for T (§9.1).
3. `last_seen` is only updated on `Down`, and no `Down` was sent. So it is stale or absent, and T counts as **gone** (§9.1).
4. Every use of a cap targeting T fails with `target_gone` and does not extend its deadline (§8.4.5). Every such cap expires within about 75s.
5. B recovers, or T reconnects elsewhere, but the caps are already gone. With no revocation there is no recovery path either; every holder must be bootstrapped again.

In the crash case, senders get `target_gone` instead of waiting for the reconnect through §9.4.

**Fix:**
- When pruning a session, set `last_seen = prune time`, so the endpoint gets a grace window instead of being gone at once.
- "Gone" must hold **continuously for `cap_ttl`** before a use stops extending the deadline. Until then, return `unavailable` or queue with a watch.
- Keep presence tombstones well beyond `cap_ttl`, for example 10 minutes, so "no record" is rare. Treat "no record" as **unknown**, not gone.
- An owner re-pushes all of its presence whenever its own membership status flips, for example after Rejoin or after its heartbeat recovers.

### H1. The presence merge is not a CRDT, so ghost sessions persist (§7.2, §7.4, §9.1, §9.2.7)

An add/remove set merged by union is not commutative. A handoff copy sent before a `Down`, or a delayed `Up` arriving after its `Down`, re-adds a closed session. The owner is alive with the same incarnation, so the ghost counts as valid indefinitely. The 5-minute reconcile only re-pushes live sessions and never removes ghosts.

Effects:
- The target is never gone, so caps to it never expire.
- Ingress loops through Forward → `not_here` → lookup → the same ghost until the TTL runs out. §9.2.7 has no retry bound.

**Fix:**
- Store presence as a **last-writer-wins register per (endpoint, owner broker, incarnation)**. The owner assigns a monotonic sequence number, and every Up, Down and reconcile carries the owner's full session list for that endpoint. The highest sequence number wins. This is a true CRDT.
- A `not_here` from the owner triggers the owner to re-assert its state (read repair).
- Bound retries to 2 per message.

### H2. Lost or missed writes are never repaired (§7.2, §7.4, §7.5, §8.3)

`Store` and `Touch` are fire-and-forget. Handoff runs only on membership change, and there is no anti-entropy. Four failures follow.

- **(a) Mesh partition with S3 still reachable.** Membership never changes, so nothing re-sends.
  - Caps created on one side get no replicas on the other.
  - Touches don't cross, so holders on the far side drop records that are in use.
  - After the partition heals, nothing triggers re-replication. A cap held only by its issuer is lost when the issuer crashes.
- **(b) Holders drop a record before ingress stops accepting it.**
  - A holder's deadline can lag the true deadline by up to `touch_threshold` (10s), because extensions under 10s are not sent.
  - Holders drop at `holder_deadline + 15s`, which can be as early as last use + 65s. Ingress Brokers with cached copies accept until last use + 75s.
  - In that window, a use is accepted and its Touch reaches holders that no longer have the record. The cap now lives only in one Broker's LRU cache.
- **(c) Blob skipped as "already live".** MintCap skips writing a blob that its local cache says is live (§8.3), but the placement set may already have expired it. The new cap references a schema nobody stores. Step 4 of §8.4 doesn't say how a failed resolution should fail.
- **(d) Mutable state in handoff.** The doc doesn't say that handoff carries mutable state (deadline, presence sequence numbers).

**Fix:**
- **Ack** `Store` and `Touch`, and keep per-peer retry queues (hinted handoff) until the record expires.
- A holder that receives a `Touch` for a key it doesn't hold answers `missing`, and the sender re-`Store`s the full record or blob.
- Run **anti-entropy** between XOR neighbours: per-range digests every few minutes, and immediately when a mesh link to a peer recovers.
- Holders keep records until `deadline + grace + touch_threshold`, closing window (b).
- Cached blobs carry a deadline, and the "already live" check uses it.
- A blob that cannot be resolved fails closed with `unavailable`.
- Handoff carries all mutable state.

### H3. Liveness and expiry depend on broker clocks (§4, §7.5)

§4 says S3 `LastModified` makes liveness clock-independent, but it is compared against the **local** clock. A broker running 61s fast sees every other member as dead, places every record on itself, and prunes all presence.

Deadlines are absolute and merged by maximum, so a broker running N seconds fast extends every cap it touches by N seconds cluster-wide, breaking the 75s bound. The NTP requirement is never checked.

**Fix:**
- Compute the clock offset from the S3 response `Date` header on every poll, and judge member liveness in S3 time.
- Refuse to serve (`closing`) when local skew exceeds 2s.
- Send a remaining TTL in `Touch` and `Store`, not an absolute deadline. Receivers convert it with their own clock and clamp merged deadlines to `now + cap_ttl`.

### H4. S3 failure modes are unspecified (§4)

- **A broker whose own PUTs fail keeps serving while everyone else considers it dead.** Its sessions become a black hole, and this feeds C1.
  - Fix: the broker fences itself, sending `closing` with a jittered `retry_after` to all sessions, once its last successful heartbeat is older than `member_ttl − 15s`.
- **Behaviour when LIST fails is undefined.** Expiring members from a stale listing would collapse the view.
  - Fix: freeze the membership view while polls fail, and expire nobody.
- **Fast join is an amplifier.** Anyone can send WireGuard initiations with random keys; the PSK only stops the handshake from completing. Each one triggers an S3 GET on the hot prefix. That can cause S3 throttling, which then stalls heartbeats and feeds C1.
  - Fix: a negative cache, plus a global rate limit on fast-join GETs.

### H5. Failure detection takes 60–90s, longer than `cap_ttl`, and dial failures are dropped (§4, §7.2, §9.5)

A crashed Broker is excluded only after `member_ttl` plus a poll interval, 60–90s. Until then it stays in every placement set and in presence. A third of writes to its placement sets are silently lost, and routes to it fail.

§9.5 also drops a message on "link failure before reply". That includes the case where the TCP connection never opened, when nothing was sent and retrying is safe.

**Fix:**
- **Local suspicion:** a Broker whose link fails or times out is marked suspect locally. It is skipped as a write target, with the next-nearest Broker taking a hinted copy (H2), and it is queried last on reads. S3 stays authoritative for membership.
- In `Forward`, distinguish "not sent" (retry on the next candidate) from "sent, no reply" (drop).

### M1. Transient lookup misses are reported as permanent errors (§7.3, §8.4, §9.1)

`lookup_fanout = 4` covers only one placement shift. During churn, for example a rolling restart where every node takes a fresh key, or several joins near one key, lookups miss. They then return `invalid_cap` (which tells the holder to bootstrap again) or `target_gone` (which kills the cap).

**Fix:**
- Return `invalid_cap` only when at least 2 members of the current placement set positively answered "not found". Otherwise return `unavailable`, which is retryable.
- During a handoff window, also query the placement set under the previous membership view.

### M2. The issuer's copy cannot be found by ID; cap placement after the issuer dies is undefined (§7.1, §7.3)

A cap's placement is "issuer + 2 nearest", but the issuer can't be derived from the ID. So other Brokers can only find 2 of the 3 copies.

If the issuer dies, or restarts with a fresh key, nothing defines what the third member becomes. The cap may stay at 2 replicas for good.

**Fix (needs owner decision, it changes D13):** the placement set is the 3 Brokers nearest `SHA-256(cap ID)`, and the issuer always keeps a pinned local copy as a cache. Uses on the issuing Broker stay local, and there are 3 findable copies. The cost is one extra remote `Store` per cap.

### M3. Reply-cap volume breaks the memory budget at high message rates (§7.6, §8.3, §15)

Each `auto_reply` delivery creates a record that lives 60–75s. At 100k msg/s that is about 7.5M live reply records, 7.5× the 1M-cap budget, plus 200k remote `Store`s/s.

**Fix (needs owner decision):**
- **Lazy replication:** a reply cap stays only on its issuer until it is either used somewhere else or is more than 2s old. Most replies come back within that time.
- **Optionally, single-use reply caps:** the record is deleted after its first use, unless `auto_reply` asks for a conversation.

### M4. XOR placement load imbalance (§7.1, §7.6)

Simulation: 100 random Broker IDs, 3-nearest placement, uniform keys.

| Virtual IDs per Broker | Max / mean load | Min / mean load |
|---|---|---|
| 1 (as designed) | **2.1×** | 0.3–0.5× |
| 4 | 1.5–1.65× | 0.6× |
| 16 | **1.3×** | 0.8× |

The uniform "~30k records per Broker" in §7.6 understates the busiest Broker by about 2×. Separately, issuer pinning concentrates a busy service's caps on one Broker.

**Fix (needs owner decision):** 16 virtual IDs per Broker, `SHA-256(broker_id ‖ i)`, picking 3 **distinct** Brokers. Ranking a key against them costs 1,600 XORs at 100 Brokers. This is cheap, but it should be cached per key prefix.

### M5. Watches can miss the reconnect (§9.4, §9.2.5)

If the target's `Up` lands before the `Watch` is registered, no `Notify` is ever sent, and the message sits until its TTL runs out. A `Notify` can also be lost, or go to holders from a different placement view. Hints are used only when presence returns nothing, not when a record exists but has no valid session.

**Fix:**
- A Watch is check-then-register: the reply carries the current sessions.
- As a backstop, re-look-up every 2s while a message is held.

### M6. Incarnation is undefined and can repeat (§4, §7.4, §9.1)

Nothing is persisted, and a graceful leave deletes the member object. A restarted Broker therefore can't derive a "newer" incarnation. If it repeats, sessions from its previous life look valid (ghosts), and Rejoin handback is skipped.

**Fix:** use a random 64-bit nonce per process start, and compare by equality, not by order.

### M7. The FIFO claim is not delivered by the pipeline (§9.2, §9.5, §11)

- Validation can block for up to `lookup_wait` (1s). If ingress validates sends in parallel, a later send with a warm cache overtakes an earlier cold one. If it validates serially per session, one cold lookup stalls the whole session.
- `not_here` retries and watch-held messages are overtaken by later messages that are routed directly.

**Fix:**
- Use a per-(session, cap) sequencer at ingress, with different caps proceeding in parallel. While a message to a target is held, later messages to that target queue behind it.
- Alternatively, state that FIFO excludes retried or held messages.

### M8. Gaps in DoS and overload defence (§6.1, §6.4, §10, §7.2)

- **Per-IP limits behind a load balancer.** TLS may terminate at a load balancer (§6.1), so per-IP handshake limits see only the balancer's IP. Fix: require PROXY protocol, or a trusted `X-Forwarded-For`.
- **Uncapped sessions and live caps.** Budgets are per session, but there is no limit on concurrent sessions per IP or on live caps per holder. Each session can keep about 6,000 caps alive just by minting (100/s × 60s), and each costs 2–3 remote Stores. Fix: per-IP session caps, and a per-holder live-cap quota enforced by the issuer.
- **Unbounded replication queues and no global mint admission.** Overload grows the replication backlog without limit. Fix: bounded per-peer queues with backpressure into ingress, and a per-Broker mint admission rate.
- **Held messages and watches have no per-target bound.** Fix: a per-target cap at the holding Broker.
- **Reconnect storms after a Broker crash.** Many endpoints behind one NAT can trip per-IP limits. Fix: token buckets with burst, jittered `retry_after` in `closing`, and jittered exponential backoff specified for SDKs.

### L1. Graceful leave ordering (§4)

The Broker hands off and then deletes its member object. Writes arriving in between are lost, and peers keep routing to it.

**Fix:**
1. Set `draining: true` in the member object and announce it.
2. Peers exclude the Broker as a write target but keep querying it.
3. The Broker hands off its records.
4. The Broker deletes its member object.

### L2. A crash right after a mint loses the cap (§7.2)

The ID is returned before any replica has it. If the issuer crashes within that window, the holder has a dead ID. This is acceptable under the "nothing waits on replication" rule.

**Fix (optional):** a `durable: true` flag on `mint` and `transfer` that waits for one replica ack, for bootstrap flows.

### L3. Cost estimate omission (§4)

Heartbeat PUTs (about 17M a month at 100 Brokers, about $86) are missing from the S3 cost estimate.

---

## 3. Liveness summary

| Liveness property | As written | With fixes |
|---|---|---|
| A message to a connected target is delivered within its TTL (stable membership, no failures) | Holds | Holds |
| … across one Broker crash | **No.** Dial failures are dropped (H5) and false `target_gone` (C1). | Holds after suspicion (≤ RPC timeout) |
| A regularly used cap stays valid | **No.** Window H2(b), partitions H2(a), false `target_gone` (C1). | Holds |
| Records converge to their placement sets after churn or partitions stop | **No.** Only membership changes trigger handoff (H2a). | Holds, through acks, hinted handoff and anti-entropy |
| Presence converges to the true sessions | **No.** Ghosts (H1), stale `last_seen` (C1). | Holds (per-owner LWW plus owner re-assert) |
| Held messages are delivered when the target reconnects | **Racy** (M5) | Holds |

---

## 4. Straightforward fixes

These don't change any owner decision:

- **C1:** grace on prune, continuous-gone rule, longer presence tombstones, owner re-push on status flip.
- **H1:** per-owner LWW presence register, read repair, bounded retries.
- **H2:** acked Store and Touch with hinted handoff, `missing` → re-Store, anti-entropy, longer holder retention, blob deadlines, fail-closed blob resolution, handoff carries mutable state.
- **H3:** S3 `Date` offset, skew fencing, relative TTLs in RPCs.
- **H4:** self-fencing, freezing the view on failed polls, rate-limited fast join.
- **H5:** local suspicion, and retrying sends that were never written.
- **M1:** `invalid_cap` only on positive not-found.
- **M5:** check-then-register watches plus a backstop.
- **M6:** random incarnation nonce.
- **M7:** per-(session, cap) sequencer.
- **M8:** the limits listed under M8.
- **L1:** draining state.
- **L3:** cost note.

## 5. Fixes needing an owner decision

1. **M2:** cap placement becomes 3 nearest by ID, with the issuer keeping a cache copy. This changes D13.
2. **M3:** lazy replication for reply caps, and whether reply caps should be single-use.
3. **M4:** virtual IDs per Broker (16).
4. **H2:** anti-entropy between XOR neighbours. Without it, convergence after a partition cannot be guaranteed. It is the biggest piece of new machinery.

---

## 6. Resolution (design revision 5)

The owner approved all fixes, with two changes: reply caps are **single-use**, and there is **no anti-entropy**. Every long-lived record is refreshed by its writer, so refreshes repair missing copies (design §7.6).

| Finding | Status | Where in design rev 5 |
|---|---|---|
| C1 presence glitch kills caps | Fixed. Prune → tombstone with `absent since = prune time`. "Gone" needs positive evidence of `cap_ttl` continuous absence; unknown ≠ gone. Self-fencing. | §4, §9.1, §8.4 step 5 |
| H1 ghost sessions | Fixed. Per-(endpoint, owner, incarnation) LWW registers carrying full state, read repair on `not_here`, max 2 re-routes. | §9.1, §9.2 step 8 |
| H2 lost writes never repaired | Fixed without anti-entropy: acked Store/Touch with retry queues, hinted copies, `missing` → re-store, holder retention `+touch_threshold`, blob cache TTLs, fail-closed blobs, handoff carries state. | §7.2, §7.4, §7.5, §7.6 |
| H3 clock dependence | Fixed. S3 `Date` offset, liveness in S3 time, fencing beyond 3s skew, relative TTLs clamped on receipt. | §4, §5, §7.2 |
| H4 S3 failure modes | Fixed. Freeze on LIST failure, self-fencing, fast-join negative cache and rate limit. | §4 |
| H5 slow failure detection, dial failures | Fixed. Local suspicion (2s RPC timeout), hinted writes, retry of unsent Forwards. | §5, §7.2, §9.2, §9.5 |
| M1 transient misses reported as permanent | Fixed. Lookup outcomes found / positive not-found / `unavailable`, plus the previous view during handoff. | §7.3 |
| M2 issuer copy not findable | Fixed (D16). 3 nearest by cap ID, plus a pinned issuer cache copy. | §7.1 |
| M3 reply-cap volume | Fixed (D17). Single-use reply caps stored only on the issuer, with no replication. Rejection-sampled IDs make the issuer the nearest Broker, so it is findable and the sole arbiter. | §7.1, §8.3, §8.4 step 6 |
| M4 load imbalance | Fixed (D18). 16 virtual IDs per Broker, ~1.3× max/mean. | §7.1 |
| M5 watch race | Fixed. Check-then-register watches, a 2s backstop re-lookup, hints for unknown targets. | §9.4 |
| M6 incarnation undefined | Fixed. Random 64-bit nonce, equality comparison. | §2, §4 |
| M7 FIFO | Fixed. Per-(session, cap) sequencer; held messages keep their place; claim stated precisely. | §9.2, §9.5 |
| M8 DoS and overload | Fixed. PROXY protocol, per-IP session and handshake limits with burst, per-session created-cap quota, per-Broker mint admission, bounded retry queues with back-pressure, held-message cap, jittered reconnects. | §6.1, §6.4, §10 |
| L1 leave ordering | Fixed. `draining` flag, then closing, handoff, delete. | §4 |
| L2 crash right after mint | Fixed. Opt-in `durable` on mint and transfer. | §6.3 |
| L3 cost omission | Fixed. PUT cost included. | §4 |
