# Nimbo design (v1)

Nimbo is a distributed, capability-based messaging fabric for AI agents, isolated runtimes and external devices. This document refines the concept notes into a design that can be built. It is written in Go and ships as one binary, `nimbod`.

Status: revision 9, implemented in this repository. Revision 5 incorporated the fixes from the [design review](review.md). Revision 6 records what building and simulation-testing it changed; those changes are listed in [§16](#16-changes-from-implementation-and-dst-revision-6) and marked **(rev 6)** where they apply. Revision 7 added the membership signal (§4). Revision 8 adds streams ([§9.7](#97-streams-rev-8)), marked **(rev 8)**; what building them changed from the proposal is in [§17](#17-streams-as-built-revision-8). Revision 9 adds delegated issuance ([§8.6](#86-delegated-issuance-rev-9)), marked **(rev 9)**. The decisions taken with the project owner are listed in [§13](#13-decision-log). Defaults chosen without the owner, which are open to change, are in [§14](#14-defaults-open-to-change).

---

## 1. Goals and non-goals

**Goals**

- One standalone service, `nimbod` (the Broker). Brokers form a Cluster peer to peer, with no consensus and no leader.
- Endpoints are permissionless and untrusted. Connecting to a Broker grants nothing. All authority comes from Capabilities.
- Sending to an Endpoint looks the same to the sender whether that Endpoint is on the same Broker or a different one.
- No control state is replicated to every Broker. Each record (a cap, an endpoint's presence, a schema or base blob) lives on a small, fixed number of Brokers chosen by XOR distance. The cost of a record is the same whatever the Cluster size.
- **Convergence by refresh.** Every record that outlives `cap_ttl` is periodically re-sent by whoever writes it. Records that are not refreshed expire. Replicas therefore converge without background anti-entropy (§7.6).
- Endpoints range from multi-tenant services to ESP32 devices. The endpoint protocol must be implementable with only X25519, HMAC-SHA256, JSON and WebSocket.

**v1 scale target:** ~100 Brokers, ~1M concurrent Endpoints and ~1M long-lived Capabilities in active use across the Cluster. Nothing in the design replicates per-endpoint or per-cap state to every Broker. Larger clusters are limited mainly by S3 membership polling and by every Broker knowing the full member list, which stays manageable into the low thousands.

**Non-goals (v1):** durable or at-least-once delivery, message payloads other than JSON (streams, §9.7, carry opaque bytes on separate data sockets), revocation, end-to-end payload encryption (possible later, because endpoint keys are X25519), multi-cluster federation, and a materialised cluster-wide view on any single Broker.

---

## 2. Glossary

| Term | Meaning |
|---|---|
| **Broker** | A `nimbod` process. Fully trusts every other Broker in its Cluster. |
| **Cluster** | The Brokers listed in one S3 prefix. |
| **Broker ID** | `SHA-256(node_public_key)`, 32 bytes. **Short ID** = its first 8 bytes. |
| **Virtual IDs** | `SHA-256(broker_id ‖ i)` for `i = 0..15`. A Broker's 16 positions in XOR space, used for placement (§7.1). |
| **Incarnation** | A random 64-bit nonce chosen at each process start. Compared only for equality. |
| **Endpoint** | A WebSocket client. Its identity is its ephemeral X25519 public key, its **Endpoint key**. |
| **Session** | One authenticated WebSocket connection from an Endpoint to a Broker. An Endpoint may hold sessions to several Brokers at once. |
| **Capability (cap)** | The right to send Messages to a **target** Endpoint. It is held by exactly one **holder** Endpoint. |
| **Cap ID** | 16 random bytes, written as unpadded lowercase base32 (26 chars). It **is** the token an Endpoint holds. Not secret, and meaningful only to its holder. |
| **Single-use cap** | A cap consumed by its first send. Reply caps are single-use. |
| **RCR** | Runtime Control Records: **cap records**, **presence registers** and **blobs**. Each one is stored on its placement set and is never gossiped cluster-wide. |
| **Placement key** | The 32-byte value a record is placed by: `SHA-256(cap ID)` for caps, `SHA-256(endpoint key)` for presence, and the blob hash for blobs. |
| **Placement set** | The 3 distinct live Brokers nearest to a placement key by XOR distance over virtual IDs. Brokers that are draining are excluded. |
| **Suspect** | A Broker this Broker currently cannot reach (§5). Suspicion is local only. |
| **Blob** | A schema or base object, addressed by `SHA-256(bytes)`. |
| **Action** | Any frame on the endpoint WebSocket. |
| **Message** | A JSON object sent to a cap. The only data Endpoints exchange. |

---

## 3. Architecture overview

```
   Endpoints (untrusted, permissionless)
   ┌────────┐ ┌────────┐ ┌────────┐ ┌──────────┐
   │ agent  │ │sandbox │ │ ESP32  │ │ Elixir   │
   └───┬────┘ └──┬──┬──┘ └───┬────┘ └────┬─────┘
  wss  │   wss   │  │ wss    │ wss       │ wss
 ┌─────▼─────────▼┐┌▼────────▼──┐ ┌──────▼──────┐
 │   nimbod  A    ││  nimbod B  │ │  nimbod C   │   Brokers (fully trusted)
 │ ws API │router ││            │ │             │
 │ caps   │RCR    ││            │ │             │
 │ mesh (magicsock + wireguard-go + gVisor netstack; DERP relays are external)
 └───────┬────────┘└─────┬──────┘ └──────┬──────┘
         └─── WireGuard over UDP / DERP ─┘
                         │
                  ┌──────▼──────┐
                  │ S3 bucket   │  membership + cluster secrets + reference clock
                  └─────────────┘
```

A Broker has six internal subsystems:

1. **Membership**: reads and writes `members/` in S3, tracks the S3 clock offset, and fences the Broker itself if it cannot heartbeat (§4).
2. **Mesh**: an in-process Tailscale data plane, plus per-peer suspicion (§5).
3. **RCR store**: XOR placement, acknowledged replication, lookup, handoff and expiry for cap, presence and blob records (§7).
4. **Capabilities**: minting, transfer, validation, sliding expiry and single-use consumption (§8).
5. **Router**: ingress validation and sequencing, endpoint location, egress delivery, forwarding, TTL queues (§9).
6. **Endpoint API**: WebSocket server, handshake and Actions (§6).

---

## 4. Cluster membership (S3)

Bucket layout under a configured prefix `s3://<bucket>/<prefix>/`:

| Key | Content | Writer |
|---|---|---|
| `cluster.json` | `cluster_id`, `wg_psk` (32B), protocol version, cluster-wide tunables. | Created once by `nimbod init` with a conditional `If-None-Match: *` PUT. Read by every Broker at startup. |
| `members.signal` | A random token. Its ETag changes whenever any member object's content changes. | Any Broker, after it joined, changed its member object or left. |
| `members/<broker-id-hex>.json` | `node_pub`, `disco_pub`, `incarnation` (random nonce), `draining` (bool), `version`, `ws_urls[]`, `endpoints[]` (direct mesh endpoints), `derp` (the DERP relay region this Broker chose as its home: hostnames, ports, as a Tailscale `DERPRegion` without its ID), `region`. | That Broker only. |

- **Clock reference.** Every S3 response carries a `Date` header. The Broker keeps a smoothed offset `s3_offset = Date − local midpoint of the request`, accurate to about ±1s. **S3 time** is `local now + s3_offset`. All membership liveness is judged in S3 time. If `|s3_offset|` exceeds `max_skew` (3s), the Broker fences itself (below) until NTP fixes the clock.
- **Heartbeat.** Each Broker re-PUTs its member object every `member_heartbeat` (15s). The content holds no timestamps, so the ETag stays the same and only `LastModified` moves.
- **Discovery.** A full refresh LISTs `members/` and GETs an object only when its ETag changed. A member is dead when `S3 time − LastModified > member_ttl` (60s).
  - **Membership signal (rev 7).** After a Broker's member object PUT changes its content (joining, a restart's new incarnation, new endpoints or DERP relay, draining) or after it DELETEs it on leaving, it PUTs `members.signal` with a random token, so the ETag changes. An empty object would not work: its ETag is always the MD5 of nothing.
  - Every Broker GETs `members.signal` every `member_signal_poll` (5s) and runs a full refresh when the ETag differs from the last one it saw (or right after a refresh in flight, whose LIST may predate the change). It does not record its own writes as seen, so a change signalled just before its own is not missed. Joins, leaves and info changes therefore reach every Broker within about 5s, without the mesh, which matters because over the Tailscale mesh a joiner's packets are dropped until peers know its key (§5).
  - A crashed Broker cannot signal, so the full refresh also runs every `member_poll` (30s) as a backstop. Heartbeats that only move `LastModified` do not signal.
  - **Freeze on failure:** if a LIST fails, the Broker keeps its last membership view and expires **nobody** until a LIST succeeds.
- **Self-fencing.** If a Broker's own last successful heartbeat is older than `member_ttl − 15s` (S3 time), or its clock is out of bounds (`|s3_offset| > max_skew`, using the median of the last 5 offset samples **(rev 6)**):
  - it sends `closing` with a jittered `retry_after_ms` to all its sessions;
  - it stops accepting new sessions;
  - it stops acting as a write target.

  Its endpoints move to healthy Brokers before peers declare it dead, so it never becomes a black hole. Fencing lifts once heartbeats succeed and the clock is within bounds again. **(rev 6)** On unfencing, the Broker takes a **new incarnation** and announces it (heartbeat PUT and `Rejoin`), exactly as after a restart: peers may have declared it dead meanwhile and tombstoned its registers, and nothing written under the old incarnation may shadow new state.
- **Fast join.** When the mesh sees a handshake from an unknown node key, the Broker GETs `members/<SHA-256(key)>.json`. If the object exists and is fresh, the peer is added at once; existence in S3 is the admission check. Only the TCP development mesh reports unknown keys: magicsock drops their packets before WireGuard sees them (§5).
  - **Abuse limits:** misses are cached negatively for 60s, and fast-join GETs are globally rate limited (10/s per Broker). Random handshake attempts therefore cannot amplify S3 load.
- **Rejoin announce.** On start, after writing its member object, a Broker sends `Rejoin{incarnation}` to every live member. This updates their views at once and triggers handoff to it (§7.4).
  - **(rev 6)** An incarnation learned from a `Rejoin` (or from any peer frame, which carries the sender's incarnation) is kept until an S3 member object **newer** than that moment says otherwise; a LIST/GET that still returns the old object must not roll the view back.
  - **(rev 6)** A peer seen with a new incarnation loses any local suspicion at once, so handoff to the restarted process is not held back.
- **Graceful leave**, in this order:
  1. PUT the member object with `draining: true` and announce it. Peers stop choosing this Broker as a write target but keep querying it.
  2. Send `closing` to sessions.
  3. Hand off all records (§7.4).
  4. DELETE the member object.
- **Cost** at 100 Brokers is about 8.6M LISTs, 17M heartbeat PUTs and 52M signal GETs a month, about $150. Any S3-compatible store works (AWS, R2, MinIO, GCS through its S3 API), with `aws-sdk-go-v2`.

**Trust model.** Write access to the prefix is cluster admin. `cluster.json` holds the WireGuard pre-shared key.

Membership is the input to every placement decision. Brokers can disagree on it for about one signal poll (5s) after a join, leave or change, or up to one backstop poll interval after a crash. Lookups, write fallbacks and handoff (§7) are designed to absorb that.

---

## 5. Broker mesh (Tailscale data plane, no control plane)

This follows tailcat's approach: Tailscale's open-source data plane used directly, with no coordination server and no OS network setup.

- **Components:** `magicsock` for UDP and DERP path selection, NAT traversal and disco; userspace `wireguard-go`; and a gVisor `netstack` that terminates TCP in-process. There is no TUN device and no root.
- **Keys.** The node key (WireGuard private key, which also fixes the Broker ID and so the Broker's position in XOR space) is loaded in this order:
  1. the `NIMBOD_NODE_KEY` environment variable, if set, in Tailscale's text form `privkey:<64 hex>`. This suits containers and Kubernetes Secrets with no persistent volume;
  2. otherwise `--state-dir/node.key`, which is created on first start;
  3. otherwise, with neither configured, a fresh key per process. The restarted Broker then lands at a new XOR position and its old share of records is handed off to others instead of back to it.

  An invalid `NIMBOD_NODE_KEY` is a fatal startup error; it never falls back silently. `nimbod keygen` prints a new key. The disco key is regenerated on every start, as in Tailscale.
- **Addressing.** Each Broker gets a ULA IPv6 `/128` derived from its Broker ID (`fd4e:6d62:6f00::/40` + 88 bits of the ID). The WireGuard peer configuration is rebuilt from membership whenever it changes. Every peer gets `wg_psk` as its pre-shared key, which protects against a DERP operator or anyone holding only public keys, and adds post-quantum protection.
- **DERP relays are external; Brokers do not relay.** Each Broker chooses its own relays: by default Tailscale's public relays (the map at `controlplane.tailscale.com/derpmap/default`, with a copy built into the binary as a fallback), or `-derp-map` (a map URL or file) or `-derp-relay host[:port]` (one relay, e.g. a self-hosted `derper`). `-derp-insecure` skips TLS certificate verification for them.
  - magicsock measures the Broker's relays and picks the nearest as its home. The Broker publishes that region (its nodes, without an ID) as `derp` in its member object.
  - Each Broker's DERP map is its own relays plus every region its peers publish. Region IDs are local to that map. A published region identical to one of the Broker's own keeps that region's ID; any other gets a stable local ID of 40000 or above and is marked `NoMeasureNoHome`, so it is used only to reach the peers that chose it. Each peer's home region is set from its published one.
  - A published opt-out of certificate verification travels with the region, since peers dial that relay too.
  - If Brokers can reach each other directly over UDP, DERP is used only for rendezvous. DERP connections honor `HTTPS_PROXY`.
  - magicsock drops packets, relayed or direct, from node keys it does not know. A Broker therefore learns about a joiner from S3 or the joiner's Rejoin, never from the joiner's first packets.
- **Connections.** Peer RPC listens on TCP `[ula]:7946` inside the netstack. Links are opened lazily to any Broker that needs to be reached and closed after `link_idle` (2 min). Each link uses two TCP streams: one for messages (`Forward`) and one for RCR traffic, so that a handoff burst cannot hold up messages. **(rev 8)** Each stream (§9.7) adds one data connection on the same port, with its own preamble `NMS1`.
- **Suspicion.** A peer becomes **suspect** locally when a dial fails or an RPC gets no reply within `rpc_timeout` (2s). Suspicion lasts 10s, doubling on each repeat up to 2 min, and clears on the first successful RPC.
  - Suspect peers are skipped as write targets (§7.2) and queried last on reads.
  - Suspicion never changes membership; S3 stays authoritative. It just gets traffic off a crashed Broker within seconds instead of the 60–90s S3 detection takes.
  - When a link to a suspect peer recovers, its retry queues are flushed (§7.2).
- **Framing.** Length-prefixed frames (`internal/peerproto`) with the preamble `NMB` + version byte. **(rev 6)** Frames are CBOR maps with integer keys (the library already used for cap records) instead of protobuf, which avoids a code generator; the schema is the Go structs in `peerproto`. Small RCR writes to the same Broker are coalesced into one frame. All durations on the wire are **remaining milliseconds**, never absolute times. RPCs:
  - `Forward` / `ForwardResult`: messages (§9);
  - `Store`, `Get`, `Touch`, `Consume`: records of each kind (§7, §8);
  - `Watch` / `Notify`: presence changes (§9.4);
  - `Rejoin`, `Handoff`: membership changes (§4, §7.4).

Pin the `tailscale.com` module version. The packages used are internal-API-ish and may change between releases.

---

## 6. Endpoints and the WebSocket protocol

### 6.1 Transport

- `wss://<broker>/v1/ws` with subprotocol `nimbo.v1`. **(rev 8)** Stream data sockets are `wss://<broker>/v1/stream?token=<token>` with subprotocol `nimbo.stream.v1`, on the same authority as the endpoint's session (§9.7). TLS may terminate at a load balancer. If it does, the load balancer **must** pass the client address with PROXY protocol v2, which the Broker requires, because per-IP limits (§6.4) depend on it.
- Every Action is one WebSocket **text** frame holding one JSON object. No binary frames on `/v1/ws`; data sockets carry both (D21).
- Keepalive uses WebSocket ping/pong; the broker pings every 20s. A session is dropped after 60s of silence.
- Encodings: IDs, including cap IDs, and keys are unpadded lowercase base32.
- **Reconnects.** SDKs reconnect with jittered exponential backoff (base 250ms, cap 30s) and honour `retry_after_ms` from `closing`. Brokers jitter `retry_after_ms` so that a draining or fenced Broker's endpoints spread out over time.

### 6.2 Handshake (endpoint authentication with X25519)

```
B → E  {"op":"challenge","v":1,"broker":"<broker id>","eph":"<B_eph pub>","nonce":"<32B>"}
E → B  {"op":"hello","key":"<E pub>","authority":"<host[:port] E dialed>",
        "proof":"<32B>","client":"esp32-sdk/0.1","max_message_bytes":16384}
B → E  {"op":"welcome","session":"<id>","limits":{...},"brokers":["wss://..."]}
```

- `ss = X25519(E_priv, B_eph_pub)`. Reject an all-zero `ss`.
- `proof = HMAC-SHA256(key = SHA-256("nimbo/v1/auth" ‖ ss), msg = nonce ‖ authority ‖ E_pub ‖ B_eph_pub)`.
- The Broker computes `X25519(B_eph_priv, E_pub)` and checks `proof`. It also checks that `authority` is one of its configured public names. This binding to the dialed host stops a malicious server from relaying a real Broker's challenge to a victim and replaying the answer. It works on ESP32 and other stacks that cannot reach TLS exporters.
- The broker's ephemeral key is single-use and lives only for the handshake.
- `welcome.brokers` suggests 2 other non-draining Broker URLs for redundancy: same region first, then the XOR-nearest to the endpoint key, which spreads endpoints evenly. Endpoints may connect to several Brokers with the same key.
- A successful session, and its close, update the endpoint's presence register (§9.1).
- Being connected grants nothing. A new Endpoint holds no caps. It can only mint caps to itself (§8.3), which grants nothing to anyone else until it transfers them.

### 6.3 Actions

Requests carry a client-chosen `rid`. Each gets exactly one `result`: `{"op":"result","rid":N,"ok":true,...}` or `{"op":"result","rid":N,"ok":false,"error":{"code":"...","detail":"..."}}`.

| Op (E → B) | Fields | Result |
|---|---|---|
| `send` (**SendMessage**) | `cap`, `payload` (JSON object), `caps` (cap IDs to carry, optional), `ttl_ms` (default 10000, max 60000), `reply` (optional: `schema`, `base`, `auto_reply`, `allow_stream`) | `msg` (message ID) and, if the target cap has `auto_reply`, `reply_id`: the lineage ID of the single-use reply cap. Replies arrive as `deliver` with `on = reply_id`. The result means the message was accepted for routing, not that it was delivered. |
| `stream` (**rev 8**) | as `send`; the cap must have `allow_stream` | `stream` (the stream ID, which is also the offer's message ID), `token` (the holder's data-socket token), `reply_id` as for `send`. The outcome follows as `stream_status`. |
| `mint` (**MintCap**) | `schema` (optional JSON Schema), `base` (optional JSON object), `auto_reply` (bool), `durable` (bool, optional), `allow_stream` (bool, optional) | `cap`: a new cap ID held by and targeting the caller. It is also the lineage ID that `deliver.on` reports for messages on this cap and its transferred copies. |
| `transfer` (**TransferCap**) | `cap`, `to` (Endpoint key), `durable` (bool, optional) | `cap`: a new cap ID bound to `to`, meaningful only to that Endpoint. Transferring a single-use cap consumes it (§8.3). |
| `renew` (**RenewCap**) | `caps` (array of cap IDs) | per-cap `{ok, ttl_ms}` or an error. The IDs do not change. |
| `inspect` | `cap` | the record (with schema and base resolved) and remaining TTL (debugging and SDK convenience) |
| `delegate_begin` (**rev 9**) | `target` (Endpoint key), and `schema`, `base`, `auto_reply`, `allow_stream` as for `mint` | a challenge: `eph` (a single-use Broker X25519 key), `broker` (Broker ID), `schema_hash`, `base_hash`, `ttl_ms` (§8.6) |
| `delegate` (**rev 9**) | `eph`, `proof`, `durable` (bool, optional) | `cap`: a new cap ID held by the caller and targeting `target` |

`durable: true` makes the call wait until **two members of the placement set** (or all of them, if fewer) hold the new record, counting the issuer if it is one. Without it the record can be lost if the issuing Broker crashes within milliseconds. Use it in bootstrap flows where the ID is handed out of band. **(rev 6)** Revision 5 waited for one remote acknowledgement; with an issuer outside the placement set, two members could then still answer "not found" and a lookup from another Broker could return `invalid_cap` for a valid cap.

| Op (B → E) | Fields |
|---|---|
| `deliver` | `msg`, `on` (lineage ID of the cap the message arrived on: the ID `mint` returned, or the sender's `reply_id`), `payload` (base fields merged in), `caps` (new cap IDs for the carried caps, now held by this Endpoint), `reply` (single-use reply cap ID if auto-reply applies) |
| `deliver` of a stream offer (**rev 8**) | as `deliver`, plus `stream` and `stream_token` (the target's data-socket token) |
| `stream_status` (**rev 8**) | `stream`, `state` (`accepted` or `failed`), `reason`, `detail`. At most one per stream, to the session that opened it. |
| `bounce` | `msg`, `reason` (`ttl_expired`, `receiver_busy`, `too_large`, `target_gone`, `unavailable`). Best effort, and only to the sending session if it is still connected. |
| `closing` | `reason`, `retry_after_ms` (jittered) |

**Error codes:**
- `bad_request`, `invalid_cap` (the placement set positively confirmed the cap does not exist), `not_holder`, `expired`, `consumed` (a single-use cap was already used), `target_gone`, `schema_violation`, `base_conflict`, `too_large`, `rate_limited`, `stream_not_allowed` (**rev 8**), `invalid_challenge` and `bad_proof` (**rev 9**).
- `unavailable` is retryable. It covers lookups that could not reach a quorum of answers, unresolvable blobs, overload, and a target whose state is unknown.

### 6.4 Limits (defaults, from `welcome.limits`)

- **Messages:** payload ≤ 64 KiB, and no larger than the receiver's `max_message_bytes`. Up to 16 carried caps per message.
- **Blobs:** schema ≤ 32 KiB, base ≤ 4 KiB. New blob bytes (a schema or base not already live in the Cluster) ≤ 64 KiB per session per 10s.
- **Per session:** 1000 actions/s and 100 mints/s. 4096 distinct caps used or renewed per 10s; sends on single-use caps are exempt, since they consume the cap instead of touching it and are bounded by the action rate. 100 lookups of unknown cap IDs per 10s. At most 10k live caps created by the session through mint or transfer, counted by the issuing Broker.
- **Per client IP:** 64 concurrent sessions and 20 handshakes/s with a burst of 200, so a NAT'd fleet can reconnect after a Broker failure. Both are configurable, with an allowlist for known service networks.
- **Per Broker:** a mint admission rate (default 20k records/s), and bounded per-peer replication queues. When either is exhausted, new mints, transfers and auto-reply sends fail with `unavailable`. Overload causes back-pressure, not unbounded memory.
- **Queues:** the delivery queue is 1000 messages or 1 MiB per session. Held messages are capped at 100 per target per Broker.
- **Delegation (rev 9):** 8 outstanding challenges per session, and 10 `delegate_begin`/s per session, each also counted as a mint. Challenges are valid for `delegation_ttl` (30s).
- **Streams (rev 8):** 64 pending holder halves per session (`rate_limited`); 16 unattached target halves per target per Broker (further offers bounce with `receiver_busy`); `stream_attach_ttl` 60s; 256 data sockets per client IP and 10 failed attaches/s per IP (then 429); 100,000 data-plane legs per Broker.

---

## 7. RCR store: XOR placement

All control state is a set of records. Each record lives on its **placement set**, a few Brokers chosen by XOR distance from its placement key. Nothing is gossiped and no Broker holds a full copy. Every Broker computes placement sets the same way, from S3 membership.

### 7.1 Placement

- **Distance** from a key to a Broker is the minimum XOR distance from the key to any of that Broker's 16 virtual IDs.
- **Placement set:** the 3 distinct, live, non-draining Brokers with the smallest distance.
- **Balance:** at 100 Brokers the most loaded Broker holds about 1.3× the mean, against 2.1× without virtual IDs (simulated, review M4).
- **Index:** Brokers keep the 1,600 virtual IDs in a sorted index, so a placement lookup is a few comparisons.

| Kind | Placement key | Stored on | Content | Mutable state |
|---|---|---|---|---|
| **Cap** | `SHA-256(cap ID)` | placement set (3), plus a pinned cache copy on the issuer | immutable cap record (§8.2), ~150 B | `ttl` |
| **Single-use cap** (reply caps and their transfers) | `SHA-256(cap ID)` | **issuer only**. The ID is chosen so the issuer is the nearest Broker (§8.3). | immutable cap record | `ttl`, `consumed` |
| **Presence** | `SHA-256(endpoint key)` | placement set (3) | one register per (owner Broker, incarnation) (§9.1) | the register |
| **Blob** | blob hash | placement set (3) | `kind` (schema/base), bytes | `ttl` |

The issuer's pinned copy keeps uses on the creating Broker local, with no network round trip. That is the common case, since the creating Broker is usually the holder's own Broker.

### 7.2 Writes

- **Acknowledged, asynchronous.** A Broker stores any local copy at once and sends `Store` (or `Touch`) to each placement-set member. No client-visible operation waits, except `durable` calls. Each peer has a bounded retry queue: entries are retried with backoff until acknowledged or the record expires. The queue is flushed when a link recovers.
- **Fallback for unreachable members.** If a placement-set member is suspect, the write goes to the next-nearest non-suspect Broker as a **hinted** copy, tagged with the intended member. The hinted holder forwards it when that member recovers, and serves it to lookups meanwhile.
- **Merge rules.** All are commutative, associative and idempotent, so no coordination is needed:
  - **cap and blob content** is immutable, so the first write wins;
  - **`ttl`:** each write carries remaining milliseconds, and the receiver converts it to its local deadline `min(now + ttl, now + cap_ttl)`, keeping the maximum. One Broker's clock error can therefore never extend a record cluster-wide;
  - **`consumed`** is sticky: false → true only;
  - **presence registers** are last-writer-wins per (endpoint, owner Broker, incarnation), ordered by the owner's sequence number (§9.1).
- **Missing records.** A holder that receives a `Touch` for a key it does not have replies `missing`. The sender then re-sends the full record (or blob bytes) with `Store`. Every Broker that touches a record holds a copy, because it validated the record first.

### 7.3 Lookups

1. **Local first:** records this Broker holds, then the local cache. The cache is an LRU of records received in a `Forward`, from earlier lookups, or created here.
2. **Otherwise** send `Get` in parallel to the `lookup_fanout` (4) nearest live Brokers.
   - Within `handoff_window` (10s) of a membership change, also ask the placement set under the previous view.
   - Answers are merged with the write rules.
   - Hinted holders answer too.
3. **Outcome:**
   - **found:** use the merged record;
   - **not found:** at least 2 members of the current placement set answered "not found", and none is suspect. Only this produces `invalid_cap` or treats presence as empty;
   - **anything else:** retry with backoff for up to `lookup_wait` (1s), which covers a record whose `Store` is still in flight. If that also fails, the result is `unavailable`, which is retryable.
   - **(rev 6)** A positive not-found is also re-checked until `lookup_wait` has passed before it becomes `invalid_cap`: an endpoint with sessions on two Brokers may use a just-minted cap on the second one before the `Store` lands. The unknown-ID lookup quota (§6.4) bounds the cost.
   - Suspect Brokers are left out of the fan-out; answers are merged as they arrive and a found, unexpired record ends the lookup early.

Blobs are immutable, so a cached blob is always valid for its content. But the cache records the blob's TTL, so the "already live in the Cluster" check (§8.3) never trusts an expired cache entry. Cached cap records can hold a stale TTL, and cached presence can be stale. §8.4 and §9.2 say when each is re-read.

### 7.4 Handoff and restarts

- **On any membership change** (a join, a leave, a member expiring, a draining flag, or a Rejoin with a new incarnation), every Broker recomputes the placement set of each record it holds. It pushes the record, **including all mutable state** (`ttl`, `consumed`, registers), to any Broker that newly belongs in that set. A holder that is no longer in the set keeps the record as cache only.
- **Presence is pushed by its owners.** A Broker re-pushes the registers for its own sessions:
  - on any session change;
  - whenever their placement set changes;
  - after its own Rejoin;
  - when a link to a holder recovers;
  - and in full every `presence_reconcile` (5 min).

  The owning Broker is the source of truth for its sessions, so lost or stale copies repair themselves.
- **Restart.** A Broker that restarts with the same node key gets a new random incarnation and announces `Rejoin` (§4). Peers hand its share of records back to it. Registers from its previous incarnation turn into tombstones (§9.1). There is no snapshot recovery, and nothing is persisted.
- A replicated record is lost only if every member of its placement set and every hinted holder fails within one handoff window. A lost cap shows up as `invalid_cap`. A single-use cap is lost with its issuer; the pending reply then fails, as with any at-most-once message.

### 7.5 Expiry (sliding)

Cap and blob records carry a TTL and use the same sliding rule. **(rev 6)** Local deadlines are kept on the **monotonic** clock; the wall clock is only compared with S3 time (§4). A wall-clock step therefore cannot stretch or shrink a TTL, which the simulator found when it stepped clocks.

- creating a record sets `ttl = cap_ttl` (60s);
- a Broker that sees a use resets its local deadline to `now + cap_ttl`. Once it has extended that by at least `touch_threshold` (10s) since its last touch, it sends `Touch{key, ttl_ms}` to the placement set, acknowledged and coalesced;
- a record is **accepted** while `now ≤ deadline + grace`;
- holders keep a record until `deadline + grace + touch_threshold` (25s past their deadline). A holder's deadline can lag the true one by up to `touch_threshold`. The extra margin means holders never drop a record that some Broker would still accept (review H2b).
- **(rev 6)** Every operation that creates a cap from another (transfer, including a single-use transfer that consumes the original, and carried-cap translation at ingress) counts as a use of the schema and base blobs, so a new cap never outlives its blobs.

This gives **soft expiry**: a record is accepted until 60–75s after its last use, as seen across the Cluster. A record used only within its first 10s sends no touches. Presence registers follow their own retention (§9.1).

### 7.6 Why no anti-entropy is needed

Every record either expires or is refreshed by its writer, and each refresh repairs missing copies:

- A **cap or blob in use** is touched at least every ~10s by each Broker that uses it. Touches go to every placement-set member. A member that lost the record answers `missing` and gets a full re-store (§7.2). A record nobody uses expires within 75s.
- A **presence register** is re-pushed by its owner on every change, placement shift, link recovery and every 5 minutes (§7.4).
- **Single-use caps** are never replicated.

After a partition heals, retry queues flush on link recovery, and the next touches and re-pushes restore full replication without a separate digest protocol. A long-lived record unused for longer than its TTL expires anyway.

### 7.7 Cost at the v1 target

| | Per Broker (100 Brokers; worst Broker ≈1.3× mean) | Cluster total |
|---|---|---|
| Presence for 1M endpoints | ~30k registers, ~3 MB | 3 writes per session change, plus 5-min reconcile (≈10k registers/s) |
| 1M long-lived caps | ~30k records, ~5 MB | ~300k touches/s, coalesced into a few frames per link per 50ms |
| New replicated cap (mint, transfer, translation) | — | 3 remote `Store`s of ~150 B |
| Reply caps | issuer only, held until consumed or 60s | no replication traffic |
| Distinct live blobs | 3% of them | bytes stored 3×, cached on use |

---

## 8. Capabilities

### 8.1 Cap ID (the token)

A cap token is **just its cap ID**: 16 random bytes, written as unpadded lowercase base32 (26 characters), for example `k3v7q2m9x4td8hj5wz6c1ry0ab`. It carries nothing else.

The ID is not secret. What makes it usable only by its holder is the cap's record, which names the holder. A Broker accepts the ID only from a session authenticated as that holder. Each transfer creates a new ID for the new holder; the original ID stays valid for its own holder, unless the cap is single-use.

### 8.2 Cap record

The record is the immutable description of one cap, keyed by its ID and encoded as deterministic CBOR (RFC 8949 §4.2.1). Endpoints see it only through `inspect`.

| Field | Type | Notes |
|---|---|---|
| `lineage` | 16B | ID of the mint this cap descends from (`= id` for a minted cap). Reported to the target as `deliver.on`. |
| `kind` | enum | `mint`, `transfer`, `reply` |
| `single_use` | bool | Consumed by its first send. Set for reply caps and inherited on transfer. |
| `target` | 32B | Endpoint key that receives Messages |
| `holder` | 32B | Endpoint key allowed to use this cap |
| `issuer` | 8B | Short ID of the Broker that created the record |
| `minted_at` | unix ms | informational |
| `schema` | 32B? | blob hash of a JSON Schema (draft 2020-12) |
| `base` | 32B? | blob hash of a JSON object |
| `auto_reply` | bool | create a single-use reply cap on each delivery |
| `hints` | [≤3]8B | Short IDs of Brokers where `target` was connected when `lineage` was minted. Routing tries them when presence is unknown or empty (§9.2). |
| `allow_stream` | bool | **(rev 8)** CBOR key 12, omitted when false, so older records encode identically. Holders may open streams (§9.7). Set only by the target (`mint`, or `send.reply` for the reply cap), inherited on transfer and carry, never turned on there. A Broker that predates it drops it, so streams fail closed with `stream_not_allowed`. |

### 8.3 Operations

All of these happen on the Broker that receives the Action, which becomes the issuer of any new record. None of them wait on the network, except with `durable`.

- **MintCap.** Hashes the supplied schema and base. Compiles the schema, rejecting an invalid one. Writes any blob that is not already live: a Broker skips the write only if its cached TTL for that blob is still running. Creates `{kind: mint, target = holder = caller, lineage = id, hints = brokers the caller is connected to}`.
- **TransferCap(cap, to).** Validates that the caller holds `cap`, counting it as a use (§8.5). Creates `{kind: transfer, holder = to}` with a new ID, copying target, single_use, schema, base, auto_reply, lineage and hints.
  - It is **never** gated on whether `to` exists, is connected or is known, so it leaks nothing about `to`.
  - The original cap stays valid, unless it is single-use: then the transfer **consumes** the original (§8.4), and the new cap is single-use too. A single-use cap therefore has move semantics.
  - This is the out-of-band bootstrap path: an owner service transfers a cap to a sandbox's public key and hands the sandbox the returned ID, for example through an environment variable. Use `durable: true` here.
- **Translation of carried caps** happens at the receive-side (egress) Broker (§9.3). It is the same as a transfer to the receiver, issued by the egress Broker. A carried single-use cap was already consumed at ingress, when it was validated.
- **Reply caps.** When a Message is delivered on a cap with `auto_reply`, the egress Broker creates `{kind: reply, single_use: true, target = sender, holder = receiver, lineage = reply_id, hints = sender's brokers}`.
  - `reply_id` is chosen at ingress and returned to the sender in the `send` result (§6.3), so the sender can match the reply without any blob.
  - Its `schema`, `base` and `auto_reply` come from the sender's `send.reply` options, each defaulting to none, none and `false`. Setting `auto_reply` on the reply lets a conversation continue one single-use cap per turn.
  - The ingress Broker writes any reply schema or base blob, and the `Forward` carries only their hashes.
- **Single-use cap IDs and storage.** A single-use cap is stored only on its issuer and never replicated. Its ID is still 128 random bits, but drawn by **rejection sampling**: the issuer draws random IDs until it is itself the nearest Broker to `SHA-256(ID)`. That takes about 100 SHA-256 operations at 100 Brokers, around 30µs. Any Broker can then find it with an ordinary lookup (§7.3), and the issuer is the single place where consumption is decided. If membership has since shifted, the 4-way lookup fan-out still reaches the issuer.

Schemas compiled from blobs are cached by hash. A unique `base` per mint costs one blob write per mint, which is why bases are small and new blob bytes have a per-session budget.

### 8.4 Validation (at the ingress Broker, for every use)

Given a cap ID presented by session `S`, authenticated as Endpoint key `K`:

1. **Look up the record** (§7.3): `invalid_cap` on a positive not-found, `unavailable` if unresolved.
2. **Check the holder:** `record.holder == K`, else `not_holder`.
3. **Check the TTL.** It must not have expired. A cached copy can hold a stale TTL; if it looks expired, re-read the record from its placement set before rejecting with `expired`.
4. **Resolve the `schema` and `base` blobs** (cache, then lookup). If a blob cannot be resolved, fail closed with `unavailable`.
5. **Check the target's state** (§9.1):
   - **gone:** fail with `target_gone`. The use does not extend the TTL; this is how caps to departed ephemeral Endpoints die;
   - **unknown:** proceed, routing through hints or a watch;
   - **present or recently absent:** proceed.
6. **Consume a single-use cap.** Send `Consume{id}` to the issuer, or do it locally if this Broker is the issuer. The issuer atomically flips `consumed` and answers `ok`, or `consumed` if it was already used. Only `ok` proceeds. If the issuer cannot be reached, the result is `unavailable`, and the cap stays unconsumed.

### 8.5 Liveness

**Rule.** A cap expires `cap_ttl` (60s) after its last **use**. A use is any of: SendMessage on it, carrying it in a Message, TransferCap of it, or RenewCap. RenewCap refreshes the TTL **without** issuing a new ID; it does not consume a single-use cap. Each cap ID has its own TTL; using a transferred copy does not extend its parent. A use of a cap is also a use of its schema and base blobs. The mechanism is §7.5.

**No revocation.** As decided, there is no RevokeCap. A cap in use lives indefinitely. An Endpoint withdraws access by disconnecting and rotating its ephemeral key, after which caps to it die with `target_gone`. Applications that need per-client withdrawal can mint one cap per client with a distinguishing `base` field and ignore messages from withdrawn clients.

### 8.6 Delegated issuance (rev 9)

An Endpoint A that holds another Endpoint B's private key can get a cap **targeting B and held by A**, without B connecting. A typical user is an orchestrator that generated a sandbox's key and wants a cap to the sandbox before the sandbox has minted one and sent it back.

**No new authority.** The cap is exactly what B would produce by minting and transferring to A, and anyone with B's key can already open a session as B and do that. Delegation spares the second session and handshake. It also avoids the harm that session does: it registers presence for B, and egress picks the target's newest session (§9.3), so it would receive B's messages while it is open.

**Protocol.**
1. A sends `delegate_begin` with `target` = B and the cap's parameters. The Broker applies the `mint` checks (sizes, schema compiles, blob budget, mint rate) and writes the blobs (§8.3). It then creates a **challenge**: a fresh X25519 key `D_eph`, kept only in A's session. It answers with `D_eph_pub`, its Broker ID and the blob hashes.
2. A, or a process holding B's key on A's behalf, computes the handshake's construction (§6.2) under its own label:
   - `ss = X25519(B_priv, D_eph_pub)`, rejecting an all-zero `ss`;
   - `proof = HMAC-SHA256(SHA-256("nimbo/v1/delegate" ‖ ss), broker_id ‖ D_eph_pub ‖ B ‖ A ‖ flags ‖ schema_hash ‖ base_hash)`;
   - `flags` holds `auto_reply`, `allow_stream`, and whether each hash is present. A missing hash is 32 zero bytes.
3. A sends `delegate` with `D_eph_pub` and the proof on the same session. The Broker computes `X25519(D_eph_priv, B)` and compares the proofs in constant time. On a match it creates `{kind: mint, target = B, holder = A, lineage = id}`, with `hints` = this Broker if B has a session here, else none.

**Properties.**
- **The proof authorizes one grant.** It names the holder and the parameters, so a proof taken from A issues nothing to anyone else. A key-holding process can also check what it approves.
- **Labels are separate.** The proof's label differs from the hello proof's (`nimbo/v1/auth`), so neither stands in for the other.
- **What is not covered.** `durable` is the caller's choice and is not part of the proof.
- **Challenge lifetime.** A challenge is single use: any `delegate` that names it spends it, whatever the outcome. It is valid for `delegation_ttl` (30s), only on the session that asked for it, with at most 8 outstanding per session.
- **Errors.** A challenge that is unknown, spent, expired or from another session fails with `invalid_challenge`; a proof that does not match fails with `bad_proof`.
- **Not gated on B.** As with MintCap and TransferCap, nothing checks B's presence. A can provision caps before B ever connects; a cap to a B that is gone fails with `target_gone` on use.
- **No new state or traffic between Brokers.** The record format is unchanged, so Brokers that predate revision 9 store, route and validate delegated caps like any mint; they only lack the two Actions. There are no peer-protocol or presence changes.
- **Cost and primitives.** Each challenge costs two X25519 operations, the same as a handshake. The endpoint protocol still needs only X25519, HMAC-SHA256 and SHA-256.

---

## 9. Presence, routing and messages

### 9.1 Presence

Presence answers "which Brokers is this Endpoint connected to right now?"

- **Registers.** An endpoint's presence record is a set of **registers**, one per (owner Broker, incarnation). Each register holds `{seq, sessions[], absent_ttl}`:
  - `seq` is the owner's monotonic sequence number;
  - `sessions` are the owner's live sessions for this endpoint;
  - when `sessions` is empty, `absent_ttl` is the time since the last one closed, sent as remaining milliseconds.

  The owner always sends the **full** register, and holders keep the highest `seq`. This is a per-owner last-writer-wins CRDT: delayed, duplicated or handed-off writes cannot resurrect a closed session.
- **Merge order (rev 6).** Registers are ordered by `seq` first; at equal `seq` a tombstone wins; among tombstones the later absence wins. So an owner can always override a holder's tombstone with a newer write (and it bumps `seq` on every full re-push), while a holder that knows the owner is dead re-tombstones any write it receives for that (owner, incarnation). Revision 5 let tombstones win outright, which left an endpoint looking absent, then `gone`, when a holder tombstoned a live owner's register during a view change.
- **Validity.** A register with sessions counts only while its owner is a live member with that incarnation.
- **Pruning.** When an owner is declared dead or changes incarnation, holders turn its registers into **tombstones** with `absent since = prune time`. The endpoint therefore gets a full `cap_ttl` grace from the moment its Broker failed, not from some earlier time.
- **Retention.** Tombstones are kept for `presence_tombstone_ttl` (10 min).
- **Read repair.** An egress Broker that answers `not_here` re-pushes its own register for that endpoint, which corrects any stale view.

An endpoint's state, as seen by a lookup:

| State | Condition | Effect |
|---|---|---|
| **present** | some valid register has sessions | route to those Brokers |
| **recently absent** | no sessions; latest absence started ≤ `cap_ttl` ago | hold the message and watch (§9.4); uses extend TTLs |
| **gone** | no sessions; absent for more than `cap_ttl`, with at least one tombstone confirming it | `target_gone`; uses do not extend TTLs |
| **unknown** | no registers found (positive not-found), or the lookup was unresolved | route through `hints`, then hold and watch; uses extend TTLs |

"Gone" requires positive evidence that the endpoint has been absent continuously for `cap_ttl`. Missing or unreachable presence data never kills caps.

### 9.2 Ingress (sender's Broker)

1. **Sequence.** Sends are processed through a per-(session, target cap) sequencer: validation, routing and hand-off of message *n + 1* starts only after message *n* is handed off, held or dropped. Different caps on the same session proceed in parallel, so one cold lookup stalls only its own cap.
2. **Validate** the target cap (§8.4) and every carried cap. Carried caps must also be held by the sender. Each validated cap counts as a use (§8.5).
3. **Validate the payload.** It must be a JSON object. Validate it against the schema blob, if `record.schema` is set. The **base fields are not** covered by the schema.
4. **Merge the base.** Merge the base blob into the payload shallowly. If the payload already has a top-level key that is in `base`, reject with `base_conflict`.
5. **Accept.** Assign `msg` (16 random bytes), set the message TTL, choose `reply_id` if the target cap has `auto_reply`, and reply `result{ok, msg, reply_id?}`.
6. **Locate the target**, cheapest source first:
   1. local sessions;
   2. the location cache (`key → brokers`, TTL 30s);
   3. a presence lookup (§7.3);
   4. if presence is empty or unknown, `record.hints`.

   Among several Brokers, prefer non-suspect ones, then pick by rendezvous hash of `(cap ID, broker id)`, so one cap's messages follow one path.
7. **Forward.** Send `Forward{msg, ttl_ms, hop, target (cap ID + record), payload, carried[] (cap ID + record), reply_id, reply_opts (blob hashes + auto_reply), ingress}`. The records travel with the message. This is the fast path: the egress Broker does not look them up, caches them, and trusts the ingress Broker's validation.
8. **Outcomes:**
   - **not sent** (dial failure, peer suspect): try the next candidate;
   - **`not_here`:** invalidate the cache entry and look up presence again;
   - **recently absent or unknown target:** hold the message and watch (§9.4).

   At most 2 re-routes per message. After that it bounces with `unavailable`.

### 9.3 Egress (target's Broker)

1. **Pick a session.** Use the target's newest live session on this Broker. If there is none, answer `not_here` and read-repair presence (§9.1). If the target disconnected after the message was accepted, hold it and watch (§9.4).
2. **Create caps.** Translate each carried cap into a transfer to the receiver, and if `auto_reply` is set, create a single-use reply cap (§8.3). These are local writes, so delivery does not wait on them.
3. **Deliver.** Write `deliver` to the session. Once written, the message is never re-sent anywhere.

### 9.4 Waiting for a reconnecting target

A Broker holding a message for a target that is recently absent or unknown sends `Watch{key, ttl_ms = message TTL}` to the presence set.

- **Check-then-register.** Each holder registers the watch and replies with the endpoint's current registers in the same step. A reconnect that landed just before the watch is therefore seen at once.
- **Notify.** Holders send `Notify` on the next register change, and the holding Broker then re-routes.
- **Backstop.** The holding Broker also re-looks-up presence every 2s, in case a `Notify` is lost or lands under a different membership view.
- **Bounds.** Watches expire with the message TTL. Held messages are capped at 100 per target per Broker (§6.4).
- **Ending.** On TTL expiry, overflow or a gone target, a best-effort `bounce` goes back to the ingress Broker and on to the sending session.

### 9.5 Delivery semantics (decided: at-most-once + TTL)

- **At most once.** A message has exactly one owner at a time. Ownership moves only when a peer replies `accepted` to a `Forward`. `not_here` and `rejected` mean the peer did not take it. A `Forward` that was never written to the link can be retried safely. If a written `Forward` gets no reply, the outcome is unknown and the message is **dropped**, never re-sent. Egress never re-sends after writing to the WebSocket.
- **Ordering.** FIFO per (sender session, target cap), as long as the target's set of Brokers is stable. The ingress sequencer (§9.2) and a single path per cap provide it, and held messages keep their place, because later messages on that cap wait behind them. No ordering is promised across caps, across sender sessions, or across a change in the target's Brokers.
- **Delivery receipts** are not a protocol feature. They are built with reply caps.
- **Backpressure.** Writes to the WebSocket rely on TCP backpressure. When the per-session queue is full, new messages bounce with `receiver_busy`.

### 9.6 What the receiver learns

The receiver gets `on` (which of its minted caps the message arrived on), the merged payload, the new IDs for the carried caps and the reply cap ID. There is no explicit "from" field. `inspect` on the reply cap shows its `target`, the sender's Endpoint key, but knowing a key grants nothing.

### 9.7 Streams (rev 8)

A **stream** is a full-duplex WebSocket pipe between a cap's holder and its target. It carries opaque text and binary messages end to end. It is allowed only on a cap whose record has `allow_stream` (§8.2), which only the target can set. Endpoints negotiate a stream on their session (the **control plane**) and carry its bytes on a separate WebSocket, the **data socket** at `/v1/stream` (the **data plane**). Each endpoint opens its data socket with a short-lived, single-use **stream token** issued by its own Broker.

**Two half-streams.** A stream is two half-streams with the same stream ID, one on each side's Broker. Neither owns the stream, and neither decides anything for the other.

| Half | Lives on | Created | Holds |
|---|---|---|---|
| holder half | the holder's Broker H | by the `stream` Action | the holder's token hash, the holder's leg, a deadline of the offer's TTL + `stream_attach_ttl` |
| target half | the target's Broker T, the one that delivers the offer | when T writes the offer's `deliver` | the target's token hash, the target's leg, the address of H (the offer's ingress), a deadline of `stream_attach_ttl` |

**Opening.**
1. The holder sends `stream` (§6.3). H validates it like a `send` (§8.4) and also requires `allow_stream`. It creates the holder half, answers with the stream ID (the offer's message ID) and the holder's token, and routes the **offer**: an ordinary message with `Forward.stream` set.
2. The offer is delivered like any message: located, held while the target reconnects, re-routed, bounced, with carried caps and auto-reply.
3. When T writes the offer's `deliver`, it creates the target half and puts the target's token in the same frame. That frame only goes to a session authenticated as the target's key, so the token needs no other binding. Delivery is at most once, so there is exactly one token per offer.

**Accepting, refusing, ignoring.** There is no Action for any of these.
- **Accept:** attach to the Broker that delivered the offer, with `stream_token`. The target holds the token, so it can attach even if the session that got the offer has dropped.
- **Refuse:** attach and close at once, with 4901 (`stream_rejected`) or any pass-through code and a reason. The holder's data socket closes with the same code and reason.
- **Ignore:** do nothing. Both halves expire; the holder gets `peer_attach_timeout`. Ignoring costs a device nothing.

**Pairing.** When the target attaches, T dials H with a **data connection** over the mesh (§5), naming the stream ID. H pairs it with the holder half if that half is live and not yet paired, and reports `stream_status accepted` to the holder's session. T answers the target's attach with 101 only once paired (403 if H has no such half, 503 if H is unreachable). On the same Broker the halves pair in process. When a half has both its endpoint's leg and the other half's leg, the runtime **splices** them. Until then the runtime does not read either connection, so early writes wait in TCP buffers.

**What Brokers exchange.** For a stream, H and T exchange only the offer's `Forward` (with `stream`, and `reply_stream` for an auto-reply cap), its `Bounce` if the offer fails, and the data connection. There are no accept, reject or token frames, and no token ever leaves its issuer.

**Data connection.** A new TCP connection inside the netstack per stream, with the preamble `NMS1` (the accept loop dispatches on the preamble). It starts with a CBOR header `{1: stream ID}`. H answers with one status byte: `paired`, `no_half` or `busy`. Then both directions carry chunks: `[flags u8: binary, fin, close][len uvarint ≤ 32 KiB][bytes]`. A close chunk carries a u16 code and the reason. There is no multiplexing; TCP keepalive.

**Pipe semantics.**
- Each WebSocket message reaches the peer as one message of the same type (text or binary). Fragmentation is not kept.
- Relays work in chunks of at most 32 KiB, one chunk per direction per hop, so no Broker buffers a whole message, there is no size limit, and a slow reader stalls the writer through every TCP window on the path.
- Reliable and ordered while up; data in flight at a close or failure may be lost and is never retried (D5).
- Close codes 1000, 3000–4899 and 4901 from an endpoint pass through with their reason; others become 1000. 4900–4999 are Nimbo's (§6.3); 1001 means the Broker is draining.
- Liveness is per hop: Brokers ping paired data sockets every 20s.

**Lifetime.** Once spliced, a stream is independent of the cap and of both sessions (D25). It ends when a side closes, a hop fails (4911 `peer_lost` on the other side), or a Broker stops. A fenced Broker keeps its established streams and fails pending halves. A draining Broker fails pending halves at once and closes established streams with 1001 after `stream_drain_grace` (30s); the other Broker relays the close.

**Failure reports to the holder** (`stream_status failed`, at most once, and only if the opening session is still connected): a bounce of the offer (`ttl_expired`, `receiver_busy`, `target_gone`, `unavailable`); the holder half's deadline (`peer_attach_timeout`); the holder's data socket being lost before pairing (`canceled`). The holder's data socket, if attached, closes with the matching code (4902, 4904, 4903, 4950, 4905).

**Node and runtime.** The node keeps the halves, tokens (hashes only) and legs; it never sees stream bytes. The runtime reports `OnStreamAttach`, `OnStreamPeer`, `OnStreamDialed` and `OnStreamLegClosed`, and the node answers with `AnswerAttach`, `DialStreamPeer`, `AnswerStreamPeer`, `SpliceStream` and `CloseStreamLeg` (§11).

---

## 10. Security and abuse resistance

- **Trust boundaries.** S3 write access means cluster admin. Brokers fully trust each other, including for RCR writes and lookups. Endpoints are untrusted. DERP relays and the network see only WireGuard ciphertext, protected by a PSK.
- **Unforgeability.** A cap ID is usable only if a record exists for it. Only a Broker running Mint, Transfer, translation or reply-cap creation for an authenticated holder writes one. IDs are 128-bit random, so a made-up ID matches no record. Holder binding means a leaked ID is useless without the holder's private key. Blobs are content-addressed, so a schema or base cannot be substituted under a record.
- **Single use.** A single-use cap has exactly one arbiter, its issuer, and `consumed` only goes from false to true. It cannot be used twice, even when sent concurrently through different Brokers.
- **No side channels on transfer.** TransferCap does not look at the recipient.
- **Clock safety.** Wire durations are relative and clamped to `cap_ttl` on receipt, and membership liveness is judged in S3 time. Brokers with bad clocks fence themselves (§4).
- **Permissionless DoS.** The limits in §6.4 apply per IP (PROXY protocol behind load balancers), per session, per created-cap quota and per Broker. Schema compilation is capped: schema ≤ 32 KiB, no remote `$ref`, compile time limited.
- **Amplification bounds:**
  - **Touches:** 4096 distinct caps per session per 10s. A send on a single-use cap consumes it rather than touching it, and is bounded by the action rate.
  - **Lookups:** 100 unknown IDs per session per 10s.
  - **Blobs:** 64 KiB of new bytes per session per 10s.
  - **Fast-join S3 GETs:** negative cache plus a global rate limit.
  - **Reconnect storms:** handshake buckets with burst, plus jittered `retry_after_ms`.
- **Replay of handshake.** The broker's ephemeral key and the nonce are single-use, and the proof binds the dialed authority.
- **Delegation (rev 9).** A delegation proof needs B's private key, as a session as B does, so delegation grants no new authority (§8.6). Its challenge key is single-use and session-bound, and the proof binds the Broker, the holder and the cap's parameters. A low-order target key yields an all-zero secret and is rejected.
- **Payload privacy.** Brokers see payloads in clear (they must validate schemas). End-to-end encryption is a future layer and could use the endpoint X25519 keys.
- **Stream tokens (rev 8).** A stream token is a local bearer credential: 32 random bytes, stored by its issuer only as `SHA-256(token)`, valid once, only on its issuer and for at most `stream_attach_ttl` (60s). It travels in the URL (D22), so it can appear in access logs. Brokers never log it; operators should redact `token` at load balancers. A leaked token is usable once, within 60s, on one Broker. Stream bytes are opaque to Brokers and not schema-checked; a data socket grants nothing beyond its one stream.

---

## 11. Go implementation layout

```
cmd/nimbod/                 main, flags, `nimbod init`, `nimbod run`, `nimbod keygen`, `nimbod ctl`
internal/config/            config file + flags, cluster.json model
internal/membership/        S3 member objects, poll/heartbeat, freeze-on-failure, self-fencing,
                            S3 clock offset, fast-join GET (+ negative cache), Rejoin, draining, change events
internal/mesh/              magicsock + wireguard-go + netstack wiring, ULA addressing, peer dial/listen, suspicion
internal/peerproto/         protobuf messages + framing, per-link coalescing, acked retry queues
internal/rcr/               virtual-ID placement index, record stores per kind, merge rules,
                            Store/Get/Touch/missing, hinted copies, lookup outcomes, LRU caches, handoff, expiry
internal/cap/               cap record codec (deterministic CBOR), mint/transfer/reply, ID rejection sampling,
                            validator, Consume
internal/presence/          per-owner LWW registers, tombstones, owner re-push, read repair, watches/notify,
                            state classification
internal/schema/            JSON Schema compile cache and validation
internal/router/            ingress sequencer, egress, Forward, location cache, held messages, bounces
internal/wsapi/             WebSocket server, PROXY protocol, handshake, Action codec, sessions, limits
internal/metrics/           Prometheus, /debug endpoints, /healthz
pkg/client/                 Go endpoint SDK (reference implementation, incl. jittered reconnect)
docs/protocol.md            endpoint wire protocol spec, for the ESP32 / Elixir SDKs
```

**Dependencies:** `tailscale.com` (magicsock, wgengine/wireguard-go, netstack, derp), `github.com/coder/websocket`, `github.com/aws/aws-sdk-go-v2/service/s3`, `github.com/pires/go-proxyproto`, `github.com/santhosh-tekuri/jsonschema/v6`, `github.com/fxamacker/cbor/v2`, `google.golang.org/protobuf`, `golang.org/x/crypto/curve25519`.

**Concurrency model (rev 6).** All Broker logic lives in `internal/node` as one **deterministic, single-threaded state machine**. It does no I/O and reads no clock or randomness of its own: everything goes through an `Env` interface (monotonic and wall clocks, randomness, timers, peer sends, session frames, S3 requests). The production runtime (`internal/host`) runs one event loop per Broker and does the I/O around it:
- one goroutine per session for reads and one for writes (bounded queue: 1000 frames or 1 MiB);
- one coalescing writer goroutine per peer link and stream, dialing lazily; a frame that could not be written is reported back as "not sent";
- one goroutine per inbound peer stream, and one per S3 request;
- per-(session, cap) sequencers, acknowledged retry queues per peer and all stores are loop-owned, so they need no locks.

The deterministic simulator (`internal/sim`, [dst.md](dst.md)) drives the same `node` code. If one loop per Broker becomes the bottleneck, JSON parsing, schema validation and X25519 can move to the session goroutines without touching the state machine's semantics.

**Package layout as built (rev 6):** the subsystems above are files of `internal/node` (`membership.go`, `peers.go`, `store.go`, `presence.go`, `caps.go`, `router.go`, `session.go`, **(rev 8)** `stream.go`: half-streams, tokens and pairing, and **(rev 9)** `delegate.go`: delegation challenges) rather than separate packages, since they share one loop. Pure pieces are packages: `ids`, `placement`, `caprec` (the cap record; `cap` is a Go builtin), `presence` (registers and classification), `peerproto`, `schema`, `config`. I/O: `s3store`, `mesh` (`TS`: Tailscale data plane, with each Broker's DERP map; `TCP`: a PSK-authenticated, unencrypted development transport selected by `"mesh":"tcp"` in `cluster.json`), `host`. Tools: `cmd/nimbod`, `cmd/nimbo-sim`.

**Observability:** each Broker exposes Prometheus metrics for its stores, retry-queue depth, hinted copies, suspicion, S3 offset and fencing, plus `/debug` endpoints. `nimbod ctl` builds cluster-wide views by listing members from S3 and querying each Broker.

**Testing:**
- Unit tests for placement, merge rules (property tests that merges are commutative, associative and idempotent), presence state classification, cap record encoding, schemas and node-key loading.
- **Deterministic simulation testing** ([dst.md](dst.md)): real `node.Node`s in a seeded simulator with simulated time, network, S3 and endpoints, injecting crashes, restarts, partitions, link resets, latency spikes, S3 errors and outages, clock steps, drains and key rotation. Every seed replays exactly.
- End-to-end tests over real WebSockets with the TCP mesh and with the Tailscale mesh (`internal/host`).
- Netem-based integration tests over the real mesh (not yet written).
- Properties checked by the simulator:
  - no message is delivered twice;
  - no single-use cap is consumed twice;
  - no cap is accepted after 75s idle (with bounded skew);
  - after faults stop, every refreshed record is back on its full placement set within one refresh interval;
  - a crashed Broker's endpoints, once reconnected elsewhere, keep all their caps.

---

## 12. Milestones

1. **Single broker:** WebSocket API, handshake, mint/transfer/renew/send, cap records and expiry, single-use reply caps, blobs, Go SDK. The RCR store runs with a membership of one.
2. **Mesh + membership:** S3 membership with the clock offset, freeze-on-failure and fencing; magicsock mesh with suspicion; external DERP relays chosen per Broker and published in membership; Forward between Brokers.
3. **Distributed RCR:** virtual-ID placement; acked Store/Touch with retries, hinted copies and `missing`; lookups with outcomes; presence registers with watches and read repair; handoff; Rejoin; draining.
4. **Hardening:** all limits and quotas, PROXY protocol, metrics and `nimbod ctl`, chaos and churn tests, protocol spec for third-party SDKs (ESP32 C, Elixir).

---

## 13. Decision log

Decided with the project owner on 2026-10-12:

| # | Question | Decision |
|---|---|---|
| D1 | Gossip peer selection (XOR-nearest alone partitions) | ~~N XOR-nearest plus ring successor/predecessor~~. Superseded by D14: there is no gossip. |
| D2 | Capability lifetime beyond 1 minute | **Any use extends the deadline**, plus **RenewCap** that refreshes it without a new token |
| D3 | Consistency of the sliding deadline | **Soft expiry**: touch at ≥10s extension, 15s grace (60–75s effective). Touches go to the record's placement set. |
| D4 | Withdrawing access | **No revocation.** The target endpoint rotates its key. |
| D5 | Delivery guarantee | **At-most-once + TTL** (default 10s, max 60s), FIFO per path, best-effort bounces |
| D6 | DERP relays | ~~Embedded in `nimbod`~~. Revised 2026-10-13: **Brokers do not relay.** Each Broker chooses its own external relays (default: Tailscale's public relays) and publishes its home relay in its member object; peers add it to their DERP maps. |
| D7 | Payload model | **JSON object only.** JSON text frames for all Actions. |
| D8 | Scale target | **~100 Brokers / ~1M endpoints / ~1M live caps** |
| D9 | Where schema and base live | **Not in the token.** Content-addressed blobs referenced by hash; placement per D15. |
| D10 | Broker node key source | **`NIMBOD_NODE_KEY` env var first**, then `--state-dir/node.key`, then a fresh key (§5) |
| D11 | Rollup latency | ~~Burst allowance before 50ms batching~~. Superseded by D12: there is no rollup. |
| D12 | Token format | **Just the random 128-bit cap ID** in base32. No record or Merkle proof in the token, and no Merkle roots. |
| D13 | Where cap records live | ~~Issuer + 2 XOR-nearest replicas~~. Superseded by D16. |
| D14 | Cluster-wide RCR propagation | **Removed.** No snapshot + WAL gossip. Endpoint presence lives on the **3 Brokers nearest `SHA-256(endpoint key)`**. Restart recovery is handoff. Metrics are per Broker. |
| D15 | Where blobs live | **3 Brokers nearest the blob hash**, cached anywhere on use |
| D16 | Cap placement (review M2) | **3 nearest by `SHA-256(cap ID)`**, with the issuer keeping a pinned cache copy |
| D17 | Reply caps (review M3) | **Single-use.** Stored only on the issuer, which is the sole arbiter of consumption, and never replicated. Transfers of single-use caps consume them. |
| D18 | Placement balance (review M4) | **16 virtual IDs per Broker** |
| D19 | Anti-entropy (review H2) | **Not needed.** Every long-lived record is refreshed by its writer: touches with `missing` → re-store, and owner re-push for presence. Unrefreshed records expire. |
| D20 | Other review findings | **Apply all** (C1, H1–H5, M1, M5–M8, L1–L3), as written in this revision |
| D21 | Streams vs. D7 and the non-goals (2026-10-13) | **Narrowed.** Actions and messages stay JSON. Streams carry opaque text and binary messages on a **separate data socket** (`/v1/stream`), opened with a temporary per-stream token, never on the Action WebSocket. |
| D22 | Stream token transport and lifetime (2026-10-13) | **In the URL only**, valid for **60s** (slow networks cannot always open a WebSocket in 10s). |
| D23 | Which Broker a data socket uses (2026-10-13) | **Each side attaches to its own Broker**, which relay over the mesh. Some Brokers are not reachable from the internet on their WebSocket listener. |
| D24 | Broker coordination for streams (2026-10-13) | **No new Broker-to-Broker control traffic and no anchor.** Only the offer's `Forward` and `Bounce` and the stream's data connection. Each side's Broker issues its own endpoint's token; endpoints see different tokens; no token leaves its issuer. |
| D25 | Accepting and lifetime (2026-10-13) | **No accept Action.** The offer carries the target's token and the target accepts by attaching. Established streams outlive the cap and both sessions, and survive a Broker fencing itself. |
| D26 | Delegated issuance (2026-10-14) | **An Endpoint that proves ownership of another Endpoint's key gets a cap targeting it, held by itself**, through a Broker challenge (§8.6). |
| D27 | Delegation proof (2026-10-14) | **X25519 key agreement with a single-use Broker key, plus HMAC-SHA256**, the handshake's construction under its own label. An XEd25519 signature was considered and dropped: ECDH proves ownership and needs no new primitive. |
| D28 | Delegated record (2026-10-14) | **`kind: mint` with `holder ≠ target`**, no new kind, so older Brokers keep decoding records during a rolling upgrade. The proof binds the holder and the cap's parameters. |

## 14. Defaults open to change

These were chosen without the owner; each is easy to change.

1. **Blob limits and budget:** schema ≤ 32 KiB, base ≤ 4 KiB, 64 KiB of new blob bytes per session per 10s.
2. **`reply_id`** is returned by `send` so that replies can be correlated without a per-request base blob.
3. **A base field collision rejects** the message (`base_conflict`) rather than letting the base silently overwrite the payload key.
4. **Validation at ingress** (sender's Broker). Invalid traffic never crosses the mesh, and egress trusts ingress.
5. **Asynchronous, acknowledged replication** with retry queues and hinted copies. `durable` is opt-in.
6. **Presence states** (present / recently absent / gone / unknown) and the 10-minute tombstone retention.
7. **Single-use ID rejection sampling**, so the issuer is the nearest Broker. The alternative, encoding the issuer in the ID, would break D12.
8. **Time:** S3 `Date` as the reference clock, `max_skew` = 3s, and relative TTLs on the wire.
9. **S3 timings:** heartbeat 15s, membership signal checked every 5s, backstop poll 30s, member TTL 60s, fence at 45s without a heartbeat, fast-join GET with negative cache, Rejoin on start.
10. **Tunables:** `replicas=3`, `virtual_ids=16`, `lookup_fanout=4`, `lookup_wait=1s`, `handoff_window=10s`, `rpc_timeout=2s`, `cap_ttl=60s`, `touch_threshold=10s`, `grace=15s`, `presence_reconcile=5m`, `presence_tombstone_ttl=10m`, location cache TTL 30s, max 2 re-routes per message.
11. **Limits:** 64 sessions and 20 handshakes/s (burst 200) per IP; 10k live created caps per session; a 20k records/s mint admission rate per Broker; 100 held messages per target.
12. **Streams (rev 8):** the limits of §6.4; 32 KiB relay chunks; `stream_drain_grace` 30s; no bandwidth limit (4929 is reserved for one); a target attach waits at most the 1.5s dial timeout plus 2s for the status byte.
13. **Delegation (rev 9):** `delegation_ttl` 30s, 8 outstanding challenges and 10 `delegate_begin`/s per session.

## 15. Future work

- SWIM-style membership dissemination if S3 polling becomes the bottleneck past a few thousand Brokers.
- Optional end-to-end payload encryption using endpoint X25519 keys.
- Attenuating transfers, where the transferrer adds base fields or tightens the schema.
- An opt-in `single_use` flag on MintCap for application-level one-shot grants.

## 16. Changes from implementation and DST (revision 6)

Building the design and running it under deterministic simulation changed the following. Each was found by a failing simulation seed unless noted.

| # | Section | Change | Why |
|---|---|---|---|
| R1 | §4 | Unfencing takes a new incarnation and announces it. | Peers that declared the fenced Broker dead had tombstoned its registers; when it came back with the same incarnation its endpoints looked absent and their caps died with `target_gone`. |
| R2 | §4 | An incarnation learned by `Rejoin` beats an older S3 member object. | After an in-place incarnation change, a LIST/GET of the not-yet-rewritten object rolled peers back to the old incarnation and they tombstoned the new registers. |
| R3 | §4, §5 | Suspicion is reset when a peer changes incarnation. | Handoff to a restarted Broker waited behind suspicion earned by its previous process (up to 2 min); rolling restarts left records under-replicated. |
| R4 | §9.1 | Presence merge orders by `seq` first; tombstone wins only at equal `seq`; owners bump `seq` on full re-push. | Same failure as R1 through other interleavings: a stale tombstone could permanently shadow a live owner. |
| R5 | §7.5 | Deadlines use the monotonic clock; the wall clock is only compared with S3 time. | A simulated clock step moved every local deadline and caps were accepted 80s after their last use. |
| R6 | §4 | The S3 clock offset is the median of the last 5 samples (was an exponential average). | A clock step took too many samples to cross `max_skew`; a skewed Broker stayed unfenced for more than 45s. |
| R7 | §7.5 | Single-use transfers also use the schema and base blobs. | A cap produced by moving a reply cap outlived its base blob and failed with `unavailable`. |
| R8 | §6.3 | `durable` waits for 2 placement members, not 1 remote acknowledgement. | Found by reasoning while implementing: with 1 ack two members could still say "not found". |
| R9 | §7.3 | A positive not-found is re-checked for `lookup_wait` before `invalid_cap`. | Found while implementing: a cap minted on one Broker and used at once through the endpoint's session on another Broker. |
| R10 | §5 | Peer frames are CBOR, not protobuf. | Avoids a code generator; no behavioural change. |
| R11 | §11 | One deterministic state machine per Broker behind an `Env`; I/O around it. | Makes the whole Broker simulation-testable. |

Defaults chosen while implementing: the development TCP mesh (`"mesh":"tcp"`, authenticated with the cluster PSK, **not encrypted**); a 1.5s peer dial timeout (below `rpc_timeout`, so dial failures count as "not sent"); 5s `durable` timeout; 10s handshake timeout; replication batches of up to 512 ops with up to 4 batches in flight per peer.

## 17. Streams as built (revision 8)

Streams follow the approved proposal (revision 5 of the streams design) with these changes, found while implementing and simulation-testing them:

| # | Change | Why |
|---|---|---|
| S1 | A refusal is reported as `stream_status accepted`, then the holder's data socket closes with the target's code (4901 and its reason). | Pairing happens when the target attaches, and a refusal is an attach followed by a close. H cannot know at pairing time whether the target will speak or close, and waiting for its first frame would hang every stream whose target waits for the holder to speak first. `accepted` therefore means "the target attached". |
| S2 | `Forward` gains `reply_stream` (key 17) as well as `stream` (key 16). | `send.reply.allow_stream` must reach the Broker that creates the reply cap. It is part of the existing message, not a new exchange. |
| S3 | New failure reason `canceled`: the holder's data socket was lost before the halves paired. | Its token is spent, so the half can never pair; the holder learns why. |
| S4 | The SDK can open a stream without attaching (`Detached`) and attach later. | Lets the holder hand the token to another process, and lets tests attach the holder after the target. |
| S5 | Pending halves on a fencing Broker fail with `unavailable`; established ones are kept. | Matches "fencing refuses new streams"; a pending half cannot complete while sessions are closed. |

