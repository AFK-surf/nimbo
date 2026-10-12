# Nimbo design (v1)

Nimbo is a distributed, capability-based messaging fabric for AI agents, isolated runtimes and external devices. This document refines the concept notes into a design that can be built. It is written in Go and ships as one binary, `nimbod`.

Status: draft for review. The decisions taken with the project owner are listed in [§13](#13-decision-log). Defaults chosen without the owner, which are open to change, are in [§14](#14-defaults-open-to-change).

---

## 1. Goals and non-goals

**Goals**

- One standalone service, `nimbod` (the Broker). Brokers form a Cluster peer to peer, with no consensus and no leader.
- Endpoints are permissionless and untrusted. Connecting to a Broker grants nothing. All authority comes from Capabilities.
- Sending to an Endpoint looks the same to the sender whether that Endpoint is on the same Broker or a different one.
- Capability churn must not cause unbounded gossip or memory. Under load, latency is traded for bounded state.
- Endpoints range from multi-tenant services to ESP32 devices. The endpoint protocol must be implementable with only X25519, HMAC-SHA256, JSON and WebSocket.

**v1 scale target:** up to ~100 Brokers, ~1M concurrent Endpoints and ~1M long-lived Capabilities in active use across the Cluster. Every Broker holds the full RCR set in memory, a few hundred MB at the top of this range.

**Non-goals (v1):** durable or at-least-once delivery, payloads other than JSON, revocation, end-to-end payload encryption (possible later, because endpoint keys are X25519), multi-cluster federation, and an endpoint directory sharded by XOR distance (the path to 1000+ Brokers, see §15).

---

## 2. Glossary

| Term | Meaning |
|---|---|
| **Broker** | A `nimbod` process. Fully trusts every other Broker in its Cluster. |
| **Cluster** | The Brokers listed in one S3 prefix. |
| **Broker ID** | `SHA-256(node_public_key)`, 32 bytes. Used for XOR distance. **Short ID** = its first 8 bytes. |
| **Endpoint** | A WebSocket client. Its identity is its ephemeral X25519 public key, its **Endpoint key**. |
| **Session** | One authenticated WebSocket connection from an Endpoint to a Broker. An Endpoint may hold sessions to several Brokers at once. |
| **Capability (cap)** | The right to send Messages to a **target** Endpoint. It is held by exactly one **holder** Endpoint. |
| **Cap record** | The immutable description of one cap: target, holder, schema and base hashes, auto-reply flag, and so on. |
| **Blob** | A schema or base object, addressed by `SHA-256(bytes)`. Blobs are published and replicated in RCR; cap records hold only their hashes. |
| **Cap ID** | 16 random bytes, written as unpadded lowercase base32 (26 chars). Not secret. |
| **Token / handle** | What an Endpoint holds: the cap record plus a Merkle inclusion proof, encoded as an opaque string. It is meaningful only to its holder. |
| **Origin** | The single-writer log of RCR entries a given Broker publishes about itself. |
| **RCR** | Runtime Control Records: endpoints, cap Merkle roots, cap liveness, blobs, broker metadata and metrics. Gossiped as snapshot + WAL per origin. |
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
 │ caps   │gossip ││            │ │             │
 │ mesh (magicsock + wireguard-go + gVisor netstack, embedded DERP)
 └───────┬────────┘└─────┬──────┘ └──────┬──────┘
         └─── WireGuard over UDP / DERP ─┘
                         │
                  ┌──────▼──────┐
                  │ S3 bucket   │  membership + cluster secrets (root of trust)
                  └─────────────┘
```

A Broker has six internal subsystems:

1. **Membership**: reads and writes `members/` in S3.
2. **Mesh**: an in-process Tailscale data plane. Brokers reach each other on private IPv6 addresses through a userspace TCP/IP stack.
3. **Gossip**: snapshot + WAL replication of RCRs over the mesh.
4. **Capabilities**: minting, Merkle rollups, token codec, validation and liveness.
5. **Router**: ingress validation, egress delivery, forwarding between Brokers, TTL queues.
6. **Endpoint API**: WebSocket server, handshake and Actions.

---

## 4. Cluster membership (S3)

Bucket layout under a configured prefix `s3://<bucket>/<prefix>/`:

| Key | Content | Writer |
|---|---|---|
| `cluster.json` | `cluster_id`, `wg_psk` (32B), `derp_mesh_key`, protocol version, cluster-wide tunables. | Created once by `nimbod init` with a conditional `If-None-Match: *` PUT. Read by every Broker at startup. |
| `members/<broker-id-hex>.json` | `node_pub`, `disco_pub`, `incarnation`, `version`, `ws_urls[]`, `derp` (region ID/code, hostname, IPv4/IPv6, STUN port) if this Broker serves DERP, `region`. | That Broker only. |

- **Heartbeat.** Each Broker re-PUTs its member object every `member_heartbeat` (15s). The content holds no timestamps, so the ETag (MD5) stays the same and only `LastModified` moves. Liveness is judged from `LastModified`, which is S3's clock, so it does not depend on broker clocks.
- **Discovery.** Each Broker LISTs `members/` every `member_poll` (30s). It GETs an object only when its ETag changed. A member whose `LastModified` is older than `member_ttl` (60s) counts as dead. Cost at 100 Brokers is roughly 9M LIST requests a month, about $45.
- **Fast join.** A new Broker's peers would otherwise wait up to one poll interval to learn about it. Instead, when the mesh sees a disco or WireGuard handshake from an unknown node key, it GETs `members/<SHA-256(key)>.json` directly. If the object exists and is fresh, the peer is added at once. Existence in S3 is the admission check.
- **Leave.** On graceful shutdown a Broker DELETEs its member object.
- Any S3-compatible store works: AWS, R2, MinIO, GCS through its S3 API. The client is `aws-sdk-go-v2`.

**Trust model.** Write access to the prefix is cluster admin. `cluster.json` holds the WireGuard pre-shared key and the DERP mesh key.

---

## 5. Broker mesh (Tailscale data plane, no control plane)

This follows tailcat's approach: Tailscale's open-source data plane used directly, with no coordination server and no OS network setup.

- **Components:** `magicsock` for UDP and DERP path selection, NAT traversal and disco; userspace `wireguard-go`; and a gVisor `netstack` that terminates TCP in-process. There is no TUN device and no root.
- **Keys.** The node key (WireGuard private key, which also fixes the Broker ID) is loaded in this order:
  1. the `NIMBOD_NODE_KEY` environment variable, if set, in Tailscale's text form `privkey:<64 hex>`. This suits containers and Kubernetes Secrets with no persistent volume;
  2. otherwise `--state-dir/node.key`, which is created on first start;
  3. otherwise, with neither configured, a fresh key per process. Restart recovery (§9.6) then cannot work.

  An invalid `NIMBOD_NODE_KEY` is a fatal startup error; it never falls back silently. `nimbod keygen` prints a new key. The disco key is regenerated on every start, as in Tailscale.
- **Addressing.** Each Broker gets a ULA IPv6 `/128` derived from its Broker ID (`fd4e:6d62:6f00::/40` + 88 bits of the ID). The WireGuard peer configuration is rebuilt from membership whenever it changes. Every peer gets `wg_psk` as its pre-shared key, which protects against a DERP operator or anyone holding only public keys, and adds post-quantum protection.
- **DERP, embedded in `nimbod`.** A Broker configured with a public hostname and a TLS certificate serves DERP at `/derp` on its HTTPS listener (the endpoint WebSocket listener can share it) and STUN on UDP 3478. It advertises itself in its member object. Every Broker builds the same DERP map from membership: relay Brokers with the same configured `region_id` form one DERP region and mesh with each other using `derp_mesh_key`. Brokers without public reachability are DERP clients only. A Cluster needs at least one relay Broker. If all Brokers can reach each other directly over UDP, DERP is used only for rendezvous.
- **Connections.** Peer RPC listens on TCP `[ula]:7946` inside the netstack. Two connection types:
  - **Gossip links**: long-lived connections to the selected gossip peers (§9.2).
  - **Data links**: opened lazily to any Broker the router needs, and closed after `data_link_idle` (2 min). Keeping them separate stops snapshot transfers from blocking messages behind them.
- **Framing.** Length-prefixed protobuf frames (`internal/peerproto`) with a version byte in the stream preamble.

Pin the `tailscale.com` module version. The packages used are internal-API-ish and may change between releases.

---

## 6. Endpoints and the WebSocket protocol

### 6.1 Transport

- `wss://<broker>/v1/ws` with subprotocol `nimbo.v1`. TLS may terminate at a load balancer.
- Every Action is one WebSocket **text** frame holding one JSON object. No binary frames in v1.
- Keepalive uses WebSocket ping/pong; the broker pings every 20s. A session is dropped after 60s of silence.
- Encodings: IDs and keys are unpadded lowercase base32. Token handles are `ncap1.` followed by base64url (no padding) of the binary token (§7.3).

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
- `welcome.brokers` suggests other Broker URLs for redundancy, preferring the same region. Endpoints may connect to several with the same key.
- A successful session causes an `EndpointUp` RCR entry, and its close causes `EndpointDown`.
- Being connected grants nothing. A new Endpoint holds no caps. It can only mint caps to itself (§7.4), which grants nothing to anyone else until it transfers them.

### 6.3 Actions

Requests carry a client-chosen `rid`. Each gets exactly one `result`: `{"op":"result","rid":N,"ok":true,...}` or `{"op":"result","rid":N,"ok":false,"error":{"code":"...","detail":"..."}}`.

| Op (E → B) | Fields | Result |
|---|---|---|
| `send` (**SendMessage**) | `cap`, `payload` (JSON object), `caps` (handles to carry, optional), `ttl_ms` (default 10000, max 60000), `reply` (optional: `schema`, `base`, `auto_reply`) | `msg` (message ID) and, if the target cap has `auto_reply`, `reply_id`: the origin ID of the reply cap. Replies arrive as `deliver` with `on = reply_id`, which gives correlation without a per-request `base`. The result means the message was accepted for routing, not that it was delivered. |
| `mint` (**MintCap**) | `schema` (optional JSON Schema), `base` (optional JSON object), `auto_reply` (bool) | `cap` (handle held by the caller, targeting the caller), `id` (origin cap ID) |
| `transfer` (**TransferCap**) | `cap`, `to` (Endpoint key) | `cap`: a handle bound to `to`, meaningful only to that Endpoint |
| `renew` (**RenewCap**) | `caps` (array of handles) | per-cap `{ok, deadline}`. The handles do not change. |
| `inspect` | `cap` | decoded record plus current liveness (debugging and SDK convenience) |

| Op (B → E) | Fields |
|---|---|
| `deliver` | `msg`, `on` (the origin cap ID the message arrived on, as returned by `mint`), `payload` (base fields merged in), `caps` (translated handles, now held by this Endpoint), `reply` (reply-cap handle if auto-reply applies) |
| `bounce` | `msg`, `reason` (`ttl_expired`, `receiver_busy`, `too_large`, `target_gone`). Best effort, and only to the sending session if it is still connected. |
| `closing` | `reason`, `retry_after_ms` (drain on shutdown) |

Error codes: `bad_request`, `invalid_cap`, `not_holder`, `expired`, `schema_violation`, `base_conflict`, `too_large`, `rate_limited`, `unavailable` (retryable).

### 6.4 Limits (defaults, from `welcome.limits`)

Payload ≤ 64 KiB, and no larger than the receiver's `max_message_bytes`. Schema ≤ 32 KiB, base ≤ 4 KiB. New blob bytes (schema or base not already live in the cluster) ≤ 64 KiB per session per 10s window. Up to 16 carried caps per message. 1000 actions/s per session, 100 mints/s per session, 4096 caps renewed or touched per session per 10s window (§10). Delivery queue per session: 1000 messages or 1 MiB.

---

## 7. Capabilities

### 7.1 Cap record

Encoded as deterministic CBOR (RFC 8949 §4.2.1):

| Field | Type | Notes |
|---|---|---|
| `v` | uint | 1 |
| `id` | 16B | this token's cap ID (random) |
| `origin` | 16B | cap ID of the mint this token descends from (`= id` for a minted cap) |
| `kind` | enum | `mint`, `transfer`, `reply` |
| `target` | 32B | Endpoint key that receives Messages |
| `holder` | 32B | Endpoint key allowed to use this token |
| `issuer` | 8B | Short ID of the Broker that rolled this record into its Merkle tree |
| `inc`, `epoch` | uint | issuer incarnation and batch number |
| `minted_at` | unix ms | |
| `schema` | 32B? | blob hash of a JSON Schema (draft 2020-12) |
| `base` | 32B? | blob hash of a JSON object |
| `auto_reply` | bool | mint a reply cap on each delivery |
| `hints` | [≤3]8B | Short IDs of Brokers where `target` was connected when `origin` was minted. Used for routing before gossip arrives. |

### 7.1a Blobs: schema and base in RCR

Schemas and base objects are **not** in the token. They are content-addressed blobs (`hash = SHA-256(bytes)`) published in RCR. The record holds only their hashes, and the Merkle leaf covers those hashes, so a token cannot be paired with a different schema or base.

- **Publish.** A Broker that issues a record referencing a blob (mint, transfer, translation or reply cap) publishes a `Blob{hash, kind, bytes}` WAL entry in the same flush as the batch root, unless the blob is already live in its view. Transfers, translations and reply caps reuse hashes the Broker already holds from validation, so usually only mints, and a reply cap with a new `reply.base` or `reply.schema`, publish bytes.
- **Dedup.** Views key blobs by hash. Mints that share a schema publish its bytes once, and every later use costs only liveness touches.
- **Liveness.** Blobs use the same sliding expiry as caps (§7.6). Publishing a blob sets its deadline to `now + cap_ttl`. Every use of a cap also uses its blobs, which generate touches under the same 10s threshold. A live cap therefore always has live blobs. A blob is dropped once `now > deadline + grace`. A Broker's snapshot includes the bytes of every blob it has published or touched that is still live, so snapshots are self-sufficient.
- **Lookup miss.** If a Broker validating a token lacks a blob, it fetches it from the issuer with a direct `GetBlobs` RPC, the same path as missing roots (§7.5).
- **Cost.** Gossip carries blob bytes once per distinct live blob, not once per cap. A unique `base` per mint does cost its bytes in gossip. That is why bases are capped at 4 KiB and new blob bytes have a per-session budget (§6.4). Request/reply correlation uses `reply_id` (§6.3), not a per-request base.
- Compiled schemas are cached by hash. A token is about 250 bytes plus a ≤512-byte proof, whatever the schema size, which keeps it within ESP32 memory.

### 7.2 Merkle rollups

Each Broker collects new leaves into a batch:

- `leaf = SHA-256(0x00 ‖ record)` and `node = SHA-256(0x01 ‖ L ‖ R)`. This is the RFC 6962 tree shape and proof format.
- **Flush policy: token bucket with burst.** Root publication is limited by a token bucket. Capacity is `rollup_burst` (default 20 roots) and it refills at one token per `rollup_interval` (50ms, configurable).
  - While the bucket has tokens, a pending batch is sealed **at once**. Each seal includes every leaf pending at that moment, so a burst of mints, or a lone mint on an idle Broker, adds no batching latency.
  - Once the bucket is empty, sealing falls back to the 50ms tick, or happens early when a batch reaches `rollup_max_leaves` (65536).
  - The bucket refills during quiet periods, so a Broker can absorb short bursts at full speed and only degrades to 50ms batching under sustained load.
  - Root publication per Broker is bounded at `rollup_burst + t / rollup_interval` roots over any window of length `t`: about 20 per second sustained, whatever the mint rate.
- The sealed root becomes one RCR WAL entry `CapRoot{inc, epoch, root, size, sealed_at}`. A batch of one leaf is the "direct" case: its root is the leaf.
- Mint, transfer, translation and reply-cap minting all wait for their batch to seal before returning or delivering the token. That wait is the latency-for-boundedness trade-off.

### 7.3 Token

```
token = 0x01 ‖ varint(len) ‖ record ‖ varint(leaf_index) ‖ varint(tree_size) ‖ path[≤16 × 32B]
handle = "ncap1." ‖ base64url(token)
```

The token is not secret: it is bound to `holder`, and using it requires a session authenticated as `holder`. A handle is at most about 1 KiB (a ~250-byte record plus a ≤512-byte proof, in base64url), however large the schema or base.

### 7.4 Operations

All of these happen on the Broker that receives the Action. Brokers keep **no per-endpoint cap tables**: tokens are stateless apart from the liveness entries in §7.6.

- **MintCap.** Hashes the supplied schema and base, compiles the schema (rejecting an invalid one), and builds the record `{kind: mint, target = holder = caller, origin = id, hints = brokers the caller is connected to}`. Returns the token once its batch seals; new blobs are published in the same flush (§7.1a).
- **TransferCap(cap, to).** Validates that the caller holds `cap`, counting it as a use (§7.6). Mints `{kind: transfer, holder = to}`, copying target, schema, base, auto_reply, origin and hints. It is **never** gated on whether `to` exists, is connected or is known, so it leaks nothing about `to`. The original token stays valid. This is the out-of-band bootstrap path: an owner service transfers a cap to a sandbox's public key and hands the sandbox the returned handle, for example through an environment variable.
- **Translation of carried caps** happens at the receive-side (egress) Broker (§8.2). It is the same as a transfer to the receiver, minted and rolled up by the egress Broker.
- **Reply caps.** When a Message is delivered on a cap with `auto_reply`, the egress Broker mints `{kind: reply, target = sender, holder = receiver, origin = reply_id, hints = sender's brokers}`. `reply_id` is chosen at ingress and returned to the sender in the `send` result (§6.3), so the sender can match replies without any blob. Its `schema`, `base` and `auto_reply` come from the sender's `send.reply` options, each defaulting to none, none and `false`. The ingress Broker hashes and publishes any reply schema or base blob, and the Forward carries only their hashes.

### 7.5 Validation (at the ingress Broker, for every use)

Given a handle presented by session `S` authenticated as Endpoint key `K`:

1. Decode the token and check the size limits. Require `record.holder == K`, else `not_holder`.
2. `L = leaf(record)`, `lid = L[:16]`.
3. **Liveness path.** If `liveness[lid]` exists and `now ≤ liveness[lid] + grace`, the token is valid. Some Broker validated it in full before creating that entry, and forging a record with the same `lid` needs a 128-bit second preimage.
4. **Mint path.** Otherwise, if `now ≤ minted_at + cap_ttl + grace`, look up `root(issuer, inc, epoch)` in the RCR view:
   - known: verify the inclusion proof. Valid → OK.
   - unknown (gossip hasn't arrived yet): send a direct `GetRoots` RPC to the issuer over a data link (singleflight, 1 RTT). If the issuer is unreachable, wait up to `root_wait` (2s) for gossip, else fail with `unavailable` (retryable).
5. Otherwise the token is `expired`.
6. Resolve the `schema` and `base` blobs from the view. On a miss, fetch them with `GetBlobs` from the issuer, using the same singleflight, `root_wait` fallback and `unavailable` error as for roots. A fetched blob must hash to the requested value.
7. If the target Endpoint has had no session anywhere for more than `cap_ttl`, fail with `target_gone`, and the use does not extend the deadline. This is how caps to departed ephemeral Endpoints die. It tells a holder whether its target is present, which the holder could already learn by sending.

Roots only need to be kept for `cap_ttl + grace` plus a margin: 90s.

### 7.6 Liveness: sliding 1-minute expiry

**Rule.** A cap expires `cap_ttl` (60s) after its last **use**. A use is any of: SendMessage on it, carrying it in a Message, TransferCap of it, or RenewCap. RenewCap refreshes the deadline **without** issuing a new token. Each token (each leaf) has its own deadline; using a transferred copy does not extend its parent.

**Mechanism: soft, eventually consistent.**

- Each Broker keeps `local_deadline[lid]` for caps used through it. Every use sets it to `now + cap_ttl`.
- The same rule applies to the cap's `schema` and `base` blobs, keyed by blob hash. A blob shared by many caps generates at most one touch per Broker per 10s.
- If `local_deadline − last_published_deadline ≥ touch_threshold` (10s), the Broker queues a `Touch{lid, deadline}`. Touches are flushed in the same 50ms cadence as roots, as one `CapTouches` WAL entry per flush, and are deduplicated within the window.
- Every Broker folds received touches into `liveness[lid] = max(deadline)`, and drops an entry once `now > deadline + grace`.
- `grace` (15s) covers the touch threshold, gossip lag and clock skew. Effective expiry is therefore **60–75s after the last use, as seen across the Cluster**. A cap used within the last 60s is accepted everywhere when gossip lag is under about 5s. A cap idle for more than 75s is rejected everywhere.
- **Cost.** A cap used only within its first 10s, which covers most reply caps, generates no touch gossip. A long-lived cap in steady use costs about one 24-byte touch per 10s. At 1M live caps that is about 100k touches/s and about 2.4 MB/s of origination across the Cluster. This is the largest gossip consumer, and per-session touch budgets bound it (§10).
- Brokers need loosely synced clocks (NTP, under 1s skew). Deadlines are absolute unix-ms values.

**No revocation.** As decided, there is no RevokeCap. A cap in use lives indefinitely. An Endpoint withdraws access by disconnecting and rotating its ephemeral key, after which caps to it die with `target_gone`. Applications that need per-client withdrawal can mint one cap per client with a distinguishing `base` field and ignore messages from withdrawn clients.

---

## 8. Messages and routing

### 8.1 Ingress (sender's Broker)

1. Validate the target cap (§7.5) and every carried cap. Carried caps must also be held by the sender. Each validated cap counts as a use (§7.6).
2. The payload must be a JSON object. Validate it against the schema blob, if `record.schema` is set. The **base fields are not** covered by the schema.
3. Merge the base blob into the payload shallowly. If the payload already has a top-level key that is in `base`, reject with `base_conflict`.
4. Assign `msg` (16 random bytes), set `deadline = now + ttl_ms`, and reply `result{ok, msg}`.
5. **Route.** The candidates are the Brokers that list a live session for `record.target` in the RCR endpoint directory (§9.4), plus `record.hints`. Prefer local delivery. Otherwise pick by rendezvous hash of `(record.id, broker id)` among the candidates. That keeps a single sender's messages on one cap on one path, so they arrive FIFO.
6. Send `Forward{msg, deadline, hop, target_record, payload, carried_records[], reply_id, reply_opts (blob hashes + auto_reply), ingress}` over a data link. The target cap's record and the carried records travel with the message. This is the fast path: the egress Broker never needs to look them up and trusts the ingress Broker's validation.

### 8.2 Egress (target's Broker)

1. Pick the target's newest live session on this Broker. If there is none, hold the message (below).
2. Translate each carried record into a transfer to the receiver (§7.4). If `auto_reply` is set, mint a reply cap. Both wait for the batch to seal.
3. Write `deliver` to the session. Once written, the message is never re-sent anywhere.

### 8.3 Delivery semantics (decided: at-most-once + TTL)

- **At most once.** A Broker hands a message to a peer only through `Forward`, and the peer replies `accepted` or `rejected(reason)`. If the link fails before the reply, the outcome is unknown and the message is **dropped**, never re-sent.
- **TTL queueing.** Any Broker holding a message it cannot deliver yet keeps it until `deadline` and re-routes when the endpoint directory changes. Examples: the ingress Broker before the target is known, or the egress Broker while the target reconnects, possibly to a different Broker. Hop limit is 4. On expiry or a full queue, a best-effort `bounce` goes back to the ingress Broker and on to the sending session.
- **Ordering.** FIFO per (sender session, target cap) while the route is stable. No ordering across failover or across caps.
- **Delivery receipts** are not a protocol feature. They are built with reply caps.
- **Backpressure.** Writes to the WebSocket rely on TCP backpressure. When the per-session queue is full, new messages bounce with `receiver_busy`.

### 8.4 What the receiver learns

The receiver gets `on` (which of its minted caps the message arrived on), the merged payload, the translated caps and the reply handle. There is no explicit "from" field. The sender's Endpoint key is visible inside the reply cap's record, as its `target`, but knowing a key grants nothing.

---

## 9. Runtime Control Records and gossip

### 9.1 Origins, snapshots and WAL

Each Broker is the **only writer** of its origin log, which is `(broker_id, incarnation, seq)`-ordered. WAL entry types:

| Entry | Content |
|---|---|
| `BrokerInfo` | version, ws_urls, region, started_at (on change) |
| `Heartbeat` | every 10s: session count, msg/s in and out, bytes/s, queue depth, CPU and memory, gossip lag |
| `EndpointUp` / `EndpointDown` | `key`, `session`, `at` (Up and Down in the same 50ms window are coalesced) |
| `CapRoot` | `epoch`, `root`, `size`, `sealed_at` |
| `CapTouches` | `[(lid, deadline)]` and `[(blob_hash, deadline)]` |
| `Blob` | `hash`, `kind` (schema/base), `bytes`. Published in the same flush as the root that first references it. |

The **snapshot** is the folded state of an origin at some `seq`: BrokerInfo, last metrics, live sessions, roots younger than 90s, this origin's unexpired touches, and the bytes of live blobs this origin has published or touched. The owner writes a new snapshot when the WAL since the last one exceeds the snapshot's size, or every 30s, and then truncates the WAL. Merging is always per origin: a higher `(incarnation, seq)` replaces a lower one, and nothing is ever merged across writers. This is why no consensus is needed.

### 9.2 Gossip peers (decided: N XOR-nearest + ring links)

From the live membership, each Broker gossips with:

- the `gossip_n` (default 8) Brokers nearest by `XOR(broker_id)`, and
- its **successor and predecessor** in the ring of Broker IDs in sorted order.

The two ring links guarantee a connected graph. Without them, XOR-nearest selection alone splits the Cluster along ID prefixes once more than N Brokers share a prefix. Links are recomputed when membership changes, with a short hold-down so peers do not flap.

### 9.3 Sync protocol (per gossip link)

1. **On connect,** both sides exchange a digest: `[(origin, inc, snap_seq, last_seq)]` for every origin they hold.
2. For each origin where one side is ahead, it sends the WAL suffix if the other side's `last_seq ≥ own snap_seq`, otherwise snapshot + WAL.
3. **Then eager push:** every entry a Broker newly accepts, from its own origin or relayed, is pushed to all gossip peers except the one it came from. Entries are deduplicated by `(origin, inc, seq)`.
4. **Anti-entropy:** digests are re-exchanged every 5s (about 4 KB at 100 origins) and fix anything the push missed.

Each Broker relays every origin it knows, which is what the concept notes require. If eager push costs too much in duplicate traffic at the top of the scale range, the next step is Plumtree-style eager/lazy push (see §15).

### 9.4 Derived views

Each Broker folds all origin states into in-memory indexes:

- **Endpoint directory:** `key → {(broker, session, since)}`. Only origins whose Broker is currently in S3 membership count for routing.
- **Roots:** `(issuer, inc, epoch) → root`.
- **Liveness:** `lid → max deadline`.
- **Blobs:** `hash → (bytes, max deadline)`, with compiled schemas cached alongside.
- **Brokers:** metrics and info for observability and `welcome.brokers` suggestions.

Rough memory at 1M endpoints and 1M live caps is about 100 MB for the directory, 40 MB for liveness and under 10 MB for roots. Blob memory depends on how many distinct schemas and bases are live; with shared schemas and small bases it should stay under ~100 MB.

### 9.5 Retention

A Broker keeps the snapshot + WAL of origins it does not own for up to `rcr_retention` (4h) after the last entry it received from that origin. A live Broker heartbeats every 10s, so this only affects dead or departed Brokers. Their state stays for routing hygiene, where it is ignored because they are not members, and for restart recovery. All RCR state lives in memory only and is never persisted.

### 9.6 Restart recovery (best effort)

On start, with a persisted node key:

1. Join membership and the mesh. Before accepting endpoints, ask the `gossip_n` nearest peers for **its own** origin's latest snapshot + WAL. Wait up to `recover_timeout` (5s).
2. Start a new incarnation `inc' = max(recovered_inc + 1, unix_ms)`. Publish a snapshot that carries over the unexpired roots, touches and blobs. Sessions are not carried over; their endpoints have disconnected and will reconnect. Peers replace the old incarnation with this one.
3. If nothing is recovered, start with a fresh incarnation.

Why it matters: without it, a new empty incarnation would replace the old one everywhere. The roots, touches and blobs that keep this Broker's previously issued tokens valid would be wiped, and live caps would break.

---

## 10. Security and abuse resistance

- **Trust boundaries.** S3 write access means cluster admin. Brokers fully trust each other. Endpoints are untrusted. DERP relays and the network see only WireGuard ciphertext, protected by a PSK.
- **Unforgeability.** A token is valid only through a Merkle proof against a root its issuer published, or through a liveness entry that a Broker created after full validation. Holder binding means a leaked handle is useless without the holder's private key. Blob hashes are inside the Merkle leaf, and blobs are content-addressed, so a schema or base cannot be substituted.
- **No side channels on transfer.** TransferCap does not look at the recipient.
- **Permissionless DoS.** Limits per IP on handshakes and per session on actions, mints, carried caps and bytes. Payload size and schema-compile limits: schema ≤ 32 KiB, no remote `$ref`, compile time capped.
- **Touch amplification.** An endpoint could mint many caps and renew them to inflate gossip. The touch budget per session (4096 distinct caps per 10s) caps this; uses beyond the budget fail with `rate_limited` and are not touched.
- **Blob amplification.** An endpoint could mint caps with a fresh unique base each time, pushing new bytes into gossip. The new-blob budget per session (64 KiB per 10s) caps this; mints beyond it fail with `rate_limited`.
- **Replay of handshake.** The broker's ephemeral key and the nonce are single-use, and the proof binds the dialed authority.
- **Payload privacy.** Brokers see payloads in clear (they must validate schemas). End-to-end encryption is a future layer and could use the endpoint X25519 keys.

---

## 11. Go implementation layout

```
cmd/nimbod/                 main, flags, `nimbod init`, `nimbod run`
internal/config/            config file + flags, cluster.json model
internal/membership/        S3 member objects, poll/heartbeat, fast-join GET
internal/mesh/              magicsock + wireguard-go + netstack wiring, ULA addressing, peer dial/listen
internal/derpsrv/           embedded DERP server + STUN, DERP map from membership
internal/peerproto/         protobuf messages + framing for broker↔broker
internal/gossip/            origin logs, snapshot/WAL, digests, peer selection (XOR + ring), sync
internal/rcr/               entry types, folding into views (directory, roots, liveness, brokers)
internal/cap/               record (deterministic CBOR), merkle (RFC 6962), token codec, rollup batcher (token bucket), validator, liveness, blob store
internal/schema/            JSON Schema compile cache and validation
internal/router/            ingress/egress, Forward, TTL queues, bounces, rendezvous routing
internal/wsapi/             WebSocket server, handshake, Action codec, session lifecycle, limits
internal/metrics/           Prometheus, /debug/rcr, /healthz
pkg/client/                 Go endpoint SDK (reference implementation of the protocol)
docs/protocol.md            endpoint wire protocol spec, for the ESP32 / Elixir SDKs
```

**Dependencies:** `tailscale.com` (magicsock, wgengine/wireguard-go, netstack, derp), `github.com/coder/websocket`, `github.com/aws/aws-sdk-go-v2/service/s3`, `github.com/santhosh-tekuri/jsonschema/v6`, `github.com/fxamacker/cbor/v2`, `google.golang.org/protobuf`, `golang.org/x/crypto/curve25519`.

**Concurrency model:** one goroutine per session for reads and one for writes; a single-writer goroutine per origin log; the rollup batcher on a ticker; views behind RCU-style copy-on-write maps (sharded for the directory and liveness).

**Testing:** unit tests for the Merkle, token, validation and liveness maths. An in-process multi-broker harness with an in-memory S3 and the mesh replaced by an in-memory transport. Netem-based integration tests over the real mesh for partitions, DERP-only paths and restarts. Property tests: no message delivered twice, no cap accepted after 75s idle, gossip converges after partitions heal.

---

## 12. Milestones

1. **Single broker:** WebSocket API, handshake, mint/transfer/renew/send, Merkle rollup with burst bucket, tokens, schema and base blobs (local only), reply caps, Go SDK.
2. **Mesh + membership:** S3 membership, magicsock mesh, embedded DERP, Forward between Brokers, endpoint directory (temporarily via direct broadcast).
3. **RCR gossip:** origin logs, snapshot + WAL, XOR + ring peers, liveness touches, root fetch, restart recovery, 4h retention.
4. **Hardening:** limits and touch budgets, metrics and debug endpoints, chaos tests, protocol spec for third-party SDKs (ESP32 C, Elixir).

---

## 13. Decision log

Decided with the project owner on 2026-10-12:

| # | Question | Decision |
|---|---|---|
| D1 | Gossip peer selection (XOR-nearest alone partitions) | N XOR-nearest **plus ring successor/predecessor** |
| D2 | Capability lifetime beyond 1 minute | **Any use extends the deadline**, plus **RenewCap** that refreshes it without a new token |
| D3 | Consistency of the sliding deadline | **Soft expiry**: touches gossiped at ≥10s extension, 15s grace (60–75s effective) |
| D4 | Withdrawing access | **No revocation.** The target endpoint rotates its key. |
| D5 | Delivery guarantee | **At-most-once + TTL** (default 10s, max 60s), FIFO per path, best-effort bounces |
| D6 | DERP relays | **Embedded in `nimbod`**, DERP map built from S3 membership |
| D7 | Payload model | **JSON object only.** JSON text frames for all Actions. |
| D8 | Scale target | **~100 Brokers / ~1M endpoints / ~1M live caps** |
| D9 | Where schema and base live | **In RCR** as content-addressed blobs; the cap record holds only their hashes (§7.1a) |
| D10 | Broker node key source | **`NIMBOD_NODE_KEY` env var first**, then `--state-dir/node.key`, then a fresh key (§5) |
| D11 | Rollup latency | **Burst allowance** (token bucket, `rollup_burst` = 20) before falling back to 50ms batching (§7.2) |

## 14. Defaults open to change

These were chosen without the owner; each is easy to change.

1. **Blob limits and budget:** schema ≤ 32 KiB, base ≤ 4 KiB, 64 KiB of new blob bytes per session per 10s. **Blob liveness** reuses the cap touch mechanism (§7.1a).
2. **`reply_id`** is returned by `send` so that replies can be correlated without a per-request base blob.
3. **A base field collision rejects** the message (`base_conflict`) rather than letting the base silently overwrite the payload key.
4. **Reply caps are ordinary caps**: reusable until idle for 60s, not single-use. The sender sets their schema, base and auto_reply.
5. **Validation at ingress** (sender's Broker). Invalid traffic never crosses the mesh, and egress trusts ingress.
6. **Missing root or blob:** a direct `GetRoots` / `GetBlobs` RPC to the issuer before falling back to waiting for gossip.
7. **A use does not extend a cap whose target has been absent for more than 60s** (`target_gone`), so caps to departed ephemeral endpoints die.
8. **No sender identity** in `deliver`, beyond what the reply cap's record implies.
9. **S3 timings:** heartbeat 15s, poll 30s, member TTL 60s, plus a fast-join GET on contact from an unknown key.
10. **Tunables:** `gossip_n=8`, `rollup_interval=50ms`, `rollup_burst=20`, `cap_ttl=60s`, `touch_threshold=10s`, `grace=15s`, `rcr_retention=4h`.

## 15. Future work

- An endpoint directory sharded by XOR distance (DHT-style), so the full endpoint set no longer has to be replicated to every Broker past ~1M endpoints or ~100 Brokers.
- Plumtree eager/lazy push to cut duplicate gossip.
- Optional end-to-end payload encryption using endpoint X25519 keys.
- Attenuating transfers, where the transferrer adds base fields or tightens the schema.
