# Nimbo design (v1)

Nimbo is a distributed, capability-based messaging fabric for AI agents, isolated runtimes and external devices. This document refines the concept notes into a design that can be built. It is written in Go and ships as one binary, `nimbod`.

Status: draft for review. The decisions taken with the project owner are listed in [§13](#13-decision-log). Defaults chosen without the owner, which are open to change, are in [§14](#14-defaults-open-to-change).

---

## 1. Goals and non-goals

**Goals**

- One standalone service, `nimbod` (the Broker). Brokers form a Cluster peer to peer, with no consensus and no leader.
- Endpoints are permissionless and untrusted. Connecting to a Broker grants nothing. All authority comes from Capabilities.
- Sending to an Endpoint looks the same to the sender whether that Endpoint is on the same Broker or a different one.
- No control state is replicated to every Broker. Each record (a cap, an endpoint's presence, a schema or base blob) lives on a small, fixed number of Brokers chosen by XOR distance. The cost of a record is the same whatever the Cluster size.
- Endpoints range from multi-tenant services to ESP32 devices. The endpoint protocol must be implementable with only X25519, HMAC-SHA256, JSON and WebSocket.

**v1 scale target:** ~100 Brokers, ~1M concurrent Endpoints and ~1M long-lived Capabilities in active use across the Cluster. Nothing in the design replicates per-endpoint or per-cap state to every Broker. Larger clusters are limited mainly by S3 membership polling and by every Broker knowing the full member list, which stays manageable into the low thousands.

**Non-goals (v1):** durable or at-least-once delivery, payloads other than JSON, revocation, end-to-end payload encryption (possible later, because endpoint keys are X25519), multi-cluster federation, and a materialised cluster-wide view on any single Broker.

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
| **Cap ID** | 16 random bytes, written as unpadded lowercase base32 (26 chars). It **is** the token an Endpoint holds. Not secret, and meaningful only to its holder. |
| **RCR** | Runtime Control Records: **cap records**, **presence records** and **blobs**. Each one is stored on its placement set (below) and is never gossiped cluster-wide. |
| **Placement key** | The 32-byte value a record is placed by: `SHA-256(cap ID)` for caps, `SHA-256(endpoint key)` for presence, and the blob hash for blobs. |
| **Placement set** | The Brokers that store a record: the 3 live Brokers nearest to its placement key by XOR distance. For caps it is the issuing Broker plus the 2 nearest others. |
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
 │ mesh (magicsock + wireguard-go + gVisor netstack, embedded DERP)
 └───────┬────────┘└─────┬──────┘ └──────┬──────┘
         └─── WireGuard over UDP / DERP ─┘
                         │
                  ┌──────▼──────┐
                  │ S3 bucket   │  membership + cluster secrets (root of trust)
                  └─────────────┘
```

A Broker has six internal subsystems:

1. **Membership**: reads and writes `members/` in S3. It is the only state every Broker shares.
2. **Mesh**: an in-process Tailscale data plane. Brokers reach each other on private IPv6 addresses through a userspace TCP/IP stack.
3. **RCR store**: XOR placement, replication, lookup, handoff and liveness for cap, presence and blob records (§7).
4. **Capabilities**: minting, transfer, validation and sliding expiry (§8).
5. **Router**: ingress validation, endpoint location, egress delivery, forwarding between Brokers, TTL queues (§9).
6. **Endpoint API**: WebSocket server, handshake and Actions (§6).

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
- **Rejoin announce.** On start, after writing its member object, a Broker sends `Rejoin{incarnation}` directly to every live member. This updates their membership view at once and triggers handoff to it (§7.4), instead of waiting for the next poll.
- **Leave.** On graceful shutdown a Broker hands off its records (§7.4) and then DELETEs its member object.
- Any S3-compatible store works: AWS, R2, MinIO, GCS through its S3 API. The client is `aws-sdk-go-v2`.

**Trust model.** Write access to the prefix is cluster admin. `cluster.json` holds the WireGuard pre-shared key and the DERP mesh key.

Membership is the input to every placement decision. Brokers can briefly disagree on it (by up to one poll interval), and the lookup fan-out and handoff in §7 are sized to absorb that.

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
- **DERP, embedded in `nimbod`.** A Broker configured with a public hostname and a TLS certificate serves DERP at `/derp` on its HTTPS listener (the endpoint WebSocket listener can share it) and STUN on UDP 3478. It advertises itself in its member object. Every Broker builds the same DERP map from membership: relay Brokers with the same configured `region_id` form one DERP region and mesh with each other using `derp_mesh_key`. Brokers without public reachability are DERP clients only. A Cluster needs at least one relay Broker. If all Brokers can reach each other directly over UDP, DERP is used only for rendezvous.
- **Connections.** Peer RPC listens on TCP `[ula]:7946` inside the netstack. Links are opened lazily to any Broker that needs to be reached and closed after `link_idle` (2 min). Each link uses two TCP streams: one for messages (`Forward`) and one for RCR traffic, so that a handoff burst cannot hold up messages.
- **Framing.** Length-prefixed protobuf frames (`internal/peerproto`) with a version byte in the stream preamble. Small RCR writes (stores, touches, presence updates) to the same Broker are coalesced into one frame. RPCs:
  - `Forward` / `ForwardResult`: messages (§9);
  - `Store`, `Get`, `Touch`: records of each kind (§7);
  - `Watch` / `Notify`: presence changes (§9.4);
  - `Rejoin`, `Handoff`: membership changes (§4, §7.4).

Pin the `tailscale.com` module version. The packages used are internal-API-ish and may change between releases.

---

## 6. Endpoints and the WebSocket protocol

### 6.1 Transport

- `wss://<broker>/v1/ws` with subprotocol `nimbo.v1`. TLS may terminate at a load balancer.
- Every Action is one WebSocket **text** frame holding one JSON object. No binary frames in v1.
- Keepalive uses WebSocket ping/pong; the broker pings every 20s. A session is dropped after 60s of silence.
- Encodings: IDs, including cap IDs, and keys are unpadded lowercase base32.

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
- `welcome.brokers` suggests 2 other Broker URLs for redundancy: same region first, then the XOR-nearest to the endpoint key, which spreads endpoints evenly. Endpoints may connect to several Brokers with the same key.
- A successful session writes an `Up` to the endpoint's presence record, and its close writes a `Down` (§9.1).
- Being connected grants nothing. A new Endpoint holds no caps. It can only mint caps to itself (§8.3), which grants nothing to anyone else until it transfers them.

### 6.3 Actions

Requests carry a client-chosen `rid`. Each gets exactly one `result`: `{"op":"result","rid":N,"ok":true,...}` or `{"op":"result","rid":N,"ok":false,"error":{"code":"...","detail":"..."}}`.

| Op (E → B) | Fields | Result |
|---|---|---|
| `send` (**SendMessage**) | `cap`, `payload` (JSON object), `caps` (cap IDs to carry, optional), `ttl_ms` (default 10000, max 60000), `reply` (optional: `schema`, `base`, `auto_reply`) | `msg` (message ID) and, if the target cap has `auto_reply`, `reply_id`: the lineage ID of the reply cap. Replies arrive as `deliver` with `on = reply_id`, which gives correlation without a per-request `base`. The result means the message was accepted for routing, not that it was delivered. |
| `mint` (**MintCap**) | `schema` (optional JSON Schema), `base` (optional JSON object), `auto_reply` (bool) | `cap`: a new cap ID held by and targeting the caller. It is also the lineage ID that `deliver.on` reports for messages on this cap and its transferred copies. |
| `transfer` (**TransferCap**) | `cap`, `to` (Endpoint key) | `cap`: a new cap ID bound to `to`, meaningful only to that Endpoint |
| `renew` (**RenewCap**) | `caps` (array of cap IDs) | per-cap `{ok, deadline}`. The IDs do not change. |
| `inspect` | `cap` | the record (with schema and base resolved) and current deadline (debugging and SDK convenience) |

| Op (B → E) | Fields |
|---|---|
| `deliver` | `msg`, `on` (lineage ID of the cap the message arrived on: the ID `mint` returned, or the sender's `reply_id`), `payload` (base fields merged in), `caps` (new cap IDs for the carried caps, now held by this Endpoint), `reply` (reply cap ID if auto-reply applies) |
| `bounce` | `msg`, `reason` (`ttl_expired`, `receiver_busy`, `too_large`, `target_gone`). Best effort, and only to the sending session if it is still connected. |
| `closing` | `reason`, `retry_after_ms` (drain on shutdown) |

Error codes: `bad_request`, `invalid_cap`, `not_holder`, `expired`, `target_gone`, `schema_violation`, `base_conflict`, `too_large`, `rate_limited`, `unavailable` (retryable).

### 6.4 Limits (defaults, from `welcome.limits`)

- **Messages:** payload ≤ 64 KiB, and no larger than the receiver's `max_message_bytes`. Up to 16 carried caps per message.
- **Blobs:** schema ≤ 32 KiB, base ≤ 4 KiB. New blob bytes (a schema or base not already live in the Cluster) ≤ 64 KiB per session per 10s.
- **Rates per session:** 1000 actions/s and 100 mints/s. 4096 distinct caps used or renewed per 10s. 100 lookups of unknown cap IDs per 10s (§10).
- **Delivery queue per session:** 1000 messages or 1 MiB.

---

## 7. RCR store: XOR placement

All control state is a set of records. Each record lives on its **placement set**, a few Brokers chosen by XOR distance from its placement key. Nothing is gossiped and no Broker holds a full copy. Every Broker computes placement sets the same way, from S3 membership.

### 7.1 Record kinds

| Kind | Placement key | Placement set | Content | Mutable state |
|---|---|---|---|---|
| **Cap** | `SHA-256(cap ID)` | issuing Broker + 2 nearest others | immutable cap record (§8.2), ~150 B | `deadline` |
| **Presence** | `SHA-256(endpoint key)` | 3 nearest | live sessions `[(broker, incarnation, session, since)]`, `last_seen` | sessions, `last_seen` |
| **Blob** | blob hash | 3 nearest | `kind` (schema/base), bytes | `deadline` |

`replicas = 3` is a cluster-wide tunable. For caps, the issuer is always one of the holders because most uses happen on the Broker that created the cap, which then needs no network round trip.

### 7.2 Writes

- **Async.** A Broker writing a record stores any local copy at once and sends `Store` to the other members of the placement set without waiting for acknowledgement, coalesced per link. No client-visible operation waits on replication.
- **Merge.** Holders merge writes deterministically:
  - cap and blob content is immutable, so the first write wins and later writes only raise `deadline` to the maximum;
  - presence session sets are keyed by `(broker, incarnation, session)`. An `Up` adds a session, a `Down` removes it, and `last_seen` takes the maximum.

  Merges are commutative and need no coordination, so there is no consensus.

### 7.3 Lookups

1. **Local first:** records this Broker holds, then the local cache. The cache is an LRU of records received in a `Forward`, from earlier lookups, or created here.
2. **Otherwise** send `Get` in parallel to the `lookup_fanout` (4) live Brokers nearest to the placement key. Asking one more than `replicas` covers a placement set that shifted in a recent membership change. Answers are merged with the same rules as writes.
3. **On a miss,** retry with backoff for up to `lookup_wait` (1s). This covers a record that was used on another Broker before its asynchronous `Store` arrived.

Blobs are immutable, so a cached blob is always valid. Cached cap records may hold a stale `deadline`, and cached presence may be stale. §8 and §9 say when each is re-read.

### 7.4 Handoff and restarts

- **On any membership change** (a join, a leave, a member expiring, or a Rejoin with a new incarnation), every Broker recomputes the placement set of each record it holds. It pushes the record to any Broker that newly belongs in that set. A holder that is no longer in the set keeps the record as cache only.
- **Presence is pushed by its owners.** A Broker also re-pushes the presence of its own live sessions whenever their placement set changes, and in full every `presence_reconcile` (5 min). The session-owning Broker is the source of truth for its sessions, so lost presence copies repair themselves.
- **Restart.** A Broker that restarts with the same node key announces `Rejoin` with a new incarnation (§4). Its peers hand its share of records back to it, and its own sessions are re-registered as endpoints reconnect. There is no snapshot recovery, and nothing is persisted.
- A record is lost only if every member of its placement set fails within one handoff window. A lost cap shows up as `invalid_cap` and the endpoint must be bootstrapped again. Lost presence is restored by its owner.

### 7.5 Expiry (sliding)

Cap and blob records have a `deadline` and use the same sliding rule:

- creating a record sets `deadline = now + cap_ttl` (60s);
- a Broker that sees a use sets its local deadline to `now + cap_ttl`. Once it has extended that by at least `touch_threshold` (10s) since its last touch, it sends `Touch{key, deadline}` to the placement set, coalesced;
- holders keep the maximum deadline and drop a record once `now > deadline + grace` (15s).

This gives **soft expiry, 60–75s after the last use** as seen across the Cluster. A record used only within its first 10s sends no touches. Presence entries are dropped `cap_ttl + grace` after `last_seen` once they have no live sessions. Brokers need loosely synced clocks (NTP, under 1s skew), since deadlines are absolute unix-ms values.

### 7.6 Cost at the v1 target

| | Per Broker (100 Brokers) | Cluster total |
|---|---|---|
| Presence for 1M endpoints | ~30k records, ~3 MB | 3 writes per connect or disconnect |
| 1M long-lived caps | ~30k records, ~5 MB | ~300k touches/s, coalesced into a few frames per link per 50ms |
| New cap (mint, transfer, translation, reply) | — | 2 remote `Store`s of ~150 B |
| Distinct live blobs | 3% of them | bytes stored 3×, cached on use |

---

## 8. Capabilities

### 8.1 Cap ID (the token)

A cap token is **just its cap ID**: 16 random bytes, written as unpadded lowercase base32 (26 characters), for example `k3v7q2m9x4td8hj5wz6c1ry0ab`. It carries nothing else.

The ID is not secret. What makes it usable only by its holder is the cap's record, which names the holder. A Broker accepts the ID only from a session authenticated as that holder. Each transfer creates a new ID for the new holder; the original ID stays valid for its own holder.

### 8.2 Cap record

The record is the immutable description of one cap, keyed by its ID and encoded as deterministic CBOR (RFC 8949 §4.2.1). Endpoints see it only through `inspect`.

| Field | Type | Notes |
|---|---|---|
| `lineage` | 16B | ID of the mint this cap descends from (`= id` for a minted cap). Reported to the target as `deliver.on`. |
| `kind` | enum | `mint`, `transfer`, `reply` |
| `target` | 32B | Endpoint key that receives Messages |
| `holder` | 32B | Endpoint key allowed to use this cap |
| `issuer` | 8B | Short ID of the Broker that created the record (a fixed member of its placement set) |
| `minted_at` | unix ms | |
| `schema` | 32B? | blob hash of a JSON Schema (draft 2020-12) |
| `base` | 32B? | blob hash of a JSON object |
| `auto_reply` | bool | mint a reply cap on each delivery |
| `hints` | [≤3]8B | Short IDs of Brokers where `target` was connected when `lineage` was minted. Routing tries them when presence is not yet known (§9.2). |

### 8.3 Operations

All of these happen on the Broker that receives the Action, which becomes the issuer of any new record. None of them wait on the network.

- **MintCap.** Hashes the supplied schema and base. Compiles the schema, rejecting an invalid one. Writes any blob not already live in the Cluster to its placement set; a Broker skips the write if it stored or touched the same blob within the last `touch_threshold`. Creates `{kind: mint, target = holder = caller, lineage = id, hints = brokers the caller is connected to}`.
- **TransferCap(cap, to).** Validates that the caller holds `cap`, counting it as a use (§8.5). Creates `{kind: transfer, holder = to}` with a new ID, copying target, schema, base, auto_reply, lineage and hints. It is **never** gated on whether `to` exists, is connected or is known, so it leaks nothing about `to`. The original cap stays valid. This is the out-of-band bootstrap path: an owner service transfers a cap to a sandbox's public key and hands the sandbox the returned ID, for example through an environment variable.
- **Translation of carried caps** happens at the receive-side (egress) Broker (§9.3). It is the same as a transfer to the receiver, issued by the egress Broker.
- **Reply caps.** When a Message is delivered on a cap with `auto_reply`, the egress Broker creates `{kind: reply, target = sender, holder = receiver, lineage = reply_id, hints = sender's brokers}`. `reply_id` is chosen at ingress and returned to the sender in the `send` result (§6.3), so the sender can match replies without any blob. Its `schema`, `base` and `auto_reply` come from the sender's `send.reply` options, each defaulting to none, none and `false`. The ingress Broker writes any reply schema or base blob, and the `Forward` carries only their hashes.

Schemas compiled from blobs are cached by hash. A unique `base` per mint costs one blob write per mint, which is why bases are small and new blob bytes have a per-session budget. Request/reply correlation uses `reply_id`, not a per-request base.

### 8.4 Validation (at the ingress Broker, for every use)

Given a cap ID presented by session `S`, authenticated as Endpoint key `K`:

1. Look up the cap record (§7.3). If it is not found, fail with `invalid_cap`.
2. Require `record.holder == K`, else `not_holder`.
3. Require `now ≤ deadline + grace`. A cached copy can hold a stale deadline. If it looks expired, re-read the record from its placement set before rejecting with `expired`.
4. Resolve the `schema` and `base` blobs: cache, then lookup.
5. The target must not be gone (§9.1). If it is, fail with `target_gone`, and the use does not extend the deadline. This is how caps to departed ephemeral Endpoints die. It tells a holder whether its target is present, which the holder could already learn by sending.

### 8.5 Liveness

**Rule.** A cap expires `cap_ttl` (60s) after its last **use**. A use is any of: SendMessage on it, carrying it in a Message, TransferCap of it, or RenewCap. RenewCap refreshes the deadline **without** issuing a new ID. Each cap ID has its own deadline; using a transferred copy does not extend its parent. A use of a cap is also a use of its schema and base blobs. The mechanism is §7.5.

**No revocation.** As decided, there is no RevokeCap. A cap in use lives indefinitely. An Endpoint withdraws access by disconnecting and rotating its ephemeral key, after which caps to it die with `target_gone`. Applications that need per-client withdrawal can mint one cap per client with a distinguishing `base` field and ignore messages from withdrawn clients.

---

## 9. Presence, routing and messages

### 9.1 Presence

Presence records answer "which Brokers is this Endpoint connected to right now?"

- **Writes.** A session opening sends `Up{key, broker, incarnation, session, since}` to the 3 Brokers nearest `SHA-256(key)`. A session closing sends `Down` and updates `last_seen`. Up and Down within the same 50ms window are coalesced.
- **Validity.** No per-endpoint lease is needed. A session entry counts only while its Broker is a live member with that same incarnation. Entries from dead or restarted Brokers are ignored and pruned. The owning Broker re-pushes its sessions on placement changes and every 5 minutes (§7.4).
- **Gone.** An Endpoint is **gone** when its presence record has no valid session and either its `last_seen` is more than `cap_ttl` ago or no record exists. Targets are always Endpoints that minted, so they were connected once. "No record" therefore means the target left more than 75s ago, or every copy was lost. A lost record is repaired by the owner's re-push if the endpoint is in fact still connected.

### 9.2 Ingress (sender's Broker)

1. Validate the target cap (§8.4) and every carried cap. Carried caps must also be held by the sender. Each validated cap counts as a use (§8.5).
2. The payload must be a JSON object. Validate it against the schema blob, if `record.schema` is set. The **base fields are not** covered by the schema.
3. Merge the base blob into the payload shallowly. If the payload already has a top-level key that is in `base`, reject with `base_conflict`.
4. Assign `msg` (16 random bytes), set `deadline = now + ttl_ms`, choose `reply_id` if the target cap has `auto_reply`, and reply `result{ok, msg, reply_id?}`.
5. **Locate the target**, cheapest source first:
   1. local sessions;
   2. the location cache (`key → brokers`, TTL 30s);
   3. a presence lookup (§7.3);
   4. if presence returns nothing, `record.hints`. This covers a just-connected target whose `Up` has not landed yet.

   Among several Brokers, pick by rendezvous hash of `(cap ID, broker id)`. That keeps one sender's messages on one cap on one path, so they arrive FIFO.
6. Send `Forward{msg, deadline, hop, target (cap ID + record), payload, carried[] (cap ID + record), reply_id, reply_opts (blob hashes + auto_reply), ingress}`. The records travel with the message. This is the fast path: the egress Broker does not look them up, caches them, and trusts the ingress Broker's validation.
7. If the reply is `not_here`, nothing was delivered. Invalidate the cache entry, look up presence again, and retry. If the target is known but has no live session, wait for it (§9.4).

### 9.3 Egress (target's Broker)

1. Pick the target's newest live session on this Broker. If there is none, answer `not_here`. If the target disconnected after the message was accepted, wait for it (§9.4).
2. Translate each carried cap into a transfer to the receiver, and if `auto_reply` is set, create a reply cap (§8.3). These are local writes with asynchronous replication, so delivery does not wait on them.
3. Write `deliver` to the session. Once written, the message is never re-sent anywhere.

### 9.4 Waiting for a reconnecting target

A Broker holding a message for a target with a recent `last_seen` but no live session sends `Watch{key, until = message deadline}` to the target's presence set. Those Brokers reply `Notify` when an `Up` arrives, and the holder then re-routes. Watches expire with their deadline, so watch state is bounded by queued messages. Hop limit is 4. If the target is gone, or the TTL or queue limit is reached, a best-effort `bounce` goes back to the ingress Broker and on to the sending session.

### 9.5 Delivery semantics (decided: at-most-once + TTL)

- **At most once.** A Broker hands a message to a peer only through `Forward`, and the peer replies `accepted`, `not_here` or `rejected(reason)`. `not_here` and `rejected` mean the peer did not take it, so retrying is safe. If the link fails before the reply, the outcome is unknown and the message is **dropped**, never re-sent.
- **Ordering.** FIFO per (sender session, target cap) while the route is stable. No ordering across failover or across caps.
- **Delivery receipts** are not a protocol feature. They are built with reply caps.
- **Backpressure.** Writes to the WebSocket rely on TCP backpressure. When the per-session queue is full, new messages bounce with `receiver_busy`.

### 9.6 What the receiver learns

The receiver gets `on` (which of its minted caps the message arrived on), the merged payload, the new IDs for the carried caps and the reply cap ID. There is no explicit "from" field. `inspect` on the reply cap shows its `target`, the sender's Endpoint key, but knowing a key grants nothing.

---

## 10. Security and abuse resistance

- **Trust boundaries.** S3 write access means cluster admin. Brokers fully trust each other, including for RCR writes and lookups. Endpoints are untrusted. DERP relays and the network see only WireGuard ciphertext, protected by a PSK.
- **Unforgeability.** A cap ID is usable only if its placement set holds a record for it. Only a Broker running Mint, Transfer, translation or reply-cap creation for an authenticated holder writes one. IDs are 128-bit random, so a made-up ID matches no record. Holder binding means a leaked ID is useless without the holder's private key. Blobs are content-addressed, so a schema or base cannot be substituted under a record.
- **No side channels on transfer.** TransferCap does not look at the recipient.
- **Permissionless DoS.** Limits per IP on handshakes and per session on actions, mints, carried caps and bytes. Schema-compile limits: schema ≤ 32 KiB, no remote `$ref`, compile time capped.
- **Touch amplification.** An endpoint could mint many caps and renew them to inflate touch traffic. The budget per session (4096 distinct caps per 10s) caps this; uses beyond the budget fail with `rate_limited`.
- **Lookup amplification.** Each unknown cap ID costs up to 4 `Get` RPCs, retried for up to 1s. A per-session budget on unknown-ID lookups (100 per 10s) caps this.
- **Blob amplification.** A fresh unique base on every mint would mean one blob write per mint. The new-blob budget per session (64 KiB per 10s) caps this.
- **Presence churn.** Reconnect storms cost 3 coalesced writes per session event. A per-IP handshake rate limit bounds the rate.
- **Replay of handshake.** The broker's ephemeral key and the nonce are single-use, and the proof binds the dialed authority.
- **Payload privacy.** Brokers see payloads in clear (they must validate schemas). End-to-end encryption is a future layer and could use the endpoint X25519 keys.

---

## 11. Go implementation layout

```
cmd/nimbod/                 main, flags, `nimbod init`, `nimbod run`, `nimbod keygen`, `nimbod ctl`
internal/config/            config file + flags, cluster.json model
internal/membership/        S3 member objects, poll/heartbeat, fast-join GET, Rejoin, change events
internal/mesh/              magicsock + wireguard-go + netstack wiring, ULA addressing, peer dial/listen
internal/derpsrv/           embedded DERP server + STUN, DERP map from membership
internal/peerproto/         protobuf messages + framing for broker↔broker, per-link write coalescing
internal/rcr/               placement (XOR), record stores per kind, merge rules, Store/Get/Touch,
                            lookup with fan-out, LRU caches, handoff, expiry sweeps
internal/cap/               cap record codec (deterministic CBOR), mint/transfer/reply, validator
internal/presence/          Up/Down, owner re-push, watches/notify, gone detection
internal/schema/            JSON Schema compile cache and validation
internal/router/            ingress/egress, Forward, location cache, TTL queues, bounces, rendezvous
internal/wsapi/             WebSocket server, handshake, Action codec, session lifecycle, limits
internal/metrics/           Prometheus, /debug endpoints, /healthz
pkg/client/                 Go endpoint SDK (reference implementation of the protocol)
docs/protocol.md            endpoint wire protocol spec, for the ESP32 / Elixir SDKs
```

**Dependencies:** `tailscale.com` (magicsock, wgengine/wireguard-go, netstack, derp), `github.com/coder/websocket`, `github.com/aws/aws-sdk-go-v2/service/s3`, `github.com/santhosh-tekuri/jsonschema/v6`, `github.com/fxamacker/cbor/v2`, `google.golang.org/protobuf`, `golang.org/x/crypto/curve25519`.

**Concurrency model:** one goroutine per session for reads and one for writes; sharded maps for record stores and caches; one coalescing writer per peer link and stream; a membership goroutine that publishes change events consumed by handoff.

**Observability:** each Broker exposes Prometheus metrics and `/debug` endpoints for its own stores. `nimbod ctl` builds cluster-wide views by listing members from S3 and querying each Broker. No Broker holds a cluster-wide view.

**Testing:**
- Unit tests for placement, merge rules, handoff, validation and expiry maths.
- An in-process multi-broker harness with an in-memory S3 and an in-memory transport replacing the mesh.
- Netem-based integration tests over the real mesh, covering partitions, DERP-only paths, restarts and churn.
- Property tests: no message delivered twice; no cap accepted after 75s idle; after churn stops, every live record is on its placement set.

---

## 12. Milestones

1. **Single broker:** WebSocket API, handshake, mint/transfer/renew/send, cap records and expiry, blobs, reply caps, Go SDK. The RCR store runs with a membership of one.
2. **Mesh + membership:** S3 membership, magicsock mesh, embedded DERP, Forward between Brokers.
3. **Distributed RCR:** XOR placement, Store/Get/Touch, lookups, presence with watches, handoff, Rejoin.
4. **Hardening:** limits and budgets, metrics and `nimbod ctl`, chaos and churn tests, protocol spec for third-party SDKs (ESP32 C, Elixir).

---

## 13. Decision log

Decided with the project owner on 2026-10-12:

| # | Question | Decision |
|---|---|---|
| D1 | Gossip peer selection (XOR-nearest alone partitions) | ~~N XOR-nearest plus ring successor/predecessor~~. Superseded by D14: there is no gossip. |
| D2 | Capability lifetime beyond 1 minute | **Any use extends the deadline**, plus **RenewCap** that refreshes it without a new token |
| D3 | Consistency of the sliding deadline | **Soft expiry**: touch at ≥10s extension, 15s grace (60–75s effective). Touches go to the record's placement set (D13, D14). |
| D4 | Withdrawing access | **No revocation.** The target endpoint rotates its key. |
| D5 | Delivery guarantee | **At-most-once + TTL** (default 10s, max 60s), FIFO per path, best-effort bounces |
| D6 | DERP relays | **Embedded in `nimbod`**, DERP map built from S3 membership |
| D7 | Payload model | **JSON object only.** JSON text frames for all Actions. |
| D8 | Scale target | **~100 Brokers / ~1M endpoints / ~1M live caps** |
| D9 | Where schema and base live | **Not in the token.** Content-addressed blobs referenced by hash; placement per D15. |
| D10 | Broker node key source | **`NIMBOD_NODE_KEY` env var first**, then `--state-dir/node.key`, then a fresh key (§5) |
| D11 | Rollup latency | ~~Burst allowance before 50ms batching~~. Superseded by D12: there is no rollup. |
| D12 | Token format | **Just the random 128-bit cap ID** in base32. No record or Merkle proof in the token, and no Merkle roots. |
| D13 | Where cap records live | **Issuer + 2 XOR-nearest replicas**, with handoff on membership change |
| D14 | Cluster-wide RCR propagation | **Removed.** No snapshot + WAL gossip. Endpoint presence lives on the **3 Brokers nearest `SHA-256(endpoint key)`**. Restart recovery is handoff. Metrics are per Broker. |
| D15 | Where blobs live | **3 Brokers nearest the blob hash**, cached anywhere on use |

## 14. Defaults open to change

These were chosen without the owner; each is easy to change.

1. **Blob limits and budget:** schema ≤ 32 KiB, base ≤ 4 KiB, 64 KiB of new blob bytes per session per 10s.
2. **`reply_id`** is returned by `send` so that replies can be correlated without a per-request base blob.
3. **A base field collision rejects** the message (`base_conflict`) rather than letting the base silently overwrite the payload key.
4. **Reply caps are ordinary caps**: reusable until idle for 60s, not single-use. The sender sets their schema, base and auto_reply.
5. **Validation at ingress** (sender's Broker). Invalid traffic never crosses the mesh, and egress trusts ingress.
6. **Asynchronous replication:** no client-visible operation waits for remote stores. A record used elsewhere within milliseconds of its creation is covered by `lookup_wait` retries.
7. **Presence validity from membership and incarnation**, with owner re-push every 5 min, instead of per-endpoint leases.
8. **Watches** at the presence set for messages waiting on a reconnecting target, instead of polling.
9. **S3 timings:** heartbeat 15s, poll 30s, member TTL 60s, plus a fast-join GET on contact from an unknown key and a Rejoin announcement on start.
10. **Tunables:** `replicas=3`, `lookup_fanout=4`, `lookup_wait=1s`, `cap_ttl=60s`, `touch_threshold=10s`, `grace=15s`, `presence_reconcile=5m`, location cache TTL 30s.

## 15. Future work

- Plumtree-style or SWIM-style membership dissemination if S3 polling becomes the bottleneck past a few thousand Brokers.
- Optional end-to-end payload encryption using endpoint X25519 keys.
- Attenuating transfers, where the transferrer adds base fields or tightens the schema.
- Lazy replication for short-lived caps (e.g. replicate a reply cap only if it survives ~1s) to cut replication traffic under high message rates.
