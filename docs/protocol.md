# Nimbo endpoint protocol (v1)

This is the wire protocol between an **Endpoint** and a **Broker** (`nimbod`). It is everything a third-party SDK (ESP32 C, Elixir, …) needs. It requires only X25519, HMAC-SHA256, SHA-256, JSON and WebSocket. The Go SDK in `pkg/client` is the reference implementation; the frame types are in `pkg/wire`.

## 1. Transport

- Connect to `wss://<broker>/v1/ws` with WebSocket subprotocol **`nimbo.v1`**. A Broker closes connections without it.
- Every frame, in both directions, is one WebSocket **text** frame holding one JSON object with an `"op"` field. Binary frames close the session.
- Streams (§10) use separate data sockets at `wss://<broker>/v1/stream?token=<token>`, subprotocol **`nimbo.stream.v1`**.
- The Broker pings every 20s. A session that does not answer is dropped after about 60s.
- Encoding: keys, cap IDs, lineage IDs, message IDs, nonces and proofs are **unpadded lowercase base32** (RFC 4648 alphabet `a-z2-7`). A 32-byte value is 52 characters; a 16-byte value is 26.
- Reconnect with jittered exponential backoff: base 250ms, doubling, capped at 30s. If the Broker sent `closing` with `retry_after_ms`, wait that long instead.

## 2. Identity

An Endpoint is identified by an X25519 public key (its **endpoint key**). Generate a fresh key per process or per identity; rotating the key is how an Endpoint withdraws access (caps targeting the old key die with `target_gone`). An Endpoint may hold sessions to several Brokers with the same key.

## 3. Handshake

```
B → E  {"op":"challenge","v":1,"broker":"<broker id hex>","eph":"<B_eph pub, b32>","nonce":"<32 bytes, b32>"}
E → B  {"op":"hello","key":"<E pub, b32>","authority":"<host[:port] E dialed>",
        "proof":"<32 bytes, b32>","client":"esp32-sdk/0.1","max_message_bytes":16384}
B → E  {"op":"welcome","session":"<id>","limits":{...},"brokers":["wss://..."]}
```

1. `ss = X25519(E_priv, B_eph_pub)`. Abort if `ss` is all zeros.
2. `k = SHA-256("nimbo/v1/auth" ‖ ss)` (the 13 ASCII bytes followed by the 32 bytes of `ss`).
3. `proof = HMAC-SHA256(k, nonce ‖ authority ‖ E_pub ‖ B_eph_pub)`, where `nonce` and the keys are raw bytes and `authority` is the UTF-8 `host[:port]` exactly as dialed (include the port only if the URL has one).

The Broker accepts the hello only if `authority` is one of its configured public names and the proof matches. Any failure closes the connection (`closing` with reason `auth_failed`). `max_message_bytes` (optional) caps the payloads delivered to this Endpoint. The handshake must complete within 10 seconds.

`welcome.limits` carries the limits below; `welcome.brokers` suggests up to two other Brokers (same region first) for redundant sessions.

A Broker that is starting, fenced or draining answers the connection with `closing` instead of `challenge`.

## 4. Requests and results

Every request carries a client-chosen `rid` (a positive integer, unique per session) and gets exactly one `result`:

```json
{"op":"result","rid":7,"ok":true, ...fields...}
{"op":"result","rid":7,"ok":false,"error":{"code":"invalid_cap","detail":"..."}}
```

Results may arrive in any order relative to other requests, except that sends on the same cap from the same session are processed in order.

### send (SendMessage)

```json
{"op":"send","rid":1,"cap":"<cap id>","payload":{...},
 "caps":["<cap id>", ...],          // optional: caps to carry (max 16)
 "ttl_ms":10000,                    // optional: default 10000, max 60000
 "reply":{"schema":{...},"base":{...},"auto_reply":false}}   // optional
```

Result: `{"msg":"<message id>","reply_id":"<lineage id>"}`. `reply_id` is present when the target cap has `auto_reply`; the reply, if any, arrives as `deliver` with `on` = `reply_id`. A successful result means the message was accepted for routing, not that it was delivered. `payload` must be a JSON object; it is validated against the cap's schema (if any), then the cap's `base` object is merged in (a payload key that is also in `base` fails with `base_conflict`).

`reply` sets the schema, base, auto-reply and `allow_stream` of the single-use reply cap the receiver gets.

### stream (open a stream)

```json
{"op":"stream","rid":6,"cap":"<cap id>","payload":{...},"caps":[...],"ttl_ms":10000,"reply":{...}}
```

The same fields and checks as `send`; the cap must also have `allow_stream` (else `stream_not_allowed`). Result: `{"stream":"<stream id>","token":"<52 chars>","msg":"<same id>","reply_id":"..."}`. The payload is delivered to the target as a stream **offer** (§5). Attach your data socket with `token` (§10). The outcome arrives later as one `stream_status` on this session.

### mint (MintCap)

```json
{"op":"mint","rid":2,"schema":{...},"base":{...},"auto_reply":true,"durable":false,"allow_stream":false}
```

Result: `{"cap":"<cap id>"}`, a cap targeting and held by the caller. This ID is also the **lineage ID** reported as `deliver.on` for messages on it and on its transferred copies. `schema` is a JSON Schema (draft 2020-12, ≤ 32 KiB, no remote `$ref`); `base` is a JSON object (≤ 4 KiB). `allow_stream: true` lets holders of the cap and of its transferred copies open streams to you (§10).

### transfer (TransferCap)

```json
{"op":"transfer","rid":3,"cap":"<cap id>","to":"<endpoint key>","durable":true}
```

Result: `{"cap":"<new cap id>"}`, held by `to`. The Broker does not check whether `to` exists or is connected. Hand the new ID to its holder out of band. Transferring a single-use cap consumes it (the new cap is single-use too).

`durable: true` (mint and transfer) returns only once two members of the cap's placement set hold the record, so the ID can be used from any Broker immediately. Use it whenever the ID leaves the session out of band.

### renew (RenewCap)

```json
{"op":"renew","rid":4,"caps":["<cap id>", ...]}      // up to 256
```

Result: `{"renewed":[{"cap":"...","ok":true,"ttl_ms":60000}, {"cap":"...","ok":false,"error":{...}}]}`. Renewing refreshes the TTL without changing the ID and does not consume single-use caps.

### inspect

```json
{"op":"inspect","rid":5,"cap":"<cap id>"}
```

Result: `{"info":{"lineage","kind","single_use","target","holder","issuer","minted_at","schema","base","auto_reply","allow_stream","ttl_ms"}}`. Only the holder may inspect a cap.

## 5. Broker → Endpoint frames

```json
{"op":"deliver","msg":"<id>","on":"<lineage id>","payload":{...},
 "caps":["<new cap id>", ...],      // carried caps, now held by this endpoint
 "reply":"<single-use cap id>"}     // when the cap has auto_reply
{"op":"deliver",...,"stream":"<stream id>","stream_token":"<52 chars>"}   // a stream offer
{"op":"stream_status","stream":"<id>","state":"accepted|failed","reason":"...","detail":"..."}
{"op":"bounce","msg":"<id>","reason":"ttl_expired|receiver_busy|too_large|target_gone|unavailable"}
{"op":"closing","reason":"fenced|draining|rate_limited|auth_failed|...","retry_after_ms":4321}
```

`on` tells the receiver which of its minted caps (or which of its earlier sends, via `reply_id`) the message arrived on. There is no sender field; `inspect` on the reply cap shows the sender's endpoint key as its target.

Bounces are best effort and go only to the sending session if it is still connected.

A `deliver` with `stream` is a stream offer: accept it, refuse it or ignore it (§10). An endpoint that does not support streams can treat it as a plain message; the stream then times out. `stream_status` goes, at most once, to the session that opened the stream, in place of a `bounce` for the offer: `accepted` when the target attached; `failed` with `ttl_expired`, `receiver_busy`, `target_gone` or `unavailable` (the offer bounced), `peer_attach_timeout` (no attach in time) or `canceled` (your data socket was lost before the target attached).

## 6. Capability lifetime

- A cap expires 60s after its last **use**: a send on it, carrying it, transferring it, or renewing it. Brokers accept it for between 60 and 75 seconds after the last use, then reject it (`expired`, later `invalid_cap`). Renew long-lived caps at least every 30-40 seconds.
- A cap's schema and base live as long as some cap that uses them.
- Caps to an Endpoint that has been gone for more than 60s fail with `target_gone` and stop being extended.
- Single-use caps (reply caps and their transfers) are consumed by their first successful send or transfer.

## 7. Error codes

| Code | Meaning | Retry? |
|---|---|---|
| `bad_request` | malformed request | no |
| `invalid_cap` | the cap does not exist (or was lost) | no |
| `not_holder` | the cap belongs to another endpoint key | no |
| `expired` | the cap's TTL ran out | no |
| `consumed` | a single-use cap was already used | no |
| `target_gone` | the target endpoint is gone | no |
| `schema_violation` | payload does not match the cap's schema | no |
| `base_conflict` | payload has a key that is in the cap's base | no |
| `too_large` | payload, schema or base over the limit | no |
| `rate_limited` | a per-session or per-IP limit was hit | after a pause |
| `unavailable` | could not reach enough Brokers, overload, unknown target state | yes, with backoff |
| `stream_not_allowed` | `stream` on a cap without `allow_stream` | no |

## 8. Limits (defaults)

Payload ≤ 64 KiB; ≤ 16 carried caps; schema ≤ 32 KiB; base ≤ 4 KiB; 64 KiB of new schema/base bytes per session per 10s; 1000 actions/s and 100 mints/s per session; 4096 distinct caps used per session per 10s; 100 lookups of unknown cap IDs per session per 10s; 10,000 live caps created per session; 64 sessions and 20 handshakes/s (burst 200) per client IP. Streams: 64 pending (not yet accepted) streams per session; 16 unanswered offers per endpoint per Broker (more bounce with `receiver_busy`); tokens valid 60s; 256 data sockets and 10 failed attaches/s per client IP. `welcome.limits` includes `max_pending_streams`, `stream_attach_ttl_ms` and `stream_sockets_per_ip`.

## 9. Delivery semantics

At most once, with a TTL. Messages sent by one session on one cap are delivered in order while the target's set of Brokers is stable. Messages to a target that is reconnecting are held until it returns or the TTL expires (then `bounce` with `ttl_expired`). Use reply caps to build delivery receipts.

## 10. Streams

A stream is a full-duplex WebSocket pipe to the target of a cap with `allow_stream`. Each side connects a **data socket** to its own Broker; the Brokers relay between them. Messages keep their type (text or binary) and boundaries.

**Holder.**
1. Send `stream` (§4). The result carries `stream` and `token`.
2. Attach: `GET wss://<authority>/v1/stream?token=<token>` with subprotocol `nimbo.stream.v1`, where `<authority>` is the one your session dialed (the token is valid only on the Broker that issued it). The token is valid for the offer's `ttl_ms` plus 60s.
3. You may write at once: messages wait in the socket's buffers until the target attaches. `stream_status` tells you whether it did.

**Target.** An offer is a `deliver` with `stream` and `stream_token` (its `payload`, `caps` and `reply` are as for any message).
- **Accept:** attach to the Broker that delivered the offer, with `stream_token`, within 60s. The Broker answers 101 only once the stream is paired with the holder.
- **Refuse:** attach and close at once with code 4901 and a reason; the holder's socket closes with the same code and reason (its `stream_status` says `accepted`, since you attached).
- **Ignore:** do nothing. The holder gets `peer_attach_timeout`.

**Attach answers.** `101` attached; `400` not a `nimbo.stream.v1` WebSocket request; `401` unknown, used or expired token, or the wrong Broker; `403` the holder's side of the stream is gone; `429` too many sockets or failed attaches from your IP; `503` the other Broker is unreachable, or this Broker is fenced or draining. A token is single use: it is spent once its Broker finds it, whatever the outcome. A 400, a 429 or a 503 from a fenced or draining Broker leaves it unused.

**Close codes.**

| Code | Meaning |
|---|---|
| 1000, 3000–4899 | sent by the other endpoint, passed through with its reason |
| 1001 | a Broker on the path is draining (after a 30s grace) |
| 4901 | `stream_rejected`: the target refused (endpoints may send it) |
| 4902 | `ttl_expired`: the offer was not delivered in time |
| 4903 | `target_gone` |
| 4904 | `receiver_busy` |
| 4905 | `peer_attach_timeout`: the other side never attached |
| 4911 | `peer_lost`: the other endpoint's socket, a Broker or the mesh failed |
| 4929 | `rate_limited` |
| 4950 | `unavailable` |

Codes 4900–4999 other than 4901 are reserved for Brokers; an endpoint that sends one (or any code outside 1000 and 3000–4899) is relayed as 1000.

**Semantics.** Reliable and ordered while the stream is up; data in flight at a close or failure may be lost and is never retried. No size limit per message. A stream outlives the cap and both sessions once attached; it ends when either side closes or a hop fails. WebSocket has no half-close: a close from either side ends the stream. Brokers ping data sockets every 20s.

