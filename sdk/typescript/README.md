# @afk-surf/nimbo

The TypeScript endpoint SDK for Nimbo. One ESM build runs unchanged in **Bun**, **Node ≥ 22**, **Cloudflare Workers** and **browsers**. It implements [docs/protocol.md](../../docs/protocol.md) with the same features as the Go SDK (`pkg/client`), plus `request`, `keepAlive` and `Delivery.respond`.

```ts
import { Client, generateKey } from "@afk-surf/nimbo";

const client = new Client({ url: "wss://broker1.example.com/v1/ws", key: generateKey() });

client.on("deliver", async (d) => {
  console.log(d.on, d.payload, d.caps);
  if (d.reply) await d.respond({ ok: true }); // send on the single-use reply cap
});

const inbox = await client.mint({ autoReply: true, durable: true });
const granted = await client.transfer(inbox, otherEndpointKey, { durable: true }); // hand `granted` to the other endpoint

// On the other endpoint:
const { msg, replyId } = await other.send(granted, { text: "hi" });
const reply = await other.request(granted, { text: "hi" }, { timeoutMs: 5000 }); // send, then await the reply
```

## Connections

`new Client(options)` returns at once. The client connects in the background and keeps a session up until `close()`:

- Every failure (network error, refused or dropped connection, failed handshake, a Broker that is starting or draining) is retried for ever, with jittered exponential backoff from 250ms to 30s (`backoffBaseMs`, `backoffMaxMs`). A Broker's `closing.retry_after_ms` replaces the delay. An attempt without a session after 15s (`connectTimeoutMs`) is abandoned and retried.
- Requests made while there is no session wait for the next one and are sent in the order they were made. Pass `signal` (an `AbortSignal`, e.g. `AbortSignal.timeout(5000)`) to stop waiting.
- A request already sent when its session drops rejects with `SessionClosedError`. The Broker may or may not have carried it out, and it is not resent: sends are at most once.
- Only invalid options throw, synchronously from the constructor: a URL that is not `ws://` or `wss://`, a malformed key, or no `WebSocket` implementation.

`client.state` is `connecting`, `open` or `closed`. The `connect` event reports each new session (its ID, the Broker's limits and the other Brokers it suggests). The `disconnect` event reports each ended session or failed attempt, with the error, the Broker's `closing` reason and the delay before the next attempt.

## API

| | |
|---|---|
| `generateKey()`, `importKey(priv)`, `exportKey(key)` | Endpoint keys (X25519). `key.publicKey` is the base32 endpoint key others transfer caps to. `exportKey` gives the private key in base32 for `importKey`. |
| `new Client(options)` | `url`, `key`, and optionally `authority` (the `host[:port]` in the handshake proof; default: the URL's), `maxMessageBytes`, `clientName`, `WebSocket`, `backoffBaseMs`, `backoffMaxMs`, `connectTimeoutMs`, `renewIntervalMs`. |
| `mint({ schema?, base?, autoReply?, durable?, allowStream? })` | Returns a new cap ID that targets and is held by this endpoint. The ID is also the lineage ID that deliveries on the cap report as `on`. |
| `delegate(target, { schema?, base?, autoReply?, durable?, allowStream? })` | Returns a new cap ID that targets the endpoint `target` and is held by this endpoint, as if `target` had minted it and transferred it here ([protocol §11](../../docs/protocol.md#11-delegated-issuance)). `target` is that endpoint's key, or a `Prover` (`{ publicKey, prove(grant) }`, e.g. a process that holds the key and returns `delegationProof(privateKey, grant)`). The target need not be connected. |
| `transfer(cap, to, { durable? })` | Returns a new cap ID held by endpoint `to`. Use `durable` whenever the ID leaves the session. |
| `send(cap, payload, { caps?, ttlMs?, reply? })` | Returns `{ msg, replyId }`. `replyId` is set when the cap has auto-reply. Resolving means the message was accepted for routing, not that it was delivered. |
| `openStream(cap, payload?, { detached?, attachTimeoutMs?, ...send options })` | Opens a stream to the target of a cap with `allowStream` and attaches this endpoint's data socket. Returns a `Stream` (see [Streams](#streams)). |
| `acceptStream(offer)`, `rejectStream(offer, reason?)` | Answer a stream offer (also `d.acceptStream()`, `d.rejectStream(reason)` on the `Delivery`). |
| `request(cap, payload, { timeoutMs?, ...send options })` | Sends, then returns the reply `Delivery` (default timeout 30s). The reply is not also emitted as `deliver`. |
| `renew(caps)` | Returns `RenewItem[]`. Batches of more than 256 caps are split. |
| `inspect(cap)` | Returns `CapInfo`. Only the holder may inspect a cap. |
| `keepAlive(...caps)`, `release(...caps)` | Renews caps every 30s until released. A cap that cannot be renewed is dropped and reported by a `capLost` event. |
| `on(event, listener)` | Events: `deliver`, `bounce`, `connect`, `disconnect` and `capLost`. Returns a function that removes the listener. |
| `close()` | Ends the client. Requests still outstanding reject with `ClientClosedError`. `using client = new Client(...)` also closes it. |

Every request method accepts `{ signal }`. Field names are camelCase versions of the protocol's names (`autoReply`, `ttlMs`, `replyId`, …).

### Errors

- **`NimboError`**: the Broker rejected the request. `code` is a protocol error code (`invalid_cap`, `schema_violation`, `unavailable`, …), and `detail` explains it. `retryable` is true for `unavailable` and `rate_limited`. `request` adds two codes: `no_reply` (the cap has no auto-reply) and `timeout`.
- **`SessionClosedError`**: the session ended before the result arrived.
- **`ClientClosedError`**: the client was closed.
- **`StreamError`**: a stream failed before its target attached. `reason` is `ttl_expired`, `receiver_busy`, `target_gone`, `unavailable` (the offer bounced), `peer_attach_timeout` or `canceled`.
- **`StreamAttachError`**: a Broker refused a data socket (an unknown, used or expired token, a token used on another Broker, the holder's side gone, the other Broker unreachable, or a rate limit). WebSocket APIs do not expose the HTTP status, so it is not more specific.
- An aborted `signal` rejects with the signal's reason.

## Streams

A stream is a full-duplex WebSocket pipe between a cap's holder and its target ([protocol §10](../../docs/protocol.md#10-streams)). The target allows them on the caps it mints; each side's data socket goes to its own Broker, at the same URL as its session but on `/v1/stream`.

```ts
// Target: allow streams on a cap, and answer offers.
const inbox = await target.mint({ allowStream: true, durable: true });
target.on("deliver", async (d) => {
  if (!d.stream) return;                       // a plain message
  if (d.payload.proto !== "echo/1") return d.rejectStream("unknown protocol"); // or ignore it
  const st = await d.acceptStream();
  for await (const m of st) st.send(m);        // echo until the holder closes
});

// Holder:
const st = await holder.openStream(granted, { proto: "echo/1" });
st.send("hello");                              // may be sent before the target accepts
await st.accepted;                             // the target attached (or StreamError)
console.log(await st.receive());               // "hello"
st.close(1000, "done");
```

- **Messages** keep their type and boundaries: a `string` is a text message, a `Uint8Array` (or `ArrayBuffer`) a binary one. Read them with `st.on("message", ...)` or with `await st.receive()` / `for await (const m of st)`; messages that arrive while there is no `message` listener wait for `receive()`. `st.bufferedAmount` paces large writes.
- **Closing:** `st.close(code, reason)` with 1000 or 3000–4899 (4901 refuses, as `rejectStream` does). `st.closed` resolves with the code and reason the other side or a Broker sent (`StreamCloseCode` names Nimbo's: 4902–4905 for failures before pairing, 4911 `peerLost`, 1001 `draining`, 1006 when the connection dropped). `st.on("close", ...)` also reports it.
- **Status:** `st.accepted` (holder only) resolves once the target attached. A refusal still resolves it, and then closes the socket with 4901. It rejects with `StreamError` if the offer bounced or nobody attached in time, and with `SessionClosedError` if the session that opened the stream ended first (the stream itself goes on).
- **Lifetime:** once attached, a stream is independent of the cap, the `Client` and its sessions: `client.close()` does not close it. An offer must be accepted within 60s of its delivery.
- **Detached:** `openStream(cap, payload, { detached: true })` does not attach; `st.token` holds the token and `await st.attach()` attaches later (within the offer's TTL plus 60s).

## Runtimes

- **Bun** and **Node ≥ 22** use the global `WebSocket`. On older Node, pass the `ws` package: `new Client({ ..., WebSocket })`.
- **Cloudflare Workers**: no `nodejs_compat` flag is needed (tested with compatibility date 2025-09-01). Two limits apply:
  - A Worker cannot open sockets at global scope, so create the client inside a handler.
  - A plain Worker's I/O belongs to the request that created it. For a session that outlives one request, keep the client in a Durable Object:

```ts
import { DurableObject } from "cloudflare:workers";
import { Client, importKey } from "@afk-surf/nimbo";

export class Endpoint extends DurableObject<Env> {
  client = new Client({ url: this.env.NIMBO_URL, key: importKey(this.env.NIMBO_KEY) });

  async fetch(req: Request) {
    const { cap, payload } = await req.json<{ cap: string; payload: Record<string, unknown> }>();
    const reply = await this.client.request(cap, payload);
    return Response.json(reply.payload);
  }
}
```

- **Browsers** (tested in Chromium, Firefox and WebKit): import the package through a bundler (esbuild, Vite, webpack, …); it needs no Node polyfills, and the page need not be a secure context. Two things to keep in mind:
  - An `https://` page can only open `wss://` Broker URLs (browsers block mixed content).
  - The endpoint's private key lives in the page. Keep it (`exportKey` / `importKey`) only where the page's own secrets belong, and remember that any script running on the page can use it.

The SDK uses only `WebSocket`, `crypto.getRandomValues` and timers. Data sockets set `binaryType = "arraybuffer"` and also accept `Blob` and Node `Buffer` messages, so `ws` and other implementations work too. X25519, SHA-256 and HMAC come from [`@noble/curves`](https://github.com/paulmillr/noble-curves) and [`@noble/hashes`](https://github.com/paulmillr/noble-hashes), its only dependencies.

## Development

```sh
npm install
npm run build
npm run typecheck
npm test            # Node, Bun and workerd (needs Go, Bun)
node test/node.ts   # or one runtime: bun test/node.ts, node test/workers.ts
npm run test:browser  # headless Chromium, Firefox and WebKit (npx playwright install --with-deps first)
```

The tests build and start [`cmd/nimbo-devcluster`](../../cmd/nimbo-devcluster), a 3-Broker cluster in one process with in-memory storage. Set `NIMBO_DEVCLUSTER` to the path of a prebuilt binary to skip the build. Unit tests run the client against an in-memory fake Broker (`test/fakebroker.ts`). The handshake proof is checked against the same vector as `pkg/wire/proof_test.go`. The workerd run uses Miniflare and lends the Worker the cluster's stop/start commands through a service binding. The browser run (`test/browser.ts`) bundles the same tests into a page, drives it with Playwright and lends it those commands through an exposed function; Chromium loads the page from `http://nimbo.test`, which is not a secure context.
