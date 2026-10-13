import {
  Client,
  ClientClosedError,
  NimboError,
  SessionClosedError,
  decodeBase32,
  encodeBase32,
  exportKey,
  generateKey,
  handshakeProof,
  importKey,
} from "../dist/index.js";
import type { ClientOptions, Delivery, DisconnectEvent } from "../dist/index.js";
import { FakeBroker } from "./fakebroker.ts";
import { Inbox, assert, eq, rejects, sleep, until } from "./harness.ts";
import type { Test } from "./harness.ts";

const enc = new TextEncoder();

function fake(opts: Partial<ClientOptions> = {}): { broker: FakeBroker; client: Client } {
  const broker = new FakeBroker();
  const client = new Client({
    url: `wss://${broker.authority.replace(/:443$/, "")}/v1/ws`,
    authority: broker.authority,
    key: generateKey(),
    WebSocket: broker.WebSocket,
    backoffBaseMs: 10,
    backoffMaxMs: 80,
    ...opts,
  });
  return { broker, client };
}

async function opened(c: Client): Promise<void> {
  await until(() => c.state === "open", "session");
}

export const unitTests: Test[] = [
  {
    name: "base32 matches RFC 4648 (lowercase, unpadded)",
    async fn() {
      const vectors: [string, string][] = [
        ["", ""],
        ["f", "my"],
        ["fo", "mzxq"],
        ["foo", "mzxw6"],
        ["foob", "mzxw6yq"],
        ["fooba", "mzxw6ytb"],
        ["foobar", "mzxw6ytboi"],
      ];
      for (const [plain, b32] of vectors) {
        eq(encodeBase32(enc.encode(plain)), b32, `encode ${plain}`);
        eq(new TextDecoder().decode(decodeBase32(b32)), plain, `decode ${b32}`);
      }
      const bytes = globalThis.crypto.getRandomValues(new Uint8Array(32));
      const s = encodeBase32(bytes);
      eq(s.length, 52, "32 bytes encode to 52 characters");
      eq([...decodeBase32(s)], [...bytes], "round trip");
      for (const bad of ["MZXQ", "mzx1", "mzxw6yr", "m", "mzx"]) {
        let threw = false;
        try {
          decodeBase32(bad);
        } catch {
          threw = true;
        }
        assert(threw, `decode ${bad} should throw`);
      }
    },
  },
  {
    name: "handshake proof matches the Go vector (pkg/wire/proof_test.go)",
    async fn() {
      const e = new Uint8Array(32).map((_, i) => i + 1);
      const b = new Uint8Array(32).map((_, i) => i + 101);
      const nonce = new Uint8Array(32).map((_, i) => 200 + i);
      const key = importKey(e);
      eq(key.publicKey, "a6rxzpauecj4rn2v3qnrb2dmwqtdoswrnkufh3il37alfodndr6a", "E_pub");
      const bPub = importKey(b).publicKey;
      eq(bPub, "k4khnhirnp3winvoos6hspjmgcwrsa6ftlcsooafy7rgtc2bbq3a", "B_eph_pub");
      const p = await handshakeProof(e, decodeBase32(key.publicKey), decodeBase32(bPub), nonce, "broker.example.com:8443");
      eq(encodeBase32(p), "eduhkxbs7jcu7n3zvn3fakah6lsnxah76bhsbunk7mwm3vb7ymia", "proof");
    },
  },
  {
    name: "keys export and import",
    async fn() {
      const k = generateKey();
      eq(k.publicKey.length, 52, "public key length");
      const s = exportKey(k);
      eq(importKey(s).publicKey, k.publicKey, "public key after import");
    },
  },
  {
    name: "constructor validates options synchronously",
    async fn() {
      const key = generateKey();
      for (const opts of [
        { url: "not a url", key },
        { url: "https://broker/v1/ws", key },
        { url: "wss://broker/v1/ws", key: { publicKey: "", privateKey: new Uint8Array(3) } },
      ]) {
        let threw = false;
        try {
          new Client({ ...opts, WebSocket: new FakeBroker().WebSocket });
        } catch (err) {
          threw = err instanceof TypeError;
        }
        assert(threw, `${JSON.stringify(opts.url)} should throw a TypeError`);
      }
    },
  },
  {
    name: "connects in the background and sends requests queued before the session, in order",
    async fn() {
      const { broker, client } = fake();
      try {
        eq(client.state, "connecting", "state right after construction");
        const connects = new Inbox<unknown>();
        client.on("connect", connects.push);
        const a = client.mint();
        const b = client.send("capx", { n: 1 });
        const c = client.send("capx", { n: 2 });
        broker.handlers.send = (f) => ({ msg: `m${(f.payload as { n: number }).n}` });
        eq(await a, "cap1", "mint");
        eq(await b, { msg: "m1", replyId: undefined }, "first send");
        eq(await c, { msg: "m2", replyId: undefined }, "second send");
        eq(broker.requests.map((r) => r.op), ["mint", "send", "send"], "request order");
        eq(broker.sockets[0]!.protocols, ["nimbo.v1"], "subprotocol");
        const hello = broker.hellos[0]!;
        eq(hello.authority, broker.authority, "hello.authority");
        eq(hello.key, client.publicKey, "hello.key");
        eq(hello.client, "nimbo-ts/0.1", "hello.client");
        await connects.next();
        eq(client.state, "open", "state");
        eq(client.session, "s1", "session");
        eq(client.limits?.maxPayloadBytes, 65536, "limits");
        eq(client.brokers, ["wss://other.test/v1/ws"], "brokers");
        const rids = broker.requests.map((r) => r.rid as number);
        assert(new Set(rids).size === rids.length && rids.every((r) => r > 0), "rids are positive and unique");
      } finally {
        client.close();
      }
    },
  },
  {
    name: "retries for ever with exponential backoff while the Broker refuses",
    async fn() {
      const broker = new FakeBroker();
      broker.mode = "refuse";
      const { client } = fake({ WebSocket: broker.WebSocket });
      try {
        const ds: DisconnectEvent[] = [];
        client.on("disconnect", (d) => ds.push(d));
        await until(() => ds.length >= 6, "6 failed attempts");
        assert(ds.every((d) => !d.wasOpen && d.error instanceof Error), "attempts failed with errors");
        // Base 10ms doubling, capped at 80ms, jittered to [d/2, d].
        const caps = [10, 20, 40, 80, 80, 80];
        ds.slice(0, 6).forEach((d, i) => {
          assert(d.retryInMs >= caps[i]! / 2 && d.retryInMs <= caps[i]!, `attempt ${i}: ${d.retryInMs}ms not in [${caps[i]! / 2}, ${caps[i]}]`);
        });
        const mint = client.mint();
        broker.mode = "ok";
        eq(await mint, "cap1", "mint once the Broker accepts");
      } finally {
        client.close();
      }
    },
  },
  {
    name: "honours closing.retry_after_ms and reports the reason",
    async fn() {
      const broker = new FakeBroker();
      broker.mode = { closing: "draining", retryAfterMs: 123 };
      const { client } = fake({ WebSocket: broker.WebSocket });
      try {
        const d = await new Promise<DisconnectEvent>((r) => client.on("disconnect", r));
        eq(d.closing, "draining", "closing reason");
        eq(d.retryInMs, 123, "retry delay");
      } finally {
        client.close();
      }
    },
  },
  {
    name: "a dropped session fails in-flight requests and reconnects; queued ones go to the next session",
    async fn() {
      const { broker, client } = fake();
      try {
        await opened(client);
        broker.handlers.inspect = () => undefined; // never answered
        const inflight = client.inspect("capx");
        await until(() => broker.requests.length === 1, "inspect sent");
        const ds = new Inbox<DisconnectEvent>();
        client.on("disconnect", ds.push);
        broker.last.drop();
        const err = await rejects(inflight);
        assert(err instanceof SessionClosedError, `got ${String(err)}`);
        const d = await ds.next();
        assert(d.wasOpen, "wasOpen");
        assert(d.retryInMs <= 10, "backoff reset to base after a session");
        eq(client.state, "connecting", "state");
        eq(await client.mint(), "cap2", "mint on the next session");
        eq(client.session, "s2", "second session");
      } finally {
        client.close();
      }
    },
  },
  {
    name: "an attempt with no welcome is abandoned after connectTimeoutMs",
    async fn() {
      const broker = new FakeBroker();
      broker.mode = "silent";
      const { client } = fake({ WebSocket: broker.WebSocket, connectTimeoutMs: 30 });
      try {
        const d = await new Promise<DisconnectEvent>((r) => client.on("disconnect", r));
        assert(d.error?.message.includes("no session"), `error: ${d.error?.message}`);
        assert(broker.sockets[0]!.closed, "socket closed");
      } finally {
        client.close();
      }
    },
  },
  {
    name: "a wrong authority fails the handshake and is retried",
    async fn() {
      const broker = new FakeBroker();
      const { client } = fake({ WebSocket: broker.WebSocket, authority: "evil.test" });
      try {
        const d = await new Promise<DisconnectEvent>((r) => client.on("disconnect", r));
        eq(d.closing, "auth_failed", "closing reason");
        await until(() => broker.sockets.length >= 2, "a second attempt");
      } finally {
        client.close();
      }
    },
  },
  {
    name: "errors carry the Broker's code",
    async fn() {
      const { broker, client } = fake();
      try {
        broker.handlers.send = () => ({ error: { code: "schema_violation", detail: "/x: want string" } });
        const err = await rejects(client.send("capx", { x: 1 }));
        assert(err instanceof NimboError, `got ${String(err)}`);
        eq(err.code, "schema_violation", "code");
        eq(err.detail, "/x: want string", "detail");
        eq(err.retryable, false, "retryable");
        eq(new NimboError("unavailable").retryable, true, "unavailable is retryable");
        const bad = await rejects(client.send("capx", [] as never));
        assert(bad instanceof TypeError, "array payload");
      } finally {
        client.close();
      }
    },
  },
  {
    name: "maps wire fields to camelCase and back",
    async fn() {
      const { broker, client } = fake();
      try {
        broker.handlers.send = () => ({ msg: "m", reply_id: "r" });
        broker.handlers.inspect = () => ({
          info: { lineage: "l", kind: "minted", target: "t", holder: "h", issuer: "i", minted_at: 5, auto_reply: true, ttl_ms: 6, base: { a: 1 } },
        });
        broker.handlers.transfer = (f) => ({ cap: `${f.cap}->${f.to}` });
        await client.mint({ schema: { type: "object" }, base: { k: "v" }, autoReply: true, durable: true });
        eq(await client.send("c", { p: 1 }, { caps: ["x"], ttlMs: 500, reply: { autoReply: true, base: { b: 2 } } }), { msg: "m", replyId: "r" }, "send result");
        eq(await client.transfer("c", "k", { durable: true }), "c->k", "transfer");
        eq(
          await client.inspect("c"),
          { lineage: "l", kind: "minted", singleUse: false, target: "t", holder: "h", issuer: "i", mintedAt: 5, autoReply: true, ttlMs: 6, base: { a: 1 } },
          "inspect",
        );
        const [mint, send, transfer] = broker.requests.map(({ rid: _, ...f }) => f);
        eq(mint, { op: "mint", schema: { type: "object" }, base: { k: "v" }, auto_reply: true, durable: true }, "mint frame");
        eq(send, { op: "send", cap: "c", payload: { p: 1 }, caps: ["x"], ttl_ms: 500, reply: { base: { b: 2 }, auto_reply: true } }, "send frame");
        eq(transfer, { op: "transfer", cap: "c", to: "k", durable: true }, "transfer frame");
      } finally {
        client.close();
      }
    },
  },
  {
    name: "renew splits batches of 256",
    async fn() {
      const { broker, client } = fake();
      try {
        const caps = Array.from({ length: 600 }, (_, i) => `c${i}`);
        const items = await client.renew(caps);
        eq(items.length, 600, "items");
        eq(items[599], { cap: "c599", ok: true, ttlMs: 60000 }, "last item");
        eq(broker.requests.map((r) => (r.caps as string[]).length), [256, 256, 88], "batch sizes");
      } finally {
        client.close();
      }
    },
  },
  {
    name: "deliveries, bounces and Delivery.respond",
    async fn() {
      const { broker, client } = fake();
      try {
        const deliveries = new Inbox<Delivery>();
        const bounces = new Inbox<unknown>();
        client.on("deliver", deliveries.push);
        const off = client.on("bounce", bounces.push);
        await opened(client);
        broker.handlers.send = (f) => ({ msg: `sent-on-${f.cap}` });
        broker.last.push({ op: "deliver", msg: "m1", on: "lin", payload: { a: 1 }, caps: ["c9"], reply: "rc" });
        const d = await deliveries.next();
        eq([d.msg, d.on, d.payload, d.caps, d.reply], ["m1", "lin", { a: 1 }, ["c9"], "rc"], "delivery");
        eq((await d.respond({ ok: true })).msg, "sent-on-rc", "respond sends on the reply cap");
        broker.last.push({ op: "deliver", msg: "m2", on: "lin", payload: {} });
        const noReply = await deliveries.next();
        const err = await rejects(noReply.respond({}));
        assert(err instanceof NimboError && err.code === "no_reply", "respond without a reply cap");
        broker.last.push({ op: "bounce", msg: "m3", reason: "ttl_expired" });
        eq(await bounces.next(), { msg: "m3", reason: "ttl_expired" }, "bounce");
        off();
        broker.last.push({ op: "bounce", msg: "m4", reason: "ttl_expired" });
        await sleep(20);
        eq(bounces.size, 0, "no bounce after unsubscribing");
      } finally {
        client.close();
      }
    },
  },
  {
    name: "request returns the reply instead of emitting it, and times out",
    async fn() {
      const { broker, client } = fake();
      try {
        const deliveries = new Inbox<Delivery>();
        client.on("deliver", deliveries.push);
        broker.handlers.send = (f, s) => {
          if (f.cap === "auto") {
            // As a real Broker does, the result goes out before any reply can.
            setTimeout(() => s.push({ op: "deliver", msg: "r1", on: "rid-1", payload: { answer: 42 } }), 0);
            return { msg: "m", reply_id: "rid-1" };
          }
          if (f.cap === "slow") return { msg: "m", reply_id: "rid-2" };
          return { msg: "m" };
        };
        const r = await client.request<{ answer: number }>("auto", { q: 1 });
        eq(r.payload.answer, 42, "reply payload");
        eq(deliveries.size, 0, "reply not emitted as deliver");
        const none = await rejects(client.request("plain", {}));
        assert(none instanceof NimboError && none.code === "no_reply", "no auto_reply");
        const slow = await rejects(client.request("slow", {}, { timeoutMs: 20 }));
        assert(slow instanceof NimboError && slow.code === "timeout", `got ${String(slow)}`);
        broker.last.push({ op: "deliver", msg: "late", on: "rid-2", payload: {} });
        eq((await deliveries.next()).msg, "late", "a late reply is an ordinary delivery");
      } finally {
        client.close();
      }
    },
  },
  {
    name: "abort signals cancel queued and in-flight requests",
    async fn() {
      const broker = new FakeBroker();
      broker.mode = "refuse";
      const { client } = fake({ WebSocket: broker.WebSocket });
      try {
        const ac = new AbortController();
        const queued = client.mint({ signal: ac.signal });
        ac.abort(new Error("stop"));
        eq(((await rejects(queued)) as Error).message, "stop", "queued request");
        eq(((await rejects(client.mint({ signal: AbortSignal.abort("pre") }))) as unknown), "pre", "already aborted");
        broker.mode = "ok";
        broker.handlers.inspect = () => undefined;
        const ac2 = new AbortController();
        const inflight = client.inspect("x", { signal: ac2.signal });
        await until(() => broker.requests.length === 1, "inspect sent");
        ac2.abort(new Error("late"));
        eq(((await rejects(inflight)) as Error).message, "late", "in-flight request");
        eq(await client.mint(), "cap2", "the session still works");
      } finally {
        client.close();
      }
    },
  },
  {
    name: "keepAlive renews in the background and reports lost caps",
    async fn() {
      const { broker, client } = fake({ renewIntervalMs: 15 });
      try {
        broker.handlers.renew = (f) => ({
          renewed: (f.caps as string[]).map((cap) =>
            cap === "gone" ? { cap, ok: false, error: { code: "invalid_cap" } } : cap === "busy" ? { cap, ok: false, error: { code: "unavailable" } } : { cap, ok: true, ttl_ms: 60000 },
          ),
        });
        const lost = new Inbox<{ cap: string; error: { code: string } }>();
        client.on("capLost", lost.push);
        client.keepAlive("a", "gone", "busy");
        eq(await lost.next(), { cap: "gone", error: { code: "invalid_cap" } }, "capLost");
        await until(() => broker.requests.filter((r) => r.op === "renew").length >= 3, "three renew rounds");
        const last = broker.requests.filter((r) => r.op === "renew").at(-1)!;
        eq(last.caps, ["a", "busy"], "still kept: a, and busy (retryable)");
        client.release("a", "busy");
        const n = broker.requests.length;
        await sleep(60);
        eq(broker.requests.length, n, "no renew after release");
      } finally {
        client.close();
      }
    },
  },
  {
    name: "close rejects everything outstanding and stops reconnecting",
    async fn() {
      const { broker, client } = fake();
      await opened(client);
      broker.handlers.inspect = () => undefined;
      const inflight = client.inspect("x");
      await until(() => broker.requests.length === 1, "inspect sent");
      const ds = new Inbox<DisconnectEvent>();
      client.on("disconnect", ds.push);
      client.close();
      assert((await rejects(inflight)) instanceof ClientClosedError, "in-flight request");
      assert((await rejects(client.mint())) instanceof ClientClosedError, "request after close");
      eq(client.state, "closed", "state");
      assert(broker.last.closed && broker.last.closeCode === 1000, "socket closed normally");
      await sleep(50);
      eq(broker.sockets.length, 1, "no reconnect");
      eq(ds.size, 0, "no disconnect event for close()");
    },
  },
];
