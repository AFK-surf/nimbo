// An in-memory Broker behind a fake WebSocket, for unit tests of the
// client's connection handling. It runs the real handshake: it checks the
// hello proof with its own X25519 and HMAC.

import { x25519 } from "@noble/curves/ed25519.js";
import { decodeBase32, encodeBase32 } from "../dist/index.js";
import type { WebSocketConstructor, WebSocketLike } from "../dist/index.js";

export type Frame = { op: string; [k: string]: unknown };

/** Answers a request frame: result fields, or `{ error }`; undefined leaves it unanswered. */
export type Handler = (f: Frame, s: FakeSocket) => Record<string, unknown> | undefined;

export class FakeBroker {
  /** Every session socket ever opened, in order. */
  readonly sockets: FakeSocket[] = [];
  /** Every stream data socket (/v1/stream) ever opened, in order. */
  readonly dataSockets: FakeSocket[] = [];
  /** How to answer data sockets: "ok" opens them, "refuse" fails the upgrade. */
  dataMode: "ok" | "refuse" = "ok";
  /** Requests received after the handshake, in order. */
  readonly requests: Frame[] = [];
  readonly hellos: Frame[] = [];
  /** How to answer the next connection attempts: "ok" runs the handshake. */
  mode: "ok" | "refuse" | { closing: string; retryAfterMs?: number } | "silent" = "ok";
  authority = "broker.test:443";
  handlers: Record<string, Handler> = {
    mint: () => ({ cap: `cap${this.requests.length}` }),
    renew: (f) => ({ renewed: (f.caps as string[]).map((cap) => ({ cap, ok: true, ttl_ms: 60000 })) }),
  };

  get WebSocket(): WebSocketConstructor {
    const broker = this;
    return class extends FakeSocket {
      constructor(url: string, protocols: string[]) {
        super(broker, url, protocols);
      }
    };
  }

  /** The newest socket. */
  get last(): FakeSocket {
    const s = this.sockets[this.sockets.length - 1];
    if (!s) throw new Error("no socket yet");
    return s;
  }

  accept(s: FakeSocket): void {
    if (new URL(s.url).pathname.endsWith("/v1/stream")) {
      this.dataSockets.push(s);
      const mode = this.dataMode;
      later(() => {
        if (s.closed) return;
        if (mode === "refuse") return s.drop(1006);
        s.emit("open", undefined);
      });
      return;
    }
    this.sockets.push(s);
    const mode = this.mode;
    later(() => {
      if (s.closed) return;
      if (mode === "refuse") return s.drop(1006);
      if (mode === "silent") return;
      s.emit("open", undefined);
      if (typeof mode === "object") {
        s.push({ op: "closing", reason: mode.closing, retry_after_ms: mode.retryAfterMs ?? 0 });
        return s.drop(1000);
      }
      s.push({ op: "challenge", v: 1, broker: "00", eph: encodeBase32(s.ephPub), nonce: encodeBase32(s.nonce) });
    });
  }

  async receive(s: FakeSocket, f: Frame): Promise<void> {
    if (!s.established) {
      if (f.op !== "hello") return s.drop(1002);
      this.hellos.push(f);
      const ePub = decodeBase32(String(f.key));
      const want = await proof(x25519.getSharedSecret(s.ephPriv, ePub), s.nonce, this.authority, ePub, s.ephPub);
      if (f.authority !== this.authority || f.proof !== encodeBase32(want)) {
        s.push({ op: "closing", reason: "auth_failed" });
        return s.drop(1008);
      }
      s.established = true;
      s.push({
        op: "welcome",
        session: `s${this.sockets.length}`,
        limits: { max_payload_bytes: 65536, max_ttl_ms: 60000 },
        brokers: ["wss://other.test/v1/ws"],
      });
      return;
    }
    this.requests.push(f);
    const h = this.handlers[f.op];
    const r = h?.(f, s);
    if (r === undefined) return;
    if ("error" in r) s.push({ op: "result", rid: f.rid, ok: false, error: r.error });
    else s.push({ op: "result", rid: f.rid, ok: true, ...r });
  }
}

type Listeners = { [k: string]: ((ev: any) => void)[] };

export class FakeSocket implements WebSocketLike {
  readonly protocol = "nimbo.v1";
  readonly ephPriv = x25519.utils.randomSecretKey();
  readonly ephPub = x25519.getPublicKey(this.ephPriv);
  readonly nonce = globalThis.crypto.getRandomValues(new Uint8Array(32));
  closed = false;
  established = false;
  closeCode: number | undefined;
  closeReason: string | undefined;
  binaryType = "blob";
  /** What the client sent on a data socket. */
  readonly sent: (string | Uint8Array)[] = [];
  #l: Listeners = {};

  readonly broker: FakeBroker;
  readonly url: string;
  readonly protocols: string[];

  constructor(broker: FakeBroker, url: string, protocols: string[]) {
    this.broker = broker;
    this.url = url;
    this.protocols = protocols;
    broker.accept(this);
  }

  addEventListener(type: string, listener: (ev: any) => void): void {
    (this.#l[type] ??= []).push(listener);
  }

  emit(type: string, ev: unknown): void {
    for (const l of this.#l[type] ?? []) l(ev);
  }

  send(data: string | ArrayBuffer | ArrayBufferView): void {
    if (this.closed) throw new Error("send on a closed socket");
    if (this.broker.dataSockets.includes(this)) {
      if (typeof data === "string") this.sent.push(data);
      else if (data instanceof ArrayBuffer) this.sent.push(new Uint8Array(data.slice(0)));
      else this.sent.push(new Uint8Array(data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength)));
      return;
    }
    void this.broker.receive(this, JSON.parse(data as string) as Frame);
  }

  /** Broker → client message on a data socket (any `data` a WebSocket may deliver). */
  pushData(data: unknown): void {
    if (this.closed) return;
    later(() => this.emit("message", { data }));
  }

  /** The other side closed a data socket cleanly with this code. */
  closeFromPeer(code: number, reason = ""): void {
    if (this.closed) return;
    this.closed = true;
    later(() => this.emit("close", { code, reason }));
  }

  close(code = 1000, reason = ""): void {
    if (this.closed) return;
    this.closed = true;
    this.closeCode = code;
    this.closeReason = reason;
    later(() => this.emit("close", { code, reason }));
  }

  /** Broker → client frame. */
  push(f: Frame): void {
    if (this.closed) return;
    later(() => this.emit("message", { data: JSON.stringify(f) }));
  }

  /**
   * Broker → client frames delivered in one task, with no microtask
   * checkpoint between them, as Node's WebSocket does for frames that
   * arrive in one read.
   */
  pushTogether(...fs: Frame[]): void {
    if (this.closed) return;
    later(() => {
      for (const f of fs) this.emit("message", { data: JSON.stringify(f) });
    });
  }

  /** The Broker or the network ends the connection. */
  drop(code = 1006): void {
    if (this.closed) return;
    this.closed = true;
    later(() => {
      if (code !== 1000) this.emit("error", {});
      this.emit("close", { code, reason: "" });
    });
  }
}

function later(f: () => void): void {
  setTimeout(f, 0);
}

async function proof(ss: Uint8Array, nonce: Uint8Array, authority: string, ePub: Uint8Array, bEph: Uint8Array) {
  const enc = new TextEncoder();
  const subtle = globalThis.crypto.subtle;
  const k = await subtle.digest("SHA-256", cat(enc.encode("nimbo/v1/auth"), ss));
  const hk = await subtle.importKey("raw", k, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  return new Uint8Array(await subtle.sign("HMAC", hk, cat(nonce, enc.encode(authority), ePub, bEph)));
}

function cat(...ps: Uint8Array[]): Uint8Array<ArrayBuffer> {
  const out = new Uint8Array(ps.reduce((n, p) => n + p.length, 0));
  let o = 0;
  for (const p of ps) {
    out.set(p, o);
    o += p.length;
  }
  return out;
}
