import { decodeBase32, encodeBase32 } from "./base32.js";
import { ClientClosedError, NimboError, SessionClosedError } from "./errors.js";
import type { EndpointKey } from "./key.js";
import { handshakeProof } from "./proof.js";
import { Stream, StreamCloseCode, StreamError, streamURL } from "./stream.js";
import type { AttachOptions, DataSocketConstructor } from "./stream.js";
import type {
  AbortOptions,
  Bounce,
  CapInfo,
  ClientEvents,
  ClientState,
  Delivery,
  JsonObject,
  Limits,
  MintOptions,
  OpenStreamOptions,
  RenewItem,
  RequestOptions,
  SendOptions,
  SendResult,
  TransferOptions,
} from "./types.js";

/** The WebSocket subprotocol of the endpoint protocol. */
export const SUBPROTOCOL = "nimbo.v1";

/** The part of the WHATWG WebSocket the client uses. */
export interface WebSocketLike {
  readonly protocol: string;
  /** Set to "arraybuffer" on stream data sockets. */
  binaryType?: string;
  readonly bufferedAmount?: number;
  /** Control sessions send strings; stream data sockets also send bytes. */
  send(data: string | ArrayBuffer | ArrayBufferView): void;
  close(code?: number, reason?: string): void;
  addEventListener(type: "open", listener: () => void): void;
  addEventListener(type: "message", listener: (ev: { data: unknown }) => void): void;
  addEventListener(type: "close", listener: (ev: { code: number; reason: string }) => void): void;
  addEventListener(type: "error", listener: (ev: unknown) => void): void;
}

export type WebSocketConstructor = new (url: string, protocols: string[]) => WebSocketLike;

export interface ClientOptions {
  /** Broker URL, `wss://host[:port]/v1/ws`. */
  url: string;
  key: EndpointKey;
  /** The `host[:port]` bound into the handshake proof; defaults to the URL's. */
  authority?: string;
  /** The largest payload this endpoint accepts; the Broker bounces bigger ones. */
  maxMessageBytes?: number;
  /** Sent as `hello.client`. */
  clientName?: string;
  /** WebSocket implementation; defaults to the global one (Bun, Node ≥ 22, Workers). */
  WebSocket?: WebSocketConstructor;
  /** First reconnect delay; doubles per failed attempt (default 250ms). */
  backoffBaseMs?: number;
  /** Largest reconnect delay (default 30s). */
  backoffMaxMs?: number;
  /** An attempt that has no session after this long is abandoned and retried (default 15s). */
  connectTimeoutMs?: number;
  /** How often `keepAlive` renews caps (default 30s). */
  renewIntervalMs?: number;
}

const DEFAULT_CLIENT_NAME = "nimbo-ts/0.1";
const RENEW_BATCH = 256;

type Listener<T> = (event: T) => void;

/** Wire frames. Field names are the protocol's (snake_case). */
interface Frame {
  op: string;
  [k: string]: unknown;
}

interface Pending {
  resolve(frame: Frame): void;
  reject(err: unknown): void;
  /** Runs synchronously when an ok result is handled, before any later frame. */
  onOk?: ((frame: Frame) => void) | undefined;
}

/** A request waiting for a session, sent in order once one opens. */
interface Queued extends Pending {
  frame: Frame;
  signal: AbortSignal | undefined;
  onAbort: (() => void) | undefined;
}

interface ReplyWaiter {
  resolve(d: Delivery): void;
  reject(err: unknown): void;
}

/** One connection attempt and, if it gets that far, its session. */
interface Conn {
  ws: WebSocketLike;
  open: boolean;
  /** A result for every request sent on this connection is pending here. */
  pending: Map<number, Pending>;
  closing: { reason: string; retryAfterMs: number } | undefined;
  timer: ReturnType<typeof setTimeout> | undefined;
  ended: boolean;
}

/**
 * A Nimbo endpoint session. The constructor returns at once; the client
 * connects in the background and reconnects for ever, with jittered
 * exponential backoff, until `close()`. Requests made while there is no
 * session wait for the next one and are sent in the order they were made.
 */
export class Client {
  readonly #opts: Required<
    Pick<ClientOptions, "backoffBaseMs" | "backoffMaxMs" | "connectTimeoutMs" | "renewIntervalMs" | "clientName">
  > &
    ClientOptions;
  readonly #WebSocket: WebSocketConstructor;
  readonly #authority: string;
  readonly #publicKey: Uint8Array;

  #state: ClientState = "connecting";
  #conn: Conn | undefined;
  #rid = 0;
  #queue: Queued[] = [];
  #backoff: number;
  #retryTimer: ReturnType<typeof setTimeout> | undefined;
  #session: string | undefined;
  #limits: Limits | undefined;
  #brokers: string[] = [];
  #listeners: { [K in keyof ClientEvents]?: Set<Listener<ClientEvents[K]>> } = {};
  #replyWaiters = new Map<string, ReplyWaiter>();
  #kept = new Set<string>();
  #renewTimer: ReturnType<typeof setInterval> | undefined;
  /** Streams this session opened, awaiting their stream_status. */
  #streamWaiters = new Map<string, { resolve(): void; reject(err: unknown): void }>();

  constructor(options: ClientOptions) {
    let u: URL;
    try {
      u = new URL(options.url);
    } catch {
      throw new TypeError(`nimbo: invalid url ${JSON.stringify(options.url)}`);
    }
    if (u.protocol !== "wss:" && u.protocol !== "ws:") throw new TypeError("nimbo: url must be ws:// or wss://");
    if (!(options.key?.privateKey instanceof Uint8Array) || options.key.privateKey.length !== 32) {
      throw new TypeError("nimbo: key must be an EndpointKey (see generateKey)");
    }
    const WS = options.WebSocket ?? (globalThis as { WebSocket?: WebSocketConstructor }).WebSocket;
    if (!WS) throw new TypeError("nimbo: no global WebSocket; pass options.WebSocket (e.g. from the ws package)");
    this.#WebSocket = WS;
    this.#opts = {
      clientName: DEFAULT_CLIENT_NAME,
      backoffBaseMs: 250,
      backoffMaxMs: 30_000,
      connectTimeoutMs: 15_000,
      renewIntervalMs: 30_000,
      ...stripUndefined(options),
    };
    // URL.host omits default ports, matching "include the port only if the URL has one".
    this.#authority = options.authority ?? u.host;
    this.#publicKey = decodeBase32(options.key.publicKey);
    this.#backoff = this.#opts.backoffBaseMs;
    this.#connect();
  }

  /** `connecting` (no session yet or between sessions), `open`, or `closed`. */
  get state(): ClientState {
    return this.#state;
  }

  /** The current session's ID, while `open`. */
  get session(): string | undefined {
    return this.#state === "open" ? this.#session : undefined;
  }

  /** Limits from the latest `welcome`. */
  get limits(): Limits | undefined {
    return this.#limits;
  }

  /** Other Brokers the latest `welcome` suggested. */
  get brokers(): readonly string[] {
    return this.#brokers;
  }

  /** This endpoint's key in base32. */
  get publicKey(): string {
    return this.#opts.key.publicKey;
  }

  /** Adds a listener; returns a function that removes it. */
  on<K extends keyof ClientEvents>(event: K, listener: Listener<ClientEvents[K]>): () => void {
    let set = this.#listeners[event] as Set<Listener<ClientEvents[K]>> | undefined;
    if (!set) {
      set = new Set();
      (this.#listeners as Record<string, unknown>)[event] = set;
    }
    set.add(listener);
    return () => {
      set.delete(listener);
    };
  }

  /** Creates a cap that targets and is held by this endpoint. Returns its ID (also its lineage ID). */
  async mint(options: MintOptions = {}): Promise<string> {
    const r = await this.#request(
      {
        op: "mint",
        schema: options.schema,
        base: options.base,
        auto_reply: options.autoReply || undefined,
        durable: options.durable || undefined,
        allow_stream: options.allowStream || undefined,
      },
      options.signal,
    );
    return r.cap as string;
  }

  /** Gives the endpoint `to` (a base32 endpoint key) a new cap for the same target. Returns its ID. */
  async transfer(cap: string, to: string, options: TransferOptions = {}): Promise<string> {
    const r = await this.#request({ op: "transfer", cap, to, durable: options.durable || undefined }, options.signal);
    return r.cap as string;
  }

  /**
   * Sends `payload` (a JSON object) to the target of `cap`. Resolving means
   * the message was accepted for routing, not that it was delivered.
   */
  async send(cap: string, payload: JsonObject, options: SendOptions = {}): Promise<SendResult> {
    const r = await this.#send("send", cap, payload, options, undefined);
    return { msg: r.msg as string, replyId: r.reply_id as string | undefined };
  }

  async #send(op: "send" | "stream", cap: string, payload: JsonObject, options: SendOptions, onOk: Pending["onOk"]): Promise<Frame> {
    if (payload === null || typeof payload !== "object" || Array.isArray(payload)) {
      throw new TypeError("nimbo: payload must be a JSON object");
    }
    const reply = options.reply && {
      schema: options.reply.schema,
      base: options.reply.base,
      auto_reply: options.reply.autoReply || undefined,
      allow_stream: options.reply.allowStream || undefined,
    };
    return this.#request(
      { op, cap, payload, caps: options.caps?.length ? options.caps : undefined, ttl_ms: options.ttlMs, reply },
      options.signal,
      onOk,
    );
  }

  /**
   * Opens a stream to the target of `cap` (which must have allow_stream):
   * `payload` goes to the target as the offer, then this endpoint's data
   * socket attaches to its Broker. Resolves once attached; messages sent
   * before the target accepts wait in the socket's buffers, and
   * `stream.accepted` reports the outcome.
   */
  async openStream(cap: string, payload: JsonObject = {}, options: OpenStreamOptions = {}): Promise<Stream> {
    let status: Promise<void> | undefined;
    const r = await this.#send("stream", cap, payload, options, (f) => {
      // The status follows the result on the session, possibly in the same
      // read: register for it while the result is handled.
      const w = deferred<void>();
      w.promise.catch(() => {});
      this.#streamWaiters.set(String(f.stream), w);
      status = w.promise;
    });
    const stream = new Stream({
      id: String(r.stream),
      replyId: typeof r.reply_id === "string" && r.reply_id !== "" ? r.reply_id : undefined,
      url: streamURL(this.#opts.url),
      token: String(r.token),
      WebSocket: this.#WebSocket as DataSocketConstructor,
      accepted: status,
    });
    if (!options.detached) await stream.attach({ signal: options.signal, timeoutMs: options.attachTimeoutMs });
    return stream;
  }

  /**
   * Accepts a stream offer (a delivery with `stream` set) by attaching this
   * endpoint's data socket. Resolves once the stream is paired with the
   * holder. Accept within 60s of the delivery.
   */
  async acceptStream(offer: Delivery, options: AttachOptions = {}): Promise<Stream> {
    if (!offer.stream || !offer.streamToken) throw new TypeError("nimbo: the delivery is not a stream offer");
    const stream = new Stream({
      id: offer.stream,
      replyId: undefined,
      url: streamURL(this.#opts.url),
      token: offer.streamToken,
      WebSocket: this.#WebSocket as DataSocketConstructor,
      accepted: undefined,
    });
    await stream.attach(options);
    return stream;
  }

  /**
   * Refuses a stream offer: attaches and closes at once with 4901, so the
   * holder's data socket closes with that code and `reason`. Ignoring an
   * offer also works; the holder then gets `peer_attach_timeout`.
   */
  async rejectStream(offer: Delivery, reason = "", options: AttachOptions = {}): Promise<void> {
    const stream = await this.acceptStream(offer, options);
    stream.close(StreamCloseCode.rejected, reason);
    await stream.closed;
  }

  /**
   * Sends on a cap with auto_reply and waits for the reply. The reply is
   * returned here and not emitted as a `deliver` event (unless it arrives
   * after the wait ended).
   */
  async request<R extends JsonObject = JsonObject>(
    cap: string,
    payload: JsonObject,
    options: RequestOptions = {},
  ): Promise<Delivery<R>> {
    const waiter = deferred<Delivery>();
    waiter.promise.catch(() => {}); // handled below; close() may reject it first
    let replyId: string | undefined;
    await this.#send("send", cap, payload, options, (f) => {
      // The reply can be dispatched right after the result, before this
      // function resumes (Node delivers frames from one read back to back),
      // so the waiter is registered while the result is handled.
      replyId = typeof f.reply_id === "string" && f.reply_id !== "" ? f.reply_id : undefined;
      if (replyId) this.#replyWaiters.set(replyId, waiter);
    });
    if (!replyId) throw new NimboError("no_reply", "the cap does not have auto_reply");
    const id: string = replyId;
    const timeoutMs = options.timeoutMs ?? 30_000;
    const signal = options.signal;
    return new Promise<Delivery<R>>((resolve, reject) => {
      const done = () => {
        clearTimeout(timer);
        signal?.removeEventListener("abort", onAbort);
        if (this.#replyWaiters.get(id) === waiter) this.#replyWaiters.delete(id);
      };
      const onAbort = () => {
        done();
        reject(signal!.reason);
      };
      const timer = setTimeout(() => {
        done();
        reject(new NimboError("timeout", `no reply within ${timeoutMs}ms`));
      }, timeoutMs);
      if (signal?.aborted) return onAbort();
      signal?.addEventListener("abort", onAbort);
      waiter.promise.then(
        (d) => {
          done();
          resolve(d as Delivery<R>);
        },
        (err) => {
          done();
          reject(err);
        },
      );
    });
  }

  /** Refreshes caps' TTLs; their IDs do not change. More than 256 caps are sent in several requests. */
  async renew(caps: string[], options: AbortOptions = {}): Promise<RenewItem[]> {
    const batches: string[][] = [];
    for (let i = 0; i < caps.length; i += RENEW_BATCH) batches.push(caps.slice(i, i + RENEW_BATCH));
    const results = await Promise.all(batches.map((b) => this.#request({ op: "renew", caps: b }, options.signal)));
    return results.flatMap((r) =>
      ((r.renewed as WireRenewItem[] | undefined) ?? []).map((it) => {
        const item: RenewItem = { cap: it.cap, ok: !!it.ok };
        if (it.ttl_ms !== undefined) item.ttlMs = it.ttl_ms;
        if (it.error) item.error = it.error;
        return item;
      }),
    );
  }

  /** Returns a cap's record. Only its holder may inspect it. */
  async inspect(cap: string, options: AbortOptions = {}): Promise<CapInfo> {
    const r = await this.#request({ op: "inspect", cap }, options.signal);
    const i = r.info as WireCapInfo;
    const info: CapInfo = {
      lineage: i.lineage,
      kind: i.kind,
      singleUse: !!i.single_use,
      target: i.target,
      holder: i.holder,
      issuer: i.issuer,
      mintedAt: i.minted_at,
      autoReply: !!i.auto_reply,
      allowStream: !!i.allow_stream,
      ttlMs: i.ttl_ms,
    };
    if (i.schema !== undefined) info.schema = i.schema;
    if (i.base !== undefined) info.base = i.base;
    return info;
  }

  /**
   * Renews these caps in the background, every 30s, until `release` or
   * `close`. A cap that cannot be renewed is dropped with a `capLost` event;
   * `unavailable` and `rate_limited` are retried at the next round.
   */
  keepAlive(...caps: string[]): void {
    for (const c of caps) this.#kept.add(c);
    if (this.#kept.size > 0 && this.#renewTimer === undefined && this.#state !== "closed") {
      this.#renewTimer = setInterval(() => void this.#renewKept(), this.#opts.renewIntervalMs);
      unref(this.#renewTimer);
    }
  }

  /** Stops renewing these caps. */
  release(...caps: string[]): void {
    for (const c of caps) this.#kept.delete(c);
    if (this.#kept.size === 0) this.#stopRenewing();
  }

  /** Ends the client for good. Requests and `request` waits still outstanding reject with `ClientClosedError`. */
  close(): void {
    if (this.#state === "closed") return;
    this.#state = "closed";
    clearTimeout(this.#retryTimer);
    this.#stopRenewing();
    this.#kept.clear();
    const err = new ClientClosedError();
    const conn = this.#conn;
    this.#conn = undefined;
    if (conn) {
      this.#endConn(conn);
      for (const p of conn.pending.values()) p.reject(err);
      conn.pending.clear();
      try {
        conn.ws.close(1000, "client closed");
      } catch {
        // already closed
      }
    }
    for (const q of this.#queue.splice(0)) {
      if (q.onAbort) q.signal?.removeEventListener("abort", q.onAbort);
      q.reject(err);
    }
    for (const w of [...this.#replyWaiters.values()]) w.reject(err);
    this.#failStreamWaiters(err);
  }

  /** stream_status goes only to the session that opened the stream. */
  #failStreamWaiters(err: Error): void {
    const ws = [...this.#streamWaiters.values()];
    this.#streamWaiters.clear();
    for (const w of ws) w.reject(err);
  }

  /** `using client = new Client(...)` closes it at the end of the scope. */
  [Symbol.dispose](): void {
    this.close();
  }

  // --- requests ---

  #request(frame: Frame, signal: AbortSignal | undefined, onOk?: Pending["onOk"]): Promise<Frame> {
    return new Promise<Frame>((resolve, reject) => {
      if (this.#state === "closed") return reject(new ClientClosedError());
      if (signal?.aborted) return reject(signal.reason);
      const conn = this.#conn;
      if (conn?.open) {
        this.#sendRequest(conn, frame, resolve, reject, signal, onOk);
        return;
      }
      const q: Queued = { frame, resolve, reject, onOk, signal, onAbort: undefined };
      if (signal) {
        q.onAbort = () => {
          const i = this.#queue.indexOf(q);
          if (i >= 0) this.#queue.splice(i, 1);
          reject(signal.reason);
        };
        signal.addEventListener("abort", q.onAbort, { once: true });
      }
      this.#queue.push(q);
    });
  }

  #sendRequest(
    conn: Conn,
    frame: Frame,
    resolve: (f: Frame) => void,
    reject: (err: unknown) => void,
    signal: AbortSignal | undefined,
    onOk: Pending["onOk"],
  ): void {
    const rid = ++this.#rid;
    const onAbort = () => {
      conn.pending.delete(rid);
      reject(signal!.reason);
    };
    const cleanup = () => signal?.removeEventListener("abort", onAbort);
    conn.pending.set(rid, {
      resolve: (f) => {
        cleanup();
        resolve(f);
      },
      reject: (e) => {
        cleanup();
        reject(e);
      },
      onOk,
    });
    signal?.addEventListener("abort", onAbort, { once: true });
    try {
      conn.ws.send(JSON.stringify({ ...frame, rid }));
    } catch {
      // The socket is going away; its close event rejects the request.
    }
  }

  // --- connection ---

  #connect(): void {
    let ws: WebSocketLike;
    try {
      ws = new this.#WebSocket(this.#opts.url, [SUBPROTOCOL]);
    } catch (err) {
      this.#scheduleRetry(false, toError(err), undefined, 0);
      return;
    }
    const conn: Conn = { ws, open: false, pending: new Map(), closing: undefined, timer: undefined, ended: false };
    this.#conn = conn;
    let handshake: "challenge" | "hello" | "done" = "challenge";
    let failure: Error | undefined;

    conn.timer = setTimeout(() => {
      failure = new Error(`nimbo: no session within ${this.#opts.connectTimeoutMs}ms`);
      this.#onEnd(conn, failure);
      try {
        ws.close();
      } catch {
        // ignore
      }
    }, this.#opts.connectTimeoutMs);

    ws.addEventListener("message", (ev) => {
      if (conn.ended) return;
      if (typeof ev.data !== "string") {
        failure = new Error("nimbo: unexpected binary frame");
        closeWith(ws, 1003, "binary frames are not part of nimbo.v1");
        return;
      }
      let f: Frame;
      try {
        f = JSON.parse(ev.data) as Frame;
      } catch {
        failure = new Error("nimbo: malformed frame from Broker");
        closeWith(ws, 1002, "malformed frame");
        return;
      }
      if (f.op === "closing") {
        conn.closing = { reason: String(f.reason ?? ""), retryAfterMs: Number(f.retry_after_ms) || 0 };
        return;
      }
      if (handshake === "done") {
        this.#onFrame(f);
        return;
      }
      if (handshake === "challenge" && f.op === "challenge") {
        handshake = "hello";
        void this.#hello(conn, f).catch((err) => {
          failure = toError(err);
          closeWith(ws, 1002, "bad challenge");
        });
        return;
      }
      if (handshake === "hello" && f.op === "welcome") {
        handshake = "done";
        this.#onWelcome(conn, f);
        return;
      }
      failure = new Error(`nimbo: unexpected ${String(f.op)} during handshake`);
      closeWith(ws, 1002, "unexpected frame");
    });
    ws.addEventListener("error", (ev) => {
      failure ??= new Error(`nimbo: websocket error${describe(ev)}`);
      // Node's WebSocket (undici) fires no close event after a failed
      // connection attempt, so an error ends the connection here.
      this.#onEnd(conn, failure);
      try {
        ws.close();
      } catch {
        // ignore
      }
    });
    ws.addEventListener("close", (ev) => {
      if (!failure && ev.code !== 1000) {
        failure = new Error(`nimbo: websocket closed (${ev.code}${ev.reason ? ` ${ev.reason}` : ""})`);
      }
      this.#onEnd(conn, failure);
    });
  }

  async #hello(conn: Conn, ch: Frame): Promise<void> {
    const eph = decodeBase32(String(ch.eph));
    const nonce = decodeBase32(String(ch.nonce));
    if (eph.length !== 32) throw new Error("nimbo: bad challenge");
    const proof = await handshakeProof(this.#opts.key.privateKey, this.#publicKey, eph, nonce, this.#authority);
    if (conn.ended) return;
    conn.ws.send(
      JSON.stringify({
        op: "hello",
        key: this.#opts.key.publicKey,
        authority: this.#authority,
        proof: encodeBase32(proof),
        client: this.#opts.clientName,
        max_message_bytes: this.#opts.maxMessageBytes,
      }),
    );
  }

  #onWelcome(conn: Conn, w: Frame): void {
    clearTimeout(conn.timer);
    conn.open = true;
    this.#state = "open";
    this.#backoff = this.#opts.backoffBaseMs;
    this.#session = String(w.session);
    this.#limits = w.limits ? toLimits(w.limits as Record<string, number>) : undefined;
    this.#brokers = Array.isArray(w.brokers) ? (w.brokers as string[]) : [];
    // Send what queued up while disconnected, in order.
    for (const q of this.#queue.splice(0)) {
      if (q.onAbort) q.signal?.removeEventListener("abort", q.onAbort);
      this.#sendRequest(conn, q.frame, q.resolve, q.reject, q.signal, q.onOk);
    }
    this.#emit("connect", { session: this.#session, limits: this.#limits, brokers: this.#brokers });
  }

  #onFrame(f: Frame): void {
    switch (f.op) {
      case "result": {
        const conn = this.#conn;
        const rid = Number(f.rid);
        const p = conn?.pending.get(rid);
        if (!conn || !p) return;
        conn.pending.delete(rid);
        if (f.ok) {
          p.onOk?.(f);
          p.resolve(f);
        } else {
          const e = f.error as { code?: string; detail?: string } | undefined;
          p.reject(new NimboError(e?.code ?? "unknown", e?.detail));
        }
        return;
      }
      case "deliver": {
        const d = this.#delivery(f);
        const waiter = this.#replyWaiters.get(d.on);
        if (waiter) {
          this.#replyWaiters.delete(d.on);
          waiter.resolve(d);
        } else {
          this.#emit("deliver", d);
        }
        return;
      }
      case "bounce":
        this.#emit("bounce", { msg: String(f.msg), reason: String(f.reason) });
        return;
      case "stream_status": {
        const id = String(f.stream);
        const w = this.#streamWaiters.get(id);
        if (!w) return;
        this.#streamWaiters.delete(id);
        if (f.state === "accepted") w.resolve();
        else w.reject(new StreamError(String(f.reason ?? "unknown"), typeof f.detail === "string" && f.detail ? f.detail : undefined));
        return;
      }
    }
  }

  #delivery(f: Frame): Delivery {
    const reply = typeof f.reply === "string" && f.reply !== "" ? f.reply : undefined;
    const client = this;
    const d: Delivery = {
      msg: String(f.msg),
      on: String(f.on),
      payload: (f.payload ?? {}) as JsonObject,
      caps: Array.isArray(f.caps) ? (f.caps as string[]) : [],
      reply,
      stream: typeof f.stream === "string" && f.stream !== "" ? f.stream : undefined,
      streamToken: typeof f.stream_token === "string" && f.stream_token !== "" ? f.stream_token : undefined,
      respond(payload: JsonObject, options?: SendOptions): Promise<SendResult> {
        if (!reply) return Promise.reject(new NimboError("no_reply", "the delivery has no reply cap"));
        return client.send(reply, payload, options);
      },
      acceptStream(options?: AttachOptions): Promise<Stream> {
        return client.acceptStream(d, options);
      },
      rejectStream(reason?: string, options?: AttachOptions): Promise<void> {
        return client.rejectStream(d, reason, options);
      },
    };
    return d;
  }

  /** Called once per connection when it ends, for whatever reason. */
  #onEnd(conn: Conn, error: Error | undefined): void {
    if (conn.ended) return;
    this.#endConn(conn);
    if (this.#conn !== conn) return; // close() already handled it
    this.#conn = undefined;
    const err = new SessionClosedError();
    for (const p of conn.pending.values()) p.reject(err);
    conn.pending.clear();
    if (conn.open) this.#failStreamWaiters(err);
    if (this.#state === "closed") return;
    this.#state = "connecting";
    const closing = conn.closing;
    if (closing && !error && !conn.open) error = new Error(`nimbo: Broker closing (${closing.reason})`);
    this.#scheduleRetry(conn.open, error, closing?.reason, closing?.retryAfterMs ?? 0);
  }

  #endConn(conn: Conn): void {
    conn.ended = true;
    clearTimeout(conn.timer);
  }

  #scheduleRetry(wasOpen: boolean, error: Error | undefined, closing: string | undefined, retryAfterMs: number): void {
    let wait: number;
    if (wasOpen) {
      // A session ended: start over from the base delay.
      this.#backoff = this.#opts.backoffBaseMs;
      wait = jitter(this.#backoff);
    } else {
      wait = jitter(this.#backoff);
      this.#backoff = Math.min(2 * this.#backoff, this.#opts.backoffMaxMs);
    }
    if (retryAfterMs > 0) wait = retryAfterMs;
    this.#retryTimer = setTimeout(() => {
      this.#retryTimer = undefined;
      if (this.#state !== "closed") this.#connect();
    }, wait);
    this.#emit("disconnect", { error, closing, wasOpen, retryInMs: wait });
  }

  // --- keepAlive ---

  async #renewKept(): Promise<void> {
    if (this.#state !== "open" || this.#kept.size === 0) return;
    let items: RenewItem[];
    try {
      items = await this.renew([...this.#kept]);
    } catch {
      return; // session trouble: next round
    }
    for (const it of items) {
      if (it.ok || !this.#kept.has(it.cap)) continue;
      const code = it.error?.code ?? "unknown";
      if (code === "unavailable" || code === "rate_limited") continue;
      this.#kept.delete(it.cap);
      this.#emit("capLost", { cap: it.cap, error: it.error ?? { code } });
    }
    if (this.#kept.size === 0) this.#stopRenewing();
  }

  #stopRenewing(): void {
    clearInterval(this.#renewTimer);
    this.#renewTimer = undefined;
  }

  #emit<K extends keyof ClientEvents>(event: K, value: ClientEvents[K]): void {
    const set = this.#listeners[event] as Set<Listener<ClientEvents[K]>> | undefined;
    if (!set) return;
    for (const l of [...set]) {
      try {
        l(value);
      } catch (err) {
        // A throwing listener must not break the session; surface it like an unhandled error.
        queueMicrotask(() => {
          throw err;
        });
      }
    }
  }
}

interface WireRenewItem {
  cap: string;
  ok?: boolean;
  ttl_ms?: number;
  error?: { code: string; detail?: string };
}

interface WireCapInfo {
  lineage: string;
  kind: string;
  single_use?: boolean;
  target: string;
  holder: string;
  issuer: string;
  minted_at: number;
  schema?: JsonObject;
  base?: JsonObject;
  auto_reply?: boolean;
  allow_stream?: boolean;
  ttl_ms: number;
}

function toLimits(l: Record<string, number>): Limits {
  return {
    maxPayloadBytes: l.max_payload_bytes ?? 0,
    maxCarriedCaps: l.max_carried_caps ?? 0,
    maxSchemaBytes: l.max_schema_bytes ?? 0,
    maxBaseBytes: l.max_base_bytes ?? 0,
    actionsPerSec: l.actions_per_sec ?? 0,
    mintsPerSec: l.mints_per_sec ?? 0,
    maxLiveCaps: l.max_live_caps ?? 0,
    defaultTtlMs: l.default_ttl_ms ?? 0,
    maxTtlMs: l.max_ttl_ms ?? 0,
    maxPendingStreams: l.max_pending_streams ?? 0,
    streamAttachTtlMs: l.stream_attach_ttl_ms ?? 0,
    streamSocketsPerIp: l.stream_sockets_per_ip ?? 0,
  };
}

function deferred<T>(): { promise: Promise<T>; resolve(v: T): void; reject(err: unknown): void } {
  let resolve!: (v: T) => void;
  let reject!: (err: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

/** A delay between d/2 and d, as the Go SDK does. */
function jitter(d: number): number {
  return d / 2 + (Math.random() * d) / 2;
}

function toError(err: unknown): Error {
  return err instanceof Error ? err : new Error(String(err));
}

function describe(ev: unknown): string {
  const e = ev as { message?: unknown; error?: unknown } | undefined;
  const m = e?.message ?? (e?.error instanceof Error ? e.error.message : undefined);
  return typeof m === "string" && m ? `: ${m}` : "";
}

/**
 * Closes with a protocol-error code where the implementation allows it.
 * WHATWG WebSockets (browsers, Node, Bun, Workers) accept only 1000 and
 * 3000–4999 and throw on others; they then close without a code.
 */
function closeWith(ws: WebSocketLike, code: number, reason: string): void {
  try {
    ws.close(code, reason);
  } catch {
    try {
      ws.close();
    } catch {
      // already closed
    }
  }
}

/** Lets Node and Bun exit while only the renew timer is pending; a no-op elsewhere. */
function unref(t: unknown): void {
  (t as { unref?: () => void } | undefined)?.unref?.();
}

function stripUndefined<T extends object>(o: T): T {
  return Object.fromEntries(Object.entries(o).filter(([, v]) => v !== undefined)) as T;
}
