// Streams (protocol §10): a full-duplex WebSocket pipe between a cap's
// holder and its target. Each side attaches a data socket to its own
// Broker with a single-use token, and the Brokers relay between them.

/** The WebSocket subprotocol of stream data sockets. */
export const STREAM_SUBPROTOCOL = "nimbo.stream.v1";

/** Close codes on data sockets. 1000 and 3000–4899 come from the other endpoint; the rest from Brokers. */
export const StreamCloseCode = {
  normal: 1000,
  /** A Broker on the path is draining. */
  draining: 1001,
  /** The target refused the stream; endpoints may send it. */
  rejected: 4901,
  ttlExpired: 4902,
  targetGone: 4903,
  receiverBusy: 4904,
  /** The other side never attached. */
  peerAttachTimeout: 4905,
  /** The other endpoint's socket, a Broker or the mesh failed. */
  peerLost: 4911,
  rateLimited: 4929,
  unavailable: 4950,
  /** No close frame: the connection dropped (never sent on the wire). */
  abnormal: 1006,
} as const;

/** One message: a string for text, bytes for binary. */
export type StreamMessage = string | Uint8Array;

export interface StreamCloseEvent {
  code: number;
  reason: string;
}

export interface StreamEvents {
  message: StreamMessage;
  close: StreamCloseEvent;
}

/** The part of the WHATWG WebSocket a data socket uses. */
export interface DataSocketLike {
  binaryType?: string;
  readonly bufferedAmount?: number;
  send(data: string | ArrayBuffer | ArrayBufferView): void;
  close(code?: number, reason?: string): void;
  addEventListener(type: "open", listener: () => void): void;
  addEventListener(type: "message", listener: (ev: { data: unknown }) => void): void;
  addEventListener(type: "close", listener: (ev: { code: number; reason: string }) => void): void;
  addEventListener(type: "error", listener: (ev: unknown) => void): void;
}

export type DataSocketConstructor = new (url: string, protocols: string[]) => DataSocketLike;

/** The stream failed before the target attached (reported on the holder's session). */
export class StreamError extends Error {
  override name = "StreamError";
  /** `ttl_expired`, `receiver_busy`, `target_gone`, `unavailable`, `peer_attach_timeout`, `canceled`, … */
  readonly reason: string;
  readonly detail: string | undefined;

  constructor(reason: string, detail?: string) {
    super(`nimbo: stream failed: ${reason}${detail ? `: ${detail}` : ""}`);
    this.reason = reason;
    this.detail = detail;
  }
}

/**
 * The Broker refused the data socket: an unknown, used or expired token (or
 * the wrong Broker), the holder's side is gone, the other Broker is
 * unreachable, or a rate limit. WebSocket APIs do not expose the HTTP
 * status, so `code` is the close code seen (usually 1006).
 */
export class StreamAttachError extends Error {
  override name = "StreamAttachError";
  readonly code: number;

  constructor(code: number, message: string) {
    super(`nimbo: stream attach failed: ${message}`);
    this.code = code;
  }
}

export interface AttachOptions {
  signal?: AbortSignal;
  /** Give up attaching after this long (default 30s). */
  timeoutMs?: number;
}

type Listener<T> = (event: T) => void;

/** @internal What the client hands a Stream. */
export interface StreamInit {
  id: string;
  replyId: string | undefined;
  /** Data-socket URL without the token. */
  url: string;
  token: string;
  WebSocket: DataSocketConstructor;
  /** Holder only: settles with the stream's status. */
  accepted: Promise<void> | undefined;
}

/**
 * One side of a stream. Messages keep their type and boundaries. Read them
 * with `on("message")`, or with `receive()` / `for await`; messages that
 * arrive while there is no `message` listener wait for `receive()`.
 *
 * A stream is independent of the `Client` that opened or accepted it: closing
 * the client or losing its session does not close the stream.
 */
export class Stream {
  /** The stream ID (on the holder's side, also the offer's message ID). */
  readonly id: string;
  /** Lineage ID replies to the offer arrive on, when the cap has auto_reply. */
  readonly replyId: string | undefined;
  /**
   * Holder: resolves once the target attached; rejects with `StreamError` if
   * the stream failed first, or `SessionClosedError` / `ClientClosedError` if
   * the status cannot arrive any more. Target: already resolved.
   */
  readonly accepted: Promise<void>;
  /** Resolves when the data socket closes, with the code and reason. Never rejects. */
  readonly closed: Promise<StreamCloseEvent>;

  #init: StreamInit;
  #ws: DataSocketLike | undefined;
  #state: "detached" | "attaching" | "open" | "closed" = "detached";
  #queue: StreamMessage[] = [];
  #readers: ((m: StreamMessage | undefined) => void)[] = [];
  #listeners: { [K in keyof StreamEvents]?: Set<Listener<StreamEvents[K]>> } = {};
  #inbound: Promise<void> = Promise.resolve();
  #closeInfo: StreamCloseEvent | undefined;
  #resolveClosed!: (e: StreamCloseEvent) => void;

  /** @internal Use `Client.openStream` or `Delivery.acceptStream`. */
  constructor(init: StreamInit) {
    this.#init = init;
    this.id = init.id;
    this.replyId = init.replyId;
    this.accepted = init.accepted ?? Promise.resolve();
    this.accepted.catch(() => {}); // a stream may be used without awaiting its status
    this.closed = new Promise((r) => (this.#resolveClosed = r));
  }

  /** `detached` (not attached yet), `attaching`, `open` or `closed`. */
  get state(): "detached" | "attaching" | "open" | "closed" {
    return this.#state;
  }

  /** The holder's token of a stream opened with `detached`, until it attaches. */
  get token(): string | undefined {
    return this.#state === "detached" ? this.#init.token : undefined;
  }

  /** Bytes queued on the socket and not yet sent; use it to pace large writes. */
  get bufferedAmount(): number {
    return this.#ws?.bufferedAmount ?? 0;
  }

  /**
   * Attaches the data socket. `openStream` and `acceptStream` do this for
   * you; call it yourself only for a stream opened with `detached`.
   */
  attach(options: AttachOptions = {}): Promise<void> {
    if (this.#state !== "detached") return Promise.reject(new Error("nimbo: stream already attached"));
    this.#state = "attaching";
    const u = new URL(this.#init.url);
    u.searchParams.set("token", this.#init.token);
    return new Promise<void>((resolve, reject) => {
      let ws: DataSocketLike;
      try {
        ws = new this.#init.WebSocket(u.toString(), [STREAM_SUBPROTOCOL]);
      } catch (err) {
        this.#finish({ code: StreamCloseCode.abnormal, reason: "" });
        return reject(new StreamAttachError(StreamCloseCode.abnormal, err instanceof Error ? err.message : String(err)));
      }
      this.#ws = ws;
      try {
        ws.binaryType = "arraybuffer";
      } catch {
        // not settable: messages are converted below
      }
      let opened = false;
      let failed = false;
      const signal = options.signal;
      const fail = (code: number, msg: string) => {
        if (opened || failed) return;
        failed = true;
        cleanup();
        try {
          ws.close();
        } catch {
          // ignore
        }
        this.#finish({ code, reason: "" });
        reject(new StreamAttachError(code, msg));
      };
      const onAbort = () => {
        if (opened || failed) return;
        failed = true;
        cleanup();
        try {
          ws.close();
        } catch {
          // ignore
        }
        this.#finish({ code: StreamCloseCode.abnormal, reason: "" });
        reject(signal!.reason);
      };
      const timeoutMs = options.timeoutMs ?? 30_000;
      const timer = setTimeout(() => fail(StreamCloseCode.abnormal, `no answer within ${timeoutMs}ms`), timeoutMs);
      const cleanup = () => {
        clearTimeout(timer);
        signal?.removeEventListener("abort", onAbort);
      };
      if (signal?.aborted) return onAbort();
      signal?.addEventListener("abort", onAbort, { once: true });

      ws.addEventListener("open", () => {
        if (failed) return;
        opened = true;
        cleanup();
        this.#state = "open";
        resolve();
      });
      ws.addEventListener("message", (ev) => {
        if (!opened) return;
        this.#onData(ev.data);
      });
      ws.addEventListener("error", () => {
        fail(StreamCloseCode.abnormal, "the Broker refused the data socket (bad, used or expired token, or the other side is gone)");
      });
      ws.addEventListener("close", (ev) => {
        if (!opened) {
          fail(ev.code, `closed before opening (${ev.code}${ev.reason ? ` ${ev.reason}` : ""})`);
          return;
        }
        // Messages still being converted are dispatched first.
        void this.#inbound.then(() => this.#finish({ code: ev.code, reason: ev.reason ?? "" }));
      });
    });
  }

  /** Sends one message: a string as text, bytes as binary. */
  send(message: StreamMessage | ArrayBuffer): void {
    if (this.#state !== "open" || !this.#ws) throw new Error(`nimbo: stream is ${this.#state}`);
    this.#ws.send(message);
  }

  /**
   * Closes the stream; the other side gets the code and reason. Use 1000 or
   * 3000–4899 (or 4901 to refuse a stream as its target).
   */
  close(code: number = StreamCloseCode.normal, reason = ""): void {
    if (this.#state === "closed") return;
    if (this.#state === "detached") {
      this.#finish({ code, reason });
      return;
    }
    try {
      this.#ws?.close(code, reason);
    } catch {
      this.#ws?.close();
    }
  }

  /** The next message, or `undefined` once the stream has closed and every message was read. */
  receive(): Promise<StreamMessage | undefined> {
    const m = this.#queue.shift();
    if (m !== undefined) return Promise.resolve(m);
    if (this.#state === "closed") return Promise.resolve(undefined);
    return new Promise((r) => this.#readers.push(r));
  }

  /** Iterates over messages until the stream closes. */
  async *[Symbol.asyncIterator](): AsyncGenerator<StreamMessage, void, undefined> {
    for (;;) {
      const m = await this.receive();
      if (m === undefined) return;
      yield m;
    }
  }

  /** Adds a listener; returns a function that removes it. */
  on<K extends keyof StreamEvents>(event: K, listener: Listener<StreamEvents[K]>): () => void {
    let set = this.#listeners[event] as Set<Listener<StreamEvents[K]>> | undefined;
    if (!set) {
      set = new Set();
      (this.#listeners as Record<string, unknown>)[event] = set;
    }
    set.add(listener);
    if (event === "close" && this.#closeInfo) {
      const info = this.#closeInfo;
      queueMicrotask(() => (listener as Listener<StreamCloseEvent>)(info));
    }
    return () => {
      set.delete(listener);
    };
  }

  #onData(data: unknown): void {
    if (typeof data === "string") return this.#chain(() => data);
    if (data instanceof ArrayBuffer) return this.#chain(() => new Uint8Array(data));
    if (ArrayBuffer.isView(data)) return this.#chain(() => new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
    const blob = data as { arrayBuffer?: () => Promise<ArrayBuffer> };
    if (typeof blob?.arrayBuffer === "function") {
      // A Blob converts asynchronously; the chain keeps message order.
      return this.#chain(async () => new Uint8Array(await blob.arrayBuffer!()));
    }
  }

  #chain(convert: () => StreamMessage | Promise<StreamMessage>): void {
    this.#inbound = this.#inbound.then(async () => this.#deliver(await convert())).catch(() => {});
  }

  #deliver(m: StreamMessage): void {
    const r = this.#readers.shift();
    if (r) return r(m);
    const set = this.#listeners.message;
    if (set && set.size > 0) {
      this.#emit("message", m);
      return;
    }
    this.#queue.push(m);
  }

  #finish(info: StreamCloseEvent): void {
    if (this.#state === "closed") return;
    this.#state = "closed";
    this.#closeInfo = info;
    for (const r of this.#readers.splice(0)) r(undefined);
    this.#resolveClosed(info);
    this.#emit("close", info);
  }

  #emit<K extends keyof StreamEvents>(event: K, value: StreamEvents[K]): void {
    const set = this.#listeners[event] as Set<Listener<StreamEvents[K]>> | undefined;
    if (!set) return;
    for (const l of [...set]) {
      try {
        l(value);
      } catch (err) {
        queueMicrotask(() => {
          throw err;
        });
      }
    }
  }
}

/** The data-socket URL on the same authority as a session URL. */
export function streamURL(sessionURL: string): string {
  const u = new URL(sessionURL);
  u.pathname = u.pathname.endsWith("/v1/ws") ? u.pathname.slice(0, -"/v1/ws".length) + "/v1/stream" : "/v1/stream";
  u.search = "";
  u.hash = "";
  return u.toString();
}
