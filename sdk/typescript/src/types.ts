/** A JSON object: what payloads, schemas and bases are. */
export type JsonObject = Record<string, unknown>;

/** Limits the Broker advertised in `welcome` (protocol §8). */
export interface Limits {
  maxPayloadBytes: number;
  maxCarriedCaps: number;
  maxSchemaBytes: number;
  maxBaseBytes: number;
  actionsPerSec: number;
  mintsPerSec: number;
  maxLiveCaps: number;
  defaultTtlMs: number;
  maxTtlMs: number;
}

/** A message that arrived on one of this endpoint's caps. */
export interface Delivery<T extends JsonObject = JsonObject> {
  /** Message ID. */
  readonly msg: string;
  /** Lineage ID it arrived on: a cap ID `mint` returned, or the `replyId` of an earlier send. */
  readonly on: string;
  /** The payload, with the cap's base fields merged in. */
  readonly payload: T;
  /** Caps the message carried, now held by this endpoint. */
  readonly caps: string[];
  /** Single-use reply cap, when the cap has auto_reply. */
  readonly reply: string | undefined;
  /** Sends `payload` on `reply`. Rejects with `NimboError("no_reply")` when there is none. */
  respond(payload: JsonObject, options?: SendOptions): Promise<SendResult>;
}

/** A message this endpoint sent that could not be delivered (best effort). */
export interface Bounce {
  msg: string;
  reason: "ttl_expired" | "receiver_busy" | "too_large" | "target_gone" | "unavailable" | (string & {});
}

export interface AbortOptions {
  /** Abandons the call: it rejects with the signal's reason. */
  signal?: AbortSignal;
}

export interface MintOptions extends AbortOptions {
  /** JSON Schema (draft 2020-12) every payload on the cap must match. */
  schema?: JsonObject;
  /** JSON object merged into every payload on the cap. */
  base?: JsonObject;
  /** Receivers get a single-use reply cap with each message. */
  autoReply?: boolean;
  /** Return only once two placement-set Brokers hold the cap; use when the ID leaves this session. */
  durable?: boolean;
}

export interface TransferOptions extends AbortOptions {
  durable?: boolean;
}

export interface ReplyOptions {
  schema?: JsonObject;
  base?: JsonObject;
  autoReply?: boolean;
}

export interface SendOptions extends AbortOptions {
  /** Caps to carry to the receiver (at most 16). */
  caps?: string[];
  /** Time to live of the message (Broker default 10s, max 60s). */
  ttlMs?: number;
  /** Schema, base and auto_reply of the reply cap the receiver gets. */
  reply?: ReplyOptions;
}

export interface SendResult {
  /** Message ID, as reported in bounces. */
  msg: string;
  /** Lineage ID replies will arrive on, when the target cap has auto_reply. */
  replyId: string | undefined;
}

export interface RequestOptions extends SendOptions {
  /** How long to wait for the reply (default 30s). */
  timeoutMs?: number;
}

export interface RenewItem {
  cap: string;
  ok: boolean;
  ttlMs?: number;
  error?: { code: string; detail?: string };
}

export interface CapInfo {
  lineage: string;
  kind: string;
  singleUse: boolean;
  /** Endpoint key messages on the cap go to. */
  target: string;
  /** Endpoint key that holds the cap. */
  holder: string;
  issuer: string;
  mintedAt: number;
  schema?: JsonObject;
  base?: JsonObject;
  autoReply: boolean;
  ttlMs: number;
}

export type ClientState = "connecting" | "open" | "closed";

export interface ConnectEvent {
  session: string;
  limits: Limits | undefined;
  /** Other Brokers the Broker suggests for redundant sessions. */
  brokers: string[];
}

export interface DisconnectEvent {
  /** Why the attempt or session ended, when it was not a clean close. */
  error: Error | undefined;
  /** Reason from the Broker's `closing` frame (`draining`, `fenced`, `auth_failed`, …). */
  closing: string | undefined;
  /** Whether a session had been established (false: the attempt failed). */
  wasOpen: boolean;
  /** When the next attempt starts. */
  retryInMs: number;
}

export interface CapLostEvent {
  cap: string;
  error: { code: string; detail?: string };
}

export interface ClientEvents {
  deliver: Delivery;
  bounce: Bounce;
  connect: ConnectEvent;
  disconnect: DisconnectEvent;
  /** A cap passed to `keepAlive` could not be renewed and is no longer kept alive. */
  capLost: CapLostEvent;
}
