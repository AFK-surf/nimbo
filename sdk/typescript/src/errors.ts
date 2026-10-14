/** Error codes a Broker reports (protocol §7). */
export type ProtocolErrorCode =
  | "bad_request"
  | "invalid_cap"
  | "not_holder"
  | "expired"
  | "consumed"
  | "target_gone"
  | "schema_violation"
  | "base_conflict"
  | "too_large"
  | "rate_limited"
  | "unavailable"
  | "stream_not_allowed"
  | "invalid_challenge"
  | "bad_proof";

/**
 * Every `NimboError` code: the protocol's, plus `no_reply` (`request` on a cap
 * without auto_reply) and `timeout` (`request` got no reply in time).
 */
export type ErrorCode = ProtocolErrorCode | "no_reply" | "timeout";

const RETRYABLE: ReadonlySet<string> = new Set(["unavailable", "rate_limited", "timeout"]);

/** A request the Broker answered with an error, or a failed `request`. */
export class NimboError extends Error {
  override name = "NimboError";
  /** A code from `ErrorCode`; a newer Broker may send codes this SDK does not know. */
  readonly code: ErrorCode | (string & {});
  readonly detail: string | undefined;

  constructor(code: string, detail?: string) {
    super(detail ? `${code}: ${detail}` : code);
    this.code = code;
    this.detail = detail;
  }

  /** Whether trying again later can succeed (`unavailable`, `rate_limited`, `timeout`). */
  get retryable(): boolean {
    return RETRYABLE.has(this.code);
  }
}

/**
 * The session ended after the request was sent and before its result came
 * back. The Broker may or may not have carried it out. It is not retried
 * for you: a send is delivered at most once.
 */
export class SessionClosedError extends Error {
  override name = "SessionClosedError";
  constructor() {
    super("nimbo: session closed before the result arrived");
  }
}

/** The client was closed. */
export class ClientClosedError extends Error {
  override name = "ClientClosedError";
  constructor() {
    super("nimbo: client closed");
  }
}
