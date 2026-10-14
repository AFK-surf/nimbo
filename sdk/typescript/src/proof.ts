import { x25519 } from "@noble/curves/ed25519.js";
import { hmac } from "@noble/hashes/hmac.js";
import { sha256 } from "@noble/hashes/sha2.js";
import type { Grant } from "./types.js";

const encoder = new TextEncoder();

/**
 * The hello proof (protocol §3):
 * HMAC-SHA256(SHA-256("nimbo/v1/auth" ‖ ss), nonce ‖ authority ‖ E_pub ‖ B_eph_pub),
 * where ss = X25519(E_priv, B_eph_pub).
 *
 * The hashes come from @noble/hashes rather than crypto.subtle, which
 * browsers offer only in secure contexts (HTTPS pages and localhost).
 */
export async function handshakeProof(
  privateKey: Uint8Array,
  publicKey: Uint8Array,
  brokerEph: Uint8Array,
  nonce: Uint8Array,
  authority: string,
): Promise<Uint8Array> {
  const ss = x25519.getSharedSecret(privateKey, brokerEph);
  if (ss.every((b) => b === 0)) throw new Error("nimbo: all-zero shared secret");
  const k = sha256(concat(encoder.encode("nimbo/v1/auth"), ss));
  return hmac(sha256, k, concat(nonce, encoder.encode(authority), publicKey, brokerEph));
}

/**
 * The grant a delegation proof covers, as protocol §11 lays it out:
 * broker_id ‖ eph ‖ target ‖ holder ‖ flags ‖ schema_hash ‖ base_hash, with
 * 32 zero bytes for a missing hash (193 bytes).
 */
export function grantMessage(g: Grant): Uint8Array {
  const flags =
    (g.autoReply ? 1 : 0) | (g.allowStream ? 2 : 0) | (g.schemaHash ? 4 : 0) | (g.baseHash ? 8 : 0);
  const zero = new Uint8Array(32);
  return concat(g.broker, g.eph, g.target, g.holder, Uint8Array.of(flags), g.schemaHash ?? zero, g.baseHash ?? zero);
}

/**
 * The delegation proof (protocol §11):
 * HMAC-SHA256(SHA-256("nimbo/v1/delegate" ‖ ss), grant), where
 * ss = X25519(target_priv, eph).
 */
export function delegationProof(privateKey: Uint8Array, g: Grant): Uint8Array {
  const ss = x25519.getSharedSecret(privateKey, g.eph);
  if (ss.every((b) => b === 0)) throw new Error("nimbo: all-zero shared secret");
  const k = sha256(concat(encoder.encode("nimbo/v1/delegate"), ss));
  return hmac(sha256, k, grantMessage(g));
}

function concat(...parts: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let o = 0;
  for (const p of parts) {
    out.set(p, o);
    o += p.length;
  }
  return out;
}
