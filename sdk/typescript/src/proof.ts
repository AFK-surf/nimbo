import { x25519 } from "@noble/curves/ed25519.js";
import { hmac } from "@noble/hashes/hmac.js";
import { sha256 } from "@noble/hashes/sha2.js";

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

function concat(...parts: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let o = 0;
  for (const p of parts) {
    out.set(p, o);
    o += p.length;
  }
  return out;
}
