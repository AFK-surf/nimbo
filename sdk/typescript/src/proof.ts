import { x25519 } from "@noble/curves/ed25519.js";

const encoder = new TextEncoder();

/**
 * The hello proof (protocol §3):
 * HMAC-SHA256(SHA-256("nimbo/v1/auth" ‖ ss), nonce ‖ authority ‖ E_pub ‖ B_eph_pub),
 * where ss = X25519(E_priv, B_eph_pub).
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
  const subtle = globalThis.crypto.subtle;
  const k = await subtle.digest("SHA-256", concat(encoder.encode("nimbo/v1/auth"), ss));
  const hk = await subtle.importKey("raw", k, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const mac = await subtle.sign("HMAC", hk, concat(nonce, encoder.encode(authority), publicKey, brokerEph));
  return new Uint8Array(mac);
}

function concat(...parts: Uint8Array[]): Uint8Array<ArrayBuffer> {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let o = 0;
  for (const p of parts) {
    out.set(p, o);
    o += p.length;
  }
  return out;
}
