import { x25519 } from "@noble/curves/ed25519.js";
import { decodeBase32, encodeBase32 } from "./base32.js";

/** An endpoint's X25519 identity. */
export interface EndpointKey {
  /** The endpoint key in base32: what others pass to `transfer`. */
  readonly publicKey: string;
  /** The 32-byte X25519 private key. */
  readonly privateKey: Uint8Array;
}

/** Creates a new random endpoint key. */
export function generateKey(): EndpointKey {
  return importKey(x25519.utils.randomSecretKey());
}

/** Rebuilds a key from its private half (32 bytes, or their base32 from `exportKey`). */
export function importKey(privateKey: Uint8Array | string): EndpointKey {
  const priv = typeof privateKey === "string" ? decodeBase32(privateKey) : Uint8Array.from(privateKey);
  if (priv.length !== 32) throw new TypeError("nimbo: an endpoint private key is 32 bytes");
  return Object.freeze({ publicKey: encodeBase32(x25519.getPublicKey(priv)), privateKey: priv });
}

/** The private key in base32, for `importKey`. Keep it secret. */
export function exportKey(key: EndpointKey): string {
  return encodeBase32(key.privateKey);
}
