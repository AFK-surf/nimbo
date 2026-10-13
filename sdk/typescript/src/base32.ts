// Unpadded lowercase RFC 4648 base32 (alphabet a-z2-7), the encoding of every
// key, ID, nonce and proof in the protocol.

const ALPHABET = "abcdefghijklmnopqrstuvwxyz234567";
const LOOKUP = new Int8Array(128).fill(-1);
for (let i = 0; i < ALPHABET.length; i++) LOOKUP[ALPHABET.charCodeAt(i)] = i;

export function encodeBase32(bytes: Uint8Array): string {
  let out = "";
  let buf = 0;
  let bits = 0;
  for (const b of bytes) {
    buf = (buf << 8) | b;
    bits += 8;
    while (bits >= 5) {
      bits -= 5;
      out += ALPHABET[(buf >>> bits) & 31];
    }
    buf &= (1 << bits) - 1;
  }
  if (bits > 0) out += ALPHABET[(buf << (5 - bits)) & 31];
  return out;
}

/** Decodes unpadded lowercase base32; throws on any other input. */
export function decodeBase32(s: string): Uint8Array {
  const out = new Uint8Array(Math.floor((s.length * 5) / 8));
  let buf = 0;
  let bits = 0;
  let j = 0;
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    const v = c < 128 ? LOOKUP[c]! : -1;
    if (v < 0) throw new SyntaxError(`invalid base32 character at ${i}`);
    buf = (buf << 5) | v;
    bits += 5;
    if (bits >= 8) {
      bits -= 8;
      out[j++] = (buf >>> bits) & 0xff;
    }
    buf &= (1 << bits) - 1;
  }
  // Leftover bits must be zero padding of the last byte, and fewer than 5.
  if (bits >= 5 || buf !== 0) throw new SyntaxError("invalid base32 length or trailing bits");
  return out;
}
