package wire

import (
	"crypto/rand"
	"testing"

	"golang.org/x/crypto/curve25519"
)

// BenchmarkHandshakeVerify is the Broker's per-session handshake cost: one
// X25519 for its ephemeral key, one to derive ss, and the HMAC proof.
func BenchmarkHandshakeVerify(b *testing.B) {
	var epriv, nonce [32]byte
	rand.Read(epriv[:])
	rand.Read(nonce[:])
	epub, _ := curve25519.X25519(epriv[:], curve25519.Basepoint)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var bpriv [32]byte
		rand.Read(bpriv[:])
		bpub, _ := curve25519.X25519(bpriv[:], curve25519.Basepoint)
		ss, _ := curve25519.X25519(bpriv[:], epub)
		Proof(ss, nonce[:], "broker.example.com", epub, bpub)
	}
}
