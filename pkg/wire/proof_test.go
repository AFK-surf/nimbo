package wire

import (
	"encoding/base32"
	"testing"

	"golang.org/x/crypto/curve25519"
)

// TestProofVector pins the handshake proof for fixed inputs. The TypeScript
// SDK's tests (sdk/typescript/test/unit.ts) check the same vector.
func TestProofVector(t *testing.T) {
	b32 := base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)
	var e, b, nonce [32]byte
	for i := range 32 {
		e[i], b[i], nonce[i] = byte(i+1), byte(i+101), byte(200+i)
	}
	ePub, _ := curve25519.X25519(e[:], curve25519.Basepoint)
	bPub, _ := curve25519.X25519(b[:], curve25519.Basepoint)
	ss, err := curve25519.X25519(e[:], bPub)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ name, got, want string }{
		{"E_pub", b32.EncodeToString(ePub), "a6rxzpauecj4rn2v3qnrb2dmwqtdoswrnkufh3il37alfodndr6a"},
		{"B_eph_pub", b32.EncodeToString(bPub), "k4khnhirnp3winvoos6hspjmgcwrsa6ftlcsooafy7rgtc2bbq3a"},
		{"proof", b32.EncodeToString(Proof(ss, nonce[:], "broker.example.com:8443", ePub, bPub)), "eduhkxbs7jcu7n3zvn3fakah6lsnxah76bhsbunk7mwm3vb7ymia"},
	} {
		if c.got != c.want {
			t.Errorf("%s = %s, want %s", c.name, c.got, c.want)
		}
	}
}

// TestDelegationProofVector pins the delegation proof (design §8.6) for fixed
// inputs. The TypeScript SDK's tests check the same vector.
func TestDelegationProofVector(t *testing.T) {
	b32 := base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)
	var target, eph, holder, broker, schema [32]byte
	for i := range 32 {
		target[i], eph[i], holder[i], broker[i], schema[i] = byte(i+1), byte(i+101), byte(i+51), byte(i+151), byte(i+201)
	}
	targetPub, _ := curve25519.X25519(target[:], curve25519.Basepoint)
	ephPub, _ := curve25519.X25519(eph[:], curve25519.Basepoint)
	holderPub, _ := curve25519.X25519(holder[:], curve25519.Basepoint)
	ss, err := curve25519.X25519(target[:], ephPub)
	if err != nil {
		t.Fatal(err)
	}
	g := Grant{Broker: broker, Eph: [32]byte(ephPub), Target: [32]byte(targetPub), Holder: [32]byte(holderPub), AutoReply: true, Schema: &schema}
	if n := len(g.Message()); n != 193 {
		t.Fatalf("message is %d bytes", n)
	}
	if got, want := b32.EncodeToString(DelegationProof(ss, &g)), "npjdjxyrahv5bzez5amj66h4o6xl4kawcmfl3jknacwwvny5frzq"; got != want {
		t.Errorf("proof = %s, want %s", got, want)
	}
	// The Broker's side of the agreement gives the same proof.
	ss2, _ := curve25519.X25519(eph[:], targetPub)
	if b32.EncodeToString(DelegationProof(ss2, &g)) != b32.EncodeToString(DelegationProof(ss, &g)) {
		t.Error("the two sides disagree")
	}
}
