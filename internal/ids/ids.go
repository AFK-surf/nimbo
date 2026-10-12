// Package ids defines the identifiers used across Nimbo: Broker IDs, endpoint
// keys, cap IDs and blob hashes, and their text encodings.
package ids

import (
	"crypto/sha256"
	"encoding/base32"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
)

// B32 is unpadded lowercase RFC 4648 base32, used for every ID and key that
// endpoints see.
var B32 = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

// BrokerID is SHA-256 of a Broker's node public key.
type BrokerID [32]byte

// Short is the first 8 bytes of a Broker ID.
type Short [8]byte

// Key is an endpoint's X25519 public key.
type Key [32]byte

// CapID is a cap's 128-bit random ID; it is the token an endpoint holds.
type CapID [16]byte

// Hash is a SHA-256 digest, used for blob addresses and placement keys.
type Hash [32]byte

// Lineage identifies the mint (or reply) a cap descends from.
type Lineage [16]byte

// MsgID identifies a message.
type MsgID [16]byte

func (b BrokerID) Short() Short {
	var s Short
	copy(s[:], b[:8])
	return s
}

func (b BrokerID) String() string { return hex.EncodeToString(b[:]) }
func (b BrokerID) Abbrev() string { return hex.EncodeToString(b[:4]) }
func (s Short) String() string    { return hex.EncodeToString(s[:]) }
func (k Key) String() string      { return B32.EncodeToString(k[:]) }
func (c CapID) String() string    { return B32.EncodeToString(c[:]) }
func (h Hash) String() string     { return B32.EncodeToString(h[:]) }
func (l Lineage) String() string  { return B32.EncodeToString(l[:]) }
func (m MsgID) String() string    { return B32.EncodeToString(m[:]) }

// IsZero reports whether the ID is all zero bytes.
func (b BrokerID) IsZero() bool { return b == BrokerID{} }

// BrokerIDFromPub derives a Broker ID from a node public key.
func BrokerIDFromPub(pub []byte) BrokerID { return BrokerID(sha256.Sum256(pub)) }

// ParseBrokerID parses the hex form used in S3 keys.
func ParseBrokerID(s string) (BrokerID, error) {
	var b BrokerID
	if len(s) != 64 {
		return b, errors.New("broker id: bad length")
	}
	_, err := hex.Decode(b[:], []byte(s))
	return b, err
}

func decodeB32(s string, out []byte) error {
	if len(s) != B32.EncodedLen(len(out)) {
		return errors.New("bad length")
	}
	n, err := B32.Decode(out, []byte(s))
	if err != nil {
		return err
	}
	if n != len(out) {
		return errors.New("bad length")
	}
	return nil
}

func ParseKey(s string) (Key, error) {
	var k Key
	err := decodeB32(s, k[:])
	return k, err
}

func ParseCapID(s string) (CapID, error) {
	var c CapID
	err := decodeB32(s, c[:])
	return c, err
}

func ParseLineage(s string) (Lineage, error) {
	var l Lineage
	err := decodeB32(s, l[:])
	return l, err
}

func ParseMsgID(s string) (MsgID, error) {
	var m MsgID
	err := decodeB32(s, m[:])
	return m, err
}

func ParseHash(s string) (Hash, error) {
	var h Hash
	err := decodeB32(s, h[:])
	return h, err
}

// PlacementKey of a cap ID.
func (c CapID) PlacementKey() Hash { return Hash(sha256.Sum256(c[:])) }

// PlacementKey of an endpoint key.
func (k Key) PlacementKey() Hash { return Hash(sha256.Sum256(k[:])) }

// Sum hashes bytes.
func Sum(b []byte) Hash { return Hash(sha256.Sum256(b)) }

// RandBytes fills b from r and panics on failure: randomness failure is fatal.
func RandBytes(r io.Reader, b []byte) {
	if _, err := io.ReadFull(r, b); err != nil {
		panic("nimbo: randomness source failed: " + err.Error())
	}
}

// RandUint64 draws a uint64 from r.
func RandUint64(r io.Reader) uint64 {
	var b [8]byte
	RandBytes(r, b[:])
	return binary.LittleEndian.Uint64(b[:])
}
