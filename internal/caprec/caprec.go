// Package caprec defines the immutable cap record (design §8.2) and its
// deterministic CBOR encoding.
package caprec

import (
	"errors"

	"github.com/fxamacker/cbor/v2"

	"github.com/afk-surf/nimbo/internal/ids"
)

// Kind of cap record.
type Kind uint8

const (
	KindMint     Kind = 1
	KindTransfer Kind = 2
	KindReply    Kind = 3
)

func (k Kind) String() string {
	switch k {
	case KindMint:
		return "mint"
	case KindTransfer:
		return "transfer"
	case KindReply:
		return "reply"
	}
	return "unknown"
}

// Record is the immutable description of one cap.
type Record struct {
	Lineage   ids.Lineage `cbor:"1,keyasint"`
	Kind      Kind        `cbor:"2,keyasint"`
	SingleUse bool        `cbor:"3,keyasint,omitempty"`
	Target    ids.Key     `cbor:"4,keyasint"`
	Holder    ids.Key     `cbor:"5,keyasint"`
	Issuer    ids.Short   `cbor:"6,keyasint"`
	MintedAt  int64       `cbor:"7,keyasint"` // unix ms, informational
	Schema    *ids.Hash   `cbor:"8,keyasint,omitempty"`
	Base      *ids.Hash   `cbor:"9,keyasint,omitempty"`
	AutoReply bool        `cbor:"10,keyasint,omitempty"`
	Hints     []ids.Short `cbor:"11,keyasint,omitempty"`
	// AllowStream lets holders open streams to the target (design §9.7).
	// Only the target sets it; copies inherit it.
	AllowStream bool `cbor:"12,keyasint,omitempty"`
}

var (
	encMode cbor.EncMode
	decMode cbor.DecMode
)

func init() {
	var err error
	encMode, err = cbor.CoreDetEncOptions().EncMode()
	if err != nil {
		panic(err)
	}
	decMode, err = cbor.DecOptions{MaxArrayElements: 64, MaxMapPairs: 32, MaxNestedLevels: 4}.DecMode()
	if err != nil {
		panic(err)
	}
}

// Encode returns the deterministic CBOR encoding.
func (r *Record) Encode() []byte {
	b, err := encMode.Marshal(r)
	if err != nil {
		panic(err)
	}
	return b
}

// Decode parses a record.
func Decode(b []byte) (*Record, error) {
	var r Record
	if err := decMode.Unmarshal(b, &r); err != nil {
		return nil, err
	}
	if r.Kind < KindMint || r.Kind > KindReply {
		return nil, errors.New("caprec: bad kind")
	}
	if len(r.Hints) > 3 {
		return nil, errors.New("caprec: too many hints")
	}
	return &r, nil
}

// Derive returns a copy of r for a new holder, as TransferCap and carried-cap
// translation do.
func (r *Record) Derive(holder ids.Key, issuer ids.Short, now int64) *Record {
	c := *r
	c.Kind = KindTransfer
	c.Holder = holder
	c.Issuer = issuer
	c.MintedAt = now
	c.Hints = append([]ids.Short(nil), r.Hints...)
	return &c
}
