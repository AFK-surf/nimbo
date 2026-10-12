package caprec

import (
	"bytes"
	"testing"

	"github.com/afk-surf/nimbo/internal/ids"
)

func TestRoundTripDeterministic(t *testing.T) {
	h := ids.Sum([]byte("schema"))
	r := &Record{Kind: KindMint, Target: ids.Key{1}, Holder: ids.Key{1}, Issuer: ids.Short{2}, MintedAt: 5, Schema: &h, Hints: []ids.Short{{3}}}
	b1 := r.Encode()
	r2, err := Decode(b1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(r2.Encode(), b1) {
		t.Fatal("not deterministic")
	}
	if *r2.Schema != h || r2.Base != nil || r2.Hints[0] != (ids.Short{3}) {
		t.Fatalf("mismatch %+v", r2)
	}
}
