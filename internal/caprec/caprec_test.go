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

// allow_stream is key 12 and omitted when false, so records without it
// encode exactly as before the field existed; copies inherit it.
func TestAllowStream(t *testing.T) {
	r := &Record{Kind: KindMint, Target: ids.Key{1}, Holder: ids.Key{1}, Issuer: ids.Short{2}, MintedAt: 5}
	plain := r.Encode()
	if bytes.Contains(plain, []byte{0x0c}) {
		t.Fatal("key 12 encoded for allow_stream=false")
	}
	r.AllowStream = true
	b := r.Encode()
	if len(b) != len(plain)+2 {
		t.Fatalf("allow_stream adds %d bytes, want 2", len(b)-len(plain))
	}
	r2, err := Decode(b)
	if err != nil || !r2.AllowStream {
		t.Fatalf("decode: %v %+v", err, r2)
	}
	if d := r2.Derive(ids.Key{9}, ids.Short{4}, 6); !d.AllowStream {
		t.Fatal("transfer dropped allow_stream")
	}
}
