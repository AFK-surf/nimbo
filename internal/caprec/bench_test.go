package caprec

import (
	"testing"

	"github.com/afk-surf/nimbo/internal/ids"
)

func sample() *Record {
	h := ids.Sum([]byte("schema"))
	return &Record{Kind: KindTransfer, Target: ids.Key{1}, Holder: ids.Key{2}, Issuer: ids.Short{3}, MintedAt: 1e12, Schema: &h, AutoReply: true, Hints: []ids.Short{{4}}}
}

func BenchmarkEncode(b *testing.B) {
	r := sample()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r.Encode()
	}
}

func BenchmarkDecode(b *testing.B) {
	enc := sample().Encode()
	b.SetBytes(int64(len(enc)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := Decode(enc); err != nil {
			b.Fatal(err)
		}
	}
}
