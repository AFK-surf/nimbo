package peerproto

import (
	"bytes"
	"testing"

	"github.com/afk-surf/nimbo/internal/ids"
)

func forward(payload int) *Envelope {
	rec := bytes.Repeat([]byte{7}, 150)
	return &Envelope{From: ids.BrokerID{1}, Inc: 9, Req: 42, Forward: &Forward{
		Msg: ids.MsgID{1}, TTLms: 10000, Hop: 1,
		Target:  CapRef{ID: ids.CapID{2}, Record: rec},
		Payload: bytes.Repeat([]byte{'a'}, payload),
		Ingress: ids.BrokerID{3}, IngressSession: 7, Sender: ids.Key{4},
	}}
}

func touches(n int) *Envelope {
	ops := make([]Op, n)
	for i := range ops {
		ops[i] = Op{Touch: &Touch{Kind: KindCap, ID: bytes.Repeat([]byte{byte(i)}, 16), TTLms: 60000}}
	}
	return &Envelope{From: ids.BrokerID{1}, Inc: 9, Req: 43, RCR: &RCRBatch{Ops: ops}}
}

func benchMarshal(b *testing.B, e *Envelope) {
	b.SetBytes(int64(len(Marshal(e))))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		Marshal(e)
	}
}

func benchUnmarshal(b *testing.B, e *Envelope) {
	enc := Marshal(e)
	b.SetBytes(int64(len(enc)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := Unmarshal(enc); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshalForward1K(b *testing.B)   { benchMarshal(b, forward(1024)) }
func BenchmarkUnmarshalForward1K(b *testing.B) { benchUnmarshal(b, forward(1024)) }
func BenchmarkMarshalTouches64(b *testing.B)   { benchMarshal(b, touches(64)) }
func BenchmarkUnmarshalTouches64(b *testing.B) { benchUnmarshal(b, touches(64)) }
