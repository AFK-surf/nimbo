package presence

import (
	"testing"
	"time"

	"github.com/afk-surf/nimbo/internal/ids"
)

func BenchmarkMergeAndClassify(b *testing.B) {
	now := time.Unix(1e6, 0)
	held := Set{
		{Owner: ids.BrokerID{1}, Inc: 1}: {Seq: 5, Sessions: 1},
		{Owner: ids.BrokerID{2}, Inc: 1}: {Seq: 3, Tomb: true, AbsentSince: now.Add(-time.Minute)},
	}
	in := Set{{Owner: ids.BrokerID{1}, Inc: 1}: {Seq: 6, Sessions: 2}}
	valid := func(RegKey) Validity { return Valid }
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s := held.Clone()
		s.MergeInto(in)
		Classify(s, now, time.Minute, valid)
	}
}

func BenchmarkWireRoundTrip(b *testing.B) {
	now := time.Unix(1e6, 0)
	s := Set{
		{Owner: ids.BrokerID{1}, Inc: 1}: {Seq: 5, Sessions: 1},
		{Owner: ids.BrokerID{2}, Inc: 1}: {Seq: 3, Tomb: true, AbsentSince: now.Add(-time.Minute)},
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		FromWire(ToWire(s, now), now, 10*time.Minute)
	}
}
