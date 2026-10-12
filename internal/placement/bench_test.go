package placement

import (
	"encoding/binary"
	"testing"

	"github.com/afk-surf/nimbo/internal/ids"
)

func benchNearest(b *testing.B, brokers, k int) {
	ix := New(brokersN(brokers), 16)
	var buf [8]byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		ix.Nearest(ids.Sum(buf[:]), k, nil)
	}
}

func brokersN(n int) []ids.BrokerID { return brokers(n) }

func BenchmarkNearest3of10(b *testing.B)   { benchNearest(b, 10, 3) }
func BenchmarkNearest3of100(b *testing.B)  { benchNearest(b, 100, 3) }
func BenchmarkNearest1of100(b *testing.B)  { benchNearest(b, 100, 1) }
func BenchmarkNearest3of1000(b *testing.B) { benchNearest(b, 1000, 3) }
