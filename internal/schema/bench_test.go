package schema

import (
	"testing"

	"github.com/afk-surf/nimbo/internal/ids"
)

const benchSchema = `{"type":"object","required":["q","n"],"properties":{"q":{"type":"string","maxLength":200},"n":{"type":"integer","minimum":0},"tags":{"type":"array","items":{"type":"string"}}}}`

func BenchmarkValidateCached(b *testing.B) {
	c := NewCache(16)
	s, err := c.Get(ids.Sum([]byte(benchSchema)), []byte(benchSchema))
	if err != nil {
		b.Fatal(err)
	}
	doc := []byte(`{"q":"what is the weather in Lisbon","n":3,"tags":["a","b","c"]}`)
	b.SetBytes(int64(len(doc)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := Validate(s, doc); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompile(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := Compile([]byte(benchSchema)); err != nil {
			b.Fatal(err)
		}
	}
}
