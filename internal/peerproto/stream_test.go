package peerproto

import (
	"bufio"
	"bytes"
	"io"
	"testing"

	"github.com/afk-surf/nimbo/internal/ids"
)

func TestStreamFraming(t *testing.T) {
	var buf bytes.Buffer
	id := ids.MsgID{1, 2, 3}
	if err := WriteStreamHeader(&buf, StreamHeader{ID: id}); err != nil {
		t.Fatal(err)
	}
	WriteChunk(&buf, ChunkBinary, []byte("ab"))
	WriteChunk(&buf, ChunkBinary|ChunkFin, nil)
	WriteChunk(&buf, ChunkClose, CloseBody(4901, "busy"))
	br := bufio.NewReader(&buf)
	var pre [4]byte
	io.ReadFull(br, pre[:])
	if pre != StreamPreamble {
		t.Fatalf("preamble %q", pre)
	}
	h, err := ReadStreamHeader(br)
	if err != nil || h.ID != id {
		t.Fatalf("header %v %v", h, err)
	}
	b := make([]byte, MaxChunk)
	f, d, err := ReadChunk(br, b)
	if err != nil || f != ChunkBinary || string(d) != "ab" {
		t.Fatalf("chunk 1: %x %q %v", f, d, err)
	}
	if f, d, _ = ReadChunk(br, b); f != ChunkBinary|ChunkFin || len(d) != 0 {
		t.Fatalf("chunk 2: %x %q", f, d)
	}
	f, d, _ = ReadChunk(br, b)
	if code, reason := ParseCloseBody(d); f != ChunkClose || code != 4901 || reason != "busy" {
		t.Fatalf("close: %x %d %q", f, code, reason)
	}
	// Oversized chunks are refused.
	buf.Reset()
	WriteChunk(&buf, 0, make([]byte, MaxChunk+1))
	if _, _, err := ReadChunk(bufio.NewReader(&buf), b); err == nil {
		t.Fatal("oversized chunk accepted")
	}
}
