package peerproto

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"

	"github.com/afk-surf/nimbo/internal/ids"
)

// Stream data connections (design §9.7). A data connection carries the bytes
// of one stream between the target's Broker (which dials) and the holder's
// Broker. It starts with StreamPreamble and a length-prefixed CBOR
// StreamHeader; the holder's Broker answers with one status byte. After a
// "paired" status, both directions carry chunk frames:
//
//	[flags u8][len uvarint][len bytes]
//
// A close frame carries a big-endian u16 close code followed by the reason.

// StreamPreamble starts every stream data connection.
var StreamPreamble = [4]byte{'N', 'M', 'S', 1}

// MaxChunk bounds the bytes of one data chunk.
const MaxChunk = 32 << 10

// maxCloseBody is a u16 code plus a WebSocket close reason (≤ 123 bytes).
const maxCloseBody = 2 + 123

// StreamHeader names the stream a data connection belongs to.
type StreamHeader struct {
	ID ids.MsgID `cbor:"1,keyasint"`
}

// Status bytes answering a StreamHeader.
const (
	StreamPaired byte = 1
	StreamNoHalf byte = 2
	StreamBusy   byte = 3
)

// Chunk flags.
const (
	ChunkBinary byte = 1 << 0 // the message is binary (else text)
	ChunkFin    byte = 1 << 1 // the last chunk of a message
	ChunkClose  byte = 1 << 2 // a close frame
)

// WriteStreamHeader writes the preamble and header.
func WriteStreamHeader(w io.Writer, h StreamHeader) error {
	b, err := encMode.Marshal(h)
	if err != nil {
		return err
	}
	buf := append([]byte(nil), StreamPreamble[:]...)
	buf = binary.AppendUvarint(buf, uint64(len(b)))
	buf = append(buf, b...)
	_, err = w.Write(buf)
	return err
}

// ReadStreamHeader reads the header that follows the preamble.
func ReadStreamHeader(r *bufio.Reader) (StreamHeader, error) {
	var h StreamHeader
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return h, err
	}
	if n > 64 {
		return h, errors.New("peerproto: stream header too large")
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return h, err
	}
	err = decMode.Unmarshal(b, &h)
	return h, err
}

// WriteChunk writes one chunk frame.
func WriteChunk(w io.Writer, flags byte, data []byte) error {
	var hdr [1 + binary.MaxVarintLen64]byte
	hdr[0] = flags
	n := binary.PutUvarint(hdr[1:], uint64(len(data)))
	if _, err := w.Write(hdr[:1+n]); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

// CloseBody encodes a close frame's body.
func CloseBody(code uint16, reason string) []byte {
	if len(reason) > maxCloseBody-2 {
		reason = reason[:maxCloseBody-2]
	}
	b := binary.BigEndian.AppendUint16(nil, code)
	return append(b, reason...)
}

// ParseCloseBody decodes a close frame's body.
func ParseCloseBody(b []byte) (uint16, string) {
	if len(b) < 2 {
		return 1000, ""
	}
	return binary.BigEndian.Uint16(b), string(b[2:])
}

// ReadChunk reads one chunk frame into buf (which must hold MaxChunk bytes)
// and returns its flags and data.
func ReadChunk(r *bufio.Reader, buf []byte) (byte, []byte, error) {
	flags, err := r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, nil, err
	}
	limit := uint64(MaxChunk)
	if flags&ChunkClose != 0 {
		limit = maxCloseBody
	}
	if n > limit || n > uint64(len(buf)) {
		return 0, nil, errors.New("peerproto: chunk too large")
	}
	if _, err := io.ReadFull(r, buf[:n]); err != nil {
		return 0, nil, err
	}
	return flags, buf[:n], nil
}
