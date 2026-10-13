// Package peerproto defines the Broker-to-Broker RPC messages (design §5) and
// their framing. Messages are CBOR maps with integer keys; every frame on a
// stream is a 4-byte big-endian length followed by one Envelope. Each stream
// starts with the 4-byte preamble "NMB" + version.
//
// All durations are remaining milliseconds, never absolute times.
package peerproto

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"

	"github.com/fxamacker/cbor/v2"

	"github.com/afk-surf/nimbo/internal/ids"
	"github.com/afk-surf/nimbo/internal/presence"
)

// Version of the peer protocol.
const Version = 1

// Preamble starts every peer stream.
var Preamble = [4]byte{'N', 'M', 'B', Version}

// MaxFrame bounds a single frame.
const MaxFrame = 8 << 20

// RecKind is the kind of an RCR record.
type RecKind uint8

const (
	KindCap      RecKind = 1
	KindBlob     RecKind = 2
	KindPresence RecKind = 3
)

// BlobKind distinguishes schema and base blobs.
type BlobKind uint8

const (
	BlobSchema BlobKind = 1
	BlobBase   BlobKind = 2
)

// Envelope carries exactly one message body.
type Envelope struct {
	From ids.BrokerID `cbor:"1,keyasint"`
	Inc  uint64       `cbor:"2,keyasint"`
	// Req is set on requests that expect a reply.
	Req uint64 `cbor:"3,keyasint,omitempty"`
	// Resp is set on replies, to the request's Req.
	Resp uint64 `cbor:"4,keyasint,omitempty"`

	Forward       *Forward       `cbor:"10,keyasint,omitempty"`
	ForwardResult *ForwardResult `cbor:"11,keyasint,omitempty"`
	RCR           *RCRBatch      `cbor:"12,keyasint,omitempty"`
	RCRAck        *RCRAck        `cbor:"13,keyasint,omitempty"`
	Get           *Get           `cbor:"14,keyasint,omitempty"`
	GetResult     *GetResult     `cbor:"15,keyasint,omitempty"`
	Consume       *Consume       `cbor:"16,keyasint,omitempty"`
	ConsumeResult *ConsumeResult `cbor:"17,keyasint,omitempty"`
	Watch         *Watch         `cbor:"18,keyasint,omitempty"`
	WatchResult   *WatchResult   `cbor:"19,keyasint,omitempty"`
	Notify        *Notify        `cbor:"20,keyasint,omitempty"`
	Rejoin        *Rejoin        `cbor:"21,keyasint,omitempty"`
	RejoinAck     *RejoinAck     `cbor:"22,keyasint,omitempty"`
	Bounce        *Bounce        `cbor:"23,keyasint,omitempty"`
}

// Rec is a cap or blob record with its mutable state.
type Rec struct {
	Kind     RecKind  `cbor:"1,keyasint"`
	ID       []byte   `cbor:"2,keyasint"` // cap ID (16B) or blob hash (32B)
	Data     []byte   `cbor:"3,keyasint"` // cap record CBOR or blob bytes
	BlobKind BlobKind `cbor:"4,keyasint,omitempty"`
	TTLms    int64    `cbor:"5,keyasint"` // remaining; negative if past deadline
	Consumed bool     `cbor:"6,keyasint,omitempty"`
	Origin   uint64   `cbor:"7,keyasint,omitempty"` // stamp of the use that set the deadline
}

// Touch extends a record's deadline.
type Touch struct {
	Kind   RecKind `cbor:"1,keyasint"`
	ID     []byte  `cbor:"2,keyasint"`
	TTLms  int64   `cbor:"3,keyasint"`
	Origin uint64  `cbor:"4,keyasint,omitempty"`
}

// PresenceStore carries full registers for one endpoint.
type PresenceStore struct {
	Key  ids.Key         `cbor:"1,keyasint"`
	Regs []presence.Wire `cbor:"2,keyasint"`
}

// Op is one replicated write.
type Op struct {
	Store    *Rec           `cbor:"1,keyasint,omitempty"`
	Touch    *Touch         `cbor:"2,keyasint,omitempty"`
	Presence *PresenceStore `cbor:"3,keyasint,omitempty"`
	// Hint names the intended placement member of a hinted copy.
	Hint *ids.BrokerID `cbor:"4,keyasint,omitempty"`
}

// RCRBatch coalesces writes to one Broker.
type RCRBatch struct {
	Ops []Op `cbor:"1,keyasint"`
}

// OpResult is the per-op outcome of an RCRBatch.
type OpResult uint8

const (
	OpOK       OpResult = 0
	OpMissing  OpResult = 1 // Touch for a record the holder does not have
	OpRejected OpResult = 2 // holder is fenced and not acting as a write target
)

// RCRAck answers an RCRBatch, one result per op.
type RCRAck struct {
	Results []OpResult `cbor:"1,keyasint"`
}

// Get looks a record up. For presence, ID is the endpoint key.
type Get struct {
	Kind RecKind `cbor:"1,keyasint"`
	ID   []byte  `cbor:"2,keyasint"`
}

// GetResult answers a Get.
type GetResult struct {
	Found bool            `cbor:"1,keyasint,omitempty"`
	Rec   *Rec            `cbor:"2,keyasint,omitempty"`
	Regs  []presence.Wire `cbor:"3,keyasint,omitempty"`
}

// Consume flips a single-use cap's consumed flag at its issuer.
type Consume struct {
	ID ids.CapID `cbor:"1,keyasint"`
}

// ConsumeCode is the outcome of a Consume.
type ConsumeCode uint8

const (
	ConsumeOK       ConsumeCode = 0
	ConsumeConsumed ConsumeCode = 1
	ConsumeMissing  ConsumeCode = 2
)

type ConsumeResult struct {
	Code ConsumeCode `cbor:"1,keyasint"`
}

// Watch registers interest in an endpoint's presence.
type Watch struct {
	Key   ids.Key `cbor:"1,keyasint"`
	TTLms int64   `cbor:"2,keyasint"`
}

// WatchResult returns the current registers (check-then-register).
type WatchResult struct {
	Regs []presence.Wire `cbor:"1,keyasint,omitempty"`
}

// Notify reports a register change to a watcher.
type Notify struct {
	Key  ids.Key         `cbor:"1,keyasint"`
	Regs []presence.Wire `cbor:"2,keyasint,omitempty"`
}

// Rejoin announces a (re)started or draining Broker. Member is the JSON
// member object as written to S3.
type Rejoin struct {
	Member []byte `cbor:"1,keyasint"`
}

type RejoinAck struct{}

// CapRef is a cap ID with its record, carried in a Forward.
type CapRef struct {
	ID     ids.CapID `cbor:"1,keyasint"`
	Record []byte    `cbor:"2,keyasint"`
}

// Forward hands a message to another Broker.
type Forward struct {
	Msg            ids.MsgID    `cbor:"1,keyasint"`
	TTLms          int64        `cbor:"2,keyasint"`
	Hop            uint8        `cbor:"3,keyasint"`
	Reroutes       uint8        `cbor:"4,keyasint,omitempty"`
	Target         CapRef       `cbor:"5,keyasint"`
	Payload        []byte       `cbor:"6,keyasint"`
	Carried        []CapRef     `cbor:"7,keyasint,omitempty"`
	ReplyID        *ids.Lineage `cbor:"8,keyasint,omitempty"`
	ReplySchema    *ids.Hash    `cbor:"9,keyasint,omitempty"`
	ReplyBase      *ids.Hash    `cbor:"10,keyasint,omitempty"`
	ReplyAuto      bool         `cbor:"11,keyasint,omitempty"`
	Ingress        ids.BrokerID `cbor:"12,keyasint"`
	IngressSession uint64       `cbor:"13,keyasint"`
	Sender         ids.Key      `cbor:"14,keyasint"`
	SenderHints    []ids.Short  `cbor:"15,keyasint,omitempty"`
	// Stream marks a stream offer (design §9.7); ReplyStream sets
	// allow_stream on the receiver's reply cap.
	Stream      bool `cbor:"16,keyasint,omitempty"`
	ReplyStream bool `cbor:"17,keyasint,omitempty"`
}

// ForwardCode is the outcome of a Forward.
type ForwardCode uint8

const (
	FwdAccepted ForwardCode = 0
	FwdNotHere  ForwardCode = 1
	FwdRejected ForwardCode = 2
)

type ForwardResult struct {
	Code   ForwardCode `cbor:"1,keyasint"`
	Reason string      `cbor:"2,keyasint,omitempty"`
}

// Bounce reports an undeliverable message back to its ingress Broker.
type Bounce struct {
	Msg     ids.MsgID `cbor:"1,keyasint"`
	Session uint64    `cbor:"2,keyasint"`
	Reason  string    `cbor:"3,keyasint"`
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
	decMode, err = cbor.DecOptions{MaxArrayElements: 1 << 20}.DecMode()
	if err != nil {
		panic(err)
	}
}

// Marshal encodes an envelope.
func Marshal(e *Envelope) []byte {
	b, err := encMode.Marshal(e)
	if err != nil {
		panic(err)
	}
	return b
}

// Unmarshal decodes an envelope.
func Unmarshal(b []byte) (*Envelope, error) {
	var e Envelope
	if err := decMode.Unmarshal(b, &e); err != nil {
		return nil, err
	}
	return &e, nil
}

// WriteFrame writes one length-prefixed envelope.
func WriteFrame(w io.Writer, e *Envelope) error {
	b := Marshal(e)
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(b)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

// ReadFrame reads one length-prefixed envelope.
func ReadFrame(r *bufio.Reader) (*Envelope, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > MaxFrame {
		return nil, errors.New("peerproto: frame too large")
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	return Unmarshal(b)
}
