// Package wire defines the endpoint WebSocket protocol (design §6): JSON text
// frames, the X25519 handshake proof, and error codes. It is shared by the
// Broker and the Go SDK; docs/protocol.md specifies it for other SDKs.
package wire

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
)

// Subprotocol is the WebSocket subprotocol name.
const Subprotocol = "nimbo.v1"

// StreamSubprotocol is the subprotocol of stream data sockets (/v1/stream).
const StreamSubprotocol = "nimbo.stream.v1"

// Error codes (design §6.3).
const (
	ErrBadRequest      = "bad_request"
	ErrInvalidCap      = "invalid_cap"
	ErrNotHolder       = "not_holder"
	ErrExpired         = "expired"
	ErrConsumed        = "consumed"
	ErrTargetGone      = "target_gone"
	ErrSchemaViolation = "schema_violation"
	ErrBaseConflict    = "base_conflict"
	ErrTooLarge        = "too_large"
	ErrRateLimited     = "rate_limited"
	ErrUnavailable     = "unavailable"
	// ErrStreamNotAllowed: stream on a cap without allow_stream.
	ErrStreamNotAllowed = "stream_not_allowed"
	// ErrInvalidChallenge: delegate named a delegation challenge that is
	// unknown, used, expired or from another session.
	ErrInvalidChallenge = "invalid_challenge"
	// ErrBadProof: a delegation proof does not match.
	ErrBadProof = "bad_proof"
)

// Bounce reasons.
const (
	BounceTTLExpired   = "ttl_expired"
	BounceReceiverBusy = "receiver_busy"
	BounceTooLarge     = "too_large"
	BounceTargetGone   = "target_gone"
	BounceUnavailable  = "unavailable"
)

// Stream states and failure reasons (stream_status).
const (
	StreamAccepted = "accepted"
	StreamFailed   = "failed"

	// ReasonPeerAttachTimeout: the other side did not attach in time.
	ReasonPeerAttachTimeout = "peer_attach_timeout"
	// ReasonPeerLost: a data connection or the other Broker failed.
	ReasonPeerLost = "peer_lost"
	// ReasonCanceled: the holder's data socket was lost before pairing.
	ReasonCanceled = "canceled"
	// ReasonStreamRejected is the reason a target refusing a stream uses.
	ReasonStreamRejected = "stream_rejected"
	ReasonDraining       = "draining"
)

// Close codes on stream data sockets. 1000 and 3000-4899 pass through from
// endpoints; 4900-4999 are reserved for Nimbo (an endpoint may send 4901 to
// refuse a stream).
const (
	CloseNormal            = 1000
	CloseDraining          = 1001
	CloseStreamRejected    = 4901
	CloseTTLExpired        = 4902
	CloseTargetGone        = 4903
	CloseReceiverBusy      = 4904
	ClosePeerAttachTimeout = 4905
	ClosePeerLost          = 4911
	CloseRateLimited       = 4929
	CloseUnavailable       = 4950
)

// CloseCodeFor maps a stream failure reason to its close code.
func CloseCodeFor(reason string) int {
	switch reason {
	case BounceTTLExpired:
		return CloseTTLExpired
	case BounceTargetGone:
		return CloseTargetGone
	case BounceReceiverBusy, BounceTooLarge:
		return CloseReceiverBusy
	case ReasonPeerAttachTimeout:
		return ClosePeerAttachTimeout
	case ReasonPeerLost:
		return ClosePeerLost
	case ErrRateLimited:
		return CloseRateLimited
	case ReasonDraining:
		return CloseDraining
	case ReasonStreamRejected:
		return CloseStreamRejected
	}
	return CloseUnavailable
}

// EndpointCloseCode is the code a Broker relays for a close code an endpoint
// sent on its data socket: 1000, 3000-4899 and 4901 pass through, anything
// else becomes 1000.
func EndpointCloseCode(code int) int {
	if code == CloseNormal || code == CloseStreamRejected || (code >= 3000 && code < 4900) {
		return code
	}
	return CloseNormal
}

// In is any endpoint → broker frame. Only the fields of its op are set.
type In struct {
	Op  string `json:"op"`
	Rid uint64 `json:"rid,omitempty"`

	// hello
	Key             string `json:"key,omitempty"`
	Authority       string `json:"authority,omitempty"`
	Proof           string `json:"proof,omitempty"`
	Client          string `json:"client,omitempty"`
	MaxMessageBytes int    `json:"max_message_bytes,omitempty"`

	// send, stream, transfer, inspect
	Cap     string          `json:"cap,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Caps    []string        `json:"caps,omitempty"` // send: carried; renew: caps to renew
	TTLms   int64           `json:"ttl_ms,omitempty"`
	Reply   *ReplyOpts      `json:"reply,omitempty"`

	// mint
	Schema    json.RawMessage `json:"schema,omitempty"`
	Base      json.RawMessage `json:"base,omitempty"`
	AutoReply bool            `json:"auto_reply,omitempty"`
	Durable   bool            `json:"durable,omitempty"`
	// AllowStream lets holders open streams to the minter.
	AllowStream bool `json:"allow_stream,omitempty"`

	// transfer
	To string `json:"to,omitempty"`

	// delegate_begin: the endpoint key the delegated cap targets (with the
	// mint fields above). delegate: the challenge's Eph and the Proof.
	Target string `json:"target,omitempty"`
	Eph    string `json:"eph,omitempty"`
}

// ReplyOpts configure the single-use reply cap of a send.
type ReplyOpts struct {
	Schema      json.RawMessage `json:"schema,omitempty"`
	Base        json.RawMessage `json:"base,omitempty"`
	AutoReply   bool            `json:"auto_reply,omitempty"`
	AllowStream bool            `json:"allow_stream,omitempty"`
}

// Challenge is the first broker frame.
type Challenge struct {
	Op     string `json:"op"` // "challenge"
	V      int    `json:"v"`
	Broker string `json:"broker"`
	Eph    string `json:"eph"`
	Nonce  string `json:"nonce"`
}

// Limits advertised in welcome.
type Limits struct {
	MaxPayloadBytes int   `json:"max_payload_bytes"`
	MaxCarriedCaps  int   `json:"max_carried_caps"`
	MaxSchemaBytes  int   `json:"max_schema_bytes"`
	MaxBaseBytes    int   `json:"max_base_bytes"`
	ActionsPerSec   int   `json:"actions_per_sec"`
	MintsPerSec     int   `json:"mints_per_sec"`
	MaxLiveCaps     int   `json:"max_live_caps"`
	DefaultTTLms    int64 `json:"default_ttl_ms"`
	MaxTTLms        int64 `json:"max_ttl_ms"`
	// Streams (design §9.7).
	MaxPendingStreams  int   `json:"max_pending_streams"`
	StreamAttachTTLms  int64 `json:"stream_attach_ttl_ms"`
	StreamSocketsPerIP int   `json:"stream_sockets_per_ip"`
}

type Welcome struct {
	Op      string   `json:"op"` // "welcome"
	Session string   `json:"session"`
	Limits  Limits   `json:"limits"`
	Brokers []string `json:"brokers,omitempty"`
}

type Error struct {
	Code   string `json:"code"`
	Detail string `json:"detail,omitempty"`
}

// RenewItem is the per-cap answer of renew.
type RenewItem struct {
	Cap   string `json:"cap"`
	OK    bool   `json:"ok"`
	TTLms int64  `json:"ttl_ms,omitempty"`
	Error *Error `json:"error,omitempty"`
}

// CapInfo is the inspect answer.
type CapInfo struct {
	Lineage     string          `json:"lineage"`
	Kind        string          `json:"kind"`
	SingleUse   bool            `json:"single_use,omitempty"`
	Target      string          `json:"target"`
	Holder      string          `json:"holder"`
	Issuer      string          `json:"issuer"`
	MintedAt    int64           `json:"minted_at"`
	Schema      json.RawMessage `json:"schema,omitempty"`
	Base        json.RawMessage `json:"base,omitempty"`
	AutoReply   bool            `json:"auto_reply,omitempty"`
	AllowStream bool            `json:"allow_stream,omitempty"`
	TTLms       int64           `json:"ttl_ms"`
}

// Result answers a request.
type Result struct {
	Op      string      `json:"op"` // "result"
	Rid     uint64      `json:"rid"`
	OK      bool        `json:"ok"`
	Error   *Error      `json:"error,omitempty"`
	Msg     string      `json:"msg,omitempty"`
	ReplyID string      `json:"reply_id,omitempty"`
	Cap     string      `json:"cap,omitempty"`
	Renewed []RenewItem `json:"renewed,omitempty"`
	Info    *CapInfo    `json:"info,omitempty"`
	// stream: the stream ID and the holder's data-socket token.
	Stream string `json:"stream,omitempty"`
	Token  string `json:"token,omitempty"`
	// delegate_begin: the challenge (a Broker ephemeral X25519 key), this
	// Broker's ID (hex), the blob hashes of the requested schema and base,
	// and how long the challenge stays valid.
	Eph        string `json:"eph,omitempty"`
	Broker     string `json:"broker,omitempty"`
	SchemaHash string `json:"schema_hash,omitempty"`
	BaseHash   string `json:"base_hash,omitempty"`
	TTLms      int64  `json:"ttl_ms,omitempty"`
}

type Deliver struct {
	Op      string          `json:"op"` // "deliver"
	Msg     string          `json:"msg"`
	On      string          `json:"on"`
	Payload json.RawMessage `json:"payload"`
	Caps    []string        `json:"caps,omitempty"`
	Reply   string          `json:"reply,omitempty"`
	// A stream offer carries the stream ID and the target's data-socket
	// token.
	Stream      string `json:"stream,omitempty"`
	StreamToken string `json:"stream_token,omitempty"`
}

// StreamStatus reports the outcome of a stream to its holder, once.
type StreamStatus struct {
	Op     string `json:"op"` // "stream_status"
	Stream string `json:"stream"`
	State  string `json:"state"` // "accepted" or "failed"
	Reason string `json:"reason,omitempty"`
	Detail string `json:"detail,omitempty"`
}

type Bounce struct {
	Op     string `json:"op"` // "bounce"
	Msg    string `json:"msg"`
	Reason string `json:"reason"`
}

type Closing struct {
	Op           string `json:"op"` // "closing"
	Reason       string `json:"reason"`
	RetryAfterMs int64  `json:"retry_after_ms"`
}

// Out is used to decode any broker → endpoint frame.
type Out struct {
	Op string `json:"op"`
	// challenge
	V      int    `json:"v,omitempty"`
	Broker string `json:"broker,omitempty"`
	Eph    string `json:"eph,omitempty"`
	Nonce  string `json:"nonce,omitempty"`
	// welcome
	Session string   `json:"session,omitempty"`
	Limits  *Limits  `json:"limits,omitempty"`
	Brokers []string `json:"brokers,omitempty"`
	// result
	Rid     uint64      `json:"rid,omitempty"`
	OK      bool        `json:"ok,omitempty"`
	Error   *Error      `json:"error,omitempty"`
	Msg     string      `json:"msg,omitempty"`
	ReplyID string      `json:"reply_id,omitempty"`
	Cap     string      `json:"cap,omitempty"`
	Renewed []RenewItem `json:"renewed,omitempty"`
	Info    *CapInfo    `json:"info,omitempty"`
	// deliver
	On      string          `json:"on,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Caps    []string        `json:"caps,omitempty"`
	Reply   string          `json:"reply,omitempty"`
	// stream result, deliver of a stream offer, stream_status
	Stream      string `json:"stream,omitempty"`
	Token       string `json:"token,omitempty"`
	StreamToken string `json:"stream_token,omitempty"`
	State       string `json:"state,omitempty"`
	Detail      string `json:"detail,omitempty"`
	// bounce, closing, stream_status
	Reason       string `json:"reason,omitempty"`
	RetryAfterMs int64  `json:"retry_after_ms,omitempty"`
	// delegate_begin result (with Eph and Broker above)
	SchemaHash string `json:"schema_hash,omitempty"`
	BaseHash   string `json:"base_hash,omitempty"`
	TTLms      int64  `json:"ttl_ms,omitempty"`
}

// Proof computes the hello proof:
// HMAC-SHA256(SHA-256("nimbo/v1/auth" ‖ ss), nonce ‖ authority ‖ E_pub ‖ B_eph_pub).
func Proof(ss, nonce []byte, authority string, ePub, bEph []byte) []byte {
	k := sha256.Sum256(append([]byte("nimbo/v1/auth"), ss...))
	m := hmac.New(sha256.New, k[:])
	m.Write(nonce)
	m.Write([]byte(authority))
	m.Write(ePub)
	m.Write(bEph)
	return m.Sum(nil)
}

// Grant is what a delegation proof authorizes: a cap targeting Target, held
// by Holder, with these parameters, issued by Broker against the challenge
// Eph (design §8.6).
type Grant struct {
	Broker      [32]byte
	Eph         [32]byte
	Target      [32]byte
	Holder      [32]byte
	AutoReply   bool
	AllowStream bool
	Schema      *[32]byte // blob hash of the schema, if any
	Base        *[32]byte // blob hash of the base, if any
}

// Grant flag bits.
const (
	GrantAutoReply   = 1 << 0
	GrantAllowStream = 1 << 1
	GrantHasSchema   = 1 << 2
	GrantHasBase     = 1 << 3
)

// Message is the grant as the delegation proof covers it:
// broker_id ‖ eph ‖ target ‖ holder ‖ flags ‖ schema_hash ‖ base_hash, with
// 32 zero bytes for a missing hash. It is 193 bytes.
func (g *Grant) Message() []byte {
	b := make([]byte, 0, 193)
	b = append(b, g.Broker[:]...)
	b = append(b, g.Eph[:]...)
	b = append(b, g.Target[:]...)
	b = append(b, g.Holder[:]...)
	var flags byte
	if g.AutoReply {
		flags |= GrantAutoReply
	}
	if g.AllowStream {
		flags |= GrantAllowStream
	}
	if g.Schema != nil {
		flags |= GrantHasSchema
	}
	if g.Base != nil {
		flags |= GrantHasBase
	}
	b = append(b, flags)
	var zero [32]byte
	for _, h := range []*[32]byte{g.Schema, g.Base} {
		if h != nil {
			b = append(b, h[:]...)
		} else {
			b = append(b, zero[:]...)
		}
	}
	return b
}

// DelegationProof computes the proof of a delegated issuance:
// HMAC-SHA256(SHA-256("nimbo/v1/delegate" ‖ ss), grant message), where
// ss = X25519(target_priv, eph).
func DelegationProof(ss []byte, g *Grant) []byte {
	k := sha256.Sum256(append([]byte("nimbo/v1/delegate"), ss...))
	m := hmac.New(sha256.New, k[:])
	m.Write(g.Message())
	return m.Sum(nil)
}
